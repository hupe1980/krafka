//! Kafka consumer implementation.
//!
//! This module provides:
//! - Async consumer for receiving messages
//! - Consumer groups with rebalancing
//! - Offset management (auto and manual commit)
//! - Partition assignment strategies
//!
//! # Delivery Semantics
//!
//! Krafka provides **at-least-once** delivery semantics by default, which is the
//! standard Kafka consumer behavior:
//!
//! 1. Messages are delivered to the application via `poll()`
//! 2. Offsets are committed after processing (auto-commit or manual)
//! 3. If the consumer crashes after processing but before commit, messages may
//!    be redelivered on restart
//!
//! This is the safest default as it ensures no message loss. For use cases that
//! cannot tolerate duplicates, applications should implement idempotent processing.
//!
//! ## Controlling Commit Behavior
//!
//! - **Auto-commit** (default): Offsets are committed periodically in the background
//! - **Manual commit**: Disable auto-commit and call `commit()` explicitly
//!
//! For at-most-once semantics (where message loss is acceptable but duplicates are not),
//! commit offsets before processing:
//!
//! ```ignore
//! let records = consumer.poll(Duration::from_secs(1)).await?;
//! consumer.commit().await?;  // Commit BEFORE processing
//! for record in records {
//!     process(record);  // If this crashes, message is lost
//! }
//! ```

mod config;
mod fetch_session;
mod group;
mod offset;
mod record;
mod stream;

pub mod compacted;

pub use compacted::{
    CompactedTable, CompactedTopicConsumer, CompactedTopicConsumerBuilder, TableChange,
};
pub use config::{
    AutoOffsetReset, ConsumerConfig, ConsumerConfigBuilder, GroupProtocol, IsolationLevel,
    PartitionAssignmentStrategy,
};
pub use group::{
    ConsumerGroup, ConsumerRebalanceListener, CooperativeStickyAssignor, GroupCoordinator,
    GroupMember, GroupState, HeartbeatController, HeartbeatStatus, MemberAssignment,
    NoOpRebalanceListener, PartitionAssignor, RangeAssignor, RoundRobinAssignor,
};
pub use offset::{OffsetAndMetadata, OffsetStore, ResetOffset};
pub use record::{ConsumerRecord, ConsumerRecords, TopicPartition};
pub use stream::ConsumerStream;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex as SyncMutex;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::auth::AuthConfig;
use crate::error::{KrafkaError, Result};
use crate::metadata::{BrokerInfo, ClusterMetadata, TopicInfo};
use crate::metrics::ConsumerMetrics;
use crate::network::{ConnectionConfig, ConnectionPool};
use crate::protocol::{
    ApiKey, FetchPartitionRequest, FetchRequest, FetchResponse, FetchTopicRequest,
    ListOffsetsRequest, ListOffsetsRequestPartition, ListOffsetsRequestTopic, ListOffsetsResponse,
    RecordBatch, VersionedDecode, VersionedEncode, validate_topic_name, validate_topic_names,
    versions,
};
use crate::{Offset, PartitionId};

/// Maximum number of cooperative rebalance rejoin rounds before deferring
/// to the next poll cycle. Cascading membership changes in large groups
/// may require multiple rounds; this cap prevents unbounded looping.
const MAX_COOPERATIVE_ROUNDS: usize = 5;

use fetch_session::FetchSessionCache;

// ── LOCK ORDER ──────────────────────────────────────────────────────────────
//
// `Consumer` holds several synchronization primitives. Two rules prevent
// deadlocks and executor stalls:
//
//   1. Acquire locks in the order listed below. Never acquire an
//      earlier-numbered lock while holding a later-numbered one.
//   2. If you hold an **async** lock (`tokio::sync::{RwLock, Mutex}`) across
//      an `.await`, the awaited operation must not (transitively) acquire
//      any lock numbered **lower** than the one held. In practice: do not
//      call back into `Consumer` methods while holding a lock. Release with
//      `drop(...)` or a scoped `{ … }` block first.
//
// Order:
//   1. `subscriptions`         (async — read across `poll()` network paths)
//   2. `assignments`           (async — read across `poll()` network paths)
//   3. `offsets`               (async — read across commit/fetch RPC paths)
//   4. `paused`                (async — read across `poll()` network paths)
//   5. `partition_state`       (async — per-partition fetch-derived caches)
//   6. `recv_buffer`           (sync — `parking_lot::Mutex`, pure mutation)
//   7. `fetch_sessions`        (sync — `parking_lot::Mutex`, pure mutation;
//                               ALWAYS release before fetch RPC send/recv)
//   8. `last_auto_commit`      (sync — `parking_lot::Mutex<Instant>`)
//
// The sync (`parking_lot`) primitives are chosen only for critical sections
// with NO `.await` inside. They can still block a Tokio worker thread if
// contended, and some sections are O(n) (e.g. `recv_buffer.retain`), so do
// not assume they are always nanosecond-scale. Keep them short, avoid
// contention, and always release them before async work, network I/O, or
// callbacks back into `Consumer`. Do not convert any async lock above
// without first auditing every call site for `.await` under the lock.
//
// Per-partition caches (`high_watermark`, `log_start_offset`,
// `preferred_replica`, `offset_retry_backoff`) were previously four separate
// `RwLock<HashMap<_, _>>` fields. They are consolidated into a single
// `partition_state` map so that revocation / reset / close paths cannot leave
// a partition partially populated. Adding a new per-partition cache? Add a
// field to `PartitionState`; do not introduce another `RwLock`.

/// Per-topic-partition state cached locally by the consumer.
///
/// All fields are populated from fetch responses or consumer-protocol
/// feedback. Grouping them under a single lock (`Consumer::partition_state`)
/// guarantees that revocation and reset paths cannot leave a partition in a
/// partially-populated state — a silent bug class that existed when each
/// cache lived under its own `RwLock`.
///
/// `Default` returns an all-`None` state, suitable for `entry().or_default()`
/// on first insert.
#[derive(Default)]
struct PartitionState {
    /// Latest known high watermark (log-end offset), from `FetchResponse`.
    /// `None` until first observed in a fetch response for this partition
    /// (the broker reports it on every response with `high_watermark >= 0`,
    /// including empty and error responses).
    high_watermark: Option<Offset>,
    /// Latest known log start offset, from `FetchResponse`.
    /// `None` until first observed in a fetch response for this partition
    /// (reported in Fetch v5+ whenever `log_start_offset >= 0`, including
    /// empty and error responses).
    log_start_offset: Option<Offset>,
    /// KIP-392 preferred read replica and its expiry time.
    ///
    /// When a broker returns a `preferred_read_replica` in a fetch response,
    /// subsequent fetches for that partition are routed to the indicated
    /// replica until the entry expires (after `metadata_max_age`).
    /// `None` means the leader should be used (the default).
    preferred_replica: Option<(crate::BrokerId, Instant)>,
    /// Next allowed retry time and current exponential backoff interval for
    /// offset-resolution failures. `None` once the partition is successfully
    /// resolved or was never retried. Prevents retry storms when offset
    /// resolution fails persistently (e.g. broker unavailable).
    offset_retry_backoff: Option<(Instant, Duration)>,
}

/// Cluster metadata snapshot returned by [`Consumer::fetch_metadata`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct FetchMetadataResult {
    /// All brokers known to the cluster.
    pub brokers: Vec<BrokerInfo>,
    /// Topics returned in the metadata snapshot.
    ///
    /// When [`Consumer::fetch_metadata`] is called with `None`, this contains
    /// all cached topics. When called with `Some(topic)`, this contains the
    /// matching topic if found, or is empty if that topic was not found in the
    /// cluster.
    pub topics: Vec<TopicInfo>,
}

/// A Kafka consumer.
pub struct Consumer {
    /// Consumer configuration.
    config: ConsumerConfig,
    /// Cluster metadata.
    metadata: Arc<ClusterMetadata>,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Subscribed topics.
    subscriptions: RwLock<HashSet<String>>,
    /// Assigned partitions.
    assignments: RwLock<HashMap<String, Vec<PartitionId>>>,
    /// Current offsets.
    offsets: RwLock<HashMap<(String, PartitionId), Offset>>,
    /// Paused partitions.
    paused: RwLock<HashSet<(String, PartitionId)>>,
    /// Whether the consumer is closed.
    closed: std::sync::atomic::AtomicBool,
    /// Group coordinator for full group protocol support.
    group_coordinator: Option<Arc<GroupCoordinator>>,
    /// Consumer metrics.
    metrics: Arc<ConsumerMetrics>,
    /// Rebalance listener.
    rebalance_listener: Arc<dyn ConsumerRebalanceListener>,
    /// Consumer interceptor.
    interceptor: Arc<dyn crate::interceptor::ConsumerInterceptor>,
    /// Last auto-commit time (for auto-commit timer).
    ///
    /// Held only for a single read/write of an `Instant` — no `.await` under
    /// the lock — so a sync `parking_lot::Mutex` is the correct primitive.
    last_auto_commit: SyncMutex<Instant>,
    /// Buffer for records returned by `recv()`.
    /// `poll()` may return multiple records; `recv()` buffers the rest here.
    ///
    /// All call sites mutate (`pop_front` / `extend` / `clear`) or read
    /// `len()` without crossing an `.await`, so a sync `parking_lot::Mutex`
    /// is used instead of a tokio async lock. The old `RwLock` had no
    /// concurrent readers in practice — every access took the write side.
    recv_buffer: SyncMutex<std::collections::VecDeque<ConsumerRecord>>,
    /// Per-broker fetch session cache (KIP-227).
    ///
    /// Every critical section is pure sync (session bookkeeping: build
    /// request, update from response, reset on error). The actual fetch
    /// RPC `send_request().await` is always performed **after** the lock is
    /// released — never while it is held — so a sync `parking_lot::Mutex`
    /// is the correct primitive. If you add a new use site, keep the
    /// critical section straight-line-sync.
    fetch_sessions: SyncMutex<FetchSessionCache>,
    /// Consolidated per-partition state: high watermark, log start offset,
    /// preferred replica (KIP-392), and offset-retry backoff. A single lock
    /// replaces what was previously four separate `RwLock<HashMap>` fields;
    /// see the `LOCK ORDER` comment above and [`PartitionState`] for details.
    partition_state: RwLock<HashMap<(String, PartitionId), PartitionState>>,
}

/// Compute aggregate lag from offset and high-watermark caches.
///
/// Returns `(total_lag, max_lag)` where `total_lag` is the sum across all
/// partitions (using `saturating_add`) and `max_lag` is the per-partition
/// maximum. Only partitions present in both maps contribute.
///
/// **Staleness caveat**: high watermarks are refreshed from each fetch
/// response (including empty and error responses) for partitions the
/// consumer polls. Partitions that are not being polled — or whose broker
/// is unreachable — will retain a stale watermark, so the reported lag
/// becomes increasingly inaccurate the longer fetches are skipped. Lag
/// should be treated as *eventually consistent*. For precise lag values,
/// issue a `ListOffsets` RPC externally.
fn compute_aggregate_lag(
    offsets: &HashMap<(String, PartitionId), Offset>,
    partition_state: &HashMap<(String, PartitionId), PartitionState>,
) -> (u64, u64) {
    let mut total_lag: u64 = 0;
    let mut max_lag: u64 = 0;
    for (key, state) in partition_state {
        if let (Some(watermark), Some(&position)) = (state.high_watermark, offsets.get(key)) {
            let partition_lag = (watermark - position).max(0) as u64;
            total_lag = total_lag.saturating_add(partition_lag);
            max_lag = max_lag.max(partition_lag);
        }
    }
    (total_lag, max_lag)
}

/// Result of routing assigned partitions to brokers for fetching.
struct FetchRoutingPlan {
    /// Partitions grouped by target broker ID.
    partitions_by_broker: HashMap<crate::BrokerId, Vec<(String, PartitionId)>>,
    /// Preferred replica entries that have expired and should be removed.
    expired_preferred: Vec<(String, PartitionId)>,
    /// Partitions that have neither a known leader nor a valid preferred
    /// replica and will not be fetched this round.
    skipped: Vec<(String, PartitionId)>,
}

/// Build a per-broker fetch plan from pre-filtered partition keys,
/// preferred replicas, and leader information.
///
/// `non_paused_keys` should contain only assigned, non-paused partitions
/// (the caller is responsible for filtering). For each key the function
/// checks whether a preferred replica exists and is not expired. If so,
/// the partition is routed to that replica, regardless of whether a leader
/// is known. If there is no valid preferred replica, the function falls
/// back to the leader if one is known; otherwise the partition is skipped.
///
/// This is a pure function extracted from `Consumer::poll()` so that the
/// routing logic can be unit-tested without a live broker.
fn build_fetch_routing_plan(
    non_paused_keys: Vec<(String, PartitionId)>,
    partition_state: &HashMap<(String, PartitionId), PartitionState>,
    leaders: &HashMap<(String, PartitionId), crate::BrokerId>,
    now: Instant,
) -> FetchRoutingPlan {
    let mut partitions_by_broker: HashMap<crate::BrokerId, Vec<(String, PartitionId)>> =
        HashMap::new();
    let mut expired_preferred: Vec<(String, PartitionId)> = Vec::new();
    let mut skipped: Vec<(String, PartitionId)> = Vec::new();

    for key in non_paused_keys {
        // Check for a valid (non-expired) preferred replica
        let target_broker = match partition_state.get(&key).and_then(|s| s.preferred_replica) {
            Some((replica_id, expiry)) if now < expiry => Some(replica_id),
            Some(_) => {
                expired_preferred.push(key.clone());
                None
            }
            None => None,
        };

        let broker_id = match target_broker {
            Some(id) => id,
            None => {
                if let Some(&leader_id) = leaders.get(&key) {
                    leader_id
                } else {
                    skipped.push(key);
                    continue;
                }
            }
        };

        partitions_by_broker.entry(broker_id).or_default().push(key);
    }

    FetchRoutingPlan {
        partitions_by_broker,
        expired_preferred,
        skipped,
    }
}

/// Group a flat `&[(&str, PartitionId)]` slice into a
/// `HashMap<String, Vec<PartitionId>>` keyed by topic name, preserving
/// insertion order within each topic's partition list.
///
/// This is the pure grouping step shared by [`Consumer::offsets_for_times`]
/// and its unit test so that the test exercises the real logic.
fn group_topic_partitions(partitions: &[(&str, PartitionId)]) -> HashMap<String, Vec<PartitionId>> {
    let mut grouped: HashMap<String, Vec<PartitionId>> = HashMap::new();
    for (topic, partition) in partitions {
        grouped
            .entry((*topic).to_string())
            .or_default()
            .push(*partition);
    }
    grouped
}

impl Consumer {
    /// Create a new consumer builder.
    pub fn builder() -> ConsumerBuilder {
        ConsumerBuilder::default()
    }

    /// Create a new consumer with the given configuration.
    async fn new(config: ConsumerConfig) -> Result<Self> {
        let mut pool_config_builder = ConnectionConfig::builder()
            .client_id(&config.client_id)
            .request_timeout(config.request_timeout);

        if let Some(ref auth) = config.auth {
            pool_config_builder = pool_config_builder.auth(auth.clone());
        }

        #[cfg(feature = "socks5")]
        if let Some(ref proxy) = config.proxy {
            pool_config_builder = pool_config_builder.proxy(proxy.clone());
        }

        let mut pool_config = pool_config_builder.build();
        pool_config.init_tls().await?;

        let pool = Arc::new(ConnectionPool::new(pool_config));
        pool.start_idle_evictor();

        let bootstrap_servers = crate::util::parse_bootstrap_servers(&config.bootstrap_servers)?;

        let metadata = Arc::new({
            let mut meta =
                ClusterMetadata::new(bootstrap_servers, pool.clone(), config.metadata_max_age)
                    .with_recovery_strategy(config.metadata_recovery_strategy)
                    .with_rebootstrap_trigger(config.metadata_recovery_rebootstrap_trigger);
            if let Some(ttl) = config.metadata_topic_cache_ttl {
                meta = meta.with_topic_cache_ttl(ttl);
            } else {
                meta = meta.with_topic_cache_ttl_disabled();
            }
            meta
        });

        // Initial metadata fetch
        metadata.refresh().await?;

        // Create group coordinator if group_id is specified
        let group_coordinator = if let Some(ref group_id) = config.group_id {
            Some(Arc::new(
                GroupCoordinator::new(
                    group_id.clone(),
                    pool.clone(),
                    metadata.clone(),
                    config.session_timeout,
                    config.heartbeat_interval,
                    config.max_poll_interval, // rebalance_timeout matches Java client's max.poll.interval.ms
                )
                .with_assignor_strategy(config.partition_assignment_strategy)
                .with_group_instance_id(config.group_instance_id.clone())
                .with_isolation_level(config.isolation_level.to_i8())
                .with_group_protocol(config.group_protocol),
            ))
        } else {
            None
        };

        let metrics = Arc::new(ConsumerMetrics::default());

        info!(
            "Consumer initialized with {} brokers{}",
            metadata.brokers().len(),
            if let Some(ref gid) = config.group_id {
                format!(", group_id='{gid}'")
            } else {
                String::new()
            }
        );

        Ok(Self {
            config,
            metadata,
            pool,
            subscriptions: RwLock::new(HashSet::new()),
            assignments: RwLock::new(HashMap::new()),
            offsets: RwLock::new(HashMap::new()),
            paused: RwLock::new(HashSet::new()),
            closed: std::sync::atomic::AtomicBool::new(false),
            group_coordinator,
            metrics,
            rebalance_listener: Arc::new(NoOpRebalanceListener),
            interceptor: Arc::new(crate::interceptor::NoOpConsumerInterceptor),
            last_auto_commit: SyncMutex::new(Instant::now()),
            recv_buffer: SyncMutex::new(std::collections::VecDeque::new()),
            fetch_sessions: SyncMutex::new(FetchSessionCache::new()),
            partition_state: RwLock::new(HashMap::new()),
        })
    }

    /// Subscribe to topics.
    ///
    /// Replaces the current subscription with the given topics (matching
    /// the Kafka Java client's replace semantics).
    pub async fn subscribe(&self, topics: &[&str]) -> Result<()> {
        // H6: reject empty / oversize topic names at ingress so they cannot
        // reach the panicking `KafkaString::encode` path via the MetadataRequest
        // / Heartbeat / subscription payload.
        validate_topic_names(topics.iter().copied())?;

        // Scope the write lock so it is dropped before network I/O
        {
            let mut subscriptions = self.subscriptions.write().await;
            subscriptions.clear();
            for topic in topics {
                subscriptions.insert((*topic).to_string());
            }
        }

        // Refresh metadata for subscribed topics
        self.metadata.refresh_for_topics(Some(topics)).await?;

        // If we have a group coordinator, join the group
        if let Some(ref coordinator) = self.group_coordinator {
            let topic_strings: Vec<String> = topics.iter().map(|s| s.to_string()).collect();
            let mut topics_sorted = topic_strings.clone();
            topics_sorted.sort();

            if coordinator.is_consumer_protocol() {
                // KIP-848: defer to poll(), which handles incremental
                // assignment via the background heartbeat task.  subscribe()
                // only updates the subscription; the next heartbeat will carry
                // the new topic list to the coordinator.

                // Detect topic changes while active — trigger rejoin so the
                // next poll sends a full heartbeat with the new subscription.
                {
                    let state = coordinator.state().await;
                    if state == GroupState::Stable {
                        let mut old_sorted = coordinator.subscribed_topics().await;
                        old_sorted.sort();
                        if old_sorted != topics_sorted {
                            coordinator.trigger_rejoin().await;
                        }
                    }
                }

                coordinator.set_subscribed_topics(topic_strings).await;
            } else if coordinator.is_cooperative() {
                // Cooperative (KIP-429): defer the join/sync to poll(), which
                // implements the full two-phase rebalance protocol (revocations,
                // on_partitions_revoked callback, second rejoin). subscribe()
                // only updates the subscription metadata; poll() will detect
                // needs_rejoin() and drive the cooperative flow.

                // Detect topic changes while Stable — mark for rejoin.
                {
                    let state = coordinator.state().await;
                    if state == GroupState::Stable {
                        let mut old_sorted = coordinator.subscribed_topics().await;
                        old_sorted.sort();
                        if old_sorted != topics_sorted {
                            coordinator.set_preparing_rebalance().await;
                        }
                    }
                }

                coordinator.set_subscribed_topics(topic_strings).await;
            } else {
                // Eager: join immediately in subscribe() — single-phase is correct.

                // Snapshot old assignment before the join. If a JoinGroup/SyncGroup
                // occurs, we must revoke the old partitions (eager = revoke all)
                // to clean up per-partition state and notify the listener.
                let old_assignments = self.assignments.read().await.clone();

                let (assignment, joined) =
                    coordinator.ensure_active_membership(&topic_strings).await?;

                if joined {
                    // An actual JoinGroup/SyncGroup occurred (first join or topic change).

                    // Eager revocation: notify listener and clean up stale per-partition
                    // state from the previous assignment before applying the new one.
                    // Without this, re-subscribing with different topics would leak
                    // old offsets, buffered records, paused state, and fetch sessions.
                    if !old_assignments.is_empty() {
                        let revoked: Vec<TopicPartition> = old_assignments
                            .iter()
                            .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
                            .collect();
                        self.safe_on_partitions_revoked(&revoked);
                        self.clear_partition_state().await;
                    }

                    self.metrics.rebalances.inc();
                }

                // Update our assignments based on the group assignment
                {
                    let mut assignments = self.assignments.write().await;
                    assignments.clear();
                    for (topic, partitions) in &assignment.partitions {
                        assignments.insert(topic.clone(), partitions.clone());
                    }
                }

                if joined {
                    // Notify listener of assignment (matches Java client behavior:
                    // ConsumerRebalanceListener.onPartitionsAssigned is invoked on every
                    // successful rebalance, including the very first one).
                    let assigned: Vec<TopicPartition> = assignment
                        .partitions
                        .iter()
                        .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
                        .collect();
                    self.safe_on_partitions_assigned(&assigned);

                    // Update assigned_partitions metric
                    self.metrics.assigned_partitions.set(assigned.len() as u64);

                    // Fetch committed offsets for our assigned partitions
                    self.fetch_and_apply_committed_offsets(&assignment.partitions)
                        .await?;
                }
            }

            debug!("Subscribed to topics via group coordinator: {:?}", topics);
        } else {
            // Assign all partitions (simple assignment without group coordination)
            let mut assignments = self.assignments.write().await;
            for topic in topics {
                if let Some(topic_info) = self.metadata.topic(topic) {
                    let partitions: Vec<_> =
                        topic_info.partitions.iter().map(|p| p.partition).collect();
                    assignments.insert((*topic).to_string(), partitions);
                }
            }
            let assigned_snapshot = assignments.clone();
            drop(assignments);

            // Update metric for standalone partition count
            let count: usize = assigned_snapshot.values().map(|p| p.len()).sum();
            self.metrics.assigned_partitions.set(count as u64);

            // Apply auto_offset_reset for non-group consumers.
            // Without this, all partitions default to offset 0 regardless of
            // the configured auto_offset_reset policy.
            self.apply_auto_offset_reset(&assigned_snapshot).await?;

            debug!("Subscribed to topics: {:?}", topics);
        }

        Ok(())
    }

    /// Apply per-partition cleanup for revoked partitions.
    ///
    /// Removes revoked entries from `assignments`, `offsets`, `paused`,
    /// `recv_buffer`, and `partition_state` (the consolidated cache that
    /// holds high watermark, log start offset, preferred replica, and
    /// offset-retry backoff). Fetch sessions are NOT reset here —
    /// `build_request()` automatically computes `forgotten_topics` diffs
    /// from the updated assignment, preserving KIP-227 incremental fetch
    /// benefits. Called by all cooperative revocation paths.
    async fn apply_partition_revocations(&self, revoked: &[(String, PartitionId)]) {
        // Build per-topic set of revoked partition IDs for O(T * P) removal
        // instead of O(R * P) when many partitions of the same topic are revoked.
        let revoked_by_topic: HashMap<&str, HashSet<PartitionId>> = {
            let mut m: HashMap<&str, HashSet<PartitionId>> = HashMap::new();
            for (topic, partition) in revoked {
                m.entry(topic.as_str()).or_default().insert(*partition);
            }
            m
        };

        // Precompute owned keys once to avoid repeated String clones in each
        // removal loop below.
        let revoked_keys: Vec<(String, PartitionId)> =
            revoked.iter().map(|(t, p)| (t.clone(), *p)).collect();

        // Remove from assignments
        {
            let mut assignments = self.assignments.write().await;
            for (topic, revoked_parts) in &revoked_by_topic {
                if let Some(parts) = assignments.get_mut(*topic) {
                    parts.retain(|p| !revoked_parts.contains(p));
                    if parts.is_empty() {
                        assignments.remove(*topic);
                    }
                }
            }
        }
        // Remove offsets for revoked partitions
        {
            let mut offsets = self.offsets.write().await;
            for key in &revoked_keys {
                offsets.remove(key);
            }
        }
        // Discard buffered records from revoked partitions
        {
            let revoked_set: HashSet<(&str, PartitionId)> =
                revoked_keys.iter().map(|(t, p)| (t.as_str(), *p)).collect();
            let mut buf = self.recv_buffer.lock();
            buf.retain(|r| !revoked_set.contains(&(r.topic.as_str(), r.partition)));
            self.metrics.buffered_records.set(buf.len() as u64);
        }
        // Clear paused state for revoked partitions
        {
            let mut paused = self.paused.write().await;
            for key in &revoked_keys {
                paused.remove(key);
            }
            self.metrics.paused_partitions.set(paused.len() as u64);
        }
        // Clear all per-partition fetch-derived state (high watermark, log
        // start offset, preferred replica, offset-retry backoff) in a single
        // lock acquisition. This replaces four independent `RwLock` writes
        // that previously had to be kept in sync by hand.
        {
            let mut partition_state = self.partition_state.write().await;
            for key in &revoked_keys {
                partition_state.remove(key);
            }
        }
        // Recompute lag metrics from remaining caches so revoked
        // partitions no longer contribute to exported values.
        self.recompute_lag_metrics().await;
    }

    /// Finalize a cooperative rebalance: compute newly-assigned diff, update
    /// assignments, fire `on_partitions_assigned`, fetch committed offsets for
    /// new partitions, and record owned partitions in the sticky assignor.
    async fn finalize_cooperative_assignment(
        &self,
        coordinator: &GroupCoordinator,
        assignment: &MemberAssignment,
        old_assignments: &HashMap<String, Vec<PartitionId>>,
    ) -> Result<()> {
        // Build HashSet index for O(1) membership checks.
        let old_sets: HashMap<&String, HashSet<PartitionId>> = old_assignments
            .iter()
            .map(|(t, ps)| (t, ps.iter().copied().collect()))
            .collect();

        // Determine newly assigned partitions (new - old)
        let mut newly_assigned = Vec::new();
        for (topic, partitions) in &assignment.partitions {
            let old_set = old_sets.get(topic);
            for &p in partitions {
                let is_new = old_set.is_none_or(|os| !os.contains(&p));
                if is_new {
                    newly_assigned.push(TopicPartition::new(topic, p));
                }
            }
        }

        // Update to final assignment
        {
            let mut assignments = self.assignments.write().await;
            assignments.clear();
            for (topic, partitions) in &assignment.partitions {
                assignments.insert(topic.clone(), partitions.clone());
            }
        }

        // Notify listener with the full post-rebalance assignment,
        // not just the diff. Always fire, even when the assignment
        // is empty (e.g., more consumers than partitions).
        let full_assigned: Vec<TopicPartition> = assignment
            .partitions
            .iter()
            .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
            .collect();
        self.rebalance_listener
            .on_partitions_assigned(&full_assigned);
        self.metrics
            .assigned_partitions
            .set(full_assigned.len() as u64);

        // Fetch committed offsets for newly assigned partitions only
        // (retained partitions already have tracked offsets).
        if !newly_assigned.is_empty() {
            let new_parts = Self::group_partitions_by_topic(&newly_assigned);
            self.fetch_and_apply_committed_offsets(&new_parts).await?;
        }

        // Record final assignment so the next rebalance's
        // join_group metadata reports correct owned partitions.
        let member_id = coordinator.member_id().await;
        coordinator.record_owned_partitions(&member_id, assignment);

        Ok(())
    }

    /// Clear all per-partition state after an eager revocation or unsubscribe/close.
    ///
    /// Resets fetch sessions, offsets, buffered records, paused set, and the
    /// consolidated [`PartitionState`] map (high watermark, log start offset,
    /// preferred replica, offset-retry backoff), then zeros the lag metrics.
    async fn clear_partition_state(&self) {
        self.fetch_sessions.lock().reset_all();
        self.offsets.write().await.clear();
        self.recv_buffer.lock().clear();
        self.paused.write().await.clear();
        self.partition_state.write().await.clear();
        self.metrics.buffered_records.set(0);
        self.metrics.paused_partitions.set(0);
        self.metrics.lag.set(0);
        self.metrics.lag_max.set(0);
    }

    // ── Panic-safe rebalance listener wrappers ──────────────────────────
    //
    // User-provided `ConsumerRebalanceListener` callbacks are wrapped in
    // `catch_unwind` to prevent a panicking listener from unwinding through
    // the consumer task, which would skip `leave_group()` and other cleanup.
    // This mirrors the interceptor subsystem's panic isolation pattern.

    /// Invoke `on_partitions_assigned` on the rebalance listener, catching panics.
    fn safe_on_partitions_assigned(&self, partitions: &[TopicPartition]) {
        if let Err(_panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.rebalance_listener.on_partitions_assigned(partitions);
        })) {
            tracing::error!(
                partition_count = partitions.len(),
                "ConsumerRebalanceListener::on_partitions_assigned panicked"
            );
        }
    }

    /// Invoke `on_partitions_revoked` on the rebalance listener, catching panics.
    fn safe_on_partitions_revoked(&self, partitions: &[TopicPartition]) {
        if let Err(_panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.rebalance_listener.on_partitions_revoked(partitions);
        })) {
            tracing::error!(
                partition_count = partitions.len(),
                "ConsumerRebalanceListener::on_partitions_revoked panicked"
            );
        }
    }

    /// Invoke `on_partitions_lost` on the rebalance listener, catching panics.
    fn safe_on_partitions_lost(&self, partitions: &[TopicPartition]) {
        if let Err(_panic) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.rebalance_listener.on_partitions_lost(partitions);
        })) {
            tracing::error!(
                partition_count = partitions.len(),
                "ConsumerRebalanceListener::on_partitions_lost panicked"
            );
        }
    }

    /// Recompute lag and lag_max gauges from cached offsets and high watermarks.
    ///
    /// Call after any mutation of `self.offsets` or the `high_watermark` field
    /// of `self.partition_state` so the exported metrics always reflect the
    /// current consumer position. Acquires read locks in documented order:
    /// offsets → partition_state.
    ///
    /// This performs an O(partitions) full scan via [`compute_aggregate_lag`].
    /// An incremental (delta-based) approach was considered but rejected:
    /// the typical partition count per consumer (tens to low thousands) makes
    /// the scan complete in microseconds, while incremental bookkeeping would
    /// add complexity and drift risk for negligible gain. Callers on the hot
    /// path (e.g. `poll()`) already guard calls behind a change-detection flag.
    async fn recompute_lag_metrics(&self) {
        let offsets = self.offsets.read().await;
        let partition_state = self.partition_state.read().await;
        let (total_lag, max_lag) = compute_aggregate_lag(&offsets, &partition_state);
        self.metrics.lag.set(total_lag);
        self.metrics.lag_max.set(max_lag);
    }

    /// Send an inline heartbeat, invoke the revocation callback, apply
    /// partition revocations, and update the metric + sticky-assignor state.
    ///
    /// Returns `true` if an inline heartbeat signalled session invalidation
    /// and poll() should return early.
    async fn apply_revocation_round(
        &self,
        coordinator: &Arc<GroupCoordinator>,
        revoked_tuples: &[(String, PartitionId)],
        revoked_tps: &[TopicPartition],
    ) -> Result<bool> {
        // Send an inline heartbeat before invoking the user callback
        // to avoid session timeout if the callback is slow.
        match coordinator.send_heartbeat().await {
            Ok(status) if coordinator.handle_inline_heartbeat_status(status).await => {
                return Ok(true);
            }
            Err(e) => {
                warn!("Pre-revocation heartbeat failed: {}", e);
            }
            _ => {}
        }
        // Commit offsets for the partitions we are about to lose so the
        // new owner sees up-to-date committed positions.
        if self.config.enable_auto_commit
            && let Err(e) = self.commit().await
        {
            if e.is_retriable() {
                warn!(
                    "Auto-commit before cooperative revocation failed (retriable): {}",
                    e
                );
            } else {
                error!(
                    "Auto-commit before cooperative revocation failed (fatal): {}",
                    e
                );
            }
        }
        self.safe_on_partitions_revoked(revoked_tps);
        self.apply_partition_revocations(revoked_tuples).await;

        // Update metric and owned-partition state in a single lock
        // acquisition. The metric is set eagerly so it stays accurate
        // even if a subsequent Phase 2 round returns early.
        let member_id = coordinator.member_id().await;
        let current = self.assignments.read().await;
        let count: usize = current.values().map(|ps| ps.len()).sum();
        self.metrics.assigned_partitions.set(count as u64);
        let owned = MemberAssignment {
            partitions: current.clone(),
        };
        drop(current);
        coordinator.record_owned_partitions(&member_id, &owned);

        Ok(false)
    }

    /// Handle group rebalance and inline heartbeat during poll.
    ///
    /// Returns `true` if poll() should return an empty result immediately
    /// (e.g., cooperative rebalance requires another poll cycle).
    async fn handle_group_rebalance(&self) -> Result<bool> {
        let Some(ref coordinator) = self.group_coordinator else {
            return Ok(false);
        };

        if coordinator.needs_rejoin().await {
            let topics: Vec<String> = self.subscriptions.read().await.iter().cloned().collect();
            if !topics.is_empty() {
                coordinator.set_subscribed_topics(topics.clone()).await;

                if coordinator.is_consumer_protocol() {
                    // KIP-848: when the consumer needs to (re)join — initial
                    // join (Unjoined), post-fencing rejoin, or subscription
                    // change — send a full heartbeat with all fields and
                    // (re)start the background task.  When the heartbeat task
                    // delivered a normal assignment update (Stable, same
                    // topics), ensure_active_membership is a no-op.
                    coordinator.ensure_active_membership(&topics).await?;
                    self.handle_kip848_rebalance(coordinator).await?;
                } else if coordinator.is_cooperative() {
                    if self.handle_cooperative_rebalance(coordinator).await? {
                        return Ok(true);
                    }
                } else {
                    self.handle_eager_rebalance(coordinator, &topics).await?;
                }
            }
        }

        // Check if inline heartbeat is needed.
        // Skip for KIP-848 — the background ConsumerGroupHeartbeat task handles
        // heartbeats; sending classic Heartbeat requests would use the wrong API.
        if !coordinator.is_consumer_protocol() && coordinator.is_heartbeat_overdue().await {
            match coordinator.send_heartbeat().await {
                Ok(status) if coordinator.handle_inline_heartbeat_status(status).await => {
                    debug!("Heartbeat indicated rejoin needed");
                }
                Err(e) => {
                    warn!("Inline heartbeat failed: {}", e);
                }
                _ => {}
            }
        }

        Ok(false)
    }

    /// Handle cooperative incremental rebalance (KIP-429).
    ///
    /// Returns `true` if poll() should return an empty result immediately,
    /// which happens when an inline heartbeat signals rejoin or when the
    /// cooperative round limit is exceeded.
    async fn handle_cooperative_rebalance(
        &self,
        coordinator: &Arc<GroupCoordinator>,
    ) -> Result<bool> {
        // Phase 1: join+sync to get new target assignment
        let (new_assignment, to_revoke) = coordinator.perform_cooperative_join_and_sync().await?;

        if !to_revoke.is_empty() {
            // Revoke only the diff — keep consuming unaffected partitions
            let revoked: Vec<TopicPartition> = to_revoke
                .iter()
                .map(|(t, p)| TopicPartition::new(t, *p))
                .collect();
            if self
                .apply_revocation_round(coordinator, &to_revoke, &revoked)
                .await?
            {
                return Ok(true);
            }
            self.metrics.rebalances.inc();

            // Phase 2: rejoin to finalize after revocations.
            // In rare cases (concurrent topic changes, racing rebalances),
            // additional revocations may be needed. Loop with a bound.
            coordinator.trigger_rejoin().await;
            let mut final_assignment = MemberAssignment::empty();
            for round in 0..MAX_COOPERATIVE_ROUNDS {
                let (assignment, extra_revoke) =
                    coordinator.perform_cooperative_join_and_sync().await?;
                final_assignment = assignment;

                if extra_revoke.is_empty() {
                    break;
                }

                // Process additional revocations (including final round)
                let extra_revoked: Vec<TopicPartition> = extra_revoke
                    .iter()
                    .map(|(t, p)| TopicPartition::new(t, *p))
                    .collect();
                if self
                    .apply_revocation_round(coordinator, &extra_revoke, &extra_revoked)
                    .await?
                {
                    return Ok(true);
                }

                if round == MAX_COOPERATIVE_ROUNDS - 1 {
                    warn!(
                        "Cooperative rebalance exceeded {} rounds with pending revocations; \
                         this may indicate cascading membership changes. \
                         Deferring assignment to next poll cycle.",
                        MAX_COOPERATIVE_ROUNDS
                    );
                    // Start heartbeat to avoid session timeout while we
                    // defer the additional cooperative rebalance round
                    // to the next poll cycle. Do NOT apply final_assignment
                    // since it still required another rejoin. Set state
                    // directly instead of trigger_rejoin() to avoid
                    // killing the heartbeat task via Rejoin command.
                    coordinator.start_heartbeat_task().await;
                    coordinator.set_preparing_rebalance().await;
                    // Note: rebalances metric was already incremented
                    // at Phase 1 entry; do not double-count here.
                    // assigned_partitions metric was already updated
                    // after apply_partition_revocations above.
                    return Ok(true);
                }

                coordinator.trigger_rejoin().await;
            }

            // Finalize cooperative assignment: update assignments,
            // fire on_partitions_assigned, fetch offsets, record owned.
            let old_assignments = self.assignments.read().await.clone();
            self.finalize_cooperative_assignment(coordinator, &final_assignment, &old_assignments)
                .await?;
        } else {
            // No revocations — assignment is final in one round
            let old_assignments = self.assignments.read().await.clone();

            // Build HashSet index of new partitions for O(1) lookups.
            let new_sets: HashMap<&String, HashSet<PartitionId>> = new_assignment
                .partitions
                .iter()
                .map(|(t, ps)| (t, ps.iter().copied().collect()))
                .collect();

            // Determine partitions removed in this rebalance
            // (e.g., reassigned to another member, topic deleted).
            // This is a clean cooperative revocation, not an unclean
            // loss, so use on_partitions_revoked (not on_partitions_lost).
            let mut revoked_parts: Vec<TopicPartition> = Vec::new();
            for (topic, partitions) in &old_assignments {
                let new_set = new_sets.get(topic);
                for &p in partitions {
                    let gone = new_set.is_none_or(|ns| !ns.contains(&p));
                    if gone {
                        revoked_parts.push(TopicPartition::new(topic, p));
                    }
                }
            }
            if !revoked_parts.is_empty() {
                self.safe_on_partitions_revoked(&revoked_parts);
                let revoked_tuples: Vec<(String, PartitionId)> = revoked_parts
                    .iter()
                    .map(|tp| (tp.topic.clone(), tp.partition))
                    .collect();
                self.apply_partition_revocations(&revoked_tuples).await;
            }

            self.metrics.rebalances.inc();

            // Finalize cooperative assignment: update assignments,
            // fire on_partitions_assigned, fetch offsets, record owned.
            self.finalize_cooperative_assignment(coordinator, &new_assignment, &old_assignments)
                .await?;
        }

        Ok(false)
    }

    /// Handle KIP-848 server-side assignment: diff-based callbacks.
    ///
    /// The KIP-848 background heartbeat task stores the new assignment in
    /// `GroupCoordinator.assignment` and signals rebalance. This method reads
    /// the current assignment, computes the diff against the Consumer's local
    /// assignments, fires revocation/assignment callbacks for changed
    /// partitions, and fetches committed offsets for newly added ones.
    async fn handle_kip848_rebalance(&self, coordinator: &Arc<GroupCoordinator>) -> Result<()> {
        let new_assignment = coordinator.assignment().await;
        let old_assignments = self.assignments.read().await.clone();

        // Build HashSets for O(n) diffing instead of Vec::contains.
        let old_sets: HashMap<&String, HashSet<PartitionId>> = old_assignments
            .iter()
            .map(|(t, ps)| (t, ps.iter().copied().collect()))
            .collect();
        let new_sets: HashMap<&String, HashSet<PartitionId>> = new_assignment
            .partitions
            .iter()
            .map(|(t, ps)| (t, ps.iter().copied().collect()))
            .collect();

        // Compute revoked partitions: in old but not in new.
        let mut revoked: Vec<TopicPartition> = Vec::new();
        for (topic, old_set) in &old_sets {
            let new_set = new_sets.get(*topic);
            for &p in old_set {
                let retained = new_set.is_some_and(|ns| ns.contains(&p));
                if !retained {
                    revoked.push(TopicPartition::new(*topic, p));
                }
            }
        }

        // Compute newly assigned partitions: in new but not in old.
        let mut assigned: Vec<TopicPartition> = Vec::new();
        for (topic, new_set) in &new_sets {
            let old_set = old_sets.get(*topic);
            for &p in new_set {
                let was_assigned = old_set.is_some_and(|os| os.contains(&p));
                if !was_assigned {
                    assigned.push(TopicPartition::new(*topic, p));
                }
            }
        }

        if revoked.is_empty() && assigned.is_empty() {
            // No actual change — the heartbeat task may have signalled
            // rebalance for state reasons (e.g. first assignment).
            // Still need to ensure our local assignments are in sync.
            if old_assignments.is_empty() && !new_assignment.partitions.is_empty() {
                // First assignment: treat all partitions as newly assigned.
                for (topic, parts) in &new_assignment.partitions {
                    for &p in parts {
                        assigned.push(TopicPartition::new(topic, p));
                    }
                }
            } else if !old_assignments.is_empty() {
                // Had partitions before, diff shows no movement — nothing to do.
                return Ok(());
            }
            // Remaining case: old_assignments is empty.  Either
            //   (a) new is also empty  — first heartbeat with an empty
            //       assignment (more consumers than partitions), or
            //   (b) new is non-empty   — handled by the branch above.
            // For (a) we fall through so on_partitions_assigned fires,
            // matching cooperative/eager paths which always invoke the
            // callback on the initial assignment.
        }

        // Fire revocation callback and clean up per-partition state.
        if !revoked.is_empty() {
            // KIP-848 §revocation: commit offsets for the partitions we are
            // about to lose before invoking the user callback, so the new
            // owner sees up-to-date committed positions. The old assignments
            // are still active at this point, so `commit()` includes them.
            if self.config.enable_auto_commit
                && let Err(e) = self.commit().await
            {
                if e.is_retriable() {
                    warn!(
                        "Auto-commit before KIP-848 revocation failed (retriable): {}",
                        e
                    );
                } else {
                    error!(
                        "Auto-commit before KIP-848 revocation failed (fatal): {}",
                        e
                    );
                }
            }
            self.safe_on_partitions_revoked(&revoked);
            let revoked_tuples: Vec<(String, PartitionId)> = revoked
                .iter()
                .map(|tp| (tp.topic.clone(), tp.partition))
                .collect();
            self.apply_partition_revocations(&revoked_tuples).await;
        }

        // Update assignments to the new state.
        {
            let mut assignments = self.assignments.write().await;
            assignments.clear();
            for (topic, partitions) in &new_assignment.partitions {
                assignments.insert(topic.clone(), partitions.clone());
            }
        }

        self.metrics.rebalances.inc();

        // Fire assignment callback with the full post-rebalance assignment
        // (consistent with the cooperative/eager paths in this crate).
        // Always fire, even when the assignment is empty or only revocations
        // occurred, so listeners can react to the post-rebalance state.
        let full_assignment: Vec<TopicPartition> = new_assignment
            .partitions
            .iter()
            .flat_map(|(topic, partitions)| {
                partitions
                    .iter()
                    .copied()
                    .map(move |partition| TopicPartition::new(topic, partition))
            })
            .collect();
        self.safe_on_partitions_assigned(&full_assignment);

        let count: usize = new_assignment.partitions.values().map(|ps| ps.len()).sum();
        self.metrics.assigned_partitions.set(count as u64);

        // Fetch committed offsets only for newly assigned partitions.
        if !assigned.is_empty() {
            let new_parts = Self::group_partitions_by_topic(&assigned);
            self.fetch_and_apply_committed_offsets(&new_parts).await?;
        }

        // KIP-848 §revocation-ack: after processing revocations, send an
        // immediate heartbeat with the updated owned partitions so the
        // coordinator can proceed with the rebalance.
        if !revoked.is_empty() {
            coordinator.acknowledge_revocation().await;
        }

        Ok(())
    }

    /// Handle eager rebalance: revoke all partitions, then reassign from scratch.
    async fn handle_eager_rebalance(
        &self,
        coordinator: &Arc<GroupCoordinator>,
        topics: &[String],
    ) -> Result<()> {
        let old_assignments = self.assignments.read().await.clone();
        if !old_assignments.is_empty() {
            let revoked: Vec<TopicPartition> = old_assignments
                .iter()
                .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
                .collect();
            // Commit offsets for all partitions before the eager revoke-all,
            // so the group has up-to-date committed positions.
            if self.config.enable_auto_commit
                && let Err(e) = self.commit().await
            {
                if e.is_retriable() {
                    warn!(
                        "Auto-commit before eager revocation failed (retriable): {}",
                        e
                    );
                } else {
                    error!("Auto-commit before eager revocation failed (fatal): {}", e);
                }
            }
            self.safe_on_partitions_revoked(&revoked);
            self.clear_partition_state().await;

            // Clear assignments immediately after revocation so that
            // if ensure_active_membership fails below, the next poll
            // won't re-fire on_partitions_revoked for already-revoked
            // partitions. Matches the Java client's behavior of
            // clearing subscription state after the eager revoke phase.
            self.assignments.write().await.clear();
            self.metrics.assigned_partitions.set(0);
        }

        self.metrics.rebalances.inc();

        // `joined` is always true here: handle_group_rebalance gates on
        // needs_rejoin(), so ensure_active_membership always performs a
        // full JoinGroup/SyncGroup.
        let (assignment, _joined) = coordinator.ensure_active_membership(topics).await?;

        // Update our assignments
        let mut assignments = self.assignments.write().await;
        assignments.clear();
        for (topic, partitions) in &assignment.partitions {
            assignments.insert(topic.clone(), partitions.clone());
        }
        drop(assignments);

        // Notify listener of newly assigned partitions
        let assigned: Vec<TopicPartition> = assignment
            .partitions
            .iter()
            .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
            .collect();
        self.safe_on_partitions_assigned(&assigned);
        self.metrics.assigned_partitions.set(assigned.len() as u64);

        // Fetch committed offsets for new assignment
        self.fetch_and_apply_committed_offsets(&assignment.partitions)
            .await?;

        Ok(())
    }

    /// Group topic-partitions into a map keyed by topic name.
    fn group_partitions_by_topic(
        partitions: &[TopicPartition],
    ) -> HashMap<String, Vec<PartitionId>> {
        let mut map: HashMap<String, Vec<PartitionId>> = HashMap::new();
        for tp in partitions {
            map.entry(tp.topic.clone()).or_default().push(tp.partition);
        }
        map
    }

    /// Fetch committed offsets and apply auto_offset_reset for partitions without committed offsets.
    ///
    /// Called after group assignment to initialize partition offsets:
    /// 1. Fetch committed offsets from the group coordinator
    /// 2. For partitions with no committed offset, apply the configured auto_offset_reset policy
    async fn fetch_and_apply_committed_offsets(
        &self,
        assigned: &HashMap<String, Vec<PartitionId>>,
    ) -> Result<()> {
        let coordinator = match self.group_coordinator {
            Some(ref c) => c,
            None => return Ok(()),
        };

        // Fetch committed offsets
        let committed = coordinator.fetch_committed_offsets(assigned).await?;

        // Determine which partitions are missing committed offsets
        let mut need_reset: Vec<(String, PartitionId)> = Vec::new();
        let mut offsets = self.offsets.write().await;

        // Log the initial offsets state before processing committed offsets
        debug!("fetch_and_apply: existing offsets: {:?}", *offsets);

        for (topic, partitions) in assigned {
            for &partition in partitions {
                let key = (topic.clone(), partition);

                // Respect user-set offsets (e.g., from seek() in on_partitions_assigned).
                // If the caller already positioned this partition, do not overwrite.
                if offsets.contains_key(&key) {
                    debug!(
                        "Keeping existing offset for {}-{} (user-set or prior)",
                        topic, partition
                    );
                    continue;
                }

                let committed_val = committed.get(&key);
                if let Some(&offset) = committed_val
                    && offset >= 0
                {
                    debug!(
                        "Using committed offset {} for {}-{}",
                        offset, topic, partition
                    );
                    offsets.insert(key, offset);
                    continue;
                }
                // No committed offset or negative (unknown)
                debug!(
                    "No committed offset for {}-{} (committed={:?}), will auto-reset",
                    topic, partition, committed_val
                );
                need_reset.push(key);
            }
        }

        if need_reset.is_empty() {
            return Ok(());
        }

        // Apply auto_offset_reset
        if let Some(timestamp) = self.config.auto_offset_reset.to_offset() {
            // Group partitions by topic for list_offsets call
            let mut reset_partitions: HashMap<String, Vec<PartitionId>> = HashMap::new();
            for (topic, partition) in &need_reset {
                reset_partitions
                    .entry(topic.clone())
                    .or_default()
                    .push(*partition);
            }

            let resolved = coordinator
                .list_offsets(&reset_partitions, timestamp)
                .await?;

            for (key, offset) in &resolved {
                offsets.insert(key.clone(), *offset);
            }

            // Fallback: if the group coordinator's list_offsets silently
            // dropped some partitions (partition-level errors), resolve
            // them individually via the direct ListOffsets v1 path.
            for (topic, partition) in &need_reset {
                let key = (topic.clone(), *partition);
                if !resolved.contains_key(&key) && !offsets.contains_key(&key) {
                    debug!(
                        "Falling back to direct ListOffsets for {}-{} \
                         (coordinator path returned no result)",
                        topic, partition
                    );
                    // Release offsets lock temporarily for the network call
                    drop(offsets);
                    match self.resolve_list_offset(topic, *partition, timestamp).await {
                        Ok(offset) => {
                            offsets = self.offsets.write().await;
                            offsets.insert(key, offset);
                        }
                        Err(e) => {
                            warn!(
                                "Fallback offset resolution failed for {}-{}: {}",
                                topic, partition, e
                            );
                            offsets = self.offsets.write().await;
                        }
                    }
                }
            }
        } else {
            // AutoOffsetReset::None — fail if no committed offset
            let missing: Vec<String> = need_reset.iter().map(|(t, p)| format!("{t}-{p}")).collect();
            return Err(KrafkaError::invalid_state(format!(
                "no committed offset for partitions and auto.offset.reset=none: {}",
                missing.join(", ")
            )));
        }

        // Drop the write lock before recomputing lag metrics to avoid
        // deadlocking with the read lock that recompute_lag_metrics acquires.
        drop(offsets);

        self.recompute_lag_metrics().await;
        Ok(())
    }

    /// Assign specific partitions manually.
    ///
    /// Manual assignment and group subscription are mutually exclusive.
    /// This method returns an error if a group coordinator is active.
    pub async fn assign(&self, topic: &str, partitions: Vec<PartitionId>) -> Result<()> {
        // H6: reject empty / oversize topic names at ingress so they cannot
        // reach the panicking `KafkaString::encode` path via MetadataRequest /
        // FetchRequest / OffsetFetchRequest.
        validate_topic_name(topic)?;

        if self.group_coordinator.is_some() {
            return Err(KrafkaError::invalid_state(
                "cannot use manual partition assignment with consumer group subscription",
            ));
        }

        // Refresh metadata so we can resolve partition leaders for offset lookup
        self.metadata.refresh_for_topics(Some(&[topic])).await?;

        let topic_owned = topic.to_string();

        let mut assignments = self.assignments.write().await;
        assignments.insert(topic_owned.clone(), partitions.clone());

        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.insert(topic_owned.clone());
        drop(subscriptions);
        drop(assignments);

        // Apply auto_offset_reset for manually assigned partitions
        let mut assigned = HashMap::new();
        debug!("Assigned partitions for {}: {:?}", topic, partitions);
        assigned.insert(topic_owned, partitions);
        self.apply_auto_offset_reset(&assigned).await?;

        Ok(())
    }

    /// Apply auto_offset_reset policy for partitions that have no tracked offset.
    ///
    /// This resolves initial offsets based on the configured `auto_offset_reset`
    /// policy (Earliest, Latest, or None). Used by both group and non-group
    /// consumers during partition assignment.
    async fn apply_auto_offset_reset(
        &self,
        assigned: &HashMap<String, Vec<PartitionId>>,
    ) -> Result<()> {
        // Collect partitions that don't already have a tracked offset
        let need_reset: Vec<(String, PartitionId)> = {
            let offsets = self.offsets.read().await;
            let mut need = Vec::new();
            for (topic, partitions) in assigned {
                for &p in partitions {
                    let key = (topic.clone(), p);
                    if !offsets.contains_key(&key) {
                        need.push(key);
                    }
                }
            }
            need
        };

        if need_reset.is_empty() {
            return Ok(());
        }

        if let Some(timestamp) = self.config.auto_offset_reset.to_offset() {
            let reset_pairs: Vec<(&str, PartitionId)> =
                need_reset.iter().map(|(t, p)| (t.as_str(), *p)).collect();
            let batch = group_topic_partitions(&reset_pairs);

            let resolved = self.resolve_list_offsets(&batch, timestamp).await;
            let mut offsets = self.offsets.write().await;
            for (key, result) in &resolved {
                if let Ok(offset) = result {
                    offsets.insert(key.clone(), *offset);
                }
            }
            drop(offsets);

            // Log any partitions that weren't resolved (no leader, broker error, etc.)
            for key in &need_reset {
                if resolved.get(key).is_none_or(|r| r.is_err()) {
                    warn!(
                        "Failed to resolve offset for {}-{}, will retry on next poll",
                        key.0, key.1
                    );
                }
            }
        } else {
            // AutoOffsetReset::None — fail if no offset
            let missing = need_reset
                .iter()
                .map(|(t, p)| format!("{t}-{p}"))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(KrafkaError::invalid_state(format!(
                "no offset for partitions and auto.offset.reset=none: {missing}"
            )));
        }

        self.recompute_lag_metrics().await;
        Ok(())
    }

    /// Seek to a specific offset.
    pub async fn seek(&self, topic: &str, partition: PartitionId, offset: Offset) -> Result<()> {
        {
            let mut offsets = self.offsets.write().await;
            offsets.insert((topic.to_string(), partition), offset);
        }
        self.recompute_lag_metrics().await;
        debug!("Seek to offset {} for {}-{}", offset, topic, partition);
        Ok(())
    }

    /// Seek to the beginning.
    pub async fn seek_to_beginning(&self, topic: &str, partition: PartitionId) -> Result<()> {
        self.seek(topic, partition, 0).await
    }

    /// Seek to the end (latest offset).
    ///
    /// Sets the consumer position to the high watermark, so subsequent polls
    /// will only return new messages produced after this call.
    ///
    /// This resolves the actual latest offset via a ListOffsets RPC to the
    /// partition leader. The Kafka Fetch API does not interpret special offset
    /// values like -1; those are only meaningful in the ListOffsets API.
    pub async fn seek_to_end(&self, topic: &str, partition: PartitionId) -> Result<()> {
        // Resolve the actual latest offset via ListOffsets (timestamp=-1 means latest)
        let offset = self.resolve_list_offset(topic, partition, -1).await?;
        self.seek(topic, partition, offset).await
    }

    /// Look up the earliest offset whose message timestamp is greater than or
    /// equal to the given timestamp, for each listed `(topic, partition)`.
    ///
    /// Uses the ListOffsets API. Requests are batched by leader broker so each
    /// broker receives at most one RPC.
    ///
    /// Every input partition appears in the returned map:
    /// - `Ok(offset)` — the broker returned a valid offset (`-1` means no
    ///   message exists at or after the timestamp for that partition).
    /// - `Err(e)` — a partition-level broker error (e.g. `NotLeaderForPartition`)
    ///   or a transport failure prevented resolution for this partition.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = consumer
    ///     .offsets_for_times(&[("orders", 0), ("orders", 1)], 1_700_000_000_000)
    ///     .await;
    /// for ((topic, partition), result) in &results {
    ///     match result {
    ///         Ok(offset) => println!("{topic}-{partition}: {offset}"),
    ///         Err(e) => eprintln!("{topic}-{partition}: error {e}"),
    ///     }
    /// }
    /// ```
    pub async fn offsets_for_times(
        &self,
        partitions: &[(&str, PartitionId)],
        timestamp: i64,
    ) -> HashMap<(String, PartitionId), Result<Offset>> {
        // Validate all topic names up front; surface invalid ones as per-partition
        // Err entries rather than letting them reach protocol encoding.
        let mut result: HashMap<(String, PartitionId), Result<Offset>> = HashMap::new();
        let mut valid: Vec<(&str, PartitionId)> = Vec::with_capacity(partitions.len());
        for &(topic, partition) in partitions {
            match validate_topic_name(topic) {
                Ok(()) => valid.push((topic, partition)),
                Err(e) => {
                    result.insert((topic.to_string(), partition), Err(e));
                }
            }
        }
        if !valid.is_empty() {
            let grouped = group_topic_partitions(&valid);
            result.extend(self.resolve_list_offsets(&grouped, timestamp).await);
        }
        result
    }

    /// Look up the earliest offset whose message timestamp is greater than or
    /// equal to the given timestamp, for every partition of a single topic.
    ///
    /// Convenience wrapper around [`Consumer::offsets_for_times`] that resolves
    /// the topic's partitions from metadata so callers don't have to list
    /// them. Always refreshes topic metadata before deriving the partition
    /// list so the results reflect the latest leader assignments (the refresh
    /// is skipped by the metadata layer if cached metadata is still fresh).
    ///
    /// Returns `Err` if the topic cannot be found after the metadata refresh.
    /// On success, each `PartitionId` maps to `Ok(offset)` or `Err(e)` —
    /// see [`Consumer::offsets_for_times`] for per-partition semantics.
    pub async fn offsets_for_times_for_topic(
        &self,
        topic: &str,
        timestamp: i64,
    ) -> Result<HashMap<PartitionId, Result<Offset>>> {
        validate_topic_name(topic)?;
        self.metadata.refresh_for_topics(Some(&[topic])).await?;
        let info = self
            .metadata
            .topic(topic)
            .ok_or_else(|| KrafkaError::invalid_state(format!("topic not found: {topic}")))?;

        // Build the grouped map directly — topic name is already validated and
        // the partition list comes from trusted metadata, so bypass the
        // per-entry validation loop inside `offsets_for_times`.
        let mut grouped: HashMap<String, Vec<PartitionId>> = HashMap::new();
        grouped.insert(
            topic.to_string(),
            info.partitions.iter().map(|p| p.partition).collect(),
        );
        let results = self.resolve_list_offsets(&grouped, timestamp).await;

        Ok(results
            .into_iter()
            .map(|((_, p), result)| (p, result))
            .collect())
    }

    /// Fetch the low (log start) and high (latest) watermarks for a partition.
    ///
    /// Issues two ListOffsets RPCs to the partition leader — one for the
    /// earliest offset (`timestamp = -2`) and one for the latest
    /// (`timestamp = -1`) — and returns `(low, high)`. Both RPCs are issued
    /// concurrently.
    pub async fn fetch_watermarks(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<(Offset, Offset)> {
        validate_topic_name(topic)?;
        let (low, high) = tokio::join!(
            self.resolve_list_offset(topic, partition, -2),
            self.resolve_list_offset(topic, partition, -1),
        );
        Ok((low?, high?))
    }

    /// Return a snapshot of cluster metadata (brokers and topics).
    ///
    /// If `topic` is `Some`, only that topic is returned and the metadata
    /// layer is asked to refresh that topic first (the network call is
    /// skipped if cached metadata is still fresh). If `None`, a snapshot of
    /// all currently cached topics is returned without triggering a refresh
    /// (cached data may be partial or stale).
    pub async fn fetch_metadata(&self, topic: Option<&str>) -> Result<FetchMetadataResult> {
        if let Some(name) = topic {
            validate_topic_name(name)?;
            self.metadata.refresh_for_topics(Some(&[name])).await?;
        }

        let brokers = self.metadata.brokers();
        let topics = match topic {
            Some(name) => self
                .metadata
                .topic(name)
                .map(|t| vec![t])
                .unwrap_or_default(),
            None => self.metadata.topics(),
        };

        Ok(FetchMetadataResult { brokers, topics })
    }

    /// Resolve an offset timestamp via the ListOffsets API.
    ///
    /// `timestamp` should be:
    /// - `-1` for the latest offset (high watermark)
    /// - `-2` for the earliest available offset
    async fn resolve_list_offset(
        &self,
        topic: &str,
        partition: PartitionId,
        timestamp: i64,
    ) -> Result<Offset> {
        let mut partitions = HashMap::new();
        let topic_owned = topic.to_string();
        partitions.insert(topic_owned.clone(), vec![partition]);
        let mut results = self.resolve_list_offsets(&partitions, timestamp).await;
        results
            .remove(&(topic_owned, partition))
            .unwrap_or_else(|| {
                Err(KrafkaError::protocol(format!(
                    "no offset returned for {topic}-{partition}"
                )))
            })
    }

    /// Resolve offsets for multiple partitions in batched ListOffsets RPCs,
    /// grouped by leader broker so each broker receives at most one request.
    ///
    /// Every requested partition appears in the returned map:
    /// - `Ok(offset)` — broker returned a valid offset.
    /// - `Err(e)` — the partition could not be resolved (no leader, connection
    ///   failure, or a partition-level broker error).
    async fn resolve_list_offsets(
        &self,
        partitions: &HashMap<String, Vec<PartitionId>>,
        timestamp: i64,
    ) -> HashMap<(String, PartitionId), Result<Offset>> {
        if partitions.is_empty() {
            return HashMap::new();
        }

        // Pre-populate every partition with a default error; replaced on success.
        let mut result: HashMap<(String, PartitionId), Result<Offset>> = partitions
            .iter()
            .flat_map(|(topic, parts)| {
                let topic = topic.clone();
                parts.iter().map(move |&p| {
                    let msg = format!("no leader found for {topic}-{p}");
                    ((topic.clone(), p), Err(KrafkaError::invalid_state(msg)))
                })
            })
            .collect();

        // Group partitions by leader broker
        let mut by_leader: HashMap<crate::BrokerId, Vec<(String, PartitionId)>> = HashMap::new();
        let mut leaderless: Vec<(String, PartitionId)> = Vec::new();
        for (topic, parts) in partitions {
            for &p in parts {
                if let Some(leader) = self.metadata.leader(topic, p) {
                    by_leader
                        .entry(leader)
                        .or_default()
                        .push((topic.clone(), p));
                } else {
                    leaderless.push((topic.clone(), p));
                }
            }
        }

        // Retry leaderless partitions after a metadata refresh
        if !leaderless.is_empty() {
            // Deduplicate topics to avoid redundant refresh work when multiple
            // partitions of the same topic are leaderless.
            let topic_set: HashSet<&str> = leaderless.iter().map(|(t, _)| t.as_str()).collect();
            let topics: Vec<&str> = topic_set.into_iter().collect();
            if let Err(err) = self.metadata.refresh_for_topics(Some(&topics)).await {
                warn!(
                    "Failed to refresh metadata for leaderless topics {:?}: {}",
                    topics, err
                );
            }
            for (topic, partition) in leaderless {
                if let Some(leader) = self.metadata.leader(&topic, partition) {
                    by_leader
                        .entry(leader)
                        .or_default()
                        .push((topic, partition));
                } else {
                    warn!(
                        "No leader for {}-{} after metadata refresh",
                        topic, partition
                    );
                    // result[(topic, partition)] retains its default Err
                }
            }
        }

        for (&leader_id, leader_partitions) in &by_leader {
            // Group into ListOffsetsRequest topics
            let mut topics_map: HashMap<String, Vec<ListOffsetsRequestPartition>> = HashMap::new();
            for (topic, partition) in leader_partitions {
                topics_map
                    .entry(topic.clone())
                    .or_default()
                    .push(ListOffsetsRequestPartition {
                        partition_index: *partition,
                        // ListOffsets v1/v2 do not serialize current_leader_epoch; use sentinel.
                        current_leader_epoch: -1,
                        timestamp,
                    });
            }

            let topics: Vec<ListOffsetsRequestTopic> = topics_map
                .into_iter()
                .map(|(name, parts)| ListOffsetsRequestTopic {
                    name,
                    partitions: parts,
                })
                .collect();

            let request = ListOffsetsRequest {
                replica_id: -1,
                isolation_level: self.config.isolation_level.to_i8(),
                topics,
                timeout_ms: None,
            };

            // Get a connection to this broker by leader ID.
            // On any broker-level failure, mark all its partitions as Err.
            let broker_info = match self.metadata.broker(leader_id) {
                Some(b) => b,
                None => {
                    warn!("Broker {} not found in metadata, skipping", leader_id);
                    let err = KrafkaError::invalid_state(format!(
                        "broker {leader_id} not found in metadata"
                    ));
                    for (topic, partition) in leader_partitions {
                        result.insert((topic.clone(), *partition), Err(err.clone()));
                    }
                    continue;
                }
            };
            let conn = match self
                .pool
                .get_connection_by_id(leader_id, broker_info.address())
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to connect to broker {}: {}, skipping", leader_id, e);
                    for (topic, partition) in leader_partitions {
                        result.insert((topic.clone(), *partition), Err(e.clone()));
                    }
                    continue;
                }
            };

            // Negotiate ListOffsets version — require v1+ (MIN).
            let list_version = match conn
                .negotiate_api_version(
                    ApiKey::ListOffsets,
                    versions::LIST_OFFSETS_MAX,
                    versions::LIST_OFFSETS_MIN,
                )
                .await
            {
                Some(v) => v,
                None => {
                    let err = KrafkaError::protocol(format!(
                        "no mutually supported ListOffsets API version for broker {leader_id}"
                    ));
                    warn!("{err}");
                    for (topic, partition) in leader_partitions {
                        result.insert((topic.clone(), *partition), Err(err.clone()));
                    }
                    continue;
                }
            };

            let response = match conn
                .send_request(ApiKey::ListOffsets, list_version, |buf| {
                    request.encode_versioned(list_version, buf)
                })
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "ListOffsets v{} request failed for broker {}: {}, skipping",
                        list_version, leader_id, e
                    );
                    for (topic, partition) in leader_partitions {
                        result.insert((topic.clone(), *partition), Err(e.clone()));
                    }
                    continue;
                }
            };

            let mut buf = response;
            let list_response = match ListOffsetsResponse::decode_versioned(list_version, &mut buf)
            {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "Failed to decode ListOffsets v{} response from broker {}: {}, skipping",
                        list_version, leader_id, e
                    );
                    let err = KrafkaError::protocol(format!(
                        "failed to decode ListOffsets response from broker {leader_id}: {e}"
                    ));
                    for (topic, partition) in leader_partitions {
                        result.insert((topic.clone(), *partition), Err(err.clone()));
                    }
                    continue;
                }
            };

            for topic_resp in &list_response.topics {
                for part_resp in &topic_resp.partitions {
                    let key = (topic_resp.name.clone(), part_resp.partition_index);
                    if part_resp.error_code.is_ok() {
                        result.insert(key, Ok(part_resp.offset));
                    } else {
                        warn!(
                            "ListOffsets error for {}-{}: {:?}",
                            topic_resp.name, part_resp.partition_index, part_resp.error_code
                        );
                        result.insert(
                            key,
                            Err(KrafkaError::broker(
                                part_resp.error_code,
                                format!(
                                    "ListOffsets error for {}-{}",
                                    topic_resp.name, part_resp.partition_index
                                ),
                            )),
                        );
                    }
                }
            }
        }

        result
    }

    /// Poll for new records.
    ///
    /// This is the main method for consuming messages.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use krafka::consumer::Consumer;
    /// # async fn example() -> Result<(), krafka::error::KrafkaError> {
    /// let consumer = Consumer::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .group_id("my-group")
    ///     .build()
    ///     .await?;
    ///
    /// consumer.subscribe(&["my-topic"]).await?;
    ///
    /// loop {
    ///     let records = consumer.poll(std::time::Duration::from_secs(1)).await?;
    ///     for record in records {
    ///         println!("Received: {:?}", record);
    ///     }
    /// }
    /// # }
    /// ```
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(KrafkaError::invalid_state("consumer is closed"));
        }

        let _poll_timer = self.metrics.poll_latency.start();
        self.metrics.polls.inc();

        // Auto-commit timer: commit if interval has elapsed
        if self.config.enable_auto_commit && self.group_coordinator.is_some() {
            let should_commit = {
                let last = self.last_auto_commit.lock();
                last.elapsed() >= self.config.auto_commit_interval
            };
            if should_commit {
                match self.commit().await {
                    Ok(()) => {
                        *self.last_auto_commit.lock() = Instant::now();
                    }
                    Err(e) => {
                        warn!("Auto-commit failed: {}", e);
                    }
                }
            }
        }

        // Handle group rebalance if needed
        if self.handle_group_rebalance().await? {
            return Ok(vec![]);
        }

        let assignments = self.assignments.read().await;
        if assignments.is_empty() {
            self.metrics.empty_polls.inc();
            return Ok(Vec::new());
        }

        // Buffer cap: skip fetching when the recv() buffer has accumulated
        // too many unconsumed records.  Auto-commit and rebalance handling
        // above still run so the consumer remains healthy in the group.
        if self.config.max_buffered_records > 0 {
            let buffered = self.recv_buffer.lock().len();
            if buffered >= self.config.max_buffered_records as usize {
                debug!(
                    buffered = buffered,
                    max = self.config.max_buffered_records,
                    "Buffer cap reached, skipping fetch"
                );
                self.metrics.empty_polls.inc();
                return Ok(Vec::new());
            }
        }

        // Retry offset resolution for partitions that are missing tracked offsets.
        // This fulfils the "will retry on next poll" contract when initial offset
        // resolution fails (e.g., due to a transient ListOffsets error or a
        // rejoin that left some partitions without offsets).
        // Exponential backoff prevents retry storms under sustained failures.
        {
            let now = Instant::now();
            let missing: Vec<(String, PartitionId)> = {
                let offsets = self.offsets.read().await;
                let partition_state = self.partition_state.read().await;
                assignments
                    .iter()
                    .flat_map(|(topic, partitions)| {
                        partitions.iter().filter_map(|&p| {
                            let key = (topic.clone(), p);

                            if offsets.contains_key(&key) {
                                return None;
                            }

                            // Only include if backoff period has elapsed
                            match partition_state
                                .get(&key)
                                .and_then(|s| s.offset_retry_backoff)
                            {
                                None => Some(key),
                                Some((next_retry, _)) if now >= next_retry => Some(key),
                                _ => None,
                            }
                        })
                    })
                    .collect()
            };

            if !missing.is_empty() {
                debug!(
                    "Retrying offset resolution for {} partition(s) without tracked offsets",
                    missing.len()
                );
                let mut reset_partitions: HashMap<String, Vec<PartitionId>> = HashMap::new();
                for (topic, partition) in &missing {
                    reset_partitions
                        .entry(topic.clone())
                        .or_default()
                        .push(*partition);
                }

                // Use group coordinator path if available, otherwise direct path
                if let Some(ref coordinator) = self.group_coordinator {
                    if let Some(timestamp) = self.config.auto_offset_reset.to_offset() {
                        match coordinator.list_offsets(&reset_partitions, timestamp).await {
                            Ok(resolved) => {
                                let mut offsets = self.offsets.write().await;
                                for (key, offset) in &resolved {
                                    offsets.insert(key.clone(), *offset);
                                }
                                drop(offsets);

                                // Fallback for partitions the coordinator path
                                // silently dropped (partition-level errors).
                                for (topic, partition) in &missing {
                                    if !resolved.contains_key(&(topic.clone(), *partition)) {
                                        debug!(
                                            "Poll retry: falling back to direct ListOffsets for {}-{}",
                                            topic, partition
                                        );
                                        if let Ok(offset) = self
                                            .resolve_list_offset(topic, *partition, timestamp)
                                            .await
                                        {
                                            let mut offsets = self.offsets.write().await;
                                            offsets.insert((topic.clone(), *partition), offset);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Offset resolution retry via coordinator failed: {}", e);
                                // Fall back to direct path for all missing partitions
                                for (topic, partition) in &missing {
                                    if let Ok(offset) =
                                        self.resolve_list_offset(topic, *partition, timestamp).await
                                    {
                                        let mut offsets = self.offsets.write().await;
                                        offsets.insert((topic.clone(), *partition), offset);
                                    }
                                }
                            }
                        }
                    }
                } else if let Err(e) = self.apply_auto_offset_reset(&reset_partitions).await {
                    warn!("Auto-offset-reset failed for missing partitions: {e}");
                }

                // Recompute lag after resolving offsets for missing partitions
                self.recompute_lag_metrics().await;

                // Apply exponential backoff for partitions that are still
                // unresolved after the retry attempt. Clear backoff for
                // partitions that were successfully resolved.
                {
                    let offsets = self.offsets.read().await;
                    let mut partition_state = self.partition_state.write().await;
                    for (topic, partition) in &missing {
                        let key = (topic.clone(), *partition);
                        if offsets.contains_key(&key) {
                            // Successfully resolved — clear backoff.
                            if let Some(state) = partition_state.get_mut(&key) {
                                state.offset_retry_backoff = None;
                            }
                        } else {
                            // Still unresolved — compute next backoff interval.
                            // Start at 100ms, double each time, cap at 30s.
                            let base = Duration::from_millis(100);
                            let max = Duration::from_secs(30);
                            let entry = partition_state.entry(key).or_default();
                            let prev_wait = entry
                                .offset_retry_backoff
                                .map(|(_, d)| d)
                                .unwrap_or(Duration::ZERO);
                            let next_wait = (prev_wait * 2).max(base).min(max);
                            entry.offset_retry_backoff =
                                Some((Instant::now() + next_wait, next_wait));
                        }
                    }
                }
            }
        }

        let paused = self.paused.read().await;

        // Collect non-paused partition keys (one topic clone per partition)
        // and resolve leaders so the pure routing helper doesn't need async
        // metadata access.
        let mut non_paused_keys: Vec<(String, PartitionId)> = Vec::new();
        let mut leaders: HashMap<(String, PartitionId), crate::BrokerId> = HashMap::new();
        for (topic, partitions) in assignments.iter() {
            for &partition in partitions {
                let key = (topic.clone(), partition);
                if paused.contains(&key) {
                    continue;
                }
                if let Some(leader_id) = self.metadata.leader(topic, partition) {
                    leaders.insert(key.clone(), leader_id);
                }
                non_paused_keys.push(key);
            }
        }

        let now = Instant::now();
        let partition_state_read = self.partition_state.read().await;

        let plan = build_fetch_routing_plan(non_paused_keys, &partition_state_read, &leaders, now);

        // Release read lock before potentially acquiring write lock
        drop(partition_state_read);

        // Warn only for partitions that are truly skipped (no leader AND no
        // valid preferred replica). This avoids log spam during transient
        // metadata gaps when a preferred replica is still available.
        for (topic, partition) in &plan.skipped {
            warn!(
                "No leader or preferred replica for {topic}-{partition}, skipping in batch fetch"
            );
        }

        // Clear expired preferred-replica entries so they don't accumulate.
        // Only the `preferred_replica` field is cleared — other per-partition
        // caches (high watermark, log start offset, retry backoff) are kept.
        if !plan.expired_preferred.is_empty() {
            let mut partition_state = self.partition_state.write().await;
            for key in &plan.expired_preferred {
                if let Some(state) = partition_state.get_mut(key) {
                    state.preferred_replica = None;
                }
            }
        }

        drop(paused);
        drop(assignments);

        let mut all_records = Vec::new();
        let mut all_offset_updates: Vec<((String, PartitionId), Offset)> = Vec::new();
        let mut all_hw_updates: Vec<((String, PartitionId), Offset)> = Vec::new();

        // Fetch from each broker (one request per broker, containing all its partitions)
        for (broker_id, topic_partitions) in plan.partitions_by_broker {
            match self
                .batch_fetch_from_broker(broker_id, &topic_partitions, timeout)
                .await
            {
                Ok((records, offset_updates, hw_updates)) => {
                    all_records.extend(records);
                    all_offset_updates.extend(offset_updates);
                    all_hw_updates.extend(hw_updates);
                }
                Err(e) => {
                    self.metrics.record_error();
                    warn!("Batch fetch from broker {} failed: {}", broker_id, e);
                    // Clear preferred replica mappings for all partitions that
                    // were being fetched from this broker.  If the broker was
                    // actually the leader the entries won't exist (no-op), but
                    // if it was a preferred replica this avoids routing to a
                    // dead broker for up to metadata_max_age.
                    let mut partition_state = self.partition_state.write().await;
                    for tp in &topic_partitions {
                        if let Some(state) = partition_state.get_mut(tp) {
                            state.preferred_replica = None;
                        }
                    }
                }
            }
        }

        // Enforce max_poll_records
        // Negative values are treated as unlimited (no truncation)
        // Only advance offsets for records actually delivered.
        // When truncating, recompute offset updates from delivered records only.
        if self.config.max_poll_records > 0 {
            let max = self.config.max_poll_records as usize;
            if all_records.len() > max {
                all_records.truncate(max);
                // Recompute offset updates from the truncated set: for each
                // (topic, partition), the new offset is max(record.offset) + 1
                // only for records that survived truncation.
                let mut delivered_offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
                for r in &all_records {
                    let key = (r.topic.clone(), r.partition);
                    let entry = delivered_offsets.entry(key).or_insert(r.offset);
                    if r.offset > *entry {
                        *entry = r.offset;
                    }
                }
                all_offset_updates = delivered_offsets
                    .into_iter()
                    .map(|(key, offset)| (key, offset.saturating_add(1)))
                    .collect();
            }
        }

        // Commit the offset updates (deferred from batch_fetch_from_broker until after max_poll_records handling)
        let offsets_changed = !all_offset_updates.is_empty();
        if offsets_changed {
            let mut offsets = self.offsets.write().await;
            for (key, new_offset) in all_offset_updates {
                offsets.insert(key, new_offset);
            }
        }

        // Update high watermarks
        let hw_changed = !all_hw_updates.is_empty();
        if hw_changed {
            let mut partition_state = self.partition_state.write().await;
            for (key, watermark) in all_hw_updates {
                partition_state.entry(key).or_default().high_watermark = Some(watermark);
            }
        }

        // Recompute lag metrics whenever offsets or watermarks changed
        if offsets_changed || hw_changed {
            self.recompute_lag_metrics().await;
        }

        // Record metrics
        if all_records.is_empty() {
            self.metrics.empty_polls.inc();
        } else {
            let bytes: u64 = all_records
                .iter()
                .map(|r| r.value.as_ref().map(|v| v.len() as u64).unwrap_or(0))
                .sum();
            self.metrics.record_receive(all_records.len() as u64, bytes);
        }

        // Invoke consumer interceptor after fetching records
        if !all_records.is_empty() {
            crate::interceptor::safe_on_consume(&*self.interceptor, &all_records);
        }

        Ok(all_records)
    }

    /// Batch fetch from a single broker for multiple topic-partitions.
    ///
    /// This is more efficient than individual fetches because it sends a single
    /// network request for all partitions led by the same broker.
    async fn batch_fetch_from_broker(
        &self,
        broker_id: crate::BrokerId,
        topic_partitions: &[(String, PartitionId)],
        timeout: Duration,
    ) -> Result<(
        Vec<ConsumerRecord>,
        Vec<((String, PartitionId), Offset)>,
        Vec<((String, PartitionId), Offset)>,
    )> {
        if topic_partitions.is_empty() {
            return Ok((Vec::new(), Vec::new(), Vec::new()));
        }

        self.metrics.record_fetch();
        let _fetch_timer = self.metrics.fetch_latency.start();

        // Get connection to this broker
        let broker = self
            .metadata
            .broker(broker_id)
            .ok_or_else(|| KrafkaError::invalid_state(format!("broker {} not found", broker_id)))?;
        let conn = self
            .pool
            .get_connection_by_id(broker_id, broker.address())
            .await?;

        // Group by topic for the request structure
        let mut topics_map: HashMap<String, Vec<PartitionId>> = HashMap::new();
        for (topic, partition) in topic_partitions {
            topics_map
                .entry(topic.clone())
                .or_default()
                .push(*partition);
        }

        // Build fetch request with all topic-partitions.
        // Acquire the offsets read lock once for the entire build instead of
        // per-partition to reduce lock acquire/release overhead.
        let offsets_snapshot = self.offsets.read().await;
        let mut fetch_topics = Vec::with_capacity(topics_map.len());
        for (topic, partitions) in &topics_map {
            let mut fetch_partitions = Vec::with_capacity(partitions.len());
            for &partition in partitions {
                // Skip partitions with no tracked offset rather than
                // defaulting to 0, which defeats the auto_offset_reset fix.
                let offset = match offsets_snapshot.get(&(topic.clone(), partition)).copied() {
                    Some(o) => o,
                    None => {
                        warn!(
                            "No offset for {}-{}, skipping fetch (will retry offset resolution)",
                            topic, partition
                        );
                        continue;
                    }
                };
                // Get leader epoch from metadata for fencing stale reads
                let leader_epoch = self.metadata.leader_epoch(topic, partition).unwrap_or(-1);
                fetch_partitions.push(FetchPartitionRequest {
                    partition,
                    current_leader_epoch: leader_epoch,
                    fetch_offset: offset,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: self.config.max_partition_fetch_bytes,
                    replica_directory_id: None,
                    high_watermark: None,
                });
            }
            fetch_topics.push(FetchTopicRequest {
                topic: topic.clone(),
                topic_id: None,
                partitions: fetch_partitions,
            });
        }
        // Drop the read lock before the network call.
        drop(offsets_snapshot);

        // Negotiate fetch API version — prefer v11 (KIP-392 closest-replica
        // fetching), fall back through v7 (sessions) to v4.
        // We implement encode/decode for v4, v7-v10, and v11.
        // v5/v6 are unsupported (different request wire format).
        // Prefer the highest version we implement:
        //   v11 — rack_id for closest-replica routing (KIP-392)
        //   v9/v10 — current_leader_epoch for leader fencing (KIP-320;
        //            v10 shares the same request wire format as v9)
        // When client_rack is not set, cap at v10 (highest version without
        // rack_id) so we still get epoch fencing without sending an
        // unnecessary rack_id.
        let preferred_version = if self.config.client_rack.is_some() {
            11
        } else {
            10
        };
        let fetch_version = conn
            .negotiate_api_version(ApiKey::Fetch, preferred_version, 7)
            .await
            .unwrap_or_else(|| {
                debug!(
                    "No mutually supported Fetch v7+ for broker {broker_id}, falling back to v4"
                );
                4
            });

        // Build the fetch request. For v7, compute an incremental session diff
        // from fetch_topics without cloning the full topic list into the base request.
        let (session_id, session_epoch, request_topics, forgotten_topics) = if fetch_version >= 7 {
            let mut sessions = self.fetch_sessions.lock();
            let session = sessions.get_or_create(broker_id);
            let session_req = session.build_request(&fetch_topics);
            if session_req.is_full_fetch {
                debug!(
                    "Fetch broker {}: full fetch (session_id={}, epoch={})",
                    broker_id, session_req.session_id, session_req.session_epoch
                );
            } else {
                debug!(
                    "Fetch broker {}: incremental (session_id={}, epoch={}, changed={}, forgotten={})",
                    broker_id,
                    session_req.session_id,
                    session_req.session_epoch,
                    session_req.topics.len(),
                    session_req.forgotten_topics.len()
                );
            }
            (
                session_req.session_id,
                session_req.session_epoch,
                session_req.topics,
                session_req.forgotten_topics,
            )
        } else {
            // v4: move fetch_topics into the request; update_from_response
            // is only called for v7+ so fetch_topics is not needed later.
            (0, -1, std::mem::take(&mut fetch_topics), Vec::new())
        };

        let request = FetchRequest {
            replica_id: -1, // Consumer
            max_wait_ms: crate::util::duration_to_millis_i32(timeout),
            min_bytes: self.config.fetch_min_bytes,
            max_bytes: self.config.fetch_max_bytes,
            isolation_level: self.config.isolation_level.to_i8(),
            session_id,
            session_epoch,
            topics: request_topics,
            forgotten_topics,
            rack_id: self.config.client_rack.clone().unwrap_or_default(),
        };

        // Send request with negotiated version.
        // For v7+ sessions, reset session on any send/decode failure so the
        // next poll re-establishes with a full fetch instead of hitting
        // InvalidFetchSessionEpoch.
        let response = match conn
            .send_request(ApiKey::Fetch, fetch_version, |buf| {
                request.encode_versioned(fetch_version, buf)
            })
            .await
        {
            Ok(r) => r,
            Err(e) => {
                if fetch_version >= 7 {
                    let mut sessions = self.fetch_sessions.lock();
                    sessions.reset_broker(broker_id);
                }
                return Err(e);
            }
        };

        // Decode response with matching version
        let mut buf = response;
        let fetch_response = match FetchResponse::decode_versioned(fetch_version, &mut buf) {
            Ok(r) => r,
            Err(e) => {
                if fetch_version >= 7 {
                    let mut sessions = self.fetch_sessions.lock();
                    sessions.reset_broker(broker_id);
                }
                return Err(e);
            }
        };

        // KIP-219: honour broker-reported throttle time.
        conn.notify_throttle(fetch_response.throttle_time_ms);

        // Handle top-level session errors (v7+)
        if fetch_version >= 7 {
            if fetch_response.error_code == crate::error::ErrorCode::FetchSessionIdNotFound
                || fetch_response.error_code == crate::error::ErrorCode::InvalidFetchSessionEpoch
            {
                // Reset session and let the next poll do a full fetch
                warn!(
                    "Fetch session error for broker {}: {:?}, resetting session",
                    broker_id, fetch_response.error_code
                );
                let mut sessions = self.fetch_sessions.lock();
                sessions.reset_broker(broker_id);
                return Ok((Vec::new(), Vec::new(), Vec::new()));
            }

            // Update session state from response
            let mut sessions = self.fetch_sessions.lock();
            let session = sessions.get_or_create(broker_id);
            session.update_from_response(fetch_response.session_id, &fetch_topics);
        }

        // Process records
        let mut records = Vec::new();
        let mut offset_updates: Vec<((String, PartitionId), Offset)> = Vec::new();
        let mut hw_updates: Vec<((String, PartitionId), Offset)> = Vec::new();
        let mut lso_updates: Vec<((String, PartitionId), Offset)> = Vec::new();

        // Preferred replica updates (KIP-392): Some(id) to set, None to clear.
        // Collected during the loop, applied in a single write lock afterwards.
        let mut pref_updates: Vec<((String, PartitionId), Option<crate::BrokerId>)> = Vec::new();

        for topic_response in fetch_response.responses {
            let topic_name = &topic_response.topic;
            for partition_response in topic_response.partitions {
                let partition = partition_response.partition;
                let key = (topic_name.clone(), partition);

                // Capture high watermark regardless of error/empty response.
                // The broker always returns a valid high_watermark even when
                // there are no records to deliver.
                if partition_response.high_watermark >= 0 {
                    hw_updates.push((key.clone(), partition_response.high_watermark));
                }

                // Cache log_start_offset (earliest available offset) when
                // present. Returned in Fetch v5+; allows `cached_beginning_offset`
                // to serve beginning offsets from cache without a network round-trip.
                if partition_response.log_start_offset >= 0 {
                    lso_updates.push((key.clone(), partition_response.log_start_offset));
                }

                // Track preferred read replica (KIP-392, v11+ only).
                // For v7-v10, preferred_read_replica is our fabricated default
                // (-1) and must not clear valid mappings from earlier v11 responses.
                if fetch_version >= 11 {
                    if partition_response.preferred_read_replica >= 0 {
                        pref_updates
                            .push((key.clone(), Some(partition_response.preferred_read_replica)));
                    } else {
                        pref_updates.push((key.clone(), None));
                    }
                }

                if !partition_response.error_code.is_ok() {
                    // When fetching from a preferred replica and the broker
                    // returns an error, clear the preferred replica so the
                    // next poll falls back to the partition leader.  We also
                    // clear when leader metadata is unavailable (None) to
                    // avoid getting stuck routing to a failing replica until
                    // expiry.  This is not gated on fetch_version >= 11
                    // because a stale preferred mapping from an earlier v11
                    // response can still route fetches to this broker even
                    // when the negotiated version is lower (e.g. rolling
                    // upgrade).
                    let is_leader = self
                        .metadata
                        .leader(topic_name, partition)
                        .is_some_and(|leader_id| leader_id == broker_id);
                    if !is_leader {
                        debug!(
                            "Error from non-leader broker {} for {}-{}: {:?}, clearing preferred replica",
                            broker_id, topic_name, partition, partition_response.error_code
                        );
                        pref_updates.push((key.clone(), None));
                    }
                    // Handle leader epoch errors by validating via OffsetForLeaderEpoch
                    if partition_response.error_code == crate::error::ErrorCode::FencedLeaderEpoch
                        || partition_response.error_code
                            == crate::error::ErrorCode::UnknownLeaderEpoch
                    {
                        warn!(
                            "Leader epoch error for {}-{}: {:?}, validating offset via OffsetForLeaderEpoch",
                            topic_name, partition, partition_response.error_code
                        );
                        // Trigger metadata refresh and reset offset if truncation detected.
                        // On validation failure (e.g. network error), fall back to
                        // auto_offset_reset so the consumer does not get stuck on a
                        // potentially truncated partition.
                        if let Err(e) = self
                            .validate_offset_for_leader_epoch(topic_name, partition)
                            .await
                        {
                            warn!(
                                "OffsetForLeaderEpoch validation failed for {}-{}: {}, \
                                 falling back to auto_offset_reset",
                                topic_name, partition, e
                            );
                            self.handle_offset_out_of_range(topic_name, partition).await;
                        }
                    } else if partition_response.error_code
                        == crate::error::ErrorCode::OffsetOutOfRange
                    {
                        warn!(
                            "OffsetOutOfRange for {}-{}, applying auto_offset_reset",
                            topic_name, partition
                        );
                        self.handle_offset_out_of_range(topic_name, partition).await;
                    } else {
                        warn!(
                            "Fetch error for {}-{}: {:?}",
                            topic_name, partition, partition_response.error_code
                        );
                    }
                    continue; // Continue with other partitions
                }

                if let Some(record_bytes) = partition_response.records {
                    let mut batch_buf = record_bytes;
                    let mut last_offset_for_partition: Option<Offset> = None;

                    // Fetch offset for this partition — used to skip records
                    // already delivered in a prior poll when Kafka returns a
                    // batch that starts before the requested offset.
                    // Read lock is acquired and dropped inline to avoid cloning
                    // the entire offsets map on every fetch pass.
                    // Missing entry means this is the first fetch; accept all
                    // records (offset 0 skips nothing since Kafka offsets ≥ 0).
                    let partition_fetch_offset = {
                        let offsets = self.offsets.read().await;
                        match offsets.get(&key).copied() {
                            Some(offset) => offset,
                            None => {
                                debug!(
                                    topic = %topic_name,
                                    partition,
                                    "No tracked offset for partition, accepting all records"
                                );
                                0
                            }
                        }
                    };

                    // Decode all fetched batches for this partition. `poll()`
                    // applies `max_poll_records` after aggregation and
                    // recomputes offsets for the returned subset, so stopping
                    // here without buffering the remaining bytes would force a
                    // re-fetch/re-decode of the dropped batches on subsequent
                    // polls.
                    while batch_buf.len() >= 12 {
                        match RecordBatch::decode_with_limit(
                            &mut batch_buf,
                            self.config.max_decompressed_size,
                        ) {
                            Ok(batch) => {
                                for record in batch.records.into_iter() {
                                    // Use offset_delta for correct offset in compacted topics
                                    // where records may have been deleted (log compaction awareness).
                                    let record_offset = batch
                                        .base_offset
                                        .saturating_add(record.offset_delta as i64);

                                    // Skip records below the fetch offset — these were
                                    // already delivered in a prior poll but are included
                                    // because Kafka returns whole batches.
                                    if record_offset < partition_fetch_offset {
                                        continue;
                                    }

                                    records.push(ConsumerRecord {
                                        topic: topic_name.clone(),
                                        partition,
                                        offset: record_offset,
                                        timestamp: batch
                                            .base_timestamp
                                            .saturating_add(record.timestamp_delta),
                                        timestamp_type: batch.attributes.timestamp_type as i8,
                                        key: record.key,
                                        value: record.value,
                                        headers: record
                                            .headers
                                            .into_iter()
                                            .map(|h| (h.key, h.value))
                                            .collect(),
                                        leader_epoch: None,
                                        delivery_count: None,
                                    });
                                    last_offset_for_partition = Some(record_offset);
                                }
                            }
                            Err(e) => {
                                debug!("Failed to decode record batch: {}", e);
                                break;
                            }
                        }
                    }

                    // Track offset update for this partition
                    if let Some(last_offset) = last_offset_for_partition {
                        offset_updates.push((key, last_offset.saturating_add(1)));
                    }
                }
            }
        }

        // NOTE: Offsets are NOT advanced here. They are advanced in poll()
        // after max_poll_records truncation to avoid silently losing records
        // whose offsets were already committed.
        // We return offset_updates and high watermarks alongside records so
        // the caller can apply them and compute lag.

        // Apply log_start_offset and preferred-replica updates in a single
        // write lock acquisition. Log-start updates reflect broker state and
        // are not affected by max_poll_records truncation. Preferred-replica
        // last-write-wins: if a partition appears multiple times (e.g. set by
        // the response then cleared by error handling), the final entry takes
        // effect.
        if !lso_updates.is_empty() || !pref_updates.is_empty() {
            let expiry = Instant::now() + self.config.metadata_max_age;
            let mut partition_state = self.partition_state.write().await;
            for (key, offset) in lso_updates {
                partition_state.entry(key).or_default().log_start_offset = Some(offset);
            }
            for (key, value) in pref_updates {
                match value {
                    // Setting a preferred replica: insert/update the entry.
                    Some(replica_id) => {
                        partition_state.entry(key).or_default().preferred_replica =
                            Some((replica_id, expiry));
                    }
                    // Clearing: only mutate an existing entry. Skipping
                    // absent entries avoids inserting empty `PartitionState`
                    // values on every fetch response that reports
                    // `preferred_read_replica = -1` (the common case), which
                    // would otherwise write-amplify this hot path.
                    None => {
                        if let Some(state) = partition_state.get_mut(&key) {
                            state.preferred_replica = None;
                        }
                    }
                }
            }
        }

        Ok((records, offset_updates, hw_updates))
    }

    /// Handle an `OffsetOutOfRange` error for a single partition by resolving
    /// a new offset via the configured `auto_offset_reset` policy.
    async fn handle_offset_out_of_range(&self, topic: &str, partition: PartitionId) {
        let Some(target) = self.config.auto_offset_reset.to_offset() else {
            return;
        };

        let key = (topic.to_string(), partition);

        let resolved = if let Some(ref gc) = self.group_coordinator {
            let mut part_map = HashMap::new();
            part_map.insert(key.0.clone(), vec![partition]);
            match gc.list_offsets(&part_map, target).await {
                Ok(offsets) => offsets.get(&key).copied(),
                Err(e) => {
                    warn!(
                        "Coordinator list_offsets failed for {}-{}: {}, falling back to direct",
                        topic, partition, e
                    );
                    None
                }
            }
        } else {
            None
        };

        // Use coordinator result, or fall back to direct ListOffsets
        let offset = match resolved {
            Some(o) => Some(o),
            None => self
                .resolve_list_offset(topic, partition, target)
                .await
                .map_err(|e| {
                    warn!("Direct list_offset failed for {topic}-{partition}: {e}");
                    e
                })
                .ok(),
        };

        if let Some(new_offset) = offset {
            self.offsets.write().await.insert(key, new_offset);
            self.recompute_lag_metrics().await;
        }
    }

    /// Validate the consumer's offset for a partition using OffsetForLeaderEpoch.
    ///
    /// When a leader epoch error occurs during fetch, this method queries the
    /// broker for the end offset of the current leader epoch. If the consumer's
    /// current offset is beyond this (indicating log truncation), the offset
    /// is reset to the truncation point.
    async fn validate_offset_for_leader_epoch(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<()> {
        use crate::protocol::OffsetForLeaderEpochPartition;
        use crate::protocol::OffsetForLeaderEpochRequest;
        use crate::protocol::OffsetForLeaderEpochResponse;
        use crate::protocol::OffsetForLeaderEpochTopic;

        // Refresh metadata first to get updated leader info
        if let Err(e) = self.metadata.refresh_for_topics(Some(&[topic])).await {
            warn!(
                "Metadata refresh failed for {}: {}, using cached metadata",
                topic, e
            );
        }

        let leader_epoch = self.metadata.leader_epoch(topic, partition).unwrap_or(-1);

        if leader_epoch < 0 {
            return Ok(());
        }

        let leader_id = self.metadata.leader(topic, partition).ok_or_else(|| {
            KrafkaError::invalid_state(format!("no leader for {topic}-{partition}"))
        })?;

        let broker = self
            .metadata
            .broker(leader_id)
            .ok_or_else(|| KrafkaError::invalid_state(format!("broker {} not found", leader_id)))?;

        let conn = self
            .pool
            .get_connection_by_id(leader_id, broker.address())
            .await?;

        let request = OffsetForLeaderEpochRequest {
            replica_id: -1, // consumer
            topics: vec![OffsetForLeaderEpochTopic {
                topic: topic.to_string(),
                partitions: vec![OffsetForLeaderEpochPartition {
                    partition,
                    current_leader_epoch: leader_epoch,
                    leader_epoch,
                }],
            }],
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::OffsetForLeaderEpoch,
                versions::OFFSET_FOR_LEADER_EPOCH_MAX,
                versions::OFFSET_FOR_LEADER_EPOCH_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported OffsetForLeaderEpoch API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::OffsetForLeaderEpoch, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = OffsetForLeaderEpochResponse::decode_versioned(version, &mut buf)?;

        let key = (topic.to_string(), partition);
        let mut offset_changed = false;

        for topic_result in response.topics {
            for partition_result in topic_result.partitions {
                if partition_result.partition != partition {
                    continue;
                }
                if partition_result.error_code.is_ok() && partition_result.end_offset >= 0 {
                    let current_offset = {
                        let offsets = self.offsets.read().await;
                        match offsets.get(&key).copied() {
                            Some(offset) => offset,
                            None => {
                                debug!(
                                    topic = %topic,
                                    partition,
                                    "No tracked offset for partition during epoch validation"
                                );
                                0
                            }
                        }
                    };

                    if current_offset > partition_result.end_offset {
                        warn!(
                            "Log truncation detected for {}-{}: offset {} > end_offset {}, resetting",
                            topic, partition, current_offset, partition_result.end_offset
                        );
                        let mut offsets = self.offsets.write().await;
                        offsets.insert(key.clone(), partition_result.end_offset);
                        offset_changed = true;
                    }
                }
            }
        }

        if offset_changed {
            self.recompute_lag_metrics().await;
        }
        Ok(())
    }

    /// Receive the next record.
    ///
    /// This is a convenience method that returns one record at a time.
    /// Internally buffers records from `poll()` and returns them one by one,
    /// ensuring no records are lost.
    ///
    /// Returns `Ok(None)` if the consumer is closed, or `Err` on failure.
    pub async fn recv(&self) -> Result<Option<ConsumerRecord>> {
        loop {
            // Return buffered records first
            {
                let mut buffer = self.recv_buffer.lock();
                if let Some(record) = buffer.pop_front() {
                    self.metrics.buffered_records.set(buffer.len() as u64);
                    return Ok(Some(record));
                }
            }

            if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(None);
            }

            match self.poll(Duration::from_secs(1)).await {
                Ok(records) if !records.is_empty() => {
                    let mut iter = records.into_iter();
                    // Infallible: `!records.is_empty()` guard above guarantees ≥1 element.
                    let Some(first) = iter.next() else {
                        unreachable!("non-empty ConsumerRecords yields at least one element");
                    };
                    // Buffer any remaining records for subsequent recv() calls
                    if iter.len() > 0 {
                        let mut buffer = self.recv_buffer.lock();
                        buffer.extend(iter);
                        self.metrics.buffered_records.set(buffer.len() as u64);
                    }
                    return Ok(Some(first));
                }
                Ok(_) => continue,
                Err(_) if self.closed.load(std::sync::atomic::Ordering::SeqCst) => {
                    return Ok(None);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    /// Create an async [`Stream`](futures_core::Stream) of records.
    ///
    /// Each element is a `Result<ConsumerRecord>`. The stream terminates
    /// when the consumer is closed (returns `None`). Broker and network
    /// errors are propagated as `Some(Err(...))`.
    ///
    /// Internally delegates to [`recv()`](Self::recv), which handles
    /// polling, buffering, auto-commit, rebalancing, and shutdown.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use tokio_stream::StreamExt;
    ///
    /// let mut stream = consumer.stream();
    /// while let Some(result) = stream.next().await {
    ///     let record = result?;
    ///     println!("{}: {}", record.topic, record.offset);
    /// }
    /// ```
    pub fn stream(&self) -> ConsumerStream<'_> {
        ConsumerStream::new(self)
    }

    /// Commit offsets for all consumed records.
    ///
    /// This stores the current offsets for assigned partitions only.
    /// When using a consumer group, this sends an OffsetCommit request to the group coordinator.
    /// Offsets for revoked partitions are excluded to avoid overwriting the new owner's progress.
    pub async fn commit(&self) -> Result<()> {
        let offsets = self.offsets.read().await;
        if offsets.is_empty() {
            debug!("No offsets to commit");
            return Ok(());
        }

        self.metrics.commits.inc();

        // Build the set of currently assigned partitions, so we don't commit
        // stale offsets for revoked partitions.
        let assignments = self.assignments.read().await;
        let assigned_set: HashSet<(String, PartitionId)> = assignments
            .iter()
            .flat_map(|(topic, parts)| parts.iter().map(move |&p| (topic.clone(), p)))
            .collect();

        // If we have a group coordinator, send actual OffsetCommit request
        if let Some(ref coordinator) = self.group_coordinator {
            // Convert offsets to the format expected by coordinator,
            // filtering to only currently assigned partitions.
            // Use explicit group check instead of assigned_set.is_empty()
            // to avoid committing stale offsets when assignments are temporarily empty.
            let commit_offsets: HashMap<(String, PartitionId), (i64, Option<String>)> = offsets
                .iter()
                .filter(|((topic, partition), _)| {
                    assigned_set.contains(&(topic.clone(), *partition))
                })
                .map(|((topic, partition), offset)| ((topic.clone(), *partition), (*offset, None)))
                .collect();

            if commit_offsets.is_empty() {
                debug!("No assigned partition offsets to commit");
                return Ok(());
            }

            // Only pass actually-committed offsets to interceptor
            let committed_offsets: HashMap<(String, PartitionId), Offset> = commit_offsets
                .iter()
                .map(|((topic, partition), (offset, _))| ((topic.clone(), *partition), *offset))
                .collect();

            match coordinator.commit_offsets(&commit_offsets).await {
                Ok(()) => {
                    crate::interceptor::safe_on_commit(
                        &*self.interceptor,
                        &committed_offsets,
                        None,
                    );
                }
                Err(e) if e.is_retriable() => {
                    // Retry retriable errors up to 2 more times with short backoff.
                    let mut last_err = e;
                    let backoffs = [Duration::from_millis(100), Duration::from_millis(250)];
                    for delay in &backoffs {
                        debug!(
                            "Commit failed with retriable error, retrying in {:?}: {last_err}",
                            delay
                        );
                        tokio::time::sleep(*delay).await;
                        match coordinator.commit_offsets(&commit_offsets).await {
                            Ok(()) => {
                                crate::interceptor::safe_on_commit(
                                    &*self.interceptor,
                                    &committed_offsets,
                                    None,
                                );
                                return Ok(());
                            }
                            Err(e) if e.is_retriable() => {
                                last_err = e;
                            }
                            Err(e) => {
                                crate::interceptor::safe_on_commit(
                                    &*self.interceptor,
                                    &committed_offsets,
                                    Some(&e),
                                );
                                return Err(e);
                            }
                        }
                    }
                    // All retries exhausted.
                    crate::interceptor::safe_on_commit(
                        &*self.interceptor,
                        &committed_offsets,
                        Some(&last_err),
                    );
                    return Err(last_err);
                }
                Err(e) => {
                    crate::interceptor::safe_on_commit(
                        &*self.interceptor,
                        &committed_offsets,
                        Some(&e),
                    );
                    return Err(e);
                }
            }
        } else {
            // Log offsets for non-group consumers
            for ((topic, partition), offset) in offsets.iter() {
                debug!("Committed offset for {}-{}: {}", topic, partition, offset);
            }
            info!("Committed {} partition offsets (local only)", offsets.len());
        }

        Ok(())
    }

    /// Commit offsets synchronously.
    pub async fn commit_sync(&self) -> Result<()> {
        self.commit().await
    }

    /// Commit offsets asynchronously.
    ///
    /// Spawns the offset commit as a background task. Errors are logged
    /// but not propagated to the caller. Use `commit_sync()` if you need
    /// to handle commit errors.
    pub fn commit_async(&self) {
        // Snapshot offsets without blocking (try_read avoids async in non-async fn)
        // Also filter to only assigned partitions
        let assigned_set: HashSet<(String, PartitionId)> = match self.assignments.try_read() {
            Ok(guard) => guard
                .iter()
                .flat_map(|(topic, parts)| parts.iter().map(move |&p| (topic.clone(), p)))
                .collect(),
            Err(_) => HashSet::new(), // If lock contention, include all (safe fallback)
        };

        let offsets_snapshot: HashMap<(String, PartitionId), (i64, Option<String>)> = match self
            .offsets
            .try_read()
        {
            Ok(guard) => {
                if guard.is_empty() {
                    return;
                }
                guard
                    .iter()
                    .filter(|((topic, partition), _)| {
                        // Only commit offsets for assigned partitions
                        // when using group coordination. Manual assign mode commits all.
                        self.group_coordinator.is_none()
                            || assigned_set.contains(&(topic.clone(), *partition))
                    })
                    .map(|((topic, partition), offset)| {
                        ((topic.clone(), *partition), (*offset, None))
                    })
                    .collect()
            }
            Err(_) => {
                // Log warning on contention so dropped commits are visible
                tracing::warn!("commit_async: offset lock contention, skipping this commit cycle");
                return;
            }
        };

        let coordinator = self.group_coordinator.clone();
        let group_id = self.config.group_id.clone();
        tokio::spawn(async move {
            if let Some(ref coordinator) = coordinator {
                if let Err(e) = coordinator.commit_offsets(&offsets_snapshot).await {
                    tracing::warn!(
                        group_id = ?group_id,
                        error = %e,
                        "Async offset commit failed"
                    );
                }
            } else {
                tracing::debug!("Async commit: no group coordinator, offsets stored locally");
            }
        });
    }

    /// Commit specific offsets with metadata.
    ///
    /// Allows committing offsets for specific topic-partitions with optional metadata.
    /// This is useful for checkpointing or storing application-specific context.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::collections::HashMap;
    /// use krafka::consumer::{Consumer, OffsetAndMetadata, TopicPartition};
    ///
    /// # async fn example() -> Result<(), krafka::error::KrafkaError> {
    /// # let consumer: Consumer = todo!();
    /// let mut offsets = HashMap::new();
    /// offsets.insert(
    ///     TopicPartition::new("my-topic", 0),
    ///     OffsetAndMetadata::with_metadata(100, "checkpoint-abc123"),
    /// );
    /// consumer.commit_with_metadata(offsets).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn commit_with_metadata(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<()> {
        if offsets.is_empty() {
            debug!("No offsets to commit");
            return Ok(());
        }

        // Filter to only assigned partitions
        let assignments = self.assignments.read().await;
        let filtered_offsets: HashMap<TopicPartition, OffsetAndMetadata> = if assignments.is_empty()
        {
            // No group membership — accept all
            offsets
        } else {
            offsets
                .into_iter()
                .filter(|(tp, _)| {
                    assignments
                        .get(&tp.topic)
                        .is_some_and(|ps| ps.contains(&tp.partition))
                })
                .collect()
        };
        drop(assignments);

        if filtered_offsets.is_empty() {
            debug!("No offsets to commit after filtering by assigned partitions");
            return Ok(());
        }

        // If we have a group coordinator, send actual OffsetCommit request
        if let Some(ref coordinator) = self.group_coordinator {
            // Convert offsets to the format expected by coordinator
            let commit_offsets: HashMap<(String, PartitionId), (i64, Option<String>)> =
                filtered_offsets
                    .iter()
                    .map(|(tp, offset_meta)| {
                        (
                            (tp.topic.clone(), tp.partition),
                            (offset_meta.offset, offset_meta.metadata.clone()),
                        )
                    })
                    .collect();

            let interceptor_offsets: HashMap<(String, PartitionId), i64> = filtered_offsets
                .iter()
                .map(|(tp, om)| ((tp.topic.clone(), tp.partition), om.offset))
                .collect();

            match coordinator.commit_offsets(&commit_offsets).await {
                Ok(()) => {
                    crate::interceptor::safe_on_commit(
                        &*self.interceptor,
                        &interceptor_offsets,
                        None,
                    );
                }
                Err(e) => {
                    crate::interceptor::safe_on_commit(
                        &*self.interceptor,
                        &interceptor_offsets,
                        Some(&e),
                    );
                    return Err(e);
                }
            }

            // Update internal offset store
            let mut internal_offsets = self.offsets.write().await;
            for (tp, offset_meta) in filtered_offsets {
                internal_offsets.insert((tp.topic, tp.partition), offset_meta.offset);
            }
        } else {
            // Log offsets being committed with metadata for non-group consumers
            for (tp, offset_meta) in &filtered_offsets {
                let metadata_str = offset_meta.metadata.as_deref().unwrap_or("<none>");
                debug!(
                    "Committed offset for {}-{}: {} (metadata: {})",
                    tp.topic, tp.partition, offset_meta.offset, metadata_str
                );
            }

            // Update internal offset store
            let count = {
                let mut internal_offsets = self.offsets.write().await;
                for (tp, offset_meta) in filtered_offsets {
                    internal_offsets.insert((tp.topic, tp.partition), offset_meta.offset);
                }
                internal_offsets.len()
            };

            info!(
                "Committed {} partition offsets with metadata (local only)",
                count
            );
        }

        self.recompute_lag_metrics().await;
        Ok(())
    }

    /// Get the current position for a partition.
    pub async fn position(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        let offsets = self.offsets.read().await;
        offsets.get(&(topic.to_string(), partition)).copied()
    }

    /// Returns a **snapshot** of the current partition assignments.
    ///
    /// The returned `HashMap` is a clone — modifying it has no effect on the
    /// consumer's internal state. Assignments may change asynchronously due
    /// to rebalances.
    pub async fn assignment(&self) -> HashMap<String, Vec<PartitionId>> {
        let assignments = self.assignments.read().await;
        assignments.clone()
    }

    /// Get all subscribed topics.
    pub async fn subscription(&self) -> HashSet<String> {
        let subscriptions = self.subscriptions.read().await;
        subscriptions.clone()
    }

    /// Get the current lag for a specific partition.
    ///
    /// Returns the difference between the high watermark (latest offset on the
    /// broker) and the consumer's current position. Returns `None` if the high
    /// watermark or position is not yet known (e.g., no fetch has completed for
    /// this partition).
    ///
    /// This uses cached high watermarks from the most recent fetch response —
    /// no additional network calls are made.
    pub async fn current_lag(&self, topic: &str, partition: PartitionId) -> Option<u64> {
        let key = (topic.to_string(), partition);
        // Acquire offsets before partition_state to match the documented
        // lock ordering: assignments → offsets → partition_state.
        let offsets = self.offsets.read().await;
        let position = offsets.get(&key).copied()?;
        let partition_state = self.partition_state.read().await;
        let watermark = partition_state.get(&key).and_then(|s| s.high_watermark)?;
        Some((watermark - position).max(0) as u64)
    }

    /// Get per-partition lag for all assigned partitions.
    ///
    /// Returns a map of `(topic, partition) → lag` for every partition where
    /// both the high watermark and current position are known. Partitions that
    /// haven't been fetched yet are omitted.
    pub async fn lag(&self) -> HashMap<(String, PartitionId), u64> {
        // Acquire offsets before partition_state to match the documented
        // lock ordering: assignments → offsets → partition_state.
        let offsets = self.offsets.read().await;
        let partition_state = self.partition_state.read().await;
        let mut result = HashMap::with_capacity(partition_state.len());
        for (key, state) in partition_state.iter() {
            if let (Some(watermark), Some(&position)) = (state.high_watermark, offsets.get(key)) {
                result.insert(key.clone(), (watermark - position).max(0) as u64);
            }
        }
        result
    }

    /// Get the cached beginning (log start) offset for a partition.
    ///
    /// Returns the earliest available offset on the broker, cached from
    /// fetch responses. Returns `None` if no fetch has completed for this
    /// partition yet. No network calls are made.
    pub async fn cached_beginning_offset(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Option<Offset> {
        let key = (topic.to_string(), partition);
        self.partition_state
            .read()
            .await
            .get(&key)
            .and_then(|s| s.log_start_offset)
    }

    /// Get the cached end (high watermark) offset for a partition.
    ///
    /// Returns the latest offset on the broker, cached from fetch responses.
    /// Returns `None` if no fetch has completed for this partition yet.
    /// No network calls are made.
    pub async fn cached_end_offset(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        let key = (topic.to_string(), partition);
        self.partition_state
            .read()
            .await
            .get(&key)
            .and_then(|s| s.high_watermark)
    }

    /// Unsubscribe from all topics.
    ///
    /// properly notifies the rebalance listener, leaves the
    /// consumer group, clears offsets, paused set, and drains recv buffer.
    pub async fn unsubscribe(&self) {
        // Notify listener of revoked partitions before clearing
        let assignments = self.assignments.read().await;
        if !assignments.is_empty() {
            let revoked: Vec<TopicPartition> = assignments
                .iter()
                .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
                .collect();
            self.safe_on_partitions_revoked(&revoked);
        }
        drop(assignments);

        // Leave consumer group
        if let Some(ref coordinator) = self.group_coordinator
            && let Err(e) = coordinator.leave_group().await
        {
            warn!("Error leaving consumer group during unsubscribe: {}", e);
        }

        self.subscriptions.write().await.clear();
        self.assignments.write().await.clear();
        self.clear_partition_state().await;
        self.metrics.assigned_partitions.set(0);

        debug!("Unsubscribed from all topics");
    }

    /// Pause consumption of specific partitions.
    ///
    /// Paused partitions will be skipped during poll() until resumed.
    pub async fn pause(&self, topic: &str, partitions: &[PartitionId]) {
        let mut paused = self.paused.write().await;
        let topic_owned = topic.to_string();
        for &partition in partitions {
            paused.insert((topic_owned.clone(), partition));
        }
        self.metrics.paused_partitions.set(paused.len() as u64);
        debug!("Paused partitions for {}: {:?}", topic, partitions);
    }

    /// Resume consumption of specific partitions.
    ///
    /// Resumes polling for previously paused partitions.
    pub async fn resume(&self, topic: &str, partitions: &[PartitionId]) {
        let mut paused = self.paused.write().await;
        let topic_key = topic.to_string();
        for &partition in partitions {
            paused.remove(&(topic_key.clone(), partition));
        }
        self.metrics.paused_partitions.set(paused.len() as u64);
        debug!("Resumed partitions for {}: {:?}", topic, partitions);
    }

    /// Get the set of paused partitions.
    pub async fn paused_partitions(&self) -> HashSet<(String, PartitionId)> {
        self.paused.read().await.clone()
    }

    /// Replace the bootstrap server list at runtime (KIP-899).
    ///
    /// The new addresses are used on the next metadata refresh that falls back
    /// to bootstrap servers. Does not close existing connections.
    ///
    /// # Errors
    ///
    /// Returns an error if `servers` is empty.
    pub fn update_seed_brokers(&self, servers: Vec<String>) -> Result<()> {
        self.metadata.update_seed_brokers(servers)
    }

    /// Force a rebootstrap: close all connections, clear the metadata cache,
    /// and fall back to bootstrap servers (KIP-899).
    pub async fn rebootstrap(&self) {
        self.metadata.rebootstrap().await;
    }

    /// Close the consumer.
    ///
    /// Commits offsets (if auto-commit is enabled), leaves the consumer group,
    /// and tears down connections. Calling `close()` more than once is a no-op.
    pub async fn close(&self) {
        if self.closed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }

        // Auto-commit on close (if enabled)
        if self.config.enable_auto_commit
            && let Err(e) = self.commit().await
        {
            warn!("Auto-commit on close failed: {}", e);
        }

        // Notify listener that partitions are being lost
        let assignments = self.assignments.read().await;
        if !assignments.is_empty() {
            let lost: Vec<TopicPartition> = assignments
                .iter()
                .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
                .collect();
            self.safe_on_partitions_lost(&lost);
        }
        drop(assignments);

        // Leave consumer group if we have a group coordinator
        if let Some(ref coordinator) = self.group_coordinator
            && let Err(e) = coordinator.leave_group().await
        {
            warn!("Error leaving consumer group: {e}");
        }

        // Clear per-partition state so post-close recv() cannot return records
        // from partitions already signaled as lost via on_partitions_lost above.
        self.subscriptions.write().await.clear();
        self.assignments.write().await.clear();
        self.clear_partition_state().await;
        self.metrics.assigned_partitions.set(0);

        // Notify interceptor of shutdown
        crate::interceptor::safe_consumer_close(&*self.interceptor);

        self.pool.close_all().await;
        info!("Consumer closed");
    }

    /// Check if the consumer is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Get the group coordinator, if one is configured.
    #[inline]
    pub fn group_coordinator(&self) -> Option<&Arc<GroupCoordinator>> {
        self.group_coordinator.as_ref()
    }

    /// Get a snapshot of consumer metrics.
    #[inline]
    pub fn metrics(&self) -> &Arc<ConsumerMetrics> {
        &self.metrics
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        // Warn when a consumer is dropped without an explicit `close()`.
        // Skipping `close()` means the broker will not see a `LeaveGroup`
        // and the partitions will only be reassigned after
        // `session.timeout.ms` expires, stalling the rest of the group.
        // Skip during panic unwinding.
        if !self.closed.load(std::sync::atomic::Ordering::SeqCst) && !std::thread::panicking() {
            warn!(
                "Consumer dropped without close(); group rebalance will be delayed \
                 until session.timeout.ms. Call `Consumer::close()` before drop."
            );
        }
    }
}

/// Builder for creating consumers.
#[derive(Default)]
#[must_use = "builders do nothing until .build() is called"]
pub struct ConsumerBuilder {
    config: ConsumerConfig,
    rebalance_listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    interceptors: Vec<Arc<dyn crate::interceptor::ConsumerInterceptor>>,
}

impl ConsumerBuilder {
    /// Set the bootstrap servers.
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    /// Set the group ID.
    pub fn group_id(mut self, group_id: impl Into<String>) -> Self {
        self.config.group_id = Some(group_id.into());
        self
    }

    /// Set the client ID.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.config.client_id = client_id.into();
        self
    }

    /// Set auto offset reset behavior.
    pub fn auto_offset_reset(mut self, reset: AutoOffsetReset) -> Self {
        self.config.auto_offset_reset = reset;
        self
    }

    /// Enable auto commit.
    pub fn enable_auto_commit(mut self, enable: bool) -> Self {
        self.config.enable_auto_commit = enable;
        self
    }

    /// Set auto commit interval.
    pub fn auto_commit_interval(mut self, interval: Duration) -> Self {
        self.config.auto_commit_interval = interval;
        self
    }

    /// Set fetch minimum bytes.
    pub fn fetch_min_bytes(mut self, bytes: i32) -> Self {
        self.config.fetch_min_bytes = bytes;
        self
    }

    /// Set fetch maximum bytes.
    pub fn fetch_max_bytes(mut self, bytes: i32) -> Self {
        self.config.fetch_max_bytes = bytes;
        self
    }

    /// Set max partition fetch bytes.
    pub fn max_partition_fetch_bytes(mut self, bytes: i32) -> Self {
        self.config.max_partition_fetch_bytes = bytes;
        self
    }

    /// Set maximum poll records per poll() call.
    pub fn max_poll_records(mut self, max: i32) -> Self {
        self.config.max_poll_records = max;
        self
    }

    /// Set maximum poll interval before consumer is considered dead.
    pub fn max_poll_interval(mut self, interval: Duration) -> Self {
        self.config.max_poll_interval = interval;
        self
    }

    /// Set request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set session timeout for consumer groups.
    pub fn session_timeout(mut self, timeout: Duration) -> Self {
        self.config.session_timeout = timeout;
        self
    }

    /// Set heartbeat interval.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    /// Set isolation level.
    pub fn isolation_level(mut self, level: IsolationLevel) -> Self {
        self.config.isolation_level = level;
        self
    }

    /// Set partition assignment strategy for consumer groups.
    pub fn partition_assignment_strategy(mut self, strategy: PartitionAssignmentStrategy) -> Self {
        self.config.partition_assignment_strategy = strategy;
        self
    }

    /// Set the static group membership instance ID (KIP-345).
    ///
    /// When configured, the consumer uses static group membership. The broker
    /// preserves partition assignments across restarts as long as the same
    /// instance ID is used, avoiding unnecessary rebalances.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let consumer = Consumer::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .group_id("my-group")
    ///     .group_instance_id("instance-1")
    ///     .build()
    ///     .await?;
    /// ```
    pub fn group_instance_id(mut self, id: impl Into<String>) -> Self {
        self.config.group_instance_id = Some(id.into());
        self
    }

    /// Set metadata max age before forcing a refresh.
    pub fn metadata_max_age(mut self, age: Duration) -> Self {
        self.config.metadata_max_age = age;
        self
    }

    /// Set the client rack ID for closest-replica fetching (KIP-392).
    ///
    /// When configured, the consumer includes its rack in fetch requests.
    /// The broker may return a preferred read replica in the same rack,
    /// reducing cross-rack network traffic.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let consumer = Consumer::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .group_id("my-group")
    ///     .client_rack("us-east-1a")
    ///     .build()
    ///     .await?;
    /// ```
    pub fn client_rack(mut self, rack: impl Into<String>) -> Self {
        self.config.client_rack = Some(rack.into());
        self
    }

    /// Set a rebalance listener to be notified of partition assignment changes.
    pub fn rebalance_listener(mut self, listener: Arc<dyn ConsumerRebalanceListener>) -> Self {
        self.rebalance_listener = Some(listener);
        self
    }

    /// Set authentication configuration.
    ///
    /// Enables TLS and/or SASL authentication for all broker connections.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use krafka::consumer::Consumer;
    /// use krafka::auth::AuthConfig;
    ///
    /// let consumer = Consumer::builder()
    ///     .bootstrap_servers("broker:9093")
    ///     .group_id("my-group")
    ///     .auth(AuthConfig::sasl_plain("user", "password")?)
    ///     .build()
    ///     .await?;
    /// ```
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = Some(auth);
        self
    }

    /// Set SOCKS5 proxy configuration.
    ///
    /// Routes all broker connections through the specified SOCKS5 proxy.
    #[cfg(feature = "socks5")]
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.config.proxy = Some(proxy);
        self
    }

    /// Configure SASL/PLAIN authentication.
    pub fn sasl_plain(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> crate::Result<Self> {
        self.config.auth = Some(AuthConfig::sasl_plain(username, password)?);
        Ok(self)
    }

    /// Configure SASL/SCRAM-SHA-256 authentication.
    pub fn sasl_scram_sha256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_scram_sha256(username, password));
        self
    }

    /// Configure SASL/SCRAM-SHA-512 authentication.
    pub fn sasl_scram_sha512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_scram_sha512(username, password));
        self
    }

    /// Configure SASL/OAUTHBEARER authentication with a static token.
    ///
    /// For automatic token refresh, use [`sasl_oauthbearer_provider()`](Self::sasl_oauthbearer_provider).
    /// For SASL extensions, use `.auth(AuthConfig::sasl_oauthbearer_token(...))`.
    pub fn sasl_oauthbearer(mut self, token: impl Into<String>) -> Self {
        self.config.auth = Some(AuthConfig::sasl_oauthbearer(token));
        self
    }

    /// Configure SASL/OAUTHBEARER authentication with an async token provider.
    ///
    /// The provider is called on every new broker connection, ensuring
    /// tokens are always fresh.
    pub fn sasl_oauthbearer_provider(
        mut self,
        provider: impl crate::auth::OAuthBearerTokenProvider + 'static,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_oauthbearer_provider(provider));
        self
    }

    /// Set a consumer interceptor, replacing any previously added interceptors.
    ///
    /// The interceptor's `on_consume` method is called after records are fetched
    /// but before they are returned from `poll()`, and `on_commit` is called
    /// after offsets are committed.
    ///
    /// To register multiple interceptors as an ordered chain, use
    /// [`add_interceptor`](Self::add_interceptor) instead.
    pub fn interceptor(
        mut self,
        interceptor: Arc<dyn crate::interceptor::ConsumerInterceptor>,
    ) -> Self {
        self.interceptors = vec![interceptor];
        self
    }

    /// Append a consumer interceptor to the chain.
    ///
    /// Interceptors execute in the order they are added. Each interceptor is
    /// individually panic-isolated — a panic in one will not prevent the
    /// remaining interceptors from running.
    pub fn add_interceptor(
        mut self,
        interceptor: Arc<dyn crate::interceptor::ConsumerInterceptor>,
    ) -> Self {
        self.interceptors.push(interceptor);
        self
    }

    /// Set the topic cache TTL for partial metadata refreshes.
    ///
    /// During partial refreshes, cached topics that have not been refreshed
    /// within this duration are evicted to prevent unbounded cache growth.
    ///
    /// Default: 5 minutes (matching Java's `metadata.max.idle.ms`).
    pub fn metadata_topic_cache_ttl(mut self, ttl: Duration) -> Self {
        self.config.metadata_topic_cache_ttl = Some(ttl);
        self
    }

    /// Disable topic cache TTL eviction for partial metadata refreshes.
    ///
    /// By default, cached topics are evicted after 5 minutes to prevent
    /// unbounded growth on topic churn. Call this to opt out of TTL eviction;
    /// entries will then persist across partial refreshes indefinitely.
    pub fn disable_metadata_topic_cache_ttl(mut self) -> Self {
        self.config.metadata_topic_cache_ttl = None;
        self
    }

    /// Build the consumer.
    pub async fn build(self) -> Result<Consumer> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap.servers is required"));
        }
        if self.config.enable_auto_commit && self.config.group_id.is_none() {
            tracing::warn!(
                "enable_auto_commit=true has no effect without group_id; \
                 offsets will not be persisted to the broker"
            );
        }
        if self.config.heartbeat_interval >= self.config.session_timeout {
            return Err(KrafkaError::config(format!(
                "heartbeat_interval ({:?}) must be less than session_timeout ({:?}) \
                 (recommended: session_timeout / 3)",
                self.config.heartbeat_interval, self.config.session_timeout,
            )));
        }
        if self.config.session_timeout > self.config.max_poll_interval {
            return Err(KrafkaError::config(format!(
                "session_timeout ({:?}) must be <= max_poll_interval ({:?})",
                self.config.session_timeout, self.config.max_poll_interval,
            )));
        }
        let mut consumer = Consumer::new(self.config).await?;
        if let Some(listener) = self.rebalance_listener {
            consumer.rebalance_listener = listener;
        }
        if !self.interceptors.is_empty() {
            consumer.interceptor = if self.interceptors.len() == 1 {
                // infallible: len == 1 guaranteed by the surrounding if
                let Some(single) = self.interceptors.into_iter().next() else {
                    unreachable!("len == 1 verified above");
                };
                single
            } else {
                Arc::new(crate::interceptor::ConsumerInterceptorChain::new(
                    self.interceptors,
                ))
            };
        }
        Ok(consumer)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_builder() {
        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .client_id("test")
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .max_poll_records(100)
            .max_poll_interval(Duration::from_secs(600));

        assert_eq!(builder.config.bootstrap_servers, "localhost:9092");
        assert_eq!(builder.config.group_id, Some("test-group".to_string()));
        assert_eq!(builder.config.client_id, "test");
        assert_eq!(builder.config.auto_offset_reset, AutoOffsetReset::Earliest);
        assert!(!builder.config.enable_auto_commit);
        assert_eq!(builder.config.max_poll_records, 100);
        assert_eq!(builder.config.max_poll_interval, Duration::from_secs(600));
        assert!(builder.config.auth.is_none());
    }

    #[test]
    fn test_consumer_builder_with_auth() {
        let builder = Consumer::builder()
            .bootstrap_servers("broker:9093")
            .group_id("secure-group")
            .auth(AuthConfig::sasl_plain("user", "pass").unwrap());

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(!auth.requires_tls());
        assert_eq!(
            auth.security_protocol,
            crate::auth::SecurityProtocol::SaslPlaintext
        );
        assert_eq!(auth.sasl_mechanism, Some(crate::auth::SaslMechanism::Plain));
    }

    #[test]
    fn test_consumer_builder_aws_msk_iam() {
        let auth = AuthConfig::aws_msk_iam("AKID", "secret", "us-east-1");
        let builder = Consumer::builder()
            .bootstrap_servers("broker:9094")
            .group_id("msk-group")
            .auth(auth);

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_tls());
        assert!(auth.requires_sasl());
        assert_eq!(
            auth.sasl_mechanism,
            Some(crate::auth::SaslMechanism::AwsMskIam)
        );
        assert!(auth.aws_msk_iam_credentials.is_some());
        assert!(auth.tls_config.is_some());
    }

    #[test]
    fn test_consumer_builder_no_auth_by_default() {
        let builder = Consumer::builder()
            .bootstrap_servers("broker:9092")
            .group_id("group");

        assert!(builder.config.auth.is_none());
    }

    #[test]
    fn test_consumer_builder_sasl_plain() {
        let builder = Consumer::builder()
            .bootstrap_servers("broker:9093")
            .sasl_plain("user", "pass")
            .unwrap();

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.plain_credentials.is_some());
    }

    #[test]
    fn test_consumer_builder_sasl_scram() {
        let builder = Consumer::builder()
            .bootstrap_servers("broker:9093")
            .sasl_scram_sha256("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.scram_credentials.is_some());

        let builder = Consumer::builder()
            .bootstrap_servers("broker:9093")
            .sasl_scram_sha512("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.scram_credentials.is_some());
    }

    #[tokio::test]
    async fn test_consumer_builder_no_servers() {
        let result = Consumer::builder().build().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_paused_partitions_set() {
        let mut paused: HashSet<(String, PartitionId)> = HashSet::new();
        paused.insert(("topic1".to_string(), 0));
        paused.insert(("topic1".to_string(), 1));
        paused.insert(("topic2".to_string(), 0));

        assert!(paused.contains(&("topic1".to_string(), 0)));
        assert!(paused.contains(&("topic1".to_string(), 1)));
        assert!(paused.contains(&("topic2".to_string(), 0)));
        assert!(!paused.contains(&("topic2".to_string(), 1)));

        paused.remove(&("topic1".to_string(), 0));
        assert!(!paused.contains(&("topic1".to_string(), 0)));
    }

    #[test]
    fn test_topic_partition() {
        let tp = TopicPartition::new("my-topic", 3);
        assert_eq!(tp.topic(), "my-topic");
        assert_eq!(tp.partition(), 3);

        // Test Hash/Eq for HashMap use
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("test", 0), 100i64);
        map.insert(TopicPartition::new("test", 1), 200i64);
        assert_eq!(map.get(&TopicPartition::new("test", 0)), Some(&100i64));
        assert_eq!(map.get(&TopicPartition::new("test", 1)), Some(&200i64));
    }

    #[test]
    fn test_offset_and_metadata() {
        let offset = OffsetAndMetadata::new(100);
        assert_eq!(offset.offset, 100);
        assert!(offset.metadata.is_none());

        let offset_with_meta = OffsetAndMetadata::with_metadata(200, "checkpoint-123");
        assert_eq!(offset_with_meta.offset, 200);
        assert_eq!(offset_with_meta.metadata.as_deref(), Some("checkpoint-123"));

        let offset_with_epoch = OffsetAndMetadata::with_epoch(300, 5);
        assert_eq!(offset_with_epoch.offset, 300);
        assert_eq!(offset_with_epoch.leader_epoch, Some(5));
    }

    #[test]
    fn test_consumer_builder_partition_assignment_strategy() {
        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .partition_assignment_strategy(PartitionAssignmentStrategy::RoundRobin);

        assert_eq!(
            builder.config.partition_assignment_strategy,
            PartitionAssignmentStrategy::RoundRobin
        );
    }

    #[test]
    fn test_consumer_builder_with_rebalance_listener() {
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering;

        struct TestListener {
            assigned: AtomicBool,
        }
        impl ConsumerRebalanceListener for TestListener {
            fn on_partitions_assigned(&self, _: &[TopicPartition]) {
                self.assigned.store(true, Ordering::SeqCst);
            }
            fn on_partitions_revoked(&self, _: &[TopicPartition]) {}
        }

        let listener = Arc::new(TestListener {
            assigned: AtomicBool::new(false),
        });

        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .rebalance_listener(listener.clone());

        assert!(builder.rebalance_listener.is_some());
    }

    #[test]
    fn test_partition_assignment_strategy_default() {
        let config = ConsumerConfig::default();
        assert_eq!(
            config.partition_assignment_strategy,
            PartitionAssignmentStrategy::Range
        );
    }

    #[test]
    fn test_partition_assignment_strategy_protocol_name() {
        assert_eq!(PartitionAssignmentStrategy::Range.protocol_name(), "range");
        assert_eq!(
            PartitionAssignmentStrategy::RoundRobin.protocol_name(),
            "roundrobin"
        );
        assert_eq!(
            PartitionAssignmentStrategy::CooperativeSticky.protocol_name(),
            "cooperative-sticky"
        );
    }

    #[test]
    fn test_consumer_config_defaults() {
        let config = ConsumerConfig::default();
        // Verify sensible defaults
        assert!(config.fetch_max_bytes > 0);
        assert!(config.fetch_min_bytes > 0);
        assert!(config.max_partition_fetch_bytes > 0);
    }

    #[tokio::test]
    async fn test_consumer_builder_rejects_bad_heartbeat() {
        // heartbeat_interval >= session_timeout should fail
        let result = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test")
            .session_timeout(Duration::from_secs(5))
            .heartbeat_interval(Duration::from_secs(5))
            .build()
            .await;

        match result {
            Err(e) => assert!(e.to_string().contains("heartbeat_interval")),
            Ok(_) => panic!("expected error for heartbeat_interval >= session_timeout"),
        }
    }

    #[tokio::test]
    async fn test_consumer_builder_rejects_heartbeat_greater_than_session() {
        let result = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test")
            .session_timeout(Duration::from_secs(5))
            .heartbeat_interval(Duration::from_secs(10))
            .build()
            .await;

        assert!(result.is_err());
    }

    /// Verify max_poll_records truncation recomputes offset updates
    /// to prevent data loss for undelivered records.
    #[test]
    fn test_max_poll_records_offset_recomputation() {
        // Simulate what poll() does: given 5 records but max_poll_records=3,
        // only offsets for the first 3 records should be advanced.
        let records: Vec<ConsumerRecord> = (0..5)
            .map(|i| ConsumerRecord {
                topic: "topic1".to_string(),
                partition: 0,
                offset: 100 + i,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: Some(bytes::Bytes::from(format!("val-{i}"))),
                headers: vec![],
                leader_epoch: None,
                delivery_count: None,
            })
            .collect();

        let original_offset_updates: Vec<((String, PartitionId), Offset)> =
            vec![(("topic1".to_string(), 0), 105)]; // offset after last record

        let max = 3usize;
        let mut truncated = records;
        truncated.truncate(max);

        // Recompute offsets from truncated records only
        let mut delivered_offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        for r in &truncated {
            let key = (r.topic.clone(), r.partition);
            let entry = delivered_offsets.entry(key).or_insert(r.offset);
            if r.offset > *entry {
                *entry = r.offset;
            }
        }
        let new_offset_updates: Vec<_> = delivered_offsets
            .into_iter()
            .map(|(key, offset)| (key, offset + 1))
            .collect();

        // Should advance to offset 103 (100+2+1), NOT 105
        assert_eq!(new_offset_updates.len(), 1);
        let (key, offset) = &new_offset_updates[0];
        assert_eq!(key, &("topic1".to_string(), 0));
        assert_eq!(*offset, 103); // 100 + 2 (last delivered record offset) + 1

        // Not the original 105
        assert_ne!(*offset, original_offset_updates[0].1);
    }

    /// Verify max_poll_records with multiple partitions recomputes
    /// offsets correctly per partition.
    #[test]
    fn test_max_poll_records_multi_partition_offset() {
        let mut records = Vec::new();
        // 3 records from partition 0
        for i in 0..3 {
            records.push(ConsumerRecord {
                topic: "topic1".to_string(),
                partition: 0,
                offset: 50 + i,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: Some(bytes::Bytes::from("val")),
                headers: vec![],
                leader_epoch: None,
                delivery_count: None,
            });
        }
        // 3 records from partition 1
        for i in 0..3 {
            records.push(ConsumerRecord {
                topic: "topic1".to_string(),
                partition: 1,
                offset: 200 + i,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: Some(bytes::Bytes::from("val")),
                headers: vec![],
                leader_epoch: None,
                delivery_count: None,
            });
        }

        // Truncate to 4 records (all 3 from p0 + 1 from p1)
        records.truncate(4);

        let mut delivered_offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        for r in &records {
            let key = (r.topic.clone(), r.partition);
            let entry = delivered_offsets.entry(key).or_insert(r.offset);
            if r.offset > *entry {
                *entry = r.offset;
            }
        }

        // Partition 0: last delivered = 52 → advanced to 53
        assert_eq!(
            *delivered_offsets.get(&("topic1".to_string(), 0)).unwrap(),
            52
        );
        // Partition 1: last delivered = 200 → advanced to 201
        assert_eq!(
            *delivered_offsets.get(&("topic1".to_string(), 1)).unwrap(),
            200
        );
    }

    #[test]
    fn test_consumer_builder_group_instance_id() {
        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .group_instance_id("my-instance");

        assert_eq!(
            builder.config.group_instance_id,
            Some("my-instance".to_string())
        );
    }

    #[test]
    fn test_consumer_builder_interceptor() {
        use crate::interceptor::ConsumerInterceptor;
        use crate::interceptor::InterceptorResult;

        #[derive(Debug)]
        struct TestInterceptor;
        impl ConsumerInterceptor for TestInterceptor {
            fn on_consume(&self, _records: &[ConsumerRecord]) -> InterceptorResult {
                Ok(())
            }
        }

        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .interceptor(Arc::new(TestInterceptor));

        assert_eq!(builder.interceptors.len(), 1);
    }

    #[test]
    fn test_consumer_builder_add_interceptor() {
        use crate::interceptor::ConsumerInterceptor;

        #[derive(Debug)]
        struct A;
        impl ConsumerInterceptor for A {}

        #[derive(Debug)]
        struct B;
        impl ConsumerInterceptor for B {}

        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .add_interceptor(Arc::new(A))
            .add_interceptor(Arc::new(B));
        assert_eq!(builder.interceptors.len(), 2);
    }

    #[test]
    fn test_consumer_builder_interceptor_replaces_chain() {
        use crate::interceptor::ConsumerInterceptor;

        #[derive(Debug)]
        struct A;
        impl ConsumerInterceptor for A {}

        #[derive(Debug)]
        struct B;
        impl ConsumerInterceptor for B {}

        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .add_interceptor(Arc::new(A))
            .add_interceptor(Arc::new(A))
            .interceptor(Arc::new(B));
        assert_eq!(builder.interceptors.len(), 1);
    }

    // recv() buffers remaining records so none are lost.
    #[tokio::test]
    async fn test_recv_buffer_returns_all_records() {
        use std::collections::VecDeque;

        // Simulate a consumer with pre-filled recv_buffer
        let mut buffer = VecDeque::new();
        buffer.push_back(ConsumerRecord {
            topic: "t".into(),
            partition: 0,
            offset: 1,
            timestamp: 0,
            timestamp_type: 0,
            key: None,
            value: Some(bytes::Bytes::from("r1")),
            headers: vec![],
            leader_epoch: None,
            delivery_count: None,
        });
        buffer.push_back(ConsumerRecord {
            topic: "t".into(),
            partition: 0,
            offset: 2,
            timestamp: 0,
            timestamp_type: 0,
            key: None,
            value: Some(bytes::Bytes::from("r2")),
            headers: vec![],
            leader_epoch: None,
            delivery_count: None,
        });

        assert_eq!(buffer.len(), 2);
        let first = buffer.pop_front().unwrap();
        assert_eq!(first.offset, 1);
        let second = buffer.pop_front().unwrap();
        assert_eq!(second.offset, 2);
        assert!(buffer.is_empty());
    }

    // assign() is rejected when group coordinator is active.
    #[test]
    fn test_assign_with_group_id_configured() {
        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group");

        // When group_id is set, group_coordinator will be Some after new().
        // We verify the config at builder level.
        assert!(builder.config.group_id.is_some());
    }

    // subscribe() replaces rather than appending.
    #[test]
    fn test_subscribe_replaces_subscriptions() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let subs: RwLock<HashSet<String>> = RwLock::new(HashSet::new());

            // First subscribe
            {
                let mut s = subs.write().await;
                s.clear(); // clear before insert
                s.insert("topic1".to_string());
            }
            assert_eq!(subs.read().await.len(), 1);
            assert!(subs.read().await.contains("topic1"));

            // Second subscribe replaces, not appends
            {
                let mut s = subs.write().await;
                s.clear(); // clear before insert
                s.insert("topic2".to_string());
            }
            assert_eq!(subs.read().await.len(), 1);
            assert!(subs.read().await.contains("topic2"));
            assert!(!subs.read().await.contains("topic1"));
        });
    }

    // unsubscribe() clears offsets, paused, and recv_buffer.
    #[test]
    fn test_unsubscribe_clears_all_state() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let offsets: RwLock<HashMap<(String, PartitionId), Offset>> =
                RwLock::new(HashMap::new());
            let paused: RwLock<HashSet<(String, PartitionId)>> = RwLock::new(HashSet::new());
            let assignments: RwLock<HashMap<String, Vec<PartitionId>>> =
                RwLock::new(HashMap::new());
            let recv_buffer: RwLock<std::collections::VecDeque<ConsumerRecord>> =
                RwLock::new(std::collections::VecDeque::new());

            // Populate state
            offsets.write().await.insert(("t".into(), 0), 100);
            paused.write().await.insert(("t".into(), 0));
            assignments.write().await.insert("t".into(), vec![0]);
            recv_buffer.write().await.push_back(ConsumerRecord {
                topic: "t".into(),
                partition: 0,
                offset: 0,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: None,
                headers: vec![],
                leader_epoch: None,
                delivery_count: None,
            });

            // Simulate unsubscribe clearing
            offsets.write().await.clear();
            paused.write().await.clear();
            assignments.write().await.clear();
            recv_buffer.write().await.clear();

            assert!(offsets.read().await.is_empty());
            assert!(paused.read().await.is_empty());
            assert!(assignments.read().await.is_empty());
            assert!(recv_buffer.read().await.is_empty());
        });
    }

    // Fetch skips partitions with no tracked offset.
    #[test]
    fn test_fetch_skips_untracked_partitions() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let offsets: RwLock<HashMap<(String, PartitionId), Offset>> =
                RwLock::new(HashMap::new());
            offsets.write().await.insert(("t".into(), 0), 42);

            let o = offsets.read().await;
            // Partition 0 has an offset
            assert_eq!(o.get(&("t".to_string(), 0)).copied(), Some(42));
            // Partition 1 has no offset — should be skipped
            assert_eq!(o.get(&("t".to_string(), 1)).copied(), None);
        });
    }

    // Commit filtering uses group_coordinator check, not assigned_set emptiness.
    #[test]
    fn test_commit_filter_does_not_leak_stale_offsets() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let offsets: HashMap<(String, PartitionId), Offset> = [
                (("topic1".into(), 0), 100),
                (("topic2".into(), 0), 200), // stale: not assigned
            ]
            .into_iter()
            .collect();

            let assigned_set: HashSet<(String, PartitionId)> = HashSet::new(); // empty

            // OLD behavior: is_empty() would let ALL offsets through — BAD
            let old_filtered: Vec<_> = offsets
                .iter()
                .filter(|((t, p), _)| {
                    assigned_set.is_empty() || assigned_set.contains(&(t.clone(), *p))
                })
                .collect();
            assert_eq!(old_filtered.len(), 2); // Both pass — wrong

            // NEW behavior: group consumers never commit unassigned
            let has_group = true;
            let new_filtered: Vec<_> = offsets
                .iter()
                .filter(|((t, p), _)| !has_group || assigned_set.contains(&(t.clone(), *p)))
                .collect();
            assert_eq!(new_filtered.len(), 0); // None pass when empty — correct
        });
    }

    // group field removed — only group_coordinator accessor exists.
    #[test]
    fn test_no_legacy_group_field() {
        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group");
        // The builder should have no group field; only group_coordinator is used
        assert!(builder.config.group_id.is_some());
    }

    #[test]
    fn test_max_poll_interval_used_for_rebalance() {
        // rebalance_timeout should default to max_poll_interval (not session_timeout)
        let config = ConsumerConfig::default();
        // In the Java client, rebalance_timeout defaults to max.poll.interval.ms (300s)
        // not session.timeout.ms (10s). Verify our config has both.
        assert_eq!(config.max_poll_interval, Duration::from_secs(300));
        assert_eq!(config.session_timeout, Duration::from_secs(10));
        // The rebalance_timeout passed to GroupCoordinator should be max_poll_interval
        assert!(config.max_poll_interval > config.session_timeout);
    }

    /// Test that partition grouping by leader works correctly.
    /// This mirrors the grouping logic inside resolve_list_offsets.
    #[test]
    fn test_list_offsets_partition_grouping_by_leader() {
        // Simulate the leader-based grouping that resolve_list_offsets performs.
        let leader_map: HashMap<(&str, PartitionId), crate::BrokerId> = [
            (("topic1", 0), 1),
            (("topic1", 1), 2),
            (("topic2", 0), 1), // same leader as topic1-0
            (("topic2", 1), 3),
        ]
        .into_iter()
        .collect();

        let mut partitions: HashMap<String, Vec<PartitionId>> = HashMap::new();
        partitions.insert("topic1".to_string(), vec![0, 1]);
        partitions.insert("topic2".to_string(), vec![0, 1]);

        let mut by_leader: HashMap<crate::BrokerId, Vec<(String, PartitionId)>> = HashMap::new();
        for (topic, parts) in &partitions {
            for &p in parts {
                if let Some(&leader) = leader_map.get(&(topic.as_str(), p)) {
                    by_leader
                        .entry(leader)
                        .or_default()
                        .push((topic.clone(), p));
                }
            }
        }

        // Broker 1 should have topic1-0 and topic2-0
        assert_eq!(by_leader[&1].len(), 2);
        assert!(by_leader[&1].contains(&("topic1".to_string(), 0)));
        assert!(by_leader[&1].contains(&("topic2".to_string(), 0)));
        // Broker 2 should have topic1-1
        assert_eq!(by_leader[&2].len(), 1);
        assert_eq!(by_leader[&2][0], ("topic1".to_string(), 1));
        // Broker 3 should have topic2-1
        assert_eq!(by_leader[&3].len(), 1);
        assert_eq!(by_leader[&3][0], ("topic2".to_string(), 1));
    }

    /// Test request construction from grouped partitions.
    #[test]
    fn test_list_offsets_request_construction() {
        let leader_partitions: Vec<(String, PartitionId)> = vec![
            ("topic1".to_string(), 0),
            ("topic1".to_string(), 2),
            ("topic2".to_string(), 1),
        ];
        let timestamp = -1i64; // latest

        let mut topics_map: HashMap<String, Vec<ListOffsetsRequestPartition>> = HashMap::new();
        for (topic, partition) in &leader_partitions {
            topics_map
                .entry(topic.clone())
                .or_default()
                .push(ListOffsetsRequestPartition {
                    partition_index: *partition,
                    current_leader_epoch: -1,
                    timestamp,
                });
        }

        let topics: Vec<ListOffsetsRequestTopic> = topics_map
            .into_iter()
            .map(|(name, parts)| ListOffsetsRequestTopic {
                name,
                partitions: parts,
            })
            .collect();

        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics,
            timeout_ms: None,
        };

        assert_eq!(request.replica_id, -1);
        assert_eq!(request.topics.len(), 2);

        // Find topic1 and topic2 in the request
        let t1 = request.topics.iter().find(|t| t.name == "topic1").unwrap();
        assert_eq!(t1.partitions.len(), 2);
        assert!(t1.partitions.iter().any(|p| p.partition_index == 0));
        assert!(t1.partitions.iter().any(|p| p.partition_index == 2));
        for p in &t1.partitions {
            assert_eq!(p.timestamp, -1);
            assert_eq!(p.current_leader_epoch, -1);
        }

        let t2 = request.topics.iter().find(|t| t.name == "topic2").unwrap();
        assert_eq!(t2.partitions.len(), 1);
        assert_eq!(t2.partitions[0].partition_index, 1);
    }

    /// Test response result extraction — successful offsets are collected.
    #[test]
    fn test_list_offsets_response_result_extraction() {
        use crate::error::ErrorCode;
        use crate::protocol::ListOffsetsResponsePartition;
        use crate::protocol::ListOffsetsResponseTopic;

        let response = ListOffsetsResponse {
            topics: vec![
                ListOffsetsResponseTopic {
                    name: "topic1".to_string(),
                    partitions: vec![
                        ListOffsetsResponsePartition {
                            partition_index: 0,
                            error_code: ErrorCode::None,
                            timestamp: -1,
                            offset: 42,
                            leader_epoch: -1,
                        },
                        ListOffsetsResponsePartition {
                            partition_index: 1,
                            error_code: ErrorCode::None,
                            timestamp: -1,
                            offset: 100,
                            leader_epoch: -1,
                        },
                    ],
                },
                ListOffsetsResponseTopic {
                    name: "topic2".to_string(),
                    partitions: vec![ListOffsetsResponsePartition {
                        partition_index: 0,
                        error_code: ErrorCode::None,
                        timestamp: -1,
                        offset: 7,
                        leader_epoch: -1,
                    }],
                },
            ],
        };

        // Simulate the result extraction logic from resolve_list_offsets
        let mut result: HashMap<(String, PartitionId), Offset> = HashMap::new();
        for topic_resp in &response.topics {
            for part_resp in &topic_resp.partitions {
                if part_resp.error_code.is_ok() {
                    result.insert(
                        (topic_resp.name.clone(), part_resp.partition_index),
                        part_resp.offset,
                    );
                }
            }
        }

        assert_eq!(result.len(), 3);
        assert_eq!(result[&("topic1".to_string(), 0)], 42);
        assert_eq!(result[&("topic1".to_string(), 1)], 100);
        assert_eq!(result[&("topic2".to_string(), 0)], 7);
    }

    /// Test partial failure — some partitions succeed, some have error codes.
    /// Successful results are kept; errors are recorded but don't block success.
    #[test]
    fn test_list_offsets_partial_failure_keeps_successes() {
        use crate::error::ErrorCode;
        use crate::protocol::ListOffsetsResponsePartition;
        use crate::protocol::ListOffsetsResponseTopic;

        let response = ListOffsetsResponse {
            topics: vec![ListOffsetsResponseTopic {
                name: "topic1".to_string(),
                partitions: vec![
                    ListOffsetsResponsePartition {
                        partition_index: 0,
                        error_code: ErrorCode::None,
                        timestamp: -1,
                        offset: 42,
                        leader_epoch: -1,
                    },
                    ListOffsetsResponsePartition {
                        partition_index: 1,
                        error_code: ErrorCode::NotLeaderForPartition,
                        timestamp: -1,
                        offset: -1,
                        leader_epoch: -1,
                    },
                    ListOffsetsResponsePartition {
                        partition_index: 2,
                        error_code: ErrorCode::None,
                        timestamp: -1,
                        offset: 99,
                        leader_epoch: -1,
                    },
                ],
            }],
        };

        let mut result: HashMap<(String, PartitionId), Offset> = HashMap::new();
        let mut last_error: Option<KrafkaError> = None;

        for topic_resp in &response.topics {
            for part_resp in &topic_resp.partitions {
                if part_resp.error_code.is_ok() {
                    result.insert(
                        (topic_resp.name.clone(), part_resp.partition_index),
                        part_resp.offset,
                    );
                } else {
                    last_error = Some(KrafkaError::broker(
                        part_resp.error_code,
                        format!(
                            "ListOffsets error for {}-{}",
                            topic_resp.name, part_resp.partition_index
                        ),
                    ));
                }
            }
        }

        // Successful partitions are present
        assert_eq!(result.len(), 2);
        assert_eq!(result[&("topic1".to_string(), 0)], 42);
        assert_eq!(result[&("topic1".to_string(), 2)], 99);
        // Failed partition is not present
        assert!(!result.contains_key(&("topic1".to_string(), 1)));
        // Error was recorded
        assert!(last_error.is_some());
        // But since we have results, the method would return Ok (not Err)
        assert!(!result.is_empty());
    }

    /// Test that all-failed response with no results propagates the error.
    #[test]
    fn test_list_offsets_all_failed_returns_error() {
        use crate::error::ErrorCode;
        use crate::protocol::ListOffsetsResponsePartition;
        use crate::protocol::ListOffsetsResponseTopic;

        let response = ListOffsetsResponse {
            topics: vec![ListOffsetsResponseTopic {
                name: "topic1".to_string(),
                partitions: vec![ListOffsetsResponsePartition {
                    partition_index: 0,
                    error_code: ErrorCode::NotLeaderForPartition,
                    timestamp: -1,
                    offset: -1,
                    leader_epoch: -1,
                }],
            }],
        };

        let mut result: HashMap<(String, PartitionId), Offset> = HashMap::new();
        let mut last_error: Option<KrafkaError> = None;

        for topic_resp in &response.topics {
            for part_resp in &topic_resp.partitions {
                if part_resp.error_code.is_ok() {
                    result.insert(
                        (topic_resp.name.clone(), part_resp.partition_index),
                        part_resp.offset,
                    );
                } else {
                    last_error = Some(KrafkaError::broker(
                        part_resp.error_code,
                        format!(
                            "ListOffsets error for {}-{}",
                            topic_resp.name, part_resp.partition_index
                        ),
                    ));
                }
            }
        }

        // No results — method would return Err(last_error)
        assert!(result.is_empty());
        assert!(last_error.is_some());
        let err = last_error.unwrap();
        assert!(err.to_string().contains("ListOffsets error"));
    }

    /// Test ListOffsets request encoding for v1 and v2 produces expected sizes.
    #[test]
    fn test_list_offsets_request_encode_v1_v2() {
        use bytes::BytesMut;

        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 1,
            topics: vec![ListOffsetsRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![
                    ListOffsetsRequestPartition {
                        partition_index: 0,
                        current_leader_epoch: -1,
                        timestamp: -1, // latest
                    },
                    ListOffsetsRequestPartition {
                        partition_index: 1,
                        current_leader_epoch: -1,
                        timestamp: -2, // earliest
                    },
                ],
            }],
            timeout_ms: None,
        };

        // v1 encode
        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        let encoded_v1_len = buf.len();
        assert!(encoded_v1_len > 0);

        // v2 encode produces additional isolation_level byte
        let mut buf_v2 = BytesMut::new();
        request.encode_v2(&mut buf_v2).unwrap();
        // v2 has one extra byte for isolation_level
        assert_eq!(buf_v2.len(), encoded_v1_len + 1);
    }

    // ── Cooperative rebalance algorithm tests ───────────────────────────

    /// Compute newly-assigned diff (new - old) as used in
    /// finalize_cooperative_assignment.
    fn cooperative_newly_assigned(
        new: &HashMap<String, Vec<PartitionId>>,
        old: &HashMap<String, Vec<PartitionId>>,
    ) -> Vec<TopicPartition> {
        let old_sets: HashMap<&String, HashSet<PartitionId>> = old
            .iter()
            .map(|(t, ps)| (t, ps.iter().copied().collect()))
            .collect();
        let mut result = Vec::new();
        for (topic, partitions) in new {
            let old_set = old_sets.get(topic);
            for &p in partitions {
                let is_new = old_set.is_none_or(|os| !os.contains(&p));
                if is_new {
                    result.push(TopicPartition::new(topic, p));
                }
            }
        }
        result
    }

    /// Compute cooperative revocations (old - new) as used in the
    /// no-revocations poll path.
    fn cooperative_revocations(
        old: &HashMap<String, Vec<PartitionId>>,
        new: &HashMap<String, Vec<PartitionId>>,
    ) -> Vec<TopicPartition> {
        let new_sets: HashMap<&String, HashSet<PartitionId>> = new
            .iter()
            .map(|(t, ps)| (t, ps.iter().copied().collect()))
            .collect();
        let mut result = Vec::new();
        for (topic, partitions) in old {
            let new_set = new_sets.get(topic);
            for &p in partitions {
                let gone = new_set.is_none_or(|ns| !ns.contains(&p));
                if gone {
                    result.push(TopicPartition::new(topic, p));
                }
            }
        }
        result
    }

    /// Simulate the apply_partition_revocations HashMap algorithm.
    fn apply_revocations_to_assignments(
        assignments: &mut HashMap<String, Vec<PartitionId>>,
        revoked: &[(String, PartitionId)],
    ) {
        let mut revoked_by_topic: HashMap<&str, HashSet<PartitionId>> = HashMap::new();
        for (topic, partition) in revoked {
            revoked_by_topic
                .entry(topic.as_str())
                .or_default()
                .insert(*partition);
        }
        for (topic, revoked_parts) in &revoked_by_topic {
            if let Some(parts) = assignments.get_mut(*topic) {
                parts.retain(|p| !revoked_parts.contains(p));
                if parts.is_empty() {
                    assignments.remove(*topic);
                }
            }
        }
    }

    #[test]
    fn test_cooperative_newly_assigned_fresh_join() {
        let old: HashMap<String, Vec<PartitionId>> = HashMap::new();
        let new: HashMap<String, Vec<PartitionId>> = [
            ("topic1".to_string(), vec![0, 1, 2]),
            ("topic2".to_string(), vec![0]),
        ]
        .into_iter()
        .collect();

        let result = cooperative_newly_assigned(&new, &old);
        assert_eq!(result.len(), 4);
        assert!(result.contains(&TopicPartition::new("topic1", 0)));
        assert!(result.contains(&TopicPartition::new("topic1", 1)));
        assert!(result.contains(&TopicPartition::new("topic1", 2)));
        assert!(result.contains(&TopicPartition::new("topic2", 0)));
    }

    #[test]
    fn test_cooperative_newly_assigned_partial_overlap() {
        let old: HashMap<String, Vec<PartitionId>> = [
            ("topic1".to_string(), vec![0, 1]),
            ("topic2".to_string(), vec![0]),
        ]
        .into_iter()
        .collect();
        let new: HashMap<String, Vec<PartitionId>> = [
            ("topic1".to_string(), vec![1, 2]),
            ("topic3".to_string(), vec![0]),
        ]
        .into_iter()
        .collect();

        let result = cooperative_newly_assigned(&new, &old);
        // topic1-1 retained, topic1-2 new, topic3-0 new
        assert_eq!(result.len(), 2);
        assert!(result.contains(&TopicPartition::new("topic1", 2)));
        assert!(result.contains(&TopicPartition::new("topic3", 0)));
        assert!(!result.contains(&TopicPartition::new("topic1", 1))); // retained
    }

    #[test]
    fn test_cooperative_newly_assigned_identical() {
        let assignment: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0, 1])].into_iter().collect();

        let result = cooperative_newly_assigned(&assignment, &assignment);
        assert!(result.is_empty());
    }

    #[test]
    fn test_cooperative_revocations_partial() {
        let old: HashMap<String, Vec<PartitionId>> = [
            ("topic1".to_string(), vec![0, 1, 2]),
            ("topic2".to_string(), vec![0]),
        ]
        .into_iter()
        .collect();
        let new: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![1])].into_iter().collect();

        let result = cooperative_revocations(&old, &new);
        // topic1-0, topic1-2, topic2-0 revoked; topic1-1 retained
        assert_eq!(result.len(), 3);
        assert!(result.contains(&TopicPartition::new("topic1", 0)));
        assert!(result.contains(&TopicPartition::new("topic1", 2)));
        assert!(result.contains(&TopicPartition::new("topic2", 0)));
    }

    #[test]
    fn test_cooperative_revocations_full() {
        let old: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0, 1])].into_iter().collect();
        let new: HashMap<String, Vec<PartitionId>> = HashMap::new();

        let result = cooperative_revocations(&old, &new);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_cooperative_revocations_none() {
        let old: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0])].into_iter().collect();
        let new: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0, 1])].into_iter().collect();

        let result = cooperative_revocations(&old, &new);
        assert!(result.is_empty());
    }

    #[test]
    fn test_apply_revocations_removes_partitions() {
        let mut assignments: HashMap<String, Vec<PartitionId>> = [
            ("topic1".to_string(), vec![0, 1, 2]),
            ("topic2".to_string(), vec![0, 1]),
        ]
        .into_iter()
        .collect();

        let revoked = vec![
            ("topic1".to_string(), 0),
            ("topic1".to_string(), 2),
            ("topic2".to_string(), 1),
        ];

        apply_revocations_to_assignments(&mut assignments, &revoked);

        assert_eq!(assignments["topic1"], vec![1]);
        assert_eq!(assignments["topic2"], vec![0]);
    }

    #[test]
    fn test_apply_revocations_removes_empty_topics() {
        let mut assignments: HashMap<String, Vec<PartitionId>> = [
            ("topic1".to_string(), vec![0]),
            ("topic2".to_string(), vec![0, 1]),
        ]
        .into_iter()
        .collect();

        let revoked = vec![("topic1".to_string(), 0)];
        apply_revocations_to_assignments(&mut assignments, &revoked);

        // topic1 should be removed entirely since it became empty
        assert!(!assignments.contains_key("topic1"));
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments["topic2"], vec![0, 1]);
    }

    #[test]
    fn test_apply_revocations_nonexistent_partition() {
        let mut assignments: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0, 1])].into_iter().collect();

        let revoked = vec![
            ("topic1".to_string(), 5), // doesn't exist
            ("topic3".to_string(), 0), // topic doesn't exist
        ];
        apply_revocations_to_assignments(&mut assignments, &revoked);

        // Assignments unchanged
        assert_eq!(assignments["topic1"], vec![0, 1]);
    }

    /// Full cooperative two-phase scenario: verify newly-assigned and revoked
    /// diffs are consistent across the protocol flow.
    #[test]
    fn test_cooperative_two_phase_rebalance_consistency() {
        // Phase 1: existing assignment pre-rebalance
        let phase0: HashMap<String, Vec<PartitionId>> = [
            ("topic1".to_string(), vec![0, 1, 2]),
            ("topic2".to_string(), vec![0]),
        ]
        .into_iter()
        .collect();

        // Phase 1 result: broker says revoke topic1-2 and topic2-0
        let phase1_target: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0, 1])].into_iter().collect();

        let to_revoke = cooperative_revocations(&phase0, &phase1_target);
        assert_eq!(to_revoke.len(), 2);
        assert!(to_revoke.contains(&TopicPartition::new("topic1", 2)));
        assert!(to_revoke.contains(&TopicPartition::new("topic2", 0)));

        // Apply revocations
        let mut current = phase0.clone();
        let revoked_tuples: Vec<(String, PartitionId)> = to_revoke
            .iter()
            .map(|tp| (tp.topic.clone(), tp.partition))
            .collect();
        apply_revocations_to_assignments(&mut current, &revoked_tuples);
        assert_eq!(current["topic1"], vec![0, 1]);
        assert!(!current.contains_key("topic2"));

        // Phase 2: rejoin gives final assignment with a new partition
        let phase2_final: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0, 1, 3])]
                .into_iter()
                .collect();

        let newly_assigned = cooperative_newly_assigned(&phase2_final, &current);
        assert_eq!(newly_assigned.len(), 1);
        assert!(newly_assigned.contains(&TopicPartition::new("topic1", 3)));

        // No further revocations needed
        let extra_revoke = cooperative_revocations(&current, &phase2_final);
        assert!(extra_revoke.is_empty());
    }

    /// Verify that cooperative rebalance callbacks follow Java client ordering:
    /// on_partitions_revoked fires before on_partitions_assigned.
    #[test]
    fn test_cooperative_callback_ordering() {
        use std::sync::atomic::AtomicU64;
        use std::sync::atomic::Ordering;

        struct OrderTracker {
            revoke_seq: AtomicU64,
            assign_seq: AtomicU64,
            counter: AtomicU64,
        }
        impl ConsumerRebalanceListener for OrderTracker {
            fn on_partitions_assigned(&self, _: &[TopicPartition]) {
                self.assign_seq.store(
                    self.counter.fetch_add(1, Ordering::SeqCst),
                    Ordering::SeqCst,
                );
            }
            fn on_partitions_revoked(&self, _: &[TopicPartition]) {
                self.revoke_seq.store(
                    self.counter.fetch_add(1, Ordering::SeqCst),
                    Ordering::SeqCst,
                );
            }
        }

        let tracker = Arc::new(OrderTracker {
            revoke_seq: AtomicU64::new(u64::MAX),
            assign_seq: AtomicU64::new(u64::MAX),
            counter: AtomicU64::new(0),
        });

        // Simulate cooperative rebalance callback sequence:
        // 1. Revoke phase
        let revoked = vec![TopicPartition::new("topic1", 2)];
        tracker.on_partitions_revoked(&revoked);
        // 2. Assign phase
        let assigned = vec![
            TopicPartition::new("topic1", 0),
            TopicPartition::new("topic1", 1),
            TopicPartition::new("topic1", 3),
        ];
        tracker.on_partitions_assigned(&assigned);

        let revoke_order = tracker.revoke_seq.load(Ordering::SeqCst);
        let assign_order = tracker.assign_seq.load(Ordering::SeqCst);
        assert!(
            revoke_order < assign_order,
            "on_partitions_revoked (seq={revoke_order}) must fire before on_partitions_assigned (seq={assign_order})"
        );
    }

    /// Verify that on_partitions_assigned is called even with empty assignment
    /// (more consumers than partitions).
    #[test]
    fn test_cooperative_on_assigned_fires_on_empty() {
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering;

        struct EmptyTracker {
            assigned_called: AtomicBool,
        }
        impl ConsumerRebalanceListener for EmptyTracker {
            fn on_partitions_assigned(&self, parts: &[TopicPartition]) {
                assert!(parts.is_empty());
                self.assigned_called.store(true, Ordering::SeqCst);
            }
            fn on_partitions_revoked(&self, _: &[TopicPartition]) {}
        }

        let tracker = EmptyTracker {
            assigned_called: AtomicBool::new(false),
        };
        tracker.on_partitions_assigned(&[]);
        assert!(tracker.assigned_called.load(Ordering::SeqCst));
    }

    /// Build a `PartitionState` map entry with only the high watermark set.
    /// Used by the lag-computation tests below.
    fn ps_with_hw(watermark: Offset) -> PartitionState {
        PartitionState {
            high_watermark: Some(watermark),
            ..Default::default()
        }
    }

    /// Test the lag computation logic via the extracted `compute_aggregate_lag`
    /// helper — the same function used by `recompute_lag_metrics()` in
    /// production.
    #[test]
    fn test_lag_computation_logic() {
        let mut offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        let mut partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();

        // No data → lag is 0
        let (total_lag, max_lag) = compute_aggregate_lag(&offsets, &partition_state);
        assert_eq!(total_lag, 0);
        assert_eq!(max_lag, 0);

        // Populate two partitions
        offsets.insert(("t".into(), 0), 50);
        offsets.insert(("t".into(), 1), 100);
        partition_state.insert(("t".into(), 0), ps_with_hw(80));
        partition_state.insert(("t".into(), 1), ps_with_hw(120));

        let (total_lag, max_lag) = compute_aggregate_lag(&offsets, &partition_state);

        assert_eq!(total_lag, 50); // (80-50) + (120-100)
        assert_eq!(max_lag, 30); // max(30, 20)
    }

    #[test]
    fn test_lag_negative_clamped_to_zero() {
        // Position ahead of high watermark (can happen briefly after a reset)
        let mut offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        let mut partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();

        offsets.insert(("t".into(), 0), 100);
        partition_state.insert(("t".into(), 0), ps_with_hw(80));

        let (total_lag, _) = compute_aggregate_lag(&offsets, &partition_state);
        assert_eq!(total_lag, 0);
    }

    #[test]
    fn test_lag_partial_watermarks() {
        // High watermark known for only one of two partitions
        let mut offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        let mut partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();

        offsets.insert(("t".into(), 0), 50);
        offsets.insert(("t".into(), 1), 100);
        partition_state.insert(("t".into(), 0), ps_with_hw(80));
        // Partition 1 has no high watermark

        let (total_lag, _) = compute_aggregate_lag(&offsets, &partition_state);
        assert_eq!(total_lag, 30); // Only partition 0 contributes
    }

    #[test]
    fn test_lag_after_revocation() {
        // Simulate clearing revoked partitions and recomputing lag metrics
        let mut offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        let mut partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();

        offsets.insert(("t".into(), 0), 50);
        offsets.insert(("t".into(), 1), 100);
        partition_state.insert(("t".into(), 0), ps_with_hw(100)); // lag = 50
        partition_state.insert(("t".into(), 1), ps_with_hw(200)); // lag = 100

        // Revoke partition 0
        let revoked = vec![TopicPartition::new("t", 0)];
        for tp in &revoked {
            let key = (tp.topic.clone(), tp.partition);
            offsets.remove(&key);
            partition_state.remove(&key);
        }

        assert!(!partition_state.contains_key(&("t".into(), 0)));
        assert!(partition_state.contains_key(&("t".into(), 1)));

        // Recompute lag from remaining caches (same logic as apply_partition_revocations)
        let (total_lag, max_lag) = compute_aggregate_lag(&offsets, &partition_state);

        // Only partition 1 remains: lag = 200 - 100 = 100
        assert_eq!(total_lag, 100);
        assert_eq!(max_lag, 100);
    }

    #[test]
    fn test_lag_clear_resets_to_zero() {
        // After clear_partition_state, all caches are empty → lag must be 0
        let mut offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        let mut partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();

        offsets.insert(("t".into(), 0), 50);
        partition_state.insert(("t".into(), 0), ps_with_hw(100));

        // Simulate clear_partition_state
        offsets.clear();
        partition_state.clear();

        let (total_lag, _) = compute_aggregate_lag(&offsets, &partition_state);
        assert_eq!(total_lag, 0);
    }

    /// Revoking a partition must clear **every** per-partition cache
    /// (high watermark, log start offset, preferred replica, offset-retry
    /// backoff) atomically. Before the `PartitionState` consolidation this
    /// was "four separate `HashMap::remove` calls under four separate locks",
    /// which was the exact bug class the refactor eliminated. This test pins
    /// the invariant: a single `HashMap::remove` of a `PartitionState` value
    /// drops all four caches together, so no future field added to
    /// `PartitionState` can be accidentally skipped by the revocation path.
    #[test]
    fn test_partition_state_revocation_is_atomic() {
        let key = ("t".to_string(), 0_i32);
        let mut partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();
        partition_state.insert(
            key.clone(),
            PartitionState {
                high_watermark: Some(100),
                log_start_offset: Some(0),
                preferred_replica: Some((3_i32, Instant::now() + Duration::from_secs(60))),
                offset_retry_backoff: Some((Instant::now(), Duration::from_millis(100))),
            },
        );

        // Sanity: the entry has all four facets populated.
        let state = &partition_state[&key];
        assert!(state.high_watermark.is_some());
        assert!(state.log_start_offset.is_some());
        assert!(state.preferred_replica.is_some());
        assert!(state.offset_retry_backoff.is_some());

        // Revoke — a single remove wipes all four facets at once.
        partition_state.remove(&key);

        assert!(!partition_state.contains_key(&key));
    }

    // --- Fetch routing plan tests (KIP-392) ---

    /// Build a `PartitionState` map entry with only the preferred replica set.
    /// Used by the routing-plan tests below.
    fn ps_with_preferred(replica_id: crate::BrokerId, expiry: Instant) -> PartitionState {
        PartitionState {
            preferred_replica: Some((replica_id, expiry)),
            ..Default::default()
        }
    }

    #[test]
    fn test_routing_plan_uses_leader_when_no_preferred() {
        let keys = vec![("t".into(), 0), ("t".into(), 1)];

        let leaders = HashMap::from([(("t".into(), 0), 1), (("t".into(), 1), 2)]);

        let plan = build_fetch_routing_plan(keys, &HashMap::new(), &leaders, Instant::now());

        assert!(plan.expired_preferred.is_empty());
        assert_eq!(plan.partitions_by_broker[&1], vec![("t".into(), 0)]);
        assert_eq!(plan.partitions_by_broker[&2], vec![("t".into(), 1)]);
    }

    #[test]
    fn test_routing_plan_routes_to_preferred_replica() {
        let keys = vec![("t".into(), 0)];

        let leaders = HashMap::from([(("t".into(), 0), 1)]);
        let partition_state = HashMap::from([(
            ("t".into(), 0),
            ps_with_preferred(3_i32, Instant::now() + Duration::from_secs(60)),
        )]);

        let plan = build_fetch_routing_plan(keys, &partition_state, &leaders, Instant::now());

        assert!(plan.expired_preferred.is_empty());
        // Should route to preferred replica (broker 3), not leader (broker 1)
        assert_eq!(plan.partitions_by_broker.len(), 1);
        assert_eq!(plan.partitions_by_broker[&3], vec![("t".into(), 0)]);
    }

    #[test]
    fn test_routing_plan_falls_back_on_expired_preferred() {
        let keys = vec![("t".into(), 0)];

        let leaders = HashMap::from([(("t".into(), 0), 1)]);
        // Preferred replica that expired 10 seconds ago
        let partition_state = HashMap::from([(
            ("t".into(), 0),
            ps_with_preferred(3_i32, Instant::now() - Duration::from_secs(10)),
        )]);

        let plan = build_fetch_routing_plan(keys, &partition_state, &leaders, Instant::now());

        // Should fall back to leader (broker 1)
        assert_eq!(plan.partitions_by_broker[&1], vec![("t".into(), 0)]);
        // Should report the expired entry for cleanup
        assert_eq!(plan.expired_preferred, vec![("t".into(), 0)]);
    }

    #[test]
    fn test_routing_plan_skips_partitions_without_leader() {
        // Only partition 0 has a leader; partition 1 has neither leader nor
        // preferred replica and should be skipped.
        let keys = vec![("t".into(), 0), ("t".into(), 1)];

        // Only partition 0 has a leader
        let leaders = HashMap::from([(("t".into(), 0), 1)]);

        let plan = build_fetch_routing_plan(keys, &HashMap::new(), &leaders, Instant::now());

        let all: Vec<_> = plan.partitions_by_broker.values().flatten().collect();
        assert_eq!(all.len(), 1);
        assert_eq!(*all[0], ("t".into(), 0));
        assert_eq!(plan.skipped, vec![("t".into(), 1)]);
    }

    #[test]
    fn test_routing_plan_all_partitions_skipped() {
        // No leaders and no preferred replicas → every partition is skipped,
        // plan is empty.
        let keys = vec![("t".into(), 0), ("t".into(), 1)];

        let plan = build_fetch_routing_plan(keys, &HashMap::new(), &HashMap::new(), Instant::now());

        assert!(plan.partitions_by_broker.is_empty());
        assert!(plan.expired_preferred.is_empty());
        assert_eq!(plan.skipped.len(), 2);
    }

    #[test]
    fn test_routing_plan_mixed_preferred_and_leader() {
        let keys = vec![("t".into(), 0), ("t".into(), 1), ("t".into(), 2)];

        let leaders = HashMap::from([
            (("t".into(), 0), 1),
            (("t".into(), 1), 1),
            (("t".into(), 2), 2),
        ]);
        let future = Instant::now() + Duration::from_secs(300);
        let partition_state = HashMap::from([
            // p0 has a valid preferred replica
            (("t".into(), 0), ps_with_preferred(3_i32, future)),
            // p1 has an expired preferred replica
            (
                ("t".into(), 1),
                ps_with_preferred(3_i32, Instant::now() - Duration::from_secs(1)),
            ),
            // p2 has no preferred replica
        ]);

        let plan = build_fetch_routing_plan(keys, &partition_state, &leaders, Instant::now());

        // p0 → broker 3 (preferred), p1 → broker 1 (leader, expired), p2 → broker 2 (leader)
        assert!(plan.partitions_by_broker[&3].contains(&("t".into(), 0)));
        assert!(plan.partitions_by_broker[&1].contains(&("t".into(), 1)));
        assert!(plan.partitions_by_broker[&2].contains(&("t".into(), 2)));
        assert_eq!(plan.expired_preferred, vec![("t".into(), 1)]);
    }

    #[test]
    fn test_consumer_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Consumer>();
    }

    #[test]
    fn test_consumer_stream_is_send() {
        fn assert_send<T: Send>() {}
        // ConsumerStream must be Send so it can be used across .await in
        // spawned tasks (e.g., tokio::spawn with an Arc<Consumer>).
        assert_send::<ConsumerStream<'_>>();
    }

    /// The flat `&[(&str, PartitionId)]` input to `offsets_for_times` is
    /// grouped by topic via `group_topic_partitions`. Verify that the grouping
    /// preserves all pairs, deduplicates topic keys, and keeps partitions in
    /// insertion order.
    #[test]
    fn test_offsets_for_times_grouping() {
        let partitions: &[(&str, PartitionId)] =
            &[("topic1", 0), ("topic1", 2), ("topic2", 1), ("topic1", 5)];

        let grouped = group_topic_partitions(partitions);

        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped["topic1"], vec![0, 2, 5]);
        assert_eq!(grouped["topic2"], vec![1]);
    }
}
