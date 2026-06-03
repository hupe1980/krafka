//! Share consumer implementation (KIP-932).
//!
//! > ⚠️ **Unstable**: This module requires the `unstable-protocol` feature flag.
//! > APIs may change without semver notice until KIP-932 is finalized in a stable
//! > Kafka release.
//!
//! Share groups provide queue-like semantics on top of Kafka topics. Multiple
//! consumers in the same share group receive non-overlapping subsets of records
//! without client-side partition assignment — all assignment is performed by
//! the server.
//!
//! # Delivery Semantics
//!
//! Share groups support **at-least-once** delivery with explicit or implicit
//! acknowledgement:
//!
//! - **Implicit** (default): previously fetched records are automatically
//!   accepted when the next `poll()` is called.
//! - **Explicit**: the application calls
//!   [`acknowledge()`](ShareConsumer::acknowledge) per record and then
//!   [`commit_sync()`](ShareConsumer::commit_sync) to flush.
//!
//! Records that are released or not acknowledged within the acquisition lock
//! timeout are redelivered to other consumers.
//!
//! # Example
//!
//! ```ignore
//! use krafka::share_consumer::{ShareConsumer, AcknowledgementMode};
//!
//! let consumer = ShareConsumer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .group_id("my-share-group")
//!     .build()
//!     .await?;
//!
//! consumer.subscribe(&["events"]).await?;
//!
//! loop {
//!     let records = consumer.poll(Duration::from_secs(1)).await?;
//!     for record in &records {
//!         process(record);
//!     }
//!     // Implicit mode: records are auto-accepted on next poll()
//! }
//! ```

mod config;
mod session;
mod stream;

pub use config::{AcknowledgeType, AcknowledgementMode, ShareConsumerConfig};
pub use stream::ShareConsumerStream;

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use std::collections::VecDeque;
use std::future::{Future, Ready, ready};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as SyncMutex};
use std::task::{Context, Poll};
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::auth::AuthConfig;
use crate::consumer::ConsumerRecord;
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::metadata::ClusterMetadata;
use crate::metrics::ConnectionMetrics;
use crate::network::{ConnectionConfig, ConnectionPool};
use crate::protocol::{
    ApiKey, FindCoordinatorRequest, FindCoordinatorResponse, RecordBatch,
    ShareAcknowledgePartition, ShareAcknowledgeRequest, ShareAcknowledgeTopic,
    ShareAcknowledgementBatch, ShareFetchPartition, ShareFetchRequest, ShareFetchTopic,
    ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse, ShareGroupTopicPartitions,
    VersionedDecode, VersionedEncode, versions,
};
use crate::{BrokerId, Offset, PartitionId};

use session::{FINAL_EPOCH, ShareSessionCache};

/// Key for tracking unacknowledged records in explicit mode.
type RecordKey = (String, PartitionId, Offset);

/// Key for piggybacked acknowledgements grouped by broker and partition.
type BrokerAckKey = ([u8; 16], PartitionId);

/// Pending piggybacked acknowledgements grouped by broker and partition.
type BrokerPendingAcks = HashMap<BrokerId, HashMap<BrokerAckKey, Vec<PendingAck>>>;

/// Sentinel broker ID used when the partition leader is not yet known at acknowledge
/// time (e.g., immediately after `subscribe()` before the first metadata refresh, or
/// after a `restore_ack_state()` where metadata is unavailable).
/// Acks with this sentinel are re-routed using fresh metadata in `poll()`.
const UNROUTED_BROKER_ID: BrokerId = -2;

#[derive(Clone)]
struct ShareAcknowledgeContext {
    metadata: Arc<ClusterMetadata>,
    pool: Arc<ConnectionPool>,
    share_sessions: Arc<tokio::sync::Mutex<ShareSessionCache>>,
    group_id: String,
    member_id: String,
    current_ack_state_generation: Arc<AtomicU64>,
    ack_state_generation: u64,
}

/// Pending acknowledgement for a share group record.
#[derive(Debug, Clone)]
struct PendingAck {
    topic: String,
    topic_id: [u8; 16],
    partition: PartitionId,
    first_offset: Offset,
    last_offset: Offset,
    ack_type: i8,
}

fn flatten_partition_acks(
    partition_acks: HashMap<BrokerAckKey, Vec<PendingAck>>,
) -> Vec<PendingAck> {
    partition_acks.into_values().flatten().collect()
}

fn drain_broker_partition_acks(
    broker_acks: &mut HashMap<BrokerAckKey, Vec<PendingAck>>,
    topic_id: [u8; 16],
    partition: PartitionId,
) -> Vec<PendingAck> {
    broker_acks
        .remove(&(topic_id, partition))
        .unwrap_or_default()
}

fn drain_broker_acks(broker_acks: &mut BrokerPendingAcks, broker_id: BrokerId) -> Vec<PendingAck> {
    broker_acks
        .remove(&broker_id)
        .map(flatten_partition_acks)
        .unwrap_or_default()
}

fn describe_share_fetch_join_error(error: &tokio::task::JoinError) -> &'static str {
    if error.is_panic() {
        "panicked"
    } else if error.is_cancelled() {
        "was cancelled"
    } else {
        "failed"
    }
}

/// Handle returned by [`ShareConsumer::commit_async`].
///
/// Await the handle to observe the final broker outcome. Dropping it detaches
/// the background task and discards the result.
#[must_use = "await the returned handle to observe share-commit outcome"]
#[non_exhaustive]
pub enum ShareCommitHandle {
    /// Immediate commit result without spawning a background task.
    Ready(Ready<Result<()>>),
    /// Background task handle that resolves to the commit result.
    Task(tokio::task::JoinHandle<Result<()>>),
}

impl ShareCommitHandle {
    fn ready(result: Result<()>) -> Self {
        Self::Ready(ready(result))
    }
}

impl Future for ShareCommitHandle {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Ready(fut) => Pin::new(fut).poll(cx),
            Self::Task(handle) => match Pin::new(handle).poll(cx) {
                Poll::Ready(Ok(result)) => Poll::Ready(result),
                Poll::Ready(Err(error)) => Poll::Ready(Err(KrafkaError::invalid_state(format!(
                    "share commit task failed: {error}"
                )))),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

/// All internal state shared between `ShareConsumer` handles.
struct ShareConsumerInner {
    /// Configuration.
    config: ShareConsumerConfig,
    /// Cluster metadata.
    metadata: Arc<ClusterMetadata>,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Subscribed topics.
    subscriptions: RwLock<HashSet<String>>,
    /// Current partition assignments from the coordinator.
    /// Maps topic name → partition IDs.
    assignments: RwLock<HashMap<String, Vec<PartitionId>>>,
    /// Client-generated member ID (UUID, per KIP-932).
    member_id: ArcSwap<String>,
    /// Current member epoch.
    member_epoch: AtomicI32,
    /// Heartbeat interval returned by the coordinator.
    heartbeat_interval_ms: AtomicI32,
    /// Whether the consumer is closed.
    closed: AtomicBool,
    /// Per-broker share session cache.
    share_sessions: Arc<tokio::sync::Mutex<ShareSessionCache>>,
    /// Pending acknowledgements sharded by broker ID.
    ///
    /// Pre-routing at acknowledge time means `poll()` can hand each broker its
    /// acks in O(1) without rescanning the entire flat map.  Acks whose leader
    /// is not yet known are stored under `UNROUTED_BROKER_ID` and re-routed
    /// using fresh metadata at the start of the next `poll()`.
    pending_acks: Arc<RwLock<BrokerPendingAcks>>,
    /// Monotonic token for the current local ack state.
    ///
    /// Incremented whenever local ack state is cleared or invalidated so
    /// detached flush tasks cannot send or requeue stale acknowledgements
    /// from an older membership/session after assignment changes,
    /// `unsubscribe()`, or `close()`.
    ack_state_generation: Arc<AtomicU64>,
    /// Explicit-mode barrier raised after a commit flush fails.
    ///
    /// While this is set, `poll()` refuses to fetch more records until the
    /// application retries `commit_sync()`/`commit_async()` successfully or
    /// local state is cleared during unsubscribe/close.
    explicit_flush_retry_required: Arc<AtomicBool>,
    /// Topic name → UUID cache (populated from heartbeat assignments and metadata).
    topic_ids: RwLock<HashMap<String, [u8; 16]>>,
    /// Buffer for records returned by `recv()`.
    recv_buffer: RwLock<VecDeque<ConsumerRecord>>,
    /// Coordinator broker ID (discovered via FindCoordinator).
    coordinator_id: RwLock<Option<BrokerId>>,
    /// Coordinator address (host:port).
    coordinator_address: RwLock<Option<String>>,
    /// Tracks unacknowledged records from the previous `poll()` in explicit mode.
    /// Must be empty before the next `poll()` can fetch new records.
    unacked_offsets: Arc<RwLock<HashSet<RecordKey>>>,
    /// Background heartbeat task. Spawned on the first `subscribe()` call;
    /// aborted by `close()` or `unsubscribe()`.
    heartbeat_task: SyncMutex<Option<tokio::task::JoinHandle<()>>>,
    /// Set by `wakeup()` to interrupt an in-progress `poll()`.
    /// Cleared at the start of each `poll()` call.
    wakeup_flag: AtomicBool,
}

impl Drop for ShareConsumerInner {
    fn drop(&mut self) {
        // Fires when the last `ShareConsumer` clone is dropped.
        // Warn if close() was never called — pending acks are silently lost
        // and the coordinator will only reclaim partitions after the heartbeat
        // lease expires. Skip the warning during panic unwinding.
        if !self.closed.load(Ordering::Relaxed) && !std::thread::panicking() {
            warn!(
                "ShareConsumer dropped without close(); pending acks may be lost and \
                 share-group rebalance will be delayed. Call `ShareConsumer::close()` before drop."
            );
        }
    }
}

/// A Kafka share consumer (KIP-932).
///
/// Provides queue-like consumption semantics where the server controls
/// partition assignment and record delivery. Unlike traditional consumer
/// groups, share consumers do not track offsets — instead they acknowledge
/// individual records.
///
/// `ShareConsumer` is cheaply cloneable: all clones share the same
/// connection pool, coordinator state, and acknowledgement buffers via an
/// internal [`Arc`]. A background heartbeat task is started on the first
/// [`subscribe()`](Self::subscribe) call and stopped on
/// [`close()`](Self::close) / [`unsubscribe()`](Self::unsubscribe).
#[derive(Clone)]
pub struct ShareConsumer(Arc<ShareConsumerInner>);

impl std::fmt::Debug for ShareConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareConsumer")
            .field("group_id", &self.0.config.group_id)
            .field("closed", &self.0.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl ShareConsumer {
    /// Create a new share consumer builder.
    pub fn builder() -> ShareConsumerBuilder {
        ShareConsumerBuilder::default()
    }

    /// Create a new share consumer with the given configuration.
    async fn new(config: ShareConsumerConfig) -> Result<Self> {
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

        let mut pool_config = pool_config_builder.build()?;
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

        metadata.refresh().await?;

        info!(
            "ShareConsumer initialized with {} brokers, group_id='{}'",
            metadata.brokers().len(),
            config.group_id
        );

        Ok(ShareConsumer(Arc::new(ShareConsumerInner {
            config,
            metadata,
            pool,
            subscriptions: RwLock::new(HashSet::new()),
            assignments: RwLock::new(HashMap::new()),
            member_id: ArcSwap::new(Arc::new(crate::util::random_uuid_v4())),
            member_epoch: AtomicI32::new(0),
            heartbeat_interval_ms: AtomicI32::new(5000),
            closed: AtomicBool::new(false),
            share_sessions: Arc::new(tokio::sync::Mutex::new(ShareSessionCache::new())),
            pending_acks: Arc::new(RwLock::new(HashMap::new())),
            ack_state_generation: Arc::new(AtomicU64::new(0)),
            explicit_flush_retry_required: Arc::new(AtomicBool::new(false)),
            topic_ids: RwLock::new(HashMap::new()),
            recv_buffer: RwLock::new(VecDeque::new()),
            coordinator_id: RwLock::new(None),
            coordinator_address: RwLock::new(None),
            unacked_offsets: Arc::new(RwLock::new(HashSet::new())),
            heartbeat_task: SyncMutex::new(None),
            wakeup_flag: AtomicBool::new(false),
        })))
    }

    /// Subscribe to topics.
    ///
    /// Replaces the current subscription. The coordinator is notified on
    /// the next heartbeat (during `poll()`).
    pub async fn subscribe(&self, topics: &[&str]) -> Result<()> {
        if self.0.closed.load(Ordering::SeqCst) {
            return Err(KrafkaError::invalid_state("share consumer is closed"));
        }

        {
            let mut subs = self.0.subscriptions.write().await;
            subs.clear();
            for topic in topics {
                subs.insert((*topic).to_string());
            }
        }

        let topic_refs: Vec<&str> = topics.to_vec();
        self.0
            .metadata
            .refresh_for_topics(Some(&topic_refs))
            .await?;

        // Resolve topic UUIDs from metadata.
        {
            let mut ids = self.0.topic_ids.write().await;
            for topic in topics {
                if let Some(uuid) = self.0.metadata.topic_id_for_name(topic) {
                    ids.insert((*topic).to_string(), uuid);
                }
            }
        }

        // Discover the coordinator and send the initial heartbeat.
        self.ensure_coordinator().await?;
        self.send_heartbeat(true).await?;

        // Spawn the background heartbeat task if not already running.
        // The task sends periodic heartbeats independent of poll() so the
        // share-group session stays alive even when poll() is slow.
        let mut task_guard = self
            .0
            .heartbeat_task
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if task_guard.as_ref().is_none_or(|h| h.is_finished()) {
            let bg = self.clone();
            *task_guard = Some(tokio::spawn(async move {
                bg.run_heartbeat_loop().await;
            }));
        }
        drop(task_guard);

        debug!(
            "Subscribed to {} topic(s) in share group '{}'",
            topics.len(),
            self.0.config.group_id
        );
        Ok(())
    }

    /// Returns the current subscription.
    pub async fn subscription(&self) -> HashSet<String> {
        self.0.subscriptions.read().await.clone()
    }

    /// Returns the current partition assignments.
    pub async fn assignment(&self) -> HashMap<String, Vec<PartitionId>> {
        self.0.assignments.read().await.clone()
    }

    /// Returns the member ID assigned by the coordinator.
    pub fn member_id(&self) -> String {
        (**self.0.member_id.load()).clone()
    }

    /// Returns the current member epoch.
    pub fn member_epoch(&self) -> i32 {
        self.0.member_epoch.load(Ordering::Acquire)
    }

    /// Poll for new records.
    ///
    /// In implicit acknowledgement mode, previously fetched records are
    /// automatically accepted. In explicit mode, all records from the
    /// previous poll must be acknowledged before calling this again.
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        if self.0.closed.load(Ordering::SeqCst) {
            return Err(KrafkaError::invalid_state("share consumer is closed"));
        }

        // Clear and check the wakeup flag before doing any work so callers
        // get an immediate error if wakeup() was called before this poll.
        if self.0.wakeup_flag.swap(false, Ordering::AcqRel) {
            return Err(KrafkaError::invalid_state("wakeup() was called"));
        }

        // Explicit mode: reject poll if records from the previous batch are unacknowledged.
        if self.0.config.acknowledgement_mode == AcknowledgementMode::Explicit {
            let unacked = self.0.unacked_offsets.read().await;
            if !unacked.is_empty() {
                return Err(KrafkaError::invalid_state(
                    "all records from the previous poll() must be acknowledged before calling poll() again",
                ));
            }
            if self.0.explicit_flush_retry_required.load(Ordering::SeqCst) {
                return Err(KrafkaError::invalid_state(
                    "the previous commit_sync()/commit_async() flush failed; retry the commit before calling poll() again",
                ));
            }
        }

        // If recv_buffer is at capacity, skip only the fetch step (after
        // heartbeat/coordination) so group membership remains healthy.
        let max_buffered = self.0.config.max_buffered_records;
        let skip_fetch_due_to_buffer_cap = if max_buffered > 0 {
            let buf_len = self.0.recv_buffer.read().await.len();
            buf_len >= max_buffered as usize
        } else {
            false
        };

        // Send heartbeat to maintain membership and receive assignments.
        // Cap the heartbeat RPC at 10 s so a slow/stuck coordinator does not
        // block the entire poll() for the full connection-level request_timeout.
        let heartbeat_result =
            tokio::time::timeout(Duration::from_secs(10), self.send_heartbeat(false)).await;
        let heartbeat_err = match heartbeat_result {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(e),
            Err(_elapsed) => Some(KrafkaError::timeout("share group heartbeat")),
        };
        if let Some(e) = heartbeat_err {
            if let KrafkaError::Broker {
                code: ErrorCode::FencedMemberEpoch,
                ..
            } = &e
            {
                warn!(
                    "Heartbeat fenced during poll for group '{}'; resetting member state",
                    self.0.config.group_id
                );
                self.0.member_epoch.store(0, Ordering::Release);
                self.clear_ack_state().await;
            }
            warn!("Heartbeat failed during poll: {e}");
            self.invalidate_coordinator().await;
            if let Err(e2) = self.ensure_coordinator().await {
                warn!("Coordinator rediscovery failed: {e2}");
            }
        }

        // If `recv()` previously buffered records, return them first so mixed
        // `recv()`/`poll()` callers do not strand available data.
        {
            let mut buffered = self.0.recv_buffer.write().await;
            if !buffered.is_empty() {
                // max_poll_records is validated >= 1 in the builder, so take >= 1.
                let take = (self.0.config.max_poll_records as usize).min(buffered.len());
                return Ok(buffered.drain(..take).collect());
            }
        }

        let assignments = self.0.assignments.read().await.clone();
        if assignments.is_empty() || skip_fetch_due_to_buffer_cap {
            return Ok(Vec::new());
        }

        // Group partitions by leader broker.
        let mut partitions_by_broker: HashMap<BrokerId, Vec<(String, PartitionId, [u8; 16])>> =
            HashMap::new();
        let topic_ids = self.0.topic_ids.read().await;
        for (topic, partitions) in &assignments {
            let Some(&topic_id) = topic_ids.get(topic) else {
                debug!("No topic UUID for '{topic}', skipping");
                continue;
            };
            for &partition in partitions {
                if let Some(leader) = self.0.metadata.leader(topic, partition) {
                    partitions_by_broker.entry(leader).or_default().push((
                        topic.clone(),
                        partition,
                        topic_id,
                    ));
                }
            }
        }
        drop(topic_ids);

        let ack_state_generation = self.0.ack_state_generation.load(Ordering::SeqCst);

        let sendable_ack_partitions: HashSet<(String, PartitionId)> = partitions_by_broker
            .values()
            .flat_map(|partitions| {
                partitions
                    .iter()
                    .map(|(topic, partition, _)| (topic.clone(), *partition))
            })
            .collect();

        // Drain acknowledgement batches to piggyback on fetch requests.
        // pending_acks is already broker-keyed: pre-routed acks pass through
        // the sendable filter only; the UNROUTED shard is also re-routed here.
        let mut failed_piggyback_acks: Vec<PendingAck> = Vec::new();
        let pre_routed: BrokerPendingAcks = {
            let mut pending = self.0.pending_acks.write().await;
            std::mem::take(&mut *pending)
        };
        let unrouted = pre_routed
            .get(&UNROUTED_BROKER_ID)
            .map(|m| m.len())
            .unwrap_or(0);
        let mut ack_batches_by_broker: BrokerPendingAcks =
            HashMap::with_capacity(pre_routed.len().saturating_sub(if unrouted > 0 {
                1
            } else {
                0
            }));
        for (broker_id, partition_acks) in pre_routed {
            if broker_id == UNROUTED_BROKER_ID {
                // Re-route using current metadata; topic name is in the ack itself.
                for ((topic_id, partition), acks) in partition_acks {
                    let topic = acks.first().map(|a| a.topic.as_str()).unwrap_or("");
                    if sendable_ack_partitions.contains(&(topic.to_string(), partition)) {
                        if let Some(bid) = self.0.metadata.leader(topic, partition) {
                            ack_batches_by_broker
                                .entry(bid)
                                .or_default()
                                .entry((topic_id, partition))
                                .or_default()
                                .extend(acks);
                        } else {
                            failed_piggyback_acks.extend(acks);
                        }
                    } else {
                        failed_piggyback_acks.extend(acks);
                    }
                }
            } else {
                // Already routed; apply sendable filter only (handles leadership
                // changes between acknowledge() and poll()).
                for ((topic_id, partition), acks) in partition_acks {
                    let topic = acks.first().map(|a| a.topic.as_str()).unwrap_or("");
                    if sendable_ack_partitions.contains(&(topic.to_string(), partition)) {
                        ack_batches_by_broker
                            .entry(broker_id)
                            .or_default()
                            .entry((topic_id, partition))
                            .or_default()
                            .extend(acks);
                    } else {
                        failed_piggyback_acks.extend(acks);
                    }
                }
            }
        }

        // Fetch from all brokers concurrently.
        let mut fetch_tasks = Vec::with_capacity(partitions_by_broker.len());
        let member_id = (**self.0.member_id.load()).clone();
        let group_id = self.0.config.group_id.clone();
        let current_ack_state_generation = self.0.ack_state_generation.clone();

        for (broker_id, partitions) in &partitions_by_broker {
            let session_epoch = {
                let mut sessions = self.0.share_sessions.lock().await;
                sessions.get_or_create(*broker_id).epoch()
            };

            // Build per-topic partition requests with piggybacked acks.
            let mut topics_map: HashMap<[u8; 16], Vec<ShareFetchPartition>> = HashMap::new();
            let broker_ack_partitions = ack_batches_by_broker.get(broker_id);
            for (_, partition, topic_id) in partitions {
                let ack_batches_for_partition: Vec<ShareAcknowledgementBatch> =
                    broker_ack_partitions
                        .and_then(|partition_acks| partition_acks.get(&(*topic_id, *partition)))
                        .map(|partition_acks| {
                            partition_acks
                                .iter()
                                .map(|a| ShareAcknowledgementBatch {
                                    first_offset: a.first_offset,
                                    last_offset: a.last_offset,
                                    acknowledge_types: vec![a.ack_type],
                                })
                                .collect()
                        })
                        .unwrap_or_default();

                topics_map
                    .entry(*topic_id)
                    .or_default()
                    .push(ShareFetchPartition {
                        partition_index: *partition,
                        acknowledgement_batches: ack_batches_for_partition,
                    });
            }

            let topics: Vec<ShareFetchTopic> = topics_map
                .into_iter()
                .map(|(topic_id, partitions)| ShareFetchTopic {
                    topic_id,
                    partitions,
                })
                .collect();

            let request = ShareFetchRequest {
                group_id: Some(group_id.clone()),
                member_id: Some(member_id.clone()),
                share_session_epoch: session_epoch,
                max_wait_ms: timeout.as_millis().min(i32::MAX as u128) as i32,
                min_bytes: self.0.config.fetch_min_bytes,
                max_bytes: self.0.config.fetch_max_bytes,
                max_records: self.0.config.max_records,
                batch_size: self.0.config.batch_size,
                topics,
                forgotten_topics: Vec::new(),
            };

            let bid = *broker_id;
            let metadata = self.0.metadata.clone();
            let pool = self.0.pool.clone();
            let current_ack_state_generation = current_ack_state_generation.clone();
            let task = tokio::spawn(async move {
                ShareConsumer::ensure_ack_state_current(
                    current_ack_state_generation.as_ref(),
                    ack_state_generation,
                )?;

                let broker_addr = metadata
                    .broker(bid)
                    .map(|b| b.address().to_string())
                    .ok_or_else(|| {
                        KrafkaError::invalid_state(format!("broker {bid} not found in metadata"))
                    })?;
                let conn = pool.get_connection_by_id(bid, &broker_addr).await?;
                let version = conn
                    .negotiate_api_version(
                        ApiKey::ShareFetch,
                        versions::SHARE_FETCH_MAX,
                        versions::SHARE_FETCH_MIN,
                    )
                    .await
                    .ok_or_else(|| {
                        KrafkaError::protocol_kind(
                            ProtocolErrorKind::UnknownApiVersion,
                            "broker does not support ShareFetch",
                        )
                    })?;

                ShareConsumer::ensure_ack_state_current(
                    current_ack_state_generation.as_ref(),
                    ack_state_generation,
                )?;

                let buf = conn
                    .send_request(ApiKey::ShareFetch, version, |buf| match version {
                        2 => request.encode_v2(buf, 0, false),
                        _ => request.encode_v1(buf),
                    })
                    .await?;

                let response = crate::protocol::ShareFetchResponse::decode_versioned(
                    version,
                    &mut buf.as_ref(),
                )?;

                // KIP-219: honour broker-reported throttle time.
                conn.notify_throttle(response.throttle_time_ms);

                Result::<(BrokerId, crate::protocol::ShareFetchResponse)>::Ok((bid, response))
            });
            fetch_tasks.push((bid, task));
        }

        // Collect results from all brokers.
        let mut all_records = Vec::new();
        let topic_ids_guard = self.0.topic_ids.read().await;

        for (broker_id, task) in fetch_tasks {
            match task.await {
                Ok(Ok((_, response))) => {
                    let mut broker_acks =
                        ack_batches_by_broker.remove(&broker_id).unwrap_or_default();

                    if !response.error_code.is_ok() {
                        failed_piggyback_acks.extend(flatten_partition_acks(broker_acks));
                        warn!(
                            "ShareFetch to broker {broker_id} returned {:?}: {}",
                            response.error_code,
                            response.error_message.as_deref().unwrap_or("unknown error")
                        );
                        let mut sessions = self.0.share_sessions.lock().await;
                        sessions.reset_broker(broker_id);
                        continue;
                    }

                    // Update session state on success.
                    {
                        let mut sessions = self.0.share_sessions.lock().await;
                        sessions.get_or_create(broker_id).on_success();
                    }

                    // Decode records from the response and restore only the
                    // partitions whose piggybacked acknowledgements failed.
                    for topic_response in &response.responses {
                        let topic_name = if let Some(name) =
                            self.0.metadata.topic_name_for_id(&topic_response.topic_id)
                        {
                            name
                        } else {
                            let found = topic_ids_guard.iter().find_map(|(name, &id)| {
                                if id == topic_response.topic_id {
                                    Some(name.clone())
                                } else {
                                    None
                                }
                            });
                            match found {
                                Some(name) => name,
                                None => {
                                    debug!(
                                        "Unknown topic UUID {:?} in ShareFetch response, skipping",
                                        topic_response.topic_id
                                    );
                                    continue;
                                }
                            }
                        };

                        for partition_response in &topic_response.partitions {
                            let partition_acks = drain_broker_partition_acks(
                                &mut broker_acks,
                                topic_response.topic_id,
                                partition_response.partition_index,
                            );

                            if !partition_response.error_code.is_ok() {
                                failed_piggyback_acks.extend(partition_acks);
                                warn!(
                                    "ShareFetch error for {topic_name}-{}: {:?}",
                                    partition_response.partition_index,
                                    partition_response.error_code
                                );
                                continue;
                            }

                            if !partition_response.acknowledge_error_code.is_ok() {
                                failed_piggyback_acks.extend(partition_acks);
                                warn!(
                                    "Piggybacked ShareFetch acknowledge error for {topic_name}-{}: {:?}: {}",
                                    partition_response.partition_index,
                                    partition_response.acknowledge_error_code,
                                    partition_response
                                        .acknowledge_error_message
                                        .as_deref()
                                        .unwrap_or("unknown error")
                                );
                                continue;
                            }

                            // Build delivery count map from acquired_records.
                            let mut delivery_counts: HashMap<Offset, i16> = HashMap::new();
                            for acquired in &partition_response.acquired_records {
                                for offset in acquired.first_offset..=acquired.last_offset {
                                    delivery_counts.insert(offset, acquired.delivery_count);
                                }
                            }

                            // Decode record batches.
                            if let Some(ref raw) = partition_response.records {
                                let mut cursor = raw.as_ref();
                                while !cursor.is_empty() {
                                    match RecordBatch::decode_with_limit(
                                        &mut cursor,
                                        self.0.config.max_decompressed_size,
                                    ) {
                                        Ok(batch) => {
                                            for record in batch.records {
                                                let record_offset =
                                                    batch.base_offset + record.offset_delta as i64;
                                                let delivery_count =
                                                    delivery_counts.get(&record_offset).copied();
                                                all_records.push(ConsumerRecord {
                                                    topic: topic_name.clone(),
                                                    partition: partition_response.partition_index,
                                                    offset: record_offset,
                                                    timestamp: batch
                                                        .base_timestamp
                                                        .saturating_add(record.timestamp_delta),
                                                    timestamp_type: batch.attributes.timestamp_type
                                                        as i8,
                                                    key: record.key,
                                                    value: record.value,
                                                    headers: record
                                                        .headers
                                                        .into_iter()
                                                        .map(|h| (h.key, h.value))
                                                        .collect(),
                                                    leader_epoch: None,
                                                    delivery_count,
                                                });
                                            }
                                        }
                                        Err(e) => {
                                            debug!(
                                                "Failed to decode record batch for {topic_name}-{}: {e}",
                                                partition_response.partition_index
                                            );
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    failed_piggyback_acks.extend(flatten_partition_acks(broker_acks));
                }
                Ok(Err(e)) => {
                    failed_piggyback_acks
                        .extend(drain_broker_acks(&mut ack_batches_by_broker, broker_id));
                    warn!("ShareFetch to broker {broker_id} failed: {e}");
                }
                Err(e) => {
                    failed_piggyback_acks
                        .extend(drain_broker_acks(&mut ack_batches_by_broker, broker_id));
                    warn!(
                        "ShareFetch task for broker {broker_id} {}: {e}",
                        describe_share_fetch_join_error(&e)
                    );
                }
            }
        }
        drop(topic_ids_guard);

        failed_piggyback_acks.extend(
            ack_batches_by_broker
                .drain()
                .flat_map(|(_, acks)| flatten_partition_acks(acks))
                .collect::<Vec<_>>(),
        );
        self.restore_pending_acks(ack_state_generation, failed_piggyback_acks, false)
            .await;

        // In implicit mode, queue all fetched records as coalesced accepts for next poll.
        if self.0.config.acknowledgement_mode == AcknowledgementMode::Implicit {
            let ids = self.0.topic_ids.read().await;
            let mut pending = self.0.pending_acks.write().await;
            Self::coalesce_implicit_acks(&all_records, &ids, &mut pending, &self.0.metadata);
        }

        // In explicit mode, track all returned records as unacknowledged.
        if self.0.config.acknowledgement_mode == AcknowledgementMode::Explicit {
            let mut unacked = self.0.unacked_offsets.write().await;
            for record in &all_records {
                unacked.insert((record.topic.clone(), record.partition, record.offset));
            }
        }

        // Truncate to max_poll_records (validated >= 1 in the builder).
        let max = self.0.config.max_poll_records as usize;
        all_records.truncate(max);

        Ok(all_records)
    }

    /// Acknowledge a record with the given type (explicit mode only).
    ///
    /// In explicit acknowledgement mode, call this for each record before
    /// calling [`commit_sync()`](Self::commit_sync). All records from the
    /// previous `poll()` must be acknowledged before calling `poll()` again.
    pub async fn acknowledge(
        &self,
        record: &ConsumerRecord,
        ack_type: AcknowledgeType,
    ) -> Result<()> {
        if self.0.config.acknowledgement_mode != AcknowledgementMode::Explicit {
            return Err(KrafkaError::invalid_state(
                "acknowledge() requires explicit acknowledgement mode",
            ));
        }

        let topic_ids = self.0.topic_ids.read().await;
        let topic_id = topic_ids.get(&record.topic).copied().ok_or_else(|| {
            KrafkaError::invalid_state(format!("no topic UUID for '{}'", record.topic))
        })?;
        drop(topic_ids);

        let record_key = (record.topic.clone(), record.partition, record.offset);
        let mut pending = self.0.pending_acks.write().await;
        let mut unacked = self.0.unacked_offsets.write().await;
        if !unacked.contains(&record_key) {
            return Err(KrafkaError::invalid_state(format!(
                "record {}-{}@{} is not pending acknowledgement",
                record.topic, record.partition, record.offset
            )));
        }
        // Route the ack to the current partition leader so poll() can piggyback
        // it on the correct broker's ShareFetch without re-scanning the whole map.
        // Fall back to UNROUTED_BROKER_ID when the leader is not yet in metadata;
        // poll() will re-route it using fresh metadata.
        let broker_id = self
            .0
            .metadata
            .leader(&record.topic, record.partition)
            .unwrap_or(UNROUTED_BROKER_ID);
        pending
            .entry(broker_id)
            .or_default()
            .entry((topic_id, record.partition))
            .or_default()
            .push(PendingAck {
                topic: record.topic.clone(),
                topic_id,
                partition: record.partition,
                first_offset: record.offset,
                last_offset: record.offset,
                ack_type: ack_type.to_i8(),
            });
        unacked.remove(&record_key);

        Ok(())
    }

    async fn restore_pending_acks(
        &self,
        ack_state_generation: u64,
        mut acks: Vec<PendingAck>,
        require_explicit_retry: bool,
    ) {
        Self::restore_ack_state(
            self.0.ack_state_generation.as_ref(),
            self.0.pending_acks.as_ref(),
            self.0.explicit_flush_retry_required.as_ref(),
            ack_state_generation,
            require_explicit_retry,
            &mut acks,
        )
        .await;
    }

    async fn restore_ack_state(
        current_generation: &AtomicU64,
        pending_acks: &RwLock<BrokerPendingAcks>,
        explicit_flush_retry_required: &AtomicBool,
        ack_state_generation: u64,
        require_explicit_retry: bool,
        acks: &mut Vec<PendingAck>,
    ) {
        if acks.is_empty() {
            return;
        }

        let mut pending = pending_acks.write().await;
        if current_generation.load(Ordering::SeqCst) != ack_state_generation {
            acks.clear();
            return;
        }
        if require_explicit_retry {
            explicit_flush_retry_required.store(true, Ordering::SeqCst);
        }
        // Re-queue under UNROUTED_BROKER_ID; poll() will re-route using fresh
        // metadata on the next call (handles leadership changes after failure).
        for ack in acks.drain(..) {
            pending
                .entry(UNROUTED_BROKER_ID)
                .or_default()
                .entry((ack.topic_id, ack.partition))
                .or_default()
                .push(ack);
        }
    }

    fn share_acknowledge_response_error(
        response: &crate::protocol::ShareAcknowledgeResponse,
    ) -> Option<KrafkaError> {
        if !response.error_code.is_ok() {
            return Some(KrafkaError::broker(
                response.error_code,
                response
                    .error_message
                    .clone()
                    .unwrap_or_else(|| "ShareAcknowledge failed".to_string()),
            ));
        }

        for topic_response in &response.responses {
            for part_response in &topic_response.partitions {
                if !part_response.error_code.is_ok() {
                    return Some(KrafkaError::broker(
                        part_response.error_code,
                        part_response.error_message.clone().unwrap_or_else(|| {
                            format!(
                                "ShareAcknowledge error for partition {}",
                                part_response.partition_index
                            )
                        }),
                    ));
                }
            }
        }

        None
    }

    /// Commit all pending acknowledgements synchronously.
    ///
    /// Sends a `ShareAcknowledge` request for all outstanding acknowledgements.
    /// In implicit mode, this flushes any buffered accepts.
    pub async fn commit_sync(&self) -> Result<()> {
        if self.0.closed.load(Ordering::SeqCst) {
            return Err(KrafkaError::invalid_state("share consumer is closed"));
        }

        self.flush_pending_acks().await
    }

    async fn flush_pending_acks(&self) -> Result<()> {
        let ack_state_generation = self.0.ack_state_generation.load(Ordering::SeqCst);

        // If there are acks in the UNROUTED shard (no leader known at
        // acknowledge() time), refresh metadata first so that
        // `send_share_acknowledge_with_state` can resolve their leaders.
        // This avoids an immediate "no leader for topic-partition" error on
        // the very first `commit_sync()` call after a fetch that piggybacked
        // the acks but hadn't yet routed them.
        //
        // IMPORTANT: collect the topic names while holding the read lock, then
        // drop the lock *before* the async metadata refresh.  Holding an async
        // RwLock guard across an await point stalls every writer that needs
        // `pending_acks.write()` (including the `std::mem::take` below).
        let unrouted_topics: HashSet<String> = {
            let pending = self.0.pending_acks.read().await;
            pending
                .get(&UNROUTED_BROKER_ID)
                .into_iter()
                .flat_map(|m| m.values().flatten())
                .map(|ack| ack.topic.clone())
                .collect()
        }; // read lock released here
        if !unrouted_topics.is_empty() {
            let topic_refs: Vec<&str> = unrouted_topics.iter().map(String::as_str).collect();
            if let Err(err) = self.0.metadata.refresh_for_topics(Some(&topic_refs)).await {
                warn!(
                    error = %err,
                    "commit_sync: metadata refresh for unrouted acks failed; \
                     commit may fail with a leader-not-found error"
                );
            }
        }

        let acks: Vec<PendingAck> = {
            let mut pending = self.0.pending_acks.write().await;
            std::mem::take(&mut *pending)
                .into_values()
                .flat_map(|broker_acks| broker_acks.into_values().flatten())
                .collect()
        };

        if acks.is_empty() {
            return Ok(());
        }

        match self.send_share_acknowledge(&acks).await {
            Ok(()) => {
                self.0
                    .explicit_flush_retry_required
                    .store(false, Ordering::SeqCst);
                Ok(())
            }
            Err(error) => {
                self.restore_pending_acks(ack_state_generation, acks, true)
                    .await;
                Err(error)
            }
        }
    }

    /// Commit all pending acknowledgements asynchronously.
    ///
    /// Await the returned handle to observe transport, decode, and broker
    /// errors. If the handle is dropped, the task continues in the background
    /// and its result is discarded.
    pub fn commit_async(&self) -> ShareCommitHandle {
        if self.0.closed.load(Ordering::SeqCst) {
            return ShareCommitHandle::ready(Err(KrafkaError::invalid_state(
                "share consumer is closed",
            )));
        }

        let member_id = (**self.0.member_id.load()).clone();

        let ack_state_generation = self.0.ack_state_generation.load(Ordering::SeqCst);
        let pending_acks = self.0.pending_acks.clone();
        let current_ack_state_generation = self.0.ack_state_generation.clone();
        let explicit_flush_retry_required = self.0.explicit_flush_retry_required.clone();
        let Ok(mut pending) = self.0.pending_acks.try_write() else {
            return ShareCommitHandle::ready(Err(KrafkaError::invalid_state(
                "commit_async: pending_acks lock contention",
            )));
        };
        let acks: Vec<PendingAck> = std::mem::take(&mut *pending)
            .into_values()
            .flat_map(|broker_acks| broker_acks.into_values().flatten())
            .collect();
        drop(pending);

        if acks.is_empty() {
            return ShareCommitHandle::ready(Ok(()));
        }

        let metadata = self.0.metadata.clone();
        let pool = self.0.pool.clone();
        let share_sessions = self.0.share_sessions.clone();
        let group_id = self.0.config.group_id.clone();
        let send_ack_state_generation = current_ack_state_generation.clone();

        ShareCommitHandle::Task(tokio::spawn(async move {
            let restore_acks = |mut acks: Vec<PendingAck>| {
                let pending_acks = pending_acks.clone();
                let current_ack_state_generation = current_ack_state_generation.clone();
                let explicit_flush_retry_required = explicit_flush_retry_required.clone();
                async move {
                    ShareConsumer::restore_ack_state(
                        current_ack_state_generation.as_ref(),
                        pending_acks.as_ref(),
                        explicit_flush_retry_required.as_ref(),
                        ack_state_generation,
                        true,
                        &mut acks,
                    )
                    .await;
                }
            };
            if let Err(error) = ShareConsumer::send_share_acknowledge_with_state(
                ShareAcknowledgeContext {
                    metadata,
                    pool,
                    share_sessions,
                    group_id,
                    member_id,
                    current_ack_state_generation: send_ack_state_generation,
                    ack_state_generation,
                },
                &acks,
            )
            .await
            {
                restore_acks(acks).await;
                return Err(error);
            }

            explicit_flush_retry_required.store(false, Ordering::SeqCst);

            Ok(())
        }))
    }

    /// Receive a single record (convenience wrapper over `poll()`).
    ///
    /// Buffers records internally; returns `None` when the consumer is closed.
    pub async fn recv(&self) -> Result<Option<ConsumerRecord>> {
        // Return from buffer first.
        {
            let mut buf = self.0.recv_buffer.write().await;
            if let Some(record) = buf.pop_front() {
                return Ok(Some(record));
            }
        }

        if self.0.closed.load(Ordering::SeqCst) {
            return Ok(None);
        }

        let records = self.poll(Duration::from_secs(1)).await?;
        if records.is_empty() {
            return Ok(None);
        }

        let mut buf = self.0.recv_buffer.write().await;
        let mut iter = records.into_iter();
        let first = iter.next();
        // Preserve overflow records so recv() never drops data.
        for record in iter {
            buf.push_back(record);
        }

        Ok(first)
    }

    /// Create an async stream of records.
    pub fn stream(&self) -> ShareConsumerStream<'_> {
        ShareConsumerStream::new(self)
    }

    /// Unsubscribe from all topics.
    ///
    /// Sends a leave heartbeat (member_epoch = -1) and clears state.
    pub async fn unsubscribe(&self) {
        // Stop the background heartbeat task before leaving the group.
        if let Some(handle) = self
            .0
            .heartbeat_task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            handle.abort();
        }

        // Leave group via heartbeat with epoch -1.
        if let Err(e) = self.leave_group().await {
            warn!("Leave group failed during unsubscribe: {e}");
        }

        self.0.subscriptions.write().await.clear();
        self.0.assignments.write().await.clear();
        self.clear_partition_state().await;
        self.0
            .member_id
            .store(Arc::new(crate::util::random_uuid_v4()));
        self.0.member_epoch.store(0, Ordering::Release);

        debug!("Unsubscribed from share group '{}'", self.0.config.group_id);
    }

    /// Close the share consumer.
    ///
    /// In implicit mode, unreleased records are released (not accepted) so
    /// they become available for other consumers. In explicit mode, pending
    /// acks are flushed. Leaves the group and closes all connections.
    /// Idempotent.
    ///
    /// Returns the first cleanup error after local state and connections have
    /// still been closed.
    pub async fn close(&self) -> Result<()> {
        if self.0.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        // Stop the background heartbeat task immediately so it does not
        // race with the leave-group heartbeat sent below.
        if let Some(handle) = self
            .0
            .heartbeat_task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            handle.abort();
        }

        // In implicit mode, convert pending accepts to releases so acquired
        // records are returned to the pool for redelivery (KIP-932 §close).
        if self.0.config.acknowledgement_mode == AcknowledgementMode::Implicit {
            let mut pending = self.0.pending_acks.write().await;
            for broker_acks in pending.values_mut() {
                for acks in broker_acks.values_mut() {
                    for ack in acks.iter_mut() {
                        ack.ack_type = AcknowledgeType::Release.to_i8();
                    }
                }
            }
        }

        let commit_result = self.flush_pending_acks().await;

        // Send FINAL_EPOCH to all established share sessions so brokers release
        // server-side session state immediately rather than waiting for timeout.
        self.close_share_sessions().await;

        // Leave the group.
        let leave_result = self.leave_group().await;

        // Clear state.
        self.0.subscriptions.write().await.clear();
        self.0.assignments.write().await.clear();
        self.clear_partition_state().await;

        self.0.pool.close_all().await;

        info!("ShareConsumer closed (group '{}')", self.0.config.group_id);

        commit_result?;
        leave_result
    }

    /// Returns true if the consumer has been closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::SeqCst)
    }

    /// Interrupt an in-progress [`poll()`](Self::poll) call from another thread or task.
    ///
    /// Sets a wakeup flag that causes the next pending `poll()` to return
    /// `Err(KrafkaError::WakeupCalled)` immediately without fetching records.
    /// The consumer remains usable — subsequent `poll()` calls proceed normally.
    ///
    /// This is safe to call concurrently with any other consumer method.
    #[inline]
    pub fn wakeup(&self) {
        self.0.wakeup_flag.store(true, Ordering::Release);
    }

    /// Close the consumer with a per-phase timeout.
    ///
    /// Equivalent to [`close()`](Self::close) but each cleanup phase
    /// (ack flush, leave-group) is individually limited to `timeout / 2`.
    /// Any cleanup error is returned after local state has been released.
    /// Idempotent.
    pub async fn close_with_timeout(&self, timeout: Duration) -> Result<()> {
        if self.0.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        if let Some(handle) = self
            .0
            .heartbeat_task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            handle.abort();
        }

        if self.0.config.acknowledgement_mode == AcknowledgementMode::Implicit {
            let mut pending = self.0.pending_acks.write().await;
            for broker_acks in pending.values_mut() {
                for acks in broker_acks.values_mut() {
                    for ack in acks.iter_mut() {
                        ack.ack_type = AcknowledgeType::Release.to_i8();
                    }
                }
            }
        }

        let phase = timeout / 2;
        let commit_result = tokio::time::timeout(phase, self.flush_pending_acks())
            .await
            .unwrap_or_else(|_| Err(KrafkaError::timeout("ack flush timed out during close")));

        self.close_share_sessions().await;

        let leave_result = tokio::time::timeout(phase, self.leave_group())
            .await
            .unwrap_or_else(|_| Err(KrafkaError::timeout("leave-group timed out during close")));

        self.0.subscriptions.write().await.clear();
        self.0.assignments.write().await.clear();
        self.clear_partition_state().await;

        self.0.pool.close_all().await;

        info!(
            "ShareConsumer closed with timeout (group '{}')",
            self.0.config.group_id
        );

        commit_result?;
        leave_result
    }

    /// Flush all pending explicit-mode acknowledgements synchronously with a timeout.
    ///
    /// Equivalent to [`commit_sync()`](Self::commit_sync) but bounded by `timeout`.
    /// Returns `Err(KrafkaError::Timeout)` if the flush does not complete in time.
    pub async fn commit_sync_with_timeout(&self, timeout: Duration) -> Result<()> {
        tokio::time::timeout(timeout, self.commit_sync())
            .await
            .unwrap_or_else(|_| Err(KrafkaError::timeout("commit_sync timed out")))
    }

    /// Acknowledge a record by topic, partition, and offset directly.
    ///
    /// Use this when the record could not be deserialized but still needs to be
    /// acknowledged to prevent indefinite redelivery. The record must have been
    /// delivered in the current poll session.
    ///
    /// Returns `Err` if the record was not found in the unacknowledged set.
    pub async fn acknowledge_by_offset(
        &self,
        topic: &str,
        partition: PartitionId,
        offset: Offset,
        ack_type: AcknowledgeType,
    ) -> Result<()> {
        if self.0.config.acknowledgement_mode != AcknowledgementMode::Explicit {
            return Err(KrafkaError::invalid_state(
                "acknowledge_by_offset() requires explicit acknowledgement mode",
            ));
        }

        let record_key: RecordKey = (topic.to_string(), partition, offset);
        let unacked = self.0.unacked_offsets.read().await;
        if !unacked.contains(&record_key) {
            return Err(KrafkaError::invalid_state(format!(
                "record {topic}-{partition}@{offset} is not pending acknowledgement"
            )));
        }
        drop(unacked);

        let topic_ids = self.0.topic_ids.read().await;
        let topic_id = topic_ids
            .get(topic)
            .copied()
            .ok_or_else(|| KrafkaError::invalid_state(format!("no topic UUID for '{topic}'")))?;
        drop(topic_ids);

        let broker_id = self
            .0
            .metadata
            .leader(topic, partition)
            .unwrap_or(UNROUTED_BROKER_ID);

        let mut pending = self.0.pending_acks.write().await;
        let mut unacked = self.0.unacked_offsets.write().await;
        pending
            .entry(broker_id)
            .or_default()
            .entry((topic_id, partition))
            .or_default()
            .push(PendingAck {
                topic: topic.to_string(),
                topic_id,
                partition,
                first_offset: offset,
                last_offset: offset,
                ack_type: ack_type.to_i8(),
            });
        unacked.remove(&record_key);

        Ok(())
    }

    /// Get the shared connection metrics handle used by this share consumer's broker pool.
    #[inline]
    pub fn connection_metrics(&self) -> Arc<ConnectionMetrics> {
        self.0.pool.metrics()
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    fn invalidate_ack_state(&self) {
        self.0.ack_state_generation.fetch_add(1, Ordering::SeqCst);
        self.0
            .explicit_flush_retry_required
            .store(false, Ordering::SeqCst);
    }

    async fn clear_ack_state(&self) {
        self.invalidate_ack_state();
        self.0.pending_acks.write().await.clear();
        self.0.unacked_offsets.write().await.clear();
    }

    /// Clear all per-partition state. Called from `unsubscribe()` and `close()`.
    async fn clear_partition_state(&self) {
        self.clear_ack_state().await;
        self.0.recv_buffer.write().await.clear();
        self.0.share_sessions.lock().await.reset_all();
        *self.0.coordinator_id.write().await = None;
        *self.0.coordinator_address.write().await = None;
    }

    /// Invalidate the cached coordinator. The next `ensure_coordinator()` call
    /// will re-discover it. Called on NOT_COORDINATOR errors or heartbeat failures.
    async fn invalidate_coordinator(&self) {
        *self.0.coordinator_id.write().await = None;
        *self.0.coordinator_address.write().await = None;
    }

    /// Coalesce implicit accept acks — merge consecutive offsets for the same
    /// (topic, partition) into a single `PendingAck` with a contiguous range,
    /// pre-routed to the current partition leader.
    fn coalesce_implicit_acks(
        records: &[ConsumerRecord],
        topic_ids: &HashMap<String, [u8; 16]>,
        pending: &mut BrokerPendingAcks,
        metadata: &ClusterMetadata,
    ) {
        // Group by (topic, partition) and sort offsets.
        let mut by_tp: HashMap<(&str, PartitionId), Vec<Offset>> = HashMap::new();
        for record in records {
            by_tp
                .entry((&record.topic, record.partition))
                .or_default()
                .push(record.offset);
        }

        for ((topic, partition), mut offsets) in by_tp {
            let Some(&topic_id) = topic_ids.get(topic) else {
                continue;
            };
            offsets.sort_unstable();

            let broker_id = metadata
                .leader(topic, partition)
                .unwrap_or(UNROUTED_BROKER_ID);

            // Merge consecutive offsets into contiguous ranges.
            let mut i = 0;
            while i < offsets.len() {
                let first = offsets[i];
                let mut last = first;
                while i + 1 < offsets.len() && offsets[i + 1] == last + 1 {
                    i += 1;
                    last = offsets[i];
                }
                pending
                    .entry(broker_id)
                    .or_default()
                    .entry((topic_id, partition))
                    .or_default()
                    .push(PendingAck {
                        topic: topic.to_string(),
                        topic_id,
                        partition,
                        first_offset: first,
                        last_offset: last,
                        ack_type: AcknowledgeType::Accept.to_i8(),
                    });
                i += 1;
            }
        }
    }

    /// Build `ShareAcknowledgeTopic` list from pending acks. Groups by
    /// topic UUID and partition, coalescing ack batches per partition.
    fn build_acknowledge_topics(acks: &[PendingAck]) -> Vec<ShareAcknowledgeTopic> {
        let mut topics_map: HashMap<
            [u8; 16],
            HashMap<PartitionId, Vec<ShareAcknowledgementBatch>>,
        > = HashMap::new();
        for ack in acks {
            topics_map
                .entry(ack.topic_id)
                .or_default()
                .entry(ack.partition)
                .or_default()
                .push(ShareAcknowledgementBatch {
                    first_offset: ack.first_offset,
                    last_offset: ack.last_offset,
                    acknowledge_types: vec![ack.ack_type],
                });
        }

        topics_map
            .into_iter()
            .map(|(topic_id, partitions_map)| ShareAcknowledgeTopic {
                topic_id,
                partitions: partitions_map
                    .into_iter()
                    .map(
                        |(partition_index, acknowledgement_batches)| ShareAcknowledgePartition {
                            partition_index,
                            acknowledgement_batches,
                        },
                    )
                    .collect(),
            })
            .collect()
    }

    /// Discover the share group coordinator via FindCoordinator.
    async fn ensure_coordinator(&self) -> Result<()> {
        if self.0.coordinator_id.read().await.is_some() {
            return Ok(());
        }

        let brokers = self.0.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::invalid_state("no brokers available"));
        }

        // Try each broker until we find the coordinator.
        let request = FindCoordinatorRequest::for_group(&self.0.config.group_id);
        for broker in &brokers {
            let conn = match self
                .0
                .pool
                .get_connection_by_id(broker.id(), broker.address())
                .await
            {
                Ok(c) => c,
                Err(_) => continue,
            };

            let version = match conn
                .negotiate_api_version(
                    ApiKey::FindCoordinator,
                    versions::FIND_COORDINATOR_MAX,
                    versions::FIND_COORDINATOR_MIN,
                )
                .await
            {
                Some(v) => v,
                None => continue,
            };

            let result = conn
                .send_request(ApiKey::FindCoordinator, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await;

            let buf = match result {
                Ok(b) => b,
                Err(e) => {
                    debug!("FindCoordinator via broker {} failed: {e}", broker.id());
                    continue;
                }
            };

            let response = FindCoordinatorResponse::decode_versioned(version, &mut buf.as_ref())?;

            if response.error_code.is_ok() {
                let coord_id = response.node_id;
                let coord_addr = format!("{}:{}", response.host, response.port);
                *self.0.coordinator_id.write().await = Some(coord_id);
                *self.0.coordinator_address.write().await = Some(coord_addr);
                debug!(
                    "Share group '{}' coordinator is broker {coord_id}",
                    self.0.config.group_id
                );
                return Ok(());
            }

            debug!(
                "FindCoordinator returned {:?} for group '{}', trying next broker",
                response.error_code, self.0.config.group_id
            );
        }

        Err(KrafkaError::invalid_state(format!(
            "could not discover coordinator for share group '{}'",
            self.0.config.group_id
        )))
    }

    /// Send a ShareGroupHeartbeat to the coordinator.
    ///
    /// If `send_subscription` is true, the subscribed topic names are included.
    /// Returns the heartbeat response.
    async fn send_heartbeat(&self, send_subscription: bool) -> Result<()> {
        let coord_id = self
            .0
            .coordinator_id
            .read()
            .await
            .ok_or_else(|| KrafkaError::invalid_state("no coordinator discovered"))?;

        let member_id = (**self.0.member_id.load()).clone();
        let member_epoch = self.0.member_epoch.load(Ordering::Acquire);

        let subscribed_topic_names = if send_subscription {
            Some(
                self.0
                    .subscriptions
                    .read()
                    .await
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };

        let request = ShareGroupHeartbeatRequest {
            group_id: self.0.config.group_id.clone(),
            member_id: member_id.clone(),
            member_epoch,
            rack_id: self.0.config.client_rack.clone(),
            subscribed_topic_names,
        };

        let coord_addr = self
            .0
            .coordinator_address
            .read()
            .await
            .clone()
            .ok_or_else(|| KrafkaError::invalid_state("no coordinator address"))?;
        let conn = self
            .0
            .pool
            .get_connection_by_id(coord_id, &coord_addr)
            .await?;
        let version = conn
            .negotiate_api_version(
                ApiKey::ShareGroupHeartbeat,
                versions::SHARE_GROUP_HEARTBEAT_MAX,
                versions::SHARE_GROUP_HEARTBEAT_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "broker does not support ShareGroupHeartbeat",
                )
            })?;

        let buf = conn
            .send_request(ApiKey::ShareGroupHeartbeat, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let response = ShareGroupHeartbeatResponse::decode_versioned(version, &mut buf.as_ref())?;

        match response.error_code {
            ErrorCode::None => {}

            // The current node is no longer the coordinator. Invalidate the
            // cached coordinator so `ensure_coordinator()` will rediscover it.
            ErrorCode::NotCoordinator
            | ErrorCode::CoordinatorNotAvailable
            | ErrorCode::CoordinatorLoadInProgress => {
                self.invalidate_coordinator().await;
                return Err(KrafkaError::broker(
                    response.error_code,
                    response
                        .error_message
                        .unwrap_or_else(|| "ShareGroupHeartbeat failed".to_string()),
                ));
            }

            // The coordinator has advanced past our epoch. The caller is
            // responsible for resetting local member state.
            ErrorCode::FencedMemberEpoch => {
                return Err(KrafkaError::broker(
                    response.error_code,
                    response
                        .error_message
                        .unwrap_or_else(|| "member epoch fenced".to_string()),
                ));
            }

            other => {
                return Err(KrafkaError::broker(
                    other,
                    response
                        .error_message
                        .unwrap_or_else(|| "ShareGroupHeartbeat failed".to_string()),
                ));
            }
        }

        // Update member state from response.
        if let Some(new_member_id) = response.member_id {
            self.0.member_id.store(Arc::new(new_member_id));
        }
        self.0
            .member_epoch
            .store(response.member_epoch, Ordering::Release);
        // Clamp the broker-supplied heartbeat interval to [50 ms, 30 s] to
        // prevent excessively fast polling (which exhausts broker connections)
        // or excessively slow polling (which causes session timeouts).
        let raw_interval_ms = response.heartbeat_interval_ms;
        const HEARTBEAT_MIN_MS: i32 = 50;
        const HEARTBEAT_MAX_MS: i32 = 30_000;
        let clamped_interval_ms = raw_interval_ms.clamp(HEARTBEAT_MIN_MS, HEARTBEAT_MAX_MS);
        if clamped_interval_ms != raw_interval_ms {
            tracing::warn!(
                raw_ms = raw_interval_ms,
                clamped_ms = clamped_interval_ms,
                min_ms = HEARTBEAT_MIN_MS,
                max_ms = HEARTBEAT_MAX_MS,
                "broker heartbeat_interval_ms is out of safe range; clamping"
            );
        }
        self.0
            .heartbeat_interval_ms
            .store(clamped_interval_ms, Ordering::Release);

        // Process assignment if present.
        if let Some(assignment) = response.assignment {
            self.apply_assignment(&assignment).await;
        }

        Ok(())
    }

    /// Apply a partition assignment from the coordinator heartbeat response.
    async fn apply_assignment(&self, assignment: &[ShareGroupTopicPartitions]) {
        let mut new_assignments: HashMap<String, Vec<PartitionId>> = HashMap::new();
        let mut topic_ids_guard = self.0.topic_ids.write().await;

        for tp in assignment {
            // Resolve topic UUID to name.
            let topic_name = if let Some(name) = self.0.metadata.topic_name_for_id(&tp.topic_id) {
                topic_ids_guard.insert(name.clone(), tp.topic_id);
                name
            } else {
                // Cache miss — try a metadata refresh next time.
                debug!(
                    "Unknown topic UUID {:?} in share assignment, skipping",
                    tp.topic_id
                );
                continue;
            };

            new_assignments.insert(topic_name, tp.partitions.clone());
        }
        drop(topic_ids_guard);

        // Reset share sessions for brokers whose partitions changed.
        let old_assignments = self.0.assignments.read().await.clone();
        if old_assignments != new_assignments {
            debug!(
                "Share group assignment changed: {} topic(s), {} partition(s)",
                new_assignments.len(),
                new_assignments.values().map(|v| v.len()).sum::<usize>()
            );
            self.clear_ack_state().await;
            self.0.share_sessions.lock().await.reset_all();
        }

        *self.0.assignments.write().await = new_assignments;
    }

    /// Send a ShareAcknowledge request for pending acks.
    ///
    /// Routes acknowledgements to the correct partition leaders. Returns an
    /// error if any leader cannot be determined or any broker rejects the acks.
    async fn send_share_acknowledge(&self, acks: &[PendingAck]) -> Result<()> {
        let member_id = (**self.0.member_id.load()).clone();
        Self::send_share_acknowledge_with_state(
            ShareAcknowledgeContext {
                metadata: self.0.metadata.clone(),
                pool: self.0.pool.clone(),
                share_sessions: self.0.share_sessions.clone(),
                group_id: self.0.config.group_id.clone(),
                member_id,
                current_ack_state_generation: self.0.ack_state_generation.clone(),
                ack_state_generation: self.0.ack_state_generation.load(Ordering::SeqCst),
            },
            acks,
        )
        .await
    }

    async fn send_share_acknowledge_with_state(
        context: ShareAcknowledgeContext,
        acks: &[PendingAck],
    ) -> Result<()> {
        let ShareAcknowledgeContext {
            metadata,
            pool,
            share_sessions,
            group_id,
            member_id,
            current_ack_state_generation,
            ack_state_generation,
        } = context;

        Self::ensure_ack_state_current(
            current_ack_state_generation.as_ref(),
            ack_state_generation,
        )?;

        // Group acks by partition leader.
        let mut broker_acks: HashMap<BrokerId, Vec<&PendingAck>> = HashMap::new();

        for ack in acks {
            let broker_id = metadata.leader(&ack.topic, ack.partition).ok_or_else(|| {
                KrafkaError::invalid_state(format!(
                    "no leader for {}-{} in metadata",
                    ack.topic, ack.partition
                ))
            })?;
            broker_acks.entry(broker_id).or_default().push(ack);
        }

        for (broker_id, broker_ack_list) in &broker_acks {
            Self::ensure_ack_state_current(
                current_ack_state_generation.as_ref(),
                ack_state_generation,
            )?;

            let topics = Self::build_acknowledge_topics(
                &broker_ack_list
                    .iter()
                    .map(|a| (*a).clone())
                    .collect::<Vec<_>>(),
            );

            let session_epoch = {
                let sessions = share_sessions.lock().await;
                sessions
                    .get(*broker_id)
                    .map(|s: &session::ShareSessionState| s.epoch())
                    .unwrap_or(0)
            };

            let request = ShareAcknowledgeRequest {
                group_id: Some(group_id.clone()),
                member_id: Some(member_id.clone()),
                share_session_epoch: session_epoch,
                topics,
            };

            let broker_addr = metadata
                .broker(*broker_id)
                .map(|b| b.address().to_string())
                .ok_or_else(|| {
                    KrafkaError::invalid_state(format!(
                        "broker {} not found in metadata",
                        broker_id
                    ))
                })?;
            let conn = pool.get_connection_by_id(*broker_id, &broker_addr).await?;
            let version = conn
                .negotiate_api_version(
                    ApiKey::ShareAcknowledge,
                    versions::SHARE_ACKNOWLEDGE_MAX,
                    versions::SHARE_ACKNOWLEDGE_MIN,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::UnknownApiVersion,
                        "broker does not support ShareAcknowledge",
                    )
                })?;

            Self::ensure_ack_state_current(
                current_ack_state_generation.as_ref(),
                ack_state_generation,
            )?;

            let buf = conn
                .send_request(ApiKey::ShareAcknowledge, version, |buf| match version {
                    2 => request.encode_v2(buf, false),
                    _ => request.encode_v1(buf),
                })
                .await?;

            let response = crate::protocol::ShareAcknowledgeResponse::decode_versioned(
                version,
                &mut buf.as_ref(),
            )?;

            if let Some(error) = Self::share_acknowledge_response_error(&response) {
                return Err(error);
            }
        }

        Ok(())
    }

    fn ensure_ack_state_current(
        current_generation: &AtomicU64,
        ack_state_generation: u64,
    ) -> Result<()> {
        if current_generation.load(Ordering::SeqCst) == ack_state_generation {
            return Ok(());
        }

        Err(KrafkaError::invalid_state(
            "share acknowledgement state was invalidated",
        ))
    }

    /// Leave the share group via heartbeat with member_epoch = -1.
    async fn leave_group(&self) -> Result<()> {
        let coord_id = match *self.0.coordinator_id.read().await {
            Some(id) => id,
            None => return Ok(()),
        };

        let member_id = (**self.0.member_id.load()).clone();
        if member_id.is_empty() {
            return Ok(());
        }

        let request = ShareGroupHeartbeatRequest {
            group_id: self.0.config.group_id.clone(),
            member_id,
            member_epoch: -1, // Leave signal
            rack_id: None,
            subscribed_topic_names: None,
        };

        let coord_addr = match self.0.coordinator_address.read().await.clone() {
            Some(addr) => addr,
            None => return Ok(()),
        };

        let conn = self
            .0
            .pool
            .get_connection_by_id(coord_id, &coord_addr)
            .await?;

        let version = match conn
            .negotiate_api_version(
                ApiKey::ShareGroupHeartbeat,
                versions::SHARE_GROUP_HEARTBEAT_MAX,
                versions::SHARE_GROUP_HEARTBEAT_MIN,
            )
            .await
        {
            Some(v) => v,
            None => {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "broker does not support ShareGroupHeartbeat",
                ));
            }
        };

        let buf = conn
            .send_request(ApiKey::ShareGroupHeartbeat, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await;

        let response = ShareGroupHeartbeatResponse::decode_versioned(version, &mut buf?.as_ref())?;

        if !response.error_code.is_ok() {
            return Err(KrafkaError::broker(
                response.error_code,
                response
                    .error_message
                    .unwrap_or_else(|| "ShareGroupHeartbeat failed".to_string()),
            ));
        }

        debug!("Left share group '{}' successfully", self.0.config.group_id);

        self.invalidate_coordinator().await;
        Ok(())
    }

    /// Send `ShareFetch` with `share_session_epoch = FINAL_EPOCH` (-1) to each
    /// broker that has an established session, allowing the broker to release
    /// server-side session state immediately instead of waiting for timeout.
    ///
    /// This is a best-effort operation: errors are logged at `debug!` level
    /// and do not prevent the consumer from closing.
    async fn close_share_sessions(&self) {
        let broker_ids = {
            let sessions = self.0.share_sessions.lock().await;
            sessions.established_broker_ids()
        };
        if broker_ids.is_empty() {
            return;
        }

        let member_id = (**self.0.member_id.load()).clone();
        let group_id = &self.0.config.group_id;

        for broker_id in broker_ids {
            let broker_addr = match self.0.metadata.broker(broker_id) {
                Some(b) => b.address().to_string(),
                None => continue,
            };

            let conn = match self
                .0
                .pool
                .get_connection_by_id(broker_id, &broker_addr)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    debug!("close_share_sessions: connection to broker {broker_id} failed: {e}");
                    continue;
                }
            };

            let version = match conn
                .negotiate_api_version(
                    ApiKey::ShareFetch,
                    versions::SHARE_FETCH_MAX,
                    versions::SHARE_FETCH_MIN,
                )
                .await
            {
                Some(v) => v,
                None => continue,
            };

            let request = ShareFetchRequest {
                group_id: Some(group_id.clone()),
                member_id: Some(member_id.clone()),
                share_session_epoch: FINAL_EPOCH,
                max_wait_ms: 0,
                min_bytes: 0,
                max_bytes: 0,
                max_records: 0,
                batch_size: 0,
                topics: Vec::new(),
                forgotten_topics: Vec::new(),
            };

            if let Err(e) = conn
                .send_request(ApiKey::ShareFetch, version, |buf| match version {
                    2 => request.encode_v2(buf, 0, false),
                    _ => request.encode_v1(buf),
                })
                .await
            {
                debug!("close_share_sessions: FINAL_EPOCH to broker {broker_id} failed: {e}");
            } else {
                debug!("close_share_sessions: sent FINAL_EPOCH to broker {broker_id}");
            }
        }
    }

    /// Background heartbeat loop.    ///
    /// Sends periodic heartbeats at the coordinator-specified interval so the
    /// share-group session stays alive independent of how often `poll()` is
    /// called.  Handles coordinator errors with automatic rediscovery and
    /// recovers from `FencedMemberEpoch` by resetting local member state.
    ///
    /// Stops when `closed` is set or the task is aborted.
    async fn run_heartbeat_loop(&self) {
        loop {
            let interval_ms = self.0.heartbeat_interval_ms.load(Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(interval_ms as u64)).await;

            if self.0.closed.load(Ordering::Relaxed) {
                break;
            }

            match self.send_heartbeat(false).await {
                Ok(()) => {}

                // Fenced epoch: the coordinator has advanced past our epoch.
                // Reset local member state so the next heartbeat starts a new
                // membership attempt.
                Err(KrafkaError::Broker {
                    code: ErrorCode::FencedMemberEpoch,
                    ..
                }) => {
                    warn!(
                        "Background heartbeat: member epoch fenced for group '{}'; resetting state",
                        self.0.config.group_id
                    );
                    self.0.member_epoch.store(0, Ordering::Release);
                    self.clear_ack_state().await;
                    self.invalidate_coordinator().await;
                    if let Err(e) = self.ensure_coordinator().await {
                        warn!(
                            "Background heartbeat: coordinator rediscovery after fence failed: {e}"
                        );
                    }
                }

                // Coordinator moved or unavailable: rediscover and retry.
                Err(ref e) if e.is_retriable() => {
                    debug!(
                        "Background heartbeat: retryable error for group '{}': {e}",
                        self.0.config.group_id
                    );
                    self.invalidate_coordinator().await;
                    if let Err(e2) = self.ensure_coordinator().await {
                        warn!("Background heartbeat: coordinator rediscovery failed: {e2}");
                    }
                }

                Err(e) => {
                    warn!(
                        "Background heartbeat error for group '{}': {e}",
                        self.0.config.group_id
                    );
                    self.invalidate_coordinator().await;
                }
            }
        }
        debug!(
            "Background heartbeat task stopped for group '{}'",
            self.0.config.group_id
        );
    }
}

/// Builder for creating share consumers.
#[derive(Default)]
#[must_use = "builders do nothing until .build() is called"]
pub struct ShareConsumerBuilder {
    config: ShareConsumerConfig,
}

impl ShareConsumerBuilder {
    /// Set the bootstrap servers.
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    /// Set the share group ID (required).
    pub fn group_id(mut self, group_id: impl Into<String>) -> Self {
        self.config.group_id = group_id.into();
        self
    }

    /// Set the client ID.
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.config.client_id = id.into();
        self
    }

    /// Set the acknowledgement mode.
    pub fn acknowledgement_mode(mut self, mode: AcknowledgementMode) -> Self {
        self.config.acknowledgement_mode = mode;
        self
    }

    /// Set the maximum number of records returned per `poll()` call.
    ///
    /// Must be >= 1. Rejected at build time otherwise. Defaults to 500.
    pub fn max_poll_records(mut self, max: i32) -> Self {
        self.config.max_poll_records = max;
        self
    }

    /// Set maximum records buffered internally by [`recv()`](ShareConsumer::recv).
    ///
    /// This is a soft threshold: once the buffer is at/above this value,
    /// `poll()` skips fetches until it drains. A single `recv()` call may
    /// buffer beyond the threshold due to batched fetch responses. Set to `0`
    /// for unlimited. Negative values are rejected at build time.
    /// Defaults to 500.
    pub fn max_buffered_records(mut self, max: i32) -> Self {
        self.config.max_buffered_records = max;
        self
    }

    /// Set the fetch max wait time in milliseconds.
    pub fn fetch_max_wait_ms(mut self, ms: i32) -> Self {
        self.config.fetch_max_wait_ms = ms;
        self
    }

    /// Set the request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set the session timeout for share group membership.
    pub fn session_timeout(mut self, timeout: Duration) -> Self {
        self.config.session_timeout = timeout;
        self
    }

    /// Set the heartbeat interval.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    /// Set authentication configuration.
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = Some(auth);
        self
    }

    /// Set the client rack ID.
    pub fn client_rack(mut self, rack: impl Into<String>) -> Self {
        self.config.client_rack = Some(rack.into());
        self
    }

    /// Set metadata max age.
    pub fn metadata_max_age(mut self, duration: Duration) -> Self {
        self.config.metadata_max_age = duration;
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

    /// Set SOCKS5 proxy configuration.
    #[cfg(feature = "socks5")]
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.config.proxy = Some(proxy);
        self
    }

    /// Set the maximum decompressed size for record batches.
    ///
    /// Compressed payloads that decompress beyond this limit are rejected as
    /// potential compression bombs. Defaults to
    /// [`RecordBatch::MAX_DECOMPRESSED_SIZE`](crate::protocol::RecordBatch::MAX_DECOMPRESSED_SIZE) (128 MiB).
    pub fn max_decompressed_size(mut self, size: usize) -> Self {
        self.config.max_decompressed_size = size;
        self
    }

    /// Build the share consumer.
    pub async fn build(self) -> Result<ShareConsumer> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap.servers is required"));
        }
        if self.config.group_id.is_empty() {
            return Err(KrafkaError::config(
                "group_id is required for share consumers",
            ));
        }
        if self.config.heartbeat_interval >= self.config.session_timeout {
            return Err(KrafkaError::config(format!(
                "heartbeat_interval ({:?}) must be less than session_timeout ({:?})",
                self.config.heartbeat_interval, self.config.session_timeout,
            )));
        }
        if self.config.max_buffered_records < 0 {
            return Err(KrafkaError::config(format!(
                "max_buffered_records ({}) must be >= 0 (use 0 for unlimited)",
                self.config.max_buffered_records,
            )));
        }
        if self.config.max_poll_records < 1 {
            return Err(KrafkaError::config(format!(
                "max_poll_records ({}) must be >= 1",
                self.config.max_poll_records,
            )));
        }
        ShareConsumer::new(self.config).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    fn test_share_consumer(acknowledgement_mode: AcknowledgementMode) -> ShareConsumer {
        let mut config = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("sg")
            .acknowledgement_mode(acknowledgement_mode)
            .config;
        config.bootstrap_servers = "localhost:9092".to_string();
        config.group_id = "sg".to_string();

        let pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
        let metadata = Arc::new(ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool.clone(),
            config.metadata_max_age,
        ));

        ShareConsumer(Arc::new(ShareConsumerInner {
            config,
            metadata,
            pool,
            subscriptions: RwLock::new(HashSet::new()),
            assignments: RwLock::new(HashMap::new()),
            member_id: ArcSwap::new(Arc::new(crate::util::random_uuid_v4())),
            member_epoch: AtomicI32::new(0),
            heartbeat_interval_ms: AtomicI32::new(3000),
            closed: AtomicBool::new(false),
            share_sessions: Arc::new(tokio::sync::Mutex::new(ShareSessionCache::new())),
            pending_acks: Arc::new(RwLock::new(HashMap::new())),
            ack_state_generation: Arc::new(AtomicU64::new(0)),
            explicit_flush_retry_required: Arc::new(AtomicBool::new(false)),
            topic_ids: RwLock::new(HashMap::new()),
            recv_buffer: RwLock::new(VecDeque::new()),
            coordinator_id: RwLock::new(None),
            coordinator_address: RwLock::new(None),
            unacked_offsets: Arc::new(RwLock::new(HashSet::new())),
            heartbeat_task: SyncMutex::new(None),
            wakeup_flag: AtomicBool::new(false),
        }))
    }

    #[test]
    fn test_share_consumer_builder_config() {
        let builder = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("my-share-group")
            .client_id("test-client")
            .acknowledgement_mode(AcknowledgementMode::Explicit)
            .max_poll_records(100)
            .session_timeout(Duration::from_secs(30))
            .heartbeat_interval(Duration::from_secs(5));

        assert_eq!(builder.config.bootstrap_servers, "localhost:9092");
        assert_eq!(builder.config.group_id, "my-share-group");
        assert_eq!(builder.config.client_id, "test-client");
        assert_eq!(
            builder.config.acknowledgement_mode,
            AcknowledgementMode::Explicit
        );
        assert_eq!(builder.config.max_poll_records, 100);
        assert_eq!(builder.config.session_timeout, Duration::from_secs(30));
        assert_eq!(builder.config.heartbeat_interval, Duration::from_secs(5));
    }

    #[test]
    fn test_share_consumer_builder_defaults() {
        let builder = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("sg");

        assert_eq!(builder.config.client_id, "krafka");
        assert_eq!(
            builder.config.acknowledgement_mode,
            AcknowledgementMode::Implicit
        );
        assert_eq!(builder.config.max_poll_records, 500);
        assert!(builder.config.auth.is_none());
    }

    #[tokio::test]
    async fn test_share_consumer_builder_validates_bootstrap() {
        let result = ShareConsumer::builder().group_id("sg").build().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("bootstrap.servers"), "got: {err}");
    }

    #[tokio::test]
    async fn test_share_consumer_builder_validates_group_id() {
        let result = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .build()
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("group_id"), "got: {err}");
    }

    #[tokio::test]
    async fn test_share_consumer_builder_validates_heartbeat() {
        let result = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("sg")
            .session_timeout(Duration::from_secs(5))
            .heartbeat_interval(Duration::from_secs(10))
            .build()
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("heartbeat_interval"), "got: {err}");
    }

    #[tokio::test]
    async fn test_share_consumer_builder_rejects_negative_max_buffered_records() {
        let result = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("sg")
            .max_buffered_records(-1)
            .build()
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("max_buffered_records"), "got: {err}");
    }

    #[tokio::test]
    async fn test_share_consumer_builder_rejects_zero_max_poll_records() {
        for bad in [0, -1, i32::MIN] {
            let result = ShareConsumer::builder()
                .bootstrap_servers("localhost:9092")
                .group_id("sg")
                .max_poll_records(bad)
                .build()
                .await;
            assert!(result.is_err(), "expected error for max_poll_records={bad}");
            let err = result.unwrap_err().to_string();
            assert!(err.contains("max_poll_records"), "got: {err}");
        }
    }

    #[test]
    fn test_acknowledge_type_to_i8() {
        assert_eq!(AcknowledgeType::Accept.to_i8(), 1);
        assert_eq!(AcknowledgeType::Release.to_i8(), 2);
        assert_eq!(AcknowledgeType::Reject.to_i8(), 3);
        assert_eq!(AcknowledgeType::Archive.to_i8(), 4);
    }

    #[test]
    fn test_acknowledgement_mode_default() {
        assert_eq!(
            AcknowledgementMode::default(),
            AcknowledgementMode::Implicit
        );
    }

    #[test]
    fn test_share_consumer_config_accessors() {
        let builder = ShareConsumer::builder()
            .bootstrap_servers("broker:9092")
            .group_id("sg-1")
            .client_id("my-client")
            .acknowledgement_mode(AcknowledgementMode::Explicit)
            .session_timeout(Duration::from_secs(20))
            .heartbeat_interval(Duration::from_secs(3));

        assert_eq!(builder.config.bootstrap_servers(), "broker:9092");
        assert_eq!(builder.config.group_id(), "sg-1");
        assert_eq!(builder.config.client_id(), "my-client");
        assert_eq!(
            builder.config.acknowledgement_mode(),
            AcknowledgementMode::Explicit
        );
        assert_eq!(builder.config.session_timeout(), Duration::from_secs(20));
        assert_eq!(builder.config.heartbeat_interval(), Duration::from_secs(3));
    }

    #[test]
    fn test_build_acknowledge_topics() {
        let acks = vec![
            PendingAck {
                topic: "t1".into(),
                topic_id: [1; 16],
                partition: 0,
                first_offset: 0,
                last_offset: 5,
                ack_type: AcknowledgeType::Accept.to_i8(),
            },
            PendingAck {
                topic: "t1".into(),
                topic_id: [1; 16],
                partition: 1,
                first_offset: 10,
                last_offset: 15,
                ack_type: AcknowledgeType::Release.to_i8(),
            },
            PendingAck {
                topic: "t2".into(),
                topic_id: [2; 16],
                partition: 0,
                first_offset: 0,
                last_offset: 3,
                ack_type: AcknowledgeType::Reject.to_i8(),
            },
        ];

        let topics = ShareConsumer::build_acknowledge_topics(&acks);
        assert_eq!(topics.len(), 2); // Two distinct topic_ids

        // Verify partition counts.
        let total_partitions: usize = topics.iter().map(|t| t.partitions.len()).sum();
        assert_eq!(total_partitions, 3);
    }

    #[test]
    fn test_share_acknowledge_response_error_detects_partition_failure() {
        let response = crate::protocol::ShareAcknowledgeResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            error_message: None,
            acquisition_lock_timeout_ms: -1,
            responses: vec![crate::protocol::ShareAcknowledgeTopicResponse {
                topic_id: [1; 16],
                partitions: vec![crate::protocol::ShareAcknowledgePartitionResponse {
                    partition_index: 7,
                    error_code: ErrorCode::UnknownTopicOrPartition,
                    error_message: Some("gone".to_string()),
                    current_leader: crate::protocol::ShareLeaderIdAndEpoch {
                        leader_id: -1,
                        leader_epoch: 0,
                    },
                }],
            }],
            node_endpoints: Vec::new(),
        };

        let error = ShareConsumer::share_acknowledge_response_error(&response)
            .expect("partition error must surface as an error");

        assert!(matches!(
            error,
            KrafkaError::Broker {
                code: ErrorCode::UnknownTopicOrPartition,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_restore_ack_state_requeues_pending_acks_without_reinserting_unacked() {
        let ack_state_generation = AtomicU64::new(0);
        let explicit_flush_retry_required = AtomicBool::new(false);
        let pending: RwLock<BrokerPendingAcks> = RwLock::new(HashMap::new());
        let mut acks_to_restore = vec![PendingAck {
            topic: "topic-a".into(),
            topic_id: [0; 16],
            partition: 2,
            first_offset: 11,
            last_offset: 13,
            ack_type: AcknowledgeType::Accept.to_i8(),
        }];

        ShareConsumer::restore_ack_state(
            &ack_state_generation,
            &pending,
            &explicit_flush_retry_required,
            0,
            false,
            &mut acks_to_restore,
        )
        .await;

        assert!(acks_to_restore.is_empty());
        assert!(!explicit_flush_retry_required.load(Ordering::SeqCst));
        let guard = pending.read().await;
        let all_acks: Vec<&PendingAck> =
            guard.values().flat_map(|b| b.values().flatten()).collect();
        assert_eq!(all_acks.len(), 1);
        assert_eq!(all_acks[0].topic, "topic-a");
        assert_eq!(all_acks[0].partition, 2);
        assert_eq!(all_acks[0].first_offset, 11);
        assert_eq!(all_acks[0].last_offset, 13);
        assert_eq!(all_acks[0].ack_type, AcknowledgeType::Accept.to_i8());
    }

    #[tokio::test]
    async fn test_restore_ack_state_skips_stale_generation() {
        let ack_state_generation = AtomicU64::new(1);
        let explicit_flush_retry_required = AtomicBool::new(false);
        let pending: RwLock<BrokerPendingAcks> = RwLock::new(HashMap::new());
        let mut acks_to_restore = vec![PendingAck {
            topic: "topic-a".into(),
            topic_id: [0; 16],
            partition: 2,
            first_offset: 11,
            last_offset: 13,
            ack_type: AcknowledgeType::Accept.to_i8(),
        }];

        ShareConsumer::restore_ack_state(
            &ack_state_generation,
            &pending,
            &explicit_flush_retry_required,
            0,
            true,
            &mut acks_to_restore,
        )
        .await;

        assert!(pending.read().await.is_empty());
        assert!(acks_to_restore.is_empty());
        assert!(!explicit_flush_retry_required.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_restore_ack_state_marks_explicit_flush_retry_required() {
        let ack_state_generation = AtomicU64::new(0);
        let explicit_flush_retry_required = AtomicBool::new(false);
        let pending: RwLock<BrokerPendingAcks> = RwLock::new(HashMap::new());
        let mut acks_to_restore = vec![PendingAck {
            topic: "topic-a".into(),
            topic_id: [0; 16],
            partition: 2,
            first_offset: 11,
            last_offset: 13,
            ack_type: AcknowledgeType::Accept.to_i8(),
        }];

        ShareConsumer::restore_ack_state(
            &ack_state_generation,
            &pending,
            &explicit_flush_retry_required,
            0,
            true,
            &mut acks_to_restore,
        )
        .await;

        assert!(acks_to_restore.is_empty());
        assert!(explicit_flush_retry_required.load(Ordering::SeqCst));
        assert_eq!(
            pending
                .read()
                .await
                .values()
                .flat_map(|b| b.values().flatten())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn test_poll_rejects_after_failed_explicit_flush() {
        let consumer = test_share_consumer(AcknowledgementMode::Explicit);
        consumer
            .0
            .explicit_flush_retry_required
            .store(true, Ordering::SeqCst);

        let error = consumer
            .poll(Duration::from_millis(1))
            .await
            .expect_err("poll must block after a failed explicit flush");

        assert!(
            error
                .to_string()
                .contains("retry the commit before calling poll() again")
        );
    }

    #[tokio::test]
    async fn test_clear_partition_state_clears_explicit_flush_retry_required() {
        let consumer = test_share_consumer(AcknowledgementMode::Explicit);
        consumer
            .0
            .explicit_flush_retry_required
            .store(true, Ordering::SeqCst);

        consumer.clear_partition_state().await;

        assert!(
            !consumer
                .0
                .explicit_flush_retry_required
                .load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn test_apply_assignment_advances_ack_state_generation_on_change() {
        let consumer = test_share_consumer(AcknowledgementMode::Explicit);
        consumer
            .0
            .assignments
            .write()
            .await
            .insert("topic-a".to_string(), vec![0]);
        consumer
            .0
            .pending_acks
            .write()
            .await
            .entry(UNROUTED_BROKER_ID)
            .or_default()
            .entry(([1; 16], 0))
            .or_default()
            .push(PendingAck {
                topic: "topic-a".to_string(),
                topic_id: [1; 16],
                partition: 0,
                first_offset: 5,
                last_offset: 5,
                ack_type: AcknowledgeType::Accept.to_i8(),
            });
        consumer
            .0
            .unacked_offsets
            .write()
            .await
            .insert(("topic-a".to_string(), 0, 5));
        consumer
            .0
            .explicit_flush_retry_required
            .store(true, Ordering::SeqCst);

        let old_generation = consumer.0.ack_state_generation.load(Ordering::SeqCst);
        consumer.apply_assignment(&[]).await;

        assert_eq!(
            consumer.0.ack_state_generation.load(Ordering::SeqCst),
            old_generation + 1
        );
        assert!(
            !consumer
                .0
                .explicit_flush_retry_required
                .load(Ordering::SeqCst)
        );
        assert!(consumer.0.pending_acks.read().await.is_empty());
        assert!(consumer.0.unacked_offsets.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_send_share_acknowledge_rejects_stale_ack_generation() {
        let consumer = test_share_consumer(AcknowledgementMode::Explicit);
        let error = ShareConsumer::send_share_acknowledge_with_state(
            ShareAcknowledgeContext {
                metadata: consumer.0.metadata.clone(),
                pool: consumer.0.pool.clone(),
                share_sessions: consumer.0.share_sessions.clone(),
                group_id: consumer.0.config.group_id.clone(),
                member_id: (**consumer.0.member_id.load()).clone(),
                current_ack_state_generation: Arc::new(AtomicU64::new(1)),
                ack_state_generation: 0,
            },
            &[PendingAck {
                topic: "topic-a".to_string(),
                topic_id: [1; 16],
                partition: 0,
                first_offset: 5,
                last_offset: 5,
                ack_type: AcknowledgeType::Accept.to_i8(),
            }],
        )
        .await
        .expect_err("stale ack generation must be rejected before sending");

        assert!(
            error
                .to_string()
                .contains("acknowledgement state was invalidated")
        );
    }

    #[tokio::test]
    async fn test_share_commit_handle_ready_flattens_result() {
        ShareCommitHandle::ready(Ok(()))
            .await
            .expect("ready ok result");

        let error = ShareCommitHandle::ready(Err(KrafkaError::invalid_state("boom")))
            .await
            .expect_err("ready error must surface");
        assert!(error.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn test_share_commit_handle_flattens_task_result() {
        let error = ShareCommitHandle::Task(tokio::spawn(async {
            Err(KrafkaError::invalid_state("task failed"))
        }))
        .await
        .expect_err("task error must surface");

        assert!(error.to_string().contains("task failed"));
    }

    #[tokio::test]
    async fn test_describe_share_fetch_join_error_reports_panic() {
        let error = tokio::spawn(async {
            panic!("boom");
        })
        .await
        .expect_err("panic must surface as a JoinError");

        assert_eq!(describe_share_fetch_join_error(&error), "panicked");
    }

    #[tokio::test]
    async fn test_describe_share_fetch_join_error_reports_cancellation() {
        let handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        handle.abort();

        let error = handle
            .await
            .expect_err("aborted task must surface as a JoinError");

        assert_eq!(describe_share_fetch_join_error(&error), "was cancelled");
    }

    #[tokio::test]
    async fn test_acknowledge_keeps_record_pending_until_ack_is_queued() {
        let consumer = Arc::new(test_share_consumer(AcknowledgementMode::Explicit));

        consumer
            .0
            .topic_ids
            .write()
            .await
            .insert("topic-a".to_string(), [7; 16]);

        let record = ConsumerRecord {
            topic: "topic-a".into(),
            partition: 3,
            offset: 11,
            timestamp: 0,
            timestamp_type: 0,
            key: None,
            value: None,
            headers: Vec::new(),
            leader_epoch: None,
            delivery_count: None,
        };
        let record_key = (record.topic.clone(), record.partition, record.offset);

        consumer
            .0
            .unacked_offsets
            .write()
            .await
            .insert(record_key.clone());

        let pending_guard = consumer.0.pending_acks.write().await;
        let task_consumer = consumer.clone();
        let task = tokio::spawn(async move {
            task_consumer
                .acknowledge(&record, AcknowledgeType::Accept)
                .await
        });

        tokio::task::yield_now().await;

        assert!(
            consumer
                .0
                .unacked_offsets
                .read()
                .await
                .contains(&record_key)
        );
        assert!(
            !task.is_finished(),
            "acknowledge should still be waiting on the pending_acks lock"
        );

        drop(pending_guard);

        task.await
            .expect("acknowledge task should join")
            .expect("acknowledge should succeed once pending lock is released");

        assert!(
            !consumer
                .0
                .unacked_offsets
                .read()
                .await
                .contains(&record_key)
        );
        let pending_guard = consumer.0.pending_acks.read().await;
        let all_acks: Vec<&PendingAck> = pending_guard
            .values()
            .flat_map(|b| b.values().flatten())
            .collect();
        assert_eq!(all_acks.len(), 1);
        assert_eq!(all_acks[0].topic, "topic-a");
        assert_eq!(all_acks[0].partition, 3);
        assert_eq!(all_acks[0].first_offset, 11);
        assert_eq!(all_acks[0].last_offset, 11);
        assert_eq!(all_acks[0].ack_type, AcknowledgeType::Accept.to_i8());
    }

    #[test]
    fn test_coalesce_implicit_acks_merges_consecutive() {
        let records = vec![
            ConsumerRecord {
                topic: "t1".into(),
                partition: 0,
                offset: 0,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: None,
                headers: Vec::new(),
                leader_epoch: None,
                delivery_count: None,
            },
            ConsumerRecord {
                topic: "t1".into(),
                partition: 0,
                offset: 1,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: None,
                headers: Vec::new(),
                leader_epoch: None,
                delivery_count: None,
            },
            ConsumerRecord {
                topic: "t1".into(),
                partition: 0,
                offset: 2,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: None,
                headers: Vec::new(),
                leader_epoch: None,
                delivery_count: None,
            },
            // Gap: offset 3 missing
            ConsumerRecord {
                topic: "t1".into(),
                partition: 0,
                offset: 4,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: None,
                headers: Vec::new(),
                leader_epoch: None,
                delivery_count: None,
            },
        ];

        let mut topic_ids = HashMap::new();
        topic_ids.insert("t1".to_string(), [1u8; 16]);

        let mut pending: BrokerPendingAcks = HashMap::new();
        // Pass a dummy ClusterMetadata — no live brokers, so all acks route to UNROUTED_BROKER_ID.
        let dummy_pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
        let dummy_metadata = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            dummy_pool,
            Duration::from_secs(300),
        );
        ShareConsumer::coalesce_implicit_acks(&records, &topic_ids, &mut pending, &dummy_metadata);

        // Should produce two ranges: [0,2] and [4,4].
        let mut all_acks: Vec<PendingAck> = pending
            .into_values()
            .flat_map(|b| b.into_values().flatten())
            .collect();
        assert_eq!(all_acks.len(), 2);
        all_acks.sort_by_key(|a| a.first_offset);
        assert_eq!(all_acks[0].first_offset, 0);
        assert_eq!(all_acks[0].last_offset, 2);
        assert_eq!(all_acks[1].first_offset, 4);
        assert_eq!(all_acks[1].last_offset, 4);
    }

    #[test]
    fn test_config_defaults_match_kip932() {
        let builder = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("sg");

        // KIP-932 specifies 45s session timeout.
        assert_eq!(builder.config.session_timeout(), Duration::from_secs(45));
        assert_eq!(builder.config.heartbeat_interval(), Duration::from_secs(5));
    }

    // ── Regression test: F-06 / ack_state_generation ordering ────────────────

    /// `ensure_ack_state_current` must accept a matching generation and reject
    /// any stale one, whether the captured snapshot is behind OR ahead of the
    /// current value (the latter is impossible in practice but good to guard).
    ///
    /// This is a unit test of the pure guard function — no async machinery
    /// required.  The same logic is exercised end-to-end by
    /// `test_send_share_acknowledge_rejects_stale_ack_generation`.
    #[test]
    fn test_ensure_ack_state_current_rejects_stale_generation() {
        let current = AtomicU64::new(5);

        // Exact match → Ok.
        assert!(
            ShareConsumer::ensure_ack_state_current(&current, 5).is_ok(),
            "matching generation must succeed"
        );

        // Stale (lower than current) — the common invalidation case.
        assert!(
            ShareConsumer::ensure_ack_state_current(&current, 4).is_err(),
            "stale lower generation must be rejected"
        );

        // Stale (higher than current) — shouldn't happen, but the guard covers it.
        assert!(
            ShareConsumer::ensure_ack_state_current(&current, 6).is_err(),
            "stale higher generation must be rejected"
        );
    }

    /// Incrementing `ack_state_generation` must be visible to concurrent
    /// callers that captured the old value, without any spurious success.
    ///
    /// Simulates: flush task captures generation, assignment changes, flush
    /// task calls `ensure_ack_state_current` and must receive an error.
    #[tokio::test]
    async fn test_ack_state_generation_flush_task_sees_invalidation() {
        let consumer = Arc::new(test_share_consumer(AcknowledgementMode::Explicit));

        // Capture the generation as a flush task would at spawn time.
        let captured_gen = consumer.0.ack_state_generation.load(Ordering::SeqCst);
        assert_eq!(captured_gen, 0);

        // Simulate assignment change / unsubscribe which advances the generation.
        consumer.clear_partition_state().await;
        let new_gen = consumer.0.ack_state_generation.load(Ordering::SeqCst);
        assert!(
            new_gen > captured_gen,
            "generation must advance after clear_partition_state"
        );

        // A detached flush task using the captured (old) generation must be blocked.
        let err =
            ShareConsumer::ensure_ack_state_current(&consumer.0.ack_state_generation, captured_gen)
                .expect_err("stale flush task must be rejected");
        assert!(err.to_string().contains("invalidated"), "got: {err}");
    }

    /// Cloning a `ShareConsumer` produces a second handle to the same state.
    #[test]
    fn test_share_consumer_clone_shares_state() {
        let consumer = test_share_consumer(AcknowledgementMode::Implicit);
        let cloned = consumer.clone();

        // Both handles share the same Arc — pointer equality confirms this.
        assert!(Arc::ptr_eq(&consumer.0, &cloned.0));

        // A store via one handle is immediately visible through the other.
        consumer.0.member_epoch.store(42, Ordering::Release);
        assert_eq!(cloned.0.member_epoch.load(Ordering::Acquire), 42);
    }

    /// The background heartbeat task field starts as `None` and can be set.
    #[test]
    fn test_heartbeat_task_starts_none() {
        let consumer = test_share_consumer(AcknowledgementMode::Implicit);
        let guard = consumer.0.heartbeat_task.lock().unwrap();
        assert!(guard.is_none(), "heartbeat task should start as None");
    }

    /// `close()` marks the consumer closed and the drop warning is suppressed.
    #[tokio::test]
    async fn test_close_is_idempotent_and_suppresses_drop_warning() {
        let consumer = test_share_consumer(AcknowledgementMode::Implicit);
        // First close: should succeed even without a coordinator.
        let _ = consumer.close().await;
        assert!(consumer.is_closed());
        // Second close: must be idempotent (no panic, no error).
        let _ = consumer.close().await;
    }
}
