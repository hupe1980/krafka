//! Cluster metadata management.
//!
//! This module handles:
//! - Fetching and caching cluster metadata
//! - Topic and partition information
//! - Broker discovery
//! - Leader election tracking
//!
//! # One writer
//!
//! Every [`ClusterMetadata`](crate::metadata::ClusterMetadata) has one writer task. Callers never fetch
//! metadata themselves: they ask the writer for topics (optionally forcing a
//! fetch the cache-age check would skip) and wait for the answer. The writer
//! unions everything requested since its last fetch into one `Metadata`
//! request, applies the retry backoff between fetches, and is the only code
//! that builds a new snapshot from a response. Leader hints and rebootstraps
//! go through the same serialized write path, so no update can overwrite
//! another one it did not see. Readers load the current snapshot without
//! locking.

// `AHashMap` is used throughout this module for all internal maps (broker IDs,
// topic names, partition IDs). `ahash` is a non-cryptographic hash function.
// Hash-flooding is not a concern here because all map keys are sourced from
// authenticated Kafka cluster metadata responses — an attacker who controls
// topic names must already have the ability to inject arbitrary cluster
// metadata, at which point hash-flooding is the least of the client's problems.
// Key lengths are also bounded by Kafka's own validation (topic names ≤ 249
// characters, broker IDs are i32).
use ahash::{AHashMap, AHashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tokio::time::Instant;

use arc_swap::ArcSwap;
use parking_lot::Mutex as SyncMutex;
use tokio::sync::{Notify, oneshot};
use tracing::{debug, info, warn};

use crate::error::{ErrorCode, KrafkaError, Result};
use crate::network::{BrokerConnection, ConnectionPool};
use crate::protocol::{
    ApiKey, MetadataRequest, MetadataResponse, VersionedDecode, VersionedEncode,
};
use crate::util::BackoffPolicy;
use crate::{BrokerId, PartitionId};

/// Strategy for recovering when the client loses the cluster, i.e. Java's
/// `metadata.recovery.strategy` (KIP-899, KIP-1102).
///
/// With [`Rebootstrap`](Self::Rebootstrap) the client drops its view of the
/// cluster and rediscovers it from the bootstrap servers when every known
/// broker is unreachable, when no metadata request has succeeded within
/// [`ClusterMetadata::with_rebootstrap_trigger`], or when a broker answers
/// `REBOOTSTRAP_REQUIRED`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum MetadataRecoveryStrategy {
    /// No automatic recovery: `metadata.recovery.strategy=none`.
    None,
    /// Rediscover the cluster from the bootstrap servers. The default.
    #[default]
    Rebootstrap,
}

/// Information about a broker.
#[non_exhaustive]
#[must_use]
#[derive(Debug, Clone)]
pub struct BrokerInfo {
    /// Broker ID.
    id: BrokerId,
    /// Broker host.
    host: String,
    /// Broker port.
    port: i32,
    /// Broker rack (optional).
    rack: Option<String>,
    /// Cached `host:port` address string.
    address: String,
}

impl BrokerInfo {
    /// Create a new `BrokerInfo`.
    pub fn new(id: BrokerId, host: String, port: i32, rack: Option<String>) -> Self {
        let address = format!("{host}:{port}");
        Self {
            id,
            host,
            port,
            rack,
            address,
        }
    }

    /// Get the broker host.
    #[inline]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Get the broker ID.
    #[inline]
    pub fn id(&self) -> BrokerId {
        self.id
    }

    /// Get the broker port.
    #[inline]
    pub fn port(&self) -> i32 {
        self.port
    }

    /// Get the broker rack, if any.
    #[inline]
    pub fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    /// Get the broker address as `host:port`.
    #[inline]
    pub fn address(&self) -> &str {
        &self.address
    }
}

/// Find the endpoint a broker advertised for `node_id` in a Fetch/Produce
/// response and turn it into a [`BrokerInfo`] (KIP-951).
///
/// The `NodeEndpoints` list accompanies a `CurrentLeader` report so the client
/// can dial a leader the metadata cache may never have seen. Returns `None`
/// when the broker named a leader but did not advertise its address, in which
/// case the caller can still pass the hint on — [`ClusterMetadata::apply_leader_hint`]
/// falls back to the cached broker map.
pub(crate) fn broker_info_for_node(
    endpoints: &[crate::protocol::NodeEndpoint],
    node_id: BrokerId,
) -> Option<BrokerInfo> {
    endpoints
        .iter()
        .find(|endpoint| endpoint.node_id == node_id)
        .map(|endpoint| {
            BrokerInfo::new(
                endpoint.node_id,
                endpoint.host.clone(),
                endpoint.port,
                endpoint.rack.clone(),
            )
        })
}

/// Information about a topic partition.
///
/// # Partitions in an error state
///
/// A partition entry is retained in [`TopicInfo::partitions`] even when the
/// broker reported a per-partition error (`LEADER_NOT_AVAILABLE` during a
/// rolling restart, for example). Such an entry has `leader == -1`,
/// `leader_epoch == -1`, and a non-OK [`error_code`](Self::error_code).
///
/// Retaining the entry keeps [`TopicInfo::partition_count`] stable, so a
/// key-hash partitioner computing `hash % partition_count` keeps routing each
/// key to the same partition during the outage. Routing fails for the
/// individual affected partitions instead, with a retriable
/// `LEADER_NOT_AVAILABLE`.
#[non_exhaustive]
#[must_use]
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Topic name.
    pub topic: String,
    /// Partition ID.
    pub partition: PartitionId,
    /// Leader broker ID. `-1` when the leader is unknown or the partition is
    /// in an error state.
    pub leader: BrokerId,
    /// Leader epoch. `-1` when unknown (Metadata < v7) or the partition is in
    /// an error state.
    pub leader_epoch: i32,
    /// Replica broker IDs.
    pub replicas: Vec<BrokerId>,
    /// In-sync replica broker IDs.
    pub isr: Vec<BrokerId>,
    /// Offline replica broker IDs.
    pub offline_replicas: Vec<BrokerId>,
    /// The per-partition error reported by the broker in the most recent
    /// metadata response. [`ErrorCode::None`] for healthy partitions.
    pub error_code: ErrorCode,
}

impl PartitionInfo {
    /// Returns `true` when the broker reported no error for this partition and
    /// a leader is known, i.e. the partition is routable.
    #[inline]
    #[must_use]
    pub fn is_routable(&self) -> bool {
        self.error_code.is_ok() && self.leader >= 0
    }
}

/// Information about a topic.
#[non_exhaustive]
#[must_use]
#[derive(Debug, Clone)]
pub struct TopicInfo {
    /// Topic name.
    pub name: String,
    /// Topic ID (Metadata v10+). All zeros when the broker did not report one.
    pub topic_id: [u8; 16],
    /// Whether the topic is internal.
    pub is_internal: bool,
    /// Partition information, keyed by partition ID for O(1) lookup.
    pub partitions: std::collections::HashMap<PartitionId, PartitionInfo>,
}

impl TopicInfo {
    /// Get the number of partitions.
    ///
    /// This is the **full** partition count as reported by the broker,
    /// including partitions currently in an error state (see
    /// [`PartitionInfo`]). Partitioners must use this value so that
    /// `hash % partition_count` stays stable while individual partitions are
    /// transiently unavailable.
    #[inline]
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Get partition info by ID — O(1).
    #[inline]
    pub fn partition(&self, partition_id: PartitionId) -> Option<&PartitionInfo> {
        self.partitions.get(&partition_id)
    }

    /// Iterate over all partition infos in unspecified order.
    #[inline]
    pub fn partitions_iter(&self) -> impl Iterator<Item = &PartitionInfo> + '_ {
        self.partitions.values()
    }

    /// Get the leader for a partition.
    ///
    /// Returns `None` when the partition is unknown **or** when it is in an
    /// error state / has no elected leader (`leader == -1`), so that routing
    /// fails for that single partition instead of dialling broker `-1`.
    #[inline]
    pub fn leader(&self, partition_id: PartitionId) -> Option<BrokerId> {
        self.partition(partition_id)
            .filter(|p| p.is_routable())
            .map(|p| p.leader)
    }

    /// Get the leader epoch for a partition.
    ///
    /// Returns `None` when the partition is unknown or the epoch is unknown
    /// (`-1`, i.e. Metadata < v7 or an error state).
    #[inline]
    pub fn leader_epoch(&self, partition_id: PartitionId) -> Option<i32> {
        self.partition(partition_id)
            .map(|p| p.leader_epoch)
            .filter(|e| *e >= 0)
    }
}

/// Default ceiling for the metadata retry backoff, mirroring Java's
/// `retry.backoff.max.ms`.
const DEFAULT_RETRY_BACKOFF_MAX: Duration = Duration::from_millis(1000);

/// Default base delay for the metadata retry backoff, mirroring Java's
/// `retry.backoff.ms`.
const DEFAULT_RETRY_BACKOFF: Duration = Duration::from_millis(100);

/// Jitter applied to the metadata retry backoff, as a fraction of the delay.
///
/// 20% scatter is enough to break up synchronised retries across a fleet
/// without materially changing the average retry rate of any single client.
const RETRY_BACKOFF_JITTER: f64 = 0.2;

/// Fraction of the rebootstrap trigger used as random extra delay before a
/// time-triggered rebootstrap is allowed to fire.
///
/// Without it, a fleet whose clients all lost the cluster at the same instant
/// would rebootstrap in lockstep and hit the seed brokers as one wave.
const REBOOTSTRAP_TRIGGER_JITTER: f64 = 0.2;

/// How many candidate addresses one connection attempt races concurrently.
const CONNECT_FANOUT: usize = 3;

/// State of the metadata-refresh rate limiter (KIP-580).
///
/// The delay between fetches grows exponentially while they keep failing and
/// resets to the base delay as soon as one succeeds.
#[derive(Debug)]
struct RefreshBackoffState {
    /// When the last fetch completed (success or failure). `None` means no
    /// fetch has completed yet, so the next one is free.
    last_attempt_completed: Option<Instant>,
    /// Number of consecutive failed fetches. Reset to zero on success.
    consecutive_failures: u32,
    /// Delay that must elapse after `last_attempt_completed` before the next
    /// fetch. Computed once per completed fetch, so the jitter is sampled once.
    current_delay: Duration,
}

impl RefreshBackoffState {
    fn new() -> Self {
        Self {
            last_attempt_completed: None,
            consecutive_failures: 0,
            current_delay: Duration::ZERO,
        }
    }

    /// How much of `current_delay` is left, or `None` if a fetch is allowed.
    fn remaining(&self) -> Option<Duration> {
        let last = self.last_attempt_completed?;
        let elapsed = last.elapsed();
        if elapsed >= self.current_delay {
            None
        } else {
            Some(self.current_delay - elapsed)
        }
    }

    /// Record a successful fetch: drop back to the base delay.
    fn record_success(&mut self, policy: &BackoffPolicy) {
        self.consecutive_failures = 0;
        self.current_delay = policy.calculate_backoff(1);
        self.last_attempt_completed = Some(Instant::now());
    }

    /// Record a failed fetch: advance one step along the exponential curve.
    fn record_failure(&mut self, policy: &BackoffPolicy) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.current_delay = policy.calculate_backoff(self.consecutive_failures);
        self.last_attempt_completed = Some(Instant::now());
    }
}

/// Milliseconds since a process-wide reference instant, never zero.
///
/// Zero is reserved as "never used" in [`TopicStamp::last_used_ms`].
fn now_millis() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let epoch = EPOCH.get_or_init(Instant::now);
    u64::try_from(epoch.elapsed().as_millis())
        .unwrap_or(u64::MAX)
        .saturating_add(1)
}

fn millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

/// When a cached topic was last fetched and last used.
///
/// The one per-topic expiry record. `refreshed` decides whether the entry is
/// current (`metadata.max.age.ms`); `last_used_ms` decides whether it is idle
/// (`metadata.max.idle.ms`). A partial refresh evicts a topic only when it is
/// neither used nor fetched within the idle TTL.
///
/// The stamp is shared by `Arc` between snapshots, so a use recorded on the
/// read path is one relaxed atomic store with no lock.
#[derive(Debug)]
struct TopicStamp {
    /// When a metadata response last included this topic.
    refreshed: Instant,
    /// [`now_millis`] at the last use; `0` when never used.
    last_used_ms: AtomicU64,
}

impl TopicStamp {
    fn fetched_now(last_used_ms: u64) -> Arc<Self> {
        Arc::new(Self {
            refreshed: Instant::now(),
            last_used_ms: AtomicU64::new(last_used_ms),
        })
    }

    /// Whether the topic was fetched or used within `ttl`.
    fn is_live(&self, now: Instant, now_ms: u64, ttl: Duration) -> bool {
        let used = self.last_used_ms.load(Ordering::Relaxed);
        now.duration_since(self.refreshed) <= ttl
            || (used != 0 && now_ms.saturating_sub(used) <= millis(ttl))
    }
}

/// One immutable snapshot of cluster metadata.
#[derive(Debug, Clone)]
struct MetadataCache {
    /// Cluster ID.
    cluster_id: Option<String>,
    /// Controller broker ID.
    controller_id: BrokerId,
    /// Brokers by ID: exactly the brokers of the last metadata response, plus
    /// endpoints registered by leader hints since.
    brokers: AHashMap<BrokerId, BrokerInfo>,
    /// Topics by name, `Arc`-wrapped so a new snapshot shares unchanged
    /// entries with the previous one.
    topics: AHashMap<String, Arc<TopicInfo>>,
    /// Topic UUID → topic name (Metadata v10+). Used by the KIP-848 consumer
    /// and the share consumer to resolve topic IDs.
    topic_ids: AHashMap<[u8; 16], Arc<String>>,
    /// Topic name → topic UUID, kept in step with `topic_ids`.
    name_to_topic_id: AHashMap<String, [u8; 16]>,
    /// Fetch and use stamps for every topic in `topics`.
    topic_stamps: AHashMap<String, Arc<TopicStamp>>,
    /// The topic-level error the broker last reported for a topic, if any.
    ///
    /// `TOPIC_AUTHORIZATION_FAILED` and `UNKNOWN_TOPIC_OR_PARTITION` call for
    /// different handling, so the reason a topic is missing is kept. Mirrors
    /// `Metadata.getError(topic)` in the Java client. Cleared for a topic as
    /// soon as it comes back without an error.
    topic_errors: AHashMap<String, ErrorCode>,
    /// When the snapshot was built.
    last_updated: Instant,
    /// Incremented by every write to the snapshot.
    generation: u64,
    /// Incremented by every rebootstrap. A fetch that started before a
    /// rebootstrap is discarded when it completes after it.
    reset_epoch: u64,
}

impl MetadataCache {
    fn new() -> Self {
        Self {
            cluster_id: None,
            controller_id: -1,
            brokers: AHashMap::new(),
            topics: AHashMap::new(),
            topic_ids: AHashMap::new(),
            name_to_topic_id: AHashMap::new(),
            topic_stamps: AHashMap::new(),
            topic_errors: AHashMap::new(),
            last_updated: Instant::now(),
            generation: 0,
            reset_epoch: 0,
        }
    }

    fn is_stale(&self, max_age: Duration) -> bool {
        self.last_updated.elapsed() > max_age
    }

    /// Whether `topic` is present **and** was itself fetched within `max_age`.
    ///
    /// `last_updated` advances on every refresh, including a partial one for a
    /// different topic, so it says nothing about the age of one entry.
    fn topic_is_fresh(&self, topic: &str, max_age: Duration) -> bool {
        self.topics.contains_key(topic)
            && self
                .topic_stamps
                .get(topic)
                .is_some_and(|stamp| stamp.refreshed.elapsed() <= max_age)
    }

    /// Build the next snapshot from a metadata response.
    ///
    /// A full refresh is authoritative for topics; a partial one merges into
    /// the current topics, evicting those idle beyond `topic_ttl`. Either way
    /// the broker map becomes the response's brokers.
    ///
    /// Within one topic ID a partition's cached leader epoch is never replaced
    /// by an older one (KIP-320). When the topic ID changed — the topic was
    /// deleted and re-created — the incoming partitions replace the cached
    /// ones regardless of epoch.
    fn merge(
        &self,
        response: MetadataResponse,
        full_refresh: bool,
        topic_ttl: Option<Duration>,
    ) -> Self {
        let now = Instant::now();
        let now_ms = now_millis();

        let brokers: AHashMap<BrokerId, BrokerInfo> = response
            .brokers
            .into_iter()
            .map(|b| {
                (
                    b.node_id,
                    BrokerInfo::new(b.node_id, b.host, b.port, b.rack),
                )
            })
            .collect();

        let retained = |name: &String| -> bool {
            match topic_ttl {
                None => true,
                Some(ttl) => self
                    .topic_stamps
                    .get(name)
                    .is_some_and(|stamp| stamp.is_live(now, now_ms, ttl)),
            }
        };

        let mut topics: AHashMap<String, Arc<TopicInfo>> = if full_refresh {
            AHashMap::new()
        } else {
            let kept: AHashMap<_, _> = self
                .topics
                .iter()
                .filter(|(name, _)| retained(name))
                .map(|(k, v)| (k.clone(), Arc::clone(v)))
                .collect();
            let evicted = self.topics.len() - kept.len();
            if evicted > 0 {
                debug!(evicted, "evicted idle topics from metadata cache");
            }
            kept
        };
        let mut topic_errors: AHashMap<String, ErrorCode> = if full_refresh {
            AHashMap::new()
        } else {
            self.topic_errors
                .iter()
                .filter(|(name, _)| match topic_ttl {
                    None => true,
                    Some(_) => topics.contains_key(name.as_str()),
                })
                .map(|(k, v)| (k.clone(), *v))
                .collect()
        };
        // Topics this response fetched; their stamps are renewed below.
        let mut fetched: Vec<String> = Vec::new();

        for topic in response.topics {
            let Some(name) = topic.name else {
                continue;
            };

            if !topic.error_code.is_ok() {
                topic_errors.insert(name.clone(), topic.error_code);
                // An unknown topic was deleted (or never existed) and leaves
                // the cache; other retriable errors are transient.
                let gone = matches!(
                    topic.error_code,
                    ErrorCode::UnknownTopicOrPartition | ErrorCode::UnknownTopicId
                );
                if topic.error_code.is_retriable() && !gone {
                    // The broker knows the topic but cannot describe it right
                    // now (LEADER_NOT_AVAILABLE while it is created, for
                    // example). Keep the previous entry, restoring it if the
                    // idle TTL just evicted it.
                    debug!(topic = %name, error = ?topic.error_code, "transient topic error; keeping cached entry");
                    if !topics.contains_key(&name)
                        && let Some(previous) = self.topics.get(&name)
                    {
                        topics.insert(name.clone(), Arc::clone(previous));
                    }
                    if topics.contains_key(&name) {
                        fetched.push(name);
                    }
                } else {
                    warn!(topic = %name, error = ?topic.error_code, "topic metadata error");
                    topics.remove(&name);
                }
                continue;
            }

            let topic_id = topic.topic_id.unwrap_or([0; 16]);
            let cached = self.topics.get(&name).filter(|cached| {
                // A different non-zero ID is a different topic: its epochs
                // start again from zero and must not be compared with the
                // deleted topic's.
                let recreated = topic_id != [0; 16]
                    && cached.topic_id != [0; 16]
                    && cached.topic_id != topic_id;
                if recreated {
                    info!(topic = %name, "topic ID changed; the topic was re-created");
                }
                !recreated
            });

            // Every partition the broker reported is retained, including
            // errored ones, so `partition_count()` stays stable.
            let partitions: std::collections::HashMap<PartitionId, PartitionInfo> = topic
                .partitions
                .into_iter()
                .map(|p| {
                    let healthy = p.error_code.is_ok();
                    if !healthy {
                        debug!(
                            topic = %name,
                            partition = p.partition_index,
                            error = ?p.error_code,
                            "partition reported an error; retaining entry with no leader"
                        );
                    }
                    let incoming = PartitionInfo {
                        topic: name.clone(),
                        partition: p.partition_index,
                        leader: if healthy { p.leader_id } else { -1 },
                        leader_epoch: if healthy { p.leader_epoch } else { -1 },
                        replicas: p.replica_nodes,
                        isr: p.isr_nodes,
                        offline_replicas: p.offline_replicas,
                        error_code: p.error_code,
                    };

                    // KIP-320: a lagging broker can answer with an older epoch
                    // than the cached one; keep the newer entry. An epoch of -1
                    // is unknown and never takes part in the comparison.
                    let merged = match cached.and_then(|t| t.partitions.get(&p.partition_index)) {
                        Some(previous)
                            if previous.leader_epoch >= 0
                                && incoming.leader_epoch >= 0
                                && incoming.leader_epoch < previous.leader_epoch =>
                        {
                            debug!(
                                topic = %name,
                                partition = p.partition_index,
                                cached_epoch = previous.leader_epoch,
                                response_epoch = incoming.leader_epoch,
                                "ignoring stale leader epoch from metadata response (KIP-320)"
                            );
                            previous.clone()
                        }
                        _ => incoming,
                    };
                    (p.partition_index, merged)
                })
                .collect();

            topic_errors.remove(&name);
            fetched.push(name.clone());
            topics.insert(
                name.clone(),
                Arc::new(TopicInfo {
                    name,
                    topic_id,
                    is_internal: topic.is_internal,
                    partitions,
                }),
            );
        }

        let mut topic_stamps: AHashMap<String, Arc<TopicStamp>> = self
            .topic_stamps
            .iter()
            .filter(|(name, _)| topics.contains_key(name.as_str()))
            .map(|(k, v)| (k.clone(), Arc::clone(v)))
            .collect();
        for name in fetched {
            let last_used = topic_stamps
                .get(&name)
                .map_or(0, |stamp| stamp.last_used_ms.load(Ordering::Relaxed));
            topic_stamps.insert(name, TopicStamp::fetched_now(last_used));
        }

        let mut topic_ids: AHashMap<[u8; 16], Arc<String>> = AHashMap::new();
        let mut name_to_topic_id: AHashMap<String, [u8; 16]> = AHashMap::new();
        for (name, info) in &topics {
            if info.topic_id != [0; 16] {
                topic_ids.insert(info.topic_id, Arc::new(name.clone()));
                name_to_topic_id.insert(name.clone(), info.topic_id);
            }
        }

        Self {
            cluster_id: response.cluster_id,
            controller_id: response.controller_id,
            brokers,
            topics,
            topic_ids,
            name_to_topic_id,
            topic_stamps,
            topic_errors,
            last_updated: now,
            generation: self.generation + 1,
            reset_epoch: self.reset_epoch,
        }
    }
}

/// Topics requested from the writer since its last fetch.
#[derive(Default)]
struct PendingFetch {
    /// Somebody asked for every topic.
    full: bool,
    /// Topics asked for by name.
    topics: AHashSet<String>,
    /// Callers waiting for the fetch that covers their request.
    waiters: Vec<oneshot::Sender<Result<()>>>,
    /// A rebootstrap was requested.
    rebootstrap: bool,
}

impl PendingFetch {
    fn is_empty(&self) -> bool {
        !self.full && !self.rebootstrap && self.waiters.is_empty()
    }
}

/// State shared by a [`ClusterMetadata`] handle and its writer task.
struct Inner {
    /// Bootstrap servers; replaceable at runtime (KIP-899).
    bootstrap_servers: ArcSwap<Vec<String>>,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// The current snapshot. Loaded lock-free; stored only under `write_lock`.
    cache: ArcSwap<MetadataCache>,
    /// Serializes every store to `cache`. Held for the in-memory merge only,
    /// never across an `.await`.
    write_lock: SyncMutex<()>,
    /// Metadata max age before a topic entry counts as stale.
    max_age: Duration,
    /// Backoff between fetches (KIP-580). `None` disables it.
    retry_backoff: Option<BackoffPolicy>,
    /// Rate-limiter state, owned by the writer.
    refresh_backoff: SyncMutex<RefreshBackoffState>,
    /// `metadata.recovery.strategy` (KIP-899).
    recovery_strategy: MetadataRecoveryStrategy,
    /// How long fetches may keep failing before a rebootstrap (KIP-1102).
    rebootstrap_trigger: Duration,
    /// Upper bound on the random delay before a rebootstrap.
    rebootstrap_jitter: Duration,
    /// Start of the current streak of failed fetches. Cleared by a successful
    /// fetch; set to *now* by a rebootstrap so the next one needs another full
    /// trigger period.
    metadata_attempt_start: SyncMutex<Option<Instant>>,
    /// Idle TTL for topic entries (`metadata.max.idle.ms`). `None` disables
    /// eviction.
    topic_cache_ttl: Option<Duration>,
    /// `allow.auto.create.topics` on topic-specific requests.
    auto_create_topics: bool,
    /// Requests waiting for the writer.
    pending: SyncMutex<PendingFetch>,
    /// Wakes the writer.
    wake: Notify,
    /// Whether the writer task has been spawned.
    writer_started: AtomicBool,
    /// Set when the owning handle is dropped; stops the writer.
    closed: AtomicBool,
}

/// Cluster metadata manager.
///
/// Reads are lock-free snapshot loads. Fetches go through the writer task;
/// see the module documentation.
pub struct ClusterMetadata {
    inner: Arc<Inner>,
}

impl Drop for ClusterMetadata {
    fn drop(&mut self) {
        self.inner.closed.store(true, Ordering::Release);
        self.inner.wake.notify_one();
    }
}

impl ClusterMetadata {
    /// Create a new cluster metadata manager.
    ///
    /// The `with_*` methods configure it; they take effect only before the
    /// first fetch.
    pub fn new(
        bootstrap_servers: Vec<String>,
        pool: Arc<ConnectionPool>,
        max_age: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                bootstrap_servers: ArcSwap::from_pointee(bootstrap_servers),
                pool,
                cache: ArcSwap::from_pointee(MetadataCache::new()),
                write_lock: SyncMutex::new(()),
                max_age,
                retry_backoff: Some(Inner::default_retry_backoff_policy()),
                refresh_backoff: SyncMutex::new(RefreshBackoffState::new()),
                recovery_strategy: MetadataRecoveryStrategy::default(),
                rebootstrap_trigger: Duration::from_secs(300),
                rebootstrap_jitter: Duration::from_millis(500),
                metadata_attempt_start: SyncMutex::new(None),
                topic_cache_ttl: Some(Duration::from_secs(300)),
                auto_create_topics: false,
                pending: SyncMutex::new(PendingFetch::default()),
                wake: Notify::new(),
                writer_started: AtomicBool::new(false),
                closed: AtomicBool::new(false),
            }),
        }
    }

    /// Apply a configuration change. Configuration is fixed once the writer
    /// task has started, which happens on the first fetch.
    fn configure(mut self, apply: impl FnOnce(&mut Inner)) -> Self {
        match Arc::get_mut(&mut self.inner) {
            Some(inner) => apply(inner),
            None => warn!("ClusterMetadata is already in use; configuration change ignored"),
        }
        self
    }

    /// Set the metadata recovery strategy, i.e. `metadata.recovery.strategy`
    /// (KIP-899). Default: [`MetadataRecoveryStrategy::Rebootstrap`].
    #[must_use]
    pub fn with_recovery_strategy(self, strategy: MetadataRecoveryStrategy) -> Self {
        self.configure(|inner| inner.recovery_strategy = strategy)
    }

    /// Set how long metadata fetches may keep failing before the client
    /// rebootstraps (`metadata.recovery.rebootstrap.trigger.ms`, KIP-1102).
    /// Only effective with [`MetadataRecoveryStrategy::Rebootstrap`].
    /// Default: 300 s.
    #[must_use]
    pub fn with_rebootstrap_trigger(self, duration: Duration) -> Self {
        self.configure(|inner| inner.rebootstrap_trigger = duration)
    }

    /// Set the topic cache TTL for partial refreshes, i.e.
    /// `metadata.max.idle.ms`.
    ///
    /// During a partial refresh, cached topics neither used nor fetched within
    /// this duration are evicted. Producing to a topic, resolving its leader,
    /// asking for its partition count, or naming it in a refresh all count as
    /// use, as `ProducerMetadata.add` does in the Java client.
    ///
    /// Full refreshes always rebuild the topic set from the response.
    ///
    /// Default: 5 minutes (matching Java's `metadata.max.idle.ms`).
    #[must_use]
    pub fn with_topic_cache_ttl(self, ttl: Duration) -> Self {
        self.configure(|inner| inner.topic_cache_ttl = Some(ttl))
    }

    /// Disable topic cache TTL eviction: partial refreshes keep every cached
    /// topic indefinitely.
    #[must_use]
    pub fn with_topic_cache_ttl_disabled(self) -> Self {
        self.configure(|inner| inner.topic_cache_ttl = None)
    }

    /// Allow the broker to create a topic this client asks about but the
    /// cluster does not have, i.e. `allow.auto.create.topics`.
    ///
    /// The flag rides on topic-specific metadata requests only. The broker
    /// must additionally be configured with `auto.create.topics.enable=true`.
    ///
    /// Defaults to `false`: a typo'd topic name that silently materialises a
    /// real topic reports nothing until the traffic is found missing. Turn it
    /// on for development and test clusters.
    #[must_use]
    pub fn with_auto_create_topics(self, allow: bool) -> Self {
        self.configure(|inner| inner.auto_create_topics = allow)
    }

    /// Set the **base** delay between metadata fetches.
    ///
    /// After `n` consecutive failed fetches the delay is `backoff × 2^(n-1)`,
    /// capped by [`with_retry_backoff_max`](Self::with_retry_backoff_max) and
    /// jittered; a successful fetch resets it to `backoff`. Mirrors
    /// `retry.backoff.ms`. Default: 100 ms.
    ///
    /// `None` disables the backoff: every request the writer receives is
    /// fetched immediately. Useful in tests only.
    ///
    /// If `backoff` exceeds the configured maximum, the maximum is raised to
    /// match.
    #[must_use]
    pub fn with_retry_backoff(self, backoff: impl Into<Option<Duration>>) -> Self {
        let backoff = backoff.into();
        self.configure(|inner| {
            inner.retry_backoff = backoff.map(|base| {
                let mut policy = inner
                    .retry_backoff
                    .take()
                    .unwrap_or_else(Inner::default_retry_backoff_policy);
                policy.initial_backoff = base;
                policy.max_backoff = policy.max_backoff.max(base);
                policy
            });
        })
    }

    /// Set the ceiling of the exponential metadata backoff, i.e.
    /// `retry.backoff.max.ms`. Default: 1 s. Values below the base delay are
    /// raised to it. No effect when the backoff is disabled.
    #[must_use]
    pub fn with_retry_backoff_max(self, max_backoff: Duration) -> Self {
        self.configure(|inner| {
            if let Some(policy) = inner.retry_backoff.as_mut() {
                policy.max_backoff = max_backoff.max(policy.initial_backoff);
            }
        })
    }

    /// Replace the whole metadata backoff policy.
    #[must_use]
    pub fn with_retry_backoff_policy(self, policy: BackoffPolicy) -> Self {
        self.configure(|inner| inner.retry_backoff = Some(policy))
    }

    /// Set the upper bound on the random delay applied before a rebootstrap.
    ///
    /// The delay is sampled uniformly from `[0, jitter)` so that a fleet which
    /// lost the cluster at the same instant does not arrive at one seed broker
    /// as a single wave. Default: 500 ms; `Duration::ZERO` rebootstraps
    /// immediately.
    #[must_use]
    pub fn with_rebootstrap_jitter(self, jitter: Duration) -> Self {
        self.configure(|inner| inner.rebootstrap_jitter = jitter)
    }

    /// Get the bootstrap servers.
    pub fn bootstrap_servers(&self) -> Vec<String> {
        (**self.inner.bootstrap_servers.load()).clone()
    }

    /// How long cached metadata may be used before a refresh.
    pub(crate) fn max_age(&self) -> Duration {
        self.inner.max_age
    }

    /// Fetch metadata for every topic in the cluster.
    pub async fn refresh(&self) -> Result<()> {
        self.request(None, true).await
    }

    /// Make sure metadata for `topics` is current, fetching it when a topic is
    /// missing or older than the metadata max age. `None` fetches every topic.
    ///
    /// Returns when the writer has applied a response that covers the
    /// request, or with the error of the fetch that tried.
    pub async fn refresh_for_topics(&self, topics: Option<&[&str]>) -> Result<()> {
        self.request(topics, false).await
    }

    /// Fetch metadata for `topics` even when the cached entries are current.
    ///
    /// Use this when a broker has said the cache is wrong rather than old —
    /// `NOT_LEADER_OR_FOLLOWER`, `FENCED_LEADER_EPOCH`, an unknown leader. The
    /// writer's backoff still applies, so a burst of such errors becomes one
    /// fetch per backoff interval. Mirrors `Metadata.requestUpdate()`.
    pub async fn force_refresh(&self, topics: Option<&[&str]>) -> Result<()> {
        self.request(topics, true).await
    }

    /// Hand a request to the writer and wait for the fetch that covers it.
    async fn request(&self, topics: Option<&[&str]>, force: bool) -> Result<()> {
        if let Some(names) = topics {
            self.touch_topics(names);
            if !force && self.inner.all_fresh(names) {
                return Ok(());
            }
        }

        let receiver = {
            let mut pending = self.inner.pending.lock();
            match topics {
                None => pending.full = true,
                Some(names) => pending
                    .topics
                    .extend(names.iter().map(|name| (*name).to_string())),
            }
            let (sender, receiver) = oneshot::channel();
            pending.waiters.push(sender);
            receiver
        };
        self.wake_writer();

        receiver
            .await
            .unwrap_or_else(|_| Err(KrafkaError::closed("the metadata writer has stopped")))
    }

    /// Spawn the writer on first use and wake it.
    fn wake_writer(&self) {
        if !self.inner.writer_started.swap(true, Ordering::AcqRel) {
            tokio::spawn(run_writer(Arc::clone(&self.inner)));
        }
        self.inner.wake.notify_one();
    }

    /// Replace the bootstrap server list at runtime (KIP-899).
    ///
    /// Takes effect on the next connection attempt that falls back to the
    /// bootstrap servers; existing connections stay open.
    ///
    /// # Errors
    ///
    /// Returns an error if `servers` is empty.
    pub fn update_seed_brokers(&self, servers: Vec<String>) -> Result<()> {
        if servers.is_empty() {
            return Err(KrafkaError::config(
                "update_seed_brokers: at least one server required",
            ));
        }
        info!(count = servers.len(), "Updating seed brokers (KIP-899)");
        self.inner.bootstrap_servers.store(Arc::new(servers));
        Ok(())
    }

    /// Rebootstrap now: drop the cluster view and rediscover the cluster from
    /// the bootstrap servers on the next fetch (KIP-899).
    ///
    /// A random delay of up to
    /// [`with_rebootstrap_jitter`](Self::with_rebootstrap_jitter) precedes the
    /// reset. In-flight requests are not aborted, and connections stay open;
    /// a fetch that was in flight when the reset happened is discarded rather
    /// than applied. Seed addresses are `host:port` strings resolved at dial
    /// time, so brokers that moved to new IPs behind the same name are found.
    pub async fn rebootstrap(&self) {
        self.inner.rebootstrap("requested").await;
    }

    /// Ask the writer to rebootstrap before its next fetch, without waiting.
    ///
    /// For protocol paths that learn the cluster changed outside a Metadata
    /// response, such as `REBOOTSTRAP_REQUIRED` in `ApiVersions` (KIP-1242).
    #[allow(dead_code)]
    pub(crate) fn request_rebootstrap(&self) {
        self.inner.pending.lock().rebootstrap = true;
        self.wake_writer();
    }

    /// Get broker info by ID.
    pub fn broker(&self, broker_id: BrokerId) -> Option<BrokerInfo> {
        self.inner.cache.load().brokers.get(&broker_id).cloned()
    }

    /// Get all brokers.
    pub fn brokers(&self) -> Vec<BrokerInfo> {
        let mut brokers: Vec<BrokerInfo> =
            self.inner.cache.load().brokers.values().cloned().collect();
        brokers.sort_by_key(BrokerInfo::id);
        brokers
    }

    /// Get topic info by name, deep-cloning the entry.
    ///
    /// Prefer [`topic_arc`](Self::topic_arc), which is an `Arc` ref-count bump
    /// instead of a full copy of the topic's partition map.
    pub fn topic(&self, name: &str) -> Option<TopicInfo> {
        self.topic_arc(name).map(|t| t.as_ref().clone())
    }

    /// Get topic info by name without copying the partition map.
    pub fn topic_arc(&self, name: &str) -> Option<Arc<TopicInfo>> {
        self.touch_topic(name);
        self.inner.cache.load().topics.get(name).map(Arc::clone)
    }

    /// Resolve a 16-byte topic UUID to a topic name.
    ///
    /// Returns `None` if the UUID is unknown — the caller should refresh and
    /// retry.
    pub fn topic_name_for_id(&self, topic_id: &[u8; 16]) -> Option<String> {
        let name = self
            .inner
            .cache
            .load()
            .topic_ids
            .get(topic_id)
            .map(|name| (**name).clone());
        if let Some(name) = name.as_deref() {
            self.touch_topic(name);
        }
        name
    }

    /// Resolve a topic name to its 16-byte UUID.
    ///
    /// Returns `None` if the topic is unknown or the broker did not report a
    /// topic ID — the caller should refresh and retry.
    pub fn topic_id_for_name(&self, name: &str) -> Option<[u8; 16]> {
        self.touch_topic(name);
        self.inner.cache.load().name_to_topic_id.get(name).copied()
    }

    /// Get all topics, deep-cloning every entry.
    ///
    /// Prefer [`topics_arc`](Self::topics_arc), which avoids copying every
    /// topic's partition map.
    pub fn topics(&self) -> Vec<TopicInfo> {
        self.inner
            .cache
            .load()
            .topics
            .values()
            .map(|t| t.as_ref().clone())
            .collect()
    }

    /// Get all topics without copying their partition maps.
    pub fn topics_arc(&self) -> Vec<Arc<TopicInfo>> {
        self.inner
            .cache
            .load()
            .topics
            .values()
            .map(Arc::clone)
            .collect()
    }

    /// Get the leader for a topic partition.
    pub fn leader(&self, topic: &str, partition: PartitionId) -> Option<BrokerId> {
        self.touch_topic(topic);
        self.inner
            .cache
            .load()
            .topics
            .get(topic)
            .and_then(|t| t.leader(partition))
    }

    /// Get the leader epoch for a topic partition.
    ///
    /// Returns `None` if the topic/partition is not found in metadata or the
    /// epoch is unknown.
    pub fn leader_epoch(&self, topic: &str, partition: PartitionId) -> Option<i32> {
        self.touch_topic(topic);
        self.inner
            .cache
            .load()
            .topics
            .get(topic)
            .and_then(|t| t.leader_epoch(partition))
    }

    /// Apply a leader reported by a broker in a Fetch/Produce response (KIP-951).
    ///
    /// When leadership moves, the broker that rejected the request with
    /// `NOT_LEADER_OR_FOLLOWER` / `FENCED_LEADER_EPOCH` names the node that
    /// should have received it and advertises its endpoint. Folding that into
    /// the cache lets the next attempt go to the right broker without a
    /// metadata round trip, for every user of this [`ClusterMetadata`].
    ///
    /// # Epoch rule
    ///
    /// The hint is ignored unless `leader_epoch` is strictly newer than the
    /// cached epoch (KIP-320). A cached epoch of `-1` is always superseded; a
    /// hint whose own epoch is `-1` never updates the partition.
    ///
    /// # Reachability
    ///
    /// `endpoint` is registered in the broker map, so a node the cache has
    /// never seen becomes routable immediately. When `endpoint` is `None` and
    /// `leader_id` is unknown the hint is dropped and `false` is returned.
    ///
    /// The hint never marks the topic as freshly fetched.
    ///
    /// The update goes through the same serialized write path as a fetch, so
    /// a fetch completing later with an older epoch keeps the hint.
    ///
    /// Returns `true` if the cache changed.
    pub fn apply_leader_hint(
        &self,
        topic: &str,
        partition: PartitionId,
        leader_id: BrokerId,
        leader_epoch: i32,
        endpoint: Option<BrokerInfo>,
    ) -> bool {
        if leader_id < 0 {
            return false;
        }
        self.touch_topic(topic);

        self.inner.write(|current| {
            let endpoint_is_new = endpoint.as_ref().is_some_and(|info| {
                current
                    .brokers
                    .get(&info.id())
                    .is_none_or(|known| known.address() != info.address())
            });
            let reachable = endpoint.is_some() || current.brokers.contains_key(&leader_id);
            let partition_is_new = reachable
                && leader_epoch >= 0
                && current
                    .topics
                    .get(topic)
                    .and_then(|t| t.partition(partition))
                    .is_some_and(|p| p.leader_epoch < 0 || leader_epoch > p.leader_epoch);

            if !endpoint_is_new && !partition_is_new {
                return None;
            }

            let mut next = current.clone();
            next.generation += 1;
            if endpoint_is_new && let Some(info) = endpoint.clone() {
                debug!(
                    node_id = info.id(),
                    address = info.address(),
                    "registering broker endpoint advertised with a leader hint (KIP-951)"
                );
                next.brokers.insert(info.id(), info);
            }
            if partition_is_new && let Some(cached_topic) = next.topics.get(topic) {
                let mut updated = TopicInfo::clone(cached_topic);
                if let Some(p) = updated.partitions.get_mut(&partition) {
                    debug!(
                        topic,
                        partition,
                        leader_id,
                        leader_epoch,
                        previous_leader = p.leader,
                        previous_epoch = p.leader_epoch,
                        "applying broker-reported leader (KIP-951)"
                    );
                    p.leader = leader_id;
                    p.leader_epoch = leader_epoch;
                    // The broker just named a live leader, so a stale
                    // partition error must not keep it unroutable.
                    p.error_code = ErrorCode::None;
                }
                next.topics.insert(topic.to_string(), Arc::new(updated));
            }
            Some(next)
        })
    }

    /// Get a connection to the leader of a partition.
    ///
    /// Fetches this topic's metadata first when the entry is stale, and forces
    /// a fetch when the cache cannot route the partition.
    ///
    /// # Errors
    ///
    /// A partition with no leader, or a leader missing from the broker map,
    /// is a retriable [`KrafkaError::Broker`] with
    /// [`ErrorCode::LeaderNotAvailable`]. A topic the broker reported an error
    /// for carries that code instead.
    pub async fn get_leader_connection(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<Arc<BrokerConnection>> {
        self.touch_topic(topic);

        let resolve = |cache: &MetadataCache| -> Option<(BrokerId, String)> {
            let leader_id = cache.topics.get(topic).and_then(|t| t.leader(partition))?;
            let address = cache.brokers.get(&leader_id)?.address().to_string();
            Some((leader_id, address))
        };

        let (resolved, fresh) = {
            let cache = self.inner.cache.load();
            (
                resolve(&cache),
                cache.topic_is_fresh(topic, self.inner.max_age),
            )
        };

        let (leader_id, address) = match resolved {
            Some(found) if fresh => found,
            stale => {
                // An unroutable partition forces a fetch: its entry may be
                // fresh by age and still wrong.
                self.request(Some(&[topic]), stale.is_none()).await?;
                let cache = self.inner.cache.load();
                match resolve(&cache) {
                    Some(found) => found,
                    None => {
                        let code = cache
                            .topic_errors
                            .get(topic)
                            .copied()
                            .unwrap_or(ErrorCode::LeaderNotAvailable);
                        return Err(KrafkaError::broker(
                            code,
                            format!("no routable leader for {topic}-{partition}"),
                        ));
                    }
                }
            }
        };

        self.inner
            .pool
            .get_connection_by_id(leader_id, &address)
            .await
    }

    /// Get a connection to a specific broker by ID.
    ///
    /// Fetches the broker list once when the ID is unknown.
    ///
    /// # Errors
    ///
    /// [`ErrorCode::BrokerNotAvailable`] when the broker is not in the cluster
    /// metadata after that fetch.
    pub async fn get_broker_connection(
        &self,
        broker_id: BrokerId,
    ) -> Result<Arc<BrokerConnection>> {
        let address = match self.broker(broker_id) {
            Some(broker) => broker.address().to_string(),
            None => {
                self.force_refresh(Some(&[])).await?;
                self.broker(broker_id)
                    .ok_or_else(|| {
                        KrafkaError::broker(
                            ErrorCode::BrokerNotAvailable,
                            format!("broker {broker_id} is not in the cluster metadata"),
                        )
                    })?
                    .address()
                    .to_string()
            }
        };

        self.inner
            .pool
            .get_connection_by_id(broker_id, &address)
            .await
    }

    /// Get the controller broker.
    ///
    /// Returns `None` when the cluster has not reported a controller yet, when
    /// the controller ID is negative (none elected, briefly normal during
    /// failover), or when the reported ID is not among the known brokers.
    pub fn controller(&self) -> Option<BrokerInfo> {
        let cache = self.inner.cache.load();
        if cache.controller_id < 0 {
            return None;
        }
        cache.brokers.get(&cache.controller_id).cloned()
    }

    /// Get the cluster ID.
    pub fn cluster_id(&self) -> Option<String> {
        self.inner.cache.load().cluster_id.clone()
    }

    /// Check if metadata needs refresh.
    pub fn needs_refresh(&self) -> bool {
        self.inner.cache.load().is_stale(self.inner.max_age)
    }

    /// Get partition count for a topic from the cache, without fetching.
    ///
    /// Returns `None` when the topic is not cached. Callers that need the
    /// count in order to make progress should use
    /// [`ensure_partition_count`](Self::ensure_partition_count).
    pub fn partition_count(&self, topic: &str) -> Option<usize> {
        self.touch_topic(topic);
        self.inner
            .cache
            .load()
            .topics
            .get(topic)
            .map(|t| t.partition_count())
    }

    /// Resolve the partition count for `topic`, fetching metadata for it when
    /// the cache does not have it.
    ///
    /// The client's equivalent of `KafkaProducer.waitOnMetadata`. A cache miss
    /// is not evidence that a topic does not exist: it may never have been
    /// fetched, it may have been evicted as idle, or it may be in the middle of
    /// being created.
    ///
    /// The call keeps fetching until `max_wait` elapses, so a topic that is
    /// still being created resolves as soon as the cluster settles. Fatal
    /// topic errors — `TOPIC_AUTHORIZATION_FAILED`, `INVALID_TOPIC_EXCEPTION` —
    /// are returned immediately, matching `Metadata.maybeThrowExceptionForTopic`.
    ///
    /// # Errors
    ///
    /// - [`KrafkaError::Broker`] with the code the broker reported for the
    ///   topic, when the broker gave a reason.
    /// - [`KrafkaError::Timeout`] when `max_wait` elapses with no answer.
    /// - The underlying fetch error when it is not retriable.
    pub async fn ensure_partition_count(&self, topic: &str, max_wait: Duration) -> Result<usize> {
        // A topic with zero partitions is what a topic mid-creation looks
        // like; keep waiting rather than hand a partitioner a modulus of zero.
        if let Some(count) = self.partition_count(topic).filter(|count| *count > 0) {
            return Ok(count);
        }

        let deadline = Instant::now() + max_wait;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, self.force_refresh(Some(&[topic]))).await {
                Err(_) => break,
                Ok(Ok(())) => {
                    if let Some(count) = self.partition_count(topic).filter(|count| *count > 0) {
                        return Ok(count);
                    }
                }
                Ok(Err(e)) if !e.is_retriable() => return Err(e),
                Ok(Err(e)) => {
                    debug!(topic, error = %e, "metadata fetch for an unknown topic failed; retrying within max_wait");
                }
            }

            if let Some(code) = self.topic_error(topic)
                && !code.is_retriable()
            {
                return Err(KrafkaError::broker(
                    code,
                    format!("metadata for topic {topic} was rejected by the broker"),
                ));
            }

            // The writer spaces forced fetches by its backoff; with the
            // backoff disabled, space them here.
            if self.inner.retry_backoff.is_none() {
                let remaining = deadline.saturating_duration_since(Instant::now());
                tokio::time::sleep(DEFAULT_RETRY_BACKOFF.min(remaining)).await;
            }
        }

        Err(match self.topic_error(topic) {
            Some(code) => KrafkaError::broker(
                code,
                format!(
                    "topic {topic} is still not present in cluster metadata after {} ms",
                    max_wait.as_millis()
                ),
            ),
            None => KrafkaError::timeout(format!(
                "topic {topic} is not present in cluster metadata after {} ms",
                max_wait.as_millis()
            )),
        })
    }

    /// The topic-level error the broker last reported for `topic`, if any.
    ///
    /// Mirrors `Metadata.getError(topic)` in the Java client. Cleared as soon
    /// as the topic comes back healthy.
    pub fn topic_error(&self, topic: &str) -> Option<ErrorCode> {
        self.inner.cache.load().topic_errors.get(topic).copied()
    }

    /// Mark `topic` as in use, resetting its idle timer.
    ///
    /// Every accessor that resolves a topic does this already. A topic that
    /// is not cached has no timer; asking for it in a refresh stamps it. No-op
    /// when TTL eviction is disabled.
    pub fn touch_topic(&self, topic: &str) {
        if self.inner.topic_cache_ttl.is_none() {
            return;
        }
        if let Some(stamp) = self.inner.cache.load().topic_stamps.get(topic) {
            stamp.last_used_ms.store(now_millis(), Ordering::Relaxed);
        }
    }

    /// Mark several topics as in use. See [`touch_topic`](Self::touch_topic).
    pub fn touch_topics(&self, topics: &[&str]) {
        if self.inner.topic_cache_ttl.is_none() {
            return;
        }
        let cache = self.inner.cache.load();
        let now = now_millis();
        for topic in topics {
            if let Some(stamp) = cache.topic_stamps.get(*topic) {
                stamp.last_used_ms.store(now, Ordering::Relaxed);
            }
        }
    }
}

impl Inner {
    /// The default metadata backoff: 100 ms base, doubling to a 1 s ceiling,
    /// with ±20% jitter.
    fn default_retry_backoff_policy() -> BackoffPolicy {
        BackoffPolicy {
            initial_backoff: DEFAULT_RETRY_BACKOFF,
            max_backoff: DEFAULT_RETRY_BACKOFF_MAX,
            backoff_multiplier: 2.0,
            jitter_factor: RETRY_BACKOFF_JITTER,
        }
    }

    /// Whether every name is cached and fetched within the max age.
    fn all_fresh(&self, names: &[&str]) -> bool {
        let cache = self.cache.load();
        !cache.brokers.is_empty()
            && names
                .iter()
                .all(|name| cache.topic_is_fresh(name, self.max_age))
    }

    /// Store a new snapshot computed from the current one. `update` returns
    /// `None` to leave the snapshot unchanged. Returns whether it changed.
    fn write(&self, update: impl FnOnce(&MetadataCache) -> Option<MetadataCache>) -> bool {
        let _serialized = self.write_lock.lock();
        let current = self.cache.load_full();
        match update(&current) {
            Some(next) => {
                self.cache.store(Arc::new(next));
                true
            }
            None => false,
        }
    }

    /// Apply a metadata response unless a rebootstrap happened since the fetch
    /// started (`reset_epoch` changed). Returns whether it was applied.
    fn apply(&self, response: MetadataResponse, full_refresh: bool, reset_epoch: u64) -> bool {
        let ttl = self.topic_cache_ttl;
        self.write(|current| {
            if current.reset_epoch != reset_epoch {
                debug!("discarding a metadata response fetched before a rebootstrap");
                return None;
            }
            let next = current.merge(response, full_refresh, ttl);
            debug!(
                brokers = next.brokers.len(),
                topics = next.topics.len(),
                "updated metadata"
            );
            Some(next)
        })
    }

    /// Time until the backoff permits the next fetch.
    fn backoff_remaining(&self) -> Option<Duration> {
        self.retry_backoff.as_ref()?;
        self.refresh_backoff.lock().remaining()
    }

    /// One fetch, as the writer runs it: count it toward the failure streak
    /// and the backoff.
    async fn refresh(&self, topics: Option<&[String]>) -> Result<()> {
        self.metadata_attempt_start
            .lock()
            .get_or_insert_with(Instant::now);
        let result = self.refresh_attempt(topics).await;
        if let Some(policy) = self.retry_backoff.as_ref() {
            let mut backoff = self.refresh_backoff.lock();
            match &result {
                Ok(()) => backoff.record_success(policy),
                Err(_) => backoff.record_failure(policy),
            }
        }
        result
    }

    /// Fetch metadata from some reachable broker and apply it.
    ///
    /// Rebootstraps (with [`MetadataRecoveryStrategy::Rebootstrap`]) when the
    /// failure streak exceeds the trigger, when no known broker is reachable,
    /// and on `REBOOTSTRAP_REQUIRED` — at most once per fetch.
    async fn refresh_attempt(&self, topics: Option<&[String]>) -> Result<()> {
        let rebootstrap_enabled = self.recovery_strategy == MetadataRecoveryStrategy::Rebootstrap;
        let mut rebootstrapped = false;
        if self.needs_rebootstrap() {
            self.rebootstrap("no successful metadata response within the rebootstrap trigger")
                .await;
            rebootstrapped = true;
        }

        // Bounded: each pass either returns or consumes the one rebootstrap,
        // and a response discarded by a concurrent rebootstrap is re-fetched
        // once against the new view.
        for _ in 0..3 {
            let reset_epoch = self.cache.load().reset_epoch;
            let conn = match self.get_any_connection().await {
                Ok(conn) => conn,
                Err(e) => {
                    if rebootstrap_enabled
                        && !rebootstrapped
                        && !self.cache.load().brokers.is_empty()
                    {
                        self.rebootstrap("no known broker is reachable").await;
                        rebootstrapped = true;
                        continue;
                    }
                    return Err(e);
                }
            };

            let version = conn
                .negotiate_api_version(
                    ApiKey::Metadata,
                    crate::protocol::versions::METADATA_MAX,
                    crate::protocol::versions::METADATA_MIN,
                )
                .unwrap_or(crate::protocol::versions::METADATA_MIN);

            // `allow_auto_topic_creation` rides only on the topic-specific
            // form; an all-topics request names nothing to create.
            let request = match topics {
                Some(names) => {
                    let mut request =
                        MetadataRequest::for_topics(names.iter().map(String::as_str).collect());
                    request.allow_auto_topic_creation = self.auto_create_topics;
                    request
                }
                None => MetadataRequest::all_topics(),
            };

            let mut response = conn
                .send_request(ApiKey::Metadata, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await?;
            let metadata = MetadataResponse::decode_versioned(version, &mut response)?;

            if metadata.error_code == ErrorCode::RebootstrapRequired
                && rebootstrap_enabled
                && !rebootstrapped
            {
                info!("broker requested a rebootstrap (REBOOTSTRAP_REQUIRED)");
                self.rebootstrap("REBOOTSTRAP_REQUIRED").await;
                rebootstrapped = true;
                continue;
            }
            if !metadata.error_code.is_ok() {
                return Err(KrafkaError::broker(
                    metadata.error_code,
                    "metadata request failed",
                ));
            }

            // Any successful response proves a broker is reachable.
            *self.metadata_attempt_start.lock() = None;

            if self.apply(metadata, topics.is_none(), reset_epoch) {
                return Ok(());
            }
        }

        Err(KrafkaError::unavailable(
            "metadata was reset while the fetch was in flight",
        ))
    }

    /// Drop the cluster view after a random delay of up to
    /// `rebootstrap_jitter`, so the next connection goes to the seeds.
    async fn rebootstrap(&self, reason: &str) {
        let delay = if self.rebootstrap_jitter.is_zero() {
            Duration::ZERO
        } else {
            use rand::Rng as _;
            let nanos = crate::util::with_rng(|rng| {
                rng.random_range(0..self.rebootstrap_jitter.as_nanos().max(1))
            });
            Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
        };
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        warn!(
            reason,
            "rebootstrapping: rediscovering the cluster from the bootstrap servers (KIP-899)"
        );
        self.reset_to_seeds();
    }

    /// Replace the snapshot with an empty one and restart the failure timer.
    fn reset_to_seeds(&self) {
        self.write(|current| {
            let mut next = MetadataCache::new();
            next.generation = current.generation + 1;
            next.reset_epoch = current.reset_epoch + 1;
            Some(next)
        });
        // Set to now, not cleared: another rebootstrap needs another full
        // trigger period of failure.
        *self.metadata_attempt_start.lock() = Some(Instant::now());
    }

    /// Whether the failure streak has outlasted the rebootstrap trigger.
    ///
    /// The trigger is extended by a random amount (up to 20%) so that clients
    /// that started failing together do not cross it on the same tick.
    fn needs_rebootstrap(&self) -> bool {
        if self.recovery_strategy != MetadataRecoveryStrategy::Rebootstrap {
            return false;
        }
        let Some(attempt_start) = *self.metadata_attempt_start.lock() else {
            return false;
        };
        let elapsed = attempt_start.elapsed();

        let effective_trigger = {
            use rand::Rng as _;
            let spread = self.rebootstrap_trigger.mul_f64(REBOOTSTRAP_TRIGGER_JITTER);
            if spread.is_zero() {
                self.rebootstrap_trigger
            } else {
                let nanos =
                    crate::util::with_rng(|rng| rng.random_range(0..spread.as_nanos().max(1)));
                self.rebootstrap_trigger
                    + Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
            }
        };

        elapsed >= effective_trigger
    }

    /// Get a connection to any available broker.
    ///
    /// Candidates are the cached brokers plus the bootstrap servers not
    /// already among them, shuffled and raced a few at a time.
    ///
    /// # Errors
    ///
    /// When no candidate connects, the error names every address tried and
    /// carries a failure as its source: an authentication failure on any
    /// address makes the whole error [`KrafkaError::Auth`]; otherwise it is a
    /// retriable network error with the last failure.
    async fn get_any_connection(&self) -> Result<Arc<BrokerConnection>> {
        let mut addrs = self.connection_candidates();
        if addrs.is_empty() {
            return Err(KrafkaError::unavailable(
                "no bootstrap servers or brokers to connect to",
            ));
        }
        {
            use rand::seq::SliceRandom as _;
            crate::util::with_rng(|rng| addrs.shuffle(rng));
        }

        use futures::StreamExt as _;

        // The most telling failure: an authentication failure wins over any
        // network one, because retrying will not fix it.
        let mut cause: Option<KrafkaError> = None;
        for chunk in addrs.chunks(CONNECT_FANOUT) {
            let mut attempts: futures::stream::FuturesUnordered<_> = chunk
                .iter()
                .map(|addr| {
                    let pool = Arc::clone(&self.pool);
                    let addr = addr.clone();
                    async move { pool.get_connection(&addr).await }
                })
                .collect();
            while let Some(attempt) = attempts.next().await {
                match attempt {
                    Ok(conn) => return Ok(conn),
                    Err(e) => {
                        if !matches!(cause, Some(KrafkaError::Auth { .. })) {
                            cause = Some(e);
                        }
                    }
                }
            }
        }

        Err(no_broker_reachable(&addrs, cause))
    }

    /// Every cached broker address, followed by each bootstrap server not
    /// already among them. Addresses are `host:port` strings resolved at dial
    /// time.
    fn connection_candidates(&self) -> Vec<String> {
        let cache = self.cache.load();
        let servers = self.bootstrap_servers.load();

        let mut addrs: Vec<String> = Vec::with_capacity(cache.brokers.len() + servers.len());
        let mut seen: AHashSet<&str> = AHashSet::with_capacity(cache.brokers.len());
        let mut brokers: Vec<&BrokerInfo> = cache.brokers.values().collect();
        brokers.sort_by_key(|b| b.id());
        for broker in brokers {
            if seen.insert(broker.address()) {
                addrs.push(broker.address().to_string());
            }
        }
        for s in servers.iter() {
            if seen.insert(s.as_str()) {
                addrs.push(s.clone());
            }
        }
        addrs
    }

    /// Fail every waiting request: the handle is gone.
    fn fail_pending(&self) {
        let pending = std::mem::take(&mut *self.pending.lock());
        for waiter in pending.waiters {
            let _ = waiter.send(Err(KrafkaError::closed("cluster metadata was dropped")));
        }
    }
}

/// The error for "no candidate address connected".
fn no_broker_reachable(addrs: &[String], last: Option<KrafkaError>) -> KrafkaError {
    let tried = addrs.join(", ");
    match last {
        Some(KrafkaError::Auth { message, source }) => KrafkaError::Auth {
            message: format!("could not connect to any of [{tried}]: {message}"),
            source,
        },
        Some(last) => KrafkaError::network(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            NoBrokerReachable { tried, last },
        )),
        None => KrafkaError::unavailable(format!("could not connect to any of [{tried}]")),
    }
}

/// Every candidate address failed; `last` is the final failure.
#[derive(Debug)]
struct NoBrokerReachable {
    tried: String,
    last: KrafkaError,
}

impl std::fmt::Display for NoBrokerReachable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "could not connect to any of [{}]; last error: {}",
            self.tried, self.last
        )
    }
}

impl std::error::Error for NoBrokerReachable {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.last)
    }
}

/// The writer task: the only code that fetches metadata and applies it.
///
/// Waits for requests, waits out the backoff (requests arriving meanwhile join
/// the same fetch), fetches the union of everything requested, and answers
/// every waiter with the outcome.
async fn run_writer(inner: Arc<Inner>) {
    loop {
        if inner.closed.load(Ordering::Acquire) {
            inner.fail_pending();
            return;
        }
        if inner.pending.lock().is_empty() {
            inner.wake.notified().await;
            continue;
        }

        if let Some(wait) = inner.backoff_remaining() {
            tokio::time::sleep(wait).await;
        }

        let batch = std::mem::take(&mut *inner.pending.lock());
        if batch.rebootstrap {
            inner.rebootstrap("requested by the protocol").await;
        }
        if batch.waiters.iter().all(oneshot::Sender::is_closed) {
            continue;
        }

        let topics: Option<Vec<String>> = if batch.full {
            None
        } else {
            Some(batch.topics.into_iter().collect())
        };
        let result = inner.refresh(topics.as_deref()).await;
        if let Err(e) = &result {
            debug!(error = %e, "metadata fetch failed");
        }
        for waiter in batch.waiters {
            let _ = waiter.send(result.clone());
        }
    }
}
#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    impl ClusterMetadata {
        /// Apply a response through the serialized write path.
        fn update_cache(&self, response: MetadataResponse, full_refresh: bool) {
            let epoch = self.inner.cache.load().reset_epoch;
            assert!(self.inner.apply(response, full_refresh, epoch));
        }
    }

    #[test]
    fn test_broker_info_address() {
        let broker = BrokerInfo::new(1, "localhost".to_string(), 9092, None);
        assert_eq!(broker.address(), "localhost:9092");
    }

    #[test]
    fn test_topic_info() {
        let topic = TopicInfo {
            name: "test".to_string(),
            topic_id: [0; 16],
            is_internal: false,
            partitions: [
                (
                    0,
                    PartitionInfo {
                        topic: "test".to_string(),
                        partition: 0,
                        leader: 1,
                        leader_epoch: 0,
                        replicas: vec![1, 2, 3],
                        isr: vec![1, 2, 3],
                        offline_replicas: vec![],
                        error_code: ErrorCode::None,
                    },
                ),
                (
                    1,
                    PartitionInfo {
                        topic: "test".to_string(),
                        partition: 1,
                        leader: 2,
                        leader_epoch: 0,
                        replicas: vec![2, 3, 1],
                        isr: vec![2, 3, 1],
                        offline_replicas: vec![],
                        error_code: ErrorCode::None,
                    },
                ),
            ]
            .into_iter()
            .collect(),
        };

        assert_eq!(topic.partition_count(), 2);
        assert_eq!(topic.leader(0), Some(1));
        assert_eq!(topic.leader(1), Some(2));
        assert_eq!(topic.leader(2), None);
    }

    #[test]
    fn test_metadata_cache_stale() {
        let cache = MetadataCache::new();
        assert!(!cache.is_stale(Duration::from_secs(60)));

        // Note: We can't easily test staleness without mocking time
    }

    #[test]
    fn test_metadata_cache_new_is_empty() {
        let cache = MetadataCache::new();
        assert!(cache.brokers.is_empty());
        assert!(cache.topics.is_empty());
        assert!(cache.cluster_id.is_none());
        assert_eq!(cache.controller_id, -1);
    }

    #[test]
    fn test_broker_info_with_rack() {
        let broker = BrokerInfo::new(
            1,
            "broker1.kafka.local".to_string(),
            9093,
            Some("us-east-1a".to_string()),
        );
        assert_eq!(broker.address(), "broker1.kafka.local:9093");
        assert_eq!(broker.rack(), Some("us-east-1a"));
    }

    #[test]
    fn test_metadata_cache_topic_ids() {
        let mut cache = MetadataCache::new();
        assert!(cache.topic_ids.is_empty());

        let uuid: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        cache
            .topic_ids
            .insert(uuid, Arc::new("my-topic".to_string()));
        assert_eq!(
            cache.topic_ids.get(&uuid),
            Some(&Arc::new("my-topic".to_string()))
        );
    }

    #[test]
    fn test_metadata_cache_new_has_empty_topic_ids() {
        let cache = MetadataCache::new();
        assert!(cache.topic_ids.is_empty());
    }

    #[test]
    fn test_metadata_recovery_strategy_default() {
        assert_eq!(
            MetadataRecoveryStrategy::default(),
            MetadataRecoveryStrategy::Rebootstrap,
            "KIP-1102 made rebootstrap the default"
        );
    }

    #[test]
    fn test_cluster_metadata_with_recovery_strategy() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        )
        .with_recovery_strategy(MetadataRecoveryStrategy::Rebootstrap)
        .with_rebootstrap_trigger(Duration::from_secs(60));

        assert_eq!(
            meta.inner.recovery_strategy,
            MetadataRecoveryStrategy::Rebootstrap
        );
        assert_eq!(meta.inner.rebootstrap_trigger, Duration::from_secs(60));
    }

    #[test]
    fn test_update_seed_brokers() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["broker1:9092".to_string()],
            pool,
            Duration::from_secs(300),
        );

        assert_eq!(meta.bootstrap_servers(), vec!["broker1:9092"]);

        meta.update_seed_brokers(vec!["broker2:9092".to_string(), "broker3:9092".to_string()])
            .unwrap();
        assert_eq!(
            meta.bootstrap_servers(),
            vec!["broker2:9092", "broker3:9092"]
        );
    }

    #[test]
    fn test_update_seed_brokers_rejects_empty() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["broker1:9092".to_string()],
            pool,
            Duration::from_secs(300),
        );

        let result = meta.update_seed_brokers(vec![]);
        assert!(result.is_err());
        // Original servers unchanged.
        assert_eq!(meta.bootstrap_servers(), vec!["broker1:9092"]);
    }

    #[test]
    fn test_needs_rebootstrap_never_fires_with_strategy_none() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        )
        .with_recovery_strategy(MetadataRecoveryStrategy::None)
        .with_rebootstrap_trigger(Duration::ZERO);
        *meta.inner.metadata_attempt_start.lock() = Some(Instant::now() - Duration::from_secs(1));

        assert!(!meta.inner.needs_rebootstrap());
    }

    #[test]
    fn test_needs_rebootstrap_not_yet_triggered() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        )
        .with_recovery_strategy(MetadataRecoveryStrategy::Rebootstrap)
        .with_rebootstrap_trigger(Duration::from_secs(300));

        // No attempt recorded yet — needs_rebootstrap should return false.
        assert!(!meta.inner.needs_rebootstrap());

        // Simulate that a refresh attempt has started.
        {
            let mut start = meta.inner.metadata_attempt_start.lock();
            *start = Some(Instant::now());
        }

        // Still shouldn't trigger — trigger is 300s, elapsed is ~0.
        assert!(!meta.inner.needs_rebootstrap());
        // Timestamp should still be recorded.
        assert!(meta.inner.metadata_attempt_start.lock().is_some());
    }

    #[tokio::test]
    async fn test_needs_rebootstrap_triggers_after_timeout() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        )
        .with_recovery_strategy(MetadataRecoveryStrategy::Rebootstrap)
        .with_rebootstrap_trigger(Duration::ZERO) // Zero trigger = immediate
        .with_rebootstrap_jitter(Duration::ZERO); // Keep the test deterministic

        // Simulate that a refresh attempt has started.
        {
            let mut start = meta.inner.metadata_attempt_start.lock();
            *start = Some(Instant::now());
        }

        // With a zero trigger, needs_rebootstrap should return true.
        assert!(meta.inner.needs_rebootstrap());

        // Perform the actual rebootstrap.
        meta.rebootstrap().await;

        // After rebootstrap, the attempt start should be set to Some(now) — not None.
        assert!(meta.inner.metadata_attempt_start.lock().is_some());
        // Cache should be reset.
        assert!(meta.inner.cache.load().brokers.is_empty());
    }

    #[tokio::test]
    async fn test_rebootstrap_clears_cache() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        )
        .with_rebootstrap_jitter(Duration::ZERO);

        // Manually inject some data into the cache.
        let mut cache = MetadataCache::new();
        cache
            .brokers
            .insert(1, BrokerInfo::new(1, "host".to_string(), 9092, None));
        meta.inner.cache.store(Arc::new(cache));
        assert!(!meta.inner.cache.load().brokers.is_empty());

        meta.rebootstrap().await;

        assert!(meta.inner.cache.load().brokers.is_empty());
        // After rebootstrap, timer is set to Some(now) — not cleared.
        assert!(meta.inner.metadata_attempt_start.lock().is_some());
    }

    #[test]
    fn test_topic_cache_ttl_default_is_five_minutes() {
        // Topic cache TTL must default to 5 min (matching Java's
        // `metadata.max.idle.ms`) to prevent unbounded metadata growth on
        // topic churn.
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        );
        assert_eq!(meta.inner.topic_cache_ttl, Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_topic_cache_ttl_disabled_opt_out() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        )
        .with_topic_cache_ttl_disabled();
        assert_eq!(meta.inner.topic_cache_ttl, None);
    }

    /// A metadata response listing `topic_names`, each with one healthy
    /// partition on broker 1.
    fn ok_topics_response(topic_names: &[&str]) -> MetadataResponse {
        use crate::protocol::{MetadataBroker, MetadataPartitionResponse, MetadataTopicResponse};
        MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![MetadataBroker {
                node_id: 1,
                host: "localhost".to_string(),
                port: 9092,
                rack: None,
            }],
            cluster_id: None,
            controller_id: 1,
            error_code: ErrorCode::None,
            topics: topic_names
                .iter()
                .map(|name| MetadataTopicResponse {
                    error_code: ErrorCode::None,
                    name: Some((*name).to_string()),
                    topic_id: None,
                    is_internal: false,
                    partitions: vec![MetadataPartitionResponse {
                        error_code: ErrorCode::None,
                        partition_index: 0,
                        leader_id: 1,
                        leader_epoch: 0,
                        replica_nodes: vec![1],
                        isr_nodes: vec![1],
                        offline_replicas: vec![],
                    }],
                })
                .collect(),
        }
    }

    fn ttl_metadata(ttl: Duration) -> ClusterMetadata {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        )
        .with_topic_cache_ttl(ttl)
    }

    /// The bug this whole mechanism exists to fix.
    ///
    /// A partial refresh names one topic, so only that topic gets a fresh
    /// fetch stamp. Evicting on that stamp threw away a topic
    /// that the client was actively producing to, purely because some *other*
    /// topic happened to be the one that needed refreshing — and the next send
    /// to it then failed as `unknown topic` for a topic that plainly exists.
    ///
    /// Eviction is an idleness rule: a topic in use stays.
    #[test]
    fn test_a_topic_in_use_survives_a_partial_refresh_that_does_not_name_it() {
        let meta = ttl_metadata(Duration::from_millis(50));

        meta.update_cache(ok_topics_response(&["topic-a", "topic-b"]), true);
        // Both entries age past the TTL, so refresh recency can no longer save
        // either of them.
        std::thread::sleep(Duration::from_millis(80));

        // ... but the client is still producing to topic-b.
        meta.touch_topic("topic-b");
        meta.update_cache(ok_topics_response(&["topic-a"]), false);

        let cache = meta.inner.cache.load();
        assert!(
            cache.topics.contains_key("topic-a"),
            "topic-a was in the response and must be cached"
        );
        assert!(
            cache.topics.contains_key("topic-b"),
            "topic-b is in active use and must not be evicted by a refresh for topic-a"
        );
    }

    /// The other half of the rule: idleness really does evict, so the cache
    /// stays bounded under topic churn.
    #[test]
    fn test_an_idle_topic_is_evicted_by_a_partial_refresh() {
        let meta = ttl_metadata(Duration::from_millis(50));

        meta.update_cache(ok_topics_response(&["topic-a", "topic-b"]), true);
        meta.touch_topic("topic-b");
        std::thread::sleep(Duration::from_millis(80));

        meta.update_cache(ok_topics_response(&["topic-a"]), false);

        let cache = meta.inner.cache.load();
        assert!(cache.topics.contains_key("topic-a"));
        assert!(
            !cache.topics.contains_key("topic-b"),
            "a topic idle for longer than the TTL must be evicted"
        );
    }

    /// A topic fetched moments ago is never evicted, even if nothing has gone
    /// through a usage-tracking accessor yet. This is the belt to the
    /// idleness braces: no caller can lose an entry inside the window in which
    /// it was fetched.
    #[test]
    fn test_a_freshly_refreshed_topic_is_not_evicted_before_it_is_used() {
        let meta = ttl_metadata(Duration::from_secs(60));

        meta.update_cache(ok_topics_response(&["topic-a", "topic-b"]), true);
        meta.update_cache(ok_topics_response(&["topic-a"]), false);

        assert!(
            meta.inner.cache.load().topics.contains_key("topic-b"),
            "a topic refreshed within the TTL must survive even with no recorded use"
        );
    }

    /// When `topic` was last used, or `None` when never.
    fn last_used(meta: &ClusterMetadata, topic: &str) -> Option<u64> {
        meta.inner
            .cache
            .load()
            .topic_stamps
            .get(topic)
            .map(|stamp| stamp.last_used_ms.load(Ordering::Relaxed))
            .filter(|ms| *ms != 0)
    }

    /// Usage is recorded by the ordinary read accessors, so a caller that only
    /// ever asks for partition counts or leaders keeps its topics warm.
    #[test]
    fn test_read_accessors_record_topic_usage() {
        let meta = ttl_metadata(Duration::from_secs(60));
        meta.update_cache(ok_topics_response(&["topic-a", "topic-b"]), true);

        assert!(
            last_used(&meta, "topic-a").is_none(),
            "a refresh alone is not a use"
        );

        assert_eq!(meta.partition_count("topic-a"), Some(1));
        assert!(last_used(&meta, "topic-a").is_some());

        assert!(meta.leader("topic-b", 0).is_some());
        assert!(last_used(&meta, "topic-b").is_some());
    }

    /// A use survives the topic being fetched again: the stamp's use time is
    /// carried into the new snapshot.
    #[test]
    fn test_a_refetch_keeps_the_last_use() {
        let meta = ttl_metadata(Duration::from_secs(60));
        meta.update_cache(ok_topics_response(&["topic-a"]), true);
        meta.touch_topic("topic-a");
        let used = last_used(&meta, "topic-a");
        assert!(used.is_some());

        meta.update_cache(ok_topics_response(&["topic-a"]), false);
        assert_eq!(last_used(&meta, "topic-a"), used);
    }

    /// With TTL eviction disabled nothing consults the use time, so nothing
    /// records it.
    #[test]
    fn test_usage_is_not_tracked_when_ttl_eviction_is_disabled() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        )
        .with_topic_cache_ttl_disabled();
        meta.update_cache(ok_topics_response(&["topic-a"]), true);

        meta.touch_topic("topic-a");
        assert!(last_used(&meta, "topic-a").is_none());
    }

    /// The expiry map holds exactly the cached topics: touching a topic the
    /// cache does not hold adds nothing, and an evicted topic takes its stamp
    /// with it.
    #[test]
    fn test_topic_stamps_track_exactly_the_cached_topics() {
        let meta = ttl_metadata(Duration::from_millis(10));
        for i in 0..50 {
            meta.touch_topic(&format!("never-fetched-{i}"));
        }
        assert!(meta.inner.cache.load().topic_stamps.is_empty());

        meta.update_cache(ok_topics_response(&["topic-a", "topic-b"]), true);
        std::thread::sleep(Duration::from_millis(30));
        meta.update_cache(ok_topics_response(&["topic-a"]), false);

        let cache = meta.inner.cache.load();
        let mut stamped: Vec<&String> = cache.topic_stamps.keys().collect();
        stamped.sort();
        assert_eq!(stamped, vec!["topic-a"]);
    }

    /// A topic error is remembered with its code, so a caller learns *why* a
    /// topic is unusable instead of a flat "unknown topic" — and forgotten as
    /// soon as the topic comes back healthy.
    #[test]
    fn test_topic_errors_are_recorded_and_cleared() {
        use crate::protocol::{MetadataBroker, MetadataTopicResponse};

        fn errored(topic: &str, code: ErrorCode) -> MetadataResponse {
            MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![MetadataBroker {
                    node_id: 1,
                    host: "localhost".to_string(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: None,
                controller_id: 1,
                error_code: ErrorCode::None,
                topics: vec![MetadataTopicResponse {
                    error_code: code,
                    name: Some(topic.to_string()),
                    topic_id: None,
                    is_internal: false,
                    partitions: vec![],
                }],
            }
        }

        let meta = ttl_metadata(Duration::from_secs(60));

        meta.update_cache(
            errored("secret", ErrorCode::TopicAuthorizationFailed),
            false,
        );
        assert_eq!(
            meta.topic_error("secret"),
            Some(ErrorCode::TopicAuthorizationFailed)
        );

        meta.update_cache(
            errored("missing", ErrorCode::UnknownTopicOrPartition),
            false,
        );
        assert_eq!(
            meta.topic_error("missing"),
            Some(ErrorCode::UnknownTopicOrPartition),
            "a retriable topic error is recorded too: it is the reason a caller is waiting"
        );

        meta.update_cache(ok_topics_response(&["missing"]), false);
        assert_eq!(
            meta.topic_error("missing"),
            None,
            "a topic that comes back healthy has no outstanding error"
        );
    }

    /// A partial refresh must not reset the fetch stamp
    /// for topics that were only retained from the cache (not present in the
    /// response).  Resetting retained timestamps makes them perpetually "fresh"
    /// so TTL eviction never fires.
    #[test]
    fn test_partial_refresh_preserves_retained_topic_timestamps() {
        use crate::protocol::{MetadataBroker, MetadataPartitionResponse, MetadataTopicResponse};

        fn make_response(topic_names: &[&str]) -> MetadataResponse {
            MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![MetadataBroker {
                    node_id: 1,
                    host: "localhost".to_string(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: None,
                controller_id: 1,
                error_code: ErrorCode::None,
                topics: topic_names
                    .iter()
                    .map(|name| MetadataTopicResponse {
                        error_code: ErrorCode::None,
                        name: Some(name.to_string()),
                        topic_id: None,
                        is_internal: false,
                        partitions: vec![MetadataPartitionResponse {
                            error_code: ErrorCode::None,
                            partition_index: 0,
                            leader_id: 1,
                            leader_epoch: 0,
                            replica_nodes: vec![1],
                            isr_nodes: vec![1],
                            offline_replicas: vec![],
                        }],
                    })
                    .collect(),
            }
        }

        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        // Use a long TTL so "topic-a" is not evicted.
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        );

        // First partial update: populate cache with "topic-a".
        meta.update_cache(make_response(&["topic-a"]), false);
        let ts_a = meta
            .inner
            .cache
            .load()
            .topic_stamps
            .get("topic-a")
            .map(|s| s.refreshed)
            .unwrap();

        // Second partial update: only "topic-b" is in the response.
        // "topic-a" is retained from the cache but must keep its original timestamp.
        meta.update_cache(make_response(&["topic-b"]), false);
        let cache = meta.inner.cache.load();

        assert!(
            cache.topics.contains_key("topic-a"),
            "topic-a should still be in the cache (TTL not yet expired)"
        );
        assert!(
            cache.topics.contains_key("topic-b"),
            "topic-b should appear after the second update"
        );

        let ts_a_after = cache
            .topic_stamps
            .get("topic-a")
            .map(|s| s.refreshed)
            .unwrap();
        assert_eq!(
            ts_a, ts_a_after,
            "retained topic-a's timestamp must not be advanced by a partial refresh"
        );
        assert!(
            cache.topic_stamps.contains_key("topic-b"),
            "freshly refreshed topic-b must have a timestamp"
        );
    }

    /// A partial refresh where a topic comes back with a
    /// transient error must reset its TTL timestamp so it is not evicted on
    /// the next refresh, and the stale cache entry must be preserved.
    #[test]
    fn test_transient_error_topic_refreshes_ttl_timestamp() {
        use crate::protocol::{MetadataBroker, MetadataPartitionResponse, MetadataTopicResponse};

        fn make_ok_response(topic_names: &[&str]) -> MetadataResponse {
            MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![MetadataBroker {
                    node_id: 1,
                    host: "localhost".to_string(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: None,
                controller_id: 1,
                error_code: ErrorCode::None,
                topics: topic_names
                    .iter()
                    .map(|name| MetadataTopicResponse {
                        error_code: ErrorCode::None,
                        name: Some(name.to_string()),
                        topic_id: None,
                        is_internal: false,
                        partitions: vec![MetadataPartitionResponse {
                            error_code: ErrorCode::None,
                            partition_index: 0,
                            leader_id: 1,
                            leader_epoch: 0,
                            replica_nodes: vec![1],
                            isr_nodes: vec![1],
                            offline_replicas: vec![],
                        }],
                    })
                    .collect(),
            }
        }

        fn make_transient_error_response(topic_name: &str) -> MetadataResponse {
            MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![MetadataBroker {
                    node_id: 1,
                    host: "localhost".to_string(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: None,
                controller_id: 1,
                error_code: ErrorCode::None,
                topics: vec![MetadataTopicResponse {
                    // LeaderNotAvailable is retriable
                    error_code: ErrorCode::LeaderNotAvailable,
                    name: Some(topic_name.to_string()),
                    topic_id: None,
                    is_internal: false,
                    partitions: vec![],
                }],
            }
        }

        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        );

        // Populate the cache with a successful refresh for "topic-a".
        meta.update_cache(make_ok_response(&["topic-a"]), false);
        let ts_before = meta
            .inner
            .cache
            .load()
            .topic_stamps
            .get("topic-a")
            .map(|s| s.refreshed)
            .unwrap();

        // A subsequent partial refresh returns a transient error for "topic-a".
        // The stale entry must be preserved AND the timestamp must advance.
        meta.update_cache(make_transient_error_response("topic-a"), false);
        let cache = meta.inner.cache.load();

        assert!(
            cache.topics.contains_key("topic-a"),
            "topic-a must be retained when the response has a transient error"
        );
        let ts_after = cache
            .topic_stamps
            .get("topic-a")
            .map(|s| s.refreshed)
            .unwrap();
        assert!(
            ts_after >= ts_before,
            "transient-error response must advance the TTL timestamp so the topic \
             is not evicted on the next refresh"
        );
    }

    /// If a topic has already been TTL-evicted before the
    /// response loop runs, a transient error in the response must restore the
    /// stale entry rather than silently losing it.
    #[test]
    fn test_transient_error_restores_ttl_evicted_topic() {
        use crate::protocol::{MetadataBroker, MetadataPartitionResponse, MetadataTopicResponse};

        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        // 1 ns TTL — any nonzero time between two calls to update_cache
        // will exceed it, so the eviction pass is guaranteed to remove
        // the seeded entry before the response loop runs.
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        )
        .with_topic_cache_ttl(Duration::from_nanos(1));

        // Seed the cache with "topic-a".
        meta.update_cache(
            MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![MetadataBroker {
                    node_id: 1,
                    host: "localhost".to_string(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: None,
                controller_id: 1,
                error_code: ErrorCode::None,
                topics: vec![MetadataTopicResponse {
                    error_code: ErrorCode::None,
                    name: Some("topic-a".to_string()),
                    topic_id: None,
                    is_internal: false,
                    partitions: vec![MetadataPartitionResponse {
                        error_code: ErrorCode::None,
                        partition_index: 0,
                        leader_id: 1,
                        leader_epoch: 0,
                        replica_nodes: vec![1],
                        isr_nodes: vec![1],
                        offline_replicas: vec![],
                    }],
                }],
            },
            false,
        );
        assert!(
            meta.inner.cache.load().topics.contains_key("topic-a"),
            "pre-condition: topic-a seeded"
        );

        // Sleep long enough that Instant::elapsed() strictly exceeds the 1 ns TTL
        // on every platform, including those with coarse clock resolution
        // (e.g. Windows default timer granularity is ~15 ms).
        std::thread::sleep(Duration::from_millis(20));

        // Partial refresh with a transient error for "topic-a".
        meta.update_cache(
            MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![MetadataBroker {
                    node_id: 1,
                    host: "localhost".to_string(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: None,
                controller_id: 1,
                error_code: ErrorCode::None,
                topics: vec![MetadataTopicResponse {
                    error_code: ErrorCode::LeaderNotAvailable,
                    name: Some("topic-a".to_string()),
                    topic_id: None,
                    is_internal: false,
                    partitions: vec![],
                }],
            },
            false,
        );

        assert!(
            meta.inner.cache.load().topics.contains_key("topic-a"),
            "topic-a must be restored from old cache after TTL eviction + transient error"
        );
    }

    /// A brand-new topic that appears in a partial refresh
    /// only with a transient error (and has no prior cache entry) must NOT
    /// create an orphaned stamp with no corresponding
    /// key in `topics`.
    #[test]
    fn test_transient_error_never_cached_topic_not_stamped() {
        use crate::protocol::{MetadataBroker, MetadataTopicResponse};

        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        );

        // Empty cache — "unknown-topic" has never been seen before.
        // A partial refresh returns a retriable error for it.
        meta.update_cache(
            MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![MetadataBroker {
                    node_id: 1,
                    host: "localhost".to_string(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: None,
                controller_id: 1,
                error_code: ErrorCode::None,
                topics: vec![MetadataTopicResponse {
                    error_code: ErrorCode::LeaderNotAvailable,
                    name: Some("unknown-topic".to_string()),
                    topic_id: None,
                    is_internal: false,
                    partitions: vec![],
                }],
            },
            false,
        );

        let cache = meta.inner.cache.load();
        assert!(
            !cache.topics.contains_key("unknown-topic"),
            "unknown-topic must not appear in topics when only a transient error was received \
             and there is no prior cache entry"
        );
        assert!(
            !cache.topic_stamps.contains_key("unknown-topic"),
            "unknown-topic must not be stamped when it is not in topics"
        );
    }

    /// When a TTL-evicted topic is restored via the transient-error path, its
    /// UUID mapping must also be restored so that `topic_id_for_name()`
    /// continues to return `Some(uuid)`; share-consumer fetch routing
    /// depends on it.
    #[test]
    fn test_transient_error_restores_uuid_mapping_for_evicted_topic() {
        use crate::protocol::{MetadataBroker, MetadataPartitionResponse, MetadataTopicResponse};

        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        )
        .with_topic_cache_ttl(Duration::from_nanos(1));

        // The UUID used for "topic-b" in the seed response.
        let uuid: [u8; 16] = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
            0x0f, 0x10,
        ];

        // Seed the cache with "topic-b" carrying a topic UUID.
        meta.update_cache(
            MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![MetadataBroker {
                    node_id: 1,
                    host: "localhost".to_string(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: None,
                controller_id: 1,
                error_code: ErrorCode::None,
                topics: vec![MetadataTopicResponse {
                    error_code: ErrorCode::None,
                    name: Some("topic-b".to_string()),
                    topic_id: Some(uuid),
                    is_internal: false,
                    partitions: vec![MetadataPartitionResponse {
                        error_code: ErrorCode::None,
                        partition_index: 0,
                        leader_id: 1,
                        leader_epoch: 0,
                        replica_nodes: vec![1],
                        isr_nodes: vec![1],
                        offline_replicas: vec![],
                    }],
                }],
            },
            false,
        );
        assert!(
            meta.inner
                .cache
                .load()
                .name_to_topic_id
                .contains_key("topic-b"),
            "pre-condition: UUID mapping seeded"
        );

        // Sleep long enough that Instant::elapsed() strictly exceeds the 1 ns TTL
        // on every platform, including those with coarse clock resolution
        // (e.g. Windows default timer granularity is ~15 ms).
        std::thread::sleep(Duration::from_millis(20));

        // Partial refresh — 1 ns TTL guarantees eviction of "topic-b" before
        // the response loop.  Transient error must restore both the topic entry
        // and its UUID mapping.
        meta.update_cache(
            MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![MetadataBroker {
                    node_id: 1,
                    host: "localhost".to_string(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: None,
                controller_id: 1,
                error_code: ErrorCode::None,
                topics: vec![MetadataTopicResponse {
                    error_code: ErrorCode::LeaderNotAvailable,
                    name: Some("topic-b".to_string()),
                    topic_id: Some(uuid),
                    is_internal: false,
                    partitions: vec![],
                }],
            },
            false,
        );

        let cache = meta.inner.cache.load();
        assert!(
            cache.topics.contains_key("topic-b"),
            "topic-b must be restored in topics"
        );
        assert_eq!(
            cache.name_to_topic_id.get("topic-b"),
            Some(&uuid),
            "UUID mapping for topic-b must be restored in name_to_topic_id"
        );
        assert!(
            cache.topic_ids.contains_key(&uuid),
            "UUID must be present in topic_ids"
        );
    }

    // ══════════════════════════════════════════════════════════════════
    // Errored partitions must be retained, not silently dropped
    // ══════════════════════════════════════════════════════════════════

    fn test_metadata() -> ClusterMetadata {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        )
    }

    fn partition_response(
        index: PartitionId,
        leader: BrokerId,
        epoch: i32,
        error: ErrorCode,
    ) -> crate::protocol::MetadataPartitionResponse {
        crate::protocol::MetadataPartitionResponse {
            error_code: error,
            partition_index: index,
            leader_id: leader,
            leader_epoch: epoch,
            replica_nodes: vec![leader],
            isr_nodes: vec![leader],
            offline_replicas: vec![],
        }
    }

    fn metadata_response(
        partitions: Vec<crate::protocol::MetadataPartitionResponse>,
    ) -> MetadataResponse {
        MetadataResponse {
            error_code: ErrorCode::None,
            throttle_time_ms: 0,
            brokers: vec![crate::protocol::MetadataBroker {
                node_id: 1,
                host: "h".into(),
                port: 9092,
                rack: None,
            }],
            cluster_id: Some("c".into()),
            controller_id: 1,
            topics: vec![crate::protocol::MetadataTopicResponse {
                error_code: ErrorCode::None,
                name: Some("t".into()),
                topic_id: None,
                is_internal: false,
                partitions,
            }],
        }
    }

    /// During a rolling restart some partitions report LEADER_NOT_AVAILABLE.
    /// Dropping them shrinks `partition_count()`, so a key-hash partitioner
    /// computing `hash % partition_count` silently re-maps every key and
    /// violates per-key ordering for the duration of the outage.
    #[test]
    fn test_errored_partitions_are_retained_so_partition_count_is_stable() {
        let meta = test_metadata();

        // 12 partitions, 3 of which are unavailable.
        let partitions = (0..12)
            .map(|i| {
                if (9..12).contains(&i) {
                    partition_response(i, -1, -1, ErrorCode::LeaderNotAvailable)
                } else {
                    partition_response(i, 1, 5, ErrorCode::None)
                }
            })
            .collect();

        meta.update_cache(metadata_response(partitions), true);

        assert_eq!(
            meta.partition_count("t"),
            Some(12),
            "partition_count must reflect the full topic, not just healthy partitions"
        );

        let topic = meta.topic_arc("t").unwrap();
        for i in 9..12 {
            let p = topic.partition(i).expect("errored partition must be kept");
            assert_eq!(p.error_code, ErrorCode::LeaderNotAvailable);
            assert_eq!(
                p.leader, -1,
                "an errored partition has no trustworthy leader"
            );
            assert!(!p.is_routable());
            assert_eq!(
                topic.leader(i),
                None,
                "routing must fail for this partition rather than dial broker -1"
            );
        }

        // Healthy partitions still route normally.
        assert_eq!(topic.leader(0), Some(1));
        assert!(topic.partition(0).unwrap().is_routable());
    }

    // ══════════════════════════════════════════════════════════════════
    // KIP-320 leader-epoch fencing on cache merge
    // ══════════════════════════════════════════════════════════════════

    /// A lagging broker answering with epoch 41 while the cache holds 42 must
    /// not revert the client to the old leader — that is exactly the silent
    /// wrong-leader window KIP-320 exists to close.
    #[test]
    fn test_stale_leader_epoch_is_ignored() {
        let meta = test_metadata();

        // Cache holds leader 2 at epoch 42.
        meta.update_cache(
            metadata_response(vec![partition_response(0, 2, 42, ErrorCode::None)]),
            true,
        );
        assert_eq!(meta.leader("t", 0), Some(2));
        assert_eq!(meta.leader_epoch("t", 0), Some(42));

        // A lagging broker reports the *previous* leader at epoch 41.
        meta.update_cache(
            metadata_response(vec![partition_response(0, 1, 41, ErrorCode::None)]),
            false,
        );

        assert_eq!(
            meta.leader("t", 0),
            Some(2),
            "a lower leader epoch must not revert the cached leader"
        );
        assert_eq!(meta.leader_epoch("t", 0), Some(42));
    }

    #[test]
    fn test_newer_leader_epoch_is_applied() {
        let meta = test_metadata();

        meta.update_cache(
            metadata_response(vec![partition_response(0, 1, 41, ErrorCode::None)]),
            true,
        );
        meta.update_cache(
            metadata_response(vec![partition_response(0, 2, 42, ErrorCode::None)]),
            false,
        );

        assert_eq!(meta.leader("t", 0), Some(2));
        assert_eq!(meta.leader_epoch("t", 0), Some(42));
    }

    #[test]
    fn test_equal_leader_epoch_is_applied() {
        let meta = test_metadata();

        meta.update_cache(
            metadata_response(vec![partition_response(0, 1, 7, ErrorCode::None)]),
            true,
        );
        // Same epoch, different leader: accept (Java accepts newEpoch >= cached).
        meta.update_cache(
            metadata_response(vec![partition_response(0, 3, 7, ErrorCode::None)]),
            false,
        );

        assert_eq!(meta.leader("t", 0), Some(3));
    }

    /// Epoch -1 means "unknown" (Metadata < v7) and must never participate in
    /// the comparison, otherwise old brokers could never update the cache.
    #[test]
    fn test_unknown_epoch_does_not_block_updates() {
        let meta = test_metadata();

        meta.update_cache(
            metadata_response(vec![partition_response(0, 1, 5, ErrorCode::None)]),
            true,
        );
        meta.update_cache(
            metadata_response(vec![partition_response(0, 4, -1, ErrorCode::None)]),
            false,
        );

        assert_eq!(meta.leader("t", 0), Some(4));
        assert_eq!(
            meta.leader_epoch("t", 0),
            None,
            "an unknown epoch reads as None, not -1"
        );
    }

    // ══════════════════════════════════════════════════════════════════
    // Controller resolution
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn test_controller_is_none_when_unelected() {
        let meta = test_metadata();
        // Fresh cache has controller_id = -1.
        assert!(
            meta.controller().is_none(),
            "controller_id -1 means no controller is elected"
        );
    }

    #[test]
    fn test_controller_is_none_when_id_not_in_broker_set() {
        let meta = test_metadata();
        let mut cache = MetadataCache::new();
        cache.controller_id = 7;
        cache
            .brokers
            .insert(1, BrokerInfo::new(1, "h".into(), 9092, None));
        meta.inner.cache.store(Arc::new(cache));

        assert!(meta.controller().is_none());
    }

    #[test]
    fn test_controller_resolves_from_metadata() {
        let meta = test_metadata();
        meta.update_cache(
            metadata_response(vec![partition_response(0, 1, 0, ErrorCode::None)]),
            true,
        );

        let controller = meta.controller().expect("controller should resolve");
        assert_eq!(controller.id(), 1);
        assert_eq!(controller.address(), "h:9092");
    }

    // ══════════════════════════════════════════════════════════════════
    // Zero-copy accessors
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn test_topic_arc_shares_the_cached_allocation() {
        let meta = test_metadata();
        meta.update_cache(
            metadata_response(vec![partition_response(0, 1, 0, ErrorCode::None)]),
            true,
        );

        let a = meta.topic_arc("t").unwrap();
        let b = meta.topic_arc("t").unwrap();
        assert!(Arc::ptr_eq(&a, &b), "topic_arc must not deep-copy");

        assert_eq!(meta.topics_arc().len(), 1);
        assert!(meta.topic_arc("missing").is_none());

        // The cloning accessor still works and agrees.
        assert_eq!(meta.topic("t").unwrap().name, a.name);
    }

    // ══════════════════════════════════════════════════════════════════
    // Exponential metadata retry backoff with jitter (KIP-580)
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn test_default_retry_backoff_is_exponential_and_capped() {
        let meta = test_metadata();
        let policy = meta
            .inner
            .retry_backoff
            .as_ref()
            .expect("enabled by default");

        assert_eq!(policy.initial_backoff, DEFAULT_RETRY_BACKOFF);
        assert_eq!(policy.max_backoff, DEFAULT_RETRY_BACKOFF_MAX);
        assert!(
            policy.backoff_multiplier > 1.0,
            "a flat curve would keep the retry rate constant while the cluster is down"
        );
        assert!(policy.jitter_factor() > 0.0, "retries must be scattered");
    }

    /// The delay must actually grow with consecutive failures, stay inside the
    /// jitter envelope, and stop growing at the ceiling.
    #[test]
    fn test_refresh_backoff_grows_with_consecutive_failures() {
        let meta = test_metadata();
        let policy = meta.inner.retry_backoff.clone().unwrap();
        let mut state = RefreshBackoffState::new();

        // Base 100 ms, ×2 per failure, ±20% jitter, ceiling 1000 ms.
        let expected_bases_ms = [100u64, 200, 400, 800, 1000, 1000];
        let mut previous_base = 0u64;

        for (failure, base_ms) in expected_bases_ms.iter().copied().enumerate() {
            state.record_failure(&policy);
            assert_eq!(state.consecutive_failures as usize, failure + 1);

            let low = Duration::from_millis((base_ms as f64 * 0.8) as u64);
            let high = Duration::from_millis((base_ms as f64 * 1.2).ceil() as u64);
            assert!(
                state.current_delay >= low && state.current_delay <= high,
                "failure {}: delay {:?} outside jitter envelope [{low:?}, {high:?}]",
                failure + 1,
                state.current_delay,
            );

            // Growth is monotonic in the base even though jitter perturbs each
            // individual sample.
            assert!(base_ms >= previous_base);
            previous_base = base_ms;
        }

        assert!(
            state.current_delay <= Duration::from_millis(1200),
            "the delay must stop growing at retry.backoff.max.ms (plus jitter)"
        );
    }

    #[test]
    fn test_refresh_backoff_is_jittered_across_clients() {
        let meta = test_metadata();
        let policy = meta.inner.retry_backoff.clone().unwrap();

        // Simulate many clients that have all failed four times in a row. If
        // the delays were identical they would retry in lockstep — the storm
        // KIP-580 exists to break up.
        let mut delays = std::collections::HashSet::new();
        for _ in 0..64 {
            let mut state = RefreshBackoffState::new();
            for _ in 0..4 {
                state.record_failure(&policy);
            }
            delays.insert(state.current_delay.as_nanos());
        }
        assert!(
            delays.len() > 1,
            "all clients computed the same backoff; jitter is not being applied"
        );
    }

    /// A successful refresh must drop the client back to the base delay;
    /// otherwise one bad minute leaves it retrying at the ceiling forever.
    #[test]
    fn test_refresh_backoff_resets_on_success() {
        let meta = test_metadata();
        let policy = meta.inner.retry_backoff.clone().unwrap();
        let mut state = RefreshBackoffState::new();

        for _ in 0..8 {
            state.record_failure(&policy);
        }
        assert_eq!(state.consecutive_failures, 8);
        assert!(state.current_delay >= Duration::from_millis(800));

        state.record_success(&policy);
        assert_eq!(state.consecutive_failures, 0);
        assert!(
            state.current_delay <= Duration::from_millis(120),
            "after a success the delay must be back at the base, got {:?}",
            state.current_delay
        );
    }

    #[test]
    fn test_refresh_backoff_remaining_is_none_before_first_attempt() {
        let state = RefreshBackoffState::new();
        assert_eq!(
            state.remaining(),
            None,
            "the very first refresh must never be rate-limited"
        );
    }

    #[test]
    fn test_refresh_backoff_remaining_expires() {
        let policy = BackoffPolicy {
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(20),
            backoff_multiplier: 2.0,
            jitter_factor: 0.0,
        };
        let mut state = RefreshBackoffState::new();
        state.record_failure(&policy);
        assert!(state.remaining().is_some());

        std::thread::sleep(Duration::from_millis(40));
        assert_eq!(
            state.remaining(),
            None,
            "once the delay has elapsed another attempt must be permitted"
        );
    }

    #[test]
    fn test_with_retry_backoff_sets_base_and_raises_max() {
        let meta = test_metadata().with_retry_backoff(Duration::from_millis(250));
        let policy = meta.inner.retry_backoff.as_ref().unwrap();
        assert_eq!(policy.initial_backoff, Duration::from_millis(250));
        assert_eq!(
            policy.max_backoff, DEFAULT_RETRY_BACKOFF_MAX,
            "a base below the default ceiling leaves the ceiling alone"
        );

        // A base above the ceiling raises the ceiling rather than inverting it.
        let meta = test_metadata().with_retry_backoff(Duration::from_secs(5));
        let policy = meta.inner.retry_backoff.as_ref().unwrap();
        assert_eq!(policy.initial_backoff, Duration::from_secs(5));
        assert_eq!(policy.max_backoff, Duration::from_secs(5));
    }

    #[test]
    fn test_with_retry_backoff_max_never_inverts_the_curve() {
        let meta = test_metadata()
            .with_retry_backoff(Duration::from_millis(500))
            .with_retry_backoff_max(Duration::from_millis(10));
        let policy = meta.inner.retry_backoff.as_ref().unwrap();
        assert_eq!(policy.max_backoff, Duration::from_millis(500));
    }

    #[test]
    fn test_with_retry_backoff_none_disables_rate_limiting() {
        let meta = test_metadata().with_retry_backoff(None);
        assert!(meta.inner.retry_backoff.is_none());
        // A max on a disabled limiter is a no-op rather than a re-enable.
        let meta = meta.with_retry_backoff_max(Duration::from_secs(1));
        assert!(meta.inner.retry_backoff.is_none());
    }

    /// A refresh that never reaches a broker must still arm the rate limiter.
    /// Leaving the connect-failure path unrecorded is what let a fully
    /// unreachable cluster be hammered without any backoff at all.
    #[tokio::test]
    async fn test_failed_refresh_arms_the_backoff() {
        let meta = ClusterMetadata::new(
            // Port 1 is not listening, so the connect attempt fails fast.
            vec!["127.0.0.1:1".to_string()],
            Arc::new(ConnectionPool::new(
                crate::network::ConnectionConfig::default(),
            )),
            Duration::from_secs(300),
        );

        assert!(meta.refresh_for_topics(Some(&["t"])).await.is_err());

        {
            let state = meta.inner.refresh_backoff.lock();
            assert_eq!(
                state.consecutive_failures, 1,
                "a connection failure is a refresh failure and must count"
            );
            assert!(state.remaining().is_some(), "the limiter must now be armed");
        }

        // The next request waits out the backoff instead of re-dialling the
        // dead broker at once.
        let started = Instant::now();
        assert!(meta.refresh_for_topics(Some(&["t"])).await.is_err());
        assert!(
            started.elapsed() >= Duration::from_millis(50),
            "the second fetch went out after {:?}, inside the backoff",
            started.elapsed()
        );
        assert_eq!(meta.inner.refresh_backoff.lock().consecutive_failures, 2);
    }

    /// Consecutive real failures must escalate the delay, not hold it flat.
    #[tokio::test]
    async fn test_consecutive_refresh_failures_escalate_the_delay() {
        let meta = ClusterMetadata::new(
            vec!["127.0.0.1:1".to_string()],
            Arc::new(ConnectionPool::new(
                crate::network::ConnectionConfig::default(),
            )),
            Duration::from_secs(300),
        )
        // Sub-millisecond base so the test does not have to sleep for long.
        .with_retry_backoff(Duration::from_micros(200))
        .with_retry_backoff_max(Duration::from_millis(50));

        let mut delays = Vec::new();
        for _ in 0..4 {
            assert!(meta.refresh().await.is_err());
            delays.push(meta.inner.refresh_backoff.lock().current_delay);
        }

        assert_eq!(meta.inner.refresh_backoff.lock().consecutive_failures, 4);
        assert!(
            delays[3] > delays[0],
            "backoff must grow across consecutive failures: {delays:?}"
        );
    }

    // ══════════════════════════════════════════════════════════════════
    // Bounded, jittered rebootstrap (KIP-899 / KIP-1102)
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn test_rebootstrap_jitter_default_and_override() {
        let meta = test_metadata();
        assert_eq!(
            meta.inner.rebootstrap_jitter,
            Duration::from_millis(500),
            "a restarted fleet must not converge on one seed broker"
        );

        let meta = test_metadata().with_rebootstrap_jitter(Duration::ZERO);
        assert_eq!(meta.inner.rebootstrap_jitter, Duration::ZERO);
    }

    /// A rebootstrap must not be able to fire again immediately: it restarts
    /// the failure timer, so a second one needs another full trigger period
    /// even when the whole cluster stays down.
    #[tokio::test]
    async fn test_rebootstrap_cannot_fire_in_a_tight_loop() {
        let meta = test_metadata()
            .with_recovery_strategy(MetadataRecoveryStrategy::Rebootstrap)
            .with_rebootstrap_trigger(Duration::from_secs(300))
            .with_rebootstrap_jitter(Duration::ZERO);

        // A long-running failure streak crosses the trigger.
        *meta.inner.metadata_attempt_start.lock() = Some(Instant::now() - Duration::from_secs(600));
        assert!(meta.inner.needs_rebootstrap());

        meta.rebootstrap().await;

        // Immediately afterwards the cluster is still down — but the trigger
        // must not be satisfied again until another 300 s of failure.
        assert!(
            !meta.inner.needs_rebootstrap(),
            "back-to-back rebootstraps would turn a cluster outage into a \
             connection-churn storm against the seed brokers"
        );
        assert!(meta.inner.metadata_attempt_start.lock().is_some());
    }

    /// The jittered deadline must never fire *before* the configured trigger.
    #[test]
    fn test_rebootstrap_trigger_jitter_only_delays() {
        let meta = test_metadata()
            .with_recovery_strategy(MetadataRecoveryStrategy::Rebootstrap)
            .with_rebootstrap_trigger(Duration::from_secs(10));

        // Just under the trigger: must never fire, however the jitter lands.
        *meta.inner.metadata_attempt_start.lock() = Some(Instant::now() - Duration::from_secs(9));
        for _ in 0..64 {
            assert!(!meta.inner.needs_rebootstrap());
        }

        // Comfortably past trigger + max jitter (10 s + 20%): always fires.
        *meta.inner.metadata_attempt_start.lock() = Some(Instant::now() - Duration::from_secs(30));
        for _ in 0..64 {
            assert!(meta.inner.needs_rebootstrap());
        }
    }

    /// After a rebootstrap the only connection candidates are the seed
    /// hostnames — the stale broker addresses are gone. Because candidates are
    /// `host:port` strings resolved at dial time, this is what makes the client
    /// pick up new broker IPs behind a load balancer instead of retrying
    /// addresses that no longer answer.
    #[tokio::test]
    async fn test_rebootstrap_reresolves_seed_brokers() {
        let meta = ClusterMetadata::new(
            vec!["seed.example.com:9092".to_string()],
            Arc::new(ConnectionPool::new(
                crate::network::ConnectionConfig::default(),
            )),
            Duration::from_secs(300),
        )
        .with_rebootstrap_jitter(Duration::ZERO);

        // Cache a broker set pointing at addresses that will go away.
        let mut cache = MetadataCache::new();
        cache
            .brokers
            .insert(1, BrokerInfo::new(1, "old-broker-1".into(), 9092, None));
        cache
            .brokers
            .insert(2, BrokerInfo::new(2, "old-broker-2".into(), 9092, None));
        meta.inner.cache.store(Arc::new(cache));

        let before = meta.inner.connection_candidates();
        assert!(before.iter().any(|a| a == "old-broker-1:9092"));
        assert!(before.iter().any(|a| a == "seed.example.com:9092"));

        meta.rebootstrap().await;

        let after = meta.inner.connection_candidates();
        assert_eq!(
            after,
            vec!["seed.example.com:9092".to_string()],
            "after a rebootstrap only the seed hostnames remain, so the next \
             dial resolves them afresh"
        );
    }

    /// Seed brokers replaced at runtime must be picked up by the next dial —
    /// the candidate list is rebuilt from the current seed list, never from a
    /// snapshot taken at construction.
    #[test]
    fn test_updated_seed_brokers_appear_in_connection_candidates() {
        let meta = test_metadata();
        assert!(
            meta.inner
                .connection_candidates()
                .contains(&"localhost:9092".to_string())
        );

        meta.update_seed_brokers(vec!["new-seed:9092".to_string()])
            .unwrap();
        assert_eq!(meta.inner.connection_candidates(), vec!["new-seed:9092"]);
    }

    #[test]
    fn test_connection_candidates_deduplicates_seeds_already_known_as_brokers() {
        let meta = test_metadata();
        let mut cache = MetadataCache::new();
        // Broker 1 advertises exactly the seed address.
        cache
            .brokers
            .insert(1, BrokerInfo::new(1, "localhost".into(), 9092, None));
        meta.inner.cache.store(Arc::new(cache));

        assert_eq!(
            meta.inner.connection_candidates(),
            vec!["localhost:9092".to_string()],
            "a seed that is also a known broker must not be dialled twice"
        );
    }

    // ══════════════════════════════════════════════════════════════════
    // Per-topic cache staleness
    // ══════════════════════════════════════════════════════════════════

    /// `last_updated` advances on every partial refresh, so it cannot be used
    /// to judge whether one particular topic is still current. A client that
    /// keeps refreshing topic A must not thereby keep an arbitrarily old entry
    /// for topic B looking fresh.
    #[test]
    fn test_topic_freshness_is_per_topic_not_cache_wide() {
        let meta = test_metadata();
        let max_age = Duration::from_secs(60);

        let mut cache = MetadataCache::new();
        cache.topics.insert(
            "stale".into(),
            Arc::new(TopicInfo {
                name: "stale".into(),
                topic_id: [0; 16],
                is_internal: false,
                partitions: std::collections::HashMap::new(),
            }),
        );
        cache.topics.insert(
            "fresh".into(),
            Arc::new(TopicInfo {
                name: "fresh".into(),
                topic_id: [0; 16],
                is_internal: false,
                partitions: std::collections::HashMap::new(),
            }),
        );
        cache.topic_stamps.insert(
            "stale".into(),
            Arc::new(TopicStamp {
                refreshed: Instant::now() - Duration::from_secs(600),
                last_used_ms: AtomicU64::new(0),
            }),
        );
        cache
            .topic_stamps
            .insert("fresh".into(), TopicStamp::fetched_now(0));
        // The cache as a whole was just written by the "fresh" refresh.
        cache.last_updated = Instant::now();
        meta.inner.cache.store(Arc::new(cache));

        let cache = meta.inner.cache.load();
        assert!(
            !cache.is_stale(max_age),
            "pre-condition: the cache as a whole looks current"
        );
        assert!(cache.topic_is_fresh("fresh", max_age));
        assert!(
            !cache.topic_is_fresh("stale", max_age),
            "a topic not refreshed within max_age is stale even though the \
             cache-wide timestamp is recent"
        );
        assert!(
            !cache.topic_is_fresh("never-seen", max_age),
            "an unknown topic is never fresh"
        );
    }

    #[test]
    fn test_topic_without_timestamp_is_not_fresh() {
        // A topic present in `topics` but with no stamp
        // has unknown age and must be treated as stale rather than trusted.
        let mut cache = MetadataCache::new();
        cache.topics.insert(
            "t".into(),
            Arc::new(TopicInfo {
                name: "t".into(),
                topic_id: [0; 16],
                is_internal: false,
                partitions: std::collections::HashMap::new(),
            }),
        );
        assert!(!cache.topic_is_fresh("t", Duration::from_secs(60)));
    }

    // ══════════════════════════════════════════════════════════════════
    // KIP-951: leaders reported in Fetch/Produce responses
    // ══════════════════════════════════════════════════════════════════

    /// A cache holding `t-0` led by broker 1 at the given epoch, with both
    /// brokers 1 and 2 already known.
    fn metadata_with_leader(epoch: i32) -> ClusterMetadata {
        let meta = test_metadata();
        meta.update_cache(
            metadata_response(vec![partition_response(0, 1, epoch, ErrorCode::None)]),
            true,
        );
        // `metadata_response` only advertises broker 1; add 2 so hints that
        // omit an endpoint still have a reachable target.
        let mut cache = MetadataCache::clone(&meta.inner.cache.load());
        cache
            .brokers
            .insert(2, BrokerInfo::new(2, "h2".into(), 9092, None));
        meta.inner.cache.store(Arc::new(cache));
        meta
    }

    fn endpoint(id: BrokerId) -> Option<BrokerInfo> {
        Some(BrokerInfo::new(id, format!("h{id}"), 9092, None))
    }

    #[test]
    fn test_leader_hint_with_a_newer_epoch_is_applied() {
        let meta = metadata_with_leader(5);

        assert!(meta.apply_leader_hint("t", 0, 2, 6, endpoint(2)));

        assert_eq!(meta.leader("t", 0), Some(2));
        assert_eq!(meta.leader_epoch("t", 0), Some(6));
    }

    #[test]
    fn test_leader_hint_with_an_older_epoch_is_ignored() {
        // A lagging broker must not be able to drag the cache back to the
        // previous leader — the same KIP-320 rule the merge path applies.
        let meta = metadata_with_leader(5);

        assert!(!meta.apply_leader_hint("t", 0, 2, 4, None));

        assert_eq!(meta.leader("t", 0), Some(1));
        assert_eq!(meta.leader_epoch("t", 0), Some(5));
    }

    #[test]
    fn test_leader_hint_with_an_equal_epoch_is_ignored() {
        // Kafka bumps the epoch on every leader change, so an equal epoch
        // carries no new information and cannot name a different leader.
        let meta = metadata_with_leader(5);

        assert!(!meta.apply_leader_hint("t", 0, 2, 5, None));

        assert_eq!(meta.leader("t", 0), Some(1));
    }

    #[test]
    fn test_leader_hint_supersedes_an_unknown_cached_epoch() {
        // `-1` means the epoch was never learned (Metadata < v7, or an error
        // state); anything the broker reports is better than that.
        let meta = test_metadata();
        meta.update_cache(
            metadata_response(vec![partition_response(
                0,
                -1,
                -1,
                ErrorCode::LeaderNotAvailable,
            )]),
            true,
        );

        assert!(meta.apply_leader_hint("t", 0, 2, 0, endpoint(2)));

        assert_eq!(meta.leader("t", 0), Some(2));
    }

    #[test]
    fn test_leader_hint_clears_a_stale_partition_error() {
        // The partition was left unroutable by a `LEADER_NOT_AVAILABLE`; the
        // hint names a live leader, so it must become routable again rather
        // than stay stranded until the next refresh.
        let meta = test_metadata();
        meta.update_cache(
            metadata_response(vec![partition_response(
                0,
                -1,
                -1,
                ErrorCode::LeaderNotAvailable,
            )]),
            true,
        );
        assert!(!meta.topic("t").unwrap().partition(0).unwrap().is_routable());

        assert!(meta.apply_leader_hint("t", 0, 2, 3, endpoint(2)));

        let topic = meta.topic("t").unwrap();
        let p = topic.partition(0).unwrap();
        assert!(p.is_routable());
        assert_eq!(p.error_code, ErrorCode::None);
    }

    #[test]
    fn test_leader_hint_registers_an_unknown_broker_endpoint() {
        let meta = metadata_with_leader(5);
        assert!(meta.broker(7).is_none());

        assert!(meta.apply_leader_hint("t", 0, 7, 6, endpoint(7)));

        assert_eq!(meta.broker(7).unwrap().address(), "h7:9092");
        assert_eq!(meta.leader("t", 0), Some(7));
    }

    #[test]
    fn test_leader_hint_for_an_unreachable_broker_is_dropped() {
        // Naming a leader the client has no address for would turn a retriable
        // error into a routing failure, so the hint is refused outright.
        let meta = metadata_with_leader(5);

        assert!(!meta.apply_leader_hint("t", 0, 99, 6, None));

        assert_eq!(meta.leader("t", 0), Some(1));
        assert!(meta.broker(99).is_none());
    }

    #[test]
    fn test_leader_hint_registers_an_endpoint_even_when_the_epoch_is_stale() {
        // The address is useful on its own: the same node may lead another
        // partition whose hint does arrive with a newer epoch.
        let meta = metadata_with_leader(5);

        assert!(meta.apply_leader_hint("t", 0, 8, 1, endpoint(8)));

        assert_eq!(meta.broker(8).unwrap().address(), "h8:9092");
        assert_eq!(
            meta.leader("t", 0),
            Some(1),
            "the stale epoch was not applied"
        );
    }

    #[test]
    fn test_leader_hint_ignores_a_negative_leader_id() {
        let meta = metadata_with_leader(5);
        assert!(!meta.apply_leader_hint("t", 0, -1, 99, None));
        assert_eq!(meta.leader("t", 0), Some(1));
    }

    #[test]
    fn test_leader_hint_does_not_invent_unknown_topics_or_partitions() {
        // Creating a topic entry from a one-partition report would make
        // `partition_count()` wrong, and a key-hash partitioner would then
        // route every key to partition 0.
        let meta = metadata_with_leader(5);

        assert!(!meta.apply_leader_hint("other", 0, 2, 9, None));
        assert!(!meta.apply_leader_hint("t", 7, 2, 9, None));

        assert!(meta.topic("other").is_none());
        assert_eq!(meta.topic("t").unwrap().partition_count(), 1);
    }

    #[test]
    fn test_leader_hint_does_not_mark_the_topic_as_freshly_refreshed() {
        // The report covers one partition; treating it as a refresh would let
        // the rest of the topic's leader map go stale unnoticed.
        let meta = metadata_with_leader(5);
        let before = meta.inner.cache.load().topic_stamps["t"].refreshed;

        assert!(meta.apply_leader_hint("t", 0, 2, 6, endpoint(2)));

        assert_eq!(meta.inner.cache.load().topic_stamps["t"].refreshed, before);
    }

    #[test]
    fn test_leader_hint_updates_an_existing_broker_address() {
        // A restarted broker can come back on a different address; the
        // endpoint the cluster is advertising now wins.
        let meta = metadata_with_leader(5);
        assert_eq!(meta.broker(2).unwrap().address(), "h2:9092");

        assert!(meta.apply_leader_hint(
            "t",
            0,
            2,
            6,
            Some(BrokerInfo::new(2, "moved".into(), 9093, None))
        ));

        assert_eq!(meta.broker(2).unwrap().address(), "moved:9093");
    }

    #[test]
    fn test_broker_info_for_node_matches_by_node_id() {
        let endpoints = vec![
            crate::protocol::NodeEndpoint {
                node_id: 4,
                host: "a".into(),
                port: 1,
                rack: None,
            },
            crate::protocol::NodeEndpoint {
                node_id: 5,
                host: "b".into(),
                port: 2,
                rack: Some("r".into()),
            },
        ];

        let found = broker_info_for_node(&endpoints, 5).unwrap();
        assert_eq!(found.address(), "b:2");
        assert_eq!(found.rack(), Some("r"));
        assert!(broker_info_for_node(&endpoints, 6).is_none());
    }

    // ══════════════════════════════════════════════════════════════════
    // One writer: topic IDs, hints and rebootstraps on the write path
    // ══════════════════════════════════════════════════════════════════

    fn response_with_id(
        id: [u8; 16],
        partitions: Vec<crate::protocol::MetadataPartitionResponse>,
    ) -> MetadataResponse {
        let mut response = metadata_response(partitions);
        response.topics[0].topic_id = Some(id);
        response
    }

    /// A re-created topic (new ID) restarts its epochs: the incoming
    /// partitions replace the cached ones even with a lower epoch.
    #[test]
    fn test_a_changed_topic_id_resets_the_leader_epochs() {
        let meta = test_metadata();
        meta.update_cache(
            response_with_id([1; 16], vec![partition_response(0, 1, 5, ErrorCode::None)]),
            true,
        );
        meta.update_cache(
            response_with_id([2; 16], vec![partition_response(0, 2, 0, ErrorCode::None)]),
            true,
        );
        assert_eq!(meta.leader("t", 0), Some(2));
        assert_eq!(meta.leader_epoch("t", 0), Some(0));
        assert_eq!(meta.topic_id_for_name("t"), Some([2; 16]));
    }

    /// Control: within one topic ID a lower epoch is still ignored (KIP-320).
    #[test]
    fn test_the_same_topic_id_keeps_the_newer_epoch() {
        let meta = test_metadata();
        meta.update_cache(
            response_with_id([1; 16], vec![partition_response(0, 1, 5, ErrorCode::None)]),
            true,
        );
        meta.update_cache(
            response_with_id([1; 16], vec![partition_response(0, 2, 4, ErrorCode::None)]),
            true,
        );
        assert_eq!(meta.leader("t", 0), Some(1));
        assert_eq!(meta.leader_epoch("t", 0), Some(5));
    }

    /// A leader hint applied while a fetch is in flight survives the fetch's
    /// older epoch: both go through the serialized write path.
    #[test]
    fn test_a_leader_hint_survives_a_later_response_with_an_older_epoch() {
        let meta = metadata_with_leader(5);
        assert!(meta.apply_leader_hint("t", 0, 2, 6, endpoint(2)));
        meta.update_cache(
            metadata_response(vec![partition_response(0, 1, 5, ErrorCode::None)]),
            false,
        );
        assert_eq!(meta.leader("t", 0), Some(2));
        assert_eq!(meta.leader_epoch("t", 0), Some(6));
    }

    /// A response fetched before a rebootstrap is discarded, so the brokers
    /// the rebootstrap dropped do not come back.
    #[test]
    fn test_a_response_fetched_before_a_rebootstrap_is_discarded() {
        let meta = test_metadata();
        let epoch_at_fetch = meta.inner.cache.load().reset_epoch;
        meta.inner.reset_to_seeds();
        let applied = meta.inner.apply(
            metadata_response(vec![partition_response(0, 1, 0, ErrorCode::None)]),
            true,
            epoch_at_fetch,
        );
        assert!(!applied);
        assert!(meta.brokers().is_empty());
        assert!(meta.topic("t").is_none());
    }

    /// Every successful response replaces the broker map, partial or not.
    #[test]
    fn test_a_partial_response_replaces_the_broker_map() {
        let meta = metadata_with_leader(1);
        assert!(meta.broker(2).is_some());
        meta.update_cache(ok_topics_response(&["other"]), false);
        assert!(
            meta.broker(2).is_none(),
            "a broker absent from the response leaves the map"
        );
        assert!(meta.broker(1).is_some());
    }

    /// A topic the broker reports unknown leaves the cache.
    #[test]
    fn test_an_unknown_topic_leaves_the_cache() {
        let meta = test_metadata();
        meta.update_cache(
            metadata_response(vec![partition_response(0, 1, 0, ErrorCode::None)]),
            true,
        );
        let mut gone = metadata_response(vec![]);
        gone.topics[0].error_code = ErrorCode::UnknownTopicOrPartition;
        meta.update_cache(gone, false);
        assert!(meta.topic("t").is_none());
        assert_eq!(
            meta.topic_error("t"),
            Some(ErrorCode::UnknownTopicOrPartition)
        );
    }
}
