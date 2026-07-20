//! Cluster metadata management.
//!
//! This module handles:
//! - Fetching and caching cluster metadata
//! - Topic and partition information
//! - Broker discovery
//! - Leader election tracking

// `AHashMap` is used throughout this module for all internal maps (broker IDs,
// topic names, partition IDs). `ahash` is a non-cryptographic hash function.
// Hash-flooding is not a concern here because all map keys are sourced from
// authenticated Kafka cluster metadata responses — an attacker who controls
// topic names must already have the ability to inject arbitrary cluster
// metadata, at which point hash-flooding is the least of the client's problems.
// Key lengths are also bounded by Kafka's own validation (topic names ≤ 249
// characters, broker IDs are i32).
use ahash::AHashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use parking_lot::Mutex as SyncMutex;
use tokio::sync::oneshot;
use tokio::time::timeout;
use tracing::{debug, info, warn};

use crate::error::{ErrorCode, KrafkaError, Result};
use crate::network::{BrokerConnection, ConnectionPool};
use crate::protocol::{
    ApiKey, MetadataRequest, MetadataResponse, VersionedDecode, VersionedEncode,
};
use crate::util::BackoffPolicy;
use crate::{BrokerId, PartitionId};

/// Strategy for recovering when metadata refresh fails for too long.
///
/// Mirrors Java's `metadata.recovery.strategy`, introduced by KIP-899 (Kafka
/// 3.8) with the two values below. KIP-899's own trigger is "no broker in the
/// current metadata is reachable"; the time-based
/// [`ClusterMetadata::with_rebootstrap_trigger`] that also drives it here comes
/// from KIP-1102 (Kafka 4.0), which additionally made `rebootstrap` the default
/// upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum MetadataRecoveryStrategy {
    /// No automatic recovery — behave like pre-KIP-899 clients. This is
    /// `metadata.recovery.strategy=none`.
    #[default]
    None,
    /// Reset to bootstrap servers and re-discover the cluster when metadata
    /// refresh has not succeeded within the configured trigger duration.
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
/// Retaining the entry is deliberate: dropping it would shrink
/// [`TopicInfo::partition_count`], and a key-hash partitioner computing
/// `hash % partition_count` would then route keys to different partitions for
/// the duration of the outage, silently violating per-key ordering. Routing is
/// instead expected to fail for the individual affected partitions.
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
    /// Whether the topic is internal.
    pub is_internal: bool,
    /// Partition information, keyed by partition ID for O(1) lookup.
    pub partitions: AHashMap<PartitionId, PartitionInfo>,
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

/// What a metadata refresh call actually did.
///
/// Returned by [`ClusterMetadata::refresh_for_topics_outcome`]. The distinction
/// matters for retry loops: a caller that reacted to a stale-leader error and
/// then re-issued its request against *identical* metadata would spin forever.
/// [`RefreshOutcome::RateLimited`] tells the caller that **no** broker
/// round-trip happened and how long to wait before a refresh can succeed.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RefreshOutcome {
    /// A metadata request was sent to a broker and the cache was updated.
    Refreshed,
    /// Every requested topic was already present in the cache and younger than
    /// `metadata.max.age.ms`; no request was sent because none was needed.
    AlreadyFresh,
    /// The refresh was suppressed by the `retry.backoff.ms` rate limiter and
    /// **the cache was not updated**. The payload is how long remains before
    /// another attempt is permitted.
    RateLimited(Duration),
}

impl RefreshOutcome {
    /// Returns `true` when the caller can rely on the cache reflecting a
    /// genuine, recent broker response — i.e. anything but
    /// [`RefreshOutcome::RateLimited`].
    #[inline]
    #[must_use]
    pub fn is_current(self) -> bool {
        !matches!(self, Self::RateLimited(_))
    }

    /// How long to wait before re-issuing, or `None` if no wait is needed.
    #[inline]
    #[must_use]
    pub fn retry_after(self) -> Option<Duration> {
        match self {
            Self::RateLimited(d) => Some(d),
            _ => None,
        }
    }
}

/// Coalescing state for concurrent metadata refresh calls.
///
/// Replaces `tokio::sync::Mutex<()>` (which was held across the entire
/// refresh network round-trip) with a subscriber list:
///
/// - `Idle`: no refresh in flight; the first caller becomes the refresher.
/// - `InFlight`: a refresh is in progress; subsequent callers may subscribe via
///   a oneshot and are woken when the refresh completes.
///
/// A caller may only subscribe when the in-flight refresh actually covers the
/// topics it needs — see [`InFlightTopics::covers`]. Otherwise it would receive
/// the refresher's `Ok(())` for a topic set that was never requested from the
/// broker, and then fail with `no leader for <topic>-<partition>` having
/// apparently just refreshed.
///
/// The `parking_lot::Mutex` wrapping this state is held for at most a few
/// microseconds (just long enough to push/drain the subscriber list) and is
/// **never** held across an `.await` point.
enum RefreshCoalescingState {
    Idle,
    InFlight {
        /// Topics the in-flight refresh asked the broker for. `All` means a
        /// full refresh, which covers every topic.
        topics: InFlightTopics,
        /// Callers waiting on this refresh.
        senders: Vec<oneshot::Sender<Result<RefreshOutcome>>>,
    },
}

/// The topic set an in-flight refresh covers.
#[derive(Debug, Clone)]
enum InFlightTopics {
    /// A full refresh — covers every topic in the cluster.
    All,
    /// A partial refresh limited to these topic names.
    Some(Vec<String>),
}

impl InFlightTopics {
    fn from_request(topics: Option<&[&str]>) -> Self {
        match topics {
            None => Self::All,
            Some(names) => Self::Some(names.iter().map(|n| (*n).to_string()).collect()),
        }
    }

    /// Whether a refresh for `requested` can be satisfied by this in-flight
    /// refresh — i.e. `requested` is a subset of what is already being fetched.
    fn covers(&self, requested: Option<&[&str]>) -> bool {
        match (self, requested) {
            // A full refresh covers anything, including another full refresh.
            (Self::All, _) => true,
            // A partial refresh can never satisfy a full refresh.
            (Self::Some(_), None) => false,
            (Self::Some(in_flight), Some(names)) => names
                .iter()
                .all(|n| in_flight.iter().any(|f| f.as_str() == *n)),
        }
    }
}

/// RAII guard that resets the coalescing state to `Idle` and notifies all
/// waiting subscribers when the refresher completes or is cancelled.
struct RefreshGuard<'a> {
    state: &'a SyncMutex<RefreshCoalescingState>,
    result: Option<Result<RefreshOutcome>>,
}

impl Drop for RefreshGuard<'_> {
    fn drop(&mut self) {
        let result = self.result.take().unwrap_or_else(|| {
            Err(KrafkaError::invalid_state(
                "metadata refresh was cancelled or panicked",
            ))
        });
        let mut st = self.state.lock();
        if let RefreshCoalescingState::InFlight {
            ref mut senders, ..
        } = *st
        {
            for tx in senders.drain(..) {
                let _ = tx.send(result.clone());
            }
        }
        *st = RefreshCoalescingState::Idle;
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

/// Fraction of [`ClusterMetadata::rebootstrap_trigger`] used as random extra
/// delay before a rebootstrap is allowed to fire.
///
/// Without it, a fleet whose clients all lost the cluster at the same instant
/// would rebootstrap in lockstep and hit the seed brokers as one wave.
const REBOOTSTRAP_TRIGGER_JITTER: f64 = 0.2;

/// State of the metadata-refresh rate limiter (KIP-580).
///
/// The delay between refresh *attempts* grows exponentially while refreshes
/// keep failing and resets as soon as one succeeds. A flat delay means every
/// failing partition on every client retries at the same fixed interval, so a
/// cluster that is already struggling receives a steady synchronised drumbeat
/// of metadata requests exactly when it can least afford it.
#[derive(Debug)]
struct RefreshBackoffState {
    /// When the last refresh attempt completed (success or failure).
    /// `None` means no attempt has completed yet, so the next one is free.
    last_attempt_completed: Option<Instant>,
    /// Number of consecutive failed refresh attempts. Reset to zero on
    /// success. Drives the exponent of the backoff curve.
    consecutive_failures: u32,
    /// Delay that must elapse after `last_attempt_completed` before another
    /// attempt is permitted.
    ///
    /// It is computed once, when the attempt completes, rather than on every
    /// rate-limit check. Recomputing it per check would re-sample the jitter
    /// and make [`RefreshOutcome::RateLimited`]'s reported remaining time jump
    /// around between two consecutive calls that observe the same state.
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

    /// How much of `current_delay` is left, or `None` if an attempt is allowed.
    fn remaining(&self) -> Option<Duration> {
        let last = self.last_attempt_completed?;
        let elapsed = last.elapsed();
        if elapsed >= self.current_delay {
            None
        } else {
            Some(self.current_delay - elapsed)
        }
    }

    /// Record a successful refresh: drop back to the base delay.
    ///
    /// The base delay is still applied (and still jittered) so that a caller
    /// looping on a genuinely changing cluster cannot turn every response into
    /// an immediate follow-up request.
    fn record_success(&mut self, policy: &BackoffPolicy) {
        self.consecutive_failures = 0;
        self.current_delay = policy.calculate_backoff(1);
        self.last_attempt_completed = Some(Instant::now());
    }

    /// Record a failed refresh: advance one step along the exponential curve.
    fn record_failure(&mut self, policy: &BackoffPolicy) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        self.current_delay = policy.calculate_backoff(self.consecutive_failures);
        self.last_attempt_completed = Some(Instant::now());
    }
}

/// Cached cluster metadata.
#[derive(Debug, Clone)]
struct MetadataCache {
    /// Cluster ID.
    cluster_id: Option<String>,
    /// Controller broker ID.
    controller_id: BrokerId,
    /// Brokers by ID.
    brokers: AHashMap<BrokerId, BrokerInfo>,
    /// Topics by name. Wrapped in `Arc` so that partial-refresh clones of
    /// the map are O(n) ref-count bumps instead of O(n) deep copies.
    topics: AHashMap<String, Arc<TopicInfo>>,
    /// Topic UUID → topic name map. Topic names are wrapped in `Arc` so that
    /// partial-refresh clones of the map are O(n) ref-count bumps instead of
    /// O(n) deep copies. Populated from metadata v10+ responses where each
    /// topic includes a 16-byte topic_id. Used by the KIP-848 consumer
    /// protocol to resolve topic UUIDs in assignments.
    topic_ids: AHashMap<[u8; 16], Arc<String>>,
    /// Reverse index: topic name → topic UUID. Kept in sync with `topic_ids`
    /// for O(1) lookups.
    ///
    /// # TOCTOU note
    ///
    /// `topic_ids` and `name_to_topic_id` are updated atomically under the
    /// cache write lock. Callers must not assume consistency between a read
    /// from one map and a subsequent independent read from the other without
    /// re-acquiring the lock.
    name_to_topic_id: AHashMap<String, [u8; 16]>,
    /// Per-topic timestamp of the last refresh that included this topic.
    /// Used for TTL-based eviction during partial refreshes.
    topic_last_refreshed: AHashMap<String, Instant>,
    /// When the metadata was last updated.
    last_updated: Instant,
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
            topic_last_refreshed: AHashMap::new(),
            last_updated: Instant::now(),
        }
    }

    fn is_stale(&self, max_age: Duration) -> bool {
        self.last_updated.elapsed() > max_age
    }

    /// Whether `topic` is present **and** was itself refreshed within
    /// `max_age`.
    ///
    /// `last_updated` advances on every refresh, including a partial one for a
    /// completely different topic, so it says nothing about the age of any
    /// individual entry. A client that keeps refreshing topic A would otherwise
    /// serve an hours-old leader map for topic B forever, because the cache as
    /// a whole never looks stale and the entry is present.
    fn topic_is_fresh(&self, topic: &str, max_age: Duration) -> bool {
        self.topics.contains_key(topic)
            && self
                .topic_last_refreshed
                .get(topic)
                .is_some_and(|ts| ts.elapsed() <= max_age)
    }
}

/// Cluster metadata manager.
pub struct ClusterMetadata {
    /// Bootstrap servers (lock-free reads via `ArcSwap` for KIP-899 `update_seed_brokers`).
    bootstrap_servers: ArcSwap<Vec<String>>,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Cached metadata (lock-free reads via `ArcSwap`).
    cache: ArcSwap<MetadataCache>,
    /// Metadata max age before refresh.
    max_age: Duration,
    /// Upper bound on how long a *subscriber* waits for an in-flight refresh
    /// driven by another task.
    ///
    /// This is deliberately **not** `max_age`: bounding the wait by the
    /// metadata max-age (300 s by default) means a nominally "bounded" wait can
    /// block a caller for five minutes behind one stalled refresher. Mirrors
    /// `request.timeout.ms` in the Java client. Default: 30 s.
    request_timeout: Duration,
    /// Coalescing state for concurrent metadata refresh calls.
    ///
    /// The `parking_lot::Mutex` is held only for microseconds (to push/drain
    /// the subscriber list). The actual network I/O happens outside the lock,
    /// preventing slow brokers from serialising all metadata callers.
    refresh_state: SyncMutex<RefreshCoalescingState>,
    /// Exponential-with-jitter backoff between successive refresh *attempts*
    /// (KIP-580). Mirrors `retry.backoff.ms` / `retry.backoff.max.ms` in the
    /// Java client: `initial_backoff` is the base delay after a success or a
    /// first failure, doubling per consecutive failure up to `max_backoff`.
    /// `None` disables rate limiting entirely.
    retry_backoff: Option<BackoffPolicy>,
    /// Rate-limiter state: when the last attempt completed, how many
    /// consecutive failures preceded it, and the delay currently in force.
    refresh_backoff: SyncMutex<RefreshBackoffState>,
    /// Recovery strategy when metadata refresh fails for too long, i.e.
    /// `metadata.recovery.strategy` (KIP-899).
    recovery_strategy: MetadataRecoveryStrategy,
    /// Duration after which a failing metadata refresh triggers a rebootstrap
    /// (only when `recovery_strategy` is [`MetadataRecoveryStrategy::Rebootstrap`]).
    /// The time-based trigger itself is KIP-1102.
    /// Default: 300 s (5 minutes), matching the Java client.
    rebootstrap_trigger: Duration,
    /// Upper bound on the random delay inserted before a rebootstrap tears
    /// down connections and re-dials the seed brokers.
    ///
    /// A fleet that loses the cluster simultaneously (a rack outage, a rolling
    /// restart that goes wrong) would otherwise all rebootstrap at the same
    /// instant and arrive at one seed broker as a single wave, which is exactly
    /// the load spike the seed broker cannot absorb while recovering.
    /// Default: 500 ms. `Duration::ZERO` disables the delay.
    rebootstrap_jitter: Duration,
    /// Instant when the current streak of metadata-refresh failures started.
    /// Reset to `None` on every successful refresh. After a rebootstrap
    /// it is set to the *current* instant (matching Java) so the next cycle
    /// starts timing immediately.
    metadata_attempt_start: SyncMutex<Option<Instant>>,
    /// Maximum age of a cached topic entry before it is evicted during partial
    /// refresh. Defaults to 5 minutes, matching the Java client's
    /// `metadata.max.idle.ms`. `None` disables TTL eviction. When set, topics
    /// not refreshed within this duration are pruned on the next partial
    /// refresh, preventing unbounded cache growth from topic churn.
    topic_cache_ttl: Option<Duration>,
}

impl ClusterMetadata {
    /// Create a new cluster metadata manager.
    pub fn new(
        bootstrap_servers: Vec<String>,
        pool: Arc<ConnectionPool>,
        max_age: Duration,
    ) -> Self {
        Self {
            bootstrap_servers: ArcSwap::from_pointee(bootstrap_servers),
            pool,
            cache: ArcSwap::from_pointee(MetadataCache::new()),
            max_age,
            request_timeout: Duration::from_secs(30),
            refresh_state: SyncMutex::new(RefreshCoalescingState::Idle),
            retry_backoff: Some(Self::default_retry_backoff_policy()),
            refresh_backoff: SyncMutex::new(RefreshBackoffState::new()),
            recovery_strategy: MetadataRecoveryStrategy::None,
            rebootstrap_trigger: Duration::from_secs(300),
            rebootstrap_jitter: Duration::from_millis(500),
            metadata_attempt_start: SyncMutex::new(None),
            // Default to 5 minutes, matching Java's `metadata.max.idle.ms`.
            // Prevents unbounded cache growth on topic churn; callers that
            // want the old unbounded behaviour can opt out via
            // `with_topic_cache_ttl_disabled()`.
            topic_cache_ttl: Some(Duration::from_secs(300)),
        }
    }

    /// Set the metadata recovery strategy, i.e. `metadata.recovery.strategy`
    /// (KIP-899).
    ///
    /// When set to [`MetadataRecoveryStrategy::Rebootstrap`], the client will
    /// automatically close all connections and fall back to bootstrap servers
    /// when metadata refresh has not succeeded within
    /// [`rebootstrap_trigger`](Self::with_rebootstrap_trigger) (that timeout
    /// trigger is KIP-1102).
    #[must_use]
    pub fn with_recovery_strategy(mut self, strategy: MetadataRecoveryStrategy) -> Self {
        self.recovery_strategy = strategy;
        self
    }

    /// Set the duration after which failed metadata refreshes trigger a
    /// rebootstrap (`metadata.recovery.rebootstrap.trigger.ms`, KIP-1102).
    /// Only effective when the recovery strategy is
    /// [`MetadataRecoveryStrategy::Rebootstrap`] (KIP-899). Default: 300 s.
    #[must_use]
    pub fn with_rebootstrap_trigger(mut self, duration: Duration) -> Self {
        self.rebootstrap_trigger = duration;
        self
    }

    /// Set the upper bound on how long a caller waits for a metadata refresh
    /// that another task is already driving.
    ///
    /// Mirrors `request.timeout.ms` in the Java client. Default: 30 s.
    #[must_use]
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set the topic cache TTL for partial refreshes.
    ///
    /// During partial refreshes, cached topics that have not been refreshed
    /// within this duration are evicted to prevent unbounded cache growth.
    /// Full refreshes always rebuild from scratch regardless of this setting.
    ///
    /// Default: 5 minutes (matching Java's `metadata.max.idle.ms`).
    #[must_use]
    pub fn with_topic_cache_ttl(mut self, ttl: Duration) -> Self {
        self.topic_cache_ttl = Some(ttl);
        self
    }

    /// Disable topic cache TTL eviction.
    ///
    /// Partial refreshes will retain cached topic entries indefinitely.
    /// Prefer the default TTL for long-lived clients that discover topics
    /// dynamically (CDC, multi-tenant gateways); disabling TTL eviction can
    /// cause unbounded cache growth on topic churn.
    #[must_use]
    pub fn with_topic_cache_ttl_disabled(mut self) -> Self {
        self.topic_cache_ttl = None;
        self
    }

    /// The default metadata retry backoff: 100 ms base, doubling to a 1 s
    /// ceiling, with ±20% jitter.
    fn default_retry_backoff_policy() -> BackoffPolicy {
        BackoffPolicy {
            initial_backoff: DEFAULT_RETRY_BACKOFF,
            max_backoff: DEFAULT_RETRY_BACKOFF_MAX,
            backoff_multiplier: 2.0,
            jitter_factor: RETRY_BACKOFF_JITTER,
        }
    }

    /// Set the **base** delay between successive metadata refresh attempts.
    ///
    /// This is the first step of the exponential curve described in
    /// [`with_retry_backoff_max`](Self::with_retry_backoff_max), not a flat
    /// interval: after `n` consecutive failed refreshes the delay is
    /// `backoff × 2^(n-1)`, capped and jittered. A successful refresh resets it
    /// back to `backoff`.
    ///
    /// Mirrors `retry.backoff.ms` in the Java client. Default: 100 ms.
    ///
    /// Passing `None` disables rate limiting entirely — every caller that asks
    /// for a refresh gets a broker round-trip. That is almost never what you
    /// want outside tests: it is the configuration that lets one unavailable
    /// partition turn a tight poll loop into a metadata-request storm.
    ///
    /// If `backoff` exceeds the configured maximum, the maximum is raised to
    /// match so the curve stays well-formed.
    #[must_use]
    pub fn with_retry_backoff(mut self, backoff: impl Into<Option<Duration>>) -> Self {
        self.retry_backoff = backoff.into().map(|base| {
            let mut policy = self
                .retry_backoff
                .take()
                .unwrap_or_else(Self::default_retry_backoff_policy);
            policy.initial_backoff = base;
            policy.max_backoff = policy.max_backoff.max(base);
            policy
        });
        self
    }

    /// Set the ceiling for the exponential metadata retry backoff (KIP-580).
    ///
    /// Consecutive refresh failures double the delay — 100 ms, 200 ms, 400 ms,
    /// … — until it reaches this ceiling, where it stays until a refresh
    /// succeeds. Mirrors `retry.backoff.max.ms` in the Java client.
    /// Default: 1 s.
    ///
    /// Values below the base delay are raised to it, so the curve is never
    /// inverted. Has no effect when rate limiting is disabled via
    /// [`with_retry_backoff(None)`](Self::with_retry_backoff).
    #[must_use]
    pub fn with_retry_backoff_max(mut self, max_backoff: Duration) -> Self {
        if let Some(policy) = self.retry_backoff.as_mut() {
            policy.max_backoff = max_backoff.max(policy.initial_backoff);
        }
        self
    }

    /// Replace the whole metadata retry backoff policy.
    ///
    /// Use this to control the multiplier or jitter factor as well as the
    /// bounds. The policy's jitter factor is clamped into `0.0..=1.0` when it
    /// is read, so an out-of-range value degrades to "no jitter" rather than
    /// misbehaving.
    #[must_use]
    pub fn with_retry_backoff_policy(mut self, policy: BackoffPolicy) -> Self {
        self.retry_backoff = Some(policy);
        self
    }

    /// Set the upper bound on the random delay applied before a rebootstrap
    /// closes connections and re-dials the seed brokers (KIP-899/KIP-1102).
    ///
    /// The delay is sampled uniformly from `[0, jitter)` on each rebootstrap so
    /// that a fleet which lost the cluster at the same instant does not arrive
    /// at one seed broker as a single synchronised wave. Default: 500 ms;
    /// `Duration::ZERO` rebootstraps immediately.
    #[must_use]
    pub fn with_rebootstrap_jitter(mut self, jitter: Duration) -> Self {
        self.rebootstrap_jitter = jitter;
        self
    }

    /// Get the bootstrap servers.
    pub fn bootstrap_servers(&self) -> Vec<String> {
        (**self.bootstrap_servers.load()).clone()
    }

    /// Refresh metadata from the cluster.
    pub async fn refresh(&self) -> Result<()> {
        self.refresh_for_topics(None).await
    }

    /// Refresh metadata for specific topics.
    ///
    /// This is the convenience wrapper around
    /// [`refresh_for_topics_outcome`](Self::refresh_for_topics_outcome): when
    /// the rate limiter suppresses an attempt, this method **waits out the
    /// remaining backoff and re-issues** rather than returning a success the
    /// caller never received. A plain `Ok(())` from this method therefore
    /// always means the cache reflects a genuine broker response (or was
    /// already fresh).
    ///
    /// Callers that want to make their own scheduling decision — for example a
    /// bounded retry loop that has other work to do while it waits — should
    /// call [`refresh_for_topics_outcome`](Self::refresh_for_topics_outcome)
    /// and inspect [`RefreshOutcome`].
    ///
    /// # Errors
    ///
    /// Besides the underlying refresh errors, returns
    /// [`KrafkaError::Timeout`] if the rate limiter suppresses every attempt
    /// within [`MAX_RATE_LIMIT_WAITS`](Self::MAX_RATE_LIMIT_WAITS) rounds. That
    /// only happens when other tasks keep winning the race for the same
    /// refresh slot; reporting it is better than returning `Ok(())` for a
    /// refresh that never touched a broker.
    pub async fn refresh_for_topics(&self, topics: Option<&[&str]>) -> Result<()> {
        let mut last_remaining = Duration::ZERO;

        for _ in 0..Self::MAX_RATE_LIMIT_WAITS {
            match self.refresh_for_topics_outcome(topics).await? {
                RefreshOutcome::Refreshed | RefreshOutcome::AlreadyFresh => return Ok(()),
                RefreshOutcome::RateLimited(remaining) => {
                    // The previous attempt completed less than the current
                    // backoff ago. Returning Ok here would hand the caller a
                    // success it never received and leave it retrying against
                    // byte-identical stale metadata. Wait out the backoff, then
                    // really refresh. The backoff grows while the cluster keeps
                    // failing, so this loop cannot become a hot spin.
                    debug!(
                        remaining_ms = remaining.as_millis(),
                        "metadata refresh rate-limited; awaiting backoff before re-issuing"
                    );
                    last_remaining = remaining;
                    tokio::time::sleep(remaining).await;
                }
            }
        }

        Err(KrafkaError::timeout(format!(
            "metadata refresh was rate-limited {} times in a row (last wait {} ms); \
             another task is monopolising the refresh slot",
            Self::MAX_RATE_LIMIT_WAITS,
            last_remaining.as_millis(),
        )))
    }

    /// How many times [`refresh_for_topics`](Self::refresh_for_topics) will
    /// wait out a rate-limit before giving up.
    ///
    /// Each round sleeps for exactly the reported remaining backoff, so under
    /// normal contention the first or second round succeeds. The bound exists
    /// so a caller can never be pinned in the loop indefinitely.
    const MAX_RATE_LIMIT_WAITS: usize = 3;

    /// Refresh metadata for specific topics, reporting what actually happened.
    ///
    /// Concurrent callers are coalesced: the first caller claims the refresher
    /// role while subsequent callers subscribe to the in-flight result via a
    /// oneshot channel. The `parking_lot::Mutex` used for coalescing is held
    /// only for microseconds to read/update the subscriber list and is **never**
    /// held across any `.await` point.
    ///
    /// A caller only joins an in-flight refresh when that refresh covers the
    /// topics it asked for (a full refresh covers everything; a partial refresh
    /// covers a superset of the requested names). A caller asking for topics the
    /// in-flight refresh will not fetch starts its own refresh instead —
    /// otherwise it would be handed an `Ok` for a topic the broker was never
    /// asked about and then fail to find a leader for it.
    ///
    /// Subscriber waits are bounded by
    /// [`request_timeout`](Self::with_request_timeout) (30 s by default). If the
    /// in-flight refresh does not complete within that window, a
    /// [`KrafkaError::Timeout`] is returned so the caller is never blocked
    /// indefinitely behind a stalled refresher (dead broker, network partition,
    /// repeated reconnection retries).
    ///
    /// The Metadata API version is negotiated with the broker (v1–v13).
    /// Versions are cumulative: rack v1, cluster_id v2, offline replicas v5,
    /// leader_epoch v7, authorized-ops v8, flexible encoding v9, topic UUIDs v10,
    /// cluster_authorized_operations removed v11, topic_id works v12,
    /// top-level error_code v13.
    /// Falls back to METADATA_MIN (v1) if the broker doesn't advertise higher
    /// Metadata support.
    ///
    /// When [`MetadataRecoveryStrategy::Rebootstrap`] is configured (KIP-899)
    /// and no broker is reachable for longer than
    /// [`rebootstrap_trigger`](Self::with_rebootstrap_trigger) (KIP-1102), all
    /// connections are closed and the client falls back to bootstrap servers.
    pub async fn refresh_for_topics_outcome(
        &self,
        topics: Option<&[&str]>,
    ) -> Result<RefreshOutcome> {
        self.refresh_for_topics_outcome_inner(topics, false).await
    }

    /// Refresh metadata for `topics`, ignoring the cache-age check.
    ///
    /// Use this when a broker has told us the cache is *wrong* rather than
    /// merely old — `NOT_LEADER_FOR_PARTITION`, `FENCED_LEADER_EPOCH`,
    /// `UNKNOWN_TOPIC_OR_PARTITION` on a topic we believe exists. A leader move
    /// does not age the cached entry, so the ordinary age gate would report
    /// `AlreadyFresh` and the caller would keep retrying against the stale
    /// leader until its delivery timeout expired without ever asking a broker.
    ///
    /// The `retry.backoff.ms` rate limiter still applies, so this cannot be
    /// used to storm the cluster: a burst of leader-move errors collapses into
    /// one request per backoff interval.
    ///
    /// This mirrors `Metadata.requestUpdate()` in the Java client, which sets an
    /// explicit update flag that the age check does not override.
    pub async fn refresh_for_topics_forced(&self, topics: Option<&[&str]>) -> Result<()> {
        let mut last_remaining = Duration::ZERO;

        for _ in 0..Self::MAX_RATE_LIMIT_WAITS {
            match self.refresh_for_topics_outcome_inner(topics, true).await? {
                RefreshOutcome::Refreshed | RefreshOutcome::AlreadyFresh => return Ok(()),
                RefreshOutcome::RateLimited(remaining) => {
                    last_remaining = remaining;
                    tokio::time::sleep(remaining).await;
                }
            }
        }

        Err(KrafkaError::timeout(format!(
            "metadata refresh remained rate-limited after {} attempts ({:?} remaining)",
            Self::MAX_RATE_LIMIT_WAITS,
            last_remaining
        )))
    }

    async fn refresh_for_topics_outcome_inner(
        &self,
        topics: Option<&[&str]>,
        force: bool,
    ) -> Result<RefreshOutcome> {
        // Coalesce concurrent calls without holding a mutex across network I/O.
        //
        // First, we atomically claim the "refresher" role, subscribe to a
        // compatible in-flight refresh, or decide to run a second concurrent
        // refresh. The parking_lot lock is released before any await.
        let role = {
            let mut state = self.refresh_state.lock();
            match *state {
                RefreshCoalescingState::Idle => {
                    // We are the refresher; claim the in-flight slot and record
                    // which topics this refresh will cover.
                    *state = RefreshCoalescingState::InFlight {
                        topics: InFlightTopics::from_request(topics),
                        senders: Vec::new(),
                    };
                    Some(None)
                }
                RefreshCoalescingState::InFlight {
                    topics: ref in_flight,
                    ref mut senders,
                } => {
                    if in_flight.covers(topics) {
                        // A compatible refresh is already in progress —
                        // subscribe to be woken when it completes. The lock is
                        // released before the await.
                        //
                        // Prune senders whose receivers have already been
                        // dropped (timed-out or cancelled callers) to prevent
                        // unbounded memory growth during a prolonged stall.
                        senders.retain(|tx| !tx.is_closed());
                        let (tx, rx) = oneshot::channel();
                        senders.push(tx);
                        Some(Some(rx))
                    } else {
                        // The in-flight refresh will not fetch what we need.
                        // Run a second, independent refresh. Cache updates are
                        // atomic `ArcSwap` merges, so concurrent refreshes are
                        // safe.
                        debug!(
                            "in-flight metadata refresh does not cover the requested topics; \
                             starting an independent refresh"
                        );
                        None
                    }
                }
            }
        }; // ← parking_lot lock released here, before any .await

        let Some(role) = role else {
            // Independent refresh: we did not claim the coalescing slot, so
            // there is no guard to drop and no subscribers to notify.
            return self.refresh_for_topics_inner_forced(topics, force).await;
        };

        if let Some(rx) = role {
            // Bound the wait by `request_timeout` so that a stalled refresher
            // (dead broker + long reconnection retries) cannot block
            // subscribers for the full metadata max-age.
            return match timeout(self.request_timeout, rx).await {
                Ok(Ok(result)) => result,
                Ok(Err(_)) => {
                    // The refresher task was cancelled or panicked.
                    // `RefreshGuard::drop` resets state to `Idle` and notifies
                    // all subscribers before dropping, so `rx` returning
                    // `Err(RecvError)` is the signal that the state is already
                    // `Idle`.  However, the refresher's result was not
                    // propagated (it errored/panicked), so we return an error
                    // here.  Recursing would retry with no bound; instead we
                    // propagate the failure and let the caller decide.
                    warn!("in-flight metadata refresh was cancelled or panicked");
                    Err(KrafkaError::invalid_state(
                        "metadata refresh was cancelled or panicked",
                    ))
                }
                Err(_elapsed) => {
                    // The original refresher is still running (state is still
                    // `InFlight`).  Recursing here would re-subscribe and time
                    // out again — unbounded recursion.  Return an error
                    // directly; the caller decides whether to retry.
                    warn!(
                        timeout_ms = self.request_timeout.as_millis(),
                        "timed out waiting for in-flight metadata refresh"
                    );
                    Err(KrafkaError::timeout(
                        "metadata refresh timed out waiting for in-flight refresh",
                    ))
                }
            };
        }

        // We are the refresher. The `RefreshGuard` ensures that all subscribers
        // are notified and the state is reset to `Idle` even if this task is
        // cancelled or the inner function panics.
        let mut guard = RefreshGuard {
            state: &self.refresh_state,
            result: None,
        };

        let result = self.refresh_for_topics_inner_forced(topics, force).await;
        guard.result = Some(result.clone());
        drop(guard); // drain subscribers and reset to Idle
        result
    }

    /// Core metadata refresh logic.  Called once the caller has resolved its
    /// coalescing role; never called directly by users.
    /// Core refresh. `force` skips the cache-age check but not the rate limiter.
    async fn refresh_for_topics_inner_forced(
        &self,
        topics: Option<&[&str]>,
        force: bool,
    ) -> Result<RefreshOutcome> {
        // Check if the requested data is already fresh.
        //
        // This is checked *before* the rate limiter: when the cache already
        // satisfies the request there is nothing to wait for.
        //
        // For partial refreshes: skip if every requested topic is present in the
        // cache and was refreshed within `max_age`. This deduplicates work when
        // multiple callers ask for overlapping topic sets — the second caller
        // finds the first caller's result still fresh and returns immediately.
        //
        // Full refreshes (`topics=None`) are never skipped: a recent partial
        // refresh does not guarantee a full-cluster snapshot.
        let cache = self.cache.load();
        if !cache.brokers.is_empty() && !force {
            let all_fresh = match topics {
                None => false,
                Some(names) => names.iter().all(|name| {
                    cache.topics.contains_key(*name)
                        && cache
                            .topic_last_refreshed
                            .get(*name)
                            .is_some_and(|ts| ts.elapsed() <= self.max_age)
                }),
            };
            if all_fresh {
                debug!("All requested topics are fresh in cache, skipping redundant request");
                return Ok(RefreshOutcome::AlreadyFresh);
            }
        }
        drop(cache);

        // Enforce the exponential inter-refresh backoff (KIP-580, mirroring
        // `retry.backoff.ms` / `retry.backoff.max.ms` in the Java client) so
        // that a tight poll loop on LEADER_NOT_AVAILABLE cannot create a
        // metadata-refresh storm — and so that the storm decays instead of
        // holding a constant rate while the cluster is unhealthy.
        //
        // Crucially this reports `RateLimited` rather than `Ok(())`: the cache
        // was *not* updated, and a caller told "refreshed" here would re-issue
        // its request against byte-identical stale metadata and make no
        // progress.
        if self.retry_backoff.is_some()
            && let Some(remaining) = self.refresh_backoff.lock().remaining()
        {
            debug!(
                remaining_ms = remaining.as_millis(),
                "metadata refresh rate-limited; no request sent"
            );
            return Ok(RefreshOutcome::RateLimited(remaining));
        }

        // Record the start of this refresh attempt so the KIP-1102 rebootstrap
        // trigger can measure how long refreshes have been failing.
        // If there is already a recorded start (from a previous failing attempt),
        // keep it — we only care about how long the *streak* has lasted.
        {
            let mut start = self.metadata_attempt_start.lock();
            start.get_or_insert_with(Instant::now);
        }

        // Every exit path from the attempt — connection failure, request
        // failure, decode failure, broker error, success — must feed the rate
        // limiter. An attempt that fails before it reaches the broker is the
        // one that most needs to back off: it is the signature of a cluster
        // that is down, and leaving it unrecorded meant those attempts ran
        // completely ungoverned.
        let result = self.refresh_attempt(topics).await;
        if let Some(policy) = self.retry_backoff.as_ref() {
            let mut backoff = self.refresh_backoff.lock();
            match &result {
                Ok(_) => backoff.record_success(policy),
                Err(_) => backoff.record_failure(policy),
            }
        }
        result
    }

    /// Perform a single metadata refresh attempt against some reachable
    /// broker, updating the cache on success.
    ///
    /// Rate limiting and freshness checks are the caller's responsibility; this
    /// function always talks to a broker. It returns
    /// [`RefreshOutcome::Refreshed`] or an error — never `AlreadyFresh` or
    /// `RateLimited`.
    async fn refresh_attempt(&self, topics: Option<&[&str]>) -> Result<RefreshOutcome> {
        // Allow at most one rebootstrap retry per refresh call.
        let mut rebootstrapped = false;

        loop {
            // Get a connection — on failure, check if rebootstrap is needed.
            let conn = match self.get_any_connection().await {
                Ok(conn) => conn,
                Err(e) => {
                    if !rebootstrapped && self.needs_rebootstrap() {
                        self.rebootstrap().await;
                        rebootstrapped = true;
                        // Retry once after rebootstrap.
                        self.get_any_connection().await?
                    } else {
                        return Err(e);
                    }
                }
            };

            // Negotiate the highest mutually supported Metadata version up to the
            // client's supported maximum (`METADATA_MAX`).
            // v1+ required, up to v13 (top-level error_code).
            let metadata_version = conn
                .negotiate_api_version(
                    ApiKey::Metadata,
                    crate::protocol::versions::METADATA_MAX,
                    crate::protocol::versions::METADATA_MIN,
                )
                .await
                .unwrap_or_else(|| {
                    debug!("Metadata API version negotiation unavailable; falling back to MIN");
                    crate::protocol::versions::METADATA_MIN
                });

            // Build and send metadata request
            let request = match topics {
                Some(t) => MetadataRequest::for_topics(t.to_vec()),
                None => MetadataRequest::all_topics(),
            };

            let response = conn
                .send_request(ApiKey::Metadata, metadata_version, |buf| {
                    request.encode_versioned(metadata_version, buf)
                })
                .await?;

            // Decode response
            let mut buf = response;
            let metadata = MetadataResponse::decode_versioned(metadata_version, &mut buf)?;

            // v13+ includes a top-level error code. Check it before processing
            // topics. Per-topic errors are still handled individually in update_cache.
            if metadata.error_code == ErrorCode::RebootstrapRequired {
                if rebootstrapped {
                    // Already retried once — don't loop forever.
                    return Err(KrafkaError::broker(
                        metadata.error_code,
                        "server requested rebootstrap but retry also returned REBOOTSTRAP_REQUIRED",
                    ));
                }
                // Server-initiated rebootstrap (KIP-1102): `REBOOTSTRAP_REQUIRED`
                // (error code 129) in the Metadata v13 top-level error field is
                // the cluster telling us to re-discover via bootstrap servers,
                // without waiting for the client-side failure timer.
                info!("Server requested rebootstrap (REBOOTSTRAP_REQUIRED)");
                self.rebootstrap().await;
                rebootstrapped = true;
                continue;
            }
            if !metadata.error_code.is_ok() {
                return Err(KrafkaError::broker(
                    metadata.error_code,
                    "metadata request failed",
                ));
            }

            // Success — clear the failure-tracking timestamp on every successful
            // response, including partial refreshes.
            //
            // The Java client resets the failure timer on any successful metadata
            // response (partial or full). A previous krafka comment argued that a
            // partial refresh doesn't prove all brokers are reachable — but
            // `metadata_attempt_start` tracks whether the client can reach *any*
            // broker, which a successful partial refresh confirms. Keeping the
            // timer running after a successful partial refresh would trigger a
            // spurious rebootstrap for consumers that never issue full refreshes.
            {
                let mut start = self.metadata_attempt_start.lock();
                *start = None;
            }

            // Update cache. A full refresh (topics=None) is authoritative — the
            // response contains every topic currently in the cluster, so we rebuild
            // from scratch. A partial refresh delta-merges into the existing cache.
            let full_refresh = topics.is_none();

            self.update_cache(metadata, full_refresh);

            return Ok(RefreshOutcome::Refreshed);
        }
    }

    /// Replace the bootstrap server list at runtime (KIP-899).
    ///
    /// This does **not** trigger a rebootstrap or close existing connections.
    /// The new addresses are used on the next metadata refresh that falls back
    /// to bootstrap servers (e.g. after all cached brokers become unreachable).
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
        self.bootstrap_servers.store(Arc::new(servers));
        Ok(())
    }

    /// Force a rebootstrap: close all connections, clear the metadata cache,
    /// and fall back to bootstrap servers — the recovery action KIP-899 defines
    /// as `metadata.recovery.strategy=rebootstrap`.
    ///
    /// The next call to [`refresh`](Self::refresh) or
    /// [`refresh_for_topics`](Self::refresh_for_topics) will re-discover the
    /// cluster from the bootstrap addresses.
    ///
    /// After rebootstrap, the failure-tracking timer is set to **now** (not
    /// cleared) so that the next refresh cycle starts timing immediately —
    /// matching the Java client's `metadataAttemptStartMs = Optional.of(now)`.
    ///
    /// # Warning: In-Flight Requests Are Cancelled
    ///
    /// `close_all()` closes every broker connection immediately. Any `Produce`,
    /// `Fetch`, or `OffsetCommit` requests that are in flight at the time of
    /// rebootstrap will be cancelled and return errors to their callers. Callers
    /// that perform retries will retry after the pool reconnects; callers with
    /// `acks=0` (fire-and-forget) or non-retryable errors may lose data.
    ///
    /// This is an inherent limitation of the connection-drop recovery strategy.
    /// For zero-data-loss recovery, use `acks=all` with retries and a
    /// [`TransactionalProducer`](crate::producer::TransactionalProducer).
    /// # Seed-broker DNS
    ///
    /// Seed brokers are held as `host:port` strings and resolved at dial time,
    /// never as cached `SocketAddr`s. Because the rebootstrap empties the
    /// metadata cache and `ConnectionPool::close_all` drains the connection
    /// maps, the next dial is a fresh resolution of the seed hostnames. A
    /// client whose brokers moved to new IPs behind a load balancer therefore
    /// recovers, instead of retrying addresses that no longer answer.
    ///
    /// # Jitter
    ///
    /// A random delay of up to
    /// [`rebootstrap_jitter`](Self::with_rebootstrap_jitter) (500 ms by
    /// default) precedes the teardown so a fleet that lost the cluster at the
    /// same moment does not hit the seed brokers as one synchronised wave.
    pub async fn rebootstrap(&self) {
        // Spread the fleet out before doing anything observable. Sampling in a
        // block keeps the (non-Send) thread-local RNG out of the future.
        let delay = if self.rebootstrap_jitter.is_zero() {
            Duration::ZERO
        } else {
            use rand::Rng as _;
            let nanos = rand::rng().random_range(0..self.rebootstrap_jitter.as_nanos().max(1));
            Duration::from_nanos(nanos.min(u64::MAX as u128) as u64)
        };
        if !delay.is_zero() {
            debug!(
                delay_ms = delay.as_millis(),
                "delaying rebootstrap by a random interval to avoid a seed-broker stampede"
            );
            tokio::time::sleep(delay).await;
        }

        warn!(
            "Rebootstrapping: closing all connections and cancelling in-flight requests (KIP-899). \
             In-flight Produce/Fetch/Commit requests will return errors; retries will recover."
        );

        // Close all pooled connections — this cancels all in-flight requests
        // and drops every cached socket, so the next dial re-resolves DNS.
        self.pool.close_all().await;

        // Reset metadata cache to empty so `get_any_connection` goes straight
        // to bootstrap servers, re-resolving their hostnames.
        self.cache.store(Arc::new(MetadataCache::new()));

        // Set the failure tracker to *now* (not None) so the next cycle starts
        // timing immediately — if the rebootstrap itself doesn't help, we'll
        // know how long it's been since we last rebootstrapped.
        {
            let mut start = self.metadata_attempt_start.lock();
            *start = Some(Instant::now());
        }
    }

    /// Check whether the rebootstrap trigger duration has elapsed.
    ///
    /// This is a pure predicate — it does **not** perform the rebootstrap.
    /// The caller is responsible for calling [`rebootstrap`](Self::rebootstrap)
    /// if this returns `true`.
    ///
    /// # Why this cannot fire in a tight loop
    ///
    /// The timer it reads, `metadata_attempt_start`, is set to *now* by
    /// [`rebootstrap`](Self::rebootstrap) rather than cleared. A second
    /// rebootstrap therefore requires another full trigger period of continuous
    /// failure, even when the whole cluster is down and every refresh fails
    /// immediately. Refresh attempts themselves are separately governed by the
    /// exponential retry backoff.
    ///
    /// The trigger is compared against a randomly extended deadline (up to 20%
    /// beyond the configured value) so clients that started failing together do
    /// not all cross the threshold on the same tick.
    fn needs_rebootstrap(&self) -> bool {
        if self.recovery_strategy != MetadataRecoveryStrategy::Rebootstrap {
            return false;
        }

        let start = self.metadata_attempt_start.lock();
        let Some(attempt_start) = *start else {
            return false;
        };
        let elapsed = attempt_start.elapsed();
        drop(start);

        let effective_trigger = {
            use rand::Rng as _;
            let spread = self.rebootstrap_trigger.mul_f64(REBOOTSTRAP_TRIGGER_JITTER);
            if spread.is_zero() {
                self.rebootstrap_trigger
            } else {
                self.rebootstrap_trigger
                    + Duration::from_nanos(
                        rand::rng()
                            .random_range(0..spread.as_nanos().max(1))
                            .min(u64::MAX as u128) as u64,
                    )
            }
        };

        if elapsed < effective_trigger {
            return false;
        }

        warn!(
            elapsed_ms = elapsed.as_millis(),
            trigger_ms = self.rebootstrap_trigger.as_millis(),
            "Metadata refresh failing too long, rebootstrap needed (KIP-1102)"
        );

        true
    }

    /// Get a connection to any available broker.
    ///
    /// Candidates are the cached brokers plus any bootstrap servers not already
    /// among them. Rather than racing *every* candidate — which on a 100-broker
    /// cluster means up to 100 concurrent TCP + TLS + SASL handshakes on each
    /// refresh — the list is shuffled and a bounded subset is raced. Shuffling
    /// keeps load spread across the cluster instead of hammering whichever
    /// broker happens to hash first.
    ///
    /// If a whole batch fails, the next batch is tried, so a partially
    /// unreachable cluster still converges on a live broker.
    async fn get_any_connection(&self) -> Result<Arc<BrokerConnection>> {
        /// How many connection attempts to race concurrently.
        const CONNECT_FANOUT: usize = 3;

        let mut addrs = self.connection_candidates();

        if addrs.is_empty() {
            return Err(KrafkaError::invalid_state(
                "no available brokers to connect to",
            ));
        }

        // Shuffle so repeated refreshes do not all stampede the same broker.
        {
            use rand::seq::SliceRandom as _;
            let mut rng = rand::rng();
            addrs.shuffle(&mut rng);
        }

        for chunk in addrs.chunks(CONNECT_FANOUT) {
            // Race this bounded batch; the first successful connection wins.
            let futs: Vec<_> = chunk
                .iter()
                .map(|addr| {
                    let pool = Arc::clone(&self.pool);
                    let addr = addr.clone();
                    Box::pin(async move { pool.get_connection(&addr).await })
                })
                .collect();

            if let Ok((conn, _rest)) = futures::future::select_ok(futs).await {
                return Ok(conn);
            }
        }

        Err(KrafkaError::invalid_state(
            "no available brokers to connect to",
        ))
    }

    /// Build the candidate address list for [`get_any_connection`]: every
    /// cached broker, followed by any seed broker not already among them.
    ///
    /// Addresses are `host:port` strings, never pre-resolved `SocketAddr`s —
    /// the connection layer resolves them on each dial. That is what lets a
    /// client recover when brokers move to new IPs behind a load balancer:
    /// after a rebootstrap the cache is empty, so the only candidates are the
    /// seed hostnames and they are resolved afresh.
    ///
    /// Deduplication uses a set rather than a linear scan; on a large cluster
    /// the scan was quadratic in the broker count on every refresh.
    fn connection_candidates(&self) -> Vec<String> {
        let cache = self.cache.load();
        let servers = self.bootstrap_servers.load();

        let mut addrs: Vec<String> = Vec::with_capacity(cache.brokers.len() + servers.len());
        let mut seen: ahash::AHashSet<&str> = ahash::AHashSet::with_capacity(cache.brokers.len());

        for broker in cache.brokers.values() {
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

    /// Update the metadata cache from a response.
    ///
    /// Builds a new snapshot and swaps it in atomically via `ArcSwap`.
    ///
    /// When `full_refresh` is true the response is authoritative (all topics in
    /// the cluster), so the broker and topic maps are rebuilt from scratch.
    /// When false (partial/topic-specific refresh), the response is delta-merged
    /// into the existing cache so that topics not in the request are preserved
    /// and broker entries referenced by preserved topics remain available.
    fn update_cache(&self, response: MetadataResponse, full_refresh: bool) {
        let old = self.cache.load();
        let now = Instant::now();

        // Full refresh: response is authoritative — start empty.
        // Partial refresh: merge into the existing broker map so preserved
        // topics cannot end up referencing brokers missing from the cache.
        let mut brokers = if full_refresh {
            AHashMap::new()
        } else {
            old.brokers.clone()
        };
        for broker in response.brokers {
            brokers.insert(
                broker.node_id,
                BrokerInfo::new(broker.node_id, broker.host, broker.port, broker.rack),
            );
        }

        // Full refresh: response is authoritative — start empty.
        // Partial refresh: delta-merge into existing topics and topic_ids,
        // optionally evicting entries older than `topic_cache_ttl`.
        let mut topics = if full_refresh {
            AHashMap::new()
        } else if let Some(ttl) = self.topic_cache_ttl {
            let retained: AHashMap<_, _> = old
                .topics
                .iter()
                .filter(|(name, _)| {
                    old.topic_last_refreshed
                        .get(*name)
                        .is_some_and(|ts| now.duration_since(*ts) <= ttl)
                })
                .map(|(k, v)| (k.clone(), Arc::clone(v)))
                .collect();
            let evicted = old.topics.len().saturating_sub(retained.len());
            if evicted > 0 {
                debug!(
                    evicted,
                    ttl_secs = ttl.as_secs(),
                    "evicted stale topics from metadata cache"
                );
            }
            retained
        } else {
            old.topics.clone()
        };
        let mut topic_ids = if full_refresh {
            AHashMap::new()
        } else if self.topic_cache_ttl.is_some() {
            // Keep only topic_ids whose names survived TTL eviction.
            old.topic_ids
                .iter()
                .filter(|(_, name)| topics.contains_key(name.as_str()))
                .map(|(k, v)| (*k, Arc::clone(v)))
                .collect()
        } else {
            old.topic_ids.clone()
        };

        // Build a reverse index (name → UUID) so we can remove the old UUID
        // for a topic name in O(1) instead of scanning the entire map.
        let mut name_to_uuid: AHashMap<String, [u8; 16]> = topic_ids
            .iter()
            .map(|(uuid, name)| (name.as_ref().clone(), *uuid))
            .collect();

        // Track which topic names are actually provided by this response so
        // that only those entries get their `topic_last_refreshed` timestamp
        // advanced to `now`.  Retained-from-cache topics must keep their
        // original timestamps; resetting them would make them perpetually
        // "fresh" and defeat TTL eviction.
        let mut response_topic_names: Vec<String> = Vec::new();

        for topic in response.topics {
            let Some(topic_name) = topic.name else {
                continue;
            };

            if !topic.error_code.is_ok() {
                if topic.error_code.is_retriable() {
                    // Transient errors (LeaderNotAvailable, RequestTimedOut, etc.)
                    // — keep the stale cache entry so callers don't see the topic
                    // as "unknown" until the next successful refresh.
                    //
                    // Also treat the transient response as a TTL refresh signal:
                    // the broker knows about this topic, so we stamp it with `now`
                    // to prevent premature TTL eviction.  Two sub-cases:
                    //
                    // 1. Topic survived TTL eviction above (still in `topics`):
                    //    no entry change needed, just reset the timestamp.
                    // 2. Topic was already TTL-evicted before the loop:
                    //    restore it from `old.topics` so it is not silently lost.
                    debug!(
                        "Topic {} has transient error: {:?}, keeping stale cache entry",
                        topic_name, topic.error_code
                    );
                    if !topics.contains_key(&topic_name)
                        && let Some(old_info) = old.topics.get(&topic_name)
                    {
                        // Restore the stale entry: the topic was TTL-evicted
                        // before the response loop, but the broker still
                        // acknowledges it (even transiently).
                        topics.insert(topic_name.clone(), Arc::clone(old_info));
                        // Also restore the UUID mapping so that
                        // `topic_id_for_name()` keeps working (e.g. for
                        // ShareConsumer fetch routing that requires topic IDs).
                        if let Some(&old_uuid) = old.name_to_topic_id.get(&topic_name)
                            && let Some(name_arc) = old.topic_ids.get(&old_uuid)
                        {
                            topic_ids.insert(old_uuid, Arc::clone(name_arc));
                            name_to_uuid.insert(topic_name.clone(), old_uuid);
                        }
                    }
                    // Only stamp TTL for topics that are actually in the cache
                    // (survived eviction or just restored from old).  Topics
                    // with a transient error but no prior cache entry are
                    // skipped, preventing orphaned entries in
                    // `topic_last_refreshed` with no corresponding `topics` key.
                    if topics.contains_key(&topic_name) {
                        response_topic_names.push(topic_name);
                    }
                } else {
                    // Permanent errors (UnknownTopicOrPartition, TopicAuthorizationFailed,
                    // InvalidTopic, etc.) — remove from cache.
                    warn!("Topic {} has error: {:?}", topic_name, topic.error_code);
                    if let Some(tid) = topic.topic_id {
                        topic_ids.remove(&tid);
                    }
                    // Also remove any stale UUID → name mapping by name, in case
                    // the error response omitted topic_id or it was an all-zero UUID.
                    if let Some(old_uuid) = name_to_uuid.remove(&topic_name) {
                        topic_ids.remove(&old_uuid);
                    }
                    topics.remove(&topic_name);
                    // No TTL timestamp is removed here: `topic_last_refreshed`
                    // is rebuilt below by filtering against the final `topics`
                    // map, so a topic dropped here cannot leave an orphaned
                    // timestamp behind. That filter is what bounds the map
                    // under high topic churn.
                }
                continue;
            }

            // Track topic UUID → name mapping (v10+).
            // Remove any old UUID that previously mapped to this name first —
            // the topic may have been recreated with a new UUID.
            if let Some(tid) = topic.topic_id {
                if let Some(old_uuid) = name_to_uuid.remove(&topic_name) {
                    topic_ids.remove(&old_uuid);
                }
                let topic_arc = Arc::new(topic_name.clone());
                topic_ids.insert(tid, topic_arc);
                name_to_uuid.insert(topic_name.clone(), tid);
            }

            // Previous view of this topic, used for the KIP-320 leader-epoch
            // merge below.
            let cached_topic = old.topics.get(&topic_name);

            // Every partition the broker reported is retained, including those
            // in an error state. Dropping errored partitions would shrink
            // `partition_count()` mid-outage and silently re-map a key-hash
            // partitioner's `hash % partition_count`, breaking per-key ordering
            // for as long as the partitions stay unavailable.
            let partitions: AHashMap<PartitionId, PartitionInfo> = topic
                .partitions
                .into_iter()
                .map(|p| {
                    if !p.offline_replicas.is_empty() {
                        debug!(
                            topic = %topic_name,
                            partition = p.partition_index,
                            offline_replicas = ?p.offline_replicas,
                            "partition has offline replicas; routing may be impaired if the leader is unavailable"
                        );
                    }

                    let healthy = p.error_code.is_ok();
                    if !healthy {
                        debug!(
                            topic = %topic_name,
                            partition = p.partition_index,
                            error = ?p.error_code,
                            "partition reported an error; retaining entry with no leader"
                        );
                    }

                    let incoming = PartitionInfo {
                        topic: topic_name.clone(),
                        partition: p.partition_index,
                        // An errored partition has no trustworthy leader; mark
                        // it unroutable rather than dialling a stale broker.
                        leader: if healthy { p.leader_id } else { -1 },
                        leader_epoch: if healthy { p.leader_epoch } else { -1 },
                        replicas: p.replica_nodes,
                        isr: p.isr_nodes,
                        offline_replicas: p.offline_replicas,
                        error_code: p.error_code,
                    };

                    // KIP-320 leader-epoch fencing (mirrors Java's
                    // `Metadata.updatePartitionMetadata`): a lagging broker can
                    // answer with an older epoch than we already hold. Applying
                    // it would revert the client to the *previous* leader until
                    // the next refresh — precisely the silent wrong-leader
                    // window KIP-320 exists to close. Keep the newer entry.
                    //
                    // Epochs of -1 mean "unknown" (Metadata < v7, or an error
                    // state) and never participate in the comparison.
                    let merged = match cached_topic.and_then(|t| t.partitions.get(&p.partition_index)) {
                        Some(cached)
                            if cached.leader_epoch >= 0
                                && incoming.leader_epoch >= 0
                                && incoming.leader_epoch < cached.leader_epoch =>
                        {
                            debug!(
                                topic = %topic_name,
                                partition = p.partition_index,
                                cached_epoch = cached.leader_epoch,
                                response_epoch = incoming.leader_epoch,
                                "ignoring stale leader epoch from metadata response (KIP-320)"
                            );
                            cached.clone()
                        }
                        _ => incoming,
                    };

                    (p.partition_index, merged)
                })
                .collect();

            response_topic_names.push(topic_name.clone());
            topics.insert(
                topic_name.clone(),
                Arc::new(TopicInfo {
                    name: topic_name,
                    is_internal: topic.is_internal,
                    partitions,
                }),
            );
        }

        // Build topic_last_refreshed:
        // - Full refresh: start empty; every topic comes from this response.
        // - Partial refresh with TTL: carry forward only entries that survived
        //   TTL eviction (with their *original* timestamps so their age is
        //   preserved); retained topics must NOT have their clock reset.
        // - Partial refresh without TTL: carry forward all existing entries
        //   that are still present in the `topics` map.  Filtering against
        //   `topics` ensures that permanently-errored topics (removed above)
        //   don't leave orphaned entries that grow unboundedly under high
        //   topic churn.
        // In all cases, only topics that appear in the current response are
        // stamped with `now`; retained-from-cache topics keep their existing
        // timestamps so TTL eviction can fire correctly on the next refresh.
        let mut topic_last_refreshed = if full_refresh {
            AHashMap::with_capacity(response_topic_names.len())
        } else if let Some(ttl) = self.topic_cache_ttl {
            old.topic_last_refreshed
                .iter()
                .filter(|(name, ts)| {
                    now.duration_since(**ts) <= ttl && topics.contains_key(name.as_str())
                })
                .map(|(k, v)| (k.clone(), *v))
                .collect()
        } else {
            // Retain only entries whose topic is still alive in the cache.
            old.topic_last_refreshed
                .iter()
                .filter(|(name, _)| topics.contains_key(name.as_str()))
                .map(|(k, v)| (k.clone(), *v))
                .collect()
        };
        // Stamp only topics included in this response with `now`.
        // For a full refresh `response_topic_names` covers all topics (the map
        // started empty).  For a partial refresh this correctly skips
        // retained-only entries, preserving their original timestamps.
        for name in response_topic_names {
            topic_last_refreshed.insert(name, now);
        }

        let new_cache = MetadataCache {
            cluster_id: response.cluster_id,
            controller_id: response.controller_id,
            brokers,
            topics,
            topic_ids,
            name_to_topic_id: name_to_uuid,
            topic_last_refreshed,
            last_updated: now,
        };

        debug!(
            "Updated metadata: {} brokers, {} topics",
            new_cache.brokers.len(),
            new_cache.topics.len()
        );

        self.cache.store(Arc::new(new_cache));
    }

    /// Get broker info by ID.
    pub fn broker(&self, broker_id: BrokerId) -> Option<BrokerInfo> {
        self.cache.load().brokers.get(&broker_id).cloned()
    }

    /// Get all brokers.
    pub fn brokers(&self) -> Vec<BrokerInfo> {
        self.cache.load().brokers.values().cloned().collect()
    }

    /// Get topic info by name, deep-cloning the entry.
    ///
    /// Prefer [`topic_arc`](Self::topic_arc), which is an `Arc` ref-count bump
    /// instead of a full copy of the topic's partition map.
    pub fn topic(&self, name: &str) -> Option<TopicInfo> {
        self.topic_arc(name).map(|t| t.as_ref().clone())
    }

    /// Get topic info by name without copying the partition map.
    ///
    /// The cache stores each [`TopicInfo`] behind an `Arc`, so this is a
    /// ref-count bump regardless of how many partitions the topic has.
    pub fn topic_arc(&self, name: &str) -> Option<Arc<TopicInfo>> {
        self.cache.load().topics.get(name).map(Arc::clone)
    }

    /// Resolve a 16-byte topic UUID to a topic name.
    ///
    /// The mapping is populated from metadata v10+ responses where each topic
    /// includes a `topic_id`. Returns `None` if the UUID is unknown — the
    /// caller should trigger a metadata refresh and retry.
    pub fn topic_name_for_id(&self, topic_id: &[u8; 16]) -> Option<String> {
        self.cache
            .load()
            .topic_ids
            .get(topic_id)
            .map(|name| (**name).clone())
    }

    /// Resolve a topic name to its 16-byte UUID.
    ///
    /// The mapping is populated from metadata v10+ responses. Returns `None`
    /// if the topic is unknown or the broker did not return a topic ID — the
    /// caller should trigger a metadata refresh and retry.
    pub fn topic_id_for_name(&self, name: &str) -> Option<[u8; 16]> {
        self.cache.load().name_to_topic_id.get(name).copied()
    }

    /// Get all topics, deep-cloning every entry.
    ///
    /// Prefer [`topics_arc`](Self::topics_arc), which avoids copying every
    /// topic's partition map.
    pub fn topics(&self) -> Vec<TopicInfo> {
        self.cache
            .load()
            .topics
            .values()
            .map(|t| t.as_ref().clone())
            .collect()
    }

    /// Get all topics without copying their partition maps.
    pub fn topics_arc(&self) -> Vec<Arc<TopicInfo>> {
        self.cache.load().topics.values().map(Arc::clone).collect()
    }

    /// Get the leader for a topic partition.
    pub fn leader(&self, topic: &str, partition: PartitionId) -> Option<BrokerId> {
        self.cache
            .load()
            .topics
            .get(topic)
            .and_then(|t| t.leader(partition))
    }

    /// Get the leader epoch for a topic partition.
    ///
    /// The leader epoch is used for fencing stale reads after leadership changes.
    /// Returns None if the topic/partition is not found in metadata.
    pub fn leader_epoch(&self, topic: &str, partition: PartitionId) -> Option<i32> {
        self.cache
            .load()
            .topics
            .get(topic)
            .and_then(|t| t.leader_epoch(partition))
    }

    /// Apply a leader reported by a broker in a Fetch/Produce response (KIP-951).
    ///
    /// When leadership moves, the broker that rejected the request with
    /// `NOT_LEADER_OR_FOLLOWER` / `FENCED_LEADER_EPOCH` also names the node that
    /// should have received it, and advertises that node's endpoint. Folding
    /// that report straight into the cache lets the very next attempt go to the
    /// right broker; without it every failover costs a full metadata round trip
    /// on top of the failed request.
    ///
    /// The hint lands in the shared cache rather than in per-client state so
    /// that a leader learned by one code path (a consumer fetch, say) is also
    /// used by every other user of the same [`ClusterMetadata`].
    ///
    /// # Epoch rule
    ///
    /// The hint is ignored unless `leader_epoch` is strictly newer than the
    /// cached epoch, mirroring the KIP-320 fencing already applied on the
    /// metadata merge path: a lagging broker must never be able to drag the
    /// cache back to a previous leader. A cached epoch of `-1` means "unknown"
    /// (Metadata < v7, or a partition in an error state) and is always
    /// superseded. A hint whose own epoch is `-1` carries no ordering
    /// information and is never applied to the partition.
    ///
    /// # Reachability
    ///
    /// `endpoint` is the address the broker advertised for `leader_id`. It is
    /// registered in the broker map, so a node the cache has never seen becomes
    /// routable immediately. When `endpoint` is `None` and `leader_id` is also
    /// absent from the broker map the hint is unusable — pointing the partition
    /// at a broker with no address would only turn a retriable error into a
    /// routing failure — so it is dropped and `false` is returned.
    ///
    /// This never stamps the topic as freshly refreshed: the report covers one
    /// partition, and suppressing the periodic refresh on the strength of it
    /// would leave the rest of the topic's leader map to rot.
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

        let mut changed = false;
        self.cache.rcu(|current| {
            // `rcu` may run this closure more than once under contention, so
            // every iteration has to start from the current snapshot's verdict.
            changed = false;

            // Registering the endpoint is worthwhile on its own: it makes the
            // node dialable even when the partition update below is skipped.
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
                return Arc::clone(current);
            }
            changed = true;

            let mut next = MetadataCache::clone(current);

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
                    // The broker just named a live leader for this partition,
                    // so a stale per-partition error must not keep
                    // `is_routable()` false and strand it until the next
                    // refresh.
                    p.error_code = ErrorCode::None;
                }
                next.topics.insert(topic.to_string(), Arc::new(updated));
            }

            Arc::new(next)
        });

        changed
    }

    /// Get a connection to the leader of a partition.
    ///
    /// Refreshes first when *this topic's* entry is missing or older than
    /// `metadata.max.age.ms`. Per-topic age is what matters: the cache-wide
    /// `last_updated` stamp advances on every partial refresh, including one
    /// for an unrelated topic, so a client that keeps refreshing topic A would
    /// otherwise route topic B from an arbitrarily old leader map.
    pub async fn get_leader_connection(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<Arc<BrokerConnection>> {
        // Resolve the leader address, refreshing at most once if this topic's
        // entry is missing or stale. Everything needed for the dial is copied
        // out so no `ArcSwap` guard is held across an `.await`.
        let resolve = |cache: &MetadataCache| -> Option<(BrokerId, String)> {
            let leader_id = cache.topics.get(topic).and_then(|t| t.leader(partition))?;
            let address = cache.brokers.get(&leader_id)?.address().to_string();
            Some((leader_id, address))
        };

        let resolved = {
            let cache = self.cache.load();
            if cache.topic_is_fresh(topic, self.max_age) {
                resolve(&cache)
            } else {
                None
            }
        };

        let (leader_id, address) = match resolved {
            Some(found) => found,
            None => {
                self.refresh_for_topics(Some(&[topic])).await?;
                let cache = self.cache.load();
                // Distinguish the two failure modes so the error names the
                // actual problem: an unroutable partition versus a leader that
                // is not in the broker set.
                let leader_id = cache
                    .topics
                    .get(topic)
                    .and_then(|t| t.leader(partition))
                    .ok_or_else(|| {
                        KrafkaError::invalid_state(format!("no leader for {topic}-{partition}"))
                    })?;
                let address = cache
                    .brokers
                    .get(&leader_id)
                    .ok_or_else(|| {
                        KrafkaError::invalid_state(format!("broker {leader_id} not found"))
                    })?
                    .address()
                    .to_string();
                (leader_id, address)
            }
        };

        self.pool.get_connection_by_id(leader_id, &address).await
    }

    /// Get a connection to a specific broker by ID.
    pub async fn get_broker_connection(
        &self,
        broker_id: BrokerId,
    ) -> Result<Arc<BrokerConnection>> {
        // Copy the address out before awaiting: holding an `ArcSwap` guard
        // across the dial keeps the reader slot occupied for the duration of a
        // TCP/TLS handshake and forces concurrent cache writers onto the
        // fallback lock path.
        let address = {
            let cache = self.cache.load();
            cache
                .brokers
                .get(&broker_id)
                .ok_or_else(|| KrafkaError::invalid_state(format!("broker {broker_id} not found")))?
                .address()
                .to_string()
        };

        self.pool.get_connection_by_id(broker_id, &address).await
    }

    /// Get the controller broker.
    ///
    /// Returns `None` when the cluster has not reported a controller yet, when
    /// the controller ID is negative (no controller elected — normal briefly
    /// during failover), or when the reported ID is not among the known brokers.
    ///
    /// Controller-only APIs (CreateTopics, DeleteTopics, CreatePartitions,
    /// IncrementalAlterConfigs, CreateAcls/DeleteAcls, AlterClientQuotas,
    /// AlterUserScramCredentials, CreateDelegationToken, ElectLeaders,
    /// AlterPartitionReassignments, UpdateFeatures) must be routed here. A
    /// non-controller broker forwards them, but during a controller failover
    /// the forwarding broker answers `NOT_CONTROLLER` (41) instead, which
    /// surfaces only as a per-item error string.
    pub fn controller(&self) -> Option<BrokerInfo> {
        let cache = self.cache.load();
        if cache.controller_id < 0 {
            return None;
        }
        cache.brokers.get(&cache.controller_id).cloned()
    }

    /// Get a connection to the cluster controller.
    ///
    /// Refreshes metadata once if the controller is currently unknown, so that
    /// a caller retrying after `NOT_CONTROLLER` picks up the newly elected
    /// controller.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorCode::UnknownControllerId`] if no controller can be
    /// resolved even after a refresh.
    pub async fn get_controller_connection(&self) -> Result<Arc<BrokerConnection>> {
        let controller = match self.controller() {
            Some(c) => c,
            None => {
                debug!("controller unknown; refreshing metadata to resolve it");
                self.refresh().await?;
                self.controller().ok_or_else(|| {
                    KrafkaError::broker(
                        ErrorCode::UnknownControllerId,
                        "cluster reported no active controller",
                    )
                })?
            }
        };

        self.pool
            .get_connection_by_id(controller.id(), controller.address())
            .await
    }

    /// Get the cluster ID.
    pub fn cluster_id(&self) -> Option<String> {
        self.cache.load().cluster_id.clone()
    }

    /// Check if metadata needs refresh.
    pub fn needs_refresh(&self) -> bool {
        self.cache.load().is_stale(self.max_age)
    }

    /// Get partition count for a topic.
    pub fn partition_count(&self, topic: &str) -> Option<usize> {
        self.cache
            .load()
            .topics
            .get(topic)
            .map(|t| t.partition_count())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_info_address() {
        let broker = BrokerInfo::new(1, "localhost".to_string(), 9092, None);
        assert_eq!(broker.address(), "localhost:9092");
    }

    #[test]
    fn test_topic_info() {
        let topic = TopicInfo {
            name: "test".to_string(),
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
            MetadataRecoveryStrategy::None,
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
            meta.recovery_strategy,
            MetadataRecoveryStrategy::Rebootstrap
        );
        assert_eq!(meta.rebootstrap_trigger, Duration::from_secs(60));
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
    fn test_needs_rebootstrap_disabled_by_default() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        );

        // Default strategy is None — should never trigger rebootstrap.
        assert!(!meta.needs_rebootstrap());
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
        assert!(!meta.needs_rebootstrap());

        // Simulate that a refresh attempt has started.
        {
            let mut start = meta.metadata_attempt_start.lock();
            *start = Some(Instant::now());
        }

        // Still shouldn't trigger — trigger is 300s, elapsed is ~0.
        assert!(!meta.needs_rebootstrap());
        // Timestamp should still be recorded.
        assert!(meta.metadata_attempt_start.lock().is_some());
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
            let mut start = meta.metadata_attempt_start.lock();
            *start = Some(Instant::now());
        }

        // With a zero trigger, needs_rebootstrap should return true.
        assert!(meta.needs_rebootstrap());

        // Perform the actual rebootstrap.
        meta.rebootstrap().await;

        // After rebootstrap, the attempt start should be set to Some(now) — not None.
        assert!(meta.metadata_attempt_start.lock().is_some());
        // Cache should be reset.
        assert!(meta.cache.load().brokers.is_empty());
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
        meta.cache.store(Arc::new(cache));
        assert!(!meta.cache.load().brokers.is_empty());

        meta.rebootstrap().await;

        assert!(meta.cache.load().brokers.is_empty());
        // After rebootstrap, timer is set to Some(now) — not cleared.
        assert!(meta.metadata_attempt_start.lock().is_some());
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
        assert_eq!(meta.topic_cache_ttl, Some(Duration::from_secs(300)));
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
        assert_eq!(meta.topic_cache_ttl, None);
    }

    /// Regression test: a partial refresh must not reset `topic_last_refreshed`
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
            .cache
            .load()
            .topic_last_refreshed
            .get("topic-a")
            .copied()
            .unwrap();

        // Second partial update: only "topic-b" is in the response.
        // "topic-a" is retained from the cache but must keep its original timestamp.
        meta.update_cache(make_response(&["topic-b"]), false);
        let cache = meta.cache.load();

        assert!(
            cache.topics.contains_key("topic-a"),
            "topic-a should still be in the cache (TTL not yet expired)"
        );
        assert!(
            cache.topics.contains_key("topic-b"),
            "topic-b should appear after the second update"
        );

        let ts_a_after = cache.topic_last_refreshed.get("topic-a").copied().unwrap();
        assert_eq!(
            ts_a, ts_a_after,
            "retained topic-a's timestamp must not be advanced by a partial refresh"
        );
        assert!(
            cache.topic_last_refreshed.contains_key("topic-b"),
            "freshly refreshed topic-b must have a timestamp"
        );
    }

    /// Regression test: a partial refresh where a topic comes back with a
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
            .cache
            .load()
            .topic_last_refreshed
            .get("topic-a")
            .copied()
            .unwrap();

        // A subsequent partial refresh returns a transient error for "topic-a".
        // The stale entry must be preserved AND the timestamp must advance.
        meta.update_cache(make_transient_error_response("topic-a"), false);
        let cache = meta.cache.load();

        assert!(
            cache.topics.contains_key("topic-a"),
            "topic-a must be retained when the response has a transient error"
        );
        let ts_after = cache.topic_last_refreshed.get("topic-a").copied().unwrap();
        assert!(
            ts_after >= ts_before,
            "transient-error response must advance the TTL timestamp so the topic \
             is not evicted on the next refresh"
        );
    }

    /// Regression test: if a topic has already been TTL-evicted before the
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
            meta.cache.load().topics.contains_key("topic-a"),
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
            meta.cache.load().topics.contains_key("topic-a"),
            "topic-a must be restored from old cache after TTL eviction + transient error"
        );
    }

    /// Regression test: a brand-new topic that appears in a partial refresh
    /// only with a transient error (and has no prior cache entry) must NOT
    /// create an orphaned entry in `topic_last_refreshed` with no corresponding
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

        let cache = meta.cache.load();
        assert!(
            !cache.topics.contains_key("unknown-topic"),
            "unknown-topic must not appear in topics when only a transient error was received \
             and there is no prior cache entry"
        );
        assert!(
            !cache.topic_last_refreshed.contains_key("unknown-topic"),
            "unknown-topic must not be stamped in topic_last_refreshed when it is not in topics"
        );
    }

    /// Regression test: when a TTL-evicted topic is restored via the
    /// transient-error path, its UUID mapping must also be restored so that
    /// `topic_id_for_name()` continues to return `Some(uuid)`.
    ///
    /// Without the fix, `topic_ids` / `name_to_topic_id` were pruned during
    /// TTL eviction and never repopulated in the transient-error branch,
    /// causing ShareConsumer fetch routing to break.
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
            meta.cache.load().name_to_topic_id.contains_key("topic-b"),
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

        let cache = meta.cache.load();
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
    // Refresh coalescing must respect the requested topic set
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn test_in_flight_full_refresh_covers_everything() {
        let all = InFlightTopics::All;
        assert!(
            all.covers(None),
            "a full refresh covers another full refresh"
        );
        assert!(all.covers(Some(&["a"])));
        assert!(all.covers(Some(&["a", "b", "c"])));
    }

    #[test]
    fn test_in_flight_partial_never_covers_full_refresh() {
        let partial = InFlightTopics::Some(vec!["a".into(), "b".into()]);
        assert!(
            !partial.covers(None),
            "a partial refresh cannot satisfy a caller asking for all topics"
        );
    }

    /// A caller asking for ["b"] must NOT be allowed to
    /// join an in-flight refresh for ["a"]. Joining it hands the caller an
    /// Ok(()) for a topic the broker was never asked about, after which
    /// `get_leader_connection` fails with "no leader for b-0" despite having
    /// just "refreshed".
    #[test]
    fn test_in_flight_partial_only_covers_subsets() {
        let in_flight = InFlightTopics::Some(vec!["a".into()]);

        assert!(in_flight.covers(Some(&["a"])), "exact match must join");
        assert!(
            !in_flight.covers(Some(&["b"])),
            "disjoint topic set must NOT join an unrelated refresh"
        );
        assert!(
            !in_flight.covers(Some(&["a", "b"])),
            "a superset must NOT join: 'b' would never be fetched"
        );

        let wider = InFlightTopics::Some(vec!["a".into(), "b".into(), "c".into()]);
        assert!(wider.covers(Some(&["a", "c"])), "a subset may join");
        assert!(!wider.covers(Some(&["a", "d"])));
    }

    #[test]
    fn test_in_flight_empty_request_is_covered() {
        let in_flight = InFlightTopics::Some(vec!["a".into()]);
        assert!(in_flight.covers(Some(&[])));
    }

    #[test]
    fn test_in_flight_from_request() {
        assert!(matches!(
            InFlightTopics::from_request(None),
            InFlightTopics::All
        ));
        match InFlightTopics::from_request(Some(&["x", "y"])) {
            InFlightTopics::Some(v) => assert_eq!(v, vec!["x".to_string(), "y".to_string()]),
            InFlightTopics::All => panic!("expected a partial refresh"),
        }
    }

    // ══════════════════════════════════════════════════════════════════
    // A rate-limited refresh must be distinguishable from a real one
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn test_refresh_outcome_rate_limited_is_not_current() {
        let limited = RefreshOutcome::RateLimited(Duration::from_millis(100));
        assert!(
            !limited.is_current(),
            "a rate-limited refresh did not contact a broker and must not read as current"
        );
        assert_eq!(limited.retry_after(), Some(Duration::from_millis(100)));

        assert!(RefreshOutcome::Refreshed.is_current());
        assert_eq!(RefreshOutcome::Refreshed.retry_after(), None);
        assert!(RefreshOutcome::AlreadyFresh.is_current());
        assert_eq!(RefreshOutcome::AlreadyFresh.retry_after(), None);
    }

    /// A refresh within `retry_backoff` of the previous one must report
    /// `RateLimited` — not `Ok(())`. Returning success there is what made the
    /// admin retry loops re-issue against byte-identical stale metadata.
    #[tokio::test]
    async fn test_refresh_reports_rate_limited_instead_of_false_success() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:1".to_string()],
            pool,
            Duration::from_secs(300),
        )
        .with_retry_backoff(Duration::from_secs(60))
        .with_retry_backoff_max(Duration::from_secs(60));

        // Pretend a refresh just completed successfully, arming the backoff.
        meta.refresh_backoff
            .lock()
            .record_success(meta.retry_backoff.as_ref().unwrap());

        let outcome = meta
            .refresh_for_topics_inner_forced(Some(&["some-topic"]), false)
            .await
            .expect("rate limiting is not an error");

        match outcome {
            RefreshOutcome::RateLimited(remaining) => {
                assert!(remaining <= Duration::from_secs(72));
                assert!(!outcome.is_current());
            }
            other => panic!("expected RateLimited, got {other:?}"),
        }
    }

    /// The freshness check runs before the rate limiter: data already in cache
    /// is returned without waiting out a backoff it does not need.
    #[tokio::test]
    async fn test_already_fresh_wins_over_rate_limiting() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:1".to_string()],
            pool,
            Duration::from_secs(300),
        )
        .with_retry_backoff(Duration::from_secs(60));

        let mut cache = MetadataCache::new();
        cache
            .brokers
            .insert(1, BrokerInfo::new(1, "h".into(), 9092, None));
        cache.topics.insert(
            "t".into(),
            Arc::new(TopicInfo {
                name: "t".into(),
                is_internal: false,
                partitions: AHashMap::new(),
            }),
        );
        cache
            .topic_last_refreshed
            .insert("t".into(), Instant::now());
        meta.cache.store(Arc::new(cache));
        meta.refresh_backoff
            .lock()
            .record_success(meta.retry_backoff.as_ref().unwrap());

        let outcome = meta
            .refresh_for_topics_inner_forced(Some(&["t"]), false)
            .await
            .unwrap();
        assert_eq!(outcome, RefreshOutcome::AlreadyFresh);
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
        meta.cache.store(Arc::new(cache));

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

    #[test]
    fn test_request_timeout_default_and_override() {
        let meta = test_metadata();
        assert_eq!(
            meta.request_timeout,
            Duration::from_secs(30),
            "subscriber waits must be bounded by request timeout, not the 300s max-age"
        );

        let meta = test_metadata().with_request_timeout(Duration::from_secs(5));
        assert_eq!(meta.request_timeout, Duration::from_secs(5));
    }

    // ══════════════════════════════════════════════════════════════════
    // Exponential metadata retry backoff with jitter (KIP-580)
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn test_default_retry_backoff_is_exponential_and_capped() {
        let meta = test_metadata();
        let policy = meta.retry_backoff.as_ref().expect("enabled by default");

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
        let policy = meta.retry_backoff.clone().unwrap();
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
        let policy = meta.retry_backoff.clone().unwrap();

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
        let policy = meta.retry_backoff.clone().unwrap();
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
        let policy = meta.retry_backoff.as_ref().unwrap();
        assert_eq!(policy.initial_backoff, Duration::from_millis(250));
        assert_eq!(
            policy.max_backoff, DEFAULT_RETRY_BACKOFF_MAX,
            "a base below the default ceiling leaves the ceiling alone"
        );

        // A base above the ceiling raises the ceiling rather than inverting it.
        let meta = test_metadata().with_retry_backoff(Duration::from_secs(5));
        let policy = meta.retry_backoff.as_ref().unwrap();
        assert_eq!(policy.initial_backoff, Duration::from_secs(5));
        assert_eq!(policy.max_backoff, Duration::from_secs(5));
    }

    #[test]
    fn test_with_retry_backoff_max_never_inverts_the_curve() {
        let meta = test_metadata()
            .with_retry_backoff(Duration::from_millis(500))
            .with_retry_backoff_max(Duration::from_millis(10));
        let policy = meta.retry_backoff.as_ref().unwrap();
        assert_eq!(policy.max_backoff, Duration::from_millis(500));
    }

    #[test]
    fn test_with_retry_backoff_none_disables_rate_limiting() {
        let meta = test_metadata().with_retry_backoff(None);
        assert!(meta.retry_backoff.is_none());
        // A max on a disabled limiter is a no-op rather than a re-enable.
        let meta = meta.with_retry_backoff_max(Duration::from_secs(1));
        assert!(meta.retry_backoff.is_none());
    }

    /// With rate limiting disabled, a rate-limited outcome is impossible even
    /// immediately after another attempt.
    #[tokio::test]
    async fn test_disabled_backoff_never_rate_limits() {
        let meta = ClusterMetadata::new(
            vec!["localhost:1".to_string()],
            Arc::new(ConnectionPool::new(
                crate::network::ConnectionConfig::default(),
            )),
            Duration::from_secs(300),
        )
        .with_retry_backoff(None);

        // No broker is listening on port 1, so this fails — but it must fail
        // with a connection error rather than being suppressed.
        let outcome = meta
            .refresh_for_topics_inner_forced(Some(&["t"]), false)
            .await;
        assert!(outcome.is_err(), "expected a connection failure");
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

        assert!(
            meta.refresh_for_topics_inner_forced(Some(&["t"]), false)
                .await
                .is_err()
        );

        {
            let state = meta.refresh_backoff.lock();
            assert_eq!(
                state.consecutive_failures, 1,
                "a connection failure is a refresh failure and must count"
            );
            assert!(state.remaining().is_some(), "the limiter must now be armed");
        }

        // The immediately following attempt is suppressed rather than
        // re-dialling the dead broker.
        let outcome = meta
            .refresh_for_topics_inner_forced(Some(&["t"]), false)
            .await
            .expect("rate limiting is not an error");
        assert!(matches!(outcome, RefreshOutcome::RateLimited(_)));

        // ...and the failure count did not advance: no attempt was made.
        assert_eq!(meta.refresh_backoff.lock().consecutive_failures, 1);
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
            assert!(
                meta.refresh_for_topics_inner_forced(None, false)
                    .await
                    .is_err()
            );
            delays.push(meta.refresh_backoff.lock().current_delay);
            // Wait out the backoff so the next call is a real attempt.
            let remaining = meta.refresh_backoff.lock().remaining();
            if let Some(r) = remaining {
                tokio::time::sleep(r).await;
            }
        }

        assert_eq!(meta.refresh_backoff.lock().consecutive_failures, 4);
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
            meta.rebootstrap_jitter,
            Duration::from_millis(500),
            "a restarted fleet must not converge on one seed broker"
        );

        let meta = test_metadata().with_rebootstrap_jitter(Duration::ZERO);
        assert_eq!(meta.rebootstrap_jitter, Duration::ZERO);
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
        *meta.metadata_attempt_start.lock() = Some(Instant::now() - Duration::from_secs(600));
        assert!(meta.needs_rebootstrap());

        meta.rebootstrap().await;

        // Immediately afterwards the cluster is still down — but the trigger
        // must not be satisfied again until another 300 s of failure.
        assert!(
            !meta.needs_rebootstrap(),
            "back-to-back rebootstraps would turn a cluster outage into a \
             connection-churn storm against the seed brokers"
        );
        assert!(meta.metadata_attempt_start.lock().is_some());
    }

    /// The jittered deadline must never fire *before* the configured trigger.
    #[test]
    fn test_rebootstrap_trigger_jitter_only_delays() {
        let meta = test_metadata()
            .with_recovery_strategy(MetadataRecoveryStrategy::Rebootstrap)
            .with_rebootstrap_trigger(Duration::from_secs(10));

        // Just under the trigger: must never fire, however the jitter lands.
        *meta.metadata_attempt_start.lock() = Some(Instant::now() - Duration::from_secs(9));
        for _ in 0..64 {
            assert!(!meta.needs_rebootstrap());
        }

        // Comfortably past trigger + max jitter (10 s + 20%): always fires.
        *meta.metadata_attempt_start.lock() = Some(Instant::now() - Duration::from_secs(30));
        for _ in 0..64 {
            assert!(meta.needs_rebootstrap());
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
        meta.cache.store(Arc::new(cache));

        let before = meta.connection_candidates();
        assert!(before.iter().any(|a| a == "old-broker-1:9092"));
        assert!(before.iter().any(|a| a == "seed.example.com:9092"));

        meta.rebootstrap().await;

        let after = meta.connection_candidates();
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
            meta.connection_candidates()
                .contains(&"localhost:9092".to_string())
        );

        meta.update_seed_brokers(vec!["new-seed:9092".to_string()])
            .unwrap();
        assert_eq!(meta.connection_candidates(), vec!["new-seed:9092"]);
    }

    #[test]
    fn test_connection_candidates_deduplicates_seeds_already_known_as_brokers() {
        let meta = test_metadata();
        let mut cache = MetadataCache::new();
        // Broker 1 advertises exactly the seed address.
        cache
            .brokers
            .insert(1, BrokerInfo::new(1, "localhost".into(), 9092, None));
        meta.cache.store(Arc::new(cache));

        assert_eq!(
            meta.connection_candidates(),
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
                is_internal: false,
                partitions: AHashMap::new(),
            }),
        );
        cache.topics.insert(
            "fresh".into(),
            Arc::new(TopicInfo {
                name: "fresh".into(),
                is_internal: false,
                partitions: AHashMap::new(),
            }),
        );
        cache
            .topic_last_refreshed
            .insert("stale".into(), Instant::now() - Duration::from_secs(600));
        cache
            .topic_last_refreshed
            .insert("fresh".into(), Instant::now());
        // The cache as a whole was just written by the "fresh" refresh.
        cache.last_updated = Instant::now();
        meta.cache.store(Arc::new(cache));

        let cache = meta.cache.load();
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
        // A topic present in `topics` but with no `topic_last_refreshed` entry
        // has unknown age and must be treated as stale rather than trusted.
        let mut cache = MetadataCache::new();
        cache.topics.insert(
            "t".into(),
            Arc::new(TopicInfo {
                name: "t".into(),
                is_internal: false,
                partitions: AHashMap::new(),
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
        let mut cache = MetadataCache::clone(&meta.cache.load());
        cache
            .brokers
            .insert(2, BrokerInfo::new(2, "h2".into(), 9092, None));
        meta.cache.store(Arc::new(cache));
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
        let before = meta.cache.load().topic_last_refreshed["t"];

        assert!(meta.apply_leader_hint("t", 0, 2, 6, endpoint(2)));

        assert_eq!(meta.cache.load().topic_last_refreshed["t"], before);
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
}
