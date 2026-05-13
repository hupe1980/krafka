//! Consumer configuration.

use std::collections::HashMap;
use std::time::Duration;

use crate::auth::AuthConfig;
use crate::metadata::MetadataRecoveryStrategy;
use crate::{Offset, PartitionId};

/// Auto offset reset behavior.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AutoOffsetReset {
    /// Start from the earliest offset.
    Earliest,
    /// Start from the latest offset.
    #[default]
    Latest,
    /// Throw an error if no offset is found.
    None,
}

impl AutoOffsetReset {
    /// Convert to the protocol offset value.
    ///
    /// Returns `None` for `AutoOffsetReset::None` since that variant should
    /// produce an error rather than a valid offset.
    #[inline]
    pub fn to_offset(&self) -> Option<i64> {
        match self {
            AutoOffsetReset::Earliest => Some(-2),
            AutoOffsetReset::Latest => Some(-1),
            AutoOffsetReset::None => None,
        }
    }
}

/// Transaction isolation level.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IsolationLevel {
    /// Read all messages, including uncommitted transactions.
    #[default]
    ReadUncommitted,
    /// Only read committed transactions.
    ReadCommitted,
}

impl IsolationLevel {
    /// Convert to the protocol i8 value.
    #[inline]
    pub fn to_i8(self) -> i8 {
        match self {
            IsolationLevel::ReadUncommitted => 0,
            IsolationLevel::ReadCommitted => 1,
        }
    }
}

/// Group protocol used by the consumer group (KIP-848).
///
/// `Classic` uses the traditional JoinGroup/SyncGroup/Heartbeat flow
/// (API keys 11, 14, 12) where the group leader performs partition
/// assignment on the client side.
///
/// `Consumer` uses the new ConsumerGroupHeartbeat flow (API key 68)
/// introduced in KIP-848, where the server performs assignment and
/// members communicate exclusively via heartbeats.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GroupProtocol {
    /// Classic group protocol (JoinGroup/SyncGroup/Heartbeat).
    #[default]
    Classic,
    /// KIP-848 consumer group protocol (ConsumerGroupHeartbeat).
    Consumer,
}

/// Partition assignment strategy for consumer groups.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PartitionAssignmentStrategy {
    /// Range assignor (default) — assigns contiguous partition ranges per topic.
    #[default]
    Range,
    /// Round-robin assignor — distributes partitions evenly across consumers.
    RoundRobin,
    /// Cooperative sticky assignor — minimizes partition movements during rebalance.
    CooperativeSticky,
}

impl PartitionAssignmentStrategy {
    /// Get the Kafka protocol name for this strategy.
    #[inline]
    pub fn protocol_name(&self) -> &'static str {
        match self {
            Self::Range => "range",
            Self::RoundRobin => "roundrobin",
            Self::CooperativeSticky => "cooperative-sticky",
        }
    }
}

/// Consumer configuration.
///
/// Use [`ConsumerConfig::builder()`] or [`Default::default()`] to construct.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Bootstrap servers (comma-separated).
    pub(crate) bootstrap_servers: String,
    /// Consumer group ID.
    pub(crate) group_id: Option<String>,
    /// Client ID.
    pub(crate) client_id: String,
    /// Auto offset reset behavior.
    pub(crate) auto_offset_reset: AutoOffsetReset,
    /// Enable automatic offset commit.
    ///
    /// When `true` (the default), offsets are committed periodically in
    /// the background at the interval specified by
    /// [`auto_commit_interval`](Self::auto_commit_interval) (default: 5 s).
    /// Commits also occur during cooperative revocation and consumer close.
    ///
    /// **Important**: auto-commit commits the offset of the last record
    /// *returned* by [`poll()`](super::Consumer::poll), not the last record
    /// *processed* by the application. If the application crashes after
    /// `poll()` returns but before processing completes, some records may
    /// be skipped on restart. For at-least-once processing guarantees,
    /// disable auto-commit and call
    /// [`commit()`](super::Consumer::commit) after processing.
    pub(crate) enable_auto_commit: bool,
    /// Auto commit interval.
    pub(crate) auto_commit_interval: Duration,
    /// Minimum bytes to fetch.
    pub(crate) fetch_min_bytes: i32,
    /// Maximum bytes to fetch.
    pub(crate) fetch_max_bytes: i32,
    /// Maximum bytes per partition.
    pub(crate) max_partition_fetch_bytes: i32,
    /// Per-topic override for the per-partition fetch byte limit.
    ///
    /// When a topic is present in this map, its partitions use the specified
    /// limit instead of [`max_partition_fetch_bytes`](Self::max_partition_fetch_bytes).
    /// Useful for mixing high-throughput and low-throughput topics in one consumer.
    pub(crate) topic_fetch_max_bytes: HashMap<String, i32>,
    /// Maximum records returned by a single [`poll()`](super::Consumer::poll) call.
    ///
    /// `0` means unlimited (no truncation). Positive values cap the batch.
    /// Must be >= 0; negative values are rejected by the builder.
    pub(crate) max_poll_records: i32,
    /// Maximum records buffered internally by [`recv()`](super::Consumer::recv).
    ///
    /// When the internal buffer reaches this limit, [`poll()`](super::Consumer::poll)
    /// skips fetching new data until the buffer drains below the threshold.
    /// This prevents unbounded memory growth when the consumer reads faster
    /// than the application processes records.
    ///
    /// For `recv()`-only callers the buffer is naturally bounded by
    /// [`max_poll_records`](Self::max_poll_records) (one `poll()` batch);
    /// this cap adds an additional guard for mixed `poll()`/`recv()` usage
    /// and concurrent `recv()` callers.
    ///
    /// Set to 0 to disable the buffer cap (unlimited). Defaults to 500.
    /// Comparable to librdkafka's `queued.max.messages.kbytes` (count-based
    /// rather than size-based).
    pub(crate) max_buffered_records: i32,
    /// Maximum poll interval.
    pub(crate) max_poll_interval: Duration,
    /// Request timeout.
    pub(crate) request_timeout: Duration,
    /// Session timeout for consumer groups.
    pub(crate) session_timeout: Duration,
    /// Heartbeat interval.
    pub(crate) heartbeat_interval: Duration,
    /// Isolation level.
    pub(crate) isolation_level: IsolationLevel,
    /// Metadata max age.
    pub(crate) metadata_max_age: Duration,
    /// Partition assignment strategy.
    pub(crate) partition_assignment_strategy: PartitionAssignmentStrategy,
    /// Group protocol selection (KIP-848).
    pub(crate) group_protocol: GroupProtocol,
    /// Static group membership instance ID (KIP-345).
    ///
    /// When set, the consumer uses static membership. The broker will not
    /// trigger a rebalance when a static member leaves and rejoins within the
    /// session timeout, as long as it uses the same instance ID.
    pub(crate) group_instance_id: Option<String>,
    /// Client rack ID for closest-replica fetching (KIP-392).
    ///
    /// When set, the broker may direct fetches to a replica in the same rack,
    /// reducing cross-rack traffic. The value should match the `broker.rack`
    /// configuration on the brokers.
    pub(crate) client_rack: Option<String>,
    /// Metadata recovery strategy (KIP-899).
    ///
    /// When set to [`MetadataRecoveryStrategy::Rebootstrap`], the consumer
    /// falls back to bootstrap servers if metadata refresh fails for longer
    /// than [`metadata_recovery_rebootstrap_trigger`](Self::metadata_recovery_rebootstrap_trigger).
    pub(crate) metadata_recovery_strategy: MetadataRecoveryStrategy,
    /// Duration after which failing metadata refreshes trigger a rebootstrap
    /// (KIP-899). Only effective with
    /// [`MetadataRecoveryStrategy::Rebootstrap`]. Default: 300 s.
    pub(crate) metadata_recovery_rebootstrap_trigger: Duration,
    /// Maximum age of cached topic entries during partial metadata refreshes.
    /// Topics not refreshed within this duration are evicted to prevent
    /// unbounded cache growth. Defaults to 5 minutes, matching the Java
    /// client's `metadata.max.idle.ms`. `None` disables TTL eviction.
    pub(crate) metadata_topic_cache_ttl: Option<Duration>,
    /// Authentication configuration (optional).
    pub(crate) auth: Option<AuthConfig>,
    /// Maximum decompressed size for record batches (compression bomb protection).
    /// Defaults to [`RecordBatch::MAX_DECOMPRESSED_SIZE`](crate::protocol::RecordBatch::MAX_DECOMPRESSED_SIZE) (128 MiB).
    pub(crate) max_decompressed_size: usize,
    /// SOCKS5 proxy configuration (optional).
    #[cfg(feature = "socks5")]
    pub(crate) proxy: Option<crate::network::ProxyConfig>,
    /// Per-partition initial offsets applied before auto-offset-reset.
    ///
    /// When a partition is first assigned and has no committed group offset,
    /// the corresponding entry from this map is used as the starting fetch
    /// position, overriding `auto_offset_reset`.
    ///
    /// Keyed by `(topic, partition)`.  Build via
    /// [`ConsumerConfigBuilder::initial_offsets`].
    pub(crate) initial_offsets: HashMap<(String, PartitionId), Offset>,
    /// Maximum number of cooperative-rebalance rejoin rounds per poll cycle.
    ///
    /// Each round handles one additional wave of partition revocations from
    /// cascading membership changes. When the cap is reached without
    /// convergence, the remaining revocations are deferred to the next
    /// poll call and a heartbeat is sent to avoid session expiry.
    ///
    /// Increasing this value speeds up convergence in large, rapidly-changing
    /// groups at the cost of holding the poll lock longer. The default of 10
    /// is sufficient for all but the most extreme churning groups.
    pub(crate) max_cooperative_rebalance_rounds: usize,
    /// Duration after which a partition's cached high watermark is considered
    /// stale by [`Consumer::lag`].
    ///
    /// If a partition's watermark has not been refreshed within this window it
    /// appears in [`LagResult::stale_partitions`]. The lag value is still
    /// returned but may be inaccurate.
    ///
    /// Default: 60 s. Set to `Duration::MAX` to disable staleness reporting.
    pub(crate) lag_staleness_threshold: Duration,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            group_id: None,
            client_id: "krafka".to_string(),
            auto_offset_reset: AutoOffsetReset::Latest,
            enable_auto_commit: true,
            auto_commit_interval: Duration::from_secs(5),
            fetch_min_bytes: 1,
            fetch_max_bytes: 52428800,          // 50 MB
            max_partition_fetch_bytes: 1048576, // 1 MB
            topic_fetch_max_bytes: HashMap::new(),
            max_poll_records: 500,
            max_buffered_records: 500,
            max_poll_interval: Duration::from_secs(300),
            request_timeout: Duration::from_secs(30),
            session_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(3),
            isolation_level: IsolationLevel::ReadUncommitted,
            metadata_max_age: Duration::from_secs(300),
            partition_assignment_strategy: PartitionAssignmentStrategy::Range,
            group_protocol: GroupProtocol::Classic,
            group_instance_id: None,
            client_rack: None,
            metadata_recovery_strategy: MetadataRecoveryStrategy::Rebootstrap,
            metadata_recovery_rebootstrap_trigger: Duration::from_secs(300),
            metadata_topic_cache_ttl: Some(Duration::from_secs(300)),
            auth: None,
            max_decompressed_size: crate::protocol::RecordBatch::MAX_DECOMPRESSED_SIZE,
            #[cfg(feature = "socks5")]
            proxy: None,
            initial_offsets: HashMap::new(),
            max_cooperative_rebalance_rounds: 10,
            lag_staleness_threshold: Duration::from_secs(60),
        }
    }
}

impl ConsumerConfig {
    /// Create a new config builder.
    pub fn builder() -> ConsumerConfigBuilder {
        ConsumerConfigBuilder::default()
    }

    /// Returns the bootstrap servers.
    #[inline]
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Returns the consumer group ID, if set.
    #[inline]
    pub fn group_id(&self) -> Option<&str> {
        self.group_id.as_deref()
    }

    /// Returns the client ID.
    #[inline]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the auto offset reset behavior.
    #[inline]
    pub fn auto_offset_reset(&self) -> AutoOffsetReset {
        self.auto_offset_reset
    }

    /// Returns whether auto commit is enabled.
    #[inline]
    pub fn enable_auto_commit(&self) -> bool {
        self.enable_auto_commit
    }

    /// Returns the auto commit interval.
    #[inline]
    pub fn auto_commit_interval(&self) -> Duration {
        self.auto_commit_interval
    }

    /// Returns the minimum bytes to fetch.
    #[inline]
    pub fn fetch_min_bytes(&self) -> i32 {
        self.fetch_min_bytes
    }

    /// Returns the maximum bytes to fetch.
    #[inline]
    pub fn fetch_max_bytes(&self) -> i32 {
        self.fetch_max_bytes
    }

    /// Returns the maximum bytes per partition.
    #[inline]
    pub fn max_partition_fetch_bytes(&self) -> i32 {
        self.max_partition_fetch_bytes
    }

    /// Returns the maximum poll records.
    #[inline]
    pub fn max_poll_records(&self) -> i32 {
        self.max_poll_records
    }

    /// Returns the maximum buffered records.
    #[inline]
    pub fn max_buffered_records(&self) -> i32 {
        self.max_buffered_records
    }

    /// Returns the maximum poll interval.
    #[inline]
    pub fn max_poll_interval(&self) -> Duration {
        self.max_poll_interval
    }

    /// Returns the request timeout.
    #[inline]
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Returns the session timeout.
    #[inline]
    pub fn session_timeout(&self) -> Duration {
        self.session_timeout
    }

    /// Returns the heartbeat interval.
    #[inline]
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    /// Returns the isolation level.
    #[inline]
    pub fn isolation_level(&self) -> IsolationLevel {
        self.isolation_level
    }

    /// Returns the metadata max age.
    #[inline]
    pub fn metadata_max_age(&self) -> Duration {
        self.metadata_max_age
    }

    /// Returns the partition assignment strategy.
    #[inline]
    pub fn partition_assignment_strategy(&self) -> PartitionAssignmentStrategy {
        self.partition_assignment_strategy
    }

    /// Returns the group protocol (KIP-848).
    #[inline]
    pub fn group_protocol(&self) -> GroupProtocol {
        self.group_protocol
    }

    /// Returns the static group membership instance ID, if set.
    #[inline]
    pub fn group_instance_id(&self) -> Option<&str> {
        self.group_instance_id.as_deref()
    }

    /// Returns the client rack ID, if set.
    #[inline]
    pub fn client_rack(&self) -> Option<&str> {
        self.client_rack.as_deref()
    }

    /// Returns the metadata recovery strategy (KIP-899).
    #[inline]
    pub fn metadata_recovery_strategy(&self) -> MetadataRecoveryStrategy {
        self.metadata_recovery_strategy
    }

    /// Returns the rebootstrap trigger duration (KIP-899).
    #[inline]
    pub fn metadata_recovery_rebootstrap_trigger(&self) -> Duration {
        self.metadata_recovery_rebootstrap_trigger
    }

    /// Returns the authentication configuration, if set.
    #[inline]
    pub fn auth(&self) -> Option<&AuthConfig> {
        self.auth.as_ref()
    }

    /// Returns the maximum decompressed size for record batches.
    #[inline]
    pub fn max_decompressed_size(&self) -> usize {
        self.max_decompressed_size
    }

    /// Returns the SOCKS5 proxy configuration, if set.
    #[cfg(feature = "socks5")]
    #[inline]
    pub fn proxy(&self) -> Option<&crate::network::ProxyConfig> {
        self.proxy.as_ref()
    }
}

/// Builder for ConsumerConfig.
#[must_use = "builders do nothing until .build() is called"]
#[derive(Debug, Default)]
pub struct ConsumerConfigBuilder {
    config: ConsumerConfig,
}

impl ConsumerConfigBuilder {
    /// Set bootstrap servers.
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    /// Set group ID.
    pub fn group_id(mut self, id: impl Into<String>) -> Self {
        self.config.group_id = Some(id.into());
        self
    }

    /// Set client ID.
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.config.client_id = id.into();
        self
    }

    /// Set auto offset reset.
    pub fn auto_offset_reset(mut self, reset: AutoOffsetReset) -> Self {
        self.config.auto_offset_reset = reset;
        self
    }

    /// Enable automatic offset commit.
    ///
    /// See [`ConsumerConfig::enable_auto_commit`] for semantics and caveats.
    pub fn enable_auto_commit(mut self, enable: bool) -> Self {
        self.config.enable_auto_commit = enable;
        self
    }

    /// Set auto commit interval.
    pub fn auto_commit_interval(mut self, interval: Duration) -> Self {
        self.config.auto_commit_interval = interval;
        self
    }

    /// Set isolation level.
    pub fn isolation_level(mut self, level: IsolationLevel) -> Self {
        self.config.isolation_level = level;
        self
    }

    /// Set authentication configuration.
    ///
    /// Enables TLS and/or SASL authentication for all connections.
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = Some(auth);
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

    /// Set SOCKS5 proxy configuration.
    ///
    /// Routes all broker connections through the specified SOCKS5 proxy.
    #[cfg(feature = "socks5")]
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.config.proxy = Some(proxy);
        self
    }

    /// Set partition assignment strategy.
    pub fn partition_assignment_strategy(mut self, strategy: PartitionAssignmentStrategy) -> Self {
        self.config.partition_assignment_strategy = strategy;
        self
    }

    /// Set the group protocol (KIP-848).
    ///
    /// `Classic` uses the traditional JoinGroup/SyncGroup/Heartbeat flow.
    /// `Consumer` uses the new server-side assignment via ConsumerGroupHeartbeat.
    pub fn group_protocol(mut self, protocol: GroupProtocol) -> Self {
        self.config.group_protocol = protocol;
        self
    }

    /// Set the static group membership instance ID (KIP-345).
    ///
    /// When set, the consumer uses static membership. The broker preserves
    /// partition assignments across restarts as long as the same instance ID
    /// is used. This avoids unnecessary rebalances when consumers restart.
    pub fn group_instance_id(mut self, id: impl Into<String>) -> Self {
        self.config.group_instance_id = Some(id.into());
        self
    }

    /// Set minimum bytes to fetch per request.
    pub fn fetch_min_bytes(mut self, bytes: i32) -> Self {
        self.config.fetch_min_bytes = bytes;
        self
    }

    /// Set maximum bytes to fetch per request.
    pub fn fetch_max_bytes(mut self, bytes: i32) -> Self {
        self.config.fetch_max_bytes = bytes;
        self
    }

    /// Set maximum bytes per partition per fetch request.
    pub fn max_partition_fetch_bytes(mut self, bytes: i32) -> Self {
        self.config.max_partition_fetch_bytes = bytes;
        self
    }

    /// Override the per-partition fetch byte limit for a specific topic.
    ///
    /// When set, partitions of `topic` use `bytes` instead of
    /// [`max_partition_fetch_bytes`](ConsumerConfigBuilder::max_partition_fetch_bytes).
    /// Can be called multiple times to configure multiple topics.
    pub fn topic_fetch_max_bytes(mut self, topic: impl Into<String>, bytes: i32) -> Self {
        self.config
            .topic_fetch_max_bytes
            .insert(topic.into(), bytes);
        self
    }

    /// Set maximum records per [`poll()`](super::Consumer::poll) call.
    ///
    /// - `-1` means unlimited — no truncation.
    /// - Positive values cap each poll batch at that many records.
    /// - `0` and other negative values are rejected at build time.
    ///
    /// Default: 500.
    pub fn max_poll_records(mut self, max: i32) -> Self {
        self.config.max_poll_records = max;
        self
    }

    /// Set maximum records buffered internally by `recv()`.
    ///
    /// When the internal buffer reaches this limit, `poll()` skips fetching
    /// new data until the buffer drains below the threshold. For
    /// `recv()`-only callers the buffer is naturally bounded by
    /// `max_poll_records`; this cap guards against mixed `poll()`/`recv()`
    /// usage and concurrent `recv()` callers.  Set to 0 to disable
    /// (unlimited). Defaults to 500.
    pub fn max_buffered_records(mut self, max: i32) -> Self {
        self.config.max_buffered_records = max;
        self
    }

    /// Set maximum poll interval before the consumer is considered dead.
    pub fn max_poll_interval(mut self, interval: Duration) -> Self {
        self.config.max_poll_interval = interval;
        self
    }

    /// Set request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set session timeout for consumer group membership.
    pub fn session_timeout(mut self, timeout: Duration) -> Self {
        self.config.session_timeout = timeout;
        self
    }

    /// Set heartbeat interval.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    /// Set metadata max age before refresh.
    pub fn metadata_max_age(mut self, duration: Duration) -> Self {
        self.config.metadata_max_age = duration;
        self
    }

    /// Set the client rack ID for closest-replica fetching (KIP-392).
    pub fn client_rack(mut self, rack: impl Into<String>) -> Self {
        self.config.client_rack = Some(rack.into());
        self
    }

    /// Set the metadata recovery strategy (KIP-899).
    pub fn metadata_recovery_strategy(mut self, strategy: MetadataRecoveryStrategy) -> Self {
        self.config.metadata_recovery_strategy = strategy;
        self
    }

    /// Set the rebootstrap trigger duration (KIP-899).
    ///
    /// Only effective when [`MetadataRecoveryStrategy::Rebootstrap`] is set.
    pub fn metadata_recovery_rebootstrap_trigger(mut self, duration: Duration) -> Self {
        self.config.metadata_recovery_rebootstrap_trigger = duration;
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

    /// Set per-partition initial offsets applied before auto-offset-reset.
    ///
    /// When a partition is first assigned and has no committed group offset,
    /// the consumer will start fetching from the given offset instead of
    /// applying `auto_offset_reset`.  This is useful for exactly-once recovery
    /// when you know the exact position to resume from.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::collections::HashMap;
    ///
    /// ConsumerConfig::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .initial_offsets(HashMap::from([
    ///         (("my-topic".to_string(), 0), 1_000),
    ///         (("my-topic".to_string(), 1), 2_000),
    ///     ]))
    ///     .build()?;
    /// ```
    pub fn initial_offsets(mut self, offsets: HashMap<(String, PartitionId), Offset>) -> Self {
        self.config.initial_offsets = offsets;
        self
    }

    /// Set the maximum number of cooperative-rebalance rejoin rounds per poll.
    ///
    /// Default: 10. Values below 1 are clamped to 1.
    pub fn max_cooperative_rebalance_rounds(mut self, rounds: usize) -> Self {
        self.config.max_cooperative_rebalance_rounds = rounds.max(1);
        self
    }

    /// Set the staleness threshold for high-watermark freshness in `lag()`.
    ///
    /// Partitions whose cached watermark is older than this threshold appear in
    /// [`LagResult::stale_partitions`]. Pass `Duration::MAX` to disable
    /// staleness reporting.
    ///
    /// Default: 60 s.
    pub fn lag_staleness_threshold(mut self, threshold: Duration) -> Self {
        self.config.lag_staleness_threshold = threshold;
        self
    }

    /// Build the config.
    ///
    /// # Errors
    ///
    /// Returns an error if timing or value constraints are violated:
    /// - `heartbeat_interval` must be less than `session_timeout`
    /// - `bootstrap_servers` must be non-empty
    /// - `group_id`, when provided, must be non-empty
    /// - `session_timeout` must be less than or equal to `max_poll_interval`
    /// - `max_buffered_records` must be >= 0 (0 disables the cap)
    /// - `fetch_min_bytes` must be <= `fetch_max_bytes`
    pub fn build(self) -> crate::Result<ConsumerConfig> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(crate::error::KrafkaError::config(
                "bootstrap_servers must not be empty",
            ));
        }
        if self.config.group_id.as_deref() == Some("") {
            return Err(crate::error::KrafkaError::config(
                "group_id must not be an empty string; omit it entirely to disable group coordination",
            ));
        }
        if self.config.heartbeat_interval >= self.config.session_timeout {
            return Err(crate::error::KrafkaError::config(format!(
                "heartbeat_interval ({:?}) must be less than session_timeout ({:?})",
                self.config.heartbeat_interval, self.config.session_timeout,
            )));
        }
        if self.config.session_timeout > self.config.max_poll_interval {
            return Err(crate::error::KrafkaError::config(format!(
                "session_timeout ({:?}) must be <= max_poll_interval ({:?})",
                self.config.session_timeout, self.config.max_poll_interval,
            )));
        }
        if self.config.max_buffered_records < 0 {
            return Err(crate::error::KrafkaError::config(format!(
                "max_buffered_records ({}) must be >= 0",
                self.config.max_buffered_records,
            )));
        }
        if self.config.fetch_min_bytes > self.config.fetch_max_bytes {
            return Err(crate::error::KrafkaError::config(format!(
                "fetch_min_bytes ({}) must be <= fetch_max_bytes ({})",
                self.config.fetch_min_bytes, self.config.fetch_max_bytes,
            )));
        }
        if self.config.max_poll_records == 0 || self.config.max_poll_records < -1 {
            return Err(crate::error::KrafkaError::config(format!(
                "max_poll_records ({}) must be -1 (unlimited) or a positive integer",
                self.config.max_poll_records,
            )));
        }
        Ok(self.config)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_auto_offset_reset_to_offset() {
        assert_eq!(AutoOffsetReset::Earliest.to_offset(), Some(-2));
        assert_eq!(AutoOffsetReset::Latest.to_offset(), Some(-1));
        assert_eq!(AutoOffsetReset::None.to_offset(), None);
    }

    #[test]
    fn test_isolation_level_to_i8() {
        assert_eq!(IsolationLevel::ReadUncommitted.to_i8(), 0);
        assert_eq!(IsolationLevel::ReadCommitted.to_i8(), 1);
    }

    #[test]
    fn test_config_default() {
        let config = ConsumerConfig::default();
        assert_eq!(config.auto_offset_reset, AutoOffsetReset::Latest);
        assert!(config.enable_auto_commit);
        assert_eq!(config.fetch_min_bytes, 1);
        assert_eq!(
            config.partition_assignment_strategy,
            PartitionAssignmentStrategy::Range
        );
        assert_eq!(config.group_protocol, GroupProtocol::Classic);
    }

    #[test]
    fn test_config_builder() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .isolation_level(IsolationLevel::ReadCommitted)
            .partition_assignment_strategy(PartitionAssignmentStrategy::CooperativeSticky)
            .build()
            .unwrap();

        assert_eq!(config.bootstrap_servers, "localhost:9092");
        assert_eq!(config.group_id, Some("test-group".to_string()));
        assert_eq!(config.auto_offset_reset, AutoOffsetReset::Earliest);
        assert!(!config.enable_auto_commit);
        assert_eq!(config.isolation_level, IsolationLevel::ReadCommitted);
        assert_eq!(
            config.partition_assignment_strategy,
            PartitionAssignmentStrategy::CooperativeSticky
        );
    }

    #[test]
    fn test_partition_assignment_strategy_protocol_names() {
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
    fn test_config_builder_fetch_min_bytes() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .fetch_min_bytes(1024)
            .build()
            .unwrap();
        assert_eq!(
            config.fetch_min_bytes, 1024,
            "fetch_min_bytes should be set by builder"
        );
    }

    #[test]
    fn test_config_builder_fetch_max_bytes() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .fetch_max_bytes(10 * 1024 * 1024)
            .build()
            .unwrap();
        assert_eq!(
            config.fetch_max_bytes,
            10 * 1024 * 1024,
            "fetch_max_bytes should be set by builder"
        );
    }

    #[test]
    fn test_config_builder_metadata_max_age() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .metadata_max_age(Duration::from_secs(60))
            .build()
            .unwrap();
        assert_eq!(
            config.metadata_max_age,
            Duration::from_secs(60),
            "metadata_max_age should be set by builder"
        );
    }

    #[test]
    fn test_config_default_group_instance_id() {
        let config = ConsumerConfig::default();
        assert!(
            config.group_instance_id.is_none(),
            "group_instance_id should be None by default"
        );
    }

    #[test]
    fn test_config_builder_group_instance_id() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("my-group")
            .group_instance_id("instance-1")
            .build()
            .unwrap();
        assert_eq!(
            config.group_instance_id,
            Some("instance-1".to_string()),
            "group_instance_id should be set by builder"
        );
    }

    #[test]
    fn test_config_default_client_rack_is_none() {
        let config = ConsumerConfig::default();
        assert!(
            config.client_rack.is_none(),
            "client_rack should be None by default"
        );
    }

    #[test]
    fn test_config_builder_client_rack() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .client_rack("us-east-1a")
            .build()
            .unwrap();
        assert_eq!(
            config.client_rack,
            Some("us-east-1a".to_string()),
            "client_rack should be set by builder"
        );
    }

    #[test]
    fn test_config_default_group_protocol_is_classic() {
        let config = ConsumerConfig::default();
        assert_eq!(
            config.group_protocol(),
            GroupProtocol::Classic,
            "group_protocol should default to Classic"
        );
    }

    #[test]
    fn test_config_builder_group_protocol_consumer() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .group_protocol(GroupProtocol::Consumer)
            .build()
            .unwrap();
        assert_eq!(
            config.group_protocol(),
            GroupProtocol::Consumer,
            "group_protocol should be Consumer when set"
        );
    }

    #[cfg(feature = "socks5")]
    #[test]
    fn test_config_builder_proxy_round_trip() {
        let config = ConsumerConfig::builder()
            .proxy(crate::network::ProxyConfig::new("proxy:1080"))
            .build()
            .unwrap();
        let proxy = config.proxy().expect("proxy should be set");
        assert_eq!(proxy.address(), "proxy:1080");
    }

    #[test]
    fn test_config_default_recovery_strategy() {
        let config = ConsumerConfig::default();
        assert_eq!(
            config.metadata_recovery_strategy,
            MetadataRecoveryStrategy::Rebootstrap,
        );
        assert_eq!(
            config.metadata_recovery_rebootstrap_trigger,
            Duration::from_secs(300),
        );
    }

    #[test]
    fn test_config_builder_recovery_strategy() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .metadata_recovery_strategy(MetadataRecoveryStrategy::Rebootstrap)
            .metadata_recovery_rebootstrap_trigger(Duration::from_secs(60))
            .build()
            .unwrap();
        assert_eq!(
            config.metadata_recovery_strategy(),
            MetadataRecoveryStrategy::Rebootstrap,
        );
        assert_eq!(
            config.metadata_recovery_rebootstrap_trigger(),
            Duration::from_secs(60),
        );
    }

    #[test]
    fn test_config_builder_rejects_negative_max_buffered_records() {
        let result = ConsumerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .max_buffered_records(-1)
            .build();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("max_buffered_records"),
            "error message should mention max_buffered_records"
        );
    }

    #[test]
    fn test_config_builder_accepts_zero_max_buffered_records() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .max_buffered_records(0)
            .build()
            .unwrap();
        assert_eq!(config.max_buffered_records(), 0);
    }

    #[test]
    fn test_config_builder_accepts_minus_one_max_poll_records_as_unlimited() {
        // -1 means unlimited — poll() must not truncate any records.
        let config = ConsumerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .max_poll_records(-1)
            .build()
            .unwrap();
        assert_eq!(
            config.max_poll_records(),
            -1,
            "max_poll_records=-1 should be accepted as unlimited"
        );
    }

    #[test]
    fn test_config_builder_rejects_zero_max_poll_records() {
        let result = ConsumerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .max_poll_records(0)
            .build();
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("max_poll_records"),
            "error message should mention max_poll_records"
        );
    }

    #[test]
    fn test_config_builder_rejects_negative_max_poll_records() {
        let result = ConsumerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .max_poll_records(-2)
            .build();
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("max_poll_records"),
            "error message should mention max_poll_records"
        );
    }

    #[test]
    fn test_config_builder_rejects_empty_bootstrap_servers() {
        let result = ConsumerConfig::builder().bootstrap_servers("").build();
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("bootstrap_servers"),
            "error message should mention bootstrap_servers"
        );
    }

    #[test]
    fn test_config_builder_rejects_empty_group_id() {
        let result = ConsumerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("")
            .build();
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("group_id"),
            "error message should mention group_id"
        );
    }
}
