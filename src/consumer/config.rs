//! Consumer configuration.

use std::time::Duration;

use ahash::AHashMap as HashMap;

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
///
/// # Stability
///
/// `GroupProtocol::Consumer` (KIP-848) requires **Kafka 3.7 or later** with
/// the new consumer group protocol enabled on the broker
/// (`group.coordinator.new.enable=true`). The implementation is functional
/// but not yet validated against the full KIP-848 specification in all
/// edge cases (incremental rebalance, epoch fencing, mixed-version clusters).
///
/// For production workloads, use `GroupProtocol::Classic` (the default)
/// unless you are specifically targeting Kafka 3.7+ and have validated
/// the KIP-848 behaviour for your workload.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GroupProtocol {
    /// Classic group protocol (JoinGroup/SyncGroup/Heartbeat).
    ///
    /// Supported on all Kafka broker versions. This is the default and
    /// recommended choice for production workloads.
    #[default]
    Classic,
    /// KIP-848 consumer group protocol (ConsumerGroupHeartbeat).
    ///
    /// **Requires Kafka 3.7+ with `group.coordinator.new.enable=true`.**
    /// Not yet fully validated for all rebalance edge cases. Prefer
    /// `Classic` for production use until this note is removed.
    Consumer,
}

/// Partition assignment strategy for consumer groups.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PartitionAssignmentStrategy {
    /// Range assignor (default) — assigns contiguous partition ranges per topic.
    ///
    /// Eager protocol: every member revokes its entire assignment before the
    /// new one is computed.
    #[default]
    Range,
    /// Round-robin assignor — distributes partitions evenly across consumers.
    ///
    /// Eager protocol.
    RoundRobin,
    /// Sticky assignor — balanced assignment that preserves as much of the
    /// previous assignment as possible.
    ///
    /// Eager protocol: like [`Range`](Self::Range) and
    /// [`RoundRobin`](Self::RoundRobin), all partitions are revoked before the
    /// new assignment is applied. The stickiness is in *which* partitions come
    /// back — a member that keeps its partitions across a rebalance avoids
    /// re-seeking and re-warming local state, even though it briefly gave them
    /// up.
    ///
    /// Prefer [`CooperativeSticky`](Self::CooperativeSticky) for new
    /// deployments: it gives the same stickiness *without* the stop-the-world
    /// revocation. This variant exists for parity with the Java client and for
    /// groups that cannot yet run the cooperative protocol.
    Sticky,
    /// Cooperative sticky assignor — minimizes partition movements during rebalance.
    ///
    /// Cooperative protocol: members only revoke the partitions that are
    /// actually being reassigned, so partitions that stay put are never
    /// interrupted.
    CooperativeSticky,
}

impl PartitionAssignmentStrategy {
    /// Get the Kafka protocol name for this strategy.
    ///
    /// This is the name sent in the JoinGroup request and matched against
    /// `JoinGroupResponse.protocol_name`, so it must stay byte-identical to
    /// the Java client's names — a mismatch makes the group unable to find a
    /// common protocol and the coordinator rejects the join.
    #[inline]
    pub fn protocol_name(&self) -> &'static str {
        match self {
            Self::Range => "range",
            Self::RoundRobin => "roundrobin",
            Self::Sticky => "sticky",
            Self::CooperativeSticky => "cooperative-sticky",
        }
    }

    /// Resolve a protocol name received from the coordinator back into a
    /// strategy.
    ///
    /// Returns `None` for names this client does not implement.
    #[inline]
    pub fn from_protocol_name(name: &str) -> Option<Self> {
        match name {
            "range" => Some(Self::Range),
            "roundrobin" => Some(Self::RoundRobin),
            "sticky" => Some(Self::Sticky),
            "cooperative-sticky" => Some(Self::CooperativeSticky),
            _ => None,
        }
    }

    /// Whether this strategy uses the cooperative (incremental) rebalance
    /// protocol rather than the eager stop-the-world one.
    #[inline]
    pub fn is_cooperative(&self) -> bool {
        matches!(self, Self::CooperativeSticky)
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
    /// Maximum time the **broker** will hold a fetch request open waiting for
    /// [`fetch_min_bytes`](Self::fetch_min_bytes) to accumulate.
    ///
    /// This is the wire-level `max_wait_ms` field and is deliberately
    /// independent of the timeout passed to [`poll()`](super::Consumer::poll).
    /// The two serve different purposes: this one bounds how long a *single*
    /// fetch request parks on the broker, while the `poll()` timeout bounds
    /// how long the *client* keeps trying. `poll()` issues fetches in a loop
    /// until its own deadline, so a long poll timeout still behaves as a long
    /// poll.
    ///
    /// Keeping them separate matters because the connection layer aborts any
    /// request that outlives `request_timeout`. Sending the caller's poll
    /// timeout as `max_wait_ms` would mean `poll(60s)` asks the broker to hold
    /// the request for 60 s while the client tears the request down at 30 s,
    /// turning an ordinary "no data available" poll into a timeout error.
    ///
    /// Effective value is `min(fetch_max_wait, remaining poll budget)`.
    ///
    /// Default: 500 ms, matching the Java client's `fetch.max.wait.ms`.
    pub(crate) fetch_max_wait: Duration,
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
    /// `-1` means unlimited (no truncation); any positive value caps the
    /// batch. `0` and values below `-1` are rejected by the builder — `0`
    /// would produce a consumer that fetches records and then truncates every
    /// batch to nothing, silently returning no data forever.
    ///
    /// Defaults to 500.
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
    /// Time allowed for TCP establishment to one broker.
    pub(crate) connect_timeout: Duration,
    /// Session timeout for consumer groups.
    ///
    /// How long the coordinator waits without a heartbeat before declaring
    /// this member dead and rebalancing the group.
    ///
    /// Default: 45 s, matching Java and librdkafka since Kafka 3.0. The older
    /// 10 s default was raised because it sat inside the range of an ordinary
    /// GC pause or scheduler stall, so healthy consumers were regularly
    /// evicted and the group churned through spurious rebalances. 10 s is also
    /// below the `group.min.session.timeout.ms` configured on many brokers,
    /// which rejects the JoinGroup outright.
    pub(crate) session_timeout: Duration,
    /// Heartbeat interval.
    pub(crate) heartbeat_interval: Duration,
    /// Isolation level.
    pub(crate) isolation_level: IsolationLevel,
    /// Metadata max age.
    pub(crate) metadata_max_age: Duration,
    /// Partition assignment strategies, in order of preference.
    ///
    /// All of these are advertised in the JoinGroup request. The coordinator
    /// picks the most-preferred protocol that *every* member of the group
    /// supports, and reports it back in `JoinGroupResponse.protocol_name`.
    ///
    /// Advertising more than one is what makes a rolling upgrade between
    /// rebalance protocols possible. To move a group from eager `range` to
    /// `cooperative-sticky`, the default `[Range, CooperativeSticky]` lets
    /// old and new members coexist: while any member still supports only
    /// `range`, the whole group stays on `range`; the moment the last old
    /// member is replaced, the coordinator upgrades the group to
    /// `cooperative-sticky` on the next rebalance. Configuring a single
    /// strategy instead forces a full group outage to switch protocols.
    ///
    /// Must not be empty.
    pub(crate) partition_assignment_strategies: Vec<PartitionAssignmentStrategy>,
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
    /// Maximum sleep duration in [`batch_recv`](super::Consumer::batch_recv) and
    /// [`recv`](super::Consumer::recv) when a poll cycle returns no new records
    /// (e.g., no assignment, rebalance in progress, or broker backpressure).
    ///
    /// A small backoff here prevents a tight busy-loop while still draining
    /// the consumer within the caller's timeout window. Default: 10 ms,
    /// which limits the no-data retry rate to ~100 iterations/second.
    /// Latency-sensitive callers may reduce this toward `Duration::ZERO`.
    pub(crate) idle_poll_backoff: Duration,
    /// Maximum time allowed for the [`RebalanceListener::on_partitions_revoked`]
    /// callback to complete before the consumer proceeds with revocation.
    ///
    /// If the callback exceeds this duration, the consumer logs a warning and
    /// continues with the rebalance. A hung listener can otherwise cause
    /// group coordinator session expiry and a forced rebalance loop.
    ///
    /// Default: 5 s. This is well inside the default `session_timeout` (45 s),
    /// so a listener that hits this bound still leaves ample margin for the
    /// rebalance to complete before the coordinator would consider the member
    /// dead.
    pub(crate) revocation_timeout: Duration,
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
            fetch_max_wait: Duration::from_millis(500),
            fetch_max_bytes: 52428800,          // 50 MB
            max_partition_fetch_bytes: 1048576, // 1 MB
            topic_fetch_max_bytes: HashMap::new(),
            max_poll_records: 500,
            max_buffered_records: 500,
            max_poll_interval: Duration::from_secs(300),
            request_timeout: Duration::from_secs(30),
            connect_timeout: crate::network::DEFAULT_CONNECT_TIMEOUT,
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(3),
            isolation_level: IsolationLevel::ReadUncommitted,
            metadata_max_age: Duration::from_secs(300),
            // Matches the Java client's default. Advertising both lets a group
            // migrate from the eager to the cooperative protocol in a single
            // rolling bounce; see the field docs.
            partition_assignment_strategies: vec![
                PartitionAssignmentStrategy::Range,
                PartitionAssignmentStrategy::CooperativeSticky,
            ],
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
            idle_poll_backoff: Duration::from_millis(10),
            revocation_timeout: Duration::from_secs(5),
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

    /// Returns the maximum time the broker may hold a fetch request open.
    #[inline]
    pub fn fetch_max_wait(&self) -> Duration {
        self.fetch_max_wait
    }

    /// Returns the partition assignment strategies in preference order.
    #[inline]
    pub fn partition_assignment_strategies(&self) -> &[PartitionAssignmentStrategy] {
        &self.partition_assignment_strategies
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

    /// Returns the connect timeout.
    #[inline]
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
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

    /// Returns the most-preferred partition assignment strategy.
    ///
    /// This is the first entry of
    /// [`partition_assignment_strategies`](Self::partition_assignment_strategies).
    /// Note that the group may negotiate a different protocol if another
    /// member does not support this one.
    #[inline]
    pub fn partition_assignment_strategy(&self) -> PartitionAssignmentStrategy {
        self.partition_assignment_strategies
            .first()
            .copied()
            .unwrap_or_default()
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

    /// Returns the maximum idle-poll backoff duration.
    #[inline]
    pub fn idle_poll_backoff(&self) -> Duration {
        self.idle_poll_backoff
    }

    /// Returns the revocation callback timeout.
    #[inline]
    pub fn revocation_timeout(&self) -> Duration {
        self.revocation_timeout
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

    /// Set a single partition assignment strategy, replacing the default
    /// preference list.
    ///
    /// Note that pinning the group to exactly one protocol removes the ability
    /// to migrate between rebalance protocols without a full group outage. Use
    /// [`partition_assignment_strategies`](Self::partition_assignment_strategies)
    /// to advertise several.
    pub fn partition_assignment_strategy(mut self, strategy: PartitionAssignmentStrategy) -> Self {
        self.config.partition_assignment_strategies = vec![strategy];
        self
    }

    /// Set the partition assignment strategies in order of preference.
    ///
    /// All are advertised in JoinGroup; the coordinator selects the
    /// most-preferred protocol supported by every member of the group. See
    /// [`ConsumerConfig::partition_assignment_strategies`] for how this
    /// enables rolling protocol migrations.
    ///
    /// An empty list is rejected at build time.
    pub fn partition_assignment_strategies(
        mut self,
        strategies: impl IntoIterator<Item = PartitionAssignmentStrategy>,
    ) -> Self {
        self.config.partition_assignment_strategies = strategies.into_iter().collect();
        self
    }

    /// Set the group protocol (KIP-848).
    ///
    /// `Classic` uses the traditional JoinGroup/SyncGroup/Heartbeat flow.
    /// `Consumer` uses the new server-side assignment via ConsumerGroupHeartbeat.
    ///
    /// **`GroupProtocol::Consumer` requires Kafka 3.7+ and is not yet
    /// recommended for production.** See [`GroupProtocol`] for details.
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

    /// Set how long the broker may hold a fetch request waiting for
    /// [`fetch_min_bytes`](Self::fetch_min_bytes) to accumulate.
    ///
    /// This is independent of the [`poll()`](super::Consumer::poll) timeout;
    /// see [`ConsumerConfig::fetch_max_wait`]. Should be kept comfortably
    /// below `request_timeout`.
    pub fn fetch_max_wait(mut self, wait: Duration) -> Self {
        self.config.fetch_max_wait = wait;
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

    /// Set request timeout: how long one in-flight request may wait for its
    /// response. Default: 30 s.
    ///
    /// Must be at least [`connect_timeout`](Self::connect_timeout), whose
    /// default is 10 s — a request's clock covers establishing the connection
    /// it is sent over, so a shorter value would expire every request before
    /// the handshake could finish. To go below 10 s, lower `connect_timeout`
    /// as well; `build()` returns a config error otherwise.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set the connect timeout: how long TCP establishment to one broker may
    /// take. Default: 10 s.
    ///
    /// This also acts as the floor on
    /// [`request_timeout`](Self::request_timeout), so lowering it is what makes
    /// a sub-10-second request timeout possible.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
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
    /// ConsumerConfig::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .initial_offsets([
    ///         (("my-topic".to_string(), 0), 1_000),
    ///         (("my-topic".to_string(), 1), 2_000),
    ///     ])
    ///     .build()?;
    /// ```
    pub fn initial_offsets(
        mut self,
        offsets: impl IntoIterator<Item = ((String, PartitionId), Offset)>,
    ) -> Self {
        self.config.initial_offsets = offsets.into_iter().collect();
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
    /// [`crate::consumer::LagResult::stale_partitions`]. Pass `Duration::MAX`
    /// to disable
    /// staleness reporting.
    ///
    /// Default: 60 s.
    pub fn lag_staleness_threshold(mut self, threshold: Duration) -> Self {
        self.config.lag_staleness_threshold = threshold;
        self
    }

    /// Set the maximum backoff sleep when a poll cycle returns no new records.
    ///
    /// In [`batch_recv`](super::Consumer::batch_recv) and
    /// [`recv`](super::Consumer::recv), if an internal `poll()` returns with
    /// no new records (e.g., no assignment, rebalance, or backpressure), the
    /// consumer sleeps for up to this duration before retrying. Smaller values
    /// reduce the response latency when records arrive during the sleep window
    /// at the cost of higher CPU usage under sustained idle conditions.
    ///
    /// Default: 10 ms.
    pub fn idle_poll_backoff(mut self, backoff: Duration) -> Self {
        self.config.idle_poll_backoff = backoff;
        self
    }

    /// Set the maximum time allowed for the `on_partitions_revoked` callback.
    ///
    /// If the callback exceeds this duration, the consumer logs a warning and
    /// proceeds with the rebalance. Default: 5 s.
    pub fn revocation_timeout(mut self, timeout: Duration) -> Self {
        self.config.revocation_timeout = timeout;
        self
    }

    /// Build the config.
    ///
    /// # Errors
    ///
    /// See the crate-internal `validate` helper for the full list of constraints.
    pub fn build(self) -> crate::Result<ConsumerConfig> {
        validate(&self.config)?;
        Ok(self.config)
    }
}

/// Validate a fully-populated [`ConsumerConfig`].
///
/// This is the single source of truth for consumer configuration constraints.
/// Both [`ConsumerConfigBuilder::build`] and
/// [`ConsumerBuilder::build`](super::ConsumerBuilder::build) route through it,
/// so a config can never reach a live [`Consumer`](super::Consumer) without
/// having been checked — regardless of which builder the caller used.
///
/// # Errors
///
/// Returns an error if any of the following is violated:
/// - `bootstrap_servers` must be non-empty
/// - `group_id`, when provided, must be non-empty
/// - `heartbeat_interval` must be less than `session_timeout`
/// - `request_timeout` must be greater than `session_timeout`
/// - `max_buffered_records` must be >= 0 (0 disables the cap)
/// - `fetch_min_bytes` must be <= `fetch_max_bytes`
/// - `max_poll_records` must be -1 (unlimited) or positive
/// - `partition_assignment_strategies` must be non-empty
pub(crate) fn validate(config: &ConsumerConfig) -> crate::Result<()> {
    if config.bootstrap_servers.is_empty() {
        return Err(crate::error::KrafkaError::config(
            "bootstrap_servers must not be empty",
        ));
    }
    if config.group_id.as_deref() == Some("") {
        return Err(crate::error::KrafkaError::config(
            "group_id must not be an empty string; omit it entirely to disable group coordination",
        ));
    }
    if config.heartbeat_interval >= config.session_timeout {
        return Err(crate::error::KrafkaError::config(format!(
            "heartbeat_interval ({:?}) must be less than session_timeout ({:?})",
            config.heartbeat_interval, config.session_timeout,
        )));
    }
    // A request timeout shorter than the session timeout is worth flagging:
    // requests that legitimately park on the coordinator for close to a
    // session's length can be aborted client-side, producing rejoin churn
    // that is hard to attribute.
    //
    // It is only a warning, not an error, because the defaults themselves sit
    // in that configuration — request_timeout is 30 s and session_timeout is
    // 45 s, matching Java, which likewise dropped this as a hard constraint
    // when the session default was raised. Rejecting it would make the default
    // config unbuildable.
    if config.request_timeout <= config.session_timeout {
        tracing::warn!(
            request_timeout = ?config.request_timeout,
            session_timeout = ?config.session_timeout,
            "request_timeout does not exceed session_timeout; long-parked coordinator \
             requests may be aborted client-side"
        );
    }
    if config.max_buffered_records < 0 {
        return Err(crate::error::KrafkaError::config(format!(
            "max_buffered_records ({}) must be >= 0",
            config.max_buffered_records,
        )));
    }
    if config.fetch_min_bytes > config.fetch_max_bytes {
        return Err(crate::error::KrafkaError::config(format!(
            "fetch_min_bytes ({}) must be <= fetch_max_bytes ({})",
            config.fetch_min_bytes, config.fetch_max_bytes,
        )));
    }
    // 0 would truncate every fetched batch to nothing, producing a consumer
    // that reads from the broker and returns no records forever.
    if config.max_poll_records == 0 || config.max_poll_records < -1 {
        return Err(crate::error::KrafkaError::config(format!(
            "max_poll_records ({}) must be -1 (unlimited) or a positive integer",
            config.max_poll_records,
        )));
    }
    if config.partition_assignment_strategies.is_empty() {
        return Err(crate::error::KrafkaError::config(
            "partition_assignment_strategies must not be empty",
        ));
    }
    Ok(())
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
            config.partition_assignment_strategy(),
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
            config.partition_assignment_strategy(),
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
            .bootstrap_servers("localhost:9092")
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
