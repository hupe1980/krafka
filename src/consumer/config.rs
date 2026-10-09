//! Consumer configuration.

use std::time::Duration;

use ahash::AHashMap as HashMap;

use crate::{Offset, PartitionId};

/// Where a partition with no committed offset, or whose position fell out of
/// range, starts (`auto.offset.reset`).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AutoOffsetReset {
    /// Start from the earliest offset.
    Earliest,
    /// Start from the latest offset.
    #[default]
    Latest,
    /// Start from the first record timestamped at or after now minus this
    /// duration, or from the end of the log when there is none (KIP-1106,
    /// Java's `by_duration:<duration>`).
    ///
    /// Unlike `Latest`, records written to a partition before the consumer
    /// first saw it are not skipped as long as they are younger than the
    /// duration. "Now" is read when the reset runs.
    ByDuration(std::time::Duration),
    /// Fail with [`KrafkaError::NoOffset`](crate::KrafkaError::NoOffset).
    None,
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
/// # Which one to choose
///
/// **Prefer [`Consumer`](Self::Consumer).** KIP-848 was declared production
/// ready in Apache Kafka 4.0 and is the default server-side protocol from 4.0
/// onwards. Apache Kafka 4.3 deprecates the classic protocol in the Java
/// consumer and logs a warning when it runs (KIP-1274; as of 2026-10-09 the
/// KIP proposes flipping the default in a later major release and removing
/// classic support after that). krafka's floor is Kafka 3.9, so `Classic`
/// remains the default here, and selecting it emits a one-time deprecation
/// warning to match the Java client.
///
/// | | `Classic` | `Consumer` (KIP-848) |
/// |---|---|---|
/// | Assignment computed by | the group leader, client-side | the group coordinator, broker-side |
/// | Rebalance | stop-the-world barrier across all members | incremental, per-member reconciliation |
/// | A slow member | stalls the whole group | affects only its own partitions |
/// | Broker requirement | any | Kafka 4.0+ (3.7+ with `group.coordinator.new.enable=true`) |
/// | Apache status | deprecated from 4.3 (KIP-1274) | production ready from 4.0 |
///
/// krafka's KIP-848 implementation is validated end to end against a
/// multi-member reconciliation suite: revoke-before-assign ordering, epoch
/// fencing, and the invariant that no partition is ever owned by two members
/// at once.
///
/// Switching is a one-line change, but note that the two protocols **cannot
/// mix within one group** on brokers below 4.0: move every member of a group
/// together, or upgrade the cluster first.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GroupProtocol {
    /// Classic group protocol (JoinGroup/SyncGroup/Heartbeat).
    ///
    /// Works against every broker version, and is still krafka's default so
    /// that upgrading the client is never itself a protocol migration.
    ///
    /// **Deprecated upstream.** Apache Kafka 4.3 (KIP-1274) logs a warning
    /// whenever the Java consumer runs this protocol, and krafka mirrors it.
    /// As of 2026-10-09 the KIP proposes making [`Consumer`](Self::Consumer)
    /// the default and later removing classic support.
    #[default]
    Classic,
    /// KIP-848 consumer group protocol (ConsumerGroupHeartbeat).
    ///
    /// Server-side assignment with incremental reconciliation: a rebalance
    /// does not stop every member, and a slow member does not stall the
    /// group.
    ///
    /// Production ready since Apache Kafka 4.0 and the recommended choice for
    /// new deployments. Requires Kafka 4.0+, or 3.7–3.9 with
    /// `group.coordinator.new.enable=true` on the broker.
    Consumer,
}

/// Partition assignment strategy for consumer groups.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PartitionAssignmentStrategy {
    /// Range assignor (default): per topic, contiguous partition ranges over
    /// the members subscribed to it.
    ///
    /// Eager protocol: a member commits and revokes its whole assignment
    /// before it rejoins, so no partition is consumed by two members at once.
    #[default]
    Range,
    /// Round-robin assignor: every partition, in turn, to the next member
    /// subscribed to its topic.
    ///
    /// Eager protocol.
    RoundRobin,
    /// Cooperative sticky assignor: members keep their partitions where the
    /// balance allows, and the counts of members with the same subscription
    /// differ by at most one, moving as few partitions as possible.
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

/// Consumer settings, as the builder collected them.
#[derive(Debug, Clone)]
pub(crate) struct ConsumerConfig {
    /// Consumer group ID.
    pub(crate) group_id: Option<String>,
    /// Auto offset reset behavior.
    pub(crate) auto_offset_reset: AutoOffsetReset,
    /// Enable automatic offset commit.
    ///
    /// When `true` (the default), `poll()` commits the positions once
    /// [`auto_commit_interval`](Self::auto_commit_interval) (default: 5 s)
    /// has elapsed, and before partitions are revoked and on close.
    ///
    /// **Important**: auto-commit commits the position after the last record
    /// *returned*, not the last record *processed* by the application. If the
    /// application crashes after a record is returned but before it is
    /// processed, that record can be skipped on restart. To commit only
    /// processed records, disable auto-commit and call
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
    /// Maximum fetched records held before they are handed out.
    ///
    /// A poll decodes up to `max_poll_records` plus the room left under this
    /// cap, so the next poll is often served without a round trip. When the
    /// buffer is full — in practice, records of paused partitions —
    /// [`poll()`](super::Consumer::poll) skips fetching.
    ///
    /// Set to 0 to disable the buffer cap (unlimited). Defaults to 500.
    /// Comparable to librdkafka's `queued.max.messages.kbytes` (count-based
    /// rather than size-based).
    pub(crate) max_buffered_records: i32,
    /// Maximum poll interval.
    pub(crate) max_poll_interval: Duration,
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
    /// Server-side assignor the coordinator uses for this member
    /// (`group.remote.assignor`, KIP-848 only); `None` lets the broker pick.
    pub(crate) group_remote_assignor: Option<String>,
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
    /// Maximum decompressed size for record batches (compression bomb protection).
    /// Defaults to 128 MiB.
    pub(crate) max_decompressed_size: usize,

    /// Per-partition initial offsets applied before auto-offset-reset.
    ///
    /// When a partition is first assigned and has no committed group offset,
    /// the corresponding entry from this map is used as the starting fetch
    /// position, overriding `auto_offset_reset`.
    ///
    /// Keyed by `(topic, partition)`.  Build via
    /// [`ConsumerBuilder::initial_offsets`](super::ConsumerBuilder::initial_offsets).
    pub(crate) initial_offsets: HashMap<(String, PartitionId), Offset>,
    /// Duration after which a partition's cached high watermark is considered
    /// stale by [`Consumer::lag`](crate::consumer::Consumer::lag).
    ///
    /// If a partition's watermark has not been refreshed within this window
    /// [`Consumer::lag`](crate::consumer::Consumer::lag) marks it stale. The
    /// lag value is still returned but may be inaccurate.
    ///
    /// Default: 60 s. Set to `Duration::MAX` to disable staleness reporting.
    pub(crate) lag_staleness_threshold: Duration,
    /// How long `poll()` waits when no partition can be fetched (all paused
    /// or backing off).
    ///
    /// A small backoff here prevents a tight busy-loop while still draining
    /// the consumer within the caller's timeout window. Default: 10 ms,
    /// which limits the no-data retry rate to ~100 iterations/second.
    /// Latency-sensitive callers may reduce this toward `Duration::ZERO`.
    pub(crate) idle_poll_backoff: Duration,
    /// Push this client's metrics to brokers that subscribe to them (KIP-714,
    /// Java `enable.metrics.push`).
    pub(crate) metrics_push: bool,
}

impl Default for ConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: None,
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
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(3),
            isolation_level: IsolationLevel::ReadUncommitted,
            // Matches the Java client's default. Advertising both lets a group
            // migrate from the eager to the cooperative protocol in a single
            // rolling bounce; see the field docs.
            partition_assignment_strategies: vec![
                PartitionAssignmentStrategy::Range,
                PartitionAssignmentStrategy::CooperativeSticky,
            ],
            group_protocol: GroupProtocol::Classic,
            group_remote_assignor: None,
            group_instance_id: None,
            client_rack: None,
            max_decompressed_size: crate::protocol::RecordBatch::MAX_DECOMPRESSED_SIZE,
            initial_offsets: HashMap::new(),
            lag_staleness_threshold: Duration::from_secs(60),
            idle_poll_backoff: Duration::from_millis(10),
            metrics_push: true,
        }
    }
}

/// Validate a [`ConsumerConfig`]; `request_timeout` is the handle's.
///
/// # Errors
///
/// Returns an error if any of the following is violated:
/// - `group_id`, when provided, must be non-empty
/// - `heartbeat_interval` must be less than `session_timeout`
/// - `max_buffered_records` must be >= 0 (0 disables the cap)
/// - `fetch_min_bytes` must be <= `fetch_max_bytes`
/// - `max_poll_records` must be -1 (unlimited) or positive
/// - `partition_assignment_strategies` must be non-empty
/// - `group_remote_assignor`, when set, must be non-empty and needs
///   [`GroupProtocol::Consumer`]
pub(crate) fn validate(config: &ConsumerConfig, request_timeout: Duration) -> crate::Result<()> {
    if config.group_id.as_deref() == Some("") {
        return Err(crate::error::KrafkaError::config(
            "group_id must not be an empty string; use Kafka::consumer_without_group for a \
             consumer without a group",
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
    if request_timeout <= config.session_timeout {
        tracing::debug!(
            request_timeout = ?request_timeout,
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
    if let Some(assignor) = &config.group_remote_assignor {
        if assignor.is_empty() {
            return Err(crate::error::KrafkaError::config(
                "group_remote_assignor must not be an empty string",
            ));
        }
        if config.group_protocol != GroupProtocol::Consumer {
            return Err(crate::error::KrafkaError::config(format!(
                "group_remote_assignor ({assignor:?}) applies only to \
                 GroupProtocol::Consumer (KIP-848); the classic protocol assigns \
                 partitions client-side with partition_assignment_strategies",
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::Kafka;

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
            config.partition_assignment_strategies[0],
            PartitionAssignmentStrategy::Range
        );
        assert_eq!(config.group_protocol, GroupProtocol::Classic);
        assert!(config.group_instance_id.is_none());
        assert!(config.client_rack.is_none());
    }

    #[tokio::test]
    async fn test_config_builder() {
        let config = Kafka::detached()
            .consumer("test-group")
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .isolation_level(IsolationLevel::ReadCommitted)
            .partition_assignment_strategy(PartitionAssignmentStrategy::CooperativeSticky)
            .fetch_min_bytes(1024)
            .fetch_max_bytes(10 * 1024 * 1024)
            .group_instance_id("instance-1")
            .client_rack("us-east-1a")
            .group_protocol(GroupProtocol::Consumer)
            .build_config()
            .unwrap();

        assert_eq!(config.group_id, Some("test-group".to_string()));
        assert_eq!(config.auto_offset_reset, AutoOffsetReset::Earliest);
        assert!(!config.enable_auto_commit);
        assert_eq!(config.isolation_level, IsolationLevel::ReadCommitted);
        assert_eq!(
            config.partition_assignment_strategies,
            vec![PartitionAssignmentStrategy::CooperativeSticky]
        );
        assert_eq!(config.fetch_min_bytes, 1024);
        assert_eq!(config.fetch_max_bytes, 10 * 1024 * 1024);
        assert_eq!(config.group_instance_id.as_deref(), Some("instance-1"));
        assert_eq!(config.client_rack.as_deref(), Some("us-east-1a"));
        assert_eq!(config.group_protocol, GroupProtocol::Consumer);
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

    #[tokio::test]
    async fn buffer_and_poll_limits_are_validated() {
        let consumer = || Kafka::detached().consumer("g");
        assert_eq!(
            consumer()
                .max_buffered_records(0)
                .build_config()
                .unwrap()
                .max_buffered_records,
            0
        );
        assert_eq!(
            consumer()
                .max_poll_records(-1)
                .build_config()
                .unwrap()
                .max_poll_records,
            -1,
            "-1 means unlimited"
        );
        for bad in [0, -2] {
            let err = consumer().max_poll_records(bad).build_config().unwrap_err();
            assert!(err.to_string().contains("max_poll_records"), "{err}");
        }
        let err = consumer()
            .max_buffered_records(-1)
            .build_config()
            .unwrap_err();
        assert!(err.to_string().contains("max_buffered_records"), "{err}");
    }
}
