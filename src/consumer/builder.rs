//! Consumer builder.

use std::sync::Arc;
use std::time::Duration;

use super::rebalance::ErasedRebalanceListener;
use super::{
    AutoOffsetReset, Consumer, ConsumerConfig, ConsumerRebalanceListener, IsolationLevel,
    PartitionAssignmentStrategy,
};
use crate::Offset;
use crate::client::Kafka;
use crate::error::Result;

/// Builder for a [`Consumer`]: consumer settings only. Obtain with
/// [`Kafka::consumer`] or [`Kafka::consumer_without_group`].
#[must_use = "builders do nothing until .build() is called"]
pub struct ConsumerBuilder {
    kafka: Kafka,
    config: ConsumerConfig,
    rebalance_listener: Option<Arc<dyn ErasedRebalanceListener>>,
    interceptors: Vec<Arc<dyn crate::interceptor::ConsumerInterceptor>>,
    key_deserializer: Option<Arc<dyn crate::serdes::Deserializer>>,
    value_deserializer: Option<Arc<dyn crate::serdes::Deserializer>>,
}

impl std::fmt::Debug for ConsumerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConsumerBuilder")
            .field("group_id", &self.config.group_id)
            .finish_non_exhaustive()
    }
}

impl ConsumerBuilder {
    pub(crate) fn new(kafka: Kafka, group_id: Option<String>) -> Self {
        Self {
            kafka,
            config: ConsumerConfig {
                group_id,
                ..ConsumerConfig::default()
            },
            rebalance_listener: None,
            interceptors: Vec::new(),
            key_deserializer: None,
            value_deserializer: None,
        }
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

    /// Override the per-partition fetch byte limit for a specific topic.
    pub fn topic_fetch_max_bytes(mut self, topic: impl Into<String>, bytes: i32) -> Self {
        self.config
            .topic_fetch_max_bytes
            .insert(topic.into(), bytes);
        self
    }

    /// Set maximum poll records per poll() call.
    pub fn max_poll_records(mut self, max: i32) -> Self {
        self.config.max_poll_records = max;
        self
    }

    /// Set the maximum number of fetched records held before they are handed
    /// out.
    ///
    /// When the buffer reaches this limit, `poll()` stops fetching until it
    /// drains, bounding memory when the application consumes more slowly than
    /// the broker delivers. `0` disables the cap. Defaults to 500.
    pub fn max_buffered_records(mut self, max: i32) -> Self {
        self.config.max_buffered_records = max;
        self
    }

    /// Set how long the broker may hold a fetch request waiting for
    /// `fetch_min_bytes` to accumulate.
    ///
    /// Independent of the [`poll()`](super::Consumer::poll) timeout: `poll()`
    /// issues fetches in a loop until its own deadline, so a short value here
    /// still supports long polling. Defaults to 500 ms, matching Java's
    /// `fetch.max.wait.ms`.
    pub fn fetch_max_wait(mut self, wait: Duration) -> Self {
        self.config.fetch_max_wait = wait;
        self
    }

    /// Set the longest gap allowed between two polls (`max.poll.interval.ms`),
    /// also sent to the coordinator as the rebalance timeout. Default: 300 s.
    ///
    /// Enforced under both group protocols: once the application has not
    /// polled for longer, the member leaves the group so its partitions are
    /// reassigned. The next poll reports them to
    /// [`on_partitions_lost`](super::ConsumerRebalanceListener::on_partitions_lost),
    /// rejoins the group and carries on; it returns no error.
    pub fn max_poll_interval(mut self, interval: Duration) -> Self {
        self.config.max_poll_interval = interval;
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

    /// Set a single partition assignment strategy for consumer groups,
    /// replacing the default preference list.
    ///
    /// Pinning the group to one protocol means it cannot be migrated to a
    /// different rebalance protocol without a full group restart; prefer
    /// [`partition_assignment_strategies`](Self::partition_assignment_strategies)
    /// where that matters.
    pub fn partition_assignment_strategy(mut self, strategy: PartitionAssignmentStrategy) -> Self {
        self.config.partition_assignment_strategies = vec![strategy];
        self
    }

    /// Set the partition assignment strategies in order of preference.
    ///
    /// All are advertised in JoinGroup; the coordinator selects the
    /// most-preferred protocol that every member of the group supports. The
    /// default is `[Range, CooperativeSticky]`, which allows a group to move
    /// from the eager to the cooperative protocol in a single rolling bounce.
    pub fn partition_assignment_strategies(
        mut self,
        strategies: impl IntoIterator<Item = PartitionAssignmentStrategy>,
    ) -> Self {
        self.config.partition_assignment_strategies = strategies.into_iter().collect();
        self
    }

    /// Set the static group membership instance ID (KIP-345).
    ///
    /// When configured, the consumer uses static group membership. The broker
    /// preserves partition assignments across restarts as long as the same
    /// instance ID is used, avoiding unnecessary rebalances.
    ///
    pub fn group_instance_id(mut self, id: impl Into<String>) -> Self {
        self.config.group_instance_id = Some(id.into());
        self
    }

    /// Set the high-watermark staleness threshold used by [`Consumer::lag`](super::Consumer::lag).
    ///
    /// A partition's high watermark is considered stale when it has not been
    /// refreshed within this duration. Stale partitions are reported in
    /// [`PartitionLag::stale`](super::PartitionLag::stale) so callers can decide whether to trust
    /// the lag value.
    ///
    /// Default: 60 seconds.
    pub fn lag_staleness_threshold(mut self, threshold: Duration) -> Self {
        self.config.lag_staleness_threshold = threshold;
        self
    }

    /// Set the client rack ID for closest-replica fetching (KIP-392).
    ///
    /// When configured, the consumer includes its rack in fetch requests.
    /// The broker may return a preferred read replica in the same rack,
    /// reducing cross-rack network traffic.
    pub fn client_rack(mut self, rack: impl Into<String>) -> Self {
        self.config.client_rack = Some(rack.into());
        self
    }

    /// Select the consumer group protocol.
    ///
    /// [`GroupProtocol::Consumer`](super::GroupProtocol::Consumer) — the
    /// KIP-848 protocol, where the coordinator computes assignments
    /// server-side and `ConsumerGroupHeartbeat` is the sole membership channel
    /// — is the **recommended** choice. It has been production ready since
    /// Apache Kafka 4.0.
    ///
    /// [`GroupProtocol::Classic`](super::GroupProtocol::Classic) remains the
    /// default so that upgrading krafka is never itself a protocol migration,
    /// but Apache Kafka 4.3 has begun deprecating it (KIP-1274) and krafka
    /// logs a one-time warning when a group starts on it.
    ///
    /// On a cluster without `ConsumerGroupHeartbeat`, `GroupProtocol::Consumer`
    /// fails the first poll with `UnknownApiVersion` naming
    /// `GroupProtocol::Classic`.
    pub fn group_protocol(mut self, protocol: super::GroupProtocol) -> Self {
        self.config.group_protocol = protocol;
        self
    }

    /// Name the server-side assignor the coordinator uses for this member
    /// (`group.remote.assignor`); Apache Kafka brokers ship `uniform` and
    /// `range`. Applies only to
    /// [`GroupProtocol::Consumer`](super::GroupProtocol::Consumer):
    /// [`build`](Self::build) rejects it with the classic protocol. Without
    /// it the broker's default assignor is used.
    ///
    /// A name the broker does not know fails the join with a non-retriable
    /// `UNSUPPORTED_ASSIGNOR` error naming it.
    pub fn group_remote_assignor(mut self, assignor: impl Into<String>) -> Self {
        self.config.group_remote_assignor = Some(assignor.into());
        self
    }

    /// Set the maximum decompressed size for a single record batch.
    ///
    /// Compressed payloads that decompress beyond this limit are rejected as
    /// potential compression bombs. Lower it when consuming from a topic whose
    /// producers are not fully trusted; the default is 128 MiB.
    pub fn max_decompressed_size(mut self, size: usize) -> Self {
        self.config.max_decompressed_size = size;
        self
    }

    /// Push this consumer's metrics to the brokers when a cluster operator
    /// subscribes to them (KIP-714, Java `enable.metrics.push`). Default: on.
    /// Nothing is sent to a cluster without a client-telemetry plugin.
    pub fn metrics_push(mut self, enable: bool) -> Self {
        self.config.metrics_push = enable;
        self
    }

    /// Set how long `poll()` waits when no partition can be fetched (all
    /// paused or backing off).
    ///
    /// Smaller values reduce latency, at the cost of CPU while idle.
    /// Default: 10 ms.
    pub fn idle_poll_backoff(mut self, backoff: Duration) -> Self {
        self.config.idle_poll_backoff = backoff;
        self
    }

    /// Set a rebalance listener to be notified of partition assignment changes.
    pub fn rebalance_listener(
        mut self,
        listener: impl ConsumerRebalanceListener + 'static,
    ) -> Self {
        self.rebalance_listener = Some(Arc::new(listener));
        self
    }

    /// Set per-partition initial offsets applied before auto-offset-reset.
    ///
    /// When a partition is first assigned and has no committed group offset,
    /// the consumer starts fetching from the given offset instead of applying
    /// `auto_offset_reset`. Useful for exactly-once recovery.
    ///
    pub fn initial_offsets<P, O>(mut self, offsets: impl IntoIterator<Item = (P, O)>) -> Self
    where
        P: std::borrow::Borrow<super::TopicPartition>,
        O: std::borrow::Borrow<Offset>,
    {
        self.config.initial_offsets = offsets
            .into_iter()
            .map(|(tp, offset)| {
                let tp = tp.borrow();
                ((tp.topic.clone(), tp.partition), *offset.borrow())
            })
            .collect();
        self
    }

    /// Append an interceptor to the chain. Interceptors run in the order they
    /// were added, each panic-isolated.
    pub fn interceptor(
        mut self,
        interceptor: impl crate::interceptor::ConsumerInterceptor + 'static,
    ) -> Self {
        self.interceptors.push(Arc::new(interceptor));
        self
    }

    /// Decode every record key before it is returned (Java
    /// `key.deserializer`). A failure is a
    /// [`RecordDeserialization`](crate::KrafkaError::RecordDeserialization)
    /// error; the records before the failing one are returned first.
    pub fn key_deserializer(
        mut self,
        deserializer: impl crate::serdes::Deserializer + 'static,
    ) -> Self {
        self.key_deserializer = Some(Arc::new(deserializer));
        self
    }

    /// Decode every record value before it is returned (Java
    /// `value.deserializer`); see [`key_deserializer`](Self::key_deserializer).
    pub fn value_deserializer(
        mut self,
        deserializer: impl crate::serdes::Deserializer + 'static,
    ) -> Self {
        self.value_deserializer = Some(Arc::new(deserializer));
        self
    }

    fn validate(&self) -> Result<()> {
        super::config::validate(&self.config, self.kafka.request_timeout())
    }

    /// The validated settings, without starting a consumer.
    #[cfg(test)]
    pub(crate) fn build_config(self) -> Result<ConsumerConfig> {
        self.validate()?;
        Ok(self.config)
    }

    /// Build the consumer. Contacts no broker: the group is joined by the
    /// first [`poll`](super::Consumer::poll) after
    /// [`subscribe`](super::Consumer::subscribe).
    ///
    /// # Errors
    ///
    /// [`KrafkaError::Config`](crate::KrafkaError::Config) naming the setting
    /// for an invalid configuration.
    // Async like every other client's `build`, so a later version may contact
    // the cluster here without breaking callers.
    #[allow(clippy::unused_async)]
    pub async fn build(self) -> Result<Consumer> {
        self.validate()?;
        if self.config.enable_auto_commit && self.config.group_id.is_none() {
            tracing::debug!("enable_auto_commit has no effect without a group");
        }
        // `session_timeout` and `max_poll_interval` bound two independent
        // failure modes, so neither has to be smaller than the other; a
        // session timeout above the poll interval is only unusual.
        if self.config.session_timeout > self.config.max_poll_interval {
            tracing::warn!(
                session_timeout = ?self.config.session_timeout,
                max_poll_interval = ?self.config.max_poll_interval,
                "session_timeout exceeds max_poll_interval; a stalled application \
                 will be removed from the group by the poll-interval check before \
                 the coordinator's session timer would notice"
            );
        }

        let mut consumer = Consumer::new(&self.kafka, self.config);
        if let Some(listener) = self.rebalance_listener {
            consumer.rebalance_listener = listener;
        }
        match self.interceptors.len() {
            0 => {}
            1 => {
                if let Some(single) = self.interceptors.into_iter().next() {
                    consumer.interceptor = single;
                }
            }
            _ => {
                consumer.interceptor = Arc::new(crate::interceptor::ConsumerInterceptorChain::new(
                    self.interceptors,
                ));
            }
        }
        consumer.key_deserializer = self.key_deserializer;
        consumer.value_deserializer = self.value_deserializer;
        Ok(consumer)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use crate::Kafka;
    use crate::consumer::{
        AutoOffsetReset, ConsumerRebalanceListener, PartitionAssignmentStrategy, TopicPartition,
    };
    use std::sync::Arc;
    use std::time::Duration;

    fn consumer() -> super::ConsumerBuilder {
        Kafka::detached().consumer("test-group")
    }

    #[tokio::test]
    async fn test_consumer_builder() {
        let builder = consumer()
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .max_poll_records(100)
            .max_poll_interval(Duration::from_secs(600));

        assert_eq!(builder.config.group_id, Some("test-group".to_string()));
        assert_eq!(builder.config.auto_offset_reset, AutoOffsetReset::Earliest);
        assert!(!builder.config.enable_auto_commit);
        assert_eq!(builder.config.max_poll_records, 100);
        assert_eq!(builder.config.max_poll_interval, Duration::from_secs(600));
    }

    #[tokio::test]
    async fn a_consumer_without_group_has_no_group_id() {
        let builder = Kafka::detached().consumer_without_group();
        assert!(builder.config.group_id.is_none());
    }

    #[tokio::test]
    async fn test_consumer_builder_with_rebalance_listener() {
        struct TestListener;
        impl ConsumerRebalanceListener for TestListener {
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) {}
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) {}
        }
        let builder = consumer().rebalance_listener(Arc::new(TestListener));
        assert!(builder.rebalance_listener.is_some());
    }

    #[tokio::test]
    async fn interceptors_append_in_order() {
        use crate::interceptor::ConsumerInterceptor;

        #[derive(Debug)]
        struct A;
        impl ConsumerInterceptor for A {}

        let builder = consumer().interceptor(A).interceptor(Arc::new(A));
        assert_eq!(builder.interceptors.len(), 2);
    }

    #[tokio::test]
    async fn invalid_settings_are_rejected_naming_the_setting() {
        for (builder, setting) in [
            (consumer().max_poll_records(0), "max_poll_records"),
            (consumer().max_poll_records(-2), "max_poll_records"),
            (consumer().max_buffered_records(-1), "max_buffered_records"),
            (
                consumer().fetch_min_bytes(1000).fetch_max_bytes(100),
                "fetch_min_bytes",
            ),
            (Kafka::detached().consumer(""), "group_id"),
            (
                consumer().partition_assignment_strategies(Vec::new()),
                "partition_assignment_strategies",
            ),
            (
                consumer()
                    .heartbeat_interval(Duration::from_secs(50))
                    .session_timeout(Duration::from_secs(45)),
                "heartbeat_interval",
            ),
        ] {
            let err = builder.build().await.err().expect("must be rejected");
            assert!(err.to_string().contains(setting), "{setting}: {err}");
        }
    }

    #[tokio::test]
    async fn session_timeout_above_max_poll_interval_is_accepted() {
        consumer()
            .session_timeout(Duration::from_secs(120))
            .max_poll_interval(Duration::from_secs(60))
            .heartbeat_interval(Duration::from_secs(3))
            .build_config()
            .expect("a warning, not a config error");
    }

    #[tokio::test]
    async fn the_default_strategies_allow_protocol_migration() {
        assert_eq!(
            consumer().config.partition_assignment_strategies,
            vec![
                PartitionAssignmentStrategy::Range,
                PartitionAssignmentStrategy::CooperativeSticky
            ]
        );
        assert_eq!(
            consumer()
                .partition_assignment_strategy(PartitionAssignmentStrategy::RoundRobin)
                .config
                .partition_assignment_strategies,
            vec![PartitionAssignmentStrategy::RoundRobin]
        );
    }
}
