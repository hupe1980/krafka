//! Consumer configuration.

use std::time::Duration;

use crate::auth::AuthConfig;

/// Auto offset reset behavior.
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
    pub fn to_offset(&self) -> Option<i64> {
        match self {
            AutoOffsetReset::Earliest => Some(-2),
            AutoOffsetReset::Latest => Some(-1),
            AutoOffsetReset::None => None,
        }
    }
}

/// Transaction isolation level.
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
    pub fn to_i8(self) -> i8 {
        match self {
            IsolationLevel::ReadUncommitted => 0,
            IsolationLevel::ReadCommitted => 1,
        }
    }
}

/// Partition assignment strategy for consumer groups.
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
    pub fn protocol_name(&self) -> &'static str {
        match self {
            Self::Range => "range",
            Self::RoundRobin => "roundrobin",
            Self::CooperativeSticky => "cooperative-sticky",
        }
    }
}

/// Consumer configuration.
#[derive(Debug, Clone)]
pub struct ConsumerConfig {
    /// Bootstrap servers (comma-separated).
    pub bootstrap_servers: String,
    /// Consumer group ID.
    pub group_id: Option<String>,
    /// Client ID.
    pub client_id: String,
    /// Auto offset reset behavior.
    pub auto_offset_reset: AutoOffsetReset,
    /// Enable auto commit.
    pub enable_auto_commit: bool,
    /// Auto commit interval.
    pub auto_commit_interval: Duration,
    /// Minimum bytes to fetch.
    pub fetch_min_bytes: i32,
    /// Maximum bytes to fetch.
    pub fetch_max_bytes: i32,
    /// Maximum bytes per partition.
    pub max_partition_fetch_bytes: i32,
    /// Maximum poll records.
    pub max_poll_records: i32,
    /// Maximum poll interval.
    pub max_poll_interval: Duration,
    /// Request timeout.
    pub request_timeout: Duration,
    /// Session timeout for consumer groups.
    pub session_timeout: Duration,
    /// Heartbeat interval.
    pub heartbeat_interval: Duration,
    /// Isolation level.
    pub isolation_level: IsolationLevel,
    /// Metadata max age.
    pub metadata_max_age: Duration,
    /// Partition assignment strategy.
    pub partition_assignment_strategy: PartitionAssignmentStrategy,
    /// Static group membership instance ID (KIP-345).
    ///
    /// When set, the consumer uses static membership. The broker will not
    /// trigger a rebalance when a static member leaves and rejoins within the
    /// session timeout, as long as it uses the same instance ID.
    pub group_instance_id: Option<String>,
    /// Authentication configuration (optional).
    pub auth: Option<AuthConfig>,
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
            max_poll_records: 500,
            max_poll_interval: Duration::from_secs(300),
            request_timeout: Duration::from_secs(30),
            session_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(3),
            isolation_level: IsolationLevel::ReadUncommitted,
            metadata_max_age: Duration::from_secs(300),
            partition_assignment_strategy: PartitionAssignmentStrategy::Range,
            group_instance_id: None,
            auth: None,
        }
    }
}

impl ConsumerConfig {
    /// Create a new config builder.
    pub fn builder() -> ConsumerConfigBuilder {
        ConsumerConfigBuilder::default()
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

    /// Set partition assignment strategy.
    pub fn partition_assignment_strategy(mut self, strategy: PartitionAssignmentStrategy) -> Self {
        self.config.partition_assignment_strategy = strategy;
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

    /// Set maximum records per poll.
    pub fn max_poll_records(mut self, max: i32) -> Self {
        self.config.max_poll_records = max;
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

    /// Build the config.
    pub fn build(self) -> ConsumerConfig {
        self.config
    }
}

#[cfg(test)]
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
            .build();

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
            .fetch_min_bytes(1024)
            .build();
        assert_eq!(
            config.fetch_min_bytes, 1024,
            "fetch_min_bytes should be set by builder"
        );
    }

    #[test]
    fn test_config_builder_fetch_max_bytes() {
        let config = ConsumerConfig::builder()
            .fetch_max_bytes(10 * 1024 * 1024)
            .build();
        assert_eq!(
            config.fetch_max_bytes,
            10 * 1024 * 1024,
            "fetch_max_bytes should be set by builder"
        );
    }

    #[test]
    fn test_config_builder_metadata_max_age() {
        let config = ConsumerConfig::builder()
            .metadata_max_age(Duration::from_secs(60))
            .build();
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
            .group_id("my-group")
            .group_instance_id("instance-1")
            .build();
        assert_eq!(
            config.group_instance_id,
            Some("instance-1".to_string()),
            "group_instance_id should be set by builder"
        );
    }
}
