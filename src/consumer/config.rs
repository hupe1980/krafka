//! Consumer configuration.

use std::time::Duration;

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
    pub fn to_offset(&self) -> i64 {
        match self {
            AutoOffsetReset::Earliest => -2,
            AutoOffsetReset::Latest => -1,
            AutoOffsetReset::None => -1,
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
        assert_eq!(AutoOffsetReset::Earliest.to_offset(), -2);
        assert_eq!(AutoOffsetReset::Latest.to_offset(), -1);
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
    }

    #[test]
    fn test_config_builder() {
        let config = ConsumerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .isolation_level(IsolationLevel::ReadCommitted)
            .build();

        assert_eq!(config.bootstrap_servers, "localhost:9092");
        assert_eq!(config.group_id, Some("test-group".to_string()));
        assert_eq!(config.auto_offset_reset, AutoOffsetReset::Earliest);
        assert!(!config.enable_auto_commit);
        assert_eq!(config.isolation_level, IsolationLevel::ReadCommitted);
    }
}
