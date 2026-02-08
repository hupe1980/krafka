//! Producer configuration.

use std::time::Duration;

use crate::protocol::Compression;

/// Required acknowledgments for produce requests.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Acks {
    /// Don't wait for any acknowledgment.
    None,
    /// Wait for leader acknowledgment.
    #[default]
    Leader,
    /// Wait for all in-sync replicas.
    All,
}

impl Acks {
    /// Convert to the protocol i16 value.
    pub fn to_i16(self) -> i16 {
        match self {
            Acks::None => 0,
            Acks::Leader => 1,
            Acks::All => -1,
        }
    }

    /// Create from i16 value.
    pub fn from_i16(value: i16) -> Self {
        match value {
            0 => Acks::None,
            1 => Acks::Leader,
            _ => Acks::All,
        }
    }
}

/// Producer configuration.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Bootstrap servers (comma-separated).
    pub bootstrap_servers: String,
    /// Client ID.
    pub client_id: String,
    /// Required acknowledgments.
    pub acks: Acks,
    /// Compression type.
    pub compression: Compression,
    /// Batch size in bytes.
    pub batch_size: usize,
    /// Time to wait before sending a batch.
    pub linger: Duration,
    /// Request timeout.
    pub request_timeout: Duration,
    /// Number of retries.
    pub retries: u32,
    /// Time between retries.
    pub retry_backoff: Duration,
    /// Max in-flight requests per connection.
    pub max_in_flight: usize,
    /// Enable idempotent producer.
    pub enable_idempotence: bool,
    /// Max block time when buffer is full.
    pub max_block: Duration,
    /// Buffer memory size.
    pub buffer_memory: usize,
    /// Metadata max age.
    pub metadata_max_age: Duration,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            client_id: "krafka".to_string(),
            acks: Acks::Leader,
            compression: Compression::None,
            batch_size: 16384,
            linger: Duration::from_millis(0),
            request_timeout: Duration::from_secs(30),
            retries: 3,
            retry_backoff: Duration::from_millis(100),
            max_in_flight: 5,
            enable_idempotence: false,
            max_block: Duration::from_secs(60),
            buffer_memory: 32 * 1024 * 1024, // 32 MB
            metadata_max_age: Duration::from_secs(300),
        }
    }
}

impl ProducerConfig {
    /// Create a new config builder.
    pub fn builder() -> ProducerConfigBuilder {
        ProducerConfigBuilder::default()
    }
}

/// Builder for ProducerConfig.
#[must_use = "builders do nothing until .build() is called"]
#[derive(Debug, Default)]
pub struct ProducerConfigBuilder {
    config: ProducerConfig,
}

impl ProducerConfigBuilder {
    /// Set bootstrap servers.
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    /// Set client ID.
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.config.client_id = id.into();
        self
    }

    /// Set acks.
    pub fn acks(mut self, acks: Acks) -> Self {
        self.config.acks = acks;
        self
    }

    /// Set compression.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.config.compression = compression;
        self
    }

    /// Set batch size.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Set linger time.
    pub fn linger(mut self, duration: Duration) -> Self {
        self.config.linger = duration;
        self
    }

    /// Build the config.
    pub fn build(self) -> ProducerConfig {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acks_to_i16() {
        assert_eq!(Acks::None.to_i16(), 0);
        assert_eq!(Acks::Leader.to_i16(), 1);
        assert_eq!(Acks::All.to_i16(), -1);
    }

    #[test]
    fn test_acks_from_i16() {
        assert_eq!(Acks::from_i16(0), Acks::None);
        assert_eq!(Acks::from_i16(1), Acks::Leader);
        assert_eq!(Acks::from_i16(-1), Acks::All);
    }

    #[test]
    fn test_config_default() {
        let config = ProducerConfig::default();
        assert_eq!(config.acks, Acks::Leader);
        assert_eq!(config.compression, Compression::None);
        assert_eq!(config.batch_size, 16384);
        assert_eq!(config.retries, 3);
    }

    #[test]
    fn test_config_builder() {
        let config = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .client_id("test")
            .acks(Acks::All)
            .compression(Compression::Lz4)
            .batch_size(32768)
            .build();

        assert_eq!(config.bootstrap_servers, "localhost:9092");
        assert_eq!(config.client_id, "test");
        assert_eq!(config.acks, Acks::All);
        assert_eq!(config.compression, Compression::Lz4);
        assert_eq!(config.batch_size, 32768);
    }
}
