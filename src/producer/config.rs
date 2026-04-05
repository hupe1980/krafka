//! Producer configuration.

use std::time::Duration;

use crate::auth::AuthConfig;
use crate::protocol::Compression;

/// Required acknowledgments for produce requests.
#[non_exhaustive]
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
    #[inline]
    pub fn to_i16(self) -> i16 {
        match self {
            Acks::None => 0,
            Acks::Leader => 1,
            Acks::All => -1,
        }
    }

    /// Create from i16 value.
    ///
    /// Known values: 0 = None, 1 = Leader, -1 = All.
    /// Unknown values default to `All` (safest default — requires full ISR ack).
    #[inline]
    pub fn from_i16(value: i16) -> Self {
        match value {
            0 => Acks::None,
            1 => Acks::Leader,
            -1 => Acks::All,
            other => {
                tracing::warn!(acks = other, "Unknown acks value, defaulting to All");
                Acks::All
            }
        }
    }
}

/// Producer configuration.
///
/// Use [`ProducerConfig::builder()`] or [`Default::default()`] to construct.
#[derive(Debug, Clone)]
pub struct ProducerConfig {
    /// Bootstrap servers (comma-separated).
    pub(crate) bootstrap_servers: String,
    /// Client ID.
    pub(crate) client_id: String,
    /// Required acknowledgments.
    pub(crate) acks: Acks,
    /// Compression type.
    pub(crate) compression: Compression,
    /// Batch size in bytes.
    pub(crate) batch_size: usize,
    /// Time to wait before sending a batch.
    pub(crate) linger: Duration,
    /// Request timeout.
    pub(crate) request_timeout: Duration,
    /// Number of retries.
    pub(crate) retries: u32,
    /// Time between retries.
    pub(crate) retry_backoff: Duration,
    /// Max in-flight requests per connection.
    pub(crate) max_in_flight: usize,
    /// Enable idempotent producer.
    pub(crate) enable_idempotence: bool,
    /// Max block time when buffer is full.
    pub(crate) max_block: Duration,
    /// Buffer memory size.
    pub(crate) buffer_memory: usize,
    /// Metadata max age.
    pub(crate) metadata_max_age: Duration,
    /// Authentication configuration (optional).
    pub(crate) auth: Option<AuthConfig>,
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
            auth: None,
        }
    }
}

impl ProducerConfig {
    /// Create a new config builder.
    pub fn builder() -> ProducerConfigBuilder {
        ProducerConfigBuilder::default()
    }

    /// Returns the bootstrap servers.
    #[inline]
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Returns the client ID.
    #[inline]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the required acknowledgments.
    #[inline]
    pub fn acks(&self) -> Acks {
        self.acks
    }

    /// Returns the compression type.
    #[inline]
    pub fn compression(&self) -> Compression {
        self.compression
    }

    /// Returns the batch size in bytes.
    #[inline]
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    /// Returns the linger time.
    #[inline]
    pub fn linger(&self) -> Duration {
        self.linger
    }

    /// Returns the request timeout.
    #[inline]
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Returns the number of retries.
    #[inline]
    pub fn retries(&self) -> u32 {
        self.retries
    }

    /// Returns the retry backoff duration.
    #[inline]
    pub fn retry_backoff(&self) -> Duration {
        self.retry_backoff
    }

    /// Returns the max in-flight requests per connection.
    #[inline]
    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
    }

    /// Returns whether idempotent producer is enabled.
    #[inline]
    pub fn enable_idempotence(&self) -> bool {
        self.enable_idempotence
    }

    /// Returns the max block time when buffer is full.
    #[inline]
    pub fn max_block(&self) -> Duration {
        self.max_block
    }

    /// Returns the buffer memory size.
    #[inline]
    pub fn buffer_memory(&self) -> usize {
        self.buffer_memory
    }

    /// Returns the metadata max age.
    #[inline]
    pub fn metadata_max_age(&self) -> Duration {
        self.metadata_max_age
    }

    /// Returns the authentication configuration, if set.
    #[inline]
    pub fn auth(&self) -> Option<&AuthConfig> {
        self.auth.as_ref()
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

    /// Set authentication configuration.
    ///
    /// Enables TLS and/or SASL authentication for all connections.
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = Some(auth);
        self
    }

    /// Set request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set number of retries.
    pub fn retries(mut self, retries: u32) -> Self {
        self.config.retries = retries;
        self
    }

    /// Set retry backoff duration.
    pub fn retry_backoff(mut self, backoff: Duration) -> Self {
        self.config.retry_backoff = backoff;
        self
    }

    /// Set max in-flight requests per connection.
    pub fn max_in_flight(mut self, max: usize) -> Self {
        self.config.max_in_flight = max;
        self
    }

    /// Set whether to enable idempotent producer.
    #[deprecated(
        since = "0.2.0",
        note = "Use TransactionalProducer for idempotent/exactly-once semantics"
    )]
    pub fn enable_idempotence(mut self, enable: bool) -> Self {
        self.config.enable_idempotence = enable;
        self
    }

    /// Set max block time when send buffer is full.
    pub fn max_block(mut self, duration: Duration) -> Self {
        self.config.max_block = duration;
        self
    }

    /// Set buffer memory size in bytes.
    pub fn buffer_memory(mut self, bytes: usize) -> Self {
        self.config.buffer_memory = bytes;
        self
    }

    /// Set metadata max age before refresh.
    pub fn metadata_max_age(mut self, duration: Duration) -> Self {
        self.config.metadata_max_age = duration;
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

    #[test]
    fn test_config_builder_request_timeout() {
        let config = ProducerConfig::builder()
            .request_timeout(Duration::from_secs(60))
            .build();
        assert_eq!(
            config.request_timeout,
            Duration::from_secs(60),
            "request_timeout should be set by builder"
        );
    }

    #[test]
    fn test_config_builder_max_in_flight() {
        let config = ProducerConfig::builder().max_in_flight(10).build();
        assert_eq!(
            config.max_in_flight, 10,
            "max_in_flight should be set by builder"
        );
    }

    #[test]
    fn test_config_builder_metadata_max_age() {
        let config = ProducerConfig::builder()
            .metadata_max_age(Duration::from_secs(120))
            .build();
        assert_eq!(
            config.metadata_max_age,
            Duration::from_secs(120),
            "metadata_max_age should be set by builder"
        );
    }

    // ── R14: Acks::from_i16 known values ──

    #[test]
    fn test_acks_from_i16_known_values() {
        assert_eq!(Acks::from_i16(0), Acks::None);
        assert_eq!(Acks::from_i16(1), Acks::Leader);
        assert_eq!(Acks::from_i16(-1), Acks::All);
    }

    #[test]
    fn test_acks_from_i16_unknown_defaults_to_all() {
        // Unknown values should default to All (safest default)
        assert_eq!(Acks::from_i16(2), Acks::All);
        assert_eq!(Acks::from_i16(99), Acks::All);
        assert_eq!(Acks::from_i16(-2), Acks::All);
    }

    #[test]
    fn test_acks_roundtrip() {
        assert_eq!(Acks::from_i16(Acks::None.to_i16()), Acks::None);
        assert_eq!(Acks::from_i16(Acks::Leader.to_i16()), Acks::Leader);
        assert_eq!(Acks::from_i16(Acks::All.to_i16()), Acks::All);
    }
}
