//! Producer configuration.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use crate::auth::AuthConfig;
use crate::dlq::DeadLetterQueue;
use crate::error::{KrafkaError, Result};
use crate::metadata::MetadataRecoveryStrategy;
use crate::protocol::Compression;

/// Required acknowledgments for produce requests.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Acks {
    /// Don't wait for any acknowledgment.
    None,
    /// Wait for leader acknowledgment.
    Leader,
    /// Wait for all in-sync replicas.
    #[default]
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
    /// Per-topic compression overrides.
    ///
    /// When a topic name is present in this map, its compression type takes
    /// precedence over the global [`compression`](Self::compression) setting.
    /// Use [`ProducerConfigBuilder::topic_compression`] to populate this map.
    pub(crate) topic_compression: HashMap<String, Compression>,
    /// Batch size in bytes.
    pub(crate) batch_size: usize,
    /// Time to wait before sending a batch.
    pub(crate) linger: Duration,
    /// Request timeout.
    pub(crate) request_timeout: Duration,
    /// Total delivery timeout for a record, including retries and time spent queued.
    pub(crate) delivery_timeout: Duration,
    /// Number of retries.
    ///
    /// Defaults to `u32::MAX` (effectively unlimited). The retry loop is
    /// always bounded by [`delivery_timeout`](ProducerConfig::delivery_timeout),
    /// which is enforced to be greater than zero. Setting `retries = u32::MAX`
    /// **without** a finite `delivery_timeout` would create an infinite loop;
    /// use a finite retry count when disabling the delivery timeout.
    pub(crate) retries: u32,
    /// Time between retries.
    pub(crate) retry_backoff: Duration,
    /// Max in-flight requests per connection.
    pub(crate) max_in_flight: usize,
    /// Maximum encoded Kafka request frame size in bytes.
    pub(crate) max_request_size: usize,
    /// Enable idempotent producer.
    ///
    /// When `true` (the default, matching KIP-679 / Kafka 3.0+), the producer
    /// obtains a Producer ID from the broker and tracks sequence numbers per
    /// partition to guarantee exactly-once delivery within a session.
    ///
    /// Requires `acks = All`. `max_in_flight` is automatically capped to 5
    /// at build time if a higher value is configured.
    pub(crate) idempotent: bool,
    /// Max block time when buffer is full.
    pub(crate) max_block: Duration,
    /// Buffer memory size.
    pub(crate) buffer_memory: usize,
    /// Metadata max age.
    pub(crate) metadata_max_age: Duration,
    /// Topic cache TTL for partial metadata refreshes.
    pub(crate) metadata_topic_cache_ttl: Option<Duration>,
    /// Metadata recovery strategy (KIP-899).
    ///
    /// When set to [`MetadataRecoveryStrategy::Rebootstrap`], the producer
    /// falls back to bootstrap servers if metadata refresh fails for longer
    /// than [`metadata_recovery_rebootstrap_trigger`](Self::metadata_recovery_rebootstrap_trigger).
    pub(crate) metadata_recovery_strategy: MetadataRecoveryStrategy,
    /// Duration after which failing metadata refreshes trigger a rebootstrap
    /// (KIP-899). Only effective with
    /// [`MetadataRecoveryStrategy::Rebootstrap`]. Default: 300 s.
    pub(crate) metadata_recovery_rebootstrap_trigger: Duration,
    /// Authentication configuration (optional).
    pub(crate) auth: Option<AuthConfig>,
    /// SOCKS5 proxy configuration (optional).
    #[cfg(feature = "socks5")]
    pub(crate) proxy: Option<crate::network::ProxyConfig>,
    /// Optional dead-letter queue for permanently-failed records.
    ///
    /// When set, records that exhaust all retries (or encounter a
    /// non-retriable error) on the direct-send path are routed to this DLQ
    /// before the error is returned to the caller.
    pub(crate) dead_letter_queue: Option<Arc<dyn DeadLetterQueue>>,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            client_id: "krafka".to_string(),
            acks: Acks::All,
            compression: Compression::None,
            topic_compression: HashMap::new(),
            batch_size: 16384,
            linger: Duration::ZERO,
            request_timeout: Duration::from_secs(30),
            delivery_timeout: Duration::from_secs(120),
            retries: u32::MAX,
            retry_backoff: Duration::from_millis(100),
            max_in_flight: 5,
            max_request_size: crate::protocol::MAX_MESSAGE_SIZE,
            idempotent: true,
            max_block: Duration::from_secs(60),
            buffer_memory: 32 * 1024 * 1024, // 32 MB
            metadata_max_age: Duration::from_secs(300),
            metadata_topic_cache_ttl: Some(Duration::from_secs(300)),
            metadata_recovery_strategy: MetadataRecoveryStrategy::Rebootstrap,
            metadata_recovery_rebootstrap_trigger: Duration::from_secs(300),
            auth: None,
            #[cfg(feature = "socks5")]
            proxy: None,
            dead_letter_queue: None,
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

    /// Returns the effective compression for a given topic.
    ///
    /// If a per-topic override was configured via
    /// [`ProducerConfigBuilder::topic_compression`], that value is returned;
    /// otherwise the global [`compression()`](Self::compression) setting is used.
    #[inline]
    pub fn compression_for(&self, topic: &str) -> Compression {
        self.topic_compression
            .get(topic)
            .copied()
            .unwrap_or(self.compression)
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

    /// Returns the total delivery timeout.
    #[inline]
    pub fn delivery_timeout(&self) -> Duration {
        self.delivery_timeout
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

    /// Returns the maximum encoded request frame size in bytes.
    #[inline]
    pub fn max_request_size(&self) -> usize {
        self.max_request_size
    }

    /// Returns whether idempotent production is enabled.
    #[inline]
    pub fn idempotent(&self) -> bool {
        self.idempotent
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

    /// Returns the topic cache TTL for partial metadata refreshes.
    #[inline]
    pub fn metadata_topic_cache_ttl(&self) -> Option<Duration> {
        self.metadata_topic_cache_ttl
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

    /// Returns the SOCKS5 proxy configuration, if set.
    #[cfg(feature = "socks5")]
    #[inline]
    pub fn proxy(&self) -> Option<&crate::network::ProxyConfig> {
        self.proxy.as_ref()
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

    /// Set a per-topic compression override.
    ///
    /// Records destined for `topic` will be compressed with `compression`
    /// regardless of the global [`compression`](Self::compression) setting.
    /// Call this method multiple times to configure different codecs per topic.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use krafka::producer::{Producer, ProducerConfig};
    /// use krafka::protocol::Compression;
    ///
    /// let config = ProducerConfig::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .compression(Compression::Gzip)         // default for all topics
    ///     .topic_compression("high-volume", Compression::Lz4)  // override for one topic
    ///     .build()?;
    /// ```
    pub fn topic_compression(mut self, topic: impl Into<String>, compression: Compression) -> Self {
        self.config
            .topic_compression
            .insert(topic.into(), compression);
        self
    }

    /// Set batch size.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Set linger time.
    ///
    /// The accumulator waits up to `duration` before sealing a batch, giving
    /// more records time to join and improving throughput at the cost of
    /// additional latency. The default is [`Duration::ZERO`] (no linger —
    /// send immediately). For sustained high-throughput workloads, values in
    /// the range of 1–100 ms are typical.
    ///
    /// # Trade-off
    ///
    /// Larger linger values reduce the number of Produce RPCs and increase
    /// batch fill rate, but add a fixed latency floor to every record.  Do not
    /// set linger without also tuning [`delivery_timeout`](Self::delivery_timeout)
    /// to be significantly larger than the linger value.
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

    /// Set SOCKS5 proxy configuration.
    ///
    /// Routes all broker connections through the specified SOCKS5 proxy.
    #[cfg(feature = "socks5")]
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.config.proxy = Some(proxy);
        self
    }

    /// Set request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set the total delivery timeout.
    ///
    /// This bounds the total time a record may spend queued and retried before
    /// it is failed locally, similar to Kafka's `delivery.timeout.ms`.
    pub fn delivery_timeout(mut self, timeout: Duration) -> Self {
        self.config.delivery_timeout = timeout;
        self
    }

    /// Set number of retries.
    ///
    /// The default is `u32::MAX` (effectively unlimited), which is safe
    /// because the retry loop is always bounded by [`delivery_timeout`](Self::delivery_timeout).
    /// Use a finite value when you want to limit retries independently of
    /// the delivery timeout.
    ///
    /// # Note on idempotent producers
    ///
    /// When idempotent production is enabled, in-order retries are guaranteed
    /// by sequence numbers, so `retries > 0` is the expected setting.
    /// Setting `retries = 0` disables retries entirely — combine with a short
    /// `delivery_timeout` for fire-and-forget semantics.
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
    ///
    /// When idempotent production is enabled, this value is automatically
    /// capped to 5 at build time (per the Kafka protocol guarantee), with an
    /// `info!` log if a higher value was explicitly configured.
    pub fn max_in_flight(mut self, max: usize) -> Self {
        self.config.max_in_flight = max;
        self
    }

    /// Set the maximum encoded Kafka request frame size in bytes.
    pub fn max_request_size(mut self, bytes: usize) -> Self {
        self.config.max_request_size = bytes;
        self
    }

    /// Enable or disable idempotent production.
    ///
    /// Idempotent production is enabled by default (matching KIP-679 / Kafka 3.0+).
    /// When enabled, the producer obtains a Producer ID from the broker and
    /// attaches sequence numbers to every batch, allowing the broker to
    /// de-duplicate retries.
    ///
    /// Requires `acks = All`. If `max_in_flight` exceeds 5, it is automatically
    /// capped to 5 at build time (with an `info!` log), matching Java client
    /// and librdkafka behaviour.
    pub fn idempotent(mut self, enable: bool) -> Self {
        self.config.idempotent = enable;
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

    /// Set a dead-letter queue for permanently-failed records.
    ///
    /// When set, records that exhaust all retries (or encounter a non-retriable
    /// error) on the direct-send path are routed to `dlq` before the error is
    /// returned to the caller. The DLQ call is fire-and-forget: errors during
    /// routing are handled by the implementation.
    ///
    /// The DLQ is **not** invoked for accumulator-path failures (linger > 0)
    /// because batched records are not individually available after batching.
    /// For accumulator-path DLQ integration, use the `on_acknowledgement`
    /// interceptor hook together with a `DeadLetterQueue` implementation.
    pub fn dead_letter_queue(mut self, dlq: Arc<dyn DeadLetterQueue>) -> Self {
        self.config.dead_letter_queue = Some(dlq);
        self
    }

    /// Build the config.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid:
    /// - `bootstrap_servers` must be non-empty
    /// - `batch_size` must be >= 1
    /// - `max_in_flight` must be >= 1
    /// - `max_request_size` must be >= 1
    /// - `delivery_timeout` must be greater than zero
    /// - Idempotent mode requires `acks = All`; `max_in_flight` is auto-capped to 5
    /// - `batch_size` must not exceed `buffer_memory` (when `buffer_memory > 0`)
    /// - `batch_size` must not exceed `max_request_size`
    pub fn build(mut self) -> Result<ProducerConfig> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap_servers must not be empty"));
        }
        // Validate client_id against the Kafka wire limit for KafkaString (i16::MAX).
        const MAX_KAFKA_STRING_LEN: usize = i16::MAX as usize;
        if self.config.client_id.len() > MAX_KAFKA_STRING_LEN {
            return Err(KrafkaError::config(format!(
                "client_id is {} bytes, exceeding the Kafka wire limit of {MAX_KAFKA_STRING_LEN}",
                self.config.client_id.len()
            )));
        }
        if self.config.batch_size == 0 {
            return Err(KrafkaError::config(format!(
                "batch_size must be >= 1 (got {})",
                self.config.batch_size
            )));
        }
        if self.config.max_in_flight == 0 {
            return Err(KrafkaError::config(format!(
                "max_in_flight must be >= 1 (got {})",
                self.config.max_in_flight
            )));
        }
        if self.config.max_request_size == 0 {
            return Err(KrafkaError::config("max_request_size must be >= 1"));
        }
        if self.config.delivery_timeout.is_zero() {
            return Err(KrafkaError::config(
                "delivery_timeout must be greater than zero",
            ));
        }
        if self.config.idempotent {
            if self.config.retries == 0 {
                return Err(KrafkaError::config(
                    "idempotent producer requires retries > 0",
                ));
            }
            if self.config.acks != Acks::All {
                return Err(KrafkaError::config(format!(
                    "idempotent producer requires acks = All (got {:?})",
                    self.config.acks
                )));
            }
            // Idempotent production requires max_in_flight ≤ 5 per the Kafka
            // protocol specification (KIP-679).  Rather than rejecting the
            // configuration with an error, we silently cap it to 5 — the same
            // behaviour as the Java client, librdkafka, and kafka-go — and emit
            // an info-level message so operators can see the adjustment.
            if self.config.max_in_flight > 5 {
                tracing::info!(
                    configured = self.config.max_in_flight,
                    effective = 5,
                    "idempotent producer requires max_in_flight ≤ 5; capping automatically"
                );
                self.config.max_in_flight = 5;
            }
            // Warn when idempotency is enabled without a transactional_id:
            // idempotent producers prevent duplicates within a session but do NOT
            // fence zombie producers after a crash/restart. Only a TransactionalProducer
            // with a stable transactional_id provides full zombie fencing (KIP-360).
            // Emitted at warn! via a OnceLock so it fires once per process and is
            // visible to operators without spamming logs.
            static IDEMPOTENT_NO_TXN_WARNED: OnceLock<()> = OnceLock::new();
            IDEMPOTENT_NO_TXN_WARNED.get_or_init(|| {
                tracing::warn!(
                    "Idempotent producer enabled without a transactional_id. \
                     This provides per-session duplicate detection (KIP-679) but not zombie \
                     fencing. Use TransactionalProducer with a stable transactional_id for \
                     exactly-once end-to-end guarantees across producer restarts (KIP-360)."
                );
            });
        }
        if self.config.buffer_memory > 0 && self.config.batch_size > self.config.buffer_memory {
            return Err(KrafkaError::config(format!(
                "batch_size must not exceed buffer_memory (got batch_size={}, buffer_memory={})",
                self.config.batch_size, self.config.buffer_memory
            )));
        }
        if self.config.batch_size > self.config.max_request_size {
            return Err(KrafkaError::config(format!(
                "batch_size must not exceed max_request_size (got batch_size={}, max_request_size={})",
                self.config.batch_size, self.config.max_request_size
            )));
        }
        // Warn when linger >= delivery_timeout — records would time out before
        // the linger period expires, making lingering counterproductive.
        if self.config.linger >= self.config.delivery_timeout {
            tracing::warn!(
                linger_ms = self.config.linger.as_millis(),
                delivery_timeout_ms = self.config.delivery_timeout.as_millis(),
                "linger >= delivery_timeout: records may expire before they are sent"
            );
        }
        // Warn when retries = u32::MAX — the retry loop is bounded by
        // delivery_timeout (validated non-zero above), but a future caller
        // that disables that guard would create an infinite loop.
        if self.config.retries == u32::MAX {
            tracing::debug!(
                "retries = u32::MAX; retry loop is bounded by delivery_timeout ({:?})",
                self.config.delivery_timeout
            );
        }
        Ok(self.config)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
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
        assert_eq!(config.acks, Acks::All);
        assert!(config.idempotent);
        assert_eq!(config.compression, Compression::None);
        assert_eq!(config.batch_size, 16384);
        assert_eq!(config.max_request_size, crate::protocol::MAX_MESSAGE_SIZE);
        assert_eq!(config.delivery_timeout, Duration::from_secs(120));
        assert_eq!(config.retries, u32::MAX);
        assert_eq!(
            config.metadata_topic_cache_ttl,
            Some(Duration::from_secs(300))
        );
    }

    #[test]
    fn test_config_builder() {
        let config = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .client_id("test")
            .acks(Acks::All)
            .compression(Compression::Lz4)
            .batch_size(32768)
            .max_request_size(65536)
            .build()
            .unwrap();

        assert_eq!(config.bootstrap_servers, "localhost:9092");
        assert_eq!(config.client_id, "test");
        assert_eq!(config.acks, Acks::All);
        assert_eq!(config.compression, Compression::Lz4);
        assert_eq!(config.batch_size, 32768);
        assert_eq!(config.max_request_size, 65536);
    }

    #[test]
    fn test_config_builder_request_timeout() {
        let config = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .request_timeout(Duration::from_secs(60))
            .build()
            .unwrap();
        assert_eq!(
            config.request_timeout,
            Duration::from_secs(60),
            "request_timeout should be set by builder"
        );
    }

    #[test]
    fn test_config_builder_delivery_timeout() {
        let config = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .delivery_timeout(Duration::from_secs(45))
            .build()
            .unwrap();
        assert_eq!(config.delivery_timeout(), Duration::from_secs(45));
    }

    #[test]
    fn test_config_builder_max_in_flight() {
        // max_in_flight=10 requires idempotent=false
        let config = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .idempotent(false)
            .max_in_flight(10)
            .build()
            .unwrap();
        assert_eq!(
            config.max_in_flight, 10,
            "max_in_flight should be set by builder"
        );
    }

    #[test]
    fn test_config_builder_metadata_max_age() {
        let config = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .metadata_max_age(Duration::from_secs(120))
            .build()
            .unwrap();
        assert_eq!(
            config.metadata_max_age,
            Duration::from_secs(120),
            "metadata_max_age should be set by builder"
        );
    }

    #[test]
    fn test_config_builder_metadata_topic_cache_ttl() {
        let config = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .metadata_topic_cache_ttl(Duration::from_secs(600))
            .build()
            .unwrap();
        assert_eq!(
            config.metadata_topic_cache_ttl(),
            Some(Duration::from_secs(600))
        );
    }

    #[test]
    fn test_config_builder_disable_metadata_topic_cache_ttl() {
        let config = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .disable_metadata_topic_cache_ttl()
            .build()
            .unwrap();
        assert_eq!(config.metadata_topic_cache_ttl(), None);
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

    #[cfg(feature = "socks5")]
    #[test]
    fn test_config_builder_proxy_round_trip() {
        let config = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .proxy(crate::network::ProxyConfig::new("proxy:1080"))
            .build()
            .unwrap();
        let proxy = config.proxy().expect("proxy should be set");
        assert_eq!(proxy.address(), "proxy:1080");
    }

    #[test]
    fn test_config_default_recovery_strategy() {
        let config = ProducerConfig::default();
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
        let config = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .metadata_recovery_strategy(MetadataRecoveryStrategy::Rebootstrap)
            .metadata_recovery_rebootstrap_trigger(Duration::from_secs(120))
            .build()
            .unwrap();
        assert_eq!(
            config.metadata_recovery_strategy(),
            MetadataRecoveryStrategy::Rebootstrap,
        );
        assert_eq!(
            config.metadata_recovery_rebootstrap_trigger(),
            Duration::from_secs(120),
        );
    }

    #[test]
    fn test_config_builder_rejects_zero_batch_size() {
        let err = ProducerConfig::builder().batch_size(0).build();
        assert!(err.is_err());
    }

    #[test]
    fn test_config_builder_rejects_zero_max_in_flight() {
        let err = ProducerConfig::builder().max_in_flight(0).build();
        assert!(err.is_err());
    }

    #[test]
    fn test_config_builder_rejects_zero_max_request_size() {
        let err = ProducerConfig::builder().max_request_size(0).build();
        assert!(err.is_err());
    }

    #[test]
    fn test_config_builder_rejects_zero_delivery_timeout() {
        let err = ProducerConfig::builder()
            .delivery_timeout(Duration::ZERO)
            .build();
        assert!(err.is_err());
    }

    #[test]
    fn test_config_builder_rejects_idempotent_without_retries() {
        let err = ProducerConfig::builder().retries(0).build();
        assert!(err.is_err());
    }

    #[test]
    fn test_config_builder_rejects_idempotent_with_acks_leader() {
        let err = ProducerConfig::builder()
            .idempotent(true)
            .acks(Acks::Leader)
            .build();
        assert!(err.is_err());
    }

    #[test]
    fn test_config_builder_autocaps_idempotent_with_high_in_flight() {
        // max_in_flight > 5 with idempotent enabled: auto-capped to 5, not an error.
        let config = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .idempotent(true)
            .max_in_flight(10)
            .build()
            .expect("should auto-cap, not error");
        assert_eq!(config.max_in_flight(), 5);
    }

    #[test]
    fn test_config_builder_idempotent_keeps_low_in_flight() {
        // max_in_flight ≤ 5 is preserved exactly.
        let config = ProducerConfig::builder()
            .bootstrap_servers("localhost:9092")
            .idempotent(true)
            .max_in_flight(3)
            .build()
            .expect("should succeed");
        assert_eq!(config.max_in_flight(), 3);
    }

    #[test]
    fn test_config_builder_rejects_batch_exceeding_buffer() {
        let err = ProducerConfig::builder()
            .batch_size(1024)
            .buffer_memory(512)
            .build();
        assert!(err.is_err());
    }

    #[test]
    fn test_config_builder_rejects_batch_exceeding_max_request_size() {
        let err = ProducerConfig::builder()
            .batch_size(1024)
            .max_request_size(512)
            .build();
        assert!(err.is_err());
    }

    #[test]
    fn test_config_builder_rejects_empty_bootstrap_servers() {
        let err = ProducerConfig::builder().bootstrap_servers("").build();
        assert!(
            err.is_err(),
            "empty bootstrap_servers should be rejected at build time"
        );
        assert!(
            err.unwrap_err().to_string().contains("bootstrap_servers"),
            "error message should mention bootstrap_servers"
        );
    }
}
