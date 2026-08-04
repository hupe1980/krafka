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
    /// Known values: `0` = `None`, `1` = `Leader`, `-1` = `All`.
    /// Returns `None` for unknown values instead of silently falling back to a
    /// default — callers must decide how to handle invalid wire values.
    #[inline]
    pub fn from_i16(value: i16) -> Option<Self> {
        match value {
            0 => Some(Acks::None),
            1 => Some(Acks::Leader),
            -1 => Some(Acks::All),
            _ => None,
        }
    }
}

/// Producer configuration.
///
/// Produced by [`Producer::builder()`](crate::producer::Producer::builder), whose
/// [`build_config`](crate::producer::ProducerBuilder::build_config) terminal
/// returns it without connecting. [`Default::default()`] also works.
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
    /// Compression level, or `None` for the codec's own default.
    ///
    /// Applies to whichever codec is active, including per-topic overrides.
    /// Kafka's own configuration splits this per codec
    /// (`compression.gzip.level`, `compression.zstd.level`, KIP-390); a single
    /// value is used here because krafka validates it against the *selected*
    /// codec at build time, so the per-codec split would only add a way to set
    /// a level for a codec you are not using.
    pub(crate) compression_level: Option<i32>,
    /// Per-topic compression overrides.
    ///
    /// When a topic name is present in this map, its compression type takes
    /// precedence over the global [`compression`](Self::compression) setting.
    /// Use [`ProducerBuilder::topic_compression`](crate::producer::ProducerBuilder::topic_compression) to populate this map.
    pub(crate) topic_compression: HashMap<String, Compression>,
    /// Batch size in bytes.
    pub(crate) batch_size: usize,
    /// Time to wait before sending a batch.
    pub(crate) linger: Duration,
    /// Request timeout.
    pub(crate) request_timeout: Duration,
    /// Time allowed for TCP establishment to one broker.
    pub(crate) connect_timeout: Duration,
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
    /// Socket- and pool-level transport tuning.
    ///
    /// Defaults reproduce krafka's historical behaviour; see
    /// [`TransportConfig`](crate::network::TransportConfig).
    pub(crate) transport: crate::network::TransportConfig,
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
            compression_level: None,
            topic_compression: HashMap::new(),
            batch_size: 16384,
            linger: Duration::ZERO,
            request_timeout: Duration::from_secs(30),
            connect_timeout: crate::network::DEFAULT_CONNECT_TIMEOUT,
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
            transport: crate::network::TransportConfig::default(),
            dead_letter_queue: None,
        }
    }
}

impl ProducerConfig {
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

    /// Returns the configured compression level, or `None` for the codec
    /// default.
    #[inline]
    pub fn compression_level(&self) -> Option<i32> {
        self.compression_level
    }

    /// Returns the effective compression for a given topic.
    ///
    /// If a per-topic override was configured via
    /// [`ProducerBuilder::topic_compression`](crate::producer::ProducerBuilder::topic_compression), that value is returned;
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

    /// Returns the connect timeout.
    #[inline]
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
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

/// Validate and normalise a [`ProducerConfig`].
///
/// # Why this is a free function
///
/// There used to be two producer builders: a public [`ProducerBuilder`] that
/// every caller uses, and a `ProducerConfigBuilder` that nothing outside this
/// crate's own tests ever touched. Each carried its own copy of these rules,
/// and the copies had **diverged**: the public path — the only one anybody ran
/// — silently skipped the client-id length limit, the infinite-retry-loop
/// guard, the compression-codec availability checks (global and per topic), the
/// retry-budget warning and the linger warning.
///
/// A `Producer::builder().compression(Zstd)` without the `zstd` feature
/// therefore *built successfully* and failed on the first send.
///
/// The second builder is gone and this is the single place the rules live.
/// Both the synchronous
/// [`build_config`](ProducerBuilder::build_config) terminal and the async
/// [`build`](ProducerBuilder::build) call it, so they cannot disagree again.
///
/// # Normalisation
///
/// Takes `&mut` because validation is not purely a predicate: an idempotent
/// producer's `max_in_flight` is capped to 5 here (KIP-679), matching the Java
/// client and librdkafka, rather than being rejected.
///
/// `has_shared_pool` relaxes the `bootstrap_servers` requirement: a client
/// built with [`with_client`](ProducerBuilder::with_client) inherits an
/// already-connected pool and has no bootstrap list of its own.
pub(crate) fn validate(config: &mut ProducerConfig, has_shared_pool: bool) -> Result<()> {
    if !has_shared_pool && config.bootstrap_servers.is_empty() {
        return Err(KrafkaError::config("bootstrap_servers must not be empty"));
    }

    // Validate client_id against the Kafka wire limit for KafkaString (i16::MAX).
    const MAX_KAFKA_STRING_LEN: usize = i16::MAX as usize;
    if config.client_id.len() > MAX_KAFKA_STRING_LEN {
        return Err(KrafkaError::config(format!(
            "client_id is {} bytes, exceeding the Kafka wire limit of {MAX_KAFKA_STRING_LEN}",
            config.client_id.len()
        )));
    }
    if config.batch_size == 0 {
        return Err(KrafkaError::config(format!(
            "batch_size must be >= 1 (got {})",
            config.batch_size
        )));
    }
    if config.max_in_flight == 0 {
        return Err(KrafkaError::config(format!(
            "max_in_flight must be >= 1 (got {})",
            config.max_in_flight
        )));
    }
    if config.max_request_size == 0 {
        return Err(KrafkaError::config("max_request_size must be >= 1"));
    }
    if config.delivery_timeout.is_zero() {
        return Err(KrafkaError::config(
            "delivery_timeout must be greater than zero",
        ));
    }
    // Reject the combination of delivery_timeout = Duration::MAX and retries = u32::MAX.
    // Both individually have well-defined semantics (MAX delivery window / unlimited retries),
    // but together they create an infinite retry loop: the delivery deadline never expires
    // so the retry counter is the only termination condition — but that counter also never
    // expires. This combination is almost certainly a misconfiguration. If you genuinely
    // want unlimited retries, set a finite delivery_timeout.
    if config.delivery_timeout == Duration::MAX && config.retries == u32::MAX {
        return Err(KrafkaError::config(
            "delivery_timeout = Duration::MAX combined with retries = u32::MAX creates \
             an infinite retry loop; set a finite delivery_timeout or reduce retries",
        ));
    }
    // Reject a compression codec that was not compiled in. This gives a clear
    // build-time-equivalent error at producer construction rather than waiting
    // until the first message is sent to discover the feature is missing.
    if !config.compression.is_available() {
        let feature = config.compression.required_feature().unwrap_or("unknown");
        return Err(KrafkaError::config(format!(
            "compression codec {:?} requires the `{feature}` Cargo feature; \
             either enable the feature or choose a different compression codec",
            config.compression
        )));
    }
    // A compression level that the selected codec cannot use is a
    // configuration error, not something to ignore: an operator who sets
    // `compression_level(9)` alongside Snappy believes they tuned something.
    if let Some(level) = config.compression_level {
        let mut codecs: Vec<(Option<&str>, Compression)> = vec![(None, config.compression)];
        for (topic, codec) in &config.topic_compression {
            codecs.push((Some(topic.as_str()), *codec));
        }
        for (topic, codec) in codecs {
            let where_ = topic.map_or_else(String::new, |t| format!(" (topic {t:?})"));
            let Some(range) = codec.level_range().filter(|_| codec.supports_level()) else {
                return Err(KrafkaError::config(format!(
                    "compression_level {level} was set but codec {codec:?}{where_} takes                      no level; krafka encodes Snappy with `snap` and LZ4 with `lz4_flex`,                      neither of which exposes one. Remove compression_level or select                      Gzip or Zstd"
                )));
            };
            if !range.contains(&level) {
                return Err(KrafkaError::config(format!(
                    "compression_level {level} is out of range for codec {codec:?}{where_};                      valid levels are {}..={}",
                    range.start(),
                    range.end()
                )));
            }
        }
    }

    // Same check for per-topic compression overrides.
    for (topic, codec) in &config.topic_compression {
        if !codec.is_available() {
            let feature = codec.required_feature().unwrap_or("unknown");
            return Err(KrafkaError::config(format!(
                "per-topic compression codec {:?} for topic {topic:?} requires the \
                 `{feature}` Cargo feature",
                codec
            )));
        }
    }
    // Warn when delivery_timeout is shorter than a single full retry cycle.
    // In this case some retry attempts can never complete before the deadline,
    // causing premature delivery failures without exhausting all retries.
    if config.retries > 0 {
        let min_budget = config
            .request_timeout
            .saturating_mul(config.retries.saturating_add(1));
        if config.delivery_timeout < min_budget {
            tracing::warn!(
                delivery_timeout_secs = config.delivery_timeout.as_secs_f64(),
                request_timeout_secs = config.request_timeout.as_secs_f64(),
                retries = config.retries,
                minimum_budget_secs = min_budget.as_secs_f64(),
                "delivery_timeout is shorter than request_timeout × (retries + 1); \
                 some retry attempts will be cut short by the delivery deadline"
            );
        }
    }
    if config.idempotent {
        if config.retries == 0 {
            return Err(KrafkaError::config(
                "idempotent producer requires retries > 0",
            ));
        }
        if config.acks != Acks::All {
            return Err(KrafkaError::config(format!(
                "idempotent producer requires acks = All (got {:?})",
                config.acks
            )));
        }
        // Idempotent production requires max_in_flight ≤ 5 per the Kafka
        // protocol specification (KIP-679).  Rather than rejecting the
        // configuration with an error, we silently cap it to 5 — the same
        // behaviour as the Java client, librdkafka, and kafka-go — and emit
        // an info-level message so operators can see the adjustment.
        if config.max_in_flight > 5 {
            tracing::info!(
                configured = config.max_in_flight,
                effective = 5,
                "idempotent producer requires max_in_flight ≤ 5; capping automatically"
            );
            config.max_in_flight = 5;
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
    if config.buffer_memory > 0 && config.batch_size > config.buffer_memory {
        return Err(KrafkaError::config(format!(
            "batch_size must not exceed buffer_memory (got batch_size={}, buffer_memory={})",
            config.batch_size, config.buffer_memory
        )));
    }
    if config.batch_size > config.max_request_size {
        return Err(KrafkaError::config(format!(
            "batch_size must not exceed max_request_size (got batch_size={}, max_request_size={})",
            config.batch_size, config.max_request_size
        )));
    }
    // Warn when linger >= delivery_timeout — records would time out before
    // the linger period expires, making lingering counterproductive.
    if config.linger >= config.delivery_timeout {
        tracing::warn!(
            linger_ms = config.linger.as_millis(),
            delivery_timeout_ms = config.delivery_timeout.as_millis(),
            "linger >= delivery_timeout: records may expire before they are sent"
        );
    }
    // Warn when retries = u32::MAX — the retry loop is bounded by
    // delivery_timeout (validated non-zero above), but a future caller
    // that disables that guard would create an infinite loop.
    if config.retries == u32::MAX {
        tracing::debug!(
            "retries = u32::MAX; retry loop is bounded by delivery_timeout ({:?})",
            config.delivery_timeout
        );
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use crate::producer::Producer;

    /// A level set alongside a codec that cannot use one must be rejected.
    ///
    /// Silently ignoring it is the failure mode worth guarding: an operator
    /// who sets `compression_level(9)` with Snappy believes they tuned
    /// something, and nothing would ever tell them otherwise.
    #[cfg(feature = "snappy")]
    #[test]
    fn compression_level_on_a_levelless_codec_is_rejected() {
        let err = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .compression(Compression::Snappy)
            .compression_level(Some(9))
            .build_config()
            .expect_err("Snappy takes no level");
        let msg = err.to_string();
        assert!(
            msg.contains("takes") && msg.contains("no level"),
            "the error must say the codec has no level, got: {msg}"
        );
    }

    /// An out-of-range level must be rejected rather than clamped at build
    /// time, so the operator learns the real range.
    #[cfg(feature = "gzip")]
    #[test]
    fn out_of_range_compression_level_is_rejected() {
        let err = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .compression(Compression::Gzip)
            .compression_level(Some(42))
            .build_config()
            .expect_err("gzip tops out at 9");
        assert!(
            err.to_string().contains("0..=9"),
            "the error must name the valid range, got: {err}"
        );
    }

    /// A valid level must survive validation and land on the config.
    #[cfg(feature = "zstd")]
    #[test]
    fn valid_compression_level_reaches_the_config() {
        let config = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .compression(Compression::Zstd)
            .compression_level(Some(1))
            .build_config()
            .expect("level 1 is valid for zstd");
        assert_eq!(config.compression_level(), Some(1));
    }

    /// A per-topic override must be validated too — otherwise a level valid
    /// for the default codec silently applies to a topic using another.
    #[cfg(all(feature = "zstd", feature = "snappy"))]
    #[test]
    fn per_topic_codec_is_validated_against_the_level() {
        let err = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .compression(Compression::Zstd)
            .compression_level(Some(1))
            .topic_compression("events", Compression::Snappy)
            .build_config()
            .expect_err("the per-topic Snappy override takes no level");
        assert!(
            err.to_string().contains("events"),
            "the error must name the offending topic, got: {err}"
        );
    }
    use super::*;

    // ── One validator, reachable from the only builder ───────────────────
    //
    // There used to be two producer builders, and their validation had
    // diverged: the public `Producer::builder()` — the only one anybody used —
    // skipped six checks the unused `ProducerConfigBuilder` performed. The
    // second builder is gone; these assert the checks it uniquely had now run
    // on the surviving path.

    /// A codec whose Cargo feature is not enabled must be rejected at build
    /// time. Before the builders were merged this passed validation and failed
    /// on the first `send()`, with the error surfacing from deep in the
    /// accumulator long after the misconfiguration was actionable.
    #[test]
    fn build_config_rejects_a_codec_that_is_not_compiled_in() {
        let Some(missing) = [
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ]
        .into_iter()
        .find(|c| !c.is_available()) else {
            // Every codec is compiled in for this feature set; nothing to assert.
            return;
        };

        let err = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .compression(missing)
            .build_config()
            .expect_err("an unavailable codec must be rejected")
            .to_string();
        assert!(
            err.contains("Cargo feature"),
            "the error must name the missing feature, got: {err}"
        );
    }

    /// Same rule for a per-topic override, which is a separate code path and
    /// was separately missing from the public builder.
    #[test]
    fn build_config_rejects_an_unavailable_per_topic_codec() {
        let Some(missing) = [
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ]
        .into_iter()
        .find(|c| !c.is_available()) else {
            return;
        };

        let err = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .topic_compression("high-volume", missing)
            .build_config()
            .expect_err("an unavailable per-topic codec must be rejected")
            .to_string();
        assert!(
            err.contains("Cargo feature"),
            "the error must name the missing feature, got: {err}"
        );
    }

    /// `delivery_timeout = MAX` with `retries = MAX` is an infinite retry loop:
    /// the deadline never expires, so the retry counter is the only termination
    /// condition — and it never expires either.
    #[test]
    fn build_config_rejects_the_infinite_retry_loop() {
        let err = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .idempotent(false)
            .delivery_timeout(Duration::MAX)
            .retries(u32::MAX)
            .build_config()
            .expect_err("MAX/MAX must be rejected")
            .to_string();
        assert!(err.contains("infinite retry loop"), "got: {err}");
    }

    /// An oversize `client_id` cannot be encoded as a Kafka string. Rejecting
    /// it at the builder keeps the wire encoder's panic path structurally
    /// unreachable.
    #[test]
    fn build_config_rejects_an_oversize_client_id() {
        let err = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .client_id("x".repeat(i16::MAX as usize + 1))
            .build_config()
            .expect_err("an oversize client_id must be rejected")
            .to_string();
        assert!(err.contains("client_id"), "got: {err}");
    }

    /// Re-validating an already-validated config is idempotent, so `build`
    /// running the validator after `build_config` did cannot change the answer.
    #[test]
    fn validation_is_idempotent() {
        let mut config = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .build_config()
            .expect("a default config is valid");
        validate(&mut config, false).expect("re-validation must succeed");
    }

    /// A shared `KrafkaClient` supplies the pool, so an empty bootstrap list is
    /// legitimate on that path and must not be rejected.
    #[test]
    fn validate_allows_an_empty_bootstrap_list_with_a_shared_pool() {
        let mut config = ProducerConfig {
            bootstrap_servers: String::new(),
            ..ProducerConfig::default()
        };
        assert!(validate(&mut config, true).is_ok());
        assert!(validate(&mut config, false).is_err());
    }

    /// A setter that stores into a field nobody reads is exactly the defect
    /// `TransportConfig` was introduced to fix, so the round-trip is asserted
    /// rather than assumed.
    #[test]
    fn test_config_builder_transport_round_trips() {
        let transport = crate::network::TransportConfig::builder()
            .tcp_keepalive(Some(std::time::Duration::from_secs(11)))
            .max_connections(Some(7))
            .build()
            .expect("valid transport config");

        let config = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .transport(transport)
            .build_config()
            .expect("config builds");

        assert_eq!(
            config.transport.tcp_keepalive(),
            Some(std::time::Duration::from_secs(11))
        );
        assert_eq!(config.transport.max_connections(), Some(7));
    }

    #[test]
    fn test_acks_to_i16() {
        assert_eq!(Acks::None.to_i16(), 0);
        assert_eq!(Acks::Leader.to_i16(), 1);
        assert_eq!(Acks::All.to_i16(), -1);
    }

    #[test]
    fn test_acks_from_i16() {
        assert_eq!(Acks::from_i16(0), Some(Acks::None));
        assert_eq!(Acks::from_i16(1), Some(Acks::Leader));
        assert_eq!(Acks::from_i16(-1), Some(Acks::All));
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
    // `Lz4` is only a valid setting when its codec is compiled in — validation
    // now rejects an unavailable codec on this path, which it did not before
    // the two producer builders were merged.
    #[cfg(feature = "lz4")]
    fn test_config_builder() {
        let config = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .client_id("test")
            .acks(Acks::All)
            .compression(Compression::Lz4)
            .batch_size(32768)
            .max_request_size(65536)
            .build_config()
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
        let config = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .request_timeout(Duration::from_secs(60))
            .build_config()
            .unwrap();
        assert_eq!(
            config.request_timeout,
            Duration::from_secs(60),
            "request_timeout should be set by builder"
        );
    }

    #[test]
    fn test_config_builder_delivery_timeout() {
        let config = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .delivery_timeout(Duration::from_secs(45))
            .build_config()
            .unwrap();
        assert_eq!(config.delivery_timeout(), Duration::from_secs(45));
    }

    #[test]
    fn test_config_builder_infinite_retry_loop_is_err() {
        // Duration::MAX + retries=u32::MAX = infinite retry loop — must be rejected
        let err = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .idempotent(false)
            .delivery_timeout(Duration::MAX)
            .retries(u32::MAX)
            .build_config()
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("infinite retry loop"),
            "expected 'infinite retry loop' in error, got: {msg}"
        );
    }

    #[test]
    fn test_config_builder_max_in_flight() {
        // max_in_flight=10 requires idempotent=false
        let config = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .idempotent(false)
            .max_in_flight(10)
            .build_config()
            .unwrap();
        assert_eq!(
            config.max_in_flight, 10,
            "max_in_flight should be set by builder"
        );
    }

    #[test]
    fn test_config_builder_metadata_max_age() {
        let config = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .metadata_max_age(Duration::from_secs(120))
            .build_config()
            .unwrap();
        assert_eq!(
            config.metadata_max_age,
            Duration::from_secs(120),
            "metadata_max_age should be set by builder"
        );
    }

    #[test]
    fn test_config_builder_metadata_topic_cache_ttl() {
        let config = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .metadata_topic_cache_ttl(Duration::from_secs(600))
            .build_config()
            .unwrap();
        assert_eq!(
            config.metadata_topic_cache_ttl(),
            Some(Duration::from_secs(600))
        );
    }

    #[test]
    fn test_config_builder_disable_metadata_topic_cache_ttl() {
        let config = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .disable_metadata_topic_cache_ttl()
            .build_config()
            .unwrap();
        assert_eq!(config.metadata_topic_cache_ttl(), None);
    }

    // ── R14: Acks::from_i16 known values ──

    #[test]
    fn test_acks_from_i16_known_values() {
        assert_eq!(Acks::from_i16(0), Some(Acks::None));
        assert_eq!(Acks::from_i16(1), Some(Acks::Leader));
        assert_eq!(Acks::from_i16(-1), Some(Acks::All));
    }

    #[test]
    fn test_acks_from_i16_unknown_returns_none() {
        // Unknown values return None — callers decide how to handle them
        assert_eq!(Acks::from_i16(2), None);
        assert_eq!(Acks::from_i16(99), None);
        assert_eq!(Acks::from_i16(-2), None);
    }

    #[test]
    fn test_acks_roundtrip() {
        assert_eq!(Acks::from_i16(Acks::None.to_i16()), Some(Acks::None));
        assert_eq!(Acks::from_i16(Acks::Leader.to_i16()), Some(Acks::Leader));
        assert_eq!(Acks::from_i16(Acks::All.to_i16()), Some(Acks::All));
    }

    #[cfg(feature = "socks5")]
    #[test]
    fn test_config_builder_proxy_round_trip() {
        let config = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .proxy(crate::network::ProxyConfig::new("proxy:1080"))
            .build_config()
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
        let config = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .metadata_recovery_strategy(MetadataRecoveryStrategy::Rebootstrap)
            .metadata_recovery_rebootstrap_trigger(Duration::from_secs(120))
            .build_config()
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
        let err = crate::producer::Producer::builder()
            .batch_size(0)
            .build_config();
        assert!(err.is_err());
    }

    #[test]
    fn test_config_builder_rejects_zero_max_in_flight() {
        let err = crate::producer::Producer::builder()
            .max_in_flight(0)
            .build_config();
        assert!(err.is_err());
    }

    #[test]
    fn test_config_builder_rejects_zero_max_request_size() {
        let err = crate::producer::Producer::builder()
            .max_request_size(0)
            .build_config();
        assert!(err.is_err());
    }

    #[test]
    fn test_config_builder_rejects_zero_delivery_timeout() {
        let err = crate::producer::Producer::builder()
            .delivery_timeout(Duration::ZERO)
            .build_config();
        assert!(err.is_err());
    }

    #[test]
    fn test_config_builder_rejects_idempotent_without_retries() {
        let err = crate::producer::Producer::builder()
            .retries(0)
            .build_config();
        assert!(err.is_err());
    }

    #[test]
    fn test_config_builder_rejects_idempotent_with_acks_leader() {
        let err = crate::producer::Producer::builder()
            .idempotent(true)
            .acks(Acks::Leader)
            .build_config();
        assert!(err.is_err());
    }

    #[test]
    fn test_config_builder_autocaps_idempotent_with_high_in_flight() {
        // max_in_flight > 5 with idempotent enabled: auto-capped to 5, not an error.
        let config = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .idempotent(true)
            .max_in_flight(10)
            .build_config()
            .expect("should auto-cap, not error");
        assert_eq!(config.max_in_flight(), 5);
    }

    #[test]
    fn test_config_builder_idempotent_keeps_low_in_flight() {
        // max_in_flight ≤ 5 is preserved exactly.
        let config = crate::producer::Producer::builder()
            .bootstrap_servers("localhost:9092")
            .idempotent(true)
            .max_in_flight(3)
            .build_config()
            .expect("should succeed");
        assert_eq!(config.max_in_flight(), 3);
    }

    #[test]
    fn test_config_builder_rejects_batch_exceeding_buffer() {
        let err = crate::producer::Producer::builder()
            .batch_size(1024)
            .buffer_memory(512)
            .build_config();
        assert!(err.is_err());
    }

    #[test]
    fn test_config_builder_rejects_batch_exceeding_max_request_size() {
        let err = crate::producer::Producer::builder()
            .batch_size(1024)
            .max_request_size(512)
            .build_config();
        assert!(err.is_err());
    }

    #[test]
    fn test_config_builder_rejects_empty_bootstrap_servers() {
        let err = crate::producer::Producer::builder()
            .bootstrap_servers("")
            .build_config();
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
