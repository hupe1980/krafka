//! Producer configuration.

use std::collections::HashMap;
use std::time::Duration;

use crate::error::{KrafkaError, Result};
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

/// Producer settings, as the builder collected them.
#[derive(Debug, Clone)]
pub(crate) struct ProducerConfig {
    /// Required acknowledgments.
    pub(crate) acks: Acks,
    /// Compression type.
    pub(crate) compression: Compression,
    /// Compression level, or `None` for the codec's own default. Applies to
    /// whichever codec is active, including per-topic overrides, and is
    /// validated against each of them.
    pub(crate) compression_level: Option<i32>,
    /// Per-topic compression overrides, taking precedence over `compression`.
    pub(crate) topic_compression: HashMap<String, Compression>,
    /// Batch size in bytes; also how many bytes of keyless records stick to
    /// one partition before the built-in partitioner switches (KIP-794).
    pub(crate) batch_size: usize,
    /// How long a batch may wait for more records before it is sent, when its
    /// partition has nothing in flight. Default 5 ms (KIP-1030).
    pub(crate) linger: Duration,
    /// Bound from a batch's creation to its records' outcome, retries and
    /// time spent queued included. The only bound on retries; at least
    /// `linger + request_timeout`.
    pub(crate) delivery_timeout: Duration,
    /// First retry delay; doubles per retry, ±20 % jitter, capped at 1 s.
    pub(crate) retry_backoff: Duration,
    /// Maximum encoded Kafka request frame size in bytes.
    pub(crate) max_request_size: usize,
    /// Idempotent production (KIP-679 default). Requires `acks = All`.
    pub(crate) idempotent: bool,
    /// One budget for everything `send()` may block on: fetching metadata for
    /// an unresolved topic, and waiting for buffer memory (`max.block.ms`).
    /// Also bounds each transaction coordinator call.
    pub(crate) max_block: Duration,
    /// Buffer memory size.
    pub(crate) buffer_memory: usize,
    /// The rack this producer runs in (Java `client.rack`).
    pub(crate) client_rack: Option<String>,
    /// Keyless records only go to partitions led in `client_rack` (KIP-1123).
    pub(crate) partitioner_rack_aware: bool,
    /// How long the coordinator lets a transaction stay open, when set
    /// explicitly. Transactional producers only.
    pub(crate) transaction_timeout: Option<Duration>,
    /// Participate in an external two-phase commit (KIP-939). Transactional
    /// producers only.
    pub(crate) two_phase_commit: bool,
    /// Push this client's metrics to brokers that subscribe to them (KIP-714,
    /// Java `enable.metrics.push`).
    pub(crate) metrics_push: bool,
}

impl Default for ProducerConfig {
    fn default() -> Self {
        Self {
            acks: Acks::All,
            compression: Compression::None,
            compression_level: None,
            topic_compression: HashMap::new(),
            batch_size: 16384,
            linger: DEFAULT_LINGER,
            delivery_timeout: Duration::from_secs(120),
            retry_backoff: Duration::from_millis(100),
            max_request_size: crate::protocol::MAX_MESSAGE_SIZE,
            idempotent: true,
            max_block: Duration::from_secs(60),
            buffer_memory: 32 * 1024 * 1024,
            client_rack: None,
            partitioner_rack_aware: false,
            transaction_timeout: None,
            two_phase_commit: false,
            metrics_push: true,
        }
    }
}

/// The default `linger` (KIP-1030).
pub(crate) const DEFAULT_LINGER: Duration = Duration::from_millis(5);

/// The transaction timeout when none is set (Java `transaction.timeout.ms`).
pub(crate) const DEFAULT_TRANSACTION_TIMEOUT: Duration = Duration::from_secs(60);

impl ProducerConfig {
    /// The transaction timeout sent with `InitProducerId`.
    pub(crate) fn transaction_timeout(&self) -> Duration {
        self.transaction_timeout
            .unwrap_or(DEFAULT_TRANSACTION_TIMEOUT)
    }
}

/// Reject `delivery_timeout < linger + request_timeout`, as Java's
/// `configureDeliveryTimeout` does: a batch could not get even one full
/// attempt in.
pub(crate) fn validate_delivery_timeout(
    delivery_timeout: Duration,
    linger: Duration,
    request_timeout: Duration,
) -> Result<()> {
    if delivery_timeout < linger + request_timeout {
        return Err(KrafkaError::config(format!(
            "delivery_timeout ({delivery_timeout:?}) must be at least linger ({linger:?}) + \
             request_timeout ({request_timeout:?})"
        )));
    }
    Ok(())
}

/// Rack-aware partitioning needs a client rack and the built-in partitioner.
pub(crate) fn validate_partitioning(
    rack_aware: bool,
    client_rack: Option<&str>,
    custom_partitioner: bool,
) -> Result<()> {
    if !rack_aware {
        return Ok(());
    }
    if client_rack.is_none_or(str::is_empty) {
        return Err(KrafkaError::config(
            "partitioner_rack_aware requires client_rack: without the client's rack there is \
             no rack to stay in",
        ));
    }
    if custom_partitioner {
        return Err(KrafkaError::config(
            "partitioner_rack_aware has no effect with a custom partitioner; remove one of \
             partitioner_rack_aware and partitioner",
        ));
    }
    Ok(())
}

/// Validate a compression codec, its level and any per-topic overrides.
///
/// Three rules:
///
/// 1. Zstd without the `zstd` Cargo feature fails at build time, not on the
///    first `send()`.
/// 2. A level set alongside a codec that has none (Snappy, LZ4) is an error —
///    an operator who sets it believes they tuned something.
/// 3. Per-topic overrides are checked against the level too, so a level valid
///    for the default codec cannot silently apply to a topic using another.
pub(crate) fn validate_compression(
    compression: Compression,
    compression_level: Option<i32>,
    topic_compression: &HashMap<String, Compression>,
) -> Result<()> {
    // Zstd encoding needs the `zstd` Cargo feature; reject it at build time
    // rather than on the first `send()`.
    let topics = topic_compression
        .iter()
        .map(|(t, c)| (Some(t.as_str()), *c));
    for (topic, codec) in std::iter::once((None, compression)).chain(topics) {
        if !codec.is_available() {
            let where_ = topic.map_or_else(String::new, |t| format!(" for topic {t:?}"));
            return Err(KrafkaError::config(format!(
                "compression codec {codec:?}{where_} requires the `zstd` Cargo feature"
            )));
        }
    }

    // A compression level that the selected codec cannot use is a
    // configuration error, not something to ignore: an operator who sets
    // `compression_level(9)` alongside Snappy believes they tuned something.
    let Some(level) = compression_level else {
        return Ok(());
    };
    let mut codecs: Vec<(Option<&str>, Compression)> = vec![(None, compression)];
    for (topic, codec) in topic_compression {
        codecs.push((Some(topic.as_str()), *codec));
    }
    for (topic, codec) in codecs {
        let where_ = topic.map_or_else(String::new, |t| format!(" (topic {t:?})"));
        let Some(range) = codec.level_range().filter(|_| codec.supports_level()) else {
            return Err(KrafkaError::config(format!(
                "compression_level {level} was set but codec {codec:?}{where_} takes no \
                 level; krafka encodes Snappy with `snap` and LZ4 with `lz4_flex`, neither \
                 of which exposes one. Remove compression_level or select Gzip or Zstd"
            )));
        };
        if !range.contains(&level) {
            return Err(KrafkaError::config(format!(
                "compression_level {level} is out of range for codec {codec:?}{where_}; \
                 valid levels are {}..={}",
                range.start(),
                range.end()
            )));
        }
    }
    Ok(())
}

/// Validate a [`ProducerConfig`]. `request_timeout` is the handle's;
/// `transactional` says whether the producer is built with
/// `build_transactional`.
pub(crate) fn validate(
    config: &ProducerConfig,
    request_timeout: Duration,
    transactional: bool,
) -> Result<()> {
    if config.batch_size == 0 {
        return Err(KrafkaError::config(format!(
            "batch_size must be >= 1 (got {})",
            config.batch_size
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
    validate_delivery_timeout(config.delivery_timeout, config.linger, request_timeout)?;
    validate_compression(
        config.compression,
        config.compression_level,
        &config.topic_compression,
    )?;
    if transactional {
        // A transactional producer is idempotent with acks=all by definition;
        // the coordinator only guarantees atomicity over replicated writes.
        if !config.idempotent || config.acks != Acks::All {
            return Err(KrafkaError::config(
                "a transactional producer requires idempotent(true) and acks = All",
            ));
        }
        if config.transaction_timeout == Some(Duration::ZERO) {
            return Err(KrafkaError::config("transaction_timeout must be > 0"));
        }
        if config.two_phase_commit && config.transaction_timeout.is_some() {
            return Err(KrafkaError::config(
                "two_phase_commit and transaction_timeout contradict each other: under \
                 KIP-939 the coordinator holds a prepared transaction until an external \
                 coordinator decides, so no transaction timeout applies. Drop one of the two.",
            ));
        }
        if config.delivery_timeout > config.transaction_timeout() {
            tracing::warn!(
                delivery_timeout_secs = config.delivery_timeout.as_secs_f64(),
                transaction_timeout_secs = config.transaction_timeout().as_secs_f64(),
                "delivery_timeout exceeds transaction_timeout; the coordinator aborts the \
                 transaction first, so the extra delivery budget is unreachable"
            );
        }
    } else {
        if config.transaction_timeout.is_some() {
            return Err(KrafkaError::config(
                "transaction_timeout applies only to build_transactional()",
            ));
        }
        if config.two_phase_commit {
            return Err(KrafkaError::config(
                "two_phase_commit applies only to build_transactional()",
            ));
        }
    }
    if config.idempotent && config.acks != Acks::All {
        return Err(KrafkaError::config(format!(
            "idempotent producer requires acks = All (got {:?})",
            config.acks
        )));
    }
    if config.buffer_memory == 0 {
        return Err(KrafkaError::config("buffer_memory must be >= 1"));
    }
    if config.batch_size > config.buffer_memory {
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
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::Kafka;

    fn producer() -> crate::producer::ProducerBuilder {
        Kafka::detached().producer()
    }

    /// A level set alongside a codec that cannot use one must be rejected:
    /// an operator who sets `compression_level(9)` with Snappy believes they
    /// tuned something.
    #[tokio::test]
    async fn compression_level_on_a_levelless_codec_is_rejected() {
        let err = producer()
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

    /// An out-of-range level is rejected rather than clamped, so the operator
    /// learns the real range.
    #[tokio::test]
    async fn out_of_range_compression_level_is_rejected() {
        let err = producer()
            .compression(Compression::Gzip)
            .compression_level(Some(42))
            .build_config()
            .expect_err("gzip tops out at 9");
        assert!(err.to_string().contains("0..=9"), "got: {err}");
    }

    #[cfg(feature = "zstd")]
    #[tokio::test]
    async fn valid_compression_level_reaches_the_config() {
        let config = producer()
            .compression(Compression::Zstd)
            .compression_level(Some(1))
            .build_config()
            .expect("level 1 is valid for zstd");
        assert_eq!(config.compression_level, Some(1));
    }

    /// A per-topic override is validated too — otherwise a level valid for
    /// the default codec silently applies to a topic using another.
    #[cfg(feature = "zstd")]
    #[tokio::test]
    async fn per_topic_codec_is_validated_against_the_level() {
        let err = producer()
            .compression(Compression::Zstd)
            .compression_level(Some(1))
            .topic_compression("events", Compression::Snappy)
            .build_config()
            .expect_err("the per-topic Snappy override takes no level");
        assert!(err.to_string().contains("events"), "got: {err}");
    }

    /// Zstd without its Cargo feature is rejected at build time, for the
    /// default codec and for a per-topic override.
    #[cfg(not(feature = "zstd"))]
    #[tokio::test]
    async fn build_rejects_zstd_without_its_feature() {
        let err = producer()
            .compression(Compression::Zstd)
            .build_config()
            .expect_err("zstd without the feature must be rejected")
            .to_string();
        assert!(err.contains("`zstd` Cargo feature"), "got: {err}");

        let err = producer()
            .topic_compression("high-volume", Compression::Zstd)
            .build_config()
            .expect_err("a per-topic zstd override must be rejected too")
            .to_string();
        assert!(err.contains("high-volume"), "got: {err}");
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
        assert_eq!(Acks::from_i16(2), None);
        assert_eq!(Acks::from_i16(-2), None);
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
        assert_eq!(config.linger, Duration::from_millis(5), "KIP-1030");
    }

    #[tokio::test]
    async fn test_config_builder() {
        let config = producer()
            .acks(Acks::All)
            .compression(Compression::Lz4)
            .batch_size(32768)
            .max_request_size(65536)
            .delivery_timeout(Duration::from_secs(45))
            .build_config()
            .unwrap();
        assert_eq!(config.acks, Acks::All);
        assert_eq!(config.compression, Compression::Lz4);
        assert_eq!(config.batch_size, 32768);
        assert_eq!(config.max_request_size, 65536);
        assert_eq!(config.delivery_timeout, Duration::from_secs(45));
    }

    #[tokio::test]
    async fn zero_sizes_and_budgets_are_rejected() {
        assert!(producer().batch_size(0).build_config().is_err());
        assert!(producer().max_request_size(0).build_config().is_err());
        assert!(
            producer()
                .delivery_timeout(Duration::ZERO)
                .build_config()
                .is_err()
        );
        let err = producer()
            .buffer_memory(0)
            .build_config()
            .expect_err("a zero buffer budget must be rejected")
            .to_string();
        assert!(err.contains("buffer_memory must be >= 1"), "got: {err}");
    }

    #[tokio::test]
    async fn idempotent_with_acks_leader_is_rejected() {
        let err = producer()
            .idempotent(true)
            .acks(Acks::Leader)
            .build_config()
            .expect_err("idempotence needs acks=all");
        assert!(err.to_string().contains("acks"), "got: {err}");
    }

    #[tokio::test]
    async fn batch_larger_than_its_bounds_is_rejected() {
        assert!(
            producer()
                .batch_size(1024)
                .buffer_memory(512)
                .build_config()
                .is_err()
        );
        assert!(
            producer()
                .batch_size(1024)
                .max_request_size(512)
                .build_config()
                .is_err()
        );
    }

    /// `delivery_timeout` must leave room for one full attempt: linger plus
    /// the handle's request timeout.
    #[tokio::test]
    async fn delivery_timeout_below_linger_plus_request_timeout_is_rejected() {
        let err = producer()
            .delivery_timeout(Duration::from_secs(10))
            .build_config()
            .expect_err("30 s request timeout does not fit in 10 s")
            .to_string();
        assert!(err.contains("request_timeout"), "got: {err}");
    }

    /// Transaction settings on a plain producer are an error, not ignored.
    #[tokio::test]
    async fn transaction_settings_need_build_transactional() {
        let err = producer()
            .transaction_timeout(Duration::from_secs(30))
            .build_config()
            .expect_err("transaction_timeout on a plain producer")
            .to_string();
        assert!(err.contains("build_transactional"), "got: {err}");
        let err = producer()
            .two_phase_commit(true)
            .build_config()
            .expect_err("two_phase_commit on a plain producer")
            .to_string();
        assert!(err.contains("build_transactional"), "got: {err}");
    }

    #[test]
    fn two_phase_commit_contradicts_an_explicit_transaction_timeout() {
        let config = ProducerConfig {
            two_phase_commit: true,
            transaction_timeout: Some(Duration::from_secs(60)),
            ..ProducerConfig::default()
        };
        let err = validate(&config, Duration::from_secs(30), true)
            .expect_err("2PC with a timeout")
            .to_string();
        assert!(err.contains("contradict"), "got: {err}");

        let config = ProducerConfig {
            two_phase_commit: true,
            ..ProducerConfig::default()
        };
        validate(&config, Duration::from_secs(30), true).expect("2PC alone is valid");
    }

    #[test]
    fn a_transactional_producer_cannot_drop_idempotence() {
        let config = ProducerConfig {
            idempotent: false,
            ..ProducerConfig::default()
        };
        assert!(validate(&config, Duration::from_secs(30), true).is_err());
        let config = ProducerConfig {
            transaction_timeout: Some(Duration::ZERO),
            ..ProducerConfig::default()
        };
        assert!(validate(&config, Duration::from_secs(30), true).is_err());
    }
}
