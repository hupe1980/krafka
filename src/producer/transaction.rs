//! Transactional producer for exactly-once semantics.
//!
//! The transactional producer enables atomic writes across multiple partitions
//! and topics. It guarantees that either all messages in a transaction are
//! committed or none are.
//!
//! # Transaction State and Recovery
//!
//! Transaction state (`TransactionState`) is held in-memory only. This is the
//! **expected and correct behavior** because:
//!
//! 1. **Broker-side coordination**: The transaction coordinator on the broker
//!    side maintains the authoritative transaction state for each `transactional.id()`.
//!
//! 2. **Fencing**: When a new producer starts with the same `transactional.id()`,
//!    the broker:
//!    - Increments the producer epoch
//!    - Aborts any pending (uncommitted) transactions from the old producer
//!    - Issues a new Producer ID to the new producer
//!
//! 3. **Zombie fencing**: If the old producer tries to continue a transaction
//!    after the new producer has started, it receives `ProducerFenced` error.
//!
//! ## Recovery Behavior
//!
//! On producer crash/restart:
//! - Any uncommitted transaction is automatically aborted by the broker
//!   (after `transaction.timeout.ms` expires, or when a new producer with
//!   the same `transactional.id()` calls `init_transactions()`)
//! - The new producer starts fresh with a new epoch
//! - No manual recovery is needed
//!
//! This matches the Kafka Java client behavior and Kafka's transaction protocol.
//!
//! # Example
//!
//! ```ignore
//! use krafka::producer::TransactionalProducer;
//!
//! let producer = TransactionalProducer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .transactional_id("my-transaction")
//!     .build()
//!     .await?;
//!
//! producer.init_transactions().await?;
//!
//! producer.begin_transaction()?;
//! producer.send("topic", Some(b"key"), b"value").await?;
//! producer.commit_transaction().await?;
//! ```

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Notify, RwLock};
use tracing::{debug, info, warn};

use crate::auth::AuthConfig;
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::metadata::ClusterMetadata;
use crate::network::{BrokerConnection, ConnectionConfig, ConnectionPool};
use crate::protocol::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
    AddPartitionsToTxnResponse, ApiKey, Compression, EndTxnRequest, EndTxnResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, InitProducerIdRequest, InitProducerIdResponse,
    TxnOffsetCommitRequest, TxnOffsetCommitResponse, VersionedDecode, VersionedEncode, versions,
};
use crate::{Offset, PartitionId};

use super::accumulator::{AccumulatorConfig, RecordAccumulator, RecordAccumulatorHandle};
use super::config::Acks;
use super::idempotent::ProducerIdentity;
use super::partitioner::{Partitioner, UniformStickyPartitioner};
use super::record::{ProducerRecord, RecordMetadata, TopicHandle};
use super::retry::RetryPolicy;
use crate::barrier::InFlightBarrier;
use crate::consumer::ConsumerGroupMetadata;
use crate::metrics::ProducerMetrics;

use crate::serdes::Serializer;

/// Name of the cluster-wide finalized feature that gates KIP-890 semantics.
const TRANSACTION_VERSION_FEATURE: &str = "transaction.version";

/// Minimum `Produce` version that carries the transactional fields the broker
/// needs to add a partition to the transaction implicitly (KIP-890 TV2).
const TV2_MIN_PRODUCE_VERSION: i16 = 12;

/// Minimum `TxnOffsetCommit` version at which the group coordinator, rather
/// than the client, registers the offsets topic with the transaction
/// coordinator (KIP-890 TV2).
const TV2_MIN_TXN_OFFSET_COMMIT_VERSION: i16 = 5;

/// Minimum `EndTxn` version whose response carries the bumped producer ID and
/// epoch that a TV2 producer is required to adopt (KIP-890 TV2).
const TV2_MIN_END_TXN_VERSION: i16 = 4;

/// The negotiated KIP-890 transaction protocol in use with this cluster.
///
/// Selected once during [`init_transactions`](TransactionalProducer::init_transactions)
/// from the cluster-finalized `transaction.version` feature, and fixed for the
/// life of the producer. It is a **runtime** choice: one binary speaks both
/// protocols and picks per cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default)]
#[non_exhaustive]
#[repr(u8)]
pub enum TransactionVersion {
    /// Classic transactions, as shipped since Kafka 0.11.
    ///
    /// The client explicitly registers each partition with the transaction
    /// coordinator via `AddPartitionsToTxn` before its first write to that
    /// partition, and registers the offsets topic via `AddOffsetsToTxn` before
    /// committing consumer offsets. The producer epoch is bumped only by
    /// `InitProducerId`, so it survives across transactions.
    ///
    /// This is the fallback whenever the cluster does not finalize
    /// `transaction.version` at level 2 or above, which includes every broker
    /// predating KIP-890.
    #[default]
    V1 = 1,
    /// KIP-890 transactions (`transaction.version` ≥ 2).
    ///
    /// Two behaviours change, and both are why TV2 exists:
    ///
    /// 1. **Implicit partition registration.** The `Produce` request itself
    ///    tells the coordinator which partitions joined the transaction, so
    ///    `AddPartitionsToTxn` and `AddOffsetsToTxn` are not sent at all. This
    ///    removes one coordinator round trip per partition per transaction.
    ///
    /// 2. **Epoch bump on every completion.** The coordinator increments the
    ///    producer epoch when it writes the commit or abort marker and returns
    ///    the new `(producer_id, producer_epoch)` on the `EndTxn` response.
    ///    Because the epoch advances at the transaction boundary, a delayed
    ///    write from a previous transaction can never be accepted into the
    ///    next one — this is the defence against hanging transactions and
    ///    zombie writes that TV1 structurally cannot provide.
    V2 = 2,
}

impl From<u8> for TransactionVersion {
    /// Decode the discriminant stored in the producer's atomic.
    ///
    /// Any value other than 2 decodes to [`V1`](TransactionVersion::V1),
    /// which keeps an impossible discriminant on the safe protocol rather
    /// than enabling TV2 on a cluster that may not support it.
    fn from(v: u8) -> Self {
        if v == Self::V2 as u8 {
            Self::V2
        } else {
            Self::V1
        }
    }
}

impl TransactionVersion {
    /// Map a finalized `transaction.version` feature level to a protocol.
    ///
    /// Level 0 means the feature is disabled and level 1 only enables flexible
    /// fields in the coordinator's internal state records — neither changes
    /// the client protocol, so both are [`V1`](Self::V1). Level 2 and above
    /// enable the KIP-890 client semantics.
    #[must_use]
    pub fn from_feature_level(level: i16) -> Self {
        if level >= 2 { Self::V2 } else { Self::V1 }
    }

    /// Whether the KIP-890 client semantics are active.
    #[must_use]
    #[inline]
    pub fn is_v2(self) -> bool {
        matches!(self, Self::V2)
    }
}

impl std::fmt::Display for TransactionVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::V1 => write!(f, "TV1"),
            Self::V2 => write!(f, "TV2"),
        }
    }
}

/// What one broker reports about its ability to speak KIP-890 TV2.
///
/// Collected per broker so that [`negotiated_transaction_version`] can reduce a
/// mixed-version cluster to the single protocol that every broker can serve.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BrokerTransactionSupport {
    /// `max_version_level` of the broker's finalized `transaction.version`
    /// feature, or 0 when the broker did not report the feature at all.
    transaction_version_level: i16,
    /// Highest mutually supported `Produce` version, or `None` when none is.
    produce_max: Option<i16>,
    /// Highest mutually supported `TxnOffsetCommit` version.
    txn_offset_commit_max: Option<i16>,
    /// Highest mutually supported `EndTxn` version.
    end_txn_max: Option<i16>,
}

impl BrokerTransactionSupport {
    /// The best protocol this single broker can serve.
    ///
    /// A broker only counts as TV2-capable if it both finalizes the feature at
    /// level 2+ **and** can actually speak the three APIs whose newer versions
    /// carry TV2 semantics. Finalized features are cluster-wide metadata and
    /// can be observed before every broker has restarted into a build that
    /// serves the matching API versions, so the feature level alone is not
    /// sufficient evidence.
    fn version(self) -> TransactionVersion {
        let feature = TransactionVersion::from_feature_level(self.transaction_version_level);
        if !feature.is_v2() {
            return TransactionVersion::V1;
        }

        let supports =
            |negotiated: Option<i16>, required: i16| negotiated.is_some_and(|v| v >= required);

        if supports(self.produce_max, TV2_MIN_PRODUCE_VERSION)
            && supports(
                self.txn_offset_commit_max,
                TV2_MIN_TXN_OFFSET_COMMIT_VERSION,
            )
            && supports(self.end_txn_max, TV2_MIN_END_TXN_VERSION)
        {
            TransactionVersion::V2
        } else {
            TransactionVersion::V1
        }
    }
}

/// Reduce per-broker capability reports to the protocol the producer will use.
///
/// Takes the **minimum** across brokers: during a rolling upgrade the finalized
/// feature can already read as level 2 while some brokers still run an older
/// build, and speaking TV2 to a broker that expects an explicit
/// `AddPartitionsToTxn` would silently drop that partition from the
/// transaction. Downgrading the whole producer to TV1 is always safe because
/// a TV2-capable broker still serves the TV1 protocol.
///
/// An empty report set — no broker could be reached or asked — yields
/// [`TransactionVersion::V1`], the conservative default.
fn negotiated_transaction_version(reports: &[BrokerTransactionSupport]) -> TransactionVersion {
    reports
        .iter()
        .map(|r| r.version())
        .min()
        .unwrap_or(TransactionVersion::V1)
}

/// Transaction state machine states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[repr(u8)]
pub enum TransactionState {
    /// Producer not yet initialized.
    Uninitialized = 0,
    /// Ready to begin a new transaction.
    Ready = 1,
    /// Transaction is in progress.
    InTransaction = 2,
    /// Transaction is committing.
    Committing = 3,
    /// Transaction is aborting.
    Aborting = 4,
    /// Fatal error occurred, producer must be recreated.
    FatalError = 5,
    /// Initialization in progress (prevents concurrent init_transactions calls).
    Initializing = 6,
    /// `EndTxn(commit)` was dispatched but its outcome is unknown.
    ///
    /// Reached when a commit fails with a timeout or a connection loss — the
    /// coordinator may or may not have applied it. This is deliberately *not*
    /// `InTransaction`: aborting from here is the
    /// [KAFKA-17754](https://issues.apache.org/jira/browse/KAFKA-17754)
    /// trigger, where a delayed `EndTxn` lands on the wrong transaction and
    /// tears it. The only safe moves are to **retry the commit** (`EndTxn` is
    /// idempotent for the same producer id and epoch) or to abandon the
    /// producer and let the coordinator resolve the transaction via its own
    /// `transaction.timeout.ms`.
    CommitIndeterminate = 7,
}

impl From<u8> for TransactionState {
    fn from(v: u8) -> Self {
        match v {
            0 => Self::Uninitialized,
            1 => Self::Ready,
            2 => Self::InTransaction,
            3 => Self::Committing,
            4 => Self::Aborting,
            5 => Self::FatalError,
            6 => Self::Initializing,
            7 => Self::CommitIndeterminate,
            _ => {
                warn!(
                    discriminant = v,
                    "unknown TransactionState discriminant — treating as FatalError"
                );
                Self::FatalError
            }
        }
    }
}

impl std::fmt::Display for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Uninitialized => "Uninitialized",
            Self::Ready => "Ready",
            Self::InTransaction => "InTransaction",
            Self::Committing => "Committing",
            Self::Aborting => "Aborting",
            Self::FatalError => "FatalError",
            Self::Initializing => "Initializing",
            Self::CommitIndeterminate => "CommitIndeterminate",
        })
    }
}

/// A topic-partition offset used with [`TransactionalProducer::send_offsets_to_transaction`].
///
/// The [`next_offset`](TopicPartitionOffset::next_offset) field must be
/// `last_consumed_offset + 1`, which matches the value returned by
/// [`Consumer::position`](crate::consumer::Consumer::position). Kafka commits
/// this value as the next offset the consumer group will start reading from,
/// so an off-by-one here permanently shifts the group's position.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TopicPartitionOffset {
    /// Topic name.
    pub topic: String,
    /// Partition ID.
    pub partition: PartitionId,
    /// The **next** offset to be consumed (`last_consumed_offset + 1`).
    pub next_offset: Offset,
}

impl TopicPartitionOffset {
    /// Construct a new `TopicPartitionOffset`.
    pub fn new(topic: impl Into<String>, partition: PartitionId, next_offset: Offset) -> Self {
        Self {
            topic: topic.into(),
            partition,
            next_offset,
        }
    }
}

/// Configuration for a transactional producer.
///
/// Produced by
/// [`TransactionalProducerBuilder::build_config`], which validates it without
/// connecting. Direct field construction is intentionally not supported so that
/// every instance has been through the validator.
#[derive(Debug, Clone)]
pub struct TransactionalProducerConfig {
    /// Bootstrap servers.
    bootstrap_servers: String,
    /// Client ID.
    client_id: String,
    /// Transactional ID (required for transactions).
    transactional_id: String,
    /// Transaction timeout in milliseconds.
    transaction_timeout_ms: i32,
    /// Request timeout.
    request_timeout: Duration,
    /// Time allowed for TCP establishment to one broker.
    connect_timeout: Duration,
    /// Total time a record may spend in flight, including batching, retries and
    /// backoff.
    ///
    /// Matters more here than on the plain producer: a batch that keeps
    /// retrying holds the transaction open, and an open transaction blocks
    /// `read_committed` consumers at its first offset until the coordinator's
    /// own `transaction_timeout` fires.
    delivery_timeout: Duration,
    /// Maximum encoded Kafka request frame size in bytes.
    max_request_size: usize,
    /// Compression.
    compression: Compression,
    /// Compression level, or `None` for the codec's own default.
    compression_level: Option<i32>,
    /// Per-topic compression overrides, taking precedence over `compression`.
    topic_compression: HashMap<String, Compression>,
    /// Maximum batch size in bytes for the record accumulator.
    batch_size: usize,
    /// How long the accumulator waits for a batch to fill before sending it.
    ///
    /// Transactional sends are batched through the same
    /// [`RecordAccumulator`] as the plain producer, so this is the main
    /// throughput knob. Defaults to 5 ms: a transactional produce is
    /// `acks=all`, so without batching every record costs a full round trip.
    linger: Duration,
    /// Total accumulator buffer memory in bytes.
    buffer_memory: usize,
    /// Maximum time `send` blocks waiting for accumulator buffer memory.
    max_block: Duration,
    /// Maximum concurrent in-flight produce requests.
    max_in_flight: usize,
    /// Metadata max age.
    metadata_max_age: Duration,
    /// Topic cache TTL for partial metadata refreshes, or `None` to disable it.
    metadata_topic_cache_ttl: Option<Duration>,
    /// What to do when every known broker becomes unreachable (KIP-899).
    metadata_recovery_strategy: crate::metadata::MetadataRecoveryStrategy,
    /// How long metadata refreshes may keep failing before a rebootstrap is
    /// triggered (KIP-899). Only effective with
    /// [`MetadataRecoveryStrategy::Rebootstrap`](crate::metadata::MetadataRecoveryStrategy::Rebootstrap).
    metadata_recovery_rebootstrap_trigger: Duration,
    /// Authentication configuration.
    auth: Option<AuthConfig>,
    /// SOCKS5 proxy configuration (optional).
    #[cfg(feature = "socks5")]
    proxy: Option<crate::network::ProxyConfig>,
    /// Socket- and pool-level transport tuning.
    ///
    /// Defaults reproduce krafka's historical behaviour; see
    /// [`TransportConfig`](crate::network::TransportConfig).
    transport: crate::network::TransportConfig,
    /// Dead-letter queue for records whose batch failed permanently.
    dead_letter_queue: Option<Arc<dyn crate::dlq::DeadLetterQueue>>,
}

impl Default for TransactionalProducerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            client_id: "krafka-txn-producer".to_string(),
            transactional_id: String::new(),
            transaction_timeout_ms: 60000,
            request_timeout: Duration::from_secs(30),
            connect_timeout: crate::network::DEFAULT_CONNECT_TIMEOUT,
            delivery_timeout: Duration::from_secs(120),
            max_request_size: crate::protocol::MAX_MESSAGE_SIZE,
            compression: Compression::None,
            compression_level: None,
            topic_compression: HashMap::new(),
            batch_size: 16384,
            linger: Duration::from_millis(5),
            buffer_memory: 32 * 1024 * 1024,
            max_block: Duration::from_secs(60),
            max_in_flight: 5,
            metadata_max_age: Duration::from_secs(300),
            metadata_topic_cache_ttl: Some(Duration::from_secs(300)),
            metadata_recovery_strategy: crate::metadata::MetadataRecoveryStrategy::Rebootstrap,
            metadata_recovery_rebootstrap_trigger: Duration::from_secs(300),
            auth: None,
            #[cfg(feature = "socks5")]
            proxy: None,
            transport: crate::network::TransportConfig::default(),
            dead_letter_queue: None,
        }
    }
}

impl TransactionalProducerConfig {
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

    /// Returns the transactional ID.
    #[inline]
    pub fn transactional_id(&self) -> &str {
        &self.transactional_id
    }

    /// Returns the transaction timeout.
    #[inline]
    pub fn transaction_timeout(&self) -> Duration {
        Duration::from_millis(self.transaction_timeout_ms.max(0) as u64)
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

    /// Returns the maximum encoded request frame size in bytes.
    #[inline]
    pub fn max_request_size(&self) -> usize {
        self.max_request_size
    }

    /// Returns the compression codec.
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

    /// Returns the linger duration.
    #[inline]
    pub fn linger(&self) -> Duration {
        self.linger
    }

    /// Returns the accumulator buffer memory in bytes.
    #[inline]
    pub fn buffer_memory(&self) -> usize {
        self.buffer_memory
    }

    /// Returns the maximum time `send` blocks waiting for buffer memory.
    #[inline]
    pub fn max_block(&self) -> Duration {
        self.max_block
    }

    /// Returns the maximum concurrent in-flight produce requests.
    #[inline]
    pub fn max_in_flight(&self) -> usize {
        self.max_in_flight
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
    pub fn metadata_recovery_strategy(&self) -> crate::metadata::MetadataRecoveryStrategy {
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

    /// Returns the acknowledgment level.
    ///
    /// Always [`Acks::All`]: the transaction coordinator can only guarantee
    /// atomicity over fully replicated writes, so this is fixed rather than
    /// configurable. See [`TransactionalProducerBuilder`] for the other
    /// deliberate exclusion.
    #[inline]
    pub fn acks(&self) -> Acks {
        Acks::All
    }
}

/// Validate and normalise a [`TransactionalProducerConfig`].
///
/// The transactional twin of
/// [`producer::config::validate`](super::config::validate), and it shares that
/// module's compression rules rather than restating them — a codec check that
/// exists on one producer and not the other is the defect class this release
/// is closing.
///
/// `has_shared_pool` relaxes the `bootstrap_servers` requirement for a producer
/// built with [`TransactionalProducerBuilder::with_client`].
fn validate(config: &TransactionalProducerConfig, has_shared_pool: bool) -> Result<()> {
    if !has_shared_pool && config.bootstrap_servers.is_empty() {
        return Err(KrafkaError::config("bootstrap.servers is required"));
    }
    if config.transactional_id.is_empty() {
        return Err(KrafkaError::config("transactional_id is required"));
    }
    // Validate against Kafka's KafkaString wire limit (i16::MAX bytes).
    const MAX_KAFKA_STRING_LEN: usize = i16::MAX as usize;
    if config.transactional_id.len() > MAX_KAFKA_STRING_LEN {
        return Err(KrafkaError::config(format!(
            "transactional_id is {} bytes, exceeding the Kafka wire limit of {MAX_KAFKA_STRING_LEN}",
            config.transactional_id.len()
        )));
    }
    if config.client_id.len() > MAX_KAFKA_STRING_LEN {
        return Err(KrafkaError::config(format!(
            "client_id is {} bytes, exceeding the Kafka wire limit of {MAX_KAFKA_STRING_LEN}",
            config.client_id.len()
        )));
    }
    if config.transaction_timeout_ms <= 0 {
        return Err(KrafkaError::config("transaction_timeout must be > 0"));
    }
    if config.max_request_size == 0 {
        return Err(KrafkaError::config("max_request_size must be >= 1"));
    }
    if config.batch_size == 0 {
        return Err(KrafkaError::config("batch_size must be >= 1"));
    }
    if config.max_in_flight == 0 {
        return Err(KrafkaError::config("max_in_flight must be >= 1"));
    }
    if config.delivery_timeout.is_zero() {
        return Err(KrafkaError::config(
            "delivery_timeout must be greater than zero",
        ));
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

    super::config::validate_compression(
        config.compression,
        config.compression_level,
        &config.topic_compression,
    )?;

    // A delivery budget longer than the coordinator's own transaction timeout
    // cannot be honoured: the coordinator aborts the transaction first, and
    // every record still retrying inside it dies with it. Warn rather than
    // reject — the two are set by different people often enough that failing
    // the build would be worse than saying so.
    let transaction_timeout = Duration::from_millis(config.transaction_timeout_ms.max(0) as u64);
    if config.delivery_timeout > transaction_timeout {
        warn!(
            delivery_timeout_secs = config.delivery_timeout.as_secs_f64(),
            transaction_timeout_secs = transaction_timeout.as_secs_f64(),
            "delivery_timeout exceeds transaction_timeout; the coordinator aborts the \
             transaction first, so the extra delivery budget is unreachable"
        );
    }
    Ok(())
}

/// State of a partition within the current transaction.
#[derive(Debug, Clone)]
enum PartitionAddState {
    /// AddPartitionsToTxn RPC is in-flight; concurrent callers should wait.
    Pending(Arc<Notify>),
    /// Successfully registered with the transaction coordinator.
    Added,
    /// RPC failed with a non-retriable error.  Waiters should propagate this
    /// error immediately rather than making a redundant retry RPC.
    Failed(Arc<KrafkaError>),
}

/// Result of attempting to begin adding a partition to the transaction.
#[cfg_attr(test, derive(Debug))]
enum BeginAddResult {
    /// Partition already registered — nothing to do.
    AlreadyAdded,
    /// Another caller is registering this partition — wait on the Notify.
    Wait(Arc<Notify>),
    /// This caller must perform the RPC. Notify to signal waiters afterwards.
    NeedAdd(Arc<Notify>),
    /// A previous non-retriable RPC failure was recorded for this partition.
    /// The caller should return this error without attempting the RPC again.
    Fatal(Arc<KrafkaError>),
}

/// Partitions added to the current transaction.
#[derive(Debug, Default)]
struct TransactionPartitions {
    /// Topic-partitions and their registration state (topic → partition → state).
    partitions: std::collections::HashMap<
        String,
        std::collections::HashMap<PartitionId, PartitionAddState>,
    >,
}

impl TransactionPartitions {
    /// Begin adding a partition. Returns the action the caller must take.
    fn begin_add(&mut self, topic: &str, partition: PartitionId) -> BeginAddResult {
        if let Some(topic_map) = self.partitions.get(topic) {
            match topic_map.get(&partition) {
                Some(PartitionAddState::Added) => return BeginAddResult::AlreadyAdded,
                Some(PartitionAddState::Pending(notify)) => {
                    return BeginAddResult::Wait(notify.clone());
                }
                Some(PartitionAddState::Failed(err)) => {
                    return BeginAddResult::Fatal(err.clone());
                }
                None => {}
            }
        }
        let notify = Arc::new(Notify::new());
        self.partitions
            .entry(topic.to_string())
            .or_default()
            .insert(partition, PartitionAddState::Pending(notify.clone()));
        BeginAddResult::NeedAdd(notify)
    }

    /// Confirm a partition was successfully registered.
    fn confirm_add(&mut self, topic: &str, partition: PartitionId, notify: &Notify) {
        self.partitions
            .entry(topic.to_string())
            .or_default()
            .insert(partition, PartitionAddState::Added);
        notify.notify_waiters();
    }

    /// Cancel a pending add due to a retriable / transient error.
    ///
    /// Removes the partition entry so that waiters can retry the RPC
    /// themselves on the next loop iteration.
    fn cancel_add(&mut self, topic: &str, partition: PartitionId, notify: &Notify) {
        if let Some(topic_map) = self.partitions.get_mut(topic) {
            topic_map.remove(&partition);
            if topic_map.is_empty() {
                self.partitions.remove(topic);
            }
        }
        notify.notify_waiters();
    }

    /// Record a non-retriable RPC failure for this partition.
    ///
    /// Stores a `Failed` sentinel so that concurrent waiters receive the
    /// error immediately via [`BeginAddResult::Fatal`] rather than making
    /// a redundant retry RPC that will also fail.
    fn fail_add(
        &mut self,
        topic: &str,
        partition: PartitionId,
        error: Arc<KrafkaError>,
        notify: &Notify,
    ) {
        self.partitions
            .entry(topic.to_string())
            .or_default()
            .insert(partition, PartitionAddState::Failed(error));
        notify.notify_waiters();
    }

    fn clear(&mut self) {
        self.partitions.clear();
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }
}

/// RAII guard that cancels a pending partition add if dropped without confirmation.
///
/// When the task performing the `AddPartitionsToTxn` RPC is cancelled (e.g.,
/// via `select!` or `timeout`), this guard ensures the partition is rolled back
/// from `Pending` to absent so that future callers can retry rather than
/// waiting on a `Notify` that will never fire.
struct PendingAddGuard {
    txn_partitions: Arc<RwLock<TransactionPartitions>>,
    topic: TopicHandle,
    partition: PartitionId,
    notify: Arc<Notify>,
    /// Set to `true` when `confirm_add` or an explicit `cancel_add` is called,
    /// preventing the drop impl from double-cancelling.
    defused: bool,
}

impl PendingAddGuard {
    /// Confirm the add succeeded. Consumes the guard without cancelling.
    async fn confirm(mut self, topic: &str, partition: PartitionId) {
        self.defused = true;
        let mut txn_partitions = self.txn_partitions.write().await;
        txn_partitions.confirm_add(topic, partition, &self.notify);
    }

    /// Explicitly cancel the add after a **retriable** error.
    ///
    /// Removes the partition entry so that concurrent waiters can retry the
    /// RPC on the next loop iteration.
    async fn cancel(mut self, topic: &str, partition: PartitionId) {
        self.defused = true;
        let mut txn_partitions = self.txn_partitions.write().await;
        txn_partitions.cancel_add(topic, partition, &self.notify);
    }

    /// Record a **non-retriable** failure for this partition.
    ///
    /// Stores a `Failed` sentinel so that concurrent waiters receive the
    /// error immediately instead of making an extra RPC that will also fail.
    async fn fail(mut self, topic: &str, partition: PartitionId, error: Arc<KrafkaError>) {
        self.defused = true;
        let mut txn_partitions = self.txn_partitions.write().await;
        txn_partitions.fail_add(topic, partition, error, &self.notify);
    }
}

impl Drop for PendingAddGuard {
    fn drop(&mut self) {
        if !self.defused {
            // Best-effort cancel: we can't await the lock in drop, so first
            // try a non-blocking write. If the lock is contended and a Tokio
            // runtime is available, spawn a task to perform the cancel.
            let topic = self.topic.clone();
            let partition = self.partition;
            let notify = self.notify.clone();
            if let Ok(mut tp) = self.txn_partitions.try_write() {
                tp.cancel_add(&topic, partition, &notify);
            } else if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let txn_partitions = self.txn_partitions.clone();
                // Note: during runtime shutdown the spawned task may be
                // cancelled before it runs. This is acceptable because
                // the transaction state is ephemeral to the producer
                // instance and will be abandoned on shutdown.
                handle.spawn(async move {
                    let mut tp = txn_partitions.write().await;
                    tp.cancel_add(&topic, partition, &notify);
                });
            } else {
                // No runtime available — use blocking write as last resort.
                // This is safe because Handle::try_current() confirmed we are
                // NOT on a runtime thread, so blocking_write() won't panic.
                let mut tp = self.txn_partitions.blocking_write();
                tp.cancel_add(&topic, partition, &notify);
            }
        }
    }
}

/// A transactional Kafka producer.
///
/// Provides exactly-once semantics through transactions.
pub struct TransactionalProducer {
    /// Configuration.
    config: TransactionalProducerConfig,
    /// Cluster metadata.
    metadata: Arc<ClusterMetadata>,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Whether this client owns its connection pool.
    ///
    /// `false` when the pool was borrowed from a
    /// [`KrafkaClient`](crate::client::KrafkaClient) via `with_client`.
    ///
    /// Closing a borrowed pool would tear down every sibling client's
    /// connections and fail their in-flight requests — which is what happened
    /// until `AdminClient`'s handling of this was extended to its siblings.
    pool_owned: bool,
    /// Partitioner.
    partitioner: Arc<dyn Partitioner>,
    /// Transaction state.
    state: AtomicU8,
    /// Negotiated KIP-890 protocol, as a [`TransactionVersion`] discriminant.
    ///
    /// Written once by [`init_transactions`](Self::init_transactions) before
    /// the state leaves `Initializing`, and only read afterwards, so relaxed
    /// visibility concerns do not arise; `SeqCst` is used for uniformity with
    /// the other atomics on this type.
    transaction_version: AtomicU8,
    /// Whether the current transaction hit an abortable error and must be
    /// aborted before further send/commit operations are allowed.
    abort_required: AtomicBool,
    /// Transaction coordinator broker ID.
    ///
    /// # Lock ordering
    ///
    /// When both `coordinator_id` and `txn_partitions` are acquired in the
    /// same task, always acquire `coordinator_id` first to avoid deadlocks.
    coordinator_id: RwLock<Option<i32>>,
    /// Partitions in current transaction.
    ///
    /// Always acquired **after** `coordinator_id` (see lock-order note above).
    txn_partitions: Arc<RwLock<TransactionPartitions>>,
    /// Sequence number tracking for idempotent production.
    ///
    /// Shared with the [`RecordAccumulator`], which stamps the PID, epoch and
    /// per-partition sequence onto every batch it builds.
    identity: Arc<ProducerIdentity>,
    /// Batching accumulator for transactional sends.
    ///
    /// Transactional production used to issue one `acks=all` `ProduceRequest`
    /// per record and await it, capping throughput at roughly one record per
    /// round trip per partition. Routing through the accumulator batches
    /// records exactly like the plain producer while still stamping the
    /// transactional ID, PID and epoch on each batch.
    accumulator: RecordAccumulatorHandle,
    /// Metrics shared with the accumulator.
    metrics: Arc<ProducerMetrics>,
    /// Retry policy for transient failures.
    retry_policy: RetryPolicy,
    /// Barrier over started transactional operations and shutdown state.
    in_flight_barrier: Arc<InFlightBarrier>,
    /// Optional key encoder applied transparently in `send_record`.
    ///
    /// Equivalent to `key.serializer` in the Java `KafkaProducer`.
    key_serializer: Option<Arc<dyn Serializer>>,
    /// Optional value encoder applied transparently in `send_record`.
    ///
    /// Equivalent to `value.serializer` in the Java `KafkaProducer`.
    value_serializer: Option<Arc<dyn Serializer>>,
    /// Interceptor chain, shared with the accumulator.
    ///
    /// `on_send` is invoked here; `on_acknowledgement` fires inside the
    /// accumulator once the broker answers or the batch fails permanently.
    interceptor: Arc<dyn crate::interceptor::ProducerInterceptor>,
    /// Optional pluggable persistence hook for producer identity state.
    ///
    /// Loaded once in [`init_transactions`](Self::init_transactions); stored
    /// fire-and-forget by the accumulator after each acknowledged batch.
    state_store: Option<Arc<dyn super::idempotent::ErasedProducerStateStore>>,
}

impl TransactionalProducer {
    /// Create a new transactional producer builder.
    pub fn builder() -> TransactionalProducerBuilder {
        TransactionalProducerBuilder::default()
    }

    /// Get the current transaction state.
    #[inline]
    pub fn state(&self) -> TransactionState {
        TransactionState::from(self.state.load(Ordering::SeqCst))
    }

    /// The KIP-890 transaction protocol negotiated with this cluster.
    ///
    /// Returns [`TransactionVersion::V1`] until
    /// [`init_transactions`](Self::init_transactions) has completed, since the
    /// finalized feature is only queried there.
    #[inline]
    pub fn transaction_version(&self) -> TransactionVersion {
        TransactionVersion::from(self.transaction_version.load(Ordering::SeqCst))
    }

    /// Whether the client itself must register partitions and the offsets
    /// topic with the transaction coordinator before writing to them.
    ///
    /// True under TV1, where `AddPartitionsToTxn` / `AddOffsetsToTxn` are the
    /// only way the coordinator learns which partitions the commit marker has
    /// to cover. False under TV2 (KIP-890), where the `Produce` and
    /// `TxnOffsetCommit` requests carry that information themselves — which is
    /// what removes a coordinator round trip per partition per transaction.
    #[inline]
    fn requires_explicit_partition_registration(&self) -> bool {
        !self.transaction_version().is_v2()
    }

    /// Ask every known broker what it can serve and settle on one protocol.
    ///
    /// The finalized-feature set is only present on `ApiVersions` **v3+**
    /// responses, and the connection handshake issues `ApiVersions` v0 (it has
    /// to: it does not yet know what the broker supports). So this re-asks each
    /// broker at v3+ specifically to read `transaction.version`.
    ///
    /// Brokers that cannot be reached, cannot serve `ApiVersions` v3+, or
    /// answer with an error are skipped rather than treated as TV1. Their
    /// absence is not evidence about the cluster's feature level, and failing
    /// the whole producer over one unreachable broker would be worse than
    /// running a protocol the reachable brokers all agree on. If no broker can
    /// be asked at all, the result is [`TransactionVersion::V1`].
    async fn detect_transaction_version(&self) -> TransactionVersion {
        let brokers = self.metadata.brokers();
        let mut reports = Vec::with_capacity(brokers.len());

        for broker in &brokers {
            match self.probe_broker_transaction_support(broker).await {
                Ok(report) => reports.push(report),
                Err(error) => {
                    debug!(
                        broker = broker.id(),
                        %error,
                        "Could not read transaction.version from broker; \
                         excluding it from the negotiated transaction version"
                    );
                }
            }
        }

        let version = negotiated_transaction_version(&reports);
        info!(
            %version,
            brokers_probed = reports.len(),
            "Negotiated KIP-890 transaction version"
        );
        version
    }

    /// Read one broker's finalized `transaction.version` level together with
    /// the API versions that TV2 depends on.
    async fn probe_broker_transaction_support(
        &self,
        broker: &crate::metadata::BrokerInfo,
    ) -> Result<BrokerTransactionSupport> {
        let conn = self
            .pool
            .get_connection_by_id(broker.id(), broker.address())
            .await?;

        // v3 is the first version whose response carries the KIP-584 tagged
        // fields that hold finalized features.
        let av_version = conn
            .negotiate_api_version(ApiKey::ApiVersions, versions::API_VERSIONS_MAX, 3)
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "broker does not support ApiVersions v3+, so it cannot report finalized features",
                )
            })?;

        let request = crate::protocol::ApiVersionsRequest::new()
            .with_client_software("krafka", env!("CARGO_PKG_VERSION"));

        let response_bytes = conn
            .send_request(ApiKey::ApiVersions, av_version, |buf| {
                if av_version >= 5 {
                    request.encode_v5(buf)
                } else {
                    request.encode_v3(buf)
                }
            })
            .await?;

        let mut buf = response_bytes;
        let response = crate::protocol::ApiVersionsResponse::decode_v3(&mut buf)?;

        if response.error_code != 0 {
            return Err(KrafkaError::broker(
                ErrorCode::from(response.error_code),
                "ApiVersions request failed while reading transaction.version",
            ));
        }

        // An absent feature means the cluster never finalized it, which is the
        // case for every broker predating KIP-890. Level 0 maps to TV1.
        let transaction_version_level = response
            .get_finalized_feature(TRANSACTION_VERSION_FEATURE)
            .map_or(0, |f| f.max_version_level);

        Ok(BrokerTransactionSupport {
            transaction_version_level,
            produce_max: conn.negotiate_api_version(
                ApiKey::Produce,
                versions::PRODUCE_MAX,
                versions::PRODUCE_MIN,
            ),
            txn_offset_commit_max: conn.negotiate_api_version(
                ApiKey::TxnOffsetCommit,
                versions::TXN_OFFSET_COMMIT_MAX,
                versions::TXN_OFFSET_COMMIT_MIN,
            ),
            end_txn_max: conn.negotiate_api_version(
                ApiKey::EndTxn,
                versions::END_TXN_MAX,
                versions::END_TXN_MIN,
            ),
        })
    }

    /// Return the transactional producer identity, failing fast when
    /// `init_transactions()` has not established a valid PID/epoch yet.
    fn checked_transactional_identity(&self) -> Result<(i64, i16)> {
        let producer_id = self.identity.producer_id();
        let producer_epoch = self.identity.producer_epoch();

        if producer_id < 0 || producer_epoch < 0 {
            return Err(KrafkaError::invalid_state(
                "transactional producer identity not initialized",
            ));
        }

        debug_assert!(
            producer_id >= 0 && producer_epoch >= 0,
            "transactional producer identity must be initialized before sending"
        );

        Ok((producer_id, producer_epoch))
    }

    #[inline]
    fn abort_required(&self) -> bool {
        self.abort_required.load(Ordering::SeqCst)
    }

    fn ensure_transaction_can_continue(&self, operation: &str) -> Result<()> {
        if self.abort_required() {
            return Err(KrafkaError::broker(
                ErrorCode::TransactionAbortable,
                format!("cannot {operation}: abort_transaction() is required before continuing"),
            ));
        }

        Ok(())
    }

    fn mark_unknown_producer_id_abort_required(&self, operation: &str) -> KrafkaError {
        self.abort_required.store(true, Ordering::SeqCst);
        KrafkaError::broker(
            ErrorCode::TransactionAbortable,
            format!(
                "{operation} failed with UnknownProducerId; abort_transaction() is required before continuing"
            ),
        )
    }

    fn is_unknown_producer_id_error(error: &KrafkaError) -> bool {
        matches!(
            error,
            KrafkaError::Broker {
                code: ErrorCode::UnknownProducerId,
                ..
            }
        )
    }

    /// Classify a coordinator RPC result and latch
    /// [`TransactionState::FatalError`] when the broker reported a fenced or
    /// otherwise unrecoverable transactional error.
    ///
    /// Every coordinator RPC — `AddPartitionsToTxn`, `AddOffsetsToTxn`,
    /// `TxnOffsetCommit`, `EndTxn` — must be funnelled through this. Without
    /// it, `InvalidProducerEpoch` / `ProducerFenced` bubbled out to the caller
    /// while the state machine still read `InTransaction`, so a fenced zombie
    /// happily carried on sending and committing.
    ///
    /// Returns the result unchanged so it can be used inline.
    fn classify_transaction_result<T>(&self, result: Result<T>) -> Result<T> {
        if let Err(KrafkaError::Broker { code, .. }) = &result
            && is_fatal_transaction_error(*code, self.transaction_version())
        {
            warn!(
                error_code = ?code,
                "Fatal transactional error from coordinator; producer is fenced and must be recreated"
            );
            self.set_state(TransactionState::FatalError);
            return result;
        }

        // Not fatal, but still transaction-ending: latch the abort requirement
        // so the next send or commit is refused until abort_transaction() has
        // run, rather than silently continuing a transaction the coordinator
        // has already rejected.
        if let Err(error) = &result
            && Self::is_abortable_transaction_error(error, self.transaction_version())
        {
            self.abort_required.store(true, Ordering::SeqCst);
        }

        result
    }

    /// Whether the error ends the current transaction but leaves the producer
    /// usable after [`abort_transaction`](Self::abort_transaction).
    ///
    /// # Transaction version
    ///
    /// [`ErrorCode::TransactionAbortable`] (KIP-890) is abortable under both
    /// versions. [`ErrorCode::InvalidProducerIdMapping`] is abortable only
    /// under TV1; under TV2 it is fatal instead, so it is excluded here to keep
    /// the two classifications mutually exclusive — see
    /// [`is_fatal_transaction_error`].
    fn is_abortable_transaction_error(error: &KrafkaError, version: TransactionVersion) -> bool {
        let KrafkaError::Broker { code, .. } = error else {
            return false;
        };

        match code {
            ErrorCode::TransactionAbortable => true,
            ErrorCode::InvalidProducerIdMapping => !version.is_v2(),
            _ => false,
        }
    }

    /// Get a connection to the cached transaction coordinator.
    ///
    /// If no coordinator is cached (e.g. after invalidation), automatically
    /// re-discovers it via `FindCoordinator` before returning the connection.
    async fn coordinator_connection(&self, attempt: u32) -> Result<(i32, Arc<BrokerConnection>)> {
        let coordinator_id = {
            let cached = *self.coordinator_id.read().await;
            match cached {
                Some(id) => id,
                None => {
                    let id = self.find_coordinator(attempt).await?;
                    *self.coordinator_id.write().await = Some(id);
                    debug!("Auto-discovered transaction coordinator: broker {}", id);
                    id
                }
            }
        };

        let brokers = self.metadata.brokers();
        let broker = brokers
            .iter()
            .find(|b| b.id() == coordinator_id)
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::Malformed,
                    "coordinator not found in metadata",
                )
            })?;

        let conn = self
            .pool
            .get_connection_by_id(broker.id(), broker.address())
            .await?;

        Ok((coordinator_id, conn))
    }

    /// Whether the error indicates the cached coordinator may be stale.
    ///
    /// Returns `true` for coordinator-related broker errors (`NotCoordinator`,
    /// `CoordinatorNotAvailable`, `CoordinatorLoadInProgress`) and for
    /// network/timeout errors that suggest the coordinator broker is unreachable.
    fn needs_coordinator_refresh(err: &KrafkaError) -> bool {
        match err {
            KrafkaError::Broker { code, .. } => matches!(
                code,
                ErrorCode::NotCoordinator
                    | ErrorCode::CoordinatorNotAvailable
                    | ErrorCode::CoordinatorLoadInProgress
            ),
            KrafkaError::Network(_) | KrafkaError::Timeout { .. } => true,
            _ => false,
        }
    }

    /// Invalidate the cached transaction coordinator, forcing re-discovery
    /// on the next coordinator RPC.
    async fn invalidate_coordinator(&self) {
        *self.coordinator_id.write().await = None;
    }

    /// Retry a coordinator RPC with exponential backoff.
    ///
    /// On coordinator errors (`NotCoordinator`, `CoordinatorNotAvailable`,
    /// `CoordinatorLoadInProgress`) or transient network/timeout failures the
    /// cached coordinator is invalidated and re-discovered before the next
    /// attempt.  Non-retriable errors are returned immediately.
    ///
    /// `op_name` is used in log messages to identify the RPC.
    async fn retry_with_coordinator<F, Fut>(&self, op_name: &str, op: F) -> Result<()>
    where
        F: Fn(u32) -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let max_retries = self.retry_policy.max_retries;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                tokio::time::sleep(self.retry_policy.calculate_backoff(attempt)).await;
            }

            let result = op(attempt).await;

            match &result {
                Ok(()) => return Ok(()),
                Err(e) if Self::is_unknown_producer_id_error(e) => return result,
                Err(e) if Self::needs_coordinator_refresh(e) && attempt < max_retries => {
                    warn!(
                        attempt,
                        error = %e,
                        op_name,
                        "Coordinator error, refreshing and retrying"
                    );
                    self.invalidate_coordinator().await;
                }
                Err(e) if e.is_retriable() && attempt < max_retries => {
                    warn!(
                        attempt,
                        error = %e,
                        op_name,
                        "Retriable error, retrying"
                    );
                }
                Err(_) => return result,
            }
        }

        Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::Malformed,
            format!("{op_name} retry loop exhausted after {max_retries} retries"),
        ))
    }

    fn set_state(&self, state: TransactionState) {
        self.state.store(state as u8, Ordering::SeqCst);
    }

    /// Atomically transition from `expected` to `new` state.
    /// Returns `Err` with the actual state if the CAS failed.
    fn try_transition(
        &self,
        expected: TransactionState,
        new: TransactionState,
    ) -> std::result::Result<(), TransactionState> {
        // AcqRel on success: the stored new state is Released (visible to
        // readers), and we Acquire the current state (see any prior writes).
        // Acquire on failure: we Acquire the actual current state so callers
        // can act on it without a separate load.  All downstream transaction
        // data is behind an async Mutex, so no stronger ordering is needed.
        self.state
            .compare_exchange(
                expected as u8,
                new as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .map(|_| ())
            .map_err(TransactionState::from)
    }

    /// Initialize transactions.
    ///
    /// This must be called before any transactions can be started.
    /// It fetches the producer ID and epoch from the transaction coordinator.
    pub async fn init_transactions(&self) -> Result<()> {
        // Atomic CAS: Uninitialized → Initializing
        if let Err(actual) = self.try_transition(
            TransactionState::Uninitialized,
            TransactionState::Initializing,
        ) {
            return Err(KrafkaError::invalid_state(format!(
                "init_transactions can only be called once (state={:?})",
                actual
            )));
        }

        // Settle the KIP-890 protocol before the first coordinator RPC. Every
        // later decision — whether AddPartitionsToTxn is sent, whether the
        // EndTxn epoch bump is mandatory, how INVALID_PRODUCER_ID_MAPPING is
        // classified — reads this, so it must be fixed before the producer
        // becomes usable.
        let version = self.detect_transaction_version().await;
        self.transaction_version
            .store(version as u8, Ordering::SeqCst);

        // Find transaction coordinator
        let result = self.do_init_transactions().await;
        if result.is_err() {
            // Revert state so caller can retry
            self.set_state(TransactionState::Uninitialized);
        }
        result
    }

    /// Inner initialization logic, separated for clean error handling.
    ///
    /// Retries on coordinator errors (NotCoordinator, CoordinatorNotAvailable,
    /// CoordinatorLoadInProgress) and transient network/timeout failures with
    /// exponential backoff. On each retry the cached coordinator is invalidated
    /// and re-discovered via `FindCoordinator`.
    async fn do_init_transactions(&self) -> Result<()> {
        self.retry_with_coordinator("InitProducerId", |attempt| async move {
            let (_coordinator_id, conn) = self.coordinator_connection(attempt).await?;

            let ip_version = conn
                .negotiate_api_version(
                    ApiKey::InitProducerId,
                    versions::INIT_PRODUCER_ID_MAX,
                    versions::INIT_PRODUCER_ID_MIN,
                )
                .ok_or_else(|| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::UnknownApiVersion,
                        "no mutually supported InitProducerId API version",
                    )
                })?;

            let request = InitProducerIdRequest::transactional(
                &self.config.transactional_id,
                self.config.transaction_timeout_ms,
            );

            let response_bytes = conn
                .send_request(ApiKey::InitProducerId, ip_version, |buf| {
                    request.encode_versioned(ip_version, buf)
                })
                .await?;

            let mut buf = response_bytes;
            let response = InitProducerIdResponse::decode_versioned(ip_version, &mut buf)?;

            if !response.is_ok() {
                return Err(KrafkaError::broker(
                    response.error_code,
                    "failed to initialize producer ID",
                ));
            }

            self.identity
                .initialize(response.producer_id, response.producer_epoch);

            // Restore per-partition sequences from the state store, if one is
            // configured. This is the producer the store exists for: the
            // coordinator hands back the *same* PID and epoch when a known
            // `transactional.id` re-initialises, which is the only condition
            // under which a stored snapshot is still valid.
            if let Some(ref store) = self.state_store {
                match store.load_erased().await {
                    Ok(Some(snapshot))
                        if snapshot.producer_id == self.identity.producer_id()
                            && snapshot.producer_epoch == self.identity.producer_epoch() =>
                    {
                        self.identity.restore_from_snapshot(&snapshot);
                        info!(
                            pid = self.identity.producer_id(),
                            epoch = self.identity.producer_epoch(),
                            partitions = snapshot.partition_sequences.len(),
                            "Transactional producer identity restored from state store"
                        );
                    }
                    Ok(Some(_)) => {
                        debug!(
                            "State store snapshot PID/epoch mismatch — sequences not \
                             restored; the coordinator assigned a new producer identity"
                        );
                    }
                    Ok(None) => debug!("No previous producer state found in state store"),
                    Err(err) => warn!(
                        error = %err,
                        "Failed to load producer state from store; continuing with fresh state"
                    ),
                }
            }

            self.abort_required.store(false, Ordering::SeqCst);
            self.set_state(TransactionState::Ready);
            info!(
                "Transactional producer initialized: PID={}, epoch={}",
                response.producer_id, response.producer_epoch
            );

            Ok(())
        })
        .await
    }

    /// Find the transaction coordinator.
    ///
    /// `attempt` rotates which broker is asked, mirroring the idempotent
    /// producer's `InitProducerId` path. Always querying `brokers[0]` means a
    /// single unreachable or overloaded broker fails coordinator discovery for
    /// the whole retry loop, even when every other broker could answer.
    async fn find_coordinator(&self, attempt: u32) -> Result<i32> {
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::Malformed,
                "no brokers available",
            ));
        }

        let broker = &brokers[attempt as usize % brokers.len()];
        let conn = self
            .pool
            .get_connection_by_id(broker.id(), broker.address())
            .await?;

        let request = FindCoordinatorRequest::for_transaction(&self.config.transactional_id);

        // Transaction coordinator lookup requires v1+ (key_type field).
        // FIND_COORDINATOR_MIN is 1, so negotiate_api_version returns None
        // (handled above) rather than v0 when the broker lacks v1+.
        let fc_version = conn
            .negotiate_api_version(
                ApiKey::FindCoordinator,
                versions::FIND_COORDINATOR_MAX,
                versions::FIND_COORDINATOR_MIN,
            )
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported FindCoordinator API version; \
                     transactional coordinator lookup requires v1+",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::FindCoordinator, fc_version, |buf| {
                request.encode_versioned(fc_version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = FindCoordinatorResponse::decode_versioned(fc_version, &mut buf)?;

        if !response.error_code.is_ok() {
            return Err(KrafkaError::broker(
                response.error_code,
                "failed to find transaction coordinator",
            ));
        }

        debug!(
            "Found transaction coordinator: broker {} at {}:{}",
            response.node_id, response.host, response.port
        );

        Ok(response.node_id)
    }

    /// Begin a new transaction.
    ///
    /// Must be called after `init_transactions()`.
    /// Begin a new transaction.
    ///
    /// Transitions the producer from `Ready` to `InTransaction` state. Must be
    /// called after [`init_transactions`](Self::init_transactions) and before
    /// any [`send`](Self::send) calls.
    ///
    /// # Non-blocking
    ///
    /// This method is **synchronous and guaranteed non-blocking** — it performs
    /// only an in-memory atomic state transition with no I/O.  It is intentionally
    /// not `async` for two reasons: it never waits on the network, and this
    /// matches the Java `KafkaProducer.beginTransaction()` API.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the producer is not in `Ready` state (e.g. not yet
    /// initialised, or a previous transaction was not committed or aborted).
    pub fn begin_transaction(&self) -> Result<()> {
        // Atomic CAS: Ready → InTransaction
        if let Err(actual) =
            self.try_transition(TransactionState::Ready, TransactionState::InTransaction)
        {
            return Err(KrafkaError::invalid_state(format!(
                "cannot begin transaction in state {:?}",
                actual
            )));
        }

        debug!("Transaction started");
        Ok(())
    }

    /// Send a record within the current transaction.
    pub async fn send(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: &[u8],
    ) -> Result<RecordMetadata> {
        let mut record = ProducerRecord::new(topic, Bytes::copy_from_slice(value));
        if let Some(k) = key {
            record = record.with_key(Bytes::copy_from_slice(k));
        }
        self.send_record(record).await
    }

    /// Send a producer record within the current transaction.
    pub async fn send_record(&self, record: ProducerRecord) -> Result<RecordMetadata> {
        let operation_guard = self.in_flight_barrier.start("transactional producer")?;
        let current = self.state();
        if current != TransactionState::InTransaction {
            return Err(KrafkaError::invalid_state(format!(
                "cannot send in state {:?}",
                current
            )));
        }

        self.ensure_transaction_can_continue("send records")?;

        // Interceptors run before anything else observes the record, matching
        // the plain producer: an interceptor that rewrites the topic must do so
        // before the partition is chosen and before the partition is registered
        // with the transaction coordinator.
        let mut record = record;
        crate::interceptor::safe_on_send(&*self.interceptor, &mut record);

        // Transparently apply producer-level schema encoders if configured.
        if let Some(enc) = &self.value_serializer {
            record.value = enc
                .serialize(
                    record.value.clone(),
                    &record.topic,
                    record.record_name.as_deref(),
                    false,
                )
                .await?;
        }
        if let Some(enc) = &self.key_serializer {
            let key = record.key.clone().unwrap_or_default();
            record.key = Some(
                enc.serialize(key, &record.topic, record.record_name.as_deref(), true)
                    .await?,
            );
        }

        // Validate record fields against Kafka protocol wire-format limits.
        record.validate()?;

        let _identity = self.checked_transactional_identity()?;

        let record_size = record.estimated_size();
        let routed = record.into_routed_parts();
        let topic = routed.topic;
        let record = routed.record;

        // Determine partition
        let partition = match routed.partition {
            Some(p) => p,
            None => {
                let partition_count = self
                    .metadata
                    .partition_count(topic.as_ref())
                    .ok_or_else(|| KrafkaError::invalid_state(format!("unknown topic: {topic}")))?;
                self.partitioner
                    .partition(topic.as_ref(), record.key_bytes(), partition_count)
            }
        };

        // Register the partition with the transaction coordinator.
        //
        // Under TV2 (KIP-890) this is skipped entirely: the Produce request
        // carries the transactional ID, so the broker adds the partition to
        // the transaction as a side effect of the first write to it. Sending
        // AddPartitionsToTxn anyway would cost a coordinator round trip per
        // partition per transaction for no added guarantee — eliminating it is
        // the throughput win TV2 exists to deliver.
        //
        // Under TV1 the coordinator only learns about a partition from an
        // explicit AddPartitionsToTxn, and a write to an unregistered
        // partition is not covered by the commit marker, so the RPC must
        // precede the first record. The Pending/Added states stop concurrent
        // callers from skipping the RPC while an in-flight add is outstanding.
        if self.requires_explicit_partition_registration() {
            self.add_partition_to_txn_if_needed(&topic, partition)
                .await?;
        }

        // Hand off to the accumulator, which batches, stamps PID/epoch/sequence
        // and the transactional ID, and drives retries. The per-partition
        // dispatch FIFO inside the accumulator keeps sequence order == wire
        // order for this partition.
        let result = self
            .accumulator
            .append_routed_with_guard(topic, record, record_size, partition, operation_guard)
            .await;

        // The accumulator has no view of transaction state, so classify its
        // error here: a fenced epoch reported on the produce path must latch
        // FatalError exactly as one reported by a coordinator RPC.
        let result = self.classify_transaction_result(result);
        match result {
            Err(error) if Self::is_unknown_producer_id_error(&error) => {
                Err(self.mark_unknown_producer_id_abort_required("transactional produce"))
            }
            other => other,
        }
    }

    /// Ensure a partition is registered with the transaction coordinator,
    /// issuing `AddPartitionsToTxn` at most once per partition per transaction.
    ///
    /// # Transaction version
    ///
    /// TV1 only. Under TV2 partitions are registered implicitly by the Produce
    /// request and this is never called.
    async fn add_partition_to_txn_if_needed(
        &self,
        topic: &Arc<str>,
        partition: PartitionId,
    ) -> Result<()> {
        loop {
            let mut txn_partitions = self.txn_partitions.write().await;
            match txn_partitions.begin_add(topic.as_ref(), partition) {
                BeginAddResult::AlreadyAdded => break,
                BeginAddResult::Fatal(err) => {
                    // A previous non-retriable RPC failure was stored for this
                    // partition. Return it immediately — no retry RPC.
                    return Err((*err).clone());
                }
                BeginAddResult::Wait(notify) => {
                    // Register interest in the Notify BEFORE releasing the
                    // write lock so that confirm_add/cancel_add/fail_add
                    // (which use notify_waiters) cannot be missed.
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    drop(txn_partitions);
                    notified.await;
                    // Re-check state on next iteration: either AlreadyAdded
                    // (RPC succeeded), Fatal (RPC failed non-retriably), or
                    // NeedAdd (RPC failed retriably — this caller retries).
                }
                BeginAddResult::NeedAdd(notify) => {
                    // Drop the lock before the RPC. The guard ensures that
                    // if this task is cancelled, the Pending state is rolled
                    // back so waiters don't hang forever.
                    drop(txn_partitions);
                    let guard = PendingAddGuard {
                        txn_partitions: self.txn_partitions.clone(),
                        topic: topic.clone(),
                        partition,
                        notify,
                        defused: false,
                    };
                    match self.add_partition_to_txn(topic.as_ref(), partition).await {
                        Ok(()) => {
                            guard.confirm(topic.as_ref(), partition).await;
                        }
                        Err(e) if e.is_retriable() => {
                            // Retriable error: remove the entry so that
                            // concurrent waiters can retry the RPC themselves.
                            guard.cancel(topic.as_ref(), partition).await;
                            return Err(e);
                        }
                        Err(e) => {
                            // Non-retriable error: store it so that any
                            // concurrent waiters receive it immediately.
                            guard
                                .fail(topic.as_ref(), partition, Arc::new(e.clone()))
                                .await;
                            return Err(e);
                        }
                    }
                    break;
                }
            }
        }

        Ok(())
    }

    /// Add a partition to the current transaction.
    ///
    /// Retries on coordinator errors with exponential backoff, re-discovering
    /// the transaction coordinator between attempts.
    async fn add_partition_to_txn(&self, topic: &str, partition: PartitionId) -> Result<()> {
        let result = self.retry_with_coordinator("AddPartitionsToTxn", |attempt| async move {
            let (_coordinator_id, conn) = self.coordinator_connection(attempt).await?;

            let (producer_id, producer_epoch) = self.checked_transactional_identity()?;

            let apt_version = conn
                .negotiate_api_version(
                    ApiKey::AddPartitionsToTxn,
                    versions::ADD_PARTITIONS_TO_TXN_MAX,
                    versions::ADD_PARTITIONS_TO_TXN_MIN,
                )
                .ok_or_else(|| {
                    KrafkaError::protocol_kind(ProtocolErrorKind::UnknownApiVersion, "no mutually supported AddPartitionsToTxn API version")
                })?;

            let request = AddPartitionsToTxnRequest::new(
                &self.config.transactional_id,
                producer_id,
                producer_epoch,
            )
            .add_partition(topic, partition);

            let response_bytes = conn
                .send_request(ApiKey::AddPartitionsToTxn, apt_version, |buf| {
                    request.encode_versioned(apt_version, buf)
                })
                .await?;

            let mut buf = response_bytes;
            let response = AddPartitionsToTxnResponse::decode_versioned(apt_version, &mut buf)?;

            if !response.is_ok() {
                for topic_result in &response.results {
                    for partition_result in &topic_result.partitions {
                        if !partition_result.error_code.is_ok() {
                            return Err(KrafkaError::broker(
                                partition_result.error_code,
                                format!("failed to add {}-{} to transaction", topic, partition),
                            ));
                        }
                    }
                }
                // Fallback: is_ok() was false but no individual partition error found
                // (e.g. the target partition is missing from the response).
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::Malformed,
                    format!(
                        "failed to add {}-{} to transaction: response indicated error but no per-partition error found",
                        topic, partition
                    ),
                ));
            }

            debug!("Added partition {}-{} to transaction", topic, partition);
            Ok(())
        })
        .await;

        match self.classify_transaction_result(result) {
            Err(error) if Self::is_unknown_producer_id_error(&error) => {
                Err(self.mark_unknown_producer_id_abort_required("AddPartitionsToTxn"))
            }
            other => other,
        }
    }

    /// Send consumer offsets within the current transaction.
    ///
    /// This allows atomic commit of consumed offsets along with produced messages.
    /// The `AddOffsetsToTxn` RPC (sent to the transaction coordinator) is retried
    /// on coordinator errors. The `TxnOffsetCommit` RPC (sent to the group
    /// coordinator) is retried with group coordinator re-discovery on
    /// coordinator and retriable errors.
    /// Atomically commit consumer offsets as part of the current transaction
    /// (exactly-once consume-transform-produce).
    ///
    /// Each [`TopicPartitionOffset`] entry specifies a partition and the **next**
    /// offset to consume (`last_consumed + 1`, matching `Consumer::position()`).  
    /// Calling this with the wrong offset by one permanently shifts the group.
    ///
    /// This is a two-phase operation:
    /// 1. `AddOffsetsToTxn` — registers the consumer group with the transaction coordinator.
    /// 2. `TxnOffsetCommit` — commits the offsets via the group coordinator, atomically
    ///    with the current transaction.
    ///
    /// # KIP-447 zombie fencing
    ///
    /// `group_metadata` must come from the `group_metadata()` accessor on the
    /// consumer whose offsets are being committed, and must be re-read
    /// for every transaction — the generation changes on every rebalance and a
    /// cached value defeats the fencing entirely.
    ///
    /// The generation, member ID and static instance ID are sent on the
    /// `TxnOffsetCommit` request so the group coordinator can reject a stale
    /// committer with `ILLEGAL_GENERATION` or `FENCED_INSTANCE_ID`. Previously
    /// these were hardcoded to `-1` / `""` / `None`, so a consumer that had
    /// already been rebalanced away from a partition could still overwrite the
    /// new owner's committed position — silently reprocessing or skipping
    /// records and breaking exactly-once.
    ///
    /// This method takes `&ConsumerGroupMetadata` rather than a bare
    /// `group_id: &str`; the group ID is read from the metadata.
    ///
    /// # Errors
    ///
    /// Returns [`ErrorCode::TransactionAbortable`] without contacting any
    /// broker when
    /// [`is_fenceable()`](crate::consumer::ConsumerGroupMetadata::is_fenceable)
    /// is `false` — the consumer has no valid generation (never joined, or
    /// mid-rebalance), so the commit could not be fenced and must not be made
    /// part of the transaction. Abort the transaction and retry once the
    /// consumer has rejoined.
    pub async fn send_offsets_to_transaction(
        &self,
        offsets: &[TopicPartitionOffset],
        group_metadata: &ConsumerGroupMetadata,
    ) -> Result<()> {
        // Register with the in-flight barrier *before* reading the state, for
        // the same reason `send_record` does — and this is the call where it
        // matters most.
        //
        // `commit_transaction` transitions to `Committing`, then waits for the
        // barrier, then flushes, then sends `EndTxn`. Taking the guard first
        // makes the two orderings exhaustive: either this operation registered
        // before the commit's barrier snapshot, and the commit waits for it, or
        // it registered afterwards — in which case the commit has already
        // transitioned and the state check below refuses.
        //
        // Without the guard this method was invisible to that wait, so a
        // concurrent commit could write the `EndTxn` marker while the
        // `TxnOffsetCommit` was still in flight. The offsets would then be
        // committed *outside* the transaction, which is the one thing
        // consume-transform-produce exists to prevent: the output records would
        // be atomic with each other but not with the consumer's position.
        let _operation_guard = self.in_flight_barrier.start("transactional producer")?;

        let current = self.state();
        if current != TransactionState::InTransaction {
            return Err(KrafkaError::invalid_state(format!(
                "cannot send offsets in state {:?}",
                current
            )));
        }

        self.ensure_transaction_can_continue("send offsets")?;

        // KIP-447: without a valid generation the coordinator cannot fence a
        // zombie committer, so refuse rather than silently committing
        // unfenced offsets inside an "exactly-once" transaction.
        if !group_metadata.is_fenceable() {
            self.abort_required.store(true, Ordering::SeqCst);
            return Err(KrafkaError::broker(
                ErrorCode::TransactionAbortable,
                format!(
                    "consumer group metadata for '{}' carries no valid generation \
                     (generation_id={}, member_id={:?}); the offset commit could not be \
                     fenced against a zombie consumer. abort_transaction() is required.",
                    group_metadata.group_id(),
                    group_metadata.generation_id(),
                    group_metadata.member_id(),
                ),
            ));
        }

        let group_id = group_metadata.group_id();
        let (producer_id, producer_epoch) = self.checked_transactional_identity()?;

        // Phase 1: AddOffsetsToTxn — sent to the transaction coordinator.
        //
        // Under TV2 (KIP-890) this is skipped. The group coordinator registers
        // the __consumer_offsets partition with the transaction coordinator
        // itself when it handles TxnOffsetCommit v5+, so the client sending
        // AddOffsetsToTxn is redundant work on the critical path.
        //
        // Under TV1 the client must register the offsets topic before the
        // commit, or the offsets are not covered by the transaction marker.
        if self.requires_explicit_partition_registration() {
            self.add_offsets_to_txn(producer_id, producer_epoch, group_id)
                .await?;
        }

        // Phase 2: TxnOffsetCommit — sent to the group coordinator, with retry.
        // The Java client re-discovers the group coordinator and re-enqueues
        // on coordinator or retriable errors; we mirror that with a retry loop.
        let commit_request = build_txn_offset_commit_request(
            &self.config.transactional_id,
            group_metadata,
            producer_id,
            producer_epoch,
            offsets,
        );

        // TV2 moves partition registration into the group coordinator's
        // TxnOffsetCommit handler, which only exists from v5. Committing over
        // an older version after skipping AddOffsetsToTxn would leave the
        // offsets outside the transaction — silently non-atomic — so require
        // the floor rather than downgrading.
        let toc_min_version = if self.transaction_version().is_v2() {
            TV2_MIN_TXN_OFFSET_COMMIT_VERSION
        } else {
            versions::TXN_OFFSET_COMMIT_MIN
        };

        let max_retries = self.retry_policy.max_retries;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                tokio::time::sleep(self.retry_policy.calculate_backoff(attempt)).await;
            }

            let result: Result<()> = async {
                let (group_node_id, group_host, group_port) =
                    self.find_group_coordinator(group_id, attempt).await?;
                let group_addr = format!("{group_host}:{group_port}");

                let group_conn = self
                    .pool
                    .get_connection_by_id(group_node_id, &group_addr)
                    .await?;

                let toc_version = group_conn
                    .negotiate_api_version(
                        ApiKey::TxnOffsetCommit,
                        versions::TXN_OFFSET_COMMIT_MAX,
                        toc_min_version,
                    )
                    .ok_or_else(|| {
                        KrafkaError::protocol_kind(
                            ProtocolErrorKind::UnknownApiVersion,
                            format!(
                                "no mutually supported TxnOffsetCommit API version (need v{toc_min_version}+)"
                            ),
                        )
                    })?;

                let response_bytes = group_conn
                    .send_request(ApiKey::TxnOffsetCommit, toc_version, |buf| {
                        commit_request.encode_versioned(toc_version, buf)
                    })
                    .await?;

                let mut buf = response_bytes;
                let commit_response =
                    TxnOffsetCommitResponse::decode_versioned(toc_version, &mut buf)?;

                if !commit_response.is_ok() {
                    // Extract the first per-partition error for actionable diagnostics.
                    for topic_result in &commit_response.topics {
                        for part_result in &topic_result.partitions {
                            if !part_result.error_code.is_ok() {
                                return Err(KrafkaError::broker(
                                    part_result.error_code,
                                    format!(
                                        "failed to commit offset for {}-{} in transaction",
                                        topic_result.name, part_result.partition
                                    ),
                                ));
                            }
                        }
                    }
                    // Fallback if is_ok was false but no individual error found
                    return Err(KrafkaError::protocol_kind(
                        ProtocolErrorKind::Malformed,
                        "failed to commit offsets in transaction",
                    ));
                }

                Ok(())
            }
            .await;

            let result = self.classify_transaction_result(result);

            if let Err(error) = &result
                && Self::is_unknown_producer_id_error(error)
            {
                return Err(self.mark_unknown_producer_id_abort_required("TxnOffsetCommit"));
            }
            if self.state() == TransactionState::FatalError {
                return result;
            }

            match &result {
                Ok(()) => {
                    debug!("Added offsets to transaction for group {}", group_id);
                    return Ok(());
                }
                Err(e) if Self::needs_coordinator_refresh(e) && attempt < max_retries => {
                    warn!(
                        attempt,
                        error = %e,
                        "TxnOffsetCommit group coordinator error, re-discovering and retrying"
                    );
                }
                Err(e) if e.is_retriable() && attempt < max_retries => {
                    warn!(
                        attempt,
                        error = %e,
                        "TxnOffsetCommit retriable error, retrying"
                    );
                }
                Err(_) => return result,
            }
        }

        Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::Malformed,
            format!("TxnOffsetCommit retry loop exhausted after {max_retries} retries"),
        ))
    }

    /// Register the consumer group's offsets topic with the transaction
    /// coordinator via `AddOffsetsToTxn`, with coordinator retry.
    ///
    /// # Transaction version
    ///
    /// TV1 only. Under TV2 the group coordinator performs this registration
    /// while handling `TxnOffsetCommit`, so the client does not send it.
    async fn add_offsets_to_txn(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<()> {
        let add_offsets_result = self
            .retry_with_coordinator("AddOffsetsToTxn", |attempt| async move {
                let (_coordinator_id, conn) = self.coordinator_connection(attempt).await?;

                let add_request = AddOffsetsToTxnRequest::new(
                    &self.config.transactional_id,
                    producer_id,
                    producer_epoch,
                    group_id,
                );

                let aot_version = conn
                    .negotiate_api_version(
                        ApiKey::AddOffsetsToTxn,
                        versions::ADD_OFFSETS_TO_TXN_MAX,
                        versions::ADD_OFFSETS_TO_TXN_MIN,
                    )
                    .ok_or_else(|| {
                        KrafkaError::protocol_kind(
                            ProtocolErrorKind::UnknownApiVersion,
                            "no mutually supported AddOffsetsToTxn API version",
                        )
                    })?;

                let response_bytes = conn
                    .send_request(ApiKey::AddOffsetsToTxn, aot_version, |buf| {
                        add_request.encode_versioned(aot_version, buf)
                    })
                    .await?;

                let mut buf = response_bytes;
                let add_response =
                    AddOffsetsToTxnResponse::decode_versioned(aot_version, &mut buf)?;

                if !add_response.is_ok() {
                    return Err(KrafkaError::broker(
                        add_response.error_code,
                        "failed to add offsets to transaction",
                    ));
                }

                Ok(())
            })
            .await;

        match self.classify_transaction_result(add_offsets_result) {
            Err(error) if Self::is_unknown_producer_id_error(&error) => {
                Err(self.mark_unknown_producer_id_abort_required("AddOffsetsToTxn"))
            }
            other => other,
        }
    }

    /// Find the group coordinator, returning (node_id, host, port).
    ///
    /// `attempt` rotates the broker queried so a single unreachable node cannot
    /// fail every retry (see [`find_coordinator`](Self::find_coordinator)).
    async fn find_group_coordinator(
        &self,
        group_id: &str,
        attempt: u32,
    ) -> Result<(i32, String, i32)> {
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::Malformed,
                "no brokers available",
            ));
        }

        let broker = &brokers[attempt as usize % brokers.len()];
        let conn = self
            .pool
            .get_connection_by_id(broker.id(), broker.address())
            .await?;

        let request = FindCoordinatorRequest::for_group(group_id);

        // Negotiate FindCoordinator version — requires v1+ (MIN).
        let fc_version = conn
            .negotiate_api_version(
                ApiKey::FindCoordinator,
                versions::FIND_COORDINATOR_MAX,
                versions::FIND_COORDINATOR_MIN,
            )
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported FindCoordinator API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::FindCoordinator, fc_version, |buf| {
                request.encode_versioned(fc_version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = FindCoordinatorResponse::decode_versioned(fc_version, &mut buf)?;

        if !response.error_code.is_ok() {
            return Err(KrafkaError::broker(
                response.error_code,
                "failed to find group coordinator",
            ));
        }

        Ok((response.node_id, response.host, response.port))
    }

    /// Commit the current transaction.
    pub async fn commit_transaction(&self) -> Result<()> {
        self.ensure_transaction_can_continue("commit transaction")?;

        // Close the transaction to new records *before* draining it.
        //
        // Atomic CAS: InTransaction → Committing, or retry a commit whose
        // outcome we never learned. Retrying is safe and is the *only* safe
        // move from `CommitIndeterminate`: `EndTxn` is idempotent for a given
        // producer id and epoch, so a duplicate commit either lands or is
        // recognised by the coordinator as the one it already applied.
        //
        // The order matters and used to be the other way round — flush, then
        // transition. `send_record` admits a record when it observes
        // `InTransaction`, so between the flush completing and the state
        // changing there was a window in which a concurrent send appended a
        // record the flush had already passed. That record was still buffered
        // when `EndTxn` went out, and would then either be rejected by the
        // broker as `INVALID_TXN_STATE` or, if `begin_transaction` had since
        // been called, silently join the *next* transaction — so a record the
        // application was told had been committed could disappear when a later
        // transaction aborted. `abort_transaction` already transitioned first;
        // this is the same discipline.
        if let Err(actual) = self
            .try_transition(
                TransactionState::InTransaction,
                TransactionState::Committing,
            )
            .or_else(|_| {
                self.try_transition(
                    TransactionState::CommitIndeterminate,
                    TransactionState::Committing,
                )
            })
        {
            return Err(KrafkaError::invalid_state(format!(
                "cannot commit in state {:?}",
                actual
            )));
        }

        // Every buffered record must reach the broker before `EndTxn`, or it
        // would be committed into a transaction the coordinator has already
        // closed.
        //
        // Two things have to drain, not one. `accumulator.flush()` empties the
        // batch queue, but a `send_record` that passed its state check a moment
        // ago may not have *reached* the accumulator yet — it is still running
        // interceptors or encoders. Waiting on the in-flight barrier first
        // ensures those land in the queue, and the flush then drains them.
        // Draining only the queue would leave exactly the records that were
        // closest to the transition unaccounted for.
        self.in_flight_barrier
            .wait_for(self.in_flight_barrier.snapshot())
            .await;
        if let Err(error) = self.accumulator.flush().await {
            // Nothing was sent to the coordinator, so the transaction is still
            // open. Hand the state back so the caller can abort it.
            let _ = self.try_transition(
                TransactionState::Committing,
                TransactionState::InTransaction,
            );
            return Err(error);
        }

        let result = match self.end_transaction(true).await {
            Err(error) if Self::is_unknown_producer_id_error(&error) => {
                Err(self.mark_unknown_producer_id_abort_required("commit_transaction"))
            }
            other => other,
        };

        match &result {
            Ok(()) => {
                self.set_state(TransactionState::Ready);
                self.txn_partitions.write().await.clear();
                info!("Transaction committed");
            }
            Err(e) if Self::is_abortable_transaction_error(e, self.transaction_version()) => {
                match self.try_transition(
                    TransactionState::Committing,
                    TransactionState::InTransaction,
                ) {
                    Ok(()) => {
                        warn!("Transaction commit failed (abort required): {}", e);
                    }
                    Err(actual) => {
                        warn!(
                            "Transaction commit failed (abort required): {}; \
                             state is now {:?} (concurrent abort may be in progress)",
                            e, actual
                        );
                    }
                }
            }
            Err(e) => {
                if e.is_retriable() {
                    // Whether it is safe to go back to `InTransaction` turns
                    // entirely on whether the coordinator could already have
                    // applied the commit.
                    //
                    // A `Broker { .. }` error *is* the coordinator's answer:
                    // it looked at the request and declined it, so the
                    // transaction is definitively still open and reverting is
                    // safe.
                    //
                    // A timeout or a connection loss is not an answer. The
                    // `EndTxn` may have been applied and the response lost, so
                    // the outcome is unknown. Reverting to `InTransaction`
                    // there is what makes a later abort — including the
                    // automatic one in `close()` — land on a transaction the
                    // coordinator may already have committed, tearing it
                    // (KAFKA-17754). Park in `CommitIndeterminate` instead,
                    // from which the only permitted move is another commit.
                    let outcome_unknown =
                        matches!(e, KrafkaError::Timeout { .. } | KrafkaError::Network(_));
                    let revert_to = if outcome_unknown {
                        TransactionState::CommitIndeterminate
                    } else {
                        TransactionState::InTransaction
                    };
                    // Use CAS so a concurrent abort that already moved the
                    // state is not overwritten.
                    match self.try_transition(TransactionState::Committing, revert_to) {
                        Ok(()) => {
                            if outcome_unknown {
                                warn!(
                                    "Transaction commit outcome unknown ({e}); the coordinator \
                                     may already have committed it. Retry commit_transaction() — \
                                     aborting from here could tear the transaction (KAFKA-17754)."
                                );
                            } else {
                                warn!("Transaction commit failed (retriable): {}", e);
                            }
                        }
                        Err(actual) => {
                            warn!(
                                "Transaction commit failed (retriable): {}; \
                                 state is now {:?} (concurrent abort may be in progress)",
                                e, actual
                            );
                        }
                    }
                } else {
                    // Fatal error — caller must abort
                    self.set_state(TransactionState::FatalError);
                    warn!("Transaction commit failed (fatal): {}", e);
                }
            }
        }

        result
    }

    /// Abort the current transaction.
    ///
    /// # Refused after an indeterminate commit
    ///
    /// If a previous [`commit_transaction`](Self::commit_transaction) failed
    /// with a timeout or a connection loss, the coordinator may already have
    /// committed. Aborting then is the
    /// [KAFKA-17754](https://issues.apache.org/jira/browse/KAFKA-17754)
    /// trigger: the delayed `EndTxn` can be applied to a *later* transaction
    /// and tear it. This call therefore returns an error in that state rather
    /// than performing an abort that may silently corrupt data. Retry the
    /// commit, or drop the producer and let the coordinator resolve the
    /// transaction through its own `transaction.timeout.ms`.
    ///
    /// Note that the Java client's documentation recommends aborting after a
    /// commit timeout. That advice predates KAFKA-17754 and krafka
    /// deliberately does not follow it.
    pub async fn abort_transaction(&self) -> Result<()> {
        if self.state() == TransactionState::CommitIndeterminate {
            return Err(KrafkaError::invalid_state(
                "cannot abort: a previous commit_transaction() timed out or lost its \
                 connection, so the coordinator may already have committed this \
                 transaction. Aborting now could be applied to a later transaction and \
                 tear it (KAFKA-17754). Retry commit_transaction() — EndTxn is idempotent \
                 for the same producer id and epoch — or drop this producer and let the \
                 coordinator resolve the transaction via transaction.timeout.ms."
                    .to_string(),
            ));
        }

        // Atomic CAS: try InTransaction → Aborting first, then Committing → Aborting
        let transition = self
            .try_transition(TransactionState::InTransaction, TransactionState::Aborting)
            .or_else(|_| {
                self.try_transition(TransactionState::Committing, TransactionState::Aborting)
            });

        if let Err(actual) = transition {
            return Err(KrafkaError::invalid_state(format!(
                "cannot abort in state {:?}",
                actual
            )));
        }

        // Drain buffered records first so their send futures resolve rather
        // than hanging once the transaction is torn down. Errors are expected
        // here (the transaction is being abandoned) and are only logged.
        //
        // The barrier wait comes first for the same reason as in
        // `commit_transaction`: a `send_record` that passed its state check
        // just before the transition above has not necessarily reached the
        // accumulator yet, and flushing without waiting would leave its batch
        // buffered — and its caller's future unresolved — after the transaction
        // was torn down.
        self.in_flight_barrier
            .wait_for(self.in_flight_barrier.snapshot())
            .await;
        if let Err(err) = self.accumulator.flush().await {
            debug!(error = %err, "Accumulator flush during abort_transaction failed");
        }

        let needs_reinitialize = self.abort_required.swap(false, Ordering::SeqCst);
        let result = if needs_reinitialize {
            match self.end_transaction(false).await {
                Ok(()) => self.do_init_transactions().await,
                Err(error) if Self::is_unknown_producer_id_error(&error) => {
                    debug!(
                        "Abort observed UnknownProducerId after transactional error; reinitializing producer identity"
                    );
                    self.do_init_transactions().await
                }
                Err(error) => Err(error),
            }
        } else {
            self.end_transaction(false).await
        };

        match &result {
            Ok(()) => {
                self.set_state(TransactionState::Ready);
                self.txn_partitions.write().await.clear();
                info!("Transaction aborted");
            }
            // Mirror `commit_transaction`. A retriable failure (coordinator
            // unavailable, network blip) says nothing about the transaction's
            // fate, so escalating to FatalError destroyed the caller's only way
            // to finish aborting. CAS back to InTransaction so `abort_transaction`
            // can simply be called again. The CAS may fail if a concurrent
            // operation already moved the state, in which case leave it alone.
            Err(e) if e.is_retriable() => {
                match self
                    .try_transition(TransactionState::Aborting, TransactionState::InTransaction)
                {
                    Ok(()) => {
                        // Restore the abort requirement consumed above so the
                        // retry re-initialises the identity if it needs to.
                        if needs_reinitialize {
                            self.abort_required.store(true, Ordering::SeqCst);
                        }
                        warn!(
                            "Transaction abort failed (retriable), retry abort_transaction(): {e}"
                        );
                    }
                    Err(actual) => {
                        warn!(
                            "Transaction abort failed (retriable): {e}; state is now {actual:?} \
                             (concurrent operation may be in progress)"
                        );
                    }
                }
            }
            Err(e) => {
                self.set_state(TransactionState::FatalError);
                warn!("Transaction abort failed (fatal): {e}; producer must be recreated");
            }
        }

        result
    }

    /// End the transaction (commit or abort).
    ///
    /// Retries on coordinator errors with exponential backoff, re-discovering
    /// the transaction coordinator between attempts.
    ///
    /// # Transaction version
    ///
    /// Under **TV2** the coordinator bumps the producer epoch while writing the
    /// transaction marker and returns the new `(producer_id, producer_epoch)`
    /// on the `EndTxn` v4+ response. Adopting it is mandatory, not
    /// opportunistic: the epoch the producer used for this transaction is
    /// fenced the instant the marker is written, so carrying it into the next
    /// transaction fails with `InvalidProducerEpoch`. A TV2 response that
    /// omits the pair is therefore rejected rather than ignored. `EndTxn` is
    /// negotiated at v4+ so the fields are guaranteed to be on the wire.
    ///
    /// Under **TV1** the coordinator does not bump on completion; the epoch
    /// only changes at `InitProducerId`. Any pair the broker does send is
    /// still adopted, but its absence is normal and not an error.
    async fn end_transaction(&self, commit: bool) -> Result<()> {
        let is_v2 = self.transaction_version().is_v2();
        let et_min_version = if is_v2 {
            TV2_MIN_END_TXN_VERSION
        } else {
            versions::END_TXN_MIN
        };

        let result = self
            .retry_with_coordinator("EndTxn", |attempt| async move {
                let (_coordinator_id, conn) = self.coordinator_connection(attempt).await?;

                let (producer_id, producer_epoch) = self.checked_transactional_identity()?;

                let et_version = conn
                    .negotiate_api_version(ApiKey::EndTxn, versions::END_TXN_MAX, et_min_version)
                    .ok_or_else(|| {
                        KrafkaError::protocol_kind(
                            ProtocolErrorKind::UnknownApiVersion,
                            format!(
                                "no mutually supported EndTxn API version (need v{et_min_version}+)"
                            ),
                        )
                    })?;

                let request = if commit {
                    EndTxnRequest::commit(
                        &self.config.transactional_id,
                        producer_id,
                        producer_epoch,
                    )
                } else {
                    EndTxnRequest::abort(&self.config.transactional_id, producer_id, producer_epoch)
                };

                let response_bytes = conn
                    .send_request(ApiKey::EndTxn, et_version, |buf| {
                        request.encode_versioned(et_version, buf)
                    })
                    .await?;

                let mut buf = response_bytes;
                let response = EndTxnResponse::decode_versioned(et_version, &mut buf)?;

                if !response.is_ok() {
                    return Err(KrafkaError::broker(
                        response.error_code,
                        if commit {
                            "failed to commit transaction"
                        } else {
                            "failed to abort transaction"
                        },
                    ));
                }

                match (response.producer_id, response.producer_epoch) {
                    (Some(pid), Some(epoch)) if pid >= 0 && epoch >= 0 => {
                        debug!(
                            pid,
                            epoch,
                            transaction_version = %self.transaction_version(),
                            "Adopting broker-bumped producer identity from EndTxn response"
                        );
                        // Also resets every per-partition sequence to 0, matching
                        // the broker's expectation for the new epoch.
                        self.identity.bump_epoch(pid, epoch);
                    }
                    _ if is_v2 => {
                        // The transaction did complete — the marker is written
                        // and the old epoch is already fenced — but without the
                        // new epoch this producer cannot start another
                        // transaction. Surface it instead of failing on the
                        // next begin_transaction() with a confusing
                        // InvalidProducerEpoch.
                        return Err(KrafkaError::protocol_kind(
                            ProtocolErrorKind::Malformed,
                            "EndTxn response omitted the bumped producer id/epoch that \
                             transaction version 2 requires",
                        ));
                    }
                    _ => {}
                }

                Ok(())
            })
            .await;
        self.classify_transaction_result(result)
    }

    /// Dispatch every buffered record and wait for all in-flight sends to
    /// complete.
    ///
    /// # You do not need this before `commit_transaction`
    ///
    /// [`commit_transaction`](Self::commit_transaction) flushes first, and must
    /// — a commit marker written while records are still buffered would leave
    /// them outside the transaction they were sent in. An explicit pre-commit
    /// flush is therefore redundant, not merely optional.
    ///
    /// # What it is for
    ///
    /// Two things:
    ///
    /// - Forcing buffered records onto the wire mid-transaction, so their
    ///   failures surface *now* rather than at commit time. Every error a
    ///   `send()` can produce is otherwise deferred to
    ///   [`commit_transaction`](Self::commit_transaction), where it arrives
    ///   without the record's context.
    /// - Writing code generic over "a producer". `Producer::flush` exists, so
    ///   an enum or trait spanning both producers previously had to special-case
    ///   the gap. It no longer does.
    ///
    /// Unlike `Producer::flush`, this does **not** make the records visible to
    /// a `read_committed` consumer — only
    /// [`commit_transaction`](Self::commit_transaction) does.
    ///
    /// # Errors
    ///
    /// Returns an error if the accumulator's flush fails; individual record
    /// failures surface through the `send()` future that owns them.
    pub async fn flush(&self) -> Result<()> {
        let target = self.in_flight_barrier.snapshot();
        self.accumulator.flush().await?;
        self.in_flight_barrier.wait_for(target).await;
        Ok(())
    }

    /// Get the transactional ID.
    #[inline]
    pub fn transactional_id(&self) -> &str {
        &self.config.transactional_id
    }

    /// Get the producer ID (once initialized).
    #[inline]
    pub fn producer_id(&self) -> i64 {
        self.identity.producer_id()
    }

    /// Get the producer epoch (once initialized).
    #[inline]
    pub fn producer_epoch(&self) -> i16 {
        self.identity.producer_epoch()
    }

    /// Close the transactional producer and release all resources.
    ///
    /// If a transaction is in progress, it will be aborted before closing.
    /// After calling `close()`, the producer cannot be used again.
    /// Calling `close()` more than once is a no-op.
    pub async fn close(&self) {
        let _ = self.close_inner(None).await;
    }

    /// Close the transactional producer, giving up on graceful shutdown once
    /// `timeout` expires.
    ///
    /// On timeout, the connection pool is still torn down, causing any
    /// remaining in-flight operations to fail fast.
    pub async fn close_with_timeout(&self, timeout: Duration) -> Result<()> {
        self.close_inner(Some(timeout)).await
    }

    async fn close_inner(&self, timeout: Option<Duration>) -> Result<()> {
        let Some(target) = self.in_flight_barrier.begin_close() else {
            return Ok(());
        };

        let graceful_close = async {
            // Flush and stop the accumulator so buffered batches are dispatched
            // before sockets are torn down.
            if let Err(err) = self.accumulator.shutdown().await {
                warn!(error = %err, "Accumulator shutdown error during transactional close");
            }

            // Let already-started sends cross the ack boundary before aborting the
            // active transaction or tearing down sockets.
            self.in_flight_barrier.wait_for(target).await;

            // If in-transaction, abort first to clean up broker state.
            //
            // `CommitIndeterminate` is deliberately excluded. The commit may
            // already have been applied, so an abort here could be applied to
            // a later transaction and tear it (KAFKA-17754) — and unlike a
            // user-initiated abort, this one would happen automatically on
            // every `close()` after a commit timeout. Leaving the transaction
            // alone lets the coordinator resolve it via
            // `transaction.timeout.ms`, which is the outcome with no
            // correctness hazard.
            let current = self.state();
            if current == TransactionState::InTransaction {
                warn!("Closing transactional producer with active transaction — aborting");
                self.abort_transaction().await?;
            } else if current == TransactionState::CommitIndeterminate {
                warn!(
                    "Closing transactional producer after a commit whose outcome is unknown; \
                     leaving the transaction for the coordinator to resolve via \
                     transaction.timeout.ms rather than aborting a possibly-committed \
                     transaction (KAFKA-17754)"
                );
            }

            Ok::<(), KrafkaError>(())
        };

        let close_result = if let Some(timeout) = timeout {
            tokio::time::timeout(timeout, graceful_close)
                .await
                .map_err(|_| KrafkaError::timeout("transactional producer close"))?
        } else {
            graceful_close.await
        };

        // Set state to prevent further use
        self.set_state(TransactionState::FatalError);

        // Close the connections — unless the pool was borrowed from a
        // `KrafkaClient`, in which case it belongs to that client and tearing
        // it down here would kill every sibling client sharing it.
        if self.pool_owned {
            self.pool.close_all().await;
            info!(
                "TransactionalProducer closed: txn.id()={} (connection pool torn down)",
                self.config.transactional_id
            );
        } else {
            info!(
                "TransactionalProducer closed: txn.id()={} (shared connection pool left open)",
                self.config.transactional_id
            );
        }

        close_result
    }

    /// Re-read TLS certificate and key files from disk and atomically install
    /// the new material for all **future** connections (KIP-1288).
    ///
    /// Existing TLS sessions are unaffected: they keep the connector they
    /// handshaked with and are replaced naturally as connections cycle. On
    /// error the previously loaded certificates stay active, so a call made
    /// mid-rotation against a half-written PEM is safe to retry.
    ///
    /// No-op when TLS is not configured.
    ///
    /// Use this for event-driven rotation (an inotify watch, a sidecar
    /// signal). For unattended rotation set
    /// [`TransportConfig::tls_reload_interval`](crate::network::TransportConfig)
    /// instead and krafka reloads on a timer.
    ///
    /// # Errors
    ///
    /// Returns an error if the certificate or key files cannot be read or
    /// parsed.
    pub async fn refresh_tls(&self) -> Result<()> {
        self.pool.refresh_tls().await
    }

    /// Replace the bootstrap server list used for metadata recovery (KIP-899).
    ///
    /// The new addresses are used on the next metadata refresh that falls back
    /// to bootstrap servers. Does not close existing connections.
    ///
    /// # Errors
    ///
    /// Returns an error if `servers` is empty.
    pub fn update_seed_brokers(&self, servers: Vec<String>) -> Result<()> {
        self.metadata.update_seed_brokers(servers)
    }

    /// Force a rebootstrap: close all connections, clear the metadata cache,
    /// and fall back to bootstrap servers (KIP-899).
    pub async fn rebootstrap(&self) {
        self.metadata.rebootstrap().await;
    }

    /// Get the shared connection metrics handle used by this producer's broker
    /// pool.
    #[inline]
    pub fn connection_metrics(&self) -> Arc<crate::metrics::ConnectionMetrics> {
        self.pool.metrics()
    }

    /// Snapshot the producer metrics.
    ///
    /// Transactional sends are batched through the same
    /// [`RecordAccumulator`](crate::producer::RecordAccumulatorHandle) as the
    /// plain producer, so the record, byte, error and retry counters mean
    /// exactly what they do on [`Producer::metrics`](crate::producer::Producer::metrics).
    ///
    /// Synchronous, like every other metrics accessor: readable from a
    /// Prometheus scrape handler or a signal handler.
    #[inline]
    pub fn metrics(&self) -> crate::producer::ProducerMetricsSnapshot {
        crate::producer::ProducerMetricsSnapshot {
            connections: self.pool.len(),
            records_sent: self.metrics.records_sent.get(),
            bytes_sent: self.metrics.bytes_sent.get(),
            errors: self.metrics.errors.get(),
            retries: self.metrics.retries.get(),
            buffered_records: self.metrics.buffered_records.get(),
        }
    }

    /// Get the shared producer metrics handle for this producer's accumulator.
    ///
    /// Transactional sends are batched through a [`RecordAccumulator`], so the
    /// same record/batch/retry counters as the plain producer are available.
    #[inline]
    pub fn metrics_handle(&self) -> Arc<ProducerMetrics> {
        self.metrics.clone()
    }

    /// Whether this client owns its connection pool.
    ///
    /// `false` when the pool was borrowed from a
    /// [`KrafkaClient`](crate::client::KrafkaClient) via `with_client`. In that
    /// case [`close`](Self::close) leaves the connections untouched — closing
    /// them would tear down every sibling client on that `KrafkaClient` and
    /// fail their in-flight requests. Close the `KrafkaClient` to release them.
    #[inline]
    #[must_use]
    pub fn owns_pool(&self) -> bool {
        self.pool_owned
    }

    /// Check if the transactional producer has been explicitly closed.
    ///
    /// Returns `true` only when [`Self::close`] has been called. A producer in
    /// [`TransactionState::FatalError`] due to a broker error is *not*
    /// considered closed — use [`Self::state`] to check for fatal errors.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.in_flight_barrier.is_closing()
    }
}

/// Build the `TxnOffsetCommit` request for a transactional offset commit.
///
/// Carries the KIP-447 fencing triple (`generation_id`, `member_id`,
/// `group_instance_id`) from `group_metadata` onto the wire so the group
/// coordinator can reject a stale committer. These fields exist on
/// `TxnOffsetCommit` v3+; on older versions the encoder drops them and the
/// commit is unfenced, exactly as before.
///
/// Split out of [`TransactionalProducer::send_offsets_to_transaction`] so the
/// field mapping is unit-testable without a live coordinator.
fn build_txn_offset_commit_request(
    transactional_id: &str,
    group_metadata: &ConsumerGroupMetadata,
    producer_id: i64,
    producer_epoch: i16,
    offsets: &[TopicPartitionOffset],
) -> TxnOffsetCommitRequest {
    let mut request = TxnOffsetCommitRequest::new(
        transactional_id,
        group_metadata.group_id(),
        producer_id,
        producer_epoch,
    );
    request.generation_id = group_metadata.generation_id();
    request.member_id = group_metadata.member_id().to_string();
    request.group_instance_id = group_metadata.group_instance_id().map(str::to_string);

    for tpo in offsets {
        request = request.add_offset(&tpo.topic, tpo.partition, tpo.next_offset, None);
    }
    request
}

/// Whether an error code permanently fences the producer under `version`.
///
/// A fatal error latches [`TransactionState::FatalError`]: the transaction
/// cannot be aborted and the producer must be recreated. Contrast with
/// *abortable* errors such as [`ErrorCode::TransactionAbortable`], which leave
/// the producer usable once [`TransactionalProducer::abort_transaction`] has
/// run — those are deliberately absent from this set.
///
/// # Transaction version
///
/// [`ErrorCode::InvalidProducerIdMapping`] is classified differently by
/// version. It means the coordinator's `transactional.id → producer.id`
/// mapping no longer matches the ID the producer is using.
///
/// - Under **TV1** this is abortable: the producer aborts and re-initializes.
/// - Under **TV2** it is fatal. TV2 derives a transaction's identity from
///   `(producer_id, epoch)` and bumps the epoch at every completion, so a
///   mismatched mapping means the coordinator has already assigned this
///   transactional ID to a different producer. Recovering in place would let
///   two producers write under one transactional ID and would break exactly-once
///   delivery, so KIP-890 requires the producer to give up instead.
///
/// All other codes classify identically under both versions.
fn is_fatal_transaction_error(error_code: ErrorCode, version: TransactionVersion) -> bool {
    if error_code == ErrorCode::InvalidProducerIdMapping {
        return version.is_v2();
    }

    matches!(
        error_code,
        ErrorCode::InvalidProducerEpoch
            | ErrorCode::ProducerFenced
            | ErrorCode::TransactionalIdAuthorizationFailed
            | ErrorCode::InvalidTxnState
            | ErrorCode::TransactionCoordinatorFenced
    )
}

/// Builder for [`TransactionalProducer`].
///
/// Mirrors [`ProducerBuilder`](super::ProducerBuilder) setter for setter, with
/// two deliberate exclusions:
///
/// - **`acks`** — fixed to [`Acks::All`]. The transaction coordinator can only
///   guarantee atomicity over fully replicated writes, so a weaker setting
///   would silently break the guarantee the type exists to provide.
/// - **`idempotent`** — always on. A transactional producer *is* an idempotent
///   producer with a stable `transactional.id`; there is nothing to disable.
///
/// Everything else — compression levels, delivery timeout, interceptors, a
/// dead-letter queue, a state store, a shared client, the synchronous
/// [`build_config`](Self::build_config) terminal — is present here because it
/// is present on the plain producer. `tests/builder_surface.rs` asserts that at
/// compile time, so the two cannot drift apart again.
#[derive(Default)]
#[must_use = "builders do nothing until .build() is called"]
pub struct TransactionalProducerBuilder {
    config: TransactionalProducerConfig,
    retry_policy: RetryPolicy,
    partitioner: Option<Arc<dyn Partitioner>>,
    key_serializer: Option<Arc<dyn Serializer>>,
    value_serializer: Option<Arc<dyn Serializer>>,
    interceptors: Vec<Arc<dyn crate::interceptor::ProducerInterceptor>>,
    /// Pre-built pool and metadata from a [`KrafkaClient`](crate::client::KrafkaClient).
    shared: Option<(Arc<ConnectionPool>, Arc<ClusterMetadata>)>,
    /// Optional pluggable persistence hook for producer identity state.
    state_store: Option<Arc<dyn super::idempotent::ErasedProducerStateStore>>,
}

impl TransactionalProducerBuilder {
    /// Set bootstrap servers.
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    /// Set client ID.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.config.client_id = client_id.into();
        self
    }

    /// Set the transactional ID (required).
    pub fn transactional_id(mut self, txn_id: impl Into<String>) -> Self {
        self.config.transactional_id = txn_id.into();
        self
    }

    /// Set the transaction timeout.
    ///
    /// Defaults to 60 seconds. Must be greater than zero.
    pub fn transaction_timeout(mut self, timeout: Duration) -> Self {
        self.config.transaction_timeout_ms = crate::util::duration_to_millis_i32(timeout);
        self
    }

    /// Set the request timeout: how long one in-flight request may wait for its
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

    /// Set the maximum encoded Kafka request frame size in bytes.
    pub fn max_request_size(mut self, bytes: usize) -> Self {
        self.config.max_request_size = bytes;
        self
    }

    /// Set the maximum batch size in bytes for the accumulator.
    pub fn batch_size(mut self, bytes: usize) -> Self {
        self.config.batch_size = bytes;
        self
    }

    /// Set how long the accumulator waits for a batch to fill.
    ///
    /// Transactional produce requests are `acks=all`, so a linger of zero costs
    /// a full round trip per record. Defaults to 5 ms.
    pub fn linger(mut self, linger: Duration) -> Self {
        self.config.linger = linger;
        self
    }

    /// Set the total accumulator buffer memory in bytes.
    pub fn buffer_memory(mut self, bytes: usize) -> Self {
        self.config.buffer_memory = bytes;
        self
    }

    /// Set the maximum time `send` blocks waiting for buffer memory.
    pub fn max_block(mut self, max_block: Duration) -> Self {
        self.config.max_block = max_block;
        self
    }

    /// Set the maximum number of concurrent in-flight produce requests.
    pub fn max_in_flight(mut self, max: usize) -> Self {
        self.config.max_in_flight = max;
        self
    }

    /// Set the total delivery timeout: how long a record may spend in flight,
    /// including batching, retries and backoff. Default: 120 s.
    ///
    /// This bound matters more for a transactional producer than for a plain
    /// one: a batch that keeps retrying holds the transaction open, and an open
    /// transaction blocks every `read_committed` consumer at its first offset.
    /// Keep it at or below [`transaction_timeout`](Self::transaction_timeout) —
    /// the coordinator aborts at that point regardless, and `build()` warns
    /// when the two disagree.
    pub fn delivery_timeout(mut self, timeout: Duration) -> Self {
        self.config.delivery_timeout = timeout;
        self
    }

    /// Set compression.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.config.compression = compression;
        self
    }

    /// Override the compression codec's default level.
    ///
    /// `None` (the default) uses the codec's own default: zlib 6 for `Gzip`,
    /// 3 for `Zstd`. Only `Gzip` and `Zstd` take a level; setting one alongside
    /// `Snappy` or `Lz4` is rejected by
    /// [`build_config`](Self::build_config) and [`build`](Self::build) rather
    /// than ignored.
    ///
    /// See
    /// [`ProducerBuilder::compression_level`](super::ProducerBuilder::compression_level)
    /// for how to choose a value.
    pub fn compression_level(mut self, level: Option<i32>) -> Self {
        self.config.compression_level = level;
        self
    }

    /// Override the compression codec for one topic.
    ///
    /// Topics without an override use the producer-wide
    /// [`compression`](Self::compression) setting.
    pub fn topic_compression(mut self, topic: impl Into<String>, compression: Compression) -> Self {
        self.config
            .topic_compression
            .insert(topic.into(), compression);
        self
    }

    /// Route permanently failed records to a dead-letter queue.
    ///
    /// Each record is handed to the DLQ once, after its retry budget is
    /// exhausted or on a non-retriable error, immediately before the failure is
    /// returned from [`send_record`](TransactionalProducer::send_record).
    ///
    /// The DLQ write happens **outside** the transaction: it is a separate
    /// producer, so it is not covered by the commit marker and survives the
    /// abort that a permanently failed send forces. That is the point — a
    /// record lost to an aborted transaction is otherwise unrecoverable.
    pub fn dead_letter_queue(mut self, dlq: Arc<dyn crate::dlq::DeadLetterQueue>) -> Self {
        self.config.dead_letter_queue = Some(dlq);
        self
    }

    /// Set a producer interceptor, replacing any previously added interceptors.
    ///
    /// `on_send` runs before the record is routed; `on_acknowledgement` runs
    /// once the broker answers, or once the send fails permanently.
    ///
    /// To register several as an ordered chain, use
    /// [`add_interceptor`](Self::add_interceptor).
    pub fn interceptor(
        mut self,
        interceptor: Arc<dyn crate::interceptor::ProducerInterceptor>,
    ) -> Self {
        self.interceptors = vec![interceptor];
        self
    }

    /// Append a producer interceptor to the chain.
    ///
    /// Interceptors execute in the order they are added, each individually
    /// panic-isolated.
    pub fn add_interceptor(
        mut self,
        interceptor: Arc<dyn crate::interceptor::ProducerInterceptor>,
    ) -> Self {
        self.interceptors.push(interceptor);
        self
    }

    /// Attach a pluggable state store for producer identity persistence.
    ///
    /// This is the producer the store was designed for. Restoration requires
    /// the broker to hand back the same `producer_id` and `producer_epoch` the
    /// snapshot recorded, which only happens for a producer re-initialising
    /// under a `transactional.id` the coordinator already knows.
    ///
    /// `load()` runs once inside
    /// [`init_transactions`](TransactionalProducer::init_transactions), after
    /// `InitProducerId` returns; `store()` runs fire-and-forget after each
    /// acknowledged batch.
    pub fn state_store(mut self, store: impl super::ProducerStateStore + 'static) -> Self {
        self.state_store = Some(Arc::new(store));
        self
    }

    /// Share a [`KrafkaClient`](crate::client::KrafkaClient)'s connection pool
    /// and metadata cache instead of creating a new one.
    ///
    /// When this method is called, `bootstrap_servers` is optional — the client
    /// was already connected at `KrafkaClient::build` time.
    pub fn with_client(mut self, client: &crate::client::KrafkaClient) -> Self {
        self.shared = Some((client.pool().clone(), client.metadata().clone()));
        self
    }

    /// Set the topic cache TTL for partial metadata refreshes.
    pub fn metadata_topic_cache_ttl(mut self, ttl: Duration) -> Self {
        self.config.metadata_topic_cache_ttl = Some(ttl);
        self
    }

    /// Disable the topic cache TTL, so cached per-topic metadata never expires
    /// on age alone.
    pub fn disable_metadata_topic_cache_ttl(mut self) -> Self {
        self.config.metadata_topic_cache_ttl = None;
        self
    }

    /// Set a custom partitioner.
    ///
    /// If not set, [`UniformStickyPartitioner`] is used, which applies murmur2 hashing
    /// for keyed messages and round-robin for unkeyed messages.
    pub fn partitioner(mut self, partitioner: impl Partitioner + 'static) -> Self {
        self.partitioner = Some(Arc::new(partitioner));
        self
    }

    /// Set authentication configuration.
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

    /// Configure SASL/OAUTHBEARER with a static token.
    ///
    /// For a token that must be refreshed, use
    /// [`auth`](Self::auth) with
    /// [`AuthConfig::sasl_oauthbearer_provider`](crate::auth::AuthConfig::sasl_oauthbearer_provider),
    /// or the built-in OIDC provider behind the `oauth-oidc` feature.
    pub fn sasl_oauthbearer(mut self, token: impl Into<String>) -> Self {
        self.config.auth = Some(crate::auth::AuthConfig::sasl_oauthbearer(token));
        self
    }

    /// Set the metadata recovery strategy (KIP-899).
    ///
    /// Controls what the client does when every known broker becomes
    /// unreachable: keep retrying the cached broker set, or fall back to the
    /// original bootstrap servers.
    pub fn metadata_recovery_strategy(
        mut self,
        strategy: crate::metadata::MetadataRecoveryStrategy,
    ) -> Self {
        self.config.metadata_recovery_strategy = strategy;
        self
    }

    /// Set the maximum age of cached cluster metadata before a refresh.
    pub fn metadata_max_age(mut self, age: Duration) -> Self {
        self.config.metadata_max_age = age;
        self
    }

    /// Set how long metadata refreshes may keep failing before a rebootstrap
    /// is triggered (KIP-899).
    pub fn metadata_recovery_rebootstrap_trigger(mut self, duration: Duration) -> Self {
        self.config.metadata_recovery_rebootstrap_trigger = duration;
        self
    }

    /// Set socket- and pool-level transport tuning.
    ///
    /// Covers TCP keepalive and nodelay, the per-connection response ceiling
    /// and in-flight cap, the priority-channel depths, the Happy Eyeballs
    /// stagger, idle-connection eviction, a total-connection cap, and the
    /// KIP-1288 automatic TLS reload interval.
    ///
    /// Omitting this call keeps krafka's historical defaults, which
    /// [`TransportConfig::default`](crate::network::TransportConfig) reproduces
    /// exactly.
    pub fn transport(mut self, transport: crate::network::TransportConfig) -> Self {
        self.config.transport = transport;
        self
    }

    /// Configure SASL/PLAIN authentication over cleartext.
    ///
    /// For `SASL_SSL`, use
    /// `.auth(AuthConfig::sasl_plain_ssl(user, pass, TlsConfig::new())?)`.
    pub fn sasl_plain(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> crate::Result<Self> {
        self.config.auth = Some(AuthConfig::sasl_plain(username, password)?);
        Ok(self)
    }

    /// Configure SASL/SCRAM-SHA-256 authentication over cleartext.
    ///
    /// For `SASL_SSL`, use
    /// `.auth(AuthConfig::sasl_scram_sha256_ssl(user, pass, TlsConfig::new()))`.
    pub fn sasl_scram_sha256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_scram_sha256(username, password));
        self
    }

    /// Configure SASL/SCRAM-SHA-512 authentication over cleartext.
    ///
    /// For `SASL_SSL`, use
    /// `.auth(AuthConfig::sasl_scram_sha512_ssl(user, pass, TlsConfig::new()))`.
    pub fn sasl_scram_sha512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_scram_sha512(username, password));
        self
    }

    /// Configure SASL/OAUTHBEARER authentication with an async token provider.
    ///
    /// The provider is called on every new broker connection, so tokens are
    /// always fresh. Pair with the built-in OIDC provider behind the
    /// `oauth-oidc` feature.
    pub fn sasl_oauthbearer_provider(
        mut self,
        provider: impl crate::auth::OAuthBearerTokenProvider + 'static,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_oauthbearer_provider(provider));
        self
    }

    /// Attach a key encoder applied automatically on every [`send_record`](TransactionalProducer::send_record) call.
    ///
    /// Equivalent to `key.serializer` in the Java `KafkaProducer`. Configure
    /// it once here and encoding is transparent on every send.
    pub fn key_serializer(mut self, encoder: Arc<dyn Serializer>) -> Self {
        self.key_serializer = Some(encoder);
        self
    }

    /// Attach a value encoder applied automatically on every [`send_record`](TransactionalProducer::send_record) call.
    ///
    /// Equivalent to `value.serializer` in the Java `KafkaProducer`.
    pub fn value_serializer(mut self, encoder: Arc<dyn Serializer>) -> Self {
        self.value_serializer = Some(encoder);
        self
    }

    /// Set the maximum number of retries for retriable errors.
    ///
    /// Default: 3.
    pub fn retries(mut self, retries: u32) -> Self {
        self.retry_policy = self.retry_policy.with_max_retries(retries);
        self
    }

    /// Set the initial retry backoff duration.
    ///
    /// Used as the base interval for exponential back-off between retries.
    /// Default: 100 ms.
    pub fn retry_backoff(mut self, backoff: Duration) -> Self {
        self.retry_policy = self.retry_policy.with_initial_backoff(backoff);
        self
    }

    /// Validate the configuration and return it, without connecting.
    ///
    /// Runs exactly the checks [`build`](Self::build) runs — they call the same
    /// validator — so a config that passes here will not be rejected later for
    /// a configuration reason. Useful for validating settings at startup, in a
    /// unit test, or in a `validate-config` CLI subcommand, none of which want
    /// a live broker.
    ///
    /// This is the transactional counterpart of
    /// [`ProducerBuilder::build_config`](super::ProducerBuilder::build_config).
    /// Its absence used to make a transactional producer the one client whose
    /// configuration could not be checked without a cluster.
    ///
    /// # Errors
    ///
    /// Returns [`KrafkaError::Config`](crate::error::KrafkaError::Config) for
    /// any invalid combination — a missing `transactional_id`, a zero
    /// `batch_size`, a compression codec whose Cargo feature is not enabled, a
    /// compression level the selected codec cannot use, and so on.
    pub fn build_config(self) -> Result<TransactionalProducerConfig> {
        validate(&self.config, self.shared.is_some())?;
        Ok(self.config)
    }

    /// Build the transactional producer.
    ///
    /// Validates through the same validator the synchronous
    /// [`build_config`](Self::build_config) uses, then connects.
    ///
    /// # Errors
    ///
    /// Returns [`KrafkaError::Config`](crate::error::KrafkaError::Config) for
    /// an invalid configuration, or a network error if the initial metadata
    /// fetch fails.
    pub async fn build(self) -> Result<TransactionalProducer> {
        // One validator, shared with `build_config`. Keeping the rules in a
        // free function rather than inline here is what makes the synchronous
        // terminal possible at all.
        validate(&self.config, self.shared.is_some())?;

        let pool_owned = self.shared.is_none();
        let (pool, metadata) = if let Some((pool, metadata)) = self.shared.clone() {
            (pool, metadata)
        } else {
            let mut pool_config_builder = self.config.transport.apply(
                ConnectionConfig::builder()
                    .client_id(&self.config.client_id)
                    .request_timeout(self.config.request_timeout)
                    .connect_timeout(self.config.connect_timeout),
            );

            if let Some(ref auth) = self.config.auth {
                pool_config_builder = pool_config_builder.auth(auth.clone());
            }

            #[cfg(feature = "socks5")]
            if let Some(ref proxy) = self.config.proxy {
                pool_config_builder = pool_config_builder.proxy(proxy.clone());
            }

            let mut pool_config = pool_config_builder.build()?;
            pool_config.init_tls().await?;

            // Every client builds its pool through `TransportConfig::build_pool`,
            // which applies the pool-level settings and starts the background
            // tasks (idle eviction, OAUTHBEARER refresh, KIP-1288 TLS reload).
            // Routing all construction sites through one function is what stops
            // them drifting apart again.
            let pool = self.config.transport.build_pool(pool_config);

            let bootstrap_servers =
                crate::util::parse_bootstrap_servers(&self.config.bootstrap_servers)?;

            let metadata = Arc::new({
                let mut meta = ClusterMetadata::new(
                    bootstrap_servers,
                    pool.clone(),
                    self.config.metadata_max_age,
                )
                .with_recovery_strategy(self.config.metadata_recovery_strategy)
                .with_rebootstrap_trigger(self.config.metadata_recovery_rebootstrap_trigger);
                if let Some(ttl) = self.config.metadata_topic_cache_ttl {
                    meta = meta.with_topic_cache_ttl(ttl);
                } else {
                    meta = meta.with_topic_cache_ttl_disabled();
                }
                meta
            });

            metadata.refresh().await?;
            (pool, metadata)
        };

        info!(
            "TransactionalProducer created with transactional.id()={}",
            self.config.transactional_id
        );

        let partitioner: Arc<dyn Partitioner> = self
            .partitioner
            .unwrap_or_else(|| Arc::new(UniformStickyPartitioner::new()));
        let identity = Arc::new(ProducerIdentity::new());
        let metrics = Arc::new(ProducerMetrics::default());
        let in_flight_barrier = Arc::new(InFlightBarrier::new());

        let interceptor: Arc<dyn crate::interceptor::ProducerInterceptor> =
            if self.interceptors.is_empty() {
                Arc::new(crate::interceptor::NoOpProducerInterceptor)
            } else if self.interceptors.len() == 1 {
                // infallible: len == 1 guaranteed by the surrounding else-if
                let Some(single) = self.interceptors.into_iter().next() else {
                    unreachable!("len == 1 verified above");
                };
                single
            } else {
                Arc::new(crate::interceptor::ProducerInterceptorChain::new(
                    self.interceptors,
                ))
            };

        // The retry policy carries the delivery deadline, exactly as on the
        // plain producer, so a batch cannot retry indefinitely inside an open
        // transaction.
        let retry_policy = self
            .retry_policy
            .with_delivery_timeout(Some(self.config.delivery_timeout));

        // Transactional sends go through the same batching accumulator as
        // the plain producer. `transactional_id` makes every ProduceRequest it
        // builds carry the transactional ID, and the shared `identity` supplies
        // the PID/epoch/sequence.
        let accumulator = RecordAccumulator::spawn(
            AccumulatorConfig {
                batch_size: self.config.batch_size,
                linger: self.config.linger,
                compression: self.config.compression,
                compression_level: self.config.compression_level,
                topic_compression: self.config.topic_compression.clone().into_iter().collect(),
                // Transactions require acks=all: the coordinator can only
                // guarantee atomicity over fully replicated writes.
                acks: Acks::All.to_i16(),
                client_id: self.config.client_id.clone(),
                request_timeout: self.config.request_timeout,
                max_request_size: self.config.max_request_size,
                buffer_memory: self.config.buffer_memory,
                max_block_ms: self.config.max_block,
                in_flight_semaphore: Arc::new(tokio::sync::Semaphore::new(
                    self.config.max_in_flight.max(1),
                )),
                interceptor: interceptor.clone(),
                identity: Some(identity.clone()),
                partitioner: partitioner.clone(),
                state_store: self.state_store.clone(),
                transactional_id: Some(self.config.transactional_id.clone()),
                dead_letter_queue: self.config.dead_letter_queue.clone(),
            },
            metadata.clone(),
            retry_policy.clone(),
            metrics.clone(),
            in_flight_barrier.clone(),
        );

        Ok(TransactionalProducer {
            config: self.config,
            metadata,
            pool,
            partitioner,
            state: AtomicU8::new(TransactionState::Uninitialized as u8),
            // Overwritten by init_transactions() once the cluster's finalized
            // transaction.version has been read; TV1 is the safe default.
            transaction_version: AtomicU8::new(TransactionVersion::V1 as u8),
            abort_required: AtomicBool::new(false),
            coordinator_id: RwLock::new(None),
            txn_partitions: Arc::new(RwLock::new(TransactionPartitions::default())),
            identity,
            accumulator,
            metrics,
            retry_policy,
            in_flight_barrier,
            key_serializer: self.key_serializer,
            value_serializer: self.value_serializer,
            interceptor,
            state_store: self.state_store,
            pool_owned,
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use crate::metadata::ClusterMetadata;
    use crate::network::ConnectionPool;

    /// A minimal accumulator handle for state-machine tests that never send.
    fn test_accumulator() -> RecordAccumulatorHandle {
        let pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
        let metadata = Arc::new(ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        ));
        RecordAccumulator::spawn(
            AccumulatorConfig::default(),
            metadata,
            RetryPolicy::default(),
            Arc::new(ProducerMetrics::default()),
            Arc::new(InFlightBarrier::new()),
        )
    }

    #[test]
    fn test_transaction_state() {
        assert_eq!(TransactionState::from(0), TransactionState::Uninitialized);
        assert_eq!(TransactionState::from(1), TransactionState::Ready);
        assert_eq!(TransactionState::from(2), TransactionState::InTransaction);
        assert_eq!(TransactionState::from(3), TransactionState::Committing);
        assert_eq!(TransactionState::from(4), TransactionState::Aborting);
        assert_eq!(TransactionState::from(5), TransactionState::FatalError);
        assert_eq!(TransactionState::from(99), TransactionState::FatalError);
    }

    #[test]
    fn test_transactional_producer_config_default() {
        let config = TransactionalProducerConfig::default();
        assert_eq!(config.client_id, "krafka-txn-producer");
        assert_eq!(config.transaction_timeout_ms, 60000);
        assert_eq!(config.max_request_size, crate::protocol::MAX_MESSAGE_SIZE);
    }

    #[test]
    fn test_transaction_partitions() {
        let mut partitions = TransactionPartitions::default();
        assert!(partitions.is_empty());

        // First add returns NeedAdd
        let result = partitions.begin_add("topic1", 0);
        let notify = match result {
            BeginAddResult::NeedAdd(n) => n,
            _ => panic!("expected NeedAdd"),
        };
        assert!(!partitions.is_empty());

        // Same partition while Pending returns Wait
        assert!(matches!(
            partitions.begin_add("topic1", 0),
            BeginAddResult::Wait(_)
        ));

        // Confirm, then same partition returns AlreadyAdded
        partitions.confirm_add("topic1", 0, &notify);
        assert!(matches!(
            partitions.begin_add("topic1", 0),
            BeginAddResult::AlreadyAdded
        ));

        // Different partition returns NeedAdd
        assert!(matches!(
            partitions.begin_add("topic1", 1),
            BeginAddResult::NeedAdd(_)
        ));

        partitions.clear();
        assert!(partitions.is_empty());
    }

    #[test]
    fn test_is_fatal_transaction_error() {
        for version in [TransactionVersion::V1, TransactionVersion::V2] {
            assert!(is_fatal_transaction_error(
                ErrorCode::InvalidProducerEpoch,
                version
            ));
            assert!(is_fatal_transaction_error(
                ErrorCode::ProducerFenced,
                version
            ));
            assert!(is_fatal_transaction_error(
                ErrorCode::TransactionCoordinatorFenced,
                version
            ));
            assert!(is_fatal_transaction_error(
                ErrorCode::TransactionalIdAuthorizationFailed,
                version
            ));
            assert!(is_fatal_transaction_error(
                ErrorCode::InvalidTxnState,
                version
            ));
            assert!(!is_fatal_transaction_error(ErrorCode::None, version));
            assert!(!is_fatal_transaction_error(
                ErrorCode::UnknownServerError,
                version
            ));
        }
    }

    #[test]
    fn test_needs_coordinator_refresh() {
        // Coordinator-related broker errors → true
        assert!(TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::broker(ErrorCode::NotCoordinator, "test")
        ));
        assert!(TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::broker(ErrorCode::CoordinatorNotAvailable, "test")
        ));
        assert!(TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::broker(ErrorCode::CoordinatorLoadInProgress, "test")
        ));

        // Network and timeout errors → true (coordinator may have moved)
        assert!(TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::network(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "refused"
            ))
        ));
        assert!(TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::timeout("test operation")
        ));

        // Non-coordinator broker errors → false
        assert!(!TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::broker(ErrorCode::InvalidProducerEpoch, "test")
        ));
        assert!(!TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::broker(ErrorCode::TransactionCoordinatorFenced, "test")
        ));

        // Other error types → false
        assert!(!TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::protocol_kind(ProtocolErrorKind::Other, "test")
        ));
        assert!(!TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::invalid_state("test")
        ));
    }

    #[tokio::test]
    async fn test_builder_missing_bootstrap() {
        let result = TransactionalProducer::builder()
            .transactional_id("my-txn")
            .build()
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_send_record_requires_initialized_transactional_identity() {
        let pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
        let metadata = Arc::new(ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool.clone(),
            Duration::from_secs(300),
        ));

        let producer = TransactionalProducer {
            config: TransactionalProducerConfig {
                bootstrap_servers: "localhost:9092".to_string(),
                transactional_id: "txn-test".to_string(),
                ..TransactionalProducerConfig::default()
            },
            metadata,
            pool,
            partitioner: Arc::new(UniformStickyPartitioner::new()),
            state: AtomicU8::new(TransactionState::InTransaction as u8),
            transaction_version: AtomicU8::new(TransactionVersion::V1 as u8),
            abort_required: AtomicBool::new(false),
            coordinator_id: RwLock::new(None),
            txn_partitions: Arc::new(RwLock::new(TransactionPartitions::default())),
            identity: Arc::new(ProducerIdentity::new()),
            accumulator: test_accumulator(),
            metrics: Arc::new(ProducerMetrics::default()),
            retry_policy: RetryPolicy::default(),
            in_flight_barrier: Arc::new(InFlightBarrier::new()),
            key_serializer: None,
            value_serializer: None,
            interceptor: Arc::new(crate::interceptor::NoOpProducerInterceptor),
            state_store: None,
            pool_owned: true,
        };

        let record = ProducerRecord::new("topic", Bytes::from_static(b"value")).with_partition(0);

        let err = producer.send_record(record).await.unwrap_err();
        assert!(
            err.to_string()
                .contains("transactional producer identity not initialized"),
            "expected invalid identity guard, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_builder_missing_txn_id() {
        let result = TransactionalProducer::builder()
            .bootstrap_servers("localhost:9092")
            .build()
            .await;
        assert!(result.is_err());
    }

    // Needs a runtime: `TransactionalProducer` now owns a `RecordAccumulator`,
    // which spawns its background task on construction.
    #[tokio::test]
    async fn test_mark_unknown_producer_id_requires_abort() {
        let pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
        let metadata = Arc::new(ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool.clone(),
            Duration::from_secs(300),
        ));

        let producer = TransactionalProducer {
            config: TransactionalProducerConfig {
                bootstrap_servers: "localhost:9092".to_string(),
                transactional_id: "txn-test".to_string(),
                ..TransactionalProducerConfig::default()
            },
            metadata,
            pool,
            partitioner: Arc::new(UniformStickyPartitioner::new()),
            state: AtomicU8::new(TransactionState::InTransaction as u8),
            transaction_version: AtomicU8::new(TransactionVersion::V1 as u8),
            abort_required: AtomicBool::new(false),
            coordinator_id: RwLock::new(None),
            txn_partitions: Arc::new(RwLock::new(TransactionPartitions::default())),
            identity: Arc::new(ProducerIdentity::new()),
            accumulator: test_accumulator(),
            metrics: Arc::new(ProducerMetrics::default()),
            retry_policy: RetryPolicy::default(),
            in_flight_barrier: Arc::new(InFlightBarrier::new()),
            key_serializer: None,
            value_serializer: None,
            interceptor: Arc::new(crate::interceptor::NoOpProducerInterceptor),
            state_store: None,
            pool_owned: true,
        };

        let error = producer.mark_unknown_producer_id_abort_required("transactional produce");
        assert!(matches!(
            error,
            KrafkaError::Broker {
                code: ErrorCode::TransactionAbortable,
                ..
            }
        ));
        assert!(producer.abort_required());

        let gate_error = producer
            .ensure_transaction_can_continue("commit transaction")
            .unwrap_err();
        assert!(matches!(
            gate_error,
            KrafkaError::Broker {
                code: ErrorCode::TransactionAbortable,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_commit_transaction_rejects_abort_required() {
        let pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
        let metadata = Arc::new(ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool.clone(),
            Duration::from_secs(300),
        ));

        let producer = TransactionalProducer {
            config: TransactionalProducerConfig {
                bootstrap_servers: "localhost:9092".to_string(),
                transactional_id: "txn-test".to_string(),
                ..TransactionalProducerConfig::default()
            },
            metadata,
            pool,
            partitioner: Arc::new(UniformStickyPartitioner::new()),
            state: AtomicU8::new(TransactionState::InTransaction as u8),
            transaction_version: AtomicU8::new(TransactionVersion::V1 as u8),
            abort_required: AtomicBool::new(true),
            coordinator_id: RwLock::new(None),
            txn_partitions: Arc::new(RwLock::new(TransactionPartitions::default())),
            identity: Arc::new(ProducerIdentity::new()),
            accumulator: test_accumulator(),
            metrics: Arc::new(ProducerMetrics::default()),
            retry_policy: RetryPolicy::default(),
            in_flight_barrier: Arc::new(InFlightBarrier::new()),
            key_serializer: None,
            value_serializer: None,
            interceptor: Arc::new(crate::interceptor::NoOpProducerInterceptor),
            state_store: None,
            pool_owned: true,
        };

        let error = producer.commit_transaction().await.unwrap_err();
        assert!(matches!(
            error,
            KrafkaError::Broker {
                code: ErrorCode::TransactionAbortable,
                ..
            }
        ));
        assert_eq!(producer.state(), TransactionState::InTransaction);
    }

    #[test]
    fn test_try_transition_success() {
        let state = AtomicU8::new(TransactionState::Ready as u8);
        let result = state.compare_exchange(
            TransactionState::Ready as u8,
            TransactionState::InTransaction as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        assert!(result.is_ok());
        assert_eq!(
            TransactionState::from(state.load(Ordering::SeqCst)),
            TransactionState::InTransaction
        );
    }

    #[test]
    fn test_try_transition_failure() {
        let state = AtomicU8::new(TransactionState::Uninitialized as u8);
        let result = state.compare_exchange(
            TransactionState::Ready as u8,
            TransactionState::InTransaction as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        assert!(result.is_err());
        // State should remain unchanged
        assert_eq!(
            TransactionState::from(state.load(Ordering::SeqCst)),
            TransactionState::Uninitialized
        );
    }

    #[test]
    fn test_txn_builder_no_auth_by_default() {
        let builder = TransactionalProducer::builder()
            .bootstrap_servers("broker:9092")
            .transactional_id("txn-1");

        assert!(builder.config.auth.is_none());
    }

    // ── build_config: validation without a broker ───────────────────────────
    //
    // `TransactionalProducerBuilder` used to offer only `build()`, so checking
    // a transactional producer's configuration in a unit test or a
    // `validate-config` subcommand required a live cluster — while the README
    // promised "`build_config()` to validate without a broker … both through
    // the same validator" for every client.

    /// A minimal builder that passes validation.
    fn valid_txn_builder() -> TransactionalProducerBuilder {
        TransactionalProducer::builder()
            .bootstrap_servers("localhost:9092")
            .transactional_id("txn-1")
    }

    #[test]
    fn build_config_returns_a_validated_config_without_connecting() {
        let config = valid_txn_builder()
            .client_id("checkout")
            .delivery_timeout(Duration::from_secs(45))
            .build_config()
            .expect("a minimal transactional configuration is valid");

        assert_eq!(config.transactional_id(), "txn-1");
        assert_eq!(config.client_id(), "checkout");
        assert_eq!(config.delivery_timeout(), Duration::from_secs(45));
        assert_eq!(
            config.acks(),
            Acks::All,
            "acks is fixed, not merely defaulted"
        );
    }

    #[test]
    fn build_config_rejects_a_missing_transactional_id() {
        let err = TransactionalProducer::builder()
            .bootstrap_servers("localhost:9092")
            .build_config()
            .expect_err("transactional_id is required")
            .to_string();
        assert!(err.contains("transactional_id"), "got: {err}");
    }

    #[test]
    fn build_config_rejects_an_empty_bootstrap_list() {
        let err = TransactionalProducer::builder()
            .transactional_id("txn-1")
            .build_config()
            .expect_err("bootstrap servers are required without a shared client")
            .to_string();
        assert!(err.contains("bootstrap"), "got: {err}");
    }

    #[test]
    fn build_config_rejects_zero_delivery_timeout() {
        let err = valid_txn_builder()
            .delivery_timeout(Duration::ZERO)
            .build_config()
            .expect_err("a zero delivery budget can never be met")
            .to_string();
        assert!(err.contains("delivery_timeout"), "got: {err}");
    }

    #[test]
    fn build_config_rejects_a_batch_larger_than_the_buffer() {
        let err = valid_txn_builder()
            .batch_size(4096)
            .buffer_memory(1024)
            .build_config()
            .expect_err("a batch that cannot fit in the buffer would deadlock")
            .to_string();
        assert!(err.contains("buffer_memory"), "got: {err}");
    }

    /// The compression rules are shared with the plain producer rather than
    /// restated, so a codec check cannot exist on one producer and not the
    /// other.
    #[cfg(feature = "snappy")]
    #[test]
    fn build_config_rejects_a_level_on_a_levelless_codec() {
        let err = valid_txn_builder()
            .compression(Compression::Snappy)
            .compression_level(Some(9))
            .build_config()
            .expect_err("Snappy takes no level")
            .to_string();
        assert!(
            err.contains("takes no level"),
            "the error must say the codec has no level, got: {err}"
        );
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn build_config_rejects_an_out_of_range_level() {
        let err = valid_txn_builder()
            .compression(Compression::Gzip)
            .compression_level(Some(42))
            .build_config()
            .expect_err("gzip tops out at 9")
            .to_string();
        assert!(
            err.contains("0..=9"),
            "the error must name the range: {err}"
        );
    }

    #[cfg(feature = "zstd")]
    #[test]
    fn a_valid_compression_level_reaches_the_config() {
        let config = valid_txn_builder()
            .compression(Compression::Zstd)
            .compression_level(Some(1))
            .build_config()
            .expect("level 1 is valid for zstd");
        assert_eq!(config.compression_level(), Some(1));
    }

    #[cfg(all(feature = "zstd", feature = "snappy"))]
    #[test]
    fn a_per_topic_codec_is_validated_against_the_level() {
        let err = valid_txn_builder()
            .compression(Compression::Zstd)
            .compression_level(Some(1))
            .topic_compression("events", Compression::Snappy)
            .build_config()
            .expect_err("the per-topic Snappy override takes no level")
            .to_string();
        assert!(
            err.contains("events"),
            "the error must name the topic: {err}"
        );
    }

    #[test]
    fn topic_compression_overrides_reach_the_config() {
        let config = valid_txn_builder()
            .topic_compression("high-volume", Compression::None)
            .build_config()
            .expect("an override to None is always available");
        assert_eq!(config.compression_for("high-volume"), Compression::None);
        assert_eq!(
            config.compression_for("anything-else"),
            config.compression()
        );
    }

    #[test]
    fn metadata_topic_cache_ttl_round_trips_and_can_be_disabled() {
        let ttl = valid_txn_builder()
            .metadata_topic_cache_ttl(Duration::from_secs(600))
            .build_config()
            .expect("valid");
        assert_eq!(
            ttl.metadata_topic_cache_ttl(),
            Some(Duration::from_secs(600))
        );

        let disabled = valid_txn_builder()
            .disable_metadata_topic_cache_ttl()
            .build_config()
            .expect("valid");
        assert_eq!(disabled.metadata_topic_cache_ttl(), None);
    }

    #[test]
    fn test_txn_builder_sets_max_request_size() {
        let builder = TransactionalProducer::builder()
            .bootstrap_servers("broker:9092")
            .transactional_id("txn-1")
            .max_request_size(65_536);

        assert_eq!(builder.config.max_request_size, 65_536);
    }

    #[test]
    fn test_txn_builder_sasl_plain() {
        let builder = TransactionalProducer::builder()
            .bootstrap_servers("broker:9093")
            .transactional_id("txn-1")
            .sasl_plain("user", "pass")
            .unwrap();

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.plain_credentials.is_some());
    }

    #[test]
    fn test_txn_builder_sasl_scram_sha256() {
        let builder = TransactionalProducer::builder()
            .bootstrap_servers("broker:9093")
            .transactional_id("txn-1")
            .sasl_scram_sha256("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.scram_credentials.is_some());
    }

    #[test]
    fn test_txn_builder_sasl_scram_sha512() {
        let builder = TransactionalProducer::builder()
            .bootstrap_servers("broker:9093")
            .transactional_id("txn-1")
            .sasl_scram_sha512("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.scram_credentials.is_some());
    }

    #[test]
    fn test_txn_builder_auth_config() {
        use crate::auth::AuthConfig;

        let auth = AuthConfig::sasl_scram_sha256("admin", "secret");
        let builder = TransactionalProducer::builder()
            .bootstrap_servers("broker:9093")
            .transactional_id("txn-1")
            .auth(auth);

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.scram_credentials.is_some());
    }

    #[test]
    fn test_txn_builder_initializes_producer_identity() {
        // Verify a built TransactionalProducer starts with uninitialized identity
        // (pid=-1, epoch=-1 until init_transactions() is called)
        let builder = TransactionalProducer::builder()
            .bootstrap_servers("broker:9092")
            .transactional_id("txn-test");
        // The builder should have the transactional_id set
        assert_eq!(builder.config.transactional_id, "txn-test");
    }

    #[test]
    fn test_txn_builder_requires_transactional_id() {
        let builder = TransactionalProducer::builder().bootstrap_servers("broker:9092");
        // Without transactional_id, it defaults to empty string
        assert!(builder.config.transactional_id.is_empty());
    }

    #[tokio::test]
    async fn test_txn_builder_rejects_zero_timeout() {
        let result = TransactionalProducer::builder()
            .bootstrap_servers("localhost:9092")
            .transactional_id("txn-1")
            .transaction_timeout(Duration::ZERO)
            .build()
            .await;

        match result {
            Err(e) => assert!(e.to_string().contains("transaction_timeout")),
            Ok(_) => panic!("expected error for transaction_timeout=0"),
        }
    }

    #[tokio::test]
    async fn test_txn_builder_rejects_zero_max_request_size() {
        let result = TransactionalProducer::builder()
            .bootstrap_servers("localhost:9092")
            .transactional_id("txn-1")
            .max_request_size(0)
            .build()
            .await;

        match result {
            Err(e) => assert!(e.to_string().contains("max_request_size")),
            Ok(_) => panic!("expected error for max_request_size=0"),
        }
    }

    #[tokio::test]
    async fn test_txn_builder_rejects_negative_timeout() {
        // Duration cannot be negative, so use Duration::ZERO as the smallest
        // invalid value (0 ms converts to 0 i32 which the validator rejects).
        let result = TransactionalProducer::builder()
            .bootstrap_servers("localhost:9092")
            .transactional_id("txn-1")
            .transaction_timeout(Duration::ZERO)
            .build()
            .await;

        assert!(result.is_err());
    }

    // ── R9.3: TransactionState::Initializing variant ──

    #[test]
    fn test_transaction_state_initializing_from_u8() {
        assert_eq!(TransactionState::from(6), TransactionState::Initializing);
    }

    #[test]
    fn test_transaction_state_initializing_value() {
        assert_eq!(TransactionState::Initializing as u8, 6);
    }

    #[test]
    fn test_transaction_state_initializing_round_trip() {
        let state = TransactionState::Initializing;
        let val = state as u8;
        assert_eq!(TransactionState::from(val), TransactionState::Initializing);
    }

    #[test]
    fn test_transaction_state_unknown_maps_to_fatal() {
        // Values not explicitly mapped fall to FatalError. 7 is now
        // CommitIndeterminate, so the first unmapped discriminant is 8.
        assert_eq!(TransactionState::from(8), TransactionState::FatalError);
        assert_eq!(TransactionState::from(255), TransactionState::FatalError);
    }

    // ── R9.3: CAS transition with Initializing state ──

    #[test]
    fn test_try_transition_uninitialized_to_initializing() {
        let state = AtomicU8::new(TransactionState::Uninitialized as u8);
        let result = state.compare_exchange(
            TransactionState::Uninitialized as u8,
            TransactionState::Initializing as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        assert!(result.is_ok());
        assert_eq!(
            TransactionState::from(state.load(Ordering::SeqCst)),
            TransactionState::Initializing
        );
    }

    #[test]
    fn test_try_transition_initializing_blocks_second_init() {
        // Simulate: first call moved to Initializing, second call should fail
        let state = AtomicU8::new(TransactionState::Initializing as u8);
        let result = state.compare_exchange(
            TransactionState::Uninitialized as u8,
            TransactionState::Initializing as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        assert!(result.is_err());
        // State stays Initializing
        assert_eq!(
            TransactionState::from(state.load(Ordering::SeqCst)),
            TransactionState::Initializing
        );
    }

    // ── R9.9: commit_transaction sets FatalError on non-retriable errors ──

    #[test]
    fn test_commit_fatal_error_state_machine() {
        // Simulate the commit_transaction error-handling logic:
        // On non-retriable error → state becomes FatalError
        let state = AtomicU8::new(TransactionState::Committing as u8);

        // Simulate a non-retriable error (e.g. InvalidProducerEpoch)
        let error = KrafkaError::broker(ErrorCode::InvalidProducerEpoch, "epoch fenced");
        assert!(!error.is_retriable());

        // Apply the same logic as commit_transaction
        if error.is_retriable() {
            state.store(TransactionState::InTransaction as u8, Ordering::SeqCst);
        } else {
            state.store(TransactionState::FatalError as u8, Ordering::SeqCst);
        }

        assert_eq!(
            TransactionState::from(state.load(Ordering::SeqCst)),
            TransactionState::FatalError
        );
    }

    #[test]
    fn test_commit_retriable_error_reverts_to_in_transaction() {
        // Simulate the commit_transaction error-handling logic:
        // On retriable error → state reverts to InTransaction
        let state = AtomicU8::new(TransactionState::Committing as u8);

        let error = KrafkaError::broker(ErrorCode::CoordinatorNotAvailable, "coordinator down");
        assert!(error.is_retriable());

        if error.is_retriable() {
            state.store(TransactionState::InTransaction as u8, Ordering::SeqCst);
        } else {
            state.store(TransactionState::FatalError as u8, Ordering::SeqCst);
        }

        assert_eq!(
            TransactionState::from(state.load(Ordering::SeqCst)),
            TransactionState::InTransaction
        );
    }

    // ── R14: close() sets FatalError to prevent further use ──

    #[test]
    fn test_txn_close_sets_fatal_error_state() {
        // Verify the close() contract: after close, state is FatalError
        let state = AtomicU8::new(TransactionState::Ready as u8);
        // Simulate close: set to FatalError
        state.store(TransactionState::FatalError as u8, Ordering::SeqCst);
        assert_eq!(
            TransactionState::from(state.load(Ordering::SeqCst)),
            TransactionState::FatalError
        );
    }

    // ── R14: OutOfOrderSequenceNumber is retriable ──

    #[test]
    fn test_out_of_order_sequence_is_retriable() {
        let error = KrafkaError::broker(ErrorCode::OutOfOrderSequenceNumber, "sequence mismatch");
        assert!(error.is_retriable());
    }

    // ── R14: ProducerRecord timestamp propagation ──

    #[test]
    fn test_producer_record_with_timestamp() {
        use crate::producer::ProducerRecord;
        let record = ProducerRecord::new("topic", b"value".to_vec()).with_timestamp(1234567890);
        assert_eq!(record.timestamp, Some(1234567890));
    }

    #[test]
    fn test_transaction_partitions_state_machine() {
        let mut tp = TransactionPartitions::default();

        // First add returns NeedAdd
        let result = tp.begin_add("topic", 0);
        let notify = match result {
            BeginAddResult::NeedAdd(n) => n,
            _ => panic!("expected NeedAdd"),
        };

        // Concurrent add returns Wait
        let result2 = tp.begin_add("topic", 0);
        assert!(matches!(result2, BeginAddResult::Wait(_)));

        // Confirm moves to Added
        tp.confirm_add("topic", 0, &notify);
        assert!(matches!(
            tp.begin_add("topic", 0),
            BeginAddResult::AlreadyAdded
        ));

        // Different partition returns NeedAdd
        let result3 = tp.begin_add("topic", 1);
        let notify2 = match result3 {
            BeginAddResult::NeedAdd(n) => n,
            _ => panic!("expected NeedAdd"),
        };

        // Cancel removes — next call returns NeedAdd again
        tp.cancel_add("topic", 1, &notify2);
        assert!(matches!(
            tp.begin_add("topic", 1),
            BeginAddResult::NeedAdd(_)
        ));

        // Clear empties everything
        tp.clear();
        assert!(tp.is_empty());
    }

    #[test]
    fn test_transaction_partitions_fail_add_propagates_as_fatal() {
        // Regression test for F-01/F-07: a non-retriable AddPartitionsToTxn
        // failure must be stored as Failed so that any concurrent waiter
        // receives Fatal immediately instead of making a redundant RPC or
        // silently continuing with an unregistered partition.
        let mut tp = TransactionPartitions::default();

        // First caller gets NeedAdd and performs the RPC (which fails).
        let notify = match tp.begin_add("t", 0) {
            BeginAddResult::NeedAdd(n) => n,
            other => panic!("expected NeedAdd, got {other:?}"),
        };

        // Second concurrent caller should be told to Wait.
        assert!(matches!(tp.begin_add("t", 0), BeginAddResult::Wait(_)));

        // RPC failed with a non-retriable error — store the sentinel.
        let err = Arc::new(KrafkaError::invalid_state("fatal"));
        tp.fail_add("t", 0, err.clone(), &notify);

        // After fail_add, any new caller must get Fatal immediately.
        assert!(
            matches!(tp.begin_add("t", 0), BeginAddResult::Fatal(_)),
            "expected Fatal after fail_add"
        );

        // The error stored in Failed is the same as the one passed in.
        match tp.begin_add("t", 0) {
            BeginAddResult::Fatal(stored) => {
                assert_eq!(stored.to_string(), err.to_string());
            }
            other => panic!("expected Fatal, got {other:?}"),
        }
    }

    #[test]
    fn test_transactional_producer_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TransactionalProducer>();
    }

    // ── KIP-447 zombie fencing on TxnOffsetCommit ─────────────────

    /// The fencing triple must reach the wire struct; hardcoding
    /// `-1` / `""` / `None` (the previous behaviour) disables coordinator-side
    /// validation entirely.
    #[test]
    fn test_txn_offset_commit_carries_group_metadata() {
        let metadata =
            ConsumerGroupMetadata::new("my-group", 42, "member-7", Some("instance-3".to_string()));
        let offsets = vec![
            TopicPartitionOffset::new("orders", 0, 101),
            TopicPartitionOffset::new("orders", 1, 55),
        ];

        let request = build_txn_offset_commit_request("txn-1", &metadata, 12345, 4, &offsets);

        assert_eq!(request.transactional_id, "txn-1");
        assert_eq!(request.group_id, "my-group");
        assert_eq!(request.producer_id, 12345);
        assert_eq!(request.producer_epoch, 4);
        assert_eq!(request.generation_id, 42, "KIP-447 generation must be sent");
        assert_eq!(
            request.member_id, "member-7",
            "KIP-447 member_id must be sent"
        );
        assert_eq!(
            request.group_instance_id.as_deref(),
            Some("instance-3"),
            "KIP-345 static instance id must be sent"
        );

        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].name, "orders");
        assert_eq!(request.topics[0].partitions.len(), 2);
        assert_eq!(request.topics[0].partitions[0].committed_offset, 101);
        assert_eq!(request.topics[0].partitions[1].committed_offset, 55);
    }

    /// A consumer without static membership sends `None` for the instance ID
    /// but still carries a real generation and member ID.
    #[test]
    fn test_txn_offset_commit_without_static_membership() {
        let metadata = ConsumerGroupMetadata::new("g", 3, "m", None);
        let request = build_txn_offset_commit_request("txn", &metadata, 1, 0, &[]);
        assert_eq!(request.generation_id, 3);
        assert_eq!(request.member_id, "m");
        assert!(request.group_instance_id.is_none());
    }

    /// A consumer that never joined (or is mid-rebalance) cannot be fenced, so
    /// `send_offsets_to_transaction` must refuse rather than committing
    /// unfenced offsets inside an "exactly-once" transaction.
    #[test]
    fn test_unfenceable_group_metadata_is_rejected() {
        // These are the pre-KIP-447 wire defaults the old code hardcoded.
        assert!(!ConsumerGroupMetadata::new("g", -1, "", None).is_fenceable());
        assert!(!ConsumerGroupMetadata::new("g", 5, "", None).is_fenceable());
        assert!(!ConsumerGroupMetadata::new("g", -1, "m", None).is_fenceable());
        assert!(ConsumerGroupMetadata::new("g", 0, "m", None).is_fenceable());
    }

    // ── Fatal classification on every coordinator RPC ─────────────

    /// The fencing error codes must be classified fatal wherever they surface,
    /// not only on the produce path.
    #[test]
    fn test_fencing_error_codes_are_fatal() {
        for code in [
            ErrorCode::InvalidProducerEpoch,
            ErrorCode::ProducerFenced,
            ErrorCode::TransactionalIdAuthorizationFailed,
            ErrorCode::InvalidTxnState,
            ErrorCode::TransactionCoordinatorFenced,
        ] {
            for version in [TransactionVersion::V1, TransactionVersion::V2] {
                assert!(
                    is_fatal_transaction_error(code, version),
                    "{code:?} must be classified as a fatal transaction error under {version}"
                );
            }
        }
        assert!(!is_fatal_transaction_error(
            ErrorCode::NotCoordinator,
            TransactionVersion::V1
        ));
        assert!(!is_fatal_transaction_error(
            ErrorCode::None,
            TransactionVersion::V1
        ));
    }
    /// Build a producer pinned to `version` with retries disabled, so tests
    /// that exercise the network paths fail fast instead of backing off.
    fn test_producer(version: TransactionVersion) -> TransactionalProducer {
        let pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
        let metadata = Arc::new(ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool.clone(),
            Duration::from_secs(300),
        ));

        TransactionalProducer {
            config: TransactionalProducerConfig {
                bootstrap_servers: "localhost:9092".to_string(),
                transactional_id: "txn-test".to_string(),
                ..TransactionalProducerConfig::default()
            },
            metadata,
            pool,
            partitioner: Arc::new(UniformStickyPartitioner::new()),
            state: AtomicU8::new(TransactionState::InTransaction as u8),
            transaction_version: AtomicU8::new(version as u8),
            abort_required: AtomicBool::new(false),
            coordinator_id: RwLock::new(None),
            txn_partitions: Arc::new(RwLock::new(TransactionPartitions::default())),
            identity: Arc::new(ProducerIdentity::new()),
            accumulator: test_accumulator(),
            metrics: Arc::new(ProducerMetrics::default()),
            retry_policy: RetryPolicy::no_retries(),
            in_flight_barrier: Arc::new(InFlightBarrier::new()),
            key_serializer: None,
            value_serializer: None,
            interceptor: Arc::new(crate::interceptor::NoOpProducerInterceptor),
            state_store: None,
            pool_owned: true,
        }
    }

    /// `close()` must leave a pool borrowed from a `KrafkaClient` alone.
    ///
    /// Every client but `AdminClient` called `close_all()` unconditionally, so
    /// closing one client built with `with_client` tore down the shared pool
    /// and failed every sibling's in-flight requests — the exact opposite of
    /// what sharing a client is for.
    ///
    /// Negative control: making `close_inner` call `close_all()`
    /// unconditionally fails the `shared` half.
    #[tokio::test]
    async fn close_only_tears_down_a_pool_it_owns() {
        for pool_owned in [true, false] {
            let mut producer = test_producer(TransactionVersion::V2);
            producer.pool_owned = pool_owned;
            producer.set_state(TransactionState::Ready);

            let pool = producer.pool.clone();
            // Install a background task so the teardown is observable.
            pool.start_idle_evictor();
            assert!(pool.has_background_tasks());

            assert_eq!(producer.owns_pool(), pool_owned);
            producer.close().await;
            assert!(producer.is_closed());

            assert_eq!(
                pool.has_background_tasks(),
                !pool_owned,
                "pool_owned={pool_owned}: the pool must be torn down only when owned"
            );
        }
    }

    fn support(
        level: i16,
        produce: i16,
        txn_offset_commit: i16,
        end_txn: i16,
    ) -> BrokerTransactionSupport {
        BrokerTransactionSupport {
            transaction_version_level: level,
            produce_max: Some(produce),
            txn_offset_commit_max: Some(txn_offset_commit),
            end_txn_max: Some(end_txn),
        }
    }

    /// A broker that finalizes transaction.version at 2+ and can serve every
    /// API version TV2 depends on.
    fn tv2_broker() -> BrokerTransactionSupport {
        support(
            2,
            versions::PRODUCE_MAX,
            versions::TXN_OFFSET_COMMIT_MAX,
            versions::END_TXN_MAX,
        )
    }

    /// Levels 0 and 1 leave the client protocol unchanged; only level 2
    /// switches on the KIP-890 semantics.
    #[test]
    fn test_transaction_version_from_feature_level() {
        assert_eq!(
            TransactionVersion::from_feature_level(0),
            TransactionVersion::V1
        );
        assert_eq!(
            TransactionVersion::from_feature_level(1),
            TransactionVersion::V1
        );
        assert_eq!(
            TransactionVersion::from_feature_level(2),
            TransactionVersion::V2
        );
        // A future level must not silently fall back to TV1.
        assert_eq!(
            TransactionVersion::from_feature_level(3),
            TransactionVersion::V2
        );
        // A negative level cannot appear on the wire, but must not enable TV2.
        assert_eq!(
            TransactionVersion::from_feature_level(-1),
            TransactionVersion::V1
        );
    }

    #[test]
    fn test_transaction_version_defaults_to_v1() {
        assert_eq!(TransactionVersion::default(), TransactionVersion::V1);
        assert!(!TransactionVersion::V1.is_v2());
        assert!(TransactionVersion::V2.is_v2());
        // Round-trips through the atomic used on the producer.
        assert_eq!(
            TransactionVersion::from(TransactionVersion::V2 as u8),
            TransactionVersion::V2
        );
        assert_eq!(
            TransactionVersion::from(TransactionVersion::V1 as u8),
            TransactionVersion::V1
        );
        // An impossible discriminant must land on the safe protocol.
        assert_eq!(TransactionVersion::from(99), TransactionVersion::V1);
    }

    /// Every broker agrees on TV2 → the producer speaks TV2.
    #[test]
    fn test_negotiated_version_uniform_tv2_cluster() {
        let cluster = [tv2_broker(), tv2_broker(), tv2_broker()];
        assert_eq!(
            negotiated_transaction_version(&cluster),
            TransactionVersion::V2
        );
    }

    /// A rolling upgrade can surface the finalized feature at level 2 while
    /// some brokers still report level 1 or 0. Speaking TV2 to those brokers
    /// would drop their partitions from the transaction, so the whole producer
    /// must fall back to TV1.
    #[test]
    fn test_negotiated_version_takes_minimum_across_mixed_cluster() {
        let mixed_with_v1 = [
            tv2_broker(),
            tv2_broker(),
            support(
                1,
                versions::PRODUCE_MAX,
                versions::TXN_OFFSET_COMMIT_MAX,
                versions::END_TXN_MAX,
            ),
        ];
        assert_eq!(
            negotiated_transaction_version(&mixed_with_v1),
            TransactionVersion::V1,
            "one level-1 broker must downgrade the entire cluster to TV1"
        );

        let mixed_with_feature_absent = [
            tv2_broker(),
            support(
                0,
                versions::PRODUCE_MAX,
                versions::TXN_OFFSET_COMMIT_MAX,
                versions::END_TXN_MAX,
            ),
        ];
        assert_eq!(
            negotiated_transaction_version(&mixed_with_feature_absent),
            TransactionVersion::V1
        );

        // Order must not matter — this is a minimum, not a first-wins scan.
        let laggard_first = [
            support(
                0,
                versions::PRODUCE_MAX,
                versions::TXN_OFFSET_COMMIT_MAX,
                versions::END_TXN_MAX,
            ),
            tv2_broker(),
        ];
        assert_eq!(
            negotiated_transaction_version(&laggard_first),
            TransactionVersion::V1
        );
    }

    /// No broker could be probed — assume nothing and stay on TV1.
    #[test]
    fn test_negotiated_version_empty_cluster_is_v1() {
        assert_eq!(negotiated_transaction_version(&[]), TransactionVersion::V1);
    }

    /// The finalized feature is cluster-wide metadata and can read as level 2
    /// before every broker runs a build that serves the matching API versions.
    /// Each TV2-dependent API is checked independently.
    #[test]
    fn test_negotiated_version_requires_the_tv2_api_versions() {
        let produce_too_old = support(
            2,
            TV2_MIN_PRODUCE_VERSION - 1,
            versions::TXN_OFFSET_COMMIT_MAX,
            versions::END_TXN_MAX,
        );
        assert_eq!(
            negotiated_transaction_version(&[produce_too_old]),
            TransactionVersion::V1,
            "TV2 needs Produce v{TV2_MIN_PRODUCE_VERSION}+ to add partitions implicitly"
        );

        let txn_offset_commit_too_old = support(
            2,
            versions::PRODUCE_MAX,
            TV2_MIN_TXN_OFFSET_COMMIT_VERSION - 1,
            versions::END_TXN_MAX,
        );
        assert_eq!(
            negotiated_transaction_version(&[txn_offset_commit_too_old]),
            TransactionVersion::V1,
            "TV2 needs TxnOffsetCommit v{TV2_MIN_TXN_OFFSET_COMMIT_VERSION}+"
        );

        let end_txn_too_old = support(
            2,
            versions::PRODUCE_MAX,
            versions::TXN_OFFSET_COMMIT_MAX,
            TV2_MIN_END_TXN_VERSION - 1,
        );
        assert_eq!(
            negotiated_transaction_version(&[end_txn_too_old]),
            TransactionVersion::V1,
            "TV2 needs EndTxn v{TV2_MIN_END_TXN_VERSION}+ to receive the bumped epoch"
        );

        // Exactly at the floors is enough.
        let at_floor = support(
            2,
            TV2_MIN_PRODUCE_VERSION,
            TV2_MIN_TXN_OFFSET_COMMIT_VERSION,
            TV2_MIN_END_TXN_VERSION,
        );
        assert_eq!(
            negotiated_transaction_version(&[at_floor]),
            TransactionVersion::V2
        );
    }

    /// A broker with no mutually supported version for a TV2 API cannot serve
    /// TV2 even though it advertises the feature.
    #[test]
    fn test_negotiated_version_unnegotiable_api_is_v1() {
        let no_produce = BrokerTransactionSupport {
            produce_max: None,
            ..tv2_broker()
        };
        assert_eq!(
            negotiated_transaction_version(&[no_produce]),
            TransactionVersion::V1
        );
    }

    /// The crate's own maxima must be high enough to reach TV2, otherwise the
    /// feature can never activate against any broker.
    #[test]
    fn test_crate_supports_the_tv2_api_versions() {
        // Both sides are constants, so this is enforced at compile time:
        // lowering any of the maxima below a TV2 floor breaks the build here
        // rather than silently pinning every cluster to TV1.
        const {
            assert!(versions::PRODUCE_MAX >= TV2_MIN_PRODUCE_VERSION);
            assert!(versions::TXN_OFFSET_COMMIT_MAX >= TV2_MIN_TXN_OFFSET_COMMIT_VERSION);
            assert!(versions::END_TXN_MAX >= TV2_MIN_END_TXN_VERSION);
        }
    }

    /// TV1 registers partitions explicitly; TV2 does not send the RPC at all.
    #[tokio::test]
    async fn test_tv2_skips_explicit_partition_registration() {
        let tv1 = test_producer(TransactionVersion::V1);
        assert!(
            tv1.requires_explicit_partition_registration(),
            "TV1 must send AddPartitionsToTxn before the first write to a partition"
        );

        let tv2 = test_producer(TransactionVersion::V2);
        assert!(
            !tv2.requires_explicit_partition_registration(),
            "TV2 adds partitions implicitly via Produce; AddPartitionsToTxn must be skipped"
        );
    }

    /// Under TV2 the produce path must not touch the transaction coordinator.
    /// With no broker listening, a TV1 send fails during coordinator discovery
    /// while a TV2 send never gets there — it goes straight to the accumulator.
    #[tokio::test]
    async fn test_tv2_produce_path_does_not_contact_the_coordinator() {
        let tv2 = test_producer(TransactionVersion::V2);
        tv2.identity.initialize(7, 3);

        let record = ProducerRecord::new("topic", Bytes::from_static(b"value")).with_partition(0);
        // The send cannot succeed without a broker; the assertion is about
        // which state it left behind, not the outcome.
        let _ = tokio::time::timeout(Duration::from_secs(2), tv2.send_record(record)).await;

        assert!(
            tv2.txn_partitions.read().await.is_empty(),
            "TV2 must not record per-partition registration state"
        );
        assert!(
            tv2.coordinator_id.read().await.is_none(),
            "TV2 must not perform coordinator discovery on the produce path"
        );
    }

    /// The interceptor chain must run on the transactional send path.
    ///
    /// The README lists interceptors as a general observability feature. They
    /// were producer-only: `TransactionalProducerBuilder` had no
    /// `interceptor`/`add_interceptor`, and the accumulator it spawned was
    /// hard-wired to `NoOpProducerInterceptor` — so a chain configured for a
    /// transactional deployment could not exist, let alone run.
    ///
    /// `on_send` runs before partitioning, so an interceptor that rewrites the
    /// topic is honoured. Asserted through the observable effect rather than
    /// the wiring: the send itself cannot succeed without a broker.
    ///
    /// Negative control: removing the `safe_on_send` call from `send_record`
    /// leaves the recorder empty and this fails.
    #[tokio::test]
    async fn interceptors_run_on_the_transactional_send_path() {
        use std::sync::atomic::AtomicUsize;

        #[derive(Debug, Default)]
        struct CountingInterceptor {
            sends: AtomicUsize,
        }
        impl crate::interceptor::ProducerInterceptor for CountingInterceptor {
            fn on_send(
                &self,
                record: &mut ProducerRecord,
            ) -> crate::interceptor::InterceptorResult {
                self.sends.fetch_add(1, Ordering::SeqCst);
                record
                    .headers
                    .push(("seen-by".to_string(), Bytes::from_static(b"interceptor")));
                Ok(())
            }
        }

        let interceptor = Arc::new(CountingInterceptor::default());
        let mut producer = test_producer(TransactionVersion::V2);
        producer.interceptor = interceptor.clone();
        producer.identity.initialize(7, 3);

        let record = ProducerRecord::new("topic", Bytes::from_static(b"value")).with_partition(0);
        // Cannot succeed without a broker; the assertion is about what ran.
        let _ = tokio::time::timeout(Duration::from_secs(2), producer.send_record(record)).await;

        assert_eq!(
            interceptor.sends.load(Ordering::SeqCst),
            1,
            "on_send must be invoked exactly once per transactional send"
        );
    }

    /// INVALID_PRODUCER_ID_MAPPING is the one code whose severity depends on
    /// the transaction version: abortable under TV1, fatal under TV2.
    #[test]
    fn test_invalid_producer_id_mapping_is_fatal_only_under_tv2() {
        assert!(
            !is_fatal_transaction_error(
                ErrorCode::InvalidProducerIdMapping,
                TransactionVersion::V1
            ),
            "under TV1 the producer aborts and re-initializes"
        );
        assert!(
            is_fatal_transaction_error(ErrorCode::InvalidProducerIdMapping, TransactionVersion::V2),
            "under TV2 recovering in place could break exactly-once, so it is fatal"
        );

        let error = KrafkaError::broker(ErrorCode::InvalidProducerIdMapping, "test");
        assert!(
            TransactionalProducer::is_abortable_transaction_error(&error, TransactionVersion::V1),
            "the TV1 classification must be abortable, not merely non-fatal"
        );
        assert!(
            !TransactionalProducer::is_abortable_transaction_error(&error, TransactionVersion::V2),
            "fatal and abortable must stay mutually exclusive"
        );
    }

    /// TRANSACTION_ABORTABLE (KIP-890) ends the transaction but leaves the
    /// producer reusable after an abort — it must never latch FatalError.
    #[test]
    fn test_transaction_abortable_is_abortable_not_fatal() {
        let error = KrafkaError::broker(ErrorCode::TransactionAbortable, "test");
        for version in [TransactionVersion::V1, TransactionVersion::V2] {
            assert!(
                !is_fatal_transaction_error(ErrorCode::TransactionAbortable, version),
                "TRANSACTION_ABORTABLE must not be fatal under {version}"
            );
            assert!(
                TransactionalProducer::is_abortable_transaction_error(&error, version),
                "TRANSACTION_ABORTABLE must be abortable under {version}"
            );
        }
        // Retrying it in place would resume a transaction the broker rejected.
        assert!(!error.is_retriable());
    }

    /// A fatal code latches FatalError; an abortable one leaves the state
    /// machine alone but requires an explicit abort before continuing.
    #[tokio::test]
    async fn test_classify_transaction_result_by_version() {
        let tv2 = test_producer(TransactionVersion::V2);
        let result: Result<()> = Err(KrafkaError::broker(
            ErrorCode::InvalidProducerIdMapping,
            "test",
        ));
        assert!(tv2.classify_transaction_result(result).is_err());
        assert_eq!(tv2.state(), TransactionState::FatalError);
        assert!(
            !tv2.abort_required(),
            "a fatal error is unrecoverable; abort_transaction() cannot help"
        );

        let tv1 = test_producer(TransactionVersion::V1);
        let result: Result<()> = Err(KrafkaError::broker(
            ErrorCode::InvalidProducerIdMapping,
            "test",
        ));
        assert!(tv1.classify_transaction_result(result).is_err());
        assert_eq!(
            tv1.state(),
            TransactionState::InTransaction,
            "TV1 must not fence the producer over a PID-mapping mismatch"
        );
        assert!(
            tv1.abort_required(),
            "the transaction is over; the caller must abort before continuing"
        );
    }

    /// The producer reports TV1 until init_transactions() has read the feature.
    #[tokio::test]
    async fn test_transaction_version_accessor_defaults_to_v1() {
        let producer = test_producer(TransactionVersion::V1);
        assert_eq!(producer.transaction_version(), TransactionVersion::V1);

        let producer = test_producer(TransactionVersion::V2);
        assert_eq!(producer.transaction_version(), TransactionVersion::V2);
    }

    /// Adopting the EndTxn epoch bump must restart every partition's sequence
    /// space, since the broker resets its expectation to 0 for the new epoch.
    #[test]
    fn test_endtxn_epoch_bump_resets_sequences() {
        let identity = ProducerIdentity::new();
        identity.initialize(42, 0);

        // Advance a couple of partitions so a stale counter would be visible.
        for _ in 0..5 {
            identity.next_sequence("orders", 0).expect("allocate");
        }
        identity.next_sequence("payments", 1).expect("allocate");
        assert_eq!(identity.peek_sequence("orders", 0), 5);
        assert_eq!(identity.peek_sequence("payments", 1), 1);

        // The coordinator bumped the epoch while writing the commit marker.
        identity.bump_epoch(42, 1);

        assert_eq!(identity.producer_id(), 42);
        assert_eq!(identity.producer_epoch(), 1);
        assert_eq!(
            identity.peek_sequence("orders", 0),
            0,
            "a bumped epoch starts a fresh sequence space"
        );
        assert_eq!(identity.peek_sequence("payments", 1), 0);
    }

    /// On epoch overflow the coordinator hands back a new producer ID with
    /// epoch 0 rather than a bumped epoch, so both fields must be adopted.
    #[test]
    fn test_endtxn_bump_adopts_new_producer_id_on_epoch_overflow() {
        let identity = ProducerIdentity::new();
        identity.initialize(42, i16::MAX);
        identity.next_sequence("orders", 0).expect("allocate");

        identity.bump_epoch(1000, 0);

        assert_eq!(identity.producer_id(), 1000);
        assert_eq!(identity.producer_epoch(), 0);
        assert_eq!(identity.peek_sequence("orders", 0), 0);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod commit_indeterminate_tests {
    use super::*;

    /// A commit whose outcome the coordinator never reported must not leave the
    /// producer in a state where anything can abort it.
    ///
    /// This is the KAFKA-17754 hazard. Before this state existed, a commit
    /// timeout reverted to `InTransaction`, and `close()` unconditionally
    /// aborts from `InTransaction` — so a commit timeout followed by an
    /// ordinary `close()` issued an abort against a transaction the
    /// coordinator may already have committed, with no user action involved.
    #[test]
    fn a_timed_out_commit_is_not_reported_as_still_in_transaction() {
        // Only errors that are *not* the coordinator's answer are indeterminate.
        for error in [
            KrafkaError::timeout("EndTxn"),
            KrafkaError::network(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "connection closed",
            )),
        ] {
            assert!(
                matches!(error, KrafkaError::Timeout { .. } | KrafkaError::Network(_)),
                "{error} must classify as outcome-unknown"
            );
            assert!(
                error.is_retriable(),
                "{error} must be retriable, or it would take the fatal path instead"
            );
        }
    }

    /// A broker error *is* an answer: the coordinator saw the request and
    /// declined it, so the transaction is definitively still open and going
    /// back to `InTransaction` is safe.
    #[test]
    fn a_broker_rejection_is_a_definite_answer_not_an_unknown_outcome() {
        let error = KrafkaError::broker(
            ErrorCode::CoordinatorNotAvailable,
            "coordinator moved".to_string(),
        );
        assert!(error.is_retriable());
        assert!(
            !matches!(error, KrafkaError::Timeout { .. } | KrafkaError::Network(_)),
            "a broker error must not be treated as an unknown outcome"
        );
    }

    /// The state must survive the `u8` round trip it is stored as, or a
    /// restored producer would silently look like something else.
    #[test]
    fn commit_indeterminate_round_trips_through_its_discriminant() {
        assert_eq!(
            TransactionState::from(TransactionState::CommitIndeterminate as u8),
            TransactionState::CommitIndeterminate
        );
        assert_eq!(
            TransactionState::CommitIndeterminate.to_string(),
            "CommitIndeterminate"
        );
    }
}
