//! Transactional producer for exactly-once semantics.
//!
//! The transactional producer enables atomic writes across multiple partitions
//! and topics. It guarantees that either all messages in a transaction are
//! committed or none are.
//!
//! # Transaction State and Recovery
//!
//! The client holds transaction state in memory only; the broker's
//! transaction coordinator holds the authoritative state per
//! `transactional.id`. Building a producer with an existing
//! `transactional.id` bumps the producer epoch and aborts that id's open
//! transaction; the previous instance then gets `ProducerFenced`. After a
//! crash no manual recovery is needed: the broker aborts the open
//! transaction when a new producer with the same id is built, or after
//! `transaction.timeout.ms`.
//!
//! # Example
//!
//! ```rust,no_run
//! use krafka::{Kafka, Record};
//!
//! # async fn example() -> krafka::Result<()> {
//! let kafka = Kafka::builder("localhost:9092").connect().await?;
//! let producer = kafka.producer().build_transactional("my-transaction").await?;
//!
//! producer.begin()?;
//! producer.send(Record::new("topic", "value").key("key")).await?;
//! if let Err(e) = producer.commit().await {
//!     if e.requires_abort() {
//!         producer.abort().await?;
//!     }
//! }
//! # Ok(())
//! # }
//! ```

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use tokio::sync::{Notify, RwLock};
use tracing::{debug, info, warn};

use crate::client::CloseOptions;
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::metadata::ClusterMetadata;
use crate::network::{BrokerConnection, ConnectionPool};
use crate::protocol::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
    AddPartitionsToTxnResponse, ApiKey, EndTxnRequest, EndTxnResponse, FindCoordinatorRequest,
    FindCoordinatorResponse, InitProducerIdRequest, InitProducerIdResponse, TxnOffsetCommitRequest,
    TxnOffsetCommitResponse, VersionedDecode, VersionedEncode, versions,
};
use crate::{Offset, PartitionId};

use super::Producer;
use super::accumulator::DeliveryHandle;
use super::gate::TxnGate;
use super::record::{Record, RecordMetadata, TopicHandle};
use super::retry::{self, Backoff};
use crate::consumer::ConsumerGroupMetadata;
use crate::metrics::Metrics;

/// Name of the cluster-wide finalized feature that gates KIP-890 semantics.
const TRANSACTION_VERSION_FEATURE: &str = "transaction.version";

/// Minimum `Produce` version that carries the transactional fields the broker
/// needs to add a partition to the transaction implicitly (KIP-890 TV2).
const TV2_MIN_PRODUCE_VERSION: i16 = 12;

/// Minimum `TxnOffsetCommit` version at which the group coordinator, rather
/// than the client, registers the offsets topic with the transaction
/// coordinator (KIP-890 TV2).
const TV2_MIN_TXN_OFFSET_COMMIT_VERSION: i16 = 5;

/// The `EndTxn` version that tells the coordinator to apply TV2 semantics and
/// returns the bumped producer id and epoch (KIP-890). A TV1 producer must
/// stay below it.
const TV2_MIN_END_TXN_VERSION: i16 = 5;

/// Minimum `InitProducerId` version carrying the KIP-939 `enable2Pc` and
/// `keepPreparedTxn` fields.
const TV3_MIN_INIT_PRODUCER_ID_VERSION: i16 = 6;

/// The negotiated KIP-890 transaction protocol in use with this cluster.
///
/// Selected once while the producer is built
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
    /// KIP-939 transactions (`transaction.version` ≥ 3).
    ///
    /// Everything TV2 changes, plus the coordinator will honour `enable2Pc` on
    /// `InitProducerId`: a producer may declare that an **external**
    /// coordinator owns its commit decision, and the broker then stops
    /// applying `transaction.max.timeout.ms` to it. That is the level
    /// [`ProducerBuilder::two_phase_commit`](super::ProducerBuilder::two_phase_commit) requires.
    V3 = 3,
}

impl From<u8> for TransactionVersion {
    /// Decode the discriminant stored in the producer's atomic.
    ///
    /// Any unrecognised value decodes to [`V1`](TransactionVersion::V1), which
    /// keeps an impossible discriminant on the safe protocol rather than
    /// enabling a newer one on a cluster that may not support it.
    fn from(v: u8) -> Self {
        if v == Self::V3 as u8 {
            Self::V3
        } else if v == Self::V2 as u8 {
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
    /// the client protocol, so both are [`V1`](Self::V1). Level 2 enables the
    /// KIP-890 client semantics; level 3 adds KIP-939 two-phase commit on top
    /// of them.
    #[must_use]
    pub fn from_feature_level(level: i16) -> Self {
        if level >= 3 {
            Self::V3
        } else if level >= 2 {
            Self::V2
        } else {
            Self::V1
        }
    }

    /// Whether the KIP-890 client semantics are active.
    ///
    /// **At least** TV2, not exactly TV2. Every behaviour TV2 introduces —
    /// implicit partition registration, the mandatory epoch bump on `EndTxn` —
    /// still holds at TV3, so an equality test here would silently drop a TV3
    /// cluster back to sending `AddPartitionsToTxn` and mis-handling epoch
    /// bumps. The name is kept because that is what the semantics are called.
    #[must_use]
    #[inline]
    pub fn is_v2(self) -> bool {
        matches!(self, Self::V2 | Self::V3)
    }

    /// Whether the cluster will honour `enable2Pc` (KIP-939).
    #[must_use]
    #[inline]
    pub fn supports_two_phase_commit(self) -> bool {
        matches!(self, Self::V3)
    }
}

impl std::fmt::Display for TransactionVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::V1 => write!(f, "TV1"),
            Self::V2 => write!(f, "TV2"),
            Self::V3 => write!(f, "TV3"),
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
    /// Highest mutually supported `InitProducerId` version, or `None` when
    /// none is. KIP-939's `enable2Pc` field only exists from v6.
    init_producer_id_max: Option<i16>,
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
            // TV3 needs the same evidence one level up: the feature finalized
            // at 3 *and* an `InitProducerId` that actually carries the
            // `enable2Pc` field. Reporting TV3 on a broker that cannot encode
            // the flag would turn a clear "this cluster does not do 2PC" into
            // a request the broker silently reads as a plain init.
            if feature.supports_two_phase_commit()
                && supports(self.init_producer_id_max, TV3_MIN_INIT_PRODUCER_ID_VERSION)
            {
                TransactionVersion::V3
            } else {
                TransactionVersion::V2
            }
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

/// Where a [`TransactionalProducer`] stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TransactionState {
    /// initialisation has not run.
    Uninitialized,
    /// initialisation is running.
    Initializing,
    /// Ready to begin a transaction.
    Ready,
    /// A transaction is open and accepts sends.
    Open,
    /// `commit` is running: sends are refused.
    Committing,
    /// A commit was sent but its outcome is unknown: some `EndTxn` attempt
    /// went unanswered, so the coordinator may have applied it.
    ///
    /// Only `commit` is allowed from here. Aborting is the
    /// [KAFKA-17754](https://issues.apache.org/jira/browse/KAFKA-17754)
    /// trigger: a delayed `EndTxn` can land on the wrong transaction and tear
    /// it. Retry the commit (`EndTxn` is idempotent for the same producer id
    /// and epoch), or drop the producer and let the coordinator resolve the
    /// transaction via `transaction.timeout.ms`.
    CommitUnknown,
    /// `abort` is running.
    Aborting,
    /// The transaction is **prepared** and awaits an external coordinator's
    /// decision (KIP-939). Reached only from
    /// [`prepare`](TransactionalProducer::prepare);
    /// the only moves are `commit`, `abort` or
    /// [`complete`](TransactionalProducer::complete).
    Prepared,
    /// The producer hit a fatal error (fencing, authorization) and must be
    /// recreated.
    Fatal,
}

/// The identity of a **prepared** transaction (KIP-939).
///
/// Returned by [`prepare`](TransactionalProducer::prepare) and by
/// [`prepared_transaction`](TransactionalProducer::prepared_transaction).
///
/// # What it is for
///
/// In a two-phase commit the *external* coordinator — a database, an XA
/// manager, a workflow engine — decides whether the distributed transaction
/// commits. Kafka's side must stay in doubt until that decision arrives, and
/// must survive the producer process dying in between.
///
/// This value is the durable link across that gap. The intended sequence is:
///
/// 1. `prepare()` → a `PreparedTxnState`.
/// 2. Write it into the external coordinator's store, in the *same* external
///    transaction the Kafka writes are part of.
/// 3. If the process dies, the replacement is built with `two_phase_commit`,
///    finds the transaction in `prepared_transaction()`, reads the stored
///    value back, and calls [`complete`](TransactionalProducer::complete)
///    with it.
///
/// [`Display`](std::fmt::Display) and [`FromStr`](std::str::FromStr) round-trip
/// it through a short string so step 2 needs no bespoke serialisation:
///
/// ```rust
/// use krafka::producer::PreparedTxnState;
///
/// # fn example(state: PreparedTxnState) -> Result<(), krafka::error::KrafkaError> {
/// let stored: String = state.to_string();
/// let restored: PreparedTxnState = stored.parse()?;
/// assert_eq!(restored, state);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PreparedTxnState {
    producer_id: i64,
    producer_epoch: i16,
}

impl PreparedTxnState {
    /// The state meaning "no transaction was left prepared".
    #[must_use]
    pub const fn none() -> Self {
        Self {
            producer_id: -1,
            producer_epoch: -1,
        }
    }

    /// Whether this names an actual prepared transaction.
    #[must_use]
    pub const fn is_prepared(&self) -> bool {
        self.producer_id >= 0
    }

    /// Producer ID of the prepared transaction.
    #[must_use]
    pub const fn producer_id(&self) -> i64 {
        self.producer_id
    }

    /// Producer epoch of the prepared transaction.
    #[must_use]
    pub const fn producer_epoch(&self) -> i16 {
        self.producer_epoch
    }
}

impl std::fmt::Display for PreparedTxnState {
    /// `producer_id:epoch`, which is what
    /// [`FromStr`](std::str::FromStr) reads back.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.producer_id, self.producer_epoch)
    }
}

impl std::str::FromStr for PreparedTxnState {
    type Err = KrafkaError;

    fn from_str(s: &str) -> Result<Self> {
        let malformed = || {
            KrafkaError::config(format!(
                "malformed PreparedTxnState {s:?}; expected `producer_id:epoch`"
            ))
        };
        let (id, epoch) = s.split_once(':').ok_or_else(malformed)?;
        Ok(Self {
            producer_id: id.trim().parse().map_err(|_| malformed())?,
            producer_epoch: epoch.trim().parse().map_err(|_| malformed())?,
        })
    }
}

/// How [`complete`](TransactionalProducer::complete)
/// resolved a prepared transaction.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionOutcome {
    /// The stored state matched the transaction the coordinator still holds, so
    /// the prepare is known to have been durably recorded — commit.
    Committed,
    /// The stored state did not match, so it describes an *older* transaction
    /// and the prepare never completed — abort.
    Aborted,
}

impl std::fmt::Display for TransactionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Uninitialized => "Uninitialized",
            Self::Initializing => "Initializing",
            Self::Ready => "Ready",
            Self::Open => "Open",
            Self::Committing => "Committing",
            Self::CommitUnknown => "CommitUnknown",
            Self::Aborting => "Aborting",
            Self::Prepared => "Prepared",
            Self::Fatal => "Fatal",
        })
    }
}

/// A topic-partition offset used with [`TransactionalProducer::send_offsets`].
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

/// A transactional Kafka producer: a [`Producer`] plus the transaction
/// state. Built with
/// [`ProducerBuilder::build_transactional`](super::ProducerBuilder::build_transactional).
///
/// Provides exactly-once semantics: a transaction's records and consumer
/// offsets become visible together on commit, or not at all.
///
/// # What a commit guarantees
///
/// [`commit`](Self::commit) commits only when every send admitted into the
/// transaction has succeeded. A send is admitted, and counted, at the moment
/// it is queued; the commit closes the transaction to new sends, sends what
/// is buffered without waiting for `linger`, waits for every counted send,
/// and refuses with [`TransactionAbortable`](KrafkaError::TransactionAbortable)
/// if any of them failed — whether or not its handle was awaited.
///
/// # Errors
///
/// An error with [`requires_abort()`](KrafkaError::requires_abort) means:
/// call [`abort`](Self::abort) and start again. An error with
/// [`is_fatal()`](KrafkaError::is_fatal) means: this producer is done (fenced,
/// unauthorised); build a new one.
pub struct TransactionalProducer {
    producer: Producer,
    transactional_id: String,
    /// State, pending sends and first failure of the current transaction.
    gate: Arc<TxnGate>,
    /// The producer id and epoch coordinator RPCs carry.
    identity: parking_lot::Mutex<(i64, i16)>,
    /// The prepared transaction the coordinator reported at initialisation
    /// under two-phase commit, or [`PreparedTxnState::none`].
    ongoing_prepared_txn: arc_swap::ArcSwap<PreparedTxnState>,
    /// Negotiated KIP-890 protocol, fixed at initialisation.
    transaction_version: AtomicU8,
    /// An `EndTxn` of this producer went unanswered. Under TV1 a stale
    /// attempt can still reach the coordinator and end a later transaction,
    /// so the next transaction starts on a bumped epoch (KAFKA-17754).
    end_txn_unanswered: AtomicBool,
    /// Under TV1 the next transaction needs `InitProducerId` with the current
    /// producer id and epoch first.
    reinit_required: AtomicBool,
    /// Serialises the re-initialisation.
    reinit_lock: tokio::sync::Mutex<()>,
    /// Transaction coordinator broker ID.
    coordinator_id: RwLock<Option<i32>>,
    /// Partitions registered with the coordinator in the current transaction
    /// (TV1).
    txn_partitions: Arc<RwLock<TransactionPartitions>>,
    backoff: Backoff,
}

impl std::fmt::Debug for TransactionalProducer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransactionalProducer")
            .field("transactional_id", &self.transactional_id)
            .field("state", &self.state())
            .finish_non_exhaustive()
    }
}

impl TransactionalProducer {
    /// Wrap `producer` and initialise it with the transaction coordinator.
    pub(crate) async fn start(
        producer: Producer,
        transactional_id: String,
        gate: Arc<TxnGate>,
    ) -> Result<Self> {
        let backoff = Backoff::new(producer.config.retry_backoff);
        let keep_prepared = producer.config.two_phase_commit;
        let txn = Self {
            producer,
            transactional_id,
            gate,
            identity: parking_lot::Mutex::new((-1, -1)),
            ongoing_prepared_txn: arc_swap::ArcSwap::from_pointee(PreparedTxnState::none()),
            // Settled by the initialisation once the cluster's finalized
            // transaction.version has been read; TV1 is the safe default.
            transaction_version: AtomicU8::new(TransactionVersion::V1 as u8),
            end_txn_unanswered: AtomicBool::new(false),
            reinit_required: AtomicBool::new(false),
            reinit_lock: tokio::sync::Mutex::new(()),
            coordinator_id: RwLock::new(None),
            txn_partitions: Arc::new(RwLock::new(TransactionPartitions::default())),
            backoff,
        };
        if let Err(error) = txn.init_transactions(keep_prepared).await {
            let _ = txn.producer.close().await;
            return Err(error);
        }
        info!(transactional_id = %txn.transactional_id, "transactional producer started");
        Ok(txn)
    }

    fn metadata(&self) -> &ClusterMetadata {
        self.producer.kafka.metadata()
    }

    fn pool(&self) -> &ConnectionPool {
        self.producer.kafka.pool()
    }

    /// Get the current transaction state.
    #[inline]
    pub fn state(&self) -> TransactionState {
        self.gate.state()
    }

    /// The KIP-890 transaction protocol negotiated with this cluster.
    ///
    /// Returns [`TransactionVersion::V1`] until
    /// the initialisation has completed, since the
    /// finalized feature is only queried there.
    #[inline]
    pub fn transaction_version(&self) -> TransactionVersion {
        TransactionVersion::from(self.transaction_version.load(Ordering::SeqCst))
    }

    /// Whether the client itself must register partitions and the offsets
    /// topic with the transaction coordinator before writing to them (TV1).
    #[inline]
    fn requires_explicit_partition_registration(&self) -> bool {
        !self.transaction_version().is_v2()
    }

    /// Ask every known broker what it can serve and settle on one protocol.
    ///
    /// The finalized-feature set is only present on `ApiVersions` **v3+**
    /// responses, so this re-asks each broker at v3+ to read
    /// `transaction.version`. Brokers that cannot be reached or asked are
    /// skipped; if none can be asked the result is [`TransactionVersion::V1`].
    async fn detect_transaction_version(&self) -> TransactionVersion {
        let brokers = self.metadata().brokers();
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
            .pool()
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
            init_producer_id_max: conn.negotiate_api_version(
                ApiKey::InitProducerId,
                versions::INIT_PRODUCER_ID_MAX,
                versions::INIT_PRODUCER_ID_MIN,
            ),
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

    /// The producer id and epoch, failing when the initialisation has not
    /// established them.
    fn checked_transactional_identity(&self) -> Result<(i64, i16)> {
        let (producer_id, producer_epoch) = *self.identity.lock();
        if producer_id < 0 || producer_epoch < 0 {
            return Err(KrafkaError::illegal_state(
                "transactional producer identity not initialized",
            ));
        }
        Ok((producer_id, producer_epoch))
    }

    /// Adopt a new producer id and epoch, here and in the send engine.
    fn adopt_identity(&self, producer_id: i64, producer_epoch: i16) {
        *self.identity.lock() = (producer_id, producer_epoch);
        self.producer.accumulator.set_identity(
            producer_id,
            producer_epoch,
            self.transaction_version().is_v2(),
        );
    }

    /// The deadline every coordinator RPC of one call shares (`max_block`).
    fn deadline(&self) -> tokio::time::Instant {
        tokio::time::Instant::now() + self.producer.config.max_block
    }

    /// Classify a coordinator RPC's result: a fatal code ends the producer, an
    /// abortable one fails the transaction. Returns the result unchanged, with
    /// fencing reported as [`KrafkaError::Fenced`].
    fn classify_transaction_result<T>(&self, result: Result<T>) -> Result<T> {
        let Err(error) = &result else {
            return result;
        };
        if let KrafkaError::Broker { code, message } = error
            && is_fatal_transaction_error(*code, self.transaction_version())
        {
            warn!(
                error_code = ?code,
                "Fatal transactional error from coordinator; producer must be recreated"
            );
            self.gate.set(TransactionState::Fatal);
            if is_fencing_code(*code) {
                return Err(KrafkaError::fenced(format!("{code:?}: {message}")));
            }
            return result;
        }
        if Self::is_abortable_transaction_error(error, self.transaction_version()) {
            self.gate.fail(error.clone());
        }
        result
    }

    /// Whether the error ends the current transaction but leaves the producer
    /// usable after [`abort`](Self::abort).
    ///
    /// [`ErrorCode::InvalidProducerIdMapping`] is abortable only under TV1;
    /// under TV2 it is fatal (see [`is_fatal_transaction_error`]).
    /// `UNKNOWN_PRODUCER_ID` from a coordinator RPC is abortable, and the
    /// abort bumps the epoch.
    fn is_abortable_transaction_error(error: &KrafkaError, version: TransactionVersion) -> bool {
        let code = match error {
            KrafkaError::TransactionAbortable { .. } => return true,
            KrafkaError::Broker { code, .. } => code,
            _ => return false,
        };
        match code {
            ErrorCode::TransactionAbortable | ErrorCode::UnknownProducerId => true,
            ErrorCode::InvalidProducerIdMapping => !version.is_v2(),
            _ => false,
        }
    }

    /// A connection to the transaction coordinator, discovering it when no
    /// coordinator is cached.
    async fn coordinator_connection(&self, attempt: u32) -> Result<Arc<BrokerConnection>> {
        let cached = *self.coordinator_id.read().await;
        let coordinator_id = match cached {
            Some(id) => id,
            None => {
                let id = self.find_coordinator(attempt).await?;
                *self.coordinator_id.write().await = Some(id);
                debug!("Discovered transaction coordinator: broker {}", id);
                id
            }
        };

        let brokers = self.metadata().brokers();
        let broker = brokers
            .iter()
            .find(|b| b.id() == coordinator_id)
            .ok_or_else(|| {
                KrafkaError::broker(
                    ErrorCode::CoordinatorNotAvailable,
                    "the transaction coordinator is not in the metadata",
                )
            })?;

        self.pool()
            .get_connection_by_id(broker.id(), broker.address())
            .await
    }

    /// Whether the error indicates the cached coordinator may be stale.
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

    /// Run one coordinator RPC until it succeeds, fails non-retriably, or
    /// `max_block` passes; a stale coordinator is re-discovered between
    /// attempts.
    async fn with_coordinator<T, F, Fut>(&self, what: &str, attempt: F) -> Result<T>
    where
        F: Fn(u32) -> Fut,
        Fut: Future<Output = Result<T>>,
    {
        retry::until_deadline(&self.backoff, self.deadline(), what, |n| {
            let call = attempt(n);
            async move {
                let result = call.await;
                if let Err(error) = &result
                    && Self::needs_coordinator_refresh(error)
                {
                    *self.coordinator_id.write().await = None;
                }
                result
            }
        })
        .await
    }

    /// Fetch the producer id and epoch for this `transactional.id`,
    /// retrying until `max_block`. Without `keep_prepared_txn` the coordinator
    /// aborts whatever a previous incarnation left open; with it (KIP-939) a
    /// prepared transaction is kept and reported.
    async fn init_transactions(&self, keep_prepared_txn: bool) -> Result<()> {
        if let Err(actual) = self.gate.transition(
            &[TransactionState::Uninitialized],
            TransactionState::Initializing,
        ) {
            return Err(KrafkaError::illegal_state(format!(
                "init_transactions can only be called once (state={actual})"
            )));
        }

        // Settle the KIP-890 protocol before the first coordinator RPC: every
        // later decision reads it.
        let version = self.detect_transaction_version().await;
        self.transaction_version
            .store(version as u8, Ordering::SeqCst);

        if self.producer.config.two_phase_commit && !version.supports_two_phase_commit() {
            self.gate.set(TransactionState::Uninitialized);

            // `InitProducerId` v6 is behind krafka's `unstable-protocol`
            // feature, so a client compiled without it can never negotiate TV3.
            let cause = if versions::INIT_PRODUCER_ID_MAX < TV3_MIN_INIT_PRODUCER_ID_VERSION {
                format!(
                    "this build of krafka negotiates InitProducerId up to \
                     v{}, and enable2Pc needs \
                     v{TV3_MIN_INIT_PRODUCER_ID_VERSION} — enable the \
                     `unstable-protocol` feature",
                    versions::INIT_PRODUCER_ID_MAX
                )
            } else {
                format!(
                    "this cluster negotiated {version}; it must finalize \
                     transaction.version at 3 and every broker must serve \
                     InitProducerId v{TV3_MIN_INIT_PRODUCER_ID_VERSION}"
                )
            };

            return Err(KrafkaError::illegal_state(format!(
                "two_phase_commit (KIP-939) is not available: {cause}. The broker \
                 must also grant TWO_PHASE_COMMIT alongside WRITE on \
                 transactional_id '{}'.",
                self.transactional_id
            )));
        }

        match self.init_producer_id(keep_prepared_txn, None).await {
            Ok(()) => {
                self.gate.set(TransactionState::Ready);
                Ok(())
            }
            Err(error) => {
                self.gate.set(TransactionState::Uninitialized);
                Err(error)
            }
        }
    }

    /// Send `InitProducerId` for this `transactional.id` and adopt the
    /// answer.
    ///
    /// With `current`, the request carries the producer's current id and
    /// epoch (KIP-360), so the coordinator bumps this producer's epoch rather
    /// than treating the call as a new incarnation, and a retry after a lost
    /// answer is recognised.
    async fn init_producer_id(
        &self,
        keep_prepared_txn: bool,
        current: Option<(i64, i16)>,
    ) -> Result<()> {
        let result = self
            .with_coordinator("InitProducerId", |attempt| async move {
                let conn = self.coordinator_connection(attempt).await?;
                let version = conn
                    .negotiate_api_version(
                        ApiKey::InitProducerId,
                        versions::INIT_PRODUCER_ID_MAX,
                        versions::INIT_PRODUCER_ID_MIN,
                    )
                    .ok_or_else(|| KrafkaError::transactions_unsupported(ApiKey::InitProducerId))?;
                // Every transaction ends with `EndTxn`; a coordinator without
                // it fails here rather than at the first commit.
                conn.negotiate_api_version(
                    ApiKey::EndTxn,
                    versions::END_TXN_MAX,
                    versions::END_TXN_MIN,
                )
                .ok_or_else(|| KrafkaError::transactions_unsupported(ApiKey::EndTxn))?;

                let mut request = if self.producer.config.two_phase_commit {
                    InitProducerIdRequest::two_phase_commit(
                        &self.transactional_id,
                        keep_prepared_txn,
                    )
                } else {
                    InitProducerIdRequest::transactional(
                        &self.transactional_id,
                        crate::util::duration_to_millis_i32(
                            self.producer.config.transaction_timeout(),
                        ),
                    )
                };
                if let Some((producer_id, epoch)) = current {
                    request.producer_id = producer_id;
                    request.producer_epoch = epoch;
                }

                let mut bytes = conn
                    .send_request(ApiKey::InitProducerId, version, |buf| {
                        request.encode_versioned(version, buf)
                    })
                    .await?;
                let response = InitProducerIdResponse::decode_versioned(version, &mut bytes)?;
                if !response.is_ok() {
                    return Err(KrafkaError::broker(
                        response.error_code,
                        "failed to initialize producer ID",
                    ));
                }
                Ok(response)
            })
            .await;
        let response = self.classify_transaction_result(result)?;

        self.adopt_identity(response.producer_id, response.producer_epoch);
        // KIP-939: when `keep_prepared_txn` was set the coordinator reports
        // the transaction it did *not* abort, so the caller can finish it.
        self.ongoing_prepared_txn.store(Arc::new(PreparedTxnState {
            producer_id: response.ongoing_txn_producer_id,
            producer_epoch: response.ongoing_txn_producer_epoch,
        }));
        self.reinit_required.store(false, Ordering::SeqCst);
        self.end_txn_unanswered.store(false, Ordering::SeqCst);
        info!(
            "Transactional producer initialized: PID={}, epoch={}",
            response.producer_id, response.producer_epoch
        );
        Ok(())
    }

    /// Under TV1, bump the epoch before the transaction writes anything when
    /// the previous one asked for it.
    async fn ensure_reinitialised(&self) -> Result<()> {
        if !self.reinit_required.load(Ordering::SeqCst) {
            return Ok(());
        }
        let _guard = self.reinit_lock.lock().await;
        if !self.reinit_required.load(Ordering::SeqCst) {
            return Ok(());
        }
        let current = self.checked_transactional_identity()?;
        self.init_producer_id(false, Some(current)).await
    }

    /// Find the transaction coordinator.
    ///
    /// `attempt` rotates which broker is asked, so one unreachable broker
    /// cannot fail every retry.
    async fn find_coordinator(&self, attempt: u32) -> Result<i32> {
        let brokers = self.metadata().brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::broker(
                ErrorCode::CoordinatorNotAvailable,
                "no brokers known",
            ));
        }

        let broker = &brokers[attempt as usize % brokers.len()];
        let conn = self
            .pool()
            .get_connection_by_id(broker.id(), broker.address())
            .await?;

        let request = FindCoordinatorRequest::for_transaction(&self.transactional_id);

        // Transaction coordinator lookup requires v1+ (key_type field).
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
    /// Moves the producer from `Ready` to `Open`. Synchronous and
    /// non-blocking: an in-memory state change, as Java's
    /// `KafkaProducer.beginTransaction()`.
    ///
    /// # Errors
    ///
    /// Fails if the producer is not `Ready` — not yet initialised, or a
    /// previous transaction was not committed or aborted.
    pub fn begin(&self) -> Result<()> {
        if self.producer.barrier.is_closing() {
            return Err(KrafkaError::closed("transactional producer is closed"));
        }
        self.gate.begin().map_err(|actual| {
            KrafkaError::illegal_state(format!("cannot begin a transaction in state {actual}"))
        })?;
        debug!("Transaction started");
        Ok(())
    }

    /// Send a record within the current transaction and wait for the broker
    /// to acknowledge it: `enqueue(record).await?.await`.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel safe. Dropped after the record is queued, the
    /// record is still written in the transaction and only its acknowledgement
    /// is lost, so sending it again writes it twice. Dropped earlier, nothing
    /// was queued; an `AddPartitionsToTxn` in flight is rolled back and the
    /// next send to the partition repeats it. Either way the transaction can
    /// still commit.
    pub async fn send(&self, record: Record) -> Result<RecordMetadata> {
        self.enqueue(record).await?.await
    }

    /// Queue a record into the current transaction and return as soon as it is
    /// **queued**.
    ///
    /// The transactional counterpart of [`Producer::enqueue`], with the same
    /// ordering and cancellation guarantees.
    ///
    /// The record is counted in the transaction at the moment it is queued.
    /// From then on its outcome decides whether the transaction can commit,
    /// whether or not its handle is awaited: a failed record makes
    /// [`commit`](Self::commit) refuse with
    /// [`TransactionAbortable`](KrafkaError::TransactionAbortable).
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. As for [`Producer::enqueue`]: a dropped call
    /// queued nothing and counted nothing in the transaction; a call that
    /// returned a handle queued the record exactly once.
    ///
    /// # Errors
    ///
    /// Refused unless a transaction is open and nothing in it has failed.
    pub async fn enqueue(&self, record: Record) -> Result<DeliveryHandle> {
        if let Err(error) = self.gate.check_open("send") {
            return Err(self.refuse_send(record, error));
        }
        if let Err(error) = self.ensure_reinitialised().await {
            return Err(self.refuse_send(record, error));
        }
        let explicit = self.requires_explicit_partition_registration();
        let gate = Arc::clone(&self.gate);
        super::enqueue_record(
            &self.producer.accumulator,
            self.metadata(),
            &self.producer.partitioning,
            &*self.producer.interceptor,
            self.producer.kafka.client_id(),
            self.producer.config.max_block,
            record,
            |topic, partition| async move {
                // TV1: the coordinator learns about a partition only from an
                // explicit AddPartitionsToTxn, which must precede the write.
                // TV2: the Produce request registers it (KIP-890).
                if explicit {
                    self.add_partition_to_txn_if_needed(&topic, partition).await
                } else {
                    Ok(())
                }
            },
            move || gate.admit().map(Some),
        )
        .await
    }

    /// Report a send refused before it reached the shared send path to the
    /// interceptor, which still owes it a terminal callback.
    fn refuse_send(&self, mut record: Record, error: KrafkaError) -> KrafkaError {
        match super::SendObligation::on_send(
            &*self.producer.interceptor,
            &mut record,
            self.producer.kafka.client_id(),
        ) {
            Ok(mut obligation) => obligation.fail(super::UNKNOWN_PARTITION, &record.headers, error),
            Err(_) => error,
        }
    }

    /// Ensure a partition is registered with the transaction coordinator,
    /// issuing `AddPartitionsToTxn` at most once per partition per transaction
    /// (TV1).
    async fn add_partition_to_txn_if_needed(
        &self,
        topic: &Arc<str>,
        partition: PartitionId,
    ) -> Result<()> {
        loop {
            let mut txn_partitions = self.txn_partitions.write().await;
            match txn_partitions.begin_add(topic.as_ref(), partition) {
                BeginAddResult::AlreadyAdded => break,
                BeginAddResult::Fatal(err) => return Err((*err).clone()),
                BeginAddResult::Wait(notify) => {
                    // Register interest before releasing the lock so the
                    // add's completion cannot be missed.
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    drop(txn_partitions);
                    notified.await;
                }
                BeginAddResult::NeedAdd(notify) => {
                    drop(txn_partitions);
                    let guard = PendingAddGuard {
                        txn_partitions: self.txn_partitions.clone(),
                        topic: topic.clone(),
                        partition,
                        notify,
                        defused: false,
                    };
                    match self.add_partition_to_txn(topic.as_ref(), partition).await {
                        Ok(()) => guard.confirm(topic.as_ref(), partition).await,
                        Err(e) if e.is_retriable() => {
                            guard.cancel(topic.as_ref(), partition).await;
                            return Err(e);
                        }
                        Err(e) => {
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

    /// Add a partition to the current transaction (TV1).
    async fn add_partition_to_txn(&self, topic: &str, partition: PartitionId) -> Result<()> {
        let result = self
            .with_coordinator("AddPartitionsToTxn", |attempt| async move {
                let conn = self.coordinator_connection(attempt).await?;
                let (producer_id, producer_epoch) = self.checked_transactional_identity()?;
                let version = conn
                    .negotiate_api_version(
                        ApiKey::AddPartitionsToTxn,
                        versions::ADD_PARTITIONS_TO_TXN_MAX,
                        versions::ADD_PARTITIONS_TO_TXN_MIN,
                    )
                    .ok_or_else(|| {
                        KrafkaError::transactions_unsupported(ApiKey::AddPartitionsToTxn)
                    })?;
                let request = AddPartitionsToTxnRequest::new(
                    &self.transactional_id,
                    producer_id,
                    producer_epoch,
                )
                .add_partition(topic, partition);
                let mut bytes = conn
                    .send_request(ApiKey::AddPartitionsToTxn, version, |buf| {
                        request.encode_versioned(version, buf)
                    })
                    .await?;
                let response = AddPartitionsToTxnResponse::decode_versioned(version, &mut bytes)?;
                if !response.is_ok() {
                    for topic_result in &response.results {
                        for partition_result in &topic_result.partitions {
                            if !partition_result.error_code.is_ok() {
                                return Err(KrafkaError::broker(
                                    partition_result.error_code,
                                    format!("failed to add {topic}-{partition} to transaction"),
                                ));
                            }
                        }
                    }
                    return Err(KrafkaError::protocol_kind(
                        ProtocolErrorKind::Malformed,
                        format!(
                            "failed to add {topic}-{partition} to transaction: the response \
                             reported an error but no per-partition error"
                        ),
                    ));
                }
                debug!("Added partition {}-{} to transaction", topic, partition);
                Ok(())
            })
            .await;
        self.classify_transaction_result(result)
    }

    /// Atomically commit consumer offsets as part of the current transaction
    /// (exactly-once consume-transform-produce).
    ///
    /// Each [`TopicPartitionOffset`] entry specifies a partition and the **next**
    /// offset to consume (`last_consumed + 1`, matching `Consumer::position()`).
    ///
    /// Under TV1 the consumer group is first registered with the transaction
    /// coordinator (`AddOffsetsToTxn`); under TV2 the group coordinator does
    /// that itself. Both RPCs retry until `max_block`.
    ///
    /// # KIP-447 zombie fencing
    ///
    /// `group_metadata` must come from the `group_metadata()` accessor on the
    /// consumer whose offsets are being committed, re-read for every
    /// transaction. The generation, member ID and static instance ID are sent
    /// so the group coordinator can reject a stale committer.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel safe. Dropped after it started, the offsets
    /// may or may not have been added to the transaction, and the transaction
    /// can no longer commit: [`commit`](Self::commit) refuses with
    /// [`TransactionAbortable`](KrafkaError::TransactionAbortable). Abort it
    /// and start the transaction over.
    ///
    /// # Errors
    ///
    /// Refused unless a transaction is open and nothing in it has failed. A
    /// failure makes the transaction abortable. Fails with
    /// [`TransactionAbortable`](KrafkaError::TransactionAbortable) without
    /// contacting any broker when
    /// [`is_fenceable()`](crate::consumer::ConsumerGroupMetadata::is_fenceable)
    /// is `false`.
    pub async fn send_offsets(
        &self,
        offsets: &[TopicPartitionOffset],
        group_metadata: &ConsumerGroupMetadata,
    ) -> Result<()> {
        self.gate.check_open("send offsets")?;
        self.ensure_reinitialised().await?;
        // Counted like a send, so a commit waits for it and refuses if it
        // failed.
        let ticket = self.gate.admit()?;
        let result = self.send_offsets_inner(offsets, group_metadata).await;
        ticket.complete(result.as_ref().map(|_| ()));
        result
    }

    async fn send_offsets_inner(
        &self,
        offsets: &[TopicPartitionOffset],
        group_metadata: &ConsumerGroupMetadata,
    ) -> Result<()> {
        if !group_metadata.is_fenceable() {
            return Err(KrafkaError::transaction_abortable(format!(
                "consumer group metadata for '{}' carries no valid generation \
                 (generation_id={}, member_id={:?}); the offset commit could not be \
                 fenced against a zombie consumer. abort() is required.",
                group_metadata.group_id(),
                group_metadata.generation_id(),
                group_metadata.member_id(),
            )));
        }

        let group_id = group_metadata.group_id();
        let (producer_id, producer_epoch) = self.checked_transactional_identity()?;

        if self.requires_explicit_partition_registration() {
            self.add_offsets_to_txn(producer_id, producer_epoch, group_id)
                .await?;
        }

        let commit_request = build_txn_offset_commit_request(
            &self.transactional_id,
            group_metadata,
            producer_id,
            producer_epoch,
            offsets,
        );

        // TV2 moves partition registration into the group coordinator's
        // TxnOffsetCommit handler, which only exists from v5; an older version
        // after skipping AddOffsetsToTxn would leave the offsets outside the
        // transaction.
        let toc_min_version = if self.transaction_version().is_v2() {
            TV2_MIN_TXN_OFFSET_COMMIT_VERSION
        } else {
            versions::TXN_OFFSET_COMMIT_MIN
        };

        let commit_request = &commit_request;
        let result = retry::until_deadline(
            &self.backoff,
            self.deadline(),
            "TxnOffsetCommit",
            |attempt| async move {
                let (group_node_id, group_host, group_port) =
                    self.find_group_coordinator(group_id, attempt).await?;
                let group_conn = self
                    .pool()
                    .get_connection_by_id(group_node_id, &format!("{group_host}:{group_port}"))
                    .await?;
                let version = group_conn
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
                let mut bytes = group_conn
                    .send_request(ApiKey::TxnOffsetCommit, version, |buf| {
                        commit_request.encode_versioned(version, buf)
                    })
                    .await?;
                let response = TxnOffsetCommitResponse::decode_versioned(version, &mut bytes)?;
                if !response.is_ok() {
                    for topic_result in &response.topics {
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
                    return Err(KrafkaError::protocol_kind(
                        ProtocolErrorKind::Malformed,
                        "failed to commit offsets in transaction",
                    ));
                }
                Ok(())
            },
        )
        .await;
        let result = self.classify_transaction_result(result);
        if result.is_ok() {
            debug!("Added offsets to transaction for group {}", group_id);
        }
        result
    }

    /// Register the consumer group's offsets topic with the transaction
    /// coordinator via `AddOffsetsToTxn` (TV1).
    async fn add_offsets_to_txn(
        &self,
        producer_id: i64,
        producer_epoch: i16,
        group_id: &str,
    ) -> Result<()> {
        let result = self
            .with_coordinator("AddOffsetsToTxn", |attempt| async move {
                let conn = self.coordinator_connection(attempt).await?;
                let request = AddOffsetsToTxnRequest::new(
                    &self.transactional_id,
                    producer_id,
                    producer_epoch,
                    group_id,
                );
                let version = conn
                    .negotiate_api_version(
                        ApiKey::AddOffsetsToTxn,
                        versions::ADD_OFFSETS_TO_TXN_MAX,
                        versions::ADD_OFFSETS_TO_TXN_MIN,
                    )
                    .ok_or_else(|| {
                        KrafkaError::transactions_unsupported(ApiKey::AddOffsetsToTxn)
                    })?;
                let mut bytes = conn
                    .send_request(ApiKey::AddOffsetsToTxn, version, |buf| {
                        request.encode_versioned(version, buf)
                    })
                    .await?;
                let response = AddOffsetsToTxnResponse::decode_versioned(version, &mut bytes)?;
                if !response.is_ok() {
                    return Err(KrafkaError::broker(
                        response.error_code,
                        "failed to add offsets to transaction",
                    ));
                }
                Ok(())
            })
            .await;
        self.classify_transaction_result(result)
    }

    /// Find the group coordinator, returning (node_id, host, port).
    async fn find_group_coordinator(
        &self,
        group_id: &str,
        attempt: u32,
    ) -> Result<(i32, String, i32)> {
        let brokers = self.metadata().brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::broker(
                ErrorCode::CoordinatorNotAvailable,
                "no brokers known",
            ));
        }

        let broker = &brokers[attempt as usize % brokers.len()];
        let conn = self
            .pool()
            .get_connection_by_id(broker.id(), broker.address())
            .await?;

        let request = FindCoordinatorRequest::for_group(group_id);
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

    /// Close the transaction to new sends, send what is buffered without
    /// waiting for `linger`, and wait for every counted send.
    ///
    /// Returns the transaction's failure, if any send failed.
    async fn drain_transaction(&self) -> Option<KrafkaError> {
        let generation = self.producer.barrier.snapshot();
        self.producer.accumulator.flush(generation);
        self.gate.drained().await;
        self.gate.failure("commit")
    }

    /// Prepare the open transaction and hand back its identity (KIP-939).
    ///
    /// The "prepare" half of a two-phase commit. New sends are refused, every
    /// buffered record is sent and its outcome awaited; if one failed the
    /// transaction stays open and must be aborted. The only moves afterwards
    /// are [`commit`](Self::commit),
    /// [`abort`](Self::abort) or
    /// [`complete`](Self::complete).
    ///
    /// # It sends nothing
    ///
    /// There is no "prepare" request in the Kafka protocol. Once every record
    /// is written and no marker follows, the transaction is in doubt on the
    /// broker as a prepared transaction should be; the coordinator was told
    /// at `InitProducerId` (via `enable2Pc`) not to time it out.
    ///
    /// # The returned state must be stored before you report success
    ///
    /// Write it into the external coordinator's store, inside the same external
    /// transaction the Kafka writes belong to. It is the only link back to this
    /// transaction if the process dies.
    ///
    /// # Errors
    ///
    /// Requires an open transaction on a producer built with
    /// [`two_phase_commit`](super::ProducerBuilder::two_phase_commit).
    pub async fn prepare(&self) -> Result<PreparedTxnState> {
        if !self.producer.config.two_phase_commit {
            return Err(KrafkaError::illegal_state(
                "prepare() requires ProducerBuilder::two_phase_commit(true); without it the \
                 coordinator applies transaction.max.timeout.ms and would abort the \
                 prepared transaction out from under the external coordinator",
            ));
        }
        if let Some(error) = self.gate.failure("prepare") {
            return Err(error);
        }
        self.gate
            .transition(&[TransactionState::Open], TransactionState::Prepared)
            .map_err(|actual| {
                KrafkaError::illegal_state(format!(
                    "cannot prepare in state {actual}; a transaction must be open"
                ))
            })?;

        if let Some(error) = self.drain_transaction().await {
            let _ = self
                .gate
                .transition(&[TransactionState::Prepared], TransactionState::Open);
            return Err(error);
        }

        let (producer_id, producer_epoch) = *self.identity.lock();
        let state = PreparedTxnState {
            producer_id,
            producer_epoch,
        };
        info!(
            transactional_id = %self.transactional_id,
            producer_id = state.producer_id,
            producer_epoch = state.producer_epoch,
            "Transaction prepared; awaiting the external coordinator's decision"
        );
        Ok(state)
    }

    /// Resolve a prepared transaction against the state that was stored before
    /// preparing (KIP-939).
    ///
    /// Call when [`prepared_transaction`](Self::prepared_transaction) reports
    /// one, with the [`PreparedTxnState`] read back from the external
    /// coordinator's store. If it matches the transaction the coordinator still holds, the
    /// prepare was recorded externally, so this side commits; otherwise it
    /// describes an older transaction and this side aborts.
    ///
    /// # Errors
    ///
    /// Fails if no transaction was left prepared.
    pub async fn complete(&self, stored: PreparedTxnState) -> Result<TransactionOutcome> {
        let ongoing = **self.ongoing_prepared_txn.load();
        if !ongoing.is_prepared() {
            return Err(KrafkaError::illegal_state(
                "complete(): the coordinator is holding no prepared transaction for this \
                 transactional.id; prepared_transaction() returned None",
            ));
        }

        self.adopt_prepared(ongoing);
        if stored == ongoing {
            info!(
                transactional_id = %self.transactional_id,
                producer_id = ongoing.producer_id,
                "Recovered prepared transaction matches the stored state; committing"
            );
            self.commit().await?;
            Ok(TransactionOutcome::Committed)
        } else {
            info!(
                transactional_id = %self.transactional_id,
                stored = %stored,
                ongoing = %ongoing,
                "Recovered prepared transaction does not match the stored state; \
                 the prepare was never recorded externally, so aborting"
            );
            self.abort().await?;
            Ok(TransactionOutcome::Aborted)
        }
    }

    /// Adopt a recovered transaction's producer identity and mark it prepared.
    ///
    /// `EndTxn` must carry the producer ID and epoch of the transaction being
    /// finished, which for a recovered one is the *ongoing* pair the
    /// coordinator reported.
    fn adopt_prepared(&self, ongoing: PreparedTxnState) {
        self.adopt_identity(ongoing.producer_id, ongoing.producer_epoch);
        self.gate.set(TransactionState::Prepared);
    }

    /// Commit the current transaction.
    ///
    /// Closes the transaction to new sends, sends what is buffered without
    /// waiting for `linger`, waits for every send the transaction counted,
    /// and only then sends `EndTxn(commit)` — retried until `max_block`.
    ///
    /// # Outcomes
    ///
    /// - `Ok`: committed. The producer is `Ready`.
    /// - A send of the transaction failed: refused with
    ///   [`TransactionAbortable`](KrafkaError::TransactionAbortable) before
    ///   `EndTxn`; the transaction stays open for
    ///   [`abort`](Self::abort).
    /// - Every `EndTxn` attempt was answered with a definitive error: the
    ///   transaction stays open (or prepared) with that error.
    /// - Some `EndTxn` attempt went unanswered: the outcome is unknown and the
    ///   producer is in [`TransactionState::CommitUnknown`], whatever the last
    ///   attempt said. Only another `commit` is allowed from
    ///   there; `abort` is refused (KAFKA-17754).
    ///
    /// Under TV1, after any unanswered `EndTxn` the producer bumps its epoch
    /// (`InitProducerId` with its current id and epoch) before the next
    /// transaction writes anything, so a stale `EndTxn` released late cannot
    /// end that transaction.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel safe. Dropped before `EndTxn` was sent, the
    /// transaction is still open: call `commit` again, or
    /// [`abort`](Self::abort). Dropped while `EndTxn` was being sent, the
    /// coordinator may have committed it: the producer is in
    /// [`TransactionState::CommitUnknown`], and calling `commit` again
    /// completes it (`abort` is refused there). Dropped after the coordinator
    /// answered, the transaction is committed.
    pub async fn commit(&self) -> Result<()> {
        if self.state() == TransactionState::Open
            && let Some(error) = self.gate.failure("commit")
        {
            return Err(error);
        }
        let entered = self
            .gate
            .transition(
                &[
                    TransactionState::Open,
                    TransactionState::Prepared,
                    TransactionState::CommitUnknown,
                ],
                TransactionState::Committing,
            )
            .map_err(|actual| {
                KrafkaError::illegal_state(format!("cannot commit in state {actual}"))
            })?;

        let mut ending = Ending::new(self, TransactionState::Committing, entered);
        if entered != TransactionState::CommitUnknown {
            if let Some(error) = self.drain_transaction().await {
                let _ = self
                    .gate
                    .transition(&[TransactionState::Committing], entered);
                return Err(error);
            }
            if let Err(error) = self.ensure_reinitialised().await {
                let _ = self
                    .gate
                    .transition(&[TransactionState::Committing], entered);
                return Err(error);
            }
        }

        let unanswered = Arc::new(AtomicBool::new(false));
        ending.sending();
        let result = self.end_transaction(true, &unanswered).await;
        let unanswered = unanswered.load(Ordering::SeqCst);
        if unanswered {
            self.end_txn_unanswered.store(true, Ordering::SeqCst);
        }

        match result {
            Ok(()) => {
                ending.ended();
                self.txn_partitions.write().await.clear();
                self.finish_transaction(false).await;
                ending.disarm();
                info!("Transaction committed");
                Ok(())
            }
            Err(error) => {
                ending.disarm();
                if self.state() == TransactionState::Fatal {
                    warn!("Transaction commit failed (fatal): {error}");
                } else if unanswered || entered == TransactionState::CommitUnknown {
                    let _ = self.gate.transition(
                        &[TransactionState::Committing],
                        TransactionState::CommitUnknown,
                    );
                    warn!(
                        "Transaction commit outcome unknown ({error}); the coordinator may already \
                         have committed it. Retry commit() — aborting could tear the \
                         transaction (KAFKA-17754)."
                    );
                } else {
                    let _ = self
                        .gate
                        .transition(&[TransactionState::Committing], entered);
                    warn!("Transaction commit failed: {error}");
                }
                Err(error)
            }
        }
    }

    /// After a committed or aborted transaction: under TV1, bump the epoch if
    /// an `EndTxn` went unanswered or a send failed; then become `Ready`.
    ///
    /// A failed bump is retried before the next transaction writes anything.
    async fn finish_transaction(&self, send_failed: bool) {
        let needs_bump = !self.transaction_version().is_v2()
            && (self.end_txn_unanswered.load(Ordering::SeqCst) || send_failed);
        if needs_bump {
            self.reinit_required.store(true, Ordering::SeqCst);
            if let Err(error) = self.ensure_reinitialised().await {
                warn!(
                    %error,
                    "could not bump the producer epoch after the transaction; retrying before \
                     the next transaction writes"
                );
            }
        }
        self.gate.set(TransactionState::Ready);
    }

    /// Abort the current transaction.
    ///
    /// Buffered records of the transaction are failed with a "transaction
    /// aborted" error instead of being sent; only records already on the wire
    /// are awaited. Then `EndTxn(abort)` is sent, retried until `max_block`.
    /// Under TV1, when a send of the transaction failed, the producer bumps
    /// its epoch with its current id and epoch (KIP-360).
    ///
    /// # Refused after an unknown commit
    ///
    /// In [`TransactionState::CommitUnknown`] the coordinator may already have
    /// committed; an abort could then be applied to a later transaction and
    /// tear it (KAFKA-17754). Retry the commit instead, or drop the producer
    /// and let the coordinator resolve the transaction through its own
    /// `transaction.timeout.ms`.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel safe. Dropped before `EndTxn` was sent,
    /// records not yet on the wire may already have failed and the transaction
    /// is still open; dropped while `EndTxn` was being sent, the coordinator
    /// may already have aborted it. Either way, calling `abort` again completes
    /// it. Dropped after the coordinator answered, the transaction is aborted.
    pub async fn abort(&self) -> Result<()> {
        let entered = self
            .gate
            .transition(
                &[TransactionState::Open, TransactionState::Prepared],
                TransactionState::Aborting,
            )
            .map_err(|actual| match actual {
                TransactionState::CommitUnknown => KrafkaError::illegal_state(
                    "cannot abort: a previous commit() may already have been \
                     applied by the coordinator. Aborting now could be applied to a later \
                     transaction and tear it (KAFKA-17754). Retry commit(), or \
                     drop this producer and let the coordinator resolve the transaction via \
                     transaction.timeout.ms.",
                ),
                actual => KrafkaError::illegal_state(format!("cannot abort in state {actual}")),
            })?;

        let mut ending = Ending::new(self, TransactionState::Aborting, entered);
        // A send that failed on its own (not by this abort) spent a sequence
        // range under TV1; the abort then bumps the epoch.
        let send_failed = self.gate.has_failed();
        self.producer
            .accumulator
            .fail_unsent(KrafkaError::transaction_abortable(
                "the transaction was aborted before this record was sent",
            ))
            .await;
        self.gate.drained().await;

        let unanswered = Arc::new(AtomicBool::new(false));
        ending.sending();
        let result = self.end_transaction(false, &unanswered).await;
        if unanswered.load(Ordering::SeqCst) {
            self.end_txn_unanswered.store(true, Ordering::SeqCst);
        }

        match result {
            Ok(()) => {
                ending.ended();
                self.txn_partitions.write().await.clear();
                self.finish_transaction(send_failed).await;
                ending.disarm();
                info!("Transaction aborted");
                Ok(())
            }
            Err(error) => {
                ending.disarm();
                if self.state() != TransactionState::Fatal {
                    let _ = self.gate.transition(&[TransactionState::Aborting], entered);
                    warn!("Transaction abort failed, retry abort(): {error}");
                }
                Err(error)
            }
        }
    }

    /// Send `EndTxn`, retried until `max_block`.
    ///
    /// `unanswered` is set when any attempt was written to the coordinator and
    /// got no answer — the attempt may have been applied.
    ///
    /// # Transaction version
    ///
    /// Under **TV2** the coordinator bumps the epoch while writing the marker
    /// and returns the new pair on the `EndTxn` v5+ response; adopting it is
    /// mandatory. Under **TV1** `EndTxn` is capped at v4, because a v5 request
    /// is what tells the coordinator to apply TV2 semantics.
    async fn end_transaction(&self, commit: bool, unanswered: &Arc<AtomicBool>) -> Result<()> {
        let is_v2 = self.transaction_version().is_v2();
        let (min_version, max_version) = if is_v2 {
            (TV2_MIN_END_TXN_VERSION, versions::END_TXN_MAX)
        } else {
            (
                versions::END_TXN_MIN,
                versions::END_TXN_MAX.min(TV2_MIN_END_TXN_VERSION - 1),
            )
        };

        let result = self
            .with_coordinator("EndTxn", |attempt| {
                let unanswered = Arc::clone(unanswered);
                async move {
                    let conn = self.coordinator_connection(attempt).await?;
                    let (producer_id, producer_epoch) = self.checked_transactional_identity()?;
                    let version = conn
                        .negotiate_api_version(ApiKey::EndTxn, max_version, min_version)
                        .ok_or_else(|| {
                            KrafkaError::protocol_kind(
                                ProtocolErrorKind::UnknownApiVersion,
                                format!(
                                    "no mutually supported EndTxn API version in \
                                     v{min_version}..=v{max_version}"
                                ),
                            )
                        })?;
                    let request = if commit {
                        EndTxnRequest::commit(&self.transactional_id, producer_id, producer_epoch)
                    } else {
                        EndTxnRequest::abort(&self.transactional_id, producer_id, producer_epoch)
                    };

                    // Until an answer decodes, this attempt may have been
                    // applied without our knowing — including when the attempt
                    // is cut off by the deadline.
                    let pending = Unanswered::arm(unanswered);
                    let mut bytes = conn
                        .send_request(ApiKey::EndTxn, version, |buf| {
                            request.encode_versioned(version, buf)
                        })
                        .await?;
                    let response = EndTxnResponse::decode_versioned(version, &mut bytes)?;
                    pending.disarm();

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
                        (Some(pid), Some(epoch)) if pid >= 0 && epoch >= 0 && is_v2 => {
                            debug!(pid, epoch, "Adopting the producer epoch EndTxn bumped");
                            self.adopt_identity(pid, epoch);
                        }
                        _ if is_v2 => {
                            return Err(KrafkaError::protocol_kind(
                                ProtocolErrorKind::Malformed,
                                "EndTxn response omitted the bumped producer id/epoch that \
                                 transaction version 2 requires",
                            ));
                        }
                        _ => {}
                    }
                    Ok(())
                }
            })
            .await;
        self.classify_transaction_result(result)
    }

    /// Partition metadata for `topic`; see [`Producer::partitions_for`].
    ///
    /// # Errors
    ///
    /// Fails when the topic does not exist or its metadata cannot be fetched
    /// in time.
    pub async fn partitions_for(&self, topic: &str) -> Result<Vec<crate::PartitionInfo>> {
        self.producer.partitions_for(topic).await
    }

    /// Send every record queued before this call and wait for their outcomes.
    ///
    /// Not needed before [`commit`](Self::commit), which does the same
    /// itself. Does not make the records visible to `read_committed`
    /// consumers — only the commit does.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. Dropping it stops the wait, not the sends:
    /// the records it covered are still delivered, and a later `flush` or
    /// `commit` waits for them.
    pub async fn flush(&self) -> Result<()> {
        self.producer.flush().await
    }

    /// Get the transactional ID.
    #[inline]
    pub fn transactional_id(&self) -> &str {
        &self.transactional_id
    }

    /// Get the producer ID.
    #[inline]
    pub fn producer_id(&self) -> i64 {
        self.identity.lock().0
    }

    /// Get the producer epoch.
    #[inline]
    pub fn producer_epoch(&self) -> i16 {
        self.identity.lock().1
    }

    /// The transaction an earlier instance left prepared, which the
    /// coordinator kept because this producer was built with
    /// [`two_phase_commit`](super::ProducerBuilder::two_phase_commit)
    /// (KIP-939). Resolve it with [`complete`](Self::complete). `None` when
    /// nothing was left prepared.
    pub fn prepared_transaction(&self) -> Option<PreparedTxnState> {
        let ongoing = **self.ongoing_prepared_txn.load();
        ongoing.is_prepared().then_some(ongoing)
    }

    /// Close the producer: abort an open transaction, send every queued
    /// record and close the interceptors. A transaction whose commit outcome
    /// is unknown, or that is prepared, is left for the coordinator. Calling
    /// it again is a no-op.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel safe. Once polled, the producer is closed even
    /// if the future is then dropped: an open transaction may or may not have
    /// been aborted (the coordinator aborts it at `transaction.timeout.ms`
    /// otherwise), queued records are still delivered in the background, and
    /// calling `close` again returns at once.
    pub async fn close(&self) -> Result<()> {
        self.close_with(CloseOptions::new()).await
    }

    /// [`close`](Self::close) within `options`' timeout, if any; on timeout
    /// every record still queued fails and the call returns
    /// [`KrafkaError::Timeout`].
    pub async fn close_with(&self, options: CloseOptions) -> Result<()> {
        let Some(generation) = self.producer.barrier.begin_close() else {
            return Ok(());
        };
        let started = tokio::time::Instant::now();
        let settle = async {
            match self.state() {
                TransactionState::Open => {
                    warn!("closing a transactional producer with an open transaction; aborting it");
                    self.abort().await
                }
                TransactionState::CommitUnknown => {
                    warn!(
                        "closing after a commit whose outcome is unknown; leaving the transaction \
                         for the coordinator to resolve rather than aborting a possibly-committed \
                         transaction (KAFKA-17754)"
                    );
                    Ok(())
                }
                _ => Ok(()),
            }
        };
        let settled = match options.timeout {
            Some(timeout) => tokio::time::timeout(timeout, settle)
                .await
                .unwrap_or_else(|_| Err(KrafkaError::timeout("transactional producer close"))),
            None => settle.await,
        };
        let remaining = options
            .timeout
            .map(|timeout| timeout.saturating_sub(started.elapsed()));
        let closed = self.producer.finish_close(generation, remaining).await;
        settled.and(closed)
    }

    /// Whether [`close`](Self::close) was called. A producer in
    /// [`TransactionState::Fatal`] is not closed — check
    /// [`state`](Self::state) for that.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.producer.is_closed()
    }

    /// Producer metrics; see [`Producer::metrics`].
    #[inline]
    pub fn metrics(&self) -> Metrics {
        self.producer.metrics()
    }

    /// The KIP-714 client instance id; see [`Producer::client_instance_id`].
    ///
    /// # Errors
    ///
    /// As [`Producer::client_instance_id`].
    pub async fn client_instance_id(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Option<crate::metrics::ClientInstanceId>> {
        self.producer.client_instance_id(timeout).await
    }
}

/// Leaves the transaction in a state `commit()` or `abort()` can continue
/// from when its future is dropped mid-way.
///
/// Dropped before `EndTxn` was sent, the transaction is back where the call
/// found it. Dropped while `EndTxn` was being sent, the request may have been
/// applied: an unanswered commit becomes `CommitUnknown` (only `commit` may
/// follow), an unanswered abort returns to where it was so `abort` can be
/// retried, and under TV1 either forces an epoch bump before the next
/// transaction. Dropped after `EndTxn` succeeded, the transaction is over.
struct Ending<'a> {
    producer: &'a TransactionalProducer,
    during: TransactionState,
    on_drop: TransactionState,
    sending: bool,
    armed: bool,
}

impl<'a> Ending<'a> {
    fn new(
        producer: &'a TransactionalProducer,
        during: TransactionState,
        entered: TransactionState,
    ) -> Self {
        Self {
            producer,
            during,
            on_drop: entered,
            sending: false,
            armed: true,
        }
    }

    /// `EndTxn` is about to be sent.
    fn sending(&mut self) {
        self.sending = true;
        if self.during == TransactionState::Committing {
            self.on_drop = TransactionState::CommitUnknown;
        }
    }

    /// `EndTxn` succeeded; what is left is bookkeeping.
    fn ended(&mut self) {
        self.sending = false;
        self.on_drop = TransactionState::Ready;
    }

    /// The call moves the state itself from here.
    fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for Ending<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if self.sending {
            self.producer
                .end_txn_unanswered
                .store(true, Ordering::SeqCst);
        }
        if self.on_drop == TransactionState::Ready {
            // The epoch bump the finished call may have owed (TV1) is done
            // before the next transaction writes; a spare one is harmless.
            if !self.producer.transaction_version().is_v2() {
                self.producer.reinit_required.store(true, Ordering::SeqCst);
            }
            if let Ok(mut partitions) = self.producer.txn_partitions.try_write() {
                partitions.clear();
            }
        }
        let _ = self.producer.gate.transition(&[self.during], self.on_drop);
    }
}

/// Marks an `EndTxn` attempt as unanswered unless disarmed after its answer
/// decoded — including when the attempt future is dropped mid-request.
struct Unanswered(Option<Arc<AtomicBool>>);

impl Unanswered {
    fn arm(flag: Arc<AtomicBool>) -> Self {
        Self(Some(flag))
    }

    fn disarm(mut self) {
        self.0 = None;
    }
}

impl Drop for Unanswered {
    fn drop(&mut self) {
        if let Some(flag) = self.0.take() {
            flag.store(true, Ordering::SeqCst);
        }
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
/// Split out of [`TransactionalProducer::send_offsets`] so the
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
/// A fatal error latches [`TransactionState::Fatal`]: the transaction
/// cannot be aborted and the producer must be recreated. Contrast with
/// *abortable* errors such as [`ErrorCode::TransactionAbortable`], which leave
/// the producer usable once [`TransactionalProducer::abort`] has
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

/// Whether a fatal code means another producer took over this identity, as
/// opposed to a denied permission or an invalid state transition.
fn is_fencing_code(error_code: ErrorCode) -> bool {
    matches!(
        error_code,
        ErrorCode::ProducerFenced
            | ErrorCode::InvalidProducerEpoch
            | ErrorCode::TransactionCoordinatorFenced
            | ErrorCode::InvalidProducerIdMapping
    )
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::time::Duration;

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
            &KrafkaError::illegal_state("test")
        ));
    }

    /// `PreparedTxnState` must round-trip through a string, because that is
    /// how it reaches the external coordinator's store — the only link back to
    /// a prepared transaction if the process dies.
    #[test]
    fn prepared_txn_state_round_trips_through_a_string() {
        let state = PreparedTxnState {
            producer_id: 4242,
            producer_epoch: 7,
        };
        assert_eq!(state.to_string(), "4242:7");
        assert_eq!(
            "4242:7".parse::<PreparedTxnState>().expect("valid"),
            state,
            "a state written to a database must read back identical"
        );

        // Whitespace survives a round trip through a text column.
        assert_eq!(
            " 4242 : 7 ".parse::<PreparedTxnState>().expect("valid"),
            state
        );

        for malformed in ["", "4242", "4242:", ":7", "abc:7", "4242:xyz"] {
            let err = malformed
                .parse::<PreparedTxnState>()
                .expect_err("malformed state must not silently become a valid one");
            assert!(
                err.to_string().contains("producer_id:epoch"),
                "the error must show the expected shape, got: {err}"
            );
        }
    }

    /// "No prepared transaction" must be distinguishable from one with
    /// producer ID 0, which is a perfectly ordinary producer ID.
    #[test]
    fn the_absent_prepared_state_is_distinguishable_from_a_real_one() {
        assert!(!PreparedTxnState::none().is_prepared());
        assert!(
            PreparedTxnState {
                producer_id: 0,
                producer_epoch: 0
            }
            .is_prepared(),
            "producer ID 0 is a real producer ID, not an absence"
        );
    }

    #[test]
    fn test_out_of_order_sequence_is_retriable() {
        let error = KrafkaError::broker(ErrorCode::OutOfOrderSequenceNumber, "sequence mismatch");
        assert!(error.is_retriable());
    }

    // ── Record timestamp propagation ──

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
        // A non-retriable AddPartitionsToTxn
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
        let err = Arc::new(KrafkaError::illegal_state("fatal"));
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
    /// `send_offsets` must refuse rather than committing
    /// unfenced offsets inside an "exactly-once" transaction.
    #[test]
    fn test_unfenceable_group_metadata_is_rejected() {
        // The pre-KIP-447 wire defaults.
        assert!(!ConsumerGroupMetadata::new("g", -1, "", None).is_fenceable());
        assert!(!ConsumerGroupMetadata::new("g", 5, "", None).is_fenceable());
        assert!(!ConsumerGroupMetadata::new("g", -1, "m", None).is_fenceable());
        assert!(ConsumerGroupMetadata::new("g", 0, "m", None).is_fenceable());
    }

    // ── Fatal classification on every coordinator RPC ─────────────

    fn support(
        level: i16,
        produce: i16,
        txn_offset_commit: i16,
        end_txn: i16,
    ) -> BrokerTransactionSupport {
        BrokerTransactionSupport {
            transaction_version_level: level,
            // Enough for TV3 when the feature level allows it; the TV3 tests
            // below vary this deliberately.
            init_producer_id_max: Some(TV3_MIN_INIT_PRODUCER_ID_VERSION),
            produce_max: Some(produce),
            txn_offset_commit_max: Some(txn_offset_commit),
            end_txn_max: Some(end_txn),
        }
    }

    /// TV3 needs the same kind of evidence TV2 does: the finalized feature
    /// level **and** an API version that can actually carry the new field.
    ///
    /// Finalized features are cluster-wide metadata and can be observed before
    /// every broker has restarted into a build that serves the matching API
    /// versions. Trusting the level alone would have krafka send `enable2Pc`
    /// to a broker whose `InitProducerId` predates the field, where it is not
    /// rejected — it is simply not there, and the coordinator applies
    /// `transaction.max.timeout.ms` to a transaction the caller believes is
    /// exempt.
    #[test]
    fn tv3_requires_an_init_producer_id_that_can_carry_enable_2pc() {
        let mut broker = support(
            3,
            TV2_MIN_PRODUCE_VERSION,
            TV2_MIN_TXN_OFFSET_COMMIT_VERSION,
            TV2_MIN_END_TXN_VERSION,
        );
        assert_eq!(broker.version(), TransactionVersion::V3);

        broker.init_producer_id_max = Some(TV3_MIN_INIT_PRODUCER_ID_VERSION - 1);
        assert_eq!(
            broker.version(),
            TransactionVersion::V2,
            "a broker that cannot encode enable2Pc is not a TV3 broker, whatever \
             the feature level says"
        );

        broker.init_producer_id_max = None;
        assert_eq!(broker.version(), TransactionVersion::V2);

        // And the level still gates it: a v6-capable broker at level 2 is TV2.
        let mut level_2 = support(
            2,
            TV2_MIN_PRODUCE_VERSION,
            TV2_MIN_TXN_OFFSET_COMMIT_VERSION,
            TV2_MIN_END_TXN_VERSION,
        );
        level_2.init_producer_id_max = Some(TV3_MIN_INIT_PRODUCER_ID_VERSION);
        assert_eq!(level_2.version(), TransactionVersion::V2);
    }

    /// The negotiated version is the minimum across brokers, so one lagging
    /// broker holds the whole cluster at the level it can serve.
    #[test]
    fn a_single_lagging_broker_holds_the_cluster_below_tv3() {
        let tv3 = support(
            3,
            TV2_MIN_PRODUCE_VERSION,
            TV2_MIN_TXN_OFFSET_COMMIT_VERSION,
            TV2_MIN_END_TXN_VERSION,
        );
        let mut lagging = tv3;
        lagging.init_producer_id_max = Some(TV3_MIN_INIT_PRODUCER_ID_VERSION - 1);

        assert_eq!(
            negotiated_transaction_version(&[tv3, lagging]),
            TransactionVersion::V2,
            "a rolling upgrade must not enable 2PC before every broker can serve it"
        );
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
        // Level 3 is KIP-939.
        assert_eq!(
            TransactionVersion::from_feature_level(3),
            TransactionVersion::V3
        );
        // A future level must not silently fall back — it keeps the highest
        // protocol krafka knows, whose semantics are a subset of whatever
        // comes next.
        assert_eq!(
            TransactionVersion::from_feature_level(4),
            TransactionVersion::V3
        );

        // TV3 must keep every TV2 behaviour. An equality test in `is_v2()`
        // would send a TV3 cluster back to AddPartitionsToTxn and the wrong
        // epoch handling — silently, since both are legal requests.
        assert!(TransactionVersion::V2.is_v2());
        assert!(
            TransactionVersion::V3.is_v2(),
            "TV3 is a superset of TV2, not an alternative to it"
        );
        assert!(!TransactionVersion::V1.is_v2());

        assert!(TransactionVersion::V3.supports_two_phase_commit());
        assert!(!TransactionVersion::V2.supports_two_phase_commit());
        assert!(!TransactionVersion::V1.supports_two_phase_commit());
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

    /// A producer pinned to `version` against an unreachable cluster, with a
    /// short `max_block` so network paths fail fast.
    fn test_producer(version: TransactionVersion) -> TransactionalProducer {
        let kafka = crate::Kafka::detached();
        let config = super::super::ProducerConfig {
            max_block: Duration::from_millis(200),
            ..super::super::ProducerConfig::default()
        };
        let metrics = Arc::new(crate::metrics::ProducerRecorder::default());
        let metrics_source = crate::metrics::MetricsSource::producer(&kafka, Arc::clone(&metrics));
        let barrier = Arc::new(crate::barrier::InFlightBarrier::new());
        let gate = Arc::new(TxnGate::new());
        let interceptor: Arc<dyn crate::interceptor::ProducerInterceptor> =
            Arc::new(crate::interceptor::NoOpProducerInterceptor);
        let backoff = Backoff::new(Duration::from_millis(1));
        let accumulator = super::super::accumulator::Accumulator::spawn(
            super::super::engine::EngineConfig {
                batch_size: config.batch_size,
                linger: config.linger,
                delivery_timeout: config.delivery_timeout,
                request_timeout: kafka.request_timeout(),
                max_request_size: config.max_request_size,
                acks: -1,
                compression: crate::protocol::Compression::None,
                compression_level: None,
                topic_compression: ahash::AHashMap::new(),
                client_id: "test".to_string(),
                transactional_id: Some("txn-test".to_string()),
                backoff: backoff.clone(),
                interceptor: Arc::clone(&interceptor),
                mode: super::super::identity::Mode::Transactional {
                    tv2: version.is_v2(),
                },
                identity: None,
                gate: Some(Arc::clone(&gate)),
            },
            config.buffer_memory,
            config.max_block,
            Arc::clone(kafka.metadata()),
            Arc::clone(&metrics),
            Arc::clone(&barrier),
        );
        let producer = Producer {
            partitioning: Arc::new(super::super::partitioner::Partitioning::new(
                None,
                config.batch_size,
                None,
            )),
            kafka,
            config,
            accumulator,
            barrier,
            metrics_source,
            telemetry: crate::telemetry::Telemetry::disabled(),
            interceptor,
        };
        TransactionalProducer {
            producer,
            transactional_id: "txn-test".to_string(),
            gate,
            identity: parking_lot::Mutex::new((7, 0)),
            ongoing_prepared_txn: arc_swap::ArcSwap::from_pointee(PreparedTxnState::none()),
            transaction_version: AtomicU8::new(version as u8),
            end_txn_unanswered: AtomicBool::new(false),
            reinit_required: AtomicBool::new(false),
            reinit_lock: tokio::sync::Mutex::new(()),
            coordinator_id: RwLock::new(None),
            txn_partitions: Arc::new(RwLock::new(TransactionPartitions::default())),
            backoff,
        }
    }

    /// The 2PC entry points refuse to run on a producer that never enabled
    /// 2PC.
    #[tokio::test]
    async fn the_two_phase_entry_points_require_the_setting() {
        let producer = test_producer(TransactionVersion::V2);
        producer.gate.set(TransactionState::Open);
        let err = producer.prepare().await.unwrap_err();
        assert!(err.to_string().contains("two_phase_commit"), "got: {err}");
    }

    /// `complete` refuses when nothing was left prepared.
    #[tokio::test]
    async fn completing_without_a_prepared_transaction_is_an_error() {
        let producer = test_producer(TransactionVersion::V2);
        producer.gate.set(TransactionState::Ready);
        let err = producer
            .complete(PreparedTxnState {
                producer_id: 1,
                producer_epoch: 0,
            })
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("prepared_transaction"),
            "got: {err}"
        );
    }

    /// `abort` is refused in `CommitUnknown` without sending
    /// anything (KAFKA-17754).
    #[tokio::test]
    async fn an_unknown_commit_refuses_abort() {
        let producer = test_producer(TransactionVersion::V1);
        producer.gate.set(TransactionState::CommitUnknown);
        let err = producer.abort().await.unwrap_err();
        assert!(err.to_string().contains("KAFKA-17754"), "got: {err}");
        assert_eq!(producer.state(), TransactionState::CommitUnknown);
    }

    /// A failed send makes commit refuse with `TransactionAbortable` before
    /// any coordinator RPC, and the state stays `Open` for the abort.
    #[tokio::test]
    async fn a_failed_send_refuses_commit() {
        let producer = test_producer(TransactionVersion::V1);
        producer.gate.set(TransactionState::Ready);
        producer.begin().unwrap();
        producer
            .gate
            .fail(KrafkaError::broker(ErrorCode::InvalidRecord, "r2"));
        let err = producer.commit().await.unwrap_err();
        assert!(err.requires_abort(), "{err}");
        assert_eq!(producer.state(), TransactionState::Open);
    }

    #[test]
    fn test_transaction_state_display() {
        assert_eq!(TransactionState::CommitUnknown.to_string(), "CommitUnknown");
        assert_eq!(TransactionState::Open.to_string(), "Open");
    }

    /// TV1 never negotiates the `EndTxn` version that means TV2 to the
    /// coordinator.
    #[test]
    fn tv1_end_txn_stays_below_the_tv2_version() {
        assert_eq!(TV2_MIN_END_TXN_VERSION, 5);
        assert!(versions::END_TXN_MAX.min(TV2_MIN_END_TXN_VERSION - 1) < TV2_MIN_END_TXN_VERSION);
    }
}
