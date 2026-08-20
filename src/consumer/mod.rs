//! Kafka consumer implementation.
//!
//! This module provides:
//! - Async consumer for receiving messages
//! - Consumer groups with rebalancing
//! - Offset management (auto and manual commit)
//! - Partition assignment strategies
//!
//! # Delivery Semantics
//!
//! Krafka provides **at-least-once** delivery semantics by default, which is the
//! standard Kafka consumer behavior:
//!
//! 1. Messages are delivered to the application via `poll()` or `recv()`
//! 2. Offsets are committed after delivery (auto-commit or manual)
//! 3. If the consumer crashes after processing but before commit, messages may
//!    be redelivered on restart
//!
//! This is the safest default as it ensures no message loss. For use cases that
//! cannot tolerate duplicates, applications should implement idempotent processing.
//!
//! ## How the guarantee is upheld
//!
//! Two properties make "no message loss" hold rather than merely being the
//! intent:
//!
//! - **Only delivered records are ever acknowledged.** A single `poll()`
//!   fetches a batch, but `recv()` hands records out one at a time and keeps
//!   the remainder in an internal buffer. Commits are clamped to the lowest
//!   offset still sitting in that buffer, so records that were fetched but
//!   never given to the application are never committed. A crash re-delivers
//!   them.
//! - **A cancelled `poll()` never loses records.** The fetch position is
//!   advanced as the very last step of `poll()`, after the records are ready
//!   to be returned and with nothing awaited in between. Dropping the future
//!   — a `tokio::time::timeout` firing, or a losing `select!` branch — leaves
//!   the position untouched, so the records are simply fetched again.
//!
//! Note that auto-commit still acknowledges records once they have been
//! *delivered*, not once they have been *processed*. If the application needs
//! the commit to reflect completed processing, disable auto-commit and call
//! [`Consumer::commit`] after processing.
//!
//! ## Controlling Commit Behavior
//!
//! - **Auto-commit** (default): Offsets are committed periodically in the background
//! - **Manual commit**: Disable auto-commit and call `commit()` explicitly
//!
//! For at-most-once semantics (where message loss is acceptable but duplicates are not),
//! commit offsets before processing:
//!
//! ```ignore
//! let records = consumer.poll(Duration::from_secs(1)).await?;
//! consumer.commit().await?;  // Commit BEFORE processing
//! for record in records {
//!     process(record);  // If this crashes, message is lost
//! }
//! ```

mod builder;
mod config;
mod fetch_session;
mod group;
mod group_metadata;
mod lock_order;
mod offset;
mod record;
mod stream;

pub mod compacted;

pub use builder::ConsumerBuilder;
pub use compacted::{
    CompactedEntry, CompactedTable, CompactedTableClearListener, CompactedTableSnapshot,
    CompactedTopicConsumer, TableChange,
};
pub use config::{
    AutoOffsetReset, ConsumerConfig, GroupProtocol, IsolationLevel, PartitionAssignmentStrategy,
};
use group::ErasedRebalanceListener;
pub use group::{
    ConsumerGroup, ConsumerRebalanceListener, CooperativeStickyAssignor, GroupCoordinator,
    GroupMember, GroupState, HeartbeatController, HeartbeatStatus, MemberAssignment,
    NoOpRebalanceListener, PartitionAssignor, PendingRebalance, RangeAssignor, RoundRobinAssignor,
    StickyAssignor,
};
pub use group_metadata::ConsumerGroupMetadata;
pub use offset::{OffsetAndMetadata, OffsetStore, ResetOffset};
pub use record::{ConsumerRecord, ConsumerRecords, TopicPartition};
pub use stream::ConsumerStream;

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use std::future::{Future, Ready, ready};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use parking_lot::Mutex as SyncMutex;
use tracing::{debug, error, info, trace, warn};

use lock_order::LeveledRwLock;

use crate::error::{KrafkaError, ProtocolErrorKind, RecvError, Result};
use crate::metadata::{BrokerInfo, ClusterMetadata, TopicInfo, broker_info_for_node};
use crate::metrics::{ConnectionMetrics, ConsumerMetrics};
use crate::network::{ConnectionConfig, ConnectionPool};
use crate::protocol::{
    ApiKey, FetchPartitionRequest, FetchRequest, FetchResponse, FetchTopicRequest,
    ListOffsetsRequest, ListOffsetsRequestPartition, ListOffsetsRequestTopic, ListOffsetsResponse,
    RecordBatch, VersionedDecode, VersionedEncode, validate_topic_name, validate_topic_names,
    versions,
};
use crate::{Offset, PartitionId};

use fetch_session::FetchSessionCache;

// ── LOCK ORDER ──────────────────────────────────────────────────────────────
//
// `Consumer` holds several synchronization primitives. Two rules prevent
// deadlocks and executor stalls:
//
//   1. Acquire locks in the order listed below. Never acquire an
//      earlier-numbered lock while holding a later-numbered one.
//   2. If you hold an **async** lock (`tokio::sync::{RwLock, Mutex}`) across
//      an `.await`, the awaited operation must not (transitively) acquire
//      any lock numbered **lower** than the one held. In practice: do not
//      call back into `Consumer` methods while holding a lock. Release with
//      `drop(...)` or a scoped `{ … }` block first.
//
// Order:
//   1. `subscriptions`         (async — read across `poll()` network paths)
//   2. `assignments`           (async — read across `poll()` network paths)
//   3. `offsets`               (async — read across commit/fetch RPC paths)
//   4. `paused`                (async — read across `poll()` network paths)
//   5. `partition_state`       (async — per-partition fetch-derived caches)
//   6. `recv_buffer`           (sync — `parking_lot::Mutex`, pure mutation)
//   7. `fetch_sessions`        (sync — `parking_lot::Mutex`, pure mutation;
//                               ALWAYS release before fetch RPC send/recv)
//   8. `last_auto_commit`      (sync — `parking_lot::Mutex<Instant>`)
//
// ── ASYNC / SYNC BOUNDARY ────────────────────────────────────────────────────
//
// DANGER: NEVER hold a sync (`parking_lot`) lock (levels 6–8) while
// awaiting a `tokio::sync` lock (levels 1–5) or any other async operation.
// `parking_lot` locks block the OS thread; if the thread is a Tokio worker
// and the awaited task needs the same worker, the runtime will deadlock.
//
// Safe pattern — release sync lock before any `.await`:
//
//   let value = {
//       let guard = self.recv_buffer.lock();
//       guard.front().cloned()         // short sync critical section
//   };                                  // guard dropped here
//   some_async_op().await;              // safe: no lock held
//
// Unsafe anti-pattern (DO NOT DO THIS):
//
//   let _guard = self.recv_buffer.lock();   // sync lock acquired
//   some_async_op().await;                  // DEADLOCK risk: blocks Tokio worker
//
// The sync (`parking_lot`) primitives are chosen only for critical sections
// with NO `.await` inside. They can still block a Tokio worker thread if
// contended, and some sections are O(n) (e.g. `recv_buffer.retain`), so do
// not assume they are always nanosecond-scale. Keep them short, avoid
// contention, and always release them before async work, network I/O, or
// callbacks back into `Consumer`. Do not convert any async lock above
// without first auditing every call site for `.await` under the lock.
//
// Per-partition caches (`high_watermark`, `log_start_offset`,
// `preferred_replica`, `offset_retry_backoff`) were previously four separate
// `RwLock<HashMap<_, _>>` fields. They are consolidated into a single
// `partition_state` map so that revocation / reset / close paths cannot leave
// a partition partially populated. Adding a new per-partition cache? Add a
// field to `PartitionState`; do not introduce another `RwLock`.

/// Per-topic-partition state cached locally by the consumer.
///
/// All fields are populated from fetch responses or consumer-protocol
/// feedback. Grouping them under a single lock (`Consumer::partition_state`)
/// guarantees that revocation and reset paths cannot leave a partition in a
/// partially-populated state — a silent bug class that existed when each
/// cache lived under its own `RwLock`.
///
/// `Default` returns an all-`None` state, suitable for `entry().or_default()`
/// on first insert.
#[derive(Default)]
struct PartitionState {
    /// Latest known high watermark (log-end offset), from `FetchResponse`.
    /// `None` until first observed in a fetch response for this partition
    /// (the broker reports it on every response with `high_watermark >= 0`,
    /// including empty and error responses).
    high_watermark: Option<Offset>,
    /// Monotonic instant at which `high_watermark` was last updated.
    /// Used by [`Consumer::lag`] to report stale partitions.
    watermark_updated_at: Option<Instant>,
    /// Latest known last-stable-offset (LSO), from `FetchResponse` v4+.
    ///
    /// The LSO is the first offset belonging to an *open* transaction: under
    /// `read_committed` the broker never delivers a record at or above it, so
    /// it — not the high watermark — is the end of the readable log.
    ///
    /// This distinction is not cosmetic. With a long-running transaction open
    /// on a partition, `high_watermark - position` never reaches zero even
    /// though the consumer has read everything it is *allowed* to read, so a
    /// `read_committed` consumer reported permanent phantom lag and
    /// [`Consumer::is_caught_up`] could never return `true`. Matches the Java
    /// client's `SubscriptionState.partitionLag(tp, isolationLevel)`.
    ///
    /// `None` until observed (Fetch v4+ reports it on every response,
    /// including empty ones); brokers use `-1` when there is no LSO.
    last_stable_offset: Option<Offset>,
    /// Latest known log start offset, from `FetchResponse`.
    /// `None` until first observed in a fetch response for this partition
    /// (reported in Fetch v5+ whenever `log_start_offset >= 0`, including
    /// empty and error responses).
    log_start_offset: Option<Offset>,
    /// KIP-392 preferred read replica and its expiry time.
    ///
    /// When a broker returns a `preferred_read_replica` in a fetch response,
    /// subsequent fetches for that partition are routed to the indicated
    /// replica until the entry expires (after `metadata_max_age`).
    /// `None` means the leader should be used (the default).
    preferred_replica: Option<(crate::BrokerId, Instant)>,
    /// Next allowed retry time and current exponential backoff interval for
    /// offset-resolution failures. `None` once the partition is successfully
    /// resolved or was never retried. Prevents retry storms when offset
    /// resolution fails persistently (e.g. broker unavailable).
    offset_retry_backoff: Option<(Instant, Duration)>,
    /// Leader epoch of the record batch the fetch position last advanced
    /// through (KIP-320).
    ///
    /// Sent back to the broker as `last_fetched_epoch` so it can verify that
    /// the client's `(position, epoch)` pair actually exists in its log. When
    /// it does not — the signature of an unclean leader election — the broker
    /// answers with a `DivergingEpoch` instead of records.
    ///
    /// `None` means the epoch at the current position is genuinely unknown:
    /// a freshly assigned partition, or one that was just repositioned by
    /// `seek()` or an offset reset. The request then carries `-1`, which
    /// disables divergence detection until the first batch is consumed.
    last_fetched_epoch: Option<i32>,
    /// Whether the current fetch position has been checked against the
    /// leader's log with `OffsetForLeaderEpoch`.
    ///
    /// `false` on a fresh assignment and after every reposition, so the
    /// consumer validates before its first fetch from that position rather
    /// than waiting for the broker to reject it. This is what closes the
    /// window in which an unclean leader election silently hands the consumer
    /// records the new leader never had.
    position_validated: bool,
}

impl PartitionState {
    /// The highest offset this consumer is permitted to read, given
    /// `isolation_level`.
    ///
    /// Under `read_uncommitted` that is the high watermark. Under
    /// `read_committed` it is the last stable offset when the broker has
    /// reported one, because the broker will not deliver a record at or above
    /// the LSO — so every lag, "caught up" and end-offset answer has to be
    /// measured against it or a single open transaction makes a fully drained
    /// consumer look permanently behind.
    ///
    /// Falls back to the high watermark when no LSO has been seen yet
    /// (Fetch below v4, or before the first response for the partition).
    #[inline]
    fn readable_end_offset(&self, isolation_level: IsolationLevel) -> Option<Offset> {
        match isolation_level {
            IsolationLevel::ReadCommitted => self.last_stable_offset.or(self.high_watermark),
            IsolationLevel::ReadUncommitted => self.high_watermark,
        }
    }
}

/// Cluster metadata snapshot returned by [`Consumer::fetch_metadata`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct FetchMetadataResult {
    /// All brokers known to the cluster.
    pub brokers: Vec<BrokerInfo>,
    /// Topics returned in the metadata snapshot.
    ///
    /// When [`Consumer::fetch_metadata`] is called with `None`, this contains
    /// all cached topics. When called with `Some(topic)`, this contains the
    /// matching topic if found, or is empty if that topic was not found in the
    /// cluster.
    pub topics: Vec<TopicInfo>,
}

/// Outcome of [`Consumer::batch_recv`].
#[non_exhaustive]
#[derive(Debug)]
pub enum BatchRecvOutcome {
    /// Records were collected (possibly a partial batch).
    Records(Vec<ConsumerRecord>),
    /// The timeout elapsed before any records were collected.
    TimedOut,
    /// The consumer closed before any records were collected.
    Closed,
    /// The call requested zero records (`max_records == 0`).
    EmptyRequest,
}

/// Result of [`Consumer::lag`], containing per-partition lag and staleness
/// information.
///
/// High watermarks are cached from the most recent fetch response. A partition
/// is considered *stale* if its cached watermark has not been updated within
/// [`ConsumerBuilder::lag_staleness_threshold`](crate::consumer::ConsumerBuilder::lag_staleness_threshold)
/// (default: 60 s). Stale lag
/// values are still returned, but the calling code can decide to treat them
/// as unreliable.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct LagResult {
    /// Per-partition lag in messages.
    ///
    /// Only partitions where both the high watermark and current position are
    /// known are included. Newly-assigned partitions that haven't yet received
    /// a fetch response are omitted.
    pub lag: HashMap<(String, PartitionId), u64>,
    /// Partitions whose cached high watermark is stale.
    ///
    /// A partition appears here when its watermark has not been refreshed
    /// within the staleness threshold. The corresponding lag value is still
    /// present in [`Self::lag`] but may be outdated.
    pub stale_partitions: Vec<(String, PartitionId)>,
}

/// A position being committed for one partition.
///
/// `leader_epoch` is the piece that makes KIP-320 truncation detection survive
/// a restart or a rebalance. Kafka stores it alongside the offset and returns
/// it from `OffsetFetch`, so the next owner of the partition can ask
/// `OffsetsForLeaderEpoch` whether the log it is about to read still contains
/// that `(offset, epoch)` pair. Committing `-1` throws the check away: the
/// resumed consumer cannot tell a truncated log from an intact one and will
/// silently read a diverged one.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommitPosition {
    /// Next offset the group should read from.
    pub offset: Offset,
    /// Leader epoch of the record this position sits just past, or `-1` when
    /// the consumer genuinely does not know it (a position that came from a
    /// seek or an offset reset rather than from consuming a batch).
    pub leader_epoch: i32,
    /// Optional application metadata stored with the offset.
    pub metadata: Option<String>,
}

/// A committed position read back from the coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommittedPosition {
    /// The committed offset.
    pub offset: Offset,
    /// Leader epoch stored with it, or `-1` if the group never committed one.
    pub leader_epoch: i32,
}

type CommitRequestOffsets = HashMap<(String, PartitionId), CommitPosition>;

/// Handle returned by [`Consumer::commit_async`].
///
/// Await the handle to observe the final commit outcome. Dropping it detaches
/// the background task and discards the result.
#[must_use = "await the returned handle to observe async offset commit outcome"]
#[non_exhaustive]
pub enum OffsetCommitHandle {
    /// Immediate commit result without spawning a background task.
    Ready(Ready<Result<()>>),
    /// Background task handle that resolves to the commit result.
    Task(tokio::task::JoinHandle<Result<()>>),
}

impl OffsetCommitHandle {
    fn ready(result: Result<()>) -> Self {
        Self::Ready(ready(result))
    }
}

impl Future for OffsetCommitHandle {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Ready(fut) => Pin::new(fut).poll(cx),
            Self::Task(handle) => match Pin::new(handle).poll(cx) {
                Poll::Ready(Ok(result)) => Poll::Ready(result),
                Poll::Ready(Err(error)) => Poll::Ready(Err(KrafkaError::invalid_state(format!(
                    "consumer commit task failed: {error}"
                )))),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

/// A Kafka consumer.
pub struct Consumer {
    /// Consumer configuration.
    config: ConsumerConfig,
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
    /// Subscribed topics. Lock level 1 — acquire first (see `LOCK ORDER`).
    subscriptions: LeveledRwLock<1, HashSet<String>>,
    /// Assigned partitions. Lock level 2 — acquire after `subscriptions`.
    assignments: LeveledRwLock<2, HashMap<String, Vec<PartitionId>>>,
    /// Current offsets. Lock level 3 — acquire after `assignments`.
    offsets: LeveledRwLock<3, HashMap<(String, PartitionId), Offset>>,
    /// Paused partitions. Lock level 4 — acquire after `offsets`.
    paused: LeveledRwLock<4, HashSet<(String, PartitionId)>>,
    /// Whether the consumer is closed.
    closed: std::sync::atomic::AtomicBool,
    /// Set by [`Consumer::wakeup`], cleared by the `poll()` that observes it.
    ///
    /// A flag *and* a `Notify` are both needed: the flag makes a `wakeup()`
    /// that lands before `poll()` is called still take effect (a bare `Notify`
    /// only wakes tasks already waiting, so that call would be lost), and the
    /// `Notify` interrupts a `poll()` already parked on a fetch.
    wakeup_flag: std::sync::atomic::AtomicBool,
    /// Wakes a `poll()` that is currently parked on a broker fetch.
    wakeup_notify: tokio::sync::Notify,
    /// Group coordinator for full group protocol support.
    group_coordinator: Option<Arc<GroupCoordinator>>,
    /// Consumer metrics.
    metrics: Arc<ConsumerMetrics>,
    /// Rebalance listener.
    rebalance_listener: Arc<dyn ErasedRebalanceListener>,
    /// Consumer interceptor.
    interceptor: Arc<dyn crate::interceptor::ConsumerInterceptor>,
    /// Last auto-commit time (for auto-commit timer).
    ///
    /// Held only for a single read/write of an `Instant` — no `.await` under
    /// the lock — so a sync `parking_lot::Mutex` is the correct primitive.
    last_auto_commit: SyncMutex<Instant>,
    /// Buffer for records returned by `recv()`.
    /// `poll()` may return multiple records; `recv()` buffers the rest here.
    ///
    /// All call sites mutate (`pop_front` / `extend` / `clear`) or read
    /// `len()` without crossing an `.await`, so a sync `parking_lot::Mutex`
    /// is used instead of a tokio async lock. The old `RwLock` had no
    /// concurrent readers in practice — every access took the write side.
    recv_buffer: SyncMutex<std::collections::VecDeque<ConsumerRecord>>,
    /// Round-robin cursor over the assigned partitions, advanced once per
    /// `poll()`.
    ///
    /// Both the broker's `fetch.max.bytes` accounting and this client's
    /// `max_poll_records` cap consume partitions in request order, so a fixed
    /// order starves whatever sits at the tail. Rotating the (sorted) partition
    /// list by one position per poll gives every partition its turn at the
    /// front, which is what the Java client achieves with
    /// `PartitionStates.moveToEnd`.
    fetch_rotation: std::sync::atomic::AtomicUsize,
    /// Per-broker fetch session cache (KIP-227).
    ///
    /// Every critical section is pure sync (session bookkeeping: build
    /// request, update from response, reset on error). The actual fetch
    /// RPC `send_request().await` is always performed **after** the lock is
    /// released — never while it is held — so a sync `parking_lot::Mutex`
    /// is the correct primitive. If you add a new use site, keep the
    /// critical section straight-line-sync.
    fetch_sessions: SyncMutex<FetchSessionCache>,
    /// Consolidated per-partition state: high watermark, log start offset,
    /// preferred replica (KIP-392), and offset-retry backoff. Lock level 5 —
    /// acquire last among async locks. A single lock replaces what was
    /// previously four separate `RwLock<HashMap>` fields; see the `LOCK ORDER`
    /// comment above and [`PartitionState`] for details.
    partition_state: LeveledRwLock<5, HashMap<(String, PartitionId), PartitionState>>,
    /// Optional key decoder applied transparently after each `poll()` / `recv()`.
    ///
    /// When set, every consumed record's key is passed through this decoder
    /// before being returned to the caller. Equivalent to `key.deserializer`
    /// in the Java `KafkaConsumer`.
    key_deserializer: Option<Arc<dyn crate::serdes::Deserializer>>,
    /// Optional value decoder applied transparently after each `poll()` / `recv()`.
    ///
    /// When set, every consumed record's value is passed through this decoder
    /// before being returned to the caller. Equivalent to `value.deserializer`
    /// in the Java `KafkaConsumer`.
    value_deserializer: Option<Arc<dyn crate::serdes::Deserializer>>,
}

/// A pending advance of a partition's fetch position, tagged with the position
/// the fetch was issued from.
///
/// Carrying `requested` is what makes a fetch response verifiable after the
/// fact. A response is only meaningful relative to the position that produced
/// it: if the application called `seek()` while the fetch was in flight, the
/// records in the response are from the *old* position and applying `next`
/// would move the consumer back to where the seek was supposed to take it away
/// from, silently discarding the seek.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FetchOffsetUpdate {
    /// Position the fetch request was built from.
    requested: Offset,
    /// Position to advance to, i.e. last delivered offset + 1.
    next: Offset,
    /// Leader epoch of the record batch that `next` falls just past, or `-1`
    /// when the response carried nothing that pins the epoch down.
    ///
    /// Travelling with the position update keeps the two in step: the epoch is
    /// only stored if the position it describes is actually applied, so the
    /// consumer never reports an epoch that does not match where it is.
    epoch: i32,
}

/// A partition whose record batch at the current fetch position could not be
/// decoded, so the partition cannot advance past it.
///
/// This is *not* a trailing batch cut short by `partition_max_bytes` — that is
/// expected and re-requested. It is corruption: a CRC mismatch, an unsupported
/// magic byte, or an out-of-range field. Re-fetching the same offset returns
/// the same bytes, so there is no recovery to automate and the condition is
/// carried out to the application rather than retried in a loop.
#[derive(Debug)]
struct PartitionFetchFault {
    /// The partition that cannot advance.
    key: (String, PartitionId),
    /// The position it is stuck at — the offset the fetch was issued from.
    offset: Offset,
    /// The decode failure, preserved so its
    /// [`ProtocolErrorKind`] reaches the caller intact.
    error: KrafkaError,
}

impl PartitionFetchFault {
    /// Render the fault as the error `poll()` returns.
    ///
    /// Names the partition and offset, and states both remedies, because
    /// neither is discoverable from a bare `CrcMismatch`.
    fn into_error(self, total_faults: usize) -> KrafkaError {
        let (topic, partition) = self.key;
        let others = match total_faults {
            0 | 1 => String::new(),
            n => format!(" ({} partitions affected in this poll)", n),
        };
        KrafkaError::protocol_kind(
            self.error
                .protocol_error_kind()
                .unwrap_or(ProtocolErrorKind::Malformed),
            format!(
                "undecodable record batch at {topic}-{partition} offset {}{others}; \
                 the partition cannot advance past it and re-fetching returns the same \
                 bytes. Either seek({topic}-{partition}) past the offset to skip the \
                 corrupt data, or pause({topic}-{partition}) to keep consuming every \
                 other partition while you investigate. Underlying error: {}",
                self.offset, self.error
            ),
        )
    }
}

/// Everything one broker's fetch produced.
///
/// A named struct rather than a tuple because the fourth field — per-partition
/// decode faults — must not be silently dropped at the call site the way a
/// widened tuple element can be.
#[derive(Debug, Default)]
struct FetchOutcome {
    /// Records decoded from every partition that decoded cleanly.
    records: Vec<ConsumerRecord>,
    /// Pending position advances, tagged with the position fetched from.
    offset_updates: Vec<((String, PartitionId), FetchOffsetUpdate)>,
    /// High watermarks reported for each partition.
    hw_updates: Vec<((String, PartitionId), Offset)>,
    /// Partitions that could not decode at their current position.
    ///
    /// Kept separate from `Result::Err` deliberately: corruption is a
    /// *partition-level* fault, and failing the whole broker request would
    /// discard records from every healthy partition sharing that leader.
    faults: Vec<PartitionFetchFault>,
}

/// Decide which pending fetch-position updates are still valid.
///
/// An update is applied only if the partition's stored position is *still* the
/// one the fetch was issued from. Anything else means the position moved while
/// the fetch was in flight — a `seek()`, an offset reset, or a rebalance — and
/// the response describes a position the consumer has deliberately left.
///
/// Returns the keys whose updates were discarded, for logging.
fn apply_fetch_offset_updates(
    offsets: &mut HashMap<(String, PartitionId), Offset>,
    updates: Vec<((String, PartitionId), FetchOffsetUpdate)>,
) -> Vec<(String, PartitionId)> {
    let mut discarded = Vec::new();
    for (key, update) in updates {
        match offsets.get(&key) {
            Some(&current) if current == update.requested => {
                offsets.insert(key, update.next);
            }
            // Either the position moved (seek/reset) or the partition was
            // revoked. In both cases the fetch is stale; drop it.
            _ => discarded.push(key),
        }
    }
    discarded
}

/// Lowest offset still awaiting delivery, per partition.
///
/// This is the boundary between what the application has seen and what the
/// consumer has merely fetched. Every offset below it has been handed out;
/// every offset from it upward is still sitting in the receive buffer, either
/// as prefetch surplus or because its partition was paused.
///
/// It is the single definition of "where the application actually is", shared
/// by [`committable_positions`], [`Consumer::position`], the lag calculations
/// and [`Consumer::is_caught_up`] — so a commit, a reported position and a
/// reported lag can never disagree about the same partition.
///
/// Costs one topic-name clone per *distinct partition* present in the buffer,
/// not one per record, and nothing at all when the buffer is empty — which is
/// the state a poll that delivered everything leaves it in.
fn lowest_undelivered_offsets(
    buffered: &std::collections::VecDeque<ConsumerRecord>,
) -> HashMap<(String, PartitionId), Offset> {
    // Accumulate against borrowed topic names, then own them once at the end:
    // the buffer holds up to `max_buffered_records` records but only a handful
    // of distinct partitions, so this is one allocation per partition instead
    // of one per record.
    let mut lowest: HashMap<(&str, PartitionId), Offset> = HashMap::new();
    for record in buffered {
        lowest
            .entry((record.topic.as_str(), record.partition))
            .and_modify(|o| {
                if record.offset < *o {
                    *o = record.offset;
                }
            })
            .or_insert(record.offset);
    }
    lowest
        .into_iter()
        .map(|((topic, partition), offset)| ((topic.to_string(), partition), offset))
        .collect()
}

/// Compute the highest offset that is safe to commit for each partition.
///
/// A partition's fetch position runs ahead of what the application has
/// actually seen: `poll()` advances it for every record it fetched, but
/// `recv()` hands those records out one at a time and parks the rest in the
/// receive buffer. Committing the fetch position would therefore acknowledge
/// records that are still sitting in the buffer — if the process dies before
/// they are consumed, the group resumes past them and they are never
/// processed.
///
/// The first still-buffered record of a partition marks the boundary: every
/// record below it has been handed to the application, and every record from
/// it upward has not. So the committable position is the fetch position
/// clamped to the lowest buffered offset.
fn committable_positions(
    positions: &HashMap<(String, PartitionId), Offset>,
    buffered: &std::collections::VecDeque<ConsumerRecord>,
) -> HashMap<(String, PartitionId), Offset> {
    let lowest_undelivered = lowest_undelivered_offsets(buffered);

    positions
        .iter()
        .map(|(key, &position)| {
            let committable = match lowest_undelivered.get(key) {
                Some(&first_undelivered) => position.min(first_undelivered),
                None => position,
            };
            (key.clone(), committable)
        })
        .collect()
}

/// Compute aggregate lag from offset and high-watermark caches.
///
/// Returns `(total_lag, max_lag)` where `total_lag` is the sum across all
/// partitions (using `saturating_add`) and `max_lag` is the per-partition
/// maximum. Only partitions present in both maps contribute.
///
/// **Staleness caveat**: high watermarks are refreshed from each fetch
/// response (including empty and error responses) for partitions the
/// consumer polls. Partitions that are not being polled — or whose broker
/// is unreachable — will retain a stale watermark, so the reported lag
/// becomes increasingly inaccurate the longer fetches are skipped. Lag
/// should be treated as *eventually consistent*. For precise lag values,
/// issue a `ListOffsets` RPC externally.
fn compute_aggregate_lag(
    offsets: &HashMap<(String, PartitionId), Offset>,
    partition_state: &HashMap<(String, PartitionId), PartitionState>,
    undelivered: &HashMap<(String, PartitionId), Offset>,
    isolation_level: IsolationLevel,
) -> (u64, u64) {
    let mut total_lag: u64 = 0;
    let mut max_lag: u64 = 0;
    for (key, state) in partition_state {
        if let (Some(end), Some(&fetch_position)) =
            (state.readable_end_offset(isolation_level), offsets.get(key))
        {
            // Measure from the *delivered* position, not the fetch position:
            // records the consumer has read ahead into the buffer have not
            // reached the application yet, so they are still lag. Reporting the
            // fetch position would understate the backlog by whatever is parked
            // and make lag disagree with `position()` and with the commit.
            let position = match undelivered.get(key) {
                Some(&first_undelivered) => fetch_position.min(first_undelivered),
                None => fetch_position,
            };
            let partition_lag = (end - position).max(0) as u64;
            total_lag = total_lag.saturating_add(partition_lag);
            max_lag = max_lag.max(partition_lag);
        }
    }
    (total_lag, max_lag)
}

fn seed_initial_offsets_for_assigned(
    assigned: &HashMap<String, Vec<PartitionId>>,
    initial_offsets: &HashMap<(String, PartitionId), Offset>,
    stored_offsets: &mut HashMap<(String, PartitionId), Offset>,
) -> usize {
    let mut inserted = 0;
    for ((topic, partition), &initial) in initial_offsets {
        if !assigned
            .get(topic)
            .is_some_and(|partitions| partitions.contains(partition))
        {
            continue;
        }

        let key = (topic.clone(), *partition);
        if let std::collections::hash_map::Entry::Vacant(e) = stored_offsets.entry(key) {
            e.insert(initial);
            inserted += 1;
        }
    }
    inserted
}

/// Whether a control batch carries an **abort** marker (as opposed to a commit
/// marker, or a control record type this client does not know).
///
/// The control record's key is `[version: i16][type: i16]`, where type `0` is
/// ABORT and `1` is COMMIT (`ControlRecordType` in the Java client). The
/// `read_committed` filter deactivates a producer's aborted-transaction state
/// only on an abort marker; treating a commit marker as an end-of-abort would
/// let the data records of a still-aborted transaction through.
///
/// Returns `false` for a control batch whose key is missing or too short —
/// the conservative answer, since it leaves the filter engaged.
fn control_batch_is_abort(batch: &RecordBatch) -> bool {
    const CONTROL_TYPE_ABORT: i16 = 0;
    batch.records.first().is_some_and(|record| {
        record.key.as_ref().is_some_and(|key| {
            key.len() >= 4 && i16::from_be_bytes([key[2], key[3]]) == CONTROL_TYPE_ABORT
        })
    })
}

/// Claim up to `want` slots from a shared record-decode budget.
///
/// Returns how many were actually claimed (`0` when the budget is exhausted).
/// One CAS per *batch* rather than per record keeps this off the per-record hot
/// path while still bounding the total work several concurrent per-broker
/// fetches may do.
fn claim_record_budget(budget: &std::sync::atomic::AtomicUsize, want: usize) -> usize {
    use std::sync::atomic::Ordering;
    let mut granted = 0;
    let _ = budget.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |remaining| {
        granted = remaining.min(want);
        if granted == 0 {
            None
        } else {
            Some(remaining - granted)
        }
    });
    granted
}

/// What walking one partition's fetched batches produced, besides the records
/// themselves (those are appended to the caller's shared output vector).
struct PartitionDecodeOutcome {
    /// Highest offset the position advanced through: the last record pushed,
    /// or the end of the last batch that was skipped or drained in full.
    /// `None` when nothing in the payload could move the position.
    last_offset: Option<Offset>,
    /// Leader epoch of the batch `last_offset` came from, reported back on
    /// the next fetch so the broker can detect divergence (KIP-320).
    last_epoch: i32,
    /// The batch at the fetch position could not be decoded, so the partition
    /// cannot advance. The caller reports it as a partition-level fault.
    error: Option<KrafkaError>,
    /// A batch failed to decode for a reason other than the benign
    /// truncated-tail case; the caller records it on the metrics sink.
    corrupt: bool,
}

/// Advance a partition's pending position bookkeeping through `batch_end`.
///
/// Every batch-level advance — control batches, aborted transactions,
/// compaction-emptied batches, and batches drained to their last record —
/// goes through here so one place enforces that a fetch response can only
/// move the position *forward*: a batch lying entirely below the fetch
/// position is ignored (its offsets were delivered by earlier polls), and a
/// batch end at or below what this response already advanced through leaves
/// the bookkeeping untouched. Batches arrive in offset order, so both guards
/// are expected to be inert; they exist so a misbehaving broker degrades to a
/// visible stall instead of silently rewinding and re-delivering records.
fn advance_through_batch(
    last_offset: &mut Option<Offset>,
    last_epoch: &mut i32,
    fetch_offset: Offset,
    batch_end: Offset,
    batch_epoch: i32,
) {
    if batch_end < fetch_offset {
        return;
    }
    if last_offset.is_none_or(|advanced| batch_end > advanced) {
        *last_offset = Some(batch_end);
        *last_epoch = batch_epoch;
    }
}

/// Walk one partition's fetched record bytes, appending deliverable records
/// to `records` and tracking how far the partition's position may advance.
///
/// The walk is where every "this offset range carries nothing to deliver"
/// case is decided — control batches, aborted transactions, batches the log
/// cleaner emptied, batches whose surviving records all predate the fetch
/// offset — and each of those cases must still advance the position past the
/// range it covers, or the partition re-fetches the same bytes forever. It is
/// a free function, welded to no socket, precisely so those decisions can be
/// pinned down by unit tests feeding it hand-built batches.
///
/// `partition_fetch_offset` is the position the fetch was issued from;
/// records below it were delivered by an earlier poll and are skipped.
/// `record_budget`, when present, bounds how many records this call may
/// decode across all concurrent per-broker fetches.
#[allow(clippy::too_many_arguments)]
fn decode_partition_batches(
    topic_name: &str,
    partition: PartitionId,
    mut batch_buf: bytes::Bytes,
    partition_fetch_offset: Offset,
    mut aborted_txns: Vec<crate::protocol::AbortedTransaction>,
    record_budget: Option<&std::sync::atomic::AtomicUsize>,
    max_decompressed_size: usize,
    records: &mut Vec<ConsumerRecord>,
) -> PartitionDecodeOutcome {
    let mut outcome = PartitionDecodeOutcome {
        last_offset: None,
        last_epoch: -1,
        error: None,
        corrupt: false,
    };

    // For READ_COMMITTED, the broker still includes data batches from aborted
    // transactions in the FetchResponse but lists their (producer_id,
    // first_offset) pairs in `aborted_transactions` so the client can filter
    // them. Control batches (abort/commit markers) are filtered below either
    // way; this state machine handles the data records themselves.
    //
    // Sort by first_offset so entries can be activated in-order as the walk
    // reaches them.
    aborted_txns.sort_unstable_by_key(|at| at.first_offset);
    let mut aborted_txns_iter = aborted_txns.iter().peekable();
    // Producer IDs currently inside an open aborted transaction.
    let mut aborted_producers: HashSet<i64> = HashSet::new();

    // Decode fetched batches, bounded by the poll's remaining record budget.
    // The position advances only over records that are actually pushed and
    // batches that are walked in full, so a batch cut short by the budget is
    // re-fetched rather than skipped.
    let mut budget_exhausted = false;
    while batch_buf.len() >= 12 {
        // Re-check before each batch: a fetch running against another broker
        // may have consumed the shared budget since the last iteration, and
        // decoding a batch only to discard it is exactly the waste this
        // bounds.
        if record_budget.is_some_and(|b| b.load(std::sync::atomic::Ordering::Relaxed) == 0) {
            break;
        }
        match RecordBatch::decode_with_limit(&mut batch_buf, max_decompressed_size) {
            Ok(batch) => {
                let batch_epoch = batch.partition_leader_epoch;
                // The offset range this batch spans, whether or not any
                // record inside it survives to delivery. `last_offset_delta`
                // is preserved by the log cleaner even when the records it
                // once counted are gone, so this is the only trustworthy
                // measure of how far the batch reaches.
                let batch_end = batch
                    .base_offset
                    .saturating_add(batch.last_offset_delta as i64);

                // Advance the aborted-transaction state machine. Activate any
                // AbortedTransaction entries whose first_offset has been
                // reached. The list is sorted by first_offset so we only peek
                // at the front.
                while aborted_txns_iter
                    .peek()
                    .is_some_and(|at| at.first_offset <= batch.base_offset)
                {
                    if let Some(at) = aborted_txns_iter.next() {
                        aborted_producers.insert(at.producer_id);
                    }
                }

                // Skip transaction control batches (commit/abort markers).
                // These are internal Kafka bookkeeping records that must not
                // be surfaced to consumers.  When the control batch belongs
                // to a tracked aborted producer we also deactivate it so
                // subsequent transactions from the same producer are not
                // incorrectly filtered.  The offset must still be advanced
                // past them so that subsequent fetches do not re-process them.
                if batch.attributes.is_control_batch {
                    // Only an ABORT marker ends an aborted
                    // transaction. Clearing on any control
                    // batch (including COMMIT markers) meant
                    // the client trusted the marker *type* it
                    // never looked at, so a coordinator that
                    // wrote a commit marker for a producer the
                    // broker had listed as aborted would stop
                    // the filter early and surface aborted
                    // records to a `read_committed` consumer.
                    // Mirrors the Java client's
                    // `CompletedFetch.containsAbortMarker`.
                    if control_batch_is_abort(&batch) {
                        aborted_producers.remove(&batch.producer_id);
                    }
                    advance_through_batch(
                        &mut outcome.last_offset,
                        &mut outcome.last_epoch,
                        partition_fetch_offset,
                        batch_end,
                        batch_epoch,
                    );
                    continue;
                }

                // Skip data records from aborted transactions.
                // Once the abort marker (control batch) is seen,
                // the producer_id is removed from the set, so
                // later committed transactions from the same
                // producer are not affected.
                if batch.attributes.is_transactional
                    && aborted_producers.contains(&batch.producer_id)
                {
                    advance_through_batch(
                        &mut outcome.last_offset,
                        &mut outcome.last_epoch,
                        partition_fetch_offset,
                        batch_end,
                        batch_epoch,
                    );
                    continue;
                }

                // The log cleaner keeps a batch's header (to preserve the
                // producer's last sequence number for idempotence) even after
                // compaction has removed every record it originally held.
                // Such a batch carries nothing to deliver, but the offset
                // range it spans is real and must still be skipped —
                // otherwise the position never leaves it and every subsequent
                // fetch re-reads the same empty batch forever. Worse, the
                // budget claim below would read an empty batch as "budget
                // exhausted" and throw away every later batch in the response.
                if batch.records.is_empty() {
                    advance_through_batch(
                        &mut outcome.last_offset,
                        &mut outcome.last_epoch,
                        partition_fetch_offset,
                        batch_end,
                        batch_epoch,
                    );
                    continue;
                }

                // Claim decode slots for this batch up front:
                // one atomic operation per batch rather than
                // per record. Whatever is left over is returned
                // below, so records skipped as already-delivered
                // do not consume the caller's budget.
                let claimed = match record_budget {
                    Some(budget) => claim_record_budget(budget, batch.records.len()),
                    None => batch.records.len(),
                };
                if claimed == 0 {
                    // Raced with a concurrent fetch that took
                    // the last slot between the pre-check above
                    // and this claim.
                    break;
                }
                let mut used = 0usize;

                for record in batch.records.into_iter() {
                    // Use offset_delta for correct offset in compacted topics
                    // where records may have been deleted (log compaction awareness).
                    let record_offset =
                        batch.base_offset.saturating_add(record.offset_delta as i64);

                    // Skip records below the fetch offset — these were
                    // already delivered in a prior poll but are included
                    // because Kafka returns whole batches.
                    if record_offset < partition_fetch_offset {
                        continue;
                    }

                    if used == claimed {
                        budget_exhausted = true;
                        break;
                    }
                    used += 1;

                    records.push(ConsumerRecord {
                        topic: topic_name.to_string(),
                        partition,
                        offset: record_offset,
                        timestamp: batch.base_timestamp.saturating_add(record.timestamp_delta),
                        timestamp_type: batch.attributes.timestamp_type as i8,
                        key: record.key,
                        value: record.value,
                        headers: record
                            .headers
                            .into_iter()
                            .map(|h| (h.key, h.value))
                            .collect(),
                        leader_epoch: Some(batch_epoch),
                        delivery_count: None,
                    });
                    outcome.last_offset = Some(record_offset);
                    outcome.last_epoch = batch_epoch;
                }

                // Hand back slots claimed for records that were
                // skipped as already-delivered, or that the
                // budget cut short.
                if let Some(budget) = record_budget
                    && claimed > used
                {
                    budget.fetch_add(claimed - used, std::sync::atomic::Ordering::Relaxed);
                }
                if budget_exhausted {
                    // The batch was cut short mid-delivery: the position must
                    // stop at the last record pushed so the remainder is
                    // re-fetched, not skipped. Deliberately no batch-level
                    // advance here.
                    break;
                }

                // The batch was walked to its end, so the position moves to
                // the end of the *offset range* it spans — not merely past
                // its last surviving record. The two differ on compacted
                // topics: when the cleaner has removed a batch's trailing
                // records, `batch_end` reaches beyond the last record, and a
                // position parked between them re-fetches this same batch,
                // finds every record below the fetch offset, delivers
                // nothing, and never advances again. Mirrors the Java
                // client's `CompletedFetch.nextFetchOffset`, which jumps to
                // `batch.nextOffset()` once a batch is drained.
                advance_through_batch(
                    &mut outcome.last_offset,
                    &mut outcome.last_epoch,
                    partition_fetch_offset,
                    batch_end,
                    batch_epoch,
                );
            }
            Err(e) => {
                // Two very different situations arrive here.
                //
                // 1. The broker cut the response at
                //    `partition_max_bytes`, so the trailing
                //    batch is incomplete. Expected and benign:
                //    the prefix that did decode advances the
                //    position and the next fetch re-requests
                //    the rest.
                //
                // 2. The bytes are corrupt (CRC mismatch,
                //    unsupported magic, an out-of-range field).
                //    Silently breaking here used to hide this:
                //    when the *first* batch was the bad one,
                //    nothing decoded, no offset update was
                //    produced, and the partition stalled
                //    forever at that offset with the reason
                //    confined to a `debug!` line.
                //
                // A decode error that leaves the partition
                // unable to advance is therefore reported to
                // the caller. Whether it is truncation or
                // corruption, a partition that cannot get past
                // its current offset is a stall, and a stall
                // the application cannot see is worse than one
                // it can.
                let made_progress = outcome.last_offset.is_some();
                let truncated_tail =
                    e.protocol_error_kind() == Some(ProtocolErrorKind::TruncatedFrame);

                if made_progress && truncated_tail {
                    trace!(
                        topic = %topic_name,
                        partition,
                        "Trailing record batch truncated by the fetch size limit"
                    );
                    break;
                }

                outcome.corrupt = true;

                if made_progress {
                    // Deliver the good prefix, but do not let
                    // the corruption pass unremarked — the next
                    // fetch will start at the bad batch and
                    // surface it through the branch below.
                    warn!(
                        topic = %topic_name,
                        partition,
                        fetch_offset = partition_fetch_offset,
                        error = %e,
                        "Corrupt record batch after a decodable prefix; \
                         delivering the prefix and stopping here"
                    );
                    break;
                }

                // Nothing decoded: this partition is stuck at
                // `partition_fetch_offset`. Report it as a
                // partition-level fault so the caller can move on to the
                // next partition — failing the whole request
                // here would throw away records from every
                // healthy partition that shares this leader,
                // which is a far larger blast radius than the
                // fault deserves.
                warn!(
                    topic = %topic_name,
                    partition,
                    fetch_offset = partition_fetch_offset,
                    error = %e,
                    "Record batch at the fetch position could not be decoded; \
                     the partition cannot advance"
                );
                outcome.error = Some(e);
                break;
            }
        }
    }

    outcome
}

/// Drop every buffered record belonging to `repositioned`.
///
/// Any call that moves a partition's position from outside the fetch loop —
/// `seek()`, `seek_many()`, an `auto.offset.reset`, a broker-reported
/// truncation — makes the records already sitting in the receive buffer
/// describe a place in the log the consumer has deliberately left. Two things
/// go wrong if they are left there:
///
/// 1. `recv()` / `batch_recv()` drain the buffer before polling, so the
///    application receives records from *before* the seek it just asked for.
/// 2. Worse, [`committable_positions`] clamps a commit down to the lowest
///    still-buffered offset. After `seek_to_end()` the buffer holds offset 100
///    while the position is 5 000, so the next commit writes **100** — the
///    group moves backwards and re-delivers everything in between. After an
///    `auto.offset.reset` the clamped offset may not exist in the log at all,
///    which puts the partition into an `OffsetOutOfRange` reset loop.
///
/// Returns the number of records discarded, so callers can keep the
/// buffered-records gauge accurate.
fn purge_buffered_records(
    buffer: &mut std::collections::VecDeque<ConsumerRecord>,
    repositioned: &HashSet<(String, PartitionId)>,
) -> usize {
    if repositioned.is_empty() || buffer.is_empty() {
        return 0;
    }
    let before = buffer.len();
    buffer.retain(|r| !contains_partition(repositioned, &r.topic, r.partition));
    before - buffer.len()
}

/// Whether a `(topic, partition)` set contains this partition, without
/// allocating.
///
/// `HashSet<(String, PartitionId)>` cannot be probed with a borrowed name:
/// `Borrow` does not reach inside a tuple, so the obvious
/// `set.contains(&(topic.to_string(), partition))` allocates a `String` **per
/// record** — on `poll()`'s delivery split, on the stale-response filter and on
/// every buffer purge. These sets hold at most the assigned-partition count and
/// are empty in the common case, so a linear scan over borrowed names is both
/// allocation-free and, at these sizes, faster than hashing.
#[inline]
fn contains_partition(
    set: &HashSet<(String, PartitionId)>,
    topic: &str,
    partition: PartitionId,
) -> bool {
    !set.is_empty()
        && set
            .iter()
            .any(|(t, p)| *p == partition && t.as_str() == topic)
}

/// Forget what the consumer knew about the leader epoch at a partition's
/// position, and mark the position as needing validation.
///
/// Called whenever a position is set from outside the fetch loop — `seek()`,
/// `seek_many()`, an `auto.offset.reset`, or a broker-reported truncation.
/// The recorded epoch describes the *old* position and would be wrong if sent
/// with the new one: the broker would compare a `(position, epoch)` pair the
/// consumer never actually observed and could report a spurious divergence.
///
/// Creates the state entry when absent so a later fetch cannot mistake an
/// unvalidated position for a validated one.
fn invalidate_position_epoch(
    partition_state: &mut HashMap<(String, PartitionId), PartitionState>,
    key: &(String, PartitionId),
) {
    let entry = partition_state.entry(key.clone()).or_default();
    entry.last_fetched_epoch = None;
    entry.position_validated = false;
}

fn apply_seek_many_offsets(
    stored_offsets: &mut HashMap<(String, PartitionId), Offset>,
    offsets: &HashMap<(String, PartitionId), Offset>,
) -> usize {
    for ((topic, partition), offset) in offsets {
        stored_offsets.insert((topic.clone(), *partition), *offset);
    }
    offsets.len()
}

fn apply_assignment_offset_precedence(
    assigned: &HashMap<String, Vec<PartitionId>>,
    committed: &HashMap<(String, PartitionId), Offset>,
    initial_offsets: &HashMap<(String, PartitionId), Offset>,
    stored_offsets: &mut HashMap<(String, PartitionId), Offset>,
) -> Vec<(String, PartitionId)> {
    let mut need_reset: Vec<(String, PartitionId)> = Vec::new();

    for (topic, partitions) in assigned {
        for &partition in partitions {
            let key = (topic.clone(), partition);

            // Respect user-set offsets (e.g., from seek() in on_partitions_assigned).
            // If the caller already positioned this partition, do not overwrite.
            if stored_offsets.contains_key(&key) {
                continue;
            }

            if let Some(&offset) = committed.get(&key)
                && offset >= 0
            {
                stored_offsets.insert(key, offset);
                continue;
            }

            // No committed offset — try caller-supplied initial_offsets before
            // falling back to auto_offset_reset.
            if let Some(&initial) = initial_offsets.get(&key) {
                stored_offsets.insert(key, initial);
                continue;
            }

            need_reset.push(key);
        }
    }

    need_reset
}

/// Move up to `max_records - batch.len()` records out of `buffer` and into
/// `batch`, **skipping** any partition in `paused`.
///
/// Paused records are left in place rather than dropped: the fetch position has
/// already advanced past them, so discarding them would skip data outright, and
/// [`committable_positions`] relies on them still being there to hold the
/// committed offset back.
///
/// Without this, `pause()` was honoured by `poll()` (which filters its own
/// return value) but silently bypassed by `recv()` / `batch_recv()`, which
/// drained the buffer unconditionally — so the same client had two different
/// answers to "does pause stop delivery?".
fn drain_buffered_records(
    buffer: &mut std::collections::VecDeque<ConsumerRecord>,
    batch: &mut Vec<ConsumerRecord>,
    max_records: usize,
    paused: &HashSet<(String, PartitionId)>,
) {
    if paused.is_empty() {
        // Fast path: nothing is paused, so the queue drains from the front.
        while batch.len() < max_records {
            match buffer.pop_front() {
                Some(r) => batch.push(r),
                None => break,
            }
        }
        return;
    }

    let mut index = 0;
    while batch.len() < max_records && index < buffer.len() {
        let is_paused = {
            let record = &buffer[index];
            contains_partition(paused, &record.topic, record.partition)
        };
        if is_paused {
            index += 1;
            continue;
        }
        if let Some(record) = buffer.remove(index) {
            batch.push(record);
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn batch_recv_with<FClosed, FPoll, FPollFut, FSetBuffered, FPaused, FPausedFut>(
    recv_buffer: &SyncMutex<std::collections::VecDeque<ConsumerRecord>>,
    mut set_buffered_records: FSetBuffered,
    max_records: usize,
    timeout: Duration,
    max_idle_backoff: Duration,
    is_closed: FClosed,
    mut paused_snapshot: FPaused,
    mut poll: FPoll,
) -> Result<BatchRecvOutcome>
where
    FClosed: Fn() -> bool,
    FPoll: FnMut(Duration) -> FPollFut,
    FPollFut: Future<Output = Result<Vec<ConsumerRecord>>>,
    FSetBuffered: FnMut(u64),
    FPaused: FnMut() -> FPausedFut,
    FPausedFut: Future<Output = HashSet<(String, PartitionId)>>,
{
    if max_records == 0 {
        return Ok(BatchRecvOutcome::EmptyRequest);
    }

    let deadline = tokio::time::Instant::now() + timeout;
    let mut batch = Vec::with_capacity(max_records);

    loop {
        // Re-read the paused set every round so a `pause()` issued while this
        // call is parked takes effect on the next drain rather than after it.
        // The guard is released before the buffer's sync lock is taken.
        let paused = paused_snapshot().await;

        // Drain buffer first.
        {
            let mut buffer = recv_buffer.lock();
            drain_buffered_records(&mut buffer, &mut batch, max_records, &paused);
            set_buffered_records(buffer.len() as u64);
        }

        if batch.len() >= max_records {
            return Ok(BatchRecvOutcome::Records(batch));
        }

        if is_closed() {
            return if batch.is_empty() {
                Ok(BatchRecvOutcome::Closed)
            } else {
                Ok(BatchRecvOutcome::Records(batch))
            };
        }

        // Compute remaining budget.
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return if batch.is_empty() {
                Ok(BatchRecvOutcome::TimedOut)
            } else {
                Ok(BatchRecvOutcome::Records(batch))
            };
        }
        let remaining = deadline - now;

        match tokio::time::timeout(remaining, poll(remaining)).await {
            Ok(Ok(records)) => {
                let before_len = batch.len();
                let mut iter = records.into_iter();
                while batch.len() < max_records {
                    match iter.next() {
                        Some(r) => batch.push(r),
                        None => break,
                    }
                }

                // Put overflow back into the recv buffer.
                let leftover: Vec<_> = iter.collect();
                if !leftover.is_empty() {
                    let mut buffer = recv_buffer.lock();
                    for r in leftover.into_iter().rev() {
                        buffer.push_front(r);
                    }
                    set_buffered_records(buffer.len() as u64);
                }

                if batch.len() >= max_records {
                    return Ok(BatchRecvOutcome::Records(batch));
                }

                // Avoid a tight busy loop when poll() returns quickly with no records
                // (e.g., no assignment, rebalance, or buffer cap).
                if batch.len() == before_len {
                    let now_after_poll = tokio::time::Instant::now();
                    if now_after_poll >= deadline {
                        return if batch.is_empty() {
                            Ok(BatchRecvOutcome::TimedOut)
                        } else {
                            Ok(BatchRecvOutcome::Records(batch))
                        };
                    }
                    let remaining_after_poll = deadline - now_after_poll;
                    let backoff = remaining_after_poll.min(max_idle_backoff);
                    tokio::time::sleep(backoff).await;
                }
            }
            Ok(Err(_)) if is_closed() => {
                return if batch.is_empty() {
                    Ok(BatchRecvOutcome::Closed)
                } else {
                    Ok(BatchRecvOutcome::Records(batch))
                };
            }
            Ok(Err(e)) => {
                // Preserve delivery semantics: if we already drained buffered
                // records before this poll error, put them back so callers
                // can retry without data loss.
                if !batch.is_empty() {
                    let mut buffer = recv_buffer.lock();
                    for record in batch.into_iter().rev() {
                        buffer.push_front(record);
                    }
                    set_buffered_records(buffer.len() as u64);
                }
                return Err(e);
            }
            Err(_elapsed) => {
                return if batch.is_empty() {
                    Ok(BatchRecvOutcome::TimedOut)
                } else {
                    Ok(BatchRecvOutcome::Records(batch))
                };
            }
        }
    }
}

/// Result of routing assigned partitions to brokers for fetching.
struct FetchRoutingPlan {
    /// Partitions grouped by target broker ID.
    partitions_by_broker: HashMap<crate::BrokerId, Vec<(String, PartitionId)>>,
    /// Preferred replica entries that have expired and should be removed.
    expired_preferred: Vec<(String, PartitionId)>,
    /// Partitions that have neither a known leader nor a valid preferred
    /// replica and will not be fetched this round.
    skipped: Vec<(String, PartitionId)>,
}

/// Build a per-broker fetch plan from pre-filtered partition keys,
/// preferred replicas, and leader information.
///
/// `non_paused_keys` should contain only assigned, non-paused partitions
/// (the caller is responsible for filtering). For each key the function
/// checks whether a preferred replica exists and is not expired. If so,
/// the partition is routed to that replica, regardless of whether a leader
/// is known. If there is no valid preferred replica, the leader from
/// `leaders` is used; otherwise the partition is skipped.
///
/// `leaders` is read from the metadata cache, which is also where a
/// broker-reported leader (KIP-951) lands, so a failover the broker announced
/// in a fetch response is already reflected here.
///
/// This is a pure function extracted from `Consumer::poll()` so that the
/// routing logic can be unit-tested without a live broker.
fn build_fetch_routing_plan(
    non_paused_keys: Vec<(String, PartitionId)>,
    partition_state: &HashMap<(String, PartitionId), PartitionState>,
    leaders: &HashMap<(String, PartitionId), crate::BrokerId>,
    now: Instant,
) -> FetchRoutingPlan {
    let mut partitions_by_broker: HashMap<crate::BrokerId, Vec<(String, PartitionId)>> =
        HashMap::new();
    let mut expired_preferred: Vec<(String, PartitionId)> = Vec::new();
    let mut skipped: Vec<(String, PartitionId)> = Vec::new();

    for key in non_paused_keys {
        // Check for a valid (non-expired) preferred replica
        let target_broker = match partition_state.get(&key).and_then(|s| s.preferred_replica) {
            Some((replica_id, expiry)) if now < expiry => Some(replica_id),
            Some(_) => {
                expired_preferred.push(key.clone());
                None
            }
            None => None,
        };

        let broker_id = match target_broker {
            Some(id) => id,
            None => match leaders.get(&key).copied() {
                Some(leader_id) => leader_id,
                None => {
                    skipped.push(key);
                    continue;
                }
            },
        };

        partitions_by_broker.entry(broker_id).or_default().push(key);
    }

    FetchRoutingPlan {
        partitions_by_broker,
        expired_preferred,
        skipped,
    }
}

/// Group a flat `&[(&str, PartitionId)]` slice into a
/// `HashMap<String, Vec<PartitionId>>` keyed by topic name, preserving
/// insertion order within each topic's partition list.
///
/// This is the pure grouping step shared by [`Consumer::offsets_for_times`]
/// and its unit test so that the test exercises the real logic.
fn group_topic_partitions(partitions: &[(&str, PartitionId)]) -> HashMap<String, Vec<PartitionId>> {
    let mut grouped: HashMap<String, Vec<PartitionId>> = HashMap::new();
    for &(topic, partition) in partitions {
        // Use get_mut to avoid allocating a String key on every lookup;
        // only insert (and therefore allocate) when the topic is first seen.
        if let Some(v) = grouped.get_mut(topic) {
            v.push(partition);
        } else {
            grouped.insert(topic.to_string(), vec![partition]);
        }
    }
    grouped
}

/// Apply a [`ListOffsetsResponse`] into a per-partition result map.
///
/// For each partition in the response:
/// - `error_code == None` → inserts `Ok(offset)`.
/// - any other error code → inserts `Err(KrafkaError::broker(...))` and logs
///   a `warn!`.
///
/// Partitions not mentioned in the response are left unchanged, so the
/// pre-populated `Err("no leader found …")` sentinel that `resolve_list_offsets`
/// inserts before calling this function is preserved for any partition the
/// broker did not report.
/// Fold a `ListOffsets` response into `result`, returning the topics whose
/// partitions were rejected because this client's leader epoch is stale.
///
/// Those topics need a metadata refresh before a retry can succeed; every
/// other error is left to the caller's ordinary backoff.
fn apply_list_offsets_response(
    response: &ListOffsetsResponse,
    result: &mut HashMap<(String, PartitionId), Result<Offset>>,
) -> Vec<String> {
    let mut stale_epoch_topics: Vec<String> = Vec::new();
    for topic_resp in &response.topics {
        for part_resp in &topic_resp.partitions {
            let key = (topic_resp.name.clone(), part_resp.partition_index);
            if part_resp.error_code.is_ok() {
                result.insert(key, Ok(part_resp.offset));
            } else {
                warn!(
                    "ListOffsets error for {}-{}: {:?}",
                    topic_resp.name, part_resp.partition_index, part_resp.error_code
                );
                if matches!(
                    part_resp.error_code,
                    crate::error::ErrorCode::FencedLeaderEpoch
                        | crate::error::ErrorCode::UnknownLeaderEpoch
                ) && !stale_epoch_topics.contains(&topic_resp.name)
                {
                    stale_epoch_topics.push(topic_resp.name.clone());
                }
                result.insert(
                    key,
                    Err(KrafkaError::broker(
                        part_resp.error_code,
                        format!(
                            "ListOffsets error for {}-{}",
                            topic_resp.name, part_resp.partition_index
                        ),
                    )),
                );
            }
        }
    }
    stale_epoch_topics
}

/// Compute revoked partitions as `old - new`.
///
/// Used by eager rebalance cleanup to preserve local state for partitions that
/// remain assigned to the same consumer after the rebalance completes.
fn revoked_partitions_diff(
    old: &HashMap<String, Vec<PartitionId>>,
    new: &HashMap<String, Vec<PartitionId>>,
) -> Vec<TopicPartition> {
    let new_sets: HashMap<&String, HashSet<PartitionId>> = new
        .iter()
        .map(|(topic, partitions)| (topic, partitions.iter().copied().collect()))
        .collect();
    let mut result = Vec::new();
    for (topic, partitions) in old {
        let new_set = new_sets.get(topic);
        for &partition in partitions {
            let gone = new_set.is_none_or(|assigned| !assigned.contains(&partition));
            if gone {
                result.push(TopicPartition::new(topic, partition));
            }
        }
    }
    result
}

impl Consumer {
    /// Create a new consumer builder.
    pub fn builder() -> ConsumerBuilder {
        ConsumerBuilder::default()
    }

    /// Create a new consumer with the given configuration.
    async fn new(
        config: ConsumerConfig,
        shared: Option<(Arc<ConnectionPool>, Arc<ClusterMetadata>)>,
    ) -> Result<Self> {
        let pool_owned = shared.is_none();
        let (pool, metadata) = if let Some((pool, metadata)) = shared {
            // Use the pre-built shared pool and metadata from a KrafkaClient.
            (pool, metadata)
        } else {
            let mut pool_config_builder = config.transport.apply(
                ConnectionConfig::builder()
                    .client_id(&config.client_id)
                    .request_timeout(config.request_timeout)
                    .connect_timeout(config.connect_timeout),
            );

            if let Some(ref auth) = config.auth {
                pool_config_builder = pool_config_builder.auth(auth.clone());
            }

            let mut pool_config = pool_config_builder.build()?;
            pool_config.init_tls().await?;

            // Every client builds its pool through `TransportConfig::build_pool`,
            // which applies the pool-level settings and starts the background
            // tasks (idle eviction, OAUTHBEARER refresh, KIP-1288 TLS reload).
            // Routing all construction sites through one function is what stops
            // them drifting apart again.
            let pool = config.transport.build_pool(pool_config);

            let bootstrap_servers =
                crate::util::parse_bootstrap_servers(&config.bootstrap_servers)?;

            let metadata = Arc::new({
                let mut meta =
                    ClusterMetadata::new(bootstrap_servers, pool.clone(), config.metadata_max_age)
                        .with_recovery_strategy(config.metadata_recovery_strategy)
                        .with_rebootstrap_trigger(config.metadata_recovery_rebootstrap_trigger);
                if let Some(ttl) = config.metadata_topic_cache_ttl {
                    meta = meta.with_topic_cache_ttl(ttl);
                } else {
                    meta = meta.with_topic_cache_ttl_disabled();
                }
                meta
            });

            // Initial metadata fetch
            metadata.refresh().await?;

            (pool, metadata)
        };

        // Create group coordinator if group_id is specified
        let group_coordinator = if let Some(ref group_id) = config.group_id {
            Some(Arc::new(
                GroupCoordinator::new(
                    group_id.clone(),
                    pool.clone(),
                    metadata.clone(),
                    config.session_timeout,
                    config.heartbeat_interval,
                    config.max_poll_interval, // rebalance_timeout matches Java client's max.poll.interval.ms
                )
                .with_assignor_strategies(config.partition_assignment_strategies.clone())
                .with_group_instance_id(config.group_instance_id.clone())
                .with_client_rack(config.client_rack.clone())
                .with_isolation_level(config.isolation_level.to_i8())
                .with_group_protocol(config.group_protocol),
            ))
        } else {
            None
        };

        let metrics = Arc::new(ConsumerMetrics::default());

        info!(
            "Consumer initialized with {} brokers{}",
            metadata.brokers().len(),
            if let Some(ref gid) = config.group_id {
                format!(", group_id='{gid}'")
            } else {
                String::new()
            }
        );

        Ok(Self {
            config,
            metadata,
            pool,
            pool_owned,
            subscriptions: LeveledRwLock::new(HashSet::new()),
            assignments: LeveledRwLock::new(HashMap::new()),
            offsets: LeveledRwLock::new(HashMap::new()),
            paused: LeveledRwLock::new(HashSet::new()),
            closed: std::sync::atomic::AtomicBool::new(false),
            wakeup_flag: std::sync::atomic::AtomicBool::new(false),
            wakeup_notify: tokio::sync::Notify::new(),
            group_coordinator,
            metrics,
            rebalance_listener: Arc::new(NoOpRebalanceListener),
            interceptor: Arc::new(crate::interceptor::NoOpConsumerInterceptor),
            last_auto_commit: SyncMutex::new(Instant::now()),
            recv_buffer: SyncMutex::new(std::collections::VecDeque::new()),
            fetch_rotation: std::sync::atomic::AtomicUsize::new(0),
            fetch_sessions: SyncMutex::new(FetchSessionCache::new()),
            partition_state: LeveledRwLock::new(HashMap::new()),
            key_deserializer: None,
            value_deserializer: None,
        })
    }

    /// Subscribe to topics.
    ///
    /// Replaces the current subscription with the given topics (matching
    /// the Kafka Java client's replace semantics).
    ///
    /// # How long this can block
    ///
    /// Only the eager (non-cooperative) classic protocol joins here; the
    /// cooperative and KIP-848 paths just record the subscription and let the
    /// next `poll()` do the work.
    ///
    /// Joining is not bounded by `request.timeout.ms`. The coordinator answers
    /// `JoinGroup` only once every member of the group has sent one, so the
    /// call is budgeted against the group's rebalance window
    /// (`max.poll.interval.ms` plus a small margin), matching the Java client.
    ///
    /// In practice it returns as soon as the other members respond. A krafka
    /// consumer rejoins from its background heartbeat task the moment the
    /// coordinator reports a rebalance, so an idle application between `poll()`
    /// calls no longer holds the group up. The full rebalance window is only
    /// reached when some member genuinely cannot answer — a client that drives
    /// `JoinGroup` from its application thread and has stopped polling, or one
    /// whose process has died and whose session has yet to expire.
    pub async fn subscribe(&self, topics: &[&str]) -> Result<()> {
        // H6: reject empty / oversize topic names at ingress so they cannot
        // reach the panicking `KafkaString::encode` path via the MetadataRequest
        // / Heartbeat / subscription payload.
        validate_topic_names(topics.iter().copied())?;

        // Re-arm poll-interval tracking. Subscribing is the deliberate restart
        // of consumption, so it clears a previous max.poll.interval.ms
        // expiry and gives an application that recovered from a stall a way
        // back into the group.
        if let Some(ref coordinator) = self.group_coordinator {
            coordinator.reset_poll_tracking();
        }

        // Scope the write lock so it is dropped before network I/O
        {
            let mut subscriptions = self.subscriptions.write().await;
            subscriptions.clear();
            for topic in topics {
                subscriptions.insert((*topic).to_string());
            }
        }

        // Refresh metadata for subscribed topics
        self.metadata.refresh_for_topics(Some(topics)).await?;

        // If we have a group coordinator, join the group
        if let Some(ref coordinator) = self.group_coordinator {
            let mut topics_sorted: Vec<String> = topics.iter().map(|s| s.to_string()).collect();
            topics_sorted.sort();

            if coordinator.is_consumer_protocol() {
                // KIP-848: defer to poll(), which handles incremental
                // assignment via the background heartbeat task.  subscribe()
                // only updates the subscription; the next heartbeat will carry
                // the new topic list to the coordinator.

                // Detect topic changes while active — trigger rejoin so the
                // next poll sends a full heartbeat with the new subscription.
                {
                    let state = coordinator.state().await;
                    if state == GroupState::Stable {
                        let mut old_sorted = coordinator.subscribed_topics().await;
                        old_sorted.sort();
                        if old_sorted != topics_sorted {
                            coordinator.trigger_rejoin().await;
                        }
                    }
                }

                coordinator.set_subscribed_topics(topics_sorted).await;
            } else if coordinator.is_cooperative() {
                // Cooperative (KIP-429): defer the join/sync to poll(), which
                // implements the full two-phase rebalance protocol (revocations,
                // on_partitions_revoked callback, second rejoin). subscribe()
                // only updates the subscription metadata; poll() will detect
                // needs_rejoin() and drive the cooperative flow.

                // Detect topic changes while Stable — mark for rejoin.
                {
                    let state = coordinator.state().await;
                    if state == GroupState::Stable {
                        let mut old_sorted = coordinator.subscribed_topics().await;
                        old_sorted.sort();
                        if old_sorted != topics_sorted {
                            coordinator.set_preparing_rebalance().await;
                        }
                    }
                }

                coordinator.set_subscribed_topics(topics_sorted).await;
            } else {
                // Eager: join immediately in subscribe() — single-phase is correct.

                // Snapshot old assignment before the join. If a JoinGroup/SyncGroup
                // occurs, we must revoke the old partitions (eager = revoke all)
                // to clean up per-partition state and notify the listener.
                let old_assignments = self.assignments.read().await.clone();

                let (assignment, joined) =
                    coordinator.ensure_active_membership(&topics_sorted).await?;

                if joined {
                    // An actual JoinGroup/SyncGroup occurred (first join or topic change).

                    // Eager revocation: notify the listener for the full previous
                    // assignment, but only clean up partitions that were actually
                    // revoked by the new assignment so retained partitions keep
                    // their local pause/offset/fetch state.
                    if !old_assignments.is_empty() {
                        let revoked: Vec<TopicPartition> = old_assignments
                            .iter()
                            .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
                            .collect();
                        self.safe_on_partitions_revoked(&revoked).await;

                        let revoked_tuples: Vec<(String, PartitionId)> =
                            revoked_partitions_diff(&old_assignments, &assignment.partitions)
                                .into_iter()
                                .map(|tp| (tp.topic, tp.partition))
                                .collect();
                        self.apply_partition_revocations(&revoked_tuples).await;
                    }

                    self.metrics.rebalances.inc();
                }

                // Update our assignments based on the group assignment
                {
                    let mut assignments = self.assignments.write().await;
                    assignments.clear();
                    for (topic, partitions) in &assignment.partitions {
                        assignments.insert(topic.clone(), partitions.clone());
                    }
                }

                if joined {
                    // Notify listener of assignment (matches Java client behavior:
                    // ConsumerRebalanceListener.onPartitionsAssigned is invoked on every
                    // successful rebalance, including the very first one).
                    let assigned: Vec<TopicPartition> = assignment
                        .partitions
                        .iter()
                        .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
                        .collect();
                    self.safe_on_partitions_assigned(&assigned).await;

                    // Update assigned_partitions metric
                    self.metrics.assigned_partitions.set(assigned.len() as u64);

                    // Fetch committed offsets for our assigned partitions
                    self.fetch_and_apply_committed_offsets(&assignment.partitions)
                        .await?;
                }
            }

            debug!("Subscribed to topics via group coordinator: {:?}", topics);
        } else {
            // Assign all partitions (simple assignment without group coordination)
            let mut assignments = self.assignments.write().await;
            for topic in topics {
                if let Some(topic_info) = self.metadata.topic(topic) {
                    let partitions: Vec<_> = topic_info
                        .partitions
                        .values()
                        .map(|p| p.partition)
                        .collect();
                    assignments.insert((*topic).to_string(), partitions);
                }
            }
            let assigned_snapshot = assignments.clone();
            drop(assignments);

            // Update metric for standalone partition count
            let count: usize = assigned_snapshot.values().map(|p| p.len()).sum();
            self.metrics.assigned_partitions.set(count as u64);

            // Apply auto_offset_reset for non-group consumers.
            // Without this, all partitions default to offset 0 regardless of
            // the configured auto_offset_reset policy.
            self.apply_auto_offset_reset(&assigned_snapshot).await?;

            debug!("Subscribed to topics: {:?}", topics);
        }

        Ok(())
    }

    /// Apply per-partition cleanup for revoked partitions.
    ///
    /// Removes revoked entries from `assignments`, `offsets`, `paused`,
    /// `recv_buffer`, and `partition_state` (the consolidated cache that
    /// holds high watermark, log start offset, preferred replica, and
    /// offset-retry backoff). Fetch sessions are NOT reset here —
    /// `build_request()` automatically computes `forgotten_topics` diffs
    /// from the updated assignment, preserving KIP-227 incremental fetch
    /// benefits. Called by all cooperative revocation paths.
    async fn apply_partition_revocations(&self, revoked: &[(String, PartitionId)]) {
        // Build per-topic set of revoked partition IDs for O(T * P) removal
        // instead of O(R * P) when many partitions of the same topic are revoked.
        let revoked_by_topic: HashMap<&str, HashSet<PartitionId>> = {
            let mut m: HashMap<&str, HashSet<PartitionId>> = HashMap::new();
            for (topic, partition) in revoked {
                m.entry(topic.as_str()).or_default().insert(*partition);
            }
            m
        };

        // Precompute owned keys once to avoid repeated String clones in each
        // removal loop below.
        let revoked_keys: Vec<(String, PartitionId)> =
            revoked.iter().map(|(t, p)| (t.clone(), *p)).collect();

        // Remove from assignments
        {
            let mut assignments = self.assignments.write().await;
            for (topic, revoked_parts) in &revoked_by_topic {
                if let Some(parts) = assignments.get_mut(*topic) {
                    parts.retain(|p| !revoked_parts.contains(p));
                    if parts.is_empty() {
                        assignments.remove(*topic);
                    }
                }
            }
        }
        // Remove offsets for revoked partitions
        {
            let mut offsets = self.offsets.write().await;
            for key in &revoked_keys {
                offsets.remove(key);
            }
        }
        // Discard buffered records from revoked partitions
        {
            let revoked_set: HashSet<(&str, PartitionId)> =
                revoked_keys.iter().map(|(t, p)| (t.as_str(), *p)).collect();
            let mut buf = self.recv_buffer.lock();
            buf.retain(|r| !revoked_set.contains(&(r.topic.as_str(), r.partition)));
            self.metrics.buffered_records.set(buf.len() as u64);
        }
        // Clear paused state for revoked partitions
        {
            let mut paused = self.paused.write().await;
            for key in &revoked_keys {
                paused.remove(key);
            }
            self.metrics.paused_partitions.set(paused.len() as u64);
        }
        // Clear all per-partition fetch-derived state (high watermark, log
        // start offset, preferred replica, offset-retry backoff) in a single
        // lock acquisition. This replaces four independent `RwLock` writes
        // that previously had to be kept in sync by hand.
        {
            let mut partition_state = self.partition_state.write().await;
            for key in &revoked_keys {
                partition_state.remove(key);
            }
        }
        // Evict fetch sessions for brokers no longer present in cluster
        // metadata. Sessions for departed brokers can never become active
        // again; evicting them prevents unbounded map growth on clusters
        // with high broker churn.
        {
            let live_broker_ids: Vec<crate::BrokerId> =
                self.metadata.brokers().iter().map(|b| b.id()).collect();
            self.fetch_sessions.lock().retain_brokers(&live_broker_ids);
        }
        // Recompute lag metrics from remaining caches so revoked
        // partitions no longer contribute to exported values.
        self.recompute_lag_metrics().await;
    }

    /// Finalize a cooperative rebalance: compute newly-assigned diff, update
    /// assignments, fire `on_partitions_assigned`, fetch committed offsets for
    /// new partitions, and record owned partitions in the sticky assignor.
    async fn finalize_cooperative_assignment(
        &self,
        coordinator: &GroupCoordinator,
        assignment: &MemberAssignment,
        old_assignments: &HashMap<String, Vec<PartitionId>>,
    ) -> Result<()> {
        // Build HashSet index for O(1) membership checks.
        let old_sets: HashMap<&String, HashSet<PartitionId>> = old_assignments
            .iter()
            .map(|(t, ps)| (t, ps.iter().copied().collect()))
            .collect();

        // Determine newly assigned partitions (new - old)
        let mut newly_assigned = Vec::new();
        for (topic, partitions) in &assignment.partitions {
            let old_set = old_sets.get(topic);
            for &p in partitions {
                let is_new = old_set.is_none_or(|os| !os.contains(&p));
                if is_new {
                    newly_assigned.push(TopicPartition::new(topic, p));
                }
            }
        }

        // Update to final assignment
        {
            let mut assignments = self.assignments.write().await;
            assignments.clear();
            for (topic, partitions) in &assignment.partitions {
                assignments.insert(topic.clone(), partitions.clone());
            }
        }

        // Notify listener with only the *newly* assigned partitions (delta),
        // consistent with the KIP-848 and cooperative-sticky paths.
        // Listeners must not assume the slice contains all owned partitions;
        // they should consult the assignment API for the full view.
        self.safe_on_partitions_assigned(&newly_assigned).await;
        let total_assigned: usize = assignment.partitions.values().map(|ps| ps.len()).sum();
        self.metrics.assigned_partitions.set(total_assigned as u64);

        // Fetch committed offsets for newly assigned partitions only
        // (retained partitions already have tracked offsets).
        if !newly_assigned.is_empty() {
            let new_parts = Self::group_partitions_by_topic(&newly_assigned);
            self.fetch_and_apply_committed_offsets(&new_parts).await?;
        }

        // Record final assignment so the next rebalance's
        // join_group metadata reports correct owned partitions.
        let member_id = coordinator.member_id().await;
        coordinator.record_owned_partitions(&member_id, assignment);

        Ok(())
    }

    /// Clear all per-partition state after an eager revocation or unsubscribe/close.
    ///
    /// Resets fetch sessions, offsets, buffered records, paused set, and the
    /// consolidated [`PartitionState`] map (high watermark, log start offset,
    /// preferred replica, offset-retry backoff), then zeros the lag metrics.
    async fn clear_partition_state(&self) {
        self.close_fetch_sessions().await;
        self.offsets.write().await.clear();
        self.recv_buffer.lock().clear();
        self.paused.write().await.clear();
        self.partition_state.write().await.clear();
        self.metrics.buffered_records.set(0);
        self.metrics.paused_partitions.set(0);
        self.metrics.lag.set(0);
        self.metrics.lag_max.set(0);
    }

    /// Tell every broker we are done with its incremental fetch session.
    ///
    /// A fetch session is server-side state: the broker remembers the exact
    /// set of partitions and offsets this client last asked for, so that
    /// subsequent requests only need to carry the differences. Abandoning a
    /// session without saying so leaves that state pinned until the broker's
    /// LRU eventually evicts it. Brokers cap the number of sessions they will
    /// keep, so a consumer that rebalances repeatedly can occupy slot after
    /// slot and push other clients off the cache entirely — they then fall
    /// back to full fetches and start seeing `FETCH_SESSION_ID_NOT_FOUND`.
    ///
    /// Sending the final epoch releases the slot immediately. Failures are
    /// logged and ignored: the local state is cleared regardless, and an
    /// unreleased session is a resource-usage problem rather than a
    /// correctness one.
    async fn close_fetch_sessions(&self) {
        // `close_all` clears local state and returns the sessions that were
        // actually established. The sync guard must be released before any
        // await.
        let closes = self.fetch_sessions.lock().close_all();

        for close in closes {
            let Some(broker) = self.metadata.broker(close.broker_id) else {
                continue;
            };
            let Ok(conn) = self
                .pool
                .get_connection_by_id(close.broker_id, broker.address())
                .await
            else {
                continue;
            };
            // Fetch sessions only exist from v7 onward.
            let Some(version) = conn.negotiate_api_version(ApiKey::Fetch, versions::FETCH_MAX, 7)
            else {
                continue;
            };

            let request = FetchRequest {
                replica_id: -1,
                max_wait_ms: 0,
                min_bytes: 0,
                max_bytes: 0,
                isolation_level: self.config.isolation_level.to_i8(),
                session_id: close.session_id,
                session_epoch: close.session_epoch,
                topics: Vec::new(),
                forgotten_topics: Vec::new(),
                rack_id: self.config.client_rack.clone().unwrap_or_default(),
            };

            if let Err(e) = conn
                .send_request(ApiKey::Fetch, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await
            {
                debug!(
                    broker_id = close.broker_id,
                    session_id = close.session_id,
                    "Failed to close fetch session: {e}"
                );
            }
        }
    }

    // ── Rebalance listener wrappers ──────────────────────────────────────
    //
    // Callbacks are awaited directly on the consumer's rebalance task; the
    // consumer blocks rebalance progress until each future resolves.  Panics
    // inside callbacks propagate to the task — keep them panic-free or handle
    // errors internally.

    /// Await `on_partitions_assigned` on the rebalance listener.
    async fn safe_on_partitions_assigned(&self, partitions: &[TopicPartition]) {
        self.rebalance_listener
            .on_partitions_assigned_erased(partitions)
            .await;
    }

    /// Await `on_partitions_revoked` on the rebalance listener.
    ///
    /// The callback is bounded by [`ConsumerConfig::revocation_timeout`].
    /// If it does not complete in time, a warning is logged and the consumer
    /// proceeds with the rebalance to avoid group coordinator session expiry.
    async fn safe_on_partitions_revoked(&self, partitions: &[TopicPartition]) {
        let timeout = self.config.revocation_timeout();
        if tokio::time::timeout(
            timeout,
            self.rebalance_listener
                .on_partitions_revoked_erased(partitions),
        )
        .await
        .is_err()
        {
            warn!(
                timeout_secs = timeout.as_secs_f64(),
                "on_partitions_revoked timed out; proceeding with revocation. \
                 A hung rebalance listener can cause group coordinator session expiry."
            );
        }
    }

    /// Await `on_partitions_lost` on the rebalance listener.
    async fn safe_on_partitions_lost(&self, partitions: &[TopicPartition]) {
        self.rebalance_listener
            .on_partitions_lost_erased(partitions)
            .await;
    }

    /// Recompute lag and lag_max gauges from cached offsets and high watermarks.
    ///
    /// Call after any mutation of `self.offsets` or the `high_watermark` field
    /// of `self.partition_state` so the exported metrics always reflect the
    /// current consumer position. Acquires read locks in documented order:
    /// offsets → partition_state.
    ///
    /// This performs an O(partitions) full scan via [`compute_aggregate_lag`].
    /// An incremental (delta-based) approach was considered but rejected:
    /// the typical partition count per consumer (tens to low thousands) makes
    /// the scan complete in microseconds, while incremental bookkeeping would
    /// add complexity and drift risk for negligible gain. Callers on the hot
    /// path (e.g. `poll()`) already guard calls behind a change-detection flag.
    async fn recompute_lag_metrics(&self) {
        let offsets = self.offsets.read().await;
        let partition_state = self.partition_state.read().await;
        let undelivered = lowest_undelivered_offsets(&self.recv_buffer.lock());
        let (total_lag, max_lag) = compute_aggregate_lag(
            &offsets,
            &partition_state,
            &undelivered,
            self.config.isolation_level,
        );
        self.metrics.lag.set(total_lag);
        self.metrics.lag_max.set(max_lag);
    }

    /// Send an inline heartbeat, invoke the revocation callback, apply
    /// partition revocations, and update the metric + sticky-assignor state.
    ///
    /// Returns `true` if an inline heartbeat signalled session invalidation
    /// and poll() should return early.
    async fn apply_revocation_round(
        &self,
        coordinator: &Arc<GroupCoordinator>,
        revoked_tuples: &[(String, PartitionId)],
        revoked_tps: &[TopicPartition],
    ) -> Result<bool> {
        // Send an inline heartbeat before invoking the user callback
        // to avoid session timeout if the callback is slow.
        match coordinator.send_heartbeat().await {
            Ok(status) if coordinator.handle_inline_heartbeat_status(status).await => {
                return Ok(true);
            }
            Err(e) => {
                warn!("Pre-revocation heartbeat failed: {}", e);
            }
            _ => {}
        }
        // Commit offsets for the partitions we are about to lose so the
        // new owner sees up-to-date committed positions.
        if self.config.enable_auto_commit
            && let Err(e) = self.commit().await
        {
            if e.is_retriable() {
                warn!(
                    "Auto-commit before cooperative revocation failed (retriable): {}",
                    e
                );
            } else {
                error!(
                    "Auto-commit before cooperative revocation failed (fatal): {}",
                    e
                );
            }
        }
        self.safe_on_partitions_revoked(revoked_tps).await;
        self.apply_partition_revocations(revoked_tuples).await;

        // Update metric and owned-partition state in a single lock
        // acquisition. The metric is set eagerly so it stays accurate
        // even if a subsequent Phase 2 round returns early.
        let member_id = coordinator.member_id().await;
        let current = self.assignments.read().await;
        let count: usize = current.values().map(|ps| ps.len()).sum();
        self.metrics.assigned_partitions.set(count as u64);
        let owned = MemberAssignment {
            partitions: current.clone(),
        };
        drop(current);
        coordinator.record_owned_partitions(&member_id, &owned);

        Ok(false)
    }

    /// Handle group rebalance and inline heartbeat during poll.
    ///
    /// Returns `true` if poll() should return an empty result immediately
    /// (e.g., cooperative rebalance requires another poll cycle).
    ///
    /// The background heartbeat task may already have completed the group's
    /// `JoinGroup`/`SyncGroup` barrier and parked the resulting assignment, so
    /// that is checked first: the coordinator has moved on to a new generation
    /// and the local view has to catch up before any fetch is issued.
    async fn handle_group_rebalance(&self, timeout: Duration) -> Result<bool> {
        let Some(ref coordinator) = self.group_coordinator else {
            return Ok(false);
        };

        // A rebalance the heartbeat task is already running is the one that
        // will produce this member's next assignment. Wait for it — up to the
        // caller's own poll budget — rather than returning empty immediately,
        // which would spin, or starting a competing JoinGroup, which would
        // leave two of this member's joins racing to define its generation.
        coordinator.await_rejoin(timeout).await;

        if let Some(pending) = coordinator.take_pending_rebalance() {
            // Callbacks and data-plane state stay on the poll path, exactly
            // where they are for a poll-driven rebalance: commit, revoke,
            // rewrite offsets/partition state/buffers, assign.
            if coordinator.is_cooperative() {
                if self
                    .handle_cooperative_rebalance(coordinator, Some(pending))
                    .await?
                {
                    return Ok(true);
                }
            } else {
                let topics: Vec<String> = self.subscriptions.read().await.iter().cloned().collect();
                self.handle_eager_rebalance(coordinator, &topics, Some(pending.assignment))
                    .await?;
            }
        } else if coordinator.rejoin_in_flight() {
            // Still rebalancing after the whole poll budget. There is nothing
            // to fetch mid-rebalance, so report an empty poll; the assignment
            // is picked up by whichever poll the rebalance finishes under.
            debug!("Background rebalance still in flight; returning an empty poll");
            return Ok(true);
        } else if coordinator.needs_rejoin().await {
            let topics: Vec<String> = self.subscriptions.read().await.iter().cloned().collect();
            if !topics.is_empty() {
                coordinator.set_subscribed_topics(topics.clone()).await;

                if coordinator.is_consumer_protocol() {
                    // KIP-848: when the consumer needs to (re)join — initial
                    // join (Unjoined), post-fencing rejoin, or subscription
                    // change — send a full heartbeat with all fields and
                    // (re)start the background task.  When the heartbeat task
                    // delivered a normal assignment update (Stable, same
                    // topics), ensure_active_membership is a no-op.
                    coordinator.ensure_active_membership(&topics).await?;
                    self.handle_kip848_rebalance(coordinator).await?;
                } else if coordinator.is_cooperative() {
                    if self.handle_cooperative_rebalance(coordinator, None).await? {
                        return Ok(true);
                    }
                } else {
                    self.handle_eager_rebalance(coordinator, &topics, None)
                        .await?;
                }
            }
        }

        // Check if inline heartbeat is needed.
        // Skip for KIP-848 — the background ConsumerGroupHeartbeat task handles
        // heartbeats; sending classic Heartbeat requests would use the wrong API.
        if !coordinator.is_consumer_protocol() && coordinator.is_heartbeat_overdue().await {
            match coordinator.send_heartbeat().await {
                Ok(status) if coordinator.handle_inline_heartbeat_status(status).await => {
                    debug!("Heartbeat indicated rejoin needed");
                }
                Err(e) => {
                    warn!("Inline heartbeat failed: {}", e);
                }
                _ => {}
            }
        }

        Ok(false)
    }

    /// Handle cooperative incremental rebalance (KIP-429).
    ///
    /// `phase1` carries a join/sync the background heartbeat task already
    /// completed; passing `None` makes this method run that first round itself.
    /// Either way the revocation callbacks, the second rejoin and the final
    /// assignment all happen here, on the poll path.
    ///
    /// Returns `true` if poll() should return an empty result immediately,
    /// which happens when an inline heartbeat signals rejoin or when the
    /// cooperative round limit is exceeded.
    async fn handle_cooperative_rebalance(
        &self,
        coordinator: &Arc<GroupCoordinator>,
        phase1: Option<PendingRebalance>,
    ) -> Result<bool> {
        // Phase 1: join+sync to get new target assignment
        let (new_assignment, to_revoke) = match phase1 {
            Some(p) => (p.assignment, p.to_revoke),
            None => coordinator.perform_cooperative_join_and_sync().await?,
        };

        if !to_revoke.is_empty() {
            // Revoke only the diff — keep consuming unaffected partitions
            let revoked: Vec<TopicPartition> = to_revoke
                .iter()
                .map(|(t, p)| TopicPartition::new(t, *p))
                .collect();
            if self
                .apply_revocation_round(coordinator, &to_revoke, &revoked)
                .await?
            {
                return Ok(true);
            }
            self.metrics.rebalances.inc();

            // Phase 2: rejoin to finalize after revocations.
            // In rare cases (concurrent topic changes, racing rebalances),
            // additional revocations may be needed. Loop with a bound.
            coordinator.trigger_rejoin().await;
            let mut final_assignment = MemberAssignment::empty();
            for round in 0..self.config.max_cooperative_rebalance_rounds {
                let (assignment, extra_revoke) =
                    coordinator.perform_cooperative_join_and_sync().await?;
                final_assignment = assignment;

                if extra_revoke.is_empty() {
                    break;
                }

                // Process additional revocations (including final round)
                let extra_revoked: Vec<TopicPartition> = extra_revoke
                    .iter()
                    .map(|(t, p)| TopicPartition::new(t, *p))
                    .collect();
                if self
                    .apply_revocation_round(coordinator, &extra_revoke, &extra_revoked)
                    .await?
                {
                    return Ok(true);
                }

                if round == self.config.max_cooperative_rebalance_rounds - 1 {
                    warn!(
                        "Cooperative rebalance exceeded {} rounds with pending revocations; \
                         this may indicate cascading membership changes. \
                         Deferring assignment to next poll cycle.",
                        self.config.max_cooperative_rebalance_rounds
                    );
                    // Start heartbeat to avoid session timeout while we
                    // defer the additional cooperative rebalance round
                    // to the next poll cycle. Do NOT apply final_assignment
                    // since it still required another rejoin. Set state
                    // directly instead of trigger_rejoin() to avoid
                    // killing the heartbeat task via Rejoin command.
                    coordinator.start_heartbeat_task().await;
                    coordinator.set_preparing_rebalance().await;
                    // Note: rebalances metric was already incremented
                    // at Phase 1 entry; do not double-count here.
                    // assigned_partitions metric was already updated
                    // after apply_partition_revocations above.
                    return Ok(true);
                }

                coordinator.trigger_rejoin().await;
            }

            // Finalize cooperative assignment: update assignments,
            // fire on_partitions_assigned, fetch offsets, record owned.
            let old_assignments = self.assignments.read().await.clone();
            self.finalize_cooperative_assignment(coordinator, &final_assignment, &old_assignments)
                .await?;
        } else {
            // No revocations — assignment is final in one round
            let old_assignments = self.assignments.read().await.clone();

            // Build HashSet index of new partitions for O(1) lookups.
            let new_sets: HashMap<&String, HashSet<PartitionId>> = new_assignment
                .partitions
                .iter()
                .map(|(t, ps)| (t, ps.iter().copied().collect()))
                .collect();

            // Determine partitions removed in this rebalance
            // (e.g., reassigned to another member, topic deleted).
            // This is a clean cooperative revocation, not an unclean
            // loss, so use on_partitions_revoked (not on_partitions_lost).
            let mut revoked_parts: Vec<TopicPartition> = Vec::new();
            for (topic, partitions) in &old_assignments {
                let new_set = new_sets.get(topic);
                for &p in partitions {
                    let gone = new_set.is_none_or(|ns| !ns.contains(&p));
                    if gone {
                        revoked_parts.push(TopicPartition::new(topic, p));
                    }
                }
            }
            if !revoked_parts.is_empty() {
                // Commit before revoking, matching the two-round cooperative
                // path and the KIP-848 path.
                //
                // `apply_partition_revocations` deletes these partitions from
                // the offset map, so any progress not committed by this point
                // is unrecoverable — there is no later opportunity, and the
                // member taking over resumes from the last periodic commit and
                // re-processes everything since.
                if self.config.enable_auto_commit
                    && self.group_coordinator.is_some()
                    && let Err(e) = self.commit().await
                {
                    warn!("Commit before cooperative revocation failed: {e}");
                }

                self.safe_on_partitions_revoked(&revoked_parts).await;
                let revoked_tuples: Vec<(String, PartitionId)> = revoked_parts
                    .iter()
                    .map(|tp| (tp.topic.clone(), tp.partition))
                    .collect();
                self.apply_partition_revocations(&revoked_tuples).await;
            }

            self.metrics.rebalances.inc();

            // Finalize cooperative assignment: update assignments,
            // fire on_partitions_assigned, fetch offsets, record owned.
            self.finalize_cooperative_assignment(coordinator, &new_assignment, &old_assignments)
                .await?;
        }

        Ok(false)
    }

    /// Handle KIP-848 server-side assignment: diff-based callbacks.
    ///
    /// The KIP-848 background heartbeat task stores the new assignment in
    /// `GroupCoordinator.assignment` and signals rebalance. This method reads
    /// the current assignment, computes the diff against the Consumer's local
    /// assignments, fires revocation/assignment callbacks for changed
    /// partitions, and fetches committed offsets for newly added ones.
    async fn handle_kip848_rebalance(&self, coordinator: &Arc<GroupCoordinator>) -> Result<()> {
        let new_assignment = coordinator.assignment().await;
        let old_assignments = self.assignments.read().await.clone();

        // Build HashSets for O(n) diffing instead of Vec::contains.
        let old_sets: HashMap<&String, HashSet<PartitionId>> = old_assignments
            .iter()
            .map(|(t, ps)| (t, ps.iter().copied().collect()))
            .collect();
        let new_sets: HashMap<&String, HashSet<PartitionId>> = new_assignment
            .partitions
            .iter()
            .map(|(t, ps)| (t, ps.iter().copied().collect()))
            .collect();

        // Compute revoked partitions: in old but not in new.
        let mut revoked: Vec<TopicPartition> = Vec::new();
        for (topic, old_set) in &old_sets {
            let new_set = new_sets.get(*topic);
            for &p in old_set {
                let retained = new_set.is_some_and(|ns| ns.contains(&p));
                if !retained {
                    revoked.push(TopicPartition::new(*topic, p));
                }
            }
        }

        // Compute newly assigned partitions: in new but not in old.
        let mut assigned: Vec<TopicPartition> = Vec::new();
        for (topic, new_set) in &new_sets {
            let old_set = old_sets.get(*topic);
            for &p in new_set {
                let was_assigned = old_set.is_some_and(|os| os.contains(&p));
                if !was_assigned {
                    assigned.push(TopicPartition::new(*topic, p));
                }
            }
        }

        if revoked.is_empty() && assigned.is_empty() {
            // No actual change — the heartbeat task may have signalled
            // rebalance for state reasons (e.g. first assignment).
            // Still need to ensure our local assignments are in sync.
            if old_assignments.is_empty() && !new_assignment.partitions.is_empty() {
                // First assignment: treat all partitions as newly assigned.
                for (topic, parts) in &new_assignment.partitions {
                    for &p in parts {
                        assigned.push(TopicPartition::new(topic, p));
                    }
                }
            } else if !old_assignments.is_empty() {
                // Had partitions before, diff shows no movement — nothing to do.
                return Ok(());
            }
            // Remaining case: old_assignments is empty.  Either
            //   (a) new is also empty  — first heartbeat with an empty
            //       assignment (more consumers than partitions), or
            //   (b) new is non-empty   — handled by the branch above.
            // For (a) we fall through so on_partitions_assigned fires,
            // matching cooperative/eager paths which always invoke the
            // callback on the initial assignment.
        }

        // Fire revocation callback and clean up per-partition state.
        if !revoked.is_empty() {
            // KIP-848 §revocation: commit offsets for the partitions we are
            // about to lose before invoking the user callback, so the new
            // owner sees up-to-date committed positions. The old assignments
            // are still active at this point, so `commit()` includes them.
            if self.config.enable_auto_commit
                && let Err(e) = self.commit().await
            {
                if e.is_retriable() {
                    warn!(
                        "Auto-commit before KIP-848 revocation failed (retriable): {}",
                        e
                    );
                } else {
                    error!(
                        "Auto-commit before KIP-848 revocation failed (fatal): {}",
                        e
                    );
                }
            }
            self.safe_on_partitions_revoked(&revoked).await;
            let revoked_tuples: Vec<(String, PartitionId)> = revoked
                .iter()
                .map(|tp| (tp.topic.clone(), tp.partition))
                .collect();
            self.apply_partition_revocations(&revoked_tuples).await;
        }

        // Update assignments to the new state.
        {
            let mut assignments = self.assignments.write().await;
            assignments.clear();
            for (topic, partitions) in &new_assignment.partitions {
                assignments.insert(topic.clone(), partitions.clone());
            }
        }

        self.metrics.rebalances.inc();

        // Fire assignment callback with only the *newly* assigned partitions
        // (delta), consistent with the classic eager/cooperative paths.
        self.safe_on_partitions_assigned(&assigned).await;

        let count: usize = new_assignment.partitions.values().map(|ps| ps.len()).sum();
        self.metrics.assigned_partitions.set(count as u64);

        // Fetch committed offsets only for newly assigned partitions.
        if !assigned.is_empty() {
            let new_parts = Self::group_partitions_by_topic(&assigned);
            self.fetch_and_apply_committed_offsets(&new_parts).await?;
        }

        // Acknowledge the reconciliation unconditionally, once callbacks have
        // run and offsets for newly assigned partitions have been fetched.
        //
        // The acknowledgement is what advances the member epoch, and the
        // coordinator treats the member as still reconciling until it arrives.
        // Sending it only when partitions were revoked leaves a member that
        // received a pure grant — the common case when a group grows, or on
        // the very first assignment — stuck at its old epoch forever: the
        // group never finishes reconciling, and commits eventually start
        // failing with STALE_MEMBER_EPOCH.
        coordinator.acknowledge_revocation().await;

        Ok(())
    }

    /// Handle eager rebalance: revoke all partitions, then reassign from scratch.
    ///
    /// `pending` carries an assignment the background heartbeat task already
    /// obtained through `JoinGroup`/`SyncGroup`. When it is `None` this method
    /// performs the join itself. The revoke-all, the callbacks and the offset
    /// fetch are identical in both cases and always run here, on the poll path.
    async fn handle_eager_rebalance(
        &self,
        coordinator: &Arc<GroupCoordinator>,
        topics: &[String],
        pending: Option<MemberAssignment>,
    ) -> Result<()> {
        let old_assignments = self.assignments.read().await.clone();
        if !old_assignments.is_empty() {
            let revoked: Vec<TopicPartition> = old_assignments
                .iter()
                .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
                .collect();
            // Commit offsets for all partitions before the eager revoke-all,
            // so the group has up-to-date committed positions.
            if self.config.enable_auto_commit
                && let Err(e) = self.commit().await
            {
                if e.is_retriable() {
                    warn!(
                        "Auto-commit before eager revocation failed (retriable): {}",
                        e
                    );
                } else {
                    error!("Auto-commit before eager revocation failed (fatal): {}", e);
                }
            }
            self.safe_on_partitions_revoked(&revoked).await;
            self.clear_partition_state().await;

            // Clear assignments immediately after revocation so that
            // if ensure_active_membership fails below, the next poll
            // won't re-fire on_partitions_revoked for already-revoked
            // partitions. Matches the Java client's behavior of
            // clearing subscription state after the eager revoke phase.
            self.assignments.write().await.clear();
            self.metrics.assigned_partitions.set(0);
        }

        self.metrics.rebalances.inc();

        let assignment = match pending {
            // The background task already ran the join/sync for this
            // generation; re-running it would start an unnecessary rebalance.
            Some(assignment) => assignment,
            // `joined` is always true here: handle_group_rebalance gates on
            // needs_rejoin(), so ensure_active_membership always performs a
            // full JoinGroup/SyncGroup.
            None => coordinator.ensure_active_membership(topics).await?.0,
        };

        // Update our assignments
        let mut assignments = self.assignments.write().await;
        assignments.clear();
        for (topic, partitions) in &assignment.partitions {
            assignments.insert(topic.clone(), partitions.clone());
        }
        drop(assignments);

        // Notify listener of newly assigned partitions
        let assigned: Vec<TopicPartition> = assignment
            .partitions
            .iter()
            .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
            .collect();
        self.safe_on_partitions_assigned(&assigned).await;
        self.metrics.assigned_partitions.set(assigned.len() as u64);

        // Fetch committed offsets for new assignment
        self.fetch_and_apply_committed_offsets(&assignment.partitions)
            .await?;

        Ok(())
    }

    /// Group topic-partitions into a map keyed by topic name.
    fn group_partitions_by_topic(
        partitions: &[TopicPartition],
    ) -> HashMap<String, Vec<PartitionId>> {
        let mut map: HashMap<String, Vec<PartitionId>> = HashMap::new();
        for tp in partitions {
            map.entry(tp.topic.clone()).or_default().push(tp.partition);
        }
        map
    }

    /// Fetch committed offsets and apply auto_offset_reset for partitions without committed offsets.
    ///
    /// Called after group assignment to initialize partition offsets:
    /// 1. Fetch committed offsets from the group coordinator
    /// 2. For partitions with no committed offset, apply the configured auto_offset_reset policy
    async fn fetch_and_apply_committed_offsets(
        &self,
        assigned: &HashMap<String, Vec<PartitionId>>,
    ) -> Result<()> {
        let coordinator = match self.group_coordinator {
            Some(ref c) => c,
            None => return Ok(()),
        };

        // Fetch committed offsets
        let committed = coordinator.fetch_committed_offsets(assigned).await?;

        // Seed the per-partition leader epoch from what the group committed.
        //
        // This is the second half of KIP-320: the epoch travels out on
        // `OffsetCommit` and comes back on `OffsetFetch`, and installing it
        // here is what lets the first fetch after a rebalance or restart send
        // a `(position, epoch)` pair the broker can check for divergence.
        // Without it the resumed consumer starts every partition with epoch
        // `-1`, which disables the check exactly when an unclean leader
        // election is most likely to have happened — while the group was
        // rebalancing.
        {
            let mut partition_state = self.partition_state.write().await;
            for (key, position) in &committed {
                if position.leader_epoch >= 0 {
                    partition_state
                        .entry(key.clone())
                        .or_default()
                        .last_fetched_epoch = Some(position.leader_epoch);
                }
            }
        }

        let committed: HashMap<(String, PartitionId), Offset> = committed
            .into_iter()
            .map(|(key, position)| (key, position.offset))
            .collect();

        let mut offsets = self.offsets.write().await;

        // Log the initial offsets state before processing committed offsets
        debug!("fetch_and_apply: existing offsets: {:?}", *offsets);

        let need_reset = apply_assignment_offset_precedence(
            assigned,
            &committed,
            &self.config.initial_offsets,
            &mut offsets,
        );

        if need_reset.is_empty() {
            return Ok(());
        }

        // Apply auto_offset_reset
        if let Some(timestamp) = self.config.auto_offset_reset.to_offset() {
            // Group partitions by topic for list_offsets call
            let mut reset_partitions: HashMap<String, Vec<PartitionId>> = HashMap::new();
            for (topic, partition) in &need_reset {
                reset_partitions
                    .entry(topic.clone())
                    .or_default()
                    .push(*partition);
            }

            let resolved = coordinator
                .list_offsets(&reset_partitions, timestamp)
                .await?;

            for (key, offset) in &resolved {
                offsets.insert(key.clone(), *offset);
            }

            // Fallback: if the group coordinator's list_offsets silently
            // dropped some partitions (partition-level errors), resolve
            // them individually via the direct ListOffsets v1 path.
            for (topic, partition) in &need_reset {
                let key = (topic.clone(), *partition);
                if !resolved.contains_key(&key) && !offsets.contains_key(&key) {
                    debug!(
                        "Falling back to direct ListOffsets for {}-{} \
                         (coordinator path returned no result)",
                        topic, partition
                    );
                    // Release offsets lock temporarily for the network call
                    drop(offsets);
                    match self.resolve_list_offset(topic, *partition, timestamp).await {
                        Ok(offset) => {
                            offsets = self.offsets.write().await;
                            offsets.insert(key, offset);
                        }
                        Err(e) => {
                            warn!(
                                "Fallback offset resolution failed for {}-{}: {}",
                                topic, partition, e
                            );
                            offsets = self.offsets.write().await;
                        }
                    }
                }
            }
        } else {
            // AutoOffsetReset::None — fail if no committed offset
            let missing: Vec<String> = need_reset.iter().map(|(t, p)| format!("{t}-{p}")).collect();
            return Err(KrafkaError::invalid_state(format!(
                "no committed offset for partitions and auto.offset.reset=none: {}",
                missing.join(", ")
            )));
        }

        // Drop the write lock before recomputing lag metrics to avoid
        // deadlocking with the read lock that recompute_lag_metrics acquires.
        drop(offsets);

        self.recompute_lag_metrics().await;
        Ok(())
    }

    /// Assign specific partitions manually.
    ///
    /// Manual assignment and group subscription are mutually exclusive.
    /// This method returns an error if a group coordinator is active.
    pub async fn assign(&self, topic: &str, partitions: Vec<PartitionId>) -> Result<()> {
        // H6: reject empty / oversize topic names at ingress so they cannot
        // reach the panicking `KafkaString::encode` path via MetadataRequest /
        // FetchRequest / OffsetFetchRequest.
        validate_topic_name(topic)?;

        if self.group_coordinator.is_some() {
            return Err(KrafkaError::invalid_state(
                "cannot use manual partition assignment with consumer group subscription",
            ));
        }

        // Refresh metadata so we can resolve partition leaders for offset lookup
        self.metadata.refresh_for_topics(Some(&[topic])).await?;

        let topic_owned = topic.to_string();

        // Replacing a topic's partition list is also a *revocation* of whatever
        // it dropped. Without this, `assign("t", vec![0, 1])` followed by
        // `assign("t", vec![0])` left partition 1's position, cached watermarks,
        // paused flag and — since the consumer reads ahead — up to
        // `max_buffered_records` of its records behind. The stale buffer entry
        // is the damaging one: `committable_positions` clamps commits to the
        // lowest buffered offset, so a partition the caller has stopped
        // consuming would keep dragging the commit for a partition it still is.
        let dropped: Vec<(String, PartitionId)> = {
            let assignments = self.assignments.read().await;
            let keep: HashSet<PartitionId> = partitions.iter().copied().collect();
            assignments
                .get(&topic_owned)
                .map(|previous| {
                    previous
                        .iter()
                        .filter(|p| !keep.contains(p))
                        .map(|p| (topic_owned.clone(), *p))
                        .collect()
                })
                .unwrap_or_default()
        };
        if !dropped.is_empty() {
            debug!(
                topic = %topic_owned,
                partitions = dropped.len(),
                "Dropping partitions removed by a narrower assign()"
            );
            self.apply_partition_revocations(&dropped).await;
        }

        let mut assignments = self.assignments.write().await;
        assignments.insert(topic_owned.clone(), partitions.clone());

        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.insert(topic_owned.clone());
        drop(subscriptions);
        drop(assignments);

        // Apply auto_offset_reset for manually assigned partitions
        let mut assigned = HashMap::new();
        debug!("Assigned partitions for {}: {:?}", topic, partitions);
        assigned.insert(topic_owned, partitions);
        self.apply_auto_offset_reset(&assigned).await?;

        Ok(())
    }

    /// Apply auto_offset_reset policy for partitions that have no tracked offset.
    ///
    /// This resolves initial offsets based on the configured `auto_offset_reset`
    /// policy (Earliest, Latest, or None). Used by both group and non-group
    /// consumers during partition assignment.
    async fn apply_auto_offset_reset(
        &self,
        assigned: &HashMap<String, Vec<PartitionId>>,
    ) -> Result<()> {
        // Pre-seed any caller-supplied initial offsets for newly assigned
        // partitions that don't yet have a tracked position. These override
        // `auto_offset_reset`.
        if !self.config.initial_offsets.is_empty() {
            let mut offsets = self.offsets.write().await;
            let inserted = seed_initial_offsets_for_assigned(
                assigned,
                &self.config.initial_offsets,
                &mut offsets,
            );
            if inserted > 0 {
                debug!("Applied {} assignment-time initial offsets", inserted);
            }
        }

        // Collect partitions that don't already have a tracked offset
        let need_reset: Vec<(String, PartitionId)> = {
            let offsets = self.offsets.read().await;
            let mut need = Vec::new();
            for (topic, partitions) in assigned {
                for &p in partitions {
                    let key = (topic.clone(), p);
                    if !offsets.contains_key(&key) {
                        need.push(key);
                    }
                }
            }
            need
        };

        if need_reset.is_empty() {
            return Ok(());
        }

        if let Some(timestamp) = self.config.auto_offset_reset.to_offset() {
            let reset_pairs: Vec<(&str, PartitionId)> =
                need_reset.iter().map(|(t, p)| (t.as_str(), *p)).collect();
            let batch = group_topic_partitions(&reset_pairs);

            let resolved = self.resolve_list_offsets(&batch, timestamp).await;
            let mut offsets = self.offsets.write().await;
            for (key, result) in &resolved {
                if let Ok(offset) = result {
                    offsets.insert(key.clone(), *offset);
                }
            }
            drop(offsets);

            // Log any partitions that weren't resolved (no leader, broker error, etc.)
            for key in &need_reset {
                if resolved.get(key).is_none_or(|r| r.is_err()) {
                    warn!(
                        "Failed to resolve offset for {}-{}, will retry on next poll",
                        key.0, key.1
                    );
                }
            }
        } else {
            // AutoOffsetReset::None — fail if no offset
            let missing = need_reset
                .iter()
                .map(|(t, p)| format!("{t}-{p}"))
                .collect::<Vec<_>>()
                .join(", ");
            return Err(KrafkaError::invalid_state(format!(
                "no offset for partitions and auto.offset.reset=none: {missing}"
            )));
        }

        self.recompute_lag_metrics().await;
        Ok(())
    }

    /// Drop buffered records for partitions whose position was just moved from
    /// outside the fetch loop, and refresh the buffered-records gauge.
    ///
    /// See [`purge_buffered_records`] for why leaving them behind corrupts both
    /// delivery and the committed offset. Synchronous: the buffer is guarded by
    /// a `parking_lot::Mutex` and no `.await` happens under the guard.
    fn discard_buffered_for(&self, repositioned: &HashSet<(String, PartitionId)>) {
        let mut buffer = self.recv_buffer.lock();
        let dropped = purge_buffered_records(&mut buffer, repositioned);
        if dropped > 0 {
            debug!(
                dropped,
                partitions = repositioned.len(),
                "Discarded buffered records for repositioned partitions"
            );
        }
        self.metrics.buffered_records.set(buffer.len() as u64);
    }

    /// Seek to a specific offset.
    ///
    /// The new position invalidates everything the consumer knew about where
    /// it was in the log: the leader epoch at the old position says nothing
    /// about the new one, and the new one has not been checked against the
    /// leader. Both are reset so the next poll re-validates the position
    /// before fetching from it.
    pub async fn seek(&self, topic: &str, partition: PartitionId, offset: Offset) -> Result<()> {
        {
            let mut offsets = self.offsets.write().await;
            offsets.insert((topic.to_string(), partition), offset);
            let mut partition_state = self.partition_state.write().await;
            invalidate_position_epoch(&mut partition_state, &(topic.to_string(), partition));
        }
        self.discard_buffered_for(&HashSet::from([(topic.to_string(), partition)]));
        self.recompute_lag_metrics().await;
        self.metrics.record_seek(1);
        debug!("Seek to offset {} for {}-{}", offset, topic, partition);
        Ok(())
    }

    /// Seek multiple partitions to the given offsets in one atomic update.
    ///
    /// Equivalent to calling [`seek`](Self::seek) for each entry, but acquires
    /// the offset lock only once and recomputes lag metrics once at the end.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use ahash::AHashMap;
    ///
    /// consumer
    ///     .seek_many(&AHashMap::from_iter([
    ///         (("orders".to_string(), 0), 1_000),
    ///         (("orders".to_string(), 1), 2_000),
    ///     ]))
    ///     .await?;
    /// ```
    pub async fn seek_many(&self, offsets: &HashMap<(String, PartitionId), Offset>) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }
        {
            let mut stored = self.offsets.write().await;
            apply_seek_many_offsets(&mut stored, offsets);
            let mut partition_state = self.partition_state.write().await;
            for key in offsets.keys() {
                invalidate_position_epoch(&mut partition_state, key);
            }
        }
        self.discard_buffered_for(&offsets.keys().cloned().collect());
        self.recompute_lag_metrics().await;
        self.metrics.record_seek(offsets.len() as u64);
        debug!("Sought {} partitions via seek_many", offsets.len());
        Ok(())
    }

    /// Seek to the beginning.
    pub async fn seek_to_beginning(&self, topic: &str, partition: PartitionId) -> Result<()> {
        self.seek(topic, partition, 0).await
    }

    /// Seek to the end (latest offset).
    ///
    /// Sets the consumer position to the high watermark, so subsequent polls
    /// will only return new messages produced after this call.
    ///
    /// This resolves the actual latest offset via a ListOffsets RPC to the
    /// partition leader. The Kafka Fetch API does not interpret special offset
    /// values like -1; those are only meaningful in the ListOffsets API.
    pub async fn seek_to_end(&self, topic: &str, partition: PartitionId) -> Result<()> {
        // Resolve the actual latest offset via ListOffsets (timestamp=-1 means latest)
        let offset = self.resolve_list_offset(topic, partition, -1).await?;
        self.seek(topic, partition, offset).await
    }

    /// Seek to the first message whose timestamp is greater than or equal to
    /// `timestamp_ms` (milliseconds since Unix epoch).
    ///
    /// Uses the Kafka `ListOffsets` API to resolve the offset, then calls
    /// [`seek`](Self::seek) on the resolved position. The seek takes effect on the next
    /// [`recv`](Self::recv) / `poll` call.
    ///
    /// # Errors
    ///
    /// Returns an error if the partition has no leader, the broker is
    /// unreachable, or no message exists at or after the given timestamp (the
    /// broker returns offset `-1` in that case).
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Start consuming from messages produced on or after 2024-01-01 00:00 UTC
    /// let ts = 1_704_067_200_000_i64; // ms
    /// consumer.seek_to_timestamp("orders", 0, ts).await?;
    /// ```
    pub async fn seek_to_timestamp(
        &self,
        topic: &str,
        partition: PartitionId,
        timestamp_ms: i64,
    ) -> Result<()> {
        let offset = self
            .resolve_list_offset(topic, partition, timestamp_ms)
            .await?;
        if offset < 0 {
            return Err(KrafkaError::invalid_state(format!(
                "no message with timestamp >= {timestamp_ms} ms found in {topic}-{partition}"
            )));
        }
        self.seek(topic, partition, offset).await
    }

    /// Look up the earliest offset whose message timestamp is greater than or
    /// equal to the given timestamp, for each listed `(topic, partition)`.
    ///
    /// Uses the ListOffsets API. Requests are batched by leader broker so each
    /// broker receives at most one RPC.
    ///
    /// Every input partition appears in the returned map:
    /// - `Ok(offset)` — the broker returned a valid offset (`-1` means no
    ///   message exists at or after the timestamp for that partition).
    /// - `Err(e)` — a partition-level broker error (e.g. `NotLeaderForPartition`)
    ///   or a transport failure prevented resolution for this partition.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = consumer
    ///     .offsets_for_times(&[("orders", 0), ("orders", 1)], 1_700_000_000_000)
    ///     .await;
    /// for ((topic, partition), result) in &results {
    ///     match result {
    ///         Ok(offset) => println!("{topic}-{partition}: {offset}"),
    ///         Err(e) => eprintln!("{topic}-{partition}: error {e}"),
    ///     }
    /// }
    /// ```
    pub async fn offsets_for_times(
        &self,
        partitions: &[(&str, PartitionId)],
        timestamp: i64,
    ) -> HashMap<(String, PartitionId), Result<Offset>> {
        // Validate all topic names up front; surface invalid ones as per-partition
        // Err entries rather than letting them reach protocol encoding.
        let mut result: HashMap<(String, PartitionId), Result<Offset>> = HashMap::new();
        let mut valid: Vec<(&str, PartitionId)> = Vec::with_capacity(partitions.len());
        for &(topic, partition) in partitions {
            match validate_topic_name(topic) {
                Ok(()) => valid.push((topic, partition)),
                Err(e) => {
                    result.insert((topic.to_string(), partition), Err(e));
                }
            }
        }
        if !valid.is_empty() {
            let grouped = group_topic_partitions(&valid);
            result.extend(self.resolve_list_offsets(&grouped, timestamp).await);
        }
        result
    }

    /// Look up the earliest offset whose message timestamp is greater than or
    /// equal to the given timestamp, for every partition of a single topic.
    ///
    /// Convenience wrapper around [`Consumer::offsets_for_times`] that resolves
    /// the topic's partitions from metadata so callers don't have to list
    /// them. Always refreshes topic metadata before deriving the partition
    /// list so the results reflect the latest leader assignments (the refresh
    /// is skipped by the metadata layer if cached metadata is still fresh).
    ///
    /// Returns `Err` if the topic cannot be found after the metadata refresh.
    /// On success, each `PartitionId` maps to `Ok(offset)` or `Err(e)` —
    /// see [`Consumer::offsets_for_times`] for per-partition semantics.
    pub async fn offsets_for_times_for_topic(
        &self,
        topic: &str,
        timestamp: i64,
    ) -> Result<HashMap<PartitionId, Result<Offset>>> {
        validate_topic_name(topic)?;
        self.metadata.refresh_for_topics(Some(&[topic])).await?;
        let info = self
            .metadata
            .topic(topic)
            .ok_or_else(|| KrafkaError::invalid_state(format!("topic not found: {topic}")))?;

        // Build the grouped map directly — topic name is already validated and
        // the partition list comes from trusted metadata, so bypass the
        // per-entry validation loop inside `offsets_for_times`.
        let mut grouped: HashMap<String, Vec<PartitionId>> = HashMap::new();
        grouped.insert(
            topic.to_string(),
            info.partitions.values().map(|p| p.partition).collect(),
        );
        let results = self.resolve_list_offsets(&grouped, timestamp).await;

        Ok(results
            .into_iter()
            .map(|((_, p), result)| (p, result))
            .collect())
    }

    /// Fetch the low (log start) and high (latest) watermarks for a partition.
    ///
    /// Issues two ListOffsets RPCs to the partition leader — one for the
    /// earliest offset (`timestamp = -2`) and one for the latest
    /// (`timestamp = -1`) — and returns `(low, high)`. Both RPCs are issued
    /// concurrently.
    pub async fn fetch_watermarks(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<(Offset, Offset)> {
        validate_topic_name(topic)?;
        let (low, high) = tokio::join!(
            self.resolve_list_offset(topic, partition, -2),
            self.resolve_list_offset(topic, partition, -1),
        );
        Ok((low?, high?))
    }

    /// Return a snapshot of cluster metadata (brokers and topics).
    ///
    /// If `topic` is `Some`, only that topic is returned and the metadata
    /// layer is asked to refresh that topic first (the network call is
    /// skipped if cached metadata is still fresh). If `None`, a snapshot of
    /// all currently cached topics is returned without triggering a refresh
    /// (cached data may be partial or stale).
    pub async fn fetch_metadata(&self, topic: Option<&str>) -> Result<FetchMetadataResult> {
        if let Some(name) = topic {
            validate_topic_name(name)?;
            self.metadata.refresh_for_topics(Some(&[name])).await?;
        }

        let brokers = self.metadata.brokers();
        let topics = match topic {
            Some(name) => self
                .metadata
                .topic(name)
                .map(|t| vec![t])
                .unwrap_or_default(),
            None => self.metadata.topics(),
        };

        Ok(FetchMetadataResult { brokers, topics })
    }

    /// Resolve an offset timestamp via the ListOffsets API.
    ///
    /// `timestamp` should be:
    /// - `-1` for the latest offset (high watermark)
    /// - `-2` for the earliest available offset
    async fn resolve_list_offset(
        &self,
        topic: &str,
        partition: PartitionId,
        timestamp: i64,
    ) -> Result<Offset> {
        let mut partitions = HashMap::new();
        let topic_owned = topic.to_string();
        partitions.insert(topic_owned.clone(), vec![partition]);
        let mut results = self.resolve_list_offsets(&partitions, timestamp).await;
        results
            .remove(&(topic_owned, partition))
            .unwrap_or_else(|| {
                Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::Malformed,
                    format!("no offset returned for {topic}-{partition}"),
                ))
            })
    }

    /// Resolve offsets for multiple partitions in batched ListOffsets RPCs,
    /// grouped by leader broker so each broker receives at most one request.
    ///
    /// Every requested partition appears in the returned map:
    /// - `Ok(offset)` — broker returned a valid offset.
    /// - `Err(e)` — the partition could not be resolved (no leader, connection
    ///   failure, or a partition-level broker error).
    async fn resolve_list_offsets(
        &self,
        partitions: &HashMap<String, Vec<PartitionId>>,
        timestamp: i64,
    ) -> HashMap<(String, PartitionId), Result<Offset>> {
        if partitions.is_empty() {
            return HashMap::new();
        }

        // Pre-populate every partition with a default error; replaced on success.
        let mut result: HashMap<(String, PartitionId), Result<Offset>> = partitions
            .iter()
            .flat_map(|(topic, parts)| {
                let topic = topic.clone();
                parts.iter().map(move |&p| {
                    let msg = format!("no leader found for {topic}-{p}");
                    ((topic.clone(), p), Err(KrafkaError::invalid_state(msg)))
                })
            })
            .collect();

        // Group partitions by leader broker
        let mut by_leader: HashMap<crate::BrokerId, Vec<(String, PartitionId)>> = HashMap::new();
        let mut leaderless: Vec<(String, PartitionId)> = Vec::new();
        for (topic, parts) in partitions {
            for &p in parts {
                if let Some(leader) = self.metadata.leader(topic, p) {
                    by_leader
                        .entry(leader)
                        .or_default()
                        .push((topic.clone(), p));
                } else {
                    leaderless.push((topic.clone(), p));
                }
            }
        }

        // Retry leaderless partitions after a metadata refresh
        if !leaderless.is_empty() {
            // Deduplicate topics to avoid redundant refresh work when multiple
            // partitions of the same topic are leaderless.
            let topic_set: HashSet<&str> = leaderless.iter().map(|(t, _)| t.as_str()).collect();
            let topics: Vec<&str> = topic_set.into_iter().collect();
            if let Err(err) = self.metadata.refresh_for_topics(Some(&topics)).await {
                warn!(
                    "Failed to refresh metadata for leaderless topics {:?}: {}",
                    topics, err
                );
            }
            for (topic, partition) in leaderless {
                if let Some(leader) = self.metadata.leader(&topic, partition) {
                    by_leader
                        .entry(leader)
                        .or_default()
                        .push((topic, partition));
                } else {
                    warn!(
                        "No leader for {}-{} after metadata refresh",
                        topic, partition
                    );
                    // result[(topic, partition)] retains its default Err
                }
            }
        }

        for (&leader_id, leader_partitions) in &by_leader {
            // Group into ListOffsetsRequest topics
            let mut topics_map: HashMap<String, Vec<ListOffsetsRequestPartition>> = HashMap::new();
            for (topic, partition) in leader_partitions {
                topics_map
                    .entry(topic.clone())
                    .or_default()
                    .push(ListOffsetsRequestPartition {
                        partition_index: *partition,
                        // Third leg of KIP-320, alongside Fetch and
                        // OffsetCommit. The broker compares this against its
                        // own epoch and answers FENCED_LEADER_EPOCH /
                        // UNKNOWN_LEADER_EPOCH when they disagree, rather than
                        // handing back an offset from a log this client's view
                        // of leadership says nothing about. Resolving
                        // `auto.offset.reset` off a stale leader is exactly how
                        // a consumer lands in a diverged log.
                        //
                        // `-1` remains correct when the epoch is genuinely
                        // unknown, and the encoder only serialises the field
                        // from v4 on, so this is inert against a broker that
                        // negotiates lower.
                        current_leader_epoch: self
                            .metadata
                            .leader_epoch(topic, *partition)
                            .unwrap_or(-1),
                        timestamp,
                    });
            }

            let topics: Vec<ListOffsetsRequestTopic> = topics_map
                .into_iter()
                .map(|(name, parts)| ListOffsetsRequestTopic {
                    name,
                    partitions: parts,
                })
                .collect();

            let request = ListOffsetsRequest {
                replica_id: -1,
                isolation_level: self.config.isolation_level.to_i8(),
                topics,
                timeout_ms: None,
            };

            // Get a connection to this broker by leader ID.
            // On any broker-level failure, mark all its partitions as Err.
            let broker_info = match self.metadata.broker(leader_id) {
                Some(b) => b,
                None => {
                    warn!("Broker {} not found in metadata, skipping", leader_id);
                    let err = KrafkaError::invalid_state(format!(
                        "broker {leader_id} not found in metadata"
                    ));
                    for (topic, partition) in leader_partitions {
                        result.insert((topic.clone(), *partition), Err(err.clone()));
                    }
                    continue;
                }
            };
            let conn = match self
                .pool
                .get_connection_by_id(leader_id, broker_info.address())
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    warn!("Failed to connect to broker {}: {}, skipping", leader_id, e);
                    for (topic, partition) in leader_partitions {
                        result.insert((topic.clone(), *partition), Err(e.clone()));
                    }
                    continue;
                }
            };

            // Negotiate ListOffsets version — require v1+ (MIN).
            let list_version = match conn.negotiate_api_version(
                ApiKey::ListOffsets,
                versions::LIST_OFFSETS_MAX,
                versions::LIST_OFFSETS_MIN,
            ) {
                Some(v) => v,
                None => {
                    let err = KrafkaError::protocol_kind(
                        ProtocolErrorKind::UnknownApiVersion,
                        format!(
                            "no mutually supported ListOffsets API version for broker {leader_id}"
                        ),
                    );
                    warn!("{err}");
                    for (topic, partition) in leader_partitions {
                        result.insert((topic.clone(), *partition), Err(err.clone()));
                    }
                    continue;
                }
            };

            let response = match conn
                .send_request(ApiKey::ListOffsets, list_version, |buf| {
                    request.encode_versioned(list_version, buf)
                })
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "ListOffsets v{} request failed for broker {}: {}, skipping",
                        list_version, leader_id, e
                    );
                    for (topic, partition) in leader_partitions {
                        result.insert((topic.clone(), *partition), Err(e.clone()));
                    }
                    continue;
                }
            };

            let mut buf = response;
            let list_response = match ListOffsetsResponse::decode_versioned(list_version, &mut buf)
            {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "Failed to decode ListOffsets v{} response from broker {}: {}, skipping",
                        list_version, leader_id, e
                    );
                    let err = KrafkaError::protocol_kind(
                        ProtocolErrorKind::Malformed,
                        format!(
                            "failed to decode ListOffsets response from broker {leader_id}: {e}"
                        ),
                    );
                    for (topic, partition) in leader_partitions {
                        result.insert((topic.clone(), *partition), Err(err.clone()));
                    }
                    continue;
                }
            };

            let stale_epoch_topics = apply_list_offsets_response(&list_response, &mut result);

            // A fenced or unknown epoch means this client's leadership view is
            // stale. The per-partition error is retriable, but retrying with
            // the *same* stale epoch would fail identically forever — the
            // refresh is what makes the retry converge.
            for topic in stale_epoch_topics {
                if let Err(e) = self
                    .metadata
                    .refresh_for_topics_forced(Some(&[&topic]))
                    .await
                {
                    debug!(
                        topic = %topic,
                        error = %e,
                        "metadata refresh after a fenced ListOffsets epoch failed"
                    );
                }
            }
        }

        result
    }

    /// Poll for new records.
    ///
    /// Performs **one** broker fetch round-trip per assigned broker, waits up
    /// to `timeout` for records to arrive, and returns all records received in
    /// that single round.  `max_poll_records` (from [`ConsumerConfig`]) caps
    /// the returned slice.
    ///
    /// **When to use `poll`**: for simple event loops where low per-call
    /// overhead matters and a single broker round-trip per iteration is
    /// acceptable.  Processing happens synchronously in the loop; the fetch
    /// latency equals your per-iteration latency.
    ///
    /// **When to use [`batch_recv`](Self::batch_recv) instead**: when you need
    /// a fixed batch size for downstream batching (e.g., bulk database inserts
    /// or transactional exactly-once pipelines). `batch_recv` drains the
    /// internal buffer first and keeps fetching until `max_records` are
    /// collected *or* the deadline elapses — it is the throughput-optimised
    /// path.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use krafka::consumer::Consumer;
    /// # async fn example() -> Result<(), krafka::error::KrafkaError> {
    /// let consumer = Consumer::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .group_id("my-group")
    ///     .build()
    ///     .await?;
    ///
    /// consumer.subscribe(&["my-topic"]).await?;
    ///
    /// loop {
    ///     let records = consumer.poll(std::time::Duration::from_secs(1)).await?;
    ///     for record in records {
    ///         println!("Received: {:?}", record);
    ///     }
    /// }
    /// # }
    /// ```
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        // Establish the lock-order tracking scope for the whole poll cycle.
        // The per-lock ordering assertions only run inside such a scope, so
        // wrapping the top-level entry point is what makes them effective in
        // debug builds. Compiles away entirely in release builds.
        lock_order::with_lock_tracking(self.poll_inner(timeout)).await
    }

    async fn poll_inner(&self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(KrafkaError::invalid_state("consumer is closed"));
        }

        // Clear and check before doing any work, so a `wakeup()` that arrived
        // before this call still interrupts it rather than being swallowed.
        if self
            .wakeup_flag
            .swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            return Err(KrafkaError::invalid_state("wakeup() was called"));
        }

        let _poll_timer = self.metrics.poll_latency.start();
        self.metrics.polls.inc();

        // Enforce max.poll.interval.ms before doing anything else.
        //
        // If the heartbeat task observed that the application stopped calling
        // poll() for longer than the configured interval, this consumer has
        // stopped heartbeating and the coordinator is reassigning its
        // partitions. Returning records now would let the application keep
        // processing partitions it no longer owns, concurrently with their new
        // owner. Report it instead so the caller can restart the consumer.
        if let Some(ref coordinator) = self.group_coordinator
            && coordinator.poll_interval_exceeded()
        {
            // These partitions are genuinely lost, not cleanly revoked: the
            // coordinator has already stopped counting this member as alive
            // and may have handed them to someone else. Committing them now
            // could overwrite the new owner's position, which is exactly why
            // `on_partitions_lost` (rather than `on_partitions_revoked`)
            // documents that listeners must not commit from it.
            let lost: Vec<TopicPartition> = {
                let assignments = self.assignments.read().await;
                assignments
                    .iter()
                    .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
                    .collect()
            };
            if !lost.is_empty() {
                self.safe_on_partitions_lost(&lost).await;
            }

            // Best-effort explicit leave so the partitions move immediately
            // rather than after the session timeout.
            if let Err(e) = coordinator.leave_group().await {
                debug!("LeaveGroup after poll-interval expiry failed: {e}");
            }
            return Err(KrafkaError::invalid_state(format!(
                "consumer exceeded max_poll_interval ({:?}) between poll() calls and was \
                 removed from group '{}'; its partitions have been reassigned. Process \
                 records faster, reduce max_poll_records, or raise max_poll_interval.",
                coordinator.max_poll_interval(),
                coordinator.group_id(),
            )));
        }

        // A fenced member (KIP-848) has had its partitions taken away without
        // a clean revocation. Drop them *before* the auto-commit below: the
        // commit path filters by this map, so a stale entry here lets the
        // fenced consumer write offsets for a partition another member now
        // owns, overwriting that member's progress.
        //
        // `on_partitions_lost` rather than `on_partitions_revoked` for the same
        // reason as the `max.poll.interval.ms` path: the partitions are gone,
        // not being handed back, and the listener contract says not to commit
        // from `lost`.
        if let Some(ref coordinator) = self.group_coordinator
            && coordinator.take_membership_lost()
        {
            let lost: Vec<TopicPartition> = {
                let assignments = self.assignments.read().await;
                assignments
                    .iter()
                    .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
                    .collect()
            };
            if !lost.is_empty() {
                warn!(
                    group = coordinator.group_id(),
                    partitions = lost.len(),
                    "Member was fenced by the coordinator; dropping its partitions"
                );
                self.assignments.write().await.clear();
                self.metrics.assigned_partitions.set(0);
                self.safe_on_partitions_lost(&lost).await;
            }
        }

        // Surface a non-retriable error recorded by the background heartbeat
        // task. These have no caller to return to when they occur, so without
        // this the application would see a consumer that simply never receives
        // records, with the reason confined to the logs.
        if let Some(ref coordinator) = self.group_coordinator
            && let Some(message) = coordinator.take_fatal_error()
        {
            return Err(KrafkaError::invalid_state(message));
        }

        // Mark the application as alive for this interval.
        if let Some(ref coordinator) = self.group_coordinator {
            coordinator.note_poll();
        }

        // Auto-commit timer: commit if interval has elapsed
        if self.config.enable_auto_commit && self.group_coordinator.is_some() {
            let should_commit = {
                let last = self.last_auto_commit.lock();
                last.elapsed() >= self.config.auto_commit_interval
            };
            if should_commit {
                match self.commit().await {
                    Ok(()) => {
                        *self.last_auto_commit.lock() = Instant::now();
                    }
                    Err(e) => {
                        warn!("Auto-commit failed: {}", e);
                    }
                }
            }
        }

        // Handle group rebalance if needed
        if self.handle_group_rebalance(timeout).await? {
            return Ok(vec![]);
        }

        let assignments = self.assignments.read().await;
        if assignments.is_empty() {
            self.metrics.empty_polls.inc();
            return Ok(Vec::new());
        }

        // How many records this poll may hand back. `-1` means unlimited.
        let max_records = if self.config.max_poll_records > 0 {
            self.config.max_poll_records as usize
        } else {
            usize::MAX
        };

        // Deliver already-fetched records before going to the network.
        //
        // The previous poll decodes past its delivery cap on purpose and parks
        // the surplus here, so a poll that finds the buffer stocked returns
        // without a single round trip. Their positions were advanced when they
        // were fetched, so they carry no offset update — the commit is held
        // behind them by `committable_positions` instead.
        let mut prefetched: Vec<ConsumerRecord> = Vec::new();
        {
            let paused = self.paused.read().await.clone();
            let mut buffer = self.recv_buffer.lock();
            drain_buffered_records(&mut buffer, &mut prefetched, max_records, &paused);
            self.metrics.buffered_records.set(buffer.len() as u64);
        }
        if !prefetched.is_empty() {
            // Anything to deliver ends the poll, matching the Java client's
            // "return as soon as the buffer is non-empty". Fetching to top up a
            // partial batch would trade the latency win for throughput the next
            // poll will get anyway.
            drop(assignments);
            return self.finish_delivery(prefetched).await;
        }

        // Buffer cap: skip fetching when the buffer still holds too many
        // records the drain above could not deliver — in practice, records for
        // partitions that are paused. Auto-commit and rebalance handling above
        // still run so the consumer remains healthy in the group.
        let buffered_after_drain = self.recv_buffer.lock().len();
        if self.config.max_buffered_records > 0
            && buffered_after_drain >= self.config.max_buffered_records as usize
        {
            debug!(
                buffered = buffered_after_drain,
                max = self.config.max_buffered_records,
                "Buffer cap reached, skipping fetch"
            );
            self.metrics.empty_polls.inc();
            return Ok(Vec::new());
        }

        // Retry offset resolution for partitions that are missing tracked offsets.
        // This fulfils the "will retry on next poll" contract when initial offset
        // resolution fails (e.g., due to a transient ListOffsets error or a
        // rejoin that left some partitions without offsets).
        // Exponential backoff prevents retry storms under sustained failures.
        {
            let now = Instant::now();
            let missing: Vec<(String, PartitionId)> = {
                let offsets = self.offsets.read().await;
                let partition_state = self.partition_state.read().await;
                assignments
                    .iter()
                    .flat_map(|(topic, partitions)| {
                        partitions.iter().filter_map(|&p| {
                            let key = (topic.clone(), p);

                            if offsets.contains_key(&key) {
                                return None;
                            }

                            // Only include if backoff period has elapsed
                            match partition_state
                                .get(&key)
                                .and_then(|s| s.offset_retry_backoff)
                            {
                                None => Some(key),
                                Some((next_retry, _)) if now >= next_retry => Some(key),
                                _ => None,
                            }
                        })
                    })
                    .collect()
            };

            if !missing.is_empty() {
                debug!(
                    "Retrying offset resolution for {} partition(s) without tracked offsets",
                    missing.len()
                );

                let mut still_missing = missing;

                // For group consumers, always re-check committed offsets first.
                // Only partitions with no committed offset may fall back to
                // caller-supplied initial_offsets.
                let mut committed_fetch_failed = false;
                if let Some(ref coordinator) = self.group_coordinator {
                    let mut topic_map: HashMap<String, Vec<PartitionId>> = HashMap::new();
                    for (topic, partition) in &still_missing {
                        topic_map.entry(topic.clone()).or_default().push(*partition);
                    }

                    if !topic_map.is_empty() {
                        match coordinator.fetch_committed_offsets(&topic_map).await {
                            Ok(committed) => {
                                // Same KIP-320 reasoning as the rebalance path:
                                // the committed epoch has to be installed with
                                // the position it describes, or the first fetch
                                // from it cannot be checked for divergence.
                                {
                                    let mut partition_state = self.partition_state.write().await;
                                    for (key, position) in &committed {
                                        if position.leader_epoch >= 0 {
                                            partition_state
                                                .entry(key.clone())
                                                .or_default()
                                                .last_fetched_epoch = Some(position.leader_epoch);
                                        }
                                    }
                                }
                                let mut offsets = self.offsets.write().await;
                                still_missing.retain(|key| {
                                    if let Some(position) = committed.get(key)
                                        && position.offset >= 0
                                    {
                                        offsets.insert(key.clone(), position.offset);
                                        false
                                    } else {
                                        true
                                    }
                                });
                            }
                            Err(e) => {
                                warn!(
                                    "Retry committed-offset fetch failed; deferring offset resolution until retry: {}",
                                    e
                                );
                                committed_fetch_failed = true;
                            }
                        }
                    }
                }

                if !committed_fetch_failed {
                    if !self.config.initial_offsets.is_empty() {
                        let mut offsets = self.offsets.write().await;
                        still_missing.retain(|key| {
                            if let Some(&initial) = self.config.initial_offsets.get(key) {
                                debug!(
                                    "Poll retry: using initial_offsets {} for {}-{}",
                                    initial, key.0, key.1
                                );
                                offsets.insert(key.clone(), initial);
                                false
                            } else {
                                true
                            }
                        });
                    }

                    if still_missing.is_empty() {
                        self.recompute_lag_metrics().await;
                        return Ok(Vec::new());
                    }

                    let mut reset_partitions: HashMap<String, Vec<PartitionId>> = HashMap::new();
                    for (topic, partition) in &still_missing {
                        reset_partitions
                            .entry(topic.clone())
                            .or_default()
                            .push(*partition);
                    }

                    // Use group coordinator path if available, otherwise direct path
                    if let Some(ref coordinator) = self.group_coordinator {
                        if let Some(timestamp) = self.config.auto_offset_reset.to_offset() {
                            match coordinator.list_offsets(&reset_partitions, timestamp).await {
                                Ok(resolved) => {
                                    let mut offsets = self.offsets.write().await;
                                    for (key, offset) in &resolved {
                                        offsets.insert(key.clone(), *offset);
                                    }
                                    drop(offsets);

                                    // Fallback for partitions the coordinator path
                                    // silently dropped (partition-level errors).
                                    for (topic, partition) in &still_missing {
                                        if !resolved.contains_key(&(topic.clone(), *partition)) {
                                            debug!(
                                                "Poll retry: falling back to direct ListOffsets for {}-{}",
                                                topic, partition
                                            );
                                            if let Ok(offset) = self
                                                .resolve_list_offset(topic, *partition, timestamp)
                                                .await
                                            {
                                                let mut offsets = self.offsets.write().await;
                                                offsets.insert((topic.clone(), *partition), offset);
                                            }
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("Offset resolution retry via coordinator failed: {}", e);
                                    // Fall back to direct path for all still-missing partitions
                                    for (topic, partition) in &still_missing {
                                        if let Ok(offset) = self
                                            .resolve_list_offset(topic, *partition, timestamp)
                                            .await
                                        {
                                            let mut offsets = self.offsets.write().await;
                                            offsets.insert((topic.clone(), *partition), offset);
                                        }
                                    }
                                }
                            }
                        }
                    } else if let Err(e) = self.apply_auto_offset_reset(&reset_partitions).await {
                        warn!("Auto-offset-reset failed for missing partitions: {e}");
                    }
                }

                // Recompute lag after resolving offsets for missing partitions
                self.recompute_lag_metrics().await;

                // Apply exponential backoff for partitions that are still
                // unresolved after the retry attempt. Clear backoff for
                // partitions that were successfully resolved.
                {
                    let offsets = self.offsets.read().await;
                    let mut partition_state = self.partition_state.write().await;
                    for (topic, partition) in &still_missing {
                        let key = (topic.clone(), *partition);
                        if offsets.contains_key(&key) {
                            // Successfully resolved — clear backoff.
                            if let Some(state) = partition_state.get_mut(&key) {
                                state.offset_retry_backoff = None;
                            }
                        } else {
                            // Still unresolved — compute next backoff interval.
                            // Start at 100ms, double each time, cap at 30s.
                            let base = Duration::from_millis(100);
                            let max = Duration::from_secs(30);
                            let entry = partition_state.entry(key).or_default();
                            let prev_wait = entry
                                .offset_retry_backoff
                                .map(|(_, d)| d)
                                .unwrap_or(Duration::ZERO);
                            let next_wait = (prev_wait * 2).max(base).min(max);
                            entry.offset_retry_backoff =
                                Some((Instant::now() + next_wait, next_wait));
                        }
                    }
                }
            }
        }

        let paused = self.paused.read().await;

        // Collect non-paused partition keys (one topic clone per partition)
        // and resolve leaders so the pure routing helper doesn't need async
        // metadata access.
        let mut non_paused_keys: Vec<(String, PartitionId)> = Vec::new();
        let mut leaders: HashMap<(String, PartitionId), crate::BrokerId> = HashMap::new();
        for (topic, partitions) in assignments.iter() {
            for &partition in partitions {
                let key = (topic.clone(), partition);
                if paused.contains(&key) {
                    continue;
                }
                if let Some(leader_id) = self.metadata.leader(topic, partition) {
                    leaders.insert(key.clone(), leader_id);
                }
                non_paused_keys.push(key);
            }
        }

        // Rotate the fetch order by one partition per poll.
        //
        // Both `fetch.max.bytes` on the broker side and `max_poll_records` on
        // the client side are consumed in the order partitions appear, so a
        // fixed order lets the partitions at the front monopolise the response
        // and starves the tail indefinitely. The previous order came from
        // `HashMap` iteration, which is unspecified — fairness was an accident
        // of the per-map hash seed rather than a property of the code.
        //
        // Sorting first makes the order deterministic so the rotation is a real
        // round robin (the Java client's `PartitionStates.moveToEnd`); the sort
        // is over the assigned-partition count, which is orders of magnitude
        // smaller than the record count it protects.
        non_paused_keys.sort_unstable();
        if !non_paused_keys.is_empty() {
            let turn = self
                .fetch_rotation
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let offset = turn % non_paused_keys.len();
            non_paused_keys.rotate_left(offset);
        }

        let now = Instant::now();
        let partition_state_read = self.partition_state.read().await;

        let plan = build_fetch_routing_plan(non_paused_keys, &partition_state_read, &leaders, now);

        // Release read lock before potentially acquiring write lock
        drop(partition_state_read);

        // Warn only for partitions that are truly skipped (no leader AND no
        // valid preferred replica). This avoids log spam during transient
        // metadata gaps when a preferred replica is still available.
        for (topic, partition) in &plan.skipped {
            warn!(
                "No leader or preferred replica for {topic}-{partition}, skipping in batch fetch"
            );
        }

        // Clear expired preferred-replica entries so they don't accumulate.
        // Only the `preferred_replica` field is cleared — other per-partition
        // caches (high watermark, log start offset, retry backoff) are kept.
        if !plan.expired_preferred.is_empty() {
            let mut partition_state = self.partition_state.write().await;
            for key in &plan.expired_preferred {
                if let Some(state) = partition_state.get_mut(key) {
                    state.preferred_replica = None;
                }
            }
        }

        drop(paused);
        drop(assignments);

        // Confirm that every position about to be fetched from actually
        // exists in its leader's log before asking for records from it.
        // Partitions that were validated earlier and have only moved forward
        // through consumed batches are skipped, so this costs one round trip
        // per repositioning, not one per poll.
        {
            let to_validate: Vec<(String, PartitionId)> = plan
                .partitions_by_broker
                .values()
                .flat_map(|keys| keys.iter().cloned())
                .collect();
            self.validate_pending_positions(&to_validate).await;
        }

        let deadline = Instant::now() + timeout;
        let mut all_records = Vec::new();
        let mut all_offset_updates: Vec<((String, PartitionId), FetchOffsetUpdate)> = Vec::new();
        let mut all_hw_updates: Vec<((String, PartitionId), Offset)> = Vec::new();
        let mut all_faults: Vec<PartitionFetchFault> = Vec::new();

        // Shared decode budget for this poll: what this poll will deliver, plus
        // what it will park for the next one.
        //
        // A fetch response may carry up to `fetch_max_bytes` (50 MB by default)
        // while `max_poll_records` (500) caps what a single poll may return.
        // Decoding the whole response and truncating threw the surplus away —
        // every record allocated, headers built, key and value sliced — only to
        // re-fetch and re-decode the same bytes next poll. With 1 KiB records
        // that is up to ~100× the necessary CPU per poll, all of it garbage.
        //
        // Decoding exactly `max_poll_records` would fix the waste but leave
        // every poll paying a network round trip. Decoding one delivery's worth
        // *plus* the buffer's free capacity fixes both: the surplus is parked in
        // `recv_buffer`, and the next poll returns it without touching the
        // network. Nothing is dropped, so the position updates below need no
        // clamping — `committable_positions` holds the commit behind whatever is
        // still parked.
        //
        // `max_poll_records == -1` means unlimited and disables the budget.
        let record_budget: Option<Arc<std::sync::atomic::AtomicUsize>> =
            if max_records == usize::MAX {
                None
            } else {
                let prefetch_headroom = if self.config.max_buffered_records > 0 {
                    (self.config.max_buffered_records as usize).saturating_sub(buffered_after_drain)
                } else {
                    // Unlimited buffer still pipelines only one poll ahead; an
                    // unbounded budget would restore the waste this exists to avoid.
                    max_records
                };
                Some(Arc::new(std::sync::atomic::AtomicUsize::new(
                    max_records.saturating_add(prefetch_headroom),
                )))
            };

        // Client-side long poll.
        //
        // Each broker request parks for at most `fetch_max_wait`, which is
        // independent of (and normally far shorter than) the caller's timeout,
        // so a single round rarely uses the whole budget. Looping here is what
        // turns those short broker-side waits into the long poll the caller
        // asked for, without ever asking a broker to hold a request longer
        // than the connection layer is willing to wait for it.
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let max_wait = self.config.fetch_max_wait.min(remaining);

            // Issue every broker's fetch concurrently against one shared
            // deadline. Sequential per-broker fetches each waiting up to the
            // full budget would let a single poll() block for N times the
            // requested timeout on an N-broker cluster, starving the inline
            // heartbeat and blowing through max.poll.interval.ms.
            let budget = record_budget.as_deref();
            let fetches =
                plan.partitions_by_broker
                    .iter()
                    .map(|(broker_id, topic_partitions)| async move {
                        let result = self
                            .batch_fetch_from_broker(*broker_id, topic_partitions, max_wait, budget)
                            .await;
                        (*broker_id, topic_partitions, result)
                    });

            // `wakeup()` must interrupt a poll that is already parked on the
            // brokers, not merely the next one. Records already collected by
            // earlier loop iterations are returned rather than discarded — an
            // interrupted poll that threw away fetched records would move
            // offsets forward with nothing delivered.
            let fetch_results = tokio::select! {
                biased;
                () = self.wakeup_notify.notified() => {
                    self.wakeup_flag
                        .store(false, std::sync::atomic::Ordering::Release);
                    if all_records.is_empty() {
                        return Err(KrafkaError::invalid_state("wakeup() was called"));
                    }
                    break;
                }
                results = futures::future::join_all(fetches) => results,
            };

            for (broker_id, topic_partitions, result) in fetch_results {
                match result {
                    Ok(outcome) => {
                        all_records.extend(outcome.records);
                        all_offset_updates.extend(outcome.offset_updates);
                        all_hw_updates.extend(outcome.hw_updates);
                        all_faults.extend(outcome.faults);
                    }
                    Err(e) => {
                        self.metrics.record_error();
                        warn!("Batch fetch from broker {} failed: {}", broker_id, e);
                        // Clear preferred replica mappings for all partitions that
                        // were being fetched from this broker.  If the broker was
                        // actually the leader the entries won't exist (no-op), but
                        // if it was a preferred replica this avoids routing to a
                        // dead broker for up to metadata_max_age.
                        let mut partition_state = self.partition_state.write().await;
                        for tp in topic_partitions {
                            if let Some(state) = partition_state.get_mut(tp) {
                                state.preferred_replica = None;
                            }
                        }
                    }
                }
            }

            // Return as soon as there is anything to deliver; only an empty
            // round is worth retrying. A known-stuck partition also ends the
            // loop: re-fetching it re-reads the same corrupt bytes, so looping
            // to the deadline would burn the whole poll budget to learn
            // nothing new.
            if !all_records.is_empty() || !all_faults.is_empty() || Instant::now() >= deadline {
                break;
            }
        }

        // Surface partition-level decode faults before anything is mutated.
        //
        // Position is advanced at the very end of this function, so returning
        // here leaves every partition exactly where it was: the records
        // collected this round are simply re-fetched, and nothing is skipped.
        // That ordering is what makes it safe to fail the poll rather than
        // quietly drop the fault on the floor — which is what the previous
        // `debug!`-and-continue did, and why a corrupt partition could stall
        // forever without the application ever being told.
        //
        // The application is not left without recourse: `pause()` on the named
        // partition excludes it at request-build time, so every other
        // partition keeps flowing while the corruption is investigated.
        if !all_faults.is_empty() {
            let total = all_faults.len();
            self.metrics.record_error();
            if let Some(fault) = all_faults.into_iter().next() {
                return Err(fault.into_error(total));
            }
        }

        // Split what was fetched into what this poll delivers and what is parked
        // for the next one.
        //
        // Two reasons a record is parked rather than delivered: its partition
        // was paused after the fetch was issued (filtering only at request-build
        // time is not enough — a fetch already in flight still returns data),
        // or the delivery cap is full.
        //
        // Nothing is dropped, which is what lets the position updates below go
        // in unclamped: every fetched record is either handed to the caller now
        // or sitting in `recv_buffer`, and `committable_positions` holds the
        // committed offset behind anything still parked. Per-partition order
        // survives the split because `all_records` is in fetch order and both
        // halves preserve it.
        let (mut delivered, held) = {
            let paused = self.paused.read().await;
            let mut delivered: Vec<ConsumerRecord> =
                Vec::with_capacity(all_records.len().min(max_records));
            let mut held: Vec<ConsumerRecord> = Vec::new();
            for record in all_records {
                let is_paused = contains_partition(&paused, &record.topic, record.partition);
                if !is_paused && delivered.len() < max_records {
                    delivered.push(record);
                } else {
                    held.push(record);
                }
            }
            (delivered, held)
        };

        // Update high watermarks
        let hw_changed = !all_hw_updates.is_empty();
        if hw_changed {
            let now = Instant::now();
            let mut partition_state = self.partition_state.write().await;
            for (key, watermark) in all_hw_updates {
                let s = partition_state.entry(key).or_default();
                s.high_watermark = Some(watermark);
                s.watermark_updated_at = Some(now);
            }
        }

        // Recompute lag metrics whenever watermarks changed. This runs before
        // the position update below, so the reported lag can be one poll
        // behind; lag is documented as eventually consistent and this ordering
        // is what keeps the position update free of any trailing await.
        if hw_changed {
            self.recompute_lag_metrics().await;
        }

        // Advance the fetch position and park the surplus last, with nothing
        // awaited between the mutations and the return.
        //
        // This function can be dropped at any await point — a
        // `tokio::time::timeout` firing, or a losing branch of a `select!`. If
        // the position were advanced earlier, such a cancellation would discard
        // the records while leaving the position past them, and the next
        // auto-commit would make that skip permanent.
        //
        // The position update and the buffer push have to be one indivisible
        // step for the same reason: a cancellation between them would either
        // advance past records nobody holds (loss) or park records the position
        // has not passed (duplicates). All three locks are therefore taken
        // *before* any mutation, and there is no `.await` from that point on —
        // so the whole block either happens or does not.
        if !all_offset_updates.is_empty() || !held.is_empty() {
            let epochs: Vec<((String, PartitionId), i32)> = all_offset_updates
                .iter()
                .map(|(key, update)| (key.clone(), update.epoch))
                .collect();

            let mut offsets = self.offsets.write().await;
            let mut partition_state = self.partition_state.write().await;
            let mut buffer = self.recv_buffer.lock();
            // ── no `.await` beyond this point ──
            let discarded = apply_fetch_offset_updates(&mut offsets, all_offset_updates);
            let discarded_set: HashSet<&(String, PartitionId)> = discarded.iter().collect();
            for (key, epoch) in &epochs {
                if discarded_set.contains(key) {
                    continue;
                }
                // The epoch belongs to the position that was just applied, so
                // it is only stored when that position survived the staleness
                // check. `-1` means the response carried nothing that pins the
                // epoch down; keep the previous value rather than blinding the
                // next divergence check.
                if *epoch >= 0 {
                    partition_state
                        .entry(key.clone())
                        .or_default()
                        .last_fetched_epoch = Some(*epoch);
                }
            }

            // A stale response's records describe a position the consumer has
            // deliberately moved away from, so they must neither be delivered
            // nor parked — returning them would hand the application records
            // from before a `seek()` it explicitly asked to skip past, and
            // parking them would do the same one poll later.
            let mut held = held;
            if !discarded.is_empty() {
                let stale: HashSet<(String, PartitionId)> =
                    discarded_set.iter().map(|k| (*k).clone()).collect();
                for (topic, partition) in &stale {
                    debug!(
                        topic = %topic,
                        partition,
                        "Discarding fetch response: position changed while the fetch was in flight"
                    );
                }
                delivered.retain(|r| !contains_partition(&stale, &r.topic, r.partition));
                held.retain(|r| !contains_partition(&stale, &r.topic, r.partition));
            }

            buffer.extend(held);
            self.metrics.buffered_records.set(buffer.len() as u64);
        }

        self.finish_delivery(delivered).await
    }

    /// Apply everything that happens to a batch of records on its way out of
    /// `poll()`: deserialization, metrics, and the consumer interceptor.
    ///
    /// Shared by both delivery paths — records served straight from the
    /// prefetch buffer and records just fetched — so a record is counted,
    /// intercepted and deserialized exactly once, at the moment it reaches the
    /// application, regardless of which poll actually fetched it.
    ///
    /// # Ordering
    ///
    /// Deserialization runs **before** the interceptor and the metrics, which
    /// is the order the Java client uses (`Fetcher` deserializes, then
    /// `ConsumerInterceptor::onConsume` sees the result) and the mirror image
    /// of the producer, where the interceptor sees the record before
    /// serialization. An interceptor therefore always observes
    /// application-level values on both sides, not wire bytes on one and
    /// values on the other.
    ///
    /// # Failure
    ///
    /// A deserializer that rejects a record is *not* allowed to lose it. The
    /// fetch position was advanced before this function was called, so
    /// dropping the batch here would skip every record in it permanently.
    /// Instead the whole batch is pushed back to the front of the receive
    /// buffer — where [`committable_positions`] holds the committed offset
    /// behind it — and the error names the exact record, so the caller can
    /// `seek()` one past it to make progress.
    async fn finish_delivery(
        &self,
        mut records: Vec<ConsumerRecord>,
    ) -> Result<Vec<ConsumerRecord>> {
        if records.is_empty() {
            self.metrics.empty_polls.inc();
            return Ok(records);
        }

        if self.key_deserializer.is_some() || self.value_deserializer.is_some() {
            for index in 0..records.len() {
                if let Err(error) = self.deserialize_in_place(&mut records[index]).await {
                    self.requeue_undelivered(records);
                    self.metrics.record_error();
                    return Err(error);
                }
            }
        }

        let bytes: u64 = records
            .iter()
            .map(|r| r.value.as_ref().map(|v| v.len() as u64).unwrap_or(0))
            .sum();
        self.metrics.record_receive(records.len() as u64, bytes);

        crate::interceptor::safe_on_consume(&*self.interceptor, &records);

        Ok(records)
    }

    /// Run the configured deserializers over one record, in place.
    ///
    /// The record is left untouched when either half fails, so the caller can
    /// put the batch back exactly as it was fetched and a later retry sees the
    /// same bytes rather than a half-decoded record.
    async fn deserialize_in_place(&self, record: &mut ConsumerRecord) -> Result<()> {
        if let (Some(decoder), Some(value)) = (&self.value_deserializer, record.value.as_ref()) {
            let decoded = decoder
                .deserialize(value.clone(), &record.topic, false)
                .await
                .map_err(|e| {
                    KrafkaError::record_deserialization(
                        &record.topic,
                        record.partition,
                        record.offset,
                        "value",
                        e.to_string(),
                    )
                })?;
            record.value = Some(decoded);
        }
        if let (Some(decoder), Some(key)) = (&self.key_deserializer, record.key.as_ref()) {
            let decoded = decoder
                .deserialize(key.clone(), &record.topic, true)
                .await
                .map_err(|e| {
                    KrafkaError::record_deserialization(
                        &record.topic,
                        record.partition,
                        record.offset,
                        "key",
                        e.to_string(),
                    )
                })?;
            record.key = Some(decoded);
        }
        Ok(())
    }

    /// Put records that were taken out of the pipeline but never handed to the
    /// application back at the **front** of the receive buffer.
    ///
    /// The front matters: these offsets are lower than anything the same poll
    /// parked at the back, so appending them would deliver a partition's
    /// records out of order on the next drain.
    fn requeue_undelivered(&self, records: Vec<ConsumerRecord>) {
        if records.is_empty() {
            return;
        }
        let mut buffer = self.recv_buffer.lock();
        for record in records.into_iter().rev() {
            buffer.push_front(record);
        }
        self.metrics.buffered_records.set(buffer.len() as u64);
    }

    /// Batch fetch from a single broker for multiple topic-partitions.
    ///
    /// This is more efficient than individual fetches because it sends a single
    /// network request for all partitions led by the same broker.
    ///
    /// `max_wait` is the broker-side `max_wait_ms` — how long the broker may
    /// hold the request waiting for `fetch_min_bytes`. It is deliberately not
    /// the caller's poll timeout; see [`ConsumerConfig::fetch_max_wait`].
    ///
    /// Each returned offset update carries the offset the fetch was *issued
    /// from* so the caller can drop updates that a concurrent `seek()`
    /// invalidated.
    ///
    /// `record_budget`, when present, is the number of records this whole poll
    /// may still decode. It is shared with the fetches running concurrently
    /// against the other brokers, and it is what keeps a 50 MB response from
    /// being fully decoded just to have all but `max_poll_records` of it
    /// discarded.
    async fn batch_fetch_from_broker(
        &self,
        broker_id: crate::BrokerId,
        topic_partitions: &[(String, PartitionId)],
        max_wait: Duration,
        record_budget: Option<&std::sync::atomic::AtomicUsize>,
    ) -> Result<FetchOutcome> {
        if topic_partitions.is_empty() {
            return Ok(FetchOutcome::default());
        }

        self.metrics.record_fetch();
        let _fetch_timer = self.metrics.fetch_latency.start();

        // Get connection to this broker. A leader learned from a fetch
        // response (KIP-951) can be a broker the metadata cache has not seen
        // yet, so fall back to the endpoint the broker advertised.
        let address = self
            .broker_address(broker_id)
            .ok_or_else(|| KrafkaError::invalid_state(format!("broker {} not found", broker_id)))?;
        let conn = self.pool.get_connection_by_id(broker_id, &address).await?;

        // Group by topic for the request structure
        let mut topics_map: HashMap<String, Vec<PartitionId>> = HashMap::new();
        for (topic, partition) in topic_partitions {
            topics_map
                .entry(topic.clone())
                .or_default()
                .push(*partition);
        }

        // Build fetch request with all topic-partitions.
        // Acquire the offsets read lock once for the entire build instead of
        // per-partition to reduce lock acquire/release overhead.
        let offsets_snapshot = self.offsets.read().await;
        // Leader epochs of the batches each partition last advanced through.
        // Sent as `last_fetched_epoch` so the broker can tell us when our log
        // no longer matches its own (KIP-320).
        let last_fetched_epochs: HashMap<(String, PartitionId), i32> = {
            let partition_state = self.partition_state.read().await;
            topic_partitions
                .iter()
                .filter_map(|key| {
                    partition_state
                        .get(key)
                        .and_then(|s| s.last_fetched_epoch)
                        .map(|epoch| (key.clone(), epoch))
                })
                .collect()
        };
        // The position each partition is being fetched from, captured once at
        // request-build time. Reading it again after the response has arrived
        // would defeat the staleness check: by then a concurrent `seek()` may
        // already have moved the position, and comparing the response against
        // the moved value would make the stale response look current.
        let mut requested_offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        let mut fetch_topics = Vec::with_capacity(topics_map.len());
        for (topic, partitions) in &topics_map {
            let mut fetch_partitions = Vec::with_capacity(partitions.len());
            for &partition in partitions {
                // Skip partitions with no tracked offset rather than
                // defaulting to 0, which defeats the auto_offset_reset fix.
                let offset = match offsets_snapshot.get(&(topic.clone(), partition)).copied() {
                    Some(o) => o,
                    None => {
                        warn!(
                            "No offset for {}-{}, skipping fetch (will retry offset resolution)",
                            topic, partition
                        );
                        continue;
                    }
                };
                requested_offsets.insert((topic.clone(), partition), offset);
                // Get leader epoch from metadata for fencing stale reads
                let leader_epoch = self.metadata.leader_epoch(topic, partition).unwrap_or(-1);
                fetch_partitions.push(FetchPartitionRequest {
                    partition,
                    current_leader_epoch: leader_epoch,
                    fetch_offset: offset,
                    // -1 disables divergence detection and is only correct
                    // when the epoch at this position is genuinely unknown.
                    last_fetched_epoch: last_fetched_epochs
                        .get(&(topic.clone(), partition))
                        .copied()
                        .unwrap_or(-1),
                    log_start_offset: -1,
                    partition_max_bytes: self
                        .config
                        .topic_fetch_max_bytes
                        .get(topic.as_str())
                        .copied()
                        .unwrap_or(self.config.max_partition_fetch_bytes),
                    replica_directory_id: None,
                    high_watermark: None,
                });
            }
            fetch_topics.push(FetchTopicRequest {
                topic: topic.clone(),
                topic_id: None,
                partitions: fetch_partitions,
            });
        }
        // Drop the read lock before the network call.
        drop(offsets_snapshot);

        // Negotiate fetch API version — prefer FETCH_MAX and fall back
        // gracefully.  Key milestones:
        //   v7  — incremental fetch sessions (KIP-227)
        //   v9  — current_leader_epoch fencing (KIP-320)
        //   v11 — rack_id for closest-replica routing (KIP-392)
        //   v13 — topic UUIDs replace topic names (KIP-516)
        //   v15 — remove ReplicaId from header (KIP-903)
        //   v17 — per-partition ReplicaDirectoryId tagged field (KIP-853)
        //   v18 — per-partition HighWatermark tagged field (KIP-1166)
        //
        // v17 and v18 only add *follower*-populated tagged fields, so a
        // consumer request at those versions is byte-identical to v16 apart
        // from the version number, and the response format is unchanged from
        // v13. Negotiating them costs nothing and keeps the client from being
        // reported as stale in broker-side client-version metrics.
        let mut fetch_version = conn
            .negotiate_api_version(ApiKey::Fetch, versions::FETCH_MAX, 7)
            .unwrap_or_else(|| {
                debug!(
                    "No mutually supported Fetch v7+ for broker {broker_id}, falling back to v4"
                );
                4
            });

        // KIP-516: Fetch v13+ sends topic UUIDs instead of names.
        // Fill in UUIDs from the metadata cache; cap to v12 if any are missing.
        if fetch_version >= 13 {
            let all_resolved = fetch_topics.iter_mut().all(|t| {
                if let Some(id) = self.metadata.topic_id_for_name(&t.topic) {
                    t.topic_id = Some(id);
                    true
                } else {
                    false
                }
            });
            if !all_resolved {
                fetch_version = 12;
            }
        }

        // When the broker supports v11+ but the client has no rack preference,
        // there is no functional reason to send rack_id; cap at v12 if rack
        // is unset and we would otherwise use v13+ (avoids accidental UUID
        // sends when IDs are not cached).  If rack IS set, v11+ is correct.
        // Note: v13+ requires rack-unrelated UUID support so we only impose
        // the rack cap on v11-v12 when NOT already constrained by UUID cache.
        // (Above UUID fallback already handles v13+ → v12 correctly.)
        // No additional cap needed here.

        // Build the fetch request. For v7, compute an incremental session diff
        // from fetch_topics without cloning the full topic list into the base request.
        let (session_id, session_epoch, request_topics, forgotten_topics) = if fetch_version >= 7 {
            let mut sessions = self.fetch_sessions.lock();
            let session = sessions.get_or_create(broker_id);
            let session_req = session.build_request(&fetch_topics);
            if session_req.is_full_fetch {
                debug!(
                    "Fetch broker {}: full fetch (session_id={}, epoch={})",
                    broker_id, session_req.session_id, session_req.session_epoch
                );
            } else {
                debug!(
                    "Fetch broker {}: incremental (session_id={}, epoch={}, changed={}, forgotten={})",
                    broker_id,
                    session_req.session_id,
                    session_req.session_epoch,
                    session_req.topics.len(),
                    session_req.forgotten_topics.len()
                );
            }
            (
                session_req.session_id,
                session_req.session_epoch,
                {
                    // KIP-516: fill topic_id for any topics sent to broker on v13+.
                    // The session builds diffs with topic_id: None; back-fill from cache.
                    let mut topics = session_req.topics;
                    if fetch_version >= 13 {
                        for t in &mut topics {
                            if t.topic_id.is_none() {
                                t.topic_id = self.metadata.topic_id_for_name(&t.topic);
                            }
                        }
                    }
                    topics
                },
                {
                    let mut forgotten = session_req.forgotten_topics;
                    if fetch_version >= 13 {
                        for t in &mut forgotten {
                            if t.topic_id.is_none() {
                                t.topic_id = self.metadata.topic_id_for_name(&t.topic);
                            }
                        }
                    }
                    forgotten
                },
            )
        } else {
            // v4: move fetch_topics into the request; update_from_response
            // is only called for v7+ so fetch_topics is not needed later.
            (0, -1, std::mem::take(&mut fetch_topics), Vec::new())
        };

        let request = FetchRequest {
            replica_id: -1, // Consumer
            max_wait_ms: crate::util::duration_to_millis_i32(max_wait),
            min_bytes: self.config.fetch_min_bytes,
            max_bytes: self.config.fetch_max_bytes,
            isolation_level: self.config.isolation_level.to_i8(),
            session_id,
            session_epoch,
            topics: request_topics,
            forgotten_topics,
            rack_id: self.config.client_rack.clone().unwrap_or_default(),
        };

        // Send request with negotiated version.
        // For v7+ sessions, reset session on any send/decode failure so the
        // next poll re-establishes with a full fetch instead of hitting
        // InvalidFetchSessionEpoch.
        let response = match conn
            .send_request(ApiKey::Fetch, fetch_version, |buf| {
                request.encode_versioned(fetch_version, buf)
            })
            .await
        {
            Ok(r) => r,
            Err(e) => {
                if fetch_version >= 7 {
                    let mut sessions = self.fetch_sessions.lock();
                    sessions.reset_broker(broker_id);
                }
                return Err(e);
            }
        };

        // Decode response with matching version
        let mut buf = response;
        let mut fetch_response = match FetchResponse::decode_versioned(fetch_version, &mut buf) {
            Ok(r) => r,
            Err(e) => {
                if fetch_version >= 7 {
                    let mut sessions = self.fetch_sessions.lock();
                    sessions.reset_broker(broker_id);
                }
                return Err(e);
            }
        };

        // KIP-219: honour broker-reported throttle time.
        conn.notify_throttle(fetch_response.throttle_time_ms);

        // KIP-516: For Fetch v13+, response topics carry a UUID but no name.
        // Resolve each UUID back to the topic name so the rest of the pipeline
        // can treat all versions uniformly.
        if fetch_version >= 13 {
            // A response whose UUID cannot be mapped back to a name is
            // *removed*, not merely logged. Leaving it in place kept an empty
            // topic name on every downstream key, so a partition's watermark,
            // log-start offset and preferred replica were recorded under
            // `("", partition)` — state that belongs to no topic, is never
            // read back, and collides across topics.
            fetch_response.responses.retain_mut(|topic_response| {
                if !topic_response.topic.is_empty() {
                    return true;
                }
                let Some(id) = topic_response.topic_id else {
                    warn!("Received FetchResponse v13+ with neither a topic name nor a topic_id");
                    return false;
                };
                match self.metadata.topic_name_for_id(&id) {
                    Some(name) => {
                        topic_response.topic = name;
                        true
                    }
                    None => {
                        warn!(
                            "Received FetchResponse v13+ with unknown topic_id {:?}; \
                             discarding its partitions (metadata will refresh)",
                            id
                        );
                        false
                    }
                }
            });
        }

        // Handle top-level session errors (v7+)
        if fetch_version >= 7 {
            if fetch_response.error_code == crate::error::ErrorCode::FetchSessionIdNotFound
                || fetch_response.error_code == crate::error::ErrorCode::InvalidFetchSessionEpoch
            {
                // Reset session and let the next poll do a full fetch
                warn!(
                    "Fetch session error for broker {}: {:?}, resetting session",
                    broker_id, fetch_response.error_code
                );
                let mut sessions = self.fetch_sessions.lock();
                sessions.reset_broker(broker_id);
                return Ok(FetchOutcome::default());
            }

            // Update session state from response
            let mut sessions = self.fetch_sessions.lock();
            let session = sessions.get_or_create(broker_id);
            session.update_from_response(fetch_response.session_id, &fetch_topics);
        }

        // Process records
        let mut records = Vec::new();
        let mut offset_updates: Vec<((String, PartitionId), FetchOffsetUpdate)> = Vec::new();
        let mut hw_updates: Vec<((String, PartitionId), Offset)> = Vec::new();
        let mut log_start_updates: Vec<((String, PartitionId), Offset)> = Vec::new();
        // Last stable offset (Fetch v4+).
        let mut stable_updates: Vec<((String, PartitionId), Offset)> = Vec::new();
        // Partitions that could not decode at their current position. Collected
        // rather than returned as `Err` so one corrupt partition cannot silence
        // the others sharing this broker.
        let mut faults: Vec<PartitionFetchFault> = Vec::new();

        // Preferred replica updates (KIP-392): Some(id) to set, None to clear.
        // Collected during the loop, applied in a single write lock afterwards.
        let mut pref_updates: Vec<((String, PartitionId), Option<crate::BrokerId>)> = Vec::new();

        // Addresses the broker advertised for the leaders it names below
        // (KIP-951). Passing one along with a hint is what makes a freshly
        // promoted broker reachable before metadata has caught up.
        let node_endpoints = std::mem::take(&mut fetch_response.node_endpoints);

        for topic_response in fetch_response.responses {
            let topic_name = &topic_response.topic;
            for partition_response in topic_response.partitions {
                let partition = partition_response.partition;
                let key = (topic_name.clone(), partition);

                // Capture high watermark regardless of error/empty response.
                // The broker always returns a valid high_watermark even when
                // there are no records to deliver.
                if partition_response.high_watermark >= 0 {
                    hw_updates.push((key.clone(), partition_response.high_watermark));
                }

                // Cache log_start_offset (earliest available offset) when
                // present. Returned in Fetch v5+; allows `cached_beginning_offset`
                // to serve beginning offsets from cache without a network round-trip.
                if partition_response.log_start_offset >= 0 {
                    log_start_updates.push((key.clone(), partition_response.log_start_offset));
                }

                // Cache the last stable offset (Fetch v4+). This is the read
                // ceiling under `read_committed`, so it is what lag and
                // caught-up checks measure against there — see
                // `PartitionState::readable_end_offset`. Recorded regardless of
                // error/empty response for the same reason as the high
                // watermark: the broker reports it either way.
                if partition_response.last_stable_offset >= 0 {
                    stable_updates.push((key.clone(), partition_response.last_stable_offset));
                }

                // Track preferred read replica (KIP-392, v11+ only).
                // For v7-v10, preferred_read_replica is our fabricated default
                // (-1) and must not clear valid mappings from earlier v11 responses.
                if fetch_version >= 11 {
                    if partition_response.preferred_read_replica >= 0 {
                        pref_updates
                            .push((key.clone(), Some(partition_response.preferred_read_replica)));
                    } else {
                        pref_updates.push((key.clone(), None));
                    }
                }

                // The broker compared the (position, epoch) pair we sent with
                // its own log and found they diverge: everything at or after
                // `end_offset` in our view of the partition was never part of
                // the current leader's log. Continuing from the old position
                // would deliver records that no longer exist upstream and skip
                // the ones that replaced them.
                if let Some(diverging) = partition_response.diverging_epoch {
                    self.truncate_to_diverging_offset(topic_name, partition, diverging)
                        .await;
                    continue;
                }

                if !partition_response.error_code.is_ok() {
                    // When fetching from a preferred replica and the broker
                    // returns an error, clear the preferred replica so the
                    // next poll falls back to the partition leader.  We also
                    // clear when leader metadata is unavailable (None) to
                    // avoid getting stuck routing to a failing replica until
                    // expiry.  This is not gated on fetch_version >= 11
                    // because a stale preferred mapping from an earlier v11
                    // response can still route fetches to this broker even
                    // when the negotiated version is lower (e.g. rolling
                    // upgrade).
                    let is_leader = self
                        .metadata
                        .leader(topic_name, partition)
                        .is_some_and(|leader_id| leader_id == broker_id);
                    if !is_leader {
                        debug!(
                            "Error from non-leader broker {} for {}-{}: {:?}, clearing preferred replica",
                            broker_id, topic_name, partition, partition_response.error_code
                        );
                        pref_updates.push((key.clone(), None));
                    }

                    // The broker that rejected this fetch also told us who
                    // should have received it (KIP-951). Folding it into the
                    // metadata cache now saves a refresh on every leader
                    // failover; without it the partition stalls for a full
                    // refresh cycle.
                    if matches!(
                        partition_response.error_code,
                        crate::error::ErrorCode::NotLeaderForPartition
                            | crate::error::ErrorCode::FencedLeaderEpoch
                    ) && let Some(leader) = partition_response.current_leader
                    {
                        debug!(
                            "Broker {} reports {}-{} now led by node {} (epoch {})",
                            broker_id, topic_name, partition, leader.leader_id, leader.leader_epoch
                        );
                        self.metadata.apply_leader_hint(
                            topic_name,
                            partition,
                            leader.leader_id,
                            leader.leader_epoch,
                            broker_info_for_node(&node_endpoints, leader.leader_id),
                        );
                    }

                    // Handle leader epoch errors by validating via OffsetForLeaderEpoch
                    if partition_response.error_code == crate::error::ErrorCode::FencedLeaderEpoch
                        || partition_response.error_code
                            == crate::error::ErrorCode::UnknownLeaderEpoch
                    {
                        warn!(
                            "Leader epoch error for {}-{}: {:?}, validating offset via OffsetForLeaderEpoch",
                            topic_name, partition, partition_response.error_code
                        );
                        // Trigger metadata refresh and reset offset if truncation detected.
                        // On validation failure (e.g. network error), fall back to
                        // auto_offset_reset so the consumer does not get stuck on a
                        // potentially truncated partition.
                        if let Err(e) = self
                            .validate_offset_for_leader_epoch(topic_name, partition)
                            .await
                        {
                            warn!(
                                "OffsetForLeaderEpoch validation failed for {}-{}: {}, \
                                 falling back to auto_offset_reset",
                                topic_name, partition, e
                            );
                            self.handle_offset_out_of_range(topic_name, partition).await;
                        }
                    } else if partition_response.error_code
                        == crate::error::ErrorCode::OffsetOutOfRange
                    {
                        warn!(
                            "OffsetOutOfRange for {}-{}, applying auto_offset_reset",
                            topic_name, partition
                        );
                        self.handle_offset_out_of_range(topic_name, partition).await;
                    } else {
                        warn!(
                            "Fetch error for {}-{}: {:?}",
                            topic_name, partition, partition_response.error_code
                        );
                    }
                    continue; // Continue with other partitions
                }

                if let Some(record_bytes) = partition_response.records {
                    // Offset this partition was actually fetched from — used to
                    // skip records already delivered in a prior poll, since
                    // Kafka returns whole batches that may start earlier.
                    //
                    // This is the value captured when the request was built,
                    // not a fresh read: re-reading here would pick up any
                    // concurrent `seek()` and silently reinterpret this
                    // response as if it had been requested from the new
                    // position.
                    let partition_fetch_offset = match requested_offsets.get(&key).copied() {
                        Some(offset) => offset,
                        None => {
                            debug!(
                                topic = %topic_name,
                                partition,
                                "Received records for a partition that was not requested, ignoring"
                            );
                            continue;
                        }
                    };

                    // Stop before decoding anything when this poll has already
                    // filled `max_poll_records`. The partition keeps its
                    // position, so the bytes are simply re-requested next poll
                    // instead of being decoded now and thrown away.
                    if record_budget
                        .is_some_and(|b| b.load(std::sync::atomic::Ordering::Relaxed) == 0)
                    {
                        continue;
                    }

                    let outcome = decode_partition_batches(
                        topic_name,
                        partition,
                        record_bytes,
                        partition_fetch_offset,
                        partition_response.aborted_transactions,
                        record_budget,
                        self.config.max_decompressed_size,
                        &mut records,
                    );

                    if outcome.corrupt {
                        self.metrics.record_batch_decode_error();
                    }

                    // A batch at the fetch position that could not be decoded
                    // leaves the partition unable to advance; record it as a
                    // partition-level fault rather than failing the whole
                    // broker request.
                    if let Some(error) = outcome.error {
                        faults.push(PartitionFetchFault {
                            key: key.clone(),
                            offset: partition_fetch_offset,
                            error,
                        });
                    }

                    // Track offset update for this partition, tagged with the
                    // position the fetch was issued from so the caller can
                    // detect that a `seek()` has since invalidated it.
                    if let Some(last_offset) = outcome.last_offset {
                        offset_updates.push((
                            key,
                            FetchOffsetUpdate {
                                requested: partition_fetch_offset,
                                next: last_offset.saturating_add(1),
                                epoch: outcome.last_epoch,
                            },
                        ));
                    }
                }
            }
        }

        // NOTE: Offsets are NOT advanced here. They are advanced in poll()
        // after max_poll_records truncation to avoid silently losing records
        // whose offsets were already committed.
        // We return offset_updates and high watermarks alongside records so
        // the caller can apply them and compute lag.

        // Apply log_start_offset and preferred-replica updates in a single
        // write lock acquisition. Log-start updates reflect broker state and
        // are not affected by max_poll_records truncation. Preferred-replica
        // last-write-wins: if a partition appears multiple times (e.g. set by
        // the response then cleared by error handling), the final entry takes
        // effect.
        if !log_start_updates.is_empty() || !pref_updates.is_empty() || !stable_updates.is_empty() {
            let expiry = Instant::now() + self.config.metadata_max_age;
            let mut partition_state = self.partition_state.write().await;
            for (key, offset) in log_start_updates {
                partition_state.entry(key).or_default().log_start_offset = Some(offset);
            }
            for (key, offset) in stable_updates {
                partition_state.entry(key).or_default().last_stable_offset = Some(offset);
            }
            for (key, value) in pref_updates {
                match value {
                    // Setting a preferred replica: insert/update the entry.
                    Some(replica_id) => {
                        partition_state.entry(key).or_default().preferred_replica =
                            Some((replica_id, expiry));
                    }
                    // Clearing: only mutate an existing entry. Skipping
                    // absent entries avoids inserting empty `PartitionState`
                    // values on every fetch response that reports
                    // `preferred_read_replica = -1` (the common case), which
                    // would otherwise write-amplify this hot path.
                    None => {
                        if let Some(state) = partition_state.get_mut(&key) {
                            state.preferred_replica = None;
                        }
                    }
                }
            }
        }

        Ok(FetchOutcome {
            records,
            offset_updates,
            hw_updates,
            faults,
        })
    }

    /// Resolve the `host:port` to connect to for a broker ID.
    ///
    /// The metadata cache is the single source of truth, including for leaders
    /// a broker named in a fetch response (KIP-951) — those endpoints are
    /// registered there by
    /// [`ClusterMetadata::apply_leader_hint`](crate::metadata::ClusterMetadata::apply_leader_hint).
    fn broker_address(&self, broker_id: crate::BrokerId) -> Option<String> {
        self.metadata
            .broker(broker_id)
            .map(|broker| broker.address().to_string())
    }

    /// Move a partition back to the point where its log still matched the
    /// leader's, after the broker reported a divergence (KIP-320).
    ///
    /// Everything the consumer holds at or beyond `end_offset` came from a log
    /// the current leader does not have — the aftermath of an unclean leader
    /// election. Three pieces of state have to move together:
    ///
    /// - the fetch position, back to `end_offset`;
    /// - buffered records at or beyond it, which would otherwise be handed to
    ///   the application even though they no longer exist upstream;
    /// - the recorded leader epoch, which described the discarded position.
    ///
    /// This is not an out-of-range condition: `end_offset` is a valid offset in
    /// the leader's log, so `auto.offset.reset` deliberately does not apply.
    /// Resetting here would move the consumer to the log's start or end and
    /// lose far more than the divergence itself.
    async fn truncate_to_diverging_offset(
        &self,
        topic: &str,
        partition: PartitionId,
        diverging: crate::protocol::DivergingEpoch,
    ) {
        let key = (topic.to_string(), partition);
        let new_position = diverging.end_offset;

        let mut offsets = self.offsets.write().await;
        let old_position = offsets.get(&key).copied();
        offsets.insert(key.clone(), new_position);
        let mut partition_state = self.partition_state.write().await;
        let entry = partition_state.entry(key).or_default();
        entry.last_fetched_epoch = None;
        // The broker has just told us exactly where the logs part company, so
        // the new position needs no further validation.
        entry.position_validated = true;
        drop(partition_state);
        drop(offsets);

        // Records already fetched from beyond the divergence point are from
        // the discarded log and must not reach the application.
        let dropped = {
            let mut buffer = self.recv_buffer.lock();
            let before = buffer.len();
            buffer.retain(|record| {
                record.topic != topic
                    || record.partition != partition
                    || record.offset < new_position
            });
            let after = buffer.len();
            self.metrics.buffered_records.set(after as u64);
            before - after
        };

        warn!(
            topic = %topic,
            partition,
            old_position = ?old_position,
            new_position,
            diverging_epoch = diverging.epoch,
            dropped_buffered_records = dropped,
            "Log truncation detected: the partition's log diverged from the leader's; \
             rewinding the fetch position"
        );

        self.metrics.record_seek(1);
        self.recompute_lag_metrics().await;
    }

    /// Check the fetch positions of partitions that have not been validated
    /// since they were last set, and truncate any that sit beyond the leader's
    /// log (KIP-320).
    ///
    /// Runs before fetching rather than in response to an error. A partition
    /// that was assigned or `seek()`-ed after an unclean leader election can
    /// hold a position the new leader never had, and the broker will happily
    /// serve records from it — the mismatch is only visible if the client asks
    /// about the epoch first. Validating up front is what turns that silent
    /// data-consistency failure into a bounded rewind.
    ///
    /// Failures are logged and the partition is left unvalidated so the next
    /// poll retries; they must not block fetching, which would turn a
    /// transient broker problem into a stalled consumer.
    async fn validate_pending_positions(&self, keys: &[(String, PartitionId)]) {
        let pending: Vec<(String, PartitionId)> = {
            let offsets = self.offsets.read().await;
            let partition_state = self.partition_state.read().await;
            keys.iter()
                .filter(|key| offsets.contains_key(*key))
                .filter(|key| {
                    partition_state
                        .get(*key)
                        .is_none_or(|state| !state.position_validated)
                })
                .cloned()
                .collect()
        };

        for (topic, partition) in pending {
            match self
                .validate_offset_for_leader_epoch_inner(&topic, partition, false)
                .await
            {
                Ok(()) => {
                    let mut partition_state = self.partition_state.write().await;
                    partition_state
                        .entry((topic, partition))
                        .or_default()
                        .position_validated = true;
                }
                Err(e) => {
                    debug!(
                        topic = %topic,
                        partition,
                        error = %e,
                        "Offset validation failed; retrying on the next poll"
                    );
                }
            }
        }
    }

    /// Handle an `OffsetOutOfRange` error for a single partition by resolving
    /// a new offset via the configured `auto_offset_reset` policy.
    async fn handle_offset_out_of_range(&self, topic: &str, partition: PartitionId) {
        let Some(target) = self.config.auto_offset_reset.to_offset() else {
            return;
        };

        let key = (topic.to_string(), partition);

        let resolved = if let Some(ref gc) = self.group_coordinator {
            let mut part_map = HashMap::new();
            part_map.insert(key.0.clone(), vec![partition]);
            match gc.list_offsets(&part_map, target).await {
                Ok(offsets) => offsets.get(&key).copied(),
                Err(e) => {
                    warn!(
                        "Coordinator list_offsets failed for {}-{}: {}, falling back to direct",
                        topic, partition, e
                    );
                    None
                }
            }
        } else {
            None
        };

        // Use coordinator result, or fall back to direct ListOffsets
        let offset = match resolved {
            Some(o) => Some(o),
            None => self
                .resolve_list_offset(topic, partition, target)
                .await
                .map_err(|e| {
                    warn!("Direct list_offset failed for {topic}-{partition}: {e}");
                    e
                })
                .ok(),
        };

        if let Some(new_offset) = offset {
            let mut offsets = self.offsets.write().await;
            offsets.insert(key.clone(), new_offset);
            let mut partition_state = self.partition_state.write().await;
            invalidate_position_epoch(&mut partition_state, &key);
            drop(partition_state);
            drop(offsets);
            // Records buffered from the old position now describe offsets that
            // may no longer exist in the log; keeping them would clamp the next
            // commit back onto them and re-enter this reset path forever.
            self.discard_buffered_for(&HashSet::from([key]));
            self.recompute_lag_metrics().await;
        }
    }

    /// Validate the consumer's offset for a partition using OffsetForLeaderEpoch,
    /// refreshing metadata first.
    ///
    /// Used on the error path, where the leader epoch the client holds is
    /// already known to be wrong and cached leader information cannot be
    /// trusted to address the request.
    async fn validate_offset_for_leader_epoch(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<()> {
        self.validate_offset_for_leader_epoch_inner(topic, partition, true)
            .await
    }

    /// Ask the leader where the epoch at the consumer's position ends, and
    /// truncate the position if it sits beyond that point.
    ///
    /// The request pairs the epoch the consumer last consumed
    /// ([`PartitionState::last_fetched_epoch`], falling back to the leader
    /// epoch from metadata when nothing has been consumed yet) with the
    /// current leader epoch. The broker answers with the last offset of that
    /// epoch. A position beyond it exists only in a log the current leader
    /// discarded, so the position is rewound to the broker's answer.
    ///
    /// `refresh_metadata` controls whether cached leader information is
    /// refreshed first. The proactive path skips the refresh: it runs on every
    /// newly positioned partition, and forcing a metadata round trip there
    /// would reintroduce the very latency this is meant to avoid.
    ///
    /// Truncation here lands on a valid offset in the leader's log, so it is
    /// deliberately not routed through `auto.offset.reset`.
    async fn validate_offset_for_leader_epoch_inner(
        &self,
        topic: &str,
        partition: PartitionId,
        refresh_metadata: bool,
    ) -> Result<()> {
        use crate::protocol::OffsetForLeaderEpochPartition;
        use crate::protocol::OffsetForLeaderEpochRequest;
        use crate::protocol::OffsetForLeaderEpochResponse;
        use crate::protocol::OffsetForLeaderEpochTopic;

        if refresh_metadata && let Err(e) = self.metadata.refresh_for_topics(Some(&[topic])).await {
            warn!(
                "Metadata refresh failed for {}: {}, using cached metadata",
                topic, e
            );
        }

        let leader_epoch = self.metadata.leader_epoch(topic, partition).unwrap_or(-1);

        if leader_epoch < 0 {
            return Ok(());
        }

        // The epoch to ask about is the one the position actually came from.
        // Without it there is nothing to compare against but the current
        // epoch, which still catches a position past the leader's log end.
        let position_epoch = {
            let partition_state = self.partition_state.read().await;
            partition_state
                .get(&(topic.to_string(), partition))
                .and_then(|state| state.last_fetched_epoch)
                .filter(|&epoch| epoch >= 0)
                .unwrap_or(leader_epoch)
        };

        let leader_id = self.metadata.leader(topic, partition).ok_or_else(|| {
            KrafkaError::invalid_state(format!("no leader for {topic}-{partition}"))
        })?;

        let broker = self
            .metadata
            .broker(leader_id)
            .ok_or_else(|| KrafkaError::invalid_state(format!("broker {} not found", leader_id)))?;

        let conn = self
            .pool
            .get_connection_by_id(leader_id, broker.address())
            .await?;

        let request = OffsetForLeaderEpochRequest {
            replica_id: -1, // consumer
            topics: vec![OffsetForLeaderEpochTopic {
                topic: topic.to_string(),
                partitions: vec![OffsetForLeaderEpochPartition {
                    partition,
                    current_leader_epoch: leader_epoch,
                    leader_epoch: position_epoch,
                }],
            }],
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::OffsetForLeaderEpoch,
                versions::OFFSET_FOR_LEADER_EPOCH_MAX,
                versions::OFFSET_FOR_LEADER_EPOCH_MIN,
            )
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported OffsetForLeaderEpoch API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::OffsetForLeaderEpoch, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = OffsetForLeaderEpochResponse::decode_versioned(version, &mut buf)?;

        let key = (topic.to_string(), partition);
        let mut offset_changed = false;

        for topic_result in response.topics {
            for partition_result in topic_result.partitions {
                if partition_result.partition != partition {
                    continue;
                }
                if partition_result.error_code.is_ok() && partition_result.end_offset >= 0 {
                    let current_offset = {
                        let offsets = self.offsets.read().await;
                        match offsets.get(&key).copied() {
                            Some(offset) => offset,
                            None => {
                                debug!(
                                    topic = %topic,
                                    partition,
                                    "No tracked offset for partition during epoch validation"
                                );
                                0
                            }
                        }
                    };

                    if current_offset > partition_result.end_offset {
                        warn!(
                            topic = %topic,
                            partition,
                            old_position = current_offset,
                            new_position = partition_result.end_offset,
                            "Log truncation detected: the position is past the end of its \
                             leader epoch; rewinding the fetch position"
                        );
                        let mut offsets = self.offsets.write().await;
                        offsets.insert(key.clone(), partition_result.end_offset);
                        let mut partition_state = self.partition_state.write().await;
                        let entry = partition_state.entry(key.clone()).or_default();
                        entry.last_fetched_epoch = None;
                        drop(partition_state);
                        drop(offsets);

                        // Buffered records from beyond the truncation point
                        // belong to the discarded log.
                        let mut buffer = self.recv_buffer.lock();
                        buffer.retain(|record| {
                            record.topic != topic
                                || record.partition != partition
                                || record.offset < partition_result.end_offset
                        });
                        self.metrics.buffered_records.set(buffer.len() as u64);
                        drop(buffer);

                        offset_changed = true;
                    }
                }
            }
        }

        if offset_changed {
            self.recompute_lag_metrics().await;
        }
        Ok(())
    }

    /// Receive the next record.
    ///
    /// This is a convenience method that returns one record at a time.
    /// Internally buffers records from `poll()` and returns them one by one,
    /// ensuring no records are lost.
    ///
    /// Returns `Err(RecvError::Closed)` when the consumer is shut down.
    /// Returns `Err(RecvError::Error(e))` on broker or network failures.
    ///
    /// # Example
    ///
    /// ```ignore
    /// loop {
    ///     match consumer.recv().await {
    ///         Ok(record)               => process(record),
    ///         Err(RecvError::Closed)   => break,
    ///         Err(RecvError::Error(e)) => return Err(e),
    ///         _ => break, // future variants (non_exhaustive)
    ///     }
    /// }
    /// ```
    pub async fn recv(&self) -> std::result::Result<ConsumerRecord, RecvError> {
        loop {
            // Return buffered records first, honouring `pause()` — the fetch
            // path filters paused partitions out of its own return value, so
            // draining the buffer unconditionally here would make `pause()`
            // mean something different depending on which read API is used.
            {
                let paused = self.paused.read().await.clone();
                let mut buffer = self.recv_buffer.lock();
                let mut one = Vec::with_capacity(1);
                drain_buffered_records(&mut buffer, &mut one, 1, &paused);
                self.metrics.buffered_records.set(buffer.len() as u64);
                if let Some(record) = one.pop() {
                    return Ok(record);
                }
            }

            if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(RecvError::Closed);
            }

            match self.poll(Duration::from_secs(1)).await {
                Ok(mut records) if !records.is_empty() => {
                    // Everything after the first record goes back to the
                    // *front* of the buffer, not the back.
                    //
                    // The poll that produced these records may have parked its
                    // own surplus at the back — records from the same fetch,
                    // and therefore from higher offsets in the same partitions.
                    // Appending here would put offsets 2..N behind offsets
                    // N+1.., so the next `recv()` would hand the application a
                    // partition's records out of order. Reinserting at the
                    // front restores fetch order for every partition at once.
                    let rest = records.split_off(1);
                    self.requeue_undelivered(rest);
                    // Infallible: `!records.is_empty()` guard above guarantees ≥1 element.
                    let Some(first) = records.pop() else {
                        unreachable!("non-empty ConsumerRecords yields at least one element");
                    };
                    return Ok(first);
                }
                Ok(_) => continue,
                Err(e) => {
                    if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
                        return Err(RecvError::Closed);
                    }
                    return Err(RecvError::Error(e));
                }
            }
        }
    }

    /// Collect up to `max_records` records, waiting at most `timeout`.
    ///
    /// Returns as soon as `max_records` have been collected **or** `timeout`
    /// elapses, whichever comes first.
    ///
    /// If the consumer closes after some records were already buffered or
    /// fetched, those records are returned as a partial batch.
    ///
    /// **When to use `batch_recv`**: for throughput-optimised pipelines that
    /// need fixed-size batches — bulk database inserts, transactional
    /// exactly-once produce-consume loops, or processing frameworks that
    /// benefit from amortising per-batch overhead.  `batch_recv` drains the
    /// internal record buffer before issuing new fetches and keeps looping
    /// until `max_records` are collected or the deadline elapses.
    ///
    /// **When to use [`poll`](Self::poll) instead**: for simple event loops
    /// that process records as they arrive and where the number of records
    /// per iteration does not need to be bounded.
    ///
    /// # `BatchRecvOutcome`
    ///
    /// The enum is `#[non_exhaustive]`, so match arms must include a catch-all
    /// (`_ => {}`) to remain forward-compatible with new variants.
    ///
    /// # Errors
    ///
    /// Returns `Err` on broker or network errors.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::time::Duration;
    ///
    /// use krafka::consumer::BatchRecvOutcome;
    ///
    /// match consumer.batch_recv(100, Duration::from_millis(200)).await? {
    ///     BatchRecvOutcome::Records(records) => {
    ///         for record in records {
    ///             println!("{}: {:?}", record.offset, record.value);
    ///         }
    ///     }
    ///     BatchRecvOutcome::TimedOut => {}
    ///     BatchRecvOutcome::Closed => break,
    ///     BatchRecvOutcome::EmptyRequest => {}
    ///     _ => {} // required: BatchRecvOutcome is #[non_exhaustive]
    /// }
    /// ```
    pub async fn batch_recv(
        &self,
        max_records: usize,
        timeout: Duration,
    ) -> Result<BatchRecvOutcome> {
        batch_recv_with(
            &self.recv_buffer,
            |len| self.metrics.buffered_records.set(len),
            max_records,
            timeout,
            self.config.idle_poll_backoff(),
            || self.closed.load(std::sync::atomic::Ordering::SeqCst),
            || async { self.paused.read().await.clone() },
            |remaining| self.poll(remaining),
        )
        .await
    }

    /// Create an async [`Stream`](futures_core::Stream) of records.
    ///
    /// Each element is a `Result<ConsumerRecord>`. The stream terminates
    /// when the consumer is closed (returns `None`). Broker and network
    /// errors are propagated as `Some(Err(...))`.
    ///
    /// Internally delegates to [`recv()`](Self::recv), which handles
    /// polling, buffering, auto-commit, rebalancing, and shutdown.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use tokio_stream::StreamExt;
    ///
    /// let mut stream = consumer.stream();
    /// while let Some(result) = stream.next().await {
    ///     let record = result?;
    ///     println!("{}: {}", record.topic, record.offset);
    /// }
    /// ```
    #[must_use = "stream does nothing unless polled"]
    pub fn stream(&self) -> ConsumerStream<'_> {
        ConsumerStream::new(self)
    }

    /// Commit offsets for all consumed records.
    ///
    /// This stores the current offsets for assigned partitions only.
    /// When using a consumer group, this sends an OffsetCommit request to the group coordinator.
    /// Offsets for revoked partitions are excluded to avoid overwriting the new owner's progress.
    pub async fn commit(&self) -> Result<()> {
        let offsets_snapshot = {
            let offsets = self.offsets.read().await;
            if offsets.is_empty() {
                debug!("No offsets to commit");
                return Ok(());
            }
            self.committable_snapshot(&offsets)
        };

        self.metrics.commits.inc();

        let assigned_set = if self.group_coordinator.is_some() {
            let assignments = self.assignments.read().await;
            Some(
                assignments
                    .iter()
                    .flat_map(|(topic, parts)| parts.iter().map(move |&p| (topic.clone(), p)))
                    .collect::<HashSet<_>>(),
            )
        } else {
            None
        };

        // Leader epochs for the positions being committed, so KIP-320
        // truncation detection survives the commit boundary.
        let commit_epochs = self.committed_leader_epochs(&offsets_snapshot).await;

        let commit_offsets = Self::build_commit_offsets(
            &offsets_snapshot,
            &commit_epochs,
            assigned_set.as_ref(),
            self.group_coordinator.is_some(),
        )?;

        if commit_offsets.is_empty() {
            debug!("No assigned partition offsets to commit");
            return Ok(());
        }

        let committed_offsets = Self::build_committed_offsets(&commit_offsets);

        if let Some(coordinator) = self.group_coordinator.clone() {
            Self::commit_group_offsets_with_retry(
                coordinator,
                self.interceptor.clone(),
                commit_offsets,
                committed_offsets,
            )
            .await
        } else {
            for ((topic, partition), offset) in &committed_offsets {
                debug!("Committed offset for {}-{}: {}", topic, partition, offset);
            }
            info!(
                "Committed {} partition offsets (local only)",
                committed_offsets.len()
            );
            Ok(())
        }
    }

    /// Commit offsets synchronously.
    pub async fn commit_sync(&self) -> Result<()> {
        self.commit().await
    }

    /// Commit offsets asynchronously.
    ///
    /// Spawns the offset commit as a background task.
    ///
    /// Await the returned handle to observe offset-snapshot, transport, and
    /// broker errors. Retriable coordinator failures use the same short
    /// backoff loop as [`Consumer::commit`]. If the handle is dropped, the
    /// task continues in the background and its result is discarded.
    pub fn commit_async(&self) -> OffsetCommitHandle {
        let assigned_set = if self.group_coordinator.is_some() {
            match self.assignments.try_read() {
                Ok(guard) => Some(
                    guard
                        .iter()
                        .flat_map(|(topic, parts)| parts.iter().map(move |&p| (topic.clone(), p)))
                        .collect::<HashSet<_>>(),
                ),
                Err(_) => {
                    return OffsetCommitHandle::ready(Err(KrafkaError::invalid_state(
                        "commit_async: assignments lock contention",
                    )));
                }
            }
        } else {
            None
        };

        let offsets_snapshot = match self.offsets.try_read() {
            Ok(guard) => {
                if guard.is_empty() {
                    return OffsetCommitHandle::ready(Ok(()));
                }
                self.metrics.commits.inc();
                // Clamp to what the application has actually received so
                // buffered-but-undelivered records are never acknowledged.
                let committable = self.committable_snapshot(&guard);
                // Non-blocking like every other snapshot in this function:
                // if the lock is contended the commit still goes out, just
                // without epochs, which is strictly what the previous
                // behaviour was for every commit.
                let epochs = match self.partition_state.try_read() {
                    Ok(state) => Self::leader_epochs_from_state(&committable, &state),
                    Err(_) => HashMap::new(),
                };
                match Self::build_commit_offsets(
                    &committable,
                    &epochs,
                    assigned_set.as_ref(),
                    self.group_coordinator.is_some(),
                ) {
                    Ok(offsets) => offsets,
                    Err(error) => return OffsetCommitHandle::ready(Err(error)),
                }
            }
            Err(_) => {
                return OffsetCommitHandle::ready(Err(KrafkaError::invalid_state(
                    "commit_async: offset lock contention",
                )));
            }
        };

        if offsets_snapshot.is_empty() {
            debug!("Async commit: no eligible partition offsets to commit");
            return OffsetCommitHandle::ready(Ok(()));
        }

        let committed_offsets: HashMap<(String, PartitionId), Offset> = offsets_snapshot
            .iter()
            .map(|((topic, partition), position)| ((topic.clone(), *partition), position.offset))
            .collect();

        let Some(coordinator) = self.group_coordinator.clone() else {
            debug!("Async commit: no group coordinator, offsets stored locally");
            return OffsetCommitHandle::ready(Ok(()));
        };

        OffsetCommitHandle::Task(tokio::spawn(Self::commit_group_offsets_with_retry(
            coordinator,
            self.interceptor.clone(),
            offsets_snapshot,
            committed_offsets,
        )))
    }

    /// Clamp each partition's fetch position to the highest offset that has
    /// actually been handed to the application.
    ///
    /// `poll()` advances the fetch position for every record it retrieved, but
    /// `recv()` returns those records one at a time and holds the remainder in
    /// the receive buffer. Committing the raw fetch position would therefore
    /// acknowledge records that are still buffered and unprocessed: with the
    /// defaults, one `poll()` fetches 500 records, the application consumes a
    /// handful, and the next auto-commit tick durably records all 500 as done.
    /// A crash at that point skips every record still in the buffer — silently,
    /// and with no way to recover them.
    ///
    /// Clamping to the lowest still-buffered offset is what makes the
    /// documented at-least-once guarantee hold: a record is only ever
    /// acknowledged after it has been delivered.
    ///
    /// The receive buffer's sync lock is taken while the `offsets` read guard
    /// is held, which respects the documented lock order (async levels before
    /// sync levels) and involves no `.await`.
    fn committable_snapshot(
        &self,
        offsets: &HashMap<(String, PartitionId), Offset>,
    ) -> HashMap<(String, PartitionId), Offset> {
        let buffer = self.recv_buffer.lock();
        committable_positions(offsets, &buffer)
    }

    /// Leader epochs for the partitions in `offsets`, read from partition state.
    async fn committed_leader_epochs(
        &self,
        offsets: &HashMap<(String, PartitionId), Offset>,
    ) -> HashMap<(String, PartitionId), i32> {
        let state = self.partition_state.read().await;
        Self::leader_epochs_from_state(offsets, &state)
    }

    /// Pure half of [`Self::committed_leader_epochs`], so the non-blocking
    /// `commit_async` path can share it without duplicating the lookup rule.
    fn leader_epochs_from_state(
        offsets: &HashMap<(String, PartitionId), Offset>,
        state: &HashMap<(String, PartitionId), PartitionState>,
    ) -> HashMap<(String, PartitionId), i32> {
        offsets
            .keys()
            .filter_map(|key| {
                state
                    .get(key)
                    .and_then(|s| s.last_fetched_epoch)
                    .filter(|&epoch| epoch >= 0)
                    .map(|epoch| (key.clone(), epoch))
            })
            .collect()
    }

    fn build_commit_offsets(
        offsets: &HashMap<(String, PartitionId), Offset>,
        epochs: &HashMap<(String, PartitionId), i32>,
        assigned_set: Option<&HashSet<(String, PartitionId)>>,
        has_group: bool,
    ) -> Result<CommitRequestOffsets> {
        if has_group && assigned_set.is_none() {
            return Err(KrafkaError::invalid_state(
                "commit_async: assignments snapshot unavailable",
            ));
        }

        Ok(offsets
            .iter()
            .filter(|((topic, partition), _)| {
                !has_group
                    || assigned_set
                        .is_some_and(|assigned| assigned.contains(&(topic.clone(), *partition)))
            })
            .map(|((topic, partition), offset)| {
                let key = (topic.clone(), *partition);
                // The epoch the consumer last advanced this partition through.
                // Absent for a position that came from a seek or an offset
                // reset, where the consumer has consumed nothing and so has no
                // epoch to vouch for.
                let leader_epoch = epochs.get(&key).copied().unwrap_or(-1);
                (
                    key,
                    CommitPosition {
                        offset: *offset,
                        leader_epoch,
                        metadata: None,
                    },
                )
            })
            .collect())
    }

    fn build_committed_offsets(
        commit_offsets: &CommitRequestOffsets,
    ) -> HashMap<(String, PartitionId), Offset> {
        commit_offsets
            .iter()
            .map(|((topic, partition), position)| ((topic.clone(), *partition), position.offset))
            .collect()
    }

    fn filter_commit_with_metadata_offsets(
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
        assigned_set: Option<&HashSet<(String, PartitionId)>>,
        has_group: bool,
    ) -> Result<HashMap<TopicPartition, OffsetAndMetadata>> {
        if has_group && assigned_set.is_none() {
            return Err(KrafkaError::invalid_state(
                "commit_with_metadata: assignments snapshot unavailable",
            ));
        }

        Ok(offsets
            .into_iter()
            .filter(|(tp, _)| {
                !has_group
                    || assigned_set.is_some_and(|assigned| {
                        assigned.contains(&(tp.topic.clone(), tp.partition))
                    })
            })
            .collect())
    }

    fn build_commit_offsets_with_metadata(
        filtered_offsets: &HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> CommitRequestOffsets {
        filtered_offsets
            .iter()
            .map(|(tp, offset_meta)| {
                (
                    (tp.topic.clone(), tp.partition),
                    CommitPosition {
                        offset: offset_meta.offset,
                        // `OffsetAndMetadata::leader_epoch` is a public field
                        // documented as the leader epoch; honour it rather
                        // than silently dropping what the caller supplied.
                        leader_epoch: offset_meta.leader_epoch.unwrap_or(-1),
                        metadata: offset_meta.metadata.clone(),
                    },
                )
            })
            .collect()
    }

    async fn retry_commit_with<F, Fut>(mut commit_once: F) -> Result<()>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        match commit_once().await {
            Ok(()) => Ok(()),
            Err(error) if error.is_retriable() => {
                let mut last_error = error;
                let backoffs = [Duration::from_millis(100), Duration::from_millis(250)];
                for delay in &backoffs {
                    debug!(
                        "Commit failed with retriable error, retrying in {:?}: {last_error}",
                        delay
                    );
                    tokio::time::sleep(*delay).await;
                    match commit_once().await {
                        Ok(()) => return Ok(()),
                        Err(error) if error.is_retriable() => {
                            last_error = error;
                        }
                        Err(error) => return Err(error),
                    }
                }
                Err(last_error)
            }
            Err(error) => Err(error),
        }
    }

    async fn commit_group_offsets_with_retry(
        coordinator: Arc<GroupCoordinator>,
        interceptor: Arc<dyn crate::interceptor::ConsumerInterceptor>,
        commit_offsets: CommitRequestOffsets,
        committed_offsets: HashMap<(String, PartitionId), Offset>,
    ) -> Result<()> {
        let result = Self::retry_commit_with(|| coordinator.commit_offsets(&commit_offsets)).await;

        match result {
            Ok(()) => {
                crate::interceptor::safe_on_commit(&*interceptor, &committed_offsets, None);
                Ok(())
            }
            Err(error) => {
                crate::interceptor::safe_on_commit(&*interceptor, &committed_offsets, Some(&error));
                Err(error)
            }
        }
    }

    /// Commit specific offsets with metadata.
    ///
    /// Allows committing offsets for specific topic-partitions with optional metadata.
    /// This is useful for checkpointing or storing application-specific context.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use ahash::AHashMap;
    /// use krafka::consumer::{Consumer, OffsetAndMetadata, TopicPartition};
    ///
    /// # async fn example() -> Result<(), krafka::error::KrafkaError> {
    /// # let consumer: Consumer = todo!();
    /// let mut offsets = AHashMap::new();
    /// offsets.insert(
    ///     TopicPartition::new("my-topic", 0),
    ///     OffsetAndMetadata::with_metadata(100, "checkpoint-abc123"),
    /// );
    /// consumer.commit_with_metadata(offsets).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn commit_with_metadata(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<()> {
        if offsets.is_empty() {
            debug!("No offsets to commit");
            return Ok(());
        }

        self.metrics.commits.inc();

        let assigned_set = if self.group_coordinator.is_some() {
            let assignments = self.assignments.read().await;
            Some(
                assignments
                    .iter()
                    .flat_map(|(topic, parts)| parts.iter().map(move |&p| (topic.clone(), p)))
                    .collect::<HashSet<_>>(),
            )
        } else {
            None
        };

        let filtered_offsets = Self::filter_commit_with_metadata_offsets(
            offsets,
            assigned_set.as_ref(),
            self.group_coordinator.is_some(),
        )?;

        if filtered_offsets.is_empty() {
            debug!("No offsets to commit after filtering by assigned partitions");
            return Ok(());
        }

        // If we have a group coordinator, send actual OffsetCommit request
        if let Some(coordinator) = self.group_coordinator.clone() {
            let commit_offsets = Self::build_commit_offsets_with_metadata(&filtered_offsets);
            let committed_offsets = Self::build_committed_offsets(&commit_offsets);

            Self::commit_group_offsets_with_retry(
                coordinator,
                self.interceptor.clone(),
                commit_offsets,
                committed_offsets,
            )
            .await?;

            // Update internal offset store
            let mut internal_offsets = self.offsets.write().await;
            for (tp, offset_meta) in filtered_offsets {
                internal_offsets.insert((tp.topic, tp.partition), offset_meta.offset);
            }
        } else {
            // Log offsets being committed with metadata for non-group consumers
            for (tp, offset_meta) in &filtered_offsets {
                let metadata_str = offset_meta.metadata.as_deref().unwrap_or("<none>");
                debug!(
                    "Committed offset for {}-{}: {} (metadata: {})",
                    tp.topic, tp.partition, offset_meta.offset, metadata_str
                );
            }

            let count = filtered_offsets.len();

            // Update internal offset store
            let mut internal_offsets = self.offsets.write().await;
            for (tp, offset_meta) in filtered_offsets {
                internal_offsets.insert((tp.topic, tp.partition), offset_meta.offset);
            }

            info!(
                "Committed {} partition offsets with metadata (local only)",
                count
            );
        }

        self.recompute_lag_metrics().await;
        Ok(())
    }

    /// The offset of the next record this partition will **deliver**.
    ///
    /// This is the offset a commit would write, so `position()` and
    /// [`commit()`](Self::commit) can never disagree. It is *not* necessarily
    /// where the next fetch starts: the consumer reads ahead of delivery and
    /// parks the surplus, so the fetch position runs in front of this one by up
    /// to `max_buffered_records`. Use [`fetch_position`](Self::fetch_position)
    /// when you want that value instead.
    ///
    /// Returns `None` when the partition has no tracked position — it is
    /// unassigned, or its offset has not been resolved yet.
    pub async fn position(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        let key = (topic.to_string(), partition);
        let offsets = self.offsets.read().await;
        let fetch_position = offsets.get(&key).copied()?;
        let buffer = self.recv_buffer.lock();
        let undelivered = lowest_undelivered_offsets(&buffer);
        Some(match undelivered.get(&key) {
            Some(&first_undelivered) => fetch_position.min(first_undelivered),
            None => fetch_position,
        })
    }

    /// The offset the next **fetch** for this partition will start from.
    ///
    /// Runs ahead of [`position`](Self::position) by however many records are
    /// currently parked in the receive buffer for this partition — prefetch
    /// surplus, or records withheld because the partition is paused. Useful for
    /// diagnosing read-ahead behaviour; [`position`](Self::position) is what
    /// application logic normally wants.
    pub async fn fetch_position(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        let offsets = self.offsets.read().await;
        offsets.get(&(topic.to_string(), partition)).copied()
    }

    /// Returns a **snapshot** of the current partition assignments.
    ///
    /// The returned `HashMap` is a clone — modifying it has no effect on the
    /// consumer's internal state. Assignments may change asynchronously due
    /// to rebalances.
    pub async fn assignment(&self) -> HashMap<String, Vec<PartitionId>> {
        let assignments = self.assignments.read().await;
        assignments.clone()
    }

    /// Get all subscribed topics.
    pub async fn subscription(&self) -> HashSet<String> {
        let subscriptions = self.subscriptions.read().await;
        subscriptions.clone()
    }

    /// Get the current lag for a specific partition.
    ///
    /// Returns the difference between the high watermark (latest offset on the
    /// broker) and the consumer's current position. Returns `None` if the high
    /// watermark or position is not yet known (e.g., no fetch has completed for
    /// this partition).
    ///
    /// This uses cached high watermarks from the most recent fetch response —
    /// no additional network calls are made.
    pub async fn current_lag(&self, topic: &str, partition: PartitionId) -> Option<u64> {
        let key = (topic.to_string(), partition);
        // Acquire offsets before partition_state to match the documented
        // lock ordering: assignments → offsets → partition_state.
        let offsets = self.offsets.read().await;
        let fetch_position = offsets.get(&key).copied()?;
        let partition_state = self.partition_state.read().await;
        let end = partition_state
            .get(&key)
            .and_then(|s| s.readable_end_offset(self.config.isolation_level))?;
        // Records read ahead into the buffer have not reached the application,
        // so they are still lag — see `compute_aggregate_lag`.
        let undelivered = lowest_undelivered_offsets(&self.recv_buffer.lock());
        let position = match undelivered.get(&key) {
            Some(&first_undelivered) => fetch_position.min(first_undelivered),
            None => fetch_position,
        };
        Some((end - position).max(0) as u64)
    }

    /// Get per-partition lag for all assigned partitions.
    ///
    /// Returns a [`LagResult`] containing per-partition lag values and a list
    /// of partitions whose cached high watermark is older than
    /// [`ConsumerBuilder::lag_staleness_threshold`](crate::consumer::ConsumerBuilder::lag_staleness_threshold)
    /// (default: 60 s).
    ///
    /// Partitions whose high watermark or position is not yet known are
    /// omitted from `LagResult::lag` entirely.
    pub async fn lag(&self) -> LagResult {
        // Acquire offsets before partition_state to match the documented
        // lock ordering: assignments → offsets → partition_state.
        let offsets = self.offsets.read().await;
        let partition_state = self.partition_state.read().await;
        let now = Instant::now();
        let threshold = self.config.lag_staleness_threshold;
        let mut lag = HashMap::with_capacity(partition_state.len());
        let mut stale_partitions = Vec::new();
        let undelivered = lowest_undelivered_offsets(&self.recv_buffer.lock());
        for (key, state) in partition_state.iter() {
            if let (Some(end), Some(&fetch_position)) = (
                state.readable_end_offset(self.config.isolation_level),
                offsets.get(key),
            ) {
                let position = match undelivered.get(key) {
                    Some(&first_undelivered) => fetch_position.min(first_undelivered),
                    None => fetch_position,
                };
                lag.insert(key.clone(), (end - position).max(0) as u64);
                let is_stale = state
                    .watermark_updated_at
                    .is_none_or(|t| now.saturating_duration_since(t) > threshold);
                if is_stale {
                    stale_partitions.push(key.clone());
                }
            }
        }
        LagResult {
            lag,
            stale_partitions,
        }
    }

    /// Get the cached beginning (log start) offset for a partition.
    ///
    /// Returns the earliest available offset on the broker, cached from
    /// fetch responses. Returns `None` if no fetch has completed for this
    /// partition yet. No network calls are made.
    pub async fn cached_beginning_offset(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Option<Offset> {
        let key = (topic.to_string(), partition);
        self.partition_state
            .read()
            .await
            .get(&key)
            .and_then(|s| s.log_start_offset)
    }

    /// Get the cached end offset for a partition — the highest offset this
    /// consumer is allowed to read.
    ///
    /// Cached from fetch responses; no network calls are made. Returns `None`
    /// if no fetch has completed for this partition yet.
    ///
    /// Under `read_uncommitted` this is the high watermark. Under
    /// `read_committed` it is the **last stable offset**, because the broker
    /// will not deliver a record at or above the LSO — comparing a position
    /// against the high watermark there reports lag the consumer can never
    /// close. Use [`cached_high_watermark`](Self::cached_high_watermark) when
    /// you specifically want the log-end offset regardless of isolation level.
    pub async fn cached_end_offset(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        let key = (topic.to_string(), partition);
        self.partition_state
            .read()
            .await
            .get(&key)
            .and_then(|s| s.readable_end_offset(self.config.isolation_level))
    }

    /// Get the cached high watermark (log-end offset) for a partition,
    /// independent of isolation level.
    ///
    /// Unlike [`cached_end_offset`](Self::cached_end_offset), this always
    /// reports the partition's log-end offset, including records inside open
    /// transactions that a `read_committed` consumer cannot yet see. The gap
    /// between the two is exactly the volume of in-flight transactional data.
    pub async fn cached_high_watermark(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Option<Offset> {
        let key = (topic.to_string(), partition);
        self.partition_state
            .read()
            .await
            .get(&key)
            .and_then(|s| s.high_watermark)
    }

    /// Get the cached last stable offset (LSO) for a partition.
    ///
    /// The first offset belonging to an open transaction. `read_committed`
    /// consumers never receive a record at or above it. Returns `None` when
    /// the broker has not reported one (Fetch below v4, or before the first
    /// response for this partition).
    pub async fn cached_last_stable_offset(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Option<Offset> {
        let key = (topic.to_string(), partition);
        self.partition_state
            .read()
            .await
            .get(&key)
            .and_then(|s| s.last_stable_offset)
    }

    /// Fetch the current end (high-watermark) offset for a single partition
    /// with a live `ListOffsets` RPC.
    ///
    /// Unlike [`cached_end_offset`](Self::cached_end_offset), which returns a
    /// value from the most-recent fetch response, this method always contacts
    /// the partition leader and returns a fresh value.  Use it when staleness
    /// is unacceptable — for example, before the first poll, when a partition
    /// is paused, or when computing precise consumer-lag metrics.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the topic name is invalid, no leader is available, or
    /// the broker returns an error.
    pub async fn fetch_end_offset(&self, topic: &str, partition: PartitionId) -> Result<Offset> {
        validate_topic_name(topic)?;
        // timestamp = -1 means "latest offset" in the ListOffsets API.
        self.resolve_list_offset(topic, partition, -1).await
    }

    /// Returns `true` if the consumer's current position has reached or
    /// exceeded the high-watermark (end offset) on **every** assigned partition.
    ///
    /// "Caught up" means there are no more records available to consume right
    /// now.  This check uses **cached** high-watermarks updated on each
    /// successful fetch response; the values are not refreshed by this call.
    ///
    /// Returns `false` if:
    /// - Any assigned partition's high-watermark has not yet been cached
    ///   (i.e. no successful fetch has completed for that partition).
    /// - The consumer's position on any partition is behind its cached end
    ///   offset.
    ///
    /// The end offset respects the configured isolation level: under
    /// `read_committed` it is the last stable offset, so an open transaction
    /// on the partition does not keep this method returning `false` forever.
    ///
    /// For a precise check against fresh broker state, call
    /// [`fetch_end_offset`](Self::fetch_end_offset) on each partition and
    /// compare it to [`position`](Self::position).
    pub async fn is_caught_up(&self) -> bool {
        // Lock ordering: assignments (2) → offsets (3) → partition_state (5).
        let assignments = self.assignments.read().await;
        if assignments.is_empty() {
            return true; // no assignment ⇒ nothing to consume
        }
        let offsets = self.offsets.read().await;
        let partition_state = self.partition_state.read().await;
        // Records parked in the buffer have been fetched but not delivered, so
        // a consumer holding them is by definition not caught up.
        let undelivered = lowest_undelivered_offsets(&self.recv_buffer.lock());

        for (topic, partitions) in assignments.iter() {
            for &partition in partitions {
                let key = (topic.clone(), partition);
                let Some(end) = partition_state
                    .get(&key)
                    .and_then(|s| s.readable_end_offset(self.config.isolation_level))
                else {
                    // End offset not yet cached — cannot confirm caught-up.
                    return false;
                };
                let fetch_position = offsets.get(&key).copied().unwrap_or(0);
                let position = match undelivered.get(&key) {
                    Some(&first_undelivered) => fetch_position.min(first_undelivered),
                    None => fetch_position,
                };
                if position < end {
                    return false;
                }
            }
        }
        true
    }

    /// Unsubscribe from all topics.
    ///
    /// Commits offsets (when auto-commit is enabled), notifies the rebalance
    /// listener, leaves the consumer group, and clears offsets, the paused
    /// set, and the receive buffer.
    ///
    /// Returns a leave-group error after local state has still been cleared.
    pub async fn unsubscribe(&self) -> Result<()> {
        // Give up the partitions the same way a rebalance would: check
        // progress in before the assignment is released. Skipping this while
        // auto-commit is enabled means everything consumed since the last
        // periodic tick is silently re-delivered to whoever picks these
        // partitions up next.
        if self.config.enable_auto_commit
            && self.group_coordinator.is_some()
            && let Err(e) = self.commit().await
        {
            warn!("Commit during unsubscribe failed: {e}");
        }

        // Notify listener of revoked partitions before clearing.
        // Collect while holding the lock, then drop the lock before .await
        // to avoid holding a read guard across an await point.
        let revoked: Vec<TopicPartition> = {
            let assignments = self.assignments.read().await;
            assignments
                .iter()
                .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
                .collect()
        };
        if !revoked.is_empty() {
            self.safe_on_partitions_revoked(&revoked).await;
        }

        // Leave consumer group
        let leave_group_result = if let Some(ref coordinator) = self.group_coordinator {
            coordinator.leave_group().await
        } else {
            Ok(())
        };

        self.subscriptions.write().await.clear();
        self.assignments.write().await.clear();
        self.clear_partition_state().await;
        self.metrics.assigned_partitions.set(0);

        debug!("Unsubscribed from all topics");
        leave_group_result
    }

    /// Pause consumption of specific partitions.
    ///
    /// Paused partitions will be skipped during poll() until resumed.
    pub async fn pause(&self, topic: &str, partitions: &[PartitionId]) {
        let mut paused = self.paused.write().await;
        let topic_owned = topic.to_string();
        for &partition in partitions {
            paused.insert((topic_owned.clone(), partition));
        }
        self.metrics.paused_partitions.set(paused.len() as u64);
        debug!("Paused partitions for {}: {:?}", topic, partitions);
    }

    /// Resume consumption of specific partitions.
    ///
    /// Resumes polling for previously paused partitions.
    pub async fn resume(&self, topic: &str, partitions: &[PartitionId]) {
        let mut paused = self.paused.write().await;
        let topic_key = topic.to_string();
        for &partition in partitions {
            paused.remove(&(topic_key.clone(), partition));
        }
        self.metrics.paused_partitions.set(paused.len() as u64);
        debug!("Resumed partitions for {}: {:?}", topic, partitions);
    }

    /// Get the set of paused partitions.
    pub async fn paused_partitions(&self) -> HashSet<(String, PartitionId)> {
        self.paused.read().await.clone()
    }

    /// Replace the bootstrap server list at runtime (KIP-899).
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

    /// Force a rebootstrap: close all connections, clear the metadata cache,
    /// and fall back to bootstrap servers (KIP-899).
    pub async fn rebootstrap(&self) {
        self.metadata.rebootstrap().await;
    }

    fn select_close_result(
        auto_commit_result: Result<()>,
        leave_group_result: Result<()>,
    ) -> Result<()> {
        match auto_commit_result {
            Err(error) if Self::should_ignore_close_auto_commit_error(&error) => leave_group_result,
            Ok(()) => leave_group_result,
            Err(error) => Err(error),
        }
    }

    fn should_ignore_close_auto_commit_error(error: &KrafkaError) -> bool {
        matches!(
            error,
            KrafkaError::Broker {
                code: crate::error::ErrorCode::UnknownMemberId
                    | crate::error::ErrorCode::IllegalGeneration
                    | crate::error::ErrorCode::RebalanceInProgress
                    | crate::error::ErrorCode::FencedMemberEpoch
                    | crate::error::ErrorCode::StaleMemberEpoch,
                ..
            }
        )
    }

    /// Close the consumer.
    ///
    /// Commits offsets (if auto-commit is enabled), leaves the consumer group,
    /// and tears down connections. Calling `close()` more than once is a no-op.
    ///
    /// Returns the first cleanup error after local state and connections have
    /// still been torn down.
    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Ok(());
        }

        // Auto-commit on close (if enabled)
        let auto_commit_result = if self.config.enable_auto_commit {
            self.commit().await
        } else {
            Ok(())
        };

        // Report a clean shutdown as a *revocation*, not a loss.
        //
        // `on_partitions_lost` means "these partitions were taken away and it
        // is no longer safe to commit them" — the trait documentation tells
        // implementors not to commit from it. A deliberate `close()` is the
        // opposite situation: the consumer still owns these partitions and
        // this is the listener's last chance to checkpoint. Firing the lost
        // callback here means a listener that does its final commit in
        // `on_partitions_revoked`, as documented, never gets to run it.
        //
        // Java draws the same distinction in `onLeavePrepare`.
        let revoked: Vec<TopicPartition> = {
            let assignments = self.assignments.read().await;
            assignments
                .iter()
                .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
                .collect()
        };
        if !revoked.is_empty() {
            self.safe_on_partitions_revoked(&revoked).await;
        }

        // Leave consumer group if we have a group coordinator
        let leave_group_result = if let Some(ref coordinator) = self.group_coordinator {
            coordinator.leave_group().await
        } else {
            Ok(())
        };

        // Clear per-partition state so post-close recv() cannot return records
        // from partitions already signaled as lost via on_partitions_lost above.
        self.subscriptions.write().await.clear();
        self.assignments.write().await.clear();
        self.clear_partition_state().await;
        self.metrics.assigned_partitions.set(0);

        // Notify interceptor of shutdown
        crate::interceptor::safe_consumer_close(&*self.interceptor);

        // A pool borrowed from a `KrafkaClient` belongs to that client: tearing
        // it down here would kill every sibling producer, admin client and
        // consumer sharing it and fail their in-flight requests. `AdminClient`
        // already got this right; its siblings did not.
        if self.pool_owned {
            self.pool.close_all().await;
            info!("Consumer closed (connection pool torn down)");
        } else {
            info!("Consumer closed (shared connection pool left open)");
        }

        Self::select_close_result(auto_commit_result, leave_group_result)
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

    /// Check if the consumer is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Committed offsets for the given partitions, read from the group
    /// coordinator.
    ///
    /// This is the authoritative answer to "where is this group?", as opposed
    /// to [`position`](Self::position), which reports where *this consumer*
    /// will read next. The two differ by exactly the records consumed but not
    /// yet committed, which is the window an at-least-once pipeline replays
    /// after a crash.
    ///
    /// A partition the group has never committed is absent from the returned
    /// map rather than reported as `0` — those are very different states, and
    /// conflating them is how a monitoring dashboard reports a healthy group
    /// sitting at the start of a topic.
    ///
    /// Requires a `group_id`; returns [`KrafkaError::InvalidState`] for an
    /// assign-only consumer, which has no coordinator to ask.
    ///
    /// Counterpart to Java's `KafkaConsumer.committed(Set<TopicPartition>)`.
    ///
    /// ```no_run
    /// # use krafka::consumer::Consumer;
    /// # async fn f(consumer: &Consumer) -> krafka::error::Result<()> {
    /// let committed = consumer.committed(&[("orders", 0), ("orders", 1)]).await?;
    /// for ((topic, partition), pos) in &committed {
    ///     println!("{topic}-{partition} committed at {}", pos.offset);
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn committed(
        &self,
        partitions: &[(&str, PartitionId)],
    ) -> Result<HashMap<(String, PartitionId), CommittedPosition>> {
        if self.is_closed() {
            return Err(KrafkaError::invalid_state("consumer is closed"));
        }

        let Some(ref coordinator) = self.group_coordinator else {
            return Err(KrafkaError::invalid_state(
                "committed() requires a group_id; an assign-only consumer has no \
                 coordinator to read committed offsets from",
            ));
        };

        if partitions.is_empty() {
            return Ok(HashMap::new());
        }

        let mut by_topic: HashMap<String, Vec<PartitionId>> = HashMap::new();
        for (topic, partition) in partitions {
            validate_topic_name(topic)?;
            by_topic
                .entry((*topic).to_string())
                .or_default()
                .push(*partition);
        }

        coordinator.fetch_committed_offsets(&by_topic).await
    }

    /// Interrupt [`poll()`](Self::poll) / [`recv()`](Self::recv) from another
    /// task.
    ///
    /// A `poll()` already parked on a broker fetch is interrupted without
    /// waiting for the brokers; a `poll()` that has not started yet returns
    /// the same error immediately, so a `wakeup()` racing the call is not
    /// lost. Records already fetched by that `poll()` are still returned
    /// rather than discarded — throwing them away would advance offsets with
    /// nothing delivered.
    ///
    /// The consumer stays usable: the next `poll()` proceeds normally. Safe to
    /// call concurrently with any other consumer method.
    ///
    /// This is the counterpart to Java's `KafkaConsumer.wakeup()`, and
    /// mirrors [`ShareConsumer::wakeup`](crate::share_consumer::ShareConsumer::wakeup).
    /// Dropping the `poll()` future also cancels it, but only from the task
    /// that owns the future — `wakeup()` works from any task, which is the
    /// case a shutdown handler actually has.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use std::time::Duration;
    /// # async fn f(
    /// #     consumer: Arc<krafka::consumer::Consumer>,
    /// #     shutdown: tokio::sync::oneshot::Receiver<()>,
    /// # ) {
    /// let c = Arc::clone(&consumer);
    /// tokio::spawn(async move {
    ///     let _ = shutdown.await;
    ///     c.wakeup(); // unblocks the poll loop below, from another task
    /// });
    ///
    /// while let Ok(records) = consumer.poll(Duration::from_secs(30)).await {
    ///     for record in records {
    ///         let _ = record;
    ///     }
    /// }
    /// # }
    /// ```
    #[inline]
    pub fn wakeup(&self) {
        self.wakeup_flag
            .store(true, std::sync::atomic::Ordering::Release);
        self.wakeup_notify.notify_waiters();
    }

    /// A live snapshot of this consumer's identity within its group.
    ///
    /// Returns `None` when the consumer has no `group_id`, or when it has not
    /// yet completed a join and therefore has no identity to report.
    ///
    /// # What it is for
    ///
    /// Pass this to a transactional producer when committing consumer offsets
    /// inside a transaction. It lets the group coordinator fence commits from
    /// a zombie consumer — one that was partitioned away, lost its partitions
    /// to a rebalance, and then returned. Without it the coordinator accepts
    /// the zombie's commit unconditionally and overwrites the position of the
    /// member that now owns those partitions, breaking exactly-once. See
    /// [`ConsumerGroupMetadata`].
    ///
    /// # Re-read it for every transaction
    ///
    /// The generation changes on every rebalance, so a cached snapshot goes
    /// stale without warning. Call this again for each transaction rather than
    /// holding on to the result:
    ///
    /// ```ignore
    /// loop {
    ///     let records = consumer.poll(Duration::from_secs(1)).await?;
    ///     producer.begin_transaction()?;
    ///     // ... produce derived records ...
    ///     let metadata = consumer.group_metadata().await
    ///         .ok_or_else(|| anyhow!("consumer is not in a group"))?;
    ///     producer.send_offsets_to_transaction(&offsets, &metadata).await?;
    ///     producer.commit_transaction().await?;
    /// }
    /// ```
    pub async fn group_metadata(&self) -> Option<ConsumerGroupMetadata> {
        self.group_coordinator.as_ref()?.group_metadata().await
    }

    /// Get the group coordinator, if one is configured.
    #[inline]
    pub fn group_coordinator(&self) -> Option<&Arc<GroupCoordinator>> {
        self.group_coordinator.as_ref()
    }

    /// Get a snapshot of consumer metrics.
    #[inline]
    pub fn metrics(&self) -> &Arc<ConsumerMetrics> {
        &self.metrics
    }

    /// Get the shared connection metrics handle used by this consumer's broker pool.
    #[inline]
    pub fn connection_metrics(&self) -> Arc<ConnectionMetrics> {
        self.pool.metrics()
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        // Warn when a consumer is dropped without an explicit `close()`.
        // Skipping `close()` means the broker will not see a `LeaveGroup`
        // and the partitions will only be reassigned after
        // `session.timeout.ms` expires, stalling the rest of the group.
        // Skip during panic unwinding.
        if !self.closed.load(std::sync::atomic::Ordering::SeqCst) && !std::thread::panicking() {
            warn!(
                "Consumer dropped without close(); group rebalance will be delayed \
                 until session.timeout.ms. Call `Consumer::close()` before drop."
            );
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use tokio::sync::RwLock;

    #[tokio::test]
    async fn test_consumer_builder_no_servers() {
        let result = Consumer::builder().build().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_paused_partitions_set() {
        let mut paused: HashSet<(String, PartitionId)> = HashSet::new();
        paused.insert(("topic1".to_string(), 0));
        paused.insert(("topic1".to_string(), 1));
        paused.insert(("topic2".to_string(), 0));

        assert!(paused.contains(&("topic1".to_string(), 0)));
        assert!(paused.contains(&("topic1".to_string(), 1)));
        assert!(paused.contains(&("topic2".to_string(), 0)));
        assert!(!paused.contains(&("topic2".to_string(), 1)));

        paused.remove(&("topic1".to_string(), 0));
        assert!(!paused.contains(&("topic1".to_string(), 0)));
    }

    #[test]
    fn test_topic_partition() {
        let tp = TopicPartition::new("my-topic", 3);
        assert_eq!(tp.topic(), "my-topic");
        assert_eq!(tp.partition(), 3);

        // Test Hash/Eq for HashMap use
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("test", 0), 100i64);
        map.insert(TopicPartition::new("test", 1), 200i64);
        assert_eq!(map.get(&TopicPartition::new("test", 0)), Some(&100i64));
        assert_eq!(map.get(&TopicPartition::new("test", 1)), Some(&200i64));
    }

    #[test]
    fn test_offset_and_metadata() {
        let offset = OffsetAndMetadata::new(100);
        assert_eq!(offset.offset, 100);
        assert!(offset.metadata.is_none());

        let offset_with_meta = OffsetAndMetadata::with_metadata(200, "checkpoint-123");
        assert_eq!(offset_with_meta.offset, 200);
        assert_eq!(offset_with_meta.metadata.as_deref(), Some("checkpoint-123"));

        let offset_with_epoch = OffsetAndMetadata::with_epoch(300, 5);
        assert_eq!(offset_with_epoch.offset, 300);
        assert_eq!(offset_with_epoch.leader_epoch, Some(5));
    }

    #[test]
    fn test_partition_assignment_strategy_default() {
        let config = ConsumerConfig::default();
        assert_eq!(
            config.partition_assignment_strategy(),
            PartitionAssignmentStrategy::Range
        );
    }

    #[test]
    fn test_partition_assignment_strategy_protocol_name() {
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
    fn test_consumer_config_defaults() {
        let config = ConsumerConfig::default();
        // Verify sensible defaults
        assert!(config.fetch_max_bytes > 0);
        assert!(config.fetch_min_bytes > 0);
        assert!(config.max_partition_fetch_bytes > 0);
    }

    #[tokio::test]
    async fn test_consumer_builder_rejects_bad_heartbeat() {
        // heartbeat_interval >= session_timeout should fail
        let result = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test")
            .session_timeout(Duration::from_secs(5))
            .heartbeat_interval(Duration::from_secs(5))
            .build()
            .await;

        match result {
            Err(e) => assert!(e.to_string().contains("heartbeat_interval")),
            Ok(_) => panic!("expected error for heartbeat_interval >= session_timeout"),
        }
    }

    #[tokio::test]
    async fn test_consumer_builder_rejects_heartbeat_greater_than_session() {
        let result = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test")
            .session_timeout(Duration::from_secs(5))
            .heartbeat_interval(Duration::from_secs(10))
            .build()
            .await;

        assert!(result.is_err());
    }

    /// Verify max_poll_records truncation recomputes offset updates
    /// to prevent data loss for undelivered records.
    #[test]
    fn test_max_poll_records_offset_recomputation() {
        // Simulate what poll() does: given 5 records but max_poll_records=3,
        // only offsets for the first 3 records should be advanced.
        let records: Vec<ConsumerRecord> = (0..5)
            .map(|i| ConsumerRecord {
                topic: "topic1".to_string(),
                partition: 0,
                offset: 100 + i,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: Some(bytes::Bytes::from(format!("val-{i}"))),
                headers: vec![],
                leader_epoch: None,
                delivery_count: None,
            })
            .collect();

        let original_offset_updates: Vec<((String, PartitionId), Offset)> =
            vec![(("topic1".to_string(), 0), 105)]; // offset after last record

        let max = 3usize;
        let mut truncated = records;
        truncated.truncate(max);

        // Recompute offsets from truncated records only
        let mut delivered_offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        for r in &truncated {
            let key = (r.topic.clone(), r.partition);
            let entry = delivered_offsets.entry(key).or_insert(r.offset);
            if r.offset > *entry {
                *entry = r.offset;
            }
        }
        let new_offset_updates: Vec<_> = delivered_offsets
            .into_iter()
            .map(|(key, offset)| (key, offset + 1))
            .collect();

        // Should advance to offset 103 (100+2+1), NOT 105
        assert_eq!(new_offset_updates.len(), 1);
        let (key, offset) = &new_offset_updates[0];
        assert_eq!(key, &("topic1".to_string(), 0));
        assert_eq!(*offset, 103); // 100 + 2 (last delivered record offset) + 1

        // Not the original 105
        assert_ne!(*offset, original_offset_updates[0].1);
    }

    /// Verify max_poll_records with multiple partitions recomputes
    /// offsets correctly per partition.
    #[test]
    fn test_max_poll_records_multi_partition_offset() {
        let mut records = Vec::new();
        // 3 records from partition 0
        for i in 0..3 {
            records.push(ConsumerRecord {
                topic: "topic1".to_string(),
                partition: 0,
                offset: 50 + i,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: Some(bytes::Bytes::from("val")),
                headers: vec![],
                leader_epoch: None,
                delivery_count: None,
            });
        }
        // 3 records from partition 1
        for i in 0..3 {
            records.push(ConsumerRecord {
                topic: "topic1".to_string(),
                partition: 1,
                offset: 200 + i,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: Some(bytes::Bytes::from("val")),
                headers: vec![],
                leader_epoch: None,
                delivery_count: None,
            });
        }

        // Truncate to 4 records (all 3 from p0 + 1 from p1)
        records.truncate(4);

        let mut delivered_offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        for r in &records {
            let key = (r.topic.clone(), r.partition);
            let entry = delivered_offsets.entry(key).or_insert(r.offset);
            if r.offset > *entry {
                *entry = r.offset;
            }
        }

        // Partition 0: last delivered = 52 → advanced to 53
        assert_eq!(
            *delivered_offsets.get(&("topic1".to_string(), 0)).unwrap(),
            52
        );
        // Partition 1: last delivered = 200 → advanced to 201
        assert_eq!(
            *delivered_offsets.get(&("topic1".to_string(), 1)).unwrap(),
            200
        );
    }

    #[tokio::test]
    async fn test_recv_buffer_returns_all_records() {
        use std::collections::VecDeque;

        // Simulate a consumer with pre-filled recv_buffer
        let mut buffer = VecDeque::new();
        buffer.push_back(ConsumerRecord {
            topic: "t".into(),
            partition: 0,
            offset: 1,
            timestamp: 0,
            timestamp_type: 0,
            key: None,
            value: Some(bytes::Bytes::from("r1")),
            headers: vec![],
            leader_epoch: None,
            delivery_count: None,
        });
        buffer.push_back(ConsumerRecord {
            topic: "t".into(),
            partition: 0,
            offset: 2,
            timestamp: 0,
            timestamp_type: 0,
            key: None,
            value: Some(bytes::Bytes::from("r2")),
            headers: vec![],
            leader_epoch: None,
            delivery_count: None,
        });

        assert_eq!(buffer.len(), 2);
        let first = buffer.pop_front().unwrap();
        assert_eq!(first.offset, 1);
        let second = buffer.pop_front().unwrap();
        assert_eq!(second.offset, 2);
        assert!(buffer.is_empty());
    }

    // subscribe() replaces rather than appending.
    #[test]
    fn test_subscribe_replaces_subscriptions() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let subs: RwLock<HashSet<String>> = RwLock::new(HashSet::new());

            // First subscribe
            {
                let mut s = subs.write().await;
                s.clear(); // clear before insert
                s.insert("topic1".to_string());
            }
            assert_eq!(subs.read().await.len(), 1);
            assert!(subs.read().await.contains("topic1"));

            // Second subscribe replaces, not appends
            {
                let mut s = subs.write().await;
                s.clear(); // clear before insert
                s.insert("topic2".to_string());
            }
            assert_eq!(subs.read().await.len(), 1);
            assert!(subs.read().await.contains("topic2"));
            assert!(!subs.read().await.contains("topic1"));
        });
    }

    // unsubscribe() clears offsets, paused, and recv_buffer.
    #[test]
    fn test_unsubscribe_clears_all_state() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let offsets: RwLock<HashMap<(String, PartitionId), Offset>> =
                RwLock::new(HashMap::new());
            let paused: RwLock<HashSet<(String, PartitionId)>> = RwLock::new(HashSet::new());
            let assignments: RwLock<HashMap<String, Vec<PartitionId>>> =
                RwLock::new(HashMap::new());
            let recv_buffer: RwLock<std::collections::VecDeque<ConsumerRecord>> =
                RwLock::new(std::collections::VecDeque::new());

            // Populate state
            offsets.write().await.insert(("t".into(), 0), 100);
            paused.write().await.insert(("t".into(), 0));
            assignments.write().await.insert("t".into(), vec![0]);
            recv_buffer.write().await.push_back(ConsumerRecord {
                topic: "t".into(),
                partition: 0,
                offset: 0,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: None,
                headers: vec![],
                leader_epoch: None,
                delivery_count: None,
            });

            // Simulate unsubscribe clearing
            offsets.write().await.clear();
            paused.write().await.clear();
            assignments.write().await.clear();
            recv_buffer.write().await.clear();

            assert!(offsets.read().await.is_empty());
            assert!(paused.read().await.is_empty());
            assert!(assignments.read().await.is_empty());
            assert!(recv_buffer.read().await.is_empty());
        });
    }

    // Fetch skips partitions with no tracked offset.
    #[test]
    fn test_fetch_skips_untracked_partitions() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let offsets: RwLock<HashMap<(String, PartitionId), Offset>> =
                RwLock::new(HashMap::new());
            offsets.write().await.insert(("t".into(), 0), 42);

            let o = offsets.read().await;
            // Partition 0 has an offset
            assert_eq!(o.get(&("t".to_string(), 0)).copied(), Some(42));
            // Partition 1 has no offset — should be skipped
            assert_eq!(o.get(&("t".to_string(), 1)).copied(), None);
        });
    }

    // ── Repositioning must not leave stale records in the receive buffer ──

    fn buffered(topic: &str, partition: PartitionId, offset: Offset) -> ConsumerRecord {
        ConsumerRecord {
            topic: topic.to_string(),
            partition,
            offset,
            timestamp: 0,
            timestamp_type: 0,
            key: None,
            value: None,
            headers: Vec::new(),
            leader_epoch: None,
            delivery_count: None,
        }
    }

    /// The defect this guards against is not "stale records get delivered" —
    /// it is that `committable_positions` clamps the commit down to the lowest
    /// still-buffered offset. After a `seek_to_end()` that clamp writes an
    /// offset from *before* the seek, moving the group backwards and
    /// re-delivering everything in between.
    #[test]
    fn purging_on_reposition_prevents_a_backwards_commit() {
        let mut buffer: std::collections::VecDeque<ConsumerRecord> =
            [buffered("orders", 0, 100), buffered("orders", 0, 101)]
                .into_iter()
                .collect();

        // The consumer has just been sought to the end of the log.
        let positions: HashMap<(String, PartitionId), Offset> =
            [(("orders".to_string(), 0), 5_000)].into_iter().collect();

        // Before the fix: the commit is dragged back to the buffered offset.
        let clamped = committable_positions(&positions, &buffer);
        assert_eq!(
            clamped.get(&("orders".to_string(), 0)).copied(),
            Some(100),
            "this is the bug: the commit would move the group backwards"
        );

        // After purging, the commit reflects the position that was sought to.
        let repositioned: HashSet<(String, PartitionId)> =
            [("orders".to_string(), 0)].into_iter().collect();
        assert_eq!(purge_buffered_records(&mut buffer, &repositioned), 2);
        let clamped = committable_positions(&positions, &buffer);
        assert_eq!(
            clamped.get(&("orders".to_string(), 0)).copied(),
            Some(5_000)
        );
    }

    #[test]
    fn purging_leaves_other_partitions_untouched() {
        let mut buffer: std::collections::VecDeque<ConsumerRecord> = [
            buffered("orders", 0, 10),
            buffered("orders", 1, 20),
            buffered("payments", 0, 30),
        ]
        .into_iter()
        .collect();

        let repositioned: HashSet<(String, PartitionId)> =
            [("orders".to_string(), 0)].into_iter().collect();
        assert_eq!(purge_buffered_records(&mut buffer, &repositioned), 1);
        assert_eq!(buffer.len(), 2);
        assert!(
            buffer
                .iter()
                .all(|r| !(r.topic == "orders" && r.partition == 0))
        );

        // An empty reposition set is a no-op, and so is an empty buffer.
        assert_eq!(purge_buffered_records(&mut buffer, &HashSet::new()), 0);
        assert_eq!(buffer.len(), 2);
    }

    /// `position`, `lag` and the commit are all derived from one boundary:
    /// the lowest offset still awaiting delivery. If they were computed
    /// separately they could disagree, and a consumer whose reported position
    /// is ahead of what it committed is indistinguishable from a bug.
    #[test]
    fn undelivered_boundary_is_shared_by_position_lag_and_commit() {
        let buffer: std::collections::VecDeque<ConsumerRecord> = [
            buffered("orders", 0, 120),
            buffered("orders", 0, 118),
            buffered("orders", 1, 7),
        ]
        .into_iter()
        .collect();

        let undelivered = lowest_undelivered_offsets(&buffer);
        assert_eq!(undelivered.len(), 2, "one entry per distinct partition");
        assert_eq!(
            undelivered.get(&("orders".to_string(), 0)).copied(),
            Some(118),
            "the *lowest* buffered offset is the boundary, not the first seen"
        );
        assert_eq!(
            undelivered.get(&("orders".to_string(), 1)).copied(),
            Some(7)
        );

        // The fetch position has read ahead to 200; delivery is at 118.
        let positions: HashMap<(String, PartitionId), Offset> = [
            (("orders".to_string(), 0), 200),
            (("orders".to_string(), 1), 10),
        ]
        .into_iter()
        .collect();

        // Commit follows the boundary.
        let committable = committable_positions(&positions, &buffer);
        assert_eq!(
            committable.get(&("orders".to_string(), 0)).copied(),
            Some(118)
        );

        // Lag follows the same boundary: the 82 records read ahead into the
        // buffer have not reached the application, so they are still lag.
        let partition_state: HashMap<(String, PartitionId), PartitionState> = [(
            ("orders".to_string(), 0),
            PartitionState {
                high_watermark: Some(200),
                ..PartitionState::default()
            },
        )]
        .into_iter()
        .collect();

        let (with_buffer, _) = compute_aggregate_lag(
            &positions,
            &partition_state,
            &undelivered,
            IsolationLevel::ReadUncommitted,
        );
        assert_eq!(
            with_buffer, 82,
            "records parked in the buffer must count as lag"
        );

        let (drained, _) = compute_aggregate_lag(
            &positions,
            &partition_state,
            &HashMap::new(),
            IsolationLevel::ReadUncommitted,
        );
        assert_eq!(drained, 0, "with nothing parked, the consumer is caught up");
    }

    #[test]
    fn undelivered_boundary_is_empty_for_an_empty_buffer() {
        let buffer: std::collections::VecDeque<ConsumerRecord> = std::collections::VecDeque::new();
        assert!(lowest_undelivered_offsets(&buffer).is_empty());
    }

    // ── pause() must be honoured by the buffer drain, not just by poll() ──

    #[test]
    fn drain_withholds_paused_partitions_without_dropping_them() {
        let mut buffer: std::collections::VecDeque<ConsumerRecord> = [
            buffered("orders", 0, 1),
            buffered("orders", 1, 2),
            buffered("orders", 0, 3),
        ]
        .into_iter()
        .collect();
        let paused: HashSet<(String, PartitionId)> =
            [("orders".to_string(), 0)].into_iter().collect();

        let mut batch = Vec::new();
        drain_buffered_records(&mut buffer, &mut batch, 10, &paused);

        assert_eq!(batch.len(), 1);
        assert_eq!(batch[0].partition, 1);
        // Paused records stay put: the fetch position is already past them, so
        // discarding would skip data outright.
        assert_eq!(buffer.len(), 2);
        assert!(buffer.iter().all(|r| r.partition == 0));
    }

    #[test]
    fn drain_fast_path_preserves_order_and_respects_max_records() {
        let mut buffer: std::collections::VecDeque<ConsumerRecord> =
            (0..5).map(|i| buffered("orders", 0, i)).collect();
        let mut batch = Vec::new();
        drain_buffered_records(&mut buffer, &mut batch, 3, &HashSet::new());
        assert_eq!(
            batch.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(buffer.len(), 2);
    }

    // ── read_committed measures against the LSO, not the high watermark ──

    #[test]
    fn readable_end_offset_follows_the_isolation_level() {
        let state = PartitionState {
            high_watermark: Some(1_000),
            last_stable_offset: Some(400),
            ..PartitionState::default()
        };

        // An open transaction sits between 400 and 1000. A read_committed
        // consumer at 400 has read everything it is allowed to read.
        assert_eq!(
            state.readable_end_offset(IsolationLevel::ReadCommitted),
            Some(400)
        );
        assert_eq!(
            state.readable_end_offset(IsolationLevel::ReadUncommitted),
            Some(1_000)
        );

        let offsets: HashMap<(String, PartitionId), Offset> =
            [(("orders".to_string(), 0), 400)].into_iter().collect();
        let partition_state: HashMap<(String, PartitionId), PartitionState> =
            [(("orders".to_string(), 0), state)].into_iter().collect();

        let (total, max) = compute_aggregate_lag(
            &offsets,
            &partition_state,
            &HashMap::new(),
            IsolationLevel::ReadCommitted,
        );
        assert_eq!(
            (total, max),
            (0, 0),
            "a drained read_committed consumer must not report phantom lag"
        );

        let (total, _) = compute_aggregate_lag(
            &offsets,
            &partition_state,
            &HashMap::new(),
            IsolationLevel::ReadUncommitted,
        );
        assert_eq!(total, 600);
    }

    #[test]
    fn readable_end_offset_falls_back_to_the_high_watermark() {
        // Fetch below v4, or before the first response for the partition.
        let state = PartitionState {
            high_watermark: Some(1_000),
            last_stable_offset: None,
            ..PartitionState::default()
        };
        assert_eq!(
            state.readable_end_offset(IsolationLevel::ReadCommitted),
            Some(1_000)
        );
    }

    // ── the poll-wide decode budget ──

    #[test]
    fn record_budget_is_claimed_in_batches_and_never_underflows() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let budget = AtomicUsize::new(500);
        assert_eq!(claim_record_budget(&budget, 200), 200);
        assert_eq!(budget.load(Ordering::Relaxed), 300);

        // A claim larger than what remains is granted only what remains.
        assert_eq!(claim_record_budget(&budget, 1_000), 300);
        assert_eq!(budget.load(Ordering::Relaxed), 0);

        // Exhausted: further claims yield nothing rather than wrapping.
        assert_eq!(claim_record_budget(&budget, 1), 0);
        assert_eq!(budget.load(Ordering::Relaxed), 0);

        // Unused slots are returned by the caller.
        budget.fetch_add(50, Ordering::Relaxed);
        assert_eq!(claim_record_budget(&budget, 80), 50);
    }

    // ── only ABORT markers end an aborted transaction ──

    #[test]
    fn control_batch_type_is_read_from_the_marker_key() {
        use crate::protocol::{Record, RecordBatch};

        let marker = |control_type: i16| {
            let mut key = bytes::BytesMut::new();
            bytes::BufMut::put_i16(&mut key, 0); // control-record version
            bytes::BufMut::put_i16(&mut key, control_type);
            let mut batch = RecordBatch::new();
            batch.attributes.is_control_batch = true;
            batch.add_record(Record {
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: Some(key.freeze()),
                value: Some(bytes::Bytes::new()),
                headers: Vec::new(),
            });
            batch
        };

        assert!(control_batch_is_abort(&marker(0)), "type 0 is ABORT");
        assert!(!control_batch_is_abort(&marker(1)), "type 1 is COMMIT");

        // A malformed or absent key leaves the filter engaged — the
        // conservative answer, since releasing it would surface aborted data.
        let mut short = RecordBatch::new();
        short.attributes.is_control_batch = true;
        short.add_record(Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key: Some(bytes::Bytes::from_static(&[0, 0])),
            value: None,
            headers: Vec::new(),
        });
        assert!(!control_batch_is_abort(&short));

        let mut keyless = RecordBatch::new();
        keyless.attributes.is_control_batch = true;
        assert!(!control_batch_is_abort(&keyless));
    }

    // ── decode_partition_batches: the batch walk that advances the position ──
    //
    // These pin down the cases where a batch's offset range carries nothing to
    // deliver but must still be advanced through, or the partition re-fetches
    // the same bytes forever. All of them are the log cleaner's doing: on a
    // compacted topic a batch can outlive its records (the header is retained
    // for producer idempotence) or keep a `last_offset_delta` that reaches
    // beyond its last surviving record. Regression tests for the production
    // stall where `poll()` returned empty forever once the position entered a
    // compacted region.

    /// Leader epoch stamped on every test batch, so tests can assert the
    /// epoch is reported alongside the position advance (KIP-320).
    const WALK_EPOCH: i32 = 7;

    fn walk_record(offset_delta: i32) -> crate::protocol::Record {
        crate::protocol::Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta,
            key: None,
            value: Some(bytes::Bytes::from_static(b"v")),
            headers: Vec::new(),
        }
    }

    /// A data batch spanning `base..=base + last_offset_delta` whose surviving
    /// records sit at the given deltas — the shape the log cleaner leaves
    /// behind when compaction removes records from a batch. An empty `deltas`
    /// is the fully-emptied batch the cleaner retains for producer state.
    fn walk_batch(base_offset: i64, last_offset_delta: i32, deltas: &[i32]) -> RecordBatch {
        let mut batch = RecordBatch::new();
        batch.base_offset = base_offset;
        batch.last_offset_delta = last_offset_delta;
        batch.partition_leader_epoch = WALK_EPOCH;
        for &delta in deltas {
            batch.add_record(walk_record(delta));
        }
        batch
    }

    /// Encode `batches` back-to-back — the wire shape of a fetch response
    /// payload — and walk them from `fetch_offset`.
    fn walk(
        batches: &[RecordBatch],
        fetch_offset: Offset,
        budget: Option<&std::sync::atomic::AtomicUsize>,
    ) -> (Vec<ConsumerRecord>, PartitionDecodeOutcome) {
        let mut buf = bytes::BytesMut::new();
        for batch in batches {
            buf.extend_from_slice(&batch.encode().expect("encode batch"));
        }
        let mut records = Vec::new();
        let outcome = decode_partition_batches(
            "walk-topic",
            0,
            buf.freeze(),
            fetch_offset,
            Vec::new(),
            budget,
            RecordBatch::MAX_DECOMPRESSED_SIZE,
            &mut records,
        );
        (records, outcome)
    }

    #[test]
    fn compaction_emptied_batch_is_skipped_not_a_stall() {
        // The cleaner kept only the header: base 1000, spanning 1000..=1004,
        // zero records. The walk must advance through the whole span.
        let (records, outcome) = walk(&[walk_batch(1000, 4, &[])], 1000, None);

        assert!(records.is_empty(), "an emptied batch delivers nothing");
        assert_eq!(
            outcome.last_offset,
            Some(1004),
            "the position must advance through the emptied batch's span, \
             or the next fetch re-reads it forever"
        );
        assert_eq!(outcome.last_epoch, WALK_EPOCH);
        assert!(outcome.error.is_none());
        assert!(!outcome.corrupt);
    }

    #[test]
    fn compaction_emptied_batch_does_not_discard_the_rest_of_the_response() {
        // Before the fix, an empty batch read as "budget exhausted" and broke
        // out of the walk, throwing away every later batch in the response —
        // with and without a budget configured.
        let batches = [walk_batch(1000, 4, &[]), walk_batch(1005, 1, &[0, 1])];

        for budget in [None, Some(std::sync::atomic::AtomicUsize::new(512))] {
            let (records, outcome) = walk(&batches, 1000, budget.as_ref());

            let offsets: Vec<Offset> = records.iter().map(|r| r.offset).collect();
            assert_eq!(offsets, vec![1005, 1006]);
            assert_eq!(outcome.last_offset, Some(1006));
            if let Some(budget) = budget {
                assert_eq!(
                    budget.load(std::sync::atomic::Ordering::Relaxed),
                    510,
                    "the emptied batch must not consume budget slots"
                );
            }
        }
    }

    #[test]
    fn drained_compacted_batch_advances_past_its_removed_tail() {
        // Compaction removed the records at deltas 1, 2, 4, and 6..=9; the
        // batch still spans 100..=109. Draining it must move the position to
        // the end of the *span*, not to the last surviving record — parking
        // at 106 would re-fetch this same batch and deliver nothing.
        let (records, outcome) = walk(&[walk_batch(100, 9, &[0, 3, 5])], 100, None);

        let offsets: Vec<Offset> = records.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, vec![100, 103, 105]);
        assert!(records.iter().all(|r| r.leader_epoch == Some(WALK_EPOCH)));
        assert_eq!(
            outcome.last_offset,
            Some(109),
            "a drained batch advances to its span end (Java client's nextFetchOffset)"
        );
    }

    #[test]
    fn position_inside_a_compacted_batch_escapes_it() {
        // The consumer previously delivered through 105 and parked at 106 —
        // inside the batch's span, above its last surviving record. The
        // broker returns the same batch; every record is below the fetch
        // offset. Before the fix nothing advanced the position and the
        // partition stalled with empty polls forever.
        let (records, outcome) = walk(&[walk_batch(100, 9, &[0, 3, 5])], 106, None);

        assert!(
            records.is_empty(),
            "everything in the batch was already delivered"
        );
        assert_eq!(
            outcome.last_offset,
            Some(109),
            "the position must escape the straddling batch"
        );
        assert_eq!(outcome.last_epoch, WALK_EPOCH);
    }

    #[test]
    fn budget_cut_batch_re_fetches_its_remainder() {
        // The batch-span advance applies only to batches walked in full: a
        // batch cut short by `max_poll_records` must leave the position at
        // the last delivered record so the remainder is re-fetched.
        let budget = std::sync::atomic::AtomicUsize::new(2);
        let (records, outcome) = walk(&[walk_batch(100, 2, &[0, 1, 2])], 100, Some(&budget));

        let offsets: Vec<Offset> = records.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, vec![100, 101]);
        assert_eq!(
            outcome.last_offset,
            Some(101),
            "a budget-cut batch must not be skipped over"
        );
        assert_eq!(budget.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[test]
    fn batch_below_the_fetch_position_cannot_rewind_it() {
        // A batch lying entirely below the fetch position describes offsets
        // already delivered. Advancing "through" it would compute a next
        // position *behind* the current one and re-deliver records. No broker
        // should send this shape; the guard keeps a buggy one duplicate-free.
        let (records, outcome) = walk(&[walk_batch(100, 4, &[])], 200, None);

        assert!(records.is_empty());
        assert_eq!(
            outcome.last_offset, None,
            "no position update at all beats a backwards one"
        );
    }

    #[test]
    fn truncated_tail_after_a_skipped_batch_is_benign() {
        // The broker cut the response mid-batch (partition_max_bytes). When
        // the walk already advanced — even if only through an emptied batch,
        // with nothing delivered — the truncation is the expected benign case,
        // not a fault: the advance is what proves the next fetch will make
        // progress past it.
        let mut buf = bytes::BytesMut::new();
        buf.extend_from_slice(&walk_batch(1000, 4, &[]).encode().expect("encode"));
        let tail = walk_batch(1005, 1, &[0, 1]).encode().expect("encode");
        buf.extend_from_slice(&tail[..tail.len() - 5]);

        let mut records = Vec::new();
        let outcome = decode_partition_batches(
            "walk-topic",
            0,
            buf.freeze(),
            1000,
            Vec::new(),
            None,
            RecordBatch::MAX_DECOMPRESSED_SIZE,
            &mut records,
        );

        assert!(records.is_empty());
        assert_eq!(outcome.last_offset, Some(1004));
        assert!(outcome.error.is_none(), "a truncated tail is not a fault");
        assert!(!outcome.corrupt);
    }

    #[test]
    fn corrupt_first_batch_reports_a_fault() {
        // Nothing decoded and nothing advanced: the partition is stuck at the
        // fetch offset, and the walk must say so rather than stall silently.
        let mut bad = bytes::BytesMut::from(
            walk_batch(1000, 1, &[0, 1])
                .encode()
                .expect("encode")
                .as_ref(),
        );
        bad[30] ^= 0xFF; // flip a CRC-covered byte

        let mut records = Vec::new();
        let outcome = decode_partition_batches(
            "walk-topic",
            0,
            bad.freeze(),
            1000,
            Vec::new(),
            None,
            RecordBatch::MAX_DECOMPRESSED_SIZE,
            &mut records,
        );

        assert!(records.is_empty());
        assert_eq!(outcome.last_offset, None);
        assert!(
            outcome.error.is_some(),
            "an undecodable position is a reported fault"
        );
        assert!(outcome.corrupt);
    }

    #[test]
    fn aborted_transaction_batches_are_filtered_but_advance_the_position() {
        // producer 9's transaction at 100..=102 was aborted; its abort marker
        // sits at 103; a later batch from the same producer at 104..=105 is
        // committed. READ_COMMITTED must deliver only the latter, while the
        // position still advances through everything it skipped.
        let mut aborted_data = walk_batch(100, 2, &[0, 1, 2]);
        aborted_data.attributes.is_transactional = true;
        aborted_data.producer_id = 9;

        let mut marker_key = bytes::BytesMut::new();
        bytes::BufMut::put_i16(&mut marker_key, 0); // control-record version
        bytes::BufMut::put_i16(&mut marker_key, 0); // type 0 = ABORT
        let mut abort_marker = walk_batch(103, 0, &[]);
        abort_marker.attributes.is_control_batch = true;
        abort_marker.producer_id = 9;
        abort_marker.add_record(crate::protocol::Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key: Some(marker_key.freeze()),
            value: Some(bytes::Bytes::new()),
            headers: Vec::new(),
        });

        let mut committed_data = walk_batch(104, 1, &[0, 1]);
        committed_data.attributes.is_transactional = true;
        committed_data.producer_id = 9;

        let mut buf = bytes::BytesMut::new();
        for batch in [&aborted_data, &abort_marker, &committed_data] {
            buf.extend_from_slice(&batch.encode().expect("encode"));
        }

        let mut records = Vec::new();
        let outcome = decode_partition_batches(
            "walk-topic",
            0,
            buf.freeze(),
            100,
            vec![crate::protocol::AbortedTransaction {
                producer_id: 9,
                first_offset: 100,
            }],
            None,
            RecordBatch::MAX_DECOMPRESSED_SIZE,
            &mut records,
        );

        let offsets: Vec<Offset> = records.iter().map(|r| r.offset).collect();
        assert_eq!(
            offsets,
            vec![104, 105],
            "only the committed transaction is delivered"
        );
        assert_eq!(outcome.last_offset, Some(105));
    }

    #[test]
    fn advance_through_batch_never_moves_backwards() {
        let mut last_offset = Some(500);
        let mut last_epoch = 3;

        // A batch end behind what this response already advanced through
        // leaves the bookkeeping untouched — epoch included.
        advance_through_batch(&mut last_offset, &mut last_epoch, 100, 400, 9);
        assert_eq!(last_offset, Some(500));
        assert_eq!(last_epoch, 3);

        // A batch end below the fetch position is ignored outright.
        advance_through_batch(&mut last_offset, &mut last_epoch, 700, 600, 9);
        assert_eq!(last_offset, Some(500));

        // Forward movement takes the new end and its epoch.
        advance_through_batch(&mut last_offset, &mut last_epoch, 100, 600, 9);
        assert_eq!(last_offset, Some(600));
        assert_eq!(last_epoch, 9);
    }

    // Commit filtering uses group_coordinator check, not assigned_set emptiness.
    #[test]
    fn test_commit_filter_does_not_leak_stale_offsets() {
        let offsets: HashMap<(String, PartitionId), Offset> = [
            (("topic1".into(), 0), 100),
            (("topic2".into(), 0), 200), // stale: not assigned
        ]
        .into_iter()
        .collect();

        let assigned_set: HashSet<(String, PartitionId)> = HashSet::new();

        let no_epochs = HashMap::new();
        let filtered =
            Consumer::build_commit_offsets(&offsets, &no_epochs, Some(&assigned_set), true)
                .expect("empty assigned set is valid and must filter everything");

        assert!(filtered.is_empty());
    }

    #[test]
    fn test_commit_filter_requires_assignment_snapshot_for_group_commit() {
        let offsets: HashMap<(String, PartitionId), Offset> =
            [(("topic1".into(), 0), 100)].into_iter().collect();

        let no_epochs = HashMap::new();
        let error = Consumer::build_commit_offsets(&offsets, &no_epochs, None, true)
            .expect_err("group commits require an assignment snapshot");

        assert!(
            error
                .to_string()
                .contains("assignments snapshot unavailable")
        );
    }

    #[test]
    fn test_commit_with_metadata_filter_does_not_leak_stale_offsets() {
        let offsets: HashMap<TopicPartition, OffsetAndMetadata> = [
            (
                TopicPartition::new("topic1", 0),
                OffsetAndMetadata::with_metadata(100, "keep"),
            ),
            (
                TopicPartition::new("topic2", 0),
                OffsetAndMetadata::with_metadata(200, "stale"),
            ),
        ]
        .into_iter()
        .collect();

        let assigned_set: HashSet<(String, PartitionId)> = HashSet::new();

        let filtered =
            Consumer::filter_commit_with_metadata_offsets(offsets, Some(&assigned_set), true)
                .expect("empty assigned set is valid and must filter everything");

        assert!(filtered.is_empty());
    }

    #[test]
    fn test_commit_with_metadata_filter_keeps_all_offsets_without_group() {
        let offsets: HashMap<TopicPartition, OffsetAndMetadata> = [
            (
                TopicPartition::new("topic1", 0),
                OffsetAndMetadata::new(100),
            ),
            (
                TopicPartition::new("topic2", 1),
                OffsetAndMetadata::new(200),
            ),
        ]
        .into_iter()
        .collect();

        let filtered = Consumer::filter_commit_with_metadata_offsets(offsets, None, false)
            .expect("standalone consumers should commit all provided offsets");

        assert_eq!(filtered.len(), 2);
    }

    #[tokio::test]
    async fn test_offset_commit_handle_ready_flattens_result() {
        OffsetCommitHandle::ready(Ok(()))
            .await
            .expect("ready ok result");

        let error = OffsetCommitHandle::ready(Err(KrafkaError::invalid_state("boom")))
            .await
            .expect_err("ready error must surface");
        assert!(error.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn test_offset_commit_handle_flattens_task_result() {
        let error = OffsetCommitHandle::Task(tokio::spawn(async {
            Err(KrafkaError::invalid_state("task failed"))
        }))
        .await
        .expect_err("task error must surface");

        assert!(error.to_string().contains("task failed"));
    }

    #[tokio::test]
    async fn test_retry_commit_with_succeeds_after_retriable_errors() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        Consumer::retry_commit_with({
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    let attempt = attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    if attempt < 2 {
                        Err(KrafkaError::broker(
                            crate::error::ErrorCode::CoordinatorLoadInProgress,
                            "retry",
                        ))
                    } else {
                        Ok(())
                    }
                }
            }
        })
        .await
        .expect("retriable errors should eventually succeed");

        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_commit_with_returns_last_retriable_error_after_exhaustion() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let error = Consumer::retry_commit_with({
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err(KrafkaError::broker(
                        crate::error::ErrorCode::CoordinatorLoadInProgress,
                        "retry",
                    ))
                }
            }
        })
        .await
        .expect_err("exhausted retriable errors must surface the final error");

        assert!(matches!(
            error,
            KrafkaError::Broker {
                code: crate::error::ErrorCode::CoordinatorLoadInProgress,
                ..
            }
        ));
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_commit_with_stops_on_non_retriable_error() {
        let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let error = Consumer::retry_commit_with({
            let attempts = attempts.clone();
            move || {
                let attempts = attempts.clone();
                async move {
                    attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    Err(KrafkaError::broker(
                        crate::error::ErrorCode::GroupAuthorizationFailed,
                        "stop",
                    ))
                }
            }
        })
        .await
        .expect_err("non-retriable errors must stop immediately");

        assert!(matches!(
            error,
            KrafkaError::Broker {
                code: crate::error::ErrorCode::GroupAuthorizationFailed,
                ..
            }
        ));
        assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[test]
    fn test_select_close_result_prefers_auto_commit_error() {
        let error = Consumer::select_close_result(
            Err(KrafkaError::invalid_state("commit failed")),
            Err(KrafkaError::invalid_state("leave failed")),
        )
        .expect_err("auto-commit error must take precedence");

        assert!(error.to_string().contains("commit failed"));
    }

    #[test]
    fn test_select_close_result_ignores_rebalance_close_commit_error() {
        let error = Consumer::select_close_result(
            Err(KrafkaError::broker(
                crate::error::ErrorCode::UnknownMemberId,
                "rebalance needed",
            )),
            Err(KrafkaError::invalid_state("leave failed")),
        )
        .expect_err("leave-group error must surface when close-time commit error is benign");

        assert!(error.to_string().contains("leave failed"));
    }

    #[test]
    fn test_select_close_result_swallows_rebalance_close_commit_error_when_leave_succeeds() {
        Consumer::select_close_result(
            Err(KrafkaError::broker(
                crate::error::ErrorCode::RebalanceInProgress,
                "rebalance needed",
            )),
            Ok(()),
        )
        .expect("rebalance-related close-time commit errors should be ignored");
    }

    #[test]
    fn test_select_close_result_returns_leave_group_error_when_commit_succeeds() {
        let error =
            Consumer::select_close_result(Ok(()), Err(KrafkaError::invalid_state("leave failed")))
                .expect_err("leave-group error must surface when commit succeeded");

        assert!(error.to_string().contains("leave failed"));
    }

    #[test]
    fn test_max_poll_interval_used_for_rebalance() {
        // rebalance_timeout should default to max_poll_interval (not session_timeout)
        let config = ConsumerConfig::default();
        // In the Java client, rebalance_timeout defaults to max.poll.interval.ms (300s)
        // not session.timeout.ms (45s). Verify our config has both.
        assert_eq!(config.max_poll_interval, Duration::from_secs(300));
        assert_eq!(
            config.session_timeout,
            Duration::from_secs(45),
            "session_timeout matches Java/librdkafka since Kafka 3.0; the older 10s \
             default caused spurious rebalances under GC pauses and is rejected by \
             brokers with group.min.session.timeout.ms > 10000"
        );
        // The rebalance_timeout passed to GroupCoordinator should be max_poll_interval
        assert!(config.max_poll_interval > config.session_timeout);
    }

    /// Test that partition grouping by leader works correctly.
    /// This mirrors the grouping logic inside resolve_list_offsets.
    #[test]
    fn test_list_offsets_partition_grouping_by_leader() {
        // Simulate the leader-based grouping that resolve_list_offsets performs.
        let leader_map: HashMap<(&str, PartitionId), crate::BrokerId> = [
            (("topic1", 0), 1),
            (("topic1", 1), 2),
            (("topic2", 0), 1), // same leader as topic1-0
            (("topic2", 1), 3),
        ]
        .into_iter()
        .collect();

        let mut partitions: HashMap<String, Vec<PartitionId>> = HashMap::new();
        partitions.insert("topic1".to_string(), vec![0, 1]);
        partitions.insert("topic2".to_string(), vec![0, 1]);

        let mut by_leader: HashMap<crate::BrokerId, Vec<(String, PartitionId)>> = HashMap::new();
        for (topic, parts) in &partitions {
            for &p in parts {
                if let Some(&leader) = leader_map.get(&(topic.as_str(), p)) {
                    by_leader
                        .entry(leader)
                        .or_default()
                        .push((topic.clone(), p));
                }
            }
        }

        // Broker 1 should have topic1-0 and topic2-0
        assert_eq!(by_leader[&1].len(), 2);
        assert!(by_leader[&1].contains(&("topic1".to_string(), 0)));
        assert!(by_leader[&1].contains(&("topic2".to_string(), 0)));
        // Broker 2 should have topic1-1
        assert_eq!(by_leader[&2].len(), 1);
        assert_eq!(by_leader[&2][0], ("topic1".to_string(), 1));
        // Broker 3 should have topic2-1
        assert_eq!(by_leader[&3].len(), 1);
        assert_eq!(by_leader[&3][0], ("topic2".to_string(), 1));
    }

    /// Test request construction from grouped partitions.
    #[test]
    fn test_list_offsets_request_construction() {
        let leader_partitions: Vec<(String, PartitionId)> = vec![
            ("topic1".to_string(), 0),
            ("topic1".to_string(), 2),
            ("topic2".to_string(), 1),
        ];
        let timestamp = -1i64; // latest

        let mut topics_map: HashMap<String, Vec<ListOffsetsRequestPartition>> = HashMap::new();
        for (topic, partition) in &leader_partitions {
            topics_map
                .entry(topic.clone())
                .or_default()
                .push(ListOffsetsRequestPartition {
                    partition_index: *partition,
                    current_leader_epoch: -1,
                    timestamp,
                });
        }

        let topics: Vec<ListOffsetsRequestTopic> = topics_map
            .into_iter()
            .map(|(name, parts)| ListOffsetsRequestTopic {
                name,
                partitions: parts,
            })
            .collect();

        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics,
            timeout_ms: None,
        };

        assert_eq!(request.replica_id, -1);
        assert_eq!(request.topics.len(), 2);

        // Find topic1 and topic2 in the request
        let t1 = request.topics.iter().find(|t| t.name == "topic1").unwrap();
        assert_eq!(t1.partitions.len(), 2);
        assert!(t1.partitions.iter().any(|p| p.partition_index == 0));
        assert!(t1.partitions.iter().any(|p| p.partition_index == 2));
        for p in &t1.partitions {
            assert_eq!(p.timestamp, -1);
            assert_eq!(p.current_leader_epoch, -1);
        }

        let t2 = request.topics.iter().find(|t| t.name == "topic2").unwrap();
        assert_eq!(t2.partitions.len(), 1);
        assert_eq!(t2.partitions[0].partition_index, 1);
    }

    /// Test response result extraction — every partition maps to Ok(offset).
    #[test]
    fn test_list_offsets_response_result_extraction() {
        use crate::error::ErrorCode;
        use crate::protocol::ListOffsetsResponsePartition;
        use crate::protocol::ListOffsetsResponseTopic;

        let response = ListOffsetsResponse {
            topics: vec![
                ListOffsetsResponseTopic {
                    name: "topic1".to_string(),
                    partitions: vec![
                        ListOffsetsResponsePartition {
                            partition_index: 0,
                            error_code: ErrorCode::None,
                            timestamp: -1,
                            offset: 42,
                            leader_epoch: -1,
                        },
                        ListOffsetsResponsePartition {
                            partition_index: 1,
                            error_code: ErrorCode::None,
                            timestamp: -1,
                            offset: 100,
                            leader_epoch: -1,
                        },
                    ],
                },
                ListOffsetsResponseTopic {
                    name: "topic2".to_string(),
                    partitions: vec![ListOffsetsResponsePartition {
                        partition_index: 0,
                        error_code: ErrorCode::None,
                        timestamp: -1,
                        offset: 7,
                        leader_epoch: -1,
                    }],
                },
            ],
        };

        let mut result: HashMap<(String, PartitionId), Result<Offset>> = HashMap::new();
        apply_list_offsets_response(&response, &mut result);

        assert_eq!(result.len(), 3);
        assert_eq!(*result[&("topic1".to_string(), 0)].as_ref().unwrap(), 42);
        assert_eq!(*result[&("topic1".to_string(), 1)].as_ref().unwrap(), 100);
        assert_eq!(*result[&("topic2".to_string(), 0)].as_ref().unwrap(), 7);
    }

    /// Test partial failure — successful partitions map to Ok, failed partition
    /// maps to Err. No partition is dropped from the result.
    #[test]
    fn test_list_offsets_partial_failure_keeps_successes() {
        use crate::error::ErrorCode;
        use crate::protocol::ListOffsetsResponsePartition;
        use crate::protocol::ListOffsetsResponseTopic;

        let response = ListOffsetsResponse {
            topics: vec![ListOffsetsResponseTopic {
                name: "topic1".to_string(),
                partitions: vec![
                    ListOffsetsResponsePartition {
                        partition_index: 0,
                        error_code: ErrorCode::None,
                        timestamp: -1,
                        offset: 42,
                        leader_epoch: -1,
                    },
                    ListOffsetsResponsePartition {
                        partition_index: 1,
                        error_code: ErrorCode::NotLeaderForPartition,
                        timestamp: -1,
                        offset: -1,
                        leader_epoch: -1,
                    },
                    ListOffsetsResponsePartition {
                        partition_index: 2,
                        error_code: ErrorCode::None,
                        timestamp: -1,
                        offset: 99,
                        leader_epoch: -1,
                    },
                ],
            }],
        };

        let mut result: HashMap<(String, PartitionId), Result<Offset>> = HashMap::new();
        apply_list_offsets_response(&response, &mut result);

        // All three partitions are present in the map.
        assert_eq!(result.len(), 3);
        // Successful partitions carry Ok(offset).
        assert_eq!(*result[&("topic1".to_string(), 0)].as_ref().unwrap(), 42);
        assert_eq!(*result[&("topic1".to_string(), 2)].as_ref().unwrap(), 99);
        // Failed partition is present with Err, not absent.
        assert!(result[&("topic1".to_string(), 1)].is_err());
        let err_msg = result[&("topic1".to_string(), 1)]
            .as_ref()
            .unwrap_err()
            .to_string();
        assert!(
            err_msg.contains("ListOffsets error"),
            "unexpected: {err_msg}"
        );
    }

    /// Test that an all-failed response returns every partition as Err —
    /// the function itself does not return a top-level error.
    #[test]
    fn test_list_offsets_all_failed_returns_error() {
        use crate::error::ErrorCode;
        use crate::protocol::ListOffsetsResponsePartition;
        use crate::protocol::ListOffsetsResponseTopic;

        let response = ListOffsetsResponse {
            topics: vec![ListOffsetsResponseTopic {
                name: "topic1".to_string(),
                partitions: vec![ListOffsetsResponsePartition {
                    partition_index: 0,
                    error_code: ErrorCode::NotLeaderForPartition,
                    timestamp: -1,
                    offset: -1,
                    leader_epoch: -1,
                }],
            }],
        };

        let mut result: HashMap<(String, PartitionId), Result<Offset>> = HashMap::new();
        apply_list_offsets_response(&response, &mut result);

        // The partition is present in the map, mapped to Err.
        assert_eq!(result.len(), 1);
        assert!(result[&("topic1".to_string(), 0)].is_err());
        let err_msg = result[&("topic1".to_string(), 0)]
            .as_ref()
            .unwrap_err()
            .to_string();
        assert!(
            err_msg.contains("ListOffsets error"),
            "unexpected: {err_msg}"
        );
    }

    /// Test ListOffsets request encoding for v1 and v2 produces expected sizes.
    #[test]
    fn test_list_offsets_request_encode_v1_v2() {
        use bytes::BytesMut;

        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 1,
            topics: vec![ListOffsetsRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![
                    ListOffsetsRequestPartition {
                        partition_index: 0,
                        current_leader_epoch: -1,
                        timestamp: -1, // latest
                    },
                    ListOffsetsRequestPartition {
                        partition_index: 1,
                        current_leader_epoch: -1,
                        timestamp: -2, // earliest
                    },
                ],
            }],
            timeout_ms: None,
        };

        // v1 encode
        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        let encoded_v1_len = buf.len();
        assert!(encoded_v1_len > 0);

        // v2 encode produces additional isolation_level byte
        let mut buf_v2 = BytesMut::new();
        request.encode_v2(&mut buf_v2).unwrap();
        // v2 has one extra byte for isolation_level
        assert_eq!(buf_v2.len(), encoded_v1_len + 1);
    }

    // ── Cooperative rebalance algorithm tests ───────────────────────────

    /// Compute newly-assigned diff (new - old) as used in
    /// finalize_cooperative_assignment.
    fn cooperative_newly_assigned(
        new: &HashMap<String, Vec<PartitionId>>,
        old: &HashMap<String, Vec<PartitionId>>,
    ) -> Vec<TopicPartition> {
        let old_sets: HashMap<&String, HashSet<PartitionId>> = old
            .iter()
            .map(|(t, ps)| (t, ps.iter().copied().collect()))
            .collect();
        let mut result = Vec::new();
        for (topic, partitions) in new {
            let old_set = old_sets.get(topic);
            for &p in partitions {
                let is_new = old_set.is_none_or(|os| !os.contains(&p));
                if is_new {
                    result.push(TopicPartition::new(topic, p));
                }
            }
        }
        result
    }

    /// Compute cooperative revocations (old - new) as used in the
    /// no-revocations poll path.
    fn cooperative_revocations(
        old: &HashMap<String, Vec<PartitionId>>,
        new: &HashMap<String, Vec<PartitionId>>,
    ) -> Vec<TopicPartition> {
        let new_sets: HashMap<&String, HashSet<PartitionId>> = new
            .iter()
            .map(|(t, ps)| (t, ps.iter().copied().collect()))
            .collect();
        let mut result = Vec::new();
        for (topic, partitions) in old {
            let new_set = new_sets.get(topic);
            for &p in partitions {
                let gone = new_set.is_none_or(|ns| !ns.contains(&p));
                if gone {
                    result.push(TopicPartition::new(topic, p));
                }
            }
        }
        result
    }

    /// Simulate the apply_partition_revocations HashMap algorithm.
    fn apply_revocations_to_assignments(
        assignments: &mut HashMap<String, Vec<PartitionId>>,
        revoked: &[(String, PartitionId)],
    ) {
        let mut revoked_by_topic: HashMap<&str, HashSet<PartitionId>> = HashMap::new();
        for (topic, partition) in revoked {
            revoked_by_topic
                .entry(topic.as_str())
                .or_default()
                .insert(*partition);
        }
        for (topic, revoked_parts) in &revoked_by_topic {
            if let Some(parts) = assignments.get_mut(*topic) {
                parts.retain(|p| !revoked_parts.contains(p));
                if parts.is_empty() {
                    assignments.remove(*topic);
                }
            }
        }
    }

    #[test]
    fn test_cooperative_newly_assigned_fresh_join() {
        let old: HashMap<String, Vec<PartitionId>> = HashMap::new();
        let new: HashMap<String, Vec<PartitionId>> = [
            ("topic1".to_string(), vec![0, 1, 2]),
            ("topic2".to_string(), vec![0]),
        ]
        .into_iter()
        .collect();

        let result = cooperative_newly_assigned(&new, &old);
        assert_eq!(result.len(), 4);
        assert!(result.contains(&TopicPartition::new("topic1", 0)));
        assert!(result.contains(&TopicPartition::new("topic1", 1)));
        assert!(result.contains(&TopicPartition::new("topic1", 2)));
        assert!(result.contains(&TopicPartition::new("topic2", 0)));
    }

    #[test]
    fn test_cooperative_newly_assigned_partial_overlap() {
        let old: HashMap<String, Vec<PartitionId>> = [
            ("topic1".to_string(), vec![0, 1]),
            ("topic2".to_string(), vec![0]),
        ]
        .into_iter()
        .collect();
        let new: HashMap<String, Vec<PartitionId>> = [
            ("topic1".to_string(), vec![1, 2]),
            ("topic3".to_string(), vec![0]),
        ]
        .into_iter()
        .collect();

        let result = cooperative_newly_assigned(&new, &old);
        // topic1-1 retained, topic1-2 new, topic3-0 new
        assert_eq!(result.len(), 2);
        assert!(result.contains(&TopicPartition::new("topic1", 2)));
        assert!(result.contains(&TopicPartition::new("topic3", 0)));
        assert!(!result.contains(&TopicPartition::new("topic1", 1))); // retained
    }

    #[test]
    fn test_cooperative_newly_assigned_identical() {
        let assignment: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0, 1])].into_iter().collect();

        let result = cooperative_newly_assigned(&assignment, &assignment);
        assert!(result.is_empty());
    }

    #[test]
    fn test_cooperative_revocations_partial() {
        let old: HashMap<String, Vec<PartitionId>> = [
            ("topic1".to_string(), vec![0, 1, 2]),
            ("topic2".to_string(), vec![0]),
        ]
        .into_iter()
        .collect();
        let new: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![1])].into_iter().collect();

        let result = cooperative_revocations(&old, &new);
        // topic1-0, topic1-2, topic2-0 revoked; topic1-1 retained
        assert_eq!(result.len(), 3);
        assert!(result.contains(&TopicPartition::new("topic1", 0)));
        assert!(result.contains(&TopicPartition::new("topic1", 2)));
        assert!(result.contains(&TopicPartition::new("topic2", 0)));
    }

    #[test]
    fn test_cooperative_revocations_full() {
        let old: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0, 1])].into_iter().collect();
        let new: HashMap<String, Vec<PartitionId>> = HashMap::new();

        let result = cooperative_revocations(&old, &new);
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_cooperative_revocations_none() {
        let old: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0])].into_iter().collect();
        let new: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0, 1])].into_iter().collect();

        let result = cooperative_revocations(&old, &new);
        assert!(result.is_empty());
    }

    #[test]
    fn test_eager_cleanup_preserves_pause_for_retained_partitions() {
        let old: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0, 1])].into_iter().collect();
        let new: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![1, 2])].into_iter().collect();

        let revoked = revoked_partitions_diff(&old, &new);
        let revoked_tuples: Vec<(String, PartitionId)> = revoked
            .into_iter()
            .map(|tp| (tp.topic, tp.partition))
            .collect();

        let mut paused: HashSet<(String, PartitionId)> =
            [("topic1".to_string(), 0), ("topic1".to_string(), 1)]
                .into_iter()
                .collect();

        for key in &revoked_tuples {
            paused.remove(key);
        }

        assert!(!paused.contains(&("topic1".to_string(), 0)));
        assert!(paused.contains(&("topic1".to_string(), 1)));
    }

    #[test]
    fn test_apply_revocations_removes_partitions() {
        let mut assignments: HashMap<String, Vec<PartitionId>> = [
            ("topic1".to_string(), vec![0, 1, 2]),
            ("topic2".to_string(), vec![0, 1]),
        ]
        .into_iter()
        .collect();

        let revoked = vec![
            ("topic1".to_string(), 0),
            ("topic1".to_string(), 2),
            ("topic2".to_string(), 1),
        ];

        apply_revocations_to_assignments(&mut assignments, &revoked);

        assert_eq!(assignments["topic1"], vec![1]);
        assert_eq!(assignments["topic2"], vec![0]);
    }

    #[test]
    fn test_apply_revocations_removes_empty_topics() {
        let mut assignments: HashMap<String, Vec<PartitionId>> = [
            ("topic1".to_string(), vec![0]),
            ("topic2".to_string(), vec![0, 1]),
        ]
        .into_iter()
        .collect();

        let revoked = vec![("topic1".to_string(), 0)];
        apply_revocations_to_assignments(&mut assignments, &revoked);

        // topic1 should be removed entirely since it became empty
        assert!(!assignments.contains_key("topic1"));
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments["topic2"], vec![0, 1]);
    }

    #[test]
    fn test_apply_revocations_nonexistent_partition() {
        let mut assignments: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0, 1])].into_iter().collect();

        let revoked = vec![
            ("topic1".to_string(), 5), // doesn't exist
            ("topic3".to_string(), 0), // topic doesn't exist
        ];
        apply_revocations_to_assignments(&mut assignments, &revoked);

        // Assignments unchanged
        assert_eq!(assignments["topic1"], vec![0, 1]);
    }

    /// Full cooperative two-phase scenario: verify newly-assigned and revoked
    /// diffs are consistent across the protocol flow.
    #[test]
    fn test_cooperative_two_phase_rebalance_consistency() {
        // Phase 1: existing assignment pre-rebalance
        let phase0: HashMap<String, Vec<PartitionId>> = [
            ("topic1".to_string(), vec![0, 1, 2]),
            ("topic2".to_string(), vec![0]),
        ]
        .into_iter()
        .collect();

        // Phase 1 result: broker says revoke topic1-2 and topic2-0
        let phase1_target: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0, 1])].into_iter().collect();

        let to_revoke = cooperative_revocations(&phase0, &phase1_target);
        assert_eq!(to_revoke.len(), 2);
        assert!(to_revoke.contains(&TopicPartition::new("topic1", 2)));
        assert!(to_revoke.contains(&TopicPartition::new("topic2", 0)));

        // Apply revocations
        let mut current = phase0.clone();
        let revoked_tuples: Vec<(String, PartitionId)> = to_revoke
            .iter()
            .map(|tp| (tp.topic.clone(), tp.partition))
            .collect();
        apply_revocations_to_assignments(&mut current, &revoked_tuples);
        assert_eq!(current["topic1"], vec![0, 1]);
        assert!(!current.contains_key("topic2"));

        // Phase 2: rejoin gives final assignment with a new partition
        let phase2_final: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0, 1, 3])]
                .into_iter()
                .collect();

        let newly_assigned = cooperative_newly_assigned(&phase2_final, &current);
        assert_eq!(newly_assigned.len(), 1);
        assert!(newly_assigned.contains(&TopicPartition::new("topic1", 3)));

        // No further revocations needed
        let extra_revoke = cooperative_revocations(&current, &phase2_final);
        assert!(extra_revoke.is_empty());
    }

    /// Verify that cooperative rebalance callbacks follow Java client ordering:
    /// on_partitions_revoked fires before on_partitions_assigned.
    #[tokio::test]
    async fn test_cooperative_callback_ordering() {
        use std::sync::atomic::AtomicU64;
        use std::sync::atomic::Ordering;

        struct OrderTracker {
            revoke_seq: AtomicU64,
            assign_seq: AtomicU64,
            counter: AtomicU64,
        }
        impl ConsumerRebalanceListener for OrderTracker {
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) {
                self.assign_seq.store(
                    self.counter.fetch_add(1, Ordering::SeqCst),
                    Ordering::SeqCst,
                );
            }
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) {
                self.revoke_seq.store(
                    self.counter.fetch_add(1, Ordering::SeqCst),
                    Ordering::SeqCst,
                );
            }
        }

        let tracker = Arc::new(OrderTracker {
            revoke_seq: AtomicU64::new(u64::MAX),
            assign_seq: AtomicU64::new(u64::MAX),
            counter: AtomicU64::new(0),
        });

        // Simulate cooperative rebalance callback sequence:
        // 1. Revoke phase
        let revoked = vec![TopicPartition::new("topic1", 2)];
        ConsumerRebalanceListener::on_partitions_revoked(&*tracker, &revoked).await;
        // 2. Assign phase
        let assigned = vec![
            TopicPartition::new("topic1", 0),
            TopicPartition::new("topic1", 1),
            TopicPartition::new("topic1", 3),
        ];
        ConsumerRebalanceListener::on_partitions_assigned(&*tracker, &assigned).await;

        let revoke_order = tracker.revoke_seq.load(Ordering::SeqCst);
        let assign_order = tracker.assign_seq.load(Ordering::SeqCst);
        assert!(
            revoke_order < assign_order,
            "on_partitions_revoked (seq={revoke_order}) must fire before on_partitions_assigned (seq={assign_order})"
        );
    }

    /// Verify that on_partitions_assigned is called even with empty assignment
    /// (more consumers than partitions).
    #[tokio::test]
    async fn test_cooperative_on_assigned_fires_on_empty() {
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering;

        struct EmptyTracker {
            assigned_called: AtomicBool,
        }
        impl ConsumerRebalanceListener for EmptyTracker {
            async fn on_partitions_assigned(&self, parts: &[TopicPartition]) {
                assert!(parts.is_empty());
                self.assigned_called.store(true, Ordering::SeqCst);
            }
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) {}
        }

        let tracker = EmptyTracker {
            assigned_called: AtomicBool::new(false),
        };
        ConsumerRebalanceListener::on_partitions_assigned(&tracker, &[]).await;
        assert!(tracker.assigned_called.load(Ordering::SeqCst));
    }

    /// Build a `PartitionState` map entry with only the high watermark set.
    /// Used by the lag-computation tests below.
    fn ps_with_hw(watermark: Offset) -> PartitionState {
        PartitionState {
            high_watermark: Some(watermark),
            ..Default::default()
        }
    }

    /// Test the lag computation logic via the extracted `compute_aggregate_lag`
    /// helper — the same function used by `recompute_lag_metrics()` in
    /// production.
    #[test]
    fn test_lag_computation_logic() {
        let mut offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        let mut partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();

        // No data → lag is 0
        let (total_lag, max_lag) = compute_aggregate_lag(
            &offsets,
            &partition_state,
            &HashMap::new(),
            IsolationLevel::ReadUncommitted,
        );
        assert_eq!(total_lag, 0);
        assert_eq!(max_lag, 0);

        // Populate two partitions
        offsets.insert(("t".into(), 0), 50);
        offsets.insert(("t".into(), 1), 100);
        partition_state.insert(("t".into(), 0), ps_with_hw(80));
        partition_state.insert(("t".into(), 1), ps_with_hw(120));

        let (total_lag, max_lag) = compute_aggregate_lag(
            &offsets,
            &partition_state,
            &HashMap::new(),
            IsolationLevel::ReadUncommitted,
        );

        assert_eq!(total_lag, 50); // (80-50) + (120-100)
        assert_eq!(max_lag, 30); // max(30, 20)
    }

    #[test]
    fn test_lag_negative_clamped_to_zero() {
        // Position ahead of high watermark (can happen briefly after a reset)
        let mut offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        let mut partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();

        offsets.insert(("t".into(), 0), 100);
        partition_state.insert(("t".into(), 0), ps_with_hw(80));

        let (total_lag, _) = compute_aggregate_lag(
            &offsets,
            &partition_state,
            &HashMap::new(),
            IsolationLevel::ReadUncommitted,
        );
        assert_eq!(total_lag, 0);
    }

    #[test]
    fn test_lag_partial_watermarks() {
        // High watermark known for only one of two partitions
        let mut offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        let mut partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();

        offsets.insert(("t".into(), 0), 50);
        offsets.insert(("t".into(), 1), 100);
        partition_state.insert(("t".into(), 0), ps_with_hw(80));
        // Partition 1 has no high watermark

        let (total_lag, _) = compute_aggregate_lag(
            &offsets,
            &partition_state,
            &HashMap::new(),
            IsolationLevel::ReadUncommitted,
        );
        assert_eq!(total_lag, 30); // Only partition 0 contributes
    }

    #[test]
    fn test_lag_after_revocation() {
        // Simulate clearing revoked partitions and recomputing lag metrics
        let mut offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        let mut partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();

        offsets.insert(("t".into(), 0), 50);
        offsets.insert(("t".into(), 1), 100);
        partition_state.insert(("t".into(), 0), ps_with_hw(100)); // lag = 50
        partition_state.insert(("t".into(), 1), ps_with_hw(200)); // lag = 100

        // Revoke partition 0
        let revoked = vec![TopicPartition::new("t", 0)];
        for tp in &revoked {
            let key = (tp.topic.clone(), tp.partition);
            offsets.remove(&key);
            partition_state.remove(&key);
        }

        assert!(!partition_state.contains_key(&("t".into(), 0)));
        assert!(partition_state.contains_key(&("t".into(), 1)));

        // Recompute lag from remaining caches (same logic as apply_partition_revocations)
        let (total_lag, max_lag) = compute_aggregate_lag(
            &offsets,
            &partition_state,
            &HashMap::new(),
            IsolationLevel::ReadUncommitted,
        );

        // Only partition 1 remains: lag = 200 - 100 = 100
        assert_eq!(total_lag, 100);
        assert_eq!(max_lag, 100);
    }

    #[test]
    fn test_lag_clear_resets_to_zero() {
        // After clear_partition_state, all caches are empty → lag must be 0
        let mut offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        let mut partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();

        offsets.insert(("t".into(), 0), 50);
        partition_state.insert(("t".into(), 0), ps_with_hw(100));

        // Simulate clear_partition_state
        offsets.clear();
        partition_state.clear();

        let (total_lag, _) = compute_aggregate_lag(
            &offsets,
            &partition_state,
            &HashMap::new(),
            IsolationLevel::ReadUncommitted,
        );
        assert_eq!(total_lag, 0);
    }

    /// Revoking a partition must clear **every** per-partition cache
    /// (high watermark, log start offset, preferred replica, offset-retry
    /// backoff) atomically. Before the `PartitionState` consolidation this
    /// was "four separate `HashMap::remove` calls under four separate locks",
    /// which was the exact bug class the refactor eliminated. This test pins
    /// the invariant: a single `HashMap::remove` of a `PartitionState` value
    /// drops all four caches together, so no future field added to
    /// `PartitionState` can be accidentally skipped by the revocation path.
    #[test]
    fn test_partition_state_revocation_is_atomic() {
        let key = ("t".to_string(), 0_i32);
        let mut partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();
        partition_state.insert(
            key.clone(),
            PartitionState {
                last_stable_offset: None,
                high_watermark: Some(100),
                log_start_offset: Some(0),
                preferred_replica: Some((3_i32, Instant::now() + Duration::from_secs(60))),
                offset_retry_backoff: Some((Instant::now(), Duration::from_millis(100))),
                watermark_updated_at: None,
                last_fetched_epoch: Some(7),
                position_validated: true,
            },
        );

        // Sanity: the entry has all four facets populated.
        let state = &partition_state[&key];
        assert!(state.high_watermark.is_some());
        assert!(state.log_start_offset.is_some());
        assert!(state.preferred_replica.is_some());
        assert!(state.offset_retry_backoff.is_some());

        // Revoke — a single remove wipes all four facets at once.
        partition_state.remove(&key);

        assert!(!partition_state.contains_key(&key));
    }

    // --- Fetch routing plan tests (KIP-392) ---

    /// Build a `PartitionState` map entry with only the preferred replica set.
    /// Used by the routing-plan tests below.
    fn ps_with_preferred(replica_id: crate::BrokerId, expiry: Instant) -> PartitionState {
        PartitionState {
            preferred_replica: Some((replica_id, expiry)),
            ..Default::default()
        }
    }

    #[test]
    fn test_routing_plan_uses_leader_when_no_preferred() {
        let keys = vec![("t".into(), 0), ("t".into(), 1)];

        let leaders = HashMap::from([(("t".into(), 0), 1), (("t".into(), 1), 2)]);

        let plan = build_fetch_routing_plan(keys, &HashMap::new(), &leaders, Instant::now());

        assert!(plan.expired_preferred.is_empty());
        assert_eq!(plan.partitions_by_broker[&1], vec![("t".into(), 0)]);
        assert_eq!(plan.partitions_by_broker[&2], vec![("t".into(), 1)]);
    }

    #[test]
    fn test_routing_plan_routes_to_preferred_replica() {
        let keys = vec![("t".into(), 0)];

        let leaders = HashMap::from([(("t".into(), 0), 1)]);
        let partition_state = HashMap::from([(
            ("t".into(), 0),
            ps_with_preferred(3_i32, Instant::now() + Duration::from_secs(60)),
        )]);

        let plan = build_fetch_routing_plan(keys, &partition_state, &leaders, Instant::now());

        assert!(plan.expired_preferred.is_empty());
        // Should route to preferred replica (broker 3), not leader (broker 1)
        assert_eq!(plan.partitions_by_broker.len(), 1);
        assert_eq!(plan.partitions_by_broker[&3], vec![("t".into(), 0)]);
    }

    #[test]
    fn test_routing_plan_falls_back_on_expired_preferred() {
        let keys = vec![("t".into(), 0)];

        let leaders = HashMap::from([(("t".into(), 0), 1)]);
        // Preferred replica that expired 10 seconds ago
        let partition_state = HashMap::from([(
            ("t".into(), 0),
            ps_with_preferred(3_i32, Instant::now() - Duration::from_secs(10)),
        )]);

        let plan = build_fetch_routing_plan(keys, &partition_state, &leaders, Instant::now());

        // Should fall back to leader (broker 1)
        assert_eq!(plan.partitions_by_broker[&1], vec![("t".into(), 0)]);
        // Should report the expired entry for cleanup
        assert_eq!(plan.expired_preferred, vec![("t".into(), 0)]);
    }

    #[test]
    fn test_routing_plan_skips_partitions_without_leader() {
        // Only partition 0 has a leader; partition 1 has neither leader nor
        // preferred replica and should be skipped.
        let keys = vec![("t".into(), 0), ("t".into(), 1)];

        // Only partition 0 has a leader
        let leaders = HashMap::from([(("t".into(), 0), 1)]);

        let plan = build_fetch_routing_plan(keys, &HashMap::new(), &leaders, Instant::now());

        let all: Vec<_> = plan.partitions_by_broker.values().flatten().collect();
        assert_eq!(all.len(), 1);
        assert_eq!(*all[0], ("t".into(), 0));
        assert_eq!(plan.skipped, vec![("t".into(), 1)]);
    }

    #[test]
    fn test_routing_plan_all_partitions_skipped() {
        // No leaders and no preferred replicas → every partition is skipped,
        // plan is empty.
        let keys = vec![("t".into(), 0), ("t".into(), 1)];

        let plan = build_fetch_routing_plan(keys, &HashMap::new(), &HashMap::new(), Instant::now());

        assert!(plan.partitions_by_broker.is_empty());
        assert!(plan.expired_preferred.is_empty());
        assert_eq!(plan.skipped.len(), 2);
    }

    #[test]
    fn test_routing_plan_mixed_preferred_and_leader() {
        let keys = vec![("t".into(), 0), ("t".into(), 1), ("t".into(), 2)];

        let leaders = HashMap::from([
            (("t".into(), 0), 1),
            (("t".into(), 1), 1),
            (("t".into(), 2), 2),
        ]);
        let future = Instant::now() + Duration::from_secs(300);
        let partition_state = HashMap::from([
            // p0 has a valid preferred replica
            (("t".into(), 0), ps_with_preferred(3_i32, future)),
            // p1 has an expired preferred replica
            (
                ("t".into(), 1),
                ps_with_preferred(3_i32, Instant::now() - Duration::from_secs(1)),
            ),
            // p2 has no preferred replica
        ]);

        let plan = build_fetch_routing_plan(keys, &partition_state, &leaders, Instant::now());

        // p0 → broker 3 (preferred), p1 → broker 1 (leader, expired), p2 → broker 2 (leader)
        assert!(plan.partitions_by_broker[&3].contains(&("t".into(), 0)));
        assert!(plan.partitions_by_broker[&1].contains(&("t".into(), 1)));
        assert!(plan.partitions_by_broker[&2].contains(&("t".into(), 2)));
        assert_eq!(plan.expired_preferred, vec![("t".into(), 1)]);
    }

    #[test]
    fn test_consumer_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Consumer>();
    }

    #[test]
    fn test_consumer_stream_is_send() {
        fn assert_send<T: Send>() {}
        // ConsumerStream must be Send so it can be used across .await in
        // spawned tasks (e.g., tokio::spawn with an Arc<Consumer>).
        assert_send::<ConsumerStream<'_>>();
    }

    /// The flat `&[(&str, PartitionId)]` input to `offsets_for_times` is
    /// grouped by topic via `group_topic_partitions`. Verify that the grouping
    /// preserves all pairs, deduplicates topic keys, and keeps partitions in
    /// insertion order.
    #[test]
    fn test_offsets_for_times_grouping() {
        let partitions: &[(&str, PartitionId)] =
            &[("topic1", 0), ("topic1", 2), ("topic2", 1), ("topic1", 5)];

        let grouped = group_topic_partitions(partitions);

        assert_eq!(grouped.len(), 2);
        assert_eq!(grouped["topic1"], vec![0, 2, 5]);
        assert_eq!(grouped["topic2"], vec![1]);
    }

    // ── batch_recv logic tests ──────────────────────────────────────────

    fn make_record(topic: &str, partition: PartitionId, offset: Offset) -> ConsumerRecord {
        ConsumerRecord {
            topic: topic.to_string(),
            partition,
            offset,
            timestamp: 0,
            timestamp_type: 0,
            key: None,
            value: None,
            headers: vec![],
            leader_epoch: None,
            delivery_count: None,
        }
    }

    fn make_test_consumer() -> Consumer {
        let config = ConsumerConfig::default();
        let pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
        let metadata = Arc::new(
            ClusterMetadata::new(
                vec!["127.0.0.1:9092".to_string()],
                pool.clone(),
                config.metadata_max_age,
            )
            .with_topic_cache_ttl_disabled(),
        );

        Consumer {
            config,
            metadata,
            pool,
            pool_owned: true,
            subscriptions: LeveledRwLock::new(HashSet::new()),
            assignments: LeveledRwLock::new(HashMap::new()),
            offsets: LeveledRwLock::new(HashMap::new()),
            paused: LeveledRwLock::new(HashSet::new()),
            closed: std::sync::atomic::AtomicBool::new(false),
            wakeup_flag: std::sync::atomic::AtomicBool::new(false),
            wakeup_notify: tokio::sync::Notify::new(),
            group_coordinator: None,
            metrics: Arc::new(ConsumerMetrics::default()),
            rebalance_listener: Arc::new(NoOpRebalanceListener),
            interceptor: Arc::new(crate::interceptor::NoOpConsumerInterceptor),
            last_auto_commit: SyncMutex::new(Instant::now()),
            recv_buffer: SyncMutex::new(std::collections::VecDeque::new()),
            fetch_rotation: std::sync::atomic::AtomicUsize::new(0),
            fetch_sessions: SyncMutex::new(FetchSessionCache::new()),
            partition_state: LeveledRwLock::new(HashMap::new()),
            key_deserializer: None,
            value_deserializer: None,
        }
    }

    #[tokio::test]
    async fn test_batch_recv_public_api_returns_closed_when_consumer_closed() {
        let consumer = make_test_consumer();
        consumer
            .closed
            .store(true, std::sync::atomic::Ordering::SeqCst);

        let outcome = consumer
            .batch_recv(5, Duration::from_millis(10))
            .await
            .unwrap();

        assert!(matches!(outcome, BatchRecvOutcome::Closed));
    }

    #[tokio::test]
    async fn test_close_clears_local_state_and_is_idempotent() {
        let consumer = make_test_consumer();

        consumer.subscriptions.write().await.insert("orders".into());
        consumer
            .assignments
            .write()
            .await
            .insert("orders".into(), vec![0]);
        consumer
            .offsets
            .write()
            .await
            .insert(("orders".into(), 0), 42);
        consumer.paused.write().await.insert(("orders".into(), 0));
        consumer.partition_state.write().await.insert(
            ("orders".into(), 0),
            PartitionState {
                high_watermark: Some(100),
                ..PartitionState::default()
            },
        );
        consumer
            .recv_buffer
            .lock()
            .push_back(make_record("orders", 0, 42));
        consumer.metrics.buffered_records.set(1);
        consumer.metrics.paused_partitions.set(1);
        consumer.metrics.lag.set(5);
        consumer.metrics.lag_max.set(5);

        consumer.close().await.expect("close succeeds");

        assert!(consumer.is_closed());
        assert!(consumer.subscriptions.read().await.is_empty());
        assert!(consumer.assignments.read().await.is_empty());
        assert!(consumer.offsets.read().await.is_empty());
        assert!(consumer.paused.read().await.is_empty());
        assert!(consumer.partition_state.read().await.is_empty());
        assert!(consumer.recv_buffer.lock().is_empty());
        assert_eq!(consumer.metrics.buffered_records.get(), 0);
        assert_eq!(consumer.metrics.paused_partitions.get(), 0);
        assert_eq!(consumer.metrics.lag.get(), 0);
        assert_eq!(consumer.metrics.lag_max.get(), 0);

        consumer
            .close()
            .await
            .expect("second close remains a no-op");
    }

    #[tokio::test]
    async fn test_unsubscribe_clears_local_state_and_is_idempotent() {
        let consumer = make_test_consumer();

        consumer.subscriptions.write().await.insert("orders".into());
        consumer
            .assignments
            .write()
            .await
            .insert("orders".into(), vec![0]);
        consumer
            .offsets
            .write()
            .await
            .insert(("orders".into(), 0), 42);
        consumer.paused.write().await.insert(("orders".into(), 0));
        consumer.partition_state.write().await.insert(
            ("orders".into(), 0),
            PartitionState {
                high_watermark: Some(100),
                ..PartitionState::default()
            },
        );
        consumer
            .recv_buffer
            .lock()
            .push_back(make_record("orders", 0, 42));
        consumer.metrics.buffered_records.set(1);
        consumer.metrics.paused_partitions.set(1);
        consumer.metrics.lag.set(5);
        consumer.metrics.lag_max.set(5);
        consumer.metrics.assigned_partitions.set(1);

        consumer.unsubscribe().await.expect("unsubscribe succeeds");

        assert!(!consumer.is_closed());
        assert!(consumer.subscriptions.read().await.is_empty());
        assert!(consumer.assignments.read().await.is_empty());
        assert!(consumer.offsets.read().await.is_empty());
        assert!(consumer.paused.read().await.is_empty());
        assert!(consumer.partition_state.read().await.is_empty());
        assert!(consumer.recv_buffer.lock().is_empty());
        assert_eq!(consumer.metrics.buffered_records.get(), 0);
        assert_eq!(consumer.metrics.paused_partitions.get(), 0);
        assert_eq!(consumer.metrics.lag.get(), 0);
        assert_eq!(consumer.metrics.lag_max.get(), 0);
        assert_eq!(consumer.metrics.assigned_partitions.get(), 0);

        consumer
            .unsubscribe()
            .await
            .expect("second unsubscribe remains a no-op");
    }

    #[tokio::test]
    async fn test_batch_recv_public_api_uses_buffer_and_updates_metric() {
        let consumer = make_test_consumer();
        {
            let mut buffer = consumer.recv_buffer.lock();
            buffer.push_back(make_record("orders", 0, 1));
            buffer.push_back(make_record("orders", 0, 2));
        }

        let outcome = consumer
            .batch_recv(1, Duration::from_millis(10))
            .await
            .unwrap();
        let BatchRecvOutcome::Records(records) = outcome else {
            panic!("expected records outcome");
        };
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].offset, 1);

        let metrics = consumer.metrics().snapshot();
        assert_eq!(metrics.buffered_records, 1);
    }

    /// Records `recv()` did not hand out go back **in front of** the poll's
    /// parked surplus, not behind it.
    ///
    /// `poll()` parks what it could not deliver at the back of the buffer, so
    /// the buffer already holds *higher* offsets by the time `recv()` returns.
    /// Appending the undelivered remainder would order offsets 2..N after
    /// N+1.., and the application would see a partition's records out of order
    /// — the one thing a Kafka consumer must never do.
    #[test]
    fn requeued_records_are_ordered_ahead_of_the_parked_surplus() {
        let consumer = make_test_consumer();

        // What a poll parked because it exceeded the delivery cap.
        {
            let mut buffer = consumer.recv_buffer.lock();
            buffer.push_back(make_record("orders", 0, 3));
            buffer.push_back(make_record("orders", 0, 4));
        }

        // What the same poll returned but the caller did not take.
        consumer.requeue_undelivered(vec![
            make_record("orders", 0, 1),
            make_record("orders", 0, 2),
        ]);

        let buffer = consumer.recv_buffer.lock();
        let offsets: Vec<Offset> = buffer.iter().map(|r| r.offset).collect();
        assert_eq!(
            offsets,
            vec![1, 2, 3, 4],
            "requeued records must precede the parked surplus"
        );
    }

    /// A deserializer failure must not consume the records it rejected.
    ///
    /// The fetch position is advanced before `finish_delivery` runs, so a
    /// batch dropped here is skipped permanently — silent data loss that no
    /// commit, lag metric or log line would reveal. The batch is put back
    /// instead, and the error names the record so the caller can seek past it.
    #[tokio::test]
    async fn a_deserializer_failure_puts_the_batch_back_and_names_the_record() {
        struct AlwaysFails;

        impl crate::serdes::Deserializer for AlwaysFails {
            fn deserialize(
                &self,
                _payload: bytes::Bytes,
                _topic: &str,
                _is_key: bool,
            ) -> std::pin::Pin<
                Box<dyn std::future::Future<Output = Result<bytes::Bytes>> + Send + '_>,
            > {
                Box::pin(async { Err(KrafkaError::serialization("bad magic byte")) })
            }
        }

        let with_value = |offset: Offset| {
            let mut record = make_record("orders", 2, offset);
            record.value = Some(bytes::Bytes::from_static(b"payload"));
            record
        };

        let mut consumer = make_test_consumer();
        consumer.value_deserializer = Some(Arc::new(AlwaysFails));

        let error = consumer
            .finish_delivery(vec![with_value(40), with_value(41)])
            .await
            .expect_err("a failing deserializer must fail the poll");

        match error {
            KrafkaError::RecordDeserialization {
                ref topic,
                partition,
                offset,
                part,
                ..
            } => {
                assert_eq!(topic, "orders");
                assert_eq!(partition, 2);
                assert_eq!(
                    offset, 40,
                    "the *first* failing record is the one to seek past"
                );
                assert_eq!(part, "value");
            }
            other => panic!("expected RecordDeserialization, got {other:?}"),
        }

        let buffer = consumer.recv_buffer.lock();
        let offsets: Vec<Offset> = buffer.iter().map(|r| r.offset).collect();
        assert_eq!(
            offsets,
            vec![40, 41],
            "no record may be lost to a deserialization failure"
        );
    }

    #[tokio::test]
    async fn test_batch_recv_with_returns_empty_request_for_zero_max_records() {
        let buffer = SyncMutex::new(std::collections::VecDeque::new());
        let outcome = batch_recv_with(
            &buffer,
            |_| {},
            0,
            Duration::from_millis(10),
            Duration::from_millis(10),
            || false,
            || async { HashSet::new() },
            |_| async { Ok(vec![]) },
        )
        .await
        .unwrap();

        assert!(matches!(outcome, BatchRecvOutcome::EmptyRequest));
    }

    #[tokio::test]
    async fn test_batch_recv_with_returns_closed_when_no_records_and_closed() {
        let buffer = SyncMutex::new(std::collections::VecDeque::new());
        let outcome = batch_recv_with(
            &buffer,
            |_| {},
            10,
            Duration::from_millis(20),
            Duration::from_millis(10),
            || true,
            || async { HashSet::new() },
            |_| async { Ok(vec![]) },
        )
        .await
        .unwrap();

        assert!(matches!(outcome, BatchRecvOutcome::Closed));
    }

    #[tokio::test]
    async fn test_batch_recv_with_rebuffers_partial_batch_on_poll_error() {
        let mut q = std::collections::VecDeque::new();
        q.push_back(make_record("t", 0, 10));
        q.push_back(make_record("t", 0, 11));
        let buffer = SyncMutex::new(q);

        let result = batch_recv_with(
            &buffer,
            |_| {},
            10,
            Duration::from_millis(20),
            Duration::from_millis(10),
            || false,
            || async { HashSet::new() },
            |_| async {
                Err(KrafkaError::network(std::io::Error::other(
                    "simulated poll failure",
                )))
            },
        )
        .await;

        assert!(result.is_err());
        let buffer = buffer.lock();
        assert_eq!(buffer.len(), 2);
        assert_eq!(buffer[0].offset, 10);
        assert_eq!(buffer[1].offset, 11);
    }

    #[tokio::test]
    async fn test_batch_recv_with_timeout_returns_timed_out_without_oversleeping() {
        let buffer = SyncMutex::new(std::collections::VecDeque::new());
        let start = tokio::time::Instant::now();

        let outcome = batch_recv_with(
            &buffer,
            |_| {},
            10,
            Duration::from_millis(15),
            Duration::from_millis(10),
            || false,
            || async { HashSet::new() },
            |_| async { Ok(vec![]) },
        )
        .await
        .unwrap();

        assert!(matches!(outcome, BatchRecvOutcome::TimedOut));
        assert!(start.elapsed() < Duration::from_millis(60));
    }

    #[tokio::test]
    async fn test_batch_recv_with_requeues_overflow_in_order() {
        let buffer = SyncMutex::new(std::collections::VecDeque::new());
        let poll_records = SyncMutex::new(Some(vec![
            make_record("t", 0, 1),
            make_record("t", 0, 2),
            make_record("t", 0, 3),
        ]));

        let outcome = batch_recv_with(
            &buffer,
            |_| {},
            2,
            Duration::from_millis(50),
            Duration::from_millis(10),
            || false,
            || async { HashSet::new() },
            |_| async { Ok(poll_records.lock().take().unwrap_or_default()) },
        )
        .await
        .unwrap();

        let BatchRecvOutcome::Records(batch) = outcome else {
            panic!("expected records outcome");
        };
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].offset, 1);
        assert_eq!(batch[1].offset, 2);

        let buffer = buffer.lock();
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer[0].offset, 3);
    }

    // ── initial_offsets precedence tests ───────────────────────────────

    #[test]
    fn test_assignment_offset_precedence_uses_initial_when_no_committed() {
        let assigned: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0])].into_iter().collect();
        let committed: HashMap<(String, PartitionId), Offset> = HashMap::new();
        let initial_offsets: HashMap<(String, PartitionId), Offset> =
            [(("topic1".to_string(), 0), 500)].into_iter().collect();
        let mut stored: HashMap<(String, PartitionId), Offset> = HashMap::new();

        let need_reset = apply_assignment_offset_precedence(
            &assigned,
            &committed,
            &initial_offsets,
            &mut stored,
        );

        assert!(need_reset.is_empty());
        assert_eq!(stored.get(&("topic1".to_string(), 0)), Some(&500));
    }

    #[test]
    fn test_seed_initial_offsets_for_assigned_filters_and_vacant_only() {
        let assigned: HashMap<String, Vec<PartitionId>> = [
            ("topic1".to_string(), vec![0, 1]),
            ("topic2".to_string(), vec![0]),
        ]
        .into_iter()
        .collect();

        let initial_offsets: HashMap<(String, PartitionId), Offset> = [
            (("topic1".to_string(), 0), 100),
            (("topic1".to_string(), 2), 200), // unassigned
            (("topic3".to_string(), 0), 300), // unassigned topic
        ]
        .into_iter()
        .collect();

        let mut stored: HashMap<(String, PartitionId), Offset> =
            [(("topic1".to_string(), 1), 999)].into_iter().collect();

        let inserted = seed_initial_offsets_for_assigned(&assigned, &initial_offsets, &mut stored);
        assert_eq!(inserted, 1);
        assert_eq!(stored.get(&("topic1".to_string(), 0)), Some(&100));
        assert_eq!(stored.get(&("topic1".to_string(), 1)), Some(&999));
        assert!(!stored.contains_key(&("topic1".to_string(), 2)));
        assert!(!stored.contains_key(&("topic3".to_string(), 0)));
    }

    #[test]
    fn test_assignment_offset_precedence_committed_wins_initial() {
        let assigned: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0])].into_iter().collect();
        let committed: HashMap<(String, PartitionId), Offset> =
            [(("topic1".to_string(), 0), 999)].into_iter().collect();
        let initial_offsets: HashMap<(String, PartitionId), Offset> =
            [(("topic1".to_string(), 0), 500)].into_iter().collect();
        let mut stored: HashMap<(String, PartitionId), Offset> = HashMap::new();

        let need_reset = apply_assignment_offset_precedence(
            &assigned,
            &committed,
            &initial_offsets,
            &mut stored,
        );

        assert!(need_reset.is_empty());
        assert_eq!(stored.get(&("topic1".to_string(), 0)), Some(&999));
    }

    #[test]
    fn test_assignment_offset_precedence_missing_offsets_require_reset() {
        let assigned: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0])].into_iter().collect();
        let committed: HashMap<(String, PartitionId), Offset> = HashMap::new();
        let initial_offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        let mut stored: HashMap<(String, PartitionId), Offset> = HashMap::new();

        let need_reset = apply_assignment_offset_precedence(
            &assigned,
            &committed,
            &initial_offsets,
            &mut stored,
        );

        assert_eq!(need_reset, vec![("topic1".to_string(), 0)]);
        assert!(stored.is_empty());
    }

    #[test]
    fn test_assignment_offset_precedence_preserves_existing_user_offset() {
        let assigned: HashMap<String, Vec<PartitionId>> =
            [("topic1".to_string(), vec![0])].into_iter().collect();
        let committed: HashMap<(String, PartitionId), Offset> =
            [(("topic1".to_string(), 0), 999)].into_iter().collect();
        let initial_offsets: HashMap<(String, PartitionId), Offset> =
            [(("topic1".to_string(), 0), 500)].into_iter().collect();
        let mut stored: HashMap<(String, PartitionId), Offset> =
            [(("topic1".to_string(), 0), 42)].into_iter().collect();

        let need_reset = apply_assignment_offset_precedence(
            &assigned,
            &committed,
            &initial_offsets,
            &mut stored,
        );

        assert!(need_reset.is_empty());
        assert_eq!(stored.get(&("topic1".to_string(), 0)), Some(&42));
    }

    #[test]
    fn test_apply_seek_many_offsets_updates_multiple_partitions() {
        let mut stored: HashMap<(String, PartitionId), Offset> =
            [(("orders".to_string(), 0), 10)].into_iter().collect();
        let updates: HashMap<(String, PartitionId), Offset> = [
            (("orders".to_string(), 0), 20),
            (("orders".to_string(), 1), 30),
        ]
        .into_iter()
        .collect();

        let updated = apply_seek_many_offsets(&mut stored, &updates);
        assert_eq!(updated, 2);
        assert_eq!(stored.get(&("orders".to_string(), 0)), Some(&20));
        assert_eq!(stored.get(&("orders".to_string(), 1)), Some(&30));
    }

    #[tokio::test]
    async fn test_seek_many_public_api_recomputes_lag_and_increments_metric() {
        let consumer = make_test_consumer();

        {
            let mut offsets = consumer.offsets.write().await;
            offsets.insert(("orders".to_string(), 0), 100);
        }
        {
            let mut state = consumer.partition_state.write().await;
            state.insert(
                ("orders".to_string(), 0),
                PartitionState {
                    high_watermark: Some(120),
                    ..PartitionState::default()
                },
            );
        }

        let updates: HashMap<(String, PartitionId), Offset> =
            [(("orders".to_string(), 0), 110)].into_iter().collect();
        consumer.seek_many(&updates).await.unwrap();

        let metrics = consumer.metrics().snapshot();
        assert_eq!(metrics.seeks, 1);
        assert_eq!(metrics.lag, 10);
        assert_eq!(metrics.lag_max, 10);
    }

    // ── Committable position (buffered-but-undelivered records) ──────────

    /// A partition with nothing buffered can commit its full fetch position.
    #[test]
    fn test_committable_position_matches_fetch_position_when_buffer_empty() {
        let mut positions = HashMap::new();
        positions.insert(("t".to_string(), 0), 500);

        let buffer = std::collections::VecDeque::new();
        let committable = committable_positions(&positions, &buffer);

        assert_eq!(committable.get(&("t".to_string(), 0)), Some(&500));
    }

    /// The core at-least-once property: records fetched into the receive
    /// buffer but not yet handed to the application must not be committed.
    /// Committing the fetch position here would acknowledge 497 records the
    /// application never saw.
    #[test]
    fn test_committable_position_clamped_to_lowest_buffered_offset() {
        let mut positions = HashMap::new();
        positions.insert(("t".to_string(), 0), 500);

        // poll() fetched 0..500 and recv() delivered 0, 1, 2; 3..500 remain.
        let buffer: std::collections::VecDeque<ConsumerRecord> =
            (3..500).map(|o| make_record("t", 0, o)).collect();

        let committable = committable_positions(&positions, &buffer);

        assert_eq!(
            committable.get(&("t".to_string(), 0)),
            Some(&3),
            "must commit only up to the first undelivered record"
        );
    }

    /// Buffered records for one partition must not hold back another.
    #[test]
    fn test_committable_position_is_per_partition() {
        let mut positions = HashMap::new();
        positions.insert(("t".to_string(), 0), 100);
        positions.insert(("t".to_string(), 1), 200);

        let buffer: std::collections::VecDeque<ConsumerRecord> =
            vec![make_record("t", 0, 40), make_record("t", 0, 41)].into();

        let committable = committable_positions(&positions, &buffer);

        assert_eq!(committable.get(&("t".to_string(), 0)), Some(&40));
        assert_eq!(
            committable.get(&("t".to_string(), 1)),
            Some(&200),
            "a partition with no buffered records is unaffected"
        );
    }

    /// Buffer order is not guaranteed to be ascending across interleaved
    /// partitions, so the minimum must be computed, not taken from the front.
    #[test]
    fn test_committable_position_uses_minimum_not_first_buffered() {
        let mut positions = HashMap::new();
        positions.insert(("t".to_string(), 0), 100);

        let buffer: std::collections::VecDeque<ConsumerRecord> = vec![
            make_record("t", 0, 70),
            make_record("t", 0, 55),
            make_record("t", 0, 90),
        ]
        .into();

        let committable = committable_positions(&positions, &buffer);
        assert_eq!(committable.get(&("t".to_string(), 0)), Some(&55));
    }

    /// The committable position never exceeds the fetch position, even if the
    /// buffer somehow holds a higher offset.
    #[test]
    fn test_committable_position_never_exceeds_fetch_position() {
        let mut positions = HashMap::new();
        positions.insert(("t".to_string(), 0), 10);

        let buffer: std::collections::VecDeque<ConsumerRecord> =
            vec![make_record("t", 0, 999)].into();

        let committable = committable_positions(&positions, &buffer);
        assert_eq!(committable.get(&("t".to_string(), 0)), Some(&10));
    }

    // ── Stale fetch responses after seek() ───────────────────────────────

    /// The ordinary case: the position is untouched while the fetch is in
    /// flight, so the update applies.
    #[test]
    fn test_fetch_update_applied_when_position_unchanged() {
        let mut offsets = HashMap::new();
        offsets.insert(("t".to_string(), 0), 1000);

        let discarded = apply_fetch_offset_updates(
            &mut offsets,
            vec![(
                ("t".to_string(), 0),
                FetchOffsetUpdate {
                    epoch: -1,
                    requested: 1000,
                    next: 1500,
                },
            )],
        );

        assert!(discarded.is_empty());
        assert_eq!(offsets.get(&("t".to_string(), 0)), Some(&1500));
    }

    /// A fetch issued from 1000 must not overwrite a seek to 100 that landed
    /// while it was in flight — otherwise the seek is silently discarded and
    /// the consumer resumes at 1500.
    #[test]
    fn test_fetch_update_discarded_after_concurrent_seek() {
        let mut offsets = HashMap::new();
        offsets.insert(("t".to_string(), 0), 100); // seek() already applied

        let discarded = apply_fetch_offset_updates(
            &mut offsets,
            vec![(
                ("t".to_string(), 0),
                FetchOffsetUpdate {
                    epoch: -1,
                    requested: 1000,
                    next: 1500,
                },
            )],
        );

        assert_eq!(discarded, vec![("t".to_string(), 0)]);
        assert_eq!(
            offsets.get(&("t".to_string(), 0)),
            Some(&100),
            "seek() must survive an in-flight fetch from the old position"
        );
    }

    /// A partition revoked while its fetch was in flight has no position at
    /// all; the update must not resurrect it.
    #[test]
    fn test_fetch_update_discarded_for_revoked_partition() {
        let mut offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();

        let discarded = apply_fetch_offset_updates(
            &mut offsets,
            vec![(
                ("t".to_string(), 0),
                FetchOffsetUpdate {
                    epoch: -1,
                    requested: 5,
                    next: 10,
                },
            )],
        );

        assert_eq!(discarded, vec![("t".to_string(), 0)]);
        assert!(offsets.is_empty());
    }

    /// A stale update for one partition must not block a valid one for
    /// another partition in the same batch.
    #[test]
    fn test_fetch_updates_are_evaluated_independently() {
        let mut offsets = HashMap::new();
        offsets.insert(("t".to_string(), 0), 100); // moved by seek
        offsets.insert(("t".to_string(), 1), 200); // untouched

        let discarded = apply_fetch_offset_updates(
            &mut offsets,
            vec![
                (
                    ("t".to_string(), 0),
                    FetchOffsetUpdate {
                        epoch: -1,
                        requested: 1000,
                        next: 1500,
                    },
                ),
                (
                    ("t".to_string(), 1),
                    FetchOffsetUpdate {
                        epoch: -1,
                        requested: 200,
                        next: 250,
                    },
                ),
            ],
        );

        assert_eq!(discarded, vec![("t".to_string(), 0)]);
        assert_eq!(offsets.get(&("t".to_string(), 0)), Some(&100));
        assert_eq!(offsets.get(&("t".to_string(), 1)), Some(&250));
    }

    // ====================================================================
    // KIP-320 log-truncation detection / KIP-951 leader discovery
    // ====================================================================

    /// The epoch committed with a position must be the epoch that position was
    /// actually read at — never a leftover from before a `seek()`.
    ///
    /// `seek`, `seek_many` and the offset-reset path all call
    /// `invalidate_position_epoch`, so a moved position has no epoch to
    /// report. Committing a stale epoch against a new offset would produce a
    /// `(offset, epoch)` pair that never existed in any log, and the KIP-320
    /// check the pair feeds would then be answering a question about a
    /// position the consumer never held.
    #[test]
    fn a_seeked_position_commits_no_leader_epoch() {
        let key = ("events".to_string(), 0);
        let mut partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();
        partition_state
            .entry(key.clone())
            .or_default()
            .last_fetched_epoch = Some(7);

        let offsets: HashMap<(String, PartitionId), Offset> = [(key.clone(), 42)].into();

        // Before the seek the epoch is reported.
        let epochs = Consumer::leader_epochs_from_state(&offsets, &partition_state);
        assert_eq!(epochs.get(&key), Some(&7));

        // After it, there is nothing to vouch for.
        invalidate_position_epoch(&mut partition_state, &key);
        let epochs = Consumer::leader_epochs_from_state(&offsets, &partition_state);
        assert!(
            epochs.get(&key).is_none(),
            "a position moved by seek() must commit -1, not the epoch it held before"
        );

        // And that is what reaches the wire.
        let committed = Consumer::build_commit_offsets(&offsets, &epochs, None, false).unwrap();
        assert_eq!(committed[&key].leader_epoch, -1);
        assert_eq!(committed[&key].offset, 42);
    }

    #[test]
    fn test_invalidate_position_epoch_clears_epoch_and_validation() {
        let key = ("t".to_string(), 0);
        let mut partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();
        let entry = partition_state.entry(key.clone()).or_default();
        entry.last_fetched_epoch = Some(11);
        entry.position_validated = true;
        entry.high_watermark = Some(900);

        invalidate_position_epoch(&mut partition_state, &key);

        let state = &partition_state[&key];
        assert_eq!(state.last_fetched_epoch, None);
        assert!(!state.position_validated);
        // Unrelated cached facts survive — only what describes the position
        // is discarded.
        assert_eq!(state.high_watermark, Some(900));
    }

    #[test]
    fn test_invalidate_position_epoch_creates_entry_when_absent() {
        let key = ("t".to_string(), 3);
        let mut partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();

        invalidate_position_epoch(&mut partition_state, &key);

        // Without an entry a later fetch could not tell an unvalidated
        // position from a validated one.
        assert!(!partition_state[&key].position_validated);
    }

    #[tokio::test]
    async fn test_seek_marks_position_for_revalidation() {
        let consumer = make_test_consumer();
        let key = ("orders".to_string(), 0);
        {
            let mut partition_state = consumer.partition_state.write().await;
            let entry = partition_state.entry(key.clone()).or_default();
            entry.last_fetched_epoch = Some(4);
            entry.position_validated = true;
        }

        consumer.seek("orders", 0, 500).await.unwrap();

        let partition_state = consumer.partition_state.read().await;
        assert_eq!(partition_state[&key].last_fetched_epoch, None);
        assert!(!partition_state[&key].position_validated);
    }

    #[tokio::test]
    async fn test_seek_many_marks_all_positions_for_revalidation() {
        let consumer = make_test_consumer();
        let keys = [("orders".to_string(), 0), ("orders".to_string(), 1)];
        {
            let mut partition_state = consumer.partition_state.write().await;
            for key in &keys {
                let entry = partition_state.entry(key.clone()).or_default();
                entry.last_fetched_epoch = Some(4);
                entry.position_validated = true;
            }
        }

        let mut targets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        targets.insert(keys[0].clone(), 10);
        targets.insert(keys[1].clone(), 20);
        consumer.seek_many(&targets).await.unwrap();

        let partition_state = consumer.partition_state.read().await;
        for key in &keys {
            assert_eq!(partition_state[key].last_fetched_epoch, None);
            assert!(!partition_state[key].position_validated);
        }
    }

    #[tokio::test]
    async fn test_truncation_rewinds_position_and_drops_buffered_records() {
        let consumer = make_test_consumer();
        let key = ("orders".to_string(), 0);

        consumer.offsets.write().await.insert(key.clone(), 1_000);
        {
            let mut partition_state = consumer.partition_state.write().await;
            let entry = partition_state.entry(key.clone()).or_default();
            entry.last_fetched_epoch = Some(7);
        }
        {
            let mut buffer = consumer.recv_buffer.lock();
            buffer.push_back(make_record("orders", 0, 940)); // below the divergence
            buffer.push_back(make_record("orders", 0, 950)); // at the divergence
            buffer.push_back(make_record("orders", 0, 980)); // beyond it
            buffer.push_back(make_record("orders", 1, 999)); // other partition
        }

        consumer
            .truncate_to_diverging_offset(
                "orders",
                0,
                crate::protocol::DivergingEpoch {
                    epoch: 6,
                    end_offset: 950,
                },
            )
            .await;

        assert_eq!(consumer.offsets.read().await[&key], 950);

        let partition_state = consumer.partition_state.read().await;
        assert_eq!(partition_state[&key].last_fetched_epoch, None);
        // The broker just told us exactly where the logs part, so nothing
        // more needs validating.
        assert!(partition_state[&key].position_validated);
        drop(partition_state);

        let buffered: Vec<(String, Offset)> = consumer
            .recv_buffer
            .lock()
            .iter()
            .map(|r| (format!("{}-{}", r.topic, r.partition), r.offset))
            .collect();
        assert_eq!(
            buffered,
            vec![("orders-0".to_string(), 940), ("orders-1".to_string(), 999)]
        );
    }

    #[tokio::test]
    async fn test_truncation_does_not_apply_auto_offset_reset() {
        // `auto.offset.reset` would move the position to the start or end of
        // the log. A divergence point is a valid offset, so the position must
        // land exactly on it.
        let consumer = make_test_consumer();
        let key = ("orders".to_string(), 0);
        consumer.offsets.write().await.insert(key.clone(), 5_000);

        consumer
            .truncate_to_diverging_offset(
                "orders",
                0,
                crate::protocol::DivergingEpoch {
                    epoch: 2,
                    end_offset: 4_096,
                },
            )
            .await;

        assert_eq!(consumer.offsets.read().await[&key], 4_096);
    }

    #[test]
    fn test_routing_plan_uses_the_metadata_leader() {
        // A broker-reported leader (KIP-951) reaches this function through the
        // metadata cache, so there is one leader source and no precedence
        // question between them.
        let now = Instant::now();
        let key = ("orders".to_string(), 0);
        let partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();

        let mut leaders = HashMap::new();
        leaders.insert(key.clone(), 9);

        let plan = build_fetch_routing_plan(vec![key.clone()], &partition_state, &leaders, now);

        assert_eq!(plan.partitions_by_broker[&9], vec![key]);
        assert!(plan.skipped.is_empty());
    }

    #[test]
    fn test_routing_plan_skips_partitions_without_a_leader() {
        let now = Instant::now();
        let key = ("orders".to_string(), 0);
        let partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();

        let plan =
            build_fetch_routing_plan(vec![key.clone()], &partition_state, &HashMap::new(), now);

        assert!(plan.partitions_by_broker.is_empty());
        assert_eq!(plan.skipped, vec![key]);
    }

    #[test]
    fn test_routing_plan_preferred_replica_wins_over_the_leader() {
        // KIP-392 read-replica routing is a deliberate choice by the broker
        // serving this consumer and is not invalidated by a leader change.
        let now = Instant::now();
        let key = ("orders".to_string(), 0);
        let mut partition_state: HashMap<(String, PartitionId), PartitionState> = HashMap::new();
        partition_state
            .entry(key.clone())
            .or_default()
            .preferred_replica = Some((5, now + Duration::from_secs(60)));

        let mut leaders = HashMap::new();
        leaders.insert(key.clone(), 9);

        let plan = build_fetch_routing_plan(vec![key.clone()], &partition_state, &leaders, now);

        assert_eq!(plan.partitions_by_broker[&5], vec![key]);
    }

    /// A leader hint carrying an endpoint makes a broker the metadata cache has
    /// never seen dialable straight away.
    #[tokio::test]
    async fn test_broker_address_uses_an_endpoint_from_a_leader_hint() {
        let consumer = make_test_consumer();
        assert_eq!(consumer.broker_address(42), None);

        assert!(consumer.metadata.apply_leader_hint(
            "orders",
            0,
            42,
            5,
            Some(BrokerInfo::new(
                42,
                "broker-42.internal".to_string(),
                9092,
                None
            )),
        ));

        assert_eq!(
            consumer.broker_address(42),
            Some("broker-42.internal:9092".to_string())
        );
    }

    #[test]
    fn test_fetch_offset_update_carries_epoch_only_when_applied() {
        let mut offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        offsets.insert(("t".to_string(), 0), 100);
        offsets.insert(("t".to_string(), 1), 200);

        let updates = vec![
            (
                ("t".to_string(), 0),
                FetchOffsetUpdate {
                    requested: 100,
                    next: 150,
                    epoch: 5,
                },
            ),
            (
                // Position moved while the fetch was in flight.
                ("t".to_string(), 1),
                FetchOffsetUpdate {
                    requested: 180,
                    next: 250,
                    epoch: 6,
                },
            ),
        ];

        let discarded = apply_fetch_offset_updates(&mut offsets, updates);

        assert_eq!(discarded, vec![("t".to_string(), 1)]);
        assert_eq!(offsets[&("t".to_string(), 0)], 150);
        // The stale update left the position alone, so its epoch must not be
        // recorded either.
        assert_eq!(offsets[&("t".to_string(), 1)], 200);
    }
}
