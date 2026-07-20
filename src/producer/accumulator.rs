//! Record accumulator for batching producer records.
//!
//! The accumulator collects records into batches per topic-partition,
//! flushing them when:
//! - The batch reaches its maximum size
//! - The linger time expires
//! - Manual flush is requested
//!
//! This enables efficient network utilization through batching while
//! providing low latency through the linger timer mechanism.

use ahash::AHashMap;
use bytes::BufMut as _;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::interval;
use tracing::{debug, trace, warn};

use super::barrier::{InFlightBarrier, InFlightOpGuard};

use super::record::{
    DeliveryConfirmation, ProducerRecord, RecordMetadata, RoutedRecord, TopicHandle,
};
use super::retry::{RetryContext, RetryPolicy};
use crate::PartitionId;
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::interceptor::ProducerInterceptor;
use crate::metadata::ClusterMetadata;
use crate::metrics::ProducerMetrics;
use crate::protocol::{
    ApiKey, Compression, ProducePartitionData, ProduceRequest, ProduceResponse, ProduceTopicData,
    RecordBatchBuilder, VersionedDecode, versions,
};

/// Maximum number of concurrent `send_extracted_batch` tasks across **all**
/// in-flight drain waves.
///
/// This shared cap (enforced by `RecordAccumulator::send_semaphore`) ensures
/// that overlapping linger waves do not compound — the combined task count
/// across all waves is always ≤ this constant.
///
/// `flush_all` (Flush/Shutdown) awaits `spawn_batches_bounded` directly so
/// completion is confirmed before the caller unblocks.  Linger-triggered
/// paths (`check_linger_expiry`, `flush_all_ready`) and single-batch flushes
/// (`flush_batch`) detach their work via `spawn_batches_detached`; the
/// semaphore gates spawning so the run loop is never flooded.
///
/// Fix for H3: prior implementations spawned one task per batch with no cap,
/// causing 10k-task bursts for high-partition topics at short linger windows.
/// This constant is deliberately fixed — batch sends are I/O-bound and the
/// per-broker connection already serialises, so extra parallelism beyond a
/// few dozen tasks adds scheduler pressure without throughput gain.
const MAX_CONCURRENT_BATCH_SENDS: usize = 64;

/// How many times a single batch lineage may be halved in response to
/// `MESSAGE_TOO_LARGE` / `RECORD_LIST_TOO_LARGE` before the error is reported
/// to the caller.
///
/// Depth 2 means the original batch splits once, and each resulting half may
/// split once more — bounding recursion at four leaf batches while still
/// recovering from the common case of a broker `message.max.bytes` that is a
/// small multiple below the producer's `max_request_size`.
///
/// Mirrors the Kafka Java client's `RecordAccumulator.splitAndReenqueue`,
/// which re-splits until the batch reaches a single record; we stop earlier
/// because an unbounded recursion here would be driven by broker responses.
const MAX_BATCH_SPLIT_DEPTH: u8 = 2;

/// Only start pruning the per-partition dispatch FIFO map once it exceeds this
/// many entries, so steady-state producers never pay for the scan.
const PARTITION_INFLIGHT_PRUNE_THRESHOLD: usize = 1024;

/// Per-`(topic, partition)` in-flight FIFO serializer.
///
/// # Why this exists
///
/// Batch sends are dispatched as independent Tokio tasks so that different
/// partitions proceed concurrently. Without coordination, wave *N+1* overlaps
/// wave *N* and two batches for the **same** partition race onto the wire in
/// arbitrary order. For an idempotent producer that is fatal: the broker sees
/// sequence *n+1* before *n* and answers `OUT_OF_ORDER_SEQUENCE_NUMBER`,
/// permanently reordering the stream or wedging the partition behind a gap.
///
/// This type is the equivalent of the Java client's
/// `RecordAccumulator.mutePartition` + `inflightBatchesBySequence`: at most one
/// batch per partition is on the wire at a time, and batches take their turn in
/// the exact order the accumulator sealed them.
///
/// # Mechanism
///
/// A ticket is drawn in the single-threaded accumulator run loop (so ticket
/// order is seal order). The send task then waits until `now_serving` reaches
/// its ticket. Completion — acknowledgement, permanent failure, or task panic —
/// advances `now_serving` through [`PartitionTurn`]'s `Drop`, so a lost task can
/// never strand the partition.
///
/// Cross-partition concurrency is untouched: each `(topic, partition)` has its
/// own instance.
#[derive(Debug, Default)]
struct PartitionInFlight {
    /// Next ticket to hand out. Only mutated from the accumulator run loop.
    next_ticket: AtomicU64,
    /// Ticket currently permitted to dispatch.
    now_serving: AtomicU64,
    /// Woken every time `now_serving` advances.
    advance: tokio::sync::Notify,
}

impl PartitionInFlight {
    /// Draw the next FIFO ticket for this partition.
    ///
    /// Called from the accumulator run loop at batch-seal time, so the ticket
    /// order is exactly the order in which batches were sealed.
    fn take_ticket(self: &Arc<Self>) -> PartitionTicket {
        let ticket = self.next_ticket.fetch_add(1, Ordering::AcqRel);
        PartitionTicket {
            slot: Arc::clone(self),
            ticket,
            acquired: false,
        }
    }

    /// Whether every ticket handed out for this partition has completed.
    ///
    /// Used to prune idle entries from the accumulator's partition map.
    fn is_idle(&self) -> bool {
        self.now_serving.load(Ordering::Acquire) == self.next_ticket.load(Ordering::Acquire)
    }
}

/// A reserved place in a partition's dispatch order.
///
/// Held by an extracted batch from seal time until the send task calls
/// [`acquire`](Self::acquire). Dropping a ticket without acquiring it still
/// advances the queue, so an abandoned wave cannot stall the partition.
#[derive(Debug)]
struct PartitionTicket {
    slot: Arc<PartitionInFlight>,
    ticket: u64,
    /// Set once the ticket has been converted into a [`PartitionTurn`], so
    /// `Drop` does not advance the queue twice.
    acquired: bool,
}

impl PartitionTicket {
    /// Wait until every earlier batch for this partition has completed, then
    /// take exclusive ownership of the partition's in-flight slot.
    ///
    /// The returned [`PartitionTurn`] must be held for the entire lifetime of
    /// the produce attempt (including retries and sequence recovery) so that
    /// the next batch cannot allocate a sequence or hit the wire until this one
    /// is resolved.
    async fn acquire(mut self) -> PartitionTurn {
        // Fast path: already our turn.
        while self.slot.now_serving.load(Ordering::Acquire) != self.ticket {
            // Register interest before re-checking so a concurrent
            // `notify_waiters` between the load and the await cannot be missed.
            let notified = self.slot.advance.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.slot.now_serving.load(Ordering::Acquire) == self.ticket {
                break;
            }
            notified.await;
        }
        self.acquired = true;
        PartitionTurn {
            slot: Arc::clone(&self.slot),
        }
    }
}

impl Drop for PartitionTicket {
    fn drop(&mut self) {
        // A ticket dropped without `acquire()` (wave aborted during shutdown)
        // must still release its place, otherwise every later batch for this
        // partition waits forever. Once acquired, the resulting `PartitionTurn`
        // owns the release instead.
        if !self.acquired {
            self.slot.now_serving.fetch_add(1, Ordering::AcqRel);
            self.slot.advance.notify_waiters();
        }
    }
}

/// Exclusive right to have one batch in flight for a partition.
///
/// Dropping advances the partition's FIFO, releasing the next batch. This
/// happens on every exit path — success, permanent failure, cancellation, or
/// panic — so the queue cannot deadlock.
#[derive(Debug)]
struct PartitionTurn {
    slot: Arc<PartitionInFlight>,
}

impl Drop for PartitionTurn {
    fn drop(&mut self) {
        self.slot.now_serving.fetch_add(1, Ordering::AcqRel);
        self.slot.advance.notify_waiters();
    }
}

/// A batch that has been sealed and removed from the accumulator, together
/// with the resources its send task must own.
struct ExtractedBatch {
    batch: AccumulatorBatch,
    guard: InFlightGuard,
    ticket: PartitionTicket,
}

/// Batches sealed in one drain wave, keyed by `(topic, partition)`.
type ExtractedBatches = Vec<((TopicHandle, PartitionId), ExtractedBatch)>;

/// Whether `error` indicates the batch was rejected purely because of its
/// encoded size, and therefore may succeed if split into smaller batches.
///
/// Covers both the broker-side rejections (`MESSAGE_TOO_LARGE` when the batch
/// exceeds the topic's `max.message.bytes`, `RECORD_LIST_TOO_LARGE`) and the
/// local frame-size guard in
/// [`encode_and_validate_produce_request`](super::encode_and_validate_produce_request).
fn is_batch_too_large(error: &KrafkaError) -> bool {
    match error {
        KrafkaError::Broker { code, .. } => matches!(
            code,
            ErrorCode::MessageTooLarge | ErrorCode::RecordListTooLarge
        ),
        KrafkaError::Protocol { kind, .. } => *kind == ProtocolErrorKind::InvalidLength,
        _ => false,
    }
}

/// Whether `compression` is CPU-heavy enough to be worth moving off the async
/// runtime worker thread.
///
/// Gzip and Zstd cost tens to hundreds of microseconds per batch and would
/// otherwise stall every other task sharing the worker. Snappy, LZ4 and `None`
/// are cheap enough that the `spawn_blocking` hop would dominate.
#[inline]
fn is_cpu_heavy_compression(compression: Compression) -> bool {
    matches!(compression, Compression::Gzip | Compression::Zstd)
}

/// Validate that `record_size` bytes can be admitted into the memory pool.
///
/// Returns an error immediately if the record would permanently block
/// `acquire_many` — either because it exceeds the effective semaphore limit
/// (`min(u32::MAX, Semaphore::MAX_PERMITS)`) or because it exceeds the
/// configured `buffer_memory` budget (permits can never accumulate to that
/// level).
///
/// The semaphore-limit check comes first so the error message is always
/// accurate: a record larger than both limits is a semaphore constraint, not
/// a tunable configuration problem.
fn max_record_semaphore_permits() -> usize {
    Semaphore::MAX_PERMITS.min(u32::MAX as usize)
}

pub(crate) fn check_record_admission(
    record_size: usize,
    memory_capacity: usize,
    max_request_size: usize,
) -> Result<()> {
    let semaphore_limit = max_record_semaphore_permits();

    if record_size > semaphore_limit {
        return Err(KrafkaError::config(format!(
            "record size {record_size} B exceeds the semaphore \
             permit-count limit ({} B; min(u32::MAX, \
             Semaphore::MAX_PERMITS)); Kafka records must be \
             smaller",
            semaphore_limit
        )));
    }
    if max_request_size > 0 && record_size > max_request_size {
        return Err(KrafkaError::config(format!(
            "record size {record_size} B exceeds max_request_size \
             ({max_request_size} B); the broker will reject the record \
             with MESSAGE_TOO_LARGE — raise ProducerConfig::max_request_size \
             or shrink the record",
        )));
    }
    if record_size > memory_capacity {
        return Err(KrafkaError::config(format!(
            "record size {record_size} B exceeds producer buffer_memory \
             capacity ({} B); raise ProducerConfig::buffer_memory or \
             shrink the record",
            memory_capacity
        )));
    }
    Ok(())
}

pub(crate) fn effective_memory_capacity(buffer_memory: usize) -> usize {
    if buffer_memory > 0 {
        if buffer_memory > Semaphore::MAX_PERMITS {
            warn!(
                requested = buffer_memory,
                effective = Semaphore::MAX_PERMITS,
                "buffer_memory exceeds Semaphore::MAX_PERMITS; clamping effective \
                 producer memory capacity"
            );
            Semaphore::MAX_PERMITS
        } else {
            buffer_memory
        }
    } else {
        Semaphore::MAX_PERMITS
    }
}

#[derive(Debug)]
pub(crate) struct BufferedRecordGuard {
    buffered_records: Arc<AtomicUsize>,
    metrics: Arc<ProducerMetrics>,
}

impl BufferedRecordGuard {
    pub(crate) fn new(buffered_records: Arc<AtomicUsize>, metrics: Arc<ProducerMetrics>) -> Self {
        buffered_records.fetch_add(1, Ordering::Relaxed);
        metrics.buffered_records.inc();
        Self {
            buffered_records,
            metrics,
        }
    }
}

impl Drop for BufferedRecordGuard {
    fn drop(&mut self) {
        self.buffered_records.fetch_sub(1, Ordering::Relaxed);
        self.metrics.buffered_records.dec();
    }
}

/// Response from the accumulator for an append attempt.
///
/// Backpressure (buffer-memory exhaustion) is handled entirely in the handle
/// via `memory_permits.acquire_many(record_size)` before the message is sent,
/// so the accumulator never returns a "buffer full" signal — by the time a
/// message arrives, its bytes are already reserved.
#[derive(Debug)]
enum AppendResponse {
    /// Record accepted — metadata will arrive via the inner Result.
    Done(Result<RecordMetadata>),
}

#[derive(Debug)]
struct AppendCommand {
    topic: TopicHandle,
    record: RoutedRecord,
    partition: PartitionId,
    record_size: usize,
    response_tx: oneshot::Sender<AppendResponse>,
    operation_guard: InFlightOpGuard,
    /// Tracks this append in the buffered-records gauge from successful
    /// admission into the channel until it is either moved into a pending
    /// batch or dropped on failure.
    _buffered_record_guard: BufferedRecordGuard,
    permit_reservation: PermitReservation,
}

/// Message sent to the accumulator background task.
#[derive(Debug)]
enum AccumulatorMessage {
    /// Add a record to the accumulator.
    ///
    /// `record_size` duplicates `permit_reservation.bytes` for easy access
    /// on the hot path; the RAII `PermitReservation` owns the release
    /// obligation. Successful paths call `permit_reservation.forget()` once
    /// an `InFlightGuard` takes over. Any path that drops this message
    /// without explicit handling (accumulator task panics, channel send
    /// race during shutdown, etc.) releases the permits via `Drop` so
    /// `buffer_memory` is never leaked.
    Append(AppendCommand),
    /// Flush all batches.
    Flush {
        response_tx: oneshot::Sender<Result<()>>,
    },
    /// Shutdown the accumulator, flush remaining batches, and signal completion.
    Shutdown { response_tx: oneshot::Sender<()> },
}

/// RAII reservation of `bytes` permits on `memory_permits`.
///
/// Created in `append_with_guard` once the handle has successfully
/// `acquire_many`-ed. Travels with the `AccumulatorMessage::Append` into
/// the accumulator task; on the success path the accumulator calls
/// `forget()` to transfer ownership to an `InFlightGuard` (which will
/// eventually `add_permits` when the batch completes). On any other path
/// — explicit rejection, task panic, message dropped during shutdown —
/// `Drop` returns the bytes to the semaphore so `buffer_memory` is
/// never permanently stranded.
struct PermitReservation {
    bytes: usize,
    memory_permits: Arc<Semaphore>,
}

impl PermitReservation {
    /// Surrender the release obligation without leaking any allocation.
    ///
    /// Sets `bytes` to zero so that `Drop` calls `add_permits(0)`, which is
    /// a no-op in Tokio. The `Arc<Semaphore>` is dropped normally at end of
    /// scope. The caller is now responsible for eventually calling
    /// `add_permits(original_bytes)` on the same semaphore (typically via
    /// `InFlightGuard::drop`).
    fn forget(mut self) {
        self.bytes = 0;
    }
}

impl Drop for PermitReservation {
    fn drop(&mut self) {
        self.memory_permits.add_permits(self.bytes);
    }
}

impl std::fmt::Debug for PermitReservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PermitReservation")
            .field("bytes", &self.bytes)
            .finish()
    }
}

/// Handle to the record accumulator.
#[derive(Clone)]
pub struct RecordAccumulatorHandle {
    sender: mpsc::Sender<AccumulatorMessage>,
    /// Byte-granular FIFO semaphore gating `buffer_memory`.
    ///
    /// Each producer `append()` reserves `record.estimated_size()` permits
    /// via `acquire_many` before sending to the accumulator, and the
    /// matching `InFlightGuard` releases the same count via `add_permits`
    /// when the batch completes. Tokio's `Semaphore` queues waiters FIFO
    /// and wakes exactly the front waiter that can satisfy its request,
    /// eliminating the thundering-herd and fairness problems of the
    /// previous `Notify::notify_waiters()` design.
    memory_permits: Arc<Semaphore>,
    /// Initial semaphore capacity (= `buffer_memory`, or `MAX_PERMITS` when
    /// unlimited). Used to reject records larger than the entire budget
    /// with a structured error instead of blocking forever on `acquire_many`.
    memory_capacity: usize,
    /// Maximum encoded Kafka request frame size in bytes (0 = unlimited).
    /// Records that would exceed this limit are rejected before enqueueing
    /// rather than waiting to be rejected by the broker with MESSAGE_TOO_LARGE.
    max_request_size: usize,
    /// Maximum time to block waiting for buffer memory.
    max_block_ms: Duration,
    /// Barrier over all producer sends, including detached batch tasks.
    in_flight_barrier: Arc<InFlightBarrier>,
    /// Number of records currently admitted under the memory budget.
    buffered_records: Arc<AtomicUsize>,
    /// Shared producer metrics used to export buffered-record state.
    metrics: Arc<ProducerMetrics>,
}

impl RecordAccumulatorHandle {
    /// Append a record to the accumulator.
    ///
    /// If the accumulator buffer is full, blocks for up to `max_block_ms`
    /// waiting for memory to be freed before returning an error, matching
    /// the Kafka Java client's `max.block.ms` backpressure behavior.
    pub async fn append(
        &self,
        record: ProducerRecord,
        partition: PartitionId,
    ) -> Result<RecordMetadata> {
        let operation_guard = self.in_flight_barrier.start("producer")?;
        self.append_with_guard(record, partition, operation_guard)
            .await
    }

    pub(crate) async fn append_with_guard(
        &self,
        record: ProducerRecord,
        partition: PartitionId,
        operation_guard: InFlightOpGuard,
    ) -> Result<RecordMetadata> {
        let record_size = record.estimated_size();
        let routed = record.into_routed_parts();
        self.append_routed_with_guard(
            routed.topic,
            routed.record,
            record_size,
            partition,
            operation_guard,
        )
        .await
    }

    pub(crate) async fn append_routed_with_guard(
        &self,
        topic: TopicHandle,
        record: RoutedRecord,
        record_size: usize,
        partition: PartitionId,
        operation_guard: InFlightOpGuard,
    ) -> Result<RecordMetadata> {
        let deadline = tokio::time::Instant::now() + self.max_block_ms;

        // Reject records that cannot physically be admitted (exceeds the
        // semaphore permit limit, max_request_size, or the configured
        // buffer_memory budget). Uses the module-level helper so all three
        // branches are unit-testable without allocating large buffers.
        check_record_admission(record_size, self.memory_capacity, self.max_request_size)?;

        // FIFO-fair reservation of `record_size` bytes from the shared pool.
        // On timeout or closed semaphore (accumulator panicked), the permit
        // future cancels cleanly with no leaked reservation.
        let permit = match tokio::time::timeout(
            deadline.saturating_duration_since(tokio::time::Instant::now()),
            self.memory_permits.acquire_many(record_size as u32),
        )
        .await
        {
            Ok(Ok(p)) => p,
            Ok(Err(_)) => return Err(KrafkaError::invalid_state("accumulator closed")),
            Err(_) => {
                return Err(KrafkaError::timeout(
                    "producer append: max_block exceeded while waiting for buffer \
                     memory (ProducerConfig::max_block / AccumulatorConfig::max_block_ms)",
                ));
            }
        };

        // Transfer from the `SemaphorePermit` future to the RAII
        // `PermitReservation`. Construction happens BEFORE `permit.forget()` so
        // there is no window where permits are orphaned if `Arc::clone` were
        // ever to panic (it never does, but the ordering makes the intent
        // explicit and keeps the two sides of the handoff adjacent).
        let permit_reservation = PermitReservation {
            bytes: record_size,
            memory_permits: self.memory_permits.clone(),
        };
        // Discard the `SemaphorePermit` without releasing its permits;
        // `permit_reservation` is now the sole release authority.
        permit.forget();

        let (response_tx, response_rx) = oneshot::channel();
        let buffered_record_guard =
            BufferedRecordGuard::new(self.buffered_records.clone(), self.metrics.clone());
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());

        // Send the Append; on failure (timeout / closed channel),
        // `permit_reservation` drops and returns the permits to the pool
        // so another waiter can proceed. On success the accumulator now
        // owns the release obligation via the message contents.
        match tokio::time::timeout(
            remaining,
            self.sender.send(AccumulatorMessage::Append(AppendCommand {
                topic,
                record,
                partition,
                record_size,
                response_tx,
                operation_guard,
                _buffered_record_guard: buffered_record_guard,
                permit_reservation,
            })),
        )
        .await
        {
            Ok(Ok(())) => {}
            Ok(Err(_)) => return Err(KrafkaError::invalid_state("accumulator closed")),
            Err(_) => {
                return Err(KrafkaError::timeout(
                    "producer append: max_block exceeded while sending to accumulator",
                ));
            }
        }

        match response_rx
            .await
            .map_err(|_| KrafkaError::invalid_state("accumulator response dropped"))?
        {
            AppendResponse::Done(result) => result,
        }
    }

    /// Flush all pending batches.
    pub async fn flush(&self) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(AccumulatorMessage::Flush { response_tx })
            .await
            .map_err(|_| KrafkaError::invalid_state("accumulator closed"))?;

        response_rx
            .await
            .map_err(|_| KrafkaError::invalid_state("accumulator response dropped"))?
    }

    /// Shutdown the accumulator, flushing all pending batches before returning.
    ///
    /// Returns an error if the accumulator task has already exited (e.g. due to
    /// a panic) and the shutdown message cannot be delivered.
    pub async fn shutdown(&self) -> Result<()> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(AccumulatorMessage::Shutdown { response_tx })
            .await
            .map_err(|_| {
                warn!("Accumulator shutdown failed: task already exited");
                KrafkaError::invalid_state("accumulator already shut down")
            })?;
        // Wait for the accumulator to finish flushing before returning.
        response_rx.await.map_err(|_| {
            warn!("Accumulator shutdown: response channel dropped before completion");
            KrafkaError::invalid_state("accumulator shutdown interrupted")
        })?;
        Ok(())
    }
}

/// Configuration for the record accumulator.
pub struct AccumulatorConfig {
    /// Maximum batch size in bytes.
    pub batch_size: usize,
    /// Time to wait before sending a partial batch.
    pub linger: Duration,
    /// Compression type for batches.
    pub compression: Compression,
    /// Per-topic compression overrides.
    ///
    /// When a topic is present in this map, its compression type takes
    /// precedence over the global [`compression`](Self::compression) field.
    pub topic_compression: AHashMap<String, Compression>,
    /// Acknowledgment level.
    pub acks: i16,
    /// Client ID used for request frame sizing.
    pub client_id: String,
    /// Request timeout.
    pub request_timeout: Duration,
    /// Maximum encoded Kafka request frame size in bytes.
    pub max_request_size: usize,
    /// Maximum total memory for buffering (bytes).
    /// When this limit is reached, append operations will block until memory is freed.
    /// Set to 0 for unlimited (not recommended for production).
    pub buffer_memory: usize,
    /// Maximum time to block waiting for buffer memory (ms).
    /// If memory is not available within this time, an error is returned.
    pub max_block_ms: Duration,
    /// In-flight semaphore for concurrency limiting (shared with direct send path).
    pub in_flight_semaphore: Arc<Semaphore>,
    /// Producer interceptor for on_acknowledgement callbacks.
    pub interceptor: Arc<dyn ProducerInterceptor>,
    /// Producer identity for idempotent production (PID, epoch, sequences).
    pub identity: Option<Arc<super::idempotent::ProducerIdentity>>,
    /// Partitioner for batch-advance notifications (KIP-794).
    ///
    /// When a batch for `(topic, partition)` fills up, the accumulator calls
    /// [`super::partitioner::Partitioner::on_new_batch`] so that batch-boundary partitioners such as
    /// [`UniformStickyPartitioner`] can advance their sticky partition before the
    /// next record is routed. Partitioners that ignore batch events (the default
    /// no-op implementation) incur no overhead.
    ///
    /// [`UniformStickyPartitioner`]: super::partitioner::UniformStickyPartitioner
    pub partitioner: Arc<dyn super::partitioner::Partitioner>,
    /// Optional pluggable persistence hook for producer identity state.
    ///
    /// When set, a snapshot is persisted (fire-and-forget) after each
    /// successful batch acknowledgement.
    pub(crate) state_store: Option<Arc<dyn super::idempotent::ErasedProducerStateStore>>,
    /// Transactional ID stamped onto every `ProduceRequest`.
    ///
    /// `Some(_)` only for a [`TransactionalProducer`](super::TransactionalProducer),
    /// which routes its sends through this accumulator so that transactional
    /// production batches instead of issuing one `acks=all` round trip per
    /// record. `None` for plain and idempotent producers.
    pub transactional_id: Option<String>,
}

impl Clone for AccumulatorConfig {
    fn clone(&self) -> Self {
        Self {
            batch_size: self.batch_size,
            linger: self.linger,
            compression: self.compression,
            topic_compression: self.topic_compression.clone(),
            acks: self.acks,
            client_id: self.client_id.clone(),
            request_timeout: self.request_timeout,
            max_request_size: self.max_request_size,
            buffer_memory: self.buffer_memory,
            max_block_ms: self.max_block_ms,
            in_flight_semaphore: self.in_flight_semaphore.clone(),
            interceptor: self.interceptor.clone(),
            identity: self.identity.clone(),
            partitioner: self.partitioner.clone(),
            state_store: self.state_store.clone(),
            transactional_id: self.transactional_id.clone(),
        }
    }
}

impl fmt::Debug for AccumulatorConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AccumulatorConfig")
            .field("batch_size", &self.batch_size)
            .field("linger", &self.linger)
            .field("compression", &self.compression)
            .field("acks", &self.acks)
            .field("client_id", &self.client_id)
            .field("request_timeout", &self.request_timeout)
            .field("max_request_size", &self.max_request_size)
            .field("buffer_memory", &self.buffer_memory)
            .field("max_block_ms", &self.max_block_ms)
            .field("interceptor", &self.interceptor)
            .field("partitioner", &"<dyn Partitioner>")
            .finish()
    }
}

impl Default for AccumulatorConfig {
    fn default() -> Self {
        Self {
            batch_size: 16384,
            linger: Duration::ZERO,
            compression: Compression::None,
            topic_compression: AHashMap::new(),
            acks: -1,
            client_id: "krafka".to_string(),
            request_timeout: Duration::from_secs(30),
            max_request_size: crate::protocol::MAX_MESSAGE_SIZE,
            buffer_memory: 32 * 1024 * 1024, // 32 MB default (same as Kafka Java client)
            max_block_ms: Duration::from_secs(60), // 60 seconds default
            in_flight_semaphore: Arc::new(Semaphore::new(5)), // default max_in_flight
            interceptor: Arc::new(crate::interceptor::NoOpProducerInterceptor),
            identity: None,
            partitioner: Arc::new(super::partitioner::UniformStickyPartitioner::new()),
            state_store: None,
            transactional_id: None,
        }
    }
}

/// A pending record waiting for its batch to be sent.
struct PendingRecord {
    record: RoutedRecord,
    response_tx: oneshot::Sender<AppendResponse>,
    offset_in_batch: i64,
    /// Estimated size in bytes for memory tracking.
    estimated_size: usize,
    /// Tracks this record in the producer buffered-records gauge until it is
    /// acknowledged or dropped on failure.
    _buffered_record_guard: BufferedRecordGuard,
    /// Producer-wide operation guard that completes only after ack/failure.
    _operation_guard: InFlightOpGuard,
}

/// RAII guard that releases `buffer_memory` permits and in-flight byte
/// tracking on drop.
///
/// Created by `extract_batch` and passed to `send_extracted_batch`.
/// When the send task completes (or panics), the guard automatically
/// decrements `in_flight_memory` for metrics and releases `bytes` permits
/// back to `memory_permits`, waking the front FIFO waiter whose request
/// can now be satisfied.
struct InFlightGuard {
    bytes: usize,
    in_flight_memory: Arc<AtomicUsize>,
    memory_permits: Arc<Semaphore>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.in_flight_memory
            .fetch_sub(self.bytes, Ordering::Relaxed);
        self.memory_permits.add_permits(self.bytes);
    }
}

/// A batch with its pending records.
struct AccumulatorBatch {
    /// Current byte-size estimate of all tracked records.
    current_size: usize,
    /// Maximum batch size in bytes.
    max_size: usize,
    /// Pending records waiting to be sent.
    pending: Vec<PendingRecord>,
    /// When the batch was created (for linger expiry).
    created_at: Instant,
}

impl AccumulatorBatch {
    fn new(max_size: usize) -> Self {
        Self {
            current_size: 0,
            max_size,
            pending: Vec::new(),
            created_at: Instant::now(),
        }
    }

    /// Return `true` if the batch contains no records.
    #[inline]
    fn is_empty(&self) -> bool {
        self.pending.is_empty()
    }

    /// Number of records currently tracked.
    #[inline]
    fn len(&self) -> usize {
        self.pending.len()
    }

    /// Return `true` when the batch has reached its maximum size.
    #[inline]
    fn is_full(&self) -> bool {
        self.current_size >= self.max_size
    }

    /// Return `true` if a record of `record_size` bytes would fit.
    ///
    /// An empty batch always accepts the first record.
    #[inline]
    fn would_fit(&self, record_size: usize) -> bool {
        self.is_empty() || self.current_size + record_size <= self.max_size
    }

    /// Increment the running byte-size tracker.
    #[inline]
    fn track(&mut self, record_size: usize) {
        self.current_size += record_size;
    }

    fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

/// The record accumulator.
pub struct RecordAccumulator {
    /// Configuration.
    config: AccumulatorConfig,
    /// Batches per topic-partition.
    batches: AHashMap<(TopicHandle, PartitionId), AccumulatorBatch>,
    /// Cluster metadata for sending.
    metadata: Arc<ClusterMetadata>,
    /// Shared semaphore limiting the total concurrent `send_extracted_batch`
    /// tasks across **all** drain waves (linger, flush, close).
    ///
    /// Each task acquires one permit before being spawned and holds it until
    /// completion.  This ensures overlapping linger waves cannot compound the
    /// task count beyond `MAX_CONCURRENT_BATCH_SENDS`.
    send_semaphore: Arc<Semaphore>,
    /// Memory held by in-flight send tasks (extracted but not yet completed).
    /// Exposed for metrics only; backpressure is enforced by `memory_permits`.
    in_flight_memory: Arc<AtomicUsize>,
    /// Retry policy for transient failures.
    retry_policy: RetryPolicy,
    /// Shared metrics.
    metrics: Arc<ProducerMetrics>,
    /// Byte-granular FIFO semaphore gating `buffer_memory` (shared with handle).
    memory_permits: Arc<Semaphore>,
    /// Partitioner reference for KIP-794 batch-boundary advance notifications.
    partitioner: Arc<dyn super::partitioner::Partitioner>,
    /// Per-`(topic, partition)` dispatch FIFO.
    ///
    /// Owned by the run loop; each sealed batch clones the `Arc` into its send
    /// task so that batch *n+1* cannot reach the wire (or allocate an
    /// idempotent sequence) until batch *n* is acknowledged or permanently
    /// failed. See [`PartitionInFlight`].
    partition_inflight: AHashMap<(TopicHandle, PartitionId), Arc<PartitionInFlight>>,
}

impl RecordAccumulator {
    /// Create a new record accumulator and return a handle.
    pub(crate) fn spawn(
        config: AccumulatorConfig,
        metadata: Arc<ClusterMetadata>,
        retry_policy: RetryPolicy,
        metrics: Arc<ProducerMetrics>,
        in_flight_barrier: Arc<InFlightBarrier>,
    ) -> RecordAccumulatorHandle {
        // Cap the channel at 256 to limit untracked memory sitting in the
        // channel before the accumulator processes it. When buffer_memory
        // is configured, we shrink further so at most ~10% of the budget
        // can be untracked. (Strictly speaking permits are already held
        // before send, so the channel sits on top of the permit layer;
        // the cap is still useful to bound scheduler queueing.)
        let channel_capacity = if config.buffer_memory > 0 {
            let batch = config.batch_size.max(1);
            (config.buffer_memory / 10 / batch).clamp(1, 256)
        } else {
            64
        };
        let (sender, receiver) = mpsc::channel(channel_capacity);

        // Semaphore capacity: `buffer_memory` when bounded, or
        // `Semaphore::MAX_PERMITS` (effectively unlimited) when `buffer_memory
        // = 0`. A single `acquire_many` call still takes a `u32` request,
        // so the per-record cap is `u32::MAX` regardless. If the caller
        // configured `buffer_memory` above `Semaphore::MAX_PERMITS`
        // (`usize::MAX >> 3`, only reachable on 32-bit targets in practice),
        // we clamp and emit a single `warn!` so the effective cap is
        // explicit rather than silent.
        let memory_capacity = effective_memory_capacity(config.buffer_memory);
        let memory_permits = Arc::new(Semaphore::new(memory_capacity));
        let in_flight_memory = Arc::new(AtomicUsize::new(0));
        let buffered_records = Arc::new(AtomicUsize::new(0));
        let handle_buffered_records = buffered_records.clone();
        let handle_metrics = metrics.clone();
        let max_block_ms = config.max_block_ms;
        let max_request_size = config.max_request_size;
        // Extract partitioner before config is moved into the accumulator.
        let accumulator_partitioner = config.partitioner.clone();

        let accumulator = Self {
            config,
            batches: AHashMap::new(),
            metadata,
            send_semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_BATCH_SENDS)),
            in_flight_memory,
            retry_policy,
            metrics,
            memory_permits: memory_permits.clone(),
            partitioner: accumulator_partitioner,
            partition_inflight: AHashMap::new(),
        };

        let memory_permits_panic = memory_permits.clone();
        tokio::spawn(async move {
            let join_handle = tokio::spawn(accumulator.run(receiver));
            if let Err(join_err) = join_handle.await {
                if join_err.is_panic() {
                    tracing::error!("Accumulator task panicked: {join_err}");
                } else {
                    tracing::error!("Accumulator task cancelled: {join_err}");
                }
                // Close the semaphore so all blocked `acquire_many` calls
                // in `append_with_guard` return an error instead of hanging
                // forever. New callers will also fail immediately.
                memory_permits_panic.close();
            }
        });

        RecordAccumulatorHandle {
            sender,
            memory_permits,
            memory_capacity,
            max_request_size,
            max_block_ms,
            in_flight_barrier,
            buffered_records: handle_buffered_records,
            metrics: handle_metrics,
        }
    }

    /// Run the accumulator background task.
    async fn run(mut self, mut receiver: mpsc::Receiver<AccumulatorMessage>) {
        // Linger timer interval - check every 1ms for expired batches
        let linger_check_interval = Duration::from_millis(1).max(self.config.linger / 10);
        let mut linger_timer = interval(linger_check_interval);
        linger_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                msg = receiver.recv() => {
                    match msg {
                        Some(AccumulatorMessage::Append(append)) => {
                            self.handle_append(append).await;
                        }
                        Some(AccumulatorMessage::Flush { response_tx }) => {
                            let result = self.flush_all().await;
                            let _ = response_tx.send(result);
                        }
                        Some(AccumulatorMessage::Shutdown { response_tx }) => {
                            debug!("Accumulator shutting down, flushing remaining batches");
                            let _ = self.flush_all().await;
                            let _ = response_tx.send(());
                            break;
                        }
                        None => {
                            debug!("Accumulator channel closed, flushing remaining batches");
                            let _ = self.flush_all().await;
                            break;
                        }
                    }
                }
                _ = linger_timer.tick() => {
                    self.check_linger_expiry();
                }
            }
        }

        debug!("Accumulator shutdown complete");
    }

    /// Handle appending a record.
    ///
    /// `record_size` duplicates `permit_reservation.bytes` for fast access;
    /// the reservation owns the release obligation. Successful paths call
    /// `permit_reservation.forget()` so the eventual `InFlightGuard` can
    /// release the permits on batch completion; error paths let the
    /// reservation drop naturally, returning the permits to the pool.
    async fn handle_append(&mut self, append: AppendCommand) {
        let AppendCommand {
            topic,
            record,
            partition,
            record_size,
            response_tx,
            operation_guard,
            _buffered_record_guard: buffered_record_guard,
            permit_reservation,
        } = append;
        let key = (topic, partition);

        // Backpressure is enforced in `append_with_guard` via
        // `memory_permits.acquire_many(record_size)`; by the time we get
        // here the bytes are already reserved, so no buffer-size check
        // is needed.

        // Get or create batch. `or_insert_with` closure needs an owned
        // topic string; `key.0.clone()` happens only on the first insert.
        let batch_size = self.config.batch_size;
        let accumulator_batch = self
            .batches
            .entry(key.clone())
            .or_insert_with(|| AccumulatorBatch::new(batch_size));

        // Check if the record fits in the current batch. If so, move it
        // directly into PendingRecord (zero clones). The batch only tracks
        // size; PendingRecord owns the record data for send_extracted_batch.
        let offset = accumulator_batch.len() as i64;
        if accumulator_batch.would_fit(record_size) {
            accumulator_batch.track(record_size);
            accumulator_batch.pending.push(PendingRecord {
                record,
                response_tx,
                offset_in_batch: offset,
                estimated_size: record_size,
                _buffered_record_guard: buffered_record_guard,
                _operation_guard: operation_guard,
            });
            // Release is now owned by the eventual `InFlightGuard`.
            permit_reservation.forget();

            // Check if batch is full
            if accumulator_batch.is_full() {
                trace!("Batch full for {}-{}, flushing", key.0, partition);
                let partition_count = self
                    .metadata
                    .partition_count(key.0.as_ref())
                    .unwrap_or(partition as usize + 1);
                self.partitioner
                    .on_new_batch(key.0.as_ref(), partition, partition_count);
                self.flush_batch(&key);
            } else if self.config.linger.is_zero() {
                // linger=0 means send immediately without waiting
                // for the next linger timer tick (up to 1ms delay otherwise).
                trace!("Linger=0 for {}-{}, flushing immediately", key.0, partition);
                self.flush_batch(&key);
            }
        } else {
            // Batch is full, flush it first and then add to new batch
            let partition_count = self
                .metadata
                .partition_count(key.0.as_ref())
                .unwrap_or(partition as usize + 1);
            self.partitioner
                .on_new_batch(key.0.as_ref(), partition, partition_count);
            self.flush_batch(&key);

            // Create new batch and add record
            let mut new_batch = AccumulatorBatch::new(batch_size);

            if new_batch.would_fit(record_size) {
                new_batch.track(record_size);
                new_batch.pending.push(PendingRecord {
                    record,
                    response_tx,
                    offset_in_batch: 0,
                    estimated_size: record_size,
                    _buffered_record_guard: buffered_record_guard,
                    _operation_guard: operation_guard,
                });
                self.batches.insert(key, new_batch);
                // Release is now owned by the eventual `InFlightGuard`.
                permit_reservation.forget();
            } else {
                // Record too large for batch size — drop the reservation,
                // which returns the permits to the pool so another
                // producer can make progress, then surface the error.
                drop(permit_reservation);
                let _ = response_tx.send(AppendResponse::Done(Err(KrafkaError::config(
                    "record too large for batch size",
                ))));
            }
        }
    }

    /// Check for batches that have exceeded their linger time and detach sends.
    ///
    /// Extracts expired batches synchronously, then dispatches them via
    /// `spawn_batches_detached` so the accumulator's run loop is never held
    /// waiting for network I/O. When `linger` is zero, delegates to
    /// `flush_all_ready`.
    fn check_linger_expiry(&mut self) {
        self.prune_idle_partition_inflight();

        if self.config.linger.is_zero() {
            self.flush_all_ready();
            return;
        }

        let keys_to_flush: Vec<_> = self
            .batches
            .iter()
            .filter(|(_, batch)| !batch.is_empty() && batch.age() >= self.config.linger)
            .map(|(key, _)| key.clone())
            .collect();

        if keys_to_flush.is_empty() {
            return;
        }

        let mut extracted = Vec::with_capacity(keys_to_flush.len());
        for key in keys_to_flush {
            trace!("Linger expired for {:?}, flushing", key);
            if let Some(item) = self.extract_batch(&key) {
                extracted.push((key, item));
            }
        }

        Self::spawn_batches_detached(
            extracted,
            &self.metadata,
            &self.config,
            &self.retry_policy,
            &self.metrics,
            self.send_semaphore.clone(),
        );
    }

    /// Flush all ready batches by detaching send tasks.
    ///
    /// Extracts all non-empty batches synchronously, then hands them off to
    /// `spawn_batches_detached` so the run loop is never blocked by network
    /// I/O. The shared `send_semaphore` caps the total concurrent task count
    /// across all in-flight waves to `MAX_CONCURRENT_BATCH_SENDS`.
    fn flush_all_ready(&mut self) {
        let keys_to_flush: Vec<_> = self
            .batches
            .iter()
            .filter(|(_, batch)| !batch.is_empty())
            .map(|(key, _)| key.clone())
            .collect();

        if keys_to_flush.is_empty() {
            return;
        }

        let mut extracted = Vec::with_capacity(keys_to_flush.len());
        for key in keys_to_flush {
            if let Some(item) = self.extract_batch(&key) {
                extracted.push((key, item));
            }
        }

        Self::spawn_batches_detached(
            extracted,
            &self.metadata,
            &self.config,
            &self.retry_policy,
            &self.metrics,
            self.send_semaphore.clone(),
        );
    }

    /// Drive a wave of batch sends to completion, capping concurrent **network
    /// sends** at `MAX_CONCURRENT_BATCH_SENDS`.
    ///
    /// Each spawned task takes its partition's dispatch turn first and only
    /// then acquires a `send_semaphore` permit, which it holds until the batch
    /// completes. Because `send_semaphore` is shared across all in-flight
    /// waves, the number of batches concurrently on the wire is bounded
    /// globally rather than per wave (F-021 fix).
    ///
    /// # Why the permit is taken inside the task
    ///
    /// It would be cheaper to acquire the permit in this loop, before spawning,
    /// and that is what this code used to do. Combined with the per-partition
    /// FIFO it deadlocks: tasks from wave *N+1* park on their partition turn
    /// while holding permits, and wave *N* — which owns the earlier tickets
    /// those tasks are waiting for — cannot spawn the unblocking task because
    /// every permit is taken. Acquiring inside the task, after the turn, means
    /// a permit is only ever held by a batch that can actually make progress,
    /// so the cycle cannot form.
    ///
    /// Tasks parked on a partition turn are idle futures, not threads; their
    /// count is bounded by the buffered batches, which `buffer_memory` already
    /// caps.
    async fn spawn_batches_bounded(
        extracted: ExtractedBatches,
        metadata: &Arc<ClusterMetadata>,
        config: &AccumulatorConfig,
        retry_policy: &RetryPolicy,
        metrics: &Arc<ProducerMetrics>,
        send_semaphore: Arc<Semaphore>,
    ) {
        let mut join_set = tokio::task::JoinSet::new();
        for (
            (topic, partition),
            ExtractedBatch {
                batch,
                guard,
                ticket,
            },
        ) in extracted
        {
            // Clone shared handles outside the acquire so the borrow ends
            // before entering the `async move` block (which must be `'static`).
            let metadata = metadata.clone();
            let config = config.clone();
            let retry_policy = retry_policy.clone();
            let metrics = metrics.clone();
            let send_semaphore = send_semaphore.clone();
            join_set.spawn(async move {
                Self::send_extracted_batch(
                    send_semaphore,
                    topic,
                    partition,
                    batch.pending,
                    batch.created_at,
                    guard,
                    ticket,
                    metadata,
                    config,
                    retry_policy,
                    metrics,
                )
                .await;
            });
        }
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result
                && e.is_panic()
            {
                warn!("send_extracted_batch task panicked: {e}");
            }
        }
    }

    /// Detach a bounded batch-send wave so the accumulator run loop is not
    /// blocked by in-flight network I/O.
    ///
    /// Clones the shared handles, spawns a single Tokio task, and returns
    /// immediately.  Inside the task, `spawn_batches_bounded` acquires
    /// permits from `send_semaphore` (shared across **all** waves) before
    /// each spawn, so the total concurrent task count across overlapping
    /// waves is bounded by `MAX_CONCURRENT_BATCH_SENDS`.
    fn spawn_batches_detached(
        extracted: ExtractedBatches,
        metadata: &Arc<ClusterMetadata>,
        config: &AccumulatorConfig,
        retry_policy: &RetryPolicy,
        metrics: &Arc<ProducerMetrics>,
        send_semaphore: Arc<Semaphore>,
    ) {
        if extracted.is_empty() {
            return;
        }
        let metadata = metadata.clone();
        let config = config.clone();
        let retry_policy = retry_policy.clone();
        let metrics = metrics.clone();
        // Fire-and-forget: drop the JoinHandle immediately to make the
        // detached semantics explicit. The spawned task is self-contained —
        // `InFlightGuard` reclaims memory permits on completion or panic —
        // so there is nothing to join.
        drop(tokio::spawn(async move {
            Self::spawn_batches_bounded(
                extracted,
                &metadata,
                &config,
                &retry_policy,
                &metrics,
                send_semaphore,
            )
            .await;
        }));
    }

    /// Extract a batch from the accumulator and account its byte count
    /// against the in-flight tracker.
    ///
    /// The permits for these bytes are already "forgotten" (ownership
    /// transferred away from the handle's acquire future when the Append
    /// message was sent); the returned `InFlightGuard` carries the
    /// obligation to release an equivalent count via `add_permits` when
    /// the send task completes or panics — see `send_extracted_batch`.
    fn extract_batch(&mut self, key: &(TopicHandle, PartitionId)) -> Option<ExtractedBatch> {
        let batch = self.batches.remove(key)?;
        if batch.is_empty() {
            return None;
        }
        let batch_memory: usize = batch.pending.iter().map(|p| p.estimated_size).sum();
        self.in_flight_memory
            .fetch_add(batch_memory, Ordering::Relaxed);
        let guard = InFlightGuard {
            bytes: batch_memory,
            in_flight_memory: self.in_flight_memory.clone(),
            memory_permits: self.memory_permits.clone(),
        };
        // Draw the partition's next FIFO ticket here, in the run loop, so that
        // ticket order is exactly seal order. The send task blocks on this
        // ticket before allocating a sequence, which is what makes idempotent
        // sequence order match wire order.
        let ticket = self
            .partition_inflight
            .entry(key.clone())
            .or_default()
            .take_ticket();
        Some(ExtractedBatch {
            batch,
            guard,
            ticket,
        })
    }

    /// Drop dispatch-FIFO entries for partitions with no outstanding batches.
    ///
    /// Without this the map would grow once per `(topic, partition)` ever
    /// written to and never shrink. Called from the linger tick, which is the
    /// only place guaranteed to run even when the producer goes idle.
    fn prune_idle_partition_inflight(&mut self) {
        if self.partition_inflight.len() <= PARTITION_INFLIGHT_PRUNE_THRESHOLD {
            return;
        }
        self.partition_inflight
            .retain(|_, slot| Arc::strong_count(slot) > 1 || !slot.is_idle());
    }

    /// Flush a specific batch by routing through `spawn_batches_detached`.
    ///
    /// All flush paths (`flush_batch`, `flush_all_ready`, `check_linger_expiry`)
    /// funnel through `spawn_batches_detached` → `spawn_batches_bounded`,
    /// ensuring the `MAX_CONCURRENT_BATCH_SENDS` ceiling is applied uniformly.
    /// A single-entry vec exits the bounded loop before any backpressure
    /// point, so this path adds no observable overhead beyond the outer
    /// wrapper task that `spawn_batches_detached` spawns.
    fn flush_batch(&mut self, key: &(TopicHandle, PartitionId)) {
        if let Some(item) = self.extract_batch(key) {
            Self::spawn_batches_detached(
                vec![(key.clone(), item)],
                &self.metadata,
                &self.config,
                &self.retry_policy,
                &self.metrics,
                self.send_semaphore.clone(),
            );
        }
    }

    /// Send an extracted batch to the broker, honouring this partition's
    /// dispatch FIFO.
    ///
    /// This is a free-standing (static) method so that batches for different
    /// partitions can be driven concurrently from a `JoinSet`.
    ///
    /// # Ordering contract
    ///
    /// The first thing this does is await [`PartitionTicket::acquire`], which
    /// blocks until every batch that the accumulator sealed *earlier* for the
    /// same `(topic, partition)` has been acknowledged or has permanently
    /// failed. Only then is the idempotent sequence range allocated. Because
    /// tickets are drawn in the single-threaded accumulator run loop, the
    /// resulting chain is:
    ///
    /// ```text
    /// seal order == ticket order == sequence-allocation order == wire order
    /// ```
    ///
    /// which is exactly what an idempotent producer needs — the broker never
    /// sees sequence *n+1* before *n*, so `OUT_OF_ORDER_SEQUENCE_NUMBER`,
    /// permanent reordering, and gap-wedged partitions are all structurally
    /// impossible. Batches for *different* partitions are unaffected and still
    /// proceed fully in parallel.
    ///
    /// The turn is released by `Drop` on every exit path, including panics, so
    /// a lost send task cannot stall its partition.
    #[allow(clippy::too_many_arguments)]
    async fn send_extracted_batch(
        send_semaphore: Arc<Semaphore>,
        topic: TopicHandle,
        partition: PartitionId,
        pending: Vec<PendingRecord>,
        enqueued_at: Instant,
        _in_flight_guard: InFlightGuard,
        ticket: PartitionTicket,
        metadata: Arc<ClusterMetadata>,
        config: AccumulatorConfig,
        retry_policy: RetryPolicy,
        metrics: Arc<ProducerMetrics>,
    ) {
        // Serialize with earlier batches for this partition. Held until
        // this function returns.
        let _turn = ticket.acquire().await;

        // Only now take a global send slot — see `spawn_batches_bounded` for
        // why the order matters. A closed semaphore means the accumulator is
        // shutting down; drop the batch (memory permits are reclaimed by
        // `InFlightGuard`, and callers observe a dropped response channel).
        let Ok(_send_slot) = send_semaphore.acquire_owned().await else {
            return;
        };

        if let Some(identity) = config.identity.as_ref() {
            // A transactional producer owns its identity lifecycle: the PID and
            // epoch come from a *transactional* InitProducerId issued against
            // the transaction coordinator, and epoch bumps happen in
            // `TransactionalProducer`. Never run the plain idempotent
            // InitProducerId here — it would replace the transactional identity
            // with an unfenced one.
            let init_result = if config.transactional_id.is_some() {
                if identity.is_initialized() {
                    Ok(())
                } else {
                    Err(KrafkaError::invalid_state(
                        "transactional producer identity not initialized; \
                         call init_transactions() before sending",
                    ))
                }
            } else {
                super::ensure_idempotent_producer_id_initialized(identity, &metadata, &retry_policy)
                    .await
            };

            if let Err(error) = init_result {
                metrics.record_error_for_topic(topic.as_ref());
                for pending_record in pending {
                    let _ = pending_record
                        .response_tx
                        .send(AppendResponse::Done(Err(error.clone())));
                }
                return;
            }
        }

        // Acquire in-flight permit before sending (accumulator was
        // bypassing max_in_flight). The permit is held until this batch completes.
        let _permit = config.in_flight_semaphore.acquire().await;
        let _timer = metrics.send_latency.start();

        Self::produce_pending(
            &topic,
            partition,
            pending,
            enqueued_at,
            &metadata,
            &config,
            &retry_policy,
            &metrics,
            0,
        )
        .await;
    }

    /// Encode one produce request for `pending`.
    ///
    /// Returns the request plus `(compressed_bytes, uncompressed_bytes)` so the
    /// caller can track the compression ratio.
    ///
    /// CPU-heavy codecs (Gzip, Zstd) are encoded on the blocking pool rather
    /// than inline, so a large batch cannot stall the async worker thread that
    /// is also driving every other partition's I/O. Cheap codecs stay inline
    /// because the `spawn_blocking` hop would cost more than the compression.
    async fn encode_batch_request(
        topic: &TopicHandle,
        partition: PartitionId,
        pending: &[PendingRecord],
        sequence: Option<i32>,
        config: &AccumulatorConfig,
    ) -> Result<(ProduceRequest, u64, u64)> {
        let effective_compression = config
            .topic_compression
            .get(topic.as_ref())
            .copied()
            .unwrap_or(config.compression);
        let mut batch_builder = RecordBatchBuilder::new().compression(effective_compression);

        // Tag with idempotent producer identity
        if let (Some(identity), Some(s)) = (&config.identity, sequence) {
            batch_builder =
                batch_builder.producer(identity.producer_id(), identity.producer_epoch(), s);
        }
        // Mark the record batch transactional so consumers with
        // `read_committed` isolation hold it back until the transaction ends.
        if config.transactional_id.is_some() {
            batch_builder = batch_builder.transactional(true);
        }

        // Accumulate uncompressed payload size before encoding.
        let uncompressed_len: u64 = pending.iter().map(|p| p.estimated_size as u64).sum();

        for p in pending {
            batch_builder = p.record.append_to_batch_builder(batch_builder);
        }
        let batch = batch_builder.build();

        let batch_bytes = if is_cpu_heavy_compression(effective_compression) {
            tokio::task::spawn_blocking(move || batch.encode())
                .await
                .map_err(|join_err| {
                    KrafkaError::invalid_state(format!(
                        "record-batch compression task failed: {join_err}"
                    ))
                })??
        } else {
            batch.encode()?
        };
        let compressed_len = batch_bytes.len() as u64;

        Ok((
            ProduceRequest {
                transactional_id: config.transactional_id.clone(),
                acks: config.acks,
                timeout_ms: crate::util::duration_to_millis_i32(config.request_timeout),
                topic_data: vec![ProduceTopicData {
                    name: topic.to_string(),
                    topic_id: None,
                    partition_data: vec![ProducePartitionData {
                        index: partition,
                        records: batch_bytes,
                    }],
                }],
            },
            compressed_len,
            uncompressed_len,
        ))
    }

    /// Allocate a sequence range, encode, send, retry, and complete `pending`.
    ///
    /// **Must** be called while holding this partition's [`PartitionTurn`]:
    /// the sequence range is allocated here, and the ordering guarantee in
    /// [`send_extracted_batch`] depends on no other batch for the partition
    /// allocating concurrently.
    ///
    /// `split_depth` bounds the `MESSAGE_TOO_LARGE` recursion — see
    /// [`MAX_BATCH_SPLIT_DEPTH`].
    ///
    /// Returns a boxed future rather than being a plain `async fn`: the body
    /// calls itself when a batch has to be split, and type-erasing the return
    /// here is what breaks the otherwise-infinite future type.
    #[allow(clippy::too_many_arguments)]
    fn produce_pending<'a>(
        topic: &'a TopicHandle,
        partition: PartitionId,
        pending: Vec<PendingRecord>,
        enqueued_at: Instant,
        metadata: &'a Arc<ClusterMetadata>,
        config: &'a AccumulatorConfig,
        retry_policy: &'a RetryPolicy,
        metrics: &'a Arc<ProducerMetrics>,
        split_depth: u8,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(Self::produce_pending_inner(
            topic,
            partition,
            pending,
            enqueued_at,
            metadata,
            config,
            retry_policy,
            metrics,
            split_depth,
        ))
    }

    /// Body of [`produce_pending`](Self::produce_pending); see that method for
    /// the contract.
    #[allow(clippy::too_many_arguments)]
    async fn produce_pending_inner(
        topic: &TopicHandle,
        partition: PartitionId,
        mut pending: Vec<PendingRecord>,
        enqueued_at: Instant,
        metadata: &Arc<ClusterMetadata>,
        config: &AccumulatorConfig,
        retry_policy: &RetryPolicy,
        metrics: &Arc<ProducerMetrics>,
        split_depth: u8,
    ) {
        let record_count = pending.len() as i32;
        if record_count == 0 {
            return;
        }

        // Allocate the sequence range for idempotent production.
        //
        // This happens under the partition turn, so allocation order is
        // dispatch order and the failing batch is always the tail *and* the
        // head of the partition's outstanding range. That is what makes the
        // rollback and out-of-order checks below decidable.
        let mut sequence: Option<i32> = match config
            .identity
            .as_ref()
            .map(|id| id.allocate_sequence(topic.as_ref(), partition, record_count))
            .transpose()
        {
            Ok(s) => s,
            Err(e) => {
                for p in pending {
                    let _ = p.response_tx.send(AppendResponse::Done(Err(e.clone())));
                }
                return;
            }
        };

        let (mut request, compressed_len, uncompressed_len) =
            match Self::encode_batch_request(topic, partition, &pending, sequence, config).await {
                Ok(r) => r,
                Err(e) => {
                    // Encode failure: nothing reached the wire, so the range
                    // is still the tail and can be safely rewound.
                    if let (Some(identity), Some(base)) = (config.identity.as_ref(), sequence) {
                        Self::release_failed_sequence_range(
                            identity,
                            topic,
                            partition,
                            base,
                            record_count,
                        );
                    }
                    for p in pending {
                        let _ = p.response_tx.send(AppendResponse::Done(Err(e.clone())));
                    }
                    return;
                }
            };

        // Track estimated compression ratio for compressed batches.
        if config.compression != Compression::None {
            metrics.record_compression(compressed_len, uncompressed_len);
        }

        // Retry loop — delivery timeout starts from when the first record
        // entered the batch (enqueued_at), not from the first send attempt,
        // so that time spent in the linger window / backpressure counts
        // against the delivery budget (matching Java client behavior).
        let mut retry_ctx = RetryContext::new_with_start(
            retry_policy.clone(),
            format!("batch({topic}-{partition})"),
            enqueued_at,
        );

        let result: std::result::Result<(i64, i64), KrafkaError> = loop {
            // Get connection to leader
            let conn = match metadata
                .get_leader_connection(topic.as_ref(), partition)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    if e.is_retriable() {
                        debug!(
                            topic = %topic,
                            partition = partition,
                            error = %e,
                            "Batch connection error, refreshing metadata"
                        );
                        if let Err(refresh_err) = metadata
                            .refresh_for_topics_forced(Some(&[topic.as_ref()]))
                            .await
                        {
                            debug!(error = %refresh_err, "Metadata refresh failed during batch retry");
                        }
                    }
                    if let Some(backoff) = retry_ctx.record_failure(&e) {
                        metrics.record_retry();
                        retry_ctx.wait(backoff).await;
                        continue;
                    }
                    break Err(e);
                }
            };

            // KIP-219: honour broker throttle before dispatching the batch.
            //
            // `send_request_with_priority` also enforces the throttle for
            // normal-priority requests, but checking here avoids performing
            // API-version negotiation while the quota window is still open,
            // reducing wasted work per iteration.
            if let Some(delay) = conn.throttle_remaining() {
                debug!(
                    delay_ms = delay.as_millis() as u64,
                    topic = %topic,
                    partition = partition,
                    "Delaying batch send due to broker throttle (KIP-219)"
                );
                tokio::time::sleep(delay).await;
            }

            // Negotiate Produce version for this broker.
            let mut produce_version = match conn
                .negotiate_api_version(
                    ApiKey::Produce,
                    versions::PRODUCE_MAX,
                    versions::PRODUCE_MIN,
                )
                .await
            {
                Some(v) => v,
                None => {
                    let e = KrafkaError::protocol_kind(
                        ProtocolErrorKind::UnknownApiVersion,
                        "no mutually supported Produce API version",
                    );
                    debug!(
                        topic = %topic,
                        partition = partition,
                        "Produce version negotiation failed, refreshing metadata"
                    );
                    if let Err(refresh_err) = metadata
                        .refresh_for_topics_forced(Some(&[topic.as_ref()]))
                        .await
                    {
                        debug!(
                            error = %refresh_err,
                            "Metadata refresh failed during batch retry"
                        );
                    }
                    if let Some(backoff) = retry_ctx.record_failure(&e) {
                        metrics.record_retry();
                        retry_ctx.wait(backoff).await;
                        continue;
                    }
                    break Err(e);
                }
            };

            // KIP-516: Produce v13+ sends topic UUIDs instead of names.
            // Fill IDs from cache; fall back to v12 if any UUID is not yet known.
            if produce_version >= 13 && !super::fill_produce_topic_ids(&mut request, metadata) {
                produce_version = 12;
            }

            let encoded_body = match super::encode_and_validate_produce_request(
                &config.client_id,
                config.max_request_size,
                produce_version,
                &request,
            ) {
                Ok(b) => b,
                // A local frame-size rejection is `is_batch_too_large`, so it
                // flows into the split path below rather than failing the
                // records outright.
                Err(error) => break Err(error),
            };

            // acks=0 (fire-and-forget): Kafka sends no response (R6.1 fix)
            if config.acks == 0 {
                match conn
                    .send_fire_and_forget(ApiKey::Produce, produce_version, |buf| {
                        buf.put_slice(&encoded_body);
                        Ok(())
                    })
                    .await
                {
                    Ok(()) => {
                        retry_ctx.record_success();
                        break Ok((-1, -1));
                    }
                    Err(e) => {
                        if let Some(backoff) = retry_ctx.record_failure(&e) {
                            metrics.record_retry();
                            retry_ctx.wait(backoff).await;
                            continue;
                        }
                        break Err(e);
                    }
                }
            }

            let response_result = conn
                .send_request(ApiKey::Produce, produce_version, |buf| {
                    buf.put_slice(&encoded_body);
                    Ok(())
                })
                .await;

            match response_result {
                Ok(mut response_buf) => {
                    match ProduceResponse::decode_versioned(produce_version, &mut response_buf) {
                        Ok(produce_response) => {
                            // KIP-219: honour broker-reported throttle time.
                            conn.notify_throttle(produce_response.throttle_time_ms);

                            let pr = produce_response
                                .responses
                                .iter()
                                .find(|r| {
                                    // KIP-516: v13+ returns empty name and a topic_id.
                                    // For prior versions match by name.
                                    if produce_version >= 13 {
                                        r.topic_id.as_ref().is_some_and(|id| {
                                            metadata.topic_name_for_id(id).as_deref()
                                                == Some(topic.as_ref())
                                        })
                                    } else {
                                        r.name == topic.as_ref()
                                    }
                                })
                                .and_then(|r| {
                                    r.partition_responses.iter().find(|p| p.index == partition)
                                });

                            match pr {
                                Some(pr) if pr.error_code.is_ok() => {
                                    retry_ctx.record_success();
                                    break Ok((pr.base_offset, pr.log_append_time_ms));
                                }
                                // DuplicateSequenceNumber: the broker already
                                // committed this batch — idempotent dedup worked.
                                // Treat as success with unknown offset, matching
                                // the Kafka Java client's completeBatch() path.
                                Some(pr)
                                    if pr.error_code == ErrorCode::DuplicateSequenceNumber
                                        && config.identity.is_some() =>
                                {
                                    debug!(
                                        topic = %topic,
                                        partition = partition,
                                        "DuplicateSequenceNumber in batch — dedup confirmed"
                                    );
                                    retry_ctx.record_success();
                                    break Ok((-1, -1));
                                }
                                Some(pr) => {
                                    let err = KrafkaError::broker(
                                        pr.error_code,
                                        format!("batch produce failed for {topic}-{partition}"),
                                    );

                                    if pr.error_code == ErrorCode::UnknownProducerId
                                        && let (Some(identity), Some(current_sequence)) =
                                            (config.identity.as_ref(), sequence)
                                    {
                                        warn!(
                                            topic = %topic,
                                            partition = partition,
                                            "UnknownProducerId in batch, reinitializing idempotent producer state"
                                        );
                                        let new_sequence = match super::recover_unknown_producer_id(
                                            identity,
                                            metadata,
                                            retry_policy,
                                            topic.as_ref(),
                                            partition,
                                            current_sequence,
                                            record_count,
                                        )
                                        .await
                                        {
                                            Ok(new_sequence) => new_sequence,
                                            Err(recovery_error) => break Err(recovery_error),
                                        };
                                        sequence = Some(new_sequence);
                                        match Self::encode_batch_request(
                                            topic, partition, &pending, sequence, config,
                                        )
                                        .await
                                        {
                                            Ok((new_request, ..)) => request = new_request,
                                            Err(encode_err) => break Err(encode_err),
                                        }
                                    } else if pr.error_code == ErrorCode::OutOfOrderSequenceNumber
                                        && let (Some(identity), Some(base)) =
                                            (config.identity.as_ref(), sequence)
                                    {
                                        // `OUT_OF_ORDER_SEQUENCE_NUMBER`
                                        // normally means an *earlier* batch never
                                        // made it into the log (truncation,
                                        // unclean leader election). Rewinding and
                                        // resending would write this batch into
                                        // that hole and report success for a
                                        // stream that is silently missing data, so
                                        // a local reset is only permitted when
                                        // this batch is provably head-of-line.
                                        match identity.can_reset_after_out_of_order(
                                            topic.as_ref(),
                                            partition,
                                            base,
                                            record_count,
                                        ) {
                                            Ok(true) => {
                                                warn!(
                                                    topic = %topic,
                                                    partition = partition,
                                                    base_sequence = base,
                                                    "OutOfOrderSequenceNumber for head-of-line batch, \
                                                     resetting sequence and retrying"
                                                );
                                                let new_seq = match identity.reset_and_allocate(
                                                    topic.as_ref(),
                                                    partition,
                                                    record_count,
                                                ) {
                                                    Ok(s) => s,
                                                    Err(e) => break Err(e),
                                                };
                                                sequence = Some(new_seq);
                                                match Self::encode_batch_request(
                                                    topic, partition, &pending, sequence, config,
                                                )
                                                .await
                                                {
                                                    Ok((r, ..)) => request = r,
                                                    Err(encode_err) => break Err(encode_err),
                                                }
                                            }
                                            Ok(false) => {
                                                break Err(super::out_of_order_data_loss_error(
                                                    topic.as_ref(),
                                                    partition,
                                                    base,
                                                ));
                                            }
                                            Err(e) => break Err(e),
                                        }
                                    } else if err.is_retriable()
                                        // A broker that rejects the batch
                                        // because leadership moved names the
                                        // new leader (KIP-951); taking it here
                                        // makes the retry go to the right node
                                        // without a metadata round trip.
                                        && !super::apply_produce_leader_hint(
                                            metadata,
                                            topic.as_ref(),
                                            partition,
                                            &produce_response,
                                            pr,
                                        )
                                        && let Err(refresh_err) = metadata
                                            .refresh_for_topics_forced(Some(&[topic.as_ref()]))
                                            .await
                                    {
                                        debug!(error = %refresh_err, "Metadata refresh failed during batch retry");
                                    }
                                    if let Some(backoff) = retry_ctx.record_failure(&err) {
                                        metrics.record_retry();
                                        retry_ctx.wait(backoff).await;
                                        continue;
                                    }
                                    break Err(err);
                                }
                                None => {
                                    break Err(KrafkaError::protocol_kind(
                                        ProtocolErrorKind::Malformed,
                                        "partition not found in response",
                                    ));
                                }
                            }
                        }
                        Err(e) => {
                            if let Some(backoff) = retry_ctx.record_failure(&e) {
                                metrics.record_retry();
                                retry_ctx.wait(backoff).await;
                                continue;
                            }
                            break Err(e);
                        }
                    }
                }
                Err(e) => {
                    if e.is_retriable() {
                        debug!(
                            topic = %topic,
                            partition = partition,
                            error = %e,
                            "Batch send error, refreshing metadata"
                        );
                        if let Err(refresh_err) = metadata
                            .refresh_for_topics_forced(Some(&[topic.as_ref()]))
                            .await
                        {
                            debug!(error = %refresh_err, "Metadata refresh failed during batch retry");
                        }
                    }
                    if let Some(backoff) = retry_ctx.record_failure(&e) {
                        metrics.record_retry();
                        retry_ctx.wait(backoff).await;
                        continue;
                    }
                    break Err(e);
                }
            }
        };

        // Complete pending records
        match result {
            Ok((base_offset, timestamp)) => {
                // Acknowledge the last sequence in the batch (base + count - 1),
                // matching Kafka Java client's batch.lastSequence() semantics.
                // This ensures reset_sequence() computes the correct next value
                // for multi-record batches after OOSN recovery.
                if let (Some(identity), Some(seq)) = (&config.identity, sequence)
                    && let Ok(last_seq) =
                        super::idempotent::last_sequence_of_batch(seq, record_count)
                {
                    identity.acknowledge(topic.as_ref(), partition, last_seq);

                    // Fire-and-forget snapshot persistence.
                    if let Some(ref store) = config.state_store {
                        let snapshot = identity.snapshot();
                        let store = Arc::clone(store);
                        tokio::spawn(async move {
                            if let Err(err) = store.store_erased(&snapshot).await {
                                tracing::warn!(error = %err, "Failed to persist producer state snapshot");
                            }
                        });
                    }
                }

                let batch_bytes_total: u64 = pending.iter().map(|p| p.estimated_size as u64).sum();
                metrics.record_batch_for_topic(
                    topic.as_ref(),
                    pending.len() as u64,
                    batch_bytes_total,
                );
                let topic_owned = topic.to_string();
                for p in pending {
                    let meta = RecordMetadata {
                        topic: topic_owned.clone(),
                        partition,
                        offset: if base_offset >= 0 {
                            base_offset + p.offset_in_batch
                        } else {
                            -1
                        },
                        timestamp,
                        delivery: if base_offset >= 0 {
                            DeliveryConfirmation::Offset
                        } else if config.acks == 0 {
                            DeliveryConfirmation::Unacknowledged
                        } else {
                            DeliveryConfirmation::Deduplicated
                        },
                    };
                    crate::interceptor::safe_on_acknowledgement(&*config.interceptor, &meta, None);
                    let _ = p.response_tx.send(AppendResponse::Done(Ok(meta)));
                }
            }
            Err(e) => {
                // Return the unused range to the partition counter, but
                // only when this batch still owns the tail. Never rewind while
                // newer allocations exist — that would hand the same sequence
                // numbers to two batches.
                let sequence_space_intact =
                    if let (Some(identity), Some(base)) = (config.identity.as_ref(), sequence) {
                        Self::release_failed_sequence_range(
                            identity,
                            topic,
                            partition,
                            base,
                            record_count,
                        )
                    } else {
                        true
                    };

                // The broker (or our own frame-size guard) rejected the
                // batch purely on size. Halve it and resubmit both halves in
                // order, each with a freshly derived sequence range. Requires
                // an intact sequence space, since the halves re-allocate from
                // the position the failed batch just released.
                if sequence_space_intact
                    && is_batch_too_large(&e)
                    && pending.len() > 1
                    && split_depth < MAX_BATCH_SPLIT_DEPTH
                {
                    let mid = pending.len() / 2;
                    let second_half = pending.split_off(mid);
                    warn!(
                        topic = %topic,
                        partition = partition,
                        records = pending.len() + second_half.len(),
                        error = %e,
                        "Batch rejected as too large; splitting and resubmitting both halves"
                    );
                    metrics.record_retry();
                    for half in [pending, second_half] {
                        Self::produce_pending(
                            topic,
                            partition,
                            half,
                            enqueued_at,
                            metadata,
                            config,
                            retry_policy,
                            metrics,
                            split_depth + 1,
                        )
                        .await;
                    }
                    return;
                }

                metrics.record_error_for_topic(topic.as_ref());
                let topic_owned = topic.to_string();
                for p in pending {
                    let meta = RecordMetadata {
                        topic: topic_owned.clone(),
                        partition,
                        offset: -1,
                        timestamp: 0,
                        delivery: DeliveryConfirmation::Failed,
                    };
                    crate::interceptor::safe_on_acknowledgement(
                        &*config.interceptor,
                        &meta,
                        Some(&e),
                    );
                    let _ = p.response_tx.send(AppendResponse::Done(Err(e.clone())));
                }
            }
        }
    }

    /// Return an unused sequence range to the partition counter after a batch
    /// failed without being committed.
    ///
    /// Returns `true` when the range was rewound and the partition's sequence
    /// space is still coherent. Returns `false` when the batch no longer owned
    /// the tail: in that case nothing is rewound, the partition's sequences are
    /// dropped, and a producer-ID re-initialisation is requested so the next
    /// send starts from a clean, broker-agreed state.
    fn release_failed_sequence_range(
        identity: &super::idempotent::ProducerIdentity,
        topic: &TopicHandle,
        partition: PartitionId,
        base_sequence: i32,
        record_count: i32,
    ) -> bool {
        match identity.rollback_sequence_range(
            topic.as_ref(),
            partition,
            base_sequence,
            record_count,
        ) {
            Ok(super::idempotent::RollbackOutcome::RolledBack) => true,
            Ok(super::idempotent::RollbackOutcome::NotTail) | Err(_) => {
                warn!(
                    topic = %topic,
                    partition = partition,
                    base_sequence,
                    "Failed batch no longer owns the tail of its sequence range; \
                     resetting partition sequences and requesting a producer-ID re-init \
                     instead of rewinding into a newer allocation"
                );
                identity.reset_partition_sequences(topic.as_ref(), partition);
                identity.request_reinit();
                false
            }
        }
    }

    /// Flush all batches, respecting the global send-task cap.
    ///
    /// Routes through `spawn_batches_bounded` so that a user-triggered
    /// `flush()` or `close()` with many partitions does not create an
    /// unbounded task burst — the same `MAX_CONCURRENT_BATCH_SENDS` ceiling
    /// that governs linger-triggered sends applies here too.
    ///
    /// Always returns `Ok(())`: individual send errors are delivered through
    /// each record's `response_tx` inside `send_extracted_batch`; there is
    /// no aggregate failure to surface at this level.
    async fn flush_all(&mut self) -> Result<()> {
        let keys: Vec<_> = self
            .batches
            .iter()
            .filter(|(_, batch)| !batch.is_empty())
            .map(|(key, _)| key.clone())
            .collect();

        let mut extracted = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(item) = self.extract_batch(&key) {
                extracted.push((key, item));
            }
        }

        Self::spawn_batches_bounded(
            extracted,
            &self.metadata,
            &self.config,
            &self.retry_policy,
            &self.metrics,
            self.send_semaphore.clone(),
        )
        .await;
        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_accumulator_config_default() {
        let config = AccumulatorConfig::default();
        assert_eq!(config.batch_size, 16384);
        assert_eq!(config.linger, Duration::ZERO);
        assert_eq!(config.acks, -1);
    }

    #[test]
    fn test_accumulator_batch_age() {
        let batch = AccumulatorBatch::new(16384);
        std::thread::sleep(Duration::from_millis(10));
        assert!(batch.age() >= Duration::from_millis(10));
    }

    #[test]
    fn test_accumulator_batch_new() {
        let batch = AccumulatorBatch::new(32768);
        assert!(batch.is_empty());
        assert!(batch.pending.is_empty());
    }

    #[test]
    fn test_accumulator_config_custom() {
        let config = AccumulatorConfig {
            batch_size: 65536,
            linger: Duration::from_millis(50),
            compression: Compression::Snappy,
            acks: 1,
            client_id: "test-client".to_string(),
            request_timeout: Duration::from_secs(10),
            max_request_size: 131072,
            buffer_memory: 64 * 1024 * 1024,
            max_block_ms: Duration::from_secs(30),
            in_flight_semaphore: Arc::new(Semaphore::new(5)),
            interceptor: Arc::new(crate::interceptor::NoOpProducerInterceptor),
            identity: None,
            partitioner: Arc::new(crate::producer::partitioner::DefaultPartitioner::new()),
            state_store: None,
            topic_compression: AHashMap::new(),
            transactional_id: None,
        };
        assert_eq!(config.batch_size, 65536);
        assert_eq!(config.linger, Duration::from_millis(50));
        assert_eq!(config.acks, 1);
        assert_eq!(config.client_id, "test-client");
        assert_eq!(config.max_request_size, 131072);
        assert_eq!(config.buffer_memory, 64 * 1024 * 1024);
    }

    #[test]
    fn test_estimate_record_size() {
        let record = ProducerRecord::new("test-topic", b"value".to_vec());
        let size = record.estimated_size();
        // Should be at least the value length + topic overhead
        assert!(size >= 5);
        assert!(size > 64); // overhead for topic name and struct

        // Record with key and headers should be larger
        let record_with_key =
            ProducerRecord::new("test-topic", b"value".to_vec()).with_key(b"key".to_vec());
        let size_with_key = record_with_key.estimated_size();
        assert!(size_with_key > size);
    }

    /// Verify linger=0 config results in immediate flush semantics.
    #[test]
    fn test_linger_zero_check_interval() {
        // With linger=0, the check interval should be 1ms (minimum)
        let linger = Duration::ZERO;
        let check_interval = Duration::from_millis(1).max(linger / 10);
        assert_eq!(check_interval, Duration::from_millis(1));
    }

    /// Verify `check_linger_expiry` calls `flush_all_ready` when linger=0.
    #[test]
    fn test_linger_zero_is_zero() {
        let config = AccumulatorConfig {
            linger: Duration::ZERO,
            ..Default::default()
        };
        assert!(config.linger.is_zero());
    }

    /// Verify `send_extracted_batch` is `'static + Send`.
    ///
    /// All detached flush paths (`spawn_batches_detached`, `flush_batch`,
    /// Compile-time assertion that the handle types used with `tokio::spawn`
    /// are `Send + Sync`. A regression here would prevent spawning the
    /// accumulator task on the multi-thread runtime.
    #[test]
    fn test_accumulator_handle_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RecordAccumulatorHandle>();
        assert_send_sync::<RecordAccumulator>();
    }

    // ── Backpressure tests ──────────────────────────────────────

    /// A zero-capacity semaphore forces `acquire_many(record_size)` to
    /// block until `max_block_ms` expires, which is the backpressure
    /// timeout path we want to exercise.
    #[tokio::test]
    async fn test_backpressure_timeout_returns_timeout_error() {
        let (sender, _receiver) = mpsc::channel::<AccumulatorMessage>(16);
        let handle = RecordAccumulatorHandle {
            sender,
            memory_permits: Arc::new(Semaphore::new(0)),
            memory_capacity: 1024 * 1024, // larger than any test record
            max_request_size: 0,
            max_block_ms: Duration::from_millis(50),
            in_flight_barrier: Arc::new(InFlightBarrier::new()),
            buffered_records: Arc::new(AtomicUsize::new(0)),
            metrics: Arc::new(ProducerMetrics::default()),
        };

        let record = ProducerRecord::new("topic", b"value".to_vec());
        let result = handle.append(record, 0).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        let err_msg = err.to_string();
        assert!(
            err_msg.contains("max_block"),
            "expected max_block in error, got: {err_msg}"
        );
        assert!(
            matches!(err, KrafkaError::Timeout { .. }),
            "expected Timeout variant, got: {err:?}"
        );
    }

    /// `Semaphore::add_permits` immediately wakes the front FIFO waiter,
    /// which is the mechanism that replaces `Notify::notify_waiters()`
    /// for backpressure release.
    #[tokio::test]
    async fn test_backpressure_unblocks_on_permit_release() {
        let sem = Arc::new(Semaphore::new(0));
        let s = sem.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            s.add_permits(128);
        });
        let result = tokio::time::timeout(Duration::from_secs(2), sem.acquire_many(64)).await;
        assert!(result.is_ok(), "acquire_many should have completed");
        assert!(
            result.unwrap().is_ok(),
            "acquire_many should have succeeded"
        );
    }

    /// Records larger than `memory_capacity` must be rejected immediately
    /// rather than blocking forever on `acquire_many` (which would never
    /// succeed against a semaphore that cannot hold that many permits).
    #[tokio::test]
    async fn test_oversize_record_rejected_immediately() {
        let (sender, _receiver) = mpsc::channel::<AccumulatorMessage>(16);
        let handle = RecordAccumulatorHandle {
            sender,
            memory_permits: Arc::new(Semaphore::new(16)),
            memory_capacity: 16, // deliberately tiny
            max_request_size: 0,
            max_block_ms: Duration::from_secs(60),
            in_flight_barrier: Arc::new(InFlightBarrier::new()),
            buffered_records: Arc::new(AtomicUsize::new(0)),
            metrics: Arc::new(ProducerMetrics::default()),
        };

        let record = ProducerRecord::new("topic", vec![0u8; 1024]);
        let start = std::time::Instant::now();
        let result = handle.append(record, 0).await;
        // Must return synchronously without waiting for max_block_ms.
        assert!(start.elapsed() < Duration::from_secs(1));
        let err = result.expect_err("oversize record must be rejected");
        assert!(
            err.to_string().contains("buffer_memory"),
            "expected buffer_memory error, got: {err}"
        );
    }

    /// A closed semaphore (panic recovery path) must propagate to
    /// in-flight `acquire_many` calls as an error, not hang them.
    #[tokio::test]
    async fn test_closed_semaphore_unblocks_waiters() {
        let (sender, _receiver) = mpsc::channel::<AccumulatorMessage>(16);
        let sem = Arc::new(Semaphore::new(0));
        let handle = RecordAccumulatorHandle {
            sender,
            memory_permits: sem.clone(),
            memory_capacity: 1024 * 1024,
            max_request_size: 0,
            max_block_ms: Duration::from_secs(60),
            in_flight_barrier: Arc::new(InFlightBarrier::new()),
            buffered_records: Arc::new(AtomicUsize::new(0)),
            metrics: Arc::new(ProducerMetrics::default()),
        };

        let sem_close = sem.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            sem_close.close();
        });

        let record = ProducerRecord::new("topic", b"value".to_vec());
        let start = std::time::Instant::now();
        let result = handle.append(record, 0).await;
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "must unblock on close, not on max_block timeout"
        );
        let err = result.expect_err("closed semaphore must surface as error");
        assert!(
            matches!(err, KrafkaError::InvalidState { .. }),
            "expected InvalidState variant, got: {err:?}"
        );
    }

    /// Regression: if the `AccumulatorMessage::Append` is dropped before
    /// the accumulator hands the permits off to an `InFlightGuard` (task
    /// panic mid-handle, receiver dropped during shutdown, etc.), the
    /// RAII `PermitReservation` must release the permits back to the
    /// pool. A leak here would permanently reduce `buffer_memory`.
    #[tokio::test]
    async fn test_permits_released_when_append_message_dropped() {
        let (sender, mut receiver) = mpsc::channel::<AccumulatorMessage>(16);
        let sem = Arc::new(Semaphore::new(1024));
        let metrics = Arc::new(ProducerMetrics::default());
        let buffered_records = Arc::new(AtomicUsize::new(0));
        let handle = RecordAccumulatorHandle {
            sender,
            memory_permits: sem.clone(),
            memory_capacity: 1024,
            max_request_size: 0,
            max_block_ms: Duration::from_millis(500),
            in_flight_barrier: Arc::new(InFlightBarrier::new()),
            buffered_records: buffered_records.clone(),
            metrics: metrics.clone(),
        };

        let record = ProducerRecord::new("topic", vec![0u8; 256]);
        let append_fut = tokio::spawn(async move { handle.append(record, 0).await });

        // Receive the Append message and immediately drop it without responding.
        // Dropping the message triggers `PermitReservation::drop`, which returns
        // the permits to the semaphore. This is deterministic — no sleep or
        // timer needed, and the test is fully reproducible under load.
        let msg = tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("timed out waiting for Append message to arrive in channel")
            .expect("channel closed before message arrived");

        assert_eq!(metrics.buffered_records.get(), 1);
        assert_eq!(buffered_records.load(Ordering::Relaxed), 1);

        drop(msg);
        drop(receiver);

        // The response_tx inside the message was dropped above; response_rx
        // returns RecvError, which surfaces as an InvalidState error.
        let _ = append_fut.await;

        // All 1024 permits must be available again — no leak.
        assert_eq!(
            sem.available_permits(),
            1024,
            "permits leaked when the Append message was dropped"
        );
        assert_eq!(metrics.buffered_records.get(), 0);
        assert_eq!(buffered_records.load(Ordering::Relaxed), 0);
    }

    /// `check_record_admission` rejects records that exceed `buffer_memory`.
    ///
    /// Tests the `buffer_memory` branch independently via the extracted
    /// helper so both admission failure modes are regression-proof without
    /// needing to allocate large buffers.
    #[test]
    fn test_check_record_admission_rejects_oversized_for_buffer() {
        let err = check_record_admission(1024, 16, 0).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("buffer_memory"),
            "error must cite buffer_memory, got: {msg}"
        );
        assert!(
            !msg.contains("u32::MAX"),
            "must not cite u32::MAX for a buffer_memory rejection, got: {msg}"
        );
    }

    /// `check_record_admission` rejects records that exceed the effective
    /// semaphore permit-count limit.
    ///
    /// Tests the semaphore-limit branch directly via the extracted helper —
    /// no large allocation needed.
    #[test]
    fn test_check_record_admission_rejects_oversized_for_semaphore_limit() {
        let oversized = max_record_semaphore_permits() + 1;
        let err = check_record_admission(oversized, usize::MAX, 0).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("Semaphore::MAX_PERMITS"),
            "error must cite the effective semaphore limit, got: {msg}"
        );
        assert!(
            !msg.contains("buffer_memory"),
            "must not cite buffer_memory for a semaphore-limit rejection, got: {msg}"
        );
    }

    #[test]
    fn test_buffered_record_guard_updates_metric() {
        let metrics = Arc::new(ProducerMetrics::default());
        let buffered_records = Arc::new(AtomicUsize::new(0));

        {
            let _guard = BufferedRecordGuard::new(buffered_records.clone(), metrics.clone());
            assert_eq!(metrics.buffered_records.get(), 1);
        }

        assert_eq!(metrics.buffered_records.get(), 0);
    }

    /// `effective_memory_capacity(0)` defaults to `Semaphore::MAX_PERMITS`.
    #[test]
    fn test_effective_memory_capacity_zero_returns_max() {
        assert_eq!(effective_memory_capacity(0), Semaphore::MAX_PERMITS);
    }

    /// `effective_memory_capacity` clamps values above `Semaphore::MAX_PERMITS`.
    ///
    /// This exercises the warning-and-clamp branch that prevents an illegal
    /// semaphore permit count from being passed to `Semaphore::new`.
    #[test]
    fn test_effective_memory_capacity_clamps_over_limit() {
        let over = Semaphore::MAX_PERMITS + 1;
        assert_eq!(effective_memory_capacity(over), Semaphore::MAX_PERMITS);
    }

    /// Values within the valid range are returned unchanged.
    #[test]
    fn test_effective_memory_capacity_passthrough() {
        let within = Semaphore::MAX_PERMITS / 2;
        assert_eq!(effective_memory_capacity(within), within);
    }

    // ── Per-partition in-flight FIFO ──────────────────────────────

    /// Batches for the same partition must take their turn in the exact order
    /// the accumulator sealed them.
    ///
    /// The tasks are spawned in **reverse** ticket order and the later tickets
    /// are given a head start, so any implementation that lets tasks race onto
    /// the wire produces a descending (or interleaved) order. Only a real FIFO
    /// can yield `0, 1, 2, …`.
    #[tokio::test]
    async fn test_same_partition_batches_dispatch_in_seal_order() {
        let slot = Arc::new(PartitionInFlight::default());
        let tickets: Vec<PartitionTicket> = (0..8).map(|_| slot.take_ticket()).collect();
        let order = Arc::new(parking_lot::Mutex::new(Vec::new()));

        let mut handles = Vec::new();
        for (i, ticket) in tickets.into_iter().enumerate().rev() {
            let order = order.clone();
            handles.push(tokio::spawn(async move {
                let turn = ticket.acquire().await;
                order.lock().push(i);
                // Hold the slot so an unsynchronised implementation would
                // visibly overlap here.
                tokio::time::sleep(Duration::from_millis(2)).await;
                drop(turn);
            }));
            // Give the just-spawned (later) ticket a head start.
            tokio::task::yield_now().await;
        }
        for h in handles {
            h.await.expect("task panicked");
        }

        assert_eq!(
            *order.lock(),
            (0..8).collect::<Vec<usize>>(),
            "same-partition batches must dispatch in seal order"
        );
    }

    /// With the FIFO in place, sequence allocation performed under the turn is
    /// strictly monotonic and gapless — which is exactly the invariant the
    /// broker checks.
    #[tokio::test]
    async fn test_sequences_are_monotonic_and_gapless_under_partition_fifo() {
        use super::super::idempotent::ProducerIdentity;

        const BATCHES: usize = 16;
        const RECORDS_PER_BATCH: i32 = 3;

        let identity = Arc::new(ProducerIdentity::new());
        identity.initialize(1, 0);
        let slot = Arc::new(PartitionInFlight::default());
        let tickets: Vec<PartitionTicket> = (0..BATCHES).map(|_| slot.take_ticket()).collect();
        let observed = Arc::new(parking_lot::Mutex::new(Vec::new()));

        let mut handles = Vec::new();
        for ticket in tickets.into_iter().rev() {
            let identity = identity.clone();
            let observed = observed.clone();
            handles.push(tokio::spawn(async move {
                let _turn = ticket.acquire().await;
                let base = identity
                    .allocate_sequence("t", 0, RECORDS_PER_BATCH)
                    .expect("allocate");
                observed.lock().push(base);
            }));
            tokio::task::yield_now().await;
        }
        for h in handles {
            h.await.expect("task panicked");
        }

        let expected: Vec<i32> = (0..BATCHES as i32).map(|i| i * RECORDS_PER_BATCH).collect();
        assert_eq!(
            *observed.lock(),
            expected,
            "sequence allocation order must follow dispatch order with no gaps"
        );
    }

    /// Serialization is per-partition only: two different partitions must be
    /// able to hold their turns simultaneously.
    #[tokio::test]
    async fn test_different_partitions_are_not_serialized() {
        let a = Arc::new(PartitionInFlight::default());
        let b = Arc::new(PartitionInFlight::default());
        let turn_a = a.take_ticket().acquire().await;

        // Partition B must not be blocked by partition A's held turn.
        let turn_b = tokio::time::timeout(Duration::from_millis(500), b.take_ticket().acquire())
            .await
            .expect("cross-partition sends must not serialize");

        drop(turn_a);
        drop(turn_b);
    }

    /// A ticket abandoned without being acquired (drain wave aborted during
    /// shutdown) must still release its place, or every later batch for that
    /// partition would wait forever.
    #[tokio::test]
    async fn test_dropped_ticket_does_not_stall_the_partition() {
        let slot = Arc::new(PartitionInFlight::default());
        let abandoned = slot.take_ticket();
        let next = slot.take_ticket();

        drop(abandoned);

        let turn = tokio::time::timeout(Duration::from_millis(500), next.acquire())
            .await
            .expect("a dropped ticket must not stall the partition FIFO");
        drop(turn);
        assert!(slot.is_idle());
    }

    /// A completed partition reports idle so it can be pruned from the map.
    #[tokio::test]
    async fn test_partition_inflight_idle_tracking() {
        let slot = Arc::new(PartitionInFlight::default());
        assert!(slot.is_idle());

        let ticket = slot.take_ticket();
        assert!(!slot.is_idle(), "an outstanding ticket means not idle");

        let turn = ticket.acquire().await;
        assert!(!slot.is_idle());
        drop(turn);
        assert!(slot.is_idle());
    }

    // ── Oversized-batch splitting ─────────────────────────────────

    /// Both broker size rejections and the local frame-size guard must be
    /// recognised as splittable.
    #[test]
    fn test_is_batch_too_large_classification() {
        assert!(is_batch_too_large(&KrafkaError::broker(
            ErrorCode::MessageTooLarge,
            "too big"
        )));
        assert!(is_batch_too_large(&KrafkaError::broker(
            ErrorCode::RecordListTooLarge,
            "too big"
        )));
        assert!(is_batch_too_large(&KrafkaError::protocol_kind(
            ProtocolErrorKind::InvalidLength,
            "produce request size 2000000 exceeds max_request_size 1000000"
        )));
    }

    /// Unrelated failures must never trigger a split — halving a batch that
    /// failed for a transient reason would double the request count for no
    /// benefit and reshuffle sequence ranges.
    #[test]
    fn test_is_batch_too_large_ignores_unrelated_errors() {
        assert!(!is_batch_too_large(&KrafkaError::broker(
            ErrorCode::NotLeaderForPartition,
            "leader moved"
        )));
        assert!(!is_batch_too_large(&KrafkaError::broker(
            ErrorCode::OutOfOrderSequenceNumber,
            "oosn"
        )));
        assert!(!is_batch_too_large(&KrafkaError::timeout("produce")));
        assert!(!is_batch_too_large(&KrafkaError::protocol_kind(
            ProtocolErrorKind::Malformed,
            "bad response"
        )));
    }

    /// The recursion is bounded and every record ends up in exactly one leaf
    /// batch — no record is dropped or duplicated by splitting.
    #[test]
    fn test_batch_split_is_bounded_and_lossless() {
        fn split(records: usize, depth: u8, leaves: &mut Vec<usize>) {
            if records > 1 && depth < MAX_BATCH_SPLIT_DEPTH {
                let mid = records / 2;
                split(mid, depth + 1, leaves);
                split(records - mid, depth + 1, leaves);
            } else {
                leaves.push(records);
            }
        }

        let mut leaves = Vec::new();
        split(100, 0, &mut leaves);
        assert_eq!(leaves.iter().sum::<usize>(), 100, "no records lost");
        assert!(
            leaves.len() <= 1 << MAX_BATCH_SPLIT_DEPTH,
            "recursion must stay bounded, got {} leaves",
            leaves.len()
        );

        // A single-record batch cannot be split any further.
        let mut single = Vec::new();
        split(1, 0, &mut single);
        assert_eq!(single, vec![1]);
    }

    /// Only the expensive codecs are moved to the blocking pool.
    #[test]
    fn test_cpu_heavy_compression_selection() {
        assert!(is_cpu_heavy_compression(Compression::Gzip));
        assert!(is_cpu_heavy_compression(Compression::Zstd));
        assert!(!is_cpu_heavy_compression(Compression::None));
        assert!(!is_cpu_heavy_compression(Compression::Snappy));
        assert!(!is_cpu_heavy_compression(Compression::Lz4));
    }
}
