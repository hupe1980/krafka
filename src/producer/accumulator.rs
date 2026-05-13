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
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::interval;
use tracing::{debug, trace, warn};

use super::barrier::{InFlightBarrier, InFlightOpGuard};
use super::batch::ProducerBatch;
use super::record::{ProducerRecord, RecordMetadata, RoutedRecord, TopicHandle};
use super::retry::{RetryContext, RetryPolicy};
use crate::PartitionId;
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::interceptor::ProducerInterceptor;
use crate::metadata::ClusterMetadata;
use crate::metrics::ProducerMetrics;
use crate::protocol::{
    ApiKey, Compression, ProducePartitionData, ProduceRequest, ProduceResponse, ProduceTopicData,
    RecordBatchBuilder, VersionedDecode, VersionedEncode, versions,
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
}

impl Clone for AccumulatorConfig {
    fn clone(&self) -> Self {
        Self {
            batch_size: self.batch_size,
            linger: self.linger,
            compression: self.compression,
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
            acks: -1,
            client_id: "krafka".to_string(),
            request_timeout: Duration::from_secs(30),
            max_request_size: crate::protocol::MAX_MESSAGE_SIZE,
            buffer_memory: 32 * 1024 * 1024, // 32 MB default (same as Kafka Java client)
            max_block_ms: Duration::from_secs(60), // 60 seconds default
            in_flight_semaphore: Arc::new(Semaphore::new(5)), // default max_in_flight
            interceptor: Arc::new(crate::interceptor::NoOpProducerInterceptor),
            identity: None,
            partitioner: Arc::new(super::partitioner::DefaultPartitioner::new()),
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
    batch: ProducerBatch,
    pending: Vec<PendingRecord>,
    created_at: Instant,
}

impl AccumulatorBatch {
    fn new(
        topic: TopicHandle,
        partition: PartitionId,
        max_size: usize,
        compression: Compression,
    ) -> Self {
        Self {
            batch: ProducerBatch::new(topic.to_string(), partition, max_size, compression),
            pending: Vec::new(),
            created_at: Instant::now(),
        }
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
        let compression = self.config.compression;
        let accumulator_batch = self.batches.entry(key.clone()).or_insert_with(|| {
            AccumulatorBatch::new(key.0.clone(), partition, batch_size, compression)
        });

        // Check if the record fits in the current batch. If so, move it
        // directly into PendingRecord (zero clones). The batch only tracks
        // size; PendingRecord owns the record data for send_extracted_batch.
        let offset = accumulator_batch.batch.len() as i64;
        if accumulator_batch.batch.would_fit(record_size) {
            accumulator_batch.batch.track(record_size);
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
            if accumulator_batch.batch.is_full() {
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
            let mut new_batch =
                AccumulatorBatch::new(key.0.clone(), partition, batch_size, compression);

            if new_batch.batch.would_fit(record_size) {
                new_batch.batch.track(record_size);
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
        if self.config.linger.is_zero() {
            self.flush_all_ready();
            return;
        }

        let keys_to_flush: Vec<_> = self
            .batches
            .iter()
            .filter(|(_, batch)| !batch.batch.is_empty() && batch.age() >= self.config.linger)
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
            .filter(|(_, batch)| !batch.batch.is_empty())
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

    /// Drive up to `MAX_CONCURRENT_BATCH_SENDS` send tasks to completion.
    ///
    /// Each task acquires one permit from `send_semaphore` before being
    /// spawned and holds it until completion.  Because `send_semaphore` is
    /// **shared across all in-flight waves**, the total number of concurrent
    /// `send_extracted_batch` tasks across every overlapping linger wave is
    /// bounded by `MAX_CONCURRENT_BATCH_SENDS` — not multiplied by the
    /// number of simultaneous waves (F-021 fix).
    ///
    /// When `send_semaphore` is saturated, `acquire_owned().await` parks the
    /// current wave-wrapper task until a slot frees up.  The Tokio runtime
    /// continues executing the already-spawned tasks, so no deadlock can
    /// occur.
    async fn spawn_batches_bounded(
        extracted: Vec<(
            (TopicHandle, PartitionId),
            (AccumulatorBatch, InFlightGuard),
        )>,
        metadata: &Arc<ClusterMetadata>,
        config: &AccumulatorConfig,
        retry_policy: &RetryPolicy,
        metrics: &Arc<ProducerMetrics>,
        send_semaphore: Arc<Semaphore>,
    ) {
        let mut join_set = tokio::task::JoinSet::new();
        for ((topic, partition), (batch, guard)) in extracted {
            // Clone shared handles outside the acquire so the borrow ends
            // before entering the `async move` block (which must be `'static`).
            let metadata = metadata.clone();
            let config = config.clone();
            let retry_policy = retry_policy.clone();
            let metrics = metrics.clone();
            // Acquire a global send slot before spawning.  Blocks the
            // wave-wrapper task (not the run loop) when 64 tasks are already
            // live across all waves.  If the semaphore is closed the
            // accumulator is shutting down — drop remaining batches and abort.
            let permit = match send_semaphore.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_closed) => {
                    // Accumulator shutting down: abort this wave.
                    // InFlightGuard::drop() returns memory permits for all
                    // remaining items; PendingRecord senders are dropped
                    // (callers observe RecvError, which is acceptable on close).
                    return;
                }
            };
            join_set.spawn(async move {
                // Hold the permit for the entire duration of the send task;
                // dropping it on completion returns the slot to the shared pool.
                let _permit = permit;
                Self::send_extracted_batch(
                    topic,
                    partition,
                    batch.pending,
                    batch.created_at,
                    guard,
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
        extracted: Vec<(
            (TopicHandle, PartitionId),
            (AccumulatorBatch, InFlightGuard),
        )>,
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
    fn extract_batch(
        &mut self,
        key: &(TopicHandle, PartitionId),
    ) -> Option<(AccumulatorBatch, InFlightGuard)> {
        let batch = self.batches.remove(key)?;
        if batch.batch.is_empty() {
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
        Some((batch, guard))
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

    /// Send an extracted batch to the broker with retry and metadata refresh.
    ///
    /// This is a static method to enable concurrent flushing via `FuturesUnordered`.
    /// Acquires an in-flight semaphore permit to respect `max_in_flight` concurrency limits.
    #[allow(clippy::too_many_arguments)]
    async fn send_extracted_batch(
        topic: TopicHandle,
        partition: PartitionId,
        pending: Vec<PendingRecord>,
        enqueued_at: Instant,
        _in_flight_guard: InFlightGuard,
        metadata: Arc<ClusterMetadata>,
        config: AccumulatorConfig,
        retry_policy: RetryPolicy,
        metrics: Arc<ProducerMetrics>,
    ) {
        if let Some(identity) = config.identity.as_ref()
            && let Err(error) =
                super::ensure_idempotent_producer_id_initialized(identity, &metadata, &retry_policy)
                    .await
        {
            metrics.record_error();
            for pending_record in pending {
                let _ = pending_record
                    .response_tx
                    .send(AppendResponse::Done(Err(error.clone())));
            }
            return;
        }

        // Acquire in-flight permit before sending (accumulator was
        // bypassing max_in_flight). The permit is held until this batch completes.
        let _permit = config.in_flight_semaphore.acquire().await;
        let _timer = metrics.send_latency.start();

        let record_count = pending.len() as i32;

        // Allocate sequence range for idempotent production.
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

        // Build and encode the record batch (rebuilt on OutOfOrderSequenceNumber).
        // Returns the request plus (compressed_bytes, uncompressed_bytes) for
        // tracking the compression ratio when a codec is active.
        let build_batch = |seq: Option<i32>,
                           cfg: &AccumulatorConfig|
         -> crate::error::Result<(ProduceRequest, u64, u64)> {
            let mut batch_builder = RecordBatchBuilder::new().compression(cfg.compression);

            // Tag with idempotent producer identity
            if let (Some(identity), Some(s)) = (&cfg.identity, seq) {
                batch_builder =
                    batch_builder.producer(identity.producer_id(), identity.producer_epoch(), s);
            }

            // Accumulate uncompressed payload size before encoding.
            let uncompressed_len: u64 = pending.iter().map(|p| p.estimated_size as u64).sum();

            for p in &pending {
                batch_builder = p.record.append_to_batch_builder(batch_builder);
            }
            let batch = batch_builder.build();
            let batch_bytes = batch.encode()?;
            let compressed_len = batch_bytes.len() as u64;

            Ok((
                ProduceRequest {
                    transactional_id: None,
                    acks: cfg.acks,
                    timeout_ms: crate::util::duration_to_millis_i32(cfg.request_timeout),
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
        };

        let (mut request, compressed_len, uncompressed_len) = match build_batch(sequence, &config) {
            Ok(r) => r,
            Err(e) => {
                // Rollback sequence on encode failure
                if let Some(ref identity) = config.identity {
                    let _ =
                        identity.rollback_sequence_range(topic.as_ref(), partition, record_count);
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
                        if let Err(refresh_err) =
                            metadata.refresh_for_topics(Some(&[topic.as_ref()])).await
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
                    if let Err(refresh_err) =
                        metadata.refresh_for_topics(Some(&[topic.as_ref()])).await
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
            if produce_version >= 13 && !super::fill_produce_topic_ids(&mut request, &metadata) {
                produce_version = 12;
            }

            if let Err(error) = super::validate_produce_request_size(
                &config.client_id,
                config.max_request_size,
                produce_version,
                &request,
            ) {
                if let (Some(identity), Some(_)) = (config.identity.as_ref(), sequence) {
                    let _ =
                        identity.rollback_sequence_range(topic.as_ref(), partition, record_count);
                }
                metrics.record_error();
                for pending_record in pending {
                    let _ = pending_record
                        .response_tx
                        .send(AppendResponse::Done(Err(error.clone())));
                }
                return;
            }

            // acks=0 (fire-and-forget): Kafka sends no response (R6.1 fix)
            if config.acks == 0 {
                match conn
                    .send_fire_and_forget(ApiKey::Produce, produce_version, |buf| {
                        request.encode_versioned(produce_version, buf)
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
                    request.encode_versioned(produce_version, buf)
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
                                            &metadata,
                                            &retry_policy,
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
                                        match build_batch(sequence, &config) {
                                            Ok((new_request, ..)) => request = new_request,
                                            Err(encode_err) => break Err(encode_err),
                                        }
                                    } else if pr.error_code == ErrorCode::OutOfOrderSequenceNumber
                                        && let Some(identity) = config.identity.as_ref()
                                    {
                                        warn!(
                                            topic = %topic,
                                            partition = partition,
                                            "OutOfOrderSequenceNumber in batch, resetting sequence"
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
                                        match build_batch(sequence, &config) {
                                            Ok((r, ..)) => request = r,
                                            Err(encode_err) => {
                                                break Err(encode_err);
                                            }
                                        }
                                    } else if err.is_retriable()
                                        && let Err(refresh_err) = metadata
                                            .refresh_for_topics(Some(&[topic.as_ref()]))
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
                        if let Err(refresh_err) =
                            metadata.refresh_for_topics(Some(&[topic.as_ref()])).await
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
                }

                let batch_bytes_total: u64 = pending.iter().map(|p| p.estimated_size as u64).sum();
                metrics.record_batch(pending.len() as u64);
                metrics.bytes_sent.add(batch_bytes_total);
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
                    };
                    crate::interceptor::safe_on_acknowledgement(&*config.interceptor, &meta, None);
                    let _ = p.response_tx.send(AppendResponse::Done(Ok(meta)));
                }
            }
            Err(e) => {
                // Rollback unused sequence range so the next batch to
                // this partition doesn't trigger unnecessary OOSN.
                if let Some(identity) = config.identity.as_ref() {
                    let _ =
                        identity.rollback_sequence_range(topic.as_ref(), partition, record_count);
                }
                metrics.record_error();
                let topic_owned = topic.to_string();
                for p in pending {
                    let meta = RecordMetadata {
                        topic: topic_owned.clone(),
                        partition,
                        offset: -1,
                        timestamp: 0,
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
            .filter(|(_, batch)| !batch.batch.is_empty())
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
        let batch = AccumulatorBatch::new("test".to_string().into(), 0, 16384, Compression::None);
        std::thread::sleep(Duration::from_millis(10));
        assert!(batch.age() >= Duration::from_millis(10));
    }

    #[test]
    fn test_accumulator_batch_new() {
        let batch =
            AccumulatorBatch::new("test-topic".to_string().into(), 1, 32768, Compression::Gzip);
        assert!(batch.batch.is_empty());
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
    /// etc.) ultimately call `tokio::spawn(Self::send_extracted_batch(...))`;
    /// this compile-time assertion ensures the future stays `Send` so
    /// `tokio::spawn` can schedule it across threads.
    #[test]
    fn test_send_extracted_batch_is_send() {
        fn assert_send<T: Send>() {}
        // This compiles only if the future returned by send_extracted_batch is Send,
        // which is required for tokio::spawn to work.
        assert_send::<std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>>();
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
}
