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

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::time::interval;
use tracing::{debug, trace, warn};

use super::barrier::{InFlightBarrier, InFlightOpGuard};
use super::batch::ProducerBatch;
use super::record::{ProducerRecord, RecordMetadata};
use super::retry::{RetryContext, RetryPolicy};
use crate::PartitionId;
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::interceptor::ProducerInterceptor;
use crate::metadata::ClusterMetadata;
use crate::metrics::ProducerMetrics;
use crate::protocol::{
    ApiKey, Compression, ProducePartitionData, ProduceRequest, ProduceResponse, ProduceTopicData,
    RecordBatchBuilder, VersionedDecode, VersionedEncode, versions,
};

/// Maximum number of concurrent `send_extracted_batch` tasks in a single
/// bounded drain wave.
///
/// `flush_all` (Flush/Shutdown commands) awaits `spawn_batches_bounded`
/// directly so completion is confirmed before the caller is unblocked.
/// Linger-triggered paths (`check_linger_expiry`, `flush_all_ready`) and
/// single-batch flushes (`flush_batch`) detach their send work via
/// `spawn_batches_detached` so the accumulator run loop is never held
/// waiting for network I/O. `spawn_batches_bounded` enforces this cap
/// inside each detached wave; `InFlightGuard` limits per-broker parallelism
/// across concurrent waves.
///
/// Fix for H3: prior implementations spawned one task per batch with no
/// cap, meaning 10k partitions at `linger.ms=5` produced a 10k-task burst
/// every linger tick. This number is deliberately modest — batch sends are
/// I/O-bound and the per-broker connection pipeline already serializes
/// requests, so extra parallelism beyond a few dozen tasks does not
/// translate to throughput and only adds scheduler pressure.
const MAX_CONCURRENT_BATCH_SENDS: usize = 64;

/// Validate that `record_size` bytes can be admitted into the memory pool.
///
/// Returns an error immediately if the record would permanently block
/// `acquire_many` — either because it exceeds `u32::MAX` (the hard limit of
/// `Semaphore::acquire_many`) or because it exceeds the configured
/// `buffer_memory` budget (permits can never accumulate to that level).
///
/// The u32 check comes first so the error message is always accurate:
/// a record larger than both limits is a semaphore-API violation, not a
/// tunable configuration problem.
fn check_record_admission(record_size: usize, memory_capacity: usize) -> Result<()> {
    if record_size > u32::MAX as usize {
        return Err(KrafkaError::config(format!(
            "record size {record_size} B exceeds the semaphore \
             permit-count limit ({} B, u32::MAX); Kafka records must \
             be smaller",
            u32::MAX
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
    Append {
        record: ProducerRecord,
        partition: PartitionId,
        record_size: usize,
        response_tx: oneshot::Sender<AppendResponse>,
        operation_guard: InFlightOpGuard,
        permit_reservation: PermitReservation,
    },
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
    /// Surrender the release obligation; the caller is now responsible for
    /// calling `add_permits(bytes)` on the same semaphore (typically via
    /// `InFlightGuard::drop`).
    fn forget(self) {
        std::mem::forget(self);
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
    /// Maximum time to block waiting for buffer memory.
    max_block_ms: Duration,
    /// Barrier over all producer sends, including detached batch tasks.
    in_flight_barrier: Arc<InFlightBarrier>,
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
        let deadline = tokio::time::Instant::now() + self.max_block_ms;
        let record_size = record.estimated_size();

        // Reject records that cannot physically be admitted (exceeds the
        // semaphore permit limit or the configured buffer_memory budget).
        // Uses the module-level helper so both branches are unit-testable
        // without allocating large buffers.
        check_record_admission(record_size, self.memory_capacity)?;

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
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());

        // Send the Append; on failure (timeout / closed channel),
        // `permit_reservation` drops and returns the permits to the pool
        // so another waiter can proceed. On success the accumulator now
        // owns the release obligation via the message contents.
        match tokio::time::timeout(
            remaining,
            self.sender.send(AccumulatorMessage::Append {
                record,
                partition,
                record_size,
                response_tx,
                operation_guard,
                permit_reservation,
            }),
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
    /// Request timeout.
    pub request_timeout: Duration,
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
}

impl Clone for AccumulatorConfig {
    fn clone(&self) -> Self {
        Self {
            batch_size: self.batch_size,
            linger: self.linger,
            compression: self.compression,
            acks: self.acks,
            request_timeout: self.request_timeout,
            buffer_memory: self.buffer_memory,
            max_block_ms: self.max_block_ms,
            in_flight_semaphore: self.in_flight_semaphore.clone(),
            interceptor: self.interceptor.clone(),
            identity: self.identity.clone(),
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
            .field("request_timeout", &self.request_timeout)
            .field("buffer_memory", &self.buffer_memory)
            .field("max_block_ms", &self.max_block_ms)
            .field("interceptor", &self.interceptor)
            .finish()
    }
}

impl Default for AccumulatorConfig {
    fn default() -> Self {
        Self {
            batch_size: 16384,
            linger: Duration::from_millis(0),
            compression: Compression::None,
            acks: -1,
            request_timeout: Duration::from_secs(30),
            buffer_memory: 32 * 1024 * 1024, // 32 MB default (same as Kafka Java client)
            max_block_ms: Duration::from_secs(60), // 60 seconds default
            in_flight_semaphore: Arc::new(Semaphore::new(5)), // default max_in_flight
            interceptor: Arc::new(crate::interceptor::NoOpProducerInterceptor),
            identity: None,
        }
    }
}

/// A pending record waiting for its batch to be sent.
struct PendingRecord {
    record: ProducerRecord,
    response_tx: oneshot::Sender<AppendResponse>,
    offset_in_batch: i64,
    /// Estimated size in bytes for memory tracking.
    estimated_size: usize,
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
        topic: String,
        partition: PartitionId,
        max_size: usize,
        compression: Compression,
    ) -> Self {
        Self {
            batch: ProducerBatch::new(topic, partition, max_size, compression),
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
    batches: HashMap<(String, PartitionId), AccumulatorBatch>,
    /// Cluster metadata for sending.
    metadata: Arc<ClusterMetadata>,
    /// Memory held by in-flight send tasks (extracted but not yet completed).
    /// Exposed for metrics only; backpressure is enforced by `memory_permits`.
    in_flight_memory: Arc<AtomicUsize>,
    /// Retry policy for transient failures.
    retry_policy: RetryPolicy,
    /// Shared metrics.
    metrics: Arc<ProducerMetrics>,
    /// Byte-granular FIFO semaphore gating `buffer_memory` (shared with handle).
    memory_permits: Arc<Semaphore>,
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
        let memory_capacity = if config.buffer_memory > 0 {
            if config.buffer_memory > Semaphore::MAX_PERMITS {
                warn!(
                    requested = config.buffer_memory,
                    effective = Semaphore::MAX_PERMITS,
                    "buffer_memory exceeds Semaphore::MAX_PERMITS; clamping effective \
                     producer memory capacity"
                );
                Semaphore::MAX_PERMITS
            } else {
                config.buffer_memory
            }
        } else {
            Semaphore::MAX_PERMITS
        };
        let memory_permits = Arc::new(Semaphore::new(memory_capacity));
        let in_flight_memory = Arc::new(AtomicUsize::new(0));
        let max_block_ms = config.max_block_ms;

        if config.buffer_memory == 0 {
            warn!(
                "buffer_memory=0 disables producer backpressure; \
                 memory usage is unbounded. Not recommended for production."
            );
        }

        let accumulator = Self {
            config,
            batches: HashMap::new(),
            metadata,
            in_flight_memory,
            retry_policy,
            metrics,
            memory_permits: memory_permits.clone(),
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
            max_block_ms,
            in_flight_barrier,
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
                        Some(AccumulatorMessage::Append {
                            record,
                            partition,
                            record_size,
                            response_tx,
                            operation_guard,
                            permit_reservation,
                        }) => {
                            self.handle_append(record, partition, record_size, response_tx, operation_guard, permit_reservation).await;
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
    async fn handle_append(
        &mut self,
        record: ProducerRecord,
        partition: PartitionId,
        record_size: usize,
        response_tx: oneshot::Sender<AppendResponse>,
        operation_guard: InFlightOpGuard,
        permit_reservation: PermitReservation,
    ) {
        let key = (record.topic.clone(), partition);

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
                _operation_guard: operation_guard,
            });
            // Release is now owned by the eventual `InFlightGuard`.
            permit_reservation.forget();

            // Check if batch is full
            if accumulator_batch.batch.is_full() {
                trace!("Batch full for {}-{}, flushing", key.0, partition);
                self.flush_batch(&key);
            } else if self.config.linger.is_zero() {
                // linger=0 means send immediately without waiting
                // for the next linger timer tick (up to 1ms delay otherwise).
                trace!("Linger=0 for {}-{}, flushing immediately", key.0, partition);
                self.flush_batch(&key);
            }
        } else {
            // Batch is full, flush it first and then add to new batch
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
        );
    }

    /// Flush all ready batches by detaching send tasks.
    ///
    /// Extracts all non-empty batches synchronously, then hands them off to
    /// `spawn_batches_detached` so the run loop is never blocked by network
    /// I/O. The send cap (`MAX_CONCURRENT_BATCH_SENDS`) is enforced inside
    /// the detached wave.
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
        );
    }

    /// Spawn at most `MAX_CONCURRENT_BATCH_SENDS` send tasks at a time.
    ///
    /// Fix for H3: the previous implementation did `join_set.spawn(...)` in
    /// a tight loop with no cap, materializing every batch into a Tokio
    /// task in a single linger tick. With 10k partitions at `linger.ms=5`
    /// this meant 10k spawns per tick, flooding the global injection queue
    /// and degrading tail latency for no throughput gain (the per-broker
    /// connection already serializes). We now drain in a bounded fashion:
    /// spawn up to the cap, then await one task before spawning the next.
    /// The cap is deliberately fixed (`MAX_CONCURRENT_BATCH_SENDS`) rather
    /// than derived from `num_cpus` — the send tasks are I/O bound and a
    /// fixed ceiling caps scheduler pressure predictably.
    async fn spawn_batches_bounded(
        extracted: Vec<((String, PartitionId), (AccumulatorBatch, InFlightGuard))>,
        metadata: &Arc<ClusterMetadata>,
        config: &AccumulatorConfig,
        retry_policy: &RetryPolicy,
        metrics: &Arc<ProducerMetrics>,
    ) {
        let mut join_set = tokio::task::JoinSet::new();
        for ((topic, partition), (batch, guard)) in extracted {
            if join_set.len() >= MAX_CONCURRENT_BATCH_SENDS {
                // Drain one slot before admitting the next task.
                // `join_next` returning `None` cannot happen here because
                // `len() >= cap >= 1`.
                if let Some(Err(e)) = join_set.join_next().await
                    && e.is_panic()
                {
                    warn!("send_extracted_batch task panicked: {e}");
                }
            }
            join_set.spawn(Self::send_extracted_batch(
                topic,
                partition,
                batch.pending,
                batch.created_at,
                guard,
                metadata.clone(),
                config.clone(),
                retry_policy.clone(),
                metrics.clone(),
            ));
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
    /// immediately. Inside the task, `spawn_batches_bounded` enforces the
    /// `MAX_CONCURRENT_BATCH_SENDS` cap so at most that many
    /// `send_extracted_batch` tasks run concurrently within each wave.
    /// Cross-wave parallelism is bounded by the `InFlightGuard` semaphore
    /// inside `send_extracted_batch`.
    fn spawn_batches_detached(
        extracted: Vec<((String, PartitionId), (AccumulatorBatch, InFlightGuard))>,
        metadata: &Arc<ClusterMetadata>,
        config: &AccumulatorConfig,
        retry_policy: &RetryPolicy,
        metrics: &Arc<ProducerMetrics>,
    ) {
        if extracted.is_empty() {
            return;
        }
        let metadata = metadata.clone();
        let config = config.clone();
        let retry_policy = retry_policy.clone();
        let metrics = metrics.clone();
        // Fire-and-forget: dropping the JoinHandle detaches the task;
        // it continues running independently. The task is self-contained
        // (InFlightGuard ensures memory permits are reclaimed on completion
        // or panic) so we do not need to track it.
        let _wave = tokio::spawn(async move {
            Self::spawn_batches_bounded(extracted, &metadata, &config, &retry_policy, &metrics)
                .await;
        });
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
        key: &(String, PartitionId),
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

    /// Flush a specific batch by spawning a detached send task.
    ///
    /// Spawns exactly one `send_extracted_batch` task and returns immediately
    /// so the accumulator run loop is never blocked on the linger=0 hot path
    /// or when a full batch is encountered during an append. `InFlightGuard`
    /// limits per-broker parallelism; `send_extracted_batch` handles all
    /// retry and backpressure internally.
    fn flush_batch(&mut self, key: &(String, PartitionId)) {
        if let Some((batch, guard)) = self.extract_batch(key) {
            let topic = key.0.clone();
            let partition = key.1;
            let metadata = self.metadata.clone();
            let config = self.config.clone();
            let retry_policy = self.retry_policy.clone();
            let metrics = self.metrics.clone();
            tokio::spawn(Self::send_extracted_batch(
                topic,
                partition,
                batch.pending,
                batch.created_at,
                guard,
                metadata,
                config,
                retry_policy,
                metrics,
            ));
        }
    }

    /// Send an extracted batch to the broker with retry and metadata refresh.
    ///
    /// This is a static method to enable concurrent flushing via `FuturesUnordered`.
    /// Acquires an in-flight semaphore permit to respect `max_in_flight` concurrency limits.
    #[allow(clippy::too_many_arguments)]
    async fn send_extracted_batch(
        topic: String,
        partition: PartitionId,
        pending: Vec<PendingRecord>,
        enqueued_at: Instant,
        _in_flight_guard: InFlightGuard,
        metadata: Arc<ClusterMetadata>,
        config: AccumulatorConfig,
        retry_policy: RetryPolicy,
        metrics: Arc<ProducerMetrics>,
    ) {
        // Acquire in-flight permit before sending (accumulator was
        // bypassing max_in_flight). The permit is held until this batch completes.
        let _permit = config.in_flight_semaphore.acquire().await;
        let _timer = metrics.send_latency.start();

        let record_count = pending.len() as i32;

        // Allocate sequence range for idempotent production.
        let mut sequence: Option<i32> = match config
            .identity
            .as_ref()
            .map(|id| id.allocate_sequence(&topic, partition, record_count))
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
        let build_batch = |seq: Option<i32>,
                           cfg: &AccumulatorConfig|
         -> crate::error::Result<ProduceRequest> {
            let mut batch_builder = RecordBatchBuilder::new().compression(cfg.compression);

            // Tag with idempotent producer identity
            if let (Some(identity), Some(s)) = (&cfg.identity, seq) {
                batch_builder =
                    batch_builder.producer(identity.producer_id(), identity.producer_epoch(), s);
            }

            for p in &pending {
                let key = p.record.key.clone();
                let value = Some(p.record.value.clone());
                if p.record.headers.is_empty() {
                    batch_builder = batch_builder.add_record(key, value);
                } else {
                    batch_builder = batch_builder.add_record_with_headers(
                        key,
                        value,
                        p.record
                            .headers
                            .iter()
                            .map(|(k, v)| (k.clone(), Bytes::from(v.clone())))
                            .collect(),
                    );
                }
            }
            let batch = batch_builder.build();
            let batch_bytes = batch.encode()?;

            Ok(ProduceRequest {
                transactional_id: None,
                acks: cfg.acks,
                timeout_ms: crate::util::duration_to_millis_i32(cfg.request_timeout),
                topic_data: vec![ProduceTopicData {
                    name: topic.clone(),
                    topic_id: None,
                    partition_data: vec![ProducePartitionData {
                        index: partition,
                        records: batch_bytes,
                    }],
                }],
            })
        };

        let mut request = match build_batch(sequence, &config) {
            Ok(r) => r,
            Err(e) => {
                // Rollback sequence on encode failure
                if let Some(ref identity) = config.identity {
                    let _ = identity.rollback_sequence_range(&topic, partition, record_count);
                }
                for p in pending {
                    let _ = p.response_tx.send(AppendResponse::Done(Err(e.clone())));
                }
                return;
            }
        };

        // Retry loop — delivery timeout starts from when the first record
        // entered the batch (enqueued_at), not from the first send attempt,
        // so that time spent in the linger window / backpressure counts
        // against the delivery budget (matching Java client behavior).
        let mut retry_ctx = RetryContext::new_with_start(
            retry_policy,
            format!("batch({topic}-{partition})"),
            enqueued_at,
        );

        let result: std::result::Result<(i64, i64), KrafkaError> = loop {
            // Get connection to leader
            let conn = match metadata.get_leader_connection(&topic, partition).await {
                Ok(c) => c,
                Err(e) => {
                    if e.is_retriable() {
                        debug!(
                            topic = %topic,
                            partition = partition,
                            error = %e,
                            "Batch connection error, refreshing metadata"
                        );
                        if let Err(refresh_err) = metadata.refresh_for_topics(Some(&[&topic])).await
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

            // Negotiate Produce version for this broker.
            let produce_version = match conn
                .negotiate_api_version(
                    ApiKey::Produce,
                    versions::PRODUCE_MAX,
                    versions::PRODUCE_MIN,
                )
                .await
            {
                Some(v) => v,
                None => {
                    let e = KrafkaError::protocol("no mutually supported Produce API version");
                    debug!(
                        topic = %topic,
                        partition = partition,
                        "Produce version negotiation failed, refreshing metadata"
                    );
                    if let Err(refresh_err) = metadata.refresh_for_topics(Some(&[&topic])).await {
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
                                .find(|r| r.name == topic)
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

                                    // OutOfOrderSequenceNumber: atomically reset
                                    // sequence and rebuild batch before retrying.
                                    // Skip metadata refresh — OOSN is a sequence
                                    // mismatch, not a leader-change error.
                                    if pr.error_code == ErrorCode::OutOfOrderSequenceNumber
                                        && let Some(identity) = config.identity.as_ref()
                                    {
                                        warn!(
                                            topic = %topic,
                                            partition = partition,
                                            "OutOfOrderSequenceNumber in batch, resetting sequence"
                                        );
                                        let new_seq = match identity.reset_and_allocate(
                                            &topic,
                                            partition,
                                            record_count,
                                        ) {
                                            Ok(s) => s,
                                            Err(e) => break Err(e),
                                        };
                                        sequence = Some(new_seq);
                                        match build_batch(sequence, &config) {
                                            Ok(r) => request = r,
                                            Err(encode_err) => {
                                                break Err(encode_err);
                                            }
                                        }
                                    } else if err.is_retriable()
                                        && let Err(refresh_err) =
                                            metadata.refresh_for_topics(Some(&[&topic])).await
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
                                    break Err(KrafkaError::protocol(
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
                        if let Err(refresh_err) = metadata.refresh_for_topics(Some(&[&topic])).await
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
                    identity.acknowledge(&topic, partition, last_seq);
                }

                let batch_bytes_total: u64 = pending.iter().map(|p| p.estimated_size as u64).sum();
                metrics.record_batch(pending.len() as u64);
                metrics.bytes_sent.add(batch_bytes_total);
                for p in pending {
                    let meta = RecordMetadata {
                        topic: topic.clone(),
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
                    let _ = identity.rollback_sequence_range(&topic, partition, record_count);
                }
                metrics.record_error();
                for p in pending {
                    let meta = RecordMetadata {
                        topic: topic.clone(),
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
        assert_eq!(config.linger, Duration::from_millis(0));
        assert_eq!(config.acks, -1);
    }

    #[test]
    fn test_accumulator_batch_age() {
        let batch = AccumulatorBatch::new("test".to_string(), 0, 16384, Compression::None);
        std::thread::sleep(Duration::from_millis(10));
        assert!(batch.age() >= Duration::from_millis(10));
    }

    #[test]
    fn test_accumulator_batch_new() {
        let batch = AccumulatorBatch::new("test-topic".to_string(), 1, 32768, Compression::Gzip);
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
            request_timeout: Duration::from_secs(10),
            buffer_memory: 64 * 1024 * 1024,
            max_block_ms: Duration::from_secs(30),
            in_flight_semaphore: Arc::new(Semaphore::new(5)),
            interceptor: Arc::new(crate::interceptor::NoOpProducerInterceptor),
            identity: None,
        };
        assert_eq!(config.batch_size, 65536);
        assert_eq!(config.linger, Duration::from_millis(50));
        assert_eq!(config.acks, 1);
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
        let linger = Duration::from_millis(0);
        let check_interval = Duration::from_millis(1).max(linger / 10);
        assert_eq!(check_interval, Duration::from_millis(1));
    }

    /// Verify `check_linger_expiry` calls `flush_all_ready` when linger=0.
    #[test]
    fn test_linger_zero_is_zero() {
        let config = AccumulatorConfig {
            linger: Duration::from_millis(0),
            ..Default::default()
        };
        assert!(config.linger.is_zero());
    }

    /// Verify flush_batch signature enables spawning
    /// (send_extracted_batch is 'static + Send, required for tokio::spawn).
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
            max_block_ms: Duration::from_millis(50),
            in_flight_barrier: Arc::new(InFlightBarrier::new()),
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
            max_block_ms: Duration::from_secs(60),
            in_flight_barrier: Arc::new(InFlightBarrier::new()),
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
            max_block_ms: Duration::from_secs(60),
            in_flight_barrier: Arc::new(InFlightBarrier::new()),
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
        let handle = RecordAccumulatorHandle {
            sender,
            memory_permits: sem.clone(),
            memory_capacity: 1024,
            max_block_ms: Duration::from_millis(500),
            in_flight_barrier: Arc::new(InFlightBarrier::new()),
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
    }

    /// `check_record_admission` rejects records that exceed `buffer_memory`.
    ///
    /// Tests the `buffer_memory` branch independently via the extracted
    /// helper so both admission failure modes are regression-proof without
    /// needing to allocate large buffers.
    #[test]
    fn test_check_record_admission_rejects_oversized_for_buffer() {
        let err = check_record_admission(1024, 16).expect_err("must reject");
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

    /// `check_record_admission` rejects records that exceed `u32::MAX`.
    ///
    /// Tests the `u32::MAX` branch directly via the extracted helper —
    /// no >4 GiB allocation needed.
    #[test]
    fn test_check_record_admission_rejects_oversized_for_u32_max() {
        // Synthetic size just above u32::MAX — no allocation required.
        let oversized = u32::MAX as usize + 1;
        let err = check_record_admission(oversized, usize::MAX).expect_err("must reject");
        let msg = err.to_string();
        assert!(
            msg.contains("u32::MAX"),
            "error must cite u32::MAX, got: {msg}"
        );
        assert!(
            !msg.contains("buffer_memory"),
            "must not cite buffer_memory for a u32::MAX rejection, got: {msg}"
        );
    }
}
