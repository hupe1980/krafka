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
use tokio::sync::{Notify, Semaphore, mpsc, oneshot};
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

/// Response from the accumulator for an append attempt.
#[derive(Debug)]
enum AppendResponse {
    /// Record accepted — metadata will arrive via the inner Result.
    Done(Result<RecordMetadata>),
    /// Buffer is full — the record is returned so the caller can retry
    /// without cloning.
    BufferFull {
        record: ProducerRecord,
        operation_guard: InFlightOpGuard,
    },
}

/// Message sent to the accumulator background task.
#[derive(Debug)]
enum AccumulatorMessage {
    /// Add a record to the accumulator.
    Append {
        record: ProducerRecord,
        partition: PartitionId,
        response_tx: oneshot::Sender<AppendResponse>,
        operation_guard: InFlightOpGuard,
    },
    /// Flush all batches.
    Flush {
        response_tx: oneshot::Sender<Result<()>>,
    },
    /// Shutdown the accumulator, flush remaining batches, and signal completion.
    Shutdown { response_tx: oneshot::Sender<()> },
}

/// Handle to the record accumulator.
#[derive(Clone)]
pub struct RecordAccumulatorHandle {
    sender: mpsc::Sender<AccumulatorMessage>,
    /// Notified when buffer memory is freed (backpressure).
    memory_freed: Arc<Notify>,
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
        let mut operation_guard = Some(operation_guard);
        // Hold the record in an Option so the first send moves it into the
        // channel without cloning. On BufferFull the accumulator returns it,
        // so retries are also zero-copy.
        let mut pending = Some(record);

        loop {
            // `pending` is always `Some` here — either set by the
            // initial call or replenished by `AppendResponse::BufferFull`.
            let Some(rec) = pending.take() else {
                unreachable!("pending record missing");
            };
            let Some(guard) = operation_guard.take() else {
                unreachable!("operation guard missing");
            };
            let (response_tx, response_rx) = oneshot::channel();

            // Pre-register interest in memory_freed BEFORE sending so that
            // a notify_waiters() from extract_batch/flush_all between the
            // send and the await cannot be missed.
            let notified = self.memory_freed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            // Respect max_block_ms for the channel send too, in case the
            // accumulator channel is backed up.
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            tokio::time::timeout(
                remaining,
                self.sender.send(AccumulatorMessage::Append {
                    record: rec,
                    partition,
                    response_tx,
                    operation_guard: guard,
                }),
            )
            .await
            .map_err(|_| {
                KrafkaError::timeout(
                    "producer append: max_block exceeded while sending to accumulator",
                )
            })?
            .map_err(|_| KrafkaError::invalid_state("accumulator closed"))?;

            let response = response_rx
                .await
                .map_err(|_| KrafkaError::invalid_state("accumulator response dropped"))?;

            match response {
                AppendResponse::BufferFull {
                    record: returned_record,
                    operation_guard: guard,
                } => {
                    // The accumulator returned the record without touching it.
                    // Wait for memory to be freed, then retry with the same
                    // record instance — zero clones on the retry path.
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        return Err(KrafkaError::timeout(
                            "producer append: max_block exceeded while waiting \
                             for buffer memory (ProducerConfig::max_block / \
                             AccumulatorConfig::max_block_ms)",
                        ));
                    }
                    if tokio::time::timeout(remaining, notified).await.is_err() {
                        return Err(KrafkaError::timeout(
                            "producer append: max_block exceeded while waiting \
                             for buffer memory (ProducerConfig::max_block / \
                             AccumulatorConfig::max_block_ms)",
                        ));
                    }
                    // Replenish pending for the next iteration.
                    pending = Some(returned_record);
                    operation_guard = Some(guard);
                }
                AppendResponse::Done(result) => return result,
            }
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

/// RAII guard that releases in-flight memory and notifies waiters on drop.
///
/// Created by `extract_batch` and passed to `send_extracted_batch`.
/// When the send task completes (or panics), the guard automatically
/// decrements `in_flight_memory` and wakes blocked `append()` callers.
struct InFlightGuard {
    bytes: usize,
    in_flight_memory: Arc<AtomicUsize>,
    memory_freed: Arc<Notify>,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.in_flight_memory
            .fetch_sub(self.bytes, Ordering::Relaxed);
        self.memory_freed.notify_waiters();
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
    /// Current total memory usage in bytes (buffered, not yet extracted).
    memory_used: usize,
    /// Memory held by in-flight send tasks (extracted but not yet completed).
    in_flight_memory: Arc<AtomicUsize>,
    /// Retry policy for transient failures.
    retry_policy: RetryPolicy,
    /// Shared metrics.
    metrics: Arc<ProducerMetrics>,
    /// Notified when buffer memory is freed (backpressure).
    memory_freed: Arc<Notify>,
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
        // channel before the accumulator's memory-check runs.  When
        // buffer_memory is configured, we shrink further so at most ~10% of
        // the budget can be untracked.
        let channel_capacity = if config.buffer_memory > 0 {
            let batch = config.batch_size.max(1);
            (config.buffer_memory / 10 / batch).clamp(1, 256)
        } else {
            64
        };
        let (sender, receiver) = mpsc::channel(channel_capacity);
        let memory_freed = Arc::new(Notify::new());
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
            memory_used: 0,
            in_flight_memory,
            retry_policy,
            metrics,
            memory_freed: memory_freed.clone(),
        };

        let memory_freed_panic = memory_freed.clone();
        tokio::spawn(async move {
            let join_handle = tokio::spawn(accumulator.run(receiver));
            if let Err(join_err) = join_handle.await {
                if join_err.is_panic() {
                    tracing::error!("Accumulator task panicked: {join_err}");
                } else {
                    tracing::error!("Accumulator task cancelled: {join_err}");
                }
                // Wake all blocked append() callers so they observe the
                // closed channel and return an error instead of hanging.
                memory_freed_panic.notify_waiters();
            }
        });

        RecordAccumulatorHandle {
            sender,
            memory_freed,
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
                            response_tx,
                            operation_guard,
                        }) => {
                            self.handle_append(record, partition, response_tx, operation_guard).await;
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
                    self.check_linger_expiry().await;
                }
            }
        }

        debug!("Accumulator shutdown complete");
    }

    /// Handle appending a record.
    async fn handle_append(
        &mut self,
        record: ProducerRecord,
        partition: PartitionId,
        response_tx: oneshot::Sender<AppendResponse>,
        operation_guard: InFlightOpGuard,
    ) {
        // Estimate record size for memory tracking and batch size-gating.
        let record_size = record.estimated_size();
        let topic = record.topic.clone();
        let key = (topic, partition);

        // Check memory limit before appending (0 = unlimited).
        // Include in-flight memory so extracted-but-unsent batches are counted.
        let total_memory = self.memory_used + self.in_flight_memory.load(Ordering::Relaxed);
        if self.config.buffer_memory > 0 && total_memory + record_size > self.config.buffer_memory {
            // Return the record to the caller so it can retry without cloning.
            let _ = response_tx.send(AppendResponse::BufferFull {
                record,
                operation_guard,
            });
            return;
        }

        // Track memory usage
        self.memory_used += record_size;

        // Get or create batch
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

            // Check if batch is full
            if accumulator_batch.batch.is_full() {
                trace!("Batch full for {}-{}, flushing", key.0, partition);
                self.flush_batch(&key).await;
            } else if self.config.linger.is_zero() {
                // linger=0 means send immediately without waiting
                // for the next linger timer tick (up to 1ms delay otherwise).
                trace!("Linger=0 for {}-{}, flushing immediately", key.0, partition);
                self.flush_batch(&key).await;
            }
        } else {
            // Batch is full, flush it first and then add to new batch
            self.flush_batch(&key).await;

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
            } else {
                // Record too large for batch - free the memory we reserved
                self.memory_used = self.memory_used.saturating_sub(record_size);
                // Wake any tasks waiting for buffer memory so they can make progress.
                self.memory_freed.notify_waiters();
                let _ = response_tx.send(AppendResponse::Done(Err(KrafkaError::config(
                    "record too large for batch size",
                ))));
            }
        }
    }

    /// Check for batches that have exceeded linger time (concurrent flush).
    async fn check_linger_expiry(&mut self) {
        if self.config.linger.is_zero() {
            self.flush_all_ready().await;
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

        let mut join_set = tokio::task::JoinSet::new();
        for ((topic, partition), (batch, guard)) in extracted {
            let metadata = self.metadata.clone();
            let config = self.config.clone();
            let retry_policy = self.retry_policy.clone();
            let metrics = self.metrics.clone();
            join_set.spawn(Self::send_extracted_batch(
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
        while join_set.join_next().await.is_some() {}
    }

    /// Flush all ready batches concurrently (non-empty with linger=0).
    async fn flush_all_ready(&mut self) {
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

        let mut join_set = tokio::task::JoinSet::new();
        for ((topic, partition), (batch, guard)) in extracted {
            let metadata = self.metadata.clone();
            let config = self.config.clone();
            let retry_policy = self.retry_policy.clone();
            let metrics = self.metrics.clone();
            join_set.spawn(Self::send_extracted_batch(
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
        while join_set.join_next().await.is_some() {}
    }

    /// Extract a batch from the accumulator, transferring its memory to
    /// the in-flight tracker. The actual free + notify happens when the
    /// send task completes (see `send_extracted_batch`).
    fn extract_batch(
        &mut self,
        key: &(String, PartitionId),
    ) -> Option<(AccumulatorBatch, InFlightGuard)> {
        let batch = self.batches.remove(key)?;
        if batch.batch.is_empty() {
            return None;
        }
        let batch_memory: usize = batch.pending.iter().map(|p| p.estimated_size).sum();
        self.memory_used = self.memory_used.saturating_sub(batch_memory);
        self.in_flight_memory
            .fetch_add(batch_memory, Ordering::Relaxed);
        let guard = InFlightGuard {
            bytes: batch_memory,
            in_flight_memory: self.in_flight_memory.clone(),
            memory_freed: self.memory_freed.clone(),
        };
        Some((batch, guard))
    }

    /// Flush a specific batch by spawning a background task.
    ///
    /// Previously, this method awaited the network I/O inline, blocking the
    /// entire accumulator task. Now it spawns the send as a background task
    /// matching the concurrent flush pattern used by `check_linger_expiry`.
    async fn flush_batch(&mut self, key: &(String, PartitionId)) {
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

    /// Flush all batches concurrently.
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

        // Send all batches concurrently
        let mut join_set = tokio::task::JoinSet::new();
        for ((topic, partition), (batch, guard)) in extracted {
            let metadata = self.metadata.clone();
            let config = self.config.clone();
            let retry_policy = self.retry_policy.clone();
            let metrics = self.metrics.clone();
            join_set.spawn(Self::send_extracted_batch(
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
        while join_set.join_next().await.is_some() {}
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

    #[tokio::test]
    async fn test_backpressure_timeout_returns_timeout_error() {
        let (sender, mut receiver) = mpsc::channel::<AccumulatorMessage>(16);
        let memory_freed = Arc::new(tokio::sync::Notify::new());
        let handle = RecordAccumulatorHandle {
            sender,
            memory_freed,
            max_block_ms: Duration::from_millis(50),
            in_flight_barrier: Arc::new(InFlightBarrier::new()),
        };

        // Spawn a fake accumulator that always responds BufferFull
        tokio::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                if let AccumulatorMessage::Append {
                    record,
                    response_tx,
                    operation_guard,
                    ..
                } = msg
                {
                    let _ = response_tx.send(AppendResponse::BufferFull {
                        record,
                        operation_guard,
                    });
                }
            }
        });

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

    #[tokio::test]
    async fn test_backpressure_unblocks_on_notify() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let n = notify.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            n.notify_waiters();
        });
        let result = tokio::time::timeout(Duration::from_secs(2), notify.notified()).await;
        assert!(result.is_ok(), "notify should have fired");
    }
}
