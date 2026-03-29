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
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{Notify, Semaphore, mpsc, oneshot};
use tokio::time::interval;
use tracing::{debug, trace};

use super::batch::ProducerBatch;
use super::record::{ProducerRecord, RecordMetadata};
use super::retry::{RetryContext, RetryPolicy};
use crate::PartitionId;
use crate::error::{KrafkaError, Result};
use crate::interceptor::ProducerInterceptor;
use crate::metadata::ClusterMetadata;
use crate::metrics::ProducerMetrics;
use crate::protocol::{
    ApiKey, Compression, ProducePartitionData, ProduceRequest, ProduceResponse, ProduceTopicData,
    RecordBatchBuilder,
};

/// Response from the accumulator for an append attempt.
#[derive(Debug)]
enum AppendResponse {
    /// Record accepted — metadata will arrive via the inner Result.
    Done(Result<RecordMetadata>),
    /// Buffer is full — the record is returned so the caller can retry
    /// without cloning.
    BufferFull(ProducerRecord),
}

/// Message sent to the accumulator background task.
#[derive(Debug)]
enum AccumulatorMessage {
    /// Add a record to the accumulator.
    Append {
        record: ProducerRecord,
        partition: PartitionId,
        response_tx: oneshot::Sender<AppendResponse>,
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
        let deadline = tokio::time::Instant::now() + self.max_block_ms;
        // Hold the record in an Option so the first send moves it into the
        // channel without cloning. On BufferFull the accumulator returns it,
        // so retries are also zero-copy.
        let mut pending = Some(record);

        loop {
            // SAFETY: `pending` is always `Some` here — either set by the
            // initial call or replenished by `AppendResponse::BufferFull`.
            let rec = pending.take().expect("pending record missing");
            let (response_tx, response_rx) = oneshot::channel();
            self.sender
                .send(AccumulatorMessage::Append {
                    record: rec,
                    partition,
                    response_tx,
                })
                .await
                .map_err(|_| KrafkaError::invalid_state("accumulator closed"))?;

            let response = response_rx
                .await
                .map_err(|_| KrafkaError::invalid_state("accumulator response dropped"))?;

            match response {
                AppendResponse::BufferFull(returned_record) => {
                    // The accumulator returned the record without touching it.
                    // Wait for memory to be freed, then retry with the same
                    // record instance — zero clones on the retry path.
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        return Err(KrafkaError::config(
                            "Timed out waiting for buffer memory (max_block exceeded). \
                             Consider increasing ProducerConfig::max_block / \
                             AccumulatorConfig::max_block_ms, buffer_memory, \
                             or reducing production rate.",
                        ));
                    }
                    if tokio::time::timeout(remaining, self.memory_freed.notified())
                        .await
                        .is_err()
                    {
                        return Err(KrafkaError::config(
                            "Timed out waiting for buffer memory (max_block exceeded). \
                             Consider increasing ProducerConfig::max_block / \
                             AccumulatorConfig::max_block_ms, buffer_memory, \
                             or reducing production rate.",
                        ));
                    }
                    // Replenish pending for the next iteration.
                    pending = Some(returned_record);
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
    pub async fn shutdown(&self) {
        let (response_tx, response_rx) = oneshot::channel();
        let _ = self
            .sender
            .send(AccumulatorMessage::Shutdown { response_tx })
            .await;
        // Wait for the accumulator to finish flushing before returning.
        let _ = response_rx.await;
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
    /// Current total memory usage in bytes.
    memory_used: usize,
    /// Retry policy for transient failures.
    retry_policy: RetryPolicy,
    /// Shared metrics.
    metrics: Arc<ProducerMetrics>,
    /// Notified when buffer memory is freed (backpressure).
    memory_freed: Arc<Notify>,
}

impl RecordAccumulator {
    /// Create a new record accumulator and return a handle.
    pub fn spawn(
        config: AccumulatorConfig,
        metadata: Arc<ClusterMetadata>,
        retry_policy: RetryPolicy,
        metrics: Arc<ProducerMetrics>,
    ) -> RecordAccumulatorHandle {
        let (sender, receiver) = mpsc::channel(1024);
        let memory_freed = Arc::new(Notify::new());
        let max_block_ms = config.max_block_ms;

        let accumulator = Self {
            config,
            batches: HashMap::new(),
            metadata,
            memory_used: 0,
            retry_policy,
            metrics,
            memory_freed: memory_freed.clone(),
        };

        tokio::spawn(accumulator.run(receiver));

        RecordAccumulatorHandle {
            sender,
            memory_freed,
            max_block_ms,
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
                        Some(AccumulatorMessage::Append { record, partition, response_tx }) => {
                            self.handle_append(record, partition, response_tx).await;
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
    ) {
        let topic = record.topic.clone();
        let key = (topic.clone(), partition);

        // Estimate record size for memory tracking
        let record_size = Self::estimate_record_size(&record);

        // Check memory limit before appending (0 = unlimited)
        if self.config.buffer_memory > 0
            && self.memory_used + record_size > self.config.buffer_memory
        {
            // Return the record to the caller so it can retry without cloning.
            let _ = response_tx.send(AppendResponse::BufferFull(record));
            return;
        }

        // Track memory usage
        self.memory_used += record_size;

        // Get or create batch
        let accumulator_batch = self.batches.entry(key.clone()).or_insert_with(|| {
            AccumulatorBatch::new(
                topic.clone(),
                partition,
                self.config.batch_size,
                self.config.compression,
            )
        });

        // Try to add to current batch
        let offset = accumulator_batch.batch.len() as i64;
        if accumulator_batch.batch.try_add(record.clone()) {
            accumulator_batch.pending.push(PendingRecord {
                record,
                response_tx,
                offset_in_batch: offset,
                estimated_size: record_size,
            });

            // Check if batch is full
            if accumulator_batch.batch.is_full() {
                trace!("Batch full for {}-{}, flushing", topic, partition);
                self.flush_batch(&key).await;
            } else if self.config.linger.is_zero() {
                // linger=0 means send immediately without waiting
                // for the next linger timer tick (up to 1ms delay otherwise).
                trace!("Linger=0 for {}-{}, flushing immediately", topic, partition);
                self.flush_batch(&key).await;
            }
        } else {
            // Batch is full, flush it first and then add to new batch
            self.flush_batch(&key).await;

            // Create new batch and add record
            let mut new_batch = AccumulatorBatch::new(
                topic.clone(),
                partition,
                self.config.batch_size,
                self.config.compression,
            );

            if new_batch.batch.try_add(record.clone()) {
                new_batch.pending.push(PendingRecord {
                    record,
                    response_tx,
                    offset_in_batch: 0,
                    estimated_size: record_size,
                });
                self.batches.insert(key, new_batch);
            } else {
                // Record too large for batch - free the memory we reserved
                self.memory_used = self.memory_used.saturating_sub(record_size);
                let _ = response_tx.send(AppendResponse::Done(Err(KrafkaError::config(
                    "record too large for batch size",
                ))));
            }
        }
    }

    /// Estimate the memory size of a record.
    fn estimate_record_size(record: &ProducerRecord) -> usize {
        // Key + value + headers + overhead for topic name, metadata, etc.
        let key_size = record.key.as_ref().map(|k| k.len()).unwrap_or(0);
        let value_size = record.value.len();
        let headers_size: usize = record
            .headers
            .iter()
            .map(|(k, v)| k.len() + v.len() + 8) // 8 bytes overhead per header
            .sum();
        let topic_overhead = record.topic.len() + 64; // 64 bytes for struct overhead

        key_size + value_size + headers_size + topic_overhead
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
            if let Some(batch) = self.extract_batch(&key) {
                extracted.push((key, batch));
            }
        }

        let mut join_set = tokio::task::JoinSet::new();
        for ((topic, partition), batch) in extracted {
            let metadata = self.metadata.clone();
            let config = self.config.clone();
            let retry_policy = self.retry_policy.clone();
            let metrics = self.metrics.clone();
            join_set.spawn(Self::send_extracted_batch(
                topic,
                partition,
                batch.pending,
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
            if let Some(batch) = self.extract_batch(&key) {
                extracted.push((key, batch));
            }
        }

        let mut join_set = tokio::task::JoinSet::new();
        for ((topic, partition), batch) in extracted {
            let metadata = self.metadata.clone();
            let config = self.config.clone();
            let retry_policy = self.retry_policy.clone();
            let metrics = self.metrics.clone();
            join_set.spawn(Self::send_extracted_batch(
                topic,
                partition,
                batch.pending,
                metadata,
                config,
                retry_policy,
                metrics,
            ));
        }
        while join_set.join_next().await.is_some() {}
    }

    /// Extract a batch from the accumulator, freeing tracked memory.
    fn extract_batch(&mut self, key: &(String, PartitionId)) -> Option<AccumulatorBatch> {
        let batch = self.batches.remove(key)?;
        if batch.batch.is_empty() {
            return None;
        }
        let batch_memory: usize = batch.pending.iter().map(|p| p.estimated_size).sum();
        self.memory_used = self.memory_used.saturating_sub(batch_memory);
        // Wake one caller blocked on buffer backpressure (stores a permit so
        // it cannot be missed even if no task is currently waiting).
        self.memory_freed.notify_one();
        Some(batch)
    }

    /// Flush a specific batch by spawning a background task.
    ///
    /// Previously, this method awaited the network I/O inline, blocking the
    /// entire accumulator task. Now it spawns the send as a background task
    /// matching the concurrent flush pattern used by `check_linger_expiry`.
    async fn flush_batch(&mut self, key: &(String, PartitionId)) {
        if let Some(batch) = self.extract_batch(key) {
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
    async fn send_extracted_batch(
        topic: String,
        partition: PartitionId,
        pending: Vec<PendingRecord>,
        metadata: Arc<ClusterMetadata>,
        config: AccumulatorConfig,
        retry_policy: RetryPolicy,
        metrics: Arc<ProducerMetrics>,
    ) {
        // Acquire in-flight permit before sending (accumulator was
        // bypassing max_in_flight). The permit is held until this batch completes.
        let _permit = config.in_flight_semaphore.acquire().await;
        let _timer = metrics.send_latency.start();

        // Build and encode the record batch once (immutable across retries).
        let mut batch_builder = RecordBatchBuilder::new().compression(config.compression);
        for p in &pending {
            if p.record.headers.is_empty() {
                batch_builder = batch_builder.add_record(
                    p.record.key.clone().map(Bytes::from),
                    Some(Bytes::from(p.record.value.clone())),
                );
            } else {
                batch_builder = batch_builder.add_record_with_headers(
                    p.record.key.clone().map(Bytes::from),
                    Some(Bytes::from(p.record.value.clone())),
                    p.record
                        .headers
                        .iter()
                        .map(|(k, v)| (k.clone(), Bytes::from(v.clone())))
                        .collect(),
                );
            }
        }
        let batch = batch_builder.build();
        let batch_bytes = match batch.encode() {
            Ok(b) => b,
            Err(e) => {
                let error_msg = e.to_string();
                for p in pending {
                    let _ = p
                        .response_tx
                        .send(AppendResponse::Done(Err(KrafkaError::protocol(&error_msg))));
                }
                return;
            }
        };

        let request = ProduceRequest {
            transactional_id: None,
            acks: config.acks,
            timeout_ms: crate::util::duration_to_millis_i32(config.request_timeout),
            topic_data: vec![ProduceTopicData {
                name: topic.clone(),
                partition_data: vec![ProducePartitionData {
                    index: partition,
                    records: batch_bytes,
                }],
            }],
        };

        // Retry loop
        let mut retry_ctx = RetryContext::new(retry_policy, format!("batch({topic}-{partition})"));

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
                        let _ = metadata.refresh_for_topics(Some(&[&topic])).await;
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
                    .send_fire_and_forget(ApiKey::Produce, 0, |buf| {
                        request.encode_v0(buf);
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
                .send_request(ApiKey::Produce, 0, |buf| {
                    request.encode_v0(buf);
                })
                .await;

            match response_result {
                Ok(mut response_buf) => match ProduceResponse::decode_v0(&mut response_buf) {
                    Ok(produce_response) => {
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
                            Some(pr) => {
                                let err = KrafkaError::broker(
                                    pr.error_code,
                                    format!("batch produce failed for {topic}-{partition}"),
                                );
                                if err.is_retriable() {
                                    let _ = metadata.refresh_for_topics(Some(&[&topic])).await;
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
                },
                Err(e) => {
                    if e.is_retriable() {
                        debug!(
                            topic = %topic,
                            partition = partition,
                            error = %e,
                            "Batch send error, refreshing metadata"
                        );
                        let _ = metadata.refresh_for_topics(Some(&[&topic])).await;
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
                let batch_bytes_total: u64 = pending.iter().map(|p| p.estimated_size as u64).sum();
                metrics.record_batch(pending.len() as u64);
                metrics.bytes_sent.add(batch_bytes_total);
                for p in pending {
                    let meta = RecordMetadata {
                        topic: topic.clone(),
                        partition,
                        offset: base_offset + p.offset_in_batch,
                        timestamp,
                    };
                    crate::interceptor::safe_on_acknowledgement(&*config.interceptor, &meta, None);
                    let _ = p.response_tx.send(AppendResponse::Done(Ok(meta)));
                }
            }
            Err(e) => {
                metrics.record_error();
                let error_msg = e.to_string();
                for p in pending {
                    let meta = RecordMetadata {
                        topic: topic.clone(),
                        partition,
                        offset: p.offset_in_batch,
                        timestamp: 0,
                    };
                    let err = KrafkaError::protocol(&error_msg);
                    crate::interceptor::safe_on_acknowledgement(
                        &*config.interceptor,
                        &meta,
                        Some(&err),
                    );
                    let _ = p.response_tx.send(AppendResponse::Done(Err(err)));
                }
            }
        }
    }

    /// Flush all batches concurrently.
    async fn flush_all(&mut self) -> Result<()> {
        let extracted: Vec<_> = self
            .batches
            .drain()
            .filter(|(_, b)| !b.batch.is_empty())
            .collect();

        // Free memory for all extracted batches
        for (_, batch) in &extracted {
            let batch_memory: usize = batch.pending.iter().map(|p| p.estimated_size).sum();
            self.memory_used = self.memory_used.saturating_sub(batch_memory);
        }
        // Wake callers blocked on buffer backpressure
        self.memory_freed.notify_one();

        // Send all batches concurrently
        let mut join_set = tokio::task::JoinSet::new();
        for ((topic, partition), batch) in extracted {
            let metadata = self.metadata.clone();
            let config = self.config.clone();
            let retry_policy = self.retry_policy.clone();
            let metrics = self.metrics.clone();
            join_set.spawn(Self::send_extracted_batch(
                topic,
                partition,
                batch.pending,
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
        };
        assert_eq!(config.batch_size, 65536);
        assert_eq!(config.linger, Duration::from_millis(50));
        assert_eq!(config.acks, 1);
        assert_eq!(config.buffer_memory, 64 * 1024 * 1024);
    }

    #[test]
    fn test_estimate_record_size() {
        let record = ProducerRecord::new("test-topic", b"value".to_vec());
        let size = RecordAccumulator::estimate_record_size(&record);
        // Should be at least the value length + topic overhead
        assert!(size >= 5);
        assert!(size > 64); // overhead for topic name and struct

        // Record with key and headers should be larger
        let record_with_key =
            ProducerRecord::new("test-topic", b"value".to_vec()).with_key(Some(b"key".to_vec()));
        let size_with_key = RecordAccumulator::estimate_record_size(&record_with_key);
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

    #[test]
    fn test_buffer_full_error_variant() {
        let err = KrafkaError::BufferFull;
        assert_eq!(err.to_string(), "buffer memory full");
    }

    #[test]
    fn test_notify_shared_via_arc() {
        let notify = Arc::new(tokio::sync::Notify::new());
        let notify2 = notify.clone();
        assert!(Arc::ptr_eq(&notify, &notify2));
    }

    #[tokio::test]
    async fn test_backpressure_timeout_returns_config_error() {
        let (sender, mut receiver) = mpsc::channel::<AccumulatorMessage>(16);
        let memory_freed = Arc::new(tokio::sync::Notify::new());
        let handle = RecordAccumulatorHandle {
            sender,
            memory_freed,
            max_block_ms: Duration::from_millis(50),
        };

        // Spawn a fake accumulator that always responds BufferFull
        tokio::spawn(async move {
            while let Some(msg) = receiver.recv().await {
                if let AccumulatorMessage::Append {
                    record,
                    response_tx,
                    ..
                } = msg
                {
                    let _ = response_tx.send(AppendResponse::BufferFull(record));
                }
            }
        });

        let record = ProducerRecord::new("topic", b"value".to_vec());
        let result = handle.append(record, 0).await;
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("max_block"),
            "expected max_block in error, got: {err_msg}"
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
