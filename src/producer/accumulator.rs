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
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio::time::interval;
use tracing::{debug, trace, warn};

use super::batch::ProducerBatch;
use super::record::{ProducerRecord, RecordMetadata};
use crate::PartitionId;
use crate::error::{KrafkaError, Result};
use crate::metadata::ClusterMetadata;
use crate::protocol::{
    ApiKey, Compression, ProducePartitionData, ProduceRequest, ProduceTopicData, RecordBatchBuilder,
};

/// Message sent to the accumulator background task.
#[derive(Debug)]
enum AccumulatorMessage {
    /// Add a record to the accumulator.
    Append {
        record: ProducerRecord,
        partition: PartitionId,
        response_tx: oneshot::Sender<Result<RecordMetadata>>,
    },
    /// Flush all batches.
    Flush {
        response_tx: oneshot::Sender<Result<()>>,
    },
    /// Shutdown the accumulator.
    Shutdown,
}

/// Handle to the record accumulator.
#[derive(Clone)]
pub struct RecordAccumulatorHandle {
    sender: mpsc::Sender<AccumulatorMessage>,
}

impl RecordAccumulatorHandle {
    /// Append a record to the accumulator.
    pub async fn append(
        &self,
        record: ProducerRecord,
        partition: PartitionId,
    ) -> Result<RecordMetadata> {
        let (response_tx, response_rx) = oneshot::channel();
        self.sender
            .send(AccumulatorMessage::Append {
                record,
                partition,
                response_tx,
            })
            .await
            .map_err(|_| KrafkaError::invalid_state("accumulator closed"))?;

        response_rx
            .await
            .map_err(|_| KrafkaError::invalid_state("accumulator response dropped"))?
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

    /// Shutdown the accumulator.
    pub async fn shutdown(&self) {
        let _ = self.sender.send(AccumulatorMessage::Shutdown).await;
    }
}

/// Configuration for the record accumulator.
#[derive(Debug, Clone)]
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
        }
    }
}

/// A pending record waiting for its batch to be sent.
struct PendingRecord {
    record: ProducerRecord,
    response_tx: oneshot::Sender<Result<RecordMetadata>>,
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
}

impl RecordAccumulator {
    /// Create a new record accumulator and return a handle.
    pub fn spawn(
        config: AccumulatorConfig,
        metadata: Arc<ClusterMetadata>,
    ) -> RecordAccumulatorHandle {
        let (sender, receiver) = mpsc::channel(1024);

        let accumulator = Self {
            config,
            batches: HashMap::new(),
            metadata,
            memory_used: 0,
        };

        tokio::spawn(accumulator.run(receiver));

        RecordAccumulatorHandle { sender }
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
                        Some(AccumulatorMessage::Shutdown) | None => {
                            debug!("Accumulator shutting down, flushing remaining batches");
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
        response_tx: oneshot::Sender<Result<RecordMetadata>>,
    ) {
        let topic = record.topic.clone();
        let key = (topic.clone(), partition);

        // Estimate record size for memory tracking
        let record_size = Self::estimate_record_size(&record);

        // Check memory limit before appending (0 = unlimited)
        if self.config.buffer_memory > 0
            && self.memory_used + record_size > self.config.buffer_memory
        {
            // Memory limit exceeded - return error
            // In a more sophisticated implementation, we could block and wait for memory
            let _ = response_tx.send(Err(KrafkaError::config(format!(
                "Buffer memory limit exceeded: {} + {} > {} bytes. \
                 Consider increasing buffer_memory or reducing production rate.",
                self.memory_used, record_size, self.config.buffer_memory
            ))));
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
                let _ =
                    response_tx.send(Err(KrafkaError::config("record too large for batch size")));
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

    /// Check for batches that have exceeded linger time.
    async fn check_linger_expiry(&mut self) {
        if self.config.linger.is_zero() {
            // Linger disabled, flush immediately
            self.flush_all_ready().await;
            return;
        }

        let keys_to_flush: Vec<_> = self
            .batches
            .iter()
            .filter(|(_, batch)| !batch.batch.is_empty() && batch.age() >= self.config.linger)
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys_to_flush {
            trace!("Linger expired for {:?}, flushing", key);
            self.flush_batch(&key).await;
        }
    }

    /// Flush all ready batches (non-empty with linger=0).
    async fn flush_all_ready(&mut self) {
        let keys_to_flush: Vec<_> = self
            .batches
            .iter()
            .filter(|(_, batch)| !batch.batch.is_empty())
            .map(|(key, _)| key.clone())
            .collect();

        for key in keys_to_flush {
            self.flush_batch(&key).await;
        }
    }

    /// Flush a specific batch.
    async fn flush_batch(&mut self, key: &(String, PartitionId)) {
        if let Some(accumulator_batch) = self.batches.remove(key) {
            if accumulator_batch.batch.is_empty() {
                return;
            }

            // Free memory for all records in this batch
            let batch_memory: usize = accumulator_batch
                .pending
                .iter()
                .map(|p| p.estimated_size)
                .sum();
            self.memory_used = self.memory_used.saturating_sub(batch_memory);

            let topic = &key.0;
            let partition = key.1;

            // Get connection to leader
            let conn_result = self.metadata.get_leader_connection(topic, partition).await;
            let conn = match conn_result {
                Ok(c) => c,
                Err(e) => {
                    // Fail all pending records with the same error message
                    let error_msg = format!("{}", e);
                    for pending in accumulator_batch.pending {
                        let _ = pending
                            .response_tx
                            .send(Err(KrafkaError::protocol(&error_msg)));
                    }
                    return;
                }
            };

            // Build record batch
            let mut batch_builder = RecordBatchBuilder::new().compression(self.config.compression);

            for pending in &accumulator_batch.pending {
                if pending.record.headers.is_empty() {
                    batch_builder = batch_builder.add_record(
                        pending.record.key.clone().map(Bytes::from),
                        Some(Bytes::from(pending.record.value.clone())),
                    );
                } else {
                    batch_builder = batch_builder.add_record_with_headers(
                        pending.record.key.clone().map(Bytes::from),
                        Some(Bytes::from(pending.record.value.clone())),
                        pending
                            .record
                            .headers
                            .clone()
                            .into_iter()
                            .map(|(k, v)| (k, Bytes::from(v)))
                            .collect(),
                    );
                }
            }

            let batch = batch_builder.build();
            let batch_bytes = match batch.encode() {
                Ok(b) => b,
                Err(e) => {
                    // Fail all pending records
                    let error_msg = format!("{}", e);
                    for pending in accumulator_batch.pending {
                        let _ = pending
                            .response_tx
                            .send(Err(KrafkaError::protocol(&error_msg)));
                    }
                    return;
                }
            };

            // Build produce request
            let request = ProduceRequest {
                transactional_id: None,
                acks: self.config.acks,
                timeout_ms: self.config.request_timeout.as_millis() as i32,
                topic_data: vec![ProduceTopicData {
                    name: topic.clone(),
                    partition_data: vec![ProducePartitionData {
                        index: partition,
                        records: batch_bytes,
                    }],
                }],
            };

            // Send request
            let response_result = conn
                .send_request(ApiKey::Produce, 0, |buf| {
                    request.encode_v0(buf);
                })
                .await;

            match response_result {
                Ok(mut response) => {
                    // Decode response
                    match crate::protocol::ProduceResponse::decode_v0(&mut response) {
                        Ok(produce_response) => {
                            // Find the partition response
                            let base_offset = produce_response
                                .responses
                                .iter()
                                .find(|r| r.name == *topic)
                                .and_then(|r| {
                                    r.partition_responses.iter().find(|p| p.index == partition)
                                })
                                .map(|p| {
                                    if !p.error_code.is_ok() {
                                        Err(format!(
                                            "broker error {:?}: {}-{}",
                                            p.error_code, topic, partition
                                        ))
                                    } else {
                                        Ok((p.base_offset, p.log_append_time_ms))
                                    }
                                });

                            // Complete pending records
                            match base_offset {
                                Some(Ok((base, timestamp))) => {
                                    for pending in accumulator_batch.pending {
                                        let _ = pending.response_tx.send(Ok(RecordMetadata {
                                            topic: topic.clone(),
                                            partition,
                                            offset: base + pending.offset_in_batch,
                                            timestamp,
                                        }));
                                    }
                                }
                                Some(Err(error_msg)) => {
                                    for pending in accumulator_batch.pending {
                                        let _ = pending
                                            .response_tx
                                            .send(Err(KrafkaError::protocol(&error_msg)));
                                    }
                                }
                                None => {
                                    for pending in accumulator_batch.pending {
                                        let _ =
                                            pending.response_tx.send(Err(KrafkaError::protocol(
                                                "partition not found in response",
                                            )));
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            let error_msg = format!("{}", e);
                            for pending in accumulator_batch.pending {
                                let _ = pending
                                    .response_tx
                                    .send(Err(KrafkaError::protocol(&error_msg)));
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!("Failed to send batch: {:?}", e);
                    let error_msg = format!("{}", e);
                    for pending in accumulator_batch.pending {
                        let _ = pending
                            .response_tx
                            .send(Err(KrafkaError::protocol(&error_msg)));
                    }
                }
            }
        }
    }

    /// Flush all batches.
    async fn flush_all(&mut self) -> Result<()> {
        let keys: Vec<_> = self.batches.keys().cloned().collect();
        for key in keys {
            self.flush_batch(&key).await;
        }
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
}
