//! Kafka producer implementation.
//!
//! This module provides:
//! - Async producer for sending messages
//! - Batching for performance with linger timer
//! - Compression support
//! - Partitioning strategies
//! - Retry handling with exponential backoff
//! - Idempotent production for exactly-once semantics
//! - Transactional production for exactly-once delivery

mod accumulator;
mod batch;
mod config;
mod idempotent;
mod partitioner;
mod record;
mod retry;
mod transaction;

pub use accumulator::{AccumulatorConfig, RecordAccumulator, RecordAccumulatorHandle};
pub use batch::ProducerBatch;
pub use config::{Acks, ProducerConfig, ProducerConfigBuilder};
pub use idempotent::{PartitionSequenceSnapshot, ProducerIdentity, ProducerIdentitySnapshot};
pub use partitioner::{
    DefaultPartitioner, HashPartitioner, Partitioner, RoundRobinPartitioner, StickyPartitioner,
    murmur2,
};
pub use record::{ProducerRecord, RecordMetadata};
pub use retry::{RetryContext, RetryPolicy};
pub use transaction::{
    TransactionState, TransactionalProducer, TransactionalProducerBuilder,
    TransactionalProducerConfig,
};

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::RwLock;
use tracing::info;

use crate::PartitionId;
use crate::error::{KrafkaError, Result};
use crate::metadata::ClusterMetadata;
use crate::network::{ConnectionConfig, ConnectionPool};
use crate::protocol::{
    ApiKey, Compression, ProducePartitionData, ProduceRequest, ProduceResponse, ProduceTopicData,
    RecordBatchBuilder,
};

/// A Kafka producer.
pub struct Producer {
    /// Producer configuration.
    config: ProducerConfig,
    /// Cluster metadata.
    metadata: Arc<ClusterMetadata>,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Partitioner.
    partitioner: Arc<dyn Partitioner>,
    /// Record accumulator for batching (when linger > 0).
    accumulator: Option<RecordAccumulatorHandle>,
    /// Pending batches by topic-partition (direct mode).
    #[allow(dead_code)]
    batches: RwLock<HashMap<(String, PartitionId), ProducerBatch>>,
    /// Whether the producer is closed.
    closed: std::sync::atomic::AtomicBool,
}

impl Producer {
    /// Create a new producer builder.
    pub fn builder() -> ProducerBuilder {
        ProducerBuilder::default()
    }

    /// Create a new producer with the given configuration.
    async fn new(config: ProducerConfig) -> Result<Self> {
        let pool_config = ConnectionConfig::builder()
            .client_id(&config.client_id)
            .request_timeout(config.request_timeout)
            .build();

        let pool = Arc::new(ConnectionPool::new(pool_config));

        // Parse bootstrap servers
        let bootstrap_servers: Vec<String> = config
            .bootstrap_servers
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        if bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("no bootstrap servers specified"));
        }

        let metadata = Arc::new(ClusterMetadata::new(
            bootstrap_servers,
            pool.clone(),
            config.metadata_max_age,
        ));

        // Initial metadata fetch
        metadata.refresh().await?;

        info!(
            "Producer initialized with {} brokers",
            metadata.brokers().await.len()
        );

        let partitioner: Arc<dyn Partitioner> = Arc::new(DefaultPartitioner::new());

        // Create accumulator if linger > 0 for batching
        let accumulator = if !config.linger.is_zero() {
            let acc_config = accumulator::AccumulatorConfig {
                batch_size: config.batch_size,
                linger: config.linger,
                compression: config.compression,
                acks: config.acks.to_i16(),
                request_timeout: config.request_timeout,
                buffer_memory: config.buffer_memory,
                max_block_ms: config.max_block,
            };
            Some(accumulator::RecordAccumulator::spawn(
                acc_config,
                metadata.clone(),
            ))
        } else {
            None
        };

        Ok(Self {
            config,
            metadata,
            pool,
            partitioner,
            accumulator,
            batches: RwLock::new(HashMap::new()),
            closed: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Send a record to a topic.
    ///
    /// This is the main method for producing messages.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use krafka::producer::Producer;
    /// # async fn example() -> Result<(), krafka::error::KrafkaError> {
    /// let producer = Producer::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .build()
    ///     .await?;
    ///
    /// let metadata = producer.send("my-topic", Some(b"key"), b"value").await?;
    /// println!("Sent to partition {} at offset {}", metadata.partition, metadata.offset);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn send(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: &[u8],
    ) -> Result<RecordMetadata> {
        let record = ProducerRecord::new(topic, value.to_vec()).with_key(key.map(|k| k.to_vec()));
        self.send_record(record).await
    }

    /// Send a record with headers.
    pub async fn send_with_headers(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: &[u8],
        headers: Vec<(String, Vec<u8>)>,
    ) -> Result<RecordMetadata> {
        let mut record =
            ProducerRecord::new(topic, value.to_vec()).with_key(key.map(|k| k.to_vec()));
        record.headers = headers;
        self.send_record(record).await
    }

    /// Send a producer record.
    pub async fn send_record(&self, record: ProducerRecord) -> Result<RecordMetadata> {
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(KrafkaError::invalid_state("producer is closed"));
        }

        let topic = record.topic.clone();

        // Determine partition
        let partition = match record.partition {
            Some(p) => p,
            None => {
                let partition_count =
                    self.metadata.partition_count(&topic).await.ok_or_else(|| {
                        KrafkaError::invalid_state(format!("unknown topic: {}", topic))
                    })?;
                self.partitioner
                    .partition(&topic, record.key.as_deref(), partition_count)
            }
        };

        // Use accumulator for batching if available (linger > 0)
        if let Some(ref accumulator) = self.accumulator {
            return accumulator.append(record, partition).await;
        }

        // Direct send (non-batched mode when linger = 0)
        self.send_to_partition(&topic, partition, record).await
    }

    /// Send a record to a specific partition.
    async fn send_to_partition(
        &self,
        topic: &str,
        partition: PartitionId,
        record: ProducerRecord,
    ) -> Result<RecordMetadata> {
        // Get connection to the leader
        let conn = self
            .metadata
            .get_leader_connection(topic, partition)
            .await?;

        // Build record batch
        let mut batch_builder = RecordBatchBuilder::new().compression(self.config.compression);

        if record.headers.is_empty() {
            batch_builder = batch_builder
                .add_record(record.key.map(Bytes::from), Some(Bytes::from(record.value)));
        } else {
            batch_builder = batch_builder.add_record_with_headers(
                record.key.map(Bytes::from),
                Some(Bytes::from(record.value)),
                record
                    .headers
                    .into_iter()
                    .map(|(k, v)| (k, Bytes::from(v)))
                    .collect(),
            );
        }

        let batch = batch_builder.build();
        let batch_bytes = batch.encode()?;

        // Build produce request
        let request = ProduceRequest {
            transactional_id: None,
            acks: self.config.acks.to_i16(),
            timeout_ms: self.config.request_timeout.as_millis() as i32,
            topic_data: vec![ProduceTopicData {
                name: topic.to_string(),
                partition_data: vec![ProducePartitionData {
                    index: partition,
                    records: batch_bytes,
                }],
            }],
        };

        // Send request
        let response = conn
            .send_request(ApiKey::Produce, 0, |buf| {
                request.encode_v0(buf);
            })
            .await?;

        // Decode response
        let mut buf = response;
        let produce_response = ProduceResponse::decode_v0(&mut buf)?;

        // Check for errors
        for topic_response in &produce_response.responses {
            for partition_response in &topic_response.partition_responses {
                if partition_response.index == partition {
                    if !partition_response.error_code.is_ok() {
                        return Err(KrafkaError::broker(
                            partition_response.error_code,
                            format!("produce failed for {}-{}", topic, partition),
                        ));
                    }

                    return Ok(RecordMetadata {
                        topic: topic.to_string(),
                        partition,
                        offset: partition_response.base_offset,
                        timestamp: partition_response.log_append_time_ms,
                    });
                }
            }
        }

        Err(KrafkaError::protocol("partition not found in response"))
    }

    /// Flush all pending records.
    pub async fn flush(&self) -> Result<()> {
        if let Some(ref accumulator) = self.accumulator {
            return accumulator.flush().await;
        }
        // In direct send mode, nothing to flush
        Ok(())
    }

    /// Close the producer.
    pub async fn close(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);

        // Shutdown accumulator first to flush pending records
        if let Some(ref accumulator) = self.accumulator {
            accumulator.shutdown().await;
        }

        self.pool.close_all().await;
        info!("Producer closed");
    }

    /// Check if the producer is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Get producer metrics.
    pub async fn metrics(&self) -> ProducerMetrics {
        ProducerMetrics {
            connections: self.pool.len().await,
        }
    }
}

/// Producer metrics.
#[derive(Debug, Clone)]
pub struct ProducerMetrics {
    /// Number of active connections.
    pub connections: usize,
}

/// Builder for creating producers.
#[must_use = "builders do nothing until .build() is called"]
#[derive(Default)]
pub struct ProducerBuilder {
    config: ProducerConfig,
}

impl ProducerBuilder {
    /// Set the bootstrap servers.
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    /// Set the client ID.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.config.client_id = client_id.into();
        self
    }

    /// Set the required acknowledgments.
    pub fn acks(mut self, acks: Acks) -> Self {
        self.config.acks = acks;
        self
    }

    /// Set the compression type.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.config.compression = compression;
        self
    }

    /// Set the batch size.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Set the linger time.
    pub fn linger(mut self, duration: Duration) -> Self {
        self.config.linger = duration;
        self
    }

    /// Set the request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set the number of retries.
    pub fn retries(mut self, retries: u32) -> Self {
        self.config.retries = retries;
        self
    }

    /// Set the retry backoff.
    pub fn retry_backoff(mut self, backoff: Duration) -> Self {
        self.config.retry_backoff = backoff;
        self
    }

    /// Enable idempotent producer.
    pub fn enable_idempotence(mut self, enable: bool) -> Self {
        self.config.enable_idempotence = enable;
        self
    }

    /// Build the producer.
    pub async fn build(self) -> Result<Producer> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap.servers is required"));
        }
        Producer::new(self.config).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_builder() {
        let builder = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .client_id("test")
            .acks(Acks::All)
            .compression(Compression::Gzip)
            .batch_size(32768)
            .linger(Duration::from_millis(10));

        assert_eq!(builder.config.bootstrap_servers, "localhost:9092");
        assert_eq!(builder.config.client_id, "test");
        assert_eq!(builder.config.acks, Acks::All);
        assert_eq!(builder.config.compression, Compression::Gzip);
        assert_eq!(builder.config.batch_size, 32768);
        assert_eq!(builder.config.linger, Duration::from_millis(10));
    }

    #[tokio::test]
    async fn test_producer_builder_no_servers() {
        let result = Producer::builder().build().await;
        assert!(result.is_err());
    }
}
