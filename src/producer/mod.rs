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

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::Semaphore;
use tracing::{debug, info};

use crate::PartitionId;
use crate::auth::AuthConfig;
use crate::error::{KrafkaError, Result};
use crate::metadata::ClusterMetadata;
use crate::metrics::ProducerMetrics as ProducerMetricsInner;
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
    /// Whether the producer is closed.
    closed: std::sync::atomic::AtomicBool,
    /// Retry policy for transient failures.
    retry_policy: RetryPolicy,
    /// Shared metrics.
    metrics: Arc<ProducerMetricsInner>,
    /// Semaphore limiting concurrent in-flight requests per producer.
    in_flight_semaphore: Arc<Semaphore>,
    /// Producer interceptor.
    interceptor: Arc<dyn crate::interceptor::ProducerInterceptor>,
}

impl Producer {
    /// Create a new producer builder.
    pub fn builder() -> ProducerBuilder {
        ProducerBuilder::default()
    }

    /// Create a new producer with the given configuration.
    async fn new(
        config: ProducerConfig,
        interceptor: Arc<dyn crate::interceptor::ProducerInterceptor>,
    ) -> Result<Self> {
        let mut pool_config_builder = ConnectionConfig::builder()
            .client_id(&config.client_id)
            .request_timeout(config.request_timeout);

        if let Some(ref auth) = config.auth {
            pool_config_builder = pool_config_builder.auth(auth.clone());
        }

        let pool_config = pool_config_builder.build();

        let pool = Arc::new(ConnectionPool::new(pool_config));

        // Parse bootstrap servers — filter out empty/whitespace entries
        let bootstrap_servers: Vec<String> = config
            .bootstrap_servers
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
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

        // Build retry policy from config
        let retry_policy = RetryPolicy::new()
            .with_max_retries(config.retries)
            .with_initial_backoff(config.retry_backoff)
            .with_max_backoff(Duration::from_secs(30));

        // Shared metrics
        let metrics = Arc::new(ProducerMetricsInner::default());

        // In-flight semaphore (shared between direct and batched send paths)
        let in_flight_semaphore = Arc::new(Semaphore::new(config.max_in_flight));

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
                in_flight_semaphore: in_flight_semaphore.clone(),
                interceptor: interceptor.clone(),
            };
            Some(accumulator::RecordAccumulator::spawn(
                acc_config,
                metadata.clone(),
                retry_policy.clone(),
                metrics.clone(),
            ))
        } else {
            None
        };

        Ok(Self {
            config: config.clone(),
            metadata,
            pool,
            partitioner,
            accumulator,
            closed: std::sync::atomic::AtomicBool::new(false),
            retry_policy,
            metrics,
            in_flight_semaphore,
            interceptor,
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
        let record = ProducerRecord::new(topic, Bytes::copy_from_slice(value))
            .with_key(key.map(Bytes::copy_from_slice));
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
        let mut record = ProducerRecord::new(topic, Bytes::copy_from_slice(value))
            .with_key(key.map(Bytes::copy_from_slice));
        record.headers = headers;
        self.send_record(record).await
    }

    /// Send a producer record.
    pub async fn send_record(&self, record: ProducerRecord) -> Result<RecordMetadata> {
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(KrafkaError::invalid_state("producer is closed"));
        }

        // Validate record fields against Kafka protocol wire-format limits
        record.validate()?;

        // Invoke interceptor before send
        let mut record = record;
        crate::interceptor::safe_on_send(&*self.interceptor, &mut record);

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
    ///
    /// Retries transient failures with exponential backoff.
    /// Triggers metadata refresh on leader-change errors.
    /// Limits concurrent in-flight requests via semaphore (max_in_flight).
    async fn send_to_partition(
        &self,
        topic: &str,
        partition: PartitionId,
        record: ProducerRecord,
    ) -> Result<RecordMetadata> {
        // Acquire in-flight permit before sending
        let _permit = self
            .in_flight_semaphore
            .acquire()
            .await
            .map_err(|_| KrafkaError::invalid_state("in-flight semaphore closed"))?;

        let mut retry_ctx = RetryContext::new(
            self.retry_policy.clone(),
            format!("produce({topic}-{partition})"),
        );

        loop {
            let result = self.do_send_to_partition(topic, partition, &record).await;
            match result {
                Ok(metadata) => {
                    retry_ctx.record_success();
                    self.metrics.record_send(
                        record.value.len() as u64
                            + record.key.as_ref().map(|k| k.len() as u64).unwrap_or(0),
                    );
                    self.metrics.connections.set(self.pool.len().await as u64);
                    crate::interceptor::safe_on_acknowledgement(
                        &*self.interceptor,
                        &metadata,
                        None,
                    );
                    return Ok(metadata);
                }
                Err(ref e) => {
                    self.metrics.record_error();

                    // Refresh metadata on leader-not-available / not-leader errors
                    if e.is_retriable() {
                        debug!(
                            topic = topic,
                            partition = partition,
                            error = %e,
                            "Transient error, refreshing metadata"
                        );
                        let _ = self.metadata.refresh_for_topics(Some(&[topic])).await;
                    }

                    if let Some(backoff) = retry_ctx.record_failure(e) {
                        self.metrics.retries.inc();
                        retry_ctx.wait(backoff).await;
                        continue;
                    }
                    // Final failure — notify interceptor
                    let err = result.unwrap_err();
                    let dummy_metadata = RecordMetadata {
                        topic: topic.to_string(),
                        partition,
                        offset: -1,
                        timestamp: 0,
                    };
                    crate::interceptor::safe_on_acknowledgement(
                        &*self.interceptor,
                        &dummy_metadata,
                        Some(&err),
                    );
                    return Err(err);
                }
            }
        }
    }

    /// Single attempt to send a record to a partition (no retry).
    async fn do_send_to_partition(
        &self,
        topic: &str,
        partition: PartitionId,
        record: &ProducerRecord,
    ) -> Result<RecordMetadata> {
        let _timer = self.metrics.send_latency.start();

        // Get connection to the leader
        let conn = self
            .metadata
            .get_leader_connection(topic, partition)
            .await?;

        // Build record batch
        let mut batch_builder = RecordBatchBuilder::new().compression(self.config.compression);

        // Propagate user-supplied timestamp to the batch
        if let Some(ts) = record.timestamp {
            batch_builder = batch_builder.base_timestamp(ts);
        }

        if record.headers.is_empty() {
            batch_builder =
                batch_builder.add_record(record.key.clone(), Some(record.value.clone()));
        } else {
            batch_builder = batch_builder.add_record_with_headers(
                record.key.clone(),
                Some(record.value.clone()),
                record
                    .headers
                    .iter()
                    .map(|(k, v)| (k.clone(), Bytes::from(v.clone())))
                    .collect(),
            );
        }

        let batch = batch_builder.build();
        let batch_bytes = batch.encode()?;

        // Build produce request
        let request = ProduceRequest {
            transactional_id: None,
            acks: self.config.acks.to_i16(),
            timeout_ms: crate::util::duration_to_millis_i32(self.config.request_timeout),
            topic_data: vec![ProduceTopicData {
                name: topic.to_string(),
                partition_data: vec![ProducePartitionData {
                    index: partition,
                    records: batch_bytes,
                }],
            }],
        };

        // acks=0 (fire-and-forget): Kafka sends no response, so don't wait for one (R6.1 fix)
        if self.config.acks == Acks::None {
            conn.send_fire_and_forget(ApiKey::Produce, 0, |buf| {
                request.encode_v0(buf);
            })
            .await?;

            return Ok(RecordMetadata {
                topic: topic.to_string(),
                partition,
                offset: -1, // Unknown — broker doesn't confirm
                timestamp: -1,
            });
        }

        // Send request and wait for response (acks=1 or acks=-1/all)
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

        // Notify interceptor of shutdown
        crate::interceptor::safe_producer_close(&*self.interceptor);

        self.pool.close_all().await;
        info!("Producer closed");
    }

    /// Check if the producer is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Get producer metrics.
    pub async fn metrics(&self) -> ProducerMetricsSnapshot {
        ProducerMetricsSnapshot {
            connections: self.pool.len().await,
            records_sent: self.metrics.records_sent.get(),
            bytes_sent: self.metrics.bytes_sent.get(),
            errors: self.metrics.errors.get(),
            retries: self.metrics.retries.get(),
        }
    }

    /// Get the shared metrics handle (for external monitoring).
    pub fn metrics_handle(&self) -> Arc<ProducerMetricsInner> {
        self.metrics.clone()
    }
}

/// Producer metrics snapshot.
#[derive(Debug, Clone)]
pub struct ProducerMetricsSnapshot {
    /// Number of active connections.
    pub connections: usize,
    /// Total records sent.
    pub records_sent: u64,
    /// Total bytes sent.
    pub bytes_sent: u64,
    /// Total errors.
    pub errors: u64,
    /// Total retries.
    pub retries: u64,
}

/// Builder for creating producers.
#[derive(Default)]
#[must_use = "builders do nothing until .build() is called"]
pub struct ProducerBuilder {
    config: ProducerConfig,
    interceptor: Option<Arc<dyn crate::interceptor::ProducerInterceptor>>,
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

    /// Set max in-flight requests per connection.
    ///
    /// Limits the number of concurrent produce requests sent to a single broker.
    /// Higher values increase throughput but can cause reordering under retries.
    /// Must be >= 1 (validated at build time). Default: 5.
    pub fn max_in_flight(mut self, max: usize) -> Self {
        self.config.max_in_flight = max;
        self
    }

    /// Set metadata max age before refresh.
    pub fn metadata_max_age(mut self, duration: Duration) -> Self {
        self.config.metadata_max_age = duration;
        self
    }

    /// Enable idempotent producer.
    ///
    /// **Note**: For full idempotent exactly-once semantics, use
    /// [`TransactionalProducer`] which handles producer ID initialization,
    /// epoch management, and sequence numbering automatically.
    ///
    /// Setting this on the regular `Producer` currently stores the config value
    /// but does not wire the full `InitProducerId` → PID/epoch → sequence flow.
    /// Use `TransactionalProducer` for production exactly-once guarantees.
    #[deprecated(
        since = "0.2.0",
        note = "Use TransactionalProducer for idempotent/exactly-once semantics"
    )]
    pub fn enable_idempotence(mut self, enable: bool) -> Self {
        self.config.enable_idempotence = enable;
        self
    }

    /// Set authentication configuration.
    ///
    /// Enables TLS and/or SASL authentication for all broker connections.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use krafka::producer::Producer;
    /// use krafka::auth::AuthConfig;
    ///
    /// let producer = Producer::builder()
    ///     .bootstrap_servers("broker:9093")
    ///     .auth(AuthConfig::sasl_plain("user", "password"))
    ///     .build()
    ///     .await?;
    /// ```
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = Some(auth);
        self
    }

    /// Configure SASL/PLAIN authentication.
    pub fn sasl_plain(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.config.auth = Some(AuthConfig::sasl_plain(username, password));
        self
    }

    /// Configure SASL/SCRAM-SHA-256 authentication.
    pub fn sasl_scram_sha256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_scram_sha256(username, password));
        self
    }

    /// Configure SASL/SCRAM-SHA-512 authentication.
    pub fn sasl_scram_sha512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_scram_sha512(username, password));
        self
    }

    /// Configure SASL/OAUTHBEARER authentication.
    ///
    /// Uses a static OAuth 2.0 bearer token. For token refresh, reconnect
    /// with a new token. For SASL extensions, use `.auth(AuthConfig::sasl_oauthbearer_token(...))`.
    pub fn sasl_oauthbearer(mut self, token: impl Into<String>) -> Self {
        self.config.auth = Some(AuthConfig::sasl_oauthbearer(token));
        self
    }

    /// Set a producer interceptor.
    ///
    /// The interceptor's `on_send` method is called before each record is sent,
    /// and `on_acknowledgement` is called after a send succeeds or fails.
    pub fn interceptor(
        mut self,
        interceptor: Arc<dyn crate::interceptor::ProducerInterceptor>,
    ) -> Self {
        self.interceptor = Some(interceptor);
        self
    }

    /// Build the producer.
    pub async fn build(self) -> Result<Producer> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap.servers is required"));
        }
        if self.config.max_in_flight == 0 {
            return Err(KrafkaError::config("max_in_flight must be >= 1"));
        }
        if self.config.batch_size == 0 {
            return Err(KrafkaError::config("batch_size must be >= 1"));
        }
        let interceptor: Arc<dyn crate::interceptor::ProducerInterceptor> = self
            .interceptor
            .unwrap_or_else(|| Arc::new(crate::interceptor::NoOpProducerInterceptor));
        let producer = Producer::new(self.config, interceptor).await?;
        Ok(producer)
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
        assert!(builder.config.auth.is_none());
    }

    #[test]
    fn test_producer_builder_with_auth() {
        let builder = Producer::builder()
            .bootstrap_servers("broker:9093")
            .auth(AuthConfig::sasl_plain("user", "pass"));

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(!auth.requires_tls());
        assert_eq!(
            auth.security_protocol,
            crate::auth::SecurityProtocol::SaslPlaintext
        );
        assert_eq!(auth.sasl_mechanism, Some(crate::auth::SaslMechanism::Plain));
    }

    #[test]
    fn test_producer_builder_aws_msk_iam() {
        let auth = AuthConfig::aws_msk_iam("AKID", "secret", "us-east-1");
        let builder = Producer::builder()
            .bootstrap_servers("broker:9094")
            .auth(auth);

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_tls());
        assert!(auth.requires_sasl());
        assert_eq!(
            auth.sasl_mechanism,
            Some(crate::auth::SaslMechanism::AwsMskIam)
        );
        assert!(auth.aws_msk_iam_credentials.is_some());
        assert!(auth.tls_config.is_some());
    }

    #[test]
    fn test_producer_builder_no_auth_by_default() {
        let builder = Producer::builder().bootstrap_servers("broker:9092");

        assert!(builder.config.auth.is_none());
    }

    #[test]
    fn test_producer_builder_sasl_plain() {
        let builder = Producer::builder()
            .bootstrap_servers("broker:9093")
            .sasl_plain("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.plain_credentials.is_some());
    }

    #[test]
    fn test_producer_builder_sasl_scram() {
        let builder = Producer::builder()
            .bootstrap_servers("broker:9093")
            .sasl_scram_sha256("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.scram_credentials.is_some());

        let builder = Producer::builder()
            .bootstrap_servers("broker:9093")
            .sasl_scram_sha512("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.scram_credentials.is_some());
    }

    #[tokio::test]
    async fn test_producer_builder_no_servers() {
        let result = Producer::builder().build().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_producer_builder_retry_config() {
        let builder = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .retries(5)
            .retry_backoff(Duration::from_millis(200));

        assert_eq!(builder.config.retries, 5);
        assert_eq!(builder.config.retry_backoff, Duration::from_millis(200));
    }

    #[test]
    fn test_producer_metrics_snapshot() {
        let snapshot = ProducerMetricsSnapshot {
            connections: 3,
            records_sent: 100,
            bytes_sent: 50000,
            errors: 2,
            retries: 5,
        };
        assert_eq!(snapshot.connections, 3);
        assert_eq!(snapshot.records_sent, 100);
        assert_eq!(snapshot.bytes_sent, 50000);
        assert_eq!(snapshot.errors, 2);
        assert_eq!(snapshot.retries, 5);
    }

    #[test]
    fn test_retry_policy_from_config() {
        let policy = RetryPolicy::new()
            .with_max_retries(10)
            .with_initial_backoff(Duration::from_millis(50))
            .with_max_backoff(Duration::from_secs(30));

        assert_eq!(policy.max_retries, 10);
        assert_eq!(policy.initial_backoff, Duration::from_millis(50));
        assert_eq!(policy.max_backoff, Duration::from_secs(30));
    }

    #[test]
    fn test_producer_config_max_in_flight_default() {
        let config = ProducerConfig::default();
        // Default max_in_flight should be a reasonable value > 0
        assert!(config.max_in_flight > 0);
    }

    #[test]
    fn test_acks_none_returns_fire_and_forget_metadata() {
        // Verify that acks=0 configuration is correctly set.
        // The actual fire-and-forget behavior is tested via integration tests,
        // but we can verify the config pipeline here.
        let builder = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .acks(Acks::None);

        assert_eq!(builder.config.acks, Acks::None);
        assert_eq!(builder.config.acks.to_i16(), 0);
    }

    #[test]
    #[allow(deprecated)]
    fn test_enable_idempotence_deprecated() {
        // Verify the deprecated method still works (sets the flag)
        let builder = Producer::builder()
            .bootstrap_servers("broker:9092")
            .enable_idempotence(true);
        assert!(builder.config.enable_idempotence);
    }

    #[tokio::test]
    async fn test_producer_builder_rejects_zero_max_in_flight() {
        let mut builder = Producer::builder().bootstrap_servers("localhost:9092");
        builder.config.max_in_flight = 0;
        let result = builder.build().await;

        match result {
            Err(e) => assert!(e.to_string().contains("max_in_flight")),
            Ok(_) => panic!("expected error for max_in_flight=0"),
        }
    }

    #[tokio::test]
    async fn test_producer_builder_rejects_zero_batch_size() {
        let mut builder = Producer::builder().bootstrap_servers("localhost:9092");
        builder.config.batch_size = 0;
        let result = builder.build().await;

        match result {
            Err(e) => assert!(e.to_string().contains("batch_size")),
            Ok(_) => panic!("expected error for batch_size=0"),
        }
    }

    #[test]
    fn test_producer_builder_interceptor() {
        use crate::interceptor::ProducerInterceptor;

        #[derive(Debug)]
        struct TestInterceptor;
        impl ProducerInterceptor for TestInterceptor {
            fn on_send(&self, _record: &mut ProducerRecord) {}
        }

        let builder = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .interceptor(Arc::new(TestInterceptor));

        assert!(builder.interceptor.is_some());
    }
}
