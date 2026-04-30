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
mod barrier;
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
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::Semaphore;
use tracing::{debug, info, warn};

use crate::PartitionId;
use crate::auth::AuthConfig;
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::metadata::ClusterMetadata;
use crate::metrics::{ConnectionMetrics, ProducerMetrics as ProducerMetricsInner};
use crate::network::{ConnectionConfig, ConnectionPool};
use crate::protocol::{
    ApiKey, Compression, InitProducerIdRequest, InitProducerIdResponse, ProducePartitionData,
    ProduceRequest, ProduceResponse, ProduceTopicData, RecordBatchBuilder, VersionedDecode,
    VersionedEncode, versions,
};

use self::barrier::InFlightBarrier;

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
    /// Barrier over all started send operations and shutdown state.
    in_flight_barrier: Arc<InFlightBarrier>,
    /// Retry policy for transient failures.
    retry_policy: RetryPolicy,
    /// Shared metrics.
    metrics: Arc<ProducerMetricsInner>,
    /// Semaphore limiting concurrent in-flight requests per producer.
    in_flight_semaphore: Arc<Semaphore>,
    /// Producer interceptor.
    interceptor: Arc<dyn crate::interceptor::ProducerInterceptor>,
    /// Producer identity for idempotent production (PID, epoch, sequences).
    identity: Option<Arc<ProducerIdentity>>,
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
        partitioner: Option<Arc<dyn Partitioner>>,
    ) -> Result<Self> {
        let mut pool_config_builder = ConnectionConfig::builder()
            .client_id(&config.client_id)
            .request_timeout(config.request_timeout);

        if let Some(ref auth) = config.auth {
            pool_config_builder = pool_config_builder.auth(auth.clone());
        }

        #[cfg(feature = "socks5")]
        if let Some(ref proxy) = config.proxy {
            pool_config_builder = pool_config_builder.proxy(proxy.clone());
        }

        let mut pool_config = pool_config_builder.build();
        pool_config.init_tls().await?;

        let pool = Arc::new(ConnectionPool::new(pool_config));
        pool.start_idle_evictor();

        let bootstrap_servers = crate::util::parse_bootstrap_servers(&config.bootstrap_servers)?;

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

        info!(
            "Producer initialized with {} brokers",
            metadata.brokers().len()
        );

        // Initialize idempotent producer identity (PID + epoch) if enabled.
        let identity = if config.idempotent {
            let identity = Arc::new(ProducerIdentity::new());
            Self::init_producer_id(&identity, &metadata, &pool, &config).await?;
            Some(identity)
        } else {
            None
        };

        let partitioner: Arc<dyn Partitioner> =
            partitioner.unwrap_or_else(|| Arc::new(DefaultPartitioner::new()));

        // Build retry policy from config
        let retry_policy = RetryPolicy::new()
            .with_max_retries(config.retries)
            .with_initial_backoff(config.retry_backoff)
            .with_max_backoff(Duration::from_secs(30))
            .with_delivery_timeout(Some(config.delivery_timeout));

        // Shared metrics
        let metrics = Arc::new(ProducerMetricsInner::default());

        // In-flight semaphore (shared between direct and batched send paths)
        let in_flight_semaphore = Arc::new(Semaphore::new(config.max_in_flight));
        let in_flight_barrier = Arc::new(InFlightBarrier::new());

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
                identity: identity.clone(),
            };
            Some(accumulator::RecordAccumulator::spawn(
                acc_config,
                metadata.clone(),
                retry_policy.clone(),
                metrics.clone(),
                in_flight_barrier.clone(),
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
            in_flight_barrier,
            retry_policy,
            metrics,
            in_flight_semaphore,
            interceptor,
            identity,
        })
    }

    /// Obtain a producer ID and epoch via `InitProducerId`.
    ///
    /// Retries on retriable errors (e.g. `CoordinatorLoadInProgress`) with
    /// exponential backoff and jitter, rotating through available brokers on
    /// each attempt so that transient per-broker failures are tolerated.
    async fn init_producer_id(
        identity: &ProducerIdentity,
        metadata: &ClusterMetadata,
        pool: &ConnectionPool,
        config: &ProducerConfig,
    ) -> Result<()> {
        let retry_policy = RetryPolicy::new()
            .with_max_retries(config.retries)
            .with_initial_backoff(config.retry_backoff)
            .with_max_backoff(Duration::from_secs(10))
            .with_delivery_timeout(Some(config.delivery_timeout));

        let started_at = Instant::now();

        for attempt in 0..=retry_policy.max_retries {
            if let Some(deadline) = retry_policy.delivery_timeout
                && started_at.elapsed() >= deadline
            {
                return Err(KrafkaError::timeout("InitProducerId"));
            }

            if attempt > 0 {
                let mut backoff = retry_policy.calculate_backoff(attempt);
                if let Some(deadline) = retry_policy.delivery_timeout {
                    let elapsed = started_at.elapsed();
                    if elapsed >= deadline {
                        return Err(KrafkaError::timeout("InitProducerId"));
                    }
                    backoff = backoff.min(deadline.saturating_sub(elapsed));
                }
                if !backoff.is_zero() {
                    tokio::time::sleep(backoff).await;
                }
            }
            if let Some(deadline) = retry_policy.delivery_timeout
                && started_at.elapsed() >= deadline
            {
                return Err(KrafkaError::timeout("InitProducerId"));
            }

            let brokers = metadata.brokers();
            if brokers.is_empty() {
                if attempt < retry_policy.max_retries {
                    warn!(attempt, "No brokers available for InitProducerId, retrying");
                    continue;
                }
                return Err(KrafkaError::protocol(
                    "no brokers available for InitProducerId",
                ));
            }

            // Rotate through brokers across attempts.
            let broker = &brokers[attempt as usize % brokers.len()];
            let conn = match pool.get_connection_by_id(broker.id, broker.address()).await {
                Ok(c) => c,
                Err(e) if e.is_retriable() && attempt < retry_policy.max_retries => {
                    warn!(
                        attempt,
                        error = %e,
                        "Connection failed for InitProducerId, retrying"
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };

            let ip_version = match conn
                .negotiate_api_version(
                    ApiKey::InitProducerId,
                    versions::INIT_PRODUCER_ID_MAX,
                    versions::INIT_PRODUCER_ID_MIN,
                )
                .await
            {
                Some(v) => v,
                None => {
                    return Err(KrafkaError::protocol(
                        "no mutually supported InitProducerId API version",
                    ));
                }
            };

            let request = InitProducerIdRequest::idempotent();
            let response_bytes = match conn
                .send_request(ApiKey::InitProducerId, ip_version, |buf| {
                    request.encode_versioned(ip_version, buf)
                })
                .await
            {
                Ok(b) => b,
                Err(e) if e.is_retriable() && attempt < retry_policy.max_retries => {
                    warn!(
                        attempt,
                        error = %e,
                        "InitProducerId request failed, retrying"
                    );
                    continue;
                }
                Err(e) => return Err(e),
            };

            let mut buf = response_bytes;
            let response = InitProducerIdResponse::decode_versioned(ip_version, &mut buf)?;

            if response.is_ok() {
                identity.initialize(response.producer_id, response.producer_epoch);
                info!(
                    "Idempotent producer initialized: PID={}, epoch={}",
                    response.producer_id, response.producer_epoch
                );
                return Ok(());
            }

            if response.error_code.is_retriable() && attempt < retry_policy.max_retries {
                warn!(
                    error_code = ?response.error_code,
                    attempt,
                    "InitProducerId returned retriable error, retrying"
                );
            } else {
                return Err(KrafkaError::broker(
                    response.error_code,
                    "failed to initialize producer ID",
                ));
            }
        }

        Err(KrafkaError::protocol(format!(
            "InitProducerId retry loop exhausted after {} retries",
            retry_policy.max_retries
        )))
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
        let mut record = ProducerRecord::new(topic, Bytes::copy_from_slice(value));
        if let Some(k) = key {
            record = record.with_key(Bytes::copy_from_slice(k));
        }
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
        let mut record = ProducerRecord::new(topic, Bytes::copy_from_slice(value));
        if let Some(k) = key {
            record = record.with_key(Bytes::copy_from_slice(k));
        }
        record.headers = headers;
        self.send_record(record).await
    }

    /// Send a producer record.
    pub async fn send_record(&self, record: ProducerRecord) -> Result<RecordMetadata> {
        let operation_guard = self.in_flight_barrier.start("producer")?;

        // Invoke interceptor before send
        let mut record = record;
        crate::interceptor::safe_on_send(&*self.interceptor, &mut record);

        // Validate record fields against Kafka protocol wire-format limits.
        // Runs after the interceptor since interceptors can mutate the record.
        record.validate()?;

        let topic = record.topic.clone();

        // Determine partition
        let partition = match record.partition {
            Some(p) => p,
            None => {
                let partition_count = self
                    .metadata
                    .partition_count(&topic)
                    .ok_or_else(|| KrafkaError::invalid_state(format!("unknown topic: {topic}")))?;
                self.partitioner
                    .partition(&topic, record.key.as_deref(), partition_count)
            }
        };

        // Use accumulator for batching if available (linger > 0)
        if let Some(ref accumulator) = self.accumulator {
            return accumulator
                .append_with_guard(record, partition, operation_guard)
                .await;
        }

        // Direct send (non-batched mode when linger = 0)
        let _operation_guard = operation_guard;
        self.send_to_partition(&topic, partition, record).await
    }

    /// Send a record to a specific partition.
    ///
    /// Retries transient failures with exponential backoff.
    /// Triggers metadata refresh on leader-change errors.
    /// Limits concurrent in-flight requests via semaphore (max_in_flight).
    /// For idempotent producers, tracks sequence numbers and handles
    /// `OutOfOrderSequenceNumber` with sequence reset + batch rebuild.
    async fn send_to_partition(
        &self,
        topic: &str,
        partition: PartitionId,
        record: ProducerRecord,
    ) -> Result<RecordMetadata> {
        // Build the owned topic string once for RecordMetadata construction,
        // avoiding repeated allocations in the retry loop.
        let topic_owned = topic.to_string();

        // Acquire in-flight permit before sending
        let _permit = self
            .in_flight_semaphore
            .acquire()
            .await
            .map_err(|_| KrafkaError::invalid_state("in-flight semaphore closed"))?;

        // Allocate sequence for idempotent production (before retry loop — retries
        // must resend the same sequence for the broker to de-duplicate).
        let mut sequence: Option<i32> = match self
            .identity
            .as_ref()
            .map(|id| id.next_sequence(topic, partition))
            .transpose()
        {
            Ok(s) => s,
            Err(e) => return Err(e),
        };

        // Build the produce request once (reused across retries).
        let mut request = match self.build_produce_request(topic, partition, &record, sequence) {
            Ok(r) => r,
            Err(e) => {
                if let Some(ref identity) = self.identity {
                    let _ = identity.rollback_sequence(topic, partition);
                }
                return Err(e);
            }
        };

        let mut retry_ctx = RetryContext::new(
            self.retry_policy.clone(),
            format!("produce({topic}-{partition})"),
        );

        loop {
            let result = self.do_send(topic, partition, &request).await;
            // Convert DuplicateSequenceNumber to success — the broker
            // already committed this batch (idempotent dedup worked).
            let result = if let Err(KrafkaError::Broker { code, .. }) = &result
                && *code == ErrorCode::DuplicateSequenceNumber
                && self.identity.is_some()
            {
                debug!(
                    topic = topic,
                    partition = partition,
                    "DuplicateSequenceNumber — dedup confirmed"
                );
                Ok(RecordMetadata {
                    topic: topic_owned.clone(),
                    partition,
                    offset: -1,
                    timestamp: -1,
                })
            } else {
                result
            };

            match result {
                Ok(metadata) => {
                    retry_ctx.record_success();

                    // Acknowledge sequence on success
                    if let (Some(identity), Some(seq)) = (&self.identity, sequence) {
                        identity.acknowledge(topic, partition, seq);
                    }

                    self.metrics.record_send(
                        record.value.len() as u64
                            + record.key.as_ref().map(|k| k.len() as u64).unwrap_or(0),
                    );
                    self.metrics.connections.set(self.pool.len() as u64);
                    crate::interceptor::safe_on_acknowledgement(
                        &*self.interceptor,
                        &metadata,
                        None,
                    );
                    return Ok(metadata);
                }
                Err(e) => {
                    // OutOfOrderSequenceNumber: atomically reset the
                    // partition sequence and rebuild the batch with a fresh
                    // sequence before retrying. Skip the metadata refresh —
                    // OOSN is a sequence mismatch, not a leader-change error.
                    if let KrafkaError::Broker { code, .. } = &e
                        && *code == ErrorCode::OutOfOrderSequenceNumber
                        && let Some(ref identity) = self.identity
                    {
                        warn!(
                            topic = topic,
                            partition = partition,
                            "OutOfOrderSequenceNumber, resetting sequence and rebuilding batch"
                        );
                        let new_seq = match identity.reset_and_allocate(topic, partition, 1) {
                            Ok(s) => s,
                            Err(e) => {
                                self.metrics.record_error();
                                return Err(e);
                            }
                        };
                        sequence = Some(new_seq);
                        match self.build_produce_request(topic, partition, &record, sequence) {
                            Ok(r) => request = r,
                            Err(build_err) => {
                                // Rollback the freshly allocated sequence
                                let _ = identity.rollback_sequence(topic, partition);
                                self.metrics.record_error();
                                let dummy_metadata = RecordMetadata {
                                    topic: topic_owned.clone(),
                                    partition,
                                    offset: -1,
                                    timestamp: 0,
                                };
                                crate::interceptor::safe_on_acknowledgement(
                                    &*self.interceptor,
                                    &dummy_metadata,
                                    Some(&build_err),
                                );
                                return Err(build_err);
                            }
                        }
                        // Fall through to retry logic (OOSN is retriable)
                    } else if e.is_retriable() {
                        // Refresh metadata on leader-not-available / not-leader errors
                        debug!(
                            topic = topic,
                            partition = partition,
                            error = %e,
                            "Transient error, refreshing metadata"
                        );
                        if let Err(refresh_err) =
                            self.metadata.refresh_for_topics(Some(&[topic])).await
                        {
                            debug!(error = %refresh_err, "Metadata refresh failed during retry");
                        }
                    }

                    if let Some(backoff) = retry_ctx.record_failure(&e) {
                        self.metrics.retries.inc();
                        retry_ctx.wait(backoff).await;
                        continue;
                    }
                    // Final failure — rollback unused sequence so the next
                    // send doesn't trigger an unnecessary OOSN round-trip.
                    if let Some(ref identity) = self.identity {
                        let _ = identity.rollback_sequence(topic, partition);
                    }
                    self.metrics.record_error();
                    let dummy_metadata = RecordMetadata {
                        topic: topic_owned.clone(),
                        partition,
                        offset: -1,
                        timestamp: 0,
                    };
                    crate::interceptor::safe_on_acknowledgement(
                        &*self.interceptor,
                        &dummy_metadata,
                        Some(&e),
                    );
                    return Err(e);
                }
            }
        }
    }

    /// Build a produce request for a single record.
    ///
    /// When `sequence` is `Some`, the batch is tagged with the idempotent
    /// producer identity (PID, epoch, base_sequence).
    fn build_produce_request(
        &self,
        topic: &str,
        partition: PartitionId,
        record: &ProducerRecord,
        sequence: Option<i32>,
    ) -> Result<ProduceRequest> {
        let mut batch_builder = RecordBatchBuilder::new().compression(self.config.compression);

        // Propagate user-supplied timestamp to the batch
        if let Some(ts) = record.timestamp {
            batch_builder = batch_builder.base_timestamp(ts);
        }

        // Tag with idempotent producer identity
        if let (Some(identity), Some(seq)) = (&self.identity, sequence) {
            batch_builder =
                batch_builder.producer(identity.producer_id(), identity.producer_epoch(), seq);
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

        Ok(ProduceRequest {
            transactional_id: None,
            acks: self.config.acks.to_i16(),
            timeout_ms: crate::util::duration_to_millis_i32(self.config.request_timeout),
            topic_data: vec![ProduceTopicData {
                name: topic.to_string(),
                topic_id: None,
                partition_data: vec![ProducePartitionData {
                    index: partition,
                    records: batch_bytes,
                }],
            }],
        })
    }

    /// Single attempt to send a pre-built produce request to a partition.
    async fn do_send(
        &self,
        topic: &str,
        partition: PartitionId,
        request: &ProduceRequest,
    ) -> Result<RecordMetadata> {
        let _timer = self.metrics.send_latency.start();

        // Get connection to the leader
        let conn = self
            .metadata
            .get_leader_connection(topic, partition)
            .await?;

        // Negotiate Produce version for this broker.
        let version = conn
            .negotiate_api_version(
                ApiKey::Produce,
                versions::PRODUCE_MAX,
                versions::PRODUCE_MIN,
            )
            .await
            .ok_or_else(|| KrafkaError::protocol("no mutually supported Produce API version"))?;

        // acks=0 (fire-and-forget): Kafka sends no response, so don't wait for one (R6.1 fix)
        if self.config.acks == Acks::None {
            conn.send_fire_and_forget(ApiKey::Produce, version, |buf| {
                request.encode_versioned(version, buf)
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
            .send_request(ApiKey::Produce, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        // Decode response
        let mut buf = response;
        let produce_response = ProduceResponse::decode_versioned(version, &mut buf)?;

        // Check for errors
        for topic_response in &produce_response.responses {
            for partition_response in &topic_response.partition_responses {
                if partition_response.index == partition {
                    if !partition_response.error_code.is_ok() {
                        return Err(KrafkaError::broker(
                            partition_response.error_code,
                            format!("produce failed for {topic}-{partition}"),
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
        let target = self.in_flight_barrier.snapshot();
        if let Some(ref accumulator) = self.accumulator {
            accumulator.flush().await?;
        }

        self.in_flight_barrier.wait_for(target).await;
        Ok(())
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

    /// Force a rebootstrap: close all connections, clear the metadata cache,
    /// and fall back to bootstrap servers (KIP-899).
    pub async fn rebootstrap(&self) {
        self.metadata.rebootstrap().await;
    }

    /// Close the producer.
    ///
    /// Flushes pending records, notifies interceptors, and tears down connections.
    /// Calling `close()` more than once is a no-op.
    pub async fn close(&self) {
        let _ = self.close_inner(None).await;
    }

    /// Close the producer, giving up on graceful shutdown once `timeout` expires.
    ///
    /// If the timeout elapses before all started sends cross the acknowledgment
    /// boundary, remaining work is failed by tearing down the connection pool.
    /// Batches in retry backoff will be aborted without notification.
    pub async fn close_with_timeout(&self, timeout: Duration) -> Result<()> {
        self.close_inner(Some(timeout)).await
    }

    async fn close_inner(&self, timeout: Option<Duration>) -> Result<()> {
        let Some(target) = self.in_flight_barrier.begin_close() else {
            return Ok(());
        };

        let graceful_close = async {
            // Shutdown accumulator first to flush pending records.
            if let Some(ref accumulator) = self.accumulator
                && let Err(e) = accumulator.shutdown().await
            {
                warn!("Accumulator shutdown error during close: {e}");
            }

            self.in_flight_barrier.wait_for(target).await;
        };

        let close_result = if let Some(timeout) = timeout {
            tokio::time::timeout(timeout, graceful_close)
                .await
                .map_err(|_| {
                    warn!("Producer close timed out; batches in retry backoff may be lost");
                    KrafkaError::timeout("producer close")
                })
        } else {
            graceful_close.await;
            Ok(())
        };

        // Notify interceptor of shutdown
        crate::interceptor::safe_producer_close(&*self.interceptor);

        self.pool.close_all().await;
        info!("Producer closed");

        close_result
    }

    /// Check if the producer is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.in_flight_barrier.is_closing()
    }

    /// Get producer metrics.
    pub async fn metrics(&self) -> ProducerMetricsSnapshot {
        ProducerMetricsSnapshot {
            connections: self.pool.len(),
            records_sent: self.metrics.records_sent.get(),
            bytes_sent: self.metrics.bytes_sent.get(),
            errors: self.metrics.errors.get(),
            retries: self.metrics.retries.get(),
        }
    }

    /// Get the shared metrics handle (for external monitoring).
    #[inline]
    pub fn metrics_handle(&self) -> Arc<ProducerMetricsInner> {
        self.metrics.clone()
    }

    /// Get the shared connection metrics handle used by this producer's broker pool.
    #[inline]
    pub fn connection_metrics(&self) -> Arc<ConnectionMetrics> {
        self.pool.metrics()
    }
}

impl Drop for Producer {
    fn drop(&mut self) {
        // Warn when a producer is dropped without an explicit `close()`:
        // unacked batches sitting in the accumulator or retry backoff
        // are discarded silently. Skip during panic unwinding.
        if !self.in_flight_barrier.is_closing() && !std::thread::panicking() {
            warn!(
                "Producer dropped without close(); in-flight batches may be lost. \
                 Call `Producer::close()` (or `close_with_timeout`) before drop to flush."
            );
        }
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
    interceptors: Vec<Arc<dyn crate::interceptor::ProducerInterceptor>>,
    partitioner: Option<Arc<dyn Partitioner>>,
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

    /// Set the total delivery timeout.
    ///
    /// Bounds the total time a record may spend queued and retried before it
    /// fails locally. Default: 120 seconds.
    pub fn delivery_timeout(mut self, timeout: Duration) -> Self {
        self.config.delivery_timeout = timeout;
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

    /// Set the topic cache TTL for partial metadata refreshes.
    ///
    /// During partial refreshes, cached topics that have not been refreshed
    /// within this duration are evicted to prevent unbounded cache growth.
    ///
    /// Default: 5 minutes (matching Java's `metadata.max.idle.ms`).
    pub fn metadata_topic_cache_ttl(mut self, ttl: Duration) -> Self {
        self.config.metadata_topic_cache_ttl = Some(ttl);
        self
    }

    /// Disable topic cache TTL eviction for partial metadata refreshes.
    ///
    /// By default, cached topics are evicted after 5 minutes to prevent
    /// unbounded growth on topic churn. Call this to opt out of TTL eviction;
    /// entries will then persist across partial refreshes indefinitely.
    pub fn disable_metadata_topic_cache_ttl(mut self) -> Self {
        self.config.metadata_topic_cache_ttl = None;
        self
    }

    /// Enable or disable idempotent production.
    ///
    /// Idempotent production is enabled by default (matching KIP-679 / Kafka 3.0+).
    /// When enabled, the producer obtains a Producer ID from the broker and
    /// attaches sequence numbers to every batch, allowing the broker to
    /// de-duplicate retries.
    ///
    /// Requires `acks = All` and `max_in_flight <= 5`.
    pub fn idempotent(mut self, enable: bool) -> Self {
        self.config.idempotent = enable;
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
    ///     .auth(AuthConfig::sasl_plain("user", "password")?)
    ///     .build()
    ///     .await?;
    /// ```
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = Some(auth);
        self
    }

    /// Configure SASL/PLAIN authentication.
    pub fn sasl_plain(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> crate::Result<Self> {
        self.config.auth = Some(AuthConfig::sasl_plain(username, password)?);
        Ok(self)
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

    /// Configure SASL/OAUTHBEARER authentication with a static token.
    ///
    /// For automatic token refresh, use [`sasl_oauthbearer_provider()`](Self::sasl_oauthbearer_provider).
    /// For SASL extensions, use `.auth(AuthConfig::sasl_oauthbearer_token(...))`.
    pub fn sasl_oauthbearer(mut self, token: impl Into<String>) -> Self {
        self.config.auth = Some(AuthConfig::sasl_oauthbearer(token));
        self
    }

    /// Configure SASL/OAUTHBEARER authentication with an async token provider.
    ///
    /// The provider is called on every new broker connection, ensuring
    /// tokens are always fresh.
    pub fn sasl_oauthbearer_provider(
        mut self,
        provider: impl crate::auth::OAuthBearerTokenProvider + 'static,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_oauthbearer_provider(provider));
        self
    }

    /// Set a custom partitioner.
    ///
    /// The partitioner determines which partition a record is sent to when
    /// [`ProducerRecord::partition`] is `None`. The default is
    /// [`DefaultPartitioner`] (murmur2 hash for keyed records, round-robin
    /// for null keys — matching the Java Kafka client).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use krafka::producer::{Producer, Partitioner, RoundRobinPartitioner};
    ///
    /// let producer = Producer::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .partitioner(RoundRobinPartitioner::new())
    ///     .build()
    ///     .await?;
    /// ```
    pub fn partitioner(mut self, partitioner: impl Partitioner + 'static) -> Self {
        self.partitioner = Some(Arc::new(partitioner));
        self
    }

    /// Set a producer interceptor, replacing any previously added interceptors.
    ///
    /// The interceptor's `on_send` method is called before each record is sent,
    /// and `on_acknowledgement` is called after a send succeeds or fails.
    ///
    /// To register multiple interceptors as an ordered chain, use
    /// [`add_interceptor`](Self::add_interceptor) instead.
    pub fn interceptor(
        mut self,
        interceptor: Arc<dyn crate::interceptor::ProducerInterceptor>,
    ) -> Self {
        self.interceptors = vec![interceptor];
        self
    }

    /// Append a producer interceptor to the chain.
    ///
    /// Interceptors execute in the order they are added. Each interceptor is
    /// individually panic-isolated — a panic in one will not prevent the
    /// remaining interceptors from running.
    ///
    /// For `on_send`, each interceptor sees the record as modified by all
    /// preceding interceptors.
    pub fn add_interceptor(
        mut self,
        interceptor: Arc<dyn crate::interceptor::ProducerInterceptor>,
    ) -> Self {
        self.interceptors.push(interceptor);
        self
    }

    /// Build the producer.
    pub async fn build(self) -> Result<Producer> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap.servers is required"));
        }
        if self.config.max_in_flight == 0 {
            return Err(KrafkaError::config(format!(
                "max_in_flight must be >= 1 (got {})",
                self.config.max_in_flight
            )));
        }
        if self.config.batch_size == 0 {
            return Err(KrafkaError::config(format!(
                "batch_size must be >= 1 (got {})",
                self.config.batch_size
            )));
        }
        if self.config.delivery_timeout.is_zero() {
            return Err(KrafkaError::config(
                "delivery_timeout must be greater than zero",
            ));
        }
        if self.config.idempotent {
            if self.config.retries == 0 {
                return Err(KrafkaError::config(
                    "idempotent producer requires retries > 0",
                ));
            }
            if self.config.acks != Acks::All {
                return Err(KrafkaError::config(format!(
                    "idempotent producer requires acks = All (got {:?})",
                    self.config.acks
                )));
            }
            if self.config.max_in_flight > 5 {
                return Err(KrafkaError::config(format!(
                    "idempotent producer requires max_in_flight <= 5 (got {})",
                    self.config.max_in_flight
                )));
            }
        }
        if self.config.buffer_memory > 0 && self.config.batch_size > self.config.buffer_memory {
            return Err(KrafkaError::config(format!(
                "batch_size must not exceed buffer_memory (got batch_size={}, buffer_memory={})",
                self.config.batch_size, self.config.buffer_memory
            )));
        }
        let interceptor: Arc<dyn crate::interceptor::ProducerInterceptor> =
            if self.interceptors.is_empty() {
                Arc::new(crate::interceptor::NoOpProducerInterceptor)
            } else if self.interceptors.len() == 1 {
                // infallible: len == 1 guaranteed by the surrounding else-if
                let Some(single) = self.interceptors.into_iter().next() else {
                    unreachable!("len == 1 verified above");
                };
                single
            } else {
                Arc::new(crate::interceptor::ProducerInterceptorChain::new(
                    self.interceptors,
                ))
            };
        let producer = Producer::new(self.config, interceptor, self.partitioner).await?;
        Ok(producer)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
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
            .auth(AuthConfig::sasl_plain("user", "pass").unwrap());

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
            .sasl_plain("user", "pass")
            .unwrap();

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
    fn test_idempotent_builder() {
        // Idempotent is enabled by default
        let builder = Producer::builder().bootstrap_servers("broker:9092");
        assert!(builder.config.idempotent);

        // Can be explicitly disabled
        let builder = Producer::builder()
            .bootstrap_servers("broker:9092")
            .idempotent(false);
        assert!(!builder.config.idempotent);
    }

    #[tokio::test]
    async fn test_idempotent_requires_acks_all() {
        let builder = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .acks(Acks::Leader)
            .idempotent(true);

        let result = builder.build().await;
        match result {
            Err(e) => assert!(e.to_string().contains("acks")),
            Ok(_) => panic!("expected config error for idempotent with acks != All"),
        }
    }

    #[tokio::test]
    async fn test_idempotent_requires_max_in_flight_le_5() {
        let mut builder = Producer::builder().bootstrap_servers("localhost:9092");
        builder.config.max_in_flight = 10;

        let result = builder.build().await;
        match result {
            Err(e) => assert!(e.to_string().contains("max_in_flight")),
            Ok(_) => panic!("expected config error for idempotent with max_in_flight > 5"),
        }
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
        use crate::interceptor::{InterceptorResult, ProducerInterceptor};

        #[derive(Debug)]
        struct TestInterceptor;
        impl ProducerInterceptor for TestInterceptor {
            fn on_send(&self, _record: &mut ProducerRecord) -> InterceptorResult {
                Ok(())
            }
        }

        let builder = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .interceptor(Arc::new(TestInterceptor));

        assert_eq!(builder.interceptors.len(), 1);
    }

    #[test]
    fn test_producer_builder_add_interceptor() {
        use crate::interceptor::ProducerInterceptor;

        #[derive(Debug)]
        struct A;
        impl ProducerInterceptor for A {}

        #[derive(Debug)]
        struct B;
        impl ProducerInterceptor for B {}

        // add_interceptor appends to chain
        let builder = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .add_interceptor(Arc::new(A))
            .add_interceptor(Arc::new(B));
        assert_eq!(builder.interceptors.len(), 2);
    }

    #[test]
    fn test_producer_builder_interceptor_replaces_chain() {
        use crate::interceptor::ProducerInterceptor;

        #[derive(Debug)]
        struct A;
        impl ProducerInterceptor for A {}

        #[derive(Debug)]
        struct B;
        impl ProducerInterceptor for B {}

        // interceptor() replaces any previously added interceptors
        let builder = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .add_interceptor(Arc::new(A))
            .add_interceptor(Arc::new(A))
            .interceptor(Arc::new(B));
        assert_eq!(builder.interceptors.len(), 1);
    }

    #[test]
    fn test_producer_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Producer>();
    }
}
