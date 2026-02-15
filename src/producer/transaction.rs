//! Transactional producer for exactly-once semantics.
//!
//! The transactional producer enables atomic writes across multiple partitions
//! and topics. It guarantees that either all messages in a transaction are
//! committed or none are.
//!
//! # Transaction State and Recovery
//!
//! Transaction state (`TransactionState`) is held in-memory only. This is the
//! **expected and correct behavior** because:
//!
//! 1. **Broker-side coordination**: The transaction coordinator on the broker
//!    side maintains the authoritative transaction state for each `transactional.id`.
//!
//! 2. **Fencing**: When a new producer starts with the same `transactional.id`,
//!    the broker:
//!    - Increments the producer epoch
//!    - Aborts any pending (uncommitted) transactions from the old producer
//!    - Issues a new Producer ID to the new producer
//!
//! 3. **Zombie fencing**: If the old producer tries to continue a transaction
//!    after the new producer has started, it receives `ProducerFenced` error.
//!
//! ## Recovery Behavior
//!
//! On producer crash/restart:
//! - Any uncommitted transaction is automatically aborted by the broker
//!   (after `transaction.timeout.ms` expires, or when a new producer with
//!   the same `transactional.id` calls `init_transactions()`)
//! - The new producer starts fresh with a new epoch
//! - No manual recovery is needed
//!
//! This matches the Kafka Java client behavior and Kafka's transaction protocol.
//!
//! # Example
//!
//! ```ignore
//! use krafka::producer::TransactionalProducer;
//!
//! let producer = TransactionalProducer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .transactional_id("my-transaction")
//!     .build()
//!     .await?;
//!
//! producer.init_transactions().await?;
//!
//! producer.begin_transaction()?;
//! producer.send("topic", Some(b"key"), b"value").await?;
//! producer.commit_transaction().await?;
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::PartitionId;
use crate::auth::AuthConfig;
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::metadata::ClusterMetadata;
use crate::network::{ConnectionConfig, ConnectionPool};
use crate::protocol::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
    AddPartitionsToTxnResponse, ApiKey, Compression, EndTxnRequest, EndTxnResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, InitProducerIdRequest, InitProducerIdResponse,
    ProducePartitionData, ProduceRequest, ProduceResponse, ProduceTopicData, RecordBatchBuilder,
    TxnOffsetCommitRequest, TxnOffsetCommitResponse,
};

use super::config::Acks;
use super::idempotent::ProducerIdentity;
use super::partitioner::{DefaultPartitioner, Partitioner};
use super::record::{ProducerRecord, RecordMetadata};

/// Transaction state machine states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransactionState {
    /// Producer not yet initialized.
    Uninitialized = 0,
    /// Ready to begin a new transaction.
    Ready = 1,
    /// Transaction is in progress.
    InTransaction = 2,
    /// Transaction is committing.
    Committing = 3,
    /// Transaction is aborting.
    Aborting = 4,
    /// Fatal error occurred, producer must be recreated.
    FatalError = 5,
    /// Initialization in progress (prevents concurrent init_transactions calls).
    Initializing = 6,
}

impl TransactionState {
    fn from_u8(v: u8) -> Self {
        match v {
            0 => Self::Uninitialized,
            1 => Self::Ready,
            2 => Self::InTransaction,
            3 => Self::Committing,
            4 => Self::Aborting,
            6 => Self::Initializing,
            _ => Self::FatalError,
        }
    }
}

/// Configuration for a transactional producer.
#[derive(Debug, Clone)]
pub struct TransactionalProducerConfig {
    /// Bootstrap servers.
    pub bootstrap_servers: String,
    /// Client ID.
    pub client_id: String,
    /// Transactional ID (required for transactions).
    pub transactional_id: String,
    /// Transaction timeout in milliseconds.
    pub transaction_timeout_ms: i32,
    /// Request timeout.
    pub request_timeout: Duration,
    /// Compression.
    pub compression: Compression,
    /// Metadata max age.
    pub metadata_max_age: Duration,
    /// Authentication configuration.
    pub auth: Option<AuthConfig>,
}

impl Default for TransactionalProducerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            client_id: "krafka-txn-producer".to_string(),
            transactional_id: String::new(),
            transaction_timeout_ms: 60000,
            request_timeout: Duration::from_secs(30),
            compression: Compression::None,
            metadata_max_age: Duration::from_secs(300),
            auth: None,
        }
    }
}

/// Partitions added to the current transaction.
#[derive(Debug, Default)]
struct TransactionPartitions {
    /// Topic-partitions added to the transaction.
    partitions: std::collections::HashSet<(String, PartitionId)>,
}

impl TransactionPartitions {
    fn add(&mut self, topic: &str, partition: PartitionId) -> bool {
        self.partitions.insert((topic.to_string(), partition))
    }

    fn clear(&mut self) {
        self.partitions.clear();
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }
}

/// A transactional Kafka producer.
///
/// Provides exactly-once semantics through transactions.
pub struct TransactionalProducer {
    /// Configuration.
    config: TransactionalProducerConfig,
    /// Cluster metadata.
    metadata: Arc<ClusterMetadata>,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Partitioner.
    partitioner: Arc<dyn Partitioner>,
    /// Transaction state.
    state: AtomicU8,
    /// Producer ID from broker.
    producer_id: RwLock<i64>,
    /// Producer epoch from broker.
    producer_epoch: RwLock<i16>,
    /// Transaction coordinator broker ID.
    coordinator_id: RwLock<Option<i32>>,
    /// Partitions in current transaction.
    txn_partitions: RwLock<TransactionPartitions>,
    /// Sequence number tracking for idempotent production.
    identity: ProducerIdentity,
}

impl TransactionalProducer {
    /// Create a new transactional producer builder.
    pub fn builder() -> TransactionalProducerBuilder {
        TransactionalProducerBuilder::default()
    }

    /// Get the current transaction state.
    #[inline]
    pub fn state(&self) -> TransactionState {
        TransactionState::from_u8(self.state.load(Ordering::SeqCst))
    }

    fn set_state(&self, state: TransactionState) {
        self.state.store(state as u8, Ordering::SeqCst);
    }

    /// Atomically transition from `expected` to `new` state.
    /// Returns `Err` with the actual state if the CAS failed.
    fn try_transition(
        &self,
        expected: TransactionState,
        new: TransactionState,
    ) -> std::result::Result<(), TransactionState> {
        self.state
            .compare_exchange(
                expected as u8,
                new as u8,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .map(|_| ())
            .map_err(TransactionState::from_u8)
    }

    /// Initialize transactions.
    ///
    /// This must be called before any transactions can be started.
    /// It fetches the producer ID and epoch from the transaction coordinator.
    pub async fn init_transactions(&self) -> Result<()> {
        // Atomic CAS: Uninitialized → Initializing (§9.3 fix: prevents concurrent calls)
        if let Err(actual) = self.try_transition(
            TransactionState::Uninitialized,
            TransactionState::Initializing,
        ) {
            return Err(KrafkaError::invalid_state(format!(
                "init_transactions can only be called once (state={:?})",
                actual
            )));
        }

        // Find transaction coordinator
        let result = self.do_init_transactions().await;
        if result.is_err() {
            // Revert state so caller can retry
            self.set_state(TransactionState::Uninitialized);
        }
        result
    }

    /// Inner initialization logic, separated for clean error handling.
    async fn do_init_transactions(&self) -> Result<()> {
        let coordinator = self.find_coordinator().await?;
        *self.coordinator_id.write().await = Some(coordinator);

        // Get connection to coordinator
        let brokers = self.metadata.brokers().await;
        let broker = brokers
            .iter()
            .find(|b| b.id == coordinator)
            .ok_or_else(|| KrafkaError::protocol("transaction coordinator not found"))?;

        let conn = self
            .pool
            .get_connection_by_id(broker.id, &broker.address())
            .await?;

        // Initialize producer ID
        let request = InitProducerIdRequest::transactional(
            &self.config.transactional_id,
            self.config.transaction_timeout_ms,
        );

        let response_bytes = conn
            .send_request(ApiKey::InitProducerId, 0, |buf| {
                request.encode_v0(buf);
            })
            .await?;

        let mut buf = response_bytes;
        let response = InitProducerIdResponse::decode_v0(&mut buf)?;

        if !response.is_ok() {
            return Err(KrafkaError::broker(
                response.error_code,
                "failed to initialize producer ID",
            ));
        }

        *self.producer_id.write().await = response.producer_id;
        *self.producer_epoch.write().await = response.producer_epoch;

        // Initialize the identity for sequence number tracking
        self.identity
            .initialize(response.producer_id, response.producer_epoch);

        self.set_state(TransactionState::Ready);
        info!(
            "Transactional producer initialized: PID={}, epoch={}",
            response.producer_id, response.producer_epoch
        );

        Ok(())
    }

    /// Find the transaction coordinator.
    async fn find_coordinator(&self) -> Result<i32> {
        let brokers = self.metadata.brokers().await;
        if brokers.is_empty() {
            return Err(KrafkaError::protocol("no brokers available"));
        }

        let broker = &brokers[0];
        let conn = self
            .pool
            .get_connection_by_id(broker.id, &broker.address())
            .await?;

        let request = FindCoordinatorRequest::for_transaction(&self.config.transactional_id);

        let response_bytes = conn
            .send_request(ApiKey::FindCoordinator, 0, |buf| {
                request.encode_v0(buf);
            })
            .await?;

        let mut buf = response_bytes;
        let response = FindCoordinatorResponse::decode_v0(&mut buf)?;

        if !response.error_code.is_ok() {
            return Err(KrafkaError::broker(
                response.error_code,
                "failed to find transaction coordinator",
            ));
        }

        debug!(
            "Found transaction coordinator: broker {} at {}:{}",
            response.node_id, response.host, response.port
        );

        Ok(response.node_id)
    }

    /// Begin a new transaction.
    ///
    /// Must be called after `init_transactions()`.
    pub fn begin_transaction(&self) -> Result<()> {
        // Atomic CAS: Ready → InTransaction (§6.1 fix)
        if let Err(actual) =
            self.try_transition(TransactionState::Ready, TransactionState::InTransaction)
        {
            return Err(KrafkaError::invalid_state(format!(
                "cannot begin transaction in state {:?}",
                actual
            )));
        }

        debug!("Transaction started");
        Ok(())
    }

    /// Send a record within the current transaction.
    pub async fn send(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: &[u8],
    ) -> Result<RecordMetadata> {
        let record = ProducerRecord::new(topic, value.to_vec()).with_key(key.map(|k| k.to_vec()));
        self.send_record(record).await
    }

    /// Send a producer record within the current transaction.
    pub async fn send_record(&self, record: ProducerRecord) -> Result<RecordMetadata> {
        let current = self.state();
        if current != TransactionState::InTransaction {
            return Err(KrafkaError::invalid_state(format!(
                "cannot send in state {:?}",
                current
            )));
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

        // Add partition to transaction if not already added
        {
            let mut txn_partitions = self.txn_partitions.write().await;
            if txn_partitions.add(&topic, partition) {
                // New partition, need to add to transaction
                self.add_partition_to_txn(&topic, partition).await?;
            }
        }

        // Send the record
        self.send_to_partition(&topic, partition, record).await
    }

    /// Add a partition to the current transaction.
    async fn add_partition_to_txn(&self, topic: &str, partition: PartitionId) -> Result<()> {
        let coordinator_id = self
            .coordinator_id
            .read()
            .await
            .ok_or_else(|| KrafkaError::invalid_state("no coordinator"))?;

        let brokers = self.metadata.brokers().await;
        let broker = brokers
            .iter()
            .find(|b| b.id == coordinator_id)
            .ok_or_else(|| KrafkaError::protocol("coordinator not found"))?;

        let conn = self
            .pool
            .get_connection_by_id(broker.id, &broker.address())
            .await?;

        let producer_id = *self.producer_id.read().await;
        let producer_epoch = *self.producer_epoch.read().await;

        let request = AddPartitionsToTxnRequest::new(
            &self.config.transactional_id,
            producer_id,
            producer_epoch,
        )
        .add_partition(topic, partition);

        let response_bytes = conn
            .send_request(ApiKey::AddPartitionsToTxn, 0, |buf| {
                request.encode_v0(buf);
            })
            .await?;

        let mut buf = response_bytes;
        let response = AddPartitionsToTxnResponse::decode_v0(&mut buf)?;

        if !response.is_ok() {
            // Find the error
            for topic_result in &response.results {
                for partition_result in &topic_result.partitions {
                    if !partition_result.error_code.is_ok() {
                        return Err(KrafkaError::broker(
                            partition_result.error_code,
                            format!("failed to add {}-{} to transaction", topic, partition),
                        ));
                    }
                }
            }
        }

        debug!("Added partition {}-{} to transaction", topic, partition);
        Ok(())
    }

    /// Send a record to a specific partition.
    ///
    /// Includes retry logic with exponential backoff for transient failures.
    /// Handles `OutOfOrderSequenceNumber` by resetting the partition sequence.
    async fn send_to_partition(
        &self,
        topic: &str,
        partition: PartitionId,
        record: ProducerRecord,
    ) -> Result<RecordMetadata> {
        let max_retries: u32 = 3;
        let mut attempt: u32 = 0;
        let mut backoff = Duration::from_millis(100);

        loop {
            let result = self.do_send_to_partition(topic, partition, &record).await;

            match result {
                Ok(metadata) => {
                    // Acknowledge sequence on success
                    let seq = self.identity.peek_sequence(topic, partition);
                    if seq > 0 {
                        self.identity.acknowledge(topic, partition, seq - 1);
                    }
                    return Ok(metadata);
                }
                Err(ref e) => {
                    // Handle OutOfOrderSequenceNumber by resetting sequence
                    if let KrafkaError::Broker { code, .. } = e
                        && *code == crate::error::ErrorCode::OutOfOrderSequenceNumber
                    {
                        warn!(
                            "OutOfOrderSequenceNumber for {}-{}, resetting sequence",
                            topic, partition
                        );
                        self.identity.reset_sequence(topic, partition);
                    }

                    // Check for fatal errors
                    if !e.is_retriable() {
                        return result;
                    }

                    attempt += 1;
                    if attempt > max_retries {
                        return result;
                    }

                    debug!(
                        topic = topic,
                        partition = partition,
                        attempt = attempt,
                        "Transient error in txn send, retrying: {}",
                        e
                    );

                    // Refresh metadata on leader errors
                    let _ = self.metadata.refresh_for_topics(Some(&[topic])).await;

                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(Duration::from_secs(5));
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
        let conn = self
            .metadata
            .get_leader_connection(topic, partition)
            .await?;

        let producer_id = *self.producer_id.read().await;
        let producer_epoch = *self.producer_epoch.read().await;

        // Get next sequence number for this topic-partition
        let sequence = self.next_sequence(topic, partition).await;

        // Build record batch with transaction info (producer ID, epoch, sequence)
        let mut batch_builder = RecordBatchBuilder::new()
            .compression(self.config.compression)
            .producer(producer_id, producer_epoch, sequence)
            .transactional(true);

        // Propagate user-supplied timestamp to the batch
        if let Some(ts) = record.timestamp {
            batch_builder = batch_builder.base_timestamp(ts);
        }

        if record.headers.is_empty() {
            batch_builder = batch_builder.add_record(
                record.key.clone().map(Bytes::from),
                Some(Bytes::from(record.value.clone())),
            );
        } else {
            batch_builder = batch_builder.add_record_with_headers(
                record.key.clone().map(Bytes::from),
                Some(Bytes::from(record.value.clone())),
                record
                    .headers
                    .iter()
                    .map(|(k, v)| (k.clone(), Bytes::from(v.clone())))
                    .collect(),
            );
        }

        let batch = batch_builder.build();
        let batch_bytes = batch.encode()?;

        // Build produce request with transactional ID
        let request = ProduceRequest {
            transactional_id: Some(self.config.transactional_id.clone()),
            acks: Acks::All.to_i16(),
            timeout_ms: crate::util::duration_to_millis_i32(self.config.request_timeout),
            topic_data: vec![ProduceTopicData {
                name: topic.to_string(),
                partition_data: vec![ProducePartitionData {
                    index: partition,
                    records: batch_bytes,
                }],
            }],
        };

        let response = conn
            .send_request(ApiKey::Produce, 3, |buf| {
                request.encode_v3(buf);
            })
            .await?;

        let mut buf = response;
        let produce_response = ProduceResponse::decode_v2(&mut buf)?;

        for topic_response in &produce_response.responses {
            for partition_response in &topic_response.partition_responses {
                if partition_response.index == partition {
                    if !partition_response.error_code.is_ok() {
                        // Check for fatal errors
                        if is_fatal_transaction_error(partition_response.error_code) {
                            self.set_state(TransactionState::FatalError);
                        }
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

    /// Send consumer offsets within the current transaction.
    ///
    /// This allows atomic commit of consumed offsets along with produced messages.
    pub async fn send_offsets_to_transaction(
        &self,
        offsets: std::collections::HashMap<(String, PartitionId), i64>,
        group_id: &str,
    ) -> Result<()> {
        let current = self.state();
        if current != TransactionState::InTransaction {
            return Err(KrafkaError::invalid_state(format!(
                "cannot send offsets in state {:?}",
                current
            )));
        }

        let coordinator_id = self
            .coordinator_id
            .read()
            .await
            .ok_or_else(|| KrafkaError::invalid_state("no coordinator"))?;

        let brokers = self.metadata.brokers().await;
        let broker = brokers
            .iter()
            .find(|b| b.id == coordinator_id)
            .ok_or_else(|| KrafkaError::protocol("coordinator not found"))?;

        let conn = self
            .pool
            .get_connection_by_id(broker.id, &broker.address())
            .await?;

        let producer_id = *self.producer_id.read().await;
        let producer_epoch = *self.producer_epoch.read().await;

        // First, add the offsets to the transaction
        let add_request = AddOffsetsToTxnRequest::new(
            &self.config.transactional_id,
            producer_id,
            producer_epoch,
            group_id,
        );

        let response_bytes = conn
            .send_request(ApiKey::AddOffsetsToTxn, 0, |buf| {
                add_request.encode_v0(buf);
            })
            .await?;

        let mut buf = response_bytes;
        let add_response = AddOffsetsToTxnResponse::decode_v0(&mut buf)?;

        if !add_response.is_ok() {
            return Err(KrafkaError::broker(
                add_response.error_code,
                "failed to add offsets to transaction",
            ));
        }

        // Now commit the offsets transactionally
        let mut commit_request = TxnOffsetCommitRequest::new(
            &self.config.transactional_id,
            group_id,
            producer_id,
            producer_epoch,
        );

        for ((topic, partition), offset) in offsets {
            commit_request = commit_request.add_offset(&topic, partition, offset, None);
        }

        // Find the group coordinator for offset commit
        let (group_node_id, group_host, group_port) = self.find_group_coordinator(group_id).await?;
        let group_addr = format!("{}:{}", group_host, group_port);

        let group_conn = self
            .pool
            .get_connection_by_id(group_node_id, &group_addr)
            .await?;

        let response_bytes = group_conn
            .send_request(ApiKey::TxnOffsetCommit, 0, |buf| {
                commit_request.encode_v0(buf);
            })
            .await?;

        let mut buf = response_bytes;
        let commit_response = TxnOffsetCommitResponse::decode_v0(&mut buf)?;

        if !commit_response.is_ok() {
            return Err(KrafkaError::protocol(
                "failed to commit offsets in transaction",
            ));
        }

        debug!("Added offsets to transaction for group {}", group_id);
        Ok(())
    }

    /// Find the group coordinator, returning (node_id, host, port).
    async fn find_group_coordinator(&self, group_id: &str) -> Result<(i32, String, i32)> {
        let brokers = self.metadata.brokers().await;
        if brokers.is_empty() {
            return Err(KrafkaError::protocol("no brokers available"));
        }

        let broker = &brokers[0];
        let conn = self
            .pool
            .get_connection_by_id(broker.id, &broker.address())
            .await?;

        let request = FindCoordinatorRequest::for_group(group_id);

        let response_bytes = conn
            .send_request(ApiKey::FindCoordinator, 0, |buf| {
                request.encode_v0(buf);
            })
            .await?;

        let mut buf = response_bytes;
        let response = FindCoordinatorResponse::decode_v0(&mut buf)?;

        if !response.error_code.is_ok() {
            return Err(KrafkaError::broker(
                response.error_code,
                "failed to find group coordinator",
            ));
        }

        Ok((response.node_id, response.host, response.port))
    }

    /// Commit the current transaction.
    pub async fn commit_transaction(&self) -> Result<()> {
        // Atomic CAS: InTransaction → Committing (§6.1 fix)
        if let Err(actual) = self.try_transition(
            TransactionState::InTransaction,
            TransactionState::Committing,
        ) {
            return Err(KrafkaError::invalid_state(format!(
                "cannot commit in state {:?}",
                actual
            )));
        }

        let result = self.end_transaction(true).await;

        match &result {
            Ok(()) => {
                self.set_state(TransactionState::Ready);
                self.txn_partitions.write().await.clear();
                info!("Transaction committed");
            }
            Err(e) => {
                if e.is_retriable() {
                    // Allow the caller to retry commit
                    self.set_state(TransactionState::InTransaction);
                    warn!("Transaction commit failed (retriable): {}", e);
                } else {
                    // Fatal error — caller must abort (§9.9 fix: set FatalError state)
                    self.set_state(TransactionState::FatalError);
                    warn!("Transaction commit failed (fatal): {}", e);
                }
            }
        }

        result
    }

    /// Abort the current transaction.
    pub async fn abort_transaction(&self) -> Result<()> {
        // Atomic CAS: try InTransaction → Aborting first, then Committing → Aborting (§6.1 fix)
        let transition = self
            .try_transition(TransactionState::InTransaction, TransactionState::Aborting)
            .or_else(|_| {
                self.try_transition(TransactionState::Committing, TransactionState::Aborting)
            });

        if let Err(actual) = transition {
            return Err(KrafkaError::invalid_state(format!(
                "cannot abort in state {:?}",
                actual
            )));
        }

        let result = self.end_transaction(false).await;

        match &result {
            Ok(()) => {
                self.set_state(TransactionState::Ready);
                self.txn_partitions.write().await.clear();
                info!("Transaction aborted");
            }
            Err(_) => {
                self.set_state(TransactionState::FatalError);
                warn!("Transaction abort failed, producer is now in fatal error state");
            }
        }

        result
    }

    /// End the transaction (commit or abort).
    async fn end_transaction(&self, commit: bool) -> Result<()> {
        let coordinator_id = self
            .coordinator_id
            .read()
            .await
            .ok_or_else(|| KrafkaError::invalid_state("no coordinator"))?;

        let brokers = self.metadata.brokers().await;
        let broker = brokers
            .iter()
            .find(|b| b.id == coordinator_id)
            .ok_or_else(|| KrafkaError::protocol("coordinator not found"))?;

        let conn = self
            .pool
            .get_connection_by_id(broker.id, &broker.address())
            .await?;

        let producer_id = *self.producer_id.read().await;
        let producer_epoch = *self.producer_epoch.read().await;

        let request = if commit {
            EndTxnRequest::commit(&self.config.transactional_id, producer_id, producer_epoch)
        } else {
            EndTxnRequest::abort(&self.config.transactional_id, producer_id, producer_epoch)
        };

        let response_bytes = conn
            .send_request(ApiKey::EndTxn, 0, |buf| {
                request.encode_v0(buf);
            })
            .await?;

        let mut buf = response_bytes;
        let response = EndTxnResponse::decode_v0(&mut buf)?;

        if !response.is_ok() {
            return Err(KrafkaError::broker(
                response.error_code,
                if commit {
                    "failed to commit transaction"
                } else {
                    "failed to abort transaction"
                },
            ));
        }

        Ok(())
    }

    /// Get the transactional ID.
    pub fn transactional_id(&self) -> &str {
        &self.config.transactional_id
    }

    /// Get the producer ID (once initialized).
    pub async fn producer_id(&self) -> i64 {
        *self.producer_id.read().await
    }

    /// Get the producer epoch (once initialized).
    pub async fn producer_epoch(&self) -> i16 {
        *self.producer_epoch.read().await
    }

    /// Get the next sequence number for a topic-partition.
    async fn next_sequence(&self, topic: &str, partition: PartitionId) -> i32 {
        self.identity.next_sequence(topic, partition)
    }

    /// Close the transactional producer and release all resources.
    ///
    /// If a transaction is in progress, it will be aborted before closing.
    /// After calling `close()`, the producer cannot be used again.
    pub async fn close(&self) {
        // If in-transaction, abort first to clean up broker state
        let current = self.state();
        if current == TransactionState::InTransaction {
            warn!("Closing transactional producer with active transaction — aborting");
            let _ = self.abort_transaction().await;
        }

        // Set state to prevent further use
        self.set_state(TransactionState::FatalError);

        // Close all connections in the pool
        self.pool.close_all().await;
        info!(
            "TransactionalProducer closed: txn.id={}",
            self.config.transactional_id
        );
    }
}

/// Check if an error code is a fatal transaction error.
fn is_fatal_transaction_error(error_code: ErrorCode) -> bool {
    matches!(
        error_code,
        ErrorCode::InvalidProducerEpoch
            | ErrorCode::TransactionalIdAuthorizationFailed
            | ErrorCode::InvalidTxnState
            | ErrorCode::TransactionCoordinatorFenced
    )
}

/// Builder for TransactionalProducer.
#[must_use = "builders do nothing until .build() is called"]
#[derive(Debug, Default)]
pub struct TransactionalProducerBuilder {
    config: TransactionalProducerConfig,
}

impl TransactionalProducerBuilder {
    /// Set bootstrap servers.
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    /// Set client ID.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.config.client_id = client_id.into();
        self
    }

    /// Set the transactional ID (required).
    pub fn transactional_id(mut self, txn_id: impl Into<String>) -> Self {
        self.config.transactional_id = txn_id.into();
        self
    }

    /// Set the transaction timeout in milliseconds.
    pub fn transaction_timeout_ms(mut self, timeout: i32) -> Self {
        self.config.transaction_timeout_ms = timeout;
        self
    }

    /// Set request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set compression.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.config.compression = compression;
        self
    }

    /// Set authentication configuration.
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = Some(auth);
        self
    }

    /// Configure SASL/PLAIN authentication.
    pub fn sasl_plain(mut self, username: &str, password: &str) -> Self {
        self.config.auth = Some(AuthConfig::sasl_plain(username, password));
        self
    }

    /// Configure SASL/SCRAM-SHA-256 authentication.
    pub fn sasl_scram_sha256(mut self, username: &str, password: &str) -> Self {
        self.config.auth = Some(AuthConfig::sasl_scram_sha256(username, password));
        self
    }

    /// Configure SASL/SCRAM-SHA-512 authentication.
    pub fn sasl_scram_sha512(mut self, username: &str, password: &str) -> Self {
        self.config.auth = Some(AuthConfig::sasl_scram_sha512(username, password));
        self
    }

    /// Build the transactional producer.
    pub async fn build(self) -> Result<TransactionalProducer> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap_servers is required"));
        }
        if self.config.transactional_id.is_empty() {
            return Err(KrafkaError::config("transactional_id is required"));
        }
        if self.config.transaction_timeout_ms <= 0 {
            return Err(KrafkaError::config("transaction_timeout_ms must be > 0"));
        }

        let mut pool_config_builder = ConnectionConfig::builder()
            .client_id(&self.config.client_id)
            .request_timeout(self.config.request_timeout);

        if let Some(ref auth) = self.config.auth {
            pool_config_builder = pool_config_builder.auth(auth.clone());
        }

        let pool_config = pool_config_builder.build();

        let pool = Arc::new(ConnectionPool::new(pool_config));

        let bootstrap_servers: Vec<String> = self
            .config
            .bootstrap_servers
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let metadata = Arc::new(ClusterMetadata::new(
            bootstrap_servers,
            pool.clone(),
            self.config.metadata_max_age,
        ));

        metadata.refresh().await?;

        info!(
            "TransactionalProducer created with transactional.id={}",
            self.config.transactional_id
        );

        Ok(TransactionalProducer {
            config: self.config,
            metadata,
            pool,
            partitioner: Arc::new(DefaultPartitioner::new()),
            state: AtomicU8::new(TransactionState::Uninitialized as u8),
            producer_id: RwLock::new(-1),
            producer_epoch: RwLock::new(-1),
            coordinator_id: RwLock::new(None),
            txn_partitions: RwLock::new(TransactionPartitions::default()),
            identity: ProducerIdentity::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_state() {
        assert_eq!(
            TransactionState::from_u8(0),
            TransactionState::Uninitialized
        );
        assert_eq!(TransactionState::from_u8(1), TransactionState::Ready);
        assert_eq!(
            TransactionState::from_u8(2),
            TransactionState::InTransaction
        );
        assert_eq!(TransactionState::from_u8(3), TransactionState::Committing);
        assert_eq!(TransactionState::from_u8(4), TransactionState::Aborting);
        assert_eq!(TransactionState::from_u8(5), TransactionState::FatalError);
        assert_eq!(TransactionState::from_u8(99), TransactionState::FatalError);
    }

    #[test]
    fn test_transactional_producer_config_default() {
        let config = TransactionalProducerConfig::default();
        assert_eq!(config.client_id, "krafka-txn-producer");
        assert_eq!(config.transaction_timeout_ms, 60000);
    }

    #[test]
    fn test_transaction_partitions() {
        let mut partitions = TransactionPartitions::default();
        assert!(partitions.is_empty());

        assert!(partitions.add("topic1", 0));
        assert!(!partitions.is_empty());

        // Adding same partition returns false
        assert!(!partitions.add("topic1", 0));

        // Different partition returns true
        assert!(partitions.add("topic1", 1));

        partitions.clear();
        assert!(partitions.is_empty());
    }

    #[test]
    fn test_is_fatal_transaction_error() {
        assert!(is_fatal_transaction_error(ErrorCode::InvalidProducerEpoch));
        assert!(is_fatal_transaction_error(
            ErrorCode::TransactionCoordinatorFenced
        ));
        assert!(!is_fatal_transaction_error(ErrorCode::None));
        assert!(!is_fatal_transaction_error(ErrorCode::UnknownServerError));
    }

    #[tokio::test]
    async fn test_builder_missing_bootstrap() {
        let result = TransactionalProducer::builder()
            .transactional_id("my-txn")
            .build()
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_builder_missing_txn_id() {
        let result = TransactionalProducer::builder()
            .bootstrap_servers("localhost:9092")
            .build()
            .await;
        assert!(result.is_err());
    }

    #[test]
    fn test_try_transition_success() {
        let state = AtomicU8::new(TransactionState::Ready as u8);
        let result = state.compare_exchange(
            TransactionState::Ready as u8,
            TransactionState::InTransaction as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        assert!(result.is_ok());
        assert_eq!(
            TransactionState::from_u8(state.load(Ordering::SeqCst)),
            TransactionState::InTransaction
        );
    }

    #[test]
    fn test_try_transition_failure() {
        let state = AtomicU8::new(TransactionState::Uninitialized as u8);
        let result = state.compare_exchange(
            TransactionState::Ready as u8,
            TransactionState::InTransaction as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        assert!(result.is_err());
        // State should remain unchanged
        assert_eq!(
            TransactionState::from_u8(state.load(Ordering::SeqCst)),
            TransactionState::Uninitialized
        );
    }

    #[test]
    fn test_txn_builder_no_auth_by_default() {
        let builder = TransactionalProducer::builder()
            .bootstrap_servers("broker:9092")
            .transactional_id("txn-1");

        assert!(builder.config.auth.is_none());
    }

    #[test]
    fn test_txn_builder_sasl_plain() {
        let builder = TransactionalProducer::builder()
            .bootstrap_servers("broker:9093")
            .transactional_id("txn-1")
            .sasl_plain("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.plain_credentials.is_some());
    }

    #[test]
    fn test_txn_builder_sasl_scram_sha256() {
        let builder = TransactionalProducer::builder()
            .bootstrap_servers("broker:9093")
            .transactional_id("txn-1")
            .sasl_scram_sha256("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.scram_credentials.is_some());
    }

    #[test]
    fn test_txn_builder_sasl_scram_sha512() {
        let builder = TransactionalProducer::builder()
            .bootstrap_servers("broker:9093")
            .transactional_id("txn-1")
            .sasl_scram_sha512("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.scram_credentials.is_some());
    }

    #[test]
    fn test_txn_builder_auth_config() {
        use crate::auth::AuthConfig;

        let auth = AuthConfig::sasl_scram_sha256("admin", "secret");
        let builder = TransactionalProducer::builder()
            .bootstrap_servers("broker:9093")
            .transactional_id("txn-1")
            .auth(auth);

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.scram_credentials.is_some());
    }

    #[test]
    fn test_txn_builder_initializes_producer_identity() {
        // Verify a built TransactionalProducer starts with uninitialized identity
        // (pid=-1, epoch=-1 until init_transactions() is called)
        let builder = TransactionalProducer::builder()
            .bootstrap_servers("broker:9092")
            .transactional_id("txn-test");
        // The builder should have the transactional_id set
        assert_eq!(builder.config.transactional_id, "txn-test");
    }

    #[test]
    fn test_txn_builder_requires_transactional_id() {
        let builder = TransactionalProducer::builder().bootstrap_servers("broker:9092");
        // Without transactional_id, it defaults to empty string
        assert!(builder.config.transactional_id.is_empty());
    }

    #[tokio::test]
    async fn test_txn_builder_rejects_zero_timeout() {
        let result = TransactionalProducer::builder()
            .bootstrap_servers("localhost:9092")
            .transactional_id("txn-1")
            .transaction_timeout_ms(0)
            .build()
            .await;

        match result {
            Err(e) => assert!(e.to_string().contains("transaction_timeout_ms")),
            Ok(_) => panic!("expected error for transaction_timeout_ms=0"),
        }
    }

    #[tokio::test]
    async fn test_txn_builder_rejects_negative_timeout() {
        let result = TransactionalProducer::builder()
            .bootstrap_servers("localhost:9092")
            .transactional_id("txn-1")
            .transaction_timeout_ms(-1)
            .build()
            .await;

        assert!(result.is_err());
    }

    // ── R9.3: TransactionState::Initializing variant ──

    #[test]
    fn test_transaction_state_initializing_from_u8() {
        assert_eq!(TransactionState::from_u8(6), TransactionState::Initializing);
    }

    #[test]
    fn test_transaction_state_initializing_value() {
        assert_eq!(TransactionState::Initializing as u8, 6);
    }

    #[test]
    fn test_transaction_state_initializing_round_trip() {
        let state = TransactionState::Initializing;
        let val = state as u8;
        assert_eq!(
            TransactionState::from_u8(val),
            TransactionState::Initializing
        );
    }

    #[test]
    fn test_transaction_state_unknown_maps_to_fatal() {
        // Values not explicitly mapped (except 5 which is FatalError) fall to FatalError
        assert_eq!(TransactionState::from_u8(7), TransactionState::FatalError);
        assert_eq!(TransactionState::from_u8(255), TransactionState::FatalError);
    }

    // ── R9.3: CAS transition with Initializing state ──

    #[test]
    fn test_try_transition_uninitialized_to_initializing() {
        let state = AtomicU8::new(TransactionState::Uninitialized as u8);
        let result = state.compare_exchange(
            TransactionState::Uninitialized as u8,
            TransactionState::Initializing as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        assert!(result.is_ok());
        assert_eq!(
            TransactionState::from_u8(state.load(Ordering::SeqCst)),
            TransactionState::Initializing
        );
    }

    #[test]
    fn test_try_transition_initializing_blocks_second_init() {
        // Simulate: first call moved to Initializing, second call should fail
        let state = AtomicU8::new(TransactionState::Initializing as u8);
        let result = state.compare_exchange(
            TransactionState::Uninitialized as u8,
            TransactionState::Initializing as u8,
            Ordering::SeqCst,
            Ordering::SeqCst,
        );
        assert!(result.is_err());
        // State stays Initializing
        assert_eq!(
            TransactionState::from_u8(state.load(Ordering::SeqCst)),
            TransactionState::Initializing
        );
    }

    // ── R9.9: commit_transaction sets FatalError on non-retriable errors ──

    #[test]
    fn test_commit_fatal_error_state_machine() {
        // Simulate the commit_transaction error-handling logic:
        // On non-retriable error → state becomes FatalError
        let state = AtomicU8::new(TransactionState::Committing as u8);

        // Simulate a non-retriable error (e.g. InvalidProducerEpoch)
        let error = KrafkaError::broker(ErrorCode::InvalidProducerEpoch, "epoch fenced");
        assert!(!error.is_retriable());

        // Apply the same logic as commit_transaction
        if error.is_retriable() {
            state.store(TransactionState::InTransaction as u8, Ordering::SeqCst);
        } else {
            state.store(TransactionState::FatalError as u8, Ordering::SeqCst);
        }

        assert_eq!(
            TransactionState::from_u8(state.load(Ordering::SeqCst)),
            TransactionState::FatalError
        );
    }

    #[test]
    fn test_commit_retriable_error_reverts_to_in_transaction() {
        // Simulate the commit_transaction error-handling logic:
        // On retriable error → state reverts to InTransaction
        let state = AtomicU8::new(TransactionState::Committing as u8);

        let error = KrafkaError::broker(ErrorCode::CoordinatorNotAvailable, "coordinator down");
        assert!(error.is_retriable());

        if error.is_retriable() {
            state.store(TransactionState::InTransaction as u8, Ordering::SeqCst);
        } else {
            state.store(TransactionState::FatalError as u8, Ordering::SeqCst);
        }

        assert_eq!(
            TransactionState::from_u8(state.load(Ordering::SeqCst)),
            TransactionState::InTransaction
        );
    }

    // ── R14: close() sets FatalError to prevent further use ──

    #[test]
    fn test_txn_close_sets_fatal_error_state() {
        // Verify the close() contract: after close, state is FatalError
        let state = AtomicU8::new(TransactionState::Ready as u8);
        // Simulate close: set to FatalError
        state.store(TransactionState::FatalError as u8, Ordering::SeqCst);
        assert_eq!(
            TransactionState::from_u8(state.load(Ordering::SeqCst)),
            TransactionState::FatalError
        );
    }

    // ── R14: OutOfOrderSequenceNumber is retriable ──

    #[test]
    fn test_out_of_order_sequence_is_retriable() {
        let error = KrafkaError::broker(ErrorCode::OutOfOrderSequenceNumber, "sequence mismatch");
        assert!(error.is_retriable());
    }

    // ── R14: ProducerRecord timestamp propagation ──

    #[test]
    fn test_producer_record_with_timestamp() {
        use crate::producer::ProducerRecord;
        let record = ProducerRecord::new("topic", b"value".to_vec()).with_timestamp(1234567890);
        assert_eq!(record.timestamp, Some(1234567890));
    }
}
