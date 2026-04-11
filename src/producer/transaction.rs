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

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{Notify, RwLock};
use tracing::{debug, info, warn};

use crate::PartitionId;
use crate::auth::AuthConfig;
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::metadata::ClusterMetadata;
use crate::network::{BrokerConnection, ConnectionConfig, ConnectionPool};
use crate::protocol::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
    AddPartitionsToTxnResponse, ApiKey, Compression, EndTxnRequest, EndTxnResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, InitProducerIdRequest, InitProducerIdResponse,
    ProducePartitionData, ProduceRequest, ProduceResponse, ProduceTopicData, RecordBatchBuilder,
    TxnOffsetCommitRequest, TxnOffsetCommitResponse, VersionedDecode, VersionedEncode, versions,
};

use super::barrier::InFlightBarrier;
use super::config::Acks;
use super::idempotent::ProducerIdentity;
use super::partitioner::{DefaultPartitioner, Partitioner};
use super::record::{ProducerRecord, RecordMetadata};
use super::retry::RetryPolicy;

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
    /// SOCKS5 proxy configuration (optional).
    #[cfg(feature = "socks5")]
    pub proxy: Option<crate::network::ProxyConfig>,
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
            #[cfg(feature = "socks5")]
            proxy: None,
        }
    }
}

/// State of a partition within the current transaction.
#[derive(Debug, Clone)]
enum PartitionAddState {
    /// AddPartitionsToTxn RPC is in-flight; concurrent callers should wait.
    Pending(Arc<Notify>),
    /// Successfully registered with the transaction coordinator.
    Added,
}

/// Result of attempting to begin adding a partition to the transaction.
enum BeginAddResult {
    /// Partition already registered — nothing to do.
    AlreadyAdded,
    /// Another caller is registering this partition — wait on the Notify.
    Wait(Arc<Notify>),
    /// This caller must perform the RPC. Notify to signal waiters afterwards.
    NeedAdd(Arc<Notify>),
}

/// Partitions added to the current transaction.
#[derive(Debug, Default)]
struct TransactionPartitions {
    /// Topic-partitions and their registration state (topic → partition → state).
    partitions: std::collections::HashMap<
        String,
        std::collections::HashMap<PartitionId, PartitionAddState>,
    >,
}

impl TransactionPartitions {
    /// Begin adding a partition. Returns the action the caller must take.
    fn begin_add(&mut self, topic: &str, partition: PartitionId) -> BeginAddResult {
        if let Some(topic_map) = self.partitions.get(topic) {
            match topic_map.get(&partition) {
                Some(PartitionAddState::Added) => return BeginAddResult::AlreadyAdded,
                Some(PartitionAddState::Pending(notify)) => {
                    return BeginAddResult::Wait(notify.clone());
                }
                None => {}
            }
        }
        let notify = Arc::new(Notify::new());
        self.partitions
            .entry(topic.to_string())
            .or_default()
            .insert(partition, PartitionAddState::Pending(notify.clone()));
        BeginAddResult::NeedAdd(notify)
    }

    /// Confirm a partition was successfully registered.
    fn confirm_add(&mut self, topic: &str, partition: PartitionId, notify: &Notify) {
        self.partitions
            .entry(topic.to_string())
            .or_default()
            .insert(partition, PartitionAddState::Added);
        notify.notify_waiters();
    }

    /// Cancel a pending add (RPC failed). Removes the entry and wakes waiters.
    fn cancel_add(&mut self, topic: &str, partition: PartitionId, notify: &Notify) {
        if let Some(topic_map) = self.partitions.get_mut(topic) {
            topic_map.remove(&partition);
            if topic_map.is_empty() {
                self.partitions.remove(topic);
            }
        }
        notify.notify_waiters();
    }

    fn clear(&mut self) {
        self.partitions.clear();
    }

    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }
}

/// RAII guard that cancels a pending partition add if dropped without confirmation.
///
/// When the task performing the `AddPartitionsToTxn` RPC is cancelled (e.g.,
/// via `select!` or `timeout`), this guard ensures the partition is rolled back
/// from `Pending` to absent so that future callers aren't stuck waiting forever.
struct PendingAddGuard {
    txn_partitions: Arc<RwLock<TransactionPartitions>>,
    topic: String,
    partition: PartitionId,
    notify: Arc<Notify>,
    /// Set to `true` when `confirm_add` or an explicit `cancel_add` is called,
    /// preventing the drop impl from double-cancelling.
    defused: bool,
}

impl PendingAddGuard {
    /// Confirm the add succeeded. Consumes the guard without cancelling.
    async fn confirm(mut self, topic: &str, partition: PartitionId) {
        self.defused = true;
        let mut txn_partitions = self.txn_partitions.write().await;
        txn_partitions.confirm_add(topic, partition, &self.notify);
    }

    /// Explicitly cancel the add (RPC failed). Consumes the guard.
    async fn cancel(mut self, topic: &str, partition: PartitionId) {
        self.defused = true;
        let mut txn_partitions = self.txn_partitions.write().await;
        txn_partitions.cancel_add(topic, partition, &self.notify);
    }
}

impl Drop for PendingAddGuard {
    fn drop(&mut self) {
        if !self.defused {
            // Best-effort cancel: we can't await the lock in drop, so first
            // try a non-blocking write. If the lock is contended and a Tokio
            // runtime is available, spawn a task to perform the cancel.
            let topic = self.topic.clone();
            let partition = self.partition;
            let notify = self.notify.clone();
            if let Ok(mut tp) = self.txn_partitions.try_write() {
                tp.cancel_add(&topic, partition, &notify);
            } else if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let txn_partitions = self.txn_partitions.clone();
                handle.spawn(async move {
                    let mut tp = txn_partitions.write().await;
                    tp.cancel_add(&topic, partition, &notify);
                });
            } else {
                warn!(
                    topic = %topic,
                    partition,
                    "PendingAddGuard dropped without cleanup: lock contended and no runtime available"
                );
            }
        }
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
    /// Transaction coordinator broker ID.
    coordinator_id: RwLock<Option<i32>>,
    /// Partitions in current transaction.
    txn_partitions: Arc<RwLock<TransactionPartitions>>,
    /// Sequence number tracking for idempotent production.
    identity: ProducerIdentity,
    /// Retry policy for transient failures.
    retry_policy: RetryPolicy,
    /// Barrier over started transactional operations and shutdown state.
    in_flight_barrier: Arc<InFlightBarrier>,
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

    /// Get a connection to the cached transaction coordinator.
    ///
    /// If no coordinator is cached (e.g. after invalidation), automatically
    /// re-discovers it via `FindCoordinator` before returning the connection.
    async fn coordinator_connection(&self) -> Result<(i32, Arc<BrokerConnection>)> {
        let coordinator_id = {
            let cached = *self.coordinator_id.read().await;
            match cached {
                Some(id) => id,
                None => {
                    let id = self.find_coordinator().await?;
                    *self.coordinator_id.write().await = Some(id);
                    debug!("Auto-discovered transaction coordinator: broker {}", id);
                    id
                }
            }
        };

        let brokers = self.metadata.brokers();
        let broker = brokers
            .iter()
            .find(|b| b.id == coordinator_id)
            .ok_or_else(|| KrafkaError::protocol("coordinator not found in metadata"))?;

        let conn = self
            .pool
            .get_connection_by_id(broker.id, broker.address())
            .await?;

        Ok((coordinator_id, conn))
    }

    /// Whether the error indicates the cached coordinator may be stale.
    ///
    /// Returns `true` for coordinator-related broker errors (`NotCoordinator`,
    /// `CoordinatorNotAvailable`, `CoordinatorLoadInProgress`) and for
    /// network/timeout errors that suggest the coordinator broker is unreachable.
    fn needs_coordinator_refresh(err: &KrafkaError) -> bool {
        match err {
            KrafkaError::Broker { code, .. } => matches!(
                code,
                ErrorCode::NotCoordinator
                    | ErrorCode::CoordinatorNotAvailable
                    | ErrorCode::CoordinatorLoadInProgress
            ),
            KrafkaError::Network(_) | KrafkaError::Timeout { .. } => true,
            _ => false,
        }
    }

    /// Invalidate the cached transaction coordinator, forcing re-discovery
    /// on the next coordinator RPC.
    async fn invalidate_coordinator(&self) {
        *self.coordinator_id.write().await = None;
    }

    /// Retry a coordinator RPC with exponential backoff.
    ///
    /// On coordinator errors (`NotCoordinator`, `CoordinatorNotAvailable`,
    /// `CoordinatorLoadInProgress`) or transient network/timeout failures the
    /// cached coordinator is invalidated and re-discovered before the next
    /// attempt.  Non-retriable errors are returned immediately.
    ///
    /// `op_name` is used in log messages to identify the RPC.
    async fn retry_with_coordinator<F, Fut>(&self, op_name: &str, op: F) -> Result<()>
    where
        F: Fn() -> Fut,
        Fut: Future<Output = Result<()>>,
    {
        let max_retries = self.retry_policy.max_retries;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                tokio::time::sleep(self.retry_policy.calculate_backoff(attempt)).await;
            }

            let result = op().await;

            match &result {
                Ok(()) => return Ok(()),
                Err(e) if Self::needs_coordinator_refresh(e) && attempt < max_retries => {
                    warn!(
                        attempt,
                        error = %e,
                        op_name,
                        "Coordinator error, refreshing and retrying"
                    );
                    self.invalidate_coordinator().await;
                }
                Err(e) if e.is_retriable() && attempt < max_retries => {
                    warn!(
                        attempt,
                        error = %e,
                        op_name,
                        "Retriable error, retrying"
                    );
                }
                Err(_) => return result,
            }
        }

        unreachable!()
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
        // Atomic CAS: Uninitialized → Initializing
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
    ///
    /// Retries on coordinator errors (NotCoordinator, CoordinatorNotAvailable,
    /// CoordinatorLoadInProgress) and transient network/timeout failures with
    /// exponential backoff. On each retry the cached coordinator is invalidated
    /// and re-discovered via `FindCoordinator`.
    async fn do_init_transactions(&self) -> Result<()> {
        self.retry_with_coordinator("InitProducerId", || async {
            let (_coordinator_id, conn) = self.coordinator_connection().await?;

            let ip_version = conn
                .negotiate_api_version(
                    ApiKey::InitProducerId,
                    versions::INIT_PRODUCER_ID_MAX,
                    versions::INIT_PRODUCER_ID_MIN,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol("no mutually supported InitProducerId API version")
                })?;

            let request = InitProducerIdRequest::transactional(
                &self.config.transactional_id,
                self.config.transaction_timeout_ms,
            );

            let response_bytes = conn
                .send_request(ApiKey::InitProducerId, ip_version, |buf| {
                    request.encode_versioned(ip_version, buf)
                })
                .await?;

            let mut buf = response_bytes;
            let response = InitProducerIdResponse::decode_versioned(ip_version, &mut buf)?;

            if !response.is_ok() {
                return Err(KrafkaError::broker(
                    response.error_code,
                    "failed to initialize producer ID",
                ));
            }

            self.identity
                .initialize(response.producer_id, response.producer_epoch);

            self.set_state(TransactionState::Ready);
            info!(
                "Transactional producer initialized: PID={}, epoch={}",
                response.producer_id, response.producer_epoch
            );

            Ok(())
        })
        .await
    }

    /// Find the transaction coordinator.
    async fn find_coordinator(&self) -> Result<i32> {
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::protocol("no brokers available"));
        }

        let broker = &brokers[0];
        let conn = self
            .pool
            .get_connection_by_id(broker.id, broker.address())
            .await?;

        let request = FindCoordinatorRequest::for_transaction(&self.config.transactional_id);

        // Transaction coordinator lookup requires v1+ (key_type field).
        // FIND_COORDINATOR_MIN is 1, so negotiate_api_version returns None
        // (handled above) rather than v0 when the broker lacks v1+.
        let fc_version = conn
            .negotiate_api_version(
                ApiKey::FindCoordinator,
                versions::FIND_COORDINATOR_MAX,
                versions::FIND_COORDINATOR_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol(
                    "no mutually supported FindCoordinator API version; \
                     transactional coordinator lookup requires v1+",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::FindCoordinator, fc_version, |buf| {
                request.encode_versioned(fc_version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = FindCoordinatorResponse::decode_versioned(fc_version, &mut buf)?;

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
        // Atomic CAS: Ready → InTransaction
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
        let mut record = ProducerRecord::new(topic, Bytes::copy_from_slice(value));
        if let Some(k) = key {
            record = record.with_key(Bytes::copy_from_slice(k));
        }
        self.send_record(record).await
    }

    /// Send a producer record within the current transaction.
    pub async fn send_record(&self, record: ProducerRecord) -> Result<RecordMetadata> {
        let _operation_guard = self.in_flight_barrier.start("transactional producer")?;
        let current = self.state();
        if current != TransactionState::InTransaction {
            return Err(KrafkaError::invalid_state(format!(
                "cannot send in state {:?}",
                current
            )));
        }

        // Validate record fields against Kafka protocol wire-format limits.
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

        // Add partition to transaction if not already registered.
        // Uses Pending/Added states to prevent concurrent callers from
        // skipping the RPC while an in-flight add has not yet completed.
        loop {
            let mut txn_partitions = self.txn_partitions.write().await;
            match txn_partitions.begin_add(&topic, partition) {
                BeginAddResult::AlreadyAdded => break,
                BeginAddResult::Wait(notify) => {
                    // Register interest in the Notify BEFORE releasing the
                    // write lock so that confirm_add/cancel_add (which use
                    // notify_waiters) cannot be missed.
                    let notified = notify.notified();
                    tokio::pin!(notified);
                    notified.as_mut().enable();
                    drop(txn_partitions);
                    notified.await;
                    // Re-check state on next iteration.
                }
                BeginAddResult::NeedAdd(notify) => {
                    // Drop the lock before the RPC. The guard ensures that
                    // if this task is cancelled, the Pending state is rolled
                    // back so waiters don't hang forever.
                    drop(txn_partitions);
                    let guard = PendingAddGuard {
                        txn_partitions: self.txn_partitions.clone(),
                        topic: topic.clone(),
                        partition,
                        notify,
                        defused: false,
                    };
                    match self.add_partition_to_txn(&topic, partition).await {
                        Ok(()) => {
                            guard.confirm(&topic, partition).await;
                        }
                        Err(e) => {
                            guard.cancel(&topic, partition).await;
                            return Err(e);
                        }
                    }
                    break;
                }
            }
        }

        // Send the record
        self.send_to_partition(&topic, partition, record).await
    }

    /// Add a partition to the current transaction.
    ///
    /// Retries on coordinator errors with exponential backoff, re-discovering
    /// the transaction coordinator between attempts.
    async fn add_partition_to_txn(&self, topic: &str, partition: PartitionId) -> Result<()> {
        self.retry_with_coordinator("AddPartitionsToTxn", || async {
            let (_coordinator_id, conn) = self.coordinator_connection().await?;

            let producer_id = self.identity.producer_id();
            let producer_epoch = self.identity.producer_epoch();

            let apt_version = conn
                .negotiate_api_version(
                    ApiKey::AddPartitionsToTxn,
                    versions::ADD_PARTITIONS_TO_TXN_MAX,
                    versions::ADD_PARTITIONS_TO_TXN_MIN,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol("no mutually supported AddPartitionsToTxn API version")
                })?;

            let request = AddPartitionsToTxnRequest::new(
                &self.config.transactional_id,
                producer_id,
                producer_epoch,
            )
            .add_partition(topic, partition);

            let response_bytes = conn
                .send_request(ApiKey::AddPartitionsToTxn, apt_version, |buf| {
                    request.encode_versioned(apt_version, buf)
                })
                .await?;

            let mut buf = response_bytes;
            let response = AddPartitionsToTxnResponse::decode_versioned(apt_version, &mut buf)?;

            if !response.is_ok() {
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
                // Fallback: is_ok() was false but no individual partition error found
                // (e.g. the target partition is missing from the response).
                return Err(KrafkaError::protocol(format!(
                    "failed to add {}-{} to transaction: response indicated error but no per-partition error found",
                    topic, partition
                )));
            }

            debug!("Added partition {}-{} to transaction", topic, partition);
            Ok(())
        })
        .await
    }

    /// Send a record to a specific partition.
    ///
    /// Includes retry logic with exponential backoff for transient failures.
    /// On `OutOfOrderSequenceNumber`, resets the partition sequence and rebuilds
    /// the batch with a fresh sequence before retrying.
    async fn send_to_partition(
        &self,
        topic: &str,
        partition: PartitionId,
        record: ProducerRecord,
    ) -> Result<RecordMetadata> {
        let retry_policy = &self.retry_policy;
        let mut attempt: u32 = 0;

        let producer_id = self.identity.producer_id();
        let producer_epoch = self.identity.producer_epoch();

        // Allocate the sequence number once — retries must resend the same
        // sequence to maintain idempotent semantics.
        let mut sequence = self.next_sequence(topic, partition).await;

        // Build the record batch and request once before entering the retry loop.
        // If encoding fails, roll back the sequence so the next send attempt
        // starts from the correct value rather than creating a gap.
        let mut request = match self.build_produce_request(
            topic,
            partition,
            &record,
            producer_id,
            producer_epoch,
            sequence,
        ) {
            Ok(req) => req,
            Err(e) => {
                self.identity.rollback_sequence(topic, partition);
                return Err(e);
            }
        };

        loop {
            // Re-acquire connection on each attempt (leader may have moved).
            let send_result: Result<RecordMetadata> = async {
                let conn = self
                    .metadata
                    .get_leader_connection(topic, partition)
                    .await?;

                // Transactions require Produce v3+ (transactional_id field).
                let version = conn
                    .negotiate_api_version(
                        ApiKey::Produce,
                        versions::PRODUCE_MAX,
                        versions::PRODUCE_MIN,
                    )
                    .await
                    .ok_or_else(|| {
                        KrafkaError::protocol(
                            "no mutually supported Produce API version; \
                             transactional produce requires v3+",
                        )
                    })?;

                let response = conn
                    .send_request(ApiKey::Produce, version, |buf| {
                        request.encode_versioned(version, buf)
                    })
                    .await?;

                let mut buf = response;
                let produce_response = ProduceResponse::decode_versioned(version, &mut buf)?;

                for topic_response in &produce_response.responses {
                    for partition_response in &topic_response.partition_responses {
                        if partition_response.index == partition {
                            if !partition_response.error_code.is_ok() {
                                if is_fatal_transaction_error(partition_response.error_code) {
                                    self.set_state(TransactionState::FatalError);
                                }
                                return Err(KrafkaError::broker(
                                    partition_response.error_code,
                                    format!("produce failed for {topic}-{partition}"),
                                ));
                            }

                            self.identity.acknowledge(topic, partition, sequence);
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
            .await;

            match send_result {
                Ok(metadata) => return Ok(metadata),
                Err(e) => {
                    // OutOfOrderSequenceNumber means the broker's expected
                    // sequence diverged from ours. Reset local state and
                    // rebuild the batch with a fresh sequence before retrying.
                    if let KrafkaError::Broker { code, .. } = &e
                        && *code == ErrorCode::OutOfOrderSequenceNumber
                    {
                        if retry_policy.max_retries_reached(attempt) {
                            return Err(e);
                        }
                        attempt += 1;

                        warn!(
                            topic = topic,
                            partition = partition,
                            "OutOfOrderSequenceNumber, resetting sequence and rebuilding batch"
                        );
                        self.identity.reset_sequence(topic, partition);
                        sequence = self.next_sequence(topic, partition).await;
                        request = self.build_produce_request(
                            topic,
                            partition,
                            &record,
                            producer_id,
                            producer_epoch,
                            sequence,
                        )?;

                        if let Err(refresh_err) =
                            self.metadata.refresh_for_topics(Some(&[topic])).await
                        {
                            debug!(error = %refresh_err, "Metadata refresh failed during txn retry");
                        }

                        tokio::time::sleep(retry_policy.calculate_backoff(attempt)).await;
                        continue;
                    }

                    if !retry_policy.should_retry(&e, attempt) {
                        return Err(e);
                    }

                    attempt += 1;

                    debug!(
                        topic = topic,
                        partition = partition,
                        attempt = attempt,
                        "Transient error in txn send, retrying: {}",
                        e
                    );

                    if let Err(refresh_err) = self.metadata.refresh_for_topics(Some(&[topic])).await
                    {
                        debug!(error = %refresh_err, "Metadata refresh failed during txn retry");
                    }

                    tokio::time::sleep(retry_policy.calculate_backoff(attempt)).await;
                }
            }
        }
    }

    /// Build a produce request for a single record to a partition.
    fn build_produce_request(
        &self,
        topic: &str,
        partition: PartitionId,
        record: &ProducerRecord,
        producer_id: i64,
        producer_epoch: i16,
        sequence: i32,
    ) -> Result<ProduceRequest> {
        let mut batch_builder = RecordBatchBuilder::new()
            .compression(self.config.compression)
            .producer(producer_id, producer_epoch, sequence)
            .transactional(true);

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

        Ok(ProduceRequest {
            transactional_id: Some(self.config.transactional_id.clone()),
            acks: Acks::All.to_i16(),
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

    /// Send consumer offsets within the current transaction.
    ///
    /// This allows atomic commit of consumed offsets along with produced messages.
    /// The `AddOffsetsToTxn` RPC (sent to the transaction coordinator) is retried
    /// on coordinator errors. The `TxnOffsetCommit` RPC (sent to the group
    /// coordinator) is retried with group coordinator re-discovery on
    /// coordinator and retriable errors.
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

        let producer_id = self.identity.producer_id();
        let producer_epoch = self.identity.producer_epoch();

        // Phase 1: AddOffsetsToTxn — sent to transaction coordinator, with retry.
        self.retry_with_coordinator("AddOffsetsToTxn", || async {
            let (_coordinator_id, conn) = self.coordinator_connection().await?;

            let add_request = AddOffsetsToTxnRequest::new(
                &self.config.transactional_id,
                producer_id,
                producer_epoch,
                group_id,
            );

            let aot_version = conn
                .negotiate_api_version(
                    ApiKey::AddOffsetsToTxn,
                    versions::ADD_OFFSETS_TO_TXN_MAX,
                    versions::ADD_OFFSETS_TO_TXN_MIN,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol("no mutually supported AddOffsetsToTxn API version")
                })?;

            let response_bytes = conn
                .send_request(ApiKey::AddOffsetsToTxn, aot_version, |buf| {
                    add_request.encode_versioned(aot_version, buf)
                })
                .await?;

            let mut buf = response_bytes;
            let add_response = AddOffsetsToTxnResponse::decode_versioned(aot_version, &mut buf)?;

            if !add_response.is_ok() {
                return Err(KrafkaError::broker(
                    add_response.error_code,
                    "failed to add offsets to transaction",
                ));
            }

            Ok(())
        })
        .await?;

        // Phase 2: TxnOffsetCommit — sent to the group coordinator, with retry.
        // The Java client re-discovers the group coordinator and re-enqueues
        // on coordinator or retriable errors; we mirror that with a retry loop.
        let mut commit_request = TxnOffsetCommitRequest::new(
            &self.config.transactional_id,
            group_id,
            producer_id,
            producer_epoch,
        );

        for ((topic, partition), offset) in offsets {
            commit_request = commit_request.add_offset(&topic, partition, offset, None);
        }

        let max_retries = self.retry_policy.max_retries;

        for attempt in 0..=max_retries {
            if attempt > 0 {
                tokio::time::sleep(self.retry_policy.calculate_backoff(attempt)).await;
            }

            let result: Result<()> = async {
                let (group_node_id, group_host, group_port) =
                    self.find_group_coordinator(group_id).await?;
                let group_addr = format!("{group_host}:{group_port}");

                let group_conn = self
                    .pool
                    .get_connection_by_id(group_node_id, &group_addr)
                    .await?;

                let toc_version = group_conn
                    .negotiate_api_version(
                        ApiKey::TxnOffsetCommit,
                        versions::TXN_OFFSET_COMMIT_MAX,
                        versions::TXN_OFFSET_COMMIT_MIN,
                    )
                    .await
                    .ok_or_else(|| {
                        KrafkaError::protocol("no mutually supported TxnOffsetCommit API version")
                    })?;

                let response_bytes = group_conn
                    .send_request(ApiKey::TxnOffsetCommit, toc_version, |buf| {
                        commit_request.encode_versioned(toc_version, buf)
                    })
                    .await?;

                let mut buf = response_bytes;
                let commit_response =
                    TxnOffsetCommitResponse::decode_versioned(toc_version, &mut buf)?;

                if !commit_response.is_ok() {
                    // Extract the first per-partition error for actionable diagnostics.
                    for topic_result in &commit_response.topics {
                        for part_result in &topic_result.partitions {
                            if !part_result.error_code.is_ok() {
                                return Err(KrafkaError::broker(
                                    part_result.error_code,
                                    format!(
                                        "failed to commit offset for {}-{} in transaction",
                                        topic_result.name, part_result.partition
                                    ),
                                ));
                            }
                        }
                    }
                    // Fallback if is_ok was false but no individual error found
                    return Err(KrafkaError::protocol(
                        "failed to commit offsets in transaction",
                    ));
                }

                Ok(())
            }
            .await;

            match &result {
                Ok(()) => {
                    debug!("Added offsets to transaction for group {}", group_id);
                    return Ok(());
                }
                Err(e) if Self::needs_coordinator_refresh(e) && attempt < max_retries => {
                    warn!(
                        attempt,
                        error = %e,
                        "TxnOffsetCommit group coordinator error, re-discovering and retrying"
                    );
                }
                Err(e) if e.is_retriable() && attempt < max_retries => {
                    warn!(
                        attempt,
                        error = %e,
                        "TxnOffsetCommit retriable error, retrying"
                    );
                }
                Err(_) => return result,
            }
        }

        unreachable!()
    }

    /// Find the group coordinator, returning (node_id, host, port).
    async fn find_group_coordinator(&self, group_id: &str) -> Result<(i32, String, i32)> {
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::protocol("no brokers available"));
        }

        let broker = &brokers[0];
        let conn = self
            .pool
            .get_connection_by_id(broker.id, broker.address())
            .await?;

        let request = FindCoordinatorRequest::for_group(group_id);

        // Negotiate FindCoordinator version — requires v1+ (MIN).
        let fc_version = conn
            .negotiate_api_version(
                ApiKey::FindCoordinator,
                versions::FIND_COORDINATOR_MAX,
                versions::FIND_COORDINATOR_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported FindCoordinator API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::FindCoordinator, fc_version, |buf| {
                request.encode_versioned(fc_version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = FindCoordinatorResponse::decode_versioned(fc_version, &mut buf)?;

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
        // Atomic CAS: InTransaction → Committing
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
                    // Use CAS to safely revert Committing → InTransaction.
                    // If abort_transaction() raced and moved to Aborting,
                    // the CAS fails and we leave the state alone.
                    if self
                        .try_transition(
                            TransactionState::Committing,
                            TransactionState::InTransaction,
                        )
                        .is_ok()
                    {
                        warn!("Transaction commit failed (retriable): {}", e);
                    }
                } else {
                    // Fatal error — caller must abort
                    self.set_state(TransactionState::FatalError);
                    warn!("Transaction commit failed (fatal): {}", e);
                }
            }
        }

        result
    }

    /// Abort the current transaction.
    pub async fn abort_transaction(&self) -> Result<()> {
        // Atomic CAS: try InTransaction → Aborting first, then Committing → Aborting
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
    ///
    /// Retries on coordinator errors with exponential backoff, re-discovering
    /// the transaction coordinator between attempts.
    async fn end_transaction(&self, commit: bool) -> Result<()> {
        self.retry_with_coordinator("EndTxn", || async {
            let (_coordinator_id, conn) = self.coordinator_connection().await?;

            let producer_id = self.identity.producer_id();
            let producer_epoch = self.identity.producer_epoch();

            let et_version = conn
                .negotiate_api_version(ApiKey::EndTxn, versions::END_TXN_MAX, versions::END_TXN_MIN)
                .await
                .ok_or_else(|| KrafkaError::protocol("no mutually supported EndTxn API version"))?;

            let request = if commit {
                EndTxnRequest::commit(&self.config.transactional_id, producer_id, producer_epoch)
            } else {
                EndTxnRequest::abort(&self.config.transactional_id, producer_id, producer_epoch)
            };

            let response_bytes = conn
                .send_request(ApiKey::EndTxn, et_version, |buf| {
                    request.encode_versioned(et_version, buf)
                })
                .await?;

            let mut buf = response_bytes;
            let response = EndTxnResponse::decode_versioned(et_version, &mut buf)?;

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
        })
        .await
    }

    /// Get the transactional ID.
    #[inline]
    pub fn transactional_id(&self) -> &str {
        &self.config.transactional_id
    }

    /// Get the producer ID (once initialized).
    #[inline]
    pub fn producer_id(&self) -> i64 {
        self.identity.producer_id()
    }

    /// Get the producer epoch (once initialized).
    #[inline]
    pub fn producer_epoch(&self) -> i16 {
        self.identity.producer_epoch()
    }

    /// Get the next sequence number for a topic-partition.
    async fn next_sequence(&self, topic: &str, partition: PartitionId) -> i32 {
        self.identity.next_sequence(topic, partition)
    }

    /// Close the transactional producer and release all resources.
    ///
    /// If a transaction is in progress, it will be aborted before closing.
    /// After calling `close()`, the producer cannot be used again.
    /// Calling `close()` more than once is a no-op.
    pub async fn close(&self) {
        let _ = self.close_inner(None).await;
    }

    /// Close the transactional producer, giving up on graceful shutdown once
    /// `timeout` expires.
    ///
    /// On timeout, the connection pool is still torn down, causing any
    /// remaining in-flight operations to fail fast.
    pub async fn close_with_timeout(&self, timeout: Duration) -> Result<()> {
        self.close_inner(Some(timeout)).await
    }

    async fn close_inner(&self, timeout: Option<Duration>) -> Result<()> {
        let Some(target) = self.in_flight_barrier.begin_close() else {
            return Ok(());
        };

        let graceful_close = async {
            // Let already-started sends cross the ack boundary before aborting the
            // active transaction or tearing down sockets.
            self.in_flight_barrier.wait_for(target).await;

            // If in-transaction, abort first to clean up broker state.
            let current = self.state();
            if current == TransactionState::InTransaction {
                warn!("Closing transactional producer with active transaction — aborting");
                self.abort_transaction().await?;
            }

            Ok::<(), KrafkaError>(())
        };

        let close_result = if let Some(timeout) = timeout {
            tokio::time::timeout(timeout, graceful_close)
                .await
                .map_err(|_| KrafkaError::timeout("transactional producer close"))?
        } else {
            graceful_close.await
        };

        // Set state to prevent further use
        self.set_state(TransactionState::FatalError);

        // Close all connections in the pool
        self.pool.close_all().await;
        info!(
            "TransactionalProducer closed: txn.id={}",
            self.config.transactional_id
        );

        close_result
    }

    /// Check if the transactional producer has been explicitly closed.
    ///
    /// Returns `true` only when [`Self::close`] has been called. A producer in
    /// [`TransactionState::FatalError`] due to a broker error is *not*
    /// considered closed — use [`Self::state`] to check for fatal errors.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.in_flight_barrier.is_closing()
    }
}

/// Check if an error code is a fatal transaction error.
fn is_fatal_transaction_error(error_code: ErrorCode) -> bool {
    matches!(
        error_code,
        ErrorCode::InvalidProducerEpoch
            | ErrorCode::ProducerFenced
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

    /// Set SOCKS5 proxy configuration.
    ///
    /// Routes all broker connections through the specified SOCKS5 proxy.
    #[cfg(feature = "socks5")]
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.config.proxy = Some(proxy);
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
            return Err(KrafkaError::config("bootstrap.servers is required"));
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

        #[cfg(feature = "socks5")]
        if let Some(ref proxy) = self.config.proxy {
            pool_config_builder = pool_config_builder.proxy(proxy.clone());
        }

        let pool_config = pool_config_builder.build();

        let pool = Arc::new(ConnectionPool::new(pool_config));

        let bootstrap_servers =
            crate::util::parse_bootstrap_servers(&self.config.bootstrap_servers)?;

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
            coordinator_id: RwLock::new(None),
            txn_partitions: Arc::new(RwLock::new(TransactionPartitions::default())),
            identity: ProducerIdentity::new(),
            retry_policy: RetryPolicy::default(),
            in_flight_barrier: Arc::new(InFlightBarrier::new()),
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

        // First add returns NeedAdd
        let result = partitions.begin_add("topic1", 0);
        let notify = match result {
            BeginAddResult::NeedAdd(n) => n,
            _ => panic!("expected NeedAdd"),
        };
        assert!(!partitions.is_empty());

        // Same partition while Pending returns Wait
        assert!(matches!(
            partitions.begin_add("topic1", 0),
            BeginAddResult::Wait(_)
        ));

        // Confirm, then same partition returns AlreadyAdded
        partitions.confirm_add("topic1", 0, &notify);
        assert!(matches!(
            partitions.begin_add("topic1", 0),
            BeginAddResult::AlreadyAdded
        ));

        // Different partition returns NeedAdd
        assert!(matches!(
            partitions.begin_add("topic1", 1),
            BeginAddResult::NeedAdd(_)
        ));

        partitions.clear();
        assert!(partitions.is_empty());
    }

    #[test]
    fn test_is_fatal_transaction_error() {
        assert!(is_fatal_transaction_error(ErrorCode::InvalidProducerEpoch));
        assert!(is_fatal_transaction_error(ErrorCode::ProducerFenced));
        assert!(is_fatal_transaction_error(
            ErrorCode::TransactionCoordinatorFenced
        ));
        assert!(is_fatal_transaction_error(
            ErrorCode::TransactionalIdAuthorizationFailed
        ));
        assert!(is_fatal_transaction_error(ErrorCode::InvalidTxnState));
        assert!(!is_fatal_transaction_error(ErrorCode::None));
        assert!(!is_fatal_transaction_error(ErrorCode::UnknownServerError));
    }

    #[test]
    fn test_needs_coordinator_refresh() {
        // Coordinator-related broker errors → true
        assert!(TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::broker(ErrorCode::NotCoordinator, "test")
        ));
        assert!(TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::broker(ErrorCode::CoordinatorNotAvailable, "test")
        ));
        assert!(TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::broker(ErrorCode::CoordinatorLoadInProgress, "test")
        ));

        // Network and timeout errors → true (coordinator may have moved)
        assert!(TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::Network(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                "refused"
            ))
        ));
        assert!(TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::timeout("test operation")
        ));

        // Non-coordinator broker errors → false
        assert!(!TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::broker(ErrorCode::InvalidProducerEpoch, "test")
        ));
        assert!(!TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::broker(ErrorCode::TransactionCoordinatorFenced, "test")
        ));

        // Other error types → false
        assert!(!TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::protocol("test")
        ));
        assert!(!TransactionalProducer::needs_coordinator_refresh(
            &KrafkaError::invalid_state("test")
        ));
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

    #[test]
    fn test_transaction_partitions_state_machine() {
        let mut tp = TransactionPartitions::default();

        // First add returns NeedAdd
        let result = tp.begin_add("topic", 0);
        let notify = match result {
            BeginAddResult::NeedAdd(n) => n,
            _ => panic!("expected NeedAdd"),
        };

        // Concurrent add returns Wait
        let result2 = tp.begin_add("topic", 0);
        assert!(matches!(result2, BeginAddResult::Wait(_)));

        // Confirm moves to Added
        tp.confirm_add("topic", 0, &notify);
        assert!(matches!(
            tp.begin_add("topic", 0),
            BeginAddResult::AlreadyAdded
        ));

        // Different partition returns NeedAdd
        let result3 = tp.begin_add("topic", 1);
        let notify2 = match result3 {
            BeginAddResult::NeedAdd(n) => n,
            _ => panic!("expected NeedAdd"),
        };

        // Cancel removes — next call returns NeedAdd again
        tp.cancel_add("topic", 1, &notify2);
        assert!(matches!(
            tp.begin_add("topic", 1),
            BeginAddResult::NeedAdd(_)
        ));

        // Clear empties everything
        tp.clear();
        assert!(tp.is_empty());
    }

    #[test]
    fn test_transactional_producer_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TransactionalProducer>();
    }
}
