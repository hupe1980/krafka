//! Share consumer implementation (KIP-932).
//!
//! Share groups provide queue-like semantics on top of Kafka topics. Multiple
//! consumers in the same share group receive non-overlapping subsets of records
//! without client-side partition assignment — all assignment is performed by
//! the server.
//!
//! # Delivery Semantics
//!
//! Share groups support **at-least-once** delivery with explicit or implicit
//! acknowledgement:
//!
//! - **Implicit** (default): previously fetched records are automatically
//!   accepted when the next `poll()` is called.
//! - **Explicit**: the application calls
//!   [`acknowledge()`](ShareConsumer::acknowledge) per record and then
//!   [`commit_sync()`](ShareConsumer::commit_sync) to flush.
//!
//! Records that are released or not acknowledged within the acquisition lock
//! timeout are redelivered to other consumers.
//!
//! # Example
//!
//! ```ignore
//! use krafka::share_consumer::{ShareConsumer, AcknowledgementMode};
//!
//! let consumer = ShareConsumer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .group_id("my-share-group")
//!     .build()
//!     .await?;
//!
//! consumer.subscribe(&["events"]).await?;
//!
//! loop {
//!     let records = consumer.poll(Duration::from_secs(1)).await?;
//!     for record in &records {
//!         process(record);
//!     }
//!     // Implicit mode: records are auto-accepted on next poll()
//! }
//! ```

mod config;
mod session;
mod stream;

pub use config::{AcknowledgeType, AcknowledgementMode, ShareConsumerConfig};
pub use stream::ShareConsumerStream;

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::{Future, Ready, ready};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::auth::AuthConfig;
use crate::consumer::ConsumerRecord;
use crate::error::{KrafkaError, Result};
use crate::metadata::ClusterMetadata;
use crate::metrics::ConnectionMetrics;
use crate::network::{ConnectionConfig, ConnectionPool};
use crate::protocol::{
    ApiKey, FindCoordinatorRequest, FindCoordinatorResponse, RecordBatch,
    ShareAcknowledgePartition, ShareAcknowledgeRequest, ShareAcknowledgeTopic,
    ShareAcknowledgementBatch, ShareFetchPartition, ShareFetchRequest, ShareFetchTopic,
    ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse, ShareGroupTopicPartitions,
    VersionedDecode, VersionedEncode, versions,
};
use crate::{BrokerId, Offset, PartitionId};

use session::ShareSessionCache;

/// Key for tracking unacknowledged records in explicit mode.
type RecordKey = (String, PartitionId, Offset);

/// Key for piggybacked acknowledgements grouped by broker and partition.
type BrokerAckKey = ([u8; 16], PartitionId);

/// Pending piggybacked acknowledgements grouped by broker and partition.
type BrokerPendingAcks = HashMap<BrokerId, HashMap<BrokerAckKey, Vec<PendingAck>>>;

#[derive(Clone)]
struct ShareAcknowledgeContext {
    metadata: Arc<ClusterMetadata>,
    pool: Arc<ConnectionPool>,
    share_sessions: Arc<tokio::sync::Mutex<ShareSessionCache>>,
    group_id: String,
    member_id: String,
    current_ack_state_generation: Arc<AtomicU64>,
    ack_state_generation: u64,
}

/// Pending acknowledgement for a share group record.
#[derive(Debug, Clone)]
struct PendingAck {
    topic: String,
    topic_id: [u8; 16],
    partition: PartitionId,
    first_offset: Offset,
    last_offset: Offset,
    ack_type: i8,
}

fn flatten_partition_acks(
    partition_acks: HashMap<BrokerAckKey, Vec<PendingAck>>,
) -> Vec<PendingAck> {
    partition_acks.into_values().flatten().collect()
}

fn drain_broker_partition_acks(
    broker_acks: &mut HashMap<BrokerAckKey, Vec<PendingAck>>,
    topic_id: [u8; 16],
    partition: PartitionId,
) -> Vec<PendingAck> {
    broker_acks
        .remove(&(topic_id, partition))
        .unwrap_or_default()
}

fn drain_broker_acks(broker_acks: &mut BrokerPendingAcks, broker_id: BrokerId) -> Vec<PendingAck> {
    broker_acks
        .remove(&broker_id)
        .map(flatten_partition_acks)
        .unwrap_or_default()
}

fn describe_share_fetch_join_error(error: &tokio::task::JoinError) -> &'static str {
    if error.is_panic() {
        "panicked"
    } else if error.is_cancelled() {
        "was cancelled"
    } else {
        "failed"
    }
}

/// Handle returned by [`ShareConsumer::commit_async`].
///
/// Await the handle to observe the final broker outcome. Dropping it detaches
/// the background task and discards the result.
#[must_use = "await the returned handle to observe share-commit outcome"]
pub enum ShareCommitHandle {
    /// Immediate commit result without spawning a background task.
    Ready(Ready<Result<()>>),
    /// Background task handle that resolves to the commit result.
    Task(tokio::task::JoinHandle<Result<()>>),
}

impl ShareCommitHandle {
    fn ready(result: Result<()>) -> Self {
        Self::Ready(ready(result))
    }
}

impl Future for ShareCommitHandle {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Ready(fut) => Pin::new(fut).poll(cx),
            Self::Task(handle) => match Pin::new(handle).poll(cx) {
                Poll::Ready(Ok(result)) => Poll::Ready(result),
                Poll::Ready(Err(error)) => Poll::Ready(Err(KrafkaError::invalid_state(format!(
                    "share commit task failed: {error}"
                )))),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

/// A Kafka share consumer (KIP-932).
///
/// Provides queue-like consumption semantics where the server controls
/// partition assignment and record delivery. Unlike traditional consumer
/// groups, share consumers do not track offsets — instead they acknowledge
/// individual records.
pub struct ShareConsumer {
    /// Configuration.
    config: ShareConsumerConfig,
    /// Cluster metadata.
    metadata: Arc<ClusterMetadata>,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Subscribed topics.
    subscriptions: RwLock<HashSet<String>>,
    /// Current partition assignments from the coordinator.
    /// Maps topic name → partition IDs.
    assignments: RwLock<HashMap<String, Vec<PartitionId>>>,
    /// Client-generated member ID (UUID, per KIP-932).
    member_id: RwLock<String>,
    /// Current member epoch.
    member_epoch: RwLock<i32>,
    /// Heartbeat interval returned by the coordinator.
    heartbeat_interval_ms: RwLock<i32>,
    /// Whether the consumer is closed.
    closed: AtomicBool,
    /// Per-broker share session cache.
    share_sessions: Arc<tokio::sync::Mutex<ShareSessionCache>>,
    /// Pending acknowledgements (accumulated between polls or before commit).
    pending_acks: Arc<RwLock<Vec<PendingAck>>>,
    /// Monotonic token for the current local ack state.
    ///
    /// Incremented whenever local ack state is cleared or invalidated so
    /// detached flush tasks cannot send or requeue stale acknowledgements
    /// from an older membership/session after assignment changes,
    /// `unsubscribe()`, or `close()`.
    ack_state_generation: Arc<AtomicU64>,
    /// Explicit-mode barrier raised after a commit flush fails.
    ///
    /// While this is set, `poll()` refuses to fetch more records until the
    /// application retries `commit_sync()`/`commit_async()` successfully or
    /// local state is cleared during unsubscribe/close.
    explicit_flush_retry_required: Arc<AtomicBool>,
    /// Topic name → UUID cache (populated from heartbeat assignments and metadata).
    topic_ids: RwLock<HashMap<String, [u8; 16]>>,
    /// Buffer for records returned by `recv()`.
    recv_buffer: RwLock<VecDeque<ConsumerRecord>>,
    /// Coordinator broker ID (discovered via FindCoordinator).
    coordinator_id: RwLock<Option<BrokerId>>,
    /// Coordinator address (host:port).
    coordinator_address: RwLock<Option<String>>,
    /// Tracks unacknowledged records from the previous `poll()` in explicit mode.
    /// Must be empty before the next `poll()` can fetch new records.
    unacked_offsets: Arc<RwLock<HashSet<RecordKey>>>,
}

impl std::fmt::Debug for ShareConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareConsumer")
            .field("group_id", &self.config.group_id)
            .field("closed", &self.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl Drop for ShareConsumer {
    fn drop(&mut self) {
        // Warn when the consumer is dropped without an explicit `close()`.
        // Without `close()` the coordinator will only reassign this
        // member's partitions after its heartbeat lease expires, and any
        // pending acknowledgements are discarded. Skip during panic unwinding.
        if !self.closed.load(Ordering::SeqCst) && !std::thread::panicking() {
            warn!(
                "ShareConsumer dropped without close(); pending acks may be lost and \
                 share-group rebalance will be delayed. Call `ShareConsumer::close()` before drop."
            );
        }
    }
}

impl ShareConsumer {
    /// Create a new share consumer builder.
    pub fn builder() -> ShareConsumerBuilder {
        ShareConsumerBuilder::default()
    }

    /// Create a new share consumer with the given configuration.
    async fn new(config: ShareConsumerConfig) -> Result<Self> {
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

        metadata.refresh().await?;

        info!(
            "ShareConsumer initialized with {} brokers, group_id='{}'",
            metadata.brokers().len(),
            config.group_id
        );

        Ok(Self {
            config,
            metadata,
            pool,
            subscriptions: RwLock::new(HashSet::new()),
            assignments: RwLock::new(HashMap::new()),
            member_id: RwLock::new(crate::util::random_uuid_v4()),
            member_epoch: RwLock::new(0),
            heartbeat_interval_ms: RwLock::new(5000),
            closed: AtomicBool::new(false),
            share_sessions: Arc::new(tokio::sync::Mutex::new(ShareSessionCache::new())),
            pending_acks: Arc::new(RwLock::new(Vec::new())),
            ack_state_generation: Arc::new(AtomicU64::new(0)),
            explicit_flush_retry_required: Arc::new(AtomicBool::new(false)),
            topic_ids: RwLock::new(HashMap::new()),
            recv_buffer: RwLock::new(VecDeque::new()),
            coordinator_id: RwLock::new(None),
            coordinator_address: RwLock::new(None),
            unacked_offsets: Arc::new(RwLock::new(HashSet::new())),
        })
    }

    /// Subscribe to topics.
    ///
    /// Replaces the current subscription. The coordinator is notified on
    /// the next heartbeat (during `poll()`).
    pub async fn subscribe(&self, topics: &[&str]) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(KrafkaError::invalid_state("share consumer is closed"));
        }

        {
            let mut subs = self.subscriptions.write().await;
            subs.clear();
            for topic in topics {
                subs.insert((*topic).to_string());
            }
        }

        let topic_refs: Vec<&str> = topics.to_vec();
        self.metadata.refresh_for_topics(Some(&topic_refs)).await?;

        // Resolve topic UUIDs from metadata.
        {
            let mut ids = self.topic_ids.write().await;
            for topic in topics {
                if let Some(uuid) = self.metadata.topic_id_for_name(topic) {
                    ids.insert((*topic).to_string(), uuid);
                }
            }
        }

        // Discover the coordinator and send the initial heartbeat.
        self.ensure_coordinator().await?;
        self.send_heartbeat(true).await?;

        debug!(
            "Subscribed to {} topic(s) in share group '{}'",
            topics.len(),
            self.config.group_id
        );
        Ok(())
    }

    /// Returns the current subscription.
    pub async fn subscription(&self) -> HashSet<String> {
        self.subscriptions.read().await.clone()
    }

    /// Returns the current partition assignments.
    pub async fn assignment(&self) -> HashMap<String, Vec<PartitionId>> {
        self.assignments.read().await.clone()
    }

    /// Returns the member ID assigned by the coordinator.
    pub async fn member_id(&self) -> String {
        self.member_id.read().await.clone()
    }

    /// Returns the current member epoch.
    pub async fn member_epoch(&self) -> i32 {
        *self.member_epoch.read().await
    }

    /// Poll for new records.
    ///
    /// In implicit acknowledgement mode, previously fetched records are
    /// automatically accepted. In explicit mode, all records from the
    /// previous poll must be acknowledged before calling this again.
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(KrafkaError::invalid_state("share consumer is closed"));
        }

        // Explicit mode: reject poll if records from the previous batch are unacknowledged.
        if self.config.acknowledgement_mode == AcknowledgementMode::Explicit {
            let unacked = self.unacked_offsets.read().await;
            if !unacked.is_empty() {
                return Err(KrafkaError::invalid_state(
                    "all records from the previous poll() must be acknowledged before calling poll() again",
                ));
            }
            if self.explicit_flush_retry_required.load(Ordering::SeqCst) {
                return Err(KrafkaError::invalid_state(
                    "the previous commit_sync()/commit_async() flush failed; retry the commit before calling poll() again",
                ));
            }
        }

        // If recv_buffer is at capacity, skip only the fetch step (after
        // heartbeat/coordination) so group membership remains healthy.
        let max_buffered = self.config.max_buffered_records;
        let skip_fetch_due_to_buffer_cap = if max_buffered > 0 {
            let buf_len = self.recv_buffer.read().await.len();
            buf_len >= max_buffered as usize
        } else {
            false
        };

        // Send heartbeat to maintain membership and receive assignments.
        // Cap the heartbeat RPC at 10 s so a slow/stuck coordinator does not
        // block the entire poll() for the full connection-level request_timeout.
        let heartbeat_result =
            tokio::time::timeout(Duration::from_secs(10), self.send_heartbeat(false)).await;
        let heartbeat_err = match heartbeat_result {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(e),
            Err(_elapsed) => Some(KrafkaError::timeout("share group heartbeat")),
        };
        if let Some(e) = heartbeat_err {
            warn!("Heartbeat failed during poll: {e}");
            self.invalidate_coordinator().await;
            if let Err(e2) = self.ensure_coordinator().await {
                warn!("Coordinator rediscovery failed: {e2}");
            }
        }

        let assignments = self.assignments.read().await.clone();
        if assignments.is_empty() || skip_fetch_due_to_buffer_cap {
            return Ok(Vec::new());
        }

        // Group partitions by leader broker.
        let mut partitions_by_broker: HashMap<BrokerId, Vec<(String, PartitionId, [u8; 16])>> =
            HashMap::new();
        let topic_ids = self.topic_ids.read().await;
        for (topic, partitions) in &assignments {
            let Some(&topic_id) = topic_ids.get(topic) else {
                debug!("No topic UUID for '{topic}', skipping");
                continue;
            };
            for &partition in partitions {
                if let Some(leader) = self.metadata.leader(topic, partition) {
                    partitions_by_broker.entry(leader).or_default().push((
                        topic.clone(),
                        partition,
                        topic_id,
                    ));
                }
            }
        }
        drop(topic_ids);

        let ack_state_generation = self.ack_state_generation.load(Ordering::SeqCst);

        let sendable_ack_partitions: HashSet<(String, PartitionId)> = partitions_by_broker
            .values()
            .flat_map(|partitions| {
                partitions
                    .iter()
                    .map(|(topic, partition, _)| (topic.clone(), *partition))
            })
            .collect();

        // Drain acknowledgement batches to piggyback on fetch requests.
        let drained_ack_batches = {
            let mut pending = self.pending_acks.write().await;
            std::mem::take(&mut *pending)
        };
        let (ack_batches, mut failed_piggyback_acks): (Vec<_>, Vec<_>) = drained_ack_batches
            .into_iter()
            .partition(|ack| sendable_ack_partitions.contains(&(ack.topic.clone(), ack.partition)));
        let mut ack_batches_by_broker: BrokerPendingAcks = HashMap::new();
        for ack in ack_batches {
            if let Some(broker_id) = self.metadata.leader(&ack.topic, ack.partition) {
                ack_batches_by_broker
                    .entry(broker_id)
                    .or_default()
                    .entry((ack.topic_id, ack.partition))
                    .or_default()
                    .push(ack);
            } else {
                failed_piggyback_acks.push(ack);
            }
        }

        // Fetch from all brokers concurrently.
        let mut fetch_tasks = Vec::with_capacity(partitions_by_broker.len());
        let member_id = self.member_id.read().await.clone();
        let group_id = self.config.group_id.clone();
        let current_ack_state_generation = self.ack_state_generation.clone();

        for (broker_id, partitions) in &partitions_by_broker {
            let session_epoch = {
                let mut sessions = self.share_sessions.lock().await;
                sessions.get_or_create(*broker_id).epoch()
            };

            // Build per-topic partition requests with piggybacked acks.
            let mut topics_map: HashMap<[u8; 16], Vec<ShareFetchPartition>> = HashMap::new();
            let broker_ack_partitions = ack_batches_by_broker.get(broker_id);
            for (_, partition, topic_id) in partitions {
                let ack_batches_for_partition: Vec<ShareAcknowledgementBatch> =
                    broker_ack_partitions
                        .and_then(|partition_acks| partition_acks.get(&(*topic_id, *partition)))
                        .map(|partition_acks| {
                            partition_acks
                                .iter()
                                .map(|a| ShareAcknowledgementBatch {
                                    first_offset: a.first_offset,
                                    last_offset: a.last_offset,
                                    acknowledge_types: vec![a.ack_type],
                                })
                                .collect()
                        })
                        .unwrap_or_default();

                topics_map
                    .entry(*topic_id)
                    .or_default()
                    .push(ShareFetchPartition {
                        partition_index: *partition,
                        acknowledgement_batches: ack_batches_for_partition,
                    });
            }

            let topics: Vec<ShareFetchTopic> = topics_map
                .into_iter()
                .map(|(topic_id, partitions)| ShareFetchTopic {
                    topic_id,
                    partitions,
                })
                .collect();

            let request = ShareFetchRequest {
                group_id: Some(group_id.clone()),
                member_id: Some(member_id.clone()),
                share_session_epoch: session_epoch,
                max_wait_ms: timeout.as_millis().min(i32::MAX as u128) as i32,
                min_bytes: self.config.fetch_min_bytes,
                max_bytes: self.config.fetch_max_bytes,
                max_records: self.config.max_records,
                batch_size: self.config.batch_size,
                topics,
                forgotten_topics: Vec::new(),
            };

            let bid = *broker_id;
            let metadata = self.metadata.clone();
            let pool = self.pool.clone();
            let current_ack_state_generation = current_ack_state_generation.clone();
            let task = tokio::spawn(async move {
                ShareConsumer::ensure_ack_state_current(
                    current_ack_state_generation.as_ref(),
                    ack_state_generation,
                )?;

                let broker_addr = metadata
                    .broker(bid)
                    .map(|b| b.address().to_string())
                    .ok_or_else(|| {
                        KrafkaError::invalid_state(format!("broker {bid} not found in metadata"))
                    })?;
                let conn = pool.get_connection_by_id(bid, &broker_addr).await?;
                let version = conn
                    .negotiate_api_version(
                        ApiKey::ShareFetch,
                        versions::SHARE_FETCH_MAX,
                        versions::SHARE_FETCH_MIN,
                    )
                    .await
                    .ok_or_else(|| KrafkaError::protocol("broker does not support ShareFetch"))?;

                ShareConsumer::ensure_ack_state_current(
                    current_ack_state_generation.as_ref(),
                    ack_state_generation,
                )?;

                let buf = conn
                    .send_request(ApiKey::ShareFetch, version, |buf| match version {
                        2 => request.encode_v2(buf, 0, false),
                        _ => request.encode_v1(buf),
                    })
                    .await?;

                let response = crate::protocol::ShareFetchResponse::decode_versioned(
                    version,
                    &mut buf.as_ref(),
                )?;

                // KIP-219: honour broker-reported throttle time.
                conn.notify_throttle(response.throttle_time_ms);

                Result::<(BrokerId, crate::protocol::ShareFetchResponse)>::Ok((bid, response))
            });
            fetch_tasks.push((bid, task));
        }

        // Collect results from all brokers.
        let mut all_records = Vec::new();
        let topic_ids_guard = self.topic_ids.read().await;

        for (broker_id, task) in fetch_tasks {
            match task.await {
                Ok(Ok((_, response))) => {
                    let mut broker_acks =
                        ack_batches_by_broker.remove(&broker_id).unwrap_or_default();

                    if !response.error_code.is_ok() {
                        failed_piggyback_acks.extend(flatten_partition_acks(broker_acks));
                        warn!(
                            "ShareFetch to broker {broker_id} returned {:?}: {}",
                            response.error_code,
                            response.error_message.as_deref().unwrap_or("unknown error")
                        );
                        let mut sessions = self.share_sessions.lock().await;
                        sessions.reset_broker(broker_id);
                        continue;
                    }

                    // Update session state on success.
                    {
                        let mut sessions = self.share_sessions.lock().await;
                        sessions.get_or_create(broker_id).on_success();
                    }

                    // Decode records from the response and restore only the
                    // partitions whose piggybacked acknowledgements failed.
                    for topic_response in &response.responses {
                        let topic_name = if let Some(name) =
                            self.metadata.topic_name_for_id(&topic_response.topic_id)
                        {
                            name
                        } else {
                            let found = topic_ids_guard.iter().find_map(|(name, &id)| {
                                if id == topic_response.topic_id {
                                    Some(name.clone())
                                } else {
                                    None
                                }
                            });
                            match found {
                                Some(name) => name,
                                None => {
                                    debug!(
                                        "Unknown topic UUID {:?} in ShareFetch response, skipping",
                                        topic_response.topic_id
                                    );
                                    continue;
                                }
                            }
                        };

                        for partition_response in &topic_response.partitions {
                            let partition_acks = drain_broker_partition_acks(
                                &mut broker_acks,
                                topic_response.topic_id,
                                partition_response.partition_index,
                            );

                            if !partition_response.error_code.is_ok() {
                                failed_piggyback_acks.extend(partition_acks);
                                warn!(
                                    "ShareFetch error for {topic_name}-{}: {:?}",
                                    partition_response.partition_index,
                                    partition_response.error_code
                                );
                                continue;
                            }

                            if !partition_response.acknowledge_error_code.is_ok() {
                                failed_piggyback_acks.extend(partition_acks);
                                warn!(
                                    "Piggybacked ShareFetch acknowledge error for {topic_name}-{}: {:?}: {}",
                                    partition_response.partition_index,
                                    partition_response.acknowledge_error_code,
                                    partition_response
                                        .acknowledge_error_message
                                        .as_deref()
                                        .unwrap_or("unknown error")
                                );
                                continue;
                            }

                            // Build delivery count map from acquired_records.
                            let mut delivery_counts: HashMap<Offset, i16> = HashMap::new();
                            for acquired in &partition_response.acquired_records {
                                for offset in acquired.first_offset..=acquired.last_offset {
                                    delivery_counts.insert(offset, acquired.delivery_count);
                                }
                            }

                            // Decode record batches.
                            if let Some(ref raw) = partition_response.records {
                                let mut cursor = raw.as_ref();
                                while !cursor.is_empty() {
                                    match RecordBatch::decode_with_limit(
                                        &mut cursor,
                                        self.config.max_decompressed_size,
                                    ) {
                                        Ok(batch) => {
                                            for record in batch.records {
                                                let record_offset =
                                                    batch.base_offset + record.offset_delta as i64;
                                                let delivery_count =
                                                    delivery_counts.get(&record_offset).copied();
                                                all_records.push(ConsumerRecord {
                                                    topic: topic_name.clone(),
                                                    partition: partition_response.partition_index,
                                                    offset: record_offset,
                                                    timestamp: batch
                                                        .base_timestamp
                                                        .saturating_add(record.timestamp_delta),
                                                    timestamp_type: batch.attributes.timestamp_type
                                                        as i8,
                                                    key: record.key,
                                                    value: record.value,
                                                    headers: record
                                                        .headers
                                                        .into_iter()
                                                        .map(|h| (h.key, h.value))
                                                        .collect(),
                                                    leader_epoch: None,
                                                    delivery_count,
                                                });
                                            }
                                        }
                                        Err(e) => {
                                            debug!(
                                                "Failed to decode record batch for {topic_name}-{}: {e}",
                                                partition_response.partition_index
                                            );
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }

                    failed_piggyback_acks.extend(flatten_partition_acks(broker_acks));
                }
                Ok(Err(e)) => {
                    failed_piggyback_acks
                        .extend(drain_broker_acks(&mut ack_batches_by_broker, broker_id));
                    warn!("ShareFetch to broker {broker_id} failed: {e}");
                }
                Err(e) => {
                    failed_piggyback_acks
                        .extend(drain_broker_acks(&mut ack_batches_by_broker, broker_id));
                    warn!(
                        "ShareFetch task for broker {broker_id} {}: {e}",
                        describe_share_fetch_join_error(&e)
                    );
                }
            }
        }
        drop(topic_ids_guard);

        failed_piggyback_acks.extend(
            ack_batches_by_broker
                .drain()
                .flat_map(|(_, acks)| flatten_partition_acks(acks))
                .collect::<Vec<_>>(),
        );
        self.restore_pending_acks(ack_state_generation, failed_piggyback_acks, false)
            .await;

        // In implicit mode, queue all fetched records as coalesced accepts for next poll.
        if self.config.acknowledgement_mode == AcknowledgementMode::Implicit {
            let ids = self.topic_ids.read().await;
            let mut pending = self.pending_acks.write().await;
            Self::coalesce_implicit_acks(&all_records, &ids, &mut pending);
        }

        // In explicit mode, track all returned records as unacknowledged.
        if self.config.acknowledgement_mode == AcknowledgementMode::Explicit {
            let mut unacked = self.unacked_offsets.write().await;
            for record in &all_records {
                unacked.insert((record.topic.clone(), record.partition, record.offset));
            }
        }

        // Truncate to max_poll_records.
        let max = self.config.max_poll_records as usize;
        if all_records.len() > max {
            all_records.truncate(max);
        }

        Ok(all_records)
    }

    /// Acknowledge a record with the given type (explicit mode only).
    ///
    /// In explicit acknowledgement mode, call this for each record before
    /// calling [`commit_sync()`](Self::commit_sync). All records from the
    /// previous `poll()` must be acknowledged before calling `poll()` again.
    pub async fn acknowledge(
        &self,
        record: &ConsumerRecord,
        ack_type: AcknowledgeType,
    ) -> Result<()> {
        if self.config.acknowledgement_mode != AcknowledgementMode::Explicit {
            return Err(KrafkaError::invalid_state(
                "acknowledge() requires explicit acknowledgement mode",
            ));
        }

        let topic_ids = self.topic_ids.read().await;
        let topic_id = topic_ids.get(&record.topic).copied().ok_or_else(|| {
            KrafkaError::invalid_state(format!("no topic UUID for '{}'", record.topic))
        })?;
        drop(topic_ids);

        let record_key = (record.topic.clone(), record.partition, record.offset);
        let mut pending = self.pending_acks.write().await;
        let mut unacked = self.unacked_offsets.write().await;
        if !unacked.contains(&record_key) {
            return Err(KrafkaError::invalid_state(format!(
                "record {}-{}@{} is not pending acknowledgement",
                record.topic, record.partition, record.offset
            )));
        }
        pending.push(PendingAck {
            topic: record.topic.clone(),
            topic_id,
            partition: record.partition,
            first_offset: record.offset,
            last_offset: record.offset,
            ack_type: ack_type.to_i8(),
        });
        unacked.remove(&record_key);

        Ok(())
    }

    async fn restore_pending_acks(
        &self,
        ack_state_generation: u64,
        mut acks: Vec<PendingAck>,
        require_explicit_retry: bool,
    ) {
        Self::restore_ack_state(
            self.ack_state_generation.as_ref(),
            self.pending_acks.as_ref(),
            self.explicit_flush_retry_required.as_ref(),
            ack_state_generation,
            require_explicit_retry,
            &mut acks,
        )
        .await;
    }

    async fn restore_ack_state(
        current_generation: &AtomicU64,
        pending_acks: &RwLock<Vec<PendingAck>>,
        explicit_flush_retry_required: &AtomicBool,
        ack_state_generation: u64,
        require_explicit_retry: bool,
        acks: &mut Vec<PendingAck>,
    ) {
        if acks.is_empty() {
            return;
        }

        let mut pending = pending_acks.write().await;
        if current_generation.load(Ordering::SeqCst) != ack_state_generation {
            acks.clear();
            return;
        }
        if require_explicit_retry {
            explicit_flush_retry_required.store(true, Ordering::SeqCst);
        }
        pending.append(acks);
    }

    fn share_acknowledge_response_error(
        response: &crate::protocol::ShareAcknowledgeResponse,
    ) -> Option<KrafkaError> {
        if !response.error_code.is_ok() {
            return Some(KrafkaError::broker(
                response.error_code,
                response
                    .error_message
                    .clone()
                    .unwrap_or_else(|| "ShareAcknowledge failed".to_string()),
            ));
        }

        for topic_response in &response.responses {
            for part_response in &topic_response.partitions {
                if !part_response.error_code.is_ok() {
                    return Some(KrafkaError::broker(
                        part_response.error_code,
                        part_response.error_message.clone().unwrap_or_else(|| {
                            format!(
                                "ShareAcknowledge error for partition {}",
                                part_response.partition_index
                            )
                        }),
                    ));
                }
            }
        }

        None
    }

    /// Commit all pending acknowledgements synchronously.
    ///
    /// Sends a `ShareAcknowledge` request for all outstanding acknowledgements.
    /// In implicit mode, this flushes any buffered accepts.
    pub async fn commit_sync(&self) -> Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(KrafkaError::invalid_state("share consumer is closed"));
        }

        self.flush_pending_acks().await
    }

    async fn flush_pending_acks(&self) -> Result<()> {
        let ack_state_generation = self.ack_state_generation.load(Ordering::SeqCst);
        let acks = {
            let mut pending = self.pending_acks.write().await;
            std::mem::take(&mut *pending)
        };

        if acks.is_empty() {
            return Ok(());
        }

        match self.send_share_acknowledge(&acks).await {
            Ok(()) => {
                self.explicit_flush_retry_required
                    .store(false, Ordering::SeqCst);
                Ok(())
            }
            Err(error) => {
                self.restore_pending_acks(ack_state_generation, acks, true)
                    .await;
                Err(error)
            }
        }
    }

    /// Commit all pending acknowledgements asynchronously.
    ///
    /// Await the returned handle to observe transport, decode, and broker
    /// errors. If the handle is dropped, the task continues in the background
    /// and its result is discarded.
    pub fn commit_async(&self) -> ShareCommitHandle {
        if self.closed.load(Ordering::SeqCst) {
            return ShareCommitHandle::ready(Err(KrafkaError::invalid_state(
                "share consumer is closed",
            )));
        }

        let member_id = match self.member_id.try_read() {
            Ok(g) => g.clone(),
            Err(_) => {
                return ShareCommitHandle::ready(Err(KrafkaError::invalid_state(
                    "commit_async: member_id lock contention",
                )));
            }
        };

        let ack_state_generation = self.ack_state_generation.load(Ordering::SeqCst);
        let pending_acks = self.pending_acks.clone();
        let current_ack_state_generation = self.ack_state_generation.clone();
        let explicit_flush_retry_required = self.explicit_flush_retry_required.clone();
        let Ok(mut pending) = self.pending_acks.try_write() else {
            return ShareCommitHandle::ready(Err(KrafkaError::invalid_state(
                "commit_async: pending_acks lock contention",
            )));
        };
        let acks = std::mem::take(&mut *pending);
        drop(pending);

        if acks.is_empty() {
            return ShareCommitHandle::ready(Ok(()));
        }

        let metadata = self.metadata.clone();
        let pool = self.pool.clone();
        let share_sessions = self.share_sessions.clone();
        let group_id = self.config.group_id.clone();
        let send_ack_state_generation = current_ack_state_generation.clone();

        ShareCommitHandle::Task(tokio::spawn(async move {
            let restore_acks = |mut acks: Vec<PendingAck>| {
                let pending_acks = pending_acks.clone();
                let current_ack_state_generation = current_ack_state_generation.clone();
                let explicit_flush_retry_required = explicit_flush_retry_required.clone();
                async move {
                    ShareConsumer::restore_ack_state(
                        current_ack_state_generation.as_ref(),
                        pending_acks.as_ref(),
                        explicit_flush_retry_required.as_ref(),
                        ack_state_generation,
                        true,
                        &mut acks,
                    )
                    .await;
                }
            };
            if let Err(error) = ShareConsumer::send_share_acknowledge_with_state(
                ShareAcknowledgeContext {
                    metadata,
                    pool,
                    share_sessions,
                    group_id,
                    member_id,
                    current_ack_state_generation: send_ack_state_generation,
                    ack_state_generation,
                },
                &acks,
            )
            .await
            {
                restore_acks(acks).await;
                return Err(error);
            }

            explicit_flush_retry_required.store(false, Ordering::SeqCst);

            Ok(())
        }))
    }

    /// Receive a single record (convenience wrapper over `poll()`).
    ///
    /// Buffers records internally; returns `None` when the consumer is closed.
    pub async fn recv(&self) -> Result<Option<ConsumerRecord>> {
        // Return from buffer first.
        {
            let mut buf = self.recv_buffer.write().await;
            if let Some(record) = buf.pop_front() {
                return Ok(Some(record));
            }
        }

        if self.closed.load(Ordering::SeqCst) {
            return Ok(None);
        }

        let records = self.poll(Duration::from_secs(1)).await?;
        if records.is_empty() {
            return Ok(None);
        }

        let mut buf = self.recv_buffer.write().await;
        let mut iter = records.into_iter();
        let first = iter.next();
        // Preserve overflow records so recv() never drops data.
        for record in iter {
            buf.push_back(record);
        }

        Ok(first)
    }

    /// Create an async stream of records.
    pub fn stream(&self) -> ShareConsumerStream<'_> {
        ShareConsumerStream::new(self)
    }

    /// Unsubscribe from all topics.
    ///
    /// Sends a leave heartbeat (member_epoch = -1) and clears state.
    pub async fn unsubscribe(&self) {
        // Leave group via heartbeat with epoch -1.
        if let Err(e) = self.leave_group().await {
            warn!("Leave group failed during unsubscribe: {e}");
        }

        self.subscriptions.write().await.clear();
        self.assignments.write().await.clear();
        self.clear_partition_state().await;
        *self.member_id.write().await = crate::util::random_uuid_v4();
        *self.member_epoch.write().await = 0;

        debug!("Unsubscribed from share group '{}'", self.config.group_id);
    }

    /// Close the share consumer.
    ///
    /// In implicit mode, unreleased records are released (not accepted) so
    /// they become available for other consumers. In explicit mode, pending
    /// acks are flushed. Leaves the group and closes all connections.
    /// Idempotent.
    ///
    /// Returns the first cleanup error after local state and connections have
    /// still been closed.
    pub async fn close(&self) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        // In implicit mode, convert pending accepts to releases so acquired
        // records are returned to the pool for redelivery (KIP-932 §close).
        if self.config.acknowledgement_mode == AcknowledgementMode::Implicit {
            let mut pending = self.pending_acks.write().await;
            for ack in pending.iter_mut() {
                ack.ack_type = AcknowledgeType::Release.to_i8();
            }
        }

        let commit_result = self.flush_pending_acks().await;

        // Leave the group.
        let leave_result = self.leave_group().await;

        // Clear state.
        self.subscriptions.write().await.clear();
        self.assignments.write().await.clear();
        self.clear_partition_state().await;

        self.pool.close_all().await;

        info!("ShareConsumer closed (group '{}')", self.config.group_id);

        commit_result?;
        leave_result
    }

    /// Returns true if the consumer has been closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Get the shared connection metrics handle used by this share consumer's broker pool.
    #[inline]
    pub fn connection_metrics(&self) -> Arc<ConnectionMetrics> {
        self.pool.metrics()
    }

    // ── Internal helpers ────────────────────────────────────────────────

    fn invalidate_ack_state(&self) {
        self.ack_state_generation.fetch_add(1, Ordering::SeqCst);
        self.explicit_flush_retry_required
            .store(false, Ordering::SeqCst);
    }

    async fn clear_ack_state(&self) {
        self.invalidate_ack_state();
        self.pending_acks.write().await.clear();
        self.unacked_offsets.write().await.clear();
    }

    /// Clear all per-partition state. Called from `unsubscribe()` and `close()`.
    async fn clear_partition_state(&self) {
        self.clear_ack_state().await;
        self.recv_buffer.write().await.clear();
        self.share_sessions.lock().await.reset_all();
        *self.coordinator_id.write().await = None;
        *self.coordinator_address.write().await = None;
    }

    /// Invalidate the cached coordinator. The next `ensure_coordinator()` call
    /// will re-discover it. Called on NOT_COORDINATOR errors or heartbeat failures.
    async fn invalidate_coordinator(&self) {
        *self.coordinator_id.write().await = None;
        *self.coordinator_address.write().await = None;
    }

    /// Coalesce implicit accept acks — merge consecutive offsets for the same
    /// (topic, partition) into a single `PendingAck` with a contiguous range.
    fn coalesce_implicit_acks(
        records: &[ConsumerRecord],
        topic_ids: &HashMap<String, [u8; 16]>,
        pending: &mut Vec<PendingAck>,
    ) {
        // Group by (topic, partition) and sort offsets.
        let mut by_tp: HashMap<(&str, PartitionId), Vec<Offset>> = HashMap::new();
        for record in records {
            by_tp
                .entry((&record.topic, record.partition))
                .or_default()
                .push(record.offset);
        }

        for ((topic, partition), mut offsets) in by_tp {
            let Some(&topic_id) = topic_ids.get(topic) else {
                continue;
            };
            offsets.sort_unstable();

            // Merge consecutive offsets into contiguous ranges.
            let mut i = 0;
            while i < offsets.len() {
                let first = offsets[i];
                let mut last = first;
                while i + 1 < offsets.len() && offsets[i + 1] == last + 1 {
                    i += 1;
                    last = offsets[i];
                }
                pending.push(PendingAck {
                    topic: topic.to_string(),
                    topic_id,
                    partition,
                    first_offset: first,
                    last_offset: last,
                    ack_type: AcknowledgeType::Accept.to_i8(),
                });
                i += 1;
            }
        }
    }

    /// Build `ShareAcknowledgeTopic` list from pending acks. Groups by
    /// topic UUID and partition, coalescing ack batches per partition.
    fn build_acknowledge_topics(acks: &[PendingAck]) -> Vec<ShareAcknowledgeTopic> {
        let mut topics_map: HashMap<
            [u8; 16],
            HashMap<PartitionId, Vec<ShareAcknowledgementBatch>>,
        > = HashMap::new();
        for ack in acks {
            topics_map
                .entry(ack.topic_id)
                .or_default()
                .entry(ack.partition)
                .or_default()
                .push(ShareAcknowledgementBatch {
                    first_offset: ack.first_offset,
                    last_offset: ack.last_offset,
                    acknowledge_types: vec![ack.ack_type],
                });
        }

        topics_map
            .into_iter()
            .map(|(topic_id, partitions_map)| ShareAcknowledgeTopic {
                topic_id,
                partitions: partitions_map
                    .into_iter()
                    .map(
                        |(partition_index, acknowledgement_batches)| ShareAcknowledgePartition {
                            partition_index,
                            acknowledgement_batches,
                        },
                    )
                    .collect(),
            })
            .collect()
    }

    /// Discover the share group coordinator via FindCoordinator.
    async fn ensure_coordinator(&self) -> Result<()> {
        if self.coordinator_id.read().await.is_some() {
            return Ok(());
        }

        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::invalid_state("no brokers available"));
        }

        // Try each broker until we find the coordinator.
        let request = FindCoordinatorRequest::for_group(&self.config.group_id);
        for broker in &brokers {
            let conn = match self
                .pool
                .get_connection_by_id(broker.id, broker.address())
                .await
            {
                Ok(c) => c,
                Err(_) => continue,
            };

            let version = match conn
                .negotiate_api_version(
                    ApiKey::FindCoordinator,
                    versions::FIND_COORDINATOR_MAX,
                    versions::FIND_COORDINATOR_MIN,
                )
                .await
            {
                Some(v) => v,
                None => continue,
            };

            let result = conn
                .send_request(ApiKey::FindCoordinator, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await;

            let buf = match result {
                Ok(b) => b,
                Err(e) => {
                    debug!("FindCoordinator via broker {} failed: {e}", broker.id);
                    continue;
                }
            };

            let response = FindCoordinatorResponse::decode_versioned(version, &mut buf.as_ref())?;

            if response.error_code.is_ok() {
                let coord_id = response.node_id;
                let coord_addr = format!("{}:{}", response.host, response.port);
                *self.coordinator_id.write().await = Some(coord_id);
                *self.coordinator_address.write().await = Some(coord_addr);
                debug!(
                    "Share group '{}' coordinator is broker {coord_id}",
                    self.config.group_id
                );
                return Ok(());
            }

            debug!(
                "FindCoordinator returned {:?} for group '{}', trying next broker",
                response.error_code, self.config.group_id
            );
        }

        Err(KrafkaError::invalid_state(format!(
            "could not discover coordinator for share group '{}'",
            self.config.group_id
        )))
    }

    /// Send a ShareGroupHeartbeat to the coordinator.
    ///
    /// If `send_subscription` is true, the subscribed topic names are included.
    /// Returns the heartbeat response.
    async fn send_heartbeat(&self, send_subscription: bool) -> Result<()> {
        let coord_id = self
            .coordinator_id
            .read()
            .await
            .ok_or_else(|| KrafkaError::invalid_state("no coordinator discovered"))?;

        let member_id = self.member_id.read().await.clone();
        let member_epoch = *self.member_epoch.read().await;

        let subscribed_topic_names = if send_subscription {
            Some(
                self.subscriptions
                    .read()
                    .await
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };

        let request = ShareGroupHeartbeatRequest {
            group_id: self.config.group_id.clone(),
            member_id: member_id.clone(),
            member_epoch,
            rack_id: self.config.client_rack.clone(),
            subscribed_topic_names,
        };

        let coord_addr = self
            .coordinator_address
            .read()
            .await
            .clone()
            .ok_or_else(|| KrafkaError::invalid_state("no coordinator address"))?;
        let conn = self
            .pool
            .get_connection_by_id(coord_id, &coord_addr)
            .await?;
        let version = conn
            .negotiate_api_version(
                ApiKey::ShareGroupHeartbeat,
                versions::SHARE_GROUP_HEARTBEAT_MAX,
                versions::SHARE_GROUP_HEARTBEAT_MIN,
            )
            .await
            .ok_or_else(|| KrafkaError::protocol("broker does not support ShareGroupHeartbeat"))?;

        let buf = conn
            .send_request(ApiKey::ShareGroupHeartbeat, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let response = ShareGroupHeartbeatResponse::decode_versioned(version, &mut buf.as_ref())?;

        if !response.error_code.is_ok() {
            return Err(KrafkaError::broker(
                response.error_code,
                response
                    .error_message
                    .unwrap_or_else(|| "ShareGroupHeartbeat failed".to_string()),
            ));
        }

        // Update member state from response.
        if let Some(new_member_id) = response.member_id {
            *self.member_id.write().await = new_member_id;
        }
        *self.member_epoch.write().await = response.member_epoch;
        // Clamp the broker-supplied heartbeat interval to [50 ms, 30 s] to
        // prevent excessively fast polling (which exhausts broker connections)
        // or excessively slow polling (which causes session timeouts).
        let raw_interval_ms = response.heartbeat_interval_ms;
        const HEARTBEAT_MIN_MS: i32 = 50;
        const HEARTBEAT_MAX_MS: i32 = 30_000;
        let clamped_interval_ms = raw_interval_ms.clamp(HEARTBEAT_MIN_MS, HEARTBEAT_MAX_MS);
        if clamped_interval_ms != raw_interval_ms {
            tracing::warn!(
                raw_ms = raw_interval_ms,
                clamped_ms = clamped_interval_ms,
                min_ms = HEARTBEAT_MIN_MS,
                max_ms = HEARTBEAT_MAX_MS,
                "broker heartbeat_interval_ms is out of safe range; clamping"
            );
        }
        *self.heartbeat_interval_ms.write().await = clamped_interval_ms;

        // Process assignment if present.
        if let Some(assignment) = response.assignment {
            self.apply_assignment(&assignment).await;
        }

        Ok(())
    }

    /// Apply a partition assignment from the coordinator heartbeat response.
    async fn apply_assignment(&self, assignment: &[ShareGroupTopicPartitions]) {
        let mut new_assignments: HashMap<String, Vec<PartitionId>> = HashMap::new();
        let mut topic_ids_guard = self.topic_ids.write().await;

        for tp in assignment {
            // Resolve topic UUID to name.
            let topic_name = if let Some(name) = self.metadata.topic_name_for_id(&tp.topic_id) {
                topic_ids_guard.insert(name.clone(), tp.topic_id);
                name
            } else {
                // Cache miss — try a metadata refresh next time.
                debug!(
                    "Unknown topic UUID {:?} in share assignment, skipping",
                    tp.topic_id
                );
                continue;
            };

            new_assignments.insert(topic_name, tp.partitions.clone());
        }
        drop(topic_ids_guard);

        // Reset share sessions for brokers whose partitions changed.
        let old_assignments = self.assignments.read().await.clone();
        if old_assignments != new_assignments {
            debug!(
                "Share group assignment changed: {} topic(s), {} partition(s)",
                new_assignments.len(),
                new_assignments.values().map(|v| v.len()).sum::<usize>()
            );
            self.clear_ack_state().await;
            self.share_sessions.lock().await.reset_all();
        }

        *self.assignments.write().await = new_assignments;
    }

    /// Send a ShareAcknowledge request for pending acks.
    ///
    /// Routes acknowledgements to the correct partition leaders. Returns an
    /// error if any leader cannot be determined or any broker rejects the acks.
    async fn send_share_acknowledge(&self, acks: &[PendingAck]) -> Result<()> {
        let member_id = self.member_id.read().await.clone();
        Self::send_share_acknowledge_with_state(
            ShareAcknowledgeContext {
                metadata: self.metadata.clone(),
                pool: self.pool.clone(),
                share_sessions: self.share_sessions.clone(),
                group_id: self.config.group_id.clone(),
                member_id,
                current_ack_state_generation: self.ack_state_generation.clone(),
                ack_state_generation: self.ack_state_generation.load(Ordering::SeqCst),
            },
            acks,
        )
        .await
    }

    async fn send_share_acknowledge_with_state(
        context: ShareAcknowledgeContext,
        acks: &[PendingAck],
    ) -> Result<()> {
        let ShareAcknowledgeContext {
            metadata,
            pool,
            share_sessions,
            group_id,
            member_id,
            current_ack_state_generation,
            ack_state_generation,
        } = context;

        Self::ensure_ack_state_current(
            current_ack_state_generation.as_ref(),
            ack_state_generation,
        )?;

        // Group acks by partition leader.
        let mut broker_acks: HashMap<BrokerId, Vec<&PendingAck>> = HashMap::new();

        for ack in acks {
            let broker_id = metadata.leader(&ack.topic, ack.partition).ok_or_else(|| {
                KrafkaError::invalid_state(format!(
                    "no leader for {}-{} in metadata",
                    ack.topic, ack.partition
                ))
            })?;
            broker_acks.entry(broker_id).or_default().push(ack);
        }

        for (broker_id, broker_ack_list) in &broker_acks {
            Self::ensure_ack_state_current(
                current_ack_state_generation.as_ref(),
                ack_state_generation,
            )?;

            let topics = Self::build_acknowledge_topics(
                &broker_ack_list
                    .iter()
                    .map(|a| (*a).clone())
                    .collect::<Vec<_>>(),
            );

            let session_epoch = {
                let sessions = share_sessions.lock().await;
                sessions
                    .get(*broker_id)
                    .map(|s: &session::ShareSessionState| s.epoch())
                    .unwrap_or(0)
            };

            let request = ShareAcknowledgeRequest {
                group_id: Some(group_id.clone()),
                member_id: Some(member_id.clone()),
                share_session_epoch: session_epoch,
                topics,
            };

            let broker_addr = metadata
                .broker(*broker_id)
                .map(|b| b.address().to_string())
                .ok_or_else(|| {
                    KrafkaError::invalid_state(format!(
                        "broker {} not found in metadata",
                        broker_id
                    ))
                })?;
            let conn = pool.get_connection_by_id(*broker_id, &broker_addr).await?;
            let version = conn
                .negotiate_api_version(
                    ApiKey::ShareAcknowledge,
                    versions::SHARE_ACKNOWLEDGE_MAX,
                    versions::SHARE_ACKNOWLEDGE_MIN,
                )
                .await
                .ok_or_else(|| KrafkaError::protocol("broker does not support ShareAcknowledge"))?;

            Self::ensure_ack_state_current(
                current_ack_state_generation.as_ref(),
                ack_state_generation,
            )?;

            let buf = conn
                .send_request(ApiKey::ShareAcknowledge, version, |buf| match version {
                    2 => request.encode_v2(buf, false),
                    _ => request.encode_v1(buf),
                })
                .await?;

            let response = crate::protocol::ShareAcknowledgeResponse::decode_versioned(
                version,
                &mut buf.as_ref(),
            )?;

            if let Some(error) = Self::share_acknowledge_response_error(&response) {
                return Err(error);
            }
        }

        Ok(())
    }

    fn ensure_ack_state_current(
        current_generation: &AtomicU64,
        ack_state_generation: u64,
    ) -> Result<()> {
        if current_generation.load(Ordering::SeqCst) == ack_state_generation {
            return Ok(());
        }

        Err(KrafkaError::invalid_state(
            "share acknowledgement state was invalidated",
        ))
    }

    /// Leave the share group via heartbeat with member_epoch = -1.
    async fn leave_group(&self) -> Result<()> {
        let coord_id = match *self.coordinator_id.read().await {
            Some(id) => id,
            None => return Ok(()),
        };

        let member_id = self.member_id.read().await.clone();
        if member_id.is_empty() {
            return Ok(());
        }

        let request = ShareGroupHeartbeatRequest {
            group_id: self.config.group_id.clone(),
            member_id,
            member_epoch: -1, // Leave signal
            rack_id: None,
            subscribed_topic_names: None,
        };

        let coord_addr = match self.coordinator_address.read().await.clone() {
            Some(addr) => addr,
            None => return Ok(()),
        };

        let conn = self
            .pool
            .get_connection_by_id(coord_id, &coord_addr)
            .await?;

        let version = match conn
            .negotiate_api_version(
                ApiKey::ShareGroupHeartbeat,
                versions::SHARE_GROUP_HEARTBEAT_MAX,
                versions::SHARE_GROUP_HEARTBEAT_MIN,
            )
            .await
        {
            Some(v) => v,
            None => {
                return Err(KrafkaError::protocol(
                    "broker does not support ShareGroupHeartbeat",
                ));
            }
        };

        let buf = conn
            .send_request(ApiKey::ShareGroupHeartbeat, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await;

        let response = ShareGroupHeartbeatResponse::decode_versioned(version, &mut buf?.as_ref())?;

        if !response.error_code.is_ok() {
            return Err(KrafkaError::broker(
                response.error_code,
                response
                    .error_message
                    .unwrap_or_else(|| "ShareGroupHeartbeat failed".to_string()),
            ));
        }

        debug!("Left share group '{}' successfully", self.config.group_id);

        self.invalidate_coordinator().await;
        Ok(())
    }
}

/// Builder for creating share consumers.
#[derive(Default)]
#[must_use = "builders do nothing until .build() is called"]
pub struct ShareConsumerBuilder {
    config: ShareConsumerConfig,
}

impl ShareConsumerBuilder {
    /// Set the bootstrap servers.
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    /// Set the share group ID (required).
    pub fn group_id(mut self, group_id: impl Into<String>) -> Self {
        self.config.group_id = group_id.into();
        self
    }

    /// Set the client ID.
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.config.client_id = id.into();
        self
    }

    /// Set the acknowledgement mode.
    pub fn acknowledgement_mode(mut self, mode: AcknowledgementMode) -> Self {
        self.config.acknowledgement_mode = mode;
        self
    }

    /// Set maximum number of records per poll.
    pub fn max_poll_records(mut self, max: i32) -> Self {
        self.config.max_poll_records = max;
        self
    }

    /// Set maximum records buffered internally by [`recv()`](ShareConsumer::recv).
    ///
    /// This is a soft threshold: once the buffer is at/above this value,
    /// `poll()` skips fetches until it drains. A single `recv()` call may
    /// buffer beyond the threshold due to batched fetch responses. Set to `0`
    /// for unlimited. Negative values are rejected at build time.
    /// Defaults to 500.
    pub fn max_buffered_records(mut self, max: i32) -> Self {
        self.config.max_buffered_records = max;
        self
    }

    /// Set the fetch max wait time in milliseconds.
    pub fn fetch_max_wait_ms(mut self, ms: i32) -> Self {
        self.config.fetch_max_wait_ms = ms;
        self
    }

    /// Set the request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set the session timeout for share group membership.
    pub fn session_timeout(mut self, timeout: Duration) -> Self {
        self.config.session_timeout = timeout;
        self
    }

    /// Set the heartbeat interval.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    /// Set authentication configuration.
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = Some(auth);
        self
    }

    /// Set the client rack ID.
    pub fn client_rack(mut self, rack: impl Into<String>) -> Self {
        self.config.client_rack = Some(rack.into());
        self
    }

    /// Set metadata max age.
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

    /// Set SOCKS5 proxy configuration.
    #[cfg(feature = "socks5")]
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.config.proxy = Some(proxy);
        self
    }

    /// Set the maximum decompressed size for record batches.
    ///
    /// Compressed payloads that decompress beyond this limit are rejected as
    /// potential compression bombs. Defaults to
    /// [`RecordBatch::MAX_DECOMPRESSED_SIZE`](crate::protocol::RecordBatch::MAX_DECOMPRESSED_SIZE) (128 MiB).
    pub fn max_decompressed_size(mut self, size: usize) -> Self {
        self.config.max_decompressed_size = size;
        self
    }

    /// Build the share consumer.
    pub async fn build(self) -> Result<ShareConsumer> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap.servers is required"));
        }
        if self.config.group_id.is_empty() {
            return Err(KrafkaError::config(
                "group_id is required for share consumers",
            ));
        }
        if self.config.heartbeat_interval >= self.config.session_timeout {
            return Err(KrafkaError::config(format!(
                "heartbeat_interval ({:?}) must be less than session_timeout ({:?})",
                self.config.heartbeat_interval, self.config.session_timeout,
            )));
        }
        if self.config.max_buffered_records < 0 {
            return Err(KrafkaError::config(format!(
                "max_buffered_records ({}) must be >= 0 (use 0 for unlimited)",
                self.config.max_buffered_records,
            )));
        }
        ShareConsumer::new(self.config).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    fn test_share_consumer(acknowledgement_mode: AcknowledgementMode) -> ShareConsumer {
        let mut config = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("sg")
            .acknowledgement_mode(acknowledgement_mode)
            .config;
        config.bootstrap_servers = "localhost:9092".to_string();
        config.group_id = "sg".to_string();

        let pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
        let metadata = Arc::new(ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool.clone(),
            config.metadata_max_age,
        ));

        ShareConsumer {
            config,
            metadata,
            pool,
            subscriptions: RwLock::new(HashSet::new()),
            assignments: RwLock::new(HashMap::new()),
            member_id: RwLock::new(crate::util::random_uuid_v4()),
            member_epoch: RwLock::new(0),
            heartbeat_interval_ms: RwLock::new(3000),
            closed: AtomicBool::new(false),
            share_sessions: Arc::new(tokio::sync::Mutex::new(ShareSessionCache::new())),
            pending_acks: Arc::new(RwLock::new(Vec::new())),
            ack_state_generation: Arc::new(AtomicU64::new(0)),
            explicit_flush_retry_required: Arc::new(AtomicBool::new(false)),
            topic_ids: RwLock::new(HashMap::new()),
            recv_buffer: RwLock::new(VecDeque::new()),
            coordinator_id: RwLock::new(None),
            coordinator_address: RwLock::new(None),
            unacked_offsets: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    #[test]
    fn test_share_consumer_builder_config() {
        let builder = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("my-share-group")
            .client_id("test-client")
            .acknowledgement_mode(AcknowledgementMode::Explicit)
            .max_poll_records(100)
            .session_timeout(Duration::from_secs(30))
            .heartbeat_interval(Duration::from_secs(5));

        assert_eq!(builder.config.bootstrap_servers, "localhost:9092");
        assert_eq!(builder.config.group_id, "my-share-group");
        assert_eq!(builder.config.client_id, "test-client");
        assert_eq!(
            builder.config.acknowledgement_mode,
            AcknowledgementMode::Explicit
        );
        assert_eq!(builder.config.max_poll_records, 100);
        assert_eq!(builder.config.session_timeout, Duration::from_secs(30));
        assert_eq!(builder.config.heartbeat_interval, Duration::from_secs(5));
    }

    #[test]
    fn test_share_consumer_builder_defaults() {
        let builder = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("sg");

        assert_eq!(builder.config.client_id, "krafka");
        assert_eq!(
            builder.config.acknowledgement_mode,
            AcknowledgementMode::Implicit
        );
        assert_eq!(builder.config.max_poll_records, 500);
        assert!(builder.config.auth.is_none());
    }

    #[tokio::test]
    async fn test_share_consumer_builder_validates_bootstrap() {
        let result = ShareConsumer::builder().group_id("sg").build().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("bootstrap.servers"), "got: {err}");
    }

    #[tokio::test]
    async fn test_share_consumer_builder_validates_group_id() {
        let result = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .build()
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("group_id"), "got: {err}");
    }

    #[tokio::test]
    async fn test_share_consumer_builder_validates_heartbeat() {
        let result = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("sg")
            .session_timeout(Duration::from_secs(5))
            .heartbeat_interval(Duration::from_secs(10))
            .build()
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("heartbeat_interval"), "got: {err}");
    }

    #[tokio::test]
    async fn test_share_consumer_builder_rejects_negative_max_buffered_records() {
        let result = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("sg")
            .max_buffered_records(-1)
            .build()
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("max_buffered_records"), "got: {err}");
    }

    #[test]
    fn test_acknowledge_type_to_i8() {
        assert_eq!(AcknowledgeType::Accept.to_i8(), 1);
        assert_eq!(AcknowledgeType::Release.to_i8(), 2);
        assert_eq!(AcknowledgeType::Reject.to_i8(), 3);
    }

    #[test]
    fn test_acknowledgement_mode_default() {
        assert_eq!(
            AcknowledgementMode::default(),
            AcknowledgementMode::Implicit
        );
    }

    #[test]
    fn test_share_consumer_config_accessors() {
        let builder = ShareConsumer::builder()
            .bootstrap_servers("broker:9092")
            .group_id("sg-1")
            .client_id("my-client")
            .acknowledgement_mode(AcknowledgementMode::Explicit)
            .session_timeout(Duration::from_secs(20))
            .heartbeat_interval(Duration::from_secs(3));

        assert_eq!(builder.config.bootstrap_servers(), "broker:9092");
        assert_eq!(builder.config.group_id(), "sg-1");
        assert_eq!(builder.config.client_id(), "my-client");
        assert_eq!(
            builder.config.acknowledgement_mode(),
            AcknowledgementMode::Explicit
        );
        assert_eq!(builder.config.session_timeout(), Duration::from_secs(20));
        assert_eq!(builder.config.heartbeat_interval(), Duration::from_secs(3));
    }

    #[test]
    fn test_build_acknowledge_topics() {
        let acks = vec![
            PendingAck {
                topic: "t1".into(),
                topic_id: [1; 16],
                partition: 0,
                first_offset: 0,
                last_offset: 5,
                ack_type: AcknowledgeType::Accept.to_i8(),
            },
            PendingAck {
                topic: "t1".into(),
                topic_id: [1; 16],
                partition: 1,
                first_offset: 10,
                last_offset: 15,
                ack_type: AcknowledgeType::Release.to_i8(),
            },
            PendingAck {
                topic: "t2".into(),
                topic_id: [2; 16],
                partition: 0,
                first_offset: 0,
                last_offset: 3,
                ack_type: AcknowledgeType::Reject.to_i8(),
            },
        ];

        let topics = ShareConsumer::build_acknowledge_topics(&acks);
        assert_eq!(topics.len(), 2); // Two distinct topic_ids

        // Verify partition counts.
        let total_partitions: usize = topics.iter().map(|t| t.partitions.len()).sum();
        assert_eq!(total_partitions, 3);
    }

    #[test]
    fn test_share_acknowledge_response_error_detects_partition_failure() {
        let response = crate::protocol::ShareAcknowledgeResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            error_message: None,
            acquisition_lock_timeout_ms: -1,
            responses: vec![crate::protocol::ShareAcknowledgeTopicResponse {
                topic_id: [1; 16],
                partitions: vec![crate::protocol::ShareAcknowledgePartitionResponse {
                    partition_index: 7,
                    error_code: ErrorCode::UnknownTopicOrPartition,
                    error_message: Some("gone".to_string()),
                    current_leader: crate::protocol::ShareLeaderIdAndEpoch {
                        leader_id: -1,
                        leader_epoch: 0,
                    },
                }],
            }],
            node_endpoints: Vec::new(),
        };

        let error = ShareConsumer::share_acknowledge_response_error(&response)
            .expect("partition error must surface as an error");

        assert!(matches!(
            error,
            KrafkaError::Broker {
                code: ErrorCode::UnknownTopicOrPartition,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_restore_ack_state_requeues_pending_acks_without_reinserting_unacked() {
        let ack_state_generation = AtomicU64::new(0);
        let explicit_flush_retry_required = AtomicBool::new(false);
        let pending = RwLock::new(Vec::new());
        let mut acks_to_restore = vec![PendingAck {
            topic: "topic-a".into(),
            topic_id: [0; 16],
            partition: 2,
            first_offset: 11,
            last_offset: 13,
            ack_type: AcknowledgeType::Accept.to_i8(),
        }];

        ShareConsumer::restore_ack_state(
            &ack_state_generation,
            &pending,
            &explicit_flush_retry_required,
            0,
            false,
            &mut acks_to_restore,
        )
        .await;

        assert!(acks_to_restore.is_empty());
        assert!(!explicit_flush_retry_required.load(Ordering::SeqCst));
        let pending = pending.read().await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].topic, "topic-a");
        assert_eq!(pending[0].partition, 2);
        assert_eq!(pending[0].first_offset, 11);
        assert_eq!(pending[0].last_offset, 13);
        assert_eq!(pending[0].ack_type, AcknowledgeType::Accept.to_i8());
    }

    #[tokio::test]
    async fn test_restore_ack_state_skips_stale_generation() {
        let ack_state_generation = AtomicU64::new(1);
        let explicit_flush_retry_required = AtomicBool::new(false);
        let pending = RwLock::new(Vec::new());
        let mut acks_to_restore = vec![PendingAck {
            topic: "topic-a".into(),
            topic_id: [0; 16],
            partition: 2,
            first_offset: 11,
            last_offset: 13,
            ack_type: AcknowledgeType::Accept.to_i8(),
        }];

        ShareConsumer::restore_ack_state(
            &ack_state_generation,
            &pending,
            &explicit_flush_retry_required,
            0,
            true,
            &mut acks_to_restore,
        )
        .await;

        assert!(pending.read().await.is_empty());
        assert!(acks_to_restore.is_empty());
        assert!(!explicit_flush_retry_required.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_restore_ack_state_marks_explicit_flush_retry_required() {
        let ack_state_generation = AtomicU64::new(0);
        let explicit_flush_retry_required = AtomicBool::new(false);
        let pending = RwLock::new(Vec::new());
        let mut acks_to_restore = vec![PendingAck {
            topic: "topic-a".into(),
            topic_id: [0; 16],
            partition: 2,
            first_offset: 11,
            last_offset: 13,
            ack_type: AcknowledgeType::Accept.to_i8(),
        }];

        ShareConsumer::restore_ack_state(
            &ack_state_generation,
            &pending,
            &explicit_flush_retry_required,
            0,
            true,
            &mut acks_to_restore,
        )
        .await;

        assert!(acks_to_restore.is_empty());
        assert!(explicit_flush_retry_required.load(Ordering::SeqCst));
        assert_eq!(pending.read().await.len(), 1);
    }

    #[tokio::test]
    async fn test_poll_rejects_after_failed_explicit_flush() {
        let consumer = test_share_consumer(AcknowledgementMode::Explicit);
        consumer
            .explicit_flush_retry_required
            .store(true, Ordering::SeqCst);

        let error = consumer
            .poll(Duration::from_millis(1))
            .await
            .expect_err("poll must block after a failed explicit flush");

        assert!(
            error
                .to_string()
                .contains("retry the commit before calling poll() again")
        );
    }

    #[tokio::test]
    async fn test_clear_partition_state_clears_explicit_flush_retry_required() {
        let consumer = test_share_consumer(AcknowledgementMode::Explicit);
        consumer
            .explicit_flush_retry_required
            .store(true, Ordering::SeqCst);

        consumer.clear_partition_state().await;

        assert!(
            !consumer
                .explicit_flush_retry_required
                .load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn test_apply_assignment_advances_ack_state_generation_on_change() {
        let consumer = test_share_consumer(AcknowledgementMode::Explicit);
        consumer
            .assignments
            .write()
            .await
            .insert("topic-a".to_string(), vec![0]);
        consumer.pending_acks.write().await.push(PendingAck {
            topic: "topic-a".to_string(),
            topic_id: [1; 16],
            partition: 0,
            first_offset: 5,
            last_offset: 5,
            ack_type: AcknowledgeType::Accept.to_i8(),
        });
        consumer
            .unacked_offsets
            .write()
            .await
            .insert(("topic-a".to_string(), 0, 5));
        consumer
            .explicit_flush_retry_required
            .store(true, Ordering::SeqCst);

        let old_generation = consumer.ack_state_generation.load(Ordering::SeqCst);
        consumer.apply_assignment(&[]).await;

        assert_eq!(
            consumer.ack_state_generation.load(Ordering::SeqCst),
            old_generation + 1
        );
        assert!(
            !consumer
                .explicit_flush_retry_required
                .load(Ordering::SeqCst)
        );
        assert!(consumer.pending_acks.read().await.is_empty());
        assert!(consumer.unacked_offsets.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_send_share_acknowledge_rejects_stale_ack_generation() {
        let consumer = test_share_consumer(AcknowledgementMode::Explicit);
        let error = ShareConsumer::send_share_acknowledge_with_state(
            ShareAcknowledgeContext {
                metadata: consumer.metadata.clone(),
                pool: consumer.pool.clone(),
                share_sessions: consumer.share_sessions.clone(),
                group_id: consumer.config.group_id.clone(),
                member_id: consumer.member_id.read().await.clone(),
                current_ack_state_generation: Arc::new(AtomicU64::new(1)),
                ack_state_generation: 0,
            },
            &[PendingAck {
                topic: "topic-a".to_string(),
                topic_id: [1; 16],
                partition: 0,
                first_offset: 5,
                last_offset: 5,
                ack_type: AcknowledgeType::Accept.to_i8(),
            }],
        )
        .await
        .expect_err("stale ack generation must be rejected before sending");

        assert!(
            error
                .to_string()
                .contains("acknowledgement state was invalidated")
        );
    }

    #[tokio::test]
    async fn test_share_commit_handle_ready_flattens_result() {
        ShareCommitHandle::ready(Ok(()))
            .await
            .expect("ready ok result");

        let error = ShareCommitHandle::ready(Err(KrafkaError::invalid_state("boom")))
            .await
            .expect_err("ready error must surface");
        assert!(error.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn test_share_commit_handle_flattens_task_result() {
        let error = ShareCommitHandle::Task(tokio::spawn(async {
            Err(KrafkaError::invalid_state("task failed"))
        }))
        .await
        .expect_err("task error must surface");

        assert!(error.to_string().contains("task failed"));
    }

    #[tokio::test]
    async fn test_describe_share_fetch_join_error_reports_panic() {
        let error = tokio::spawn(async {
            panic!("boom");
        })
        .await
        .expect_err("panic must surface as a JoinError");

        assert_eq!(describe_share_fetch_join_error(&error), "panicked");
    }

    #[tokio::test]
    async fn test_describe_share_fetch_join_error_reports_cancellation() {
        let handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        handle.abort();

        let error = handle
            .await
            .expect_err("aborted task must surface as a JoinError");

        assert_eq!(describe_share_fetch_join_error(&error), "was cancelled");
    }

    #[tokio::test]
    async fn test_acknowledge_keeps_record_pending_until_ack_is_queued() {
        let consumer = Arc::new(test_share_consumer(AcknowledgementMode::Explicit));

        consumer
            .topic_ids
            .write()
            .await
            .insert("topic-a".to_string(), [7; 16]);

        let record = ConsumerRecord {
            topic: "topic-a".into(),
            partition: 3,
            offset: 11,
            timestamp: 0,
            timestamp_type: 0,
            key: None,
            value: None,
            headers: Vec::new(),
            leader_epoch: None,
            delivery_count: None,
        };
        let record_key = (record.topic.clone(), record.partition, record.offset);

        consumer
            .unacked_offsets
            .write()
            .await
            .insert(record_key.clone());

        let pending_guard = consumer.pending_acks.write().await;
        let task_consumer = consumer.clone();
        let task = tokio::spawn(async move {
            task_consumer
                .acknowledge(&record, AcknowledgeType::Accept)
                .await
        });

        tokio::task::yield_now().await;

        assert!(consumer.unacked_offsets.read().await.contains(&record_key));
        assert!(
            !task.is_finished(),
            "acknowledge should still be waiting on the pending_acks lock"
        );

        drop(pending_guard);

        task.await
            .expect("acknowledge task should join")
            .expect("acknowledge should succeed once pending lock is released");

        assert!(!consumer.unacked_offsets.read().await.contains(&record_key));
        let pending = consumer.pending_acks.read().await;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].topic, "topic-a");
        assert_eq!(pending[0].partition, 3);
        assert_eq!(pending[0].first_offset, 11);
        assert_eq!(pending[0].last_offset, 11);
        assert_eq!(pending[0].ack_type, AcknowledgeType::Accept.to_i8());
    }

    #[test]
    fn test_coalesce_implicit_acks_merges_consecutive() {
        let records = vec![
            ConsumerRecord {
                topic: "t1".into(),
                partition: 0,
                offset: 0,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: None,
                headers: Vec::new(),
                leader_epoch: None,
                delivery_count: None,
            },
            ConsumerRecord {
                topic: "t1".into(),
                partition: 0,
                offset: 1,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: None,
                headers: Vec::new(),
                leader_epoch: None,
                delivery_count: None,
            },
            ConsumerRecord {
                topic: "t1".into(),
                partition: 0,
                offset: 2,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: None,
                headers: Vec::new(),
                leader_epoch: None,
                delivery_count: None,
            },
            // Gap: offset 3 missing
            ConsumerRecord {
                topic: "t1".into(),
                partition: 0,
                offset: 4,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: None,
                headers: Vec::new(),
                leader_epoch: None,
                delivery_count: None,
            },
        ];

        let mut topic_ids = HashMap::new();
        topic_ids.insert("t1".to_string(), [1u8; 16]);

        let mut pending = Vec::new();
        ShareConsumer::coalesce_implicit_acks(&records, &topic_ids, &mut pending);

        // Should produce two ranges: [0,2] and [4,4].
        assert_eq!(pending.len(), 2);
        pending.sort_by_key(|a| a.first_offset);
        assert_eq!(pending[0].first_offset, 0);
        assert_eq!(pending[0].last_offset, 2);
        assert_eq!(pending[1].first_offset, 4);
        assert_eq!(pending[1].last_offset, 4);
    }

    #[test]
    fn test_config_defaults_match_kip932() {
        let builder = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("sg");

        // KIP-932 specifies 45s session timeout.
        assert_eq!(builder.config.session_timeout(), Duration::from_secs(45));
        assert_eq!(builder.config.heartbeat_interval(), Duration::from_secs(5));
    }
}
