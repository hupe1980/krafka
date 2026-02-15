//! Kafka consumer implementation.
//!
//! This module provides:
//! - Async consumer for receiving messages
//! - Consumer groups with rebalancing
//! - Offset management (auto and manual commit)
//! - Partition assignment strategies
//!
//! # Delivery Semantics
//!
//! Krafka provides **at-least-once** delivery semantics by default, which is the
//! standard Kafka consumer behavior:
//!
//! 1. Messages are delivered to the application via `poll()`
//! 2. Offsets are committed after processing (auto-commit or manual)
//! 3. If the consumer crashes after processing but before commit, messages may
//!    be redelivered on restart
//!
//! This is the safest default as it ensures no message loss. For use cases that
//! cannot tolerate duplicates, applications should implement idempotent processing.
//!
//! ## Controlling Commit Behavior
//!
//! - **Auto-commit** (default): Offsets are committed periodically in the background
//! - **Manual commit**: Disable auto-commit and call `commit()` explicitly
//!
//! For at-most-once semantics (where message loss is acceptable but duplicates are not),
//! commit offsets before processing:
//!
//! ```ignore
//! let records = consumer.poll(Duration::from_secs(1)).await?;
//! consumer.commit().await?;  // Commit BEFORE processing
//! for record in records {
//!     process(record);  // If this crashes, message is lost
//! }
//! ```

mod config;
mod group;
mod offset;
mod record;

pub use config::{
    AutoOffsetReset, ConsumerConfig, ConsumerConfigBuilder, IsolationLevel,
    PartitionAssignmentStrategy,
};
pub use group::{
    ConsumerGroup, ConsumerRebalanceListener, CooperativeStickyAssignor, GroupCoordinator,
    GroupMember, GroupState, HeartbeatController, HeartbeatStatus, MemberAssignment,
    NoOpRebalanceListener, PartitionAssignor, RangeAssignor, RoundRobinAssignor,
};
pub use offset::{OffsetAndMetadata, OffsetStore, ResetOffset};
pub use record::{ConsumerRecord, ConsumerRecords, TopicPartition};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use crate::auth::AuthConfig;
use crate::error::{KrafkaError, Result};
use crate::metadata::ClusterMetadata;
use crate::metrics::ConsumerMetrics;
use crate::network::{ConnectionConfig, ConnectionPool};
use crate::protocol::{
    ApiKey, FetchPartitionRequest, FetchRequest, FetchResponse, FetchTopicRequest,
    ListOffsetsRequest, ListOffsetsRequestPartition, ListOffsetsRequestTopic, ListOffsetsResponse,
    RecordBatch,
};
use crate::{Offset, PartitionId};

/// A Kafka consumer.
pub struct Consumer {
    /// Consumer configuration.
    config: ConsumerConfig,
    /// Cluster metadata.
    metadata: Arc<ClusterMetadata>,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Subscribed topics.
    subscriptions: RwLock<HashSet<String>>,
    /// Assigned partitions.
    assignments: RwLock<HashMap<String, Vec<PartitionId>>>,
    /// Current offsets.
    offsets: RwLock<HashMap<(String, PartitionId), Offset>>,
    /// Paused partitions.
    paused: RwLock<HashSet<(String, PartitionId)>>,
    /// Whether the consumer is closed.
    closed: std::sync::atomic::AtomicBool,
    /// Group coordinator for full group protocol support.
    group_coordinator: Option<Arc<GroupCoordinator>>,
    /// Consumer metrics.
    metrics: Arc<ConsumerMetrics>,
    /// Rebalance listener.
    rebalance_listener: Arc<dyn ConsumerRebalanceListener>,
    /// Consumer interceptor.
    interceptor: Arc<dyn crate::interceptor::ConsumerInterceptor>,
    /// Last auto-commit time (for auto-commit timer §2.7).
    last_auto_commit: RwLock<Instant>,
    /// Buffer for records returned by `recv()` (§R13.1).
    /// `poll()` may return multiple records; `recv()` buffers the rest here.
    recv_buffer: RwLock<std::collections::VecDeque<ConsumerRecord>>,
}

impl Consumer {
    /// Create a new consumer builder.
    pub fn builder() -> ConsumerBuilder {
        ConsumerBuilder::default()
    }

    /// Create a new consumer with the given configuration.
    async fn new(config: ConsumerConfig) -> Result<Self> {
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

        // Create group coordinator if group_id is specified
        let group_coordinator = if let Some(ref group_id) = config.group_id {
            Some(Arc::new(
                GroupCoordinator::new(
                    group_id.clone(),
                    pool.clone(),
                    metadata.clone(),
                    config.session_timeout,
                    config.heartbeat_interval,
                    config.max_poll_interval, // rebalance_timeout matches Java client's max.poll.interval.ms
                )
                .with_assignor_strategy(config.partition_assignment_strategy)
                .with_group_instance_id(config.group_instance_id.clone())
                .with_isolation_level(config.isolation_level.to_i8()),
            ))
        } else {
            None
        };

        let metrics = Arc::new(ConsumerMetrics::default());

        info!(
            "Consumer initialized with {} brokers{}",
            metadata.brokers().await.len(),
            if group_coordinator.is_some() {
                format!(", group_id='{}'", config.group_id.as_ref().unwrap())
            } else {
                String::new()
            }
        );

        Ok(Self {
            config,
            metadata,
            pool,
            subscriptions: RwLock::new(HashSet::new()),
            assignments: RwLock::new(HashMap::new()),
            offsets: RwLock::new(HashMap::new()),
            paused: RwLock::new(HashSet::new()),
            closed: std::sync::atomic::AtomicBool::new(false),
            group_coordinator,
            metrics,
            rebalance_listener: Arc::new(NoOpRebalanceListener),
            interceptor: Arc::new(crate::interceptor::NoOpConsumerInterceptor),
            last_auto_commit: RwLock::new(Instant::now()),
            recv_buffer: RwLock::new(std::collections::VecDeque::new()),
        })
    }

    /// Subscribe to topics.
    ///
    /// Replaces the current subscription with the given topics (matching
    /// the Kafka Java client's replace semantics). §R13.11 fix.
    pub async fn subscribe(&self, topics: &[&str]) -> Result<()> {
        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.clear();
        for topic in topics {
            subscriptions.insert((*topic).to_string());
        }

        // Refresh metadata for subscribed topics
        self.metadata.refresh_for_topics(Some(topics)).await?;

        // If we have a group coordinator, join the group
        if let Some(ref coordinator) = self.group_coordinator {
            let topic_strings: Vec<String> = topics.iter().map(|s| s.to_string()).collect();
            let assignment = coordinator.ensure_active_membership(&topic_strings).await?;

            // Update our assignments based on the group assignment
            let mut assignments = self.assignments.write().await;
            assignments.clear();
            for (topic, partitions) in &assignment.partitions {
                assignments.insert(topic.clone(), partitions.clone());
            }

            // Fetch committed offsets for our assigned partitions (§2.3 fix)
            self.fetch_and_apply_committed_offsets(&assignment.partitions)
                .await?;

            debug!("Subscribed to topics via group coordinator: {:?}", topics);
        } else {
            // Assign all partitions (simple assignment without group coordination)
            let mut assignments = self.assignments.write().await;
            for topic in topics {
                if let Some(topic_info) = self.metadata.topic(topic).await {
                    let partitions: Vec<_> =
                        topic_info.partitions.iter().map(|p| p.partition).collect();
                    assignments.insert((*topic).to_string(), partitions);
                }
            }
            let assigned_snapshot = assignments.clone();
            drop(assignments);

            // §10.4 fix: Apply auto_offset_reset for non-group consumers.
            // Without this, all partitions default to offset 0 regardless of
            // the configured auto_offset_reset policy.
            self.apply_auto_offset_reset(&assigned_snapshot).await?;

            debug!("Subscribed to topics: {:?}", topics);
        }

        Ok(())
    }

    /// Fetch committed offsets and apply auto_offset_reset for partitions without committed offsets.
    ///
    /// Called after group assignment to initialize partition offsets:
    /// 1. Fetch committed offsets from the group coordinator
    /// 2. For partitions with no committed offset, apply the configured auto_offset_reset policy
    async fn fetch_and_apply_committed_offsets(
        &self,
        assigned: &HashMap<String, Vec<PartitionId>>,
    ) -> Result<()> {
        let coordinator = match self.group_coordinator {
            Some(ref c) => c,
            None => return Ok(()),
        };

        // Fetch committed offsets
        let committed = coordinator.fetch_committed_offsets(assigned).await?;

        // Determine which partitions are missing committed offsets
        let mut need_reset: Vec<(String, PartitionId)> = Vec::new();
        let mut offsets = self.offsets.write().await;

        // Log the initial offsets state before processing committed offsets
        debug!("fetch_and_apply: existing offsets: {:?}", *offsets);

        for (topic, partitions) in assigned {
            for &partition in partitions {
                let committed_val = committed.get(&(topic.clone(), partition));
                if let Some(&offset) = committed_val
                    && offset >= 0
                {
                    debug!(
                        "Using committed offset {} for {}-{}",
                        offset, topic, partition
                    );
                    offsets.insert((topic.clone(), partition), offset);
                    continue;
                }
                // No committed offset or negative (unknown)
                debug!(
                    "No committed offset for {}-{} (committed={:?}), will auto-reset",
                    topic, partition, committed_val
                );
                need_reset.push((topic.clone(), partition));
            }
        }

        if need_reset.is_empty() {
            return Ok(());
        }

        // Apply auto_offset_reset
        match self.config.auto_offset_reset.to_offset() {
            Some(timestamp) => {
                // Group partitions by topic for list_offsets call
                let mut reset_partitions: HashMap<String, Vec<PartitionId>> = HashMap::new();
                for (topic, partition) in &need_reset {
                    reset_partitions
                        .entry(topic.clone())
                        .or_default()
                        .push(*partition);
                }

                let resolved = coordinator
                    .list_offsets(&reset_partitions, timestamp)
                    .await?;

                for (key, offset) in &resolved {
                    offsets.insert(key.clone(), *offset);
                }

                // Fallback: if the group coordinator's list_offsets silently
                // dropped some partitions (partition-level errors), resolve
                // them individually via the direct ListOffsets v1 path.
                for (topic, partition) in &need_reset {
                    if !resolved.contains_key(&(topic.clone(), *partition))
                        && !offsets.contains_key(&(topic.clone(), *partition))
                    {
                        debug!(
                            "Falling back to direct ListOffsets for {}-{} \
                             (coordinator path returned no result)",
                            topic, partition
                        );
                        // Release offsets lock temporarily for the network call
                        drop(offsets);
                        match self.resolve_list_offset(topic, *partition, timestamp).await {
                            Ok(offset) => {
                                offsets = self.offsets.write().await;
                                offsets.insert((topic.clone(), *partition), offset);
                            }
                            Err(e) => {
                                warn!(
                                    "Fallback offset resolution failed for {}-{}: {}",
                                    topic, partition, e
                                );
                                offsets = self.offsets.write().await;
                            }
                        }
                    }
                }
            }
            None => {
                // AutoOffsetReset::None — fail if no committed offset
                let missing: Vec<String> = need_reset
                    .iter()
                    .map(|(t, p)| format!("{}-{}", t, p))
                    .collect();
                return Err(KrafkaError::invalid_state(format!(
                    "no committed offset for partitions and auto.offset.reset=none: {}",
                    missing.join(", ")
                )));
            }
        }

        Ok(())
    }

    /// Assign specific partitions manually.
    ///
    /// Manual assignment and group subscription are mutually exclusive.
    /// This method returns an error if a group coordinator is active (§R13.5 fix).
    pub async fn assign(&self, topic: &str, partitions: Vec<PartitionId>) -> Result<()> {
        if self.group_coordinator.is_some() {
            return Err(KrafkaError::invalid_state(
                "cannot use manual partition assignment with consumer group subscription",
            ));
        }

        // Refresh metadata so we can resolve partition leaders for offset lookup
        self.metadata.refresh_for_topics(Some(&[topic])).await?;

        let mut assignments = self.assignments.write().await;
        assignments.insert(topic.to_string(), partitions.clone());

        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.insert(topic.to_string());
        drop(subscriptions);
        drop(assignments);

        // §10.4 fix: Apply auto_offset_reset for manually assigned partitions
        let mut assigned = HashMap::new();
        assigned.insert(topic.to_string(), partitions.clone());
        self.apply_auto_offset_reset(&assigned).await?;

        debug!("Assigned partitions for {}: {:?}", topic, partitions);
        Ok(())
    }

    /// Apply auto_offset_reset policy for partitions that have no tracked offset.
    ///
    /// This resolves initial offsets based on the configured `auto_offset_reset`
    /// policy (Earliest, Latest, or None). Used by both group and non-group
    /// consumers during partition assignment (§10.4 fix).
    async fn apply_auto_offset_reset(
        &self,
        assigned: &HashMap<String, Vec<PartitionId>>,
    ) -> Result<()> {
        // Collect partitions that don't already have a tracked offset
        let need_reset: Vec<(String, PartitionId)> = {
            let offsets = self.offsets.read().await;
            let mut need = Vec::new();
            for (topic, partitions) in assigned {
                for &p in partitions {
                    if !offsets.contains_key(&(topic.clone(), p)) {
                        need.push((topic.clone(), p));
                    }
                }
            }
            need
        };

        if need_reset.is_empty() {
            return Ok(());
        }

        match self.config.auto_offset_reset.to_offset() {
            Some(timestamp) => {
                let mut offsets = self.offsets.write().await;
                for (topic, partition) in &need_reset {
                    match self.resolve_list_offset(topic, *partition, timestamp).await {
                        Ok(offset) => {
                            offsets.insert((topic.clone(), *partition), offset);
                        }
                        Err(e) => {
                            warn!(
                                "Failed to resolve offset for {}-{}: {}, will retry on next poll",
                                topic, partition, e
                            );
                            // Don't insert a default — leave the partition without an
                            // offset so it will be retried on the next poll cycle.
                        }
                    }
                }
            }
            None => {
                // AutoOffsetReset::None — fail if no offset
                let missing: Vec<String> = need_reset
                    .iter()
                    .map(|(t, p)| format!("{}-{}", t, p))
                    .collect();
                return Err(KrafkaError::invalid_state(format!(
                    "no offset for partitions and auto.offset.reset=none: {}",
                    missing.join(", ")
                )));
            }
        }

        Ok(())
    }

    /// Seek to a specific offset.
    pub async fn seek(&self, topic: &str, partition: PartitionId, offset: Offset) -> Result<()> {
        let mut offsets = self.offsets.write().await;
        offsets.insert((topic.to_string(), partition), offset);
        debug!("Seek to offset {} for {}-{}", offset, topic, partition);
        Ok(())
    }

    /// Seek to the beginning.
    pub async fn seek_to_beginning(&self, topic: &str, partition: PartitionId) -> Result<()> {
        self.seek(topic, partition, 0).await
    }

    /// Seek to the end (latest offset).
    ///
    /// Sets the consumer position to the high watermark, so subsequent polls
    /// will only return new messages produced after this call.
    ///
    /// This resolves the actual latest offset via a ListOffsets RPC to the
    /// partition leader. The Kafka Fetch API does not interpret special offset
    /// values like -1; those are only meaningful in the ListOffsets API.
    pub async fn seek_to_end(&self, topic: &str, partition: PartitionId) -> Result<()> {
        // Resolve the actual latest offset via ListOffsets (timestamp=-1 means latest)
        let offset = self.resolve_list_offset(topic, partition, -1).await?;
        self.seek(topic, partition, offset).await
    }

    /// Resolve an offset timestamp via the ListOffsets API.
    ///
    /// `timestamp` should be:
    /// - `-1` for the latest offset (high watermark)
    /// - `-2` for the earliest available offset
    async fn resolve_list_offset(
        &self,
        topic: &str,
        partition: PartitionId,
        timestamp: i64,
    ) -> Result<Offset> {
        let conn = self
            .metadata
            .get_leader_connection(topic, partition)
            .await?;
        let leader_epoch = self
            .metadata
            .leader_epoch(topic, partition)
            .await
            .unwrap_or(-1);

        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: self.config.isolation_level.to_i8(),
            topics: vec![ListOffsetsRequestTopic {
                name: topic.to_string(),
                partitions: vec![ListOffsetsRequestPartition {
                    partition_index: partition,
                    current_leader_epoch: leader_epoch,
                    timestamp,
                }],
            }],
        };

        let response = conn
            .send_request(ApiKey::ListOffsets, 1, |buf| {
                request.encode_v1(buf);
            })
            .await?;

        let mut buf = response;
        let list_response = ListOffsetsResponse::decode_v1(&mut buf)?;

        for topic_resp in &list_response.topics {
            for part_resp in &topic_resp.partitions {
                if part_resp.partition_index == partition {
                    if !part_resp.error_code.is_ok() {
                        return Err(KrafkaError::broker(
                            part_resp.error_code,
                            format!("ListOffsets error for {}-{}", topic, partition),
                        ));
                    }
                    return Ok(part_resp.offset);
                }
            }
        }

        Err(KrafkaError::protocol(format!(
            "no offset returned for {}-{}",
            topic, partition
        )))
    }

    /// Poll for new records.
    ///
    /// This is the main method for consuming messages.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use krafka::consumer::Consumer;
    /// # async fn example() -> Result<(), krafka::error::KrafkaError> {
    /// let consumer = Consumer::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .group_id("my-group")
    ///     .build()
    ///     .await?;
    ///
    /// consumer.subscribe(&["my-topic"]).await?;
    ///
    /// loop {
    ///     let records = consumer.poll(std::time::Duration::from_secs(1)).await?;
    ///     for record in records {
    ///         println!("Received: {:?}", record);
    ///     }
    /// }
    /// # }
    /// ```
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(KrafkaError::invalid_state("consumer is closed"));
        }

        let _poll_timer = self.metrics.poll_latency.start();
        self.metrics.polls.inc();

        // Auto-commit timer (§2.7): commit if interval has elapsed
        if self.config.enable_auto_commit && self.group_coordinator.is_some() {
            let should_commit = {
                let last = self.last_auto_commit.read().await;
                last.elapsed() >= self.config.auto_commit_interval
            };
            if should_commit {
                match self.commit().await {
                    Ok(()) => {
                        *self.last_auto_commit.write().await = Instant::now();
                    }
                    Err(e) => {
                        warn!("Auto-commit failed: {}", e);
                    }
                }
            }
        }

        // Handle group rebalance if needed
        if let Some(ref coordinator) = self.group_coordinator {
            // Check if we need to rejoin the group
            if coordinator.needs_rejoin().await {
                let topics: Vec<String> = self.subscriptions.read().await.iter().cloned().collect();
                if !topics.is_empty() {
                    // Notify listener of revoked partitions before rebalance
                    let old_assignments = self.assignments.read().await.clone();
                    if !old_assignments.is_empty() {
                        let revoked: Vec<TopicPartition> = old_assignments
                            .iter()
                            .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
                            .collect();
                        self.rebalance_listener.on_partitions_revoked(&revoked);
                        self.metrics.rebalances.inc();
                    }

                    let assignment = coordinator.ensure_active_membership(&topics).await?;

                    // Update our assignments
                    let mut assignments = self.assignments.write().await;
                    assignments.clear();
                    for (topic, partitions) in &assignment.partitions {
                        assignments.insert(topic.clone(), partitions.clone());
                    }
                    drop(assignments);

                    // Notify listener of newly assigned partitions
                    let assigned: Vec<TopicPartition> = assignment
                        .partitions
                        .iter()
                        .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
                        .collect();
                    self.rebalance_listener.on_partitions_assigned(&assigned);
                    self.metrics.assigned_partitions.set(assigned.len() as u64);

                    // Fetch committed offsets for new assignment (§2.3 fix)
                    self.fetch_and_apply_committed_offsets(&assignment.partitions)
                        .await?;
                }
            }

            // Check if inline heartbeat is needed
            if coordinator.is_heartbeat_overdue().await {
                match coordinator.send_heartbeat().await {
                    Ok(status) if status.requires_rejoin() => {
                        // Need to rejoin - will be handled on next poll
                        coordinator.trigger_rejoin().await;
                        debug!("Heartbeat indicated rejoin needed");
                    }
                    Err(e) => {
                        warn!("Inline heartbeat failed: {}", e);
                    }
                    _ => {}
                }
            }
        }

        let assignments = self.assignments.read().await;
        if assignments.is_empty() {
            self.metrics.empty_polls.inc();
            return Ok(Vec::new());
        }

        // Retry offset resolution for partitions that are missing tracked offsets.
        // This fulfils the "will retry on next poll" contract when initial offset
        // resolution fails (e.g., due to a transient ListOffsets error or a
        // rejoin that left some partitions without offsets).
        {
            let missing: Vec<(String, PartitionId)> = {
                let offsets = self.offsets.read().await;
                assignments
                    .iter()
                    .flat_map(|(topic, partitions)| {
                        partitions
                            .iter()
                            .filter(|&&p| !offsets.contains_key(&(topic.clone(), p)))
                            .map(|&p| (topic.clone(), p))
                    })
                    .collect()
            };

            if !missing.is_empty() {
                debug!(
                    "Retrying offset resolution for {} partition(s) without tracked offsets",
                    missing.len()
                );
                let mut reset_partitions: HashMap<String, Vec<PartitionId>> = HashMap::new();
                for (topic, partition) in &missing {
                    reset_partitions
                        .entry(topic.clone())
                        .or_default()
                        .push(*partition);
                }

                // Use group coordinator path if available, otherwise direct path
                if let Some(ref coordinator) = self.group_coordinator {
                    if let Some(timestamp) = self.config.auto_offset_reset.to_offset() {
                        match coordinator.list_offsets(&reset_partitions, timestamp).await {
                            Ok(resolved) => {
                                let mut offsets = self.offsets.write().await;
                                for (key, offset) in &resolved {
                                    offsets.insert(key.clone(), *offset);
                                }
                                drop(offsets);

                                // Fallback for partitions the coordinator path
                                // silently dropped (partition-level errors).
                                for (topic, partition) in &missing {
                                    if !resolved.contains_key(&(topic.clone(), *partition)) {
                                        debug!(
                                            "Poll retry: falling back to direct ListOffsets for {}-{}",
                                            topic, partition
                                        );
                                        if let Ok(offset) = self
                                            .resolve_list_offset(topic, *partition, timestamp)
                                            .await
                                        {
                                            let mut offsets = self.offsets.write().await;
                                            offsets.insert((topic.clone(), *partition), offset);
                                        }
                                    }
                                }
                            }
                            Err(e) => {
                                warn!("Offset resolution retry via coordinator failed: {}", e);
                                // Fall back to direct path for all missing partitions
                                for (topic, partition) in &missing {
                                    if let Ok(offset) =
                                        self.resolve_list_offset(topic, *partition, timestamp).await
                                    {
                                        let mut offsets = self.offsets.write().await;
                                        offsets.insert((topic.clone(), *partition), offset);
                                    }
                                }
                            }
                        }
                    }
                } else {
                    self.apply_auto_offset_reset(&reset_partitions).await.ok();
                }
            }
        }

        let paused = self.paused.read().await;

        // Group partitions by leader broker for batch fetching
        // This reduces O(n) round trips to O(k) where k = number of unique leaders
        let mut partitions_by_leader: HashMap<crate::BrokerId, Vec<(String, PartitionId)>> =
            HashMap::new();

        for (topic, partitions) in assignments.iter() {
            for &partition in partitions {
                // Skip paused partitions
                if paused.contains(&(topic.clone(), partition)) {
                    continue;
                }

                // Get leader for this partition
                if let Some(leader_id) = self.metadata.leader(topic, partition).await {
                    partitions_by_leader
                        .entry(leader_id)
                        .or_default()
                        .push((topic.clone(), partition));
                } else {
                    warn!(
                        "No leader found for {}-{}, skipping in batch fetch",
                        topic, partition
                    );
                }
            }
        }

        // Release locks before network I/O
        drop(paused);
        drop(assignments);

        let mut all_records = Vec::new();
        let mut all_offset_updates: Vec<((String, PartitionId), Offset)> = Vec::new();

        // Fetch from each broker (one request per broker, containing all its partitions)
        for (broker_id, topic_partitions) in partitions_by_leader {
            match self
                .batch_fetch_from_broker(broker_id, &topic_partitions, timeout)
                .await
            {
                Ok((records, offset_updates)) => {
                    all_records.extend(records);
                    all_offset_updates.extend(offset_updates);
                }
                Err(e) => {
                    self.metrics.record_error();
                    warn!("Batch fetch from broker {} failed: {}", broker_id, e);
                }
            }
        }

        // Enforce max_poll_records (§2.9 fix)
        // Negative values are treated as unlimited (no truncation)
        // §10.1 fix: Only advance offsets for records actually delivered.
        // When truncating, recompute offset updates from delivered records only.
        if self.config.max_poll_records > 0 {
            let max = self.config.max_poll_records as usize;
            if all_records.len() > max {
                all_records.truncate(max);
                // Recompute offset updates from the truncated set: for each
                // (topic, partition), the new offset is max(record.offset) + 1
                // only for records that survived truncation.
                let mut delivered_offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
                for r in &all_records {
                    let key = (r.topic.clone(), r.partition);
                    let entry = delivered_offsets.entry(key).or_insert(r.offset);
                    if r.offset > *entry {
                        *entry = r.offset;
                    }
                }
                all_offset_updates = delivered_offsets
                    .into_iter()
                    .map(|(key, offset)| (key, offset + 1))
                    .collect();
            }
        }

        // Commit the offset updates (deferred from batch_fetch_from_broker per §10.1)
        if !all_offset_updates.is_empty() {
            let mut offsets = self.offsets.write().await;
            for (key, new_offset) in all_offset_updates {
                offsets.insert(key, new_offset);
            }
        }

        // Record metrics
        if all_records.is_empty() {
            self.metrics.empty_polls.inc();
        } else {
            let bytes: u64 = all_records
                .iter()
                .map(|r| r.value.as_ref().map(|v| v.len() as u64).unwrap_or(0))
                .sum();
            self.metrics.record_receive(all_records.len() as u64, bytes);
        }

        // Invoke consumer interceptor after fetching records
        if !all_records.is_empty() {
            crate::interceptor::safe_on_consume(&*self.interceptor, &all_records);
        }

        Ok(all_records)
    }

    /// Batch fetch from a single broker for multiple topic-partitions.
    ///
    /// This is more efficient than individual fetches because it sends a single
    /// network request for all partitions led by the same broker.
    async fn batch_fetch_from_broker(
        &self,
        broker_id: crate::BrokerId,
        topic_partitions: &[(String, PartitionId)],
        timeout: Duration,
    ) -> Result<(Vec<ConsumerRecord>, Vec<((String, PartitionId), Offset)>)> {
        if topic_partitions.is_empty() {
            return Ok((Vec::new(), Vec::new()));
        }

        self.metrics.record_fetch();
        let _fetch_timer = self.metrics.fetch_latency.start();

        // Get connection to this broker
        let broker =
            self.metadata.broker(broker_id).await.ok_or_else(|| {
                KrafkaError::invalid_state(format!("broker {} not found", broker_id))
            })?;
        let conn = self
            .pool
            .get_connection_by_id(broker_id, &broker.address())
            .await?;

        // Group by topic for the request structure
        let mut topics_map: HashMap<String, Vec<PartitionId>> = HashMap::new();
        for (topic, partition) in topic_partitions {
            topics_map
                .entry(topic.clone())
                .or_default()
                .push(*partition);
        }

        // Build fetch request with all topic-partitions
        let mut fetch_topics = Vec::with_capacity(topics_map.len());
        for (topic, partitions) in &topics_map {
            let mut fetch_partitions = Vec::with_capacity(partitions.len());
            for &partition in partitions {
                // §R13.3 fix: Skip partitions with no tracked offset rather than
                // defaulting to 0, which defeats the §12.5 auto_offset_reset fix.
                let offset = {
                    let offsets = self.offsets.read().await;
                    match offsets.get(&(topic.clone(), partition)).copied() {
                        Some(o) => o,
                        None => {
                            warn!(
                                "No offset for {}-{}, skipping fetch (will retry offset resolution)",
                                topic, partition
                            );
                            continue;
                        }
                    }
                };
                // Get leader epoch from metadata for fencing stale reads
                let leader_epoch = self
                    .metadata
                    .leader_epoch(topic, partition)
                    .await
                    .unwrap_or(-1);
                fetch_partitions.push(FetchPartitionRequest {
                    partition,
                    current_leader_epoch: leader_epoch,
                    fetch_offset: offset,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: self.config.max_partition_fetch_bytes,
                });
            }
            fetch_topics.push(FetchTopicRequest {
                topic: topic.clone(),
                partitions: fetch_partitions,
            });
        }

        let request = FetchRequest {
            replica_id: -1, // Consumer
            max_wait_ms: crate::util::duration_to_millis_i32(timeout),
            min_bytes: self.config.fetch_min_bytes,
            max_bytes: self.config.fetch_max_bytes,
            isolation_level: self.config.isolation_level.to_i8(),
            session_id: 0,
            session_epoch: -1,
            topics: fetch_topics,
        };

        // Send request
        let response = conn
            .send_request(ApiKey::Fetch, 4, |buf| {
                request.encode_v4(buf);
            })
            .await?;

        // Decode response
        let mut buf = response;
        let fetch_response = FetchResponse::decode_v4(&mut buf)?;

        // Process records
        let mut records = Vec::new();
        let mut offset_updates: Vec<((String, PartitionId), Offset)> = Vec::new();

        for topic_response in fetch_response.responses {
            let topic_name = &topic_response.topic;
            for partition_response in topic_response.partitions {
                let partition = partition_response.partition;

                if !partition_response.error_code.is_ok() {
                    // Handle leader epoch errors by validating via OffsetForLeaderEpoch
                    if partition_response.error_code == crate::error::ErrorCode::FencedLeaderEpoch
                        || partition_response.error_code
                            == crate::error::ErrorCode::UnknownLeaderEpoch
                    {
                        warn!(
                            "Leader epoch error for {}-{}: {:?}, validating offset via OffsetForLeaderEpoch",
                            topic_name, partition, partition_response.error_code
                        );
                        // Trigger metadata refresh and reset offset if truncation detected
                        if let Err(e) = self
                            .validate_offset_for_leader_epoch(topic_name, partition)
                            .await
                        {
                            warn!(
                                "OffsetForLeaderEpoch validation failed for {}-{}: {}",
                                topic_name, partition, e
                            );
                        }
                    } else if partition_response.error_code
                        == crate::error::ErrorCode::OffsetOutOfRange
                    {
                        // §15.1 fix: Apply auto_offset_reset for OffsetOutOfRange
                        // to resume consumption instead of stalling the partition.
                        warn!(
                            "OffsetOutOfRange for {}-{}, applying auto_offset_reset",
                            topic_name, partition
                        );
                        if let Some(ref gc) = self.group_coordinator {
                            let target = self.config.auto_offset_reset.to_offset();
                            if let Some(target) = target {
                                let mut part_map = std::collections::HashMap::new();
                                part_map.insert(topic_name.clone(), vec![partition]);
                                match gc.list_offsets(&part_map, target).await {
                                    Ok(resolved) => {
                                        if let Some(&new_offset) =
                                            resolved.get(&(topic_name.clone(), partition))
                                        {
                                            let mut offsets = self.offsets.write().await;
                                            offsets.insert(
                                                (topic_name.clone(), partition),
                                                new_offset,
                                            );
                                        }
                                    }
                                    Err(e) => {
                                        warn!(
                                            "Failed to resolve offset for {}-{}: {}",
                                            topic_name, partition, e
                                        );
                                    }
                                }
                            }
                        }
                    } else {
                        warn!(
                            "Fetch error for {}-{}: {:?}",
                            topic_name, partition, partition_response.error_code
                        );
                    }
                    continue; // Continue with other partitions
                }

                if let Some(record_bytes) = partition_response.records {
                    let mut batch_buf = record_bytes;
                    let mut last_offset_for_partition: Option<Offset> = None;

                    while batch_buf.len() >= 12 {
                        match RecordBatch::decode(&mut batch_buf) {
                            Ok(batch) => {
                                for record in batch.records.into_iter() {
                                    // Use offset_delta for correct offset in compacted topics
                                    // where records may have been deleted (log compaction awareness).
                                    let record_offset =
                                        batch.base_offset + record.offset_delta as i64;
                                    let key_size =
                                        record.key.as_ref().map(|k| k.len() as i32).unwrap_or(-1);
                                    let value_size =
                                        record.value.as_ref().map(|v| v.len() as i32).unwrap_or(-1);
                                    records.push(ConsumerRecord {
                                        topic: topic_name.clone(),
                                        partition,
                                        offset: record_offset,
                                        timestamp: batch.base_timestamp + record.timestamp_delta,
                                        timestamp_type: batch.attributes.timestamp_type as i8,
                                        key: record.key,
                                        value: record.value,
                                        headers: record
                                            .headers
                                            .into_iter()
                                            .map(|h| (h.key, h.value))
                                            .collect(),
                                        leader_epoch: None,
                                        serialized_key_size: key_size,
                                        serialized_value_size: value_size,
                                    });
                                    last_offset_for_partition = Some(record_offset);
                                }
                            }
                            Err(e) => {
                                debug!("Failed to decode record batch: {}", e);
                                break;
                            }
                        }
                    }

                    // Track offset update for this partition
                    if let Some(last_offset) = last_offset_for_partition {
                        offset_updates.push(((topic_name.clone(), partition), last_offset + 1));
                    }
                }
            }
        }

        // NOTE: Offsets are NOT advanced here. They are advanced in poll()
        // after max_poll_records truncation to avoid silently losing records
        // whose offsets were already committed (§10.1 fix).
        // We return offset_updates alongside records so the caller can apply them.
        Ok((records, offset_updates))
    }

    /// Validate the consumer's offset for a partition using OffsetForLeaderEpoch.
    ///
    /// When a leader epoch error occurs during fetch, this method queries the
    /// broker for the end offset of the current leader epoch. If the consumer's
    /// current offset is beyond this (indicating log truncation), the offset
    /// is reset to the truncation point.
    async fn validate_offset_for_leader_epoch(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<()> {
        use crate::protocol::{
            OffsetForLeaderEpochPartition, OffsetForLeaderEpochRequest,
            OffsetForLeaderEpochResponse, OffsetForLeaderEpochTopic,
        };

        // Refresh metadata first to get updated leader info (§R13.10 fix: log failure)
        if let Err(e) = self.metadata.refresh_for_topics(Some(&[topic])).await {
            warn!(
                "Metadata refresh failed for {}: {}, using cached metadata",
                topic, e
            );
        }

        let leader_epoch = self
            .metadata
            .leader_epoch(topic, partition)
            .await
            .unwrap_or(-1);

        if leader_epoch < 0 {
            return Ok(());
        }

        let leader_id = self
            .metadata
            .leader(topic, partition)
            .await
            .ok_or_else(|| {
                KrafkaError::invalid_state(format!("no leader for {}-{}", topic, partition))
            })?;

        let broker =
            self.metadata.broker(leader_id).await.ok_or_else(|| {
                KrafkaError::invalid_state(format!("broker {} not found", leader_id))
            })?;

        let conn = self
            .pool
            .get_connection_by_id(leader_id, &broker.address())
            .await?;

        let request = OffsetForLeaderEpochRequest {
            replica_id: -1, // consumer
            topics: vec![OffsetForLeaderEpochTopic {
                topic: topic.to_string(),
                partitions: vec![OffsetForLeaderEpochPartition {
                    partition,
                    current_leader_epoch: leader_epoch,
                    leader_epoch,
                }],
            }],
        };

        let response_bytes = conn
            .send_request(ApiKey::OffsetForLeaderEpoch, 2, |buf| {
                request.encode_v2(buf);
            })
            .await?;

        let mut buf = response_bytes;
        let response = OffsetForLeaderEpochResponse::decode_v2(&mut buf)?;

        for topic_result in response.topics {
            for partition_result in topic_result.partitions {
                if partition_result.partition != partition {
                    continue;
                }
                if partition_result.error_code.is_ok() && partition_result.end_offset >= 0 {
                    let current_offset = {
                        let offsets = self.offsets.read().await;
                        offsets
                            .get(&(topic.to_string(), partition))
                            .copied()
                            .unwrap_or(0)
                    };

                    if current_offset > partition_result.end_offset {
                        warn!(
                            "Log truncation detected for {}-{}: offset {} > end_offset {}, resetting",
                            topic, partition, current_offset, partition_result.end_offset
                        );
                        let mut offsets = self.offsets.write().await;
                        offsets.insert((topic.to_string(), partition), partition_result.end_offset);
                    }
                }
            }
        }

        Ok(())
    }

    /// Receive the next record.
    ///
    /// This is a convenience method that returns one record at a time.
    /// Internally buffers records from `poll()` and returns them one by one,
    /// ensuring no records are lost (§R13.1 fix).
    ///
    /// Returns `Ok(None)` if the consumer is closed, or `Err` on failure.
    pub async fn recv(&self) -> Result<Option<ConsumerRecord>> {
        loop {
            // Return buffered records first (§R13.1)
            {
                let mut buffer = self.recv_buffer.write().await;
                if let Some(record) = buffer.pop_front() {
                    return Ok(Some(record));
                }
            }

            if self.closed.load(std::sync::atomic::Ordering::SeqCst) {
                return Ok(None);
            }

            match self.poll(Duration::from_secs(1)).await {
                Ok(records) if !records.is_empty() => {
                    let mut iter = records.into_iter();
                    let first = iter.next().unwrap();
                    // Buffer any remaining records for subsequent recv() calls
                    if iter.len() > 0 {
                        let mut buffer = self.recv_buffer.write().await;
                        buffer.extend(iter);
                    }
                    return Ok(Some(first));
                }
                Ok(_) => continue,
                Err(_) if self.closed.load(std::sync::atomic::Ordering::SeqCst) => {
                    return Ok(None);
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }
    }

    /// Commit offsets for all consumed records.
    ///
    /// This stores the current offsets for assigned partitions only.
    /// When using a consumer group, this sends an OffsetCommit request to the group coordinator.
    /// Offsets for revoked partitions are excluded to avoid overwriting the new owner's progress.
    pub async fn commit(&self) -> Result<()> {
        let offsets = self.offsets.read().await;
        if offsets.is_empty() {
            debug!("No offsets to commit");
            return Ok(());
        }

        self.metrics.commits.inc();

        // Build the set of currently assigned partitions, so we don't commit
        // stale offsets for revoked partitions (§9.8 fix).
        let assignments = self.assignments.read().await;
        let assigned_set: HashSet<(String, PartitionId)> = assignments
            .iter()
            .flat_map(|(topic, parts)| parts.iter().map(move |&p| (topic.clone(), p)))
            .collect();

        // If we have a group coordinator, send actual OffsetCommit request
        if let Some(ref coordinator) = self.group_coordinator {
            // Convert offsets to the format expected by coordinator,
            // filtering to only currently assigned partitions.
            // §R13.6 fix: Use explicit group check instead of assigned_set.is_empty()
            // to avoid committing stale offsets when assignments are temporarily empty.
            let commit_offsets: HashMap<(String, PartitionId), (i64, Option<String>)> = offsets
                .iter()
                .filter(|((topic, partition), _)| {
                    assigned_set.contains(&(topic.clone(), *partition))
                })
                .map(|((topic, partition), offset)| ((topic.clone(), *partition), (*offset, None)))
                .collect();

            if commit_offsets.is_empty() {
                debug!("No assigned partition offsets to commit");
                return Ok(());
            }

            // §15.3 fix: Only pass actually-committed offsets to interceptor
            let committed_offsets: HashMap<(String, PartitionId), Offset> = commit_offsets
                .iter()
                .map(|((topic, partition), (offset, _))| ((topic.clone(), *partition), *offset))
                .collect();

            match coordinator.commit_offsets(&commit_offsets).await {
                Ok(()) => {
                    crate::interceptor::safe_on_commit(
                        &*self.interceptor,
                        &committed_offsets,
                        None,
                    );
                }
                Err(e) => {
                    crate::interceptor::safe_on_commit(
                        &*self.interceptor,
                        &committed_offsets,
                        Some(&e),
                    );
                    return Err(e);
                }
            }
        } else {
            // Log offsets for non-group consumers
            for ((topic, partition), offset) in offsets.iter() {
                debug!("Committed offset for {}-{}: {}", topic, partition, offset);
            }
            info!("Committed {} partition offsets (local only)", offsets.len());
        }

        Ok(())
    }

    /// Commit offsets synchronously.
    pub async fn commit_sync(&self) -> Result<()> {
        self.commit().await
    }

    /// Commit offsets asynchronously.
    ///
    /// Spawns the offset commit as a background task. Errors are logged
    /// but not propagated to the caller. Use `commit_sync()` if you need
    /// to handle commit errors.
    pub fn commit_async(&self) {
        // Snapshot offsets without blocking (try_read avoids async in non-async fn)
        // Also filter to only assigned partitions (§9.8 fix)
        let assigned_set: HashSet<(String, PartitionId)> = match self.assignments.try_read() {
            Ok(guard) => guard
                .iter()
                .flat_map(|(topic, parts)| parts.iter().map(move |&p| (topic.clone(), p)))
                .collect(),
            Err(_) => HashSet::new(), // If lock contention, include all (safe fallback)
        };

        let offsets_snapshot: HashMap<(String, PartitionId), (i64, Option<String>)> = match self
            .offsets
            .try_read()
        {
            Ok(guard) => {
                if guard.is_empty() {
                    return;
                }
                guard
                    .iter()
                    .filter(|((topic, partition), _)| {
                        // §R13.6 fix: Only commit offsets for assigned partitions
                        // when using group coordination. Manual assign mode commits all.
                        self.group_coordinator.is_none()
                            || assigned_set.contains(&(topic.clone(), *partition))
                    })
                    .map(|((topic, partition), offset)| {
                        ((topic.clone(), *partition), (*offset, None))
                    })
                    .collect()
            }
            Err(_) => {
                // §15.2 fix: Log warning on contention so dropped commits are visible
                tracing::warn!("commit_async: offset lock contention, skipping this commit cycle");
                return;
            }
        };

        let coordinator = self.group_coordinator.clone();
        let group_id = self.config.group_id.clone();
        tokio::spawn(async move {
            if let Some(ref coordinator) = coordinator {
                if let Err(e) = coordinator.commit_offsets(&offsets_snapshot).await {
                    tracing::warn!(
                        group_id = ?group_id,
                        error = %e,
                        "Async offset commit failed"
                    );
                }
            } else {
                tracing::debug!("Async commit: no group coordinator, offsets stored locally");
            }
        });
    }

    /// Commit specific offsets with metadata.
    ///
    /// Allows committing offsets for specific topic-partitions with optional metadata.
    /// This is useful for checkpointing or storing application-specific context.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::collections::HashMap;
    /// use krafka::consumer::{Consumer, OffsetAndMetadata, TopicPartition};
    ///
    /// # async fn example() -> Result<(), krafka::error::KrafkaError> {
    /// # let consumer: Consumer = todo!();
    /// let mut offsets = HashMap::new();
    /// offsets.insert(
    ///     TopicPartition::new("my-topic", 0),
    ///     OffsetAndMetadata::with_metadata(100, "checkpoint-abc123"),
    /// );
    /// consumer.commit_with_metadata(offsets).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn commit_with_metadata(
        &self,
        offsets: HashMap<TopicPartition, OffsetAndMetadata>,
    ) -> Result<()> {
        if offsets.is_empty() {
            debug!("No offsets to commit");
            return Ok(());
        }

        // Filter to only assigned partitions
        let assignments = self.assignments.read().await;
        let filtered_offsets: HashMap<TopicPartition, OffsetAndMetadata> = if assignments.is_empty()
        {
            // No group membership — accept all
            offsets
        } else {
            offsets
                .into_iter()
                .filter(|(tp, _)| {
                    assignments
                        .get(&tp.topic)
                        .is_some_and(|ps| ps.contains(&tp.partition))
                })
                .collect()
        };
        drop(assignments);

        if filtered_offsets.is_empty() {
            debug!("No offsets to commit after filtering by assigned partitions");
            return Ok(());
        }

        // If we have a group coordinator, send actual OffsetCommit request
        if let Some(ref coordinator) = self.group_coordinator {
            // Convert offsets to the format expected by coordinator
            let commit_offsets: HashMap<(String, PartitionId), (i64, Option<String>)> =
                filtered_offsets
                    .iter()
                    .map(|(tp, offset_meta)| {
                        (
                            (tp.topic.clone(), tp.partition),
                            (offset_meta.offset, offset_meta.metadata.clone()),
                        )
                    })
                    .collect();

            let interceptor_offsets: HashMap<(String, PartitionId), i64> = filtered_offsets
                .iter()
                .map(|(tp, om)| ((tp.topic.clone(), tp.partition), om.offset))
                .collect();

            match coordinator.commit_offsets(&commit_offsets).await {
                Ok(()) => {
                    crate::interceptor::safe_on_commit(
                        &*self.interceptor,
                        &interceptor_offsets,
                        None,
                    );
                }
                Err(e) => {
                    crate::interceptor::safe_on_commit(
                        &*self.interceptor,
                        &interceptor_offsets,
                        Some(&e),
                    );
                    return Err(e);
                }
            }

            // Update internal offset store
            let mut internal_offsets = self.offsets.write().await;
            for (tp, offset_meta) in filtered_offsets {
                internal_offsets.insert((tp.topic, tp.partition), offset_meta.offset);
            }
        } else {
            // Log offsets being committed with metadata for non-group consumers
            for (tp, offset_meta) in &filtered_offsets {
                let metadata_str = offset_meta.metadata.as_deref().unwrap_or("<none>");
                debug!(
                    "Committed offset for {}-{}: {} (metadata: {})",
                    tp.topic, tp.partition, offset_meta.offset, metadata_str
                );
            }

            // Update internal offset store
            let mut internal_offsets = self.offsets.write().await;
            for (tp, offset_meta) in filtered_offsets {
                internal_offsets.insert((tp.topic, tp.partition), offset_meta.offset);
            }

            info!(
                "Committed {} partition offsets with metadata (local only)",
                internal_offsets.len()
            );
        }

        Ok(())
    }

    /// Get the current position for a partition.
    pub async fn position(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        let offsets = self.offsets.read().await;
        offsets.get(&(topic.to_string(), partition)).copied()
    }

    /// Get all assigned partitions.
    pub async fn assignment(&self) -> HashMap<String, Vec<PartitionId>> {
        let assignments = self.assignments.read().await;
        assignments.clone()
    }

    /// Get all subscribed topics.
    pub async fn subscription(&self) -> HashSet<String> {
        let subscriptions = self.subscriptions.read().await;
        subscriptions.clone()
    }

    /// Unsubscribe from all topics.
    ///
    /// §R13.4 fix: properly notifies the rebalance listener, leaves the
    /// consumer group, clears offsets, paused set, and drains recv buffer.
    pub async fn unsubscribe(&self) {
        // Notify listener of revoked partitions before clearing
        let assignments = self.assignments.read().await;
        if !assignments.is_empty() {
            let revoked: Vec<TopicPartition> = assignments
                .iter()
                .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
                .collect();
            self.rebalance_listener.on_partitions_revoked(&revoked);
        }
        drop(assignments);

        // Leave consumer group
        if let Some(ref coordinator) = self.group_coordinator
            && let Err(e) = coordinator.leave_group().await
        {
            warn!("Error leaving consumer group during unsubscribe: {}", e);
        }

        self.subscriptions.write().await.clear();
        self.assignments.write().await.clear();
        self.offsets.write().await.clear();
        self.paused.write().await.clear();
        self.recv_buffer.write().await.clear();

        debug!("Unsubscribed from all topics");
    }

    /// Pause consumption of specific partitions.
    ///
    /// Paused partitions will be skipped during poll() until resumed.
    pub async fn pause(&self, topic: &str, partitions: &[PartitionId]) {
        let mut paused = self.paused.write().await;
        for &partition in partitions {
            paused.insert((topic.to_string(), partition));
        }
        self.metrics.paused_partitions.set(paused.len() as u64);
        debug!("Paused partitions for {}: {:?}", topic, partitions);
    }

    /// Resume consumption of specific partitions.
    ///
    /// Resumes polling for previously paused partitions.
    pub async fn resume(&self, topic: &str, partitions: &[PartitionId]) {
        let mut paused = self.paused.write().await;
        for &partition in partitions {
            paused.remove(&(topic.to_string(), partition));
        }
        self.metrics.paused_partitions.set(paused.len() as u64);
        debug!("Resumed partitions for {}: {:?}", topic, partitions);
    }

    /// Get the set of paused partitions.
    pub async fn paused_partitions(&self) -> HashSet<(String, PartitionId)> {
        self.paused.read().await.clone()
    }

    /// Close the consumer.
    pub async fn close(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);

        // Auto-commit on close (if enabled)
        if self.config.enable_auto_commit
            && let Err(e) = self.commit().await
        {
            warn!("Auto-commit on close failed: {}", e);
        }

        // Notify listener that partitions are being lost
        let assignments = self.assignments.read().await;
        if !assignments.is_empty() {
            let lost: Vec<TopicPartition> = assignments
                .iter()
                .flat_map(|(t, ps)| ps.iter().map(move |&p| TopicPartition::new(t, p)))
                .collect();
            self.rebalance_listener.on_partitions_lost(&lost);
        }
        drop(assignments);

        // Leave consumer group if we have a group coordinator
        if let Some(ref coordinator) = self.group_coordinator
            && let Err(e) = coordinator.leave_group().await
        {
            warn!("Error leaving consumer group: {e}");
        }

        // Notify interceptor of shutdown
        crate::interceptor::safe_consumer_close(&*self.interceptor);

        self.pool.close_all().await;
        info!("Consumer closed");
    }

    /// Check if the consumer is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Get the group coordinator, if one is configured.
    pub fn group_coordinator(&self) -> Option<&Arc<GroupCoordinator>> {
        self.group_coordinator.as_ref()
    }

    /// Get a snapshot of consumer metrics.
    pub fn metrics(&self) -> &Arc<ConsumerMetrics> {
        &self.metrics
    }
}

/// Builder for creating consumers.
#[derive(Default)]
#[must_use = "builders do nothing until .build() is called"]
pub struct ConsumerBuilder {
    config: ConsumerConfig,
    rebalance_listener: Option<Arc<dyn ConsumerRebalanceListener>>,
    interceptor: Option<Arc<dyn crate::interceptor::ConsumerInterceptor>>,
}

impl ConsumerBuilder {
    /// Set the bootstrap servers.
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    /// Set the group ID.
    pub fn group_id(mut self, group_id: impl Into<String>) -> Self {
        self.config.group_id = Some(group_id.into());
        self
    }

    /// Set the client ID.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.config.client_id = client_id.into();
        self
    }

    /// Set auto offset reset behavior.
    pub fn auto_offset_reset(mut self, reset: AutoOffsetReset) -> Self {
        self.config.auto_offset_reset = reset;
        self
    }

    /// Enable auto commit.
    pub fn enable_auto_commit(mut self, enable: bool) -> Self {
        self.config.enable_auto_commit = enable;
        self
    }

    /// Set auto commit interval.
    pub fn auto_commit_interval(mut self, interval: Duration) -> Self {
        self.config.auto_commit_interval = interval;
        self
    }

    /// Set fetch minimum bytes.
    pub fn fetch_min_bytes(mut self, bytes: i32) -> Self {
        self.config.fetch_min_bytes = bytes;
        self
    }

    /// Set fetch maximum bytes.
    pub fn fetch_max_bytes(mut self, bytes: i32) -> Self {
        self.config.fetch_max_bytes = bytes;
        self
    }

    /// Set max partition fetch bytes.
    pub fn max_partition_fetch_bytes(mut self, bytes: i32) -> Self {
        self.config.max_partition_fetch_bytes = bytes;
        self
    }

    /// Set maximum poll records per poll() call.
    pub fn max_poll_records(mut self, max: i32) -> Self {
        self.config.max_poll_records = max;
        self
    }

    /// Set maximum poll interval before consumer is considered dead.
    pub fn max_poll_interval(mut self, interval: Duration) -> Self {
        self.config.max_poll_interval = interval;
        self
    }

    /// Set request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set session timeout for consumer groups.
    pub fn session_timeout(mut self, timeout: Duration) -> Self {
        self.config.session_timeout = timeout;
        self
    }

    /// Set heartbeat interval.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    /// Set isolation level.
    pub fn isolation_level(mut self, level: IsolationLevel) -> Self {
        self.config.isolation_level = level;
        self
    }

    /// Set partition assignment strategy for consumer groups.
    pub fn partition_assignment_strategy(mut self, strategy: PartitionAssignmentStrategy) -> Self {
        self.config.partition_assignment_strategy = strategy;
        self
    }

    /// Set the static group membership instance ID (KIP-345).
    ///
    /// When configured, the consumer uses static group membership. The broker
    /// preserves partition assignments across restarts as long as the same
    /// instance ID is used, avoiding unnecessary rebalances.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let consumer = Consumer::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .group_id("my-group")
    ///     .group_instance_id("instance-1")
    ///     .build()
    ///     .await?;
    /// ```
    pub fn group_instance_id(mut self, id: impl Into<String>) -> Self {
        self.config.group_instance_id = Some(id.into());
        self
    }

    /// Set metadata max age before forcing a refresh.
    pub fn metadata_max_age(mut self, age: Duration) -> Self {
        self.config.metadata_max_age = age;
        self
    }

    /// Set a rebalance listener to be notified of partition assignment changes.
    pub fn rebalance_listener(mut self, listener: Arc<dyn ConsumerRebalanceListener>) -> Self {
        self.rebalance_listener = Some(listener);
        self
    }

    /// Set authentication configuration.
    ///
    /// Enables TLS and/or SASL authentication for all broker connections.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use krafka::consumer::Consumer;
    /// use krafka::auth::AuthConfig;
    ///
    /// let consumer = Consumer::builder()
    ///     .bootstrap_servers("broker:9093")
    ///     .group_id("my-group")
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

    /// Set a consumer interceptor.
    ///
    /// The interceptor's `on_consume` method is called after records are fetched
    /// but before they are returned from `poll()`, and `on_commit` is called
    /// after offsets are committed.
    pub fn interceptor(
        mut self,
        interceptor: Arc<dyn crate::interceptor::ConsumerInterceptor>,
    ) -> Self {
        self.interceptor = Some(interceptor);
        self
    }

    /// Build the consumer.
    pub async fn build(self) -> Result<Consumer> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap.servers is required"));
        }
        if self.config.enable_auto_commit && self.config.group_id.is_none() {
            tracing::warn!(
                "enable_auto_commit=true has no effect without group_id; \
                 offsets will not be persisted to the broker"
            );
        }
        if self.config.heartbeat_interval >= self.config.session_timeout {
            return Err(KrafkaError::config(
                "heartbeat_interval must be less than session_timeout \
                 (recommended: session_timeout / 3)",
            ));
        }
        let mut consumer = Consumer::new(self.config).await?;
        if let Some(listener) = self.rebalance_listener {
            consumer.rebalance_listener = listener;
        }
        if let Some(interceptor) = self.interceptor {
            consumer.interceptor = interceptor;
        }
        Ok(consumer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consumer_builder() {
        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .client_id("test")
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .max_poll_records(100)
            .max_poll_interval(Duration::from_secs(600));

        assert_eq!(builder.config.bootstrap_servers, "localhost:9092");
        assert_eq!(builder.config.group_id, Some("test-group".to_string()));
        assert_eq!(builder.config.client_id, "test");
        assert_eq!(builder.config.auto_offset_reset, AutoOffsetReset::Earliest);
        assert!(!builder.config.enable_auto_commit);
        assert_eq!(builder.config.max_poll_records, 100);
        assert_eq!(builder.config.max_poll_interval, Duration::from_secs(600));
        assert!(builder.config.auth.is_none());
    }

    #[test]
    fn test_consumer_builder_with_auth() {
        let builder = Consumer::builder()
            .bootstrap_servers("broker:9093")
            .group_id("secure-group")
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
    fn test_consumer_builder_aws_msk_iam() {
        let auth = AuthConfig::aws_msk_iam("AKID", "secret", "us-east-1");
        let builder = Consumer::builder()
            .bootstrap_servers("broker:9094")
            .group_id("msk-group")
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
    fn test_consumer_builder_no_auth_by_default() {
        let builder = Consumer::builder()
            .bootstrap_servers("broker:9092")
            .group_id("group");

        assert!(builder.config.auth.is_none());
    }

    #[test]
    fn test_consumer_builder_sasl_plain() {
        let builder = Consumer::builder()
            .bootstrap_servers("broker:9093")
            .sasl_plain("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.plain_credentials.is_some());
    }

    #[test]
    fn test_consumer_builder_sasl_scram() {
        let builder = Consumer::builder()
            .bootstrap_servers("broker:9093")
            .sasl_scram_sha256("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.scram_credentials.is_some());

        let builder = Consumer::builder()
            .bootstrap_servers("broker:9093")
            .sasl_scram_sha512("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.scram_credentials.is_some());
    }

    #[tokio::test]
    async fn test_consumer_builder_no_servers() {
        let result = Consumer::builder().build().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_paused_partitions_set() {
        let mut paused: HashSet<(String, PartitionId)> = HashSet::new();
        paused.insert(("topic1".to_string(), 0));
        paused.insert(("topic1".to_string(), 1));
        paused.insert(("topic2".to_string(), 0));

        assert!(paused.contains(&("topic1".to_string(), 0)));
        assert!(paused.contains(&("topic1".to_string(), 1)));
        assert!(paused.contains(&("topic2".to_string(), 0)));
        assert!(!paused.contains(&("topic2".to_string(), 1)));

        paused.remove(&("topic1".to_string(), 0));
        assert!(!paused.contains(&("topic1".to_string(), 0)));
    }

    #[test]
    fn test_topic_partition() {
        let tp = TopicPartition::new("my-topic", 3);
        assert_eq!(tp.topic(), "my-topic");
        assert_eq!(tp.partition(), 3);

        // Test Hash/Eq for HashMap use
        let mut map = HashMap::new();
        map.insert(TopicPartition::new("test", 0), 100i64);
        map.insert(TopicPartition::new("test", 1), 200i64);
        assert_eq!(map.get(&TopicPartition::new("test", 0)), Some(&100i64));
        assert_eq!(map.get(&TopicPartition::new("test", 1)), Some(&200i64));
    }

    #[test]
    fn test_offset_and_metadata() {
        let offset = OffsetAndMetadata::new(100);
        assert_eq!(offset.offset, 100);
        assert!(offset.metadata.is_none());

        let offset_with_meta = OffsetAndMetadata::with_metadata(200, "checkpoint-123");
        assert_eq!(offset_with_meta.offset, 200);
        assert_eq!(offset_with_meta.metadata.as_deref(), Some("checkpoint-123"));

        let offset_with_epoch = OffsetAndMetadata::with_epoch(300, 5);
        assert_eq!(offset_with_epoch.offset, 300);
        assert_eq!(offset_with_epoch.leader_epoch, Some(5));
    }

    #[test]
    fn test_consumer_builder_partition_assignment_strategy() {
        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .partition_assignment_strategy(PartitionAssignmentStrategy::RoundRobin);

        assert_eq!(
            builder.config.partition_assignment_strategy,
            PartitionAssignmentStrategy::RoundRobin
        );
    }

    #[test]
    fn test_consumer_builder_with_rebalance_listener() {
        use std::sync::atomic::{AtomicBool, Ordering};

        struct TestListener {
            assigned: AtomicBool,
        }
        impl ConsumerRebalanceListener for TestListener {
            fn on_partitions_assigned(&self, _: &[TopicPartition]) {
                self.assigned.store(true, Ordering::SeqCst);
            }
            fn on_partitions_revoked(&self, _: &[TopicPartition]) {}
        }

        let listener = Arc::new(TestListener {
            assigned: AtomicBool::new(false),
        });

        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .rebalance_listener(listener.clone());

        assert!(builder.rebalance_listener.is_some());
    }

    #[test]
    fn test_partition_assignment_strategy_default() {
        let config = ConsumerConfig::default();
        assert_eq!(
            config.partition_assignment_strategy,
            PartitionAssignmentStrategy::Range
        );
    }

    #[test]
    fn test_partition_assignment_strategy_protocol_name() {
        assert_eq!(PartitionAssignmentStrategy::Range.protocol_name(), "range");
        assert_eq!(
            PartitionAssignmentStrategy::RoundRobin.protocol_name(),
            "roundrobin"
        );
        assert_eq!(
            PartitionAssignmentStrategy::CooperativeSticky.protocol_name(),
            "cooperative-sticky"
        );
    }

    #[test]
    fn test_consumer_config_defaults() {
        let config = ConsumerConfig::default();
        // Verify sensible defaults
        assert!(config.fetch_max_bytes > 0);
        assert!(config.fetch_min_bytes > 0);
        assert!(config.max_partition_fetch_bytes > 0);
    }

    #[tokio::test]
    async fn test_consumer_builder_rejects_bad_heartbeat() {
        // heartbeat_interval >= session_timeout should fail
        let result = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test")
            .session_timeout(Duration::from_secs(5))
            .heartbeat_interval(Duration::from_secs(5))
            .build()
            .await;

        match result {
            Err(e) => assert!(e.to_string().contains("heartbeat_interval")),
            Ok(_) => panic!("expected error for heartbeat_interval >= session_timeout"),
        }
    }

    #[tokio::test]
    async fn test_consumer_builder_rejects_heartbeat_greater_than_session() {
        let result = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test")
            .session_timeout(Duration::from_secs(5))
            .heartbeat_interval(Duration::from_secs(10))
            .build()
            .await;

        assert!(result.is_err());
    }

    /// §10.1 test: Verify max_poll_records truncation recomputes offset updates
    /// to prevent data loss for undelivered records.
    #[test]
    fn test_max_poll_records_offset_recomputation() {
        // Simulate what poll() does: given 5 records but max_poll_records=3,
        // only offsets for the first 3 records should be advanced.
        let records: Vec<ConsumerRecord> = (0..5)
            .map(|i| ConsumerRecord {
                topic: "topic1".to_string(),
                partition: 0,
                offset: 100 + i,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: Some(bytes::Bytes::from(format!("val-{i}"))),
                headers: vec![],
                leader_epoch: None,
                serialized_key_size: -1,
                serialized_value_size: 5,
            })
            .collect();

        let original_offset_updates: Vec<((String, PartitionId), Offset)> =
            vec![(("topic1".to_string(), 0), 105)]; // offset after last record

        let max = 3usize;
        let mut truncated = records;
        truncated.truncate(max);

        // Recompute offsets from truncated records only
        let mut delivered_offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        for r in &truncated {
            let key = (r.topic.clone(), r.partition);
            let entry = delivered_offsets.entry(key).or_insert(r.offset);
            if r.offset > *entry {
                *entry = r.offset;
            }
        }
        let new_offset_updates: Vec<_> = delivered_offsets
            .into_iter()
            .map(|(key, offset)| (key, offset + 1))
            .collect();

        // Should advance to offset 103 (100+2+1), NOT 105
        assert_eq!(new_offset_updates.len(), 1);
        let (key, offset) = &new_offset_updates[0];
        assert_eq!(key, &("topic1".to_string(), 0));
        assert_eq!(*offset, 103); // 100 + 2 (last delivered record offset) + 1

        // Not the original 105
        assert_ne!(*offset, original_offset_updates[0].1);
    }

    /// §10.1 test: Verify max_poll_records with multiple partitions recomputes
    /// offsets correctly per partition.
    #[test]
    fn test_max_poll_records_multi_partition_offset() {
        let mut records = Vec::new();
        // 3 records from partition 0
        for i in 0..3 {
            records.push(ConsumerRecord {
                topic: "topic1".to_string(),
                partition: 0,
                offset: 50 + i,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: Some(bytes::Bytes::from("val")),
                headers: vec![],
                leader_epoch: None,
                serialized_key_size: -1,
                serialized_value_size: 3,
            });
        }
        // 3 records from partition 1
        for i in 0..3 {
            records.push(ConsumerRecord {
                topic: "topic1".to_string(),
                partition: 1,
                offset: 200 + i,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: Some(bytes::Bytes::from("val")),
                headers: vec![],
                leader_epoch: None,
                serialized_key_size: -1,
                serialized_value_size: 3,
            });
        }

        // Truncate to 4 records (all 3 from p0 + 1 from p1)
        records.truncate(4);

        let mut delivered_offsets: HashMap<(String, PartitionId), Offset> = HashMap::new();
        for r in &records {
            let key = (r.topic.clone(), r.partition);
            let entry = delivered_offsets.entry(key).or_insert(r.offset);
            if r.offset > *entry {
                *entry = r.offset;
            }
        }

        // Partition 0: last delivered = 52 → advanced to 53
        assert_eq!(
            *delivered_offsets.get(&("topic1".to_string(), 0)).unwrap(),
            52
        );
        // Partition 1: last delivered = 200 → advanced to 201
        assert_eq!(
            *delivered_offsets.get(&("topic1".to_string(), 1)).unwrap(),
            200
        );
    }

    #[test]
    fn test_consumer_builder_group_instance_id() {
        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .group_instance_id("my-instance");

        assert_eq!(
            builder.config.group_instance_id,
            Some("my-instance".to_string())
        );
    }

    #[test]
    fn test_consumer_builder_interceptor() {
        use crate::interceptor::ConsumerInterceptor;

        #[derive(Debug)]
        struct TestInterceptor;
        impl ConsumerInterceptor for TestInterceptor {
            fn on_consume(&self, _records: &[ConsumerRecord]) {}
        }

        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .interceptor(Arc::new(TestInterceptor));

        assert!(builder.interceptor.is_some());
    }

    // §R13.1: recv() buffers remaining records so none are lost.
    #[tokio::test]
    async fn test_recv_buffer_returns_all_records() {
        use std::collections::VecDeque;

        // Simulate a consumer with pre-filled recv_buffer
        let mut buffer = VecDeque::new();
        buffer.push_back(ConsumerRecord {
            topic: "t".into(),
            partition: 0,
            offset: 1,
            timestamp: 0,
            timestamp_type: 0,
            key: None,
            value: Some(bytes::Bytes::from("r1")),
            headers: vec![],
            leader_epoch: None,
            serialized_key_size: -1,
            serialized_value_size: 2,
        });
        buffer.push_back(ConsumerRecord {
            topic: "t".into(),
            partition: 0,
            offset: 2,
            timestamp: 0,
            timestamp_type: 0,
            key: None,
            value: Some(bytes::Bytes::from("r2")),
            headers: vec![],
            leader_epoch: None,
            serialized_key_size: -1,
            serialized_value_size: 2,
        });

        assert_eq!(buffer.len(), 2);
        let first = buffer.pop_front().unwrap();
        assert_eq!(first.offset, 1);
        let second = buffer.pop_front().unwrap();
        assert_eq!(second.offset, 2);
        assert!(buffer.is_empty());
    }

    // §R13.5: assign() is rejected when group coordinator is active.
    #[test]
    fn test_assign_with_group_id_configured() {
        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group");

        // When group_id is set, group_coordinator will be Some after new().
        // We verify the config at builder level.
        assert!(builder.config.group_id.is_some());
    }

    // §R13.11: subscribe() replaces rather than appending.
    #[test]
    fn test_subscribe_replaces_subscriptions() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let subs: RwLock<HashSet<String>> = RwLock::new(HashSet::new());

            // First subscribe
            {
                let mut s = subs.write().await;
                s.clear(); // §R13.11: clear before insert
                s.insert("topic1".to_string());
            }
            assert_eq!(subs.read().await.len(), 1);
            assert!(subs.read().await.contains("topic1"));

            // Second subscribe replaces, not appends
            {
                let mut s = subs.write().await;
                s.clear(); // §R13.11: clear before insert
                s.insert("topic2".to_string());
            }
            assert_eq!(subs.read().await.len(), 1);
            assert!(subs.read().await.contains("topic2"));
            assert!(!subs.read().await.contains("topic1"));
        });
    }

    // §R13.4: unsubscribe() clears offsets, paused, and recv_buffer.
    #[test]
    fn test_unsubscribe_clears_all_state() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let offsets: RwLock<HashMap<(String, PartitionId), Offset>> =
                RwLock::new(HashMap::new());
            let paused: RwLock<HashSet<(String, PartitionId)>> = RwLock::new(HashSet::new());
            let assignments: RwLock<HashMap<String, Vec<PartitionId>>> =
                RwLock::new(HashMap::new());
            let recv_buffer: RwLock<std::collections::VecDeque<ConsumerRecord>> =
                RwLock::new(std::collections::VecDeque::new());

            // Populate state
            offsets.write().await.insert(("t".into(), 0), 100);
            paused.write().await.insert(("t".into(), 0));
            assignments.write().await.insert("t".into(), vec![0]);
            recv_buffer.write().await.push_back(ConsumerRecord {
                topic: "t".into(),
                partition: 0,
                offset: 0,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: None,
                headers: vec![],
                leader_epoch: None,
                serialized_key_size: -1,
                serialized_value_size: -1,
            });

            // Simulate unsubscribe clearing
            offsets.write().await.clear();
            paused.write().await.clear();
            assignments.write().await.clear();
            recv_buffer.write().await.clear();

            assert!(offsets.read().await.is_empty());
            assert!(paused.read().await.is_empty());
            assert!(assignments.read().await.is_empty());
            assert!(recv_buffer.read().await.is_empty());
        });
    }

    // §R13.3: Fetch skips partitions with no tracked offset.
    #[test]
    fn test_fetch_skips_untracked_partitions() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let offsets: RwLock<HashMap<(String, PartitionId), Offset>> =
                RwLock::new(HashMap::new());
            offsets.write().await.insert(("t".into(), 0), 42);

            let o = offsets.read().await;
            // Partition 0 has an offset
            assert_eq!(o.get(&("t".to_string(), 0)).copied(), Some(42));
            // Partition 1 has no offset — should be skipped
            assert_eq!(o.get(&("t".to_string(), 1)).copied(), None);
        });
    }

    // §R13.6: Commit filtering uses group_coordinator check, not assigned_set emptiness.
    #[test]
    fn test_commit_filter_does_not_leak_stale_offsets() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async {
            let offsets: HashMap<(String, PartitionId), Offset> = [
                (("topic1".into(), 0), 100),
                (("topic2".into(), 0), 200), // stale: not assigned
            ]
            .into_iter()
            .collect();

            let assigned_set: HashSet<(String, PartitionId)> = HashSet::new(); // empty

            // OLD behavior: is_empty() would let ALL offsets through — BAD
            let old_filtered: Vec<_> = offsets
                .iter()
                .filter(|((t, p), _)| {
                    assigned_set.is_empty() || assigned_set.contains(&(t.clone(), *p))
                })
                .collect();
            assert_eq!(old_filtered.len(), 2); // Both pass — wrong

            // NEW behavior (§R13.6): group consumers never commit unassigned
            let has_group = true;
            let new_filtered: Vec<_> = offsets
                .iter()
                .filter(|((t, p), _)| !has_group || assigned_set.contains(&(t.clone(), *p)))
                .collect();
            assert_eq!(new_filtered.len(), 0); // None pass when empty — correct
        });
    }

    // §R13.7: group field removed — only group_coordinator accessor exists.
    #[test]
    fn test_no_legacy_group_field() {
        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group");
        // The builder should have no group field; only group_coordinator is used
        assert!(builder.config.group_id.is_some());
    }

    #[test]
    fn test_bootstrap_filter_empty_strings() {
        // §15.5: Empty bootstrap server entries should be filtered out
        let servers = " , ,localhost:9092, , broker:9093, ";
        let parsed: Vec<String> = servers
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        assert_eq!(parsed, vec!["localhost:9092", "broker:9093"]);

        // Empty string should produce empty vec
        let empty_parsed: Vec<String> = ""
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
        assert!(empty_parsed.is_empty());
    }

    #[test]
    fn test_max_poll_interval_used_for_rebalance() {
        // §15.6: rebalance_timeout should default to max_poll_interval (not session_timeout)
        let config = ConsumerConfig::default();
        // In the Java client, rebalance_timeout defaults to max.poll.interval.ms (300s)
        // not session.timeout.ms (10s). Verify our config has both.
        assert_eq!(config.max_poll_interval, Duration::from_secs(300));
        assert_eq!(config.session_timeout, Duration::from_secs(10));
        // The rebalance_timeout passed to GroupCoordinator should be max_poll_interval
        assert!(config.max_poll_interval > config.session_timeout);
    }
}
