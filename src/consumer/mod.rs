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

pub use config::{AutoOffsetReset, ConsumerConfig, ConsumerConfigBuilder, IsolationLevel};
pub use group::{
    ConsumerGroup, ConsumerRebalanceListener, CooperativeStickyAssignor, GroupCoordinator,
    GroupMember, GroupState, HeartbeatController, HeartbeatStatus, MemberAssignment,
    NoOpRebalanceListener, PartitionAssignor, RangeAssignor, RoundRobinAssignor,
};
pub use offset::{OffsetAndMetadata, OffsetStore, ResetOffset};
pub use record::{ConsumerRecord, ConsumerRecords, TopicPartition};

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use crate::error::{KrafkaError, Result};
use crate::metadata::ClusterMetadata;
use crate::network::{ConnectionConfig, ConnectionPool};
use crate::protocol::{
    ApiKey, FetchPartitionRequest, FetchRequest, FetchResponse, FetchTopicRequest, RecordBatch,
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
    /// Consumer group (legacy, for backwards compatibility).
    group: Option<Arc<ConsumerGroup>>,
    /// Group coordinator for full group protocol support.
    group_coordinator: Option<Arc<GroupCoordinator>>,
}

impl Consumer {
    /// Create a new consumer builder.
    pub fn builder() -> ConsumerBuilder {
        ConsumerBuilder::default()
    }

    /// Create a new consumer with the given configuration.
    async fn new(config: ConsumerConfig) -> Result<Self> {
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

        // Create group coordinator if group_id is specified
        let group_coordinator = if let Some(ref group_id) = config.group_id {
            Some(Arc::new(GroupCoordinator::new(
                group_id.clone(),
                pool.clone(),
                metadata.clone(),
                config.session_timeout,
                config.heartbeat_interval,
                config.session_timeout, // rebalance_timeout defaults to session_timeout
            )))
        } else {
            None
        };

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
            group: None,
            group_coordinator,
        })
    }

    /// Subscribe to topics.
    pub async fn subscribe(&self, topics: &[&str]) -> Result<()> {
        let mut subscriptions = self.subscriptions.write().await;
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
            for (topic, partitions) in assignment.partitions {
                assignments.insert(topic, partitions);
            }

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

            debug!("Subscribed to topics: {:?}", topics);
        }

        Ok(())
    }

    /// Assign specific partitions manually.
    pub async fn assign(&self, topic: &str, partitions: Vec<PartitionId>) -> Result<()> {
        let mut assignments = self.assignments.write().await;
        assignments.insert(topic.to_string(), partitions.clone());

        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.insert(topic.to_string());

        debug!("Assigned partitions for {}: {:?}", topic, partitions);
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
    /// Note: This uses a special offset value (-1) which Kafka interprets as "latest".
    pub async fn seek_to_end(&self, topic: &str, partition: PartitionId) -> Result<()> {
        // Use the special offset -1 which Kafka interprets as "latest"
        // This is the standard way to request the latest offset
        self.seek(topic, partition, -1).await
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

        // Handle group rebalance if needed
        if let Some(ref coordinator) = self.group_coordinator {
            // Check if we need to rejoin the group
            if coordinator.needs_rejoin().await {
                let topics: Vec<String> = self.subscriptions.read().await.iter().cloned().collect();
                if !topics.is_empty() {
                    let assignment = coordinator.ensure_active_membership(&topics).await?;

                    // Update our assignments
                    let mut assignments = self.assignments.write().await;
                    assignments.clear();
                    for (topic, partitions) in assignment.partitions {
                        assignments.insert(topic, partitions);
                    }
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
            return Ok(Vec::new());
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

        // Fetch from each broker (one request per broker, containing all its partitions)
        for (broker_id, topic_partitions) in partitions_by_leader {
            match self
                .batch_fetch_from_broker(broker_id, &topic_partitions, timeout)
                .await
            {
                Ok(records) => all_records.extend(records),
                Err(e) => {
                    warn!("Batch fetch from broker {} failed: {}", broker_id, e);
                }
            }
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
    ) -> Result<Vec<ConsumerRecord>> {
        if topic_partitions.is_empty() {
            return Ok(Vec::new());
        }

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
                let offset = {
                    let offsets = self.offsets.read().await;
                    offsets
                        .get(&(topic.clone(), partition))
                        .copied()
                        .unwrap_or(0)
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
            max_wait_ms: timeout.as_millis() as i32,
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
                    warn!(
                        "Fetch error for {}-{}: {:?}",
                        topic_name, partition, partition_response.error_code
                    );
                    continue; // Continue with other partitions
                }

                if let Some(record_bytes) = partition_response.records {
                    let mut batch_buf = record_bytes;
                    let mut last_offset_for_partition: Option<Offset> = None;

                    while batch_buf.len() >= 12 {
                        match RecordBatch::decode(&mut batch_buf) {
                            Ok(batch) => {
                                for (i, record) in batch.records.into_iter().enumerate() {
                                    let record_offset = batch.base_offset + i as i64;
                                    let key_size =
                                        record.key.as_ref().map(|k| k.len() as i32).unwrap_or(-1);
                                    let value_size =
                                        record.value.as_ref().map(|v| v.len() as i32).unwrap_or(-1);
                                    records.push(ConsumerRecord {
                                        topic: topic_name.clone(),
                                        partition,
                                        offset: record_offset,
                                        timestamp: batch.base_timestamp + record.timestamp_delta,
                                        timestamp_type: 0, // CreateTime
                                        key: record.key,
                                        value: record.value,
                                        headers: record
                                            .headers
                                            .into_iter()
                                            .filter_map(|h| h.value.map(|v| (h.key, v)))
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

        // Batch update offsets
        if !offset_updates.is_empty() {
            let mut offsets = self.offsets.write().await;
            for (key, new_offset) in offset_updates {
                offsets.insert(key, new_offset);
            }
        }

        Ok(records)
    }

    /// Receive the next record.
    ///
    /// This is a convenience method that polls for a single record.
    pub async fn recv(&self) -> Option<ConsumerRecord> {
        loop {
            match self.poll(Duration::from_secs(1)).await {
                Ok(records) if !records.is_empty() => {
                    return Some(records.into_iter().next().unwrap());
                }
                Ok(_) => continue,
                Err(e) => {
                    error!("Error polling: {}", e);
                    return None;
                }
            }
        }
    }

    /// Commit offsets for all consumed records.
    ///
    /// This stores the current offsets for all assigned partitions.
    /// When using a consumer group, this sends an OffsetCommit request to the group coordinator.
    pub async fn commit(&self) -> Result<()> {
        let offsets = self.offsets.read().await;
        if offsets.is_empty() {
            debug!("No offsets to commit");
            return Ok(());
        }

        // If we have a group coordinator, send actual OffsetCommit request
        if let Some(ref coordinator) = self.group_coordinator {
            // Convert offsets to the format expected by coordinator
            let commit_offsets: HashMap<(String, PartitionId), (i64, Option<String>)> = offsets
                .iter()
                .map(|((topic, partition), offset)| ((topic.clone(), *partition), (*offset, None)))
                .collect();

            coordinator.commit_offsets(&commit_offsets).await?;
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
    pub fn commit_async(&self) {
        drop(self.commit());
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

        // If we have a group coordinator, send actual OffsetCommit request
        if let Some(ref coordinator) = self.group_coordinator {
            // Convert offsets to the format expected by coordinator
            let commit_offsets: HashMap<(String, PartitionId), (i64, Option<String>)> = offsets
                .iter()
                .map(|(tp, offset_meta)| {
                    (
                        (tp.topic.clone(), tp.partition),
                        (offset_meta.offset, offset_meta.metadata.clone()),
                    )
                })
                .collect();

            coordinator.commit_offsets(&commit_offsets).await?;

            // Update internal offset store
            let mut internal_offsets = self.offsets.write().await;
            for (tp, offset_meta) in offsets {
                internal_offsets.insert((tp.topic, tp.partition), offset_meta.offset);
            }
        } else {
            // Log offsets being committed with metadata for non-group consumers
            for (tp, offset_meta) in &offsets {
                let metadata_str = offset_meta.metadata.as_deref().unwrap_or("<none>");
                debug!(
                    "Committed offset for {}-{}: {} (metadata: {})",
                    tp.topic, tp.partition, offset_meta.offset, metadata_str
                );
            }

            // Update internal offset store
            let mut internal_offsets = self.offsets.write().await;
            for (tp, offset_meta) in offsets {
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
    pub async fn unsubscribe(&self) {
        let mut subscriptions = self.subscriptions.write().await;
        subscriptions.clear();

        let mut assignments = self.assignments.write().await;
        assignments.clear();

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
        debug!("Resumed partitions for {}: {:?}", topic, partitions);
    }

    /// Get the set of paused partitions.
    pub async fn paused_partitions(&self) -> HashSet<(String, PartitionId)> {
        self.paused.read().await.clone()
    }

    /// Close the consumer.
    pub async fn close(&self) {
        self.closed.store(true, std::sync::atomic::Ordering::SeqCst);

        // Leave consumer group if we have a group coordinator
        if let Some(ref coordinator) = self.group_coordinator
            && let Err(e) = coordinator.leave_group().await
        {
            warn!("Error leaving consumer group: {e}");
        }

        self.pool.close_all().await;
        info!("Consumer closed");
    }

    /// Check if the consumer is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Get the consumer group, if one is configured.
    pub fn group(&self) -> Option<&Arc<ConsumerGroup>> {
        self.group.as_ref()
    }

    /// Get the group coordinator, if one is configured.
    pub fn group_coordinator(&self) -> Option<&Arc<GroupCoordinator>> {
        self.group_coordinator.as_ref()
    }
}

/// Builder for creating consumers.
#[must_use = "builders do nothing until .build() is called"]
#[derive(Default)]
pub struct ConsumerBuilder {
    config: ConsumerConfig,
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

    /// Build the consumer.
    pub async fn build(self) -> Result<Consumer> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap.servers is required"));
        }
        Consumer::new(self.config).await
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
}
