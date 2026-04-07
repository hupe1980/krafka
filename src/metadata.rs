//! Cluster metadata management.
//!
//! This module handles:
//! - Fetching and caching cluster metadata
//! - Topic and partition information
//! - Broker discovery
//! - Leader election tracking

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use tokio::sync::Mutex;
use tracing::{debug, warn};

use crate::error::{KrafkaError, Result};
use crate::network::{BrokerConnection, ConnectionPool};
use crate::protocol::{
    ApiKey, MetadataRequest, MetadataResponse, VersionedDecode, VersionedEncode,
};
use crate::{BrokerId, PartitionId};

/// Information about a broker.
#[non_exhaustive]
#[must_use]
#[derive(Debug, Clone)]
pub struct BrokerInfo {
    /// Broker ID.
    pub id: BrokerId,
    /// Broker host.
    host: String,
    /// Broker port.
    port: i32,
    /// Broker rack (optional).
    rack: Option<String>,
    /// Cached `host:port` address string.
    address: String,
}

impl BrokerInfo {
    /// Create a new `BrokerInfo`.
    pub fn new(id: BrokerId, host: String, port: i32, rack: Option<String>) -> Self {
        let address = format!("{host}:{port}");
        Self {
            id,
            host,
            port,
            rack,
            address,
        }
    }

    /// Get the broker host.
    #[inline]
    pub fn host(&self) -> &str {
        &self.host
    }

    /// Get the broker port.
    #[inline]
    pub fn port(&self) -> i32 {
        self.port
    }

    /// Get the broker rack, if any.
    #[inline]
    pub fn rack(&self) -> Option<&str> {
        self.rack.as_deref()
    }

    /// Get the broker address as `host:port`.
    #[inline]
    pub fn address(&self) -> &str {
        &self.address
    }
}

/// Information about a topic partition.
#[non_exhaustive]
#[must_use]
#[derive(Debug, Clone)]
pub struct PartitionInfo {
    /// Topic name.
    pub topic: String,
    /// Partition ID.
    pub partition: PartitionId,
    /// Leader broker ID.
    pub leader: BrokerId,
    /// Leader epoch.
    pub leader_epoch: i32,
    /// Replica broker IDs.
    pub replicas: Vec<BrokerId>,
    /// In-sync replica broker IDs.
    pub isr: Vec<BrokerId>,
    /// Offline replica broker IDs.
    pub offline_replicas: Vec<BrokerId>,
}

/// Information about a topic.
#[non_exhaustive]
#[must_use]
#[derive(Debug, Clone)]
pub struct TopicInfo {
    /// Topic name.
    pub name: String,
    /// Whether the topic is internal.
    pub is_internal: bool,
    /// Partition information.
    pub partitions: Vec<PartitionInfo>,
}

impl TopicInfo {
    /// Get the number of partitions.
    #[inline]
    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    /// Get partition info by ID.
    #[inline]
    pub fn partition(&self, partition_id: PartitionId) -> Option<&PartitionInfo> {
        self.partitions.iter().find(|p| p.partition == partition_id)
    }

    /// Get the leader for a partition.
    #[inline]
    pub fn leader(&self, partition_id: PartitionId) -> Option<BrokerId> {
        self.partition(partition_id).map(|p| p.leader)
    }

    /// Get the leader epoch for a partition.
    #[inline]
    pub fn leader_epoch(&self, partition_id: PartitionId) -> Option<i32> {
        self.partition(partition_id).map(|p| p.leader_epoch)
    }
}

/// Cached cluster metadata.
#[derive(Debug, Clone)]
struct MetadataCache {
    /// Cluster ID.
    cluster_id: Option<String>,
    /// Controller broker ID.
    controller_id: BrokerId,
    /// Brokers by ID.
    brokers: HashMap<BrokerId, BrokerInfo>,
    /// Topics by name. Wrapped in `Arc` so that partial-refresh clones of
    /// the map are O(n) ref-count bumps instead of O(n) deep copies.
    topics: HashMap<String, Arc<TopicInfo>>,
    /// Topic UUID → topic name map. Topic names are wrapped in `Arc` so that
    /// partial-refresh clones of the map are O(n) ref-count bumps instead of
    /// O(n) deep copies. Populated from metadata v10+ responses where each
    /// topic includes a 16-byte topic_id. Used by the KIP-848 consumer
    /// protocol to resolve topic UUIDs in assignments.
    topic_ids: HashMap<[u8; 16], Arc<String>>,
    /// When the metadata was last updated.
    last_updated: Instant,
}

impl MetadataCache {
    fn new() -> Self {
        Self {
            cluster_id: None,
            controller_id: -1,
            brokers: HashMap::new(),
            topics: HashMap::new(),
            topic_ids: HashMap::new(),
            last_updated: Instant::now(),
        }
    }

    fn is_stale(&self, max_age: Duration) -> bool {
        self.last_updated.elapsed() > max_age
    }
}

/// Cluster metadata manager.
pub struct ClusterMetadata {
    /// Bootstrap servers.
    bootstrap_servers: Vec<String>,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Cached metadata (lock-free reads via `ArcSwap`).
    cache: ArcSwap<MetadataCache>,
    /// Metadata max age before refresh.
    max_age: Duration,
    /// Coalescing lock: prevents concurrent metadata refreshes.
    /// Multiple callers wait on the same in-flight refresh instead of stampeding.
    refresh_lock: Mutex<()>,
}

impl ClusterMetadata {
    /// Create a new cluster metadata manager.
    pub fn new(
        bootstrap_servers: Vec<String>,
        pool: Arc<ConnectionPool>,
        max_age: Duration,
    ) -> Self {
        Self {
            bootstrap_servers,
            pool,
            cache: ArcSwap::from_pointee(MetadataCache::new()),
            max_age,
            refresh_lock: Mutex::new(()),
        }
    }

    /// Get the bootstrap servers.
    pub fn bootstrap_servers(&self) -> &[String] {
        &self.bootstrap_servers
    }

    /// Refresh metadata from the cluster.
    pub async fn refresh(&self) -> Result<()> {
        self.refresh_for_topics(None).await
    }

    /// Refresh metadata for specific topics.
    ///
    /// Uses a coalescing lock to prevent concurrent metadata stampedes.
    /// If a refresh is already in-flight, callers wait for it to complete.
    ///
    /// The Metadata API version is negotiated with the broker (v0–v8).
    /// Versions are cumulative: rack v1, cluster_id v2, offline replicas v5,
    /// leader_epoch v7, authorized-ops v8.
    /// Encode/decode for v9–v13 (flexible encoding v9, topic UUIDs v10) exists
    /// but is not yet activated — see `METADATA_MAX`.
    /// Falls back to v0 if the broker doesn’t advertise Metadata support.
    pub async fn refresh_for_topics(&self, topics: Option<&[&str]>) -> Result<()> {
        // Coalesce concurrent calls: only one refresh in-flight at a time
        let _guard = self.refresh_lock.lock().await;

        // After acquiring the lock, check if metadata was just refreshed by another caller.
        // If it was refreshed within the last 100ms, skip the redundant request — but only
        // for partial refreshes where all requested topics are already present. Full refreshes
        // are never skipped: a recent partial refresh does not guarantee a full-cluster snapshot.
        let cache = self.cache.load();
        if cache.last_updated.elapsed() < Duration::from_millis(100) && !cache.brokers.is_empty() {
            let all_present = match topics {
                None => false,
                Some(names) => names.iter().all(|name| cache.topics.contains_key(*name)),
            };
            if all_present {
                debug!("Metadata was recently refreshed, skipping redundant request");
                return Ok(());
            }
        }

        // Get a connection
        let conn = self.get_any_connection().await?;

        // Negotiate the highest mutually supported version (v0-v12, no gaps).
        // Cumulative: rack v1, cluster_id v2, offline replicas v5, leader_epoch v7,
        // authorized-ops v8, flexible encoding v9, topic UUIDs v10.
        // Falls back to v0 if the broker doesn't advertise Metadata support
        // (mirrors the Fetch negotiation pattern in consumer).
        let metadata_version = conn
            .negotiate_api_version_max(ApiKey::Metadata, crate::protocol::versions::METADATA_MAX)
            .await
            .unwrap_or_else(|| {
                debug!("Metadata API version negotiation unavailable; falling back to v0");
                0
            });

        // Build and send metadata request
        let request = match topics {
            Some(t) => MetadataRequest::for_topics(t.to_vec()),
            None => MetadataRequest::all_topics(),
        };

        let response = conn
            .send_request(ApiKey::Metadata, metadata_version, |buf| {
                request.encode_versioned(metadata_version, buf)
            })
            .await?;

        // Decode response
        let mut buf = response;
        let metadata = MetadataResponse::decode_versioned(metadata_version, &mut buf)?;

        // Update cache. A full refresh (topics=None) is authoritative — the
        // response contains every topic currently in the cluster, so we rebuild
        // from scratch. A partial refresh delta-merges into the existing cache.
        let full_refresh = topics.is_none();
        self.update_cache(metadata, full_refresh);

        Ok(())
    }

    /// Get a connection to any available broker.
    async fn get_any_connection(&self) -> Result<Arc<BrokerConnection>> {
        // Try to use a cached broker first
        let cache = self.cache.load();
        for broker in cache.brokers.values() {
            if let Ok(conn) = self.pool.get_connection(broker.address()).await {
                return Ok(conn);
            }
        }

        // Fall back to bootstrap servers
        for server in &self.bootstrap_servers {
            if let Ok(conn) = self.pool.get_connection(server).await {
                return Ok(conn);
            }
        }

        Err(KrafkaError::invalid_state(
            "no available brokers to connect to",
        ))
    }

    /// Update the metadata cache from a response.
    ///
    /// Builds a new snapshot and swaps it in atomically via `ArcSwap`.
    ///
    /// When `full_refresh` is true the response is authoritative (all topics in
    /// the cluster), so the broker and topic maps are rebuilt from scratch.
    /// When false (partial/topic-specific refresh), the response is delta-merged
    /// into the existing cache so that topics not in the request are preserved
    /// and broker entries referenced by preserved topics remain available.
    fn update_cache(&self, response: MetadataResponse, full_refresh: bool) {
        let old = self.cache.load();

        // Full refresh: response is authoritative — start empty.
        // Partial refresh: merge into the existing broker map so preserved
        // topics cannot end up referencing brokers missing from the cache.
        let mut brokers = if full_refresh {
            HashMap::new()
        } else {
            old.brokers.clone()
        };
        for broker in response.brokers {
            brokers.insert(
                broker.node_id,
                BrokerInfo::new(broker.node_id, broker.host, broker.port, broker.rack),
            );
        }

        // Full refresh: response is authoritative — start empty.
        // Partial refresh: delta-merge into existing topics and topic_ids.
        let mut topics = if full_refresh {
            HashMap::new()
        } else {
            old.topics.clone()
        };
        let mut topic_ids = if full_refresh {
            HashMap::new()
        } else {
            old.topic_ids.clone()
        };

        for topic in response.topics {
            let Some(topic_name) = topic.name else {
                continue;
            };

            if !topic.error_code.is_ok() {
                warn!("Topic {} has error: {:?}", topic_name, topic.error_code);
                // Remove from both maps on error (topic may have been deleted).
                if let Some(tid) = topic.topic_id {
                    topic_ids.remove(&tid);
                }
                topics.remove(&topic_name);
                continue;
            }

            // Track topic UUID → name mapping (v10+).
            if let Some(tid) = topic.topic_id {
                topic_ids.insert(tid, Arc::new(topic_name.clone()));
            }

            let partitions: Vec<PartitionInfo> = topic
                .partitions
                .into_iter()
                .filter(|p| p.error_code.is_ok())
                .map(|p| PartitionInfo {
                    topic: topic_name.clone(),
                    partition: p.partition_index,
                    leader: p.leader_id,
                    leader_epoch: p.leader_epoch,
                    replicas: p.replica_nodes,
                    isr: p.isr_nodes,
                    offline_replicas: p.offline_replicas,
                })
                .collect();

            topics.insert(
                topic_name.clone(),
                Arc::new(TopicInfo {
                    name: topic_name,
                    is_internal: topic.is_internal,
                    partitions,
                }),
            );
        }

        let new_cache = MetadataCache {
            cluster_id: response.cluster_id,
            controller_id: response.controller_id,
            brokers,
            topics,
            topic_ids,
            last_updated: Instant::now(),
        };

        debug!(
            "Updated metadata: {} brokers, {} topics",
            new_cache.brokers.len(),
            new_cache.topics.len()
        );

        self.cache.store(Arc::new(new_cache));
    }

    /// Get broker info by ID.
    pub fn broker(&self, broker_id: BrokerId) -> Option<BrokerInfo> {
        self.cache.load().brokers.get(&broker_id).cloned()
    }

    /// Get all brokers.
    pub fn brokers(&self) -> Vec<BrokerInfo> {
        self.cache.load().brokers.values().cloned().collect()
    }

    /// Get topic info by name.
    pub fn topic(&self, name: &str) -> Option<TopicInfo> {
        self.cache
            .load()
            .topics
            .get(name)
            .map(|t| t.as_ref().clone())
    }

    /// Resolve a 16-byte topic UUID to a topic name.
    ///
    /// The mapping is populated from metadata v10+ responses where each topic
    /// includes a `topic_id`. Returns `None` if the UUID is unknown — the
    /// caller should trigger a metadata refresh and retry.
    pub fn topic_name_for_id(&self, topic_id: &[u8; 16]) -> Option<String> {
        self.cache
            .load()
            .topic_ids
            .get(topic_id)
            .map(|name| (**name).clone())
    }

    /// Get all topics.
    pub fn topics(&self) -> Vec<TopicInfo> {
        self.cache
            .load()
            .topics
            .values()
            .map(|t| t.as_ref().clone())
            .collect()
    }

    /// Get the leader for a topic partition.
    pub fn leader(&self, topic: &str, partition: PartitionId) -> Option<BrokerId> {
        self.cache
            .load()
            .topics
            .get(topic)
            .and_then(|t| t.leader(partition))
    }

    /// Get the leader epoch for a topic partition.
    ///
    /// The leader epoch is used for fencing stale reads after leadership changes.
    /// Returns None if the topic/partition is not found in metadata.
    pub fn leader_epoch(&self, topic: &str, partition: PartitionId) -> Option<i32> {
        self.cache
            .load()
            .topics
            .get(topic)
            .and_then(|t| t.leader_epoch(partition))
    }

    /// Get a connection to the leader of a partition.
    pub async fn get_leader_connection(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<Arc<BrokerConnection>> {
        // Refresh if stale or topic is unknown, then re-load the updated snapshot.
        // Otherwise reuse the snapshot we already have.
        let cache = self.cache.load();
        let cache = if cache.is_stale(self.max_age) || !cache.topics.contains_key(topic) {
            drop(cache);
            self.refresh_for_topics(Some(&[topic])).await?;
            self.cache.load()
        } else {
            cache
        };

        let leader_id = cache
            .topics
            .get(topic)
            .and_then(|t| t.leader(partition))
            .ok_or_else(|| {
                KrafkaError::invalid_state(format!("no leader for {topic}-{partition}"))
            })?;

        let broker = cache
            .brokers
            .get(&leader_id)
            .ok_or_else(|| KrafkaError::invalid_state(format!("broker {} not found", leader_id)))?;

        self.pool
            .get_connection_by_id(leader_id, broker.address())
            .await
    }

    /// Get a connection to a specific broker by ID.
    pub async fn get_broker_connection(
        &self,
        broker_id: BrokerId,
    ) -> Result<Arc<BrokerConnection>> {
        let cache = self.cache.load();
        let broker = cache
            .brokers
            .get(&broker_id)
            .ok_or_else(|| KrafkaError::invalid_state(format!("broker {} not found", broker_id)))?;

        self.pool
            .get_connection_by_id(broker_id, broker.address())
            .await
    }

    /// Get the controller broker.
    pub fn controller(&self) -> Option<BrokerInfo> {
        let cache = self.cache.load();
        cache.brokers.get(&cache.controller_id).cloned()
    }

    /// Get the cluster ID.
    pub fn cluster_id(&self) -> Option<String> {
        self.cache.load().cluster_id.clone()
    }

    /// Check if metadata needs refresh.
    pub fn needs_refresh(&self) -> bool {
        self.cache.load().is_stale(self.max_age)
    }

    /// Get partition count for a topic.
    pub fn partition_count(&self, topic: &str) -> Option<usize> {
        self.cache
            .load()
            .topics
            .get(topic)
            .map(|t| t.partition_count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_info_address() {
        let broker = BrokerInfo::new(1, "localhost".to_string(), 9092, None);
        assert_eq!(broker.address(), "localhost:9092");
    }

    #[test]
    fn test_topic_info() {
        let topic = TopicInfo {
            name: "test".to_string(),
            is_internal: false,
            partitions: vec![
                PartitionInfo {
                    topic: "test".to_string(),
                    partition: 0,
                    leader: 1,
                    leader_epoch: 0,
                    replicas: vec![1, 2, 3],
                    isr: vec![1, 2, 3],
                    offline_replicas: vec![],
                },
                PartitionInfo {
                    topic: "test".to_string(),
                    partition: 1,
                    leader: 2,
                    leader_epoch: 0,
                    replicas: vec![2, 3, 1],
                    isr: vec![2, 3, 1],
                    offline_replicas: vec![],
                },
            ],
        };

        assert_eq!(topic.partition_count(), 2);
        assert_eq!(topic.leader(0), Some(1));
        assert_eq!(topic.leader(1), Some(2));
        assert_eq!(topic.leader(2), None);
    }

    #[test]
    fn test_metadata_cache_stale() {
        let cache = MetadataCache::new();
        assert!(!cache.is_stale(Duration::from_secs(60)));

        // Note: We can't easily test staleness without mocking time
    }

    #[test]
    fn test_metadata_cache_new_is_empty() {
        let cache = MetadataCache::new();
        assert!(cache.brokers.is_empty());
        assert!(cache.topics.is_empty());
        assert!(cache.cluster_id.is_none());
        assert_eq!(cache.controller_id, -1);
    }

    #[test]
    fn test_broker_info_with_rack() {
        let broker = BrokerInfo::new(
            1,
            "broker1.kafka.local".to_string(),
            9093,
            Some("us-east-1a".to_string()),
        );
        assert_eq!(broker.address(), "broker1.kafka.local:9093");
        assert_eq!(broker.rack(), Some("us-east-1a"));
    }

    #[test]
    fn test_metadata_cache_topic_ids() {
        let mut cache = MetadataCache::new();
        assert!(cache.topic_ids.is_empty());

        let uuid: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        cache
            .topic_ids
            .insert(uuid, Arc::new("my-topic".to_string()));
        assert_eq!(
            cache.topic_ids.get(&uuid),
            Some(&Arc::new("my-topic".to_string()))
        );
    }

    #[test]
    fn test_metadata_cache_new_has_empty_topic_ids() {
        let cache = MetadataCache::new();
        assert!(cache.topic_ids.is_empty());
    }
}
