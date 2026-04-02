//! Cluster metadata management.
//!
//! This module handles:
//! - Fetching and caching cluster metadata
//! - Topic and partition information
//! - Broker discovery
//! - Leader election tracking

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, RwLock};
use tracing::{debug, warn};

use crate::error::{KrafkaError, Result};
use crate::network::{BrokerConnection, ConnectionPool};
use crate::protocol::{
    ApiKey, MetadataRequest, MetadataResponse, VersionedDecode, VersionedEncode,
};
use crate::{BrokerId, PartitionId};

/// Information about a broker.
#[must_use]
#[derive(Debug, Clone)]
pub struct BrokerInfo {
    /// Broker ID.
    pub id: BrokerId,
    /// Broker host.
    pub host: String,
    /// Broker port.
    pub port: i32,
    /// Broker rack (optional).
    pub rack: Option<String>,
}

impl BrokerInfo {
    /// Get the broker address as host:port.
    pub fn address(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }
}

/// Information about a topic partition.
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
    /// Topics by name.
    topics: HashMap<String, TopicInfo>,
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
    /// Cached metadata.
    cache: RwLock<MetadataCache>,
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
            cache: RwLock::new(MetadataCache::new()),
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
    /// The Metadata API version is negotiated with the broker:
    /// - Preferred: v7+ (cluster_id, broker rack, leader epoch, offline replicas)
    /// - Fallback: v0 (basic broker + topic metadata)
    pub async fn refresh_for_topics(&self, topics: Option<&[&str]>) -> Result<()> {
        // Coalesce concurrent calls: only one refresh in-flight at a time
        let _guard = self.refresh_lock.lock().await;

        // After acquiring the lock, check if metadata was just refreshed by another caller.
        // If it was refreshed within the last 100ms, skip the redundant request.
        {
            let cache = self.cache.read().await;
            if cache.last_updated.elapsed() < Duration::from_millis(100)
                && !cache.brokers.is_empty()
            {
                debug!("Metadata was recently refreshed, skipping redundant request");
                return Ok(());
            }
        }

        // Get a connection
        let conn = self.get_any_connection().await?;

        // Negotiate the highest mutually supported version (v0-v8, no gaps).
        // v7+ gives us leader_epoch, broker rack, and offline replicas.
        let metadata_version = conn
            .negotiate_api_version_max(ApiKey::Metadata, crate::protocol::versions::METADATA_MAX)
            .await
            .unwrap_or_else(|| {
                debug!("Broker does not advertise Metadata support, falling back to v0");
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

        // Update cache
        self.update_cache(metadata).await;

        Ok(())
    }

    /// Get a connection to any available broker.
    async fn get_any_connection(&self) -> Result<Arc<BrokerConnection>> {
        // Try to use a cached broker first
        {
            let cache = self.cache.read().await;
            for broker in cache.brokers.values() {
                if let Ok(conn) = self.pool.get_connection(&broker.address()).await {
                    return Ok(conn);
                }
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
    async fn update_cache(&self, response: MetadataResponse) {
        let mut cache = self.cache.write().await;

        cache.cluster_id = response.cluster_id;
        cache.controller_id = response.controller_id;

        // Update brokers
        cache.brokers.clear();
        for broker in response.brokers {
            cache.brokers.insert(
                broker.node_id,
                BrokerInfo {
                    id: broker.node_id,
                    host: broker.host,
                    port: broker.port,
                    rack: broker.rack,
                },
            );
        }

        // Update topics — clear stale entries first so deleted topics
        // don't persist in cache. Topics present in the response
        // with errors (e.g. UnknownTopicOrPartition for deleted topics) are
        // explicitly removed.
        let response_topic_names: HashSet<String> = response
            .topics
            .iter()
            .filter_map(|t| t.name.clone())
            .collect();

        // Remove topics that were requested but came back with errors
        // (they may have been deleted)
        cache
            .topics
            .retain(|name, _| !response_topic_names.contains(name));

        for topic in response.topics {
            if !topic.error_code.is_ok() {
                warn!(
                    "Topic {} has error: {:?}",
                    topic.name.as_deref().unwrap_or("unknown"),
                    topic.error_code
                );
                // Don't insert — the retain above already removed the stale entry
                continue;
            }

            let topic_name = match &topic.name {
                Some(name) => name.clone(),
                None => continue,
            };

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

            cache.topics.insert(
                topic_name.clone(),
                TopicInfo {
                    name: topic_name,
                    is_internal: topic.is_internal,
                    partitions,
                },
            );
        }

        cache.last_updated = Instant::now();
        debug!(
            "Updated metadata: {} brokers, {} topics",
            cache.brokers.len(),
            cache.topics.len()
        );
    }

    /// Get broker info by ID.
    pub async fn broker(&self, broker_id: BrokerId) -> Option<BrokerInfo> {
        let cache = self.cache.read().await;
        cache.brokers.get(&broker_id).cloned()
    }

    /// Get all brokers.
    pub async fn brokers(&self) -> Vec<BrokerInfo> {
        let cache = self.cache.read().await;
        cache.brokers.values().cloned().collect()
    }

    /// Get topic info by name.
    pub async fn topic(&self, name: &str) -> Option<TopicInfo> {
        let cache = self.cache.read().await;
        cache.topics.get(name).cloned()
    }

    /// Get all topics.
    pub async fn topics(&self) -> Vec<TopicInfo> {
        let cache = self.cache.read().await;
        cache.topics.values().cloned().collect()
    }

    /// Get the leader for a topic partition.
    pub async fn leader(&self, topic: &str, partition: PartitionId) -> Option<BrokerId> {
        let cache = self.cache.read().await;
        cache.topics.get(topic).and_then(|t| t.leader(partition))
    }

    /// Get the leader epoch for a topic partition.
    ///
    /// The leader epoch is used for fencing stale reads after leadership changes.
    /// Returns None if the topic/partition is not found in metadata.
    pub async fn leader_epoch(&self, topic: &str, partition: PartitionId) -> Option<i32> {
        let cache = self.cache.read().await;
        cache
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
        // Check if we need to refresh metadata
        {
            let cache = self.cache.read().await;
            if cache.is_stale(self.max_age) || !cache.topics.contains_key(topic) {
                drop(cache);
                self.refresh_for_topics(Some(&[topic])).await?;
            }
        }

        let (broker_id, broker_addr) = {
            let cache = self.cache.read().await;

            let leader_id = cache
                .topics
                .get(topic)
                .and_then(|t| t.leader(partition))
                .ok_or_else(|| {
                    KrafkaError::invalid_state(format!("no leader for {}-{}", topic, partition))
                })?;

            let broker = cache.brokers.get(&leader_id).ok_or_else(|| {
                KrafkaError::invalid_state(format!("broker {} not found", leader_id))
            })?;

            (leader_id, broker.address())
        };

        self.pool
            .get_connection_by_id(broker_id, &broker_addr)
            .await
    }

    /// Get the controller broker.
    pub async fn controller(&self) -> Option<BrokerInfo> {
        let cache = self.cache.read().await;
        cache.brokers.get(&cache.controller_id).cloned()
    }

    /// Get the cluster ID.
    pub async fn cluster_id(&self) -> Option<String> {
        let cache = self.cache.read().await;
        cache.cluster_id.clone()
    }

    /// Check if metadata needs refresh.
    pub async fn needs_refresh(&self) -> bool {
        let cache = self.cache.read().await;
        cache.is_stale(self.max_age)
    }

    /// Get partition count for a topic.
    pub async fn partition_count(&self, topic: &str) -> Option<usize> {
        let cache = self.cache.read().await;
        cache.topics.get(topic).map(|t| t.partition_count())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_broker_info_address() {
        let broker = BrokerInfo {
            id: 1,
            host: "localhost".to_string(),
            port: 9092,
            rack: None,
        };
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
        let broker = BrokerInfo {
            id: 1,
            host: "broker1.kafka.local".to_string(),
            port: 9093,
            rack: Some("us-east-1a".to_string()),
        };
        assert_eq!(broker.address(), "broker1.kafka.local:9093");
        assert_eq!(broker.rack.as_deref(), Some("us-east-1a"));
    }
}
