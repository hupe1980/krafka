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
use parking_lot::Mutex as SyncMutex;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::error::{ErrorCode, KrafkaError, Result};
use crate::network::{BrokerConnection, ConnectionPool};
use crate::protocol::{
    ApiKey, MetadataRequest, MetadataResponse, VersionedDecode, VersionedEncode,
};
use crate::{BrokerId, PartitionId};

/// Strategy for recovering when metadata refresh fails for too long.
///
/// Mirrors Java's `MetadataRecoveryStrategy` from KIP-899. When the client
/// cannot reach any broker in its current metadata for longer than
/// [`ClusterMetadata::with_rebootstrap_trigger`], it can automatically fall back
/// to the bootstrap servers to re-discover the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum MetadataRecoveryStrategy {
    /// No automatic recovery — behave like pre-KIP-899 clients.
    #[default]
    None,
    /// Reset to bootstrap servers and re-discover the cluster when metadata
    /// refresh has not succeeded within the configured trigger duration.
    Rebootstrap,
}

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
    /// Reverse index: topic name → topic UUID. Kept in sync with `topic_ids`
    /// for O(1) lookups.
    name_to_topic_id: HashMap<String, [u8; 16]>,
    /// Per-topic timestamp of the last refresh that included this topic.
    /// Used for TTL-based eviction during partial refreshes.
    topic_last_refreshed: HashMap<String, Instant>,
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
            name_to_topic_id: HashMap::new(),
            topic_last_refreshed: HashMap::new(),
            last_updated: Instant::now(),
        }
    }

    fn is_stale(&self, max_age: Duration) -> bool {
        self.last_updated.elapsed() > max_age
    }
}

/// Cluster metadata manager.
pub struct ClusterMetadata {
    /// Bootstrap servers (lock-free reads via `ArcSwap` for KIP-899 `update_seed_brokers`).
    bootstrap_servers: ArcSwap<Vec<String>>,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Cached metadata (lock-free reads via `ArcSwap`).
    cache: ArcSwap<MetadataCache>,
    /// Metadata max age before refresh.
    max_age: Duration,
    /// Coalescing lock: prevents concurrent metadata refreshes.
    /// Multiple callers wait on the same in-flight refresh instead of stampeding.
    refresh_lock: Mutex<()>,
    /// Recovery strategy when metadata refresh fails for too long (KIP-899).
    recovery_strategy: MetadataRecoveryStrategy,
    /// Duration after which a failing metadata refresh triggers a rebootstrap
    /// (only when `recovery_strategy` is [`MetadataRecoveryStrategy::Rebootstrap`]).
    /// Default: 300 s (5 minutes), matching the Java client.
    rebootstrap_trigger: Duration,
    /// Instant when the current streak of metadata-refresh failures started.
    /// Reset to `None` on every successful refresh. After a rebootstrap
    /// it is set to the *current* instant (matching Java) so the next cycle
    /// starts timing immediately.
    metadata_attempt_start: SyncMutex<Option<Instant>>,
    /// Maximum age of a cached topic entry before it is evicted during partial
    /// refresh. Defaults to 5 minutes, matching the Java client's
    /// `metadata.max.idle.ms`. `None` disables TTL eviction. When set, topics
    /// not refreshed within this duration are pruned on the next partial
    /// refresh, preventing unbounded cache growth from topic churn.
    topic_cache_ttl: Option<Duration>,
}

impl ClusterMetadata {
    /// Create a new cluster metadata manager.
    pub fn new(
        bootstrap_servers: Vec<String>,
        pool: Arc<ConnectionPool>,
        max_age: Duration,
    ) -> Self {
        Self {
            bootstrap_servers: ArcSwap::from_pointee(bootstrap_servers),
            pool,
            cache: ArcSwap::from_pointee(MetadataCache::new()),
            max_age,
            refresh_lock: Mutex::new(()),
            recovery_strategy: MetadataRecoveryStrategy::None,
            rebootstrap_trigger: Duration::from_secs(300),
            metadata_attempt_start: SyncMutex::new(None),
            // Default to 5 minutes, matching Java's `metadata.max.idle.ms`.
            // Prevents unbounded cache growth on topic churn; callers that
            // want the old unbounded behaviour can opt out via
            // `with_topic_cache_ttl_disabled()`.
            topic_cache_ttl: Some(Duration::from_secs(300)),
        }
    }

    /// Set the metadata recovery strategy (KIP-899).
    ///
    /// When set to [`MetadataRecoveryStrategy::Rebootstrap`], the client will
    /// automatically close all connections and fall back to bootstrap servers
    /// when metadata refresh has not succeeded within
    /// [`rebootstrap_trigger`](Self::with_rebootstrap_trigger).
    #[must_use]
    pub fn with_recovery_strategy(mut self, strategy: MetadataRecoveryStrategy) -> Self {
        self.recovery_strategy = strategy;
        self
    }

    /// Set the duration after which failed metadata refreshes trigger a
    /// rebootstrap (KIP-899). Only effective when recovery strategy is
    /// [`MetadataRecoveryStrategy::Rebootstrap`]. Default: 300 s.
    #[must_use]
    pub fn with_rebootstrap_trigger(mut self, duration: Duration) -> Self {
        self.rebootstrap_trigger = duration;
        self
    }

    /// Set the topic cache TTL for partial refreshes.
    ///
    /// During partial refreshes, cached topics that have not been refreshed
    /// within this duration are evicted to prevent unbounded cache growth.
    /// Full refreshes always rebuild from scratch regardless of this setting.
    ///
    /// Default: 5 minutes (matching Java's `metadata.max.idle.ms`).
    #[must_use]
    pub fn with_topic_cache_ttl(mut self, ttl: Duration) -> Self {
        self.topic_cache_ttl = Some(ttl);
        self
    }

    /// Disable topic cache TTL eviction.
    ///
    /// Partial refreshes will retain cached topic entries indefinitely.
    /// Prefer the default TTL for long-lived clients that discover topics
    /// dynamically (CDC, multi-tenant gateways); disabling TTL eviction can
    /// cause unbounded cache growth on topic churn.
    #[must_use]
    pub fn with_topic_cache_ttl_disabled(mut self) -> Self {
        self.topic_cache_ttl = None;
        self
    }

    /// Get the bootstrap servers.
    pub fn bootstrap_servers(&self) -> Vec<String> {
        (**self.bootstrap_servers.load()).clone()
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
    /// The Metadata API version is negotiated with the broker (v1–v13).
    /// Versions are cumulative: rack v1, cluster_id v2, offline replicas v5,
    /// leader_epoch v7, authorized-ops v8, flexible encoding v9, topic UUIDs v10,
    /// cluster_authorized_operations removed v11, topic_id works v12,
    /// top-level error_code v13.
    /// Falls back to METADATA_MIN (v1) if the broker doesn't advertise higher
    /// Metadata support.
    ///
    /// When [`MetadataRecoveryStrategy::Rebootstrap`] is configured and no
    /// broker is reachable for longer than [`rebootstrap_trigger`](Self::with_rebootstrap_trigger),
    /// all connections are closed and the client falls back to bootstrap
    /// servers (KIP-899).
    pub async fn refresh_for_topics(&self, topics: Option<&[&str]>) -> Result<()> {
        // Coalesce concurrent calls: only one refresh in-flight at a time
        let _guard = self.refresh_lock.lock().await;

        // After acquiring the lock, check if the requested data is already fresh.
        //
        // For partial refreshes: skip if every requested topic is present in the
        // cache and was refreshed within `max_age`. This deduplicates work when
        // multiple callers ask for overlapping topic sets — the second caller
        // finds the first caller's result still fresh and returns immediately.
        //
        // Full refreshes (`topics=None`) are never skipped: a recent partial
        // refresh does not guarantee a full-cluster snapshot.
        let cache = self.cache.load();
        if !cache.brokers.is_empty() {
            let all_fresh = match topics {
                None => false,
                Some(names) => names.iter().all(|name| {
                    cache.topics.contains_key(*name)
                        && cache
                            .topic_last_refreshed
                            .get(*name)
                            .is_some_and(|ts| ts.elapsed() <= self.max_age)
                }),
            };
            if all_fresh {
                debug!("All requested topics are fresh in cache, skipping redundant request");
                return Ok(());
            }
        }

        // Record the start of this refresh attempt for KIP-899 failure tracking.
        // If there is already a recorded start (from a previous failing attempt),
        // keep it — we only care about how long the *streak* has lasted.
        {
            let mut start = self.metadata_attempt_start.lock();
            start.get_or_insert_with(Instant::now);
        }

        // Allow at most one rebootstrap retry per refresh call.
        let mut rebootstrapped = false;

        loop {
            // Get a connection — on failure, check if rebootstrap is needed.
            let conn = match self.get_any_connection().await {
                Ok(conn) => conn,
                Err(e) => {
                    if !rebootstrapped && self.needs_rebootstrap() {
                        self.rebootstrap().await;
                        rebootstrapped = true;
                        // Retry once after rebootstrap.
                        self.get_any_connection().await?
                    } else {
                        return Err(e);
                    }
                }
            };

            // Negotiate the highest mutually supported Metadata version up to the
            // client's supported maximum (`METADATA_MAX`).
            // v1+ required, up to v13 (top-level error_code).
            let metadata_version = conn
                .negotiate_api_version(
                    ApiKey::Metadata,
                    crate::protocol::versions::METADATA_MAX,
                    crate::protocol::versions::METADATA_MIN,
                )
                .await
                .unwrap_or_else(|| {
                    debug!("Metadata API version negotiation unavailable; falling back to MIN");
                    crate::protocol::versions::METADATA_MIN
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

            // v13+ includes a top-level error code. Check it before processing
            // topics. Per-topic errors are still handled individually in update_cache.
            if metadata.error_code == ErrorCode::RebootstrapRequired {
                if rebootstrapped {
                    // Already retried once — don't loop forever.
                    return Err(KrafkaError::broker(
                        metadata.error_code,
                        "server requested rebootstrap but retry also returned REBOOTSTRAP_REQUIRED",
                    ));
                }
                // Server-initiated rebootstrap (KIP-899): the cluster is telling
                // us to re-discover via bootstrap servers.
                info!("Server requested rebootstrap (REBOOTSTRAP_REQUIRED)");
                self.rebootstrap().await;
                rebootstrapped = true;
                continue;
            }
            if !metadata.error_code.is_ok() {
                return Err(KrafkaError::broker(
                    metadata.error_code,
                    "metadata request failed",
                ));
            }

            // Success — clear the failure-tracking timestamp only on full
            // refreshes. A partial refresh succeeding does not prove that all
            // brokers are reachable, so keep the rebootstrap timer running.

            // Update cache. A full refresh (topics=None) is authoritative — the
            // response contains every topic currently in the cluster, so we rebuild
            // from scratch. A partial refresh delta-merges into the existing cache.
            let full_refresh = topics.is_none();

            if full_refresh {
                let mut start = self.metadata_attempt_start.lock();
                *start = None;
            }

            self.update_cache(metadata, full_refresh);

            return Ok(());
        }
    }

    /// Replace the bootstrap server list at runtime (KIP-899).
    ///
    /// This does **not** trigger a rebootstrap or close existing connections.
    /// The new addresses are used on the next metadata refresh that falls back
    /// to bootstrap servers (e.g. after all cached brokers become unreachable).
    ///
    /// # Errors
    ///
    /// Returns an error if `servers` is empty.
    pub fn update_seed_brokers(&self, servers: Vec<String>) -> Result<()> {
        if servers.is_empty() {
            return Err(KrafkaError::config(
                "update_seed_brokers: at least one server required",
            ));
        }
        info!(count = servers.len(), "Updating seed brokers (KIP-899)");
        self.bootstrap_servers.store(Arc::new(servers));
        Ok(())
    }

    /// Force a rebootstrap: close all connections, clear the metadata cache,
    /// and fall back to bootstrap servers (KIP-899).
    ///
    /// The next call to [`refresh`](Self::refresh) or
    /// [`refresh_for_topics`](Self::refresh_for_topics) will re-discover the
    /// cluster from the bootstrap addresses.
    ///
    /// After rebootstrap, the failure-tracking timer is set to **now** (not
    /// cleared) so that the next refresh cycle starts timing immediately —
    /// matching the Java client's `metadataAttemptStartMs = Optional.of(now)`.
    pub async fn rebootstrap(&self) {
        info!("Rebootstrapping: closing all connections and resetting metadata (KIP-899)");

        // Close all pooled connections.
        self.pool.close_all().await;

        // Reset metadata cache to empty so `get_any_connection` goes straight
        // to bootstrap servers.
        self.cache.store(Arc::new(MetadataCache::new()));

        // Set the failure tracker to *now* (not None) so the next cycle starts
        // timing immediately — if the rebootstrap itself doesn't help, we'll
        // know how long it's been since we last rebootstrapped.
        {
            let mut start = self.metadata_attempt_start.lock();
            *start = Some(Instant::now());
        }
    }

    /// Check whether the rebootstrap trigger duration has elapsed.
    ///
    /// This is a pure predicate — it does **not** perform the rebootstrap.
    /// The caller is responsible for calling [`rebootstrap`](Self::rebootstrap)
    /// if this returns `true`.
    fn needs_rebootstrap(&self) -> bool {
        if self.recovery_strategy != MetadataRecoveryStrategy::Rebootstrap {
            return false;
        }

        let start = self.metadata_attempt_start.lock();
        let Some(attempt_start) = *start else {
            return false;
        };

        let elapsed = attempt_start.elapsed();
        if elapsed < self.rebootstrap_trigger {
            return false;
        }

        warn!(
            elapsed_ms = elapsed.as_millis(),
            trigger_ms = self.rebootstrap_trigger.as_millis(),
            "Metadata refresh failing too long, rebootstrap needed (KIP-899)"
        );

        true
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
        let servers = self.bootstrap_servers.load();
        for server in servers.iter() {
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
        let now = Instant::now();

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
        // Partial refresh: delta-merge into existing topics and topic_ids,
        // optionally evicting entries older than `topic_cache_ttl`.
        let mut topics = if full_refresh {
            HashMap::new()
        } else if let Some(ttl) = self.topic_cache_ttl {
            let retained: HashMap<_, _> = old
                .topics
                .iter()
                .filter(|(name, _)| {
                    old.topic_last_refreshed
                        .get(*name)
                        .is_some_and(|ts| now.duration_since(*ts) <= ttl)
                })
                .map(|(k, v)| (k.clone(), Arc::clone(v)))
                .collect();
            let evicted = old.topics.len().saturating_sub(retained.len());
            if evicted > 0 {
                debug!(
                    evicted,
                    ttl_secs = ttl.as_secs(),
                    "evicted stale topics from metadata cache"
                );
            }
            retained
        } else {
            old.topics.clone()
        };
        let mut topic_ids = if full_refresh {
            HashMap::new()
        } else if self.topic_cache_ttl.is_some() {
            // Keep only topic_ids whose names survived TTL eviction.
            old.topic_ids
                .iter()
                .filter(|(_, name)| topics.contains_key(name.as_str()))
                .map(|(k, v)| (*k, Arc::clone(v)))
                .collect()
        } else {
            old.topic_ids.clone()
        };

        // Build a reverse index (name → UUID) so we can remove the old UUID
        // for a topic name in O(1) instead of scanning the entire map.
        let mut name_to_uuid: HashMap<String, [u8; 16]> = topic_ids
            .iter()
            .map(|(uuid, name)| (name.as_ref().clone(), *uuid))
            .collect();

        // Track which topic names are actually provided by this response so
        // that only those entries get their `topic_last_refreshed` timestamp
        // advanced to `now`.  Retained-from-cache topics must keep their
        // original timestamps; resetting them would make them perpetually
        // "fresh" and defeat TTL eviction.
        let mut response_topic_names: Vec<String> = Vec::new();

        for topic in response.topics {
            let Some(topic_name) = topic.name else {
                continue;
            };

            if !topic.error_code.is_ok() {
                if topic.error_code.is_retriable() {
                    // Transient errors (LeaderNotAvailable, RequestTimedOut, etc.)
                    // — keep stale cache entry so callers don't see the topic as
                    // "unknown" until the next successful refresh.
                    debug!(
                        "Topic {} has transient error: {:?}, keeping stale cache entry",
                        topic_name, topic.error_code
                    );
                } else {
                    // Permanent errors (UnknownTopicOrPartition, TopicAuthorizationFailed,
                    // InvalidTopic, etc.) — remove from cache.
                    warn!("Topic {} has error: {:?}", topic_name, topic.error_code);
                    if let Some(tid) = topic.topic_id {
                        topic_ids.remove(&tid);
                    }
                    // Also remove any stale UUID → name mapping by name, in case
                    // the error response omitted topic_id or it was an all-zero UUID.
                    if let Some(old_uuid) = name_to_uuid.remove(&topic_name) {
                        topic_ids.remove(&old_uuid);
                    }
                    topics.remove(&topic_name);
                }
                continue;
            }

            // Track topic UUID → name mapping (v10+).
            // Remove any old UUID that previously mapped to this name first —
            // the topic may have been recreated with a new UUID.
            if let Some(tid) = topic.topic_id {
                if let Some(old_uuid) = name_to_uuid.remove(&topic_name) {
                    topic_ids.remove(&old_uuid);
                }
                let topic_arc = Arc::new(topic_name.clone());
                topic_ids.insert(tid, topic_arc);
                name_to_uuid.insert(topic_name.clone(), tid);
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

            response_topic_names.push(topic_name.clone());
            topics.insert(
                topic_name.clone(),
                Arc::new(TopicInfo {
                    name: topic_name,
                    is_internal: topic.is_internal,
                    partitions,
                }),
            );
        }

        // Build topic_last_refreshed:
        // - Full refresh: start empty; every topic comes from this response.
        // - Partial refresh with TTL: carry forward only entries that survived
        //   TTL eviction (with their *original* timestamps so their age is
        //   preserved); retained topics must NOT have their clock reset.
        // - Partial refresh without TTL: carry forward all existing entries.
        // In all cases, only topics that appear in the current response are
        // stamped with `now`; retained-from-cache topics keep their existing
        // timestamps so TTL eviction can fire correctly on the next refresh.
        let mut topic_last_refreshed = if full_refresh {
            HashMap::with_capacity(response_topic_names.len())
        } else if let Some(ttl) = self.topic_cache_ttl {
            old.topic_last_refreshed
                .iter()
                .filter(|(_name, ts)| now.duration_since(**ts) <= ttl)
                .map(|(k, v)| (k.clone(), *v))
                .collect()
        } else {
            old.topic_last_refreshed.clone()
        };
        // Stamp only topics included in this response with `now`.
        // For a full refresh `response_topic_names` covers all topics (the map
        // started empty).  For a partial refresh this correctly skips
        // retained-only entries, preserving their original timestamps.
        for name in response_topic_names {
            topic_last_refreshed.insert(name, now);
        }

        let new_cache = MetadataCache {
            cluster_id: response.cluster_id,
            controller_id: response.controller_id,
            brokers,
            topics,
            topic_ids,
            name_to_topic_id: name_to_uuid,
            topic_last_refreshed,
            last_updated: now,
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

    /// Resolve a topic name to its 16-byte UUID.
    ///
    /// The mapping is populated from metadata v10+ responses. Returns `None`
    /// if the topic is unknown or the broker did not return a topic ID — the
    /// caller should trigger a metadata refresh and retry.
    pub fn topic_id_for_name(&self, name: &str) -> Option<[u8; 16]> {
        self.cache.load().name_to_topic_id.get(name).copied()
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

    #[test]
    fn test_metadata_recovery_strategy_default() {
        assert_eq!(
            MetadataRecoveryStrategy::default(),
            MetadataRecoveryStrategy::None,
        );
    }

    #[test]
    fn test_cluster_metadata_with_recovery_strategy() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        )
        .with_recovery_strategy(MetadataRecoveryStrategy::Rebootstrap)
        .with_rebootstrap_trigger(Duration::from_secs(60));

        assert_eq!(
            meta.recovery_strategy,
            MetadataRecoveryStrategy::Rebootstrap
        );
        assert_eq!(meta.rebootstrap_trigger, Duration::from_secs(60));
    }

    #[test]
    fn test_update_seed_brokers() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["broker1:9092".to_string()],
            pool,
            Duration::from_secs(300),
        );

        assert_eq!(meta.bootstrap_servers(), vec!["broker1:9092"]);

        meta.update_seed_brokers(vec!["broker2:9092".to_string(), "broker3:9092".to_string()])
            .unwrap();
        assert_eq!(
            meta.bootstrap_servers(),
            vec!["broker2:9092", "broker3:9092"]
        );
    }

    #[test]
    fn test_update_seed_brokers_rejects_empty() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["broker1:9092".to_string()],
            pool,
            Duration::from_secs(300),
        );

        let result = meta.update_seed_brokers(vec![]);
        assert!(result.is_err());
        // Original servers unchanged.
        assert_eq!(meta.bootstrap_servers(), vec!["broker1:9092"]);
    }

    #[test]
    fn test_needs_rebootstrap_disabled_by_default() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        );

        // Default strategy is None — should never trigger rebootstrap.
        assert!(!meta.needs_rebootstrap());
    }

    #[test]
    fn test_needs_rebootstrap_not_yet_triggered() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        )
        .with_recovery_strategy(MetadataRecoveryStrategy::Rebootstrap)
        .with_rebootstrap_trigger(Duration::from_secs(300));

        // No attempt recorded yet — needs_rebootstrap should return false.
        assert!(!meta.needs_rebootstrap());

        // Simulate that a refresh attempt has started.
        {
            let mut start = meta.metadata_attempt_start.lock();
            *start = Some(Instant::now());
        }

        // Still shouldn't trigger — trigger is 300s, elapsed is ~0.
        assert!(!meta.needs_rebootstrap());
        // Timestamp should still be recorded.
        assert!(meta.metadata_attempt_start.lock().is_some());
    }

    #[tokio::test]
    async fn test_needs_rebootstrap_triggers_after_timeout() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        )
        .with_recovery_strategy(MetadataRecoveryStrategy::Rebootstrap)
        .with_rebootstrap_trigger(Duration::ZERO); // Zero trigger = immediate

        // Simulate that a refresh attempt has started.
        {
            let mut start = meta.metadata_attempt_start.lock();
            *start = Some(Instant::now());
        }

        // With a zero trigger, needs_rebootstrap should return true.
        assert!(meta.needs_rebootstrap());

        // Perform the actual rebootstrap.
        meta.rebootstrap().await;

        // After rebootstrap, the attempt start should be set to Some(now) — not None.
        assert!(meta.metadata_attempt_start.lock().is_some());
        // Cache should be reset.
        assert!(meta.cache.load().brokers.is_empty());
    }

    #[tokio::test]
    async fn test_rebootstrap_clears_cache() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        );

        // Manually inject some data into the cache.
        let mut cache = MetadataCache::new();
        cache
            .brokers
            .insert(1, BrokerInfo::new(1, "host".to_string(), 9092, None));
        meta.cache.store(Arc::new(cache));
        assert!(!meta.cache.load().brokers.is_empty());

        meta.rebootstrap().await;

        assert!(meta.cache.load().brokers.is_empty());
        // After rebootstrap, timer is set to Some(now) — not cleared.
        assert!(meta.metadata_attempt_start.lock().is_some());
    }

    #[test]
    fn test_topic_cache_ttl_default_is_five_minutes() {
        // Locks in the M2 default: topic cache TTL must be 5 min (matching
        // Java's `metadata.max.idle.ms`) to prevent unbounded metadata growth
        // on topic churn. See FINDINGS.md M2.
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        );
        assert_eq!(meta.topic_cache_ttl, Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_topic_cache_ttl_disabled_opt_out() {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        )
        .with_topic_cache_ttl_disabled();
        assert_eq!(meta.topic_cache_ttl, None);
    }

    /// Regression test: a partial refresh must not reset `topic_last_refreshed`
    /// for topics that were only retained from the cache (not present in the
    /// response).  Resetting retained timestamps makes them perpetually "fresh"
    /// so TTL eviction never fires.
    #[test]
    fn test_partial_refresh_preserves_retained_topic_timestamps() {
        use crate::protocol::{MetadataBroker, MetadataPartitionResponse, MetadataTopicResponse};

        fn make_response(topic_names: &[&str]) -> MetadataResponse {
            MetadataResponse {
                throttle_time_ms: 0,
                brokers: vec![MetadataBroker {
                    node_id: 1,
                    host: "localhost".to_string(),
                    port: 9092,
                    rack: None,
                }],
                cluster_id: None,
                controller_id: 1,
                error_code: ErrorCode::None,
                topics: topic_names
                    .iter()
                    .map(|name| MetadataTopicResponse {
                        error_code: ErrorCode::None,
                        name: Some(name.to_string()),
                        topic_id: None,
                        is_internal: false,
                        partitions: vec![MetadataPartitionResponse {
                            error_code: ErrorCode::None,
                            partition_index: 0,
                            leader_id: 1,
                            leader_epoch: 0,
                            replica_nodes: vec![1],
                            isr_nodes: vec![1],
                            offline_replicas: vec![],
                        }],
                    })
                    .collect(),
            }
        }

        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        // Use a long TTL so "topic-a" is not evicted.
        let meta = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        );

        // First partial update: populate cache with "topic-a".
        meta.update_cache(make_response(&["topic-a"]), false);
        let ts_a = meta
            .cache
            .load()
            .topic_last_refreshed
            .get("topic-a")
            .copied()
            .unwrap();

        // Second partial update: only "topic-b" is in the response.
        // "topic-a" is retained from the cache but must keep its original timestamp.
        meta.update_cache(make_response(&["topic-b"]), false);
        let cache = meta.cache.load();

        assert!(
            cache.topics.contains_key("topic-a"),
            "topic-a should still be in the cache (TTL not yet expired)"
        );
        assert!(
            cache.topics.contains_key("topic-b"),
            "topic-b should appear after the second update"
        );

        let ts_a_after = cache.topic_last_refreshed.get("topic-a").copied().unwrap();
        assert_eq!(
            ts_a, ts_a_after,
            "retained topic-a's timestamp must not be advanced by a partial refresh"
        );
        assert!(
            cache.topic_last_refreshed.contains_key("topic-b"),
            "freshly refreshed topic-b must have a timestamp"
        );
    }
}
