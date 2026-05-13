//! Fetch session management (KIP-227).
//!
//! Implements incremental fetch sessions to reduce fetch request payload
//! sizes. Instead of sending the full partition list on every poll, the
//! broker tracks session state and the client only sends partition changes.
//!
//! A per-broker `FetchSessionState` tracks:
//! - `session_id` (returned by the broker) and `session_epoch` (maintained by
//!   the client)
//! - The set of topics/partitions (with their fetch offsets and parameters)
//!   that are currently registered in the session
//!
//! On each fetch cycle the consumer computes a diff against the previous
//! session state and sends only the new/changed partitions in the `topics`
//! field plus any removed partitions in the `forgotten_topics` field.
//!
//! ## Topic keying (KIP-227 + KIP-516)
//!
//! When the broker supports Fetch v13+, each topic carries a 128-bit UUID
//! (`topic_id`). When a non-zero UUID is present the session uses it as the
//! primary key for the internal state map, reducing per-poll diff lookups
//! from variable-length string comparisons to fixed 16-byte equality tests.
//! Fallback to name-based keys is automatic when UUIDs are unavailable
//! (Fetch ≤ v12 or zero UUID).

use std::collections::{HashMap, HashSet};

use crate::protocol::{FetchForgottenTopic, FetchPartitionRequest, FetchTopicRequest};
use crate::{BrokerId, PartitionId};

/// Epoch value indicating the initial (full) fetch.
pub const INITIAL_EPOCH: i32 = 0;

/// All-zeroes UUID sentinel — indicates "no UUID" in Kafka wire protocol.
const ZERO_UUID: [u8; 16] = [0u8; 16];

/// Internal key used to track a topic inside a fetch session.
///
/// When the broker provides a non-zero `topic_id` (Fetch v13+), the UUID is
/// used directly, enabling O(1) diff lookups with 16-byte fixed-size keys.
/// When no UUID is available the topic name is used as a fallback.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum SessionKey {
    /// Topic name (Fetch v12 and below, or zero UUID).
    Name(String),
    /// Topic UUID (Fetch v13+).
    Uuid([u8; 16]),
}

impl SessionKey {
    fn from_request(topic: &FetchTopicRequest) -> Self {
        match topic.topic_id {
            Some(id) if id != ZERO_UUID => Self::Uuid(id),
            _ => Self::Name(topic.topic.clone()),
        }
    }
}

/// Tracked state for one topic inside a fetch session.
#[derive(Debug)]
struct TopicSession {
    /// Topic name — always stored so wire requests can be built correctly
    /// even when the primary session key is a UUID.
    name: String,
    /// Per-partition fetch state.
    partitions: HashMap<PartitionId, PartitionState>,
}

/// Snapshot of partition fetch parameters, used to detect changes between
/// consecutive fetches so that incremental requests only carry deltas.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PartitionState {
    fetch_offset: i64,
    partition_max_bytes: i32,
}

/// Per-broker fetch session state.
#[derive(Debug)]
pub struct FetchSessionState {
    /// Broker ID this session belongs to.
    ///
    /// Not read in production code but included in `Debug` output for
    /// diagnostics (the compiler intentionally ignores derive usage for
    /// dead-code analysis).
    #[allow(dead_code)]
    broker_id: BrokerId,
    /// Session ID returned by the broker (0 = no session).
    session_id: i32,
    /// Current epoch.
    epoch: i32,
    /// Topics currently registered in the session.
    ///
    /// Keyed by [`SessionKey::Uuid`] when a non-zero `topic_id` is available
    /// (Fetch v13+), otherwise by [`SessionKey::Name`].  Both variants map to
    /// a [`TopicSession`] which retains the human-readable topic name so that
    /// wire requests can be constructed correctly regardless of key type.
    topics: HashMap<SessionKey, TopicSession>,
}

/// The result of computing a fetch request from session state.
#[derive(Debug)]
pub struct FetchSessionRequest {
    /// Session ID to send.
    pub session_id: i32,
    /// Session epoch to send.
    pub session_epoch: i32,
    /// Topics/partitions to include in the request (new or changed).
    pub topics: Vec<FetchTopicRequest>,
    /// Topics/partitions to remove from the session.
    pub forgotten_topics: Vec<FetchForgottenTopic>,
    /// Whether this is a full fetch (epoch 0) vs incremental.
    pub is_full_fetch: bool,
}

impl FetchSessionState {
    /// Create a new fetch session for a broker. Starts with no session.
    pub fn new(broker_id: BrokerId) -> Self {
        Self {
            broker_id,
            session_id: 0,
            epoch: INITIAL_EPOCH,
            topics: HashMap::new(),
        }
    }

    /// Returns true if the session is established (session_id != 0).
    pub fn has_session(&self) -> bool {
        self.session_id != 0
    }

    #[cfg(test)]
    pub fn broker_id(&self) -> BrokerId {
        self.broker_id
    }

    #[cfg(test)]
    pub fn session_id(&self) -> i32 {
        self.session_id
    }

    #[cfg(test)]
    pub fn epoch(&self) -> i32 {
        self.epoch
    }

    #[cfg(test)]
    pub fn partition_count(&self) -> usize {
        self.topics.values().map(|t| t.partitions.len()).sum()
    }

    /// Build the fetch request parameters by computing the diff between the
    /// desired partition set and the current session state.
    ///
    /// When the supplied topics carry non-zero `topic_id` values (Fetch v13+),
    /// the diff uses 16-byte UUID keys for O(1) lookup.  For Fetch ≤ v12 the
    /// fallback is topic-name keys, identical to the prior behaviour.
    ///
    /// Returns a `FetchSessionRequest` containing:
    /// - `session_id` / `session_epoch` to set on the wire
    /// - `topics` with only new/changed partitions (or all if full fetch)
    /// - `forgotten_topics` with removed partitions
    pub fn build_request(&self, desired: &[FetchTopicRequest]) -> FetchSessionRequest {
        if !self.has_session() {
            // No established session → full fetch (epoch 0).
            return FetchSessionRequest {
                session_id: 0,
                session_epoch: INITIAL_EPOCH,
                topics: desired.to_vec(),
                forgotten_topics: Vec::new(),
                is_full_fetch: true,
            };
        }

        // Incremental fetch: compute diff.
        // self.epoch already represents the next epoch to send;
        // it is bumped by update_from_response() after a successful response.
        let epoch = self.epoch;

        // Build a keyed map of the desired partitions for O(1) lookup.
        // Key matches the scheme used to store session state: UUID when
        // available, topic name otherwise.
        let total: usize = desired.iter().map(|t| t.partitions.len()).sum();
        let mut desired_map: HashMap<SessionKey, HashMap<PartitionId, &FetchPartitionRequest>> =
            HashMap::with_capacity(desired.len());
        for topic in desired {
            let key = SessionKey::from_request(topic);
            let part_map = desired_map.entry(key).or_default();
            part_map.reserve(topic.partitions.len());
            for part in &topic.partitions {
                part_map.insert(part.partition, part);
            }
        }
        let _ = total; // capacity hint used above

        // 1. Find new or changed partitions.
        let mut changed: HashMap<&str, Vec<FetchPartitionRequest>> = HashMap::new();
        for topic in desired {
            let key = SessionKey::from_request(topic);
            let session_topic = self.topics.get(&key);
            for part in &topic.partitions {
                let is_new_or_changed =
                    match session_topic.and_then(|t| t.partitions.get(&part.partition)) {
                        None => true,
                        Some(prev) => {
                            prev.fetch_offset != part.fetch_offset
                                || prev.partition_max_bytes != part.partition_max_bytes
                        }
                    };
                if is_new_or_changed {
                    changed
                        .entry(topic.topic.as_str())
                        .or_default()
                        .push(part.clone());
                }
            }
        }

        // 2. Find removed topics/partitions.
        let desired_keys: HashSet<SessionKey> =
            desired.iter().map(SessionKey::from_request).collect();
        let mut forgotten_map: HashMap<&str, Vec<i32>> = HashMap::new();
        for (key, session_topic) in &self.topics {
            if desired_keys.contains(key) {
                // Topic still present — check for removed partitions.
                let desired_parts = desired_map.get(key);
                for &partition in session_topic.partitions.keys() {
                    let still_wanted = desired_parts.and_then(|m| m.get(&partition)).is_some();
                    if !still_wanted {
                        forgotten_map
                            .entry(session_topic.name.as_str())
                            .or_default()
                            .push(partition);
                    }
                }
            } else {
                // Entire topic removed.
                let parts: Vec<i32> = session_topic.partitions.keys().copied().collect();
                if !parts.is_empty() {
                    forgotten_map
                        .entry(session_topic.name.as_str())
                        .or_default()
                        .extend(parts);
                }
            }
        }

        // Build a name → UUID lookup so we can set `topic_id` correctly in both
        // the changed-topics and forgotten-topics lists.
        //
        // KIP-516: when Fetch v13+ sessions are keyed by UUID the broker uses
        // `topic_id` as the primary key; sending `topic_id: None` in incremental
        // or forgotten-topic entries causes the broker to treat those entries as
        // name-keyed, silently mismatching the UUID-keyed session state.
        let name_to_uuid: HashMap<&str, [u8; 16]> = desired
            .iter()
            .filter_map(|t| {
                t.topic_id
                    .filter(|id| *id != ZERO_UUID)
                    .map(|id| (t.topic.as_str(), id))
            })
            .collect();

        let topics: Vec<FetchTopicRequest> = changed
            .into_iter()
            .map(|(name, partitions)| FetchTopicRequest {
                topic: name.to_string(),
                topic_id: name_to_uuid.get(name).copied(),
                partitions,
            })
            .collect();

        let forgotten_topics: Vec<FetchForgottenTopic> = forgotten_map
            .into_iter()
            .map(|(name, partitions)| FetchForgottenTopic {
                topic: name.to_string(),
                topic_id: name_to_uuid.get(name).copied(),
                partitions,
            })
            .collect();

        FetchSessionRequest {
            session_id: self.session_id,
            session_epoch: epoch,
            topics,
            forgotten_topics,
            is_full_fetch: false,
        }
    }

    /// Update session state after a successful fetch response.
    ///
    /// - If the broker returned a non-zero `session_id`, establish/continue
    ///   the session and bump the epoch.
    /// - If the broker returned `session_id == 0`, the session was closed.
    ///
    /// `desired` is the full partition list the consumer requested this cycle,
    /// used to rebuild the tracked topic/partition set.
    pub fn update_from_response(
        &mut self,
        response_session_id: i32,
        desired: &[FetchTopicRequest],
    ) {
        if response_session_id == 0 {
            // Broker doesn't support sessions or chose to close ours.
            self.reset();
            return;
        }

        // Establish or continue the session.
        self.session_id = response_session_id;
        self.epoch = self.next_epoch();

        // Rebuild tracked topic/partition set from the full desired list.
        // This ensures our state matches what the broker tracked.
        self.topics.clear();
        for topic in desired {
            let key = SessionKey::from_request(topic);
            let mut partitions = HashMap::with_capacity(topic.partitions.len());
            for part in &topic.partitions {
                partitions.insert(
                    part.partition,
                    PartitionState {
                        fetch_offset: part.fetch_offset,
                        partition_max_bytes: part.partition_max_bytes,
                    },
                );
            }
            self.topics.insert(
                key,
                TopicSession {
                    name: topic.topic.clone(),
                    partitions,
                },
            );
        }
    }

    /// Reset session state (e.g., after a session error or rebalance).
    /// The next fetch will be a full fetch.
    pub fn reset(&mut self) {
        self.session_id = 0;
        self.epoch = INITIAL_EPOCH;
        self.topics.clear();
    }

    fn next_epoch(&self) -> i32 {
        if self.epoch == i32::MAX {
            // Wrap around (matching the Java client).
            1
        } else {
            self.epoch + 1
        }
    }
}

/// Cache of fetch sessions, one per broker.
#[derive(Debug, Default)]
pub struct FetchSessionCache {
    sessions: HashMap<BrokerId, FetchSessionState>,
}

impl FetchSessionCache {
    /// Create a new empty cache.
    pub fn new() -> Self {
        Self {
            sessions: HashMap::new(),
        }
    }

    /// Get or create the session state for a broker.
    pub fn get_or_create(&mut self, broker_id: BrokerId) -> &mut FetchSessionState {
        self.sessions
            .entry(broker_id)
            .or_insert_with(|| FetchSessionState::new(broker_id))
    }

    /// Reset the session for a specific broker (e.g., on error).
    pub fn reset_broker(&mut self, broker_id: BrokerId) {
        if let Some(session) = self.sessions.get_mut(&broker_id) {
            session.reset();
        }
    }

    /// Reset all sessions (e.g., on rebalance).
    pub fn reset_all(&mut self) {
        for session in self.sessions.values_mut() {
            session.reset();
        }
    }

    /// Remove sessions for brokers not in the given set.
    ///
    /// Call this during metadata refresh or after a rebalance when the set of
    /// live brokers changes, so sessions for departed brokers do not accumulate
    /// indefinitely. The next fetch to a re-joined broker will start a fresh
    /// full fetch, which is correct behavior.
    pub(crate) fn retain_brokers(&mut self, broker_ids: &[BrokerId]) {
        let broker_set: HashSet<_> = broker_ids.iter().copied().collect();
        self.sessions.retain(|id, _| broker_set.contains(id));
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn make_topic_request(topic: &str, partitions: &[(i32, i64, i32)]) -> FetchTopicRequest {
        FetchTopicRequest {
            topic: topic.to_string(),
            topic_id: None,
            partitions: partitions
                .iter()
                .map(|&(partition, offset, max_bytes)| FetchPartitionRequest {
                    partition,
                    current_leader_epoch: -1,
                    fetch_offset: offset,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: max_bytes,
                    replica_directory_id: None,
                    high_watermark: None,
                })
                .collect(),
        }
    }

    fn make_topic_request_with_epoch(
        topic: &str,
        partitions: &[(i32, i64, i32, i32)],
    ) -> FetchTopicRequest {
        FetchTopicRequest {
            topic: topic.to_string(),
            topic_id: None,
            partitions: partitions
                .iter()
                .map(
                    |&(partition, offset, max_bytes, epoch)| FetchPartitionRequest {
                        partition,
                        current_leader_epoch: epoch,
                        fetch_offset: offset,
                        last_fetched_epoch: -1,
                        log_start_offset: -1,
                        partition_max_bytes: max_bytes,
                        replica_directory_id: None,
                        high_watermark: None,
                    },
                )
                .collect(),
        }
    }

    #[test]
    fn test_new_session_starts_with_no_session() {
        let state = FetchSessionState::new(1);
        assert_eq!(state.session_id(), 0);
        assert_eq!(state.epoch(), INITIAL_EPOCH);
        assert!(!state.has_session());
        assert_eq!(state.partition_count(), 0);
    }

    #[test]
    fn test_first_fetch_is_full() {
        let state = FetchSessionState::new(1);
        let desired = vec![make_topic_request("topic-a", &[(0, 100, 1048576)])];

        let req = state.build_request(&desired);
        assert!(req.is_full_fetch);
        assert_eq!(req.session_id, 0);
        assert_eq!(req.session_epoch, INITIAL_EPOCH);
        assert_eq!(req.topics.len(), 1);
        assert!(req.forgotten_topics.is_empty());
    }

    #[test]
    fn test_session_established_after_response() {
        let mut state = FetchSessionState::new(1);
        let desired = vec![make_topic_request("topic-a", &[(0, 100, 1048576)])];

        // Simulate broker returning session_id=42
        state.update_from_response(42, &desired);

        assert!(state.has_session());
        assert_eq!(state.session_id(), 42);
        assert_eq!(state.epoch(), 1);
        assert_eq!(state.partition_count(), 1);
    }

    #[test]
    fn test_incremental_fetch_no_changes() {
        let mut state = FetchSessionState::new(1);
        let desired = vec![make_topic_request("topic-a", &[(0, 100, 1048576)])];

        state.update_from_response(42, &desired);

        // Same request again — no changes.
        let req = state.build_request(&desired);
        assert!(!req.is_full_fetch);
        assert_eq!(req.session_id, 42);
        assert_eq!(req.session_epoch, 1); // epoch maintained by client
        assert!(req.topics.is_empty()); // no changes
        assert!(req.forgotten_topics.is_empty()); // no removals
    }

    #[test]
    fn test_incremental_fetch_offset_changed() {
        let mut state = FetchSessionState::new(1);
        let desired = vec![make_topic_request("topic-a", &[(0, 100, 1048576)])];
        state.update_from_response(42, &desired);

        // Offset advanced.
        let desired2 = vec![make_topic_request("topic-a", &[(0, 200, 1048576)])];
        let req = state.build_request(&desired2);

        assert!(!req.is_full_fetch);
        assert_eq!(req.topics.len(), 1);
        assert_eq!(req.topics[0].partitions[0].fetch_offset, 200);
        assert!(req.forgotten_topics.is_empty());
    }

    #[test]
    fn test_incremental_fetch_partition_added() {
        let mut state = FetchSessionState::new(1);
        let desired = vec![make_topic_request("topic-a", &[(0, 100, 1048576)])];
        state.update_from_response(42, &desired);

        // Add partition 1.
        let desired2 = vec![make_topic_request(
            "topic-a",
            &[(0, 100, 1048576), (1, 0, 1048576)],
        )];
        let req = state.build_request(&desired2);

        assert!(!req.is_full_fetch);
        // Only the new partition 1 should appear.
        assert_eq!(req.topics.len(), 1);
        assert_eq!(req.topics[0].partitions.len(), 1);
        assert_eq!(req.topics[0].partitions[0].partition, 1);
        assert!(req.forgotten_topics.is_empty());
    }

    #[test]
    fn test_incremental_fetch_partition_removed() {
        let mut state = FetchSessionState::new(1);
        let desired = vec![make_topic_request(
            "topic-a",
            &[(0, 100, 1048576), (1, 50, 1048576)],
        )];
        state.update_from_response(42, &desired);

        // Remove partition 1.
        let desired2 = vec![make_topic_request("topic-a", &[(0, 100, 1048576)])];
        let req = state.build_request(&desired2);

        assert!(!req.is_full_fetch);
        assert!(req.topics.is_empty()); // no changes to p0
        assert_eq!(req.forgotten_topics.len(), 1);
        assert_eq!(req.forgotten_topics[0].topic, "topic-a");
        assert_eq!(req.forgotten_topics[0].partitions, vec![1]);
    }

    #[test]
    fn test_session_reset() {
        let mut state = FetchSessionState::new(1);
        let desired = vec![make_topic_request("topic-a", &[(0, 100, 1048576)])];
        state.update_from_response(42, &desired);
        assert!(state.has_session());

        state.reset();
        assert!(!state.has_session());
        assert_eq!(state.session_id(), 0);
        assert_eq!(state.epoch(), INITIAL_EPOCH);
        assert_eq!(state.partition_count(), 0);
    }

    #[test]
    fn test_broker_returns_zero_session_id_closes_session() {
        let mut state = FetchSessionState::new(1);
        let desired = vec![make_topic_request("topic-a", &[(0, 100, 1048576)])];
        state.update_from_response(42, &desired);
        assert!(state.has_session());

        // Broker closes session.
        state.update_from_response(0, &desired);
        assert!(!state.has_session());
    }

    #[test]
    fn test_epoch_wraps_at_max() {
        let mut state = FetchSessionState::new(1);
        state.session_id = 42;
        state.epoch = i32::MAX;

        let desired = vec![make_topic_request("topic-a", &[(0, 100, 1048576)])];
        let req = state.build_request(&desired);
        // build_request uses self.epoch directly.
        assert_eq!(req.session_epoch, i32::MAX);

        // update_from_response bumps via next_epoch, which wraps to 1.
        state.update_from_response(42, &desired);
        assert_eq!(state.epoch(), 1);
    }

    #[test]
    fn test_mixed_changes_and_removals() {
        let mut state = FetchSessionState::new(1);
        let desired = vec![
            make_topic_request("topic-a", &[(0, 100, 1048576), (1, 50, 1048576)]),
            make_topic_request("topic-b", &[(0, 200, 1048576)]),
        ];
        state.update_from_response(42, &desired);

        // Change: topic-a p0 offset advanced.
        // Remove: topic-b p0.
        // Add: topic-a p2 (new).
        let desired2 = vec![make_topic_request(
            "topic-a",
            &[(0, 300, 1048576), (1, 50, 1048576), (2, 0, 1048576)],
        )];
        let req = state.build_request(&desired2);

        assert!(!req.is_full_fetch);

        // Changed/new: topic-a p0 (offset changed) and p2 (new).
        let changed_partitions: Vec<i32> = req
            .topics
            .iter()
            .flat_map(|t| t.partitions.iter().map(|p| p.partition))
            .collect();
        assert!(changed_partitions.contains(&0));
        assert!(changed_partitions.contains(&2));
        assert!(!changed_partitions.contains(&1)); // p1 unchanged

        // Forgotten: topic-b p0.
        assert_eq!(req.forgotten_topics.len(), 1);
        assert_eq!(req.forgotten_topics[0].topic, "topic-b");
    }

    #[test]
    fn test_leader_epoch_change_not_tracked() {
        let mut state = FetchSessionState::new(1);
        let desired = vec![make_topic_request_with_epoch(
            "topic-a",
            &[(0, 100, 1048576, 5)],
        )];
        state.update_from_response(42, &desired);

        // Same offset, same max_bytes, but leader epoch changed.
        // Since Fetch v7 does not serialize current_leader_epoch,
        // the diff should NOT detect this as a change.
        let desired2 = vec![make_topic_request_with_epoch(
            "topic-a",
            &[(0, 100, 1048576, 6)],
        )];
        let req = state.build_request(&desired2);

        assert!(!req.is_full_fetch);
        assert!(req.topics.is_empty()); // no changes detected
        assert!(req.forgotten_topics.is_empty());
    }

    #[test]
    fn test_cache_get_or_create() {
        let mut cache = FetchSessionCache::new();
        {
            let session = cache.get_or_create(1);
            assert_eq!(session.broker_id(), 1);
            assert!(!session.has_session());
        }

        // Same broker returns same session.
        {
            let session = cache.get_or_create(1);
            session.session_id = 42;
        }
        {
            let session = cache.get_or_create(1);
            assert_eq!(session.session_id(), 42);
        }
    }

    #[test]
    fn test_cache_reset_broker() {
        let mut cache = FetchSessionCache::new();
        let desired = vec![make_topic_request("t", &[(0, 0, 1048576)])];
        cache.get_or_create(1).update_from_response(42, &desired);
        cache.get_or_create(2).update_from_response(43, &desired);

        cache.reset_broker(1);
        assert!(!cache.get_or_create(1).has_session());
        assert!(cache.get_or_create(2).has_session());
    }

    #[test]
    fn test_cache_reset_all() {
        let mut cache = FetchSessionCache::new();
        let desired = vec![make_topic_request("t", &[(0, 0, 1048576)])];
        cache.get_or_create(1).update_from_response(42, &desired);
        cache.get_or_create(2).update_from_response(43, &desired);

        cache.reset_all();
        assert!(!cache.get_or_create(1).has_session());
        assert!(!cache.get_or_create(2).has_session());
    }

    #[test]
    fn test_cache_retain_brokers() {
        let mut cache = FetchSessionCache::new();
        let desired = vec![make_topic_request("t", &[(0, 0, 1048576)])];
        cache.get_or_create(1).update_from_response(42, &desired);
        cache.get_or_create(2).update_from_response(43, &desired);
        cache.get_or_create(3).update_from_response(44, &desired);

        cache.retain_brokers(&[1, 3]);
        assert!(cache.get_or_create(1).has_session());
        assert!(!cache.get_or_create(2).has_session()); // was removed, recreated
        assert!(cache.get_or_create(3).has_session());
    }

    #[test]
    fn test_full_fetch_after_reset() {
        let mut state = FetchSessionState::new(1);
        let desired = vec![make_topic_request("topic-a", &[(0, 100, 1048576)])];
        state.update_from_response(42, &desired);

        state.reset();

        // After reset, next fetch should be full.
        let req = state.build_request(&desired);
        assert!(req.is_full_fetch);
        assert_eq!(req.session_id, 0);
        assert_eq!(req.session_epoch, INITIAL_EPOCH);
    }

    fn make_uuid_topic_request(
        topic: &str,
        uuid: [u8; 16],
        partitions: &[(i32, i64, i32)],
    ) -> FetchTopicRequest {
        FetchTopicRequest {
            topic: topic.to_string(),
            topic_id: Some(uuid),
            partitions: partitions
                .iter()
                .map(|&(partition, offset, max_bytes)| FetchPartitionRequest {
                    partition,
                    current_leader_epoch: -1,
                    fetch_offset: offset,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: max_bytes,
                    replica_directory_id: None,
                    high_watermark: None,
                })
                .collect(),
        }
    }

    #[test]
    fn test_uuid_keyed_incremental_no_changes() {
        let uuid: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let mut state = FetchSessionState::new(1);
        let desired = vec![make_uuid_topic_request(
            "topic-a",
            uuid,
            &[(0, 100, 1048576)],
        )];
        state.update_from_response(42, &desired);

        assert_eq!(state.partition_count(), 1);

        // Same request — no changes.
        let req = state.build_request(&desired);
        assert!(!req.is_full_fetch);
        assert!(req.topics.is_empty());
        assert!(req.forgotten_topics.is_empty());
    }

    #[test]
    fn test_uuid_keyed_offset_change() {
        let uuid: [u8; 16] = [0xAA; 16];
        let mut state = FetchSessionState::new(1);
        let desired = vec![make_uuid_topic_request(
            "topic-a",
            uuid,
            &[(0, 100, 1048576)],
        )];
        state.update_from_response(42, &desired);

        let desired2 = vec![make_uuid_topic_request(
            "topic-a",
            uuid,
            &[(0, 200, 1048576)],
        )];
        let req = state.build_request(&desired2);

        assert!(!req.is_full_fetch);
        assert_eq!(req.topics.len(), 1);
        assert_eq!(req.topics[0].topic, "topic-a");
        assert_eq!(req.topics[0].partitions[0].fetch_offset, 200);
    }

    #[test]
    fn test_uuid_keyed_partition_removed() {
        let uuid: [u8; 16] = [0xBB; 16];
        let mut state = FetchSessionState::new(1);
        let desired = vec![make_uuid_topic_request(
            "topic-a",
            uuid,
            &[(0, 100, 1048576), (1, 50, 1048576)],
        )];
        state.update_from_response(42, &desired);

        // Remove partition 1.
        let desired2 = vec![make_uuid_topic_request(
            "topic-a",
            uuid,
            &[(0, 100, 1048576)],
        )];
        let req = state.build_request(&desired2);

        assert!(!req.is_full_fetch);
        assert!(req.topics.is_empty()); // partition 0 unchanged
        assert_eq!(req.forgotten_topics.len(), 1);
        assert_eq!(req.forgotten_topics[0].topic, "topic-a");
        assert_eq!(req.forgotten_topics[0].partitions, vec![1]);
    }

    #[test]
    fn test_zero_uuid_falls_back_to_name_key() {
        // topic_id = [0; 16] (zero UUID sentinel) must fall back to name keying.
        let zero_uuid = [0u8; 16];
        let mut state = FetchSessionState::new(1);
        let desired = vec![make_uuid_topic_request(
            "topic-a",
            zero_uuid,
            &[(0, 100, 1048576)],
        )];
        state.update_from_response(42, &desired);

        // Same desired → no diff.
        let req = state.build_request(&desired);
        assert!(!req.is_full_fetch);
        assert!(req.topics.is_empty());
        assert!(req.forgotten_topics.is_empty());
    }

    #[test]
    fn test_mixed_uuid_and_name_keying() {
        // Two topics: one with UUID, one without.
        let uuid: [u8; 16] = [0xCC; 16];
        let mut state = FetchSessionState::new(1);
        let desired = vec![
            make_uuid_topic_request("topic-uuid", uuid, &[(0, 100, 1048576)]),
            make_topic_request("topic-name", &[(0, 200, 1048576)]),
        ];
        state.update_from_response(42, &desired);
        assert_eq!(state.partition_count(), 2);

        // No changes.
        let req = state.build_request(&desired);
        assert!(!req.is_full_fetch);
        assert!(req.topics.is_empty());
        assert!(req.forgotten_topics.is_empty());
    }

    #[test]
    fn test_empty_desired_produces_all_forgotten() {
        let mut state = FetchSessionState::new(1);
        let desired = vec![make_topic_request(
            "topic-a",
            &[(0, 100, 1048576), (1, 50, 1048576)],
        )];
        state.update_from_response(42, &desired);

        // All partitions removed.
        let desired2: Vec<FetchTopicRequest> = vec![];
        let req = state.build_request(&desired2);

        assert!(!req.is_full_fetch);
        assert!(req.topics.is_empty());
        assert_eq!(req.forgotten_topics.len(), 1);
        assert_eq!(req.forgotten_topics[0].partitions.len(), 2);
    }

    #[test]
    fn test_max_bytes_change_detected() {
        let mut state = FetchSessionState::new(1);
        let desired = vec![make_topic_request("topic-a", &[(0, 100, 1048576)])];
        state.update_from_response(42, &desired);

        // max_bytes changed.
        let desired2 = vec![make_topic_request("topic-a", &[(0, 100, 2097152)])];
        let req = state.build_request(&desired2);

        assert!(!req.is_full_fetch);
        assert_eq!(req.topics.len(), 1);
        assert_eq!(req.topics[0].partitions[0].partition_max_bytes, 2097152);
    }
}
