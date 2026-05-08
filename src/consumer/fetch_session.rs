//! Fetch session management (KIP-227).
//!
//! Implements incremental fetch sessions to reduce fetch request payload
//! sizes. Instead of sending the full partition list on every poll, the
//! broker tracks session state and the client only sends partition changes.
//!
//! A per-broker `FetchSessionState` tracks:
//! - `session_id` (returned by the broker) and `session_epoch` (maintained by
//!   the client)
//! - The set of partitions (with their fetch offsets and parameters) that
//!   are currently registered in the session
//!
//! On each fetch cycle the consumer computes a diff against the previous
//! session state and sends only the new/changed partitions in the `topics`
//! field plus any removed partitions in the `forgotten_topics` field.

use std::collections::{HashMap, HashSet};

use crate::protocol::{FetchForgottenTopic, FetchPartitionRequest, FetchTopicRequest};
use crate::{BrokerId, PartitionId};

/// Epoch value indicating the initial (full) fetch.
pub const INITIAL_EPOCH: i32 = 0;

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
    /// Partitions currently registered in the session, keyed by
    /// topic → partition → state. Nested map avoids cloning topic strings
    /// per partition on every update.
    partitions: HashMap<String, HashMap<PartitionId, PartitionState>>,
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
            partitions: HashMap::new(),
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
        self.partitions.values().map(|m| m.len()).sum()
    }

    /// Build the fetch request parameters by computing the diff between the
    /// desired partition set and the current session state.
    ///
    /// Returns a `FetchSessionRequest` containing:
    /// - `session_id` / `session_epoch` to set on the wire
    /// - `topics` with only new/changed partitions (or all if full fetch)
    /// - `forgotten_topics` with removed partitions
    pub fn build_request(&self, desired: &[FetchTopicRequest]) -> FetchSessionRequest {
        // Build a flat set of the desired partitions for fast lookup.
        // Keys borrow topic strings from `desired` to avoid per-poll cloning.
        let total_partitions: usize = desired.iter().map(|t| t.partitions.len()).sum();
        let mut desired_map: HashMap<(&str, PartitionId), &FetchPartitionRequest> =
            HashMap::with_capacity(total_partitions);
        for topic in desired {
            for part in &topic.partitions {
                desired_map.insert((topic.topic.as_str(), part.partition), part);
            }
        }

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

        // 1. Find new or changed partitions.
        let mut changed: HashMap<&str, Vec<FetchPartitionRequest>> = HashMap::new();
        for (&(topic, partition), req) in &desired_map {
            let is_new_or_changed = match self.partitions.get(topic).and_then(|m| m.get(&partition))
            {
                None => true, // New partition.
                Some(prev) => {
                    prev.fetch_offset != req.fetch_offset
                        || prev.partition_max_bytes != req.partition_max_bytes
                }
            };
            if is_new_or_changed {
                changed.entry(topic).or_default().push((*req).clone());
            }
        }

        // 2. Find removed partitions.
        let mut forgotten_map: HashMap<&str, Vec<i32>> = HashMap::new();
        for (topic, partitions) in &self.partitions {
            for &partition in partitions.keys() {
                if !desired_map.contains_key(&(topic.as_str(), partition)) {
                    forgotten_map
                        .entry(topic.as_str())
                        .or_default()
                        .push(partition);
                }
            }
        }

        let topics: Vec<FetchTopicRequest> = changed
            .into_iter()
            .map(|(topic, partitions)| FetchTopicRequest {
                topic: topic.to_string(),
                topic_id: None,
                partitions,
            })
            .collect();

        let forgotten_topics: Vec<FetchForgottenTopic> = forgotten_map
            .into_iter()
            .map(|(topic, partitions)| FetchForgottenTopic {
                topic: topic.to_string(),
                topic_id: None,
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
    /// used to rebuild the tracked partition set.
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

        // Rebuild tracked partition set from the full desired list.
        // This ensures our state matches what the broker tracked.
        self.partitions.clear();
        for topic in desired {
            let topic_map = self.partitions.entry(topic.topic.clone()).or_default();
            for part in &topic.partitions {
                topic_map.insert(
                    part.partition,
                    PartitionState {
                        fetch_offset: part.fetch_offset,
                        partition_max_bytes: part.partition_max_bytes,
                    },
                );
            }
        }
    }

    /// Reset session state (e.g., after a session error or rebalance).
    /// The next fetch will be a full fetch.
    pub fn reset(&mut self) {
        self.session_id = 0;
        self.epoch = INITIAL_EPOCH;
        self.partitions.clear();
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
