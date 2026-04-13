//! Consumer group coordination.
//!
//! This module provides consumer group coordination primitives including:
//! - [`ConsumerGroup`] state machine for group coordination
//! - [`GroupCoordinator`] for managing group membership and heartbeats
//! - [`MemberAssignment`] for tracking partition assignments
//! - [`PartitionAssignor`] trait and implementations for partition assignment strategies
//! - [`ConsumerRebalanceListener`] trait for rebalance callbacks

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, info, warn};

use crate::PartitionId;
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::metadata::ClusterMetadata;
use crate::network::{BrokerConnection, ConnectionPool};
use crate::protocol::{
    ApiKey, ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse,
    ConsumerGroupTopicPartitions, FindCoordinatorRequest, FindCoordinatorResponse,
    HeartbeatRequest, HeartbeatResponse, JoinGroupRequest, JoinGroupRequestProtocol,
    JoinGroupResponse, JoinGroupResponseMember, LeaveGroupMember, LeaveGroupRequest,
    LeaveGroupResponse, ListOffsetsRequest, ListOffsetsRequestPartition, ListOffsetsRequestTopic,
    ListOffsetsResponse, MAX_DECODE_ARRAY_LEN, OffsetCommitRequest, OffsetCommitRequestPartition,
    OffsetCommitRequestTopic, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchRequestTopic,
    OffsetFetchResponse, SyncGroupRequest, SyncGroupRequestAssignment, SyncGroupResponse,
    VersionedDecode, VersionedEncode,
    versions::{
        CONSUMER_GROUP_HEARTBEAT_MAX, CONSUMER_GROUP_HEARTBEAT_MIN, FIND_COORDINATOR_MAX,
        FIND_COORDINATOR_MIN, HEARTBEAT_MAX, HEARTBEAT_MIN, JOIN_GROUP_MAX, JOIN_GROUP_MIN,
        LEAVE_GROUP_MAX, LEAVE_GROUP_MIN, LIST_OFFSETS_MAX, LIST_OFFSETS_MIN, OFFSET_COMMIT_MAX,
        OFFSET_COMMIT_MIN, OFFSET_FETCH_MAX, OFFSET_FETCH_MIN, SYNC_GROUP_MAX, SYNC_GROUP_MIN,
    },
};

/// Callback interface for partition rebalance events.
///
/// Implement this trait to receive notifications when the consumer's
/// partition assignment changes during a rebalance.
///
/// # Example
///
/// ```rust,ignore
/// use krafka::consumer::{ConsumerRebalanceListener, TopicPartition};
///
/// struct MyListener;
///
/// impl ConsumerRebalanceListener for MyListener {
///     fn on_partitions_assigned(&self, partitions: &[TopicPartition]) {
///         println!("Assigned: {:?}", partitions);
///     }
///
///     fn on_partitions_revoked(&self, partitions: &[TopicPartition]) {
///         println!("Revoked: {:?}", partitions);
///         // Commit offsets before losing partitions
///     }
/// }
/// ```
pub trait ConsumerRebalanceListener: Send + Sync {
    /// Called after partitions have been assigned to this consumer.
    ///
    /// The `partitions` parameter contains the **full set** of partitions now
    /// owned by this consumer after the rebalance — not just newly added ones.
    /// It may include partitions that were already assigned before the rebalance.
    ///
    /// > **Note:** The Java `ConsumerRebalanceListener.onPartitionsAssigned`
    /// > passes only *newly added* partitions for cooperative rebalances.
    /// > This crate always passes the full set for simplicity and to avoid
    /// > protocol-dependent callback semantics.
    fn on_partitions_assigned(&self, partitions: &[crate::consumer::TopicPartition]);

    /// Called before partitions are revoked from this consumer.
    ///
    /// This is triggered during a rebalance before the consumer loses
    /// its current partitions. Use this to commit offsets synchronously
    /// if needed.
    fn on_partitions_revoked(&self, partitions: &[crate::consumer::TopicPartition]);

    /// Called when partitions are lost due to an unclean shutdown.
    ///
    /// This is called when the consumer unexpectedly loses its partition
    /// assignment (e.g., session timeout). Unlike `on_partitions_revoked`,
    /// offsets may already be committed by another consumer.
    fn on_partitions_lost(&self, partitions: &[crate::consumer::TopicPartition]) {
        // Default implementation delegates to revoked
        self.on_partitions_revoked(partitions);
    }
}

/// A no-op rebalance listener that does nothing on rebalance events.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoOpRebalanceListener;

impl ConsumerRebalanceListener for NoOpRebalanceListener {
    fn on_partitions_assigned(&self, _partitions: &[crate::consumer::TopicPartition]) {}
    fn on_partitions_revoked(&self, _partitions: &[crate::consumer::TopicPartition]) {}
}

/// Consumer group state.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum GroupState {
    /// Not yet joined.
    #[default]
    Unjoined,
    /// Joining the group.
    Joining,
    /// Awaiting sync.
    AwaitingSync,
    /// Stable and consuming.
    Stable,
    /// Preparing to rebalance.
    PreparingRebalance,
    /// Leaving the group.
    Leaving,
    /// Dead.
    Dead,
}

/// Member assignment in a consumer group.
#[derive(Debug, Clone, Default)]
pub struct MemberAssignment {
    /// Assigned partitions per topic.
    pub partitions: HashMap<String, Vec<PartitionId>>,
}

impl MemberAssignment {
    /// Create an empty assignment.
    pub fn empty() -> Self {
        Self::default()
    }

    /// Add partitions for a topic.
    pub fn add(&mut self, topic: impl Into<String>, partitions: Vec<PartitionId>) {
        self.partitions.insert(topic.into(), partitions);
    }

    /// Get partitions for a topic.
    pub fn get(&self, topic: &str) -> Option<&[PartitionId]> {
        self.partitions.get(topic).map(|v| v.as_slice())
    }

    /// Get all assigned topic-partitions.
    pub fn all_partitions(&self) -> Vec<(&str, PartitionId)> {
        let mut result = Vec::new();
        for (topic, partitions) in &self.partitions {
            for &partition in partitions {
                result.push((topic.as_str(), partition));
            }
        }
        result
    }

    /// Check if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }
}

/// A consumer group member.
#[derive(Debug, Clone)]
pub struct GroupMember {
    /// Member ID assigned by the coordinator.
    pub member_id: String,
    /// Client ID.
    pub client_id: String,
    /// Client host.
    pub client_host: String,
    /// Member metadata.
    pub metadata: Vec<u8>,
    /// Member assignment.
    pub assignment: Vec<u8>,
}

/// Consumer group coordinator.
#[derive(Debug)]
pub struct ConsumerGroup {
    /// Group ID.
    group_id: String,
    /// Member ID (assigned by coordinator).
    member_id: Arc<RwLock<Option<String>>>,
    /// Generation ID.
    generation_id: Arc<RwLock<i32>>,
    /// Current state.
    state: Arc<RwLock<GroupState>>,
    /// Current assignment.
    assignment: Arc<RwLock<MemberAssignment>>,
    /// Coordinator broker ID.
    coordinator_id: Arc<RwLock<Option<i32>>>,
    /// Session timeout.
    session_timeout: Duration,
    /// Heartbeat interval.
    heartbeat_interval: Duration,
    /// Rebalance timeout.
    rebalance_timeout: Duration,
}

impl ConsumerGroup {
    /// Create a new consumer group.
    pub fn new(
        group_id: impl Into<String>,
        session_timeout: Duration,
        heartbeat_interval: Duration,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            member_id: Arc::new(RwLock::new(None)),
            generation_id: Arc::new(RwLock::new(-1)),
            state: Arc::new(RwLock::new(GroupState::Unjoined)),
            assignment: Arc::new(RwLock::new(MemberAssignment::empty())),
            coordinator_id: Arc::new(RwLock::new(None)),
            session_timeout,
            heartbeat_interval,
            rebalance_timeout: session_timeout,
        }
    }

    /// Get the group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Get the session timeout.
    pub fn session_timeout(&self) -> Duration {
        self.session_timeout
    }

    /// Get the heartbeat interval.
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }

    /// Get the rebalance timeout.
    pub fn rebalance_timeout(&self) -> Duration {
        self.rebalance_timeout
    }

    /// Get the current state.
    pub async fn state(&self) -> GroupState {
        *self.state.read().await
    }

    /// Get the member ID.
    pub async fn member_id(&self) -> Option<String> {
        self.member_id.read().await.clone()
    }

    /// Get the generation ID.
    pub async fn generation_id(&self) -> i32 {
        *self.generation_id.read().await
    }

    /// Get the current assignment.
    pub async fn assignment(&self) -> MemberAssignment {
        self.assignment.read().await.clone()
    }

    /// Get the coordinator broker ID.
    pub async fn coordinator_id(&self) -> Option<i32> {
        *self.coordinator_id.read().await
    }

    /// Set the coordinator broker ID.
    pub async fn set_coordinator(&self, broker_id: i32) {
        *self.coordinator_id.write().await = Some(broker_id);
    }

    /// Set the state.
    pub async fn set_state(&self, state: GroupState) {
        *self.state.write().await = state;
    }

    /// Update member ID and generation after joining.
    pub async fn join_complete(&self, member_id: String, generation_id: i32) {
        *self.member_id.write().await = Some(member_id);
        *self.generation_id.write().await = generation_id;
    }

    /// Update assignment after sync.
    pub async fn sync_complete(&self, assignment: MemberAssignment) {
        *self.assignment.write().await = assignment;
        *self.state.write().await = GroupState::Stable;
    }

    /// Reset group state on error or leave.
    pub async fn reset(&self) {
        *self.member_id.write().await = None;
        *self.generation_id.write().await = -1;
        *self.state.write().await = GroupState::Unjoined;
        *self.assignment.write().await = MemberAssignment::empty();
    }

    /// Check if a rebalance is needed.
    pub async fn needs_rejoin(&self) -> bool {
        matches!(
            *self.state.read().await,
            GroupState::Unjoined | GroupState::PreparingRebalance
        )
    }

    /// Validate we're in a valid state to commit.
    pub async fn validate_for_commit(&self) -> Result<()> {
        let state = *self.state.read().await;
        match state {
            GroupState::Stable => Ok(()),
            GroupState::Unjoined => Err(KrafkaError::invalid_state(
                "cannot commit: not part of a group",
            )),
            GroupState::PreparingRebalance | GroupState::AwaitingSync => Err(
                KrafkaError::invalid_state("cannot commit: rebalance in progress"),
            ),
            _ => Err(KrafkaError::invalid_state(format!(
                "cannot commit in state: {state:?}",
            ))),
        }
    }
}

/// Partition assignment strategy.
pub trait PartitionAssignor: Send + Sync {
    /// Strategy name.
    fn name(&self) -> &str;

    /// Assign partitions to members.
    fn assign(
        &self,
        topics: &[String],
        partitions: &HashMap<String, Vec<PartitionId>>,
        members: &[GroupMember],
    ) -> HashMap<String, MemberAssignment>;
}

/// Range partition assignor (default).
#[derive(Debug, Default)]
pub struct RangeAssignor;

impl PartitionAssignor for RangeAssignor {
    fn name(&self) -> &str {
        "range"
    }

    fn assign(
        &self,
        topics: &[String],
        partitions: &HashMap<String, Vec<PartitionId>>,
        members: &[GroupMember],
    ) -> HashMap<String, MemberAssignment> {
        let mut assignments: HashMap<String, MemberAssignment> = HashMap::new();

        // Initialize assignments for all members
        for member in members {
            assignments.insert(member.member_id.clone(), MemberAssignment::empty());
        }

        // Assign partitions for each topic
        for topic in topics {
            if let Some(topic_partitions) = partitions.get(topic) {
                let mut sorted_partitions = topic_partitions.clone();
                sorted_partitions.sort();

                let num_partitions = sorted_partitions.len();
                let num_members = members.len();

                if num_members == 0 {
                    continue;
                }

                let partitions_per_member = num_partitions / num_members;
                let extra = num_partitions % num_members;

                let mut partition_idx = 0;
                for (member_idx, member) in members.iter().enumerate() {
                    let count = partitions_per_member + if member_idx < extra { 1 } else { 0 };
                    let member_partitions: Vec<PartitionId> =
                        sorted_partitions[partition_idx..partition_idx + count].to_vec();
                    partition_idx += count;

                    if !member_partitions.is_empty()
                        && let Some(assignment) = assignments.get_mut(&member.member_id)
                    {
                        assignment.add(topic.clone(), member_partitions);
                    }
                }
            }
        }

        assignments
    }
}

/// Round-robin partition assignor.
#[derive(Debug, Default)]
pub struct RoundRobinAssignor;

impl PartitionAssignor for RoundRobinAssignor {
    fn name(&self) -> &str {
        "roundrobin"
    }

    fn assign(
        &self,
        topics: &[String],
        partitions: &HashMap<String, Vec<PartitionId>>,
        members: &[GroupMember],
    ) -> HashMap<String, MemberAssignment> {
        let mut assignments: HashMap<String, MemberAssignment> = HashMap::new();

        // Initialize assignments for all members
        for member in members {
            assignments.insert(member.member_id.clone(), MemberAssignment::empty());
        }

        if members.is_empty() {
            return assignments;
        }

        // Collect all topic-partitions
        let mut all_partitions: Vec<(String, PartitionId)> = Vec::new();
        for topic in topics {
            if let Some(topic_partitions) = partitions.get(topic) {
                for &partition in topic_partitions {
                    all_partitions.push((topic.clone(), partition));
                }
            }
        }

        // Sort by topic then partition
        all_partitions.sort();

        // Track partitions per topic per member
        let mut member_topic_partitions: HashMap<String, HashMap<String, Vec<PartitionId>>> =
            HashMap::new();
        for member in members {
            member_topic_partitions.insert(member.member_id.clone(), HashMap::new());
        }

        // Round-robin assign
        for (idx, (topic, partition)) in all_partitions.into_iter().enumerate() {
            let member = &members[idx % members.len()];
            let member_topics = member_topic_partitions
                .get_mut(&member.member_id)
                .expect("member must exist in pre-populated map");
            member_topics.entry(topic).or_default().push(partition);
        }

        // Build final assignments
        for (member_id, topic_partitions) in member_topic_partitions {
            let mut assignment = MemberAssignment::empty();
            for (topic, partitions) in topic_partitions {
                assignment.add(topic, partitions);
            }
            assignments.insert(member_id, assignment);
        }

        assignments
    }
}

/// Cooperative sticky partition assignor.
///
/// This assignor implements the cooperative rebalance protocol which minimizes
/// partition movement during rebalances. It maintains "stickiness" by trying to
/// keep partitions with their current owners while ensuring fair distribution.
///
/// Key features:
/// - Minimizes partition movement during rebalances
/// - Maintains balanced partition distribution
/// - Supports incremental cooperative rebalancing
///
/// # Example
///
/// ```
/// use krafka::consumer::{CooperativeStickyAssignor, PartitionAssignor};
///
/// let assignor = CooperativeStickyAssignor::new();
/// assert_eq!(assignor.name(), "cooperative-sticky");
/// ```
#[derive(Debug, Default)]
pub struct CooperativeStickyAssignor {
    /// Previous assignments for stickiness (member_id -> (topic, partitions))
    previous_assignments: parking_lot::RwLock<HashMap<String, HashMap<String, Vec<PartitionId>>>>,
}

impl CooperativeStickyAssignor {
    /// Create a new cooperative sticky assignor.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the current assignments for future stickiness.
    pub fn record_assignment(&self, member_id: &str, assignment: &MemberAssignment) {
        let mut prev = self.previous_assignments.write();
        prev.insert(member_id.to_string(), assignment.partitions.clone());
    }

    /// Clear previous assignment for a member that left.
    pub fn clear_member(&self, member_id: &str) {
        self.previous_assignments.write().remove(member_id);
    }

    /// Retain only the given member IDs, removing stale entries.
    pub(crate) fn retain_members(&self, member_ids: &HashSet<&str>) {
        self.previous_assignments
            .write()
            .retain(|k, _| member_ids.contains(k.as_str()));
    }

    /// Get partitions that should be revoked (for incremental rebalance).
    pub fn get_partitions_to_revoke(
        &self,
        member_id: &str,
        new_assignment: &MemberAssignment,
    ) -> Vec<(String, PartitionId)> {
        let prev = self.previous_assignments.read();
        let mut revoked = Vec::new();

        if let Some(old_partitions) = prev.get(member_id) {
            for (topic, old_parts) in old_partitions {
                let new_parts = new_assignment.get(topic).unwrap_or(&[]);
                for &old_part in old_parts {
                    if !new_parts.contains(&old_part) {
                        revoked.push((topic.clone(), old_part));
                    }
                }
            }
        }

        revoked
    }
}

impl PartitionAssignor for CooperativeStickyAssignor {
    fn name(&self) -> &str {
        "cooperative-sticky"
    }

    fn assign(
        &self,
        topics: &[String],
        partitions: &HashMap<String, Vec<PartitionId>>,
        members: &[GroupMember],
    ) -> HashMap<String, MemberAssignment> {
        let mut assignments: HashMap<String, MemberAssignment> = HashMap::new();

        // Initialize assignments for all members
        for member in members {
            assignments.insert(member.member_id.clone(), MemberAssignment::empty());
        }

        if members.is_empty() {
            return assignments;
        }

        // Collect all topic-partitions
        let mut all_partitions: Vec<(String, PartitionId)> = Vec::new();
        for topic in topics {
            if let Some(topic_partitions) = partitions.get(topic) {
                for &partition in topic_partitions {
                    all_partitions.push((topic.clone(), partition));
                }
            }
        }

        // Get previous assignments for stickiness.
        let prev_guard = self.previous_assignments.read();
        let prev_assignments = &*prev_guard;

        // Track which partitions are already assigned (sticky)
        let mut sticky_assignments: HashMap<(String, PartitionId), String> = HashMap::new();
        let mut member_partition_counts: HashMap<String, usize> = HashMap::new();

        // First pass: honor previous assignments (stickiness)
        for member in members {
            let mid = member.member_id.clone();
            member_partition_counts.entry(mid.clone()).or_insert(0);

            if let Some(prev) = prev_assignments.get(&member.member_id) {
                for (topic, prev_parts) in prev {
                    // Only keep partitions that are still available
                    if let Some(available_parts) = partitions.get(topic) {
                        for &part in prev_parts {
                            if available_parts.contains(&part) {
                                let key = (topic.clone(), part);
                                if let std::collections::hash_map::Entry::Vacant(e) =
                                    sticky_assignments.entry(key)
                                {
                                    e.insert(mid.clone());
                                    *member_partition_counts.get_mut(&mid).unwrap() += 1;
                                }
                            }
                        }
                    }
                }
            }
        }

        // Calculate target partitions per member for balance
        let total_partitions = all_partitions.len();
        let num_members = members.len();
        let min_per_member = total_partitions / num_members;
        let extra = total_partitions % num_members;

        // Second pass: assign unassigned partitions while maintaining balance
        for (topic, partition) in &all_partitions {
            let key = (topic.clone(), *partition);
            if sticky_assignments.contains_key(&key) {
                continue; // Already assigned via stickiness
            }

            // Find member with fewest partitions that needs more
            let mut best_member: Option<&str> = None;
            let mut min_count = usize::MAX;

            for (idx, member) in members.iter().enumerate() {
                let target = min_per_member + if idx < extra { 1 } else { 0 };
                let current = *member_partition_counts.get(&member.member_id).unwrap_or(&0);

                if current < target && current < min_count {
                    min_count = current;
                    best_member = Some(&member.member_id);
                }
            }

            // If everyone is at target, find anyone below max
            if best_member.is_none() {
                for member in members {
                    let current = *member_partition_counts.get(&member.member_id).unwrap_or(&0);
                    if current < min_count {
                        min_count = current;
                        best_member = Some(&member.member_id);
                    }
                }
            }

            if let Some(member_id) = best_member {
                // SAFETY: member_partition_counts was populated for all members in the first pass.
                *member_partition_counts.get_mut(member_id).unwrap() += 1;
                sticky_assignments.insert(key, member_id.to_string());
            }
        }

        // Third pass: rebalance if needed (steal from overloaded members)
        // This ensures no member has more than ceil(total/members) partitions
        let max_per_member = total_partitions.div_ceil(num_members);

        loop {
            let mut moved = false;

            // Find overloaded and underloaded members
            let mut overloaded: Vec<String> = Vec::new();
            let mut underloaded: Vec<String> = Vec::new();

            for member in members {
                let count = *member_partition_counts.get(&member.member_id).unwrap_or(&0);
                if count > max_per_member {
                    overloaded.push(member.member_id.clone());
                } else if count < max_per_member {
                    // use max_per_member (ceil) as the underloaded threshold.
                    // Using min_per_member (floor) left members that could accept more
                    // partitions undetected, causing unbalanced 3-1-1 distributions
                    // instead of balanced 2-2-1.
                    underloaded.push(member.member_id.clone());
                }
            }

            if overloaded.is_empty() || underloaded.is_empty() {
                break;
            }

            // Move one partition from overloaded to underloaded
            'outer: for over_member in &overloaded {
                for under_member in &underloaded {
                    // Find a partition to move
                    for (_key, owner) in sticky_assignments.iter_mut() {
                        if owner == over_member {
                            *owner = under_member.clone();
                            if let Some(count) = member_partition_counts.get_mut(over_member) {
                                *count = count.saturating_sub(1);
                            }
                            // SAFETY: member_partition_counts was populated for all members
                            // in the first pass.
                            *member_partition_counts.get_mut(under_member).unwrap() += 1;
                            moved = true;
                            break 'outer;
                        }
                    }
                }
            }

            if !moved {
                break;
            }
        }

        // Build final assignments from sticky_assignments
        for ((topic, partition), member_id) in sticky_assignments {
            if let Some(assignment) = assignments.get_mut(&member_id) {
                assignment
                    .partitions
                    .entry(topic)
                    .or_default()
                    .push(partition);
            }
        }

        // Sort partitions within each topic for consistency
        for assignment in assignments.values_mut() {
            for parts in assignment.partitions.values_mut() {
                parts.sort();
            }
        }

        assignments
    }
}

/// Controller for managing periodic heartbeat tasks.
///
/// The heartbeat controller sends heartbeats at a configurable interval
/// to keep the consumer alive in its group. It tracks the last heartbeat
/// time and can detect session timeouts.
#[derive(Debug)]
pub struct HeartbeatController {
    /// Heartbeat interval.
    interval: Duration,
    /// Session timeout.
    session_timeout: Duration,
    /// Last successful heartbeat time.
    last_heartbeat: Arc<RwLock<Option<std::time::Instant>>>,
    /// Whether the controller is running.
    running: Arc<std::sync::atomic::AtomicBool>,
    /// Whether a rebalance has been detected by the heartbeat task.
    rebalance_needed: Arc<std::sync::atomic::AtomicBool>,
    /// Whether the member session has been invalidated by a heartbeat error
    /// (UNKNOWN_MEMBER_ID, ILLEGAL_GENERATION, SESSION_TIMEOUT).
    /// When set, needs_rejoin() will clear member_id and generation_id
    /// in addition to triggering a rebalance.
    member_invalidated: Arc<std::sync::atomic::AtomicBool>,
}

impl HeartbeatController {
    /// Create a new heartbeat controller.
    pub fn new(interval: Duration, session_timeout: Duration) -> Self {
        Self {
            interval,
            session_timeout,
            last_heartbeat: Arc::new(RwLock::new(None)),
            running: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            rebalance_needed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            member_invalidated: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Get the heartbeat interval.
    #[inline]
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Get the session timeout.
    #[inline]
    pub fn session_timeout(&self) -> Duration {
        self.session_timeout
    }

    /// Check if the controller is running.
    #[inline]
    pub fn is_running(&self) -> bool {
        self.running.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Start the heartbeat controller.
    pub fn start(&self) {
        self.running
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Stop the heartbeat controller.
    pub fn stop(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }

    /// Signal that a rebalance is needed (called from heartbeat task).
    pub fn signal_rebalance(&self) {
        self.rebalance_needed
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Check and clear the rebalance-needed flag.
    pub fn take_rebalance_needed(&self) -> bool {
        self.rebalance_needed
            .swap(false, std::sync::atomic::Ordering::AcqRel)
    }

    /// Signal that the member session has been invalidated
    /// (UNKNOWN_MEMBER_ID, ILLEGAL_GENERATION, or session timeout).
    /// Also sets the rebalance_needed flag.
    pub fn signal_member_invalidated(&self) {
        self.member_invalidated
            .store(true, std::sync::atomic::Ordering::Release);
        self.rebalance_needed
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Check and clear the member-invalidated flag.
    pub fn take_member_invalidated(&self) -> bool {
        self.member_invalidated
            .swap(false, std::sync::atomic::Ordering::AcqRel)
    }

    /// Record a successful heartbeat.
    pub async fn heartbeat_success(&self) {
        *self.last_heartbeat.write().await = Some(std::time::Instant::now());
    }

    /// Get the time since the last heartbeat.
    pub async fn time_since_last_heartbeat(&self) -> Option<Duration> {
        self.last_heartbeat.read().await.map(|t| t.elapsed())
    }

    /// Check if the session may have timed out.
    pub async fn may_have_timed_out(&self) -> bool {
        if let Some(elapsed) = self.time_since_last_heartbeat().await {
            elapsed > self.session_timeout
        } else {
            false
        }
    }

    /// Wait for the next heartbeat interval.
    ///
    /// This is a convenience method for use in heartbeat loops.
    pub async fn wait_for_next_interval(&self) {
        tokio::time::sleep(self.interval).await;
    }
}

/// Heartbeat response status from the coordinator.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatStatus {
    /// Heartbeat accepted, continue normally.
    Ok,
    /// Rebalance in progress, rejoin required.
    RebalanceNeeded,
    /// Unknown member, rejoin required.
    UnknownMember,
    /// Illegal generation, rejoin required.
    IllegalGeneration,
    /// Session timed out, rejoin required.
    SessionTimeout,
    /// Fatal error, leave group.
    FatalError,
}

impl HeartbeatStatus {
    /// Whether a rejoin is required based on this status.
    #[inline]
    pub fn requires_rejoin(&self) -> bool {
        matches!(
            self,
            Self::RebalanceNeeded
                | Self::UnknownMember
                | Self::IllegalGeneration
                | Self::SessionTimeout
        )
    }

    /// Whether this is a fatal error requiring group leave.
    #[inline]
    pub fn is_fatal(&self) -> bool {
        matches!(self, Self::FatalError)
    }

    /// Whether this status indicates the member session has been invalidated
    /// (as opposed to a simple rebalance-in-progress).
    #[inline]
    pub fn is_session_invalidating(&self) -> bool {
        matches!(
            self,
            Self::UnknownMember | Self::IllegalGeneration | Self::SessionTimeout
        )
    }

    /// Convert from an ErrorCode.
    pub fn from_error_code(code: ErrorCode) -> Self {
        match code {
            ErrorCode::None => Self::Ok,
            ErrorCode::RebalanceInProgress => Self::RebalanceNeeded,
            ErrorCode::UnknownMemberId => Self::UnknownMember,
            ErrorCode::IllegalGeneration => Self::IllegalGeneration,
            ErrorCode::CoordinatorNotAvailable
            | ErrorCode::NotCoordinator
            | ErrorCode::CoordinatorLoadInProgress => Self::SessionTimeout,
            _ => Self::FatalError,
        }
    }
}

// ============================================================================
// Group Coordinator
// ============================================================================

/// Commands for the heartbeat background task.
#[derive(Debug)]
pub enum HeartbeatCommand {
    /// Stop the heartbeat task.
    Stop,
    /// Trigger a rejoin.
    Rejoin,
    /// Send an immediate heartbeat with current owned partitions to
    /// acknowledge a revocation (KIP-848 §revocation-ack).
    AcknowledgeRevocation,
}

/// Group coordinator that manages group membership, heartbeats, and offset commits.
///
/// This struct encapsulates all the logic for consumer group protocol:
/// - Finding the group coordinator broker
/// - Joining and syncing with the group
/// - Sending periodic heartbeats in a background task
/// - Committing offsets to the coordinator
///
/// # Example
///
/// ```rust,ignore
/// use krafka::consumer::GroupCoordinator;
///
/// let coordinator = GroupCoordinator::new(
///     group_id,
///     pool,
///     metadata,
///     session_timeout,
///     heartbeat_interval,
///     rebalance_timeout,
/// );
///
/// // Find coordinator and join group
/// let topics = vec!["topic1".to_string()];
/// let (assignment, joined) = coordinator.ensure_active_membership(&topics).await?;
///
/// // Commit offsets
/// coordinator.commit_offsets(&offsets).await?;
/// ```
pub struct GroupCoordinator {
    /// Group ID.
    group_id: String,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Cluster metadata.
    metadata: Arc<ClusterMetadata>,
    /// Session timeout.
    session_timeout: Duration,
    /// Heartbeat interval.
    heartbeat_interval: Duration,
    /// Rebalance timeout.
    rebalance_timeout: Duration,
    /// Coordinator connection.
    coordinator_conn: Arc<RwLock<Option<Arc<BrokerConnection>>>>,
    /// Coordinator node ID.
    coordinator_id: RwLock<Option<i32>>,
    /// Member ID assigned by coordinator.
    member_id: Arc<RwLock<String>>,
    /// Generation ID.
    generation_id: Arc<RwLock<i32>>,
    /// Current group state.
    state: Arc<RwLock<GroupState>>,
    /// Current partition assignment.
    assignment: Arc<RwLock<MemberAssignment>>,
    /// Heartbeat controller.
    heartbeat_controller: Arc<HeartbeatController>,
    /// Channel to control heartbeat task.
    heartbeat_cmd_tx: RwLock<Option<mpsc::Sender<HeartbeatCommand>>>,
    /// Subscribed topics.
    subscribed_topics: RwLock<Vec<String>>,
    /// Protocol type (always "consumer").
    protocol_type: String,
    /// Partition assignment strategy.
    assignment_strategy: crate::consumer::config::PartitionAssignmentStrategy,
    /// Partition assignor name.
    assignor_name: String,
    /// Static group membership instance ID (KIP-345).
    group_instance_id: Option<String>,
    /// Persistent sticky assignor (retains previous assignments across rebalances).
    sticky_assignor: CooperativeStickyAssignor,
    /// Transaction isolation level (0 = read_uncommitted, 1 = read_committed).
    isolation_level: i8,
    /// Group protocol selection (KIP-848).
    group_protocol: crate::consumer::config::GroupProtocol,
    /// Member epoch for the KIP-848 consumer protocol.
    ///
    /// Replaces `generation_id` semantics: 0 = join, -1 = leave,
    /// -2 = static member temporary leave.
    member_epoch: Arc<RwLock<i32>>,
    /// Raw target assignment received from the KIP-848 coordinator (topic UUIDs
    /// and partition lists). Stored so that unresolved UUIDs can be re-resolved
    /// on the next metadata refresh instead of being permanently lost.
    target_assignment: Arc<RwLock<Vec<ConsumerGroupTopicPartitions>>>,
    /// Local cache of topic UUID → name mappings discovered during assignment
    /// resolution. Serves as a fallback when the metadata cache is flushed
    /// (e.g. during a full refresh). Mirrors the Java client's
    /// `assignedTopicNamesCache`. Cleared on leave/reset/fencing.
    topic_names_cache: Arc<RwLock<HashMap<[u8; 16], String>>>,
}

impl GroupCoordinator {
    /// Create a new group coordinator.
    pub fn new(
        group_id: impl Into<String>,
        pool: Arc<ConnectionPool>,
        metadata: Arc<ClusterMetadata>,
        session_timeout: Duration,
        heartbeat_interval: Duration,
        rebalance_timeout: Duration,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            pool,
            metadata,
            session_timeout,
            heartbeat_interval,
            rebalance_timeout,
            coordinator_conn: Arc::new(RwLock::new(None)),
            coordinator_id: RwLock::new(None),
            member_id: Arc::new(RwLock::new(String::new())),
            generation_id: Arc::new(RwLock::new(-1)),
            state: Arc::new(RwLock::new(GroupState::Unjoined)),
            assignment: Arc::new(RwLock::new(MemberAssignment::empty())),
            heartbeat_controller: Arc::new(HeartbeatController::new(
                heartbeat_interval,
                session_timeout,
            )),
            heartbeat_cmd_tx: RwLock::new(None),
            subscribed_topics: RwLock::new(Vec::new()),
            protocol_type: "consumer".to_string(),
            assignment_strategy: crate::consumer::config::PartitionAssignmentStrategy::Range,
            assignor_name: "range".to_string(),
            group_instance_id: None,
            sticky_assignor: CooperativeStickyAssignor::new(),
            isolation_level: 0,
            group_protocol: crate::consumer::config::GroupProtocol::Classic,
            member_epoch: Arc::new(RwLock::new(0)),
            target_assignment: Arc::new(RwLock::new(Vec::new())),
            topic_names_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Set the partition assignment strategy (builder pattern).
    pub fn with_assignor_strategy(
        mut self,
        strategy: crate::consumer::config::PartitionAssignmentStrategy,
    ) -> Self {
        self.assignor_name = strategy.protocol_name().to_string();
        self.assignment_strategy = strategy;
        self
    }

    /// Set the static group membership instance ID (KIP-345, builder pattern).
    pub fn with_group_instance_id(mut self, id: Option<String>) -> Self {
        self.group_instance_id = id;
        self
    }

    /// Set the transaction isolation level (builder pattern).
    pub fn with_isolation_level(mut self, level: i8) -> Self {
        self.isolation_level = level;
        self
    }

    /// Set the group protocol (KIP-848, builder pattern).
    pub fn with_group_protocol(mut self, protocol: crate::consumer::config::GroupProtocol) -> Self {
        self.group_protocol = protocol;
        self
    }

    /// Whether the current assignment strategy is cooperative.
    ///
    /// Always returns `false` for the KIP-848 consumer protocol, which uses
    /// server-side assignment and does not use JoinGroup/SyncGroup semantics.
    pub fn is_cooperative(&self) -> bool {
        !self.is_consumer_protocol()
            && self.assignment_strategy
                == crate::consumer::config::PartitionAssignmentStrategy::CooperativeSticky
    }

    /// Whether the consumer uses the KIP-848 consumer group protocol.
    pub fn is_consumer_protocol(&self) -> bool {
        self.group_protocol == crate::consumer::config::GroupProtocol::Consumer
    }

    /// Get the group ID.
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Get the current state.
    pub async fn state(&self) -> GroupState {
        *self.state.read().await
    }

    /// Get the member ID.
    pub async fn member_id(&self) -> String {
        self.member_id.read().await.clone()
    }

    /// Get the generation ID.
    pub async fn generation_id(&self) -> i32 {
        *self.generation_id.read().await
    }

    /// Get the current assignment.
    pub async fn assignment(&self) -> MemberAssignment {
        self.assignment.read().await.clone()
    }

    /// Get the current subscribed topics.
    pub async fn subscribed_topics(&self) -> Vec<String> {
        self.subscribed_topics.read().await.clone()
    }

    /// Set the subscribed topics.
    pub async fn set_subscribed_topics(&self, topics: Vec<String>) {
        *self.subscribed_topics.write().await = topics;
    }

    /// Check if the group needs to rejoin.
    pub async fn needs_rejoin(&self) -> bool {
        // Check heartbeat controller's rebalance flag first (immediate detection from R8.3)
        if self.heartbeat_controller.take_rebalance_needed() {
            // If the heartbeat detected a session-invalidating error
            // (UNKNOWN_MEMBER_ID, ILLEGAL_GENERATION, session timeout),
            // clear the member identity so the next join_group() sends
            // a fresh empty member_id. This must happen here (not in
            // the heartbeat task) because we need access to sticky_assignor.
            if self.heartbeat_controller.take_member_invalidated() {
                if self.is_consumer_protocol() {
                    // KIP-848: preserve member_id — spec requires fenced
                    // members to "rejoin with the same member id and
                    // epoch 0". Reset epoch and assignment state but
                    // keep the member identity for re-registration.
                    self.reset_for_kip848_fencing().await;
                    return true;
                }
                self.reset_member_identity().await;
            }
            // For KIP-848: the heartbeat task signals rebalance when a new
            // assignment arrives and sets the state to Stable. Don't
            // downgrade Stable → PreparingRebalance — the consumer just
            // needs to process the assignment diff without re-joining.
            if !(self.is_consumer_protocol()
                && matches!(*self.state.read().await, GroupState::Stable))
            {
                *self.state.write().await = GroupState::PreparingRebalance;
            }
            return true;
        }
        matches!(
            *self.state.read().await,
            GroupState::Unjoined | GroupState::PreparingRebalance
        )
    }

    /// Find the group coordinator broker.
    pub async fn find_coordinator(&self) -> Result<()> {
        debug!("Finding coordinator for group '{}'", self.group_id);

        // Get a connection to any broker
        let conn = self.get_any_connection().await?;

        // Send FindCoordinator request with version negotiation.
        // Fall back to v0 when ApiVersions is unavailable — v0 is sufficient
        // for group coordinator lookup and compatible with all brokers.
        let request = FindCoordinatorRequest::for_group(&self.group_id);
        let fc_version = conn
            .negotiate_api_version(
                ApiKey::FindCoordinator,
                FIND_COORDINATOR_MAX,
                FIND_COORDINATOR_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol(format!(
                    "broker does not support FindCoordinator v{}-v{}",
                    FIND_COORDINATOR_MIN, FIND_COORDINATOR_MAX,
                ))
            })?;
        let response = conn
            .send_request(ApiKey::FindCoordinator, fc_version, |buf| {
                request.encode_versioned(fc_version, buf)
            })
            .await?;

        let mut buf = response;
        let find_response = FindCoordinatorResponse::decode_versioned(fc_version, &mut buf)?;

        if !find_response.error_code.is_ok() {
            return Err(KrafkaError::broker(
                find_response.error_code,
                format!(
                    "Failed to find coordinator: {:?}",
                    find_response.error_message
                ),
            ));
        }

        // Connect to the coordinator
        let coordinator_addr = format!("{}:{}", find_response.host, find_response.port);
        let coordinator_conn = self.pool.get_connection(&coordinator_addr).await?;

        *self.coordinator_conn.write().await = Some(coordinator_conn);
        *self.coordinator_id.write().await = Some(find_response.node_id);

        info!(
            "Found coordinator for group '{}': node {} at {}",
            self.group_id, find_response.node_id, coordinator_addr
        );

        Ok(())
    }

    /// Get the coordinator connection, finding it if necessary.
    /// Checks liveness and SASL session expiry of cached connections and re-discovers if unusable.
    async fn get_coordinator_connection(&self) -> Result<Arc<BrokerConnection>> {
        {
            let conn = self.coordinator_conn.read().await;
            if let Some(ref c) = *conn {
                if c.is_usable() {
                    return Ok(c.clone());
                }
                // Connection is dead or SASL session expired, clear it and re-discover
                drop(conn);
                *self.coordinator_conn.write().await = None;
                debug!("Coordinator connection is unusable, re-discovering");
            }
        }

        self.find_coordinator().await?;

        let conn = self.coordinator_conn.read().await;
        conn.clone()
            .ok_or_else(|| KrafkaError::invalid_state("coordinator not found"))
    }

    /// Get any available broker connection.
    async fn get_any_connection(&self) -> Result<Arc<BrokerConnection>> {
        // Try cached brokers first
        let brokers = self.metadata.brokers();
        for broker in brokers {
            if let Ok(conn) = self.pool.get_connection(broker.address()).await {
                return Ok(conn);
            }
        }

        // Fall back to bootstrap servers
        for server in &self.metadata.bootstrap_servers() {
            if let Ok(conn) = self.pool.get_connection(server).await {
                return Ok(conn);
            }
        }

        Err(KrafkaError::invalid_state("no available brokers"))
    }

    /// Join the consumer group.
    pub async fn join_group(&self) -> Result<JoinGroupResponse> {
        let conn = self.get_coordinator_connection().await?;

        let member_id = self.member_id.read().await.clone();
        let topics = self.subscribed_topics.read().await.clone();

        // Get owned partitions for cooperative metadata
        let owned_partitions = if self.is_cooperative() {
            self.sticky_assignor
                .previous_assignments
                .read()
                .get(&member_id)
                .cloned()
                .unwrap_or_default()
        } else {
            HashMap::new()
        };

        // Build consumer protocol metadata
        let metadata = self.encode_consumer_metadata(&topics, &owned_partitions)?;

        let request = JoinGroupRequest {
            group_id: self.group_id.clone(),
            session_timeout_ms: crate::util::duration_to_millis_i32(self.session_timeout),
            rebalance_timeout_ms: crate::util::duration_to_millis_i32(self.rebalance_timeout),
            member_id: member_id.clone(),
            group_instance_id: self.group_instance_id.clone(),
            protocol_type: self.protocol_type.clone(),
            protocols: vec![JoinGroupRequestProtocol {
                name: self.assignor_name.clone(),
                metadata: metadata.freeze(),
            }],
            reason: None,
        };

        debug!(
            "Joining group '{}' with member_id '{}'",
            self.group_id, member_id
        );

        *self.state.write().await = GroupState::Joining;

        // Negotiate JoinGroup version. Static membership (group_instance_id)
        // requires v5+ where the GroupInstanceId field is available.
        let join_group_min = if self.group_instance_id.is_some() {
            5
        } else {
            JOIN_GROUP_MIN
        };
        let jg_version = conn
            .negotiate_api_version(ApiKey::JoinGroup, JOIN_GROUP_MAX, join_group_min)
            .await
            .ok_or_else(|| {
                KrafkaError::protocol(format!(
                    "broker does not support JoinGroup v{}-v{}",
                    join_group_min, JOIN_GROUP_MAX,
                ))
            })?;

        let response = conn
            .send_request(ApiKey::JoinGroup, jg_version, |buf| {
                request.encode_versioned(jg_version, buf)
            })
            .await?;

        let mut buf = response;
        let mut join_response = JoinGroupResponse::decode_versioned(jg_version, &mut buf)?;

        // KIP-394 (v4+): broker returns MemberIdRequired with a newly
        // assigned member_id.  Save the id and retry the JoinGroup request
        // exactly once, which is the expected two-step join handshake.
        if join_response.error_code == ErrorCode::MemberIdRequired {
            debug!(
                "Received MemberIdRequired for group '{}', retrying with assigned member_id '{}'",
                self.group_id, join_response.member_id
            );

            // Persist the broker-assigned member_id.
            *self.member_id.write().await = join_response.member_id.clone();

            // Rebuild the request with the assigned member_id.
            let retry_request = JoinGroupRequest {
                member_id: join_response.member_id.clone(),
                ..request.clone()
            };

            let retry_response = conn
                .send_request(ApiKey::JoinGroup, jg_version, |buf| {
                    retry_request.encode_versioned(jg_version, buf)
                })
                .await?;

            let mut retry_buf = retry_response;
            join_response = JoinGroupResponse::decode_versioned(jg_version, &mut retry_buf)?;
        }

        if !join_response.error_code.is_ok() {
            // Reset member identity on session-invalidating errors so the
            // next rejoin attempt sends an empty member_id (fresh registration)
            // instead of the dead one. Matches the Java client's behavior in
            // AbstractCoordinator.resetStateOnResponseError().
            if join_response.error_code == ErrorCode::UnknownMemberId
                || join_response.error_code == ErrorCode::IllegalGeneration
            {
                self.reset_member_identity().await;
            }
            *self.state.write().await = GroupState::Unjoined;
            return Err(KrafkaError::broker(
                join_response.error_code,
                "Failed to join group",
            ));
        }

        // Update member ID and generation.
        // If the broker assigned a different member_id (e.g., first join
        // with empty id, or broker-side reassignment), clear the old
        // entry from sticky_assignor to prevent unbounded accumulation
        // of orphaned previous_assignments keyed by stale member IDs.
        {
            let old_member_id = self.member_id.read().await.clone();
            if !old_member_id.is_empty() && old_member_id != join_response.member_id {
                self.sticky_assignor.clear_member(&old_member_id);
            }
        }
        *self.member_id.write().await = join_response.member_id.clone();
        *self.generation_id.write().await = join_response.generation_id;
        *self.state.write().await = GroupState::AwaitingSync;

        info!(
            "Joined group '{}': member_id='{}', generation={}, is_leader={}",
            self.group_id,
            join_response.member_id,
            join_response.generation_id,
            join_response.is_leader()
        );

        Ok(join_response)
    }

    /// Sync with the group after joining.
    pub async fn sync_group(&self, join_response: &JoinGroupResponse) -> Result<MemberAssignment> {
        let conn = self.get_coordinator_connection().await?;

        let member_id = self.member_id.read().await.clone();
        let generation_id = *self.generation_id.read().await;
        let topics = self.subscribed_topics.read().await.clone();

        // If we're the leader, compute assignments
        let assignments = if join_response.is_leader() {
            self.compute_assignments(&topics, &join_response.members)
                .await?
        } else {
            Vec::new()
        };

        let request = SyncGroupRequest {
            group_id: self.group_id.clone(),
            generation_id,
            member_id: member_id.clone(),
            group_instance_id: self.group_instance_id.clone(),
            protocol_type: Some(self.protocol_type.clone()),
            protocol_name: join_response.protocol_name.clone(),
            assignments,
        };

        debug!(
            "Syncing group '{}': generation={}, is_leader={}",
            self.group_id,
            generation_id,
            join_response.is_leader()
        );

        // Negotiate SyncGroup version — v3+ required (KIP-345 static membership).
        let sg_version = conn
            .negotiate_api_version(ApiKey::SyncGroup, SYNC_GROUP_MAX, SYNC_GROUP_MIN)
            .await
            .ok_or_else(|| {
                KrafkaError::protocol(format!(
                    "broker does not support SyncGroup v{}-v{}",
                    SYNC_GROUP_MIN, SYNC_GROUP_MAX,
                ))
            })?;

        let response = conn
            .send_request(ApiKey::SyncGroup, sg_version, |buf| {
                request.encode_versioned(sg_version, buf)
            })
            .await?;

        let mut buf = response;
        let sync_response = SyncGroupResponse::decode_versioned(sg_version, &mut buf)?;

        if !sync_response.error_code.is_ok() {
            // Reset member identity on session-invalidating errors.
            // After a failed sync with UNKNOWN_MEMBER_ID or ILLEGAL_GENERATION,
            // the broker no longer recognizes our member_id + generation_id pair.
            // Clearing them ensures the next rejoin sends a fresh empty
            // member_id for re-registration.
            // REBALANCE_IN_PROGRESS means the session is still valid but the
            // group is rebalancing — keep member_id so we can rejoin faster.
            if sync_response.error_code == ErrorCode::UnknownMemberId
                || sync_response.error_code == ErrorCode::IllegalGeneration
            {
                self.reset_member_identity().await;
            }
            *self.state.write().await = GroupState::Unjoined;
            return Err(KrafkaError::broker(
                sync_response.error_code,
                "Failed to sync group",
            ));
        }

        // Decode the assignment
        let assignment = self.decode_consumer_assignment(&sync_response.assignment)?;

        // Note: for cooperative mode, record_assignment() is NOT called here.
        // The poll loop defers it until after get_partitions_to_revoke() has
        // compared old vs new, so the previous-assignment baseline stays intact.

        // Update state
        *self.assignment.write().await = assignment.clone();
        *self.state.write().await = GroupState::Stable;

        info!(
            "Synced group '{}': received {} topic assignments",
            self.group_id,
            assignment.partitions.len()
        );

        for (topic, partitions) in &assignment.partitions {
            debug!("  {} -> {:?}", topic, partitions);
        }

        Ok(assignment)
    }

    /// Ensure active group membership, joining/rejoining as needed.
    ///
    /// Returns `(assignment, joined)` where `joined` is `true` when an actual
    /// JoinGroup/SyncGroup round-trip occurred (first join or topic change).
    /// When the group is already Stable with unchanged topics, returns the
    /// cached assignment with `joined = false`.
    ///
    /// For eager (non-cooperative) protocols, performs a single join+sync.
    /// For cooperative protocols, the caller should use
    /// `perform_cooperative_join_and_sync` instead for the two-phase flow.
    pub async fn ensure_active_membership(
        &self,
        topics: &[String],
    ) -> Result<(MemberAssignment, bool)> {
        // Dispatch based on group protocol
        if self.is_consumer_protocol() {
            return self.ensure_active_membership_consumer(topics).await;
        }

        // Classic protocol: JoinGroup/SyncGroup/Heartbeat
        // Detect topic changes: if the subscription changed while Stable,
        // force a rejoin so the broker learns the new subscription.
        let new_topics = topics.to_vec();
        {
            let state = *self.state.read().await;
            if state == GroupState::Stable {
                let old_topics = self.subscribed_topics.read().await;
                let mut old_sorted = old_topics.clone();
                drop(old_topics);
                old_sorted.sort();
                let mut new_sorted = new_topics.clone();
                new_sorted.sort();
                if old_sorted != new_sorted {
                    // Topics changed — must rejoin to update broker subscription.
                    // Use set_preparing_rebalance (not trigger_rejoin) so the
                    // heartbeat task keeps running while perform_join_and_sync
                    // does the actual rejoin below.
                    self.set_preparing_rebalance().await;
                }
            }
        }

        // Update subscribed topics
        self.set_subscribed_topics(new_topics).await;

        let state = *self.state.read().await;
        if state == GroupState::Stable {
            // Already stable with same topics, return current assignment
            Ok((self.assignment.read().await.clone(), false))
        } else {
            // Need to join/rejoin
            let assignment = self.perform_join_and_sync().await?;
            Ok((assignment, true))
        }
    }

    /// Perform the full join and sync sequence.
    async fn perform_join_and_sync(&self) -> Result<MemberAssignment> {
        // Find coordinator if needed
        if self.coordinator_conn.read().await.is_none() {
            self.find_coordinator().await?;
        }

        // Join group
        let join_response = self.join_group().await?;

        // Sync group
        let assignment = self.sync_group(&join_response).await?;

        // Start heartbeat task
        self.start_heartbeat_task().await;

        Ok(assignment)
    }

    /// Perform cooperative incremental rebalance (KIP-429).
    ///
    /// Two-phase protocol:
    /// 1. Join/sync to get the new target assignment
    /// 2. Compute which partitions to revoke (old - new)
    /// 3. If revocations are needed, return them so the caller can
    ///    revoke and then trigger a second rejoin
    /// 4. If no revocations, the assignment is final
    ///
    /// Returns `(assignment, partitions_to_revoke)`. If `partitions_to_revoke`
    /// is non-empty, the caller must revoke those partitions and call this
    /// method again.
    pub async fn perform_cooperative_join_and_sync(
        &self,
    ) -> Result<(MemberAssignment, Vec<(String, PartitionId)>)> {
        // Find coordinator if needed
        if self.coordinator_conn.read().await.is_none() {
            self.find_coordinator().await?;
        }

        // Join group
        let join_response = self.join_group().await?;

        // Sync group to get new target assignment
        let new_assignment = self.sync_group(&join_response).await?;

        // Compute what needs to be revoked
        let member_id = self.member_id.read().await.clone();
        let to_revoke = self
            .sticky_assignor
            .get_partitions_to_revoke(&member_id, &new_assignment);

        if to_revoke.is_empty() {
            // No revocations needed — assignment is final
            self.start_heartbeat_task().await;
            Ok((new_assignment, Vec::new()))
        } else {
            info!(
                "Cooperative rebalance: revoking {} partition(s) before second rejoin",
                to_revoke.len()
            );
            // Don't start heartbeat yet — we need another rejoin after revocation.
            // The caller (e.g. the poll loop) will update the owned-partitions baseline
            // in sticky_assignor after applying these revocations and finalizing the assignment.
            Ok((new_assignment, to_revoke))
        }
    }

    /// Start the background heartbeat task.
    pub(crate) async fn start_heartbeat_task(&self) {
        // Stop existing task if any
        self.stop_heartbeat_task().await;

        // Clear any stale rebalance/invalidation signals from the previous
        // heartbeat task. Between sending the Stop command and the old task
        // terminating, it may have received REBALANCE_IN_PROGRESS or a
        // session-invalidating error. Those signals are now stale — we just
        // completed a successful join/sync.
        self.heartbeat_controller.take_rebalance_needed();
        self.heartbeat_controller.take_member_invalidated();

        let (cmd_tx, mut cmd_rx) = mpsc::channel::<HeartbeatCommand>(10);
        *self.heartbeat_cmd_tx.write().await = Some(cmd_tx);

        let group_id = self.group_id.clone();
        let heartbeat_interval = self.heartbeat_interval;
        let heartbeat_controller = self.heartbeat_controller.clone();

        // Clone Arc references so the task reads current values on each heartbeat
        let member_id_ref = self.member_id.clone();
        let generation_id_ref = self.generation_id.clone();
        let coordinator_conn_ref = self.coordinator_conn.clone();
        let group_instance_id = self.group_instance_id.clone();

        // start() before spawn is safe here — the classic task has no early-return
        // paths before the loop. KIP-848's task calls start() *inside* spawn after
        // version negotiation to avoid marking running=true on negotiation failure.
        heartbeat_controller.start();

        tokio::spawn(async move {
            debug!("Starting heartbeat task for group '{}'", group_id);

            let mut interval = tokio::time::interval(heartbeat_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            // Cache the negotiated heartbeat version per coordinator connection.
            // The version is stable for a given connection (API versions don't
            // change until reconnect), so we only re-negotiate when the
            // connection identity changes.
            let mut cached_hb_version: Option<i16> = None;
            let mut cached_conn_id: Option<usize> = None;

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if !heartbeat_controller.is_running() {
                            break;
                        }

                        // Read current values on each heartbeat (not stale copies)
                        let coordinator_conn = coordinator_conn_ref.read().await.clone();
                        let member_id = member_id_ref.read().await.clone();
                        let generation_id = *generation_id_ref.read().await;

                        // Send heartbeat
                        if let Some(ref conn) = coordinator_conn {
                            // Re-negotiate only when the coordinator connection changes.
                            let conn_id = std::sync::Arc::as_ptr(conn) as usize;
                            let hb_version = if cached_conn_id == Some(conn_id) {
                                // Safe to unwrap: cached_hb_version is always set
                                // together with cached_conn_id.
                                cached_hb_version.unwrap()
                            } else {
                                match conn
                                    .negotiate_api_version(
                                        ApiKey::Heartbeat,
                                        HEARTBEAT_MAX,
                                        HEARTBEAT_MIN,
                                    )
                                    .await
                                {
                                    Some(v) => {
                                        cached_conn_id = Some(conn_id);
                                        cached_hb_version = Some(v);
                                        v
                                    }
                                    None => {
                                        warn!(
                                            "Broker does not support Heartbeat v{}-v{} for group '{}', triggering rebalance",
                                            HEARTBEAT_MIN, HEARTBEAT_MAX, group_id
                                        );
                                        *coordinator_conn_ref.write().await = None;
                                        heartbeat_controller.signal_rebalance();
                                        heartbeat_controller.stop();
                                        break;
                                    }
                                }
                            };

                            let request = HeartbeatRequest {
                                group_id: group_id.clone(),
                                generation_id,
                                member_id: member_id.clone(),
                                group_instance_id: group_instance_id.clone(),
                            };
                            let send_result = conn
                                .send_request(ApiKey::Heartbeat, hb_version, |buf| {
                                    request.encode_versioned(hb_version, buf)
                                })
                                .await;

                            match send_result
                            {
                                Ok(response) => {
                                    let mut buf = response;
                                    let decode_result = HeartbeatResponse::decode_versioned(hb_version, &mut buf);
                                    if let Ok(hb_response) = decode_result {
                                        let status = HeartbeatStatus::from_error_code(hb_response.error_code);
                                        match status {
                                            HeartbeatStatus::Ok => {
                                                heartbeat_controller.heartbeat_success().await;
                                                debug!("Heartbeat successful for group '{}'", group_id);
                                            }
                                            HeartbeatStatus::RebalanceNeeded => {
                                                warn!("Rebalance needed for group '{}', stopping heartbeat", group_id);
                                                heartbeat_controller.signal_rebalance();
                                                heartbeat_controller.stop();
                                                break;
                                            }
                                            status if status.requires_rejoin() => {
                                                warn!("Heartbeat status {:?} requires rejoin for group '{}'", status, group_id);
                                                // This arm only fires for session-invalidating
                                                // errors (UnknownMember, IllegalGeneration,
                                                // SessionTimeout) — RebalanceNeeded is handled
                                                // above. Signal that member identity must be
                                                // cleared. The actual cleanup (sticky_assignor +
                                                // member_id + generation_id) happens in
                                                // needs_rejoin() which has full access to the
                                                // coordinator.
                                                heartbeat_controller.signal_member_invalidated();
                                                heartbeat_controller.stop();
                                                break;
                                            }
                                            HeartbeatStatus::FatalError => {
                                                error!("Fatal heartbeat error for group '{}'", group_id);
                                                heartbeat_controller.stop();
                                                break;
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("Heartbeat failed for group '{}': {}", group_id, e);
                                    // Network error — the coordinator connection
                                    // may be dead. Clear it and exit the heartbeat
                                    // loop so the consumer poll loop can rediscover
                                    // the coordinator and rejoin.
                                    *coordinator_conn_ref.write().await = None;
                                    heartbeat_controller.signal_rebalance();
                                    heartbeat_controller.stop();
                                    break;
                                }
                            }
                        }
                    }
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(HeartbeatCommand::Stop) | None => {
                                debug!("Stopping heartbeat task for group '{}'", group_id);
                                heartbeat_controller.stop();
                                break;
                            }
                            Some(HeartbeatCommand::Rejoin) => {
                                debug!("Rejoin requested for group '{}'", group_id);
                                heartbeat_controller.stop();
                                break;
                            }
                            Some(HeartbeatCommand::AcknowledgeRevocation) => {
                                // Not applicable to the classic protocol — ignore.
                            }
                        }
                    }
                }
            }

            debug!("Heartbeat task ended for group '{}'", group_id);
        });
    }

    /// Stop the background heartbeat task.
    pub async fn stop_heartbeat_task(&self) {
        let tx = self.heartbeat_cmd_tx.write().await.take();
        if let Some(tx) = tx {
            let _ = tx.send(HeartbeatCommand::Stop).await;
        }
        self.heartbeat_controller.stop();
    }

    /// Trigger a rejoin.
    pub async fn trigger_rejoin(&self) {
        *self.state.write().await = GroupState::PreparingRebalance;
        let tx = self.heartbeat_cmd_tx.read().await.clone();
        if let Some(tx) = tx {
            let _ = tx.send(HeartbeatCommand::Rejoin).await;
        }
    }

    /// Signal the heartbeat task to send an immediate full heartbeat with
    /// the current owned partitions, acknowledging a revocation (KIP-848).
    pub async fn acknowledge_revocation(&self) {
        let tx = self.heartbeat_cmd_tx.read().await.clone();
        if let Some(tx) = tx {
            let _ = tx.send(HeartbeatCommand::AcknowledgeRevocation).await;
        }
    }

    /// Mark state as PreparingRebalance without stopping the heartbeat task.
    /// Used when we want the next poll to re-enter rebalance but need the
    /// background heartbeat to keep running (e.g., round-limit deferral).
    pub async fn set_preparing_rebalance(&self) {
        *self.state.write().await = GroupState::PreparingRebalance;
    }

    /// Record owned partitions in the sticky assignor for the next rebalance.
    /// The poll loop calls this after applying revocations or finalizing assignment
    /// so that the next join_group metadata reports the correct owned state.
    pub fn record_owned_partitions(&self, member_id: &str, assignment: &MemberAssignment) {
        self.sticky_assignor
            .record_assignment(member_id, assignment);
    }

    /// Send a KIP-848 ConsumerGroupHeartbeat (API key 68).
    ///
    /// This is the sole membership and assignment API for the new consumer
    /// protocol. It replaces JoinGroup + SyncGroup + Heartbeat + LeaveGroup.
    ///
    /// - `member_epoch = 0` → join the group
    /// - `member_epoch = -1` → leave the group
    /// - `member_epoch = -2` → static member temporary leave
    ///
    /// Returns the decoded response. The caller is responsible for updating
    /// local state (member_epoch, assignment, heartbeat interval) from the
    /// response.
    pub async fn consumer_group_heartbeat(
        &self,
        subscribed_topic_names: Option<Vec<String>>,
        topic_partitions: Option<Vec<ConsumerGroupTopicPartitions>>,
    ) -> Result<ConsumerGroupHeartbeatResponse> {
        let conn = self.get_coordinator_connection().await?;

        // KIP-1082 (v1+): member ID must be client-generated. Generate a
        // UUID on the first heartbeat and persist it for the member lifetime.
        // Use a single write lock to avoid a TOCTOU race where two concurrent
        // callers could both see an empty ID and both generate a UUID.
        {
            let mut id = self.member_id.write().await;
            if id.is_empty() {
                *id = crate::util::random_uuid_v4();
            }
        }
        let member_id = self.member_id.read().await.clone();
        let member_epoch = *self.member_epoch.read().await;

        let request = ConsumerGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: member_id.clone(),
            member_epoch,
            instance_id: self.group_instance_id.clone(),
            rack_id: None,
            rebalance_timeout_ms: crate::util::duration_to_millis_i32(self.rebalance_timeout),
            subscribed_topic_names,
            subscribed_topic_regex: None,
            server_assignor: None,
            topic_partitions,
        };

        debug!(
            "Sending ConsumerGroupHeartbeat for group '{}': member_id='{}', epoch={}",
            self.group_id, member_id, member_epoch
        );

        let Some(hb_version) = conn
            .negotiate_api_version(
                ApiKey::ConsumerGroupHeartbeat,
                CONSUMER_GROUP_HEARTBEAT_MAX,
                CONSUMER_GROUP_HEARTBEAT_MIN,
            )
            .await
        else {
            return Err(KrafkaError::protocol(
                "ConsumerGroupHeartbeat is unsupported by the broker; \
                 KIP-848/GroupProtocol::Consumer cannot be used on this cluster",
            ));
        };

        let response = conn
            .send_request(ApiKey::ConsumerGroupHeartbeat, hb_version, |buf| {
                request.encode_versioned(hb_version, buf)
            })
            .await?;

        let mut buf = response;
        let hb_response = ConsumerGroupHeartbeatResponse::decode_versioned(hb_version, &mut buf)?;

        if !hb_response.error_code.is_ok() {
            // StaleMemberEpoch: our epoch is behind. The response carries the
            // correct epoch — update local state and fall through to the
            // normal state-update path so the next heartbeat uses the fresh
            // epoch. This is recoverable and should not be surfaced as an error.
            if hb_response.error_code == ErrorCode::StaleMemberEpoch {
                debug!(
                    "ConsumerGroupHeartbeat StaleMemberEpoch for group '{}' — \
                     updating epoch to {}",
                    self.group_id, hb_response.member_epoch
                );
                *self.member_epoch.write().await = hb_response.member_epoch;
                // Fall through — the rest of the method updates member_id,
                // assignment, etc. from this same response.
            } else {
                // Handle fencing and unknown member errors
                if hb_response.error_code == ErrorCode::UnknownMemberId
                    || hb_response.error_code == ErrorCode::FencedMemberEpoch
                    || hb_response.error_code == ErrorCode::UnreleasedInstanceId
                {
                    warn!(
                        "ConsumerGroupHeartbeat error for group '{}': {:?} — resetting member state",
                        self.group_id, hb_response.error_code
                    );
                    if self.is_consumer_protocol() {
                        // KIP-848: preserve member_id for re-registration.
                        *self.member_epoch.write().await = 0;
                    } else {
                        self.reset_member_identity().await;
                    }
                }
                return Err(KrafkaError::broker(
                    hb_response.error_code,
                    format!(
                        "ConsumerGroupHeartbeat failed: {}",
                        hb_response
                            .error_message
                            .as_deref()
                            .unwrap_or("unknown error")
                    ),
                ));
            }
        }

        // Update member state from the response
        if let Some(ref new_member_id) = hb_response.member_id {
            let old = self.member_id.read().await.clone();
            if old != *new_member_id {
                if !old.is_empty() {
                    self.sticky_assignor.clear_member(&old);
                }
                *self.member_id.write().await = new_member_id.clone();
            }
        }
        *self.member_epoch.write().await = hb_response.member_epoch;

        // Update assignment if the coordinator provided one
        if let Some(ref assignment) = hb_response.assignment {
            // Store the raw target for re-resolution on future metadata refreshes.
            *self.target_assignment.write().await = assignment.topic_partitions.clone();

            let (new_assignment, has_unresolved) = Self::resolve_assignment(
                &self.metadata,
                &self.topic_names_cache,
                &assignment.topic_partitions,
            )
            .await;
            *self.assignment.write().await = new_assignment;
            *self.state.write().await = GroupState::Stable;

            if has_unresolved {
                debug!(
                    "Triggering metadata refresh to resolve unresolved topic UUIDs for group '{}'",
                    self.group_id
                );
                if let Err(e) = self.metadata.refresh().await {
                    warn!(
                        "Metadata refresh for UUID resolution failed for group '{}': {}",
                        self.group_id, e
                    );
                    // Continue with stale metadata — the background heartbeat
                    // task re-resolves UUIDs on every tick, so unresolved
                    // partitions will be picked up on the next successful
                    // metadata refresh.
                }
                // Re-resolve after refresh. If topic UUIDs are still
                // unresolved, KIP-848 cannot operate because UUID→name
                // mappings require Metadata v10+.
                // Fail fast with a clear error rather
                // than silently keeping an empty/partial assignment.
                let target = self.target_assignment.read().await.clone();
                let (resolved, still_unresolved) =
                    Self::resolve_assignment(&self.metadata, &self.topic_names_cache, &target)
                        .await;
                *self.assignment.write().await = resolved;

                if still_unresolved {
                    return Err(KrafkaError::protocol(
                        "ConsumerGroupHeartbeat assignment contains topic UUIDs that could not \
                         be resolved after metadata refresh. KIP-848 requires Metadata v10+ \
                         to map topic IDs to names.",
                    ));
                }
            }
        }

        info!(
            "ConsumerGroupHeartbeat OK for group '{}': member_id='{}', epoch={}, interval={}ms",
            self.group_id,
            hb_response.member_id.as_deref().unwrap_or(""),
            hb_response.member_epoch,
            hb_response.heartbeat_interval_ms
        );

        Ok(hb_response)
    }

    /// Resolve topic UUIDs from a heartbeat assignment to topic names.
    ///
    /// Resolution order (mirrors the Java client's two-level lookup):
    /// 1. Cluster metadata cache (populated from metadata v10+ responses).
    /// 2. Local topic names cache (survives metadata cache flushes).
    ///
    /// Successfully resolved names are inserted into `topic_names_cache`.
    /// Returns `(assignment, has_unresolved)`. When `has_unresolved` is
    /// `true`, the caller should trigger a metadata refresh and store the
    /// raw target assignment for later re-resolution.
    async fn resolve_assignment(
        metadata: &Arc<ClusterMetadata>,
        topic_names_cache: &Arc<RwLock<HashMap<[u8; 16], String>>>,
        topic_partitions: &[ConsumerGroupTopicPartitions],
    ) -> (MemberAssignment, bool) {
        let mut assignment = MemberAssignment::empty();
        let mut has_unresolved = false;
        let mut cache = topic_names_cache.write().await;
        for tp in topic_partitions {
            // 1. Try the global metadata cache.
            if let Some(name) = metadata.topic_name_for_id(&tp.topic_id) {
                cache.insert(tp.topic_id, name.clone());
                assignment.add(name, tp.partitions.clone());
                continue;
            }
            // 2. Fallback to the local names cache.
            if let Some(name) = cache.get(&tp.topic_id) {
                assignment.add(name.clone(), tp.partitions.clone());
                continue;
            }
            warn!(
                "Cannot resolve topic UUID {:02x?} to a name — \
                 will retry after next metadata refresh. \
                 Partitions {:?} skipped for now.",
                tp.topic_id, tp.partitions
            );
            has_unresolved = true;
        }
        (assignment, has_unresolved)
    }

    /// Ensure active membership using the KIP-848 consumer protocol.
    ///
    /// For the initial join, sends a heartbeat with epoch 0 and subscribed
    /// topics. For subsequent heartbeats, sends the current epoch.
    async fn ensure_active_membership_consumer(
        &self,
        topics: &[String],
    ) -> Result<(MemberAssignment, bool)> {
        let state = *self.state.read().await;

        let new_topics = topics.to_vec();
        match state {
            GroupState::Stable => {
                // Already stable — check if topics changed
                let old_topics = self.subscribed_topics.read().await.clone();
                let mut old_sorted = old_topics;
                old_sorted.sort();
                let mut new_sorted = new_topics.clone();
                new_sorted.sort();
                if old_sorted == new_sorted {
                    return Ok((self.assignment.read().await.clone(), false));
                }
                // Topics changed — send heartbeat with new subscription
            }
            GroupState::Unjoined => {
                // Need to find coordinator first
                if self.coordinator_conn.read().await.is_none() {
                    self.find_coordinator().await?;
                }
            }
            GroupState::Leaving | GroupState::Dead => {
                return Err(KrafkaError::invalid_state(format!(
                    "Cannot send consumer heartbeat: group state is {state:?}"
                )));
            }
            // PreparingRebalance / Joining / AwaitingSync: proceed to
            // send a heartbeat — for KIP-848, heartbeat is the sole
            // communication channel and sending one is always valid.
            _ => {}
        }

        let subscribed = Some(new_topics.clone());
        self.set_subscribed_topics(new_topics).await;

        let resp = self.consumer_group_heartbeat(subscribed, None).await?;

        // Start heartbeat task for KIP-848
        self.start_consumer_heartbeat_task(resp.heartbeat_interval_ms)
            .await;

        let joined = matches!(*self.state.read().await, GroupState::Stable);
        let assignment = self.assignment.read().await.clone();
        Ok((assignment, joined))
    }

    /// Start a background heartbeat task for the KIP-848 consumer protocol.
    ///
    /// Unlike the classic protocol, the KIP-848 heartbeat is the sole
    /// communication channel — it carries assignment updates and error codes.
    async fn start_consumer_heartbeat_task(&self, interval_ms: i32) {
        // Stop existing task if any
        self.stop_heartbeat_task().await;
        self.heartbeat_controller.take_rebalance_needed();
        self.heartbeat_controller.take_member_invalidated();

        let (cmd_tx, mut cmd_rx) = mpsc::channel::<HeartbeatCommand>(10);
        *self.heartbeat_cmd_tx.write().await = Some(cmd_tx);

        let group_id = self.group_id.clone();
        let interval = Duration::from_millis(interval_ms.max(1000) as u64);
        let heartbeat_controller = self.heartbeat_controller.clone();
        let member_id_ref = self.member_id.clone();
        let member_epoch_ref = self.member_epoch.clone();
        let coordinator_conn_ref = self.coordinator_conn.clone();
        let group_instance_id = self.group_instance_id.clone();
        let assignment_ref = self.assignment.clone();
        let state_ref = self.state.clone();
        let metadata_ref = self.metadata.clone();
        let target_assignment_ref = self.target_assignment.clone();
        let topic_names_cache_ref = self.topic_names_cache.clone();
        let subscribed_topics_snapshot = self.subscribed_topics.read().await.clone();
        let rebalance_timeout = self.rebalance_timeout;

        tokio::spawn(async move {
            debug!(
                "Starting KIP-848 heartbeat task for group '{}' (interval={:?})",
                group_id, interval
            );

            // Negotiate the ConsumerGroupHeartbeat version once at task start.
            // Only mark the controller as running after successful negotiation
            // so that early-return paths don't leave it stuck in a running state.
            let hb_version = {
                let coordinator_conn = coordinator_conn_ref.read().await.clone();
                if let Some(ref conn) = coordinator_conn {
                    match conn
                        .negotiate_api_version(
                            ApiKey::ConsumerGroupHeartbeat,
                            CONSUMER_GROUP_HEARTBEAT_MAX,
                            CONSUMER_GROUP_HEARTBEAT_MIN,
                        )
                        .await
                    {
                        Some(v) => v,
                        None => {
                            error!(
                                "ConsumerGroupHeartbeat unsupported by broker; \
                                 KIP-848 heartbeat task for group '{}' cannot run",
                                group_id
                            );
                            return;
                        }
                    }
                } else {
                    error!(
                        "No coordinator connection for KIP-848 heartbeat task (group '{}')",
                        group_id
                    );
                    return;
                }
            };

            heartbeat_controller.start();

            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            let mut current_interval_ms = interval.as_millis() as i32;
            // KIP-848 spec: "The member must set all (top-level) fields when
            // it joins for the first time or when an error/timeout occurs."
            // Start `true` so the very first tick sends a full heartbeat.
            let mut send_full_heartbeat = true;

            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        if !heartbeat_controller.is_running() {
                            break;
                        }

                        let coordinator_conn = coordinator_conn_ref.read().await.clone();
                        let member_id = member_id_ref.read().await.clone();
                        let epoch = *member_epoch_ref.read().await;

                        if let Some(ref conn) = coordinator_conn {
                            // Build owned-partition snapshot from target
                            // assignment for revocation acknowledgment.
                            let owned_partitions = {
                                let ta = target_assignment_ref.read().await;
                                if ta.is_empty() { None } else { Some(ta.clone()) }
                            };

                            let (sub_names, rebal_timeout_ms, topic_parts) = if send_full_heartbeat {
                                (
                                    Some(subscribed_topics_snapshot.clone()),
                                    crate::util::duration_to_millis_i32(rebalance_timeout),
                                    owned_partitions,
                                )
                            } else {
                                (None, -1, None)
                            };

                            let request = ConsumerGroupHeartbeatRequest {
                                group_id: group_id.clone(),
                                member_id,
                                member_epoch: epoch,
                                instance_id: group_instance_id.clone(),
                                rack_id: None,
                                rebalance_timeout_ms: rebal_timeout_ms,
                                subscribed_topic_names: sub_names,
                                subscribed_topic_regex: None,
                                server_assignor: None,
                                topic_partitions: topic_parts,
                            };

                            match conn.send_request(
                                ApiKey::ConsumerGroupHeartbeat,
                                hb_version,
                                |buf| request.encode_versioned(hb_version, buf),
                            ).await {
                                Ok(response_bytes) => {
                                    let mut buf = response_bytes;
                                    match ConsumerGroupHeartbeatResponse::decode_versioned(hb_version, &mut buf) {
                                        Ok(resp) => {
                                            if resp.error_code.is_ok() {
                                                *member_epoch_ref.write().await = resp.member_epoch;

                                                // Update assignment if the coordinator sent one.
                                                if let Some(ref new_assign) = resp.assignment {
                                                    *target_assignment_ref.write().await =
                                                        new_assign.topic_partitions.clone();
                                                    let (resolved, has_unresolved) =
                                                        Self::resolve_assignment(
                                                            &metadata_ref,
                                                            &topic_names_cache_ref,
                                                            &new_assign.topic_partitions,
                                                        )
                                                        .await;
                                                    *assignment_ref.write().await = resolved;
                                                    *state_ref.write().await = GroupState::Stable;

                                                    // Signal rebalance so the Consumer layer
                                                    // picks up the new assignment, fires
                                                    // callbacks, and starts fetching.
                                                    heartbeat_controller.signal_rebalance();

                                                    if has_unresolved {
                                                        debug!(
                                                            "Triggering metadata refresh for unresolved UUIDs in group '{}'",
                                                            group_id
                                                        );
                                                        if let Err(e) = metadata_ref.refresh().await {
                                                            warn!(
                                                                "Metadata refresh for UUID resolution failed for group '{}': {}",
                                                                group_id, e
                                                            );
                                                        }
                                                        // Re-resolve with updated metadata.
                                                        let target = target_assignment_ref.read().await.clone();
                                                        let (re_resolved, still_unresolved) =
                                                            Self::resolve_assignment(
                                                                &metadata_ref,
                                                                &topic_names_cache_ref,
                                                                &target,
                                                            )
                                                            .await;
                                                        *assignment_ref.write().await = re_resolved;

                                                        if still_unresolved {
                                                            warn!(
                                                                "KIP-848 topic UUIDs still unresolved after metadata refresh \
                                                                 for group '{}'. Metadata v10+ is required to map topic IDs \
                                                                 to names.",
                                                                group_id
                                                            );
                                                        }
                                                    }
                                                } else {
                                                    // No new assignment — re-resolve target in
                                                    // case a metadata refresh filled in UUIDs.
                                                    let target = target_assignment_ref.read().await.clone();
                                                    if !target.is_empty() {
                                                        let (resolved, still_unresolved) =
                                                            Self::resolve_assignment(
                                                                &metadata_ref,
                                                                &topic_names_cache_ref,
                                                                &target,
                                                            )
                                                            .await;
                                                        *assignment_ref.write().await = resolved;

                                                        if still_unresolved {
                                                            warn!(
                                                                "KIP-848 topic UUIDs still unresolved \
                                                                 for group '{}'. Metadata v10+ is required \
                                                                 to map topic IDs to names.",
                                                                group_id
                                                            );
                                                        }
                                                    }
                                                }

                                                heartbeat_controller.heartbeat_success().await;
                                                send_full_heartbeat = false;

                                                // Update interval if the coordinator changed it.
                                                let new_ms = resp.heartbeat_interval_ms.max(1000);
                                                if new_ms != current_interval_ms {
                                                    debug!(
                                                        "KIP-848 heartbeat interval changed for '{}': {}ms → {}ms",
                                                        group_id, current_interval_ms, new_ms
                                                    );
                                                    current_interval_ms = new_ms;
                                                    let new_dur = Duration::from_millis(new_ms as u64);
                                                    tick = tokio::time::interval(new_dur);
                                                    tick.set_missed_tick_behavior(
                                                        tokio::time::MissedTickBehavior::Delay,
                                                    );
                                                    // Consume the immediate first tick.
                                                    tick.tick().await;
                                                }
                                            } else if resp.error_code == ErrorCode::RebalanceInProgress {
                                                send_full_heartbeat = true;
                                                heartbeat_controller.signal_rebalance();
                                            } else if resp.error_code == ErrorCode::StaleMemberEpoch {
                                                // Stale epoch: our epoch is behind.
                                                // The coordinator includes the current
                                                // epoch in the response, so update local
                                                // state before the next heartbeat to
                                                // avoid retrying indefinitely with a
                                                // stale value.
                                                *member_epoch_ref.write().await = resp.member_epoch;
                                                debug!(
                                                    "KIP-848 StaleMemberEpoch for '{}' — \
                                                     updated epoch to {}, will retry on next heartbeat",
                                                    group_id, resp.member_epoch
                                                );
                                                send_full_heartbeat = true;
                                                heartbeat_controller.heartbeat_success().await;
                                            } else if resp.error_code == ErrorCode::UnknownMemberId
                                                || resp.error_code == ErrorCode::FencedMemberEpoch
                                                || resp.error_code == ErrorCode::UnreleasedInstanceId
                                            {
                                                warn!(
                                                    "KIP-848 heartbeat error for '{}': {:?}",
                                                    group_id, resp.error_code
                                                );
                                                heartbeat_controller.signal_member_invalidated();
                                                heartbeat_controller.signal_rebalance();
                                                // Stop the task — the consumer poll loop will
                                                // detect the fencing via needs_rejoin(), perform
                                                // a KIP-848 fencing reset, and restart the task
                                                // with a full heartbeat (all top-level fields)
                                                // via ensure_active_membership().
                                                break;
                                            } else if resp.error_code == ErrorCode::UnsupportedAssignor
                                            {
                                                warn!(
                                                    "KIP-848 unsupported assignor for '{}': {:?}",
                                                    group_id, resp.error_message
                                                );
                                                send_full_heartbeat = true;
                                                heartbeat_controller.signal_rebalance();
                                            } else if resp.error_code
                                                == ErrorCode::InvalidRegularExpression
                                            {
                                                error!(
                                                    "KIP-848 invalid regex subscription for '{}': {:?}",
                                                    group_id, resp.error_message
                                                );
                                                // Fatal configuration error — don't retry.
                                                break;
                                            } else if resp.error_code == ErrorCode::NotCoordinator
                                                || resp.error_code
                                                    == ErrorCode::CoordinatorNotAvailable
                                            {
                                                warn!(
                                                    "KIP-848 coordinator stale for '{}': {:?}",
                                                    group_id, resp.error_code
                                                );
                                                // Clear cached coordinator so the next
                                                // get_coordinator_connection() triggers
                                                // rediscovery.
                                                *coordinator_conn_ref.write().await = None;
                                                heartbeat_controller.signal_rebalance();
                                                // Stop the task — the consumer poll loop
                                                // will rediscover the coordinator and
                                                // restart the task via
                                                // ensure_active_membership().
                                                break;
                                            } else if resp.error_code
                                                == ErrorCode::CoordinatorLoadInProgress
                                            {
                                                // Transient: coordinator is loading state.
                                                // Keep the connection and retry on the
                                                // next heartbeat tick.
                                                send_full_heartbeat = true;
                                                debug!(
                                                    "KIP-848 coordinator loading for '{}', will retry",
                                                    group_id
                                                );
                                            } else {
                                                send_full_heartbeat = true;
                                                warn!(
                                                    "KIP-848 heartbeat error for '{}': {:?}",
                                                    group_id, resp.error_code
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            send_full_heartbeat = true;
                                            warn!(
                                                "Failed to decode KIP-848 heartbeat response for '{}': {}",
                                                group_id, e
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!(
                                        "Failed to send KIP-848 heartbeat for '{}': {}",
                                        group_id, e
                                    );
                                    // Network error — the coordinator connection
                                    // may be dead. Clear it and exit the heartbeat
                                    // loop so the consumer poll loop can rediscover
                                    // the coordinator and rejoin via
                                    // ensure_active_membership().
                                    *coordinator_conn_ref.write().await = None;
                                    heartbeat_controller.signal_rebalance();
                                    break;
                                }
                            }
                        }
                    }
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(HeartbeatCommand::Stop) | None => break,
                            Some(HeartbeatCommand::Rejoin) => break,
                            Some(HeartbeatCommand::AcknowledgeRevocation) => {
                                // KIP-848 §revocation-ack: after the consumer
                                // layer processes revocations, send an immediate
                                // heartbeat with the updated owned partitions so
                                // the coordinator can proceed.
                                send_full_heartbeat = true;
                                tick.reset();
                                // The next tick fires immediately because we
                                // just reset the interval, which means the
                                // loop will go around and send the full HB.
                            }
                        }
                    }
                }
            }

            heartbeat_controller.stop();
            debug!("KIP-848 heartbeat task ended for group '{}'", group_id);
        });
    }

    /// Send a single heartbeat (for inline heartbeat during poll).
    pub async fn send_heartbeat(&self) -> Result<HeartbeatStatus> {
        let conn = self.get_coordinator_connection().await?;
        let member_id = self.member_id.read().await.clone();
        let generation_id = *self.generation_id.read().await;

        let request = HeartbeatRequest {
            group_id: self.group_id.clone(),
            generation_id,
            member_id,
            group_instance_id: self.group_instance_id.clone(),
        };

        // Negotiate heartbeat version with broker (MIN=3, KIP-345 static membership).
        let hb_version = conn
            .negotiate_api_version(ApiKey::Heartbeat, HEARTBEAT_MAX, HEARTBEAT_MIN)
            .await
            .ok_or_else(|| {
                KrafkaError::protocol(format!(
                    "broker does not support Heartbeat v{}-v{}",
                    HEARTBEAT_MIN, HEARTBEAT_MAX,
                ))
            })?;
        let response = conn
            .send_request(ApiKey::Heartbeat, hb_version, |buf| {
                request.encode_versioned(hb_version, buf)
            })
            .await?;

        let mut buf = response;
        let hb_response = HeartbeatResponse::decode_versioned(hb_version, &mut buf)?;

        let status = HeartbeatStatus::from_error_code(hb_response.error_code);
        if status == HeartbeatStatus::Ok {
            self.heartbeat_controller.heartbeat_success().await;
        }

        Ok(status)
    }

    /// Handle inline heartbeat status by clearing member identity for
    /// session-invalidating errors before triggering a rejoin.
    ///
    /// Returns `true` if a rejoin was triggered and the caller should
    /// abort the current rebalance phase (return early from poll).
    pub async fn handle_inline_heartbeat_status(&self, status: HeartbeatStatus) -> bool {
        if status.requires_rejoin() {
            if status.is_session_invalidating() {
                self.reset_member_identity().await;
            }
            self.trigger_rejoin().await;
            true
        } else {
            false
        }
    }

    /// Commit offsets to the coordinator.
    pub async fn commit_offsets(
        &self,
        offsets: &HashMap<(String, PartitionId), (i64, Option<String>)>,
    ) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }

        // Validate state
        let state = *self.state.read().await;
        if state != GroupState::Stable {
            return Err(KrafkaError::invalid_state(format!(
                "cannot commit offsets: group state is {:?}",
                state
            )));
        }

        let conn = self.get_coordinator_connection().await?;

        let oc_version = conn
            .negotiate_api_version(ApiKey::OffsetCommit, OFFSET_COMMIT_MAX, OFFSET_COMMIT_MIN)
            .await
            .ok_or_else(|| {
                KrafkaError::protocol(format!(
                    "broker does not support OffsetCommit v{}-v{}",
                    OFFSET_COMMIT_MIN, OFFSET_COMMIT_MAX,
                ))
            })?;

        let member_id = self.member_id.read().await.clone();
        // For KIP-848 (consumer protocol), the "generation ID" wire field
        // carries the member epoch instead of the classic generation ID.
        // This semantic overload is only valid from v9+ — at earlier versions
        // the broker strictly validates against the classic group generation,
        // so we fall back to the classic generation_id.
        let generation_id = if self.is_consumer_protocol() && oc_version >= 9 {
            *self.member_epoch.read().await
        } else {
            *self.generation_id.read().await
        };

        // Group offsets by topic
        let mut topics_map: HashMap<String, Vec<OffsetCommitRequestPartition>> = HashMap::new();
        for ((topic, partition), (offset, metadata)) in offsets {
            topics_map
                .entry(topic.clone())
                .or_default()
                .push(OffsetCommitRequestPartition {
                    partition_index: *partition,
                    committed_offset: *offset,
                    committed_leader_epoch: -1,
                    commit_timestamp: -1,
                    committed_metadata: metadata.clone(),
                });
        }

        let topics: Vec<OffsetCommitRequestTopic> = topics_map
            .into_iter()
            .map(|(name, partitions)| OffsetCommitRequestTopic {
                name,
                topic_id: None,
                partitions,
            })
            .collect();

        let request = OffsetCommitRequest {
            group_id: self.group_id.clone(),
            generation_id,
            member_id,
            group_instance_id: self.group_instance_id.clone(),
            retention_time_ms: -1,
            topics,
        };

        debug!(
            "Committing {} offsets for group '{}'",
            offsets.len(),
            self.group_id
        );

        let response = conn
            .send_request(ApiKey::OffsetCommit, oc_version, |buf| {
                request.encode_versioned(oc_version, buf)
            })
            .await?;

        let mut buf = response;
        let commit_response = OffsetCommitResponse::decode_versioned(oc_version, &mut buf)?;

        // Check for errors
        for topic in &commit_response.topics {
            for partition in &topic.partitions {
                if !partition.error_code.is_ok() {
                    // For KIP-848, StaleMemberEpoch is transient — the
                    // background heartbeat task will update our epoch.
                    // Don't trigger a rebalance; let the caller retry.
                    if self.is_consumer_protocol()
                        && partition.error_code == ErrorCode::StaleMemberEpoch
                    {
                        return Err(KrafkaError::broker(
                            partition.error_code,
                            format!(
                                "Offset commit failed for {}-{}: stale epoch, retry after heartbeat",
                                topic.name, partition.partition_index
                            ),
                        ));
                    }
                    // Handle rebalance errors specially
                    if partition.error_code == ErrorCode::RebalanceInProgress
                        || partition.error_code == ErrorCode::IllegalGeneration
                        || partition.error_code == ErrorCode::UnknownMemberId
                        || partition.error_code == ErrorCode::FencedMemberEpoch
                        || partition.error_code == ErrorCode::StaleMemberEpoch
                    {
                        *self.state.write().await = GroupState::PreparingRebalance;
                        return Err(KrafkaError::broker(
                            partition.error_code,
                            format!(
                                "Offset commit failed for {}-{}: rebalance needed",
                                topic.name, partition.partition_index
                            ),
                        ));
                    }
                    // Stale coordinator — clear cached connection for rediscovery.
                    if partition.error_code == ErrorCode::NotCoordinator
                        || partition.error_code == ErrorCode::CoordinatorNotAvailable
                    {
                        *self.coordinator_conn.write().await = None;
                    }
                    return Err(KrafkaError::broker(
                        partition.error_code,
                        format!(
                            "Offset commit failed for {}-{}",
                            topic.name, partition.partition_index
                        ),
                    ));
                }
            }
        }

        info!(
            "Committed {} offsets for group '{}'",
            offsets.len(),
            self.group_id
        );
        Ok(())
    }

    /// Fetch committed offsets from the coordinator.
    ///
    /// Returns the committed offset for each topic-partition, or `None` if
    /// no offset has been committed for that partition.
    pub async fn fetch_committed_offsets(
        &self,
        partitions: &HashMap<String, Vec<crate::PartitionId>>,
    ) -> Result<HashMap<(String, crate::PartitionId), i64>> {
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }

        let conn = self.get_coordinator_connection().await?;

        let topics: Vec<OffsetFetchRequestTopic> = partitions
            .iter()
            .map(|(topic, parts)| OffsetFetchRequestTopic {
                name: topic.clone(),
                topic_id: None,
                partition_indexes: parts.clone(),
            })
            .collect();

        // Negotiate version: v0 returns UNKNOWN_TOPIC_OR_PARTITION on modern
        // brokers, so we floor at v1. At v6+ the wire switches to flexible
        // encoding, v8+ uses the batched Groups format (KIP-709), and v9
        // adds MemberId/MemberEpoch for KIP-848 epoch validation.
        let of_version = conn
            .negotiate_api_version(ApiKey::OffsetFetch, OFFSET_FETCH_MAX, OFFSET_FETCH_MIN)
            .await
            .ok_or_else(|| {
                KrafkaError::protocol(format!(
                    "broker does not support OffsetFetch v{}-v{}",
                    OFFSET_FETCH_MIN, OFFSET_FETCH_MAX,
                ))
            })?;

        // For KIP-848, populate MemberId/MemberEpoch so the broker can validate
        // membership and surface STALE_MEMBER_EPOCH when appropriate.
        // These fields only exist on the wire from v9+; at earlier versions
        // the encode path ignores them, so we leave defaults.
        let (offset_fetch_member_id, offset_fetch_member_epoch) =
            if self.is_consumer_protocol() && of_version >= 9 {
                (
                    Some(self.member_id.read().await.clone()),
                    *self.member_epoch.read().await,
                )
            } else {
                (None, -1)
            };

        let request = OffsetFetchRequest {
            group_id: self.group_id.clone(),
            topics: Some(topics),
            require_stable: false,
            member_id: offset_fetch_member_id,
            member_epoch: offset_fetch_member_epoch,
        };

        debug!(
            "Fetching committed offsets for group '{}' ({} topics)",
            self.group_id,
            partitions.len()
        );

        let response = conn
            .send_request(ApiKey::OffsetFetch, of_version, |buf| {
                request.encode_versioned(of_version, buf)
            })
            .await?;

        let mut buf = response;
        let offset_response = OffsetFetchResponse::decode_versioned(of_version, &mut buf)?;

        // Check group-level error (v2+ top-level ErrorCode, v8+ per-group ErrorCode).
        // Errors like NOT_COORDINATOR, STALE_MEMBER_EPOCH, or UNKNOWN_MEMBER_ID
        // appear here and must be surfaced before iterating partitions.
        if !offset_response.error_code.is_ok() {
            if offset_response.error_code == ErrorCode::StaleMemberEpoch
                || offset_response.error_code == ErrorCode::UnknownMemberId
                || offset_response.error_code == ErrorCode::FencedMemberEpoch
            {
                *self.state.write().await = GroupState::PreparingRebalance;
            } else if offset_response.error_code == ErrorCode::NotCoordinator
                || offset_response.error_code == ErrorCode::CoordinatorNotAvailable
            {
                // Stale coordinator — clear the cached connection so the next
                // call to get_coordinator_connection() triggers rediscovery.
                *self.coordinator_conn.write().await = None;
            }
            return Err(KrafkaError::broker(
                offset_response.error_code,
                format!("OffsetFetch failed for group '{}'", self.group_id),
            ));
        }

        let mut result = HashMap::new();
        for topic in &offset_response.topics {
            for partition in &topic.partitions {
                if partition.error_code.is_ok() && partition.committed_offset >= 0 {
                    result.insert(
                        (topic.name.clone(), partition.partition_index),
                        partition.committed_offset,
                    );
                }
            }
        }

        info!(
            "Fetched {} committed offsets for group '{}'",
            result.len(),
            self.group_id
        );
        Ok(result)
    }

    /// List offsets (earliest/latest) for the given partitions.
    ///
    /// `timestamp` should be -1 for latest or -2 for earliest.
    pub async fn list_offsets(
        &self,
        partitions: &HashMap<String, Vec<crate::PartitionId>>,
        timestamp: i64,
    ) -> Result<HashMap<(String, crate::PartitionId), i64>> {
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }

        // We need to send ListOffsets to the leader of each partition
        let mut result = HashMap::new();

        // Group by leader
        let mut partitions_by_leader: HashMap<crate::BrokerId, Vec<(String, crate::PartitionId)>> =
            HashMap::new();
        let mut leaderless: Vec<(String, crate::PartitionId)> = Vec::new();
        for (topic, parts) in partitions {
            for &partition in parts {
                if let Some(leader_id) = self.metadata.leader(topic, partition) {
                    partitions_by_leader
                        .entry(leader_id)
                        .or_default()
                        .push((topic.clone(), partition));
                } else {
                    leaderless.push((topic.clone(), partition));
                }
            }
        }

        // Warn about leaderless partitions and try after a metadata refresh
        if !leaderless.is_empty() {
            warn!(
                "No leader found for {} partition(s), refreshing metadata: {:?}",
                leaderless.len(),
                leaderless
            );
            let topics: Vec<&str> = leaderless.iter().map(|(t, _)| t.as_str()).collect();
            if let Err(refresh_err) = self.metadata.refresh_for_topics(Some(&topics)).await {
                debug!(error = %refresh_err, "Metadata refresh failed for leaderless partitions");
            }

            // Retry resolution after refresh
            for (topic, partition) in leaderless {
                if let Some(leader_id) = self.metadata.leader(&topic, partition) {
                    partitions_by_leader
                        .entry(leader_id)
                        .or_default()
                        .push((topic, partition));
                } else {
                    warn!(
                        "Still no leader for {}-{} after metadata refresh, skipping",
                        topic, partition
                    );
                }
            }
        }

        for (leader_id, leader_partitions) in &partitions_by_leader {
            // Group partitions by topic
            let mut topics_map: HashMap<String, Vec<ListOffsetsRequestPartition>> = HashMap::new();
            for (topic, partition) in leader_partitions {
                topics_map
                    .entry(topic.clone())
                    .or_default()
                    .push(ListOffsetsRequestPartition {
                        partition_index: *partition,
                        current_leader_epoch: -1,
                        timestamp,
                    });
            }

            let topics: Vec<ListOffsetsRequestTopic> = topics_map
                .into_iter()
                .map(|(name, parts)| ListOffsetsRequestTopic {
                    name,
                    partitions: parts,
                })
                .collect();

            let request = ListOffsetsRequest {
                replica_id: -1,
                isolation_level: self.isolation_level,
                topics,
                timeout_ms: None,
            };

            // Get connection to this leader directly by ID
            let conn = self.metadata.get_broker_connection(*leader_id).await?;

            let lo_version = conn
                .negotiate_api_version(ApiKey::ListOffsets, LIST_OFFSETS_MAX, LIST_OFFSETS_MIN)
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol(format!(
                        "broker does not support ListOffsets v{}-v{}",
                        LIST_OFFSETS_MIN, LIST_OFFSETS_MAX,
                    ))
                })?;
            let response = conn
                .send_request(ApiKey::ListOffsets, lo_version, |buf| {
                    request.encode_versioned(lo_version, buf)
                })
                .await?;

            let mut buf = response;
            let list_response = ListOffsetsResponse::decode_versioned(lo_version, &mut buf)?;

            for topic_resp in &list_response.topics {
                for part_resp in &topic_resp.partitions {
                    if part_resp.error_code.is_ok() {
                        result.insert(
                            (topic_resp.name.clone(), part_resp.partition_index),
                            part_resp.offset,
                        );
                    } else {
                        // Log partition-level errors instead of silently
                        // dropping them. Callers should handle missing partitions.
                        warn!(
                            "ListOffsets error for {}-{}: {:?}",
                            topic_resp.name, part_resp.partition_index, part_resp.error_code
                        );
                    }
                }
            }
        }

        Ok(result)
    }

    /// Leave the consumer group.
    pub async fn leave_group(&self) -> Result<()> {
        let state = *self.state.read().await;
        if state == GroupState::Unjoined || state == GroupState::Dead {
            return Ok(());
        }

        // KIP-848: stop heartbeat first (prevent normal heartbeat from
        // racing with the leave-epoch heartbeat), then send the leave.
        if self.is_consumer_protocol() {
            self.stop_heartbeat_task().await;
            return self.leave_group_consumer().await;
        }

        // Classic protocol: send LeaveGroup while heartbeat still keeps the
        // member alive on the broker, then stop heartbeat afterward.
        let conn = match self.get_coordinator_connection().await {
            Ok(c) => c,
            Err(_) => {
                // If we can't get a connection, just stop heartbeat and reset state
                self.stop_heartbeat_task().await;
                self.reset().await;
                return Ok(());
            }
        };

        let member_id = self.member_id.read().await.clone();

        *self.state.write().await = GroupState::Leaving;

        // v3+ uses only the `members` array; the top-level `member_id`
        // must be empty to avoid ambiguous single-vs-batch leave semantics.
        let request = LeaveGroupRequest {
            group_id: self.group_id.clone(),
            member_id: String::new(),
            members: vec![LeaveGroupMember {
                member_id: member_id.clone(),
                group_instance_id: self.group_instance_id.clone(),
                reason: None,
            }],
        };

        debug!(
            "Leaving group '{}', member_id='{}'",
            self.group_id, member_id
        );

        // Send leave group request (don't wait too long)
        // Negotiate version with broker (MIN=3, KIP-345 batch leave).
        let lg_version = conn
            .negotiate_api_version(ApiKey::LeaveGroup, LEAVE_GROUP_MAX, LEAVE_GROUP_MIN)
            .await
            .ok_or_else(|| {
                KrafkaError::protocol(format!(
                    "broker does not support LeaveGroup v{}-v{}",
                    LEAVE_GROUP_MIN, LEAVE_GROUP_MAX,
                ))
            })?;
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            conn.send_request(ApiKey::LeaveGroup, lg_version, |buf| {
                request.encode_versioned(lg_version, buf)
            }),
        )
        .await;

        // Decode the response and check for errors
        match result {
            Ok(Ok(response_bytes)) => {
                let mut buf = response_bytes;
                let decode_result = LeaveGroupResponse::decode_versioned(lg_version, &mut buf);
                match decode_result {
                    Ok(r) if r.error_code.is_ok() => {
                        // Check per-member errors (v3 batch leave)
                        for member in &r.members {
                            if !member.error_code.is_ok() {
                                warn!(
                                    "LeaveGroup per-member error for '{}' (member '{}'): {:?}",
                                    self.group_id, member.member_id, member.error_code
                                );
                            }
                        }
                        info!("Left group '{}'", self.group_id);
                    }
                    Ok(r) => {
                        warn!(
                            "LeaveGroup error for '{}': {:?}",
                            self.group_id, r.error_code
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to decode LeaveGroup response for '{}': {}",
                            self.group_id, e
                        );
                    }
                }
            }
            Ok(Err(e)) => {
                warn!(
                    "Failed to send LeaveGroup request for '{}': {}",
                    self.group_id, e
                );
            }
            Err(_) => {
                warn!("LeaveGroup request timed out for '{}'", self.group_id);
            }
        }

        self.stop_heartbeat_task().await;
        self.reset().await;
        Ok(())
    }

    /// Leave the group using the KIP-848 consumer protocol.
    ///
    /// Sends a ConsumerGroupHeartbeat with `member_epoch = -1` for dynamic
    /// members (permanent leave) or `-2` for static members (temporary leave,
    /// broker keeps assignment for session-timeout window so the instance can
    /// rejoin quickly).
    async fn leave_group_consumer(&self) -> Result<()> {
        let conn = match self.get_coordinator_connection().await {
            Ok(c) => c,
            Err(_) => {
                self.reset().await;
                return Ok(());
            }
        };

        // KIP-848: -1 = permanent leave, -2 = static-member temporary leave.
        let leave_epoch: i32 = if self.group_instance_id.is_some() {
            -2
        } else {
            -1
        };

        let member_id = self.member_id.read().await.clone();
        *self.state.write().await = GroupState::Leaving;
        *self.member_epoch.write().await = leave_epoch;

        let request = ConsumerGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: member_id.clone(),
            member_epoch: leave_epoch,
            instance_id: self.group_instance_id.clone(),
            rack_id: None,
            rebalance_timeout_ms: -1,
            subscribed_topic_names: None,
            subscribed_topic_regex: None,
            server_assignor: None,
            topic_partitions: None,
        };

        debug!(
            "Leaving group '{}' via KIP-848 heartbeat, member_id='{}', epoch={}",
            self.group_id, member_id, leave_epoch
        );

        let Some(hb_version) = conn
            .negotiate_api_version(
                ApiKey::ConsumerGroupHeartbeat,
                CONSUMER_GROUP_HEARTBEAT_MAX,
                CONSUMER_GROUP_HEARTBEAT_MIN,
            )
            .await
        else {
            warn!(
                "ConsumerGroupHeartbeat unsupported; cannot send KIP-848 leave for '{}'",
                self.group_id
            );
            return Ok(());
        };

        let result = tokio::time::timeout(
            Duration::from_secs(5),
            conn.send_request(ApiKey::ConsumerGroupHeartbeat, hb_version, |buf| {
                request.encode_versioned(hb_version, buf)
            }),
        )
        .await;

        match result {
            Ok(Ok(response_bytes)) => {
                let mut buf = response_bytes;
                match ConsumerGroupHeartbeatResponse::decode_versioned(hb_version, &mut buf) {
                    Ok(resp) if resp.error_code.is_ok() => {
                        info!("Left group '{}' via KIP-848", self.group_id);
                    }
                    Ok(resp) => {
                        warn!(
                            "KIP-848 LeaveGroup error for '{}': {:?}",
                            self.group_id, resp.error_code
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to decode KIP-848 leave response for '{}': {}",
                            self.group_id, e
                        );
                    }
                }
            }
            Ok(Err(e)) => {
                warn!(
                    "Failed to send KIP-848 leave for '{}': {}",
                    self.group_id, e
                );
            }
            Err(_) => {
                warn!("KIP-848 leave request timed out for '{}'", self.group_id);
            }
        }

        self.reset().await;
        Ok(())
    }

    /// Reset coordinator state.
    async fn reset(&self) {
        self.reset_member_identity().await;
        *self.state.write().await = GroupState::Unjoined;
        *self.assignment.write().await = MemberAssignment::empty();
        self.target_assignment.write().await.clear();
        self.topic_names_cache.write().await.clear();
        *self.coordinator_conn.write().await = None;
        *self.coordinator_id.write().await = None;
    }

    /// Reset group state for KIP-848 fencing errors (FencedMemberEpoch,
    /// UnknownMemberId, UnreleasedInstanceId).
    ///
    /// Unlike [`reset_member_identity`], this preserves `member_id`:
    /// KIP-848 requires fenced members to "rejoin with the same member id
    /// and epoch 0".  Assignment and target state are cleared because the
    /// coordinator revoked all partitions on fencing.
    async fn reset_for_kip848_fencing(&self) {
        *self.member_epoch.write().await = 0;
        *self.generation_id.write().await = -1;
        *self.state.write().await = GroupState::Unjoined;
        *self.assignment.write().await = MemberAssignment::empty();
        self.target_assignment.write().await.clear();
        self.topic_names_cache.write().await.clear();
    }

    /// Clear member identity (member_id, generation_id) and any associated
    /// sticky assignor state.
    ///
    /// Called on session-invalidating errors (UNKNOWN_MEMBER_ID,
    /// ILLEGAL_GENERATION, session timeout) so the next join_group() sends
    /// a fresh empty member_id for re-registration.  Also called by reset()
    /// during leave_group/close to prevent orphaned previous_assignments.
    async fn reset_member_identity(&self) {
        let old = self.member_id.read().await.clone();
        if !old.is_empty() {
            self.sticky_assignor.clear_member(&old);
        }
        *self.member_id.write().await = String::new();
        *self.generation_id.write().await = -1;
        *self.member_epoch.write().await = 0;
    }

    /// Encode consumer protocol metadata.
    ///
    /// For cooperative-sticky, encodes version 1 metadata which includes owned
    /// partitions. This allows the leader to know each member's current assignment
    /// for computing incremental revocations.
    fn encode_consumer_metadata(
        &self,
        topics: &[String],
        owned_partitions: &HashMap<String, Vec<PartitionId>>,
    ) -> Result<BytesMut> {
        let mut buf = BytesMut::new();

        if self.is_cooperative() {
            // Version 1: includes owned partitions for cooperative protocol
            buf.put_i16(1);
        } else {
            // Version 0: topics only
            buf.put_i16(0);
        }

        // Topics array — sorted for deterministic encoding so the broker
        // does not detect spurious metadata changes between generations.
        let mut sorted_topics: Vec<&String> = topics.iter().collect();
        sorted_topics.sort();
        buf.put_i32(crate::protocol::array_len_i32(sorted_topics.len())?);
        for topic in &sorted_topics {
            let topic_len = i16::try_from(topic.len()).map_err(|_| {
                KrafkaError::protocol(format!(
                    "topic name '{}' exceeds Kafka i16 length limit ({} bytes)",
                    topic,
                    topic.len()
                ))
            })?;
            buf.put_i16(topic_len);
            buf.put_slice(topic.as_bytes());
        }
        // User data (empty)
        buf.put_i32(-1);

        if self.is_cooperative() {
            // Owned partitions (version 1+) — sorted for deterministic encoding.
            let mut sorted_owned: Vec<(&String, &Vec<PartitionId>)> =
                owned_partitions.iter().collect();
            sorted_owned.sort_by_key(|(topic, _)| topic.as_str());
            buf.put_i32(crate::protocol::array_len_i32(sorted_owned.len())?);
            for (topic, partitions) in &sorted_owned {
                let topic_len = i16::try_from(topic.len()).map_err(|_| {
                    KrafkaError::protocol(format!(
                        "topic name '{}' exceeds Kafka i16 length limit",
                        topic
                    ))
                })?;
                buf.put_i16(topic_len);
                buf.put_slice(topic.as_bytes());
                let mut sorted_parts = partitions.to_vec();
                sorted_parts.sort();
                buf.put_i32(crate::protocol::array_len_i32(sorted_parts.len())?);
                for &p in &sorted_parts {
                    buf.put_i32(p);
                }
            }
        }

        Ok(buf)
    }

    /// Decode consumer protocol metadata from JoinGroup member metadata.
    ///
    /// Returns the subscribed topics and, for version >= 1, the owned partitions.
    fn decode_consumer_metadata(data: &[u8]) -> (Vec<String>, HashMap<String, Vec<PartitionId>>) {
        if data.len() < 2 {
            return (Vec::new(), HashMap::new());
        }
        let mut buf = data;

        let version = buf.get_i16();

        // Decode topics
        let mut topics = Vec::new();
        if buf.remaining() >= 4 {
            let topic_count = buf.get_i32();
            let count = topic_count.max(0) as usize;
            if count > 10_000 {
                warn!(
                    "decode_consumer_metadata: topic count {} exceeds cap, returning early",
                    count
                );
                return (topics, HashMap::new());
            }
            let safe_count = count.min(buf.remaining() / 2);
            for _ in 0..safe_count {
                if buf.remaining() < 2 {
                    return (topics, HashMap::new());
                }
                let len = buf.get_i16();
                if len < 0 || buf.remaining() < len as usize {
                    return (topics, HashMap::new());
                }
                topics.push(String::from_utf8_lossy(&buf.copy_to_bytes(len as usize)).into_owned());
            }
        }

        // Skip user_data
        if buf.remaining() >= 4 {
            let user_data_len = buf.get_i32();
            if user_data_len > 0 {
                if buf.remaining() < user_data_len as usize {
                    return (topics, HashMap::new());
                }
                buf.advance(user_data_len as usize);
            }
        }

        // Decode owned partitions (version 1+)
        let mut owned = HashMap::new();
        if version >= 1 && buf.remaining() >= 4 {
            let topic_count = buf.get_i32();
            let count = topic_count.max(0) as usize;
            if count > 10_000 {
                warn!(
                    "decode_consumer_metadata: owned topic count {} exceeds cap, returning early",
                    count
                );
                return (topics, owned);
            }
            let safe_topic_count = count.min(buf.remaining() / 6);
            for _ in 0..safe_topic_count {
                if buf.remaining() < 2 {
                    return (topics, owned);
                }
                let len = buf.get_i16();
                if len < 0 || buf.remaining() < len as usize {
                    return (topics, owned);
                }
                let topic = String::from_utf8_lossy(&buf.copy_to_bytes(len as usize)).into_owned();
                if buf.remaining() < 4 {
                    return (topics, owned);
                }
                let part_count = buf.get_i32();
                let pcount = part_count.max(0) as usize;
                if pcount > 10_000 {
                    warn!(
                        "decode_consumer_metadata: partition count {} for '{}' exceeds cap, returning early",
                        pcount, topic
                    );
                    return (topics, owned);
                }
                let safe_part_count = pcount.min(buf.remaining() / 4);
                let mut parts = Vec::with_capacity(safe_part_count);
                for _ in 0..safe_part_count {
                    if buf.remaining() < 4 {
                        return (topics, owned);
                    }
                    parts.push(buf.get_i32());
                }
                owned.insert(topic, parts);
            }
        }

        (topics, owned)
    }

    /// Decode consumer assignment from SyncGroup response.
    fn decode_consumer_assignment(&self, data: &Bytes) -> Result<MemberAssignment> {
        if data.is_empty() {
            return Ok(MemberAssignment::empty());
        }

        let mut buf = data.clone();
        if buf.remaining() < 2 {
            return Ok(MemberAssignment::empty());
        }

        // Version
        let _version = buf.get_i16();

        // Topics array
        if buf.remaining() < 4 {
            return Ok(MemberAssignment::empty());
        }
        let topic_count = buf.get_i32();
        if topic_count < 0 {
            return Ok(MemberAssignment::empty());
        }
        // Cap iteration by max array length and remaining buffer to prevent allocation DoS
        let safe_topic_count = (topic_count as usize)
            .min(MAX_DECODE_ARRAY_LEN)
            .min(buf.remaining() / 6);
        if safe_topic_count < topic_count as usize {
            warn!(
                "assignment topic count {} exceeds buffer capacity, decoding {} topics",
                topic_count, safe_topic_count
            );
        }

        let mut assignment = MemberAssignment::empty();

        for _ in 0..safe_topic_count {
            if buf.remaining() < 2 {
                break;
            }
            let topic_len_i16 = buf.get_i16();
            if topic_len_i16 < 0 {
                break;
            }
            let topic_len = topic_len_i16 as usize;
            if buf.remaining() < topic_len {
                break;
            }
            let topic = String::from_utf8_lossy(&buf.copy_to_bytes(topic_len)).into_owned();

            if buf.remaining() < 4 {
                break;
            }
            let partition_count = buf.get_i32();
            if partition_count < 0 {
                break;
            }
            let safe_partition_count = (partition_count as usize)
                .min(MAX_DECODE_ARRAY_LEN)
                .min(buf.remaining() / 4);
            if safe_partition_count < partition_count as usize {
                warn!(
                    "assignment partition count {} for '{}' exceeds buffer/cap, decoding {}",
                    partition_count, topic, safe_partition_count
                );
            }
            let mut partitions = Vec::with_capacity(safe_partition_count);

            for _ in 0..safe_partition_count {
                if buf.remaining() < 4 {
                    break;
                }
                partitions.push(buf.get_i32());
            }

            assignment.add(topic, partitions);
        }

        Ok(assignment)
    }

    /// Compute assignments when we are the group leader.
    async fn compute_assignments(
        &self,
        topics: &[String],
        members: &[JoinGroupResponseMember],
    ) -> Result<Vec<SyncGroupRequestAssignment>> {
        // Get partition info for all topics
        let mut topic_partitions: HashMap<String, Vec<PartitionId>> = HashMap::new();
        for topic in topics {
            if let Some(topic_info) = self.metadata.topic(topic) {
                let partitions: Vec<_> =
                    topic_info.partitions.iter().map(|p| p.partition).collect();
                topic_partitions.insert(topic.clone(), partitions);
            }
        }

        // For cooperative protocol, decode member metadata to extract owned partitions
        // and feed them into the sticky assignor before computing new assignments.
        // Prune stale members first to prevent unbounded growth of previous_assignments.
        if self.is_cooperative() {
            let current_member_ids: HashSet<&str> =
                members.iter().map(|m| m.member_id.as_str()).collect();
            self.sticky_assignor.retain_members(&current_member_ids);
            for m in members {
                let (_member_topics, owned) = Self::decode_consumer_metadata(&m.metadata);
                let assignment = MemberAssignment { partitions: owned };
                self.sticky_assignor
                    .record_assignment(&m.member_id, &assignment);
            }
        }

        // Convert to GroupMember for assignor
        let group_members: Vec<GroupMember> = members
            .iter()
            .map(|m| GroupMember {
                member_id: m.member_id.clone(),
                client_id: String::new(),
                client_host: String::new(),
                metadata: m.metadata.to_vec(),
                assignment: vec![],
            })
            .collect();

        // Use configured assignor strategy
        let assignments = match self.assignment_strategy {
            crate::consumer::config::PartitionAssignmentStrategy::Range => {
                let assignor = RangeAssignor;
                assignor.assign(topics, &topic_partitions, &group_members)
            }
            crate::consumer::config::PartitionAssignmentStrategy::RoundRobin => {
                let assignor = RoundRobinAssignor;
                assignor.assign(topics, &topic_partitions, &group_members)
            }
            crate::consumer::config::PartitionAssignmentStrategy::CooperativeSticky => self
                .sticky_assignor
                .assign(topics, &topic_partitions, &group_members),
        };

        // Encode assignments
        let mut result = Vec::with_capacity(members.len());
        for member in members {
            let member_assignment = assignments
                .get(&member.member_id)
                .cloned()
                .unwrap_or_else(MemberAssignment::empty);

            let encoded = self.encode_consumer_assignment(&member_assignment)?;

            result.push(SyncGroupRequestAssignment {
                member_id: member.member_id.clone(),
                assignment: encoded.freeze(),
            });
        }

        Ok(result)
    }

    /// Encode consumer assignment for SyncGroup request.
    fn encode_consumer_assignment(&self, assignment: &MemberAssignment) -> Result<BytesMut> {
        let mut buf = BytesMut::new();
        // Version
        buf.put_i16(0);
        // Topics array
        buf.put_i32(crate::protocol::array_len_i32(assignment.partitions.len())?);
        for (topic, partitions) in &assignment.partitions {
            let topic_len = i16::try_from(topic.len()).map_err(|_| {
                KrafkaError::protocol(format!(
                    "topic name '{}' exceeds Kafka i16 length limit ({} bytes)",
                    topic,
                    topic.len()
                ))
            })?;
            buf.put_i16(topic_len);
            buf.put_slice(topic.as_bytes());
            buf.put_i32(crate::protocol::array_len_i32(partitions.len())?);
            for &partition in partitions {
                buf.put_i32(partition);
            }
        }
        // User data (empty)
        buf.put_i32(-1);
        Ok(buf)
    }

    /// Check if heartbeat is overdue (for inline heartbeat during poll).
    pub async fn is_heartbeat_overdue(&self) -> bool {
        if let Some(elapsed) = self.heartbeat_controller.time_since_last_heartbeat().await {
            elapsed > self.heartbeat_interval
        } else {
            // No heartbeat recorded yet, should send one
            *self.state.read().await == GroupState::Stable
        }
    }
}

impl std::fmt::Debug for GroupCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GroupCoordinator")
            .field("group_id", &self.group_id)
            .field("session_timeout", &self.session_timeout)
            .field("heartbeat_interval", &self.heartbeat_interval)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_coordinator(
        strategy: crate::consumer::config::PartitionAssignmentStrategy,
    ) -> GroupCoordinator {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        GroupCoordinator::new(
            "test-group",
            pool.clone(),
            Arc::new(ClusterMetadata::new(
                vec!["localhost:9092".to_string()],
                pool,
                Duration::from_secs(300),
            )),
            Duration::from_secs(10),
            Duration::from_secs(3),
            Duration::from_secs(30),
        )
        .with_assignor_strategy(strategy)
    }

    #[test]
    fn test_member_assignment() {
        let mut assignment = MemberAssignment::empty();
        assert!(assignment.is_empty());

        assignment.add("topic1", vec![0, 1, 2]);
        assignment.add("topic2", vec![0, 1]);

        assert!(!assignment.is_empty());
        assert_eq!(assignment.get("topic1"), Some(vec![0, 1, 2].as_slice()));
        assert_eq!(assignment.all_partitions().len(), 5);
    }

    #[tokio::test]
    async fn test_consumer_group_state() {
        let group = ConsumerGroup::new(
            "test-group",
            Duration::from_secs(10),
            Duration::from_secs(3),
        );

        assert_eq!(group.state().await, GroupState::Unjoined);
        assert!(group.member_id().await.is_none());
        assert_eq!(group.generation_id().await, -1);

        group.join_complete("member-1".to_string(), 1).await;
        assert_eq!(group.member_id().await, Some("member-1".to_string()));
        assert_eq!(group.generation_id().await, 1);
    }

    #[tokio::test]
    async fn test_consumer_group_reset() {
        let group = ConsumerGroup::new(
            "test-group",
            Duration::from_secs(10),
            Duration::from_secs(3),
        );

        group.join_complete("member-1".to_string(), 1).await;
        group.set_state(GroupState::Stable).await;

        group.reset().await;
        assert_eq!(group.state().await, GroupState::Unjoined);
        assert!(group.member_id().await.is_none());
    }

    #[test]
    fn test_range_assignor() {
        let assignor = RangeAssignor;

        let topics = vec!["topic1".to_string()];
        let mut partitions = HashMap::new();
        partitions.insert("topic1".to_string(), vec![0, 1, 2]);

        let members = vec![
            GroupMember {
                member_id: "m1".to_string(),
                client_id: "c1".to_string(),
                client_host: "host1".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "host2".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
        ];

        let assignments = assignor.assign(&topics, &partitions, &members);

        // m1 should get 2 partitions (0, 1), m2 should get 1 partition (2)
        let m1_assignment = assignments.get("m1").unwrap();
        let m2_assignment = assignments.get("m2").unwrap();

        assert_eq!(m1_assignment.get("topic1"), Some(vec![0, 1].as_slice()));
        assert_eq!(m2_assignment.get("topic1"), Some(vec![2].as_slice()));
    }

    #[test]
    fn test_roundrobin_assignor() {
        let assignor = RoundRobinAssignor;

        let topics = vec!["topic1".to_string()];
        let mut partitions = HashMap::new();
        partitions.insert("topic1".to_string(), vec![0, 1, 2, 3]);

        let members = vec![
            GroupMember {
                member_id: "m1".to_string(),
                client_id: "c1".to_string(),
                client_host: "host1".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "host2".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
        ];

        let assignments = assignor.assign(&topics, &partitions, &members);

        // m1 gets 0, 2; m2 gets 1, 3
        let m1_partitions = assignments.get("m1").unwrap().get("topic1").unwrap();
        let m2_partitions = assignments.get("m2").unwrap().get("topic1").unwrap();

        assert_eq!(m1_partitions.len(), 2);
        assert_eq!(m2_partitions.len(), 2);
    }

    #[test]
    fn test_noop_rebalance_listener() {
        use crate::consumer::TopicPartition;

        let listener = NoOpRebalanceListener;

        // All methods should be no-ops (not panic)
        let partitions = vec![
            TopicPartition::new("topic1", 0),
            TopicPartition::new("topic2", 1),
        ];

        // These should all be no-ops and not panic
        listener.on_partitions_assigned(&partitions);
        listener.on_partitions_revoked(&partitions);
        listener.on_partitions_lost(&partitions);
    }

    #[test]
    fn test_rebalance_listener_trait_bounds() {
        // Ensure trait bounds are satisfied for async contexts
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<NoOpRebalanceListener>();
    }

    #[test]
    fn test_custom_rebalance_listener() {
        use crate::consumer::TopicPartition;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingListener {
            assigned_count: AtomicUsize,
            revoked_count: AtomicUsize,
            lost_count: AtomicUsize,
        }

        impl ConsumerRebalanceListener for CountingListener {
            fn on_partitions_assigned(&self, partitions: &[TopicPartition]) {
                self.assigned_count
                    .fetch_add(partitions.len(), Ordering::Relaxed);
            }

            fn on_partitions_revoked(&self, partitions: &[TopicPartition]) {
                self.revoked_count
                    .fetch_add(partitions.len(), Ordering::Relaxed);
            }

            fn on_partitions_lost(&self, partitions: &[TopicPartition]) {
                self.lost_count
                    .fetch_add(partitions.len(), Ordering::Relaxed);
            }
        }

        let listener = Arc::new(CountingListener {
            assigned_count: AtomicUsize::new(0),
            revoked_count: AtomicUsize::new(0),
            lost_count: AtomicUsize::new(0),
        });

        let partitions = vec![
            TopicPartition::new("topic1", 0),
            TopicPartition::new("topic1", 1),
            TopicPartition::new("topic2", 0),
        ];

        listener.on_partitions_assigned(&partitions);
        assert_eq!(listener.assigned_count.load(Ordering::Relaxed), 3);

        listener.on_partitions_revoked(&partitions[..2]);
        assert_eq!(listener.revoked_count.load(Ordering::Relaxed), 2);

        listener.on_partitions_lost(&partitions[..1]);
        assert_eq!(listener.lost_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_heartbeat_controller_creation() {
        let controller = HeartbeatController::new(Duration::from_secs(3), Duration::from_secs(30));

        assert_eq!(controller.interval(), Duration::from_secs(3));
        assert_eq!(controller.session_timeout(), Duration::from_secs(30));
        assert!(!controller.is_running());
    }

    #[test]
    fn test_heartbeat_controller_start_stop() {
        let controller = HeartbeatController::new(Duration::from_secs(3), Duration::from_secs(30));

        assert!(!controller.is_running());
        controller.start();
        assert!(controller.is_running());
        controller.stop();
        assert!(!controller.is_running());
    }

    #[tokio::test]
    async fn test_heartbeat_controller_success() {
        let controller = HeartbeatController::new(Duration::from_secs(3), Duration::from_secs(30));

        // Initially, no heartbeat recorded
        assert!(controller.time_since_last_heartbeat().await.is_none());
        assert!(!controller.may_have_timed_out().await);

        // Record a heartbeat
        controller.heartbeat_success().await;

        // Now we should have a recent heartbeat
        let elapsed = controller.time_since_last_heartbeat().await.unwrap();
        assert!(elapsed < Duration::from_secs(1));
        assert!(!controller.may_have_timed_out().await);
    }

    #[test]
    fn test_heartbeat_status_requires_rejoin() {
        assert!(!HeartbeatStatus::Ok.requires_rejoin());
        assert!(HeartbeatStatus::RebalanceNeeded.requires_rejoin());
        assert!(HeartbeatStatus::UnknownMember.requires_rejoin());
        assert!(HeartbeatStatus::IllegalGeneration.requires_rejoin());
        assert!(HeartbeatStatus::SessionTimeout.requires_rejoin());
        assert!(!HeartbeatStatus::FatalError.requires_rejoin());
    }

    #[test]
    fn test_heartbeat_status_is_fatal() {
        assert!(!HeartbeatStatus::Ok.is_fatal());
        assert!(!HeartbeatStatus::RebalanceNeeded.is_fatal());
        assert!(HeartbeatStatus::FatalError.is_fatal());
    }

    #[test]
    fn test_heartbeat_status_is_session_invalidating() {
        assert!(!HeartbeatStatus::Ok.is_session_invalidating());
        assert!(!HeartbeatStatus::RebalanceNeeded.is_session_invalidating());
        assert!(HeartbeatStatus::UnknownMember.is_session_invalidating());
        assert!(HeartbeatStatus::IllegalGeneration.is_session_invalidating());
        assert!(HeartbeatStatus::SessionTimeout.is_session_invalidating());
        assert!(!HeartbeatStatus::FatalError.is_session_invalidating());
    }

    #[test]
    fn test_cooperative_sticky_assignor_basic() {
        let assignor = CooperativeStickyAssignor::new();
        assert_eq!(assignor.name(), "cooperative-sticky");

        let topics = vec!["topic1".to_string()];
        let mut partitions = HashMap::new();
        partitions.insert("topic1".to_string(), vec![0, 1, 2, 3]);

        let members = vec![
            GroupMember {
                member_id: "member-1".to_string(),
                client_id: "client-1".to_string(),
                client_host: "host-1".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "member-2".to_string(),
                client_id: "client-2".to_string(),
                client_host: "host-2".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
        ];

        let assignments = assignor.assign(&topics, &partitions, &members);

        assert_eq!(assignments.len(), 2);
        let member1_parts: Vec<_> = assignments.get("member-1").unwrap().all_partitions();
        let member2_parts: Vec<_> = assignments.get("member-2").unwrap().all_partitions();

        // Each member should have 2 partitions (4 total / 2 members)
        assert_eq!(member1_parts.len(), 2);
        assert_eq!(member2_parts.len(), 2);
    }

    #[test]
    fn test_cooperative_sticky_assignor_stickiness() {
        let assignor = CooperativeStickyAssignor::new();

        let topics = vec!["topic1".to_string()];
        let mut partitions = HashMap::new();
        partitions.insert("topic1".to_string(), vec![0, 1, 2, 3]);

        let members = vec![
            GroupMember {
                member_id: "member-1".to_string(),
                client_id: "client-1".to_string(),
                client_host: "host-1".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "member-2".to_string(),
                client_id: "client-2".to_string(),
                client_host: "host-2".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
        ];

        // First assignment
        let assignments = assignor.assign(&topics, &partitions, &members);

        // Record assignments for stickiness
        for (member_id, assignment) in &assignments {
            assignor.record_assignment(member_id, assignment);
        }

        // Assign again with same members - should maintain stickiness
        let second_assignments = assignor.assign(&topics, &partitions, &members);

        // Assignments should be identical (sticky)
        for member_id in ["member-1", "member-2"] {
            let first = assignments.get(member_id).unwrap();
            let second = second_assignments.get(member_id).unwrap();
            assert_eq!(first.partitions, second.partitions);
        }
    }

    #[test]
    fn test_cooperative_sticky_assignor_new_member() {
        let assignor = CooperativeStickyAssignor::new();

        let topics = vec!["topic1".to_string()];
        let mut partitions = HashMap::new();
        partitions.insert("topic1".to_string(), vec![0, 1, 2, 3, 4, 5]);

        // Initially 2 members
        let members_initial = vec![
            GroupMember {
                member_id: "member-1".to_string(),
                client_id: "client-1".to_string(),
                client_host: "host-1".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "member-2".to_string(),
                client_id: "client-2".to_string(),
                client_host: "host-2".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
        ];

        let initial_assignments = assignor.assign(&topics, &partitions, &members_initial);

        // Record assignments
        for (member_id, assignment) in &initial_assignments {
            assignor.record_assignment(member_id, assignment);
        }

        // Add a third member
        let members_new = vec![
            GroupMember {
                member_id: "member-1".to_string(),
                client_id: "client-1".to_string(),
                client_host: "host-1".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "member-2".to_string(),
                client_id: "client-2".to_string(),
                client_host: "host-2".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "member-3".to_string(),
                client_id: "client-3".to_string(),
                client_host: "host-3".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
        ];

        let new_assignments = assignor.assign(&topics, &partitions, &members_new);

        // All 3 members should get 2 partitions each (6 / 3 = 2)
        for member_id in ["member-1", "member-2", "member-3"] {
            let parts = new_assignments.get(member_id).unwrap().all_partitions();
            assert_eq!(
                parts.len(),
                2,
                "Member {member_id} should have 2 partitions"
            );
        }
    }

    #[test]
    fn test_cooperative_sticky_get_partitions_to_revoke() {
        let assignor = CooperativeStickyAssignor::new();

        // Record old assignment
        let mut old_assignment = MemberAssignment::empty();
        old_assignment.add("topic1", vec![0, 1, 2]);
        assignor.record_assignment("member-1", &old_assignment);

        // New assignment loses partition 2
        let mut new_assignment = MemberAssignment::empty();
        new_assignment.add("topic1", vec![0, 1]);

        let revoked = assignor.get_partitions_to_revoke("member-1", &new_assignment);

        assert_eq!(revoked.len(), 1);
        assert_eq!(revoked[0], ("topic1".to_string(), 2));
    }

    #[test]
    fn test_heartbeat_controller_signal_rebalance() {
        let controller = HeartbeatController::new(Duration::from_secs(3), Duration::from_secs(30));

        // Initially no rebalance needed
        assert!(
            !controller.take_rebalance_needed(),
            "initially rebalance_needed should be false"
        );

        // Signal rebalance
        controller.signal_rebalance();

        // Flag should now be true
        assert!(
            controller.take_rebalance_needed(),
            "after signal_rebalance(), take_rebalance_needed should return true"
        );
    }

    #[test]
    fn test_heartbeat_controller_take_rebalance_needed_resets() {
        let controller = HeartbeatController::new(Duration::from_secs(3), Duration::from_secs(30));

        controller.signal_rebalance();

        // First take should return true
        assert!(
            controller.take_rebalance_needed(),
            "first take_rebalance_needed after signal should return true"
        );

        // Second take should return false (flag was reset)
        assert!(
            !controller.take_rebalance_needed(),
            "second take_rebalance_needed should return false after reset"
        );

        // Signal again and verify it works again
        controller.signal_rebalance();
        assert!(
            controller.take_rebalance_needed(),
            "take_rebalance_needed should return true again after another signal"
        );
    }

    /// CooperativeSticky rebalancing with uneven partition count.
    ///
    /// With 5 partitions and 3 members, the correct distribution is 2-2-1.
    /// Before the fix, stickiness could produce 3-1-1 because the underloaded
    /// threshold used min_per_member (floor=1) instead of max_per_member (ceil=2).
    #[test]
    fn test_cooperative_sticky_uneven_partitions() {
        let assignor = CooperativeStickyAssignor::new();

        let topics = vec!["topic1".to_string()];
        let mut partitions = HashMap::new();
        partitions.insert("topic1".to_string(), vec![0, 1, 2, 3, 4]);

        let members = vec![
            GroupMember {
                member_id: "m1".to_string(),
                client_id: "c1".to_string(),
                client_host: "h1".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "h2".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "m3".to_string(),
                client_id: "c3".to_string(),
                client_host: "h3".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
        ];

        // Pre-seed sticky assignment to create an imbalanced state:
        // m1 has 3 partitions (0,1,2), m2 has 1 (3), m3 has 1 (4)
        let mut m1_assignment = MemberAssignment::empty();
        m1_assignment.add("topic1", vec![0, 1, 2]);
        assignor.record_assignment("m1", &m1_assignment);

        let mut m2_assignment = MemberAssignment::empty();
        m2_assignment.add("topic1", vec![3]);
        assignor.record_assignment("m2", &m2_assignment);

        let mut m3_assignment = MemberAssignment::empty();
        m3_assignment.add("topic1", vec![4]);
        assignor.record_assignment("m3", &m3_assignment);

        let assignments = assignor.assign(&topics, &partitions, &members);

        // With fix, no member should have more than ceil(5/3) = 2 partitions
        for member_id in ["m1", "m2", "m3"] {
            let count = assignments.get(member_id).unwrap().all_partitions().len();
            assert!(
                count <= 2,
                "Member {member_id} has {count} partitions, max should be 2"
            );
        }

        // Total partitions should still be 5
        let total: usize = ["m1", "m2", "m3"]
            .iter()
            .map(|m| assignments.get(*m).unwrap().all_partitions().len())
            .sum();
        assert_eq!(total, 5, "Total partitions should be 5");
    }

    /// CooperativeSticky with exactly even partition count.
    #[test]
    fn test_cooperative_sticky_even_partitions() {
        let assignor = CooperativeStickyAssignor::new();

        let topics = vec!["topic1".to_string()];
        let mut partitions = HashMap::new();
        partitions.insert("topic1".to_string(), vec![0, 1, 2, 3, 4, 5]);

        let members = vec![
            GroupMember {
                member_id: "m1".to_string(),
                client_id: "c1".to_string(),
                client_host: "h1".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "h2".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "m3".to_string(),
                client_id: "c3".to_string(),
                client_host: "h3".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
        ];

        let assignments = assignor.assign(&topics, &partitions, &members);

        // Each member should have exactly 2 partitions (6/3 = 2)
        for member_id in ["m1", "m2", "m3"] {
            let count = assignments.get(member_id).unwrap().all_partitions().len();
            assert_eq!(
                count, 2,
                "Member {member_id} should have exactly 2 partitions"
            );
        }
    }

    #[test]
    fn test_encode_decode_consumer_metadata_v0() {
        let coordinator =
            test_coordinator(crate::consumer::config::PartitionAssignmentStrategy::Range);

        let topics = vec!["topic1".to_string(), "topic2".to_string()];
        let owned = HashMap::new();
        let encoded = coordinator
            .encode_consumer_metadata(&topics, &owned)
            .unwrap();

        let (decoded_topics, decoded_owned) = GroupCoordinator::decode_consumer_metadata(&encoded);

        assert_eq!(decoded_topics, topics);
        assert!(decoded_owned.is_empty());
    }

    #[test]
    fn test_encode_decode_consumer_metadata_v1_with_owned() {
        let coordinator = test_coordinator(
            crate::consumer::config::PartitionAssignmentStrategy::CooperativeSticky,
        );

        let topics = vec!["topic1".to_string(), "topic2".to_string()];
        let mut owned = HashMap::new();
        owned.insert("topic1".to_string(), vec![0, 1, 2]);
        owned.insert("topic2".to_string(), vec![0]);

        let encoded = coordinator
            .encode_consumer_metadata(&topics, &owned)
            .unwrap();

        let (decoded_topics, decoded_owned) = GroupCoordinator::decode_consumer_metadata(&encoded);

        assert_eq!(decoded_topics, topics);
        assert_eq!(decoded_owned.len(), 2);
        assert_eq!(decoded_owned.get("topic1").unwrap(), &vec![0, 1, 2]);
        assert_eq!(decoded_owned.get("topic2").unwrap(), &vec![0]);
    }

    #[test]
    fn test_encode_decode_consumer_metadata_v1_empty_owned() {
        let coordinator = test_coordinator(
            crate::consumer::config::PartitionAssignmentStrategy::CooperativeSticky,
        );

        let topics = vec!["topic1".to_string()];
        let owned = HashMap::new();

        let encoded = coordinator
            .encode_consumer_metadata(&topics, &owned)
            .unwrap();

        let (decoded_topics, decoded_owned) = GroupCoordinator::decode_consumer_metadata(&encoded);

        assert_eq!(decoded_topics, vec!["topic1".to_string()]);
        assert!(decoded_owned.is_empty());
    }

    #[test]
    fn test_decode_consumer_metadata_empty() {
        let (topics, owned) = GroupCoordinator::decode_consumer_metadata(&[]);
        assert!(topics.is_empty());
        assert!(owned.is_empty());
    }

    #[test]
    fn test_decode_consumer_metadata_truncated() {
        // Only version byte, no topics
        let (topics, owned) = GroupCoordinator::decode_consumer_metadata(&[0, 0]);
        assert!(topics.is_empty());
        assert!(owned.is_empty());
    }

    #[test]
    fn test_cooperative_sticky_record_and_revoke_across_rebalances() {
        let assignor = CooperativeStickyAssignor::new();

        let topics = vec!["topic1".to_string()];
        let mut partitions = HashMap::new();
        partitions.insert("topic1".to_string(), vec![0, 1, 2, 3, 4, 5]);

        // Round 1: 2 members
        let members = vec![
            GroupMember {
                member_id: "m1".to_string(),
                client_id: "c1".to_string(),
                client_host: "h1".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "h2".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
        ];

        let round1 = assignor.assign(&topics, &partitions, &members);
        for (mid, assignment) in &round1 {
            assignor.record_assignment(mid, assignment);
        }

        // Each member gets 3 partitions
        assert_eq!(round1.get("m1").unwrap().all_partitions().len(), 3);
        assert_eq!(round1.get("m2").unwrap().all_partitions().len(), 3);

        // Round 2: 3 members (m3 joins)
        let members3 = vec![
            GroupMember {
                member_id: "m1".to_string(),
                client_id: "c1".to_string(),
                client_host: "h1".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "h2".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "m3".to_string(),
                client_id: "c3".to_string(),
                client_host: "h3".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
        ];

        let round2 = assignor.assign(&topics, &partitions, &members3);

        // Each member gets 2 partitions (6/3)
        for mid in ["m1", "m2", "m3"] {
            assert_eq!(
                round2.get(mid).unwrap().all_partitions().len(),
                2,
                "Member {mid} should have 2 partitions after rebalance"
            );
        }

        // m1 and m2 should have been revoked 1 partition each
        let m1_revoke = assignor.get_partitions_to_revoke("m1", round2.get("m1").unwrap());
        let m2_revoke = assignor.get_partitions_to_revoke("m2", round2.get("m2").unwrap());

        assert_eq!(m1_revoke.len(), 1, "m1 should revoke 1 partition");
        assert_eq!(m2_revoke.len(), 1, "m2 should revoke 1 partition");
    }

    #[test]
    fn test_cooperative_sticky_member_leaves() {
        let assignor = CooperativeStickyAssignor::new();

        let topics = vec!["topic1".to_string()];
        let mut partitions = HashMap::new();
        partitions.insert("topic1".to_string(), vec![0, 1, 2, 3, 4, 5]);

        // Round 1: 3 members
        let members3 = vec![
            GroupMember {
                member_id: "m1".to_string(),
                client_id: "c1".to_string(),
                client_host: "h1".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "h2".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "m3".to_string(),
                client_id: "c3".to_string(),
                client_host: "h3".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
        ];

        let round1 = assignor.assign(&topics, &partitions, &members3);
        for (mid, a) in &round1 {
            assignor.record_assignment(mid, a);
        }
        // 2 each
        for mid in ["m1", "m2", "m3"] {
            assert_eq!(round1.get(mid).unwrap().all_partitions().len(), 2);
        }

        // m3 leaves
        assignor.clear_member("m3");

        let members2 = vec![
            GroupMember {
                member_id: "m1".to_string(),
                client_id: "c1".to_string(),
                client_host: "h1".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "h2".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
        ];

        let round2 = assignor.assign(&topics, &partitions, &members2);

        // Each remaining member gets 3 (6/2)
        assert_eq!(round2.get("m1").unwrap().all_partitions().len(), 3);
        assert_eq!(round2.get("m2").unwrap().all_partitions().len(), 3);

        // m1 should NOT have any revocations (only gains)
        let m1_revoke = assignor.get_partitions_to_revoke("m1", round2.get("m1").unwrap());
        assert!(
            m1_revoke.is_empty(),
            "m1 should not revoke anything when gaining partitions"
        );
    }

    #[test]
    fn test_cooperative_sticky_no_revocations_same_assignment() {
        let assignor = CooperativeStickyAssignor::new();

        let mut assignment = MemberAssignment::empty();
        assignment.add("topic1", vec![0, 1]);
        assignor.record_assignment("m1", &assignment);

        // Same assignment → no revocations
        let to_revoke = assignor.get_partitions_to_revoke("m1", &assignment);
        assert!(to_revoke.is_empty());
    }

    #[test]
    fn test_cooperative_sticky_revoke_unknown_member() {
        let assignor = CooperativeStickyAssignor::new();

        let assignment = MemberAssignment::empty();
        let to_revoke = assignor.get_partitions_to_revoke("unknown", &assignment);
        assert!(to_revoke.is_empty());
    }

    #[test]
    fn test_is_cooperative() {
        let range = test_coordinator(crate::consumer::config::PartitionAssignmentStrategy::Range);
        assert!(!range.is_cooperative());

        let cooperative = test_coordinator(
            crate::consumer::config::PartitionAssignmentStrategy::CooperativeSticky,
        );
        assert!(cooperative.is_cooperative());
    }

    #[test]
    fn test_cooperative_sticky_multi_topic_assignment() {
        let assignor = CooperativeStickyAssignor::new();

        let topics = vec!["t1".to_string(), "t2".to_string()];
        let mut partitions = HashMap::new();
        partitions.insert("t1".to_string(), vec![0, 1, 2]);
        partitions.insert("t2".to_string(), vec![0, 1, 2]);

        let members = vec![
            GroupMember {
                member_id: "m1".to_string(),
                client_id: "c1".to_string(),
                client_host: "h1".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "h2".to_string(),
                metadata: vec![],
                assignment: vec![],
            },
        ];

        let assignments = assignor.assign(&topics, &partitions, &members);

        // 6 total partitions / 2 members = 3 each
        let m1_total = assignments.get("m1").unwrap().all_partitions().len();
        let m2_total = assignments.get("m2").unwrap().all_partitions().len();
        assert_eq!(m1_total, 3);
        assert_eq!(m2_total, 3);
        assert_eq!(m1_total + m2_total, 6);
    }

    #[test]
    fn test_cooperative_sticky_revoke_across_topics() {
        let assignor = CooperativeStickyAssignor::new();

        // Old assignment: m1 has t1-[0,1] and t2-[0]
        let mut old = MemberAssignment::empty();
        old.add("t1", vec![0, 1]);
        old.add("t2", vec![0]);
        assignor.record_assignment("m1", &old);

        // New assignment: m1 only has t1-[0]
        let mut new_a = MemberAssignment::empty();
        new_a.add("t1", vec![0]);

        let revoked = assignor.get_partitions_to_revoke("m1", &new_a);
        assert_eq!(revoked.len(), 2);

        let mut sorted = revoked.clone();
        sorted.sort();
        assert!(sorted.contains(&("t1".to_string(), 1)));
        assert!(sorted.contains(&("t2".to_string(), 0)));
    }

    #[test]
    fn test_decode_consumer_metadata_overcounted_partitions() {
        // Build v1 metadata where owned partitions claim 5000 entries
        // but only 3 fit in the buffer. The safe loop bound must cap iteration
        // based on remaining bytes (5000 is within the hard cap of 10,000).
        let mut buf = BytesMut::new();
        buf.put_i16(1); // version 1
        buf.put_i32(1); // 1 subscribed topic
        let topic = b"sub";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        buf.put_i32(-1); // no user data

        // Owned partitions section
        buf.put_i32(1); // 1 owned topic
        let owned_topic = b"test";
        buf.put_i16(owned_topic.len() as i16);
        buf.put_slice(owned_topic);
        buf.put_i32(5_000); // claim 5000 partitions
        buf.put_i32(0); // only 3 actual partition values
        buf.put_i32(1);
        buf.put_i32(2);

        let (topics, owned) = GroupCoordinator::decode_consumer_metadata(&buf);
        assert_eq!(topics, vec!["sub".to_string()]);
        // Should decode only the 3 partitions that fit, not spin 1M times
        let parts = owned.get("test").unwrap();
        assert_eq!(parts, &[0, 1, 2]);
    }

    #[test]
    fn test_cooperative_sticky_record_after_no_revocation_rebalance() {
        // Simulates the no-revocation path: after sync, the caller records
        // the final assignment. Verify that the next get_partitions_to_revoke
        // uses it correctly.
        let assignor = CooperativeStickyAssignor::new();

        // Simulate first rebalance result (no prior state)
        let mut first = MemberAssignment::empty();
        first.add("t1", vec![0, 1, 2]);
        // Caller records final assignment (as the poll loop now does)
        assignor.record_assignment("m1", &first);

        // Verify owned state was persisted
        let prev = assignor.previous_assignments.read();
        assert_eq!(prev.get("m1").unwrap().get("t1").unwrap(), &vec![0, 1, 2]);
        drop(prev);

        // Second rebalance: some partitions moved away
        let mut second = MemberAssignment::empty();
        second.add("t1", vec![0, 1]); // partition 2 moved
        let revoked = assignor.get_partitions_to_revoke("m1", &second);
        assert_eq!(revoked, vec![("t1".to_string(), 2)]);
    }
}
