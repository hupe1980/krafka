//! Consumer group coordination.
//!
//! This module provides consumer group coordination primitives including:
//! - [`ConsumerGroup`] state machine for group coordination
//! - [`GroupCoordinator`] for managing group membership and heartbeats
//! - [`MemberAssignment`] for tracking partition assignments
//! - [`PartitionAssignor`] trait and implementations for partition assignment strategies
//! - [`ConsumerRebalanceListener`] trait for rebalance callbacks

use std::collections::HashMap;
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
    ApiKey, FindCoordinatorRequest, FindCoordinatorResponse, HeartbeatRequest, HeartbeatResponse,
    JoinGroupRequest, JoinGroupRequestProtocol, JoinGroupResponse, JoinGroupResponseMember,
    LeaveGroupMember, LeaveGroupRequest, LeaveGroupResponse, ListOffsetsRequest,
    ListOffsetsRequestPartition, ListOffsetsRequestTopic, ListOffsetsResponse, OffsetCommitRequest,
    OffsetCommitRequestPartition, OffsetCommitRequestTopic, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchRequestTopic, OffsetFetchResponse, SyncGroupRequest,
    SyncGroupRequestAssignment, SyncGroupResponse,
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
    /// This is triggered after a rebalance when the consumer receives
    /// its new partition assignment.
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
                "Cannot commit: not part of a group",
            )),
            GroupState::PreparingRebalance | GroupState::AwaitingSync => Err(
                KrafkaError::invalid_state("Cannot commit: rebalance in progress"),
            ),
            _ => Err(KrafkaError::invalid_state(format!(
                "Cannot commit in state: {:?}",
                state
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
    previous_assignments: std::sync::RwLock<HashMap<String, HashMap<String, Vec<PartitionId>>>>,
}

impl CooperativeStickyAssignor {
    /// Create a new cooperative sticky assignor.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the current assignments for future stickiness.
    pub fn record_assignment(&self, member_id: &str, assignment: &MemberAssignment) {
        match self.previous_assignments.write() {
            Ok(mut prev) => {
                prev.insert(member_id.to_string(), assignment.partitions.clone());
            }
            Err(poison) => {
                warn!("sticky assignor lock poisoned, clearing stale state");
                let mut prev = poison.into_inner();
                prev.clear();
                prev.insert(member_id.to_string(), assignment.partitions.clone());
                self.previous_assignments.clear_poison();
            }
        }
    }

    /// Clear previous assignment for a member that left.
    pub fn clear_member(&self, member_id: &str) {
        match self.previous_assignments.write() {
            Ok(mut prev) => {
                prev.remove(member_id);
            }
            Err(poison) => {
                warn!("sticky assignor lock poisoned on clear_member, clearing all");
                poison.into_inner().clear();
                self.previous_assignments.clear_poison();
            }
        }
    }

    /// Get partitions that should be revoked (for incremental rebalance).
    pub fn get_partitions_to_revoke(
        &self,
        member_id: &str,
        new_assignment: &MemberAssignment,
    ) -> Vec<(String, PartitionId)> {
        let prev = match self.previous_assignments.read() {
            Ok(guard) => guard,
            Err(_poison) => {
                warn!("sticky assignor lock poisoned on read, treating as empty");
                self.previous_assignments.clear_poison();
                return Vec::new();
            }
        };
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
        // On poison, fall back to an empty map (non-sticky) and clear the poison
        // so subsequent calls resume normal sticky behavior.
        let default_prev = HashMap::new();
        let prev_guard = self.previous_assignments.read();
        let prev_assignments = match &prev_guard {
            Ok(guard) => guard,
            Err(_) => {
                warn!("sticky assignor lock poisoned during assign, treating as empty");
                self.previous_assignments.clear_poison();
                &default_prev
            }
        };

        // Track which partitions are already assigned (sticky)
        let mut sticky_assignments: HashMap<(String, PartitionId), String> = HashMap::new();
        let mut member_partition_counts: HashMap<String, usize> = HashMap::new();

        // First pass: honor previous assignments (stickiness)
        for member in members {
            member_partition_counts.insert(member.member_id.clone(), 0);

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
                                    e.insert(member.member_id.clone());
                                    *member_partition_counts
                                        .entry(member.member_id.clone())
                                        .or_insert(0) += 1;
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
                sticky_assignments.insert(key, member_id.to_string());
                *member_partition_counts
                    .entry(member_id.to_string())
                    .or_insert(0) += 1;
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
                            *member_partition_counts
                                .entry(under_member.clone())
                                .or_insert(0) += 1;
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
/// coordinator.ensure_active_membership(&["topic1"]).await?;
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
    state: RwLock<GroupState>,
    /// Current partition assignment.
    assignment: RwLock<MemberAssignment>,
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
    pub(crate) sticky_assignor: CooperativeStickyAssignor,
    /// Transaction isolation level (0 = read_uncommitted, 1 = read_committed).
    isolation_level: i8,
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
            state: RwLock::new(GroupState::Unjoined),
            assignment: RwLock::new(MemberAssignment::empty()),
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

    /// Whether the current assignment strategy is cooperative.
    pub fn is_cooperative(&self) -> bool {
        self.assignment_strategy
            == crate::consumer::config::PartitionAssignmentStrategy::CooperativeSticky
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

    /// Set the subscribed topics.
    pub async fn set_subscribed_topics(&self, topics: Vec<String>) {
        *self.subscribed_topics.write().await = topics;
    }

    /// Check if the group needs to rejoin.
    pub async fn needs_rejoin(&self) -> bool {
        // Check heartbeat controller's rebalance flag first (immediate detection from R8.3)
        if self.heartbeat_controller.take_rebalance_needed() {
            *self.state.write().await = GroupState::PreparingRebalance;
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

        // Send FindCoordinator request
        let request = FindCoordinatorRequest::for_group(&self.group_id);
        let response = conn
            .send_request(ApiKey::FindCoordinator, 1, |buf| request.encode_v1(buf))
            .await?;

        let mut buf = response;
        let find_response = FindCoordinatorResponse::decode_v1(&mut buf)?;

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
    /// Checks liveness of cached connections and re-discovers if dead.
    async fn get_coordinator_connection(&self) -> Result<Arc<BrokerConnection>> {
        {
            let conn = self.coordinator_conn.read().await;
            if let Some(ref c) = *conn {
                if c.is_alive() {
                    return Ok(c.clone());
                }
                // Connection is dead, clear it and re-discover
                drop(conn);
                *self.coordinator_conn.write().await = None;
                debug!("Coordinator connection is dead, re-discovering");
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
        let brokers = self.metadata.brokers().await;
        for broker in brokers {
            if let Ok(conn) = self.pool.get_connection(&broker.address()).await {
                return Ok(conn);
            }
        }

        // Fall back to bootstrap servers
        for server in self.metadata.bootstrap_servers() {
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
            match self.sticky_assignor.previous_assignments.read() {
                Ok(guard) => guard.get(&member_id).cloned().unwrap_or_default(),
                Err(poison) => {
                    warn!("sticky assignor lock poisoned in join_group, treating as empty");
                    drop(poison.into_inner());
                    self.sticky_assignor.previous_assignments.clear_poison();
                    HashMap::new()
                }
            }
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
        };

        debug!(
            "Joining group '{}' with member_id '{}'",
            self.group_id, member_id
        );

        *self.state.write().await = GroupState::Joining;

        // Use v5 for static membership (KIP-345), v0 otherwise
        let response = if self.group_instance_id.is_some() {
            conn.send_request(ApiKey::JoinGroup, 5, |buf| request.encode_v5(buf))
                .await?
        } else {
            conn.send_request(ApiKey::JoinGroup, 0, |buf| request.encode_v0(buf))
                .await?
        };

        let mut buf = response;
        let join_response = if self.group_instance_id.is_some() {
            JoinGroupResponse::decode_v5(&mut buf)?
        } else {
            JoinGroupResponse::decode_v0(&mut buf)?
        };

        if !join_response.error_code.is_ok() {
            *self.state.write().await = GroupState::Unjoined;
            return Err(KrafkaError::broker(
                join_response.error_code,
                "Failed to join group",
            ));
        }

        // Update member ID and generation
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

        // Use v3 for static membership (KIP-345), v0 otherwise.
        // v3 includes group_instance_id; v0 silently discards it.
        let response = if self.group_instance_id.is_some() {
            conn.send_request(ApiKey::SyncGroup, 3, |buf| request.encode_v3(buf))
                .await?
        } else {
            conn.send_request(ApiKey::SyncGroup, 0, |buf| request.encode_v0(buf))
                .await?
        };

        let mut buf = response;
        // v1+ adds throttle_time_ms; v0 omits it
        let sync_response = if self.group_instance_id.is_some() {
            SyncGroupResponse::decode_v1(&mut buf)?
        } else {
            SyncGroupResponse::decode_v0(&mut buf)?
        };

        if !sync_response.error_code.is_ok() {
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
    /// For eager (non-cooperative) protocols, performs a single join+sync.
    /// For cooperative protocols, the caller should use
    /// `perform_cooperative_join_and_sync` instead for the two-phase flow.
    pub async fn ensure_active_membership(&self, topics: &[String]) -> Result<MemberAssignment> {
        // Update subscribed topics
        self.set_subscribed_topics(topics.to_vec()).await;

        let state = *self.state.read().await;
        match state {
            GroupState::Stable => {
                // Already stable, return current assignment
                Ok(self.assignment.read().await.clone())
            }
            _ => {
                // Need to join/rejoin
                self.perform_join_and_sync().await
            }
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
            // Don't start heartbeat yet — we need another rejoin after revocation
            // The assignment from this round is kept in sticky_assignor for the next round
            Ok((new_assignment, to_revoke))
        }
    }

    /// Start the background heartbeat task.
    pub(crate) async fn start_heartbeat_task(&self) {
        // Stop existing task if any
        self.stop_heartbeat_task().await;

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

        heartbeat_controller.start();

        tokio::spawn(async move {
            debug!("Starting heartbeat task for group '{}'", group_id);

            let mut interval = tokio::time::interval(heartbeat_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

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
                            let request = HeartbeatRequest {
                                group_id: group_id.clone(),
                                generation_id,
                                member_id: member_id.clone(),
                                group_instance_id: group_instance_id.clone(),
                            };

                            // Use v3 for static membership (KIP-345), v0 otherwise
                            let send_result = if group_instance_id.is_some() {
                                conn.send_request(ApiKey::Heartbeat, 3, |buf| {
                                    request.encode_v3(buf)
                                })
                                .await
                            } else {
                                conn.send_request(ApiKey::Heartbeat, 0, |buf| {
                                    request.encode_v0(buf)
                                })
                                .await
                            };

                            match send_result
                            {
                                Ok(response) => {
                                    let mut buf = response;
                                    let decode_result = if group_instance_id.is_some() {
                                        HeartbeatResponse::decode_v1(&mut buf)
                                    } else {
                                        HeartbeatResponse::decode_v0(&mut buf)
                                    };
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
                                                heartbeat_controller.signal_rebalance();
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

        // Use v3 for static membership (KIP-345), v0 otherwise
        let response = if self.group_instance_id.is_some() {
            conn.send_request(ApiKey::Heartbeat, 3, |buf| request.encode_v3(buf))
                .await?
        } else {
            conn.send_request(ApiKey::Heartbeat, 0, |buf| request.encode_v0(buf))
                .await?
        };

        let mut buf = response;
        let hb_response = if self.group_instance_id.is_some() {
            HeartbeatResponse::decode_v1(&mut buf)?
        } else {
            HeartbeatResponse::decode_v0(&mut buf)?
        };

        let status = HeartbeatStatus::from_error_code(hb_response.error_code);
        if status == HeartbeatStatus::Ok {
            self.heartbeat_controller.heartbeat_success().await;
        }

        Ok(status)
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
                "Cannot commit offsets: group state is {:?}",
                state
            )));
        }

        let conn = self.get_coordinator_connection().await?;
        let member_id = self.member_id.read().await.clone();
        let generation_id = *self.generation_id.read().await;

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
            .map(|(name, partitions)| OffsetCommitRequestTopic { name, partitions })
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
            .send_request(ApiKey::OffsetCommit, 2, |buf| request.encode_v2(buf))
            .await?;

        let mut buf = response;
        let commit_response = OffsetCommitResponse::decode_v0(&mut buf)?;

        // Check for errors
        for topic in &commit_response.topics {
            for partition in &topic.partitions {
                if !partition.error_code.is_ok() {
                    // Handle rebalance errors specially
                    if partition.error_code == ErrorCode::RebalanceInProgress
                        || partition.error_code == ErrorCode::IllegalGeneration
                        || partition.error_code == ErrorCode::UnknownMemberId
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
                partition_indexes: parts.clone(),
            })
            .collect();

        let request = OffsetFetchRequest {
            group_id: self.group_id.clone(),
            topics: Some(topics),
            require_stable: false,
        };

        debug!(
            "Fetching committed offsets for group '{}' ({} topics)",
            self.group_id,
            partitions.len()
        );

        // Use API version 1 for OffsetFetch: v0 returns UNKNOWN_TOPIC_OR_PARTITION
        // on modern Kafka brokers (Confluent 7.x / Apache Kafka 3.x) because v0
        // originally targeted ZooKeeper-based offset storage. v1+ correctly reads
        // from the __consumer_offsets internal topic.
        // v0 and v1 share identical request wire format; the only response
        // difference is a trailing error_code in v1 that decode_v0 ignores.
        let response = conn
            .send_request(ApiKey::OffsetFetch, 1, |buf| request.encode_v0(buf))
            .await?;

        let mut buf = response;
        let offset_response = OffsetFetchResponse::decode_v0(&mut buf)?;
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
                if let Some(leader_id) = self.metadata.leader(topic, partition).await {
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
            let _ = self.metadata.refresh_for_topics(Some(&topics)).await;

            // Retry resolution after refresh
            for (topic, partition) in leaderless {
                if let Some(leader_id) = self.metadata.leader(&topic, partition).await {
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

        for leader_partitions in partitions_by_leader.values() {
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
            };

            // Get connection to this leader
            let (topic_sample, partition_sample) = &leader_partitions[0];
            let conn = self
                .metadata
                .get_leader_connection(topic_sample, *partition_sample)
                .await?;

            let response = conn
                .send_request(ApiKey::ListOffsets, 2, |buf| request.encode_v2(buf))
                .await?;

            let mut buf = response;
            let list_response = ListOffsetsResponse::decode_v2(&mut buf)?;

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

        // Stop heartbeat task
        self.stop_heartbeat_task().await;

        let conn = match self.get_coordinator_connection().await {
            Ok(c) => c,
            Err(_) => {
                // If we can't get a connection, just reset state
                self.reset().await;
                return Ok(());
            }
        };

        let member_id = self.member_id.read().await.clone();

        *self.state.write().await = GroupState::Leaving;

        let request = LeaveGroupRequest {
            group_id: self.group_id.clone(),
            member_id: member_id.clone(),
            members: if self.group_instance_id.is_some() {
                vec![LeaveGroupMember {
                    member_id: member_id.clone(),
                    group_instance_id: self.group_instance_id.clone(),
                }]
            } else {
                vec![]
            },
        };

        debug!(
            "Leaving group '{}', member_id='{}'",
            self.group_id, member_id
        );

        // Send leave group request (don't wait too long)
        // Use v3 for static membership (KIP-345), v0 otherwise
        let result = if self.group_instance_id.is_some() {
            tokio::time::timeout(
                Duration::from_secs(5),
                conn.send_request(ApiKey::LeaveGroup, 3, |buf| request.encode_v3(buf)),
            )
            .await
        } else {
            tokio::time::timeout(
                Duration::from_secs(5),
                conn.send_request(ApiKey::LeaveGroup, 0, |buf| request.encode_v0(buf)),
            )
            .await
        };

        // Decode the response and check for errors
        match result {
            Ok(Ok(response_bytes)) => {
                let mut buf = response_bytes;
                let decode_result = if self.group_instance_id.is_some() {
                    LeaveGroupResponse::decode_v3(&mut buf)
                } else {
                    LeaveGroupResponse::decode_v0(&mut buf)
                };
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

        self.reset().await;
        Ok(())
    }

    /// Reset coordinator state.
    async fn reset(&self) {
        *self.member_id.write().await = String::new();
        *self.generation_id.write().await = -1;
        *self.state.write().await = GroupState::Unjoined;
        *self.assignment.write().await = MemberAssignment::empty();
        *self.coordinator_conn.write().await = None;
        *self.coordinator_id.write().await = None;
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

        // Topics array
        buf.put_i32(crate::protocol::array_len_i32(topics.len())?);
        for topic in topics {
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
            // Owned partitions (version 1+)
            buf.put_i32(crate::protocol::array_len_i32(owned_partitions.len())?);
            for (topic, partitions) in owned_partitions {
                let topic_len = i16::try_from(topic.len()).map_err(|_| {
                    KrafkaError::protocol(format!(
                        "topic name '{}' exceeds Kafka i16 length limit",
                        topic
                    ))
                })?;
                buf.put_i16(topic_len);
                buf.put_slice(topic.as_bytes());
                buf.put_i32(crate::protocol::array_len_i32(partitions.len())?);
                for &p in partitions {
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
        let mut buf = Bytes::copy_from_slice(data);

        let version = buf.get_i16();

        // Decode topics
        let mut topics = Vec::new();
        if buf.remaining() >= 4 {
            let topic_count = buf.get_i32();
            // Cap by hard limit and remaining buffer to prevent allocation DoS
            let safe_count = (topic_count.max(0) as usize)
                .min(10_000)
                .min(buf.remaining() / 2);
            for _ in 0..safe_count {
                if buf.remaining() < 2 {
                    break;
                }
                let len = buf.get_i16();
                if len < 0 || buf.remaining() < len as usize {
                    break;
                }
                topics.push(String::from_utf8_lossy(&buf.copy_to_bytes(len as usize)).to_string());
            }
        }

        // Skip user_data
        if buf.remaining() >= 4 {
            let user_data_len = buf.get_i32();
            if user_data_len > 0 && buf.remaining() >= user_data_len as usize {
                buf.advance(user_data_len as usize);
            }
        }

        // Decode owned partitions (version 1+)
        let mut owned = HashMap::new();
        if version >= 1 && buf.remaining() >= 4 {
            let topic_count = buf.get_i32();
            // Cap topic count by hard limit and remaining buffer to prevent allocation DoS
            let safe_topic_count = (topic_count.max(0) as usize)
                .min(10_000)
                .min(buf.remaining() / 6);
            for _ in 0..safe_topic_count {
                if buf.remaining() < 2 {
                    break;
                }
                let len = buf.get_i16();
                if len < 0 || buf.remaining() < len as usize {
                    break;
                }
                let topic = String::from_utf8_lossy(&buf.copy_to_bytes(len as usize)).to_string();
                if buf.remaining() < 4 {
                    break;
                }
                let part_count = buf.get_i32();
                // Cap allocation by both a hard limit and remaining buffer bytes
                let safe_part_count = (part_count.max(0) as usize)
                    .min(10_000)
                    .min(buf.remaining() / 4);
                let mut parts = Vec::with_capacity(safe_part_count);
                for _ in 0..safe_part_count {
                    if buf.remaining() < 4 {
                        break;
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
        // Cap iteration by remaining buffer to prevent allocation DoS
        let safe_topic_count = (topic_count as usize).min(buf.remaining() / 6);
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
            let topic = String::from_utf8_lossy(&buf.copy_to_bytes(topic_len)).to_string();

            if buf.remaining() < 4 {
                break;
            }
            let partition_count = buf.get_i32();
            if partition_count < 0 {
                break;
            }
            let safe_partition_count = (partition_count as usize)
                .min(10_000)
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
            if let Some(topic_info) = self.metadata.topic(topic).await {
                let partitions: Vec<_> =
                    topic_info.partitions.iter().map(|p| p.partition).collect();
                topic_partitions.insert(topic.clone(), partitions);
            }
        }

        // For cooperative protocol, decode member metadata to extract owned partitions
        // and feed them into the sticky assignor before computing new assignments.
        if self.is_cooperative() {
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
        // Build v1 metadata where owned partitions claim 1_000_000 entries
        // but only 3 fit in the buffer. The safe loop bound must cap iteration.
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
        buf.put_i32(1_000_000); // claim 1M partitions
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
        let prev = assignor.previous_assignments.read().unwrap();
        assert_eq!(prev.get("m1").unwrap().get("t1").unwrap(), &vec![0, 1, 2]);
        drop(prev);

        // Second rebalance: some partitions moved away
        let mut second = MemberAssignment::empty();
        second.add("t1", vec![0, 1]); // partition 2 moved
        let revoked = assignor.get_partitions_to_revoke("m1", &second);
        assert_eq!(revoked, vec![("t1".to_string(), 2)]);
    }
}
