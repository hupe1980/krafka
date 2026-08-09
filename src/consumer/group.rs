//! Consumer group coordination.
//!
//! This module provides consumer group coordination primitives including:
//! - [`ConsumerGroup`] state machine for group coordination
//! - [`GroupCoordinator`] for managing group membership and heartbeats
//! - [`MemberAssignment`] for tracking partition assignments
//! - [`PartitionAssignor`] trait and implementations for partition assignment strategies
//! - [`ConsumerRebalanceListener`] trait for rebalance callbacks

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::sync::{RwLock, mpsc};
use tracing::{debug, error, info, warn};

use crate::PartitionId;
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
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

/// Slack added on top of the group's rebalance timeout when bounding a
/// `JoinGroup` client-side.
///
/// The coordinator parks a `JoinGroup` until every member has rejoined or the
/// rebalance timeout elapses, then answers. The slack lets that answer reach us
/// instead of racing the client-side deadline.
const JOIN_GROUP_TIMEOUT_SLACK: Duration = Duration::from_secs(5);

/// How many times `OffsetFetch` is re-issued while the coordinator reports that
/// a partition's committed offset is staged inside an unresolved transaction
/// (`UNSTABLE_OFFSET_COMMIT`, KIP-447).
///
/// Sized to absorb a normal transaction round trip — a few hundred
/// milliseconds — not a transaction that is genuinely stuck. Beyond it the
/// error is surfaced and the poll loop's offset-resolution retry becomes the
/// outer loop, which is unbounded and already backs off.
const UNSTABLE_OFFSET_MAX_ATTEMPTS: u32 = 5;

/// Whether `OffsetFetch` must ask the coordinator for **stable** offsets
/// (KIP-447), given the consumer's isolation level.
///
/// Only `read_committed` needs it. Asking for it under `read_uncommitted`
/// would block a consumer's startup on an unrelated producer's open
/// transaction while it is already, by configuration, willing to read
/// uncommitted data.
fn require_stable_for(isolation_level: i8) -> bool {
    isolation_level == crate::consumer::IsolationLevel::ReadCommitted.to_i8()
}

/// The first partition whose committed offset is staged inside an unresolved
/// transaction, if any.
///
/// Separated from the fetch so the decision can be tested: the dangerous
/// failure here is *not* noticing, because an unnoticed
/// `UNSTABLE_OFFSET_COMMIT` leaves the partition out of the result map and
/// every caller reads a missing entry as "never committed" — which means
/// `auto.offset.reset`.
fn first_unstable_offset(response: &OffsetFetchResponse) -> Option<(&str, crate::PartitionId)> {
    response.topics.iter().find_map(|topic| {
        topic
            .partitions
            .iter()
            .find(|p| p.error_code == ErrorCode::UnstableOffsetCommit)
            .map(|p| (topic.name.as_str(), p.partition_index))
    })
}

/// Callback interface for partition rebalance events.
/// Async callback interface for partition rebalance events.
///
/// Implement this trait to receive notifications when the consumer's
/// partition assignment changes during a rebalance.  All methods are
/// `async` and are **awaited on the consumer's poll/rebalance task**;
/// the consumer blocks rebalance progress until each future resolves.
///
/// # Execution contract
///
/// - Async I/O (offset commits, cache flushes, …) can be `await`-ed
///   directly — no `block_in_place` or auxiliary threads needed.
/// - Keep callbacks fast; long-running work should be delegated to a
///   dedicated channel or background task.
/// - **Do not acquire consumer locks from inside a callback** — the
///   consumer may already hold them, which would deadlock.
/// - Panics inside callbacks propagate to the consumer task and
///   terminate it. Handle expected errors with `Result` internally.
///
/// # Example
///
/// ```rust,ignore
/// use krafka::consumer::{ConsumerRebalanceListener, TopicPartition};
///
/// struct MyListener;
///
/// impl ConsumerRebalanceListener for MyListener {
///     async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) {
///         println!("Assigned: {:?}", partitions);
///     }
///
///     async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) {
///         // commit offsets directly — fully async, no blocking needed
///         println!("Revoked: {:?}", partitions);
///     }
/// }
/// ```
pub trait ConsumerRebalanceListener: Send + Sync {
    /// Called after partitions have been assigned to this consumer.
    ///
    /// The `partitions` slice contains the **newly added** partitions for this
    /// rebalance round.  The semantics match the Java client:
    ///
    /// | Rebalance protocol | `partitions` contains |
    /// |---|---|
    /// | Initial join (first poll after subscribe) | all assigned partitions |
    /// | Eager rebalance (classic protocol) | all assigned partitions (entire set is new after revoke-all) |
    /// | Cooperative rebalance (KIP-429) | **only newly added** partitions (delta vs previous round) |
    /// | KIP-848 / new consumer protocol | **only newly added** partitions (diff-based) |
    ///
    /// For cooperative and KIP-848 rebalances the slice may be empty if the
    /// rebalance left this consumer's assignment unchanged.  To obtain the
    /// **full** post-rebalance assignment call
    /// [`crate::consumer::Consumer::assignment`] from inside the callback.
    fn on_partitions_assigned<'a>(
        &'a self,
        partitions: &'a [crate::consumer::TopicPartition],
    ) -> impl std::future::Future<Output = ()> + Send + 'a;

    /// Called before partitions are revoked from this consumer.
    ///
    /// Triggered before the consumer loses its current partitions during a
    /// rebalance. Commit offsets here if needed.
    fn on_partitions_revoked<'a>(
        &'a self,
        partitions: &'a [crate::consumer::TopicPartition],
    ) -> impl std::future::Future<Output = ()> + Send + 'a;

    /// Called when partitions are lost due to an unclean shutdown (default: no-op).
    ///
    /// Unlike `on_partitions_revoked`, **the consumer has likely already been
    /// fenced**. Do **not** commit offsets here — another consumer may already
    /// own these partitions and a commit would silently overwrite their progress.
    /// Override to add loss-specific cleanup (e.g., invalidating local caches).
    fn on_partitions_lost<'a>(
        &'a self,
        _partitions: &'a [crate::consumer::TopicPartition],
    ) -> impl std::future::Future<Output = ()> + Send + 'a {
        async {}
    }
}

/// A no-op rebalance listener that does nothing on rebalance events.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoOpRebalanceListener;

impl ConsumerRebalanceListener for NoOpRebalanceListener {
    async fn on_partitions_assigned(&self, _partitions: &[crate::consumer::TopicPartition]) {}
    async fn on_partitions_revoked(&self, _partitions: &[crate::consumer::TopicPartition]) {}
}

/// Blanket impl so callers can share a listener via `Arc` while still passing
/// `listener.clone()` directly (i.e. `Arc<T>`) to `ConsumerBuilder::rebalance_listener`.
impl<T: ConsumerRebalanceListener + Send + Sync> ConsumerRebalanceListener for std::sync::Arc<T> {
    fn on_partitions_assigned<'a>(
        &'a self,
        partitions: &'a [crate::consumer::TopicPartition],
    ) -> impl std::future::Future<Output = ()> + Send + 'a {
        (**self).on_partitions_assigned(partitions)
    }

    fn on_partitions_revoked<'a>(
        &'a self,
        partitions: &'a [crate::consumer::TopicPartition],
    ) -> impl std::future::Future<Output = ()> + Send + 'a {
        (**self).on_partitions_revoked(partitions)
    }

    fn on_partitions_lost<'a>(
        &'a self,
        partitions: &'a [crate::consumer::TopicPartition],
    ) -> impl std::future::Future<Output = ()> + Send + 'a {
        (**self).on_partitions_lost(partitions)
    }
}

// ── Object-safe erased trait for Arc<dyn …> storage ──────────────────────
//
// `ConsumerRebalanceListener` uses `async fn` (RPITIT) which is not
// dyn-compatible.  `ErasedRebalanceListener` mirrors it with
// `Pin<Box<dyn Future>>` returns so the Consumer can store
// `Arc<dyn ErasedRebalanceListener>` without generic parameters.
// A blanket impl converts any `ConsumerRebalanceListener` transparently.
// This is the same pattern used for `SchemaRegistryClient`.
pub(crate) trait ErasedRebalanceListener: Send + Sync {
    fn on_partitions_assigned_erased<'a>(
        &'a self,
        partitions: &'a [crate::consumer::TopicPartition],
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;

    fn on_partitions_revoked_erased<'a>(
        &'a self,
        partitions: &'a [crate::consumer::TopicPartition],
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;

    fn on_partitions_lost_erased<'a>(
        &'a self,
        partitions: &'a [crate::consumer::TopicPartition],
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;
}

impl<T: ConsumerRebalanceListener> ErasedRebalanceListener for T {
    fn on_partitions_assigned_erased<'a>(
        &'a self,
        partitions: &'a [crate::consumer::TopicPartition],
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(self.on_partitions_assigned(partitions))
    }

    fn on_partitions_revoked_erased<'a>(
        &'a self,
        partitions: &'a [crate::consumer::TopicPartition],
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(self.on_partitions_revoked(partitions))
    }

    fn on_partitions_lost_erased<'a>(
        &'a self,
        partitions: &'a [crate::consumer::TopicPartition],
    ) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(self.on_partitions_lost(partitions))
    }
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

impl std::fmt::Display for GroupState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Unjoined => "Unjoined",
            Self::Joining => "Joining",
            Self::AwaitingSync => "AwaitingSync",
            Self::Stable => "Stable",
            Self::PreparingRebalance => "PreparingRebalance",
            Self::Leaving => "Leaving",
            Self::Dead => "Dead",
        })
    }
}

/// Member assignment in a consumer group.
#[non_exhaustive]
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
    pub fn all_partitions(&self) -> impl Iterator<Item = (&str, PartitionId)> + '_ {
        self.partitions
            .iter()
            .flat_map(|(topic, partitions)| partitions.iter().map(move |&p| (topic.as_str(), p)))
    }

    /// Check if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.partitions.is_empty()
    }
}

/// A consumer group member.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct GroupMember {
    /// Member ID assigned by the coordinator.
    pub member_id: String,
    /// Client ID.
    pub client_id: String,
    /// Client host.
    pub client_host: String,
    /// Member metadata.
    pub metadata: Bytes,
    /// Member assignment.
    pub assignment: Bytes,
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
        *self.coordinator_id.write().await = None;
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
            let Some(member_topics) = member_topic_partitions.get_mut(&member.member_id) else {
                unreachable!("member must exist in pre-populated map");
            };
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
                                    *member_partition_counts.entry(mid.clone()).or_insert(0) += 1;
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
                let member_id = member_id.to_string();
                *member_partition_counts
                    .entry(member_id.clone())
                    .or_insert(0) += 1;
                sticky_assignments.insert(key, member_id);
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
                    for owner in sticky_assignments.values_mut() {
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

/// Eager sticky partition assignor (KIP-54).
///
/// Produces the same balanced, movement-minimising assignment as
/// [`CooperativeStickyAssignor`], but advertises the `"sticky"` protocol name,
/// which puts the group on the **eager** rebalance protocol: every member
/// revokes its entire assignment before the new one is computed and applied.
///
/// The stickiness therefore shows up in *which* partitions a member gets back
/// rather than in avoiding the interruption — a member that keeps the same
/// partitions across a rebalance still stops consuming for the duration of the
/// rebalance, but it does not have to re-seek or rebuild partition-local state
/// afterwards.
///
/// # Which one should I use?
///
/// Prefer [`CooperativeStickyAssignor`] for new deployments: it delivers the
/// same placement without the stop-the-world revocation. `StickyAssignor`
/// exists for parity with the Java client and for groups that cannot yet move
/// to the cooperative protocol — for example a group whose other members are
/// older clients that only speak eager protocols.
///
/// # Example
///
/// ```
/// use krafka::consumer::{PartitionAssignor, StickyAssignor};
///
/// let assignor = StickyAssignor::new();
/// assert_eq!(assignor.name(), "sticky");
/// ```
#[derive(Debug, Default)]
pub struct StickyAssignor {
    /// The placement algorithm is identical to the cooperative assignor's, so
    /// it is reused wholesale rather than duplicated; only the advertised
    /// protocol name (and hence the rebalance protocol) differs.
    inner: CooperativeStickyAssignor,
}

impl StickyAssignor {
    /// Create a new eager sticky assignor.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the current assignment for a member so the next rebalance can
    /// keep those partitions with the same owner.
    pub fn record_assignment(&self, member_id: &str, assignment: &MemberAssignment) {
        self.inner.record_assignment(member_id, assignment);
    }

    /// Forget a member that has left the group.
    pub fn clear_member(&self, member_id: &str) {
        self.inner.clear_member(member_id);
    }
}

impl PartitionAssignor for StickyAssignor {
    fn name(&self) -> &str {
        "sticky"
    }

    fn assign(
        &self,
        topics: &[String],
        partitions: &HashMap<String, Vec<PartitionId>>,
        members: &[GroupMember],
    ) -> HashMap<String, MemberAssignment> {
        self.inner.assign(topics, partitions, members)
    }
}

/// Tracks how long it has been since the application last called `poll()`.
///
/// # Why the consumer needs this
///
/// Heartbeats are sent by a background task, so they keep flowing whether or
/// not the application is making progress. An application that stops calling
/// `poll()` — deadlocked, stuck on a slow downstream call, or looping forever
/// on one record — therefore looks perfectly healthy to the coordinator. It
/// holds its partitions indefinitely while consuming nothing from them, and
/// because the group never rebalances, no other member can take over. Nothing
/// in the system reports an error; the partitions simply stop advancing.
///
/// `max.poll.interval.ms` is the bound that makes that failure visible. The
/// heartbeat task compares the elapsed time against it and, once exceeded,
/// stops heartbeating so the coordinator can reassign the partitions to a
/// member that is actually consuming.
#[derive(Debug)]
pub(crate) struct PollTracker {
    /// When `poll()` was last entered.
    last_poll: parking_lot::Mutex<std::time::Instant>,
    /// Maximum permitted gap between `poll()` calls.
    max_poll_interval: Duration,
    /// Set once the interval has been exceeded, so `poll()` can report it.
    exceeded: std::sync::atomic::AtomicBool,
}

impl PollTracker {
    /// Create a tracker armed from now.
    pub(crate) fn new(max_poll_interval: Duration) -> Self {
        Self {
            last_poll: parking_lot::Mutex::new(std::time::Instant::now()),
            max_poll_interval,
            exceeded: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Record that the application has just called `poll()`.
    pub(crate) fn note_poll(&self) {
        *self.last_poll.lock() = std::time::Instant::now();
    }

    /// Time since the last `poll()`.
    pub(crate) fn elapsed(&self) -> Duration {
        self.last_poll.lock().elapsed()
    }

    /// The configured maximum poll interval.
    pub(crate) fn max_poll_interval(&self) -> Duration {
        self.max_poll_interval
    }

    /// Whether the application has exceeded the maximum poll interval.
    pub(crate) fn is_expired(&self) -> bool {
        self.elapsed() > self.max_poll_interval
    }

    /// Latch the expired state. Returns `true` the first time it is set, so
    /// the caller can act on the transition exactly once.
    pub(crate) fn mark_exceeded(&self) -> bool {
        !self
            .exceeded
            .swap(true, std::sync::atomic::Ordering::SeqCst)
    }

    /// Whether the tracker has been latched as exceeded.
    pub(crate) fn exceeded(&self) -> bool {
        self.exceeded.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Clear the latch and restart the timer, e.g. after rejoining the group.
    pub(crate) fn reset(&self) {
        self.exceeded
            .store(false, std::sync::atomic::Ordering::SeqCst);
        self.note_poll();
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
    /// Last successful heartbeat time (nanos-level precision, sync access).
    last_heartbeat: Arc<parking_lot::Mutex<Option<std::time::Instant>>>,
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
            last_heartbeat: Arc::new(parking_lot::Mutex::new(None)),
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
        self.running.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Start the heartbeat controller.
    pub fn start(&self) {
        self.running
            .store(true, std::sync::atomic::Ordering::Release);
    }

    /// Stop the heartbeat controller.
    pub fn stop(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::Release);
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
    pub fn heartbeat_success(&self) {
        *self.last_heartbeat.lock() = Some(std::time::Instant::now());
    }

    /// Get the time since the last heartbeat.
    pub fn time_since_last_heartbeat(&self) -> Option<Duration> {
        (*self.last_heartbeat.lock()).map(|t| t.elapsed())
    }

    /// Check if the session may have timed out.
    pub fn may_have_timed_out(&self) -> bool {
        self.time_since_last_heartbeat()
            .is_some_and(|elapsed| elapsed > self.session_timeout)
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
#[non_exhaustive]
pub enum HeartbeatCommand {
    /// Stop the heartbeat task.
    Stop,
    /// Trigger a rejoin.
    Rejoin,
    /// Send an immediate heartbeat with current owned partitions to
    /// acknowledge a revocation (KIP-848 §revocation-ack).
    AcknowledgeRevocation,
}

/// A completed `JoinGroup`/`SyncGroup` round that is waiting for `poll()` to
/// apply it.
///
/// The classic rebalance has two halves with very different requirements. The
/// first half — `JoinGroup` and `SyncGroup` — is the group's synchronisation
/// barrier: the coordinator cannot answer *any* member's `JoinGroup` until
/// every member has sent one, so a member that delays it holds up the whole
/// group. It touches nothing but group identity (member id, generation, the
/// selected protocol and the opaque assignment bytes), so the background
/// heartbeat task can run it safely.
///
/// The second half — revocation callbacks, rewriting the offset map, partition
/// state and the receive buffer, then assignment callbacks — is the consumer's
/// data plane and runs user code. Nothing outside the group needs it to happen
/// promptly; it only gates *this* member's own fetching. Doing it on a
/// background task would race the application, so the background task parks
/// its result here and `poll()` applies it at the point it already applies a
/// rebalance.
#[derive(Debug, Clone)]
pub struct PendingRebalance {
    /// The assignment the coordinator handed this member.
    pub assignment: MemberAssignment,
    /// Partitions that must be revoked before a second cooperative round
    /// (KIP-429). Always empty for eager protocols.
    pub to_revoke: Vec<(String, PartitionId)>,
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
/// Rebalance-coordinated state for the group.
///
/// All four fields are updated atomically so readers can never observe a
/// mismatched generation: `state == Stable` with an empty `assignment`, or
/// `generation_id` from one epoch with `member_id` from another.
#[derive(Debug)]
struct GroupInner {
    /// Member ID assigned by the coordinator.  Empty string before join.
    member_id: String,
    /// Generation ID (-1 before first join).
    generation_id: i32,
    /// Current group state.
    state: GroupState,
    /// Current partition assignment.
    assignment: MemberAssignment,
}

impl GroupInner {
    fn initial() -> Self {
        Self {
            member_id: String::new(),
            generation_id: -1,
            state: GroupState::Unjoined,
            assignment: MemberAssignment::empty(),
        }
    }
}

/// Emit the KIP-1274 phase-1 deprecation warning for the classic rebalance
/// protocol, at most once per process.
///
/// Apache Kafka 4.3 logs this from the Java consumer on every classic-protocol
/// start; krafka mirrors it so that an operator reading either client's logs
/// learns the same thing. The timeline it refers to is fixed upstream: 4.3
/// warns, 5.0 flips the default to the KIP-848 protocol and deprecates the
/// classic one in `KafkaConsumer`, 6.0 removes it.
///
/// Once per process rather than once per consumer: an application that creates
/// many short-lived consumers would otherwise turn a migration notice into log
/// spam, which is the reliable way to make sure nobody reads it.
pub fn warn_classic_protocol_deprecated() {
    static WARNED: std::sync::Once = std::sync::Once::new();
    WARNED.call_once(|| {
        warn!(
            "Consumer group is using the CLASSIC rebalance protocol. Apache Kafka 4.3 \
             deprecated it (KIP-1274): the default becomes the KIP-848 consumer protocol \
             in Kafka 5.0 and classic support is removed in 6.0. Switch with \
             `Consumer::builder().group_protocol(GroupProtocol::Consumer)` — it needs \
             Kafka 4.0+ (or 3.7-3.9 with `group.coordinator.new.enable=true`), moves \
             assignment to the broker, and makes rebalances incremental instead of \
             stop-the-world. This warning is emitted once per process."
        );
    });
}

/// Manages the consumer group lifecycle: join, sync, heartbeat, and leave.
///
/// Communicates with the group coordinator broker via the Kafka group management
/// protocol (KIP-848 new consumer protocol when supported, classic protocol as
/// fallback). Drives membership, partition assignment, and offset commit/fetch
/// on behalf of a [`Consumer`](super::Consumer).
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
    /// Rebalance-coordinated state (member_id, generation_id, state, assignment)
    /// consolidated under a single lock for atomic updates across rebalances.
    inner: Arc<RwLock<GroupInner>>,
    /// Heartbeat controller.
    heartbeat_controller: Arc<HeartbeatController>,
    /// Tracks time since the application last called `poll()` so a stalled
    /// application stops being heartbeated as a healthy member.
    poll_tracker: Arc<PollTracker>,
    /// A non-retriable error observed by the background heartbeat task.
    ///
    /// The KIP-848 heartbeat is the sole communication channel with the
    /// coordinator, so errors surface on a background task with no caller to
    /// return them to. Conditions that retrying cannot fix — authorization
    /// failures, a malformed or unsupported request, a group that is already
    /// at its size limit — would otherwise be retried silently forever, with
    /// the consumer simply never receiving an assignment and never reporting
    /// why. Recording the error here lets the next `poll()` return it.
    fatal_error: Arc<parking_lot::Mutex<Option<String>>>,
    /// Set when the coordinator fences this member (KIP-848), meaning its
    /// partitions have been taken away without a clean revocation.
    ///
    /// The coordinator-side reset clears *its* view of the assignment, but the
    /// consumer keeps a separate map that drives fetching and — critically —
    /// bounds which partitions `commit()` is allowed to write. Leaving that map
    /// populated after fencing lets an auto-commit overwrite the progress of
    /// whichever member now owns those partitions. This flag is how the
    /// coordinator tells the consumer to drop them.
    membership_lost: Arc<std::sync::atomic::AtomicBool>,
    /// Channel to control heartbeat task.
    heartbeat_cmd_tx: RwLock<Option<mpsc::Sender<HeartbeatCommand>>>,
    /// Result of a `JoinGroup`/`SyncGroup` the background heartbeat task ran on
    /// this member's behalf, waiting for the next `poll()` to apply it.
    ///
    /// A `parking_lot::Mutex` rather than an async lock: every access is a
    /// `take` or a `replace` of a small value with no `.await` under the guard,
    /// so it can be read from the poll path and written from the heartbeat task
    /// without participating in the lock hierarchy at all.
    pending_rebalance: Arc<parking_lot::Mutex<Option<PendingRebalance>>>,
    /// `true` while the heartbeat task has a `JoinGroup`/`SyncGroup` in flight.
    ///
    /// `poll()` reads this to avoid starting a second, competing join for the
    /// same rebalance: two `JoinGroup`s from one member race to define its
    /// generation, and whichever loses leaves the consumer applying an
    /// assignment the coordinator has already superseded.
    ///
    /// A `watch` channel rather than a flag so `poll()` can *wait* for the
    /// rebalance instead of returning empty over and over. A plain flag would
    /// turn a slow rebalance — one waiting on some other member that really has
    /// stopped responding — into a poll loop spinning at full speed for the
    /// length of the rebalance timeout.
    rejoin_in_flight: tokio::sync::watch::Sender<bool>,
    /// Incremented every time a heartbeat task is started.
    ///
    /// `stop_heartbeat_task` asks the current task to exit but does not wait
    /// for it, so a task can still be running its shutdown when its successor
    /// is already live. The epoch lets a task recognise that it has been
    /// superseded and skip its cleanup, instead of resetting group state that
    /// now belongs to the join its successor just completed.
    heartbeat_epoch: Arc<std::sync::atomic::AtomicU64>,
    /// Subscribed topics.
    subscribed_topics: RwLock<Vec<String>>,
    /// Protocol type (always "consumer").
    protocol_type: String,
    /// Partition assignment strategies in preference order.
    ///
    /// Every one of these is advertised in JoinGroup; the coordinator picks
    /// the most-preferred protocol that all members support.
    assignment_strategies: Vec<crate::consumer::config::PartitionAssignmentStrategy>,
    /// The strategy the coordinator actually selected for the group, latched
    /// from `JoinGroupResponse.protocol_name`.
    ///
    /// Until the first successful join this holds the most-preferred
    /// configured strategy. It must be read (rather than assuming the
    /// preferred one) wherever behaviour depends on the protocol — most
    /// importantly [`is_cooperative`](GroupCoordinator::is_cooperative) —
    /// because the group may well have settled on a different protocol than
    /// this member would have chosen.
    ///
    /// A sync lock: the value is `Copy` and every access is a single
    /// uncontended read or write with no `.await` under the guard, so this
    /// stays callable from sync context such as
    /// [`is_cooperative`](GroupCoordinator::is_cooperative).
    negotiated_strategy: parking_lot::RwLock<crate::consumer::config::PartitionAssignmentStrategy>,
    /// Static group membership instance ID (KIP-345).
    group_instance_id: Option<String>,
    /// Client rack ID for closest-replica fetching and server-side rack-aware
    /// assignment (KIP-392 / KIP-848). Sent in every ConsumerGroupHeartbeat
    /// request so the coordinator can place the member on a rack-local replica.
    client_rack: Option<String>,
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
    /// Partitions this member currently *owns*, as opposed to the ones the
    /// coordinator wants it to own.
    ///
    /// This is what the heartbeat reports back as its owned-partition list,
    /// and it advances only once revocation callbacks have run and the
    /// consumer has genuinely stopped fetching the partitions being given up.
    ///
    /// It has to be separate from [`target_assignment`], which is overwritten
    /// with the coordinator's new target the instant a heartbeat response
    /// arrives. Reporting the target as if it were owned would tell the
    /// coordinator that partitions have been released while the consumer is
    /// still fetching them, and the coordinator would hand them to another
    /// member — two consumers reading the same partition at once.
    ///
    /// [`target_assignment`]: Self::target_assignment
    owned_assignment: Arc<RwLock<Vec<ConsumerGroupTopicPartitions>>>,
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
            inner: Arc::new(RwLock::new(GroupInner::initial())),
            heartbeat_controller: Arc::new(HeartbeatController::new(
                heartbeat_interval,
                session_timeout,
            )),
            // `rebalance_timeout` is set from `max.poll.interval.ms`, which is
            // exactly the bound the poll tracker enforces.
            poll_tracker: Arc::new(PollTracker::new(rebalance_timeout)),
            fatal_error: Arc::new(parking_lot::Mutex::new(None)),
            membership_lost: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            heartbeat_cmd_tx: RwLock::new(None),
            pending_rebalance: Arc::new(parking_lot::Mutex::new(None)),
            rejoin_in_flight: tokio::sync::watch::Sender::new(false),
            heartbeat_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            subscribed_topics: RwLock::new(Vec::new()),
            protocol_type: "consumer".to_string(),
            assignment_strategies: vec![
                crate::consumer::config::PartitionAssignmentStrategy::Range,
            ],
            negotiated_strategy: parking_lot::RwLock::new(
                crate::consumer::config::PartitionAssignmentStrategy::Range,
            ),
            group_instance_id: None,
            client_rack: None,
            sticky_assignor: CooperativeStickyAssignor::new(),
            isolation_level: 0,
            group_protocol: crate::consumer::config::GroupProtocol::Classic,
            member_epoch: Arc::new(RwLock::new(0)),
            target_assignment: Arc::new(RwLock::new(Vec::new())),
            owned_assignment: Arc::new(RwLock::new(Vec::new())),
            topic_names_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Set a single partition assignment strategy (builder pattern).
    pub fn with_assignor_strategy(
        self,
        strategy: crate::consumer::config::PartitionAssignmentStrategy,
    ) -> Self {
        self.with_assignor_strategies(vec![strategy])
    }

    /// Set the partition assignment strategies in preference order (builder
    /// pattern).
    ///
    /// All are advertised in JoinGroup. An empty list is ignored so the
    /// coordinator always has at least one protocol to offer.
    pub fn with_assignor_strategies(
        mut self,
        strategies: Vec<crate::consumer::config::PartitionAssignmentStrategy>,
    ) -> Self {
        if strategies.is_empty() {
            return self;
        }
        // Until the coordinator tells us otherwise, assume our own first
        // preference; this keeps `is_cooperative()` meaningful before the
        // first join completes.
        if let Some(&first) = strategies.first() {
            self.negotiated_strategy = parking_lot::RwLock::new(first);
        }
        self.assignment_strategies = strategies;
        self
    }

    /// Set the static group membership instance ID (KIP-345, builder pattern).
    pub fn with_group_instance_id(mut self, id: Option<String>) -> Self {
        self.group_instance_id = id;
        self
    }

    /// Set the client rack ID for KIP-392 rack-aware assignment (builder pattern).
    ///
    /// When set, the value is sent in every `ConsumerGroupHeartbeat` request
    /// so that the KIP-848 coordinator can place the member on a rack-local
    /// replica, reducing cross-rack traffic in multi-AZ deployments.
    pub fn with_client_rack(mut self, rack: Option<String>) -> Self {
        self.client_rack = rack;
        self
    }

    /// Set the transaction isolation level (builder pattern).
    pub fn with_isolation_level(mut self, level: i8) -> Self {
        self.isolation_level = level;
        self
    }

    /// Set the group protocol (KIP-848, builder pattern).
    ///
    /// Selecting [`GroupProtocol::Classic`] emits the KIP-1274 phase-1
    /// deprecation warning, once per process.
    ///
    /// [`GroupProtocol::Classic`]: crate::consumer::config::GroupProtocol::Classic
    pub fn with_group_protocol(mut self, protocol: crate::consumer::config::GroupProtocol) -> Self {
        if protocol == crate::consumer::config::GroupProtocol::Classic {
            warn_classic_protocol_deprecated();
        }
        self.group_protocol = protocol;
        self
    }

    /// The assignment strategy the group has actually settled on.
    ///
    /// Before the first successful join this is the most-preferred configured
    /// strategy; afterwards it is whatever the coordinator selected.
    pub fn negotiated_strategy(&self) -> crate::consumer::config::PartitionAssignmentStrategy {
        *self.negotiated_strategy.read()
    }

    /// Record the protocol the coordinator selected for the group.
    ///
    /// The coordinator picks the highest-preference protocol supported by
    /// *every* member, which need not be this member's first choice — during a
    /// rolling migration a single old member holds the whole group on the old
    /// protocol. Latching the coordinator's answer is what keeps this client's
    /// rebalance behaviour in step with the rest of the group; assuming our
    /// own preference instead would, for example, run the cooperative
    /// incremental revocation path while the group is actually on an eager
    /// protocol that expects everything to be revoked.
    ///
    /// An unrecognised name is ignored (with a warning) rather than treated as
    /// a hard failure: it can only happen if the coordinator picked a protocol
    /// this client never advertised.
    fn latch_negotiated_strategy(&self, protocol_name: &str) {
        if protocol_name.is_empty() {
            return;
        }
        match crate::consumer::config::PartitionAssignmentStrategy::from_protocol_name(
            protocol_name,
        ) {
            Some(strategy) => {
                let mut current = self.negotiated_strategy.write();
                if *current != strategy {
                    info!(
                        "Group '{}' negotiated assignment protocol '{}' (was '{}')",
                        self.group_id,
                        protocol_name,
                        current.protocol_name()
                    );
                }
                *current = strategy;
            }
            None => {
                warn!(
                    "Coordinator selected unknown assignment protocol '{}' for group '{}'; \
                     keeping '{}'",
                    protocol_name,
                    self.group_id,
                    self.negotiated_strategy.read().protocol_name()
                );
            }
        }
    }

    /// Whether the negotiated assignment strategy is cooperative.
    ///
    /// Always returns `false` for the KIP-848 consumer protocol, which uses
    /// server-side assignment and does not use JoinGroup/SyncGroup semantics.
    pub fn is_cooperative(&self) -> bool {
        !self.is_consumer_protocol() && self.negotiated_strategy().is_cooperative()
    }

    /// Record that the application has just called `poll()`.
    ///
    /// Must be called at the top of every `poll()` so the heartbeat task can
    /// tell a live application from a stalled one.
    pub(crate) fn note_poll(&self) {
        self.poll_tracker.note_poll();
    }

    /// Whether the application has exceeded `max.poll.interval.ms` and has
    /// consequently stopped being heartbeated into the group.
    pub(crate) fn poll_interval_exceeded(&self) -> bool {
        self.poll_tracker.exceeded()
    }

    /// The configured maximum poll interval.
    pub(crate) fn max_poll_interval(&self) -> Duration {
        self.poll_tracker.max_poll_interval()
    }

    /// Re-arm poll-interval tracking, e.g. after the consumer has rejoined.
    pub(crate) fn reset_poll_tracking(&self) {
        self.poll_tracker.reset();
    }

    /// Take any non-retriable error recorded by the background heartbeat task.
    ///
    /// Clears the slot, so each fatal condition is reported to the application
    /// exactly once.
    pub(crate) fn take_fatal_error(&self) -> Option<String> {
        self.fatal_error.lock().take()
    }

    /// Check and clear the "this member was fenced" flag.
    ///
    /// Returns `true` exactly once per fencing event, so the consumer performs
    /// the partition hand-back once rather than on every subsequent poll.
    pub(crate) fn take_membership_lost(&self) -> bool {
        self.membership_lost
            .swap(false, std::sync::atomic::Ordering::AcqRel)
    }

    /// Take a snapshot of this member's group identity.
    ///
    /// Returns `None` before the member has joined, i.e. while there is no
    /// identity for the coordinator to fence against.
    ///
    /// Under the KIP-848 consumer protocol the member epoch takes the place of
    /// the classic generation id, since that is the value the coordinator
    /// validates commits against.
    pub async fn group_metadata(&self) -> Option<crate::consumer::ConsumerGroupMetadata> {
        let inner = self.inner.read().await;
        let member_id = inner.member_id.clone();
        let generation_id = inner.generation_id;
        drop(inner);

        if member_id.is_empty() {
            return None;
        }

        let generation = if self.is_consumer_protocol() {
            *self.member_epoch.read().await
        } else {
            generation_id
        };

        Some(crate::consumer::ConsumerGroupMetadata::new(
            self.group_id.clone(),
            generation,
            member_id,
            self.group_instance_id.clone(),
        ))
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
        self.inner.read().await.state
    }

    /// Get the member ID.
    pub async fn member_id(&self) -> String {
        self.inner.read().await.member_id.clone()
    }

    /// Get the generation ID.
    pub async fn generation_id(&self) -> i32 {
        self.inner.read().await.generation_id
    }

    /// Get the current assignment.
    pub async fn assignment(&self) -> MemberAssignment {
        self.inner.read().await.assignment.clone()
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
                && matches!(self.inner.read().await.state, GroupState::Stable))
            {
                self.inner.write().await.state = GroupState::PreparingRebalance;
            }
            return true;
        }
        matches!(
            self.inner.read().await.state,
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
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    format!(
                        "broker does not support FindCoordinator v{}-v{}",
                        FIND_COORDINATOR_MIN, FIND_COORDINATOR_MAX,
                    ),
                )
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

        // Snapshot the previous coordinator node ID before overwriting it.
        // Used below to detect a coordinator change vs. a simple reconnect.
        let old_coordinator_id = *self.coordinator_id.read().await;

        *self.coordinator_conn.write().await = Some(coordinator_conn);
        *self.coordinator_id.write().await = Some(find_response.node_id);

        // If the coordinator broker changed, the previous generation_id /
        // member_epoch are unknown to the new coordinator.  Reset membership
        // state immediately so that the next ensure_active_membership() call
        // triggers a fresh rejoin rather than sending fetches or commits with
        // a stale generation.  Skipped on first discovery (old_coordinator_id
        // is None) because the consumer hasn't joined yet.
        if let Some(old_id) = old_coordinator_id
            && old_id != find_response.node_id
        {
            info!(
                "Group coordinator for '{}' changed from node {} to node {} — resetting membership state to force rejoin",
                self.group_id, old_id, find_response.node_id
            );
            self.reset_member_identity().await;
            self.inner.write().await.state = GroupState::PreparingRebalance;
        }

        info!(
            "Found coordinator for group '{}': node {} at {}",
            self.group_id, find_response.node_id, coordinator_addr
        );

        Ok(())
    }

    /// Client-side budget for a single `JoinGroup` round-trip.
    ///
    /// The coordinator answers a `JoinGroup` only once the rebalance it belongs
    /// to has converged, so the request's latency is governed by the group's
    /// rebalance timeout rather than by `request.timeout.ms`. This mirrors the
    /// Java client's `joinGroupTimeoutMs`. The connection layer additionally
    /// floors the result at its own `request_timeout`, so this can only ever
    /// lengthen the budget.
    pub fn join_group_timeout(&self) -> Duration {
        self.rebalance_timeout
            .saturating_add(JOIN_GROUP_TIMEOUT_SLACK)
    }

    /// Drop the cached coordinator when the broker tells us it is no longer
    /// the coordinator for this group.
    ///
    /// `get_coordinator_connection` only re-discovers when the cached socket
    /// is *unusable*. After a coordinator failover the old broker is usually
    /// still alive and reachable — it just no longer owns this group — so the
    /// connection stays "usable" and every subsequent request is sent to a
    /// broker that can only ever answer `NOT_COORDINATOR`. Clearing the cache
    /// here is what forces the next call to run FindCoordinator again.
    ///
    /// Returns `true` if the error was coordinator-related (and therefore
    /// retriable after re-discovery).
    async fn invalidate_coordinator_on_error(&self, error_code: ErrorCode) -> bool {
        let is_coordinator_error = matches!(
            error_code,
            ErrorCode::NotCoordinator
                | ErrorCode::CoordinatorNotAvailable
                | ErrorCode::CoordinatorLoadInProgress
        );

        if is_coordinator_error {
            debug!(
                "Coordinator for group '{}' returned {:?}; dropping cached coordinator \
                 so the next request re-runs FindCoordinator",
                self.group_id, error_code
            );
            *self.coordinator_conn.write().await = None;
            *self.coordinator_id.write().await = None;
        }

        is_coordinator_error
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

        let member_id = self.inner.read().await.member_id.clone();
        let topics = self.subscribed_topics.read().await.clone();
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
            // Advertise every configured strategy, most-preferred first. The
            // coordinator intersects these lists across all members and picks
            // the first one they all support, which is what allows a group to
            // change rebalance protocol during a rolling bounce instead of
            // requiring a full stop.
            protocols: {
                let metadata = metadata.freeze();
                self.assignment_strategies
                    .iter()
                    .map(|strategy| JoinGroupRequestProtocol {
                        name: strategy.protocol_name().to_string(),
                        metadata: metadata.clone(),
                    })
                    .collect()
            },
            reason: None,
        };

        debug!(
            "Joining group '{}' with member_id '{}'",
            self.group_id, member_id
        );

        self.inner.write().await.state = GroupState::Joining;

        // Negotiate JoinGroup version. Static membership (group_instance_id)
        // requires v5+ where the GroupInstanceId field is available.
        let join_group_min = if self.group_instance_id.is_some() {
            5
        } else {
            JOIN_GROUP_MIN
        };
        let jg_version = conn
            .negotiate_api_version(ApiKey::JoinGroup, JOIN_GROUP_MAX, join_group_min)
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    format!(
                        "broker does not support JoinGroup v{}-v{}",
                        join_group_min, JOIN_GROUP_MAX,
                    ),
                )
            })?;

        // A JoinGroup is a long-poll: the coordinator holds it open until every
        // member of the group has rejoined, or until the group's rebalance
        // timeout expires. That window is `rebalance_timeout`
        // (`max.poll.interval.ms`, 5 minutes by default) and is unrelated to
        // `request.timeout.ms`. Bounding it by the ordinary request timeout
        // aborts joins client-side in the middle of a perfectly healthy
        // rebalance — any rebalance that takes longer to converge than
        // `request.timeout.ms`, such as one waiting on an idle member's session
        // to expire, fails instead of completing.
        let join_timeout = self.join_group_timeout();

        let response = conn
            .send_request_with_timeout(ApiKey::JoinGroup, jg_version, join_timeout, |buf| {
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
            self.inner.write().await.member_id = join_response.member_id.clone();

            // Rebuild the request with the assigned member_id.
            let retry_request = JoinGroupRequest {
                member_id: join_response.member_id.clone(),
                ..request.clone()
            };

            let retry_response = conn
                .send_request_with_timeout(ApiKey::JoinGroup, jg_version, join_timeout, |buf| {
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
            self.invalidate_coordinator_on_error(join_response.error_code)
                .await;
            self.inner.write().await.state = GroupState::Unjoined;
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
            let old_member_id = self.inner.read().await.member_id.clone();
            if !old_member_id.is_empty() && old_member_id != join_response.member_id {
                self.sticky_assignor.clear_member(&old_member_id);
            }
        }
        {
            let mut inner = self.inner.write().await;
            inner.member_id = join_response.member_id.clone();
            inner.generation_id = join_response.generation_id;
            inner.state = GroupState::AwaitingSync;
        }

        // Adopt the protocol the coordinator chose for the group before any
        // rebalance logic runs, so revocation follows the group's actual
        // protocol rather than this member's preference.
        if let Some(ref protocol_name) = join_response.protocol_name {
            self.latch_negotiated_strategy(protocol_name);
        }

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

        let (member_id, generation_id) = {
            let inner = self.inner.read().await;
            (inner.member_id.clone(), inner.generation_id)
        };
        let topics = self.subscribed_topics.read().await.clone();

        // If we're the leader, compute assignments — unless the coordinator
        // told us not to.
        //
        // KIP-814: when a static member rejoins and the coordinator still holds
        // a valid assignment for the group, it sets `skip_assignment` on the
        // leader's JoinGroup response and sends no member metadata. The leader
        // must then send an *empty* assignment and let the coordinator's
        // persisted one stand.
        //
        // Ignoring the flag happened to be harmless only because the member
        // list arrives empty in that case, so the assignor produced nothing to
        // send. That is obedience by accident: a leader that computes an
        // assignment whenever it is the leader has taken assignment authority
        // the coordinator explicitly reclaimed, and any future response that
        // pairs `skip_assignment` with a non-empty member list would have it
        // overwrite the coordinator's decision.
        let assignments = if join_response.is_leader() && !join_response.skip_assignment {
            self.compute_assignments(&topics, &join_response.members)?
        } else {
            if join_response.skip_assignment {
                debug!(
                    group = %self.group_id,
                    "Coordinator set skip_assignment (KIP-814); deferring to its                      existing assignment"
                );
            }
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
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    format!(
                        "broker does not support SyncGroup v{}-v{}",
                        SYNC_GROUP_MIN, SYNC_GROUP_MAX,
                    ),
                )
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
            self.invalidate_coordinator_on_error(sync_response.error_code)
                .await;
            self.inner.write().await.state = GroupState::Unjoined;
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
        {
            let mut inner = self.inner.write().await;
            inner.assignment = assignment.clone();
            inner.state = GroupState::Stable;
        }

        debug!(
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
        self: &Arc<Self>,
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
            let state = self.inner.read().await.state;
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

        let inner = self.inner.read().await;
        if inner.state == GroupState::Stable {
            // Already stable with same topics, return current assignment
            Ok((inner.assignment.clone(), false))
        } else {
            drop(inner);
            // Need to join/rejoin
            let assignment = self.perform_join_and_sync().await?;
            Ok((assignment, true))
        }
    }

    /// Run one `JoinGroup`/`SyncGroup` round trip.
    ///
    /// This is the group's synchronisation barrier and nothing else: it moves
    /// member id, generation, negotiated protocol and the raw assignment bytes
    /// forward, and touches no consumer data-plane state. That is what makes it
    /// safe to call either from `poll()` or from the background heartbeat task.
    ///
    /// Deliberately does *not* start the heartbeat task, so the background task
    /// can call it without asking `stop_heartbeat_task` to terminate itself.
    async fn join_and_sync(&self) -> Result<MemberAssignment> {
        // Find coordinator if needed
        if self.coordinator_conn.read().await.is_none() {
            self.find_coordinator().await?;
        }

        // Join group
        let join_response = self.join_group().await?;

        // Sync group
        self.sync_group(&join_response).await
    }

    /// Partitions this member must give up to reach `assignment`, per the
    /// cooperative-sticky protocol. Empty for eager protocols, which revoke
    /// everything and start over.
    async fn cooperative_revocations(
        &self,
        assignment: &MemberAssignment,
    ) -> Vec<(String, PartitionId)> {
        if !self.is_cooperative() {
            return Vec::new();
        }
        let member_id = self.inner.read().await.member_id.clone();
        self.sticky_assignor
            .get_partitions_to_revoke(&member_id, assignment)
    }

    /// Perform the full join and sync sequence, then (re)start heartbeating.
    async fn perform_join_and_sync(self: &Arc<Self>) -> Result<MemberAssignment> {
        let assignment = self.join_and_sync().await?;

        // Start heartbeat task
        self.start_heartbeat_task().await;

        Ok(assignment)
    }

    /// Run a rebalance from the background heartbeat task and park the result.
    ///
    /// Called when a heartbeat comes back `REBALANCE_IN_PROGRESS`. Completing
    /// the join/sync here is what releases every *other* member of the group:
    /// the coordinator holds their `JoinGroup` responses until this member's
    /// arrives, and it would otherwise not arrive until the application next
    /// called `poll()`.
    async fn background_rejoin(&self) -> Result<PendingRebalance> {
        let assignment = self.join_and_sync().await?;
        let to_revoke = self.cooperative_revocations(&assignment).await;
        Ok(PendingRebalance {
            assignment,
            to_revoke,
        })
    }

    /// Take the assignment a background rebalance parked for `poll()`, if any.
    pub(crate) fn take_pending_rebalance(&self) -> Option<PendingRebalance> {
        self.pending_rebalance.lock().take()
    }

    /// Discard any parked assignment. Called whenever membership is torn down,
    /// so a stale generation's assignment cannot be applied after a leave or a
    /// fresh join.
    pub(crate) fn clear_pending_rebalance(&self) {
        *self.pending_rebalance.lock() = None;
    }

    /// Whether the background heartbeat task is currently mid-rebalance.
    ///
    /// `poll()` uses this to hold off driving its own `JoinGroup` rather than
    /// racing the one already in flight.
    pub(crate) fn rejoin_in_flight(&self) -> bool {
        *self.rejoin_in_flight.borrow()
    }

    /// Block until the background rebalance finishes, or `budget` elapses.
    ///
    /// Returns immediately when no rebalance is in flight. Subscribing before
    /// the check is what makes this free of the race a notification primitive
    /// would have: a rebalance that completes in the gap marks the receiver
    /// changed, so the wait returns at once rather than sitting out the budget.
    pub(crate) async fn await_rejoin(&self, budget: Duration) {
        let mut rx = self.rejoin_in_flight.subscribe();
        if !*rx.borrow_and_update() {
            return;
        }
        let _ = tokio::time::timeout(budget, rx.changed()).await;
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
        self: &Arc<Self>,
    ) -> Result<(MemberAssignment, Vec<(String, PartitionId)>)> {
        let new_assignment = self.join_and_sync().await?;

        // Compute what needs to be revoked
        let to_revoke = self.cooperative_revocations(&new_assignment).await;

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
    ///
    /// Beyond keeping the session alive, this task owns the group's
    /// synchronisation barrier: when a heartbeat reports
    /// `REBALANCE_IN_PROGRESS` it runs `JoinGroup`/`SyncGroup` itself and parks
    /// the resulting assignment for the next `poll()`. Waiting for `poll()` to
    /// send the `JoinGroup` would stall every other member of the group behind
    /// this application's poll interval.
    pub(crate) async fn start_heartbeat_task(self: &Arc<Self>) {
        // Stop existing task if any
        self.stop_heartbeat_task().await;

        // Clear any stale rebalance/invalidation signals from the previous
        // heartbeat task. Between sending the Stop command and the old task
        // terminating, it may have received REBALANCE_IN_PROGRESS or a
        // session-invalidating error. Those signals are now stale — we just
        // completed a successful join/sync.
        self.heartbeat_controller.take_rebalance_needed();
        self.heartbeat_controller.take_member_invalidated();

        // Likewise any assignment the previous task parked: it belongs to a
        // generation that the join/sync we just finished has superseded.
        self.clear_pending_rebalance();
        self.rejoin_in_flight.send_replace(false);

        let (cmd_tx, mut cmd_rx) = mpsc::channel::<HeartbeatCommand>(10);
        *self.heartbeat_cmd_tx.write().await = Some(cmd_tx);

        let group_id = self.group_id.clone();
        let heartbeat_interval = self.heartbeat_interval;
        let heartbeat_controller = self.heartbeat_controller.clone();
        let poll_tracker = self.poll_tracker.clone();

        // Clone Arc references so the task reads current values on each heartbeat
        let inner_ref = self.inner.clone();
        let coordinator_conn_ref = self.coordinator_conn.clone();
        let group_instance_id = self.group_instance_id.clone();
        // Weak, so a `Consumer` that is dropped without `close()` is still
        // collected rather than being kept alive by its own heartbeat task.
        let coordinator_ref = Arc::downgrade(self);
        // Claim an epoch after the old task has been told to stop, so this
        // task's number is strictly newer than any task still shutting down.
        let epoch = self
            .heartbeat_epoch
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;

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
            // connection identity changes.  Storing both fields as a single
            // Option ensures they are always set and cleared atomically.
            let mut cached_hb: Option<(usize, i16)> = None;

            // A rebalance this task runs on the application's behalf.
            //
            // The join/sync goes on its own task so heartbeats keep flowing
            // while `JoinGroup` sits at the coordinator: that request is a long
            // poll bounded by the *rebalance* timeout, far longer than the
            // session timeout it would otherwise blow through if it blocked
            // this loop.
            //
            // The rebalance task parks its own result; this channel only says
            // that it finished, so the loop knows it may start another one.
            let (rejoin_tx, mut rejoin_rx) = mpsc::channel::<Result<usize>>(1);
            let mut rejoin_handle: Option<tokio::task::JoinHandle<()>> = None;

            loop {
                tokio::select! {
                    // Biased so a completed rebalance is parked before the next
                    // heartbeat goes out, keeping the parked assignment and the
                    // generation the heartbeat reports in step.
                    biased;

                    rejoin_result = rejoin_rx.recv() => {
                        rejoin_handle = None;
                        match rejoin_result {
                            Some(Ok(topic_count)) => {
                                debug!(
                                    "Background rebalance for group '{}' completed with {} \
                                     topic(s) assigned; the assignment will be applied on the \
                                     next poll()",
                                    group_id, topic_count
                                );
                            }
                            Some(Err(e)) => {
                                warn!(
                                    "Background rebalance for group '{}' failed: {}; \
                                     handing the rejoin back to poll()",
                                    group_id, e
                                );
                                // join_group/sync_group have already reset
                                // whatever state the error invalidated. Let
                                // poll() retry the whole sequence, including
                                // coordinator rediscovery, from a clean start.
                                heartbeat_controller.signal_rebalance();
                                heartbeat_controller.stop();
                                break;
                            }
                            // `rejoin_tx` is held by this task for its whole
                            // lifetime, so the channel cannot close under us.
                            None => {}
                        }
                    }

                    _ = interval.tick() => {
                        if !heartbeat_controller.is_running() {
                            break;
                        }

                        // Stop vouching for an application that has stopped
                        // consuming. Continuing to heartbeat here would keep
                        // this member's partitions assigned to a process that
                        // is not reading them, with no rebalance and no error
                        // ever surfacing.
                        //
                        // Leave the group explicitly rather than merely going
                        // quiet: a LeaveGroup makes the coordinator reassign
                        // the partitions immediately, whereas lapsing costs a
                        // further `session.timeout.ms` of unconsumed traffic.
                        // `leave_group` skips the RPC for static members
                        // (KIP-345), which is the documented behaviour — a
                        // static member that exceeds the interval keeps its
                        // assignment until its session expires, so a restart
                        // can reclaim it.
                        if poll_tracker.is_expired() {
                            if poll_tracker.mark_exceeded() {
                                warn!(
                                    "Application has not called poll() for {:?}, exceeding \
                                     max_poll_interval ({:?}); leaving group '{}' so its \
                                     partitions can be reassigned",
                                    poll_tracker.elapsed(),
                                    poll_tracker.max_poll_interval(),
                                    group_id
                                );
                            }
                            heartbeat_controller.stop();
                            if let Some(coordinator) = coordinator_ref.upgrade()
                                && let Err(e) = coordinator.leave_group().await
                            {
                                debug!(
                                    "LeaveGroup after max_poll_interval expiry failed for \
                                     group '{}': {}; the session will lapse instead",
                                    group_id, e
                                );
                            }
                            break;
                        }

                        // Read current values on each heartbeat (not stale copies)
                        let coordinator_conn = coordinator_conn_ref.read().await.clone();
                        let inner = inner_ref.read().await;
                        let member_id = inner.member_id.clone();
                        let generation_id = inner.generation_id;
                        drop(inner);

                        // Send heartbeat
                        if let Some(ref conn) = coordinator_conn {
                            // Re-negotiate only when the coordinator connection changes.
                            let conn_id = std::sync::Arc::as_ptr(conn) as usize;
                            let hb_version = match cached_hb {
                                Some((id, v)) if id == conn_id => v,
                                _ => match conn
                                    .negotiate_api_version(
                                        ApiKey::Heartbeat,
                                        HEARTBEAT_MAX,
                                        HEARTBEAT_MIN,
                                    )
                                {
                                    Some(v) => {
                                        cached_hb = Some((conn_id, v));
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
                                },
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
                                                heartbeat_controller.heartbeat_success();
                                                debug!("Heartbeat successful for group '{}'", group_id);
                                            }
                                            HeartbeatStatus::RebalanceNeeded => {
                                                // Keep heartbeating. The coordinator holds a
                                                // heartbeating member in the group for the full
                                                // rebalance timeout; one that goes quiet is
                                                // evicted after session.timeout.ms and has to
                                                // re-register with an empty member id, losing
                                                // its sticky assignment for no reason.
                                                if rejoin_handle.is_none() {
                                                    let Some(coordinator) = coordinator_ref.upgrade() else {
                                                        heartbeat_controller.stop();
                                                        break;
                                                    };
                                                    debug!(
                                                        "Rebalance in progress for group '{}'; \
                                                         rejoining in the background",
                                                        group_id
                                                    );
                                                    coordinator.rejoin_in_flight.send_replace(true);
                                                    let tx = rejoin_tx.clone();
                                                    rejoin_handle = Some(tokio::spawn(async move {
                                                        let outcome = match coordinator.background_rejoin().await {
                                                            Ok(pending) => {
                                                                let topics = pending.assignment.partitions.len();
                                                                // Park the assignment *before*
                                                                // clearing the in-flight flag, so a
                                                                // poll() woken by that flag always
                                                                // finds the result waiting for it.
                                                                *coordinator.pending_rebalance.lock() = Some(pending);
                                                                Ok(topics)
                                                            }
                                                            Err(e) => Err(e),
                                                        };
                                                        coordinator.rejoin_in_flight.send_replace(false);
                                                        let _ = tx.send(outcome).await;
                                                    }));
                                                }
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
                                                //
                                                // Any assignment a background rebalance parked
                                                // belongs to a generation the coordinator has now
                                                // forgotten. Applying it would have poll() start
                                                // fetching partitions this member no longer holds,
                                                // alongside whoever was given them instead.
                                                if let Some(coordinator) = coordinator_ref.upgrade() {
                                                    coordinator.clear_pending_rebalance();
                                                }
                                                heartbeat_controller.signal_member_invalidated();
                                                heartbeat_controller.stop();
                                                break;
                                            }
                                            HeartbeatStatus::FatalError => {
                                                error!("Fatal heartbeat error for group '{}'", group_id);
                                                if let Some(coordinator) = coordinator_ref.upgrade() {
                                                    coordinator.clear_pending_rebalance();
                                                }
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

            // A rebalance still in flight belongs to this task; abandoning it
            // would leave the coordinator in Joining/AwaitingSync, which
            // `needs_rejoin()` does not treat as needing a rejoin — the
            // consumer would then never rebalance again. Cancel it and hand
            // the rebalance back to poll().
            if let Some(handle) = rejoin_handle {
                handle.abort();
                if let Some(coordinator) = coordinator_ref.upgrade()
                    // Only if no successor has started. A newer task means
                    // `start_heartbeat_task` ran, which happens after a
                    // successful join/sync — resetting the group to
                    // PreparingRebalance here would discard that join and
                    // force a rebalance that nothing asked for.
                    && coordinator
                        .heartbeat_epoch
                        .load(std::sync::atomic::Ordering::Acquire)
                        == epoch
                {
                    coordinator.rejoin_in_flight.send_replace(false);
                    // The join may have parked a result in the instant before
                    // the abort landed; it belongs to a rebalance nobody
                    // finished, so poll() must redo it rather than apply it.
                    coordinator.clear_pending_rebalance();
                    coordinator.inner.write().await.state = GroupState::PreparingRebalance;
                    heartbeat_controller.signal_rebalance();
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
        // Any parked assignment predates the rejoin being requested here, so
        // applying it afterwards would install a superseded generation's view.
        self.clear_pending_rebalance();
        self.inner.write().await.state = GroupState::PreparingRebalance;
        let tx = self.heartbeat_cmd_tx.read().await.clone();
        if let Some(tx) = tx {
            let _ = tx.send(HeartbeatCommand::Rejoin).await;
        }
    }

    /// Acknowledge a completed reconciliation (KIP-848).
    ///
    /// Call this only once revocation callbacks have run and the consumer has
    /// actually stopped fetching any partitions it is giving up. It promotes
    /// the coordinator's target to this member's owned set and asks the
    /// heartbeat task to report it immediately, which is what advances the
    /// member epoch and lets the coordinator consider the rebalance complete.
    ///
    /// Acknowledging early — before the consumer has stopped fetching — hands
    /// the partitions to another member while this one is still reading them.
    pub async fn acknowledge_revocation(&self) {
        {
            let target = self.target_assignment.read().await.clone();
            *self.owned_assignment.write().await = target;
        }

        let tx = self.heartbeat_cmd_tx.read().await.clone();
        if let Some(tx) = tx {
            let _ = tx.send(HeartbeatCommand::AcknowledgeRevocation).await;
        }
    }

    /// Mark state as PreparingRebalance without stopping the heartbeat task.
    /// Used when we want the next poll to re-enter rebalance but need the
    /// background heartbeat to keep running (e.g., round-limit deferral).
    pub async fn set_preparing_rebalance(&self) {
        self.inner.write().await.state = GroupState::PreparingRebalance;
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
        let member_id = {
            let mut inner = self.inner.write().await;
            if inner.member_id.is_empty() {
                inner.member_id = crate::util::random_uuid_v4();
            }
            inner.member_id.clone()
        };
        let member_epoch = *self.member_epoch.read().await;

        let request = ConsumerGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: member_id.clone(),
            member_epoch,
            instance_id: self.group_instance_id.clone(),
            rack_id: self.client_rack.clone(),
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

        let Some(hb_version) = conn.negotiate_api_version(
            ApiKey::ConsumerGroupHeartbeat,
            CONSUMER_GROUP_HEARTBEAT_MAX,
            CONSUMER_GROUP_HEARTBEAT_MIN,
        ) else {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::UnknownApiVersion,
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
                // UNRELEASED_INSTANCE_ID is terminal, not a fencing error to
                // recover from.
                //
                // It means another live process is already registered with
                // this `group.instance.id`. Resetting the epoch and retrying
                // cannot help — the duplicate is a deployment mistake (two
                // processes configured with the same instance id), and the
                // coordinator will keep rejecting this member for as long as
                // the other one lives. Retrying turns that misconfiguration
                // into a silent hot loop instead of a visible failure, so
                // surface it and stop, as the Java client does.
                if hb_response.error_code == ErrorCode::UnreleasedInstanceId {
                    error!(
                        "group.instance.id {:?} is already in use by another live member of \
                         group '{}'. This is a configuration error: two processes cannot share \
                         one instance id. Not retrying.",
                        self.group_instance_id, self.group_id
                    );
                    self.inner.write().await.state = GroupState::Dead;
                    return Err(KrafkaError::broker(
                        hb_response.error_code,
                        format!(
                            "group.instance.id {:?} is already in use by another member of \
                             group '{}'",
                            self.group_instance_id, self.group_id
                        ),
                    ));
                }

                // Handle fencing and unknown member errors
                if hb_response.error_code == ErrorCode::UnknownMemberId
                    || hb_response.error_code == ErrorCode::FencedMemberEpoch
                {
                    warn!(
                        "ConsumerGroupHeartbeat error for group '{}': {:?} — resetting member state",
                        self.group_id, hb_response.error_code
                    );
                    if self.is_consumer_protocol() {
                        // KIP-848 is explicit that a fenced member "is expected
                        // to immediately give up all its partitions and rejoin
                        // the group with a full heartbeat ... and a member epoch
                        // equal to zero". Zeroing the epoch alone satisfies only
                        // the second half: the local assignment would survive,
                        // and this consumer would keep fetching and committing
                        // partitions the coordinator has already handed to
                        // another member — a silent split-brain over those
                        // partitions.
                        //
                        // `reset_for_kip848_fencing` exists for exactly this and
                        // is what the background heartbeat task's fencing path
                        // ends up running; using it here keeps the two paths from
                        // disagreeing about what fencing means.
                        self.reset_for_kip848_fencing().await;
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
            let mut inner = self.inner.write().await;
            if inner.member_id != *new_member_id {
                if !inner.member_id.is_empty() {
                    self.sticky_assignor.clear_member(&inner.member_id);
                }
                inner.member_id = new_member_id.clone();
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
            {
                let mut inner = self.inner.write().await;
                inner.assignment = new_assignment;
                inner.state = GroupState::Stable;
            }

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
                self.inner.write().await.assignment = resolved;

                if still_unresolved {
                    return Err(KrafkaError::protocol_kind(
                        ProtocolErrorKind::Malformed,
                        "ConsumerGroupHeartbeat assignment contains topic UUIDs that could not \
                         be resolved after metadata refresh. KIP-848 requires Metadata v10+ \
                         to map topic IDs to names.",
                    ));
                }
            }
        } else if hb_response.member_epoch > 0 {
            // A null Assignment means "nothing changed since your last
            // heartbeat" — it is the *normal* steady-state response, not an
            // absence of membership. The coordinator only sends the field when
            // the assignment actually moves.
            //
            // Treating a null assignment as "not yet joined" left the member
            // stuck outside `Stable`, which `needs_rejoin()` reads as "rejoin
            // required", so the poll loop sent another full heartbeat, got
            // another null assignment, and looped — tens of thousands of
            // ConsumerGroupHeartbeat requests per second against the
            // coordinator. Acknowledging the epoch is what closes that loop:
            // an accepted non-zero epoch *is* the coordinator confirming
            // membership.
            let mut inner = self.inner.write().await;
            if inner.state != GroupState::Stable {
                debug!(
                    "ConsumerGroupHeartbeat for group '{}' accepted at epoch {} with no \
                     assignment change; membership is stable",
                    self.group_id, hb_response.member_epoch
                );
                inner.state = GroupState::Stable;
            }
        }

        debug!(
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
        let new_topics = topics.to_vec();
        let state = self.inner.read().await.state;
        match state {
            GroupState::Stable => {
                // Already stable — check if topics changed
                let old_topics = self.subscribed_topics.read().await.clone();
                let mut old_sorted = old_topics;
                old_sorted.sort();
                let mut new_sorted = new_topics.clone();
                new_sorted.sort();
                if old_sorted == new_sorted {
                    return Ok((self.inner.read().await.assignment.clone(), false));
                }
                // Topics changed — send heartbeat with new subscription
            }
            GroupState::Unjoined if self.coordinator_conn.read().await.is_none() => {
                // Need to find coordinator first
                self.find_coordinator().await?;
            }
            GroupState::Unjoined => {}
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

        // A joining member (member_epoch == 0) must send an *empty* owned
        // partition list, not a null one. `None` encodes a null compact array,
        // which brokers reject with INVALID_REQUEST at epoch 0 — the member
        // then never joins at all. An empty list is the correct way to say
        // "I currently own nothing".
        let owned_partitions = if *self.member_epoch.read().await == 0 {
            Some(Vec::new())
        } else {
            None
        };

        let resp = self
            .consumer_group_heartbeat(subscribed, owned_partitions)
            .await?;

        // Start heartbeat task for KIP-848
        self.start_consumer_heartbeat_task(resp.heartbeat_interval_ms)
            .await;

        let inner = self.inner.read().await;
        let joined = matches!(inner.state, GroupState::Stable);
        let assignment = inner.assignment.clone();
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
        let inner_ref = self.inner.clone();
        let member_epoch_ref = self.member_epoch.clone();
        let coordinator_conn_ref = self.coordinator_conn.clone();
        let group_instance_id = self.group_instance_id.clone();
        let client_rack = self.client_rack.clone();
        let metadata_ref = self.metadata.clone();
        let target_assignment_ref = self.target_assignment.clone();
        let owned_assignment_ref = self.owned_assignment.clone();
        let topic_names_cache_ref = self.topic_names_cache.clone();
        let subscribed_topics_snapshot = self.subscribed_topics.read().await.clone();
        let rebalance_timeout = self.rebalance_timeout;
        let fatal_error_ref = self.fatal_error.clone();

        tokio::spawn(async move {
            debug!(
                "Starting KIP-848 heartbeat task for group '{}' (interval={:?})",
                group_id, interval
            );

            // Grows while unrecognised error codes keep coming back and resets
            // on the first success, so a persistent unknown failure degrades to
            // slow polling rather than saturating the coordinator.
            let mut unknown_error_backoff = Duration::ZERO;

            // Negotiate the ConsumerGroupHeartbeat version once at task start.
            // Only mark the controller as running after successful negotiation
            // so that early-return paths don't leave it stuck in a running state.
            let hb_version = {
                let coordinator_conn = coordinator_conn_ref.read().await.clone();
                if let Some(ref conn) = coordinator_conn {
                    match conn.negotiate_api_version(
                        ApiKey::ConsumerGroupHeartbeat,
                        CONSUMER_GROUP_HEARTBEAT_MAX,
                        CONSUMER_GROUP_HEARTBEAT_MIN,
                    ) {
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
            // Track the current interval in ms so we can compare against broker-provided
            // updates. Use the clamped value derived from interval_ms to stay consistent.
            let mut current_interval_ms = interval_ms.max(1000);
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
                        let member_id = inner_ref.read().await.member_id.clone();
                        let epoch = *member_epoch_ref.read().await;

                        if let Some(ref conn) = coordinator_conn {
                            // Report what this member actually owns, not what
                            // the coordinator most recently asked it to own —
                            // the two differ for the whole window between a
                            // heartbeat response arriving and the consumer
                            // finishing its revocation callbacks.
                            //
                            // At epoch 0 the list must be present-but-empty:
                            // a null array is rejected with INVALID_REQUEST.
                            let owned_partitions = {
                                let owned = owned_assignment_ref.read().await;
                                if owned.is_empty() && epoch != 0 {
                                    None
                                } else {
                                    Some(owned.clone())
                                }
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
                                rack_id: client_rack.clone(),
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
                                                // A good response clears any accumulated
                                                // unknown-error backoff.
                                                unknown_error_backoff = Duration::ZERO;
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
                                                    {
                                                        let mut inner = inner_ref.write().await;
                                                        inner.assignment = resolved;
                                                        inner.state = GroupState::Stable;
                                                    }

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
                                                        inner_ref.write().await.assignment = re_resolved;

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
                                                        inner_ref.write().await.assignment = resolved;

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

                                                heartbeat_controller.heartbeat_success();
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
                                                heartbeat_controller.heartbeat_success();
                                            } else if resp.error_code == ErrorCode::UnreleasedInstanceId
                                            {
                                                // Terminal: another live member already
                                                // holds this group.instance.id. No amount
                                                // of retrying frees it, because the
                                                // duplicate is a deployment mistake rather
                                                // than a transient condition. Treating it
                                                // as recoverable fencing spins this task
                                                // forever while the consumer never joins
                                                // and never explains why.
                                                error!(
                                                    "group.instance.id {:?} is already in use by \
                                                     another live member of group '{}'; two \
                                                     processes cannot share one instance id. \
                                                     Stopping heartbeats.",
                                                    group_instance_id, group_id
                                                );
                                                *fatal_error_ref.lock() = Some(format!(
                                                    "group.instance.id {group_instance_id:?} is \
                                                     already in use by another member of group \
                                                     '{group_id}'"
                                                ));
                                                heartbeat_controller.signal_member_invalidated();
                                                break;
                                            } else if resp.error_code == ErrorCode::UnknownMemberId
                                                || resp.error_code == ErrorCode::FencedMemberEpoch
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
                                            } else if matches!(
                                                resp.error_code,
                                                ErrorCode::GroupAuthorizationFailed
                                                    | ErrorCode::InvalidRequest
                                                    | ErrorCode::InvalidGroupId
                                                    | ErrorCode::GroupMaxSizeReached
                                                    | ErrorCode::UnsupportedVersion
                                            ) {
                                                // None of these can be fixed by trying
                                                // again: the credentials are wrong, the
                                                // request or group id is malformed, the
                                                // group is full, or the broker does not
                                                // speak this version. Retrying them
                                                // silently leaves the application with a
                                                // consumer that never receives records and
                                                // no indication of the cause, so record the
                                                // error for poll() and stop.
                                                error!(
                                                    "KIP-848 non-retriable heartbeat error for \
                                                     '{}': {:?} ({:?})",
                                                    group_id, resp.error_code, resp.error_message
                                                );
                                                *fatal_error_ref.lock() = Some(format!(
                                                    "consumer group '{}' heartbeat failed with \
                                                     non-retriable error {:?}{}",
                                                    group_id,
                                                    resp.error_code,
                                                    resp.error_message
                                                        .as_deref()
                                                        .map(|m| format!(": {m}"))
                                                        .unwrap_or_default(),
                                                ));
                                                heartbeat_controller.signal_member_invalidated();
                                                break;
                                            } else {
                                                // Unrecognised code: retry, but back off so
                                                // a persistent unknown error cannot become a
                                                // hot loop against the coordinator.
                                                send_full_heartbeat = true;
                                                unknown_error_backoff = (unknown_error_backoff * 2)
                                                    .clamp(
                                                        Duration::from_millis(100),
                                                        Duration::from_secs(30),
                                                    );
                                                warn!(
                                                    "KIP-848 heartbeat error for '{}': {:?}; \
                                                     retrying in {:?}",
                                                    group_id, resp.error_code, unknown_error_backoff
                                                );
                                                tokio::time::sleep(unknown_error_backoff).await;
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
        let (member_id, generation_id) = {
            let inner = self.inner.read().await;
            (inner.member_id.clone(), inner.generation_id)
        };

        let request = HeartbeatRequest {
            group_id: self.group_id.clone(),
            generation_id,
            member_id,
            group_instance_id: self.group_instance_id.clone(),
        };

        // Negotiate heartbeat version with broker (MIN=3, KIP-345 static membership).
        let hb_version = conn
            .negotiate_api_version(ApiKey::Heartbeat, HEARTBEAT_MAX, HEARTBEAT_MIN)
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    format!(
                        "broker does not support Heartbeat v{}-v{}",
                        HEARTBEAT_MIN, HEARTBEAT_MAX,
                    ),
                )
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
            self.heartbeat_controller.heartbeat_success();
        }

        Ok(status)
    }

    /// Handle inline heartbeat status by clearing member identity for
    /// session-invalidating errors before triggering a rejoin.
    ///
    /// Returns `true` if a rejoin was triggered and the caller should
    /// abort the current rebalance phase (return early from poll).
    pub async fn handle_inline_heartbeat_status(&self, status: HeartbeatStatus) -> bool {
        if !status.requires_rejoin() {
            return false;
        }

        if status.is_session_invalidating() {
            // The coordinator no longer recognises this member. Drop the
            // identity and hand the rejoin to poll(), which will re-register
            // from scratch.
            self.reset_member_identity().await;
            self.trigger_rejoin().await;
            return true;
        }

        // Plain REBALANCE_IN_PROGRESS.
        //
        // `trigger_rejoin` tells the heartbeat task to exit, which is the right
        // thing only when there is no heartbeat task to do the work. While one
        // is running it will read the same status on its next tick and drive
        // the JoinGroup/SyncGroup straight away; tearing it down here would
        // discard that and push the rejoin back onto the next poll() — the
        // delay this inline heartbeat exists to avoid — and would abandon a
        // rebalance already in flight.
        if self.heartbeat_controller.is_running() {
            return false;
        }

        self.trigger_rejoin().await;
        true
    }

    /// Commit offsets to the coordinator.
    pub async fn commit_offsets(
        &self,
        offsets: &HashMap<(String, PartitionId), crate::consumer::CommitPosition>,
    ) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }

        // A commit is valid whenever this member holds a generation the
        // coordinator will still accept — not only while the group is Stable.
        //
        // Gating on `state == Stable` breaks the most important commit of all:
        // the one issued just before partitions are revoked. By the time a
        // rebalance is known to be starting the state has already moved to
        // PreparingRebalance, so a Stable-only gate rejects every pre-rebalance
        // commit and the consumer silently hands its partitions to another
        // member while still holding uncommitted progress. The next owner then
        // re-reads from the last periodic commit.
        //
        // The coordinator's own rule is the generation, which is what Java
        // checks (`generation != NO_GENERATION`): the commit is fenced if the
        // generation is stale, and accepted otherwise. Mirror that here and
        // let the broker be the authority.
        {
            let inner = self.inner.read().await;
            let state = inner.state;
            let generation_id = inner.generation_id;
            drop(inner);

            if state == GroupState::Dead {
                return Err(KrafkaError::invalid_state(
                    "cannot commit offsets: group is dead",
                ));
            }

            // The KIP-848 protocol tracks liveness through the member epoch
            // rather than the classic generation id.
            let has_generation = if self.is_consumer_protocol() {
                *self.member_epoch.read().await >= 0
            } else {
                generation_id >= 0
            };

            if !has_generation {
                return Err(KrafkaError::invalid_state(format!(
                    "cannot commit offsets: no valid generation (group state is {state:?})",
                )));
            }
        }

        let conn = self.get_coordinator_connection().await?;

        let oc_version = conn
            .negotiate_api_version(ApiKey::OffsetCommit, OFFSET_COMMIT_MAX, OFFSET_COMMIT_MIN)
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    format!(
                        "broker does not support OffsetCommit v{}-v{}",
                        OFFSET_COMMIT_MIN, OFFSET_COMMIT_MAX,
                    ),
                )
            })?;

        let member_id = self.inner.read().await.member_id.clone();
        // carries the member epoch instead of the classic generation ID.
        // This semantic overload is only valid from v9+ — at earlier versions
        // the broker strictly validates against the classic group generation,
        // so we fall back to the classic generation_id.
        let generation_id = if self.is_consumer_protocol() && oc_version >= 9 {
            *self.member_epoch.read().await
        } else {
            self.inner.read().await.generation_id
        };

        // Group offsets by topic
        let mut topics_map: HashMap<String, Vec<OffsetCommitRequestPartition>> = HashMap::new();
        for ((topic, partition), position) in offsets {
            topics_map
                .entry(topic.clone())
                .or_default()
                .push(OffsetCommitRequestPartition {
                    partition_index: *partition,
                    committed_offset: position.offset,
                    // Persisted from v6 onward and read back by `OffsetFetch`.
                    // This is what lets the *next* owner of the partition —
                    // after a restart or a rebalance — ask
                    // `OffsetsForLeaderEpoch` whether the log still contains
                    // this `(offset, epoch)` pair. Committing a hardcoded `-1`
                    // silently disables KIP-320 truncation detection across
                    // every commit boundary, which is exactly the window an
                    // unclean leader election opens.
                    committed_leader_epoch: position.leader_epoch,
                    commit_timestamp: -1,
                    committed_metadata: position.metadata.clone(),
                });
        }

        let mut topics: Vec<OffsetCommitRequestTopic> = topics_map
            .into_iter()
            .map(|(name, partitions)| OffsetCommitRequestTopic {
                name,
                topic_id: None,
                partitions,
            })
            .collect();

        // KIP-848 v10+: replace topic name with topic_id on the wire.
        // Fall back to v9 if any UUID is missing from the metadata cache.
        let oc_version = if oc_version >= 10 {
            let all_known = topics.iter_mut().all(|t| {
                if let Some(id) = self.metadata.topic_id_for_name(&t.name) {
                    t.topic_id = Some(id);
                    true
                } else {
                    false
                }
            });
            if all_known { oc_version } else { 9 }
        } else {
            oc_version
        };

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
        let mut commit_response = OffsetCommitResponse::decode_versioned(oc_version, &mut buf)?;

        // KIP-848 v10: response topics carry topic_id instead of name —
        // resolve back to name for downstream error messages.
        if oc_version >= 10 {
            for t in &mut commit_response.topics {
                if t.name.is_empty()
                    && let Some(id) = t.topic_id
                    && let Some(name) = self.metadata.topic_name_for_id(&id)
                {
                    t.name = name;
                }
            }
        }

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
                        self.inner.write().await.state = GroupState::PreparingRebalance;
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

        debug!(
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
    /// Fetch the group's committed offsets, absorbing an in-flight
    /// transactional commit.
    ///
    /// A `read_committed` consumer asks for **stable** offsets (KIP-447), and
    /// the coordinator answers `UNSTABLE_OFFSET_COMMIT` for any partition whose
    /// offset is staged inside a transaction that has not resolved. That is a
    /// wait, not a failure: the transaction commits or aborts within its own
    /// timeout, and the answer changes.
    ///
    /// The retry lives here rather than at the call sites because those are
    /// `subscribe()` and the rebalance's assignment step — failing either
    /// because some unrelated producer happened to have a transaction open is
    /// not a useful error. Backoff is jittered: a rebalancing group has every
    /// member calling this against the same coordinator at the same moment.
    ///
    /// Bounded, not unbounded. A transaction may legitimately stay open for
    /// `transaction.timeout.ms` (60 s by default), which is longer than the
    /// session timeout — so past this budget the error is surfaced, and the
    /// poll loop's own offset-resolution retry takes over as the outer loop.
    pub async fn fetch_committed_offsets(
        &self,
        partitions: &HashMap<String, Vec<crate::PartitionId>>,
    ) -> Result<HashMap<(String, crate::PartitionId), crate::consumer::CommittedPosition>> {
        let backoff = crate::util::BackoffPolicy::default();
        let mut last_error = None;

        for attempt in 0..UNSTABLE_OFFSET_MAX_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(backoff.calculate_backoff(attempt)).await;
            }
            match self.fetch_committed_offsets_once(partitions).await {
                Err(error)
                    if matches!(
                        error,
                        KrafkaError::Broker {
                            code: ErrorCode::UnstableOffsetCommit,
                            ..
                        }
                    ) =>
                {
                    debug!(
                        group = %self.group_id,
                        attempt = attempt + 1,
                        "committed offsets are staged inside an in-flight transaction; retrying"
                    );
                    last_error = Some(error);
                }
                other => return other,
            }
        }

        Err(last_error.unwrap_or_else(|| {
            KrafkaError::broker(
                ErrorCode::UnstableOffsetCommit,
                "committed offsets did not stabilise",
            )
        }))
    }

    async fn fetch_committed_offsets_once(
        &self,
        partitions: &HashMap<String, Vec<crate::PartitionId>>,
    ) -> Result<HashMap<(String, crate::PartitionId), crate::consumer::CommittedPosition>> {
        if partitions.is_empty() {
            return Ok(HashMap::new());
        }

        let conn = self.get_coordinator_connection().await?;

        let mut topics: Vec<OffsetFetchRequestTopic> = partitions
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
        // adds MemberId/MemberEpoch for KIP-848 epoch validation,
        // v10 KIP-848 topic_id replaces topic name on the wire.
        let of_version = conn
            .negotiate_api_version(ApiKey::OffsetFetch, OFFSET_FETCH_MAX, OFFSET_FETCH_MIN)
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    format!(
                        "broker does not support OffsetFetch v{}-v{}",
                        OFFSET_FETCH_MIN, OFFSET_FETCH_MAX,
                    ),
                )
            })?;

        // KIP-848 v10+: replace topic name with topic_id on the wire.
        // Fall back to v9 if any UUID is missing from the metadata cache.
        let of_version = if of_version >= 10 {
            let all_known = topics.iter_mut().all(|t| {
                if let Some(id) = self.metadata.topic_id_for_name(&t.name) {
                    t.topic_id = Some(id);
                    true
                } else {
                    false
                }
            });
            if all_known { of_version } else { 9 }
        } else {
            of_version
        };

        // For KIP-848, populate MemberId/MemberEpoch so the broker can validate
        // membership and surface STALE_MEMBER_EPOCH when appropriate.
        // These fields only exist on the wire from v9+; at earlier versions
        // the encode path ignores them, so we leave defaults.
        let (offset_fetch_member_id, offset_fetch_member_epoch) =
            if self.is_consumer_protocol() && of_version >= 9 {
                (
                    Some(self.inner.read().await.member_id.clone()),
                    *self.member_epoch.read().await,
                )
            } else {
                (None, -1)
            };

        // KIP-447: a `read_committed` consumer must ask for **stable** offsets.
        //
        // Without this the coordinator returns the latest offset written to
        // `__consumer_offsets`, including one written by a `TxnOffsetCommit`
        // whose transaction has not completed. If that transaction later
        // aborts, the offset it staged is retracted — but a consumer that read
        // it has already resumed past those records, and they are never
        // reprocessed. Silent data loss on the exactly-once recovery path, in
        // the window a crash is most likely to land in.
        //
        // `read_uncommitted` consumers ask for the unstable value deliberately:
        // they are already reading uncommitted data, and blocking their
        // startup on an unrelated producer's open transaction would be worse.
        let require_stable = require_stable_for(self.isolation_level);

        let request = OffsetFetchRequest {
            group_id: self.group_id.clone(),
            topics: Some(topics),
            require_stable,
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
        let mut offset_response = OffsetFetchResponse::decode_versioned(of_version, &mut buf)?;

        // KIP-848 v10: response topics carry topic_id instead of name —
        // resolve back to name for downstream result map keys.
        if of_version >= 10 {
            for t in &mut offset_response.topics {
                if t.name.is_empty()
                    && let Some(id) = t.topic_id
                    && let Some(name) = self.metadata.topic_name_for_id(&id)
                {
                    t.name = name;
                }
            }
        }

        // Check group-level error (v2+ top-level ErrorCode, v8+ per-group ErrorCode).
        // Errors like NOT_COORDINATOR, STALE_MEMBER_EPOCH, or UNKNOWN_MEMBER_ID
        // appear here and must be surfaced before iterating partitions.
        if !offset_response.error_code.is_ok() {
            if offset_response.error_code == ErrorCode::StaleMemberEpoch
                || offset_response.error_code == ErrorCode::UnknownMemberId
                || offset_response.error_code == ErrorCode::FencedMemberEpoch
            {
                self.inner.write().await.state = GroupState::PreparingRebalance;
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

        // A partition whose offset is staged inside an in-flight transaction
        // answers `UNSTABLE_OFFSET_COMMIT` when `require_stable` is set. That
        // is a *retry*, not an absent offset.
        //
        // Falling through to the loop below would drop the partition from the
        // result, and every caller reads a missing entry as "this group has
        // never committed here" — which sends the partition through
        // `auto.offset.reset`. On an exactly-once pipeline that turns a
        // 200 ms wait for a transaction to resolve into a rewind to the start
        // of the topic, or a jump to its end. The silent-drop is the more
        // dangerous half of this defect: it only bites once `require_stable`
        // is set, so fixing the flag without this would have made things worse.
        if let Some((topic, partition)) = first_unstable_offset(&offset_response) {
            return Err(KrafkaError::broker(
                ErrorCode::UnstableOffsetCommit,
                format!(
                    "committed offset for {topic}-{partition} is staged inside an \
                     in-flight transaction; retry once it commits or aborts"
                ),
            ));
        }

        let mut result = HashMap::new();
        for topic in &offset_response.topics {
            for partition in &topic.partitions {
                if partition.error_code.is_ok() && partition.committed_offset >= 0 {
                    result.insert(
                        (topic.name.clone(), partition.partition_index),
                        crate::consumer::CommittedPosition {
                            offset: partition.committed_offset,
                            // Carried forward so the resumed position can be
                            // validated against the leader's log before the
                            // first fetch (KIP-320). `-1` when the group last
                            // committed without one.
                            leader_epoch: partition.committed_leader_epoch,
                        },
                    );
                }
            }
        }

        debug!(
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
                .ok_or_else(|| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::UnknownApiVersion,
                        format!(
                            "broker does not support ListOffsets v{}-v{}",
                            LIST_OFFSETS_MIN, LIST_OFFSETS_MAX,
                        ),
                    )
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
        // Drop any assignment a background rebalance parked before the reset
        // paths below get a chance to: a member on its way out must not leave
        // one behind for a later poll() to pick up. Done first so it also
        // covers the already-Unjoined early return.
        self.clear_pending_rebalance();

        let state = self.inner.read().await.state;
        if state == GroupState::Unjoined || state == GroupState::Dead {
            return Ok(());
        }

        // KIP-848: stop heartbeat first (prevent normal heartbeat from
        // racing with the leave-epoch heartbeat), then send the leave.
        // Static members are handled inside `leave_group_consumer`, which
        // sends member_epoch = -2 (temporary leave) rather than -1.
        if self.is_consumer_protocol() {
            self.stop_heartbeat_task().await;
            return self.leave_group_consumer().await;
        }

        // Static members (KIP-345) must NOT send LeaveGroup.
        //
        // The entire point of `group.instance.id` is that a member can restart
        // without disturbing the group: the coordinator holds the assignment
        // against the instance id for up to the session timeout and hands the
        // same partitions back when the process returns. Sending LeaveGroup
        // surrenders that reservation immediately and triggers the group-wide
        // rebalance that static membership exists to avoid — which would make
        // static membership strictly worse than dynamic, since it pays the
        // rebalance cost on every restart *and* the extra configuration.
        //
        // The Java client makes the same distinction by only leaving when
        // `isDynamicMember()` holds.
        if self.group_instance_id.is_some() {
            debug!(
                "Skipping LeaveGroup for static member of group '{}' (instance id {:?}); \
                 the coordinator retains the assignment until the session expires",
                self.group_id, self.group_instance_id
            );
            self.stop_heartbeat_task().await;
            self.reset_for_static_leave().await;
            return Ok(());
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

        let member_id = self.inner.read().await.member_id.clone();

        self.inner.write().await.state = GroupState::Leaving;

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
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    format!(
                        "broker does not support LeaveGroup v{}-v{}",
                        LEAVE_GROUP_MIN, LEAVE_GROUP_MAX,
                    ),
                )
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

        let member_id = self.inner.read().await.member_id.clone();
        self.inner.write().await.state = GroupState::Leaving;
        *self.member_epoch.write().await = leave_epoch;

        let request = ConsumerGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: member_id.clone(),
            member_epoch: leave_epoch,
            instance_id: self.group_instance_id.clone(),
            rack_id: self.client_rack.clone(),
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

        let Some(hb_version) = conn.negotiate_api_version(
            ApiKey::ConsumerGroupHeartbeat,
            CONSUMER_GROUP_HEARTBEAT_MAX,
            CONSUMER_GROUP_HEARTBEAT_MIN,
        ) else {
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
        self.clear_pending_rebalance();
        self.reset_member_identity().await;
        {
            let mut inner = self.inner.write().await;
            inner.state = GroupState::Unjoined;
            inner.assignment = MemberAssignment::empty();
        }
        self.target_assignment.write().await.clear();
        self.owned_assignment.write().await.clear();
        self.topic_names_cache.write().await.clear();
        *self.coordinator_conn.write().await = None;
        *self.coordinator_id.write().await = None;
    }

    /// Reset local state after a static member (KIP-345) shuts down without
    /// sending LeaveGroup.
    ///
    /// This drops everything tied to the current *session* — group state, the
    /// assignment, the KIP-848 target assignment, the topic-name cache, and
    /// the coordinator connection — but deliberately **keeps `member_id`**.
    ///
    /// Keeping it is the whole point. The coordinator still holds this
    /// instance's assignment against its `group.instance.id`, and it matches a
    /// returning process by the `(instance id, member id)` pair. A process
    /// that clears its member id and rejoins with an empty one, while the
    /// coordinator still has a live registration for that instance id, is
    /// treated as a *second* member claiming an already-owned instance and is
    /// rejected with `UNRELEASED_INSTANCE_ID` — the restart then fails
    /// repeatedly until the old session finally times out, which is exactly
    /// the outage static membership was configured to prevent.
    ///
    /// The sticky assignor entry is dropped along with the assignment so the
    /// two cannot disagree: a stale entry would make the next JoinGroup
    /// advertise ownership of partitions this consumer has already stopped
    /// fetching.
    async fn reset_for_static_leave(&self) {
        self.clear_pending_rebalance();
        let member_id = self.inner.read().await.member_id.clone();
        if !member_id.is_empty() {
            self.sticky_assignor.clear_member(&member_id);
        }
        {
            let mut inner = self.inner.write().await;
            // The generation is definitely stale once we stop heartbeating;
            // only the member id survives.
            inner.generation_id = -1;
            inner.state = GroupState::Unjoined;
            inner.assignment = MemberAssignment::empty();
        }
        self.target_assignment.write().await.clear();
        self.owned_assignment.write().await.clear();
        self.topic_names_cache.write().await.clear();
        *self.coordinator_conn.write().await = None;
        *self.coordinator_id.write().await = None;
    }

    /// Reset group state for KIP-848 fencing errors (FencedMemberEpoch,
    /// UnknownMemberId, UnreleasedInstanceId).
    ///
    /// Unlike `reset_member_identity`, this preserves `member_id`:
    /// KIP-848 requires fenced members to "rejoin with the same member id
    /// and epoch 0". Sticky assignor, assignment, and target state are
    /// cleared because the coordinator revoked all partitions on fencing.
    async fn reset_for_kip848_fencing(&self) {
        // Signal before mutating: the consumer's map is the one that gates
        // commits, and it must be dropped whether or not anything below
        // succeeds.
        self.membership_lost
            .store(true, std::sync::atomic::Ordering::Release);
        self.clear_pending_rebalance();
        let member_id = self.inner.read().await.member_id.clone();
        if !member_id.is_empty() {
            self.sticky_assignor.clear_member(&member_id);
        }
        *self.member_epoch.write().await = 0;
        {
            let mut inner = self.inner.write().await;
            inner.generation_id = -1;
            inner.state = GroupState::Unjoined;
            inner.assignment = MemberAssignment::empty();
        }
        self.target_assignment.write().await.clear();
        self.owned_assignment.write().await.clear();
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
        let mut inner = self.inner.write().await;
        if !inner.member_id.is_empty() {
            self.sticky_assignor.clear_member(&inner.member_id);
        }
        inner.member_id.clear();
        inner.generation_id = -1;
        drop(inner);
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
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::InvalidLength,
                    format!(
                        "topic name '{}' exceeds Kafka i16 length limit ({} bytes)",
                        topic,
                        topic.len()
                    ),
                )
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
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::InvalidLength,
                        format!("topic name '{}' exceeds Kafka i16 length limit", topic),
                    )
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
                match String::from_utf8(buf.copy_to_bytes(len as usize).to_vec()) {
                    Ok(t) => topics.push(t),
                    Err(e) => {
                        warn!("decode_consumer_metadata: invalid UTF-8 in topic name: {e}");
                        return (topics, HashMap::new());
                    }
                }
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
                let topic = match String::from_utf8(buf.copy_to_bytes(len as usize).to_vec()) {
                    Ok(t) => t,
                    Err(e) => {
                        warn!("decode_consumer_metadata: invalid UTF-8 in owned topic name: {e}");
                        return (topics, owned);
                    }
                };
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
            let topic = String::from_utf8(buf.copy_to_bytes(topic_len).to_vec()).map_err(|e| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::InvalidUtf8,
                    format!("invalid UTF-8 in assignment topic name: {e}"),
                )
            })?;

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
    fn compute_assignments(
        &self,
        topics: &[String],
        members: &[JoinGroupResponseMember],
    ) -> Result<Vec<SyncGroupRequestAssignment>> {
        // Get partition info for all topics
        let mut topic_partitions: HashMap<String, Vec<PartitionId>> = HashMap::new();
        for topic in topics {
            if let Some(topic_info) = self.metadata.topic(topic) {
                let partitions: Vec<_> = topic_info
                    .partitions
                    .values()
                    .map(|p| p.partition)
                    .collect();
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
                metadata: m.metadata.clone(),
                assignment: Bytes::new(),
            })
            .collect();

        // Assign using the protocol the coordinator selected for the group,
        // not this member's preference — as leader we are computing the
        // assignment on behalf of every member, so it must match the protocol
        // they all agreed on.
        let assignments = match self.negotiated_strategy() {
            crate::consumer::config::PartitionAssignmentStrategy::Range => {
                let assignor = RangeAssignor;
                assignor.assign(topics, &topic_partitions, &group_members)
            }
            crate::consumer::config::PartitionAssignmentStrategy::RoundRobin => {
                let assignor = RoundRobinAssignor;
                assignor.assign(topics, &topic_partitions, &group_members)
            }
            // Both sticky variants compute the same placement; they differ
            // only in whether partitions are revoked eagerly or incrementally,
            // which is handled by the rebalance path rather than here. The
            // coordinator's persistent sticky state is shared between them.
            crate::consumer::config::PartitionAssignmentStrategy::Sticky
            | crate::consumer::config::PartitionAssignmentStrategy::CooperativeSticky => self
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
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::InvalidLength,
                    format!(
                        "topic name '{}' exceeds Kafka i16 length limit ({} bytes)",
                        topic,
                        topic.len()
                    ),
                )
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
        if let Some(elapsed) = self.heartbeat_controller.time_since_last_heartbeat() {
            elapsed > self.heartbeat_interval
        } else {
            // No heartbeat recorded yet, should send one
            self.inner.read().await.state == GroupState::Stable
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
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {

    // ── KIP-447: stable committed offsets ─────────────────────────────────
    //
    // A `read_committed` consumer that resumes from an offset staged inside a
    // transaction which later *aborts* has skipped records the abort was
    // supposed to make it reprocess. Silent data loss on the exactly-once
    // recovery path, in the window a crash is most likely to land in.

    /// `require_stable` follows the isolation level, and only that.
    #[test]
    fn require_stable_is_asked_for_only_under_read_committed() {
        use crate::consumer::IsolationLevel;

        assert!(
            require_stable_for(IsolationLevel::ReadCommitted.to_i8()),
            "a read_committed consumer must not resume from an unresolved \
             transactional offset"
        );
        assert!(
            !require_stable_for(IsolationLevel::ReadUncommitted.to_i8()),
            "a read_uncommitted consumer must not be blocked on someone \
             else's open transaction"
        );
    }

    fn offset_fetch_partition(
        partition_index: crate::PartitionId,
        error_code: ErrorCode,
    ) -> crate::protocol::OffsetFetchResponsePartition {
        crate::protocol::OffsetFetchResponsePartition {
            partition_index,
            committed_offset: 42,
            committed_leader_epoch: 7,
            metadata: None,
            error_code,
        }
    }

    fn offset_fetch_response(
        partitions: Vec<crate::protocol::OffsetFetchResponsePartition>,
    ) -> OffsetFetchResponse {
        OffsetFetchResponse {
            throttle_time_ms: 0,
            topics: vec![crate::protocol::OffsetFetchResponseTopic {
                name: "orders".to_string(),
                topic_id: None,
                partitions,
            }],
            error_code: ErrorCode::None,
        }
    }

    /// An `UNSTABLE_OFFSET_COMMIT` partition must be *noticed*.
    ///
    /// This is the more dangerous half of the defect, and it only becomes
    /// reachable once `require_stable` is set — so fixing the flag without
    /// this would have made things worse than leaving both alone. The
    /// result-building loop keeps partitions whose `error_code.is_ok()`, so an
    /// unnoticed unstable partition is simply absent from the map, and every
    /// caller reads a missing entry as "this group never committed here":
    /// `auto.offset.reset`, i.e. a rewind to the start of the topic or a jump
    /// to its end, in place of a few hundred milliseconds of waiting.
    #[test]
    fn an_unstable_offset_is_reported_rather_than_dropped() {
        let response = offset_fetch_response(vec![
            offset_fetch_partition(0, ErrorCode::None),
            offset_fetch_partition(1, ErrorCode::UnstableOffsetCommit),
        ]);

        assert_eq!(
            first_unstable_offset(&response),
            Some(("orders", 1)),
            "a partition staged inside an in-flight transaction must be surfaced"
        );
    }

    /// A clean response reports nothing, so the common path pays no penalty.
    #[test]
    fn a_stable_response_reports_no_unstable_partition() {
        let response = offset_fetch_response(vec![
            offset_fetch_partition(0, ErrorCode::None),
            // An unrelated per-partition error must not be mistaken for one.
            offset_fetch_partition(1, ErrorCode::UnknownTopicOrPartition),
        ]);
        assert_eq!(first_unstable_offset(&response), None);
    }

    /// The retry must be able to end, and `UNSTABLE_OFFSET_COMMIT` must be
    /// classified retriable or the outer poll loop would give up on it.
    #[test]
    fn the_unstable_offset_retry_is_bounded_and_the_error_is_retriable() {
        assert!((2..=10).contains(&UNSTABLE_OFFSET_MAX_ATTEMPTS));
        assert!(
            ErrorCode::UnstableOffsetCommit.is_retriable(),
            "the poll loop's offset-resolution retry is the outer loop here"
        );
    }

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

    /// A `JoinGroup` must be given the group's rebalance window, not the
    /// ordinary request budget.
    ///
    /// The coordinator parks a `JoinGroup` until the rebalance it belongs to
    /// converges — every other member rejoining, or the rebalance timeout
    /// elapsing. With the default consumer configuration that window is
    /// `max.poll.interval.ms` (5 min) while `request.timeout.ms` is 30 s, so
    /// bounding the join by the latter aborts healthy rebalances client-side:
    /// any rebalance that needs longer than 30 s to converge (for instance one
    /// waiting on an idle member's 45 s session to lapse) fails the join
    /// instead of completing it.
    #[test]
    fn test_join_group_timeout_covers_the_rebalance_window() {
        let coordinator =
            test_coordinator(crate::consumer::config::PartitionAssignmentStrategy::Range);

        // test_coordinator is built with a 30 s rebalance timeout.
        assert_eq!(
            coordinator.join_group_timeout(),
            Duration::from_secs(30) + JOIN_GROUP_TIMEOUT_SLACK,
        );
        assert!(
            coordinator.join_group_timeout() > coordinator.rebalance_timeout,
            "the join budget must outlast the rebalance window so the \
             coordinator's answer can reach us"
        );

        // With the shipped consumer defaults the join budget must comfortably
        // exceed the default request timeout of 30 s.
        let defaults = crate::consumer::config::ConsumerConfig::default();
        let coordinator = GroupCoordinator::new(
            "test-group",
            Arc::new(ConnectionPool::new(
                crate::network::ConnectionConfig::default(),
            )),
            Arc::new(ClusterMetadata::new(
                vec!["localhost:9092".to_string()],
                Arc::new(ConnectionPool::new(
                    crate::network::ConnectionConfig::default(),
                )),
                Duration::from_secs(300),
            )),
            defaults.session_timeout(),
            defaults.heartbeat_interval(),
            defaults.max_poll_interval(),
        );
        assert!(
            coordinator.join_group_timeout() > defaults.request_timeout(),
            "join budget {:?} must exceed request_timeout {:?}",
            coordinator.join_group_timeout(),
            defaults.request_timeout(),
        );
        assert!(
            coordinator.join_group_timeout() > defaults.session_timeout(),
            "join budget {:?} must outlast session_timeout {:?}, or a rebalance \
             that waits for an idle member's session to lapse can never complete",
            coordinator.join_group_timeout(),
            defaults.session_timeout(),
        );
    }

    // ── Background rebalance hand-off ──────────────────────────────────────
    //
    // The heartbeat task runs JoinGroup/SyncGroup and parks the result; poll()
    // consumes it and applies the callbacks and data-plane changes. These tests
    // pin down the hand-off itself — that the assignment survives exactly one
    // trip across it, and that every path which invalidates the generation
    // drops it rather than letting poll() apply a superseded view.

    fn pending(topic: &str, partitions: Vec<PartitionId>) -> PendingRebalance {
        let mut assignment = MemberAssignment::empty();
        assignment.add(topic, partitions);
        PendingRebalance {
            assignment,
            to_revoke: Vec::new(),
        }
    }

    #[test]
    fn test_pending_rebalance_is_delivered_exactly_once() {
        let coordinator =
            test_coordinator(crate::consumer::config::PartitionAssignmentStrategy::Range);

        assert!(
            coordinator.take_pending_rebalance().is_none(),
            "a coordinator that has never rebalanced has nothing parked"
        );

        *coordinator.pending_rebalance.lock() = Some(pending("t", vec![0, 1]));

        let taken = coordinator
            .take_pending_rebalance()
            .expect("the parked assignment must be handed to the first caller");
        assert_eq!(taken.assignment.get("t"), Some([0, 1].as_slice()));

        assert!(
            coordinator.take_pending_rebalance().is_none(),
            "taking must consume: a second poll() re-applying the same \
             assignment would re-fire on_partitions_assigned for partitions \
             it already owns"
        );
    }

    #[tokio::test]
    async fn test_trigger_rejoin_drops_a_parked_assignment() {
        let coordinator =
            test_coordinator(crate::consumer::config::PartitionAssignmentStrategy::Range);
        *coordinator.pending_rebalance.lock() = Some(pending("t", vec![0]));

        coordinator.trigger_rejoin().await;

        assert!(
            coordinator.take_pending_rebalance().is_none(),
            "the parked assignment predates the rejoin just requested, so \
             applying it would install a generation the group has left behind"
        );
        assert_eq!(coordinator.state().await, GroupState::PreparingRebalance);
    }

    #[tokio::test]
    async fn test_reset_drops_a_parked_assignment() {
        let coordinator =
            test_coordinator(crate::consumer::config::PartitionAssignmentStrategy::Range);
        *coordinator.pending_rebalance.lock() = Some(pending("t", vec![0]));

        coordinator.reset().await;

        assert!(
            coordinator.take_pending_rebalance().is_none(),
            "a consumer that has left the group must not go on to assign \
             itself partitions from its old membership"
        );
    }

    #[tokio::test]
    async fn test_starting_the_heartbeat_task_drops_a_parked_assignment() {
        let coordinator = Arc::new(test_coordinator(
            crate::consumer::config::PartitionAssignmentStrategy::Range,
        ));
        *coordinator.pending_rebalance.lock() = Some(pending("t", vec![0]));

        // start_heartbeat_task runs after a fresh join/sync, whose assignment
        // supersedes anything the previous task parked.
        coordinator.start_heartbeat_task().await;
        coordinator.stop_heartbeat_task().await;

        assert!(coordinator.take_pending_rebalance().is_none());
    }

    #[tokio::test]
    async fn test_await_rejoin_returns_immediately_when_idle() {
        let coordinator =
            test_coordinator(crate::consumer::config::PartitionAssignmentStrategy::Range);

        assert!(!coordinator.rejoin_in_flight());

        // A poll() with no background rebalance running must not spend any of
        // its budget here — this is the ordinary path through every poll.
        let start = std::time::Instant::now();
        coordinator.await_rejoin(Duration::from_secs(30)).await;
        assert!(
            start.elapsed() < Duration::from_secs(1),
            "await_rejoin blocked for {:?} with no rebalance in flight",
            start.elapsed()
        );
    }

    #[tokio::test]
    async fn test_await_rejoin_wakes_when_the_rebalance_finishes() {
        let coordinator = Arc::new(test_coordinator(
            crate::consumer::config::PartitionAssignmentStrategy::Range,
        ));
        coordinator.rejoin_in_flight.send_replace(true);
        assert!(coordinator.rejoin_in_flight());

        let finisher = coordinator.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            finisher.rejoin_in_flight.send_replace(false);
        });

        // The budget here is far longer than the rebalance takes: poll() must
        // resume as soon as the assignment is ready, not sit out its timeout.
        let start = std::time::Instant::now();
        coordinator.await_rejoin(Duration::from_secs(30)).await;
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "await_rejoin did not wake on completion; waited {:?}",
            start.elapsed()
        );
        assert!(!coordinator.rejoin_in_flight());
    }

    #[tokio::test]
    async fn test_await_rejoin_gives_up_after_the_budget() {
        let coordinator =
            test_coordinator(crate::consumer::config::PartitionAssignmentStrategy::Range);
        coordinator.rejoin_in_flight.send_replace(true);

        // A rebalance that is genuinely stuck — waiting on some other member
        // that has stopped responding — must not hold poll() past its timeout.
        let start = std::time::Instant::now();
        coordinator.await_rejoin(Duration::from_millis(100)).await;
        assert!(start.elapsed() >= Duration::from_millis(100));
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "await_rejoin overran its budget by {:?}",
            start.elapsed()
        );
        assert!(
            coordinator.rejoin_in_flight(),
            "the rebalance is still running; poll() reports an empty result \
             rather than assuming it finished"
        );
    }

    #[tokio::test]
    async fn test_eager_protocols_never_produce_cooperative_revocations() {
        let coordinator =
            test_coordinator(crate::consumer::config::PartitionAssignmentStrategy::Range);
        let mut assignment = MemberAssignment::empty();
        assignment.add("t", vec![0, 1]);

        assert!(
            coordinator
                .cooperative_revocations(&assignment)
                .await
                .is_empty(),
            "eager rebalances revoke everything and start over; a partial \
             revocation list would make poll() take the two-round path"
        );
    }

    /// `stop_heartbeat_task` asks the current task to exit but does not wait
    /// for it, so a task can still be shutting down while its successor is
    /// live. Each task therefore carries an epoch and only cleans up if it is
    /// still the current one — otherwise a straggler would reset the group to
    /// PreparingRebalance on top of the join its successor had just completed,
    /// forcing a rebalance nothing asked for.
    #[tokio::test]
    async fn test_each_heartbeat_task_gets_a_newer_epoch() {
        let coordinator = Arc::new(test_coordinator(
            crate::consumer::config::PartitionAssignmentStrategy::Range,
        ));
        let epoch_of = || {
            coordinator
                .heartbeat_epoch
                .load(std::sync::atomic::Ordering::Acquire)
        };

        let initial = epoch_of();
        coordinator.start_heartbeat_task().await;
        let first = epoch_of();
        coordinator.start_heartbeat_task().await;
        let second = epoch_of();
        coordinator.stop_heartbeat_task().await;

        assert!(
            first > initial && second > first,
            "each task must claim a strictly newer epoch ({initial} -> {first} -> {second})"
        );
    }

    /// An inline heartbeat that reports a plain rebalance must not tear down a
    /// running heartbeat task: that task is the thing that rejoins promptly,
    /// and `trigger_rejoin` would stop it and push the work back onto the next
    /// `poll()` — reinstating exactly the delay this change removes.
    #[tokio::test]
    async fn test_inline_heartbeat_leaves_a_running_task_to_rebalance() {
        let coordinator =
            test_coordinator(crate::consumer::config::PartitionAssignmentStrategy::Range);
        coordinator.heartbeat_controller.start();
        *coordinator.pending_rebalance.lock() = Some(pending("t", vec![0]));

        let handled = coordinator
            .handle_inline_heartbeat_status(HeartbeatStatus::RebalanceNeeded)
            .await;

        assert!(
            !handled,
            "poll() must not treat the rebalance as its own to drive"
        );
        assert_ne!(
            coordinator.state().await,
            GroupState::PreparingRebalance,
            "the heartbeat task owns this rebalance; poll() must not reset the state under it"
        );
        assert!(
            coordinator.take_pending_rebalance().is_some(),
            "a parked assignment must survive an inline heartbeat"
        );
    }

    /// With no heartbeat task running there is nothing to drive the rejoin in
    /// the background, so the inline heartbeat must fall back to handing it to
    /// `poll()`.
    #[tokio::test]
    async fn test_inline_heartbeat_drives_the_rejoin_when_no_task_runs() {
        let coordinator =
            test_coordinator(crate::consumer::config::PartitionAssignmentStrategy::Range);
        assert!(!coordinator.heartbeat_controller.is_running());

        let handled = coordinator
            .handle_inline_heartbeat_status(HeartbeatStatus::RebalanceNeeded)
            .await;

        assert!(handled);
        assert_eq!(coordinator.state().await, GroupState::PreparingRebalance);
    }

    /// A session-invalidating heartbeat outranks a parked assignment: the
    /// coordinator has forgotten this member, so the generation the assignment
    /// belongs to no longer exists. Applying it would have `poll()` fetch
    /// partitions alongside whichever member was given them instead.
    #[tokio::test]
    async fn test_session_invalidation_outranks_a_parked_assignment() {
        let coordinator =
            test_coordinator(crate::consumer::config::PartitionAssignmentStrategy::Range);
        coordinator.heartbeat_controller.start();
        *coordinator.pending_rebalance.lock() = Some(pending("t", vec![0]));

        let handled = coordinator
            .handle_inline_heartbeat_status(HeartbeatStatus::UnknownMember)
            .await;

        assert!(handled, "an invalidated member must rejoin from poll()");
        assert!(
            coordinator.take_pending_rebalance().is_none(),
            "the parked assignment belongs to a generation the coordinator \
             has discarded"
        );
    }

    /// `REBALANCE_IN_PROGRESS` is the one rejoin reason that leaves the session
    /// intact, which is why the heartbeat task now keeps heartbeating through
    /// it instead of exiting. If it were ever reclassified as
    /// session-invalidating, the task would start clearing member identity on
    /// every ordinary rebalance and every member would lose its sticky
    /// assignment.
    #[test]
    fn test_rebalance_in_progress_does_not_invalidate_the_session() {
        assert!(HeartbeatStatus::RebalanceNeeded.requires_rejoin());
        assert!(!HeartbeatStatus::RebalanceNeeded.is_session_invalidating());

        for status in [
            HeartbeatStatus::UnknownMember,
            HeartbeatStatus::IllegalGeneration,
            HeartbeatStatus::SessionTimeout,
        ] {
            assert!(
                status.is_session_invalidating(),
                "{status:?} must still stop the heartbeat task"
            );
        }
    }

    #[test]
    fn test_member_assignment() {
        let mut assignment = MemberAssignment::empty();
        assert!(assignment.is_empty());

        assignment.add("topic1", vec![0, 1, 2]);
        assignment.add("topic2", vec![0, 1]);

        assert!(!assignment.is_empty());
        assert_eq!(assignment.get("topic1"), Some(vec![0, 1, 2].as_slice()));
        assert_eq!(assignment.all_partitions().count(), 5);
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

    #[tokio::test]
    async fn test_kip848_fencing_reset_clears_sticky_assignor_state() {
        let coordinator = test_coordinator(
            crate::consumer::config::PartitionAssignmentStrategy::CooperativeSticky,
        )
        .with_group_protocol(crate::consumer::config::GroupProtocol::Consumer);

        {
            let mut inner = coordinator.inner.write().await;
            inner.member_id = "member-1".to_string();
            inner.generation_id = 42;
            inner.state = GroupState::Stable;
        }
        *coordinator.member_epoch.write().await = 7;

        let mut assignment = MemberAssignment::empty();
        assignment.add("topic-a", vec![0, 1]);
        coordinator.inner.write().await.assignment = assignment.clone();
        coordinator
            .sticky_assignor
            .record_assignment("member-1", &assignment);

        coordinator
            .target_assignment
            .write()
            .await
            .push(ConsumerGroupTopicPartitions {
                topic_id: [1; 16],
                partitions: vec![0, 1],
            });
        coordinator
            .topic_names_cache
            .write()
            .await
            .insert([1; 16], "topic-a".to_string());

        coordinator.reset_for_kip848_fencing().await;

        assert_eq!(coordinator.inner.read().await.member_id, "member-1");
        assert_eq!(*coordinator.member_epoch.read().await, 0);
        assert_eq!(coordinator.inner.read().await.generation_id, -1);
        assert_eq!(coordinator.inner.read().await.state, GroupState::Unjoined);
        assert!(coordinator.inner.read().await.assignment.is_empty());
        assert!(coordinator.target_assignment.read().await.is_empty());
        assert!(coordinator.topic_names_cache.read().await.is_empty());
        assert!(
            !coordinator
                .sticky_assignor
                .previous_assignments
                .read()
                .contains_key("member-1")
        );
        assert!(
            coordinator
                .sticky_assignor
                .get_partitions_to_revoke("member-1", &MemberAssignment::empty())
                .is_empty(),
            "fencing reset should clear preserved sticky assignments for the fenced member"
        );
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
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "host2".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
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
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "host2".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
        ];

        let assignments = assignor.assign(&topics, &partitions, &members);

        // m1 gets 0, 2; m2 gets 1, 3
        let m1_partitions = assignments.get("m1").unwrap().get("topic1").unwrap();
        let m2_partitions = assignments.get("m2").unwrap().get("topic1").unwrap();

        assert_eq!(m1_partitions.len(), 2);
        assert_eq!(m2_partitions.len(), 2);
    }

    #[tokio::test]
    async fn test_noop_rebalance_listener() {
        use crate::consumer::TopicPartition;

        let listener = NoOpRebalanceListener;

        // All methods should be no-ops (not panic)
        let partitions = vec![
            TopicPartition::new("topic1", 0),
            TopicPartition::new("topic2", 1),
        ];

        // These should all be no-ops and not panic
        ConsumerRebalanceListener::on_partitions_assigned(&listener, &partitions).await;
        ConsumerRebalanceListener::on_partitions_revoked(&listener, &partitions).await;
        ConsumerRebalanceListener::on_partitions_lost(&listener, &partitions).await;
    }

    #[test]
    fn test_rebalance_listener_trait_bounds() {
        // Ensure trait bounds are satisfied for async contexts
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<NoOpRebalanceListener>();
    }

    #[tokio::test]
    async fn test_custom_rebalance_listener() {
        use crate::consumer::TopicPartition;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        struct CountingListener {
            assigned_count: AtomicUsize,
            revoked_count: AtomicUsize,
            lost_count: AtomicUsize,
        }

        impl ConsumerRebalanceListener for CountingListener {
            async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) {
                self.assigned_count
                    .fetch_add(partitions.len(), Ordering::Relaxed);
            }

            async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) {
                self.revoked_count
                    .fetch_add(partitions.len(), Ordering::Relaxed);
            }

            async fn on_partitions_lost(&self, partitions: &[TopicPartition]) {
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

        ConsumerRebalanceListener::on_partitions_assigned(&*listener, &partitions).await;
        assert_eq!(listener.assigned_count.load(Ordering::Relaxed), 3);

        ConsumerRebalanceListener::on_partitions_revoked(&*listener, &partitions[..2]).await;
        assert_eq!(listener.revoked_count.load(Ordering::Relaxed), 2);

        ConsumerRebalanceListener::on_partitions_lost(&*listener, &partitions[..1]).await;
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

    #[test]
    fn test_heartbeat_controller_success() {
        let controller = HeartbeatController::new(Duration::from_secs(3), Duration::from_secs(30));

        // Initially, no heartbeat recorded
        assert!(controller.time_since_last_heartbeat().is_none());
        assert!(!controller.may_have_timed_out());

        // Record a heartbeat
        controller.heartbeat_success();

        // Now we should have a recent heartbeat
        let elapsed = controller.time_since_last_heartbeat().unwrap();
        assert!(elapsed < Duration::from_secs(1));
        assert!(!controller.may_have_timed_out());
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
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "member-2".to_string(),
                client_id: "client-2".to_string(),
                client_host: "host-2".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
        ];

        let assignments = assignor.assign(&topics, &partitions, &members);

        assert_eq!(assignments.len(), 2);
        let member1_parts: Vec<_> = assignments
            .get("member-1")
            .unwrap()
            .all_partitions()
            .collect();
        let member2_parts: Vec<_> = assignments
            .get("member-2")
            .unwrap()
            .all_partitions()
            .collect();

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
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "member-2".to_string(),
                client_id: "client-2".to_string(),
                client_host: "host-2".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
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
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "member-2".to_string(),
                client_id: "client-2".to_string(),
                client_host: "host-2".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
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
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "member-2".to_string(),
                client_id: "client-2".to_string(),
                client_host: "host-2".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "member-3".to_string(),
                client_id: "client-3".to_string(),
                client_host: "host-3".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
        ];

        let new_assignments = assignor.assign(&topics, &partitions, &members_new);

        // All 3 members should get 2 partitions each (6 / 3 = 2)
        for member_id in ["member-1", "member-2", "member-3"] {
            let part_count = new_assignments
                .get(member_id)
                .unwrap()
                .all_partitions()
                .count();
            assert_eq!(part_count, 2, "Member {member_id} should have 2 partitions");
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
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "h2".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "m3".to_string(),
                client_id: "c3".to_string(),
                client_host: "h3".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
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
            let count = assignments.get(member_id).unwrap().all_partitions().count();
            assert!(
                count <= 2,
                "Member {member_id} has {count} partitions, max should be 2"
            );
        }

        // Total partitions should still be 5
        let total: usize = ["m1", "m2", "m3"]
            .iter()
            .map(|m| assignments.get(*m).unwrap().all_partitions().count())
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
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "h2".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "m3".to_string(),
                client_id: "c3".to_string(),
                client_host: "h3".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
        ];

        let assignments = assignor.assign(&topics, &partitions, &members);

        // Each member should have exactly 2 partitions (6/3 = 2)
        for member_id in ["m1", "m2", "m3"] {
            let count = assignments.get(member_id).unwrap().all_partitions().count();
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
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "h2".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
        ];

        let round1 = assignor.assign(&topics, &partitions, &members);
        for (mid, assignment) in &round1 {
            assignor.record_assignment(mid, assignment);
        }

        // Each member gets 3 partitions
        assert_eq!(round1.get("m1").unwrap().all_partitions().count(), 3);
        assert_eq!(round1.get("m2").unwrap().all_partitions().count(), 3);

        // Round 2: 3 members (m3 joins)
        let members3 = vec![
            GroupMember {
                member_id: "m1".to_string(),
                client_id: "c1".to_string(),
                client_host: "h1".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "h2".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "m3".to_string(),
                client_id: "c3".to_string(),
                client_host: "h3".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
        ];

        let round2 = assignor.assign(&topics, &partitions, &members3);

        // Each member gets 2 partitions (6/3)
        for mid in ["m1", "m2", "m3"] {
            assert_eq!(
                round2.get(mid).unwrap().all_partitions().count(),
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
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "h2".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "m3".to_string(),
                client_id: "c3".to_string(),
                client_host: "h3".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
        ];

        let round1 = assignor.assign(&topics, &partitions, &members3);
        for (mid, a) in &round1 {
            assignor.record_assignment(mid, a);
        }
        // 2 each
        for mid in ["m1", "m2", "m3"] {
            assert_eq!(round1.get(mid).unwrap().all_partitions().count(), 2);
        }

        // m3 leaves
        assignor.clear_member("m3");

        let members2 = vec![
            GroupMember {
                member_id: "m1".to_string(),
                client_id: "c1".to_string(),
                client_host: "h1".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "h2".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
        ];

        let round2 = assignor.assign(&topics, &partitions, &members2);

        // Each remaining member gets 3 (6/2)
        assert_eq!(round2.get("m1").unwrap().all_partitions().count(), 3);
        assert_eq!(round2.get("m2").unwrap().all_partitions().count(), 3);

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
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
            GroupMember {
                member_id: "m2".to_string(),
                client_id: "c2".to_string(),
                client_host: "h2".to_string(),
                metadata: bytes::Bytes::new(),
                assignment: bytes::Bytes::new(),
            },
        ];

        let assignments = assignor.assign(&topics, &partitions, &members);

        // 6 total partitions / 2 members = 3 each
        let m1_total = assignments.get("m1").unwrap().all_partitions().count();
        let m2_total = assignments.get("m2").unwrap().all_partitions().count();
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
        buf.put_i16(i16::try_from(topic.len()).unwrap());
        buf.put_slice(topic);
        buf.put_i32(-1); // no user data

        // Owned partitions section
        buf.put_i32(1); // 1 owned topic
        let owned_topic = b"test";
        buf.put_i16(i16::try_from(owned_topic.len()).unwrap());
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

    // ── Eager sticky assignor ────────────────────────────────────────────

    fn gm(id: &str) -> GroupMember {
        GroupMember {
            member_id: id.to_string(),
            client_id: String::new(),
            client_host: String::new(),
            metadata: Bytes::new(),
            assignment: Bytes::new(),
        }
    }

    #[test]
    fn test_sticky_assignor_protocol_name() {
        // The name is the wire protocol identifier the coordinator matches
        // across members; it must be exactly Java's.
        assert_eq!(StickyAssignor::new().name(), "sticky");
    }

    #[test]
    fn test_sticky_assignor_distributes_all_partitions_evenly() {
        let assignor = StickyAssignor::new();
        let topics = vec!["t".to_string()];
        let mut partitions = HashMap::new();
        partitions.insert("t".to_string(), vec![0, 1, 2, 3, 4, 5]);
        let members = vec![gm("m1"), gm("m2"), gm("m3")];

        let result = assignor.assign(&topics, &partitions, &members);

        let mut all: Vec<PartitionId> = result
            .values()
            .flat_map(|a| a.all_partitions().map(|(_, p)| p))
            .collect();
        all.sort();
        assert_eq!(all, vec![0, 1, 2, 3, 4, 5], "every partition assigned once");

        for member in &members {
            let count = result[&member.member_id].all_partitions().count();
            assert_eq!(count, 2, "6 partitions across 3 members is 2 each");
        }
    }

    #[test]
    fn test_sticky_assignor_keeps_partitions_with_previous_owner() {
        let assignor = StickyAssignor::new();
        let topics = vec!["t".to_string()];
        let mut partitions = HashMap::new();
        partitions.insert("t".to_string(), vec![0, 1, 2, 3]);

        // First round with two members.
        let members = vec![gm("m1"), gm("m2")];
        let first = assignor.assign(&topics, &partitions, &members);
        for member in &members {
            assignor.record_assignment(&member.member_id, &first[&member.member_id]);
        }

        let m1_before: HashSet<PartitionId> =
            first["m1"].all_partitions().map(|(_, p)| p).collect();

        // Same membership rebalances again — stickiness means nothing moves.
        let second = assignor.assign(&topics, &partitions, &members);
        let m1_after: HashSet<PartitionId> =
            second["m1"].all_partitions().map(|(_, p)| p).collect();

        assert_eq!(
            m1_before, m1_after,
            "a stable group must not shuffle partitions between rebalances"
        );
    }

    #[test]
    fn test_sticky_assignor_handles_no_members() {
        let assignor = StickyAssignor::new();
        let topics = vec!["t".to_string()];
        let mut partitions = HashMap::new();
        partitions.insert("t".to_string(), vec![0, 1]);

        let result = assignor.assign(&topics, &partitions, &[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_sticky_assignor_clear_member_drops_stickiness() {
        let assignor = StickyAssignor::new();
        let topics = vec!["t".to_string()];
        let mut partitions = HashMap::new();
        partitions.insert("t".to_string(), vec![0, 1, 2, 3]);

        let members = vec![gm("m1"), gm("m2")];
        let first = assignor.assign(&topics, &partitions, &members);
        for member in &members {
            assignor.record_assignment(&member.member_id, &first[&member.member_id]);
        }

        // m2 leaves; its partitions must be redistributed to m1.
        assignor.clear_member("m2");
        let solo = vec![gm("m1")];
        let second = assignor.assign(&topics, &partitions, &solo);

        assert_eq!(
            second["m1"].all_partitions().count(),
            4,
            "the remaining member takes over the departed member's partitions"
        );
    }

    // ── Assignor protocol negotiation ────────────────────────────────────

    #[test]
    fn test_strategy_protocol_name_round_trip() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        for s in [S::Range, S::RoundRobin, S::Sticky, S::CooperativeSticky] {
            assert_eq!(S::from_protocol_name(s.protocol_name()), Some(s));
        }
        assert_eq!(S::from_protocol_name("no-such-assignor"), None);
    }

    #[test]
    fn test_only_cooperative_sticky_is_cooperative() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        // The eager sticky assignor must NOT report as cooperative: doing so
        // would run incremental revocation while the group expects every
        // member to give up its whole assignment.
        assert!(!S::Sticky.is_cooperative());
        assert!(!S::Range.is_cooperative());
        assert!(!S::RoundRobin.is_cooperative());
        assert!(S::CooperativeSticky.is_cooperative());
    }

    #[test]
    fn test_negotiated_strategy_defaults_to_first_preference() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        let c = test_coordinator(S::Range)
            .with_assignor_strategies(vec![S::CooperativeSticky, S::Range]);
        assert_eq!(c.negotiated_strategy(), S::CooperativeSticky);
    }

    #[test]
    fn test_latch_negotiated_strategy_adopts_coordinator_choice() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        // This member prefers cooperative-sticky, but an older member holds
        // the group on range. The coordinator's choice must win, otherwise
        // this member runs the wrong rebalance protocol.
        let c = test_coordinator(S::Range)
            .with_assignor_strategies(vec![S::CooperativeSticky, S::Range]);
        assert!(c.is_cooperative());

        c.latch_negotiated_strategy("range");

        assert_eq!(c.negotiated_strategy(), S::Range);
        assert!(
            !c.is_cooperative(),
            "must follow the group onto the eager protocol"
        );
    }

    #[test]
    fn test_latch_negotiated_strategy_ignores_unknown_name() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        let c = test_coordinator(S::Range).with_assignor_strategies(vec![S::Range]);
        c.latch_negotiated_strategy("something-we-never-advertised");
        assert_eq!(c.negotiated_strategy(), S::Range);
    }

    #[test]
    fn test_empty_strategy_list_is_ignored() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        // The coordinator must always have at least one protocol to offer.
        let c = test_coordinator(S::RoundRobin).with_assignor_strategies(vec![]);
        assert_eq!(c.negotiated_strategy(), S::RoundRobin);
    }

    // ── Poll interval tracking ───────────────────────────────────────────

    #[test]
    fn test_poll_tracker_not_expired_while_polling() {
        let t = PollTracker::new(Duration::from_secs(60));
        assert!(!t.is_expired());
        t.note_poll();
        assert!(!t.is_expired());
        assert!(!t.exceeded());
    }

    #[test]
    fn test_poll_tracker_expires_after_interval() {
        // A zero interval means any elapsed time counts as a stall, which
        // makes the transition observable without sleeping.
        let t = PollTracker::new(Duration::ZERO);
        std::thread::sleep(Duration::from_millis(5));
        assert!(t.is_expired());
    }

    #[test]
    fn test_poll_tracker_note_poll_clears_expiry() {
        let t = PollTracker::new(Duration::from_millis(30));
        std::thread::sleep(Duration::from_millis(60));
        assert!(t.is_expired());
        t.note_poll();
        assert!(!t.is_expired(), "a fresh poll re-arms the tracker");
    }

    #[test]
    fn test_poll_tracker_mark_exceeded_latches_once() {
        let t = PollTracker::new(Duration::ZERO);
        assert!(t.mark_exceeded(), "first call reports the transition");
        assert!(!t.mark_exceeded(), "subsequent calls do not");
        assert!(t.exceeded());
    }

    #[test]
    fn test_poll_tracker_reset_clears_latch() {
        let t = PollTracker::new(Duration::from_secs(60));
        t.mark_exceeded();
        assert!(t.exceeded());
        t.reset();
        assert!(
            !t.exceeded(),
            "reset gives a recovered application a way back"
        );
        assert!(!t.is_expired());
    }

    // ── Group metadata snapshot ──────────────────────────────────────────

    #[tokio::test]
    async fn test_group_metadata_none_before_join() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        let c = test_coordinator(S::Range);
        assert!(
            c.group_metadata().await.is_none(),
            "no member id yet means there is no identity to fence against"
        );
    }

    #[tokio::test]
    async fn test_group_metadata_reports_generation_and_member() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        let c = test_coordinator(S::Range);
        {
            let mut inner = c.inner.write().await;
            inner.member_id = "member-7".to_string();
            inner.generation_id = 12;
        }

        let m = c.group_metadata().await.expect("joined member");
        assert_eq!(m.group_id(), "test-group");
        assert_eq!(m.member_id(), "member-7");
        assert_eq!(m.generation_id(), 12);
        assert!(m.is_fenceable());
    }

    #[tokio::test]
    async fn test_group_metadata_uses_member_epoch_under_kip848() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        let c = test_coordinator(S::Range)
            .with_group_protocol(crate::consumer::config::GroupProtocol::Consumer);
        {
            let mut inner = c.inner.write().await;
            inner.member_id = "member-7".to_string();
            inner.generation_id = 12;
        }
        *c.member_epoch.write().await = 99;

        let m = c.group_metadata().await.expect("joined member");
        assert_eq!(
            m.generation_id(),
            99,
            "KIP-848 commits are validated against the member epoch, not the \
             classic generation"
        );
    }

    // ── Static membership leave (KIP-345) ────────────────────────────────

    #[tokio::test]
    async fn test_static_leave_preserves_member_id() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        let c = test_coordinator(S::Range).with_group_instance_id(Some("inst-1".to_string()));
        {
            let mut inner = c.inner.write().await;
            inner.member_id = "member-1".to_string();
            inner.generation_id = 5;
            inner.state = GroupState::Stable;
            inner.assignment.add("t", vec![0, 1]);
        }

        c.reset_for_static_leave().await;

        let inner = c.inner.read().await;
        assert_eq!(
            inner.member_id, "member-1",
            "clearing the member id makes the restarted process rejoin as a \
             second claimant of the instance id and get UNRELEASED_INSTANCE_ID"
        );
        assert_eq!(inner.state, GroupState::Unjoined);
        assert_eq!(inner.generation_id, -1);
        assert!(inner.assignment.is_empty());
    }

    #[tokio::test]
    async fn test_static_leave_clears_session_state() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        let c = test_coordinator(S::Range).with_group_instance_id(Some("inst-1".to_string()));
        {
            let mut inner = c.inner.write().await;
            inner.member_id = "member-1".to_string();
        }
        c.topic_names_cache
            .write()
            .await
            .insert([1u8; 16], "t".to_string());

        c.reset_for_static_leave().await;

        assert!(c.topic_names_cache.read().await.is_empty());
        assert!(c.target_assignment.read().await.is_empty());
        assert!(c.coordinator_conn.read().await.is_none());
    }

    #[tokio::test]
    async fn test_leave_group_is_noop_for_static_member() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        // A static member must not send LeaveGroup: doing so surrenders the
        // coordinator-held assignment and triggers the group-wide rebalance
        // that group.instance.id exists to prevent. There is no broker here,
        // so reaching the network path would fail; completing cleanly proves
        // the guard short-circuited first.
        let c = test_coordinator(S::Range).with_group_instance_id(Some("inst-1".to_string()));
        {
            let mut inner = c.inner.write().await;
            inner.member_id = "member-1".to_string();
            inner.state = GroupState::Stable;
        }

        c.leave_group().await.expect("static leave is local-only");

        assert_eq!(c.inner.read().await.member_id, "member-1");
        assert_eq!(c.inner.read().await.state, GroupState::Unjoined);
    }

    // ── KIP-848 owned vs target assignment ───────────────────────────────

    fn tp(partitions: Vec<PartitionId>) -> ConsumerGroupTopicPartitions {
        ConsumerGroupTopicPartitions {
            topic_id: [7u8; 16],
            partitions,
        }
    }

    #[tokio::test]
    async fn test_owned_assignment_starts_empty() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        let c = test_coordinator(S::Range);
        assert!(c.owned_assignment.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_owned_assignment_does_not_track_target_until_acknowledged() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        let c = test_coordinator(S::Range)
            .with_group_protocol(crate::consumer::config::GroupProtocol::Consumer);

        // Coordinator hands down a new, smaller target. Until the consumer
        // has actually stopped fetching the dropped partitions, reporting
        // this as "owned" would let another member start reading them
        // concurrently.
        *c.target_assignment.write().await = vec![tp(vec![0])];

        assert!(
            c.owned_assignment.read().await.is_empty(),
            "owned set must not follow the target automatically"
        );
    }

    #[tokio::test]
    async fn test_acknowledge_promotes_target_to_owned() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        let c = test_coordinator(S::Range)
            .with_group_protocol(crate::consumer::config::GroupProtocol::Consumer);

        *c.target_assignment.write().await = vec![tp(vec![0, 1])];
        // No heartbeat task is running, so this only performs the promotion.
        c.acknowledge_revocation().await;

        assert_eq!(*c.owned_assignment.read().await, vec![tp(vec![0, 1])]);
    }

    #[tokio::test]
    async fn test_reset_clears_owned_assignment() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        let c = test_coordinator(S::Range)
            .with_group_protocol(crate::consumer::config::GroupProtocol::Consumer);

        *c.target_assignment.write().await = vec![tp(vec![0])];
        c.acknowledge_revocation().await;
        assert!(!c.owned_assignment.read().await.is_empty());

        c.reset_for_kip848_fencing().await;
        assert!(
            c.owned_assignment.read().await.is_empty(),
            "a fenced member owns nothing"
        );
    }

    // ── Fatal error reporting ────────────────────────────────────────────

    #[test]
    fn test_fatal_error_is_absent_by_default() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        let c = test_coordinator(S::Range);
        assert!(c.take_fatal_error().is_none());
    }

    #[test]
    fn test_fatal_error_is_reported_once() {
        use crate::consumer::config::PartitionAssignmentStrategy as S;
        let c = test_coordinator(S::Range);
        *c.fatal_error.lock() = Some("boom".to_string());

        assert_eq!(c.take_fatal_error().as_deref(), Some("boom"));
        assert!(
            c.take_fatal_error().is_none(),
            "each fatal condition surfaces to the application exactly once"
        );
    }
}
