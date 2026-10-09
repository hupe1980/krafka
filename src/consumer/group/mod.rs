//! Consumer group membership: the coordinator connection, the classic and
//! KIP-848 protocols, offset commit and fetch.
//!
//! Every lock in here is a `parking_lot` lock that is never held across an
//! `.await`: network calls run outside the lock and their result is applied in
//! one short critical section.

mod classic;
mod heartbeat;
mod kip848;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::time::Duration;

use ahash::AHashMap as HashMap;
use parking_lot::{Mutex, RwLock};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

pub(crate) use classic::warn_classic_protocol_deprecated;
use heartbeat::{HeartbeatCommand, HeartbeatController, PollTracker};

use crate::PartitionId;
use crate::client::GroupMembershipOperation;
use crate::consumer::config::{GroupProtocol, PartitionAssignmentStrategy};
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::metadata::ClusterMetadata;
use crate::network::{BrokerConnection, ConnectionPool};
use crate::protocol::{
    ApiKey, ConsumerGroupTopicPartitions, FindCoordinatorRequest, FindCoordinatorResponse,
    LeaveGroupMember, LeaveGroupRequest, LeaveGroupResponse, OffsetCommitRequest,
    OffsetCommitRequestPartition, OffsetCommitRequestTopic, OffsetCommitResponse,
    OffsetFetchRequest, OffsetFetchRequestTopic, OffsetFetchResponse, VersionedDecode,
    VersionedEncode,
    versions::{
        FIND_COORDINATOR_MAX, FIND_COORDINATOR_MIN, LEAVE_GROUP_MAX, LEAVE_GROUP_MIN,
        OFFSET_COMMIT_MAX, OFFSET_COMMIT_MIN, OFFSET_FETCH_MAX, OFFSET_FETCH_MIN,
    },
};

/// How many times `OffsetFetch` is re-issued while the coordinator reports that
/// a partition's committed offset is staged inside an unresolved transaction
/// (`UNSTABLE_OFFSET_COMMIT`, KIP-447).
///
/// Sized to absorb a normal transaction round trip, not a transaction that is
/// genuinely stuck. Beyond it the error is surfaced and the poll loop's
/// position initialisation, which backs off, becomes the outer loop.
const UNSTABLE_OFFSET_MAX_ATTEMPTS: u32 = 5;

/// How many times a join re-discovers the coordinator and retries.
///
/// `NOT_COORDINATOR`, `COORDINATOR_NOT_AVAILABLE` and
/// `COORDINATOR_LOAD_IN_PROGRESS` are all retriable after re-discovery: the
/// group moved, or its coordinator is still loading `__consumer_offsets`.
pub(crate) const COORDINATOR_REDISCOVERY_MAX_ATTEMPTS: u32 = 5;

/// Whether an error means "ask FindCoordinator again and retry".
pub(crate) fn is_coordinator_retriable(error: &KrafkaError) -> bool {
    matches!(
        error,
        KrafkaError::Broker {
            code: ErrorCode::NotCoordinator
                | ErrorCode::CoordinatorNotAvailable
                | ErrorCode::CoordinatorLoadInProgress,
            ..
        }
    )
}

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
/// An unnoticed `UNSTABLE_OFFSET_COMMIT` would leave the partition out of the
/// result map, and every caller reads a missing entry as "never committed" —
/// which means `auto.offset.reset`.
fn first_unstable_offset(response: &OffsetFetchResponse) -> Option<(&str, PartitionId)> {
    response.topics.iter().find_map(|topic| {
        topic
            .partitions
            .iter()
            .find(|p| p.error_code == ErrorCode::UnstableOffsetCommit)
            .map(|p| (topic.name.as_str(), p.partition_index))
    })
}

/// Consumer group state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum GroupState {
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

/// A member's assignment: partitions per topic.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct MemberAssignment {
    pub(crate) partitions: HashMap<String, Vec<PartitionId>>,
}

impl MemberAssignment {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn add(&mut self, topic: impl Into<String>, partitions: Vec<PartitionId>) {
        self.partitions.insert(topic.into(), partitions);
    }

    #[cfg(test)]
    pub(crate) fn get(&self, topic: &str) -> Option<&[PartitionId]> {
        self.partitions.get(topic).map(|v| v.as_slice())
    }

    pub(crate) fn all_partitions(&self) -> impl Iterator<Item = (&str, PartitionId)> + '_ {
        self.partitions
            .iter()
            .flat_map(|(topic, partitions)| partitions.iter().map(move |&p| (topic.as_str(), p)))
    }
}

/// Member identity and assignment, updated together so a reader never sees a
/// generation from one join with a member id from another.
#[derive(Debug)]
struct GroupInner {
    /// Member ID assigned by the coordinator. Empty before the first join.
    member_id: String,
    /// Generation ID (-1 before the first join).
    generation_id: i32,
    state: GroupState,
    /// The assignment the coordinator last handed this member.
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

/// Group membership for one [`Consumer`](super::Consumer).
///
/// Drives the classic (`JoinGroup`/`SyncGroup`/`Heartbeat`) or the KIP-848
/// (`ConsumerGroupHeartbeat`) protocol, commits and fetches offsets. The
/// consumer applies what this produces — assignments, losses — on its poll
/// path; this type never touches the consumer's partition state.
pub(crate) struct GroupCoordinator {
    group_id: String,
    pool: Arc<ConnectionPool>,
    metadata: Arc<ClusterMetadata>,
    session_timeout: Duration,
    heartbeat_interval: Duration,
    /// `max_poll_interval`: the rebalance timeout sent to the coordinator.
    rebalance_timeout: Duration,
    /// The group coordinator's address. Its connection is the pool's
    /// coordination connection, fetched per request so the pool's
    /// replacement rules (dead socket, KIP-368 re-authentication) apply.
    coordinator_addr: RwLock<Option<String>>,
    coordinator_id: Mutex<Option<i32>>,
    inner: Arc<RwLock<GroupInner>>,
    heartbeat: Arc<HeartbeatController>,
    /// Time since the application last polled; read by whichever heartbeat
    /// task runs.
    poll_tracker: Arc<PollTracker>,
    /// A non-retriable error the background heartbeat task observed, for the
    /// next `poll()` to return.
    fatal_error: Arc<Mutex<Option<KrafkaError>>>,
    /// Set when the coordinator fenced this member or no longer knows it on a
    /// path the poll does not drive (an inline KIP-848 heartbeat): its
    /// partitions are lost.
    membership_lost: Arc<AtomicBool>,
    heartbeat_cmd_tx: Mutex<Option<mpsc::Sender<HeartbeatCommand>>>,
    /// The outcome of a `JoinGroup`/`SyncGroup` round that ran on its own
    /// task, waiting for `poll()` to apply it.
    pending_rebalance: Arc<Mutex<Option<Result<MemberAssignment>>>>,
    /// `true` while a join round runs. A `watch` so `poll()` can wait for it.
    rejoin_in_flight: tokio::sync::watch::Sender<bool>,
    /// Incremented every time a heartbeat task is started, so a task that is
    /// still shutting down can tell it has been superseded.
    heartbeat_epoch: Arc<AtomicU64>,
    /// Incremented whenever membership is torn down (leave, reset). A join
    /// round started before that discards its result.
    membership_epoch: AtomicU64,
    subscribed_topics: RwLock<Vec<String>>,
    /// Advertised in JoinGroup in preference order.
    assignment_strategies: Vec<PartitionAssignmentStrategy>,
    /// The strategy the coordinator selected, latched from
    /// `JoinGroupResponse.protocol_name`. Until the first join, the first
    /// configured strategy.
    negotiated_strategy: RwLock<PartitionAssignmentStrategy>,
    group_instance_id: Option<String>,
    client_rack: Option<String>,
    isolation_level: i8,
    group_protocol: GroupProtocol,
    /// KIP-848 server-side assignor, sent with every full heartbeat.
    server_assignor: Option<String>,
    /// Partitions the consumer currently owns, reported in the JoinGroup
    /// subscription of a cooperative member.
    owned: Mutex<HashMap<String, Vec<PartitionId>>>,
    /// KIP-848 member epoch: 0 to join, -1 to leave, -2 for a static member's
    /// temporary leave.
    member_epoch: Arc<AtomicI32>,
    /// Raw KIP-848 target assignment (topic ids), kept so ids that could not
    /// be resolved yet are re-resolved after a metadata refresh.
    target_assignment: Arc<RwLock<Vec<ConsumerGroupTopicPartitions>>>,
    /// Partitions this KIP-848 member reports as owned. Advances only once the
    /// consumer has applied the assignment, so the coordinator never hands a
    /// partition on while this member still fetches it.
    owned_assignment: Arc<RwLock<Vec<ConsumerGroupTopicPartitions>>>,
    /// Topic id → name, a fallback when the metadata cache is flushed.
    topic_names_cache: Arc<RwLock<HashMap<[u8; 16], String>>>,
    /// A KIP-848 heartbeat delivered a new assignment for `poll()` to apply.
    assignment_changed: Arc<AtomicBool>,
}

impl GroupCoordinator {
    /// Create a coordinator for `group_id`.
    pub(crate) fn new(
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
            coordinator_addr: RwLock::new(None),
            coordinator_id: Mutex::new(None),
            inner: Arc::new(RwLock::new(GroupInner::initial())),
            heartbeat: Arc::new(HeartbeatController::default()),
            poll_tracker: Arc::new(PollTracker::new(rebalance_timeout)),
            fatal_error: Arc::new(Mutex::new(None)),
            membership_lost: Arc::new(AtomicBool::new(false)),
            heartbeat_cmd_tx: Mutex::new(None),
            pending_rebalance: Arc::new(Mutex::new(None)),
            rejoin_in_flight: tokio::sync::watch::Sender::new(false),
            heartbeat_epoch: Arc::new(AtomicU64::new(0)),
            membership_epoch: AtomicU64::new(0),
            subscribed_topics: RwLock::new(Vec::new()),
            assignment_strategies: vec![PartitionAssignmentStrategy::Range],
            negotiated_strategy: RwLock::new(PartitionAssignmentStrategy::Range),
            group_instance_id: None,
            client_rack: None,
            isolation_level: 0,
            group_protocol: GroupProtocol::Classic,
            server_assignor: None,
            owned: Mutex::new(HashMap::new()),
            member_epoch: Arc::new(AtomicI32::new(0)),
            target_assignment: Arc::new(RwLock::new(Vec::new())),
            owned_assignment: Arc::new(RwLock::new(Vec::new())),
            topic_names_cache: Arc::new(RwLock::new(HashMap::new())),
            assignment_changed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Set the partition assignment strategies in preference order. An empty
    /// list is ignored so the coordinator always has a protocol to offer.
    pub(crate) fn with_assignor_strategies(
        mut self,
        strategies: Vec<PartitionAssignmentStrategy>,
    ) -> Self {
        if let Some(&first) = strategies.first() {
            self.negotiated_strategy = RwLock::new(first);
            self.assignment_strategies = strategies;
        }
        self
    }

    /// Set the static group membership instance id (KIP-345).
    pub(crate) fn with_group_instance_id(mut self, id: Option<String>) -> Self {
        self.group_instance_id = id;
        self
    }

    /// Set the KIP-848 server-side assignor (`group.remote.assignor`).
    pub(crate) fn with_server_assignor(mut self, assignor: Option<String>) -> Self {
        self.server_assignor = assignor;
        self
    }

    /// Set the client rack id (KIP-392 / KIP-848 / KIP-881).
    pub(crate) fn with_client_rack(mut self, rack: Option<String>) -> Self {
        self.client_rack = rack;
        self
    }

    /// Set the transaction isolation level (0 = read_uncommitted).
    pub(crate) fn with_isolation_level(mut self, level: i8) -> Self {
        self.isolation_level = level;
        self
    }

    /// Set the group protocol. Selecting the classic protocol emits the
    /// KIP-1274 deprecation warning, once per process.
    pub(crate) fn with_group_protocol(mut self, protocol: GroupProtocol) -> Self {
        if protocol == GroupProtocol::Classic {
            warn_classic_protocol_deprecated();
        }
        self.group_protocol = protocol;
        self
    }

    /// The assignment strategy the group has settled on.
    pub(crate) fn negotiated_strategy(&self) -> PartitionAssignmentStrategy {
        *self.negotiated_strategy.read()
    }

    /// Whether the classic group runs the cooperative protocol. Always
    /// `false` under KIP-848.
    pub(crate) fn is_cooperative(&self) -> bool {
        !self.is_consumer_protocol() && self.negotiated_strategy().is_cooperative()
    }

    /// Whether the consumer uses the KIP-848 consumer group protocol.
    pub(crate) fn is_consumer_protocol(&self) -> bool {
        self.group_protocol == GroupProtocol::Consumer
    }

    pub(crate) fn group_id(&self) -> &str {
        &self.group_id
    }

    pub(crate) fn state(&self) -> GroupState {
        self.inner.read().state
    }

    pub(crate) fn member_id(&self) -> String {
        self.inner.read().member_id.clone()
    }

    /// The assignment the coordinator last handed this member.
    pub(crate) fn assignment(&self) -> MemberAssignment {
        self.inner.read().assignment.clone()
    }

    pub(crate) fn subscribed_topics(&self) -> Vec<String> {
        self.subscribed_topics.read().clone()
    }

    // ── liveness ────────────────────────────────────────────────────────

    /// Record that the application polled.
    pub(crate) fn note_poll(&self) {
        self.poll_tracker.note_poll();
    }

    /// Whether the heartbeat task saw the application exceed
    /// `max_poll_interval` and left the group.
    pub(crate) fn poll_interval_exceeded(&self) -> bool {
        self.poll_tracker.exceeded()
    }

    pub(crate) fn max_poll_interval(&self) -> Duration {
        self.poll_tracker.max_poll_interval()
    }

    /// Clear an expiry after the partitions were reported lost, so the member
    /// rejoins on this poll.
    pub(crate) fn rejoin_after_expiry(&self) {
        self.poll_tracker.reset();
        self.member_epoch.store(0, Ordering::Release);
        self.inner.write().state = GroupState::Unjoined;
    }

    /// Take a non-retriable error recorded by the heartbeat task, once.
    pub(crate) fn take_fatal_error(&self) -> Option<KrafkaError> {
        self.fatal_error.lock().take()
    }

    /// Whether this member's partitions were lost since the last call: the
    /// coordinator fenced it, or no longer knows it. Resets the member's
    /// identity as its protocol requires, so the next poll rejoins.
    pub(crate) fn take_lost(&self) -> bool {
        let invalidated = self.heartbeat.take_member_invalidated();
        if invalidated {
            if self.is_consumer_protocol() {
                // KIP-848: a fenced member rejoins with the same member id at
                // epoch 0.
                self.reset_for_kip848_fencing();
            } else {
                self.reset_member_identity();
                self.inner.write().state = GroupState::Unjoined;
            }
        }
        let lost = self.membership_lost.swap(false, Ordering::AcqRel);
        invalidated || lost
    }

    // ── subscription and rejoin ─────────────────────────────────────────

    /// Record the subscribed topics. A changed subscription of a member that
    /// is in the group needs a rejoin so the coordinator learns it.
    pub(crate) fn set_subscription(&self, topics: Vec<String>) {
        let mut sorted = topics;
        sorted.sort();
        let changed = {
            let mut current = self.subscribed_topics.write();
            let changed = *current != sorted;
            *current = sorted;
            changed
        };
        if changed && self.state() == GroupState::Stable {
            if self.is_consumer_protocol() {
                // The next poll sends a full heartbeat with the new topics.
                self.stop_heartbeat_task();
                self.inner.write().state = GroupState::PreparingRebalance;
            } else {
                self.request_rejoin();
            }
        }
    }

    /// Ask for a rejoin on the next poll, keeping the heartbeat task running.
    pub(crate) fn request_rejoin(&self) {
        self.inner.write().state = GroupState::PreparingRebalance;
    }

    /// Whether this member has to (re)join the group.
    pub(crate) fn needs_rejoin(&self) -> bool {
        if self.heartbeat.take_rebalance_needed() && !self.is_consumer_protocol() {
            self.inner.write().state = GroupState::PreparingRebalance;
        }
        matches!(
            self.inner.read().state,
            GroupState::Unjoined | GroupState::PreparingRebalance
        )
    }

    /// Record the partitions the consumer owns, for the next JoinGroup.
    pub(crate) fn set_owned(&self, assignment: &HashMap<String, Vec<PartitionId>>) {
        *self.owned.lock() = assignment.clone();
    }

    /// Take the outcome of a finished join round, if any.
    pub(crate) fn take_pending_rebalance(&self) -> Option<Result<MemberAssignment>> {
        self.pending_rebalance.lock().take()
    }

    fn clear_pending_rebalance(&self) {
        *self.pending_rebalance.lock() = None;
    }

    /// Whether a join round is running.
    pub(crate) fn rejoin_in_flight(&self) -> bool {
        *self.rejoin_in_flight.borrow()
    }

    /// Wait until the running join round finishes, or `budget` elapses.
    ///
    /// Subscribing before the check makes this free of a lost wake-up: a
    /// round that finishes in between marks the receiver changed.
    pub(crate) async fn await_rejoin(&self, budget: Duration) {
        let mut rx = self.rejoin_in_flight.subscribe();
        if !*rx.borrow_and_update() {
            return;
        }
        let _ = tokio::time::timeout(budget, rx.changed()).await;
    }

    /// A snapshot of this member's identity, for fencing transactional offset
    /// commits. `None` before the member has joined.
    pub(crate) fn group_metadata(&self) -> Option<crate::consumer::ConsumerGroupMetadata> {
        let (member_id, generation_id) = {
            let inner = self.inner.read();
            (inner.member_id.clone(), inner.generation_id)
        };
        if member_id.is_empty() {
            return None;
        }
        let generation = if self.is_consumer_protocol() {
            self.member_epoch.load(Ordering::Acquire)
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

    // ── coordinator connection ──────────────────────────────────────────

    /// Find the group coordinator broker.
    pub(crate) async fn find_coordinator(&self) -> Result<()> {
        debug!("Finding coordinator for group '{}'", self.group_id);

        let conn = self.get_any_connection().await?;
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
                        "broker does not support FindCoordinator v{FIND_COORDINATOR_MIN}-v{FIND_COORDINATOR_MAX}"
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

        let coordinator_addr = format!("{}:{}", find_response.host, find_response.port);
        let old_coordinator_id = self.coordinator_id.lock().replace(find_response.node_id);
        *self.coordinator_addr.write() = Some(coordinator_addr.clone());

        // A new coordinator does not know the previous generation or member
        // epoch; rejoin rather than commit or heartbeat with a stale one.
        if let Some(old_id) = old_coordinator_id
            && old_id != find_response.node_id
        {
            info!(
                "Group coordinator for '{}' changed from node {} to node {}; rejoining",
                self.group_id, old_id, find_response.node_id
            );
            self.reset_member_identity();
            self.inner.write().state = GroupState::PreparingRebalance;
        }

        info!(
            "Found coordinator for group '{}': node {} at {}",
            self.group_id, find_response.node_id, coordinator_addr
        );
        Ok(())
    }

    /// Drop the cached coordinator when the broker says it is not (or not yet)
    /// the coordinator, so the next request re-runs FindCoordinator. The old
    /// broker usually stays reachable after a coordinator move, so a liveness
    /// check alone would keep sending to it.
    fn invalidate_coordinator_on_error(&self, error_code: ErrorCode) -> bool {
        let is_coordinator_error = matches!(
            error_code,
            ErrorCode::NotCoordinator
                | ErrorCode::CoordinatorNotAvailable
                | ErrorCode::CoordinatorLoadInProgress
        );
        if is_coordinator_error {
            debug!(
                "Coordinator for group '{}' returned {:?}; re-discovering",
                self.group_id, error_code
            );
            self.drop_coordinator();
        }
        is_coordinator_error
    }

    /// Forget the coordinator so the next request re-discovers it. The node
    /// id is kept: a rediscovery that lands on another node rejoins, since
    /// the new coordinator does not know this generation.
    fn drop_coordinator(&self) {
        *self.coordinator_addr.write() = None;
    }

    fn has_coordinator(&self) -> bool {
        self.coordinator_addr.read().is_some()
    }

    /// The pool's coordination connection to the group coordinator,
    /// discovering the coordinator first when none is known. A connection
    /// that cannot be had drops the coordinator, so the next request
    /// re-discovers it.
    async fn get_coordinator_connection(&self) -> Result<Arc<BrokerConnection>> {
        let known = self.coordinator_addr.read().clone();
        let address = match known {
            Some(address) => address,
            None => {
                self.find_coordinator().await?;
                self.coordinator_addr.read().clone().ok_or_else(|| {
                    KrafkaError::broker(ErrorCode::CoordinatorNotAvailable, "coordinator not found")
                })?
            }
        };
        self.pool
            .get_coordinator_connection(&address)
            .await
            .inspect_err(|_| self.drop_coordinator())
    }

    async fn get_any_connection(&self) -> Result<Arc<BrokerConnection>> {
        for broker in self.metadata.brokers() {
            if let Ok(conn) = self.pool.get_connection(broker.address()).await {
                return Ok(conn);
            }
        }
        for server in &self.metadata.bootstrap_servers() {
            if let Ok(conn) = self.pool.get_connection(server).await {
                return Ok(conn);
            }
        }
        Err(KrafkaError::unavailable("no available brokers"))
    }

    // ── heartbeat task control ──────────────────────────────────────────

    /// Ask the running heartbeat task to stop.
    pub(crate) fn stop_heartbeat_task(&self) {
        if let Some(tx) = self.heartbeat_cmd_tx.lock().take() {
            let _ = tx.try_send(HeartbeatCommand::Stop);
        }
        self.heartbeat.stop();
    }

    fn send_heartbeat_command(&self, command: HeartbeatCommand) {
        if let Some(tx) = self.heartbeat_cmd_tx.lock().as_ref() {
            let _ = tx.try_send(command);
        }
    }

    // ── offsets ─────────────────────────────────────────────────────────

    /// Commit offsets to the coordinator.
    pub(crate) async fn commit_offsets(
        &self,
        offsets: &HashMap<(String, PartitionId), crate::consumer::CommitPosition>,
    ) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }

        // A commit is valid whenever this member holds a generation the
        // coordinator still accepts, not only while the group is stable: the
        // commit just before a revocation is made while a rebalance is
        // already in progress. The coordinator fences a stale generation.
        let (state, generation_id, member_id) = {
            let inner = self.inner.read();
            (inner.state, inner.generation_id, inner.member_id.clone())
        };
        if state == GroupState::Dead {
            return Err(KrafkaError::illegal_state(
                "cannot commit offsets: group is dead",
            ));
        }
        let has_generation = if self.is_consumer_protocol() {
            self.member_epoch.load(Ordering::Acquire) >= 0
        } else {
            generation_id >= 0
        };
        if !has_generation {
            return Err(KrafkaError::illegal_state(format!(
                "cannot commit offsets: no valid generation (group state is {state:?})",
            )));
        }

        let conn = self.get_coordinator_connection().await?;
        let oc_version = conn
            .negotiate_api_version(ApiKey::OffsetCommit, OFFSET_COMMIT_MAX, OFFSET_COMMIT_MIN)
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    format!(
                        "broker does not support OffsetCommit v{OFFSET_COMMIT_MIN}-v{OFFSET_COMMIT_MAX}"
                    ),
                )
            })?;

        // From v9 the generation field carries the KIP-848 member epoch.
        let generation_id = if self.is_consumer_protocol() && oc_version >= 9 {
            self.member_epoch.load(Ordering::Acquire)
        } else {
            generation_id
        };

        // Ordered, so a commit's request and the error it reports first do
        // not depend on hash order.
        let mut topics_map: std::collections::BTreeMap<String, Vec<OffsetCommitRequestPartition>> =
            std::collections::BTreeMap::new();
        for ((topic, partition), position) in offsets {
            topics_map
                .entry(topic.clone())
                .or_default()
                .push(OffsetCommitRequestPartition {
                    partition_index: *partition,
                    committed_offset: position.offset,
                    // Persisted from v6 and read back by OffsetFetch, so the
                    // next owner can validate the position against the
                    // leader's log (KIP-320).
                    committed_leader_epoch: position.leader_epoch,
                    commit_timestamp: -1,
                    committed_metadata: position.metadata.clone(),
                });
        }
        let mut topics: Vec<OffsetCommitRequestTopic> = topics_map
            .into_iter()
            .map(|(name, mut partitions)| {
                partitions.sort_by_key(|p| p.partition_index);
                (name, partitions)
            })
            .map(|(name, partitions)| OffsetCommitRequestTopic {
                name,
                topic_id: None,
                partitions,
            })
            .collect();

        // v10+ sends topic ids; fall back to v9 if one is not cached.
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

        for topic in &commit_response.topics {
            for partition in &topic.partitions {
                if partition.error_code.is_ok() {
                    continue;
                }
                // Under KIP-848 a stale epoch is transient: the heartbeat
                // task moves the epoch forward and the commit is retried.
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
                if matches!(
                    partition.error_code,
                    ErrorCode::RebalanceInProgress
                        | ErrorCode::IllegalGeneration
                        | ErrorCode::UnknownMemberId
                        | ErrorCode::FencedMemberEpoch
                        | ErrorCode::StaleMemberEpoch
                ) {
                    if !self.is_consumer_protocol() {
                        self.request_rejoin();
                    }
                    return Err(KrafkaError::broker(
                        partition.error_code,
                        format!(
                            "Offset commit failed for {}-{}: rebalance needed",
                            topic.name, partition.partition_index
                        ),
                    ));
                }
                if matches!(
                    partition.error_code,
                    ErrorCode::NotCoordinator | ErrorCode::CoordinatorNotAvailable
                ) {
                    self.drop_coordinator();
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

        debug!(
            "Committed {} offsets for group '{}'",
            offsets.len(),
            self.group_id
        );
        Ok(())
    }

    /// Fetch the group's committed offsets, absorbing an in-flight
    /// transactional commit.
    ///
    /// A `read_committed` consumer asks for stable offsets (KIP-447), and the
    /// coordinator answers `UNSTABLE_OFFSET_COMMIT` while a transaction that
    /// staged an offset is unresolved. That is a wait, retried here with a
    /// jittered backoff and a bounded budget.
    pub(crate) async fn fetch_committed_offsets(
        &self,
        partitions: &HashMap<String, Vec<PartitionId>>,
    ) -> Result<HashMap<(String, PartitionId), crate::consumer::CommittedPosition>> {
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
        partitions: &HashMap<String, Vec<PartitionId>>,
    ) -> Result<HashMap<(String, PartitionId), crate::consumer::CommittedPosition>> {
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

        let of_version = conn
            .negotiate_api_version(ApiKey::OffsetFetch, OFFSET_FETCH_MAX, OFFSET_FETCH_MIN)
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    format!(
                        "broker does not support OffsetFetch v{OFFSET_FETCH_MIN}-v{OFFSET_FETCH_MAX}"
                    ),
                )
            })?;

        // v10+ sends topic ids; fall back to v9 if one is not cached.
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

        // From v9, a KIP-848 member sends its id and epoch so the broker can
        // validate them.
        let (member_id, member_epoch) = if self.is_consumer_protocol() && of_version >= 9 {
            (
                Some(self.member_id()),
                self.member_epoch.load(Ordering::Acquire),
            )
        } else {
            (None, -1)
        };

        let request = OffsetFetchRequest {
            group_id: self.group_id.clone(),
            topics: Some(topics),
            require_stable: require_stable_for(self.isolation_level),
            member_id,
            member_epoch,
        };

        let response = conn
            .send_request(ApiKey::OffsetFetch, of_version, |buf| {
                request.encode_versioned(of_version, buf)
            })
            .await?;
        let mut buf = response;
        let mut offset_response = OffsetFetchResponse::decode_versioned(of_version, &mut buf)?;

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

        if !offset_response.error_code.is_ok() {
            if matches!(
                offset_response.error_code,
                ErrorCode::NotCoordinator | ErrorCode::CoordinatorNotAvailable
            ) {
                self.drop_coordinator();
            } else if matches!(
                offset_response.error_code,
                ErrorCode::StaleMemberEpoch
                    | ErrorCode::UnknownMemberId
                    | ErrorCode::FencedMemberEpoch
            ) && !self.is_consumer_protocol()
            {
                self.request_rejoin();
            }
            return Err(KrafkaError::broker(
                offset_response.error_code,
                format!("OffsetFetch failed for group '{}'", self.group_id),
            ));
        }

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
                            leader_epoch: partition.committed_leader_epoch,
                        },
                    );
                }
            }
        }
        Ok(result)
    }

    // ── leaving ─────────────────────────────────────────────────────────

    /// Leave the group the default way: see [`leave_group_with`](Self::leave_group_with).
    pub(crate) async fn leave_group(&self) -> Result<()> {
        self.leave_group_with(GroupMembershipOperation::Default)
            .await
    }

    /// Stop being a member, telling the coordinator as `operation` says
    /// (KIP-1092).
    ///
    /// By default a static member (KIP-345) of the classic protocol sends no
    /// LeaveGroup: the coordinator keeps its assignment for the session
    /// timeout so a restart reclaims it. Under KIP-848 a static member leaves
    /// with epoch -2, which keeps the reservation in the same way.
    /// `LeaveGroup` makes every member leave (classic `LeaveGroup`, KIP-848
    /// epoch -1); `RemainInGroup` sends nothing.
    pub(crate) async fn leave_group_with(&self, operation: GroupMembershipOperation) -> Result<()> {
        self.membership_epoch.fetch_add(1, Ordering::AcqRel);
        self.clear_pending_rebalance();

        let state = self.state();
        if state == GroupState::Unjoined || state == GroupState::Dead {
            self.stop_heartbeat_task();
            return Ok(());
        }

        let is_static = self.group_instance_id.is_some();
        if operation == GroupMembershipOperation::RemainInGroup {
            debug!(
                "Remaining in group '{}' on close; the session will lapse",
                self.group_id
            );
            self.stop_heartbeat_task();
            self.reset_for_static_leave();
            return Ok(());
        }

        if self.is_consumer_protocol() {
            self.stop_heartbeat_task();
            let leave_epoch = if is_static && operation == GroupMembershipOperation::Default {
                -2
            } else {
                -1
            };
            return self.leave_group_consumer(leave_epoch).await;
        }

        if is_static && operation == GroupMembershipOperation::Default {
            debug!(
                "Skipping LeaveGroup for static member of group '{}' (instance id {:?})",
                self.group_id, self.group_instance_id
            );
            self.stop_heartbeat_task();
            self.reset_for_static_leave();
            return Ok(());
        }

        let conn = match self.get_coordinator_connection().await {
            Ok(c) => c,
            Err(_) => {
                self.stop_heartbeat_task();
                self.reset();
                return Ok(());
            }
        };

        let member_id = {
            let mut inner = self.inner.write();
            inner.state = GroupState::Leaving;
            inner.member_id.clone()
        };

        // v3+ uses only the `members` array.
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

        if let Some(lg_version) =
            conn.negotiate_api_version(ApiKey::LeaveGroup, LEAVE_GROUP_MAX, LEAVE_GROUP_MIN)
        {
            let result = tokio::time::timeout(
                Duration::from_secs(5),
                conn.send_request(ApiKey::LeaveGroup, lg_version, |buf| {
                    request.encode_versioned(lg_version, buf)
                }),
            )
            .await;
            match result {
                Ok(Ok(response_bytes)) => {
                    let mut buf = response_bytes;
                    match LeaveGroupResponse::decode_versioned(lg_version, &mut buf) {
                        Ok(r) if r.error_code.is_ok() => {
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
                        Ok(r) => warn!(
                            "LeaveGroup error for '{}': {:?}",
                            self.group_id, r.error_code
                        ),
                        Err(e) => warn!(
                            "Failed to decode LeaveGroup response for '{}': {}",
                            self.group_id, e
                        ),
                    }
                }
                Ok(Err(e)) => warn!(
                    "Failed to send LeaveGroup request for '{}': {}",
                    self.group_id, e
                ),
                Err(_) => warn!("LeaveGroup request timed out for '{}'", self.group_id),
            }
        } else {
            warn!(
                "Broker does not support LeaveGroup v{LEAVE_GROUP_MIN}-v{LEAVE_GROUP_MAX}; \
                 the session will lapse instead"
            );
        }

        self.stop_heartbeat_task();
        self.reset();
        Ok(())
    }

    /// Forget the membership: identity, assignment, coordinator.
    fn reset(&self) {
        self.clear_pending_rebalance();
        self.reset_member_identity();
        {
            let mut inner = self.inner.write();
            inner.state = GroupState::Unjoined;
            inner.assignment = MemberAssignment::empty();
        }
        self.owned.lock().clear();
        self.target_assignment.write().clear();
        self.owned_assignment.write().clear();
        self.topic_names_cache.write().clear();
        self.drop_coordinator();
        *self.coordinator_id.lock() = None;
    }

    /// Reset after a static member stopped without leaving. Keeps the member
    /// id: the coordinator matches a returning static member by instance id
    /// and member id, and an empty member id against a live registration is
    /// rejected with `UNRELEASED_INSTANCE_ID`.
    fn reset_for_static_leave(&self) {
        self.clear_pending_rebalance();
        {
            let mut inner = self.inner.write();
            inner.generation_id = -1;
            inner.state = GroupState::Unjoined;
            inner.assignment = MemberAssignment::empty();
        }
        self.owned.lock().clear();
        self.target_assignment.write().clear();
        self.owned_assignment.write().clear();
        self.topic_names_cache.write().clear();
        self.drop_coordinator();
        *self.coordinator_id.lock() = None;
    }

    /// Reset after KIP-848 fencing. Keeps the member id — a fenced member
    /// "rejoins with the same member id and epoch 0" — and drops the
    /// assignment the coordinator has taken back.
    fn reset_for_kip848_fencing(&self) {
        self.clear_pending_rebalance();
        self.member_epoch.store(0, Ordering::Release);
        {
            let mut inner = self.inner.write();
            inner.generation_id = -1;
            inner.state = GroupState::Unjoined;
            inner.assignment = MemberAssignment::empty();
        }
        self.target_assignment.write().clear();
        self.owned_assignment.write().clear();
        self.topic_names_cache.write().clear();
    }

    /// Clear member id and generation so the next join registers afresh.
    fn reset_member_identity(&self) {
        {
            let mut inner = self.inner.write();
            inner.member_id.clear();
            inner.generation_id = -1;
        }
        self.member_epoch.store(0, Ordering::Release);
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
mod tests;
