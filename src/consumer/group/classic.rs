//! The classic group protocol: `JoinGroup`, `SyncGroup` (with the leader's
//! client-side assignment) and the `Heartbeat` task.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ahash::AHashMap as HashMap;
use bytes::{Bytes, BytesMut};
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::heartbeat::HeartbeatCommand;
use super::{
    COORDINATOR_REDISCOVERY_MAX_ATTEMPTS, GroupCoordinator, GroupState, MemberAssignment,
    is_coordinator_retriable,
};
use crate::PartitionId;
use crate::consumer::assignor::{self, MemberSubscription};
use crate::consumer::config::PartitionAssignmentStrategy;
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    ApiKey, CONSUMER_PROTOCOL_TYPE, ConsumerProtocolAssignment, ConsumerProtocolSubscription,
    ConsumerProtocolTopicPartitions, HeartbeatRequest, HeartbeatResponse, JoinGroupRequest,
    JoinGroupRequestProtocol, JoinGroupResponse, JoinGroupResponseMember, SyncGroupRequest,
    SyncGroupRequestAssignment, SyncGroupResponse, VersionedDecode, VersionedEncode,
    decode_consumer_protocol_assignment, decode_consumer_protocol_subscription,
    encode_consumer_protocol_assignment, encode_consumer_protocol_subscription,
    versions::{
        HEARTBEAT_MAX, HEARTBEAT_MIN, JOIN_GROUP_MAX, JOIN_GROUP_MIN, SYNC_GROUP_MAX,
        SYNC_GROUP_MIN,
    },
};

/// Slack added on top of the group's rebalance timeout when bounding a
/// `JoinGroup` client-side, so the coordinator's answer at the end of the
/// rebalance window reaches us instead of racing our own deadline.
const JOIN_GROUP_TIMEOUT_SLACK: Duration = Duration::from_secs(5);

/// Emit the KIP-1274 deprecation warning for the classic protocol, once per
/// process.
pub(crate) fn warn_classic_protocol_deprecated() {
    static WARNED: std::sync::Once = std::sync::Once::new();
    WARNED.call_once(|| {
        warn!(
            "Consumer group is using the CLASSIC rebalance protocol, which Apache Kafka 4.3 \
             deprecates (KIP-1274). Switch with \
             `kafka.consumer(group).group_protocol(GroupProtocol::Consumer)` — it needs \
             Kafka 4.0+ (or 3.7-3.9 with `group.coordinator.new.enable=true`), moves \
             assignment to the broker, and makes rebalances incremental instead of \
             stop-the-world. This warning is emitted once per process."
        );
    });
}

impl GroupCoordinator {
    /// Client-side budget for one `JoinGroup`: the coordinator answers only
    /// once the rebalance converged, so the group's rebalance timeout, not
    /// `request.timeout.ms`, bounds it.
    pub(crate) fn join_group_timeout(&self) -> Duration {
        self.rebalance_timeout
            .saturating_add(JOIN_GROUP_TIMEOUT_SLACK)
    }

    /// Adopt the protocol the coordinator selected for the group. It is the
    /// most-preferred one every member supports, which need not be this
    /// member's first choice.
    pub(super) fn latch_negotiated_strategy(&self, protocol_name: &str) {
        if protocol_name.is_empty() {
            return;
        }
        match PartitionAssignmentStrategy::from_protocol_name(protocol_name) {
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
            None => warn!(
                "Coordinator selected unknown assignment protocol '{}' for group '{}'",
                protocol_name, self.group_id
            ),
        }
    }

    /// Start a `JoinGroup`/`SyncGroup` round on its own task, unless one is
    /// already running.
    ///
    /// The round runs detached from the caller: dropping a `poll()` that
    /// waits for it neither cancels it halfway — which would leave the group
    /// state stuck in `Joining` — nor loses its outcome, which is parked for
    /// [`take_pending_rebalance`](Self::take_pending_rebalance).
    ///
    /// `restart_heartbeat` (re)starts the heartbeat task after a successful
    /// round; the heartbeat task itself passes `false` for the cooperative
    /// background round it starts, and keeps heartbeating.
    pub(crate) fn spawn_rejoin(self: &Arc<Self>, restart_heartbeat: bool) {
        if self.rejoin_in_flight.send_replace(true) {
            return;
        }
        let epoch = self.membership_epoch.load(Ordering::Acquire);
        let coordinator = Arc::clone(self);
        tokio::spawn(async move {
            let outcome = coordinator.join_and_sync().await;
            if coordinator.membership_epoch.load(Ordering::Acquire) == epoch {
                if outcome.is_ok() && restart_heartbeat {
                    coordinator.start_heartbeat_task();
                }
                // Park before clearing the flag, so a poll woken by the flag
                // finds the outcome waiting.
                *coordinator.pending_rebalance.lock() = Some(outcome);
            } else {
                debug!(
                    group = %coordinator.group_id,
                    "Discarding a join that finished after the member left"
                );
            }
            coordinator.rejoin_in_flight.send_replace(false);
        });
    }

    /// One `JoinGroup`/`SyncGroup` round trip, retrying coordinator errors
    /// after re-discovery (bounded).
    async fn join_and_sync(&self) -> Result<MemberAssignment> {
        let backoff = crate::util::BackoffPolicy::default();
        let mut last_error = None;

        for attempt in 0..COORDINATOR_REDISCOVERY_MAX_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(backoff.calculate_backoff(attempt)).await;
            }
            if !self.has_coordinator() {
                match self.find_coordinator().await {
                    Ok(()) => {}
                    Err(error) if is_coordinator_retriable(&error) => {
                        last_error = Some(error);
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }

            let result = match self.join_group().await {
                Ok(join_response) => self.sync_group(&join_response).await,
                Err(error) => Err(error),
            };
            match result {
                Ok(assignment) => return Ok(assignment),
                Err(error) if is_coordinator_retriable(&error) => {
                    debug!(
                        "Group '{}' join/sync hit {error}; re-discovering the coordinator \
                         (attempt {}/{})",
                        self.group_id,
                        attempt + 1,
                        COORDINATOR_REDISCOVERY_MAX_ATTEMPTS
                    );
                    last_error = Some(error);
                }
                Err(error) => return Err(error),
            }
        }

        Err(last_error.unwrap_or_else(|| {
            KrafkaError::broker(ErrorCode::NotCoordinator, "Failed to join group")
        }))
    }

    /// Join the consumer group.
    async fn join_group(&self) -> Result<JoinGroupResponse> {
        let conn = self.get_coordinator_connection().await?;

        let (member_id, generation_id) = {
            let inner = self.inner.read();
            (inner.member_id.clone(), inner.generation_id)
        };
        let topics = self.subscribed_topics();
        let owned = if self.is_cooperative() {
            self.owned.lock().clone()
        } else {
            HashMap::new()
        };

        let subscription = self.build_subscription(&topics, &owned, generation_id);
        let mut metadata = BytesMut::new();
        encode_consumer_protocol_subscription(&subscription, &mut metadata)?;
        let metadata = metadata.freeze();

        let request = JoinGroupRequest {
            group_id: self.group_id.clone(),
            session_timeout_ms: crate::util::duration_to_millis_i32(self.session_timeout),
            rebalance_timeout_ms: crate::util::duration_to_millis_i32(self.rebalance_timeout),
            member_id: member_id.clone(),
            group_instance_id: self.group_instance_id.clone(),
            protocol_type: CONSUMER_PROTOCOL_TYPE.to_string(),
            // Every configured strategy, most-preferred first: the coordinator
            // picks the first one every member supports, which is what lets a
            // group change protocol in a rolling bounce.
            protocols: self
                .assignment_strategies
                .iter()
                .map(|strategy| JoinGroupRequestProtocol {
                    name: strategy.protocol_name().to_string(),
                    metadata: metadata.clone(),
                })
                .collect(),
            reason: None,
        };

        debug!(
            "Joining group '{}' with member_id '{}'",
            self.group_id, member_id
        );
        self.inner.write().state = GroupState::Joining;

        // Static membership needs the GroupInstanceId field (v5+).
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
                        "broker does not support JoinGroup v{join_group_min}-v{JOIN_GROUP_MAX}"
                    ),
                )
            })?;
        let join_timeout = self.join_group_timeout();

        let response = conn
            .send_request_with_timeout(ApiKey::JoinGroup, jg_version, join_timeout, |buf| {
                request.encode_versioned(jg_version, buf)
            })
            .await;
        let response = match response {
            Ok(r) => r,
            Err(e) => {
                self.inner.write().state = GroupState::Unjoined;
                return Err(e);
            }
        };
        let mut buf = response;
        let mut join_response = JoinGroupResponse::decode_versioned(jg_version, &mut buf)?;

        // KIP-394 (v4+): the first join returns MEMBER_ID_REQUIRED with the
        // member id to use; retry once with it.
        if join_response.error_code == ErrorCode::MemberIdRequired {
            self.inner.write().member_id = join_response.member_id.clone();
            let retry_request = JoinGroupRequest {
                member_id: join_response.member_id.clone(),
                ..request.clone()
            };
            let retry_response = conn
                .send_request_with_timeout(ApiKey::JoinGroup, jg_version, join_timeout, |buf| {
                    retry_request.encode_versioned(jg_version, buf)
                })
                .await;
            let retry_response = match retry_response {
                Ok(r) => r,
                Err(e) => {
                    self.inner.write().state = GroupState::Unjoined;
                    return Err(e);
                }
            };
            let mut retry_buf = retry_response;
            join_response = JoinGroupResponse::decode_versioned(jg_version, &mut retry_buf)?;
        }

        if !join_response.error_code.is_ok() {
            // The coordinator no longer recognises this member id or
            // generation; register afresh next time.
            if matches!(
                join_response.error_code,
                ErrorCode::UnknownMemberId | ErrorCode::IllegalGeneration
            ) {
                self.reset_member_identity();
            }
            self.invalidate_coordinator_on_error(join_response.error_code);
            self.inner.write().state = GroupState::Unjoined;
            return Err(KrafkaError::broker(
                join_response.error_code,
                "Failed to join group",
            ));
        }

        {
            let mut inner = self.inner.write();
            inner.member_id = join_response.member_id.clone();
            inner.generation_id = join_response.generation_id;
            inner.state = GroupState::AwaitingSync;
        }
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

    /// Sync with the group after joining; the leader computes and sends every
    /// member's assignment.
    async fn sync_group(&self, join_response: &JoinGroupResponse) -> Result<MemberAssignment> {
        let conn = self.get_coordinator_connection().await?;
        let (member_id, generation_id) = {
            let inner = self.inner.read();
            (inner.member_id.clone(), inner.generation_id)
        };

        // KIP-814: a coordinator that still holds a valid assignment for a
        // rejoining static leader sets `skip_assignment`; the leader then
        // sends nothing and the persisted assignment stands.
        let assignments = if join_response.is_leader() && !join_response.skip_assignment {
            self.compute_assignments(&join_response.members).await?
        } else {
            Vec::new()
        };

        let request = SyncGroupRequest {
            group_id: self.group_id.clone(),
            generation_id,
            member_id,
            group_instance_id: self.group_instance_id.clone(),
            protocol_type: Some(CONSUMER_PROTOCOL_TYPE.to_string()),
            protocol_name: join_response.protocol_name.clone(),
            assignments,
        };

        let sg_version = conn
            .negotiate_api_version(ApiKey::SyncGroup, SYNC_GROUP_MAX, SYNC_GROUP_MIN)
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    format!(
                        "broker does not support SyncGroup v{SYNC_GROUP_MIN}-v{SYNC_GROUP_MAX}"
                    ),
                )
            })?;
        let response = conn
            .send_request(ApiKey::SyncGroup, sg_version, |buf| {
                request.encode_versioned(sg_version, buf)
            })
            .await;
        let response = match response {
            Ok(r) => r,
            Err(e) => {
                self.inner.write().state = GroupState::Unjoined;
                return Err(e);
            }
        };
        let mut buf = response;
        let sync_response = SyncGroupResponse::decode_versioned(sg_version, &mut buf)?;

        if !sync_response.error_code.is_ok() {
            // REBALANCE_IN_PROGRESS keeps the member id for a faster rejoin;
            // a forgotten member or generation registers afresh.
            if matches!(
                sync_response.error_code,
                ErrorCode::UnknownMemberId | ErrorCode::IllegalGeneration
            ) {
                self.reset_member_identity();
            }
            self.invalidate_coordinator_on_error(sync_response.error_code);
            self.inner.write().state = GroupState::Unjoined;
            return Err(KrafkaError::broker(
                sync_response.error_code,
                "Failed to sync group",
            ));
        }

        let assignment = decode_consumer_assignment(&sync_response.assignment)?;
        {
            let mut inner = self.inner.write();
            inner.assignment = assignment.clone();
            inner.state = GroupState::Stable;
        }
        debug!(
            "Synced group '{}': {} topic(s) assigned",
            self.group_id,
            assignment.partitions.len()
        );
        Ok(assignment)
    }

    /// Compute every member's assignment as the group leader.
    ///
    /// Every member's subscription is decoded; the assignment covers the union
    /// of the subscribed topics (metadata is refreshed for any the cache does
    /// not hold), and a member only receives partitions of topics it
    /// subscribed to.
    async fn compute_assignments(
        &self,
        members: &[JoinGroupResponseMember],
    ) -> Result<Vec<SyncGroupRequestAssignment>> {
        let mut subscriptions = Vec::with_capacity(members.len());
        for m in members {
            let subscription = decode_consumer_protocol_subscription(&m.metadata).map_err(|e| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::Malformed,
                    format!(
                        "member '{}' sent an undecodable subscription: {e}",
                        m.member_id
                    ),
                )
            })?;
            subscriptions.push(MemberSubscription::from_protocol(
                &m.member_id,
                subscription,
            ));
        }

        let topics = assignor::subscribed_topics(&subscriptions);
        let missing: Vec<&str> = topics
            .iter()
            .filter(|t| self.metadata.topic(t).is_none())
            .map(String::as_str)
            .collect();
        if !missing.is_empty()
            && let Err(e) = self.metadata.refresh_for_topics(Some(&missing)).await
        {
            warn!(
                "Metadata refresh for subscribed topics {:?} failed: {}; assigning what is known",
                missing, e
            );
        }
        let mut partitions: HashMap<String, Vec<PartitionId>> = HashMap::new();
        for topic in &topics {
            if let Some(info) = self.metadata.topic(topic) {
                partitions.insert(
                    topic.clone(),
                    info.partitions.values().map(|p| p.partition).collect(),
                );
            }
        }

        // The protocol the group agreed on, not this member's preference.
        let negotiated = self.negotiated_strategy();
        let mut assignments = assignor::assign(negotiated, &subscriptions, &partitions);
        if negotiated.is_cooperative() {
            assignor::withhold_transferring_partitions(&mut assignments, &subscriptions);
        }

        let mut result = Vec::with_capacity(members.len());
        for member in members {
            let member_assignment = assignments
                .get(&member.member_id)
                .cloned()
                .unwrap_or_default();
            result.push(SyncGroupRequestAssignment {
                member_id: member.member_id.clone(),
                assignment: encode_consumer_assignment(&member_assignment)?.freeze(),
            });
        }
        Ok(result)
    }

    /// This member's `ConsumerProtocolSubscription`, at the lowest version
    /// that carries everything it has to say: v0 topics only; v2 (KIP-429)
    /// owned partitions and generation for the cooperative protocol; v3
    /// (KIP-881) a rack. Topics and partitions are sorted so an unchanged
    /// subscription encodes to identical bytes.
    pub(crate) fn build_subscription(
        &self,
        topics: &[String],
        owned_partitions: &HashMap<String, Vec<PartitionId>>,
        generation_id: i32,
    ) -> ConsumerProtocolSubscription {
        let mut sorted_topics = topics.to_vec();
        sorted_topics.sort();
        let mut subscription = ConsumerProtocolSubscription::new(sorted_topics);

        if self.is_cooperative() {
            let mut owned: Vec<ConsumerProtocolTopicPartitions> = owned_partitions
                .iter()
                .map(|(topic, partitions)| {
                    let mut partitions = partitions.clone();
                    partitions.sort_unstable();
                    ConsumerProtocolTopicPartitions {
                        topic: topic.clone(),
                        partitions,
                    }
                })
                .collect();
            owned.sort_by(|a, b| a.topic.cmp(&b.topic));
            subscription = subscription
                .with_owned_partitions(owned)
                .with_generation_id(generation_id);
        }
        if let Some(rack) = &self.client_rack {
            subscription = subscription.with_rack_id(rack.clone());
        }
        subscription
    }

    /// Start the classic heartbeat task, replacing a running one.
    ///
    /// The task keeps the session alive and enforces `max_poll_interval`: once
    /// the application has not polled for longer, it leaves the group (no
    /// LeaveGroup for a static member) and stops, and the next poll reports
    /// the partitions lost and rejoins.
    ///
    /// On `REBALANCE_IN_PROGRESS` a cooperative member runs the join round in
    /// the background right away, since it keeps consuming the partitions the
    /// round lets it keep. An eager member only raises the flag: it must
    /// commit and revoke on its poll path before it may rejoin.
    pub(crate) fn start_heartbeat_task(self: &Arc<Self>) {
        self.stop_heartbeat_task();
        // Signals the previous task raised belong to the generation the join
        // that is starting this task has replaced.
        self.heartbeat.take_rebalance_needed();
        self.heartbeat.take_member_invalidated();

        let (cmd_tx, mut cmd_rx) = mpsc::channel::<HeartbeatCommand>(10);
        *self.heartbeat_cmd_tx.lock() = Some(cmd_tx);

        let coordinator = Arc::downgrade(self);
        let group_id = self.group_id.clone();
        let heartbeat_interval = self.heartbeat_interval;
        let heartbeat = self.heartbeat.clone();
        let poll_tracker = self.poll_tracker.clone();
        let epoch = self.heartbeat_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        heartbeat.start();

        tokio::spawn(async move {
            debug!("Starting heartbeat task for group '{}'", group_id);
            let mut interval = tokio::time::interval(heartbeat_interval);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if !heartbeat.is_running() {
                            break;
                        }
                        let Some(coordinator) = coordinator.upgrade() else { break };
                        if coordinator.heartbeat_epoch.load(Ordering::Acquire) != epoch {
                            break;
                        }

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
                            heartbeat.stop();
                            if let Err(e) = coordinator.leave_group().await {
                                debug!(
                                    "LeaveGroup after max_poll_interval expiry failed for \
                                     group '{}': {}; the session will lapse instead",
                                    group_id, e
                                );
                            }
                            break;
                        }

                        match coordinator.send_heartbeat().await {
                            Ok(ErrorCode::None) => {}
                            Ok(ErrorCode::RebalanceInProgress) => {
                                if coordinator.is_cooperative() {
                                    debug!(
                                        "Rebalance in progress for group '{}'; rejoining in \
                                         the background",
                                        group_id
                                    );
                                    coordinator.spawn_rejoin(false);
                                } else {
                                    heartbeat.signal_rebalance();
                                }
                            }
                            Ok(code @ (ErrorCode::UnknownMemberId | ErrorCode::IllegalGeneration)) => {
                                warn!(
                                    "Heartbeat for group '{}' answered {:?}: the member's \
                                     partitions are lost",
                                    group_id, code
                                );
                                heartbeat.signal_member_invalidated();
                                heartbeat.stop();
                                break;
                            }
                            Ok(
                                code @ (ErrorCode::NotCoordinator
                                | ErrorCode::CoordinatorNotAvailable
                                | ErrorCode::CoordinatorLoadInProgress),
                            ) => {
                                debug!(
                                    "Heartbeat for group '{}' answered {:?}; re-discovering \
                                     the coordinator",
                                    group_id, code
                                );
                                coordinator.drop_coordinator();
                            }
                            Ok(code) => {
                                error!("Fatal heartbeat error for group '{}': {:?}", group_id, code);
                                *coordinator.fatal_error.lock() = Some(KrafkaError::broker(
                                    code,
                                    format!("consumer group '{group_id}' heartbeat failed"),
                                ));
                                heartbeat.signal_member_invalidated();
                                heartbeat.stop();
                                break;
                            }
                            Err(e) => {
                                // The connection may be dead; the next tick
                                // re-discovers the coordinator. A member the
                                // coordinator evicted meanwhile learns it from
                                // the next answer.
                                debug!("Heartbeat failed for group '{}': {}", group_id, e);
                                coordinator.drop_coordinator();
                            }
                        }
                    }
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(HeartbeatCommand::AcknowledgeRevocation) => {}
                            Some(HeartbeatCommand::Stop) | None => {
                                debug!("Stopping heartbeat task for group '{}'", group_id);
                                break;
                            }
                        }
                    }
                }
            }
            debug!("Heartbeat task ended for group '{}'", group_id);
        });
    }

    /// Send one classic heartbeat and return the coordinator's answer.
    async fn send_heartbeat(&self) -> Result<ErrorCode> {
        let conn = self.get_coordinator_connection().await?;
        let (member_id, generation_id) = {
            let inner = self.inner.read();
            (inner.member_id.clone(), inner.generation_id)
        };
        let request = HeartbeatRequest {
            group_id: self.group_id.clone(),
            generation_id,
            member_id,
            group_instance_id: self.group_instance_id.clone(),
        };
        let hb_version = conn
            .negotiate_api_version(ApiKey::Heartbeat, HEARTBEAT_MAX, HEARTBEAT_MIN)
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    format!("broker does not support Heartbeat v{HEARTBEAT_MIN}-v{HEARTBEAT_MAX}"),
                )
            })?;
        let response = conn
            .send_request(ApiKey::Heartbeat, hb_version, |buf| {
                request.encode_versioned(hb_version, buf)
            })
            .await?;
        let mut buf = response;
        let hb_response = HeartbeatResponse::decode_versioned(hb_version, &mut buf)?;
        Ok(hb_response.error_code)
    }
}

/// Decode the `ConsumerProtocolAssignment` blob from a SyncGroup response.
fn decode_consumer_assignment(data: &Bytes) -> Result<MemberAssignment> {
    let mut assignment = MemberAssignment::empty();
    for tp in decode_consumer_protocol_assignment(data)?.assigned_partitions {
        assignment.add(tp.topic, tp.partitions);
    }
    Ok(assignment)
}

/// Encode one member's assignment at v0 (the field set has not changed
/// since), sorted so the same assignment always produces the same bytes.
fn encode_consumer_assignment(assignment: &MemberAssignment) -> Result<BytesMut> {
    let mut assigned: Vec<ConsumerProtocolTopicPartitions> = assignment
        .partitions
        .iter()
        .map(|(topic, partitions)| {
            let mut partitions = partitions.clone();
            partitions.sort_unstable();
            ConsumerProtocolTopicPartitions {
                topic: topic.clone(),
                partitions,
            }
        })
        .collect();
    assigned.sort_by(|a, b| a.topic.cmp(&b.topic));
    let mut buf = BytesMut::new();
    encode_consumer_protocol_assignment(&ConsumerProtocolAssignment::new(0, assigned), &mut buf)?;
    Ok(buf)
}
