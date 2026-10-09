//! The KIP-848 consumer group protocol: membership, assignment and liveness
//! all travel on `ConsumerGroupHeartbeat`.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use super::heartbeat::HeartbeatCommand;
use super::{
    COORDINATOR_REDISCOVERY_MAX_ATTEMPTS, GroupCoordinator, GroupState, MemberAssignment,
    is_coordinator_retriable,
};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::{
    ApiKey, ConsumerGroupHeartbeatRequest, ConsumerGroupHeartbeatResponse,
    ConsumerGroupTopicPartitions, VersionedDecode, VersionedEncode,
    versions::{CONSUMER_GROUP_HEARTBEAT_MAX, CONSUMER_GROUP_HEARTBEAT_MIN},
};

/// Smallest heartbeat interval the task uses, whatever the coordinator asks.
const MIN_INTERVAL_MS: i32 = 1000;

impl GroupCoordinator {
    /// Join (or rejoin) the group with a full heartbeat, then start the
    /// heartbeat task. Does nothing for a member that is already in the group.
    pub(crate) async fn join_consumer_group(self: &Arc<Self>) -> Result<()> {
        match self.state() {
            GroupState::Stable if self.heartbeat.is_running() => return Ok(()),
            GroupState::Leaving | GroupState::Dead => {
                return Err(KrafkaError::illegal_state(format!(
                    "Cannot send consumer heartbeat: group state is {:?}",
                    self.state()
                )));
            }
            _ => {}
        }

        let topics = self.subscribed_topics();
        // A joining member (epoch 0) sends an empty owned-partition list, not
        // a null one: brokers reject a null list at epoch 0.
        let owned = if self.member_epoch.load(Ordering::Acquire) == 0 {
            Some(Vec::new())
        } else {
            Some(self.owned_assignment.read().clone())
        };

        let backoff = crate::util::BackoffPolicy::default();
        let mut last_error = None;
        let mut response = None;
        for attempt in 0..COORDINATOR_REDISCOVERY_MAX_ATTEMPTS {
            if attempt > 0 {
                tokio::time::sleep(backoff.calculate_backoff(attempt)).await;
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
            }
            match self
                .consumer_group_heartbeat(Some(topics.clone()), owned.clone())
                .await
            {
                Ok(ok) => {
                    response = Some(ok);
                    break;
                }
                Err(error) if is_coordinator_retriable(&error) => {
                    debug!(
                        "KIP-848 join for group '{}' hit {error}; re-discovering the \
                         coordinator (attempt {}/{})",
                        self.group_id,
                        attempt + 1,
                        COORDINATOR_REDISCOVERY_MAX_ATTEMPTS
                    );
                    last_error = Some(error);
                }
                Err(error) => return Err(error),
            }
        }
        let Some(response) = response else {
            return Err(last_error.unwrap_or_else(|| {
                KrafkaError::broker(
                    ErrorCode::NotCoordinator,
                    "ConsumerGroupHeartbeat failed: coordinator unavailable",
                )
            }));
        };

        self.start_consumer_heartbeat_task(response.heartbeat_interval_ms);
        Ok(())
    }

    /// Take the flag a heartbeat raises when it delivered a new assignment.
    pub(crate) fn take_assignment_changed(&self) -> bool {
        self.assignment_changed.swap(false, Ordering::AcqRel)
    }

    /// Acknowledge the assignment the consumer has just applied: it becomes
    /// the owned set the heartbeat reports, which is what lets the
    /// coordinator advance the member epoch and hand released partitions on.
    ///
    /// Only call it once revocation callbacks ran and the consumer stopped
    /// fetching what it gave up.
    pub(crate) fn acknowledge_assignment(&self) {
        let target = self.target_assignment.read().clone();
        *self.owned_assignment.write() = target;
        self.send_heartbeat_command(HeartbeatCommand::AcknowledgeRevocation);
    }

    /// Send one `ConsumerGroupHeartbeat` (API key 68) and apply its answer.
    ///
    /// An error answer never changes the member epoch. `FENCED_MEMBER_EPOCH`,
    /// `UNKNOWN_MEMBER_ID` and `STALE_MEMBER_EPOCH` mean the coordinator no
    /// longer counts this member at the epoch it sent: its partitions are lost
    /// and it rejoins at epoch 0 with the same member id.
    async fn consumer_group_heartbeat(
        &self,
        subscribed_topic_names: Option<Vec<String>>,
        topic_partitions: Option<Vec<ConsumerGroupTopicPartitions>>,
    ) -> Result<ConsumerGroupHeartbeatResponse> {
        let conn = self.get_coordinator_connection().await?;

        // KIP-1082 (v1+): the member id is generated by the client, once.
        let member_id = {
            let mut inner = self.inner.write();
            if inner.member_id.is_empty() {
                inner.member_id = crate::util::random_uuid_v4();
            }
            inner.member_id.clone()
        };
        let member_epoch = self.member_epoch.load(Ordering::Acquire);
        // The assignor travels with the subscription: on a join and on every
        // full heartbeat.
        let server_assignor = subscribed_topic_names
            .as_ref()
            .and(self.server_assignor.clone());

        let request = ConsumerGroupHeartbeatRequest {
            group_id: self.group_id.clone(),
            member_id: member_id.clone(),
            member_epoch,
            instance_id: self.group_instance_id.clone(),
            rack_id: self.client_rack.clone(),
            rebalance_timeout_ms: crate::util::duration_to_millis_i32(self.rebalance_timeout),
            subscribed_topic_names,
            subscribed_topic_regex: None,
            server_assignor,
            topic_partitions,
        };

        let Some(hb_version) = conn.negotiate_api_version(
            ApiKey::ConsumerGroupHeartbeat,
            CONSUMER_GROUP_HEARTBEAT_MAX,
            CONSUMER_GROUP_HEARTBEAT_MIN,
        ) else {
            return Err(KrafkaError::kip848_unsupported());
        };

        let response = conn
            .send_request(ApiKey::ConsumerGroupHeartbeat, hb_version, |buf| {
                request.encode_versioned(hb_version, buf)
            })
            .await?;
        let mut buf = response;
        let response = ConsumerGroupHeartbeatResponse::decode_versioned(hb_version, &mut buf)?;

        if !response.error_code.is_ok() {
            match response.error_code {
                // Another live process holds this group.instance.id: a
                // deployment mistake retrying cannot fix.
                ErrorCode::UnreleasedInstanceId => {
                    error!(
                        "group.instance.id {:?} is already in use by another live member of \
                         group '{}'. Two processes cannot share one instance id.",
                        self.group_instance_id, self.group_id
                    );
                    self.inner.write().state = GroupState::Dead;
                }
                ErrorCode::UnknownMemberId
                | ErrorCode::FencedMemberEpoch
                | ErrorCode::StaleMemberEpoch => {
                    warn!(
                        "ConsumerGroupHeartbeat for group '{}' answered {:?}; the member's \
                         partitions are lost",
                        self.group_id, response.error_code
                    );
                    self.reset_for_kip848_fencing();
                    self.membership_lost.store(true, Ordering::Release);
                }
                code => {
                    self.invalidate_coordinator_on_error(code);
                }
            }
            let message = response.error_message.as_deref().unwrap_or("unknown error");
            return Err(KrafkaError::broker(
                response.error_code,
                if response.error_code == ErrorCode::UnsupportedAssignor {
                    format!(
                        "the coordinator of group '{}' does not support group_remote_assignor \
                         {:?}: {message}",
                        self.group_id,
                        self.server_assignor.as_deref().unwrap_or_default()
                    )
                } else {
                    format!("ConsumerGroupHeartbeat failed: {message}")
                },
            ));
        }

        self.apply_heartbeat_response(&response).await?;
        debug!(
            "ConsumerGroupHeartbeat OK for group '{}': member_id='{}', epoch={}",
            self.group_id, member_id, response.member_epoch
        );
        Ok(response)
    }

    /// Apply a successful heartbeat answer: member id, epoch, and a new
    /// target assignment if the coordinator sent one.
    async fn apply_heartbeat_response(
        &self,
        response: &ConsumerGroupHeartbeatResponse,
    ) -> Result<()> {
        if let Some(ref new_member_id) = response.member_id {
            let mut inner = self.inner.write();
            if inner.member_id != *new_member_id {
                inner.member_id = new_member_id.clone();
            }
        }
        self.member_epoch
            .store(response.member_epoch, Ordering::Release);

        let Some(ref assignment) = response.assignment else {
            // A null assignment means "unchanged"; an accepted epoch confirms
            // membership. Topic ids that did not resolve earlier may resolve
            // now.
            if !self.target_assignment.read().is_empty() {
                self.resolve_target_assignment();
            } else if response.member_epoch > 0 {
                let mut inner = self.inner.write();
                if inner.state != GroupState::Stable {
                    inner.state = GroupState::Stable;
                    self.assignment_changed.store(true, Ordering::Release);
                }
            }
            return Ok(());
        };

        *self.target_assignment.write() = assignment.topic_partitions.clone();
        let mut unresolved = self.resolve_target_assignment();
        if unresolved {
            if let Err(e) = self.metadata.refresh().await {
                warn!(
                    "Metadata refresh for topic ids of group '{}' failed: {}",
                    self.group_id, e
                );
            }
            unresolved = self.resolve_target_assignment();
        }
        if unresolved {
            warn!(
                "KIP-848 assignment for group '{}' names topic ids that metadata does not \
                 resolve; they are assigned once it does",
                self.group_id
            );
        }
        Ok(())
    }

    /// Resolve the stored target assignment's topic ids to names and install
    /// it. Returns whether some ids are still unknown.
    ///
    /// Resolution tries the metadata cache, then the local id → name cache
    /// that survives a metadata flush.
    fn resolve_target_assignment(&self) -> bool {
        let target = self.target_assignment.read().clone();
        let mut assignment = MemberAssignment::empty();
        let mut unresolved = false;
        {
            let mut cache = self.topic_names_cache.write();
            for tp in &target {
                if let Some(name) = self.metadata.topic_name_for_id(&tp.topic_id) {
                    cache.insert(tp.topic_id, name.clone());
                    assignment.add(name, tp.partitions.clone());
                } else if let Some(name) = cache.get(&tp.topic_id) {
                    assignment.add(name.clone(), tp.partitions.clone());
                } else {
                    unresolved = true;
                }
            }
        }
        let mut inner = self.inner.write();
        if inner.assignment != assignment || inner.state != GroupState::Stable {
            inner.assignment = assignment;
            inner.state = GroupState::Stable;
            self.assignment_changed.store(true, Ordering::Release);
        }
        unresolved
    }

    /// Start the KIP-848 heartbeat task, replacing a running one.
    ///
    /// Besides heartbeating at the interval the coordinator asks for, the task
    /// enforces `max_poll_interval`: once the application has not polled for
    /// longer, it leaves the group (member epoch -1, or -2 for a static
    /// member) and stops, so the coordinator reassigns the partitions. The
    /// next poll reports them lost and rejoins at epoch 0.
    fn start_consumer_heartbeat_task(self: &Arc<Self>, interval_ms: i32) {
        self.stop_heartbeat_task();
        self.heartbeat.take_rebalance_needed();
        self.heartbeat.take_member_invalidated();

        let (cmd_tx, mut cmd_rx) = mpsc::channel::<HeartbeatCommand>(10);
        *self.heartbeat_cmd_tx.lock() = Some(cmd_tx);

        let coordinator = Arc::downgrade(self);
        let group_id = self.group_id.clone();
        let heartbeat = self.heartbeat.clone();
        let poll_tracker = self.poll_tracker.clone();
        let epoch = self.heartbeat_epoch.fetch_add(1, Ordering::AcqRel) + 1;
        heartbeat.start();

        tokio::spawn(async move {
            let mut current_ms = interval_ms.max(MIN_INTERVAL_MS);
            let mut tick = tokio::time::interval(Duration::from_millis(current_ms as u64));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // Every field is sent on the first heartbeat and after an error
            // (KIP-848).
            let mut send_full = true;
            let mut unknown_error_backoff = Duration::ZERO;
            debug!("Starting KIP-848 heartbeat task for group '{}'", group_id);

            loop {
                tokio::select! {
                    _ = tick.tick() => {
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
                                debug!("Leave after max_poll_interval expiry failed for '{}': {}", group_id, e);
                            }
                            break;
                        }

                        let (topics, owned) = if send_full {
                            let owned = coordinator.owned_assignment.read().clone();
                            (Some(coordinator.subscribed_topics()), Some(owned))
                        } else {
                            (None, None)
                        };
                        match coordinator.consumer_group_heartbeat(topics, owned).await {
                            Ok(response) => {
                                unknown_error_backoff = Duration::ZERO;
                                send_full = false;
                                let new_ms = response.heartbeat_interval_ms.max(MIN_INTERVAL_MS);
                                if new_ms != current_ms {
                                    current_ms = new_ms;
                                    tick = tokio::time::interval(Duration::from_millis(new_ms as u64));
                                    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                                    tick.tick().await;
                                }
                            }
                            Err(error @ KrafkaError::Broker { code, .. }) => match code {
                                ErrorCode::UnknownMemberId
                                | ErrorCode::FencedMemberEpoch
                                | ErrorCode::StaleMemberEpoch => {
                                    // `consumer_group_heartbeat` reset the
                                    // membership; the poll path reports the
                                    // loss and rejoins.
                                    heartbeat.signal_member_invalidated();
                                    heartbeat.stop();
                                    break;
                                }
                                ErrorCode::UnreleasedInstanceId => {
                                    *coordinator.fatal_error.lock() = Some(KrafkaError::fenced(format!(
                                        "group.instance.id {:?} is already in use by another \
                                         member of group '{}'",
                                        coordinator.group_instance_id, group_id
                                    )));
                                    heartbeat.signal_member_invalidated();
                                    heartbeat.stop();
                                    break;
                                }
                                ErrorCode::NotCoordinator
                                | ErrorCode::CoordinatorNotAvailable
                                | ErrorCode::CoordinatorLoadInProgress
                                | ErrorCode::RebalanceInProgress => {
                                    send_full = true;
                                }
                                ErrorCode::UnsupportedAssignor => {
                                    error!("KIP-848 heartbeat for '{}': {}", group_id, error);
                                    *coordinator.fatal_error.lock() = Some(error);
                                    heartbeat.signal_member_invalidated();
                                    heartbeat.stop();
                                    break;
                                }
                                ErrorCode::GroupAuthorizationFailed
                                | ErrorCode::InvalidRequest
                                | ErrorCode::InvalidGroupId
                                | ErrorCode::GroupMaxSizeReached
                                | ErrorCode::UnsupportedVersion
                                | ErrorCode::InvalidRegularExpression => {
                                    error!(
                                        "KIP-848 non-retriable heartbeat error for '{}': {:?}",
                                        group_id, code
                                    );
                                    *coordinator.fatal_error.lock() = Some(KrafkaError::broker(
                                        code,
                                        format!(
                                            "consumer group '{group_id}' heartbeat failed with \
                                             a non-retriable error"
                                        ),
                                    ));
                                    heartbeat.signal_member_invalidated();
                                    heartbeat.stop();
                                    break;
                                }
                                code => {
                                    // Back off an unrecognised error so it
                                    // cannot turn into a hot loop.
                                    send_full = true;
                                    unknown_error_backoff = (unknown_error_backoff * 2)
                                        .clamp(Duration::from_millis(100), Duration::from_secs(30));
                                    warn!(
                                        "KIP-848 heartbeat error for '{}': {:?}; retrying in {:?}",
                                        group_id, code, unknown_error_backoff
                                    );
                                    tokio::time::sleep(unknown_error_backoff).await;
                                }
                            },
                            Err(e) => {
                                // A dead connection: the next tick re-discovers
                                // the coordinator.
                                debug!("KIP-848 heartbeat for '{}' failed: {}", group_id, e);
                                coordinator.drop_coordinator();
                                send_full = true;
                            }
                        }
                    }
                    cmd = cmd_rx.recv() => {
                        match cmd {
                            Some(HeartbeatCommand::AcknowledgeRevocation) => {
                                // A fresh interval fires at once, which is the
                                // point: the coordinator waits for this ack.
                                send_full = true;
                                tick = tokio::time::interval(Duration::from_millis(current_ms as u64));
                                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                            }
                            Some(HeartbeatCommand::Stop) | None => break,
                        }
                    }
                }
            }
            debug!("KIP-848 heartbeat task ended for group '{}'", group_id);
        });
    }

    /// Leave with a heartbeat at `leave_epoch`: -1 leaves, -2 is a static
    /// member's temporary leave, which keeps its assignment for the session
    /// timeout (KIP-848).
    pub(super) async fn leave_group_consumer(&self, leave_epoch: i32) -> Result<()> {
        let conn = match self.get_coordinator_connection().await {
            Ok(c) => c,
            Err(_) => {
                self.reset();
                return Ok(());
            }
        };
        let member_id = {
            let mut inner = self.inner.write();
            inner.state = GroupState::Leaving;
            inner.member_id.clone()
        };
        self.member_epoch.store(leave_epoch, Ordering::Release);

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
            self.reset();
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
            Ok(Ok(bytes)) => {
                let mut buf = bytes;
                match ConsumerGroupHeartbeatResponse::decode_versioned(hb_version, &mut buf) {
                    Ok(resp) if resp.error_code.is_ok() => {
                        info!("Left group '{}' via KIP-848", self.group_id);
                    }
                    Ok(resp) => warn!(
                        "KIP-848 leave error for '{}': {:?}",
                        self.group_id, resp.error_code
                    ),
                    Err(e) => warn!(
                        "Failed to decode KIP-848 leave response for '{}': {}",
                        self.group_id, e
                    ),
                }
            }
            Ok(Err(e)) => warn!(
                "Failed to send KIP-848 leave for '{}': {}",
                self.group_id, e
            ),
            Err(_) => warn!("KIP-848 leave request timed out for '{}'", self.group_id),
        }

        self.reset();
        Ok(())
    }
}
