//! Share-group membership: coordinator discovery, the heartbeat loop,
//! rejoining after fencing, and installing assignments.
//!
//! Only the background loop heartbeats, at the interval the coordinator
//! returns. A member the coordinator fenced (`FENCED_MEMBER_EPOCH`) or forgot
//! (`UNKNOWN_MEMBER_ID`) drops every partition's state and rejoins at epoch 0
//! with its full subscription.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::Duration;

use tracing::{debug, info, warn};

use super::state::{Assigned, Backoff, Inner};
use crate::consumer::TopicPartition;
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::{
    ApiKey, FindCoordinatorRequest, FindCoordinatorResponse, ShareGroupHeartbeatRequest,
    ShareGroupHeartbeatResponse, ShareGroupTopicPartitions, VersionedDecode, VersionedEncode,
    versions,
};

/// Bounds on the coordinator-supplied heartbeat interval.
const HEARTBEAT_MIN: Duration = Duration::from_millis(50);
const HEARTBEAT_MAX: Duration = Duration::from_secs(30);

/// The member's view of its group.
#[derive(Debug)]
pub(crate) struct MemberState {
    /// Client-generated member id (KIP-932), kept across rejoins.
    pub member_id: String,
    pub member_epoch: i32,
    pub subscription: Vec<String>,
    /// The coordinator acknowledged `subscription`; later heartbeats omit it.
    pub subscription_acknowledged: bool,
    pub heartbeat_interval: Duration,
    /// The group coordinator's address. Its requests go over the pool's
    /// coordination connection to it.
    pub coordinator: Option<String>,
    /// The last assignment the coordinator sent, by topic id.
    pub target: Vec<ShareGroupTopicPartitions>,
}

impl MemberState {
    pub(crate) fn new() -> Self {
        Self {
            member_id: crate::util::random_uuid_v4(),
            member_epoch: 0,
            subscription: Vec::new(),
            subscription_acknowledged: false,
            heartbeat_interval: Duration::from_secs(5),
            coordinator: None,
            target: Vec::new(),
        }
    }
}

/// Whether a heartbeat error means the member must rejoin at epoch 0.
pub(crate) fn is_fenced(error: &KrafkaError) -> bool {
    matches!(
        error,
        KrafkaError::Broker {
            code: ErrorCode::FencedMemberEpoch | ErrorCode::UnknownMemberId,
            ..
        }
    )
}

/// Discover the coordinator and send the joining heartbeat, retrying
/// coordinator errors (routine while a coordinator loads) and a fence.
pub(crate) async fn join(inner: &Arc<Inner>) -> Result<()> {
    let backoff = crate::util::BackoffPolicy::default();
    let mut last_error = None;
    for attempt in 0..crate::consumer::COORDINATOR_REDISCOVERY_MAX_ATTEMPTS {
        if attempt > 0 {
            tokio::time::sleep(backoff.calculate_backoff(attempt)).await;
        }
        match heartbeat(inner).await {
            Ok(()) => return Ok(()),
            Err(error)
                if crate::consumer::is_coordinator_retriable(&error) || is_fenced(&error) =>
            {
                debug!(
                    group = %inner.config.group_id,
                    attempt,
                    "share group join hit {error}; retrying"
                );
                last_error = Some(error);
            }
            Err(error) => return Err(error),
        }
    }
    Err(last_error.unwrap_or_else(|| {
        KrafkaError::broker(
            ErrorCode::CoordinatorNotAvailable,
            "could not join the share group",
        )
    }))
}

/// Start the heartbeat loop unless it runs.
pub(crate) fn start(inner: &Arc<Inner>) {
    let mut task = inner.heartbeat_task.lock();
    if task.as_ref().is_none_or(|t| t.is_finished()) {
        // A weak reference: the loop must not keep a dropped consumer alive.
        *task = Some(tokio::spawn(run(Arc::downgrade(inner))));
    }
}

/// Stop the heartbeat loop.
pub(crate) fn stop(inner: &Inner) {
    if let Some(task) = inner.heartbeat_task.lock().take() {
        task.abort();
    }
}

async fn run(inner: Weak<Inner>) {
    let mut wait = match inner.upgrade() {
        Some(inner) => inner.member.lock().heartbeat_interval,
        None => return,
    };
    let mut failures: Option<Backoff> = None;
    loop {
        tokio::time::sleep(wait).await;
        let Some(inner) = inner.upgrade() else {
            return;
        };
        if inner.closed.load(Ordering::Acquire) {
            return;
        }
        let result = heartbeat(&inner).await;
        wait = inner.member.lock().heartbeat_interval;
        match result {
            Ok(()) => failures = None,
            Err(error) => {
                let first = failures.is_none();
                let backoff = Backoff::after(failures);
                failures = Some(backoff);
                let paced = backoff
                    .until
                    .saturating_duration_since(tokio::time::Instant::now());
                // A fenced member rejoins at once (paced if it keeps being
                // fenced); any other failure retries after a backoff, never
                // after the full interval.
                wait = if is_fenced(&error) {
                    if first { Duration::ZERO } else { paced }
                } else {
                    warn!(group = %inner.config.group_id, "share group heartbeat failed: {error}");
                    wait.min(paced)
                };
            }
        }
    }
}

/// Send one heartbeat and apply its answer.
pub(crate) async fn heartbeat(inner: &Arc<Inner>) -> Result<()> {
    let address = ensure_coordinator(inner).await?;
    let request = {
        let member = inner.member.lock();
        let rejoining = member.member_epoch == 0 || !member.subscription_acknowledged;
        ShareGroupHeartbeatRequest {
            group_id: inner.config.group_id.clone(),
            member_id: member.member_id.clone(),
            member_epoch: member.member_epoch,
            rack_id: inner.config.client_rack.clone(),
            subscribed_topic_names: rejoining.then(|| member.subscription.clone()),
        }
    };

    let conn = match inner.pool.get_coordinator_connection(&address).await {
        Ok(conn) => conn,
        Err(error) => {
            inner.member.lock().coordinator = None;
            return Err(error);
        }
    };
    let version = conn
        .negotiate_api_version(
            ApiKey::ShareGroupHeartbeat,
            versions::SHARE_GROUP_HEARTBEAT_MAX,
            versions::SHARE_GROUP_HEARTBEAT_MIN,
        )
        .ok_or_else(|| KrafkaError::share_groups_unsupported(ApiKey::ShareGroupHeartbeat))?;
    let buf = conn
        .send_request(ApiKey::ShareGroupHeartbeat, version, |buf| {
            request.encode_versioned(version, buf)
        })
        .await;
    let buf = match buf {
        Ok(buf) => buf,
        Err(error) => {
            inner.member.lock().coordinator = None;
            return Err(error);
        }
    };
    let response = ShareGroupHeartbeatResponse::decode_versioned(version, &mut buf.as_ref())?;

    if !response.error_code.is_ok() {
        let error = KrafkaError::broker(
            response.error_code,
            response
                .error_message
                .unwrap_or_else(|| "ShareGroupHeartbeat failed".to_string()),
        );
        if crate::consumer::is_coordinator_retriable(&error) {
            inner.member.lock().coordinator = None;
        } else if is_fenced(&error) {
            fence(inner, &error);
        }
        return Err(error);
    }

    {
        let mut member = inner.member.lock();
        if let Some(id) = response.member_id.filter(|id| !id.is_empty()) {
            member.member_id = id;
        }
        member.member_epoch = response.member_epoch;
        if request
            .subscribed_topic_names
            .as_ref()
            .is_some_and(|sent| *sent == member.subscription)
        {
            member.subscription_acknowledged = true;
        }
        let interval = Duration::from_millis(response.heartbeat_interval_ms.max(0) as u64);
        member.heartbeat_interval = interval.clamp(HEARTBEAT_MIN, HEARTBEAT_MAX);
        if let Some(target) = response.assignment {
            member.target = target;
        }
    }
    if resolve_assignment(inner) {
        refresh_subscription(inner).await;
        resolve_assignment(inner);
    }
    Ok(())
}

/// Install the coordinator's assignment for every topic id metadata can
/// name. Returns whether some topic ids are still unknown.
pub(crate) fn resolve_assignment(inner: &Arc<Inner>) -> bool {
    let target = inner.member.lock().target.clone();
    let mut assigned = Vec::new();
    let mut unresolved = Vec::new();
    for topic in target {
        match inner.metadata.topic_name_for_id(&topic.topic_id) {
            Some(name) => {
                for &partition in &topic.partitions {
                    assigned.push(Assigned {
                        partition: TopicPartition::new(&name, partition),
                        topic_id: topic.topic_id,
                    });
                }
            }
            None => unresolved.push((topic.topic_id, topic.partitions)),
        }
    }
    assigned.sort_by(|a, b| {
        (&a.partition.topic, a.partition.partition)
            .cmp(&(&b.partition.topic, b.partition.partition))
    });
    let pending = !unresolved.is_empty();
    if inner.install_assignment(assigned, unresolved) {
        debug!(group = %inner.config.group_id, "share group assignment changed");
        inner.records_ready.notify_waiters();
        inner.wake_nodes();
    }
    pending
}

/// Refresh metadata for the subscribed topics.
pub(crate) async fn refresh_subscription(inner: &Inner) {
    let topics = inner.member.lock().subscription.clone();
    if topics.is_empty() {
        return;
    }
    let refs: Vec<&str> = topics.iter().map(String::as_str).collect();
    if let Err(error) = inner.metadata.refresh_for_topics(Some(&refs)).await {
        debug!(%error, "share consumer metadata refresh failed");
    }
}

/// The member lost its membership: drop every partition's state and rejoin
/// at epoch 0.
fn fence(inner: &Arc<Inner>, error: &KrafkaError) {
    warn!(
        group = %inner.config.group_id,
        "share group member must rejoin: {error}"
    );
    {
        let mut member = inner.member.lock();
        member.member_epoch = 0;
        member.subscription_acknowledged = false;
        member.target.clear();
    }
    let resolved = inner.drop_partition_state(error);
    inner.install_assignment(Vec::new(), Vec::new());
    inner.report(&resolved);
    inner.records_ready.notify_waiters();
    inner.wake_nodes();
}

/// Leave the group (member epoch `-1`), if a coordinator is known.
pub(crate) async fn leave(inner: &Inner) -> Result<()> {
    let Some(address) = inner.member.lock().coordinator.clone() else {
        return Ok(());
    };
    let request = ShareGroupHeartbeatRequest {
        group_id: inner.config.group_id.clone(),
        member_id: inner.member.lock().member_id.clone(),
        member_epoch: -1,
        rack_id: None,
        subscribed_topic_names: None,
    };
    let conn = inner.pool.get_coordinator_connection(&address).await?;
    let version = conn
        .negotiate_api_version(
            ApiKey::ShareGroupHeartbeat,
            versions::SHARE_GROUP_HEARTBEAT_MAX,
            versions::SHARE_GROUP_HEARTBEAT_MIN,
        )
        .ok_or_else(|| KrafkaError::share_groups_unsupported(ApiKey::ShareGroupHeartbeat))?;
    let buf = conn
        .send_request(ApiKey::ShareGroupHeartbeat, version, |buf| {
            request.encode_versioned(version, buf)
        })
        .await?;
    let response = ShareGroupHeartbeatResponse::decode_versioned(version, &mut buf.as_ref())?;
    inner.member.lock().coordinator = None;
    if !response.error_code.is_ok() {
        return Err(KrafkaError::broker(
            response.error_code,
            response
                .error_message
                .unwrap_or_else(|| "leaving the share group failed".to_string()),
        ));
    }
    info!(group = %inner.config.group_id, "left the share group");
    Ok(())
}

/// The cached coordinator, or one found with `FindCoordinator`.
async fn ensure_coordinator(inner: &Inner) -> Result<String> {
    if let Some(coordinator) = inner.member.lock().coordinator.clone() {
        return Ok(coordinator);
    }
    let brokers = inner.metadata.brokers();
    if brokers.is_empty() {
        return Err(KrafkaError::unavailable("no brokers available"));
    }
    let group = &inner.config.group_id;
    let request = FindCoordinatorRequest::for_group(group);
    let mut last_error = None;
    for broker in &brokers {
        let conn = match inner
            .pool
            .get_connection_by_id(broker.id(), broker.address())
            .await
        {
            Ok(conn) => conn,
            Err(error) => {
                last_error = Some(error);
                continue;
            }
        };
        let Some(version) = conn.negotiate_api_version(
            ApiKey::FindCoordinator,
            versions::FIND_COORDINATOR_MAX,
            versions::FIND_COORDINATOR_MIN,
        ) else {
            continue;
        };
        let buf = match conn
            .send_request(ApiKey::FindCoordinator, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await
        {
            Ok(buf) => buf,
            Err(error) => {
                last_error = Some(error);
                continue;
            }
        };
        let response = FindCoordinatorResponse::decode_versioned(version, &mut buf.as_ref())?;
        if response.error_code.is_ok() {
            let coordinator = format!("{}:{}", response.host, response.port);
            inner.member.lock().coordinator = Some(coordinator.clone());
            debug!(group = %group, node = response.node_id, "found the share group coordinator");
            return Ok(coordinator);
        }
        last_error = Some(KrafkaError::broker(
            response.error_code,
            response
                .error_message
                .filter(|m| !m.is_empty())
                .unwrap_or_else(|| format!("no coordinator for share group '{group}'")),
        ));
    }
    Err(last_error.unwrap_or_else(|| {
        KrafkaError::broker(
            ErrorCode::CoordinatorNotAvailable,
            format!("no coordinator for share group '{group}'"),
        )
    }))
}
