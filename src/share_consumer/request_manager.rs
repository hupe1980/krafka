//! One request manager per broker node.
//!
//! The manager is the only sender of `ShareFetch` and `ShareAcknowledge` to
//! its node, with at most one request in flight, and it owns the node's share
//! session. Callers never send: `poll()`, `ack()`, `commit()` and `close()`
//! change the shared state and wake the manager, which decides what to send
//! next. Its rules:
//!
//! - A request at epoch 0 is a `ShareFetch` without acknowledgements. Pending
//!   acknowledgements wait for the session it opens.
//! - A network error, a timeout or a top-level error resets the session.
//! - An acknowledgement goes only to the node that acquired the record. When
//!   metadata says another node leads the partition, it fails locally with
//!   `NOT_LEADER_OR_FOLLOWER`.
//! - No fetch is sent while records acquired from the node are buffered, and
//!   every fetch asks for at most `max_poll_records` records.
//! - Leader errors apply the response's `CurrentLeader`/`NodeEndpoints`,
//!   refresh metadata and back the partition off.

use std::sync::atomic::{AtomicI16, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::{debug, warn};

use super::acks::{self, AckRange, Resolved};
use super::completed_fetch;
use super::config::AcquireMode;
use super::session::{FINAL_EPOCH, SessionDelta, SessionPartition, ShareSession};
use super::state::{Assigned, Backoff, Inner, NodeHandle};
use crate::BrokerId;
use crate::consumer::TopicPartition;
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::metadata::BrokerInfo;
use crate::network::BrokerConnection;
use crate::protocol::{
    ApiKey, ShareAcknowledgePartition, ShareAcknowledgeRequest, ShareAcknowledgeResponse,
    ShareAcknowledgeTopic, ShareFetchForgottenTopic, ShareFetchPartition, ShareFetchRequest,
    ShareFetchResponse, ShareFetchTopic, ShareLeaderIdAndEpoch, ShareNodeEndpoint, VersionedDecode,
    versions,
};

/// How long an idle manager sleeps before re-checking leadership on its own.
const IDLE_TICK: Duration = Duration::from_millis(500);

/// Acknowledgements taken from the book for one request.
type TakenAcks = Vec<(TopicPartition, [u8; 16], Vec<AckRange>)>;

/// Start the request manager of `node`.
pub(crate) fn spawn(inner: &Arc<Inner>, node: BrokerId) -> NodeHandle {
    let notify = Arc::new(Notify::new());
    let ack_version = Arc::new(AtomicI16::new(-1));
    let manager = Manager {
        node,
        session: ShareSession::default(),
        backoff: None,
        opened_for_close: false,
        session_generation: inner.session_generation.load(Ordering::Acquire),
        ack_version: Arc::clone(&ack_version),
    };
    let task = tokio::spawn(manager.run(Arc::downgrade(inner), Arc::clone(&notify)));
    NodeHandle {
        notify,
        ack_version,
        task,
    }
}

/// What the manager sends next.
enum Plan {
    Wait(Duration),
    Exit,
    Fetch(FetchPlan),
    Acknowledge(AckPlan),
}

struct FetchPlan {
    wanted: Vec<Assigned>,
    delta: SessionDelta,
    acks: TakenAcks,
    max_wait: Duration,
}

struct AckPlan {
    acks: TakenAcks,
    /// Close the session (epoch `-1`) and stop.
    close: bool,
}

struct Manager {
    node: BrokerId,
    session: ShareSession,
    /// Backoff after a failed request.
    backoff: Option<Backoff>,
    /// A session was opened only to deliver acknowledgements on close.
    opened_for_close: bool,
    /// The member the session belongs to, as [`Inner::session_generation`].
    session_generation: u64,
    ack_version: Arc<AtomicI16>,
}

impl Manager {
    async fn run(mut self, inner: Weak<Inner>, notify: Arc<Notify>) {
        loop {
            let notified = notify.notified();
            let Some(inner) = inner.upgrade() else {
                return;
            };
            let (plan, failed) = self.plan(&inner);
            if !failed.is_empty() {
                inner.report(&failed);
                let topics: Vec<String> =
                    failed.iter().map(|r| r.partition.topic.clone()).collect();
                refresh_topics(&inner, topics).await;
            }
            match plan {
                Plan::Exit => return,
                Plan::Wait(wait) => {
                    drop(inner);
                    let _ = tokio::time::timeout(wait, notified).await;
                }
                Plan::Fetch(plan) => self.fetch(&inner, plan).await,
                Plan::Acknowledge(plan) => {
                    let close = plan.close;
                    self.acknowledge(&inner, plan).await;
                    if close {
                        return;
                    }
                }
            }
        }
    }

    /// Decide the next request from the shared state.
    fn plan(&mut self, inner: &Inner) -> (Plan, Vec<Resolved>) {
        let node = self.node;
        let now = Instant::now();
        let generation = inner.session_generation.load(Ordering::Acquire);
        if generation != self.session_generation {
            self.session_generation = generation;
            self.session.reset();
        }
        let mut state = inner.state.lock();

        if state.closing {
            if self.session.is_established() {
                let acks = state.book.take(node, None);
                return (Plan::Acknowledge(AckPlan { acks, close: true }), Vec::new());
            }
            if state.book.has_pending(node) && !self.opened_for_close {
                self.opened_for_close = true;
                return (Plan::Fetch(open_only()), Vec::new());
            }
            let failed = state.book.fail_pending(
                |(n, _), _| *n == node,
                |_| KrafkaError::closed(format!("no share session on node {node} at close")),
            );
            return (Plan::Exit, failed);
        }
        if state.fatal.is_some() {
            return (Plan::Wait(IDLE_TICK), Vec::new());
        }

        // An acknowledgement belongs to the node that acquired the record.
        let metadata = &inner.metadata;
        let failed = state.book.fail_pending(
            |(n, tp), _| {
                *n == node
                    && metadata
                        .leader(&tp.topic, tp.partition)
                        .is_some_and(|leader| leader != node)
            },
            |(_, tp)| {
                KrafkaError::broker(
                    ErrorCode::NotLeaderForPartition,
                    format!(
                        "node {node} acquired these records of {}-{} but no longer leads it",
                        tp.topic, tp.partition
                    ),
                )
            },
        );

        if let Some(backoff) = self.backoff
            && backoff.until > now
        {
            return (Plan::Wait(backoff.until - now), failed);
        }

        let mut next_partition_retry = IDLE_TICK;
        let wanted: Vec<Assigned> = state
            .assigned
            .iter()
            .filter(|a| metadata.leader(&a.partition.topic, a.partition.partition) == Some(node))
            .filter(|a| match state.backoff.get(&a.partition) {
                Some(b) if b.until > now => {
                    next_partition_retry = next_partition_retry.min(b.until - now);
                    false
                }
                _ => true,
            })
            .cloned()
            .collect();
        let can_fetch = state.fetch_wanted && !wanted.is_empty() && !state.buffers_node(node);
        let pending = state.book.has_pending(node);
        let max_wait = inner.config.fetch_max_wait;

        if !self.session.is_established() {
            if can_fetch {
                let delta = self.session.delta(&session_partitions(&wanted));
                return (
                    Plan::Fetch(FetchPlan {
                        wanted,
                        delta,
                        acks: Vec::new(),
                        max_wait,
                    }),
                    failed,
                );
            }
            if pending {
                return (Plan::Fetch(open_only()), failed);
            }
            return (Plan::Wait(next_partition_retry), failed);
        }

        if pending {
            let wanted_partitions: Vec<TopicPartition> =
                wanted.iter().map(|a| a.partition.clone()).collect();
            let standalone = !can_fetch
                || state.book.has_waiters(node)
                || state.book.pending_for(node).any(|(tp, entry)| {
                    acks::has_renew(&entry.pending) || !wanted_partitions.contains(tp)
                });
            if standalone {
                let acks = state.book.take(node, None);
                return (Plan::Acknowledge(AckPlan { acks, close: false }), failed);
            }
        }
        if can_fetch {
            let wanted_partitions: Vec<TopicPartition> =
                wanted.iter().map(|a| a.partition.clone()).collect();
            let acks = state.book.take(node, Some(&wanted_partitions));
            let delta = self.session.delta(&session_partitions(&wanted));
            return (
                Plan::Fetch(FetchPlan {
                    wanted,
                    delta,
                    acks,
                    max_wait,
                }),
                failed,
            );
        }
        (Plan::Wait(next_partition_retry), failed)
    }

    /// Connect to the node and record its `ShareAcknowledge` version.
    async fn connect(&self, inner: &Inner) -> Result<Arc<BrokerConnection>> {
        let address = inner
            .metadata
            .broker(self.node)
            .map(|b| b.address().to_string())
            .ok_or_else(|| {
                KrafkaError::unavailable(format!("broker {} not in metadata", self.node))
            })?;
        let conn = inner.pool.get_connection_by_id(self.node, &address).await?;
        if let Some(version) = conn.negotiate_api_version(
            ApiKey::ShareAcknowledge,
            versions::SHARE_ACKNOWLEDGE_MAX,
            versions::SHARE_ACKNOWLEDGE_MIN,
        ) {
            self.ack_version.store(version, Ordering::Release);
        }
        Ok(conn)
    }

    async fn send_fetch(&self, inner: &Inner, plan: &FetchPlan) -> Result<ShareFetchResponse> {
        let conn = self.connect(inner).await?;
        let version = conn
            .negotiate_api_version(
                ApiKey::ShareFetch,
                versions::SHARE_FETCH_MAX,
                versions::SHARE_FETCH_MIN,
            )
            .ok_or_else(|| KrafkaError::share_groups_unsupported(ApiKey::ShareFetch))?;
        let acquire_mode = inner.config.acquire_mode;
        if acquire_mode == AcquireMode::RecordLimit && version < 2 {
            return Err(KrafkaError::config(format!(
                "AcquireMode::RecordLimit needs KIP-1206 (ShareFetch v2, Kafka 4.2+); \
                 broker {} supports ShareFetch v{version}",
                self.node
            )));
        }

        let max_records = inner.config.max_poll_records;
        let mut topics: Vec<ShareFetchTopic> = Vec::new();
        let mut add = |topic_id: [u8; 16], partition: ShareFetchPartition| match topics
            .iter_mut()
            .find(|t| t.topic_id == topic_id)
        {
            Some(topic) => topic.partitions.push(partition),
            None => topics.push(ShareFetchTopic {
                topic_id,
                partitions: vec![partition],
            }),
        };
        let mut listed: HashSet<SessionPartition> = HashSet::new();
        for (tp, topic_id, ranges) in &plan.acks {
            listed.insert((*topic_id, tp.partition));
            add(
                *topic_id,
                ShareFetchPartition {
                    partition_index: tp.partition,
                    acknowledgement_batches: acks::to_batches(ranges),
                },
            );
        }
        for &(topic_id, partition) in &plan.delta.added {
            if listed.insert((topic_id, partition)) {
                add(
                    topic_id,
                    ShareFetchPartition {
                        partition_index: partition,
                        acknowledgement_batches: Vec::new(),
                    },
                );
            }
        }
        let mut forgotten_topics: Vec<ShareFetchForgottenTopic> = Vec::new();
        for &(topic_id, partition) in &plan.delta.forgotten {
            match forgotten_topics.iter_mut().find(|t| t.topic_id == topic_id) {
                Some(topic) => topic.partitions.push(partition),
                None => forgotten_topics.push(ShareFetchForgottenTopic {
                    topic_id,
                    partitions: vec![partition],
                }),
            }
        }

        let request = ShareFetchRequest {
            group_id: Some(inner.config.group_id.clone()),
            member_id: Some(inner.member.lock().member_id.clone()),
            share_session_epoch: self.session.epoch(),
            max_wait_ms: crate::util::duration_to_millis_i32(plan.max_wait),
            min_bytes: inner.config.fetch_min_bytes,
            max_bytes: inner.config.fetch_max_bytes,
            max_records,
            batch_size: inner.config.batch_size.min(max_records),
            topics,
            forgotten_topics,
        };
        let buf = conn
            .send_request(ApiKey::ShareFetch, version, |buf| {
                if version >= 2 {
                    request.encode_v2(buf, acquire_mode.to_i8(), false)
                } else {
                    request.encode_v1(buf)
                }
            })
            .await?;
        let response = ShareFetchResponse::decode_versioned(version, &mut buf.as_ref())?;
        conn.notify_throttle(response.throttle_time_ms);
        Ok(response)
    }

    async fn fetch(&mut self, inner: &Arc<Inner>, plan: FetchPlan) {
        match self.send_fetch(inner, &plan).await {
            Err(error) => self.on_failure(inner, &plan.acks, error, true),
            Ok(response) if !response.error_code.is_ok() => {
                let error = KrafkaError::broker(
                    response.error_code,
                    response
                        .error_message
                        .unwrap_or_else(|| "ShareFetch failed".to_string()),
                );
                self.on_failure(inner, &plan.acks, error, true);
            }
            Ok(response) => self.on_fetch(inner, plan, response).await,
        }
    }

    async fn on_fetch(
        &mut self,
        inner: &Arc<Inner>,
        plan: FetchPlan,
        response: ShareFetchResponse,
    ) {
        let node = self.node;
        self.session.on_fetch(&plan.delta);
        self.backoff = None;
        if response.acquisition_lock_timeout_ms > 0 {
            inner
                .acquisition_lock_timeout_ms
                .store(response.acquisition_lock_timeout_ms, Ordering::Relaxed);
        }

        let names = topic_names(&plan.wanted, &plan.acks);
        let retry_cutoff = Some(Instant::now() - inner.lock_timeout());
        let mut acked: HashSet<TopicPartition> = HashSet::new();
        let mut resolved = Vec::new();
        let mut moves = Vec::new();
        let mut arrived = false;
        {
            let mut state = inner.state.lock();
            for topic in &response.responses {
                let Some(name) = names
                    .get(&topic.topic_id)
                    .cloned()
                    .or_else(|| inner.metadata.topic_name_for_id(&topic.topic_id))
                else {
                    debug!(
                        node,
                        "ShareFetch answered for an unknown topic id; skipping it"
                    );
                    continue;
                };
                for partition in &topic.partitions {
                    let tp = TopicPartition::new(&name, partition.partition_index);
                    if plan.acks.iter().any(|(p, _, _)| *p == tp) {
                        let outcome = if !partition.acknowledge_error_code.is_ok() {
                            Err(KrafkaError::broker(
                                partition.acknowledge_error_code,
                                partition
                                    .acknowledge_error_message
                                    .clone()
                                    .unwrap_or_else(|| "acknowledgement failed".to_string()),
                            ))
                        } else if acks::is_leader_error(partition.error_code) {
                            Err(KrafkaError::broker(
                                partition.error_code,
                                "the node does not lead the partition",
                            ))
                        } else {
                            Ok(())
                        };
                        resolved.extend(state.book.settle(node, &tp, outcome, retry_cutoff));
                        acked.insert(tp.clone());
                    }
                    if !partition.error_code.is_ok() {
                        let previous = state.backoff.get(&tp).copied();
                        state.backoff.insert(tp.clone(), Backoff::after(previous));
                        if acks::is_leader_error(partition.error_code) {
                            moves.push((tp, partition.current_leader.clone()));
                        } else {
                            warn!(
                                node,
                                topic = %tp.topic,
                                partition = tp.partition,
                                error = ?partition.error_code,
                                "ShareFetch failed for a partition"
                            );
                        }
                        continue;
                    }
                    state.backoff.remove(&tp);
                    let processed = completed_fetch::process(
                        &name,
                        topic.topic_id,
                        partition.partition_index,
                        node,
                        partition.records.as_ref(),
                        &partition.acquired_records,
                        inner.config.max_decompressed_size,
                    );
                    for range in processed.acks {
                        state.book.add(node, tp.clone(), topic.topic_id, range);
                    }
                    if !processed.fetch.records.is_empty() {
                        state.buffer.push_back(processed.fetch);
                        arrived = true;
                    }
                }
            }
            resolved.extend(self.settle_unanswered(&mut state.book, &plan.acks, &acked));
        }
        if arrived {
            inner.records_ready.notify_waiters();
        }
        inner.report(&resolved);
        self.apply_moves(inner, moves, &response.node_endpoints)
            .await;
    }

    async fn send_acknowledge(
        &self,
        inner: &Inner,
        plan: &AckPlan,
    ) -> Result<ShareAcknowledgeResponse> {
        let conn = self.connect(inner).await?;
        let version = conn
            .negotiate_api_version(
                ApiKey::ShareAcknowledge,
                versions::SHARE_ACKNOWLEDGE_MAX,
                versions::SHARE_ACKNOWLEDGE_MIN,
            )
            .ok_or_else(|| KrafkaError::share_groups_unsupported(ApiKey::ShareAcknowledge))?;
        let mut topics: Vec<ShareAcknowledgeTopic> = Vec::new();
        let mut renew = false;
        for (tp, topic_id, ranges) in &plan.acks {
            renew |= acks::has_renew(ranges);
            let partition = ShareAcknowledgePartition {
                partition_index: tp.partition,
                acknowledgement_batches: acks::to_batches(ranges),
            };
            match topics.iter_mut().find(|t| t.topic_id == *topic_id) {
                Some(topic) => topic.partitions.push(partition),
                None => topics.push(ShareAcknowledgeTopic {
                    topic_id: *topic_id,
                    partitions: vec![partition],
                }),
            }
        }
        let request = ShareAcknowledgeRequest {
            group_id: Some(inner.config.group_id.clone()),
            member_id: Some(inner.member.lock().member_id.clone()),
            share_session_epoch: if plan.close {
                FINAL_EPOCH
            } else {
                self.session.epoch()
            },
            topics,
        };
        let buf = conn
            .send_request(ApiKey::ShareAcknowledge, version, |buf| {
                if version >= 2 {
                    request.encode_v2(buf, renew)
                } else {
                    request.encode_v1(buf)
                }
            })
            .await?;
        let response = ShareAcknowledgeResponse::decode_versioned(version, &mut buf.as_ref())?;
        conn.notify_throttle(response.throttle_time_ms);
        Ok(response)
    }

    async fn acknowledge(&mut self, inner: &Arc<Inner>, plan: AckPlan) {
        let node = self.node;
        let response = match self.send_acknowledge(inner, &plan).await {
            Ok(response) if response.error_code.is_ok() => response,
            Ok(response) => {
                let error = KrafkaError::broker(
                    response.error_code,
                    response
                        .error_message
                        .unwrap_or_else(|| "ShareAcknowledge failed".to_string()),
                );
                return self.on_failure(inner, &plan.acks, error, !plan.close);
            }
            Err(error) => return self.on_failure(inner, &plan.acks, error, !plan.close),
        };
        if plan.close {
            self.session.reset();
        } else {
            self.session.on_acknowledge();
        }
        self.backoff = None;
        if response.acquisition_lock_timeout_ms > 0 {
            inner
                .acquisition_lock_timeout_ms
                .store(response.acquisition_lock_timeout_ms, Ordering::Relaxed);
        }

        let names = topic_names(&[], &plan.acks);
        let retry_cutoff = (!plan.close).then(|| Instant::now() - inner.lock_timeout());
        let mut acked: HashSet<TopicPartition> = HashSet::new();
        let mut resolved = Vec::new();
        let mut moves = Vec::new();
        {
            let mut state = inner.state.lock();
            for topic in &response.responses {
                let Some(name) = names.get(&topic.topic_id) else {
                    continue;
                };
                for partition in &topic.partitions {
                    let tp = TopicPartition::new(name, partition.partition_index);
                    let outcome = if partition.error_code.is_ok() {
                        Ok(())
                    } else {
                        if acks::is_leader_error(partition.error_code) {
                            moves.push((tp.clone(), partition.current_leader.clone()));
                        }
                        Err(KrafkaError::broker(
                            partition.error_code,
                            partition
                                .error_message
                                .clone()
                                .unwrap_or_else(|| "acknowledgement failed".to_string()),
                        ))
                    };
                    resolved.extend(state.book.settle(node, &tp, outcome, retry_cutoff));
                    acked.insert(tp);
                }
            }
            resolved.extend(self.settle_unanswered(&mut state.book, &plan.acks, &acked));
        }
        inner.report(&resolved);
        self.apply_moves(inner, moves, &response.node_endpoints)
            .await;
    }

    /// A request failed as a whole. Its acknowledgements are retried or
    /// resolved by class; the session is reset when `reset` is set.
    fn on_failure(&mut self, inner: &Inner, taken: &TakenAcks, error: KrafkaError, reset: bool) {
        let node = self.node;
        debug!(node, %error, "share request failed");
        if reset {
            self.session.reset();
        }
        self.backoff = Some(Backoff::after(self.backoff));
        let fatal = matches!(
            &error,
            KrafkaError::Config { .. }
                | KrafkaError::Protocol {
                    kind: ProtocolErrorKind::UnknownApiVersion,
                    ..
                }
        );
        let retry_cutoff = reset.then(|| Instant::now() - inner.lock_timeout());
        let resolved: Vec<Resolved> = {
            let mut state = inner.state.lock();
            if fatal {
                state.fatal = Some(error.clone());
            }
            taken
                .iter()
                .filter_map(|(tp, _, _)| {
                    state
                        .book
                        .settle(node, tp, Err(error.clone()), retry_cutoff)
                })
                .collect()
        };
        if fatal {
            warn!(node, %error, "share consumer cannot fetch");
            inner.records_ready.notify_waiters();
        }
        inner.report(&resolved);
    }

    /// Resolve acknowledgements a response did not answer.
    fn settle_unanswered(
        &self,
        book: &mut acks::AckBook,
        taken: &TakenAcks,
        answered: &HashSet<TopicPartition>,
    ) -> Vec<Resolved> {
        taken
            .iter()
            .filter(|(tp, _, _)| !answered.contains(tp))
            .filter_map(|(tp, _, _)| {
                book.settle(
                    self.node,
                    tp,
                    Err(KrafkaError::broker(
                        ErrorCode::UnknownServerError,
                        "the broker returned no acknowledgement result for the partition",
                    )),
                    None,
                )
            })
            .collect()
    }

    /// Apply leader changes a response reported and refresh metadata.
    async fn apply_moves(
        &self,
        inner: &Arc<Inner>,
        moves: Vec<(TopicPartition, ShareLeaderIdAndEpoch)>,
        endpoints: &[ShareNodeEndpoint],
    ) {
        if moves.is_empty() {
            return;
        }
        for (tp, leader) in &moves {
            let endpoint = endpoints
                .iter()
                .find(|e| e.node_id == leader.leader_id)
                .map(|e| BrokerInfo::new(e.node_id, e.host.clone(), e.port, e.rack.clone()));
            inner.metadata.apply_leader_hint(
                &tp.topic,
                tp.partition,
                leader.leader_id,
                leader.leader_epoch,
                endpoint,
            );
        }
        refresh_topics(inner, moves.into_iter().map(|(tp, _)| tp.topic).collect()).await;
        inner.wake_nodes();
    }
}

/// A `ShareFetch` that only opens a session: no partitions, no wait.
fn open_only() -> FetchPlan {
    FetchPlan {
        wanted: Vec::new(),
        delta: SessionDelta::default(),
        acks: Vec::new(),
        max_wait: Duration::ZERO,
    }
}

fn session_partitions(wanted: &[Assigned]) -> Vec<SessionPartition> {
    wanted
        .iter()
        .map(|a| (a.topic_id, a.partition.partition))
        .collect()
}

/// Topic names by id, from what the request was about.
fn topic_names(wanted: &[Assigned], taken: &TakenAcks) -> HashMap<[u8; 16], String> {
    let mut names = HashMap::new();
    for a in wanted {
        names.insert(a.topic_id, a.partition.topic.clone());
    }
    for (tp, topic_id, _) in taken {
        names.insert(*topic_id, tp.topic.clone());
    }
    names
}

/// Refresh metadata for `topics`, logging a failure.
async fn refresh_topics(inner: &Inner, mut topics: Vec<String>) {
    topics.sort_unstable();
    topics.dedup();
    if topics.is_empty() {
        return;
    }
    let refs: Vec<&str> = topics.iter().map(String::as_str).collect();
    if let Err(error) = inner.metadata.refresh_for_topics(Some(&refs)).await {
        debug!(%error, "metadata refresh after a leader change failed");
    }
}

#[cfg(all(test, feature = "test-broker"))]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::testing::FakeBroker;

    fn manager(node: BrokerId) -> Manager {
        Manager {
            node,
            session: ShareSession::default(),
            backoff: None,
            opened_for_close: false,
            session_generation: 0,
            ack_version: Arc::new(AtomicI16::new(-1)),
        }
    }

    /// A network error, a timeout and a top-level error each reset the
    /// node's session, so the next request reopens it at epoch 0. Control:
    /// without the reset the next request reuses an epoch the broker may
    /// already have consumed.
    #[tokio::test]
    async fn a_failed_request_resets_the_session() {
        let broker = FakeBroker::start().await.unwrap();
        let consumer = crate::Kafka::builder(broker.bootstrap_servers())
            .connect()
            .await
            .unwrap()
            .share_consumer("g")
            .build()
            .await
            .unwrap();
        let errors = [
            KrafkaError::unavailable("connection reset"),
            KrafkaError::timeout("ShareFetch"),
            KrafkaError::broker(ErrorCode::InvalidShareSessionEpoch, "stale"),
        ];
        for error in errors {
            let mut m = manager(0);
            m.session.on_fetch(&SessionDelta::default());
            assert!(m.session.is_established());
            m.on_failure(&consumer.0, &Vec::new(), error.clone(), true);
            assert!(
                !m.session.is_established(),
                "{error} must reset the session"
            );
            assert!(m.backoff.is_some(), "{error} backs the node off");
        }
        consumer.close().await.unwrap();
    }
}
