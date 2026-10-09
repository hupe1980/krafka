//! State shared by the share consumer's handles and its background tasks.
//!
//! One synchronous lock guards everything the application and the request
//! managers both touch: the assignment, the fetch buffer, the delivered
//! records and the ack book. It is never held across an `.await`.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI16, AtomicI32, AtomicU64, Ordering};
use std::time::Duration;

use ahash::AHashMap as HashMap;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use super::acks::{AckBook, AckRange, AckType, Resolved};
use super::commit::{self, AcknowledgementCommitCallback};
use super::completed_fetch::CompletedFetch;
use super::config::{AcknowledgementMode, ShareConsumerConfig};
use super::membership::MemberState;
use super::request_manager;
use crate::consumer::{ConsumerRecord, TopicPartition};
use crate::error::{KrafkaError, Result};
use crate::metadata::ClusterMetadata;
use crate::network::ConnectionPool;
use crate::serdes::Deserializer;
use crate::{BrokerId, Offset, PartitionId};

/// First and longest delay between attempts after a partition or node error.
const BACKOFF_INITIAL: Duration = Duration::from_millis(100);
const BACKOFF_MAX: Duration = Duration::from_secs(1);

/// Exponential backoff state of a partition or a node.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Backoff {
    attempts: u32,
    pub until: Instant,
}

impl Backoff {
    /// The backoff after one more failure.
    pub(crate) fn after(previous: Option<Self>) -> Self {
        let attempts = previous.map_or(1, |b| b.attempts.saturating_add(1));
        let delay = BACKOFF_INITIAL
            .saturating_mul(1u32 << (attempts - 1).min(16))
            .min(BACKOFF_MAX);
        Self {
            attempts,
            until: Instant::now() + delay,
        }
    }
}

/// A partition assigned to this member whose topic name is known.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Assigned {
    pub partition: TopicPartition,
    pub topic_id: [u8; 16],
}

/// Who acquired a delivered record.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Acquisition {
    pub node: BrokerId,
    pub topic_id: [u8; 16],
}

/// Everything behind [`Inner::state`].
#[derive(Debug, Default)]
pub(crate) struct State {
    /// Assigned partitions whose topic id resolved to a name.
    pub assigned: Vec<Assigned>,
    /// Assigned topic ids metadata does not know yet.
    pub unresolved: Vec<([u8; 16], Vec<PartitionId>)>,
    /// Fetched records not yet handed to the application. Every record here
    /// holds an acquisition lock on the broker.
    pub buffer: VecDeque<CompletedFetch>,
    /// Records handed to the application and not yet acknowledged.
    pub outstanding: HashMap<TopicPartition, BTreeMap<Offset, Acquisition>>,
    pub book: AckBook,
    /// A `poll()` is waiting for records.
    pub fetch_wanted: bool,
    /// An error to return from the next `poll()`, after the records that
    /// preceded it were returned.
    pub deferred_error: Option<KrafkaError>,
    /// An error every `poll()` returns from now on.
    pub fatal: Option<KrafkaError>,
    /// Partitions not to fetch until their backoff passes.
    pub backoff: HashMap<TopicPartition, Backoff>,
    /// `close()` has started: request managers send their last request.
    pub closing: bool,
}

impl State {
    /// Whether records acquired from `node` wait in the buffer.
    pub(crate) fn buffers_node(&self, node: BrokerId) -> bool {
        self.buffer
            .iter()
            .any(|f| f.node == node && !f.records.is_empty())
    }
}

/// A request manager's handle.
#[derive(Debug)]
pub(crate) struct NodeHandle {
    pub notify: Arc<Notify>,
    /// The negotiated `ShareAcknowledge` version, `-1` until known.
    pub ack_version: Arc<AtomicI16>,
    pub task: JoinHandle<()>,
}

/// Shared by every [`ShareConsumer`](super::ShareConsumer) clone.
pub(crate) struct Inner {
    pub config: ShareConsumerConfig,
    pub metadata: Arc<ClusterMetadata>,
    pub pool: Arc<ConnectionPool>,
    pub metrics: Arc<crate::metrics::ConsumerRecorder>,
    /// Where `metrics()` and the KIP-714 reporter read from.
    pub metrics_source: Arc<crate::metrics::MetricsSource>,
    /// The KIP-714 reporter.
    pub telemetry: crate::telemetry::Telemetry,
    pub key_deserializer: Option<Arc<dyn Deserializer>>,
    pub value_deserializer: Option<Arc<dyn Deserializer>>,
    pub callback: Option<AcknowledgementCommitCallback>,
    pub member: Mutex<MemberState>,
    pub state: Mutex<State>,
    /// Woken when records arrive, the assignment changes, an error is set,
    /// on `wakeup()` and on close.
    pub records_ready: Notify,
    /// Serializes `poll()`/`recv()`.
    pub poll_lock: tokio::sync::Mutex<()>,
    pub closed: AtomicBool,
    /// `close()` has stopped the request managers; none may start again.
    pub shut_down: AtomicBool,
    pub wakeup: AtomicBool,
    /// Bumped when the member id changes; request managers then reset their
    /// sessions, which belong to the old member.
    pub session_generation: AtomicU64,
    /// Acquisition-lock duration last reported by a broker (KIP-1222), in
    /// milliseconds, or `-1`.
    pub acquisition_lock_timeout_ms: AtomicI32,
    pub nodes: Mutex<HashMap<BrokerId, NodeHandle>>,
    pub heartbeat_task: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(task) = self.heartbeat_task.lock().take() {
            task.abort();
        }
        for (_, node) in self.nodes.lock().drain() {
            node.task.abort();
        }
        if !self.closed.load(Ordering::Relaxed) && !std::thread::panicking() {
            tracing::warn!(
                "ShareConsumer dropped without close(); its acquired records stay locked until \
                 their acquisition locks expire and the group notices the member is gone"
            );
        }
    }
}

impl Inner {
    /// How long an acknowledgement may be retried in the background.
    pub(crate) fn lock_timeout(&self) -> Duration {
        let ms = self.acquisition_lock_timeout_ms.load(Ordering::Relaxed);
        if ms > 0 {
            Duration::from_millis(ms as u64)
        } else {
            Duration::from_secs(30)
        }
    }

    /// Start request managers for every node that leads an assigned
    /// partition or holds acknowledgements, and wake them all.
    pub(crate) fn wake_nodes(self: &Arc<Self>) {
        let mut wanted: Vec<BrokerId> = {
            let state = self.state.lock();
            let mut nodes = state.book.nodes();
            nodes.extend(state.assigned.iter().filter_map(|a| {
                self.metadata
                    .leader(&a.partition.topic, a.partition.partition)
            }));
            nodes
        };
        wanted.sort_unstable();
        wanted.dedup();

        // Lock order: `state` is never taken while `nodes` is held.
        let mut nodes = self.nodes.lock();
        if self.shut_down.load(Ordering::Acquire) {
            return;
        }
        for node in wanted {
            nodes
                .entry(node)
                .or_insert_with(|| request_manager::spawn(self, node));
        }
        for handle in nodes.values() {
            handle.notify.notify_one();
        }
    }

    /// Wake one request manager.
    pub(crate) fn wake_node(self: &Arc<Self>, node: BrokerId) {
        let notify = self.nodes.lock().get(&node).map(|h| Arc::clone(&h.notify));
        match notify {
            Some(notify) => notify.notify_one(),
            None => self.wake_nodes(),
        }
    }

    /// Pass resolved acknowledgements to the callback.
    pub(crate) fn report(&self, resolved: &[Resolved]) {
        commit::report(self.callback.as_ref(), resolved);
    }

    /// Turn every record the application holds into an `ACCEPT` (implicit
    /// mode). Returns whether anything was queued.
    pub(crate) fn accept_delivered(&self) -> bool {
        if self.config.acknowledgement_mode != AcknowledgementMode::Implicit {
            return false;
        }
        let mut state = self.state.lock();
        let outstanding = std::mem::take(&mut state.outstanding);
        let queued = !outstanding.is_empty();
        for (partition, records) in outstanding {
            for (offset, acquisition) in records {
                state.book.add(
                    acquisition.node,
                    partition.clone(),
                    acquisition.topic_id,
                    AckRange::one(offset, AckType::Accept),
                );
            }
        }
        queued
    }

    /// Acknowledge one delivered record (explicit mode).
    pub(crate) fn acknowledge(
        self: &Arc<Self>,
        record: &ConsumerRecord,
        kind: AckType,
    ) -> Result<()> {
        if self.config.acknowledgement_mode != AcknowledgementMode::Explicit {
            return Err(KrafkaError::illegal_state(
                "acknowledging a record needs AcknowledgementMode::Explicit",
            ));
        }
        if self.closed.load(Ordering::Acquire) {
            return Err(KrafkaError::closed("share consumer is closed"));
        }
        let partition = TopicPartition::new(&*record.topic, record.partition);
        let node = {
            let mut state = self.state.lock();
            let acquisition = state
                .outstanding
                .get(&partition)
                .and_then(|records| records.get(&record.offset))
                .copied()
                .ok_or_else(|| {
                    KrafkaError::illegal_state(format!(
                        "record {}-{}@{} is not awaiting acknowledgement",
                        record.topic, record.partition, record.offset
                    ))
                })?;
            if kind == AckType::Renew {
                let version = self
                    .nodes
                    .lock()
                    .get(&acquisition.node)
                    .map_or(-1, |h| h.ack_version.load(Ordering::Acquire));
                if version < 2 {
                    return Err(KrafkaError::protocol_kind(
                        crate::error::ProtocolErrorKind::UnknownApiVersion,
                        format!(
                            "renewing an acquisition lock needs KIP-1222 (ShareAcknowledge v2, \
                             Kafka 4.2+); broker {} negotiated v{version}",
                            acquisition.node
                        ),
                    ));
                }
            } else if let Some(records) = state.outstanding.get_mut(&partition) {
                records.remove(&record.offset);
                if records.is_empty() {
                    state.outstanding.remove(&partition);
                }
            }
            state.book.add(
                acquisition.node,
                partition,
                acquisition.topic_id,
                AckRange::one(record.offset, kind),
            );
            acquisition.node
        };
        self.wake_node(node);
        Ok(())
    }

    /// Drop every partition's state after the member lost its membership.
    /// Pending acknowledgements fail with `error`; returns them for reporting.
    pub(crate) fn drop_partition_state(&self, error: &KrafkaError) -> Vec<Resolved> {
        let mut state = self.state.lock();
        state.buffer.clear();
        state.outstanding.clear();
        state.backoff.clear();
        state.deferred_error = None;
        state.book.fail_pending(|_, _| true, |_| error.clone())
    }

    /// Install a new assignment. Only the state of revoked partitions is
    /// dropped: their buffered records are released and their delivered,
    /// unacknowledged records forgotten. Their queued acknowledgements are
    /// still sent to the node that acquired them.
    pub(crate) fn install_assignment(
        &self,
        assigned: Vec<Assigned>,
        unresolved: Vec<([u8; 16], Vec<PartitionId>)>,
    ) -> bool {
        let mut state = self.state.lock();
        let revoked: Vec<TopicPartition> = state
            .assigned
            .iter()
            .filter(|old| !assigned.iter().any(|new| new.partition == old.partition))
            .map(|old| old.partition.clone())
            .collect();
        let changed = state.assigned != assigned || state.unresolved != unresolved;
        let state = &mut *state;
        for partition in &revoked {
            let mut kept = VecDeque::with_capacity(state.buffer.len());
            for fetch in state.buffer.drain(..) {
                if fetch.topic == partition.topic && fetch.partition == partition.partition {
                    for record in &fetch.records {
                        state.book.add(
                            fetch.node,
                            partition.clone(),
                            fetch.topic_id,
                            AckRange::one(record.offset, AckType::Release),
                        );
                    }
                } else {
                    kept.push_back(fetch);
                }
            }
            state.buffer = kept;
            state.outstanding.remove(partition);
            state.backoff.remove(partition);
        }
        state.assigned = assigned;
        state.unresolved = unresolved;
        changed
    }

    /// Hand at most `max` buffered records to the application.
    ///
    /// Deserialization runs on copies; the buffer is only changed once every
    /// copy is decoded. A record that fails to deserialize is released and its
    /// error returned after the records that preceded it.
    pub(crate) fn take_records(
        self: &Arc<Self>,
        max: usize,
    ) -> Result<Option<Vec<ConsumerRecord>>> {
        let candidates: Vec<ConsumerRecord> = {
            let state = self.state.lock();
            state
                .buffer
                .iter()
                .flat_map(|f| f.records.iter())
                .take(max)
                .cloned()
                .collect()
        };
        if candidates.is_empty() {
            return Ok(None);
        }

        let mut decoded = Vec::with_capacity(candidates.len());
        let mut failure = None;
        for mut record in candidates {
            match self.deserialize(&mut record) {
                Ok(()) => decoded.push(record),
                Err(error) => {
                    failure = Some((record, error));
                    break;
                }
            }
        }

        let mut delivered = Vec::with_capacity(decoded.len());
        let mut released_node = None;
        {
            let mut state = self.state.lock();
            for record in decoded {
                if let Some(acquisition) = pop_front(&mut state, &record) {
                    state
                        .outstanding
                        .entry(TopicPartition::new(&*record.topic, record.partition))
                        .or_default()
                        .insert(record.offset, acquisition);
                    delivered.push(record);
                }
            }
            if let Some((record, _)) = &failure
                && let Some(acquisition) = pop_front(&mut state, record)
            {
                state.book.add(
                    acquisition.node,
                    TopicPartition::new(&*record.topic, record.partition),
                    acquisition.topic_id,
                    AckRange::one(record.offset, AckType::Release),
                );
                released_node = Some(acquisition.node);
            }
            state.buffer.retain(|f| !f.records.is_empty());
            if let Some((_, error)) = &failure
                && !delivered.is_empty()
            {
                state.deferred_error = Some(error.clone());
            }
        }
        if let Some(node) = released_node {
            self.wake_node(node);
        }

        if !delivered.is_empty() {
            let bytes: u64 = delivered
                .iter()
                .map(|r| r.value.as_ref().map_or(0, |v| v.len() as u64))
                .sum();
            self.metrics.record_receive(delivered.len() as u64, bytes);
            return Ok(Some(delivered));
        }
        match failure {
            Some((_, error)) => Err(error),
            None => Ok(None),
        }
    }

    /// Apply the configured deserializers to `record`.
    fn deserialize(&self, record: &mut ConsumerRecord) -> Result<()> {
        let decode = |decoder: &Arc<dyn Deserializer>,
                      record: &ConsumerRecord,
                      payload: &bytes::Bytes,
                      is_key: bool| {
            decoder
                .deserialize(&record.topic, &record.headers, payload.clone(), is_key)
                .map_err(|e| {
                    KrafkaError::record_deserialization(
                        &*record.topic,
                        record.partition,
                        record.offset,
                        if is_key { "key" } else { "value" },
                        e.to_string(),
                    )
                })
        };
        if let (Some(decoder), Some(value)) = (&self.value_deserializer, record.value.as_ref()) {
            record.value = Some(decode(decoder, record, value, false)?);
        }
        if let (Some(decoder), Some(key)) = (&self.key_deserializer, record.key.as_ref()) {
            record.key = Some(decode(decoder, record, key, true)?);
        }
        Ok(())
    }
}

/// Remove `record` from the front of its completed fetch, if it is still
/// there, returning who acquired it.
fn pop_front(state: &mut State, record: &ConsumerRecord) -> Option<Acquisition> {
    let fetch = state.buffer.iter_mut().find(|f| {
        *f.topic == *record.topic
            && f.partition == record.partition
            && f.records.front().is_some_and(|r| r.offset == record.offset)
    })?;
    fetch.records.pop_front();
    Some(Acquisition {
        node: fetch.node,
        topic_id: fetch.topic_id,
    })
}
