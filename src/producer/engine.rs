//! The send engine: one task per producer that owns every piece of send state.
//!
//! The engine holds the per-partition queues, the producer identity, the
//! per-broker in-flight counts and the in-flight requests themselves, which it
//! polls inline — no task is spawned per batch or per request. Each wave
//! builds at most one Produce request per broker, carrying the head batch of
//! every ready partition that broker leads:
//!
//! - one batch per partition on the wire, sent in seal order;
//! - at most [`MAX_IN_FLIGHT_PER_BROKER`] requests per broker;
//! - a request never exceeds `max_request_size`, except a single batch that
//!   does on its own.
//!
//! Every batch resolves by its deadline (`created + delivery_timeout`),
//! checked before every attempt and by the engine's timer whether the batch
//! is open, queued, backing off or in flight. A flush only seals; nothing in
//! the loop awaits a batch.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use ahash::{AHashMap, AHashSet};
use bytes::{BufMut as _, Bytes};
use futures::future::BoxFuture;
use futures::stream::{FuturesUnordered, StreamExt};
use parking_lot::Mutex;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, warn};

use super::TransactionState;
use super::batch::{self, Batch, RecordData, Waiter};
use super::gate::TxnGate;
use super::identity::{
    Action, BatchIdentity, Context, Failure, Identity, Mode, PartitionAnswer, PartitionKey,
    PartitionSeq, classify, is_definitive_non_append,
};
use super::record::{DeliveryConfirmation, RecordMetadata, TopicHandle};
use super::retry::Backoff;
use crate::BrokerId;
use crate::PartitionId;
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::interceptor::ProducerInterceptor;
use crate::metadata::ClusterMetadata;
use crate::metrics::ProducerRecorder;
use crate::protocol::{
    ApiKey, Compression, InitProducerIdRequest, InitProducerIdResponse, ProducePartitionData,
    ProduceRequest, ProduceResponse, ProduceTopicData, VersionedDecode, VersionedEncode, versions,
};

/// Produce requests in flight per broker connection (Java's idempotent
/// maximum). With one batch per partition on the wire, a second request to a
/// broker only ever carries other partitions.
pub(crate) const MAX_IN_FLIGHT_PER_BROKER: usize = 5;

/// How often a batch may be halved after `MESSAGE_TOO_LARGE`.
const MAX_SPLIT_DEPTH: u8 = 2;

/// Commands drained per loop turn before the engine drains batches, so
/// concurrent senders coalesce.
const COMMANDS_PER_TURN: usize = 4096;

/// Request framing allowance per batch beyond its estimated record bytes.
const PER_BATCH_OVERHEAD: usize = 128;

/// When nothing is due, the loop still wakes this often.
const IDLE_TICK: Duration = Duration::from_secs(1);

/// What the send engine needs to know about its producer.
#[derive(Clone)]
pub(crate) struct EngineConfig {
    pub(crate) batch_size: usize,
    pub(crate) linger: Duration,
    pub(crate) delivery_timeout: Duration,
    pub(crate) request_timeout: Duration,
    pub(crate) max_request_size: usize,
    pub(crate) acks: i16,
    pub(crate) compression: Compression,
    pub(crate) compression_level: Option<i32>,
    pub(crate) topic_compression: AHashMap<String, Compression>,
    pub(crate) client_id: String,
    pub(crate) transactional_id: Option<String>,
    pub(crate) backoff: Backoff,
    pub(crate) interceptor: Arc<dyn ProducerInterceptor>,
    pub(crate) mode: Mode,
    /// The identity an idempotent producer starts with.
    pub(crate) identity: Option<(i64, i16)>,
    /// The transactional producer's gate, told when the engine hits a fatal
    /// error.
    pub(crate) gate: Option<Arc<TxnGate>>,
}

impl std::fmt::Debug for EngineConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineConfig")
            .field("batch_size", &self.batch_size)
            .field("linger", &self.linger)
            .field("delivery_timeout", &self.delivery_timeout)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}

/// State the engine shares with the producer handle.
#[derive(Debug, Default)]
pub(crate) struct Shared {
    /// Set when the producer can send nothing more.
    fatal: Mutex<Option<KrafkaError>>,
}

impl Shared {
    pub(crate) fn fatal(&self) -> Option<KrafkaError> {
        self.fatal.lock().clone()
    }

    fn set_fatal(&self, error: KrafkaError) {
        let mut fatal = self.fatal.lock();
        if fatal.is_none() {
            *fatal = Some(error);
        }
    }
}

/// One record entering the engine.
#[derive(Debug)]
pub(crate) struct Append {
    pub(crate) topic: TopicHandle,
    pub(crate) partition: PartitionId,
    pub(crate) data: RecordData,
    pub(crate) size: usize,
    /// The barrier generation the send belongs to.
    pub(crate) generation: u64,
    pub(crate) waiter: Waiter,
}

/// A message to the engine.
#[derive(Debug)]
// `Append` is the per-record message; boxing it would cost an allocation per
// send to save slot space on the rare control messages.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Command {
    Append(Append),
    /// Seal every open batch, and every later append of a generation up to
    /// this one, so nothing of it waits for `linger`.
    Flush {
        generation: u64,
    },
    /// Fail every batch not on the wire with `error` (a transaction abort).
    FailUnsent {
        error: KrafkaError,
        done: oneshot::Sender<()>,
    },
    /// Adopt a transactional identity; partitions restart at sequence 0.
    SetIdentity {
        producer_id: i64,
        epoch: i16,
        tv2: bool,
    },
    /// Fail everything, including what is on the wire, and refuse new sends.
    Terminate {
        error: KrafkaError,
    },
}

/// Where the producer's identity stands.
#[derive(Debug)]
enum IdentityState {
    /// `idempotent(false)`: batches carry no producer id.
    Plain,
    /// The transactional producer has not initialised yet.
    Uninit,
    /// A new producer id is being fetched.
    Init,
    Ready(Identity),
    Fatal(KrafkaError),
}

#[derive(Debug, Default)]
struct PartitionQueue {
    open: Option<Batch>,
    sealed: VecDeque<Batch>,
    in_flight: Option<Batch>,
    seq: PartitionSeq,
    backoff_until: Option<Instant>,
    last_acked_offset: Option<i64>,
}

impl PartitionQueue {
    fn is_idle(&self) -> bool {
        self.open.is_none() && self.sealed.is_empty() && self.in_flight.is_none()
    }

    fn seal_open(&mut self) {
        if let Some(open) = self.open.take() {
            self.sealed.push_back(open);
        }
    }
}

/// A batch handed to a request future.
struct WireBatch {
    key: PartitionKey,
    records: Arc<Vec<RecordData>>,
    stamp: Option<BatchIdentity>,
    encoded: Option<Bytes>,
    estimated: usize,
}

/// What became of one batch in a request.
#[derive(Debug)]
enum BatchOutcome {
    /// Never handed to a connection.
    NotWritten(KrafkaError),
    /// Its encoded frame alone exceeds `max_request_size`; not written.
    TooLarge,
    /// Written, but no answer came: timeout, lost connection, a response that
    /// does not decode or does not mention the partition.
    NoAnswer(KrafkaError),
    /// The broker's answer for the partition.
    Answer {
        code: ErrorCode,
        base_offset: i64,
        log_append_time: i64,
        log_start_offset: i64,
    },
    /// `acks = 0`: written, and no answer is coming.
    Unacknowledged,
}

struct BatchResult {
    key: PartitionKey,
    encoded: Option<Bytes>,
    outcome: BatchOutcome,
}

struct RequestDone {
    broker: BrokerId,
    results: Vec<BatchResult>,
}

/// What a request future needs; shared by all of them.
struct RequestContext {
    metadata: Arc<ClusterMetadata>,
    metrics: Arc<ProducerRecorder>,
    client_id: String,
    max_request_size: usize,
    acks: i16,
    timeout_ms: i32,
    transactional_id: Option<String>,
    compression: Compression,
    compression_level: Option<i32>,
    topic_compression: AHashMap<String, Compression>,
}

impl RequestContext {
    fn compression_for(&self, topic: &str) -> Compression {
        self.topic_compression
            .get(topic)
            .copied()
            .unwrap_or(self.compression)
    }
}

/// Start the engine for one producer. Returns the command sender.
pub(crate) fn spawn(
    config: EngineConfig,
    metadata: Arc<ClusterMetadata>,
    metrics: Arc<ProducerRecorder>,
    shared: Arc<Shared>,
) -> (mpsc::UnboundedSender<Command>, tokio::task::JoinHandle<()>) {
    let (sender, receiver) = mpsc::unbounded_channel();
    let identity = match (config.mode, config.identity) {
        (Mode::Plain, _) => IdentityState::Plain,
        (Mode::Idempotent, Some((producer_id, epoch))) => IdentityState::Ready(Identity {
            producer_id,
            epoch,
            generation: 1,
        }),
        (Mode::Idempotent, None) => IdentityState::Init,
        (Mode::Transactional { .. }, _) => IdentityState::Uninit,
    };
    let context = Arc::new(RequestContext {
        metadata: Arc::clone(&metadata),
        metrics: Arc::clone(&metrics),
        client_id: config.client_id.clone(),
        max_request_size: config.max_request_size,
        acks: config.acks,
        timeout_ms: crate::util::duration_to_millis_i32(config.request_timeout),
        transactional_id: config.transactional_id.clone(),
        compression: config.compression,
        compression_level: config.compression_level,
        topic_compression: config.topic_compression.clone(),
    });
    let engine = Engine {
        mode: config.mode,
        config,
        metadata,
        metrics,
        shared,
        context,
        partitions: AHashMap::new(),
        active: AHashSet::new(),
        brokers: AHashMap::new(),
        requests: FuturesUnordered::new(),
        identity,
        bump_requested: false,
        epoch_retry_used: false,
        stamped_unresolved: 0,
        sealed_through: None,
        refresh: None,
        next_refresh_at: None,
        reinit: None,
        generation: 1,
        channel_closed: false,
    };
    let handle = tokio::spawn(engine.run(receiver));
    (sender, handle)
}

struct Engine {
    config: EngineConfig,
    mode: Mode,
    metadata: Arc<ClusterMetadata>,
    metrics: Arc<ProducerRecorder>,
    shared: Arc<Shared>,
    context: Arc<RequestContext>,
    partitions: AHashMap<PartitionKey, PartitionQueue>,
    /// Partitions with anything open, queued or in flight.
    active: AHashSet<PartitionKey>,
    /// Produce requests in flight per broker.
    brokers: AHashMap<BrokerId, usize>,
    requests: FuturesUnordered<BoxFuture<'static, RequestDone>>,
    identity: IdentityState,
    /// A terminal failure of a stamped batch asked for a new epoch; applied
    /// once no stamped batch is unresolved.
    bump_requested: bool,
    epoch_retry_used: bool,
    /// Stamped batches without a final outcome, under any generation.
    stamped_unresolved: usize,
    /// Appends of this generation or earlier are sealed at once.
    sealed_through: Option<u64>,
    refresh: Option<BoxFuture<'static, ()>>,
    next_refresh_at: Option<Instant>,
    reinit: Option<BoxFuture<'static, Result<(i64, i16)>>>,
    /// The last identity generation handed out; only grows.
    generation: u64,
    channel_closed: bool,
}

impl Engine {
    async fn run(mut self, mut commands: mpsc::UnboundedReceiver<Command>) {
        loop {
            let now = Instant::now();
            self.expire(now);
            self.drain(now);
            if self.channel_closed && self.active.is_empty() && self.requests.is_empty() {
                break;
            }
            let wake = self.next_wake(now);
            tokio::select! {
                command = commands.recv(), if !self.channel_closed => match command {
                    Some(command) => {
                        self.handle(command);
                        for _ in 1..COMMANDS_PER_TURN {
                            match commands.try_recv() {
                                Ok(command) => self.handle(command),
                                Err(_) => break,
                            }
                        }
                    }
                    None => {
                        // Every handle is gone: send what is buffered, then stop.
                        self.channel_closed = true;
                        self.sealed_through = Some(u64::MAX);
                        for key in self.active.clone() {
                            if let Some(queue) = self.partitions.get_mut(&key) {
                                queue.seal_open();
                            }
                        }
                    }
                },
                Some(done) = self.requests.next(), if !self.requests.is_empty() => {
                    self.complete(done);
                }
                () = poll_slot(&mut self.refresh), if self.refresh.is_some() => {
                    self.refresh = None;
                }
                init = poll_slot(&mut self.reinit), if self.reinit.is_some() => {
                    self.reinit = None;
                    self.reinitialised(init);
                }
                () = tokio::time::sleep_until(wake) => {}
            }
        }
        debug!("producer send engine stopped");
    }

    // ── Commands ────────────────────────────────────────────────────────

    fn handle(&mut self, command: Command) {
        match command {
            Command::Append(append) => self.append(append),
            Command::Flush { generation } => {
                self.sealed_through = Some(
                    self.sealed_through
                        .map_or(generation, |g| g.max(generation)),
                );
                for key in &self.active {
                    if let Some(queue) = self.partitions.get_mut(key) {
                        queue.seal_open();
                    }
                }
            }
            Command::FailUnsent { error, done } => {
                for key in self.active.clone() {
                    let Some(queue) = self.partitions.get_mut(&key) else {
                        continue;
                    };
                    let unsent: Vec<Batch> =
                        queue.sealed.drain(..).chain(queue.open.take()).collect();
                    for batch in unsent {
                        self.fail(&key, batch, error.clone());
                    }
                }
                self.prune();
                let _ = done.send(());
            }
            Command::SetIdentity {
                producer_id,
                epoch,
                tv2,
            } => {
                self.mode = Mode::Transactional { tv2 };
                if let IdentityState::Ready(identity) = &self.identity
                    && identity.producer_id == producer_id
                    && identity.epoch == epoch
                {
                    return;
                }
                self.generation += 1;
                self.identity = IdentityState::Ready(Identity {
                    producer_id,
                    epoch,
                    generation: self.generation,
                });
            }
            Command::Terminate { error } => {
                self.shared.set_fatal(error.clone());
                self.identity = IdentityState::Fatal(error.clone());
                for key in self.active.clone() {
                    let Some(queue) = self.partitions.get_mut(&key) else {
                        continue;
                    };
                    let unsent: Vec<Batch> =
                        queue.sealed.drain(..).chain(queue.open.take()).collect();
                    if let Some(in_flight) = queue.in_flight.as_mut() {
                        answer_all(
                            &*self.config.interceptor,
                            &key,
                            in_flight,
                            &Err(error.clone()),
                        );
                    }
                    for batch in unsent {
                        self.fail(&key, batch, error.clone());
                    }
                }
                self.prune();
            }
        }
    }

    fn append(&mut self, append: Append) {
        let Append {
            topic,
            partition,
            data,
            size,
            generation,
            waiter,
        } = append;
        if let IdentityState::Fatal(error) = &self.identity {
            waiter.answer(
                &*self.config.interceptor,
                &topic,
                partition,
                &data.headers,
                Err(error.clone()),
            );
            return;
        }
        let now = Instant::now();
        let batch_size = self.config.batch_size;
        let deadline = now + self.config.delivery_timeout;
        let seal_now = self.sealed_through.is_some_and(|g| generation <= g);
        let key: PartitionKey = (topic, partition);
        let queue = self.partitions.entry(key.clone()).or_default();
        if queue
            .open
            .as_ref()
            .is_some_and(|open| !open.fits(size, batch_size))
        {
            queue.seal_open();
        }
        let open = queue.open.get_or_insert_with(|| Batch::new(now, deadline));
        open.push(data, size, generation, waiter);
        if open.bytes >= batch_size || seal_now {
            queue.seal_open();
        }
        self.active.insert(key);
    }

    // ── Time ────────────────────────────────────────────────────────────

    /// Answer every batch whose deadline has passed.
    fn expire(&mut self, now: Instant) {
        let mut expired: Vec<(PartitionKey, Batch)> = Vec::new();
        for key in &self.active {
            let Some(queue) = self.partitions.get_mut(key) else {
                continue;
            };
            if queue.open.as_ref().is_some_and(|b| b.deadline <= now)
                && let Some(open) = queue.open.take()
            {
                expired.push((key.clone(), open));
            }
            // Seal order is creation order, so expired batches are in front.
            while queue.sealed.front().is_some_and(|b| b.deadline <= now) {
                if let Some(batch) = queue.sealed.pop_front() {
                    expired.push((key.clone(), batch));
                }
            }
            // An expired batch on the wire answers its callers now and keeps
            // the partition's slot until its request resolves.
            if let Some(in_flight) = queue.in_flight.as_mut()
                && in_flight.waiters.is_some()
                && in_flight.deadline <= now
            {
                let error = delivery_timeout(in_flight, true);
                self.metrics.record_error_for_topic(&key.0);
                answer_all(&*self.config.interceptor, key, in_flight, &Err(error));
            }
        }
        for (key, batch) in expired {
            let error = delivery_timeout(&batch, batch.maybe_appended);
            self.fail(&key, batch, error);
        }
    }

    fn next_wake(&self, now: Instant) -> Instant {
        let mut wake = now + IDLE_TICK;
        let linger = self.config.linger;
        for key in &self.active {
            let Some(queue) = self.partitions.get(key) else {
                continue;
            };
            if let Some(open) = &queue.open {
                wake = wake.min(open.deadline);
                // A batch whose linger has passed and that `drain` still left
                // waits for a response, a bump or a command, not for a timer.
                let lingered = open.created + linger;
                if queue.in_flight.is_none()
                    && queue.sealed.is_empty()
                    && !linger.is_zero()
                    && lingered > now
                {
                    wake = wake.min(lingered);
                }
            }
            if let Some(front) = queue.sealed.front() {
                wake = wake.min(front.deadline);
            }
            if let Some(in_flight) = &queue.in_flight
                && in_flight.waiters.is_some()
            {
                wake = wake.min(in_flight.deadline);
            }
            if let Some(until) = queue.backoff_until
                && queue.in_flight.is_none()
                && until > now
            {
                wake = wake.min(until);
            }
        }
        if let Some(at) = self.next_refresh_at {
            wake = wake.min(at);
        }
        wake.max(now)
    }

    // ── Drain ───────────────────────────────────────────────────────────

    fn drain(&mut self, now: Instant) {
        self.apply_bump();
        let can_stamp = match &self.identity {
            IdentityState::Plain => true,
            IdentityState::Ready(_) => !self.bump_requested,
            _ => false,
        };
        let identity = match &self.identity {
            IdentityState::Ready(identity) => Some(*identity),
            _ => None,
        };

        let linger = self.config.linger;
        // Ordered by broker, so requests go out in the same order every run.
        let mut ready: std::collections::BTreeMap<BrokerId, Vec<PartitionKey>> =
            std::collections::BTreeMap::new();
        let mut leaderless: Vec<Arc<str>> = Vec::new();
        for key in &self.active {
            let Some(queue) = self.partitions.get(key) else {
                continue;
            };
            if queue.in_flight.is_some() || queue.backoff_until.is_some_and(|t| t > now) {
                continue;
            }
            let head = match queue.sealed.front() {
                Some(head) => head,
                None => match &queue.open {
                    Some(open) if linger.is_zero() || open.created + linger <= now => open,
                    _ => continue,
                },
            };
            if self.mode.has_identity() && head.stamp.is_none() && !can_stamp {
                continue;
            }
            match self.metadata.leader(&key.0, key.1).filter(|id| *id >= 0) {
                Some(leader) => {
                    if self.brokers.get(&leader).copied().unwrap_or(0) < MAX_IN_FLIGHT_PER_BROKER {
                        ready.entry(leader).or_default().push(key.clone());
                    }
                }
                None => leaderless.push(Arc::clone(&key.0)),
            }
        }

        if !leaderless.is_empty() {
            self.request_refresh(leaderless, now);
        }

        let max_request_size = self.config.max_request_size;
        for (broker, mut keys) in ready {
            keys.sort_unstable();
            let mut wire: Vec<WireBatch> = Vec::with_capacity(keys.len());
            let mut size = PER_BATCH_OVERHEAD;
            for key in keys {
                let Some(queue) = self.partitions.get_mut(&key) else {
                    continue;
                };
                let head_bytes = queue
                    .sealed
                    .front()
                    .or(queue.open.as_ref())
                    .map_or(0, |b| b.bytes + PER_BATCH_OVERHEAD + key.0.len());
                let head_alone = queue.sealed.front().is_some_and(|b| b.alone);
                if !wire.is_empty() && (head_alone || size + head_bytes > max_request_size) {
                    continue;
                }
                let Some(mut batch) = queue.sealed.pop_front().or_else(|| queue.open.take()) else {
                    continue;
                };
                if self.mode.has_identity() && batch.stamp.is_none() {
                    let Some(identity) = identity else {
                        // Unreachable: `can_stamp` was checked above.
                        queue.sealed.push_front(batch);
                        continue;
                    };
                    let count = i32::try_from(batch.len()).unwrap_or(i32::MAX);
                    batch.stamp = Some(queue.seq.stamp(identity, count));
                    batch.encoded = None;
                    self.stamped_unresolved += 1;
                }
                batch.attempts += 1;
                size += head_bytes;
                wire.push(WireBatch {
                    key: key.clone(),
                    records: Arc::clone(&batch.records),
                    stamp: batch.stamp,
                    encoded: batch.encoded.clone(),
                    estimated: batch.bytes,
                });
                let alone = batch.alone;
                queue.in_flight = Some(batch);
                queue.backoff_until = None;
                if alone {
                    break;
                }
            }
            if wire.is_empty() {
                continue;
            }
            *self.brokers.entry(broker).or_default() += 1;
            let context = Arc::clone(&self.context);
            self.requests.push(Box::pin(produce(context, broker, wire)));
        }
    }

    fn request_refresh(&mut self, topics: Vec<Arc<str>>, now: Instant) {
        if self.refresh.is_some() || self.next_refresh_at.is_some_and(|at| at > now) {
            return;
        }
        self.next_refresh_at = Some(now + self.config.backoff.delay(1));
        let metadata = Arc::clone(&self.metadata);
        let mut topics: Vec<String> = topics.iter().map(|t| t.to_string()).collect();
        topics.sort_unstable();
        topics.dedup();
        self.refresh = Some(Box::pin(async move {
            let names: Vec<&str> = topics.iter().map(String::as_str).collect();
            if let Err(error) = metadata.force_refresh(Some(&names)).await {
                debug!(%error, "metadata refresh for leaderless partitions failed");
            }
        }));
    }

    /// Move to the next epoch once nothing stamped is unresolved.
    fn apply_bump(&mut self) {
        if matches!(self.identity, IdentityState::Init) {
            self.start_reinit();
            return;
        }
        if !self.bump_requested || self.stamped_unresolved > 0 {
            return;
        }
        let IdentityState::Ready(identity) = &self.identity else {
            return;
        };
        match identity.bumped(self.generation + 1) {
            Some(next) => {
                self.generation += 1;
                debug!(
                    producer_id = next.producer_id,
                    epoch = next.epoch,
                    "bumped the producer epoch; every partition restarts at sequence 0"
                );
                self.identity = IdentityState::Ready(next);
                self.bump_requested = false;
            }
            None => self.start_reinit(),
        }
    }

    /// Fetch a new producer id: the epoch is exhausted.
    fn start_reinit(&mut self) {
        if self.reinit.is_some() {
            return;
        }
        self.identity = IdentityState::Init;
        let metadata = Arc::clone(&self.metadata);
        let backoff = self.config.backoff.clone();
        let deadline = tokio::time::Instant::now() + self.config.delivery_timeout;
        self.reinit = Some(Box::pin(async move {
            init_producer_id(&metadata, &backoff, deadline).await
        }));
    }

    fn reinitialised(&mut self, result: Result<(i64, i16)>) {
        match result {
            Ok((producer_id, epoch)) => {
                self.generation += 1;
                self.identity = IdentityState::Ready(Identity {
                    producer_id,
                    epoch,
                    generation: self.generation,
                });
                self.bump_requested = false;
            }
            // `drain` starts another attempt.
            Err(error) if error.is_retriable() => {
                debug!(%error, "InitProducerId failed; retrying");
            }
            Err(error) => self.fatal(error),
        }
    }

    // ── Completion ──────────────────────────────────────────────────────

    fn complete(&mut self, done: RequestDone) {
        if let Some(count) = self.brokers.get_mut(&done.broker) {
            *count = count.saturating_sub(1);
        }
        let now = Instant::now();
        for result in done.results {
            let key = result.key;
            let Some(queue) = self.partitions.get_mut(&key) else {
                continue;
            };
            let Some(mut batch) = queue.in_flight.take() else {
                continue;
            };
            if result.encoded.is_some() {
                batch.encoded = result.encoded;
            }
            self.resolve(key, batch, result.outcome, now);
        }
        self.prune();
    }

    fn resolve(
        &mut self,
        key: PartitionKey,
        mut batch: Batch,
        outcome: BatchOutcome,
        now: Instant,
    ) {
        // Answered at its deadline while on the wire: whatever happened now,
        // the batch is finished.
        let expired = batch.waiters.is_none();
        match outcome {
            BatchOutcome::Unacknowledged => {
                self.acknowledge(&key, batch, -1, -1, DeliveryConfirmation::Unacknowledged);
            }
            BatchOutcome::Answer {
                code,
                base_offset,
                log_append_time,
                log_start_offset,
            } => {
                batch.written = true;
                if code != ErrorCode::None && !is_definitive_non_append(code) {
                    batch.maybe_appended = true;
                }
                let last_acked_offset = self.partitions.get(&key).and_then(|q| q.last_acked_offset);
                let action = classify(
                    Context {
                        mode: self.mode,
                        record_count: batch.len(),
                        last_acked_offset,
                        epoch_retry_used: self.epoch_retry_used,
                    },
                    PartitionAnswer {
                        code,
                        log_start_offset,
                    },
                );
                self.act(
                    key,
                    batch,
                    action,
                    code,
                    base_offset,
                    log_append_time,
                    now,
                    expired,
                );
            }
            BatchOutcome::NoAnswer(error) => {
                batch.written = true;
                batch.maybe_appended = true;
                self.retry_or_fail(key, batch, error, now, expired);
            }
            BatchOutcome::NotWritten(error) => {
                if error.is_retriable() {
                    self.retry_or_fail(key, batch, error, now, expired);
                } else {
                    self.fail(&key, batch, error);
                }
            }
            BatchOutcome::TooLarge => {
                let action = if batch.len() > 1 && !matches!(self.mode, Mode::Transactional { .. })
                {
                    Action::Split {
                        bump: batch.stamp.is_some(),
                    }
                } else {
                    Action::Fail {
                        failure: if matches!(self.mode, Mode::Transactional { .. }) {
                            Failure::Abortable
                        } else {
                            Failure::Broker
                        },
                        bump: matches!(self.mode, Mode::Idempotent),
                    }
                };
                self.act(
                    key,
                    batch,
                    action,
                    ErrorCode::MessageTooLarge,
                    -1,
                    -1,
                    now,
                    expired,
                );
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn act(
        &mut self,
        key: PartitionKey,
        mut batch: Batch,
        action: Action,
        code: ErrorCode,
        base_offset: i64,
        log_append_time: i64,
        now: Instant,
        expired: bool,
    ) {
        match action {
            Action::Ack { duplicate } => {
                let delivery = if duplicate && base_offset < 0 {
                    DeliveryConfirmation::Deduplicated
                } else {
                    DeliveryConfirmation::Offset
                };
                self.acknowledge(&key, batch, base_offset, log_append_time, delivery);
            }
            Action::Retry => {
                let error =
                    KrafkaError::broker(code, format!("produce to {}-{} failed", key.0, key.1));
                self.retry_or_fail(key, batch, error, now, expired);
            }
            Action::Restamp if !expired && batch.deadline > now => {
                if code == ErrorCode::InvalidProducerEpoch {
                    self.epoch_retry_used = true;
                }
                if code == ErrorCode::UnknownProducerId {
                    debug!(
                        topic = %key.0,
                        partition = key.1,
                        "UNKNOWN_PRODUCER_ID after retention removed the producer's batches; \
                         bumping the epoch and resending"
                    );
                }
                self.unstamp(&mut batch);
                self.bump_requested = true;
                if let Some(queue) = self.partitions.get_mut(&key) {
                    queue.sealed.push_front(batch);
                }
            }
            Action::Restamp => {
                let error = delivery_timeout(&batch, batch.maybe_appended);
                self.fail(&key, batch, error);
            }
            Action::Split { bump }
                if !expired && batch.deadline > now && batch.split_depth < MAX_SPLIT_DEPTH =>
            {
                if bump {
                    self.bump_requested = true;
                }
                self.unstamp(&mut batch);
                warn!(
                    topic = %key.0,
                    partition = key.1,
                    records = batch.len(),
                    "batch rejected as too large; splitting it in two"
                );
                self.metrics.record_retry();
                match batch.split() {
                    Some((head, tail)) => {
                        if let Some(queue) = self.partitions.get_mut(&key) {
                            queue.sealed.push_front(tail);
                            queue.sealed.push_front(head);
                        }
                    }
                    None => unreachable_single_split(),
                }
            }
            Action::Split { .. } => {
                let error = KrafkaError::broker(
                    code,
                    format!("batch for {}-{} is too large", key.0, key.1),
                );
                self.fail(&key, batch, error);
            }
            Action::Fail { failure, .. } => {
                let error = match failure {
                    Failure::Broker if code == ErrorCode::UnsupportedCompressionType => {
                        let codec = self
                            .config
                            .topic_compression
                            .get(&*key.0)
                            .copied()
                            .unwrap_or(self.config.compression);
                        KrafkaError::compression_unsupported(codec, &key.0, key.1)
                    }
                    Failure::Broker => {
                        KrafkaError::broker(code, format!("produce to {}-{} failed", key.0, key.1))
                    }
                    Failure::OutOfOrder => {
                        self.metrics.data_loss_detected.inc();
                        warn!(
                            topic = %key.0,
                            partition = key.1,
                            ?code,
                            "the broker lost an earlier batch of this producer (log truncation \
                             or unclean leader election); failing the batch and bumping the epoch"
                        );
                        KrafkaError::out_of_order_sequence(
                            key.0.to_string(),
                            key.1,
                            format!(
                                "{code:?}: an earlier batch of this producer is missing from the \
                                 log; this batch was not written and the producer moved to a new \
                                 epoch"
                            ),
                        )
                    }
                    Failure::Abortable => KrafkaError::transaction_abortable(format!(
                        "produce to {}-{} failed with {code:?}",
                        key.0, key.1
                    )),
                };
                self.fail(&key, batch, error);
            }
            Action::Fatal => {
                let error = if matches!(
                    code,
                    ErrorCode::ProducerFenced | ErrorCode::InvalidProducerEpoch
                ) {
                    KrafkaError::fenced(format!(
                        "{code:?}: another producer with this identity took over"
                    ))
                } else {
                    KrafkaError::broker(code, format!("produce to {}-{} failed", key.0, key.1))
                };
                self.fail(&key, batch, error.clone());
                self.fatal(error);
            }
        }
    }

    /// Retry with the same stamp after a backoff, unless the deadline passed.
    fn retry_or_fail(
        &mut self,
        key: PartitionKey,
        mut batch: Batch,
        error: KrafkaError,
        now: Instant,
        expired: bool,
    ) {
        if expired || batch.deadline <= now {
            let mut timeout = delivery_timeout(&batch, batch.maybe_appended);
            if let KrafkaError::DeliveryTimeout { message, .. } = &mut timeout {
                message.push_str(&format!("; last error: {error}"));
            }
            self.fail(&key, batch, timeout);
            return;
        }
        self.metrics.record_retry();
        debug!(
            topic = %key.0,
            partition = key.1,
            attempt = batch.attempts,
            %error,
            "retrying produce"
        );
        let delay = self.config.backoff.delay(batch.attempts);
        batch.last_error = Some(error);
        if let Some(queue) = self.partitions.get_mut(&key) {
            queue.backoff_until = Some((now + delay).min(batch.deadline));
            queue.sealed.push_front(batch);
        }
    }

    fn acknowledge(
        &mut self,
        key: &PartitionKey,
        mut batch: Batch,
        base_offset: i64,
        log_append_time: i64,
        delivery: DeliveryConfirmation,
    ) {
        if batch.stamp.take().is_some() {
            self.stamped_unresolved = self.stamped_unresolved.saturating_sub(1);
        }
        let count = batch.len();
        if let Some(queue) = self.partitions.get_mut(key) {
            queue.backoff_until = None;
            if base_offset >= 0 {
                queue.last_acked_offset = Some(base_offset + count as i64 - 1);
            }
        }
        self.metrics
            .record_batch_for_topic(&key.0, count as u64, batch.bytes as u64);
        self.metrics.send_latency.record(batch.created.elapsed());
        let Some(waiters) = batch.waiters.take() else {
            return;
        };
        for (index, (waiter, data)) in waiters.into_iter().zip(batch.records.iter()).enumerate() {
            let metadata = RecordMetadata {
                topic: key.0.to_string(),
                partition: key.1,
                offset: if base_offset >= 0 {
                    base_offset + index as i64
                } else {
                    -1
                },
                timestamp: if log_append_time >= 0 {
                    log_append_time
                } else {
                    data.timestamp
                },
                delivery,
            };
            waiter.answer(
                &*self.config.interceptor,
                &key.0,
                key.1,
                &data.headers,
                Ok(metadata),
            );
        }
    }

    /// Fail a batch's records. A stamped batch spends its range: the
    /// idempotent producer moves to the next epoch.
    fn fail(&mut self, key: &PartitionKey, mut batch: Batch, error: KrafkaError) {
        if batch.stamp.take().is_some() {
            self.stamped_unresolved = self.stamped_unresolved.saturating_sub(1);
            if matches!(self.mode, Mode::Idempotent) {
                self.bump_requested = true;
            }
        }
        if let Some(queue) = self.partitions.get_mut(key) {
            queue.backoff_until = None;
        }
        if batch.waiters.is_none() {
            return;
        }
        self.metrics.record_error_for_topic(&key.0);
        answer_all(&*self.config.interceptor, key, &mut batch, &Err(error));
    }

    fn unstamp(&mut self, batch: &mut Batch) {
        if batch.stamp.take().is_some() {
            self.stamped_unresolved = self.stamped_unresolved.saturating_sub(1);
        }
        batch.encoded = None;
        batch.written = false;
    }

    /// The producer can send nothing more.
    fn fatal(&mut self, error: KrafkaError) {
        warn!(%error, "producer hit a fatal error; every later send fails");
        self.shared.set_fatal(error.clone());
        self.identity = IdentityState::Fatal(error.clone());
        if let Some(gate) = &self.config.gate {
            gate.set(TransactionState::Fatal);
        }
        for key in self.active.clone() {
            let Some(queue) = self.partitions.get_mut(&key) else {
                continue;
            };
            let unsent: Vec<Batch> = queue.sealed.drain(..).chain(queue.open.take()).collect();
            for batch in unsent {
                self.fail(&key, batch, error.clone());
            }
        }
    }

    /// Forget partitions with nothing left.
    fn prune(&mut self) {
        let partitions = &mut self.partitions;
        self.active
            .retain(|key| partitions.get(key).is_some_and(|queue| !queue.is_idle()));
        if partitions.len() > 4 * self.active.len() + 1024 {
            let active = &self.active;
            partitions
                .retain(|key, queue| active.contains(key) || queue.last_acked_offset.is_some());
        }
    }
}

#[cold]
fn unreachable_single_split() {
    debug_assert!(false, "a batch of one record is never split");
}

/// Await an optional boxed future slot without consuming it.
async fn poll_slot<T>(slot: &mut Option<BoxFuture<'static, T>>) -> T {
    match slot.as_mut() {
        Some(future) => future.await,
        None => std::future::pending().await,
    }
}

fn delivery_timeout(batch: &Batch, possibly_written: bool) -> KrafkaError {
    let last = batch
        .last_error
        .as_ref()
        .map_or_else(String::new, |e| format!("; last error: {e}"));
    KrafkaError::delivery_timeout(
        possibly_written,
        format!(
            "not acknowledged within delivery_timeout after {} attempt(s){last}",
            batch.attempts
        ),
    )
}

/// Answer every caller of `batch` with `result`'s error, or with nothing for
/// success (callers acknowledge through [`Engine::acknowledge`]).
fn answer_all(
    interceptor: &dyn ProducerInterceptor,
    key: &PartitionKey,
    batch: &mut Batch,
    result: &Result<()>,
) {
    let Some(waiters) = batch.waiters.take() else {
        return;
    };
    let Err(error) = result else {
        return;
    };
    for (waiter, data) in waiters.into_iter().zip(batch.records.iter()) {
        waiter.answer(
            interceptor,
            &key.0,
            key.1,
            &data.headers,
            Err(error.clone()),
        );
    }
}

// ── The request ─────────────────────────────────────────────────────────────

/// Send one Produce request to `broker` carrying `batches`.
async fn produce(
    context: Arc<RequestContext>,
    broker: BrokerId,
    batches: Vec<WireBatch>,
) -> RequestDone {
    let not_written = |batches: Vec<WireBatch>, error: KrafkaError| RequestDone {
        broker,
        results: batches
            .into_iter()
            .map(|b| BatchResult {
                key: b.key,
                encoded: b.encoded,
                outcome: BatchOutcome::NotWritten(error.clone()),
            })
            .collect(),
    };

    let connection = match context.metadata.get_broker_connection(broker).await {
        Ok(connection) => connection,
        Err(error) => {
            refresh_for(&context, batches.iter().map(|b| &b.key)).await;
            return not_written(batches, error);
        }
    };
    connection.await_throttle().await;
    let Some(mut version) = connection.negotiate_api_version(
        ApiKey::Produce,
        versions::PRODUCE_MAX,
        versions::PRODUCE_MIN,
    ) else {
        let error = KrafkaError::protocol_kind(
            ProtocolErrorKind::UnknownApiVersion,
            "no mutually supported Produce API version",
        );
        return not_written(batches, error);
    };

    // Encode what has no bytes yet; a stamp never changes, so a retry reuses
    // them.
    let mut results: Vec<BatchResult> = Vec::with_capacity(batches.len());
    let mut sendable: Vec<(PartitionKey, Bytes)> = Vec::with_capacity(batches.len());
    for batch in batches {
        let bytes = match batch.encoded {
            Some(bytes) => Ok(bytes),
            None => encode_batch(&context, &batch).await,
        };
        match bytes {
            Ok(bytes) => sendable.push((batch.key, bytes)),
            Err(error) => results.push(BatchResult {
                key: batch.key,
                encoded: None,
                outcome: BatchOutcome::NotWritten(error),
            }),
        }
    }
    if sendable.is_empty() {
        return RequestDone { broker, results };
    }

    let mut request = ProduceRequest {
        transactional_id: context.transactional_id.clone(),
        acks: context.acks,
        timeout_ms: context.timeout_ms,
        topic_data: Vec::new(),
    };
    for (key, bytes) in &sendable {
        let data = ProducePartitionData {
            index: key.1,
            records: bytes.clone(),
        };
        match request
            .topic_data
            .iter_mut()
            .find(|t| t.name.as_str() == key.0.as_ref())
        {
            Some(topic) => topic.partition_data.push(data),
            None => request.topic_data.push(ProduceTopicData {
                name: key.0.to_string(),
                topic_id: None,
                partition_data: vec![data],
            }),
        }
    }
    // KIP-516: v13+ names topics by id; fall back to v12 when one is unknown.
    if version >= 13 && !super::fill_produce_topic_ids(&mut request, &context.metadata) {
        version = 12;
    }
    let body = match super::encode_and_validate_produce_request(
        &context.client_id,
        context.max_request_size,
        version,
        &request,
    ) {
        Ok(body) => body,
        Err(error) if error.protocol_error_kind() == Some(ProtocolErrorKind::FrameTooLarge) => {
            let single = sendable.len() == 1;
            for (key, bytes) in sendable {
                results.push(BatchResult {
                    key,
                    encoded: Some(bytes),
                    outcome: if single {
                        BatchOutcome::TooLarge
                    } else {
                        BatchOutcome::NotWritten(KrafkaError::protocol_kind(
                            ProtocolErrorKind::Malformed,
                            "coalesced produce request exceeded max_request_size; resending \
                             its batches separately",
                        ))
                    },
                });
            }
            return RequestDone { broker, results };
        }
        Err(error) => {
            for (key, bytes) in sendable {
                results.push(BatchResult {
                    key,
                    encoded: Some(bytes),
                    outcome: BatchOutcome::NotWritten(error.clone()),
                });
            }
            return RequestDone { broker, results };
        }
    };

    if context.acks == 0 {
        let sent = connection
            .send_fire_and_forget(ApiKey::Produce, version, |buf| {
                buf.put_slice(&body);
                Ok(())
            })
            .await;
        for (key, bytes) in sendable {
            results.push(BatchResult {
                key,
                encoded: Some(bytes),
                outcome: match &sent {
                    Ok(()) => BatchOutcome::Unacknowledged,
                    Err(error) => BatchOutcome::NoAnswer(error.clone()),
                },
            });
        }
        return RequestDone { broker, results };
    }

    let response = connection
        .send_request(ApiKey::Produce, version, |buf| {
            buf.put_slice(&body);
            Ok(())
        })
        .await
        .and_then(|mut bytes| ProduceResponse::decode_versioned(version, &mut bytes));
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            if error.is_retriable() {
                refresh_for(&context, sendable.iter().map(|(k, _)| k)).await;
            }
            for (key, bytes) in sendable {
                results.push(BatchResult {
                    key,
                    encoded: Some(bytes),
                    outcome: BatchOutcome::NoAnswer(error.clone()),
                });
            }
            return RequestDone { broker, results };
        }
    };
    connection.notify_throttle(response.throttle_time_ms);

    let mut needs_refresh: Vec<PartitionKey> = Vec::new();
    for (key, bytes) in sendable {
        let answer = response
            .responses
            .iter()
            .find(|r| {
                if version >= 13 {
                    r.topic_id.as_ref().is_some_and(|id| {
                        context.metadata.topic_name_for_id(id).as_deref() == Some(key.0.as_ref())
                    })
                } else {
                    r.name == key.0.as_ref()
                }
            })
            .and_then(|r| r.partition_responses.iter().find(|p| p.index == key.1));
        let outcome = match answer {
            Some(p) => {
                if p.error_code.is_retriable()
                    && !super::apply_produce_leader_hint(
                        &context.metadata,
                        &key.0,
                        key.1,
                        &response,
                        p,
                    )
                {
                    needs_refresh.push(key.clone());
                }
                BatchOutcome::Answer {
                    code: p.error_code,
                    base_offset: p.base_offset,
                    log_append_time: p.log_append_time_ms,
                    log_start_offset: p.log_start_offset,
                }
            }
            None => BatchOutcome::NoAnswer(KrafkaError::protocol_kind(
                ProtocolErrorKind::Malformed,
                format!("produce response does not mention {}-{}", key.0, key.1),
            )),
        };
        results.push(BatchResult {
            key,
            encoded: Some(bytes),
            outcome,
        });
    }
    if !needs_refresh.is_empty() {
        refresh_for(&context, needs_refresh.iter()).await;
    }
    RequestDone { broker, results }
}

async fn refresh_for<'a>(context: &RequestContext, keys: impl Iterator<Item = &'a PartitionKey>) {
    let mut topics: Vec<&str> = keys.map(|k| k.0.as_ref()).collect();
    topics.sort_unstable();
    topics.dedup();
    if let Err(error) = context.metadata.force_refresh(Some(&topics)).await {
        debug!(%error, "metadata refresh after a produce failure failed");
    }
}

/// Encode one batch, on the blocking pool for the CPU-heavy codecs.
async fn encode_batch(context: &RequestContext, batch: &WireBatch) -> Result<Bytes> {
    let compression = context.compression_for(&batch.key.0);
    let level = context.compression_level;
    let transactional = context.transactional_id.is_some();
    let stamp = batch.stamp;
    let records = Arc::clone(&batch.records);
    let encoded = if matches!(compression, Compression::Gzip | Compression::Zstd) {
        tokio::task::spawn_blocking(move || {
            batch::encode(&records, stamp, transactional, compression, level)
        })
        .await
        .map_err(|e| {
            KrafkaError::compression(format!("record-batch compression task failed: {e}"))
        })??
    } else {
        batch::encode(&records, stamp, transactional, compression, level)?
    };
    if compression != Compression::None {
        context
            .metrics
            .record_compression(encoded.len() as u64, batch.estimated as u64);
    }
    Ok(encoded)
}

/// Fetch a producer id and epoch for an idempotent producer, retrying across
/// brokers until `deadline`.
pub(crate) async fn init_producer_id(
    metadata: &ClusterMetadata,
    backoff: &Backoff,
    deadline: tokio::time::Instant,
) -> Result<(i64, i16)> {
    super::retry::until_deadline(backoff, deadline, "InitProducerId", |attempt| async move {
        let brokers = metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::timeout("InitProducerId: no brokers known"));
        }
        let broker = &brokers[attempt as usize % brokers.len()];
        let connection = metadata.get_broker_connection(broker.id()).await?;
        let version = connection
            .negotiate_api_version(
                ApiKey::InitProducerId,
                versions::INIT_PRODUCER_ID_MAX,
                versions::INIT_PRODUCER_ID_MIN,
            )
            .ok_or_else(|| KrafkaError::idempotence_unavailable(None))?;
        let request = InitProducerIdRequest::idempotent();
        let mut bytes = connection
            .send_request(ApiKey::InitProducerId, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;
        let response = InitProducerIdResponse::decode_versioned(version, &mut bytes)?;
        if response.error_code.is_retriable() {
            return Err(KrafkaError::broker(
                response.error_code,
                "InitProducerId failed",
            ));
        }
        if !response.is_ok() {
            return Err(KrafkaError::idempotence_unavailable(Some(
                response.error_code,
            )));
        }
        Ok((response.producer_id, response.producer_epoch))
    })
    .await
}
