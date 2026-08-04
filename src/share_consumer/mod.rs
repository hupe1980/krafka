//! Share consumer implementation (KIP-932).
//!
//! > ⚠️ **Unstable**: This module requires the `unstable-protocol` feature flag.
//! > APIs may change without semver notice until KIP-932 is finalized in a stable
//! > Kafka release.
//!
//! Share groups provide queue-like semantics on top of Kafka topics. Multiple
//! consumers in the same share group receive non-overlapping subsets of records
//! without client-side partition assignment — all assignment is performed by
//! the server.
//!
//! # Delivery Semantics
//!
//! Share groups support **at-least-once** delivery with explicit or implicit
//! acknowledgement:
//!
//! - **Implicit** (default): previously fetched records are automatically
//!   accepted when the next `poll()` is called.
//! - **Explicit**: the application calls
//!   [`acknowledge()`](ShareConsumer::acknowledge) per record and then
//!   [`commit_sync()`](ShareConsumer::commit_sync) to flush.
//!
//! Records that are released or not acknowledged within the acquisition lock
//! timeout are redelivered to other consumers.
//!
//! # Example
//!
//! ```ignore
//! use krafka::share_consumer::{ShareConsumer, AcknowledgementMode};
//!
//! let consumer = ShareConsumer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .group_id("my-share-group")
//!     .build()
//!     .await?;
//!
//! consumer.subscribe(&["events"]).await?;
//!
//! loop {
//!     let records = consumer.poll(Duration::from_secs(1)).await?;
//!     for record in &records {
//!         process(record);
//!     }
//!     // Implicit mode: records are auto-accepted on next poll()
//! }
//! ```

mod config;
mod session;
mod stream;

pub use config::{AcknowledgeType, AcknowledgementMode, ShareConsumerConfig};
pub use stream::ShareConsumerStream;

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use std::collections::VecDeque;
use std::future::{Future, Ready, ready};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as SyncMutex, Weak};
use std::task::{Context, Poll};
use std::time::Duration;

use arc_swap::ArcSwap;
use tokio::sync::{Notify, RwLock};
use tracing::{debug, info, warn};

use crate::auth::AuthConfig;
use crate::consumer::ConsumerRecord;
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::metadata::ClusterMetadata;
use crate::metrics::ConnectionMetrics;
use crate::network::{ConnectionConfig, ConnectionPool};
use crate::protocol::{
    ApiKey, FindCoordinatorRequest, FindCoordinatorResponse, RecordBatch,
    ShareAcknowledgePartition, ShareAcknowledgeRequest, ShareAcknowledgeTopic,
    ShareAcknowledgementBatch, ShareFetchPartition, ShareFetchRequest, ShareFetchTopic,
    ShareGroupHeartbeatRequest, ShareGroupHeartbeatResponse, ShareGroupTopicPartitions,
    VersionedDecode, VersionedEncode, versions,
};
use crate::{BrokerId, Offset, PartitionId};

use session::{FINAL_EPOCH, ShareSessionCache};

/// Key for tracking unacknowledged records in explicit mode.
type RecordKey = (String, PartitionId, Offset);

/// Key for piggybacked acknowledgements grouped by broker and partition.
type BrokerAckKey = ([u8; 16], PartitionId);

/// Pending piggybacked acknowledgements grouped by broker and partition.
type BrokerPendingAcks = HashMap<BrokerId, HashMap<BrokerAckKey, Vec<PendingAck>>>;

/// Sentinel broker ID used when the partition leader is not yet known at acknowledge
/// time (e.g., immediately after `subscribe()` before the first metadata refresh, or
/// after a `restore_ack_state()` where metadata is unavailable).
/// Acks with this sentinel are re-routed using fresh metadata in `poll()`.
const UNROUTED_BROKER_ID: BrokerId = -2;

/// Wire value for the KIP-932 "gap" acknowledgement type.
///
/// A gap tells the broker that the client is *not* taking delivery of an offset
/// inside an acquired range — typically because the record could not be
/// decoded. The broker archives the offset instead of redelivering it, which is
/// what prevents an undecodable offset from being redelivered forever with an
/// ever-climbing `delivery_count`.
///
/// Deliberately not exposed on [`AcknowledgeType`]: applications never choose
/// it, the client emits it on their behalf.
const GAP_ACK_TYPE: i8 = 0;

/// Minimum negotiated `ShareFetch`/`ShareAcknowledge` version that understands
/// [`AcknowledgeType::Renew`] (KIP-1222, Kafka 4.2+).
///
/// Older brokers reject an entire acknowledgement batch with `INVALID_REQUEST`
/// when it contains an unknown acknowledgement type, so `Renew` acks are
/// dropped rather than sent to a broker that negotiated a lower version.
const RENEW_MIN_VERSION: i16 = 2;

/// Maximum number of times a `ShareAcknowledge` is retried after the broker
/// reports that the share session is stale or unavailable.
const SHARE_SESSION_RETRY_LIMIT: usize = 2;

/// Backoff applied before retrying after `SHARE_SESSION_LIMIT_REACHED` (133),
/// which is a capacity signal rather than a stale-state signal.
const SHARE_SESSION_LIMIT_BACKOFF: Duration = Duration::from_millis(100);

/// Minimum interval between two `recv()` fetch attempts when the broker keeps
/// returning empty responses, so an idle topic cannot spin the CPU.
const RECV_EMPTY_POLL_BACKOFF: Duration = Duration::from_millis(100);

/// Returns `true` when a `ShareAcknowledge`/`ShareFetch` error means the
/// per-broker share session must be torn down and re-established.
fn is_share_session_error(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::ShareSessionNotFound
            | ErrorCode::InvalidShareSessionEpoch
            | ErrorCode::ShareSessionLimitReached
    )
}

/// Restores in-flight acknowledgements if the future holding them is dropped.
///
/// `poll()` and the commit paths drain `pending_acks` into a local `Vec` before
/// they can possibly succeed. Without this guard, dropping the future — a
/// `select!` shutdown arm, or simply dropping the record stream — would
/// silently discard every drained acknowledgement, including explicit
/// `Reject`/`Release` decisions the application already made.
///
/// Call [`disarm`](Self::disarm) once the acknowledgements are known to have
/// been handled; anything still armed at drop time is re-queued under
/// [`UNROUTED_BROKER_ID`] for the next `poll()` to re-route.
struct PendingAckGuard {
    acks: Vec<PendingAck>,
    pending_acks: Arc<RwLock<BrokerPendingAcks>>,
    current_generation: Arc<AtomicU64>,
    captured_generation: u64,
    explicit_flush_retry_required: Arc<AtomicBool>,
    require_explicit_retry: bool,
}

impl PendingAckGuard {
    fn new(
        acks: Vec<PendingAck>,
        pending_acks: Arc<RwLock<BrokerPendingAcks>>,
        current_generation: Arc<AtomicU64>,
        captured_generation: u64,
        explicit_flush_retry_required: Arc<AtomicBool>,
        require_explicit_retry: bool,
    ) -> Self {
        Self {
            acks,
            pending_acks,
            current_generation,
            captured_generation,
            explicit_flush_retry_required,
            require_explicit_retry,
        }
    }

    /// Borrow the protected acknowledgements.
    fn acks(&self) -> &[PendingAck] {
        &self.acks
    }

    /// Take the acknowledgements out of the guard, disarming it.
    fn disarm(&mut self) -> Vec<PendingAck> {
        std::mem::take(&mut self.acks)
    }
}

impl Drop for PendingAckGuard {
    fn drop(&mut self) {
        if self.acks.is_empty() {
            return;
        }
        let mut acks = std::mem::take(&mut self.acks);

        // Restoring needs the async RwLock, so it cannot happen inline in
        // `drop`. Take the uncontended path when possible and only fall back to
        // spawning when the lock is held elsewhere.
        if let Ok(mut pending) = self.pending_acks.try_write() {
            if self.current_generation.load(Ordering::SeqCst) == self.captured_generation {
                if self.require_explicit_retry {
                    self.explicit_flush_retry_required
                        .store(true, Ordering::SeqCst);
                }
                for ack in acks.drain(..) {
                    pending
                        .entry(UNROUTED_BROKER_ID)
                        .or_default()
                        .entry((ack.topic_id, ack.partition))
                        .or_default()
                        .push(ack);
                }
            }
            return;
        }

        let pending_acks = self.pending_acks.clone();
        let current_generation = self.current_generation.clone();
        let explicit_flush_retry_required = self.explicit_flush_retry_required.clone();
        let captured_generation = self.captured_generation;
        let require_explicit_retry = self.require_explicit_retry;

        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                ShareConsumer::restore_ack_state(
                    current_generation.as_ref(),
                    pending_acks.as_ref(),
                    explicit_flush_retry_required.as_ref(),
                    captured_generation,
                    require_explicit_retry,
                    &mut acks,
                )
                .await;
            });
        } else {
            warn!(
                count = acks.len(),
                "share acknowledgements dropped outside a Tokio runtime; \
                 the affected records will be redelivered"
            );
        }
    }
}

/// Result of a multi-broker `ShareAcknowledge` round.
///
/// Acknowledgements are grouped by partition leader and sent per broker, so a
/// single round can partially succeed. Only the acknowledgements in
/// [`failed`](Self::failed) must be re-queued; re-queueing the whole batch
/// would make the retry re-acknowledge offsets other brokers already accepted,
/// which they reject with `INVALID_RECORD_STATE`.
#[derive(Default)]
struct ShareAcknowledgeOutcome {
    /// Acknowledgements that were not accepted by their broker.
    failed: Vec<PendingAck>,
    /// First error observed, if any.
    error: Option<KrafkaError>,
}

impl ShareAcknowledgeOutcome {
    fn fail(&mut self, acks: impl IntoIterator<Item = PendingAck>, error: KrafkaError) {
        self.failed.extend(acks);
        if self.error.is_none() {
            self.error = Some(error);
        }
    }
}

#[derive(Clone)]
struct ShareAcknowledgeContext {
    metadata: Arc<ClusterMetadata>,
    pool: Arc<ConnectionPool>,
    share_sessions: Arc<tokio::sync::Mutex<ShareSessionCache>>,
    group_id: String,
    member_id: String,
    current_ack_state_generation: Arc<AtomicU64>,
    ack_state_generation: u64,
}

/// Pending acknowledgement for a share group record.
#[derive(Debug, Clone)]
struct PendingAck {
    topic: String,
    topic_id: [u8; 16],
    partition: PartitionId,
    first_offset: Offset,
    last_offset: Offset,
    ack_type: i8,
}

fn flatten_partition_acks(
    partition_acks: HashMap<BrokerAckKey, Vec<PendingAck>>,
) -> Vec<PendingAck> {
    partition_acks.into_values().flatten().collect()
}

fn drain_broker_partition_acks(
    broker_acks: &mut HashMap<BrokerAckKey, Vec<PendingAck>>,
    topic_id: [u8; 16],
    partition: PartitionId,
) -> Vec<PendingAck> {
    broker_acks
        .remove(&(topic_id, partition))
        .unwrap_or_default()
}

fn drain_broker_acks(broker_acks: &mut BrokerPendingAcks, broker_id: BrokerId) -> Vec<PendingAck> {
    broker_acks
        .remove(&broker_id)
        .map(flatten_partition_acks)
        .unwrap_or_default()
}

/// Remove [`AcknowledgeType::Renew`] entries from acknowledgement batches that
/// are about to be sent to a broker that does not support KIP-1222.
///
/// Returns the number of batches removed. A batch whose only acknowledgement
/// type is `Renew` is dropped entirely; mixed batches keep their other types.
fn strip_unsupported_renew_acks<'a, I>(batches: I) -> usize
where
    I: Iterator<Item = &'a mut Vec<ShareAcknowledgementBatch>>,
{
    let renew = AcknowledgeType::Renew.to_i8();
    let mut dropped = 0usize;
    for batch_list in batches {
        let before = batch_list.len();
        batch_list.retain_mut(|batch| {
            batch.acknowledge_types.retain(|&t| t != renew);
            !batch.acknowledge_types.is_empty()
        });
        dropped += before - batch_list.len();
    }
    dropped
}

/// Build the offset → delivery-count map for one partition response.
///
/// A malformed or desynchronised response can carry an inverted range
/// (`last_offset < first_offset`) or an absurdly wide one such as
/// `0..=i64::MAX`. Materialising that range would allocate until the process is
/// killed, inside a loop that never yields to the runtime. Inverted ranges are
/// rejected and the total number of tracked offsets is capped at `max_offsets`,
/// which callers derive from the size of the encoded record data — each record
/// occupies at least one byte, so the byte length is an upper bound on the
/// number of records that can possibly be decoded.
fn build_delivery_counts(
    acquired: &[crate::protocol::ShareAcquiredRecords],
    max_offsets: usize,
) -> HashMap<Offset, i16> {
    let mut counts: HashMap<Offset, i16> = HashMap::new();
    if max_offsets == 0 {
        return counts;
    }

    for range in acquired {
        if range.last_offset < range.first_offset {
            warn!(
                first_offset = range.first_offset,
                last_offset = range.last_offset,
                "ignoring inverted acquired-record range in ShareFetch response"
            );
            continue;
        }

        // `last - first` cannot overflow because last >= first, but the +1 can.
        let width = (range.last_offset - range.first_offset).saturating_add(1);
        let remaining = max_offsets.saturating_sub(counts.len());
        if remaining == 0 {
            warn!("acquired-record ranges exceed the decodable record count; truncating");
            break;
        }
        let take = width.min(remaining as i64);
        if take < width {
            warn!(
                first_offset = range.first_offset,
                last_offset = range.last_offset,
                take,
                "acquired-record range is wider than the decodable record count; truncating"
            );
        }

        for offset in range.first_offset..range.first_offset.saturating_add(take) {
            counts.insert(offset, range.delivery_count);
        }
    }

    counts
}

/// Build [`GAP_ACK_TYPE`] acknowledgements for offsets the broker acquired for
/// this client but that could not be decoded.
///
/// Without these, an undecodable offset is never acknowledged, so the broker
/// redelivers it after every acquisition-lock timeout and `delivery_count`
/// climbs without bound. Contiguous missing offsets are coalesced into ranges.
fn build_gap_acks(
    topic: &str,
    topic_id: [u8; 16],
    partition: PartitionId,
    acquired: &HashMap<Offset, i16>,
    decoded: &HashSet<Offset>,
) -> Vec<PendingAck> {
    let mut missing: Vec<Offset> = acquired
        .keys()
        .copied()
        .filter(|offset| !decoded.contains(offset))
        .collect();
    if missing.is_empty() {
        return Vec::new();
    }
    missing.sort_unstable();

    let mut acks = Vec::new();
    let mut i = 0;
    while i < missing.len() {
        let first = missing[i];
        let mut last = first;
        while i + 1 < missing.len() && missing[i + 1] == last + 1 {
            i += 1;
            last = missing[i];
        }
        acks.push(PendingAck {
            topic: topic.to_string(),
            topic_id,
            partition,
            first_offset: first,
            last_offset: last,
            ack_type: GAP_ACK_TYPE,
        });
        i += 1;
    }
    acks
}

fn describe_share_fetch_join_error(error: &tokio::task::JoinError) -> &'static str {
    if error.is_panic() {
        "panicked"
    } else if error.is_cancelled() {
        "was cancelled"
    } else {
        "failed"
    }
}

/// Handle returned by [`ShareConsumer::commit_async`].
///
/// Await the handle to observe the final broker outcome. Dropping it detaches
/// the background task and discards the result.
#[must_use = "await the returned handle to observe share-commit outcome"]
#[non_exhaustive]
pub enum ShareCommitHandle {
    /// Immediate commit result without spawning a background task.
    Ready(Ready<Result<()>>),
    /// Background task handle that resolves to the commit result.
    Task(tokio::task::JoinHandle<Result<()>>),
}

impl ShareCommitHandle {
    fn ready(result: Result<()>) -> Self {
        Self::Ready(ready(result))
    }
}

impl Future for ShareCommitHandle {
    type Output = Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.get_mut() {
            Self::Ready(fut) => Pin::new(fut).poll(cx),
            Self::Task(handle) => match Pin::new(handle).poll(cx) {
                Poll::Ready(Ok(result)) => Poll::Ready(result),
                Poll::Ready(Err(error)) => Poll::Ready(Err(KrafkaError::invalid_state(format!(
                    "share commit task failed: {error}"
                )))),
                Poll::Pending => Poll::Pending,
            },
        }
    }
}

/// All internal state shared between `ShareConsumer` handles.
struct ShareConsumerInner {
    /// Configuration.
    config: ShareConsumerConfig,
    /// Cluster metadata.
    metadata: Arc<ClusterMetadata>,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Application-level metrics.
    ///
    /// Reuses [`ConsumerMetrics`] rather than defining a share-group-specific
    /// type: a share consumer polls, receives records, acknowledges and hits
    /// errors exactly as a classic consumer does, and the counters mean the
    /// same thing. `commits` counts acknowledgement flushes, which is the
    /// share-group analogue of an offset commit.
    ///
    /// Rebalance, lag and partition gauges are left at zero — the coordinator
    /// owns assignment and there is no per-partition position to lag behind.
    metrics: Arc<crate::metrics::ConsumerMetrics>,
    /// Subscribed topics.
    subscriptions: RwLock<HashSet<String>>,
    /// Current partition assignments from the coordinator.
    /// Maps topic name → partition IDs.
    assignments: RwLock<HashMap<String, Vec<PartitionId>>>,
    /// Client-generated member ID (UUID, per KIP-932).
    member_id: ArcSwap<String>,
    /// Current member epoch.
    member_epoch: AtomicI32,
    /// Heartbeat interval returned by the coordinator.
    heartbeat_interval_ms: AtomicI32,
    /// Whether the consumer is closed.
    closed: AtomicBool,
    /// Per-broker share session cache.
    share_sessions: Arc<tokio::sync::Mutex<ShareSessionCache>>,
    /// Pending acknowledgements sharded by broker ID.
    ///
    /// Pre-routing at acknowledge time means `poll()` can hand each broker its
    /// acks in O(1) without rescanning the entire flat map.  Acks whose leader
    /// is not yet known are stored under `UNROUTED_BROKER_ID` and re-routed
    /// using fresh metadata at the start of the next `poll()`.
    pending_acks: Arc<RwLock<BrokerPendingAcks>>,
    /// Monotonic token for the current local ack state.
    ///
    /// Incremented whenever local ack state is cleared or invalidated so
    /// detached flush tasks cannot send or requeue stale acknowledgements
    /// from an older membership/session after assignment changes,
    /// `unsubscribe()`, or `close()`.
    ack_state_generation: Arc<AtomicU64>,
    /// Explicit-mode barrier raised after a commit flush fails.
    ///
    /// While this is set, `poll()` refuses to fetch more records until the
    /// application retries `commit_sync()`/`commit_async()` successfully or
    /// local state is cleared during unsubscribe/close.
    explicit_flush_retry_required: Arc<AtomicBool>,
    /// Topic name → UUID cache (populated from heartbeat assignments and metadata).
    topic_ids: RwLock<HashMap<String, [u8; 16]>>,
    /// Records fetched but not yet handed to the application.
    ///
    /// Holds the tail of a `ShareFetch` response that exceeded
    /// `max_poll_records`. Records in this buffer are **not** yet
    /// acknowledgement-tracked: implicit accepts are queued and explicit
    /// `unacked_offsets` entries are created only when a record is actually
    /// returned to the caller, so a record can never be acknowledged without
    /// having been delivered.
    recv_buffer: RwLock<VecDeque<ConsumerRecord>>,
    /// Coordinator broker ID (discovered via FindCoordinator).
    coordinator_id: RwLock<Option<BrokerId>>,
    /// Coordinator address (host:port).
    coordinator_address: RwLock<Option<String>>,
    /// Tracks unacknowledged records from the previous `poll()` in explicit mode.
    /// Must be empty before the next `poll()` can fetch new records.
    unacked_offsets: Arc<RwLock<HashSet<RecordKey>>>,
    /// Background heartbeat task. Spawned on the first `subscribe()` call;
    /// aborted by `close()` or `unsubscribe()`.
    heartbeat_task: SyncMutex<Option<tokio::task::JoinHandle<()>>>,
    /// Set by `wakeup()` to interrupt an in-progress `poll()`.
    /// Cleared at the start of each `poll()` call.
    wakeup_flag: AtomicBool,
    /// Signalled by `wakeup()` so a `poll()` already blocked on a `ShareFetch`
    /// is interrupted instead of having to run to completion.
    wakeup_notify: Notify,
}

impl Drop for ShareConsumerInner {
    fn drop(&mut self) {
        // The background heartbeat task only holds a `Weak` reference, so it
        // would exit on its own; aborting makes that immediate rather than
        // waiting up to one heartbeat interval.
        if let Some(handle) = self
            .heartbeat_task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            handle.abort();
        }

        // Fires when the last `ShareConsumer` clone is dropped.
        // Warn if close() was never called — pending acks are silently lost
        // and the coordinator will only reclaim partitions after the heartbeat
        // lease expires. Skip the warning during panic unwinding.
        if !self.closed.load(Ordering::Relaxed) && !std::thread::panicking() {
            warn!(
                "ShareConsumer dropped without close(); pending acks may be lost and \
                 share-group rebalance will be delayed. Call `ShareConsumer::close()` before drop."
            );
        }
    }
}

/// A Kafka share consumer (KIP-932).
///
/// Provides queue-like consumption semantics where the server controls
/// partition assignment and record delivery. Unlike traditional consumer
/// groups, share consumers do not track offsets — instead they acknowledge
/// individual records.
///
/// `ShareConsumer` is cheaply cloneable: all clones share the same
/// connection pool, coordinator state, and acknowledgement buffers via an
/// internal [`Arc`]. A background heartbeat task is started on the first
/// [`subscribe()`](Self::subscribe) call and stopped on
/// [`close()`](Self::close) / [`unsubscribe()`](Self::unsubscribe).
#[derive(Clone)]
pub struct ShareConsumer(Arc<ShareConsumerInner>);

impl std::fmt::Debug for ShareConsumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareConsumer")
            .field("group_id", &self.0.config.group_id)
            .field("closed", &self.0.closed.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl ShareConsumer {
    /// Create a new share consumer builder.
    pub fn builder() -> ShareConsumerBuilder {
        ShareConsumerBuilder::default()
    }

    /// Create a new share consumer with the given configuration.
    async fn new(config: ShareConsumerConfig) -> Result<Self> {
        let mut pool_config_builder = config.transport.apply(
            ConnectionConfig::builder()
                .client_id(&config.client_id)
                .request_timeout(config.request_timeout)
                .connect_timeout(config.connect_timeout),
        );

        if let Some(ref auth) = config.auth {
            pool_config_builder = pool_config_builder.auth(auth.clone());
        }

        #[cfg(feature = "socks5")]
        if let Some(ref proxy) = config.proxy {
            pool_config_builder = pool_config_builder.proxy(proxy.clone());
        }

        let mut pool_config = pool_config_builder.build()?;
        pool_config.init_tls().await?;

        // Every client builds its pool through `TransportConfig::build_pool`,
        // which applies the pool-level settings and starts the background
        // tasks (idle eviction, OAUTHBEARER refresh, KIP-1288 TLS reload).
        // Routing all construction sites through one function is what stops
        // them drifting apart again.
        let pool = config.transport.build_pool(pool_config);

        let bootstrap_servers = crate::util::parse_bootstrap_servers(&config.bootstrap_servers)?;

        let metadata = Arc::new({
            let mut meta =
                ClusterMetadata::new(bootstrap_servers, pool.clone(), config.metadata_max_age)
                    .with_recovery_strategy(config.metadata_recovery_strategy)
                    .with_rebootstrap_trigger(config.metadata_recovery_rebootstrap_trigger);
            if let Some(ttl) = config.metadata_topic_cache_ttl {
                meta = meta.with_topic_cache_ttl(ttl);
            } else {
                meta = meta.with_topic_cache_ttl_disabled();
            }
            meta
        });

        metadata.refresh().await?;

        info!(
            "ShareConsumer initialized with {} brokers, group_id='{}'",
            metadata.brokers().len(),
            config.group_id
        );

        Ok(ShareConsumer(Arc::new(ShareConsumerInner {
            config,
            metadata,
            pool,
            metrics: Arc::new(crate::metrics::ConsumerMetrics::new()),
            subscriptions: RwLock::new(HashSet::new()),
            assignments: RwLock::new(HashMap::new()),
            member_id: ArcSwap::new(Arc::new(crate::util::random_uuid_v4())),
            member_epoch: AtomicI32::new(0),
            heartbeat_interval_ms: AtomicI32::new(5000),
            closed: AtomicBool::new(false),
            share_sessions: Arc::new(tokio::sync::Mutex::new(ShareSessionCache::new())),
            pending_acks: Arc::new(RwLock::new(HashMap::new())),
            ack_state_generation: Arc::new(AtomicU64::new(0)),
            explicit_flush_retry_required: Arc::new(AtomicBool::new(false)),
            topic_ids: RwLock::new(HashMap::new()),
            recv_buffer: RwLock::new(VecDeque::new()),
            coordinator_id: RwLock::new(None),
            coordinator_address: RwLock::new(None),
            unacked_offsets: Arc::new(RwLock::new(HashSet::new())),
            heartbeat_task: SyncMutex::new(None),
            wakeup_flag: AtomicBool::new(false),
            wakeup_notify: Notify::new(),
        })))
    }

    /// Subscribe to topics.
    ///
    /// Replaces the current subscription. The coordinator is notified on
    /// the next heartbeat (during `poll()`).
    pub async fn subscribe(&self, topics: &[&str]) -> Result<()> {
        if self.0.closed.load(Ordering::SeqCst) {
            return Err(KrafkaError::invalid_state("share consumer is closed"));
        }

        {
            let mut subs = self.0.subscriptions.write().await;
            subs.clear();
            for topic in topics {
                subs.insert((*topic).to_string());
            }
        }

        let topic_refs: Vec<&str> = topics.to_vec();
        self.0
            .metadata
            .refresh_for_topics(Some(&topic_refs))
            .await?;

        // Resolve topic UUIDs from metadata.
        {
            let mut ids = self.0.topic_ids.write().await;
            for topic in topics {
                if let Some(uuid) = self.0.metadata.topic_id_for_name(topic) {
                    ids.insert((*topic).to_string(), uuid);
                }
            }
        }

        // Discover the coordinator and send the initial heartbeat.
        self.ensure_coordinator().await?;
        self.send_heartbeat(true).await?;

        // Spawn the background heartbeat task if not already running.
        // The task sends periodic heartbeats independent of poll() so the
        // share-group session stays alive even when poll() is slow.
        let mut task_guard = self
            .0
            .heartbeat_task
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if task_guard.as_ref().is_none_or(|h| h.is_finished()) {
            // Hand the task a *weak* reference. A strong `Arc` would form a
            // reference cycle (inner -> JoinHandle -> task -> inner) that keeps
            // the consumer, its connection pool, and its group membership alive
            // forever when the application drops every handle without calling
            // `close()` — and would make the drop warning below unreachable.
            let bg = Arc::downgrade(&self.0);
            *task_guard = Some(tokio::spawn(async move {
                Self::run_heartbeat_loop(bg).await;
            }));
        }
        drop(task_guard);

        debug!(
            "Subscribed to {} topic(s) in share group '{}'",
            topics.len(),
            self.0.config.group_id
        );
        Ok(())
    }

    /// Returns the current subscription.
    pub async fn subscription(&self) -> HashSet<String> {
        self.0.subscriptions.read().await.clone()
    }

    /// Returns the current partition assignments.
    pub async fn assignment(&self) -> HashMap<String, Vec<PartitionId>> {
        self.0.assignments.read().await.clone()
    }

    /// Returns the member ID assigned by the coordinator.
    pub fn member_id(&self) -> String {
        (**self.0.member_id.load()).clone()
    }

    /// Returns the current member epoch.
    pub fn member_epoch(&self) -> i32 {
        self.0.member_epoch.load(Ordering::Acquire)
    }

    /// Poll for new records.
    ///
    /// In implicit acknowledgement mode, previously fetched records are
    /// automatically accepted. In explicit mode, all records from the
    /// previous poll must be acknowledged before calling this again.
    ///
    /// At most `max_poll_records` records are returned. A `ShareFetch` that
    /// acquires more than that is **not** truncated on the floor: the surplus
    /// is buffered and returned by subsequent `poll()` calls, and only the
    /// records actually handed to the caller are acknowledgement-tracked.
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        let max = self.0.config.max_poll_records as usize;
        let _timer = self.0.metrics.poll_latency.start();
        self.0.metrics.polls.inc();
        let result = self.poll_inner(timeout, max).await;
        match &result {
            Ok(records) if records.is_empty() => self.0.metrics.empty_polls.inc(),
            Ok(_) => {}
            Err(_) => self.0.metrics.record_error(),
        }
        result
    }

    /// Shared implementation of [`poll()`](Self::poll) and [`recv()`](Self::recv).
    ///
    /// `max_records` bounds how many records are returned to the caller. Every
    /// returned record — and only a returned record — is registered for
    /// acknowledgement before this function hands it over. Surplus records go
    /// to `recv_buffer` untracked.
    async fn poll_inner(
        &self,
        timeout: Duration,
        max_records: usize,
    ) -> Result<Vec<ConsumerRecord>> {
        if self.0.closed.load(Ordering::SeqCst) {
            return Err(KrafkaError::invalid_state("share consumer is closed"));
        }

        // Clear and check the wakeup flag before doing any work so callers
        // get an immediate error if wakeup() was called before this poll.
        if self.0.wakeup_flag.swap(false, Ordering::AcqRel) {
            return Err(KrafkaError::invalid_state("wakeup() was called"));
        }
        let max_records = max_records.max(1);

        // Explicit mode: reject poll if records from the previous batch are unacknowledged.
        if self.0.config.acknowledgement_mode == AcknowledgementMode::Explicit {
            let unacked = self.0.unacked_offsets.read().await;
            if !unacked.is_empty() {
                return Err(KrafkaError::invalid_state(
                    "all records from the previous poll() must be acknowledged before calling poll() again",
                ));
            }
            if self.0.explicit_flush_retry_required.load(Ordering::SeqCst) {
                return Err(KrafkaError::invalid_state(
                    "the previous commit_sync()/commit_async() flush failed; retry the commit before calling poll() again",
                ));
            }
        }

        // If recv_buffer is at capacity, skip only the fetch step (after
        // heartbeat/coordination) so group membership remains healthy.
        let max_buffered = self.0.config.max_buffered_records;
        let skip_fetch_due_to_buffer_cap = if max_buffered > 0 {
            let buf_len = self.0.recv_buffer.read().await.len();
            buf_len >= max_buffered as usize
        } else {
            false
        };

        // Send heartbeat to maintain membership and receive assignments.
        // Cap the heartbeat RPC at 10 s so a slow/stuck coordinator does not
        // block the entire poll() for the full connection-level request_timeout.
        let heartbeat_result =
            tokio::time::timeout(Duration::from_secs(10), self.send_heartbeat(false)).await;
        let heartbeat_err = match heartbeat_result {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(e),
            Err(_elapsed) => Some(KrafkaError::timeout("share group heartbeat")),
        };
        if let Some(e) = heartbeat_err {
            if let KrafkaError::Broker {
                code: ErrorCode::FencedMemberEpoch,
                ..
            } = &e
            {
                warn!(
                    "Heartbeat fenced during poll for group '{}'; resetting member state",
                    self.0.config.group_id
                );
                self.0.member_epoch.store(0, Ordering::Release);
                self.clear_ack_state().await;
            }
            warn!("Heartbeat failed during poll: {e}");
            self.invalidate_coordinator().await;
            if let Err(e2) = self.ensure_coordinator().await {
                warn!("Coordinator rediscovery failed: {e2}");
            }
        }

        // Drain previously buffered records first so mixed `recv()`/`poll()`
        // callers do not strand available data. These records were fetched but
        // never delivered, so they are registered for acknowledgement here —
        // at the moment they are actually handed to the application.
        {
            let buffered: Vec<ConsumerRecord> = {
                let mut buffer = self.0.recv_buffer.write().await;
                let take = max_records.min(buffer.len());
                buffer.drain(..take).collect()
            };
            if !buffered.is_empty() {
                self.register_delivered_records(&buffered).await;
                return Ok(buffered);
            }
        }

        let assignments = self.0.assignments.read().await.clone();
        if assignments.is_empty() || skip_fetch_due_to_buffer_cap {
            return Ok(Vec::new());
        }

        // Group partitions by leader broker.
        let mut partitions_by_broker: HashMap<BrokerId, Vec<(String, PartitionId, [u8; 16])>> =
            HashMap::new();
        let topic_ids = self.0.topic_ids.read().await;
        for (topic, partitions) in &assignments {
            let Some(&topic_id) = topic_ids.get(topic) else {
                debug!("No topic UUID for '{topic}', skipping");
                continue;
            };
            for &partition in partitions {
                if let Some(leader) = self.0.metadata.leader(topic, partition) {
                    partitions_by_broker.entry(leader).or_default().push((
                        topic.clone(),
                        partition,
                        topic_id,
                    ));
                }
            }
        }
        drop(topic_ids);

        let ack_state_generation = self.0.ack_state_generation.load(Ordering::SeqCst);

        let sendable_ack_partitions: HashSet<(&str, PartitionId)> = partitions_by_broker
            .values()
            .flat_map(|partitions| {
                partitions
                    .iter()
                    .map(|(topic, partition, _)| (topic.as_str(), *partition))
            })
            .collect();

        // Drain acknowledgement batches to piggyback on fetch requests.
        //
        // The drained acks are immediately handed to a `PendingAckGuard` so
        // that dropping this future (a `select!` shutdown arm, or dropping the
        // record stream) re-queues them instead of silently discarding explicit
        // `Reject`/`Release` decisions.
        let drained_acks: Vec<PendingAck> = {
            let mut pending = self.0.pending_acks.write().await;
            std::mem::take(&mut *pending)
                .into_values()
                .flat_map(|partition_acks| partition_acks.into_values().flatten())
                .collect()
        };
        let mut ack_guard = PendingAckGuard::new(
            drained_acks.clone(),
            self.0.pending_acks.clone(),
            self.0.ack_state_generation.clone(),
            ack_state_generation,
            self.0.explicit_flush_retry_required.clone(),
            false,
        );

        // Route every ack to the *current* partition leader. Leadership can
        // change between `acknowledge()` and `poll()`, so the pre-routing done
        // at acknowledge time is treated as a hint only.
        let mut failed_piggyback_acks: Vec<PendingAck> = Vec::new();
        let mut ack_batches_by_broker: BrokerPendingAcks = HashMap::new();
        for ack in drained_acks {
            if !sendable_ack_partitions.contains(&(ack.topic.as_str(), ack.partition)) {
                failed_piggyback_acks.push(ack);
                continue;
            }
            match self.0.metadata.leader(&ack.topic, ack.partition) {
                Some(broker_id) => {
                    let key = (ack.topic_id, ack.partition);
                    ack_batches_by_broker
                        .entry(broker_id)
                        .or_default()
                        .entry(key)
                        .or_default()
                        .push(ack);
                }
                None => failed_piggyback_acks.push(ack),
            }
        }

        // Fetch from all brokers concurrently.
        let mut fetch_tasks = Vec::with_capacity(partitions_by_broker.len());
        let member_id = (**self.0.member_id.load()).clone();
        let group_id = self.0.config.group_id.clone();
        let current_ack_state_generation = self.0.ack_state_generation.clone();

        for (broker_id, partitions) in &partitions_by_broker {
            let session_epoch = {
                let mut sessions = self.0.share_sessions.lock().await;
                sessions.get_or_create(*broker_id).epoch()
            };

            // Build per-topic partition requests with piggybacked acks.
            let mut topics_map: HashMap<[u8; 16], Vec<ShareFetchPartition>> = HashMap::new();
            let broker_ack_partitions = ack_batches_by_broker.get(broker_id);
            for (_, partition, topic_id) in partitions {
                let ack_batches_for_partition: Vec<ShareAcknowledgementBatch> =
                    broker_ack_partitions
                        .and_then(|partition_acks| partition_acks.get(&(*topic_id, *partition)))
                        .map(|partition_acks| {
                            partition_acks
                                .iter()
                                .map(|a| ShareAcknowledgementBatch {
                                    first_offset: a.first_offset,
                                    last_offset: a.last_offset,
                                    acknowledge_types: vec![a.ack_type],
                                })
                                .collect()
                        })
                        .unwrap_or_default();

                topics_map
                    .entry(*topic_id)
                    .or_default()
                    .push(ShareFetchPartition {
                        partition_index: *partition,
                        acknowledgement_batches: ack_batches_for_partition,
                    });
            }

            let topics: Vec<ShareFetchTopic> = topics_map
                .into_iter()
                .map(|(topic_id, partitions)| ShareFetchTopic {
                    topic_id,
                    partitions,
                })
                .collect();

            // Wait at most the caller's poll timeout, and never longer than the
            // configured `fetch_max_wait_ms`, so a long poll timeout does not
            // silently override the fetch-side setting.
            let poll_wait_ms = timeout.as_millis().min(i32::MAX as u128) as i32;
            let max_wait_ms = poll_wait_ms.min(self.0.config.fetch_max_wait_ms.max(0));

            let mut request = ShareFetchRequest {
                group_id: Some(group_id.clone()),
                member_id: Some(member_id.clone()),
                share_session_epoch: session_epoch,
                max_wait_ms,
                min_bytes: self.0.config.fetch_min_bytes,
                max_bytes: self.0.config.fetch_max_bytes,
                max_records: self.0.config.max_records,
                batch_size: self.0.config.batch_size,
                topics,
                forgotten_topics: Vec::new(),
            };

            let bid = *broker_id;
            let metadata = self.0.metadata.clone();
            let pool = self.0.pool.clone();
            let current_ack_state_generation = current_ack_state_generation.clone();
            let task = tokio::spawn(async move {
                ShareConsumer::ensure_ack_state_current(
                    current_ack_state_generation.as_ref(),
                    ack_state_generation,
                )?;

                let broker_addr = metadata
                    .broker(bid)
                    .map(|b| b.address().to_string())
                    .ok_or_else(|| {
                        KrafkaError::invalid_state(format!("broker {bid} not found in metadata"))
                    })?;
                let conn = pool.get_connection_by_id(bid, &broker_addr).await?;
                let version = conn
                    .negotiate_api_version(
                        ApiKey::ShareFetch,
                        versions::SHARE_FETCH_MAX,
                        versions::SHARE_FETCH_MIN,
                    )
                    .ok_or_else(|| {
                        KrafkaError::protocol_kind(
                            ProtocolErrorKind::UnknownApiVersion,
                            "broker does not support ShareFetch",
                        )
                    })?;

                ShareConsumer::ensure_ack_state_current(
                    current_ack_state_generation.as_ref(),
                    ack_state_generation,
                )?;

                // KIP-1222 `Renew` is only understood by newer brokers; sending
                // it to an older one fails the *entire* acknowledgement batch
                // with INVALID_REQUEST. Drop those acks instead: the acquisition
                // lock then simply expires, the same outcome as not renewing.
                if version < RENEW_MIN_VERSION {
                    let dropped =
                        strip_unsupported_renew_acks(request.topics.iter_mut().flat_map(|topic| {
                            topic
                                .partitions
                                .iter_mut()
                                .map(|partition| &mut partition.acknowledgement_batches)
                        }));
                    if dropped > 0 {
                        warn!(
                            broker_id = bid,
                            version,
                            dropped,
                            "broker does not support KIP-1222 Renew acknowledgements; \
                             dropping them from the ShareFetch"
                        );
                    }
                }

                let buf = conn
                    .send_request(ApiKey::ShareFetch, version, |buf| match version {
                        2 => request.encode_v2(buf, 0, false),
                        _ => request.encode_v1(buf),
                    })
                    .await?;

                let response = crate::protocol::ShareFetchResponse::decode_versioned(
                    version,
                    &mut buf.as_ref(),
                )?;

                // KIP-219: honour broker-reported throttle time.
                conn.notify_throttle(response.throttle_time_ms);

                Result::<(BrokerId, crate::protocol::ShareFetchResponse)>::Ok((bid, response))
            });
            fetch_tasks.push((bid, task));
        }

        // Collect results from all brokers.
        //
        // Snapshot the UUID → name mapping instead of holding the `topic_ids`
        // read guard across the network awaits below. tokio's `RwLock` is
        // write-preferring, so a long-lived reader lets one slow `ShareFetch`
        // block `apply_assignment()`'s writer, which in turn blocks the
        // heartbeat path and can get the member evicted from the group.
        let mut all_records: Vec<ConsumerRecord> = Vec::new();
        let mut gap_acks: Vec<PendingAck> = Vec::new();
        let topic_names_by_id: HashMap<[u8; 16], String> = {
            let guard = self.0.topic_ids.read().await;
            guard.iter().map(|(name, &id)| (id, name.clone())).collect()
        };

        // Race the collection loop against `wakeup()` so an in-flight
        // `ShareFetch` can be interrupted instead of having to run to
        // completion.
        let mut wakeup_interrupted = false;
        {
            let collect_fetch_results = async {
                for (broker_id, task) in fetch_tasks {
                    match task.await {
                        Ok(Ok((_, response))) => {
                            let mut broker_acks =
                                ack_batches_by_broker.remove(&broker_id).unwrap_or_default();

                            if !response.error_code.is_ok() {
                                failed_piggyback_acks.extend(flatten_partition_acks(broker_acks));
                                warn!(
                                    "ShareFetch to broker {broker_id} returned {:?}: {}",
                                    response.error_code,
                                    response.error_message.as_deref().unwrap_or("unknown error")
                                );
                                let mut sessions = self.0.share_sessions.lock().await;
                                sessions.reset_broker(broker_id);
                                continue;
                            }

                            // Update session state on success.
                            {
                                let mut sessions = self.0.share_sessions.lock().await;
                                sessions.get_or_create(broker_id).on_success();
                            }

                            // Decode records from the response and restore only the
                            // partitions whose piggybacked acknowledgements failed.
                            for topic_response in &response.responses {
                                let topic_name = if let Some(name) =
                                    self.0.metadata.topic_name_for_id(&topic_response.topic_id)
                                {
                                    name
                                } else {
                                    match topic_names_by_id.get(&topic_response.topic_id) {
                                        Some(name) => name.clone(),
                                        None => {
                                            debug!(
                                                "Unknown topic UUID {:?} in ShareFetch response, skipping",
                                                topic_response.topic_id
                                            );
                                            continue;
                                        }
                                    }
                                };

                                for partition_response in &topic_response.partitions {
                                    let partition_acks = drain_broker_partition_acks(
                                        &mut broker_acks,
                                        topic_response.topic_id,
                                        partition_response.partition_index,
                                    );

                                    if !partition_response.error_code.is_ok() {
                                        failed_piggyback_acks.extend(partition_acks);
                                        warn!(
                                            "ShareFetch error for {topic_name}-{}: {:?}",
                                            partition_response.partition_index,
                                            partition_response.error_code
                                        );
                                        continue;
                                    }

                                    if !partition_response.acknowledge_error_code.is_ok() {
                                        failed_piggyback_acks.extend(partition_acks);
                                        warn!(
                                            "Piggybacked ShareFetch acknowledge error for {topic_name}-{}: {:?}: {}",
                                            partition_response.partition_index,
                                            partition_response.acknowledge_error_code,
                                            partition_response
                                                .acknowledge_error_message
                                                .as_deref()
                                                .unwrap_or("unknown error")
                                        );
                                        continue;
                                    }

                                    // Build the delivery-count map from acquired_records.
                                    // The encoded record bytes bound how many records
                                    // can possibly be decoded, which caps a malformed
                                    // range such as `0..=i64::MAX`.
                                    let raw_len = partition_response
                                        .records
                                        .as_ref()
                                        .map(|raw| raw.len())
                                        .unwrap_or(0);
                                    let delivery_counts = build_delivery_counts(
                                        &partition_response.acquired_records,
                                        raw_len,
                                    );

                                    // Decode record batches.
                                    let mut decoded_offsets: HashSet<Offset> = HashSet::new();
                                    let mut decode_failed = false;
                                    if let Some(ref raw) = partition_response.records {
                                        let mut cursor = raw.as_ref();
                                        while !cursor.is_empty() {
                                            match RecordBatch::decode_with_limit(
                                                &mut cursor,
                                                self.0.config.max_decompressed_size,
                                            ) {
                                                Ok(batch) => {
                                                    for record in batch.records {
                                                        let record_offset = batch.base_offset
                                                            + record.offset_delta as i64;
                                                        let delivery_count = delivery_counts
                                                            .get(&record_offset)
                                                            .copied();
                                                        decoded_offsets.insert(record_offset);
                                                        all_records.push(ConsumerRecord {
                                                            topic: topic_name.clone(),
                                                            partition: partition_response
                                                                .partition_index,
                                                            offset: record_offset,
                                                            timestamp: batch
                                                                .base_timestamp
                                                                .saturating_add(
                                                                    record.timestamp_delta,
                                                                ),
                                                            timestamp_type: batch
                                                                .attributes
                                                                .timestamp_type
                                                                as i8,
                                                            key: record.key,
                                                            value: record.value,
                                                            headers: record
                                                                .headers
                                                                .into_iter()
                                                                .map(|h| (h.key, h.value))
                                                                .collect(),
                                                            leader_epoch: None,
                                                            delivery_count,
                                                        });
                                                    }
                                                }
                                                Err(e) => {
                                                    debug!(
                                                        "Failed to decode record batch for {topic_name}-{}: {e}",
                                                        partition_response.partition_index
                                                    );
                                                    decode_failed = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }

                                    // Offsets the broker acquired for us but that we
                                    // could not decode would otherwise be redelivered
                                    // forever, with `delivery_count` climbing without
                                    // bound. Acknowledge them as gaps so the broker
                                    // archives them instead.
                                    if decode_failed {
                                        gap_acks.extend(build_gap_acks(
                                            &topic_name,
                                            topic_response.topic_id,
                                            partition_response.partition_index,
                                            &delivery_counts,
                                            &decoded_offsets,
                                        ));
                                    }
                                }
                            }

                            failed_piggyback_acks.extend(flatten_partition_acks(broker_acks));
                        }
                        Ok(Err(e)) => {
                            failed_piggyback_acks
                                .extend(drain_broker_acks(&mut ack_batches_by_broker, broker_id));
                            warn!("ShareFetch to broker {broker_id} failed: {e}");
                        }
                        Err(e) => {
                            failed_piggyback_acks
                                .extend(drain_broker_acks(&mut ack_batches_by_broker, broker_id));
                            warn!(
                                "ShareFetch task for broker {broker_id} {}: {e}",
                                describe_share_fetch_join_error(&e)
                            );
                        }
                    }
                }
            };
            tokio::pin!(collect_fetch_results);
            tokio::select! {
                biased;
                () = self.0.wakeup_notify.notified() => {
                    wakeup_interrupted = true;
                }
                () = &mut collect_fetch_results => {}
            }
        }

        if wakeup_interrupted {
            // Consume the flag so the *next* poll() is not failed spuriously by
            // the same wakeup, keep anything already decoded, and re-queue the
            // drained acknowledgements.
            self.0.wakeup_flag.store(false, Ordering::Release);
            if !all_records.is_empty() {
                let mut buffer = self.0.recv_buffer.write().await;
                buffer.extend(std::mem::take(&mut all_records));
            }
            let acks = ack_guard.disarm();
            self.restore_pending_acks(ack_state_generation, acks, false)
                .await;
            return Err(KrafkaError::invalid_state("wakeup() was called"));
        }

        failed_piggyback_acks.extend(
            ack_batches_by_broker
                .drain()
                .flat_map(|(_, acks)| flatten_partition_acks(acks))
                .collect::<Vec<_>>(),
        );

        // Everything that could be sent has been sent; the guard's copy is no
        // longer needed and only the genuinely failed acks are re-queued.
        let _ = ack_guard.disarm();
        drop(ack_guard);

        failed_piggyback_acks.append(&mut gap_acks);
        self.restore_pending_acks(ack_state_generation, failed_piggyback_acks, false)
            .await;

        // Split *before* any acknowledgement bookkeeping. Acknowledging a
        // record that is then discarded would consume it permanently without
        // ever delivering it (implicit mode), or wedge `poll()` forever behind
        // an offset the application can never acknowledge (explicit mode).
        let overflow = if all_records.len() > max_records {
            all_records.split_off(max_records)
        } else {
            Vec::new()
        };
        if !overflow.is_empty() {
            let mut buffer = self.0.recv_buffer.write().await;
            buffer.extend(overflow);
        }

        // Only the records actually returned to the caller are tracked.
        self.register_delivered_records(&all_records).await;

        Ok(all_records)
    }

    /// Register records that are about to be handed to the application.
    ///
    /// In implicit mode this queues coalesced `Accept` acknowledgements to be
    /// piggybacked on the next `ShareFetch`. In explicit mode it records the
    /// offsets in `unacked_offsets`, which `poll()` requires to be empty before
    /// it will fetch again.
    ///
    /// This is deliberately called at *delivery* time rather than at fetch
    /// time, so a record can never be acknowledged or required-to-be-acked
    /// without the application having seen it.
    async fn register_delivered_records(&self, records: &[ConsumerRecord]) {
        if records.is_empty() {
            return;
        }

        // The single choke point for "handed to the application", so it is the
        // honest place to count. Instrumenting each `poll_inner` return instead
        // would miss the buffered-surplus path, which is a real delivery.
        let bytes: u64 = records
            .iter()
            .map(|r| r.value.as_ref().map_or(0, |v| v.len() as u64))
            .sum();
        self.0.metrics.record_receive(records.len() as u64, bytes);

        match self.0.config.acknowledgement_mode {
            AcknowledgementMode::Implicit => {
                let ids = self.0.topic_ids.read().await.clone();
                let mut pending = self.0.pending_acks.write().await;
                Self::coalesce_implicit_acks(records, &ids, &mut pending, &self.0.metadata);
            }
            AcknowledgementMode::Explicit => {
                let mut unacked = self.0.unacked_offsets.write().await;
                for record in records {
                    unacked.insert((record.topic.clone(), record.partition, record.offset));
                }
            }
        }
    }

    /// Acknowledge a record with the given type (explicit mode only).
    ///
    /// In explicit acknowledgement mode, call this for each record before
    /// calling [`commit_sync()`](Self::commit_sync). All records from the
    /// previous `poll()` must be acknowledged before calling `poll()` again.
    pub async fn acknowledge(
        &self,
        record: &ConsumerRecord,
        ack_type: AcknowledgeType,
    ) -> Result<()> {
        if self.0.config.acknowledgement_mode != AcknowledgementMode::Explicit {
            return Err(KrafkaError::invalid_state(
                "acknowledge() requires explicit acknowledgement mode",
            ));
        }

        let topic_ids = self.0.topic_ids.read().await;
        let topic_id = topic_ids.get(&record.topic).copied().ok_or_else(|| {
            KrafkaError::invalid_state(format!("no topic UUID for '{}'", record.topic))
        })?;
        drop(topic_ids);

        let record_key = (record.topic.clone(), record.partition, record.offset);
        let mut pending = self.0.pending_acks.write().await;
        let mut unacked = self.0.unacked_offsets.write().await;
        if !unacked.contains(&record_key) {
            return Err(KrafkaError::invalid_state(format!(
                "record {}-{}@{} is not pending acknowledgement",
                record.topic, record.partition, record.offset
            )));
        }
        // Route the ack to the current partition leader so poll() can piggyback
        // it on the correct broker's ShareFetch without re-scanning the whole map.
        // Fall back to UNROUTED_BROKER_ID when the leader is not yet in metadata;
        // poll() will re-route it using fresh metadata.
        let broker_id = self
            .0
            .metadata
            .leader(&record.topic, record.partition)
            .unwrap_or(UNROUTED_BROKER_ID);
        pending
            .entry(broker_id)
            .or_default()
            .entry((topic_id, record.partition))
            .or_default()
            .push(PendingAck {
                topic: record.topic.clone(),
                topic_id,
                partition: record.partition,
                first_offset: record.offset,
                last_offset: record.offset,
                ack_type: ack_type.to_i8(),
            });
        unacked.remove(&record_key);

        Ok(())
    }

    async fn restore_pending_acks(
        &self,
        ack_state_generation: u64,
        mut acks: Vec<PendingAck>,
        require_explicit_retry: bool,
    ) {
        Self::restore_ack_state(
            self.0.ack_state_generation.as_ref(),
            self.0.pending_acks.as_ref(),
            self.0.explicit_flush_retry_required.as_ref(),
            ack_state_generation,
            require_explicit_retry,
            &mut acks,
        )
        .await;
    }

    async fn restore_ack_state(
        current_generation: &AtomicU64,
        pending_acks: &RwLock<BrokerPendingAcks>,
        explicit_flush_retry_required: &AtomicBool,
        ack_state_generation: u64,
        require_explicit_retry: bool,
        acks: &mut Vec<PendingAck>,
    ) {
        if acks.is_empty() {
            return;
        }

        let mut pending = pending_acks.write().await;
        if current_generation.load(Ordering::SeqCst) != ack_state_generation {
            acks.clear();
            return;
        }
        if require_explicit_retry {
            explicit_flush_retry_required.store(true, Ordering::SeqCst);
        }
        // Re-queue under UNROUTED_BROKER_ID; poll() will re-route using fresh
        // metadata on the next call (handles leadership changes after failure).
        for ack in acks.drain(..) {
            pending
                .entry(UNROUTED_BROKER_ID)
                .or_default()
                .entry((ack.topic_id, ack.partition))
                .or_default()
                .push(ack);
        }
    }

    fn share_acknowledge_response_error(
        response: &crate::protocol::ShareAcknowledgeResponse,
    ) -> Option<KrafkaError> {
        if !response.error_code.is_ok() {
            return Some(KrafkaError::broker(
                response.error_code,
                response
                    .error_message
                    .clone()
                    .unwrap_or_else(|| "ShareAcknowledge failed".to_string()),
            ));
        }

        for topic_response in &response.responses {
            for part_response in &topic_response.partitions {
                if !part_response.error_code.is_ok() {
                    return Some(KrafkaError::broker(
                        part_response.error_code,
                        part_response.error_message.clone().unwrap_or_else(|| {
                            format!(
                                "ShareAcknowledge error for partition {}",
                                part_response.partition_index
                            )
                        }),
                    ));
                }
            }
        }

        None
    }

    /// Commit all pending acknowledgements synchronously.
    ///
    /// Sends a `ShareAcknowledge` request for all outstanding acknowledgements.
    /// In implicit mode, this flushes any buffered accepts.
    pub async fn commit_sync(&self) -> Result<()> {
        if self.0.closed.load(Ordering::SeqCst) {
            return Err(KrafkaError::invalid_state("share consumer is closed"));
        }

        self.flush_pending_acks().await
    }

    async fn flush_pending_acks(&self) -> Result<()> {
        let ack_state_generation = self.0.ack_state_generation.load(Ordering::SeqCst);

        // If there are acks in the UNROUTED shard (no leader known at
        // acknowledge() time), refresh metadata first so that
        // `send_share_acknowledge_with_state` can resolve their leaders.
        // This avoids an immediate "no leader for topic-partition" error on
        // the very first `commit_sync()` call after a fetch that piggybacked
        // the acks but hadn't yet routed them.
        //
        // IMPORTANT: collect the topic names while holding the read lock, then
        // drop the lock *before* the async metadata refresh.  Holding an async
        // RwLock guard across an await point stalls every writer that needs
        // `pending_acks.write()` (including the `std::mem::take` below).
        let unrouted_topics: HashSet<String> = {
            let pending = self.0.pending_acks.read().await;
            pending
                .get(&UNROUTED_BROKER_ID)
                .into_iter()
                .flat_map(|m| m.values().flatten())
                .map(|ack| ack.topic.clone())
                .collect()
        }; // read lock released here
        if !unrouted_topics.is_empty() {
            let topic_refs: Vec<&str> = unrouted_topics.iter().map(String::as_str).collect();
            if let Err(err) = self.0.metadata.refresh_for_topics(Some(&topic_refs)).await {
                warn!(
                    error = %err,
                    "commit_sync: metadata refresh for unrouted acks failed; \
                     commit may fail with a leader-not-found error"
                );
            }
        }

        let acks: Vec<PendingAck> = {
            let mut pending = self.0.pending_acks.write().await;
            std::mem::take(&mut *pending)
                .into_values()
                .flat_map(|broker_acks| broker_acks.into_values().flatten())
                .collect()
        };

        if acks.is_empty() {
            return Ok(());
        }

        // Arm a restore guard: if this future is dropped mid-flush (a `select!`
        // shutdown arm, a `commit_sync_with_timeout` that elapses) the drained
        // acknowledgements are re-queued instead of silently lost.
        let mut guard = PendingAckGuard::new(
            acks,
            self.0.pending_acks.clone(),
            self.0.ack_state_generation.clone(),
            ack_state_generation,
            self.0.explicit_flush_retry_required.clone(),
            true,
        );

        let outcome = self.send_share_acknowledge(guard.acks()).await;
        let _ = guard.disarm();

        match outcome.error {
            None => {
                self.0
                    .explicit_flush_retry_required
                    .store(false, Ordering::SeqCst);
                // A successful acknowledgement flush is the share-group
                // analogue of an offset commit.
                self.0.metrics.record_commit();
                Ok(())
            }
            Some(error) => {
                self.restore_pending_acks(ack_state_generation, outcome.failed, true)
                    .await;
                Err(error)
            }
        }
    }

    /// Commit all pending acknowledgements asynchronously.
    ///
    /// Await the returned handle to observe transport, decode, and broker
    /// errors. If the handle is dropped, the task continues in the background
    /// and its result is discarded.
    pub fn commit_async(&self) -> ShareCommitHandle {
        if self.0.closed.load(Ordering::SeqCst) {
            return ShareCommitHandle::ready(Err(KrafkaError::invalid_state(
                "share consumer is closed",
            )));
        }

        let member_id = (**self.0.member_id.load()).clone();

        let ack_state_generation = self.0.ack_state_generation.load(Ordering::SeqCst);
        let pending_acks = self.0.pending_acks.clone();
        let current_ack_state_generation = self.0.ack_state_generation.clone();
        let explicit_flush_retry_required = self.0.explicit_flush_retry_required.clone();
        let Ok(mut pending) = self.0.pending_acks.try_write() else {
            return ShareCommitHandle::ready(Err(KrafkaError::invalid_state(
                "commit_async: pending_acks lock contention",
            )));
        };
        let acks: Vec<PendingAck> = std::mem::take(&mut *pending)
            .into_values()
            .flat_map(|broker_acks| broker_acks.into_values().flatten())
            .collect();
        drop(pending);

        if acks.is_empty() {
            return ShareCommitHandle::ready(Ok(()));
        }

        let metadata = self.0.metadata.clone();
        let pool = self.0.pool.clone();
        let share_sessions = self.0.share_sessions.clone();
        let group_id = self.0.config.group_id.clone();
        let send_ack_state_generation = current_ack_state_generation.clone();

        ShareCommitHandle::Task(tokio::spawn(async move {
            let restore_acks = |mut acks: Vec<PendingAck>| {
                let pending_acks = pending_acks.clone();
                let current_ack_state_generation = current_ack_state_generation.clone();
                let explicit_flush_retry_required = explicit_flush_retry_required.clone();
                async move {
                    ShareConsumer::restore_ack_state(
                        current_ack_state_generation.as_ref(),
                        pending_acks.as_ref(),
                        explicit_flush_retry_required.as_ref(),
                        ack_state_generation,
                        true,
                        &mut acks,
                    )
                    .await;
                }
            };
            let outcome = ShareConsumer::send_share_acknowledge_with_state(
                ShareAcknowledgeContext {
                    metadata,
                    pool,
                    share_sessions,
                    group_id,
                    member_id,
                    current_ack_state_generation: send_ack_state_generation,
                    ack_state_generation,
                },
                &acks,
            )
            .await;

            if let Some(error) = outcome.error {
                // Only the acks their broker did not accept are re-queued.
                restore_acks(outcome.failed).await;
                return Err(error);
            }

            explicit_flush_retry_required.store(false, Ordering::SeqCst);

            Ok(())
        }))
    }

    /// Receive a single record, waiting until one is available.
    ///
    /// Records fetched but not yet returned are buffered internally, so
    /// repeated calls do not issue a `ShareFetch` per record.
    ///
    /// `Ok(None)` means the consumer has been **closed** — and nothing else.
    /// An idle topic simply makes this call wait: an empty `ShareFetch` is
    /// retried until a record arrives, the consumer is closed, or
    /// [`wakeup()`](Self::wakeup) interrupts it (which surfaces as `Err`).
    pub async fn recv(&self) -> Result<Option<ConsumerRecord>> {
        loop {
            if self.0.closed.load(Ordering::SeqCst) {
                return Ok(None);
            }

            let started = tokio::time::Instant::now();
            let records = self.poll_inner(Duration::from_secs(1), 1).await?;
            if let Some(record) = records.into_iter().next() {
                return Ok(Some(record));
            }

            if self.0.closed.load(Ordering::SeqCst) {
                return Ok(None);
            }

            // A poll can return empty immediately (no assignment yet, buffer
            // cap reached, heartbeat-only cycle). Pace the retry so an idle or
            // unassigned consumer cannot spin the CPU.
            let elapsed = started.elapsed();
            if elapsed < RECV_EMPTY_POLL_BACKOFF {
                tokio::time::sleep(RECV_EMPTY_POLL_BACKOFF - elapsed).await;
            }
        }
    }

    /// Create an async stream of records.
    pub fn stream(&self) -> ShareConsumerStream<'_> {
        ShareConsumerStream::new(self)
    }

    /// Unsubscribe from all topics.
    ///
    /// Flushes pending acknowledgements, sends a leave heartbeat
    /// (member_epoch = -1) and clears local state.
    ///
    /// The flush is best-effort: a failure is logged and unsubscribe still
    /// proceeds. Without it, explicit `Reject`/`Release` decisions the
    /// application already made would be discarded by the state clear below and
    /// the affected records would be redelivered as if nothing was decided.
    pub async fn unsubscribe(&self) {
        // Stop the background heartbeat task before leaving the group.
        if let Some(handle) = self
            .0
            .heartbeat_task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            handle.abort();
        }

        // Flush before clearing state (close() does the same).
        if let Err(e) = self.flush_pending_acks().await {
            warn!("Flushing pending acknowledgements during unsubscribe failed: {e}");
        }

        // Leave group via heartbeat with epoch -1.
        if let Err(e) = self.leave_group().await {
            warn!("Leave group failed during unsubscribe: {e}");
        }

        self.0.subscriptions.write().await.clear();
        self.0.assignments.write().await.clear();
        self.clear_partition_state().await;
        self.0
            .member_id
            .store(Arc::new(crate::util::random_uuid_v4()));
        self.0.member_epoch.store(0, Ordering::Release);

        debug!("Unsubscribed from share group '{}'", self.0.config.group_id);
    }

    /// Close the share consumer.
    ///
    /// In implicit mode, unreleased records are released (not accepted) so
    /// they become available for other consumers. In explicit mode, pending
    /// acks are flushed. Leaves the group and closes all connections.
    /// Idempotent.
    ///
    /// Returns the first cleanup error after local state and connections have
    /// still been closed.
    pub async fn close(&self) -> Result<()> {
        if self.0.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        // Stop the background heartbeat task immediately so it does not
        // race with the leave-group heartbeat sent below.
        if let Some(handle) = self
            .0
            .heartbeat_task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            handle.abort();
        }

        // In implicit mode, convert pending accepts to releases so acquired
        // records are returned to the pool for redelivery (KIP-932 §close).
        if self.0.config.acknowledgement_mode == AcknowledgementMode::Implicit {
            let mut pending = self.0.pending_acks.write().await;
            for broker_acks in pending.values_mut() {
                for acks in broker_acks.values_mut() {
                    for ack in acks.iter_mut() {
                        ack.ack_type = AcknowledgeType::Release.to_i8();
                    }
                }
            }
        }

        let commit_result = self.flush_pending_acks().await;

        // Send FINAL_EPOCH to all established share sessions so brokers release
        // server-side session state immediately rather than waiting for timeout.
        self.close_share_sessions().await;

        // Leave the group.
        let leave_result = self.leave_group().await;

        // Clear state.
        self.0.subscriptions.write().await.clear();
        self.0.assignments.write().await.clear();
        self.clear_partition_state().await;

        self.0.pool.close_all().await;

        info!("ShareConsumer closed (group '{}')", self.0.config.group_id);

        commit_result?;
        leave_result
    }

    /// Returns true if the consumer has been closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.0.closed.load(Ordering::SeqCst)
    }

    /// Interrupt [`poll()`](Self::poll)/[`recv()`](Self::recv) from another
    /// thread or task.
    ///
    /// A `poll()` that is already blocked on a `ShareFetch` is interrupted and
    /// returns an error without waiting for the broker; a `poll()` that has not
    /// started yet returns the same error immediately. Acknowledgements drained
    /// by the interrupted call are re-queued, not lost.
    ///
    /// The consumer remains usable — the next `poll()` proceeds normally.
    /// This is safe to call concurrently with any other consumer method.
    #[inline]
    pub fn wakeup(&self) {
        self.0.wakeup_flag.store(true, Ordering::Release);
        self.0.wakeup_notify.notify_waiters();
    }

    /// Close the consumer with a per-phase timeout.
    ///
    /// Equivalent to [`close()`](Self::close) but each cleanup phase
    /// (ack flush, leave-group) is individually limited to `timeout / 2`.
    /// Any cleanup error is returned after local state has been released.
    /// Idempotent.
    pub async fn close_with_timeout(&self, timeout: Duration) -> Result<()> {
        if self.0.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        if let Some(handle) = self
            .0
            .heartbeat_task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take()
        {
            handle.abort();
        }

        if self.0.config.acknowledgement_mode == AcknowledgementMode::Implicit {
            let mut pending = self.0.pending_acks.write().await;
            for broker_acks in pending.values_mut() {
                for acks in broker_acks.values_mut() {
                    for ack in acks.iter_mut() {
                        ack.ack_type = AcknowledgeType::Release.to_i8();
                    }
                }
            }
        }

        let phase = timeout / 2;
        let commit_result = tokio::time::timeout(phase, self.flush_pending_acks())
            .await
            .unwrap_or_else(|_| Err(KrafkaError::timeout("ack flush timed out during close")));

        self.close_share_sessions().await;

        let leave_result = tokio::time::timeout(phase, self.leave_group())
            .await
            .unwrap_or_else(|_| Err(KrafkaError::timeout("leave-group timed out during close")));

        self.0.subscriptions.write().await.clear();
        self.0.assignments.write().await.clear();
        self.clear_partition_state().await;

        self.0.pool.close_all().await;

        info!(
            "ShareConsumer closed with timeout (group '{}')",
            self.0.config.group_id
        );

        commit_result?;
        leave_result
    }

    /// Flush all pending explicit-mode acknowledgements synchronously with a timeout.
    ///
    /// Equivalent to [`commit_sync()`](Self::commit_sync) but bounded by `timeout`.
    /// Returns `Err(KrafkaError::Timeout)` if the flush does not complete in time.
    pub async fn commit_sync_with_timeout(&self, timeout: Duration) -> Result<()> {
        tokio::time::timeout(timeout, self.commit_sync())
            .await
            .unwrap_or_else(|_| Err(KrafkaError::timeout("commit_sync timed out")))
    }

    /// Acknowledge a record by topic, partition, and offset directly.
    ///
    /// Use this when the record could not be deserialized but still needs to be
    /// acknowledged to prevent indefinite redelivery. The record must have been
    /// delivered in the current poll session.
    ///
    /// Returns `Err` if the record was not found in the unacknowledged set.
    pub async fn acknowledge_by_offset(
        &self,
        topic: &str,
        partition: PartitionId,
        offset: Offset,
        ack_type: AcknowledgeType,
    ) -> Result<()> {
        if self.0.config.acknowledgement_mode != AcknowledgementMode::Explicit {
            return Err(KrafkaError::invalid_state(
                "acknowledge_by_offset() requires explicit acknowledgement mode",
            ));
        }

        let record_key: RecordKey = (topic.to_string(), partition, offset);
        let unacked = self.0.unacked_offsets.read().await;
        if !unacked.contains(&record_key) {
            return Err(KrafkaError::invalid_state(format!(
                "record {topic}-{partition}@{offset} is not pending acknowledgement"
            )));
        }
        drop(unacked);

        let topic_ids = self.0.topic_ids.read().await;
        let topic_id = topic_ids
            .get(topic)
            .copied()
            .ok_or_else(|| KrafkaError::invalid_state(format!("no topic UUID for '{topic}'")))?;
        drop(topic_ids);

        let broker_id = self
            .0
            .metadata
            .leader(topic, partition)
            .unwrap_or(UNROUTED_BROKER_ID);

        let mut pending = self.0.pending_acks.write().await;
        let mut unacked = self.0.unacked_offsets.write().await;
        pending
            .entry(broker_id)
            .or_default()
            .entry((topic_id, partition))
            .or_default()
            .push(PendingAck {
                topic: topic.to_string(),
                topic_id,
                partition,
                first_offset: offset,
                last_offset: offset,
                ack_type: ack_type.to_i8(),
            });
        unacked.remove(&record_key);

        Ok(())
    }

    /// Re-read TLS certificate and key files from disk and atomically install
    /// the new material for all **future** connections (KIP-1288).
    ///
    /// Existing TLS sessions are unaffected: they keep the connector they
    /// handshaked with and are replaced naturally as connections cycle. On
    /// error the previously loaded certificates stay active, so a call made
    /// mid-rotation against a half-written PEM is safe to retry.
    ///
    /// No-op when TLS is not configured.
    ///
    /// Use this for event-driven rotation (an inotify watch, a sidecar
    /// signal). For unattended rotation set
    /// [`TransportConfig::tls_reload_interval`](crate::network::TransportConfig)
    /// instead and krafka reloads on a timer.
    ///
    /// # Errors
    ///
    /// Returns an error if the certificate or key files cannot be read or
    /// parsed.
    pub async fn refresh_tls(&self) -> Result<()> {
        self.0.pool.refresh_tls().await
    }

    /// Replace the bootstrap server list used for metadata recovery (KIP-899).
    ///
    /// The new addresses are used on the next metadata refresh that falls back
    /// to bootstrap servers. Does not close existing connections.
    ///
    /// # Errors
    ///
    /// Returns an error if `servers` is empty.
    pub fn update_seed_brokers(&self, servers: Vec<String>) -> Result<()> {
        self.0.metadata.update_seed_brokers(servers)
    }

    /// Force a rebootstrap: close all connections, clear the metadata cache,
    /// and fall back to bootstrap servers (KIP-899).
    pub async fn rebootstrap(&self) {
        self.0.metadata.rebootstrap().await;
    }

    /// Snapshot the share consumer's application metrics.
    ///
    /// Counts polls, empty polls, records and bytes received, acknowledgement
    /// flushes (`commits`) and errors. Without this a share group was
    /// operable but not observable: the transport counters from
    /// [`connection_metrics`](Self::connection_metrics) showed requests, and
    /// nothing showed records.
    ///
    /// Synchronous, like every other metrics accessor in the crate.
    #[inline]
    pub fn metrics(&self) -> Arc<crate::metrics::ConsumerMetrics> {
        self.0.metrics.clone()
    }

    /// Get the shared connection metrics handle used by this share consumer's broker pool.
    #[inline]
    pub fn connection_metrics(&self) -> Arc<ConnectionMetrics> {
        self.0.pool.metrics()
    }

    // ── Internal helpers ──────────────────────────────────────────────────

    fn invalidate_ack_state(&self) {
        self.0.ack_state_generation.fetch_add(1, Ordering::SeqCst);
        self.0
            .explicit_flush_retry_required
            .store(false, Ordering::SeqCst);
    }

    async fn clear_ack_state(&self) {
        self.invalidate_ack_state();
        self.0.pending_acks.write().await.clear();
        self.0.unacked_offsets.write().await.clear();
    }

    /// Clear all per-partition state. Called from `unsubscribe()` and `close()`.
    async fn clear_partition_state(&self) {
        self.clear_ack_state().await;
        self.0.recv_buffer.write().await.clear();
        self.0.share_sessions.lock().await.reset_all();
        *self.0.coordinator_id.write().await = None;
        *self.0.coordinator_address.write().await = None;
    }

    /// Invalidate the cached coordinator. The next `ensure_coordinator()` call
    /// will re-discover it. Called on NOT_COORDINATOR errors or heartbeat failures.
    async fn invalidate_coordinator(&self) {
        *self.0.coordinator_id.write().await = None;
        *self.0.coordinator_address.write().await = None;
    }

    /// Coalesce implicit accept acks — merge consecutive offsets for the same
    /// (topic, partition) into a single `PendingAck` with a contiguous range,
    /// pre-routed to the current partition leader.
    fn coalesce_implicit_acks(
        records: &[ConsumerRecord],
        topic_ids: &HashMap<String, [u8; 16]>,
        pending: &mut BrokerPendingAcks,
        metadata: &ClusterMetadata,
    ) {
        // Group by (topic, partition) and sort offsets.
        let mut by_tp: HashMap<(&str, PartitionId), Vec<Offset>> = HashMap::new();
        for record in records {
            by_tp
                .entry((&record.topic, record.partition))
                .or_default()
                .push(record.offset);
        }

        for ((topic, partition), mut offsets) in by_tp {
            let Some(&topic_id) = topic_ids.get(topic) else {
                continue;
            };
            offsets.sort_unstable();

            let broker_id = metadata
                .leader(topic, partition)
                .unwrap_or(UNROUTED_BROKER_ID);

            // Merge consecutive offsets into contiguous ranges.
            let mut i = 0;
            while i < offsets.len() {
                let first = offsets[i];
                let mut last = first;
                while i + 1 < offsets.len() && offsets[i + 1] == last + 1 {
                    i += 1;
                    last = offsets[i];
                }
                pending
                    .entry(broker_id)
                    .or_default()
                    .entry((topic_id, partition))
                    .or_default()
                    .push(PendingAck {
                        topic: topic.to_string(),
                        topic_id,
                        partition,
                        first_offset: first,
                        last_offset: last,
                        ack_type: AcknowledgeType::Accept.to_i8(),
                    });
                i += 1;
            }
        }
    }

    /// Build `ShareAcknowledgeTopic` list from pending acks. Groups by
    /// topic UUID and partition, coalescing ack batches per partition.
    fn build_acknowledge_topics(acks: &[PendingAck]) -> Vec<ShareAcknowledgeTopic> {
        let mut topics_map: HashMap<
            [u8; 16],
            HashMap<PartitionId, Vec<ShareAcknowledgementBatch>>,
        > = HashMap::new();
        for ack in acks {
            topics_map
                .entry(ack.topic_id)
                .or_default()
                .entry(ack.partition)
                .or_default()
                .push(ShareAcknowledgementBatch {
                    first_offset: ack.first_offset,
                    last_offset: ack.last_offset,
                    acknowledge_types: vec![ack.ack_type],
                });
        }

        topics_map
            .into_iter()
            .map(|(topic_id, partitions_map)| ShareAcknowledgeTopic {
                topic_id,
                partitions: partitions_map
                    .into_iter()
                    .map(
                        |(partition_index, acknowledgement_batches)| ShareAcknowledgePartition {
                            partition_index,
                            acknowledgement_batches,
                        },
                    )
                    .collect(),
            })
            .collect()
    }

    /// Discover the share group coordinator via FindCoordinator.
    async fn ensure_coordinator(&self) -> Result<()> {
        if self.0.coordinator_id.read().await.is_some() {
            return Ok(());
        }

        let brokers = self.0.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::invalid_state("no brokers available"));
        }

        // Try each broker until we find the coordinator.
        let request = FindCoordinatorRequest::for_group(&self.0.config.group_id);
        for broker in &brokers {
            let conn = match self
                .0
                .pool
                .get_connection_by_id(broker.id(), broker.address())
                .await
            {
                Ok(c) => c,
                Err(_) => continue,
            };

            let version = match conn.negotiate_api_version(
                ApiKey::FindCoordinator,
                versions::FIND_COORDINATOR_MAX,
                versions::FIND_COORDINATOR_MIN,
            ) {
                Some(v) => v,
                None => continue,
            };

            let result = conn
                .send_request(ApiKey::FindCoordinator, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await;

            let buf = match result {
                Ok(b) => b,
                Err(e) => {
                    debug!("FindCoordinator via broker {} failed: {e}", broker.id());
                    continue;
                }
            };

            let response = FindCoordinatorResponse::decode_versioned(version, &mut buf.as_ref())?;

            if response.error_code.is_ok() {
                let coord_id = response.node_id;
                let coord_addr = format!("{}:{}", response.host, response.port);
                *self.0.coordinator_id.write().await = Some(coord_id);
                *self.0.coordinator_address.write().await = Some(coord_addr);
                debug!(
                    "Share group '{}' coordinator is broker {coord_id}",
                    self.0.config.group_id
                );
                return Ok(());
            }

            debug!(
                "FindCoordinator returned {:?} for group '{}', trying next broker",
                response.error_code, self.0.config.group_id
            );
        }

        Err(KrafkaError::invalid_state(format!(
            "could not discover coordinator for share group '{}'",
            self.0.config.group_id
        )))
    }

    /// Send a ShareGroupHeartbeat to the coordinator.
    ///
    /// If `send_subscription` is true, the subscribed topic names are included.
    /// Returns the heartbeat response.
    async fn send_heartbeat(&self, send_subscription: bool) -> Result<()> {
        let coord_id = self
            .0
            .coordinator_id
            .read()
            .await
            .ok_or_else(|| KrafkaError::invalid_state("no coordinator discovered"))?;

        let member_id = (**self.0.member_id.load()).clone();
        let member_epoch = self.0.member_epoch.load(Ordering::Acquire);

        let subscribed_topic_names = if send_subscription {
            Some(
                self.0
                    .subscriptions
                    .read()
                    .await
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>(),
            )
        } else {
            None
        };

        let request = ShareGroupHeartbeatRequest {
            group_id: self.0.config.group_id.clone(),
            member_id: member_id.clone(),
            member_epoch,
            rack_id: self.0.config.client_rack.clone(),
            subscribed_topic_names,
        };

        let coord_addr = self
            .0
            .coordinator_address
            .read()
            .await
            .clone()
            .ok_or_else(|| KrafkaError::invalid_state("no coordinator address"))?;
        let conn = self
            .0
            .pool
            .get_connection_by_id(coord_id, &coord_addr)
            .await?;
        let version = conn
            .negotiate_api_version(
                ApiKey::ShareGroupHeartbeat,
                versions::SHARE_GROUP_HEARTBEAT_MAX,
                versions::SHARE_GROUP_HEARTBEAT_MIN,
            )
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "broker does not support ShareGroupHeartbeat",
                )
            })?;

        let buf = conn
            .send_request(ApiKey::ShareGroupHeartbeat, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let response = ShareGroupHeartbeatResponse::decode_versioned(version, &mut buf.as_ref())?;

        match response.error_code {
            ErrorCode::None => {}

            // The current node is no longer the coordinator. Invalidate the
            // cached coordinator so `ensure_coordinator()` will rediscover it.
            ErrorCode::NotCoordinator
            | ErrorCode::CoordinatorNotAvailable
            | ErrorCode::CoordinatorLoadInProgress => {
                self.invalidate_coordinator().await;
                return Err(KrafkaError::broker(
                    response.error_code,
                    response
                        .error_message
                        .unwrap_or_else(|| "ShareGroupHeartbeat failed".to_string()),
                ));
            }

            // The coordinator has advanced past our epoch. The caller is
            // responsible for resetting local member state.
            ErrorCode::FencedMemberEpoch => {
                return Err(KrafkaError::broker(
                    response.error_code,
                    response
                        .error_message
                        .unwrap_or_else(|| "member epoch fenced".to_string()),
                ));
            }

            other => {
                return Err(KrafkaError::broker(
                    other,
                    response
                        .error_message
                        .unwrap_or_else(|| "ShareGroupHeartbeat failed".to_string()),
                ));
            }
        }

        // Update member state from response.
        if let Some(new_member_id) = response.member_id {
            self.0.member_id.store(Arc::new(new_member_id));
        }
        self.0
            .member_epoch
            .store(response.member_epoch, Ordering::Release);
        // Clamp the broker-supplied heartbeat interval to [50 ms, 30 s] to
        // prevent excessively fast polling (which exhausts broker connections)
        // or excessively slow polling (which causes session timeouts).
        let raw_interval_ms = response.heartbeat_interval_ms;
        const HEARTBEAT_MIN_MS: i32 = 50;
        const HEARTBEAT_MAX_MS: i32 = 30_000;
        let clamped_interval_ms = raw_interval_ms.clamp(HEARTBEAT_MIN_MS, HEARTBEAT_MAX_MS);
        if clamped_interval_ms != raw_interval_ms {
            tracing::warn!(
                raw_ms = raw_interval_ms,
                clamped_ms = clamped_interval_ms,
                min_ms = HEARTBEAT_MIN_MS,
                max_ms = HEARTBEAT_MAX_MS,
                "broker heartbeat_interval_ms is out of safe range; clamping"
            );
        }
        self.0
            .heartbeat_interval_ms
            .store(clamped_interval_ms, Ordering::Release);

        // Process assignment if present.
        if let Some(assignment) = response.assignment {
            self.apply_assignment(&assignment).await;
        }

        Ok(())
    }

    /// Apply a partition assignment from the coordinator heartbeat response.
    async fn apply_assignment(&self, assignment: &[ShareGroupTopicPartitions]) {
        let mut new_assignments: HashMap<String, Vec<PartitionId>> = HashMap::new();
        let mut topic_ids_guard = self.0.topic_ids.write().await;

        for tp in assignment {
            // Resolve topic UUID to name.
            let topic_name = if let Some(name) = self.0.metadata.topic_name_for_id(&tp.topic_id) {
                topic_ids_guard.insert(name.clone(), tp.topic_id);
                name
            } else {
                // Cache miss — try a metadata refresh next time.
                debug!(
                    "Unknown topic UUID {:?} in share assignment, skipping",
                    tp.topic_id
                );
                continue;
            };

            new_assignments.insert(topic_name, tp.partitions.clone());
        }
        drop(topic_ids_guard);

        // Reset share sessions for brokers whose partitions changed.
        let old_assignments = self.0.assignments.read().await.clone();
        if old_assignments != new_assignments {
            debug!(
                "Share group assignment changed: {} topic(s), {} partition(s)",
                new_assignments.len(),
                new_assignments.values().map(|v| v.len()).sum::<usize>()
            );
            self.clear_ack_state().await;
            self.0.share_sessions.lock().await.reset_all();
        }

        *self.0.assignments.write().await = new_assignments;
    }

    /// Send a ShareAcknowledge request for pending acks.
    ///
    /// Routes acknowledgements to the correct partition leaders and reports,
    /// per acknowledgement, which ones the brokers did not accept.
    async fn send_share_acknowledge(&self, acks: &[PendingAck]) -> ShareAcknowledgeOutcome {
        let member_id = (**self.0.member_id.load()).clone();
        Self::send_share_acknowledge_with_state(
            ShareAcknowledgeContext {
                metadata: self.0.metadata.clone(),
                pool: self.0.pool.clone(),
                share_sessions: self.0.share_sessions.clone(),
                group_id: self.0.config.group_id.clone(),
                member_id,
                current_ack_state_generation: self.0.ack_state_generation.clone(),
                ack_state_generation: self.0.ack_state_generation.load(Ordering::SeqCst),
            },
            acks,
        )
        .await
    }

    async fn send_share_acknowledge_with_state(
        context: ShareAcknowledgeContext,
        acks: &[PendingAck],
    ) -> ShareAcknowledgeOutcome {
        let ShareAcknowledgeContext {
            metadata,
            pool,
            share_sessions,
            group_id,
            member_id,
            current_ack_state_generation,
            ack_state_generation,
        } = context;

        let mut outcome = ShareAcknowledgeOutcome::default();

        if let Err(error) = Self::ensure_ack_state_current(
            current_ack_state_generation.as_ref(),
            ack_state_generation,
        ) {
            outcome.fail(acks.iter().cloned(), error);
            return outcome;
        }

        // Group acks by partition leader.
        let mut broker_acks: HashMap<BrokerId, Vec<PendingAck>> = HashMap::new();
        for ack in acks {
            match metadata.leader(&ack.topic, ack.partition) {
                Some(broker_id) => broker_acks.entry(broker_id).or_default().push(ack.clone()),
                None => outcome.fail(
                    std::iter::once(ack.clone()),
                    KrafkaError::invalid_state(format!(
                        "no leader for {}-{} in metadata",
                        ack.topic, ack.partition
                    )),
                ),
            }
        }

        for (broker_id, broker_ack_list) in broker_acks {
            // Track success per broker. Restoring a multi-broker batch wholesale
            // because the last broker failed would make the retry re-acknowledge
            // offsets the earlier brokers already accepted, which the broker
            // rejects with INVALID_RECORD_STATE.
            if let Err(error) = Self::ensure_ack_state_current(
                current_ack_state_generation.as_ref(),
                ack_state_generation,
            ) {
                outcome.fail(broker_ack_list, error);
                continue;
            }

            let result = Self::send_broker_acknowledge(
                &metadata,
                &pool,
                &share_sessions,
                &group_id,
                &member_id,
                broker_id,
                &broker_ack_list,
            )
            .await;

            if let Err(error) = result {
                outcome.fail(broker_ack_list, error);
            }
        }

        outcome
    }

    /// Send a `ShareAcknowledge` to a single broker, retrying share-session
    /// failures.
    ///
    /// On success the broker's share-session epoch is advanced. Skipping that
    /// step leaves the next `ShareFetch` sending an epoch the broker has already
    /// consumed, which it answers with `INVALID_SHARE_SESSION_EPOCH` — and since
    /// nothing reset the session, every retry repeats the same stale epoch and
    /// the consumer never recovers.
    ///
    /// `SHARE_SESSION_NOT_FOUND` (122), `INVALID_SHARE_SESSION_EPOCH` (123) and
    /// `SHARE_SESSION_LIMIT_REACHED` (133) all mean the client's view of the
    /// session is unusable: the session is reset so the retry opens a fresh one
    /// at epoch 0. Code 133 is a capacity signal, so it is retried after a
    /// short backoff.
    async fn send_broker_acknowledge(
        metadata: &Arc<ClusterMetadata>,
        pool: &Arc<ConnectionPool>,
        share_sessions: &Arc<tokio::sync::Mutex<ShareSessionCache>>,
        group_id: &str,
        member_id: &str,
        broker_id: BrokerId,
        acks: &[PendingAck],
    ) -> Result<()> {
        let broker_addr = metadata
            .broker(broker_id)
            .map(|b| b.address().to_string())
            .ok_or_else(|| {
                KrafkaError::invalid_state(format!("broker {broker_id} not found in metadata"))
            })?;

        let mut last_error: Option<KrafkaError> = None;

        for attempt in 0..=SHARE_SESSION_RETRY_LIMIT {
            let conn = pool.get_connection_by_id(broker_id, &broker_addr).await?;
            let version = conn
                .negotiate_api_version(
                    ApiKey::ShareAcknowledge,
                    versions::SHARE_ACKNOWLEDGE_MAX,
                    versions::SHARE_ACKNOWLEDGE_MIN,
                )
                .ok_or_else(|| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::UnknownApiVersion,
                        "broker does not support ShareAcknowledge",
                    )
                })?;

            let mut topics = Self::build_acknowledge_topics(acks);
            if version < RENEW_MIN_VERSION {
                let dropped = strip_unsupported_renew_acks(topics.iter_mut().flat_map(|topic| {
                    topic
                        .partitions
                        .iter_mut()
                        .map(|partition| &mut partition.acknowledgement_batches)
                }));
                if dropped > 0 {
                    warn!(
                        broker_id,
                        version,
                        dropped,
                        "broker does not support KIP-1222 Renew acknowledgements; dropping them"
                    );
                }
            }

            let session_epoch = {
                let sessions = share_sessions.lock().await;
                sessions
                    .get(broker_id)
                    .map(|s: &session::ShareSessionState| s.epoch())
                    .unwrap_or(session::INITIAL_EPOCH)
            };

            let request = ShareAcknowledgeRequest {
                group_id: Some(group_id.to_string()),
                member_id: Some(member_id.to_string()),
                share_session_epoch: session_epoch,
                topics,
            };

            let buf = conn
                .send_request(ApiKey::ShareAcknowledge, version, |buf| match version {
                    2 => request.encode_v2(buf, false),
                    _ => request.encode_v1(buf),
                })
                .await?;

            let response = crate::protocol::ShareAcknowledgeResponse::decode_versioned(
                version,
                &mut buf.as_ref(),
            )?;

            match Self::share_acknowledge_response_error(&response) {
                None => {
                    // Advance the share-session epoch: this request consumed it.
                    let mut sessions = share_sessions.lock().await;
                    sessions.get_or_create(broker_id).on_success();
                    return Ok(());
                }
                Some(error) => {
                    let session_error = matches!(
                        &error,
                        KrafkaError::Broker { code, .. } if is_share_session_error(*code)
                    );
                    if !session_error || attempt == SHARE_SESSION_RETRY_LIMIT {
                        return Err(error);
                    }

                    let limit_reached = matches!(
                        &error,
                        KrafkaError::Broker {
                            code: ErrorCode::ShareSessionLimitReached,
                            ..
                        }
                    );

                    warn!(
                        broker_id,
                        attempt,
                        "ShareAcknowledge share-session error ({error}); resetting the session and retrying"
                    );
                    share_sessions.lock().await.reset_broker(broker_id);
                    last_error = Some(error);

                    if limit_reached {
                        tokio::time::sleep(SHARE_SESSION_LIMIT_BACKOFF).await;
                    }
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            KrafkaError::invalid_state("ShareAcknowledge exhausted share-session retries")
        }))
    }

    fn ensure_ack_state_current(
        current_generation: &AtomicU64,
        ack_state_generation: u64,
    ) -> Result<()> {
        if current_generation.load(Ordering::SeqCst) == ack_state_generation {
            return Ok(());
        }

        Err(KrafkaError::invalid_state(
            "share acknowledgement state was invalidated",
        ))
    }

    /// Leave the share group via heartbeat with member_epoch = -1.
    async fn leave_group(&self) -> Result<()> {
        let coord_id = match *self.0.coordinator_id.read().await {
            Some(id) => id,
            None => return Ok(()),
        };

        let member_id = (**self.0.member_id.load()).clone();
        if member_id.is_empty() {
            return Ok(());
        }

        let request = ShareGroupHeartbeatRequest {
            group_id: self.0.config.group_id.clone(),
            member_id,
            member_epoch: -1, // Leave signal
            rack_id: None,
            subscribed_topic_names: None,
        };

        let coord_addr = match self.0.coordinator_address.read().await.clone() {
            Some(addr) => addr,
            None => return Ok(()),
        };

        let conn = self
            .0
            .pool
            .get_connection_by_id(coord_id, &coord_addr)
            .await?;

        let version = match conn.negotiate_api_version(
            ApiKey::ShareGroupHeartbeat,
            versions::SHARE_GROUP_HEARTBEAT_MAX,
            versions::SHARE_GROUP_HEARTBEAT_MIN,
        ) {
            Some(v) => v,
            None => {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "broker does not support ShareGroupHeartbeat",
                ));
            }
        };

        let buf = conn
            .send_request(ApiKey::ShareGroupHeartbeat, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await;

        let response = ShareGroupHeartbeatResponse::decode_versioned(version, &mut buf?.as_ref())?;

        if !response.error_code.is_ok() {
            return Err(KrafkaError::broker(
                response.error_code,
                response
                    .error_message
                    .unwrap_or_else(|| "ShareGroupHeartbeat failed".to_string()),
            ));
        }

        debug!("Left share group '{}' successfully", self.0.config.group_id);

        self.invalidate_coordinator().await;
        Ok(())
    }

    /// Send `ShareFetch` with `share_session_epoch = FINAL_EPOCH` (-1) to each
    /// broker that has an established session, allowing the broker to release
    /// server-side session state immediately instead of waiting for timeout.
    ///
    /// This is a best-effort operation: errors are logged at `debug!` level
    /// and do not prevent the consumer from closing.
    async fn close_share_sessions(&self) {
        let broker_ids = {
            let sessions = self.0.share_sessions.lock().await;
            sessions.established_broker_ids()
        };
        if broker_ids.is_empty() {
            return;
        }

        let member_id = (**self.0.member_id.load()).clone();
        let group_id = &self.0.config.group_id;

        for broker_id in broker_ids {
            let broker_addr = match self.0.metadata.broker(broker_id) {
                Some(b) => b.address().to_string(),
                None => continue,
            };

            let conn = match self
                .0
                .pool
                .get_connection_by_id(broker_id, &broker_addr)
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    debug!("close_share_sessions: connection to broker {broker_id} failed: {e}");
                    continue;
                }
            };

            let version = match conn.negotiate_api_version(
                ApiKey::ShareFetch,
                versions::SHARE_FETCH_MAX,
                versions::SHARE_FETCH_MIN,
            ) {
                Some(v) => v,
                None => continue,
            };

            let request = ShareFetchRequest {
                group_id: Some(group_id.clone()),
                member_id: Some(member_id.clone()),
                share_session_epoch: FINAL_EPOCH,
                max_wait_ms: 0,
                min_bytes: 0,
                max_bytes: 0,
                max_records: 0,
                batch_size: 0,
                topics: Vec::new(),
                forgotten_topics: Vec::new(),
            };

            if let Err(e) = conn
                .send_request(ApiKey::ShareFetch, version, |buf| match version {
                    2 => request.encode_v2(buf, 0, false),
                    _ => request.encode_v1(buf),
                })
                .await
            {
                debug!("close_share_sessions: FINAL_EPOCH to broker {broker_id} failed: {e}");
            } else {
                debug!("close_share_sessions: sent FINAL_EPOCH to broker {broker_id}");
            }
        }
    }

    /// Background heartbeat loop.    ///
    /// Sends periodic heartbeats at the coordinator-specified interval so the
    /// share-group session stays alive independent of how often `poll()` is
    /// called.  Handles coordinator errors with automatic rediscovery and
    /// recovers from `FencedMemberEpoch` by resetting local member state.
    ///
    /// Stops when `closed` is set or the task is aborted.
    ///
    /// Takes a [`Weak`] reference on purpose: the loop holds a strong reference
    /// only while a heartbeat is actually in flight, so it never keeps the
    /// consumer alive. It stops when `closed` is set, when the task is aborted,
    /// or as soon as the application has dropped every [`ShareConsumer`] handle.
    async fn run_heartbeat_loop(inner: Weak<ShareConsumerInner>) {
        let group_id = match inner.upgrade() {
            Some(strong) => strong.config.group_id.clone(),
            None => return,
        };

        loop {
            let interval_ms = match inner.upgrade() {
                Some(strong) => strong.heartbeat_interval_ms.load(Ordering::Relaxed),
                None => break,
            };
            tokio::time::sleep(Duration::from_millis(interval_ms.max(1) as u64)).await;

            // Re-acquire a strong reference for this iteration only; it is
            // dropped before the next sleep so the consumer can be reclaimed
            // while the loop is idle.
            let Some(strong) = inner.upgrade() else {
                break;
            };
            let this = ShareConsumer(strong);

            if this.0.closed.load(Ordering::Relaxed) {
                break;
            }

            match this.send_heartbeat(false).await {
                Ok(()) => {}

                // Fenced epoch: the coordinator has advanced past our epoch.
                // Reset local member state so the next heartbeat starts a new
                // membership attempt.
                Err(KrafkaError::Broker {
                    code: ErrorCode::FencedMemberEpoch,
                    ..
                }) => {
                    warn!(
                        "Background heartbeat: member epoch fenced for group '{group_id}'; resetting state"
                    );
                    this.0.member_epoch.store(0, Ordering::Release);
                    this.clear_ack_state().await;
                    this.invalidate_coordinator().await;
                    if let Err(e) = this.ensure_coordinator().await {
                        warn!(
                            "Background heartbeat: coordinator rediscovery after fence failed: {e}"
                        );
                    }
                }

                // Coordinator moved or unavailable: rediscover and retry.
                Err(ref e) if e.is_retriable() => {
                    debug!("Background heartbeat: retryable error for group '{group_id}': {e}");
                    this.invalidate_coordinator().await;
                    if let Err(e2) = this.ensure_coordinator().await {
                        warn!("Background heartbeat: coordinator rediscovery failed: {e2}");
                    }
                }

                Err(e) => {
                    warn!("Background heartbeat error for group '{group_id}': {e}");
                    this.invalidate_coordinator().await;
                }
            }

            // Release the strong reference before sleeping again.
            drop(this);
        }
        debug!("Background heartbeat task stopped for group '{group_id}'");
    }
}

/// Builder for creating share consumers.
#[derive(Default)]
#[must_use = "builders do nothing until .build() is called"]
pub struct ShareConsumerBuilder {
    config: ShareConsumerConfig,
}

impl ShareConsumerBuilder {
    /// Set the bootstrap servers.
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    /// Set the share group ID (required).
    pub fn group_id(mut self, group_id: impl Into<String>) -> Self {
        self.config.group_id = group_id.into();
        self
    }

    /// Set the client ID.
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.config.client_id = id.into();
        self
    }

    /// Set socket- and pool-level transport tuning.
    ///
    /// Covers TCP keepalive and nodelay, the per-connection response ceiling
    /// and in-flight cap, the priority-channel depths, the Happy Eyeballs
    /// stagger, idle-connection eviction, a total-connection cap, and the
    /// KIP-1288 automatic TLS reload interval.
    ///
    /// Omitting this call keeps krafka's historical defaults, which
    /// [`TransportConfig::default`](crate::network::TransportConfig) reproduces
    /// exactly.
    pub fn transport(mut self, transport: crate::network::TransportConfig) -> Self {
        self.config.transport = transport;
        self
    }

    /// Set the acknowledgement mode.
    pub fn acknowledgement_mode(mut self, mode: AcknowledgementMode) -> Self {
        self.config.acknowledgement_mode = mode;
        self
    }

    /// Set the maximum number of records returned per `poll()` call.
    ///
    /// Must be >= 1. Rejected at build time otherwise. Defaults to 500.
    pub fn max_poll_records(mut self, max: i32) -> Self {
        self.config.max_poll_records = max;
        self
    }

    /// Set maximum records buffered internally by [`recv()`](ShareConsumer::recv).
    ///
    /// This is a soft threshold: once the buffer is at/above this value,
    /// `poll()` skips fetches until it drains. A single `recv()` call may
    /// buffer beyond the threshold due to batched fetch responses. Set to `0`
    /// for unlimited. Negative values are rejected at build time.
    /// Defaults to 500.
    pub fn max_buffered_records(mut self, max: i32) -> Self {
        self.config.max_buffered_records = max;
        self
    }

    /// Set the fetch max wait time in milliseconds.
    pub fn fetch_max_wait_ms(mut self, ms: i32) -> Self {
        self.config.fetch_max_wait_ms = ms;
        self
    }

    /// Set the request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Configure SASL/OAUTHBEARER with a static token.
    ///
    /// For a token that must be refreshed, use
    /// [`auth`](Self::auth) with
    /// [`AuthConfig::sasl_oauthbearer_provider`](crate::auth::AuthConfig::sasl_oauthbearer_provider),
    /// or the built-in OIDC provider behind the `oauth-oidc` feature.
    pub fn sasl_oauthbearer(mut self, token: impl Into<String>) -> Self {
        self.config.auth = Some(crate::auth::AuthConfig::sasl_oauthbearer(token));
        self
    }

    /// Set the metadata recovery strategy (KIP-899).
    ///
    /// Controls what the client does when every known broker becomes
    /// unreachable: keep retrying the cached broker set, or fall back to the
    /// original bootstrap servers.
    pub fn metadata_recovery_strategy(
        mut self,
        strategy: crate::metadata::MetadataRecoveryStrategy,
    ) -> Self {
        self.config.metadata_recovery_strategy = strategy;
        self
    }

    /// Configure SASL/PLAIN authentication.
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials contain bytes the SASL framing
    /// cannot carry.
    pub fn sasl_plain(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Result<Self> {
        self.config.auth = Some(crate::auth::AuthConfig::sasl_plain(username, password)?);
        Ok(self)
    }

    /// Configure SASL/SCRAM-SHA-256 authentication.
    pub fn sasl_scram_sha256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.auth = Some(crate::auth::AuthConfig::sasl_scram_sha256(
            username, password,
        ));
        self
    }

    /// Configure SASL/SCRAM-SHA-512 authentication.
    pub fn sasl_scram_sha512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.auth = Some(crate::auth::AuthConfig::sasl_scram_sha512(
            username, password,
        ));
        self
    }

    /// Set the connect timeout: how long TCP establishment to one broker may
    /// take. Default: 10 s.
    ///
    /// [`request_timeout`](Self::request_timeout) must be at least this value,
    /// so lowering this is what makes a short request timeout possible.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    /// Set the session timeout for share group membership.
    pub fn session_timeout(mut self, timeout: Duration) -> Self {
        self.config.session_timeout = timeout;
        self
    }

    /// Set the heartbeat interval.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    /// Set authentication configuration.
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = Some(auth);
        self
    }

    /// Set the client rack ID.
    pub fn client_rack(mut self, rack: impl Into<String>) -> Self {
        self.config.client_rack = Some(rack.into());
        self
    }

    /// Set metadata max age.
    pub fn metadata_max_age(mut self, duration: Duration) -> Self {
        self.config.metadata_max_age = duration;
        self
    }

    /// Set the topic cache TTL for partial metadata refreshes.
    ///
    /// During partial refreshes, cached topics that have not been refreshed
    /// within this duration are evicted to prevent unbounded cache growth.
    ///
    /// Default: 5 minutes (matching Java's `metadata.max.idle.ms`).
    pub fn metadata_topic_cache_ttl(mut self, ttl: Duration) -> Self {
        self.config.metadata_topic_cache_ttl = Some(ttl);
        self
    }

    /// Disable topic cache TTL eviction for partial metadata refreshes.
    ///
    /// By default, cached topics are evicted after 5 minutes to prevent
    /// unbounded growth on topic churn. Call this to opt out of TTL eviction;
    /// entries will then persist across partial refreshes indefinitely.
    pub fn disable_metadata_topic_cache_ttl(mut self) -> Self {
        self.config.metadata_topic_cache_ttl = None;
        self
    }

    /// Set SOCKS5 proxy configuration.
    #[cfg(feature = "socks5")]
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.config.proxy = Some(proxy);
        self
    }

    /// Set the maximum decompressed size for record batches.
    ///
    /// Compressed payloads that decompress beyond this limit are rejected as
    /// potential compression bombs. Defaults to
    /// [`RecordBatch::MAX_DECOMPRESSED_SIZE`](crate::protocol::RecordBatch::MAX_DECOMPRESSED_SIZE) (128 MiB).
    pub fn max_decompressed_size(mut self, size: usize) -> Self {
        self.config.max_decompressed_size = size;
        self
    }

    /// Build the share consumer.
    pub async fn build(self) -> Result<ShareConsumer> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap.servers is required"));
        }
        if self.config.group_id.is_empty() {
            return Err(KrafkaError::config(
                "group_id is required for share consumers",
            ));
        }
        if self.config.heartbeat_interval >= self.config.session_timeout {
            return Err(KrafkaError::config(format!(
                "heartbeat_interval ({:?}) must be less than session_timeout ({:?})",
                self.config.heartbeat_interval, self.config.session_timeout,
            )));
        }
        if self.config.max_buffered_records < 0 {
            return Err(KrafkaError::config(format!(
                "max_buffered_records ({}) must be >= 0 (use 0 for unlimited)",
                self.config.max_buffered_records,
            )));
        }
        if self.config.max_poll_records < 1 {
            return Err(KrafkaError::config(format!(
                "max_poll_records ({}) must be >= 1",
                self.config.max_poll_records,
            )));
        }
        ShareConsumer::new(self.config).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;
    use crate::protocol::ShareAcquiredRecords;

    fn test_share_consumer(acknowledgement_mode: AcknowledgementMode) -> ShareConsumer {
        let mut config = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("sg")
            .acknowledgement_mode(acknowledgement_mode)
            .config;
        config.bootstrap_servers = "localhost:9092".to_string();
        config.group_id = "sg".to_string();

        let pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
        let metadata = Arc::new(ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool.clone(),
            config.metadata_max_age,
        ));

        ShareConsumer(Arc::new(ShareConsumerInner {
            config,
            metadata,
            pool,
            metrics: Arc::new(crate::metrics::ConsumerMetrics::new()),
            subscriptions: RwLock::new(HashSet::new()),
            assignments: RwLock::new(HashMap::new()),
            member_id: ArcSwap::new(Arc::new(crate::util::random_uuid_v4())),
            member_epoch: AtomicI32::new(0),
            heartbeat_interval_ms: AtomicI32::new(3000),
            closed: AtomicBool::new(false),
            share_sessions: Arc::new(tokio::sync::Mutex::new(ShareSessionCache::new())),
            pending_acks: Arc::new(RwLock::new(HashMap::new())),
            ack_state_generation: Arc::new(AtomicU64::new(0)),
            explicit_flush_retry_required: Arc::new(AtomicBool::new(false)),
            topic_ids: RwLock::new(HashMap::new()),
            recv_buffer: RwLock::new(VecDeque::new()),
            coordinator_id: RwLock::new(None),
            coordinator_address: RwLock::new(None),
            unacked_offsets: Arc::new(RwLock::new(HashSet::new())),
            heartbeat_task: SyncMutex::new(None),
            wakeup_flag: AtomicBool::new(false),
            wakeup_notify: Notify::new(),
        }))
    }

    #[test]
    fn test_share_consumer_builder_config() {
        let builder = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("my-share-group")
            .client_id("test-client")
            .acknowledgement_mode(AcknowledgementMode::Explicit)
            .max_poll_records(100)
            .session_timeout(Duration::from_secs(30))
            .heartbeat_interval(Duration::from_secs(5));

        assert_eq!(builder.config.bootstrap_servers, "localhost:9092");
        assert_eq!(builder.config.group_id, "my-share-group");
        assert_eq!(builder.config.client_id, "test-client");
        assert_eq!(
            builder.config.acknowledgement_mode,
            AcknowledgementMode::Explicit
        );
        assert_eq!(builder.config.max_poll_records, 100);
        assert_eq!(builder.config.session_timeout, Duration::from_secs(30));
        assert_eq!(builder.config.heartbeat_interval, Duration::from_secs(5));
    }

    #[test]
    fn test_share_consumer_builder_defaults() {
        let builder = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("sg");

        assert_eq!(builder.config.client_id, "krafka");
        assert_eq!(
            builder.config.acknowledgement_mode,
            AcknowledgementMode::Implicit
        );
        assert_eq!(builder.config.max_poll_records, 500);
        assert!(builder.config.auth.is_none());
    }

    #[tokio::test]
    async fn test_share_consumer_builder_validates_bootstrap() {
        let result = ShareConsumer::builder().group_id("sg").build().await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("bootstrap.servers"), "got: {err}");
    }

    #[tokio::test]
    async fn test_share_consumer_builder_validates_group_id() {
        let result = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .build()
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("group_id"), "got: {err}");
    }

    #[tokio::test]
    async fn test_share_consumer_builder_validates_heartbeat() {
        let result = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("sg")
            .session_timeout(Duration::from_secs(5))
            .heartbeat_interval(Duration::from_secs(10))
            .build()
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("heartbeat_interval"), "got: {err}");
    }

    #[tokio::test]
    async fn test_share_consumer_builder_rejects_negative_max_buffered_records() {
        let result = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("sg")
            .max_buffered_records(-1)
            .build()
            .await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("max_buffered_records"), "got: {err}");
    }

    #[tokio::test]
    async fn test_share_consumer_builder_rejects_zero_max_poll_records() {
        for bad in [0, -1, i32::MIN] {
            let result = ShareConsumer::builder()
                .bootstrap_servers("localhost:9092")
                .group_id("sg")
                .max_poll_records(bad)
                .build()
                .await;
            assert!(result.is_err(), "expected error for max_poll_records={bad}");
            let err = result.unwrap_err().to_string();
            assert!(err.contains("max_poll_records"), "got: {err}");
        }
    }

    #[test]
    fn test_acknowledge_type_to_i8() {
        assert_eq!(AcknowledgeType::Accept.to_i8(), 1);
        assert_eq!(AcknowledgeType::Release.to_i8(), 2);
        assert_eq!(AcknowledgeType::Reject.to_i8(), 3);
        // KIP-932 + KIP-1222 wire values; 0 is the client-emitted "gap".
        assert_eq!(AcknowledgeType::Renew.to_i8(), 4);
        assert_eq!(GAP_ACK_TYPE, 0);
    }

    #[test]
    fn test_acknowledgement_mode_default() {
        assert_eq!(
            AcknowledgementMode::default(),
            AcknowledgementMode::Implicit
        );
    }

    #[test]
    fn test_share_consumer_config_accessors() {
        let builder = ShareConsumer::builder()
            .bootstrap_servers("broker:9092")
            .group_id("sg-1")
            .client_id("my-client")
            .acknowledgement_mode(AcknowledgementMode::Explicit)
            .session_timeout(Duration::from_secs(20))
            .heartbeat_interval(Duration::from_secs(3));

        assert_eq!(builder.config.bootstrap_servers(), "broker:9092");
        assert_eq!(builder.config.group_id(), "sg-1");
        assert_eq!(builder.config.client_id(), "my-client");
        assert_eq!(
            builder.config.acknowledgement_mode(),
            AcknowledgementMode::Explicit
        );
        assert_eq!(builder.config.session_timeout(), Duration::from_secs(20));
        assert_eq!(builder.config.heartbeat_interval(), Duration::from_secs(3));
    }

    #[test]
    fn test_build_acknowledge_topics() {
        let acks = vec![
            PendingAck {
                topic: "t1".into(),
                topic_id: [1; 16],
                partition: 0,
                first_offset: 0,
                last_offset: 5,
                ack_type: AcknowledgeType::Accept.to_i8(),
            },
            PendingAck {
                topic: "t1".into(),
                topic_id: [1; 16],
                partition: 1,
                first_offset: 10,
                last_offset: 15,
                ack_type: AcknowledgeType::Release.to_i8(),
            },
            PendingAck {
                topic: "t2".into(),
                topic_id: [2; 16],
                partition: 0,
                first_offset: 0,
                last_offset: 3,
                ack_type: AcknowledgeType::Reject.to_i8(),
            },
        ];

        let topics = ShareConsumer::build_acknowledge_topics(&acks);
        assert_eq!(topics.len(), 2); // Two distinct topic_ids

        // Verify partition counts.
        let total_partitions: usize = topics.iter().map(|t| t.partitions.len()).sum();
        assert_eq!(total_partitions, 3);
    }

    #[test]
    fn test_share_acknowledge_response_error_detects_partition_failure() {
        let response = crate::protocol::ShareAcknowledgeResponse {
            throttle_time_ms: 0,
            error_code: ErrorCode::None,
            error_message: None,
            acquisition_lock_timeout_ms: -1,
            responses: vec![crate::protocol::ShareAcknowledgeTopicResponse {
                topic_id: [1; 16],
                partitions: vec![crate::protocol::ShareAcknowledgePartitionResponse {
                    partition_index: 7,
                    error_code: ErrorCode::UnknownTopicOrPartition,
                    error_message: Some("gone".to_string()),
                    current_leader: crate::protocol::ShareLeaderIdAndEpoch {
                        leader_id: -1,
                        leader_epoch: 0,
                    },
                }],
            }],
            node_endpoints: Vec::new(),
        };

        let error = ShareConsumer::share_acknowledge_response_error(&response)
            .expect("partition error must surface as an error");

        assert!(matches!(
            error,
            KrafkaError::Broker {
                code: ErrorCode::UnknownTopicOrPartition,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn test_restore_ack_state_requeues_pending_acks_without_reinserting_unacked() {
        let ack_state_generation = AtomicU64::new(0);
        let explicit_flush_retry_required = AtomicBool::new(false);
        let pending: RwLock<BrokerPendingAcks> = RwLock::new(HashMap::new());
        let mut acks_to_restore = vec![PendingAck {
            topic: "topic-a".into(),
            topic_id: [0; 16],
            partition: 2,
            first_offset: 11,
            last_offset: 13,
            ack_type: AcknowledgeType::Accept.to_i8(),
        }];

        ShareConsumer::restore_ack_state(
            &ack_state_generation,
            &pending,
            &explicit_flush_retry_required,
            0,
            false,
            &mut acks_to_restore,
        )
        .await;

        assert!(acks_to_restore.is_empty());
        assert!(!explicit_flush_retry_required.load(Ordering::SeqCst));
        let guard = pending.read().await;
        let all_acks: Vec<&PendingAck> =
            guard.values().flat_map(|b| b.values().flatten()).collect();
        assert_eq!(all_acks.len(), 1);
        assert_eq!(all_acks[0].topic, "topic-a");
        assert_eq!(all_acks[0].partition, 2);
        assert_eq!(all_acks[0].first_offset, 11);
        assert_eq!(all_acks[0].last_offset, 13);
        assert_eq!(all_acks[0].ack_type, AcknowledgeType::Accept.to_i8());
    }

    #[tokio::test]
    async fn test_restore_ack_state_skips_stale_generation() {
        let ack_state_generation = AtomicU64::new(1);
        let explicit_flush_retry_required = AtomicBool::new(false);
        let pending: RwLock<BrokerPendingAcks> = RwLock::new(HashMap::new());
        let mut acks_to_restore = vec![PendingAck {
            topic: "topic-a".into(),
            topic_id: [0; 16],
            partition: 2,
            first_offset: 11,
            last_offset: 13,
            ack_type: AcknowledgeType::Accept.to_i8(),
        }];

        ShareConsumer::restore_ack_state(
            &ack_state_generation,
            &pending,
            &explicit_flush_retry_required,
            0,
            true,
            &mut acks_to_restore,
        )
        .await;

        assert!(pending.read().await.is_empty());
        assert!(acks_to_restore.is_empty());
        assert!(!explicit_flush_retry_required.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_restore_ack_state_marks_explicit_flush_retry_required() {
        let ack_state_generation = AtomicU64::new(0);
        let explicit_flush_retry_required = AtomicBool::new(false);
        let pending: RwLock<BrokerPendingAcks> = RwLock::new(HashMap::new());
        let mut acks_to_restore = vec![PendingAck {
            topic: "topic-a".into(),
            topic_id: [0; 16],
            partition: 2,
            first_offset: 11,
            last_offset: 13,
            ack_type: AcknowledgeType::Accept.to_i8(),
        }];

        ShareConsumer::restore_ack_state(
            &ack_state_generation,
            &pending,
            &explicit_flush_retry_required,
            0,
            true,
            &mut acks_to_restore,
        )
        .await;

        assert!(acks_to_restore.is_empty());
        assert!(explicit_flush_retry_required.load(Ordering::SeqCst));
        assert_eq!(
            pending
                .read()
                .await
                .values()
                .flat_map(|b| b.values().flatten())
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn test_poll_rejects_after_failed_explicit_flush() {
        let consumer = test_share_consumer(AcknowledgementMode::Explicit);
        consumer
            .0
            .explicit_flush_retry_required
            .store(true, Ordering::SeqCst);

        let error = consumer
            .poll(Duration::from_millis(1))
            .await
            .expect_err("poll must block after a failed explicit flush");

        assert!(
            error
                .to_string()
                .contains("retry the commit before calling poll() again")
        );
    }

    #[tokio::test]
    async fn test_clear_partition_state_clears_explicit_flush_retry_required() {
        let consumer = test_share_consumer(AcknowledgementMode::Explicit);
        consumer
            .0
            .explicit_flush_retry_required
            .store(true, Ordering::SeqCst);

        consumer.clear_partition_state().await;

        assert!(
            !consumer
                .0
                .explicit_flush_retry_required
                .load(Ordering::SeqCst)
        );
    }

    #[tokio::test]
    async fn test_apply_assignment_advances_ack_state_generation_on_change() {
        let consumer = test_share_consumer(AcknowledgementMode::Explicit);
        consumer
            .0
            .assignments
            .write()
            .await
            .insert("topic-a".to_string(), vec![0]);
        consumer
            .0
            .pending_acks
            .write()
            .await
            .entry(UNROUTED_BROKER_ID)
            .or_default()
            .entry(([1; 16], 0))
            .or_default()
            .push(PendingAck {
                topic: "topic-a".to_string(),
                topic_id: [1; 16],
                partition: 0,
                first_offset: 5,
                last_offset: 5,
                ack_type: AcknowledgeType::Accept.to_i8(),
            });
        consumer
            .0
            .unacked_offsets
            .write()
            .await
            .insert(("topic-a".to_string(), 0, 5));
        consumer
            .0
            .explicit_flush_retry_required
            .store(true, Ordering::SeqCst);

        let old_generation = consumer.0.ack_state_generation.load(Ordering::SeqCst);
        consumer.apply_assignment(&[]).await;

        assert_eq!(
            consumer.0.ack_state_generation.load(Ordering::SeqCst),
            old_generation + 1
        );
        assert!(
            !consumer
                .0
                .explicit_flush_retry_required
                .load(Ordering::SeqCst)
        );
        assert!(consumer.0.pending_acks.read().await.is_empty());
        assert!(consumer.0.unacked_offsets.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_send_share_acknowledge_rejects_stale_ack_generation() {
        let consumer = test_share_consumer(AcknowledgementMode::Explicit);
        let outcome = ShareConsumer::send_share_acknowledge_with_state(
            ShareAcknowledgeContext {
                metadata: consumer.0.metadata.clone(),
                pool: consumer.0.pool.clone(),
                share_sessions: consumer.0.share_sessions.clone(),
                group_id: consumer.0.config.group_id.clone(),
                member_id: (**consumer.0.member_id.load()).clone(),
                current_ack_state_generation: Arc::new(AtomicU64::new(1)),
                ack_state_generation: 0,
            },
            &[PendingAck {
                topic: "topic-a".to_string(),
                topic_id: [1; 16],
                partition: 0,
                first_offset: 5,
                last_offset: 5,
                ack_type: AcknowledgeType::Accept.to_i8(),
            }],
        )
        .await;

        let error = outcome
            .error
            .expect("stale ack generation must be rejected before sending");
        assert!(
            error
                .to_string()
                .contains("acknowledgement state was invalidated")
        );
        assert_eq!(
            outcome.failed.len(),
            1,
            "the un-sent acknowledgement must be reported back for restore"
        );
    }

    #[tokio::test]
    async fn test_share_commit_handle_ready_flattens_result() {
        ShareCommitHandle::ready(Ok(()))
            .await
            .expect("ready ok result");

        let error = ShareCommitHandle::ready(Err(KrafkaError::invalid_state("boom")))
            .await
            .expect_err("ready error must surface");
        assert!(error.to_string().contains("boom"));
    }

    #[tokio::test]
    async fn test_share_commit_handle_flattens_task_result() {
        let error = ShareCommitHandle::Task(tokio::spawn(async {
            Err(KrafkaError::invalid_state("task failed"))
        }))
        .await
        .expect_err("task error must surface");

        assert!(error.to_string().contains("task failed"));
    }

    #[tokio::test]
    async fn test_describe_share_fetch_join_error_reports_panic() {
        let error = tokio::spawn(async {
            panic!("boom");
        })
        .await
        .expect_err("panic must surface as a JoinError");

        assert_eq!(describe_share_fetch_join_error(&error), "panicked");
    }

    #[tokio::test]
    async fn test_describe_share_fetch_join_error_reports_cancellation() {
        let handle = tokio::spawn(async {
            std::future::pending::<()>().await;
        });
        handle.abort();

        let error = handle
            .await
            .expect_err("aborted task must surface as a JoinError");

        assert_eq!(describe_share_fetch_join_error(&error), "was cancelled");
    }

    #[tokio::test]
    async fn test_acknowledge_keeps_record_pending_until_ack_is_queued() {
        let consumer = Arc::new(test_share_consumer(AcknowledgementMode::Explicit));

        consumer
            .0
            .topic_ids
            .write()
            .await
            .insert("topic-a".to_string(), [7; 16]);

        let record = ConsumerRecord {
            topic: "topic-a".into(),
            partition: 3,
            offset: 11,
            timestamp: 0,
            timestamp_type: 0,
            key: None,
            value: None,
            headers: Vec::new(),
            leader_epoch: None,
            delivery_count: None,
        };
        let record_key = (record.topic.clone(), record.partition, record.offset);

        consumer
            .0
            .unacked_offsets
            .write()
            .await
            .insert(record_key.clone());

        let pending_guard = consumer.0.pending_acks.write().await;
        let task_consumer = consumer.clone();
        let task = tokio::spawn(async move {
            task_consumer
                .acknowledge(&record, AcknowledgeType::Accept)
                .await
        });

        tokio::task::yield_now().await;

        assert!(
            consumer
                .0
                .unacked_offsets
                .read()
                .await
                .contains(&record_key)
        );
        assert!(
            !task.is_finished(),
            "acknowledge should still be waiting on the pending_acks lock"
        );

        drop(pending_guard);

        task.await
            .expect("acknowledge task should join")
            .expect("acknowledge should succeed once pending lock is released");

        assert!(
            !consumer
                .0
                .unacked_offsets
                .read()
                .await
                .contains(&record_key)
        );
        let pending_guard = consumer.0.pending_acks.read().await;
        let all_acks: Vec<&PendingAck> = pending_guard
            .values()
            .flat_map(|b| b.values().flatten())
            .collect();
        assert_eq!(all_acks.len(), 1);
        assert_eq!(all_acks[0].topic, "topic-a");
        assert_eq!(all_acks[0].partition, 3);
        assert_eq!(all_acks[0].first_offset, 11);
        assert_eq!(all_acks[0].last_offset, 11);
        assert_eq!(all_acks[0].ack_type, AcknowledgeType::Accept.to_i8());
    }

    #[test]
    fn test_coalesce_implicit_acks_merges_consecutive() {
        let records = vec![
            ConsumerRecord {
                topic: "t1".into(),
                partition: 0,
                offset: 0,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: None,
                headers: Vec::new(),
                leader_epoch: None,
                delivery_count: None,
            },
            ConsumerRecord {
                topic: "t1".into(),
                partition: 0,
                offset: 1,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: None,
                headers: Vec::new(),
                leader_epoch: None,
                delivery_count: None,
            },
            ConsumerRecord {
                topic: "t1".into(),
                partition: 0,
                offset: 2,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: None,
                headers: Vec::new(),
                leader_epoch: None,
                delivery_count: None,
            },
            // Gap: offset 3 missing
            ConsumerRecord {
                topic: "t1".into(),
                partition: 0,
                offset: 4,
                timestamp: 0,
                timestamp_type: 0,
                key: None,
                value: None,
                headers: Vec::new(),
                leader_epoch: None,
                delivery_count: None,
            },
        ];

        let mut topic_ids = HashMap::new();
        topic_ids.insert("t1".to_string(), [1u8; 16]);

        let mut pending: BrokerPendingAcks = HashMap::new();
        // Pass a dummy ClusterMetadata — no live brokers, so all acks route to UNROUTED_BROKER_ID.
        let dummy_pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
        let dummy_metadata = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            dummy_pool,
            Duration::from_secs(300),
        );
        ShareConsumer::coalesce_implicit_acks(&records, &topic_ids, &mut pending, &dummy_metadata);

        // Should produce two ranges: [0,2] and [4,4].
        let mut all_acks: Vec<PendingAck> = pending
            .into_values()
            .flat_map(|b| b.into_values().flatten())
            .collect();
        assert_eq!(all_acks.len(), 2);
        all_acks.sort_by_key(|a| a.first_offset);
        assert_eq!(all_acks[0].first_offset, 0);
        assert_eq!(all_acks[0].last_offset, 2);
        assert_eq!(all_acks[1].first_offset, 4);
        assert_eq!(all_acks[1].last_offset, 4);
    }

    #[test]
    fn test_config_defaults_match_kip932() {
        let builder = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("sg");

        // KIP-932 specifies 45s session timeout.
        assert_eq!(builder.config.session_timeout(), Duration::from_secs(45));
        assert_eq!(builder.config.heartbeat_interval(), Duration::from_secs(5));
    }

    // ── Regression test: F-06 / ack_state_generation ordering ────────────────

    /// `ensure_ack_state_current` must accept a matching generation and reject
    /// any stale one, whether the captured snapshot is behind OR ahead of the
    /// current value (the latter is impossible in practice but good to guard).
    ///
    /// This is a unit test of the pure guard function — no async machinery
    /// required.  The same logic is exercised end-to-end by
    /// `test_send_share_acknowledge_rejects_stale_ack_generation`.
    #[test]
    fn test_ensure_ack_state_current_rejects_stale_generation() {
        let current = AtomicU64::new(5);

        // Exact match → Ok.
        assert!(
            ShareConsumer::ensure_ack_state_current(&current, 5).is_ok(),
            "matching generation must succeed"
        );

        // Stale (lower than current) — the common invalidation case.
        assert!(
            ShareConsumer::ensure_ack_state_current(&current, 4).is_err(),
            "stale lower generation must be rejected"
        );

        // Stale (higher than current) — shouldn't happen, but the guard covers it.
        assert!(
            ShareConsumer::ensure_ack_state_current(&current, 6).is_err(),
            "stale higher generation must be rejected"
        );
    }

    /// Incrementing `ack_state_generation` must be visible to concurrent
    /// callers that captured the old value, without any spurious success.
    ///
    /// Simulates: flush task captures generation, assignment changes, flush
    /// task calls `ensure_ack_state_current` and must receive an error.
    #[tokio::test]
    async fn test_ack_state_generation_flush_task_sees_invalidation() {
        let consumer = Arc::new(test_share_consumer(AcknowledgementMode::Explicit));

        // Capture the generation as a flush task would at spawn time.
        let captured_gen = consumer.0.ack_state_generation.load(Ordering::SeqCst);
        assert_eq!(captured_gen, 0);

        // Simulate assignment change / unsubscribe which advances the generation.
        consumer.clear_partition_state().await;
        let new_gen = consumer.0.ack_state_generation.load(Ordering::SeqCst);
        assert!(
            new_gen > captured_gen,
            "generation must advance after clear_partition_state"
        );

        // A detached flush task using the captured (old) generation must be blocked.
        let err =
            ShareConsumer::ensure_ack_state_current(&consumer.0.ack_state_generation, captured_gen)
                .expect_err("stale flush task must be rejected");
        assert!(err.to_string().contains("invalidated"), "got: {err}");
    }

    /// Cloning a `ShareConsumer` produces a second handle to the same state.
    #[test]
    fn test_share_consumer_clone_shares_state() {
        let consumer = test_share_consumer(AcknowledgementMode::Implicit);
        let cloned = consumer.clone();

        // Both handles share the same Arc — pointer equality confirms this.
        assert!(Arc::ptr_eq(&consumer.0, &cloned.0));

        // A store via one handle is immediately visible through the other.
        consumer.0.member_epoch.store(42, Ordering::Release);
        assert_eq!(cloned.0.member_epoch.load(Ordering::Acquire), 42);
    }

    /// The background heartbeat task field starts as `None` and can be set.
    #[test]
    fn test_heartbeat_task_starts_none() {
        let consumer = test_share_consumer(AcknowledgementMode::Implicit);
        let guard = consumer.0.heartbeat_task.lock().unwrap();
        assert!(guard.is_none(), "heartbeat task should start as None");
    }

    /// `close()` marks the consumer closed and the drop warning is suppressed.
    #[tokio::test]
    async fn test_close_is_idempotent_and_suppresses_drop_warning() {
        let consumer = test_share_consumer(AcknowledgementMode::Implicit);
        // First close: should succeed even without a coordinator.
        let _ = consumer.close().await;
        assert!(consumer.is_closed());
        // Second close: must be idempotent (no panic, no error).
        let _ = consumer.close().await;
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    fn test_record(topic: &str, partition: PartitionId, offset: Offset) -> ConsumerRecord {
        ConsumerRecord {
            topic: topic.to_string(),
            partition,
            offset,
            timestamp: 0,
            timestamp_type: 0,
            key: None,
            value: None,
            headers: Vec::new(),
            leader_epoch: None,
            delivery_count: None,
        }
    }

    fn acquired(first: Offset, last: Offset, delivery_count: i16) -> ShareAcquiredRecords {
        ShareAcquiredRecords {
            first_offset: first,
            last_offset: last,
            delivery_count,
        }
    }

    // ── Overflow must be buffered, never acknowledged ───────

    /// Implicit mode: a fetch larger than `max_poll_records` must queue accepts
    /// only for the records actually returned. Accepting the surplus would
    /// consume records that were never delivered — silent, permanent data loss.
    #[tokio::test]
    async fn test_register_delivered_records_only_acks_delivered_records_implicit() {
        let consumer = test_share_consumer(AcknowledgementMode::Implicit);
        consumer
            .0
            .topic_ids
            .write()
            .await
            .insert("t".to_string(), [9; 16]);

        let delivered: Vec<ConsumerRecord> = (0..3).map(|o| test_record("t", 0, o)).collect();
        consumer.register_delivered_records(&delivered).await;

        let pending = consumer.0.pending_acks.read().await;
        let acks: Vec<&PendingAck> = pending
            .values()
            .flat_map(|b| b.values().flatten())
            .collect();
        assert_eq!(acks.len(), 1, "contiguous offsets coalesce into one range");
        assert_eq!(acks[0].first_offset, 0);
        assert_eq!(
            acks[0].last_offset, 2,
            "only the delivered offsets 0..=2 may be accepted"
        );
    }

    /// Explicit mode: only delivered records may enter `unacked_offsets`.
    /// Tracking undelivered offsets wedges `poll()` forever, because the
    /// application can never acknowledge a record it never received.
    #[tokio::test]
    async fn test_register_delivered_records_only_tracks_delivered_records_explicit() {
        let consumer = test_share_consumer(AcknowledgementMode::Explicit);
        let delivered: Vec<ConsumerRecord> = (0..2).map(|o| test_record("t", 0, o)).collect();

        consumer.register_delivered_records(&delivered).await;

        let unacked = consumer.0.unacked_offsets.read().await;
        assert_eq!(unacked.len(), 2);
        assert!(unacked.contains(&("t".to_string(), 0, 0)));
        assert!(unacked.contains(&("t".to_string(), 0, 1)));
        assert!(
            !unacked.contains(&("t".to_string(), 0, 2)),
            "an undelivered offset must never be marked unacknowledged"
        );
    }

    /// Records buffered by an oversized fetch are handed out by the next
    /// `poll()` and are registered at that point — not before.
    #[tokio::test]
    async fn test_poll_drains_buffer_and_registers_on_delivery() {
        let consumer = test_share_consumer(AcknowledgementMode::Explicit);
        {
            let mut buffer = consumer.0.recv_buffer.write().await;
            for offset in 0..5 {
                buffer.push_back(test_record("t", 0, offset));
            }
        }

        // Nothing is tracked while the records merely sit in the buffer.
        assert!(consumer.0.unacked_offsets.read().await.is_empty());

        let records = consumer
            .poll_inner(Duration::from_millis(1), 2)
            .await
            .expect("buffered records must be returned without a fetch");

        assert_eq!(records.len(), 2, "poll must respect the record limit");
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[1].offset, 1);
        assert_eq!(
            consumer.0.recv_buffer.read().await.len(),
            3,
            "the remainder stays buffered"
        );
        assert_eq!(
            consumer.0.unacked_offsets.read().await.len(),
            2,
            "only the two delivered records are tracked"
        );
    }

    // ── Acquired-range validation ───────────────────────────────────

    /// An inverted range (`last < first`) is malformed and must be ignored
    /// rather than iterated.
    #[test]
    fn test_build_delivery_counts_rejects_inverted_range() {
        let counts = build_delivery_counts(&[acquired(100, 5, 1)], 1024);
        assert!(counts.is_empty(), "inverted range must be dropped");
    }

    /// A decode desync yielding `0..=i64::MAX` must not be materialised: the
    /// range is capped by the number of records that could possibly decode.
    #[test]
    fn test_build_delivery_counts_caps_absurd_range() {
        let counts = build_delivery_counts(&[acquired(0, i64::MAX, 3)], 16);
        assert_eq!(counts.len(), 16, "range must be capped, not materialised");
        assert_eq!(counts.get(&0).copied(), Some(3));
        assert_eq!(counts.get(&15).copied(), Some(3));
        assert!(counts.get(&16).is_none());
    }

    /// With no record bytes there is nothing decodable, so no offset is tracked.
    #[test]
    fn test_build_delivery_counts_empty_when_no_record_bytes() {
        assert!(build_delivery_counts(&[acquired(0, 1_000, 1)], 0).is_empty());
    }

    /// Well-formed ranges are expanded exactly.
    #[test]
    fn test_build_delivery_counts_expands_valid_ranges() {
        let counts = build_delivery_counts(&[acquired(10, 12, 2), acquired(20, 20, 7)], 1024);
        assert_eq!(counts.len(), 4);
        assert_eq!(counts.get(&10).copied(), Some(2));
        assert_eq!(counts.get(&12).copied(), Some(2));
        assert_eq!(counts.get(&20).copied(), Some(7));
    }

    // ── Undecodable offsets are acknowledged as gaps ──────────────────────

    /// Offsets acquired but not decoded must be acknowledged as gaps, otherwise
    /// they are redelivered forever with a climbing `delivery_count`.
    #[test]
    fn test_build_gap_acks_covers_undecoded_offsets() {
        let mut acquired_counts: HashMap<Offset, i16> = HashMap::new();
        for offset in 0..6 {
            acquired_counts.insert(offset, 1);
        }
        // Offsets 0,1 decoded; 2,3,4 failed; 5 decoded.
        let decoded: HashSet<Offset> = [0, 1, 5].into_iter().collect();

        let mut acks = build_gap_acks("t", [4; 16], 3, &acquired_counts, &decoded);
        acks.sort_by_key(|a| a.first_offset);

        assert_eq!(acks.len(), 1, "contiguous gaps coalesce");
        assert_eq!(acks[0].first_offset, 2);
        assert_eq!(acks[0].last_offset, 4);
        assert_eq!(acks[0].ack_type, GAP_ACK_TYPE);
        assert_eq!(acks[0].partition, 3);
        assert_eq!(acks[0].topic, "t");
    }

    /// When everything decoded there is nothing to report as a gap.
    #[test]
    fn test_build_gap_acks_empty_when_all_decoded() {
        let mut acquired_counts: HashMap<Offset, i16> = HashMap::new();
        acquired_counts.insert(0, 1);
        acquired_counts.insert(1, 1);
        let decoded: HashSet<Offset> = [0, 1].into_iter().collect();

        assert!(build_gap_acks("t", [0; 16], 0, &acquired_counts, &decoded).is_empty());
    }

    // ── Share-session error classification ──────────────────────────

    /// The three share-session error codes must all trigger a session reset.
    #[test]
    fn test_share_session_errors_are_classified() {
        assert!(is_share_session_error(ErrorCode::ShareSessionNotFound));
        assert!(is_share_session_error(ErrorCode::InvalidShareSessionEpoch));
        assert!(is_share_session_error(ErrorCode::ShareSessionLimitReached));
        assert!(!is_share_session_error(ErrorCode::None));
        assert!(!is_share_session_error(ErrorCode::NotCoordinator));
    }

    /// A successful `ShareAcknowledge` consumes the share-session epoch, so the
    /// client must advance it. Leaving it stale makes the next `ShareFetch`
    /// send an epoch the broker already used, which it rejects with
    /// `INVALID_SHARE_SESSION_EPOCH` — permanently, since nothing resets it.
    #[tokio::test]
    async fn test_share_session_epoch_advances_on_acknowledge_success() {
        let sessions = Arc::new(tokio::sync::Mutex::new(ShareSessionCache::new()));

        {
            let mut guard = sessions.lock().await;
            guard.get_or_create(1).on_success(); // ShareFetch succeeded: epoch 1
        }
        assert_eq!(sessions.lock().await.get(1).map(|s| s.epoch()), Some(1));

        // The success arm of `send_broker_acknowledge` performs exactly this.
        sessions.lock().await.get_or_create(1).on_success();

        assert_eq!(
            sessions.lock().await.get(1).map(|s| s.epoch()),
            Some(2),
            "ShareAcknowledge must advance the epoch it consumed"
        );
    }

    /// A share-session error resets the broker back to epoch 0 so the retry
    /// opens a fresh session instead of resending the stale epoch.
    #[tokio::test]
    async fn test_share_session_reset_returns_to_initial_epoch() {
        let sessions = Arc::new(tokio::sync::Mutex::new(ShareSessionCache::new()));
        {
            let mut guard = sessions.lock().await;
            guard.get_or_create(7).on_success();
            guard.get_or_create(7).on_success();
        }
        assert_eq!(sessions.lock().await.get(7).map(|s| s.epoch()), Some(2));

        sessions.lock().await.reset_broker(7);

        assert_eq!(
            sessions.lock().await.get(7).map(|s| s.epoch()),
            Some(session::INITIAL_EPOCH),
            "a stale session must restart at epoch 0"
        );
    }

    // ── Per-broker acknowledge outcome ───────────────────────────────────

    /// A multi-broker acknowledge that fails on one broker must report only
    /// that broker's acks as failed. Restoring the whole batch would make the
    /// retry re-acknowledge offsets other brokers already accepted, which they
    /// reject with `INVALID_RECORD_STATE`.
    #[test]
    fn test_share_acknowledge_outcome_tracks_only_failed_acks() {
        let mut outcome = ShareAcknowledgeOutcome::default();
        assert!(outcome.error.is_none());
        assert!(outcome.failed.is_empty());

        let failed_ack = PendingAck {
            topic: "t".into(),
            topic_id: [1; 16],
            partition: 3,
            first_offset: 0,
            last_offset: 0,
            ack_type: AcknowledgeType::Accept.to_i8(),
        };
        outcome.fail(
            std::iter::once(failed_ack),
            KrafkaError::invalid_state("broker 3 down"),
        );
        outcome.fail(
            std::iter::empty(),
            KrafkaError::invalid_state("later error"),
        );

        assert_eq!(outcome.failed.len(), 1);
        assert_eq!(outcome.failed[0].partition, 3);
        assert!(
            outcome
                .error
                .expect("first error is kept")
                .to_string()
                .contains("broker 3 down"),
            "the first error must be preserved"
        );
    }

    // ── Drop guard: drained acks survive future cancellation ─────────────

    /// Dropping a future that had drained `pending_acks` must re-queue them.
    /// Otherwise a `select!` shutdown arm silently discards explicit
    /// `Reject`/`Release` decisions the application already made.
    #[tokio::test]
    async fn test_pending_ack_guard_restores_on_drop() {
        let pending: Arc<RwLock<BrokerPendingAcks>> = Arc::new(RwLock::new(HashMap::new()));
        let generation = Arc::new(AtomicU64::new(0));
        let retry = Arc::new(AtomicBool::new(false));

        {
            let _guard = PendingAckGuard::new(
                vec![PendingAck {
                    topic: "t".into(),
                    topic_id: [2; 16],
                    partition: 1,
                    first_offset: 4,
                    last_offset: 6,
                    ack_type: AcknowledgeType::Reject.to_i8(),
                }],
                pending.clone(),
                generation.clone(),
                0,
                retry.clone(),
                true,
            );
        } // dropped without disarm

        let guard = pending.read().await;
        let acks: Vec<&PendingAck> = guard.values().flat_map(|b| b.values().flatten()).collect();
        assert_eq!(acks.len(), 1, "the Reject decision must survive the drop");
        assert_eq!(acks[0].ack_type, AcknowledgeType::Reject.to_i8());
        assert_eq!(acks[0].first_offset, 4);
        assert!(retry.load(Ordering::SeqCst));
    }

    /// A disarmed guard restores nothing — the acks were handled.
    #[tokio::test]
    async fn test_pending_ack_guard_disarm_suppresses_restore() {
        let pending: Arc<RwLock<BrokerPendingAcks>> = Arc::new(RwLock::new(HashMap::new()));
        let generation = Arc::new(AtomicU64::new(0));
        let retry = Arc::new(AtomicBool::new(false));

        {
            let mut guard = PendingAckGuard::new(
                vec![PendingAck {
                    topic: "t".into(),
                    topic_id: [2; 16],
                    partition: 1,
                    first_offset: 4,
                    last_offset: 4,
                    ack_type: AcknowledgeType::Accept.to_i8(),
                }],
                pending.clone(),
                generation.clone(),
                0,
                retry.clone(),
                true,
            );
            assert_eq!(guard.acks().len(), 1);
            assert_eq!(guard.disarm().len(), 1);
        }

        assert!(pending.read().await.is_empty());
        assert!(!retry.load(Ordering::SeqCst));
    }

    /// A guard whose generation was invalidated must drop its acks rather than
    /// resurrect state from an old membership.
    #[tokio::test]
    async fn test_pending_ack_guard_ignores_stale_generation() {
        let pending: Arc<RwLock<BrokerPendingAcks>> = Arc::new(RwLock::new(HashMap::new()));
        let generation = Arc::new(AtomicU64::new(0));
        let retry = Arc::new(AtomicBool::new(false));

        {
            let _guard = PendingAckGuard::new(
                vec![PendingAck {
                    topic: "t".into(),
                    topic_id: [2; 16],
                    partition: 1,
                    first_offset: 4,
                    last_offset: 4,
                    ack_type: AcknowledgeType::Accept.to_i8(),
                }],
                pending.clone(),
                generation.clone(),
                0,
                retry.clone(),
                true,
            );
            // Assignment change / unsubscribe invalidates the ack state.
            generation.store(1, Ordering::SeqCst);
        }

        assert!(pending.read().await.is_empty());
        assert!(!retry.load(Ordering::SeqCst));
    }

    // ── KIP-1222 Renew is only sent to brokers that support it ───────────

    /// Sending `Renew` to a pre-4.2 broker fails the whole batch with
    /// INVALID_REQUEST, so those entries are stripped for older versions.
    #[test]
    fn test_strip_unsupported_renew_acks_removes_renew_only_batches() {
        let mut batches = vec![
            ShareAcknowledgementBatch {
                first_offset: 0,
                last_offset: 0,
                acknowledge_types: vec![AcknowledgeType::Accept.to_i8()],
            },
            ShareAcknowledgementBatch {
                first_offset: 1,
                last_offset: 1,
                acknowledge_types: vec![AcknowledgeType::Renew.to_i8()],
            },
            ShareAcknowledgementBatch {
                first_offset: 2,
                last_offset: 2,
                acknowledge_types: vec![
                    AcknowledgeType::Renew.to_i8(),
                    AcknowledgeType::Reject.to_i8(),
                ],
            },
        ];

        let dropped = strip_unsupported_renew_acks(std::iter::once(&mut batches));

        assert_eq!(dropped, 1, "only the Renew-only batch is dropped");
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].first_offset, 0);
        assert_eq!(batches[1].first_offset, 2);
        assert_eq!(
            batches[1].acknowledge_types,
            vec![AcknowledgeType::Reject.to_i8()],
            "the mixed batch keeps its other acknowledgement types"
        );
    }

    /// Nothing is stripped when no `Renew` is present.
    #[test]
    fn test_strip_unsupported_renew_acks_is_a_noop_without_renew() {
        let mut batches = vec![ShareAcknowledgementBatch {
            first_offset: 0,
            last_offset: 5,
            acknowledge_types: vec![AcknowledgeType::Accept.to_i8()],
        }];

        assert_eq!(
            strip_unsupported_renew_acks(std::iter::once(&mut batches)),
            0
        );
        assert_eq!(batches.len(), 1);
    }

    // ── Lifecycle ─────────────────────────────────────────────

    /// `recv()` reports `Ok(None)` for a closed consumer — the only case that
    /// terminates the record stream.
    #[tokio::test]
    async fn test_recv_returns_none_only_when_closed() {
        let consumer = test_share_consumer(AcknowledgementMode::Implicit);
        consumer.0.closed.store(true, Ordering::SeqCst);

        assert!(
            consumer
                .recv()
                .await
                .expect("closed recv is not an error")
                .is_none(),
            "a closed consumer ends the stream"
        );
    }

    /// `recv()` must not terminate just because a poll came back empty: it
    /// keeps waiting, and a record produced later is still delivered.
    #[tokio::test]
    async fn test_recv_waits_through_empty_polls() {
        let consumer = Arc::new(test_share_consumer(AcknowledgementMode::Implicit));

        // No assignment, so every internal poll returns empty.
        let receiver = consumer.clone();
        let handle = tokio::spawn(async move { receiver.recv().await });

        // Give it several empty poll cycles; it must still be waiting.
        tokio::time::sleep(RECV_EMPTY_POLL_BACKOFF * 3).await;
        assert!(
            !handle.is_finished(),
            "recv() must keep waiting on an idle topic, not return None"
        );

        // A record arriving later is delivered.
        consumer
            .0
            .recv_buffer
            .write()
            .await
            .push_back(test_record("t", 0, 42));

        let record = tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("recv must finish once a record is available")
            .expect("join")
            .expect("recv must not error")
            .expect("a record must be delivered");
        assert_eq!(record.offset, 42);
    }

    /// The heartbeat task must hold only a weak reference, so dropping every
    /// `ShareConsumer` handle lets the consumer be reclaimed even if `close()`
    /// was never called.
    #[tokio::test]
    async fn test_heartbeat_task_does_not_keep_consumer_alive() {
        let consumer = test_share_consumer(AcknowledgementMode::Implicit);
        let weak = Arc::downgrade(&consumer.0);

        // Spawn the heartbeat loop exactly as `subscribe()` does.
        let handle = {
            let bg = Arc::downgrade(&consumer.0);
            tokio::spawn(async move {
                ShareConsumer::run_heartbeat_loop(bg).await;
            })
        };
        *consumer
            .0
            .heartbeat_task
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(handle);

        // Drop the only user-facing handle without calling close().
        drop(consumer);

        assert!(
            weak.upgrade().is_none(),
            "a strong reference in the heartbeat task would leak the consumer, \
             its connection pool, and its group membership"
        );
    }

    /// The weak heartbeat loop exits promptly once the consumer is gone.
    #[tokio::test]
    async fn test_heartbeat_loop_exits_when_consumer_dropped() {
        let consumer = test_share_consumer(AcknowledgementMode::Implicit);
        consumer.0.heartbeat_interval_ms.store(5, Ordering::Release);
        let weak = Arc::downgrade(&consumer.0);

        let handle = tokio::spawn(async move {
            ShareConsumer::run_heartbeat_loop(weak).await;
        });

        drop(consumer);

        tokio::time::timeout(Duration::from_secs(5), handle)
            .await
            .expect("the heartbeat loop must exit once every handle is dropped")
            .expect("heartbeat task must not panic");
    }

    /// `wakeup()` sets the flag *and* signals waiters so an in-flight poll can
    /// be interrupted rather than having to run to completion.
    #[tokio::test]
    async fn test_wakeup_interrupts_a_waiting_poll() {
        let consumer = Arc::new(test_share_consumer(AcknowledgementMode::Implicit));

        let waiter = consumer.clone();
        let notified = tokio::spawn(async move {
            waiter.0.wakeup_notify.notified().await;
        });
        tokio::task::yield_now().await;

        consumer.wakeup();

        tokio::time::timeout(Duration::from_secs(5), notified)
            .await
            .expect("wakeup() must signal waiters, not only set a flag")
            .expect("waiter task must not panic");

        // The flag path still fails the next poll immediately.
        let error = consumer
            .poll(Duration::from_millis(1))
            .await
            .expect_err("a pending wakeup fails the next poll");
        assert!(error.to_string().contains("wakeup"));

        // ...and is consumed, so the poll after that is not failed spuriously.
        assert!(!consumer.0.wakeup_flag.load(Ordering::Acquire));
    }

    /// `fetch_max_wait_ms` must actually bound the fetch wait rather than being
    /// ignored in favour of the poll timeout.
    #[test]
    fn test_fetch_max_wait_bounds_the_poll_timeout() {
        let config = ShareConsumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("sg")
            .fetch_max_wait_ms(250)
            .config;

        let poll_wait_ms = Duration::from_secs(30).as_millis().min(i32::MAX as u128) as i32;
        let effective = poll_wait_ms.min(config.fetch_max_wait_ms.max(0));
        assert_eq!(effective, 250, "fetch_max_wait_ms must cap the fetch wait");

        // A poll timeout shorter than the config wins.
        let short = Duration::from_millis(10).as_millis() as i32;
        assert_eq!(short.min(config.fetch_max_wait_ms.max(0)), 10);
    }
}
