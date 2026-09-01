//! An in-process fake Kafka broker for deterministic client tests.
//!
//! `FakeBroker` binds a real TCP listener on `127.0.0.1:0` and speaks the
//! real Kafka wire protocol, so a real [`Producer`](crate::producer::Producer),
//! [`Consumer`](crate::consumer::Consumer) or
//! [`AdminClient`](crate::admin::AdminClient) connects to it exactly as it
//! would to a broker. What it adds over a real broker is *control*: a test can
//! inject a specific error on a specific API, delay a specific response, move a
//! partition leader or a group coordinator, and then assert on what the client
//! did about it.
//!
//! That closes a gap that Docker-based integration tests cannot: reproducing a
//! `NOT_CONTROLLER` at exactly the right moment, or a response that arrives
//! after the client gave up on it, is a matter of timing luck against a real
//! cluster and a single line here.
//!
//! # Example
//!
//! Drive the control hook to make `CreateTopics` fail once with
//! `NOT_CONTROLLER`, and assert the admin client re-resolved the controller and
//! retried rather than surfacing the error:
//!
//! ```rust,no_run
//! use std::time::Duration;
//!
//! use krafka::admin::{AdminClient, NewTopic};
//! use krafka::error::ErrorCode;
//! use krafka::protocol::ApiKey;
//! use krafka::testing::{Control, FakeBroker};
//!
//! # async fn example() -> Result<(), krafka::error::KrafkaError> {
//! let broker = FakeBroker::start().await?;
//!
//! // The first CreateTopics is rejected; everything after it is served normally.
//! broker.on_once(ApiKey::CreateTopics, |_req| {
//!     Control::Error(ErrorCode::NotController)
//! });
//!
//! let admin = AdminClient::builder()
//!     .bootstrap_servers(broker.bootstrap_servers())
//!     .build()
//!     .await?;
//!
//! admin
//!     .create_topics(
//!         vec![NewTopic::new("orders", 3, 1)?],
//!         Duration::from_secs(15),
//!         false,
//!     )
//!     .await?;
//!
//! // Two attempts, with a metadata refresh in between to re-resolve the controller.
//! assert_eq!(broker.request_count(ApiKey::CreateTopics), 2);
//! assert!(broker.request_count(ApiKey::Metadata) >= 1);
//! # Ok(())
//! # }
//! ```
//!
//! # What the defaults cover
//!
//! `ApiVersions`, `Metadata`, `FindCoordinator`, `Produce`, `Fetch`,
//! `ListOffsets`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`,
//! `OffsetCommit`, `OffsetFetch`, `CreateTopics` and `DeleteTopics`, plus the
//! full transaction protocol — `InitProducerId` with KIP-360 fencing,
//! `AddPartitionsToTxn`, `AddOffsetsToTxn`, `TxnOffsetCommit` and `EndTxn`,
//! with real commit and abort control batches and `read_committed` isolation.
//! [`FakeBroker::set_transaction_version`](crate::testing::FakeBroker::set_transaction_version)
//! selects between the TV1 and KIP-890
//! TV2 protocols the same way a real cluster does.
//!
//! Logs are in memory, per topic-partition, and nothing is persisted. Any other
//! API is simply not advertised in `ApiVersions`, so the client's own version
//! negotiation refuses it before a request is sent.
//!
//! # Version pinning
//!
//! Each API is advertised with `min == max`, which pins the client onto the one
//! version this broker implements. See the `wire` module for the rationale and the
//! per-API choices.
//!
//! # Determinism and its limits
//!
//! All cluster state sits behind one lock that a handler holds for the whole of
//! one request, so request handling is serialised and state transitions are
//! reproducible. Ordering *within* a connection is exactly the order the client
//! sent, since responses are written before the next request is read.
//!
//! What is not deterministic: when a client opens more than one connection —
//! which `krafka` does, one per broker plus the pool's own — the interleaving
//! *between* connections is whatever the runtime schedules. Tests that need a
//! strict global order should assert on per-API counts and sequences rather
//! than on a total ordering of all requests.

mod handlers;
mod state;
mod wire;

#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::Mutex;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use crate::consumer::ConsumerRecord;

use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::ApiKey;
use crate::protocol::{Decode, KafkaString, TaggedFields};
use crate::protocol::{RequestHeader, ResponseHeader};

pub use state::{
    BrokerNode, ClusterState, CommittedOffset, GroupMember, GroupState, PartitionState, TopicState,
};

/// Largest request frame the fake broker will accept, as a guard against a
/// malformed length prefix.
const MAX_FRAME_LEN: usize = 64 * 1024 * 1024;

/// What a control hook tells the broker to do with one request.
///
/// Returned from the closure registered with [`FakeBroker::on`] and friends.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Control {
    /// Fall through to the default handler.
    Pass,
    /// Answer with a structurally valid response carrying this error code.
    ///
    /// The code is placed in whatever field the API actually has — top level,
    /// per topic, or per partition — so the client runs its normal error
    /// handling rather than its malformed-frame path.
    Error(ErrorCode),
    /// Wait, then fall through to the default handler.
    ///
    /// Use this to push a response past the client's request timeout. The
    /// connection stays open and the late response is still written, which is
    /// what makes "does the client survive a response it no longer wants?"
    /// testable.
    Delay(Duration),
    /// Wait, then apply the nested control.
    DelayThen(Duration, Box<Control>),
    /// Drop the connection without answering.
    Disconnect,
    /// Never answer, but hold the connection open.
    ///
    /// The client should hit its own request timeout. Unlike [`Control::Delay`]
    /// no response is ever written, so this also blocks every later request on
    /// the same connection — Kafka responses are ordered per connection.
    Silence,
    /// Answer a `Fetch` normally, but corrupt the record bytes so the batch
    /// fails its CRC32C check.
    ///
    /// Models on-disk or on-the-wire corruption: the response framing is
    /// intact and the error is *inside* the record batch, which is the only
    /// way to reach the client's batch-decode failure path. A byte inside the
    /// CRC-covered region is flipped, leaving `batch_length` untouched so the
    /// surrounding response still parses.
    ///
    /// Only modelled for `Fetch`; applying it to any other API is an error
    /// rather than a silent pass-through, so a test cannot quietly assert
    /// nothing.
    CorruptRecords,
}

/// A request the broker received, as recorded for assertions.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RecordedRequest {
    /// Which API was called.
    pub api_key: ApiKey,
    /// The version the client negotiated.
    pub api_version: i16,
    /// The correlation ID the client used.
    pub correlation_id: i32,
    /// The client ID from the request header, if any.
    pub client_id: Option<String>,
    /// The broker node the request arrived at.
    pub node_id: i32,
    /// Monotonic sequence number across every request to the whole cluster.
    pub sequence: u64,
}

/// The request a control hook is deciding about.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RequestInfo {
    /// Which API was called.
    pub api_key: ApiKey,
    /// The version the client negotiated.
    pub api_version: i16,
    /// The correlation ID the client used.
    pub correlation_id: i32,
    /// The client ID from the request header, if any.
    pub client_id: Option<String>,
    /// The broker node the request arrived at.
    pub node_id: i32,
    /// How many requests for this API the broker has already seen, counting
    /// from zero. Lets a hook branch on "the third Produce" without keeping
    /// state of its own.
    pub api_call_index: u64,
}

type HookFn = Arc<dyn Fn(&RequestInfo) -> Control + Send + Sync>;

struct Hook {
    apply: HookFn,
    /// Remaining firings, or `None` for unlimited.
    remaining: Option<u32>,
}

#[derive(Default)]
struct Hooks {
    by_api: HashMap<ApiKey, Vec<Hook>>,
}

impl Hooks {
    /// Consume the first hook registered for this API that still has firings
    /// left, returning the control it produced.
    fn take(&mut self, info: &RequestInfo) -> Option<Control> {
        let hooks = self.by_api.get_mut(&info.api_key)?;
        let hook = hooks.first_mut()?;
        let control = (hook.apply)(info);
        if let Some(remaining) = hook.remaining.as_mut() {
            *remaining = remaining.saturating_sub(1);
            if *remaining == 0 {
                hooks.remove(0);
            }
        }
        Some(control)
    }
}

struct Shared {
    cluster: Mutex<ClusterState>,
    hooks: Mutex<Hooks>,
    log: Mutex<Vec<RecordedRequest>>,
    sequence: AtomicU64,
}

impl Shared {
    fn record(&self, request: RecordedRequest) {
        self.log.lock().push(request);
    }

    fn api_call_index(&self, api_key: ApiKey) -> u64 {
        self.log
            .lock()
            .iter()
            .filter(|r| r.api_key == api_key)
            .count() as u64
    }
}

/// A fake Kafka broker, or a small cluster of them, running in this process.
///
/// Dropping the handle shuts every listener down and aborts the accept and
/// connection tasks.
pub struct FakeBroker {
    shared: Arc<Shared>,
    addrs: Vec<SocketAddr>,
    tasks: Vec<JoinHandle<()>>,
}

impl std::fmt::Debug for FakeBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FakeBroker")
            .field("addrs", &self.addrs)
            .finish_non_exhaustive()
    }
}

impl Drop for FakeBroker {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl FakeBroker {
    /// Start a single-broker cluster.
    pub async fn start() -> Result<Self> {
        Self::start_cluster(1).await
    }

    /// Start a cluster of `broker_count` brokers, each on its own loopback
    /// port, all sharing one set of cluster state.
    ///
    /// Broker IDs are `0..broker_count`, and broker 0 is the initial
    /// controller. Multiple brokers are what make a *real* leader or
    /// coordinator move testable: the client has to notice the move and
    /// reconnect somewhere else, rather than being handed a different answer on
    /// the same socket.
    pub async fn start_cluster(broker_count: usize) -> Result<Self> {
        let broker_count = broker_count.max(1);

        let mut listeners = Vec::with_capacity(broker_count);
        let mut addrs = Vec::with_capacity(broker_count);
        for _ in 0..broker_count {
            let listener = TcpListener::bind("127.0.0.1:0").await.map_err(io_error)?;
            addrs.push(listener.local_addr().map_err(io_error)?);
            listeners.push(listener);
        }

        let mut cluster = ClusterState::new(broker_count);
        for (broker, addr) in cluster.brokers.iter_mut().zip(&addrs) {
            broker.host = addr.ip().to_string();
            broker.port = i32::from(addr.port());
        }

        let shared = Arc::new(Shared {
            cluster: Mutex::new(cluster),
            hooks: Mutex::new(Hooks::default()),
            log: Mutex::new(Vec::new()),
            sequence: AtomicU64::new(0),
        });

        let tasks = listeners
            .into_iter()
            .enumerate()
            .map(|(index, listener)| {
                let shared = Arc::clone(&shared);
                let node_id = index as i32;
                tokio::spawn(async move { accept_loop(listener, node_id, shared).await })
            })
            .collect();

        Ok(Self {
            shared,
            addrs,
            tasks,
        })
    }

    /// Bootstrap string for every broker, ready to hand to a client builder.
    pub fn bootstrap_servers(&self) -> String {
        self.addrs
            .iter()
            .map(SocketAddr::to_string)
            .collect::<Vec<_>>()
            .join(",")
    }

    /// Address of one broker by node ID.
    pub fn broker_addr(&self, node_id: i32) -> Option<SocketAddr> {
        self.addrs.get(usize::try_from(node_id).ok()?).copied()
    }

    // -- control hooks ------------------------------------------------------

    /// Register a hook that applies to every request for `api_key`.
    ///
    /// Registering a second hook for the same API queues it behind the first;
    /// the queue is consumed in registration order as earlier hooks run out of
    /// firings. An unlimited hook therefore blocks anything queued behind it.
    pub fn on<F>(&self, api_key: ApiKey, hook: F)
    where
        F: Fn(&RequestInfo) -> Control + Send + Sync + 'static,
    {
        self.register(api_key, hook, None);
    }

    /// Register a hook that applies to the next request for `api_key` only.
    pub fn on_once<F>(&self, api_key: ApiKey, hook: F)
    where
        F: Fn(&RequestInfo) -> Control + Send + Sync + 'static,
    {
        self.register(api_key, hook, Some(1));
    }

    /// Register a hook that applies to the next `times` requests for `api_key`.
    pub fn on_times<F>(&self, api_key: ApiKey, times: u32, hook: F)
    where
        F: Fn(&RequestInfo) -> Control + Send + Sync + 'static,
    {
        self.register(api_key, hook, Some(times.max(1)));
    }

    fn register<F>(&self, api_key: ApiKey, hook: F, remaining: Option<u32>)
    where
        F: Fn(&RequestInfo) -> Control + Send + Sync + 'static,
    {
        self.shared
            .hooks
            .lock()
            .by_api
            .entry(api_key)
            .or_default()
            .push(Hook {
                apply: Arc::new(hook),
                remaining,
            });
    }

    /// Remove every registered hook.
    pub fn clear_hooks(&self) {
        self.shared.hooks.lock().by_api.clear();
    }

    // -- observation --------------------------------------------------------

    /// Every request the broker has served, in arrival order.
    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.shared.log.lock().clone()
    }

    /// How many requests for `api_key` the broker has served.
    pub fn request_count(&self, api_key: ApiKey) -> usize {
        self.shared
            .log
            .lock()
            .iter()
            .filter(|r| r.api_key == api_key)
            .count()
    }

    /// Node IDs that served requests for `api_key`, in arrival order.
    ///
    /// Useful for asserting that a client actually moved to a different broker
    /// after a coordinator or leader change rather than retrying the old one.
    pub fn request_nodes(&self, api_key: ApiKey) -> Vec<i32> {
        self.shared
            .log
            .lock()
            .iter()
            .filter(|r| r.api_key == api_key)
            .map(|r| r.node_id)
            .collect()
    }

    /// Forget every recorded request.
    pub fn clear_requests(&self) {
        self.shared.log.lock().clear();
    }

    /// Wait until `api_key` has been seen at least `count` times, or the
    /// timeout expires.
    ///
    /// Returns `true` if the count was reached. Prefer this to a bare sleep:
    /// it makes the test's real precondition explicit and finishes as soon as
    /// it holds.
    pub async fn wait_for_requests(
        &self,
        api_key: ApiKey,
        count: usize,
        timeout: Duration,
    ) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.request_count(api_key) >= count {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    /// Wait until `api_key` has been served by `node_id` at least once, or the
    /// timeout expires.
    ///
    /// Use this instead of [`wait_for_requests`](Self::wait_for_requests) when
    /// the point of the test is *which broker* a request reached. Counting a
    /// request on any node lets a request that was already in flight against
    /// the old node satisfy the wait, so the test stops watching before the
    /// interesting one arrives — a race that only shows up on a loaded machine.
    pub async fn wait_for_request_on_node(
        &self,
        api_key: ApiKey,
        node_id: i32,
        timeout: Duration,
    ) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.request_nodes(api_key).contains(&node_id) {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    // -- cluster manipulation ----------------------------------------------

    /// Read or mutate the cluster state directly.
    ///
    /// The escape hatch for anything the named helpers below do not cover. The
    /// state lock is held for the duration of the closure, so no request is
    /// served while it runs.
    pub fn with_state<T>(&self, f: impl FnOnce(&mut ClusterState) -> T) -> T {
        f(&mut self.shared.cluster.lock())
    }

    /// Create a topic with `partitions` partitions, spreading leadership over
    /// the online brokers. Returns `false` if the topic already existed.
    pub fn create_topic(&self, name: &str, partitions: i32) -> bool {
        self.shared.cluster.lock().create_topic(name, partitions)
    }

    /// Grow an existing topic to `partitions` partitions, as a
    /// `CreatePartitions` admin call would.
    ///
    /// Returns the number of partitions added; `0` if the topic does not exist
    /// or already has at least that many. Kafka never removes partitions, and
    /// neither does this.
    pub fn add_partitions(&self, topic: &str, partitions: i32) -> usize {
        self.shared.cluster.lock().add_partitions(topic, partitions)
    }

    /// Move a partition's leader to `node_id` and bump its leader epoch.
    ///
    /// Returns `false` if the topic-partition does not exist. The epoch bump is
    /// what makes the change visible to a client holding the old epoch: its
    /// next fetch against the old leader is answered with a leader-epoch error
    /// rather than silently succeeding.
    pub fn set_leader(&self, topic: &str, partition: i32, node_id: i32) -> bool {
        let mut cluster = self.shared.cluster.lock();
        match cluster.partition_mut(topic, partition) {
            Some(p) => {
                p.leader = node_id;
                p.leader_epoch += 1;
                if !p.replicas.contains(&node_id) {
                    p.replicas.push(node_id);
                }
                if !p.isr.contains(&node_id) {
                    p.isr.push(node_id);
                }
                true
            }
            None => false,
        }
    }

    /// Bump a partition's leader epoch without changing the leader.
    ///
    /// Returns `false` if the topic-partition does not exist.
    pub fn bump_leader_epoch(&self, topic: &str, partition: i32) -> bool {
        let mut cluster = self.shared.cluster.lock();
        match cluster.partition_mut(topic, partition) {
            Some(p) => {
                p.leader_epoch += 1;
                true
            }
            None => false,
        }
    }

    /// Point a consumer group's coordinator at `node_id`.
    pub fn set_group_coordinator(&self, group_id: &str, node_id: i32) {
        self.shared
            .cluster
            .lock()
            .group_coordinators
            .insert(group_id.to_string(), node_id);
    }

    /// Point a transactional ID's coordinator at `node_id`.
    pub fn set_txn_coordinator(&self, transactional_id: &str, node_id: i32) {
        self.shared
            .cluster
            .lock()
            .txn_coordinators
            .insert(transactional_id.to_string(), node_id);
    }

    /// Set the cluster controller. `-1` means "no controller elected".
    pub fn set_controller(&self, node_id: i32) {
        self.shared.cluster.lock().controller_id = node_id;
    }

    /// Mark a broker up or down.
    ///
    /// A broker marked down is still listed in Metadata — real Kafka keeps
    /// listing brokers it has lost — but is never chosen as a coordinator, and
    /// coordinator lookups that resolve to it are answered
    /// `COORDINATOR_NOT_AVAILABLE`. Its listener keeps accepting, so this
    /// models a broker that is up but out of the cluster's view rather than one
    /// whose socket is gone.
    pub fn set_broker_online(&self, node_id: i32, online: bool) {
        let mut cluster = self.shared.cluster.lock();
        if let Some(broker) = cluster.brokers.iter_mut().find(|b| b.node_id == node_id) {
            broker.online = online;
        }
    }

    /// Advertise a different `ApiVersions` range for one API.
    ///
    /// The broker normally advertises exactly the one version each handler
    /// speaks. This overrides that, so a test can *be* an older broker and
    /// exercise the client's degradation path rather than asserting a
    /// re-implementation of the condition.
    ///
    /// The handlers still serve their own version, so the range given here
    /// must include it unless the test expects the request to be refused
    /// before it is sent — which is the usual reason to reach for this.
    ///
    /// ```ignore
    /// // A broker predating KIP-584's `validate_only` field.
    /// broker.set_api_versions(ApiKey::UpdateFeatures, 0, 0);
    /// ```
    pub fn set_api_versions(&self, api_key: ApiKey, min_version: i16, max_version: i16) {
        self.shared
            .cluster
            .lock()
            .api_version_overrides
            .insert(api_key, (min_version, max_version));
    }

    /// Cluster-finalized level of a feature (KIP-584), if `UpdateFeatures` has
    /// set one.
    pub fn finalized_feature(&self, feature: &str) -> Option<i16> {
        self.shared
            .cluster
            .lock()
            .finalized_features
            .get(feature)
            .copied()
    }

    /// Committed offset for a group's topic-partition, if any.
    pub fn committed_offset(&self, group_id: &str, topic: &str, partition: i32) -> Option<i64> {
        self.shared
            .cluster
            .lock()
            .groups
            .get(group_id)
            .and_then(|g| g.offsets.get(&(topic.to_string(), partition)))
            .map(|c| c.offset)
    }

    /// Offset the next record appended to a partition will receive, which for
    /// this broker is also the high watermark.
    pub fn next_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.shared
            .cluster
            .lock()
            .partition(topic, partition)
            .map(|p| p.next_offset)
    }

    /// Finalize the cluster's `transaction.version` level (KIP-890).
    ///
    /// This is the switch between the two transaction protocols, and it is the
    /// same switch a real cluster uses — the client reads the finalized feature
    /// out of `ApiVersions` and negotiates from it, so nothing here is
    /// special-cased for testing.
    ///
    /// | Level | Protocol | What the client does |
    /// |---|---|---|
    /// | `0` or `1` *(default)* | TV1 | Registers partitions with `AddPartitionsToTxn` and the offsets topic with `AddOffsetsToTxn` before writing |
    /// | `2` | TV2 | Sends neither: `Produce` and `TxnOffsetCommit` carry the transactional ID, and `EndTxn` returns a bumped epoch |
    ///
    /// A fresh broker finalizes nothing, so the default is TV1 — the
    /// conservative protocol, and the one a client must still speak against an
    /// older cluster.
    ///
    /// ```rust,no_run
    /// # use krafka::testing::FakeBroker;
    /// # async fn example() -> krafka::error::Result<()> {
    /// let broker = FakeBroker::start().await?;
    /// broker.set_transaction_version(2); // negotiate KIP-890
    /// # Ok(()) }
    /// ```
    pub fn set_transaction_version(&self, level: i16) {
        let mut cluster = self.shared.cluster.lock();
        cluster
            .finalized_features
            .insert("transaction.version".to_string(), level);
        cluster.finalized_features_epoch += 1;
    }

    /// Producer ID and epoch the coordinator currently holds for a
    /// transactional ID, if `InitProducerId` has run for it.
    ///
    /// The epoch is what proves fencing happened: re-initialising the same
    /// transactional ID must return the same producer ID with a higher epoch,
    /// and under KIP-890 every completed transaction bumps it again.
    pub fn transactional_producer(&self, transactional_id: &str) -> Option<(i64, i16)> {
        self.shared
            .cluster
            .lock()
            .transactions
            .get(transactional_id)
            .map(|t| (t.producer_id, t.producer_epoch))
    }

    /// Whether a transaction is currently open for `transactional_id`.
    pub fn transaction_is_open(&self, transactional_id: &str) -> bool {
        self.shared
            .cluster
            .lock()
            .transactions
            .get(transactional_id)
            .is_some_and(|t| t.open)
    }

    /// Last stable offset of a partition: the first offset a `read_committed`
    /// consumer may not read past.
    ///
    /// Equal to the high watermark when no transaction is open on the
    /// partition, and pinned at the open transaction's first record otherwise.
    pub fn last_stable_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.shared
            .cluster
            .lock()
            .partition(topic, partition)
            .map(|p| p.last_stable_offset())
    }

    /// Every record on `topic` that a `read_committed` consumer would see.
    ///
    /// Reads the broker's own log directly: no consumer, no polling, no
    /// timeout. A test asserting exactly-once behaviour previously had to build
    /// a consumer with the right isolation level, subscribe, poll in a bounded
    /// loop and collect — twenty-five lines whose iteration count is the sort
    /// of thing that becomes flaky when someone tunes it.
    ///
    /// Excludes records inside a transaction that aborted, and records inside a
    /// transaction that is still open (they sit at or past the last stable
    /// offset). Control batches — the commit and abort markers themselves —
    /// are never included, as they are never delivered to an application.
    ///
    /// Records are ordered by partition, then by offset. Returns an empty
    /// vector for a topic that does not exist.
    ///
    /// # Errors
    ///
    /// Returns an error only if a stored batch cannot be decoded, which means
    /// the fake broker's own log is corrupt.
    ///
    /// # Example
    ///
    /// The difference between the two accessors *is* the assertion:
    ///
    /// ```rust,ignore
    /// assert_eq!(broker.committed_records("events")?.len(), 3);
    /// assert_eq!(broker.all_records("events")?.len(), 8); // 5 were aborted
    /// ```
    pub fn committed_records(&self, topic: &str) -> Result<Vec<ConsumerRecord>> {
        self.read_records(topic, true)
    }

    /// Every record on `topic`, including those in aborted and still-open
    /// transactions.
    ///
    /// The `read_uncommitted` view. See
    /// [`committed_records`](Self::committed_records).
    ///
    /// # Errors
    ///
    /// As [`committed_records`](Self::committed_records).
    pub fn all_records(&self, topic: &str) -> Result<Vec<ConsumerRecord>> {
        self.read_records(topic, false)
    }

    /// Shared implementation of the two record accessors.
    ///
    /// `committed_only` applies the two filters a `read_committed` fetch does:
    /// stop at the last stable offset, and drop batches belonging to a
    /// transaction that aborted. A batch is part of an aborted transaction when
    /// its producer ID matches an entry and its base offset falls between that
    /// transaction's first offset and its marker — which is exact here, where
    /// the broker knows both, and simpler than the marker-scanning state
    /// machine a client has to run.
    fn read_records(&self, topic: &str, committed_only: bool) -> Result<Vec<ConsumerRecord>> {
        use crate::protocol::RecordBatch;

        let cluster = self.shared.cluster.lock();
        let Some(topic_state) = cluster.topics.get(topic) else {
            return Ok(Vec::new());
        };

        let mut out = Vec::new();
        for (index, partition) in topic_state.partitions.iter().enumerate() {
            let partition_id = i32::try_from(index).unwrap_or(i32::MAX);
            let limit = if committed_only {
                partition.last_stable_offset()
            } else {
                i64::MAX
            };

            for stored in &partition.log {
                let mut buf = stored.clone();
                let batch = RecordBatch::decode(&mut buf)?;
                let base = batch.base_offset;
                let last = base.saturating_add(i64::from(batch.last_offset_delta));

                if last >= limit {
                    continue;
                }
                if batch.attributes.is_control_batch {
                    continue;
                }
                if committed_only
                    && batch.attributes.is_transactional
                    && partition.aborted_transactions.iter().any(
                        |(producer_id, first_offset, marker_offset)| {
                            *producer_id == batch.producer_id
                                && base >= *first_offset
                                && base < *marker_offset
                        },
                    )
                {
                    continue;
                }

                for record in batch.records {
                    out.push(ConsumerRecord {
                        topic: topic.to_string(),
                        partition: partition_id,
                        offset: base.saturating_add(i64::from(record.offset_delta)),
                        timestamp: batch.base_timestamp.saturating_add(record.timestamp_delta),
                        timestamp_type: batch.attributes.timestamp_type as i8,
                        key: record.key,
                        value: record.value,
                        headers: record
                            .headers
                            .into_iter()
                            .map(|h| (h.key, h.value))
                            .collect(),
                        leader_epoch: Some(batch.partition_leader_epoch),
                        delivery_count: None,
                    });
                }
            }
        }

        Ok(out)
    }

    /// Aborted transactions recorded on a partition, as
    /// `(producer_id, first_offset)`.
    ///
    /// This is exactly what a `read_committed` fetch reports, and what the
    /// consumer uses to drop the aborted data records.
    pub fn aborted_transactions(&self, topic: &str, partition: i32) -> Vec<(i64, i64)> {
        self.shared
            .cluster
            .lock()
            .partition(topic, partition)
            .map(|p| p.aborted_transactions_from(0))
            .unwrap_or_default()
    }
}

fn io_error(e: io::Error) -> KrafkaError {
    KrafkaError::network(e)
}

async fn accept_loop(listener: TcpListener, node_id: i32, shared: Arc<Shared>) {
    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                debug!(node_id, %peer, "fake broker accepted a connection");
                let shared = Arc::clone(&shared);
                tokio::spawn(async move {
                    if let Err(e) = serve(stream, node_id, shared).await {
                        debug!(node_id, "fake broker connection ended: {e}");
                    }
                });
            }
            Err(e) => {
                warn!(node_id, "fake broker accept failed: {e}");
                return;
            }
        }
    }
}

/// Serve one connection until the peer closes it or a control hook drops it.
///
/// Requests are handled strictly in order: the response is written before the
/// next frame is read. That matches Kafka's per-connection response ordering
/// and is what makes a delayed response also delay everything behind it.
async fn serve(mut stream: TcpStream, node_id: i32, shared: Arc<Shared>) -> Result<()> {
    loop {
        let mut len_buf = [0u8; 4];
        match stream.read_exact(&mut len_buf).await {
            Ok(_) => {}
            // A clean EOF is the client closing the connection, not a failure.
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(e) => return Err(io_error(e)),
        }

        let len = i32::from_be_bytes(len_buf);
        if len < 0 || len as usize > MAX_FRAME_LEN {
            return Err(KrafkaError::protocol_kind(
                crate::error::ProtocolErrorKind::Malformed,
                format!("fake broker: implausible request frame length {len}"),
            ));
        }

        // Grow as bytes arrive rather than pre-sizing from the declared
        // length. `MAX_FRAME_LEN` already bounds the damage, but pre-sizing is
        // the one habit the client's own `read_framed_response` deliberately
        // avoids — a peer that declares a large frame and then dribbles should
        // only ever hold the memory it has actually sent. Modelling that here
        // too keeps the harness from teaching the opposite lesson.
        const CHUNK: usize = 8 * 1024;
        let len = len as usize;
        let mut frame = Vec::with_capacity(len.min(CHUNK));
        let mut chunk = [0u8; CHUNK];
        while frame.len() < len {
            let want = (len - frame.len()).min(CHUNK);
            let read = stream.read(&mut chunk[..want]).await.map_err(io_error)?;
            if read == 0 {
                return Err(io_error(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    format!(
                        "fake broker: peer closed after {} of {len} frame bytes",
                        frame.len()
                    ),
                )));
            }
            frame.extend_from_slice(&chunk[..read]);
        }
        let mut frame = Bytes::from(frame);

        let header = read_request_header(&mut frame)?;
        let api_key = header.api_key;

        let sequence = shared.sequence.fetch_add(1, Ordering::Relaxed);
        let info = RequestInfo {
            api_key,
            api_version: header.api_version,
            correlation_id: header.correlation_id,
            client_id: header.client_id.clone(),
            node_id,
            api_call_index: shared.api_call_index(api_key),
        };
        shared.record(RecordedRequest {
            api_key,
            api_version: header.api_version,
            correlation_id: header.correlation_id,
            client_id: header.client_id.clone(),
            node_id,
            sequence,
        });

        let mut control = shared.hooks.lock().take(&info).unwrap_or(Control::Pass);

        // Delays run outside every lock so other connections keep being served.
        loop {
            match control {
                Control::Delay(d) => {
                    tokio::time::sleep(d).await;
                    control = Control::Pass;
                }
                Control::DelayThen(d, inner) => {
                    tokio::time::sleep(d).await;
                    control = *inner;
                }
                _ => break,
            }
        }

        match control {
            Control::Disconnect => return Ok(()),
            Control::Silence => {
                // Hold the connection open and answer nothing further. Kafka
                // responses are ordered per connection, so nothing after this
                // could be answered anyway.
                std::future::pending::<()>().await;
                return Ok(());
            }
            _ => {}
        }

        let mut body = BytesMut::new();
        let outcome = match control {
            Control::Error(code) => {
                handlers::dispatch_error(api_key, header.api_version, &mut frame, code, &mut body)
            }
            Control::CorruptRecords => {
                let mut cluster = shared.cluster.lock();
                handlers::dispatch_corrupt(api_key, &mut frame, node_id, &mut cluster, &mut body)
            }
            _ => {
                let mut cluster = shared.cluster.lock();
                handlers::dispatch(
                    api_key,
                    header.api_version,
                    &mut frame,
                    node_id,
                    &mut cluster,
                    &mut body,
                )
            }
        };
        outcome?;

        let mut out = BytesMut::with_capacity(body.len() + 8);
        out.put_i32(0); // placeholder for the frame length
        write_response_header(&mut out, api_key, header.api_version, header.correlation_id);
        out.put_slice(&body);
        let frame_len = i32::try_from(out.len() - 4).map_err(|_| {
            KrafkaError::protocol_kind(
                crate::error::ProtocolErrorKind::Malformed,
                "fake broker: response frame exceeds i32::MAX",
            )
        })?;
        out[0..4].copy_from_slice(&frame_len.to_be_bytes());

        stream.write_all(&out).await.map_err(io_error)?;
        stream.flush().await.map_err(io_error)?;
    }
}

/// Parsed request header, mirroring [`RequestHeader`]'s encoders.
struct ParsedHeader {
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<String>,
}

fn read_request_header(buf: &mut Bytes) -> Result<ParsedHeader> {
    let api_key = ApiKey::from_i16(i16::decode(buf)?);
    let api_version = i16::decode(buf)?;
    let correlation_id = i32::decode(buf)?;
    // ClientId uses the standard two-byte length prefix in both header v1 and
    // header v2 — the Kafka spec marks it `flexibleVersions: "none"`.
    let client_id = KafkaString::decode(buf)?.0;
    if RequestHeader::header_version(api_key, api_version) == 2 {
        let _ = TaggedFields::decode(buf)?;
    }
    Ok(ParsedHeader {
        api_key,
        api_version,
        correlation_id,
        client_id,
    })
}

fn write_response_header(
    out: &mut BytesMut,
    api_key: ApiKey,
    api_version: i16,
    correlation_id: i32,
) {
    out.put_i32(correlation_id);
    if ResponseHeader::header_version(api_key, api_version) == 1 {
        // Empty tagged fields: a single zero varint.
        out.put_u8(0);
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod unit_tests {
    use super::*;
    use bytes::Buf;

    #[test]
    fn once_hooks_fire_exactly_once_then_fall_through() {
        let mut hooks = Hooks::default();
        hooks
            .by_api
            .entry(ApiKey::Metadata)
            .or_default()
            .push(Hook {
                apply: Arc::new(|_| Control::Error(ErrorCode::NotController)),
                remaining: Some(1),
            });

        let info = RequestInfo {
            api_key: ApiKey::Metadata,
            api_version: 8,
            correlation_id: 1,
            client_id: None,
            node_id: 0,
            api_call_index: 0,
        };

        assert!(matches!(
            hooks.take(&info),
            Some(Control::Error(ErrorCode::NotController))
        ));
        assert!(hooks.take(&info).is_none(), "the hook must not fire twice");
    }

    #[test]
    fn queued_hooks_are_consumed_in_registration_order() {
        let mut hooks = Hooks::default();
        let entry = hooks.by_api.entry(ApiKey::Produce).or_default();
        entry.push(Hook {
            apply: Arc::new(|_| Control::Error(ErrorCode::NotLeaderForPartition)),
            remaining: Some(2),
        });
        entry.push(Hook {
            apply: Arc::new(|_| Control::Disconnect),
            remaining: Some(1),
        });

        let info = RequestInfo {
            api_key: ApiKey::Produce,
            api_version: 8,
            correlation_id: 1,
            client_id: None,
            node_id: 0,
            api_call_index: 0,
        };

        assert!(matches!(hooks.take(&info), Some(Control::Error(_))));
        assert!(matches!(hooks.take(&info), Some(Control::Error(_))));
        assert!(matches!(hooks.take(&info), Some(Control::Disconnect)));
        assert!(hooks.take(&info).is_none());
    }

    /// The header round-trip must agree with the client's encoder for both a
    /// non-flexible and a flexible API version.
    #[test]
    fn request_headers_round_trip_against_the_client_encoder() {
        for (api_key, version) in [(ApiKey::Metadata, 8i16), (ApiKey::Metadata, 12i16)] {
            let header = RequestHeader::new(api_key, version, 77).with_client_id("krafka-test");
            let mut buf = BytesMut::new();
            header.encode(&mut buf).unwrap();
            let mut buf = buf.freeze();

            let parsed = read_request_header(&mut buf).unwrap();
            assert_eq!(parsed.api_key, api_key);
            assert_eq!(parsed.api_version, version);
            assert_eq!(parsed.correlation_id, 77);
            assert_eq!(parsed.client_id.as_deref(), Some("krafka-test"));
            assert_eq!(buf.remaining(), 0, "header reader left bytes behind");
        }
    }

    #[tokio::test]
    async fn a_started_cluster_advertises_one_address_per_broker() {
        let broker = FakeBroker::start_cluster(3).await.unwrap();
        assert_eq!(broker.bootstrap_servers().split(',').count(), 3);
        assert!(broker.broker_addr(2).is_some());
        assert!(broker.broker_addr(3).is_none());
    }

    #[tokio::test]
    async fn moving_a_leader_bumps_the_epoch() {
        let broker = FakeBroker::start_cluster(2).await.unwrap();
        assert!(broker.create_topic("orders", 1));

        let before = broker.with_state(|s| {
            let p = s.partition("orders", 0).expect("partition exists");
            (p.leader, p.leader_epoch)
        });
        assert_eq!(before, (0, 0));

        assert!(broker.set_leader("orders", 0, 1));
        let after = broker.with_state(|s| {
            let p = s.partition("orders", 0).expect("partition exists");
            (p.leader, p.leader_epoch)
        });
        assert_eq!(after, (1, 1));

        assert!(!broker.set_leader("missing", 0, 1));
    }
}
