//! Request routing for the admin client.
//!
//! Every admin operation runs through [`Call`]: it names a [`Target`] for each
//! key it works on (a topic, a partition, a group, a broker, or the single
//! unit key of a whole-call request), and the driver resolves each target to a
//! node, batches the keys per node, sends the batches concurrently, and
//! retries keys whose attempt failed in a way that can succeed elsewhere or
//! later — with node re-selection and backoff, inside the call's deadline.
//!
//! # What is retried
//!
//! A *read* is retried on any retriable error, including a connection that
//! dropped with the request in flight. A *write* is retried only when it is
//! known not to have been applied: the connection could not be opened, or the
//! broker answered with a code that means "not here, not done"
//! (`NOT_CONTROLLER`, `NOT_COORDINATOR`, `COORDINATOR_LOAD_IN_PROGRESS`,
//! `COORDINATOR_NOT_AVAILABLE`, `NOT_LEADER_OR_FOLLOWER`,
//! `LEADER_NOT_AVAILABLE`, `THROTTLING_QUOTA_EXCEEDED`). A write that was sent
//! and never answered fails with its `Network` or `Timeout` error, because it
//! may have been applied.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::hash::Hash;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use tracing::debug;

use super::AdminClient;
use crate::BrokerId;
use crate::consumer::TopicPartition;
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::network::BrokerConnection;
use crate::protocol::{
    ApiKey, FindCoordinatorRequest, FindCoordinatorResponse, VersionedDecode, VersionedEncode,
    versions,
};

/// Where a key's request must go.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) enum Target {
    /// Any broker; another one is tried when the chosen one fails.
    AnyBroker,
    /// The active controller.
    Controller,
    /// One specific broker.
    Broker(BrokerId),
    /// The coordinator of a consumer or share group.
    GroupCoordinator(String),
    /// The coordinator of a transactional ID.
    TransactionCoordinator(String),
    /// The leader of a partition.
    Leader(TopicPartition),
}

/// Whether the operation changes cluster state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Mode {
    /// Retried on any retriable error.
    Read,
    /// Retried only when not applied.
    Write,
}

/// One admin operation in flight: its name, mode and deadline.
pub(super) struct Call<'a> {
    admin: &'a AdminClient,
    api: &'static str,
    mode: Mode,
    timeout: Duration,
    deadline: Instant,
}

/// A resolved node.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct Node {
    id: BrokerId,
    address: String,
}

/// The outcome of resolving one target.
enum Lookup {
    Found(Node),
    /// Try again next round; the error is reported if the deadline passes.
    Retry(KrafkaError),
    /// The key fails with this error.
    Fail(KrafkaError),
}

/// What became of one batch.
enum Batch<K, V> {
    /// The connection could not be opened: nothing was sent.
    NotSent(KrafkaError),
    /// Sent, but no usable answer arrived.
    Unanswered(KrafkaError),
    /// Answered, per key.
    Answered(Vec<(K, Result<V>)>),
}

/// Broker codes that mean a write was not applied and may be re-sent.
fn not_applied(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::NotController
            | ErrorCode::NotCoordinator
            | ErrorCode::CoordinatorLoadInProgress
            | ErrorCode::CoordinatorNotAvailable
            | ErrorCode::NotLeaderForPartition
            | ErrorCode::LeaderNotAvailable
            | ErrorCode::ThrottlingQuotaExceeded
    )
}

/// Broker codes the driver re-sends a read for. Retriable codes except those
/// that answer the question: an unknown topic stays unknown, and an election
/// that is not needed has nothing to wait for.
fn retriable_read(code: ErrorCode) -> bool {
    code.is_retriable()
        && !matches!(
            code,
            ErrorCode::UnknownTopicOrPartition
                | ErrorCode::UnknownTopicId
                | ErrorCode::ElectionNotNeeded
        )
}

/// Codes after which the cached routing is wrong and metadata is refetched.
fn stale_routing(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::NotController
            | ErrorCode::NotLeaderForPartition
            | ErrorCode::LeaderNotAvailable
            | ErrorCode::FencedLeaderEpoch
            | ErrorCode::UnknownLeaderEpoch
    )
}

/// Codes after which a coordinator must be looked up again.
fn stale_coordinator(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::NotCoordinator | ErrorCode::CoordinatorNotAvailable
    )
}

/// Negotiate the highest version of `api` both sides support.
pub(super) fn negotiate(conn: &BrokerConnection, api: ApiKey, min: i16, max: i16) -> Result<i16> {
    conn.negotiate_api_version(api, max, min).ok_or_else(|| {
        KrafkaError::protocol_kind(
            ProtocolErrorKind::UnknownApiVersion,
            format!(
                "broker {} supports no {api:?} version in v{min}..=v{max}",
                conn.address()
            ),
        )
    })
}

/// Send `request` at `version` and decode the response.
pub(super) async fn exchange<Req, Resp>(
    conn: &BrokerConnection,
    api: ApiKey,
    version: i16,
    request: &Req,
) -> Result<Resp>
where
    Req: VersionedEncode,
    Resp: VersionedDecode,
{
    let mut response = conn
        .send_request(api, version, |buf| request.encode_versioned(version, buf))
        .await?;
    Resp::decode_versioned(version, &mut response)
}

/// A broker's per-item answer as a `Result`: `Ok(())` for no error, otherwise
/// a [`KrafkaError::Broker`] carrying the code and the broker's message.
pub(super) fn answer(code: ErrorCode, message: Option<String>) -> Result<()> {
    if code.is_ok() {
        Ok(())
    } else {
        Err(KrafkaError::broker(
            code,
            message.unwrap_or_else(|| format!("{code:?}")),
        ))
    }
}

impl AdminClient {
    /// Start an admin operation. `timeout` overrides the default API timeout.
    pub(super) fn call(
        &self,
        api: &'static str,
        mode: Mode,
        timeout: Option<Duration>,
    ) -> Result<Call<'_>> {
        if self.is_closed() {
            return Err(KrafkaError::closed("AdminClient is closed"));
        }
        let timeout = timeout.unwrap_or(self.config.default_api_timeout);
        Ok(Call {
            admin: self,
            api,
            mode,
            timeout,
            deadline: Instant::now() + timeout,
        })
    }
}

impl Call<'_> {
    /// Time left before the deadline.
    pub(super) fn remaining(&self) -> Duration {
        self.deadline.saturating_duration_since(Instant::now())
    }

    /// The remaining time as a broker-side `timeout_ms`.
    pub(super) fn remaining_ms(&self) -> i32 {
        crate::util::duration_to_millis_i32(self.remaining())
    }

    /// Send one request to `target` and return its answer.
    ///
    /// `send` returns a [`KrafkaError::Broker`] for an answered error, which
    /// is retried like a per-key error; any other error is a transport
    /// failure.
    pub(super) async fn single<V, S, Fut>(&self, target: Target, send: S) -> Result<V>
    where
        S: Fn(Arc<BrokerConnection>) -> Fut,
        Fut: Future<Output = Result<V>>,
    {
        let send = &send;
        let mut results = self
            .fan_out(
                vec![()],
                |()| target.clone(),
                |conn, _keys| async move {
                    match send(conn).await {
                        Ok(value) => Ok(vec![((), Ok(value))]),
                        Err(e @ KrafkaError::Broker { .. }) => Ok(vec![((), Err(e))]),
                        Err(e) => Err(e),
                    }
                },
            )
            .await;
        results.remove(&()).unwrap_or_else(|| {
            Err(KrafkaError::illegal_state(format!(
                "{}: the driver produced no result",
                self.api
            )))
        })
    }

    /// Run `send` for every key at the node its target resolves to, keys of
    /// one node in one batch, and return a result per key.
    ///
    /// `send` returns `Err` when the batch as a whole failed (transport,
    /// decoding, a top-level error that applies to every key) and otherwise a
    /// result per key. A key it does not answer fails.
    pub(super) async fn fan_out<K, V, T, S, Fut>(
        &self,
        keys: Vec<K>,
        target: T,
        send: S,
    ) -> HashMap<K, Result<V>>
    where
        K: Clone + Eq + Hash,
        T: Fn(&K) -> Target,
        S: Fn(Arc<BrokerConnection>, Vec<K>) -> Fut,
        Fut: Future<Output = Result<Vec<(K, Result<V>)>>>,
    {
        let mut results: HashMap<K, Result<V>> = HashMap::with_capacity(keys.len());
        let mut last_error: HashMap<K, KrafkaError> = HashMap::new();
        let mut pending: Vec<K> = keys;
        let mut routing = Routing::default();
        let mut round: u32 = 0;

        while !pending.is_empty() {
            if self.remaining().is_zero() {
                for key in pending.drain(..) {
                    let error = self.expired(last_error.remove(&key));
                    results.insert(key, Err(error));
                }
                break;
            }

            // Resolve every pending key to a node.
            let targets: Vec<Target> = pending.iter().map(&target).collect();
            let lookups = self.resolve(&targets, &mut routing).await;
            let mut batches: HashMap<Node, Vec<K>> = HashMap::new();
            let mut retry: Vec<K> = Vec::new();
            for ((key, target), lookup) in pending.drain(..).zip(targets).zip(lookups) {
                match lookup {
                    Lookup::Found(node) => batches.entry(node).or_default().push(key),
                    Lookup::Retry(e) => {
                        routing.note_failure(&target, &e);
                        last_error.insert(key.clone(), e);
                        retry.push(key);
                    }
                    Lookup::Fail(e) => {
                        results.insert(key, Err(e));
                    }
                }
            }

            // Send the batches concurrently.
            let pool = &self.admin.pool;
            let send = &send;
            let outcomes = futures::future::join_all(batches.into_iter().map(|(node, keys)| {
                let remaining = self.remaining();
                async move {
                    let conn = match tokio::time::timeout(
                        remaining,
                        pool.get_connection_by_id(node.id, &node.address),
                    )
                    .await
                    {
                        Ok(Ok(conn)) => conn,
                        Ok(Err(e)) => return (node, keys, Batch::NotSent(e)),
                        Err(_) => {
                            return (
                                node,
                                keys,
                                Batch::NotSent(KrafkaError::timeout("connecting")),
                            );
                        }
                    };
                    let remaining = self.remaining();
                    let outcome =
                        match tokio::time::timeout(remaining, send(conn, keys.clone())).await {
                            Ok(Ok(answers)) => Batch::Answered(answers),
                            Ok(Err(e)) => Batch::Unanswered(e),
                            Err(_) => Batch::Unanswered(KrafkaError::timeout(format!(
                                "{} sent and unanswered when the deadline passed",
                                self.api
                            ))),
                        };
                    (node, keys, outcome)
                }
            }))
            .await;

            for (node, keys, outcome) in outcomes {
                match outcome {
                    Batch::NotSent(e) => {
                        routing.failed_nodes.insert(node.id);
                        for key in keys {
                            routing.note_failure(&target(&key), &e);
                            if e.is_retriable() {
                                last_error.insert(key.clone(), e.clone());
                                retry.push(key);
                            } else {
                                results.insert(key, Err(e.clone()));
                            }
                        }
                    }
                    Batch::Unanswered(e) => {
                        routing.failed_nodes.insert(node.id);
                        for key in keys {
                            routing.note_failure(&target(&key), &e);
                            if self.mode == Mode::Read && e.is_retriable() {
                                last_error.insert(key.clone(), e.clone());
                                retry.push(key);
                            } else {
                                results.insert(key, Err(e.clone()));
                            }
                        }
                    }
                    Batch::Answered(answers) => {
                        let mut unanswered: HashSet<K> = keys.iter().cloned().collect();
                        for (key, result) in answers {
                            if !unanswered.remove(&key) {
                                continue;
                            }
                            match result {
                                Ok(value) => {
                                    results.insert(key, Ok(value));
                                }
                                Err(e) if self.retriable(&e) => {
                                    routing.note_failure(&target(&key), &e);
                                    last_error.insert(key.clone(), e);
                                    retry.push(key);
                                }
                                Err(e) => {
                                    results.insert(key, Err(e));
                                }
                            }
                        }
                        for key in unanswered {
                            results.insert(
                                key,
                                Err(KrafkaError::protocol_kind(
                                    ProtocolErrorKind::Malformed,
                                    format!(
                                        "{}: broker {} returned no result for a requested item",
                                        self.api, node.id
                                    ),
                                )),
                            );
                        }
                    }
                }
            }

            pending = retry;
            if pending.is_empty() {
                break;
            }
            round = round.saturating_add(1);
            debug!(
                api = self.api,
                round,
                pending = pending.len(),
                "retrying admin request"
            );
            routing.refresh_metadata(self).await;
            let backoff = self.admin.config.retry_backoff.calculate_backoff(round);
            tokio::time::sleep(backoff.min(self.remaining())).await;
        }

        results
    }

    /// Whether an answered per-key error should be retried.
    fn retriable(&self, error: &KrafkaError) -> bool {
        match (self.mode, error) {
            (Mode::Read, KrafkaError::Broker { code, .. }) => retriable_read(*code),
            (Mode::Write, KrafkaError::Broker { code, .. }) => not_applied(*code),
            (Mode::Read, other) => other.is_retriable(),
            (Mode::Write, _) => false,
        }
    }

    /// The error for a key still pending at the deadline: the broker's own
    /// answer when there was one, otherwise a timeout naming the last failure.
    fn expired(&self, last: Option<KrafkaError>) -> KrafkaError {
        match last {
            Some(e @ KrafkaError::Broker { .. }) => e,
            Some(e) => KrafkaError::timeout(format!(
                "{} did not complete within {:?}; last error: {e}",
                self.api, self.timeout
            )),
            None => KrafkaError::timeout(format!(
                "{} did not complete within {:?}",
                self.api, self.timeout
            )),
        }
    }

    /// Resolve each target to a node.
    async fn resolve(&self, targets: &[Target], routing: &mut Routing) -> Vec<Lookup> {
        let metadata = &self.admin.metadata;

        // Leaders: one fetch for every topic the cache cannot route.
        let unroutable: Vec<&str> = targets
            .iter()
            .filter_map(|t| match t {
                Target::Leader(tp) if leader_node(self.admin, tp).is_none() => {
                    Some(tp.topic.as_str())
                }
                _ => None,
            })
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        if !unroutable.is_empty() {
            self.bounded(metadata.force_refresh(Some(&unroutable)))
                .await;
        }

        // Coordinators: one lookup per distinct key, concurrently.
        let missing: HashSet<Target> = targets
            .iter()
            .filter(|t| {
                matches!(
                    t,
                    Target::GroupCoordinator(_) | Target::TransactionCoordinator(_)
                ) && !routing.coordinators.contains_key(*t)
            })
            .cloned()
            .collect();
        let excluded = routing.failed_nodes.clone();
        let found = futures::future::join_all(missing.into_iter().map(|t| {
            let excluded = &excluded;
            async move {
                let lookup = self.find_coordinator(&t, excluded).await;
                (t, lookup)
            }
        }))
        .await;
        let mut lookups_by_target: HashMap<Target, Lookup> = HashMap::new();
        for (t, lookup) in found {
            match lookup {
                Lookup::Found(node) => {
                    routing.coordinators.insert(t, node);
                }
                Lookup::Retry(e) => {
                    if let Some(id) = node_of_lookup_error(&e) {
                        routing.failed_nodes.insert(id);
                    }
                    lookups_by_target.insert(t, Lookup::Retry(e));
                }
                other => {
                    lookups_by_target.insert(t, other);
                }
            }
        }

        // Any broker: one node per round, so the keys batch together.
        let any = if targets.contains(&Target::AnyBroker) {
            Some(self.any_broker(&routing.failed_nodes).await)
        } else {
            None
        };

        let mut controller_refreshed = false;
        let mut lookups = Vec::with_capacity(targets.len());
        for t in targets {
            let lookup = match t {
                Target::AnyBroker => match &any {
                    Some(Ok(node)) => Lookup::Found(node.clone()),
                    Some(Err(e)) => Lookup::Retry(e.clone()),
                    None => Lookup::Retry(KrafkaError::unavailable("no broker")),
                },
                Target::Controller => {
                    if metadata.controller().is_none() && !controller_refreshed {
                        controller_refreshed = true;
                        self.bounded(metadata.force_refresh(Some(&[]))).await;
                    }
                    match metadata.controller() {
                        Some(c) => Lookup::Found(Node {
                            id: c.id(),
                            address: c.address().to_string(),
                        }),
                        None => Lookup::Retry(KrafkaError::broker(
                            ErrorCode::NotController,
                            "the cluster reports no active controller",
                        )),
                    }
                }
                Target::Broker(id) => {
                    if metadata.broker(*id).is_none() && !routing.brokers_refreshed {
                        routing.brokers_refreshed = true;
                        self.bounded(metadata.force_refresh(Some(&[]))).await;
                    }
                    match metadata.broker(*id) {
                        Some(b) => Lookup::Found(Node {
                            id: *id,
                            address: b.address().to_string(),
                        }),
                        None => Lookup::Fail(KrafkaError::broker(
                            ErrorCode::BrokerNotAvailable,
                            format!("broker {id} is not in the cluster metadata"),
                        )),
                    }
                }
                Target::GroupCoordinator(_) | Target::TransactionCoordinator(_) => {
                    match routing.coordinators.get(t) {
                        Some(node) => Lookup::Found(node.clone()),
                        None => match lookups_by_target.get(t) {
                            Some(Lookup::Fail(e)) => Lookup::Fail(e.clone()),
                            Some(Lookup::Retry(e)) => Lookup::Retry(e.clone()),
                            _ => Lookup::Retry(KrafkaError::broker(
                                ErrorCode::CoordinatorNotAvailable,
                                "coordinator lookup did not complete",
                            )),
                        },
                    }
                }
                Target::Leader(tp) => match leader_node(self.admin, tp) {
                    Some(node) => Lookup::Found(node),
                    None => match metadata.topic_error(&tp.topic) {
                        Some(code) if !retriable_read(code) => Lookup::Fail(KrafkaError::broker(
                            code,
                            format!("topic {} cannot be routed", tp.topic),
                        )),
                        _ if metadata
                            .topic_arc(&tp.topic)
                            .is_some_and(|t| t.partition(tp.partition).is_none()) =>
                        {
                            Lookup::Fail(KrafkaError::broker(
                                ErrorCode::UnknownTopicOrPartition,
                                format!("{}-{} does not exist", tp.topic, tp.partition),
                            ))
                        }
                        _ => Lookup::Retry(KrafkaError::broker(
                            ErrorCode::LeaderNotAvailable,
                            format!("no leader for {}-{}", tp.topic, tp.partition),
                        )),
                    },
                },
            };
            lookups.push(lookup);
        }
        lookups
    }

    /// A broker to serve an any-broker request: a random one not yet failed
    /// in this call, or any random one when every broker has failed.
    async fn any_broker(&self, failed: &HashSet<BrokerId>) -> Result<Node> {
        use rand::seq::IndexedRandom as _;

        let metadata = &self.admin.metadata;
        let mut brokers = metadata.brokers();
        if brokers.is_empty() {
            self.bounded(metadata.refresh()).await;
            brokers = metadata.brokers();
        }
        let healthy: Vec<_> = brokers
            .iter()
            .filter(|b| !failed.contains(&b.id()))
            .collect();
        let pool: Vec<_> = if healthy.is_empty() {
            brokers.iter().collect()
        } else {
            healthy
        };
        crate::util::with_rng(|rng| pool.choose(rng).copied())
            .map(|b| Node {
                id: b.id(),
                address: b.address().to_string(),
            })
            .ok_or_else(|| KrafkaError::unavailable("the cluster metadata lists no brokers"))
    }

    /// Look up the coordinator for a group or transactional ID on any broker.
    async fn find_coordinator(&self, target: &Target, excluded: &HashSet<BrokerId>) -> Lookup {
        let request = match target {
            Target::GroupCoordinator(group) => FindCoordinatorRequest::for_group(group),
            Target::TransactionCoordinator(id) => FindCoordinatorRequest::for_transaction(id),
            _ => {
                return Lookup::Fail(KrafkaError::illegal_state(
                    "find_coordinator called for a non-coordinator target",
                ));
            }
        };
        let node = match self.any_broker(excluded).await {
            Ok(node) => node,
            Err(e) => return Lookup::Retry(e),
        };
        let attempt = async {
            let conn = self
                .admin
                .pool
                .get_connection_by_id(node.id, &node.address)
                .await?;
            let version = negotiate(
                &conn,
                ApiKey::FindCoordinator,
                versions::FIND_COORDINATOR_MIN,
                versions::FIND_COORDINATOR_MAX,
            )?;
            exchange::<_, FindCoordinatorResponse>(
                &conn,
                ApiKey::FindCoordinator,
                version,
                &request,
            )
            .await
        };
        let response = match tokio::time::timeout(self.remaining(), attempt).await {
            Ok(Ok(response)) => response,
            Ok(Err(e)) if e.is_retriable() => {
                return Lookup::Retry(LookupFailure::wrap(node.id, e));
            }
            Ok(Err(e)) => return Lookup::Fail(e),
            Err(_) => return Lookup::Retry(KrafkaError::timeout("FindCoordinator")),
        };
        if response.error_code.is_ok() {
            return Lookup::Found(Node {
                id: response.node_id,
                address: format!("{}:{}", response.host, response.port),
            });
        }
        let error = KrafkaError::broker(
            response.error_code,
            response
                .error_message
                .unwrap_or_else(|| format!("FindCoordinator for '{}' failed", request.key)),
        );
        if retriable_read(response.error_code) {
            Lookup::Retry(error)
        } else {
            Lookup::Fail(error)
        }
    }

    /// Run a metadata fetch, bounded by the deadline. Failures are left to
    /// the lookup that follows.
    async fn bounded(&self, fetch: impl Future<Output = Result<()>>) {
        match tokio::time::timeout(self.remaining(), fetch).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => debug!(api = self.api, error = %e, "metadata fetch failed"),
            Err(_) => debug!(api = self.api, "metadata fetch outlasted the deadline"),
        }
    }
}

/// The partition leader from cached metadata.
fn leader_node(admin: &AdminClient, tp: &TopicPartition) -> Option<Node> {
    let id = admin.metadata.leader(&tp.topic, tp.partition)?;
    let broker = admin.metadata.broker(id)?;
    Some(Node {
        id,
        address: broker.address().to_string(),
    })
}

/// Per-call routing state.
#[derive(Default)]
struct Routing {
    /// Nodes that failed in this call; any-broker requests avoid them.
    failed_nodes: HashSet<BrokerId>,
    /// Coordinators found in this call.
    coordinators: HashMap<Target, Node>,
    /// Whether a fetch for a missing broker ID already ran.
    brokers_refreshed: bool,
    /// Topics whose routing went stale this round.
    stale_topics: HashSet<String>,
    /// The controller went stale this round.
    stale_controller: bool,
}

impl Routing {
    /// Forget what a failure says is wrong.
    fn note_failure(&mut self, target: &Target, error: &KrafkaError) {
        let code = match error {
            KrafkaError::Broker { code, .. } => Some(*code),
            _ => None,
        };
        match target {
            Target::GroupCoordinator(_) | Target::TransactionCoordinator(_) => {
                if code.is_none_or(stale_coordinator) {
                    self.coordinators.remove(target);
                }
            }
            Target::Controller => {
                if code.is_none_or(stale_routing) {
                    self.stale_controller = true;
                }
            }
            Target::Leader(tp) => {
                if code.is_none_or(stale_routing) {
                    self.stale_topics.insert(tp.topic.clone());
                }
            }
            Target::AnyBroker | Target::Broker(_) => {}
        }
    }

    /// Refetch the metadata failures in this round made stale.
    async fn refresh_metadata(&mut self, call: &Call<'_>) {
        let metadata = &call.admin.metadata;
        if !self.stale_topics.is_empty() {
            let topics: Vec<String> = self.stale_topics.drain().collect();
            let names: Vec<&str> = topics.iter().map(String::as_str).collect();
            call.bounded(metadata.force_refresh(Some(&names))).await;
        } else if self.stale_controller {
            call.bounded(metadata.force_refresh(Some(&[]))).await;
        }
        self.stale_controller = false;
    }
}

/// A lookup transport failure, remembering which broker served it so the
/// next lookup avoids it.
#[derive(Debug)]
struct LookupFailure {
    node: BrokerId,
    source: KrafkaError,
}

impl LookupFailure {
    fn wrap(node: BrokerId, source: KrafkaError) -> KrafkaError {
        KrafkaError::network(std::io::Error::other(Self { node, source }))
    }
}

impl std::fmt::Display for LookupFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "coordinator lookup on broker {} failed: {}",
            self.node, self.source
        )
    }
}

impl std::error::Error for LookupFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

/// The broker a failed coordinator lookup ran on.
fn node_of_lookup_error(error: &KrafkaError) -> Option<BrokerId> {
    match error {
        KrafkaError::Network(io) => io
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<LookupFailure>())
            .map(|failure| failure.node),
        _ => None,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn writes_retry_only_codes_that_mean_not_applied() {
        for code in [
            ErrorCode::NotController,
            ErrorCode::NotCoordinator,
            ErrorCode::CoordinatorLoadInProgress,
            ErrorCode::NotLeaderForPartition,
            ErrorCode::LeaderNotAvailable,
            ErrorCode::ThrottlingQuotaExceeded,
        ] {
            assert!(not_applied(code), "{code:?}");
        }
        for code in [
            ErrorCode::RequestTimedOut,
            ErrorCode::NetworkException,
            ErrorCode::TopicAlreadyExists,
            ErrorCode::None,
        ] {
            assert!(!not_applied(code), "{code:?}");
        }
    }

    #[test]
    fn reads_do_not_retry_answers() {
        assert!(retriable_read(ErrorCode::CoordinatorLoadInProgress));
        assert!(retriable_read(ErrorCode::NotLeaderForPartition));
        assert!(!retriable_read(ErrorCode::UnknownTopicOrPartition));
        assert!(!retriable_read(ErrorCode::ElectionNotNeeded));
        assert!(!retriable_read(ErrorCode::GroupAuthorizationFailed));
    }

    #[test]
    fn a_lookup_failure_names_its_broker() {
        let error = LookupFailure::wrap(
            7,
            KrafkaError::network(std::io::Error::other("connection reset")),
        );
        assert!(error.is_retriable());
        assert_eq!(node_of_lookup_error(&error), Some(7));
        assert_eq!(node_of_lookup_error(&KrafkaError::timeout("x")), None);
    }

    #[test]
    fn an_answer_keeps_the_broker_code() {
        assert!(answer(ErrorCode::None, None).is_ok());
        match answer(ErrorCode::TopicAlreadyExists, Some("exists".into())) {
            Err(KrafkaError::Broker { code, message }) => {
                assert_eq!(code, ErrorCode::TopicAlreadyExists);
                assert_eq!(message, "exists");
            }
            other => panic!("expected a broker error, got {other:?}"),
        }
    }
}
