//! Connection pool for managing broker connections.
//!
//! - **One data connection per broker**, multiplexed, as the Java client
//!   keeps one per node. A group coordinator gets a second connection
//!   ([`ConnectionPurpose::Coordination`]) so its heartbeats never queue
//!   behind fetches and produces on the broker's one-request-at-a-time
//!   socket.
//! - **One dial per call.** `get_connection*` makes at most one connection
//!   attempt, bounded by `connect_timeout`; concurrent callers for the same
//!   connection share it. The caller's own deadline applies by dropping the
//!   future.
//! - **Per-address reconnect backoff.** After a failed dial, calls for that
//!   address fail fast with a retriable error until the backoff (50 ms
//!   doubling to 1 s, with jitter) has passed; a successful dial resets it.
//! - **A cap that never blocks a replacement.** `max_total_connections`
//!   counts live connections and dials in progress; replacing a dead or
//!   session-expired connection frees its slot first.

use ahash::AHashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use parking_lot::{Mutex, RwLock};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::connection::{BrokerConnection, ConnectionConfig};
use crate::BrokerId;
use crate::error::{KrafkaError, Result};
use crate::metrics::ConnectionRecorder;
use crate::util::BackoffPolicy;

/// Default idle-eviction timeout for a pooled connection.
///
/// Matches the Apache Kafka Java client's `connections.max.idle.ms = 540_000`
/// (9 minutes). `librdkafka` defaults to 10 min and `franz-go` to 20 min; 9
/// min is the most conservative of the reference clients and avoids
/// accumulating sockets to rotated-out brokers on long-lived clients whose
/// metadata churns (broker scale-up/down, topic drift).
pub const DEFAULT_MAX_IDLE: Duration = Duration::from_secs(9 * 60);

/// Reconnect backoff after a failed dial to an address: the Java client's
/// `reconnect.backoff.ms` (50 ms) doubling to `reconnect.backoff.max.ms`
/// (1 s), with 20 % jitter.
const RECONNECT_BACKOFF: BackoffPolicy = BackoffPolicy {
    initial_backoff: Duration::from_millis(50),
    max_backoff: Duration::from_secs(1),
    backoff_multiplier: 2.0,
    jitter_factor: 0.2,
};

/// Which traffic a pooled connection carries.
///
/// The broker reads one request per connection at a time, so a heartbeat
/// written behind a long-polling fetch on the same socket waits for the
/// fetch. Coordination traffic therefore gets its own connection to the
/// coordinator's address.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ConnectionPurpose {
    /// Produce, fetch, metadata, admin and every other request.
    Data,
    /// Requests a group member sends to its group coordinator: `JoinGroup`,
    /// `SyncGroup`, `Heartbeat`, `LeaveGroup`, `ConsumerGroupHeartbeat`,
    /// `ShareGroupHeartbeat`, `OffsetCommit` and `OffsetFetch`.
    Coordination,
}

/// A pooled connection's identity: where it goes and what it carries.
type PoolKey = (String, ConnectionPurpose);

/// Callers waiting for one dial.
type DialWaiters = Vec<oneshot::Sender<Result<Arc<BrokerConnection>>>>;

/// Failed-dial history for one address.
#[derive(Debug)]
struct ReconnectState {
    /// Consecutive failed dials.
    failures: u32,
    /// No dial before this instant.
    next_attempt_at: Instant,
    /// The error of the last failed dial.
    last_error: KrafkaError,
}

impl ReconnectState {
    /// The error a call gets inside the backoff window. A non-retriable dial
    /// failure (authentication, configuration) is repeated as it was, so the
    /// backoff never hides it behind a retriable error.
    fn error_in_window(&self, address: &str, now: Instant) -> KrafkaError {
        if !self.last_error.is_retriable() {
            return self.last_error.clone();
        }
        KrafkaError::network(std::io::Error::new(
            std::io::ErrorKind::ConnectionRefused,
            format!(
                "not reconnecting to {address} for another {:?} after {} failed attempt(s); \
                 last error: {}",
                self.next_attempt_at.saturating_duration_since(now),
                self.failures,
                self.last_error
            ),
        ))
    }
}

/// Everything the pool knows, under one lock.
struct PoolState {
    /// Data connections keyed by broker ID (always a cluster-assigned ID).
    by_id: AHashMap<BrokerId, Arc<BrokerConnection>>,
    /// Every pooled connection, keyed by address and purpose.
    by_key: AHashMap<PoolKey, Arc<BrokerConnection>>,
    /// Dials in progress and the callers waiting on each. Each one holds a
    /// slot under the connection cap.
    dialing: AHashMap<PoolKey, DialWaiters>,
    /// Backoff state for addresses whose last dial failed.
    reconnect: AHashMap<String, ReconnectState>,
    /// Incremented by `close_all`; a dial that started before the increment
    /// does not install its connection.
    generation: u64,
    /// Whether the coordination-fallback warning has been logged.
    fallback_warned: bool,
}

impl PoolState {
    fn new() -> Self {
        Self {
            by_id: AHashMap::new(),
            by_key: AHashMap::new(),
            dialing: AHashMap::new(),
            reconnect: AHashMap::new(),
            generation: 0,
            fallback_warned: false,
        }
    }

    /// Connections counted against the cap: live pooled connections plus
    /// dials in progress.
    fn counted_connections(&self) -> usize {
        self.by_key.values().filter(|c| c.is_alive()).count() + self.dialing.len()
    }
}

/// What a caller does after looking at the pool state.
enum DialStart {
    /// A usable connection exists.
    Ready(Arc<BrokerConnection>),
    /// A dial is running; wait for its result.
    Wait(oneshot::Receiver<Result<Arc<BrokerConnection>>>),
    /// A new connection would exceed the cap.
    CapReached(usize),
}

/// A pool of connections to Kafka brokers.
///
/// The hot `get_connection*` path is a read lock and a hash lookup. Every
/// change to the pool — installing a connection, starting a dial, recording a
/// failed dial — happens under the write lock of the same `PoolState`, so
/// the cap and the dial coalescing see one consistent view.
pub struct ConnectionPool {
    /// Pool state, shared with the dial tasks.
    state: Arc<RwLock<PoolState>>,
    /// Connection config.
    config: ConnectionConfig,
    /// Maximum time a connection may sit idle (no submitted requests)
    /// before the idle-evictor removes it from the pool. `None` disables
    /// eviction. Default: 9 min, matching the Java client's
    /// `connections.max.idle.ms`.
    max_idle: Option<Duration>,
    /// Maximum number of live connections across all brokers and purposes.
    ///
    /// `None` (default) means unlimited.
    max_total_connections: Option<usize>,
    /// Handle of the background idle-eviction task, if one was spawned.
    /// Aborted by `close_all`.
    evictor_handle: Mutex<Option<JoinHandle<()>>>,
    /// Handle of the background OAUTHBEARER proactive-refresh task.
    /// `None` when the pool is not configured with an OAUTHBEARER provider.
    /// Aborted by `close_all` alongside the idle-evictor.
    oauth_refresh_handle: Mutex<Option<JoinHandle<()>>>,
    /// Handle of the background TLS certificate reload task (KIP-1288).
    /// `None` unless `TransportConfig::tls_reload_interval` was set.
    /// Aborted by `close_all`.
    tls_reload_handle: Mutex<Option<JoinHandle<()>>>,
}

impl ConnectionPool {
    /// Create a new connection pool.
    ///
    /// Idle eviction is **not** started automatically. After wrapping the pool
    /// in an `Arc`, call [`Self::start_idle_evictor`] to activate the
    /// background sweep task. Without that call, connections are never evicted
    /// **automatically** (though [`Self::evict_idle`] can still be called
    /// manually, regardless of [`Self::with_max_idle`]).
    pub fn new(config: ConnectionConfig) -> Self {
        Self {
            state: Arc::new(RwLock::new(PoolState::new())),
            config,
            max_idle: Some(DEFAULT_MAX_IDLE),
            max_total_connections: None,
            evictor_handle: Mutex::new(None),
            oauth_refresh_handle: Mutex::new(None),
            tls_reload_handle: Mutex::new(None),
        }
    }

    /// Create a new connection pool, wrap it in an `Arc`, and start the
    /// background idle-evictor immediately.
    ///
    /// This is the recommended constructor for production use: the evictor
    /// is activated automatically if a Tokio runtime is available (the same
    /// runtime-availability guard as [`Self::start_idle_evictor`] applies —
    /// if no runtime is detected the pool is returned without eviction and
    /// a `warn!` is emitted).
    ///
    /// Use [`Self::new`] + [`Self::with_max_idle`] + manual
    /// [`Self::start_idle_evictor`] when you need to configure the pool
    /// before starting eviction, or when you need the raw `Self` rather
    /// than an `Arc`.
    pub fn start(config: ConnectionConfig) -> Arc<Self> {
        let pool = Arc::new(Self::new(config));
        pool.start_idle_evictor();
        pool
    }

    /// The recorder every connection of this pool writes to.
    #[inline]
    pub(crate) fn recorder(&self) -> &Arc<ConnectionRecorder> {
        self.config.connection_metrics()
    }

    /// The pool's connection counters.
    pub fn metrics(&self) -> crate::metrics::ConnectionMetrics {
        self.recorder().snapshot()
    }

    /// The usable data connections open now.
    pub(crate) fn open_connections(&self) -> Vec<Arc<BrokerConnection>> {
        self.state
            .read()
            .by_key
            .iter()
            .filter(|((_, purpose), conn)| *purpose == ConnectionPurpose::Data && conn.is_usable())
            .map(|(_, conn)| Arc::clone(conn))
            .collect()
    }

    /// Override the idle-eviction timeout.
    ///
    /// Connections that have submitted no requests for longer than this
    /// are removed from the pool by the background evictor (see
    /// [`ConnectionPool::start_idle_evictor`]). `None` disables eviction.
    ///
    /// Default: 9 minutes (`connections.max.idle.ms = 540_000`), matching
    /// the Apache Kafka Java client.
    #[must_use]
    pub fn with_max_idle(mut self, max_idle: Option<Duration>) -> Self {
        self.max_idle = max_idle;
        self
    }

    /// Returns the configured idle-eviction timeout.
    #[inline]
    pub fn max_idle(&self) -> Option<Duration> {
        self.max_idle
    }

    /// Set a cap on the total number of live connections across all brokers.
    ///
    /// A call that would open a connection beyond `limit` fails with a
    /// retriable [`KrafkaError::Network`] naming the cap and the address.
    /// Replacing a dead or session-expired connection is not growth and is
    /// never refused. A coordination connection that would exceed the cap
    /// falls back to the data connection for the same address.
    ///
    /// `None` (default) removes the cap.
    #[must_use]
    pub fn with_max_total_connections(mut self, limit: impl Into<Option<usize>>) -> Self {
        self.max_total_connections = limit.into();
        self
    }

    /// Returns the configured total-connection cap, if any.
    #[inline]
    pub fn max_total_connections(&self) -> Option<usize> {
        self.max_total_connections
    }

    /// Re-read TLS certificate files from disk and atomically update the
    /// shared connector used by all future connections and reconnections.
    ///
    /// Existing TLS sessions are unaffected. On error the previous connector
    /// remains active.
    pub async fn refresh_tls(&self) -> crate::error::Result<()> {
        self.config.refresh_tls().await
    }

    /// Get or create the data connection to a broker by address.
    ///
    /// Makes at most one connection attempt, bounded by `connect_timeout`.
    /// Inside the address's reconnect backoff window it fails immediately
    /// with a retriable error. Dropping the returned future abandons the wait
    /// but not the dial, whose connection is kept for the next caller.
    pub async fn get_connection(&self, address: &str) -> Result<Arc<BrokerConnection>> {
        self.get(address, ConnectionPurpose::Data).await
    }

    /// Get or create the coordination connection to a group coordinator.
    ///
    /// A separate connection from the data connection to the same address,
    /// so coordination requests are never queued behind data requests. When
    /// the connection cap leaves no room for it, the data connection is used
    /// instead; the pool's `coordination_fallbacks` counter counts those
    /// calls and the pool logs one warning.
    pub async fn get_coordinator_connection(&self, address: &str) -> Result<Arc<BrokerConnection>> {
        self.get(address, ConnectionPurpose::Coordination).await
    }

    /// Get or create the data connection to a broker by ID.
    ///
    /// The connection is also registered under the broker ID. A cached entry
    /// is used only if it still reaches `address`: a broker keeps its node ID
    /// across a move (a Kubernetes reschedule, a changed
    /// `advertised.listeners`), and a connection to the old endpoint would
    /// misroute every request for it.
    pub async fn get_connection_by_id(
        &self,
        broker_id: BrokerId,
        address: &str,
    ) -> Result<Arc<BrokerConnection>> {
        {
            let s = self.state.read();
            if let Some(conn) = s.by_id.get(&broker_id)
                && conn.is_usable()
                && conn.address() == address
            {
                return Ok(conn.clone());
            }
        }

        let conn = self.get(address, ConnectionPurpose::Data).await?;
        self.state.write().by_id.insert(broker_id, conn.clone());
        Ok(conn)
    }

    async fn get(
        &self,
        address: &str,
        purpose: ConnectionPurpose,
    ) -> Result<Arc<BrokerConnection>> {
        let key = (address.to_string(), purpose);
        {
            let s = self.state.read();
            if let Some(conn) = s.by_key.get(&key)
                && conn.is_usable()
            {
                return Ok(conn.clone());
            }
        }

        let rx = match self.start_dial(key)? {
            DialStart::Ready(conn) => return Ok(conn),
            DialStart::Wait(rx) => rx,
            DialStart::CapReached(limit) => {
                if purpose == ConnectionPurpose::Coordination {
                    self.record_coordination_fallback(address, limit);
                    return Box::pin(self.get(address, ConnectionPurpose::Data)).await;
                }
                return Err(connection_cap_error(limit, address));
            }
        };
        rx.await.map_err(|_| {
            KrafkaError::network(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                format!("connection attempt to {address} was abandoned"),
            ))
        })?
    }

    /// Under the write lock: reuse, join a running dial, fail fast inside the
    /// backoff window, refuse at the cap, or start a dial.
    fn start_dial(&self, key: PoolKey) -> Result<DialStart> {
        let now = Instant::now();
        let mut s = self.state.write();

        if let Some(conn) = s.by_key.get(&key)
            && conn.is_usable()
        {
            return Ok(DialStart::Ready(conn.clone()));
        }
        if let Some(waiters) = s.dialing.get_mut(&key) {
            let (tx, rx) = oneshot::channel();
            waiters.push(tx);
            return Ok(DialStart::Wait(rx));
        }
        if let Some(state) = s.reconnect.get(&key.0)
            && now < state.next_attempt_at
        {
            return Err(state.error_in_window(&key.0, now));
        }

        // The entry being replaced is dead or past its re-authentication
        // point. It leaves the pool before the cap is checked, so a
        // replacement never counts as growth.
        if let Some(stale) = s.by_key.remove(&key) {
            s.by_id.retain(|_, c| !Arc::ptr_eq(c, &stale));
            if stale.is_alive() {
                info!(
                    address = %key.0,
                    purpose = ?key.1,
                    "Replacing connection due to SASL session expiry (KIP-368)"
                );
                stale.close_when_idle();
            }
        }

        if let Some(limit) = self.max_total_connections
            && s.counted_connections() >= limit
        {
            return Ok(DialStart::CapReached(limit));
        }

        let (tx, rx) = oneshot::channel();
        s.dialing.insert(key.clone(), vec![tx]);
        let generation = s.generation;
        drop(s);

        self.spawn_dial(key, generation);
        Ok(DialStart::Wait(rx))
    }

    /// Dial in a task of its own, so a caller that stops waiting does not
    /// cancel the attempt other callers share.
    fn spawn_dial(&self, key: PoolKey, generation: u64) {
        let state = Arc::clone(&self.state);
        let config = self.config.clone();
        tokio::spawn(async move {
            let connect_timeout = config.connect_timeout;
            let address = key.0.clone();
            let result = match tokio::time::timeout(
                connect_timeout,
                BrokerConnection::connect(&address, config),
            )
            .await
            {
                Ok(Ok(conn)) => Ok(Arc::new(conn)),
                Ok(Err(e)) => Err(e),
                Err(_) => Err(KrafkaError::timeout(format!(
                    "connection to {address} was not established within {connect_timeout:?}"
                ))),
            };

            let (waiters, result) = {
                let mut s = state.write();
                let waiters = s.dialing.remove(&key).unwrap_or_default();
                let result = match result {
                    Ok(conn) if s.generation != generation => {
                        conn.close_when_idle();
                        Err(pool_closed_error(&address))
                    }
                    Ok(conn) => {
                        s.reconnect.remove(&address);
                        s.by_key.insert(key, conn.clone());
                        Ok(conn)
                    }
                    Err(e) => {
                        let failures = s.reconnect.get(&address).map_or(0, |r| r.failures) + 1;
                        let backoff = RECONNECT_BACKOFF.calculate_backoff(failures);
                        warn!(
                            address = %address,
                            failures,
                            backoff_ms = backoff.as_millis() as u64,
                            error = %e,
                            "Connection attempt failed"
                        );
                        s.reconnect.insert(
                            address,
                            ReconnectState {
                                failures,
                                next_attempt_at: Instant::now() + backoff,
                                last_error: e.clone(),
                            },
                        );
                        Err(e)
                    }
                };
                (waiters, result)
            };
            for waiter in waiters {
                let _ = waiter.send(result.clone());
            }
        });
    }

    fn record_coordination_fallback(&self, address: &str, limit: usize) {
        self.recorder().record_coordination_fallback();
        let first = {
            let mut s = self.state.write();
            !std::mem::replace(&mut s.fallback_warned, true)
        };
        if first {
            warn!(
                address = %address,
                max_total_connections = limit,
                "Connection cap reached: coordination requests share the data connection; \
                 heartbeats may wait behind fetches. Raise the cap to isolate them."
            );
        }
    }

    /// Remove connections that have sat idle for at least `max_idle`.
    ///
    /// Entries are removed from both indexes (by broker ID and by address and
    /// purpose). Each evicted connection is then closed explicitly, so the
    /// socket is torn down promptly even if other `Arc` clones of it exist.
    ///
    /// Returns the number of *unique* connections evicted. A single socket
    /// registered under both a broker ID and a bootstrap address counts
    /// once: the collected `Arc`s are deduplicated by pointer identity
    /// before the count is returned, so the `debug!` log and the return
    /// value reflect distinct sockets. No-op when `max_idle` is `None`.
    ///
    /// Safe to call concurrently with `get_connection_by_id` /
    /// `get_bootstrap_connection`: any connection re-inserted between the
    /// scan and the removal step is re-checked under the write lock, so
    /// newly installed connections are never accidentally evicted.
    pub fn evict_idle(&self) -> usize {
        let Some(max_idle) = self.max_idle else {
            return 0;
        };

        // Single write lock covers both maps atomically.
        // Re-check each candidate under the lock: another task may have
        // refreshed `last_used_nanos` (or replaced the entry) between the
        // idle check and here — `remove` + re-insert on miss preserves
        // freshly-used connections.
        let mut removed: Vec<Arc<BrokerConnection>> = Vec::new();
        {
            let mut s = self.state.write();

            // Collect stale IDs first to avoid borrow conflicts.
            let stale_ids: Vec<BrokerId> = s
                .by_id
                .iter()
                .filter(|(_, c)| c.idle_duration() >= max_idle)
                .map(|(id, _)| *id)
                .collect();
            for id in stale_ids {
                if let Some(c) = s.by_id.remove(&id) {
                    if c.idle_duration() >= max_idle {
                        removed.push(c);
                    } else {
                        s.by_id.insert(id, c);
                    }
                }
            }

            let stale_keys: Vec<PoolKey> = s
                .by_key
                .iter()
                .filter(|(_, c)| c.idle_duration() >= max_idle)
                .map(|(key, _)| key.clone())
                .collect();
            for key in stale_keys {
                if let Some(c) = s.by_key.remove(&key) {
                    if c.idle_duration() >= max_idle {
                        removed.push(c);
                    } else {
                        s.by_key.insert(key, c);
                    }
                }
            }
        }

        if removed.is_empty() {
            return 0;
        }

        // A single connection typically lives in both maps (same `Arc`
        // registered under broker id and bootstrap address). Dedup by
        // `Arc::as_ptr` so the eviction count reflects unique sockets
        // and `Drop` runs once per connection without inflation.
        removed.sort_by_key(|c| Arc::as_ptr(c) as usize);
        removed.dedup_by(|a, b| Arc::ptr_eq(a, b));

        let count = removed.len();
        debug!(
            evicted = count,
            max_idle_ms = max_idle.as_millis(),
            "Evicted idle connections"
        );
        for conn in removed {
            conn.close_now();
        }
        count
    }

    /// Spawn a background task that periodically calls [`Self::evict_idle`].
    ///
    /// Idempotent: a second call while a previous evictor is still running
    /// aborts the previous handle before installing the new one. The task
    /// is automatically aborted by [`Self::close_all`].
    ///
    /// The sweep interval is `max_idle / 9`, clamped to a minimum of 1 s
    /// and a maximum of 60 s. This matches the Java client's approach: a
    /// connection may sit idle for up to `max_idle + interval` before
    /// removal, so a fractional-sweep keeps actual idle time close to
    /// the configured bound.
    ///
    /// No-op when `max_idle` is `None` or when called outside a Tokio
    /// runtime context. The runtime guard keeps the library panic-free
    /// for integrations that construct a pool without `tokio::spawn`
    /// being available (e.g. ad-hoc tests or synchronous tooling); such
    /// callers simply lose the background sweep and can call
    /// [`Self::evict_idle`] explicitly instead.
    pub fn start_idle_evictor(self: &Arc<Self>) {
        // Started unconditionally and *first*: OAUTHBEARER refresh is not part
        // of idle eviction and must survive both early returns below
        // (`max_idle == None` is a supported configuration). Coupling the two
        // meant the cached token was never refreshed, so every reconnect after
        // the real `exp` re-sent a dead JWT and locked the client out.
        self.start_token_refresh();

        let Some(max_idle) = self.max_idle else {
            return;
        };
        // Guard against being called outside a Tokio runtime so that
        // `tokio::spawn` never panics. This mirrors the `BrokerConnection`
        // Drop path, which also checks for a live runtime before spawning.
        if tokio::runtime::Handle::try_current().is_err() {
            warn!("start_idle_evictor called outside a Tokio runtime; idle eviction disabled");
            return;
        }
        // Sweep about 9× during one idle window, clamped to sensible bounds.
        // 9 is the same divisor the Java client uses.
        let interval = (max_idle / 9)
            .max(Duration::from_secs(1))
            .min(Duration::from_secs(60));

        // Dead-man-switch: if the pool is dropped the weak upgrade fails
        // and the task exits cleanly on its next tick.
        let weak = Arc::downgrade(self);
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // Skip the immediate fire; the first eviction happens after
            // `interval`, not at startup.
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(pool) = weak.upgrade() else {
                    break;
                };
                pool.evict_idle();
            }
        });
        if let Some(prev) = self.evictor_handle.lock().replace(handle) {
            prev.abort();
        }
    }

    /// Start the background OAUTHBEARER proactive token-refresh task.
    ///
    /// No-op unless the pool's [`AuthConfig`](crate::auth::AuthConfig) carries
    /// an OAUTHBEARER *provider* (a static token has nothing to refresh), and
    /// no-op outside a Tokio runtime.
    ///
    /// Idempotent: a second call aborts the previous task before installing
    /// the new one. [`Self::close_all`] aborts it.
    ///
    /// Deliberately **independent of idle eviction**. Token refresh is a
    /// credential-lifetime concern, not a connection-hygiene one, and must run
    /// even when `max_idle` is `None`. [`Self::start_idle_evictor`] and
    /// [`Self::start`] both call it, so most callers never need it directly.
    pub fn start_token_refresh(self: &Arc<Self>) {
        let Some(provider) = self
            .config
            .auth
            .as_ref()
            .and_then(|a| a.oauthbearer_provider())
        else {
            return;
        };
        // Bind before the runtime check: token fetches on the *connection*
        // path happen with or without a background refresh task, and they are
        // the ones an operator most needs counted.
        provider.bind_metrics(Arc::clone(self.recorder()));

        if tokio::runtime::Handle::try_current().is_err() {
            warn!(
                "start_token_refresh called outside a Tokio runtime; OAUTHBEARER \
                 proactive refresh disabled. Tokens are still refreshed lazily on \
                 the connection path."
            );
            return;
        }
        let refresh_handle = provider.start_refresh_task();
        if let Some(prev) = self.oauth_refresh_handle.lock().replace(refresh_handle) {
            prev.abort();
        }
    }

    /// Start the background TLS certificate reload task (KIP-1288).
    ///
    /// Every `interval`, re-reads the configured certificate, key and
    /// trust-store files from disk and atomically swaps the connector used by
    /// all *future* connections. Existing TLS sessions keep the connector they
    /// handshaked with.
    ///
    /// No-op unless the pool's `AuthConfig` carries a TLS configuration —
    /// there is nothing on disk to reload otherwise — and no-op outside a Tokio
    /// runtime.
    ///
    /// A failed reload (file missing mid-rotation, half-written PEM) is logged
    /// at `warn!` and the previous connector stays active, so a non-atomic
    /// rotation converges on the next tick instead of breaking every new
    /// connection in between.
    ///
    /// Idempotent: a second call aborts the previous task. [`Self::close_all`]
    /// aborts it. Started automatically by
    /// [`TransportConfig::tls_reload_interval`](super::TransportConfig::tls_reload_interval).
    pub fn start_tls_reload(self: &Arc<Self>, interval: Duration) {
        if interval.is_zero() {
            warn!("start_tls_reload called with a zero interval; ignoring");
            return;
        }
        if self
            .config
            .auth
            .as_ref()
            .and_then(|a| a.tls_config.as_ref())
            .is_none()
        {
            return;
        }
        if tokio::runtime::Handle::try_current().is_err() {
            warn!(
                "start_tls_reload called outside a Tokio runtime; automatic TLS \
                 reloading disabled. Call `refresh_tls()` explicitly instead."
            );
            return;
        }

        // Dead-man switch: once the pool is dropped the weak upgrade fails and
        // the task exits on its next tick rather than holding the pool alive.
        let weak = Arc::downgrade(self);
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            ticker.tick().await; // skip the immediate fire
            loop {
                ticker.tick().await;
                let Some(pool) = weak.upgrade() else {
                    break;
                };
                match pool.refresh_tls().await {
                    Ok(()) => debug!("Periodic TLS reload completed"),
                    Err(e) => warn!(
                        error = %e,
                        "Periodic TLS reload failed; keeping the previously loaded \
                         certificates and retrying on the next tick"
                    ),
                }
            }
        });
        if let Some(prev) = self.tls_reload_handle.lock().replace(handle) {
            prev.abort();
        }
    }

    /// Close all connections and empty the pool.
    ///
    /// Stops the background tasks, removes every connection under one write
    /// lock, fails callers waiting on a dial in progress (whose connection,
    /// once established, is closed rather than installed), then closes each
    /// connection outside the lock.
    // Async so it composes with the clients' async shutdown paths.
    #[allow(clippy::unused_async)]
    pub async fn close_all(&self) {
        if let Some(handle) = self.evictor_handle.lock().take() {
            handle.abort();
        }
        if let Some(handle) = self.oauth_refresh_handle.lock().take() {
            handle.abort();
        }
        if let Some(handle) = self.tls_reload_handle.lock().take() {
            handle.abort();
        }

        let (connections, dialing) = {
            let mut s = self.state.write();
            s.generation += 1;
            s.reconnect.clear();
            let mut connections: Vec<_> = s.by_id.drain().map(|(_, c)| c).collect();
            connections.extend(s.by_key.drain().map(|(_, c)| c));
            (connections, std::mem::take(&mut s.dialing))
        };

        for ((address, _), waiters) in dialing {
            let err = pool_closed_error(&address);
            for waiter in waiters {
                let _ = waiter.send(Err(err.clone()));
            }
        }

        // A connection registered under a broker ID is also in `by_key`;
        // closing twice is harmless.
        for conn in connections {
            conn.close_now();
        }
    }

    /// Number of usable connections known by broker ID.
    ///
    /// Bootstrap connections that have not yet been associated with a broker
    /// ID (i.e. only in the address map) are **not** counted.
    pub fn len(&self) -> usize {
        let s = self.state.read();
        s.by_id.values().filter(|c| c.is_usable()).count()
    }

    /// Returns `true` if no usable connections known by broker ID exist.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// The error for a connection the cap leaves no room for. Retriable: a
/// connection may close or be evicted.
fn connection_cap_error(limit: usize, address: &str) -> KrafkaError {
    KrafkaError::network(std::io::Error::other(format!(
        "connection pool limit reached: {limit} connections open, none to {address} \
         (raise `max_connections` or reduce the number of brokers in use)"
    )))
}

fn pool_closed_error(address: &str) -> KrafkaError {
    KrafkaError::network(std::io::Error::new(
        std::io::ErrorKind::ConnectionAborted,
        format!("pool closed while connecting to {address}"),
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_pool_new() {
        let pool = ConnectionPool::new(ConnectionConfig::default());
        // Just verify it creates without error
        let _ = pool;
    }

    #[tokio::test]
    async fn test_pool_close_all_clears_both_maps() {
        let pool = ConnectionPool::new(ConnectionConfig::default());
        {
            let s = pool.state.read();
            assert!(s.by_id.is_empty());
            assert!(s.by_key.is_empty());
        }
        // close_all on empty pool should not panic
        pool.close_all().await;
    }

    #[test]
    fn test_max_idle_default_matches_java_client() {
        // 9 minutes = 540_000 ms, matching Apache Kafka Java client's
        // default `connections.max.idle.ms`.
        let pool = ConnectionPool::new(ConnectionConfig::default());
        assert_eq!(pool.max_idle(), Some(Duration::from_millis(9 * 60 * 1000)));
        assert_eq!(DEFAULT_MAX_IDLE, Duration::from_secs(540));
    }

    #[test]
    fn test_with_max_idle_none_disables_eviction() {
        let pool = ConnectionPool::new(ConnectionConfig::default()).with_max_idle(None);
        assert_eq!(pool.max_idle(), None);
        // `evict_idle` is a no-op when disabled.
        assert_eq!(pool.evict_idle(), 0);
    }

    #[test]
    fn test_evict_idle_on_empty_pool_is_noop() {
        let pool = ConnectionPool::new(ConnectionConfig::default());
        assert_eq!(pool.evict_idle(), 0);
    }

    #[tokio::test]
    async fn test_start_idle_evictor_installs_and_aborts_task() {
        let pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
        assert!(pool.evictor_handle.lock().is_none());
        pool.start_idle_evictor();
        assert!(pool.evictor_handle.lock().is_some());

        // Idempotent: second call replaces the handle.
        pool.start_idle_evictor();
        assert!(pool.evictor_handle.lock().is_some());

        // close_all aborts.
        pool.close_all().await;
        assert!(pool.evictor_handle.lock().is_none());
    }

    #[tokio::test]
    async fn test_start_idle_evictor_noop_when_max_idle_disabled() {
        let pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()).with_max_idle(None));
        pool.start_idle_evictor();
        assert!(pool.evictor_handle.lock().is_none());
    }

    #[test]
    fn test_start_idle_evictor_noop_outside_tokio_runtime() {
        // No `#[tokio::test]`: this synchronous test runs without a runtime,
        // so `start_idle_evictor` must take the `Handle::try_current()` early
        // return rather than panic inside `tokio::spawn`.
        let pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
        pool.start_idle_evictor();
        assert!(
            pool.evictor_handle.lock().is_none(),
            "evictor must not be installed without a Tokio runtime"
        );
    }

    #[test]
    fn test_evict_idle_removes_stale_from_both_maps() {
        // Stub connection is idle for 10 s; max_idle is 100 ms, so the
        // entry is stale in both indexes.
        let pool = ConnectionPool::new(ConnectionConfig::default())
            .with_max_idle(Some(Duration::from_millis(100)));
        let stale = Arc::new(BrokerConnection::test_stub_idle_for(
            "b1:9092",
            Duration::from_secs(10),
        ));
        {
            let mut s = pool.state.write();
            s.by_id.insert(1, stale.clone());
            s.by_key
                .insert(("b1:9092".to_string(), ConnectionPurpose::Data), stale);
        }

        // Same socket shared across both maps must dedup to a single
        // eviction.
        assert_eq!(pool.evict_idle(), 1);
        {
            let s = pool.state.read();
            assert!(s.by_id.is_empty());
            assert!(s.by_key.is_empty());
        }
    }

    #[test]
    fn test_evict_idle_retains_fresh_and_evicts_stale() {
        let pool = ConnectionPool::new(ConnectionConfig::default())
            .with_max_idle(Some(Duration::from_millis(100)));
        let stale = Arc::new(BrokerConnection::test_stub_idle_for(
            "b1:9092",
            Duration::from_secs(10),
        ));
        let fresh = Arc::new(BrokerConnection::test_stub_idle_for(
            "b2:9092",
            Duration::from_millis(10),
        ));
        {
            let mut s = pool.state.write();
            s.by_id.insert(1, stale);
            s.by_id.insert(2, fresh);
        }

        assert_eq!(pool.evict_idle(), 1);
        let s = pool.state.read();
        assert!(!s.by_id.contains_key(&1));
        assert!(s.by_id.contains_key(&2));
    }

    #[test]
    fn test_evict_idle_rescued_after_refresh() {
        // Pin the freshness side of the contract: a connection that has
        // been marked used is not evicted even if its `created_at` is old.
        // This covers the same code path the write-lock re-check uses
        // (a refresh invalidates the stale decision).
        let pool = ConnectionPool::new(ConnectionConfig::default())
            .with_max_idle(Some(Duration::from_millis(100)));
        let conn = Arc::new(BrokerConnection::test_stub_idle_for(
            "b1:9092",
            Duration::from_secs(10),
        ));
        conn.test_mark_fresh();
        pool.state.write().by_id.insert(1, conn);

        assert_eq!(pool.evict_idle(), 0);
        assert!(pool.state.read().by_id.contains_key(&1));
    }

    #[test]
    fn test_max_total_connections_default_is_none() {
        let pool = ConnectionPool::new(ConnectionConfig::default());
        assert_eq!(pool.max_total_connections(), None);
    }

    #[test]
    fn test_with_max_total_connections_sets_limit() {
        let pool =
            ConnectionPool::new(ConnectionConfig::default()).with_max_total_connections(10usize);
        assert_eq!(pool.max_total_connections(), Some(10));
    }

    #[test]
    fn test_with_max_total_connections_none_removes_limit() {
        let pool = ConnectionPool::new(ConnectionConfig::default())
            .with_max_total_connections(5usize)
            .with_max_total_connections(None);
        assert_eq!(pool.max_total_connections(), None);
    }

    // ── OAUTHBEARER refresh must not be coupled to idle eviction ───────

    fn oauth_pool_config() -> ConnectionConfig {
        ConnectionConfig::builder()
            .auth(crate::auth::AuthConfig::sasl_oauthbearer_provider(
                || async { Ok(crate::auth::OAuthBearerToken::new("jwt")) },
            ))
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_token_refresh_starts_even_when_idle_eviction_disabled() {
        // `with_max_idle(None)` is a supported configuration; it must not skip
        // `start_refresh_task`, or one JWT stays cached for the process lifetime.
        let pool = Arc::new(ConnectionPool::new(oauth_pool_config()).with_max_idle(None));
        pool.start_idle_evictor();

        assert!(
            pool.evictor_handle.lock().is_none(),
            "eviction is disabled, as configured"
        );
        assert!(
            pool.oauth_refresh_handle.lock().is_some(),
            "token refresh must run regardless of max_idle"
        );
        pool.close_all().await;
    }

    #[tokio::test]
    async fn test_token_refresh_starts_alongside_idle_evictor() {
        let pool = Arc::new(ConnectionPool::new(oauth_pool_config()));
        pool.start_idle_evictor();
        assert!(pool.evictor_handle.lock().is_some());
        assert!(pool.oauth_refresh_handle.lock().is_some());
        pool.close_all().await;
        assert!(
            pool.oauth_refresh_handle.lock().is_none(),
            "aborted on close"
        );
    }

    #[tokio::test]
    async fn test_start_token_refresh_is_noop_without_provider() {
        // A static token has nothing to refresh.
        let pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
        pool.start_token_refresh();
        assert!(pool.oauth_refresh_handle.lock().is_none());
    }

    #[test]
    fn test_start_token_refresh_is_noop_outside_runtime() {
        let pool = Arc::new(ConnectionPool::new(oauth_pool_config()));
        pool.start_token_refresh();
        assert!(
            pool.oauth_refresh_handle.lock().is_none(),
            "must not panic in tokio::spawn without a runtime"
        );
    }

    /// Token fetches must land on the pool's own `ConnectionRecorder`.
    ///
    /// The counters are useless if nothing binds them: an OAUTHBEARER provider
    /// is called per connection, and a misconfigured `token_endpoint` is
    /// otherwise indistinguishable from an unreachable broker.
    ///
    /// Negative control: removing the `bind_metrics` call from
    /// `start_token_refresh` leaves the counter at zero and this fails.
    #[tokio::test]
    async fn token_fetches_are_reported_to_the_pools_metrics() {
        let config = oauth_pool_config();
        let pool = Arc::new(ConnectionPool::new(config));
        pool.start_token_refresh();

        let provider = pool
            .config
            .auth
            .as_ref()
            .and_then(|a| a.oauthbearer_provider())
            .expect("the config carries a provider")
            .clone();
        provider.provide_token().await.expect("provider succeeds");

        assert_eq!(
            pool.metrics().oauth_token_fetches,
            1,
            "the connection-path fetch must reach the pool's metrics"
        );
        pool.close_all().await;
    }

    /// Binding must happen even outside a Tokio runtime, where the background
    /// refresh task cannot start — the connection path still fetches there.
    #[test]
    fn metrics_are_bound_even_when_the_refresh_task_cannot_start() {
        let pool = Arc::new(ConnectionPool::new(oauth_pool_config()));
        pool.start_token_refresh();

        let provider = pool
            .config
            .auth
            .as_ref()
            .and_then(|a| a.oauthbearer_provider())
            .expect("the config carries a provider")
            .clone();

        let metrics = Arc::clone(pool.recorder());
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime")
            .block_on(async { provider.provide_token().await })
            .expect("provider succeeds");

        assert_eq!(metrics.oauth_token_fetches.get(), 1);
    }

    // ── Replacement at the cap, backoff reset ──────────────────────────

    /// A connection past its KIP-368 re-authentication point is replaced
    /// even when it fills the cap, and the old one is asked to close.
    ///
    /// Reverted-line control: with the stale entry left in `by_key` until
    /// after the cap check, `counted_connections` is 1 and this fails with
    /// the cap error.
    #[cfg(feature = "test-broker")]
    #[tokio::test]
    async fn a_session_expired_connection_is_replaced_at_the_cap() {
        let broker = crate::testing::FakeBroker::start().await.unwrap();
        let addr = broker.bootstrap_servers();
        let pool = ConnectionPool::new(ConnectionConfig::default()).with_max_total_connections(1);
        let expired = Arc::new(BrokerConnection::test_stub_session_expired(&addr));
        assert!(expired.is_alive() && !expired.is_usable());
        pool.state
            .write()
            .by_key
            .insert((addr.clone(), ConnectionPurpose::Data), expired.clone());

        let fresh = pool
            .get_connection(&addr)
            .await
            .expect("replaced, not refused");
        assert!(!Arc::ptr_eq(&expired, &fresh));
        assert!(fresh.is_usable());
    }

    /// A successful dial clears the address's backoff state.
    #[cfg(feature = "test-broker")]
    #[tokio::test]
    async fn a_successful_dial_resets_the_backoff() {
        let broker = crate::testing::FakeBroker::start().await.unwrap();
        let addr = broker.bootstrap_servers();
        let pool = ConnectionPool::new(ConnectionConfig::default());
        pool.state.write().reconnect.insert(
            addr.clone(),
            ReconnectState {
                failures: 4,
                next_attempt_at: Instant::now(),
                last_error: KrafkaError::timeout("earlier failure"),
            },
        );
        pool.get_connection(&addr).await.unwrap();
        assert!(!pool.state.read().reconnect.contains_key(&addr));
    }

    /// Inside the window a non-retriable dial failure is repeated as it was,
    /// so backoff never hides an authentication or configuration error.
    #[tokio::test]
    async fn the_backoff_window_repeats_a_non_retriable_error() {
        let pool = ConnectionPool::new(ConnectionConfig::default());
        pool.state.write().reconnect.insert(
            "b1:9092".to_string(),
            ReconnectState {
                failures: 1,
                next_attempt_at: Instant::now() + Duration::from_secs(60),
                last_error: KrafkaError::auth("bad credentials"),
            },
        );
        let err = pool
            .get_connection("b1:9092")
            .await
            .err()
            .expect("fails inside the window");
        assert!(matches!(err, KrafkaError::Auth { .. }), "{err:?}");
    }

    /// The periodic TLS reload first loads the files `tls_reload_interval`
    /// after it starts, not before.
    #[tokio::test(start_paused = true)]
    async fn tls_reload_fires_at_its_interval() {
        use crate::auth::{AuthConfig, TlsConfig};
        let ca = format!("{}/src/auth/testdata/ca.pem", env!("CARGO_MANIFEST_DIR"));
        let config = ConnectionConfig::builder()
            .auth(AuthConfig::ssl(TlsConfig::new().with_ca_cert(ca)))
            .build()
            .unwrap();
        let pool = Arc::new(ConnectionPool::new(config));
        let interval = Duration::from_secs(60);
        let loaded = |pool: &ConnectionPool| pool.config.tls_connector.load().is_some();
        let start = Instant::now();
        pool.start_tls_reload(interval);

        tokio::time::sleep_until(start + interval - Duration::from_millis(1)).await;
        assert!(!loaded(&pool), "reloaded before the interval");
        // The reload reads the files on the blocking pool, which holds the
        // paused clock until it is done.
        tokio::time::sleep_until(start + interval).await;
        while !loaded(&pool) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            start.elapsed() <= interval + Duration::from_millis(5),
            "reloaded {:?} after start",
            start.elapsed()
        );
    }
}
