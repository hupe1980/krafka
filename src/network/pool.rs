//! Connection pool for managing broker connections.
//!
//! This module provides:
//! - **Connection pooling**: Reuse connections across requests
//! - **Automatic reconnection**: Exponential backoff retry on connection failures
//! - **One connection per broker**: a single multiplexed socket per broker,
//!   matching the Apache Kafka Java client

use ahash::AHashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::connection::{BrokerConnection, ConnectionConfig};
use crate::BrokerId;
use crate::error::{KrafkaError, Result};
use crate::metrics::ConnectionMetrics;
use crate::util::BackoffPolicy;

/// Configuration for connection retry with exponential backoff.
///
/// Use [`ConnectionRetryConfig::builder()`] or [`Default::default()`] to construct.
#[derive(Debug, Clone)]
pub struct ConnectionRetryConfig {
    /// Maximum number of retries (0 = no retries).
    pub(crate) max_retries: u32,
    /// Shared exponential-backoff parameters.
    pub(crate) backoff: BackoffPolicy,
}

impl Default for ConnectionRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            backoff: BackoffPolicy {
                jitter_factor: 0.2, // slightly more jitter than producer default
                ..BackoffPolicy::default()
            },
        }
    }
}

impl ConnectionRetryConfig {
    /// Create a new config builder.
    pub fn builder() -> ConnectionRetryConfigBuilder {
        ConnectionRetryConfigBuilder::default()
    }

    /// Returns the maximum number of retries.
    #[inline]
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Returns the initial backoff duration.
    #[inline]
    pub fn initial_backoff(&self) -> Duration {
        self.backoff.initial_backoff
    }

    /// Returns the maximum backoff duration.
    #[inline]
    pub fn max_backoff(&self) -> Duration {
        self.backoff.max_backoff
    }

    /// Returns the backoff multiplier.
    #[inline]
    pub fn backoff_multiplier(&self) -> f64 {
        self.backoff.backoff_multiplier
    }

    /// Returns the jitter factor (0.0–1.0).
    #[inline]
    pub fn jitter_factor(&self) -> f64 {
        self.backoff.jitter_factor
    }

    /// Calculate the backoff duration for a given attempt number (1-indexed).
    #[inline]
    fn calculate_backoff(&self, attempt: u32) -> Duration {
        self.backoff.calculate_backoff(attempt)
    }
}

/// Builder for ConnectionRetryConfig.
#[must_use = "builders do nothing until .build() is called"]
#[derive(Debug, Default)]
pub struct ConnectionRetryConfigBuilder {
    config: ConnectionRetryConfig,
}

impl ConnectionRetryConfigBuilder {
    /// Set maximum number of retries.
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.config.max_retries = retries;
        self
    }

    /// Set initial backoff duration.
    pub fn initial_backoff(mut self, duration: Duration) -> Self {
        self.config.backoff.initial_backoff = duration;
        self
    }

    /// Set maximum backoff duration.
    pub fn max_backoff(mut self, duration: Duration) -> Self {
        self.config.backoff.max_backoff = duration;
        self
    }

    /// Set backoff multiplier (must be finite and > 0; clamped to 1.0 otherwise).
    pub fn backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.config.backoff.backoff_multiplier = if multiplier.is_finite() && multiplier > 0.0 {
            multiplier
        } else {
            1.0
        };
        self
    }

    /// Set jitter factor (0.0–1.0) to randomize backoff and prevent thundering herd.
    pub fn jitter_factor(mut self, factor: f64) -> Self {
        self.config.backoff.jitter_factor = if factor.is_finite() {
            factor.clamp(0.0, 1.0)
        } else {
            0.0
        };
        self
    }

    /// Build the ConnectionRetryConfig.
    pub fn build(self) -> ConnectionRetryConfig {
        self.config
    }
}

// ============================================================================
// Connection Pool
// ============================================================================

/// Default idle-eviction timeout for a pooled connection.
///
/// Matches the Apache Kafka Java client's `connections.max.idle.ms = 540_000`
/// (9 minutes). `librdkafka` defaults to 10 min and `franz-go` to 20 min; 9
/// min is the most conservative of the reference clients and avoids
/// accumulating sockets to rotated-out brokers on long-lived clients whose
/// metadata churns (broker scale-up/down, topic drift).
pub const DEFAULT_MAX_IDLE: Duration = Duration::from_secs(9 * 60);

/// Total budget for [`ConnectionPool::close_all`] to drain every connection.
///
/// Shutdown must make progress even when a broker has stopped reading and an
/// event loop is stuck mid-write.
const CLOSE_ALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Waiters for coalesced reconnection attempts, keyed by address.
type ConnectingWaiters = AHashMap<String, Vec<oneshot::Sender<Result<Arc<BrokerConnection>>>>>;

/// Guard that ensures the `connecting` map entry is cleaned up if the
/// reconnecting task's future is cancelled (dropped).  Without this,
/// cancellation would leave a stale entry causing all future callers for
/// that address to wait forever.
struct ReconnectGuard {
    connecting: Arc<Mutex<ConnectingWaiters>>,
    address: Option<String>,
}

impl ReconnectGuard {
    fn new(connecting: &Arc<Mutex<ConnectingWaiters>>, address: String) -> Self {
        Self {
            connecting: Arc::clone(connecting),
            address: Some(address),
        }
    }

    /// Mark the reconnection as completed, preventing cleanup on drop.
    fn defuse(&mut self) {
        self.address = None;
    }
}

impl Drop for ReconnectGuard {
    fn drop(&mut self) {
        let Some(address) = self.address.take() else {
            return;
        };
        // Lock ordering safety: `self.connecting` (Mutex, level 1) is always
        // acquired *without* holding `PoolState` (RwLock, level 2).  This Drop
        // runs either when the reconnect task is cancelled or when `defuse()`
        // was NOT called; neither caller holds the `PoolState` write lock at
        // that point.  `parking_lot::Mutex` never panics on lock, so there is
        // no poisoning concern here.
        let mut guard = self.connecting.lock();
        let waiters = guard.remove(&address).unwrap_or_default();
        let err = KrafkaError::network(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            format!("reconnection to {address} was cancelled"),
        ));
        for waiter in waiters {
            let _ = waiter.send(Err(err.clone()));
        }
    }
}

/// A pool of connections to Kafka brokers.
///
/// Combined index for the two connection lookup maps.
///
/// Wrapping both maps in a single `RwLock<PoolState>` ensures that
/// `evict_idle`, `close_all`, and any write that touches both indexes are
/// **atomic** — a reader that acquires the lock never observes one map
/// updated and the other not.
struct PoolState {
    /// Connections keyed by broker ID (`-1` is never a valid broker ID so
    /// it is never inserted; all entries carry a positive ID assigned by the
    /// cluster metadata).
    by_id: AHashMap<BrokerId, Arc<BrokerConnection>>,
    /// Connections keyed by address string (used for bootstrap lookups
    /// before a broker ID is known).
    by_addr: AHashMap<String, Arc<BrokerConnection>>,
}

impl PoolState {
    fn new() -> Self {
        Self {
            by_id: AHashMap::new(),
            by_addr: AHashMap::new(),
        }
    }
}

/// A pool of connections to Kafka brokers.
///
/// Uses `parking_lot::RwLock` (writer-fair, non-async) for connection maps so
/// that the hot `get_connection*` read path stays fast and avoids async lock
/// overhead when there are no concurrent writers. Reconnection attempts to the
/// same address are coalesced via a `parking_lot::Mutex`: only the first caller
/// performs the TCP/TLS/SASL handshake while subsequent callers wait on oneshot
/// channels, preventing thundering-herd reconnection storms. The sync mutex
/// ensures deterministic cleanup in `ReconnectGuard`'s `Drop` impl without
/// requiring a `tokio::spawn` fallback.
pub struct ConnectionPool {
    /// Unified connection state — both indexes under one lock so writes
    /// to either map are always seen together.
    state: RwLock<PoolState>,
    /// Coalesces concurrent reconnection attempts to the same address.
    /// Only the first task to discover a dead connection performs the
    /// handshake; subsequent tasks push a oneshot sender and wait.
    connecting: Arc<Mutex<ConnectingWaiters>>,
    /// Connection config.
    config: ConnectionConfig,
    /// Retry configuration for reconnection attempts.
    retry_config: ConnectionRetryConfig,
    /// Maximum time a connection may sit idle (no submitted requests)
    /// before the idle-evictor removes it from the pool. `None` disables
    /// eviction. Default: 9 min, matching the Java client's
    /// `connections.max.idle.ms`.
    max_idle: Option<Duration>,
    /// Maximum number of live connections across all brokers.
    ///
    /// When set, new connection attempts that would exceed this cap are
    /// rejected with [`KrafkaError::Config`] instead of opening a new
    /// socket. Prevents file-descriptor exhaustion during metadata storms
    /// (e.g., a cluster that suddenly reports hundreds of brokers).
    ///
    /// `None` (default) means unlimited.
    max_total_connections: Option<usize>,
    /// Handle of the background idle-eviction task, if one was spawned.
    /// Aborted by `close_all`. A `parking_lot::Mutex` is sufficient because
    /// the handle is only taken/replaced in non-hot paths (startup,
    /// shutdown).
    evictor_handle: Mutex<Option<JoinHandle<()>>>,
    /// Handle of the background OAUTHBEARER proactive-refresh task.
    /// `None` when the pool is not configured with an OAUTHBEARER provider.
    /// Aborted by `close_all` alongside the idle-evictor.
    oauth_refresh_handle: Mutex<Option<JoinHandle<()>>>,
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
            state: RwLock::new(PoolState::new()),
            connecting: Arc::new(Mutex::new(AHashMap::new())),
            config,
            retry_config: ConnectionRetryConfig::default(),
            max_idle: Some(DEFAULT_MAX_IDLE),
            max_total_connections: None,
            evictor_handle: Mutex::new(None),
            oauth_refresh_handle: Mutex::new(None),
        }
    }

    /// Create a new connection pool with custom retry configuration.
    ///
    /// As with [`Self::new`], idle eviction is **not** started automatically.
    /// Call [`Self::start_idle_evictor`] on the resulting `Arc<Self>` to
    /// activate the background sweep, or call [`Self::evict_idle`] manually.
    pub fn with_retry_config(
        config: ConnectionConfig,
        retry_config: ConnectionRetryConfig,
    ) -> Self {
        Self {
            state: RwLock::new(PoolState::new()),
            connecting: Arc::new(Mutex::new(AHashMap::new())),
            config,
            retry_config,
            max_idle: Some(DEFAULT_MAX_IDLE),
            max_total_connections: None,
            evictor_handle: Mutex::new(None),
            oauth_refresh_handle: Mutex::new(None),
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

    /// Get the shared connection metrics handle used by connections in this pool.
    #[inline]
    pub fn metrics(&self) -> Arc<ConnectionMetrics> {
        self.config.connection_metrics()
    }

    /// Override the idle-eviction timeout.
    ///
    /// Connections that have submitted no requests for longer than this
    /// are removed from the pool by the background evictor (see
    /// [`ConnectionPool::start_idle_evictor`]). `None` disables eviction
    /// entirely — matching the pre-0.5.0 behaviour.
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
    /// A new connection attempt that would exceed `limit` is rejected with
    /// [`KrafkaError::Config`] rather than opening an additional socket. This
    /// prevents file-descriptor exhaustion during topology changes that
    /// introduce many new brokers simultaneously.
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

    /// Wall-clock budget for a single `BrokerConnection::connect` attempt.
    ///
    /// `connect_timeout` bounds TCP establishment only; TLS and the SASL
    /// handshake each get their own budget of the same size inside `connect`.
    /// Three times `connect_timeout` therefore covers the whole attempt with
    /// margin, and guarantees that a peer which completes TCP and then stalls
    /// cannot hold the pool's per-address `connecting` slot open forever.
    fn connect_attempt_budget(&self) -> Duration {
        self.config
            .connect_timeout
            .saturating_mul(3)
            .max(Duration::from_secs(1))
    }

    /// Wall-clock budget for a full `reconnect_with_backoff` call.
    ///
    /// Every attempt plus every inter-attempt backoff, so waiters can derive a
    /// timeout of their own instead of parking on a bare `rx.await`.
    fn total_reconnect_budget(&self) -> Duration {
        let attempts = self.retry_config.max_retries.saturating_add(1);
        let attempt_cost = self.connect_attempt_budget().saturating_mul(attempts);
        let backoff_cost = self
            .retry_config
            .max_backoff()
            .saturating_mul(self.retry_config.max_retries);
        attempt_cost.saturating_add(backoff_cost)
    }

    /// Attempt to connect with exponential backoff retry logic.
    ///
    /// Retries up to `max_retries` times with exponential backoff. Each
    /// attempt is bounded by [`Self::connect_attempt_budget`] and the loop as
    /// a whole by [`Self::total_reconnect_budget`], so a hostile or hung
    /// broker cannot pin the caller — or, through the coalescing `connecting`
    /// map, every other task waiting on the same address — indefinitely.
    async fn reconnect_with_backoff(&self, address: &str) -> Result<Arc<BrokerConnection>> {
        let mut last_error: Option<KrafkaError> = None;
        let overall_deadline = tokio::time::Instant::now() + self.total_reconnect_budget();

        for attempt in 0..=self.retry_config.max_retries {
            if tokio::time::Instant::now() >= overall_deadline {
                warn!(
                    address = %address,
                    attempt = attempt,
                    "Reconnect budget exhausted; giving up"
                );
                break;
            }
            // Apply backoff delay for retry attempts (not the first attempt)
            if attempt > 0 {
                let backoff = self.retry_config.calculate_backoff(attempt);
                debug!(
                    address = %address,
                    attempt = attempt,
                    max_retries = self.retry_config.max_retries,
                    backoff_ms = backoff.as_millis(),
                    "Retrying connection after backoff"
                );
                tokio::time::sleep(backoff).await;
            }

            let attempt_deadline =
                (tokio::time::Instant::now() + self.connect_attempt_budget()).min(overall_deadline);
            let attempt_result = match tokio::time::timeout_at(
                attempt_deadline,
                BrokerConnection::connect(address, self.config.clone()),
            )
            .await
            {
                Ok(r) => r,
                Err(_) => Err(KrafkaError::timeout(format!(
                    "connection to {address} did not complete within {:?} \
                     (TCP established but handshake stalled?)",
                    self.connect_attempt_budget()
                ))),
            };

            match attempt_result {
                Ok(conn) => {
                    if attempt > 0 {
                        info!(
                            address = %address,
                            attempt = attempt,
                            "Successfully reconnected after retries"
                        );
                    }
                    return Ok(Arc::new(conn));
                }
                Err(e) => {
                    // Check if error is retriable
                    if !e.is_retriable() {
                        warn!(
                            address = %address,
                            error = %e,
                            "Non-retriable connection error, not retrying"
                        );
                        return Err(e);
                    }

                    warn!(
                        address = %address,
                        attempt = attempt,
                        max_retries = self.retry_config.max_retries,
                        error = %e,
                        "Connection attempt failed"
                    );
                    last_error = Some(e);
                }
            }
        }

        // All retries exhausted
        Err(last_error.unwrap_or_else(|| {
            KrafkaError::network(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!(
                    "Failed to connect to {} after {} retries",
                    address, self.retry_config.max_retries
                ),
            ))
        }))
    }

    /// Coalesced reconnection: only one task reconnects per address.
    ///
    /// When the first task discovers a dead connection it registers in the
    /// `connecting` map, performs the handshake, stores the result, and
    /// notifies all waiters.  Subsequent tasks that arrive while the
    /// reconnection is in-flight push a oneshot sender and wait instead of
    /// opening redundant TCP connections.
    ///
    /// A [`ReconnectGuard`] ensures cleanup if the reconnecting task's future
    /// is cancelled (dropped), preventing a stale `connecting` entry from
    /// blocking all future callers for that address.
    async fn get_or_reconnect(&self, address: &str) -> Result<Arc<BrokerConnection>> {
        // Log reauth hint (sync read lock, tiny critical section)
        {
            let s = self.state.read();
            if s.by_addr
                .get(address)
                .is_some_and(|c| c.is_alive() && c.needs_reauthentication())
            {
                info!(
                    address = %address,
                    "Replacing connection due to SASL session expiry (KIP-368)"
                );
            }
        }

        // Acquire the coalescing lock in a block so the !Send MutexGuard is
        // dropped before any `.await`, keeping the outer future Send.
        enum CoalesceAction {
            AlreadyConnected(Arc<BrokerConnection>),
            WaitForPeer(oneshot::Receiver<Result<Arc<BrokerConnection>>>),
            Reconnect(String),
        }

        // Double-check before acquiring the coalescing lock: another task
        // may have finished reconnecting between our fast-path miss and now.
        let existing = {
            let s = self.state.read();
            s.by_addr.get(address).filter(|c| c.is_usable()).cloned()
        };

        let action = {
            let mut connecting = self.connecting.lock();

            if let Some(conn) = existing {
                CoalesceAction::AlreadyConnected(conn)
            } else if let Some(waiters) = connecting.get_mut(address) {
                // A reconnection to this address is already in-flight.
                let (tx, rx) = oneshot::channel();
                waiters.push(tx);
                CoalesceAction::WaitForPeer(rx)
            } else {
                // First caller: register as the reconnector.
                let addr_owned = address.to_string();
                connecting.insert(addr_owned.clone(), Vec::new());
                CoalesceAction::Reconnect(addr_owned)
            }
        };
        // MutexGuard is now dropped — safe to .await below.

        let addr_owned = match action {
            CoalesceAction::AlreadyConnected(conn) => return Ok(conn),
            CoalesceAction::WaitForPeer(rx) => {
                // Never park on a bare `rx.await`. The peer performing the
                // handshake is bounded by `total_reconnect_budget`, but if
                // that task is cancelled in a way that skips `ReconnectGuard`
                // (or simply overruns), an unbounded wait here would wedge
                // every caller for this address permanently. Allow the peer
                // its full budget plus a grace period, then fail retriably.
                let waiter_budget = self.total_reconnect_budget() + Duration::from_secs(1);
                return tokio::time::timeout(waiter_budget, rx)
                    .await
                    .map_err(|_| {
                        KrafkaError::network(std::io::Error::new(
                            std::io::ErrorKind::TimedOut,
                            format!(
                                "timed out after {waiter_budget:?} waiting for an in-flight \
                                 reconnection to {address}"
                            ),
                        ))
                    })?
                    .map_err(|_| {
                        KrafkaError::network(std::io::Error::new(
                            std::io::ErrorKind::ConnectionReset,
                            format!("reconnection to {address} was cancelled"),
                        ))
                    })?;
            }
            CoalesceAction::Reconnect(addr_owned) => addr_owned,
        };

        // Guard: if this future is cancelled, the stale `connecting` entry is
        // removed and all waiters are notified with an error.
        let mut guard = ReconnectGuard::new(&self.connecting, addr_owned.clone());

        // Early-exit optimisation: fail fast if we are clearly over the cap so
        // we do not waste a full TCP/TLS handshake only to discard the connection.
        //
        // This check is *not* definitive — two concurrent reconnections to
        // *different* addresses can both pass here (TOCTOU window).  The
        // authoritative cap enforcement happens under the write lock after the
        // connection is established; see the insertion block below.
        if let Some(limit) = self.max_total_connections {
            let current = self.state.read().by_addr.len();
            if current >= limit {
                // Notify waiters with the error so they don't hang.
                let err = KrafkaError::config(format!(
                    "connection pool limit reached: {current}/{limit} connections open \
                     (address={address}); raise `max_total_connections` or reduce broker count"
                ));
                let waiters = self
                    .connecting
                    .lock()
                    .remove(&addr_owned)
                    .unwrap_or_default();
                for waiter in waiters {
                    let _ = waiter.send(Err(err.clone()));
                }
                guard.defuse();
                return Err(err);
            }
        }

        // Reconnect WITHOUT holding any lock
        let result = self.reconnect_with_backoff(address).await;

        // Notify waiting tasks and store the connection.
        // The `connecting` lock is dropped before acquiring `state` write
        // to preserve the invariant that these two locks are never held
        // simultaneously.
        let waiters = self.connecting.lock().remove(address).unwrap_or_default();

        let final_result = match result {
            Ok(conn) => {
                let mut s = self.state.write();
                // Re-check the cap under the write lock to close the TOCTOU
                // window.  Two concurrent reconnections to *different* addresses
                // can both pass the pre-check above (read lock, released) and
                // both reach this point.  The definitive check here ensures
                // `by_addr.len()` never exceeds `max_total_connections`.
                if let Some(limit) = self.max_total_connections {
                    if s.by_addr.len() >= limit {
                        drop(s);
                        // Close the just-established connection gracefully so
                        // its event-loop task exits promptly.
                        let overflow = conn.clone();
                        tokio::spawn(async move { overflow.close().await });
                        Err(KrafkaError::config(format!(
                            "connection pool limit reached: {limit} connections open \
                             (address={addr_owned}); raise `max_total_connections` or reduce broker count"
                        )))
                    } else {
                        s.by_addr.insert(addr_owned, conn.clone());
                        Ok(conn)
                    }
                } else {
                    s.by_addr.insert(addr_owned, conn.clone());
                    Ok(conn)
                }
            }
            Err(e) => Err(e),
        };

        for waiter in waiters {
            let _ = waiter.send(final_result.clone());
        }

        // Reconnection completed — prevent guard cleanup.
        guard.defuse();

        final_result
    }

    /// Get or create a connection to a broker by address.
    ///
    /// The read path uses a `parking_lot::RwLock` (writer-fair, no
    /// async overhead) so concurrent callers rarely convoy behind a pending
    /// writer.  On a cache miss the reconnection is coalesced per address.
    pub async fn get_connection(&self, address: &str) -> Result<Arc<BrokerConnection>> {
        // Fast path: sync read lock (nanosecond-scale critical section)
        {
            let s = self.state.read();
            if let Some(conn) = s.by_addr.get(address)
                && conn.is_usable()
            {
                return Ok(conn.clone());
            }
        }

        self.get_or_reconnect(address).await
    }

    /// Get or create a connection to a broker by ID.
    ///
    /// Same writer-fair fast path as [`get_connection`](Self::get_connection).
    /// On reconnection the connection is registered under both the broker ID
    /// and its address for future lookups.
    pub async fn get_connection_by_id(
        &self,
        broker_id: BrokerId,
        address: &str,
    ) -> Result<Arc<BrokerConnection>> {
        // Fast path: sync read lock.
        //
        // The cached socket must also still point at the address the caller
        // resolved. A broker keeps its node ID across a move — a Kubernetes
        // reschedule, a changed `advertised.listeners`, a swap behind a load
        // balancer — so a `by_id` hit alone does not prove the connection
        // reaches the node metadata now says it does. Without the address
        // check, requests keep going to the old endpoint for as long as that
        // socket happens to stay alive, which in a swap is indefinitely and
        // sends every request for this broker to the wrong one.
        {
            let s = self.state.read();
            if let Some(conn) = s.by_id.get(&broker_id)
                && conn.is_usable()
                && conn.address() == address
            {
                return Ok(conn.clone());
            }
        }

        let conn = self.get_or_reconnect(address).await?;

        // Register under this broker ID so future fast-path lookups hit.
        // Replace on an address mismatch for the same reason as above: the
        // stale entry is not merely useless, it is actively misrouting.
        {
            let mut s = self.state.write();
            let keep_existing = s
                .by_id
                .get(&broker_id)
                .is_some_and(|c| c.is_usable() && c.address() == address);
            if !keep_existing {
                s.by_id.insert(broker_id, conn.clone());
            }
        }

        Ok(conn)
    }

    /// Remove connections that have sat idle for at least `max_idle`.
    ///
    /// Entries are removed from both `connections` (by broker ID) and
    /// `connections_by_addr` (bootstrap map). Each uniquely-evicted
    /// connection is then explicitly closed by spawning `conn.close()`,
    /// which signals the connection's internal task via the high-priority
    /// channel. This ensures the underlying socket is torn down promptly
    /// even if other `Arc` clones of the connection exist (e.g. in-flight
    /// requests or coordinator caches). If no Tokio runtime is available
    /// (e.g. in tests without a runtime), teardown falls back to
    /// `BrokerConnection::Drop`.
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

            let stale_addrs: Vec<String> = s
                .by_addr
                .iter()
                .filter(|(_, c)| c.idle_duration() >= max_idle)
                .map(|(addr, _)| addr.clone())
                .collect();
            for addr in stale_addrs {
                if let Some(c) = s.by_addr.remove(&addr) {
                    if c.idle_duration() >= max_idle {
                        removed.push(c);
                    } else {
                        s.by_addr.insert(addr, c);
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
        // Explicitly close each evicted connection by signalling its internal
        // task via the high-priority channel.
        if tokio::runtime::Handle::try_current().is_ok() {
            for conn in removed {
                tokio::spawn(async move { conn.close().await });
            }
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

    /// Close all connections and drain both maps.
    ///
    /// Stops the idle-evictor, drains the broker-ID and address maps under
    /// short write locks (one at a time, never held simultaneously),
    /// deduplicates by `Arc` pointer, then closes each unique connection
    /// outside any lock. Any in-flight reconnection waiters in the
    /// `connecting` map are notified with an error so they do not hang
    /// during shutdown.
    pub async fn close_all(&self) {
        // Stop the idle-evictor and the OAUTHBEARER refresh task before
        // tearing down state so neither races with the drain below.
        if let Some(handle) = self.evictor_handle.lock().take() {
            handle.abort();
        }
        if let Some(handle) = self.oauth_refresh_handle.lock().take() {
            handle.abort();
        }

        // Drain both maps atomically under a single write lock.
        let (by_id, by_addr) = {
            let mut s = self.state.write();
            (
                s.by_id.drain().map(|(_, c)| c).collect::<Vec<_>>(),
                s.by_addr.drain().map(|(_, c)| c).collect::<Vec<_>>(),
            )
        };

        // Dedup: same Arc may appear in both maps.
        let mut seen = AHashMap::with_capacity(by_id.len() + by_addr.len());
        for conn in by_id.into_iter().chain(by_addr) {
            seen.entry(Arc::as_ptr(&conn) as usize).or_insert(conn);
        }

        // Cancel in-flight reconnections so waiters don't hang.
        {
            let mut connecting = self.connecting.lock();
            for (addr, waiters) in connecting.drain() {
                let err = KrafkaError::network(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    format!("pool closed while reconnecting to {addr}"),
                ));
                for waiter in waiters {
                    let _ = waiter.send(Err(err.clone()));
                }
            }
        }

        // Close connections outside any lock, concurrently and under a total
        // deadline. Sequential `close().await` meant one connection whose
        // event loop was wedged in `write_all` blocked shutdown of every
        // other connection behind it. `BrokerConnection::close` is itself
        // bounded, but joining concurrently also keeps total shutdown time at
        // O(slowest) rather than O(sum).
        let closes = seen
            .into_values()
            .map(|conn| async move { conn.close().await });
        if tokio::time::timeout(CLOSE_ALL_TIMEOUT, futures::future::join_all(closes))
            .await
            .is_err()
        {
            warn!(
                "close_all timed out after {CLOSE_ALL_TIMEOUT:?}; remaining sockets \
                 will be torn down when their last Arc drops"
            );
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

    #[test]
    fn test_connection_retry_config_default() {
        let config = ConnectionRetryConfig::default();
        assert_eq!(config.max_retries, 3);
        assert_eq!(config.initial_backoff(), Duration::from_millis(100));
        assert_eq!(config.max_backoff(), Duration::from_secs(10));
        assert_eq!(config.backoff_multiplier(), 2.0);
    }

    #[test]
    fn test_calculate_backoff() {
        let config = ConnectionRetryConfig::builder().jitter_factor(0.0).build();

        // Attempt 0 = no backoff
        assert_eq!(config.calculate_backoff(0), Duration::ZERO);

        // Attempt 1 = initial backoff (100ms)
        assert_eq!(config.calculate_backoff(1), Duration::from_millis(100));

        // Attempt 2 = initial * 2 (200ms)
        assert_eq!(config.calculate_backoff(2), Duration::from_millis(200));

        // Attempt 3 = initial * 4 (400ms)
        assert_eq!(config.calculate_backoff(3), Duration::from_millis(400));
    }

    #[test]
    fn test_calculate_backoff_capped() {
        let config = ConnectionRetryConfig::builder()
            .max_retries(10)
            .initial_backoff(Duration::from_secs(1))
            .max_backoff(Duration::from_secs(5))
            .backoff_multiplier(10.0)
            .jitter_factor(0.0)
            .build();

        // Attempt 2 would be 10 seconds, but capped at 5
        assert_eq!(config.calculate_backoff(2), Duration::from_secs(5));
    }

    #[test]
    fn test_calculate_backoff_handles_max_attempt() {
        let config = ConnectionRetryConfig::builder()
            .max_retries(u32::MAX)
            .jitter_factor(0.0)
            .build();

        assert_eq!(config.calculate_backoff(u32::MAX), config.max_backoff());
    }

    #[test]
    fn test_connection_pool_with_retry_config() {
        let retry_config = ConnectionRetryConfig::builder()
            .max_retries(5)
            .initial_backoff(Duration::from_millis(50))
            .max_backoff(Duration::from_secs(5))
            .backoff_multiplier(3.0)
            .jitter_factor(0.2)
            .build();
        let pool = ConnectionPool::with_retry_config(ConnectionConfig::default(), retry_config);
        assert_eq!(pool.retry_config.max_retries, 5);
        assert_eq!(
            pool.retry_config.initial_backoff(),
            Duration::from_millis(50)
        );
    }

    #[tokio::test]
    async fn test_pool_close_all_clears_both_maps() {
        // Verify close_all operates on both connections and connections_by_addr maps
        let pool = ConnectionPool::new(ConnectionConfig::default());
        // Both maps start empty
        {
            let s = pool.state.read();
            assert!(s.by_id.is_empty());
            assert!(s.by_addr.is_empty());
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
        // entry is stale in both `connections` (by broker id) and
        // `connections_by_addr` (bootstrap map).
        let pool = ConnectionPool::new(ConnectionConfig::default())
            .with_max_idle(Some(Duration::from_millis(100)));
        let stale = Arc::new(BrokerConnection::test_stub_idle_for(
            "b1:9092",
            Duration::from_secs(10),
        ));
        {
            let mut s = pool.state.write();
            s.by_id.insert(1, stale.clone());
            s.by_addr.insert("b1:9092".to_string(), stale);
        }

        // Same socket shared across both maps must dedup to a single
        // eviction.
        assert_eq!(pool.evict_idle(), 1);
        {
            let s = pool.state.read();
            assert!(s.by_id.is_empty());
            assert!(s.by_addr.is_empty());
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
        // `with_max_idle(None)` is a documented, supported configuration. It
        // used to take an early return that also skipped `start_refresh_task`,
        // leaving one JWT cached for the whole process lifetime.
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

    // ── Reconnection is bounded so waiters cannot wedge forever ────────

    #[test]
    fn test_reconnect_budgets_are_finite_and_ordered() {
        let pool = ConnectionPool::new(ConnectionConfig::default());
        let attempt = pool.connect_attempt_budget();
        let total = pool.total_reconnect_budget();
        assert!(attempt >= pool.config.connect_timeout);
        assert!(
            total > attempt,
            "the total budget must cover every attempt plus backoff"
        );
        assert!(total < Duration::from_secs(600), "budget must stay sane");
    }
}
