//! Connection pool for managing broker connections.
//!
//! This module provides:
//! - **Connection pooling**: Reuse connections across requests
//! - **Multi-connection bundles**: Multiple connections per broker for extreme throughput
//! - **Automatic reconnection**: Exponential backoff retry on connection failures
//! - **Round-robin selection**: Load balance requests across connection bundles

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use parking_lot::{Mutex, RwLock};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use super::connection::{BrokerConnection, ConnectionConfig};
use crate::BrokerId;
use crate::error::{KrafkaError, Result};

/// Configuration for connection retry with exponential backoff.
///
/// Use [`ConnectionRetryConfig::builder()`] or [`Default::default()`] to construct.
#[derive(Debug, Clone)]
pub struct ConnectionRetryConfig {
    /// Maximum number of retries (0 = no retries).
    pub(crate) max_retries: u32,
    /// Initial backoff duration.
    pub(crate) initial_backoff: Duration,
    /// Maximum backoff duration (caps exponential growth).
    pub(crate) max_backoff: Duration,
    /// Backoff multiplier for exponential growth.
    pub(crate) backoff_multiplier: f64,
    /// Jitter factor (0.0–1.0) to randomize backoff and prevent thundering herd.
    pub(crate) jitter_factor: f64,
}

impl Default for ConnectionRetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter_factor: 0.2,
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
        self.initial_backoff
    }

    /// Returns the maximum backoff duration.
    #[inline]
    pub fn max_backoff(&self) -> Duration {
        self.max_backoff
    }

    /// Returns the backoff multiplier.
    #[inline]
    pub fn backoff_multiplier(&self) -> f64 {
        self.backoff_multiplier
    }

    /// Returns the jitter factor (0.0–1.0).
    #[inline]
    pub fn jitter_factor(&self) -> f64 {
        self.jitter_factor
    }

    /// Calculate the backoff duration for a given attempt number (1-indexed).
    #[inline]
    fn calculate_backoff(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }

        // Exponential backoff: initial * multiplier^(attempt-1)
        let base_backoff =
            self.initial_backoff.as_secs_f64() * self.backoff_multiplier.powi((attempt - 1) as i32);

        // Cap at max backoff
        let capped_backoff = base_backoff.min(self.max_backoff.as_secs_f64());

        // Add jitter: ±jitter_factor * backoff (randomized to prevent thundering herd)
        let jitter_range = capped_backoff * self.jitter_factor;
        let jitter = if self.jitter_factor > 0.0 {
            use rand::Rng;
            let mut rng = rand::rng();
            rng.random_range(-jitter_range..=jitter_range)
        } else {
            0.0
        };

        let final_backoff = (capped_backoff + jitter).max(0.0);

        // Defensive: Duration::from_secs_f64 panics on NaN/Inf
        if !final_backoff.is_finite() {
            warn!(
                attempt,
                final_backoff,
                "Backoff calculation produced non-finite value, falling back to max_backoff"
            );
            return self.max_backoff;
        }

        Duration::from_secs_f64(final_backoff)
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
        self.config.initial_backoff = duration;
        self
    }

    /// Set maximum backoff duration.
    pub fn max_backoff(mut self, duration: Duration) -> Self {
        self.config.max_backoff = duration;
        self
    }

    /// Set backoff multiplier (must be finite and > 0; clamped to 1.0 otherwise).
    pub fn backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.config.backoff_multiplier = if multiplier.is_finite() && multiplier > 0.0 {
            multiplier
        } else {
            1.0
        };
        self
    }

    /// Set jitter factor (0.0–1.0) to randomize backoff and prevent thundering herd.
    pub fn jitter_factor(mut self, factor: f64) -> Self {
        self.config.jitter_factor = if factor.is_finite() {
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
// Connection Bundle
// ============================================================================

/// A bundle of connections to a single broker.
///
/// For extreme high-throughput scenarios (>100k msg/s per broker), multiple
/// TCP connections can parallelize I/O operations. This bundle manages
/// multiple connections and distributes requests using round-robin selection.
///
/// # Example
///
/// ```rust,ignore
/// // Create a bundle with 4 connections for high-throughput
/// let config = ConnectionConfig::builder()
///     .connections_per_broker(4)
///     .build();
///
/// let bundle = BrokerConnectionBundle::connect("broker-1:9092", config).await?;
/// let conn = bundle.select(); // Round-robin selection
/// ```
pub struct BrokerConnectionBundle {
    /// Address of the broker.
    address: String,
    /// Connections in the bundle.
    connections: Vec<Arc<BrokerConnection>>,
    /// Round-robin counter for connection selection.
    counter: AtomicUsize,
}

impl BrokerConnectionBundle {
    /// Create a new connection bundle with the configured number of connections.
    ///
    /// Connections are established in parallel for faster startup.
    pub async fn connect(address: &str, config: ConnectionConfig) -> Result<Self> {
        let num_connections = config.connections_per_broker.max(1);

        if num_connections == 1 {
            // Fast path for single connection (most common case)
            let conn = BrokerConnection::connect(address, config).await?;
            return Ok(Self {
                address: address.to_string(),
                connections: vec![Arc::new(conn)],
                counter: AtomicUsize::new(0),
            });
        }

        // Establish multiple connections in parallel
        let addr_owned = address.to_string();
        let mut handles = Vec::with_capacity(num_connections);
        for _ in 0..num_connections {
            let addr = addr_owned.clone();
            let cfg = config.clone();
            handles.push(tokio::spawn(async move {
                BrokerConnection::connect(&addr, cfg).await
            }));
        }

        // Collect results
        let mut connections = Vec::with_capacity(num_connections);
        for handle in handles {
            let conn = handle.await.map_err(|e| {
                KrafkaError::invalid_state(format!("Connection task failed: {e}"))
            })??;
            connections.push(Arc::new(conn));
        }

        debug!(
            "Created connection bundle with {} connections to {}",
            connections.len(),
            addr_owned
        );

        Ok(Self {
            address: addr_owned,
            connections,
            counter: AtomicUsize::new(0),
        })
    }

    /// Get a connection using round-robin selection.
    ///
    /// This is the primary way to get a connection for sending requests.
    /// For single-connection bundles, this always returns the same connection.
    #[inline]
    pub fn select(&self) -> Arc<BrokerConnection> {
        if self.connections.len() == 1 {
            return self.connections[0].clone();
        }

        let index = self.counter.fetch_add(1, Ordering::Relaxed) % self.connections.len();
        self.connections[index].clone()
    }

    /// Get a specific connection by index.
    ///
    /// Useful for request affinity scenarios where you want to ensure
    /// related requests go to the same connection.
    #[inline]
    pub fn get(&self, index: usize) -> Option<Arc<BrokerConnection>> {
        self.connections
            .get(index % self.connections.len())
            .cloned()
    }

    /// Get the first connection (useful for single-connection bundles).
    #[inline]
    pub fn first(&self) -> Arc<BrokerConnection> {
        self.connections[0].clone()
    }

    /// Get the address of the broker.
    #[inline]
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Get the number of connections in the bundle.
    #[inline]
    pub fn len(&self) -> usize {
        self.connections.len()
    }

    /// Check if the bundle is empty (should never be true).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.connections.is_empty()
    }

    /// Check if all connections in the bundle are usable (alive and
    /// not past their SASL session expiry).
    #[inline]
    pub fn all_usable(&self) -> bool {
        self.connections.iter().all(|c| c.is_usable())
    }

    /// Check if any connection in the bundle is usable.
    #[inline]
    pub fn any_usable(&self) -> bool {
        self.connections.iter().any(|c| c.is_usable())
    }

    /// Get the number of usable connections.
    #[inline]
    pub fn usable_count(&self) -> usize {
        self.connections.iter().filter(|c| c.is_usable()).count()
    }

    /// Select a usable connection.
    ///
    /// Uses round-robin selection but skips dead or session-expired connections.
    /// Returns None if no usable connection exists.
    pub fn select_usable(&self) -> Option<Arc<BrokerConnection>> {
        let len = self.connections.len();
        if len == 0 {
            return None;
        }
        let start = self.counter.fetch_add(1, Ordering::Relaxed) % len;

        // Check up to len connections starting from the round-robin position
        for i in 0..len {
            let index = (start + i) % len;
            if self.connections[index].is_usable() {
                return Some(self.connections[index].clone());
            }
        }

        None
    }

    /// Close all connections in the bundle.
    pub async fn close_all(&self) {
        for conn in &self.connections {
            conn.close().await;
        }
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

/// Waiters for coalesced reconnection attempts, keyed by address.
type ConnectingWaiters = HashMap<String, Vec<oneshot::Sender<Result<Arc<BrokerConnection>>>>>;

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
        // parking_lot::Mutex::lock() is always available in Drop — no
        // need for try_lock/spawn fallback.
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
/// Uses `parking_lot::RwLock` (writer-fair, non-async) for connection
/// maps so that the hot `get_connection*` read path stays fast and avoids
/// async lock overhead when there are no concurrent writers.  Reconnection attempts to the same address are coalesced
/// via a `parking_lot::Mutex`: only the first caller performs the
/// TCP/TLS/SASL handshake while subsequent callers wait on oneshot channels,
/// preventing thundering-herd reconnection storms.  The sync mutex ensures
/// deterministic cleanup in `ReconnectGuard`'s `Drop` impl without
/// requiring a `tokio::spawn` fallback.
pub struct ConnectionPool {
    /// Connections by broker ID.
    connections: RwLock<HashMap<BrokerId, Arc<BrokerConnection>>>,
    /// Connections by address (for bootstrap).
    connections_by_addr: RwLock<HashMap<String, Arc<BrokerConnection>>>,
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
    /// Handle of the background idle-eviction task, if one was spawned.
    /// Aborted by `close_all`. A `parking_lot::Mutex` is sufficient because
    /// the handle is only taken/replaced in non-hot paths (startup,
    /// shutdown).
    evictor_handle: Mutex<Option<JoinHandle<()>>>,
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
            connections: RwLock::new(HashMap::new()),
            connections_by_addr: RwLock::new(HashMap::new()),
            connecting: Arc::new(Mutex::new(HashMap::new())),
            config,
            retry_config: ConnectionRetryConfig::default(),
            max_idle: Some(DEFAULT_MAX_IDLE),
            evictor_handle: Mutex::new(None),
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
            connections: RwLock::new(HashMap::new()),
            connections_by_addr: RwLock::new(HashMap::new()),
            connecting: Arc::new(Mutex::new(HashMap::new())),
            config,
            retry_config,
            max_idle: Some(DEFAULT_MAX_IDLE),
            evictor_handle: Mutex::new(None),
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

    /// Re-read TLS certificate files from disk and atomically update the
    /// shared connector used by all future connections and reconnections.
    ///
    /// Existing TLS sessions are unaffected. On error the previous connector
    /// remains active.
    pub async fn refresh_tls(&self) -> crate::error::Result<()> {
        self.config.refresh_tls().await
    }

    /// Attempt to connect with exponential backoff retry logic.
    ///
    /// This method will retry connection attempts up to `max_retries` times,
    /// with exponential backoff between attempts.
    async fn reconnect_with_backoff(&self, address: &str) -> Result<Arc<BrokerConnection>> {
        let mut last_error: Option<KrafkaError> = None;

        for attempt in 0..=self.retry_config.max_retries {
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

            match BrokerConnection::connect(address, self.config.clone()).await {
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
            let conns = self.connections_by_addr.read();
            if conns
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
        // This read is done *outside* the `connecting` critical section so
        // the two locks are never held simultaneously (see the no-nested-lock
        // invariant documented in the reconnector completion path below).
        let existing = {
            let conns = self.connections_by_addr.read();
            conns.get(address).filter(|c| c.is_usable()).cloned()
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
                return rx.await.map_err(|_| {
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

        // Reconnect WITHOUT holding any lock
        let result = self.reconnect_with_backoff(address).await;

        // Notify waiting tasks and store the connection.
        // Safety invariant: this path must not hold `connecting` and
        // `connections_by_addr` at the same time.  We remove the waiters
        // from `connecting`, drop that lock immediately, and only then
        // update `connections_by_addr`.  Future changes must preserve this
        // no-nested-lock invariant; if code ever needs to hold both locks
        // simultaneously, it must define and follow a single global order.
        let waiters = self.connecting.lock().remove(address).unwrap_or_default();

        if let Ok(conn) = &result {
            self.connections_by_addr
                .write()
                .insert(addr_owned, conn.clone());
        }

        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }

        // Reconnection completed — prevent guard cleanup.
        guard.defuse();

        result
    }

    /// Get or create a connection to a broker by address.
    ///
    /// The read path uses a `parking_lot::RwLock` (writer-fair, no
    /// async overhead) so concurrent callers rarely convoy behind a pending
    /// writer.  On a cache miss the reconnection is coalesced per address.
    pub async fn get_connection(&self, address: &str) -> Result<Arc<BrokerConnection>> {
        // Fast path: sync read lock (nanosecond-scale critical section)
        {
            let conns = self.connections_by_addr.read();
            if let Some(conn) = conns.get(address)
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
        // Fast path: sync read lock
        {
            let conns = self.connections.read();
            if let Some(conn) = conns.get(&broker_id)
                && conn.is_usable()
            {
                return Ok(conn.clone());
            }
        }

        let conn = self.get_or_reconnect(address).await?;

        // Register under this broker ID so future fast-path lookups hit.
        {
            let mut by_id = self.connections.write();
            if !by_id.get(&broker_id).is_some_and(|c| c.is_usable()) {
                by_id.insert(broker_id, conn.clone());
            }
        }

        Ok(conn)
    }

    /// Remove connections that have sat idle for at least `max_idle`.
    ///
    /// Entries are removed from both `connections` (by broker ID) and
    /// `connections_by_addr` (bootstrap map); the underlying
    /// [`BrokerConnection`] is not explicitly `close()`-ed — its own `Drop`
    /// impl dispatches a close command when the last `Arc` reference is
    /// released, which correctly defers teardown if another caller is
    /// still holding the connection (e.g. mid-request).
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

        // Snapshot candidate keys under read locks; defer mutation so
        // critical sections stay short.
        let stale_ids: Vec<BrokerId> = {
            let conns = self.connections.read();
            conns
                .iter()
                .filter(|(_, c)| c.idle_duration() >= max_idle)
                .map(|(id, _)| *id)
                .collect()
        };
        let stale_addrs: Vec<String> = {
            let conns = self.connections_by_addr.read();
            conns
                .iter()
                .filter(|(_, c)| c.idle_duration() >= max_idle)
                .map(|(addr, _)| addr.clone())
                .collect()
        };

        if stale_ids.is_empty() && stale_addrs.is_empty() {
            return 0;
        }

        let mut removed: Vec<Arc<BrokerConnection>> =
            Vec::with_capacity(stale_ids.len() + stale_addrs.len());

        if !stale_ids.is_empty() {
            let mut conns = self.connections.write();
            for id in &stale_ids {
                // Re-check under the write lock: another task may have
                // refreshed `last_used_nanos` (or replaced the entry)
                // between the snapshot and here. `remove` + re-insert on
                // miss avoids a second hashmap lookup on the hot path.
                if let Some(c) = conns.remove(id) {
                    if c.idle_duration() >= max_idle {
                        removed.push(c);
                    } else {
                        conns.insert(*id, c);
                    }
                }
            }
        }

        if !stale_addrs.is_empty() {
            let mut conns = self.connections_by_addr.write();
            for addr in &stale_addrs {
                if let Some(c) = conns.remove(addr) {
                    if c.idle_duration() >= max_idle {
                        removed.push(c);
                    } else {
                        conns.insert(addr.clone(), c);
                    }
                }
            }
        }

        // A single connection typically lives in both maps (same `Arc`
        // registered under broker id and bootstrap address). Dedup by
        // `Arc::as_ptr` so the eviction count reflects unique sockets
        // and `Drop` runs once per connection without inflation.
        removed.sort_by_key(|c| Arc::as_ptr(c) as usize);
        removed.dedup_by(|a, b| Arc::ptr_eq(a, b));

        let count = removed.len();
        if count > 0 {
            debug!(
                evicted = count,
                max_idle_ms = max_idle.as_millis(),
                "Evicted idle connections"
            );
        }
        // Drop `removed` here: `BrokerConnection::Drop` dispatches the
        // close via a spawned task iff a runtime is active, and only when
        // the last `Arc` reference is released.
        drop(removed);
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

    /// Close all connections and drain both maps.
    ///
    /// Stops the idle-evictor, drains the broker-ID and address maps under
    /// short write locks (one at a time, never held simultaneously),
    /// deduplicates by `Arc` pointer, then closes each unique connection
    /// outside any lock. Any in-flight reconnection waiters in the
    /// `connecting` map are notified with an error so they do not hang
    /// during shutdown.
    pub async fn close_all(&self) {
        // Stop the idle-evictor before tearing down state so it does not
        // race with the drain below.
        if let Some(handle) = self.evictor_handle.lock().take() {
            handle.abort();
        }

        // Drain both maps, acquiring each write lock independently.
        let by_id: Vec<Arc<BrokerConnection>> =
            self.connections.write().drain().map(|(_, c)| c).collect();
        let by_addr: Vec<Arc<BrokerConnection>> = self
            .connections_by_addr
            .write()
            .drain()
            .map(|(_, c)| c)
            .collect();

        // Dedup: same Arc may appear in both maps.
        let mut seen = HashMap::with_capacity(by_id.len() + by_addr.len());
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

        // Close connections outside any lock.
        for conn in seen.into_values() {
            conn.close().await;
        }
    }

    /// Number of usable connections known by broker ID.
    ///
    /// Bootstrap connections that have not yet been associated with a broker
    /// ID (i.e. only in the address map) are **not** counted.
    pub fn len(&self) -> usize {
        let connections = self.connections.read();
        connections.values().filter(|c| c.is_usable()).count()
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
        assert_eq!(config.initial_backoff, Duration::from_millis(100));
        assert_eq!(config.max_backoff, Duration::from_secs(10));
        assert_eq!(config.backoff_multiplier, 2.0);
    }

    #[test]
    fn test_calculate_backoff() {
        let config = ConnectionRetryConfig {
            jitter_factor: 0.0, // disable jitter for deterministic test
            ..ConnectionRetryConfig::default()
        };

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
        let config = ConnectionRetryConfig {
            max_retries: 10,
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(5),
            backoff_multiplier: 10.0,
            jitter_factor: 0.0, // disable jitter for deterministic test
        };

        // Attempt 2 would be 10 seconds, but capped at 5
        assert_eq!(config.calculate_backoff(2), Duration::from_secs(5));
    }

    #[test]
    fn test_connection_pool_with_retry_config() {
        let retry_config = ConnectionRetryConfig {
            max_retries: 5,
            initial_backoff: Duration::from_millis(50),
            max_backoff: Duration::from_secs(5),
            backoff_multiplier: 3.0,
            jitter_factor: 0.2,
        };
        let pool = ConnectionPool::with_retry_config(ConnectionConfig::default(), retry_config);
        assert_eq!(pool.retry_config.max_retries, 5);
        assert_eq!(pool.retry_config.initial_backoff, Duration::from_millis(50));
    }

    #[test]
    fn test_connections_per_broker_config() {
        // Default is 1
        let config = ConnectionConfig::default();
        assert_eq!(config.connections_per_broker, 1);

        // Custom value
        let config = ConnectionConfig::builder()
            .connections_per_broker(4)
            .build();
        assert_eq!(config.connections_per_broker, 4);

        // Zero becomes 1 (minimum)
        let config = ConnectionConfig::builder()
            .connections_per_broker(0)
            .build();
        assert_eq!(config.connections_per_broker, 1);
    }

    #[tokio::test]
    async fn test_pool_close_all_clears_both_maps() {
        // Verify close_all operates on both connections and connections_by_addr maps
        let pool = ConnectionPool::new(ConnectionConfig::default());
        // Both maps start empty
        assert!(pool.connections.read().is_empty());
        assert!(pool.connections_by_addr.read().is_empty());
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
        pool.connections.write().insert(1, stale.clone());
        pool.connections_by_addr
            .write()
            .insert("b1:9092".to_string(), stale);

        // Same socket shared across both maps must dedup to a single
        // eviction.
        assert_eq!(pool.evict_idle(), 1);
        assert!(pool.connections.read().is_empty());
        assert!(pool.connections_by_addr.read().is_empty());
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
            let mut w = pool.connections.write();
            w.insert(1, stale);
            w.insert(2, fresh);
        }

        assert_eq!(pool.evict_idle(), 1);
        let kept = pool.connections.read();
        assert!(!kept.contains_key(&1));
        assert!(kept.contains_key(&2));
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
        pool.connections.write().insert(1, conn);

        assert_eq!(pool.evict_idle(), 0);
        assert!(pool.connections.read().contains_key(&1));
    }
}
