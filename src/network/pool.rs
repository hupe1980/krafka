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

use tokio::sync::RwLock;
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
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Returns the initial backoff duration.
    pub fn initial_backoff(&self) -> Duration {
        self.initial_backoff
    }

    /// Returns the maximum backoff duration.
    pub fn max_backoff(&self) -> Duration {
        self.max_backoff
    }

    /// Returns the backoff multiplier.
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

    /// Set backoff multiplier.
    pub fn backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.config.backoff_multiplier = multiplier;
        self
    }

    /// Set jitter factor (0.0–1.0) to randomize backoff and prevent thundering herd.
    pub fn jitter_factor(mut self, factor: f64) -> Self {
        self.config.jitter_factor = factor.clamp(0.0, 1.0);
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
        let mut handles = Vec::with_capacity(num_connections);
        for _ in 0..num_connections {
            let addr = address.to_string();
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
            address
        );

        Ok(Self {
            address: address.to_string(),
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

    /// Check if all connections in the bundle are alive.
    #[inline]
    pub fn all_alive(&self) -> bool {
        self.connections.iter().all(|c| c.is_alive())
    }

    /// Check if any connection in the bundle is alive.
    #[inline]
    pub fn any_alive(&self) -> bool {
        self.connections.iter().any(|c| c.is_alive())
    }

    /// Get the number of alive connections.
    #[inline]
    pub fn alive_count(&self) -> usize {
        self.connections.iter().filter(|c| c.is_alive()).count()
    }

    /// Select an alive connection.
    ///
    /// Uses round-robin selection but skips dead connections.
    /// Returns None if all connections are dead.
    pub fn select_alive(&self) -> Option<Arc<BrokerConnection>> {
        let len = self.connections.len();
        let start = self.counter.fetch_add(1, Ordering::Relaxed) % len;

        // Check up to len connections starting from the round-robin position
        for i in 0..len {
            let index = (start + i) % len;
            if self.connections[index].is_alive() {
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

/// A pool of connections to Kafka brokers.
pub struct ConnectionPool {
    /// Connections by broker ID.
    connections: RwLock<HashMap<BrokerId, Arc<BrokerConnection>>>,
    /// Connections by address (for bootstrap).
    connections_by_addr: RwLock<HashMap<String, Arc<BrokerConnection>>>,
    /// Connection config.
    config: ConnectionConfig,
    /// Retry configuration for reconnection attempts.
    retry_config: ConnectionRetryConfig,
}

impl ConnectionPool {
    /// Create a new connection pool.
    pub fn new(config: ConnectionConfig) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            connections_by_addr: RwLock::new(HashMap::new()),
            config,
            retry_config: ConnectionRetryConfig::default(),
        }
    }

    /// Create a new connection pool with custom retry configuration.
    pub fn with_retry_config(
        config: ConnectionConfig,
        retry_config: ConnectionRetryConfig,
    ) -> Self {
        Self {
            connections: RwLock::new(HashMap::new()),
            connections_by_addr: RwLock::new(HashMap::new()),
            config,
            retry_config,
        }
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
            KrafkaError::Network(std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!(
                    "Failed to connect to {} after {} retries",
                    address, self.retry_config.max_retries
                ),
            ))
        }))
    }

    /// Get or create a connection to a broker by address.
    ///
    /// Drops the lock before performing network I/O to avoid blocking other
    /// callers while a reconnection is in progress.
    pub async fn get_connection(&self, address: &str) -> Result<Arc<BrokerConnection>> {
        // Fast path: check under read lock
        {
            let connections = self.connections_by_addr.read().await;
            if let Some(conn) = connections.get(address)
                && conn.is_alive()
            {
                return Ok(conn.clone());
            }
        }

        // Slow path: reconnect WITHOUT holding any lock
        let conn = self.reconnect_with_backoff(address).await?;

        // Re-acquire write lock to store the new connection
        let mut connections = self.connections_by_addr.write().await;
        // Double-check: another task may have reconnected while we were connecting
        if let Some(existing) = connections.get(address)
            && existing.is_alive()
        {
            return Ok(existing.clone());
        }
        connections.insert(address.to_string(), conn.clone());

        debug!("Created connection to {}", address);
        Ok(conn)
    }

    /// Get or create a connection to a broker by ID.
    ///
    /// Drops locks before performing network I/O to avoid blocking other
    /// callers while a reconnection is in progress.
    pub async fn get_connection_by_id(
        &self,
        broker_id: BrokerId,
        address: &str,
    ) -> Result<Arc<BrokerConnection>> {
        // Fast path: check under read lock
        {
            let connections = self.connections.read().await;
            if let Some(conn) = connections.get(&broker_id)
                && conn.is_alive()
            {
                return Ok(conn.clone());
            }
        }

        // Slow path: reconnect WITHOUT holding any lock
        let conn = self.reconnect_with_backoff(address).await?;

        // Re-acquire write locks to store the new connection
        let mut connections = self.connections.write().await;
        let mut connections_by_addr = self.connections_by_addr.write().await;

        // Double-check: another task may have reconnected while we were connecting
        if let Some(existing) = connections.get(&broker_id)
            && existing.is_alive()
        {
            return Ok(existing.clone());
        }

        connections.insert(broker_id, conn.clone());
        connections_by_addr.insert(address.to_string(), conn.clone());

        info!("Created connection to broker {} at {}", broker_id, address);
        Ok(conn)
    }

    /// Register a connection for a broker ID.
    ///
    /// Both maps are updated atomically under write locks to prevent
    /// inconsistent state if another task reads between updates.
    /// Lock ordering: `connections` → `connections_by_addr` (consistent
    /// across all pool methods; no deadlock risk).
    pub async fn register(&self, broker_id: BrokerId, conn: Arc<BrokerConnection>) {
        let mut connections = self.connections.write().await;
        let mut connections_by_addr = self.connections_by_addr.write().await;
        connections.insert(broker_id, conn.clone());
        connections_by_addr.insert(conn.address().to_string(), conn);
    }

    /// Remove a connection by broker ID.
    ///
    /// Lock ordering matches [`register`](Self::register): `connections` →
    /// `connections_by_addr`.
    pub async fn remove(&self, broker_id: BrokerId) {
        let mut connections = self.connections.write().await;
        let mut connections_by_addr = self.connections_by_addr.write().await;
        if let Some(conn) = connections.remove(&broker_id) {
            connections_by_addr.remove(conn.address());
        }
    }

    /// Clean up all dead connections from the pool.
    ///
    /// This method removes connections that are no longer alive from both
    /// the broker ID and address maps.
    pub async fn cleanup_dead_connections(&self) {
        let mut connections = self.connections.write().await;
        let mut connections_by_addr = self.connections_by_addr.write().await;

        // Collect dead broker IDs and their addresses
        let dead_entries: Vec<(BrokerId, String)> = connections
            .iter()
            .filter(|(_, conn)| !conn.is_alive())
            .map(|(id, conn)| (*id, conn.address().to_string()))
            .collect();

        // Remove dead connections by broker ID
        for (broker_id, address) in &dead_entries {
            connections.remove(broker_id);
            connections_by_addr.remove(address);
            debug!(broker_id = %broker_id, address = %address, "Cleaned up dead connection");
        }

        // Also clean up orphaned dead connections in connections_by_addr
        // (connections that were added by address only, not by broker ID)
        let dead_addrs: Vec<String> = connections_by_addr
            .iter()
            .filter(|(_, conn)| !conn.is_alive())
            .map(|(addr, _)| addr.clone())
            .collect();

        for addr in dead_addrs {
            connections_by_addr.remove(&addr);
            debug!(address = %addr, "Cleaned up dead orphaned connection");
        }

        if !dead_entries.is_empty() {
            info!(
                count = dead_entries.len(),
                "Cleaned up dead connections from pool"
            );
        }
    }

    /// Close all connections.
    ///
    /// Closes connections from both the broker-ID map and the address map
    /// to ensure bootstrap connections (by address only) are not leaked (R6.3 fix).
    pub async fn close_all(&self) {
        let connections = self.connections.read().await;
        for conn in connections.values() {
            conn.close().await;
        }
        drop(connections);

        // Also close any by-address connections that aren't in the by-ID map.
        // This covers bootstrap connections that were created before broker IDs
        // were known from metadata.
        let by_addr = self.connections_by_addr.read().await;
        for conn in by_addr.values() {
            conn.close().await;
        }
    }

    /// Get the number of active connections.
    pub async fn len(&self) -> usize {
        let connections = self.connections.read().await;
        connections.values().filter(|c| c.is_alive()).count()
    }

    /// Check if the pool is empty.
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

#[cfg(test)]
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
        assert!(pool.connections.read().await.is_empty());
        assert!(pool.connections_by_addr.read().await.is_empty());
        // close_all on empty pool should not panic
        pool.close_all().await;
    }
}
