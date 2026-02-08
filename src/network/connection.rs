//! Broker connection implementation.
//!
//! This module provides connection handling with support for:
//! - **Request priority**: High-priority requests (heartbeats, metadata) are processed
//!   before normal-priority requests to prevent consumer group ejection during backpressure.
//! - **Multi-connection bundles**: Multiple connections per broker for extreme high-throughput.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::timeout;
use tracing::{debug, error, trace, warn};

use crate::CorrelationId;
use crate::error::{KrafkaError, Result};
use crate::protocol::{
    ApiKey, ApiVersionRange, ApiVersionsRequest, ApiVersionsResponse, Decoder, Encoder,
    RequestHeader, ResponseHeader,
};
use crate::util::CorrelationIdGenerator;

/// Request priority level.
///
/// High-priority requests are processed before normal-priority requests,
/// which is critical for preventing consumer group ejection during backpressure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequestPriority {
    /// High priority for time-critical requests like heartbeats and metadata.
    ///
    /// These requests are processed first to prevent consumer group ejection
    /// during periods of high throughput or backpressure.
    High,
    /// Normal priority for data requests like produce and fetch.
    Normal,
}

impl RequestPriority {
    /// Determine the priority for an API key.
    ///
    /// Time-sensitive coordination requests get high priority.
    #[inline]
    pub fn for_api_key(api_key: ApiKey) -> Self {
        match api_key {
            // Group coordination - must not be delayed
            ApiKey::Heartbeat | ApiKey::ConsumerGroupHeartbeat => Self::High,
            // Metadata refresh - critical for proper routing
            ApiKey::Metadata => Self::High,
            // Coordinator discovery - needed for heartbeats
            ApiKey::FindCoordinator => Self::High,
            // Leader discovery
            ApiKey::LeaderAndIsr => Self::High,
            // API version negotiation
            ApiKey::ApiVersions => Self::High,
            // Everything else is normal priority
            _ => Self::Normal,
        }
    }
}

/// Configuration for broker connections.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Connection timeout.
    pub connect_timeout: Duration,
    /// Request timeout.
    pub request_timeout: Duration,
    /// Socket send buffer size.
    pub send_buffer_size: Option<usize>,
    /// Socket receive buffer size.
    pub recv_buffer_size: Option<usize>,
    /// TCP nodelay.
    pub nodelay: bool,
    /// Client ID.
    pub client_id: String,
    /// Number of connections per broker for high-throughput scenarios.
    ///
    /// Default is 1. For extreme high-throughput (>100k msg/s per broker),
    /// consider 2-4 connections to parallelize I/O operations.
    pub connections_per_broker: usize,
    /// High-priority channel capacity for heartbeats and metadata requests.
    ///
    /// This should be small since high-priority requests should be rare.
    pub high_priority_channel_capacity: usize,
    /// Normal-priority channel capacity for produce and fetch requests.
    pub normal_priority_channel_capacity: usize,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(30),
            send_buffer_size: None,
            recv_buffer_size: None,
            nodelay: true,
            client_id: "krafka".to_string(),
            connections_per_broker: 1,
            high_priority_channel_capacity: 64,
            normal_priority_channel_capacity: 256,
        }
    }
}

impl ConnectionConfig {
    /// Create a new connection config builder.
    pub fn builder() -> ConnectionConfigBuilder {
        ConnectionConfigBuilder::default()
    }
}

/// Builder for ConnectionConfig.
#[must_use = "builders do nothing until .build() is called"]
#[derive(Debug, Default)]
pub struct ConnectionConfigBuilder(ConnectionConfig);

impl ConnectionConfigBuilder {
    /// Set the connect timeout.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.0.connect_timeout = timeout;
        self
    }

    /// Set the request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.0.request_timeout = timeout;
        self
    }

    /// Set the client ID.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.0.client_id = client_id.into();
        self
    }

    /// Set TCP nodelay.
    pub fn nodelay(mut self, nodelay: bool) -> Self {
        self.0.nodelay = nodelay;
        self
    }

    /// Set the number of connections per broker.
    ///
    /// For extreme high-throughput (>100k msg/s per broker), use 2-4 connections.
    /// Default is 1.
    pub fn connections_per_broker(mut self, count: usize) -> Self {
        self.0.connections_per_broker = count.max(1);
        self
    }

    /// Set the high-priority channel capacity.
    ///
    /// This channel is used for heartbeats and metadata requests.
    pub fn high_priority_channel_capacity(mut self, capacity: usize) -> Self {
        self.0.high_priority_channel_capacity = capacity.max(16);
        self
    }

    /// Set the normal-priority channel capacity.
    ///
    /// This channel is used for produce and fetch requests.
    pub fn normal_priority_channel_capacity(mut self, capacity: usize) -> Self {
        self.0.normal_priority_channel_capacity = capacity.max(64);
        self
    }

    /// Build the config.
    pub fn build(self) -> ConnectionConfig {
        self.0
    }
}

/// A pending request waiting for a response.
struct PendingRequest {
    response_tx: oneshot::Sender<Result<Bytes>>,
    api_key: ApiKey,
    api_version: i16,
}

/// Command sent to the connection task.
enum ConnectionCommand {
    /// Send a request and wait for response.
    Request {
        data: Bytes,
        correlation_id: CorrelationId,
        api_key: ApiKey,
        api_version: i16,
        response_tx: oneshot::Sender<Result<Bytes>>,
    },
    /// Close the connection.
    Close,
}

/// A connection to a Kafka broker.
///
/// This connection supports priority-based request handling:
/// - High-priority requests (heartbeats, metadata) are processed first
/// - Normal-priority requests (produce, fetch) are processed when no high-priority pending
pub struct BrokerConnection {
    /// Broker address.
    address: String,
    /// Connection config.
    config: ConnectionConfig,
    /// Correlation ID generator.
    correlation_id_gen: Arc<CorrelationIdGenerator>,
    /// High-priority command sender (heartbeats, metadata).
    high_priority_tx: mpsc::Sender<ConnectionCommand>,
    /// Normal-priority command sender (produce, fetch).
    normal_priority_tx: mpsc::Sender<ConnectionCommand>,
    /// API versions supported by the broker.
    api_versions: Arc<Mutex<HashMap<ApiKey, ApiVersionRange>>>,
    /// Whether the connection is alive.
    alive: Arc<std::sync::atomic::AtomicBool>,
    /// Statistics for monitoring.
    stats: Arc<ConnectionStats>,
}

/// Connection statistics for monitoring.
#[derive(Debug, Default)]
pub struct ConnectionStats {
    /// Total high-priority requests sent.
    pub high_priority_requests: AtomicU64,
    /// Total normal-priority requests sent.
    pub normal_priority_requests: AtomicU64,
    /// High-priority requests that bypassed the queue (processed immediately).
    pub high_priority_bypasses: AtomicU64,
}

impl ConnectionStats {
    /// Get the total high-priority requests sent.
    #[inline]
    pub fn high_priority_count(&self) -> u64 {
        self.high_priority_requests.load(Ordering::Relaxed)
    }

    /// Get the total normal-priority requests sent.
    #[inline]
    pub fn normal_priority_count(&self) -> u64 {
        self.normal_priority_requests.load(Ordering::Relaxed)
    }

    /// Get the number of high-priority bypasses.
    #[inline]
    pub fn bypass_count(&self) -> u64 {
        self.high_priority_bypasses.load(Ordering::Relaxed)
    }
}

impl BrokerConnection {
    /// Connect to a broker.
    pub async fn connect(address: &str, config: ConnectionConfig) -> Result<Self> {
        let stream = timeout(config.connect_timeout, TcpStream::connect(address))
            .await
            .map_err(|_| KrafkaError::timeout("connection"))?
            .map_err(KrafkaError::Network)?;

        stream.set_nodelay(config.nodelay)?;

        debug!("Connected to broker at {}", address);

        // Create priority channels
        let (high_priority_tx, high_priority_rx) =
            mpsc::channel(config.high_priority_channel_capacity);
        let (normal_priority_tx, normal_priority_rx) =
            mpsc::channel(config.normal_priority_channel_capacity);

        let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let alive_clone = alive.clone();
        let stats = Arc::new(ConnectionStats::default());
        let stats_clone = stats.clone();

        let connection = Self {
            address: address.to_string(),
            config: config.clone(),
            correlation_id_gen: Arc::new(CorrelationIdGenerator::new()),
            high_priority_tx,
            normal_priority_tx,
            api_versions: Arc::new(Mutex::new(HashMap::new())),
            alive,
            stats,
        };

        // Spawn the connection task with priority handling
        let _api_versions = connection.api_versions.clone();
        tokio::spawn(async move {
            if let Err(e) = Self::run_connection_with_priority(
                stream,
                high_priority_rx,
                normal_priority_rx,
                config.request_timeout,
                stats_clone,
            )
            .await
            {
                error!("Connection error: {}", e);
            }
            alive_clone.store(false, std::sync::atomic::Ordering::SeqCst);
        });

        // Fetch API versions
        connection.fetch_api_versions().await?;

        Ok(connection)
    }

    /// Run the connection event loop with priority handling.
    ///
    /// High-priority requests are always checked first using try_recv,
    /// ensuring heartbeats are never starved by backpressure on data requests.
    async fn run_connection_with_priority(
        stream: TcpStream,
        mut high_priority_rx: mpsc::Receiver<ConnectionCommand>,
        mut normal_priority_rx: mpsc::Receiver<ConnectionCommand>,
        _request_timeout: Duration,
        stats: Arc<ConnectionStats>,
    ) -> Result<()> {
        let (mut reader, mut writer) = stream.into_split();
        let pending: Arc<Mutex<HashMap<CorrelationId, PendingRequest>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let pending_clone = pending.clone();

        // Spawn reader task
        let reader_handle = tokio::spawn(async move {
            let mut decoder = Decoder::new();
            let mut buf = vec![0u8; 65536];

            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => {
                        debug!("Connection closed by peer");
                        break;
                    }
                    Ok(n) => {
                        decoder.extend(&buf[..n]);

                        // Process all complete messages
                        while let Some(response) = decoder.decode()? {
                            Self::handle_response(&pending_clone, response).await?;
                        }
                    }
                    Err(e) => {
                        error!("Read error: {}", e);
                        return Err(KrafkaError::Network(e));
                    }
                }
            }

            Ok::<_, KrafkaError>(())
        });

        // Process commands with priority
        loop {
            // Try high-priority first (non-blocking)
            if let Ok(cmd) = high_priority_rx.try_recv() {
                stats.high_priority_bypasses.fetch_add(1, Ordering::Relaxed);
                if Self::handle_command(&mut writer, &pending, cmd).await? {
                    break;
                }
                continue;
            }

            // Wait for either channel
            tokio::select! {
                // Bias towards high-priority
                biased;

                cmd = high_priority_rx.recv() => {
                    match cmd {
                        Some(cmd) => {
                            if Self::handle_command(&mut writer, &pending, cmd).await? {
                                break;
                            }
                        }
                        None => break, // Channel closed
                    }
                }
                cmd = normal_priority_rx.recv() => {
                    match cmd {
                        Some(cmd) => {
                            if Self::handle_command(&mut writer, &pending, cmd).await? {
                                break;
                            }
                        }
                        None => break, // Channel closed
                    }
                }
            }
        }

        // Wait for reader to finish
        drop(writer);
        let _ = reader_handle.await;

        Ok(())
    }

    /// Handle a single connection command.
    ///
    /// Returns `true` if the connection should close.
    async fn handle_command(
        writer: &mut tokio::net::tcp::OwnedWriteHalf,
        pending: &Mutex<HashMap<CorrelationId, PendingRequest>>,
        cmd: ConnectionCommand,
    ) -> Result<bool> {
        match cmd {
            ConnectionCommand::Request {
                data,
                correlation_id,
                api_key,
                api_version,
                response_tx,
            } => {
                // Store pending request
                {
                    let mut pending = pending.lock().await;
                    pending.insert(
                        correlation_id,
                        PendingRequest {
                            response_tx,
                            api_key,
                            api_version,
                        },
                    );
                }

                // Send request
                if let Err(e) = writer.write_all(&data).await {
                    error!("Write error: {}", e);
                    let mut pending = pending.lock().await;
                    if let Some(req) = pending.remove(&correlation_id) {
                        let _ = req.response_tx.send(Err(KrafkaError::Network(e)));
                    }
                    return Ok(false);
                }
                // Ensure data is sent immediately
                if let Err(e) = writer.flush().await {
                    error!("Flush error: {}", e);
                }
                Ok(false)
            }
            ConnectionCommand::Close => {
                debug!("Closing connection");
                Ok(true)
            }
        }
    }

    /// Handle an incoming response.
    async fn handle_response(
        pending: &Mutex<HashMap<CorrelationId, PendingRequest>>,
        response: Bytes,
    ) -> Result<()> {
        // Read correlation ID from response
        if response.len() < 4 {
            return Err(KrafkaError::protocol("response too short"));
        }

        let correlation_id =
            i32::from_be_bytes([response[0], response[1], response[2], response[3]]);

        let mut pending = pending.lock().await;
        if let Some(req) = pending.remove(&correlation_id) {
            trace!("Received response for correlation_id={}", correlation_id);

            // Decode response header
            let mut response_buf = response.slice(..);
            let _header = ResponseHeader::decode(&mut response_buf, req.api_key, req.api_version)?;

            // Return the remaining bytes as the response body
            let header_size = response.len() - response_buf.len();
            let body = response.slice(header_size..);

            let _ = req.response_tx.send(Ok(body));
        } else {
            warn!(
                "Received response for unknown correlation_id={}",
                correlation_id
            );
        }

        Ok(())
    }

    /// Fetch API versions from the broker.
    async fn fetch_api_versions(&self) -> Result<()> {
        let request =
            ApiVersionsRequest::new().with_client_software("krafka", env!("CARGO_PKG_VERSION"));

        let correlation_id = self.correlation_id_gen.next();
        let mut encoder = Encoder::new();

        // Build request
        let pos = encoder.start_message();
        let header = RequestHeader::new(ApiKey::ApiVersions, 0, correlation_id)
            .with_client_id(&self.config.client_id);
        header.encode_v1(encoder.buffer_mut());
        request.encode_v0(encoder.buffer_mut());
        encoder.finish_message(pos);

        // Send request (use high priority for API versions)
        let (response_tx, response_rx) = oneshot::channel();
        self.high_priority_tx
            .send(ConnectionCommand::Request {
                data: encoder.take(),
                correlation_id,
                api_key: ApiKey::ApiVersions,
                api_version: 0,
                response_tx,
            })
            .await
            .map_err(|_| KrafkaError::invalid_state("connection closed"))?;

        self.stats
            .high_priority_requests
            .fetch_add(1, Ordering::Relaxed);

        // Wait for response
        let response = timeout(self.config.request_timeout, response_rx)
            .await
            .map_err(|_| KrafkaError::timeout("api versions request"))?
            .map_err(|_| KrafkaError::invalid_state("response channel closed"))??;

        // Decode response
        let mut buf = response;
        let api_versions_response = ApiVersionsResponse::decode_v0(&mut buf)?;

        if api_versions_response.error_code != 0 {
            return Err(KrafkaError::protocol(format!(
                "ApiVersions error: {}",
                api_versions_response.error_code
            )));
        }

        // Store API versions
        let mut versions = self.api_versions.lock().await;
        for range in api_versions_response.api_keys {
            versions.insert(range.api_key, range);
        }

        debug!("Fetched {} API versions", versions.len());
        Ok(())
    }

    /// Choose the appropriate channel based on request priority.
    #[inline]
    fn channel_for_priority(&self, priority: RequestPriority) -> &mpsc::Sender<ConnectionCommand> {
        match priority {
            RequestPriority::High => &self.high_priority_tx,
            RequestPriority::Normal => &self.normal_priority_tx,
        }
    }

    /// Send a request with automatic priority based on API key.
    ///
    /// Priority is determined automatically:
    /// - High: Heartbeat, Metadata, FindCoordinator, ApiVersions
    /// - Normal: Produce, Fetch, and all other requests
    pub async fn send_request(
        &self,
        api_key: ApiKey,
        api_version: i16,
        request_body: impl FnOnce(&mut BytesMut),
    ) -> Result<Bytes> {
        let priority = RequestPriority::for_api_key(api_key);
        self.send_request_with_priority(api_key, api_version, priority, request_body)
            .await
    }

    /// Send a request with explicit priority.
    ///
    /// Use this when you need to override the automatic priority selection.
    pub async fn send_request_with_priority(
        &self,
        api_key: ApiKey,
        api_version: i16,
        priority: RequestPriority,
        request_body: impl FnOnce(&mut BytesMut),
    ) -> Result<Bytes> {
        let correlation_id = self.correlation_id_gen.next();
        let mut encoder = Encoder::new();

        // Build request
        let pos = encoder.start_message();
        let header = RequestHeader::new(api_key, api_version, correlation_id)
            .with_client_id(&self.config.client_id);
        header.encode(encoder.buffer_mut());
        request_body(encoder.buffer_mut());
        encoder.finish_message(pos);

        // Send request to appropriate channel
        let (response_tx, response_rx) = oneshot::channel();
        let channel = self.channel_for_priority(priority);
        channel
            .send(ConnectionCommand::Request {
                data: encoder.take(),
                correlation_id,
                api_key,
                api_version,
                response_tx,
            })
            .await
            .map_err(|_| KrafkaError::invalid_state("connection closed"))?;

        // Update stats
        match priority {
            RequestPriority::High => {
                self.stats
                    .high_priority_requests
                    .fetch_add(1, Ordering::Relaxed);
            }
            RequestPriority::Normal => {
                self.stats
                    .normal_priority_requests
                    .fetch_add(1, Ordering::Relaxed);
            }
        }

        // Wait for response
        let response = timeout(self.config.request_timeout, response_rx)
            .await
            .map_err(|_| KrafkaError::timeout("request"))?
            .map_err(|_| KrafkaError::invalid_state("response channel closed"))??;

        Ok(response)
    }

    /// Get the supported API version for a specific API.
    pub async fn get_api_version(&self, api_key: ApiKey) -> Option<ApiVersionRange> {
        let versions = self.api_versions.lock().await;
        versions.get(&api_key).copied()
    }

    /// Negotiate the best API version for a given API key.
    ///
    /// Returns the highest mutually supported version between the client and broker.
    ///
    /// # Arguments
    ///
    /// * `api_key` - The API key to negotiate
    /// * `client_max` - Maximum version the client supports
    /// * `client_min` - Minimum version the client supports (default 0)
    ///
    /// # Returns
    ///
    /// The negotiated version, or None if:
    /// - The broker doesn't support this API
    /// - There's no overlap between client and broker versions
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Client supports Fetch v4-v12
    /// let version = conn.negotiate_api_version(ApiKey::Fetch, 12, 4).await;
    /// ```
    pub async fn negotiate_api_version(
        &self,
        api_key: ApiKey,
        client_max: i16,
        client_min: i16,
    ) -> Option<i16> {
        let versions = self.api_versions.lock().await;
        versions
            .get(&api_key)
            .and_then(|range| range.negotiate(client_max, client_min))
    }

    /// Negotiate the best API version with minimum version defaulting to 0.
    pub async fn negotiate_api_version_max(&self, api_key: ApiKey, client_max: i16) -> Option<i16> {
        self.negotiate_api_version(api_key, client_max, 0).await
    }

    /// Check if the connection is alive.
    #[inline]
    pub fn is_alive(&self) -> bool {
        self.alive.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Get the broker address.
    #[inline]
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Get connection statistics.
    #[inline]
    pub fn stats(&self) -> &ConnectionStats {
        &self.stats
    }

    /// Close the connection.
    pub async fn close(&self) {
        // Use high-priority channel for close command
        let _ = self.high_priority_tx.send(ConnectionCommand::Close).await;
    }
}

impl Drop for BrokerConnection {
    fn drop(&mut self) {
        // Attempt to close the connection via high-priority channel
        let tx = self.high_priority_tx.clone();
        tokio::spawn(async move {
            let _ = tx.send(ConnectionCommand::Close).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_connection_config_builder() {
        let config = ConnectionConfig::builder()
            .connect_timeout(Duration::from_secs(5))
            .request_timeout(Duration::from_secs(15))
            .client_id("test-client")
            .nodelay(false)
            .build();

        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.request_timeout, Duration::from_secs(15));
        assert_eq!(config.client_id, "test-client");
        assert!(!config.nodelay);
    }

    #[test]
    fn test_connection_config_default() {
        let config = ConnectionConfig::default();
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert_eq!(config.client_id, "krafka");
        assert!(config.nodelay);
        assert_eq!(config.connections_per_broker, 1);
        assert_eq!(config.high_priority_channel_capacity, 64);
        assert_eq!(config.normal_priority_channel_capacity, 256);
    }

    #[test]
    fn test_connection_config_builder_with_priority() {
        let config = ConnectionConfig::builder()
            .connections_per_broker(4)
            .high_priority_channel_capacity(32)
            .normal_priority_channel_capacity(512)
            .build();

        assert_eq!(config.connections_per_broker, 4);
        assert_eq!(config.high_priority_channel_capacity, 32);
        assert_eq!(config.normal_priority_channel_capacity, 512);
    }

    #[test]
    fn test_connection_config_min_values() {
        // Ensure minimums are enforced
        let config = ConnectionConfig::builder()
            .connections_per_broker(0) // Should become 1
            .high_priority_channel_capacity(0) // Should become 16
            .normal_priority_channel_capacity(0) // Should become 64
            .build();

        assert_eq!(config.connections_per_broker, 1);
        assert_eq!(config.high_priority_channel_capacity, 16);
        assert_eq!(config.normal_priority_channel_capacity, 64);
    }

    #[test]
    fn test_request_priority_for_api_key() {
        // High priority APIs
        assert_eq!(
            RequestPriority::for_api_key(ApiKey::Heartbeat),
            RequestPriority::High
        );
        assert_eq!(
            RequestPriority::for_api_key(ApiKey::Metadata),
            RequestPriority::High
        );
        assert_eq!(
            RequestPriority::for_api_key(ApiKey::FindCoordinator),
            RequestPriority::High
        );
        assert_eq!(
            RequestPriority::for_api_key(ApiKey::ApiVersions),
            RequestPriority::High
        );

        // Normal priority APIs
        assert_eq!(
            RequestPriority::for_api_key(ApiKey::Produce),
            RequestPriority::Normal
        );
        assert_eq!(
            RequestPriority::for_api_key(ApiKey::Fetch),
            RequestPriority::Normal
        );
        assert_eq!(
            RequestPriority::for_api_key(ApiKey::OffsetCommit),
            RequestPriority::Normal
        );
        assert_eq!(
            RequestPriority::for_api_key(ApiKey::OffsetFetch),
            RequestPriority::Normal
        );
    }

    #[test]
    fn test_connection_stats_default() {
        let stats = ConnectionStats::default();
        assert_eq!(stats.high_priority_count(), 0);
        assert_eq!(stats.normal_priority_count(), 0);
        assert_eq!(stats.bypass_count(), 0);
    }

    #[test]
    fn test_connection_stats_increment() {
        let stats = ConnectionStats::default();
        stats.high_priority_requests.fetch_add(5, Ordering::Relaxed);
        stats
            .normal_priority_requests
            .fetch_add(10, Ordering::Relaxed);
        stats.high_priority_bypasses.fetch_add(2, Ordering::Relaxed);

        assert_eq!(stats.high_priority_count(), 5);
        assert_eq!(stats.normal_priority_count(), 10);
        assert_eq!(stats.bypass_count(), 2);
    }
}
