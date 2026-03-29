//! Broker connection implementation.
//!
//! This module provides connection handling with support for:
//! - **Request priority**: High-priority requests (heartbeats, metadata) are processed
//!   before normal-priority requests to prevent consumer group ejection during backpressure.
//! - **Multi-connection bundles**: Multiple connections per broker for extreme high-throughput.
//! - **TLS/SSL encryption**: Automatic TLS upgrade when configured.
//! - **SASL authentication**: PLAIN, SCRAM-SHA-256/512, AWS MSK IAM handshake on connect.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpSocket;
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::time::timeout;
use tracing::{debug, error, info, trace, warn};

use crate::CorrelationId;
use crate::auth::{AuthConfig, SaslMechanism, SecurityProtocol, connect_tls};
use crate::error::{KrafkaError, Result};
use crate::protocol::{
    ApiKey, ApiVersionRange, ApiVersionsRequest, ApiVersionsResponse, Decoder, Encoder,
    RequestHeader, ResponseHeader, SaslAuthenticateRequest, SaslAuthenticateResponse,
    SaslHandshakeRequest, SaslHandshakeResponse,
};
use crate::util::CorrelationIdGenerator;

use super::secure::SaslAuthenticator;

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
    /// Maximum response size in bytes.
    ///
    /// Responses larger than this are rejected to prevent excessive memory allocation.
    /// Default: 100 MB (matching `MAX_MESSAGE_SIZE`).
    pub max_response_size: usize,
    /// Authentication configuration (optional).
    ///
    /// When set, the connection will perform TLS upgrade and/or SASL
    /// authentication handshake during establishment.
    pub auth: Option<AuthConfig>,
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
            max_response_size: crate::protocol::MAX_MESSAGE_SIZE,
            auth: None,
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

    /// Set the maximum response size in bytes.
    ///
    /// Responses exceeding this limit are rejected. Default: 100 MB.
    pub fn max_response_size(mut self, size: usize) -> Self {
        self.0.max_response_size = size.max(1024); // at least 1 KB
        self
    }

    /// Set authentication configuration.
    ///
    /// When set, the connection will perform TLS upgrade and/or SASL
    /// authentication handshake during establishment.
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.0.auth = Some(auth);
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
    /// When this request was sent, for per-request timeout enforcement.
    sent_at: Instant,
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
    /// Send data without registering a pending response (fire-and-forget).
    ///
    /// Used for `acks=0` produce requests where the broker sends no response.
    /// The data is written to the wire without inserting into the pending map.
    FireAndForget { data: Bytes },
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
    ///
    /// When `config.auth` is set, the connection will:
    /// 1. Establish a TCP connection
    /// 2. Upgrade to TLS if required by the security protocol
    /// 3. Perform SASL authentication handshake if required
    /// 4. Fetch API versions
    pub async fn connect(address: &str, config: ConnectionConfig) -> Result<Self> {
        // Use tokio::net::lookup_host to support both IP:port and hostname:port
        // (e.g. "kafka:9092" when brokers run inside containers).
        // Bound DNS resolution by the connect timeout so a slow resolver cannot
        // make connect() block indefinitely.
        let mut addrs = timeout(config.connect_timeout, tokio::net::lookup_host(address))
            .await
            .map_err(|_| KrafkaError::timeout("DNS resolution"))?
            .map_err(KrafkaError::Network)?;
        let first_addr = addrs.next().ok_or_else(|| {
            KrafkaError::invalid_state(format!("no addresses resolved for '{address}'"))
        })?;
        // Prefer IPv4 when available — IPv6 may be resolved first but not routable.
        let addr = addrs.find(|a| a.is_ipv4()).unwrap_or(first_addr);

        let socket = if addr.is_ipv6() {
            TcpSocket::new_v6()
        } else {
            TcpSocket::new_v4()
        }
        .map_err(KrafkaError::Network)?;

        // Apply socket buffer sizes before connecting
        if let Some(size) = config.send_buffer_size {
            socket
                .set_send_buffer_size(size as u32)
                .map_err(KrafkaError::Network)?;
        }
        if let Some(size) = config.recv_buffer_size {
            socket
                .set_recv_buffer_size(size as u32)
                .map_err(KrafkaError::Network)?;
        }

        let stream = timeout(config.connect_timeout, socket.connect(addr))
            .await
            .map_err(|_| KrafkaError::timeout("connection"))?
            .map_err(KrafkaError::Network)?;

        stream.set_nodelay(config.nodelay)?;

        debug!("Connected to broker at {address}");

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

        let request_timeout = config.request_timeout;

        // Determine if we need TLS and/or SASL
        let needs_tls = config.auth.as_ref().is_some_and(|a| a.requires_tls());
        let needs_sasl = config.auth.as_ref().is_some_and(|a| a.requires_sasl());

        if needs_tls {
            // TLS path: upgrade stream then optionally do SASL
            let auth = config.auth.as_ref().unwrap();
            let tls_config = auth
                .tls_config
                .as_ref()
                .ok_or_else(|| KrafkaError::config("TLS required but no TLS config provided"))?;

            // Extract hostname (without port) for TLS SNI.
            // Handle IPv6 bracket notation like [::1]:9092.
            let hostname = extract_sni_hostname(address);
            let tls_stream = connect_tls(stream, hostname, tls_config).await?;

            info!("TLS handshake completed for {address}");

            if needs_sasl {
                // TLS + SASL: authenticate on the TLS stream, then run event loop
                let mut tls_stream = tls_stream;
                Self::perform_sasl_handshake(
                    &mut tls_stream,
                    auth,
                    address,
                    &config.client_id,
                    config.max_response_size,
                )
                .await?;

                // Spawn the connection task with TLS stream
                let (reader, writer) = tokio::io::split(tls_stream);
                tokio::spawn(async move {
                    if let Err(e) = Self::run_connection_loop(
                        reader,
                        writer,
                        high_priority_rx,
                        normal_priority_rx,
                        request_timeout,
                        stats_clone,
                    )
                    .await
                    {
                        error!("Connection error: {e}");
                    }
                    alive_clone.store(false, std::sync::atomic::Ordering::SeqCst);
                });
            } else {
                // TLS only, no SASL
                let (reader, writer) = tokio::io::split(tls_stream);
                tokio::spawn(async move {
                    if let Err(e) = Self::run_connection_loop(
                        reader,
                        writer,
                        high_priority_rx,
                        normal_priority_rx,
                        request_timeout,
                        stats_clone,
                    )
                    .await
                    {
                        error!("Connection error: {e}");
                    }
                    alive_clone.store(false, std::sync::atomic::Ordering::SeqCst);
                });
            }
        } else if needs_sasl {
            // SASL without TLS
            let auth = config.auth.as_ref().unwrap();
            let mut stream = stream;
            Self::perform_sasl_handshake(
                &mut stream,
                auth,
                address,
                &config.client_id,
                config.max_response_size,
            )
            .await?;

            let (reader, writer) = stream.into_split();
            tokio::spawn(async move {
                if let Err(e) = Self::run_connection_loop(
                    reader,
                    writer,
                    high_priority_rx,
                    normal_priority_rx,
                    request_timeout,
                    stats_clone,
                )
                .await
                {
                    error!("Connection error: {e}");
                }
                alive_clone.store(false, std::sync::atomic::Ordering::SeqCst);
            });
        } else {
            // Plain TCP — fast path (most common for local dev)
            let (reader, writer) = stream.into_split();
            tokio::spawn(async move {
                if let Err(e) = Self::run_connection_loop(
                    reader,
                    writer,
                    high_priority_rx,
                    normal_priority_rx,
                    request_timeout,
                    stats_clone,
                )
                .await
                {
                    error!("Connection error: {e}");
                }
                alive_clone.store(false, std::sync::atomic::Ordering::SeqCst);
            });
        }

        // Fetch API versions
        connection.fetch_api_versions().await?;

        Ok(connection)
    }

    /// Perform the SASL handshake and authentication on a stream.
    ///
    /// This sends:
    /// 1. SaslHandshake request to negotiate the mechanism
    /// 2. SaslAuthenticate request(s) for the actual authentication
    ///
    /// For multi-step mechanisms (SCRAM-SHA-*), the challenge-response
    /// loop is handled automatically.
    async fn perform_sasl_handshake<S>(
        stream: &mut S,
        auth: &AuthConfig,
        address: &str,
        client_id: &str,
        max_response_size: usize,
    ) -> Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut authenticator = SaslAuthenticator::new(auth)
            .ok_or_else(|| KrafkaError::auth("Failed to create SASL authenticator"))?;

        // Warn about SASL PLAIN over cleartext — credentials sent unencrypted
        if auth.security_protocol == SecurityProtocol::SaslPlaintext
            && auth.sasl_mechanism == Some(SaslMechanism::Plain)
        {
            warn!(
                "SASL PLAIN credentials will be sent in cleartext to {}. \
                 Use SASL_SSL (sasl_plain_ssl) for production environments.",
                address
            );
        }

        // For MSK IAM, set the broker host
        let hostname = address.split(':').next().unwrap_or(address);
        authenticator.set_msk_host(auth, hostname);

        let mechanism_name = authenticator.mechanism_name().to_string();

        debug!("Starting SASL handshake with mechanism {mechanism_name} for {address}");

        // Step 1: SaslHandshake request
        let handshake_request = SaslHandshakeRequest::new(&mechanism_name);
        let mut encoder = Encoder::new();
        let pos = encoder.start_message();
        let header = RequestHeader::new(ApiKey::SaslHandshake, 1, 0).with_client_id(client_id);
        header.encode_v1(encoder.buffer_mut());
        handshake_request.encode_v1(encoder.buffer_mut());
        encoder.finish_message(pos);

        stream
            .write_all(&encoder.take())
            .await
            .map_err(KrafkaError::Network)?;
        stream.flush().await.map_err(KrafkaError::Network)?;

        // Read handshake response
        let response_bytes = Self::read_framed_response(stream, max_response_size).await?;
        let mut response_buf = response_bytes.clone();
        let _header = ResponseHeader::decode(&mut response_buf, ApiKey::SaslHandshake, 1)?;

        let handshake_response = SaslHandshakeResponse::decode_v0(&mut response_buf)?;
        if !handshake_response.is_ok() {
            return Err(KrafkaError::auth(format!(
                "SASL handshake failed: {:?}. Broker supports: {:?}",
                handshake_response.error_code, handshake_response.enabled_mechanisms
            )));
        }

        debug!(
            "SASL handshake accepted mechanism {mechanism_name}, broker supports: {:?}",
            handshake_response.enabled_mechanisms
        );

        // Step 2: SaslAuthenticate - initial response
        let initial_bytes = authenticator.initial_response();
        Self::send_sasl_authenticate(stream, &initial_bytes, client_id).await?;

        let auth_response =
            Self::read_sasl_authenticate_response(stream, max_response_size).await?;
        if !auth_response.error_code.is_ok() {
            return Err(KrafkaError::auth(format!(
                "SASL authentication failed: {:?} - {}",
                auth_response.error_code,
                auth_response.error_message.unwrap_or_default()
            )));
        }

        // Step 3: Challenge-response loop (for SCRAM-SHA-*)
        if !authenticator.is_complete() {
            let mut challenge = auth_response.auth_bytes;

            while let Some(response_bytes) = authenticator.process_challenge(&challenge)? {
                Self::send_sasl_authenticate(stream, &response_bytes, client_id).await?;
                let resp = Self::read_sasl_authenticate_response(stream, max_response_size).await?;
                if !resp.error_code.is_ok() {
                    return Err(KrafkaError::auth(format!(
                        "SASL authentication step failed: {:?} - {}",
                        resp.error_code,
                        resp.error_message.unwrap_or_default()
                    )));
                }
                challenge = resp.auth_bytes;

                if authenticator.is_complete() {
                    break;
                }
            }
        }

        info!("SASL authentication completed ({mechanism_name}) for {address}");
        Ok(())
    }

    /// Send a SaslAuthenticate request on a raw stream.
    async fn send_sasl_authenticate<S>(
        stream: &mut S,
        auth_bytes: &[u8],
        client_id: &str,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let request = SaslAuthenticateRequest::new(auth_bytes.to_vec());
        let mut encoder = Encoder::new();
        let pos = encoder.start_message();
        let header = RequestHeader::new(ApiKey::SaslAuthenticate, 0, 1).with_client_id(client_id);
        header.encode(encoder.buffer_mut());
        request.encode_v0(encoder.buffer_mut());
        encoder.finish_message(pos);

        stream
            .write_all(&encoder.take())
            .await
            .map_err(KrafkaError::Network)?;
        stream.flush().await.map_err(KrafkaError::Network)?;
        Ok(())
    }

    /// Read a SaslAuthenticate response from a raw stream.
    async fn read_sasl_authenticate_response<S>(
        stream: &mut S,
        max_response_size: usize,
    ) -> Result<SaslAuthenticateResponse>
    where
        S: AsyncRead + Unpin,
    {
        let response_bytes = Self::read_framed_response(stream, max_response_size).await?;
        let mut buf = response_bytes.clone();
        let _header = ResponseHeader::decode(&mut buf, ApiKey::SaslAuthenticate, 0)?;
        SaslAuthenticateResponse::decode_v0(&mut buf)
    }

    /// Read a length-prefixed Kafka response from a stream.
    async fn read_framed_response<S>(stream: &mut S, max_response_size: usize) -> Result<Bytes>
    where
        S: AsyncRead + Unpin,
    {
        // Read 4-byte length prefix
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(KrafkaError::Network)?;
        let len_i32 = i32::from_be_bytes(len_buf);

        if len_i32 <= 0 || (len_i32 as usize) > max_response_size {
            return Err(KrafkaError::protocol(format!(
                "Invalid response length: {len_i32} (max: {max_response_size})"
            )));
        }

        let len = len_i32 as usize;

        // Read the response body
        let mut body = vec![0u8; len];
        stream
            .read_exact(&mut body)
            .await
            .map_err(KrafkaError::Network)?;

        Ok(Bytes::from(body))
    }

    /// Run the connection event loop with priority handling.
    ///
    /// This is generic over the stream type, supporting both plain TCP and TLS.
    /// High-priority requests are always checked first using try_recv,
    /// ensuring heartbeats are never starved by backpressure on data requests.
    async fn run_connection_loop<R, W>(
        mut reader: R,
        mut writer: W,
        mut high_priority_rx: mpsc::Receiver<ConnectionCommand>,
        mut normal_priority_rx: mpsc::Receiver<ConnectionCommand>,
        request_timeout: Duration,
        stats: Arc<ConnectionStats>,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let pending: Arc<Mutex<HashMap<CorrelationId, PendingRequest>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let pending_clone = pending.clone();

        // Per-request timeout sweep: check every 1s for timed-out in-flight requests
        let pending_for_timeout = pending.clone();
        let timeout_sweep_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                let now = Instant::now();
                let mut pending_map = pending_for_timeout.lock().await;
                let timed_out: Vec<CorrelationId> = pending_map
                    .iter()
                    .filter(|(_, req)| now.duration_since(req.sent_at) > request_timeout)
                    .map(|(&id, _)| id)
                    .collect();
                for id in timed_out {
                    if let Some(req) = pending_map.remove(&id) {
                        warn!(
                            correlation_id = id,
                            "Request timed out after {:?}", request_timeout
                        );
                        let _ = req.response_tx.send(Err(KrafkaError::timeout(format!(
                            "request {} timed out after {:?}",
                            id, request_timeout
                        ))));
                    }
                }
            }
        });

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
        timeout_sweep_handle.abort();
        let _ = reader_handle.await;

        // Drain pending requests and notify callers that the connection is closed
        {
            let mut pending_map = pending.lock().await;
            for (_, req) in pending_map.drain() {
                let _ = req
                    .response_tx
                    .send(Err(KrafkaError::invalid_state("connection closed")));
            }
        }

        Ok(())
    }

    /// Handle a single connection command.
    ///
    /// Returns `true` if the connection should close.
    async fn handle_command<W: AsyncWrite + Unpin>(
        writer: &mut W,
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
                            sent_at: Instant::now(),
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
                    // Propagate flush failure to the pending request so the caller
                    // doesn't hang waiting for a response that will never arrive.
                    let mut pending = pending.lock().await;
                    if let Some(req) = pending.remove(&correlation_id) {
                        let _ = req.response_tx.send(Err(KrafkaError::Network(e)));
                    }
                    return Ok(false);
                }
                Ok(false)
            }
            ConnectionCommand::Close => {
                debug!("Closing connection");
                Ok(true)
            }
            ConnectionCommand::FireAndForget { data } => {
                if let Err(e) = writer.write_all(&data).await {
                    error!("Fire-and-forget write error: {}", e);
                }
                if let Err(e) = writer.flush().await {
                    error!("Fire-and-forget flush error: {}", e);
                }
                Ok(false)
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

    /// Send a request without waiting for a response (fire-and-forget).
    ///
    /// Used for `acks=0` produce requests where the Kafka broker does not
    /// send a response. The request is written to the wire but no response
    /// channel is registered in the pending map, avoiding resource leaks.
    pub async fn send_fire_and_forget(
        &self,
        api_key: ApiKey,
        api_version: i16,
        request_body: impl FnOnce(&mut BytesMut),
    ) -> Result<()> {
        let correlation_id = self.correlation_id_gen.next();
        let mut encoder = Encoder::new();

        // Build request
        let pos = encoder.start_message();
        let header = RequestHeader::new(api_key, api_version, correlation_id)
            .with_client_id(&self.config.client_id);
        header.encode(encoder.buffer_mut());
        request_body(encoder.buffer_mut());
        encoder.finish_message(pos);

        // Send as fire-and-forget — no pending entry is created
        let channel = self.channel_for_priority(RequestPriority::Normal);
        channel
            .send(ConnectionCommand::FireAndForget {
                data: encoder.take(),
            })
            .await
            .map_err(|_| KrafkaError::invalid_state("connection closed"))?;

        self.stats
            .normal_priority_requests
            .fetch_add(1, Ordering::Relaxed);

        Ok(())
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
        // Only attempt close if a Tokio runtime is active — avoids panic
        // when dropped outside a runtime (e.g., during process exit or in tests).
        if let Ok(_handle) = tokio::runtime::Handle::try_current() {
            let tx = self.high_priority_tx.clone();
            tokio::spawn(async move {
                let _ = tx.send(ConnectionCommand::Close).await;
            });
        }
    }
}

// Re-export from shared utility for local use and tests
use crate::util::extract_sni_hostname;

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
        assert!(config.auth.is_none());
    }

    #[test]
    fn test_connection_config_with_auth() {
        use crate::auth::AuthConfig;
        let config = ConnectionConfig::builder()
            .client_id("test")
            .auth(AuthConfig::sasl_plain("user", "pass"))
            .build();

        assert_eq!(config.client_id, "test");
        let auth = config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(!auth.requires_tls());
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

    /// Mock Kafka broker that handles the SASL handshake protocol.
    ///
    /// Accepts a connection, reads SaslHandshakeRequest, SaslAuthenticateRequest,
    /// and ApiVersionsRequest, responding to each with valid responses.
    /// Returns the captured auth bytes from SaslAuthenticate for verification.
    async fn run_mock_sasl_broker(listener: tokio::net::TcpListener) -> (String, Vec<u8>) {
        use bytes::BufMut;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (mut stream, _) = listener.accept().await.unwrap();

        // Helper: read a length-prefixed Kafka frame
        async fn read_frame(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await.unwrap();
            let len = i32::from_be_bytes(len_buf) as usize;
            let mut body = vec![0u8; len];
            stream.read_exact(&mut body).await.unwrap();
            body
        }

        // Helper: write a length-prefixed Kafka frame
        async fn write_frame(stream: &mut tokio::net::TcpStream, data: &[u8]) {
            let len = data.len() as i32;
            stream.write_all(&len.to_be_bytes()).await.unwrap();
            stream.write_all(data).await.unwrap();
            stream.flush().await.unwrap();
        }

        // 1. Read SaslHandshakeRequest
        let req = read_frame(&mut stream).await;
        // Parse: api_key(2) + api_version(2) + correlation_id(4) = bytes[4..8]
        let correlation_id = i32::from_be_bytes(req[4..8].try_into().unwrap());
        // Parse mechanism name: skip header (api_key + version + corr_id + client_id)
        // client_id is a KafkaString: i16 len + bytes
        let client_id_len = i16::from_be_bytes(req[8..10].try_into().unwrap());
        let mech_offset = if client_id_len < 0 {
            10 // null client_id
        } else {
            10 + client_id_len as usize
        };
        let mech_len =
            i16::from_be_bytes(req[mech_offset..mech_offset + 2].try_into().unwrap()) as usize;
        let mechanism =
            String::from_utf8(req[mech_offset + 2..mech_offset + 2 + mech_len].to_vec()).unwrap();

        // Send SaslHandshakeResponse: correlation_id + error_code(0) + 1 mechanism
        let mut resp = BytesMut::new();
        resp.put_i32(correlation_id);
        resp.put_i16(0); // error_code = NONE
        resp.put_i32(1); // 1 enabled mechanism
        let mech_bytes = mechanism.as_bytes();
        resp.put_i16(mech_bytes.len() as i16);
        resp.put_slice(mech_bytes);
        write_frame(&mut stream, &resp).await;

        // 2. Read SaslAuthenticateRequest
        let req = read_frame(&mut stream).await;
        let correlation_id = i32::from_be_bytes(req[4..8].try_into().unwrap());
        // Parse auth_bytes: skip header, find KafkaBytes (i32 len + bytes)
        let client_id_len = i16::from_be_bytes(req[8..10].try_into().unwrap());
        let auth_offset = if client_id_len < 0 {
            10
        } else {
            10 + client_id_len as usize
        };
        let auth_bytes_len =
            i32::from_be_bytes(req[auth_offset..auth_offset + 4].try_into().unwrap()) as usize;
        let auth_bytes = req[auth_offset + 4..auth_offset + 4 + auth_bytes_len].to_vec();

        // Send SaslAuthenticateResponse: correlation_id + error_code(0) + null message + empty bytes
        let mut resp = BytesMut::new();
        resp.put_i32(correlation_id);
        resp.put_i16(0); // error_code = NONE
        resp.put_i16(-1_i16); // error_message = null (KafkaString)
        resp.put_i32(0); // auth_bytes = empty (KafkaBytes, 0 length)
        write_frame(&mut stream, &resp).await;

        // 3. Read ApiVersionsRequest
        let req = read_frame(&mut stream).await;
        let correlation_id = i32::from_be_bytes(req[4..8].try_into().unwrap());

        // Send ApiVersionsResponse: correlation_id + error_code(0) + 0 api keys
        let mut resp = BytesMut::new();
        resp.put_i32(correlation_id);
        resp.put_i16(0); // error_code
        resp.put_i32(0); // 0 api keys
        write_frame(&mut stream, &resp).await;

        (mechanism, auth_bytes)
    }

    #[tokio::test]
    async fn test_sasl_plain_handshake_with_mock_broker() {
        // Start a mock broker
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let addr_str = addr.to_string();

        // Run mock broker in background
        let mock_handle = tokio::spawn(run_mock_sasl_broker(listener));

        // Connect with SASL/PLAIN auth
        let config = ConnectionConfig::builder()
            .client_id("test-client")
            .auth(crate::auth::AuthConfig::sasl_plain(
                "testuser",
                "testpassword",
            ))
            .build();

        let conn = BrokerConnection::connect(&addr_str, config).await;
        assert!(
            conn.is_ok(),
            "Connection with SASL/PLAIN should succeed: {:?}",
            conn.err()
        );

        let conn = conn.unwrap();
        assert!(conn.is_alive());

        // Verify the mock received the correct handshake
        let (mechanism, auth_bytes) = mock_handle.await.unwrap();
        assert_eq!(mechanism, "PLAIN");

        // SASL PLAIN format: \0username\0password
        assert_eq!(auth_bytes, b"\0testuser\0testpassword");

        conn.close().await;
    }

    #[tokio::test]
    async fn test_no_sasl_handshake_without_auth() {
        // Start a mock broker that only handles ApiVersionsRequest (no SASL)
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let addr_str = addr.to_string();

        let mock_handle = tokio::spawn(async move {
            use bytes::BufMut;
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let (mut stream, _) = listener.accept().await.unwrap();

            // Should receive ApiVersionsRequest directly (no SASL handshake)
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await.unwrap();
            let len = i32::from_be_bytes(len_buf) as usize;
            let mut body = vec![0u8; len];
            stream.read_exact(&mut body).await.unwrap();

            // Verify it's ApiVersions (api_key = 18), not SaslHandshake (api_key = 17)
            let api_key = i16::from_be_bytes(body[0..2].try_into().unwrap());
            let correlation_id = i32::from_be_bytes(body[4..8].try_into().unwrap());

            // Send ApiVersionsResponse
            let mut resp = BytesMut::new();
            resp.put_i32(correlation_id);
            resp.put_i16(0); // error_code
            resp.put_i32(0); // 0 api keys
            let len = resp.len() as i32;
            stream.write_all(&len.to_be_bytes()).await.unwrap();
            stream.write_all(&resp).await.unwrap();
            stream.flush().await.unwrap();

            api_key
        });

        // Connect without auth
        let config = ConnectionConfig::builder().client_id("test-client").build();

        let conn = BrokerConnection::connect(&addr_str, config).await;
        assert!(conn.is_ok());

        let api_key = mock_handle.await.unwrap();
        assert_eq!(
            api_key, 18,
            "First request without auth should be ApiVersions (18), not SaslHandshake (17)"
        );

        conn.unwrap().close().await;
    }

    #[tokio::test]
    async fn test_sasl_handshake_failure_rejects_connection() {
        // Mock broker that rejects the SASL handshake
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let addr_str = addr.to_string();

        tokio::spawn(async move {
            use bytes::BufMut;
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let (mut stream, _) = listener.accept().await.unwrap();

            // Read SaslHandshakeRequest
            let mut len_buf = [0u8; 4];
            stream.read_exact(&mut len_buf).await.unwrap();
            let len = i32::from_be_bytes(len_buf) as usize;
            let mut body = vec![0u8; len];
            stream.read_exact(&mut body).await.unwrap();
            let correlation_id = i32::from_be_bytes(body[4..8].try_into().unwrap());

            // Send error response (unsupported mechanism, error_code = 33)
            let mut resp = BytesMut::new();
            resp.put_i32(correlation_id);
            resp.put_i16(33); // error_code = UNSUPPORTED_SASL_MECHANISM
            resp.put_i32(1); // 1 supported mechanism
            let mech = b"GSSAPI";
            resp.put_i16(mech.len() as i16);
            resp.put_slice(mech);
            let len = resp.len() as i32;
            stream.write_all(&len.to_be_bytes()).await.unwrap();
            stream.write_all(&resp).await.unwrap();
            stream.flush().await.unwrap();
        });

        let config = ConnectionConfig::builder()
            .client_id("test-client")
            .auth(crate::auth::AuthConfig::sasl_plain("user", "pass"))
            .build();

        let result = BrokerConnection::connect(&addr_str, config).await;
        assert!(
            result.is_err(),
            "Connection should fail when SASL handshake is rejected"
        );
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("Expected error"),
        };
        assert!(
            err.to_string().contains("SASL handshake failed"),
            "Error should mention SASL handshake failure: {err}"
        );
    }

    #[tokio::test]
    async fn test_sasl_auth_failure_rejects_connection() {
        // Mock broker that accepts handshake but rejects authentication
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let addr_str = addr.to_string();

        tokio::spawn(async move {
            use bytes::BufMut;
            use tokio::io::{AsyncReadExt, AsyncWriteExt};

            let (mut stream, _) = listener.accept().await.unwrap();

            // Helper
            async fn read_frame(stream: &mut tokio::net::TcpStream) -> Vec<u8> {
                let mut len_buf = [0u8; 4];
                stream.read_exact(&mut len_buf).await.unwrap();
                let len = i32::from_be_bytes(len_buf) as usize;
                let mut body = vec![0u8; len];
                stream.read_exact(&mut body).await.unwrap();
                body
            }

            // 1. SaslHandshake — accept
            let req = read_frame(&mut stream).await;
            let correlation_id = i32::from_be_bytes(req[4..8].try_into().unwrap());
            let mut resp = BytesMut::new();
            resp.put_i32(correlation_id);
            resp.put_i16(0); // OK
            resp.put_i32(1);
            let mech = b"PLAIN";
            resp.put_i16(mech.len() as i16);
            resp.put_slice(mech);
            let len = resp.len() as i32;
            stream.write_all(&len.to_be_bytes()).await.unwrap();
            stream.write_all(&resp).await.unwrap();
            stream.flush().await.unwrap();

            // 2. SaslAuthenticate — reject with auth error
            let req = read_frame(&mut stream).await;
            let correlation_id = i32::from_be_bytes(req[4..8].try_into().unwrap());
            let mut resp = BytesMut::new();
            resp.put_i32(correlation_id);
            resp.put_i16(58); // error_code = SASL_AUTHENTICATION_FAILED
            // error_message: "Authentication failed"
            let msg = b"Authentication failed";
            resp.put_i16(msg.len() as i16);
            resp.put_slice(msg);
            resp.put_i32(0); // empty auth_bytes
            let len = resp.len() as i32;
            stream.write_all(&len.to_be_bytes()).await.unwrap();
            stream.write_all(&resp).await.unwrap();
            stream.flush().await.unwrap();
        });

        let config = ConnectionConfig::builder()
            .client_id("test-client")
            .auth(crate::auth::AuthConfig::sasl_plain("user", "wrongpass"))
            .build();

        let result = BrokerConnection::connect(&addr_str, config).await;
        assert!(
            result.is_err(),
            "Connection should fail when authentication is rejected"
        );
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("Expected error"),
        };
        assert!(
            err.to_string().contains("authentication failed")
                || err.to_string().contains("Authentication failed"),
            "Error should mention auth failure: {err}"
        );
    }

    #[test]
    fn test_connection_config_socket_buffer_sizes() {
        let mut config = ConnectionConfig::default();
        assert!(config.send_buffer_size.is_none());
        assert!(config.recv_buffer_size.is_none());

        config.send_buffer_size = Some(1024 * 1024);
        config.recv_buffer_size = Some(512 * 1024);
        assert_eq!(config.send_buffer_size, Some(1024 * 1024));
        assert_eq!(config.recv_buffer_size, Some(512 * 1024));
    }

    #[tokio::test]
    async fn test_connection_invalid_address_format() {
        let config = ConnectionConfig::default();
        // Invalid address (not a valid SocketAddr) should return an error
        let result = BrokerConnection::connect("not-a-valid-address", config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_read_framed_response_rejects_negative_length() {
        // Simulate a stream that sends a negative i32 as the length prefix
        let data: [u8; 4] = (-1i32).to_be_bytes();
        let mut cursor = std::io::Cursor::new(data);
        let result =
            BrokerConnection::read_framed_response(&mut cursor, crate::protocol::MAX_MESSAGE_SIZE)
                .await;
        assert!(result.is_err(), "negative frame length should be rejected");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("Invalid response length: -1"),
            "error should show negative value: {err_msg}"
        );
    }

    #[tokio::test]
    async fn test_read_framed_response_rejects_zero_length() {
        let data: [u8; 4] = 0i32.to_be_bytes();
        let mut cursor = std::io::Cursor::new(data);
        let result =
            BrokerConnection::read_framed_response(&mut cursor, crate::protocol::MAX_MESSAGE_SIZE)
                .await;
        assert!(result.is_err(), "zero frame length should be rejected");
    }

    #[test]
    fn test_connection_config_default_max_response_size() {
        let config = ConnectionConfig::default();
        assert_eq!(
            config.max_response_size,
            100 * 1024 * 1024,
            "default max_response_size should be MAX_MESSAGE_SIZE (100 MB)"
        );
        assert_eq!(
            config.max_response_size,
            crate::protocol::MAX_MESSAGE_SIZE,
            "default max_response_size should equal protocol::MAX_MESSAGE_SIZE"
        );
    }

    #[test]
    fn test_connection_config_builder_max_response_size() {
        let config = ConnectionConfig::builder()
            .max_response_size(50 * 1024 * 1024)
            .build();
        assert_eq!(
            config.max_response_size,
            50 * 1024 * 1024,
            "max_response_size should be settable via builder"
        );
    }

    #[test]
    fn test_connection_config_builder_max_response_size_minimum() {
        // Setting a value below 1024 should be clamped to 1024
        let config = ConnectionConfig::builder().max_response_size(100).build();
        assert_eq!(
            config.max_response_size, 1024,
            "max_response_size should be clamped to minimum of 1024 bytes"
        );

        let config_zero = ConnectionConfig::builder().max_response_size(0).build();
        assert_eq!(
            config_zero.max_response_size, 1024,
            "max_response_size(0) should clamp to 1024"
        );
    }

    #[tokio::test]
    async fn test_connect_resolves_hostname() {
        // Bind a TCP listener so we have a real port to connect to.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        // Use "localhost" (a hostname, not an IP) to verify DNS resolution works.
        let hostname_addr = format!("localhost:{port}");
        let config = ConnectionConfig::builder()
            .connect_timeout(Duration::from_secs(2))
            .request_timeout(Duration::from_secs(2))
            .build();

        // The connect will resolve "localhost" via lookup_host, establish TCP,
        // then fail on the ApiVersions handshake because our listener doesn't
        // speak the Kafka protocol — but it must NOT fail with an address
        // parsing error.
        let result = BrokerConnection::connect(&hostname_addr, config).await;
        match result {
            Ok(_) => {} // Unlikely but acceptable — means the mock spoke enough Kafka
            Err(err) => {
                let err_msg = format!("{err}");
                assert!(
                    !err_msg.contains("invalid address"),
                    "should not fail on address resolution, got: {err_msg}"
                );
            }
        }
    }

    #[tokio::test]
    async fn test_connect_dns_failure_is_retriable() {
        let config = ConnectionConfig::builder()
            .connect_timeout(Duration::from_secs(5))
            .build();
        let result =
            BrokerConnection::connect("this-host-does-not-exist.invalid:9092", config).await;
        match result {
            Ok(_) => panic!("connect to non-existent host should fail"),
            Err(err) => {
                assert!(
                    err.is_retriable(),
                    "DNS resolution failure should be retriable (Network), got: {err}"
                );
            }
        }
    }

    #[test]
    fn test_extract_sni_hostname_ipv4() {
        assert_eq!(extract_sni_hostname("192.168.1.1:9092"), "192.168.1.1");
    }

    #[test]
    fn test_extract_sni_hostname_hostname() {
        assert_eq!(
            extract_sni_hostname("broker.example.com:9092"),
            "broker.example.com"
        );
    }

    #[test]
    fn test_extract_sni_hostname_ipv6_brackets() {
        assert_eq!(extract_sni_hostname("[::1]:9092"), "::1");
    }

    #[test]
    fn test_extract_sni_hostname_ipv6_full() {
        assert_eq!(extract_sni_hostname("[2001:db8::1]:9092"), "2001:db8::1");
    }

    #[test]
    fn test_extract_sni_hostname_ipv6_unbracketed() {
        // `2001:db8::1:9092` is a valid 8-group IPv6 address, so the function
        // correctly returns it as-is. Use bracket notation `[2001:db8::1]:9092`
        // to unambiguously separate host from port.
        assert_eq!(extract_sni_hostname("2001:db8::1:9092"), "2001:db8::1:9092");
        // When the string is NOT a valid IPv6 address, the last :segment
        // is stripped as a port.
        assert_eq!(extract_sni_hostname("2001:db8::zz:9092"), "2001:db8::zz");
    }

    #[test]
    fn test_extract_sni_hostname_ipv6_no_port() {
        assert_eq!(extract_sni_hostname("2001:db8::1"), "2001:db8::1");
    }

    #[test]
    fn test_extract_sni_hostname_no_port() {
        assert_eq!(
            extract_sni_hostname("broker.example.com"),
            "broker.example.com"
        );
    }
}
