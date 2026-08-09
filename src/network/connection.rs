//! Broker connection implementation.
//!
//! This module provides connection handling with support for:
//! - **Request priority**: High-priority requests (heartbeats, metadata) are processed
//!   before normal-priority requests to prevent consumer group ejection during backpressure.
//! - **TLS/SSL encryption**: Automatic TLS upgrade when configured.
//! - **SASL authentication**: PLAIN, SCRAM-SHA-256/512, AWS MSK IAM handshake on connect.

use ahash::AHashMap;
use futures::FutureExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime};

use arc_swap::ArcSwap;
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
#[cfg(feature = "socks5")]
use tokio::net::TcpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::time::timeout;
#[cfg(feature = "socks5")]
use tokio::time::timeout_at;
use tokio_rustls::TlsConnector;
use tokio_util::time::{DelayQueue, delay_queue};
use tracing::{debug, error, info, trace, warn};

use crate::CorrelationId;
use crate::auth::msk_iam::MAX_SIGV4_CLOCK_SKEW_SECS;
use crate::auth::tls::build_tls_connector;
use crate::auth::{
    AuthConfig, ChannelBinding, SaslMechanism, SecurityProtocol, connect_tls,
    extract_tls_server_end_point,
};
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::metrics::ConnectionMetrics;

/// Named parameter bundle for a connection event-loop task.
///
/// Replacing a 13-positional-argument macro with a named struct eliminates
/// the risk of silent argument transpositions at call sites.  Every field
/// has the same type as before; they just carry explicit names now.
struct ConnectionLoopParams {
    /// Broker address string used in log messages.
    address: String,
    /// Receiver end of the high-priority request channel.
    high_priority_rx: mpsc::Receiver<ConnectionCommand>,
    /// Receiver end of the normal-priority request channel.
    normal_priority_rx: mpsc::Receiver<ConnectionCommand>,
    /// Per-request timeout applied via the `DelayQueue` timer wheel.
    request_timeout: Duration,
    /// Shared connection statistics counters.
    stats: Arc<ConnectionStats>,
    /// Shared connection metrics (latency, error counts, etc.).
    metrics: Arc<ConnectionMetrics>,
    /// Maximum frame size the reader will accept before closing the connection.
    max_response_size: usize,
    /// Maximum number of concurrently in-flight requests.
    max_in_flight_requests: usize,
    /// How many high-priority requests may bypass normal-priority in a row.
    max_high_priority_bypasses: usize,
}

/// SOCKS5 proxy configuration for connecting to brokers through a proxy.
///
/// When set on a [`ConnectionConfig`], all TCP connections to Kafka brokers
/// are tunneled through the specified SOCKS5 proxy. The proxy performs DNS
/// resolution of the broker address, which is essential for VPN/bastion
/// setups where broker hostnames are not resolvable from the client network.
///
/// TLS and SASL authentication are layered on top of the proxied connection
/// transparently — no additional configuration is needed.
///
/// # Example
///
/// Set it on the client directly:
///
/// ```rust,no_run
/// use krafka::network::ProxyConfig;
/// use krafka::producer::Producer;
///
/// # async fn example() -> Result<(), krafka::error::KrafkaError> {
/// let producer = Producer::builder()
///     .bootstrap_servers("broker:9092")
///     .proxy(ProxyConfig::new("bastion:1080"))
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// Or on a shared [`TransportConfig`](crate::network::TransportConfig), which
/// is the right shape when several clients travel the same network path — the
/// proxy is a property of the path, not of the client:
///
/// ```rust,no_run
/// use krafka::network::{ProxyConfig, TransportConfig};
///
/// # fn example() -> Result<(), krafka::error::KrafkaError> {
/// let transport = TransportConfig::builder()
///     .proxy(ProxyConfig::new("bastion:1080"))
///     .build()?;
/// # let _ = transport;
/// # Ok(())
/// # }
/// ```
///
/// Both write to the same place: `ProducerBuilder::proxy` and its siblings are
/// shorthand for setting it on the builder's transport config, so there is no
/// precedence rule to get wrong.
///
/// The previous example here built a [`ConnectionConfig`], which no client
/// builder accepts — a reader who followed it reached a type they could not
/// use.
#[cfg(feature = "socks5")]
#[derive(Clone)]
pub struct ProxyConfig {
    /// SOCKS5 proxy address (`host:port`).
    address: String,
    /// Optional proxy authentication credentials.
    credentials: Option<ProxyCredentials>,
}

#[cfg(feature = "socks5")]
impl ProxyConfig {
    /// Create a new SOCKS5 proxy configuration.
    pub fn new(address: impl Into<String>) -> Self {
        Self {
            address: address.into(),
            credentials: None,
        }
    }

    /// Create a SOCKS5 proxy configuration with username/password authentication.
    pub fn with_credentials(
        address: impl Into<String>,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            address: address.into(),
            credentials: Some(ProxyCredentials {
                username: zeroize::Zeroizing::new(username.into()),
                password: zeroize::Zeroizing::new(password.into()),
            }),
        }
    }

    /// Returns the proxy address.
    #[inline]
    pub fn address(&self) -> &str {
        &self.address
    }

    /// Returns the proxy credentials, if set.
    #[inline]
    pub fn credentials(&self) -> Option<&ProxyCredentials> {
        self.credentials.as_ref()
    }
}

#[cfg(feature = "socks5")]
impl std::fmt::Debug for ProxyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyConfig")
            .field("address", &self.address)
            .field(
                "credentials",
                if self.credentials.is_some() {
                    &"[REDACTED]"
                } else {
                    &"None"
                },
            )
            .finish()
    }
}

/// Credentials for SOCKS5 proxy authentication.
///
/// Both fields are stored as [`zeroize::Zeroizing<String>`] so that the
/// password (and username) are reliably scrubbed from memory when the struct
/// drops or is cloned — including any intermediate copies produced by the
/// SOCKS5 handshake path.
#[cfg(feature = "socks5")]
#[derive(Clone, zeroize::ZeroizeOnDrop)]
pub struct ProxyCredentials {
    /// Proxy username.
    username: zeroize::Zeroizing<String>,
    /// Proxy password — stored as `Zeroizing<String>` so that any copy of this
    /// field is also zeroed on drop, providing defense-in-depth beyond the
    /// struct-level `ZeroizeOnDrop`.
    password: zeroize::Zeroizing<String>,
}

#[cfg(feature = "socks5")]
impl ProxyCredentials {
    /// Returns the proxy username.
    #[inline]
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Returns the proxy password.
    #[inline]
    pub fn password(&self) -> &str {
        &self.password
    }
}

#[cfg(feature = "socks5")]
impl std::fmt::Debug for ProxyCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProxyCredentials")
            .field("username", &self.username.as_str())
            .field("password", &"[REDACTED]")
            .finish()
    }
}
use crate::protocol::{
    ApiKey, ApiVersionRange, ApiVersionsRequest, ApiVersionsResponse, Decoder, Encoder,
    FinalizedFeature, RequestHeader, ResponseHeader, SaslAuthenticateRequest,
    SaslAuthenticateResponse, SaslHandshakeRequest, SaslHandshakeResponse, SupportedFeature,
};
use crate::util::{CorrelationIdGenerator, NO_RESPONSE_CORRELATION_ID, extract_sni_hostname};

use super::secure::{ChallengeResponse, SaslAuthenticator};

/// Request priority level.
///
/// High-priority requests are processed before normal-priority requests,
/// which is critical for preventing consumer group ejection during backpressure.
#[non_exhaustive]
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
            // Group coordination — must not be delayed behind produce/fetch
            // backpressure.  Heartbeat delays > session.timeout.ms trigger
            // rebalances; JoinGroup/SyncGroup delays stall the entire group;
            // LeaveGroup delays leave stale entries in the coordinator;
            // OffsetCommit delays risk duplicate delivery on restart.
            // ShareGroupHeartbeat (KIP-932) has the same session-timeout
            // sensitivity as ConsumerGroupHeartbeat — missing it here would
            // cause share group evictions under produce/fetch backpressure.
            ApiKey::Heartbeat
            | ApiKey::ConsumerGroupHeartbeat
            | ApiKey::ShareGroupHeartbeat
            | ApiKey::JoinGroup
            | ApiKey::SyncGroup
            | ApiKey::LeaveGroup
            | ApiKey::OffsetCommit => Self::High,
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
///
/// Use [`ConnectionConfig::builder()`] or [`Default::default()`] to construct.
/// Call [`init_tls()`](ConnectionConfig::init_tls) after building when TLS is
/// configured to pre-build and cache the TLS connector, avoiding repeated disk
/// I/O for certificates on every reconnection.
///
/// # Memory Sizing
///
/// The theoretical per-connection memory ceiling is:
///
/// ```text
/// max_response_size × max_in_flight_requests
/// ```
///
/// With the defaults (100 MB × 10 = **1 GB**) that ceiling is rarely
/// approached in practice because the broker limits outstanding fetches via
/// `fetch.max.bytes`; however, for high-throughput consumer deployments you
/// should size these values intentionally:
///
/// | Workload | `max_response_size` | `max_in_flight_requests` | Ceiling |
/// |----------|--------------------|--------------------------|---------||
/// | Default  | 100 MB             | 10                       | 1 GB    |
/// | Consumer | 50 MB              | 16                       | 800 MB  |
/// | Producer | 10 MB              | 5 (idempotent)           | 50 MB   |
///
/// The Java client defaults to `fetch.max.bytes = 50 MB` and
/// `max.in.flight.requests.per.connection = 5`.  Consider lowering these
/// values to match the Java defaults if RSS is a concern.
/// Default time allowed for TCP establishment to one broker.
///
/// This is also the floor on `request_timeout`: a request cannot be given less
/// time than the connection it travels over is allowed to take, or it would
/// expire before the handshake could finish. Clients that want a request
/// timeout below this must lower `connect_timeout` to match — every client
/// builder exposes a `connect_timeout` setter for exactly that.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct ConnectionConfig {
    /// Connection timeout. See [`DEFAULT_CONNECT_TIMEOUT`].
    pub(crate) connect_timeout: Duration,
    /// Request timeout.
    pub(crate) request_timeout: Duration,
    /// Socket send buffer size.
    pub(crate) send_buffer_size: Option<usize>,
    /// Socket receive buffer size.
    pub(crate) recv_buffer_size: Option<usize>,
    /// TCP nodelay.
    pub(crate) nodelay: bool,
    /// Client ID.
    pub(crate) client_id: String,
    /// High-priority channel capacity for heartbeats and metadata requests.
    ///
    /// This should be small since high-priority requests should be rare.
    pub(crate) high_priority_channel_capacity: usize,
    /// Normal-priority channel capacity for produce and fetch requests.
    pub(crate) normal_priority_channel_capacity: usize,
    /// Maximum response size in bytes.
    ///
    /// Responses larger than this are rejected to prevent excessive memory allocation.
    /// Default: 100 MB (matching `MAX_MESSAGE_SIZE`).
    pub(crate) max_response_size: usize,
    /// Maximum number of in-flight requests per connection.
    ///
    /// When this limit is reached, new requests are rejected with an error
    /// until existing requests complete or time out. This prevents unbounded
    /// memory growth from a stalled broker or runaway producer.
    ///
    /// Default: 10. This matches the Kafka Java client default and common Go
    /// client defaults (franz-go). Use 5 for idempotent producers to match
    /// Kafka's `max.in.flight.requests.per.connection` safety guarantee.
    /// Use 1 for strictly-ordered partitions.
    pub(crate) max_in_flight_requests: usize,
    /// Maximum consecutive high-priority commands the event loop processes
    /// before forcing one normal-priority drain.
    ///
    /// Higher values give heartbeats stronger priority at the cost of
    /// potentially delaying produce/fetch requests under heavy load.
    /// Default: 4.
    pub(crate) max_high_priority_bypasses_per_round: usize,
    /// Authentication configuration (optional).
    ///
    /// When set, the connection will perform TLS upgrade and/or SASL
    /// authentication handshake during establishment.
    pub(crate) auth: Option<AuthConfig>,
    /// Cached TLS connector built from [`AuthConfig::tls_config`].
    ///
    /// Populated by [`init_tls()`](ConnectionConfig::init_tls). When present,
    /// connections reuse this connector instead of reading certificate files
    /// from disk on every connection attempt.
    ///
    /// Wrapped in `Arc<ArcSwap<…>>` so that all clones of this config share
    /// the same connector and [`refresh_tls()`](ConnectionConfig::refresh_tls)
    /// atomically updates it for every future connection.
    pub(crate) tls_connector: Arc<ArcSwap<Option<TlsConnector>>>,
    /// TCP keepalive interval.
    ///
    /// When set, enables TCP keepalive on all broker connections with the
    /// given interval. This prevents idle connections from being silently
    /// dropped by firewalls and load balancers.
    pub(crate) tcp_keepalive: Option<Duration>,
    /// Happy Eyeballs connection attempt delay (RFC 8305 §5).
    ///
    /// The delay between staggered connection attempts when racing multiple
    /// addresses. Clamped to 100 ms – 2 s at connect time (RFC 8305 §5).
    /// Default: 250 ms.
    pub(crate) connection_attempt_delay: Duration,
    /// Shared clock offset for MSK IAM signing (seconds).
    ///
    /// When SASL/MSK_IAM authentication fails with a signature-mismatch
    /// that looks like clock skew, the connection layer stores the estimated
    /// offset here.  Subsequent reconnection attempts apply this offset to
    /// `SystemTime::now()` so the SigV4 timestamp matches the broker's
    /// clock.  Default: 0 (no adjustment).
    ///
    /// Relaxed ordering is correct: the offset is a single best-effort
    /// correction value; there is no dependent data that must be synchronised
    /// with it, and the only adverse consequence of a torn read is re-sending
    /// one slightly-off signature (which the broker will reject with another
    /// clock-skew error, triggering another update).
    pub(crate) msk_iam_clock_offset_secs: Arc<AtomicI64>,
    /// Shared connection metrics recorded by broker connections created from this config.
    pub(crate) connection_metrics: Arc<ConnectionMetrics>,
    /// SOCKS5 proxy configuration (optional).
    ///
    /// When set, all connections are tunneled through the proxy.
    /// Requires the `socks5` feature.
    #[cfg(feature = "socks5")]
    pub(crate) proxy: Option<ProxyConfig>,
}

impl std::fmt::Debug for ConnectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("ConnectionConfig");
        s.field("connect_timeout", &self.connect_timeout)
            .field("request_timeout", &self.request_timeout)
            .field("send_buffer_size", &self.send_buffer_size)
            .field("recv_buffer_size", &self.recv_buffer_size)
            .field("nodelay", &self.nodelay)
            .field("client_id", &self.client_id)
            .field(
                "high_priority_channel_capacity",
                &self.high_priority_channel_capacity,
            )
            .field(
                "normal_priority_channel_capacity",
                &self.normal_priority_channel_capacity,
            )
            .field("max_response_size", &self.max_response_size)
            .field("max_in_flight_requests", &self.max_in_flight_requests)
            .field(
                "max_high_priority_bypasses_per_round",
                &self.max_high_priority_bypasses_per_round,
            )
            .field("auth", &self.auth)
            .field("tls_connector", &self.tls_connector.load().is_some())
            .field("tcp_keepalive", &self.tcp_keepalive)
            .field("connection_attempt_delay", &self.connection_attempt_delay)
            .field(
                "msk_iam_clock_offset_secs",
                &self.msk_iam_clock_offset_secs.load(Ordering::Relaxed),
            );
        #[cfg(feature = "socks5")]
        s.field("proxy", &self.proxy);
        s.finish()
    }
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        // SAFETY: the default builder values satisfy all validation invariants.
        #[allow(clippy::expect_used)]
        ConnectionConfigBuilder::default()
            .build()
            .expect("default ConnectionConfig values are always valid")
    }
}

impl ConnectionConfig {
    /// Create a new connection config builder.
    pub fn builder() -> ConnectionConfigBuilder {
        ConnectionConfigBuilder::default()
    }

    /// Pre-build and cache the TLS connector from the configured certificates.
    ///
    /// When TLS is configured, this reads the certificate and key files once
    /// (via `spawn_blocking`) and stores the resulting [`TlsConnector`] for
    /// reuse across all connections and reconnections. Without this call,
    /// every connection attempt re-reads the files from disk.
    ///
    /// This is a no-op when no TLS configuration is present.
    ///
    /// # Errors
    ///
    /// Returns an error if certificate or key files cannot be read or parsed.
    pub async fn init_tls(&mut self) -> Result<()> {
        if let Some(ref auth) = self.auth
            && let Some(ref tls_config) = auth.tls_config
        {
            let connector = build_tls_connector(tls_config).await?;
            self.tls_connector.store(Arc::new(Some(connector)));
        }
        Ok(())
    }

    /// Re-read certificate files from disk and atomically replace the cached
    /// TLS connector.
    ///
    /// All future connections (including reconnections from the pool) will use
    /// the new certificates. Existing TLS sessions are unaffected — they
    /// continue using the connector that was active at handshake time.
    ///
    /// Call this after rotating certificates on disk, or on a periodic timer
    /// (e.g. once per hour) to pick up renewed certificates without a client
    /// restart.
    ///
    /// This is a no-op when no TLS configuration is present.
    ///
    /// # Errors
    ///
    /// Returns an error if the new certificate or key files cannot be read or
    /// parsed. The existing (old) connector remains active on failure.
    pub async fn refresh_tls(&self) -> Result<()> {
        if let Some(ref auth) = self.auth
            && let Some(ref tls_config) = auth.tls_config
        {
            let connector = build_tls_connector(tls_config).await?;
            self.tls_connector.store(Arc::new(Some(connector)));
            info!("TLS connector refreshed from disk");
        }
        Ok(())
    }

    /// Returns the connection timeout.
    #[inline]
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns the request timeout.
    #[inline]
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Returns the socket send buffer size, if set.
    #[inline]
    pub fn send_buffer_size(&self) -> Option<usize> {
        self.send_buffer_size
    }

    /// Returns the socket receive buffer size, if set.
    #[inline]
    pub fn recv_buffer_size(&self) -> Option<usize> {
        self.recv_buffer_size
    }

    /// Returns whether TCP nodelay is enabled.
    #[inline]
    pub fn nodelay(&self) -> bool {
        self.nodelay
    }

    /// Returns the client ID.
    #[inline]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the high-priority channel capacity.
    #[inline]
    pub fn high_priority_channel_capacity(&self) -> usize {
        self.high_priority_channel_capacity
    }

    /// Returns the normal-priority channel capacity.
    #[inline]
    pub fn normal_priority_channel_capacity(&self) -> usize {
        self.normal_priority_channel_capacity
    }

    /// Returns the maximum response size in bytes.
    #[inline]
    pub fn max_response_size(&self) -> usize {
        self.max_response_size
    }

    /// Returns the maximum number of in-flight requests per connection.
    #[inline]
    pub fn max_in_flight_requests(&self) -> usize {
        self.max_in_flight_requests
    }

    /// Returns the authentication configuration, if set.
    #[inline]
    pub fn auth(&self) -> Option<&AuthConfig> {
        self.auth.as_ref()
    }

    /// Returns the Happy Eyeballs connection attempt delay.
    #[inline]
    pub fn connection_attempt_delay(&self) -> Duration {
        self.connection_attempt_delay
    }

    /// Returns the shared connection metrics handle.
    #[inline]
    pub fn connection_metrics(&self) -> Arc<ConnectionMetrics> {
        self.connection_metrics.clone()
    }

    /// Returns the SOCKS5 proxy configuration, if set.
    ///
    /// Requires the `socks5` feature.
    #[cfg(feature = "socks5")]
    #[inline]
    pub fn proxy(&self) -> Option<&ProxyConfig> {
        self.proxy.as_ref()
    }
}

/// Builder for ConnectionConfig.
#[must_use = "builders do nothing until .build() is called"]
#[derive(Debug)]
pub struct ConnectionConfigBuilder(ConnectionConfig);

impl Default for ConnectionConfigBuilder {
    /// Creates a builder pre-populated with production-safe defaults.
    ///
    /// These are the **sole** authoritative default values for every field.
    /// `ConnectionConfig::default()` delegates here and runs `build()`, so
    /// all defaults are validated by the same rules as user-supplied configs.
    fn default() -> Self {
        ConnectionConfigBuilder(ConnectionConfig {
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            request_timeout: Duration::from_secs(30),
            send_buffer_size: None,
            recv_buffer_size: None,
            nodelay: true,
            client_id: "krafka".to_string(),
            high_priority_channel_capacity: 64,
            normal_priority_channel_capacity: 256,
            max_response_size: crate::protocol::MAX_MESSAGE_SIZE,
            max_in_flight_requests: 10,
            max_high_priority_bypasses_per_round: 4,
            auth: None,
            tls_connector: Arc::new(ArcSwap::new(Arc::new(None))),
            tcp_keepalive: Some(Duration::from_secs(60)),
            connection_attempt_delay: Duration::from_millis(250),
            msk_iam_clock_offset_secs: Arc::new(AtomicI64::new(0)),
            connection_metrics: Arc::new(ConnectionMetrics::default()),
            #[cfg(feature = "socks5")]
            proxy: None,
        })
    }
}

impl ConnectionConfigBuilder {
    /// Set the connect timeout: how long TCP establishment to one broker may
    /// take. Default: [`DEFAULT_CONNECT_TIMEOUT`].
    ///
    /// [`build`](Self::build) rejects a `request_timeout` shorter than this, so
    /// lowering `connect_timeout` is what makes a short `request_timeout`
    /// possible.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.0.connect_timeout = timeout;
        self
    }

    /// Set the request timeout: how long one in-flight request may wait for its
    /// response. Default: 30 s.
    ///
    /// Must be at least [`connect_timeout`](Self::connect_timeout), since the
    /// request's clock covers establishing the connection it is sent over.
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

    /// Set `SO_SNDBUF` for every broker socket, or `None` for the OS default.
    ///
    /// See [`TransportConfig::socket_send_buffer`](crate::network::TransportConfig::socket_send_buffer),
    /// which is how a client builder reaches this.
    pub fn socket_send_buffer(mut self, bytes: Option<usize>) -> Self {
        self.0.send_buffer_size = bytes;
        self
    }

    /// Set `SO_RCVBUF` for every broker socket, or `None` for the OS default.
    ///
    /// See [`TransportConfig::socket_receive_buffer`](crate::network::TransportConfig::socket_receive_buffer).
    pub fn socket_receive_buffer(mut self, bytes: Option<usize>) -> Self {
        self.0.recv_buffer_size = bytes;
        self
    }

    /// Set the maximum number of in-flight requests per connection.
    ///
    /// Limits the number of requests waiting for a response on a single
    /// connection. Default: 10. This matches the Kafka Java client default.
    ///
    /// # Idempotent / transactional producers
    ///
    /// Kafka's `max.in.flight.requests.per.connection ≤ 5` rule exists because
    /// the Java client permits several batches per *partition* to be on the
    /// wire at once and has to repair their order after a retry.
    ///
    /// krafka does not: the record accumulator admits exactly one batch per
    /// partition, dispatched in the order batches were sealed, so a partition's
    /// sequence order and its wire order cannot diverge. This setting is
    /// therefore purely a transport concern — how many requests may be
    /// outstanding on one socket — and raising it weakens no ordering or
    /// idempotence guarantee.
    pub fn max_in_flight_requests(mut self, max: usize) -> Self {
        self.0.max_in_flight_requests = max.max(1);
        self
    }

    /// Set the maximum consecutive high-priority commands processed before
    /// forcing one normal-priority drain.
    ///
    /// Higher values let heartbeats and metadata requests cut through
    /// backpressure more aggressively at the cost of slightly higher
    /// produce/fetch latency under heavy load. Must be at least 1.
    /// Default: 4.
    pub fn max_high_priority_bypasses_per_round(mut self, n: usize) -> Self {
        self.0.max_high_priority_bypasses_per_round = n.max(1);
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

    /// Set the TCP keepalive interval.
    ///
    /// When set, enables TCP keepalive on all broker connections.
    /// Pass `None` to disable keepalive. Default: 60 seconds.
    pub fn tcp_keepalive(mut self, interval: Option<Duration>) -> Self {
        self.0.tcp_keepalive = interval;
        self
    }

    /// Set the Happy Eyeballs connection attempt delay (RFC 8305 §5).
    ///
    /// This controls the stagger interval between parallel connection
    /// attempts. Clamped to 100 ms – 2 s at connect time.
    /// Default: 250 ms.
    pub fn connection_attempt_delay(mut self, delay: Duration) -> Self {
        self.0.connection_attempt_delay = delay;
        self
    }

    /// Set the shared connection metrics handle.
    pub fn connection_metrics(mut self, metrics: Arc<ConnectionMetrics>) -> Self {
        self.0.connection_metrics = metrics;
        self
    }

    /// Set SOCKS5 proxy configuration.
    ///
    /// When set, all connections are tunneled through the specified SOCKS5
    /// proxy. The proxy performs DNS resolution, which is essential for
    /// VPN/bastion setups where broker hostnames are not directly resolvable.
    ///
    /// Requires the `socks5` feature.
    #[cfg(feature = "socks5")]
    pub fn proxy(mut self, proxy: ProxyConfig) -> Self {
        self.0.proxy = Some(proxy);
        self
    }

    /// Build the config.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `client_id` exceeds the Kafka wire limit (`i16::MAX` = 32 767 bytes).
    /// - `request_timeout` is shorter than `connect_timeout` (requests would
    ///   always time out before the TCP handshake completes).
    pub fn build(self) -> crate::error::Result<ConnectionConfig> {
        const MAX_CLIENT_ID_LEN: usize = i16::MAX as usize;
        if self.0.client_id.len() > MAX_CLIENT_ID_LEN {
            return Err(crate::error::KrafkaError::config(format!(
                "client_id is {} bytes, exceeding the Kafka wire limit of {MAX_CLIENT_ID_LEN}",
                self.0.client_id.len()
            )));
        }
        if self.0.request_timeout < self.0.connect_timeout {
            return Err(crate::error::KrafkaError::config(format!(
                "request_timeout ({:?}) must be >= connect_timeout ({:?}); \
                 otherwise all requests time out before the connection completes. \
                 Lower connect_timeout to match if you want a shorter request_timeout",
                self.0.request_timeout, self.0.connect_timeout
            )));
        }

        // Warn when the theoretical per-connection memory ceiling exceeds 1 GiB.
        // The ceiling is max_response_size × max_in_flight_requests; it is rarely
        // reached in practice but can be surprising in high-concurrency setups.
        const WARN_CEILING_BYTES: usize = 1024 * 1024 * 1024; // 1 GiB
        let ceiling = self
            .0
            .max_response_size
            .saturating_mul(self.0.max_in_flight_requests);
        if ceiling > WARN_CEILING_BYTES {
            tracing::warn!(
                max_response_size = self.0.max_response_size,
                max_in_flight_requests = self.0.max_in_flight_requests,
                ceiling_bytes = ceiling,
                "ConnectionConfig memory ceiling ({} × {} = {} bytes) exceeds 1 GiB; \
                 consider lowering max_response_size or max_in_flight_requests",
                self.0.max_response_size,
                self.0.max_in_flight_requests,
                ceiling,
            );
        }

        Ok(self.0)
    }
}

/// Maximum frame size accepted during the pre-authentication SASL handshake.
///
/// The handshake exchanges a `SaslHandshakeResponse` (an error code plus a
/// mechanism-name list) and `SaslAuthenticateResponse`s (an error message plus
/// a SCRAM/OAUTHBEARER challenge). Real frames run to a few hundred bytes;
/// 64 KiB is generous by three orders of magnitude.
///
/// This is deliberately **not** `max_response_size` (100 MiB by default). An
/// unauthenticated peer must never be able to make the client allocate a
/// 100 MiB buffer on its say-so — that is a ~1 600 000× pre-auth memory
/// amplifier against a single 4-byte length prefix, per connection.
pub(crate) const MAX_SASL_FRAME_BYTES: usize = 64 * 1024;

/// Chunk size used when incrementally reading a framed handshake response.
///
/// The declared length is not trusted enough to pre-size the buffer; bytes are
/// appended as they arrive, so a peer that declares a large frame and then
/// dribbles only ever holds the memory it has actually sent.
const HANDSHAKE_READ_CHUNK: usize = 8 * 1024;

/// Largest broker-requested throttle this client will honour, in milliseconds.
///
/// A real quota delay is at most one quota window, which brokers cap well below
/// this. The bound exists because the value is read by peeking the response's
/// leading INT32 rather than by decoding the whole body: if a version table
/// entry were ever wrong, the peeked bytes would be an array length or an
/// error code, and an unbounded value would stall the connection for hours.
/// Clamping turns that class of mistake into a bounded, observable delay.
const MAX_HONOURED_THROTTLE_MS: i32 = 5 * 60 * 1000;

/// Read `throttle_time_ms` out of a response body without decoding it (KIP-219).
///
/// Returns `None` when this API and version do not begin with the field, when
/// the body is too short, or when the value is not a plausible throttle. See
/// [`ApiKey::leading_throttle_time_min_version`] for which APIs qualify and why
/// `Produce` is not one of them.
fn leading_throttle_time_ms(api_key: ApiKey, api_version: i16, body: &[u8]) -> Option<i32> {
    if api_version < api_key.leading_throttle_time_min_version()? {
        return None;
    }
    let bytes: [u8; 4] = body.get(..4)?.try_into().ok()?;
    let throttle_time_ms = i32::from_be_bytes(bytes);
    if throttle_time_ms > 0 && throttle_time_ms <= MAX_HONOURED_THROTTLE_MS {
        Some(throttle_time_ms)
    } else {
        None
    }
}

/// How long [`BrokerConnection::close`] waits on a full high-priority channel.
///
/// `close()` must never block shutdown indefinitely; a connection whose event
/// loop is wedged in `write_all` is exactly the case that matters.
const CLOSE_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// The canonical error for "this connection is gone".
///
/// Connection loss is **routine and recoverable**: a broker rolling restart or
/// a broker-side `connections.max.idle.ms` reap produces a clean EOF on a
/// perfectly healthy client. It is therefore reported as
/// [`KrafkaError::Network`], which [`KrafkaError::is_retriable`] classifies as
/// retriable — never as `InvalidState`, which falls into the `_ => false` arm
/// and tells callers a fully recoverable event is permanent.
fn connection_closed_error() -> KrafkaError {
    KrafkaError::network(std::io::Error::new(
        std::io::ErrorKind::ConnectionReset,
        "connection closed",
    ))
}

/// Feature levels reported by a broker in its `ApiVersions` response (KIP-584).
///
/// Populated during the connection handshake when the broker accepted
/// `ApiVersions` v3 or newer; the fields ride in that response's tagged fields
/// and simply do not exist at v0–v2, so a pre-KIP-584 broker leaves this empty.
///
/// Read it with [`BrokerConnection::broker_features`]. The common use is
/// gating optional behaviour on a cluster-wide finalized level, e.g. only
/// attempting KIP-890 transaction semantics when `transaction.version` is
/// finalized at 2 or higher.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct BrokerFeatures {
    /// The `ApiVersions` version that was actually negotiated for this
    /// connection. Below 3 the remaining fields are always empty.
    pub negotiated_version: i16,
    /// Feature ranges this individual broker can support.
    pub supported: Vec<SupportedFeature>,
    /// Epoch of [`Self::finalized`]. `-1` means the broker reported none.
    pub finalized_epoch: i64,
    /// Cluster-wide finalized feature levels. Only meaningful when
    /// [`Self::finalized_epoch`] is non-negative.
    pub finalized: Vec<FinalizedFeature>,
}

impl BrokerFeatures {
    /// Cluster-wide finalized max level for `name`, if the cluster reported one.
    ///
    /// Returns `None` both when the feature is absent and when the broker
    /// reported no finalized features at all, so callers get one uniform
    /// "unknown — do not assume" answer instead of having to check the epoch
    /// separately.
    #[must_use]
    pub fn finalized_level(&self, name: &str) -> Option<i16> {
        if self.finalized_epoch < 0 {
            return None;
        }
        self.finalized
            .iter()
            .find(|f| f.name == name)
            .map(|f| f.max_version_level)
    }

    /// Version range this broker advertises for `name`, if any.
    #[must_use]
    pub fn supported_range(&self, name: &str) -> Option<(i16, i16)> {
        self.supported
            .iter()
            .find(|f| f.name == name)
            .map(|f| (f.min_version, f.max_version))
    }
}

/// A pending request waiting for a response.
struct PendingRequest {
    response_tx: oneshot::Sender<Result<Bytes>>,
    api_key: ApiKey,
    api_version: i16,
    /// In-flight slot held for the lifetime of this request.
    ///
    /// Dropped when the entry leaves the pending map (response dispatched,
    /// timeout fired, or connection drained), which is what releases a
    /// submitter blocked in [`BrokerConnection::send_request_with_priority`].
    _permit: tokio::sync::OwnedSemaphorePermit,
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
        /// Wall-clock budget for writing this request and awaiting its
        /// response.
        ///
        /// Normally the connection's `request_timeout`, but APIs the broker
        /// legitimately parks (JoinGroup during a group rebalance) carry a
        /// longer budget so the event loop does not expire a request the
        /// broker is still holding on purpose.
        timeout: Duration,
        /// In-flight slot acquired by the submitter before enqueueing.
        ///
        /// Holding a permit *before* the channel send is what turns the
        /// in-flight cap into real backpressure: the submitter waits for a
        /// free slot instead of the event loop rejecting a request that has
        /// already been queued.
        permit: tokio::sync::OwnedSemaphorePermit,
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
    api_versions: Arc<parking_lot::Mutex<AHashMap<ApiKey, ApiVersionRange>>>,
    /// Broker/cluster feature levels learned from the ApiVersions handshake
    /// (KIP-584). Empty when the broker only spoke ApiVersions v0-v2.
    broker_features: Arc<parking_lot::Mutex<BrokerFeatures>>,
    /// Whether the connection is alive.
    alive: Arc<std::sync::atomic::AtomicBool>,
    /// When the SASL session expires (KIP-368).
    ///
    /// `None` when authentication is not used or the broker reported a
    /// session lifetime of zero (no expiry).
    session_expiry: Option<Instant>,
    /// Statistics for monitoring.
    stats: Arc<ConnectionStats>,
    /// KIP-219: deadline until which normal-priority requests should be
    /// delayed because the broker signalled quota throttling.
    throttle_until: Arc<parking_lot::Mutex<Instant>>,
    /// Instant anchor used with `last_used_nanos` to compute idle duration
    /// without locking. Set once at connect time; never mutated.
    created_at: Instant,
    /// Monotonic-nanoseconds since `created_at` of the last submitted
    /// request. Updated on every `send_request_with_priority` and
    /// `send_fire_and_forget` entry. Read by `ConnectionPool::evict_idle`
    /// to decide whether a connection has been idle past
    /// `connections.max.idle.ms`. An `AtomicU64` rather than a lock keeps
    /// the network hot path free of contention; the only race is two
    /// concurrent senders both storing a "recent" value, which is fine
    /// because either observer still reads "recently used".
    last_used_nanos: AtomicU64,
    /// One permit per allowed in-flight request.
    ///
    /// Submitters acquire an owned permit *before* enqueueing, and the permit
    /// travels with the request into the pending map, so it is released only
    /// when the request finally resolves. This makes `max_in_flight_requests`
    /// a blocking backpressure limit rather than a rejection threshold — which
    /// is why the normal-priority channel (256 slots) can be far deeper than
    /// the in-flight cap (10 by default) without over-admitting work.
    in_flight: Arc<tokio::sync::Semaphore>,
}

/// Connection statistics for monitoring.
///
/// All counters are updated and read with [`Ordering::Relaxed`] — they are
/// monotonically incrementing metrics used only for observability (not for
/// synchronising other state), so no happens-before relationship is required.
#[derive(Debug, Default)]
#[non_exhaustive]
pub struct ConnectionStats {
    /// Total high-priority requests sent.
    pub high_priority_requests: AtomicU64,
    /// Total normal-priority requests sent.
    pub normal_priority_requests: AtomicU64,
    /// High-priority requests that bypassed the queue (processed immediately).
    pub high_priority_bypasses: AtomicU64,
    /// Number of times the loop yielded to normal-priority work after hitting
    /// the high-priority bypass budget.
    pub high_priority_bypass_yields: AtomicU64,
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

    /// Get the number of fairness yields after exhausting the bypass budget.
    #[inline]
    pub fn bypass_yield_count(&self) -> u64 {
        self.high_priority_bypass_yields.load(Ordering::Relaxed)
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
        // Establish TCP stream — either direct or through a SOCKS5 proxy.
        let stream = Self::establish_tcp(address, &config).await?;

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

        let mut connection = Self {
            address: address.to_string(),
            config: config.clone(),
            correlation_id_gen: Arc::new(CorrelationIdGenerator::new()),
            high_priority_tx,
            normal_priority_tx,
            api_versions: Arc::new(parking_lot::Mutex::new(AHashMap::new())),
            broker_features: Arc::new(parking_lot::Mutex::new(BrokerFeatures::default())),
            alive,
            session_expiry: None,
            stats,
            throttle_until: Arc::new(parking_lot::Mutex::new(Instant::now())),
            created_at: Instant::now(),
            last_used_nanos: AtomicU64::new(0),
            in_flight: Arc::new(tokio::sync::Semaphore::new(config.max_in_flight_requests)),
        };

        let request_timeout = config.request_timeout;

        // Deadline for everything after TCP establishment: TLS handshake and
        // the full SASL exchange. Measured from *now* (post-TCP) so a slow but
        // legitimate TCP connect does not eat the handshake's budget, while a
        // peer that completes TCP and then goes silent still cannot hang
        // `connect()` indefinitely.
        let handshake_deadline = tokio::time::Instant::now() + config.connect_timeout;

        // Build the event-loop parameter bundle once.  The struct is moved
        // into `spawn_connection_task` in whichever auth path executes —
        // the channels are consumed exactly once.
        let loop_params = ConnectionLoopParams {
            address: address.to_string(),
            high_priority_rx,
            normal_priority_rx,
            request_timeout,
            stats: stats_clone,
            metrics: config.connection_metrics.clone(),
            max_response_size: config.max_response_size,
            max_in_flight_requests: config.max_in_flight_requests,
            max_high_priority_bypasses: config.max_high_priority_bypasses_per_round,
        };

        // Determine auth requirements and dispatch to the appropriate path.
        // Using `filter` means `auth` is already in scope — no secondary
        // unreachable guard needed to re-establish the invariant.
        if let Some(auth) = config.auth.as_ref().filter(|a| a.requires_tls()) {
            // TLS path: upgrade stream then optionally do SASL
            let tls_config = auth
                .tls_config
                .as_ref()
                .ok_or_else(|| KrafkaError::config("TLS required but no TLS config provided"))?;

            // Use cached TLS connector or build one from config. Calling
            // `init_tls()` before first use avoids this fallback and the
            // repeated disk I/O it entails.
            let connector = match &**config.tls_connector.load() {
                Some(c) => c.clone(),
                None => build_tls_connector(tls_config).await?,
            };

            // Extract hostname (without port) for TLS SNI.
            // Handle IPv6 bracket notation like [::1]:9092.
            let hostname = extract_sni_hostname(address)?;
            let tls_start = std::time::Instant::now();
            // `connect_timeout` bounds TCP establishment only. A peer that
            // completes TCP and then stalls the TLS handshake would otherwise
            // hang `connect()` forever — and, via the pool's per-address
            // `connecting` slot, every other task targeting this broker.
            let tls_stream = tokio::time::timeout_at(
                handshake_deadline,
                connect_tls(
                    stream,
                    hostname,
                    tls_config.sni_hostname.as_deref(),
                    &connector,
                ),
            )
            .await
            .map_err(|_| {
                KrafkaError::timeout(format!(
                    "TLS handshake with {address} did not complete within {:?}",
                    config.connect_timeout
                ))
            })??;
            config
                .connection_metrics
                .record_tls_handshake(tls_start.elapsed());

            info!("TLS handshake completed for {address}");

            if auth.requires_sasl() {
                // TLS + SASL: authenticate on the TLS stream, then run event loop
                let mut tls_stream = tls_stream;

                // Extract tls-server-end-point channel binding data (RFC 5929 §4.1)
                // before the stream is consumed. This binds the SCRAM exchange to
                // this specific TLS session.
                // Channel binding is opt-out (`AuthConfig::with_scram_channel_binding`).
                // Default-on keeps the strongest behaviour; brokers that do not
                // implement RFC 5929 reject a bound exchange with an opaque
                // failure, and there is no in-band way to detect that, so the
                // fallback has to be an explicit configuration choice.
                let channel_binding = if auth.scram_channel_binding {
                    extract_tls_server_end_point(&tls_stream)
                        .map(ChannelBinding::TlsServerEndPoint)
                        .unwrap_or(ChannelBinding::None)
                } else {
                    debug!(
                        "SCRAM channel binding disabled by configuration for {address}; \
                         using unbound n,, GS2 framing"
                    );
                    ChannelBinding::None
                };

                let session_lifetime_ms = Self::perform_sasl_handshake(
                    &mut tls_stream,
                    auth,
                    address,
                    &config.client_id,
                    request_timeout,
                    handshake_deadline,
                    channel_binding,
                    &config.msk_iam_clock_offset_secs,
                )
                .await?;

                connection.session_expiry =
                    Self::effective_session_expiry(session_lifetime_ms, auth);

                // Spawn the connection task with TLS stream
                let (reader, writer) = tokio::io::split(tls_stream);
                config.connection_metrics.record_connect();
                Self::spawn_connection_task(reader, writer, loop_params, alive_clone);
            } else {
                // TLS only, no SASL
                let (reader, writer) = tokio::io::split(tls_stream);
                config.connection_metrics.record_connect();
                Self::spawn_connection_task(reader, writer, loop_params, alive_clone);
            }
        } else if let Some(auth) = config.auth.as_ref().filter(|a| a.requires_sasl()) {
            // SASL without TLS
            let mut stream = stream;
            let session_lifetime_ms = Self::perform_sasl_handshake(
                &mut stream,
                auth,
                address,
                &config.client_id,
                request_timeout,
                handshake_deadline,
                ChannelBinding::None,
                &config.msk_iam_clock_offset_secs,
            )
            .await?;

            connection.session_expiry = Self::effective_session_expiry(session_lifetime_ms, auth);

            let (reader, writer) = stream.into_split();
            config.connection_metrics.record_connect();
            Self::spawn_connection_task(reader, writer, loop_params, alive_clone);
        } else {
            // Plain TCP — fast path (most common for local dev)
            let (reader, writer) = stream.into_split();
            config.connection_metrics.record_connect();
            Self::spawn_connection_task(reader, writer, loop_params, alive_clone);
        }

        // Fetch API versions
        connection.fetch_api_versions().await?;

        Ok(connection)
    }

    /// Establish a TCP connection — direct or through a SOCKS5 proxy.
    async fn establish_tcp(
        address: &str,
        config: &ConnectionConfig,
    ) -> Result<tokio::net::TcpStream> {
        #[cfg(feature = "socks5")]
        if let Some(ref proxy) = config.proxy {
            return Self::connect_via_proxy(address, proxy, config).await;
        }

        Self::connect_direct(address, config).await
    }

    /// Direct TCP connection using Happy Eyeballs v2 (RFC 8305).
    ///
    /// Resolves DNS, interleaves IPv6/IPv4 addresses, and races staggered
    /// connection attempts — returning the first successful socket.
    async fn connect_direct(
        address: &str,
        config: &ConnectionConfig,
    ) -> Result<tokio::net::TcpStream> {
        super::happy_eyeballs::connect_happy_eyeballs(address, config).await
    }

    /// Connect through a SOCKS5 proxy.
    ///
    /// The proxy performs DNS resolution of the broker address, which is
    /// essential for VPN/bastion setups where broker hostnames are not
    /// resolvable from the client network.
    #[cfg(feature = "socks5")]
    async fn connect_via_proxy(
        address: &str,
        proxy: &ProxyConfig,
        config: &ConnectionConfig,
    ) -> Result<tokio::net::TcpStream> {
        use tokio_socks::tcp::Socks5Stream;

        debug!("Connecting to {address} via SOCKS5 proxy {}", proxy.address);

        // Use a single deadline for the entire proxy connect path (DNS + TCP + SOCKS5)
        // so the overall wall-clock time never exceeds connect_timeout.
        let deadline = tokio::time::Instant::now() + config.connect_timeout;

        // Resolve proxy address and create a socket with buffer sizes applied.
        let addrs: Vec<std::net::SocketAddr> =
            timeout_at(deadline, tokio::net::lookup_host(&proxy.address))
                .await
                .map_err(|_| KrafkaError::timeout("SOCKS5 proxy DNS resolution"))?
                .map_err(KrafkaError::network)?
                .collect();

        if addrs.is_empty() {
            return Err(KrafkaError::invalid_state(format!(
                "no addresses resolved for SOCKS5 proxy '{}'",
                proxy.address
            )));
        }

        // Try proxy addresses in resolver order.
        let proxy_addr = addrs[0];

        let socket = Self::create_socket(proxy_addr, config)?;

        // Connect to the proxy and perform the SOCKS5 handshake, bounded by
        // the remaining budget from the same deadline.
        let proxy_stream = timeout_at(deadline, async {
            let tcp = socket
                .connect(proxy_addr)
                .await
                .map_err(KrafkaError::network)?;

            // SOCKS5 handshake — pass the broker address as a string so the
            // proxy performs DNS resolution (remote resolution).
            let socks = if let Some(ref creds) = proxy.credentials {
                Socks5Stream::connect_with_password_and_socket(
                    tcp,
                    address,
                    creds.username(),
                    creds.password(),
                )
                .await
            } else {
                Socks5Stream::connect_with_socket(tcp, address).await
            }
            .map_err(|e| {
                KrafkaError::network(std::io::Error::other(format!("SOCKS5 proxy error: {e}")))
            })?;

            Ok::<_, KrafkaError>(socks.into_inner())
        })
        .await
        .map_err(|_| KrafkaError::timeout("SOCKS5 proxy connection"))??;

        info!(
            "SOCKS5 tunnel established to {address} via {}",
            proxy.address
        );

        Ok(proxy_stream)
    }

    /// Create a TCP socket for the given address with buffer sizes and keepalive applied.
    #[cfg(feature = "socks5")]
    fn create_socket(addr: std::net::SocketAddr, config: &ConnectionConfig) -> Result<TcpSocket> {
        super::happy_eyeballs::create_socket(addr, config)
    }

    /// Perform the SASL handshake and authentication on a stream.
    ///
    /// This sends:
    /// 1. SaslHandshake request to negotiate the mechanism
    /// 2. SaslAuthenticate request(s) for the actual authentication
    ///
    /// For multi-step mechanisms (SCRAM-SHA-*), the challenge-response
    /// loop is handled automatically.
    ///
    /// The `channel_binding` parameter is forwarded to the SCRAM client when
    /// the mechanism is SCRAM-SHA-*. Pass [`ChannelBinding::TlsServerEndPoint`]
    /// when the underlying transport is TLS, or [`ChannelBinding::None`] for
    /// plaintext SASL.
    ///
    /// Returns the session lifetime in milliseconds reported by the broker
    /// (KIP-368). A value of `0` means the broker does not enforce
    /// session expiry.
    #[allow(clippy::too_many_arguments)]
    async fn perform_sasl_handshake<S>(
        stream: &mut S,
        auth: &AuthConfig,
        address: &str,
        client_id: &str,
        request_timeout: Duration,
        deadline: tokio::time::Instant,
        channel_binding: ChannelBinding,
        msk_iam_clock_offset_secs: &Arc<AtomicI64>,
    ) -> Result<i64>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        // For MSK IAM with a credential provider, resolve fresh credentials
        // before creating the authenticator.
        let resolved_msk_iam;
        let auth = if let Some(resolved) = timeout(request_timeout, auth.resolve_msk_iam_provider())
            .await
            .map_err(|_| KrafkaError::timeout("MSK IAM credential provider"))??
        {
            debug!("Resolved MSK IAM credentials from provider for {address}");
            resolved_msk_iam = resolved;
            &resolved_msk_iam
        } else {
            auth
        };

        // For OAUTHBEARER with a provider, resolve a fresh token before
        // creating the authenticator (which is synchronous).
        // Apply the request timeout so a hung provider cannot stall reconnect loops.
        let resolved_auth;
        let auth = if let Some(resolved) =
            timeout(request_timeout, auth.resolve_provider_to_token())
                .await
                .map_err(|_| KrafkaError::timeout("OAUTHBEARER token provider"))??
        {
            debug!("Resolved OAUTHBEARER token from provider for {address}");
            resolved_auth = resolved;
            &resolved_auth
        } else {
            auth
        };

        let mut authenticator = SaslAuthenticator::new(auth, channel_binding)?
            .ok_or_else(|| KrafkaError::auth("Failed to create SASL authenticator"))?;

        // Warn about SASL PLAIN over cleartext — credentials sent unencrypted.
        // Emitted on every connect so misconfigurations are visible in logs
        // regardless of reconnect history.
        if auth.security_protocol == SecurityProtocol::SaslPlaintext
            && auth.sasl_mechanism == Some(SaslMechanism::Plain)
        {
            warn!(
                "SASL PLAIN credentials will be sent in cleartext to {}. \
                 Use SASL_SSL (sasl_plain_ssl) for production environments.",
                address
            );
        }

        // For MSK IAM, set the broker host (handles IPv6 brackets like [::1]:9092)
        let hostname = extract_sni_hostname(address)?;
        let clock_offset = msk_iam_clock_offset_secs.load(Ordering::Relaxed);
        authenticator.set_msk_host(auth, hostname, clock_offset)?;

        let mechanism_name = authenticator.mechanism_name().to_string();

        debug!("Starting SASL handshake with mechanism {mechanism_name} for {address}");

        // Step 1: SaslHandshake request
        let handshake_request = SaslHandshakeRequest::new(&mechanism_name);
        let mut encoder = Encoder::with_capacity(64);
        let pos = encoder.start_message();
        let header = RequestHeader::new(ApiKey::SaslHandshake, 1, 0).with_client_id(client_id);
        header.encode_v1(encoder.buffer_mut())?;
        handshake_request.encode_v1(encoder.buffer_mut())?;
        encoder.finish_message(pos)?;

        stream
            .write_all(&encoder.take())
            .await
            .map_err(KrafkaError::network)?;
        stream.flush().await.map_err(KrafkaError::network)?;

        // Read handshake response.
        //
        // Bounded in *time* by the handshake deadline and in *size* by
        // MAX_SASL_FRAME_BYTES. Neither bound existed before: an unauthenticated
        // peer could declare a 100 MiB frame and dribble bytes (pinning 100 MiB
        // of zeroed heap per connection, pre-auth), or simply never write at
        // all and hang connect() forever.
        let mut response_buf =
            Self::read_handshake_frame(stream, deadline, "SaslHandshake").await?;
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
        let initial_bytes = authenticator.initial_response()?;
        Self::send_sasl_authenticate(stream, &initial_bytes, client_id).await?;

        let auth_response = Self::read_sasl_authenticate_response(stream, deadline).await?;
        if !auth_response.error_code.is_ok() {
            let err_msg = auth_response.error_message.unwrap_or_default();
            // Best-effort clock skew detection for MSK IAM.
            // AWS SigV4 errors for clock skew typically contain
            // phrases like "Signature expired" or "request time
            // too skewed".  When detected, apply a ±5 min offset
            // so the next reconnection attempt uses a corrected
            // timestamp.  This is a single-shot heuristic; more
            // sophisticated NTP-style correction is out of scope.
            if mechanism_name == "AWS_MSK_IAM" {
                let lower = err_msg.to_ascii_lowercase();
                if lower.contains("signature expired")
                    || lower.contains("signature not yet current")
                    || lower.contains("request time too")
                    || lower.contains("clock")
                    || lower.contains("time skew")
                {
                    // Try to extract an ISO-8601 timestamp from the error to
                    // compute the exact offset; fall back to a ±5 min nudge.
                    let skew = Self::extract_clock_skew_secs(&err_msg);
                    let prev = msk_iam_clock_offset_secs.load(Ordering::Relaxed);
                    let nudge = if skew != 0 {
                        skew
                    } else if lower.contains("expired") || lower.contains("past") {
                        // Signature expired → local clock is behind broker.
                        300
                    } else {
                        // Not yet current → local clock is ahead of broker.
                        -300
                    };
                    let adjusted =
                        Self::clamp_msk_iam_clock_offset_secs(prev.saturating_add(nudge));
                    msk_iam_clock_offset_secs.store(adjusted, Ordering::Relaxed);
                    warn!(
                        "MSK IAM auth failed with possible clock skew ({}); \
                         adjusted clock offset to {}s for next attempt",
                        err_msg, adjusted,
                    );
                }
            }
            return Err(KrafkaError::auth(format!(
                "SASL authentication failed: {:?} - {}",
                auth_response.error_code, err_msg
            )));
        }

        let mut session_lifetime_ms = auth_response.session_lifetime_ms;

        // Step 3: Challenge-response loop (for SCRAM-SHA-*)
        // Capped at MAX_SASL_ROUNDS to guard against malicious brokers.
        const MAX_SASL_ROUNDS: usize = 10;

        if !authenticator.is_complete() {
            let mut challenge = auth_response.auth_bytes;
            let mut rounds = 0;

            loop {
                match authenticator.process_challenge(&challenge).await? {
                    ChallengeResponse::Done => break,
                    ChallengeResponse::AckThenFail { ack, error } => {
                        // Send the protocol-required ack (e.g., OAuthBearer \x01)
                        // then surface the auth error without reading a response —
                        // the server may close the connection immediately.
                        let _ = Self::send_sasl_authenticate(stream, &ack, client_id).await;
                        return Err(error);
                    }
                    ChallengeResponse::Continue(response_bytes) => {
                        rounds += 1;
                        if rounds > MAX_SASL_ROUNDS {
                            return Err(KrafkaError::auth(format!(
                                "SASL challenge-response exceeded {MAX_SASL_ROUNDS} rounds"
                            )));
                        }

                        Self::send_sasl_authenticate(stream, &response_bytes, client_id).await?;

                        let resp = Self::read_sasl_authenticate_response(stream, deadline).await?;
                        if !resp.error_code.is_ok() {
                            return Err(KrafkaError::auth(format!(
                                "SASL authentication step failed: {:?} - {}",
                                resp.error_code,
                                resp.error_message.unwrap_or_default()
                            )));
                        }

                        // The last successful response carries the session lifetime.
                        session_lifetime_ms = resp.session_lifetime_ms;
                        challenge = resp.auth_bytes;

                        if authenticator.is_complete() {
                            break;
                        }
                    }
                }
            }
        }

        info!("SASL authentication completed ({mechanism_name}) for {address}");

        if session_lifetime_ms > 0 {
            debug!("Broker reported session lifetime of {session_lifetime_ms}ms for {address}");
        }

        Ok(session_lifetime_ms)
    }

    /// Send a SaslAuthenticate v1 request on a raw stream.
    ///
    /// Uses API version 1 so the broker returns `session_lifetime_ms`
    /// in the response (KIP-368).
    async fn send_sasl_authenticate<S>(
        stream: &mut S,
        auth_bytes: &[u8],
        client_id: &str,
    ) -> Result<()>
    where
        S: AsyncWrite + Unpin,
    {
        let request = SaslAuthenticateRequest::new(auth_bytes.to_vec());
        let mut encoder = Encoder::with_capacity(64 + auth_bytes.len());
        let pos = encoder.start_message();
        let header = RequestHeader::new(ApiKey::SaslAuthenticate, 1, 1).with_client_id(client_id);
        header.encode(encoder.buffer_mut())?;
        request.encode_v1(encoder.buffer_mut())?;
        encoder.finish_message(pos)?;

        stream
            .write_all(&encoder.take())
            .await
            .map_err(KrafkaError::network)?;
        stream.flush().await.map_err(KrafkaError::network)?;
        Ok(())
    }

    /// Read a SaslAuthenticate v1 response from a raw stream.
    ///
    /// Decodes using v1 to obtain the `session_lifetime_ms` field (KIP-368).
    async fn read_sasl_authenticate_response<S>(
        stream: &mut S,
        deadline: tokio::time::Instant,
    ) -> Result<SaslAuthenticateResponse>
    where
        S: AsyncRead + Unpin,
    {
        let mut buf = Self::read_handshake_frame(stream, deadline, "SaslAuthenticate").await?;
        let _header = ResponseHeader::decode(&mut buf, ApiKey::SaslAuthenticate, 1)?;
        SaslAuthenticateResponse::decode_v1(&mut buf)
    }

    /// Read one pre-authentication frame, bounded in both size and time.
    ///
    /// Combines the [`MAX_SASL_FRAME_BYTES`] size cap of
    /// [`Self::read_framed_response`] with an absolute deadline, so a peer
    /// that completes TCP (and TLS) and then dribbles or stalls cannot hold
    /// the connection attempt open.
    async fn read_handshake_frame<S>(
        stream: &mut S,
        deadline: tokio::time::Instant,
        what: &str,
    ) -> Result<Bytes>
    where
        S: AsyncRead + Unpin,
    {
        tokio::time::timeout_at(
            deadline,
            Self::read_framed_response(stream, MAX_SASL_FRAME_BYTES),
        )
        .await
        .map_err(|_| {
            KrafkaError::timeout(format!(
                "timed out reading the {what} response during SASL handshake"
            ))
        })?
    }

    /// Read a length-prefixed Kafka response from a raw, **pre-authentication**
    /// stream.
    ///
    /// Only used by the SASL handshake path, where the peer has not proved
    /// anything yet. Two properties matter here that do not matter for the
    /// post-auth decoder:
    ///
    /// - `max_len` is the small [`MAX_SASL_FRAME_BYTES`] cap, not
    ///   `max_response_size`. A hostile bootstrap endpoint must not be able to
    ///   make the client reserve 100 MiB per connection before authenticating.
    /// - The body is accumulated in [`HANDSHAKE_READ_CHUNK`] steps rather than
    ///   pre-sized from the declared length, so a peer that declares a large
    ///   frame and then dribbles bytes only ever holds the memory it has
    ///   actually sent.
    ///
    /// The caller is responsible for bounding this in time; see
    /// [`Self::perform_sasl_handshake`], which wraps every call in a
    /// `timeout_at` against the handshake deadline.
    async fn read_framed_response<S>(stream: &mut S, max_len: usize) -> Result<Bytes>
    where
        S: AsyncRead + Unpin,
    {
        // Read 4-byte length prefix
        let mut len_buf = [0u8; 4];
        stream
            .read_exact(&mut len_buf)
            .await
            .map_err(KrafkaError::network)?;
        let len_i32 = i32::from_be_bytes(len_buf);

        if len_i32 <= 0 || (len_i32 as usize) > max_len {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                format!(
                    "Invalid pre-authentication response length: {len_i32} (max: {max_len}); \
                     refusing to allocate on an unauthenticated peer's say-so"
                ),
            ));
        }

        let len = len_i32 as usize;

        // Grow the buffer as bytes actually arrive instead of trusting `len`
        // enough to allocate it up front.
        let mut body = Vec::with_capacity(len.min(HANDSHAKE_READ_CHUNK));
        let mut chunk = [0u8; HANDSHAKE_READ_CHUNK];
        while body.len() < len {
            let want = (len - body.len()).min(HANDSHAKE_READ_CHUNK);
            let n = stream
                .read(&mut chunk[..want])
                .await
                .map_err(KrafkaError::network)?;
            if n == 0 {
                return Err(KrafkaError::network(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    format!(
                        "peer closed during SASL handshake after {} of {len} bytes",
                        body.len()
                    ),
                )));
            }
            body.extend_from_slice(&chunk[..n]);
        }

        Ok(Bytes::from(body))
    }

    /// Try to extract a clock skew (in seconds) from an AWS SigV4 error message.
    ///
    /// AWS error messages for clock skew embed an ISO-8601 basic-format timestamp
    /// (e.g. `20250413T120000Z`) that represents the server's view of "now".
    /// If such a timestamp is found, returns `server_unix - local_unix` in seconds.
    /// Returns `0` if no parseable timestamp is present.
    ///
    /// When the error contains multiple timestamps (e.g., "Signature not yet
    /// current: `<request_ts>` is not yet valid, not before `<validity_start>`"),
    /// the **last** parseable timestamp is used. AWS validity-window messages
    /// place the closest approximation to server-current-time last, so this
    /// yields a more accurate offset than returning the first match.
    fn extract_clock_skew_secs(error_msg: &str) -> i64 {
        // AWS SigV4 basic-format: YYYYMMDDTHHMMSSZ (16 bytes, ASCII-only).
        const AWS_TS_LEN: usize = 16;

        let bytes = error_msg.as_bytes();
        if bytes.len() < AWS_TS_LEN {
            return 0;
        }
        // Scan every byte-aligned window of AWS_TS_LEN bytes. The grammar is
        // ASCII-only, so indexing into `error_msg` at these offsets is safe
        // (no UTF-8 split risk — a successful parse guarantees ASCII content).
        //
        // We collect the LAST valid timestamp: AWS "not yet current" errors
        // embed both the request timestamp and the validity-window start; the
        // latter (last) is the better approximation of server-current-time.
        let mut last_server_unix: Option<i64> = None;
        for i in 0..=bytes.len() - AWS_TS_LEN {
            // Cheap pre-filter: byte 8 must be 'T', byte 15 must be 'Z'.
            if bytes[i + 8] != b'T' || bytes[i + 15] != b'Z' {
                continue;
            }
            if let Some(unix_secs) = Self::parse_aws_ts_unix(&bytes[i..i + AWS_TS_LEN]) {
                last_server_unix = Some(unix_secs);
            }
        }
        if let Some(server_unix) = last_server_unix {
            let local_unix = SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0);
            return server_unix - local_unix;
        }
        0
    }

    fn clamp_msk_iam_clock_offset_secs(offset: i64) -> i64 {
        offset.clamp(-MAX_SIGV4_CLOCK_SKEW_SECS, MAX_SIGV4_CLOCK_SKEW_SECS)
    }

    /// Parse an AWS SigV4 compact timestamp `YYYYMMDDTHHMMSSZ` (exactly 16 ASCII
    /// bytes, pre-validated for `T` at byte 8 and `Z` at byte 15) into Unix
    /// seconds since 1970-01-01T00:00:00Z.
    ///
    /// Returns `None` if any field is out of range, non-ASCII-decimal, or the
    /// calendar date is invalid (e.g. Feb 29 in a non-leap year).
    fn parse_aws_ts_unix(s: &[u8]) -> Option<i64> {
        debug_assert_eq!(s.len(), 16);
        debug_assert_eq!(s[8], b'T');
        debug_assert_eq!(s[15], b'Z');

        /// Parse two ASCII decimal digits into a `u32`. Returns `None` if any
        /// byte is not in `b'0'..=b'9'`.
        fn d2(hi: u8, lo: u8) -> Option<u32> {
            let h = hi.wrapping_sub(b'0');
            let l = lo.wrapping_sub(b'0');
            if h > 9 || l > 9 {
                return None;
            }
            Some(h as u32 * 10 + l as u32)
        }

        let year = {
            let [a, b, c, d] = [s[0], s[1], s[2], s[3]].map(|x| x.wrapping_sub(b'0'));
            if a > 9 || b > 9 || c > 9 || d > 9 {
                return None;
            }
            a as i64 * 1000 + b as i64 * 100 + c as i64 * 10 + d as i64
        };
        let month = d2(s[4], s[5])? as i64;
        let day = d2(s[6], s[7])? as i64;
        let hour = d2(s[9], s[10])? as i64;
        let min = d2(s[11], s[12])? as i64;
        let sec = d2(s[13], s[14])? as i64;

        if !(1..=12).contains(&month) {
            return None;
        }
        if hour > 23 || min > 59 || sec > 59 {
            return None;
        }

        // Validate day against the actual number of days in the month,
        // accounting for Gregorian leap years.
        let is_leap = (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
        const DAYS: [i64; 13] = [0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        let max_day = if month == 2 && is_leap {
            29
        } else {
            DAYS[month as usize]
        };
        if !(1..=max_day).contains(&day) {
            return None;
        }

        // Convert Gregorian calendar date to days since Unix epoch (1970-01-01)
        // using the proleptic Julian Day Number algorithm.
        //   JDN = day + (153m+2)/5 + 365y + y/4 - y/100 + y/400 - 32045
        // where  a = (14-month)/12,  y = year+4800-a,  m = month+12a-3
        let a = (14 - month) / 12;
        let y = year + 4800 - a;
        let m = month + 12 * a - 3;
        let jdn = day + (153 * m + 2) / 5 + 365 * y + y / 4 - y / 100 + y / 400 - 32045;
        // Unix epoch = JDN 2440588 (1970-01-01).
        let days_since_epoch = jdn - 2_440_588;

        Some(days_since_epoch * 86_400 + hour * 3_600 + min * 60 + sec)
    }

    /// Spawn a connection event-loop task.
    ///
    /// Wraps `run_connection_loop` with panic catching and close/error
    /// recording, then stores `false` in `alive` when the loop exits for
    /// any reason (clean, error, or panic).
    fn spawn_connection_task<R, W>(
        reader: R,
        writer: W,
        params: ConnectionLoopParams,
        alive: Arc<std::sync::atomic::AtomicBool>,
    ) -> tokio::task::JoinHandle<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        // Clone metrics for the close/error path — the original moves into
        // run_connection_loop via `params`.
        let close_metrics = params.metrics.clone();
        tokio::spawn(async move {
            let result =
                std::panic::AssertUnwindSafe(Self::run_connection_loop(reader, writer, params))
                    .catch_unwind()
                    .await;
            match result {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    close_metrics.record_error();
                    error!("Connection error: {e}");
                }
                Err(_panic_payload) => {
                    close_metrics.record_error();
                    error!("Connection event loop panicked; all in-flight requests failed");
                }
            }
            close_metrics.record_close();
            alive.store(false, std::sync::atomic::Ordering::Release);
        })
    }

    /// Run the connection event loop with priority handling.
    ///
    /// This is generic over the stream type, supporting both plain TCP and TLS.
    /// High-priority requests are always checked first using try_recv,
    /// ensuring heartbeats are never starved by backpressure on data requests.
    async fn run_connection_loop<R, W>(
        mut reader: R,
        mut writer: W,
        params: ConnectionLoopParams,
    ) -> Result<()>
    where
        R: AsyncRead + Unpin + Send + 'static,
        W: AsyncWrite + Unpin + Send + 'static,
    {
        let ConnectionLoopParams {
            address: broker_address,
            mut high_priority_rx,
            mut normal_priority_rx,
            request_timeout,
            stats,
            metrics,
            max_response_size,
            max_in_flight_requests,
            max_high_priority_bypasses: max_high_priority_bypasses_per_round,
        } = params;
        // All pending request state is owned exclusively by this task.
        // No Arc<Mutex> needed — all access is single-threaded on this event loop.
        let mut pending: AHashMap<CorrelationId, PendingRequest> = AHashMap::new();

        // Per-request timeout via timer-wheel (tokio_util::time::DelayQueue).
        // Cost: O(log n) per insertion/expiration vs O(n × connections) for the
        // old 1-second polling task.  Each entry fires exactly once at
        // `enqueue_time + request_timeout`.
        let mut delay_queue: DelayQueue<CorrelationId> = DelayQueue::new();
        // Maps correlation_id → queue key for O(1) cancellation on response receipt.
        let mut delay_keys: AHashMap<CorrelationId, delay_queue::Key> = AHashMap::new();

        // Correlation IDs whose client-side timeout fired while the request was
        // still outstanding. Kafka brokers do not cancel work when a client
        // times out -- they answer late. Without this record a late response
        // looks like a never-issued correlation ID, i.e. protocol desync, and
        // would tear down a healthy connection along with every other in-flight
        // request on it. Bounded by `max_in_flight_requests` (FIFO eviction),
        // so it cannot grow without limit.
        let mut timed_out: std::collections::VecDeque<CorrelationId> =
            std::collections::VecDeque::new();

        // Reader task sends decoded response frames to this loop via a bounded
        // channel.  The capacity matches max_in_flight_requests: the broker
        // can only send responses for outstanding requests, so this cap is
        // an exact fit.  It also provides back-pressure — if the main loop
        // is momentarily stalled (e.g., on a write), the reader suspends
        // instead of buffering unboundedly.
        let (frame_tx, mut frame_rx) =
            mpsc::channel::<Result<Bytes>>(max_in_flight_requests.max(1));

        let reader_handle = tokio::spawn(async move {
            let mut decoder = Decoder::with_max_size(max_response_size);
            let mut buf = vec![0u8; 65536];
            loop {
                match reader.read(&mut buf).await {
                    Ok(0) => {
                        debug!("Connection closed by peer");
                        break;
                    }
                    Ok(n) => {
                        decoder.extend(&buf[..n]);
                        loop {
                            match decoder.decode() {
                                Ok(Some(frame)) => {
                                    // Exit silently when the main loop has already gone away.
                                    if frame_tx.send(Ok(frame)).await.is_err() {
                                        return Ok::<_, KrafkaError>(());
                                    }
                                }
                                Ok(None) => break,
                                Err(e) => {
                                    let _ = frame_tx.send(Err(e)).await;
                                    return Ok(());
                                }
                            }
                        }
                    }
                    Err(e) => {
                        let _ = frame_tx.send(Err(KrafkaError::network(e))).await;
                        return Ok(());
                    }
                }
            }
            Ok(())
        });

        let mut terminal_error: Option<KrafkaError> = None;
        let mut consecutive_high_priority_commands = 0usize;
        let mut deferred_high_priority_cmd: Option<ConnectionCommand> = None;

        // Main event loop — lock-free on the hot path.
        loop {
            if consecutive_high_priority_commands >= max_high_priority_bypasses_per_round {
                if deferred_high_priority_cmd.is_none() {
                    match high_priority_rx.try_recv() {
                        Ok(ConnectionCommand::Close) => {
                            consecutive_high_priority_commands = 0;
                            match Self::process_loop_command(
                                &mut writer,
                                &mut pending,
                                &mut delay_queue,
                                &mut delay_keys,
                                ConnectionCommand::Close,
                                max_in_flight_requests,
                                request_timeout,
                            )
                            .await
                            {
                                Ok(true) => break,
                                Ok(false) => {}
                                Err(err) => {
                                    terminal_error = Some(err);
                                    break;
                                }
                            }
                            continue;
                        }
                        Ok(cmd) => {
                            deferred_high_priority_cmd = Some(cmd);
                        }
                        Err(mpsc::error::TryRecvError::Empty)
                        | Err(mpsc::error::TryRecvError::Disconnected) => {}
                    }
                }

                match normal_priority_rx.try_recv() {
                    Ok(cmd) => {
                        stats
                            .high_priority_bypass_yields
                            .fetch_add(1, Ordering::Relaxed);
                        metrics.record_high_priority_bypass_yield();
                        consecutive_high_priority_commands = 0;
                        match Self::process_loop_command(
                            &mut writer,
                            &mut pending,
                            &mut delay_queue,
                            &mut delay_keys,
                            cmd,
                            max_in_flight_requests,
                            request_timeout,
                        )
                        .await
                        {
                            Ok(true) => break,
                            Ok(false) => {}
                            Err(err) => {
                                terminal_error = Some(err);
                                break;
                            }
                        }
                        continue;
                    }
                    Err(mpsc::error::TryRecvError::Empty)
                    | Err(mpsc::error::TryRecvError::Disconnected) => {
                        consecutive_high_priority_commands = 0;
                    }
                }
            }

            if let Some(cmd) = deferred_high_priority_cmd.take() {
                consecutive_high_priority_commands += 1;
                match Self::process_loop_command(
                    &mut writer,
                    &mut pending,
                    &mut delay_queue,
                    &mut delay_keys,
                    cmd,
                    max_in_flight_requests,
                    request_timeout,
                )
                .await
                {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(err) => {
                        terminal_error = Some(err);
                        break;
                    }
                }
                continue;
            }

            // Fast path: drain the high-priority channel without yielding to the
            // scheduler.  Heartbeats are the most latency-sensitive request type.
            if let Ok(cmd) = high_priority_rx.try_recv() {
                stats.high_priority_bypasses.fetch_add(1, Ordering::Relaxed);
                metrics.record_high_priority_bypass();
                consecutive_high_priority_commands += 1;
                match Self::process_loop_command(
                    &mut writer,
                    &mut pending,
                    &mut delay_queue,
                    &mut delay_keys,
                    cmd,
                    max_in_flight_requests,
                    request_timeout,
                )
                .await
                {
                    Ok(true) => break,
                    Ok(false) => {}
                    Err(err) => {
                        terminal_error = Some(err);
                        break;
                    }
                }
                continue;
            }

            tokio::select! {
                biased;

                // Response frames from the reader task — dispatch immediately so
                // callers receive results as soon as the bytes arrive.
                frame_result = frame_rx.recv() => {
                    consecutive_high_priority_commands = 0;
                    match frame_result {
                        Some(Ok(frame)) => {
                            if let Err(e) = Self::dispatch_response(
                                &mut pending,
                                &mut delay_queue,
                                &mut delay_keys,
                                &mut timed_out,
                                frame,
                                &broker_address,
                            ) {
                                // Protocol desynchronisation — close the connection.
                                terminal_error = Some(e);
                                break;
                            }
                        }
                        Some(Err(e)) => {
                            terminal_error = Some(e);
                            break;
                        }
                        None => {
                            // Reader task exited (peer closed the connection).
                            break;
                        }
                    }
                }

                // Timer-wheel: fires exactly when a per-request deadline expires.
                // O(log n) cost vs O(n × connections) for the old 1-second sweep.
                Some(expired) = std::future::poll_fn(|cx| {
                    use futures_core::Stream;
                    std::pin::Pin::new(&mut delay_queue).poll_next(cx)
                }) => {
                    consecutive_high_priority_commands = 0;
                    let id = expired.into_inner();
                    if let Some(req) = pending.remove(&id) {
                        delay_keys.remove(&id);
                        // Remember the ID so a late broker response is dropped
                        // quietly instead of being read as a protocol desync.
                        if timed_out.len() >= max_in_flight_requests {
                            timed_out.pop_front();
                        }
                        timed_out.push_back(id);
                        warn!(
                            correlation_id = id,
                            "Request timed out after {:?}", request_timeout
                        );
                        let _ = req.response_tx.send(Err(KrafkaError::timeout(format!(
                            "request {id} timed out after {request_timeout:?}"
                        ))));
                    }
                }

                // High-priority commands (heartbeats, metadata, coordinator lookups).
                cmd = high_priority_rx.recv() => {
                    match cmd {
                        Some(cmd) => {
                            consecutive_high_priority_commands += 1;
                            match Self::process_loop_command(
                                &mut writer,
                                &mut pending,
                                &mut delay_queue,
                                &mut delay_keys,
                                cmd,
                                max_in_flight_requests,
                                request_timeout,
                            )
                            .await {
                                Ok(true) => break,
                                Ok(false) => {}
                                Err(err) => {
                                    terminal_error = Some(err);
                                    break;
                                }
                            }
                        }
                        None => break,
                    }
                }

                // Normal-priority commands (produce, fetch, and all others).
                cmd = normal_priority_rx.recv() => {
                    match cmd {
                        Some(cmd) => {
                            consecutive_high_priority_commands = 0;
                            match Self::process_loop_command(
                                &mut writer,
                                &mut pending,
                                &mut delay_queue,
                                &mut delay_keys,
                                cmd,
                                max_in_flight_requests,
                                request_timeout,
                            )
                            .await {
                                Ok(true) => break,
                                Ok(false) => {}
                                Err(err) => {
                                    terminal_error = Some(err);
                                    break;
                                }
                            }
                        }
                        None => break,
                    }
                }
            }
        }

        // Shut the write half down *before* dropping it.
        //
        // For a plain TCP stream, dropping the half closes the socket and the
        // peer sees FIN either way. For a **split TLS stream** it does not:
        // the rustls session needs an explicit `shutdown()` to emit the
        // `close_notify` alert and half-close the TCP connection underneath.
        // Without it the broker logs an unclean truncation on every normal
        // disconnect — noisy, and indistinguishable from a real truncation
        // attack.
        //
        // Best-effort: the peer may already be gone, and a failure here has no
        // bearing on the teardown that follows.
        let _ = writer.shutdown().await;
        drop(writer);
        reader_handle.abort();

        // Drain all in-flight requests and notify callers that the connection
        // is gone.
        //
        // A clean EOF here is the *expected* outcome of a broker rolling
        // restart or an idle reap, so callers get a retriable network error
        // and reconnect — not `InvalidState`, which they would surface as a
        // permanent failure.
        let pending_error = terminal_error
            .clone()
            .unwrap_or_else(connection_closed_error);
        for (_, req) in pending.drain() {
            let _ = req.response_tx.send(Err(pending_error.clone()));
        }

        if let Some(err) = terminal_error {
            return Err(err);
        }

        Ok(())
    }

    async fn process_loop_command<W: AsyncWrite + Unpin>(
        writer: &mut W,
        pending: &mut AHashMap<CorrelationId, PendingRequest>,
        delay_queue: &mut DelayQueue<CorrelationId>,
        delay_keys: &mut AHashMap<CorrelationId, delay_queue::Key>,
        cmd: ConnectionCommand,
        max_in_flight_requests: usize,
        request_timeout: Duration,
    ) -> Result<bool> {
        Self::handle_command_direct(
            writer,
            pending,
            delay_queue,
            delay_keys,
            cmd,
            max_in_flight_requests,
            request_timeout,
        )
        .await
    }

    /// Handle a single connection command.
    ///
    /// Returns `true` if the connection should close.
    ///
    /// # Lock-free hot path
    ///
    /// The pending map is owned by the single event-loop task — all insertions
    /// and removals are O(1) HashMap operations with no synchronization overhead.
    async fn handle_command_direct<W: AsyncWrite + Unpin>(
        writer: &mut W,
        pending: &mut AHashMap<CorrelationId, PendingRequest>,
        delay_queue: &mut DelayQueue<CorrelationId>,
        delay_keys: &mut AHashMap<CorrelationId, delay_queue::Key>,
        cmd: ConnectionCommand,
        max_in_flight_requests: usize,
        request_timeout: Duration,
    ) -> Result<bool> {
        match cmd {
            ConnectionCommand::Request {
                data,
                correlation_id,
                api_key,
                api_version,
                response_tx,
                timeout: budget,
                permit,
            } => {
                if pending.contains_key(&correlation_id) {
                    let error = KrafkaError::invalid_state(format!(
                        "correlation ID collision on broker connection: correlation_id={correlation_id}, pending_requests={}; closing connection",
                        pending.len()
                    ));
                    error!(
                        correlation_id,
                        pending_requests = pending.len(),
                        "Detected correlation ID collision; closing connection"
                    );
                    let _ = response_tx.send(Err(error.clone()));
                    return Err(error);
                }

                // Defence in depth. The submitter already holds an in-flight
                // permit (see `BrokerConnection::in_flight`), so the semaphore
                // makes this branch unreachable; reaching it would mean permit
                // accounting is broken.
                //
                // It reports a *retriable* network error rather than
                // `InvalidState`: an in-flight cap is transient backpressure,
                // and reporting it as permanent turns a momentary queue depth
                // into a hard client failure.
                if pending.len() >= max_in_flight_requests {
                    warn!(
                        pending = pending.len(),
                        max = max_in_flight_requests,
                        "Rejecting request: max in-flight requests reached \
                         (in-flight permit accounting inconsistent)"
                    );
                    let _ = response_tx.send(Err(KrafkaError::network(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        format!("max in-flight requests ({max_in_flight_requests}) reached; retry"),
                    ))));
                    return Ok(false);
                }

                // Snapshot the deadline before touching the wire so that the
                // end-to-end budget (write + network round-trip) is exactly
                // one budget, not up to 2× the budget.
                let deadline = tokio::time::Instant::now() + budget;

                // Write to the wire.  Register in pending only after a successful
                // write so we never create a leaked entry for an undelivered request.
                //
                // Uses the same absolute deadline as the DelayQueue entry below so
                // write + response wait together consume exactly one request_timeout
                // budget.  A stalled TCP write cannot freeze the event loop (and
                // therefore block all in-flight timeout processing) for longer than
                // the remaining budget.
                let write_result = tokio::time::timeout_at(deadline, async {
                    writer.write_all(&data).await?;
                    writer.flush().await
                })
                .await;
                match write_result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        error!("Write error: {}", e);
                        let msg = e.to_string();
                        let _ = response_tx.send(Err(KrafkaError::network(e)));
                        // `write_all` may have written a partial frame before
                        // failing. Keeping the connection alive would append
                        // the next request's bytes to that truncated frame,
                        // desynchronizing the broker's parser for the life of
                        // the socket. Tear the connection down instead — the
                        // same reasoning as the write-timeout arm below.
                        return Err(KrafkaError::network(std::io::Error::new(
                            std::io::ErrorKind::BrokenPipe,
                            format!("write failed, stream indeterminate: {msg}"),
                        )));
                    }
                    Err(_) => {
                        let msg = format!("write timed out after {budget:?}");
                        error!("{msg}");
                        let _ = response_tx.send(Err(KrafkaError::timeout(msg.clone())));
                        // The stream is in an indeterminate state — close the connection.
                        return Err(KrafkaError::timeout(msg));
                    }
                }

                // Register pending entry and arm the per-request timeout at the
                // same absolute deadline used for the write, so the whole
                // request (write + response wait) is bounded by one budget.
                let key = delay_queue.insert_at(correlation_id, deadline);
                delay_keys.insert(correlation_id, key);
                pending.insert(
                    correlation_id,
                    PendingRequest {
                        response_tx,
                        api_key,
                        api_version,
                        _permit: permit,
                    },
                );
                Ok(false)
            }
            ConnectionCommand::Close => {
                debug!("Closing connection");
                Ok(true)
            }
            ConnectionCommand::FireAndForget { data } => {
                // No response is expected, so a relative timeout is sufficient —
                // there is no second phase to share a deadline with.
                let write_result = tokio::time::timeout(request_timeout, async {
                    writer.write_all(&data).await?;
                    writer.flush().await
                })
                .await;
                match write_result {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => {
                        // As above: a partial write leaves the stream
                        // indeterminate, so the connection must not be reused.
                        error!("Fire-and-forget write error: {}", e);
                        return Err(KrafkaError::network(e));
                    }
                    Err(_) => {
                        error!(
                            "Fire-and-forget write timed out after {:?}",
                            request_timeout
                        );
                        return Err(KrafkaError::timeout(format!(
                            "fire-and-forget write timed out after {request_timeout:?}"
                        )));
                    }
                }
                Ok(false)
            }
        }
    }

    /// Dispatch an incoming response frame to the waiting caller.
    ///
    /// Looks up the correlation ID in the pending map, cancels the associated
    /// timeout, decodes the response header, and delivers the body.
    ///
    /// Returns `Err` only on protocol-level desynchronisation (unknown
    /// correlation ID or undecodable response header) — both indicate a corrupt
    /// stream and require the connection to be closed.
    fn dispatch_response(
        pending: &mut AHashMap<CorrelationId, PendingRequest>,
        delay_queue: &mut DelayQueue<CorrelationId>,
        delay_keys: &mut AHashMap<CorrelationId, delay_queue::Key>,
        timed_out: &mut std::collections::VecDeque<CorrelationId>,
        response: Bytes,
        broker_address: &str,
    ) -> Result<()> {
        if response.len() < 4 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::TruncatedFrame,
                format!(
                    "response too short from broker {broker_address}: frame_bytes={}",
                    response.len()
                ),
            ));
        }

        let correlation_id =
            i32::from_be_bytes([response[0], response[1], response[2], response[3]]);

        let pending_before_remove = pending.len();
        if let Some(req) = pending.remove(&correlation_id) {
            // Cancel the timeout — the response arrived before the deadline.
            if let Some(key) = delay_keys.remove(&correlation_id) {
                delay_queue.remove(&key);
            }

            trace!("Received response for correlation_id={}", correlation_id);

            let mut response_buf = response.slice(..);
            match ResponseHeader::decode(&mut response_buf, req.api_key, req.api_version) {
                Ok(_header) => {
                    let header_size = response.len() - response_buf.len();
                    let body = response.slice(header_size..);
                    let _ = req.response_tx.send(Ok(body));
                }
                Err(e) => {
                    // Header decode failure means the stream is desynchronised
                    // — notify the caller and tear down the connection.
                    let response_header_version =
                        ResponseHeader::header_version(req.api_key, req.api_version);
                    let context = format!(
                        "response header decode failed: broker={broker_address}, api_key={:?}, api_version={}, response_header_version={}, correlation_id={correlation_id}, frame_bytes={}, pending_before_remove={pending_before_remove}, error={e}",
                        req.api_key,
                        req.api_version,
                        response_header_version,
                        response.len(),
                    );
                    warn!(
                        broker = broker_address,
                        api_key = ?req.api_key,
                        api_version = req.api_version,
                        response_header_version,
                        correlation_id,
                        frame_bytes = response.len(),
                        pending_before_remove,
                        error = %e,
                        "Failed to decode response header; closing connection"
                    );
                    let _ = req.response_tx.send(Err(KrafkaError::protocol_kind(
                        ProtocolErrorKind::Malformed,
                        context.clone(),
                    )));
                    return Err(KrafkaError::protocol_kind(
                        ProtocolErrorKind::Malformed,
                        format!("{context}; stream desynchronized"),
                    ));
                }
            }
        } else if let Some(pos) = timed_out.iter().position(|&id| id == correlation_id) {
            // Late response for a request whose client-side timeout already
            // fired. The caller has been given a Timeout error; the broker is
            // behaving correctly by answering. Drop the frame and keep the
            // connection -- tearing it down here would fail every other
            // in-flight request and, under load, cause a reconnect storm.
            timed_out.remove(pos);
            debug!(
                correlation_id,
                broker = broker_address,
                frame_bytes = response.len(),
                "Discarding late response for a timed-out request"
            );
        } else {
            // Genuinely never-issued correlation ID: protocol desync.
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::Malformed,
                format!(
                    "Received response for unknown correlation_id={correlation_id} from broker {broker_address}; frame_bytes={}, pending_requests={pending_before_remove}; closing connection",
                    response.len()
                ),
            ));
        }

        Ok(())
    }

    /// Acquire an in-flight slot, bounded by `deadline`.
    ///
    /// Blocking here *is* the backpressure mechanism: when
    /// `max_in_flight_requests` requests are already outstanding, the
    /// submitter waits for one to resolve rather than queueing work the event
    /// loop would refuse.
    async fn acquire_in_flight(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<tokio::sync::OwnedSemaphorePermit> {
        tokio::time::timeout_at(deadline, self.in_flight.clone().acquire_owned())
            .await
            .map_err(|_| {
                KrafkaError::timeout(format!(
                    "waiting for an in-flight slot on {} (max_in_flight_requests={})",
                    self.address, self.config.max_in_flight_requests
                ))
            })?
            // The semaphore is never closed while the connection exists, so
            // this can only fail during teardown.
            .map_err(|_| connection_closed_error())
    }

    /// Fetch API versions from the broker.
    ///
    /// # Why this negotiates instead of pinning v0
    ///
    /// ApiVersions is the one API whose version cannot be negotiated from a
    /// previous ApiVersions response — it is the bootstrap. Pinning it to v0
    /// is safe but costs two things that matter:
    ///
    /// - **KIP-511.** `ClientSoftwareName` / `ClientSoftwareVersion` only exist
    ///   from v3. Sent at v0 they are silently dropped, so every broker-side
    ///   `client.software.name` metric reports krafka as unknown.
    /// - **KIP-584.** `SupportedFeatures` / `FinalizedFeatures` ride in v3+
    ///   tagged fields. At v0 the client never learns the cluster's finalized
    ///   feature levels and has to spend a separate round trip to get them.
    ///
    /// So we do what the Java client does: send the highest version we can
    /// encode, and on `UNSUPPORTED_VERSION` fall back. A broker that rejects
    /// the version answers with a **v0-format body** (this is mandated by the
    /// protocol precisely so the fallback is decodable) whose `api_keys` names
    /// the range it does support, giving us the right version in one retry
    /// rather than a blind walk down.
    async fn fetch_api_versions(&self) -> Result<()> {
        /// Bounds the fallback walk: highest → broker-advertised max → v0.
        /// Purely defensive — a broker that keeps answering UNSUPPORTED_VERSION
        /// for a version it just advertised is broken, and must not spin here.
        const MAX_ATTEMPTS: usize = 3;

        let request =
            ApiVersionsRequest::new().with_client_software("krafka", env!("CARGO_PKG_VERSION"));

        let mut attempt_version = crate::protocol::versions::API_VERSIONS_MAX;
        let mut attempts = 0usize;

        let response = loop {
            attempts += 1;
            let body = self.send_api_versions(&request, attempt_version).await?;

            // The error code is the leading INT16 in every ApiVersions response
            // version, so it can be read before committing to a body format.
            let error_code = body
                .get(..2)
                .map(|b| i16::from_be_bytes([b[0], b[1]]))
                .ok_or_else(|| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::TruncatedFrame,
                        "ApiVersions response too short to contain an error code",
                    )
                })?;

            let unsupported = error_code == ErrorCode::UnsupportedVersion.to_i16();
            let mut buf = body;
            let decoded = if unsupported {
                // Mandated v0 body layout, regardless of the version we sent.
                ApiVersionsResponse::decode_v0(&mut buf)?
            } else {
                Self::decode_api_versions_at(attempt_version, &mut buf)?
            };

            if !unsupported {
                break decoded;
            }

            // Prefer the ceiling the broker just told us about; otherwise drop
            // to v0, which every broker since 0.10 supports.
            let next = decoded
                .get_api_version(ApiKey::ApiVersions)
                .map(|range| range.max_version)
                .filter(|&max| max >= 0 && max < attempt_version)
                .unwrap_or(0);

            if next >= attempt_version || attempts >= MAX_ATTEMPTS {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    format!(
                        "broker {} rejected ApiVersions v{attempt_version} with \
                         UNSUPPORTED_VERSION and offered no lower usable version",
                        self.address
                    ),
                ));
            }

            debug!(
                broker = %self.address,
                rejected = attempt_version,
                retrying_with = next,
                "Broker rejected the ApiVersions version; falling back"
            );
            attempt_version = next;
        };

        if response.error_code != 0 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::Other,
                format!("ApiVersions error: {}", response.error_code),
            ));
        }

        // Cache the cluster's feature levels (KIP-584) so callers do not need a
        // second ApiVersions round trip to read them. Empty when the broker
        // only spoke v0/v1/v2.
        {
            let mut features = self.broker_features.lock();
            features.negotiated_version = attempt_version;
            features.supported.clone_from(&response.supported_features);
            features.finalized_epoch = response.finalized_features_epoch;
            features.finalized.clone_from(&response.finalized_features);
        }

        let mut versions = self.api_versions.lock();
        for range in response.api_keys {
            versions.insert(range.api_key, range);
        }

        debug!(
            broker = %self.address,
            api_versions_version = attempt_version,
            apis = versions.len(),
            "Negotiated broker API versions"
        );
        Ok(())
    }

    /// Decode an ApiVersions response body at the version it was requested at.
    fn decode_api_versions_at(version: i16, buf: &mut Bytes) -> Result<ApiVersionsResponse> {
        match version {
            0 => ApiVersionsResponse::decode_v0(buf),
            1..=2 => ApiVersionsResponse::decode_v1(buf),
            // v3, v4 and v5 share one response wire format; v4 only relaxes a
            // field constraint and v5's additions are request-side (KIP-1242).
            _ => ApiVersionsResponse::decode_v3(buf),
        }
    }

    /// Send one ApiVersions request at `version` and return the raw body.
    async fn send_api_versions(&self, request: &ApiVersionsRequest, version: i16) -> Result<Bytes> {
        let correlation_id = self.correlation_id_gen.next();
        let mut encoder = Encoder::with_capacity(128);

        let pos = encoder.start_message();
        let header = RequestHeader::new(ApiKey::ApiVersions, version, correlation_id)
            .with_client_id(&self.config.client_id);
        // `encode` picks header v1 or v2 from `ApiKey::flexible_version()`,
        // which is 3 for ApiVersions — exactly the boundary the body encoders
        // below switch on, so the two can never disagree.
        header.encode(encoder.buffer_mut())?;
        match version {
            0..=2 => request.encode_v0(encoder.buffer_mut())?,
            3..=4 => request.encode_v3(encoder.buffer_mut())?,
            _ => request.encode_v5(encoder.buffer_mut())?,
        }
        encoder.finish_message(pos)?;

        // One deadline covers slot acquisition, the channel send, and the
        // response wait, so the whole call is bounded by request_timeout.
        let deadline = tokio::time::Instant::now() + self.config.request_timeout;
        let permit = self.acquire_in_flight(deadline).await?;

        let (response_tx, response_rx) = oneshot::channel();
        tokio::time::timeout_at(
            deadline,
            self.high_priority_tx.send(ConnectionCommand::Request {
                data: encoder.take(),
                correlation_id,
                api_key: ApiKey::ApiVersions,
                api_version: version,
                response_tx,
                timeout: self.config.request_timeout,
                permit,
            }),
        )
        .await
        .map_err(|_| KrafkaError::timeout("enqueuing api versions request"))?
        .map_err(|_| connection_closed_error())?;

        self.stats
            .high_priority_requests
            .fetch_add(1, Ordering::Relaxed);
        self.config
            .connection_metrics
            .record_high_priority_request();

        tokio::time::timeout_at(deadline, response_rx)
            .await
            .map_err(|_| KrafkaError::timeout("api versions request"))?
            .map_err(|_| connection_closed_error())?
    }

    /// Broker and cluster feature levels learned during the ApiVersions
    /// handshake (KIP-584).
    ///
    /// Empty when the broker only supports ApiVersions v0–v2, which predate
    /// the tagged fields that carry them.
    #[must_use]
    pub fn broker_features(&self) -> BrokerFeatures {
        self.broker_features.lock().clone()
    }

    /// Choose the appropriate channel based on request priority.
    #[inline]
    fn channel_for_priority(&self, priority: RequestPriority) -> &mpsc::Sender<ConnectionCommand> {
        match priority {
            RequestPriority::High => &self.high_priority_tx,
            RequestPriority::Normal => &self.normal_priority_tx,
        }
    }

    /// Record a broker-reported throttle time (KIP-219).
    ///
    /// When the broker returns `throttle_time_ms > 0` in a response, the
    /// client should voluntarily delay subsequent normal-priority requests
    /// by that amount. High-priority requests (heartbeats, metadata) are
    /// never delayed.
    pub fn notify_throttle(&self, throttle_time_ms: i32) {
        if throttle_time_ms > 0 {
            let new_deadline = Instant::now() + Duration::from_millis(throttle_time_ms as u64);
            let mut deadline = self.throttle_until.lock();
            if new_deadline > *deadline {
                debug!(
                    throttle_ms = throttle_time_ms,
                    broker = %self.address,
                    "Broker throttle applied (KIP-219)"
                );
                *deadline = new_deadline;
            }
        }
    }

    /// Return the remaining throttle delay for this connection, if any.
    ///
    /// Returns `Some(duration)` if the broker's throttle window has not yet
    /// elapsed, `None` otherwise.
    ///
    /// Prefer [`await_throttle`](Self::await_throttle) if you intend to wait:
    /// sleeping on this value directly consumes the window without recording
    /// it, which is how the producer path came to report zero throttle delay
    /// while being throttled.
    #[inline]
    pub fn throttle_remaining(&self) -> Option<Duration> {
        self.throttle_until
            .lock()
            .checked_duration_since(Instant::now())
    }

    /// Sleep out any remaining broker-imposed throttle, **recording** the
    /// delay (KIP-219).
    ///
    /// The recording is the reason this exists rather than callers sleeping on
    /// [`throttle_remaining`](Self::throttle_remaining) themselves. The
    /// producer did exactly that, one layer above the request path — and
    /// because the sleep consumed the throttle window, the connection layer's
    /// own check then found nothing left to wait for and recorded nothing. The
    /// single metric that answers "is the broker throttling us" read zero on
    /// the path most likely to be throttled, which is worse than having no
    /// metric at all: a zero that means "not measured" looks like evidence.
    ///
    /// Returns the delay that was applied, if any.
    pub async fn await_throttle(&self) -> Option<Duration> {
        let remaining = self.throttle_remaining()?;
        debug!(
            delay_ms = remaining.as_millis() as u64,
            broker = %self.address,
            "Delaying request due to broker throttle (KIP-219)"
        );
        self.config
            .connection_metrics
            .record_throttle_delay(remaining);
        tokio::time::sleep(remaining).await;
        Some(remaining)
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
        request_body: impl FnOnce(&mut BytesMut) -> Result<()>,
    ) -> Result<Bytes> {
        let priority = RequestPriority::for_api_key(api_key);
        self.send_request_with_priority(api_key, api_version, priority, request_body)
            .await
    }

    /// Send a request that the broker is expected to hold open for longer than
    /// `request_timeout`.
    ///
    /// A few Kafka APIs are long-polls on the broker side: the coordinator
    /// parks a `JoinGroup` for as long as the group's rebalance takes, which is
    /// bounded by the group's rebalance timeout (`max.poll.interval.ms`), not
    /// by the client's `request.timeout.ms`. Sending those with the ordinary
    /// budget aborts them client-side mid-rebalance, which looks like a
    /// coordinator failure and restarts the join from scratch — while the
    /// broker still holds the original request.
    ///
    /// `timeout` is a floor of `request_timeout`, never a way to shorten it.
    pub async fn send_request_with_timeout(
        &self,
        api_key: ApiKey,
        api_version: i16,
        timeout: Duration,
        request_body: impl FnOnce(&mut BytesMut) -> Result<()>,
    ) -> Result<Bytes> {
        let priority = RequestPriority::for_api_key(api_key);
        let budget = timeout.max(self.config.request_timeout);
        self.send_inner(api_key, api_version, priority, budget, request_body)
            .await
    }

    /// Send a request with explicit priority.
    ///
    /// Use this when you need to override the automatic priority selection.
    /// Normal-priority requests are delayed when the broker has signalled
    /// quota throttling (KIP-219).
    ///
    /// # Timeout
    ///
    /// A single deadline is computed on entry and covers **every** phase:
    /// acquiring an in-flight slot, enqueueing on the priority channel, and
    /// waiting for the response. Total wall-clock time is therefore bounded by
    /// `request_timeout`. Previously only the response wait was bounded, so a
    /// full channel could push the total past 2× `request_timeout`.
    ///
    /// # Backpressure
    ///
    /// When `max_in_flight_requests` requests are already outstanding this
    /// call *waits* for a slot rather than failing. The semaphore, not the
    /// channel depth, decides how much work is admitted.
    pub async fn send_request_with_priority(
        &self,
        api_key: ApiKey,
        api_version: i16,
        priority: RequestPriority,
        request_body: impl FnOnce(&mut BytesMut) -> Result<()>,
    ) -> Result<Bytes> {
        self.send_inner(
            api_key,
            api_version,
            priority,
            self.config.request_timeout,
            request_body,
        )
        .await
    }

    /// Shared submission path for [`send_request_with_priority`] and
    /// [`send_request_with_timeout`]; `budget` bounds the whole call.
    ///
    /// [`send_request_with_priority`]: Self::send_request_with_priority
    /// [`send_request_with_timeout`]: Self::send_request_with_timeout
    async fn send_inner(
        &self,
        api_key: ApiKey,
        api_version: i16,
        priority: RequestPriority,
        budget: Duration,
        request_body: impl FnOnce(&mut BytesMut) -> Result<()>,
    ) -> Result<Bytes> {
        // M1: refresh the idle timestamp on every submission so the pool's
        // idle-evictor does not close an actively used connection.
        self.mark_used();

        // One deadline for the whole call — see the doc comment above.
        let deadline = tokio::time::Instant::now() + budget;

        // KIP-219: honour broker throttle for normal-priority requests.
        if priority == RequestPriority::Normal {
            self.await_throttle().await;
        }

        let correlation_id = self.correlation_id_gen.next();
        let mut encoder = Encoder::with_capacity(256);

        // Build request
        let pos = encoder.start_message();
        let header = RequestHeader::new(api_key, api_version, correlation_id)
            .with_client_id(&self.config.client_id);
        header.encode(encoder.buffer_mut())?;
        request_body(encoder.buffer_mut())?;
        encoder.finish_message(pos)?;

        // Acquire an in-flight slot before enqueueing. This is the real
        // backpressure point: without it the 256-slot normal channel admits
        // ~25× more work than the default in-flight cap of 10 accepts.
        let permit = self.acquire_in_flight(deadline).await?;

        // Send request to appropriate channel
        let (response_tx, response_rx) = oneshot::channel();
        let channel = self.channel_for_priority(priority);
        tokio::time::timeout_at(
            deadline,
            channel.send(ConnectionCommand::Request {
                data: encoder.take(),
                correlation_id,
                api_key,
                api_version,
                response_tx,
                timeout: budget,
                permit,
            }),
        )
        .await
        .map_err(|_| {
            KrafkaError::timeout(format!(
                "enqueuing {api_key:?} request to {} (channel full)",
                self.address
            ))
        })?
        .map_err(|_| connection_closed_error())?;

        // Update stats
        match priority {
            RequestPriority::High => {
                self.stats
                    .high_priority_requests
                    .fetch_add(1, Ordering::Relaxed);
                self.config
                    .connection_metrics
                    .record_high_priority_request();
            }
            RequestPriority::Normal => {
                self.stats
                    .normal_priority_requests
                    .fetch_add(1, Ordering::Relaxed);
                self.config
                    .connection_metrics
                    .record_normal_priority_request();
            }
        }

        // Wait for the response on the *same* deadline the send used, so send
        // and receive share one request_timeout budget rather than one each.
        let response = tokio::time::timeout_at(deadline, response_rx)
            .await
            .map_err(|_| KrafkaError::timeout("request"))?
            .map_err(|_| connection_closed_error())??;

        // KIP-219: honour the throttle the broker asked for, for every API that
        // reports it as the response's leading field. Doing it here means an
        // admin client backs off under quota pressure without each of the ~50
        // admin call sites having to forward a field it does not otherwise use.
        if let Some(throttle_time_ms) = leading_throttle_time_ms(api_key, api_version, &response) {
            self.notify_throttle(throttle_time_ms);
        }

        Ok(response)
    }

    /// Send a request without waiting for a response (fire-and-forget).
    ///
    /// Used for `acks=0` produce requests where the Kafka broker does not
    /// send a response. The request is written to the wire but no response
    /// channel is registered in the pending map, avoiding resource leaks and
    /// preserving the normal correlation-ID space for requests that expect
    /// responses.
    ///
    /// # Quota feedback is one-directional here (KIP-219)
    ///
    /// No response means no `throttle_time_ms`, so a producer running purely
    /// at `acks=0` never *learns* a throttle from its own traffic. A throttle
    /// learned from any other API on this connection is still honoured — the
    /// accumulator checks [`throttle_remaining`](Self::throttle_remaining)
    /// before dispatching a batch — but a client that sends nothing else keeps
    /// writing at full rate until the broker mutes the channel itself.
    ///
    /// This is inherent to `acks=0` rather than a gap in the implementation:
    /// the field simply does not exist on a request the broker never answers.
    /// It is one more reason `acks=0` trades away more than durability.
    ///
    /// No in-flight permit is acquired either, for the same reason: the
    /// permit's job is to bound the pending map, and this path never inserts
    /// into it.
    pub async fn send_fire_and_forget(
        &self,
        api_key: ApiKey,
        api_version: i16,
        request_body: impl FnOnce(&mut BytesMut) -> Result<()>,
    ) -> Result<()> {
        // M1: refresh the idle timestamp on every submission.
        self.mark_used();

        let mut encoder = Encoder::with_capacity(256);

        // Build request
        let pos = encoder.start_message();
        let header = RequestHeader::new(api_key, api_version, NO_RESPONSE_CORRELATION_ID)
            .with_client_id(&self.config.client_id);
        header.encode(encoder.buffer_mut())?;
        request_body(encoder.buffer_mut())?;
        encoder.finish_message(pos)?;

        // Send as fire-and-forget — no pending entry is created
        let channel = self.channel_for_priority(RequestPriority::Normal);
        tokio::time::timeout(
            self.config.request_timeout,
            channel.send(ConnectionCommand::FireAndForget {
                data: encoder.take(),
            }),
        )
        .await
        .map_err(|_| {
            KrafkaError::timeout(format!(
                "enqueuing fire-and-forget {api_key:?} to {} (channel full)",
                self.address
            ))
        })?
        .map_err(|_| connection_closed_error())?;

        self.stats
            .normal_priority_requests
            .fetch_add(1, Ordering::Relaxed);
        self.config
            .connection_metrics
            .record_normal_priority_request();

        Ok(())
    }

    /// Get the supported API version for a specific API.
    ///
    /// Synchronous: the negotiated table is a `parking_lot::Mutex` populated
    /// during the handshake, so this is a map lookup. It was `async` with no
    /// `await` inside, which forced every caller — and therefore every caller's
    /// caller — to be async for a lock read.
    pub fn get_api_version(&self, api_key: ApiKey) -> Option<ApiVersionRange> {
        let versions = self.api_versions.lock();
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
    /// let version = conn.negotiate_api_version(ApiKey::Fetch, 12, 4);
    /// ```
    pub fn negotiate_api_version(
        &self,
        api_key: ApiKey,
        client_max: i16,
        client_min: i16,
    ) -> Option<i16> {
        let versions = self.api_versions.lock();
        versions
            .get(&api_key)
            .and_then(|range| range.negotiate(client_max, client_min))
    }

    /// Negotiate the best API version with minimum version defaulting to 0.
    pub fn negotiate_api_version_max(&self, api_key: ApiKey, client_max: i16) -> Option<i16> {
        self.negotiate_api_version(api_key, client_max, 0)
    }

    /// Compute the session expiry instant from a broker-reported lifetime.
    ///
    /// Returns `None` when `session_lifetime_ms` is zero or negative (no expiry).
    /// Otherwise picks a random reauthentication point between 85 % and 95 %
    /// of the session lifetime.  The jitter avoids a thundering-herd where
    /// many connections to the same broker all need replacement at the same
    /// instant.  This matches the approach taken by the Java Kafka client.
    fn compute_session_expiry(session_lifetime_ms: i64) -> Option<Instant> {
        if session_lifetime_ms <= 0 {
            return None;
        }
        // Randomised window: 85 % base + up to 10 % jitter = 85-95 % of lifetime.
        // Mirrors Java client's pctWindowFactor (0.85) + jitter (0.10).
        const MIN_REAUTH_MS: u64 = 100;
        let base_factor: f64 = 0.85;
        let jitter_range: f64 = 0.10;
        let jitter: f64 = rand::random::<f64>() * jitter_range;
        let factor = base_factor + jitter;
        let computed_reauth_ms = (session_lifetime_ms as f64 * factor) as u64;
        let reauth_ms = computed_reauth_ms.max(MIN_REAUTH_MS);
        if computed_reauth_ms < MIN_REAUTH_MS {
            warn!(
                session_lifetime_ms,
                computed_reauth_ms,
                reauth_ms,
                "broker reported unusually small SASL session lifetime; clamping reauthentication delay"
            );
        }
        Some(Instant::now() + Duration::from_millis(reauth_ms))
    }

    /// Compute session expiry, falling back to an OAuthBearer token lifetime
    /// when the broker does not report `session_lifetime_ms` (KIP-368).
    ///
    /// The token's `lifetime_ms` is an epoch-millisecond timestamp. We convert
    /// it to a remaining duration before passing it through the standard
    /// jittered-window logic.
    fn effective_session_expiry(session_lifetime_ms: i64, auth: &AuthConfig) -> Option<Instant> {
        if session_lifetime_ms > 0 {
            return Self::compute_session_expiry(session_lifetime_ms);
        }

        // Fall back to OAuthBearer token lifetime if available.
        if let Some(token) = auth.oauthbearer_token.as_ref()
            && let Some(expiry_epoch_ms) = token.lifetime_ms()
        {
            let now_epoch_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as i64;
            let remaining_ms = expiry_epoch_ms.saturating_sub(now_epoch_ms);
            if remaining_ms > 0 {
                return Self::compute_session_expiry(remaining_ms);
            }
        }

        None
    }

    /// Whether the SASL session is approaching expiry and the connection
    /// should be replaced (KIP-368).
    ///
    /// Returns `false` when no session lifetime was reported by the broker.
    #[inline]
    pub fn needs_reauthentication(&self) -> bool {
        self.session_expiry
            .is_some_and(|expiry| Instant::now() >= expiry)
    }

    /// The instant at which the client should start reauthentication, if any.
    #[inline]
    pub fn session_expiry(&self) -> Option<Instant> {
        self.session_expiry
    }

    /// Check if the connection is alive.
    #[inline]
    pub fn is_alive(&self) -> bool {
        self.alive.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Whether the connection is alive and its SASL session has not expired.
    ///
    /// This is the primary check used by the connection pool to decide if an
    /// existing connection can be reused or must be replaced.
    #[inline]
    pub fn is_usable(&self) -> bool {
        self.is_alive() && !self.needs_reauthentication()
    }

    /// Record that the connection was just used for a request.
    ///
    /// Called from the submission paths (`send_request_with_priority`,
    /// `send_fire_and_forget`). Stores monotonic nanos since `created_at`
    /// into `last_used_nanos`; reads happen from [`idle_duration`].
    #[inline]
    fn mark_used(&self) {
        let elapsed = self.created_at.elapsed().as_nanos();
        // Saturate on the (astronomical) overflow boundary rather than panic.
        let nanos = u64::try_from(elapsed).unwrap_or(u64::MAX);
        self.last_used_nanos.store(nanos, Ordering::Relaxed);
    }

    /// Duration since the last submitted request on this connection.
    ///
    /// A freshly connected socket that has sent no requests reports its
    /// full age (since `created_at`) as idle — identical to Java's
    /// `connections.max.idle.ms` accounting.
    #[inline]
    pub fn idle_duration(&self) -> Duration {
        let last = self.last_used_nanos.load(Ordering::Relaxed);
        let now = self.created_at.elapsed();
        now.saturating_sub(Duration::from_nanos(last))
    }

    /// Test-only: construct a minimal, non-I/O-capable `BrokerConnection`
    /// with `created_at` backdated by `idle_for`, so `idle_duration()`
    /// reports at least `idle_for`. Used by pool eviction tests that need
    /// to exercise `evict_idle` without standing up a real broker.
    ///
    /// The returned connection:
    /// - has dropped receivers for both priority channels (sending on it
    ///   will fail; this is intentional — the stub is only consumed by
    ///   the eviction scan, which never sends);
    /// - is marked `alive = true` so `is_alive()` reports consistently;
    /// - has `last_used_nanos = 0` so idle time equals full age.
    #[cfg(test)]
    #[allow(clippy::expect_used)]
    pub(crate) fn test_stub_idle_for(address: &str, idle_for: Duration) -> Self {
        let (high_priority_tx, _) = mpsc::channel(1);
        let (normal_priority_tx, _) = mpsc::channel(1);
        Self {
            address: address.to_string(),
            config: ConnectionConfig::default(),
            correlation_id_gen: Arc::new(CorrelationIdGenerator::new()),
            high_priority_tx,
            normal_priority_tx,
            api_versions: Arc::new(parking_lot::Mutex::new(AHashMap::new())),
            broker_features: Arc::new(parking_lot::Mutex::new(BrokerFeatures::default())),
            alive: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            session_expiry: None,
            stats: Arc::new(ConnectionStats::default()),
            throttle_until: Arc::new(parking_lot::Mutex::new(Instant::now())),
            created_at: Instant::now()
                .checked_sub(idle_for)
                // `unwrap`: test idle_for values are always small (≤ 10s) and
                // any system uptime on which tests run exceeds that easily;
                // failing loudly here is better than silently yielding a fresh
                // timestamp that makes eviction tests vacuously pass.
                .expect("idle_for exceeds system uptime; cannot backdate Instant"),
            last_used_nanos: AtomicU64::new(0),
            in_flight: Arc::new(tokio::sync::Semaphore::new(1)),
        }
    }

    /// Test-only: refresh `last_used_nanos` to "now" without going through
    /// a send path. Used to verify the evictor's race re-check rescues a
    /// connection that was refreshed between the snapshot and the write.
    #[cfg(test)]
    pub(crate) fn test_mark_fresh(&self) {
        self.mark_used();
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
    ///
    /// Signals the event loop over the high-priority channel. The send is
    /// bounded by [`CLOSE_SEND_TIMEOUT`]: a connection whose event loop is
    /// wedged in `write_all` cannot accept the command, and an unbounded
    /// `send().await` there would block the caller — and, through
    /// [`ConnectionPool::close_all`](super::ConnectionPool::close_all), the
    /// whole client shutdown — indefinitely.
    ///
    /// Giving up is safe: the socket is still torn down when the last `Arc` to
    /// this connection drops.
    pub async fn close(&self) {
        // Fast path: a free channel slot closes without ever yielding.
        match self.high_priority_tx.try_send(ConnectionCommand::Close) {
            Ok(()) => {}
            // Receiver already gone — the loop has exited; nothing to signal.
            Err(mpsc::error::TrySendError::Closed(_)) => {}
            Err(mpsc::error::TrySendError::Full(cmd)) => {
                if timeout(CLOSE_SEND_TIMEOUT, self.high_priority_tx.send(cmd))
                    .await
                    .is_err()
                {
                    warn!(
                        broker = %self.address,
                        "close() timed out after {CLOSE_SEND_TIMEOUT:?} waiting for the \
                         high-priority channel; the event loop is stalled. Socket teardown \
                         falls back to Drop."
                    );
                }
            }
        }
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// Build an `ApiVersionsResponse` body (after the correlation ID) that
    /// advertises no APIs, shaped for `api_version`.
    ///
    /// The hand-rolled mock brokers below have to match the version the client
    /// actually sent: since the handshake negotiates rather than pinning v0, a
    /// v0-shaped body answering a v4 request decodes as a truncated frame.
    fn mock_api_versions_body(api_version: i16) -> BytesMut {
        use bytes::BufMut as _;
        let mut body = BytesMut::new();
        body.put_i16(0); // error_code = NONE
        if api_version >= 3 {
            // Flexible: compact array (raw 1 == zero elements), throttle, tags.
            body.put_u8(1);
            body.put_i32(0); // throttle_time_ms
            body.put_u8(0); // empty tagged fields
        } else {
            body.put_i32(0); // 0 api keys
            if api_version >= 1 {
                body.put_i32(0); // throttle_time_ms
            }
        }
        body
    }

    /// A standalone in-flight permit for tests that construct a
    /// `ConnectionCommand::Request` or `PendingRequest` directly, without
    /// going through `send_request_with_priority`.
    fn test_permit() -> tokio::sync::OwnedSemaphorePermit {
        Arc::new(tokio::sync::Semaphore::new(1))
            .try_acquire_owned()
            .expect("fresh semaphore always has a permit")
    }

    #[test]
    fn test_connection_config_builder() {
        let config = ConnectionConfig::builder()
            .connect_timeout(Duration::from_secs(5))
            .request_timeout(Duration::from_secs(15))
            .client_id("test-client")
            .nodelay(false)
            .build()
            .unwrap();

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
        assert_eq!(config.high_priority_channel_capacity, 64);
        assert_eq!(config.normal_priority_channel_capacity, 256);
        assert!(config.auth.is_none());
    }

    #[test]
    fn test_connection_config_uses_shared_metrics_handle() {
        let metrics = Arc::new(ConnectionMetrics::default());
        let config = ConnectionConfig::builder()
            .connection_metrics(metrics.clone())
            .build()
            .unwrap();

        config.connection_metrics.record_high_priority_request();
        assert_eq!(metrics.high_priority_requests.get(), 1);
        assert!(Arc::ptr_eq(&metrics, &config.connection_metrics()));
    }

    #[test]
    fn test_connection_config_with_auth() {
        use crate::auth::AuthConfig;
        let config = ConnectionConfig::builder()
            .client_id("test")
            .auth(AuthConfig::sasl_plain("user", "pass").unwrap())
            .build()
            .unwrap();

        assert_eq!(config.client_id, "test");
        let auth = config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(!auth.requires_tls());
    }

    #[test]
    fn test_connection_config_builder_with_priority() {
        let config = ConnectionConfig::builder()
            .high_priority_channel_capacity(32)
            .normal_priority_channel_capacity(512)
            .build()
            .unwrap();

        assert_eq!(config.high_priority_channel_capacity, 32);
        assert_eq!(config.normal_priority_channel_capacity, 512);
    }

    #[test]
    fn test_connection_config_min_values() {
        // Ensure minimums are enforced
        let config = ConnectionConfig::builder()
            .high_priority_channel_capacity(0) // Should become 16
            .normal_priority_channel_capacity(0) // Should become 64
            .build()
            .unwrap();

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
        // Group coordination and offset commit are time-sensitive (rebalance / session timeout).
        assert_eq!(
            RequestPriority::for_api_key(ApiKey::ConsumerGroupHeartbeat),
            RequestPriority::High,
            "ConsumerGroupHeartbeat must be High to prevent KIP-848 rebalances"
        );
        assert_eq!(
            RequestPriority::for_api_key(ApiKey::ShareGroupHeartbeat),
            RequestPriority::High,
            "ShareGroupHeartbeat must be High to prevent KIP-932 share group evictions"
        );
        assert_eq!(
            RequestPriority::for_api_key(ApiKey::JoinGroup),
            RequestPriority::High
        );
        assert_eq!(
            RequestPriority::for_api_key(ApiKey::SyncGroup),
            RequestPriority::High
        );
        assert_eq!(
            RequestPriority::for_api_key(ApiKey::LeaveGroup),
            RequestPriority::High
        );
        assert_eq!(
            RequestPriority::for_api_key(ApiKey::OffsetCommit),
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
        assert_eq!(stats.bypass_yield_count(), 0);
    }

    #[test]
    fn test_connection_stats_increment() {
        let stats = ConnectionStats::default();
        stats.high_priority_requests.fetch_add(5, Ordering::Relaxed);
        stats
            .normal_priority_requests
            .fetch_add(10, Ordering::Relaxed);
        stats.high_priority_bypasses.fetch_add(2, Ordering::Relaxed);
        stats
            .high_priority_bypass_yields
            .fetch_add(1, Ordering::Relaxed);

        assert_eq!(stats.high_priority_count(), 5);
        assert_eq!(stats.normal_priority_count(), 10);
        assert_eq!(stats.bypass_count(), 2);
        assert_eq!(stats.bypass_yield_count(), 1);
    }

    /// A broker that answers *after* the client-side timeout must not be
    /// treated as protocol desync. Kafka does not cancel work on client
    /// timeout, so late responses are normal under load; tearing down the
    /// connection would fail every other in-flight request on it.
    #[test]
    fn test_dispatch_response_late_response_does_not_desync_connection() {
        let correlation_id: CorrelationId = 42;
        let mut pending = AHashMap::new(); // request already removed by the timeout
        let mut delay_queue = DelayQueue::new();
        let mut delay_keys = AHashMap::new();
        let mut timed_out = std::collections::VecDeque::from([correlation_id]);

        let result = BrokerConnection::dispatch_response(
            &mut pending,
            &mut delay_queue,
            &mut delay_keys,
            &mut timed_out,
            Bytes::copy_from_slice(&correlation_id.to_be_bytes()),
            "broker-1:9092",
        );
        assert!(
            result.is_ok(),
            "late response must be discarded, not reported as desync: {result:?}"
        );
        // The ID is consumed, so a second frame with the same ID is a genuine
        // desync rather than another free pass.
        assert!(timed_out.is_empty());

        let second = BrokerConnection::dispatch_response(
            &mut pending,
            &mut delay_queue,
            &mut delay_keys,
            &mut timed_out,
            Bytes::copy_from_slice(&correlation_id.to_be_bytes()),
            "broker-1:9092",
        );
        assert!(
            second.is_err(),
            "a truly unknown correlation id is still fatal"
        );
    }

    #[test]
    fn test_dispatch_response_header_decode_error_includes_context() {
        let correlation_id = 7;
        let (response_tx, mut response_rx) = oneshot::channel();
        let mut pending = AHashMap::new();
        pending.insert(
            correlation_id,
            PendingRequest {
                response_tx,
                api_key: ApiKey::Metadata,
                api_version: 9,
                _permit: test_permit(),
            },
        );
        let mut delay_queue = DelayQueue::new();
        let mut delay_keys = AHashMap::new();

        let mut timed_out = std::collections::VecDeque::new();
        let err = BrokerConnection::dispatch_response(
            &mut pending,
            &mut delay_queue,
            &mut delay_keys,
            &mut timed_out,
            Bytes::copy_from_slice(&correlation_id.to_be_bytes()),
            "broker-1:9092",
        )
        .unwrap_err();
        let caller_err = response_rx.try_recv().unwrap().unwrap_err();
        let err_text = caller_err.to_string();

        assert!(err.to_string().contains("stream desynchronized"));
        assert!(err_text.contains("broker=broker-1:9092"));
        assert!(err_text.contains("api_key=Metadata"));
        assert!(err_text.contains("api_version=9"));
        assert!(err_text.contains("response_header_version=1"));
        assert!(err_text.contains("correlation_id=7"));
        assert!(err_text.contains("frame_bytes=4"));
    }

    /// Mock Kafka broker that handles the SASL handshake protocol.
    ///
    /// Accepts a connection, reads SaslHandshakeRequest, SaslAuthenticateRequest,
    /// and ApiVersionsRequest, responding to each with valid responses.
    /// The `session_lifetime_ms` value is included in the SaslAuthenticate v1
    /// response (KIP-368). The broker stays open until the test signals
    /// shutdown so the connection remains usable after the initial handshake.
    /// Returns the captured auth bytes from SaslAuthenticate for verification.
    async fn run_mock_sasl_broker(
        listener: tokio::net::TcpListener,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> (String, Vec<u8>) {
        run_mock_sasl_broker_with_lifetime(listener, 0, shutdown_rx).await
    }

    /// Like [`run_mock_sasl_broker`] but lets the caller set the session
    /// lifetime reported in the SaslAuthenticateResponse (KIP-368).
    async fn run_mock_sasl_broker_with_lifetime(
        listener: tokio::net::TcpListener,
        session_lifetime_ms: i64,
        shutdown_rx: oneshot::Receiver<()>,
    ) -> (String, Vec<u8>) {
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

        // Send SaslAuthenticateResponse v1: correlation_id + error_code(0) + null message + empty bytes + session_lifetime_ms
        let mut resp = BytesMut::new();
        resp.put_i32(correlation_id);
        resp.put_i16(0); // error_code = NONE
        resp.put_i16(-1_i16); // error_message = null (KafkaString)
        resp.put_i32(0); // auth_bytes = empty (KafkaBytes, 0 length)
        resp.put_i64(session_lifetime_ms); // session_lifetime_ms (v1)
        write_frame(&mut stream, &resp).await;

        // 3. Read ApiVersionsRequest
        let req = read_frame(&mut stream).await;
        let api_version = i16::from_be_bytes(req[2..4].try_into().unwrap());
        let correlation_id = i32::from_be_bytes(req[4..8].try_into().unwrap());

        // Answer in the shape the client asked for; the handshake negotiates
        // the ApiVersions version rather than pinning v0.
        let mut resp = BytesMut::new();
        resp.put_i32(correlation_id);
        resp.put_slice(&mock_api_versions_body(api_version));
        write_frame(&mut stream, &resp).await;

        let _ = shutdown_rx.await;

        (mechanism, auth_bytes)
    }

    #[tokio::test]
    async fn test_sasl_plain_handshake_with_mock_broker() {
        // Start a mock broker
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let addr_str = addr.to_string();

        // Run mock broker in background
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let mock_handle = tokio::spawn(run_mock_sasl_broker(listener, shutdown_rx));

        // Connect with SASL/PLAIN auth
        let config = ConnectionConfig::builder()
            .client_id("test-client")
            .auth(crate::auth::AuthConfig::sasl_plain("testuser", "testpassword").unwrap())
            .build()
            .unwrap();

        let conn = BrokerConnection::connect(&addr_str, config).await;
        assert!(
            conn.is_ok(),
            "Connection with SASL/PLAIN should succeed: {:?}",
            conn.err()
        );

        let conn = conn.unwrap();
        assert!(conn.is_alive());

        conn.close().await;
        let _ = shutdown_tx.send(());

        // Verify the mock received the correct handshake
        let (mechanism, auth_bytes) = mock_handle.await.unwrap();
        assert_eq!(mechanism, "PLAIN");

        // SASL PLAIN format: \0username\0password
        assert_eq!(auth_bytes, b"\0testuser\0testpassword");
    }

    #[tokio::test]
    async fn test_sasl_oauthbearer_provider_handshake_with_mock_broker() {
        use crate::auth::OAuthBearerToken;

        // Start a mock broker
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let addr_str = addr.to_string();

        // Run mock broker in background
        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let mock_handle = tokio::spawn(run_mock_sasl_broker(listener, shutdown_rx));

        // Connect with OAUTHBEARER provider (not a static token)
        let config = ConnectionConfig::builder()
            .client_id("test-client")
            .auth(crate::auth::AuthConfig::sasl_oauthbearer_provider(
                || async { Ok(OAuthBearerToken::new("provider-jwt-token")) },
            ))
            .build()
            .unwrap();

        let conn = BrokerConnection::connect(&addr_str, config).await;
        assert!(
            conn.is_ok(),
            "Connection with OAUTHBEARER provider should succeed: {:?}",
            conn.err()
        );

        let conn = conn.unwrap();
        assert!(conn.is_alive());

        conn.close().await;
        let _ = shutdown_tx.send(());

        // Verify the mock received the correct OAUTHBEARER handshake
        let (mechanism, auth_bytes) = mock_handle.await.unwrap();
        assert_eq!(mechanism, "OAUTHBEARER");

        // GS2 format: n,,\x01auth=Bearer <token>\x01\x01
        let expected = OAuthBearerToken::new("provider-jwt-token").to_gs2_initial_response();
        assert_eq!(auth_bytes, expected);
    }

    #[tokio::test]
    async fn test_sasl_oauthbearer_provider_timeout() {
        // Provider that hangs forever
        let config = ConnectionConfig::builder()
            .client_id("test-client")
            .connect_timeout(Duration::from_millis(50))
            .request_timeout(Duration::from_millis(100))
            .auth(crate::auth::AuthConfig::sasl_oauthbearer_provider(
                || async {
                    // Simulate a hung OAuth server
                    tokio::time::sleep(Duration::from_secs(60)).await;
                    Ok(crate::auth::OAuthBearerToken::new("never"))
                },
            ))
            .build()
            .unwrap();

        // We need a listening socket so TCP connect succeeds before the handshake
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let addr_str = addr.to_string();

        // Accept in background so the connect() doesn't hang
        tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            // Keep the stream alive so the client side doesn't get a connection reset
            tokio::time::sleep(Duration::from_secs(5)).await;
        });

        let result = BrokerConnection::connect(&addr_str, config).await;
        assert!(
            result.is_err(),
            "Connection should fail when provider times out"
        );
        let err = match result {
            Err(e) => e.to_string(),
            Ok(_) => panic!("Expected error"),
        };
        assert!(
            err.contains("timed out") || err.contains("timeout"),
            "Error should mention timeout: {err}"
        );
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
            let api_version = i16::from_be_bytes(body[2..4].try_into().unwrap());
            let correlation_id = i32::from_be_bytes(body[4..8].try_into().unwrap());

            // Send ApiVersionsResponse
            let mut resp = BytesMut::new();
            resp.put_i32(correlation_id);
            resp.put_slice(&mock_api_versions_body(api_version));
            let len = resp.len() as i32;
            stream.write_all(&len.to_be_bytes()).await.unwrap();
            stream.write_all(&resp).await.unwrap();
            stream.flush().await.unwrap();

            api_key
        });

        // Connect without auth
        let config = ConnectionConfig::builder()
            .client_id("test-client")
            .build()
            .unwrap();

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
            .auth(crate::auth::AuthConfig::sasl_plain("user", "pass").unwrap())
            .build()
            .unwrap();

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

            // 2. SaslAuthenticate — reject with auth error (v1 format)
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
            resp.put_i64(0); // session_lifetime_ms (v1)
            let len = resp.len() as i32;
            stream.write_all(&len.to_be_bytes()).await.unwrap();
            stream.write_all(&resp).await.unwrap();
            stream.flush().await.unwrap();
        });

        let config = ConnectionConfig::builder()
            .client_id("test-client")
            .auth(crate::auth::AuthConfig::sasl_plain("user", "wrongpass").unwrap())
            .build()
            .unwrap();

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
            BrokerConnection::read_framed_response(&mut cursor, MAX_SASL_FRAME_BYTES).await;
        assert!(result.is_err(), "negative frame length should be rejected");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("response length: -1"),
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

    #[tokio::test]
    async fn test_connection_loop_enforces_configured_max_response_size() {
        let (client, mut server) = tokio::io::duplex(256);
        let (reader, writer) = tokio::io::split(client);
        let (_high_tx, high_rx) = mpsc::channel(4);
        let (normal_tx, normal_rx) = mpsc::channel(4);
        let stats = Arc::new(ConnectionStats::default());
        let metrics = Arc::new(ConnectionMetrics::default());

        let loop_task = tokio::spawn(BrokerConnection::run_connection_loop(
            reader,
            writer,
            ConnectionLoopParams {
                address: "test-broker".to_string(),
                high_priority_rx: high_rx,
                normal_priority_rx: normal_rx,
                request_timeout: Duration::from_secs(30),
                stats,
                metrics,
                max_response_size: 16,
                max_in_flight_requests: 256,
                max_high_priority_bypasses: 4,
            },
        ));

        let (response_tx, response_rx) = oneshot::channel();
        normal_tx
            .send(ConnectionCommand::Request {
                data: Bytes::from_static(b"ping"),
                correlation_id: 7,
                api_key: ApiKey::Metadata,
                api_version: 0,
                response_tx,
                timeout: Duration::from_secs(30),
                permit: test_permit(),
            })
            .await
            .unwrap();

        let mut request = [0u8; 4];
        server.read_exact(&mut request).await.unwrap();
        assert_eq!(&request, b"ping");

        server.write_all(&(32i32).to_be_bytes()).await.unwrap();
        server.write_all(&[0u8; 32]).await.unwrap();
        server.flush().await.unwrap();

        let err = response_rx.await.unwrap().unwrap_err();
        assert!(
            err.to_string()
                .contains("message size 32 exceeds maximum 16"),
            "pending request should receive the configured frame-limit error: {err}"
        );

        let loop_err = loop_task.await.unwrap().unwrap_err();
        assert!(
            loop_err
                .to_string()
                .contains("message size 32 exceeds maximum 16"),
            "connection loop should stop on oversized steady-state frames: {loop_err}"
        );
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

    #[tokio::test]
    async fn test_connection_loop_yields_to_normal_priority_after_bypass_budget() {
        use tokio::io::AsyncReadExt;

        let (client, mut server) = tokio::io::duplex(4096);
        let (reader, writer) = tokio::io::split(client);
        let (high_tx, high_rx) = mpsc::channel(16);
        let (normal_tx, normal_rx) = mpsc::channel(16);
        let stats = Arc::new(ConnectionStats::default());
        let metrics = Arc::new(ConnectionMetrics::default());

        for index in 0..8 {
            let (response_tx, _response_rx) = oneshot::channel();
            high_tx
                .try_send(ConnectionCommand::Request {
                    data: Bytes::copy_from_slice(format!("H{index:03}").as_bytes()),
                    correlation_id: index + 1,
                    api_key: ApiKey::Heartbeat,
                    api_version: 0,
                    response_tx,
                    timeout: Duration::from_secs(30),
                    permit: test_permit(),
                })
                .unwrap();
        }

        let (normal_response_tx, _normal_response_rx) = oneshot::channel();
        normal_tx
            .try_send(ConnectionCommand::Request {
                data: Bytes::from_static(b"N000"),
                correlation_id: 100,
                api_key: ApiKey::Produce,
                api_version: 0,
                response_tx: normal_response_tx,
                timeout: Duration::from_secs(30),
                permit: test_permit(),
            })
            .unwrap();

        let loop_task = tokio::spawn(BrokerConnection::run_connection_loop(
            reader,
            writer,
            ConnectionLoopParams {
                address: "test-broker".to_string(),
                high_priority_rx: high_rx,
                normal_priority_rx: normal_rx,
                request_timeout: Duration::from_secs(30),
                stats: stats.clone(),
                metrics: metrics.clone(),
                max_response_size: crate::protocol::MAX_MESSAGE_SIZE,
                max_in_flight_requests: 32,
                max_high_priority_bypasses: 4,
            },
        ));

        let mut writes = Vec::new();
        for _ in 0..5 {
            let mut frame = [0u8; 4];
            server.read_exact(&mut frame).await.unwrap();
            writes.push(String::from_utf8(frame.to_vec()).unwrap());
        }

        assert_eq!(writes[0], "H000");
        assert_eq!(writes[1], "H001");
        assert_eq!(writes[2], "H002");
        assert_eq!(writes[3], "H003");
        assert_eq!(writes[4], "N000");
        assert_eq!(stats.bypass_yield_count(), 1);
        assert_eq!(metrics.snapshot().high_priority_bypass_yields, 1);

        loop_task.abort();
    }

    #[tokio::test]
    async fn test_connection_loop_rejects_correlation_id_collision() {
        use tokio::io::AsyncReadExt;

        let (client, mut server) = tokio::io::duplex(4096);
        let (reader, writer) = tokio::io::split(client);
        let (_high_tx, high_rx) = mpsc::channel(4);
        let (normal_tx, normal_rx) = mpsc::channel(4);
        let stats = Arc::new(ConnectionStats::default());
        let metrics = Arc::new(ConnectionMetrics::default());

        let loop_task = tokio::spawn(BrokerConnection::run_connection_loop(
            reader,
            writer,
            ConnectionLoopParams {
                address: "test-broker".to_string(),
                high_priority_rx: high_rx,
                normal_priority_rx: normal_rx,
                request_timeout: Duration::from_secs(30),
                stats,
                metrics,
                max_response_size: crate::protocol::MAX_MESSAGE_SIZE,
                max_in_flight_requests: 32,
                max_high_priority_bypasses: 4,
            },
        ));

        let (first_response_tx, first_response_rx) = oneshot::channel();
        normal_tx
            .send(ConnectionCommand::Request {
                data: Bytes::from_static(b"req1"),
                correlation_id: 77,
                api_key: ApiKey::Metadata,
                api_version: 0,
                response_tx: first_response_tx,
                timeout: Duration::from_secs(30),
                permit: test_permit(),
            })
            .await
            .unwrap();

        let mut first_write = [0u8; 4];
        server.read_exact(&mut first_write).await.unwrap();
        assert_eq!(&first_write, b"req1");

        let (second_response_tx, second_response_rx) = oneshot::channel();
        normal_tx
            .send(ConnectionCommand::Request {
                data: Bytes::from_static(b"req2"),
                correlation_id: 77,
                api_key: ApiKey::Metadata,
                api_version: 0,
                response_tx: second_response_tx,
                timeout: Duration::from_secs(30),
                permit: test_permit(),
            })
            .await
            .unwrap();

        let second_err = second_response_rx.await.unwrap().unwrap_err();
        assert!(second_err.to_string().contains("correlation ID collision"));

        let first_err = first_response_rx.await.unwrap().unwrap_err();
        assert!(first_err.to_string().contains("correlation ID collision"));

        let loop_err = loop_task.await.unwrap().unwrap_err();
        assert!(loop_err.to_string().contains("correlation ID collision"));
    }

    #[test]
    fn test_connection_config_builder_max_response_size() {
        let config = ConnectionConfig::builder()
            .max_response_size(50 * 1024 * 1024)
            .build()
            .unwrap();
        assert_eq!(
            config.max_response_size,
            50 * 1024 * 1024,
            "max_response_size should be settable via builder"
        );
    }

    #[test]
    fn test_connection_config_builder_max_response_size_minimum() {
        // Setting a value below 1024 should be clamped to 1024
        let config = ConnectionConfig::builder()
            .max_response_size(100)
            .build()
            .unwrap();
        assert_eq!(
            config.max_response_size, 1024,
            "max_response_size should be clamped to minimum of 1024 bytes"
        );

        let config_zero = ConnectionConfig::builder()
            .max_response_size(0)
            .build()
            .unwrap();
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
            .build()
            .unwrap();

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
            .build()
            .unwrap();
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

    #[cfg(feature = "socks5")]
    #[test]
    fn test_proxy_config_new() {
        let proxy = ProxyConfig::new("proxy.example.com:1080");
        assert_eq!(proxy.address(), "proxy.example.com:1080");
        assert!(proxy.credentials().is_none());
    }

    #[cfg(feature = "socks5")]
    #[test]
    fn test_proxy_config_with_credentials() {
        let proxy = ProxyConfig::with_credentials("proxy.example.com:1080", "user", "s3cret");
        assert_eq!(proxy.address(), "proxy.example.com:1080");
        let creds = proxy.credentials().expect("should have credentials");
        assert_eq!(creds.username(), "user");
        assert_eq!(creds.password(), "s3cret");
    }

    #[cfg(feature = "socks5")]
    #[test]
    fn test_proxy_config_debug_redacts_credentials() {
        let proxy = ProxyConfig::with_credentials("proxy.example.com:1080", "admin", "hunter2");
        let debug_str = format!("{proxy:?}");
        assert!(
            debug_str.contains("proxy.example.com:1080"),
            "Debug should contain the address"
        );
        assert!(
            !debug_str.contains("hunter2"),
            "Debug must NOT contain the password"
        );
        assert!(
            debug_str.contains("[REDACTED]"),
            "Debug should show [REDACTED] for credentials"
        );
    }

    #[cfg(feature = "socks5")]
    #[test]
    fn test_proxy_credentials_debug_redacts() {
        let proxy = ProxyConfig::with_credentials("proxy.example.com:1080", "user", "password123");
        let creds = proxy.credentials().expect("should have credentials");
        let debug_str = format!("{creds:?}");
        assert!(
            !debug_str.contains("password123"),
            "Debug must NOT contain the password"
        );
        assert!(
            debug_str.contains("[REDACTED]"),
            "Debug should show [REDACTED]"
        );
    }

    #[cfg(feature = "socks5")]
    #[test]
    fn test_connection_config_builder_with_proxy() {
        let proxy = ProxyConfig::new("socks5.internal:1080");
        let config = ConnectionConfig::builder()
            .client_id("proxy-test")
            .proxy(proxy)
            .build()
            .unwrap();

        assert!(config.proxy.is_some());
        assert_eq!(
            config.proxy.as_ref().unwrap().address(),
            "socks5.internal:1080"
        );
    }

    #[cfg(feature = "socks5")]
    #[tokio::test]
    async fn test_connect_via_proxy_dns_failure_is_retriable() {
        let proxy = ProxyConfig::new("this-proxy-does-not-exist.invalid:1080");
        let config = ConnectionConfig::builder()
            .connect_timeout(Duration::from_secs(5))
            .proxy(proxy)
            .build()
            .unwrap();
        let result = BrokerConnection::connect("broker:9092", config).await;
        match result {
            Ok(_) => panic!("connect through non-existent proxy should fail"),
            Err(err) => {
                assert!(
                    err.is_retriable(),
                    "proxy DNS failure should be retriable (Network or Timeout), got: {err}"
                );
            }
        }
    }

    #[cfg(feature = "socks5")]
    #[tokio::test]
    async fn test_connect_via_proxy_stalled_handshake_times_out() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = ProxyConfig::new(listener.local_addr().unwrap().to_string());
        let config = ConnectionConfig::builder()
            .connect_timeout(Duration::from_millis(75))
            .proxy(proxy.clone())
            .build()
            .unwrap();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let mock_proxy = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            let _ = shutdown_rx.await;
        });

        let started_at = Instant::now();
        let err = BrokerConnection::connect_via_proxy("broker.internal:9092", &proxy, &config)
            .await
            .unwrap_err();

        assert!(matches!(err, KrafkaError::Timeout { .. }));
        assert!(
            err.to_string().contains("SOCKS5 proxy connection"),
            "timeout should identify the proxy connect path: {err}"
        );
        assert!(
            started_at.elapsed() < Duration::from_secs(1),
            "proxy handshake timeout should respect the configured deadline"
        );

        let _ = shutdown_tx.send(());
        mock_proxy.await.unwrap();
    }

    #[tokio::test]
    async fn test_send_fire_and_forget_uses_reserved_correlation_id() {
        let (high_priority_tx, _high_priority_rx) = mpsc::channel(1);
        let (normal_priority_tx, mut normal_priority_rx) = mpsc::channel(1);
        let conn = BrokerConnection {
            address: "test-broker".to_string(),
            config: ConnectionConfig::default(),
            correlation_id_gen: Arc::new(CorrelationIdGenerator::new()),
            high_priority_tx,
            normal_priority_tx,
            api_versions: Arc::new(parking_lot::Mutex::new(AHashMap::new())),
            broker_features: Arc::new(parking_lot::Mutex::new(BrokerFeatures::default())),
            alive: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            session_expiry: None,
            stats: Arc::new(ConnectionStats::default()),
            throttle_until: Arc::new(parking_lot::Mutex::new(Instant::now())),
            created_at: Instant::now(),
            last_used_nanos: AtomicU64::new(0),
            in_flight: Arc::new(tokio::sync::Semaphore::new(1)),
        };

        conn.send_fire_and_forget(ApiKey::Produce, 0, |_| Ok(()))
            .await
            .unwrap();

        let Some(ConnectionCommand::FireAndForget { data }) = normal_priority_rx.recv().await
        else {
            panic!("expected fire-and-forget command");
        };

        let frame_len = i32::from_be_bytes(data[..4].try_into().unwrap()) as usize;
        assert_eq!(frame_len, data.len() - 4);
        let correlation_id = i32::from_be_bytes(data[8..12].try_into().unwrap());
        assert_eq!(correlation_id, NO_RESPONSE_CORRELATION_ID);
        assert_eq!(conn.correlation_id_gen.next(), 1);
    }

    // ========================================================================
    // KIP-368: Session lifetime / reauthentication
    // ========================================================================

    #[test]
    fn test_compute_session_expiry_zero_means_no_expiry() {
        assert!(
            BrokerConnection::compute_session_expiry(0).is_none(),
            "session_lifetime_ms = 0 should mean no expiry"
        );
    }

    #[test]
    fn test_compute_session_expiry_negative_means_no_expiry() {
        assert!(
            BrokerConnection::compute_session_expiry(-1).is_none(),
            "negative session_lifetime_ms should mean no expiry"
        );
    }

    #[test]
    fn test_compute_session_expiry_applies_jittered_margin() {
        let before = Instant::now();
        let expiry = BrokerConnection::compute_session_expiry(10_000).unwrap();
        let after = Instant::now();

        // Randomised window: 85-95% of 10_000ms = 8_500-9_500ms
        let expected_low = before + Duration::from_millis(8_500);
        let expected_high = after + Duration::from_millis(9_500);

        assert!(
            expiry >= expected_low && expiry <= expected_high,
            "expiry should be between 8.5s and 9.5s from now (85-95% of 10s)"
        );
    }

    #[test]
    fn test_compute_session_expiry_jitter_varies() {
        // Call multiple times and verify we don't always get the exact same value.
        // With 10% jitter on a 100s lifetime, outcomes should vary.
        let results: Vec<Instant> = (0..20)
            .map(|_| BrokerConnection::compute_session_expiry(100_000).unwrap())
            .collect();
        let first = results[0];
        let any_different = results.iter().any(|r| *r != first);
        assert!(
            any_different,
            "20 calls should produce at least one different expiry (randomised jitter)"
        );
    }

    #[test]
    fn test_compute_session_expiry_small_lifetime() {
        // Even very short lifetimes should produce a valid expiry
        let expiry = BrokerConnection::compute_session_expiry(100);
        assert!(expiry.is_some(), "100ms lifetime should produce an expiry");
    }

    #[tokio::test]
    async fn test_session_lifetime_tracked_from_broker() {
        // Mock broker that reports a 60-second session lifetime
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let mock_handle = tokio::spawn(run_mock_sasl_broker_with_lifetime(
            listener,
            60_000,
            shutdown_rx,
        ));

        let config = ConnectionConfig::builder()
            .client_id("test-client")
            .auth(crate::auth::AuthConfig::sasl_plain("user", "pass").unwrap())
            .build()
            .unwrap();

        let conn = BrokerConnection::connect(&addr, config).await.unwrap();

        // The connection should have a session expiry set
        assert!(
            conn.session_expiry().is_some(),
            "session_expiry should be set when broker reports a lifetime"
        );

        // The expiry should be roughly 51-57s from now (85-95% of 60s, randomised)
        let remaining = conn.session_expiry().unwrap() - Instant::now();
        assert!(
            remaining > Duration::from_secs(49) && remaining < Duration::from_secs(58),
            "session expiry should be ~51-57s from now (85-95% of 60s), got {:?}",
            remaining
        );

        // Should not need reauthentication immediately
        assert!(
            !conn.needs_reauthentication(),
            "fresh connection should not need reauthentication"
        );

        // is_usable should be true
        assert!(conn.is_usable(), "fresh connection should be usable");

        conn.close().await;
        let _ = shutdown_tx.send(());
        mock_handle.await.unwrap();
    }

    #[tokio::test]
    async fn test_no_session_expiry_when_lifetime_zero() {
        // Mock broker that reports 0 session lifetime (no expiry)
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let mock_handle =
            tokio::spawn(run_mock_sasl_broker_with_lifetime(listener, 0, shutdown_rx));

        let config = ConnectionConfig::builder()
            .client_id("test-client")
            .auth(crate::auth::AuthConfig::sasl_plain("user", "pass").unwrap())
            .build()
            .unwrap();

        let conn = BrokerConnection::connect(&addr, config).await.unwrap();

        assert!(
            conn.session_expiry().is_none(),
            "session_expiry should be None when broker reports 0"
        );
        assert!(
            !conn.needs_reauthentication(),
            "should never need reauth with no session lifetime"
        );
        assert!(conn.is_usable());

        conn.close().await;
        let _ = shutdown_tx.send(());
        mock_handle.await.unwrap();
    }

    // ========================================================================
    // KIP-219: Broker throttle compliance
    // ========================================================================

    // The KIP-219 behaviour is exercised against a real `BrokerConnection` in
    // `testing::tests::broker_throttle_is_honoured_and_counted`. Three unit
    // tests used to live here that built their own `parking_lot::Mutex<Instant>`
    // and asserted over `checked_duration_since` — they tested `std::time`
    // arithmetic, never touched `BrokerConnection`, and so could not have
    // caught the defect where the producer's throttle wait went uncounted.

    #[test]
    fn test_extract_clock_skew_secs_valid_timestamp() {
        // Simulate an AWS error containing a server timestamp.
        // The exact skew depends on when the test runs, but the function
        // should return a non-zero value for a timestamp far from now.
        let msg = "Signature expired: 20200101T000000Z is now past";
        let skew = BrokerConnection::extract_clock_skew_secs(msg);
        // 2020-01-01 is in the past, so skew should be negative.
        assert!(skew < 0, "expected negative skew, got {skew}");
    }

    #[test]
    fn test_extract_clock_skew_secs_no_timestamp() {
        let msg = "some random error message";
        assert_eq!(BrokerConnection::extract_clock_skew_secs(msg), 0);
    }

    #[test]
    fn test_extract_clock_skew_secs_malformed_timestamp() {
        let msg = "Signature expired: 2020XXYYT000000Z";
        assert_eq!(BrokerConnection::extract_clock_skew_secs(msg), 0);
    }

    #[test]
    fn test_extract_clock_skew_secs_invalid_calendar_date() {
        // Month 13 / day 32 / hour 25 — the hand-rolled parser accepted these
        // with range checks; `time` rejects them at parse time.
        assert_eq!(
            BrokerConnection::extract_clock_skew_secs("foo 20201301T000000Z bar"),
            0
        );
        assert_eq!(
            BrokerConnection::extract_clock_skew_secs("foo 20200132T000000Z bar"),
            0
        );
        assert_eq!(
            BrokerConnection::extract_clock_skew_secs("foo 20200101T250000Z bar"),
            0
        );
    }

    #[test]
    fn test_extract_clock_skew_secs_leap_day() {
        // Feb 29 2020 is valid (leap year); Feb 29 2021 is not.
        assert_ne!(
            BrokerConnection::extract_clock_skew_secs("stamp=20200229T120000Z"),
            0
        );
        assert_eq!(
            BrokerConnection::extract_clock_skew_secs("stamp=20210229T120000Z"),
            0
        );
    }

    #[test]
    fn test_extract_clock_skew_secs_embedded_in_longer_message() {
        // Multiple 'T' chars earlier in the message should not fool the scanner.
        let msg = "RequestTime=THIS IS TEXT; expired; server 20200101T000000Z -- request rejected";
        let skew = BrokerConnection::extract_clock_skew_secs(msg);
        assert!(skew < 0);
    }

    #[test]
    fn test_extract_clock_skew_secs_multiple_timestamps_uses_last() {
        // AWS "Signature not yet current" errors embed both the request
        // timestamp and the validity-window start. The last timestamp
        // (validity-window start) is the closer approximation of server time
        // and should be used.
        //
        // Use two timestamps far apart so the choice is unambiguous:
        //  - first:  2020-01-01 (far past → large negative skew)
        //  - second: 2099-01-01 (far future → large positive skew)
        // If we get a positive skew, the function used the LAST timestamp.
        let msg = "Signature not yet current: 20200101T000000Z is not yet valid, \
                   not before 20990101T000000Z; check your system clock";
        let skew = BrokerConnection::extract_clock_skew_secs(msg);
        assert!(
            skew > 0,
            "expected positive skew (last timestamp used), got {skew}"
        );
    }

    #[test]
    fn test_msk_iam_clock_offset_default() {
        let config = ConnectionConfig::default();
        assert_eq!(config.msk_iam_clock_offset_secs.load(Ordering::Relaxed), 0);
    }

    // ── parse_aws_ts_unix ─────────────────────────────────────────────────────

    #[test]
    fn test_parse_aws_ts_unix_epoch() {
        // 1970-01-01T00:00:00Z == Unix timestamp 0.
        let ts = BrokerConnection::parse_aws_ts_unix(b"19700101T000000Z");
        assert_eq!(ts, Some(0));
    }

    #[test]
    fn test_parse_aws_ts_unix_known_date() {
        // 2020-01-01T00:00:00Z == 1577836800  (verified with date -d).
        let ts = BrokerConnection::parse_aws_ts_unix(b"20200101T000000Z");
        assert_eq!(ts, Some(1_577_836_800));
    }

    #[test]
    fn test_parse_aws_ts_unix_leap_day_valid() {
        // 2020-02-29 is valid (2020 is a leap year).
        assert!(BrokerConnection::parse_aws_ts_unix(b"20200229T000000Z").is_some());
    }

    #[test]
    fn test_parse_aws_ts_unix_leap_day_invalid() {
        // 2021-02-29 does not exist.
        assert_eq!(
            BrokerConnection::parse_aws_ts_unix(b"20210229T000000Z"),
            None
        );
    }

    #[test]
    fn test_parse_aws_ts_unix_invalid_month() {
        assert_eq!(
            BrokerConnection::parse_aws_ts_unix(b"20201301T000000Z"),
            None
        );
        assert_eq!(
            BrokerConnection::parse_aws_ts_unix(b"20200001T000000Z"),
            None
        );
    }

    #[test]
    fn test_parse_aws_ts_unix_invalid_hour() {
        assert_eq!(
            BrokerConnection::parse_aws_ts_unix(b"20200101T250000Z"),
            None
        );
    }

    #[test]
    fn test_parse_aws_ts_unix_non_digit_chars() {
        assert_eq!(
            BrokerConnection::parse_aws_ts_unix(b"2020XXYYT000000Z"),
            None
        );
    }

    #[test]
    fn test_msk_iam_clock_offset_clamps_to_sigv4_window() {
        assert_eq!(BrokerConnection::clamp_msk_iam_clock_offset_secs(450), 300);
        assert_eq!(
            BrokerConnection::clamp_msk_iam_clock_offset_secs(-450),
            -300
        );
        assert_eq!(BrokerConnection::clamp_msk_iam_clock_offset_secs(120), 120);
    }

    /// An in-flight request must be failed with a timeout error when no
    /// response arrives within `request_timeout`.  This tests the
    /// `DelayQueue`-driven per-request timeout path introduced in the H2 fix.
    #[tokio::test]
    async fn test_request_times_out_when_no_response() {
        let (client, _server) = tokio::io::duplex(4096);
        let (reader, writer) = tokio::io::split(client);
        let (_high_tx, high_rx) = mpsc::channel(4);
        let (normal_tx, normal_rx) = mpsc::channel(4);
        let stats = Arc::new(ConnectionStats::default());
        let metrics = Arc::new(ConnectionMetrics::default());

        // Very short timeout so the test completes quickly.
        let request_timeout = Duration::from_millis(50);

        tokio::spawn(BrokerConnection::run_connection_loop(
            reader,
            writer,
            ConnectionLoopParams {
                address: "test-broker".to_string(),
                high_priority_rx: high_rx,
                normal_priority_rx: normal_rx,
                request_timeout,
                stats,
                metrics,
                max_response_size: crate::protocol::MAX_MESSAGE_SIZE,
                max_in_flight_requests: 256,
                max_high_priority_bypasses: 4,
            },
        ));

        let (response_tx, response_rx) = oneshot::channel();
        normal_tx
            .send(ConnectionCommand::Request {
                // Minimal 4-byte payload; the server side (_server) never replies.
                data: Bytes::from_static(b"test"),
                correlation_id: 42,
                api_key: ApiKey::Produce,
                api_version: 0,
                response_tx,
                timeout: request_timeout,
                permit: test_permit(),
            })
            .await
            .unwrap();

        let err = response_rx.await.unwrap().unwrap_err();
        assert!(
            err.to_string().contains("timed out"),
            "expected timeout error, got: {err}"
        );
    }

    /// A response that arrives before the deadline must cancel the timer so
    /// no spurious timeout error is delivered after the successful response.
    #[tokio::test]
    async fn test_response_cancels_timeout() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (client, mut server) = tokio::io::duplex(4096);
        let (reader, writer) = tokio::io::split(client);
        let (_high_tx, high_rx) = mpsc::channel(4);
        let (normal_tx, normal_rx) = mpsc::channel(4);
        let stats = Arc::new(ConnectionStats::default());
        let metrics = Arc::new(ConnectionMetrics::default());

        // Long enough to not fire during the test.
        let request_timeout = Duration::from_secs(5);
        let correlation_id: i32 = 99;

        tokio::spawn(BrokerConnection::run_connection_loop(
            reader,
            writer,
            ConnectionLoopParams {
                address: "test-broker".to_string(),
                high_priority_rx: high_rx,
                normal_priority_rx: normal_rx,
                request_timeout,
                stats,
                metrics,
                max_response_size: crate::protocol::MAX_MESSAGE_SIZE,
                max_in_flight_requests: 256,
                max_high_priority_bypasses: 4,
            },
        ));

        let (response_tx, response_rx) = oneshot::channel();
        normal_tx
            .send(ConnectionCommand::Request {
                data: Bytes::from_static(b"test"),
                correlation_id,
                api_key: ApiKey::Produce,
                api_version: 0,
                response_tx,
                timeout: request_timeout,
                permit: test_permit(),
            })
            .await
            .unwrap();

        // Drain the request bytes the loop wrote to the wire.
        let mut buf = [0u8; 4];
        server.read_exact(&mut buf).await.unwrap();

        // Send a valid response: 4-byte length prefix + 4-byte correlation_id.
        // For Produce v0, ResponseHeader v0 = correlation_id only (4 bytes).
        let body = correlation_id.to_be_bytes();
        server.write_all(&(4i32).to_be_bytes()).await.unwrap();
        server.write_all(&body).await.unwrap();
        server.flush().await.unwrap();

        let result = response_rx.await.unwrap();
        assert!(
            result.is_ok(),
            "expected successful response before timeout, got: {:?}",
            result.unwrap_err()
        );
    }

    /// A request carrying its own, longer budget must survive past the
    /// connection's `request_timeout`.
    ///
    /// `JoinGroup` is the case that matters: the coordinator holds it open for
    /// the length of the group's rebalance, which is bounded by
    /// `max.poll.interval.ms` and routinely exceeds `request.timeout.ms`. If
    /// the event loop expired it on the connection-wide budget, every rebalance
    /// slower than `request.timeout.ms` would fail client-side while the broker
    /// was still working on it.
    #[tokio::test]
    async fn test_per_request_timeout_outlives_connection_request_timeout() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let (client, mut server) = tokio::io::duplex(4096);
        let (reader, writer) = tokio::io::split(client);
        let (_high_tx, high_rx) = mpsc::channel(4);
        let (normal_tx, normal_rx) = mpsc::channel(4);
        let stats = Arc::new(ConnectionStats::default());
        let metrics = Arc::new(ConnectionMetrics::default());

        let correlation_id: i32 = 4242;

        tokio::spawn(BrokerConnection::run_connection_loop(
            reader,
            writer,
            ConnectionLoopParams {
                address: "test-broker".to_string(),
                high_priority_rx: high_rx,
                normal_priority_rx: normal_rx,
                // The connection-wide budget the request must not be held to.
                request_timeout: Duration::from_millis(150),
                stats,
                metrics,
                max_response_size: crate::protocol::MAX_MESSAGE_SIZE,
                max_in_flight_requests: 256,
                max_high_priority_bypasses: 4,
            },
        ));

        let (response_tx, response_rx) = oneshot::channel();
        normal_tx
            .send(ConnectionCommand::Request {
                data: Bytes::from_static(b"join"),
                correlation_id,
                api_key: ApiKey::JoinGroup,
                api_version: 0,
                response_tx,
                timeout: Duration::from_secs(5),
                permit: test_permit(),
            })
            .await
            .unwrap();

        let mut buf = [0u8; 4];
        server.read_exact(&mut buf).await.unwrap();

        // Answer well after the connection-wide timeout, well within the
        // request's own budget — exactly how a parked JoinGroup behaves.
        tokio::time::sleep(Duration::from_millis(600)).await;
        server.write_all(&(4i32).to_be_bytes()).await.unwrap();
        server
            .write_all(&correlation_id.to_be_bytes())
            .await
            .unwrap();
        server.flush().await.unwrap();

        let result = response_rx.await.unwrap();
        assert!(
            result.is_ok(),
            "a request with its own longer budget must not be expired at the \
             connection's request_timeout, got: {:?}",
            result.unwrap_err()
        );
    }

    // ── Connection loss / backpressure must be retriable ───────────────

    #[test]
    fn test_connection_closed_error_is_retriable() {
        // A clean EOF from a broker rolling restart or an idle reap is
        // recoverable. Reporting it as `InvalidState` (not in `is_retriable`'s
        // matched arms) would make callers give up permanently.
        let err = connection_closed_error();
        assert!(
            err.is_retriable(),
            "connection loss must be retriable, got: {err:?}"
        );
        assert!(matches!(err, KrafkaError::Network(_)));
    }

    #[test]
    fn test_in_flight_cap_error_is_retriable() {
        // The defence-in-depth rejection inside the event loop must not tell
        // callers that transient backpressure is permanent.
        let err = KrafkaError::network(std::io::Error::new(
            std::io::ErrorKind::WouldBlock,
            "max in-flight requests (10) reached; retry",
        ));
        assert!(err.is_retriable());
    }

    #[tokio::test]
    async fn test_in_flight_permits_bound_concurrency() {
        // The semaphore, not the channel depth, decides admission.
        let sem = Arc::new(tokio::sync::Semaphore::new(2));
        let a = sem.clone().try_acquire_owned().unwrap();
        let b = sem.clone().try_acquire_owned().unwrap();
        assert!(
            sem.clone().try_acquire_owned().is_err(),
            "third acquire must block once the cap is reached"
        );
        drop(a);
        assert!(
            sem.clone().try_acquire_owned().is_ok(),
            "releasing a permit must admit the next request"
        );
        drop(b);
    }

    // ── Pre-authentication reads are bounded in size and time ──────────

    #[tokio::test]
    async fn test_read_framed_response_rejects_oversized_sasl_frame() {
        // A hostile bootstrap endpoint declaring a large frame pre-auth must
        // be rejected against MAX_SASL_FRAME_BYTES, not max_response_size.
        let declared = (MAX_SASL_FRAME_BYTES as i32) + 1;
        let mut cursor = std::io::Cursor::new(declared.to_be_bytes().to_vec());
        let err = BrokerConnection::read_framed_response(&mut cursor, MAX_SASL_FRAME_BYTES)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("pre-authentication"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_sasl_frame_cap_is_far_below_max_response_size() {
        // Regression guard for the pre-auth allocation amplification factor:
        // 100 MiB of zeroed heap per connection, pre-auth, from a 4-byte
        // length prefix.
        const { assert!(MAX_SASL_FRAME_BYTES < crate::protocol::MAX_MESSAGE_SIZE / 1000) };
    }

    #[tokio::test]
    async fn test_read_framed_response_does_not_preallocate_declared_length() {
        // Peer declares a large (but legal) frame and then sends nothing.
        // The read must fail with EOF rather than having already reserved the
        // full declared length.
        let declared = (MAX_SASL_FRAME_BYTES as i32) - 1;
        let mut data = declared.to_be_bytes().to_vec();
        data.extend_from_slice(b"only a few bytes");
        let mut cursor = std::io::Cursor::new(data);
        let err = BrokerConnection::read_framed_response(&mut cursor, MAX_SASL_FRAME_BYTES)
            .await
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("peer closed during SASL handshake"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn test_read_handshake_frame_times_out_on_silent_peer() {
        // TCP completes, then the peer never writes.
        // Without a deadline this hangs connect() — and, through the pool's
        // per-address coalescing slot, every task targeting that broker.
        let (mut client, _server) = tokio::io::duplex(64);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(50);
        let err = BrokerConnection::read_handshake_frame(&mut client, deadline, "SaslHandshake")
            .await
            .unwrap_err();
        assert!(matches!(err, KrafkaError::Timeout { .. }), "got: {err:?}");
    }

    #[tokio::test]
    async fn test_read_handshake_frame_times_out_on_dribbling_peer() {
        // Declares a frame, sends the prefix, then stalls forever.
        let (mut client, mut server) = tokio::io::duplex(1024);
        tokio::spawn(async move {
            let _ = server.write_all(&1024i32.to_be_bytes()).await;
            let _ = server.write_all(b"partial").await;
            // Hold the stream open without completing the frame.
            std::future::pending::<()>().await;
        });
        let deadline = tokio::time::Instant::now() + Duration::from_millis(100);
        let err = BrokerConnection::read_handshake_frame(&mut client, deadline, "SaslAuthenticate")
            .await
            .unwrap_err();
        assert!(matches!(err, KrafkaError::Timeout { .. }), "got: {err:?}");
    }

    // ── SCRAM channel binding is opt-out, default on ───────────────────

    #[test]
    fn test_scram_channel_binding_defaults_to_enabled() {
        let auth = crate::auth::AuthConfig::sasl_scram_sha256("u", "p");
        assert!(
            auth.scram_channel_binding(),
            "channel binding must stay on by default"
        );
    }

    #[test]
    fn test_scram_channel_binding_can_be_disabled() {
        let auth =
            crate::auth::AuthConfig::sasl_scram_sha256("u", "p").with_scram_channel_binding(false);
        assert!(!auth.scram_channel_binding());
    }

    // ══════════════════════════════════════════════════════════════════
    // KIP-219: reading throttle_time_ms off the response without decoding
    // ══════════════════════════════════════════════════════════════════

    use bytes::BufMut as _;

    fn body_with_leading_i32(value: i32) -> Vec<u8> {
        value.to_be_bytes().to_vec()
    }

    #[test]
    fn test_leading_throttle_is_read_for_an_api_that_reports_it_first() {
        let body = body_with_leading_i32(1234);
        assert_eq!(
            leading_throttle_time_ms(ApiKey::CreateTopics, 4, &body),
            Some(1234)
        );
    }

    #[test]
    fn test_leading_throttle_is_ignored_below_the_versions_that_carry_it() {
        // CreateTopics gained throttle_time_ms in v2; in v0/v1 the first four
        // bytes are the topics array length, which must never be read as a
        // throttle.
        let body = body_with_leading_i32(3);
        assert_eq!(
            leading_throttle_time_ms(ApiKey::CreateTopics, 1, &body),
            None
        );
        assert_eq!(
            leading_throttle_time_ms(ApiKey::CreateTopics, 2, &body),
            Some(3)
        );
    }

    #[test]
    fn test_produce_is_excluded_from_the_leading_throttle_hook() {
        // Produce reports throttle_time_ms as the response's *trailing* field;
        // its leading bytes are the topics array length. The produce paths
        // forward the decoded value themselves.
        let body = body_with_leading_i32(50_000);
        assert_eq!(leading_throttle_time_ms(ApiKey::Produce, 10, &body), None);
        assert!(
            ApiKey::Produce
                .leading_throttle_time_min_version()
                .is_none()
        );
    }

    #[test]
    fn test_apis_without_a_leading_throttle_are_excluded() {
        for api_key in [
            ApiKey::ApiVersions,
            ApiKey::SaslHandshake,
            ApiKey::SaslAuthenticate,
            ApiKey::OffsetDelete,
            ApiKey::CreateDelegationToken,
            ApiKey::DescribeDelegationToken,
            ApiKey::WriteTxnMarkers,
            ApiKey::DescribeQuorum,
        ] {
            assert!(
                api_key.leading_throttle_time_min_version().is_none(),
                "{api_key:?} does not report throttle_time_ms first"
            );
        }
    }

    #[test]
    fn test_implausible_and_non_positive_throttles_are_ignored() {
        // A value this large is not a throttle any broker would ask for; it is
        // far more likely a table mistake, and honouring it would stall the
        // connection for hours.
        let huge = body_with_leading_i32(MAX_HONOURED_THROTTLE_MS + 1);
        assert_eq!(leading_throttle_time_ms(ApiKey::Metadata, 8, &huge), None);

        let at_limit = body_with_leading_i32(MAX_HONOURED_THROTTLE_MS);
        assert_eq!(
            leading_throttle_time_ms(ApiKey::Metadata, 8, &at_limit),
            Some(MAX_HONOURED_THROTTLE_MS)
        );

        for value in [0, -1, i32::MIN] {
            assert_eq!(
                leading_throttle_time_ms(ApiKey::Metadata, 8, &body_with_leading_i32(value)),
                None
            );
        }
    }

    #[test]
    fn test_a_truncated_body_is_not_peeked() {
        assert_eq!(
            leading_throttle_time_ms(ApiKey::Metadata, 8, &[0, 0, 1]),
            None
        );
        assert_eq!(leading_throttle_time_ms(ApiKey::Metadata, 8, &[]), None);
    }

    /// Cross-check the version table against the crate's own decoders: build a
    /// minimal response body, peek it, and confirm the real decoder reads the
    /// same number out of the same bytes.
    #[test]
    fn test_the_peeked_value_agrees_with_the_real_decoders() {
        use crate::protocol::VersionedDecode;

        // CreateTopics v4: throttle_time_ms, then an i32 array length.
        let mut body = BytesMut::new();
        body.put_i32(777);
        body.put_i32(0);
        let body = body.freeze();
        assert_eq!(
            leading_throttle_time_ms(ApiKey::CreateTopics, 4, &body),
            Some(777)
        );
        let decoded =
            crate::protocol::CreateTopicsResponse::decode_versioned(4, &mut body.clone()).unwrap();
        assert_eq!(decoded.throttle_time_ms, 777);

        // DeleteGroups v2 (flexible): throttle_time_ms, compact array, tags.
        let mut body = BytesMut::new();
        body.put_i32(4242);
        body.put_u8(1); // compact array length 0
        body.put_u8(0); // empty tagged fields
        let body = body.freeze();
        assert_eq!(
            leading_throttle_time_ms(ApiKey::DeleteGroups, 2, &body),
            Some(4242)
        );
        let decoded =
            crate::protocol::DeleteGroupsResponse::decode_versioned(2, &mut body.clone()).unwrap();
        assert_eq!(decoded.throttle_time_ms, 4242);
    }

    // ══════════════════════════════════════════════════════════════════
    // connect_timeout as the floor on request_timeout
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn test_a_short_request_timeout_is_accepted_once_connect_timeout_is_lowered() {
        let config = ConnectionConfig::builder()
            .request_timeout(Duration::from_secs(2))
            .connect_timeout(Duration::from_secs(2))
            .build()
            .expect("a 2 s request timeout is reachable by lowering connect_timeout");

        assert_eq!(config.request_timeout, Duration::from_secs(2));
        assert_eq!(config.connect_timeout, Duration::from_secs(2));
    }

    #[test]
    fn test_a_request_timeout_below_the_default_connect_timeout_is_rejected() {
        let err = ConnectionConfig::builder()
            .request_timeout(Duration::from_secs(2))
            .build()
            .expect_err("request_timeout below connect_timeout must not build");

        let message = err.to_string();
        assert!(
            message.contains("connect_timeout"),
            "the error must name the setter to change: {message}"
        );
    }

    #[test]
    fn test_the_default_connect_timeout_constant_is_what_the_builder_uses() {
        let config = ConnectionConfig::default();
        assert_eq!(config.connect_timeout, DEFAULT_CONNECT_TIMEOUT);
    }
}
