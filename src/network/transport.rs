//! Socket- and pool-level settings of a [`Kafka`](crate::Kafka) handle.
//!
//! [`ConnectionConfig`] and [`ConnectionPool`] carry the socket- and
//! pool-level settings — TCP keepalive, the per-connection response ceiling,
//! the in-flight cap, the idle-eviction window, a total-connection cap, the
//! SOCKS5 route and the TLS reload interval. [`TransportConfig`] collects
//! them from the [`KafkaBuilder`](crate::KafkaBuilder) setters of the same
//! names; `TransportConfig::default()` matches `ConnectionConfig::default()`
//! and the pool defaults. Every client built from one handle shares the pool,
//! so they share one network path by construction.
//!
//! [`ConnectionConfig`]: super::ConnectionConfig
//! [`ConnectionPool`]: super::ConnectionPool

use std::sync::Arc;
use std::time::Duration;

use crate::error::{KrafkaError, Result};

use super::connection::{ConnectionConfig, ConnectionConfigBuilder};
use super::pool::{ConnectionPool, DEFAULT_MAX_IDLE};

/// Socket- and pool-level tuning applied to every broker connection of a
/// [`Kafka`](crate::Kafka) handle, collected by its builder. [`Default`]
/// leaves every field at krafka's default.
#[derive(Debug, Clone)]
pub struct TransportConfig {
    /// Disable Nagle's algorithm on every broker socket. Default: `true`.
    ///
    /// Kafka requests are already batched by the producer accumulator and the
    /// consumer fetch, so Nagle only adds latency. Turn it off only if you are
    /// deliberately trading latency for packet count on a constrained link.
    pub(crate) tcp_nodelay: bool,

    /// TCP keepalive interval, or `None` to leave keepalive off.
    /// Default: `Some(60 s)`.
    ///
    /// This is the knob for the classic "the consumer stops receiving after
    /// exactly N minutes" failure: a stateful firewall, NAT gateway or cloud
    /// load balancer silently drops an idle flow, and neither side notices
    /// until the next request times out. Set it below the middlebox's idle
    /// timeout.
    pub(crate) tcp_keepalive: Option<Duration>,

    /// Largest response frame the client will accept, in bytes.
    /// Default: 100 MiB.
    ///
    /// A frame declaring more than this closes the connection rather than
    /// allocating. Two forces pull in opposite directions:
    ///
    /// - **Raise it** when a topic can legitimately produce a response larger
    ///   than the ceiling. Kafka guarantees at least one complete record batch
    ///   per partition even when it exceeds `fetch.max.bytes`, so a topic whose
    ///   `max.message.bytes` is above this value produces a fetch the client
    ///   cannot read — and, because the same bytes come back on every retry, a
    ///   permanently stalled partition.
    /// - **Lower it** to bound worst-case memory: the per-connection ceiling is
    ///   `max_response_size × max_in_flight_requests`.
    ///
    /// Minimum 1 KiB; smaller values are raised to it.
    pub(crate) max_response_size: usize,

    /// Requests that may be outstanding on one connection before submitters
    /// block. Default: 10, matching the Kafka Java client.
    ///
    /// This is real backpressure, not a rejection threshold: a submitter waits
    /// for a free slot. Lower it to bound memory (see
    /// [`max_response_size`](Self::max_response_size)); raise it to keep a
    /// high-latency link busy.
    ///
    /// Note that the producer's own `max_in_flight` is a separate, per-batch
    /// limit and is what enforces the idempotent-ordering cap of 5.
    ///
    /// Minimum 1.
    pub(crate) max_in_flight_requests: usize,

    /// Happy Eyeballs (RFC 8305 §5) stagger between parallel connection
    /// attempts. Default: 250 ms, clamped to 100 ms – 2 s at connect time.
    pub(crate) connection_attempt_delay: Duration,

    /// How long a pooled connection may sit unused before the background
    /// evictor closes it, or `None` to disable eviction.
    /// Default: `Some(9 min)`, matching the Java client's
    /// `connections.max.idle.ms`.
    pub(crate) connections_max_idle: Option<Duration>,

    /// Cap on live connections across all brokers, or `None` for unlimited.
    /// Default: `None`.
    ///
    /// A connection attempt that would exceed the cap fails instead of opening
    /// another socket. Set it on clusters whose broker count can jump — a
    /// metadata refresh that suddenly reports hundreds of brokers otherwise
    /// exhausts the process's file descriptors.
    pub(crate) max_connections: Option<usize>,

    /// SOCKS5 route every broker connection is tunnelled through, or `None`
    /// for a direct connection. Default: `None`.
    ///
    /// A broker reachable only through a bastion is reachable only through
    /// it for every client of the handle.
    pub(crate) proxy: Option<crate::network::ProxyConfig>,

    /// `SO_SNDBUF` for every broker socket, or `None` to leave the OS default.
    /// Default: `None`.
    ///
    /// Equivalent to the Java client's `send.buffer.bytes` and librdkafka's
    /// `socket.send.buffer.bytes`. The kernel may round or cap the request; on
    /// Linux the effective value is roughly double what is asked for, and
    /// `net.core.wmem_max` is the ceiling.
    ///
    /// Worth setting on a high bandwidth-delay-product link — cross-region
    /// replication, a producer writing across availability zones — where the
    /// default socket buffer, not the network, is the throughput ceiling.
    pub(crate) socket_send_buffer: Option<usize>,

    /// `SO_RCVBUF` for every broker socket, or `None` to leave the OS default.
    /// Default: `None`.
    ///
    /// Equivalent to the Java client's `receive.buffer.bytes` and librdkafka's
    /// `socket.receive.buffer.bytes`. The consumer side of
    /// [`socket_send_buffer`](Self::socket_send_buffer), and the one that
    /// matters for a fetch-heavy client on a long link.
    pub(crate) socket_receive_buffer: Option<usize>,

    /// Interval at which TLS certificate and key files are re-read from disk,
    /// or `None` to never reload automatically. Default: `None`.
    ///
    /// This is krafka's answer to KIP-1288 (SSL hot reload, Kafka 4.2): a
    /// client whose certificates are rotated by cert-manager, Vault or an
    /// SDS sidecar picks up the new material without a restart. Existing TLS
    /// sessions keep the connector they handshaked with; every connection
    /// opened after a successful reload uses the new one.
    ///
    /// A reload that fails (file missing mid-rotation, half-written PEM) is
    /// logged and the previous connector stays active, so an atomic-rename
    /// rotation and a non-atomic one both converge.
    ///
    /// For an event-driven rotation, call
    /// [`Kafka::refresh_tls`](crate::Kafka::refresh_tls) instead of — or in
    /// addition to — setting an interval.
    pub(crate) tls_reload_interval: Option<Duration>,
}

impl Default for TransportConfig {
    /// `TCP_NODELAY` on, 60 s keepalive, `MAX_MESSAGE_SIZE` responses, 10
    /// in-flight requests per connection, 250 ms connection-attempt delay,
    /// `DEFAULT_MAX_IDLE` idle timeout; no connection cap, proxy, socket
    /// buffer sizes or TLS reload.
    fn default() -> Self {
        Self {
            tcp_nodelay: true,
            tcp_keepalive: Some(Duration::from_secs(60)),
            max_response_size: crate::protocol::MAX_MESSAGE_SIZE,
            max_in_flight_requests: 10,
            connection_attempt_delay: Duration::from_millis(250),
            connections_max_idle: Some(DEFAULT_MAX_IDLE),
            max_connections: None,
            proxy: None,
            socket_send_buffer: None,
            socket_receive_buffer: None,
            tls_reload_interval: None,
        }
    }
}

impl TransportConfig {
    /// Create a builder pre-populated with the defaults.
    pub fn builder() -> TransportConfigBuilder {
        TransportConfigBuilder(Self::default())
    }

    /// Whether TCP nodelay is enabled.
    #[inline]
    #[must_use]
    pub fn tcp_nodelay(&self) -> bool {
        self.tcp_nodelay
    }

    /// The TCP keepalive interval, if any.
    #[inline]
    #[must_use]
    pub fn tcp_keepalive(&self) -> Option<Duration> {
        self.tcp_keepalive
    }

    /// The maximum accepted response frame size in bytes.
    #[inline]
    #[must_use]
    pub fn max_response_size(&self) -> usize {
        self.max_response_size
    }

    /// The per-connection in-flight request cap.
    #[inline]
    #[must_use]
    pub fn max_in_flight_requests(&self) -> usize {
        self.max_in_flight_requests
    }

    /// The SOCKS5 route, or `None` for a direct connection.
    #[inline]
    #[must_use]
    pub fn proxy(&self) -> Option<&crate::network::ProxyConfig> {
        self.proxy.as_ref()
    }

    /// The configured `SO_SNDBUF`, or `None` for the OS default.
    #[inline]
    #[must_use]
    pub fn socket_send_buffer(&self) -> Option<usize> {
        self.socket_send_buffer
    }

    /// The configured `SO_RCVBUF`, or `None` for the OS default.
    #[inline]
    #[must_use]
    pub fn socket_receive_buffer(&self) -> Option<usize> {
        self.socket_receive_buffer
    }

    /// The Happy Eyeballs connection-attempt stagger.
    #[inline]
    #[must_use]
    pub fn connection_attempt_delay(&self) -> Duration {
        self.connection_attempt_delay
    }

    /// The idle-eviction window, if eviction is enabled.
    #[inline]
    #[must_use]
    pub fn connections_max_idle(&self) -> Option<Duration> {
        self.connections_max_idle
    }

    /// The total-connection cap, if any.
    #[inline]
    #[must_use]
    pub fn max_connections(&self) -> Option<usize> {
        self.max_connections
    }

    /// The automatic TLS-reload interval, if any.
    #[inline]
    #[must_use]
    pub fn tls_reload_interval(&self) -> Option<Duration> {
        self.tls_reload_interval
    }

    /// Apply the connection-level fields to a [`ConnectionConfigBuilder`].
    ///
    /// Pool-level fields ([`connections_max_idle`](Self::connections_max_idle),
    /// [`max_connections`](Self::max_connections),
    /// [`tls_reload_interval`](Self::tls_reload_interval)) are applied by
    /// [`Self::build_pool`] instead — they belong to the pool, not the socket.
    pub(crate) fn apply(&self, builder: ConnectionConfigBuilder) -> ConnectionConfigBuilder {
        let builder = builder
            .nodelay(self.tcp_nodelay)
            .tcp_keepalive(self.tcp_keepalive)
            .max_response_size(self.max_response_size)
            .max_in_flight_requests(self.max_in_flight_requests)
            .connection_attempt_delay(self.connection_attempt_delay)
            .socket_send_buffer(self.socket_send_buffer)
            .socket_receive_buffer(self.socket_receive_buffer);
        match self.proxy.clone() {
            Some(proxy) => builder.proxy(proxy),
            None => builder,
        }
    }

    /// Build a fully configured, running [`ConnectionPool`] from `config`.
    ///
    /// Applies the pool-level fields, starts the idle evictor (which also
    /// starts the OAUTHBEARER proactive-refresh task) and, when
    /// [`tls_reload_interval`](Self::tls_reload_interval) is set, the periodic
    /// TLS reload task.
    ///
    /// Every client construction path goes through here, which is what keeps
    /// the six of them from drifting apart again.
    pub(crate) fn build_pool(&self, config: ConnectionConfig) -> Arc<ConnectionPool> {
        let pool = Arc::new(
            ConnectionPool::new(config)
                .with_max_idle(self.connections_max_idle)
                .with_max_total_connections(self.max_connections),
        );
        pool.start_idle_evictor();
        if let Some(interval) = self.tls_reload_interval {
            pool.start_tls_reload(interval);
        }
        pool
    }
}

/// Builder for [`TransportConfig`].
///
/// Obtain with [`TransportConfig::builder`]. Every setter is optional; unset
/// fields keep krafka's historical defaults.
#[must_use = "builders do nothing until .build() is called"]
#[derive(Debug)]
pub struct TransportConfigBuilder(TransportConfig);

impl Default for TransportConfigBuilder {
    fn default() -> Self {
        TransportConfig::builder()
    }
}

impl TransportConfigBuilder {
    /// Set TCP nodelay. See [`TransportConfig::tcp_nodelay`].
    pub fn tcp_nodelay(mut self, enabled: bool) -> Self {
        self.0.tcp_nodelay = enabled;
        self
    }

    /// Set the TCP keepalive interval, or `None` to disable keepalive.
    /// See [`TransportConfig::tcp_keepalive`].
    pub fn tcp_keepalive(mut self, interval: Option<Duration>) -> Self {
        self.0.tcp_keepalive = interval;
        self
    }

    /// Set the maximum accepted response frame size.
    /// See [`TransportConfig::max_response_size`].
    pub fn max_response_size(mut self, bytes: usize) -> Self {
        self.0.max_response_size = bytes;
        self
    }

    /// Set the per-connection in-flight request cap.
    /// See [`TransportConfig::max_in_flight_requests`].
    pub fn max_in_flight_requests(mut self, max: usize) -> Self {
        self.0.max_in_flight_requests = max;
        self
    }

    /// See [`TransportConfig::proxy`].
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.0.proxy = Some(proxy);
        self
    }

    /// See [`TransportConfig::socket_send_buffer`].
    pub fn socket_send_buffer(mut self, bytes: Option<usize>) -> Self {
        self.0.socket_send_buffer = bytes;
        self
    }

    /// See [`TransportConfig::socket_receive_buffer`].
    pub fn socket_receive_buffer(mut self, bytes: Option<usize>) -> Self {
        self.0.socket_receive_buffer = bytes;
        self
    }

    /// Set the Happy Eyeballs connection-attempt stagger (RFC 8305 §5).
    pub fn connection_attempt_delay(mut self, delay: Duration) -> Self {
        self.0.connection_attempt_delay = delay;
        self
    }

    /// Set the idle-eviction window, or `None` to keep connections forever.
    /// See [`TransportConfig::connections_max_idle`].
    pub fn connections_max_idle(mut self, max_idle: Option<Duration>) -> Self {
        self.0.connections_max_idle = max_idle;
        self
    }

    /// Cap the total number of live connections, or `None` for unlimited.
    /// See [`TransportConfig::max_connections`].
    pub fn max_connections(mut self, limit: Option<usize>) -> Self {
        self.0.max_connections = limit;
        self
    }

    /// Re-read TLS certificate files from disk every `interval` (KIP-1288).
    /// Pass `None` to disable automatic reloading.
    /// See [`TransportConfig::tls_reload_interval`].
    pub fn tls_reload_interval(mut self, interval: Option<Duration>) -> Self {
        self.0.tls_reload_interval = interval;
        self
    }

    /// Validate and build the config.
    ///
    /// # Errors
    ///
    /// Returns [`KrafkaError::Config`] when a value cannot be honoured:
    ///
    /// - `max_in_flight_requests` is 0 — no request could ever be sent.
    /// - `max_response_size` is below 1 KiB — smaller than a Kafka response
    ///   header plus its smallest useful body.
    /// - `connections_max_idle` or `tls_reload_interval` is `Some(ZERO)` — a
    ///   zero-period background task is a busy loop, not a schedule.
    ///
    /// Values that are merely unusual (a very deep channel, a 1 GiB response
    /// ceiling) are accepted; the connection layer warns where it matters.
    pub fn build(self) -> Result<TransportConfig> {
        if self.0.max_in_flight_requests == 0 {
            return Err(KrafkaError::config(
                "max_in_flight_requests must be >= 1; 0 would block every request forever",
            ));
        }
        const MIN_RESPONSE_SIZE: usize = 1024;
        if self.0.max_response_size < MIN_RESPONSE_SIZE {
            return Err(KrafkaError::config(format!(
                "max_response_size is {} B; the minimum is {MIN_RESPONSE_SIZE} B",
                self.0.max_response_size
            )));
        }
        if self.0.connections_max_idle == Some(Duration::ZERO) {
            return Err(KrafkaError::config(
                "connections_max_idle must be > 0; pass None to disable idle eviction",
            ));
        }
        if self.0.tls_reload_interval == Some(Duration::ZERO) {
            return Err(KrafkaError::config(
                "tls_reload_interval must be > 0; pass None to disable automatic TLS reloading",
            ));
        }
        if self.0.max_connections == Some(0) {
            return Err(KrafkaError::config(
                "max_connections must be >= 1; pass None for unlimited",
            ));
        }
        Ok(self.0)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// The defaults must equal `ConnectionConfig`'s own defaults.
    #[test]
    fn defaults_match_historical_connection_config() {
        let transport = TransportConfig::default();
        let legacy = ConnectionConfig::default();

        assert_eq!(transport.tcp_nodelay, legacy.nodelay());
        assert_eq!(transport.max_response_size, legacy.max_response_size());
        assert_eq!(
            transport.max_in_flight_requests,
            legacy.max_in_flight_requests()
        );
        assert_eq!(
            transport.connection_attempt_delay,
            legacy.connection_attempt_delay()
        );
        assert_eq!(transport.connections_max_idle, Some(DEFAULT_MAX_IDLE));
        assert_eq!(transport.max_connections, None);
        assert_eq!(transport.tls_reload_interval, None);
    }

    /// Applying the defaults to a fresh builder must be a no-op, so
    /// Socket buffer sizes must survive the journey from a client builder to
    /// the `ConnectionConfig` the socket is opened from.
    ///
    /// `SO_SNDBUF` / `SO_RCVBUF` were declared on `ConnectionConfig`, had public
    /// accessors, and were applied to the real socket by `happy_eyeballs.rs` —
    /// with no setter anywhere in the crate. Every krafka connection therefore
    /// took the OS default, which on a high bandwidth-delay-product link is the
    /// throughput ceiling. The gap survived because the only test that touched
    /// the fields assigned them directly, which crate-internal code can do and a
    /// user cannot.
    #[test]
    fn socket_buffer_sizes_reach_the_connection_config() {
        let transport = TransportConfig::builder()
            .socket_send_buffer(Some(4 * 1024 * 1024))
            .socket_receive_buffer(Some(2 * 1024 * 1024))
            .build()
            .expect("socket buffer sizes are always valid");

        assert_eq!(transport.socket_send_buffer(), Some(4 * 1024 * 1024));
        assert_eq!(transport.socket_receive_buffer(), Some(2 * 1024 * 1024));

        let applied = transport
            .apply(ConnectionConfig::builder())
            .build()
            .expect("a valid transport config yields a valid connection config");

        assert_eq!(
            applied.send_buffer_size(),
            Some(4 * 1024 * 1024),
            "SO_SNDBUF must reach the socket, not stop at the transport config"
        );
        assert_eq!(applied.recv_buffer_size(), Some(2 * 1024 * 1024));
    }

    /// A SOCKS5 route set on the transport config must reach the socket.
    ///
    /// Reported by a downstream project that mapped its own transport settings
    /// onto `TransportConfig` — the obvious thing to do with a type of that
    /// name — and shipped a producer that silently bypassed the proxy its
    /// deployment required. In the topology the setting exists for (brokers
    /// behind a bastion that also resolves their hostnames) the connection
    /// simply failed and looked like a broker outage; where the brokers were
    /// directly reachable, traffic left by the wrong egress path and nothing
    /// said so.
    ///
    /// What made it a trap rather than an omission is that this module's own
    /// documentation described a `TransportConfig` as carrying "the SOCKS5
    /// route", and warned that a client left on the default transport gets
    /// "no proxy" — describing a capability the type did not have.
    #[test]
    fn a_proxy_on_the_transport_config_reaches_the_connection() {
        use crate::network::ProxyConfig;

        let transport = TransportConfig::builder()
            .proxy(ProxyConfig::new("bastion:1080"))
            .build()
            .expect("a proxy address is not validated at build time");

        assert_eq!(transport.proxy().map(|p| p.address()), Some("bastion:1080"));

        let applied = transport
            .apply(ConnectionConfig::builder())
            .build()
            .expect("a valid transport config yields a valid connection config");

        assert_eq!(
            applied.proxy().map(|p| p.address()),
            Some("bastion:1080"),
            "the SOCKS5 route must survive TransportConfig -> ConnectionConfig"
        );
    }

    /// Leaving them unset must keep the OS default rather than forcing a size.
    #[test]
    fn socket_buffer_sizes_default_to_the_os() {
        let applied = TransportConfig::default()
            .apply(ConnectionConfig::builder())
            .build()
            .unwrap();
        assert_eq!(applied.send_buffer_size(), None);
        assert_eq!(applied.recv_buffer_size(), None);
    }

    /// Default transport settings produce the default connection config.
    #[test]
    fn applying_defaults_changes_nothing() {
        let applied = TransportConfig::default()
            .apply(ConnectionConfig::builder())
            .build()
            .unwrap();
        let plain = ConnectionConfig::default();

        assert_eq!(applied.nodelay(), plain.nodelay());
        assert_eq!(applied.max_response_size(), plain.max_response_size());
        assert_eq!(
            applied.max_in_flight_requests(),
            plain.max_in_flight_requests()
        );
        assert_eq!(
            applied.connection_attempt_delay(),
            plain.connection_attempt_delay()
        );
    }

    /// Every field must survive the builder → `ConnectionConfig` hop; a field
    /// that is settable but not applied gets caught here.
    #[test]
    fn every_connection_field_reaches_the_connection_config() {
        let transport = TransportConfig::builder()
            .tcp_nodelay(false)
            .tcp_keepalive(Some(Duration::from_secs(17)))
            .max_response_size(7 * 1024 * 1024)
            .max_in_flight_requests(3)
            .connection_attempt_delay(Duration::from_millis(400))
            .socket_send_buffer(Some(1024 * 1024))
            .socket_receive_buffer(Some(512 * 1024))
            .build()
            .unwrap();

        let config = transport
            .apply(ConnectionConfig::builder())
            .build()
            .unwrap();

        assert!(!config.nodelay());
        assert_eq!(config.max_response_size(), 7 * 1024 * 1024);
        assert_eq!(config.max_in_flight_requests(), 3);
        assert_eq!(
            config.connection_attempt_delay(),
            Duration::from_millis(400)
        );
        assert_eq!(config.send_buffer_size(), Some(1024 * 1024));
        assert_eq!(config.recv_buffer_size(), Some(512 * 1024));
    }

    #[test]
    fn rejects_zero_in_flight() {
        let err = TransportConfig::builder()
            .max_in_flight_requests(0)
            .build()
            .unwrap_err()
            .to_string();
        assert!(err.contains("max_in_flight_requests"), "got: {err}");
    }

    #[test]
    fn rejects_tiny_response_ceiling() {
        let err = TransportConfig::builder()
            .max_response_size(64)
            .build()
            .unwrap_err()
            .to_string();
        assert!(err.contains("max_response_size"), "got: {err}");
    }

    #[test]
    fn rejects_zero_intervals() {
        assert!(
            TransportConfig::builder()
                .connections_max_idle(Some(Duration::ZERO))
                .build()
                .is_err()
        );
        assert!(
            TransportConfig::builder()
                .tls_reload_interval(Some(Duration::ZERO))
                .build()
                .is_err()
        );
        assert!(
            TransportConfig::builder()
                .max_connections(Some(0))
                .build()
                .is_err()
        );
    }

    /// `None` is the documented way to switch a period off and must not be
    /// confused with the rejected zero.
    #[test]
    fn none_disables_rather_than_erroring() {
        let config = TransportConfig::builder()
            .connections_max_idle(None)
            .tls_reload_interval(None)
            .max_connections(None)
            .tcp_keepalive(None)
            .build()
            .unwrap();
        assert_eq!(config.connections_max_idle(), None);
        assert_eq!(config.tls_reload_interval(), None);
        assert_eq!(config.max_connections(), None);
        assert_eq!(config.tcp_keepalive(), None);
    }

    /// The pool-level fields must land on the pool, not be silently dropped
    /// the way `with_max_idle` / `with_max_total_connections` were.
    #[tokio::test]
    async fn pool_level_fields_reach_the_pool() {
        let transport = TransportConfig::builder()
            .connections_max_idle(Some(Duration::from_secs(120)))
            .max_connections(Some(42))
            .build()
            .unwrap();

        let pool = transport.build_pool(ConnectionConfig::default());
        assert_eq!(pool.max_idle(), Some(Duration::from_secs(120)));
        assert_eq!(pool.max_total_connections(), Some(42));
        pool.close_all().await;
    }
}
