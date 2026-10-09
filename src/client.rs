//! The shared [`Kafka`] handle: one place for every connection setting.
//!
//! A `Kafka` owns a connection pool and a cluster-metadata cache. Every client
//! is built from it — [`producer`](Kafka::producer),
//! [`consumer`](Kafka::consumer), [`share_consumer`](Kafka::share_consumer),
//! [`admin`](Kafka::admin) — and shares both, so one producer and two
//! consumers against a 5-broker cluster open 5 connections, not 15. Role
//! builders carry only role settings: a client cannot run with security,
//! transport or a `client_id` other than the handle's.
//!
//! The handle is cheap to clone. Clients hold a clone, so dropping the handle
//! while clients are alive is fine; the pool closes when its last owner is
//! dropped. Separate pools — a different identity, different credentials, an
//! isolated failure domain — are separate handles.
//!
//! ```rust,no_run
//! use krafka::Kafka;
//!
//! # async fn example() -> krafka::Result<()> {
//! let kafka = Kafka::builder("localhost:9092")
//!     .client_id("orders")
//!     .connect()
//!     .await?;
//!
//! let producer = kafka.producer().build().await?;
//! let consumer = kafka.consumer("my-group").build().await?;
//! # Ok(())
//! # }
//! ```

use std::sync::{Arc, Weak};
use std::time::Duration;

use tracing::info;

use crate::auth::AuthConfig;
use crate::error::{KrafkaError, Result};
use crate::metadata::{ClusterMetadata, MetadataRecoveryStrategy};
use crate::metrics::{Metrics, MetricsSource};
use crate::network::{
    ConnectionConfig, ConnectionPool, ProxyConfig, TransportConfig, TransportConfigBuilder,
};

/// A connection to a Kafka cluster that every client is built from.
///
/// Construct with [`Kafka::builder`]. Cheap to clone: clones share the pool
/// and the metadata cache.
#[derive(Clone)]
pub struct Kafka {
    inner: Arc<KafkaInner>,
}

struct KafkaInner {
    pool: Arc<ConnectionPool>,
    metadata: Arc<ClusterMetadata>,
    client_id: String,
    request_timeout: Duration,
    /// The clients built from this handle, for [`Kafka::metrics`].
    clients: parking_lot::Mutex<Vec<Weak<MetricsSource>>>,
}

impl std::fmt::Debug for Kafka {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Kafka")
            .field("client_id", &self.inner.client_id)
            .field("connections", &self.inner.pool.len())
            .finish_non_exhaustive()
    }
}

impl Kafka {
    /// Start configuring a connection to `bootstrap_servers`, a
    /// comma-separated list of `host:port` pairs such as
    /// `"broker1:9092,broker2:9092"`.
    pub fn builder(bootstrap_servers: impl Into<String>) -> KafkaBuilder {
        KafkaBuilder {
            bootstrap_servers: bootstrap_servers.into(),
            client_id: "krafka".to_string(),
            security: None,
            request_timeout: Duration::from_secs(30),
            connect_timeout: crate::network::DEFAULT_CONNECT_TIMEOUT,
            metadata_max_age: Duration::from_secs(300),
            metadata_recovery_strategy: MetadataRecoveryStrategy::Rebootstrap,
            metadata_recovery_rebootstrap_trigger: Duration::from_secs(300),
            metadata_topic_cache_ttl: Some(Duration::from_secs(300)),
            allow_auto_create_topics: false,
            transport: TransportConfig::builder(),
            #[cfg(feature = "test-broker")]
            connector: None,
        }
    }

    /// Start a producer.
    pub fn producer(&self) -> crate::producer::ProducerBuilder {
        crate::producer::ProducerBuilder::new(self.clone())
    }

    /// Start a consumer in consumer group `group_id`.
    pub fn consumer(&self, group_id: impl Into<String>) -> crate::consumer::ConsumerBuilder {
        crate::consumer::ConsumerBuilder::new(self.clone(), Some(group_id.into()))
    }

    /// Start a consumer that belongs to no group: partitions are
    /// [`assign`](crate::consumer::Consumer::assign)ed by hand (or every
    /// partition of the subscribed topics is taken), and offsets are not
    /// committed.
    pub fn consumer_without_group(&self) -> crate::consumer::ConsumerBuilder {
        crate::consumer::ConsumerBuilder::new(self.clone(), None)
    }

    /// Start a share consumer (KIP-932) in share group `group_id`.
    pub fn share_consumer(
        &self,
        group_id: impl Into<String>,
    ) -> crate::share_consumer::ShareConsumerBuilder {
        crate::share_consumer::ShareConsumerBuilder::new(self.clone(), group_id.into())
    }

    /// An admin client. Contacts no broker until its first call.
    pub fn admin(&self) -> crate::admin::AdminClient {
        crate::admin::AdminClient::new(self.clone())
    }

    /// Re-read TLS certificate and key files from disk and use them for every
    /// connection opened from now on, by every client of this handle
    /// (KIP-1288). Existing TLS sessions are unaffected. No-op without TLS.
    ///
    /// # Errors
    ///
    /// Returns an error if the files cannot be read or parsed; the previous
    /// material stays active.
    pub async fn refresh_tls(&self) -> Result<()> {
        self.inner.pool.refresh_tls().await
    }

    /// Replace the bootstrap server list used when the client falls back to
    /// bootstrapping (KIP-899). Existing connections stay open.
    ///
    /// # Errors
    ///
    /// Returns an error if `servers` is empty.
    pub fn update_seed_brokers(&self, servers: Vec<String>) -> Result<()> {
        self.inner.metadata.update_seed_brokers(servers)
    }

    /// Close every connection, clear the metadata cache and rediscover the
    /// cluster from the bootstrap servers (KIP-899).
    pub async fn rebootstrap(&self) {
        self.inner.metadata.rebootstrap().await;
    }

    /// The metrics of every live client built from this handle, summed, with
    /// the shared pool's connection counters counted once. The snapshot
    /// carries no `client_id`, so its
    /// [`prometheus_text`](Metrics::prometheus_text) has no client label.
    ///
    /// A client's counters leave the sum when the client is dropped.
    pub fn metrics(&self) -> Metrics {
        let mut total = Metrics::default();
        self.inner
            .clients
            .lock()
            .retain(|client| match client.upgrade() {
                Some(source) => {
                    total.add_client(&source.client_counters());
                    true
                }
                None => false,
            });
        total.connections = self.inner.pool.metrics();
        total
    }

    /// Count `source` in [`Kafka::metrics`] while it lives.
    pub(crate) fn register_metrics(&self, source: &Arc<MetricsSource>) {
        let mut clients = self.inner.clients.lock();
        clients.retain(|client| client.strong_count() > 0);
        clients.push(Arc::downgrade(source));
    }

    pub(crate) fn pool(&self) -> &Arc<ConnectionPool> {
        &self.inner.pool
    }

    pub(crate) fn metadata(&self) -> &Arc<ClusterMetadata> {
        &self.inner.metadata
    }

    pub(crate) fn client_id(&self) -> &str {
        &self.inner.client_id
    }

    pub(crate) fn request_timeout(&self) -> Duration {
        self.inner.request_timeout
    }

    /// A handle over an unconnected pool, for unit tests that exercise a
    /// builder's validation without a broker.
    #[cfg(test)]
    pub(crate) fn detached() -> Self {
        let pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
        let metadata = Arc::new(ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            Arc::clone(&pool),
            Duration::from_secs(300),
        ));
        Self {
            inner: Arc::new(KafkaInner {
                pool,
                metadata,
                client_id: "krafka".to_string(),
                request_timeout: Duration::from_secs(30),
                clients: parking_lot::Mutex::default(),
            }),
        }
    }
}

/// How a client's `close_with` shuts down.
///
/// Every client has `close()`, which is `close_with(CloseOptions::new())`.
/// Without a timeout each client uses its own default: a producer waits for
/// every queued record, a consumer and a share consumer bound the close by
/// 30 s.
#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct CloseOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) group_membership_operation: GroupMembershipOperation,
}

impl CloseOptions {
    /// The client's default close.
    pub fn new() -> Self {
        Self::default()
    }

    /// Bound the whole close by `timeout`.
    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Whether a [`Consumer`](crate::consumer::Consumer) in a group leaves it
    /// on close (KIP-1092). Other clients ignore it: a share consumer always
    /// leaves its group.
    pub fn group_membership_operation(mut self, operation: GroupMembershipOperation) -> Self {
        self.group_membership_operation = operation;
        self
    }
}

/// What a closing [`Consumer`](crate::consumer::Consumer) does about its group
/// membership (KIP-1092), set with
/// [`CloseOptions::group_membership_operation`].
///
/// | | Classic protocol | Consumer protocol (KIP-848) |
/// |---|---|---|
/// | `Default`, dynamic member | `LeaveGroup` | heartbeat at epoch −1 |
/// | `Default`, static member | nothing | heartbeat at epoch −2 |
/// | `LeaveGroup` | `LeaveGroup` (with the instance id) | heartbeat at epoch −1 |
/// | `RemainInGroup` | nothing | nothing |
///
/// A member that remains keeps its partitions until the session timeout
/// expires, so a restart within it rejoins without a rebalance only if it is a
/// static member; a dynamic member that remains is removed when its session
/// times out.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum GroupMembershipOperation {
    /// Dynamic members leave; static members keep their membership.
    #[default]
    Default,
    /// Leave the group, static members included.
    LeaveGroup,
    /// Send nothing: the coordinator removes the member when its session
    /// expires.
    RemainInGroup,
}

/// Builder for [`Kafka`]: every connection setting.
///
/// Obtain with [`Kafka::builder`]. Nothing is validated until
/// [`connect`](Self::connect).
#[must_use = "builders do nothing until .connect().await is called"]
pub struct KafkaBuilder {
    bootstrap_servers: String,
    client_id: String,
    security: Option<AuthConfig>,
    request_timeout: Duration,
    connect_timeout: Duration,
    metadata_max_age: Duration,
    metadata_recovery_strategy: MetadataRecoveryStrategy,
    metadata_recovery_rebootstrap_trigger: Duration,
    metadata_topic_cache_ttl: Option<Duration>,
    allow_auto_create_topics: bool,
    transport: TransportConfigBuilder,
    #[cfg(feature = "test-broker")]
    connector: Option<crate::network::connector::Connector>,
}

impl std::fmt::Debug for KafkaBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaBuilder")
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("client_id", &self.client_id)
            .finish_non_exhaustive()
    }
}

impl KafkaBuilder {
    /// The client id sent in every request header. Default: `"krafka"`.
    ///
    /// Shared by every client of the handle; build a second handle for a
    /// second identity.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = client_id.into();
        self
    }

    /// TLS and SASL for every connection: see [`AuthConfig`]. Default:
    /// plaintext.
    pub fn security(mut self, security: AuthConfig) -> Self {
        self.security = Some(security);
        self
    }

    /// How long one request may wait for its response. Default: 30 s.
    ///
    /// Must be at least [`connect_timeout`](Self::connect_timeout): a
    /// request's clock covers establishing the connection it is sent over.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// How long establishing a TCP connection to one broker may take.
    /// Default: 10 s.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// How long cluster metadata may be cached before a background refresh.
    /// Default: 5 min.
    pub fn metadata_max_age(mut self, age: Duration) -> Self {
        self.metadata_max_age = age;
        self
    }

    /// What to do when every known broker is unreachable (KIP-899).
    /// Default: [`MetadataRecoveryStrategy::Rebootstrap`].
    pub fn metadata_recovery_strategy(mut self, strategy: MetadataRecoveryStrategy) -> Self {
        self.metadata_recovery_strategy = strategy;
        self
    }

    /// How long metadata refreshes may keep failing before the client
    /// rebootstraps (KIP-1102). Default: 5 min.
    pub fn metadata_recovery_rebootstrap_trigger(mut self, duration: Duration) -> Self {
        self.metadata_recovery_rebootstrap_trigger = duration;
        self
    }

    /// How long a topic's metadata stays cached without being used, or
    /// `None` to keep it forever (Java `metadata.max.idle.ms`).
    /// Default: 5 min.
    pub fn metadata_topic_cache_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.metadata_topic_cache_ttl = ttl;
        self
    }

    /// Let the broker create a topic a client asks about but the cluster does
    /// not have (`allow.auto.create.topics`; the broker must also have
    /// `auto.create.topics.enable=true`). Default: `false` — a typo'd topic
    /// name otherwise silently becomes a real topic.
    pub fn allow_auto_create_topics(mut self, allow: bool) -> Self {
        self.allow_auto_create_topics = allow;
        self
    }

    /// Route every connection through a SOCKS5 proxy. Default: direct.
    pub fn proxy(mut self, proxy: ProxyConfig) -> Self {
        self.transport = self.transport.proxy(proxy);
        self
    }

    /// Disable Nagle's algorithm on every broker socket. Default: `true`.
    pub fn tcp_nodelay(mut self, enabled: bool) -> Self {
        self.transport = self.transport.tcp_nodelay(enabled);
        self
    }

    /// TCP keepalive interval, or `None` to leave keepalive off.
    /// Default: 60 s. Set it below the idle timeout of any firewall or load
    /// balancer between the client and the brokers.
    pub fn tcp_keepalive(mut self, interval: Option<Duration>) -> Self {
        self.transport = self.transport.tcp_keepalive(interval);
        self
    }

    /// Largest response frame accepted, in bytes. Default: 100 MiB; at least
    /// 1 KiB. A frame declaring more closes the connection.
    pub fn max_response_size(mut self, bytes: usize) -> Self {
        self.transport = self.transport.max_response_size(bytes);
        self
    }

    /// Requests that may be outstanding on one connection before senders
    /// wait. Default: 10; at least 1.
    pub fn max_in_flight_requests(mut self, max: usize) -> Self {
        self.transport = self.transport.max_in_flight_requests(max);
        self
    }

    /// `SO_SNDBUF` for every broker socket, or `None` for the OS default
    /// (Java `send.buffer.bytes`). Default: `None`.
    pub fn socket_send_buffer(mut self, bytes: Option<usize>) -> Self {
        self.transport = self.transport.socket_send_buffer(bytes);
        self
    }

    /// `SO_RCVBUF` for every broker socket, or `None` for the OS default
    /// (Java `receive.buffer.bytes`). Default: `None`.
    pub fn socket_receive_buffer(mut self, bytes: Option<usize>) -> Self {
        self.transport = self.transport.socket_receive_buffer(bytes);
        self
    }

    /// Stagger between parallel connection attempts to one broker's
    /// addresses (Happy Eyeballs, RFC 8305). Default: 250 ms, clamped to
    /// 100 ms – 2 s.
    pub fn connection_attempt_delay(mut self, delay: Duration) -> Self {
        self.transport = self.transport.connection_attempt_delay(delay);
        self
    }

    /// How long a connection may sit unused before it is closed, or `None`
    /// to keep it (Java `connections.max.idle.ms`). Default: 9 min.
    pub fn connections_max_idle(mut self, max_idle: Option<Duration>) -> Self {
        self.transport = self.transport.connections_max_idle(max_idle);
        self
    }

    /// Cap on live connections across all brokers, or `None` for no cap.
    /// Default: `None`.
    pub fn max_connections(mut self, limit: Option<usize>) -> Self {
        self.transport = self.transport.max_connections(limit);
        self
    }

    /// Re-read TLS certificate and key files every `interval` (KIP-1288), or
    /// `None` to reload only on [`Kafka::refresh_tls`]. Default: `None`.
    pub fn tls_reload_interval(mut self, interval: Option<Duration>) -> Self {
        self.transport = self.transport.tls_reload_interval(interval);
        self
    }

    /// Dial every broker through `connector` instead of the network.
    #[cfg(feature = "test-broker")]
    pub(crate) fn connector(mut self, connector: crate::network::connector::Connector) -> Self {
        self.connector = Some(connector);
        self
    }

    /// Validate the settings, open the pool and bootstrap the cluster
    /// metadata.
    ///
    /// # Errors
    ///
    /// [`KrafkaError::Config`] naming the setting for an invalid
    /// configuration (an empty bootstrap list, empty SASL credentials, a
    /// request timeout below the connect timeout, …); a network, timeout or
    /// authentication error if no bootstrap server answers.
    pub async fn connect(self) -> Result<Kafka> {
        if self.bootstrap_servers.trim().is_empty() {
            return Err(KrafkaError::config("bootstrap_servers is required"));
        }
        if self.client_id.len() > i16::MAX as usize {
            return Err(KrafkaError::config(format!(
                "client_id is {} bytes, exceeding the Kafka wire limit of {}",
                self.client_id.len(),
                i16::MAX
            )));
        }
        if let Some(security) = &self.security {
            security.validate()?;
        }
        let transport = self.transport.build()?;
        let bootstrap_servers = crate::util::parse_bootstrap_servers(&self.bootstrap_servers)?;

        let mut pool_config = transport.apply(
            ConnectionConfig::builder()
                .client_id(&self.client_id)
                .request_timeout(self.request_timeout)
                .connect_timeout(self.connect_timeout),
        );
        if let Some(security) = self.security {
            pool_config = pool_config.auth(security);
        }
        let mut pool_config = pool_config.build()?;
        #[cfg(feature = "test-broker")]
        {
            pool_config.connector = self.connector;
        }
        pool_config.init_tls().await?;
        let pool = transport.build_pool(pool_config);

        let metadata =
            ClusterMetadata::new(bootstrap_servers, Arc::clone(&pool), self.metadata_max_age)
                .with_recovery_strategy(self.metadata_recovery_strategy)
                .with_rebootstrap_trigger(self.metadata_recovery_rebootstrap_trigger)
                .with_auto_create_topics(self.allow_auto_create_topics);
        let metadata = Arc::new(match self.metadata_topic_cache_ttl {
            Some(ttl) => metadata.with_topic_cache_ttl(ttl),
            None => metadata.with_topic_cache_ttl_disabled(),
        });
        metadata.refresh().await?;

        info!(
            bootstrap_servers = %self.bootstrap_servers,
            brokers = metadata.brokers().len(),
            "connected"
        );

        Ok(Kafka {
            inner: Arc::new(KafkaInner {
                pool,
                metadata,
                client_id: self.client_id,
                request_timeout: self.request_timeout,
                clients: parking_lot::Mutex::default(),
            }),
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn an_empty_bootstrap_list_is_a_config_error() {
        let err = Kafka::builder("").connect().await.unwrap_err();
        assert!(matches!(err, KrafkaError::Config { .. }), "{err:?}");
        assert!(err.to_string().contains("bootstrap_servers"), "{err}");
    }

    #[tokio::test]
    async fn empty_sasl_credentials_are_a_config_error_naming_the_setting() {
        let err = Kafka::builder("127.0.0.1:1")
            .security(AuthConfig::sasl_plain("", "secret"))
            .connect()
            .await
            .unwrap_err();
        assert!(matches!(err, KrafkaError::Config { .. }), "{err:?}");
        assert!(err.to_string().contains("username"), "{err}");
    }

    #[tokio::test]
    async fn an_invalid_transport_setting_is_a_config_error() {
        let err = Kafka::builder("127.0.0.1:1")
            .max_in_flight_requests(0)
            .connect()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("max_in_flight_requests"), "{err}");
    }

    /// A 2 s request timeout is reachable only by lowering `connect_timeout`
    /// too; the connection layer rejects a request timeout below it.
    #[tokio::test]
    async fn a_request_timeout_below_connect_timeout_names_the_setting() {
        let err = Kafka::builder("127.0.0.1:1")
            .request_timeout(Duration::from_secs(2))
            .connect()
            .await
            .unwrap_err();
        assert!(err.to_string().contains("connect_timeout"), "{err}");

        let err = Kafka::builder("127.0.0.1:1")
            .request_timeout(Duration::from_secs(2))
            .connect_timeout(Duration::from_secs(2))
            .connect()
            .await
            .unwrap_err();
        assert!(!err.to_string().contains("connect_timeout"), "{err}");
    }

    #[tokio::test]
    async fn an_unreachable_cluster_fails_with_network_or_timeout() {
        let err = Kafka::builder("127.0.0.1:1")
            .request_timeout(Duration::from_secs(2))
            .connect_timeout(Duration::from_secs(1))
            .connect()
            .await
            .unwrap_err();
        assert!(
            matches!(err, KrafkaError::Network(_) | KrafkaError::Timeout { .. }),
            "{err:?}"
        );
    }
}
