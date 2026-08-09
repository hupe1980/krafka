//! Shared transport for connection pooling and metadata across multiple clients.
//!
//! By default every [`Producer`](crate::producer::Producer),
//! [`TransactionalProducer`](crate::producer::TransactionalProducer),
//! [`Consumer`](crate::consumer::Consumer), `ShareConsumer` and
//! [`AdminClient`](crate::admin::AdminClient) creates its own TCP connection
//! pool and metadata cache. An application that runs one producer and two
//! consumers against a 5-broker cluster therefore opens **15** TCP
//! connections.
//!
//! A [`KrafkaClient`] holds a single shared pool and a single shared metadata
//! cache. Passing one to each builder via
//! [`.with_client()`](crate::producer::ProducerBuilder::with_client) reduces
//! the connection count to **5** regardless of how many client objects are
//! created. Every client builder accepts it.
//!
//! # Who closes the pool
//!
//! The `KrafkaClient` does. A client built with `.with_client(..)` **borrows**
//! the pool: its own `close()` shuts that client down and leaves the sockets
//! alone, and its `owns_pool()` returns `false`. Closing the `KrafkaClient`
//! releases them.
//!
//! This is load-bearing rather than cosmetic. A client that tore down a
//! borrowed pool would kill every sibling's connections and fail their
//! in-flight Produce and Fetch requests — undoing the whole point of sharing.
//! Only [`AdminClient`](crate::admin::AdminClient) got this right initially;
//! its four siblings called `close_all()` unconditionally until this release.
//!
//! # One transport for all of them
//!
//! A [`TransportConfig`](crate::network::TransportConfig) given to the
//! `KrafkaClient` applies to every attached client, since there is one pool.
//! That is the reliable way to guarantee a SOCKS5 route, a file-descriptor cap
//! or a KIP-1288 TLS reload interval covers the whole process — with separate
//! pools, one client left on the defaults quietly takes a different network
//! path.
//!
//! # Example
//!
//! ```rust,no_run
//! use krafka::client::KrafkaClient;
//! use krafka::producer::Producer;
//! use krafka::consumer::Consumer;
//!
//! # async fn example() -> Result<(), krafka::error::KrafkaError> {
//! let client = KrafkaClient::builder("localhost:9092")
//!     .build()
//!     .await?;
//!
//! // Both share the same five TCP connections.
//! let producer = Producer::builder()
//!     .with_client(&client)
//!     .build()
//!     .await?;
//!
//! let consumer = Consumer::builder()
//!     .with_client(&client)
//!     .group_id("my-group")
//!     .build()
//!     .await?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::auth::AuthConfig;
use crate::error::{KrafkaError, Result};
use crate::metadata::{ClusterMetadata, MetadataRecoveryStrategy};
use crate::network::{ConnectionConfig, ConnectionConfigBuilder, ConnectionPool};

/// Shared connection pool and metadata cache.
///
/// Construct with [`KrafkaClient::builder`] and pass to each client builder via
/// `.with_client(&client)`. The idle-connection evictor and (when configured)
/// the OAUTHBEARER proactive-refresh task are started once here and shared by
/// all attached clients.
///
/// The `KrafkaClient` is cheap to clone: all clones share the same `Arc`-wrapped
/// pool and metadata.
#[derive(Clone)]
pub struct KrafkaClient {
    pool: Arc<ConnectionPool>,
    metadata: Arc<ClusterMetadata>,
}

impl KrafkaClient {
    /// Create a new builder for `bootstrap_servers`.
    ///
    /// `bootstrap_servers` must be a comma-separated list of `host:port` pairs,
    /// e.g. `"broker1:9092,broker2:9092"`.
    pub fn builder(bootstrap_servers: impl Into<String>) -> KrafkaClientBuilder {
        KrafkaClientBuilder {
            bootstrap_servers: bootstrap_servers.into(),
            client_id: "krafka".to_string(),
            auth: None,
            request_timeout: Duration::from_secs(30),
            connect_timeout: crate::network::DEFAULT_CONNECT_TIMEOUT,
            metadata_max_age: Duration::from_secs(300),
            metadata_recovery_strategy: MetadataRecoveryStrategy::Rebootstrap,
            metadata_recovery_rebootstrap_trigger: Duration::from_secs(300),
            metadata_topic_cache_ttl: Some(Duration::from_secs(300)),
            transport: crate::network::TransportConfig::default(),
            #[cfg(feature = "socks5")]
            proxy: None,
        }
    }

    /// Returns a reference to the shared connection pool.
    ///
    /// Prefer [`ProducerBuilder::with_client`](crate::producer::ProducerBuilder::with_client)
    /// over accessing the pool directly.
    pub fn pool(&self) -> &Arc<ConnectionPool> {
        &self.pool
    }

    /// Re-read TLS certificate and key files from disk and atomically install
    /// the new material for all **future** connections opened from this shared
    /// pool (KIP-1288).
    ///
    /// Existing TLS sessions are unaffected. Because a `KrafkaClient` owns the
    /// pool that its producers, consumers and admin clients share, one call
    /// here rotates certificates for all of them.
    ///
    /// No-op when TLS is not configured.
    ///
    /// # Errors
    ///
    /// Returns an error if the certificate or key files cannot be read or
    /// parsed; the previous material stays active.
    pub async fn refresh_tls(&self) -> Result<()> {
        self.pool.refresh_tls().await
    }

    /// Returns a reference to the shared metadata cache.
    ///
    /// Prefer [`ProducerBuilder::with_client`](crate::producer::ProducerBuilder::with_client)
    /// over accessing the metadata directly.
    pub fn metadata(&self) -> &Arc<ClusterMetadata> {
        &self.metadata
    }
}

/// Builder for [`KrafkaClient`].
///
/// Obtain via [`KrafkaClient::builder`].
#[must_use = "builders do nothing until .build().await is called"]
pub struct KrafkaClientBuilder {
    bootstrap_servers: String,
    client_id: String,
    auth: Option<AuthConfig>,
    request_timeout: Duration,
    connect_timeout: Duration,
    metadata_max_age: Duration,
    metadata_recovery_strategy: MetadataRecoveryStrategy,
    metadata_recovery_rebootstrap_trigger: Duration,
    metadata_topic_cache_ttl: Option<Duration>,
    transport: crate::network::TransportConfig,
    #[cfg(feature = "socks5")]
    proxy: Option<crate::network::ProxyConfig>,
}

impl KrafkaClientBuilder {
    /// Set the client ID sent in every Kafka request header.
    ///
    /// Default: `"krafka"`.
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.client_id = id.into();
        self
    }

    /// Set authentication configuration (TLS, SASL/PLAIN, SCRAM, MSK IAM,
    /// OAUTHBEARER …).
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Configure SASL/OAUTHBEARER with an async token provider.
    ///
    /// The provider is called on every new broker connection and is backed by
    /// the built-in caching/coalescing layer. A proactive background refresh
    /// task starts when `build()` completes.
    pub fn sasl_oauthbearer_provider(
        mut self,
        provider: impl crate::auth::OAuthBearerTokenProvider + 'static,
    ) -> Self {
        self.auth = Some(AuthConfig::sasl_oauthbearer_provider(provider));
        self
    }

    /// Set the per-request timeout for metadata and API-version checks.
    ///
    /// Default: 30 s. Must be at least
    /// [`connect_timeout`](Self::connect_timeout), whose default is 10 s — a
    /// request's clock covers establishing the connection it is sent over, so a
    /// shorter value would expire every request before the handshake could
    /// finish. To go below 10 s, lower `connect_timeout` as well; `build()`
    /// returns a config error otherwise.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set how long TCP establishment to one broker may take.
    ///
    /// Default: 10 s. This also acts as the floor on
    /// [`request_timeout`](Self::request_timeout), so lowering it is what makes
    /// a sub-10-second request timeout possible.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Set how long cluster metadata may be cached before an automatic
    /// background refresh.
    ///
    /// Default: 5 min.
    pub fn metadata_max_age(mut self, duration: Duration) -> Self {
        self.metadata_max_age = duration;
        self
    }

    /// Set the metadata recovery strategy for lost-cluster detection.
    ///
    /// Default: [`MetadataRecoveryStrategy::Rebootstrap`].
    pub fn metadata_recovery_strategy(mut self, strategy: MetadataRecoveryStrategy) -> Self {
        self.metadata_recovery_strategy = strategy;
        self
    }

    /// Set the idle duration after which, if no metadata refresh has succeeded,
    /// the client triggers a rebootstrap (when the strategy is `Rebootstrap`).
    ///
    /// Default: 5 min.
    pub fn metadata_recovery_rebootstrap_trigger(mut self, duration: Duration) -> Self {
        self.metadata_recovery_rebootstrap_trigger = duration;
        self
    }

    /// Set the per-topic TTL for partial metadata refreshes.
    ///
    /// Default: 5 min.
    pub fn metadata_topic_cache_ttl(mut self, ttl: Duration) -> Self {
        self.metadata_topic_cache_ttl = Some(ttl);
        self
    }

    /// Disable per-topic TTL eviction for partial metadata refreshes.
    ///
    /// Entries will then persist across partial refreshes indefinitely.
    pub fn disable_metadata_topic_cache_ttl(mut self) -> Self {
        self.metadata_topic_cache_ttl = None;
        self
    }

    /// Configure SASL/PLAIN authentication.
    ///
    /// # Errors
    ///
    /// Returns an error if the credentials contain bytes the SASL framing
    /// cannot carry.
    pub fn sasl_plain(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> crate::Result<Self> {
        self.auth = Some(AuthConfig::sasl_plain(username, password)?);
        Ok(self)
    }

    /// Configure SASL/SCRAM-SHA-256 authentication.
    pub fn sasl_scram_sha256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.auth = Some(AuthConfig::sasl_scram_sha256(username, password));
        self
    }

    /// Configure SASL/SCRAM-SHA-512 authentication.
    pub fn sasl_scram_sha512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.auth = Some(AuthConfig::sasl_scram_sha512(username, password));
        self
    }

    /// Configure SASL/OAUTHBEARER with a static token.
    ///
    /// For a token that must be refreshed, use
    /// [`auth`](Self::auth) with
    /// [`AuthConfig::sasl_oauthbearer_provider`](crate::auth::AuthConfig::sasl_oauthbearer_provider),
    /// or the built-in OIDC provider behind the `oauth-oidc` feature.
    pub fn sasl_oauthbearer(mut self, token: impl Into<String>) -> Self {
        self.auth = Some(AuthConfig::sasl_oauthbearer(token));
        self
    }

    /// Configure a SOCKS5 proxy for all broker connections.
    #[cfg(feature = "socks5")]
    #[cfg_attr(docsrs, doc(cfg(feature = "socks5")))]
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.proxy = Some(proxy);
        self
    }

    /// Set socket- and pool-level transport tuning.
    ///
    /// Covers TCP keepalive and nodelay, the per-connection response ceiling
    /// and in-flight cap, the priority-channel depths, the Happy Eyeballs
    /// stagger, idle-connection eviction, a total-connection cap, and the
    /// KIP-1288 automatic TLS reload interval.
    ///
    /// Omitting this call keeps krafka's historical defaults, which
    /// [`TransportConfig::default`](crate::network::TransportConfig) reproduces
    /// exactly.
    pub fn transport(mut self, transport: crate::network::TransportConfig) -> Self {
        self.transport = transport;
        self
    }

    /// Build the shared client, establish the initial metadata fetch, and
    /// start background tasks (idle evictor, OAUTHBEARER token refresh).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `bootstrap_servers` is empty
    /// - TLS initialisation fails
    /// - The initial metadata fetch fails
    pub async fn build(self) -> Result<KrafkaClient> {
        if self.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap_servers is required"));
        }

        let mut pool_config_builder: ConnectionConfigBuilder = self.transport.apply(
            ConnectionConfig::builder()
                .client_id(&self.client_id)
                .request_timeout(self.request_timeout)
                .connect_timeout(self.connect_timeout),
        );

        if let Some(ref auth) = self.auth {
            pool_config_builder = pool_config_builder.auth(auth.clone());
        }

        #[cfg(feature = "socks5")]
        if let Some(ref proxy) = self.proxy {
            pool_config_builder = pool_config_builder.proxy(proxy.clone());
        }

        let mut pool_config = pool_config_builder.build()?;
        pool_config.init_tls().await?;

        // Every client builds its pool through `TransportConfig::build_pool`,
        // which applies the pool-level settings and starts the background
        // tasks (idle eviction, OAUTHBEARER refresh, KIP-1288 TLS reload).
        // Routing all construction sites through one function is what stops
        // them drifting apart again.
        let pool = self.transport.build_pool(pool_config);

        let bootstrap_servers = crate::util::parse_bootstrap_servers(&self.bootstrap_servers)?;

        let mut meta = ClusterMetadata::new(bootstrap_servers, pool.clone(), self.metadata_max_age)
            .with_recovery_strategy(self.metadata_recovery_strategy)
            .with_rebootstrap_trigger(self.metadata_recovery_rebootstrap_trigger);
        meta = match self.metadata_topic_cache_ttl {
            Some(ttl) => meta.with_topic_cache_ttl(ttl),
            None => meta.with_topic_cache_ttl_disabled(),
        };
        let metadata = Arc::new(meta);

        metadata.refresh().await?;

        info!(
            bootstrap_servers = %self.bootstrap_servers,
            brokers = metadata.brokers().len(),
            "KrafkaClient initialized"
        );

        Ok(KrafkaClient { pool, metadata })
    }
}
