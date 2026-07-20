//! Shared transport for connection pooling and metadata across multiple clients.
//!
//! By default every [`Producer`](crate::producer::Producer),
//! [`Consumer`](crate::consumer::Consumer), and
//! [`AdminClient`](crate::admin::AdminClient) creates its own TCP connection
//! pool and metadata cache. An application that runs one producer and two
//! consumers against a 5-broker cluster therefore opens **15** TCP
//! connections.
//!
//! A [`KrafkaClient`] holds a single shared pool and a single shared metadata
//! cache. Passing one to each builder via
//! [`.with_client()`](crate::producer::ProducerBuilder::with_client) reduces
//! the connection count to **5** regardless of how many client objects are
//! created.
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

    /// Configure a SOCKS5 proxy for all broker connections.
    #[cfg(feature = "socks5")]
    #[cfg_attr(docsrs, doc(cfg(feature = "socks5")))]
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.proxy = Some(proxy);
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
            return Err(KrafkaError::config("bootstrap_servers must not be empty"));
        }

        let mut pool_config_builder: ConnectionConfigBuilder = ConnectionConfig::builder()
            .client_id(&self.client_id)
            .request_timeout(self.request_timeout)
            .connect_timeout(self.connect_timeout);

        if let Some(ref auth) = self.auth {
            pool_config_builder = pool_config_builder.auth(auth.clone());
        }

        #[cfg(feature = "socks5")]
        if let Some(ref proxy) = self.proxy {
            pool_config_builder = pool_config_builder.proxy(proxy.clone());
        }

        let mut pool_config = pool_config_builder.build()?;
        pool_config.init_tls().await?;

        let pool = Arc::new(ConnectionPool::new(pool_config));
        pool.start_idle_evictor();

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
