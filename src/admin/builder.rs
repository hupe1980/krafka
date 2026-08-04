//! Builder for [`AdminClient`].

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::auth::AuthConfig;
use crate::error::{KrafkaError, Result};
use crate::metadata::ClusterMetadata;
use crate::network::{ConnectionConfig, ConnectionPool};

use super::{AdminClient, AdminConfig};

/// Builder for AdminClient.
#[must_use = "builders do nothing until .build() is called"]
#[derive(Default)]
pub struct AdminClientBuilder {
    config: AdminConfig,
    /// Pre-built pool and metadata from a [`KrafkaClient`](crate::client::KrafkaClient).
    shared: Option<(Arc<ConnectionPool>, Arc<ClusterMetadata>)>,
}

impl AdminClientBuilder {
    /// Set bootstrap servers.
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    /// Set client ID.
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.config.client_id = id.into();
        self
    }

    /// Set request timeout: how long one in-flight request may wait for its
    /// response. Default: 30 s.
    ///
    /// Must be at least [`connect_timeout`](Self::connect_timeout), whose
    /// default is 10 s — a request's clock covers establishing the connection
    /// it is sent over, so a shorter value would expire every request before
    /// the handshake could finish. To go below 10 s, lower `connect_timeout`
    /// as well; `build()` returns a config error otherwise.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set the connect timeout: how long TCP establishment to one broker may
    /// take. Default: 10 s.
    ///
    /// This also acts as the floor on
    /// [`request_timeout`](Self::request_timeout), so lowering it is what makes
    /// a sub-10-second request timeout possible.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    /// Set authentication configuration.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use krafka::admin::AdminClient;
    /// use krafka::auth::AuthConfig;
    ///
    /// let client = AdminClient::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .auth(AuthConfig::sasl_plain("user", "password")?)
    ///     .build()
    ///     .await?;
    /// ```
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = Some(auth);
        self
    }

    /// Set the metadata recovery strategy (KIP-1102).
    ///
    /// Controls what the client does when every known broker becomes
    /// unreachable: keep retrying the cached broker set, or fall back to the
    /// original bootstrap servers.
    pub fn metadata_recovery_strategy(
        mut self,
        strategy: crate::metadata::MetadataRecoveryStrategy,
    ) -> Self {
        self.config.metadata_recovery_strategy = strategy;
        self
    }

    /// How long metadata must stay unrefreshable before a rebootstrap fires.
    ///
    /// Only effective with
    /// [`MetadataRecoveryStrategy::Rebootstrap`](crate::metadata::MetadataRecoveryStrategy::Rebootstrap).
    pub fn metadata_recovery_rebootstrap_trigger(mut self, duration: Duration) -> Self {
        self.config.metadata_recovery_rebootstrap_trigger = duration;
        self
    }

    /// Set SOCKS5 proxy configuration.
    ///
    /// Routes all broker connections through the specified SOCKS5 proxy.
    #[cfg(feature = "socks5")]
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.config.proxy = Some(proxy);
        self
    }

    /// Set the maximum age of cached cluster metadata before a refresh.
    ///
    /// Default: 5 min, matching the other clients.
    pub fn metadata_max_age(mut self, age: Duration) -> Self {
        self.config.metadata_max_age = age;
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
        self.config.transport = transport;
        self
    }

    /// Configure SASL/PLAIN authentication.
    pub fn sasl_plain(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> crate::Result<Self> {
        self.config.auth = Some(AuthConfig::sasl_plain(username, password)?);
        Ok(self)
    }

    /// Configure SASL/SCRAM-SHA-256 authentication.
    pub fn sasl_scram_sha256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_scram_sha256(username, password));
        self
    }

    /// Configure SASL/SCRAM-SHA-512 authentication.
    pub fn sasl_scram_sha512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_scram_sha512(username, password));
        self
    }

    /// Configure SASL/OAUTHBEARER authentication with a static token.
    ///
    /// For automatic token refresh, use [`sasl_oauthbearer_provider()`](Self::sasl_oauthbearer_provider).
    /// For SASL extensions, use `.auth(AuthConfig::sasl_oauthbearer_token(...))`.
    pub fn sasl_oauthbearer(mut self, token: impl Into<String>) -> Self {
        self.config.auth = Some(AuthConfig::sasl_oauthbearer(token));
        self
    }

    /// Configure SASL/OAUTHBEARER authentication with an async token provider.
    ///
    /// The provider is called on every new broker connection, ensuring
    /// tokens are always fresh.
    pub fn sasl_oauthbearer_provider(
        mut self,
        provider: impl crate::auth::OAuthBearerTokenProvider + 'static,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_oauthbearer_provider(provider));
        self
    }

    /// Share a [`KrafkaClient`](crate::client::KrafkaClient)'s connection pool
    /// and metadata cache instead of creating a new one.
    ///
    /// When this method is called, `bootstrap_servers` is optional on the
    /// builder (the client was already connected at `KrafkaClient::build` time).
    pub fn with_client(mut self, client: &crate::client::KrafkaClient) -> Self {
        self.shared = Some((client.pool().clone(), client.metadata().clone()));
        self
    }
    /// Validate the configuration and return it, without connecting.
    ///
    /// Runs the same checks [`build`](Self::build) runs before it opens a
    /// socket, so a config that passes here will not be rejected later for a
    /// configuration reason.
    ///
    /// # Errors
    ///
    /// Returns [`KrafkaError::Config`](crate::error::KrafkaError::Config) when
    /// `bootstrap_servers` is empty and no shared client was supplied.
    pub fn build_config(self) -> Result<AdminConfig> {
        if self.shared.is_none() && self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap.servers is required"));
        }
        Ok(self.config)
    }

    /// Build the admin client.
    pub async fn build(self) -> Result<AdminClient> {
        if self.shared.is_none() && self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap.servers is required"));
        }

        // `pool_owned` decides whether `AdminClient::close()` may tear the pool
        // down. A pool borrowed from a `KrafkaClient` is shared with that
        // client's producers and consumers and must survive admin close.
        let pool_owned = self.shared.is_none();

        let (pool, metadata) = if let Some((pool, metadata)) = self.shared {
            // Use the pre-built shared pool and metadata from a KrafkaClient.
            (pool, metadata)
        } else {
            let bootstrap_servers =
                crate::util::parse_bootstrap_servers(&self.config.bootstrap_servers)?;

            // Create connection config with client ID and auth
            let mut conn_config_builder = self.config.transport.apply(
                ConnectionConfig::builder()
                    .client_id(&self.config.client_id)
                    .request_timeout(self.config.request_timeout)
                    .connect_timeout(self.config.connect_timeout),
            );

            if let Some(ref auth) = self.config.auth {
                conn_config_builder = conn_config_builder.auth(auth.clone());
            }

            #[cfg(feature = "socks5")]
            if let Some(ref proxy) = self.config.proxy {
                conn_config_builder = conn_config_builder.proxy(proxy.clone());
            }

            let mut conn_config = conn_config_builder.build()?;
            conn_config.init_tls().await?;

            // Every client builds its pool through `TransportConfig::build_pool`,
            // which applies the pool-level settings and starts the background
            // tasks (idle eviction, OAUTHBEARER refresh, KIP-1288 TLS reload).
            // Routing all construction sites through one function is what stops
            // them drifting apart again.
            let pool = self.config.transport.build_pool(conn_config);
            let metadata = Arc::new(
                ClusterMetadata::new(
                    bootstrap_servers,
                    pool.clone(),
                    self.config.metadata_max_age,
                )
                .with_recovery_strategy(self.config.metadata_recovery_strategy)
                .with_rebootstrap_trigger(self.config.metadata_recovery_rebootstrap_trigger)
                // Bound waits on an in-flight refresh by the request
                // timeout rather than the 300 s metadata max-age.
                .with_request_timeout(self.config.request_timeout),
            );

            metadata.refresh().await?;

            (pool, metadata)
        };

        info!(
            "AdminClient initialized with auth: {}",
            if self.config.auth.is_some() {
                "configured"
            } else {
                "none"
            }
        );

        Ok(AdminClient {
            config: self.config,
            metadata,
            pool,
            pool_owned,
            closed: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::super::AdminClient;

    #[test]
    fn test_admin_builder_with_auth() {
        use crate::auth::AuthConfig;

        let builder = AdminClient::builder()
            .bootstrap_servers("broker:9093")
            .auth(AuthConfig::sasl_plain("user", "pass").unwrap());

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(!auth.requires_tls());
        assert!(auth.plain_credentials.is_some());
    }

    #[test]
    fn test_admin_builder_sasl_plain() {
        let builder = AdminClient::builder()
            .bootstrap_servers("broker:9093")
            .sasl_plain("admin", "admin-secret")
            .unwrap();

        let auth = builder.config.auth.as_ref().unwrap();
        assert_eq!(
            auth.security_protocol,
            crate::auth::SecurityProtocol::SaslPlaintext
        );
        assert_eq!(auth.sasl_mechanism, Some(crate::auth::SaslMechanism::Plain));
        let creds = auth.plain_credentials.as_ref().unwrap();
        assert_eq!(creds.username, "admin");
    }

    #[test]
    fn test_admin_builder_sasl_scram() {
        let builder = AdminClient::builder()
            .bootstrap_servers("broker:9093")
            .sasl_scram_sha256("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert_eq!(
            auth.sasl_mechanism,
            Some(crate::auth::SaslMechanism::ScramSha256)
        );
        assert!(auth.scram_credentials.is_some());

        let builder = AdminClient::builder()
            .bootstrap_servers("broker:9093")
            .sasl_scram_sha512("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert_eq!(
            auth.sasl_mechanism,
            Some(crate::auth::SaslMechanism::ScramSha512)
        );
        assert!(auth.scram_credentials.is_some());
    }

    #[test]
    fn test_admin_builder_aws_msk_iam() {
        use crate::auth::AuthConfig;

        let auth = AuthConfig::aws_msk_iam("AKID", "secret", "us-east-1");
        let builder = AdminClient::builder()
            .bootstrap_servers("broker:9094")
            .auth(auth);

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_tls());
        assert!(auth.requires_sasl());
        assert_eq!(
            auth.sasl_mechanism,
            Some(crate::auth::SaslMechanism::AwsMskIam)
        );
        assert!(auth.aws_msk_iam_credentials.is_some());
        assert!(auth.tls_config.is_some());
    }

    #[test]
    fn test_admin_builder_no_auth_by_default() {
        let builder = AdminClient::builder().bootstrap_servers("broker:9092");

        assert!(builder.config.auth.is_none());
    }
}
