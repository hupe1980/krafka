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

    /// Set request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
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

    /// Set SOCKS5 proxy configuration.
    ///
    /// Routes all broker connections through the specified SOCKS5 proxy.
    #[cfg(feature = "socks5")]
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.config.proxy = Some(proxy);
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

    /// Build the admin client.
    pub async fn build(self) -> Result<AdminClient> {
        if self.shared.is_none() && self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap.servers is required"));
        }

        let (pool, metadata) = if let Some((pool, metadata)) = self.shared {
            // Use the pre-built shared pool and metadata from a KrafkaClient.
            (pool, metadata)
        } else {
            let bootstrap_servers =
                crate::util::parse_bootstrap_servers(&self.config.bootstrap_servers)?;

            // Create connection config with client ID and auth
            let mut conn_config_builder = ConnectionConfig::builder()
                .client_id(&self.config.client_id)
                .request_timeout(self.config.request_timeout);

            if let Some(ref auth) = self.config.auth {
                conn_config_builder = conn_config_builder.auth(auth.clone());
            }

            #[cfg(feature = "socks5")]
            if let Some(ref proxy) = self.config.proxy {
                conn_config_builder = conn_config_builder.proxy(proxy.clone());
            }

            let mut conn_config = conn_config_builder.build()?;
            conn_config.init_tls().await?;

            let pool = Arc::new(ConnectionPool::new(conn_config));
            pool.start_idle_evictor();
            let metadata = Arc::new(
                ClusterMetadata::new(bootstrap_servers, pool.clone(), Duration::from_secs(300))
                    .with_recovery_strategy(self.config.metadata_recovery_strategy)
                    .with_rebootstrap_trigger(self.config.metadata_recovery_rebootstrap_trigger),
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
