//! Authentication for Kafka connections.
//!
//! This module provides:
//! - PLAINTEXT (no auth)
//! - TLS/SSL support with rustls
//! - SASL/PLAIN
//! - SASL/SCRAM-SHA-256 and SASL/SCRAM-SHA-512
//! - SASL/AWS_MSK_IAM for AWS MSK
//! - SASL/OAUTHBEARER (RFC 7628 / KIP-255)
//!
//! # Security Note
//!
//! All credential types in this module use memory zeroization on drop to prevent
//! sensitive data from remaining in memory after use.

pub mod msk_iam;
pub mod oauthbearer;
pub mod scram;
pub mod tls;

pub use msk_iam::MskIamAuthenticator;
pub use oauthbearer::{OAuthBearerToken, OAuthBearerTokenProvider, OAuthBearerTokenProviderHandle};
pub use scram::{ScramClient, ScramMechanism, ScramState};
pub use tls::{MaybeSecureStream, build_tls_config, connect_tls};

use std::fmt;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

/// Security protocol for Kafka connections.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SecurityProtocol {
    /// No encryption or authentication.
    #[default]
    Plaintext,
    /// TLS encryption without SASL.
    Ssl,
    /// SASL authentication without encryption.
    SaslPlaintext,
    /// SASL authentication with TLS encryption.
    SaslSsl,
}

impl fmt::Display for SecurityProtocol {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SecurityProtocol::Plaintext => write!(f, "PLAINTEXT"),
            SecurityProtocol::Ssl => write!(f, "SSL"),
            SecurityProtocol::SaslPlaintext => write!(f, "SASL_PLAINTEXT"),
            SecurityProtocol::SaslSsl => write!(f, "SASL_SSL"),
        }
    }
}

/// SASL mechanism for authentication.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SaslMechanism {
    /// PLAIN authentication (username/password).
    Plain,
    /// SCRAM-SHA-256 authentication.
    ScramSha256,
    /// SCRAM-SHA-512 authentication.
    ScramSha512,
    /// AWS MSK IAM authentication.
    AwsMskIam,
    /// OAuth Bearer token authentication.
    OAuthBearer,
    /// GSSAPI (Kerberos) authentication.
    Gssapi,
}

impl fmt::Display for SaslMechanism {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SaslMechanism::Plain => write!(f, "PLAIN"),
            SaslMechanism::ScramSha256 => write!(f, "SCRAM-SHA-256"),
            SaslMechanism::ScramSha512 => write!(f, "SCRAM-SHA-512"),
            SaslMechanism::AwsMskIam => write!(f, "AWS_MSK_IAM"),
            SaslMechanism::OAuthBearer => write!(f, "OAUTHBEARER"),
            SaslMechanism::Gssapi => write!(f, "GSSAPI"),
        }
    }
}

/// SASL PLAIN credentials.
///
/// Password is automatically zeroized on drop for security.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct PlainCredentials {
    /// Username.
    pub username: String,
    /// Password (zeroized on drop).
    pub password: String,
}

impl PlainCredentials {
    /// Create new PLAIN credentials.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }

    /// Build the SASL PLAIN authentication message.
    ///
    /// The returned `Zeroizing<Vec<u8>>` is automatically zeroized on drop
    /// to prevent the password from lingering in freed heap memory.
    pub fn to_auth_bytes(&self) -> Zeroizing<Vec<u8>> {
        // SASL PLAIN format: \0username\0password
        let mut auth = Vec::new();
        auth.push(0);
        auth.extend_from_slice(self.username.as_bytes());
        auth.push(0);
        auth.extend_from_slice(self.password.as_bytes());
        Zeroizing::new(auth)
    }
}

impl fmt::Debug for PlainCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlainCredentials")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

/// SCRAM credentials for SCRAM-SHA-256 or SCRAM-SHA-512.
///
/// Password is automatically zeroized on drop for security.
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct ScramCredentials {
    /// Username.
    pub username: String,
    /// Password (zeroized on drop).
    pub password: String,
}

impl ScramCredentials {
    /// Create new SCRAM credentials.
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

impl fmt::Debug for ScramCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScramCredentials")
            .field("username", &self.username)
            .field("password", &"[REDACTED]")
            .finish()
    }
}

/// AWS MSK IAM credentials.
///
/// Secret access key and session token are automatically zeroized on drop for security.
#[non_exhaustive]
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct AwsMskIamCredentials {
    /// AWS access key ID.
    pub access_key_id: String,
    /// AWS secret access key (zeroized on drop).
    pub secret_access_key: String,
    /// AWS session token (for temporary credentials, zeroized on drop).
    pub session_token: Option<String>,
    /// AWS region.
    pub region: String,
}

impl AwsMskIamCredentials {
    /// Create new AWS MSK IAM credentials.
    pub fn new(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token: None,
            region: region.into(),
        }
    }

    /// Create with session token (for temporary credentials).
    pub fn with_session_token(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        session_token: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            access_key_id: access_key_id.into(),
            secret_access_key: secret_access_key.into(),
            session_token: Some(session_token.into()),
            region: region.into(),
        }
    }

    /// Create credentials from environment variables.
    ///
    /// Reads from:
    /// - `AWS_ACCESS_KEY_ID` - Required
    /// - `AWS_SECRET_ACCESS_KEY` - Required    
    /// - `AWS_SESSION_TOKEN` - Optional (for temporary credentials)
    /// - `AWS_REGION` or `AWS_DEFAULT_REGION` - Required
    ///
    /// # Errors
    ///
    /// Returns error if required environment variables are not set.
    pub fn from_env() -> crate::error::Result<Self> {
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").map_err(|_| {
            crate::error::KrafkaError::config("AWS_ACCESS_KEY_ID environment variable not set")
        })?;

        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").map_err(|_| {
            crate::error::KrafkaError::config("AWS_SECRET_ACCESS_KEY environment variable not set")
        })?;

        let session_token = std::env::var("AWS_SESSION_TOKEN").ok();

        let region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .map_err(|_| {
                crate::error::KrafkaError::config(
                    "AWS_REGION or AWS_DEFAULT_REGION environment variable not set",
                )
            })?;

        Ok(Self {
            access_key_id,
            secret_access_key,
            session_token,
            region,
        })
    }

    /// Create credentials from the AWS SDK default credential chain.
    ///
    /// This loads credentials from (in order):
    /// 1. Environment variables
    /// 2. Shared credentials file (~/.aws/credentials)
    /// 3. IAM role for EC2/ECS/Lambda
    /// 4. Web identity token (for EKS)
    ///
    /// Requires the `aws-msk` feature.
    ///
    /// # Errors
    ///
    /// Returns error if credentials cannot be loaded from any source.
    #[cfg(feature = "aws-msk")]
    pub async fn from_default_chain(region: impl Into<String>) -> crate::error::Result<Self> {
        use aws_config::BehaviorVersion;
        use aws_credential_types::provider::ProvideCredentials;

        let region_str = region.into();
        let region = aws_config::Region::new(region_str.clone());

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(region)
            .load()
            .await;

        let credentials_provider = config.credentials_provider().ok_or_else(|| {
            crate::error::KrafkaError::config("No credentials provider available in AWS config")
        })?;

        let credentials = credentials_provider
            .provide_credentials()
            .await
            .map_err(|e| {
                crate::error::KrafkaError::config(format!("Failed to load AWS credentials: {e}"))
            })?;

        Ok(Self {
            access_key_id: credentials.access_key_id().to_string(),
            secret_access_key: credentials.secret_access_key().to_string(),
            session_token: credentials.session_token().map(|s| s.to_string()),
            region: region_str,
        })
    }
}

impl fmt::Debug for AwsMskIamCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AwsMskIamCredentials")
            .field("access_key_id", &self.access_key_id)
            .field("secret_access_key", &"[REDACTED]")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("region", &self.region)
            .finish()
    }
}

/// TLS configuration.
///
/// Use [`TlsConfig::new()`] or [`Default::default()`] to construct.
/// For insecure mode, enable the `danger-insecure-tls` feature and use
/// `TlsConfig::insecure()`.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// Path to CA certificate file.
    pub(crate) ca_cert_path: Option<String>,
    /// Path to client certificate file.
    pub(crate) client_cert_path: Option<String>,
    /// Path to client private key file.
    pub(crate) client_key_path: Option<String>,
    /// Whether to load root certificates from the platform trust store.
    pub(crate) use_native_roots: bool,
    /// Whether to verify server certificates (defaults to `true`).
    pub(crate) verify_server_cert: bool,
    /// Server name indication (SNI) hostname.
    pub(crate) sni_hostname: Option<String>,
}

impl Default for TlsConfig {
    /// Returns a secure default: certificate verification enabled.
    fn default() -> Self {
        Self {
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            use_native_roots: false,
            verify_server_cert: true,
            sni_hostname: None,
        }
    }
}

impl TlsConfig {
    /// Create a new TLS config that verifies server certificates.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a TLS config for self-signed certificates.
    ///
    /// Create a TLS config that skips server certificate verification.
    ///
    /// **Warning:** This disables TLS security entirely. Use only for local
    /// development or testing with self-signed certificates. For production
    /// use with self-signed certs, prefer [`with_ca_cert()`](Self::with_ca_cert)
    /// to supply the CA certificate explicitly.
    ///
    /// Requires the `danger-insecure-tls` crate feature.
    #[cfg(feature = "danger-insecure-tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "danger-insecure-tls")))]
    pub fn insecure() -> Self {
        Self {
            verify_server_cert: false,
            ..Default::default()
        }
    }

    /// Set the CA certificate path.
    pub fn with_ca_cert(mut self, path: impl Into<String>) -> Self {
        self.ca_cert_path = Some(path.into());
        self
    }

    /// Load root certificates from the platform trust store.
    ///
    /// Requires the `native-tls-roots` crate feature. When enabled, the native
    /// trust anchors are used as the base trust store and any CA certificate
    /// configured via [`with_ca_cert()`](Self::with_ca_cert) is added on top.
    pub fn with_native_roots(mut self) -> Self {
        self.use_native_roots = true;
        self
    }

    /// Set client certificate and key paths.
    pub fn with_client_cert(
        mut self,
        cert_path: impl Into<String>,
        key_path: impl Into<String>,
    ) -> Self {
        self.client_cert_path = Some(cert_path.into());
        self.client_key_path = Some(key_path.into());
        self
    }

    /// Set the SNI hostname.
    pub fn with_sni_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.sni_hostname = Some(hostname.into());
        self
    }

    /// Returns the CA certificate path, if set.
    pub fn ca_cert_path(&self) -> Option<&str> {
        self.ca_cert_path.as_deref()
    }

    /// Returns the client certificate path, if set.
    pub fn client_cert_path(&self) -> Option<&str> {
        self.client_cert_path.as_deref()
    }

    /// Returns the client key path, if set.
    pub fn client_key_path(&self) -> Option<&str> {
        self.client_key_path.as_deref()
    }

    /// Returns whether platform-native root certificates are enabled.
    pub fn use_native_roots(&self) -> bool {
        self.use_native_roots
    }

    /// Returns whether server certificates are verified.
    pub fn verify_server_cert(&self) -> bool {
        self.verify_server_cert
    }

    /// Returns the SNI hostname, if set.
    pub fn sni_hostname(&self) -> Option<&str> {
        self.sni_hostname.as_deref()
    }
}

/// Complete authentication configuration.
///
/// Use factory methods like [`AuthConfig::plaintext()`], [`AuthConfig::ssl()`],
/// [`AuthConfig::sasl_plain()`], etc. to construct.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// Security protocol.
    pub(crate) security_protocol: SecurityProtocol,
    /// SASL mechanism (if using SASL).
    pub(crate) sasl_mechanism: Option<SaslMechanism>,
    /// SASL PLAIN credentials.
    pub(crate) plain_credentials: Option<PlainCredentials>,
    /// SASL SCRAM credentials.
    pub(crate) scram_credentials: Option<ScramCredentials>,
    /// AWS MSK IAM credentials.
    pub(crate) aws_msk_iam_credentials: Option<AwsMskIamCredentials>,
    /// OAUTHBEARER token.
    pub(crate) oauthbearer_token: Option<OAuthBearerToken>,
    /// OAUTHBEARER token provider for automatic token refresh.
    pub(crate) oauthbearer_provider: Option<OAuthBearerTokenProviderHandle>,
    /// TLS configuration.
    pub(crate) tls_config: Option<TlsConfig>,
}

impl AuthConfig {
    /// Create a plaintext (no auth) configuration.
    pub fn plaintext() -> Self {
        Self {
            security_protocol: SecurityProtocol::Plaintext,
            ..Default::default()
        }
    }

    /// Create a TLS-only configuration.
    pub fn ssl(tls_config: TlsConfig) -> Self {
        Self {
            security_protocol: SecurityProtocol::Ssl,
            tls_config: Some(tls_config),
            ..Default::default()
        }
    }

    /// Create a SASL/PLAIN configuration.
    pub fn sasl_plain(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::Plain),
            plain_credentials: Some(PlainCredentials::new(username, password)),
            ..Default::default()
        }
    }

    /// Create a SASL/PLAIN over TLS configuration.
    pub fn sasl_plain_ssl(
        username: impl Into<String>,
        password: impl Into<String>,
        tls_config: TlsConfig,
    ) -> Self {
        Self {
            security_protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::Plain),
            plain_credentials: Some(PlainCredentials::new(username, password)),
            tls_config: Some(tls_config),
            ..Default::default()
        }
    }

    /// Create a SASL/SCRAM-SHA-256 configuration.
    pub fn sasl_scram_sha256(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::ScramSha256),
            scram_credentials: Some(ScramCredentials::new(username, password)),
            ..Default::default()
        }
    }

    /// Create a SASL/SCRAM-SHA-512 configuration.
    pub fn sasl_scram_sha512(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::ScramSha512),
            scram_credentials: Some(ScramCredentials::new(username, password)),
            ..Default::default()
        }
    }

    /// Create an AWS MSK IAM configuration.
    pub fn aws_msk_iam(
        access_key_id: impl Into<String>,
        secret_access_key: impl Into<String>,
        region: impl Into<String>,
    ) -> Self {
        Self {
            security_protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::AwsMskIam),
            aws_msk_iam_credentials: Some(AwsMskIamCredentials::new(
                access_key_id,
                secret_access_key,
                region,
            )),
            tls_config: Some(TlsConfig::new()),
            ..Default::default()
        }
    }

    /// Create an AWS MSK IAM configuration with pre-loaded credentials.
    ///
    /// Use this with `AwsMskIamCredentials::from_env()` or
    /// `AwsMskIamCredentials::from_default_chain()`.
    pub fn aws_msk_iam_with_credentials(credentials: AwsMskIamCredentials) -> Self {
        Self {
            security_protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::AwsMskIam),
            aws_msk_iam_credentials: Some(credentials),
            tls_config: Some(TlsConfig::new()),
            ..Default::default()
        }
    }

    /// Create a SASL/OAUTHBEARER configuration with a static token.
    ///
    /// Uses SASL_PLAINTEXT. For TLS, use [`sasl_oauthbearer_ssl()`](Self::sasl_oauthbearer_ssl).
    /// For automatic token refresh on reconnection, use
    /// [`sasl_oauthbearer_provider()`](Self::sasl_oauthbearer_provider) instead.
    ///
    /// # Example
    ///
    /// ```rust
    /// use krafka::auth::AuthConfig;
    /// let config = AuthConfig::sasl_oauthbearer("my-jwt-token");
    /// ```
    pub fn sasl_oauthbearer(token: impl Into<String>) -> Self {
        Self {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            oauthbearer_token: Some(OAuthBearerToken::new(token)),
            ..Default::default()
        }
    }

    /// Create a SASL/OAUTHBEARER over TLS configuration.
    pub fn sasl_oauthbearer_ssl(token: impl Into<String>, tls_config: TlsConfig) -> Self {
        Self {
            security_protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            oauthbearer_token: Some(OAuthBearerToken::new(token)),
            tls_config: Some(tls_config),
            ..Default::default()
        }
    }

    /// Create a SASL/OAUTHBEARER configuration with a pre-built token.
    ///
    /// Use this when you need SASL extensions (e.g., for Confluent Cloud).
    ///
    /// # Example
    ///
    /// ```rust
    /// use krafka::auth::{AuthConfig, OAuthBearerToken};
    /// let token = OAuthBearerToken::new("my-jwt-token")
    ///     .with_extension("logicalCluster", "lkc-abc123");
    /// let config = AuthConfig::sasl_oauthbearer_token(token);
    /// ```
    pub fn sasl_oauthbearer_token(token: OAuthBearerToken) -> Self {
        Self {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            oauthbearer_token: Some(token),
            ..Default::default()
        }
    }

    /// Create a SASL/OAUTHBEARER over TLS configuration with a pre-built token.
    pub fn sasl_oauthbearer_token_ssl(token: OAuthBearerToken, tls_config: TlsConfig) -> Self {
        Self {
            security_protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            oauthbearer_token: Some(token),
            tls_config: Some(tls_config),
            ..Default::default()
        }
    }

    /// Create a SASL/OAUTHBEARER configuration with an async token provider.
    ///
    /// The provider is called on every new broker connection (including
    /// automatic reconnections), so tokens are always fresh.
    ///
    /// # Example
    ///
    /// ```rust
    /// use krafka::auth::{AuthConfig, OAuthBearerToken};
    ///
    /// let config = AuthConfig::sasl_oauthbearer_provider(|| async {
    ///     // Fetch a fresh token from your OAuth server
    ///     Ok(OAuthBearerToken::new("fresh-jwt-token"))
    /// });
    /// ```
    pub fn sasl_oauthbearer_provider(provider: impl OAuthBearerTokenProvider + 'static) -> Self {
        Self {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            oauthbearer_provider: Some(OAuthBearerTokenProviderHandle::new(provider)),
            ..Default::default()
        }
    }

    /// Create a SASL/OAUTHBEARER over TLS configuration with an async token provider.
    pub fn sasl_oauthbearer_provider_ssl(
        provider: impl OAuthBearerTokenProvider + 'static,
        tls_config: TlsConfig,
    ) -> Self {
        Self {
            security_protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            oauthbearer_provider: Some(OAuthBearerTokenProviderHandle::new(provider)),
            tls_config: Some(tls_config),
            ..Default::default()
        }
    }

    /// If this config has an OAUTHBEARER provider, resolve a fresh token
    /// and return a new `AuthConfig` with the token set and the provider
    /// cleared. Returns `None` if no provider is configured (the caller
    /// should use `self` as-is).
    ///
    /// This must be called before passing a provider-based config to
    /// [`SaslAuthenticator::new()`](crate::network::SaslAuthenticator::new),
    /// which requires a resolved token.
    ///
    /// # Errors
    ///
    /// Returns an error if the provider fails to fetch a token.
    pub async fn resolve_provider_to_token(&self) -> crate::error::Result<Option<AuthConfig>> {
        if self.sasl_mechanism == Some(SaslMechanism::OAuthBearer)
            && let Some(ref provider) = self.oauthbearer_provider
        {
            let token = provider.provide_token().await?;
            Ok(Some(AuthConfig {
                oauthbearer_token: Some(token),
                oauthbearer_provider: None,
                ..self.clone()
            }))
        } else {
            Ok(None)
        }
    }

    /// Check if TLS is required.
    pub fn requires_tls(&self) -> bool {
        matches!(
            self.security_protocol,
            SecurityProtocol::Ssl | SecurityProtocol::SaslSsl
        )
    }

    /// Check if SASL is required.
    pub fn requires_sasl(&self) -> bool {
        matches!(
            self.security_protocol,
            SecurityProtocol::SaslPlaintext | SecurityProtocol::SaslSsl
        )
    }

    /// Returns the security protocol.
    pub fn security_protocol(&self) -> &SecurityProtocol {
        &self.security_protocol
    }

    /// Returns the SASL mechanism, if set.
    pub fn sasl_mechanism(&self) -> Option<&SaslMechanism> {
        self.sasl_mechanism.as_ref()
    }

    /// Returns the PLAIN credentials, if set.
    pub fn plain_credentials(&self) -> Option<&PlainCredentials> {
        self.plain_credentials.as_ref()
    }

    /// Returns the SCRAM credentials, if set.
    pub fn scram_credentials(&self) -> Option<&ScramCredentials> {
        self.scram_credentials.as_ref()
    }

    /// Returns the AWS MSK IAM credentials, if set.
    pub fn aws_msk_iam_credentials(&self) -> Option<&AwsMskIamCredentials> {
        self.aws_msk_iam_credentials.as_ref()
    }

    /// Returns the OAUTHBEARER token, if set.
    pub fn oauthbearer_token(&self) -> Option<&OAuthBearerToken> {
        self.oauthbearer_token.as_ref()
    }

    /// Returns the OAUTHBEARER token provider handle, if set.
    pub fn oauthbearer_provider(&self) -> Option<&OAuthBearerTokenProviderHandle> {
        self.oauthbearer_provider.as_ref()
    }

    /// Returns the TLS configuration, if set.
    pub fn tls_config(&self) -> Option<&TlsConfig> {
        self.tls_config.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_security_protocol_display() {
        assert_eq!(SecurityProtocol::Plaintext.to_string(), "PLAINTEXT");
        assert_eq!(SecurityProtocol::Ssl.to_string(), "SSL");
        assert_eq!(
            SecurityProtocol::SaslPlaintext.to_string(),
            "SASL_PLAINTEXT"
        );
        assert_eq!(SecurityProtocol::SaslSsl.to_string(), "SASL_SSL");
    }

    #[test]
    fn test_sasl_mechanism_display() {
        assert_eq!(SaslMechanism::Plain.to_string(), "PLAIN");
        assert_eq!(SaslMechanism::ScramSha256.to_string(), "SCRAM-SHA-256");
        assert_eq!(SaslMechanism::AwsMskIam.to_string(), "AWS_MSK_IAM");
    }

    #[test]
    fn test_plain_credentials() {
        let creds = PlainCredentials::new("user", "pass");
        let auth_bytes = creds.to_auth_bytes();
        assert_eq!(&*auth_bytes, b"\0user\0pass");
    }

    #[test]
    fn test_auth_config_plaintext() {
        let config = AuthConfig::plaintext();
        assert_eq!(config.security_protocol, SecurityProtocol::Plaintext);
        assert!(!config.requires_tls());
        assert!(!config.requires_sasl());
    }

    #[test]
    fn test_auth_config_sasl_plain() {
        let config = AuthConfig::sasl_plain("user", "pass");
        assert_eq!(config.security_protocol, SecurityProtocol::SaslPlaintext);
        assert_eq!(config.sasl_mechanism, Some(SaslMechanism::Plain));
        assert!(config.plain_credentials.is_some());
        assert!(!config.requires_tls());
        assert!(config.requires_sasl());
    }

    #[test]
    fn test_auth_config_aws_msk_iam() {
        let config = AuthConfig::aws_msk_iam("access_key", "secret_key", "us-east-1");
        assert_eq!(config.security_protocol, SecurityProtocol::SaslSsl);
        assert_eq!(config.sasl_mechanism, Some(SaslMechanism::AwsMskIam));
        assert!(config.aws_msk_iam_credentials.is_some());
        assert!(config.requires_tls());
        assert!(config.requires_sasl());
    }

    #[test]
    fn test_tls_config() {
        let config = TlsConfig::new()
            .with_ca_cert("/path/to/ca.pem")
            .with_client_cert("/path/to/client.pem", "/path/to/client.key")
            .with_native_roots();

        assert!(config.verify_server_cert);
        assert!(config.use_native_roots);
        assert_eq!(config.ca_cert_path, Some("/path/to/ca.pem".to_string()));
        assert_eq!(
            config.client_cert_path,
            Some("/path/to/client.pem".to_string())
        );
    }

    #[test]
    fn test_credentials_debug_redacts_password() {
        let creds = PlainCredentials::new("user", "secret");
        let debug_str = format!("{creds:?}");
        assert!(debug_str.contains("user"));
        assert!(debug_str.contains("[REDACTED]"));
        assert!(!debug_str.contains("secret"));
    }

    #[test]
    fn test_aws_msk_credentials_manual_creation() {
        let creds = AwsMskIamCredentials::new("AKID123", "secret123", "us-west-2");
        assert_eq!(creds.access_key_id, "AKID123");
        assert_eq!(creds.region, "us-west-2");
        assert!(creds.session_token.is_none());
    }

    #[test]
    fn test_aws_msk_credentials_with_session_token() {
        let creds = AwsMskIamCredentials::with_session_token(
            "AKID123",
            "secret123",
            "token123",
            "us-east-1",
        );
        assert_eq!(creds.access_key_id, "AKID123");
        assert_eq!(creds.session_token, Some("token123".to_string()));
    }

    #[test]
    fn test_aws_msk_credentials_debug_redacts() {
        let creds = AwsMskIamCredentials::new("AKID123", "supersecret", "us-east-1");
        let debug_str = format!("{creds:?}");
        assert!(debug_str.contains("AKID123"));
        assert!(debug_str.contains("[REDACTED]"));
        assert!(!debug_str.contains("supersecret"));
    }

    #[test]
    fn test_auth_config_sasl_oauthbearer() {
        let config = AuthConfig::sasl_oauthbearer("my-token");
        assert_eq!(config.security_protocol, SecurityProtocol::SaslPlaintext);
        assert_eq!(config.sasl_mechanism, Some(SaslMechanism::OAuthBearer));
        assert!(config.oauthbearer_token.is_some());
        assert!(!config.requires_tls());
        assert!(config.requires_sasl());
    }

    #[test]
    fn test_auth_config_sasl_oauthbearer_ssl() {
        let config = AuthConfig::sasl_oauthbearer_ssl("my-token", TlsConfig::new());
        assert_eq!(config.security_protocol, SecurityProtocol::SaslSsl);
        assert_eq!(config.sasl_mechanism, Some(SaslMechanism::OAuthBearer));
        assert!(config.oauthbearer_token.is_some());
        assert!(config.tls_config.is_some());
        assert!(config.requires_tls());
        assert!(config.requires_sasl());
    }

    #[test]
    fn test_auth_config_sasl_oauthbearer_token() {
        let token = OAuthBearerToken::new("jwt").with_extension("logicalCluster", "lkc-1");
        let config = AuthConfig::sasl_oauthbearer_token(token);
        assert_eq!(config.sasl_mechanism, Some(SaslMechanism::OAuthBearer));
        assert!(config.oauthbearer_token.is_some());
    }

    #[test]
    fn test_auth_config_sasl_oauthbearer_token_ssl() {
        let token = OAuthBearerToken::new("jwt");
        let config = AuthConfig::sasl_oauthbearer_token_ssl(token, TlsConfig::new());
        assert_eq!(config.security_protocol, SecurityProtocol::SaslSsl);
        assert_eq!(config.sasl_mechanism, Some(SaslMechanism::OAuthBearer));
        assert!(config.oauthbearer_token.is_some());
        assert!(config.tls_config.is_some());
    }

    // Note: from_env() is tested manually since environment variable modification
    // is unsafe in Rust 2024 edition. from_default_chain() requires async and
    // is tested via integration tests with the aws-msk feature.

    #[test]
    fn test_auth_config_sasl_oauthbearer_provider() {
        let config =
            AuthConfig::sasl_oauthbearer_provider(|| async { Ok(OAuthBearerToken::new("tok")) });
        assert_eq!(config.security_protocol, SecurityProtocol::SaslPlaintext);
        assert_eq!(config.sasl_mechanism, Some(SaslMechanism::OAuthBearer));
        assert!(config.oauthbearer_provider.is_some());
        assert!(config.oauthbearer_token.is_none());
        assert!(!config.requires_tls());
        assert!(config.requires_sasl());
    }

    #[test]
    fn test_auth_config_sasl_oauthbearer_provider_ssl() {
        let config = AuthConfig::sasl_oauthbearer_provider_ssl(
            || async { Ok(OAuthBearerToken::new("tok")) },
            TlsConfig::new(),
        );
        assert_eq!(config.security_protocol, SecurityProtocol::SaslSsl);
        assert_eq!(config.sasl_mechanism, Some(SaslMechanism::OAuthBearer));
        assert!(config.oauthbearer_provider.is_some());
        assert!(config.tls_config.is_some());
        assert!(config.requires_tls());
        assert!(config.requires_sasl());
    }

    #[test]
    fn test_auth_config_provider_debug_no_secrets() {
        let config =
            AuthConfig::sasl_oauthbearer_provider(|| async { Ok(OAuthBearerToken::new("secret")) });
        let debug = format!("{config:?}");
        assert!(!debug.contains("secret"));
        assert!(debug.contains("[OAuthBearerTokenProvider]"));
    }

    #[tokio::test]
    async fn test_resolve_provider_to_token_calls_provider() {
        let config =
            AuthConfig::sasl_oauthbearer_provider(|| async { Ok(OAuthBearerToken::new("fresh")) });
        let resolved = config.resolve_provider_to_token().await.unwrap().unwrap();

        // Token is set
        assert!(resolved.oauthbearer_token.is_some());
        assert_eq!(
            resolved
                .oauthbearer_token
                .unwrap()
                .to_gs2_initial_response(),
            OAuthBearerToken::new("fresh").to_gs2_initial_response()
        );
        // Provider is cleared
        assert!(resolved.oauthbearer_provider.is_none());
        // Mechanism and protocol are preserved
        assert_eq!(resolved.sasl_mechanism, Some(SaslMechanism::OAuthBearer));
        assert_eq!(resolved.security_protocol, SecurityProtocol::SaslPlaintext);
    }

    #[tokio::test]
    async fn test_resolve_provider_to_token_preserves_tls() {
        let config = AuthConfig::sasl_oauthbearer_provider_ssl(
            || async { Ok(OAuthBearerToken::new("tok")) },
            TlsConfig::new(),
        );
        let resolved = config.resolve_provider_to_token().await.unwrap().unwrap();

        assert!(resolved.tls_config.is_some());
        assert_eq!(resolved.security_protocol, SecurityProtocol::SaslSsl);
    }

    #[tokio::test]
    async fn test_resolve_provider_to_token_returns_none_for_static() {
        let config = AuthConfig::sasl_oauthbearer("static-tok");
        assert!(config.resolve_provider_to_token().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_resolve_provider_to_token_returns_none_for_non_oauth() {
        let config = AuthConfig::sasl_plain("user", "pass");
        assert!(config.resolve_provider_to_token().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_resolve_provider_to_token_propagates_error() {
        let config = AuthConfig::sasl_oauthbearer_provider(|| async {
            Err(crate::error::KrafkaError::auth("oauth server down"))
        });
        let err = config.resolve_provider_to_token().await.unwrap_err();
        assert!(err.to_string().contains("oauth server down"));
    }
}
