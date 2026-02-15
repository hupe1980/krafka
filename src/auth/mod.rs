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
pub use oauthbearer::OAuthBearerToken;
pub use scram::{ScramClient, ScramMechanism, ScramState};
pub use tls::{
    MaybeSecureStream, build_tls_config, build_tls_config_async, connect_tls, create_tls_connector,
    create_tls_connector_async, load_certs_async, load_private_key_async,
};

use std::fmt;
use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

/// Security protocol for Kafka connections.
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
                crate::error::KrafkaError::config(format!("Failed to load AWS credentials: {}", e))
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
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Path to CA certificate file.
    pub ca_cert_path: Option<String>,
    /// Path to client certificate file.
    pub client_cert_path: Option<String>,
    /// Path to client private key file.
    pub client_key_path: Option<String>,
    /// Whether to verify server certificates.
    pub verify_server_cert: bool,
    /// Server name indication (SNI) hostname.
    pub sni_hostname: Option<String>,
}

impl TlsConfig {
    /// Create a new TLS config that verifies server certificates.
    pub fn new() -> Self {
        Self {
            verify_server_cert: true,
            ..Default::default()
        }
    }

    /// Create a TLS config for self-signed certificates.
    ///
    /// **Note:** This method is deprecated. Use `with_ca_cert()` instead.
    /// Skipping certificate verification is not supported as it defeats
    /// the purpose of TLS and exposes connections to man-in-the-middle attacks.
    ///
    /// For testing with self-signed certificates, provide the CA certificate
    /// using the `with_ca_cert()` method.
    #[deprecated(
        since = "0.1.0",
        note = "Use with_ca_cert() with your CA certificate instead. Insecure mode is not supported."
    )]
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
}

/// Complete authentication configuration.
#[derive(Debug, Clone, Default)]
pub struct AuthConfig {
    /// Security protocol.
    pub security_protocol: SecurityProtocol,
    /// SASL mechanism (if using SASL).
    pub sasl_mechanism: Option<SaslMechanism>,
    /// SASL PLAIN credentials.
    pub plain_credentials: Option<PlainCredentials>,
    /// SASL SCRAM credentials.
    pub scram_credentials: Option<ScramCredentials>,
    /// AWS MSK IAM credentials.
    pub aws_msk_iam_credentials: Option<AwsMskIamCredentials>,
    /// OAUTHBEARER token.
    pub oauthbearer_token: Option<OAuthBearerToken>,
    /// TLS configuration.
    pub tls_config: Option<TlsConfig>,
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

    /// Create a SASL/OAUTHBEARER configuration.
    ///
    /// Uses SASL_PLAINTEXT. For TLS, use `sasl_oauthbearer_ssl()` or
    /// call `.sasl_oauthbearer_token()` with a pre-built `OAuthBearerToken`.
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
            .with_client_cert("/path/to/client.pem", "/path/to/client.key");

        assert!(config.verify_server_cert);
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
}
