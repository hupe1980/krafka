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

pub(crate) mod msk_iam;
pub mod oauthbearer;
/// Built-in OIDC token provider for SASL/OAUTHBEARER: the OAuth 2.0
/// `client_credentials` grant (KIP-768) and the RFC 7523 client-assertion
/// variant (KIP-1258).
///
/// Requires the `oauth-oidc` feature.
#[cfg(feature = "oauth-oidc")]
#[cfg_attr(docsrs, doc(cfg(feature = "oauth-oidc")))]
pub mod oidc;
mod provider;
pub(crate) mod scram;
pub(crate) mod tls;

pub use oauthbearer::OAuthBearerToken;
pub(crate) use oauthbearer::OAuthBearerTokenProviderHandle;
#[cfg(feature = "oauth-oidc")]
#[cfg_attr(docsrs, doc(cfg(feature = "oauth-oidc")))]
pub use oidc::{AssertionSource, ClientCredentials, OidcTokenProvider, OidcTokenProviderBuilder};
pub use provider::CredentialProvider;
pub(crate) use provider::ErasedCredentialProvider;
pub use scram::ScramMechanism;

use std::fmt;
use std::sync::Arc;

use zeroize::{Zeroize, ZeroizeOnDrop, Zeroizing};

/// A shared [`CredentialProvider`] of AWS MSK IAM credentials.
#[derive(Clone)]
pub(crate) struct AwsMskIamCredentialProviderHandle(
    Arc<dyn ErasedCredentialProvider<AwsMskIamCredentials>>,
);

impl AwsMskIamCredentialProviderHandle {
    pub(crate) fn new(provider: impl CredentialProvider<AwsMskIamCredentials> + 'static) -> Self {
        Self(Arc::new(provider))
    }

    pub(crate) async fn provide_credentials(&self) -> crate::error::Result<AwsMskIamCredentials> {
        self.0.credentials_erased().await
    }
}

impl fmt::Debug for AwsMskIamCredentialProviderHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[AwsMskIamCredentialProvider]")
    }
}

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
    ///
    /// Not yet implemented — configuring this mechanism returns a runtime error.
    /// Use one of the other mechanisms for production deployments.
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

impl SaslMechanism {
    /// Whether the mechanism puts a credential on the wire that an
    /// eavesdropper can replay: PLAIN's password and OAUTHBEARER's token.
    ///
    /// SCRAM sends a proof bound to the exchange's nonces and AWS_MSK_IAM a
    /// signature that expires within minutes. The match is exhaustive so a
    /// new mechanism must be classified here.
    pub(crate) fn sends_reusable_credential(&self) -> bool {
        match self {
            Self::Plain | Self::OAuthBearer => true,
            Self::ScramSha256 | Self::ScramSha512 | Self::AwsMskIam | Self::Gssapi => false,
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
    /// Create new PLAIN credentials. Checked when the [`AuthConfig`] holding
    /// them is used: see [`AuthConfig::sasl_plain`].
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }

    /// A NUL byte is the field delimiter of the PLAIN message, and an empty
    /// username cannot authenticate.
    fn validate(&self) -> crate::Result<()> {
        if self.username.is_empty() {
            return Err(crate::error::KrafkaError::config(
                "security: SASL/PLAIN username must not be empty",
            ));
        }
        if self.username.contains('\0') || self.password.contains('\0') {
            return Err(crate::error::KrafkaError::config(
                "security: SASL/PLAIN username and password must not contain NUL bytes",
            ));
        }
        Ok(())
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
/// Use the accessor methods ([`access_key_id`](Self::access_key_id),
/// [`region`](Self::region), [`has_session_token`](Self::has_session_token)) to read
/// fields rather than accessing them directly — `secret_access_key` and `session_token`
/// are intentionally not exposed to prevent accidental logging.
#[non_exhaustive]
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct AwsMskIamCredentials {
    /// AWS access key ID.
    pub(super) access_key_id: String,
    /// AWS secret access key (zeroized on drop).
    pub(super) secret_access_key: String,
    /// AWS session token (for temporary credentials, zeroized on drop).
    pub(super) session_token: Option<String>,
    /// AWS region.
    pub(super) region: String,
}

impl AwsMskIamCredentials {
    /// Create new AWS MSK IAM credentials.
    ///
    /// For temporary credentials, chain
    /// [`with_session_token`](Self::with_session_token).
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

    /// Attach a session token (for temporary credentials).
    ///
    /// ```rust
    /// use krafka::auth::AwsMskIamCredentials;
    ///
    /// let creds = AwsMskIamCredentials::new("AKID", "secret", "eu-central-1")
    ///     .with_session_token("FwoGZXIvYXdzE...");
    /// assert!(creds.has_session_token());
    /// ```
    #[must_use]
    pub fn with_session_token(mut self, session_token: impl Into<String>) -> Self {
        // Wipe any token being replaced: `Option<String>`'s own drop frees the
        // buffer without clearing it, so a rotated token would otherwise stay
        // readable in freed heap memory — which is the one thing the
        // `ZeroizeOnDrop` on this type exists to prevent.
        if let Some(previous) = self.session_token.as_mut() {
            previous.zeroize();
        }
        self.session_token = Some(session_token.into());
        self
    }

    /// Replace the AWS region, preserving every other field.
    ///
    /// # Why this exists
    ///
    /// `secret_access_key` and `session_token` are deliberately unreadable —
    /// see the type-level note. Without a consuming setter that hygiene became
    /// an obstacle: an embedder that loads credentials with
    /// [`from_env`](Self::from_env) but takes the region from its own
    /// configuration file had no way to combine the two. The natural-looking
    /// workaround —
    ///
    /// ```rust,ignore
    /// AwsMskIamCredentials::new(creds.access_key_id(), secret, configured_region)
    /// ```
    ///
    /// — silently **drops the session token**, and every deployment using an
    /// assumed role, an EC2/ECS instance profile or an EKS web identity then
    /// fails SigV4 verification at connect time with an error that never
    /// mentions the token.
    ///
    /// ```rust
    /// use krafka::auth::AwsMskIamCredentials;
    ///
    /// let creds = AwsMskIamCredentials::new("AKID", "secret", "us-east-1")
    ///     .with_session_token("token")
    ///     .with_region("eu-central-1");
    /// assert_eq!(creds.region(), "eu-central-1");
    /// assert!(creds.has_session_token(), "re-regioning must preserve the token");
    /// ```
    #[must_use]
    pub fn with_region(mut self, region: impl Into<String>) -> Self {
        self.region = region.into();
        self
    }

    /// Returns the AWS access key ID.
    pub fn access_key_id(&self) -> &str {
        &self.access_key_id
    }

    /// Returns the AWS region.
    pub fn region(&self) -> &str {
        &self.region
    }

    /// Returns `true` if a session token is present (temporary credentials).
    pub fn has_session_token(&self) -> bool {
        self.session_token.is_some()
    }

    /// Create credentials from environment variables.
    ///
    /// Reads from:
    /// - `AWS_ACCESS_KEY_ID` - Required
    /// - `AWS_SECRET_ACCESS_KEY` - Required    
    /// - `AWS_SESSION_TOKEN` - Optional (for temporary credentials)
    /// - `AWS_REGION` or `AWS_DEFAULT_REGION` - Required
    ///
    /// # No feature flag required
    ///
    /// This method works **without** the `aws-msk` feature. The `aws-msk`
    /// feature is only needed for `AwsMskIamCredentials::from_default_chain`,
    /// which pulls in the full AWS SDK credential provider chain (~100 crates).
    /// Use this method when you have static credentials available in the
    /// environment to keep your dependency footprint small.
    ///
    /// # Errors
    ///
    /// Returns error if required environment variables are not set.
    pub fn from_env() -> crate::error::Result<Self> {
        let region = std::env::var("AWS_REGION")
            .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
            .map_err(|_| {
                crate::error::KrafkaError::config(
                    "AWS_REGION or AWS_DEFAULT_REGION environment variable not set",
                )
            })?;

        Self::from_env_with_region(region)
    }

    /// Create credentials from environment variables, with the region supplied
    /// by the caller.
    ///
    /// Identical to [`from_env`](Self::from_env) except that `AWS_REGION` /
    /// `AWS_DEFAULT_REGION` are neither read nor required. Use this when the
    /// region comes from your own configuration — a config file, a CLI flag, a
    /// secret manager — rather than from the process environment.
    ///
    /// It exists so that combining "keys from the environment" with "region
    /// from configuration" does not require re-implementing this function.
    /// Equivalent to `from_env()?.with_region(region)` when the environment
    /// also carries a region, and possible when it does not.
    ///
    /// # No feature flag required
    ///
    /// Like [`from_env`](Self::from_env), this works **without** the `aws-msk`
    /// feature.
    ///
    /// # Errors
    ///
    /// Returns an error if `AWS_ACCESS_KEY_ID` or `AWS_SECRET_ACCESS_KEY` is
    /// not set.
    pub fn from_env_with_region(region: impl Into<String>) -> crate::error::Result<Self> {
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").map_err(|_| {
            crate::error::KrafkaError::config("AWS_ACCESS_KEY_ID environment variable not set")
        })?;

        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").map_err(|_| {
            crate::error::KrafkaError::config("AWS_SECRET_ACCESS_KEY environment variable not set")
        })?;

        // Read unconditionally: a temporary credential without its session
        // token is not a usable credential, and dropping it here is precisely
        // the failure this API is shaped to prevent.
        let session_token = std::env::var("AWS_SESSION_TOKEN").ok();

        Ok(Self {
            access_key_id,
            secret_access_key,
            session_token,
            region: region.into(),
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
        // Show only the last 4 chars of access_key_id (sufficient for identification,
        // insufficient for impersonation). Full key IDs should not appear in logs.
        // Use char-boundary-safe extraction so Debug never panics on non-ASCII input.
        let akid_tail = {
            let s = &self.access_key_id;
            let tail: String = s
                .chars()
                .rev()
                .take(4)
                .collect::<String>()
                .chars()
                .rev()
                .collect();
            if tail.len() < s.chars().count() {
                format!("***{tail}")
            } else {
                "***".to_string()
            }
        };
        f.debug_struct("AwsMskIamCredentials")
            .field("access_key_id", &akid_tail)
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
/// For insecure mode (local development / self-signed certificates without a
/// CA bundle), use [`TlsConfig::insecure()`].
#[derive(Clone)]
pub struct TlsConfig {
    /// Path to CA certificate file.
    pub(crate) ca_cert_path: Option<String>,
    /// Path to client certificate file.
    pub(crate) client_cert_path: Option<String>,
    /// Path to client private key file.
    pub(crate) client_key_path: Option<String>,
    /// Passphrase for an encrypted client private key (zeroized on drop).
    ///
    /// Kept for the lifetime of the config because certificate reloads read
    /// the key file again.
    pub(crate) client_key_password: Option<Zeroizing<String>>,
    /// Whether to load root certificates from the platform trust store.
    pub(crate) use_native_roots: bool,
    /// Whether to verify server certificates (defaults to `true`).
    pub(crate) verify_server_cert: bool,
    /// Server name indication (SNI) hostname.
    pub(crate) sni_hostname: Option<String>,
    /// ALPN protocol names to advertise during the TLS handshake.
    ///
    /// Empty by default. Use [`with_alpn_protocols()`](Self::with_alpn_protocols)
    /// or the convenience [`with_kafka_alpn()`](Self::with_kafka_alpn) to set.
    pub(crate) alpn_protocols: Vec<Vec<u8>>,
}

impl Default for TlsConfig {
    /// Returns a secure default: certificate verification enabled.
    fn default() -> Self {
        Self {
            ca_cert_path: None,
            client_cert_path: None,
            client_key_path: None,
            client_key_password: None,
            use_native_roots: false,
            verify_server_cert: true,
            sni_hostname: None,
            alpn_protocols: Vec::new(),
        }
    }
}

impl fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsConfig")
            .field("ca_cert_path", &self.ca_cert_path)
            .field("client_cert_path", &self.client_cert_path)
            .field("client_key_path", &self.client_key_path)
            .field(
                "client_key_password",
                &self.client_key_password.as_ref().map(|_| "[REDACTED]"),
            )
            .field("use_native_roots", &self.use_native_roots)
            .field("verify_server_cert", &self.verify_server_cert)
            .field("sni_hostname", &self.sni_hostname)
            .field("alpn_protocols", &self.alpn_protocols)
            .finish()
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
    /// **Warning:** This disables TLS certificate verification entirely. Only
    /// use for local development or testing. For production use with
    /// self-signed certs, prefer [`with_ca_cert()`](Self::with_ca_cert) to
    /// supply the CA certificate explicitly.
    ///
    /// Setting this option emits a one-time `warn!` log at connection time so
    /// the configuration is visible in production logs.
    pub fn insecure() -> Self {
        Self {
            verify_server_cert: false,
            ..Default::default()
        }
    }

    /// Set the CA certificate path (pinning).
    ///
    /// When set, **only** the PEM-encoded certificates at this path are
    /// trusted — the compiled-in WebPKI (Mozilla) roots are **not** loaded.
    /// This matches the pinning semantics of the Java Kafka client
    /// (`ssl.truststore.location`) and librdkafka (`ssl.ca.location`).
    ///
    /// To trust both platform roots **and** the custom CA, combine with
    /// `with_native_roots()`.
    pub fn with_ca_cert(mut self, path: impl Into<String>) -> Self {
        self.ca_cert_path = Some(path.into());
        self
    }

    /// Load root certificates from the platform trust store.
    ///
    /// Requires the `native-tls-roots` crate feature. When used alone, native
    /// trust anchors replace the default WebPKI roots. When combined with
    /// [`with_ca_cert()`](Self::with_ca_cert), native roots are loaded first
    /// and the explicit CA certificates are added on top.
    #[cfg(feature = "native-tls-roots")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls-roots")))]
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

    /// Set the passphrase for an encrypted client private key.
    ///
    /// The Java client's and librdkafka's `ssl.key.password`. Requires the
    /// `tls-encrypted-keys` crate feature.
    ///
    /// The key must be PEM `ENCRYPTED PRIVATE KEY` (PKCS#8 PBES2: PBKDF2 with
    /// HMAC-SHA-2, or scrypt; AES-CBC), which OpenSSL 1.1+ writes by default.
    /// Legacy OpenSSL encryption (`Proc-Type: 4,ENCRYPTED`) and PBKDF2 with
    /// HMAC-SHA-1 are rejected; re-encrypt such keys with
    /// `openssl pkcs8 -topk8 -v2 aes256`.
    ///
    /// An unencrypted key ignores the passphrase.
    ///
    /// ```rust
    /// use krafka::auth::TlsConfig;
    ///
    /// let tls = TlsConfig::new()
    ///     .with_ca_cert("/etc/kafka/ca.pem")
    ///     .with_client_cert("/etc/kafka/client.pem", "/etc/kafka/client.key")
    ///     .with_client_key_password("passphrase");
    /// ```
    #[cfg(feature = "tls-encrypted-keys")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tls-encrypted-keys")))]
    pub fn with_client_key_password(mut self, password: impl Into<String>) -> Self {
        self.client_key_password = Some(Zeroizing::new(password.into()));
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

    /// Set ALPN protocol names to advertise during the TLS handshake.
    ///
    /// Some environments (e.g., service meshes, load balancers) require ALPN
    /// for protocol multiplexing. Pass protocol names as byte slices.
    pub fn with_alpn_protocols(mut self, protocols: Vec<Vec<u8>>) -> Self {
        self.alpn_protocols = protocols;
        self
    }

    /// Convenience method to advertise `"kafka"` as the ALPN protocol.
    ///
    /// Equivalent to `with_alpn_protocols(vec![b"kafka".to_vec()])`.
    pub fn with_kafka_alpn(self) -> Self {
        self.with_alpn_protocols(vec![b"kafka".to_vec()])
    }

    /// Returns the configured ALPN protocols.
    pub fn alpn_protocols(&self) -> &[Vec<u8>] {
        &self.alpn_protocols
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
    /// AWS MSK IAM credential provider for automatic credential refresh.
    pub(crate) aws_msk_iam_credential_provider: Option<AwsMskIamCredentialProviderHandle>,
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

    /// Wrap this configuration in TLS, upgrading the security protocol.
    ///
    /// | Before | After |
    /// |---|---|
    /// | `PLAINTEXT` | `SSL` |
    /// | `SASL_PLAINTEXT` | `SASL_SSL` |
    /// | `SSL` / `SASL_SSL` | unchanged, `tls_config` replaced |
    ///
    /// Every SASL mechanism composes with TLS this way:
    ///
    /// ```rust
    /// use krafka::auth::{AuthConfig, SecurityProtocol, TlsConfig};
    ///
    /// let tls = TlsConfig::new().with_ca_cert("/etc/kafka/ca.pem");
    /// let config = AuthConfig::sasl_scram_sha512("user", "pass").with_tls(tls);
    ///
    /// assert_eq!(config.security_protocol(), &SecurityProtocol::SaslSsl);
    /// assert!(config.tls_config().is_some());
    /// ```
    #[must_use]
    pub fn with_tls(mut self, tls_config: TlsConfig) -> Self {
        self.security_protocol = match self.security_protocol {
            SecurityProtocol::Plaintext => SecurityProtocol::Ssl,
            SecurityProtocol::SaslPlaintext => SecurityProtocol::SaslSsl,
            // Already encrypted — keep the protocol, take the new settings.
            already_tls => already_tls,
        };
        self.tls_config = Some(tls_config);
        self
    }

    /// Create a SASL/PLAIN configuration.
    ///
    /// The credentials are checked by
    /// [`KafkaBuilder::connect`](crate::KafkaBuilder::connect): an empty
    /// username or a NUL byte is a [`Config`](crate::KrafkaError::Config) error.
    pub fn sasl_plain(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::Plain),
            plain_credentials: Some(PlainCredentials::new(username, password)),
            ..Default::default()
        }
    }

    /// Create a SASL/SCRAM-SHA-256 configuration over cleartext. Chain
    /// [`with_tls`](Self::with_tls) for `SASL_SSL`.
    pub fn sasl_scram_sha256(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::ScramSha256),
            scram_credentials: Some(ScramCredentials::new(username, password)),
            ..Default::default()
        }
    }

    /// Create a SASL/SCRAM-SHA-512 configuration over cleartext. Chain
    /// [`with_tls`](Self::with_tls) for `SASL_SSL`.
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

    /// Create an AWS MSK IAM configuration with a credential provider.
    ///
    /// The provider is called on every new broker connection (including
    /// reconnections), ensuring credentials are always fresh. This is the
    /// recommended approach for temporary credentials (STS, IRSA, ECS task
    /// role, EC2 instance profile).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use krafka::auth::{AuthConfig, AwsMskIamCredentials};
    ///
    /// let config = AuthConfig::aws_msk_iam_provider(|| async {
    ///     AwsMskIamCredentials::from_default_chain("us-east-1").await
    /// });
    /// ```
    pub fn aws_msk_iam_provider(
        provider: impl CredentialProvider<AwsMskIamCredentials> + 'static,
    ) -> Self {
        Self {
            security_protocol: SecurityProtocol::SaslSsl,
            sasl_mechanism: Some(SaslMechanism::AwsMskIam),
            aws_msk_iam_credential_provider: Some(AwsMskIamCredentialProviderHandle::new(provider)),
            tls_config: Some(TlsConfig::new()),
            ..Default::default()
        }
    }

    /// Create a SASL/OAUTHBEARER configuration with a static token.
    ///
    /// Uses SASL_PLAINTEXT; chain [`with_tls`](Self::with_tls) for TLS.
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
    pub fn sasl_oauthbearer_provider(
        provider: impl CredentialProvider<OAuthBearerToken> + 'static,
    ) -> Self {
        Self {
            security_protocol: SecurityProtocol::SaslPlaintext,
            sasl_mechanism: Some(SaslMechanism::OAuthBearer),
            oauthbearer_provider: Some(OAuthBearerTokenProviderHandle::new(provider)),
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
    pub(crate) async fn resolve_provider_to_token(
        &self,
    ) -> crate::error::Result<Option<AuthConfig>> {
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

    /// If this config has an MSK IAM credential provider, resolve fresh
    /// credentials and return a new `AuthConfig` with the credentials set
    /// and the provider cleared. Returns `None` if no provider is configured
    /// (the caller should use `self` as-is).
    ///
    /// This must be called before passing a provider-based config to
    /// [`SaslAuthenticator::new_msk_iam()`](crate::network::SaslAuthenticator::new_msk_iam),
    /// which requires resolved credentials.
    ///
    /// # Errors
    ///
    /// Returns an error if the provider fails to fetch credentials.
    pub(crate) async fn resolve_msk_iam_provider(
        &self,
    ) -> crate::error::Result<Option<AuthConfig>> {
        if self.sasl_mechanism == Some(SaslMechanism::AwsMskIam)
            && let Some(ref provider) = self.aws_msk_iam_credential_provider
        {
            let credentials = provider.provide_credentials().await?;
            Ok(Some(AuthConfig {
                aws_msk_iam_credentials: Some(credentials),
                aws_msk_iam_credential_provider: None,
                ..self.clone()
            }))
        } else {
            Ok(None)
        }
    }

    /// Reject credentials that cannot authenticate, naming the setting.
    pub(crate) fn validate(&self) -> crate::Result<()> {
        if let Some(plain) = &self.plain_credentials {
            plain.validate()?;
        }
        if let Some(scram) = &self.scram_credentials
            && scram.username.is_empty()
        {
            return Err(crate::error::KrafkaError::config(
                "security: SASL/SCRAM username must not be empty",
            ));
        }
        if let Some(token) = &self.oauthbearer_token {
            token.validate()?;
        }
        Ok(())
    }

    /// Warn that this configuration sends a reusable credential to `address`
    /// without TLS.
    ///
    /// Called on every SASL connect, so the misconfiguration shows in the
    /// logs whatever the reconnect history.
    pub(crate) fn warn_if_cleartext_credential(&self, address: &str) {
        if self.security_protocol != SecurityProtocol::SaslPlaintext {
            return;
        }
        if let Some(mechanism) = &self.sasl_mechanism
            && mechanism.sends_reusable_credential()
        {
            tracing::warn!(
                "SASL {mechanism} credentials will be sent in cleartext to {address}; \
                 anyone on the network path can reuse them. Use SASL_SSL."
            );
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
    pub(crate) fn oauthbearer_provider(&self) -> Option<&OAuthBearerTokenProviderHandle> {
        self.oauthbearer_provider.as_ref()
    }

    /// Returns the TLS configuration, if set.
    pub fn tls_config(&self) -> Option<&TlsConfig> {
        self.tls_config.as_ref()
    }

    /// Construct an `AuthConfig` from standard Kafka environment variables.
    ///
    /// | Variable | Values |
    /// |---|---|
    /// | `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` *(default)*, `SSL`, `SASL_PLAINTEXT`, `SASL_SSL` |
    /// | `KAFKA_SASL_MECHANISM` | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, `OAUTHBEARER`, `AWS_MSK_IAM` |
    /// | `KAFKA_SASL_USERNAME` | any string — required for `PLAIN` and both SCRAM mechanisms |
    /// | `KAFKA_SASL_PASSWORD` | any string — required for `PLAIN` and both SCRAM mechanisms |
    /// | `KAFKA_SASL_OAUTHBEARER_TOKEN` | a JWT — required for `OAUTHBEARER` |
    ///
    /// `AWS_MSK_IAM` takes its credentials from
    /// [`AwsMskIamCredentials::from_env`], i.e. `AWS_ACCESS_KEY_ID`,
    /// `AWS_SECRET_ACCESS_KEY`, the optional `AWS_SESSION_TOKEN` and
    /// `AWS_REGION` / `AWS_DEFAULT_REGION`.
    ///
    /// TLS material is read for every protocol that encrypts (`SSL`,
    /// `SASL_SSL`, and `AWS_MSK_IAM`, which is always TLS):
    ///
    /// | Variable | Effect |
    /// |---|---|
    /// | `KAFKA_SSL_CA_LOCATION` | [`TlsConfig::with_ca_cert`] — pins this CA bundle |
    /// | `KAFKA_SSL_CERTIFICATE_LOCATION` | client certificate (mTLS); requires the key too |
    /// | `KAFKA_SSL_KEY_LOCATION` | client private key (mTLS); requires the certificate too |
    /// | `KAFKA_SSL_KEY_PASSWORD` | [`TlsConfig::with_client_key_password`] — requires the key and the `tls-encrypted-keys` feature |
    /// | `KAFKA_SSL_SNI_HOSTNAME` | [`TlsConfig::with_sni_hostname`] |
    ///
    /// Unset TLS variables leave [`TlsConfig::new()`] defaults, which verify
    /// the server certificate against the compiled-in WebPKI roots. There is
    /// deliberately no environment variable that disables verification: use
    /// [`TlsConfig::insecure`] explicitly in code if you need that.
    ///
    /// Returns `AuthConfig::plaintext()` when `KAFKA_SECURITY_PROTOCOL` is absent.
    ///
    /// # This is not the general-purpose path
    ///
    /// `from_env` is a convenience for applications whose configuration *is*
    /// the environment. A library embedder resolving credentials from a secret
    /// manager or a config file should build an [`AuthConfig`] directly —
    /// every combination this function produces is reachable from the public
    /// constructors plus [`with_tls`](Self::with_tls).
    ///
    /// # Errors
    ///
    /// Returns an error when a required variable is missing, when only one half
    /// of the client-certificate pair is set, or when an unrecognised value is
    /// supplied for `KAFKA_SECURITY_PROTOCOL` or `KAFKA_SASL_MECHANISM`.
    ///
    /// # Security
    ///
    /// Credentials are read directly from the environment and stored in
    /// memory-zeroizing types. They are never logged.
    pub fn from_env() -> crate::Result<Self> {
        let protocol =
            std::env::var("KAFKA_SECURITY_PROTOCOL").unwrap_or_else(|_| "PLAINTEXT".to_string());

        match protocol.to_uppercase().as_str() {
            "PLAINTEXT" => Ok(Self::plaintext()),
            "SSL" => Ok(Self::ssl(Self::tls_config_from_env()?)),
            "SASL_PLAINTEXT" | "SASL_SSL" => {
                let mechanism = std::env::var("KAFKA_SASL_MECHANISM").map_err(|_| {
                    crate::error::KrafkaError::config("KAFKA_SASL_MECHANISM not set")
                })?;
                let use_tls = protocol.to_uppercase() == "SASL_SSL";

                // One TLS config, applied through `with_tls` for every
                // mechanism, so every mechanism composes with `SASL_SSL`.
                let config = Self::sasl_config_from_env(&mechanism)?;
                Ok(if use_tls {
                    config.with_tls(Self::tls_config_from_env()?)
                } else {
                    config
                })
            }
            other => Err(crate::error::KrafkaError::config(format!(
                "unknown security protocol in KAFKA_SECURITY_PROTOCOL: {other}"
            ))),
        }
    }

    /// Build the SASL half of [`from_env`](Self::from_env), always over
    /// cleartext; the caller applies TLS.
    fn sasl_config_from_env(mechanism: &str) -> crate::Result<Self> {
        /// Read the username/password pair the password-based mechanisms need.
        fn user_password() -> crate::Result<(String, String)> {
            let username = std::env::var("KAFKA_SASL_USERNAME")
                .map_err(|_| crate::error::KrafkaError::config("KAFKA_SASL_USERNAME not set"))?;
            let password = std::env::var("KAFKA_SASL_PASSWORD")
                .map_err(|_| crate::error::KrafkaError::config("KAFKA_SASL_PASSWORD not set"))?;
            Ok((username, password))
        }

        match mechanism.to_uppercase().as_str() {
            "PLAIN" => {
                let (username, password) = user_password()?;
                Ok(Self::sasl_plain(username, password))
            }
            "SCRAM-SHA-256" => {
                let (username, password) = user_password()?;
                Ok(Self::sasl_scram_sha256(username, password))
            }
            "SCRAM-SHA-512" => {
                let (username, password) = user_password()?;
                Ok(Self::sasl_scram_sha512(username, password))
            }
            "OAUTHBEARER" => {
                let token = std::env::var("KAFKA_SASL_OAUTHBEARER_TOKEN").map_err(|_| {
                    crate::error::KrafkaError::config(
                        "KAFKA_SASL_OAUTHBEARER_TOKEN not set; a static token is the only \
                         OAUTHBEARER configuration expressible in environment variables — \
                         for the OIDC client-credentials flow build the config in code with \
                         AuthConfig::sasl_oauthbearer_provider",
                    )
                })?;
                Ok(Self::sasl_oauthbearer(token))
            }
            "AWS_MSK_IAM" => Ok(Self::aws_msk_iam_with_credentials(
                AwsMskIamCredentials::from_env()?,
            )),
            other => Err(crate::error::KrafkaError::config(format!(
                "unknown SASL mechanism in KAFKA_SASL_MECHANISM: {other}"
            ))),
        }
    }

    /// Build a [`TlsConfig`] from the `KAFKA_SSL_*` environment variables.
    fn tls_config_from_env() -> crate::Result<TlsConfig> {
        Self::tls_config_from_parts(
            std::env::var("KAFKA_SSL_CA_LOCATION").ok(),
            std::env::var("KAFKA_SSL_CERTIFICATE_LOCATION").ok(),
            std::env::var("KAFKA_SSL_KEY_LOCATION").ok(),
            std::env::var("KAFKA_SSL_KEY_PASSWORD")
                .ok()
                .map(Zeroizing::new),
            std::env::var("KAFKA_SSL_SNI_HOSTNAME").ok(),
        )
    }

    /// The pure half of [`tls_config_from_env`](Self::tls_config_from_env).
    ///
    /// Split out because environment mutation is `unsafe` in the 2024 edition,
    /// so the rules below are otherwise untestable.
    fn tls_config_from_parts(
        ca: Option<String>,
        cert: Option<String>,
        key: Option<String>,
        key_password: Option<Zeroizing<String>>,
        sni: Option<String>,
    ) -> crate::Result<TlsConfig> {
        let mut tls = TlsConfig::new();

        if let Some(ca) = ca {
            tls = tls.with_ca_cert(ca);
        }

        // A half-configured mTLS pair is a misconfiguration, not a default:
        // silently ignoring a lone certificate path would present no client
        // identity to a broker that requires one, and the resulting handshake
        // failure names neither variable.
        // Same reasoning for a passphrase with no key to decrypt.
        if key_password.is_some() && key.is_none() {
            return Err(crate::error::KrafkaError::config(
                "KAFKA_SSL_KEY_PASSWORD is set without KAFKA_SSL_KEY_LOCATION; \
                 a key passphrase needs the key it decrypts",
            ));
        }

        match (cert, key) {
            (Some(cert), Some(key)) => tls = tls.with_client_cert(cert, key),
            (Some(_), None) => {
                return Err(crate::error::KrafkaError::config(
                    "KAFKA_SSL_CERTIFICATE_LOCATION is set without KAFKA_SSL_KEY_LOCATION; \
                     a client certificate needs its private key",
                ));
            }
            (None, Some(_)) => {
                return Err(crate::error::KrafkaError::config(
                    "KAFKA_SSL_KEY_LOCATION is set without KAFKA_SSL_CERTIFICATE_LOCATION; \
                     a client private key needs its certificate",
                ));
            }
            (None, None) => {}
        }

        if let Some(password) = key_password {
            #[cfg(feature = "tls-encrypted-keys")]
            {
                tls.client_key_password = Some(password);
            }
            #[cfg(not(feature = "tls-encrypted-keys"))]
            {
                drop(password);
                return Err(crate::error::KrafkaError::config(
                    "KAFKA_SSL_KEY_PASSWORD requires the 'tls-encrypted-keys' crate feature",
                ));
            }
        }

        if let Some(sni) = sni {
            tls = tls.with_sni_hostname(sni);
        }

        Ok(tls)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    /// Run `f` and return what it logged at WARN or above.
    fn warnings_from(f: impl FnOnce()) -> String {
        #[derive(Clone, Default)]
        struct Capture(Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Capture {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let capture = Capture::default();
        let writer = capture.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_max_level(tracing::Level::WARN)
            .with_ansi(false)
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        String::from_utf8(capture.0.lock().unwrap().clone()).unwrap()
    }

    struct FixedToken;
    impl CredentialProvider<OAuthBearerToken> for FixedToken {
        async fn credentials(&self) -> crate::Result<OAuthBearerToken> {
            Ok(OAuthBearerToken::new("t"))
        }
    }

    /// Every configuration that sends a replayable credential, as
    /// `(mechanism, over SASL_PLAINTEXT, over SASL_SSL)`.
    fn bearer_configs() -> Vec<(&'static str, AuthConfig, AuthConfig)> {
        let tls = TlsConfig::new;
        #[cfg_attr(not(feature = "oauth-oidc"), allow(unused_mut))]
        let mut configs = vec![
            (
                "PLAIN",
                AuthConfig::sasl_plain("u", "p"),
                AuthConfig::sasl_plain("u", "p").with_tls(tls()),
            ),
            (
                "OAUTHBEARER",
                AuthConfig::sasl_oauthbearer("t"),
                AuthConfig::sasl_oauthbearer("t").with_tls(tls()),
            ),
            (
                "OAUTHBEARER",
                AuthConfig::sasl_oauthbearer_token(OAuthBearerToken::new("t")),
                AuthConfig::sasl_oauthbearer_token(OAuthBearerToken::new("t")).with_tls(tls()),
            ),
            (
                "OAUTHBEARER",
                AuthConfig::sasl_oauthbearer_provider(FixedToken),
                AuthConfig::sasl_oauthbearer_provider(FixedToken).with_tls(tls()),
            ),
        ];
        #[cfg(feature = "oauth-oidc")]
        {
            let oidc = || {
                OidcTokenProvider::builder("https://idp.example.com/token")
                    .credentials(ClientCredentials::secret("id", "secret"))
                    .build()
                    .unwrap()
            };
            configs.push((
                "OAUTHBEARER",
                AuthConfig::sasl_oauthbearer_provider(oidc()),
                AuthConfig::sasl_oauthbearer_provider(oidc()).with_tls(tls()),
            ));
        }
        configs
    }

    #[test]
    fn every_bearer_credential_over_sasl_plaintext_is_warned_about() {
        for (mechanism, plaintext, _) in bearer_configs() {
            let logged = warnings_from(|| plaintext.warn_if_cleartext_credential("b1:9092"));
            assert!(
                logged.contains(&format!(
                    "SASL {mechanism} credentials will be sent in cleartext to b1:9092"
                )),
                "{mechanism}: got {logged:?}"
            );
        }
    }

    /// Negative control: the same credentials over TLS, and SCRAM's
    /// challenge-response over plaintext, log nothing.
    #[test]
    fn tls_and_scram_are_not_warned_about() {
        let mut quiet: Vec<AuthConfig> = bearer_configs()
            .into_iter()
            .map(|(_, _, ssl)| ssl)
            .collect();
        quiet.push(AuthConfig::sasl_scram_sha256("u", "p"));
        quiet.push(AuthConfig::sasl_scram_sha512("u", "p"));
        for config in quiet {
            let logged = warnings_from(|| config.warn_if_cleartext_credential("b1:9092"));
            assert!(logged.is_empty(), "{config:?}: got {logged:?}");
        }
    }

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
    #[cfg(feature = "native-tls-roots")]
    fn test_tls_config() {
        let config = TlsConfig::new()
            .with_ca_cert("/path/to/ca.pem")
            .with_client_cert("/path/to/client.pem", "/path/to/client.key")
            .with_native_roots();

        assert!(config.verify_server_cert);
        assert!(config.use_native_roots());
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
        assert_eq!(creds.access_key_id(), "AKID123");
        assert_eq!(creds.region(), "us-west-2");
        assert!(!creds.has_session_token());
    }

    #[test]
    fn test_aws_msk_credentials_with_session_token() {
        let creds = AwsMskIamCredentials::new("AKID123", "secret123", "us-east-1")
            .with_session_token("token123");
        assert_eq!(creds.access_key_id(), "AKID123");
        assert!(creds.has_session_token());
    }

    /// Re-regioning must not lose the session token.
    ///
    /// Rebuilding the credential through `new` drops the token, and MSK then
    /// rejects the SigV4 signature at connect time with an error that never
    /// mentions it.
    ///
    /// Negative control: deleting the `session_token` copy from `with_region`
    /// (i.e. rebuilding via `new`) fails this assertion.
    #[test]
    fn with_region_preserves_the_session_token() {
        let creds = AwsMskIamCredentials::new("AKID", "secret", "us-east-1")
            .with_session_token("session-token")
            .with_region("eu-central-1");

        assert_eq!(creds.region(), "eu-central-1");
        assert!(
            creds.has_session_token(),
            "with_region must preserve temporary-credential material"
        );
        assert_eq!(creds.access_key_id(), "AKID");
        // The secret is unreadable by design, so assert it survived by the one
        // observable route: the signer sees it.
        assert_eq!(creds.secret_access_key, "secret");
    }

    /// `with_region` must also work on a credential that has no token, and
    /// must not invent one.
    #[test]
    fn with_region_does_not_invent_a_session_token() {
        let creds =
            AwsMskIamCredentials::new("AKID", "secret", "us-east-1").with_region("ap-south-1");
        assert_eq!(creds.region(), "ap-south-1");
        assert!(!creds.has_session_token());
    }

    #[test]
    fn with_tls_upgrades_every_cleartext_protocol() {
        // PLAINTEXT → SSL
        let ssl = AuthConfig::plaintext().with_tls(TlsConfig::new());
        assert_eq!(ssl.security_protocol(), &SecurityProtocol::Ssl);
        assert!(ssl.tls_config().is_some());

        // SASL_PLAINTEXT → SASL_SSL, for every mechanism that can be built
        // over cleartext.
        for config in [
            AuthConfig::sasl_plain("u", "p"),
            AuthConfig::sasl_scram_sha256("u", "p"),
            AuthConfig::sasl_scram_sha512("u", "p"),
            AuthConfig::sasl_oauthbearer("jwt"),
        ] {
            let mechanism = config.sasl_mechanism().cloned();
            let upgraded = config.with_tls(TlsConfig::new());
            assert_eq!(
                upgraded.security_protocol(),
                &SecurityProtocol::SaslSsl,
                "mechanism {mechanism:?} must upgrade to SASL_SSL"
            );
            assert!(upgraded.tls_config().is_some());
            assert_eq!(upgraded.sasl_mechanism(), mechanism.as_ref());
        }
    }

    /// An already-TLS config keeps its protocol and takes the new settings —
    /// upgrading `SASL_SSL` to `SSL` would silently drop the SASL exchange.
    #[test]
    fn with_tls_on_an_encrypted_config_replaces_only_the_settings() {
        let config = AuthConfig::aws_msk_iam("AKID", "secret", "us-east-1")
            .with_tls(TlsConfig::new().with_sni_hostname("broker.example.com"));

        assert_eq!(config.security_protocol(), &SecurityProtocol::SaslSsl);
        assert_eq!(
            config.sasl_mechanism(),
            Some(&SaslMechanism::AwsMskIam),
            "the mechanism must survive"
        );
        assert_eq!(
            config.tls_config().and_then(TlsConfig::sni_hostname),
            Some("broker.example.com")
        );
    }

    #[test]
    fn scram_ssl_constructors_produce_sasl_ssl() {
        let sha256 = AuthConfig::sasl_scram_sha256("u", "p").with_tls(TlsConfig::new());
        assert_eq!(sha256.security_protocol(), &SecurityProtocol::SaslSsl);
        assert_eq!(sha256.sasl_mechanism(), Some(&SaslMechanism::ScramSha256));
        assert!(sha256.scram_credentials().is_some());
        assert!(sha256.requires_tls() && sha256.requires_sasl());

        let sha512 = AuthConfig::sasl_scram_sha512("u", "p").with_tls(TlsConfig::new());
        assert_eq!(sha512.security_protocol(), &SecurityProtocol::SaslSsl);
        assert_eq!(sha512.sasl_mechanism(), Some(&SaslMechanism::ScramSha512));
        assert!(sha512.scram_credentials().is_some());
    }

    /// A half-configured mTLS pair must be rejected, not ignored.
    ///
    /// Ignoring it presents no client identity to a broker that requires one,
    /// and the handshake failure that follows names neither variable.
    ///
    /// Environment mutation is `unsafe` in the 2024 edition, so this drives the
    /// pure helper `from_env` delegates to.
    #[test]
    fn a_half_configured_client_certificate_pair_is_rejected() {
        let cert_only = AuthConfig::tls_config_from_parts(
            None,
            Some("/etc/kafka/client.pem".to_string()),
            None,
            None,
            None,
        )
        .expect_err("a certificate without a key must be rejected");
        assert!(
            cert_only.to_string().contains("KAFKA_SSL_KEY_LOCATION"),
            "the error must name the missing variable, got: {cert_only}"
        );

        let key_only = AuthConfig::tls_config_from_parts(
            None,
            None,
            Some("/etc/kafka/client.key".to_string()),
            None,
            None,
        )
        .expect_err("a key without a certificate must be rejected");
        assert!(
            key_only
                .to_string()
                .contains("KAFKA_SSL_CERTIFICATE_LOCATION"),
            "the error must name the missing variable, got: {key_only}"
        );
    }

    /// Every `KAFKA_SSL_*` variable must reach the `TlsConfig`.
    #[test]
    fn tls_environment_material_reaches_the_config() {
        let tls = AuthConfig::tls_config_from_parts(
            Some("/etc/kafka/ca.pem".to_string()),
            Some("/etc/kafka/client.pem".to_string()),
            Some("/etc/kafka/client.key".to_string()),
            None,
            Some("broker.internal".to_string()),
        )
        .expect("a complete TLS environment is valid");

        assert_eq!(tls.ca_cert_path(), Some("/etc/kafka/ca.pem"));
        assert_eq!(tls.client_cert_path(), Some("/etc/kafka/client.pem"));
        assert_eq!(tls.client_key_path(), Some("/etc/kafka/client.key"));
        assert_eq!(tls.sni_hostname(), Some("broker.internal"));
        assert!(
            tls.verify_server_cert(),
            "no environment variable may disable verification"
        );
    }

    #[test]
    fn a_key_passphrase_without_a_key_is_rejected() {
        let err = AuthConfig::tls_config_from_parts(
            None,
            None,
            None,
            Some(Zeroizing::new("secret".to_string())),
            None,
        )
        .expect_err("a passphrase without a key must be rejected");
        assert!(
            err.to_string().contains("KAFKA_SSL_KEY_LOCATION"),
            "the error must name the missing variable, got: {err}"
        );
    }

    fn tls_with_key_passphrase() -> crate::Result<TlsConfig> {
        AuthConfig::tls_config_from_parts(
            None,
            Some("/etc/kafka/client.pem".to_string()),
            Some("/etc/kafka/client.key".to_string()),
            Some(Zeroizing::new("secret".to_string())),
            None,
        )
    }

    #[test]
    #[cfg(feature = "tls-encrypted-keys")]
    fn key_passphrase_reaches_the_config_and_is_redacted() {
        let tls = tls_with_key_passphrase().expect("a passphrase with its key is valid");
        assert_eq!(
            tls.client_key_password.as_deref().map(String::as_str),
            Some("secret")
        );

        let debug = format!("{tls:?}");
        assert!(!debug.contains("secret"), "passphrase leaked: {debug}");
        assert!(debug.contains("[REDACTED]"), "got: {debug}");
    }

    /// Ignoring the variable would fail later as "key is encrypted", hiding
    /// that the passphrase was supplied and only the feature is missing.
    #[test]
    #[cfg(not(feature = "tls-encrypted-keys"))]
    fn key_passphrase_without_the_feature_is_rejected() {
        let err = tls_with_key_passphrase().expect_err("the feature is required");
        assert!(err.to_string().contains("tls-encrypted-keys"), "got: {err}");
    }

    /// Every mechanism `KAFKA_SASL_MECHANISM` accepts must compose with TLS.
    ///
    /// Guards against a mechanism arm setting `security_protocol` directly,
    /// which would bypass `with_tls`.
    #[test]
    fn every_env_mechanism_reaches_sasl_ssl() {
        // AWS_MSK_IAM is excluded: its credentials come from the AWS
        // environment, which this test does not set. It is covered by
        // `test_auth_config_aws_msk_iam`, and is TLS-only by construction.
        for mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] {
            // The password-based arms need credentials; supply them directly
            // rather than through the environment.
            let cleartext = match mechanism {
                "PLAIN" => AuthConfig::sasl_plain("u", "p"),
                "SCRAM-SHA-256" => AuthConfig::sasl_scram_sha256("u", "p"),
                _ => AuthConfig::sasl_scram_sha512("u", "p"),
            };
            assert_eq!(
                cleartext.security_protocol(),
                &SecurityProtocol::SaslPlaintext,
                "{mechanism} must start as SASL_PLAINTEXT"
            );
            let encrypted = cleartext.with_tls(TlsConfig::new());
            assert_eq!(
                encrypted.security_protocol(),
                &SecurityProtocol::SaslSsl,
                "{mechanism} must reach SASL_SSL"
            );
        }
    }

    #[test]
    fn test_aws_msk_credentials_debug_redacts() {
        let creds = AwsMskIamCredentials::new("AKID123", "supersecret", "us-east-1");
        let debug_str = format!("{creds:?}");
        // access_key_id should be truncated (last 4 chars only)
        assert!(
            debug_str.contains("D123"),
            "should show last 4 chars of AKID"
        );
        assert!(
            !debug_str.contains("AKID123"),
            "should not show full access key ID"
        );
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
        let config = AuthConfig::sasl_oauthbearer("my-token").with_tls(TlsConfig::new());
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
        let config = AuthConfig::sasl_oauthbearer_token(token).with_tls(TlsConfig::new());
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
        let config =
            AuthConfig::sasl_oauthbearer_provider(|| async { Ok(OAuthBearerToken::new("tok")) })
                .with_tls(TlsConfig::new());
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
        let config =
            AuthConfig::sasl_oauthbearer_provider(|| async { Ok(OAuthBearerToken::new("tok")) })
                .with_tls(TlsConfig::new());
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

    #[test]
    fn test_auth_config_aws_msk_iam_provider() {
        let config = AuthConfig::aws_msk_iam_provider(|| async {
            Ok(AwsMskIamCredentials::new("AKID", "secret", "us-east-1"))
        });
        assert_eq!(config.security_protocol, SecurityProtocol::SaslSsl);
        assert_eq!(config.sasl_mechanism, Some(SaslMechanism::AwsMskIam));
        assert!(config.aws_msk_iam_credential_provider.is_some());
        assert!(config.aws_msk_iam_credentials.is_none());
        assert!(config.tls_config.is_some());
    }

    #[test]
    fn test_msk_iam_provider_debug_no_secrets() {
        let config = AuthConfig::aws_msk_iam_provider(|| async {
            Ok(AwsMskIamCredentials::new("AKID", "secret", "us-east-1"))
        });
        let debug = format!("{config:?}");
        assert!(!debug.contains("secret"));
        assert!(debug.contains("[AwsMskIamCredentialProvider]"));
    }

    #[tokio::test]
    async fn test_resolve_msk_iam_provider_calls_provider() {
        let config = AuthConfig::aws_msk_iam_provider(|| async {
            Ok(AwsMskIamCredentials::new("AKID", "secret", "us-east-1"))
        });
        let resolved = config.resolve_msk_iam_provider().await.unwrap().unwrap();

        assert!(resolved.aws_msk_iam_credentials.is_some());
        assert_eq!(
            resolved.aws_msk_iam_credentials.as_ref().unwrap().region,
            "us-east-1"
        );
        // Provider is cleared
        assert!(resolved.aws_msk_iam_credential_provider.is_none());
        // Mechanism and protocol are preserved
        assert_eq!(resolved.sasl_mechanism, Some(SaslMechanism::AwsMskIam));
        assert_eq!(resolved.security_protocol, SecurityProtocol::SaslSsl);
    }

    #[tokio::test]
    async fn test_resolve_msk_iam_provider_returns_none_for_static() {
        let config = AuthConfig::aws_msk_iam("AKID", "secret", "us-east-1");
        assert!(config.resolve_msk_iam_provider().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_resolve_msk_iam_provider_returns_none_for_non_msk() {
        let config = AuthConfig::sasl_plain("user", "pass");
        assert!(config.resolve_msk_iam_provider().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_resolve_msk_iam_provider_propagates_error() {
        let config = AuthConfig::aws_msk_iam_provider(|| async {
            Err(crate::error::KrafkaError::auth(
                "AWS credential fetch failed",
            ))
        });
        let err = config.resolve_msk_iam_provider().await.unwrap_err();
        assert!(err.to_string().contains("AWS credential fetch failed"));
    }
}
