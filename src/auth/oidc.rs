//! Built-in OIDC token provider for SASL/OAUTHBEARER.
//!
//! Implements the OAuth 2.0 `client_credentials` grant against an OIDC token
//! endpoint — the flow Apache Kafka added in KIP-768 and `librdkafka` exposes
//! as `sasl.oauthbearer.method=oidc` — plus the RFC 7523 **client assertion**
//! variant that Kafka 4.3 added in KIP-1258.
//!
//! [`OidcTokenProvider`] is a [`CredentialProvider`]
//! of OAUTHBEARER tokens: an HTTPS POST, form encoding, JSON parsing and
//! `expires_in` arithmetic, so an application does not write its own OAuth
//! client.
//!
//! # Two ways to authenticate to the token endpoint
//!
//! | Method | Config | What is sent |
//! |---|---|---|
//! | Client secret (KIP-768) | [`ClientCredentials::secret`] | HTTP Basic `client_id:client_secret` |
//! | Client assertion (KIP-1258, RFC 7523) | [`ClientCredentials::assertion`] | `client_assertion_type` + a signed JWT |
//!
//! With a client assertion the credential on the wire is a short-lived
//! signature, and the private key never leaves the workload.
//!
//! # No cryptography dependency
//!
//! krafka does **not** sign the assertion; the choice of RSA/ECDSA
//! implementation stays with the application. The signed JWT is *sourced*:
//!
//! - [`AssertionSource::file`] — read from disk on every token request, so a
//!   SPIFFE agent, a Vault sidecar or a projected Kubernetes service-account
//!   token can rotate it without a restart. Same as Kafka's
//!   `sasl.oauthbearer.assertion.file`.
//! - [`AssertionSource::provider`] — you produce the JWT, signing it with
//!   whatever library you already depend on.
//!
//! # Example
//!
//! ```rust,no_run
//! use krafka::auth::{AuthConfig, oidc::{AssertionSource, ClientCredentials, OidcTokenProvider}};
//! use std::time::Duration;
//!
//! # fn example() -> Result<(), krafka::error::KrafkaError> {
//! // Client secret (KIP-768).
//! let provider = OidcTokenProvider::builder("https://idp.example.com/oauth2/token")
//!     .credentials(ClientCredentials::secret("my-client-id", "my-client-secret"))
//!     .scope("kafka:write")
//!     .build()?;
//!
//! // Or a sidecar-issued assertion (KIP-1258).
//! let provider = OidcTokenProvider::builder("https://idp.example.com/oauth2/token")
//!     .credentials(ClientCredentials::assertion(
//!         AssertionSource::file("/var/run/secrets/oauth/assertion.jwt"),
//!     ))
//!     .client_id("my-client-id")
//!     .request_timeout(Duration::from_secs(10))
//!     .build()?;
//!
//! let auth = AuthConfig::sasl_oauthbearer_provider(provider);
//! # let _ = auth;
//! # Ok(())
//! # }
//! ```

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, warn};
use zeroize::Zeroizing;

use crate::error::{KrafkaError, Result};
use crate::http::{HttpClient, base64_encode};

use super::TlsConfig;
use super::oauthbearer::OAuthBearerToken;
use super::{CredentialProvider, ErasedCredentialProvider};

/// RFC 7523 §2.2 client-assertion type, sent verbatim as a form parameter.
const JWT_BEARER_ASSERTION_TYPE: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";

/// Largest token-endpoint response body accepted, in bytes.
///
/// A token response is a small JSON object; real ones run to a few kilobytes
/// even with a large JWT. The HTTP client enforces the cap while reading, so a
/// hostile or misconfigured endpoint — or an HTML error page from a captive
/// portal — cannot make it buffer more on every reconnect.
const MAX_TOKEN_RESPONSE_BYTES: usize = 1024 * 1024;

/// Largest assertion file accepted, in bytes.
///
/// A JWT that does not fit in 64 KiB is not a JWT. Bounding the read means a
/// mis-pointed path (a log file, a device node) fails fast instead of pulling
/// an arbitrary amount of data into memory on every token request.
const MAX_ASSERTION_FILE_BYTES: u64 = 64 * 1024;

/// Where the signed client-assertion JWT comes from (KIP-1258, RFC 7523).
///
/// krafka never signs the assertion itself — see the [module docs](self) for
/// why.
#[derive(Clone)]
pub struct AssertionSource(Source);

#[derive(Clone)]
enum Source {
    File(PathBuf),
    Static(Zeroizing<String>),
    Provider(Arc<dyn ErasedCredentialProvider<String>>),
}

impl std::fmt::Debug for AssertionSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.0 {
            Source::File(path) => f.debug_tuple("File").field(path).finish(),
            Source::Static(_) => f.debug_tuple("Static").field(&"[REDACTED]").finish(),
            Source::Provider(_) => f.write_str("Provider(<fn>)"),
        }
    }
}

impl AssertionSource {
    /// Read the JWT from a file, on **every** token request.
    ///
    /// A SPIFFE agent, Vault sidecar or projected Kubernetes service-account
    /// token rewrites the file as the assertion rotates; re-reading picks up
    /// each new value. Surrounding whitespace is trimmed.
    pub fn file(path: impl Into<PathBuf>) -> Self {
        Self(Source::File(path.into()))
    }

    /// A fixed, pre-signed JWT. Only useful for tests and short-lived jobs:
    /// a static assertion fails authentication once it expires.
    pub fn fixed(jwt: impl Into<String>) -> Self {
        Self(Source::Static(Zeroizing::new(jwt.into())))
    }

    /// Produce the JWT on demand, signing it however you like. Called on
    /// every token request, so it must be cheap or cache.
    pub fn provider(provider: impl CredentialProvider<String> + 'static) -> Self {
        Self(Source::Provider(Arc::new(provider)))
    }

    /// Resolve the current signed JWT.
    async fn resolve(&self) -> Result<Zeroizing<String>> {
        match &self.0 {
            Source::Static(jwt) => Ok(jwt.clone()),
            Source::Provider(provider) => Ok(Zeroizing::new(provider.credentials_erased().await?)),
            Source::File(path) => {
                let metadata = tokio::fs::metadata(path).await.map_err(|e| {
                    KrafkaError::auth(format!(
                        "cannot stat client-assertion file {}: {e}",
                        path.display()
                    ))
                })?;
                if metadata.len() > MAX_ASSERTION_FILE_BYTES {
                    return Err(KrafkaError::auth(format!(
                        "client-assertion file {} is {} bytes, above the {MAX_ASSERTION_FILE_BYTES} \
                         byte limit; a JWT is never this large, so this path is probably wrong",
                        path.display(),
                        metadata.len()
                    )));
                }
                let raw = tokio::fs::read_to_string(path).await.map_err(|e| {
                    KrafkaError::auth(format!(
                        "cannot read client-assertion file {}: {e}",
                        path.display()
                    ))
                })?;
                let trimmed = raw.trim();
                if trimmed.is_empty() {
                    // A sidecar that truncates before rewriting produces this
                    // for a few milliseconds. Naming it beats a token endpoint
                    // answering `invalid_client` with no context.
                    return Err(KrafkaError::auth(format!(
                        "client-assertion file {} is empty; if a sidecar rotates it, \
                         this is the truncate-then-write window and the next attempt \
                         should succeed",
                        path.display()
                    )));
                }
                Ok(Zeroizing::new(trimmed.to_string()))
            }
        }
    }
}

/// How the client authenticates to the OIDC **token endpoint**.
///
/// This is distinct from how it authenticates to Kafka: the result of either
/// method is an access token, and that token is what SASL/OAUTHBEARER carries.
///
/// `Debug` redacts the secret. `Zeroizing` scrubs memory on drop but its own
/// `Debug` delegates to the inner `String`, so a derived impl here would print
/// the client secret into any log line that formatted the config — which is
/// how credentials end up in log aggregators.
#[derive(Clone)]
pub enum ClientCredentials {
    /// `client_secret_basic` (KIP-768): the client ID and secret are sent as
    /// HTTP Basic credentials.
    Secret {
        /// OAuth client ID.
        client_id: String,
        /// OAuth client secret.
        client_secret: Zeroizing<String>,
    },
    /// `private_key_jwt` / client assertion (KIP-1258, RFC 7523): a signed JWT
    /// is sent instead of a shared secret.
    Assertion {
        /// Where the signed JWT comes from.
        source: AssertionSource,
    },
}

impl std::fmt::Debug for ClientCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Secret { client_id, .. } => f
                .debug_struct("Secret")
                .field("client_id", client_id)
                .field("client_secret", &"[REDACTED]")
                .finish(),
            Self::Assertion { source } => {
                f.debug_struct("Assertion").field("source", source).finish()
            }
        }
    }
}

impl ClientCredentials {
    /// Authenticate with a client ID and secret (`client_secret_basic`).
    pub fn secret(client_id: impl Into<String>, client_secret: impl Into<String>) -> Self {
        Self::Secret {
            client_id: client_id.into(),
            client_secret: Zeroizing::new(client_secret.into()),
        }
    }

    /// Authenticate with a signed client assertion (RFC 7523, KIP-1258).
    pub fn assertion(source: AssertionSource) -> Self {
        Self::Assertion { source }
    }
}

/// A [`CredentialProvider`] of OAUTHBEARER tokens, fetched from an OIDC
/// token endpoint using the `client_credentials` grant.
///
/// Build with [`OidcTokenProvider::builder`]. Caching and proactive refresh are
/// handled by the layer above — `AuthConfig::sasl_oauthbearer_provider` wraps
/// this in a store that serves a cached token until it approaches expiry — so
/// this type performs one HTTP round trip per call and no more.
pub struct OidcTokenProvider {
    token_endpoint: String,
    credentials: ClientCredentials,
    client_id: Option<String>,
    scope: Option<String>,
    form_parameters: Vec<(String, String)>,
    sasl_extensions: Vec<(String, String)>,
    http: HttpClient,
}

impl std::fmt::Debug for OidcTokenProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OidcTokenProvider")
            .field("token_endpoint", &self.token_endpoint)
            .field("credentials", &self.credentials)
            .field("client_id", &self.client_id)
            .field("scope", &self.scope)
            .field("form_parameters", &self.form_parameters.len())
            .field("sasl_extensions", &self.sasl_extensions.len())
            .finish()
    }
}

impl OidcTokenProvider {
    /// Start building a provider for `token_endpoint`.
    pub fn builder(token_endpoint: impl Into<String>) -> OidcTokenProviderBuilder {
        OidcTokenProviderBuilder {
            token_endpoint: token_endpoint.into(),
            credentials: None,
            client_id: None,
            scope: None,
            form_parameters: Vec::new(),
            sasl_extensions: Vec::new(),
            request_timeout: None,
            trust: TlsConfig::new(),
        }
    }

    /// Build the `application/x-www-form-urlencoded` request body and the
    /// optional `Authorization` header for one token request.
    ///
    /// Every buffer that holds the client secret or the assertion is sized
    /// exactly before it is written, so no reallocation frees a copy, and is
    /// zeroized on drop.
    async fn build_request(&self) -> Result<(Zeroizing<String>, Option<Zeroizing<String>>)> {
        let mut pairs: Vec<(&str, &str)> = vec![("grant_type", "client_credentials")];

        let jwt;
        let auth_header = match &self.credentials {
            ClientCredentials::Secret {
                client_id,
                client_secret,
            } => {
                // RFC 6749 §2.3.1: the client id and secret are form-urlencoded
                // *before* being joined and base64'd, so a secret containing
                // `:` cannot be misread as a field separator.
                let mut raw = Zeroizing::new(String::with_capacity(
                    form_urlencoded_len(client_id) + 1 + form_urlencoded_len(client_secret),
                ));
                form_urlencode_into(&mut raw, client_id);
                raw.push(':');
                form_urlencode_into(&mut raw, client_secret);
                // `base64_encode` allocates its output at its exact length.
                let encoded = Zeroizing::new(base64_encode(raw.as_bytes()));
                let mut header = Zeroizing::new(String::with_capacity(6 + encoded.len()));
                header.push_str("Basic ");
                header.push_str(&encoded);
                Some(header)
            }
            ClientCredentials::Assertion { source } => {
                jwt = source.resolve().await?;
                pairs.push(("client_assertion_type", JWT_BEARER_ASSERTION_TYPE));
                pairs.push(("client_assertion", &jwt));
                None
            }
        };

        // `client_id` is optional alongside an assertion (the `sub`/`iss`
        // claims usually carry it) and redundant alongside Basic auth, but some
        // providers require it in the body either way.
        if let Some(client_id) = &self.client_id {
            pairs.push(("client_id", client_id));
        }
        if let Some(scope) = &self.scope {
            pairs.push(("scope", scope));
        }
        for (key, value) in &self.form_parameters {
            pairs.push((key, value));
        }

        Ok((encode_form(&pairs), auth_header))
    }

    async fn fetch_token(&self) -> Result<OAuthBearerToken> {
        let (form, auth_header) = self.build_request().await?;

        let response = self
            .http
            .post_form(
                &self.token_endpoint,
                form.as_bytes(),
                auth_header.as_ref().map(|h| h.as_str()),
            )
            .await
            .map_err(|e| self.endpoint_error(e))?;

        if !(200..300).contains(&response.status) {
            // RFC 6749 §5.2 error bodies are small JSON objects naming the
            // failure (`invalid_client`, `invalid_scope`, …). Surfacing that
            // beats "HTTP 400", which tells an operator nothing about which of
            // half a dozen settings is wrong.
            let detail = describe_oauth_error(&response.body);
            return Err(KrafkaError::auth(format!(
                "token endpoint {} returned HTTP {}{detail}",
                self.token_endpoint, response.status
            )));
        }

        let mut parsed: TokenResponse = serde_json::from_slice(&response.body).map_err(|e| {
            KrafkaError::auth(format!(
                "token endpoint {} returned a body that is not a valid OAuth token \
                 response: {e}",
                self.token_endpoint
            ))
        })?;

        if parsed.access_token.is_empty() {
            return Err(KrafkaError::auth(format!(
                "token endpoint {} returned an empty access_token",
                self.token_endpoint
            )));
        }

        // Moves the buffer out without copying; the token zeroizes it on drop.
        let mut token = OAuthBearerToken::new(std::mem::take(&mut *parsed.access_token));

        match parsed.expires_in {
            Some(seconds) if seconds > 0 => {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_err(|_| KrafkaError::auth("system clock predates Unix epoch"))?
                    .as_millis();
                let expiry_ms = i64::try_from(now_ms)
                    .ok()
                    .and_then(|now| seconds.checked_mul(1000).and_then(|d| now.checked_add(d)));
                match expiry_ms {
                    Some(ms) => token = token.with_lifetime_ms(ms),
                    None => warn!(
                        expires_in = seconds,
                        "token endpoint reported an expires_in that overflows i64 \
                         milliseconds; treating the token as having no known expiry"
                    ),
                }
            }
            Some(seconds) => warn!(
                expires_in = seconds,
                "token endpoint reported a non-positive expires_in; treating the token \
                 as having no known expiry"
            ),
            None => debug!(
                "token endpoint returned no expires_in; the token will be re-fetched on \
                 the provider store's unknown-expiry schedule"
            ),
        }

        // SASL extensions are deliberately *not* the same list as the form
        // parameters: an audience hint sent to the identity provider is not
        // something Kafka should see, and a Confluent Cloud `logicalCluster` is
        // not something the identity provider should see.
        for (key, value) in &self.sasl_extensions {
            token = token.with_extension(key, value);
        }

        token.validate()?;
        Ok(token)
    }
}

impl OidcTokenProvider {
    /// Name the endpoint in a transport or TLS failure; the HTTP client's own
    /// message names only the host.
    fn endpoint_error(&self, error: KrafkaError) -> KrafkaError {
        match error {
            KrafkaError::Auth { message, source } => KrafkaError::Auth {
                message: format!("token endpoint {}: {message}", self.token_endpoint),
                source,
            },
            other => other,
        }
    }
}

impl CredentialProvider<OAuthBearerToken> for OidcTokenProvider {
    async fn credentials(&self) -> Result<OAuthBearerToken> {
        self.fetch_token().await
    }
}

/// Builder for [`OidcTokenProvider`].
#[must_use = "builders do nothing until .build() is called"]
#[derive(Debug)]
pub struct OidcTokenProviderBuilder {
    token_endpoint: String,
    credentials: Option<ClientCredentials>,
    client_id: Option<String>,
    scope: Option<String>,
    form_parameters: Vec<(String, String)>,
    sasl_extensions: Vec<(String, String)>,
    request_timeout: Option<Duration>,
    /// Trust store for the token endpoint; only its CA and native-roots
    /// settings are set.
    trust: TlsConfig,
}

impl OidcTokenProviderBuilder {
    /// Set how the client authenticates to the token endpoint. Required.
    pub fn credentials(mut self, credentials: ClientCredentials) -> Self {
        self.credentials = Some(credentials);
        self
    }

    /// Send `client_id` as a form parameter.
    ///
    /// Redundant with [`ClientCredentials::secret`] (which already carries it
    /// in the Basic header) but required by some providers alongside a client
    /// assertion.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Request a specific OAuth scope.
    pub fn scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }

    /// Add an extra form parameter to the token request.
    ///
    /// For provider-specific extensions such as `audience` or `resource`.
    pub fn form_parameter(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.form_parameters.push((key.into(), value.into()));
        self
    }

    /// Attach a SASL extension to every token this provider issues.
    ///
    /// These travel in the OAUTHBEARER exchange with **Kafka**, not in the
    /// token request to the identity provider — the two are different
    /// audiences, and conflating them leaks each side's routing hints to the
    /// other. Confluent Cloud requires `logicalCluster` and `identityPoolId`
    /// here.
    ///
    /// ```rust,no_run
    /// # use krafka::auth::oidc::{ClientCredentials, OidcTokenProvider};
    /// # fn f() -> Result<(), krafka::error::KrafkaError> {
    /// OidcTokenProvider::builder("https://idp.example.com/token")
    ///     .credentials(ClientCredentials::secret("id", "secret"))
    ///     .sasl_extension("logicalCluster", "lkc-123")
    ///     .sasl_extension("identityPoolId", "pool-456")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn sasl_extension(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.sasl_extensions.push((key.into(), value.into()));
        self
    }

    /// Bound one token request (connect + TLS + write + read).
    ///
    /// Defaults to the shared HTTP client default. Worth lowering: this call
    /// sits on the connection path, so a hung identity provider otherwise
    /// delays every reconnect.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Trust only the PEM CA bundle at `path` for the token endpoint
    /// (pinning), instead of the default WebPKI roots.
    ///
    /// Independent of the Kafka TLS settings: an identity provider and the
    /// brokers usually have different issuers. The rule is the same as
    /// [`TlsConfig::with_ca_cert`]: the bundle replaces the defaults, and
    /// combined with [`native_roots`](Self::native_roots) the two add up.
    ///
    /// ```rust,no_run
    /// # use krafka::auth::oidc::{ClientCredentials, OidcTokenProvider};
    /// # fn f() -> Result<(), krafka::error::KrafkaError> {
    /// OidcTokenProvider::builder("https://keycloak.internal/realms/kafka/protocol/openid-connect/token")
    ///     .credentials(ClientCredentials::secret("id", "secret"))
    ///     .ca_cert("/etc/pki/internal-ca.pem")
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn ca_cert(mut self, path: impl Into<String>) -> Self {
        self.trust.ca_cert_path = Some(path.into());
        self
    }

    /// Trust the platform's root store for the token endpoint, instead of the
    /// default WebPKI roots. With [`ca_cert`](Self::ca_cert), both are trusted.
    #[cfg(feature = "native-tls-roots")]
    #[cfg_attr(docsrs, doc(cfg(feature = "native-tls-roots")))]
    pub fn native_roots(mut self) -> Self {
        self.trust.use_native_roots = true;
        self
    }

    /// Validate and build the provider.
    ///
    /// # Errors
    ///
    /// Returns [`KrafkaError::Config`] if the endpoint is empty, is not an
    /// absolute `http`/`https` URL, if no credentials were supplied, or if the
    /// trust store cannot be loaded (an unreadable [`ca_cert`](Self::ca_cert)
    /// bundle, or no usable native roots).
    ///
    /// A plain-`http` endpoint is **rejected**: the request carries either a
    /// client secret or a signed assertion, and the response carries an access
    /// token. None of the three may cross the network in cleartext.
    pub fn build(self) -> Result<OidcTokenProvider> {
        if self.token_endpoint.is_empty() {
            return Err(KrafkaError::config("OIDC token_endpoint must not be empty"));
        }
        if self.token_endpoint.starts_with("http://") {
            return Err(KrafkaError::config(format!(
                "OIDC token endpoint {} uses plain HTTP; the request carries a client \
                 credential and the response carries an access token, so https is \
                 required",
                self.token_endpoint
            )));
        }
        if !self.token_endpoint.starts_with("https://") {
            return Err(KrafkaError::config(format!(
                "OIDC token endpoint {} is not an absolute https URL",
                self.token_endpoint
            )));
        }
        let credentials = self.credentials.ok_or_else(|| {
            KrafkaError::config(
                "OIDC token provider needs credentials: ClientCredentials::secret(..) \
                 for KIP-768, or ClientCredentials::assertion(..) for KIP-1258",
            )
        })?;

        Ok(OidcTokenProvider {
            token_endpoint: self.token_endpoint,
            credentials,
            client_id: self.client_id,
            scope: self.scope,
            form_parameters: self.form_parameters,
            sasl_extensions: self.sasl_extensions,
            http: HttpClient::new(
                Arc::new(super::tls::build_tls_config_sync(&self.trust)?),
                self.request_timeout,
                MAX_TOKEN_RESPONSE_BYTES,
            ),
        })
    }
}

/// A successful RFC 6749 §5.1 token response.
#[derive(serde::Deserialize)]
struct TokenResponse {
    #[serde(deserialize_with = "zeroizing_string")]
    access_token: Zeroizing<String>,
    #[serde(default)]
    expires_in: Option<i64>,
}

fn zeroizing_string<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Zeroizing<String>, D::Error> {
    <String as serde::Deserialize>::deserialize(deserializer).map(Zeroizing::new)
}

/// Extract the RFC 6749 §5.2 `error` / `error_description` from a failure body.
///
/// Returns a leading-space-prefixed fragment ready to append to a message, or
/// an empty string when the body is not a recognisable OAuth error.
fn describe_oauth_error(body: &[u8]) -> String {
    #[derive(serde::Deserialize)]
    struct OAuthError {
        error: Option<String>,
        error_description: Option<String>,
    }

    let Ok(parsed) = serde_json::from_slice::<OAuthError>(body) else {
        return String::new();
    };
    match (parsed.error, parsed.error_description) {
        (Some(code), Some(description)) => format!(" — {code}: {description}"),
        (Some(code), None) => format!(" — {code}"),
        (None, Some(description)) => format!(" — {description}"),
        (None, None) => String::new(),
    }
}

/// Encode `pairs` as an `application/x-www-form-urlencoded` body, in a buffer
/// allocated once at its final length and zeroized on drop.
fn encode_form(pairs: &[(&str, &str)]) -> Zeroizing<String> {
    let len = pairs
        .iter()
        .map(|(k, v)| form_urlencoded_len(k) + 1 + form_urlencoded_len(v))
        .sum::<usize>()
        + pairs.len().saturating_sub(1);
    let mut form = Zeroizing::new(String::with_capacity(len));
    for (i, (key, value)) in pairs.iter().enumerate() {
        if i > 0 {
            form.push('&');
        }
        form_urlencode_into(&mut form, key);
        form.push('=');
        form_urlencode_into(&mut form, value);
    }
    form
}

fn is_unreserved(byte: u8) -> bool {
    matches!(byte, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~')
}

/// Length of `value` once [`form_urlencode_into`] has encoded it.
fn form_urlencoded_len(value: &str) -> usize {
    value
        .bytes()
        .map(|b| if is_unreserved(b) || b == b' ' { 1 } else { 3 })
        .sum()
}

/// Percent-encode a value for `application/x-www-form-urlencoded`, appending
/// to `out`.
///
/// Anything outside the RFC 3986 unreserved set is escaped, and a space becomes
/// `+` per the HTML form encoding Kafka's own OAuth clients use. Encoding by
/// allow-list rather than by escaping a deny-list means a JWT's `.` separators,
/// a secret's `&`, and any non-ASCII byte are all handled without a special
/// case — and a value can never terminate its own field.
fn form_urlencode_into(out: &mut String, value: &str) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in value.bytes() {
        if is_unreserved(byte) {
            out.push(char::from(byte));
        } else if byte == b' ' {
            out.push('+');
        } else {
            out.push('%');
            out.push(char::from(HEX[usize::from(byte >> 4)]));
            out.push(char::from(HEX[usize::from(byte & 0x0F)]));
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    // ── form encoding ────────────────────────────────────────────────────

    fn form_urlencode(value: &str) -> String {
        let mut out = String::new();
        form_urlencode_into(&mut out, value);
        assert_eq!(
            out.len(),
            form_urlencoded_len(value),
            "pre-sizing for {value:?}"
        );
        out
    }

    /// The buffer is allocated once: a reallocation would free a copy of
    /// the secret without zeroizing it.
    #[test]
    fn the_form_is_allocated_at_its_final_length() {
        let form = encode_form(&[("a b", "x&y"), ("client_assertion", "h.p.s"), ("k", "ü")]);
        assert_eq!(&*form, "a+b=x%26y&client_assertion=h.p.s&k=%C3%BC");
        assert_eq!(form.capacity(), form.len());
    }

    /// Every byte outside the unreserved set must be escaped. A secret
    /// containing `&` or `=` that escaped unencoded would inject extra form
    /// fields — the form-encoding equivalent of the HTTP request splitting a
    /// previous review found in the schema-registry path.
    #[test]
    fn form_urlencode_escapes_separators() {
        assert_eq!(form_urlencode("a&b=c"), "a%26b%3Dc");
        assert_eq!(form_urlencode("plain-value_1.0~x"), "plain-value_1.0~x");
        assert_eq!(form_urlencode("a b"), "a+b");
        assert_eq!(form_urlencode("sl/ash"), "sl%2Fash");
        assert_eq!(form_urlencode("ü"), "%C3%BC");
        assert_eq!(form_urlencode("nl\n"), "nl%0A");
    }

    /// A JWT is `base64url.base64url.base64url`; every character in that
    /// alphabet is unreserved, so a well-formed assertion passes through
    /// unchanged and stays byte-identical to what was signed.
    #[test]
    fn form_urlencode_leaves_a_jwt_intact() {
        let jwt = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhYmMtMTIzIn0.c2lnbmF0dXJl-_x";
        assert_eq!(form_urlencode(jwt), jwt);
    }

    // ── request construction ─────────────────────────────────────────────

    fn provider(credentials: ClientCredentials) -> OidcTokenProvider {
        OidcTokenProvider::builder("https://idp.example.com/token")
            .credentials(credentials)
            .build()
            .expect("valid provider")
    }

    #[tokio::test]
    async fn secret_credentials_use_http_basic() {
        let p = provider(ClientCredentials::secret("id", "secret"));
        let (form, auth) = p.build_request().await.unwrap();

        assert_eq!(&*form, "grant_type=client_credentials");
        let auth = auth.expect("Basic header present");
        assert!(auth.starts_with("Basic "), "got: {}", *auth);
        // base64("id:secret")
        assert_eq!(&*auth, "Basic aWQ6c2VjcmV0");
    }

    /// RFC 6749 §2.3.1 form-urlencodes each half *before* joining, so a secret
    /// containing a colon cannot be misparsed as a field separator by the
    /// authorization server.
    #[tokio::test]
    async fn secret_with_colon_is_encoded_before_joining() {
        let p = provider(ClientCredentials::secret("id", "pa:ss"));
        let (_, auth) = p.build_request().await.unwrap();
        let auth = auth.unwrap();
        let encoded = auth.strip_prefix("Basic ").unwrap();
        let decoded = base64_decode_for_test(encoded);
        assert_eq!(decoded, "id:pa%3Ass");
    }

    #[tokio::test]
    async fn assertion_credentials_use_the_jwt_bearer_type() {
        let jwt = "header.payload.signature";
        let p = provider(ClientCredentials::assertion(AssertionSource::fixed(jwt)));
        let (form, auth) = p.build_request().await.unwrap();

        assert!(auth.is_none(), "assertion flow sends no Basic header");
        assert!(form.starts_with("grant_type=client_credentials"));
        // The URN's colons must be percent-encoded, or the value would end at
        // the first `:` the authorization server's parser disagreed about.
        assert!(
            form.contains(
                "client_assertion_type=\
                 urn%3Aietf%3Aparams%3Aoauth%3Aclient-assertion-type%3Ajwt-bearer"
            ),
            "got: {}",
            *form
        );
        assert!(
            form.contains(&format!("client_assertion={jwt}")),
            "got: {}",
            *form
        );
    }

    #[tokio::test]
    async fn scope_and_client_id_are_appended() {
        let p = OidcTokenProvider::builder("https://idp.example.com/token")
            .credentials(ClientCredentials::assertion(AssertionSource::fixed(
                "a.b.c",
            )))
            .client_id("my client")
            .scope("kafka:write kafka:read")
            .form_parameter("audience", "kafka")
            .build()
            .unwrap();
        let (form, _) = p.build_request().await.unwrap();
        assert!(form.contains("&client_id=my+client"), "got: {}", *form);
        assert!(
            form.contains("&scope=kafka%3Awrite+kafka%3Aread"),
            "got: {}",
            *form
        );
        assert!(form.contains("&audience=kafka"), "got: {}", *form);
    }

    // ── assertion sources ────────────────────────────────────────────────

    #[tokio::test]
    async fn file_assertion_is_trimmed() {
        let dir = std::env::temp_dir().join(format!("krafka-oidc-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("assertion.jwt");
        std::fs::write(&path, "  a.b.c\n").unwrap();

        let source = AssertionSource::file(path.clone());
        assert_eq!(&*source.resolve().await.unwrap(), "a.b.c");

        std::fs::remove_file(&path).ok();
    }

    /// A sidecar that truncates before rewriting leaves an empty file for a
    /// moment. The error must name that, not surface as an opaque
    /// `invalid_client` from the identity provider.
    #[tokio::test]
    async fn empty_assertion_file_names_the_rotation_window() {
        let dir = std::env::temp_dir().join(format!("krafka-oidc-empty-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("assertion.jwt");
        std::fs::write(&path, "\n  \n").unwrap();

        let err = AssertionSource::file(path.clone())
            .resolve()
            .await
            .expect_err("empty file must error");
        assert!(err.to_string().contains("empty"), "got: {err}");

        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn missing_assertion_file_names_the_path() {
        let path = PathBuf::from("/nonexistent/krafka/assertion.jwt");
        let err = AssertionSource::file(path)
            .resolve()
            .await
            .expect_err("missing file must error");
        assert!(err.to_string().contains("assertion.jwt"), "got: {err}");
    }

    #[tokio::test]
    async fn callback_assertion_is_invoked_each_time() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let source = AssertionSource::provider(move || {
            let counter = counter.clone();
            async move {
                let n = counter.fetch_add(1, Ordering::SeqCst);
                Ok(format!("jwt-{n}"))
            }
        });

        assert_eq!(&*source.resolve().await.unwrap(), "jwt-0");
        assert_eq!(&*source.resolve().await.unwrap(), "jwt-1");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    // ── builder validation ───────────────────────────────────────────────

    /// The token request carries a credential and the response carries an
    /// access token; neither may travel in cleartext.
    #[test]
    fn plain_http_endpoint_is_rejected() {
        let err = OidcTokenProvider::builder("http://idp.example.com/token")
            .credentials(ClientCredentials::secret("id", "secret"))
            .build()
            .expect_err("http must be rejected")
            .to_string();
        assert!(err.contains("https is required"), "got: {err}");
    }

    #[test]
    fn relative_endpoint_is_rejected() {
        assert!(
            OidcTokenProvider::builder("/token")
                .credentials(ClientCredentials::secret("id", "secret"))
                .build()
                .is_err()
        );
        assert!(
            OidcTokenProvider::builder("")
                .credentials(ClientCredentials::secret("id", "secret"))
                .build()
                .is_err()
        );
    }

    #[test]
    fn missing_credentials_are_rejected() {
        let err = OidcTokenProvider::builder("https://idp.example.com/token")
            .build()
            .expect_err("credentials are required")
            .to_string();
        assert!(err.contains("ClientCredentials"), "got: {err}");
    }

    /// Neither the secret nor the assertion may appear in a log line.
    #[test]
    fn debug_redacts_the_credential() {
        let secret = format!(
            "{:?}",
            provider(ClientCredentials::secret("id", "super-secret"))
        );
        assert!(!secret.contains("super-secret"), "got: {secret}");

        let assertion = format!("{:?}", AssertionSource::fixed("a.b.c"));
        assert!(!assertion.contains("a.b.c"), "got: {assertion}");
        assert!(assertion.contains("REDACTED"), "got: {assertion}");
    }

    /// SASL extensions must reach the issued token, and must **not** leak into
    /// the token-endpoint form: the identity provider and Kafka are different
    /// audiences.
    #[tokio::test]
    async fn sasl_extensions_do_not_leak_into_the_token_request() {
        let p = OidcTokenProvider::builder("https://idp.example.com/token")
            .credentials(ClientCredentials::secret("id", "secret"))
            .sasl_extension("logicalCluster", "lkc-123")
            .form_parameter("audience", "kafka")
            .build()
            .unwrap();

        let (form, _) = p.build_request().await.unwrap();
        assert!(form.contains("&audience=kafka"), "got: {}", *form);
        assert!(
            !form.contains("logicalCluster"),
            "SASL extensions must not be sent to the identity provider: {}",
            *form
        );
        assert_eq!(p.sasl_extensions.len(), 1);
    }

    // ── error surfacing ──────────────────────────────────────────────────

    /// RFC 6749 §5.2 error bodies name which setting is wrong. "HTTP 400" does
    /// not.
    #[test]
    fn oauth_error_body_is_surfaced() {
        let body = br#"{"error":"invalid_client","error_description":"unknown client id"}"#;
        assert_eq!(
            describe_oauth_error(body),
            " — invalid_client: unknown client id"
        );

        let code_only = br#"{"error":"invalid_scope"}"#;
        assert_eq!(describe_oauth_error(code_only), " — invalid_scope");
    }

    /// An HTML error page from a proxy must not produce a confusing parse
    /// failure in the error path itself.
    #[test]
    fn non_json_error_body_degrades_quietly() {
        assert_eq!(describe_oauth_error(b"<html>502 Bad Gateway</html>"), "");
        assert_eq!(describe_oauth_error(b""), "");
    }

    // ── token response parsing ───────────────────────────────────────────

    #[test]
    fn token_response_expires_in_is_optional() {
        let with: TokenResponse =
            serde_json::from_slice(br#"{"access_token":"t","expires_in":3600}"#).unwrap();
        assert_eq!(*with.access_token, "t");
        assert_eq!(with.expires_in, Some(3600));

        let without: TokenResponse =
            serde_json::from_slice(br#"{"access_token":"t","token_type":"Bearer"}"#).unwrap();
        assert_eq!(without.expires_in, None);
    }

    fn base64_decode_for_test(input: &str) -> String {
        use base64::Engine as _;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(input)
            .expect("valid base64");
        String::from_utf8(bytes).expect("valid utf8")
    }

    // ── the token endpoint over HTTPS ────────────────────────────────────

    fn testdata(name: &str) -> String {
        format!("{}/src/auth/testdata/{name}", env!("CARGO_MANIFEST_DIR"))
    }

    /// Serve one HTTPS request on 127.0.0.1 with `server.pem` (issued by the
    /// test CA `ca.pem`), answering with `response`. Returns the endpoint URL.
    async fn token_endpoint(response: Vec<u8>) -> String {
        use rustls::pki_types::pem::PemObject;
        use rustls::pki_types::{CertificateDer, PrivateKeyDer};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let certs = CertificateDer::pem_file_iter(testdata("server.pem"))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let key = PrivateKeyDer::from_pem_file(testdata("server.key")).unwrap();
        let config = rustls::ServerConfig::builder_with_provider(
            crate::auth::tls::resolve_crypto_provider(),
        )
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(config));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let Ok(mut tls) = acceptor.accept(tcp).await else {
                return;
            };
            let mut request = Vec::new();
            let mut buf = [0u8; 4096];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                match tls.read(&mut buf).await {
                    Ok(0) | Err(_) => return,
                    Ok(n) => request.extend_from_slice(&buf[..n]),
                }
            }
            let _ = tls.write_all(&response).await;
            let _ = tls.shutdown().await;
        });
        format!("https://127.0.0.1:{port}/token")
    }

    fn ok_response(body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    #[tokio::test]
    async fn an_endpoint_behind_a_private_ca_is_reachable_with_that_ca() {
        let endpoint =
            token_endpoint(ok_response(r#"{"access_token":"tok-1","expires_in":60}"#)).await;
        let token = OidcTokenProvider::builder(&endpoint)
            .credentials(ClientCredentials::secret("id", "secret"))
            .ca_cert(testdata("ca.pem"))
            .build()
            .unwrap()
            .fetch_token()
            .await
            .expect("the pinned CA must verify the endpoint");
        let initial = token.to_gs2_initial_response();
        assert!(
            initial.windows(17).any(|w| w == b"auth=Bearer tok-1"),
            "the fetched token must be the endpoint's"
        );
    }

    /// Negative control: the default WebPKI roots do not know the test CA.
    #[tokio::test]
    async fn without_its_ca_the_endpoint_fails_verification() {
        let endpoint = token_endpoint(ok_response(r#"{"access_token":"tok-1"}"#)).await;
        let err = OidcTokenProvider::builder(&endpoint)
            .credentials(ClientCredentials::secret("id", "secret"))
            .build()
            .unwrap()
            .fetch_token()
            .await
            .expect_err("an unknown issuer must be rejected")
            .to_string();
        assert!(err.contains(&endpoint), "got: {err}");
        assert!(err.contains("UnknownIssuer"), "got: {err}");
    }

    #[test]
    fn an_unreadable_ca_bundle_fails_the_build() {
        let err = OidcTokenProvider::builder("https://idp.example.com/token")
            .credentials(ClientCredentials::secret("id", "secret"))
            .ca_cert("/nonexistent/krafka/ca.pem")
            .build()
            .expect_err("a missing CA bundle must not fall back to other roots")
            .to_string();
        assert!(err.contains("/nonexistent/krafka/ca.pem"), "got: {err}");
    }

    /// The token cap reaches the HTTP client: a 2 MiB body fails at the
    /// 1 MiB limit instead of being buffered and rejected afterwards.
    #[tokio::test]
    async fn an_oversized_token_response_stops_at_the_token_cap() {
        let mut response = b"HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n".to_vec();
        response.extend(std::iter::repeat_n(b' ', 2 * MAX_TOKEN_RESPONSE_BYTES));
        let endpoint = token_endpoint(response).await;
        let err = OidcTokenProvider::builder(&endpoint)
            .credentials(ClientCredentials::secret("id", "secret"))
            .ca_cert(testdata("ca.pem"))
            .build()
            .unwrap()
            .fetch_token()
            .await
            .expect_err("a 2 MiB token response must be refused")
            .to_string();
        assert!(
            err.contains(&format!("exceeds {MAX_TOKEN_RESPONSE_BYTES}-byte limit")),
            "got: {err}"
        );
    }
}
