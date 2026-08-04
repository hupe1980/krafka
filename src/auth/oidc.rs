//! Built-in OIDC token provider for SASL/OAUTHBEARER.
//!
//! Implements the OAuth 2.0 `client_credentials` grant against an OIDC token
//! endpoint — the flow Apache Kafka added in KIP-768 and `librdkafka` exposes
//! as `sasl.oauthbearer.method=oidc` — plus the RFC 7523 **client assertion**
//! variant that Kafka 4.3 added in KIP-1258.
//!
//! # Why this exists
//!
//! [`OAuthBearerTokenProvider`] has always let an application supply tokens,
//! but that made every krafka user write their own OAuth client: an HTTPS POST,
//! form encoding, JSON parsing, `expires_in` arithmetic and a retry policy.
//! Every other major Kafka client ships that flow. This is it.
//!
//! # Two ways to authenticate to the token endpoint
//!
//! | Method | Config | What is sent |
//! |---|---|---|
//! | Client secret (KIP-768) | [`ClientCredentials::secret`] | HTTP Basic `client_id:client_secret` |
//! | Client assertion (KIP-1258, RFC 7523) | [`ClientCredentials::assertion`] | `client_assertion_type` + a signed JWT |
//!
//! Client assertion is the stronger of the two: the credential on the wire is a
//! short-lived signature rather than a long-lived shared secret, and the
//! private key never leaves the workload. It is also what makes krafka usable
//! with identity providers that refuse `client_secret_basic` outright.
//!
//! # No cryptography dependency
//!
//! krafka does **not** sign the assertion for you, and deliberately so. Signing
//! needs RSA or ECDSA, and pinning a specific implementation on every user of a
//! Kafka client is a supply-chain decision that belongs to the application.
//!
//! Instead the signed JWT is *sourced*:
//!
//! - [`AssertionSource::File`] — read from disk on every token request. This is
//!   the cloud-native path: a SPIFFE agent, a Vault sidecar or a projected
//!   Kubernetes service-account token writes the file and rotates it, and
//!   krafka picks up each new value without a restart. It is also exactly what
//!   Kafka's own `sasl.oauthbearer.assertion.file` does.
//! - [`AssertionSource::Callback`] — you produce the JWT, signing it with
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
//!         AssertionSource::File("/var/run/secrets/oauth/assertion.jwt".into()),
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

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, warn};
use zeroize::Zeroizing;

use crate::error::{KrafkaError, Result};
use crate::http::{HttpClient, base64_encode};

use super::oauthbearer::{OAuthBearerToken, OAuthBearerTokenProvider};

/// RFC 7523 §2.2 client-assertion type, sent verbatim as a form parameter.
const JWT_BEARER_ASSERTION_TYPE: &str = "urn:ietf:params:oauth:client-assertion-type:jwt-bearer";

/// Largest token-endpoint response body accepted, in bytes.
///
/// A token response is a small JSON object; real ones run to a few kilobytes
/// even with a large JWT. The cap stops a hostile or misconfigured endpoint —
/// or an HTML error page from a captive portal — from being buffered without
/// bound on every reconnect.
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
pub enum AssertionSource {
    /// Read the JWT from a file, on **every** token request.
    ///
    /// Re-reading rather than caching is the point: a SPIFFE agent, Vault
    /// sidecar or projected Kubernetes service-account token rewrites the file
    /// as the assertion rotates, and a cached value would keep presenting a
    /// dead credential until the process restarted.
    ///
    /// Surrounding whitespace (a trailing newline from `echo`, say) is
    /// trimmed.
    File(PathBuf),

    /// A fixed, pre-signed JWT.
    ///
    /// Only useful for tests and short-lived jobs: assertions are meant to be
    /// short-lived, so a static one becomes a permanent authentication failure
    /// the moment it expires.
    Static(Zeroizing<String>),

    /// Produce the JWT on demand, signing it however you like.
    ///
    /// Called on every token request, so it must be cheap or do its own
    /// caching.
    #[allow(clippy::type_complexity)]
    Callback(Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<String>> + Send>> + Send + Sync>),
}

impl std::fmt::Debug for AssertionSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::File(path) => f.debug_tuple("File").field(path).finish(),
            Self::Static(_) => f.debug_tuple("Static").field(&"[REDACTED]").finish(),
            Self::Callback(_) => f.write_str("Callback(<fn>)"),
        }
    }
}

impl AssertionSource {
    /// Resolve the current signed JWT.
    async fn resolve(&self) -> Result<Zeroizing<String>> {
        match self {
            Self::Static(jwt) => Ok(jwt.clone()),
            Self::Callback(f) => Ok(Zeroizing::new(f().await?)),
            Self::File(path) => {
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

/// An [`OAuthBearerTokenProvider`] that fetches access tokens from an OIDC
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
        }
    }

    /// Build the `application/x-www-form-urlencoded` request body and the
    /// optional `Authorization` header for one token request.
    async fn build_request(&self) -> Result<(Zeroizing<String>, Option<Zeroizing<String>>)> {
        let mut form = String::from("grant_type=client_credentials");

        let auth_header = match &self.credentials {
            ClientCredentials::Secret {
                client_id,
                client_secret,
            } => {
                // RFC 6749 §2.3.1: the client id and secret are form-urlencoded
                // *before* being joined and base64'd, so a secret containing
                // `:` cannot be misread as a field separator.
                let raw = format!(
                    "{}:{}",
                    form_urlencode(client_id),
                    form_urlencode(client_secret)
                );
                Some(Zeroizing::new(format!(
                    "Basic {}",
                    base64_encode(raw.as_bytes())
                )))
            }
            ClientCredentials::Assertion { source } => {
                let jwt = source.resolve().await?;
                form.push_str("&client_assertion_type=");
                form.push_str(&form_urlencode(JWT_BEARER_ASSERTION_TYPE));
                form.push_str("&client_assertion=");
                form.push_str(&form_urlencode(&jwt));
                None
            }
        };

        // `client_id` is optional alongside an assertion (the `sub`/`iss`
        // claims usually carry it) and redundant alongside Basic auth, but some
        // providers require it in the body either way.
        if let Some(client_id) = &self.client_id {
            form.push_str("&client_id=");
            form.push_str(&form_urlencode(client_id));
        }
        if let Some(scope) = &self.scope {
            form.push_str("&scope=");
            form.push_str(&form_urlencode(scope));
        }
        for (key, value) in &self.form_parameters {
            form.push('&');
            form.push_str(&form_urlencode(key));
            form.push('=');
            form.push_str(&form_urlencode(value));
        }

        Ok((Zeroizing::new(form), auth_header))
    }

    async fn fetch_token(&self) -> Result<OAuthBearerToken> {
        let (form, auth_header) = self.build_request().await?;

        let response = self
            .http
            .request(
                "POST",
                &self.token_endpoint,
                &[
                    ("Content-Type", "application/x-www-form-urlencoded"),
                    ("Accept", "application/json"),
                ],
                Some(form.as_bytes()),
                auth_header.as_ref().map(|h| h.as_str()),
            )
            .await?;

        if response.body.len() > MAX_TOKEN_RESPONSE_BYTES {
            return Err(KrafkaError::auth(format!(
                "token endpoint returned {} bytes, above the {MAX_TOKEN_RESPONSE_BYTES} byte limit",
                response.body.len()
            )));
        }

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

        let parsed: TokenResponse = serde_json::from_slice(&response.body).map_err(|e| {
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

        let mut token = OAuthBearerToken::new(parsed.access_token);

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

impl OAuthBearerTokenProvider for OidcTokenProvider {
    fn provide_token(&self) -> Pin<Box<dyn Future<Output = Result<OAuthBearerToken>> + Send + '_>> {
        Box::pin(self.fetch_token())
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

    /// Validate and build the provider.
    ///
    /// # Errors
    ///
    /// Returns [`KrafkaError::Config`] if the endpoint is empty, is not an
    /// absolute `http`/`https` URL, or if no credentials were supplied.
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
            http: HttpClient::with_webpki_roots(self.request_timeout)?,
        })
    }
}

/// A successful RFC 6749 §5.1 token response.
#[derive(serde::Deserialize)]
struct TokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<i64>,
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

/// Percent-encode a value for `application/x-www-form-urlencoded`.
///
/// Anything outside the RFC 3986 unreserved set is escaped, and a space becomes
/// `+` per the HTML form encoding Kafka's own OAuth clients use. Encoding by
/// allow-list rather than by escaping a deny-list means a JWT's `.` separators,
/// a secret's `&`, and any non-ASCII byte are all handled without a special
/// case — and a value can never terminate its own field.
fn form_urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.as_bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(*byte as char);
            }
            b' ' => out.push('+'),
            other => {
                out.push('%');
                out.push(
                    char::from_digit((other >> 4) as u32, 16)
                        .unwrap_or('0')
                        .to_ascii_uppercase(),
                );
                out.push(
                    char::from_digit((other & 0x0F) as u32, 16)
                        .unwrap_or('0')
                        .to_ascii_uppercase(),
                );
            }
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    // ── form encoding ────────────────────────────────────────────────────

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
        let p = provider(ClientCredentials::assertion(AssertionSource::Static(
            Zeroizing::new(jwt.to_string()),
        )));
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
            .credentials(ClientCredentials::assertion(AssertionSource::Static(
                Zeroizing::new("a.b.c".into()),
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

        let source = AssertionSource::File(path.clone());
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

        let err = AssertionSource::File(path.clone())
            .resolve()
            .await
            .expect_err("empty file must error");
        assert!(err.to_string().contains("empty"), "got: {err}");

        std::fs::remove_file(&path).ok();
    }

    #[tokio::test]
    async fn missing_assertion_file_names_the_path() {
        let path = PathBuf::from("/nonexistent/krafka/assertion.jwt");
        let err = AssertionSource::File(path)
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
        let source = AssertionSource::Callback(Arc::new(move || {
            let counter = counter.clone();
            Box::pin(async move {
                let n = counter.fetch_add(1, Ordering::SeqCst);
                Ok(format!("jwt-{n}"))
            })
        }));

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

        let assertion = format!(
            "{:?}",
            AssertionSource::Static(Zeroizing::new("a.b.c".into()))
        );
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
        assert_eq!(with.access_token, "t");
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
}
