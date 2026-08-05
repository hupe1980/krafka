//! SASL/OAUTHBEARER authentication (RFC 7628, KIP-255).
//!
//! Implements the SASL/OAUTHBEARER mechanism for Kafka. The client sends an
//! OAuth 2.0 bearer token in the GS2 framing format, and the server validates
//! the token against its configured OAuth/OIDC provider.
//!
//! # Token format (RFC 7628)
//!
//! The initial client response uses the GS2 framing:
//!
//! ```text
//! n,,\x01auth=Bearer <token>\x01\x01
//! ```
//!
//! Optional SASL extensions can be appended before the terminator:
//!
//! ```text
//! n,,\x01auth=Bearer <token>\x01key1=value1\x01key2=value2\x01\x01
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use krafka::auth::{AuthConfig, OAuthBearerToken};
//!
//! // Static token
//! let config = AuthConfig::sasl_oauthbearer("my-jwt-token");
//!
//! // Token with SASL extensions (e.g., Confluent Cloud)
//! let token = OAuthBearerToken::new("my-jwt-token")
//!     .with_extension("logicalCluster", "lkc-123")
//!     .with_extension("identityPoolId", "pool-456");
//! let config = AuthConfig::sasl_oauthbearer_token(token);
//!
//! // Automatic token refresh via provider (recommended for production)
//! let config = AuthConfig::sasl_oauthbearer_provider(|| async {
//!     let jwt = my_oauth_client.get_access_token().await?;
//!     Ok(OAuthBearerToken::new(jwt))
//! });
//! ```

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, warn};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::error::{KrafkaError, Result};
use crate::metrics::ConnectionMetrics;

const OAUTHBEARER_EXPIRY_SKEW_MARGIN_MS: i64 = 30_000;

/// Maximum time a token with an *unknown* expiry may be served from cache.
///
/// A token without `lifetime_ms` carries no expiry the client can reason
/// about, but it is not immortal: the real `exp` claim inside the JWT still
/// applies and the provider may rotate the credential at any time. Caching
/// such a token for the lifetime of the process means every reconnect after
/// the true expiry re-sends a dead credential — the client locks itself out
/// permanently and no amount of retrying recovers it.
///
/// Bounding the entry by wall-clock age turns that permanent failure into at
/// most one stale-token window. Five minutes is well under any realistic
/// OAuth access-token lifetime (typically 15 min – 1 h), so it costs only a
/// handful of extra provider calls per hour.
pub(crate) const OAUTHBEARER_UNKNOWN_EXPIRY_MAX_AGE: Duration = Duration::from_secs(300);

fn current_epoch_ms() -> Result<i64> {
    let d = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| KrafkaError::auth("system clock predates Unix epoch"))?;
    i64::try_from(d.as_millis()).map_err(|_| KrafkaError::auth("current epoch_ms overflows i64"))
}

/// Trait for providing fresh OAuth 2.0 bearer tokens on each broker connection.
///
/// Implement this to integrate with your OAuth/OIDC provider. The provider is
/// called on every new broker connection (including automatic reconnections),
/// ensuring tokens are always fresh.
///
/// # Examples
///
/// ```rust,ignore
/// use krafka::auth::{OAuthBearerToken, OAuthBearerTokenProvider};
/// use krafka::error::Result;
/// use std::future::Future;
/// use std::pin::Pin;
///
/// struct MyProvider { /* OAuth client */ }
///
/// impl OAuthBearerTokenProvider for MyProvider {
///     fn provide_token(&self) -> Pin<Box<dyn Future<Output = Result<OAuthBearerToken>> + Send + '_>> {
///         Box::pin(async move {
///             // Fetch a fresh token from your OAuth server
///             let jwt = my_oauth_client.get_access_token().await?;
///             Ok(OAuthBearerToken::new(jwt))
///         })
///     }
/// }
/// ```
pub trait OAuthBearerTokenProvider: Send + Sync {
    /// Fetch a fresh OAuth 2.0 bearer token.
    ///
    /// Called on every new broker connection. Implementations should handle
    /// token caching and refresh internally if desired.
    fn provide_token(&self) -> Pin<Box<dyn Future<Output = Result<OAuthBearerToken>> + Send + '_>>;
}

/// Blanket impl: any `Fn() -> Future<Output = Result<OAuthBearerToken>>` is a provider.
///
/// The `'static` bound on `Fut` is required because the trait method signature
/// uses an anonymous lifetime (`+ '_`), and the compiler cannot prove the
/// future outlives `&self` without it. In practice this is not restrictive:
/// closures that own their captured state (the common case) produce `'static`
/// futures. For borrowing patterns, implement `OAuthBearerTokenProvider`
/// directly.
impl<F, Fut> OAuthBearerTokenProvider for F
where
    F: Fn() -> Fut + Send + Sync,
    Fut: Future<Output = Result<OAuthBearerToken>> + Send + 'static,
{
    fn provide_token(&self) -> Pin<Box<dyn Future<Output = Result<OAuthBearerToken>> + Send + '_>> {
        Box::pin(self())
    }
}

/// Internal state shared across all clones of [`OAuthBearerTokenProviderHandle`].
///
/// Wrapped in `Arc` so cloning the handle shares the cache and the coalescing
/// mutex rather than creating independent instances.
struct OAuthTokenStoreInner {
    provider: Arc<dyn OAuthBearerTokenProvider>,
    /// Most recently fetched token. `None` until the first call to `provide_token`.
    cached: RwLock<Option<CachedToken>>,
    /// At most one refresh in flight at a time. Concurrent callers that find
    /// the cached token stale queue behind this lock; the first winner fetches
    /// while the rest get the result on unlock.
    refreshing: Mutex<()>,
    /// Connection metrics to report token fetches into, once a pool has bound
    /// itself. See [`OAuthBearerTokenProviderHandle::bind_metrics`].
    metrics: OnceLock<Arc<ConnectionMetrics>>,
}

impl OAuthTokenStoreInner {
    /// Fetch one token from the wrapped provider, recording the outcome.
    ///
    /// Every fetch in the crate goes through here — the on-connect resolution
    /// and the background proactive refresh both — so the counters cannot
    /// cover one path and miss the other.
    ///
    /// The `warn!` on failure is the load-bearing part even without metrics: a
    /// misconfigured `token_endpoint` used to surface only as connection
    /// failures, with nothing anywhere naming the OAuth round trip as the
    /// cause.
    async fn fetch(&self) -> Result<OAuthBearerToken> {
        let started = Instant::now();
        match self.provider.provide_token().await {
            Ok(token) => {
                if let Some(metrics) = self.metrics.get() {
                    metrics.record_oauth_token_fetch(started.elapsed(), token.lifetime_ms());
                }
                debug!(
                    lifetime_ms = token.lifetime_ms(),
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "OAUTHBEARER token fetched"
                );
                Ok(token)
            }
            Err(e) => {
                if let Some(metrics) = self.metrics.get() {
                    metrics.record_oauth_token_fetch_failure();
                }
                warn!(
                    error = %e,
                    elapsed_ms = started.elapsed().as_millis() as u64,
                    "OAUTHBEARER token fetch failed; broker connections needing a fresh \
                     token will fail until the provider recovers"
                );
                Err(e)
            }
        }
    }
}

/// A cached token together with the instant it was fetched.
///
/// The fetch instant is what makes [`OAUTHBEARER_UNKNOWN_EXPIRY_MAX_AGE`]
/// enforceable for tokens that carry no `lifetime_ms`.
#[derive(Clone)]
struct CachedToken {
    token: OAuthBearerToken,
    fetched_at: Instant,
}

impl CachedToken {
    fn new(token: OAuthBearerToken) -> Self {
        Self {
            token,
            fetched_at: Instant::now(),
        }
    }

    /// Whether this entry must be replaced before use.
    ///
    /// Two independent conditions, either of which disqualifies the entry:
    ///
    /// - the token declares an expiry and is inside the 30 s skew window
    ///   ([`OAuthBearerToken::needs_refresh`]); or
    /// - the token declares **no** expiry and the entry is older than
    ///   [`OAUTHBEARER_UNKNOWN_EXPIRY_MAX_AGE`].
    ///
    /// The second condition is the load-bearing one. `needs_refresh()` is
    /// `lifetime_ms.is_some_and(..)`, which is `false` when `lifetime_ms` is
    /// `None` — exactly how `OAuthBearerToken::new(jwt)` builds a token. On
    /// that path alone the cache would hold one JWT forever.
    fn is_stale(&self) -> bool {
        if self.token.lifetime_ms().is_none() {
            return self.fetched_at.elapsed() >= OAUTHBEARER_UNKNOWN_EXPIRY_MAX_AGE;
        }
        self.token.needs_refresh()
    }
}

/// Handle wrapping a cached, coalescing [`OAuthBearerTokenProvider`].
///
/// All clones of a handle share the same internal [`Arc`], so they all read
/// from and write to the same token cache. A background proactive-refresh
/// task (started by [`Self::start_refresh_task`]) wakes before the token
/// expires and pre-warms the cache, ensuring that broker reconnects never
/// need to wait for a provider call.
///
/// # Token lifecycle
///
/// 1. First call to [`provide_token`](Self::provide_token) fetches and caches a token.
/// 2. Subsequent calls return the cached token until it enters the 30-second
///    expiry window (controlled by `OAUTHBEARER_EXPIRY_SKEW_MARGIN_MS`).
/// 3. The first caller that finds the token stale acquires the coalescing lock
///    and refreshes. Concurrent callers wait and receive the same new token.
/// 4. The proactive refresh task wakes at step (2) so that (3) rarely happens
///    on a connection path.
#[derive(Clone)]
pub struct OAuthBearerTokenProviderHandle(Arc<OAuthTokenStoreInner>);

impl OAuthBearerTokenProviderHandle {
    /// Create a new handle wrapping the given provider.
    pub fn new(provider: impl OAuthBearerTokenProvider + 'static) -> Self {
        Self(Arc::new(OAuthTokenStoreInner {
            provider: Arc::new(provider),
            cached: RwLock::new(None),
            refreshing: Mutex::new(()),
            metrics: OnceLock::new(),
        }))
    }

    /// Report token fetches into `metrics`.
    ///
    /// Called by [`ConnectionPool`](crate::network::ConnectionPool) when it
    /// starts the proactive-refresh task, so `oauth_token_fetches`,
    /// `oauth_token_fetch_failures`, `oauth_token_fetch_latency` and
    /// `oauth_token_expiry_epoch_ms` show up on the same
    /// [`ConnectionMetrics`] a client already exports.
    ///
    /// **First binding wins.** All clones of a handle share one store, so an
    /// [`AuthConfig`](super::AuthConfig) cloned across two pools reports into
    /// whichever bound first. Every fetch is still logged either way, and
    /// clients that share a pool — the recommended shape — share one
    /// `ConnectionMetrics` anyway.
    pub(crate) fn bind_metrics(&self, metrics: Arc<ConnectionMetrics>) {
        // `set` fails only when already bound, which is exactly the
        // first-binding-wins rule; there is nothing to report.
        let _ = self.0.metrics.set(metrics);
    }

    /// Return a fresh token, using the cache when available.
    ///
    /// Returns the cached token if it is neither inside the expiry skew margin
    /// nor — for a token with no declared expiry — older than
    /// a fixed maximum age. Otherwise acquires the
    /// coalescing refresh lock, re-checks (another task may have just
    /// refreshed), and calls the provider exactly once while concurrent
    /// callers wait.
    ///
    /// A token with no `lifetime_ms` is **never** cached indefinitely; see
    /// the cache staleness check.
    pub async fn provide_token(&self) -> Result<OAuthBearerToken> {
        // Fast path: cached token is still fresh.
        {
            let guard = self.0.cached.read().await;
            if let Some(entry) = guard.as_ref()
                && !entry.is_stale()
            {
                return Ok(entry.token.clone());
            }
        }

        // Slow path: coalesce concurrent refreshes under one lock.
        let _coalesce = self.0.refreshing.lock().await;

        // Re-check after acquiring the lock — another task may have already refreshed.
        {
            let guard = self.0.cached.read().await;
            if let Some(entry) = guard.as_ref()
                && !entry.is_stale()
            {
                return Ok(entry.token.clone());
            }
        }

        // Fetch a fresh token and update the cache.
        let token = self.0.fetch().await?;
        *self.0.cached.write().await = Some(CachedToken::new(token.clone()));
        Ok(token)
    }

    /// Test-only: seed the cache with a token whose fetch instant is
    /// backdated by `age`, so staleness can be exercised without a clock mock
    /// (tokio's `test-util` time control is not enabled for this crate).
    #[cfg(test)]
    pub(crate) async fn test_seed_cache(&self, token: OAuthBearerToken, age: Duration) {
        let fetched_at = Instant::now().checked_sub(age).unwrap_or_else(Instant::now);
        *self.0.cached.write().await = Some(CachedToken { token, fetched_at });
    }

    /// Start a background task that proactively refreshes the token before expiry.
    ///
    /// The task wakes when the cached token enters the 30-second expiry skew
    /// window and calls the provider to pre-warm the cache. This ensures that
    /// broker connections and reconnects always find a ready token without
    /// blocking on a provider call.
    ///
    /// When no token has been fetched yet, or the cached token has no
    /// `lifetime_ms`, the task re-fetches every
    /// the unknown-expiry maximum age — the same cadence on which
    /// [`Self::provide_token`] considers such an entry stale.
    ///
    /// Returns the [`JoinHandle`] so the caller can abort the task on shutdown.
    /// Must be called from within a Tokio runtime.
    pub fn start_refresh_task(&self) -> JoinHandle<()> {
        let inner = self.0.clone();
        tokio::spawn(async move {
            loop {
                // Compute how long to sleep before the next refresh window.
                let sleep_duration = {
                    let guard = inner.cached.read().await;
                    match guard.as_ref().and_then(|e| e.token.lifetime_ms()) {
                        Some(lifetime_ms) => {
                            // Wake at `lifetime_ms - OAUTHBEARER_EXPIRY_SKEW_MARGIN_MS`.
                            let wake_at_ms =
                                lifetime_ms.saturating_sub(OAUTHBEARER_EXPIRY_SKEW_MARGIN_MS);
                            let now_ms = current_epoch_ms().unwrap_or(i64::MAX);
                            let remaining_ms = wake_at_ms.saturating_sub(now_ms).max(0);
                            Duration::from_millis(remaining_ms as u64)
                        }
                        None => {
                            // No token yet, or the cached token declares no
                            // expiry. Re-fetch on the same cadence that makes
                            // an unknown-expiry entry stale, so the cache can
                            // never serve a token older than that.
                            OAUTHBEARER_UNKNOWN_EXPIRY_MAX_AGE
                        }
                    }
                };

                if !sleep_duration.is_zero() {
                    tokio::time::sleep(sleep_duration).await;
                }

                // Proactively refresh under the coalescing lock. `fetch`
                // records the outcome and logs a failure at WARN.
                let _coalesce = inner.refreshing.lock().await;
                match inner.fetch().await {
                    Ok(token) => {
                        *inner.cached.write().await = Some(CachedToken::new(token));
                    }
                    Err(_) => {
                        // Back off briefly before restarting the sleep loop
                        // so a transient provider failure doesn't busy-spin.
                        drop(_coalesce);
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        })
    }
}

impl fmt::Debug for OAuthBearerTokenProviderHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[OAuthBearerTokenProvider]")
    }
}

/// OAuth 2.0 bearer token for SASL/OAUTHBEARER authentication.
///
/// Implements the SASL/OAUTHBEARER mechanism as defined in RFC 7628 and KIP-255.
/// The token value is zeroized from memory on drop.
///
/// # Examples
///
/// ```rust
/// use krafka::auth::OAuthBearerToken;
///
/// // Simple token
/// let token = OAuthBearerToken::new("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...");
///
/// // Token with SASL extensions
/// let token = OAuthBearerToken::new("eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9...")
///     .with_extension("logicalCluster", "lkc-abc123")
///     .with_extension("identityPoolId", "pool-xyz789");
/// ```
#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct OAuthBearerToken {
    /// The OAuth 2.0 bearer token value (zeroized on drop).
    token_value: String,
    /// Optional SASL extensions (key=value pairs). Not secrets; skipped for zeroize.
    /// Uses `BTreeMap` for deterministic ordering.
    #[zeroize(skip)]
    extensions: BTreeMap<String, String>,
    /// Optional token expiry as milliseconds since the Unix epoch.
    ///
    /// When set, the client validates the token is not expired before sending
    /// it to the broker— preventing wasted authentication round-trips
    /// and potential infinite retry loops with stale tokens.
    #[zeroize(skip)]
    lifetime_ms: Option<i64>,
}

impl OAuthBearerToken {
    /// Create a new OAuth bearer token.
    ///
    /// The `token_value` should be a valid OAuth 2.0 access token, typically a JWT.
    ///
    /// # Example
    ///
    /// ```rust
    /// use krafka::auth::OAuthBearerToken;
    /// let token = OAuthBearerToken::new("my-jwt-token");
    /// ```
    pub fn new(token_value: impl Into<String>) -> Self {
        Self {
            token_value: token_value.into(),
            extensions: BTreeMap::new(),
            lifetime_ms: None,
        }
    }

    /// Add a SASL extension key-value pair.
    ///
    /// Extensions are sent as part of the initial OAUTHBEARER message and can be
    /// used for additional authentication context. For example, Confluent Cloud
    /// uses `logicalCluster` and `identityPoolId` extensions.
    ///
    /// # Example
    ///
    /// ```rust
    /// use krafka::auth::OAuthBearerToken;
    /// let token = OAuthBearerToken::new("my-jwt-token")
    ///     .with_extension("logicalCluster", "lkc-abc123");
    /// ```
    pub fn with_extension(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.extensions.insert(key.into(), value.into());
        self
    }

    /// Set the token expiry time as milliseconds since the Unix epoch.
    ///
    /// When set, the client validates the token is not expired before sending
    /// it to the broker and rejects tokens in the final 30 seconds before
    /// expiry to avoid clock-skew races during SASL/OAUTHBEARER handshakes.
    ///
    /// # Example
    ///
    /// ```rust
    /// use krafka::auth::OAuthBearerToken;
    /// let token = OAuthBearerToken::new("my-jwt-token")
    ///     .with_lifetime_ms(1700000000000); // expires at this epoch-ms
    /// ```
    pub fn with_lifetime_ms(mut self, lifetime_ms: i64) -> Self {
        self.lifetime_ms = Some(lifetime_ms);
        self
    }

    /// Returns the token expiry time in milliseconds since the Unix epoch, if set.
    pub fn lifetime_ms(&self) -> Option<i64> {
        self.lifetime_ms
    }

    /// Returns `true` if the token has a known expiry and that expiry is in the past.
    ///
    /// Returns `true` conservatively when the system clock is unavailable (fail-safe).
    pub fn is_expired(&self) -> bool {
        self.lifetime_ms
            .is_some_and(|lifetime_ms| current_epoch_ms().map_or(true, |now| now >= lifetime_ms))
    }

    /// Returns `true` if the token should be refreshed before starting a new
    /// SASL handshake.
    ///
    /// Tokens in their final 30 seconds are treated as stale even if their
    /// expiry timestamp is still in the future. This mirrors the common Kafka
    /// client practice of refreshing before the edge of expiry so broker/client
    /// clock skew does not cause avoidable authentication failures.
    ///
    /// Returns `true` conservatively when the system clock is unavailable (fail-safe).
    pub fn needs_refresh(&self) -> bool {
        self.lifetime_ms.is_some_and(|lifetime_ms| {
            current_epoch_ms().map_or(true, |now| {
                now >= lifetime_ms.saturating_sub(OAUTHBEARER_EXPIRY_SKEW_MARGIN_MS)
            })
        })
    }

    /// Validate the token value and extension key/value pairs for GS2-frame safety.
    ///
    /// The GS2 framing format (RFC 7628) uses `\x01` as a separator and requires
    /// the token value to consist of printable ASCII characters only (0x21–0x7E
    /// plus 0x20 SPACE, excluding the `\x01` control character used as a
    /// field separator). Null bytes and other control characters would silently
    /// corrupt the handshake payload and cause broker authentication failures
    /// that are difficult to diagnose.
    ///
    /// Returns `Err` if the token value or any extension key/value contains
    /// a `\x01` byte, a null byte, or any non-printable/non-ASCII character.
    pub fn validate(&self) -> crate::error::Result<()> {
        if self.token_value.is_empty() {
            return Err(crate::error::KrafkaError::auth(
                "OAuthBearer token value must not be empty",
            ));
        }
        if let Some(bad) = self
            .token_value
            .bytes()
            .find(|&b| !(0x20..=0x7E).contains(&b))
        {
            return Err(crate::error::KrafkaError::auth(format!(
                "OAuthBearer token value contains an invalid byte 0x{bad:02X}; \
                 token must consist of printable ASCII characters (0x20–0x7E) only"
            )));
        }
        for (key, value) in &self.extensions {
            for s in [key.as_str(), value.as_str()] {
                if let Some(bad) = s.bytes().find(|&b| !(0x20..=0x7E).contains(&b)) {
                    return Err(crate::error::KrafkaError::auth(format!(
                        "OAuthBearer extension key/value contains an invalid byte 0x{bad:02X}; \
                         extension strings must consist of printable ASCII characters only"
                    )));
                }
            }
        }
        Ok(())
    }

    /// Build the initial client response in GS2 framing format (RFC 7628).
    ///
    /// Format: `n,,\x01auth=Bearer <token>[\x01key=value]*\x01\x01`
    pub(crate) fn to_gs2_initial_response(&self) -> Vec<u8> {
        // Pre-calculate capacity to avoid reallocations
        let mut capacity = 3 + 1 + 12 + self.token_value.len() + 2; // n,,\x01auth=Bearer \x01\x01
        for (k, v) in &self.extensions {
            capacity += 1 + k.len() + 1 + v.len(); // \x01key=value
        }

        let mut response = Vec::with_capacity(capacity);

        // GS2 header: no channel binding, no authorization identity
        response.extend_from_slice(b"n,,");

        // Authorization: Bearer <token>
        response.push(0x01);
        response.extend_from_slice(b"auth=Bearer ");
        response.extend_from_slice(self.token_value.as_bytes());

        // SASL extensions
        for (key, value) in &self.extensions {
            response.push(0x01);
            response.extend_from_slice(key.as_bytes());
            response.push(b'=');
            response.extend_from_slice(value.as_bytes());
        }

        // Terminator
        response.push(0x01);
        response.push(0x01);

        response
    }

    /// Process the server's response after the initial client message.
    ///
    /// On success, the server sends an empty response. On failure, the server
    /// sends a JSON error payload with status, scope, and OpenID configuration.
    ///
    /// Returns `Ok(())` on success or an error with the server's message on failure.
    pub(crate) fn process_server_response(&self, challenge: &[u8]) -> Result<()> {
        // Empty challenge means the server accepted the token
        if challenge.is_empty() {
            return Ok(());
        }

        // Server sent an error — parse the JSON error response
        let error_msg = String::from_utf8_lossy(challenge);
        Err(KrafkaError::auth(format!(
            "OAUTHBEARER authentication failed: {error_msg}"
        )))
    }
}

impl fmt::Debug for OAuthBearerToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OAuthBearerToken")
            .field("token_value", &"[REDACTED]")
            .field("extensions", &self.extensions)
            .field("lifetime_ms", &self.lifetime_ms)
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_oauthbearer_token_basic() {
        let token = OAuthBearerToken::new("my-jwt-token");
        let response = token.to_gs2_initial_response();

        // Verify GS2 format: n,,\x01auth=Bearer my-jwt-token\x01\x01
        let expected = b"n,,\x01auth=Bearer my-jwt-token\x01\x01";
        assert_eq!(response, expected);
    }

    #[test]
    fn test_oauthbearer_token_with_single_extension() {
        let token = OAuthBearerToken::new("my-token").with_extension("logicalCluster", "lkc-123");
        let response = token.to_gs2_initial_response();
        let response_str = String::from_utf8_lossy(&response);

        assert!(response_str.starts_with("n,,\x01auth=Bearer my-token"));
        assert!(response_str.contains("\x01logicalCluster=lkc-123"));
        assert!(response_str.ends_with("\x01\x01"));
    }

    #[test]
    fn test_oauthbearer_token_with_multiple_extensions() {
        let token = OAuthBearerToken::new("tok")
            .with_extension("ext1", "val1")
            .with_extension("ext2", "val2");
        let response = token.to_gs2_initial_response();
        let response_str = String::from_utf8_lossy(&response);

        assert!(response_str.starts_with("n,,\x01auth=Bearer tok"));
        assert!(response_str.contains("ext1=val1"));
        assert!(response_str.contains("ext2=val2"));
        assert!(response_str.ends_with("\x01\x01"));
    }

    #[test]
    fn test_oauthbearer_debug_redacts_token() {
        let token = OAuthBearerToken::new("secret-token-value");
        let debug = format!("{token:?}");
        assert!(!debug.contains("secret-token-value"));
        assert!(debug.contains("[REDACTED]"));
    }

    #[test]
    fn test_oauthbearer_server_response_success_empty() {
        let token = OAuthBearerToken::new("tok");
        assert!(token.process_server_response(b"").is_ok());
    }

    #[test]
    fn test_oauthbearer_server_response_error_json() {
        let token = OAuthBearerToken::new("tok");
        let error_json = br#"{"status":"invalid_token","scope":"openid"}"#;
        let result = token.process_server_response(error_json);
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("invalid_token"));
    }

    #[test]
    fn test_oauthbearer_gs2_format_compliance() {
        // Verify exact RFC 7628 wire format
        let token = OAuthBearerToken::new("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.sig");
        let response = token.to_gs2_initial_response();

        // Must start with GS2 header
        assert_eq!(&response[..3], b"n,,");
        // Followed by \x01
        assert_eq!(response[3], 0x01);
        // auth=Bearer prefix
        assert_eq!(&response[4..16], b"auth=Bearer ");
        // Ends with double \x01
        let len = response.len();
        assert_eq!(response[len - 2], 0x01);
        assert_eq!(response[len - 1], 0x01);
    }

    #[test]
    fn test_oauthbearer_empty_token_produces_valid_gs2() {
        // Edge case: empty token value
        let token = OAuthBearerToken::new("");
        let response = token.to_gs2_initial_response();
        assert_eq!(response, b"n,,\x01auth=Bearer \x01\x01");
    }

    // ── OAuthBearerToken::validate() ──

    #[test]
    fn test_oauthbearer_validate_valid_token() {
        let token = OAuthBearerToken::new("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.sig");
        assert!(token.validate().is_ok());
    }

    #[test]
    fn test_oauthbearer_validate_empty_token_is_err() {
        let token = OAuthBearerToken::new("");
        assert!(token.validate().is_err());
    }

    #[test]
    fn test_oauthbearer_validate_null_byte_is_err() {
        let token = OAuthBearerToken::new("tok\x00en");
        assert!(token.validate().is_err(), "null byte must be rejected");
    }

    #[test]
    fn test_oauthbearer_validate_gs2_separator_is_err() {
        let token = OAuthBearerToken::new("tok\x01en");
        assert!(
            token.validate().is_err(),
            "0x01 (GS2 separator) must be rejected in token value"
        );
    }

    #[test]
    fn test_oauthbearer_validate_non_ascii_is_err() {
        // Construct a token containing a high byte (0x80) that is valid UTF-8
        // in a 2-byte sequence (U+0080, encoded as 0xC2 0x80).
        // Our validator rejects any byte > 0x7E, so this must be rejected.
        let token = OAuthBearerToken::new("\u{0080}");
        assert!(
            token.validate().is_err(),
            "non-ASCII (multi-byte UTF-8) byte must be rejected"
        );
    }

    #[test]
    fn test_oauthbearer_validate_extension_null_byte_is_err() {
        let token = OAuthBearerToken::new("valid-token").with_extension("key\x00", "value");
        assert!(
            token.validate().is_err(),
            "null byte in extension key must be rejected"
        );
    }

    #[test]
    fn test_oauthbearer_validate_extension_gs2_separator_in_value_is_err() {
        let token = OAuthBearerToken::new("valid-token").with_extension("key", "val\x01ue");
        assert!(
            token.validate().is_err(),
            "0x01 in extension value must be rejected"
        );
    }

    #[test]
    fn test_oauthbearer_validate_valid_extension() {
        let token = OAuthBearerToken::new("valid-token")
            .with_extension("logicalCluster", "lkc-abc123")
            .with_extension("identityPoolId", "pool-xyz789");
        assert!(token.validate().is_ok());
    }

    #[test]
    fn test_oauthbearer_token_clone() {
        let token = OAuthBearerToken::new("tok").with_extension("k", "v");
        let cloned = token.clone();
        assert_eq!(
            cloned.to_gs2_initial_response(),
            token.to_gs2_initial_response()
        );
    }

    #[tokio::test]
    async fn test_token_provider_closure_impl() {
        let provider = || async { Ok(OAuthBearerToken::new("from-closure")) };
        let token = provider.provide_token().await.unwrap();
        assert_eq!(
            token.to_gs2_initial_response(),
            OAuthBearerToken::new("from-closure").to_gs2_initial_response()
        );
    }

    #[tokio::test]
    async fn test_token_provider_handle() {
        let handle = OAuthBearerTokenProviderHandle::new(|| async {
            Ok(OAuthBearerToken::new("handle-token"))
        });
        let token = handle.provide_token().await.unwrap();
        assert_eq!(
            token.to_gs2_initial_response(),
            OAuthBearerToken::new("handle-token").to_gs2_initial_response()
        );
    }

    #[test]
    fn test_token_provider_handle_clone() {
        let handle =
            OAuthBearerTokenProviderHandle::new(|| async { Ok(OAuthBearerToken::new("tok")) });
        let cloned = handle.clone();
        // Both point to the same Arc
        assert!(Arc::ptr_eq(&handle.0, &cloned.0));
    }

    #[test]
    fn test_token_provider_handle_debug_no_secrets() {
        let handle = OAuthBearerTokenProviderHandle::new(|| async {
            Ok(OAuthBearerToken::new("super-secret"))
        });
        let debug = format!("{handle:?}");
        assert_eq!(debug, "[OAuthBearerTokenProvider]");
        assert!(!debug.contains("super-secret"));
    }

    #[tokio::test]
    async fn test_token_provider_error_propagation() {
        let handle = OAuthBearerTokenProviderHandle::new(|| async {
            Err(KrafkaError::auth("token expired"))
        });
        let result = handle.provide_token().await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("token expired"));
    }

    #[tokio::test]
    async fn test_token_provider_struct_impl() {
        struct StaticProvider {
            token: String,
        }
        impl OAuthBearerTokenProvider for StaticProvider {
            fn provide_token(
                &self,
            ) -> Pin<Box<dyn Future<Output = Result<OAuthBearerToken>> + Send + '_>> {
                let token = self.token.clone();
                Box::pin(async move { Ok(OAuthBearerToken::new(token)) })
            }
        }

        let provider = StaticProvider {
            token: "struct-token".to_string(),
        };
        let handle = OAuthBearerTokenProviderHandle::new(provider);
        let token = handle.provide_token().await.unwrap();
        assert_eq!(
            token.to_gs2_initial_response(),
            OAuthBearerToken::new("struct-token").to_gs2_initial_response()
        );
    }

    #[test]
    fn test_oauthbearer_token_not_expired_without_lifetime() {
        let token = OAuthBearerToken::new("tok");
        assert!(!token.is_expired());
        assert!(token.lifetime_ms().is_none());
    }

    #[test]
    fn test_oauthbearer_token_not_expired_future_lifetime() {
        // Set expiry 1 hour in the future
        let future_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            + 3_600_000;
        let token = OAuthBearerToken::new("tok").with_lifetime_ms(future_ms);
        assert!(!token.is_expired());
        assert_eq!(token.lifetime_ms(), Some(future_ms));
    }

    #[test]
    fn test_oauthbearer_token_expired_past_lifetime() {
        // Set expiry 1 hour in the past
        let past_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64
            - 3_600_000;
        let token = OAuthBearerToken::new("tok").with_lifetime_ms(past_ms);
        assert!(token.is_expired());
    }

    #[test]
    fn test_oauthbearer_token_needs_refresh_near_expiry() {
        let near_future_ms = current_epoch_ms().unwrap() + 10_000;
        let token = OAuthBearerToken::new("tok").with_lifetime_ms(near_future_ms);

        assert!(!token.is_expired());
        assert!(token.needs_refresh());
    }

    #[test]
    fn test_oauthbearer_token_does_not_need_refresh_with_safe_margin() {
        let future_ms = current_epoch_ms().unwrap() + 60_000;
        let token = OAuthBearerToken::new("tok").with_lifetime_ms(future_ms);

        assert!(!token.needs_refresh());
    }

    // ── An unknown-expiry token must never be cached forever ───────────

    #[test]
    fn test_cached_token_without_lifetime_is_fresh_when_new() {
        let entry = CachedToken::new(OAuthBearerToken::new("jwt"));
        assert!(
            !entry.is_stale(),
            "a just-fetched token must be served from cache"
        );
    }

    #[test]
    fn test_cached_token_without_lifetime_goes_stale_with_age() {
        // `needs_refresh()` is `lifetime_ms.is_some_and(..)`,
        // so it is FALSE forever when lifetime_ms is None — which is exactly
        // how `OAuthBearerToken::new(jwt)` (and the crate's own doc examples)
        // build a token. Without an age bound, one JWT is cached for the whole
        // process lifetime and every reconnect after the real `exp` re-sends a
        // dead credential.
        let token = OAuthBearerToken::new("jwt");
        assert!(
            !token.needs_refresh(),
            "precondition: an unknown-expiry token never reports needs_refresh"
        );
        let entry = CachedToken {
            token,
            fetched_at: Instant::now()
                .checked_sub(OAUTHBEARER_UNKNOWN_EXPIRY_MAX_AGE + Duration::from_secs(1))
                .expect("test backdate within system uptime"),
        };
        assert!(
            entry.is_stale(),
            "an unknown-expiry token older than the max age must be refetched"
        );
    }

    #[test]
    fn test_cached_token_with_lifetime_still_uses_skew_margin() {
        // The age bound must not weaken the existing expiry logic.
        let near = current_epoch_ms().unwrap() + 10_000;
        let entry = CachedToken::new(OAuthBearerToken::new("jwt").with_lifetime_ms(near));
        assert!(entry.is_stale(), "inside the 30s skew window");

        let far = current_epoch_ms().unwrap() + 3_600_000;
        let entry = CachedToken::new(OAuthBearerToken::new("jwt").with_lifetime_ms(far));
        assert!(!entry.is_stale(), "well outside the skew window");
    }

    // ── Token-fetch observability ───────────────────────────────────────────
    //
    // An OAUTHBEARER provider is called per connection, so a misconfigured
    // `token_endpoint` used to surface only as connection failures: nothing
    // counted the OAuth round trip and nothing named it as the cause. These
    // pin the counters to the two paths that fetch.

    #[tokio::test]
    async fn a_successful_fetch_is_counted_with_its_expiry() {
        let metrics = Arc::new(ConnectionMetrics::new());
        let expiry = current_epoch_ms().unwrap() + 3_600_000;
        let handle = OAuthBearerTokenProviderHandle::new(move || async move {
            Ok(OAuthBearerToken::new("jwt").with_lifetime_ms(expiry))
        });
        handle.bind_metrics(metrics.clone());

        handle.provide_token().await.expect("provider succeeds");

        assert_eq!(metrics.oauth_token_fetches.get(), 1);
        assert_eq!(metrics.oauth_token_fetch_failures.get(), 0);
        assert_eq!(
            metrics.oauth_token_expiry_epoch_ms.get(),
            expiry as u64,
            "the dashboard's remaining-lifetime panel reads this gauge"
        );

        // A cache hit is not a fetch.
        handle.provide_token().await.expect("cached");
        assert_eq!(
            metrics.oauth_token_fetches.get(),
            1,
            "serving from cache must not be counted as a fetch"
        );
    }

    #[tokio::test]
    async fn a_failed_fetch_is_counted_separately() {
        let metrics = Arc::new(ConnectionMetrics::new());
        let handle = OAuthBearerTokenProviderHandle::new(|| async {
            Err(KrafkaError::auth("token endpoint returned HTTP 401"))
        });
        handle.bind_metrics(metrics.clone());

        let err = handle
            .provide_token()
            .await
            .expect_err("the provider fails");
        assert!(err.to_string().contains("401"), "the error must survive");

        assert_eq!(metrics.oauth_token_fetches.get(), 1);
        assert_eq!(metrics.oauth_token_fetch_failures.get(), 1);
        assert_eq!(
            metrics.oauth_token_expiry_epoch_ms.get(),
            0,
            "a failure must not claim a token expiry"
        );
    }

    /// A failure must not clear the expiry of a token that is still valid —
    /// that would make one transient blip look like a total credential loss.
    #[tokio::test]
    async fn a_failed_refresh_leaves_the_previous_expiry_alone() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let metrics = Arc::new(ConnectionMetrics::new());
        let expiry = current_epoch_ms().unwrap() + 3_600_000;
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let handle = OAuthBearerTokenProviderHandle::new(move || {
            let c = c.clone();
            async move {
                if c.fetch_add(1, Ordering::SeqCst) == 0 {
                    Ok(OAuthBearerToken::new("jwt").with_lifetime_ms(expiry))
                } else {
                    Err(KrafkaError::auth("identity provider unavailable"))
                }
            }
        });
        handle.bind_metrics(metrics.clone());

        handle.provide_token().await.expect("first fetch succeeds");
        // Force the next call past the cache.
        handle
            .test_seed_cache(
                OAuthBearerToken::new("jwt"),
                OAUTHBEARER_UNKNOWN_EXPIRY_MAX_AGE + Duration::from_secs(1),
            )
            .await;
        handle
            .provide_token()
            .await
            .expect_err("second fetch fails");

        assert_eq!(metrics.oauth_token_fetches.get(), 2);
        assert_eq!(metrics.oauth_token_fetch_failures.get(), 1);
        assert_eq!(
            metrics.oauth_token_expiry_epoch_ms.get(),
            expiry as u64,
            "a failed refresh must leave the known expiry in place"
        );
    }

    /// Metrics are optional: an unbound handle must fetch exactly as before.
    #[tokio::test]
    async fn an_unbound_handle_still_fetches() {
        let handle =
            OAuthBearerTokenProviderHandle::new(|| async { Ok(OAuthBearerToken::new("jwt")) });
        assert!(handle.provide_token().await.is_ok());
    }

    #[tokio::test]
    async fn test_provide_token_refetches_after_max_age() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let handle = OAuthBearerTokenProviderHandle::new(move || {
            let c = c.clone();
            async move {
                let n = c.fetch_add(1, Ordering::SeqCst);
                Ok(OAuthBearerToken::new(format!("jwt-{n}")))
            }
        });

        // First call populates the cache.
        let first = handle.provide_token().await.unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Second call is served from cache (no provider call).
        let _ = handle.provide_token().await.unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "fresh entry must be cached"
        );

        // Backdate the entry past the max age — the next call must refetch.
        handle
            .test_seed_cache(
                first,
                OAUTHBEARER_UNKNOWN_EXPIRY_MAX_AGE + Duration::from_secs(1),
            )
            .await;
        let refreshed = handle.provide_token().await.unwrap();
        assert_eq!(
            calls.load(Ordering::SeqCst),
            2,
            "a token past the unknown-expiry max age must be refetched"
        );
        assert_eq!(
            refreshed.to_gs2_initial_response(),
            OAuthBearerToken::new("jwt-1").to_gs2_initial_response()
        );
    }
}
