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
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, warn};
use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::error::{KrafkaError, Result};

const OAUTHBEARER_EXPIRY_SKEW_MARGIN_MS: i64 = 30_000;

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
    cached: RwLock<Option<OAuthBearerToken>>,
    /// At most one refresh in flight at a time. Concurrent callers that find
    /// the cached token stale queue behind this lock; the first winner fetches
    /// while the rest get the result on unlock.
    refreshing: Mutex<()>,
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
        }))
    }

    /// Return a fresh token, using the cache when available.
    ///
    /// Returns the cached token if it is not within the expiry skew margin.
    /// Otherwise acquires the coalescing refresh lock, re-checks (another task
    /// may have just refreshed), and calls the provider exactly once while
    /// concurrent callers wait.
    pub async fn provide_token(&self) -> Result<OAuthBearerToken> {
        // Fast path: cached token is still fresh.
        {
            let guard = self.0.cached.read().await;
            if let Some(token) = guard.as_ref()
                && !token.needs_refresh()
            {
                return Ok(token.clone());
            }
        }

        // Slow path: coalesce concurrent refreshes under one lock.
        let _coalesce = self.0.refreshing.lock().await;

        // Re-check after acquiring the lock — another task may have already refreshed.
        {
            let guard = self.0.cached.read().await;
            if let Some(token) = guard.as_ref()
                && !token.needs_refresh()
            {
                return Ok(token.clone());
            }
        }

        // Fetch a fresh token and update the cache.
        let token = self.0.provider.provide_token().await?;
        *self.0.cached.write().await = Some(token.clone());
        Ok(token)
    }

    /// Start a background task that proactively refreshes the token before expiry.
    ///
    /// The task wakes when the cached token enters the 30-second expiry skew
    /// window and calls the provider to pre-warm the cache. This ensures that
    /// broker connections and reconnects always find a ready token without
    /// blocking on a provider call.
    ///
    /// When no token has been fetched yet, or the token has no `lifetime_ms`,
    /// the task retries every 30 seconds until a token with a known lifetime
    /// is cached.
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
                    match guard.as_ref().and_then(|t| t.lifetime_ms()) {
                        Some(lifetime_ms) => {
                            // Wake at `lifetime_ms - OAUTHBEARER_EXPIRY_SKEW_MARGIN_MS`.
                            let wake_at_ms =
                                lifetime_ms.saturating_sub(OAUTHBEARER_EXPIRY_SKEW_MARGIN_MS);
                            let now_ms = current_epoch_ms().unwrap_or(i64::MAX);
                            let remaining_ms = wake_at_ms.saturating_sub(now_ms).max(0);
                            Duration::from_millis(remaining_ms as u64)
                        }
                        None => {
                            // No token yet or token has no expiry.
                            // Retry every 30 s until we get one with a lifetime.
                            Duration::from_secs(30)
                        }
                    }
                };

                if !sleep_duration.is_zero() {
                    tokio::time::sleep(sleep_duration).await;
                }

                // Proactively refresh under the coalescing lock.
                let _coalesce = inner.refreshing.lock().await;
                match inner.provider.provide_token().await {
                    Ok(token) => {
                        debug!(
                            lifetime_ms = token.lifetime_ms(),
                            "OAuthBearer token proactively refreshed"
                        );
                        *inner.cached.write().await = Some(token);
                    }
                    Err(e) => {
                        warn!(
                            error = %e,
                            "OAuthBearer proactive token refresh failed; \
                             will retry on next refresh window"
                        );
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
}
