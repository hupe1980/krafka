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
//! ```

use std::collections::HashMap;
use std::fmt;

use zeroize::{Zeroize, ZeroizeOnDrop};

use crate::error::{KrafkaError, Result};

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
    #[zeroize(skip)]
    extensions: HashMap<String, String>,
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
            extensions: HashMap::new(),
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
            .finish()
    }
}

#[cfg(test)]
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
        let token =
            OAuthBearerToken::new("my-token").with_extension("logicalCluster", "lkc-123");
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
        assert_eq!(
            &response[4..16],
            b"auth=Bearer "
        );
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
        let token = OAuthBearerToken::new("tok")
            .with_extension("k", "v");
        let cloned = token.clone();
        assert_eq!(
            cloned.to_gs2_initial_response(),
            token.to_gs2_initial_response()
        );
    }
}
