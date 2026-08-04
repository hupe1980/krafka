use crate::protocol::decode_capacity;
use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_bytes};
use crate::error::{ErrorCode, Result};
use crate::protocol::check_decode_array_len;
use crate::protocol::primitives::{Decode, KafkaBytes, KafkaString, TryEncode};

// ============================================================================
// SaslHandshake (API Key 17) - SASL Mechanism Negotiation
// ============================================================================

/// Request to negotiate SASL mechanism.
#[derive(Debug, Clone)]
pub struct SaslHandshakeRequest {
    /// SASL mechanism name (e.g., "PLAIN", "SCRAM-SHA-256").
    pub mechanism: String,
}

impl SaslHandshakeRequest {
    /// Create a new SASL handshake request.
    #[inline]
    pub fn new(mechanism: impl Into<String>) -> Self {
        Self {
            mechanism: mechanism.into(),
        }
    }

    /// Encode as version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.mechanism.clone())).try_encode(buf)?;
        Ok(())
    }

    /// Encode as version 1.
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        // Same as v0
        self.encode_v0(buf)?;
        Ok(())
    }
}

/// Response from SASL handshake.
#[derive(Debug, Clone)]
pub struct SaslHandshakeResponse {
    /// Error code.
    pub error_code: ErrorCode,
    /// List of mechanisms enabled on the broker.
    pub enabled_mechanisms: Vec<String>,
}

impl SaslHandshakeResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let count = check_decode_array_len(i32::decode(buf)?)?;
        let mut enabled_mechanisms = Vec::with_capacity(decode_capacity(count, buf.remaining()));

        for _ in 0..count {
            if let Some(mech) = KafkaString::decode(buf)?.0 {
                enabled_mechanisms.push(mech);
            }
        }

        Ok(Self {
            error_code,
            enabled_mechanisms,
        })
    }

    /// Check if the response indicates success.
    #[inline]
    pub fn is_ok(&self) -> bool {
        self.error_code.is_ok()
    }
}

// ============================================================================
// SaslAuthenticate (API Key 36) - SASL Authentication
// ============================================================================

/// Request to authenticate via SASL.
///
/// # `Debug` is redacted
///
/// [`auth_bytes`](Self::auth_bytes) is the raw mechanism payload, which *is*
/// the credential:
///
/// | Mechanism | What `auth_bytes` contains |
/// |---|---|
/// | `PLAIN` | `\0username\0password` — the password in cleartext |
/// | `OAUTHBEARER` | `n,,\x01auth=Bearer <token>\x01\x01` — the bearer token verbatim |
/// | `SCRAM-SHA-*` | the client-first/client-final messages, including the client proof |
/// | `AWS_MSK_IAM` | a SigV4-signed payload |
///
/// A derived `Debug` would print all of that into any log line, error context
/// or panic message that formatted the request. The manual impl reports the
/// length only, matching what [`AuthConfig`](crate::auth::AuthConfig),
/// `ProxyCredentials` and the delegation-token types already do.
#[derive(Clone)]
pub struct SaslAuthenticateRequest {
    /// SASL authentication bytes.
    ///
    /// Treat as secret — see the type-level documentation.
    pub auth_bytes: Vec<u8>,
}

impl std::fmt::Debug for SaslAuthenticateRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaslAuthenticateRequest")
            .field(
                "auth_bytes",
                &format_args!("[REDACTED; {} bytes]", self.auth_bytes.len()),
            )
            .finish()
    }
}

impl SaslAuthenticateRequest {
    /// Create a new SASL authenticate request.
    #[inline]
    pub fn new(auth_bytes: Vec<u8>) -> Self {
        Self { auth_bytes }
    }

    /// Encode as version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaBytes(Some(bytes::Bytes::from(self.auth_bytes.clone()))).try_encode(buf)?;
        Ok(())
    }

    /// Encode as version 1.
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        // Same as v0
        self.encode_v0(buf)?;
        Ok(())
    }
}

/// Response from SASL authentication.
///
/// `Debug` redacts [`auth_bytes`](Self::auth_bytes) for the same reason as
/// [`SaslAuthenticateRequest`]: on a multi-step mechanism the server's
/// challenge is part of the authentication exchange, and on a failed
/// negotiation it can echo client-supplied material back.
#[derive(Clone)]
pub struct SaslAuthenticateResponse {
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message (if any).
    pub error_message: Option<String>,
    /// Authentication response bytes.
    ///
    /// Treat as secret — see the type-level documentation.
    pub auth_bytes: Vec<u8>,
    /// Session lifetime in milliseconds (v1+).
    pub session_lifetime_ms: i64,
}

impl std::fmt::Debug for SaslAuthenticateResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaslAuthenticateResponse")
            .field("error_code", &self.error_code)
            .field("error_message", &self.error_message)
            .field(
                "auth_bytes",
                &format_args!("[REDACTED; {} bytes]", self.auth_bytes.len()),
            )
            .field("session_lifetime_ms", &self.session_lifetime_ms)
            .finish()
    }
}

impl SaslAuthenticateResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode(buf)?.0;
        let auth_bytes = non_nullable_bytes("auth_bytes", KafkaBytes::decode(buf)?.0)?.to_vec();

        Ok(Self {
            error_code,
            error_message,
            auth_bytes,
            session_lifetime_ms: 0,
        })
    }

    /// Decode from version 1.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode(buf)?.0;
        let auth_bytes = non_nullable_bytes("auth_bytes", KafkaBytes::decode(buf)?.0)?.to_vec();
        let session_lifetime_ms = i64::decode(buf)?;

        Ok(Self {
            error_code,
            error_message,
            auth_bytes,
            session_lifetime_ms,
        })
    }

    /// Check if the response indicates success.
    #[inline]
    pub fn is_ok(&self) -> bool {
        self.error_code.is_ok()
    }

    /// Check if authentication is complete.
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.error_code.is_ok() && self.auth_bytes.is_empty()
    }
}

impl VersionedEncode for SaslHandshakeRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            _ => return unsupported_encode!("SaslHandshakeRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for SaslHandshakeResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=1 => Self::decode_v0(buf),
            _ => unsupported_decode!("SaslHandshakeResponse", version),
        }
    }
}

impl VersionedEncode for SaslAuthenticateRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            _ => return unsupported_encode!("SaslAuthenticateRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for SaslAuthenticateResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            _ => unsupported_decode!("SaslAuthenticateResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod redaction_tests {
    use super::*;

    /// For SASL/PLAIN the request payload *is* the password. A derived `Debug`
    /// printed it verbatim into any log line, error context or panic message
    /// that formatted the request.
    #[test]
    fn sasl_authenticate_request_debug_redacts_the_credential() {
        // Exactly what the PLAIN mechanism puts on the wire.
        let payload = b"\0alice\0hunter2".to_vec();
        let rendered = format!("{:?}", SaslAuthenticateRequest::new(payload.clone()));

        assert!(
            !rendered.contains("hunter2"),
            "the password must not appear in Debug output: {rendered}"
        );
        assert!(
            !rendered.contains("alice"),
            "the username must not appear either: {rendered}"
        );
        assert!(rendered.contains("REDACTED"), "got: {rendered}");
        // The length is still reported, which is what makes the redaction
        // debuggable rather than merely opaque.
        assert!(
            rendered.contains(&payload.len().to_string()),
            "got: {rendered}"
        );
    }

    /// An OAUTHBEARER payload carries the bearer token verbatim.
    #[test]
    fn sasl_authenticate_request_debug_redacts_a_bearer_token() {
        let jwt = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhbGljZSJ9.c2ln";
        let payload = format!("n,,\x01auth=Bearer {jwt}\x01\x01").into_bytes();
        let rendered = format!("{:?}", SaslAuthenticateRequest::new(payload));
        assert!(!rendered.contains(jwt), "got: {rendered}");
        assert!(rendered.contains("REDACTED"), "got: {rendered}");
    }

    /// The response carries the server's half of a multi-step exchange.
    #[test]
    fn sasl_authenticate_response_debug_redacts_the_challenge() {
        let response = SaslAuthenticateResponse {
            error_code: ErrorCode::None,
            error_message: None,
            auth_bytes: b"v=rmF9pqV8S7suAoZWja4dJRkFsKQ=".to_vec(),
            session_lifetime_ms: 3_600_000,
        };
        let rendered = format!("{response:?}");
        assert!(
            !rendered.contains("rmF9pqV8S7suAoZWja4dJRkFsKQ"),
            "got: {rendered}"
        );
        assert!(rendered.contains("REDACTED"), "got: {rendered}");
        // Non-secret fields stay visible — the point is redaction, not silence.
        assert!(rendered.contains("3600000"), "got: {rendered}");
        assert!(rendered.contains("error_code"), "got: {rendered}");
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::protocol::*;
    use bytes::BytesMut;

    #[test]
    fn test_sasl_handshake_request() {
        let request = SaslHandshakeRequest::new("PLAIN");
        assert_eq!(request.mechanism, "PLAIN");

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_sasl_authenticate_request() {
        let auth_bytes = vec![0, b'u', b's', b'e', b'r', 0, b'p', b'a', b's', b's'];
        let request = SaslAuthenticateRequest::new(auth_bytes.clone());
        assert_eq!(request.auth_bytes, auth_bytes);

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_versioned_encode_rejects_negative_version() {
        let request = MetadataRequest::all_topics();
        let mut buf = BytesMut::new();
        let result = request.encode_versioned(-1, &mut buf);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("unsupported"), "got: {msg}");
    }

    #[test]
    fn test_versioned_decode_rejects_negative_version() {
        let mut buf = bytes::Bytes::new();
        let result = MetadataResponse::decode_versioned(-1, &mut buf);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("unsupported"), "got: {msg}");
    }

    #[test]
    fn test_versioned_encode_decode_roundtrip_sasl_handshake() {
        let request = SaslHandshakeRequest::new("SCRAM-SHA-256");
        let mut buf = BytesMut::new();
        request.encode_versioned(0, &mut buf).unwrap();
        assert!(!buf.is_empty());
        // High version still works (dispatches to latest encoder)
        let mut buf2 = BytesMut::new();
        request.encode_versioned(1, &mut buf2).unwrap();
        assert!(!buf2.is_empty());
    }
}
