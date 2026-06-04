//! AWS MSK IAM authentication using AWS Signature v4.
//!
//! This module implements the AWS_MSK_IAM SASL mechanism for authenticating
//! with Amazon Managed Streaming for Apache Kafka (MSK) using IAM credentials.
//!
//! The authentication process:
//! 1. Client sends a signed authentication payload
//! 2. MSK verifies the signature against IAM
//! 3. MSK returns success/failure
//!
//! # Example
//!
//! ## With explicit credentials (not recommended for production)
//!
//! ```ignore
//! use krafka::auth::{MskIamAuthenticator, AwsMskIamCredentials};
//!
//! let credentials = AwsMskIamCredentials::new("AKID", "secret", "us-east-1");
//! let authenticator = MskIamAuthenticator::new(&credentials, "broker.kafka.us-east-1.amazonaws.com")?;
//! let payload = authenticator.create_auth_payload();
//! ```
//!
//! ## From environment variables
//!
//! ```ignore
//! use krafka::auth::AwsMskIamCredentials;
//!
//! // Loads from AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN, AWS_REGION
//! let credentials = AwsMskIamCredentials::from_env()?;
//! ```
//!
//! ## From AWS SDK default chain (requires `aws-msk` feature)
//!
//! ```ignore
//! use krafka::auth::AwsMskIamCredentials;
//!
//! // Loads from default chain (env vars, instance profile, ECS task role, etc.)
//! let credentials = AwsMskIamCredentials::from_default_chain("us-east-1").await?;
//! ```

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use zeroize::Zeroizing;

use crate::auth::AwsMskIamCredentials;

type HmacSha256 = Hmac<Sha256>;

/// AWS service name for Kafka.
const SERVICE_NAME: &str = "kafka-cluster";

/// AWS Signature v4 algorithm identifier.
const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// MSK IAM action for connect.
const ACTION: &str = "kafka-cluster:Connect";

/// User agent for MSK IAM (includes crate version for diagnostics).
const USER_AGENT: &str = concat!("krafka-rust-client/", env!("CARGO_PKG_VERSION"));

/// Maximum clock offset applied to MSK IAM SigV4 timestamps.
pub(crate) const MAX_SIGV4_CLOCK_SKEW_SECS: i64 = 300;

/// MSK IAM authenticator using AWS Signature v4.
pub struct MskIamAuthenticator {
    /// AWS access key ID.
    access_key_id: String,
    /// AWS secret access key.
    secret_access_key: String,
    /// AWS session token (optional).
    session_token: Option<String>,
    /// AWS region.
    region: String,
    /// Broker host (without port).
    host: String,
    /// Internal clock offset in seconds for automatic skew compensation.
    ///
    /// Set by the connection layer when MSK IAM authentication fails with a
    /// clock-skew error. Not exposed publicly — SigV4 timestamps should
    /// come from the system clock; skew is handled operationally via NTP.
    clock_offset_secs: i64,
}

impl std::fmt::Debug for MskIamAuthenticator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Show only the last 4 chars of access_key_id (sufficient for identification,
        // insufficient for impersonation). Full key IDs should not appear in logs.
        let akid_tail = if self.access_key_id.len() > 4 {
            format!("***{}", &self.access_key_id[self.access_key_id.len() - 4..])
        } else {
            "***".to_string()
        };
        f.debug_struct("MskIamAuthenticator")
            .field("access_key_id", &akid_tail)
            .field("secret_access_key", &"[REDACTED]")
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| "[REDACTED]"),
            )
            .field("region", &self.region)
            .field("host", &self.host)
            .finish()
    }
}

impl Drop for MskIamAuthenticator {
    fn drop(&mut self) {
        // Zeroize sensitive fields on drop
        use zeroize::Zeroize;
        self.secret_access_key.zeroize();
        if let Some(ref mut token) = self.session_token {
            token.zeroize();
        }
    }
}

impl MskIamAuthenticator {
    /// Create a new MSK IAM authenticator.
    ///
    /// The `host` may include a port suffix (e.g. `broker:9098` or `[::1]:9092`);
    /// IPv6 addresses in brackets are handled correctly.
    pub fn new(credentials: &AwsMskIamCredentials, host: impl Into<String>) -> crate::Result<Self> {
        let host_str = host.into();
        let host_without_port = crate::util::extract_sni_hostname(&host_str)?.to_string();

        Ok(Self {
            access_key_id: credentials.access_key_id.clone(),
            secret_access_key: credentials.secret_access_key.clone(),
            session_token: credentials.session_token.clone(),
            region: credentials.region.clone(),
            host: host_without_port,
            clock_offset_secs: 0,
        })
    }

    /// Create a new MSK IAM authenticator with a clock offset.
    ///
    /// Used internally by the connection layer for automatic clock-skew
    /// compensation when MSK IAM authentication fails with a signature
    /// mismatch. Not part of the public API — SigV4 timestamps should
    /// come from the system clock.
    ///
    /// The offset is limited to ±5 minutes, matching the useful SigV4
    /// tolerance window. Larger offsets indicate a misconfigured local clock.
    pub(crate) fn new_with_clock_offset(
        credentials: &AwsMskIamCredentials,
        host: impl Into<String>,
        clock_offset_secs: i64,
    ) -> crate::Result<Self> {
        if !(-MAX_SIGV4_CLOCK_SKEW_SECS..=MAX_SIGV4_CLOCK_SKEW_SECS).contains(&clock_offset_secs) {
            return Err(crate::error::KrafkaError::config(format!(
                "clock_offset_secs ({clock_offset_secs}) exceeds ±{MAX_SIGV4_CLOCK_SKEW_SECS}s; \
                 AWS SigV4 only tolerates roughly ±5 minutes"
            )));
        }
        let mut auth = Self::new(credentials, host)?;
        auth.clock_offset_secs = clock_offset_secs;
        Ok(auth)
    }

    /// Create the authentication payload to send to MSK.
    ///
    /// Returns JSON-formatted signed authentication payload. If an internal
    /// clock offset has been set (for automatic skew compensation), it is
    /// applied to `SystemTime::now()` before signing.
    pub fn create_auth_payload(&self) -> Vec<u8> {
        let now = SystemTime::now();
        let adjusted = if self.clock_offset_secs >= 0 {
            let offset = std::time::Duration::from_secs(self.clock_offset_secs as u64);
            now.checked_add(offset).unwrap_or(now)
        } else {
            let offset = std::time::Duration::from_secs(self.clock_offset_secs.unsigned_abs());
            now.checked_sub(offset).unwrap_or(std::time::UNIX_EPOCH)
        };
        self.create_auth_payload_at(adjusted)
    }

    /// Create the authentication payload at a specific timestamp (for testing).
    pub fn create_auth_payload_at(&self, timestamp: SystemTime) -> Vec<u8> {
        let (date_stamp, amz_date) = format_timestamp(timestamp);

        // Build the canonical request
        let (canonical_request, signed_headers) =
            self.build_canonical_request(&amz_date, &date_stamp);

        // Create string to sign
        let credential_scope = format!(
            "{}/{}/{}/aws4_request",
            date_stamp, self.region, SERVICE_NAME
        );
        let string_to_sign =
            self.build_string_to_sign(&amz_date, &credential_scope, &canonical_request);

        // Calculate signature
        let signature =
            self.calculate_signature(&date_stamp, &self.region, SERVICE_NAME, &string_to_sign);

        // Build the authentication payload.
        // All user-controlled string fields (host, access_key_id, region via
        // credential_scope, session_token) are JSON-escaped per RFC 8259 §7 to
        // prevent injection if a custom credential provider ever returns
        // characters with JSON significance (`"`, `\`, control chars).
        // Fixed fields (user-agent, action, algorithm) are compile-time
        // constants; hex/ASCII fields (signature, amz_date, signed_headers)
        // come from SigV4 internals and contain no JSON metacharacters.
        let host_esc = json_escape_string(&self.host);
        let akid_esc = json_escape_string(&self.access_key_id);
        let scope_esc = json_escape_string(&credential_scope);

        let mut payload = format!(
            r#"{{"version":"2020_10_22","host":"{}","user-agent":"{}","action":"{}","x-amz-algorithm":"{}","x-amz-credential":"{}/{}","x-amz-date":"{}","x-amz-signedheaders":"{}","x-amz-signature":"{}""#,
            host_esc,
            USER_AGENT,
            ACTION,
            ALGORITHM,
            akid_esc,
            scope_esc,
            amz_date,
            signed_headers,
            signature
        );

        // Add session token if present
        if let Some(token) = &self.session_token {
            let token_esc = json_escape_string(token);
            // write! to String is infallible.
            let Ok(()) = write!(payload, r#","x-amz-security-token":"{}""#, token_esc) else {
                unreachable!("write! to String never fails");
            };
        }

        payload.push('}');

        payload.into_bytes()
    }

    /// Build the canonical request for signing.
    fn build_canonical_request(&self, amz_date: &str, _date_stamp: &str) -> (String, String) {
        let http_method = "GET";
        let canonical_uri = "/";
        let canonical_query_string = format!("Action={}", url_encode(ACTION));

        // Build canonical headers
        let mut headers: BTreeMap<String, String> = BTreeMap::new();
        headers.insert("host".to_string(), self.host.clone());
        headers.insert("x-amz-date".to_string(), amz_date.to_string());

        if let Some(ref token) = self.session_token {
            headers.insert("x-amz-security-token".to_string(), token.clone());
        }

        let canonical_headers: String = headers
            .iter()
            .map(|(k, v)| format!("{}:{}\n", k, v))
            .collect();

        let signed_headers: String = headers.keys().cloned().collect::<Vec<_>>().join(";");

        // Empty payload hash for GET
        let payload_hash = hex_encode(&sha256(&[]));

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            http_method,
            canonical_uri,
            canonical_query_string,
            canonical_headers,
            signed_headers,
            payload_hash
        );

        (canonical_request, signed_headers)
    }

    /// Build the string to sign.
    fn build_string_to_sign(
        &self,
        amz_date: &str,
        credential_scope: &str,
        canonical_request: &str,
    ) -> String {
        let canonical_request_hash = hex_encode(&sha256(canonical_request.as_bytes()));
        format!(
            "{}\n{}\n{}\n{}",
            ALGORITHM, amz_date, credential_scope, canonical_request_hash
        )
    }

    /// Calculate the signature using the signing key.
    fn calculate_signature(
        &self,
        date_stamp: &str,
        region: &str,
        service: &str,
        string_to_sign: &str,
    ) -> String {
        let signing_key = self.derive_signing_key(date_stamp, region, service);
        let signature = hmac_sha256(&signing_key, string_to_sign.as_bytes());
        hex_encode(&signature)
    }

    /// Derive the signing key using the AWS v4 key derivation.
    ///
    /// All intermediate HMAC keys are wrapped in [`Zeroizing`] to ensure
    /// they are scrubbed from memory on drop, preventing credential
    /// extraction from core dumps or swap files.
    fn derive_signing_key(
        &self,
        date_stamp: &str,
        region: &str,
        service: &str,
    ) -> Zeroizing<Vec<u8>> {
        let secret = Zeroizing::new(format!("AWS4{}", self.secret_access_key));
        let k_date = Zeroizing::new(hmac_sha256(secret.as_bytes(), date_stamp.as_bytes()));
        let k_region = Zeroizing::new(hmac_sha256(&k_date, region.as_bytes()));
        let k_service = Zeroizing::new(hmac_sha256(&k_region, service.as_bytes()));
        Zeroizing::new(hmac_sha256(&k_service, b"aws4_request"))
    }
}

/// Format a timestamp for AWS Signature v4.
fn format_timestamp(time: SystemTime) -> (String, String) {
    let duration = time.duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = duration.as_secs();

    // Calculate date components
    let days = secs / 86400;
    let remaining = secs % 86400;
    let hours = remaining / 3600;
    let minutes = (remaining % 3600) / 60;
    let seconds = remaining % 60;

    // Convert days since epoch to date
    let (year, month, day) = days_to_ymd(days);

    let date_stamp = format!("{:04}{:02}{:02}", year, month, day);
    let amz_date = format!(
        "{:04}{:02}{:02}T{:02}{:02}{:02}Z",
        year, month, day, hours, minutes, seconds
    );

    (date_stamp, amz_date)
}

/// Convert days since epoch to year/month/day.
fn days_to_ymd(days: u64) -> (i32, u32, u32) {
    // Algorithm from https://howardhinnant.github.io/date_algorithms.html
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y } as i32;

    (year, m, d)
}

/// Compute SHA-256 hash.
#[inline]
fn sha256(data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().to_vec()
}

/// Compute HMAC-SHA256.
#[inline]
fn hmac_sha256(key: &[u8], data: &[u8]) -> Vec<u8> {
    // new_from_slice accepts any key length per RFC 2104; the error variant is unreachable.
    let Ok(mut mac) = HmacSha256::new_from_slice(key) else {
        unreachable!("HMAC accepts any key length per RFC 2104");
    };
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}

/// JSON-escape a string per RFC 8259 §7.
///
/// Escapes `"`, `\`, and ASCII control characters (U+0000–U+001F).
/// The result is safe for interpolation inside a JSON string literal
/// (between the enclosing double-quote characters).
fn json_escape_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\x08' => out.push_str("\\b"),
            '\x09' => out.push_str("\\t"),
            '\x0A' => out.push_str("\\n"),
            '\x0C' => out.push_str("\\f"),
            '\x0D' => out.push_str("\\r"),
            c if u32::from(c) < 0x20 => {
                // write! to String is infallible; fmt::Error is never returned.
                let Ok(()) = write!(out, "\\u{:04X}", u32::from(c)) else {
                    unreachable!("write! to String never fails");
                };
            }
            c => out.push(c),
        }
    }
    out
}

/// URL-encode a string per RFC 3986.
fn url_encode(s: &str) -> String {
    let mut encoded = String::with_capacity(s.len() * 3);
    for byte in s.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => {
                encoded.push_str(&format!("%{:02X}", byte));
            }
        }
    }
    encoded
}

/// Hex-encode bytes.
#[inline]
fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{:02x}", b)).collect()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_credentials() -> AwsMskIamCredentials {
        AwsMskIamCredentials::new(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "us-east-1",
        )
    }

    #[test]
    fn test_msk_iam_authenticator_creation() {
        let creds = test_credentials();
        let auth =
            MskIamAuthenticator::new(&creds, "broker.kafka.us-east-1.amazonaws.com:9098").unwrap();
        assert_eq!(auth.host, "broker.kafka.us-east-1.amazonaws.com");
        assert_eq!(auth.region, "us-east-1");
    }

    #[test]
    fn test_auth_payload_is_valid_json() {
        let creds = test_credentials();
        let auth =
            MskIamAuthenticator::new(&creds, "broker.kafka.us-east-1.amazonaws.com").unwrap();
        let payload = auth.create_auth_payload();

        // Should be valid UTF-8
        let payload_str = String::from_utf8(payload.clone()).unwrap();

        // Should contain expected fields
        assert!(payload_str.contains("\"version\":\"2020_10_22\""));
        assert!(payload_str.contains("\"host\":\"broker.kafka.us-east-1.amazonaws.com\""));
        assert!(payload_str.contains("\"user-agent\":\"krafka-rust-client/"));
        assert!(payload_str.contains("\"action\":\"kafka-cluster:Connect\""));
        assert!(payload_str.contains("\"x-amz-algorithm\":\"AWS4-HMAC-SHA256\""));
        assert!(payload_str.contains("\"x-amz-credential\":"));
        assert!(payload_str.contains("\"x-amz-date\":"));
        assert!(payload_str.contains("\"x-amz-signedheaders\":"));
        assert!(payload_str.contains("\"x-amz-signature\":"));

        // Should be valid JSON (starts and ends with braces)
        assert!(payload_str.starts_with('{'));
        assert!(payload_str.ends_with('}'));
    }

    #[test]
    fn test_auth_payload_with_session_token() {
        let creds = AwsMskIamCredentials::with_session_token(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "FwoGZXIvYXdzEBYaDNZSNzRZzDJiLuQ8l==",
            "us-east-1",
        );
        let auth =
            MskIamAuthenticator::new(&creds, "broker.kafka.us-east-1.amazonaws.com").unwrap();
        let payload = auth.create_auth_payload();

        let payload_str = String::from_utf8(payload).unwrap();
        assert!(payload_str.contains("\"x-amz-security-token\":"));
    }

    #[test]
    fn test_deterministic_signature_at_same_time() {
        let creds = test_credentials();
        let auth =
            MskIamAuthenticator::new(&creds, "broker.kafka.us-east-1.amazonaws.com").unwrap();

        // Use a fixed timestamp
        let fixed_time = UNIX_EPOCH + Duration::from_secs(1700000000); // Nov 14, 2023

        let payload1 = auth.create_auth_payload_at(fixed_time);
        let payload2 = auth.create_auth_payload_at(fixed_time);

        assert_eq!(payload1, payload2);
    }

    #[test]
    fn test_format_timestamp() {
        let timestamp = UNIX_EPOCH + Duration::from_secs(1700000000);
        let (date_stamp, amz_date) = format_timestamp(timestamp);

        assert_eq!(date_stamp, "20231114");
        assert_eq!(amz_date, "20231114T221320Z");
    }

    #[test]
    fn test_url_encode() {
        assert_eq!(url_encode("hello"), "hello");
        assert_eq!(url_encode("hello world"), "hello%20world");
        assert_eq!(
            url_encode("kafka-cluster:Connect"),
            "kafka-cluster%3AConnect"
        );
    }

    #[test]
    fn test_json_escape_string() {
        // Baseline — safe characters pass through unchanged
        assert_eq!(json_escape_string("hello"), "hello");
        assert_eq!(json_escape_string("us-east-1"), "us-east-1");

        // RFC 8259 §7 mandatory escapes
        assert_eq!(json_escape_string("say \"hi\""), r#"say \"hi\""#);
        assert_eq!(json_escape_string(r"back\slash"), r"back\\slash");
        assert_eq!(json_escape_string("\x08here"), r"\bhere");
        assert_eq!(json_escape_string("tab\there"), r"tab\there");
        assert_eq!(json_escape_string("new\nline"), r"new\nline");
        assert_eq!(json_escape_string("\x0Cpage"), r"\fpage");
        assert_eq!(json_escape_string("cr\rhere"), r"cr\rhere");

        // Other control characters use \uXXXX
        assert_eq!(json_escape_string("\x00"), r"\u0000"); // null
        assert_eq!(json_escape_string("\x0B"), r"\u000B"); // vertical tab — no RFC 8259 named escape
        assert_eq!(json_escape_string("\x01\x1f"), r"\u0001\u001F");
    }

    #[test]
    fn test_payload_json_injection_safety() {
        // Directly construct an authenticator with JSON metacharacters in every
        // user-supplied field that flows into the payload. This cannot happen via
        // the public `new()` constructor (the hostname parser rejects it), but a
        // future code path might. The test verifies the escaping layer holds.
        let auth = MskIamAuthenticator {
            access_key_id: r#"AK"ID\injected"#.to_string(),
            secret_access_key: "secret".to_string(),
            session_token: Some(r#"tok\"en"#.to_string()),
            // region flows into credential_scope; inject a quote there too
            region: r#"us-east-"1"#.to_string(),
            host: r#"host"with"quotes.example.com"#.to_string(),
            clock_offset_secs: 0,
        };
        let fixed_time = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let payload_str = String::from_utf8(auth.create_auth_payload_at(fixed_time)).unwrap();

        // Unescaped metacharacters must not appear mid-value
        assert!(
            !payload_str.contains(r#","AK"ID"#),
            "unescaped quote in access_key_id"
        );
        assert!(
            !payload_str.contains(r#","host":"host"with"#),
            "unescaped quote in host"
        );

        // Escaped forms must be present
        assert!(
            payload_str.contains(r#"AK\"ID\\injected"#),
            "access_key_id not escaped"
        );
        assert!(
            payload_str.contains(r#"host\"with\"quotes"#),
            "host not escaped"
        );
        assert!(
            payload_str.contains(r#"tok\\\"en"#),
            "session_token not escaped"
        );

        // region flows into x-amz-credential via credential_scope — must be escaped there too
        assert!(
            !payload_str.contains(r#"us-east-"1"#),
            "unescaped quote in region (via credential_scope)"
        );
        assert!(
            payload_str.contains(r#"us-east-\"1"#),
            "region not escaped in credential_scope"
        );

        // Output must still be well-formed (braces match)
        assert!(payload_str.starts_with('{'));
        assert!(payload_str.ends_with('}'));
    }

    #[test]
    fn test_signing_key_derivation() {
        let creds = test_credentials();
        let auth =
            MskIamAuthenticator::new(&creds, "broker.kafka.us-east-1.amazonaws.com").unwrap();

        // This tests the key derivation follows AWS v4 spec
        let key = auth.derive_signing_key("20231114", "us-east-1", "kafka-cluster");
        assert_eq!(key.len(), 32); // SHA-256 produces 32 bytes
    }

    #[test]
    fn test_different_regions() {
        let creds = AwsMskIamCredentials::new("AKID", "secret", "eu-west-1");
        let auth =
            MskIamAuthenticator::new(&creds, "broker.kafka.eu-west-1.amazonaws.com").unwrap();

        let payload_str = String::from_utf8(auth.create_auth_payload()).unwrap();
        assert!(payload_str.contains("eu-west-1"));
    }

    // ── MskIamAuthenticator Debug redaction & zeroize ──

    #[test]
    fn test_msk_iam_debug_redacts_secrets() {
        let creds = AwsMskIamCredentials::with_session_token(
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "FwoGZXIvYXdzEBYaDNZSNzRZzDJiLuQ8l==",
            "us-east-1",
        );
        let auth =
            MskIamAuthenticator::new(&creds, "broker.kafka.us-east-1.amazonaws.com").unwrap();
        let debug_output = format!("{:?}", auth);

        // Must NOT contain the actual secret key or session token
        assert!(
            !debug_output.contains("wJalrXUtnFEMI"),
            "Secret key leaked in Debug output"
        );
        assert!(
            !debug_output.contains("FwoGZXIvYXdz"),
            "Session token leaked in Debug output"
        );
        // Must contain [REDACTED] markers
        assert!(debug_output.contains("[REDACTED]"));
        // Access key ID must be truncated — only last 4 chars visible
        assert!(
            debug_output.contains("MPLE"),
            "should show last 4 chars of access key ID"
        );
        assert!(
            !debug_output.contains("AKIAIOSFODNN7EXAMPLE"),
            "full access key ID must not appear in Debug output"
        );
    }

    #[test]
    fn test_msk_iam_zeroize_on_drop() {
        // Verify Drop does not panic
        let creds = test_credentials();
        let auth = MskIamAuthenticator::new(&creds, "broker:9098").unwrap();
        drop(auth);
    }

    #[test]
    fn test_msk_iam_clock_offset_positive() {
        let creds = test_credentials();
        let auth_no_offset = MskIamAuthenticator::new(&creds, "broker:9098").unwrap();
        let auth_offset =
            MskIamAuthenticator::new_with_clock_offset(&creds, "broker:9098", 300).unwrap();

        let payload_no = String::from_utf8(auth_no_offset.create_auth_payload()).unwrap();
        let payload_off = String::from_utf8(auth_offset.create_auth_payload()).unwrap();

        // Both should be valid JSON, but the x-amz-date values should differ
        // because one is shifted by 1 hour.
        assert!(payload_no.contains("\"x-amz-date\":"));
        assert!(payload_off.contains("\"x-amz-date\":"));

        // Extract dates to compare
        let date_no = extract_amz_date(&payload_no);
        let date_off = extract_amz_date(&payload_off);
        assert_ne!(
            date_no, date_off,
            "clock offset should produce different timestamps"
        );
    }

    #[test]
    fn test_msk_iam_clock_offset_negative() {
        let creds = test_credentials();
        let auth = MskIamAuthenticator::new_with_clock_offset(&creds, "broker:9098", -300).unwrap();
        let payload = String::from_utf8(auth.create_auth_payload()).unwrap();
        assert!(payload.contains("\"x-amz-date\":"));
    }

    #[test]
    fn test_msk_iam_clock_offset_rejects_outside_sigv4_window() {
        let creds = test_credentials();
        let err =
            MskIamAuthenticator::new_with_clock_offset(&creds, "broker:9098", 301).unwrap_err();
        assert!(err.to_string().contains("±300s"));

        let err = MskIamAuthenticator::new_with_clock_offset(&creds, "broker:9098", i64::MIN)
            .unwrap_err();
        assert!(err.to_string().contains("±300s"));
    }

    /// Helper to extract `x-amz-date` value from the JSON payload.
    fn extract_amz_date(json: &str) -> String {
        let key = "\"x-amz-date\":\"";
        let start = json.find(key).unwrap() + key.len();
        let end = json[start..].find('"').unwrap() + start;
        json[start..end].to_string()
    }
}
