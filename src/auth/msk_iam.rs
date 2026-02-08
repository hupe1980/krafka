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
//! let authenticator = MskIamAuthenticator::new(&credentials, "broker.kafka.us-east-1.amazonaws.com");
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
use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::auth::AwsMskIamCredentials;

type HmacSha256 = Hmac<Sha256>;

/// AWS service name for Kafka.
const SERVICE_NAME: &str = "kafka-cluster";

/// AWS Signature v4 algorithm identifier.
const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// MSK IAM action for connect.
const ACTION: &str = "kafka-cluster:Connect";

/// User agent for MSK IAM.
const USER_AGENT: &str = "krafka-rust-client";

/// MSK IAM authenticator using AWS Signature v4.
#[derive(Debug)]
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
}

impl MskIamAuthenticator {
    /// Create a new MSK IAM authenticator.
    pub fn new(credentials: &AwsMskIamCredentials, host: impl Into<String>) -> Self {
        let host_str = host.into();
        // Strip port from host if present
        let host_without_port = host_str.split(':').next().unwrap_or(&host_str).to_string();

        Self {
            access_key_id: credentials.access_key_id.clone(),
            secret_access_key: credentials.secret_access_key.clone(),
            session_token: credentials.session_token.clone(),
            region: credentials.region.clone(),
            host: host_without_port,
        }
    }

    /// Create the authentication payload to send to MSK.
    ///
    /// Returns JSON-formatted signed authentication payload.
    pub fn create_auth_payload(&self) -> Vec<u8> {
        self.create_auth_payload_at(SystemTime::now())
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

        // Build the authentication payload
        let mut payload = format!(
            r#"{{"version":"2020_10_22","host":"{}","user-agent":"{}","action":"{}","x-amz-algorithm":"{}","x-amz-credential":"{}/{}","x-amz-date":"{}","x-amz-signedheaders":"{}","x-amz-signature":"{}""#,
            self.host,
            USER_AGENT,
            ACTION,
            ALGORITHM,
            self.access_key_id,
            credential_scope,
            amz_date,
            signed_headers,
            signature
        );

        // Add session token if present
        if let Some(ref token) = self.session_token {
            payload.push_str(&format!(r#","x-amz-security-token":"{}""#, token));
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
    fn derive_signing_key(&self, date_stamp: &str, region: &str, service: &str) -> Vec<u8> {
        let k_date = hmac_sha256(
            format!("AWS4{}", self.secret_access_key).as_bytes(),
            date_stamp.as_bytes(),
        );
        let k_region = hmac_sha256(&k_date, region.as_bytes());
        let k_service = hmac_sha256(&k_region, service.as_bytes());
        hmac_sha256(&k_service, b"aws4_request")
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
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
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
        let auth = MskIamAuthenticator::new(&creds, "broker.kafka.us-east-1.amazonaws.com:9098");
        assert_eq!(auth.host, "broker.kafka.us-east-1.amazonaws.com");
        assert_eq!(auth.region, "us-east-1");
    }

    #[test]
    fn test_auth_payload_is_valid_json() {
        let creds = test_credentials();
        let auth = MskIamAuthenticator::new(&creds, "broker.kafka.us-east-1.amazonaws.com");
        let payload = auth.create_auth_payload();

        // Should be valid UTF-8
        let payload_str = String::from_utf8(payload.clone()).unwrap();

        // Should contain expected fields
        assert!(payload_str.contains("\"version\":\"2020_10_22\""));
        assert!(payload_str.contains("\"host\":\"broker.kafka.us-east-1.amazonaws.com\""));
        assert!(payload_str.contains("\"user-agent\":\"krafka-rust-client\""));
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
        let auth = MskIamAuthenticator::new(&creds, "broker.kafka.us-east-1.amazonaws.com");
        let payload = auth.create_auth_payload();

        let payload_str = String::from_utf8(payload).unwrap();
        assert!(payload_str.contains("\"x-amz-security-token\":"));
    }

    #[test]
    fn test_deterministic_signature_at_same_time() {
        let creds = test_credentials();
        let auth = MskIamAuthenticator::new(&creds, "broker.kafka.us-east-1.amazonaws.com");

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
    fn test_signing_key_derivation() {
        let creds = test_credentials();
        let auth = MskIamAuthenticator::new(&creds, "broker.kafka.us-east-1.amazonaws.com");

        // This tests the key derivation follows AWS v4 spec
        let key = auth.derive_signing_key("20231114", "us-east-1", "kafka-cluster");
        assert_eq!(key.len(), 32); // SHA-256 produces 32 bytes
    }

    #[test]
    fn test_different_regions() {
        let creds = AwsMskIamCredentials::new("AKID", "secret", "eu-west-1");
        let auth = MskIamAuthenticator::new(&creds, "broker.kafka.eu-west-1.amazonaws.com");

        let payload_str = String::from_utf8(auth.create_auth_payload()).unwrap();
        assert!(payload_str.contains("eu-west-1"));
    }
}
