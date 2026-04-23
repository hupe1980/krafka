use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::auth::scram::ScramMechanism;
use crate::error::{ErrorCode, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{check_compact_nullable_array_len, encode_compact_array_len};

// ============================================================================
// DescribeUserScramCredentials API (Key 50)
//
// v0 baseline. All versions use flexible encoding.
// ============================================================================

/// DescribeUserScramCredentials request.
///
/// When `users` is `None` or empty, all SCRAM credentials are described.
#[derive(Debug, Clone)]
pub struct DescribeUserScramCredentialsRequest {
    /// Users to describe, or `None` for all.
    pub users: Option<Vec<String>>,
}

impl DescribeUserScramCredentialsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::DescribeUserScramCredentials
    }

    /// Encode for version 0 (flexible encoding).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        match &self.users {
            None => {
                // Compact nullable array: 0 means null.
                crate::util::varint::encode_unsigned_varint(0, buf);
            }
            Some(users) => {
                encode_compact_array_len(users.len(), buf)?;
                for user in users {
                    KafkaString::new(user).try_encode_compact(buf)?;
                    TaggedFields::default().try_encode(buf)?;
                }
            }
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for DescribeUserScramCredentialsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf),
            _ => unsupported_encode!("DescribeUserScramCredentialsRequest", version),
        }
    }
}

// ── Response ─────────────────────────────────────────────────────────────

/// SCRAM mechanism and iteration count for a credential.
#[derive(Debug, Clone)]
pub struct ScramCredentialInfo {
    /// SCRAM mechanism.
    pub mechanism: ScramMechanism,
    /// Iteration count used for the credential.
    pub iterations: i32,
}

/// Per-user SCRAM credential description.
#[derive(Debug, Clone)]
pub struct DescribeUserScramCredentialsResultEntry {
    /// User name.
    pub user: String,
    /// Per-user error code.
    pub error_code: ErrorCode,
    /// Per-user error message, or `None` if no error.
    pub error_message: Option<String>,
    /// SCRAM credentials for this user.
    pub credential_infos: Vec<ScramCredentialInfo>,
}

/// DescribeUserScramCredentials response.
#[derive(Debug, Clone)]
pub struct DescribeUserScramCredentialsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code.
    pub error_code: ErrorCode,
    /// Top-level error message, or `None` if no error.
    pub error_message: Option<String>,
    /// Per-user results.
    pub results: Vec<DescribeUserScramCredentialsResultEntry>,
}

impl DescribeUserScramCredentialsResponse {
    /// Decode from version 0 (flexible encoding).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode_compact(buf)?.0;

        let result_count =
            check_compact_nullable_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut results = Vec::with_capacity(result_count);
        for _ in 0..result_count {
            let user = non_nullable_string("user", KafkaString::decode_compact(buf)?.0)?;
            let user_error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let user_error_message = KafkaString::decode_compact(buf)?.0;
            let cred_count = check_compact_nullable_array_len(
                crate::util::varint::decode_unsigned_varint(buf)?,
            )?;
            let mut credential_infos = Vec::with_capacity(cred_count);
            for _ in 0..cred_count {
                let mechanism = ScramMechanism::from_wire_byte(i8::decode(buf)?)?;
                let iterations = i32::decode(buf)?;
                let _ = TaggedFields::decode(buf)?;
                credential_infos.push(ScramCredentialInfo {
                    mechanism,
                    iterations,
                });
            }
            let _ = TaggedFields::decode(buf)?;
            results.push(DescribeUserScramCredentialsResultEntry {
                user,
                error_code: user_error_code,
                error_message: user_error_message,
                credential_infos,
            });
        }
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            results,
        })
    }
}

impl VersionedDecode for DescribeUserScramCredentialsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("DescribeUserScramCredentialsResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use crate::auth::scram::ScramMechanism;
    use bytes::BytesMut;

    /// Build a compact (flexible) string: varint(len+1) followed by raw bytes.
    fn put_compact_string(buf: &mut BytesMut, s: &str) {
        crate::util::varint::encode_unsigned_varint((s.len() + 1) as u32, buf);
        buf.put_slice(s.as_bytes());
    }

    fn put_compact_null_string(buf: &mut BytesMut) {
        crate::util::varint::encode_unsigned_varint(0, buf);
    }

    fn put_empty_tagged_fields(buf: &mut BytesMut) {
        crate::util::varint::encode_unsigned_varint(0, buf);
    }

    fn put_compact_array_len(buf: &mut BytesMut, count: usize) {
        crate::util::varint::encode_unsigned_varint((count + 1) as u32, buf);
    }

    #[test]
    fn test_describe_scram_request_api_key() {
        assert_eq!(
            DescribeUserScramCredentialsRequest::api_key(),
            ApiKey::DescribeUserScramCredentials
        );
    }

    #[test]
    fn test_describe_scram_request_all_users() {
        let request = DescribeUserScramCredentialsRequest { users: None };
        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        // compact nullable null (varint 0) + tagged fields (varint 0)
        assert_eq!(buf.len(), 2);
    }

    #[test]
    fn test_describe_scram_request_specific_users() {
        let request = DescribeUserScramCredentialsRequest {
            users: Some(vec!["alice".to_string(), "bob".to_string()]),
        };
        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_describe_scram_versioned_unsupported() {
        let request = DescribeUserScramCredentialsRequest { users: None };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(-1, &mut buf).is_err());
        assert!(request.encode_versioned(1, &mut buf).is_err());
    }

    #[test]
    fn test_describe_scram_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        put_compact_null_string(&mut buf); // error_message
        // results: 1 user
        put_compact_array_len(&mut buf, 1);
        put_compact_string(&mut buf, "alice");
        buf.put_i16(0); // user error_code
        put_compact_null_string(&mut buf); // user error_message
        // credential_infos: 1
        put_compact_array_len(&mut buf, 1);
        buf.put_i8(1); // mechanism = SCRAM-SHA-512
        buf.put_i32(8192); // iterations
        put_empty_tagged_fields(&mut buf); // credential tagged fields
        put_empty_tagged_fields(&mut buf); // user tagged fields
        put_empty_tagged_fields(&mut buf); // top-level tagged fields

        let resp = DescribeUserScramCredentialsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].user, "alice");
        assert!(resp.results[0].error_code.is_ok());
        assert_eq!(resp.results[0].credential_infos.len(), 1);
        assert_eq!(
            resp.results[0].credential_infos[0].mechanism,
            ScramMechanism::Sha256
        );
        assert_eq!(resp.results[0].credential_infos[0].iterations, 8192);
    }

    #[test]
    fn test_describe_scram_response_top_level_error() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i16(31); // CLUSTER_AUTHORIZATION_FAILED
        put_compact_string(&mut buf, "Not authorized");
        put_compact_array_len(&mut buf, 0); // empty results
        put_empty_tagged_fields(&mut buf);

        let resp = DescribeUserScramCredentialsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert!(!resp.error_code.is_ok());
        assert_eq!(resp.error_message.as_deref(), Some("Not authorized"));
    }

    #[test]
    fn test_describe_scram_versioned_decode_unsupported() {
        let buf = BytesMut::new();
        assert!(
            DescribeUserScramCredentialsResponse::decode_versioned(-1, &mut buf.clone().freeze())
                .is_err()
        );
        assert!(
            DescribeUserScramCredentialsResponse::decode_versioned(1, &mut buf.freeze()).is_err()
        );
    }
}
