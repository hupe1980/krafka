use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode};
use crate::error::{ErrorCode, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};

// ============================================================================
// Heartbeat request/response
// ============================================================================

/// Heartbeat request.
#[derive(Debug, Clone)]
pub struct HeartbeatRequest {
    /// Group ID.
    pub group_id: String,
    /// Generation ID.
    pub generation_id: i32,
    /// Member ID.
    pub member_id: String,
    /// Group instance ID (v3+).
    pub group_instance_id: Option<String>,
}

impl HeartbeatRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::Heartbeat
    }

    /// Encode for version 3 (adds group_instance_id for KIP-345 static membership).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode(buf)?,
            None => KafkaString::null().try_encode(buf)?,
        }
        Ok(())
    }

    /// Encode for version 4 (flexible: compact strings + tagged fields).
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode_compact(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Heartbeat response.
#[derive(Debug, Clone)]
pub struct HeartbeatResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
}

impl HeartbeatResponse {
    /// Decode from version 3 (non-flexible).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }

    /// Decode from version 4 (flexible: tagged fields trailer).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }
}

impl VersionedEncode for HeartbeatRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            3 => self.encode_v3(buf)?,
            4 => self.encode_v4(buf)?,
            _ => return unsupported_encode!("HeartbeatRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for HeartbeatResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            3 => Self::decode_v3(buf),
            4 => Self::decode_v4(buf),
            _ => unsupported_decode!("HeartbeatResponse", version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::BytesMut;
    use rstest::rstest;

    /// Helper: write empty tagged fields (varint 0).
    fn put_tagged_fields(buf: &mut BytesMut) {
        buf.put_u8(0);
    }

    #[test]
    fn test_heartbeat_request_encode_v3() {
        let request = HeartbeatRequest {
            group_id: "my-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
        };

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();

        // v3 should include group_instance_id
        let data = String::from_utf8_lossy(&buf_v3);
        assert!(data.contains("instance-1"));
    }

    #[test]
    fn test_heartbeat_request_encode_v3_null_instance_id() {
        let request = HeartbeatRequest {
            group_id: "my-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: None,
        };

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();

        // v3 with null instance_id encodes a null marker (-1 as i16)
        assert!(!buf_v3.is_empty());
    }

    #[test]
    fn test_heartbeat_response_decode_v3() {
        let mut buf = BytesMut::new();
        // v3 is flexible: throttle_time_ms, error_code, tagged_fields
        buf.put_i32(50); // throttle_time_ms
        buf.put_i16(0); // error_code (None)
        put_tagged_fields(&mut buf);

        let resp = HeartbeatResponse::decode_versioned(3, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert_eq!(resp.error_code, ErrorCode::None);
    }

    #[test]
    fn test_heartbeat_response_below_min_rejected() {
        let mut buf1 = BytesMut::new();
        buf1.put_i16(0);
        assert!(HeartbeatResponse::decode_versioned(0, &mut buf1.freeze()).is_err());
        let mut buf2 = BytesMut::new();
        buf2.put_i16(0);
        assert!(HeartbeatResponse::decode_versioned(2, &mut buf2.freeze()).is_err());
    }

    #[rstest]
    // Heartbeat MIN=3
    #[case::hb_v0(0)]
    #[case::hb_v1(1)]
    #[case::hb_v2(2)]
    fn test_heartbeat_encode_below_min(#[case] version: i16) {
        let request = HeartbeatRequest {
            group_id: "g".to_string(),
            generation_id: 0,
            member_id: String::new(),
            group_instance_id: None,
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }

    #[rstest]
    // Decode floor: HeartbeatResponse MIN=3
    #[case::hb_resp_v0(0)]
    #[case::hb_resp_v1(1)]
    #[case::hb_resp_v2(2)]
    fn test_heartbeat_response_decode_below_min(#[case] version: i16) {
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        buf.put_i16(0);
        assert!(HeartbeatResponse::decode_versioned(version, &mut buf.freeze()).is_err());
    }

    // ── Heartbeat v4 flexible round-trip ──────────────────────────────

    #[test]
    fn test_heartbeat_request_encode_v4_flexible() {
        let request = HeartbeatRequest {
            group_id: "my-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
        };

        let mut buf = BytesMut::new();
        request.encode_v4(&mut buf).unwrap();
        assert!(!buf.is_empty());

        // Verify it encodes differently from v3 (compact strings are shorter).
        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();
        assert_ne!(buf.as_ref(), buf_v3.as_ref());
    }

    #[test]
    fn test_heartbeat_response_decode_v4_flexible() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(42);
        // error_code
        buf.put_i16(0);
        // tagged fields (empty)
        buf.put_u8(0);

        let resp = HeartbeatResponse::decode_v4(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 42);
        assert!(resp.error_code.is_ok());
    }

    #[test]
    fn test_heartbeat_v4_dispatch() {
        let request = HeartbeatRequest {
            group_id: "g".to_string(),
            generation_id: 0,
            member_id: "m".to_string(),
            group_instance_id: None,
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(4, &mut buf).unwrap();
        assert!(!buf.is_empty());
    }
}
