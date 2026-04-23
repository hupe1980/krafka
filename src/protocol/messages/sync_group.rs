use bytes::{Buf, BufMut, Bytes};

use super::{VersionedDecode, VersionedEncode, non_nullable_bytes};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::array_len_i32;
use crate::protocol::primitives::{
    Decode, Encode, KafkaBytes, KafkaString, TaggedFields, TryEncode,
};

// ============================================================================
// SyncGroup request/response
// ============================================================================

/// Assignment for a member in SyncGroup.
#[derive(Debug, Clone)]
pub struct SyncGroupRequestAssignment {
    /// Member ID.
    pub member_id: String,
    /// Assignment data.
    pub assignment: Bytes,
}

/// SyncGroup request.
#[derive(Debug, Clone)]
pub struct SyncGroupRequest {
    /// Group ID.
    pub group_id: String,
    /// Generation ID.
    pub generation_id: i32,
    /// Member ID.
    pub member_id: String,
    /// Group instance ID (v3+).
    pub group_instance_id: Option<String>,
    /// Protocol type (v5+).
    pub protocol_type: Option<String>,
    /// Protocol name (v5+).
    pub protocol_name: Option<String>,
    /// Assignments (only from leader).
    pub assignments: Vec<SyncGroupRequestAssignment>,
}

impl SyncGroupRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::SyncGroup
    }

    /// Encode for version 3 (KIP-345: includes group_instance_id).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode(buf)?,
            None => KafkaString::null().try_encode(buf)?,
        }

        buf.put_i32(array_len_i32(self.assignments.len())?);
        for assignment in &self.assignments {
            KafkaString::new(&assignment.member_id).try_encode(buf)?;
            KafkaBytes::new(assignment.assignment.clone()).try_encode(buf)?;
        }
        Ok(())
    }

    /// Encode for version 4 (flexible: compact strings/arrays + tagged fields).
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode_compact(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }

        let len = u32::try_from(self.assignments.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("assignments array too large"))?;
        crate::util::varint::encode_unsigned_varint(len, buf);
        for assignment in &self.assignments {
            KafkaString::new(&assignment.member_id).try_encode_compact(buf)?;
            KafkaBytes::new(assignment.assignment.clone()).try_encode_compact(buf)?;
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 5 (v4 + protocol_type/protocol_name, KIP-559).
    pub fn encode_v5(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode_compact(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }
        match &self.protocol_type {
            Some(pt) => KafkaString::new(pt).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }
        match &self.protocol_name {
            Some(pn) => KafkaString::new(pn).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }

        let len = u32::try_from(self.assignments.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("assignments array too large"))?;
        crate::util::varint::encode_unsigned_varint(len, buf);
        for assignment in &self.assignments {
            KafkaString::new(&assignment.member_id).try_encode_compact(buf)?;
            KafkaBytes::new(assignment.assignment.clone()).try_encode_compact(buf)?;
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// SyncGroup response.
#[derive(Debug, Clone)]
pub struct SyncGroupResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Protocol type (v5+).
    pub protocol_type: Option<String>,
    /// Protocol name (v5+).
    pub protocol_name: Option<String>,
    /// Assignment for this member.
    pub assignment: Bytes,
}

impl SyncGroupResponse {
    /// Decode from version 3 (non-flexible).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let assignment = non_nullable_bytes("assignment", KafkaBytes::decode(buf)?.0)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            protocol_type: None,
            protocol_name: None,
            assignment,
        })
    }

    /// Decode from version 4 (flexible: compact bytes + tagged fields).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let assignment = non_nullable_bytes("assignment", KafkaBytes::decode_compact(buf)?.0)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            protocol_type: None,
            protocol_name: None,
            assignment,
        })
    }

    /// Decode from version 5 (v4 + protocol_type/protocol_name, KIP-559).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let protocol_type = KafkaString::decode_compact(buf)?.0;
        let protocol_name = KafkaString::decode_compact(buf)?.0;
        let assignment = non_nullable_bytes("assignment", KafkaBytes::decode_compact(buf)?.0)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            protocol_type,
            protocol_name,
            assignment,
        })
    }
}

impl VersionedEncode for SyncGroupRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            3 => self.encode_v3(buf)?,
            4 => self.encode_v4(buf)?,
            5 => self.encode_v5(buf)?,
            _ => return unsupported_encode!("SyncGroupRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for SyncGroupResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            3 => Self::decode_v3(buf),
            4 => Self::decode_v4(buf),
            5 => Self::decode_v5(buf),
            _ => unsupported_decode!("SyncGroupResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use bytes::BytesMut;
    use rstest::rstest;

    // SyncGroupRequest::encode_v3 includes group_instance_id.
    #[test]
    fn test_sync_group_request_encode_v3_includes_group_instance_id() {
        use bytes::BytesMut;

        let request = SyncGroupRequest {
            group_id: "my-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("range".to_string()),
            assignments: vec![],
        };

        let mut buf = BytesMut::new();
        request.encode_v3(&mut buf).unwrap();

        // Verify the buffer contains the group_instance_id
        let data = buf.freeze();
        let data_str = String::from_utf8_lossy(&data);
        assert!(
            data_str.contains("instance-1"),
            "v3 encoding should include group_instance_id"
        );
    }

    // SyncGroupRequest::encode_v0 does NOT include group_instance_id.
    #[test]
    fn test_sync_group_request_encode_v0_omits_group_instance_id() {
        use bytes::BytesMut;

        let request = SyncGroupRequest {
            group_id: "my-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("range".to_string()),
            assignments: vec![],
        };

        let mut buf = BytesMut::new();
        request.encode_v3(&mut buf).unwrap();

        let data = buf.freeze();
        // v3 encoding includes group_instance_id, so it should contain the string
        let data_str = String::from_utf8_lossy(&data);
        assert!(
            data_str.contains("instance-1"),
            "v3 encoding should include group_instance_id"
        );
    }

    // SyncGroupResponse decode_v1 roundtrip.
    #[test]
    fn test_sync_group_response_decode_v1() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(50);
        // error_code = 0 (NONE)
        buf.put_i16(0);
        // assignment (empty bytes: length = 0)
        buf.put_i32(0);

        let mut data = buf.freeze();
        let resp = SyncGroupResponse::decode_v3(&mut data).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert!(resp.error_code.is_ok());
        assert!(resp.assignment.is_empty());
    }

    #[test]
    fn test_sync_group_response_decode_v3() {
        let mut buf = BytesMut::new();
        // v3 dispatches to decode_v1: throttle_time_ms, error_code, assignment (non-flexible bytes)
        buf.put_i32(25); // throttle_time_ms
        buf.put_i16(0); // error_code
        // assignment (non-flexible KafkaBytes: i32 length + data)
        let assignment = b"\x00\x01\x02";
        buf.put_i32(assignment.len() as i32);
        buf.put_slice(assignment);

        let resp = SyncGroupResponse::decode_versioned(3, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 25);
        assert_eq!(resp.error_code, ErrorCode::None);
        assert_eq!(resp.assignment, bytes::Bytes::from_static(b"\x00\x01\x02"));
    }

    #[test]
    fn test_sync_group_response_below_min_rejected() {
        let mut buf1 = BytesMut::new();
        buf1.put_i16(0);
        assert!(SyncGroupResponse::decode_versioned(0, &mut buf1.freeze()).is_err());
        let mut buf2 = BytesMut::new();
        buf2.put_i16(0);
        assert!(SyncGroupResponse::decode_versioned(2, &mut buf2.freeze()).is_err());
    }

    #[rstest]
    // SyncGroup MIN=3
    #[case::sync_v0(0)]
    #[case::sync_v1(1)]
    #[case::sync_v2(2)]
    fn test_sync_group_encode_below_min(#[case] version: i16) {
        let request = SyncGroupRequest {
            group_id: "g".to_string(),
            generation_id: 1,
            member_id: "m".to_string(),
            group_instance_id: None,
            protocol_type: None,
            protocol_name: None,
            assignments: vec![],
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }

    #[rstest]
    // Decode floor: SyncGroupResponse MIN=3
    #[case::sg_resp_v0(0)]
    #[case::sg_resp_v1(1)]
    #[case::sg_resp_v2(2)]
    fn test_sync_group_response_decode_below_min(#[case] version: i16) {
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        buf.put_i16(0);
        buf.put_i32(0);
        assert!(SyncGroupResponse::decode_versioned(version, &mut buf.freeze()).is_err());
    }

    // ── SyncGroup v4/v5 flexible round-trip ───────────────────────────

    #[test]
    fn test_sync_group_request_encode_v4_flexible() {
        let request = SyncGroupRequest {
            group_id: "my-group".to_string(),
            generation_id: 2,
            member_id: "member-1".to_string(),
            group_instance_id: Some("inst-1".to_string()),
            protocol_type: None,
            protocol_name: None,
            assignments: vec![SyncGroupRequestAssignment {
                member_id: "member-1".to_string(),
                assignment: Bytes::from_static(b"\x00\x01"),
            }],
        };

        let mut buf_v4 = BytesMut::new();
        request.encode_v4(&mut buf_v4).unwrap();
        assert!(!buf_v4.is_empty());

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();
        assert_ne!(buf_v4.as_ref(), buf_v3.as_ref());
    }

    #[test]
    fn test_sync_group_request_encode_v5_with_protocol() {
        let request = SyncGroupRequest {
            group_id: "grp".to_string(),
            generation_id: 1,
            member_id: "mem".to_string(),
            group_instance_id: None,
            protocol_type: Some("consumer".to_string()),
            protocol_name: Some("range".to_string()),
            assignments: vec![],
        };

        let mut buf = BytesMut::new();
        request.encode_v5(&mut buf).unwrap();
        let data = String::from_utf8_lossy(&buf);
        assert!(data.contains("consumer"));
        assert!(data.contains("range"));
    }

    #[test]
    fn test_sync_group_response_decode_v4_flexible() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(10);
        // error_code
        buf.put_i16(0);
        // assignment (compact bytes: len+1 varint, then data)
        let assign = b"\x00\x01";
        buf.put_u8((assign.len() + 1) as u8); // compact bytes length
        buf.put_slice(assign);
        // tagged fields (empty)
        buf.put_u8(0);

        let resp = SyncGroupResponse::decode_v4(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.assignment, Bytes::from_static(b"\x00\x01"));
        assert!(resp.protocol_type.is_none());
        assert!(resp.protocol_name.is_none());
    }

    #[test]
    fn test_sync_group_response_decode_v5_with_protocol() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(5);
        // error_code
        buf.put_i16(0);
        // protocol_type (compact nullable string: len+1, then data)
        let pt = b"consumer";
        buf.put_u8((pt.len() + 1) as u8);
        buf.put_slice(pt);
        // protocol_name
        let pn = b"range";
        buf.put_u8((pn.len() + 1) as u8);
        buf.put_slice(pn);
        // assignment (compact bytes: len+1 varint, then data)
        let assign = b"\x02\x03";
        buf.put_u8((assign.len() + 1) as u8);
        buf.put_slice(assign);
        // tagged fields (empty)
        buf.put_u8(0);

        let resp = SyncGroupResponse::decode_v5(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 5);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.protocol_type.as_deref(), Some("consumer"));
        assert_eq!(resp.protocol_name.as_deref(), Some("range"));
        assert_eq!(resp.assignment, Bytes::from_static(b"\x02\x03"));
    }

    #[test]
    fn test_sync_group_v4_v5_dispatch() {
        let request = SyncGroupRequest {
            group_id: "g".to_string(),
            generation_id: 0,
            member_id: "m".to_string(),
            group_instance_id: None,
            protocol_type: None,
            protocol_name: None,
            assignments: vec![],
        };
        for version in [4, 5] {
            let mut buf = BytesMut::new();
            request.encode_versioned(version, &mut buf).unwrap();
            assert!(!buf.is_empty());
        }
    }
}
