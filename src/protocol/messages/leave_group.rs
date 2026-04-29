use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{array_len_i32, check_compact_array_len, check_decode_array_len};

// ============================================================================
// LeaveGroup request/response
// ============================================================================

/// Member leaving in LeaveGroup (v3+).
#[derive(Debug, Clone)]
pub struct LeaveGroupMember {
    /// Member ID.
    pub member_id: String,
    /// Group instance ID.
    pub group_instance_id: Option<String>,
    /// Reason for leaving (v5+, KIP-800).
    pub reason: Option<String>,
}

/// LeaveGroup request.
#[derive(Debug, Clone)]
pub struct LeaveGroupRequest {
    /// Group ID.
    pub group_id: String,
    /// Member ID (v0-v2).
    pub member_id: String,
    /// Members (v3+).
    pub members: Vec<LeaveGroupMember>,
}

impl LeaveGroupRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::LeaveGroup
    }

    /// Encode for version 3 (batch leave with per-member group_instance_id).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        buf.put_i32(array_len_i32(self.members.len())?);
        for member in &self.members {
            KafkaString::new(&member.member_id).try_encode(buf)?;
            match &member.group_instance_id {
                Some(id) => KafkaString::new(id).try_encode(buf)?,
                None => KafkaString::null().try_encode(buf)?,
            }
        }
        Ok(())
    }

    /// Encode for version 4 (flexible: compact strings/arrays + tagged fields).
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        let len = u32::try_from(self.members.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("members array too large"))?;
        crate::util::varint::encode_unsigned_varint(len, buf);
        for member in &self.members {
            KafkaString::new(&member.member_id).try_encode_compact(buf)?;
            match &member.group_instance_id {
                Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
                None => KafkaString::null().try_encode_compact(buf)?,
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 5 (v4 + reason field per member, KIP-800).
    pub fn encode_v5(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        let len = u32::try_from(self.members.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("members array too large"))?;
        crate::util::varint::encode_unsigned_varint(len, buf);
        for member in &self.members {
            KafkaString::new(&member.member_id).try_encode_compact(buf)?;
            match &member.group_instance_id {
                Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
                None => KafkaString::null().try_encode_compact(buf)?,
            }
            match &member.reason {
                Some(r) => KafkaString::new(r).try_encode_compact(buf)?,
                None => KafkaString::null().try_encode_compact(buf)?,
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Member result in LeaveGroup response (v3+).
#[derive(Debug, Clone)]
pub struct LeaveGroupResponseMember {
    /// Member ID.
    pub member_id: String,
    /// Group instance ID.
    pub group_instance_id: Option<String>,
    /// Per-member error code.
    pub error_code: ErrorCode,
}

/// LeaveGroup response.
#[derive(Debug, Clone)]
pub struct LeaveGroupResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Per-member results (v3+ only, empty for earlier versions).
    pub members: Vec<LeaveGroupResponseMember>,
}

impl LeaveGroupResponse {
    /// Decode from version 3 (KIP-345 batch leave with per-member results).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let member_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let member_id = non_nullable_string("member_id", KafkaString::decode(buf)?.0)?;
            let group_instance_id = KafkaString::decode(buf)?.0;
            let member_error_code = ErrorCode::from_i16(i16::decode(buf)?);
            members.push(LeaveGroupResponseMember {
                member_id,
                group_instance_id,
                error_code: member_error_code,
            });
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            members,
        })
    }

    /// Decode from version 4 (flexible: compact strings/arrays + tagged fields).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let member_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let member_id = non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;
            let group_instance_id = KafkaString::decode_compact(buf)?.0;
            let member_error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let _ = TaggedFields::decode(buf)?;
            members.push(LeaveGroupResponseMember {
                member_id,
                group_instance_id,
                error_code: member_error_code,
            });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            members,
        })
    }

    /// Decode from version 5 (v4, wire-identical — reason field is request-only).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_v4(buf)
    }
}

impl VersionedEncode for LeaveGroupRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            3 => self.encode_v3(buf)?,
            4 => self.encode_v4(buf)?,
            5 => self.encode_v5(buf)?,
            _ => return unsupported_encode!("LeaveGroupRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for LeaveGroupResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            3 => Self::decode_v3(buf),
            4 => Self::decode_v4(buf),
            5 => Self::decode_v5(buf),
            _ => unsupported_decode!("LeaveGroupResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use bytes::BytesMut;
    use rstest::rstest;

    // LeaveGroupResponse decode_v3 roundtrip (v0/v1 no longer supported).
    #[test]
    fn test_leave_group_response_decode_v3_no_members() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(100);
        // error_code = 0 (NONE)
        buf.put_i16(0);
        // members array length = 0
        buf.put_i32(0);

        let mut data = buf.freeze();
        let resp = LeaveGroupResponse::decode_v3(&mut data).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.throttle_time_ms, 100);
        assert!(resp.members.is_empty());
    }

    #[test]
    fn test_leave_group_response_decode_v3_with_members() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(50);
        // top-level error_code = 0 (NONE)
        buf.put_i16(0);
        // members array length = 2
        buf.put_i32(2);

        // member 1: member_id = "m-1", group_instance_id = "i-1", error_code = 0
        let m1 = b"m-1";
        buf.put_i16(m1.len() as i16);
        buf.put_slice(m1);
        let i1 = b"i-1";
        buf.put_i16(i1.len() as i16);
        buf.put_slice(i1);
        buf.put_i16(0);

        // member 2: member_id = "m-2", group_instance_id = null, error_code = 79 (FENCED_INSTANCE_ID)
        let m2 = b"m-2";
        buf.put_i16(m2.len() as i16);
        buf.put_slice(m2);
        buf.put_i16(-1); // null group_instance_id
        buf.put_i16(79);

        let mut data = buf.freeze();
        let resp = LeaveGroupResponse::decode_v3(&mut data).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.members.len(), 2);

        assert_eq!(resp.members[0].member_id, "m-1");
        assert_eq!(resp.members[0].group_instance_id, Some("i-1".to_string()));
        assert!(resp.members[0].error_code.is_ok());

        assert_eq!(resp.members[1].member_id, "m-2");
        assert_eq!(resp.members[1].group_instance_id, None);
        assert!(!resp.members[1].error_code.is_ok());
    }

    #[rstest]
    // LeaveGroup MIN=3
    #[case::leave_v0(0)]
    #[case::leave_v1(1)]
    #[case::leave_v2(2)]
    fn test_leave_group_encode_below_min(#[case] version: i16) {
        let request = LeaveGroupRequest {
            group_id: "g".to_string(),
            member_id: String::new(),
            members: vec![],
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }

    // ── LeaveGroup v4/v5 flexible round-trip ──────────────────────────

    #[test]
    fn test_leave_group_request_encode_v4_flexible() {
        let request = LeaveGroupRequest {
            group_id: "grp".to_string(),
            member_id: "m".to_string(),
            members: vec![LeaveGroupMember {
                member_id: "m1".to_string(),
                group_instance_id: Some("inst-1".to_string()),
                reason: None,
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
    fn test_leave_group_request_encode_v5_with_reason() {
        let request = LeaveGroupRequest {
            group_id: "grp".to_string(),
            member_id: "m".to_string(),
            members: vec![LeaveGroupMember {
                member_id: "m1".to_string(),
                group_instance_id: None,
                reason: Some("shutting down".to_string()),
            }],
        };

        let mut buf = BytesMut::new();
        request.encode_v5(&mut buf).unwrap();
        let data = String::from_utf8_lossy(&buf);
        assert!(data.contains("shutting down"));
    }

    #[test]
    fn test_leave_group_response_decode_v4_flexible() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(15);
        // error_code
        buf.put_i16(0);
        // members array (compact: len+1 varint) = 1 member → varint(2)
        buf.put_u8(2);
        // member_id (compact string: len+1, then data)
        let mid = b"m1";
        buf.put_u8((mid.len() + 1) as u8);
        buf.put_slice(mid);
        // group_instance_id (compact nullable: null → 0)
        buf.put_u8(0);
        // per-member error_code
        buf.put_i16(0);
        // tagged fields (per member)
        buf.put_u8(0);
        // tagged fields (top-level)
        buf.put_u8(0);

        let resp = LeaveGroupResponse::decode_v4(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 15);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.members.len(), 1);
        assert_eq!(resp.members[0].member_id, "m1");
        assert!(resp.members[0].group_instance_id.is_none());
        assert!(resp.members[0].error_code.is_ok());
    }

    #[test]
    fn test_leave_group_v4_v5_dispatch() {
        let request = LeaveGroupRequest {
            group_id: "g".to_string(),
            member_id: "m".to_string(),
            members: vec![],
        };
        for version in [4, 5] {
            let mut buf = BytesMut::new();
            request.encode_versioned(version, &mut buf).unwrap();
            assert!(!buf.is_empty());
        }
    }
}
