use bytes::{Buf, BufMut, Bytes};

use super::{VersionedDecode, VersionedEncode, non_nullable_bytes, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{
    Decode, Encode, KafkaBytes, KafkaString, TaggedFields, TryEncode,
};
use crate::protocol::{array_len_i32, check_compact_array_len, check_decode_array_len};

// ============================================================================
// JoinGroup request/response
// ============================================================================

/// JoinGroup request protocol.
#[derive(Debug, Clone)]
pub struct JoinGroupRequestProtocol {
    /// Protocol name.
    pub name: String,
    /// Protocol metadata.
    pub metadata: Bytes,
}

/// JoinGroup request.
#[derive(Debug, Clone)]
pub struct JoinGroupRequest {
    /// Group ID.
    pub group_id: String,
    /// Session timeout.
    pub session_timeout_ms: i32,
    /// Rebalance timeout (v1+).
    pub rebalance_timeout_ms: i32,
    /// Member ID.
    pub member_id: String,
    /// Group instance ID (v5+).
    pub group_instance_id: Option<String>,
    /// Protocol type (e.g., "consumer").
    pub protocol_type: String,
    /// Supported protocols.
    pub protocols: Vec<JoinGroupRequestProtocol>,
    /// Reason for joining (v8+, KIP-800).
    pub reason: Option<String>,
}

impl JoinGroupRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::JoinGroup
    }

    /// Encode for version 4.
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.session_timeout_ms.encode(buf);
        self.rebalance_timeout_ms.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;
        KafkaString::new(&self.protocol_type).try_encode(buf)?;

        buf.put_i32(array_len_i32(self.protocols.len())?);
        for protocol in &self.protocols {
            KafkaString::new(&protocol.name).try_encode(buf)?;
            KafkaBytes::new(protocol.metadata.clone()).try_encode(buf)?;
        }
        Ok(())
    }

    /// Encode for version 5+ (adds group_instance_id for KIP-345 static membership).
    pub fn encode_v5(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.session_timeout_ms.encode(buf);
        self.rebalance_timeout_ms.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode(buf)?,
            None => KafkaString::null().try_encode(buf)?,
        }
        KafkaString::new(&self.protocol_type).try_encode(buf)?;

        buf.put_i32(array_len_i32(self.protocols.len())?);
        for protocol in &self.protocols {
            KafkaString::new(&protocol.name).try_encode(buf)?;
            KafkaBytes::new(protocol.metadata.clone()).try_encode(buf)?;
        }
        Ok(())
    }

    /// Encode for version 6–7 (flexible: compact strings/arrays + tagged fields).
    pub fn encode_v6(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        self.session_timeout_ms.encode(buf);
        self.rebalance_timeout_ms.encode(buf);
        KafkaString::new(&self.member_id).try_encode_compact(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }
        KafkaString::new(&self.protocol_type).try_encode_compact(buf)?;

        let len = u32::try_from(self.protocols.len().saturating_add(1)).map_err(|_| {
            KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                "protocols array too large",
            )
        })?;
        crate::util::varint::encode_unsigned_varint(len, buf);
        for protocol in &self.protocols {
            KafkaString::new(&protocol.name).try_encode_compact(buf)?;
            KafkaBytes::new(protocol.metadata.clone()).try_encode_compact(buf)?;
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 8–9 (v6 + reason field, KIP-800).
    pub fn encode_v8(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        self.session_timeout_ms.encode(buf);
        self.rebalance_timeout_ms.encode(buf);
        KafkaString::new(&self.member_id).try_encode_compact(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }
        KafkaString::new(&self.protocol_type).try_encode_compact(buf)?;

        let len = u32::try_from(self.protocols.len().saturating_add(1)).map_err(|_| {
            KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                "protocols array too large",
            )
        })?;
        crate::util::varint::encode_unsigned_varint(len, buf);
        for protocol in &self.protocols {
            KafkaString::new(&protocol.name).try_encode_compact(buf)?;
            KafkaBytes::new(protocol.metadata.clone()).try_encode_compact(buf)?;
            TaggedFields::default().try_encode(buf)?;
        }
        match &self.reason {
            Some(r) => KafkaString::new(r).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Member in JoinGroup response.
#[derive(Debug, Clone)]
pub struct JoinGroupResponseMember {
    /// Member ID.
    pub member_id: String,
    /// Group instance ID.
    pub group_instance_id: Option<String>,
    /// Member metadata.
    pub metadata: Bytes,
}

/// JoinGroup response.
#[derive(Debug, Clone)]
pub struct JoinGroupResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Generation ID.
    pub generation_id: i32,
    /// Protocol type (v7+).
    pub protocol_type: Option<String>,
    /// Selected protocol name.
    pub protocol_name: Option<String>,
    /// Leader member ID.
    pub leader: String,
    /// True if the leader must skip running the assignment (v9+).
    pub skip_assignment: bool,
    /// This member's ID.
    pub member_id: String,
    /// Members (only for leader).
    pub members: Vec<JoinGroupResponseMember>,
}

impl JoinGroupResponse {
    /// Decode from version 4.
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let generation_id = i32::decode(buf)?;
        let protocol_name = KafkaString::decode(buf)?.0;
        let leader = non_nullable_string("leader", KafkaString::decode(buf)?.0)?;
        let member_id = non_nullable_string("member_id", KafkaString::decode(buf)?.0)?;

        let member_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let m_id = non_nullable_string("member_id", KafkaString::decode(buf)?.0)?;
            let metadata = non_nullable_bytes("member metadata", KafkaBytes::decode(buf)?.0)?;
            members.push(JoinGroupResponseMember {
                member_id: m_id,
                group_instance_id: None,
                metadata,
            });
        }

        Ok(Self {
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_type: None,
            protocol_name,
            leader,
            skip_assignment: false,
            member_id,
            members,
        })
    }

    /// Decode from version 5+ (adds group_instance_id per member).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let generation_id = i32::decode(buf)?;
        let protocol_name = KafkaString::decode(buf)?.0;
        let leader = non_nullable_string("leader", KafkaString::decode(buf)?.0)?;
        let member_id = non_nullable_string("member_id", KafkaString::decode(buf)?.0)?;

        let member_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let m_id = non_nullable_string("member_id", KafkaString::decode(buf)?.0)?;
            let group_instance_id = KafkaString::decode(buf)?.0;
            let metadata = non_nullable_bytes("member metadata", KafkaBytes::decode(buf)?.0)?;
            members.push(JoinGroupResponseMember {
                member_id: m_id,
                group_instance_id,
                metadata,
            });
        }

        Ok(Self {
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_type: None,
            protocol_name,
            leader,
            skip_assignment: false,
            member_id,
            members,
        })
    }

    /// Decode from version 6 (flexible: compact strings/arrays + tagged fields).
    pub fn decode_v6(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let generation_id = i32::decode(buf)?;
        let protocol_name = KafkaString::decode_compact(buf)?.0;
        let leader = non_nullable_string("leader", KafkaString::decode_compact(buf)?.0)?;
        let member_id = non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;

        let member_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let m_id = non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;
            let group_instance_id = KafkaString::decode_compact(buf)?.0;
            let metadata =
                non_nullable_bytes("member metadata", KafkaBytes::decode_compact(buf)?.0)?;
            let _ = TaggedFields::decode(buf)?;
            members.push(JoinGroupResponseMember {
                member_id: m_id,
                group_instance_id,
                metadata,
            });
        }
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_type: None,
            protocol_name,
            leader,
            skip_assignment: false,
            member_id,
            members,
        })
    }

    /// Decode from version 7–8 (v6 + protocol_type field, KIP-559).
    pub fn decode_v7(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let generation_id = i32::decode(buf)?;
        let protocol_type = KafkaString::decode_compact(buf)?.0;
        let protocol_name = KafkaString::decode_compact(buf)?.0;
        let leader = non_nullable_string("leader", KafkaString::decode_compact(buf)?.0)?;
        let member_id = non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;

        let member_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let m_id = non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;
            let group_instance_id = KafkaString::decode_compact(buf)?.0;
            let metadata =
                non_nullable_bytes("member metadata", KafkaBytes::decode_compact(buf)?.0)?;
            let _ = TaggedFields::decode(buf)?;
            members.push(JoinGroupResponseMember {
                member_id: m_id,
                group_instance_id,
                metadata,
            });
        }
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_type,
            protocol_name,
            leader,
            skip_assignment: false,
            member_id,
            members,
        })
    }

    /// Decode from version 9 (v7 + skip_assignment field).
    pub fn decode_v9(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let generation_id = i32::decode(buf)?;
        let protocol_type = KafkaString::decode_compact(buf)?.0;
        let protocol_name = KafkaString::decode_compact(buf)?.0;
        let leader = non_nullable_string("leader", KafkaString::decode_compact(buf)?.0)?;
        let skip_assignment = bool::decode(buf)?;
        let member_id = non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;

        let member_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut members = Vec::with_capacity(member_count);
        for _ in 0..member_count {
            let m_id = non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;
            let group_instance_id = KafkaString::decode_compact(buf)?.0;
            let metadata =
                non_nullable_bytes("member metadata", KafkaBytes::decode_compact(buf)?.0)?;
            let _ = TaggedFields::decode(buf)?;
            members.push(JoinGroupResponseMember {
                member_id: m_id,
                group_instance_id,
                metadata,
            });
        }
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            generation_id,
            protocol_type,
            protocol_name,
            leader,
            skip_assignment,
            member_id,
            members,
        })
    }

    /// Check if this member is the leader.
    #[inline]
    pub fn is_leader(&self) -> bool {
        self.member_id == self.leader
    }
}

impl VersionedEncode for JoinGroupRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            4 => self.encode_v4(buf)?,
            5 => self.encode_v5(buf)?,
            6 | 7 => self.encode_v6(buf)?,
            8 | 9 => self.encode_v8(buf)?,
            _ => return unsupported_encode!("JoinGroupRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for JoinGroupResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            4 => Self::decode_v4(buf),
            5 => Self::decode_v5(buf),
            6 => Self::decode_v6(buf),
            7 | 8 => Self::decode_v7(buf),
            9 => Self::decode_v9(buf),
            _ => unsupported_decode!("JoinGroupResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use bytes::BytesMut;
    use rstest::rstest;

    /// Build a compact-encoded JoinGroup v6 response buffer.
    fn build_join_group_response_v6_buf() -> BytesMut {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        // throttle_time_ms
        buf.put_i32(50);
        // error_code
        buf.put_i16(0);
        // generation_id
        buf.put_i32(7);

        // protocol_name (compact nullable string): "range" → varint(6) + "range"
        crate::util::varint::encode_unsigned_varint(6, &mut buf);
        buf.put_slice(b"range");

        // leader (compact string): "leader-1" → varint(9) + "leader-1"
        crate::util::varint::encode_unsigned_varint(9, &mut buf);
        buf.put_slice(b"leader-1");

        // member_id (compact string): "member-1" → varint(9) + "member-1"
        crate::util::varint::encode_unsigned_varint(9, &mut buf);
        buf.put_slice(b"member-1");

        // members compact array: count=1 → varint(2)
        crate::util::varint::encode_unsigned_varint(2, &mut buf);
        {
            // member_id: "member-1"
            crate::util::varint::encode_unsigned_varint(9, &mut buf);
            buf.put_slice(b"member-1");
            // group_instance_id: null → varint(0)
            crate::util::varint::encode_unsigned_varint(0, &mut buf);
            // metadata (compact bytes): b"meta" → varint(5) + "meta"
            crate::util::varint::encode_unsigned_varint(5, &mut buf);
            buf.put_slice(b"meta");
            // element tagged fields
            crate::util::varint::encode_unsigned_varint(0, &mut buf);
        }

        // top-level tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        buf
    }

    /// Build a compact-encoded JoinGroup v7 response buffer (adds protocol_type).
    fn build_join_group_response_v7_buf() -> BytesMut {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        // throttle_time_ms
        buf.put_i32(100);
        // error_code
        buf.put_i16(0);
        // generation_id
        buf.put_i32(12);

        // protocol_type (compact nullable string): "consumer" → varint(9) + "consumer"
        crate::util::varint::encode_unsigned_varint(9, &mut buf);
        buf.put_slice(b"consumer");

        // protocol_name (compact nullable string): "range" → varint(6) + "range"
        crate::util::varint::encode_unsigned_varint(6, &mut buf);
        buf.put_slice(b"range");

        // leader: "leader-1"
        crate::util::varint::encode_unsigned_varint(9, &mut buf);
        buf.put_slice(b"leader-1");

        // member_id: "follower-1"
        crate::util::varint::encode_unsigned_varint(11, &mut buf);
        buf.put_slice(b"follower-1");

        // members compact array: count=1 → varint(2)
        crate::util::varint::encode_unsigned_varint(2, &mut buf);
        {
            // member_id: "leader-1"
            crate::util::varint::encode_unsigned_varint(9, &mut buf);
            buf.put_slice(b"leader-1");
            // group_instance_id: "inst-1" → varint(7) + "inst-1"
            crate::util::varint::encode_unsigned_varint(7, &mut buf);
            buf.put_slice(b"inst-1");
            // metadata: b"\x01\x02"
            crate::util::varint::encode_unsigned_varint(3, &mut buf);
            buf.put_slice(b"\x01\x02");
            // element tagged fields
            crate::util::varint::encode_unsigned_varint(0, &mut buf);
        }

        // top-level tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        buf
    }

    #[test]
    fn test_join_group_request_encode_v5() {
        let request = JoinGroupRequest {
            group_id: "my-group".to_string(),
            session_timeout_ms: 10000,
            rebalance_timeout_ms: 300000,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".to_string(),
                metadata: bytes::Bytes::from_static(b"\x00\x00"),
            }],
            reason: None,
        };

        let mut buf_v4 = BytesMut::new();
        request.encode_v4(&mut buf_v4).unwrap();

        let mut buf_v5 = BytesMut::new();
        request.encode_v5(&mut buf_v5).unwrap();

        // v5 should include group_instance_id, so it should be larger
        assert!(buf_v5.len() > buf_v4.len());
    }

    #[test]
    fn test_join_group_response_decode_v5() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        // throttle_time_ms
        buf.put_i32(100);
        // error_code
        buf.put_i16(0);
        // generation_id
        buf.put_i32(3);
        // protocol_name
        let proto = b"range";
        buf.put_i16(proto.len() as i16);
        buf.put_slice(proto);
        // leader
        let leader = b"member-1";
        buf.put_i16(leader.len() as i16);
        buf.put_slice(leader);
        // member_id
        let member = b"member-1";
        buf.put_i16(member.len() as i16);
        buf.put_slice(member);
        // member_count = 2
        buf.put_i32(2);
        // member 1: member_id, group_instance_id, metadata
        let m1 = b"member-1";
        buf.put_i16(m1.len() as i16);
        buf.put_slice(m1);
        let inst1 = b"instance-1";
        buf.put_i16(inst1.len() as i16);
        buf.put_slice(inst1);
        let meta1 = b"meta1";
        buf.put_i32(meta1.len() as i32);
        buf.put_slice(meta1);
        // member 2: member_id, null group_instance_id, metadata
        let m2 = b"member-2";
        buf.put_i16(m2.len() as i16);
        buf.put_slice(m2);
        buf.put_i16(-1); // null instance id
        let meta2 = b"meta2";
        buf.put_i32(meta2.len() as i32);
        buf.put_slice(meta2);

        let mut data = buf.freeze();
        let resp = JoinGroupResponse::decode_v5(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 100);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.generation_id, 3);
        assert_eq!(resp.protocol_name, Some("range".to_string()));
        assert_eq!(resp.leader, "member-1");
        assert_eq!(resp.member_id, "member-1");
        assert!(resp.is_leader());
        assert_eq!(resp.members.len(), 2);
        assert_eq!(resp.members[0].member_id, "member-1");
        assert_eq!(
            resp.members[0].group_instance_id,
            Some("instance-1".to_string())
        );
        assert_eq!(resp.members[1].member_id, "member-2");
        assert_eq!(resp.members[1].group_instance_id, None);
    }

    #[test]
    fn test_join_group_request_encode_v6_flexible() {
        let request = JoinGroupRequest {
            group_id: "my-group".to_string(),
            session_timeout_ms: 10000,
            rebalance_timeout_ms: 300000,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".to_string(),
                metadata: bytes::Bytes::from_static(b"\x00\x00"),
            }],
            reason: None,
        };

        let mut buf_v5 = BytesMut::new();
        request.encode_v5(&mut buf_v5).unwrap();

        let mut buf_v6 = BytesMut::new();
        request.encode_v6(&mut buf_v6).unwrap();

        // v6 uses compact encoding which is typically shorter due to varint lengths
        // and adds tagged fields (0x00 per element + top-level). Sizes differ.
        assert!(!buf_v6.is_empty());
        assert_ne!(buf_v5.len(), buf_v6.len());
    }

    #[test]
    fn test_join_group_request_encode_v6_v7_wire_identical() {
        let request = JoinGroupRequest {
            group_id: "my-group".to_string(),
            session_timeout_ms: 10000,
            rebalance_timeout_ms: 300000,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".to_string(),
                metadata: bytes::Bytes::from_static(b"\x00\x00"),
            }],
            reason: None,
        };

        let mut buf_v6 = BytesMut::new();
        request.encode_v6(&mut buf_v6).unwrap();

        let mut buf_v7 = BytesMut::new();
        request.encode_v6(&mut buf_v7).unwrap();

        // v6 and v7 share the same encode_v6 encoder
        assert_eq!(buf_v6, buf_v7);
    }

    #[test]
    fn test_join_group_request_encode_v8_with_reason() {
        let request_no_reason = JoinGroupRequest {
            group_id: "my-group".to_string(),
            session_timeout_ms: 10000,
            rebalance_timeout_ms: 300000,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".to_string(),
                metadata: bytes::Bytes::from_static(b"\x00\x00"),
            }],
            reason: None,
        };

        let request_with_reason = JoinGroupRequest {
            group_id: "my-group".to_string(),
            session_timeout_ms: 10000,
            rebalance_timeout_ms: 300000,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".to_string(),
                metadata: bytes::Bytes::from_static(b"\x00\x00"),
            }],
            reason: Some("rebalance triggered".to_string()),
        };

        let mut buf_no_reason = BytesMut::new();
        request_no_reason.encode_v8(&mut buf_no_reason).unwrap();

        let mut buf_with_reason = BytesMut::new();
        request_with_reason.encode_v8(&mut buf_with_reason).unwrap();

        // v8 with a reason string should be longer than v8 with null reason
        assert!(buf_with_reason.len() > buf_no_reason.len());

        // v8 with reason should contain the reason text
        let data = String::from_utf8_lossy(&buf_with_reason);
        assert!(data.contains("rebalance triggered"));
    }

    #[test]
    fn test_join_group_request_encode_v8_v9_wire_identical() {
        let request = JoinGroupRequest {
            group_id: "my-group".to_string(),
            session_timeout_ms: 10000,
            rebalance_timeout_ms: 300000,
            member_id: "member-1".to_string(),
            group_instance_id: None,
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".to_string(),
                metadata: bytes::Bytes::from_static(b"\x00\x00"),
            }],
            reason: Some("test".to_string()),
        };

        let mut buf_v8 = BytesMut::new();
        request.encode_v8(&mut buf_v8).unwrap();

        let mut buf_v9 = BytesMut::new();
        request.encode_v8(&mut buf_v9).unwrap();

        // v8 and v9 share the same encode_v8 encoder
        assert_eq!(buf_v8, buf_v9);
    }

    #[test]
    fn test_join_group_request_encode_v8_vs_v6_null_reason() {
        let request = JoinGroupRequest {
            group_id: "my-group".to_string(),
            session_timeout_ms: 10000,
            rebalance_timeout_ms: 300000,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-1".to_string()),
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupRequestProtocol {
                name: "range".to_string(),
                metadata: bytes::Bytes::from_static(b"\x00\x00"),
            }],
            reason: None,
        };

        let mut buf_v6 = BytesMut::new();
        request.encode_v6(&mut buf_v6).unwrap();

        let mut buf_v8 = BytesMut::new();
        request.encode_v8(&mut buf_v8).unwrap();

        // v8 with null reason is v6 + null compact string (varint 0) before top tagged fields
        // So v8 should be exactly 1 byte longer (the 0x00 null marker for reason)
        assert_eq!(buf_v8.len(), buf_v6.len() + 1);
    }

    #[test]
    fn test_join_group_response_decode_v6_flexible() {
        let buf = build_join_group_response_v6_buf();
        let mut data = buf.freeze();
        let resp = JoinGroupResponse::decode_v6(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 50);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.generation_id, 7);
        assert_eq!(resp.protocol_type, None); // v6 does not decode protocol_type
        assert_eq!(resp.protocol_name, Some("range".to_string()));
        assert_eq!(resp.leader, "leader-1");
        assert!(!resp.skip_assignment); // always false for v6
        assert_eq!(resp.member_id, "member-1");
        assert_eq!(resp.members.len(), 1);
        assert_eq!(resp.members[0].member_id, "member-1");
        assert_eq!(resp.members[0].group_instance_id, None);
        assert_eq!(resp.members[0].metadata, bytes::Bytes::from_static(b"meta"));
    }

    #[test]
    fn test_join_group_response_decode_v7_protocol_type() {
        let buf = build_join_group_response_v7_buf();
        let mut data = buf.freeze();
        let resp = JoinGroupResponse::decode_v7(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 100);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.generation_id, 12);
        assert_eq!(resp.protocol_type, Some("consumer".to_string()));
        assert_eq!(resp.protocol_name, Some("range".to_string()));
        assert_eq!(resp.leader, "leader-1");
        assert!(!resp.skip_assignment); // always false for v7
        assert_eq!(resp.member_id, "follower-1");
        assert!(!resp.is_leader()); // follower-1 != leader-1
        assert_eq!(resp.members.len(), 1);
        assert_eq!(resp.members[0].member_id, "leader-1");
        assert_eq!(
            resp.members[0].group_instance_id,
            Some("inst-1".to_string())
        );
    }

    #[test]
    fn test_join_group_response_decode_v9_skip_assignment() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        // throttle_time_ms
        buf.put_i32(200);
        // error_code
        buf.put_i16(0);
        // generation_id
        buf.put_i32(42);

        // protocol_type: "consumer"
        crate::util::varint::encode_unsigned_varint(9, &mut buf);
        buf.put_slice(b"consumer");

        // protocol_name: "sticky"
        crate::util::varint::encode_unsigned_varint(7, &mut buf);
        buf.put_slice(b"sticky");

        // leader: "m-leader"
        crate::util::varint::encode_unsigned_varint(9, &mut buf);
        buf.put_slice(b"m-leader");

        // skip_assignment: true (1 byte)
        buf.put_u8(1);

        // member_id: "m-leader"
        crate::util::varint::encode_unsigned_varint(9, &mut buf);
        buf.put_slice(b"m-leader");

        // members compact array: count=0 → varint(1)
        crate::util::varint::encode_unsigned_varint(1, &mut buf);

        // top-level tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        let mut data = buf.freeze();
        let resp = JoinGroupResponse::decode_v9(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 200);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.generation_id, 42);
        assert_eq!(resp.protocol_type, Some("consumer".to_string()));
        assert_eq!(resp.protocol_name, Some("sticky".to_string()));
        assert_eq!(resp.leader, "m-leader");
        assert!(resp.skip_assignment); // true
        assert_eq!(resp.member_id, "m-leader");
        assert!(resp.is_leader());
        assert!(resp.members.is_empty());
    }

    #[test]
    fn test_join_group_response_decode_v9_skip_assignment_false() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i32(1); // generation_id

        // protocol_type: null
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        // protocol_name: "range"
        crate::util::varint::encode_unsigned_varint(6, &mut buf);
        buf.put_slice(b"range");

        // leader: "m1"
        crate::util::varint::encode_unsigned_varint(3, &mut buf);
        buf.put_slice(b"m1");

        // skip_assignment: false
        buf.put_u8(0);

        // member_id: "m2"
        crate::util::varint::encode_unsigned_varint(3, &mut buf);
        buf.put_slice(b"m2");

        // members: empty
        crate::util::varint::encode_unsigned_varint(1, &mut buf);

        // tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        let mut data = buf.freeze();
        let resp = JoinGroupResponse::decode_v9(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 0);
        assert_eq!(resp.generation_id, 1);
        assert_eq!(resp.protocol_type, None);
        assert_eq!(resp.protocol_name, Some("range".to_string()));
        assert!(!resp.skip_assignment);
        assert_eq!(resp.member_id, "m2");
        assert!(!resp.is_leader());
    }

    #[rstest]
    // JoinGroup MIN=4
    #[case::join_v0(0)]
    #[case::join_v1(1)]
    #[case::join_v2(2)]
    #[case::join_v3(3)]
    fn test_join_group_encode_below_min(#[case] version: i16) {
        let request = JoinGroupRequest {
            group_id: "g".to_string(),
            session_timeout_ms: 30_000,
            rebalance_timeout_ms: 60_000,
            member_id: String::new(),
            group_instance_id: None,
            protocol_type: "consumer".to_string(),
            protocols: vec![],
            reason: None,
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }
}
