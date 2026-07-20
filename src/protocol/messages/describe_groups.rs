use bytes::{Buf, BufMut, Bytes};

use super::{VersionedDecode, VersionedEncode, non_nullable_bytes, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{
    Decode, Encode, KafkaBytes, KafkaString, TaggedFields, TryEncode,
};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, decode_capacity,
};

// ============================================================================
// DescribeGroups API (Key 15)
// ============================================================================

/// DescribeGroups request.
#[derive(Debug, Clone)]
pub struct DescribeGroupsRequest {
    /// Group IDs to describe.
    pub groups: Vec<String>,
    /// Whether to include authorized operations (v3+).
    pub include_authorized_operations: bool,
}

impl DescribeGroupsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::DescribeGroups
    }

    /// Encode for version 1–2 (groups array only, no authorized ops).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.groups.len())?);
        for group in &self.groups {
            KafkaString::new(group).try_encode(buf)?;
        }
        Ok(())
    }

    /// Encode for version 3–4 (adds include_authorized_operations).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.groups.len())?);
        for group in &self.groups {
            KafkaString::new(group).try_encode(buf)?;
        }
        self.include_authorized_operations.encode(buf);
        Ok(())
    }

    /// Encode for version 5–6 (flexible: compact strings + tagged fields).
    pub fn encode_v5(&self, buf: &mut impl BufMut) -> Result<()> {
        let len = u32::try_from(self.groups.len().saturating_add(1)).map_err(|_| {
            KrafkaError::protocol_kind(ProtocolErrorKind::InvalidLength, "groups array too large")
        })?;
        crate::util::varint::encode_unsigned_varint(len, buf);
        for group in &self.groups {
            KafkaString::new(group).try_encode_compact(buf)?;
        }
        self.include_authorized_operations.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// A member in a described group.
#[derive(Debug, Clone)]
pub struct DescribeGroupMember {
    /// Member ID.
    pub member_id: String,
    /// Group instance ID (static membership).
    pub group_instance_id: Option<String>,
    /// Client ID.
    pub client_id: String,
    /// Client host.
    pub client_host: String,
    /// Member metadata.
    pub member_metadata: Bytes,
    /// Member assignment.
    pub member_assignment: Bytes,
}

/// A described group.
#[derive(Debug, Clone)]
pub struct DescribedGroup {
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message (v6+, KIP-1043).
    pub error_message: Option<String>,
    /// Group ID.
    pub group_id: String,
    /// Group state (e.g., "Stable", "Empty", "Dead").
    pub group_state: String,
    /// Protocol type (e.g., "consumer").
    pub protocol_type: String,
    /// Protocol data (assignor name).
    pub protocol_data: String,
    /// Group members.
    pub members: Vec<DescribeGroupMember>,
    /// Authorized operations bitfield (v3+, -2147483648 when not requested).
    pub authorized_operations: i32,
}

/// DescribeGroups response.
#[derive(Debug, Clone)]
pub struct DescribeGroupsResponse {
    /// Throttle time (v1+).
    pub throttle_time_ms: i32,
    /// Described groups.
    pub groups: Vec<DescribedGroup>,
}

impl DescribeGroupsResponse {
    /// Decode from version 1–2 (non-flexible, no authorized_operations, no instance_id).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut groups = Vec::with_capacity(decode_capacity(group_count, buf.remaining()));

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let group_id = non_nullable_string("group_id", KafkaString::decode(buf)?.0)?;
            let group_state = non_nullable_string("group_state", KafkaString::decode(buf)?.0)?;
            let protocol_type = non_nullable_string("protocol_type", KafkaString::decode(buf)?.0)?;
            let protocol_data = non_nullable_string("protocol_data", KafkaString::decode(buf)?.0)?;

            let member_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut members = Vec::with_capacity(decode_capacity(member_count, buf.remaining()));
            for _ in 0..member_count {
                let member_id = non_nullable_string("member_id", KafkaString::decode(buf)?.0)?;
                let client_id = non_nullable_string("client_id", KafkaString::decode(buf)?.0)?;
                let client_host = non_nullable_string("client_host", KafkaString::decode(buf)?.0)?;
                let member_metadata =
                    non_nullable_bytes("member_metadata", KafkaBytes::decode(buf)?.0)?;
                let member_assignment =
                    non_nullable_bytes("member_assignment", KafkaBytes::decode(buf)?.0)?;
                members.push(DescribeGroupMember {
                    member_id,
                    group_instance_id: None,
                    client_id,
                    client_host,
                    member_metadata,
                    member_assignment,
                });
            }

            groups.push(DescribedGroup {
                error_code,
                error_message: None,
                group_id,
                group_state,
                protocol_type,
                protocol_data,
                members,
                authorized_operations: i32::MIN,
            });
        }

        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }

    /// Decode from version 3 (adds authorized_operations per group).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut groups = Vec::with_capacity(decode_capacity(group_count, buf.remaining()));

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let group_id = non_nullable_string("group_id", KafkaString::decode(buf)?.0)?;
            let group_state = non_nullable_string("group_state", KafkaString::decode(buf)?.0)?;
            let protocol_type = non_nullable_string("protocol_type", KafkaString::decode(buf)?.0)?;
            let protocol_data = non_nullable_string("protocol_data", KafkaString::decode(buf)?.0)?;

            let member_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut members = Vec::with_capacity(decode_capacity(member_count, buf.remaining()));
            for _ in 0..member_count {
                let member_id = non_nullable_string("member_id", KafkaString::decode(buf)?.0)?;
                let client_id = non_nullable_string("client_id", KafkaString::decode(buf)?.0)?;
                let client_host = non_nullable_string("client_host", KafkaString::decode(buf)?.0)?;
                let member_metadata =
                    non_nullable_bytes("member_metadata", KafkaBytes::decode(buf)?.0)?;
                let member_assignment =
                    non_nullable_bytes("member_assignment", KafkaBytes::decode(buf)?.0)?;
                members.push(DescribeGroupMember {
                    member_id,
                    group_instance_id: None,
                    client_id,
                    client_host,
                    member_metadata,
                    member_assignment,
                });
            }

            let authorized_operations = i32::decode(buf)?;

            groups.push(DescribedGroup {
                error_code,
                error_message: None,
                group_id,
                group_state,
                protocol_type,
                protocol_data,
                members,
                authorized_operations,
            });
        }

        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }

    /// Decode from version 4 (adds group_instance_id per member).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut groups = Vec::with_capacity(decode_capacity(group_count, buf.remaining()));

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let group_id = non_nullable_string("group_id", KafkaString::decode(buf)?.0)?;
            let group_state = non_nullable_string("group_state", KafkaString::decode(buf)?.0)?;
            let protocol_type = non_nullable_string("protocol_type", KafkaString::decode(buf)?.0)?;
            let protocol_data = non_nullable_string("protocol_data", KafkaString::decode(buf)?.0)?;

            let member_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut members = Vec::with_capacity(decode_capacity(member_count, buf.remaining()));
            for _ in 0..member_count {
                let member_id = non_nullable_string("member_id", KafkaString::decode(buf)?.0)?;
                let group_instance_id = KafkaString::decode(buf)?.0;
                let client_id = non_nullable_string("client_id", KafkaString::decode(buf)?.0)?;
                let client_host = non_nullable_string("client_host", KafkaString::decode(buf)?.0)?;
                let member_metadata =
                    non_nullable_bytes("member_metadata", KafkaBytes::decode(buf)?.0)?;
                let member_assignment =
                    non_nullable_bytes("member_assignment", KafkaBytes::decode(buf)?.0)?;
                members.push(DescribeGroupMember {
                    member_id,
                    group_instance_id,
                    client_id,
                    client_host,
                    member_metadata,
                    member_assignment,
                });
            }

            let authorized_operations = i32::decode(buf)?;

            groups.push(DescribedGroup {
                error_code,
                error_message: None,
                group_id,
                group_state,
                protocol_type,
                protocol_data,
                members,
                authorized_operations,
            });
        }

        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }

    /// Decode from version 5 (flexible: compact strings, tagged fields, group_instance_id).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut groups = Vec::with_capacity(decode_capacity(group_count, buf.remaining()));

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let group_id = non_nullable_string("group_id", KafkaString::decode_compact(buf)?.0)?;
            let group_state =
                non_nullable_string("group_state", KafkaString::decode_compact(buf)?.0)?;
            let protocol_type =
                non_nullable_string("protocol_type", KafkaString::decode_compact(buf)?.0)?;
            let protocol_data =
                non_nullable_string("protocol_data", KafkaString::decode_compact(buf)?.0)?;

            let member_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut members = Vec::with_capacity(decode_capacity(member_count, buf.remaining()));
            for _ in 0..member_count {
                let member_id =
                    non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;
                let group_instance_id = KafkaString::decode_compact(buf)?.0;
                let client_id =
                    non_nullable_string("client_id", KafkaString::decode_compact(buf)?.0)?;
                let client_host =
                    non_nullable_string("client_host", KafkaString::decode_compact(buf)?.0)?;
                let member_metadata =
                    non_nullable_bytes("member_metadata", KafkaBytes::decode_compact(buf)?.0)?;
                let member_assignment =
                    non_nullable_bytes("member_assignment", KafkaBytes::decode_compact(buf)?.0)?;
                let _ = TaggedFields::decode(buf)?;
                members.push(DescribeGroupMember {
                    member_id,
                    group_instance_id,
                    client_id,
                    client_host,
                    member_metadata,
                    member_assignment,
                });
            }

            let authorized_operations = i32::decode(buf)?;
            let _ = TaggedFields::decode(buf)?;

            groups.push(DescribedGroup {
                error_code,
                error_message: None,
                group_id,
                group_state,
                protocol_type,
                protocol_data,
                members,
                authorized_operations,
            });
        }

        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }

    /// Decode from version 6 (adds error_message per group, KIP-1043).
    pub fn decode_v6(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut groups = Vec::with_capacity(decode_capacity(group_count, buf.remaining()));

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            let group_id = non_nullable_string("group_id", KafkaString::decode_compact(buf)?.0)?;
            let group_state =
                non_nullable_string("group_state", KafkaString::decode_compact(buf)?.0)?;
            let protocol_type =
                non_nullable_string("protocol_type", KafkaString::decode_compact(buf)?.0)?;
            let protocol_data =
                non_nullable_string("protocol_data", KafkaString::decode_compact(buf)?.0)?;

            let member_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut members = Vec::with_capacity(decode_capacity(member_count, buf.remaining()));
            for _ in 0..member_count {
                let member_id =
                    non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;
                let group_instance_id = KafkaString::decode_compact(buf)?.0;
                let client_id =
                    non_nullable_string("client_id", KafkaString::decode_compact(buf)?.0)?;
                let client_host =
                    non_nullable_string("client_host", KafkaString::decode_compact(buf)?.0)?;
                let member_metadata =
                    non_nullable_bytes("member_metadata", KafkaBytes::decode_compact(buf)?.0)?;
                let member_assignment =
                    non_nullable_bytes("member_assignment", KafkaBytes::decode_compact(buf)?.0)?;
                let _ = TaggedFields::decode(buf)?;
                members.push(DescribeGroupMember {
                    member_id,
                    group_instance_id,
                    client_id,
                    client_host,
                    member_metadata,
                    member_assignment,
                });
            }

            let authorized_operations = i32::decode(buf)?;
            let _ = TaggedFields::decode(buf)?;

            groups.push(DescribedGroup {
                error_code,
                error_message,
                group_id,
                group_state,
                protocol_type,
                protocol_data,
                members,
                authorized_operations,
            });
        }

        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}

impl VersionedEncode for DescribeGroupsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1 | 2 => self.encode_v1(buf)?,
            3 | 4 => self.encode_v3(buf)?,
            5 | 6 => self.encode_v5(buf)?,
            _ => return unsupported_encode!("DescribeGroupsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DescribeGroupsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 | 2 => Self::decode_v1(buf),
            3 => Self::decode_v3(buf),
            4 => Self::decode_v4(buf),
            5 => Self::decode_v5(buf),
            6 => Self::decode_v6(buf),
            _ => unsupported_decode!("DescribeGroupsResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use bytes::BytesMut;

    #[test]
    fn test_describe_groups_request() {
        let request = DescribeGroupsRequest {
            groups: vec!["group-1".to_string(), "group-2".to_string()],
            include_authorized_operations: false,
        };
        assert_eq!(request.groups.len(), 2);
        assert_eq!(DescribeGroupsRequest::api_key(), ApiKey::DescribeGroups);

        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_describe_groups_response_decode_v1() {
        // Build a minimal v1 response: throttle_time_ms + 1 group with 0 members
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(0);
        // group count
        buf.put_i32(1);
        // error_code
        buf.put_i16(0);
        // group_id
        let group_id = "test-group";
        buf.put_i16(group_id.len() as i16);
        buf.put_slice(group_id.as_bytes());
        // group_state
        let state = "Stable";
        buf.put_i16(state.len() as i16);
        buf.put_slice(state.as_bytes());
        // protocol_type
        let ptype = "consumer";
        buf.put_i16(ptype.len() as i16);
        buf.put_slice(ptype.as_bytes());
        // protocol_data
        let pdata = "range";
        buf.put_i16(pdata.len() as i16);
        buf.put_slice(pdata.as_bytes());
        // members count
        buf.put_i32(0);

        let response = DescribeGroupsResponse::decode_v1(&mut buf.freeze()).unwrap();
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.groups.len(), 1);
        assert_eq!(response.groups[0].group_id, "test-group");
        assert_eq!(response.groups[0].group_state, "Stable");
        assert_eq!(response.groups[0].protocol_type, "consumer");
        assert_eq!(response.groups[0].protocol_data, "range");
        assert!(response.groups[0].error_code.is_ok());
        assert!(response.groups[0].members.is_empty());
    }

    #[test]
    fn test_describe_groups_request_encode_v3_authorized_ops() {
        let request = DescribeGroupsRequest {
            groups: vec!["my-group".to_string()],
            include_authorized_operations: true,
        };

        let mut buf_v1 = BytesMut::new();
        request.encode_v1(&mut buf_v1).unwrap();

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();

        // v3 adds the include_authorized_operations bool (1 byte)
        assert_eq!(buf_v3.len(), buf_v1.len() + 1);
    }

    #[test]
    fn test_describe_groups_request_encode_v5_flexible() {
        let request = DescribeGroupsRequest {
            groups: vec!["my-group".to_string()],
            include_authorized_operations: true,
        };

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();

        let mut buf_v5 = BytesMut::new();
        request.encode_v5(&mut buf_v5).unwrap();

        // v5 uses compact encoding (varint lengths instead of i32/i16)
        assert!(!buf_v5.is_empty());
        assert_ne!(buf_v3.len(), buf_v5.len());
    }

    #[test]
    fn test_describe_groups_response_decode_v3_authorized_ops() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        buf.put_i32(10); // throttle_time_ms
        buf.put_i32(1); // group count

        buf.put_i16(0); // error_code
        let gid = b"test-group";
        buf.put_i16(gid.len() as i16);
        buf.put_slice(gid);
        let state = b"Stable";
        buf.put_i16(state.len() as i16);
        buf.put_slice(state);
        let ptype = b"consumer";
        buf.put_i16(ptype.len() as i16);
        buf.put_slice(ptype);
        let pdata = b"range";
        buf.put_i16(pdata.len() as i16);
        buf.put_slice(pdata);
        buf.put_i32(0); // member count
        buf.put_i32(0x0000_001F); // authorized_operations

        let mut data = buf.freeze();
        let resp = DescribeGroupsResponse::decode_v3(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 10);
        assert_eq!(resp.groups.len(), 1);
        assert_eq!(resp.groups[0].group_id, "test-group");
        assert_eq!(resp.groups[0].authorized_operations, 0x0000_001F);
        assert_eq!(resp.groups[0].error_message, None);
    }

    #[test]
    fn test_describe_groups_response_decode_v4_instance_id() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        buf.put_i32(0); // throttle_time_ms
        buf.put_i32(1); // group count

        buf.put_i16(0); // error_code
        let gid = b"grp";
        buf.put_i16(gid.len() as i16);
        buf.put_slice(gid);
        let state = b"Stable";
        buf.put_i16(state.len() as i16);
        buf.put_slice(state);
        let ptype = b"consumer";
        buf.put_i16(ptype.len() as i16);
        buf.put_slice(ptype);
        let pdata = b"range";
        buf.put_i16(pdata.len() as i16);
        buf.put_slice(pdata);

        // 1 member
        buf.put_i32(1);
        let mid = b"member-1";
        buf.put_i16(mid.len() as i16);
        buf.put_slice(mid);
        // group_instance_id (v4+): "inst-1"
        let inst = b"inst-1";
        buf.put_i16(inst.len() as i16);
        buf.put_slice(inst);
        let cid = b"client-1";
        buf.put_i16(cid.len() as i16);
        buf.put_slice(cid);
        let host = b"/10.0.0.1";
        buf.put_i16(host.len() as i16);
        buf.put_slice(host);
        buf.put_i32(0); // member_metadata (empty bytes)
        buf.put_i32(0); // member_assignment (empty bytes)

        buf.put_i32(i32::MIN); // authorized_operations (not requested)

        let mut data = buf.freeze();
        let resp = DescribeGroupsResponse::decode_v4(&mut data).unwrap();

        assert_eq!(resp.groups.len(), 1);
        assert_eq!(resp.groups[0].members.len(), 1);
        assert_eq!(
            resp.groups[0].members[0].group_instance_id,
            Some("inst-1".to_string())
        );
        assert_eq!(resp.groups[0].members[0].client_id, "client-1");
        assert_eq!(resp.groups[0].members[0].client_host, "/10.0.0.1");
        assert_eq!(resp.groups[0].authorized_operations, i32::MIN);
    }

    #[test]
    fn test_describe_groups_response_decode_v5_flexible() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        buf.put_i32(5); // throttle_time_ms

        // groups compact array: 1 group → varint(2)
        crate::util::varint::encode_unsigned_varint(2, &mut buf);
        {
            buf.put_i16(0); // error_code

            // group_id (compact): "grp"
            crate::util::varint::encode_unsigned_varint(4, &mut buf);
            buf.put_slice(b"grp");
            // group_state (compact): "Empty"
            crate::util::varint::encode_unsigned_varint(6, &mut buf);
            buf.put_slice(b"Empty");
            // protocol_type (compact): "consumer"
            crate::util::varint::encode_unsigned_varint(9, &mut buf);
            buf.put_slice(b"consumer");
            // protocol_data (compact): ""
            crate::util::varint::encode_unsigned_varint(1, &mut buf);

            // members compact array: 0 → varint(1)
            crate::util::varint::encode_unsigned_varint(1, &mut buf);

            buf.put_i32(0x0F); // authorized_operations
            // group tagged fields
            crate::util::varint::encode_unsigned_varint(0, &mut buf);
        }

        // top-level tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        let mut data = buf.freeze();
        let resp = DescribeGroupsResponse::decode_v5(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 5);
        assert_eq!(resp.groups.len(), 1);
        assert_eq!(resp.groups[0].group_id, "grp");
        assert_eq!(resp.groups[0].group_state, "Empty");
        assert_eq!(resp.groups[0].error_message, None);
        assert_eq!(resp.groups[0].authorized_operations, 0x0F);
        assert!(resp.groups[0].members.is_empty());
    }

    #[test]
    fn test_describe_groups_response_decode_v6_error_message() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        buf.put_i32(0); // throttle_time_ms

        // groups compact array: 1 group → varint(2)
        crate::util::varint::encode_unsigned_varint(2, &mut buf);
        {
            buf.put_i16(69); // error_code (GROUP_ID_NOT_FOUND)

            // error_message (compact nullable): "group not found"
            let msg = b"group not found";
            crate::util::varint::encode_unsigned_varint((msg.len() + 1) as u32, &mut buf);
            buf.put_slice(msg);

            // group_id (compact): "missing-grp"
            let gid = b"missing-grp";
            crate::util::varint::encode_unsigned_varint((gid.len() + 1) as u32, &mut buf);
            buf.put_slice(gid);
            // group_state (compact): ""
            crate::util::varint::encode_unsigned_varint(1, &mut buf);
            // protocol_type (compact): ""
            crate::util::varint::encode_unsigned_varint(1, &mut buf);
            // protocol_data (compact): ""
            crate::util::varint::encode_unsigned_varint(1, &mut buf);

            // members compact array: 0 → varint(1)
            crate::util::varint::encode_unsigned_varint(1, &mut buf);

            buf.put_i32(i32::MIN); // authorized_operations
            // group tagged fields
            crate::util::varint::encode_unsigned_varint(0, &mut buf);
        }

        // top-level tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        let mut data = buf.freeze();
        let resp = DescribeGroupsResponse::decode_v6(&mut data).unwrap();

        assert_eq!(resp.groups.len(), 1);
        assert!(!resp.groups[0].error_code.is_ok());
        assert_eq!(
            resp.groups[0].error_message,
            Some("group not found".to_string())
        );
        assert_eq!(resp.groups[0].group_id, "missing-grp");
    }
}
