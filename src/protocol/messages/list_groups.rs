use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{check_compact_array_len, check_decode_array_len};

// ============================================================================
// ListGroups API (Key 16)
// ============================================================================

/// ListGroups request.
#[derive(Debug, Clone)]
pub struct ListGroupsRequest {
    /// State filter (v4+, KIP-518). Empty means all states.
    pub states_filter: Vec<String>,
    /// Type filter (v5+, KIP-848). Empty means all types.
    pub types_filter: Vec<String>,
}

impl ListGroupsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::ListGroups
    }

    /// Encode for version 1–2 (empty body).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        let _ = buf;
        Ok(())
    }

    /// Encode for version 3 (flexible, empty body + tagged fields).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 4 (flexible, adds states_filter).
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        let len = u32::try_from(self.states_filter.len().saturating_add(1)).map_err(|_| {
            KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                "states_filter array too large",
            )
        })?;
        crate::util::varint::encode_unsigned_varint(len, buf);
        for state in &self.states_filter {
            KafkaString::new(state).try_encode_compact(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 5 (flexible, adds types_filter).
    pub fn encode_v5(&self, buf: &mut impl BufMut) -> Result<()> {
        let states_len =
            u32::try_from(self.states_filter.len().saturating_add(1)).map_err(|_| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::InvalidLength,
                    "states_filter array too large",
                )
            })?;
        crate::util::varint::encode_unsigned_varint(states_len, buf);
        for state in &self.states_filter {
            KafkaString::new(state).try_encode_compact(buf)?;
        }
        let types_len = u32::try_from(self.types_filter.len().saturating_add(1)).map_err(|_| {
            KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                "types_filter array too large",
            )
        })?;
        crate::util::varint::encode_unsigned_varint(types_len, buf);
        for t in &self.types_filter {
            KafkaString::new(t).try_encode_compact(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// A listed group.
#[derive(Debug, Clone)]
pub struct ListedGroup {
    /// Group ID.
    pub group_id: String,
    /// Protocol type.
    pub protocol_type: String,
    /// Group state (v4+, KIP-518).
    pub group_state: Option<String>,
    /// Group type (v5+, KIP-848).
    pub group_type: Option<String>,
}

/// ListGroups response.
#[derive(Debug, Clone)]
pub struct ListGroupsResponse {
    /// Throttle time (v1+).
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Listed groups.
    pub groups: Vec<ListedGroup>,
}

impl ListGroupsResponse {
    /// Decode from version 1–2 (non-flexible).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let group_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);
        for _ in 0..group_count {
            let group_id = non_nullable_string("group_id", KafkaString::decode(buf)?.0)?;
            let protocol_type = non_nullable_string("protocol_type", KafkaString::decode(buf)?.0)?;
            groups.push(ListedGroup {
                group_id,
                protocol_type,
                group_state: None,
                group_type: None,
            });
        }
        Ok(Self {
            throttle_time_ms,
            error_code,
            groups,
        })
    }

    /// Decode from version 3 (flexible, no new fields).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);
        for _ in 0..group_count {
            let group_id = non_nullable_string("group_id", KafkaString::decode_compact(buf)?.0)?;
            let protocol_type =
                non_nullable_string("protocol_type", KafkaString::decode_compact(buf)?.0)?;
            let _ = TaggedFields::decode(buf)?;
            groups.push(ListedGroup {
                group_id,
                protocol_type,
                group_state: None,
                group_type: None,
            });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            groups,
        })
    }

    /// Decode from version 4 (adds group_state per group, KIP-518).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);
        for _ in 0..group_count {
            let group_id = non_nullable_string("group_id", KafkaString::decode_compact(buf)?.0)?;
            let protocol_type =
                non_nullable_string("protocol_type", KafkaString::decode_compact(buf)?.0)?;
            let group_state =
                non_nullable_string("group_state", KafkaString::decode_compact(buf)?.0)?;
            let _ = TaggedFields::decode(buf)?;
            groups.push(ListedGroup {
                group_id,
                protocol_type,
                group_state: Some(group_state),
                group_type: None,
            });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            groups,
        })
    }

    /// Decode from version 5 (adds group_type per group, KIP-848).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);
        for _ in 0..group_count {
            let group_id = non_nullable_string("group_id", KafkaString::decode_compact(buf)?.0)?;
            let protocol_type =
                non_nullable_string("protocol_type", KafkaString::decode_compact(buf)?.0)?;
            let group_state =
                non_nullable_string("group_state", KafkaString::decode_compact(buf)?.0)?;
            let group_type =
                non_nullable_string("group_type", KafkaString::decode_compact(buf)?.0)?;
            let _ = TaggedFields::decode(buf)?;
            groups.push(ListedGroup {
                group_id,
                protocol_type,
                group_state: Some(group_state),
                group_type: Some(group_type),
            });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            groups,
        })
    }
}

impl VersionedEncode for ListGroupsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1 | 2 => self.encode_v1(buf)?,
            3 => self.encode_v3(buf)?,
            4 => self.encode_v4(buf)?,
            5 => self.encode_v5(buf)?,
            _ => return unsupported_encode!("ListGroupsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for ListGroupsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 | 2 => Self::decode_v1(buf),
            3 => Self::decode_v3(buf),
            4 => Self::decode_v4(buf),
            5 => Self::decode_v5(buf),
            _ => unsupported_decode!("ListGroupsResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use bytes::BytesMut;

    #[test]
    fn test_list_groups_request() {
        let request = ListGroupsRequest {
            states_filter: Vec::new(),
            types_filter: Vec::new(),
        };
        assert_eq!(ListGroupsRequest::api_key(), ApiKey::ListGroups);
        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        // ListGroups v1 has an empty body
        assert!(buf.is_empty());
    }

    #[test]
    fn test_list_groups_response_decode_v1() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(0);
        // error_code
        buf.put_i16(0);
        // groups count
        buf.put_i32(2);
        // group 1
        let g1 = "group-a";
        buf.put_i16(g1.len() as i16);
        buf.put_slice(g1.as_bytes());
        let pt1 = "consumer";
        buf.put_i16(pt1.len() as i16);
        buf.put_slice(pt1.as_bytes());
        // group 2
        let g2 = "group-b";
        buf.put_i16(g2.len() as i16);
        buf.put_slice(g2.as_bytes());
        let pt2 = "consumer";
        buf.put_i16(pt2.len() as i16);
        buf.put_slice(pt2.as_bytes());

        let response = ListGroupsResponse::decode_v1(&mut buf.freeze()).unwrap();
        assert!(response.error_code.is_ok());
        assert_eq!(response.groups.len(), 2);
        assert_eq!(response.groups[0].group_id, "group-a");
        assert_eq!(response.groups[1].group_id, "group-b");
        assert_eq!(response.groups[0].group_state, None);
        assert_eq!(response.groups[0].group_type, None);
    }

    #[test]
    fn test_list_groups_request_encode_v3_flexible() {
        let request = ListGroupsRequest {
            states_filter: Vec::new(),
            types_filter: Vec::new(),
        };

        let mut buf_v1 = BytesMut::new();
        request.encode_v1(&mut buf_v1).unwrap();
        assert!(buf_v1.is_empty());

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();

        // v3 adds tagged fields (just 0x00 for empty)
        assert_eq!(buf_v3.len(), 1);
        assert_eq!(buf_v3[0], 0x00);
    }

    #[test]
    fn test_list_groups_request_encode_v4_states_filter() {
        let request = ListGroupsRequest {
            states_filter: vec!["Stable".to_string(), "Empty".to_string()],
            types_filter: Vec::new(),
        };

        let mut buf = BytesMut::new();
        request.encode_v4(&mut buf).unwrap();

        // Should contain the state filter strings
        let data = String::from_utf8_lossy(&buf);
        assert!(data.contains("Stable"));
        assert!(data.contains("Empty"));
    }

    #[test]
    fn test_list_groups_request_encode_v5_types_filter() {
        let request = ListGroupsRequest {
            states_filter: vec!["Stable".to_string()],
            types_filter: vec!["classic".to_string(), "consumer".to_string()],
        };

        let mut buf = BytesMut::new();
        request.encode_v5(&mut buf).unwrap();

        let data = String::from_utf8_lossy(&buf);
        assert!(data.contains("Stable"));
        assert!(data.contains("classic"));
        assert!(data.contains("consumer"));
    }

    #[test]
    fn test_list_groups_response_decode_v3_flexible() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        buf.put_i32(10); // throttle_time_ms
        buf.put_i16(0); // error_code

        // groups compact array: 1 → varint(2)
        crate::util::varint::encode_unsigned_varint(2, &mut buf);
        {
            // group_id: "grp-1"
            crate::util::varint::encode_unsigned_varint(6, &mut buf);
            buf.put_slice(b"grp-1");
            // protocol_type: "consumer"
            crate::util::varint::encode_unsigned_varint(9, &mut buf);
            buf.put_slice(b"consumer");
            // element tagged fields
            crate::util::varint::encode_unsigned_varint(0, &mut buf);
        }

        // top-level tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        let mut data = buf.freeze();
        let resp = ListGroupsResponse::decode_v3(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 10);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.groups.len(), 1);
        assert_eq!(resp.groups[0].group_id, "grp-1");
        assert_eq!(resp.groups[0].protocol_type, "consumer");
        assert_eq!(resp.groups[0].group_state, None);
        assert_eq!(resp.groups[0].group_type, None);
    }

    #[test]
    fn test_list_groups_response_decode_v4_group_state() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code

        // groups compact array: 1 → varint(2)
        crate::util::varint::encode_unsigned_varint(2, &mut buf);
        {
            // group_id: "grp-1"
            crate::util::varint::encode_unsigned_varint(6, &mut buf);
            buf.put_slice(b"grp-1");
            // protocol_type: "consumer"
            crate::util::varint::encode_unsigned_varint(9, &mut buf);
            buf.put_slice(b"consumer");
            // group_state: "Stable"
            crate::util::varint::encode_unsigned_varint(7, &mut buf);
            buf.put_slice(b"Stable");
            // element tagged fields
            crate::util::varint::encode_unsigned_varint(0, &mut buf);
        }

        // top-level tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        let mut data = buf.freeze();
        let resp = ListGroupsResponse::decode_v4(&mut data).unwrap();

        assert_eq!(resp.groups.len(), 1);
        assert_eq!(resp.groups[0].group_state, Some("Stable".to_string()));
        assert_eq!(resp.groups[0].group_type, None);
    }

    #[test]
    fn test_list_groups_response_decode_v5_group_type() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code

        // groups compact array: 2 → varint(3)
        crate::util::varint::encode_unsigned_varint(3, &mut buf);
        {
            // group 1
            crate::util::varint::encode_unsigned_varint(6, &mut buf);
            buf.put_slice(b"grp-1");
            crate::util::varint::encode_unsigned_varint(9, &mut buf);
            buf.put_slice(b"consumer");
            crate::util::varint::encode_unsigned_varint(7, &mut buf);
            buf.put_slice(b"Stable");
            crate::util::varint::encode_unsigned_varint(8, &mut buf);
            buf.put_slice(b"classic");
            crate::util::varint::encode_unsigned_varint(0, &mut buf);

            // group 2
            crate::util::varint::encode_unsigned_varint(6, &mut buf);
            buf.put_slice(b"grp-2");
            crate::util::varint::encode_unsigned_varint(1, &mut buf); // empty protocol_type
            crate::util::varint::encode_unsigned_varint(6, &mut buf);
            buf.put_slice(b"Empty");
            crate::util::varint::encode_unsigned_varint(9, &mut buf);
            buf.put_slice(b"consumer");
            crate::util::varint::encode_unsigned_varint(0, &mut buf);
        }

        // top-level tagged fields
        crate::util::varint::encode_unsigned_varint(0, &mut buf);

        let mut data = buf.freeze();
        let resp = ListGroupsResponse::decode_v5(&mut data).unwrap();

        assert_eq!(resp.groups.len(), 2);
        assert_eq!(resp.groups[0].group_id, "grp-1");
        assert_eq!(resp.groups[0].group_state, Some("Stable".to_string()));
        assert_eq!(resp.groups[0].group_type, Some("classic".to_string()));
        assert_eq!(resp.groups[1].group_id, "grp-2");
        assert_eq!(resp.groups[1].group_state, Some("Empty".to_string()));
        assert_eq!(resp.groups[1].group_type, Some("consumer".to_string()));
    }
}
