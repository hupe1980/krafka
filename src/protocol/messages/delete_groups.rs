use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, encode_compact_array_len,
};

// ============================================================================
// DeleteGroups API (Key 42)
// ============================================================================

/// DeleteGroups request (API Key 42).
#[derive(Debug, Clone)]
pub struct DeleteGroupsRequest {
    /// Group names to delete.
    pub group_names: Vec<String>,
}

impl DeleteGroupsRequest {
    /// Create a new request.
    pub fn new(group_names: Vec<String>) -> Self {
        Self { group_names }
    }

    /// Encode for version 0–1 (non-flexible).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        array_len_i32(self.group_names.len())?.encode(buf);
        for name in &self.group_names {
            KafkaString::new(name).try_encode(buf)?;
        }
        Ok(())
    }

    /// Encode for version 2 (flexible encoding).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.group_names.len(), buf)?;
        for name in &self.group_names {
            KafkaString::new(name).try_encode_compact(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// DeleteGroups response (API Key 42).
#[derive(Debug, Clone)]
pub struct DeleteGroupsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Deletion results.
    pub results: Vec<DeletableGroupResult>,
}

/// Result for a single group deletion.
#[derive(Debug, Clone)]
pub struct DeletableGroupResult {
    /// Group ID.
    pub group_id: String,
    /// Error code.
    pub error_code: ErrorCode,
}

impl DeleteGroupsResponse {
    /// Decode from version 0–1 (non-flexible).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let group_id = non_nullable_string("group_id", KafkaString::decode(buf)?.0)?;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            results.push(DeletableGroupResult {
                group_id,
                error_code,
            });
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    /// Decode from version 2 (flexible encoding).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let group_id = non_nullable_string("group_id", KafkaString::decode_compact(buf)?.0)?;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let _ = TaggedFields::decode(buf)?;
            results.push(DeletableGroupResult {
                group_id,
                error_code,
            });
        }

        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }
}

impl VersionedEncode for DeleteGroupsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 | 1 => self.encode_v0(buf)?,
            2 => self.encode_v2(buf)?,
            _ => return unsupported_encode!("DeleteGroupsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DeleteGroupsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 | 1 => Self::decode_v0(buf),
            2 => Self::decode_v2(buf),
            _ => unsupported_decode!("DeleteGroupsResponse", version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::util::varint;
    use bytes::BytesMut;

    /// Helper: encode a compact string into `buf`.
    fn put_compact_string(buf: &mut BytesMut, s: Option<&str>) {
        match s {
            Some(val) => {
                buf.put_u8((val.len() + 1) as u8);
                buf.put_slice(val.as_bytes());
            }
            None => buf.put_u8(0),
        }
    }

    /// Helper: write empty tagged fields (varint 0).
    fn put_tagged_fields(buf: &mut BytesMut) {
        buf.put_u8(0);
    }

    #[test]
    fn test_delete_groups_request_encode_v0_round_trip() {
        let req = DeleteGroupsRequest::new(vec!["g1".to_string(), "g2".to_string()]);
        let mut buf = BytesMut::new();
        req.encode_v0(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_i32(), 2); // 2 groups
        assert_eq!(cur.get_i16(), 2);
        let mut g = vec![0u8; 2];
        cur.copy_to_slice(&mut g);
        assert_eq!(g, b"g1");
        assert_eq!(cur.get_i16(), 2);
        cur.copy_to_slice(&mut g);
        assert_eq!(g, b"g2");
        assert!(cur.is_empty());
    }

    #[test]
    fn test_delete_groups_request_encode_v2_flexible() {
        let req = DeleteGroupsRequest::new(vec!["grp".to_string()]);
        let mut buf = BytesMut::new();
        req.encode_v2(&mut buf).unwrap();

        let mut cur = &buf[..];
        let arr = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(arr, 2); // 1 + 1
        let name_v = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(name_v, 4); // len("grp") + 1
        let mut g = vec![0u8; 3];
        cur.copy_to_slice(&mut g);
        assert_eq!(g, b"grp");
        assert_eq!(cur.get_u8(), 0); // tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn test_delete_groups_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(20); // throttle_time_ms
        buf.put_i32(2); // 2 results
        buf.put_i16(3);
        buf.put_slice(b"ga1");
        buf.put_i16(0); // NONE
        buf.put_i16(2);
        buf.put_slice(b"gb");
        buf.put_i16(69); // NON_EMPTY_GROUP

        let resp = DeleteGroupsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 20);
        assert_eq!(resp.results.len(), 2);
        assert_eq!(resp.results[0].group_id, "ga1");
        assert!(resp.results[0].error_code.is_ok());
        assert_eq!(resp.results[1].group_id, "gb");
        assert!(!resp.results[1].error_code.is_ok());
    }

    #[test]
    fn test_delete_groups_response_decode_v2_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(2, &mut buf); // 1 result
        put_compact_string(&mut buf, Some("mygroup"));
        buf.put_i16(0); // NONE
        put_tagged_fields(&mut buf); // result tagged fields
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = DeleteGroupsResponse::decode_v2(&mut buf.freeze()).unwrap();
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].group_id, "mygroup");
        assert!(resp.results[0].error_code.is_ok());
    }
}
