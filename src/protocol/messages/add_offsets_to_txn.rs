use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode};
use crate::error::{ErrorCode, Result};
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};

/// AddOffsetsToTxn request (API Key 25).
///
/// Adds consumer group offsets to a transaction.
#[derive(Debug, Clone)]
pub struct AddOffsetsToTxnRequest {
    /// Transactional ID.
    pub transactional_id: String,
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Consumer group ID.
    pub group_id: String,
}

impl AddOffsetsToTxnRequest {
    /// Create a new request.
    pub fn new(
        transactional_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
        group_id: impl Into<String>,
    ) -> Self {
        Self {
            transactional_id: transactional_id.into(),
            producer_id,
            producer_epoch,
            group_id: group_id.into(),
        }
    }

    /// Encode as version 0–2.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        KafkaString(Some(self.group_id.clone())).try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 3–4 (flexible: compact strings + tagged fields).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode_compact(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        KafkaString(Some(self.group_id.clone())).try_encode_compact(buf)?;
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// AddOffsetsToTxn response.
#[derive(Debug, Clone)]
pub struct AddOffsetsToTxnResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
}

impl AddOffsetsToTxnResponse {
    /// Decode from version 0–2.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }

    /// Decode from version 3–4 (flexible: tagged fields appended).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
        })
    }

    /// Check if successful.
    #[inline]
    pub fn is_ok(&self) -> bool {
        self.error_code.is_ok()
    }
}

impl VersionedEncode for AddOffsetsToTxnRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=2 => self.encode_v0(buf)?,
            3 | 4 => self.encode_v3(buf)?,
            _ => return unsupported_encode!("AddOffsetsToTxnRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for AddOffsetsToTxnResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=2 => Self::decode_v0(buf),
            3 | 4 => Self::decode_v3(buf),
            _ => unsupported_decode!("AddOffsetsToTxnResponse", version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::primitives::{Decode, KafkaString};
    use bytes::BytesMut;
    use rstest::rstest;

    #[test]
    fn test_add_offsets_to_txn_v0_wire_format() {
        let request = AddOffsetsToTxnRequest::new("txn-1", 100, 5, "grp-1");

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        let mut data = buf.freeze();

        let txn_id = KafkaString::decode(&mut data).unwrap().0.unwrap();
        assert_eq!(txn_id, "txn-1");
        assert_eq!(i64::decode(&mut data).unwrap(), 100);
        assert_eq!(i16::decode(&mut data).unwrap(), 5);
        let group = KafkaString::decode(&mut data).unwrap().0.unwrap();
        assert_eq!(group, "grp-1");
        assert!(!data.has_remaining());
    }

    #[test]
    fn test_add_offsets_to_txn_v3_flexible() {
        let request = AddOffsetsToTxnRequest::new("txn-1", 100, 5, "grp-1");

        let mut v0 = BytesMut::new();
        request.encode_v0(&mut v0).unwrap();
        let mut v3 = BytesMut::new();
        request.encode_v3(&mut v3).unwrap();
        assert_ne!(v0.len(), v3.len());

        // v4 uses same wire format as v3
        let mut v4 = BytesMut::new();
        request.encode_versioned(4, &mut v4).unwrap();
        assert_eq!(v3.freeze(), v4.freeze());
    }

    #[rstest]
    #[case::v1(1)]
    #[case::v2(2)]
    fn test_add_offsets_to_txn_v1_v2_same_as_v0(#[case] version: i16) {
        let request = AddOffsetsToTxnRequest::new("txn-1", 100, 5, "grp-1");
        let mut v0 = BytesMut::new();
        request.encode_versioned(0, &mut v0).unwrap();
        let mut vn = BytesMut::new();
        request.encode_versioned(version, &mut vn).unwrap();
        assert_eq!(v0.freeze(), vn.freeze());
    }

    #[test]
    fn test_add_offsets_to_txn_response_v0_wire() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i16(0); // error_code

        let resp = AddOffsetsToTxnResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        assert!(resp.error_code.is_ok());
    }

    #[test]
    fn test_add_offsets_to_txn_response_v3_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(5); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_u8(0); // tagged fields

        let resp = AddOffsetsToTxnResponse::decode_v3(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 5);
        assert!(resp.error_code.is_ok());
    }

    #[rstest]
    #[case::v3(3)]
    #[case::v4(4)]
    fn test_add_offsets_to_txn_response_v3_v4_decode(#[case] version: i16) {
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        buf.put_i16(0);
        buf.put_u8(0);
        let resp = AddOffsetsToTxnResponse::decode_versioned(version, &mut buf.freeze()).unwrap();
        assert!(resp.error_code.is_ok());
    }

    #[test]
    fn test_add_offsets_to_txn_request() {
        let request = AddOffsetsToTxnRequest::new("my-txn", 12345, 0, "my-group");

        assert_eq!(request.transactional_id, "my-txn");
        assert_eq!(request.group_id, "my-group");

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }
}
