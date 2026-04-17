use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode};
use crate::error::{ErrorCode, Result};
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};

// ============================================================================
// InitProducerId (API Key 22) - Idempotent Producer Support
// ============================================================================

/// Request to initialize producer ID for idempotent/transactional production.
#[derive(Debug, Clone)]
pub struct InitProducerIdRequest {
    /// Transactional ID (null for non-transactional producers).
    pub transactional_id: Option<String>,
    /// Transaction timeout in milliseconds (-1 for non-transactional).
    pub transaction_timeout_ms: i32,
    /// Producer ID to use (for recovery; -1 for new producer).
    pub producer_id: i64,
    /// Producer epoch to use (for recovery; -1 for new producer).
    pub producer_epoch: i16,
    /// Enable two-phase commit for transactions (v6+, KIP-939).
    pub enable_2pc: bool,
    /// Keep ongoing prepared transaction instead of aborting (v6+, KIP-939).
    pub keep_prepared_txn: bool,
}

impl InitProducerIdRequest {
    /// Create a request for a non-transactional idempotent producer.
    #[inline]
    pub fn idempotent() -> Self {
        Self {
            transactional_id: None,
            transaction_timeout_ms: -1,
            producer_id: -1,
            producer_epoch: -1,
            enable_2pc: false,
            keep_prepared_txn: false,
        }
    }

    /// Create a request for a transactional producer.
    #[inline]
    pub fn transactional(transactional_id: &str, timeout_ms: i32) -> Self {
        Self {
            transactional_id: Some(transactional_id.to_string()),
            transaction_timeout_ms: timeout_ms,
            producer_id: -1,
            producer_epoch: -1,
            enable_2pc: false,
            keep_prepared_txn: false,
        }
    }

    /// Encode as version 0–1.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(self.transactional_id.clone()).try_encode(buf)?;
        self.transaction_timeout_ms.encode(buf);
        Ok(())
    }

    /// Encode for version 2 (flexible: compact strings + tagged fields).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(self.transactional_id.clone()).try_encode_compact(buf)?;
        self.transaction_timeout_ms.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 3–5 (flexible + ProducerId/ProducerEpoch for epoch recovery).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(self.transactional_id.clone()).try_encode_compact(buf)?;
        self.transaction_timeout_ms.encode(buf);
        buf.put_i64(self.producer_id);
        buf.put_i16(self.producer_epoch);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 6 (KIP-939: two-phase commit).
    pub fn encode_v6(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(self.transactional_id.clone()).try_encode_compact(buf)?;
        self.transaction_timeout_ms.encode(buf);
        buf.put_i64(self.producer_id);
        buf.put_i16(self.producer_epoch);
        buf.put_u8(u8::from(self.enable_2pc));
        buf.put_u8(u8::from(self.keep_prepared_txn));
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Response from InitProducerId.
#[derive(Debug, Clone)]
pub struct InitProducerIdResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Producer ID assigned by the broker.
    pub producer_id: i64,
    /// Producer epoch assigned by the broker.
    pub producer_epoch: i16,
    /// Producer ID for ongoing transaction when KeepPreparedTxn is used (v6+, KIP-939).
    pub ongoing_txn_producer_id: i64,
    /// Producer epoch for ongoing transaction when KeepPreparedTxn is used (v6+, KIP-939).
    pub ongoing_txn_producer_epoch: i16,
}

impl InitProducerIdResponse {
    /// Decode from version 0–1.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let producer_id = i64::decode(buf)?;
        let producer_epoch = i16::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
            ongoing_txn_producer_id: -1,
            ongoing_txn_producer_epoch: -1,
        })
    }

    /// Decode from version 2–5 (flexible: tagged fields appended).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let producer_id = i64::decode(buf)?;
        let producer_epoch = i16::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
            ongoing_txn_producer_id: -1,
            ongoing_txn_producer_epoch: -1,
        })
    }

    /// Decode from version 6 (KIP-939: two-phase commit).
    pub fn decode_v6(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let producer_id = i64::decode(buf)?;
        let producer_epoch = i16::decode(buf)?;
        let ongoing_txn_producer_id = i64::decode(buf)?;
        let ongoing_txn_producer_epoch = i16::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            producer_id,
            producer_epoch,
            ongoing_txn_producer_id,
            ongoing_txn_producer_epoch,
        })
    }

    /// Check if the response indicates success.
    #[inline]
    pub fn is_ok(&self) -> bool {
        self.error_code.is_ok()
    }
}

impl VersionedEncode for InitProducerIdRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 | 1 => self.encode_v0(buf)?,
            2 => self.encode_v2(buf)?,
            3..=5 => self.encode_v3(buf)?,
            6 => self.encode_v6(buf)?,
            _ => return unsupported_encode!("InitProducerIdRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for InitProducerIdResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 | 1 => Self::decode_v0(buf),
            2..=5 => Self::decode_v2(buf),
            6 => Self::decode_v6(buf),
            _ => unsupported_decode!("InitProducerIdResponse", version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::varint;
    use bytes::BytesMut;
    use rstest::rstest;

    #[test]
    fn test_init_producer_id_request() {
        let request = InitProducerIdRequest::idempotent();
        assert!(request.transactional_id.is_none());
        assert_eq!(request.producer_id, -1);
        assert_eq!(request.producer_epoch, -1);

        let request = InitProducerIdRequest::transactional("my-txn", 60000);
        assert_eq!(request.transactional_id.as_deref(), Some("my-txn"));
        assert_eq!(request.transaction_timeout_ms, 60000);
    }

    // ── InitProducerId wire-format tests ──

    #[test]
    fn test_init_producer_id_request_v0_wire_format() {
        let request = InitProducerIdRequest::transactional("txn-1", 30000);
        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        let mut cur = &buf[..];
        // nullable string: len=5 "txn-1"
        assert_eq!(cur.get_i16(), 5);
        let mut name = vec![0u8; 5];
        cur.copy_to_slice(&mut name);
        assert_eq!(name, b"txn-1");
        assert_eq!(cur.get_i32(), 30000);
        assert!(cur.is_empty());
    }

    #[test]
    fn test_init_producer_id_request_v1_same_as_v0() {
        let request = InitProducerIdRequest::transactional("t", 1000);
        let mut buf_v0 = BytesMut::new();
        request.encode_versioned(0, &mut buf_v0).unwrap();
        let mut buf_v1 = BytesMut::new();
        request.encode_versioned(1, &mut buf_v1).unwrap();
        assert_eq!(buf_v0, buf_v1);
    }

    #[test]
    fn test_init_producer_id_request_v2_flexible() {
        let request = InitProducerIdRequest::idempotent();
        let mut buf = BytesMut::new();
        request.encode_v2(&mut buf).unwrap();
        let mut cur = &buf[..];
        // compact nullable string: varint(0) = null
        let len_varint = crate::util::varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(len_varint, 0); // null transactional_id
        assert_eq!(cur.get_i32(), -1); // transaction_timeout_ms
        assert_eq!(cur.get_u8(), 0); // empty tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn test_init_producer_id_request_v3_includes_pid_epoch() {
        let mut request = InitProducerIdRequest::transactional("txn", 5000);
        request.producer_id = 42;
        request.producer_epoch = 3;
        let mut buf = BytesMut::new();
        request.encode_v3(&mut buf).unwrap();
        let mut cur = &buf[..];
        // compact string: varint(4) then 3 bytes
        let name_varint = crate::util::varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(name_varint, 4); // len+1=3+1
        let mut name = vec![0u8; 3];
        cur.copy_to_slice(&mut name);
        assert_eq!(name, b"txn");
        assert_eq!(cur.get_i32(), 5000);
        assert_eq!(cur.get_i64(), 42); // producer_id
        assert_eq!(cur.get_i16(), 3); // producer_epoch
        assert_eq!(cur.get_u8(), 0); // tagged fields
        assert!(cur.is_empty());
    }

    #[rstest]
    #[case::v3(3)]
    #[case::v4(4)]
    #[case::v5(5)]
    fn test_init_producer_id_request_v3_v5_same_wire(#[case] version: i16) {
        let request = InitProducerIdRequest::transactional("t", 1000);
        let mut buf_v3 = BytesMut::new();
        request.encode_versioned(3, &mut buf_v3).unwrap();
        let mut buf = BytesMut::new();
        request.encode_versioned(version, &mut buf).unwrap();
        assert_eq!(buf, buf_v3, "v{version} encode should equal v3");
    }

    #[test]
    fn test_init_producer_id_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(50); // throttle_time_ms
        buf.put_i16(0); // error_code (NONE)
        buf.put_i64(1000); // producer_id
        buf.put_i16(5); // producer_epoch
        let mut data = buf.freeze();
        let resp = InitProducerIdResponse::decode_v0(&mut data).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.producer_id, 1000);
        assert_eq!(resp.producer_epoch, 5);
    }

    #[test]
    fn test_init_producer_id_response_decode_v2_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i64(42); // producer_id
        buf.put_i16(1); // producer_epoch
        buf.put_u8(0); // tagged fields
        let mut data = buf.freeze();
        let resp = InitProducerIdResponse::decode_v2(&mut data).unwrap();
        assert_eq!(resp.producer_id, 42);
        assert_eq!(resp.producer_epoch, 1);
    }

    #[rstest]
    #[case::v2(2)]
    #[case::v3(3)]
    #[case::v4(4)]
    #[case::v5(5)]
    fn test_init_producer_id_response_v2_v5_decode(#[case] version: i16) {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i64(99); // producer_id
        buf.put_i16(7); // producer_epoch
        buf.put_u8(0); // tagged fields
        let mut data = buf.freeze();
        let resp = InitProducerIdResponse::decode_versioned(version, &mut data).unwrap();
        assert_eq!(resp.producer_id, 99);
        assert_eq!(resp.producer_epoch, 7);
    }

    // ===================================================================
    // Story 18.5: InitProducerId v6 Round-Trip Test
    // ===================================================================

    #[test]
    fn test_init_producer_id_v6_round_trip() {
        let req = InitProducerIdRequest::idempotent();
        let mut buf = BytesMut::new();
        req.encode_versioned(6, &mut buf).unwrap();
        assert!(!buf.is_empty());

        // Build a v6 response manually.
        let mut resp_buf = BytesMut::new();
        resp_buf.put_i32(5); // throttle_time_ms
        resp_buf.put_i16(0); // error_code (None)
        resp_buf.put_i64(100); // producer_id
        resp_buf.put_i16(1); // producer_epoch
        resp_buf.put_i64(200); // ongoing_txn_producer_id
        resp_buf.put_i16(2); // ongoing_txn_producer_epoch
        varint::encode_unsigned_varint(0, &mut resp_buf); // tagged fields

        let resp = InitProducerIdResponse::decode_versioned(6, &mut resp_buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 5);
        assert_eq!(resp.producer_id, 100);
        assert_eq!(resp.producer_epoch, 1);
        assert_eq!(resp.ongoing_txn_producer_id, 200);
        assert_eq!(resp.ongoing_txn_producer_epoch, 2);
    }

    #[test]
    fn test_init_producer_id_v2_sets_new_fields_defaults() {
        // v2 decode should set ongoing_txn fields to -1.
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i64(42); // producer_id
        buf.put_i16(0); // producer_epoch
        varint::encode_unsigned_varint(0, &mut buf); // tagged fields

        let resp = InitProducerIdResponse::decode_versioned(2, &mut buf.freeze()).unwrap();
        assert_eq!(resp.ongoing_txn_producer_id, -1);
        assert_eq!(resp.ongoing_txn_producer_epoch, -1);
    }
}
