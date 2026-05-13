use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode};
use crate::error::{ErrorCode, Result};
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};

/// Transaction result for partition operations.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionResult {
    /// Commit the transaction.
    Commit,
    /// Abort the transaction.
    Abort,
}

impl TransactionResult {
    /// Convert to boolean for wire format.
    #[inline]
    pub fn to_bool(self) -> bool {
        match self {
            TransactionResult::Commit => true,
            TransactionResult::Abort => false,
        }
    }

    /// Create from boolean.
    #[inline]
    pub fn from_bool(committed: bool) -> Self {
        if committed {
            TransactionResult::Commit
        } else {
            TransactionResult::Abort
        }
    }
}

/// EndTxn request (API Key 26).
///
/// Commits or aborts a transaction.
#[derive(Debug, Clone)]
pub struct EndTxnRequest {
    /// Transactional ID.
    pub transactional_id: String,
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Whether to commit (true) or abort (false).
    pub committed: bool,
}

impl EndTxnRequest {
    /// Create a commit request.
    pub fn commit(
        transactional_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Self {
        Self {
            transactional_id: transactional_id.into(),
            producer_id,
            producer_epoch,
            committed: true,
        }
    }

    /// Create an abort request.
    pub fn abort(
        transactional_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Self {
        Self {
            transactional_id: transactional_id.into(),
            producer_id,
            producer_epoch,
            committed: false,
        }
    }

    /// Encode as version 0–2.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        self.committed.encode(buf);
        Ok(())
    }

    /// Encode for version 3–5 (flexible: compact strings + tagged fields).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode_compact(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        self.committed.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// EndTxn response.
#[derive(Debug, Clone)]
pub struct EndTxnResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Bumped producer ID returned by the broker (KIP-890, v4+).
    ///
    /// `None` for protocol versions 0–3. Present when the broker supports
    /// KIP-890 epoch bumping after each `EndTxn`; the client must store this
    /// value and use it for subsequent transactions.
    pub producer_id: Option<i64>,
    /// Bumped producer epoch returned by the broker (KIP-890, v4+).
    ///
    /// `None` for protocol versions 0–3.
    pub producer_epoch: Option<i16>,
}

impl EndTxnResponse {
    /// Decode from version 0–2.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        Ok(Self {
            throttle_time_ms,
            error_code,
            producer_id: None,
            producer_epoch: None,
        })
    }

    /// Decode from version 3 (flexible: tagged fields appended, no epoch).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            producer_id: None,
            producer_epoch: None,
        })
    }

    /// Decode from version 5+ (KIP-890: bumped `ProducerId` and
    /// `ProducerEpoch` appended before tagged fields).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let producer_id = i64::decode(buf)?;
        let producer_epoch = i16::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            producer_id: Some(producer_id),
            producer_epoch: Some(producer_epoch),
        })
    }

    /// Check if successful.
    #[inline]
    pub fn is_ok(&self) -> bool {
        self.error_code.is_ok()
    }
}

impl VersionedEncode for EndTxnRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=2 => self.encode_v0(buf)?,
            3..=5 => self.encode_v3(buf)?,
            _ => return unsupported_encode!("EndTxnRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for EndTxnResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=2 => Self::decode_v0(buf),
            3..=4 => Self::decode_v3(buf),
            5 => Self::decode_v5(buf),
            _ => unsupported_decode!("EndTxnResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::protocol::primitives::{Decode, KafkaString};
    use bytes::BytesMut;
    use rstest::rstest;

    #[test]
    fn test_end_txn_v0_wire_format() {
        let request = EndTxnRequest::commit("txn-1", 100, 5);

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        let mut data = buf.freeze();

        let txn_id = KafkaString::decode(&mut data).unwrap().0.unwrap();
        assert_eq!(txn_id, "txn-1");
        assert_eq!(i64::decode(&mut data).unwrap(), 100);
        assert_eq!(i16::decode(&mut data).unwrap(), 5);
        assert_eq!(u8::from(bool::decode(&mut data).unwrap()), 1); // committed=true
        assert!(!data.has_remaining());
    }

    #[test]
    fn test_end_txn_v3_flexible() {
        let request = EndTxnRequest::commit("txn-1", 100, 5);

        let mut v0 = BytesMut::new();
        request.encode_v0(&mut v0).unwrap();
        let mut v3 = BytesMut::new();
        request.encode_v3(&mut v3).unwrap();

        // v3 appends tagged fields byte even if empty
        // Verify v3 encodes without error and both produce valid output
        assert!(!v3.is_empty());
        assert!(!v0.is_empty());
    }

    #[rstest]
    #[case::v1(1)]
    #[case::v2(2)]
    fn test_end_txn_v1_v2_same_as_v0(#[case] version: i16) {
        let request = EndTxnRequest::abort("txn-1", 100, 5);
        let mut v0 = BytesMut::new();
        request.encode_versioned(0, &mut v0).unwrap();
        let mut vn = BytesMut::new();
        request.encode_versioned(version, &mut vn).unwrap();
        assert_eq!(v0.freeze(), vn.freeze());
    }

    #[rstest]
    #[case::v3(3)]
    #[case::v4(4)]
    #[case::v5(5)]
    fn test_end_txn_v3_v5_same_wire(#[case] version: i16) {
        let request = EndTxnRequest::commit("txn-1", 100, 5);
        let mut v3 = BytesMut::new();
        request.encode_versioned(3, &mut v3).unwrap();
        let mut vn = BytesMut::new();
        request.encode_versioned(version, &mut vn).unwrap();
        assert_eq!(v3.freeze(), vn.freeze());
    }

    #[test]
    fn test_end_txn_response_v0_wire() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i16(0); // error_code

        let resp = EndTxnResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.producer_id, None);
        assert_eq!(resp.producer_epoch, None);
    }

    #[test]
    fn test_end_txn_response_v3_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(5); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_u8(0); // tagged fields

        let resp = EndTxnResponse::decode_v3(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 5);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.producer_id, None);
        assert_eq!(resp.producer_epoch, None);
    }

    #[rstest]
    #[case::v3(3)]
    fn test_end_txn_response_v3_decode_versioned(#[case] version: i16) {
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        buf.put_i16(0);
        buf.put_u8(0);
        let resp = EndTxnResponse::decode_versioned(version, &mut buf.freeze()).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.producer_id, None);
        assert_eq!(resp.producer_epoch, None);
    }

    /// KIP-890: v5 response includes bumped ProducerId and ProducerEpoch.
    #[rstest]
    #[case::v5(5)]
    fn test_end_txn_response_v5_epoch_bump(#[case] version: i16) {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code (NONE)
        buf.put_i64(42); // ProducerId (bumped)
        buf.put_i16(3); // ProducerEpoch (bumped)
        buf.put_u8(0); // tagged fields

        let resp = EndTxnResponse::decode_versioned(version, &mut buf.freeze()).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.producer_id, Some(42));
        assert_eq!(resp.producer_epoch, Some(3));
    }

    #[test]
    fn test_end_txn_response_v4_decode_versioned_uses_v3_shape() {
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        buf.put_i16(0);
        buf.put_u8(0); // tagged fields

        let resp = EndTxnResponse::decode_versioned(4, &mut buf.freeze()).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.producer_id, None);
        assert_eq!(resp.producer_epoch, None);
    }

    #[test]
    fn test_transaction_result() {
        assert!(TransactionResult::Commit.to_bool());
        assert!(!TransactionResult::Abort.to_bool());
        assert_eq!(
            TransactionResult::from_bool(true),
            TransactionResult::Commit
        );
        assert_eq!(
            TransactionResult::from_bool(false),
            TransactionResult::Abort
        );
    }

    #[test]
    fn test_end_txn_request() {
        let commit = EndTxnRequest::commit("my-txn", 12345, 0);
        assert!(commit.committed);

        let abort = EndTxnRequest::abort("my-txn", 12345, 0);
        assert!(!abort.committed);

        let mut buf = BytesMut::new();
        commit.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }
}
