use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{check_compact_array_len, encode_compact_array_len};

// ============================================================================
// ListTransactions API (Key 66)
//
// v0 baseline. All versions use flexible encoding.
// v1 adds DurationFilter (KIP-994).
// ============================================================================

/// ListTransactions request.
#[derive(Debug, Clone)]
pub struct ListTransactionsRequest {
    /// Filter by transaction states (empty = all).
    pub state_filters: Vec<String>,
    /// Filter by producer IDs (empty = all).
    pub producer_id_filters: Vec<i64>,
    /// Filter by minimum duration in millis (v1+). `-1` for no filter.
    pub duration_filter: i64,
}

impl ListTransactionsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::ListTransactions
    }

    /// Create a request that lists all transactions.
    pub fn all() -> Self {
        Self {
            state_filters: Vec::new(),
            producer_id_filters: Vec::new(),
            duration_filter: -1,
        }
    }

    /// Encode for version 0 (flexible encoding).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.state_filters.len(), buf)?;
        for s in &self.state_filters {
            KafkaString::new(s).try_encode_compact(buf)?;
        }
        encode_compact_array_len(self.producer_id_filters.len(), buf)?;
        for &pid in &self.producer_id_filters {
            pid.encode(buf);
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 1 (adds duration filter).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.state_filters.len(), buf)?;
        for s in &self.state_filters {
            KafkaString::new(s).try_encode_compact(buf)?;
        }
        encode_compact_array_len(self.producer_id_filters.len(), buf)?;
        for &pid in &self.producer_id_filters {
            pid.encode(buf);
        }
        self.duration_filter.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for ListTransactionsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf),
            1 => self.encode_v1(buf),
            _ => unsupported_encode!("ListTransactionsRequest", version),
        }
    }
}

// ── Response ─────────────────────────────────────────────────────────────

/// A transaction state entry from ListTransactions.
#[derive(Debug, Clone)]
pub struct ListTransactionsStateEntry {
    /// Transactional ID.
    pub transactional_id: String,
    /// Producer ID.
    pub producer_id: i64,
    /// Current transaction state.
    pub transaction_state: String,
}

/// ListTransactions response.
#[derive(Debug, Clone)]
pub struct ListTransactionsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code.
    pub error_code: ErrorCode,
    /// State filters that were not recognized by the coordinator.
    pub unknown_state_filters: Vec<String>,
    /// Transaction states.
    pub transaction_states: Vec<ListTransactionsStateEntry>,
}

impl ListTransactionsResponse {
    /// Decode from version 0–1 (both have the same response wire format).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);

        let unknown_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut unknown_state_filters = Vec::with_capacity(unknown_count);
        for _ in 0..unknown_count {
            unknown_state_filters.push(non_nullable_string(
                "unknown_state_filter",
                KafkaString::decode_compact(buf)?.0,
            )?);
        }

        let state_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut transaction_states = Vec::with_capacity(state_count);
        for _ in 0..state_count {
            let transactional_id =
                non_nullable_string("transactional_id", KafkaString::decode_compact(buf)?.0)?;
            let producer_id = i64::decode(buf)?;
            let transaction_state =
                non_nullable_string("transaction_state", KafkaString::decode_compact(buf)?.0)?;
            let _ = TaggedFields::decode(buf)?;
            transaction_states.push(ListTransactionsStateEntry {
                transactional_id,
                producer_id,
                transaction_state,
            });
        }
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            unknown_state_filters,
            transaction_states,
        })
    }
}

impl VersionedDecode for ListTransactionsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=1 => Self::decode_v0(buf),
            _ => unsupported_decode!("ListTransactionsResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use bytes::BytesMut;

    fn put_compact_string(buf: &mut BytesMut, s: &str) {
        crate::util::varint::encode_unsigned_varint((s.len() + 1) as u32, buf);
        buf.put_slice(s.as_bytes());
    }

    fn put_empty_tagged_fields(buf: &mut BytesMut) {
        crate::util::varint::encode_unsigned_varint(0, buf);
    }

    fn put_compact_array_len(buf: &mut BytesMut, count: usize) {
        crate::util::varint::encode_unsigned_varint((count + 1) as u32, buf);
    }

    #[test]
    fn test_list_transactions_api_key() {
        assert_eq!(ListTransactionsRequest::api_key(), ApiKey::ListTransactions);
    }

    #[test]
    fn test_list_transactions_request_all() {
        let request = ListTransactionsRequest::all();
        assert!(request.state_filters.is_empty());
        assert!(request.producer_id_filters.is_empty());
        assert_eq!(request.duration_filter, -1);
    }

    #[test]
    fn test_list_transactions_request_encode_v0() {
        let request = ListTransactionsRequest {
            state_filters: vec!["Ongoing".to_string()],
            producer_id_filters: vec![1000],
            duration_filter: -1,
        };
        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_list_transactions_request_encode_v1() {
        let request = ListTransactionsRequest {
            state_filters: Vec::new(),
            producer_id_filters: Vec::new(),
            duration_filter: 60_000,
        };
        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_list_transactions_versioned_unsupported() {
        let request = ListTransactionsRequest::all();
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(2, &mut buf).is_err());
    }

    #[test]
    fn test_list_transactions_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        put_compact_array_len(&mut buf, 0); // unknown_state_filters
        put_compact_array_len(&mut buf, 2); // transaction_states
        // txn 1
        put_compact_string(&mut buf, "txn-1");
        buf.put_i64(1000); // producer_id
        put_compact_string(&mut buf, "Ongoing");
        put_empty_tagged_fields(&mut buf);
        // txn 2
        put_compact_string(&mut buf, "txn-2");
        buf.put_i64(2000); // producer_id
        put_compact_string(&mut buf, "PrepareCommit");
        put_empty_tagged_fields(&mut buf);
        put_empty_tagged_fields(&mut buf); // top-level

        let resp = ListTransactionsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert!(resp.error_code.is_ok());
        assert!(resp.unknown_state_filters.is_empty());
        assert_eq!(resp.transaction_states.len(), 2);
        assert_eq!(resp.transaction_states[0].transactional_id, "txn-1");
        assert_eq!(resp.transaction_states[0].producer_id, 1000);
        assert_eq!(resp.transaction_states[0].transaction_state, "Ongoing");
        assert_eq!(resp.transaction_states[1].transactional_id, "txn-2");
        assert_eq!(
            resp.transaction_states[1].transaction_state,
            "PrepareCommit"
        );
    }

    #[test]
    fn test_list_transactions_response_with_unknown_filters() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        put_compact_array_len(&mut buf, 1); // unknown_state_filters
        put_compact_string(&mut buf, "BadState");
        put_compact_array_len(&mut buf, 0); // empty transaction_states
        put_empty_tagged_fields(&mut buf);

        let resp = ListTransactionsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.unknown_state_filters, vec!["BadState"]);
        assert!(resp.transaction_states.is_empty());
    }

    #[test]
    fn test_list_transactions_versioned_decode_unsupported() {
        let buf = BytesMut::new();
        assert!(ListTransactionsResponse::decode_versioned(2, &mut buf.freeze()).is_err());
    }

    #[test]
    fn test_list_transactions_versioned_encode_dispatches() {
        let request = ListTransactionsRequest::all();
        for v in 0..=1 {
            let mut buf = BytesMut::new();
            request.encode_versioned(v, &mut buf).unwrap();
            assert!(!buf.is_empty(), "v{v} should produce non-empty output");
        }
    }
}
