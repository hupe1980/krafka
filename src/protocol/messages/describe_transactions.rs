use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{check_compact_array_len, decode_capacity, encode_compact_array_len};

// ============================================================================
// DescribeTransactions API (Key 65)
//
// v0 baseline. All versions use flexible encoding.
// ============================================================================

/// DescribeTransactions request.
#[derive(Debug, Clone)]
pub struct DescribeTransactionsRequest {
    /// Transactional IDs to describe.
    pub transactional_ids: Vec<String>,
}

impl DescribeTransactionsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::DescribeTransactions
    }

    /// Encode for version 0 (flexible encoding).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.transactional_ids.len(), buf)?;
        for tid in &self.transactional_ids {
            KafkaString::new(tid).try_encode_compact(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for DescribeTransactionsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf),
            _ => unsupported_encode!("DescribeTransactionsRequest", version),
        }
    }
}

// ── Response ─────────────────────────────────────────────────────────────

/// Topic-partitions involved in a transaction.
#[derive(Debug, Clone)]
pub struct TransactionTopicData {
    /// Topic name.
    pub topic: String,
    /// Partition IDs included in the transaction.
    pub partitions: Vec<i32>,
}

/// State of a single transaction.
#[derive(Debug, Clone)]
pub struct TransactionStateEntry {
    /// Error code.
    pub error_code: ErrorCode,
    /// Transactional ID.
    pub transactional_id: String,
    /// Current transaction state (e.g. "Ongoing", "PrepareCommit").
    pub transaction_state: String,
    /// Transaction timeout in milliseconds.
    pub transaction_timeout_ms: i32,
    /// Transaction start time in milliseconds.
    pub transaction_start_time_ms: i64,
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Topic-partitions in the current transaction.
    pub topics: Vec<TransactionTopicData>,
}

/// DescribeTransactions response.
#[derive(Debug, Clone)]
pub struct DescribeTransactionsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Transaction states.
    pub transaction_states: Vec<TransactionStateEntry>,
}

impl DescribeTransactionsResponse {
    /// Decode from version 0 (flexible encoding).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let state_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut transaction_states =
            Vec::with_capacity(decode_capacity(state_count, buf.remaining()));
        for _ in 0..state_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let transactional_id =
                non_nullable_string("transactional_id", KafkaString::decode_compact(buf)?.0)?;
            let transaction_state =
                non_nullable_string("transaction_state", KafkaString::decode_compact(buf)?.0)?;
            let transaction_timeout_ms = i32::decode(buf)?;
            let transaction_start_time_ms = i64::decode(buf)?;
            let producer_id = i64::decode(buf)?;
            let producer_epoch = i16::decode(buf)?;

            let topic_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut topics = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));
            for _ in 0..topic_count {
                let topic = non_nullable_string("topic", KafkaString::decode_compact(buf)?.0)?;
                let partition_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut partitions =
                    Vec::with_capacity(decode_capacity(partition_count, buf.remaining()));
                for _ in 0..partition_count {
                    partitions.push(i32::decode(buf)?);
                }
                let _ = TaggedFields::decode(buf)?;
                topics.push(TransactionTopicData { topic, partitions });
            }
            let _ = TaggedFields::decode(buf)?;
            transaction_states.push(TransactionStateEntry {
                error_code,
                transactional_id,
                transaction_state,
                transaction_timeout_ms,
                transaction_start_time_ms,
                producer_id,
                producer_epoch,
                topics,
            });
        }
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            transaction_states,
        })
    }
}

impl VersionedDecode for DescribeTransactionsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("DescribeTransactionsResponse", version),
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
    fn test_describe_transactions_api_key() {
        assert_eq!(
            DescribeTransactionsRequest::api_key(),
            ApiKey::DescribeTransactions
        );
    }

    #[test]
    fn test_describe_transactions_request_encode_v0() {
        let request = DescribeTransactionsRequest {
            transactional_ids: vec!["txn-1".to_string(), "txn-2".to_string()],
        };
        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_describe_transactions_versioned_unsupported() {
        let request = DescribeTransactionsRequest {
            transactional_ids: Vec::new(),
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(1, &mut buf).is_err());
    }

    #[test]
    fn test_describe_transactions_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        put_compact_array_len(&mut buf, 1); // transaction_states
        buf.put_i16(0); // error_code
        put_compact_string(&mut buf, "txn-1"); // transactional_id
        put_compact_string(&mut buf, "Ongoing"); // transaction_state
        buf.put_i32(30_000); // transaction_timeout_ms
        buf.put_i64(1_700_000_000_000); // transaction_start_time_ms
        buf.put_i64(1000); // producer_id
        buf.put_i16(5); // producer_epoch
        put_compact_array_len(&mut buf, 1); // topics
        put_compact_string(&mut buf, "my-topic");
        put_compact_array_len(&mut buf, 2); // partitions
        buf.put_i32(0);
        buf.put_i32(1);
        put_empty_tagged_fields(&mut buf); // topic tagged fields
        put_empty_tagged_fields(&mut buf); // state tagged fields
        put_empty_tagged_fields(&mut buf); // top-level

        let resp = DescribeTransactionsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.transaction_states.len(), 1);
        let s = &resp.transaction_states[0];
        assert!(s.error_code.is_ok());
        assert_eq!(s.transactional_id, "txn-1");
        assert_eq!(s.transaction_state, "Ongoing");
        assert_eq!(s.producer_id, 1000);
        assert_eq!(s.producer_epoch, 5);
        assert_eq!(s.topics.len(), 1);
        assert_eq!(s.topics[0].topic, "my-topic");
        assert_eq!(s.topics[0].partitions, vec![0, 1]);
    }

    #[test]
    fn test_describe_transactions_versioned_decode_unsupported() {
        let buf = BytesMut::new();
        assert!(DescribeTransactionsResponse::decode_versioned(1, &mut buf.freeze()).is_err());
    }
}
