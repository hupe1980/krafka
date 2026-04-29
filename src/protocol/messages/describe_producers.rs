use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{check_compact_array_len, encode_compact_array_len};

// ============================================================================
// DescribeProducers API (Key 61)
//
// v0 baseline. All versions use flexible encoding.
// ============================================================================

/// Topic-partitions to describe active producers for.
#[derive(Debug, Clone)]
pub struct DescribeProducersTopicRequest {
    /// Topic name.
    pub name: String,
    /// Partition indexes to describe.
    pub partition_indexes: Vec<i32>,
}

/// DescribeProducers request.
#[derive(Debug, Clone)]
pub struct DescribeProducersRequest {
    /// Topics and partitions to describe.
    pub topics: Vec<DescribeProducersTopicRequest>,
}

impl DescribeProducersRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::DescribeProducers
    }

    /// Encode for version 0 (flexible encoding).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode_compact(buf)?;
            encode_compact_array_len(topic.partition_indexes.len(), buf)?;
            for &p in &topic.partition_indexes {
                p.encode(buf);
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for DescribeProducersRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf),
            _ => unsupported_encode!("DescribeProducersRequest", version),
        }
    }
}

// ── Response ─────────────────────────────────────────────────────────────

/// Active producer state on a partition.
#[derive(Debug, Clone)]
pub struct ProducerState {
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i32,
    /// Last sequence number sent. `-1` if unknown.
    pub last_sequence: i32,
    /// Last timestamp sent. `-1` if unknown.
    pub last_timestamp: i64,
    /// Current epoch of the producer group.
    pub coordinator_epoch: i32,
    /// Current transaction start offset. `-1` if not in a transaction.
    pub current_txn_start_offset: i64,
}

/// Per-partition producer description.
#[derive(Debug, Clone)]
pub struct DescribeProducersPartitionResponse {
    /// Partition index.
    pub partition_index: i32,
    /// Error code for this partition.
    pub error_code: ErrorCode,
    /// Error message, or `None` if no error.
    pub error_message: Option<String>,
    /// Active producers on this partition.
    pub active_producers: Vec<ProducerState>,
}

/// Per-topic producer description.
#[derive(Debug, Clone)]
pub struct DescribeProducersTopicResponse {
    /// Topic name.
    pub name: String,
    /// Per-partition results.
    pub partitions: Vec<DescribeProducersPartitionResponse>,
}

/// DescribeProducers response.
#[derive(Debug, Clone)]
pub struct DescribeProducersResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-topic results.
    pub topics: Vec<DescribeProducersTopicResponse>,
}

impl DescribeProducersResponse {
    /// Decode from version 0 (flexible encoding).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let name = non_nullable_string("topic", KafkaString::decode_compact(buf)?.0)?;
            let partition_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let error_message = KafkaString::decode_compact(buf)?.0;
                let producer_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut active_producers = Vec::with_capacity(producer_count);
                for _ in 0..producer_count {
                    let producer_id = i64::decode(buf)?;
                    let producer_epoch = i32::decode(buf)?;
                    let last_sequence = i32::decode(buf)?;
                    let last_timestamp = i64::decode(buf)?;
                    let coordinator_epoch = i32::decode(buf)?;
                    let current_txn_start_offset = i64::decode(buf)?;
                    let _ = TaggedFields::decode(buf)?;
                    active_producers.push(ProducerState {
                        producer_id,
                        producer_epoch,
                        last_sequence,
                        last_timestamp,
                        coordinator_epoch,
                        current_txn_start_offset,
                    });
                }
                let _ = TaggedFields::decode(buf)?;
                partitions.push(DescribeProducersPartitionResponse {
                    partition_index,
                    error_code,
                    error_message,
                    active_producers,
                });
            }
            let _ = TaggedFields::decode(buf)?;
            topics.push(DescribeProducersTopicResponse { name, partitions });
        }
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

impl VersionedDecode for DescribeProducersResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("DescribeProducersResponse", version),
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

    fn put_compact_null_string(buf: &mut BytesMut) {
        crate::util::varint::encode_unsigned_varint(0, buf);
    }

    fn put_empty_tagged_fields(buf: &mut BytesMut) {
        crate::util::varint::encode_unsigned_varint(0, buf);
    }

    fn put_compact_array_len(buf: &mut BytesMut, count: usize) {
        crate::util::varint::encode_unsigned_varint((count + 1) as u32, buf);
    }

    #[test]
    fn test_describe_producers_api_key() {
        assert_eq!(
            DescribeProducersRequest::api_key(),
            ApiKey::DescribeProducers
        );
    }

    #[test]
    fn test_describe_producers_request_encode_v0() {
        let request = DescribeProducersRequest {
            topics: vec![DescribeProducersTopicRequest {
                name: "my-topic".to_string(),
                partition_indexes: vec![0, 1],
            }],
        };
        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_describe_producers_versioned_unsupported() {
        let request = DescribeProducersRequest { topics: Vec::new() };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(1, &mut buf).is_err());
    }

    #[test]
    fn test_describe_producers_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        put_compact_array_len(&mut buf, 1); // topics
        put_compact_string(&mut buf, "my-topic");
        put_compact_array_len(&mut buf, 1); // partitions
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code
        put_compact_null_string(&mut buf); // error_message
        put_compact_array_len(&mut buf, 1); // active_producers
        buf.put_i64(1000); // producer_id
        buf.put_i32(5); // producer_epoch
        buf.put_i32(42); // last_sequence
        buf.put_i64(1_700_000_000_000); // last_timestamp
        buf.put_i32(3); // coordinator_epoch
        buf.put_i64(100); // current_txn_start_offset
        put_empty_tagged_fields(&mut buf); // producer tagged fields
        put_empty_tagged_fields(&mut buf); // partition tagged fields
        put_empty_tagged_fields(&mut buf); // topic tagged fields
        put_empty_tagged_fields(&mut buf); // top-level

        let resp = DescribeProducersResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name, "my-topic");
        let p = &resp.topics[0].partitions[0];
        assert!(p.error_code.is_ok());
        assert_eq!(p.active_producers.len(), 1);
        assert_eq!(p.active_producers[0].producer_id, 1000);
        assert_eq!(p.active_producers[0].producer_epoch, 5);
        assert_eq!(p.active_producers[0].last_sequence, 42);
        assert_eq!(p.active_producers[0].current_txn_start_offset, 100);
    }

    #[test]
    fn test_describe_producers_versioned_decode_unsupported() {
        let buf = BytesMut::new();
        assert!(DescribeProducersResponse::decode_versioned(1, &mut buf.freeze()).is_err());
    }
}
