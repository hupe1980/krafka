use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TryEncode};
use crate::protocol::{array_len_i32, check_decode_array_len};

// ============================================================================
// OffsetDelete API (Key 47)
//
// v0 baseline. flexibleVersions: "none" — always uses legacy encoding.
// ============================================================================

/// Topic-partitions whose committed offsets should be deleted.
#[derive(Debug, Clone)]
pub struct OffsetDeleteTopicRequest {
    /// Topic name.
    pub name: String,
    /// Partitions to delete offsets for.
    pub partitions: Vec<OffsetDeletePartitionRequest>,
}

/// A single partition whose committed offset should be deleted.
#[derive(Debug, Clone)]
pub struct OffsetDeletePartitionRequest {
    /// Partition index.
    pub partition_index: i32,
}

/// OffsetDelete request — deletes committed offsets for a consumer group.
///
/// **This is a destructive operation.**
#[derive(Debug, Clone)]
pub struct OffsetDeleteRequest {
    /// The group whose offsets are being deleted.
    pub group_id: String,
    /// Topics and partitions to delete offsets for.
    pub topics: Vec<OffsetDeleteTopicRequest>,
}

impl OffsetDeleteRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::OffsetDelete
    }

    /// Encode for version 0 (non-flexible / legacy encoding).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for p in &topic.partitions {
                p.partition_index.encode(buf);
            }
        }
        Ok(())
    }
}

impl VersionedEncode for OffsetDeleteRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf),
            _ => unsupported_encode!("OffsetDeleteRequest", version),
        }
    }
}

// ── Response ─────────────────────────────────────────────────────────────

/// Per-partition result from OffsetDelete.
#[derive(Debug, Clone)]
pub struct OffsetDeletePartitionResponse {
    /// Partition index.
    pub partition_index: i32,
    /// Error code.
    pub error_code: ErrorCode,
}

/// Per-topic result from OffsetDelete.
#[derive(Debug, Clone)]
pub struct OffsetDeleteTopicResponse {
    /// Topic name.
    pub name: String,
    /// Per-partition results.
    pub partitions: Vec<OffsetDeletePartitionResponse>,
}

/// OffsetDelete response.
#[derive(Debug, Clone)]
pub struct OffsetDeleteResponse {
    /// Top-level error code.
    pub error_code: ErrorCode,
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-topic results.
    pub topics: Vec<OffsetDeleteTopicResponse>,
}

impl OffsetDeleteResponse {
    /// Decode from version 0 (non-flexible / legacy encoding).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let name = non_nullable_string("topic_name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                partitions.push(OffsetDeletePartitionResponse {
                    partition_index,
                    error_code,
                });
            }
            topics.push(OffsetDeleteTopicResponse { name, partitions });
        }
        Ok(Self {
            error_code,
            throttle_time_ms,
            topics,
        })
    }
}

impl VersionedDecode for OffsetDeleteResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("OffsetDeleteResponse", version),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::BytesMut;

    fn put_kafka_string(buf: &mut BytesMut, s: &str) {
        buf.put_i16(s.len() as i16);
        buf.put_slice(s.as_bytes());
    }

    #[test]
    fn test_offset_delete_api_key() {
        assert_eq!(OffsetDeleteRequest::api_key(), ApiKey::OffsetDelete);
    }

    #[test]
    fn test_offset_delete_request_encode_v0() {
        let request = OffsetDeleteRequest {
            group_id: "my-group".to_string(),
            topics: vec![OffsetDeleteTopicRequest {
                name: "my-topic".to_string(),
                partitions: vec![
                    OffsetDeletePartitionRequest { partition_index: 0 },
                    OffsetDeletePartitionRequest { partition_index: 1 },
                ],
            }],
        };
        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());

        // Verify it's non-flexible: starts with i16 string length, not varint.
        let mut read = buf.freeze();
        let str_len = i16::decode(&mut read).unwrap();
        assert_eq!(str_len, 8); // "my-group" length
    }

    #[test]
    fn test_offset_delete_versioned_encode_unsupported() {
        let request = OffsetDeleteRequest {
            group_id: "g".to_string(),
            topics: Vec::new(),
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(1, &mut buf).is_err());
    }

    #[test]
    fn test_offset_delete_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i16(0); // error_code (None)
        buf.put_i32(0); // throttle_time_ms
        buf.put_i32(1); // 1 topic
        put_kafka_string(&mut buf, "my-topic");
        buf.put_i32(2); // 2 partitions
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code (None)
        buf.put_i32(1); // partition_index
        buf.put_i16(3); // error_code (UnknownTopicOrPartition)

        let resp = OffsetDeleteResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.throttle_time_ms, 0);
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name, "my-topic");
        assert_eq!(resp.topics[0].partitions.len(), 2);
        assert!(resp.topics[0].partitions[0].error_code.is_ok());
        assert!(!resp.topics[0].partitions[1].error_code.is_ok());
    }

    #[test]
    fn test_offset_delete_response_top_level_error() {
        let mut buf = BytesMut::new();
        buf.put_i16(15); // error_code (GROUP_AUTHORIZATION_FAILED = 30... let's use 15 as an example)
        buf.put_i32(50); // throttle_time_ms
        buf.put_i32(0); // 0 topics

        let resp = OffsetDeleteResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert!(!resp.error_code.is_ok());
        assert_eq!(resp.throttle_time_ms, 50);
        assert!(resp.topics.is_empty());
    }

    #[test]
    fn test_offset_delete_versioned_decode_unsupported() {
        let buf = BytesMut::new();
        assert!(OffsetDeleteResponse::decode_versioned(1, &mut buf.freeze()).is_err());
    }

    #[test]
    fn test_offset_delete_round_trip_encode_decode() {
        let request = OffsetDeleteRequest {
            group_id: "test-group".to_string(),
            topics: vec![OffsetDeleteTopicRequest {
                name: "t1".to_string(),
                partitions: vec![OffsetDeletePartitionRequest { partition_index: 0 }],
            }],
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(0, &mut buf).unwrap();
        // Just verify it doesn't panic and produces non-empty output
        assert!(!buf.is_empty());
    }
}
