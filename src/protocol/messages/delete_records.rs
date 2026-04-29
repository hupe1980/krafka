use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, encode_compact_array_len,
};

// ============================================================================
// DeleteRecords API (Key 21)
// ============================================================================

/// Partition data for DeleteRecords request.
#[derive(Debug, Clone)]
pub struct DeleteRecordsPartition {
    /// Partition index.
    pub partition_index: i32,
    /// The offset before which records should be deleted.
    /// Records with offsets less than this value will be deleted.
    pub offset: i64,
}

/// Topic data for DeleteRecords request.
#[derive(Debug, Clone)]
pub struct DeleteRecordsTopic {
    /// Topic name.
    pub name: String,
    /// Partitions.
    pub partitions: Vec<DeleteRecordsPartition>,
}

/// DeleteRecords request.
#[derive(Debug, Clone)]
pub struct DeleteRecordsRequest {
    /// Topics to delete records from.
    pub topics: Vec<DeleteRecordsTopic>,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
}

impl DeleteRecordsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::DeleteRecords
    }

    /// Encode for version 0–1 (v1 same wire format as v0).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition_index.encode(buf);
                partition.offset.encode(buf);
            }
        }
        self.timeout_ms.encode(buf);
        Ok(())
    }

    /// Encode for version 2 (flexible encoding).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode_compact(buf)?;
            encode_compact_array_len(topic.partitions.len(), buf)?;
            for partition in &topic.partitions {
                partition.partition_index.encode(buf);
                partition.offset.encode(buf);
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        self.timeout_ms.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Partition result for DeleteRecords response.
#[derive(Debug, Clone)]
pub struct DeleteRecordsPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Low watermark after deletion.
    pub low_watermark: i64,
    /// Error code.
    pub error_code: ErrorCode,
}

/// Topic result for DeleteRecords response.
#[derive(Debug, Clone)]
pub struct DeleteRecordsTopicResult {
    /// Topic name.
    pub name: String,
    /// Partitions.
    pub partitions: Vec<DeleteRecordsPartitionResult>,
}

/// DeleteRecords response.
#[derive(Debug, Clone)]
pub struct DeleteRecordsResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Topic results.
    pub topics: Vec<DeleteRecordsTopicResult>,
}

impl DeleteRecordsResponse {
    /// Decode from version 0–1 (v1 same wire format as v0).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let low_watermark = i64::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                partitions.push(DeleteRecordsPartitionResult {
                    partition_index,
                    low_watermark,
                    error_code,
                });
            }

            topics.push(DeleteRecordsTopicResult { name, partitions });
        }

        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }

    /// Decode from version 2 (flexible encoding).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let partition_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let low_watermark = i64::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let _ = TaggedFields::decode(buf)?;
                partitions.push(DeleteRecordsPartitionResult {
                    partition_index,
                    low_watermark,
                    error_code,
                });
            }

            let _ = TaggedFields::decode(buf)?;
            topics.push(DeleteRecordsTopicResult { name, partitions });
        }

        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

impl VersionedEncode for DeleteRecordsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 | 1 => self.encode_v0(buf)?,
            2 => self.encode_v2(buf)?,
            _ => return unsupported_encode!("DeleteRecordsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DeleteRecordsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 | 1 => Self::decode_v0(buf),
            2 => Self::decode_v2(buf),
            _ => unsupported_decode!("DeleteRecordsResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use bytes::BytesMut;

    #[test]
    fn test_delete_records_request() {
        let request = DeleteRecordsRequest {
            topics: vec![DeleteRecordsTopic {
                name: "my-topic".to_string(),
                partitions: vec![
                    DeleteRecordsPartition {
                        partition_index: 0,
                        offset: 100,
                    },
                    DeleteRecordsPartition {
                        partition_index: 1,
                        offset: 200,
                    },
                ],
            }],
            timeout_ms: 30000,
        };
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].partitions.len(), 2);
        assert_eq!(DeleteRecordsRequest::api_key(), ApiKey::DeleteRecords);

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_delete_records_response_decode_v0() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(0);
        // topics count
        buf.put_i32(1);
        // topic name
        let topic = "my-topic";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic.as_bytes());
        // partitions count
        buf.put_i32(1);
        // partition_index
        buf.put_i32(0);
        // low_watermark
        buf.put_i64(100);
        // error_code
        buf.put_i16(0);

        let response = DeleteRecordsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].name, "my-topic");
        assert_eq!(response.topics[0].partitions.len(), 1);
        assert_eq!(response.topics[0].partitions[0].partition_index, 0);
        assert_eq!(response.topics[0].partitions[0].low_watermark, 100);
        assert!(response.topics[0].partitions[0].error_code.is_ok());
    }

    #[test]
    fn test_delete_records_v1_same_as_v0() {
        let request = DeleteRecordsRequest {
            topics: vec![DeleteRecordsTopic {
                name: "test".to_string(),
                partitions: vec![DeleteRecordsPartition {
                    partition_index: 0,
                    offset: 100,
                }],
            }],
            timeout_ms: 30_000,
        };
        let mut v0 = BytesMut::new();
        request.encode_versioned(0, &mut v0).unwrap();
        let mut v1 = BytesMut::new();
        request.encode_versioned(1, &mut v1).unwrap();
        assert_eq!(v0, v1); // v1 same wire format as v0
    }

    #[test]
    fn test_delete_records_v2_flexible() {
        let request = DeleteRecordsRequest {
            topics: vec![DeleteRecordsTopic {
                name: "test".to_string(),
                partitions: vec![DeleteRecordsPartition {
                    partition_index: 0,
                    offset: 100,
                }],
            }],
            timeout_ms: 30_000,
        };
        let mut v0 = BytesMut::new();
        request.encode_v0(&mut v0).unwrap();
        let mut v2 = BytesMut::new();
        request.encode_v2(&mut v2).unwrap();
        assert_ne!(v0.len(), v2.len());
    }

    #[test]
    fn test_delete_records_response_v2_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(2); // topics: compact array count=1+1=2
        buf.put_u8(5); // topic name: compact string "test"
        buf.put_slice(b"test");
        buf.put_u8(2); // partitions: compact array count=1+1=2
        buf.put_i32(0); // partition_index
        buf.put_i64(50); // low_watermark
        buf.put_i16(0); // error_code
        buf.put_u8(0); // per-partition tagged fields
        buf.put_u8(0); // per-topic tagged fields
        buf.put_u8(0); // top-level tagged fields

        let mut frozen = buf.freeze();
        let resp = DeleteRecordsResponse::decode_v2(&mut frozen).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name, "test");
        assert_eq!(resp.topics[0].partitions[0].low_watermark, 50);
    }
}
