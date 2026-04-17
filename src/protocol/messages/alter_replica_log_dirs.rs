use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, encode_compact_array_len,
};

// ============================================================================
// AlterReplicaLogDirs API (Key 34)
//
// v0 removed in Kafka 4.0; v1 is the baseline (non-flexible).
// v2 flexible encoding.
// ============================================================================

/// Topic-partitions to move to a specific log directory.
#[derive(Debug, Clone)]
pub struct AlterReplicaLogDirTopic {
    /// Topic name.
    pub name: String,
    /// Partition indexes to move.
    pub partitions: Vec<i32>,
}

/// A log directory assignment: move the given topic-partitions to `path`.
#[derive(Debug, Clone)]
pub struct AlterReplicaLogDir {
    /// Absolute path of the target log directory on the broker.
    pub path: String,
    /// Topics and partitions to move into this directory.
    pub topics: Vec<AlterReplicaLogDirTopic>,
}

/// AlterReplicaLogDirs request.
#[derive(Debug, Clone)]
pub struct AlterReplicaLogDirsRequest {
    /// Log directory assignments.
    pub dirs: Vec<AlterReplicaLogDir>,
}

impl AlterReplicaLogDirsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::AlterReplicaLogDirs
    }

    /// Encode for version 1 (non-flexible).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.dirs.len())?);
        for dir in &self.dirs {
            KafkaString::new(&dir.path).try_encode(buf)?;
            buf.put_i32(array_len_i32(dir.topics.len())?);
            for topic in &dir.topics {
                KafkaString::new(&topic.name).try_encode(buf)?;
                buf.put_i32(array_len_i32(topic.partitions.len())?);
                for &p in &topic.partitions {
                    p.encode(buf);
                }
            }
        }
        Ok(())
    }

    /// Encode for version 2 (flexible encoding).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.dirs.len(), buf)?;
        for dir in &self.dirs {
            KafkaString::new(&dir.path).try_encode_compact(buf)?;
            encode_compact_array_len(dir.topics.len(), buf)?;
            for topic in &dir.topics {
                KafkaString::new(&topic.name).try_encode_compact(buf)?;
                encode_compact_array_len(topic.partitions.len(), buf)?;
                for &p in &topic.partitions {
                    p.encode(buf);
                }
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for AlterReplicaLogDirsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1 => self.encode_v1(buf),
            2 => self.encode_v2(buf),
            _ => unsupported_encode!("AlterReplicaLogDirsRequest", version),
        }
    }
}

// ── Response ─────────────────────────────────────────────────────────────

/// Per-partition result from AlterReplicaLogDirs.
#[derive(Debug, Clone)]
pub struct AlterReplicaLogDirsPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Error code.
    pub error_code: ErrorCode,
}

/// Per-topic result from AlterReplicaLogDirs.
#[derive(Debug, Clone)]
pub struct AlterReplicaLogDirsTopicResult {
    /// Topic name.
    pub topic_name: String,
    /// Per-partition results.
    pub partitions: Vec<AlterReplicaLogDirsPartitionResult>,
}

/// AlterReplicaLogDirs response.
#[derive(Debug, Clone)]
pub struct AlterReplicaLogDirsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-topic results.
    pub results: Vec<AlterReplicaLogDirsTopicResult>,
}

impl AlterReplicaLogDirsResponse {
    /// Decode from version 1 (non-flexible).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut results = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let topic_name = non_nullable_string("topic_name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                partitions.push(AlterReplicaLogDirsPartitionResult {
                    partition_index,
                    error_code,
                });
            }
            results.push(AlterReplicaLogDirsTopicResult {
                topic_name,
                partitions,
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
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut results = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let topic_name =
                non_nullable_string("topic_name", KafkaString::decode_compact(buf)?.0)?;
            let partition_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let _ = TaggedFields::decode(buf)?;
                partitions.push(AlterReplicaLogDirsPartitionResult {
                    partition_index,
                    error_code,
                });
            }
            let _ = TaggedFields::decode(buf)?;
            results.push(AlterReplicaLogDirsTopicResult {
                topic_name,
                partitions,
            });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            results,
        })
    }
}

impl VersionedDecode for AlterReplicaLogDirsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 => Self::decode_v1(buf),
            2 => Self::decode_v2(buf),
            _ => unsupported_decode!("AlterReplicaLogDirsResponse", version),
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
    fn test_alter_replica_log_dirs_api_key() {
        assert_eq!(
            AlterReplicaLogDirsRequest::api_key(),
            ApiKey::AlterReplicaLogDirs
        );
    }

    #[test]
    fn test_alter_replica_log_dirs_encode_v1() {
        let request = AlterReplicaLogDirsRequest {
            dirs: vec![AlterReplicaLogDir {
                path: "/data/kafka-logs".to_string(),
                topics: vec![AlterReplicaLogDirTopic {
                    name: "my-topic".to_string(),
                    partitions: vec![0, 1, 2],
                }],
            }],
        };
        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_alter_replica_log_dirs_encode_v2() {
        let request = AlterReplicaLogDirsRequest {
            dirs: vec![AlterReplicaLogDir {
                path: "/data/kafka-logs-2".to_string(),
                topics: vec![AlterReplicaLogDirTopic {
                    name: "other-topic".to_string(),
                    partitions: vec![0],
                }],
            }],
        };
        let mut buf = BytesMut::new();
        request.encode_v2(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_alter_replica_log_dirs_versioned_dispatch() {
        let request = AlterReplicaLogDirsRequest { dirs: Vec::new() };
        for v in 1..=2 {
            let mut buf = BytesMut::new();
            request.encode_versioned(v, &mut buf).unwrap();
        }
    }

    #[test]
    fn test_alter_replica_log_dirs_versioned_unsupported() {
        let request = AlterReplicaLogDirsRequest { dirs: Vec::new() };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(0, &mut buf).is_err());
        assert!(request.encode_versioned(3, &mut buf).is_err());
    }

    #[test]
    fn test_alter_replica_log_dirs_response_decode_v1() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i32(1); // 1 topic
        put_kafka_string(&mut buf, "my-topic");
        buf.put_i32(2); // 2 partitions
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code (None)
        buf.put_i32(1); // partition_index
        buf.put_i16(0); // error_code (None)

        let resp = AlterReplicaLogDirsResponse::decode_v1(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 0);
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].topic_name, "my-topic");
        assert_eq!(resp.results[0].partitions.len(), 2);
        assert!(resp.results[0].partitions[0].error_code.is_ok());
        assert!(resp.results[0].partitions[1].error_code.is_ok());
    }

    #[test]
    fn test_alter_replica_log_dirs_response_decode_v2() {
        let mut buf = BytesMut::new();
        buf.put_i32(100); // throttle_time_ms
        put_compact_array_len(&mut buf, 1); // 1 topic
        put_compact_string(&mut buf, "my-topic");
        put_compact_array_len(&mut buf, 1); // 1 partition
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code
        put_empty_tagged_fields(&mut buf); // partition tagged fields
        put_empty_tagged_fields(&mut buf); // topic tagged fields
        put_empty_tagged_fields(&mut buf); // top-level

        let resp = AlterReplicaLogDirsResponse::decode_v2(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 100);
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].topic_name, "my-topic");
        assert!(resp.results[0].partitions[0].error_code.is_ok());
    }

    #[test]
    fn test_alter_replica_log_dirs_versioned_decode_dispatch() {
        // v1 decode
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        buf.put_i32(0); // 0 topics
        let resp = AlterReplicaLogDirsResponse::decode_versioned(1, &mut buf.freeze()).unwrap();
        assert!(resp.results.is_empty());

        // v2 decode
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        put_compact_array_len(&mut buf, 0);
        put_empty_tagged_fields(&mut buf);
        let resp = AlterReplicaLogDirsResponse::decode_versioned(2, &mut buf.freeze()).unwrap();
        assert!(resp.results.is_empty());
    }

    #[test]
    fn test_alter_replica_log_dirs_versioned_decode_unsupported() {
        let buf = BytesMut::new();
        assert!(
            AlterReplicaLogDirsResponse::decode_versioned(0, &mut buf.clone().freeze()).is_err()
        );
        assert!(AlterReplicaLogDirsResponse::decode_versioned(3, &mut buf.freeze()).is_err());
    }
}
