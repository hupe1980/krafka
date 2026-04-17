use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, encode_compact_array_len,
};

// ============================================================================
// DescribeLogDirs API (Key 35)
//
// v0 removed in Kafka 4.0; v1 is the baseline.
// v2 flexible encoding (compact strings + tagged fields).
// v3 adds top-level ErrorCode.
// v4 adds TotalBytes + UsableBytes per log dir.
// ============================================================================

/// Topic filter for a DescribeLogDirs request.
#[derive(Debug, Clone)]
pub struct DescribableLogDirTopic {
    /// Topic name.
    pub topic: String,
    /// Partition indexes to describe. If empty, all partitions are described.
    pub partitions: Vec<i32>,
}

/// DescribeLogDirs request.
///
/// When `topics` is `None`, all log directories for all topics on the broker
/// are described. When `Some`, only the specified topic-partitions are included.
#[derive(Debug, Clone)]
pub struct DescribeLogDirsRequest {
    /// Topics to describe, or `None` for all topics.
    pub topics: Option<Vec<DescribableLogDirTopic>>,
}

impl DescribeLogDirsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::DescribeLogDirs
    }

    /// Create a request for all log directories.
    pub fn all() -> Self {
        Self { topics: None }
    }

    /// Create a request for specific topics and partitions.
    pub fn for_topics(topics: Vec<DescribableLogDirTopic>) -> Self {
        Self {
            topics: Some(topics),
        }
    }

    /// Encode for version 1 (non-flexible).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        match &self.topics {
            None => {
                // Nullable array: -1 means null (all topics).
                buf.put_i32(-1);
            }
            Some(topics) => {
                buf.put_i32(array_len_i32(topics.len())?);
                for topic in topics {
                    KafkaString::new(&topic.topic).try_encode(buf)?;
                    buf.put_i32(array_len_i32(topic.partitions.len())?);
                    for &partition in &topic.partitions {
                        partition.encode(buf);
                    }
                }
            }
        }
        Ok(())
    }

    /// Encode for version 2+ (flexible encoding).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        match &self.topics {
            None => {
                // Compact nullable array: 0 means null.
                crate::util::varint::encode_unsigned_varint(0, buf);
            }
            Some(topics) => {
                encode_compact_array_len(topics.len(), buf)?;
                for topic in topics {
                    KafkaString::new(&topic.topic).try_encode_compact(buf)?;
                    encode_compact_array_len(topic.partitions.len(), buf)?;
                    for &partition in &topic.partitions {
                        partition.encode(buf);
                    }
                    TaggedFields::default().try_encode(buf)?;
                }
            }
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for DescribeLogDirsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1 => self.encode_v1(buf)?,
            2..=4 => self.encode_v2(buf)?,
            _ => return unsupported_encode!("DescribeLogDirsRequest", version),
        }
        Ok(())
    }
}

// ── Response ─────────────────────────────────────────────────────────────

/// Partition info within a log directory.
#[derive(Debug, Clone)]
pub struct DescribeLogDirsPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Size of the log segments in this partition in bytes.
    pub partition_size: i64,
    /// Lag of the log's LEO with respect to the partition's high watermark
    /// (current log) or the current replica's LEO (future log).
    pub offset_lag: i64,
    /// `true` if this log was created by `AlterReplicaLogDirs` and will
    /// replace the current log in the future.
    pub is_future_key: bool,
}

/// Topic info within a log directory.
#[derive(Debug, Clone)]
pub struct DescribeLogDirsTopic {
    /// Topic name.
    pub name: String,
    /// Partition details.
    pub partitions: Vec<DescribeLogDirsPartition>,
}

/// A single log directory result.
#[derive(Debug, Clone)]
pub struct DescribeLogDirsResult {
    /// Per-directory error code.
    pub error_code: ErrorCode,
    /// Absolute path of the log directory.
    pub log_dir: String,
    /// Topics stored in this directory.
    pub topics: Vec<DescribeLogDirsTopic>,
    /// Total size in bytes of the volume (v4+). `-1` if unavailable.
    pub total_bytes: i64,
    /// Usable size in bytes of the volume (v4+). `-1` if unavailable.
    pub usable_bytes: i64,
}

/// DescribeLogDirs response.
#[derive(Debug, Clone)]
pub struct DescribeLogDirsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code (v3+). `ErrorCode::None` for older versions.
    pub error_code: ErrorCode,
    /// Log directory results.
    pub results: Vec<DescribeLogDirsResult>,
}

impl DescribeLogDirsResponse {
    /// Decode from version 1 (non-flexible, no top-level error, no volume info).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count = check_decode_array_len(i32::decode(buf)?)?;
        let results = Self::decode_results_v1(buf, result_count)?;

        Ok(Self {
            throttle_time_ms,
            error_code: ErrorCode::None,
            results,
        })
    }

    /// Decode from version 2 (flexible, no top-level error, no volume info).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let results = Self::decode_results_v2(buf, result_count, false)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code: ErrorCode::None,
            results,
        })
    }

    /// Decode from version 3 (flexible, top-level error, no volume info).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let result_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let results = Self::decode_results_v2(buf, result_count, false)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            results,
        })
    }

    /// Decode from version 4 (flexible, top-level error, volume info).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let result_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let results = Self::decode_results_v2(buf, result_count, true)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            results,
        })
    }

    /// Decode result entries with non-flexible encoding (v1).
    ///
    /// Non-flexible versions never carry volume info, so `total_bytes` and
    /// `usable_bytes` default to `-1`.
    fn decode_results_v1(buf: &mut impl Buf, count: usize) -> Result<Vec<DescribeLogDirsResult>> {
        let mut results = Vec::with_capacity(count);
        for _ in 0..count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let log_dir = non_nullable_string("log_dir", KafkaString::decode(buf)?.0)?;
            let topic_count = check_decode_array_len(i32::decode(buf)?)?;
            let topics = Self::decode_topics_v1(buf, topic_count)?;

            results.push(DescribeLogDirsResult {
                error_code,
                log_dir,
                topics,
                total_bytes: -1,
                usable_bytes: -1,
            });
        }
        Ok(results)
    }

    /// Decode result entries with flexible encoding (v2+).
    fn decode_results_v2(
        buf: &mut impl Buf,
        count: usize,
        has_volume_info: bool,
    ) -> Result<Vec<DescribeLogDirsResult>> {
        let mut results = Vec::with_capacity(count);
        for _ in 0..count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let log_dir = non_nullable_string("log_dir", KafkaString::decode_compact(buf)?.0)?;
            let topic_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let topics = Self::decode_topics_v2(buf, topic_count)?;

            let (total_bytes, usable_bytes) = if has_volume_info {
                (i64::decode(buf)?, i64::decode(buf)?)
            } else {
                (-1, -1)
            };

            let _ = TaggedFields::decode(buf)?;
            results.push(DescribeLogDirsResult {
                error_code,
                log_dir,
                topics,
                total_bytes,
                usable_bytes,
            });
        }
        Ok(results)
    }

    /// Decode topic entries with non-flexible encoding (v1).
    fn decode_topics_v1(buf: &mut impl Buf, count: usize) -> Result<Vec<DescribeLogDirsTopic>> {
        let mut topics = Vec::with_capacity(count);
        for _ in 0..count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let partitions = Self::decode_partitions_v1(buf, partition_count)?;
            topics.push(DescribeLogDirsTopic { name, partitions });
        }
        Ok(topics)
    }

    /// Decode topic entries with flexible encoding (v2+).
    fn decode_topics_v2(buf: &mut impl Buf, count: usize) -> Result<Vec<DescribeLogDirsTopic>> {
        let mut topics = Vec::with_capacity(count);
        for _ in 0..count {
            let name = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let partition_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let partitions = Self::decode_partitions_v2(buf, partition_count)?;
            let _ = TaggedFields::decode(buf)?;
            topics.push(DescribeLogDirsTopic { name, partitions });
        }
        Ok(topics)
    }

    /// Decode partition entries (non-flexible).
    fn decode_partitions_v1(
        buf: &mut impl Buf,
        count: usize,
    ) -> Result<Vec<DescribeLogDirsPartition>> {
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            let partition_index = i32::decode(buf)?;
            let partition_size = i64::decode(buf)?;
            let offset_lag = i64::decode(buf)?;
            let is_future_key = bool::decode(buf)?;
            partitions.push(DescribeLogDirsPartition {
                partition_index,
                partition_size,
                offset_lag,
                is_future_key,
            });
        }
        Ok(partitions)
    }

    /// Decode partition entries (flexible).
    fn decode_partitions_v2(
        buf: &mut impl Buf,
        count: usize,
    ) -> Result<Vec<DescribeLogDirsPartition>> {
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            let partition_index = i32::decode(buf)?;
            let partition_size = i64::decode(buf)?;
            let offset_lag = i64::decode(buf)?;
            let is_future_key = bool::decode(buf)?;
            let _ = TaggedFields::decode(buf)?;
            partitions.push(DescribeLogDirsPartition {
                partition_index,
                partition_size,
                offset_lag,
                is_future_key,
            });
        }
        Ok(partitions)
    }
}

impl VersionedDecode for DescribeLogDirsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 => Self::decode_v1(buf),
            2 => Self::decode_v2(buf),
            3 => Self::decode_v3(buf),
            4 => Self::decode_v4(buf),
            _ => unsupported_decode!("DescribeLogDirsResponse", version),
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

    #[test]
    fn test_describe_log_dirs_request_all() {
        let request = DescribeLogDirsRequest::all();
        assert!(request.topics.is_none());
        assert_eq!(DescribeLogDirsRequest::api_key(), ApiKey::DescribeLogDirs);

        // v1 encode: null array = -1
        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        assert_eq!(&buf[..], &(-1i32).to_be_bytes());
    }

    #[test]
    fn test_describe_log_dirs_request_for_topics() {
        let request = DescribeLogDirsRequest::for_topics(vec![DescribableLogDirTopic {
            topic: "my-topic".to_string(),
            partitions: vec![0, 1, 2],
        }]);
        assert_eq!(request.topics.as_ref().unwrap().len(), 1);

        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_describe_log_dirs_request_roundtrip_v1() {
        let request = DescribeLogDirsRequest::for_topics(vec![
            DescribableLogDirTopic {
                topic: "topic-a".to_string(),
                partitions: vec![0, 1],
            },
            DescribableLogDirTopic {
                topic: "topic-b".to_string(),
                partitions: vec![2],
            },
        ]);

        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        // Verify non-empty and starts with array count = 2
        assert_eq!(i32::from_be_bytes(buf[0..4].try_into().unwrap()), 2);
    }

    #[test]
    fn test_describe_log_dirs_request_roundtrip_v2() {
        let request = DescribeLogDirsRequest::for_topics(vec![DescribableLogDirTopic {
            topic: "flex-topic".to_string(),
            partitions: vec![0],
        }]);

        let mut buf = BytesMut::new();
        request.encode_v2(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_describe_log_dirs_response_decode_v1() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(0);
        // results count = 1
        buf.put_i32(1);
        // result[0].error_code
        buf.put_i16(0);
        // result[0].log_dir = "/var/kafka-logs"
        let dir = "/var/kafka-logs";
        buf.put_i16(dir.len() as i16);
        buf.put_slice(dir.as_bytes());
        // result[0].topics count = 1
        buf.put_i32(1);
        // topic.name = "my-topic"
        let topic = "my-topic";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic.as_bytes());
        // topic.partitions count = 1
        buf.put_i32(1);
        // partition_index
        buf.put_i32(0);
        // partition_size
        buf.put_i64(1024);
        // offset_lag
        buf.put_i64(5);
        // is_future_key
        buf.put_u8(0);

        let resp = DescribeLogDirsResponse::decode_v1(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 0);
        assert_eq!(resp.error_code, ErrorCode::None);
        assert_eq!(resp.results.len(), 1);

        let result = &resp.results[0];
        assert!(result.error_code.is_ok());
        assert_eq!(result.log_dir, "/var/kafka-logs");
        assert_eq!(result.total_bytes, -1);
        assert_eq!(result.usable_bytes, -1);
        assert_eq!(result.topics.len(), 1);

        let topic = &result.topics[0];
        assert_eq!(topic.name, "my-topic");
        assert_eq!(topic.partitions.len(), 1);

        let partition = &topic.partitions[0];
        assert_eq!(partition.partition_index, 0);
        assert_eq!(partition.partition_size, 1024);
        assert_eq!(partition.offset_lag, 5);
        assert!(!partition.is_future_key);
    }

    #[test]
    fn test_describe_log_dirs_response_decode_v1_error() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(100);
        // results count = 1
        buf.put_i32(1);
        // result[0].error_code = KAFKA_STORAGE_ERROR (56)
        buf.put_i16(56);
        // result[0].log_dir
        let dir = "/mnt/broken";
        buf.put_i16(dir.len() as i16);
        buf.put_slice(dir.as_bytes());
        // result[0].topics count = 0
        buf.put_i32(0);

        let resp = DescribeLogDirsResponse::decode_v1(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 100);
        assert_eq!(resp.results.len(), 1);
        assert!(!resp.results[0].error_code.is_ok());
        assert_eq!(resp.results[0].log_dir, "/mnt/broken");
        assert!(resp.results[0].topics.is_empty());
    }

    #[test]
    fn test_describe_log_dirs_response_decode_v1_multiple_dirs() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i32(2); // results count

        // dir 1
        buf.put_i16(0); // error_code
        let dir1 = "/data1";
        buf.put_i16(dir1.len() as i16);
        buf.put_slice(dir1.as_bytes());
        buf.put_i32(0); // topics count

        // dir 2
        buf.put_i16(0); // error_code
        let dir2 = "/data2";
        buf.put_i16(dir2.len() as i16);
        buf.put_slice(dir2.as_bytes());
        buf.put_i32(0); // topics count

        let resp = DescribeLogDirsResponse::decode_v1(&mut buf.freeze()).unwrap();
        assert_eq!(resp.results.len(), 2);
        assert_eq!(resp.results[0].log_dir, "/data1");
        assert_eq!(resp.results[1].log_dir, "/data2");
    }

    #[test]
    fn test_describe_log_dirs_versioned_encode_unsupported() {
        let request = DescribeLogDirsRequest::all();
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(0, &mut buf).is_err());
        assert!(request.encode_versioned(5, &mut buf).is_err());
    }

    #[test]
    fn test_describe_log_dirs_versioned_decode_unsupported() {
        let buf = BytesMut::new();
        assert!(DescribeLogDirsResponse::decode_versioned(0, &mut buf.clone().freeze()).is_err());
        assert!(DescribeLogDirsResponse::decode_versioned(5, &mut buf.freeze()).is_err());
    }

    #[test]
    fn test_describe_log_dirs_request_null_topics_v2() {
        let request = DescribeLogDirsRequest::all();
        let mut buf = BytesMut::new();
        request.encode_v2(&mut buf).unwrap();
        // Compact nullable array null = varint(0), then tagged fields (varint(0))
        assert_eq!(buf.len(), 2);
    }

    /// Build a compact (flexible) string: varint(len+1) followed by raw bytes.
    fn put_compact_string(buf: &mut BytesMut, s: &str) {
        crate::util::varint::encode_unsigned_varint((s.len() + 1) as u32, buf);
        buf.put_slice(s.as_bytes());
    }

    /// Write a zero-length tagged-fields section.
    fn put_empty_tagged_fields(buf: &mut BytesMut) {
        crate::util::varint::encode_unsigned_varint(0, buf);
    }

    /// Write a compact array length: varint(count + 1).
    fn put_compact_array_len(buf: &mut BytesMut, count: usize) {
        crate::util::varint::encode_unsigned_varint((count + 1) as u32, buf);
    }

    #[test]
    fn test_describe_log_dirs_response_decode_v3_top_level_error() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(50);
        // top-level error_code = CLUSTER_AUTHORIZATION_FAILED (31)
        buf.put_i16(31);
        // results compact array: count = 1
        put_compact_array_len(&mut buf, 1);
        // result[0].error_code
        buf.put_i16(0);
        // result[0].log_dir compact string
        put_compact_string(&mut buf, "/var/log");
        // topics compact array: count = 0
        put_compact_array_len(&mut buf, 0);
        // result tagged fields
        put_empty_tagged_fields(&mut buf);
        // top-level tagged fields
        put_empty_tagged_fields(&mut buf);

        let resp = DescribeLogDirsResponse::decode_v3(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert!(!resp.error_code.is_ok());
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].log_dir, "/var/log");
        assert!(resp.results[0].error_code.is_ok());
        // v3 still has no volume info
        assert_eq!(resp.results[0].total_bytes, -1);
        assert_eq!(resp.results[0].usable_bytes, -1);
    }

    #[test]
    fn test_describe_log_dirs_response_decode_v4_volume_info() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(0);
        // top-level error_code = NONE
        buf.put_i16(0);
        // results compact array: count = 1
        put_compact_array_len(&mut buf, 1);
        // result[0].error_code
        buf.put_i16(0);
        // result[0].log_dir
        put_compact_string(&mut buf, "/data/kafka");
        // topics compact array: count = 1
        put_compact_array_len(&mut buf, 1);
        // topic name
        put_compact_string(&mut buf, "events");
        // partitions compact array: count = 1
        put_compact_array_len(&mut buf, 1);
        buf.put_i32(3); // partition_index
        buf.put_i64(2048); // partition_size
        buf.put_i64(10); // offset_lag
        buf.put_u8(1); // is_future_key = true
        put_empty_tagged_fields(&mut buf); // partition tagged fields
        put_empty_tagged_fields(&mut buf); // topic tagged fields
        // v4 volume info
        buf.put_i64(500_000_000_000); // total_bytes = 500 GB
        buf.put_i64(200_000_000_000); // usable_bytes = 200 GB
        put_empty_tagged_fields(&mut buf); // result tagged fields
        // top-level tagged fields
        put_empty_tagged_fields(&mut buf);

        let resp = DescribeLogDirsResponse::decode_v4(&mut buf.freeze()).unwrap();
        assert_eq!(resp.error_code, ErrorCode::None);
        assert_eq!(resp.results.len(), 1);

        let dir = &resp.results[0];
        assert_eq!(dir.log_dir, "/data/kafka");
        assert_eq!(dir.total_bytes, 500_000_000_000);
        assert_eq!(dir.usable_bytes, 200_000_000_000);
        assert_eq!(dir.topics.len(), 1);

        let topic = &dir.topics[0];
        assert_eq!(topic.name, "events");
        assert_eq!(topic.partitions.len(), 1);

        let p = &topic.partitions[0];
        assert_eq!(p.partition_index, 3);
        assert_eq!(p.partition_size, 2048);
        assert_eq!(p.offset_lag, 10);
        assert!(p.is_future_key);
    }

    #[test]
    fn test_describe_log_dirs_response_decode_v2_flexible_no_error() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(10);
        // results compact array: count = 1
        put_compact_array_len(&mut buf, 1);
        // result[0].error_code
        buf.put_i16(0);
        // result[0].log_dir compact string
        put_compact_string(&mut buf, "/kafka-logs");
        // topics compact array: count = 1
        put_compact_array_len(&mut buf, 1);
        // topic name
        put_compact_string(&mut buf, "test-topic");
        // partitions compact array: count = 1
        put_compact_array_len(&mut buf, 1);
        buf.put_i32(0); // partition_index
        buf.put_i64(4096); // partition_size
        buf.put_i64(0); // offset_lag
        buf.put_u8(0); // is_future_key = false
        put_empty_tagged_fields(&mut buf); // partition tagged fields
        put_empty_tagged_fields(&mut buf); // topic tagged fields
        put_empty_tagged_fields(&mut buf); // result tagged fields
        // top-level tagged fields
        put_empty_tagged_fields(&mut buf);

        let resp = DescribeLogDirsResponse::decode_v2(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        // v2 has no top-level error code
        assert_eq!(resp.error_code, ErrorCode::None);
        assert_eq!(resp.results.len(), 1);

        let dir = &resp.results[0];
        assert_eq!(dir.log_dir, "/kafka-logs");
        assert!(dir.error_code.is_ok());
        // v2 has no volume info
        assert_eq!(dir.total_bytes, -1);
        assert_eq!(dir.usable_bytes, -1);

        let topic = &dir.topics[0];
        assert_eq!(topic.name, "test-topic");
        assert_eq!(topic.partitions[0].partition_size, 4096);
    }

    #[test]
    fn test_describe_log_dirs_versioned_encode_dispatches_v3_v4() {
        // v3 and v4 requests use the same wire format as v2.
        let request = DescribeLogDirsRequest::for_topics(vec![DescribableLogDirTopic {
            topic: "t".to_string(),
            partitions: vec![0],
        }]);

        let mut buf_v2 = BytesMut::new();
        request.encode_versioned(2, &mut buf_v2).unwrap();

        let mut buf_v3 = BytesMut::new();
        request.encode_versioned(3, &mut buf_v3).unwrap();

        let mut buf_v4 = BytesMut::new();
        request.encode_versioned(4, &mut buf_v4).unwrap();

        // All three should produce identical bytes.
        assert_eq!(buf_v2, buf_v3);
        assert_eq!(buf_v3, buf_v4);
    }
}
