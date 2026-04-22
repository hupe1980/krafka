use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{array_len_i32, check_compact_array_len, check_decode_array_len};

// ============================================================================
// ListOffsets request/response
// ============================================================================

/// Partition in ListOffsets request.
#[derive(Debug, Clone)]
pub struct ListOffsetsRequestPartition {
    /// Partition index.
    pub partition_index: i32,
    /// The current leader epoch (use -1 if not available).
    pub current_leader_epoch: i32,
    /// The target timestamp (-1 = latest, -2 = earliest).
    pub timestamp: i64,
}

/// Topic in ListOffsets request.
#[derive(Debug, Clone)]
pub struct ListOffsetsRequestTopic {
    /// Topic name.
    pub name: String,
    /// Partitions.
    pub partitions: Vec<ListOffsetsRequestPartition>,
}

/// ListOffsets request.
#[derive(Debug, Clone)]
pub struct ListOffsetsRequest {
    /// Broker ID of the requester (-1 for a consumer).
    pub replica_id: i32,
    /// Isolation level (0 = read_uncommitted, 1 = read_committed).
    pub isolation_level: i8,
    /// Topics.
    pub topics: Vec<ListOffsetsRequestTopic>,
    /// Timeout in milliseconds for async remote storage reads (v10+).
    pub timeout_ms: Option<i32>,
}

impl ListOffsetsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::ListOffsets
    }

    /// Encode for version 1 (single offset response per partition).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(self.replica_id);
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                buf.put_i32(partition.partition_index);
                buf.put_i64(partition.timestamp);
            }
        }
        Ok(())
    }

    /// Encode for version 2–3 (includes `isolation_level`).
    ///
    /// Version 2 adds `isolation_level` after `replica_id`, which controls
    /// whether transactional (uncommitted) records are visible.
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(self.replica_id);
        buf.put_i8(self.isolation_level);
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                buf.put_i32(partition.partition_index);
                buf.put_i64(partition.timestamp);
            }
        }
        Ok(())
    }

    /// Encode for version 4–5 (adds `current_leader_epoch` per partition).
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(self.replica_id);
        buf.put_i8(self.isolation_level);
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                buf.put_i32(partition.partition_index);
                buf.put_i32(partition.current_leader_epoch);
                buf.put_i64(partition.timestamp);
            }
        }
        Ok(())
    }

    /// Encode for version 6–8 (flexible: compact strings, varint arrays, tagged fields).
    pub fn encode_v6(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(self.replica_id);
        buf.put_i8(self.isolation_level);

        let topic_len = u32::try_from(self.topics.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("topics array too large"))?;
        crate::util::varint::encode_unsigned_varint(topic_len, buf);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode_compact(buf)?;
            let part_len = u32::try_from(topic.partitions.len().saturating_add(1))
                .map_err(|_| KrafkaError::protocol("partitions array too large"))?;
            crate::util::varint::encode_unsigned_varint(part_len, buf);
            for partition in &topic.partitions {
                buf.put_i32(partition.partition_index);
                buf.put_i32(partition.current_leader_epoch);
                buf.put_i64(partition.timestamp);
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 10–11 (adds `timeout_ms` after topics).
    pub fn encode_v10(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(self.replica_id);
        buf.put_i8(self.isolation_level);

        let topic_len = u32::try_from(self.topics.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("topics array too large"))?;
        crate::util::varint::encode_unsigned_varint(topic_len, buf);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode_compact(buf)?;
            let part_len = u32::try_from(topic.partitions.len().saturating_add(1))
                .map_err(|_| KrafkaError::protocol("partitions array too large"))?;
            crate::util::varint::encode_unsigned_varint(part_len, buf);
            for partition in &topic.partitions {
                buf.put_i32(partition.partition_index);
                buf.put_i32(partition.current_leader_epoch);
                buf.put_i64(partition.timestamp);
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        buf.put_i32(self.timeout_ms.unwrap_or(0));
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Partition in ListOffsets response.
#[derive(Debug, Clone)]
pub struct ListOffsetsResponsePartition {
    /// Partition index.
    pub partition_index: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// The result timestamp (-1 if not set).
    pub timestamp: i64,
    /// The result offset.
    pub offset: i64,
    /// The leader epoch of the returned offset (v4+, -1 if not available).
    pub leader_epoch: i32,
}

/// Topic in ListOffsets response.
#[derive(Debug, Clone)]
pub struct ListOffsetsResponseTopic {
    /// Topic name.
    pub name: String,
    /// Partitions.
    pub partitions: Vec<ListOffsetsResponsePartition>,
}

/// ListOffsets response.
#[derive(Debug, Clone)]
pub struct ListOffsetsResponse {
    /// Topics.
    pub topics: Vec<ListOffsetsResponseTopic>,
}

impl ListOffsetsResponse {
    /// Decode version 1 response.
    ///
    /// Uses checked reads to avoid panics on truncated data and guards
    /// negative counts to prevent OOM allocations.
    ///
    /// The v1 response format has the topics array directly (no throttle_time_ms).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        if buf.remaining() < 4 {
            return Err(crate::error::KrafkaError::protocol(
                "ListOffsetsResponse: truncated (no topic count)",
            ));
        }
        let topic_count = check_decode_array_len(buf.get_i32())?;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            if buf.remaining() < 4 {
                return Err(crate::error::KrafkaError::protocol(
                    "ListOffsetsResponse: truncated (no partition count)",
                ));
            }
            let partition_count = check_decode_array_len(buf.get_i32())?;
            let mut partitions = Vec::with_capacity(partition_count);
            // Each partition needs 4 + 2 + 8 + 8 = 22 bytes
            for _ in 0..partition_count {
                if buf.remaining() < 22 {
                    return Err(crate::error::KrafkaError::protocol(
                        "ListOffsetsResponse: truncated partition data",
                    ));
                }
                let partition_index = buf.get_i32();
                let error_code = ErrorCode::from(buf.get_i16());
                let timestamp = buf.get_i64();
                let offset = buf.get_i64();
                partitions.push(ListOffsetsResponsePartition {
                    partition_index,
                    error_code,
                    timestamp,
                    offset,
                    leader_epoch: -1,
                });
            }
            topics.push(ListOffsetsResponseTopic { name, partitions });
        }
        Ok(Self { topics })
    }

    /// Decode version 2–3 response (includes `throttle_time_ms`).
    ///
    /// The v2+ response format starts with `throttle_time_ms` (INT32)
    /// followed by the same topics array as v1.
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        if buf.remaining() < 4 {
            return Err(crate::error::KrafkaError::protocol(
                "ListOffsetsResponse v2: truncated (no throttle_time_ms)",
            ));
        }
        let _throttle_time_ms = buf.get_i32();

        // Remainder is identical to v1
        Self::decode_v1(buf)
    }

    /// Decode version 4–5 response (adds `leader_epoch` per partition).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        if buf.remaining() < 4 {
            return Err(crate::error::KrafkaError::protocol(
                "ListOffsetsResponse v4: truncated (no throttle_time_ms)",
            ));
        }
        let _throttle_time_ms = buf.get_i32();

        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);
        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let timestamp = i64::decode(buf)?;
                let offset = i64::decode(buf)?;
                let leader_epoch = i32::decode(buf)?;
                partitions.push(ListOffsetsResponsePartition {
                    partition_index,
                    error_code,
                    timestamp,
                    offset,
                    leader_epoch,
                });
            }
            topics.push(ListOffsetsResponseTopic { name, partitions });
        }
        Ok(Self { topics })
    }

    /// Decode version 6–8 response (flexible: compact strings, varint arrays, tagged fields).
    pub fn decode_v6(buf: &mut impl Buf) -> Result<Self> {
        let _throttle_time_ms = i32::decode(buf)?;

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
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let timestamp = i64::decode(buf)?;
                let offset = i64::decode(buf)?;
                let leader_epoch = i32::decode(buf)?;
                let _ = TaggedFields::decode(buf)?;
                partitions.push(ListOffsetsResponsePartition {
                    partition_index,
                    error_code,
                    timestamp,
                    offset,
                    leader_epoch,
                });
            }
            let _ = TaggedFields::decode(buf)?;
            topics.push(ListOffsetsResponseTopic { name, partitions });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self { topics })
    }
}

// ---------------------------------------------------------------------------
// VersionedEncode / VersionedDecode implementations
// ---------------------------------------------------------------------------

impl VersionedEncode for ListOffsetsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1 => self.encode_v1(buf)?,
            2 | 3 => self.encode_v2(buf)?,
            4 | 5 => self.encode_v4(buf)?,
            6..=9 => self.encode_v6(buf)?,
            10..=11 => self.encode_v10(buf)?,
            _ => return unsupported_encode!("ListOffsetsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for ListOffsetsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 => Self::decode_v1(buf),
            2 | 3 => Self::decode_v2(buf),
            4 | 5 => Self::decode_v4(buf),
            6..=11 => Self::decode_v6(buf),
            _ => unsupported_decode!("ListOffsetsResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::util::varint;
    use bytes::BytesMut;

    // ── R14: ListOffsetsResponse decode safety ──

    #[test]
    fn test_list_offsets_response_decode_v1_empty() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // 0 topics
        let mut data = buf.freeze();
        let resp = ListOffsetsResponse::decode_v1(&mut data).unwrap();
        assert!(resp.topics.is_empty());
    }

    #[test]
    fn test_list_offsets_response_decode_v1_truncated_no_topic_count() {
        let mut buf = BytesMut::new();
        buf.put_i8(0); // only 1 byte — not enough for i32
        let mut data = buf.freeze();
        let result = ListOffsetsResponse::decode_v1(&mut data);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_offsets_response_decode_v1_negative_topic_count() {
        let mut buf = BytesMut::new();
        buf.put_i32(-1); // negative count
        let mut data = buf.freeze();
        let result = ListOffsetsResponse::decode_v1(&mut data);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_offsets_response_decode_v1_negative_partition_count() {
        let mut buf = BytesMut::new();
        buf.put_i32(1); // 1 topic
        // topic name (short string)
        buf.put_i16(4);
        buf.put_slice(b"test");
        buf.put_i32(-1); // negative partition count
        let mut data = buf.freeze();
        let result = ListOffsetsResponse::decode_v1(&mut data);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_offsets_response_decode_v1_truncated_partition() {
        let mut buf = BytesMut::new();
        buf.put_i32(1); // 1 topic
        buf.put_i16(4);
        buf.put_slice(b"test");
        buf.put_i32(1); // 1 partition
        buf.put_i32(0); // partition_index only (missing error_code, timestamp, offset)
        let mut data = buf.freeze();
        let result = ListOffsetsResponse::decode_v1(&mut data);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_offsets_response_decode_v1_valid() {
        let mut buf = BytesMut::new();
        buf.put_i32(1); // 1 topic
        buf.put_i16(5);
        buf.put_slice(b"topic");
        buf.put_i32(1); // 1 partition
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code (NONE)
        buf.put_i64(1234567890); // timestamp
        buf.put_i64(42); // offset
        let mut data = buf.freeze();
        let resp = ListOffsetsResponse::decode_v1(&mut data).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name, "topic");
        assert_eq!(resp.topics[0].partitions.len(), 1);
        assert_eq!(resp.topics[0].partitions[0].offset, 42);
        assert_eq!(resp.topics[0].partitions[0].timestamp, 1234567890);
    }

    // ── ListOffsetsResponse decode_v2 (with throttle_time_ms) ──

    #[test]
    fn test_list_offsets_response_decode_v2_empty() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i32(0); // 0 topics
        let mut data = buf.freeze();
        let resp = ListOffsetsResponse::decode_v2(&mut data).unwrap();
        assert!(resp.topics.is_empty());
    }

    #[test]
    fn test_list_offsets_response_decode_v2_valid() {
        let mut buf = BytesMut::new();
        buf.put_i32(100); // throttle_time_ms
        buf.put_i32(1); // 1 topic
        buf.put_i16(5);
        buf.put_slice(b"topic");
        buf.put_i32(1); // 1 partition
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code (NONE)
        buf.put_i64(1234567890); // timestamp
        buf.put_i64(42); // offset
        let mut data = buf.freeze();
        let resp = ListOffsetsResponse::decode_v2(&mut data).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].partitions[0].offset, 42);
    }

    #[test]
    fn test_list_offsets_response_decode_v2_truncated() {
        let mut buf = BytesMut::new();
        buf.put_i8(0); // only 1 byte — not enough for throttle_time_ms
        let mut data = buf.freeze();
        let result = ListOffsetsResponse::decode_v2(&mut data);
        assert!(result.is_err());
    }

    // ── R14: ListOffsetsRequest encode_v2 with isolation_level ──

    #[test]
    fn test_list_offsets_request_encode_v2_includes_isolation_level() {
        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 1, // read_committed
            topics: vec![ListOffsetsRequestTopic {
                name: "test-topic".to_string(),
                partitions: vec![ListOffsetsRequestPartition {
                    partition_index: 0,
                    current_leader_epoch: -1,
                    timestamp: -1,
                }],
            }],
            timeout_ms: None,
        };

        let mut buf = BytesMut::new();
        request.encode_v2(&mut buf).unwrap();

        // replica_id (4 bytes) + isolation_level (1 byte) + topics
        assert_eq!(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]), -1);
        assert_eq!(buf[4], 1); // isolation_level = read_committed
    }

    #[test]
    fn test_list_offsets_request_encode_v1_no_isolation_level() {
        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 1,
            topics: vec![],
            timeout_ms: None,
        };

        let mut buf_v1 = BytesMut::new();
        request.encode_v1(&mut buf_v1).unwrap();

        let mut buf_v2 = BytesMut::new();
        request.encode_v2(&mut buf_v2).unwrap();

        // v2 should be 1 byte longer (isolation_level)
        assert_eq!(buf_v2.len(), buf_v1.len() + 1);
    }

    // ── ListOffsetsRequest encode_v4 (adds current_leader_epoch) ──

    #[test]
    fn test_list_offsets_request_encode_v4_includes_leader_epoch() {
        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 1,
            topics: vec![ListOffsetsRequestTopic {
                name: "t".to_string(),
                partitions: vec![ListOffsetsRequestPartition {
                    partition_index: 0,
                    current_leader_epoch: 5,
                    timestamp: -1,
                }],
            }],
            timeout_ms: None,
        };

        let mut buf = BytesMut::new();
        request.encode_v4(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_i32(), -1); // replica_id
        assert_eq!(cur.get_i8(), 1); // isolation_level
        assert_eq!(cur.get_i32(), 1); // 1 topic
        let name_len = cur.get_i16() as usize;
        let mut name_bytes = vec![0u8; name_len];
        cur.copy_to_slice(&mut name_bytes);
        assert_eq!(name_bytes, b"t");
        assert_eq!(cur.get_i32(), 1); // 1 partition
        assert_eq!(cur.get_i32(), 0); // partition_index
        assert_eq!(cur.get_i32(), 5); // current_leader_epoch (v4+)
        assert_eq!(cur.get_i64(), -1); // timestamp
        assert!(cur.is_empty());
    }

    // ── ListOffsetsRequest encode_v6 (flexible) ──

    #[test]
    fn test_list_offsets_request_encode_v6_round_trip() {
        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsRequestTopic {
                name: "topic".to_string(),
                partitions: vec![ListOffsetsRequestPartition {
                    partition_index: 3,
                    current_leader_epoch: 7,
                    timestamp: 1000,
                }],
            }],
            timeout_ms: None,
        };

        let mut buf = BytesMut::new();
        request.encode_v6(&mut buf).unwrap();

        // Decode manually to verify flexible encoding
        let mut cur = &buf[..];
        assert_eq!(cur.get_i32(), -1); // replica_id
        assert_eq!(cur.get_i8(), 0); // isolation_level
        // compact array: varint(topics.len + 1) = varint(2)
        let topic_varint = crate::util::varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(topic_varint, 2); // 1 topic + 1
        // compact string: varint(name.len + 1) = varint(6) then 5 bytes
        let name_varint = crate::util::varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(name_varint, 6);
        let mut name_bytes = vec![0u8; 5];
        cur.copy_to_slice(&mut name_bytes);
        assert_eq!(name_bytes, b"topic");
        // compact array for partitions: varint(2)
        let part_varint = crate::util::varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(part_varint, 2);
        assert_eq!(cur.get_i32(), 3); // partition_index
        assert_eq!(cur.get_i32(), 7); // current_leader_epoch
        assert_eq!(cur.get_i64(), 1000); // timestamp
        assert_eq!(cur.get_u8(), 0); // partition tagged fields (empty)
        assert_eq!(cur.get_u8(), 0); // topic tagged fields (empty)
        assert_eq!(cur.get_u8(), 0); // top-level tagged fields (empty)
        assert!(cur.is_empty());
    }

    // ── ListOffsetsResponse decode_v4 (with leader_epoch) ──

    #[test]
    fn test_list_offsets_response_decode_v4_valid() {
        let mut buf = BytesMut::new();
        buf.put_i32(50); // throttle_time_ms
        buf.put_i32(1); // 1 topic
        buf.put_i16(5);
        buf.put_slice(b"topic");
        buf.put_i32(1); // 1 partition
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code (NONE)
        buf.put_i64(1234567890); // timestamp
        buf.put_i64(42); // offset
        buf.put_i32(10); // leader_epoch
        let mut data = buf.freeze();
        let resp = ListOffsetsResponse::decode_v4(&mut data).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].partitions[0].offset, 42);
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, 10);
    }

    #[test]
    fn test_list_offsets_response_decode_v4_leader_epoch_sentinel() {
        // v1/v2/v3 responses should have leader_epoch = -1 (field not present)
        let mut buf = BytesMut::new();
        buf.put_i32(1); // 1 topic
        buf.put_i16(1);
        buf.put_slice(b"t");
        buf.put_i32(1); // 1 partition
        buf.put_i32(0);
        buf.put_i16(0);
        buf.put_i64(0);
        buf.put_i64(5);
        let mut data = buf.freeze();
        let resp = ListOffsetsResponse::decode_v1(&mut data).unwrap();
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, -1);
    }

    // ── ListOffsetsResponse decode_v6 (flexible) ──

    #[test]
    fn test_list_offsets_response_decode_v6_valid() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        // compact array: varint(2) = 1 topic + 1
        crate::util::varint::encode_unsigned_varint(2, &mut buf);
        // compact string: varint(6) then "topic"
        crate::util::varint::encode_unsigned_varint(6, &mut buf);
        buf.put_slice(b"topic");
        // compact partitions array: varint(2) = 1 partition + 1
        crate::util::varint::encode_unsigned_varint(2, &mut buf);
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code
        buf.put_i64(9999); // timestamp
        buf.put_i64(100); // offset
        buf.put_i32(3); // leader_epoch
        buf.put_u8(0); // partition tagged fields
        buf.put_u8(0); // topic tagged fields
        buf.put_u8(0); // top-level tagged fields
        let mut data = buf.freeze();
        let resp = ListOffsetsResponse::decode_v6(&mut data).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name, "topic");
        assert_eq!(resp.topics[0].partitions[0].offset, 100);
        assert_eq!(resp.topics[0].partitions[0].timestamp, 9999);
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, 3);
    }

    #[test]
    fn test_list_offsets_response_decode_v6_empty() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        crate::util::varint::encode_unsigned_varint(1, &mut buf); // 0 topics (1 - 1)
        buf.put_u8(0); // top-level tagged fields
        let mut data = buf.freeze();
        let resp = ListOffsetsResponse::decode_v6(&mut data).unwrap();
        assert!(resp.topics.is_empty());
    }

    #[test]
    fn test_list_offsets_request_encode_v4_vs_v2_has_leader_epoch() {
        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsRequestTopic {
                name: "t".to_string(),
                partitions: vec![ListOffsetsRequestPartition {
                    partition_index: 0,
                    current_leader_epoch: -1,
                    timestamp: -1,
                }],
            }],
            timeout_ms: None,
        };

        let mut buf_v2 = BytesMut::new();
        request.encode_v2(&mut buf_v2).unwrap();

        let mut buf_v4 = BytesMut::new();
        request.encode_v4(&mut buf_v4).unwrap();

        // v4 adds current_leader_epoch (i32 = 4 bytes) per partition
        assert_eq!(buf_v4.len(), buf_v2.len() + 4);
    }

    // ── ListOffsets v10 (timeout_ms) ──

    #[test]
    fn test_list_offsets_request_encode_v10_includes_timeout_ms() {
        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![],
            timeout_ms: Some(5000),
        };
        let mut buf = BytesMut::new();
        request.encode_v10(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_i32(), -1); // replica_id
        assert_eq!(cur.get_i8(), 0); // isolation_level
        let topics_varint = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(topics_varint, 1); // 0 topics + 1
        assert_eq!(cur.get_i32(), 5000); // timeout_ms
        let tagged = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(tagged, 0); // empty tagged fields
        assert!(!cur.has_remaining());
    }

    #[test]
    fn test_list_offsets_request_encode_v10_default_timeout() {
        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 1,
            topics: vec![],
            timeout_ms: None,
        };
        let mut buf = BytesMut::new();
        request.encode_v10(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_i32(), -1);
        assert_eq!(cur.get_i8(), 1);
        let _ = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(cur.get_i32(), 0); // defaults to 0
    }

    #[test]
    fn test_list_offsets_response_decode_v11_same_as_v6() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        varint::encode_unsigned_varint(2, &mut buf); // 1 topic + 1
        varint::encode_unsigned_varint(4, &mut buf); // "abc".len() + 1
        buf.put_slice(b"abc");
        varint::encode_unsigned_varint(2, &mut buf); // 1 partition + 1
        buf.put_i32(0);
        buf.put_i16(0);
        buf.put_i64(12345);
        buf.put_i64(999);
        buf.put_i32(7);
        varint::encode_unsigned_varint(0, &mut buf); // partition tagged
        varint::encode_unsigned_varint(0, &mut buf); // topic tagged
        varint::encode_unsigned_varint(0, &mut buf); // top tagged

        let resp = ListOffsetsResponse::decode_versioned(11, &mut buf.freeze()).unwrap();
        assert_eq!(resp.topics[0].name, "abc");
        assert_eq!(resp.topics[0].partitions[0].timestamp, 12345);
        assert_eq!(resp.topics[0].partitions[0].offset, 999);
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, 7);
    }
}
