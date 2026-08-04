//! Share-group offset administration (API keys 90–92, KIP-932 / KIP-1226).
//!
//! Share groups (KIP-932) keep a *share-partition start offset* per
//! `(group, topic, partition)` rather than a committed consumer offset. These
//! three APIs are how an operator inspects and manipulates it:
//!
//! | Key | API | Purpose |
//! |-----|-----|---------|
//! | 90 | `DescribeShareGroupOffsets` | Read the start offset (and, at v1, the lag) |
//! | 91 | `AlterShareGroupOffsets` | Reset the start offset — only while the group is empty |
//! | 92 | `DeleteShareGroupOffsets` | Drop the group's state for whole topics |
//!
//! Without them, `ShareConsumer` is a share group you can run but cannot
//! operate: no lag monitoring, no reset-to-earliest, no cleanup after a topic
//! is retired.
//!
//! All three are flexible from v0 and are served by the **group coordinator**,
//! not an arbitrary broker.
//!
//! # Empty-group requirement
//!
//! `AlterShareGroupOffsets` and `DeleteShareGroupOffsets` answer
//! `NON_EMPTY_GROUP` unless every member has left. That mirrors
//! `AlterConsumerGroupOffsets` and exists for the same reason: rewriting the
//! start offset under a live member would hand it records it has already
//! acquired.

use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{check_compact_array_len, decode_capacity, encode_compact_array_len};
use crate::util::varint::decode_unsigned_varint;

// ============================================================================
// DescribeShareGroupOffsets (Key 90)
// ============================================================================

/// A topic (and optional partition subset) to describe offsets for.
#[derive(Debug, Clone)]
pub struct DescribeShareGroupOffsetsRequestTopic {
    /// Topic name.
    pub topic_name: String,
    /// Partitions to describe. An empty list describes no partitions; use a
    /// `None` topic list on the group to describe every topic-partition.
    pub partitions: Vec<i32>,
}

/// One group in a [`DescribeShareGroupOffsetsRequest`].
#[derive(Debug, Clone)]
pub struct DescribeShareGroupOffsetsRequestGroup {
    /// Share group identifier.
    pub group_id: String,
    /// Topics to describe, or `None` for **all** topic-partitions the group
    /// has state for. `None` and `Some(vec![])` are different requests: the
    /// former means "everything", the latter "nothing".
    pub topics: Option<Vec<DescribeShareGroupOffsetsRequestTopic>>,
}

/// `DescribeShareGroupOffsets` request (API key 90, KIP-932).
#[derive(Debug, Clone)]
pub struct DescribeShareGroupOffsetsRequest {
    /// Groups to describe.
    pub groups: Vec<DescribeShareGroupOffsetsRequestGroup>,
}

impl DescribeShareGroupOffsetsRequest {
    /// The API key this request is sent under.
    #[must_use]
    pub fn api_key() -> ApiKey {
        ApiKey::DescribeShareGroupOffsets
    }

    /// Describe every topic-partition of a single group.
    #[must_use]
    pub fn all_topics(group_id: impl Into<String>) -> Self {
        Self {
            groups: vec![DescribeShareGroupOffsetsRequestGroup {
                group_id: group_id.into(),
                topics: None,
            }],
        }
    }

    /// Encode for version 0 (flexible; v1 is request-identical).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.groups.len(), buf)?;
        for group in &self.groups {
            KafkaString::new(&group.group_id).try_encode_compact(buf)?;
            match &group.topics {
                // Nullable compact array: raw 0 is null, which the broker
                // reads as "all topic-partitions".
                None => crate::util::varint::encode_unsigned_varint(0, buf),
                Some(topics) => {
                    encode_compact_array_len(topics.len(), buf)?;
                    for topic in topics {
                        KafkaString::new(&topic.topic_name).try_encode_compact(buf)?;
                        encode_compact_array_len(topic.partitions.len(), buf)?;
                        for &p in &topic.partitions {
                            p.encode(buf);
                        }
                        TaggedFields::default().try_encode(buf)?;
                    }
                }
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for DescribeShareGroupOffsetsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            // v1 (KIP-1226) only adds `Lag` to the response.
            0..=1 => self.encode_v0(buf),
            _ => unsupported_encode!("DescribeShareGroupOffsetsRequest", version),
        }
    }
}

/// Per-partition result in a [`DescribeShareGroupOffsetsResponse`].
#[derive(Debug, Clone)]
pub struct DescribeShareGroupOffsetsResponsePartition {
    /// Partition index.
    pub partition_index: i32,
    /// The share-partition start offset: the earliest offset the group may
    /// still deliver.
    pub start_offset: i64,
    /// Leader epoch of the partition.
    pub leader_epoch: i32,
    /// Share-partition lag (v1+, KIP-1226), or `-1` when the broker did not
    /// report it — which includes every v0 response.
    pub lag: i64,
    /// Partition-level error code.
    pub error_code: ErrorCode,
    /// Partition-level error message.
    pub error_message: Option<String>,
}

/// Per-topic result in a [`DescribeShareGroupOffsetsResponse`].
#[derive(Debug, Clone)]
pub struct DescribeShareGroupOffsetsResponseTopic {
    /// Topic name.
    pub topic_name: String,
    /// Topic UUID.
    pub topic_id: [u8; 16],
    /// Partition results.
    pub partitions: Vec<DescribeShareGroupOffsetsResponsePartition>,
}

/// Per-group result in a [`DescribeShareGroupOffsetsResponse`].
#[derive(Debug, Clone)]
pub struct DescribeShareGroupOffsetsResponseGroup {
    /// Share group identifier.
    pub group_id: String,
    /// Topic results.
    pub topics: Vec<DescribeShareGroupOffsetsResponseTopic>,
    /// Group-level error code.
    pub error_code: ErrorCode,
    /// Group-level error message.
    pub error_message: Option<String>,
}

/// `DescribeShareGroupOffsets` response (API key 90).
#[derive(Debug, Clone)]
pub struct DescribeShareGroupOffsetsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-group results.
    pub groups: Vec<DescribeShareGroupOffsetsResponseGroup>,
}

impl DescribeShareGroupOffsetsResponse {
    /// Decode a v0 response (no `Lag`).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_inner(buf, false)
    }

    /// Decode a v1 response (adds `Lag` per partition, KIP-1226).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_inner(buf, true)
    }

    fn decode_inner(buf: &mut impl Buf, has_lag: bool) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count = check_compact_array_len(decode_unsigned_varint(buf)?)?;
        let mut groups = Vec::with_capacity(decode_capacity(group_count, buf.remaining()));
        for _ in 0..group_count {
            let group_id = non_nullable_string("group id", KafkaString::decode_compact(buf)?.0)?;
            let topic_count = check_compact_array_len(decode_unsigned_varint(buf)?)?;
            let mut topics = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));
            for _ in 0..topic_count {
                let topic_name =
                    non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
                let topic_id = read_uuid(buf)?;
                let partition_count = check_compact_array_len(decode_unsigned_varint(buf)?)?;
                let mut partitions =
                    Vec::with_capacity(decode_capacity(partition_count, buf.remaining()));
                for _ in 0..partition_count {
                    let partition_index = i32::decode(buf)?;
                    let start_offset = i64::decode(buf)?;
                    let leader_epoch = i32::decode(buf)?;
                    let lag = if has_lag { i64::decode(buf)? } else { -1 };
                    let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                    let error_message = KafkaString::decode_compact(buf)?.0;
                    let _ = TaggedFields::decode(buf)?;
                    partitions.push(DescribeShareGroupOffsetsResponsePartition {
                        partition_index,
                        start_offset,
                        leader_epoch,
                        lag,
                        error_code,
                        error_message,
                    });
                }
                let _ = TaggedFields::decode(buf)?;
                topics.push(DescribeShareGroupOffsetsResponseTopic {
                    topic_name,
                    topic_id,
                    partitions,
                });
            }
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            let _ = TaggedFields::decode(buf)?;
            groups.push(DescribeShareGroupOffsetsResponseGroup {
                group_id,
                topics,
                error_code,
                error_message,
            });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}

impl VersionedDecode for DescribeShareGroupOffsetsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            _ => unsupported_decode!("DescribeShareGroupOffsetsResponse", version),
        }
    }
}

// ============================================================================
// AlterShareGroupOffsets (Key 91)
// ============================================================================

/// A partition whose share-partition start offset should be reset.
#[derive(Debug, Clone)]
pub struct AlterShareGroupOffsetsRequestPartition {
    /// Partition index.
    pub partition_index: i32,
    /// New share-partition start offset.
    pub start_offset: i64,
}

/// A topic in an [`AlterShareGroupOffsetsRequest`].
#[derive(Debug, Clone)]
pub struct AlterShareGroupOffsetsRequestTopic {
    /// Topic name.
    pub topic_name: String,
    /// Partitions to reset.
    pub partitions: Vec<AlterShareGroupOffsetsRequestPartition>,
}

/// `AlterShareGroupOffsets` request (API key 91, KIP-932).
///
/// The group must be **empty**; the coordinator answers `NON_EMPTY_GROUP`
/// otherwise.
#[derive(Debug, Clone)]
pub struct AlterShareGroupOffsetsRequest {
    /// Share group identifier.
    pub group_id: String,
    /// Topics to alter.
    pub topics: Vec<AlterShareGroupOffsetsRequestTopic>,
}

impl AlterShareGroupOffsetsRequest {
    /// The API key this request is sent under.
    #[must_use]
    pub fn api_key() -> ApiKey {
        ApiKey::AlterShareGroupOffsets
    }

    /// Encode for version 0 (flexible).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            KafkaString::new(&topic.topic_name).try_encode_compact(buf)?;
            encode_compact_array_len(topic.partitions.len(), buf)?;
            for partition in &topic.partitions {
                partition.partition_index.encode(buf);
                partition.start_offset.encode(buf);
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for AlterShareGroupOffsetsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf),
            _ => unsupported_encode!("AlterShareGroupOffsetsRequest", version),
        }
    }
}

/// Per-partition result in an [`AlterShareGroupOffsetsResponse`].
#[derive(Debug, Clone)]
pub struct AlterShareGroupOffsetsResponsePartition {
    /// Partition index.
    pub partition_index: i32,
    /// Partition-level error code.
    pub error_code: ErrorCode,
    /// Partition-level error message.
    pub error_message: Option<String>,
}

/// Per-topic result in an [`AlterShareGroupOffsetsResponse`].
#[derive(Debug, Clone)]
pub struct AlterShareGroupOffsetsResponseTopic {
    /// Topic name.
    pub topic_name: String,
    /// Topic UUID.
    pub topic_id: [u8; 16],
    /// Partition results.
    pub partitions: Vec<AlterShareGroupOffsetsResponsePartition>,
}

/// `AlterShareGroupOffsets` response (API key 91).
#[derive(Debug, Clone)]
pub struct AlterShareGroupOffsetsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code.
    pub error_code: ErrorCode,
    /// Top-level error message.
    pub error_message: Option<String>,
    /// Per-topic results.
    pub responses: Vec<AlterShareGroupOffsetsResponseTopic>,
}

impl AlterShareGroupOffsetsResponse {
    /// Decode a v0 response.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode_compact(buf)?.0;
        let topic_count = check_compact_array_len(decode_unsigned_varint(buf)?)?;
        let mut responses = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));
        for _ in 0..topic_count {
            let topic_name =
                non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let topic_id = read_uuid(buf)?;
            let partition_count = check_compact_array_len(decode_unsigned_varint(buf)?)?;
            let mut partitions =
                Vec::with_capacity(decode_capacity(partition_count, buf.remaining()));
            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let partition_error = ErrorCode::from_i16(i16::decode(buf)?);
                let partition_message = KafkaString::decode_compact(buf)?.0;
                let _ = TaggedFields::decode(buf)?;
                partitions.push(AlterShareGroupOffsetsResponsePartition {
                    partition_index,
                    error_code: partition_error,
                    error_message: partition_message,
                });
            }
            let _ = TaggedFields::decode(buf)?;
            responses.push(AlterShareGroupOffsetsResponseTopic {
                topic_name,
                topic_id,
                partitions,
            });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            responses,
        })
    }
}

impl VersionedDecode for AlterShareGroupOffsetsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("AlterShareGroupOffsetsResponse", version),
        }
    }
}

// ============================================================================
// DeleteShareGroupOffsets (Key 92)
// ============================================================================

/// `DeleteShareGroupOffsets` request (API key 92, KIP-932).
///
/// Deletes the group's share-partition state for whole topics. The group must
/// be **empty**.
#[derive(Debug, Clone)]
pub struct DeleteShareGroupOffsetsRequest {
    /// Share group identifier.
    pub group_id: String,
    /// Topic names whose offsets should be deleted.
    pub topics: Vec<String>,
}

impl DeleteShareGroupOffsetsRequest {
    /// The API key this request is sent under.
    #[must_use]
    pub fn api_key() -> ApiKey {
        ApiKey::DeleteShareGroupOffsets
    }

    /// Encode for version 0 (flexible).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            KafkaString::new(topic).try_encode_compact(buf)?;
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for DeleteShareGroupOffsetsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf),
            _ => unsupported_encode!("DeleteShareGroupOffsetsRequest", version),
        }
    }
}

/// Per-topic result in a [`DeleteShareGroupOffsetsResponse`].
#[derive(Debug, Clone)]
pub struct DeleteShareGroupOffsetsResponseTopic {
    /// Topic name.
    pub topic_name: String,
    /// Topic UUID.
    pub topic_id: [u8; 16],
    /// Topic-level error code.
    pub error_code: ErrorCode,
    /// Topic-level error message.
    pub error_message: Option<String>,
}

/// `DeleteShareGroupOffsets` response (API key 92).
#[derive(Debug, Clone)]
pub struct DeleteShareGroupOffsetsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code.
    pub error_code: ErrorCode,
    /// Top-level error message.
    pub error_message: Option<String>,
    /// Per-topic results.
    pub responses: Vec<DeleteShareGroupOffsetsResponseTopic>,
}

impl DeleteShareGroupOffsetsResponse {
    /// Decode a v0 response.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode_compact(buf)?.0;
        let topic_count = check_compact_array_len(decode_unsigned_varint(buf)?)?;
        let mut responses = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));
        for _ in 0..topic_count {
            let topic_name =
                non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let topic_id = read_uuid(buf)?;
            let topic_error = ErrorCode::from_i16(i16::decode(buf)?);
            let topic_message = KafkaString::decode_compact(buf)?.0;
            let _ = TaggedFields::decode(buf)?;
            responses.push(DeleteShareGroupOffsetsResponseTopic {
                topic_name,
                topic_id,
                error_code: topic_error,
                error_message: topic_message,
            });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            responses,
        })
    }
}

impl VersionedDecode for DeleteShareGroupOffsetsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("DeleteShareGroupOffsetsResponse", version),
        }
    }
}

/// Read a fixed 16-byte Kafka UUID, erroring rather than panicking on a short
/// buffer.
fn read_uuid(buf: &mut impl Buf) -> Result<[u8; 16]> {
    if buf.remaining() < 16 {
        return Err(crate::error::KrafkaError::protocol_kind(
            crate::error::ProtocolErrorKind::TruncatedFrame,
            "not enough bytes for topic_id UUID",
        ));
    }
    let mut id = [0u8; 16];
    buf.copy_to_slice(&mut id);
    Ok(id)
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

    fn put_compact_array_len(buf: &mut BytesMut, count: usize) {
        crate::util::varint::encode_unsigned_varint((count + 1) as u32, buf);
    }

    fn put_tags(buf: &mut BytesMut) {
        buf.put_u8(0);
    }

    #[test]
    fn api_keys_are_correct() {
        assert_eq!(
            DescribeShareGroupOffsetsRequest::api_key().to_i16(),
            90,
            "DescribeShareGroupOffsets is key 90"
        );
        assert_eq!(AlterShareGroupOffsetsRequest::api_key().to_i16(), 91);
        assert_eq!(DeleteShareGroupOffsetsRequest::api_key().to_i16(), 92);
    }

    /// `topics: None` must encode as the *null* compact array (raw varint 0),
    /// which is what the broker reads as "every topic-partition". Encoding it
    /// as an empty array (raw 1) would describe nothing at all — a silent
    /// wrong answer rather than an error.
    #[test]
    fn describe_all_topics_encodes_null_not_empty() {
        let mut null_buf = BytesMut::new();
        DescribeShareGroupOffsetsRequest::all_topics("g")
            .encode_v0(&mut null_buf)
            .unwrap();

        let mut empty_buf = BytesMut::new();
        DescribeShareGroupOffsetsRequest {
            groups: vec![DescribeShareGroupOffsetsRequestGroup {
                group_id: "g".to_string(),
                topics: Some(Vec::new()),
            }],
        }
        .encode_v0(&mut empty_buf)
        .unwrap();

        assert_ne!(
            null_buf, empty_buf,
            "null topics (all) and empty topics (none) must not encode identically"
        );

        // Walk the null form: groups array, group id, then the null marker.
        let mut cur = &null_buf[..];
        assert_eq!(decode_unsigned_varint(&mut cur).unwrap(), 2); // 1 group
        assert_eq!(
            KafkaString::decode_compact(&mut cur).unwrap().0.as_deref(),
            Some("g")
        );
        assert_eq!(
            decode_unsigned_varint(&mut cur).unwrap(),
            0,
            "null topics array"
        );
    }

    #[test]
    fn describe_request_versioned_dispatch() {
        let request = DescribeShareGroupOffsetsRequest::all_topics("g");
        let mut v0 = BytesMut::new();
        let mut v1 = BytesMut::new();
        request.encode_versioned(0, &mut v0).unwrap();
        request.encode_versioned(1, &mut v1).unwrap();
        assert_eq!(v0, v1, "v1 is request-identical to v0");
        let mut v2 = BytesMut::new();
        assert!(request.encode_versioned(2, &mut v2).is_err());
    }

    /// v1 inserts `Lag` between `LeaderEpoch` and `ErrorCode`. Decoding a v1
    /// body with the v0 layout would read the lag's high bytes as the error
    /// code, so the two layouts are asserted separately against the same
    /// logical response.
    #[test]
    fn describe_response_v1_reads_lag_and_v0_defaults_it() {
        fn body(with_lag: bool) -> BytesMut {
            let mut buf = BytesMut::new();
            buf.put_i32(0); // throttle_time_ms
            put_compact_array_len(&mut buf, 1); // 1 group
            put_compact_string(&mut buf, "share-group");
            put_compact_array_len(&mut buf, 1); // 1 topic
            put_compact_string(&mut buf, "events");
            buf.put_slice(&[7u8; 16]); // topic_id
            put_compact_array_len(&mut buf, 1); // 1 partition
            buf.put_i32(3); // partition_index
            buf.put_i64(1000); // start_offset
            buf.put_i32(9); // leader_epoch
            if with_lag {
                buf.put_i64(42); // lag (v1+)
            }
            buf.put_i16(0); // partition error_code
            put_compact_null_string(&mut buf);
            put_tags(&mut buf); // partition tags
            put_tags(&mut buf); // topic tags
            buf.put_i16(0); // group error_code
            put_compact_null_string(&mut buf);
            put_tags(&mut buf); // group tags
            put_tags(&mut buf); // top-level tags
            buf
        }

        let v1 = DescribeShareGroupOffsetsResponse::decode_versioned(1, &mut body(true).freeze())
            .expect("v1 decodes");
        let partition = &v1.groups[0].topics[0].partitions[0];
        assert_eq!(partition.partition_index, 3);
        assert_eq!(partition.start_offset, 1000);
        assert_eq!(partition.leader_epoch, 9);
        assert_eq!(partition.lag, 42);
        assert_eq!(v1.groups[0].topics[0].topic_id, [7u8; 16]);
        assert!(partition.error_code.is_ok());

        let v0 = DescribeShareGroupOffsetsResponse::decode_versioned(0, &mut body(false).freeze())
            .expect("v0 decodes");
        assert_eq!(
            v0.groups[0].topics[0].partitions[0].lag, -1,
            "v0 carries no lag; the sentinel must say 'unknown', not 0"
        );
    }

    #[test]
    fn alter_request_encodes_group_then_topics() {
        let request = AlterShareGroupOffsetsRequest {
            group_id: "sg".to_string(),
            topics: vec![AlterShareGroupOffsetsRequestTopic {
                topic_name: "events".to_string(),
                partitions: vec![AlterShareGroupOffsetsRequestPartition {
                    partition_index: 1,
                    start_offset: 500,
                }],
            }],
        };
        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(
            KafkaString::decode_compact(&mut cur).unwrap().0.as_deref(),
            Some("sg")
        );
        assert_eq!(decode_unsigned_varint(&mut cur).unwrap(), 2); // 1 topic
        assert_eq!(
            KafkaString::decode_compact(&mut cur).unwrap().0.as_deref(),
            Some("events")
        );
        assert_eq!(decode_unsigned_varint(&mut cur).unwrap(), 2); // 1 partition
        assert_eq!(i32::decode(&mut cur).unwrap(), 1);
        assert_eq!(i64::decode(&mut cur).unwrap(), 500);
    }

    #[test]
    fn alter_response_decodes_top_level_error() {
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        buf.put_i16(68); // NON_EMPTY_GROUP
        put_compact_string(&mut buf, "group is not empty");
        put_compact_array_len(&mut buf, 0);
        put_tags(&mut buf);

        let resp = AlterShareGroupOffsetsResponse::decode_versioned(0, &mut buf.freeze()).unwrap();
        assert!(!resp.error_code.is_ok());
        assert_eq!(resp.error_message.as_deref(), Some("group is not empty"));
        assert!(resp.responses.is_empty());
    }

    #[test]
    fn delete_request_and_response_round_trip_shape() {
        let request = DeleteShareGroupOffsetsRequest {
            group_id: "sg".to_string(),
            topics: vec!["a".to_string(), "b".to_string()],
        };
        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        let mut cur = &buf[..];
        assert_eq!(
            KafkaString::decode_compact(&mut cur).unwrap().0.as_deref(),
            Some("sg")
        );
        assert_eq!(decode_unsigned_varint(&mut cur).unwrap(), 3); // 2 topics

        let mut resp_buf = BytesMut::new();
        resp_buf.put_i32(0);
        resp_buf.put_i16(0);
        put_compact_null_string(&mut resp_buf);
        put_compact_array_len(&mut resp_buf, 1);
        put_compact_string(&mut resp_buf, "a");
        resp_buf.put_slice(&[1u8; 16]);
        resp_buf.put_i16(0);
        put_compact_null_string(&mut resp_buf);
        put_tags(&mut resp_buf);
        put_tags(&mut resp_buf);

        let resp =
            DeleteShareGroupOffsetsResponse::decode_versioned(0, &mut resp_buf.freeze()).unwrap();
        assert_eq!(resp.responses.len(), 1);
        assert_eq!(resp.responses[0].topic_name, "a");
        assert_eq!(resp.responses[0].topic_id, [1u8; 16]);
        assert!(resp.responses[0].error_code.is_ok());
    }

    /// A truncated UUID must surface as a protocol error rather than panicking
    /// in `copy_to_slice` — broker responses are untrusted input.
    #[test]
    fn truncated_topic_id_is_an_error_not_a_panic() {
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        buf.put_i16(0);
        put_compact_null_string(&mut buf);
        put_compact_array_len(&mut buf, 1);
        put_compact_string(&mut buf, "a");
        buf.put_slice(&[1u8; 8]); // only half a UUID

        let err = DeleteShareGroupOffsetsResponse::decode_versioned(0, &mut buf.freeze())
            .expect_err("short UUID must error");
        assert!(err.to_string().contains("topic_id"), "got: {err}");
    }
}
