use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, decode_capacity,
};

// ============================================================================
// OffsetFetch request/response
// ============================================================================

/// Topic in OffsetFetch request.
#[derive(Debug, Clone)]
pub struct OffsetFetchRequestTopic {
    /// Topic name (v1–v9).
    pub name: String,
    /// Topic ID (v10+, KIP-848). Replaces `name` when set.
    pub topic_id: Option<[u8; 16]>,
    /// Partition indices.
    pub partition_indexes: Vec<i32>,
}

/// OffsetFetch request.
///
/// Constructed internally by the consumer group coordinator.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct OffsetFetchRequest {
    /// Group ID.
    pub group_id: String,
    /// Topics (null = all topics in group).
    pub topics: Option<Vec<OffsetFetchRequestTopic>>,
    /// Require stable offsets (v7+).
    pub require_stable: bool,
    /// Member ID for KIP-848 consumer protocol (v9+, nullable).
    pub member_id: Option<String>,
    /// Member epoch for KIP-848 consumer protocol (v9+).
    pub member_epoch: i32,
}

impl OffsetFetchRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::OffsetFetch
    }

    /// Encode for version 1-5 (non-flexible).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.encode_topics_non_flexible(buf)
    }

    /// Encode for version 6 (flexible: compact strings/arrays + tagged fields).
    pub fn encode_v6(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        self.encode_topics_compact(buf)?;
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 7 (flexible + require_stable).
    pub fn encode_v7(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        self.encode_topics_compact(buf)?;
        buf.put_u8(u8::from(self.require_stable));
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 8 (batched multi-group format, KIP-709).
    ///
    /// v8 wraps the request in a `Groups` compact array. We encode our
    /// single group as a one-element array.
    pub fn encode_v8(&self, buf: &mut impl BufMut) -> Result<()> {
        // Groups compact array: 1 element → varint len+1 = 2
        crate::util::varint::encode_unsigned_varint(2, buf);
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        self.encode_topics_compact(buf)?;
        TaggedFields::default().try_encode(buf)?; // per-group tagged fields
        buf.put_u8(u8::from(self.require_stable));
        TaggedFields::default().try_encode(buf)?; // top-level tagged fields
        Ok(())
    }

    /// Encode for version 9 (v8 + MemberId/MemberEpoch per group, KIP-848).
    pub fn encode_v9(&self, buf: &mut impl BufMut) -> Result<()> {
        // Groups compact array: 1 element
        crate::util::varint::encode_unsigned_varint(2, buf);
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        match &self.member_id {
            Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }
        self.member_epoch.encode(buf);
        self.encode_topics_compact(buf)?;
        TaggedFields::default().try_encode(buf)?; // per-group tagged fields
        buf.put_u8(u8::from(self.require_stable));
        TaggedFields::default().try_encode(buf)?; // top-level tagged fields
        Ok(())
    }

    /// Encode for version 10 (topic ID replaces topic name, KIP-848).
    pub fn encode_v10(&self, buf: &mut impl BufMut) -> Result<()> {
        // Groups compact array: 1 element
        crate::util::varint::encode_unsigned_varint(2, buf);
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        match &self.member_id {
            Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }
        self.member_epoch.encode(buf);
        // Topics with topic_id instead of name
        match &self.topics {
            Some(topics) => {
                let len = u32::try_from(topics.len().saturating_add(1)).map_err(|_| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::InvalidLength,
                        "topics array too large",
                    )
                })?;
                crate::util::varint::encode_unsigned_varint(len, buf);
                for topic in topics {
                    buf.put_slice(&topic.topic_id.unwrap_or([0u8; 16]));
                    let parts_len = u32::try_from(topic.partition_indexes.len().saturating_add(1))
                        .map_err(|_| {
                            KrafkaError::protocol_kind(
                                ProtocolErrorKind::InvalidLength,
                                "partitions array too large",
                            )
                        })?;
                    crate::util::varint::encode_unsigned_varint(parts_len, buf);
                    for partition in &topic.partition_indexes {
                        partition.encode(buf);
                    }
                    TaggedFields::default().try_encode(buf)?;
                }
            }
            None => {
                // null compact array: varint 0
                crate::util::varint::encode_unsigned_varint(0, buf);
            }
        }
        TaggedFields::default().try_encode(buf)?; // per-group tagged fields
        buf.put_u8(u8::from(self.require_stable));
        TaggedFields::default().try_encode(buf)?; // top-level tagged fields
        Ok(())
    }

    /// Encode topics for non-flexible versions (v0-v5).
    fn encode_topics_non_flexible(&self, buf: &mut impl BufMut) -> Result<()> {
        match &self.topics {
            Some(topics) => {
                buf.put_i32(array_len_i32(topics.len())?);
                for topic in topics {
                    KafkaString::new(&topic.name).try_encode(buf)?;
                    buf.put_i32(array_len_i32(topic.partition_indexes.len())?);
                    for partition in &topic.partition_indexes {
                        partition.encode(buf);
                    }
                }
            }
            None => {
                buf.put_i32(-1);
            }
        }
        Ok(())
    }

    /// Encode topics for flexible versions (v6+).
    fn encode_topics_compact(&self, buf: &mut impl BufMut) -> Result<()> {
        match &self.topics {
            Some(topics) => {
                let len = u32::try_from(topics.len().saturating_add(1)).map_err(|_| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::InvalidLength,
                        "topics array too large",
                    )
                })?;
                crate::util::varint::encode_unsigned_varint(len, buf);
                for topic in topics {
                    KafkaString::new(&topic.name).try_encode_compact(buf)?;
                    let parts_len = u32::try_from(topic.partition_indexes.len().saturating_add(1))
                        .map_err(|_| {
                            KrafkaError::protocol_kind(
                                ProtocolErrorKind::InvalidLength,
                                "partitions array too large",
                            )
                        })?;
                    crate::util::varint::encode_unsigned_varint(parts_len, buf);
                    for partition in &topic.partition_indexes {
                        partition.encode(buf);
                    }
                    TaggedFields::default().try_encode(buf)?;
                }
            }
            None => {
                // null compact array: varint 0
                crate::util::varint::encode_unsigned_varint(0, buf);
            }
        }
        Ok(())
    }
}

/// Partition in OffsetFetch response.
#[derive(Debug, Clone)]
pub struct OffsetFetchResponsePartition {
    /// Partition index.
    pub partition_index: i32,
    /// Committed offset.
    pub committed_offset: i64,
    /// Committed leader epoch (v5+).
    pub committed_leader_epoch: i32,
    /// Metadata.
    pub metadata: Option<String>,
    /// Error code.
    pub error_code: ErrorCode,
}

/// Topic in OffsetFetch response.
#[derive(Debug, Clone)]
pub struct OffsetFetchResponseTopic {
    /// Topic name (v1–v9).
    pub name: String,
    /// Topic ID (v10+, KIP-848). Replaces `name` when set.
    pub topic_id: Option<[u8; 16]>,
    /// Partitions.
    pub partitions: Vec<OffsetFetchResponsePartition>,
}

/// OffsetFetch response.
#[derive(Debug, Clone)]
pub struct OffsetFetchResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Topics.
    pub topics: Vec<OffsetFetchResponseTopic>,
    /// Error code (v2+).
    pub error_code: ErrorCode,
}

impl OffsetFetchResponse {
    /// Decode from version 1.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: 0,
            topics: Self::decode_topics(buf)?,
            error_code: ErrorCode::None,
        })
    }

    /// Decode from version 2.
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let topics = Self::decode_topics(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        Ok(Self {
            throttle_time_ms: 0,
            topics,
            error_code,
        })
    }

    /// Decode from version 3-4.
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topics = Self::decode_topics(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        Ok(Self {
            throttle_time_ms,
            topics,
            error_code,
        })
    }

    /// Decode from version 5 (v3 + committed_leader_epoch per partition).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topics = Self::decode_topics_v5(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        Ok(Self {
            throttle_time_ms,
            topics,
            error_code,
        })
    }

    /// Decode from version 6-7 (flexible + committed_leader_epoch).
    pub fn decode_v6(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topics = Self::decode_topics_compact(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            topics,
            error_code,
        })
    }

    /// Decode from version 8-9 (batched multi-group format, KIP-709).
    ///
    /// v8-v9 wraps the response in a `Groups` compact array; we extract
    /// the first group entry.
    pub fn decode_v8(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        if group_count == 0 {
            let _ = TaggedFields::decode(buf)?;
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::Malformed,
                "OffsetFetchResponse v8-v9 contained empty Groups array",
            ));
        }

        // Decode first group
        let _group_id = KafkaString::decode_compact(buf)?;
        let topics = Self::decode_topics_compact(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let _ = TaggedFields::decode(buf)?; // per-group tagged fields

        // Skip remaining groups
        for _ in 1..group_count {
            let _ = KafkaString::decode_compact(buf)?;
            Self::skip_topics_compact(buf)?;
            let _ = i16::decode(buf)?;
            let _ = TaggedFields::decode(buf)?;
        }
        let _ = TaggedFields::decode(buf)?; // top-level tagged fields

        Ok(Self {
            throttle_time_ms,
            topics,
            error_code,
        })
    }

    /// Decode from version 10 (topic ID replaces topic name, KIP-848).
    ///
    /// v10 wraps the response in a `Groups` compact array; we extract
    /// the first group entry. Topics use TopicId instead of Name.
    pub fn decode_v10(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        if group_count == 0 {
            let _ = TaggedFields::decode(buf)?;
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::Malformed,
                "OffsetFetchResponse v10 contained empty Groups array",
            ));
        }

        // Decode first group
        let _group_id = KafkaString::decode_compact(buf)?;
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut topics = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));

        for _ in 0..topic_count {
            if buf.remaining() < 16 {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::TruncatedFrame,
                    "not enough bytes for topic_id UUID",
                ));
            }
            let mut topic_id = [0u8; 16];
            buf.copy_to_slice(&mut topic_id);

            let part_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(decode_capacity(part_count, buf.remaining()));

            for _ in 0..part_count {
                let partition_index = i32::decode(buf)?;
                let committed_offset = i64::decode(buf)?;
                let committed_leader_epoch = i32::decode(buf)?;
                let metadata = KafkaString::decode_compact(buf)?.0;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let _ = TaggedFields::decode(buf)?;

                partitions.push(OffsetFetchResponsePartition {
                    partition_index,
                    committed_offset,
                    committed_leader_epoch,
                    metadata,
                    error_code,
                });
            }
            let _ = TaggedFields::decode(buf)?;
            topics.push(OffsetFetchResponseTopic {
                name: String::new(),
                topic_id: Some(topic_id),
                partitions,
            });
        }
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let _ = TaggedFields::decode(buf)?; // per-group tagged fields

        // Skip remaining groups
        for _ in 1..group_count {
            let _ = KafkaString::decode_compact(buf)?;
            // Skip topics with topic_id format
            let tc = check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            for _ in 0..tc {
                if buf.remaining() < 16 {
                    return Err(KrafkaError::protocol_kind(
                        ProtocolErrorKind::TruncatedFrame,
                        "not enough bytes for topic_id UUID",
                    ));
                }
                buf.advance(16); // skip topic_id
                let pc =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                for _ in 0..pc {
                    let _ = i32::decode(buf)?; // partition_index
                    let _ = i64::decode(buf)?; // committed_offset
                    let _ = i32::decode(buf)?; // committed_leader_epoch
                    let _ = KafkaString::decode_compact(buf)?; // metadata
                    let _ = i16::decode(buf)?; // error_code
                    let _ = TaggedFields::decode(buf)?;
                }
                let _ = TaggedFields::decode(buf)?;
            }
            let _ = i16::decode(buf)?;
            let _ = TaggedFields::decode(buf)?;
        }
        let _ = TaggedFields::decode(buf)?; // top-level tagged fields

        Ok(Self {
            throttle_time_ms,
            topics,
            error_code,
        })
    }

    /// Shared topics array decoder for v0-v4 (no committed_leader_epoch on wire).
    fn decode_topics(buf: &mut impl Buf) -> Result<Vec<OffsetFetchResponseTopic>> {
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions =
                Vec::with_capacity(decode_capacity(partition_count, buf.remaining()));

            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let committed_offset = i64::decode(buf)?;
                let metadata = KafkaString::decode(buf)?.0;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);

                partitions.push(OffsetFetchResponsePartition {
                    partition_index,
                    committed_offset,
                    committed_leader_epoch: -1,
                    metadata,
                    error_code,
                });
            }

            topics.push(OffsetFetchResponseTopic {
                name,
                topic_id: None,
                partitions,
            });
        }

        Ok(topics)
    }

    /// Shared topics array decoder for v5 (adds committed_leader_epoch).
    fn decode_topics_v5(buf: &mut impl Buf) -> Result<Vec<OffsetFetchResponseTopic>> {
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions =
                Vec::with_capacity(decode_capacity(partition_count, buf.remaining()));

            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let committed_offset = i64::decode(buf)?;
                let committed_leader_epoch = i32::decode(buf)?;
                let metadata = KafkaString::decode(buf)?.0;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);

                partitions.push(OffsetFetchResponsePartition {
                    partition_index,
                    committed_offset,
                    committed_leader_epoch,
                    metadata,
                    error_code,
                });
            }

            topics.push(OffsetFetchResponseTopic {
                name,
                topic_id: None,
                partitions,
            });
        }

        Ok(topics)
    }

    /// Shared topics array decoder for flexible versions (v6+, compact encoding + leader epoch).
    fn decode_topics_compact(buf: &mut impl Buf) -> Result<Vec<OffsetFetchResponseTopic>> {
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut topics = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let part_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(decode_capacity(part_count, buf.remaining()));

            for _ in 0..part_count {
                let partition_index = i32::decode(buf)?;
                let committed_offset = i64::decode(buf)?;
                let committed_leader_epoch = i32::decode(buf)?;
                let metadata = KafkaString::decode_compact(buf)?.0;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let _ = TaggedFields::decode(buf)?;

                partitions.push(OffsetFetchResponsePartition {
                    partition_index,
                    committed_offset,
                    committed_leader_epoch,
                    metadata,
                    error_code,
                });
            }
            let _ = TaggedFields::decode(buf)?;
            topics.push(OffsetFetchResponseTopic {
                name,
                topic_id: None,
                partitions,
            });
        }

        Ok(topics)
    }

    /// Skip a compact topics array (for skipping extra groups in v8+ batched format).
    fn skip_topics_compact(buf: &mut impl Buf) -> Result<()> {
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        for _ in 0..topic_count {
            let _ = KafkaString::decode_compact(buf)?;
            let part_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            for _ in 0..part_count {
                let _ = i32::decode(buf)?; // partition_index
                let _ = i64::decode(buf)?; // committed_offset
                let _ = i32::decode(buf)?; // committed_leader_epoch
                let _ = KafkaString::decode_compact(buf)?; // metadata
                let _ = i16::decode(buf)?; // error_code
                let _ = TaggedFields::decode(buf)?;
            }
            let _ = TaggedFields::decode(buf)?;
        }
        Ok(())
    }

    /// Get the offset for a specific topic-partition.
    pub fn get_offset(&self, topic: &str, partition: i32) -> Option<i64> {
        self.topics.iter().find(|t| t.name == topic).and_then(|t| {
            t.partitions
                .iter()
                .find(|p| p.partition_index == partition)
                .map(|p| p.committed_offset)
        })
    }
}

// ---------------------------------------------------------------------------
// VersionedEncode / VersionedDecode implementations
// ---------------------------------------------------------------------------

impl VersionedEncode for OffsetFetchRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1..=5 => self.encode_v1(buf)?,
            6 => self.encode_v6(buf)?,
            7 => self.encode_v7(buf)?,
            8 => self.encode_v8(buf)?,
            9 => self.encode_v9(buf)?,
            10 => self.encode_v10(buf)?,
            _ => return unsupported_encode!("OffsetFetchRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for OffsetFetchResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 => Self::decode_v1(buf),
            2 => Self::decode_v2(buf),
            3..=4 => Self::decode_v3(buf),
            5 => Self::decode_v5(buf),
            6..=7 => Self::decode_v6(buf),
            8..=9 => Self::decode_v8(buf),
            10 => Self::decode_v10(buf),
            _ => unsupported_decode!("OffsetFetchResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::util::varint;
    use bytes::BytesMut;
    use rstest::rstest;

    fn sample_offset_fetch_request() -> OffsetFetchRequest {
        OffsetFetchRequest {
            group_id: "my-group".to_string(),
            topics: Some(vec![OffsetFetchRequestTopic {
                name: "topic-1".to_string(),
                topic_id: None,
                partition_indexes: vec![0, 1, 2],
            }]),
            require_stable: true,
            member_id: Some("consumer-1".to_string()),
            member_epoch: 7,
        }
    }

    // ---- Story 1.3: OffsetFetch ----

    #[test]
    fn test_offset_fetch_request_v1_encode() {
        let request = OffsetFetchRequest {
            group_id: "grp1".to_string(),
            topics: Some(vec![OffsetFetchRequestTopic {
                name: "topic1".to_string(),
                topic_id: None,
                partition_indexes: vec![0, 1],
            }]),
            member_id: None,
            member_epoch: -1,
            require_stable: false,
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(1, &mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_offset_fetch_request_below_min_rejected() {
        let request = OffsetFetchRequest {
            group_id: "g".to_string(),
            topics: None,
            member_id: None,
            member_epoch: -1,
            require_stable: false,
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(0, &mut buf).is_err());
    }

    #[rstest]
    // OffsetFetch MIN=1
    #[case::of_v0(0)]
    fn test_offset_fetch_encode_below_min(#[case] version: i16) {
        let request = OffsetFetchRequest {
            group_id: "g".to_string(),
            topics: None,
            member_id: None,
            member_epoch: -1,
            require_stable: false,
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }

    #[rstest]
    #[case::v1_min(1)]
    #[case::v5(5)]
    fn test_offset_fetch_request_encode_non_flexible(#[case] version: i16) {
        let request = sample_offset_fetch_request();
        let mut buf = BytesMut::new();
        request.encode_versioned(version, &mut buf).unwrap();
        assert!(!buf.is_empty());
        // v1-v5 all use encode_v1.
        let mut buf2 = BytesMut::new();
        request.encode_v1(&mut buf2).unwrap();
        assert_eq!(buf, buf2);
    }

    #[test]
    fn test_offset_fetch_request_v6_flexible() {
        let request = sample_offset_fetch_request();
        let mut buf_v5 = BytesMut::new();
        request.encode_versioned(5, &mut buf_v5).unwrap();
        let mut buf_v6 = BytesMut::new();
        request.encode_versioned(6, &mut buf_v6).unwrap();
        assert_ne!(
            buf_v5.as_ref(),
            buf_v6.as_ref(),
            "v6 flexible should differ from v5"
        );
    }

    #[test]
    fn test_offset_fetch_request_v7_has_require_stable() {
        let request = sample_offset_fetch_request();
        let mut buf_v6 = BytesMut::new();
        request.encode_versioned(6, &mut buf_v6).unwrap();
        let mut buf_v7 = BytesMut::new();
        request.encode_versioned(7, &mut buf_v7).unwrap();
        // v7 adds require_stable (1 byte) — should be longer.
        assert!(
            buf_v7.len() > buf_v6.len(),
            "v7 should be longer (require_stable)"
        );
    }

    #[test]
    fn test_offset_fetch_request_v8_batched_groups() {
        let request = sample_offset_fetch_request();
        let mut buf_v7 = BytesMut::new();
        request.encode_versioned(7, &mut buf_v7).unwrap();
        let mut buf_v8 = BytesMut::new();
        request.encode_versioned(8, &mut buf_v8).unwrap();
        // v8 wraps the single group in a Groups array — different structure.
        assert_ne!(
            buf_v7.as_ref(),
            buf_v8.as_ref(),
            "v8 batched format should differ from v7"
        );
    }

    #[test]
    fn test_offset_fetch_request_v9_has_member_epoch() {
        let request = sample_offset_fetch_request();
        let mut buf_v8 = BytesMut::new();
        request.encode_versioned(8, &mut buf_v8).unwrap();
        let mut buf_v9 = BytesMut::new();
        request.encode_versioned(9, &mut buf_v9).unwrap();
        // v9 adds member_id + member_epoch — should be longer.
        assert!(
            buf_v9.len() > buf_v8.len(),
            "v9 should be longer (member_id + member_epoch)"
        );
    }

    #[test]
    fn test_offset_fetch_request_null_topics() {
        // null topics = "fetch all topics in group"
        let request = OffsetFetchRequest {
            group_id: "g".to_string(),
            topics: None,
            require_stable: false,
            member_id: None,
            member_epoch: -1,
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(1, &mut buf).unwrap();
        assert!(!buf.is_empty());

        let mut buf_v6 = BytesMut::new();
        request.encode_versioned(6, &mut buf_v6).unwrap();
        assert!(!buf_v6.is_empty());
    }

    #[test]
    fn test_offset_fetch_response_decode_v1() {
        let mut buf = BytesMut::new();
        // topics count
        buf.put_i32(1);
        let topic = b"topic-1";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        // partitions count
        buf.put_i32(1);
        buf.put_i32(0); // partition_index
        buf.put_i64(42); // committed_offset
        let meta = b"meta";
        buf.put_i16(meta.len() as i16);
        buf.put_slice(meta);
        buf.put_i16(0); // error_code

        let resp = OffsetFetchResponse::decode_versioned(1, &mut buf.freeze()).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name, "topic-1");
        assert_eq!(resp.topics[0].partitions[0].committed_offset, 42);
        assert_eq!(
            resp.topics[0].partitions[0].metadata.as_deref(),
            Some("meta")
        );
    }

    #[test]
    fn test_offset_fetch_response_decode_v2_has_error_code() {
        let mut buf = BytesMut::new();
        // topics count
        buf.put_i32(1);
        let topic = b"t";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        buf.put_i32(1); // partitions count
        buf.put_i32(0); // partition_index
        buf.put_i64(10); // committed_offset
        buf.put_i16(-1); // null metadata
        buf.put_i16(0); // partition error_code
        buf.put_i16(0); // top-level error_code (new in v2)

        let resp = OffsetFetchResponse::decode_versioned(2, &mut buf.freeze()).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.topics[0].partitions[0].committed_offset, 10);
    }

    #[test]
    fn test_offset_fetch_response_decode_v5_leader_epoch() {
        // v5 adds committed_leader_epoch per partition.
        let mut buf = BytesMut::new();
        buf.put_i32(50); // throttle_time_ms
        buf.put_i32(1); // topics count
        let topic = b"t";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        buf.put_i32(1); // partitions count
        buf.put_i32(0); // partition_index
        buf.put_i64(100); // committed_offset
        buf.put_i32(7); // committed_leader_epoch (new in v5)
        buf.put_i16(-1); // null metadata
        buf.put_i16(0); // partition error_code
        buf.put_i16(0); // top-level error_code

        let resp = OffsetFetchResponse::decode_versioned(5, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert_eq!(resp.topics[0].partitions[0].committed_leader_epoch, 7);
    }

    #[test]
    fn test_offset_fetch_response_decode_v6_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        // topics compact array: 1 + 1
        varint::encode_unsigned_varint(2, &mut buf);
        let topic = b"flex-topic";
        varint::encode_unsigned_varint(topic.len() as u32 + 1, &mut buf);
        buf.put_slice(topic);
        // partitions compact array: 1 + 1
        varint::encode_unsigned_varint(2, &mut buf);
        buf.put_i32(0); // partition_index
        buf.put_i64(200); // committed_offset
        buf.put_i32(3); // committed_leader_epoch
        // null compact string metadata
        varint::encode_unsigned_varint(0, &mut buf);
        buf.put_i16(0); // partition error_code
        varint::encode_unsigned_varint(0, &mut buf); // partition tagged fields
        varint::encode_unsigned_varint(0, &mut buf); // topic tagged fields
        buf.put_i16(0); // top-level error_code
        varint::encode_unsigned_varint(0, &mut buf); // top-level tagged fields

        let resp = OffsetFetchResponse::decode_versioned(6, &mut buf.freeze()).unwrap();
        assert_eq!(resp.topics[0].name, "flex-topic");
        assert_eq!(resp.topics[0].partitions[0].committed_offset, 200);
        assert_eq!(resp.topics[0].partitions[0].committed_leader_epoch, 3);
    }

    #[test]
    fn test_offset_fetch_response_decode_v8_batched() {
        // v8-v9: batched multi-group format. Single group wrapped in Groups array.
        // Wire order per decode_v8: group_id, topics (compact), error_code, group tagged fields.
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        // Groups compact array: 1 group + 1
        varint::encode_unsigned_varint(2, &mut buf);
        // group_id compact string
        let group = b"my-group";
        varint::encode_unsigned_varint(group.len() as u32 + 1, &mut buf);
        buf.put_slice(group);
        // topics compact array: 1 + 1
        varint::encode_unsigned_varint(2, &mut buf);
        let topic = b"t1";
        varint::encode_unsigned_varint(topic.len() as u32 + 1, &mut buf);
        buf.put_slice(topic);
        // partitions compact array: 1 + 1
        varint::encode_unsigned_varint(2, &mut buf);
        buf.put_i32(0); // partition_index
        buf.put_i64(500); // committed_offset
        buf.put_i32(2); // committed_leader_epoch
        // null metadata
        varint::encode_unsigned_varint(0, &mut buf);
        buf.put_i16(0); // partition error_code
        varint::encode_unsigned_varint(0, &mut buf); // partition tagged fields
        varint::encode_unsigned_varint(0, &mut buf); // topic tagged fields
        buf.put_i16(0); // group error_code (after topics, per decode_v8)
        varint::encode_unsigned_varint(0, &mut buf); // group tagged fields
        varint::encode_unsigned_varint(0, &mut buf); // top-level tagged fields

        let resp = OffsetFetchResponse::decode_versioned(8, &mut buf.freeze()).unwrap();
        assert_eq!(resp.topics[0].name, "t1");
        assert_eq!(resp.topics[0].partitions[0].committed_offset, 500);

        // v9 uses the same decoder.
        let mut buf2 = BytesMut::new();
        buf2.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(2, &mut buf2); // 1 group + 1
        let grp = b"g";
        varint::encode_unsigned_varint(grp.len() as u32 + 1, &mut buf2);
        buf2.put_slice(grp);
        // topics
        varint::encode_unsigned_varint(2, &mut buf2); // 1 topic + 1
        let t = b"t";
        varint::encode_unsigned_varint(t.len() as u32 + 1, &mut buf2);
        buf2.put_slice(t);
        varint::encode_unsigned_varint(2, &mut buf2); // 1 partition + 1
        buf2.put_i32(0);
        buf2.put_i64(1);
        buf2.put_i32(0);
        varint::encode_unsigned_varint(0, &mut buf2); // null metadata
        buf2.put_i16(0); // partition error_code
        varint::encode_unsigned_varint(0, &mut buf2); // partition tagged fields
        varint::encode_unsigned_varint(0, &mut buf2); // topic tagged fields
        buf2.put_i16(0); // group error_code
        varint::encode_unsigned_varint(0, &mut buf2); // group tagged fields
        varint::encode_unsigned_varint(0, &mut buf2); // top-level tagged fields

        let resp2 = OffsetFetchResponse::decode_versioned(9, &mut buf2.freeze()).unwrap();
        assert_eq!(resp2.topics[0].partitions[0].committed_offset, 1);
    }

    // ── OffsetFetch v10 (topic_id in Groups wrapper) ──

    #[test]
    fn test_offset_fetch_request_encode_v10_topic_id() {
        let topic_id: [u8; 16] = [0xEE; 16];
        let request = OffsetFetchRequest {
            group_id: "g".to_string(),
            topics: Some(vec![OffsetFetchRequestTopic {
                name: String::new(),
                topic_id: Some(topic_id),
                partition_indexes: vec![0],
            }]),
            require_stable: true,
            member_id: Some("m1".to_string()),
            member_epoch: 3,
        };
        let mut buf = BytesMut::new();
        request.encode_v10(&mut buf).unwrap();

        // Verify groups wrapper is present
        let mut cur = &buf[..];
        let groups_varint = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(groups_varint, 2); // 1 group + 1
        // group_id
        let gid_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(gid_len, 2); // "g".len() + 1
        cur.advance(1);
        // member_id
        let mid_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(mid_len, 3); // "m1".len() + 1
        cur.advance(2);
        assert_eq!(cur.get_i32(), 3); // member_epoch
        // topics
        let topics_varint = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(topics_varint, 2); // 1 topic + 1
        // topic_id UUID
        let mut read_id = [0u8; 16];
        cur.copy_to_slice(&mut read_id);
        assert_eq!(read_id, topic_id);
    }

    #[test]
    fn test_offset_fetch_response_decode_v10_topic_id() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(2, &mut buf); // 1 group + 1
        // group_id compact string
        varint::encode_unsigned_varint(3, &mut buf); // "g1".len() + 1
        buf.put_slice(b"g1");
        // topics compact array
        varint::encode_unsigned_varint(2, &mut buf); // 1 topic + 1
        let topic_id: [u8; 16] = [0xFF; 16];
        buf.put_slice(&topic_id);
        // partitions compact array
        varint::encode_unsigned_varint(2, &mut buf); // 1 partition + 1
        buf.put_i32(0); // partition_index
        buf.put_i64(77); // committed_offset
        buf.put_i32(4); // committed_leader_epoch
        varint::encode_unsigned_varint(0, &mut buf); // metadata null
        buf.put_i16(0); // error_code
        varint::encode_unsigned_varint(0, &mut buf); // partition tagged
        varint::encode_unsigned_varint(0, &mut buf); // topic tagged
        buf.put_i16(0); // group error_code
        varint::encode_unsigned_varint(0, &mut buf); // group tagged
        varint::encode_unsigned_varint(0, &mut buf); // top tagged

        let resp = OffsetFetchResponse::decode_versioned(10, &mut buf.freeze()).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].topic_id, Some(topic_id));
        assert!(resp.topics[0].name.is_empty());
        assert_eq!(resp.topics[0].partitions[0].committed_offset, 77);
        assert_eq!(resp.topics[0].partitions[0].committed_leader_epoch, 4);
    }
}
