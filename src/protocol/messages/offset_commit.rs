use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, decode_capacity,
};

// ============================================================================
// OffsetCommit request/response
// ============================================================================

/// Partition in OffsetCommit request.
#[derive(Debug, Clone)]
pub struct OffsetCommitRequestPartition {
    /// Partition index.
    pub partition_index: i32,
    /// Committed offset.
    pub committed_offset: i64,
    /// Committed leader epoch (v6+).
    pub committed_leader_epoch: i32,
    /// Commit timestamp (v1; deprecated in v2+).
    pub commit_timestamp: i64,
    /// Metadata.
    pub committed_metadata: Option<String>,
}

/// Topic in OffsetCommit request.
#[derive(Debug, Clone)]
pub struct OffsetCommitRequestTopic {
    /// Topic name (v2–v9).
    pub name: String,
    /// Topic ID (v10+, KIP-848). Replaces `name` when set.
    pub topic_id: Option<[u8; 16]>,
    /// Partitions.
    pub partitions: Vec<OffsetCommitRequestPartition>,
}

/// OffsetCommit request.
#[derive(Debug, Clone)]
pub struct OffsetCommitRequest {
    /// Group ID.
    pub group_id: String,
    /// Generation ID (v1+).
    pub generation_id: i32,
    /// Member ID (v1+).
    pub member_id: String,
    /// Group instance ID (v7+).
    pub group_instance_id: Option<String>,
    /// Retention time (v2-v4; deprecated).
    pub retention_time_ms: i64,
    /// Topics.
    pub topics: Vec<OffsetCommitRequestTopic>,
}

impl OffsetCommitRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::OffsetCommit
    }

    /// Encode for version 2-4.
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;
        self.retention_time_ms.encode(buf);

        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition_index.encode(buf);
                partition.committed_offset.encode(buf);
                match &partition.committed_metadata {
                    Some(m) => KafkaString::new(m).try_encode(buf)?,
                    None => KafkaString::null().try_encode(buf)?,
                }
            }
        }
        Ok(())
    }

    /// Encode for version 5 (v2 without retention_time_ms).
    pub fn encode_v5(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;

        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition_index.encode(buf);
                partition.committed_offset.encode(buf);
                match &partition.committed_metadata {
                    Some(m) => KafkaString::new(m).try_encode(buf)?,
                    None => KafkaString::null().try_encode(buf)?,
                }
            }
        }
        Ok(())
    }

    /// Encode for version 6 (v5 + committed_leader_epoch per partition).
    pub fn encode_v6(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;

        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition_index.encode(buf);
                partition.committed_offset.encode(buf);
                partition.committed_leader_epoch.encode(buf);
                match &partition.committed_metadata {
                    Some(m) => KafkaString::new(m).try_encode(buf)?,
                    None => KafkaString::null().try_encode(buf)?,
                }
            }
        }
        Ok(())
    }

    /// Encode for version 7 (v6 + group_instance_id).
    pub fn encode_v7(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode(buf)?,
            None => KafkaString::null().try_encode(buf)?,
        }

        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition_index.encode(buf);
                partition.committed_offset.encode(buf);
                partition.committed_leader_epoch.encode(buf);
                match &partition.committed_metadata {
                    Some(m) => KafkaString::new(m).try_encode(buf)?,
                    None => KafkaString::null().try_encode(buf)?,
                }
            }
        }
        Ok(())
    }

    /// Encode for version 8-9 (flexible: compact strings/arrays + tagged fields).
    ///
    /// v9 is wire-identical to v8 (KIP-848 adds STALE_MEMBER_EPOCH semantics only).
    pub fn encode_v8(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode_compact(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }

        let topics_len = u32::try_from(self.topics.len().saturating_add(1)).map_err(|_| {
            KrafkaError::protocol_kind(ProtocolErrorKind::InvalidLength, "topics array too large")
        })?;
        crate::util::varint::encode_unsigned_varint(topics_len, buf);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode_compact(buf)?;
            let parts_len =
                u32::try_from(topic.partitions.len().saturating_add(1)).map_err(|_| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::InvalidLength,
                        "partitions array too large",
                    )
                })?;
            crate::util::varint::encode_unsigned_varint(parts_len, buf);
            for partition in &topic.partitions {
                partition.partition_index.encode(buf);
                partition.committed_offset.encode(buf);
                partition.committed_leader_epoch.encode(buf);
                match &partition.committed_metadata {
                    Some(m) => KafkaString::new(m).try_encode_compact(buf)?,
                    None => KafkaString::null().try_encode_compact(buf)?,
                }
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 10 (topic ID replaces topic name, KIP-848).
    pub fn encode_v10(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.group_id).try_encode_compact(buf)?;
        self.generation_id.encode(buf);
        KafkaString::new(&self.member_id).try_encode_compact(buf)?;
        match &self.group_instance_id {
            Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }

        let topics_len = u32::try_from(self.topics.len().saturating_add(1)).map_err(|_| {
            KrafkaError::protocol_kind(ProtocolErrorKind::InvalidLength, "topics array too large")
        })?;
        crate::util::varint::encode_unsigned_varint(topics_len, buf);
        for topic in &self.topics {
            buf.put_slice(&topic.topic_id.unwrap_or([0u8; 16]));
            let parts_len =
                u32::try_from(topic.partitions.len().saturating_add(1)).map_err(|_| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::InvalidLength,
                        "partitions array too large",
                    )
                })?;
            crate::util::varint::encode_unsigned_varint(parts_len, buf);
            for partition in &topic.partitions {
                partition.partition_index.encode(buf);
                partition.committed_offset.encode(buf);
                partition.committed_leader_epoch.encode(buf);
                match &partition.committed_metadata {
                    Some(m) => KafkaString::new(m).try_encode_compact(buf)?,
                    None => KafkaString::null().try_encode_compact(buf)?,
                }
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Partition in OffsetCommit response.
#[derive(Debug, Clone)]
pub struct OffsetCommitResponsePartition {
    /// Partition index.
    pub partition_index: i32,
    /// Error code.
    pub error_code: ErrorCode,
}

/// Topic in OffsetCommit response.
#[derive(Debug, Clone)]
pub struct OffsetCommitResponseTopic {
    /// Topic name (v2–v9).
    pub name: String,
    /// Topic ID (v10+, KIP-848). Replaces `name` when set.
    pub topic_id: Option<[u8; 16]>,
    /// Partitions.
    pub partitions: Vec<OffsetCommitResponsePartition>,
}

/// OffsetCommit response.
#[derive(Debug, Clone)]
pub struct OffsetCommitResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Topics.
    pub topics: Vec<OffsetCommitResponseTopic>,
}

impl OffsetCommitResponse {
    /// Decode from version 2.
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        Ok(Self {
            throttle_time_ms: 0,
            topics: Self::decode_topics(buf)?,
        })
    }

    /// Decode from version 3-7 (non-flexible, adds throttle_time_ms).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            topics: Self::decode_topics(buf)?,
        })
    }

    /// Decode from version 8-9 (flexible: compact strings/arrays + tagged fields).
    pub fn decode_v8(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topics = Self::decode_topics_compact(buf)?;
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }

    /// Decode from version 10 (topic ID replaces topic name, KIP-848).
    pub fn decode_v10(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;

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
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let _ = TaggedFields::decode(buf)?;
                partitions.push(OffsetCommitResponsePartition {
                    partition_index,
                    error_code,
                });
            }
            let _ = TaggedFields::decode(buf)?;
            topics.push(OffsetCommitResponseTopic {
                name: String::new(),
                topic_id: Some(topic_id),
                partitions,
            });
        }
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }

    /// Shared topics array decoder for non-flexible versions.
    fn decode_topics(buf: &mut impl Buf) -> Result<Vec<OffsetCommitResponseTopic>> {
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions =
                Vec::with_capacity(decode_capacity(partition_count, buf.remaining()));

            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                partitions.push(OffsetCommitResponsePartition {
                    partition_index,
                    error_code,
                });
            }

            topics.push(OffsetCommitResponseTopic {
                name,
                topic_id: None,
                partitions,
            });
        }

        Ok(topics)
    }

    /// Check if all partitions succeeded.
    pub fn all_success(&self) -> bool {
        self.topics
            .iter()
            .flat_map(|t| t.partitions.iter())
            .all(|p| p.error_code.is_ok())
    }

    /// Shared topics array decoder for flexible versions (v8+).
    fn decode_topics_compact(buf: &mut impl Buf) -> Result<Vec<OffsetCommitResponseTopic>> {
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
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let _ = TaggedFields::decode(buf)?;
                partitions.push(OffsetCommitResponsePartition {
                    partition_index,
                    error_code,
                });
            }
            let _ = TaggedFields::decode(buf)?;
            topics.push(OffsetCommitResponseTopic {
                name,
                topic_id: None,
                partitions,
            });
        }

        Ok(topics)
    }
}

// ---------------------------------------------------------------------------
// VersionedEncode / VersionedDecode implementations
// ---------------------------------------------------------------------------

impl VersionedEncode for OffsetCommitRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            2..=4 => self.encode_v2(buf)?,
            5 => self.encode_v5(buf)?,
            6 => self.encode_v6(buf)?,
            7 => self.encode_v7(buf)?,
            8..=9 => self.encode_v8(buf)?,
            10 => self.encode_v10(buf)?,
            _ => return unsupported_encode!("OffsetCommitRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for OffsetCommitResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            2 => Self::decode_v2(buf),
            3..=7 => Self::decode_v3(buf),
            8..=9 => Self::decode_v8(buf),
            10 => Self::decode_v10(buf),
            _ => unsupported_decode!("OffsetCommitResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::protocol::*;
    use crate::util::varint;
    use bytes::BytesMut;
    use rstest::rstest;

    fn sample_offset_commit_request() -> OffsetCommitRequest {
        OffsetCommitRequest {
            group_id: "my-group".to_string(),
            generation_id: 5,
            member_id: "member-1".to_string(),
            group_instance_id: Some("instance-x".to_string()),
            retention_time_ms: 86_400_000,
            topics: vec![OffsetCommitRequestTopic {
                name: "orders".to_string(),
                topic_id: None,
                partitions: vec![
                    OffsetCommitRequestPartition {
                        partition_index: 0,
                        committed_offset: 42,
                        committed_leader_epoch: 3,
                        commit_timestamp: -1,
                        committed_metadata: Some("metadata".to_string()),
                    },
                    OffsetCommitRequestPartition {
                        partition_index: 1,
                        committed_offset: 100,
                        committed_leader_epoch: 5,
                        commit_timestamp: -1,
                        committed_metadata: None,
                    },
                ],
            }],
        }
    }

    // ── TxnOffsetCommit (Story 10.4) ────────────────────────────────────

    #[test]
    fn test_txn_offset_commit_v3_flexible() {
        let request =
            TxnOffsetCommitRequest::new("txn-1", "grp-1", 100, 0).add_offset("topic1", 0, 50, None);

        let mut v2 = BytesMut::new();
        request.encode_v2(&mut v2).unwrap();
        let mut v3 = BytesMut::new();
        request.encode_v3(&mut v3).unwrap();

        // v3 adds generation_id (4), member_id (compact, 1), group_instance_id (compact, 1)
        // + per-element tagged fields bytes. Overall different wire format.
        assert_ne!(v2.freeze(), v3.clone().freeze());

        // Dispatch routes v3-v5 to encode_v3
        let mut v5 = BytesMut::new();
        request.encode_versioned(5, &mut v5).unwrap();
        assert_eq!(v3.freeze(), v5.freeze());
    }

    #[test]
    fn test_txn_offset_commit_v3_with_member_info() {
        let mut request = TxnOffsetCommitRequest::new("txn-1", "grp-1", 100, 0).add_offset(
            "topic1",
            0,
            50,
            Some("meta".to_string()),
        );
        request.generation_id = 42;
        request.member_id = "member-1".to_string();
        request.group_instance_id = Some("instance-1".to_string());

        let mut buf = BytesMut::new();
        request.encode_v3(&mut buf).unwrap();
        // Should encode successfully with member-level fields
        assert!(!buf.is_empty());

        // Verify it's larger than without member info
        let default_request = TxnOffsetCommitRequest::new("txn-1", "grp-1", 100, 0).add_offset(
            "topic1",
            0,
            50,
            Some("meta".to_string()),
        );
        let mut buf2 = BytesMut::new();
        default_request.encode_v3(&mut buf2).unwrap();
        assert!(buf.len() > buf2.len());
    }

    #[test]
    fn test_txn_offset_commit_response_v3_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(5); // throttle_time_ms
        crate::util::varint::encode_unsigned_varint(2, &mut buf); // topics: 1 + 1
        let name = b"t1";
        crate::util::varint::encode_unsigned_varint(name.len() as u32 + 1, &mut buf);
        buf.put_slice(name);
        crate::util::varint::encode_unsigned_varint(2, &mut buf); // partitions: 1 + 1
        buf.put_i32(0); // partition
        buf.put_i16(0); // error_code
        buf.put_u8(0); // partition tagged fields
        buf.put_u8(0); // topic tagged fields
        buf.put_u8(0); // top-level tagged fields

        let resp = TxnOffsetCommitResponse::decode_v3(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 5);
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name, "t1");
        assert!(resp.is_ok());
    }

    #[rstest]
    #[case::v3(3)]
    #[case::v4(4)]
    #[case::v5(5)]
    fn test_txn_offset_commit_response_v3_v5_decode(#[case] version: i16) {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        crate::util::varint::encode_unsigned_varint(2, &mut buf); // topics: 1 + 1
        let name = b"t1";
        crate::util::varint::encode_unsigned_varint(name.len() as u32 + 1, &mut buf);
        buf.put_slice(name);
        crate::util::varint::encode_unsigned_varint(1, &mut buf); // partitions: 0 + 1 (empty)
        buf.put_u8(0); // topic tagged fields
        buf.put_u8(0); // top-level tagged fields
        let resp = TxnOffsetCommitResponse::decode_versioned(version, &mut buf.freeze()).unwrap();
        assert_eq!(resp.topics[0].name, "t1");
        assert!(resp.topics[0].partitions.is_empty());
    }

    #[test]
    fn test_txn_offset_commit_request() {
        let request = TxnOffsetCommitRequest::new("my-txn", "my-group", 12345, 0)
            .add_offset("topic1", 0, 100, Some("metadata".to_string()))
            .add_offset("topic1", 1, 200, None);

        assert_eq!(request.transactional_id, "my-txn");
        assert_eq!(request.group_id, "my-group");
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].partitions.len(), 2);

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    // ── TxnOffsetCommit v2 dispatch ──────────────────────────────────

    #[test]
    fn test_txn_offset_commit_v2_dispatch() {
        let request =
            TxnOffsetCommitRequest::new("txn-1", "grp-1", 100, 0).add_offset("topic1", 0, 50, None);

        let mut buf = BytesMut::new();
        request.encode_versioned(2, &mut buf).unwrap();
        assert!(!buf.is_empty());

        // v2 should include committed_leader_epoch per partition.
        let mut buf_v0 = BytesMut::new();
        request.encode_versioned(0, &mut buf_v0).unwrap();
        // v2 encodes additional leader_epoch (i32) per partition → larger.
        assert!(buf.len() > buf_v0.len());
    }

    #[test]
    fn test_txn_offset_commit_response_decode_v2() {
        // Response v0-v2 are wire-identical.
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(0);
        // topics count
        buf.put_i32(1);
        // topic name
        let topic = b"t1";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        // partitions count
        buf.put_i32(1);
        // partition
        buf.put_i32(0);
        // error_code
        buf.put_i16(0);

        let resp = TxnOffsetCommitResponse::decode_versioned(2, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 0);
        assert_eq!(resp.topics.len(), 1);
        assert!(resp.is_ok());
    }

    // ---- Story 1.2: OffsetCommit ----

    #[test]
    fn test_offset_commit_request_v2_encode() {
        let request = OffsetCommitRequest {
            group_id: "grp".to_string(),
            generation_id: 1,
            member_id: "m1".to_string(),
            group_instance_id: None,
            retention_time_ms: 86_400_000,
            topics: vec![OffsetCommitRequestTopic {
                name: "t".to_string(),
                topic_id: None,
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 100,
                    committed_leader_epoch: -1,
                    commit_timestamp: -1,
                    committed_metadata: Some("meta".to_string()),
                }],
            }],
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(2, &mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_offset_commit_request_below_min_rejected() {
        let request = OffsetCommitRequest {
            group_id: "g".to_string(),
            generation_id: 0,
            member_id: String::new(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: vec![],
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(0, &mut buf).is_err());
        let mut buf2 = BytesMut::new();
        assert!(request.encode_versioned(1, &mut buf2).is_err());
    }

    #[test]
    fn test_offset_commit_response_decode_v2() {
        let mut buf = BytesMut::new();
        // 1 topic
        buf.put_i32(1);
        let topic = b"t";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        // 1 partition
        buf.put_i32(1);
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code

        let resp = OffsetCommitResponse::decode_versioned(2, &mut buf.freeze()).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].partitions[0].error_code, ErrorCode::None);
    }

    #[rstest]
    // OffsetCommit MIN=2
    #[case::oc_v0(0)]
    #[case::oc_v1(1)]
    fn test_offset_commit_encode_below_min(#[case] version: i16) {
        let request = OffsetCommitRequest {
            group_id: "g".to_string(),
            generation_id: 0,
            member_id: String::new(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: vec![],
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }

    #[rstest]
    #[case::v2(2)]
    #[case::v4(4)]
    fn test_offset_commit_request_v2_to_v4_has_retention_time(#[case] version: i16) {
        let request = sample_offset_commit_request();
        let mut buf = BytesMut::new();
        request.encode_versioned(version, &mut buf).unwrap();
        assert!(!buf.is_empty());
        // All v2-v4 use encode_v2 — verify identical wire output.
        let mut buf2 = BytesMut::new();
        request.encode_v2(&mut buf2).unwrap();
        assert_eq!(buf, buf2);
    }

    #[test]
    fn test_offset_commit_request_v5_drops_retention_time() {
        let request = sample_offset_commit_request();
        let mut buf_v2 = BytesMut::new();
        request.encode_versioned(2, &mut buf_v2).unwrap();
        let mut buf_v5 = BytesMut::new();
        request.encode_versioned(5, &mut buf_v5).unwrap();
        // v5 omits retention_time_ms (8 bytes) — should be shorter.
        assert!(
            buf_v5.len() < buf_v2.len(),
            "v5 should be shorter (no retention_time_ms)"
        );
    }

    #[test]
    fn test_offset_commit_request_v6_has_leader_epoch() {
        let request = sample_offset_commit_request();
        let mut buf_v5 = BytesMut::new();
        request.encode_versioned(5, &mut buf_v5).unwrap();
        let mut buf_v6 = BytesMut::new();
        request.encode_versioned(6, &mut buf_v6).unwrap();
        // v6 adds committed_leader_epoch (4 bytes per partition) — should be longer.
        assert!(
            buf_v6.len() > buf_v5.len(),
            "v6 should be longer (committed_leader_epoch)"
        );
    }

    #[test]
    fn test_offset_commit_request_v8_flexible() {
        let request = sample_offset_commit_request();
        let mut buf_v7 = BytesMut::new();
        request.encode_versioned(7, &mut buf_v7).unwrap();
        let mut buf_v8 = BytesMut::new();
        request.encode_versioned(8, &mut buf_v8).unwrap();
        // Flexible encoding uses compact strings (varints) — typically shorter.
        assert_ne!(
            buf_v7.as_ref(),
            buf_v8.as_ref(),
            "v8 flexible should differ from v7"
        );
    }

    #[rstest]
    #[case::v8(8)]
    #[case::v9(9)]
    fn test_offset_commit_request_v8_v9_wire_identical(#[case] version: i16) {
        let request = sample_offset_commit_request();
        let mut buf = BytesMut::new();
        request.encode_versioned(version, &mut buf).unwrap();
        let mut buf_v8 = BytesMut::new();
        request.encode_v8(&mut buf_v8).unwrap();
        assert_eq!(buf, buf_v8, "v8 and v9 should be wire-identical");
    }

    #[test]
    fn test_offset_commit_response_decode_v2_wire_format() {
        let mut buf = BytesMut::new();
        // topics count
        buf.put_i32(1);
        let topic = b"orders";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        // partitions count
        buf.put_i32(1);
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code (NONE)

        let resp = OffsetCommitResponse::decode_versioned(2, &mut buf.freeze()).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name, "orders");
        assert_eq!(resp.topics[0].partitions[0].partition_index, 0);
        assert!(resp.topics[0].partitions[0].error_code.is_ok());
        // v2 has no throttle_time_ms.
        assert_eq!(resp.throttle_time_ms, 0);
    }

    #[test]
    fn test_offset_commit_response_decode_v3_throttle() {
        // v3+: throttle_time_ms added at start.
        let mut buf = BytesMut::new();
        buf.put_i32(200); // throttle_time_ms
        buf.put_i32(1); // topics count
        let topic = b"t";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        buf.put_i32(1); // partitions count
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code

        let resp = OffsetCommitResponse::decode_versioned(3, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 200);
        assert_eq!(resp.topics[0].name, "t");
    }

    #[test]
    fn test_offset_commit_response_decode_v8_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(100); // throttle_time_ms
        // topics compact array: 1 topic + 1
        varint::encode_unsigned_varint(2, &mut buf);
        // topic name compact string
        let topic = b"committed-topic";
        varint::encode_unsigned_varint(topic.len() as u32 + 1, &mut buf);
        buf.put_slice(topic);
        // partitions compact array: 1 + 1
        varint::encode_unsigned_varint(2, &mut buf);
        buf.put_i32(3); // partition_index
        buf.put_i16(0); // error_code
        varint::encode_unsigned_varint(0, &mut buf); // partition tagged fields
        varint::encode_unsigned_varint(0, &mut buf); // topic tagged fields
        varint::encode_unsigned_varint(0, &mut buf); // top-level tagged fields

        let resp = OffsetCommitResponse::decode_versioned(8, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 100);
        assert_eq!(resp.topics[0].name, "committed-topic");
        assert_eq!(resp.topics[0].partitions[0].partition_index, 3);
        assert!(resp.topics[0].partitions[0].error_code.is_ok());
    }

    #[test]
    fn test_offset_commit_response_v9_decodes_same_as_v8() {
        // v9 is wire-identical to v8 for the response.
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        varint::encode_unsigned_varint(2, &mut buf);
        let topic = b"t1";
        varint::encode_unsigned_varint(3, &mut buf);
        buf.put_slice(topic);
        varint::encode_unsigned_varint(2, &mut buf);
        buf.put_i32(0);
        buf.put_i16(0);
        varint::encode_unsigned_varint(0, &mut buf);
        varint::encode_unsigned_varint(0, &mut buf);
        varint::encode_unsigned_varint(0, &mut buf);

        let resp = OffsetCommitResponse::decode_versioned(9, &mut buf.freeze()).unwrap();
        assert!(resp.topics[0].partitions[0].error_code.is_ok());
    }

    // ── OffsetCommit v10 (topic_id) ──

    #[test]
    fn test_offset_commit_request_encode_v10_topic_id() {
        let topic_id: [u8; 16] = [0xCC; 16];
        let request = OffsetCommitRequest {
            group_id: "grp".to_string(),
            generation_id: 1,
            member_id: "m".to_string(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: vec![OffsetCommitRequestTopic {
                name: String::new(),
                topic_id: Some(topic_id),
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 50,
                    committed_leader_epoch: 2,
                    commit_timestamp: -1,
                    committed_metadata: None,
                }],
            }],
        };
        let mut buf = BytesMut::new();
        request.encode_v10(&mut buf).unwrap();

        let mut cur = &buf[..];
        // group_id compact string
        let gid_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(gid_len, 4); // "grp".len() + 1
        let mut gid_bytes = vec![0u8; 3];
        cur.copy_to_slice(&mut gid_bytes);
        assert_eq!(&gid_bytes, b"grp");
        assert_eq!(cur.get_i32(), 1); // generation_id
        // member_id compact string
        let mid_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(mid_len, 2); // "m".len() + 1
        cur.advance(1);
        // group_instance_id nullable compact string = null (0)
        let gii_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(gii_len, 0);
        // topics compact array
        let topic_count = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(topic_count, 2); // 1 + 1
        // topic_id UUID
        let mut read_id = [0u8; 16];
        cur.copy_to_slice(&mut read_id);
        assert_eq!(read_id, topic_id);
    }

    #[test]
    fn test_offset_commit_response_decode_v10_topic_id() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(2, &mut buf); // 1 topic + 1
        let topic_id: [u8; 16] = [0xDD; 16];
        buf.put_slice(&topic_id);
        varint::encode_unsigned_varint(2, &mut buf); // 1 partition + 1
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code
        varint::encode_unsigned_varint(0, &mut buf); // partition tagged
        varint::encode_unsigned_varint(0, &mut buf); // topic tagged
        varint::encode_unsigned_varint(0, &mut buf); // top tagged

        let resp = OffsetCommitResponse::decode_versioned(10, &mut buf.freeze()).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].topic_id, Some(topic_id));
        assert!(resp.topics[0].name.is_empty());
    }
}
