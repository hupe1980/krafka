use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{array_len_i32, check_compact_array_len, check_decode_array_len};

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

        let topics_len = u32::try_from(self.topics.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("topics array too large"))?;
        crate::util::varint::encode_unsigned_varint(topics_len, buf);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode_compact(buf)?;
            let parts_len = u32::try_from(topic.partitions.len().saturating_add(1))
                .map_err(|_| KrafkaError::protocol("partitions array too large"))?;
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

        let topics_len = u32::try_from(self.topics.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("topics array too large"))?;
        crate::util::varint::encode_unsigned_varint(topics_len, buf);
        for topic in &self.topics {
            buf.put_slice(&topic.topic_id.unwrap_or([0u8; 16]));
            let parts_len = u32::try_from(topic.partitions.len().saturating_add(1))
                .map_err(|_| KrafkaError::protocol("partitions array too large"))?;
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
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            if buf.remaining() < 16 {
                return Err(KrafkaError::protocol("not enough bytes for topic_id UUID"));
            }
            let mut topic_id = [0u8; 16];
            buf.copy_to_slice(&mut topic_id);

            let part_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(part_count);

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
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

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
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let part_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(part_count);

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
                let len = u32::try_from(topics.len().saturating_add(1))
                    .map_err(|_| KrafkaError::protocol("topics array too large"))?;
                crate::util::varint::encode_unsigned_varint(len, buf);
                for topic in topics {
                    buf.put_slice(&topic.topic_id.unwrap_or([0u8; 16]));
                    let parts_len = u32::try_from(topic.partition_indexes.len().saturating_add(1))
                        .map_err(|_| KrafkaError::protocol("partitions array too large"))?;
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
                let len = u32::try_from(topics.len().saturating_add(1))
                    .map_err(|_| KrafkaError::protocol("topics array too large"))?;
                crate::util::varint::encode_unsigned_varint(len, buf);
                for topic in topics {
                    KafkaString::new(&topic.name).try_encode_compact(buf)?;
                    let parts_len = u32::try_from(topic.partition_indexes.len().saturating_add(1))
                        .map_err(|_| KrafkaError::protocol("partitions array too large"))?;
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
            return Err(KrafkaError::protocol(
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
            return Err(KrafkaError::protocol(
                "OffsetFetchResponse v10 contained empty Groups array",
            ));
        }

        // Decode first group
        let _group_id = KafkaString::decode_compact(buf)?;
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            if buf.remaining() < 16 {
                return Err(KrafkaError::protocol("not enough bytes for topic_id UUID"));
            }
            let mut topic_id = [0u8; 16];
            buf.copy_to_slice(&mut topic_id);

            let part_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(part_count);

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
                    return Err(KrafkaError::protocol("not enough bytes for topic_id UUID"));
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
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

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
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

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
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let part_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(part_count);

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

// ============================================================================
// OffsetForLeaderEpoch API (Key 23)
// ============================================================================

/// Partition in OffsetForLeaderEpoch request.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochPartition {
    /// Partition index.
    pub partition: i32,
    /// Current leader epoch (v2+, for fencing).
    pub current_leader_epoch: i32,
    /// Requested leader epoch.
    pub leader_epoch: i32,
}

/// Topic in OffsetForLeaderEpoch request.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochTopic {
    /// Topic name.
    pub topic: String,
    /// Partitions.
    pub partitions: Vec<OffsetForLeaderEpochPartition>,
}

/// OffsetForLeaderEpoch request.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochRequest {
    /// Replica ID (-1 for consumers, broker ID for followers).
    pub replica_id: i32,
    /// Topics.
    pub topics: Vec<OffsetForLeaderEpochTopic>,
}

impl OffsetForLeaderEpochRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::OffsetForLeaderEpoch
    }

    /// Encode for version 2 (adds current_leader_epoch per partition for fencing).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.topic).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.current_leader_epoch.encode(buf);
                partition.leader_epoch.encode(buf);
            }
        }
        Ok(())
    }

    /// Encode for version 3 (adds replica_id field).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        self.replica_id.encode(buf);
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.topic).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.current_leader_epoch.encode(buf);
                partition.leader_epoch.encode(buf);
            }
        }
        Ok(())
    }

    /// Encode for version 4 (flexible: compact strings/arrays + tagged fields).
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        self.replica_id.encode(buf);
        let topics_len = u32::try_from(self.topics.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("topics array too large"))?;
        crate::util::varint::encode_unsigned_varint(topics_len, buf);
        for topic in &self.topics {
            KafkaString::new(&topic.topic).try_encode_compact(buf)?;
            let parts_len = u32::try_from(topic.partitions.len().saturating_add(1))
                .map_err(|_| KrafkaError::protocol("partitions array too large"))?;
            crate::util::varint::encode_unsigned_varint(parts_len, buf);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.current_leader_epoch.encode(buf);
                partition.leader_epoch.encode(buf);
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Partition in OffsetForLeaderEpoch response.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochPartitionResult {
    /// Error code.
    pub error_code: ErrorCode,
    /// Partition index.
    pub partition: i32,
    /// Leader epoch.
    pub leader_epoch: i32,
    /// End offset for the requested leader epoch.
    pub end_offset: i64,
}

/// Topic in OffsetForLeaderEpoch response.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochTopicResult {
    /// Topic name.
    pub topic: String,
    /// Partitions.
    pub partitions: Vec<OffsetForLeaderEpochPartitionResult>,
}

/// OffsetForLeaderEpoch response.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochResponse {
    /// Throttle time (v2+).
    pub throttle_time_ms: i32,
    /// Topics.
    pub topics: Vec<OffsetForLeaderEpochTopicResult>,
}

impl OffsetForLeaderEpochResponse {
    /// Decode from version 2-3 (non-flexible, adds throttle_time_ms header).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let topic = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let partition = i32::decode(buf)?;
                let leader_epoch = i32::decode(buf)?;
                let end_offset = i64::decode(buf)?;
                partitions.push(OffsetForLeaderEpochPartitionResult {
                    error_code,
                    partition,
                    leader_epoch,
                    end_offset,
                });
            }

            topics.push(OffsetForLeaderEpochTopicResult { topic, partitions });
        }

        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }

    /// Decode from version 4 (flexible: compact strings/arrays + tagged fields).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let topic = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let partition_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let partition = i32::decode(buf)?;
                let leader_epoch = i32::decode(buf)?;
                let end_offset = i64::decode(buf)?;
                let _ = TaggedFields::decode(buf)?;
                partitions.push(OffsetForLeaderEpochPartitionResult {
                    error_code,
                    partition,
                    leader_epoch,
                    end_offset,
                });
            }
            let _ = TaggedFields::decode(buf)?;
            topics.push(OffsetForLeaderEpochTopicResult { topic, partitions });
        }
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            topics,
        })
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

impl VersionedEncode for OffsetForLeaderEpochRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            2 => self.encode_v2(buf)?,
            3 => self.encode_v3(buf)?,
            4 => self.encode_v4(buf)?,
            _ => return unsupported_encode!("OffsetForLeaderEpochRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for OffsetForLeaderEpochResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            2..=3 => Self::decode_v2(buf),
            4 => Self::decode_v4(buf),
            _ => unsupported_decode!("OffsetForLeaderEpochResponse", version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::*;
    use crate::util::varint;
    use bytes::BytesMut;
    use rstest::rstest;

    // ===================================================================
    // Story 1.2: OffsetCommitRequest/Response Wire-Format Tests
    // ===================================================================

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

    // ===================================================================
    // Story 1.3: OffsetFetchRequest/Response Wire-Format Tests
    // ===================================================================

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

    #[test]
    fn test_offset_for_leader_epoch_request() {
        let request = OffsetForLeaderEpochRequest {
            replica_id: -1,
            topics: vec![OffsetForLeaderEpochTopic {
                topic: "my-topic".to_string(),
                partitions: vec![OffsetForLeaderEpochPartition {
                    partition: 0,
                    current_leader_epoch: 5,
                    leader_epoch: 4,
                }],
            }],
        };
        assert_eq!(
            OffsetForLeaderEpochRequest::api_key(),
            ApiKey::OffsetForLeaderEpoch
        );

        let mut buf = BytesMut::new();
        request.encode_v2(&mut buf).unwrap();
        assert!(!buf.is_empty());

        let mut buf3 = BytesMut::new();
        request.encode_v3(&mut buf3).unwrap();
        // v3 includes replica_id prefix, so it should be longer
        assert!(buf3.len() > buf.len());
    }

    #[test]
    fn test_offset_for_leader_epoch_response_decode_v2() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        // throttle_time_ms (v2 adds this)
        buf.put_i32(50);
        // topic_count = 1
        buf.put_i32(1);
        // topic name
        let topic = b"my-topic";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        // partition_count = 1
        buf.put_i32(1);
        // error_code
        buf.put_i16(0);
        // partition
        buf.put_i32(0);
        // leader_epoch
        buf.put_i32(5);
        // end_offset
        buf.put_i64(1000);

        let mut data = buf.freeze();
        let resp = OffsetForLeaderEpochResponse::decode_v2(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 50);
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].topic, "my-topic");
        assert_eq!(resp.topics[0].partitions.len(), 1);
        assert!(resp.topics[0].partitions[0].error_code.is_ok());
        assert_eq!(resp.topics[0].partitions[0].partition, 0);
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, 5);
        assert_eq!(resp.topics[0].partitions[0].end_offset, 1000);
    }

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
    // OffsetForLeaderEpoch MIN=2
    #[case::ofl_v0(0)]
    #[case::ofl_v1(1)]
    fn test_offset_for_leader_epoch_encode_below_min(#[case] version: i16) {
        let request = OffsetForLeaderEpochRequest {
            replica_id: -1,
            topics: vec![],
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }

    // ── OffsetForLeaderEpoch v4 flexible round-trip ───────────────────

    #[test]
    fn test_offset_for_leader_epoch_request_encode_v4_flexible() {
        let request = OffsetForLeaderEpochRequest {
            replica_id: -1,
            topics: vec![OffsetForLeaderEpochTopic {
                topic: "my-topic".to_string(),
                partitions: vec![OffsetForLeaderEpochPartition {
                    partition: 0,
                    current_leader_epoch: 5,
                    leader_epoch: 3,
                }],
            }],
        };

        let mut buf_v4 = BytesMut::new();
        request.encode_v4(&mut buf_v4).unwrap();
        assert!(!buf_v4.is_empty());

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();
        assert_ne!(buf_v4.as_ref(), buf_v3.as_ref());
    }

    #[test]
    fn test_offset_for_leader_epoch_response_decode_v4_flexible() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(20);
        // topics array (compact: len+1 varint) = 1 topic → varint(2)
        buf.put_u8(2);
        // topic name (compact string)
        let topic = b"my-topic";
        buf.put_u8((topic.len() + 1) as u8);
        buf.put_slice(topic);
        // partitions array (compact) = 1 partition → varint(2)
        buf.put_u8(2);
        // error_code
        buf.put_i16(0);
        // partition
        buf.put_i32(0);
        // leader_epoch
        buf.put_i32(5);
        // end_offset
        buf.put_i64(1000);
        // tagged fields (per partition)
        buf.put_u8(0);
        // tagged fields (per topic)
        buf.put_u8(0);
        // tagged fields (top-level)
        buf.put_u8(0);

        let resp = OffsetForLeaderEpochResponse::decode_v4(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 20);
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].topic, "my-topic");
        assert_eq!(resp.topics[0].partitions.len(), 1);
        assert!(resp.topics[0].partitions[0].error_code.is_ok());
        assert_eq!(resp.topics[0].partitions[0].partition, 0);
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, 5);
        assert_eq!(resp.topics[0].partitions[0].end_offset, 1000);
    }

    #[test]
    fn test_offset_for_leader_epoch_v4_dispatch() {
        let request = OffsetForLeaderEpochRequest {
            replica_id: -1,
            topics: vec![],
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(4, &mut buf).unwrap();
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
