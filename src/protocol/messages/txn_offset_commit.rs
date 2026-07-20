use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, decode_capacity,
    encode_compact_array_len,
};

/// TxnOffsetCommit request (API Key 28).
///
/// Commits offsets as part of a transaction.
#[derive(Debug, Clone)]
pub struct TxnOffsetCommitRequest {
    /// Transactional ID.
    pub transactional_id: String,
    /// Consumer group ID.
    pub group_id: String,
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Generation ID of the consumer (v3+, default -1).
    pub generation_id: i32,
    /// Member ID assigned by the group coordinator (v3+, default "").
    pub member_id: String,
    /// Unique consumer instance ID provided by the end user (v3+, default None).
    pub group_instance_id: Option<String>,
    /// Offsets to commit by topic.
    pub topics: Vec<TxnOffsetCommitTopic>,
}

/// Topic in TxnOffsetCommit request.
#[derive(Debug, Clone)]
pub struct TxnOffsetCommitTopic {
    /// Topic name.
    pub name: String,
    /// Partitions with offsets.
    pub partitions: Vec<TxnOffsetCommitPartition>,
}

/// Partition offset in TxnOffsetCommit request.
#[derive(Debug, Clone)]
pub struct TxnOffsetCommitPartition {
    /// Partition index.
    pub partition: i32,
    /// Offset to commit.
    pub committed_offset: i64,
    /// Leader epoch (optional, -1 if not used).
    pub committed_leader_epoch: i32,
    /// Metadata.
    pub metadata: Option<String>,
}

impl TxnOffsetCommitRequest {
    /// Create a new request.
    pub fn new(
        transactional_id: impl Into<String>,
        group_id: impl Into<String>,
        producer_id: i64,
        producer_epoch: i16,
    ) -> Self {
        Self {
            transactional_id: transactional_id.into(),
            group_id: group_id.into(),
            producer_id,
            producer_epoch,
            generation_id: -1,
            member_id: String::new(),
            group_instance_id: None,
            topics: Vec::new(),
        }
    }

    /// Add an offset to commit.
    pub fn add_offset(
        mut self,
        topic: impl Into<String>,
        partition: i32,
        offset: i64,
        metadata: Option<String>,
    ) -> Self {
        let topic_name = topic.into();
        let partition_data = TxnOffsetCommitPartition {
            partition,
            committed_offset: offset,
            committed_leader_epoch: -1,
            metadata,
        };

        if let Some(t) = self.topics.iter_mut().find(|t| t.name == topic_name) {
            if let Some(p) = t.partitions.iter_mut().find(|p| p.partition == partition) {
                *p = partition_data;
            } else {
                t.partitions.push(partition_data);
            }
        } else {
            self.topics.push(TxnOffsetCommitTopic {
                name: topic_name,
                partitions: vec![partition_data],
            });
        }
        self
    }

    /// Encode as version 0–1.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode(buf)?;
        KafkaString(Some(self.group_id.clone())).try_encode(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        array_len_i32(self.topics.len())?.encode(buf);
        for topic in &self.topics {
            KafkaString(Some(topic.name.clone())).try_encode(buf)?;
            array_len_i32(topic.partitions.len())?.encode(buf);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.committed_offset.encode(buf);
                KafkaString(partition.metadata.clone()).try_encode(buf)?;
            }
        }
        Ok(())
    }

    /// Encode as version 2 (adds committed leader epoch).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode(buf)?;
        KafkaString(Some(self.group_id.clone())).try_encode(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        array_len_i32(self.topics.len())?.encode(buf);
        for topic in &self.topics {
            KafkaString(Some(topic.name.clone())).try_encode(buf)?;
            array_len_i32(topic.partitions.len())?.encode(buf);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.committed_offset.encode(buf);
                partition.committed_leader_epoch.encode(buf);
                KafkaString(partition.metadata.clone()).try_encode(buf)?;
            }
        }
        Ok(())
    }

    /// Encode as version 3–5 (flexible, adds generation_id, member_id, group_instance_id).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode_compact(buf)?;
        KafkaString(Some(self.group_id.clone())).try_encode_compact(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        self.generation_id.encode(buf);
        KafkaString(Some(self.member_id.clone())).try_encode_compact(buf)?;
        KafkaString(self.group_instance_id.clone()).try_encode_compact(buf)?;
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            KafkaString(Some(topic.name.clone())).try_encode_compact(buf)?;
            encode_compact_array_len(topic.partitions.len(), buf)?;
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.committed_offset.encode(buf);
                partition.committed_leader_epoch.encode(buf);
                KafkaString(partition.metadata.clone()).try_encode_compact(buf)?;
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// TxnOffsetCommit response.
#[derive(Debug, Clone)]
pub struct TxnOffsetCommitResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Results by topic.
    pub topics: Vec<TxnOffsetCommitTopicResult>,
}

/// Result for a topic in TxnOffsetCommit.
#[derive(Debug, Clone)]
pub struct TxnOffsetCommitTopicResult {
    /// Topic name.
    pub name: String,
    /// Partition results.
    pub partitions: Vec<TxnOffsetCommitPartitionResult>,
}

/// Result for a partition in TxnOffsetCommit.
#[derive(Debug, Clone)]
pub struct TxnOffsetCommitPartitionResult {
    /// Partition index.
    pub partition: i32,
    /// Error code.
    pub error_code: ErrorCode,
}

impl TxnOffsetCommitResponse {
    /// Decode from version 0–2.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions =
                Vec::with_capacity(decode_capacity(partition_count, buf.remaining()));

            for _ in 0..partition_count {
                let partition = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                partitions.push(TxnOffsetCommitPartitionResult {
                    partition,
                    error_code,
                });
            }

            topics.push(TxnOffsetCommitTopicResult { name, partitions });
        }

        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }

    /// Decode from version 3–5 (flexible: compact strings, varint arrays, tagged fields).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut topics = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let partition_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions =
                Vec::with_capacity(decode_capacity(partition_count, buf.remaining()));

            for _ in 0..partition_count {
                let partition = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let _ = TaggedFields::decode(buf)?;
                partitions.push(TxnOffsetCommitPartitionResult {
                    partition,
                    error_code,
                });
            }

            let _ = TaggedFields::decode(buf)?;
            topics.push(TxnOffsetCommitTopicResult { name, partitions });
        }

        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }

    /// Check if all offsets were committed successfully.
    pub fn is_ok(&self) -> bool {
        self.topics
            .iter()
            .all(|t| t.partitions.iter().all(|p| p.error_code.is_ok()))
    }
}

impl VersionedEncode for TxnOffsetCommitRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=1 => self.encode_v0(buf)?,
            2 => self.encode_v2(buf)?,
            3..=5 => self.encode_v3(buf)?,
            _ => return unsupported_encode!("TxnOffsetCommitRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for TxnOffsetCommitResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=2 => Self::decode_v0(buf),
            3..=5 => Self::decode_v3(buf),
            _ => unsupported_decode!("TxnOffsetCommitResponse", version),
        }
    }
}
