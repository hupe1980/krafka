use bytes::{Buf, BufMut, Bytes, BytesMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{
    Decode, Encode, KafkaBytes, KafkaString, TaggedField, TaggedFields, TryEncode,
};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_compact_nullable_array_len,
    check_decode_array_len, check_decode_nullable_array_len,
};

/// This struct is `#[non_exhaustive]`; use [`Default::default()`] and then
/// set the fields you need.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FetchRequest {
    /// Replica ID (-1 for consumers).
    pub replica_id: i32,
    /// Max wait time in milliseconds.
    pub max_wait_ms: i32,
    /// Min bytes to return.
    pub min_bytes: i32,
    /// Max bytes to return (v3+).
    pub max_bytes: i32,
    /// Isolation level (v4+).
    pub isolation_level: i8,
    /// Session ID (v7+).
    pub session_id: i32,
    /// Session epoch (v7+).
    pub session_epoch: i32,
    /// Topic data.
    pub topics: Vec<FetchTopicRequest>,
    /// Forgotten topics/partitions to remove from the session (v7+).
    pub forgotten_topics: Vec<FetchForgottenTopic>,
    /// Consumer rack ID for closest-replica routing (v11+, KIP-392).
    pub rack_id: String,
}

impl Default for FetchRequest {
    fn default() -> Self {
        Self {
            replica_id: -1,
            max_wait_ms: 0,
            min_bytes: 0,
            max_bytes: 0,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: Vec::new(),
            forgotten_topics: Vec::new(),
            rack_id: String::new(),
        }
    }
}

/// Topic-partitions to forget from a fetch session (v7+).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct FetchForgottenTopic {
    /// Topic name (v7–v12).
    pub topic: String,
    /// Topic ID (v13+, KIP-516). Replaces `topic` when set.
    pub topic_id: Option<[u8; 16]>,
    /// Partition IDs to forget.
    pub partitions: Vec<i32>,
}

/// Topic in fetch request.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct FetchTopicRequest {
    /// Topic name (v4–v12).
    pub topic: String,
    /// Topic ID (v13+, KIP-516). Replaces `topic` when set.
    pub topic_id: Option<[u8; 16]>,
    /// Partition data.
    pub partitions: Vec<FetchPartitionRequest>,
}

/// Partition in fetch request.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FetchPartitionRequest {
    /// Partition ID.
    pub partition: i32,
    /// Current leader epoch (v9+).
    pub current_leader_epoch: i32,
    /// Fetch offset.
    pub fetch_offset: i64,
    /// Last fetched epoch (v12+).
    pub last_fetched_epoch: i32,
    /// Log start offset (v5+).
    pub log_start_offset: i64,
    /// Partition max bytes.
    pub partition_max_bytes: i32,
    /// Directory ID of the follower fetching (v17+, KIP-853). Tagged field tag 0.
    pub replica_directory_id: Option<[u8; 16]>,
    /// High-watermark known by the replica (v18+, KIP-1166). Tagged field tag 1.
    pub high_watermark: Option<i64>,
}

impl Default for FetchPartitionRequest {
    fn default() -> Self {
        Self {
            partition: 0,
            current_leader_epoch: -1,
            fetch_offset: 0,
            last_fetched_epoch: -1,
            log_start_offset: -1,
            partition_max_bytes: 0,
            replica_directory_id: None,
            high_watermark: None,
        }
    }
}

impl FetchRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::Fetch
    }

    /// Encode for version 4.
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode_inner_v4(buf, false)
    }

    /// Encode for version 5–6 (v4 + log_start_offset per partition).
    pub fn encode_v5(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode_inner_v4(buf, true)
    }

    /// Shared encoder for v4–v6. When `include_log_start_offset` is true,
    /// emits `log_start_offset` per partition (v5+).
    fn encode_inner_v4(&self, buf: &mut impl BufMut, include_log_start_offset: bool) -> Result<()> {
        self.replica_id.encode(buf);
        self.max_wait_ms.encode(buf);
        self.min_bytes.encode(buf);
        self.max_bytes.encode(buf);
        self.isolation_level.encode(buf);
        self.encode_topics_inner(buf, include_log_start_offset)
    }

    /// Shared topics array encoder. When `include_log_start_offset` is true,
    /// emits `log_start_offset` per partition (v5+).
    fn encode_topics_inner(
        &self,
        buf: &mut impl BufMut,
        include_log_start_offset: bool,
    ) -> Result<()> {
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.topic).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.fetch_offset.encode(buf);
                if include_log_start_offset {
                    partition.log_start_offset.encode(buf);
                }
                partition.partition_max_bytes.encode(buf);
            }
        }
        Ok(())
    }

    /// Encode for version 7 (fetch sessions: session_id, session_epoch, forgotten_topics).
    pub fn encode_v7(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode_inner_v7(buf, false)
    }

    /// Encode for version 9 (v7 + `current_leader_epoch` per partition, KIP-320).
    pub fn encode_v9(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode_inner_v7(buf, true)
    }

    /// Shared encoder for v7–v10. When `include_leader_epoch` is true, emits
    /// `current_leader_epoch` between partition id and fetch_offset (v9+, KIP-320).
    fn encode_inner_v7(&self, buf: &mut impl BufMut, include_leader_epoch: bool) -> Result<()> {
        self.replica_id.encode(buf);
        self.max_wait_ms.encode(buf);
        self.min_bytes.encode(buf);
        self.max_bytes.encode(buf);
        self.isolation_level.encode(buf);
        self.session_id.encode(buf);
        self.session_epoch.encode(buf);

        // Topics array
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.topic).try_encode(buf)?;

            // Partitions array
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                if include_leader_epoch {
                    // current_leader_epoch introduced in v9 (KIP-320)
                    partition.current_leader_epoch.encode(buf);
                }
                partition.fetch_offset.encode(buf);
                // log_start_offset introduced in v5
                partition.log_start_offset.encode(buf);
                partition.partition_max_bytes.encode(buf);
            }
        }

        // Forgotten topics array (v7+)
        buf.put_i32(array_len_i32(self.forgotten_topics.len())?);
        for forgotten in &self.forgotten_topics {
            KafkaString::new(&forgotten.topic).try_encode(buf)?;
            buf.put_i32(array_len_i32(forgotten.partitions.len())?);
            for &partition in &forgotten.partitions {
                partition.encode(buf);
            }
        }
        Ok(())
    }

    /// Encode for version 11 (v9 + rack_id for closest-replica routing, KIP-392).
    pub fn encode_v11(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode_v9(buf)?;
        KafkaString::new(&self.rack_id).try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 12 (flexible: compact strings/arrays + tagged fields + last_fetched_epoch).
    pub fn encode_v12(&self, buf: &mut impl BufMut) -> Result<()> {
        self.replica_id.encode(buf);
        self.max_wait_ms.encode(buf);
        self.min_bytes.encode(buf);
        self.max_bytes.encode(buf);
        self.isolation_level.encode(buf);
        self.session_id.encode(buf);
        self.session_epoch.encode(buf);

        // Topics compact array
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
                partition.fetch_offset.encode(buf);
                partition.last_fetched_epoch.encode(buf);
                partition.log_start_offset.encode(buf);
                partition.partition_max_bytes.encode(buf);
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }

        // Forgotten topics compact array
        let forgotten_len = u32::try_from(self.forgotten_topics.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("forgotten topics array too large"))?;
        crate::util::varint::encode_unsigned_varint(forgotten_len, buf);
        for forgotten in &self.forgotten_topics {
            KafkaString::new(&forgotten.topic).try_encode_compact(buf)?;
            let fp_len = u32::try_from(forgotten.partitions.len().saturating_add(1))
                .map_err(|_| KrafkaError::protocol("forgotten partitions array too large"))?;
            crate::util::varint::encode_unsigned_varint(fp_len, buf);
            for &partition in &forgotten.partitions {
                partition.encode(buf);
            }
            TaggedFields::default().try_encode(buf)?;
        }

        KafkaString::new(&self.rack_id).try_encode_compact(buf)?;
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 13–14 (topic ID replaces topic name, KIP-516).
    pub fn encode_v13(&self, buf: &mut impl BufMut) -> Result<()> {
        self.replica_id.encode(buf);
        self.max_wait_ms.encode(buf);
        self.min_bytes.encode(buf);
        self.max_bytes.encode(buf);
        self.isolation_level.encode(buf);
        self.session_id.encode(buf);
        self.session_epoch.encode(buf);

        // Topics compact array — topic_id (uuid) instead of topic name
        let topics_len = u32::try_from(self.topics.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("topics array too large"))?;
        crate::util::varint::encode_unsigned_varint(topics_len, buf);
        for topic in &self.topics {
            buf.put_slice(&topic.topic_id.unwrap_or([0u8; 16]));
            let parts_len = u32::try_from(topic.partitions.len().saturating_add(1))
                .map_err(|_| KrafkaError::protocol("partitions array too large"))?;
            crate::util::varint::encode_unsigned_varint(parts_len, buf);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.current_leader_epoch.encode(buf);
                partition.fetch_offset.encode(buf);
                partition.last_fetched_epoch.encode(buf);
                partition.log_start_offset.encode(buf);
                partition.partition_max_bytes.encode(buf);
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }

        // Forgotten topics compact array — topic_id (uuid) instead of topic name
        let forgotten_len = u32::try_from(self.forgotten_topics.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("forgotten topics array too large"))?;
        crate::util::varint::encode_unsigned_varint(forgotten_len, buf);
        for forgotten in &self.forgotten_topics {
            buf.put_slice(&forgotten.topic_id.unwrap_or([0u8; 16]));
            let fp_len = u32::try_from(forgotten.partitions.len().saturating_add(1))
                .map_err(|_| KrafkaError::protocol("forgotten partitions array too large"))?;
            crate::util::varint::encode_unsigned_varint(fp_len, buf);
            for &partition in &forgotten.partitions {
                partition.encode(buf);
            }
            TaggedFields::default().try_encode(buf)?;
        }

        KafkaString::new(&self.rack_id).try_encode_compact(buf)?;
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 15–16 (ReplicaId removed; ReplicaState is tagged, KIP-903).
    pub fn encode_v15(&self, buf: &mut impl BufMut) -> Result<()> {
        // v15+ removes ReplicaId from the wire format.
        // ReplicaState is a tagged field (tag 1) with defaults -1/-1,
        // so consumers can omit it via empty tagged fields.
        self.encode_v15_inner(buf, false, false)
    }

    /// Encode for version 17 (KIP-853: ReplicaDirectoryId tagged field per partition).
    pub fn encode_v17(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode_v15_inner(buf, true, false)
    }

    /// Encode for version 18 (KIP-1166: HighWatermark tagged field per partition).
    pub fn encode_v18(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode_v15_inner(buf, true, true)
    }

    /// Shared encoder for v17–v18. Emits per-partition tagged fields:
    /// - `replica_directory_id` (tag 0, v17+, KIP-853)
    /// - `high_watermark` (tag 1, v18+, KIP-1166)
    fn encode_v15_inner(
        &self,
        buf: &mut impl BufMut,
        include_directory_id: bool,
        include_high_watermark: bool,
    ) -> Result<()> {
        self.max_wait_ms.encode(buf);
        self.min_bytes.encode(buf);
        self.max_bytes.encode(buf);
        self.isolation_level.encode(buf);
        self.session_id.encode(buf);
        self.session_epoch.encode(buf);

        // Topics compact array — topic_id (uuid)
        let topics_len = u32::try_from(self.topics.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("topics array too large"))?;
        crate::util::varint::encode_unsigned_varint(topics_len, buf);
        for topic in &self.topics {
            buf.put_slice(&topic.topic_id.unwrap_or([0u8; 16]));
            let parts_len = u32::try_from(topic.partitions.len().saturating_add(1))
                .map_err(|_| KrafkaError::protocol("partitions array too large"))?;
            crate::util::varint::encode_unsigned_varint(parts_len, buf);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.current_leader_epoch.encode(buf);
                partition.fetch_offset.encode(buf);
                partition.last_fetched_epoch.encode(buf);
                partition.log_start_offset.encode(buf);
                partition.partition_max_bytes.encode(buf);
                // Per-partition tagged fields
                let mut partition_tags = Vec::new();
                if include_directory_id && let Some(dir_id) = partition.replica_directory_id {
                    let mut tag_buf = BytesMut::with_capacity(16);
                    tag_buf.put_slice(&dir_id);
                    partition_tags.push(TaggedField {
                        tag: 0,
                        data: tag_buf.freeze(),
                    });
                }
                if include_high_watermark && let Some(hwm) = partition.high_watermark {
                    let mut tag_buf = BytesMut::with_capacity(8);
                    tag_buf.put_i64(hwm);
                    partition_tags.push(TaggedField {
                        tag: 1,
                        data: tag_buf.freeze(),
                    });
                }
                TaggedFields(partition_tags).try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }

        // Forgotten topics compact array
        let forgotten_len = u32::try_from(self.forgotten_topics.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("forgotten topics array too large"))?;
        crate::util::varint::encode_unsigned_varint(forgotten_len, buf);
        for forgotten in &self.forgotten_topics {
            buf.put_slice(&forgotten.topic_id.unwrap_or([0u8; 16]));
            let fp_len = u32::try_from(forgotten.partitions.len().saturating_add(1))
                .map_err(|_| KrafkaError::protocol("forgotten partitions array too large"))?;
            crate::util::varint::encode_unsigned_varint(fp_len, buf);
            for &partition in &forgotten.partitions {
                partition.encode(buf);
            }
            TaggedFields::default().try_encode(buf)?;
        }

        KafkaString::new(&self.rack_id).try_encode_compact(buf)?;
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Fetch response.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct FetchResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Error code (v7+).
    pub error_code: ErrorCode,
    /// Session ID (v7+).
    pub session_id: i32,
    /// Topic responses.
    pub responses: Vec<FetchTopicResponse>,
}

/// Topic in fetch response.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct FetchTopicResponse {
    /// Topic name (v4–v12).
    pub topic: String,
    /// Topic ID (v13+, KIP-516). Replaces `topic` when set.
    pub topic_id: Option<[u8; 16]>,
    /// Partition responses.
    pub partitions: Vec<FetchPartitionResponse>,
}

/// Partition in fetch response.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FetchPartitionResponse {
    /// Partition ID.
    pub partition: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// High watermark.
    pub high_watermark: i64,
    /// Last stable offset (v4+).
    pub last_stable_offset: i64,
    /// Log start offset (v5+).
    pub log_start_offset: i64,
    /// Aborted transactions (v4+).
    pub aborted_transactions: Vec<AbortedTransaction>,
    /// Preferred read replica ID for closest-replica routing (v11+, KIP-392).
    ///
    /// When >= 0, the consumer should preferentially fetch from this replica
    /// (reduces cross-rack traffic). When -1, no preference; use the leader.
    /// v7-v10 responses always set this to -1.
    pub preferred_read_replica: i32,
    /// Record batches.
    pub records: Option<Bytes>,
}

impl Default for FetchPartitionResponse {
    fn default() -> Self {
        Self {
            partition: 0,
            error_code: ErrorCode::None,
            high_watermark: -1,
            last_stable_offset: -1,
            log_start_offset: -1,
            aborted_transactions: Vec::new(),
            preferred_read_replica: -1,
            records: None,
        }
    }
}

/// Aborted transaction info.
#[derive(Debug, Clone)]
pub struct AbortedTransaction {
    /// Producer ID.
    pub producer_id: i64,
    /// First offset.
    pub first_offset: i64,
}

impl FetchResponse {
    /// Decode from version 4 (includes last_stable_offset and aborted_transactions).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_inner_v4(buf, false)
    }

    /// Decode from version 5–6 (v4 + log_start_offset per partition).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_inner_v4(buf, true)
    }

    /// Shared decoder for v4–v6. When `include_log_start_offset` is true,
    /// reads `log_start_offset` per partition (v5+); otherwise defaults to `-1`.
    fn decode_inner_v4(buf: &mut impl Buf, include_log_start_offset: bool) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut responses = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let topic = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let partition = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let high_watermark = i64::decode(buf)?;
                let last_stable_offset = i64::decode(buf)?;
                let log_start_offset = if include_log_start_offset {
                    i64::decode(buf)?
                } else {
                    -1
                };
                let aborted_tx_count = check_decode_nullable_array_len(i32::decode(buf)?)?;
                let mut aborted_transactions = Vec::with_capacity(aborted_tx_count);
                for _ in 0..aborted_tx_count {
                    aborted_transactions.push(AbortedTransaction {
                        producer_id: i64::decode(buf)?,
                        first_offset: i64::decode(buf)?,
                    });
                }
                let records = KafkaBytes::decode(buf)?.0;

                partitions.push(FetchPartitionResponse {
                    partition,
                    error_code,
                    high_watermark,
                    last_stable_offset,
                    log_start_offset,
                    aborted_transactions,
                    preferred_read_replica: -1,
                    records,
                });
            }

            responses.push(FetchTopicResponse {
                topic,
                topic_id: None,
                partitions,
            });
        }

        Ok(Self {
            throttle_time_ms,
            error_code: ErrorCode::None,
            session_id: 0,
            responses,
        })
    }

    /// Decode from version 7 (includes error_code, session_id, log_start_offset).
    pub fn decode_v7(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_inner_v7(buf, false)
    }

    /// Decode from version 11 (v7 + preferred_read_replica per partition, KIP-392).
    pub fn decode_v11(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_inner_v7(buf, true)
    }

    /// Shared decoder for v7–v11. When `has_preferred_read_replica` is true,
    /// reads `preferred_read_replica` between aborted_transactions and records
    /// (v11+, KIP-392); otherwise defaults to `-1`.
    fn decode_inner_v7(buf: &mut impl Buf, has_preferred_read_replica: bool) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let session_id = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut responses = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let topic = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let partition = i32::decode(buf)?;
                let partition_error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let high_watermark = i64::decode(buf)?;
                let last_stable_offset = i64::decode(buf)?;
                let log_start_offset = i64::decode(buf)?;
                let aborted_tx_count = check_decode_nullable_array_len(i32::decode(buf)?)?;
                let mut aborted_transactions = Vec::with_capacity(aborted_tx_count);
                for _ in 0..aborted_tx_count {
                    aborted_transactions.push(AbortedTransaction {
                        producer_id: i64::decode(buf)?,
                        first_offset: i64::decode(buf)?,
                    });
                }
                let preferred_read_replica = if has_preferred_read_replica {
                    i32::decode(buf)?
                } else {
                    -1
                };
                let records = KafkaBytes::decode(buf)?.0;

                partitions.push(FetchPartitionResponse {
                    partition,
                    error_code: partition_error_code,
                    high_watermark,
                    last_stable_offset,
                    log_start_offset,
                    aborted_transactions,
                    preferred_read_replica,
                    records,
                });
            }

            responses.push(FetchTopicResponse {
                topic,
                topic_id: None,
                partitions,
            });
        }

        Ok(Self {
            throttle_time_ms,
            error_code,
            session_id,
            responses,
        })
    }

    /// Decode from version 12 (flexible: compact strings/arrays + tagged fields).
    pub fn decode_v12(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let session_id = i32::decode(buf)?;

        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut responses = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let topic = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let part_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(part_count);

            for _ in 0..part_count {
                let partition = i32::decode(buf)?;
                let partition_error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let high_watermark = i64::decode(buf)?;
                let last_stable_offset = i64::decode(buf)?;
                let log_start_offset = i64::decode(buf)?;
                // Aborted transactions: compact nullable array
                let aborted_tx_count = check_compact_nullable_array_len(
                    crate::util::varint::decode_unsigned_varint(buf)?,
                )?;
                let mut aborted_transactions = Vec::with_capacity(aborted_tx_count);
                for _ in 0..aborted_tx_count {
                    aborted_transactions.push(AbortedTransaction {
                        producer_id: i64::decode(buf)?,
                        first_offset: i64::decode(buf)?,
                    });
                    let _ = TaggedFields::decode(buf)?;
                }
                let preferred_read_replica = i32::decode(buf)?;
                let records = KafkaBytes::decode_compact(buf)?.0;
                let _ = TaggedFields::decode(buf)?; // partition tagged fields

                partitions.push(FetchPartitionResponse {
                    partition,
                    error_code: partition_error_code,
                    high_watermark,
                    last_stable_offset,
                    log_start_offset,
                    aborted_transactions,
                    preferred_read_replica,
                    records,
                });
            }
            let _ = TaggedFields::decode(buf)?; // topic tagged fields
            responses.push(FetchTopicResponse {
                topic,
                topic_id: None,
                partitions,
            });
        }
        let _ = TaggedFields::decode(buf)?; // top-level tagged fields

        Ok(Self {
            throttle_time_ms,
            error_code,
            session_id,
            responses,
        })
    }

    /// Decode from version 13–16 (topic ID replaces topic name, KIP-516).
    ///
    /// v14 adds no new wire fields. v15 is the same on the response side.
    /// v16 adds `NodeEndpoints` as a top-level tagged field which is consumed
    /// generically by `TaggedFields::decode`.
    pub fn decode_v13(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let session_id = i32::decode(buf)?;

        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut responses = Vec::with_capacity(topic_count);

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
                let partition = i32::decode(buf)?;
                let partition_error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let high_watermark = i64::decode(buf)?;
                let last_stable_offset = i64::decode(buf)?;
                let log_start_offset = i64::decode(buf)?;
                // Aborted transactions: compact nullable array
                let aborted_tx_count = check_compact_nullable_array_len(
                    crate::util::varint::decode_unsigned_varint(buf)?,
                )?;
                let mut aborted_transactions = Vec::with_capacity(aborted_tx_count);
                for _ in 0..aborted_tx_count {
                    aborted_transactions.push(AbortedTransaction {
                        producer_id: i64::decode(buf)?,
                        first_offset: i64::decode(buf)?,
                    });
                    let _ = TaggedFields::decode(buf)?;
                }
                let preferred_read_replica = i32::decode(buf)?;
                let records = KafkaBytes::decode_compact(buf)?.0;
                let _ = TaggedFields::decode(buf)?; // partition tagged fields

                partitions.push(FetchPartitionResponse {
                    partition,
                    error_code: partition_error_code,
                    high_watermark,
                    last_stable_offset,
                    log_start_offset,
                    aborted_transactions,
                    preferred_read_replica,
                    records,
                });
            }
            let _ = TaggedFields::decode(buf)?; // topic tagged fields
            responses.push(FetchTopicResponse {
                topic: String::new(),
                topic_id: Some(topic_id),
                partitions,
            });
        }
        let _ = TaggedFields::decode(buf)?; // top-level tagged fields

        Ok(Self {
            throttle_time_ms,
            error_code,
            session_id,
            responses,
        })
    }
}

// FindCoordinator request/response

impl VersionedEncode for FetchRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            4 => self.encode_v4(buf)?,
            // v5-v6 add log_start_offset per partition
            5 | 6 => self.encode_v5(buf)?,
            // v7-v8 share the same wire format (fetch sessions, KIP-227)
            7 | 8 => self.encode_v7(buf)?,
            // v9 adds current_leader_epoch per partition (KIP-320)
            9 | 10 => self.encode_v9(buf)?,
            // v11 adds rack_id for closest-replica routing (KIP-392)
            11 => self.encode_v11(buf)?,
            // v12 flexible encoding (compact strings/arrays + tagged fields)
            12 => self.encode_v12(buf)?,
            // v13-v14 topic ID replaces topic name (KIP-516)
            13 | 14 => self.encode_v13(buf)?,
            // v15-v16 ReplicaId removed; ReplicaState is tagged (KIP-903/KIP-951)
            15 | 16 => self.encode_v15(buf)?,
            // v17 adds ReplicaDirectoryId per partition (KIP-853)
            17 => self.encode_v17(buf)?,
            // v18 adds HighWatermark per partition (KIP-1166)
            18 => self.encode_v18(buf)?,
            _ => return unsupported_encode!("FetchRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for FetchResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            4 => Self::decode_v4(buf),
            // v5-v6 add log_start_offset per partition
            5 | 6 => Self::decode_v5(buf),
            // v7-v10 share the same wire format
            7..=10 => Self::decode_v7(buf),
            // v11 adds preferred_read_replica per partition (KIP-392)
            11 => Self::decode_v11(buf),
            // v12 flexible encoding (compact strings/arrays + tagged fields)
            12 => Self::decode_v12(buf),
            // v13-v18 topic ID replaces topic name (KIP-516); tagged fields handle additions
            13..=18 => Self::decode_v13(buf),
            _ => unsupported_decode!("FetchResponse", version),
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

    #[test]
    fn test_fetch_request_encode_v7_includes_session_fields() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 42,
            session_epoch: 3,
            topics: vec![FetchTopicRequest {
                topic: "test-topic".to_string(),
                topic_id: None,
                partitions: vec![FetchPartitionRequest {
                    partition: 0,
                    current_leader_epoch: -1,
                    fetch_offset: 100,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: 1048576,
                    replica_directory_id: None,
                    high_watermark: None,
                }],
            }],
            forgotten_topics: vec![FetchForgottenTopic {
                topic: "old-topic".to_string(),
                topic_id: None,
                partitions: vec![1, 2],
            }],
            rack_id: String::new(),
        };

        let mut buf_v4 = BytesMut::new();
        request.encode_v4(&mut buf_v4).unwrap();

        let mut buf_v7 = BytesMut::new();
        request.encode_v7(&mut buf_v7).unwrap();

        // v7 adds session_id(4) + session_epoch(4) + log_start_offset per partition(8)
        // + forgotten_topics array (4 + topic string + partitions)
        assert!(buf_v7.len() > buf_v4.len());

        // Verify session_id and session_epoch at expected offsets:
        // replica_id(4) + max_wait_ms(4) + min_bytes(4) + max_bytes(4) + isolation_level(1) = 17
        let session_id_bytes = &buf_v7[17..21];
        assert_eq!(i32::from_be_bytes(session_id_bytes.try_into().unwrap()), 42);
        let session_epoch_bytes = &buf_v7[21..25];
        assert_eq!(
            i32::from_be_bytes(session_epoch_bytes.try_into().unwrap()),
            3
        );
    }

    #[test]
    fn test_fetch_request_encode_v7_empty_forgotten_topics() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };

        let mut buf = BytesMut::new();
        request.encode_v7(&mut buf).unwrap();

        // Should still encode: header fields + empty topics array(4) + empty forgotten array(4)
        // replica_id(4) + max_wait_ms(4) + min_bytes(4) + max_bytes(4) + isolation_level(1)
        // + session_id(4) + session_epoch(4) + topics_count(4) + forgotten_count(4) = 33
        assert_eq!(buf.len(), 33);
    }

    #[test]
    fn test_fetch_response_decode_v7_round_trip() {
        // Build a v7 response manually: throttle(4) + error_code(2) + session_id(4) + topics
        let mut raw = BytesMut::new();
        raw.put_i32(100); // throttle_time_ms
        raw.put_i16(0); // error_code (None)
        raw.put_i32(42); // session_id
        raw.put_i32(1); // 1 topic
        // topic name
        raw.put_i16(5);
        raw.put_slice(b"topic");
        raw.put_i32(1); // 1 partition
        raw.put_i32(0); // partition id
        raw.put_i16(0); // error_code
        raw.put_i64(1000); // high_watermark
        raw.put_i64(999); // last_stable_offset
        raw.put_i64(0); // log_start_offset
        raw.put_i32(0); // 0 aborted transactions
        raw.put_i32(-1); // records (null/-1 length)

        let mut buf = raw.freeze();
        let resp = FetchResponse::decode_v7(&mut buf).unwrap();

        assert_eq!(resp.throttle_time_ms, 100);
        assert_eq!(resp.error_code, ErrorCode::None);
        assert_eq!(resp.session_id, 42);
        assert_eq!(resp.responses.len(), 1);
        assert_eq!(resp.responses[0].topic, "topic");
        assert_eq!(resp.responses[0].partitions.len(), 1);
        assert_eq!(resp.responses[0].partitions[0].partition, 0);
        assert_eq!(resp.responses[0].partitions[0].high_watermark, 1000);
        assert_eq!(resp.responses[0].partitions[0].last_stable_offset, 999);
        assert_eq!(resp.responses[0].partitions[0].log_start_offset, 0);
    }

    #[test]
    fn test_fetch_response_decode_v7_session_error() {
        let mut raw = BytesMut::new();
        raw.put_i32(0); // throttle_time_ms
        raw.put_i16(70); // FetchSessionIdNotFound
        raw.put_i32(0); // session_id
        raw.put_i32(0); // 0 topics

        let mut buf = raw.freeze();
        let resp = FetchResponse::decode_v7(&mut buf).unwrap();

        assert_eq!(resp.error_code, ErrorCode::FetchSessionIdNotFound);
        assert_eq!(resp.session_id, 0);
        assert!(resp.responses.is_empty());
    }

    #[test]
    fn test_fetch_response_decode_v7_vs_v4_extra_fields() {
        // v4 response: no error_code, no session_id
        let mut raw_v4 = BytesMut::new();
        raw_v4.put_i32(50); // throttle_time_ms
        raw_v4.put_i32(0); // 0 topics

        let mut buf_v4 = raw_v4.freeze();
        let resp_v4 = FetchResponse::decode_v4(&mut buf_v4).unwrap();
        assert_eq!(resp_v4.throttle_time_ms, 50);
        assert_eq!(resp_v4.error_code, ErrorCode::None); // default
        assert_eq!(resp_v4.session_id, 0); // default

        // v7 response: has error_code + session_id
        let mut raw_v7 = BytesMut::new();
        raw_v7.put_i32(50); // throttle_time_ms
        raw_v7.put_i16(0); // error_code
        raw_v7.put_i32(99); // session_id
        raw_v7.put_i32(0); // 0 topics

        let mut buf_v7 = raw_v7.freeze();
        let resp_v7 = FetchResponse::decode_v7(&mut buf_v7).unwrap();
        assert_eq!(resp_v7.throttle_time_ms, 50);
        assert_eq!(resp_v7.session_id, 99);
    }

    #[test]
    fn test_fetch_request_encode_v5_includes_log_start_offset() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 1,
            session_id: 0,
            session_epoch: 0,
            topics: vec![FetchTopicRequest {
                topic: "t".to_string(),
                topic_id: None,
                partitions: vec![FetchPartitionRequest {
                    partition: 0,
                    current_leader_epoch: -1,
                    fetch_offset: 100,
                    last_fetched_epoch: -1,
                    log_start_offset: 42,
                    partition_max_bytes: 1048576,
                    replica_directory_id: None,
                    high_watermark: None,
                }],
            }],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };

        let mut buf_v4 = BytesMut::new();
        request.encode_v4(&mut buf_v4).unwrap();

        let mut buf_v5 = BytesMut::new();
        request.encode_v5(&mut buf_v5).unwrap();

        // v5 adds log_start_offset (8 bytes) per partition compared to v4
        assert_eq!(
            buf_v5.len(),
            buf_v4.len() + 8,
            "v5 should be 8 bytes longer than v4 (log_start_offset per partition)"
        );

        // Helper: skip the shared v4/v5 header + first partition's common
        // fields up to (and including) fetch_offset, returning bytes consumed.
        fn header_and_partition_prefix_len(buf: &[u8]) -> usize {
            let mut c: &[u8] = buf;
            i32::decode(&mut c).unwrap(); // replica_id
            i32::decode(&mut c).unwrap(); // max_wait_ms
            i32::decode(&mut c).unwrap(); // min_bytes
            i32::decode(&mut c).unwrap(); // max_bytes
            i8::decode(&mut c).unwrap(); // isolation_level
            i32::decode(&mut c).unwrap(); // topic_count
            KafkaString::decode(&mut c).unwrap(); // topic_name
            i32::decode(&mut c).unwrap(); // partition_count
            i32::decode(&mut c).unwrap(); // partition_id
            i64::decode(&mut c).unwrap(); // fetch_offset
            buf.len() - c.len()
        }

        // v5: log_start_offset sits between fetch_offset and partition_max_bytes
        let skip = header_and_partition_prefix_len(&buf_v5);
        let mut cursor: &[u8] = &buf_v5[skip..];
        let log_start_offset = i64::decode(&mut cursor).unwrap();
        let partition_max_bytes = i32::decode(&mut cursor).unwrap();
        assert_eq!(
            log_start_offset, 42,
            "v5 log_start_offset at expected position"
        );
        assert_eq!(partition_max_bytes, 1048576);

        // v4: no log_start_offset — partition_max_bytes follows fetch_offset directly
        let skip = header_and_partition_prefix_len(&buf_v4);
        let mut cursor_v4: &[u8] = &buf_v4[skip..];
        let v4_partition_max_bytes = i32::decode(&mut cursor_v4).unwrap();
        assert_eq!(
            v4_partition_max_bytes, 1048576,
            "v4 has no log_start_offset"
        );
        assert!(cursor_v4.is_empty(), "v4 buffer fully consumed");
    }

    #[test]
    fn test_fetch_request_encode_v6_same_as_v5() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![FetchTopicRequest {
                topic: "t".to_string(),
                topic_id: None,
                partitions: vec![FetchPartitionRequest {
                    partition: 0,
                    current_leader_epoch: -1,
                    fetch_offset: 100,
                    last_fetched_epoch: -1,
                    log_start_offset: 10,
                    partition_max_bytes: 1048576,
                    replica_directory_id: None,
                    high_watermark: None,
                }],
            }],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };

        let mut buf_v5 = BytesMut::new();
        request.encode_versioned(5, &mut buf_v5).unwrap();

        let mut buf_v6 = BytesMut::new();
        request.encode_versioned(6, &mut buf_v6).unwrap();

        assert_eq!(buf_v6, buf_v5, "v6 should produce same bytes as v5");
    }

    #[test]
    fn test_fetch_response_decode_v5_round_trip() {
        // Build a v5 response manually: throttle(4) + topics (no error_code/session_id)
        let mut raw = BytesMut::new();
        raw.put_i32(100); // throttle_time_ms
        raw.put_i32(1); // 1 topic
        raw.put_i16(5); // topic name length
        raw.put_slice(b"topic");
        raw.put_i32(1); // 1 partition
        raw.put_i32(0); // partition id
        raw.put_i16(0); // error_code
        raw.put_i64(1000); // high_watermark
        raw.put_i64(999); // last_stable_offset
        raw.put_i64(42); // log_start_offset (v5+)
        raw.put_i32(0); // 0 aborted transactions
        raw.put_i32(-1); // records (null)

        let mut buf = raw.freeze();
        let resp = FetchResponse::decode_v5(&mut buf).unwrap();

        assert_eq!(resp.throttle_time_ms, 100);
        assert_eq!(resp.error_code, ErrorCode::None);
        assert_eq!(resp.session_id, 0);
        assert_eq!(resp.responses.len(), 1);
        assert_eq!(resp.responses[0].topic, "topic");
        assert_eq!(resp.responses[0].partitions.len(), 1);

        let p = &resp.responses[0].partitions[0];
        assert_eq!(p.partition, 0);
        assert_eq!(p.high_watermark, 1000);
        assert_eq!(p.last_stable_offset, 999);
        assert_eq!(p.log_start_offset, 42);
        assert_eq!(p.preferred_read_replica, -1);
    }

    #[test]
    fn test_fetch_response_decode_v5_vs_v4_log_start_offset() {
        // v4 response: no log_start_offset per partition
        let mut raw_v4 = BytesMut::new();
        raw_v4.put_i32(50); // throttle_time_ms
        raw_v4.put_i32(1); // 1 topic
        raw_v4.put_i16(1); // topic name length
        raw_v4.put_slice(b"t");
        raw_v4.put_i32(1); // 1 partition
        raw_v4.put_i32(0); // partition id
        raw_v4.put_i16(0); // error_code
        raw_v4.put_i64(500); // high_watermark
        raw_v4.put_i64(499); // last_stable_offset
        raw_v4.put_i32(-1); // aborted transactions (null)
        raw_v4.put_i32(-1); // records (null)

        let mut buf_v4 = raw_v4.freeze();
        let resp_v4 = FetchResponse::decode_v4(&mut buf_v4).unwrap();
        assert_eq!(resp_v4.responses[0].partitions[0].log_start_offset, -1);

        // v5 response: includes log_start_offset
        let mut raw_v5 = BytesMut::new();
        raw_v5.put_i32(50); // throttle_time_ms
        raw_v5.put_i32(1); // 1 topic
        raw_v5.put_i16(1); // topic name length
        raw_v5.put_slice(b"t");
        raw_v5.put_i32(1); // 1 partition
        raw_v5.put_i32(0); // partition id
        raw_v5.put_i16(0); // error_code
        raw_v5.put_i64(500); // high_watermark
        raw_v5.put_i64(499); // last_stable_offset
        raw_v5.put_i64(10); // log_start_offset (v5+)
        raw_v5.put_i32(-1); // aborted transactions (null)
        raw_v5.put_i32(-1); // records (null)

        let mut buf_v5 = raw_v5.freeze();
        let resp_v5 = FetchResponse::decode_v5(&mut buf_v5).unwrap();
        assert_eq!(resp_v5.responses[0].partitions[0].log_start_offset, 10);
    }

    #[test]
    fn test_versioned_decode_fetch_v5_v6_dispatches_to_decode_v5() {
        let mut raw = BytesMut::new();
        raw.put_i32(0); // throttle_time_ms
        raw.put_i32(0); // 0 topics

        let data = raw.freeze();
        for version in 5..=6 {
            let resp = FetchResponse::decode_versioned(version, &mut data.clone()).unwrap();
            assert_eq!(resp.throttle_time_ms, 0);
            assert!(resp.responses.is_empty());
        }
    }

    #[test]
    fn test_versioned_encode_fetch_request_dispatches_correctly() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: vec![],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };
        // v4 (MIN) and v7 should use different encoders and produce different output
        let mut buf_v4 = BytesMut::new();
        request.encode_versioned(4, &mut buf_v4).unwrap();
        let mut buf_v7 = BytesMut::new();
        request.encode_versioned(7, &mut buf_v7).unwrap();
        // v7 encodes extra fields (session_id, session_epoch) so should be longer
        assert!(buf_v7.len() > buf_v4.len());
        // v0 should now be unsupported
        let mut buf_v0 = BytesMut::new();
        assert!(request.encode_versioned(0, &mut buf_v0).is_err());
    }

    #[test]
    fn test_fetch_request_encode_v11_appends_rack_id() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![FetchTopicRequest {
                topic: "t".to_string(),
                topic_id: None,
                partitions: vec![FetchPartitionRequest {
                    partition: 0,
                    current_leader_epoch: 5,
                    fetch_offset: 100,
                    last_fetched_epoch: -1,
                    log_start_offset: 0,
                    partition_max_bytes: 1048576,
                    replica_directory_id: None,
                    high_watermark: None,
                }],
            }],
            forgotten_topics: vec![],
            rack_id: "us-east-1a".to_string(),
        };

        let mut buf_v9 = BytesMut::new();
        request.encode_v9(&mut buf_v9).unwrap();

        let mut buf_v11 = BytesMut::new();
        request.encode_v11(&mut buf_v11).unwrap();

        // v11 is v9 + rack_id string (2 bytes length + 10 bytes "us-east-1a")
        assert_eq!(buf_v11.len(), buf_v9.len() + 2 + 10);

        // The v11 buffer should start with the same bytes as v9
        assert_eq!(&buf_v11[..buf_v9.len()], &buf_v9[..]);
    }

    #[test]
    fn test_fetch_request_encode_v11_empty_rack_id() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };

        let mut buf_v9 = BytesMut::new();
        request.encode_v9(&mut buf_v9).unwrap();

        let mut buf_v11 = BytesMut::new();
        request.encode_v11(&mut buf_v11).unwrap();

        // Empty rack_id: 2-byte length prefix (0) only
        assert_eq!(buf_v11.len(), buf_v9.len() + 2);
    }

    #[test]
    fn test_fetch_response_decode_v11_preferred_read_replica() {
        // Build a v11 response: same as v7 but with preferred_read_replica per partition
        let mut raw = BytesMut::new();
        raw.put_i32(100); // throttle_time_ms
        raw.put_i16(0); // error_code (None)
        raw.put_i32(42); // session_id
        raw.put_i32(1); // 1 topic
        // topic name
        raw.put_i16(5);
        raw.put_slice(b"topic");
        raw.put_i32(1); // 1 partition
        raw.put_i32(0); // partition id
        raw.put_i16(0); // error_code
        raw.put_i64(1000); // high_watermark
        raw.put_i64(999); // last_stable_offset
        raw.put_i64(0); // log_start_offset
        raw.put_i32(0); // 0 aborted transactions
        raw.put_i32(3); // preferred_read_replica = broker 3
        raw.put_i32(-1); // records (null/-1 length)

        let mut buf = raw.freeze();
        let resp = FetchResponse::decode_v11(&mut buf).unwrap();

        assert_eq!(resp.throttle_time_ms, 100);
        assert_eq!(resp.session_id, 42);
        assert_eq!(resp.responses.len(), 1);
        let part = &resp.responses[0].partitions[0];
        assert_eq!(part.partition, 0);
        assert_eq!(part.high_watermark, 1000);
        assert_eq!(part.preferred_read_replica, 3);
    }

    #[test]
    fn test_fetch_response_decode_v11_no_preferred_replica() {
        let mut raw = BytesMut::new();
        raw.put_i32(0); // throttle_time_ms
        raw.put_i16(0); // error_code
        raw.put_i32(0); // session_id
        raw.put_i32(1); // 1 topic
        raw.put_i16(1);
        raw.put_slice(b"t");
        raw.put_i32(1); // 1 partition
        raw.put_i32(0); // partition id
        raw.put_i16(0); // error_code
        raw.put_i64(500); // high_watermark
        raw.put_i64(499); // last_stable_offset
        raw.put_i64(0); // log_start_offset
        raw.put_i32(0); // 0 aborted transactions
        raw.put_i32(-1); // preferred_read_replica = -1 (no preference)
        raw.put_i32(-1); // records (null)

        let mut buf = raw.freeze();
        let resp = FetchResponse::decode_v11(&mut buf).unwrap();

        assert_eq!(resp.responses[0].partitions[0].preferred_read_replica, -1);
    }

    #[test]
    fn test_versioned_encode_fetch_v11_dispatches_to_encode_v11() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![],
            forgotten_topics: vec![],
            rack_id: "rack-a".to_string(),
        };

        let mut buf_direct = BytesMut::new();
        request.encode_v11(&mut buf_direct).unwrap();

        let mut buf_versioned = BytesMut::new();
        request.encode_versioned(11, &mut buf_versioned).unwrap();

        assert_eq!(buf_direct, buf_versioned);
    }

    #[test]
    fn test_versioned_decode_fetch_v11_dispatches_to_decode_v11() {
        let mut raw = BytesMut::new();
        raw.put_i32(0); // throttle_time_ms
        raw.put_i16(0); // error_code
        raw.put_i32(0); // session_id
        raw.put_i32(0); // 0 topics

        let data = raw.freeze();
        let resp = FetchResponse::decode_versioned(11, &mut data.clone()).unwrap();
        assert_eq!(resp.throttle_time_ms, 0);
        assert!(resp.responses.is_empty());
    }

    #[test]
    fn test_versioned_encode_fetch_v8_dispatches_to_v7() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![FetchTopicRequest {
                topic: "t".to_string(),
                topic_id: None,
                partitions: vec![FetchPartitionRequest {
                    partition: 0,
                    current_leader_epoch: -1,
                    fetch_offset: 100,
                    last_fetched_epoch: -1,
                    log_start_offset: 0,
                    partition_max_bytes: 1048576,
                    replica_directory_id: None,
                    high_watermark: None,
                }],
            }],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };

        let mut buf_v7 = BytesMut::new();
        request.encode_versioned(7, &mut buf_v7).unwrap();

        let mut buf_v8 = BytesMut::new();
        request.encode_versioned(8, &mut buf_v8).unwrap();
        assert_eq!(buf_v8, buf_v7, "v8 should produce same bytes as v7");
    }

    #[test]
    fn test_versioned_encode_fetch_v9_v10_dispatches_to_v9() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1048576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![FetchTopicRequest {
                topic: "t".to_string(),
                topic_id: None,
                partitions: vec![FetchPartitionRequest {
                    partition: 0,
                    current_leader_epoch: 5,
                    fetch_offset: 100,
                    last_fetched_epoch: -1,
                    log_start_offset: 0,
                    partition_max_bytes: 1048576,
                    replica_directory_id: None,
                    high_watermark: None,
                }],
            }],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };

        let mut buf_v9 = BytesMut::new();
        request.encode_v9(&mut buf_v9).unwrap();

        for version in 9..=10 {
            let mut buf = BytesMut::new();
            request.encode_versioned(version, &mut buf).unwrap();
            assert_eq!(buf, buf_v9, "v{version} should produce same bytes as v9");
        }

        // v9+ includes current_leader_epoch (4 bytes per partition) that v7 omits
        let mut buf_v7 = BytesMut::new();
        request.encode_v7(&mut buf_v7).unwrap();
        assert_eq!(
            buf_v9.len(),
            buf_v7.len() + 4,
            "v9 should be 4 bytes longer than v7 (current_leader_epoch per partition)"
        );
    }

    // ---- Story 1.5: Fetch ----

    #[test]
    fn test_fetch_request_v4_encodes() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_048_576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: vec![],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(4, &mut buf).unwrap();
        let mut r = buf.freeze();
        assert_eq!(i32::decode(&mut r).unwrap(), -1); // replica_id
        assert_eq!(i32::decode(&mut r).unwrap(), 500); // max_wait_ms
        assert_eq!(i32::decode(&mut r).unwrap(), 1); // min_bytes
        assert_eq!(i32::decode(&mut r).unwrap(), 1_048_576); // max_bytes
        assert_eq!(i8::decode(&mut r).unwrap(), 0); // isolation_level
    }

    #[test]
    fn test_fetch_request_v12_encodes() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_048_576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(12, &mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_fetch_request_below_min_rejected() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_048_576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: vec![],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(0, &mut buf).is_err());
        let mut buf2 = BytesMut::new();
        assert!(request.encode_versioned(3, &mut buf2).is_err());
    }

    #[rstest]
    // Fetch MIN=4
    #[case::fetch_v0(0)]
    #[case::fetch_v1(1)]
    #[case::fetch_v2(2)]
    #[case::fetch_v3(3)]
    fn test_fetch_encode_below_min(#[case] version: i16) {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_048_576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: vec![],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }

    // ===================================================================
    // Story 1.5: Fetch v12 Wire-Format Tests
    // ===================================================================

    #[test]
    fn test_fetch_request_encode_v12_flexible() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_048_576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: vec![FetchTopicRequest {
                topic: "t1".to_string(),
                topic_id: None,
                partitions: vec![FetchPartitionRequest {
                    partition: 0,
                    current_leader_epoch: 5,
                    fetch_offset: 100,
                    last_fetched_epoch: 3,
                    log_start_offset: 0,
                    partition_max_bytes: 1_048_576,
                    replica_directory_id: None,
                    high_watermark: None,
                }],
            }],
            forgotten_topics: vec![],
            rack_id: "us-east-1a".to_string(),
        };
        let mut buf_v11 = BytesMut::new();
        request.encode_versioned(11, &mut buf_v11).unwrap();
        let mut buf_v12 = BytesMut::new();
        request.encode_versioned(12, &mut buf_v12).unwrap();
        assert!(!buf_v12.is_empty());
        // v12 flexible should differ from v11 non-flexible.
        assert_ne!(buf_v11.as_ref(), buf_v12.as_ref());
    }

    #[test]
    fn test_fetch_request_v12_last_fetched_epoch() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_048_576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: vec![FetchTopicRequest {
                topic: "t".to_string(),
                topic_id: None,
                partitions: vec![FetchPartitionRequest {
                    partition: 0,
                    current_leader_epoch: 1,
                    fetch_offset: 0,
                    last_fetched_epoch: 42, // non-default value
                    log_start_offset: 0,
                    partition_max_bytes: 1_048_576,
                    replica_directory_id: None,
                    high_watermark: None,
                }],
            }],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };

        // Encode v12 — should include last_fetched_epoch.
        let mut buf_v12 = BytesMut::new();
        request.encode_versioned(12, &mut buf_v12).unwrap();

        // Encode v11 — does not include last_fetched_epoch.
        let mut buf_v11 = BytesMut::new();
        request.encode_versioned(11, &mut buf_v11).unwrap();

        // v12 includes last_fetched_epoch (4 bytes per partition) — different size.
        assert_ne!(buf_v11.len(), buf_v12.len());
    }

    #[test]
    fn test_fetch_response_decode_v12_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i32(0); // session_id
        // responses compact array: 1 topic + 1
        varint::encode_unsigned_varint(2, &mut buf);
        // topic name
        let topic = b"t1";
        varint::encode_unsigned_varint(topic.len() as u32 + 1, &mut buf);
        buf.put_slice(topic);
        // partitions compact array: 1 + 1
        varint::encode_unsigned_varint(2, &mut buf);
        buf.put_i32(0); // partition
        buf.put_i16(0); // error_code
        buf.put_i64(1000); // high_watermark
        buf.put_i64(999); // last_stable_offset
        buf.put_i64(0); // log_start_offset
        // aborted_transactions compact array: empty (1 = 0 + 1)
        varint::encode_unsigned_varint(1, &mut buf);
        buf.put_i32(-1); // preferred_read_replica
        // records compact nullable bytes: null (0 varint = null)
        varint::encode_unsigned_varint(0, &mut buf);
        varint::encode_unsigned_varint(0, &mut buf); // partition tagged fields
        varint::encode_unsigned_varint(0, &mut buf); // topic tagged fields
        varint::encode_unsigned_varint(0, &mut buf); // top-level tagged fields

        let resp = FetchResponse::decode_versioned(12, &mut buf.freeze()).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.responses.len(), 1);
        assert_eq!(resp.responses[0].topic, "t1");
        assert_eq!(resp.responses[0].partitions[0].high_watermark, 1000);
        assert_eq!(resp.responses[0].partitions[0].last_stable_offset, 999);
        assert_eq!(resp.responses[0].partitions[0].preferred_read_replica, -1);
    }

    #[test]
    fn test_fetch_response_v11_still_decodes() {
        // Verify v11 non-flexible still works after v12 activation.
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i32(0); // session_id
        buf.put_i32(1); // topics count
        let topic = b"t";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        buf.put_i32(1); // partitions count
        buf.put_i32(0); // partition
        buf.put_i16(0); // error_code
        buf.put_i64(100); // high_watermark
        buf.put_i64(100); // last_stable_offset
        buf.put_i64(0); // log_start_offset
        buf.put_i32(0); // aborted_transactions count (empty)
        buf.put_i32(-1); // preferred_read_replica
        buf.put_i32(-1); // records null bytes (length = -1)

        let resp = FetchResponse::decode_versioned(11, &mut buf.freeze()).unwrap();
        assert_eq!(resp.responses[0].topic, "t");
        assert_eq!(resp.responses[0].partitions[0].high_watermark, 100);
        assert_eq!(resp.responses[0].partitions[0].preferred_read_replica, -1);
    }

    // ── Fetch v13 (topic_id) ──

    #[test]
    fn test_fetch_response_decode_v13_topic_id() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i32(0); // session_id
        varint::encode_unsigned_varint(2, &mut buf); // 1 topic + 1
        let topic_id: [u8; 16] = [7; 16];
        buf.put_slice(&topic_id);
        varint::encode_unsigned_varint(2, &mut buf); // 1 partition + 1
        buf.put_i32(0); // partition index
        buf.put_i16(0); // error_code
        buf.put_i64(200); // high_watermark
        buf.put_i64(200); // last_stable_offset
        buf.put_i64(0); // log_start_offset
        varint::encode_unsigned_varint(1, &mut buf); // 0 aborted txns
        buf.put_i32(-1); // preferred_read_replica
        varint::encode_unsigned_varint(0, &mut buf); // records null compact bytes
        varint::encode_unsigned_varint(0, &mut buf); // partition tagged
        varint::encode_unsigned_varint(0, &mut buf); // topic tagged
        varint::encode_unsigned_varint(0, &mut buf); // top-level tagged

        let resp = FetchResponse::decode_versioned(13, &mut buf.freeze()).unwrap();
        assert_eq!(resp.responses.len(), 1);
        assert_eq!(resp.responses[0].topic_id, Some(topic_id));
        assert!(resp.responses[0].topic.is_empty());
        assert_eq!(resp.responses[0].partitions[0].high_watermark, 200);
    }

    #[test]
    fn test_fetch_request_encode_v13_topic_id() {
        let topic_id: [u8; 16] = [0xBB; 16];
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_048_576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: -1,
            topics: vec![FetchTopicRequest {
                topic: String::new(),
                topic_id: Some(topic_id),
                partitions: vec![],
            }],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };
        let mut buf = BytesMut::new();
        request.encode_v13(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_i32(), -1); // replica_id
        assert_eq!(cur.get_i32(), 500); // max_wait_ms
        assert_eq!(cur.get_i32(), 1); // min_bytes
        assert_eq!(cur.get_i32(), 1_048_576); // max_bytes
        assert_eq!(cur.get_i8(), 0); // isolation_level
        assert_eq!(cur.get_i32(), 0); // session_id
        assert_eq!(cur.get_i32(), -1); // session_epoch
        let topics_varint = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(topics_varint, 2); // 1 topic + 1
        let mut read_id = [0u8; 16];
        cur.copy_to_slice(&mut read_id);
        assert_eq!(read_id, topic_id);
    }

    // ── Fetch v15 (no ReplicaId on wire) ──

    #[test]
    fn test_fetch_request_encode_v15_no_replica_id() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 300,
            min_bytes: 1,
            max_bytes: 512,
            isolation_level: 1,
            session_id: 5,
            session_epoch: 2,
            topics: vec![],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };
        let mut buf = BytesMut::new();
        request.encode_v15(&mut buf).unwrap();

        let mut cur = &buf[..];
        // v15: first field is max_wait_ms (not replica_id)
        assert_eq!(cur.get_i32(), 300); // max_wait_ms
        assert_eq!(cur.get_i32(), 1); // min_bytes
        assert_eq!(cur.get_i32(), 512); // max_bytes
        assert_eq!(cur.get_i8(), 1); // isolation_level
        assert_eq!(cur.get_i32(), 5); // session_id
        assert_eq!(cur.get_i32(), 2); // session_epoch
    }

    // ===================================================================
    // Story 18.1: Fetch v17-v18 Wire-Format Tests
    // ===================================================================

    #[test]
    fn test_fetch_request_v17_encodes_replica_directory_id() {
        let dir_id = [1u8; 16];
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_048_576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![FetchTopicRequest {
                topic: "t".to_string(),
                topic_id: Some([2u8; 16]),
                partitions: vec![FetchPartitionRequest {
                    partition: 0,
                    current_leader_epoch: -1,
                    fetch_offset: 100,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: 1_048_576,
                    replica_directory_id: Some(dir_id),
                    high_watermark: None,
                }],
            }],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(17, &mut buf).unwrap();
        assert!(!buf.is_empty());
        // v17 should include directory ID tag but no high watermark.
        let encoded = buf.freeze();
        // The directory ID bytes should appear in the output.
        assert!(
            encoded.windows(16).any(|w| w == dir_id),
            "expected replica_directory_id in v17 output"
        );
    }

    #[test]
    fn test_fetch_request_v18_encodes_high_watermark() {
        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: 500,
            min_bytes: 1,
            max_bytes: 1_048_576,
            isolation_level: 0,
            session_id: 0,
            session_epoch: 0,
            topics: vec![FetchTopicRequest {
                topic: "t".to_string(),
                topic_id: Some([3u8; 16]),
                partitions: vec![FetchPartitionRequest {
                    partition: 0,
                    current_leader_epoch: -1,
                    fetch_offset: 100,
                    last_fetched_epoch: -1,
                    log_start_offset: -1,
                    partition_max_bytes: 1_048_576,
                    replica_directory_id: None,
                    high_watermark: Some(9999),
                }],
            }],
            forgotten_topics: vec![],
            rack_id: String::new(),
        };
        let mut buf_v17 = BytesMut::new();
        request.encode_versioned(17, &mut buf_v17).unwrap();
        let mut buf_v18 = BytesMut::new();
        request.encode_versioned(18, &mut buf_v18).unwrap();
        // v18 with high_watermark should be longer than v17 without it.
        assert!(
            buf_v18.len() > buf_v17.len(),
            "v18 should be longer due to high_watermark tagged field"
        );
    }

    #[test]
    fn test_fetch_response_decode_v17_v18_reuses_v13() {
        // v17 and v18 have the same response format as v13.
        // Build a minimal v13 response and verify it decodes under v17/v18.
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i32(0); // session_id
        varint::encode_unsigned_varint(1, &mut buf); // compact array len (0 elements + 1)
        varint::encode_unsigned_varint(0, &mut buf); // tagged fields
        for version in [17, 18] {
            let clone = buf.clone();
            let resp = FetchResponse::decode_versioned(version, &mut clone.freeze()).unwrap();
            assert_eq!(resp.throttle_time_ms, 10);
        }
    }
}
