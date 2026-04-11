use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, encode_compact_array_len,
};

// ============================================================================
// CreateTopics request/response
// ============================================================================

/// Topic configuration for CreateTopics.
#[derive(Debug, Clone)]
pub struct CreatableTopicConfig {
    /// Config name.
    pub name: String,
    /// Config value.
    pub value: Option<String>,
}

/// Topic to create.
#[derive(Debug, Clone)]
pub struct CreatableTopic {
    /// Topic name.
    pub name: String,
    /// Number of partitions (-1 = default).
    pub num_partitions: i32,
    /// Replication factor (-1 = default).
    pub replication_factor: i16,
    /// Manual replica assignments.
    pub assignments: Vec<CreatableReplicaAssignment>,
    /// Topic configs.
    pub configs: Vec<CreatableTopicConfig>,
}

/// Replica assignment.
#[derive(Debug, Clone)]
pub struct CreatableReplicaAssignment {
    /// Partition index.
    pub partition_index: i32,
    /// Broker IDs.
    pub broker_ids: Vec<i32>,
}

/// CreateTopics request.
#[derive(Debug, Clone)]
pub struct CreateTopicsRequest {
    /// Topics to create.
    pub topics: Vec<CreatableTopic>,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
    /// Validate only (v1+).
    pub validate_only: bool,
}

impl CreateTopicsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::CreateTopics
    }

    /// Encode for version 2–4 (non-flexible).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            topic.num_partitions.encode(buf);
            topic.replication_factor.encode(buf);

            buf.put_i32(array_len_i32(topic.assignments.len())?);
            for assignment in &topic.assignments {
                assignment.partition_index.encode(buf);
                buf.put_i32(array_len_i32(assignment.broker_ids.len())?);
                for broker in &assignment.broker_ids {
                    broker.encode(buf);
                }
            }

            buf.put_i32(array_len_i32(topic.configs.len())?);
            for config in &topic.configs {
                KafkaString::new(&config.name).try_encode(buf)?;
                match &config.value {
                    Some(v) => KafkaString::new(v).try_encode(buf)?,
                    None => KafkaString::null().try_encode(buf)?,
                }
            }
        }
        self.timeout_ms.encode(buf);
        buf.put_u8(if self.validate_only { 1 } else { 0 });
        Ok(())
    }

    /// Encode for version 5–7 (flexible encoding).
    pub fn encode_v5(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode_compact(buf)?;
            topic.num_partitions.encode(buf);
            topic.replication_factor.encode(buf);

            encode_compact_array_len(topic.assignments.len(), buf)?;
            for assignment in &topic.assignments {
                assignment.partition_index.encode(buf);
                encode_compact_array_len(assignment.broker_ids.len(), buf)?;
                for broker in &assignment.broker_ids {
                    broker.encode(buf);
                }
                TaggedFields::default().try_encode(buf)?; // per-assignment tagged fields
            }

            encode_compact_array_len(topic.configs.len(), buf)?;
            for config in &topic.configs {
                KafkaString::new(&config.name).try_encode_compact(buf)?;
                match &config.value {
                    Some(v) => KafkaString::new(v).try_encode_compact(buf)?,
                    None => KafkaString::null().try_encode_compact(buf)?,
                }
                TaggedFields::default().try_encode(buf)?; // per-config tagged fields
            }
            TaggedFields::default().try_encode(buf)?; // per-topic tagged fields
        }
        self.timeout_ms.encode(buf);
        buf.put_u8(if self.validate_only { 1 } else { 0 });
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Result for a created topic.
#[derive(Debug, Clone)]
pub struct CreatableTopicResult {
    /// Topic name.
    pub name: String,
    /// Topic UUID (v7+).
    pub topic_id: Option<[u8; 16]>,
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message (v1+).
    pub error_message: Option<String>,
    /// Number of partitions (v5+).
    pub num_partitions: i32,
    /// Replication factor (v5+).
    pub replication_factor: i16,
}

/// CreateTopics response.
#[derive(Debug, Clone)]
pub struct CreateTopicsResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Topics.
    pub topics: Vec<CreatableTopicResult>,
}

impl CreateTopicsResponse {
    /// Decode from version 2–4 (non-flexible).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            topics: Self::decode_topics_v1(buf)?,
        })
    }

    /// Decode from version 5–6 (flexible, with num_partitions/replication_factor/configs).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topics = Self::decode_topics_flexible(buf, false)?;
        TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }

    /// Decode from version 7 (flexible, adds topic_id UUID).
    pub fn decode_v7(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topics = Self::decode_topics_flexible(buf, true)?;
        TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }

    /// Shared topics array decoder for v1–v4 (non-flexible, includes error_message).
    fn decode_topics_v1(buf: &mut impl Buf) -> Result<Vec<CreatableTopicResult>> {
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;

            topics.push(CreatableTopicResult {
                name,
                topic_id: None,
                error_code,
                error_message,
                num_partitions: -1,
                replication_factor: -1,
            });
        }

        Ok(topics)
    }

    /// Shared topics array decoder for v5+ (flexible, compact encoding).
    fn decode_topics_flexible(
        buf: &mut impl Buf,
        has_topic_id: bool,
    ) -> Result<Vec<CreatableTopicResult>> {
        let raw = crate::util::varint::decode_unsigned_varint(buf)?;
        let topic_count = check_compact_array_len(raw)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;

            let topic_id = if has_topic_id {
                if buf.remaining() < 16 {
                    return Err(KrafkaError::protocol("not enough bytes for topic_id UUID"));
                }
                let mut id = [0u8; 16];
                buf.copy_to_slice(&mut id);
                if id == [0u8; 16] { None } else { Some(id) }
            } else {
                None
            };

            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;

            // v5+ fields: num_partitions, replication_factor
            let num_partitions = i32::decode(buf)?;
            let replication_factor = i16::decode(buf)?;

            // v5+ configs (nullable compact array) — skip for now
            let configs_raw = crate::util::varint::decode_unsigned_varint(buf)?;
            if configs_raw > 0 {
                let configs_len = check_compact_array_len(configs_raw)?;
                for _ in 0..configs_len {
                    let _name = KafkaString::decode_compact(buf)?; // config name
                    let _value = KafkaString::decode_compact(buf)?; // config value (nullable)
                    let _read_only = bool::decode(buf)?; // ReadOnly
                    let _config_source = i8::decode(buf)?; // ConfigSource
                    let _is_sensitive = bool::decode(buf)?; // IsSensitive
                    TaggedFields::decode(buf)?; // per-config tagged fields
                }
            }

            TaggedFields::decode(buf)?; // per-topic tagged fields (TopicConfigErrorCode as tag 0)

            topics.push(CreatableTopicResult {
                name,
                topic_id,
                error_code,
                error_message,
                num_partitions,
                replication_factor,
            });
        }

        Ok(topics)
    }
}

// ============================================================================
// DeleteTopics request/response
// ============================================================================

/// DeleteTopics request.
#[derive(Debug, Clone)]
pub struct DeleteTopicsRequest {
    /// Topic names (v1–v5).
    pub topic_names: Vec<String>,
    /// Topics with optional topic_id for v6+ deletion.
    pub topics: Vec<DeleteTopicState>,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
}

/// Topic entry for v6 DeleteTopics (supports deletion by name or UUID).
#[derive(Debug, Clone)]
pub struct DeleteTopicState {
    /// Topic name (nullable in v6+).
    pub name: Option<String>,
    /// Topic UUID (v6+).
    pub topic_id: [u8; 16],
}

impl DeleteTopicsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::DeleteTopics
    }

    /// Encode for version 1–3 (non-flexible).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.topic_names.len())?);
        for name in &self.topic_names {
            KafkaString::new(name).try_encode(buf)?;
        }
        self.timeout_ms.encode(buf);
        Ok(())
    }

    /// Encode for version 4–5 (flexible encoding, still TopicNames).
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.topic_names.len(), buf)?;
        for name in &self.topic_names {
            KafkaString::new(name).try_encode_compact(buf)?;
        }
        self.timeout_ms.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 6 (flexible, Topics array with Name + TopicId).
    pub fn encode_v6(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            match &topic.name {
                Some(n) => KafkaString::new(n).try_encode_compact(buf)?,
                None => KafkaString::null().try_encode_compact(buf)?,
            }
            buf.put_slice(&topic.topic_id);
            TaggedFields::default().try_encode(buf)?; // per-topic tagged fields
        }
        self.timeout_ms.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Result for a deleted topic.
#[derive(Debug, Clone)]
pub struct DeletableTopicResult {
    /// Topic name (nullable in v6+).
    pub name: Option<String>,
    /// Topic UUID (v6+).
    pub topic_id: Option<[u8; 16]>,
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message (v5+).
    pub error_message: Option<String>,
}

/// DeleteTopics response.
#[derive(Debug, Clone)]
pub struct DeleteTopicsResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Responses.
    pub responses: Vec<DeletableTopicResult>,
}

impl DeleteTopicsResponse {
    /// Decode from version 1–3 (non-flexible).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            responses: Self::decode_responses_v1(buf)?,
        })
    }

    /// Decode from version 4 (flexible, no error_message, no topic_id).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let raw = crate::util::varint::decode_unsigned_varint(buf)?;
        let count = check_compact_array_len(raw)?;
        let mut responses = Vec::with_capacity(count);

        for _ in 0..count {
            let name = KafkaString::decode_compact(buf)?.0;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            TaggedFields::decode(buf)?;

            responses.push(DeletableTopicResult {
                name,
                topic_id: None,
                error_code,
                error_message: None,
            });
        }

        TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            responses,
        })
    }

    /// Decode from version 5 (flexible, adds error_message).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let raw = crate::util::varint::decode_unsigned_varint(buf)?;
        let count = check_compact_array_len(raw)?;
        let mut responses = Vec::with_capacity(count);

        for _ in 0..count {
            let name = KafkaString::decode_compact(buf)?.0;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            TaggedFields::decode(buf)?;

            responses.push(DeletableTopicResult {
                name,
                topic_id: None,
                error_code,
                error_message,
            });
        }

        TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            responses,
        })
    }

    /// Decode from version 6 (flexible, adds topic_id).
    pub fn decode_v6(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let raw = crate::util::varint::decode_unsigned_varint(buf)?;
        let count = check_compact_array_len(raw)?;
        let mut responses = Vec::with_capacity(count);

        for _ in 0..count {
            let name = KafkaString::decode_compact(buf)?.0;

            if buf.remaining() < 16 {
                return Err(KrafkaError::protocol("not enough bytes for topic_id UUID"));
            }
            let mut id = [0u8; 16];
            buf.copy_to_slice(&mut id);
            let topic_id = if id == [0u8; 16] { None } else { Some(id) };

            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            TaggedFields::decode(buf)?;

            responses.push(DeletableTopicResult {
                name,
                topic_id,
                error_code,
                error_message,
            });
        }

        TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            responses,
        })
    }

    /// Shared responses array decoder for v1–v3 (non-flexible).
    fn decode_responses_v1(buf: &mut impl Buf) -> Result<Vec<DeletableTopicResult>> {
        let response_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut responses = Vec::with_capacity(response_count);

        for _ in 0..response_count {
            let name = KafkaString::decode(buf)?.0;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);

            responses.push(DeletableTopicResult {
                name,
                topic_id: None,
                error_code,
                error_message: None,
            });
        }

        Ok(responses)
    }
}

// ============================================================================
// CreatePartitions API (Key 37)
// ============================================================================

/// CreatePartitions request.
#[derive(Debug, Clone)]
pub struct CreatePartitionsRequest {
    /// Topics to create partitions for.
    pub topics: Vec<CreatePartitionsTopic>,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
    /// If true, validate the request without actually creating partitions.
    pub validate_only: bool,
}

/// Topic in CreatePartitions request.
#[derive(Debug, Clone)]
pub struct CreatePartitionsTopic {
    /// Topic name.
    pub name: String,
    /// New total partition count.
    pub count: i32,
    /// Assignment of new partitions to brokers.
    pub assignments: Option<Vec<CreatePartitionsAssignment>>,
}

/// Partition assignment in CreatePartitions request.
#[derive(Debug, Clone)]
pub struct CreatePartitionsAssignment {
    /// Broker IDs to assign the partition replicas to.
    pub broker_ids: Vec<i32>,
}

impl CreatePartitionsRequest {
    /// Create a simple partition increase request.
    pub fn new(topic: impl Into<String>, count: i32, timeout: std::time::Duration) -> Self {
        Self {
            topics: vec![CreatePartitionsTopic {
                name: topic.into(),
                count,
                assignments: None,
            }],
            timeout_ms: crate::util::duration_to_millis_i32(timeout),
            validate_only: false,
        }
    }

    /// Encode for version 0–1 (non-flexible).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        // Array of topics
        array_len_i32(self.topics.len())?.encode(buf);
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode(buf)?;
            topic.count.encode(buf);

            // Assignments (nullable array)
            match &topic.assignments {
                None => (-1i32).encode(buf),
                Some(assignments) => {
                    array_len_i32(assignments.len())?.encode(buf);
                    for assignment in assignments {
                        array_len_i32(assignment.broker_ids.len())?.encode(buf);
                        for &broker_id in &assignment.broker_ids {
                            broker_id.encode(buf);
                        }
                    }
                }
            }
        }
        self.timeout_ms.encode(buf);
        buf.put_u8(if self.validate_only { 1 } else { 0 });
        Ok(())
    }

    /// Encode for version 2–3 (flexible encoding).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode_compact(buf)?;
            topic.count.encode(buf);

            // Assignments (nullable compact array)
            match &topic.assignments {
                None => {
                    crate::util::varint::encode_unsigned_varint(0, buf);
                }
                Some(assignments) => {
                    encode_compact_array_len(assignments.len(), buf)?;
                    for assignment in assignments {
                        encode_compact_array_len(assignment.broker_ids.len(), buf)?;
                        for &broker_id in &assignment.broker_ids {
                            broker_id.encode(buf);
                        }
                        TaggedFields::default().try_encode(buf)?;
                    }
                }
            }
            TaggedFields::default().try_encode(buf)?; // per-topic tagged fields
        }
        self.timeout_ms.encode(buf);
        buf.put_u8(if self.validate_only { 1 } else { 0 });
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// CreatePartitions response.
#[derive(Debug, Clone)]
pub struct CreatePartitionsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Results per topic.
    pub results: Vec<CreatePartitionsTopicResult>,
}

/// Result for a topic in CreatePartitions response.
#[derive(Debug, Clone)]
pub struct CreatePartitionsTopicResult {
    /// Topic name.
    pub name: String,
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
}

impl CreatePartitionsResponse {
    /// Decode from version 0–1 (non-flexible).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;

            results.push(CreatePartitionsTopicResult {
                name,
                error_code,
                error_message,
            });
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    /// Decode from version 2–3 (flexible encoding).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let raw = crate::util::varint::decode_unsigned_varint(buf)?;
        let result_count = check_compact_array_len(raw)?;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let name = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            TaggedFields::decode(buf)?; // per-result tagged fields

            results.push(CreatePartitionsTopicResult {
                name,
                error_code,
                error_message,
            });
        }

        TaggedFields::decode(buf)?; // top-level tagged fields
        Ok(Self {
            throttle_time_ms,
            results,
        })
    }
}

// ============================================================================
// DescribeTopicPartitions API (Key 75)
// ============================================================================

/// A cursor for paginated DescribeTopicPartitions requests.
#[derive(Debug, Clone)]
pub struct DescribeTopicPartitionsCursor {
    /// Topic name to start from.
    pub topic_name: String,
    /// Partition index to start from.
    pub partition_index: i32,
}

/// DescribeTopicPartitions request (API Key 75). Flexible from v0.
#[derive(Debug, Clone)]
pub struct DescribeTopicPartitionsRequest {
    /// Topics to describe.
    pub topics: Vec<String>,
    /// Maximum number of partitions in the response.
    pub response_partition_limit: i32,
    /// Pagination cursor (null for first page).
    pub cursor: Option<DescribeTopicPartitionsCursor>,
}

impl DescribeTopicPartitionsRequest {
    /// Create a new request for the given topics.
    pub fn new(topics: Vec<String>) -> Self {
        Self {
            topics,
            response_partition_limit: 2000,
            cursor: None,
        }
    }

    /// Encode for version 0 (flexible from v0).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            KafkaString::new(topic).try_encode_compact(buf)?;
            TaggedFields::default().try_encode(buf)?;
        }
        self.response_partition_limit.encode(buf);
        match &self.cursor {
            None => {
                // Nullable struct: tag byte 0xFF means null for tagged structs… actually
                // for nullable structs in flexible, 0xFF = null
                buf.put_u8(0xFF);
            }
            Some(c) => {
                KafkaString::new(&c.topic_name).try_encode_compact(buf)?;
                c.partition_index.encode(buf);
                TaggedFields::default().try_encode(buf)?;
            }
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Per-partition info in DescribeTopicPartitions response.
#[derive(Debug, Clone)]
pub struct DescribeTopicPartitionsPartition {
    /// Error code.
    pub error_code: ErrorCode,
    /// Partition index.
    pub partition_index: i32,
    /// Leader broker ID.
    pub leader_id: i32,
    /// Leader epoch.
    pub leader_epoch: i32,
    /// Replica broker IDs.
    pub replica_nodes: Vec<i32>,
    /// ISR broker IDs.
    pub isr_nodes: Vec<i32>,
    /// Eligible leader replicas (may be null).
    pub eligible_leader_replicas: Option<Vec<i32>>,
    /// Last known ELR (may be null).
    pub last_known_elr: Option<Vec<i32>>,
    /// Offline replica broker IDs.
    pub offline_replicas: Vec<i32>,
}

/// Per-topic info in DescribeTopicPartitions response.
#[derive(Debug, Clone)]
pub struct DescribeTopicPartitionsTopic {
    /// Error code.
    pub error_code: ErrorCode,
    /// Topic name.
    pub name: Option<String>,
    /// Topic ID.
    pub topic_id: [u8; 16],
    /// Whether the topic is internal.
    pub is_internal: bool,
    /// Partitions.
    pub partitions: Vec<DescribeTopicPartitionsPartition>,
    /// Authorized operations bitfield.
    pub topic_authorized_operations: i32,
}

/// DescribeTopicPartitions response (API Key 75). Flexible from v0.
#[derive(Debug, Clone)]
pub struct DescribeTopicPartitionsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Topics.
    pub topics: Vec<DescribeTopicPartitionsTopic>,
    /// Pagination cursor for next page (null if no more pages).
    pub next_cursor: Option<DescribeTopicPartitionsCursor>,
}

impl DescribeTopicPartitionsResponse {
    /// Helper: decode compact nullable i32 array.
    fn decode_compact_nullable_i32_array(buf: &mut impl Buf) -> Result<Option<Vec<i32>>> {
        let raw = crate::util::varint::decode_unsigned_varint(buf)?;
        if raw == 0 {
            return Ok(None);
        }
        let count = check_compact_array_len(raw)?;
        let mut arr = Vec::with_capacity(count);
        for _ in 0..count {
            arr.push(i32::decode(buf)?);
        }
        Ok(Some(arr))
    }

    /// Decode from version 0 (flexible from v0).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let name = KafkaString::decode_compact(buf)?.0;
            let mut topic_id = [0u8; 16];
            if buf.remaining() < 16 {
                return Err(KrafkaError::protocol("short buf for topic_id"));
            }
            buf.copy_to_slice(&mut topic_id);
            let is_internal = i8::decode(buf)? != 0;

            let part_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(part_count);
            for _ in 0..part_count {
                let p_error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let partition_index = i32::decode(buf)?;
                let leader_id = i32::decode(buf)?;
                let leader_epoch = i32::decode(buf)?;

                let replica_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut replica_nodes = Vec::with_capacity(replica_count);
                for _ in 0..replica_count {
                    replica_nodes.push(i32::decode(buf)?);
                }

                let isr_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut isr_nodes = Vec::with_capacity(isr_count);
                for _ in 0..isr_count {
                    isr_nodes.push(i32::decode(buf)?);
                }

                let eligible_leader_replicas = Self::decode_compact_nullable_i32_array(buf)?;
                let last_known_elr = Self::decode_compact_nullable_i32_array(buf)?;

                let offline_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut offline_replicas = Vec::with_capacity(offline_count);
                for _ in 0..offline_count {
                    offline_replicas.push(i32::decode(buf)?);
                }

                let _ = TaggedFields::decode(buf)?;
                partitions.push(DescribeTopicPartitionsPartition {
                    error_code: p_error_code,
                    partition_index,
                    leader_id,
                    leader_epoch,
                    replica_nodes,
                    isr_nodes,
                    eligible_leader_replicas,
                    last_known_elr,
                    offline_replicas,
                });
            }

            let topic_authorized_operations = i32::decode(buf)?;
            let _ = TaggedFields::decode(buf)?;

            topics.push(DescribeTopicPartitionsTopic {
                error_code,
                name,
                topic_id,
                is_internal,
                partitions,
                topic_authorized_operations,
            });
        }

        // Decode next_cursor (nullable struct)
        // For nullable structs in flexible encoding, 0xFF byte means null
        let next_cursor = if buf.remaining() > 0 {
            let marker = buf.chunk()[0];
            if marker == 0xFF {
                buf.advance(1);
                let _ = TaggedFields::decode(buf)?;
                None
            } else {
                let topic_name =
                    non_nullable_string("cursor topic", KafkaString::decode_compact(buf)?.0)?;
                let partition_index = i32::decode(buf)?;
                let _ = TaggedFields::decode(buf)?;
                let _ = TaggedFields::decode(buf)?;
                Some(DescribeTopicPartitionsCursor {
                    topic_name,
                    partition_index,
                })
            }
        } else {
            None
        };

        Ok(Self {
            throttle_time_ms,
            topics,
            next_cursor,
        })
    }
}

impl VersionedEncode for CreateTopicsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            2..=4 => self.encode_v2(buf)?,
            5..=7 => self.encode_v5(buf)?,
            _ => return unsupported_encode!("CreateTopicsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for CreateTopicsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            2..=4 => Self::decode_v2(buf),
            5 | 6 => Self::decode_v5(buf),
            7 => Self::decode_v7(buf),
            _ => unsupported_decode!("CreateTopicsResponse", version),
        }
    }
}

impl VersionedEncode for DeleteTopicsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1..=3 => self.encode_v1(buf)?,
            4 | 5 => self.encode_v4(buf)?,
            6 => self.encode_v6(buf)?,
            _ => return unsupported_encode!("DeleteTopicsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DeleteTopicsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1..=3 => Self::decode_v1(buf),
            4 => Self::decode_v4(buf),
            5 => Self::decode_v5(buf),
            6 => Self::decode_v6(buf),
            _ => unsupported_decode!("DeleteTopicsResponse", version),
        }
    }
}

impl VersionedEncode for CreatePartitionsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 | 1 => self.encode_v0(buf)?,
            2 | 3 => self.encode_v2(buf)?,
            _ => return unsupported_encode!("CreatePartitionsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for CreatePartitionsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 | 1 => Self::decode_v0(buf),
            2 | 3 => Self::decode_v2(buf),
            _ => unsupported_decode!("CreatePartitionsResponse", version),
        }
    }
}

impl VersionedEncode for DescribeTopicPartitionsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("DescribeTopicPartitionsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DescribeTopicPartitionsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("DescribeTopicPartitionsResponse", version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::util::varint;
    use bytes::BytesMut;
    use rstest::rstest;

    // -----------------------------------------------------------------------
    // ConsumerGroupHeartbeat (API key 68, KIP-848)
    // -----------------------------------------------------------------------

    /// Helper: encode a compact string into `buf`.
    /// Non-null string: varint(len + 1) then bytes.
    /// Null string: varint(0).
    fn put_compact_string(buf: &mut BytesMut, s: Option<&str>) {
        match s {
            Some(val) => {
                // len + 1 fits in one byte for small strings
                buf.put_u8((val.len() + 1) as u8);
                buf.put_slice(val.as_bytes());
            }
            None => buf.put_u8(0),
        }
    }

    /// Helper: write empty tagged fields (varint 0).
    fn put_tagged_fields(buf: &mut BytesMut) {
        buf.put_u8(0);
    }

    // ── Epic 12: Topic Management flexible versions ──────────────────

    #[test]
    fn test_create_topics_v3_v4_same_wire_as_v2() {
        let request = CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "test".to_string(),
                num_partitions: 3,
                replication_factor: 1,
                assignments: vec![],
                configs: vec![],
            }],
            timeout_ms: 30_000,
            validate_only: false,
        };
        let mut v2 = BytesMut::new();
        request.encode_versioned(2, &mut v2).unwrap();
        let mut v3 = BytesMut::new();
        request.encode_versioned(3, &mut v3).unwrap();
        let mut v4 = BytesMut::new();
        request.encode_versioned(4, &mut v4).unwrap();
        assert_eq!(v2, v3);
        assert_eq!(v2, v4);
    }

    #[test]
    fn test_create_topics_v5_flexible() {
        let request = CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: "test".to_string(),
                num_partitions: 3,
                replication_factor: 1,
                assignments: vec![],
                configs: vec![],
            }],
            timeout_ms: 30_000,
            validate_only: false,
        };
        let mut v2 = BytesMut::new();
        request.encode_v2(&mut v2).unwrap();
        let mut v5 = BytesMut::new();
        request.encode_v5(&mut v5).unwrap();
        assert_ne!(v2.len(), v5.len());
        // v5, v6, v7 use same request wire
        let mut v7 = BytesMut::new();
        request.encode_versioned(7, &mut v7).unwrap();
        assert_eq!(v5, v7);
    }

    #[test]
    fn test_create_topics_response_v5_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(2); // topics: compact array count=1+1=2
        buf.put_u8(5); // name: compact string "test"
        buf.put_slice(b"test");
        buf.put_i16(0); // error_code
        buf.put_u8(0); // error_message: compact null
        buf.put_i32(3); // num_partitions
        buf.put_i16(1); // replication_factor
        buf.put_u8(1); // configs: compact nullable array count=0+1=1 (empty)
        buf.put_u8(0); // per-topic tagged fields
        buf.put_u8(0); // top-level tagged fields

        let mut frozen = buf.freeze();
        let resp = CreateTopicsResponse::decode_v5(&mut frozen).unwrap();
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].name, "test");
        assert_eq!(resp.topics[0].num_partitions, 3);
        assert_eq!(resp.topics[0].replication_factor, 1);
        assert!(resp.topics[0].topic_id.is_none());
    }

    #[test]
    fn test_create_topics_response_v7_with_topic_id() {
        let topic_uuid = [1u8, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(2); // topics: compact array count=1+1=2
        buf.put_u8(5); // name: compact string "test"
        buf.put_slice(b"test");
        buf.put_slice(&topic_uuid); // topic_id (16 bytes UUID)
        buf.put_i16(0); // error_code
        buf.put_u8(0); // error_message: compact null
        buf.put_i32(3); // num_partitions
        buf.put_i16(1); // replication_factor
        buf.put_u8(1); // configs: compact nullable array empty
        buf.put_u8(0); // per-topic tagged fields
        buf.put_u8(0); // top-level tagged fields

        let mut frozen = buf.freeze();
        let resp = CreateTopicsResponse::decode_v7(&mut frozen).unwrap();
        assert_eq!(resp.topics[0].topic_id, Some(topic_uuid));
        assert_eq!(resp.topics[0].num_partitions, 3);
    }

    #[test]
    fn test_delete_topics_v2_v3_same_wire_as_v1() {
        let request = DeleteTopicsRequest {
            topic_names: vec!["test".to_string()],
            topics: vec![],
            timeout_ms: 30_000,
        };
        let mut v1 = BytesMut::new();
        request.encode_versioned(1, &mut v1).unwrap();
        let mut v2 = BytesMut::new();
        request.encode_versioned(2, &mut v2).unwrap();
        let mut v3 = BytesMut::new();
        request.encode_versioned(3, &mut v3).unwrap();
        assert_eq!(v1, v2);
        assert_eq!(v1, v3);
    }

    #[test]
    fn test_delete_topics_v4_flexible() {
        let request = DeleteTopicsRequest {
            topic_names: vec!["test".to_string()],
            topics: vec![],
            timeout_ms: 30_000,
        };
        let mut v1 = BytesMut::new();
        request.encode_v1(&mut v1).unwrap();
        let mut v4 = BytesMut::new();
        request.encode_v4(&mut v4).unwrap();
        assert_ne!(v1.len(), v4.len());
        // v4 and v5 share wire format
        let mut v5 = BytesMut::new();
        request.encode_versioned(5, &mut v5).unwrap();
        assert_eq!(v4, v5);
    }

    #[test]
    fn test_delete_topics_v6_with_topic_id() {
        let topic_uuid = [0xAA; 16];
        let request = DeleteTopicsRequest {
            topic_names: vec![],
            topics: vec![DeleteTopicState {
                name: None,
                topic_id: topic_uuid,
            }],
            timeout_ms: 30_000,
        };
        let mut buf = BytesMut::new();
        request.encode_v6(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_delete_topics_response_v4_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(2); // responses: compact array count=1+1=2
        buf.put_u8(5); // name: compact string "test"
        buf.put_slice(b"test");
        buf.put_i16(0); // error_code
        buf.put_u8(0); // per-response tagged fields
        buf.put_u8(0); // top-level tagged fields

        let mut frozen = buf.freeze();
        let resp = DeleteTopicsResponse::decode_v4(&mut frozen).unwrap();
        assert_eq!(resp.responses.len(), 1);
        assert_eq!(resp.responses[0].name.as_deref(), Some("test"));
        assert!(resp.responses[0].topic_id.is_none());
        assert!(resp.responses[0].error_message.is_none());
    }

    #[test]
    fn test_delete_topics_response_v5_with_error_message() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(2); // responses: compact array count=1+1=2
        buf.put_u8(5); // name: compact string "test"
        buf.put_slice(b"test");
        buf.put_i16(0); // error_code
        buf.put_u8(0); // error_message: compact null
        buf.put_u8(0); // per-response tagged fields
        buf.put_u8(0); // top-level tagged fields

        let mut frozen = buf.freeze();
        let resp = DeleteTopicsResponse::decode_v5(&mut frozen).unwrap();
        assert_eq!(resp.responses[0].name.as_deref(), Some("test"));
        assert!(resp.responses[0].error_message.is_none());
    }

    #[test]
    fn test_delete_topics_response_v6_with_topic_id() {
        let topic_uuid = [0xBB; 16];
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(2); // responses: compact array count=1+1=2
        buf.put_u8(0); // name: compact null (deleted by UUID)
        buf.put_slice(&topic_uuid); // topic_id: 16 bytes
        buf.put_i16(0); // error_code
        buf.put_u8(0); // error_message: compact null
        buf.put_u8(0); // per-response tagged fields
        buf.put_u8(0); // top-level tagged fields

        let mut frozen = buf.freeze();
        let resp = DeleteTopicsResponse::decode_v6(&mut frozen).unwrap();
        assert!(resp.responses[0].name.is_none());
        assert_eq!(resp.responses[0].topic_id, Some(topic_uuid));
    }

    #[test]
    fn test_create_partitions_v1_same_as_v0() {
        let request = CreatePartitionsRequest::new("test", 6, std::time::Duration::from_secs(30));
        let mut v0 = BytesMut::new();
        request.encode_versioned(0, &mut v0).unwrap();
        let mut v1 = BytesMut::new();
        request.encode_versioned(1, &mut v1).unwrap();
        assert_eq!(v0, v1);
    }

    #[test]
    fn test_create_partitions_v2_flexible() {
        let request = CreatePartitionsRequest::new("test", 6, std::time::Duration::from_secs(30));
        let mut v0 = BytesMut::new();
        request.encode_v0(&mut v0).unwrap();
        let mut v2 = BytesMut::new();
        request.encode_v2(&mut v2).unwrap();
        assert_ne!(v0.len(), v2.len());
        // v2 and v3 same wire
        let mut v3 = BytesMut::new();
        request.encode_versioned(3, &mut v3).unwrap();
        assert_eq!(v2, v3);
    }

    #[test]
    fn test_create_partitions_response_v2_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(2); // results: compact array count=1+1=2
        buf.put_u8(5); // name: compact string "test"
        buf.put_slice(b"test");
        buf.put_i16(0); // error_code
        buf.put_u8(0); // error_message: compact null
        buf.put_u8(0); // per-result tagged fields
        buf.put_u8(0); // top-level tagged fields

        let mut frozen = buf.freeze();
        let resp = CreatePartitionsResponse::decode_v2(&mut frozen).unwrap();
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].name, "test");
        assert!(resp.results[0].error_code.is_ok());
    }

    #[rstest]
    // CreateTopics MIN=2
    #[case::ct_v0(0)]
    #[case::ct_v1(1)]
    fn test_create_topics_encode_below_min(#[case] version: i16) {
        let request = CreateTopicsRequest {
            topics: vec![],
            timeout_ms: 30_000,
            validate_only: false,
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }

    // ── DescribeTopicPartitions v0 ──

    #[test]
    fn test_describe_topic_partitions_request_encode_v0() {
        let req = DescribeTopicPartitionsRequest {
            topics: vec!["t1".to_string()],
            response_partition_limit: 500,
            cursor: None,
        };
        let mut buf = BytesMut::new();
        req.encode_v0(&mut buf).unwrap();

        let mut cur = &buf[..];
        let arr = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(arr, 2); // 1 topic + 1
        let name_v = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(name_v, 3); // len("t1") + 1
        let mut name = vec![0u8; 2];
        cur.copy_to_slice(&mut name);
        assert_eq!(name, b"t1");
        assert_eq!(cur.get_u8(), 0); // topic tagged fields
        assert_eq!(cur.get_i32(), 500); // response_partition_limit
        assert_eq!(cur.get_u8(), 0xFF); // null cursor
        assert_eq!(cur.get_u8(), 0); // top-level tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn test_describe_topic_partitions_request_encode_v0_with_cursor() {
        let req = DescribeTopicPartitionsRequest {
            topics: vec!["t1".to_string()],
            response_partition_limit: 100,
            cursor: Some(DescribeTopicPartitionsCursor {
                topic_name: "t1".to_string(),
                partition_index: 5,
            }),
        };
        let mut buf = BytesMut::new();
        req.encode_v0(&mut buf).unwrap();

        let mut cur = &buf[..];
        let _ = varint::decode_unsigned_varint(&mut cur).unwrap(); // topic array
        let _ = varint::decode_unsigned_varint(&mut cur).unwrap(); // topic name
        cur.advance(2); // "t1"
        assert_eq!(cur.get_u8(), 0); // topic tagged fields
        assert_eq!(cur.get_i32(), 100); // limit
        // cursor present: compact string then i32 then tagged fields
        let cursor_name_v = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(cursor_name_v, 3); // len("t1") + 1
        cur.advance(2); // "t1"
        assert_eq!(cur.get_i32(), 5); // partition_index
        assert_eq!(cur.get_u8(), 0); // cursor tagged fields
        assert_eq!(cur.get_u8(), 0); // top-level tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn test_describe_topic_partitions_response_decode_v0_null_cursor() {
        let mut buf = BytesMut::new();
        buf.put_i32(15); // throttle_time_ms
        varint::encode_unsigned_varint(2, &mut buf); // 1 topic
        buf.put_i16(0); // error_code
        put_compact_string(&mut buf, Some("tp")); // topic name
        buf.put_slice(&[0u8; 16]); // topic_id
        buf.put_i8(0); // is_internal = false
        varint::encode_unsigned_varint(2, &mut buf); // 1 partition
        // partition
        buf.put_i16(0); // error_code
        buf.put_i32(0); // partition_index
        buf.put_i32(0); // leader_id
        buf.put_i32(1); // leader_epoch
        varint::encode_unsigned_varint(2, &mut buf); // 1 replica
        buf.put_i32(0);
        varint::encode_unsigned_varint(2, &mut buf); // 1 isr
        buf.put_i32(0);
        varint::encode_unsigned_varint(0, &mut buf); // ELR: null
        varint::encode_unsigned_varint(0, &mut buf); // last_known_elr: null
        varint::encode_unsigned_varint(1, &mut buf); // offline_replicas: empty
        put_tagged_fields(&mut buf); // partition tagged fields
        buf.put_i32(0); // topic_authorized_operations
        put_tagged_fields(&mut buf); // topic tagged fields
        buf.put_u8(0xFF); // null cursor
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = DescribeTopicPartitionsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 15);
        assert_eq!(resp.topics.len(), 1);
        let t = &resp.topics[0];
        assert!(t.error_code.is_ok());
        assert_eq!(t.name.as_deref(), Some("tp"));
        assert!(!t.is_internal);
        assert_eq!(t.partitions.len(), 1);
        let p = &t.partitions[0];
        assert_eq!(p.partition_index, 0);
        assert_eq!(p.leader_id, 0);
        assert_eq!(p.leader_epoch, 1);
        assert_eq!(p.replica_nodes, vec![0]);
        assert_eq!(p.isr_nodes, vec![0]);
        assert!(p.eligible_leader_replicas.is_none());
        assert!(p.last_known_elr.is_none());
        assert!(p.offline_replicas.is_empty());
        assert!(resp.next_cursor.is_none());
    }

    #[test]
    fn test_describe_topic_partitions_response_decode_v0_with_cursor() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(1, &mut buf); // 0 topics
        // cursor present
        put_compact_string(&mut buf, Some("next")); // cursor topic_name
        buf.put_i32(10); // cursor partition_index
        put_tagged_fields(&mut buf); // cursor tagged fields
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = DescribeTopicPartitionsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert!(resp.topics.is_empty());
        assert!(resp.next_cursor.is_some());
        let cursor = resp.next_cursor.as_ref().unwrap();
        assert_eq!(cursor.topic_name, "next");
        assert_eq!(cursor.partition_index, 10);
    }
}
