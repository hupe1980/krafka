use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
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
                    return Err(KrafkaError::protocol_kind(
                        ProtocolErrorKind::TruncatedFrame,
                        "not enough bytes for topic_id UUID",
                    ));
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use bytes::BytesMut;
    use rstest::rstest;

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
}
