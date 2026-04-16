use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, encode_compact_array_len,
};

// ============================================================================
// ElectLeaders API (Key 43)
//
// v0 baseline (preferred election only).
// v1 adds ElectionType for preferred or unclean election (KIP-460).
//    Also adds a top-level ErrorCode in the response.
// v2 flexible encoding (compact strings + tagged fields).
// ============================================================================

/// Type of leader election to perform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
#[repr(i8)]
pub enum ElectionType {
    /// Elect the preferred replica as leader.
    Preferred = 0,
    /// Elect the first live replica as leader even if there are no in-sync
    /// replicas (unclean election).
    Unclean = 1,
}

/// Topic-partitions filter for an ElectLeaders request.
#[derive(Debug, Clone)]
pub struct ElectLeadersTopicPartitions {
    /// Topic name.
    pub topic: String,
    /// Partition indexes to elect leaders for.
    pub partitions: Vec<i32>,
}

/// ElectLeaders request.
///
/// When `topic_partitions` is `None`, leaders for all eligible partitions are
/// elected.
#[derive(Debug, Clone)]
pub struct ElectLeadersRequest {
    /// Type of election (v1+). Ignored for v0 (always preferred).
    pub election_type: ElectionType,
    /// Partitions to elect leaders for, or `None` for all.
    pub topic_partitions: Option<Vec<ElectLeadersTopicPartitions>>,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
}

impl ElectLeadersRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::ElectLeaders
    }

    /// Encode for version 0 (non-flexible, no election type).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        Self::encode_topic_partitions_v0(&self.topic_partitions, buf)?;
        self.timeout_ms.encode(buf);
        Ok(())
    }

    /// Encode for version 1 (non-flexible, with election type).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        (self.election_type as i8).encode(buf);
        Self::encode_topic_partitions_v0(&self.topic_partitions, buf)?;
        self.timeout_ms.encode(buf);
        Ok(())
    }

    /// Encode for version 2 (flexible encoding, with election type).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        (self.election_type as i8).encode(buf);
        match &self.topic_partitions {
            None => {
                // Compact nullable array: 0 means null.
                crate::util::varint::encode_unsigned_varint(0, buf);
            }
            Some(topics) => {
                encode_compact_array_len(topics.len(), buf)?;
                for tp in topics {
                    KafkaString::new(&tp.topic).try_encode_compact(buf)?;
                    encode_compact_array_len(tp.partitions.len(), buf)?;
                    for &p in &tp.partitions {
                        p.encode(buf);
                    }
                    TaggedFields::default().try_encode(buf)?;
                }
            }
        }
        self.timeout_ms.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode topic-partitions for non-flexible versions (v0-v1).
    fn encode_topic_partitions_v0(
        topic_partitions: &Option<Vec<ElectLeadersTopicPartitions>>,
        buf: &mut impl BufMut,
    ) -> Result<()> {
        match topic_partitions {
            None => {
                buf.put_i32(-1); // nullable array: null
            }
            Some(topics) => {
                buf.put_i32(array_len_i32(topics.len())?);
                for tp in topics {
                    KafkaString::new(&tp.topic).try_encode(buf)?;
                    buf.put_i32(array_len_i32(tp.partitions.len())?);
                    for &p in &tp.partitions {
                        p.encode(buf);
                    }
                }
            }
        }
        Ok(())
    }
}

impl VersionedEncode for ElectLeadersRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf),
            1 => self.encode_v1(buf),
            2 => self.encode_v2(buf),
            _ => unsupported_encode!("ElectLeadersRequest", version),
        }
    }
}

// ── Response ─────────────────────────────────────────────────────────────

/// Per-partition election result.
#[derive(Debug, Clone)]
pub struct ElectLeadersPartitionResult {
    /// Partition ID.
    pub partition_id: i32,
    /// Error code for this partition.
    pub error_code: ErrorCode,
    /// Error message, or `None` if successful.
    pub error_message: Option<String>,
}

/// Per-topic election result.
#[derive(Debug, Clone)]
pub struct ElectLeadersTopicResult {
    /// Topic name.
    pub topic: String,
    /// Partition results.
    pub partition_results: Vec<ElectLeadersPartitionResult>,
}

/// ElectLeaders response.
#[derive(Debug, Clone)]
pub struct ElectLeadersResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code (v1+). `ErrorCode::None` for v0.
    pub error_code: ErrorCode,
    /// Per-topic election results.
    pub replica_election_results: Vec<ElectLeadersTopicResult>,
}

impl ElectLeadersResponse {
    /// Decode from version 0 (non-flexible, no top-level error).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let replica_election_results = Self::decode_topics_v0(buf, topic_count)?;

        Ok(Self {
            throttle_time_ms,
            error_code: ErrorCode::None,
            replica_election_results,
        })
    }

    /// Decode from version 1 (non-flexible, top-level error).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let replica_election_results = Self::decode_topics_v0(buf, topic_count)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            replica_election_results,
        })
    }

    /// Decode from version 2 (flexible encoding, top-level error).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let replica_election_results = Self::decode_topics_v2(buf, topic_count)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            replica_election_results,
        })
    }

    /// Decode topic results (non-flexible, v0-v1).
    fn decode_topics_v0(buf: &mut impl Buf, count: usize) -> Result<Vec<ElectLeadersTopicResult>> {
        let mut topics = Vec::with_capacity(count);
        for _ in 0..count {
            let topic = non_nullable_string("topic", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let partition_results = Self::decode_partitions_v0(buf, partition_count)?;
            topics.push(ElectLeadersTopicResult {
                topic,
                partition_results,
            });
        }
        Ok(topics)
    }

    /// Decode topic results (flexible, v2).
    fn decode_topics_v2(buf: &mut impl Buf, count: usize) -> Result<Vec<ElectLeadersTopicResult>> {
        let mut topics = Vec::with_capacity(count);
        for _ in 0..count {
            let topic = non_nullable_string("topic", KafkaString::decode_compact(buf)?.0)?;
            let partition_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let partition_results = Self::decode_partitions_v2(buf, partition_count)?;
            let _ = TaggedFields::decode(buf)?;
            topics.push(ElectLeadersTopicResult {
                topic,
                partition_results,
            });
        }
        Ok(topics)
    }

    /// Decode partition results (non-flexible).
    fn decode_partitions_v0(
        buf: &mut impl Buf,
        count: usize,
    ) -> Result<Vec<ElectLeadersPartitionResult>> {
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            let partition_id = i32::decode(buf)?;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;
            partitions.push(ElectLeadersPartitionResult {
                partition_id,
                error_code,
                error_message,
            });
        }
        Ok(partitions)
    }

    /// Decode partition results (flexible).
    fn decode_partitions_v2(
        buf: &mut impl Buf,
        count: usize,
    ) -> Result<Vec<ElectLeadersPartitionResult>> {
        let mut partitions = Vec::with_capacity(count);
        for _ in 0..count {
            let partition_id = i32::decode(buf)?;
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            let _ = TaggedFields::decode(buf)?;
            partitions.push(ElectLeadersPartitionResult {
                partition_id,
                error_code,
                error_message,
            });
        }
        Ok(partitions)
    }
}

impl VersionedDecode for ElectLeadersResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            2 => Self::decode_v2(buf),
            _ => unsupported_decode!("ElectLeadersResponse", version),
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
    fn test_elect_leaders_request_api_key() {
        assert_eq!(ElectLeadersRequest::api_key(), ApiKey::ElectLeaders);
    }

    #[test]
    fn test_elect_leaders_request_v0_all() {
        let request = ElectLeadersRequest {
            election_type: ElectionType::Preferred,
            topic_partitions: None,
            timeout_ms: 60_000,
        };

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        // null array (-1) + timeout (60000)
        assert_eq!(i32::from_be_bytes(buf[0..4].try_into().unwrap()), -1);
        assert_eq!(i32::from_be_bytes(buf[4..8].try_into().unwrap()), 60_000);
    }

    #[test]
    fn test_elect_leaders_request_v1_with_topics() {
        let request = ElectLeadersRequest {
            election_type: ElectionType::Unclean,
            topic_partitions: Some(vec![ElectLeadersTopicPartitions {
                topic: "my-topic".to_string(),
                partitions: vec![0, 1],
            }]),
            timeout_ms: 30_000,
        };

        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        // First byte: election_type = 1 (Unclean)
        assert_eq!(buf[0], 1);
    }

    #[test]
    fn test_elect_leaders_request_v2_null() {
        let request = ElectLeadersRequest {
            election_type: ElectionType::Preferred,
            topic_partitions: None,
            timeout_ms: 10_000,
        };

        let mut buf = BytesMut::new();
        request.encode_v2(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_elect_leaders_versioned_encode_unsupported() {
        let request = ElectLeadersRequest {
            election_type: ElectionType::Preferred,
            topic_partitions: None,
            timeout_ms: 60_000,
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(-1, &mut buf).is_err());
        assert!(request.encode_versioned(3, &mut buf).is_err());
    }

    #[test]
    fn test_elect_leaders_response_decode_v0() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(0);
        // topic count = 1
        buf.put_i32(1);
        // topic name
        let topic = "my-topic";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic.as_bytes());
        // partition count = 1
        buf.put_i32(1);
        // partition_id
        buf.put_i32(0);
        // error_code = NONE
        buf.put_i16(0);
        // error_message = null
        buf.put_i16(-1);

        let resp = ElectLeadersResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 0);
        assert_eq!(resp.error_code, ErrorCode::None);
        assert_eq!(resp.replica_election_results.len(), 1);

        let topic_result = &resp.replica_election_results[0];
        assert_eq!(topic_result.topic, "my-topic");
        assert_eq!(topic_result.partition_results.len(), 1);

        let p = &topic_result.partition_results[0];
        assert_eq!(p.partition_id, 0);
        assert!(p.error_code.is_ok());
        assert!(p.error_message.is_none());
    }

    #[test]
    fn test_elect_leaders_response_decode_v1_with_error() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(100);
        // top-level error_code = ELECTION_NOT_NEEDED (84)
        buf.put_i16(84);
        // topic count = 1
        buf.put_i32(1);
        // topic name
        let topic = "test";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic.as_bytes());
        // partition count = 1
        buf.put_i32(1);
        // partition_id
        buf.put_i32(2);
        // error_code = ELECTION_NOT_NEEDED
        buf.put_i16(84);
        // error_message
        let msg = "Election not needed";
        buf.put_i16(msg.len() as i16);
        buf.put_slice(msg.as_bytes());

        let resp = ElectLeadersResponse::decode_v1(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 100);
        assert!(!resp.error_code.is_ok());
        assert_eq!(
            resp.replica_election_results[0].partition_results[0].partition_id,
            2
        );
        assert_eq!(
            resp.replica_election_results[0].partition_results[0]
                .error_message
                .as_deref(),
            Some("Election not needed")
        );
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
    fn test_elect_leaders_response_decode_v2() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(0);
        // top-level error_code = NONE
        buf.put_i16(0);
        // topics compact array: count = 1
        put_compact_array_len(&mut buf, 1);
        // topic name
        put_compact_string(&mut buf, "flex-topic");
        // partitions compact array: count = 1
        put_compact_array_len(&mut buf, 1);
        // partition_id
        buf.put_i32(5);
        // error_code = NONE
        buf.put_i16(0);
        // error_message = null (compact nullable: 0)
        crate::util::varint::encode_unsigned_varint(0, &mut buf);
        // partition tagged fields
        put_empty_tagged_fields(&mut buf);
        // topic tagged fields
        put_empty_tagged_fields(&mut buf);
        // top-level tagged fields
        put_empty_tagged_fields(&mut buf);

        let resp = ElectLeadersResponse::decode_v2(&mut buf.freeze()).unwrap();
        assert_eq!(resp.error_code, ErrorCode::None);
        assert_eq!(resp.replica_election_results.len(), 1);
        assert_eq!(resp.replica_election_results[0].topic, "flex-topic");
        assert_eq!(
            resp.replica_election_results[0].partition_results[0].partition_id,
            5
        );
        assert!(
            resp.replica_election_results[0].partition_results[0]
                .error_code
                .is_ok()
        );
        assert!(
            resp.replica_election_results[0].partition_results[0]
                .error_message
                .is_none()
        );
    }

    #[test]
    fn test_elect_leaders_versioned_decode_unsupported() {
        let buf = BytesMut::new();
        assert!(ElectLeadersResponse::decode_versioned(-1, &mut buf.clone().freeze()).is_err());
        assert!(ElectLeadersResponse::decode_versioned(3, &mut buf.freeze()).is_err());
    }

    #[test]
    fn test_elect_leaders_request_v0_v1_field_order() {
        // v0 has no election type byte; v1 prepends it.
        let request = ElectLeadersRequest {
            election_type: ElectionType::Preferred,
            topic_partitions: None,
            timeout_ms: 60_000,
        };

        let mut buf_v0 = BytesMut::new();
        request.encode_v0(&mut buf_v0).unwrap();

        let mut buf_v1 = BytesMut::new();
        request.encode_v1(&mut buf_v1).unwrap();

        // v1 has one extra byte (election_type) at the front
        assert_eq!(buf_v1.len(), buf_v0.len() + 1);
        assert_eq!(buf_v1[0], 0); // Preferred = 0
        assert_eq!(&buf_v1[1..], &buf_v0[..]);
    }

    #[test]
    fn test_elect_leaders_response_decode_v0_multiple_topics() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i32(2); // topic count

        // topic 1
        let t1 = "topic-a";
        buf.put_i16(t1.len() as i16);
        buf.put_slice(t1.as_bytes());
        buf.put_i32(0); // no partitions

        // topic 2
        let t2 = "topic-b";
        buf.put_i16(t2.len() as i16);
        buf.put_slice(t2.as_bytes());
        buf.put_i32(0); // no partitions

        let resp = ElectLeadersResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.replica_election_results.len(), 2);
        assert_eq!(resp.replica_election_results[0].topic, "topic-a");
        assert_eq!(resp.replica_election_results[1].topic, "topic-b");
    }

    #[test]
    fn test_elect_leaders_versioned_encode_dispatches() {
        let request = ElectLeadersRequest {
            election_type: ElectionType::Unclean,
            topic_partitions: Some(vec![ElectLeadersTopicPartitions {
                topic: "t".to_string(),
                partitions: vec![0],
            }]),
            timeout_ms: 5_000,
        };

        // All three versions should succeed.
        for v in 0..=2 {
            let mut buf = BytesMut::new();
            request.encode_versioned(v, &mut buf).unwrap();
            assert!(!buf.is_empty(), "v{v} should produce non-empty output");
        }
    }
}
