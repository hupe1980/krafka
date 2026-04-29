use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    check_compact_array_len, check_compact_nullable_array_len, encode_compact_array_len,
};

// ============================================================================
// ListPartitionReassignments API (Key 46)
//
// v0 baseline. All versions use flexible encoding.
// ============================================================================

/// Topic filter for a ListPartitionReassignments request.
#[derive(Debug, Clone)]
pub struct ListPartitionReassignmentsTopic {
    /// Topic name.
    pub name: String,
    /// Partition indexes to list reassignments for.
    pub partition_indexes: Vec<i32>,
}

/// ListPartitionReassignments request.
///
/// When `topics` is `None`, all ongoing reassignments are listed.
#[derive(Debug, Clone)]
pub struct ListPartitionReassignmentsRequest {
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
    /// Topics to list, or `None` for all ongoing reassignments.
    pub topics: Option<Vec<ListPartitionReassignmentsTopic>>,
}

impl ListPartitionReassignmentsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::ListPartitionReassignments
    }

    /// Create a request for all ongoing reassignments.
    pub fn all() -> Self {
        Self {
            timeout_ms: 60_000,
            topics: None,
        }
    }

    /// Encode for version 0 (flexible encoding).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        self.timeout_ms.encode(buf);
        match &self.topics {
            None => {
                // Compact nullable array: 0 means null.
                crate::util::varint::encode_unsigned_varint(0, buf);
            }
            Some(topics) => {
                encode_compact_array_len(topics.len(), buf)?;
                for topic in topics {
                    KafkaString::new(&topic.name).try_encode_compact(buf)?;
                    encode_compact_array_len(topic.partition_indexes.len(), buf)?;
                    for &p in &topic.partition_indexes {
                        p.encode(buf);
                    }
                    TaggedFields::default().try_encode(buf)?;
                }
            }
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for ListPartitionReassignmentsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf),
            _ => unsupported_encode!("ListPartitionReassignmentsRequest", version),
        }
    }
}

// ── Response ─────────────────────────────────────────────────────────────

/// An ongoing partition reassignment.
#[derive(Debug, Clone)]
pub struct OngoingPartitionReassignment {
    /// Partition index.
    pub partition_index: i32,
    /// Current replica set.
    pub replicas: Vec<i32>,
    /// Replicas currently being added.
    pub adding_replicas: Vec<i32>,
    /// Replicas currently being removed.
    pub removing_replicas: Vec<i32>,
}

/// An ongoing topic reassignment.
#[derive(Debug, Clone)]
pub struct OngoingTopicReassignment {
    /// Topic name.
    pub name: String,
    /// Partition reassignments.
    pub partitions: Vec<OngoingPartitionReassignment>,
}

/// ListPartitionReassignments response.
#[derive(Debug, Clone)]
pub struct ListPartitionReassignmentsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code.
    pub error_code: ErrorCode,
    /// Top-level error message, or `None` if no error.
    pub error_message: Option<String>,
    /// Ongoing reassignments per topic.
    pub topics: Vec<OngoingTopicReassignment>,
}

impl ListPartitionReassignmentsResponse {
    /// Decode from version 0 (flexible encoding).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode_compact(buf)?.0;

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
                let replicas = Self::decode_broker_id_array(buf)?;
                let adding_replicas = Self::decode_broker_id_array(buf)?;
                let removing_replicas = Self::decode_broker_id_array(buf)?;
                let _ = TaggedFields::decode(buf)?;
                partitions.push(OngoingPartitionReassignment {
                    partition_index,
                    replicas,
                    adding_replicas,
                    removing_replicas,
                });
            }
            let _ = TaggedFields::decode(buf)?;
            topics.push(OngoingTopicReassignment { name, partitions });
        }
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            topics,
        })
    }

    /// Decode a compact array of broker IDs (i32).
    fn decode_broker_id_array(buf: &mut impl Buf) -> Result<Vec<i32>> {
        let count =
            check_compact_nullable_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut ids = Vec::with_capacity(count);
        for _ in 0..count {
            ids.push(i32::decode(buf)?);
        }
        Ok(ids)
    }
}

impl VersionedDecode for ListPartitionReassignmentsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("ListPartitionReassignmentsResponse", version),
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use bytes::BytesMut;

    #[test]
    fn test_list_partition_reassignments_api_key() {
        assert_eq!(
            ListPartitionReassignmentsRequest::api_key(),
            ApiKey::ListPartitionReassignments
        );
    }

    #[test]
    fn test_list_partition_reassignments_request_all() {
        let request = ListPartitionReassignmentsRequest::all();
        assert!(request.topics.is_none());
        assert_eq!(request.timeout_ms, 60_000);
    }

    #[test]
    fn test_list_partition_reassignments_request_encode_v0_null() {
        let request = ListPartitionReassignmentsRequest::all();
        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        // timeout (4 bytes) + compact nullable null (varint 0) + tagged fields (varint 0)
        assert_eq!(buf.len(), 6);
    }

    #[test]
    fn test_list_partition_reassignments_request_encode_v0_with_topics() {
        let request = ListPartitionReassignmentsRequest {
            timeout_ms: 30_000,
            topics: Some(vec![ListPartitionReassignmentsTopic {
                name: "my-topic".to_string(),
                partition_indexes: vec![0, 1, 2],
            }]),
        };

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_list_partition_reassignments_versioned_unsupported() {
        let request = ListPartitionReassignmentsRequest::all();
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(-1, &mut buf).is_err());
        assert!(request.encode_versioned(1, &mut buf).is_err());
    }

    /// Build a compact (flexible) string: varint(len+1) followed by raw bytes.
    fn put_compact_string(buf: &mut BytesMut, s: &str) {
        crate::util::varint::encode_unsigned_varint((s.len() + 1) as u32, buf);
        buf.put_slice(s.as_bytes());
    }

    /// Write a compact nullable string set to null: varint(0).
    fn put_compact_null_string(buf: &mut BytesMut) {
        crate::util::varint::encode_unsigned_varint(0, buf);
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
    fn test_list_partition_reassignments_response_decode_v0() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(0);
        // error_code = NONE
        buf.put_i16(0);
        // error_message = null
        put_compact_null_string(&mut buf);
        // topics compact array: count = 1
        put_compact_array_len(&mut buf, 1);
        // topic name
        put_compact_string(&mut buf, "my-topic");
        // partitions compact array: count = 1
        put_compact_array_len(&mut buf, 1);
        // partition_index
        buf.put_i32(0);
        // replicas: [1, 2, 3]
        put_compact_array_len(&mut buf, 3);
        buf.put_i32(1);
        buf.put_i32(2);
        buf.put_i32(3);
        // adding_replicas: [3]
        put_compact_array_len(&mut buf, 1);
        buf.put_i32(3);
        // removing_replicas: [1]
        put_compact_array_len(&mut buf, 1);
        buf.put_i32(1);
        // partition tagged fields
        put_empty_tagged_fields(&mut buf);
        // topic tagged fields
        put_empty_tagged_fields(&mut buf);
        // top-level tagged fields
        put_empty_tagged_fields(&mut buf);

        let resp = ListPartitionReassignmentsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 0);
        assert!(resp.error_code.is_ok());
        assert!(resp.error_message.is_none());
        assert_eq!(resp.topics.len(), 1);

        let topic = &resp.topics[0];
        assert_eq!(topic.name, "my-topic");
        assert_eq!(topic.partitions.len(), 1);

        let p = &topic.partitions[0];
        assert_eq!(p.partition_index, 0);
        assert_eq!(p.replicas, vec![1, 2, 3]);
        assert_eq!(p.adding_replicas, vec![3]);
        assert_eq!(p.removing_replicas, vec![1]);
    }

    #[test]
    fn test_list_partition_reassignments_response_empty() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code = NONE
        put_compact_null_string(&mut buf);
        put_compact_array_len(&mut buf, 0); // empty topics
        put_empty_tagged_fields(&mut buf);

        let resp = ListPartitionReassignmentsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert!(resp.error_code.is_ok());
        assert!(resp.topics.is_empty());
    }

    #[test]
    fn test_list_partition_reassignments_response_top_level_error() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i16(31); // error_code = CLUSTER_AUTHORIZATION_FAILED
        put_compact_string(&mut buf, "Not authorized");
        put_compact_array_len(&mut buf, 0); // empty topics
        put_empty_tagged_fields(&mut buf);

        let resp = ListPartitionReassignmentsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert!(!resp.error_code.is_ok());
        assert_eq!(resp.error_message.as_deref(), Some("Not authorized"));
    }

    #[test]
    fn test_list_partition_reassignments_versioned_decode_unsupported() {
        let buf = BytesMut::new();
        assert!(
            ListPartitionReassignmentsResponse::decode_versioned(-1, &mut buf.clone().freeze())
                .is_err()
        );
        assert!(
            ListPartitionReassignmentsResponse::decode_versioned(1, &mut buf.freeze()).is_err()
        );
    }

    #[test]
    fn test_list_partition_reassignments_response_multiple_topics() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        put_compact_null_string(&mut buf);
        // 2 topics
        put_compact_array_len(&mut buf, 2);

        // topic 1
        put_compact_string(&mut buf, "topic-a");
        put_compact_array_len(&mut buf, 1);
        buf.put_i32(0); // partition_index
        put_compact_array_len(&mut buf, 2); // replicas
        buf.put_i32(1);
        buf.put_i32(2);
        put_compact_array_len(&mut buf, 0); // adding
        put_compact_array_len(&mut buf, 0); // removing
        put_empty_tagged_fields(&mut buf); // partition
        put_empty_tagged_fields(&mut buf); // topic

        // topic 2
        put_compact_string(&mut buf, "topic-b");
        put_compact_array_len(&mut buf, 1);
        buf.put_i32(1); // partition_index
        put_compact_array_len(&mut buf, 3); // replicas
        buf.put_i32(1);
        buf.put_i32(2);
        buf.put_i32(3);
        put_compact_array_len(&mut buf, 1); // adding
        buf.put_i32(3);
        put_compact_array_len(&mut buf, 1); // removing
        buf.put_i32(1);
        put_empty_tagged_fields(&mut buf); // partition
        put_empty_tagged_fields(&mut buf); // topic

        // top-level tagged fields
        put_empty_tagged_fields(&mut buf);

        let resp = ListPartitionReassignmentsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.topics.len(), 2);
        assert_eq!(resp.topics[0].name, "topic-a");
        assert_eq!(resp.topics[1].name, "topic-b");
        assert_eq!(resp.topics[1].partitions[0].adding_replicas, vec![3]);
        assert_eq!(resp.topics[1].partitions[0].removing_replicas, vec![1]);
    }
}
