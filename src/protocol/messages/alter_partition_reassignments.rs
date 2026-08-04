use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{check_compact_array_len, decode_capacity, encode_compact_array_len};

// ============================================================================
// AlterPartitionReassignments API (Key 45)
//
// v0 baseline, v1 adds AllowReplicationFactorChange (Kafka 4.1).
// All versions use flexible encoding.
// ============================================================================

/// A partition reassignment specification.
#[derive(Debug, Clone)]
pub struct ReassignablePartition {
    /// Partition index.
    pub partition_index: i32,
    /// Target replica set, or `None` to cancel a pending reassignment.
    pub replicas: Option<Vec<i32>>,
}

/// A topic with partitions to reassign.
#[derive(Debug, Clone)]
pub struct ReassignableTopic {
    /// Topic name.
    pub name: String,
    /// Partitions to reassign.
    pub partitions: Vec<ReassignablePartition>,
}

/// AlterPartitionReassignments request.
#[derive(Debug, Clone)]
pub struct AlterPartitionReassignmentsRequest {
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
    /// Whether a reassignment may change a partition's replication factor
    /// (v1+, Kafka 4.1).
    ///
    /// `true` — the broker default and the v0 behaviour — accepts a target
    /// replica set of a different size than the current one, silently changing
    /// the topic's replication factor.
    ///
    /// `false` asks the broker to reject any such move with
    /// `INVALID_REPLICATION_FACTOR` instead. That is the safer choice for a
    /// rebalance tool: an off-by-one in a generated reassignment plan then
    /// fails loudly rather than quietly reducing durability.
    ///
    /// Ignored when the negotiated version is v0, where the field does not
    /// exist on the wire; see
    /// [`AdminClient::alter_partition_reassignments`](crate::admin::AdminClient::alter_partition_reassignments).
    pub allow_replication_factor_change: bool,
    /// Topics to reassign.
    pub topics: Vec<ReassignableTopic>,
}

impl AlterPartitionReassignmentsRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::AlterPartitionReassignments
    }

    /// Encode for version 0 (flexible encoding).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode_inner(buf, false)
    }

    /// Encode for version 1 (adds `AllowReplicationFactorChange`).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode_inner(buf, true)
    }

    fn encode_inner(
        &self,
        buf: &mut impl BufMut,
        include_allow_replication_factor_change: bool,
    ) -> Result<()> {
        self.timeout_ms.encode(buf);
        if include_allow_replication_factor_change {
            self.allow_replication_factor_change.encode(buf);
        }
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            KafkaString::new(&topic.name).try_encode_compact(buf)?;
            encode_compact_array_len(topic.partitions.len(), buf)?;
            for partition in &topic.partitions {
                partition.partition_index.encode(buf);
                match &partition.replicas {
                    None => {
                        // Compact nullable array: 0 means null.
                        crate::util::varint::encode_unsigned_varint(0, buf);
                    }
                    Some(replicas) => {
                        encode_compact_array_len(replicas.len(), buf)?;
                        for &r in replicas {
                            r.encode(buf);
                        }
                    }
                }
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for AlterPartitionReassignmentsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf),
            1 => self.encode_v1(buf),
            _ => unsupported_encode!("AlterPartitionReassignmentsRequest", version),
        }
    }
}

// ── Response ─────────────────────────────────────────────────────────────

/// Per-partition reassignment result.
#[derive(Debug, Clone)]
pub struct ReassignablePartitionResponse {
    /// Partition index.
    pub partition_index: i32,
    /// Error code for this partition.
    pub error_code: ErrorCode,
    /// Error message, or `None` if successful.
    pub error_message: Option<String>,
}

/// Per-topic reassignment result.
#[derive(Debug, Clone)]
pub struct ReassignableTopicResponse {
    /// Topic name.
    pub name: String,
    /// Partition results.
    pub partitions: Vec<ReassignablePartitionResponse>,
}

/// AlterPartitionReassignments response.
#[derive(Debug, Clone)]
pub struct AlterPartitionReassignmentsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code.
    pub error_code: ErrorCode,
    /// Top-level error message, or `None` if no error.
    pub error_message: Option<String>,
    /// v1+: the `AllowReplicationFactorChange` value the broker actually
    /// applied. Always `true` at v0, where the field does not exist and the
    /// broker's behaviour is unconditionally "allowed".
    pub allow_replication_factor_change: bool,
    /// Per-topic results.
    pub responses: Vec<ReassignableTopicResponse>,
}

impl AlterPartitionReassignmentsResponse {
    /// Decode from version 0 (flexible encoding).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_inner(buf, false)
    }

    /// Decode from version 1 (adds `AllowReplicationFactorChange`).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_inner(buf, true)
    }

    fn decode_inner(buf: &mut impl Buf, has_allow_replication_factor_change: bool) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let allow_replication_factor_change = if has_allow_replication_factor_change {
            bool::decode(buf)?
        } else {
            true
        };
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode_compact(buf)?.0;

        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut responses = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));
        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let partition_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions =
                Vec::with_capacity(decode_capacity(partition_count, buf.remaining()));
            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let error_message = KafkaString::decode_compact(buf)?.0;
                let _ = TaggedFields::decode(buf)?;
                partitions.push(ReassignablePartitionResponse {
                    partition_index,
                    error_code,
                    error_message,
                });
            }
            let _ = TaggedFields::decode(buf)?;
            responses.push(ReassignableTopicResponse { name, partitions });
        }
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            allow_replication_factor_change,
            responses,
        })
    }
}

impl VersionedDecode for AlterPartitionReassignmentsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            _ => unsupported_decode!("AlterPartitionReassignmentsResponse", version),
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
    fn test_alter_partition_reassignments_api_key() {
        assert_eq!(
            AlterPartitionReassignmentsRequest::api_key(),
            ApiKey::AlterPartitionReassignments
        );
    }

    #[test]
    fn test_alter_partition_reassignments_request_encode_v0() {
        let request = AlterPartitionReassignmentsRequest {
            timeout_ms: 60_000,
            allow_replication_factor_change: true,
            topics: vec![ReassignableTopic {
                name: "my-topic".to_string(),
                partitions: vec![
                    ReassignablePartition {
                        partition_index: 0,
                        replicas: Some(vec![1, 2, 3]),
                    },
                    ReassignablePartition {
                        partition_index: 1,
                        replicas: None, // cancel
                    },
                ],
            }],
        };

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_alter_partition_reassignments_versioned_unsupported() {
        let request = AlterPartitionReassignmentsRequest {
            timeout_ms: 60_000,
            allow_replication_factor_change: true,
            topics: Vec::new(),
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(-1, &mut buf).is_err());
        assert!(request.encode_versioned(2, &mut buf).is_err());
    }

    /// v1 inserts `AllowReplicationFactorChange` immediately after `TimeoutMs`
    /// and before the topics array. Getting that order wrong would shift every
    /// subsequent field and make the broker reject the request.
    #[test]
    fn test_alter_partition_reassignments_request_encode_v1_field_order() {
        let request = AlterPartitionReassignmentsRequest {
            timeout_ms: 30_000,
            allow_replication_factor_change: false,
            topics: Vec::new(),
        };

        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(i32::decode(&mut cur).unwrap(), 30_000);
        assert!(
            !bool::decode(&mut cur).unwrap(),
            "AllowReplicationFactorChange must follow TimeoutMs"
        );
        // Empty compact topics array is varint(1).
        assert_eq!(
            crate::util::varint::decode_unsigned_varint(&mut cur).unwrap(),
            1
        );
        // Top-level tagged fields.
        assert_eq!(
            crate::util::varint::decode_unsigned_varint(&mut cur).unwrap(),
            0
        );
        assert!(cur.is_empty());
    }

    /// v0 must not emit the v1-only field — a v0 broker would read it as the
    /// leading byte of the topics array.
    #[test]
    fn test_alter_partition_reassignments_request_v0_omits_v1_field() {
        let request = AlterPartitionReassignmentsRequest {
            timeout_ms: 30_000,
            allow_replication_factor_change: false,
            topics: Vec::new(),
        };
        let mut v0 = BytesMut::new();
        let mut v1 = BytesMut::new();
        request.encode_v0(&mut v0).unwrap();
        request.encode_v1(&mut v1).unwrap();
        assert_eq!(v1.len(), v0.len() + 1, "v1 adds exactly one bool byte");
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
    fn test_alter_partition_reassignments_response_decode_v0() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(0);
        // error_code = NONE
        buf.put_i16(0);
        // error_message = null
        put_compact_null_string(&mut buf);
        // responses compact array: count = 1
        put_compact_array_len(&mut buf, 1);
        // topic name
        put_compact_string(&mut buf, "my-topic");
        // partitions compact array: count = 2
        put_compact_array_len(&mut buf, 2);
        // partition 0: success
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code = NONE
        put_compact_null_string(&mut buf); // error_message
        put_empty_tagged_fields(&mut buf); // partition tagged fields
        // partition 1: error
        buf.put_i32(1); // partition_index
        buf.put_i16(87); // error_code
        put_compact_string(&mut buf, "Reassignment in progress");
        put_empty_tagged_fields(&mut buf); // partition tagged fields
        // topic tagged fields
        put_empty_tagged_fields(&mut buf);
        // top-level tagged fields
        put_empty_tagged_fields(&mut buf);

        let resp = AlterPartitionReassignmentsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 0);
        assert!(resp.error_code.is_ok());
        assert!(resp.error_message.is_none());
        assert_eq!(resp.responses.len(), 1);

        let topic = &resp.responses[0];
        assert_eq!(topic.name, "my-topic");
        assert_eq!(topic.partitions.len(), 2);
        assert!(topic.partitions[0].error_code.is_ok());
        assert!(!topic.partitions[1].error_code.is_ok());
        assert_eq!(
            topic.partitions[1].error_message.as_deref(),
            Some("Reassignment in progress")
        );
    }

    #[test]
    fn test_alter_partition_reassignments_response_top_level_error() {
        let mut buf = BytesMut::new();
        buf.put_i32(50); // throttle_time_ms
        buf.put_i16(31); // error_code = CLUSTER_AUTHORIZATION_FAILED
        put_compact_string(&mut buf, "Not authorized");
        // empty responses
        put_compact_array_len(&mut buf, 0);
        put_empty_tagged_fields(&mut buf); // top-level tagged fields

        let resp = AlterPartitionReassignmentsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert!(!resp.error_code.is_ok());
        assert_eq!(resp.error_message.as_deref(), Some("Not authorized"));
        assert!(resp.responses.is_empty());
    }

    #[test]
    fn test_alter_partition_reassignments_versioned_decode_unsupported() {
        let buf = BytesMut::new();
        assert!(
            AlterPartitionReassignmentsResponse::decode_versioned(-1, &mut buf.clone().freeze())
                .is_err()
        );
        assert!(
            AlterPartitionReassignmentsResponse::decode_versioned(2, &mut buf.freeze()).is_err()
        );
    }

    /// v1 echoes the applied `AllowReplicationFactorChange` right after
    /// `ThrottleTimeMs`, ahead of the error code.
    #[test]
    fn test_alter_partition_reassignments_response_decode_v1() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(0); // allow_replication_factor_change = false
        buf.put_i16(0); // error_code = NONE
        put_compact_null_string(&mut buf); // error_message
        put_compact_array_len(&mut buf, 0); // responses
        put_empty_tagged_fields(&mut buf);

        let resp = AlterPartitionReassignmentsResponse::decode_versioned(1, &mut buf.freeze())
            .expect("v1 response decodes");
        assert!(!resp.allow_replication_factor_change);
        assert!(resp.error_code.is_ok());
    }

    /// At v0 the field is absent from the wire; the decoded value reports the
    /// broker's actual v0 behaviour, which is "changes allowed".
    #[test]
    fn test_alter_partition_reassignments_response_v0_defaults_allow_true() {
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        buf.put_i16(0);
        put_compact_null_string(&mut buf);
        put_compact_array_len(&mut buf, 0);
        put_empty_tagged_fields(&mut buf);

        let resp = AlterPartitionReassignmentsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert!(resp.allow_replication_factor_change);
    }

    #[test]
    fn test_alter_partition_reassignments_request_empty_topics() {
        let request = AlterPartitionReassignmentsRequest {
            timeout_ms: 30_000,
            allow_replication_factor_change: true,
            topics: Vec::new(),
        };
        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        // timeout (4 bytes) + compact array len=0 (varint 1) + tagged fields (varint 0)
        assert!(!buf.is_empty());
    }
}
