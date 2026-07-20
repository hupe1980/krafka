use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode};
use crate::error::{ErrorCode, ProtocolErrorKind, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{check_compact_array_len, decode_capacity, encode_compact_array_len};
use crate::util::varint::decode_unsigned_varint;

// ============================================================================
// DescribeQuorum API (Key 55)
//
// v0 baseline (flexible encoding).
// v1 adds LastFetchTimestamp + LastCaughtUpTimestamp in ReplicaState (KIP-836).
// v2 adds ErrorMessage, Nodes, ReplicaDirectoryId (KIP-853).
// ============================================================================

/// Partition to describe in the quorum request.
#[derive(Debug, Clone)]
pub struct DescribeQuorumPartitionRequest {
    /// Partition index.
    pub partition_index: i32,
}

/// Topic to describe in the quorum request.
#[derive(Debug, Clone)]
pub struct DescribeQuorumTopicRequest {
    /// Topic name.
    pub topic_name: String,
    /// Partitions to describe.
    pub partitions: Vec<DescribeQuorumPartitionRequest>,
}

/// DescribeQuorum request (API key 55).
#[derive(Debug, Clone)]
pub struct DescribeQuorumRequest {
    /// Topics to describe.
    pub topics: Vec<DescribeQuorumTopicRequest>,
}

impl DescribeQuorumRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::DescribeQuorum
    }

    /// Encode for version 0 (flexible encoding).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            KafkaString::new(&topic.topic_name).try_encode_compact(buf)?;
            encode_compact_array_len(topic.partitions.len(), buf)?;
            for partition in &topic.partitions {
                partition.partition_index.encode(buf);
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for DescribeQuorumRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf),
            _ => unsupported_encode!("DescribeQuorumRequest", version),
        }
    }
}

/// State of a replica (voter or observer) in the quorum.
#[derive(Debug, Clone)]
pub struct QuorumReplicaState {
    /// Replica broker ID.
    pub replica_id: i32,
    /// Last known log end offset, or -1 if unknown.
    pub log_end_offset: i64,
}

/// Per-partition data in the quorum response.
#[derive(Debug, Clone)]
pub struct DescribeQuorumPartitionResponse {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition error code.
    pub error_code: ErrorCode,
    /// Leader broker ID, or -1 if unknown.
    pub leader_id: i32,
    /// Latest known leader epoch.
    pub leader_epoch: i32,
    /// High watermark offset.
    pub high_watermark: i64,
    /// Current voters.
    pub current_voters: Vec<QuorumReplicaState>,
    /// Observers.
    pub observers: Vec<QuorumReplicaState>,
}

/// Per-topic data in the quorum response.
#[derive(Debug, Clone)]
pub struct DescribeQuorumTopicResponse {
    /// Topic name.
    pub topic_name: String,
    /// Per-partition data.
    pub partitions: Vec<DescribeQuorumPartitionResponse>,
}

/// DescribeQuorum response (API key 55).
#[derive(Debug, Clone)]
pub struct DescribeQuorumResponse {
    /// Top-level error code.
    pub error_code: ErrorCode,
    /// Topics data.
    pub topics: Vec<DescribeQuorumTopicResponse>,
}

impl DescribeQuorumResponse {
    /// Decode replica state array for v0.
    fn decode_replica_states_v0(buf: &mut impl Buf) -> Result<Vec<QuorumReplicaState>> {
        let count = check_compact_array_len(decode_unsigned_varint(buf)?)? as usize;
        let mut states = Vec::with_capacity(decode_capacity(count, buf.remaining()));
        for _ in 0..count {
            let replica_id = i32::decode(buf)?;
            let log_end_offset = i64::decode(buf)?;
            TaggedFields::decode(buf)?;
            states.push(QuorumReplicaState {
                replica_id,
                log_end_offset,
            });
        }
        Ok(states)
    }

    /// Decode from version 0 (flexible encoding).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from(i16::decode(buf)?);
        let topic_count = check_compact_array_len(decode_unsigned_varint(buf)?)? as usize;
        let mut topics = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));
        for _ in 0..topic_count {
            let topic_name = {
                let len = decode_unsigned_varint(buf)? as usize;
                if len < 1 {
                    return Err(crate::error::KrafkaError::protocol_kind(
                        ProtocolErrorKind::Malformed,
                        "compact string length 0 is null but field is non-nullable",
                    ));
                }
                let str_len = len - 1;
                if buf.remaining() < str_len {
                    return Err(crate::error::KrafkaError::protocol_kind(
                        ProtocolErrorKind::TruncatedFrame,
                        "not enough bytes for compact string",
                    ));
                }
                let bytes = buf.copy_to_bytes(str_len);
                String::from_utf8(bytes.to_vec()).map_err(|e| {
                    crate::error::KrafkaError::protocol_kind(
                        ProtocolErrorKind::InvalidUtf8,
                        format!("invalid UTF-8: {e}"),
                    )
                })?
            };
            let partition_count = check_compact_array_len(decode_unsigned_varint(buf)?)? as usize;
            let mut partitions =
                Vec::with_capacity(decode_capacity(partition_count, buf.remaining()));
            for _ in 0..partition_count {
                let partition_index = i32::decode(buf)?;
                let partition_error_code = ErrorCode::from(i16::decode(buf)?);
                let leader_id = i32::decode(buf)?;
                let leader_epoch = i32::decode(buf)?;
                let high_watermark = i64::decode(buf)?;
                let current_voters = Self::decode_replica_states_v0(buf)?;
                let observers = Self::decode_replica_states_v0(buf)?;
                TaggedFields::decode(buf)?;
                partitions.push(DescribeQuorumPartitionResponse {
                    partition_index,
                    error_code: partition_error_code,
                    leader_id,
                    leader_epoch,
                    high_watermark,
                    current_voters,
                    observers,
                });
            }
            TaggedFields::decode(buf)?;
            topics.push(DescribeQuorumTopicResponse {
                topic_name,
                partitions,
            });
        }
        TaggedFields::decode(buf)?;
        Ok(Self { error_code, topics })
    }
}

impl VersionedDecode for DescribeQuorumResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("DescribeQuorumResponse", version),
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
    fn describe_quorum_request_encode_v0() {
        let request = DescribeQuorumRequest {
            topics: vec![DescribeQuorumTopicRequest {
                topic_name: "__cluster_metadata".to_string(),
                partitions: vec![DescribeQuorumPartitionRequest { partition_index: 0 }],
            }],
        };

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn describe_quorum_response_decode_v0() {
        let mut buf = BytesMut::new();

        // error_code: NONE
        buf.put_i16(0);
        // topics compact array: 1 element
        buf.put_u8(2);
        // topic name
        let name = b"__cluster_metadata";
        buf.put_u8((name.len() + 1) as u8);
        buf.put_slice(name);
        // partitions compact array: 1 element
        buf.put_u8(2);
        // partition_index
        buf.put_i32(0);
        // partition error_code
        buf.put_i16(0);
        // leader_id
        buf.put_i32(1);
        // leader_epoch
        buf.put_i32(5);
        // high_watermark
        buf.put_i64(100);
        // current_voters: 2 elements
        buf.put_u8(3);
        // voter 1
        buf.put_i32(1);
        buf.put_i64(100);
        buf.put_u8(0); // tagged fields
        // voter 2
        buf.put_i32(2);
        buf.put_i64(98);
        buf.put_u8(0); // tagged fields
        // observers: 1 element
        buf.put_u8(2);
        buf.put_i32(3);
        buf.put_i64(95);
        buf.put_u8(0); // tagged fields
        // partition tagged fields
        buf.put_u8(0);
        // topic tagged fields
        buf.put_u8(0);
        // top-level tagged fields
        buf.put_u8(0);

        let mut read_buf = buf.freeze();
        let response = DescribeQuorumResponse::decode_v0(&mut read_buf).unwrap();

        assert!(response.error_code.is_ok());
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].topic_name, "__cluster_metadata");

        let partition = &response.topics[0].partitions[0];
        assert_eq!(partition.partition_index, 0);
        assert!(partition.error_code.is_ok());
        assert_eq!(partition.leader_id, 1);
        assert_eq!(partition.leader_epoch, 5);
        assert_eq!(partition.high_watermark, 100);
        assert_eq!(partition.current_voters.len(), 2);
        assert_eq!(partition.current_voters[0].replica_id, 1);
        assert_eq!(partition.current_voters[0].log_end_offset, 100);
        assert_eq!(partition.current_voters[1].replica_id, 2);
        assert_eq!(partition.current_voters[1].log_end_offset, 98);
        assert_eq!(partition.observers.len(), 1);
        assert_eq!(partition.observers[0].replica_id, 3);
        assert_eq!(partition.observers[0].log_end_offset, 95);
    }

    #[test]
    fn describe_quorum_request_roundtrip_v0() {
        let request = DescribeQuorumRequest {
            topics: vec![DescribeQuorumTopicRequest {
                topic_name: "__cluster_metadata".to_string(),
                partitions: vec![
                    DescribeQuorumPartitionRequest { partition_index: 0 },
                    DescribeQuorumPartitionRequest { partition_index: 1 },
                ],
            }],
        };

        let mut buf = BytesMut::new();
        request.encode_versioned(0, &mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn describe_quorum_response_empty_topics() {
        let mut buf = BytesMut::new();
        // error_code
        buf.put_i16(0);
        // empty topics array
        buf.put_u8(1);
        // tagged fields
        buf.put_u8(0);

        let mut read_buf = buf.freeze();
        let response = DescribeQuorumResponse::decode_v0(&mut read_buf).unwrap();
        assert!(response.topics.is_empty());
    }

    #[test]
    fn describe_quorum_versioned_encode_dispatch() {
        let request = DescribeQuorumRequest { topics: vec![] };

        let mut buf = BytesMut::new();
        request.encode_versioned(0, &mut buf).unwrap();

        let mut buf2 = BytesMut::new();
        assert!(request.encode_versioned(99, &mut buf2).is_err());
    }

    #[test]
    fn describe_quorum_versioned_decode_dispatch() {
        let mut buf = BytesMut::new();
        buf.put_i16(0); // error_code
        buf.put_u8(1); // empty topics
        buf.put_u8(0); // tagged fields

        let mut read_buf = buf.freeze();
        DescribeQuorumResponse::decode_versioned(0, &mut read_buf).unwrap();

        let mut empty = BytesMut::new().freeze();
        assert!(DescribeQuorumResponse::decode_versioned(99, &mut empty).is_err());
    }

    #[test]
    fn describe_quorum_response_with_error() {
        let mut buf = BytesMut::new();
        // error_code: UNKNOWN_SERVER_ERROR (-1 is_ok false)
        buf.put_i16(-1);
        // empty topics
        buf.put_u8(1);
        // tagged fields
        buf.put_u8(0);

        let mut read_buf = buf.freeze();
        let response = DescribeQuorumResponse::decode_v0(&mut read_buf).unwrap();
        assert!(!response.error_code.is_ok());
    }

    #[test]
    fn describe_quorum_response_empty_voters_and_observers() {
        let mut buf = BytesMut::new();

        buf.put_i16(0); // error_code
        buf.put_u8(2); // 1 topic
        let name = b"t";
        buf.put_u8((name.len() + 1) as u8);
        buf.put_slice(name);
        buf.put_u8(2); // 1 partition
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code
        buf.put_i32(1); // leader_id
        buf.put_i32(0); // leader_epoch
        buf.put_i64(0); // high_watermark
        buf.put_u8(1); // empty current_voters
        buf.put_u8(1); // empty observers
        buf.put_u8(0); // partition tagged fields
        buf.put_u8(0); // topic tagged fields
        buf.put_u8(0); // top-level tagged fields

        let mut read_buf = buf.freeze();
        let response = DescribeQuorumResponse::decode_v0(&mut read_buf).unwrap();
        let partition = &response.topics[0].partitions[0];
        assert!(partition.current_voters.is_empty());
        assert!(partition.observers.is_empty());
    }
}
