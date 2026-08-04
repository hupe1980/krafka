use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
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
// v2 adds ErrorMessage (top level and per partition), a top-level Nodes array,
// and ReplicaDirectoryId in ReplicaState (KIP-853).
//
// The *request* is unchanged across all three versions; every addition is
// response-side.
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
            // v1 and v2 are request-wire-identical to v0: KIP-836 and KIP-853
            // both only added fields to the *response*.
            0..=2 => self.encode_v0(buf),
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
    /// Leader wall-clock time of this follower's most recent fetch (v1+,
    /// KIP-836), or `-1` when unknown — which includes every v0 response and
    /// the leader's own entry.
    ///
    /// Together with [`last_caught_up_timestamp`](Self::last_caught_up_timestamp)
    /// this is what turns `DescribeQuorum` from "who is in the quorum" into
    /// "which voter is falling behind, and since when" — the reason KIP-836
    /// exists.
    pub last_fetch_timestamp: i64,
    /// Leader wall-clock append time of the offset this follower last fetched
    /// (v1+, KIP-836), or `-1` when unknown.
    pub last_caught_up_timestamp: i64,
    /// Directory UUID of the replica's log directory (v2+, KIP-853), or `None`
    /// below v2 where the field does not exist on the wire.
    ///
    /// KRaft voters are identified by `(replica_id, directory_id)` from
    /// KIP-853 onwards, so that a node re-created with the same ID but a fresh
    /// disk is not mistaken for the original. A reconfiguration tool that
    /// ignores this can remove the wrong voter.
    pub replica_directory_id: Option<[u8; 16]>,
}

/// A listener endpoint advertised by a quorum node (v2+, KIP-853).
#[derive(Debug, Clone)]
pub struct QuorumListener {
    /// Listener name, e.g. `CONTROLLER`.
    pub name: String,
    /// Host name.
    pub host: String,
    /// Port. Encoded on the wire as `uint16`, so always in `0..=65535`.
    pub port: u16,
}

/// A node in the quorum, with the endpoints it can be reached on (v2+, KIP-853).
///
/// Empty below v2, where `DescribeQuorum` reported replica IDs but no way to
/// contact them — leaving a client that wanted to talk to a specific voter to
/// cross-reference `DescribeCluster` and hope the two agreed.
#[derive(Debug, Clone)]
pub struct QuorumNode {
    /// Node ID, matching a `replica_id` in the voter/observer lists.
    pub node_id: i32,
    /// Listener endpoints for this node.
    pub listeners: Vec<QuorumListener>,
}

/// Per-partition data in the quorum response.
#[derive(Debug, Clone)]
pub struct DescribeQuorumPartitionResponse {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition error code.
    pub error_code: ErrorCode,
    /// Per-partition error message (v2+, KIP-853), or `None` when the broker
    /// sent none — which includes every v0/v1 response.
    pub error_message: Option<String>,
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
    /// Top-level error message (v2+, KIP-853), or `None` when the broker sent
    /// none.
    pub error_message: Option<String>,
    /// Topics data.
    pub topics: Vec<DescribeQuorumTopicResponse>,
    /// Quorum nodes and their endpoints (v2+, KIP-853). Empty below v2.
    pub nodes: Vec<QuorumNode>,
}

impl DescribeQuorumResponse {
    /// Decode a replica-state array.
    ///
    /// `has_timestamps` selects the v1+ layout, which appends
    /// `LastFetchTimestamp` and `LastCaughtUpTimestamp` to each entry *before*
    /// its tagged fields. Reading them at v0 would consume the next entry's
    /// bytes, so the flag is threaded from the version rather than guessed.
    fn decode_replica_states(buf: &mut impl Buf, version: i16) -> Result<Vec<QuorumReplicaState>> {
        let count = check_compact_array_len(decode_unsigned_varint(buf)?)? as usize;
        let mut states = Vec::with_capacity(decode_capacity(count, buf.remaining()));
        for _ in 0..count {
            let replica_id = i32::decode(buf)?;
            // KIP-853 slots ReplicaDirectoryId immediately after ReplicaId and
            // *before* LogEndOffset. Reading it in the wrong place would shift
            // every subsequent field of every subsequent entry.
            let replica_directory_id = if version >= 2 {
                Some(read_uuid(buf)?)
            } else {
                None
            };
            let log_end_offset = i64::decode(buf)?;
            let (last_fetch_timestamp, last_caught_up_timestamp) = if version >= 1 {
                (i64::decode(buf)?, i64::decode(buf)?)
            } else {
                (-1, -1)
            };
            TaggedFields::decode(buf)?;
            states.push(QuorumReplicaState {
                replica_id,
                log_end_offset,
                last_fetch_timestamp,
                last_caught_up_timestamp,
                replica_directory_id,
            });
        }
        Ok(states)
    }

    /// Decode the v2+ `Nodes` array: node ID plus its listener endpoints.
    fn decode_nodes(buf: &mut impl Buf) -> Result<Vec<QuorumNode>> {
        let count = check_compact_array_len(decode_unsigned_varint(buf)?)? as usize;
        let mut nodes = Vec::with_capacity(decode_capacity(count, buf.remaining()));
        for _ in 0..count {
            let node_id = i32::decode(buf)?;
            let listener_count = check_compact_array_len(decode_unsigned_varint(buf)?)? as usize;
            let mut listeners =
                Vec::with_capacity(decode_capacity(listener_count, buf.remaining()));
            for _ in 0..listener_count {
                let name =
                    non_nullable_string("listener name", KafkaString::decode_compact(buf)?.0)?;
                let host =
                    non_nullable_string("listener host", KafkaString::decode_compact(buf)?.0)?;
                // `uint16` on the wire: two bytes, unsigned, no sign extension.
                if buf.remaining() < 2 {
                    return Err(crate::error::KrafkaError::protocol_kind(
                        ProtocolErrorKind::TruncatedFrame,
                        "not enough bytes for listener port",
                    ));
                }
                let port = buf.get_u16();
                TaggedFields::decode(buf)?;
                listeners.push(QuorumListener { name, host, port });
            }
            TaggedFields::decode(buf)?;
            nodes.push(QuorumNode { node_id, listeners });
        }
        Ok(nodes)
    }

    /// Decode from version 0 (flexible encoding).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_inner(buf, 0)
    }

    /// Decode from version 1 (adds KIP-836 replica timestamps).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_inner(buf, 1)
    }

    /// Decode from version 2 (adds KIP-853 error messages, `Nodes`, and
    /// per-replica directory IDs).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_inner(buf, 2)
    }

    fn decode_inner(buf: &mut impl Buf, version: i16) -> Result<Self> {
        let error_code = ErrorCode::from(i16::decode(buf)?);
        let error_message = if version >= 2 {
            KafkaString::decode_compact(buf)?.0
        } else {
            None
        };
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
                // KIP-853 places the per-partition ErrorMessage right after
                // ErrorCode, ahead of LeaderId.
                let partition_error_message = if version >= 2 {
                    KafkaString::decode_compact(buf)?.0
                } else {
                    None
                };
                let leader_id = i32::decode(buf)?;
                let leader_epoch = i32::decode(buf)?;
                let high_watermark = i64::decode(buf)?;
                let current_voters = Self::decode_replica_states(buf, version)?;
                let observers = Self::decode_replica_states(buf, version)?;
                TaggedFields::decode(buf)?;
                partitions.push(DescribeQuorumPartitionResponse {
                    partition_index,
                    error_code: partition_error_code,
                    error_message: partition_error_message,
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
        // The Nodes array sits between the topics array and the top-level
        // tagged fields.
        let nodes = if version >= 2 {
            Self::decode_nodes(buf)?
        } else {
            Vec::new()
        };
        TaggedFields::decode(buf)?;
        Ok(Self {
            error_code,
            error_message,
            topics,
            nodes,
        })
    }
}

/// Read a fixed 16-byte Kafka UUID, erroring rather than panicking on a short
/// buffer.
fn read_uuid(buf: &mut impl Buf) -> Result<[u8; 16]> {
    if buf.remaining() < 16 {
        return Err(crate::error::KrafkaError::protocol_kind(
            ProtocolErrorKind::TruncatedFrame,
            "not enough bytes for replica_directory_id UUID",
        ));
    }
    let mut id = [0u8; 16];
    buf.copy_to_slice(&mut id);
    Ok(id)
}

impl VersionedDecode for DescribeQuorumResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            2 => Self::decode_v2(buf),
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

    /// v1 appends two `int64` timestamps to every `ReplicaState` entry, before
    /// its tagged fields. Decoding a v1 body with the v0 layout would read the
    /// first timestamp byte as a tagged-field count and desynchronise the rest
    /// of the response, so this test walks a two-voter, one-observer body.
    #[test]
    fn describe_quorum_response_decode_v1_timestamps() {
        let mut buf = BytesMut::new();
        buf.put_i16(0); // error_code
        buf.put_u8(2); // 1 topic
        let name = b"__cluster_metadata";
        buf.put_u8((name.len() + 1) as u8);
        buf.put_slice(name);
        buf.put_u8(2); // 1 partition
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // error_code
        buf.put_i32(1); // leader_id
        buf.put_i32(5); // leader_epoch
        buf.put_i64(100); // high_watermark
        // current_voters: 2 entries, each with v1 timestamps
        buf.put_u8(3);
        buf.put_i32(1);
        buf.put_i64(100);
        buf.put_i64(-1); // leader reports -1 for its own last fetch
        buf.put_i64(-1);
        buf.put_u8(0);
        buf.put_i32(2);
        buf.put_i64(98);
        buf.put_i64(1_700_000_000_000);
        buf.put_i64(1_699_999_999_000);
        buf.put_u8(0);
        // observers: 1 entry
        buf.put_u8(2);
        buf.put_i32(3);
        buf.put_i64(95);
        buf.put_i64(1_700_000_000_500);
        buf.put_i64(1_699_999_998_000);
        buf.put_u8(0);
        buf.put_u8(0); // partition tagged fields
        buf.put_u8(0); // topic tagged fields
        buf.put_u8(0); // top-level tagged fields

        let response = DescribeQuorumResponse::decode_versioned(1, &mut buf.freeze())
            .expect("v1 response decodes");
        let partition = &response.topics[0].partitions[0];
        assert_eq!(partition.current_voters.len(), 2);
        assert_eq!(
            partition.current_voters[1].last_fetch_timestamp,
            1_700_000_000_000
        );
        assert_eq!(
            partition.current_voters[1].last_caught_up_timestamp,
            1_699_999_999_000
        );
        assert_eq!(partition.observers.len(), 1);
        assert_eq!(
            partition.observers[0].last_fetch_timestamp,
            1_700_000_000_500
        );
        // The trailing tagged-field bytes were consumed in the right places.
        assert!(response.error_code.is_ok());
    }

    /// A v0 response carries no timestamps, so they decode as the documented
    /// "unknown" sentinel rather than reading into the next field.
    #[test]
    fn describe_quorum_response_v0_timestamps_are_unknown() {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_u8(2);
        let name = b"t";
        buf.put_u8((name.len() + 1) as u8);
        buf.put_slice(name);
        buf.put_u8(2);
        buf.put_i32(0);
        buf.put_i16(0);
        buf.put_i32(1);
        buf.put_i32(0);
        buf.put_i64(0);
        buf.put_u8(2); // 1 voter
        buf.put_i32(1);
        buf.put_i64(10);
        buf.put_u8(0);
        buf.put_u8(1); // no observers
        buf.put_u8(0);
        buf.put_u8(0);
        buf.put_u8(0);

        let response = DescribeQuorumResponse::decode_v0(&mut buf.freeze()).unwrap();
        let voter = &response.topics[0].partitions[0].current_voters[0];
        assert_eq!(voter.last_fetch_timestamp, -1);
        assert_eq!(voter.last_caught_up_timestamp, -1);
    }

    /// v1 is request-wire-identical to v0.
    #[test]
    fn describe_quorum_request_v1_matches_v0() {
        let request = DescribeQuorumRequest {
            topics: vec![DescribeQuorumTopicRequest {
                topic_name: "__cluster_metadata".to_string(),
                partitions: vec![DescribeQuorumPartitionRequest { partition_index: 0 }],
            }],
        };
        let mut v0 = BytesMut::new();
        let mut v1 = BytesMut::new();
        request.encode_versioned(0, &mut v0).unwrap();
        request.encode_versioned(1, &mut v1).unwrap();
        assert_eq!(v0, v1);
    }

    /// v2 (KIP-853) inserts fields in three places at once: a top-level
    /// `ErrorMessage` after the error code, a per-partition `ErrorMessage`
    /// after the partition error code, a 16-byte `ReplicaDirectoryId` between
    /// each replica's ID and its log end offset, and a `Nodes` array between
    /// the topics array and the top-level tagged fields. Getting any one of
    /// them wrong desynchronises everything after it, so this walks a full
    /// body with two voters, one observer and two nodes.
    #[test]
    fn describe_quorum_response_decode_v2_full_body() {
        let mut buf = BytesMut::new();
        buf.put_i16(0); // top-level error_code
        put_compact_null_string(&mut buf); // top-level error_message (v2)
        buf.put_u8(2); // 1 topic
        put_compact_string(&mut buf, "__cluster_metadata");
        buf.put_u8(2); // 1 partition
        buf.put_i32(0); // partition_index
        buf.put_i16(0); // partition error_code
        put_compact_null_string(&mut buf); // partition error_message (v2)
        buf.put_i32(1); // leader_id
        buf.put_i32(5); // leader_epoch
        buf.put_i64(100); // high_watermark
        // current_voters: 2 entries with directory IDs and timestamps
        buf.put_u8(3);
        buf.put_i32(1);
        buf.put_slice(&[0xAA; 16]); // replica_directory_id (v2)
        buf.put_i64(100);
        buf.put_i64(-1);
        buf.put_i64(-1);
        buf.put_u8(0);
        buf.put_i32(2);
        buf.put_slice(&[0xBB; 16]);
        buf.put_i64(98);
        buf.put_i64(1_700_000_000_000);
        buf.put_i64(1_699_999_999_000);
        buf.put_u8(0);
        // observers: 1 entry
        buf.put_u8(2);
        buf.put_i32(3);
        buf.put_slice(&[0xCC; 16]);
        buf.put_i64(95);
        buf.put_i64(1_700_000_000_500);
        buf.put_i64(1_699_999_998_000);
        buf.put_u8(0);
        buf.put_u8(0); // partition tagged fields
        buf.put_u8(0); // topic tagged fields
        // Nodes array (v2): 2 nodes, the first with one listener
        buf.put_u8(3);
        buf.put_i32(1);
        buf.put_u8(2); // 1 listener
        put_compact_string(&mut buf, "CONTROLLER");
        put_compact_string(&mut buf, "broker-1.internal");
        buf.put_u16(9093);
        buf.put_u8(0); // listener tagged fields
        buf.put_u8(0); // node tagged fields
        buf.put_i32(2);
        buf.put_u8(1); // no listeners
        buf.put_u8(0); // node tagged fields
        buf.put_u8(0); // top-level tagged fields

        let response = DescribeQuorumResponse::decode_versioned(2, &mut buf.freeze())
            .expect("v2 response decodes");

        let partition = &response.topics[0].partitions[0];
        assert_eq!(
            partition.leader_id, 1,
            "leader_id must survive the v2 inserts"
        );
        assert_eq!(partition.high_watermark, 100);
        assert_eq!(partition.current_voters.len(), 2);
        assert_eq!(
            partition.current_voters[0].replica_directory_id,
            Some([0xAA; 16])
        );
        assert_eq!(partition.current_voters[1].log_end_offset, 98);
        assert_eq!(
            partition.current_voters[1].last_fetch_timestamp,
            1_700_000_000_000
        );
        assert_eq!(
            partition.observers[0].replica_directory_id,
            Some([0xCC; 16])
        );

        assert_eq!(response.nodes.len(), 2);
        assert_eq!(response.nodes[0].node_id, 1);
        assert_eq!(response.nodes[0].listeners.len(), 1);
        assert_eq!(response.nodes[0].listeners[0].name, "CONTROLLER");
        assert_eq!(response.nodes[0].listeners[0].host, "broker-1.internal");
        assert_eq!(response.nodes[0].listeners[0].port, 9093);
        assert!(response.nodes[1].listeners.is_empty());
    }

    /// A port above `i16::MAX` must decode as an unsigned value. Kafka types
    /// this field `uint16`, so reading it signed would report port 49152 as
    /// -16384.
    #[test]
    fn describe_quorum_v2_port_is_unsigned() {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_compact_null_string(&mut buf);
        buf.put_u8(1); // no topics
        buf.put_u8(2); // 1 node
        buf.put_i32(7);
        buf.put_u8(2); // 1 listener
        put_compact_string(&mut buf, "CONTROLLER");
        put_compact_string(&mut buf, "h");
        buf.put_u16(49152);
        buf.put_u8(0);
        buf.put_u8(0);
        buf.put_u8(0);

        let response = DescribeQuorumResponse::decode_versioned(2, &mut buf.freeze()).unwrap();
        assert_eq!(response.nodes[0].listeners[0].port, 49152);
    }

    /// Below v2 the new fields do not exist on the wire and must decode to
    /// their documented "absent" values rather than reading into the next
    /// field.
    #[test]
    fn describe_quorum_v1_has_no_v2_fields() {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        buf.put_u8(2);
        put_compact_string(&mut buf, "t");
        buf.put_u8(2);
        buf.put_i32(0);
        buf.put_i16(0);
        buf.put_i32(1);
        buf.put_i32(0);
        buf.put_i64(0);
        buf.put_u8(2); // 1 voter
        buf.put_i32(1);
        buf.put_i64(10);
        buf.put_i64(-1);
        buf.put_i64(-1);
        buf.put_u8(0);
        buf.put_u8(1); // no observers
        buf.put_u8(0);
        buf.put_u8(0);
        buf.put_u8(0);

        let response = DescribeQuorumResponse::decode_versioned(1, &mut buf.freeze()).unwrap();
        assert!(response.error_message.is_none());
        assert!(response.nodes.is_empty());
        let partition = &response.topics[0].partitions[0];
        assert!(partition.error_message.is_none());
        assert!(partition.current_voters[0].replica_directory_id.is_none());
    }

    /// All three versions share one request encoding.
    #[test]
    fn describe_quorum_request_v2_matches_v0() {
        let request = DescribeQuorumRequest {
            topics: vec![DescribeQuorumTopicRequest {
                topic_name: "__cluster_metadata".to_string(),
                partitions: vec![DescribeQuorumPartitionRequest { partition_index: 0 }],
            }],
        };
        let mut v0 = BytesMut::new();
        let mut v2 = BytesMut::new();
        request.encode_versioned(0, &mut v0).unwrap();
        request.encode_versioned(2, &mut v2).unwrap();
        assert_eq!(v0, v2);
        let mut v3 = BytesMut::new();
        assert!(request.encode_versioned(3, &mut v3).is_err());
    }

    /// A truncated directory UUID must be a protocol error, not a panic.
    #[test]
    fn describe_quorum_v2_truncated_directory_id_errors() {
        let mut buf = BytesMut::new();
        buf.put_i16(0);
        put_compact_null_string(&mut buf);
        buf.put_u8(2);
        put_compact_string(&mut buf, "t");
        buf.put_u8(2);
        buf.put_i32(0);
        buf.put_i16(0);
        put_compact_null_string(&mut buf);
        buf.put_i32(1);
        buf.put_i32(0);
        buf.put_i64(0);
        buf.put_u8(2); // 1 voter
        buf.put_i32(1);
        buf.put_slice(&[0xAA; 8]); // only half a UUID

        let err = DescribeQuorumResponse::decode_versioned(2, &mut buf.freeze())
            .expect_err("short UUID must error");
        assert!(
            err.to_string().contains("replica_directory_id"),
            "got: {err}"
        );
    }

    fn put_compact_string(buf: &mut BytesMut, s: &str) {
        crate::util::varint::encode_unsigned_varint((s.len() + 1) as u32, buf);
        buf.put_slice(s.as_bytes());
    }

    fn put_compact_null_string(buf: &mut BytesMut) {
        crate::util::varint::encode_unsigned_varint(0, buf);
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
