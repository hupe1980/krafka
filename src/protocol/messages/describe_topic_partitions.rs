use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{check_compact_array_len, decode_capacity, encode_compact_array_len};

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
        // Nullable struct in a flexible version: a single signed presence byte
        // precedes the struct. `-1` means null; `1` means the struct fields
        // follow. The presence byte is written in *both* branches — omitting it
        // for the present case desynchronises the broker's reader.
        match &self.cursor {
            None => {
                buf.put_i8(-1);
            }
            Some(c) => {
                buf.put_i8(1);
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
    /// Decode the trailing nullable `next_cursor` struct and the response-level
    /// tagged fields.
    ///
    /// Non-tagged nullable structs in flexible versions use a single signed
    /// byte as a presence marker: a negative value (the broker writes `-1`)
    /// means null, `1` means the struct fields follow. This mirrors the Kafka
    /// generator's reader:
    /// `if (_readable.readByte() < 0) { … null … } else { … read struct … }`.
    ///
    /// An exhausted buffer is a truncated frame, not a well-formed final page:
    /// the marker is mandatory, so a response that ends before it is malformed.
    fn decode_next_cursor(buf: &mut impl Buf) -> Result<Option<DescribeTopicPartitionsCursor>> {
        if buf.remaining() < 1 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::TruncatedFrame,
                "not enough bytes for next_cursor presence tag",
            ));
        }
        let presence = buf.get_i8();
        if presence < 0 {
            // Null cursor — only the response-level tagged fields remain.
            let _ = TaggedFields::decode(buf)?;
            return Ok(None);
        }
        if presence != 1 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::Malformed,
                format!(
                    "invalid next_cursor presence tag: expected negative for null or 1 for present, got {presence}"
                ),
            ));
        }

        let topic_name = non_nullable_string("cursor topic", KafkaString::decode_compact(buf)?.0)?;
        let partition_index = i32::decode(buf)?;
        // Cursor struct tagged fields, then response-level tagged fields.
        let _ = TaggedFields::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;
        Ok(Some(DescribeTopicPartitionsCursor {
            topic_name,
            partition_index,
        }))
    }

    /// Helper: decode compact nullable i32 array.
    fn decode_compact_nullable_i32_array(buf: &mut impl Buf) -> Result<Option<Vec<i32>>> {
        let raw = crate::util::varint::decode_unsigned_varint(buf)?;
        if raw == 0 {
            return Ok(None);
        }
        let count = check_compact_array_len(raw)?;
        let mut arr = Vec::with_capacity(decode_capacity(count, buf.remaining()));
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
        let mut topics = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));

        for _ in 0..topic_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let name = KafkaString::decode_compact(buf)?.0;
            let mut topic_id = [0u8; 16];
            if buf.remaining() < 16 {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::TruncatedFrame,
                    "short buf for topic_id",
                ));
            }
            buf.copy_to_slice(&mut topic_id);
            let is_internal = i8::decode(buf)? != 0;

            let part_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(decode_capacity(part_count, buf.remaining()));
            for _ in 0..part_count {
                let p_error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let partition_index = i32::decode(buf)?;
                let leader_id = i32::decode(buf)?;
                let leader_epoch = i32::decode(buf)?;

                let replica_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut replica_nodes =
                    Vec::with_capacity(decode_capacity(replica_count, buf.remaining()));
                for _ in 0..replica_count {
                    replica_nodes.push(i32::decode(buf)?);
                }

                let isr_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut isr_nodes = Vec::with_capacity(decode_capacity(isr_count, buf.remaining()));
                for _ in 0..isr_count {
                    isr_nodes.push(i32::decode(buf)?);
                }

                let eligible_leader_replicas = Self::decode_compact_nullable_i32_array(buf)?;
                let last_known_elr = Self::decode_compact_nullable_i32_array(buf)?;

                let offline_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut offline_replicas =
                    Vec::with_capacity(decode_capacity(offline_count, buf.remaining()));
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

        // Decode next_cursor (nullable struct).
        //
        // Non-tagged nullable structs in flexible versions are prefixed with a
        // single signed presence byte: negative (the broker writes `-1`) means
        // null, `1` means the struct fields follow. The presence byte must be
        // consumed in *both* branches — reading the struct starting at the
        // marker would parse `0x01` as a compact-string length and shift every
        // subsequent field.
        let next_cursor = Self::decode_next_cursor(buf)?;

        Ok(Self {
            throttle_time_ms,
            topics,
            next_cursor,
        })
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
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use crate::util::varint;
    use bytes::BytesMut;

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
        assert_eq!(cur.get_i8(), -1); // null cursor presence byte
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
        // Nullable struct: presence byte 1, then compact string, i32, tagged fields.
        assert_eq!(cur.get_i8(), 1, "present cursor must be prefixed with 1");
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
        buf.put_i8(-1); // null cursor presence byte
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
        // cursor present: presence byte 1, then the struct fields
        buf.put_i8(1);
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

    // ── Regression: nullable-struct presence byte ──────────────────────

    /// The presence byte must be consumed before the struct fields.
    ///
    /// Previously the non-null branch began decoding `topic_name` *at* the
    /// marker, so `0x01` was read as a compact-string length (yielding an empty
    /// name) and `partition_index` then consumed the first four bytes of the
    /// real topic name. Pagination silently walked the wrong cursor.
    #[test]
    fn decode_v0_cursor_does_not_consume_presence_byte_as_string_len() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(1, &mut buf); // 0 topics
        buf.put_i8(1); // cursor present
        put_compact_string(&mut buf, Some("orders"));
        buf.put_i32(7);
        put_tagged_fields(&mut buf); // cursor tagged fields
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = DescribeTopicPartitionsResponse::decode_v0(&mut buf.freeze()).unwrap();
        let cursor = resp.next_cursor.expect("cursor must be present");
        assert_eq!(cursor.topic_name, "orders");
        assert_eq!(cursor.partition_index, 7);
    }

    /// Encode and decode must agree on the wire format for a present cursor.
    ///
    /// The request encoder's trailing bytes (the nullable cursor struct plus
    /// the message-level tagged fields) are byte-for-byte what the response
    /// decoder expects in the same position, so feeding one to the other is a
    /// true round trip through both encode and decode.
    #[test]
    fn cursor_encode_decode_agree_on_presence_byte() {
        let req = DescribeTopicPartitionsRequest {
            topics: vec![],
            response_partition_limit: 0,
            cursor: Some(DescribeTopicPartitionsCursor {
                topic_name: "events".to_string(),
                partition_index: 42,
            }),
        };
        let mut enc = BytesMut::new();
        req.encode_v0(&mut enc).unwrap();

        // Skip the request-only prefix: the empty topics compact array (one
        // varint byte) and `response_partition_limit` (four bytes).
        let mut cur = &enc[..];
        assert_eq!(varint::decode_unsigned_varint(&mut cur).unwrap(), 1);
        assert_eq!(cur.get_i32(), 0);
        let cursor_and_tail = cur.to_vec();

        // The first of those bytes must be the presence marker.
        assert_eq!(
            cursor_and_tail[0], 1,
            "present cursor must be prefixed with 1"
        );

        let mut resp_buf = BytesMut::new();
        resp_buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(1, &mut resp_buf); // 0 topics
        resp_buf.put_slice(&cursor_and_tail);

        let resp = DescribeTopicPartitionsResponse::decode_v0(&mut resp_buf.freeze()).unwrap();
        let cursor = resp
            .next_cursor
            .expect("cursor must survive the round trip");
        assert_eq!(cursor.topic_name, "events");
        assert_eq!(cursor.partition_index, 42);
    }

    /// An exhausted buffer is a truncated frame, not a well-formed final page.
    #[test]
    fn decode_v0_missing_cursor_marker_is_truncated_frame() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(1, &mut buf); // 0 topics
        // ...and nothing else: the presence byte is missing.

        let err = DescribeTopicPartitionsResponse::decode_v0(&mut buf.freeze()).unwrap_err();
        assert!(
            format!("{err}").contains("presence tag"),
            "expected a truncated-frame error, got: {err}"
        );
    }

    /// A presence byte that is neither negative nor exactly 1 is malformed.
    #[test]
    fn decode_v0_rejects_bogus_cursor_marker() {
        let mut buf = BytesMut::new();
        buf.put_i32(0);
        varint::encode_unsigned_varint(1, &mut buf); // 0 topics
        buf.put_i8(7); // neither -1 nor 1
        assert!(DescribeTopicPartitionsResponse::decode_v0(&mut buf.freeze()).is_err());
    }
}
