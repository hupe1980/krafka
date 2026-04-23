use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::primitives::{Decode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{check_compact_array_len, encode_compact_array_len};

// ============================================================================
// ConsumerGroupDescribe API (Key 69)
// ============================================================================

/// ConsumerGroupDescribe request (API Key 69). Flexible from v0.
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescribeRequest {
    /// Group IDs to describe.
    pub group_ids: Vec<String>,
    /// Whether to include authorized operations.
    pub include_authorized_operations: bool,
}

impl ConsumerGroupDescribeRequest {
    /// Create a new request.
    pub fn new(group_ids: Vec<String>) -> Self {
        Self {
            group_ids,
            include_authorized_operations: false,
        }
    }

    /// Encode for version 0–1 (flexible from v0; v1 request is same wire format as v0).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.group_ids.len(), buf)?;
        for id in &self.group_ids {
            KafkaString::new(id).try_encode_compact(buf)?;
        }
        buf.put_u8(u8::from(self.include_authorized_operations));
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Topic-partition assignment used in ConsumerGroupDescribe response.
#[derive(Debug, Clone)]
pub struct DescribeGroupTopicPartition {
    /// Topic ID.
    pub topic_id: [u8; 16],
    /// Topic name.
    pub topic_name: String,
    /// Partition indices.
    pub partitions: Vec<i32>,
}

/// Assignment (target or current) for a consumer group member.
#[derive(Debug, Clone)]
pub struct DescribeGroupAssignment {
    /// Topic-partition assignments.
    pub topic_partitions: Vec<DescribeGroupTopicPartition>,
}

/// A member in ConsumerGroupDescribe response.
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescribeMember {
    /// Member ID.
    pub member_id: String,
    /// Instance ID (static membership).
    pub instance_id: Option<String>,
    /// Rack ID.
    pub rack_id: Option<String>,
    /// Current member epoch.
    pub member_epoch: i32,
    /// Client ID.
    pub client_id: String,
    /// Client host.
    pub client_host: String,
    /// Subscribed topic names.
    pub subscribed_topic_names: Vec<String>,
    /// Subscribed topic regex.
    pub subscribed_topic_regex: Option<String>,
    /// Current assignment.
    pub assignment: DescribeGroupAssignment,
    /// Target assignment.
    pub target_assignment: DescribeGroupAssignment,
    /// Member type (v1+). -1=unknown, 0=classic, 1=consumer.
    pub member_type: i8,
}

/// A described group in ConsumerGroupDescribe response.
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescribeGroup {
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// Group ID.
    pub group_id: String,
    /// Group state.
    pub group_state: String,
    /// Group epoch.
    pub group_epoch: i32,
    /// Assignment epoch.
    pub assignment_epoch: i32,
    /// Assignor name.
    pub assignor_name: String,
    /// Members.
    pub members: Vec<ConsumerGroupDescribeMember>,
    /// Authorized operations bitfield.
    pub authorized_operations: i32,
}

/// ConsumerGroupDescribe response (API Key 69). Flexible from v0.
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescribeResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Described groups.
    pub groups: Vec<ConsumerGroupDescribeGroup>,
}

impl ConsumerGroupDescribeResponse {
    /// Decode assignment (shared between current and target).
    fn decode_assignment(buf: &mut impl Buf) -> Result<DescribeGroupAssignment> {
        let tp_count = check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut topic_partitions = Vec::with_capacity(tp_count);
        for _ in 0..tp_count {
            let mut topic_id = [0u8; 16];
            if buf.remaining() < 16 {
                return Err(KrafkaError::protocol("short buf for topic_id"));
            }
            buf.copy_to_slice(&mut topic_id);
            let topic_name =
                non_nullable_string("topic_name", KafkaString::decode_compact(buf)?.0)?;
            let partition_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);
            for _ in 0..partition_count {
                partitions.push(i32::decode(buf)?);
            }
            let _ = TaggedFields::decode(buf)?;
            topic_partitions.push(DescribeGroupTopicPartition {
                topic_id,
                topic_name,
                partitions,
            });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(DescribeGroupAssignment { topic_partitions })
    }

    /// Decode from version 0 (flexible from v0).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            let group_id = non_nullable_string("group_id", KafkaString::decode_compact(buf)?.0)?;
            let group_state =
                non_nullable_string("group_state", KafkaString::decode_compact(buf)?.0)?;
            let group_epoch = i32::decode(buf)?;
            let assignment_epoch = i32::decode(buf)?;
            let assignor_name =
                non_nullable_string("assignor_name", KafkaString::decode_compact(buf)?.0)?;

            let member_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut members = Vec::with_capacity(member_count);
            for _ in 0..member_count {
                let member_id =
                    non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;
                let instance_id = KafkaString::decode_compact(buf)?.0;
                let rack_id = KafkaString::decode_compact(buf)?.0;
                let member_epoch = i32::decode(buf)?;
                let client_id =
                    non_nullable_string("client_id", KafkaString::decode_compact(buf)?.0)?;
                let client_host =
                    non_nullable_string("client_host", KafkaString::decode_compact(buf)?.0)?;

                let sub_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut subscribed_topic_names = Vec::with_capacity(sub_count);
                for _ in 0..sub_count {
                    subscribed_topic_names.push(non_nullable_string(
                        "subscribed_topic",
                        KafkaString::decode_compact(buf)?.0,
                    )?);
                }
                let subscribed_topic_regex = KafkaString::decode_compact(buf)?.0;

                let assignment = Self::decode_assignment(buf)?;
                let target_assignment = Self::decode_assignment(buf)?;

                let _ = TaggedFields::decode(buf)?;
                members.push(ConsumerGroupDescribeMember {
                    member_id,
                    instance_id,
                    rack_id,
                    member_epoch,
                    client_id,
                    client_host,
                    subscribed_topic_names,
                    subscribed_topic_regex,
                    assignment,
                    target_assignment,
                    member_type: -1,
                });
            }

            let authorized_operations = i32::decode(buf)?;
            let _ = TaggedFields::decode(buf)?;

            groups.push(ConsumerGroupDescribeGroup {
                error_code,
                error_message,
                group_id,
                group_state,
                group_epoch,
                assignment_epoch,
                assignor_name,
                members,
                authorized_operations,
            });
        }

        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }

    /// Decode from version 1 (adds member_type per member; KIP-1099).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let group_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut groups = Vec::with_capacity(group_count);

        for _ in 0..group_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            let group_id = non_nullable_string("group_id", KafkaString::decode_compact(buf)?.0)?;
            let group_state =
                non_nullable_string("group_state", KafkaString::decode_compact(buf)?.0)?;
            let group_epoch = i32::decode(buf)?;
            let assignment_epoch = i32::decode(buf)?;
            let assignor_name =
                non_nullable_string("assignor_name", KafkaString::decode_compact(buf)?.0)?;

            let member_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut members = Vec::with_capacity(member_count);
            for _ in 0..member_count {
                let member_id =
                    non_nullable_string("member_id", KafkaString::decode_compact(buf)?.0)?;
                let instance_id = KafkaString::decode_compact(buf)?.0;
                let rack_id = KafkaString::decode_compact(buf)?.0;
                let member_epoch = i32::decode(buf)?;
                let client_id =
                    non_nullable_string("client_id", KafkaString::decode_compact(buf)?.0)?;
                let client_host =
                    non_nullable_string("client_host", KafkaString::decode_compact(buf)?.0)?;

                let sub_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut subscribed_topic_names = Vec::with_capacity(sub_count);
                for _ in 0..sub_count {
                    subscribed_topic_names.push(non_nullable_string(
                        "subscribed_topic",
                        KafkaString::decode_compact(buf)?.0,
                    )?);
                }
                let subscribed_topic_regex = KafkaString::decode_compact(buf)?.0;

                let assignment = Self::decode_assignment(buf)?;
                let target_assignment = Self::decode_assignment(buf)?;
                let member_type = i8::decode(buf)?;

                let _ = TaggedFields::decode(buf)?;
                members.push(ConsumerGroupDescribeMember {
                    member_id,
                    instance_id,
                    rack_id,
                    member_epoch,
                    client_id,
                    client_host,
                    subscribed_topic_names,
                    subscribed_topic_regex,
                    assignment,
                    target_assignment,
                    member_type,
                });
            }

            let authorized_operations = i32::decode(buf)?;
            let _ = TaggedFields::decode(buf)?;

            groups.push(ConsumerGroupDescribeGroup {
                error_code,
                error_message,
                group_id,
                group_state,
                group_epoch,
                assignment_epoch,
                assignor_name,
                members,
                authorized_operations,
            });
        }

        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            groups,
        })
    }
}

impl VersionedEncode for ConsumerGroupDescribeRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 | 1 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("ConsumerGroupDescribeRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for ConsumerGroupDescribeResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            _ => unsupported_decode!("ConsumerGroupDescribeResponse", version),
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
    fn put_compact_string(buf: &mut BytesMut, s: Option<&str>) {
        match s {
            Some(val) => {
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

    #[test]
    fn test_consumer_group_describe_request_encode_v0() {
        let req = ConsumerGroupDescribeRequest::new(vec!["g1".to_string()]);
        let mut buf = BytesMut::new();
        req.encode_v0(&mut buf).unwrap();

        let mut cur = &buf[..];
        let arr = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(arr, 2); // 1 + 1
        let id_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(id_len, 3); // len("g1") + 1
        let mut id_bytes = vec![0u8; 2];
        cur.copy_to_slice(&mut id_bytes);
        assert_eq!(id_bytes, b"g1");
        assert_eq!(cur.get_u8(), 0); // include_authorized_operations = false
        assert_eq!(cur.get_u8(), 0); // tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn test_consumer_group_describe_response_decode_v0_empty_group() {
        let mut buf = BytesMut::new();
        buf.put_i32(5); // throttle_time_ms
        varint::encode_unsigned_varint(2, &mut buf); // 1 group
        buf.put_i16(0); // error_code
        put_compact_string(&mut buf, None); // error_message null
        put_compact_string(&mut buf, Some("g1")); // group_id
        put_compact_string(&mut buf, Some("Stable")); // group_state
        buf.put_i32(1); // group_epoch
        buf.put_i32(1); // assignment_epoch
        put_compact_string(&mut buf, Some("range")); // assignor_name
        varint::encode_unsigned_varint(1, &mut buf); // 0 members
        buf.put_i32(0); // authorized_operations
        put_tagged_fields(&mut buf); // group tagged fields
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = ConsumerGroupDescribeResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 5);
        assert_eq!(resp.groups.len(), 1);
        let g = &resp.groups[0];
        assert!(g.error_code.is_ok());
        assert_eq!(g.group_id, "g1");
        assert_eq!(g.group_state, "Stable");
        assert_eq!(g.group_epoch, 1);
        assert_eq!(g.assignor_name, "range");
        assert!(g.members.is_empty());
    }

    #[test]
    fn test_consumer_group_describe_response_decode_v0_with_member() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(2, &mut buf); // 1 group
        buf.put_i16(0); // error_code
        put_compact_string(&mut buf, None); // error_message
        put_compact_string(&mut buf, Some("g")); // group_id
        put_compact_string(&mut buf, Some("S")); // group_state
        buf.put_i32(2); // group_epoch
        buf.put_i32(2); // assignment_epoch
        put_compact_string(&mut buf, Some("a")); // assignor_name
        varint::encode_unsigned_varint(2, &mut buf); // 1 member
        // member fields
        put_compact_string(&mut buf, Some("m1")); // member_id
        put_compact_string(&mut buf, None); // instance_id null
        put_compact_string(&mut buf, Some("rack0")); // rack_id
        buf.put_i32(3); // member_epoch
        put_compact_string(&mut buf, Some("client")); // client_id
        put_compact_string(&mut buf, Some("/127.0.0.1")); // client_host
        varint::encode_unsigned_varint(2, &mut buf); // 1 subscribed_topic_name
        put_compact_string(&mut buf, Some("tp1"));
        put_compact_string(&mut buf, None); // subscribed_topic_regex null
        // assignment (current): 1 topic_partition
        varint::encode_unsigned_varint(2, &mut buf); // 1 tp
        buf.put_slice(&[0u8; 16]); // topic_id
        put_compact_string(&mut buf, Some("tp1")); // topic_name
        varint::encode_unsigned_varint(3, &mut buf); // 2 partitions
        buf.put_i32(0);
        buf.put_i32(1);
        put_tagged_fields(&mut buf); // tp tagged fields
        put_tagged_fields(&mut buf); // assignment tagged fields
        // target_assignment: empty
        varint::encode_unsigned_varint(1, &mut buf); // 0 tp
        put_tagged_fields(&mut buf); // target assignment tagged fields
        put_tagged_fields(&mut buf); // member tagged fields
        buf.put_i32(-2_147_483_648); // authorized_operations
        put_tagged_fields(&mut buf); // group tagged fields
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = ConsumerGroupDescribeResponse::decode_v0(&mut buf.freeze()).unwrap();
        let g = &resp.groups[0];
        assert_eq!(g.members.len(), 1);
        let m = &g.members[0];
        assert_eq!(m.member_id, "m1");
        assert!(m.instance_id.is_none());
        assert_eq!(m.rack_id.as_deref(), Some("rack0"));
        assert_eq!(m.member_epoch, 3);
        assert_eq!(m.subscribed_topic_names, vec!["tp1"]);
        assert_eq!(m.assignment.topic_partitions.len(), 1);
        assert_eq!(m.assignment.topic_partitions[0].partitions, vec![0, 1]);
        assert!(m.target_assignment.topic_partitions.is_empty());
        assert_eq!(m.member_type, -1); // v0 default
    }

    #[test]
    fn test_consumer_group_describe_response_decode_v1_member_type() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(2, &mut buf); // 1 group
        buf.put_i16(0); // error_code
        put_compact_string(&mut buf, None); // error_message
        put_compact_string(&mut buf, Some("g")); // group_id
        put_compact_string(&mut buf, Some("S")); // state
        buf.put_i32(1);
        buf.put_i32(1); // epochs
        put_compact_string(&mut buf, Some("a")); // assignor
        varint::encode_unsigned_varint(2, &mut buf); // 1 member
        put_compact_string(&mut buf, Some("m")); // member_id
        put_compact_string(&mut buf, None); // instance_id
        put_compact_string(&mut buf, None); // rack_id
        buf.put_i32(1); // member_epoch
        put_compact_string(&mut buf, Some("c")); // client_id
        put_compact_string(&mut buf, Some("h")); // client_host
        varint::encode_unsigned_varint(1, &mut buf); // 0 subscribed topics
        put_compact_string(&mut buf, None); // subscribed_topic_regex
        // current assignment: empty
        varint::encode_unsigned_varint(1, &mut buf);
        put_tagged_fields(&mut buf);
        // target assignment: empty
        varint::encode_unsigned_varint(1, &mut buf);
        put_tagged_fields(&mut buf);
        buf.put_i8(1); // member_type = consumer (v1 field)
        put_tagged_fields(&mut buf); // member tagged fields
        buf.put_i32(0); // authorized_operations
        put_tagged_fields(&mut buf); // group tagged fields
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = ConsumerGroupDescribeResponse::decode_v1(&mut buf.freeze()).unwrap();
        let m = &resp.groups[0].members[0];
        assert_eq!(m.member_type, 1); // consumer
    }
}
