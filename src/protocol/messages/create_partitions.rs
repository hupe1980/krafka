use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, decode_capacity,
    encode_compact_array_len,
};

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
        let mut results = Vec::with_capacity(decode_capacity(result_count, buf.remaining()));

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
        let mut results = Vec::with_capacity(decode_capacity(result_count, buf.remaining()));

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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use bytes::BytesMut;

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
}
