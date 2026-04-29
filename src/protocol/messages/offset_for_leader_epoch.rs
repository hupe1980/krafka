use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{array_len_i32, check_compact_array_len, check_decode_array_len};

// ============================================================================
// OffsetForLeaderEpoch API (Key 23)
// ============================================================================

/// Partition in OffsetForLeaderEpoch request.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochPartition {
    /// Partition index.
    pub partition: i32,
    /// Current leader epoch (v2+, for fencing).
    pub current_leader_epoch: i32,
    /// Requested leader epoch.
    pub leader_epoch: i32,
}

/// Topic in OffsetForLeaderEpoch request.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochTopic {
    /// Topic name.
    pub topic: String,
    /// Partitions.
    pub partitions: Vec<OffsetForLeaderEpochPartition>,
}

/// OffsetForLeaderEpoch request.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochRequest {
    /// Replica ID (-1 for consumers, broker ID for followers).
    pub replica_id: i32,
    /// Topics.
    pub topics: Vec<OffsetForLeaderEpochTopic>,
}

impl OffsetForLeaderEpochRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::OffsetForLeaderEpoch
    }

    /// Encode for version 2 (adds current_leader_epoch per partition for fencing).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.topic).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.current_leader_epoch.encode(buf);
                partition.leader_epoch.encode(buf);
            }
        }
        Ok(())
    }

    /// Encode for version 3 (adds replica_id field).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        self.replica_id.encode(buf);
        buf.put_i32(array_len_i32(self.topics.len())?);
        for topic in &self.topics {
            KafkaString::new(&topic.topic).try_encode(buf)?;
            buf.put_i32(array_len_i32(topic.partitions.len())?);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.current_leader_epoch.encode(buf);
                partition.leader_epoch.encode(buf);
            }
        }
        Ok(())
    }

    /// Encode for version 4 (flexible: compact strings/arrays + tagged fields).
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        self.replica_id.encode(buf);
        let topics_len = u32::try_from(self.topics.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("topics array too large"))?;
        crate::util::varint::encode_unsigned_varint(topics_len, buf);
        for topic in &self.topics {
            KafkaString::new(&topic.topic).try_encode_compact(buf)?;
            let parts_len = u32::try_from(topic.partitions.len().saturating_add(1))
                .map_err(|_| KrafkaError::protocol("partitions array too large"))?;
            crate::util::varint::encode_unsigned_varint(parts_len, buf);
            for partition in &topic.partitions {
                partition.partition.encode(buf);
                partition.current_leader_epoch.encode(buf);
                partition.leader_epoch.encode(buf);
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Partition in OffsetForLeaderEpoch response.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochPartitionResult {
    /// Error code.
    pub error_code: ErrorCode,
    /// Partition index.
    pub partition: i32,
    /// Leader epoch.
    pub leader_epoch: i32,
    /// End offset for the requested leader epoch.
    pub end_offset: i64,
}

/// Topic in OffsetForLeaderEpoch response.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochTopicResult {
    /// Topic name.
    pub topic: String,
    /// Partitions.
    pub partitions: Vec<OffsetForLeaderEpochPartitionResult>,
}

/// OffsetForLeaderEpoch response.
#[derive(Debug, Clone)]
pub struct OffsetForLeaderEpochResponse {
    /// Throttle time (v2+).
    pub throttle_time_ms: i32,
    /// Topics.
    pub topics: Vec<OffsetForLeaderEpochTopicResult>,
}

impl OffsetForLeaderEpochResponse {
    /// Decode from version 2-3 (non-flexible, adds throttle_time_ms header).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let topic = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let partition = i32::decode(buf)?;
                let leader_epoch = i32::decode(buf)?;
                let end_offset = i64::decode(buf)?;
                partitions.push(OffsetForLeaderEpochPartitionResult {
                    error_code,
                    partition,
                    leader_epoch,
                    end_offset,
                });
            }

            topics.push(OffsetForLeaderEpochTopicResult { topic, partitions });
        }

        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }

    /// Decode from version 4 (flexible: compact strings/arrays + tagged fields).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut topics = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let topic = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let partition_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let partition = i32::decode(buf)?;
                let leader_epoch = i32::decode(buf)?;
                let end_offset = i64::decode(buf)?;
                let _ = TaggedFields::decode(buf)?;
                partitions.push(OffsetForLeaderEpochPartitionResult {
                    error_code,
                    partition,
                    leader_epoch,
                    end_offset,
                });
            }
            let _ = TaggedFields::decode(buf)?;
            topics.push(OffsetForLeaderEpochTopicResult { topic, partitions });
        }
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            topics,
        })
    }
}

// ---------------------------------------------------------------------------
// VersionedEncode / VersionedDecode implementations
// ---------------------------------------------------------------------------

impl VersionedEncode for OffsetForLeaderEpochRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            2 => self.encode_v2(buf)?,
            3 => self.encode_v3(buf)?,
            4 => self.encode_v4(buf)?,
            _ => return unsupported_encode!("OffsetForLeaderEpochRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for OffsetForLeaderEpochResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            2..=3 => Self::decode_v2(buf),
            4 => Self::decode_v4(buf),
            _ => unsupported_decode!("OffsetForLeaderEpochResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use rstest::rstest;

    #[test]
    fn test_offset_for_leader_epoch_request() {
        let request = OffsetForLeaderEpochRequest {
            replica_id: -1,
            topics: vec![OffsetForLeaderEpochTopic {
                topic: "my-topic".to_string(),
                partitions: vec![OffsetForLeaderEpochPartition {
                    partition: 0,
                    current_leader_epoch: 5,
                    leader_epoch: 4,
                }],
            }],
        };
        assert_eq!(
            OffsetForLeaderEpochRequest::api_key(),
            ApiKey::OffsetForLeaderEpoch
        );

        let mut buf = BytesMut::new();
        request.encode_v2(&mut buf).unwrap();
        assert!(!buf.is_empty());

        let mut buf3 = BytesMut::new();
        request.encode_v3(&mut buf3).unwrap();
        // v3 includes replica_id prefix, so it should be longer
        assert!(buf3.len() > buf.len());
    }

    #[test]
    fn test_offset_for_leader_epoch_response_decode_v2() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();

        // throttle_time_ms (v2 adds this)
        buf.put_i32(50);
        // topic_count = 1
        buf.put_i32(1);
        // topic name
        let topic = b"my-topic";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        // partition_count = 1
        buf.put_i32(1);
        // error_code
        buf.put_i16(0);
        // partition
        buf.put_i32(0);
        // leader_epoch
        buf.put_i32(5);
        // end_offset
        buf.put_i64(1000);

        let mut data = buf.freeze();
        let resp = OffsetForLeaderEpochResponse::decode_v2(&mut data).unwrap();

        assert_eq!(resp.throttle_time_ms, 50);
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].topic, "my-topic");
        assert_eq!(resp.topics[0].partitions.len(), 1);
        assert!(resp.topics[0].partitions[0].error_code.is_ok());
        assert_eq!(resp.topics[0].partitions[0].partition, 0);
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, 5);
        assert_eq!(resp.topics[0].partitions[0].end_offset, 1000);
    }

    #[rstest]
    // OffsetForLeaderEpoch MIN=2
    #[case::ofl_v0(0)]
    #[case::ofl_v1(1)]
    fn test_offset_for_leader_epoch_encode_below_min(#[case] version: i16) {
        let request = OffsetForLeaderEpochRequest {
            replica_id: -1,
            topics: vec![],
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }

    // ── OffsetForLeaderEpoch v4 flexible round-trip ───────────────────

    #[test]
    fn test_offset_for_leader_epoch_request_encode_v4_flexible() {
        let request = OffsetForLeaderEpochRequest {
            replica_id: -1,
            topics: vec![OffsetForLeaderEpochTopic {
                topic: "my-topic".to_string(),
                partitions: vec![OffsetForLeaderEpochPartition {
                    partition: 0,
                    current_leader_epoch: 5,
                    leader_epoch: 3,
                }],
            }],
        };

        let mut buf_v4 = BytesMut::new();
        request.encode_v4(&mut buf_v4).unwrap();
        assert!(!buf_v4.is_empty());

        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();
        assert_ne!(buf_v4.as_ref(), buf_v3.as_ref());
    }

    #[test]
    fn test_offset_for_leader_epoch_response_decode_v4_flexible() {
        let mut buf = BytesMut::new();
        // throttle_time_ms
        buf.put_i32(20);
        // topics array (compact: len+1 varint) = 1 topic → varint(2)
        buf.put_u8(2);
        // topic name (compact string)
        let topic = b"my-topic";
        buf.put_u8((topic.len() + 1) as u8);
        buf.put_slice(topic);
        // partitions array (compact) = 1 partition → varint(2)
        buf.put_u8(2);
        // error_code
        buf.put_i16(0);
        // partition
        buf.put_i32(0);
        // leader_epoch
        buf.put_i32(5);
        // end_offset
        buf.put_i64(1000);
        // tagged fields (per partition)
        buf.put_u8(0);
        // tagged fields (per topic)
        buf.put_u8(0);
        // tagged fields (top-level)
        buf.put_u8(0);

        let resp = OffsetForLeaderEpochResponse::decode_v4(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 20);
        assert_eq!(resp.topics.len(), 1);
        assert_eq!(resp.topics[0].topic, "my-topic");
        assert_eq!(resp.topics[0].partitions.len(), 1);
        assert!(resp.topics[0].partitions[0].error_code.is_ok());
        assert_eq!(resp.topics[0].partitions[0].partition, 0);
        assert_eq!(resp.topics[0].partitions[0].leader_epoch, 5);
        assert_eq!(resp.topics[0].partitions[0].end_offset, 1000);
    }

    #[test]
    fn test_offset_for_leader_epoch_v4_dispatch() {
        let request = OffsetForLeaderEpochRequest {
            replica_id: -1,
            topics: vec![],
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(4, &mut buf).unwrap();
        assert!(!buf.is_empty());
    }
}
