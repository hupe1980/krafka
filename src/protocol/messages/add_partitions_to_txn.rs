use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, decode_capacity,
    encode_compact_array_len,
};

/// AddPartitionsToTxn request (API Key 24).
///
/// Adds partitions to an ongoing transaction.
#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnRequest {
    /// Transactional ID.
    pub transactional_id: String,
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Topics to add.
    pub topics: Vec<AddPartitionsToTxnTopic>,
}

/// Topic in AddPartitionsToTxn request.
#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnTopic {
    /// Topic name.
    pub name: String,
    /// Partition indices.
    pub partitions: Vec<i32>,
}

impl AddPartitionsToTxnRequest {
    /// Create a new request.
    pub fn new(transactional_id: impl Into<String>, producer_id: i64, producer_epoch: i16) -> Self {
        Self {
            transactional_id: transactional_id.into(),
            producer_id,
            producer_epoch,
            topics: Vec::new(),
        }
    }

    /// Add a topic-partition.
    pub fn add_partition(mut self, topic: impl Into<String>, partition: i32) -> Self {
        let topic_name = topic.into();
        if let Some(t) = self.topics.iter_mut().find(|t| t.name == topic_name) {
            if !t.partitions.contains(&partition) {
                t.partitions.push(partition);
            }
        } else {
            self.topics.push(AddPartitionsToTxnTopic {
                name: topic_name,
                partitions: vec![partition],
            });
        }
        self
    }

    /// Encode as version 0–2.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        array_len_i32(self.topics.len())?.encode(buf);
        for topic in &self.topics {
            KafkaString(Some(topic.name.clone())).try_encode(buf)?;
            array_len_i32(topic.partitions.len())?.encode(buf);
            for partition in &topic.partitions {
                partition.encode(buf);
            }
        }
        Ok(())
    }

    /// Encode for version 3 (flexible: compact strings, varint arrays, tagged fields).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(Some(self.transactional_id.clone())).try_encode_compact(buf)?;
        self.producer_id.encode(buf);
        self.producer_epoch.encode(buf);
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            KafkaString(Some(topic.name.clone())).try_encode_compact(buf)?;
            encode_compact_array_len(topic.partitions.len(), buf)?;
            for partition in &topic.partitions {
                partition.encode(buf);
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 4–5 (batched transactions, broker-compatible).
    ///
    /// v4+ wraps the transaction in a `Transactions` compact array with
    /// `VerifyOnly = false`. The client always sends a single transaction.
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        // Transactions compact array: 1 + 1 = varint(2)
        crate::util::varint::encode_unsigned_varint(2, buf);
        KafkaString(Some(self.transactional_id.clone())).try_encode_compact(buf)?;
        buf.put_i64(self.producer_id);
        buf.put_i16(self.producer_epoch);
        buf.put_u8(0); // VerifyOnly = false
        encode_compact_array_len(self.topics.len(), buf)?;
        for topic in &self.topics {
            KafkaString(Some(topic.name.clone())).try_encode_compact(buf)?;
            encode_compact_array_len(topic.partitions.len(), buf)?;
            for partition in &topic.partitions {
                partition.encode(buf);
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        TaggedFields::default().try_encode(buf)?; // top-level tagged fields
        Ok(())
    }
}

/// AddPartitionsToTxn response.
#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Results by topic.
    pub results: Vec<AddPartitionsToTxnTopicResult>,
}

/// Result for a topic in AddPartitionsToTxn.
#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnTopicResult {
    /// Topic name.
    pub name: String,
    /// Partition results.
    pub partitions: Vec<AddPartitionsToTxnPartitionResult>,
}

/// Result for a partition in AddPartitionsToTxn.
#[derive(Debug, Clone)]
pub struct AddPartitionsToTxnPartitionResult {
    /// Partition index.
    pub partition: i32,
    /// Error code.
    pub error_code: ErrorCode,
}

impl AddPartitionsToTxnResponse {
    /// Decode from version 0–2.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut results = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partitions =
                Vec::with_capacity(decode_capacity(partition_count, buf.remaining()));

            for _ in 0..partition_count {
                let partition = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                partitions.push(AddPartitionsToTxnPartitionResult {
                    partition,
                    error_code,
                });
            }

            results.push(AddPartitionsToTxnTopicResult { name, partitions });
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    /// Decode from version 3 (flexible: compact strings, varint arrays, tagged fields).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut results = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let partition_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partitions =
                Vec::with_capacity(decode_capacity(partition_count, buf.remaining()));

            for _ in 0..partition_count {
                let partition = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let _ = TaggedFields::decode(buf)?;
                partitions.push(AddPartitionsToTxnPartitionResult {
                    partition,
                    error_code,
                });
            }

            let _ = TaggedFields::decode(buf)?;
            results.push(AddPartitionsToTxnTopicResult { name, partitions });
        }

        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    /// Decode from version 4–5 (batched: `ResultsByTransaction` compact array).
    ///
    /// Extracts results from the first transaction in the batched response.
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?); // top-level error code (v4+)
        if !error_code.is_ok() {
            return Err(crate::error::KrafkaError::broker(
                error_code,
                "AddPartitionsToTxn v4+ top-level error",
            ));
        }

        let txn_count = check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut results = Vec::new();

        for i in 0..txn_count {
            let _transactional_id = KafkaString::decode_compact(buf)?.0;
            let topic_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;

            for _ in 0..topic_count {
                let name = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
                let partition_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut partitions =
                    Vec::with_capacity(decode_capacity(partition_count, buf.remaining()));

                for _ in 0..partition_count {
                    let partition = i32::decode(buf)?;
                    let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                    let _ = TaggedFields::decode(buf)?;
                    partitions.push(AddPartitionsToTxnPartitionResult {
                        partition,
                        error_code,
                    });
                }

                let _ = TaggedFields::decode(buf)?;
                if i == 0 {
                    results.push(AddPartitionsToTxnTopicResult { name, partitions });
                }
            }

            let _ = TaggedFields::decode(buf)?;
        }

        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    /// Check if all partitions were added successfully.
    pub fn is_ok(&self) -> bool {
        self.results
            .iter()
            .all(|t| t.partitions.iter().all(|p| p.error_code.is_ok()))
    }
}

impl VersionedEncode for AddPartitionsToTxnRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0..=2 => self.encode_v0(buf)?,
            3 => self.encode_v3(buf)?,
            4 | 5 => self.encode_v4(buf)?,
            _ => return unsupported_encode!("AddPartitionsToTxnRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for AddPartitionsToTxnResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0..=2 => Self::decode_v0(buf),
            3 => Self::decode_v3(buf),
            4 | 5 => Self::decode_v4(buf),
            _ => unsupported_decode!("AddPartitionsToTxnResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::protocol::primitives::{Decode, KafkaString};
    use bytes::BytesMut;
    use rstest::rstest;

    #[test]
    fn test_add_partitions_to_txn_v0_wire_format() {
        let request = AddPartitionsToTxnRequest::new("txn-1", 100, 5)
            .add_partition("topic1", 0)
            .add_partition("topic1", 1);

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        let mut data = buf.freeze();

        // transactional_id (2-byte len + 5 bytes "txn-1")
        let txn_id = KafkaString::decode(&mut data).unwrap().0.unwrap();
        assert_eq!(txn_id, "txn-1");
        assert_eq!(i64::decode(&mut data).unwrap(), 100); // producer_id
        assert_eq!(i16::decode(&mut data).unwrap(), 5); // producer_epoch
        assert_eq!(i32::decode(&mut data).unwrap(), 1); // topics count
        let name = KafkaString::decode(&mut data).unwrap().0.unwrap();
        assert_eq!(name, "topic1");
        assert_eq!(i32::decode(&mut data).unwrap(), 2); // partitions count
        assert_eq!(i32::decode(&mut data).unwrap(), 0); // partition 0
        assert_eq!(i32::decode(&mut data).unwrap(), 1); // partition 1
        assert!(!data.has_remaining());
    }

    #[test]
    fn test_add_partitions_to_txn_v3_flexible() {
        let request = AddPartitionsToTxnRequest::new("txn-1", 100, 5).add_partition("t1", 2);

        let mut v0 = BytesMut::new();
        request.encode_v0(&mut v0).unwrap();
        let mut v3 = BytesMut::new();
        request.encode_v3(&mut v3).unwrap();

        // v3 uses compact strings (varint length) instead of i16 length
        // but also has tagged fields => different size
        assert_ne!(v0.len(), v3.len());

        // Round-trip via dispatch
        let mut buf = BytesMut::new();
        request.encode_versioned(3, &mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_add_partitions_to_txn_v4_batched() {
        let request = AddPartitionsToTxnRequest::new("txn-1", 100, 5).add_partition("t1", 0);

        let mut v3 = BytesMut::new();
        request.encode_v3(&mut v3).unwrap();
        let mut v4 = BytesMut::new();
        request.encode_v4(&mut v4).unwrap();

        // v4 wraps in Transactions array + VerifyOnly bool => larger
        assert!(v4.len() > v3.len());

        // Dispatch routes v4 and v5 to encode_v4
        let mut buf5 = BytesMut::new();
        request.encode_versioned(5, &mut buf5).unwrap();
        assert_eq!(v4.freeze(), buf5.freeze());
    }

    #[rstest]
    #[case::v1(1)]
    #[case::v2(2)]
    fn test_add_partitions_to_txn_v1_v2_same_as_v0(#[case] version: i16) {
        let request = AddPartitionsToTxnRequest::new("txn-1", 100, 5).add_partition("t1", 0);
        let mut v0 = BytesMut::new();
        request.encode_versioned(0, &mut v0).unwrap();
        let mut vn = BytesMut::new();
        request.encode_versioned(version, &mut vn).unwrap();
        assert_eq!(v0.freeze(), vn.freeze());
    }

    #[test]
    fn test_add_partitions_to_txn_response_v0_wire() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i32(1); // topics count
        let name = b"t1";
        buf.put_i16(name.len() as i16);
        buf.put_slice(name);
        buf.put_i32(1); // partitions count
        buf.put_i32(0); // partition
        buf.put_i16(0); // error_code (NONE)

        let resp = AddPartitionsToTxnResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].name, "t1");
        assert_eq!(resp.results[0].partitions[0].partition, 0);
        assert!(resp.results[0].partitions[0].error_code.is_ok());
    }

    #[test]
    fn test_add_partitions_to_txn_response_v3_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(5); // throttle_time_ms
        crate::util::varint::encode_unsigned_varint(2, &mut buf); // topics: 1 + 1
        // topic name compact string: len+1 as varint
        let name = b"t1";
        crate::util::varint::encode_unsigned_varint(name.len() as u32 + 1, &mut buf);
        buf.put_slice(name);
        crate::util::varint::encode_unsigned_varint(2, &mut buf); // partitions: 1 + 1
        buf.put_i32(3); // partition
        buf.put_i16(0); // error_code
        buf.put_u8(0); // partition tagged fields
        buf.put_u8(0); // topic tagged fields
        buf.put_u8(0); // top-level tagged fields

        let resp = AddPartitionsToTxnResponse::decode_v3(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 5);
        assert_eq!(resp.results[0].name, "t1");
        assert_eq!(resp.results[0].partitions[0].partition, 3);
    }

    #[test]
    fn test_add_partitions_to_txn_response_v4_batched() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // top-level error code (v4+)
        crate::util::varint::encode_unsigned_varint(2, &mut buf); // txn count: 1 + 1
        // transactional_id compact string
        let txn = b"txn-1";
        crate::util::varint::encode_unsigned_varint(txn.len() as u32 + 1, &mut buf);
        buf.put_slice(txn);
        crate::util::varint::encode_unsigned_varint(2, &mut buf); // topics: 1 + 1
        let topic = b"t1";
        crate::util::varint::encode_unsigned_varint(topic.len() as u32 + 1, &mut buf);
        buf.put_slice(topic);
        crate::util::varint::encode_unsigned_varint(2, &mut buf); // partitions: 1 + 1
        buf.put_i32(0); // partition
        buf.put_i16(0); // error_code
        buf.put_u8(0); // partition tagged fields
        buf.put_u8(0); // topic tagged fields
        buf.put_u8(0); // txn tagged fields
        buf.put_u8(0); // top-level tagged fields

        let resp = AddPartitionsToTxnResponse::decode_v4(&mut buf.freeze()).unwrap();
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].name, "t1");
    }

    #[test]
    fn test_add_partitions_to_txn_response_v4_top_level_error() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(16); // top-level error code: NOT_COORDINATOR
        // rest of the buffer doesn't matter — decode should fail at the error check
        crate::util::varint::encode_unsigned_varint(1, &mut buf); // txn count: 0
        buf.put_u8(0); // top-level tagged fields

        let err = AddPartitionsToTxnResponse::decode_v4(&mut buf.freeze()).unwrap_err();
        assert!(err.to_string().contains("top-level error"));
    }

    #[test]
    fn test_add_partitions_to_txn_request() {
        let request = AddPartitionsToTxnRequest::new("my-txn", 12345, 0)
            .add_partition("topic1", 0)
            .add_partition("topic1", 1)
            .add_partition("topic2", 0);

        assert_eq!(request.transactional_id, "my-txn");
        assert_eq!(request.producer_id, 12345);
        assert_eq!(request.topics.len(), 2);

        let topic1 = request.topics.iter().find(|t| t.name == "topic1").unwrap();
        assert_eq!(topic1.partitions, vec![0, 1]);

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }
}
