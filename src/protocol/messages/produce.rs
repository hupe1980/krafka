use bytes::{Buf, BufMut, Bytes};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{
    Decode, Encode, KafkaBytes, KafkaString, TaggedFields, TryEncode,
};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_compact_nullable_array_len,
    check_decode_array_len, check_decode_nullable_array_len,
};

/// Produce request.
#[derive(Debug, Clone)]
pub struct ProduceRequest {
    /// Transactional ID (v3+).
    pub transactional_id: Option<String>,
    /// Required acks (-1, 0, 1).
    pub acks: i16,
    /// Timeout in milliseconds.
    pub timeout_ms: i32,
    /// Topic data.
    pub topic_data: Vec<ProduceTopicData>,
}

/// Topic data in produce request.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ProduceTopicData {
    /// Topic name (v3–v12).
    pub name: String,
    /// Topic ID (v13+, KIP-516). Replaces `name` when set.
    pub topic_id: Option<[u8; 16]>,
    /// Partition data.
    pub partition_data: Vec<ProducePartitionData>,
}

/// Partition data in produce request.
#[derive(Debug, Clone)]
pub struct ProducePartitionData {
    /// Partition index.
    pub index: i32,
    /// Record batch data.
    pub records: Bytes,
}

impl ProduceRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::Produce
    }

    /// Encode for version 3-8.
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        match &self.transactional_id {
            Some(id) => KafkaString::new(id).try_encode(buf)?,
            None => KafkaString::null().try_encode(buf)?,
        }
        self.acks.encode(buf);
        self.timeout_ms.encode(buf);

        // Topics array
        buf.put_i32(array_len_i32(self.topic_data.len())?);
        for topic in &self.topic_data {
            KafkaString::new(&topic.name).try_encode(buf)?;

            // Partitions array
            buf.put_i32(array_len_i32(topic.partition_data.len())?);
            for partition in &topic.partition_data {
                partition.index.encode(buf);
                KafkaBytes::new(partition.records.clone()).try_encode(buf)?;
            }
        }
        Ok(())
    }

    /// Encode for version 9-11 (flexible: compact strings/arrays + tagged fields).
    pub fn encode_v9(&self, buf: &mut impl BufMut) -> Result<()> {
        match &self.transactional_id {
            Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }
        self.acks.encode(buf);
        self.timeout_ms.encode(buf);

        let topics_len = u32::try_from(self.topic_data.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("topics array too large"))?;
        crate::util::varint::encode_unsigned_varint(topics_len, buf);
        for topic in &self.topic_data {
            KafkaString::new(&topic.name).try_encode_compact(buf)?;
            let parts_len = u32::try_from(topic.partition_data.len().saturating_add(1))
                .map_err(|_| KrafkaError::protocol("partitions array too large"))?;
            crate::util::varint::encode_unsigned_varint(parts_len, buf);
            for partition in &topic.partition_data {
                partition.index.encode(buf);
                KafkaBytes::new(partition.records.clone()).try_encode_compact(buf)?;
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 13 (topic ID replaces topic name, KIP-516).
    pub fn encode_v13(&self, buf: &mut impl BufMut) -> Result<()> {
        match &self.transactional_id {
            Some(id) => KafkaString::new(id).try_encode_compact(buf)?,
            None => KafkaString::null().try_encode_compact(buf)?,
        }
        self.acks.encode(buf);
        self.timeout_ms.encode(buf);

        let topics_len = u32::try_from(self.topic_data.len().saturating_add(1))
            .map_err(|_| KrafkaError::protocol("topics array too large"))?;
        crate::util::varint::encode_unsigned_varint(topics_len, buf);
        for topic in &self.topic_data {
            let topic_id = topic.topic_id.ok_or_else(|| {
                KrafkaError::protocol("topic_id is required for Produce v13+ (KIP-516)")
            })?;
            buf.put_slice(&topic_id);
            let parts_len = u32::try_from(topic.partition_data.len().saturating_add(1))
                .map_err(|_| KrafkaError::protocol("partitions array too large"))?;
            crate::util::varint::encode_unsigned_varint(parts_len, buf);
            for partition in &topic.partition_data {
                partition.index.encode(buf);
                KafkaBytes::new(partition.records.clone()).try_encode_compact(buf)?;
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Produce response.
#[derive(Debug, Clone, Default)]
pub struct ProduceResponse {
    /// Topic responses.
    pub responses: Vec<ProduceTopicResponse>,
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
}

/// Topic response in produce response.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ProduceTopicResponse {
    /// Topic name (v3–v12).
    pub name: String,
    /// Topic ID (v13+, KIP-516). Replaces `name` when set.
    pub topic_id: Option<[u8; 16]>,
    /// Partition responses.
    pub partition_responses: Vec<ProducePartitionResponse>,
}

/// Partition response in produce response.
#[derive(Debug, Clone)]
pub struct ProducePartitionResponse {
    /// Partition index.
    pub index: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Base offset.
    pub base_offset: i64,
    /// Log append time.
    pub log_append_time_ms: i64,
    /// Log start offset (v5+).
    pub log_start_offset: i64,
}

impl ProduceResponse {
    /// Decode from version 3-4.
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut responses = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partition_responses = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let index = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let base_offset = i64::decode(buf)?;
                let log_append_time_ms = i64::decode(buf)?;

                partition_responses.push(ProducePartitionResponse {
                    index,
                    error_code,
                    base_offset,
                    log_append_time_ms,
                    log_start_offset: -1,
                });
            }

            responses.push(ProduceTopicResponse {
                name,
                topic_id: None,
                partition_responses,
            });
        }

        let throttle_time_ms = i32::decode(buf)?;

        Ok(Self {
            responses,
            throttle_time_ms,
        })
    }

    /// Decode from version 5-7 (v2 + log_start_offset per partition).
    pub fn decode_v5(buf: &mut impl Buf) -> Result<Self> {
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut responses = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partition_responses = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let index = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let base_offset = i64::decode(buf)?;
                let log_append_time_ms = i64::decode(buf)?;
                let log_start_offset = i64::decode(buf)?;

                partition_responses.push(ProducePartitionResponse {
                    index,
                    error_code,
                    base_offset,
                    log_append_time_ms,
                    log_start_offset,
                });
            }

            responses.push(ProduceTopicResponse {
                name,
                topic_id: None,
                partition_responses,
            });
        }

        let throttle_time_ms = i32::decode(buf)?;

        Ok(Self {
            responses,
            throttle_time_ms,
        })
    }

    /// Decode from version 8 (v5 + record_errors + error_message per partition).
    ///
    /// `RecordErrors` and `ErrorMessage` are read and discarded — they only
    /// appear for idempotent/transactional edge cases.
    pub fn decode_v8(buf: &mut impl Buf) -> Result<Self> {
        let topic_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut responses = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode(buf)?.0)?;
            let partition_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut partition_responses = Vec::with_capacity(partition_count);

            for _ in 0..partition_count {
                let index = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let base_offset = i64::decode(buf)?;
                let log_append_time_ms = i64::decode(buf)?;
                let log_start_offset = i64::decode(buf)?;
                // RecordErrors array — read and discard
                let record_errors_count = check_decode_nullable_array_len(i32::decode(buf)?)?;
                for _ in 0..record_errors_count {
                    let _ = i32::decode(buf)?; // batch_index
                    let _ = KafkaString::decode(buf)?; // batch_index_error_message
                }
                // ErrorMessage — read and discard
                let _ = KafkaString::decode(buf)?;

                partition_responses.push(ProducePartitionResponse {
                    index,
                    error_code,
                    base_offset,
                    log_append_time_ms,
                    log_start_offset,
                });
            }

            responses.push(ProduceTopicResponse {
                name,
                topic_id: None,
                partition_responses,
            });
        }

        let throttle_time_ms = i32::decode(buf)?;

        Ok(Self {
            responses,
            throttle_time_ms,
        })
    }

    /// Decode from version 9-12 (flexible: compact strings/arrays + tagged fields).
    pub fn decode_v9(buf: &mut impl Buf) -> Result<Self> {
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut responses = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            let name = non_nullable_string("topic name", KafkaString::decode_compact(buf)?.0)?;
            let part_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partition_responses = Vec::with_capacity(part_count);

            for _ in 0..part_count {
                let index = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let base_offset = i64::decode(buf)?;
                let log_append_time_ms = i64::decode(buf)?;
                let log_start_offset = i64::decode(buf)?;
                // RecordErrors compact nullable array — read and discard
                let re_count = check_compact_nullable_array_len(
                    crate::util::varint::decode_unsigned_varint(buf)?,
                )?;
                if re_count > 0 {
                    for _ in 0..re_count {
                        let _ = i32::decode(buf)?;
                        let _ = KafkaString::decode_compact(buf)?;
                        let _ = TaggedFields::decode(buf)?;
                    }
                }
                // ErrorMessage — read and discard
                let _ = KafkaString::decode_compact(buf)?;
                let _ = TaggedFields::decode(buf)?;

                partition_responses.push(ProducePartitionResponse {
                    index,
                    error_code,
                    base_offset,
                    log_append_time_ms,
                    log_start_offset,
                });
            }
            let _ = TaggedFields::decode(buf)?;
            responses.push(ProduceTopicResponse {
                name,
                topic_id: None,
                partition_responses,
            });
        }

        let throttle_time_ms = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            responses,
            throttle_time_ms,
        })
    }

    /// Decode from version 13 (topic ID replaces topic name, KIP-516).
    pub fn decode_v13(buf: &mut impl Buf) -> Result<Self> {
        let topic_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut responses = Vec::with_capacity(topic_count);

        for _ in 0..topic_count {
            if buf.remaining() < 16 {
                return Err(KrafkaError::protocol("not enough bytes for topic_id UUID"));
            }
            let mut topic_id = [0u8; 16];
            buf.copy_to_slice(&mut topic_id);

            let part_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut partition_responses = Vec::with_capacity(part_count);

            for _ in 0..part_count {
                let index = i32::decode(buf)?;
                let error_code = ErrorCode::from_i16(i16::decode(buf)?);
                let base_offset = i64::decode(buf)?;
                let log_append_time_ms = i64::decode(buf)?;
                let log_start_offset = i64::decode(buf)?;
                // RecordErrors compact nullable array — read and discard
                let re_count = check_compact_nullable_array_len(
                    crate::util::varint::decode_unsigned_varint(buf)?,
                )?;
                if re_count > 0 {
                    for _ in 0..re_count {
                        let _ = i32::decode(buf)?;
                        let _ = KafkaString::decode_compact(buf)?;
                        let _ = TaggedFields::decode(buf)?;
                    }
                }
                // ErrorMessage — read and discard
                let _ = KafkaString::decode_compact(buf)?;
                let _ = TaggedFields::decode(buf)?;

                partition_responses.push(ProducePartitionResponse {
                    index,
                    error_code,
                    base_offset,
                    log_append_time_ms,
                    log_start_offset,
                });
            }
            let _ = TaggedFields::decode(buf)?;
            responses.push(ProduceTopicResponse {
                name: String::new(),
                topic_id: Some(topic_id),
                partition_responses,
            });
        }

        let throttle_time_ms = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            responses,
            throttle_time_ms,
        })
    }
}

// Fetch request/response

impl VersionedEncode for ProduceRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            3..=8 => self.encode_v3(buf)?,
            9..=12 => self.encode_v9(buf)?,
            13 => self.encode_v13(buf)?,
            _ => return unsupported_encode!("ProduceRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for ProduceResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            3..=4 => Self::decode_v3(buf),
            5..=7 => Self::decode_v5(buf),
            8 => Self::decode_v8(buf),
            9..=12 => Self::decode_v9(buf),
            13 => Self::decode_v13(buf),
            _ => unsupported_decode!("ProduceResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::protocol::*;
    use crate::util::varint;
    use bytes::BytesMut;
    use rstest::rstest;

    // ====================================================================
    // Epic 1 — Round-trip wire-format tests
    // ====================================================================

    // ---- Story 1.1: Produce ----

    fn sample_produce_request() -> ProduceRequest {
        ProduceRequest {
            transactional_id: None,
            acks: -1,
            timeout_ms: 30_000,
            topic_data: vec![ProduceTopicData {
                name: "orders".to_string(),
                topic_id: None,
                partition_data: vec![ProducePartitionData {
                    index: 0,
                    records: bytes::Bytes::from_static(&[0xCA, 0xFE]),
                }],
            }],
        }
    }

    // ===================================================================
    // Story 1.1: ProduceRequest/Response Wire-Format Tests
    // ===================================================================

    /// Helper: build a sample ProduceRequest with realistic payload.
    fn sample_produce_request_with_data() -> ProduceRequest {
        ProduceRequest {
            transactional_id: Some("txn-abc".to_string()),
            acks: -1,
            timeout_ms: 30_000,
            topic_data: vec![
                ProduceTopicData {
                    name: "orders".to_string(),
                    topic_id: None,
                    partition_data: vec![
                        ProducePartitionData {
                            index: 0,
                            records: Bytes::from_static(&[0xDE, 0xAD, 0xBE, 0xEF]),
                        },
                        ProducePartitionData {
                            index: 1,
                            records: Bytes::from_static(&[0xCA, 0xFE]),
                        },
                    ],
                },
                ProduceTopicData {
                    name: "events".to_string(),
                    topic_id: None,
                    partition_data: vec![ProducePartitionData {
                        index: 0,
                        records: Bytes::from_static(&[0x01, 0x02, 0x03]),
                    }],
                },
            ],
        }
    }

    #[test]
    fn test_produce_request_v3_encodes_transactional_id() {
        let request = sample_produce_request();
        let mut buf = BytesMut::new();
        request.encode_versioned(3, &mut buf).unwrap();
        let mut r = buf.freeze();
        // v3: starts with nullable transactional_id (null = -1 i16 length)
        let tid = KafkaString::decode(&mut r).unwrap().0;
        assert!(tid.is_none());
        assert_eq!(i16::decode(&mut r).unwrap(), -1); // acks
        assert_eq!(i32::decode(&mut r).unwrap(), 30_000); // timeout_ms
    }

    #[test]
    fn test_produce_request_v9_flexible_encoding() {
        let request = sample_produce_request();
        let mut buf = BytesMut::new();
        request.encode_versioned(9, &mut buf).unwrap();
        // v9 should be longer than v3 due to tagged fields overhead
        let mut buf3 = BytesMut::new();
        request.encode_versioned(3, &mut buf3).unwrap();
        assert!(!buf.is_empty());
        assert!(!buf3.is_empty());
    }

    #[test]
    fn test_produce_request_below_min_rejected() {
        let request = sample_produce_request();
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(0, &mut buf).is_err());
        let mut buf2 = BytesMut::new();
        assert!(request.encode_versioned(2, &mut buf2).is_err());
    }

    #[test]
    fn test_produce_response_decode_v3() {
        let mut buf = BytesMut::new();
        // 1 topic
        buf.put_i32(1);
        let topic = b"orders";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        // 1 partition
        buf.put_i32(1);
        buf.put_i32(0); // index
        buf.put_i16(0); // error_code (None)
        buf.put_i64(42); // base_offset
        buf.put_i64(1_700_000_000_000); // log_append_time_ms (v2+)
        // throttle_time_ms
        buf.put_i32(100);

        let resp = ProduceResponse::decode_versioned(3, &mut buf.freeze()).unwrap();
        assert_eq!(resp.responses.len(), 1);
        assert_eq!(resp.responses[0].name, "orders");
        assert_eq!(resp.responses[0].partition_responses[0].base_offset, 42);
        assert_eq!(
            resp.responses[0].partition_responses[0].log_append_time_ms,
            1_700_000_000_000
        );
        assert_eq!(resp.throttle_time_ms, 100);
    }

    #[test]
    fn test_produce_response_decode_v5_has_log_start_offset() {
        let mut buf = BytesMut::new();
        // 1 topic, 1 partition
        buf.put_i32(1);
        let topic = b"t1";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        buf.put_i32(1);
        buf.put_i32(0); // index
        buf.put_i16(0); // error_code
        buf.put_i64(100); // base_offset
        buf.put_i64(-1); // log_append_time_ms
        buf.put_i64(50); // log_start_offset (v5+)
        buf.put_i32(0); // throttle_time_ms

        let resp = ProduceResponse::decode_versioned(5, &mut buf.freeze()).unwrap();
        assert_eq!(
            resp.responses[0].partition_responses[0].log_start_offset,
            50
        );
    }

    #[rstest]
    // Produce MIN=3
    #[case::produce_v0(0)]
    #[case::produce_v1(1)]
    #[case::produce_v2(2)]
    fn test_produce_encode_below_min(#[case] version: i16) {
        let request = sample_produce_request();
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }

    #[rstest]
    #[case::v3_min(3)]
    #[case::v8_last_non_flexible(8)]
    fn test_produce_request_encode_non_flexible(#[case] version: i16) {
        let request = sample_produce_request_with_data();
        let mut buf = BytesMut::new();
        request.encode_versioned(version, &mut buf).unwrap();
        assert!(!buf.is_empty());
        // v3-v8 all use encode_v3 — verify identical wire output.
        let mut buf2 = BytesMut::new();
        request.encode_v3(&mut buf2).unwrap();
        assert_eq!(buf, buf2);
    }

    #[rstest]
    #[case::v9_first_flexible(9)]
    #[case::v11_max(11)]
    fn test_produce_request_encode_flexible(#[case] version: i16) {
        let request = sample_produce_request_with_data();
        let mut buf = BytesMut::new();
        request.encode_versioned(version, &mut buf).unwrap();
        assert!(!buf.is_empty());
        // Flexible should differ from non-flexible.
        let mut buf_v3 = BytesMut::new();
        request.encode_v3(&mut buf_v3).unwrap();
        assert_ne!(
            buf.as_ref(),
            buf_v3.as_ref(),
            "Flexible should differ from non-flexible"
        );
    }

    #[test]
    fn test_produce_request_null_transactional_id() {
        let request = ProduceRequest {
            transactional_id: None,
            acks: 1,
            timeout_ms: 5000,
            topic_data: vec![ProduceTopicData {
                name: "t".to_string(),
                topic_id: None,
                partition_data: vec![ProducePartitionData {
                    index: 0,
                    records: Bytes::from_static(&[0x00]),
                }],
            }],
        };
        let mut buf_v3 = BytesMut::new();
        request.encode_versioned(3, &mut buf_v3).unwrap();
        // Null string is -1 as i16 for non-flexible.
        assert_eq!(i16::from_be_bytes([buf_v3[0], buf_v3[1]]), -1);

        let mut buf_v9 = BytesMut::new();
        request.encode_versioned(9, &mut buf_v9).unwrap();
        // Null compact string is 0x00 varint for flexible.
        assert_eq!(buf_v9[0], 0x00);
    }

    #[test]
    fn test_produce_request_empty_topic_data() {
        let request = ProduceRequest {
            transactional_id: None,
            acks: 0,
            timeout_ms: 1000,
            topic_data: vec![],
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(3, &mut buf).unwrap();
        assert!(!buf.is_empty());

        let mut buf_flex = BytesMut::new();
        request.encode_versioned(9, &mut buf_flex).unwrap();
        assert!(!buf_flex.is_empty());
    }

    #[test]
    fn test_produce_response_decode_v3_wire_format() {
        // v3-v4 wire format: topics array (non-flexible), no log_start_offset.
        let mut buf = BytesMut::new();
        // topics count
        buf.put_i32(1);
        // topic name
        let topic = b"orders";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        // partitions count
        buf.put_i32(1);
        // partition index
        buf.put_i32(0);
        // error_code
        buf.put_i16(0);
        // base_offset
        buf.put_i64(42);
        // log_append_time_ms
        buf.put_i64(1_700_000_000_000);
        // throttle_time_ms
        buf.put_i32(100);

        let resp = ProduceResponse::decode_versioned(3, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 100);
        assert_eq!(resp.responses.len(), 1);
        assert_eq!(resp.responses[0].name, "orders");
        assert_eq!(resp.responses[0].partition_responses.len(), 1);
        assert_eq!(resp.responses[0].partition_responses[0].index, 0);
        assert!(resp.responses[0].partition_responses[0].error_code.is_ok());
        assert_eq!(resp.responses[0].partition_responses[0].base_offset, 42);
        assert_eq!(
            resp.responses[0].partition_responses[0].log_append_time_ms,
            1_700_000_000_000
        );
        // v3 doesn't have log_start_offset — sentinel default.
        assert_eq!(
            resp.responses[0].partition_responses[0].log_start_offset,
            -1
        );
    }

    #[test]
    fn test_produce_response_decode_v5_log_start_offset() {
        // v5-v7: adds log_start_offset per partition.
        let mut buf = BytesMut::new();
        buf.put_i32(1); // topics count
        let topic = b"events";
        buf.put_i16(topic.len() as i16);
        buf.put_slice(topic);
        buf.put_i32(1); // partitions count
        buf.put_i32(2); // partition index
        buf.put_i16(0); // error_code
        buf.put_i64(100); // base_offset
        buf.put_i64(-1); // log_append_time_ms
        buf.put_i64(50); // log_start_offset (new in v5)
        buf.put_i32(0); // throttle_time_ms

        let resp = ProduceResponse::decode_versioned(5, &mut buf.freeze()).unwrap();
        assert_eq!(
            resp.responses[0].partition_responses[0].log_start_offset,
            50
        );
        assert_eq!(resp.responses[0].partition_responses[0].base_offset, 100);
    }

    #[test]
    fn test_produce_response_decode_v9_flexible() {
        // v9-v11: flexible encoding with compact arrays/strings + tagged fields.
        let mut buf = BytesMut::new();
        // topics compact array: count + 1 as varint
        varint::encode_unsigned_varint(2, &mut buf); // 1 topic + 1
        // topic name compact string
        let topic = b"test-topic";
        varint::encode_unsigned_varint(topic.len() as u32 + 1, &mut buf);
        buf.put_slice(topic);
        // partitions compact array
        varint::encode_unsigned_varint(2, &mut buf); // 1 partition + 1
        buf.put_i32(0); // partition index
        buf.put_i16(0); // error_code
        buf.put_i64(999); // base_offset
        buf.put_i64(-1); // log_append_time_ms
        buf.put_i64(10); // log_start_offset
        // RecordErrors compact nullable array: 1 means 0 elements
        varint::encode_unsigned_varint(1, &mut buf);
        // ErrorMessage null compact string
        varint::encode_unsigned_varint(0, &mut buf);
        // per-partition tagged fields
        varint::encode_unsigned_varint(0, &mut buf);
        // per-topic tagged fields
        varint::encode_unsigned_varint(0, &mut buf);
        // throttle_time_ms
        buf.put_i32(50);
        // top-level tagged fields
        varint::encode_unsigned_varint(0, &mut buf);

        let resp = ProduceResponse::decode_versioned(9, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert_eq!(resp.responses.len(), 1);
        assert_eq!(resp.responses[0].name, "test-topic");
        assert_eq!(resp.responses[0].partition_responses[0].base_offset, 999);
        assert_eq!(
            resp.responses[0].partition_responses[0].log_start_offset,
            10
        );
    }

    #[test]
    fn test_produce_response_decode_v9_multi_topic_multi_partition() {
        let mut buf = BytesMut::new();
        // 2 topics
        varint::encode_unsigned_varint(3, &mut buf); // 2 + 1

        // Topic 1: "t1" with 2 partitions
        varint::encode_unsigned_varint(3, &mut buf); // "t1" len + 1
        buf.put_slice(b"t1");
        varint::encode_unsigned_varint(3, &mut buf); // 2 partitions + 1
        for p in [0i32, 1] {
            buf.put_i32(p); // index
            buf.put_i16(0); // error_code
            buf.put_i64(p as i64 * 100); // base_offset
            buf.put_i64(-1); // log_append_time_ms
            buf.put_i64(0); // log_start_offset
            varint::encode_unsigned_varint(1, &mut buf); // RecordErrors empty
            varint::encode_unsigned_varint(0, &mut buf); // ErrorMessage null
            varint::encode_unsigned_varint(0, &mut buf); // tagged fields
        }
        varint::encode_unsigned_varint(0, &mut buf); // topic tagged fields

        // Topic 2: "t2" with 1 partition
        varint::encode_unsigned_varint(3, &mut buf);
        buf.put_slice(b"t2");
        varint::encode_unsigned_varint(2, &mut buf); // 1 partition + 1
        buf.put_i32(0);
        buf.put_i16(0);
        buf.put_i64(500);
        buf.put_i64(-1);
        buf.put_i64(0);
        varint::encode_unsigned_varint(1, &mut buf);
        varint::encode_unsigned_varint(0, &mut buf);
        varint::encode_unsigned_varint(0, &mut buf);
        varint::encode_unsigned_varint(0, &mut buf);

        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(0, &mut buf);

        let resp = ProduceResponse::decode_versioned(11, &mut buf.freeze()).unwrap();
        assert_eq!(resp.responses.len(), 2);
        assert_eq!(resp.responses[0].name, "t1");
        assert_eq!(resp.responses[0].partition_responses.len(), 2);
        assert_eq!(resp.responses[0].partition_responses[1].base_offset, 100);
        assert_eq!(resp.responses[1].name, "t2");
        assert_eq!(resp.responses[1].partition_responses[0].base_offset, 500);
    }

    // ===================================================================
    // Round-trip tests for new protocol versions (Backlog 2)
    // ===================================================================

    // ── Produce v12 (same wire as v11) ──

    #[test]
    fn test_produce_response_decode_v12_same_as_v11() {
        let mut buf = BytesMut::new();
        varint::encode_unsigned_varint(2, &mut buf); // 1 topic + 1
        let topic = b"tp";
        varint::encode_unsigned_varint(topic.len() as u32 + 1, &mut buf);
        buf.put_slice(topic);
        varint::encode_unsigned_varint(2, &mut buf); // 1 partition + 1
        buf.put_i32(0);
        buf.put_i16(0);
        buf.put_i64(42);
        buf.put_i64(-1);
        buf.put_i64(0);
        varint::encode_unsigned_varint(1, &mut buf); // empty RecordErrors
        varint::encode_unsigned_varint(0, &mut buf); // null ErrorMessage
        varint::encode_unsigned_varint(0, &mut buf); // partition tagged
        varint::encode_unsigned_varint(0, &mut buf); // topic tagged
        buf.put_i32(5); // throttle_time_ms
        varint::encode_unsigned_varint(0, &mut buf); // top tagged

        let resp = ProduceResponse::decode_versioned(12, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 5);
        assert_eq!(resp.responses[0].name, "tp");
        assert_eq!(resp.responses[0].partition_responses[0].base_offset, 42);
    }

    // ── Produce v13 (topic_id UUID) ──

    #[test]
    fn test_produce_response_decode_v13_topic_id() {
        let mut buf = BytesMut::new();
        varint::encode_unsigned_varint(2, &mut buf); // 1 topic + 1
        let topic_id: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        buf.put_slice(&topic_id);
        varint::encode_unsigned_varint(2, &mut buf); // 1 partition + 1
        buf.put_i32(0);
        buf.put_i16(0);
        buf.put_i64(99);
        buf.put_i64(-1);
        buf.put_i64(0);
        varint::encode_unsigned_varint(1, &mut buf);
        varint::encode_unsigned_varint(0, &mut buf);
        varint::encode_unsigned_varint(0, &mut buf);
        varint::encode_unsigned_varint(0, &mut buf);
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(0, &mut buf);

        let resp = ProduceResponse::decode_versioned(13, &mut buf.freeze()).unwrap();
        assert_eq!(resp.responses[0].topic_id, Some(topic_id));
        assert!(resp.responses[0].name.is_empty());
        assert_eq!(resp.responses[0].partition_responses[0].base_offset, 99);
    }

    #[test]
    fn test_produce_request_encode_v13_topic_id() {
        let topic_id: [u8; 16] = [0xAA; 16];
        let request = ProduceRequest {
            transactional_id: None,
            acks: -1,
            timeout_ms: 1500,
            topic_data: vec![ProduceTopicData {
                name: String::new(),
                topic_id: Some(topic_id),
                partition_data: vec![],
            }],
        };
        let mut buf = BytesMut::new();
        request.encode_v13(&mut buf).unwrap();

        let mut cur = &buf[..];
        // nullable compact string for transactional_id = null (varint 0)
        let txn_varint = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(txn_varint, 0);
        assert_eq!(cur.get_i16(), -1); // acks
        assert_eq!(cur.get_i32(), 1500); // timeout_ms
        // compact array: 1 topic + 1 = 2
        let topics_varint = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(topics_varint, 2);
        // 16-byte topic_id
        let mut read_id = [0u8; 16];
        cur.copy_to_slice(&mut read_id);
        assert_eq!(read_id, topic_id);
    }
}
