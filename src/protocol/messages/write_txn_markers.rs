use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode};
use crate::error::{ErrorCode, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{check_compact_array_len, encode_compact_array_len};
use crate::util::varint::decode_unsigned_varint;

// ============================================================================
// WriteTxnMarkers API (Key 27)
//
// v0 was removed in Kafka 4.0. v1 is the baseline (flexible encoding).
// v2 adds TransactionVersion field (KIP-1228).
// ============================================================================

/// A topic and its partitions to write a transaction marker for.
#[derive(Debug, Clone)]
pub struct WritableTxnMarkerTopic {
    /// Topic name.
    pub name: String,
    /// Partition indexes.
    pub partition_indexes: Vec<i32>,
}

/// A single transaction marker to write.
#[derive(Debug, Clone)]
pub struct WritableTxnMarker {
    /// The current producer ID.
    pub producer_id: i64,
    /// The current epoch associated with the producer ID.
    pub producer_epoch: i16,
    /// The result of the transaction (false = ABORT, true = COMMIT).
    pub transaction_result: bool,
    /// Topics and partitions to write the marker for.
    pub topics: Vec<WritableTxnMarkerTopic>,
    /// Epoch associated with the transaction state partition
    /// hosted by this transaction coordinator.
    pub coordinator_epoch: i32,
}

/// WriteTxnMarkers request (API key 27).
#[derive(Debug, Clone)]
pub struct WriteTxnMarkersRequest {
    /// The transaction markers to be written.
    pub markers: Vec<WritableTxnMarker>,
}

impl WriteTxnMarkersRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::WriteTxnMarkers
    }

    /// Encode for version 1 (flexible encoding, baseline after v0 removal).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.markers.len(), buf)?;
        for marker in &self.markers {
            marker.producer_id.encode(buf);
            marker.producer_epoch.encode(buf);
            let txn_result: i8 = if marker.transaction_result { 1 } else { 0 };
            txn_result.encode(buf);
            encode_compact_array_len(marker.topics.len(), buf)?;
            for topic in &marker.topics {
                KafkaString::new(&topic.name).try_encode_compact(buf)?;
                encode_compact_array_len(topic.partition_indexes.len(), buf)?;
                for &p in &topic.partition_indexes {
                    p.encode(buf);
                }
                TaggedFields::default().try_encode(buf)?;
            }
            marker.coordinator_epoch.encode(buf);
            TaggedFields::default().try_encode(buf)?;
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for WriteTxnMarkersRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1 => self.encode_v1(buf),
            _ => unsupported_encode!("WriteTxnMarkersRequest", version),
        }
    }
}

/// Per-partition result of writing a transaction marker.
#[derive(Debug, Clone)]
pub struct WritableTxnMarkerPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Error code, or `Ok` if no error.
    pub error_code: ErrorCode,
}

/// Per-topic result of writing a transaction marker.
#[derive(Debug, Clone)]
pub struct WritableTxnMarkerTopicResult {
    /// Topic name.
    pub name: String,
    /// Results per partition.
    pub partitions: Vec<WritableTxnMarkerPartitionResult>,
}

/// Per-marker result for a single producer in the response.
#[derive(Debug, Clone)]
pub struct WritableTxnMarkerResult {
    /// The current producer ID in use by the transactional ID.
    pub producer_id: i64,
    /// Results per topic.
    pub topics: Vec<WritableTxnMarkerTopicResult>,
}

/// WriteTxnMarkers response (API key 27).
#[derive(Debug, Clone)]
pub struct WriteTxnMarkersResponse {
    /// Results for each marker.
    pub markers: Vec<WritableTxnMarkerResult>,
}

impl WriteTxnMarkersResponse {
    /// Decode from version 1 (flexible encoding).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let marker_count = check_compact_array_len(decode_unsigned_varint(buf)?)? as usize;
        let mut markers = Vec::with_capacity(marker_count);
        for _ in 0..marker_count {
            let producer_id = i64::decode(buf)?;
            let topic_count = check_compact_array_len(decode_unsigned_varint(buf)?)? as usize;
            let mut topics = Vec::with_capacity(topic_count);
            for _ in 0..topic_count {
                let name = {
                    let len = decode_unsigned_varint(buf)? as usize;
                    if len < 1 {
                        return Err(crate::error::KrafkaError::protocol(
                            "compact string length 0 is null but field is non-nullable",
                        ));
                    }
                    let str_len = len - 1;
                    if buf.remaining() < str_len {
                        return Err(crate::error::KrafkaError::protocol(
                            "not enough bytes for compact string",
                        ));
                    }
                    let bytes = buf.copy_to_bytes(str_len);
                    String::from_utf8(bytes.to_vec()).map_err(|e| {
                        crate::error::KrafkaError::protocol(format!("invalid UTF-8: {e}"))
                    })?
                };
                let partition_count =
                    check_compact_array_len(decode_unsigned_varint(buf)?)? as usize;
                let mut partitions = Vec::with_capacity(partition_count);
                for _ in 0..partition_count {
                    let partition_index = i32::decode(buf)?;
                    let error_code = ErrorCode::from(i16::decode(buf)?);
                    TaggedFields::decode(buf)?;
                    partitions.push(WritableTxnMarkerPartitionResult {
                        partition_index,
                        error_code,
                    });
                }
                TaggedFields::decode(buf)?;
                topics.push(WritableTxnMarkerTopicResult { name, partitions });
            }
            TaggedFields::decode(buf)?;
            markers.push(WritableTxnMarkerResult {
                producer_id,
                topics,
            });
        }
        TaggedFields::decode(buf)?;
        Ok(Self { markers })
    }
}

impl VersionedDecode for WriteTxnMarkersResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 => Self::decode_v1(buf),
            _ => unsupported_decode!("WriteTxnMarkersResponse", version),
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
    fn write_txn_markers_request_roundtrip_v1() {
        let request = WriteTxnMarkersRequest {
            markers: vec![WritableTxnMarker {
                producer_id: 42,
                producer_epoch: 5,
                transaction_result: true,
                topics: vec![WritableTxnMarkerTopic {
                    name: "test-topic".to_string(),
                    partition_indexes: vec![0, 1, 2],
                }],
                coordinator_epoch: 10,
            }],
        };

        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();

        // Verify we can at least encode without error and produce non-empty output
        assert!(!buf.is_empty());
    }

    #[test]
    fn write_txn_markers_response_decode_v1() {
        // Build a valid v1 response manually
        let mut buf = BytesMut::new();

        // markers compact array: 1 element (varint = 2)
        buf.put_u8(2);
        // producer_id
        buf.put_i64(42);
        // topics compact array: 1 element (varint = 2)
        buf.put_u8(2);
        // topic name (compact string: length+1 as varint)
        let name = b"test-topic";
        buf.put_u8((name.len() + 1) as u8);
        buf.put_slice(name);
        // partitions compact array: 1 element (varint = 2)
        buf.put_u8(2);
        // partition_index
        buf.put_i32(0);
        // error_code
        buf.put_i16(0);
        // partition tagged fields
        buf.put_u8(0);
        // topic tagged fields
        buf.put_u8(0);
        // marker tagged fields
        buf.put_u8(0);
        // top-level tagged fields
        buf.put_u8(0);

        let mut read_buf = buf.freeze();
        let response = WriteTxnMarkersResponse::decode_v1(&mut read_buf).unwrap();

        assert_eq!(response.markers.len(), 1);
        assert_eq!(response.markers[0].producer_id, 42);
        assert_eq!(response.markers[0].topics.len(), 1);
        assert_eq!(response.markers[0].topics[0].name, "test-topic");
        assert_eq!(response.markers[0].topics[0].partitions.len(), 1);
        assert_eq!(
            response.markers[0].topics[0].partitions[0].partition_index,
            0
        );
        assert!(
            response.markers[0].topics[0].partitions[0]
                .error_code
                .is_ok()
        );
    }

    #[test]
    fn write_txn_markers_request_abort_v1() {
        let request = WriteTxnMarkersRequest {
            markers: vec![WritableTxnMarker {
                producer_id: 100,
                producer_epoch: 0,
                transaction_result: false, // ABORT
                topics: vec![WritableTxnMarkerTopic {
                    name: "txn-topic".to_string(),
                    partition_indexes: vec![0],
                }],
                coordinator_epoch: 1,
            }],
        };

        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn write_txn_markers_response_empty_markers() {
        let mut buf = BytesMut::new();
        // Empty markers array (varint = 1 means 0 elements)
        buf.put_u8(1);
        // Top-level tagged fields
        buf.put_u8(0);

        let mut read_buf = buf.freeze();
        let response = WriteTxnMarkersResponse::decode_v1(&mut read_buf).unwrap();
        assert!(response.markers.is_empty());
    }

    #[test]
    fn write_txn_markers_versioned_encode_dispatch() {
        let request = WriteTxnMarkersRequest { markers: vec![] };

        let mut buf = BytesMut::new();
        request.encode_versioned(1, &mut buf).unwrap();

        // Version 0 should be unsupported (removed in Kafka 4.0)
        let mut buf2 = BytesMut::new();
        assert!(request.encode_versioned(0, &mut buf2).is_err());
    }

    #[test]
    fn write_txn_markers_versioned_decode_dispatch() {
        let mut buf = BytesMut::new();
        // Empty markers
        buf.put_u8(1);
        buf.put_u8(0);

        let mut read_buf = buf.freeze();
        WriteTxnMarkersResponse::decode_versioned(1, &mut read_buf).unwrap();

        // Version 0 should be unsupported
        let mut empty = BytesMut::new().freeze();
        assert!(WriteTxnMarkersResponse::decode_versioned(0, &mut empty).is_err());
    }

    #[test]
    fn write_txn_markers_response_with_error() {
        let mut buf = BytesMut::new();

        // 1 marker
        buf.put_u8(2);
        // producer_id
        buf.put_i64(99);
        // 1 topic
        buf.put_u8(2);
        // topic name
        let name = b"err-topic";
        buf.put_u8((name.len() + 1) as u8);
        buf.put_slice(name);
        // 1 partition
        buf.put_u8(2);
        // partition_index
        buf.put_i32(3);
        // error_code: NOT_LEADER_OR_FOLLOWER (6)
        buf.put_i16(6);
        // partition tagged fields
        buf.put_u8(0);
        // topic tagged fields
        buf.put_u8(0);
        // marker tagged fields
        buf.put_u8(0);
        // top-level tagged fields
        buf.put_u8(0);

        let mut read_buf = buf.freeze();
        let response = WriteTxnMarkersResponse::decode_v1(&mut read_buf).unwrap();

        assert_eq!(
            response.markers[0].topics[0].partitions[0].partition_index,
            3
        );
        assert!(
            !response.markers[0].topics[0].partitions[0]
                .error_code
                .is_ok()
        );
    }
}
