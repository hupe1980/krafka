use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode};
use crate::error::{ErrorCode, ProtocolErrorKind, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{check_compact_array_len, decode_capacity, encode_compact_array_len};
use crate::util::varint::decode_unsigned_varint;

// ============================================================================
// WriteTxnMarkers API (Key 27)
//
// v0 was removed in Kafka 4.0. v1 is the baseline (flexible encoding).
// v2 adds a TransactionVersion field per marker (KIP-1228, Kafka 4.2).
// The response wire format is unchanged at v2.
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
    /// Transaction version of this marker (v2+, KIP-1228).
    ///
    /// `0` and `1` both mean the legacy protocol (TV0/TV1), `2` means TV2.
    /// The field is `ignorable` in the Kafka schema, so leaving it at the
    /// default of `0` is safe and is what [`Self::legacy_transaction_version`]
    /// documents. It is simply not written when the negotiated version is v1.
    pub transaction_version: i8,
}

impl WritableTxnMarker {
    /// The transaction version that means "legacy (TV0/TV1)" — the value to
    /// use unless the coordinator is running TV2 transactions.
    pub const fn legacy_transaction_version() -> i8 {
        0
    }
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
        self.encode(buf, false)
    }

    /// Encode for version 2 (adds `TransactionVersion` per marker, KIP-1228).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        self.encode(buf, true)
    }

    /// Shared body encoder. v2 differs from v1 by exactly one `int8` appended
    /// to each marker after `coordinator_epoch`, before the marker's tagged
    /// fields.
    fn encode(&self, buf: &mut impl BufMut, with_transaction_version: bool) -> Result<()> {
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
            if with_transaction_version {
                marker.transaction_version.encode(buf);
            }
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
            2 => self.encode_v2(buf),
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
    /// Decode from version 1–2 (flexible encoding).
    ///
    /// KIP-1228 bumped the response's `validVersions` to `1-2` without adding
    /// or changing a field, so v2 shares this decoder.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let marker_count = check_compact_array_len(decode_unsigned_varint(buf)?)? as usize;
        let mut markers = Vec::with_capacity(decode_capacity(marker_count, buf.remaining()));
        for _ in 0..marker_count {
            let producer_id = i64::decode(buf)?;
            let topic_count = check_compact_array_len(decode_unsigned_varint(buf)?)? as usize;
            let mut topics = Vec::with_capacity(decode_capacity(topic_count, buf.remaining()));
            for _ in 0..topic_count {
                let name = {
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
                let partition_count =
                    check_compact_array_len(decode_unsigned_varint(buf)?)? as usize;
                let mut partitions =
                    Vec::with_capacity(decode_capacity(partition_count, buf.remaining()));
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
            1 | 2 => Self::decode_v1(buf),
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
                transaction_version: 0,
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
                transaction_version: 2,
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

        for v in [1, 2] {
            let mut buf = BytesMut::new();
            request.encode_versioned(v, &mut buf).unwrap();
        }

        // Version 0 should be unsupported (removed in Kafka 4.0)
        let mut buf2 = BytesMut::new();
        assert!(request.encode_versioned(0, &mut buf2).is_err());
        let mut buf3 = BytesMut::new();
        assert!(request.encode_versioned(3, &mut buf3).is_err());
    }

    /// v2 appends exactly one `int8` per marker, positioned after
    /// `coordinator_epoch` and before the marker's tagged fields.
    #[test]
    fn write_txn_markers_request_v2_appends_transaction_version() {
        let request = WriteTxnMarkersRequest {
            markers: vec![WritableTxnMarker {
                producer_id: 42,
                producer_epoch: 5,
                transaction_result: true,
                topics: vec![WritableTxnMarkerTopic {
                    name: "t".to_string(),
                    partition_indexes: vec![0],
                }],
                coordinator_epoch: 10,
                transaction_version: 2,
            }],
        };

        let mut v1 = BytesMut::new();
        request.encode_v1(&mut v1).unwrap();
        let mut v2 = BytesMut::new();
        request.encode_v2(&mut v2).unwrap();

        assert_eq!(v2.len(), v1.len() + 1);
        // The v1 body ends with the marker's tagged fields then the top-level
        // tagged fields; v2 splices TransactionVersion in front of both.
        assert_eq!(v2[v1.len() - 2], 2);
        assert_eq!(&v2[..v1.len() - 2], &v1[..v1.len() - 2]);
    }

    /// A default marker still encodes at v2: the field is `ignorable` with a
    /// default of 0, meaning legacy TV0/TV1 semantics.
    #[test]
    fn write_txn_markers_request_v2_legacy_default() {
        assert_eq!(WritableTxnMarker::legacy_transaction_version(), 0);

        let request = WriteTxnMarkersRequest {
            markers: vec![WritableTxnMarker {
                producer_id: 1,
                producer_epoch: 0,
                transaction_result: false,
                topics: Vec::new(),
                coordinator_epoch: 0,
                transaction_version: WritableTxnMarker::legacy_transaction_version(),
            }],
        };
        let mut buf = BytesMut::new();
        request.encode_v2(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn write_txn_markers_versioned_decode_dispatch() {
        let mut buf = BytesMut::new();
        // Empty markers
        buf.put_u8(1);
        buf.put_u8(0);

        let mut read_buf = buf.freeze();
        WriteTxnMarkersResponse::decode_versioned(1, &mut read_buf).unwrap();

        // v2 did not change the response wire format, so the same bytes decode.
        let mut v2_buf = BytesMut::new();
        v2_buf.put_u8(1);
        v2_buf.put_u8(0);
        let mut v2_read = v2_buf.freeze();
        assert!(
            WriteTxnMarkersResponse::decode_versioned(2, &mut v2_read)
                .unwrap()
                .markers
                .is_empty()
        );

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
