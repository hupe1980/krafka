use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::check_compact_array_len;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
macro_rules! unsupported_encode {
    ($type:expr, $version:expr) => {
        Err(KrafkaError::protocol(format!(
            "unsupported {} encode version {}",
            $type, $version
        )))
    };
}

macro_rules! unsupported_decode {
    ($type:expr, $version:expr) => {
        Err(KrafkaError::protocol(format!(
            "unsupported {} decode version {}",
            $type, $version
        )))
    };
}

/// Find coordinator request.
#[derive(Debug, Clone)]
pub struct FindCoordinatorRequest {
    /// Key (group ID or transactional ID).
    pub key: String,
    /// Key type (0 = group, 1 = txn).
    pub key_type: i8,
}

impl FindCoordinatorRequest {
    /// Create a request for a consumer group.
    pub fn for_group(group_id: &str) -> Self {
        Self {
            key: group_id.to_string(),
            key_type: 0,
        }
    }

    /// Create a request for a transaction.
    pub fn for_transaction(transactional_id: &str) -> Self {
        Self {
            key: transactional_id.to_string(),
            key_type: 1,
        }
    }

    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::FindCoordinator
    }

    /// Encode for version 1-2.
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.key).try_encode(buf)?;
        self.key_type.encode(buf);
        Ok(())
    }

    /// Encode for version 3 (flexible: compact strings + tagged fields).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString::new(&self.key).try_encode_compact(buf)?;
        self.key_type.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 4–6 (batched coordinator lookup, KIP-699).
    ///
    /// v4 replaces the single `Key` field with `KeyType` + `CoordinatorKeys`
    /// compact array. We encode our single key as a one-element array.
    /// v5 (KIP-890) and v6 (KIP-932) share the same wire format.
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        self.key_type.encode(buf);
        // CoordinatorKeys: compact array with 1 element (varint len+1 = 2)
        crate::util::varint::encode_unsigned_varint(2, buf);
        KafkaString::new(&self.key).try_encode_compact(buf)?;
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// Find coordinator response.
#[derive(Debug, Clone)]
pub struct FindCoordinatorResponse {
    /// Throttle time.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// Coordinator node ID.
    pub node_id: i32,
    /// Coordinator host.
    pub host: String,
    /// Coordinator port.
    pub port: i32,
}

impl FindCoordinatorResponse {
    /// Decode from version 1-2.
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode(buf)?.0;
        let node_id = i32::decode(buf)?;
        let host = KafkaString::decode(buf)?.0.ok_or_else(|| {
            KrafkaError::protocol("FindCoordinator host must be a non-null string")
        })?;
        let port = i32::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            node_id,
            host,
            port,
        })
    }

    /// Decode from version 3 (flexible: compact strings + tagged fields).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode_compact(buf)?.0;
        let node_id = i32::decode(buf)?;
        let host = KafkaString::decode_compact(buf)?.0.ok_or_else(|| {
            KrafkaError::protocol("FindCoordinator host must be a non-null compact string")
        })?;
        let port = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            node_id,
            host,
            port,
        })
    }

    /// Decode from version 4–6 (batched coordinators array, KIP-699).
    ///
    /// v4 returns a compact `Coordinators` array. We extract the first entry.
    /// v5 (KIP-890) and v6 (KIP-932) share the same wire format.
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let count = check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        if count == 0 {
            let _ = TaggedFields::decode(buf)?;
            return Err(KrafkaError::protocol(
                "FindCoordinator v4: empty coordinators array",
            ));
        }

        // Decode first coordinator
        let _key = KafkaString::decode_compact(buf)?.0;
        let node_id = i32::decode(buf)?;
        let host = KafkaString::decode_compact(buf)?.0.ok_or_else(|| {
            KrafkaError::protocol("FindCoordinator host must be a non-null compact string")
        })?;
        let port = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode_compact(buf)?.0;
        let _ = TaggedFields::decode(buf)?;

        // Skip remaining coordinators
        for _ in 1..count {
            let _ = KafkaString::decode_compact(buf)?;
            let _ = i32::decode(buf)?;
            let _ = KafkaString::decode_compact(buf)?;
            let _ = i32::decode(buf)?;
            let _ = i16::decode(buf)?;
            let _ = KafkaString::decode_compact(buf)?;
            let _ = TaggedFields::decode(buf)?;
        }
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            node_id,
            host,
            port,
        })
    }
}

impl VersionedEncode for FindCoordinatorRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1..=2 => self.encode_v1(buf)?,
            3 => self.encode_v3(buf)?,
            4..=6 => self.encode_v4(buf)?,
            _ => return unsupported_encode!("FindCoordinatorRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for FindCoordinatorResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1..=2 => Self::decode_v1(buf),
            3 => Self::decode_v3(buf),
            4..=6 => Self::decode_v4(buf),
            _ => unsupported_decode!("FindCoordinatorResponse", version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::*;
    use crate::util::varint;
    use bytes::BytesMut;
    use rstest::rstest;

    #[test]
    fn test_find_coordinator_request() {
        let request = FindCoordinatorRequest::for_group("my-group");
        assert_eq!(request.key, "my-group");
        assert_eq!(request.key_type, 0);

        let request = FindCoordinatorRequest::for_transaction("my-txn");
        assert_eq!(request.key, "my-txn");
        assert_eq!(request.key_type, 1);
    }

    // ---- Story 1.4: FindCoordinator ----

    #[test]
    fn test_find_coordinator_request_v1_encode() {
        let request = FindCoordinatorRequest {
            key: "grp".to_string(),
            key_type: 0,
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(1, &mut buf).unwrap();
        let mut r = buf.freeze();
        // v1: key (string)
        let key = KafkaString::decode(&mut r).unwrap().0.unwrap();
        assert_eq!(key, "grp");
        // key_type (i8)
        assert_eq!(i8::decode(&mut r).unwrap(), 0);
    }

    #[test]
    fn test_find_coordinator_request_below_min_rejected() {
        let request = FindCoordinatorRequest {
            key: "g".to_string(),
            key_type: 0,
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(0, &mut buf).is_err());
    }

    #[test]
    fn test_find_coordinator_response_decode_v1() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        let msg = b"";
        buf.put_i16(msg.len() as i16);
        buf.put_slice(msg); // error_message
        buf.put_i32(1); // node_id
        let host = b"broker1";
        buf.put_i16(host.len() as i16);
        buf.put_slice(host); // host
        buf.put_i32(9092); // port

        let resp = FindCoordinatorResponse::decode_versioned(1, &mut buf.freeze()).unwrap();
        assert_eq!(resp.node_id, 1);
        assert_eq!(resp.host, "broker1");
        assert_eq!(resp.port, 9092);
        assert_eq!(resp.throttle_time_ms, 0);
    }

    #[rstest]
    // FindCoordinator MIN=1
    #[case::find_coordinator_v0(0)]
    fn test_find_coordinator_encode_below_min(#[case] version: i16) {
        let request = FindCoordinatorRequest {
            key: "g".to_string(),
            key_type: 0,
        };
        let mut buf = BytesMut::new();
        assert!(request.encode_versioned(version, &mut buf).is_err());
    }

    // ===================================================================
    // Story 1.4: FindCoordinator Wire-Format Tests
    // ===================================================================

    #[rstest]
    #[case::v1(1)]
    #[case::v2(2)]
    fn test_find_coordinator_request_v1_v2(#[case] version: i16) {
        let request = FindCoordinatorRequest {
            key: "my-group".to_string(),
            key_type: 0, // group
        };
        let mut buf = BytesMut::new();
        request.encode_versioned(version, &mut buf).unwrap();
        let mut buf2 = BytesMut::new();
        request.encode_v1(&mut buf2).unwrap();
        assert_eq!(buf, buf2);
    }

    #[test]
    fn test_find_coordinator_request_v3_flexible() {
        let request = FindCoordinatorRequest {
            key: "txn-id-1".to_string(),
            key_type: 1, // transaction
        };
        let mut buf_v2 = BytesMut::new();
        request.encode_versioned(2, &mut buf_v2).unwrap();
        let mut buf_v3 = BytesMut::new();
        request.encode_versioned(3, &mut buf_v3).unwrap();
        assert_ne!(
            buf_v2.as_ref(),
            buf_v3.as_ref(),
            "v3 flexible should differ from v2"
        );
    }

    #[test]
    fn test_find_coordinator_request_v4_batched_keys() {
        let request = FindCoordinatorRequest {
            key: "my-group".to_string(),
            key_type: 0,
        };
        let mut buf_v3 = BytesMut::new();
        request.encode_versioned(3, &mut buf_v3).unwrap();
        let mut buf_v4 = BytesMut::new();
        request.encode_versioned(4, &mut buf_v4).unwrap();
        // v4 wraps key in CoordinatorKeys array — different structure.
        assert_ne!(
            buf_v3.as_ref(),
            buf_v4.as_ref(),
            "v4 batched should differ from v3"
        );
    }

    #[test]
    fn test_find_coordinator_response_decode_v1_wire_format() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        // error_message nullable string
        buf.put_i16(-1); // null
        buf.put_i32(1); // node_id
        let host = b"broker-1";
        buf.put_i16(host.len() as i16);
        buf.put_slice(host);
        buf.put_i32(9092); // port

        let resp = FindCoordinatorResponse::decode_versioned(1, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 0);
        assert!(resp.error_code.is_ok());
        assert!(resp.error_message.is_none());
        assert_eq!(resp.node_id, 1);
        assert_eq!(resp.host, "broker-1");
        assert_eq!(resp.port, 9092);
    }

    #[test]
    fn test_find_coordinator_response_decode_v3_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(50); // throttle_time_ms
        buf.put_i16(0); // error_code
        // error_message null compact string
        varint::encode_unsigned_varint(0, &mut buf);
        buf.put_i32(2); // node_id
        // host compact string
        let host = b"kafka-0.internal";
        varint::encode_unsigned_varint(host.len() as u32 + 1, &mut buf);
        buf.put_slice(host);
        buf.put_i32(9093); // port
        varint::encode_unsigned_varint(0, &mut buf); // tagged fields

        let resp = FindCoordinatorResponse::decode_versioned(3, &mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert_eq!(resp.node_id, 2);
        assert_eq!(resp.host, "kafka-0.internal");
        assert_eq!(resp.port, 9093);
    }

    #[test]
    fn test_find_coordinator_response_decode_v4_batched() {
        // v4: batched coordinators array, first element extracted.
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        // Coordinators compact array: 1 + 1
        varint::encode_unsigned_varint(2, &mut buf);
        // key compact string
        let key = b"my-group";
        varint::encode_unsigned_varint(key.len() as u32 + 1, &mut buf);
        buf.put_slice(key);
        buf.put_i32(3); // node_id
        // host compact string
        let host = b"broker-3";
        varint::encode_unsigned_varint(host.len() as u32 + 1, &mut buf);
        buf.put_slice(host);
        buf.put_i32(9094); // port
        buf.put_i16(0); // error_code
        // error_message null compact string
        varint::encode_unsigned_varint(0, &mut buf);
        varint::encode_unsigned_varint(0, &mut buf); // coordinator tagged fields
        varint::encode_unsigned_varint(0, &mut buf); // top-level tagged fields

        let resp = FindCoordinatorResponse::decode_versioned(4, &mut buf.freeze()).unwrap();
        assert_eq!(resp.node_id, 3);
        assert_eq!(resp.host, "broker-3");
        assert_eq!(resp.port, 9094);
        assert!(resp.error_code.is_ok());
    }

    // ── FindCoordinator v5–v6 (same wire format as v4) ──

    #[rstest]
    #[case::v5(5)]
    #[case::v6(6)]
    fn test_find_coordinator_request_v5_v6_same_as_v4(#[case] version: i16) {
        let request = FindCoordinatorRequest {
            key: "my-group".to_string(),
            key_type: 0,
        };
        let mut buf_v4 = BytesMut::new();
        request.encode_versioned(4, &mut buf_v4).unwrap();
        let mut buf = BytesMut::new();
        request.encode_versioned(version, &mut buf).unwrap();
        assert_eq!(buf, buf_v4, "v{version} encode should equal v4");
    }

    #[rstest]
    #[case::v5(5)]
    #[case::v6(6)]
    fn test_find_coordinator_response_v5_v6_decode(#[case] version: i16) {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(2, &mut buf); // 1 coordinator + 1
        let key = b"grp";
        varint::encode_unsigned_varint(key.len() as u32 + 1, &mut buf);
        buf.put_slice(key);
        buf.put_i32(7); // node_id
        let host = b"host-7";
        varint::encode_unsigned_varint(host.len() as u32 + 1, &mut buf);
        buf.put_slice(host);
        buf.put_i32(9092); // port
        buf.put_i16(0); // error_code
        varint::encode_unsigned_varint(0, &mut buf); // error_message null
        varint::encode_unsigned_varint(0, &mut buf); // coordinator tagged fields
        varint::encode_unsigned_varint(0, &mut buf); // top-level tagged fields

        let resp = FindCoordinatorResponse::decode_versioned(version, &mut buf.freeze()).unwrap();
        assert_eq!(resp.node_id, 7);
        assert_eq!(resp.host, "host-7");
        assert_eq!(resp.port, 9092);
    }
}
