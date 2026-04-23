use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::check_compact_array_len;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};

// ============================================================================
// DescribeCluster API (Key 60)
// ============================================================================

/// DescribeCluster request (API Key 60). Flexible from v0.
#[derive(Debug, Clone)]
pub struct DescribeClusterRequest {
    /// Whether to include cluster authorized operations.
    pub include_cluster_authorized_operations: bool,
    /// Endpoint type to describe (v1+). 1=brokers, 2=controllers.
    pub endpoint_type: i8,
    /// Whether to include fenced brokers (v2+).
    pub include_fenced_brokers: bool,
}

impl Default for DescribeClusterRequest {
    fn default() -> Self {
        Self {
            include_cluster_authorized_operations: false,
            endpoint_type: 1,
            include_fenced_brokers: false,
        }
    }
}

impl DescribeClusterRequest {
    /// Encode for version 0 (flexible from v0).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_u8(u8::from(self.include_cluster_authorized_operations));
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 1 (adds endpoint_type).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_u8(u8::from(self.include_cluster_authorized_operations));
        self.endpoint_type.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 2 (adds include_fenced_brokers).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_u8(u8::from(self.include_cluster_authorized_operations));
        self.endpoint_type.encode(buf);
        buf.put_u8(u8::from(self.include_fenced_brokers));
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// DescribeCluster response (API Key 60). Flexible from v0.
#[derive(Debug, Clone)]
pub struct DescribeClusterResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code.
    pub error_code: ErrorCode,
    /// Top-level error message.
    pub error_message: Option<String>,
    /// Endpoint type (v1+). 1=brokers, 2=controllers.
    pub endpoint_type: i8,
    /// Cluster ID.
    pub cluster_id: String,
    /// Controller broker ID.
    pub controller_id: i32,
    /// Brokers in the cluster.
    pub brokers: Vec<DescribeClusterBroker>,
    /// Cluster authorized operations (bitfield).
    pub cluster_authorized_operations: i32,
}

/// Broker info in DescribeCluster response.
#[derive(Debug, Clone)]
pub struct DescribeClusterBroker {
    /// Broker ID.
    pub broker_id: i32,
    /// Broker hostname.
    pub host: String,
    /// Broker port.
    pub port: i32,
    /// Rack (if assigned).
    pub rack: Option<String>,
    /// Whether the broker is fenced (v2+).
    pub is_fenced: bool,
}

impl DescribeClusterResponse {
    /// Decode from version 0 (flexible from v0).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode_compact(buf)?.0;
        let cluster_id = non_nullable_string("cluster_id", KafkaString::decode_compact(buf)?.0)?;
        let controller_id = i32::decode(buf)?;

        let broker_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut brokers = Vec::with_capacity(broker_count);
        for _ in 0..broker_count {
            let broker_id = i32::decode(buf)?;
            let host = non_nullable_string("host", KafkaString::decode_compact(buf)?.0)?;
            let port = i32::decode(buf)?;
            let rack = KafkaString::decode_compact(buf)?.0;
            let _ = TaggedFields::decode(buf)?;
            brokers.push(DescribeClusterBroker {
                broker_id,
                host,
                port,
                rack,
                is_fenced: false,
            });
        }

        let cluster_authorized_operations = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            endpoint_type: 1,
            cluster_id,
            controller_id,
            brokers,
            cluster_authorized_operations,
        })
    }

    /// Decode from version 1 (adds endpoint_type).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode_compact(buf)?.0;
        let endpoint_type = i8::decode(buf)?;
        let cluster_id = non_nullable_string("cluster_id", KafkaString::decode_compact(buf)?.0)?;
        let controller_id = i32::decode(buf)?;

        let broker_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut brokers = Vec::with_capacity(broker_count);
        for _ in 0..broker_count {
            let broker_id = i32::decode(buf)?;
            let host = non_nullable_string("host", KafkaString::decode_compact(buf)?.0)?;
            let port = i32::decode(buf)?;
            let rack = KafkaString::decode_compact(buf)?.0;
            let _ = TaggedFields::decode(buf)?;
            brokers.push(DescribeClusterBroker {
                broker_id,
                host,
                port,
                rack,
                is_fenced: false,
            });
        }

        let cluster_authorized_operations = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            endpoint_type,
            cluster_id,
            controller_id,
            brokers,
            cluster_authorized_operations,
        })
    }

    /// Decode from version 2 (adds is_fenced per broker).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode_compact(buf)?.0;
        let endpoint_type = i8::decode(buf)?;
        let cluster_id = non_nullable_string("cluster_id", KafkaString::decode_compact(buf)?.0)?;
        let controller_id = i32::decode(buf)?;

        let broker_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut brokers = Vec::with_capacity(broker_count);
        for _ in 0..broker_count {
            let broker_id = i32::decode(buf)?;
            let host = non_nullable_string("host", KafkaString::decode_compact(buf)?.0)?;
            let port = i32::decode(buf)?;
            let rack = KafkaString::decode_compact(buf)?.0;
            let is_fenced = i8::decode(buf)? != 0;
            let _ = TaggedFields::decode(buf)?;
            brokers.push(DescribeClusterBroker {
                broker_id,
                host,
                port,
                rack,
                is_fenced,
            });
        }

        let cluster_authorized_operations = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            endpoint_type,
            cluster_id,
            controller_id,
            brokers,
            cluster_authorized_operations,
        })
    }
}

impl VersionedEncode for DescribeClusterRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            2 => self.encode_v2(buf)?,
            _ => return unsupported_encode!("DescribeClusterRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DescribeClusterResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            2 => Self::decode_v2(buf),
            _ => unsupported_decode!("DescribeClusterResponse", version),
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
    fn test_describe_cluster_request_encode_v0() {
        let req = DescribeClusterRequest {
            include_cluster_authorized_operations: true,
            endpoint_type: 1,
            include_fenced_brokers: false,
        };
        let mut buf = BytesMut::new();
        req.encode_v0(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_u8(), 1); // include_cluster_authorized_operations
        assert_eq!(cur.get_u8(), 0); // tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn test_describe_cluster_request_encode_v1_endpoint_type() {
        let req = DescribeClusterRequest {
            include_cluster_authorized_operations: false,
            endpoint_type: 2,
            include_fenced_brokers: false,
        };
        let mut buf = BytesMut::new();
        req.encode_v1(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_u8(), 0); // include_cluster_authorized_operations
        assert_eq!(cur.get_i8(), 2); // endpoint_type
        assert_eq!(cur.get_u8(), 0); // tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn test_describe_cluster_request_encode_v2_fenced() {
        let req = DescribeClusterRequest {
            include_cluster_authorized_operations: true,
            endpoint_type: 1,
            include_fenced_brokers: true,
        };
        let mut buf = BytesMut::new();
        req.encode_v2(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_u8(), 1);
        assert_eq!(cur.get_i8(), 1);
        assert_eq!(cur.get_u8(), 1); // include_fenced_brokers
        assert_eq!(cur.get_u8(), 0); // tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn test_describe_cluster_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i16(0); // error_code
        put_compact_string(&mut buf, None); // error_message null
        put_compact_string(&mut buf, Some("cluster-1")); // cluster_id
        buf.put_i32(0); // controller_id
        varint::encode_unsigned_varint(2, &mut buf); // 1 broker
        buf.put_i32(0); // broker_id
        put_compact_string(&mut buf, Some("host-0")); // host
        buf.put_i32(9092); // port
        put_compact_string(&mut buf, Some("rack-a")); // rack
        put_tagged_fields(&mut buf); // broker tagged fields
        buf.put_i32(-2_147_483_648); // cluster_authorized_operations
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = DescribeClusterResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.cluster_id, "cluster-1");
        assert_eq!(resp.controller_id, 0);
        assert_eq!(resp.brokers.len(), 1);
        assert_eq!(resp.brokers[0].broker_id, 0);
        assert_eq!(resp.brokers[0].host, "host-0");
        assert_eq!(resp.brokers[0].port, 9092);
        assert_eq!(resp.brokers[0].rack.as_deref(), Some("rack-a"));
        assert!(!resp.brokers[0].is_fenced); // default false in v0
        assert_eq!(resp.endpoint_type, 1); // default for v0
    }

    #[test]
    fn test_describe_cluster_response_decode_v1_endpoint_type() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        put_compact_string(&mut buf, None); // null error_message
        buf.put_i8(2); // endpoint_type = 2 (controllers)
        put_compact_string(&mut buf, Some("c")); // cluster_id
        buf.put_i32(1); // controller_id
        varint::encode_unsigned_varint(1, &mut buf); // 0 brokers
        buf.put_i32(0); // authorized_operations
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = DescribeClusterResponse::decode_v1(&mut buf.freeze()).unwrap();
        assert_eq!(resp.endpoint_type, 2);
        assert_eq!(resp.cluster_id, "c");
        assert!(resp.brokers.is_empty());
    }

    #[test]
    fn test_describe_cluster_response_decode_v2_is_fenced() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        put_compact_string(&mut buf, None); // null error_message
        buf.put_i8(1); // endpoint_type
        put_compact_string(&mut buf, Some("c")); // cluster_id
        buf.put_i32(0); // controller_id
        varint::encode_unsigned_varint(3, &mut buf); // 2 brokers
        // broker 0: not fenced
        buf.put_i32(0);
        put_compact_string(&mut buf, Some("h0"));
        buf.put_i32(9092);
        put_compact_string(&mut buf, None); // rack null
        buf.put_i8(0); // is_fenced = false
        put_tagged_fields(&mut buf);
        // broker 1: fenced
        buf.put_i32(1);
        put_compact_string(&mut buf, Some("h1"));
        buf.put_i32(9093);
        put_compact_string(&mut buf, None);
        buf.put_i8(1); // is_fenced = true
        put_tagged_fields(&mut buf);
        buf.put_i32(0); // authorized_operations
        put_tagged_fields(&mut buf);

        let resp = DescribeClusterResponse::decode_v2(&mut buf.freeze()).unwrap();
        assert_eq!(resp.brokers.len(), 2);
        assert!(!resp.brokers[0].is_fenced);
        assert!(resp.brokers[1].is_fenced);
    }
}
