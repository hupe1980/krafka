use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::check_compact_array_len;
use crate::protocol::primitives::{Decode, KafkaString, TaggedFields, TryEncode};

// ============================================================================
// ListClientMetricsResources API (Key 74)
// ============================================================================

/// ListClientMetricsResources request (API Key 74). Flexible from v0.
#[derive(Debug, Clone)]
pub struct ListClientMetricsResourcesRequest;

impl ListClientMetricsResourcesRequest {
    /// Encode for version 0 (flexible from v0, empty body).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// A client metrics resource name.
#[derive(Debug, Clone)]
pub struct ClientMetricsResource {
    /// Resource name.
    pub name: String,
}

/// ListClientMetricsResources response (API Key 74). Flexible from v0.
#[derive(Debug, Clone)]
pub struct ListClientMetricsResourcesResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Client metrics resource names.
    pub client_metrics_resources: Vec<ClientMetricsResource>,
}

impl ListClientMetricsResourcesResponse {
    /// Decode from version 0 (flexible from v0).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let resource_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut client_metrics_resources = Vec::with_capacity(resource_count);
        for _ in 0..resource_count {
            let name =
                non_nullable_string("metric resource name", KafkaString::decode_compact(buf)?.0)?;
            let _ = TaggedFields::decode(buf)?;
            client_metrics_resources.push(ClientMetricsResource { name });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            client_metrics_resources,
        })
    }
}

impl VersionedEncode for ListClientMetricsResourcesRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            _ => return unsupported_encode!("ListClientMetricsResourcesRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for ListClientMetricsResourcesResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            _ => unsupported_decode!("ListClientMetricsResourcesResponse", version),
        }
    }
}

#[cfg(test)]
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
    fn test_list_client_metrics_resources_request_encode_v0() {
        let req = ListClientMetricsResourcesRequest;
        let mut buf = BytesMut::new();
        req.encode_v0(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_u8(), 0); // tagged fields only
        assert!(cur.is_empty());
    }

    #[test]
    fn test_list_client_metrics_resources_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        varint::encode_unsigned_varint(3, &mut buf); // 2 resources
        put_compact_string(&mut buf, Some("metric-a"));
        put_tagged_fields(&mut buf); // resource tagged fields
        put_compact_string(&mut buf, Some("metric-b"));
        put_tagged_fields(&mut buf); // resource tagged fields
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = ListClientMetricsResourcesResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 0);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.client_metrics_resources.len(), 2);
        assert_eq!(resp.client_metrics_resources[0].name, "metric-a");
        assert_eq!(resp.client_metrics_resources[1].name, "metric-b");
    }

    #[test]
    fn test_list_client_metrics_resources_response_decode_v0_empty() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i16(0); // error_code
        varint::encode_unsigned_varint(1, &mut buf); // 0 resources
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = ListClientMetricsResourcesResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        assert!(resp.client_metrics_resources.is_empty());
    }
}
