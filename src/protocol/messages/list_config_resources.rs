use crate::protocol::decode_capacity;
use bytes::{Buf, BufMut};

use super::{ConfigResourceType, VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::check_compact_array_len;
use crate::protocol::encode_compact_array_len;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};

// ============================================================================
// ListConfigResources API (Key 74)
//
// Flexible from v0.
//
// Kafka 4.1 renamed this API from `ListClientMetricsResources` (KIP-1142).
// v0 is byte-identical to the old v0 and lists only client-metrics resources.
// v1 adds a requested `ResourceTypes` list to the request and echoes the
// resource type back on every response entry.
// ============================================================================

/// ListConfigResources request (API key 74). Flexible from v0.
#[derive(Debug, Clone, Default)]
pub struct ListConfigResourcesRequest {
    /// Resource types to list (v1+).
    ///
    /// Empty means "every type the broker supports by default". Ignored when
    /// the negotiated version is v0, which can only list client metrics.
    pub resource_types: Vec<ConfigResourceType>,
}

impl ListConfigResourcesRequest {
    /// Request every resource type the broker lists by default.
    pub fn all() -> Self {
        Self::default()
    }

    /// Request only the given resource types (v1+).
    pub fn with_types(resource_types: Vec<ConfigResourceType>) -> Self {
        Self { resource_types }
    }

    /// Encode for version 0 (empty body, flexible so tagged fields still trail).
    ///
    /// `resource_types` is not on the v0 wire: the broker always answers with
    /// client-metrics resources only.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 1 (adds the `ResourceTypes` array, KIP-1142).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.resource_types.len(), buf)?;
        for ty in &self.resource_types {
            ty.to_i8().encode(buf);
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// A config resource named in a `ListConfigResources` response.
#[derive(Debug, Clone)]
pub struct ListedConfigResource {
    /// Resource name.
    pub name: String,
    /// Resource type (v1+).
    ///
    /// The field is `ignorable` in the Kafka schema with a default of
    /// `CLIENT_METRICS`, which is also what a v0 response implies, so v0
    /// decodes report [`ConfigResourceType::ClientMetrics`].
    pub resource_type: ConfigResourceType,
}

/// ListConfigResources response (API key 74). Flexible from v0.
#[derive(Debug, Clone)]
pub struct ListConfigResourcesResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// The config resources the broker knows about.
    pub config_resources: Vec<ListedConfigResource>,
}

impl ListConfigResourcesResponse {
    /// Decode from version 0 (flexible from v0).
    ///
    /// v0 predates KIP-1142 and only ever lists client-metrics subscriptions,
    /// so every entry is reported as [`ConfigResourceType::ClientMetrics`].
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        Self::decode(buf, false)
    }

    /// Decode from version 1 (adds `ResourceType` per entry, KIP-1142).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        Self::decode(buf, true)
    }

    fn decode(buf: &mut impl Buf, with_resource_type: bool) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let resource_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut config_resources =
            Vec::with_capacity(decode_capacity(resource_count, buf.remaining()));
        for _ in 0..resource_count {
            let name =
                non_nullable_string("config resource name", KafkaString::decode_compact(buf)?.0)?;
            let resource_type = if with_resource_type {
                ConfigResourceType::from_i8(i8::decode(buf)?)
            } else {
                ConfigResourceType::ClientMetrics
            };
            let _ = TaggedFields::decode(buf)?;
            config_resources.push(ListedConfigResource {
                name,
                resource_type,
            });
        }
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            throttle_time_ms,
            error_code,
            config_resources,
        })
    }
}

impl VersionedEncode for ListConfigResourcesRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            _ => return unsupported_encode!("ListConfigResourcesRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for ListConfigResourcesResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            _ => unsupported_decode!("ListConfigResourcesResponse", version),
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
    fn resource_type_wire_values_round_trip() {
        // The values are sparse; a dense assumption would silently mis-tag.
        for (ty, wire) in [
            (ConfigResourceType::Group, 32i8),
            (ConfigResourceType::ClientMetrics, 16),
            (ConfigResourceType::BrokerLogger, 8),
            (ConfigResourceType::Broker, 4),
            (ConfigResourceType::Topic, 2),
        ] {
            assert_eq!(ty.to_i8(), wire);
            assert_eq!(ConfigResourceType::from_i8(wire), ty);
        }
        // A type this client does not know about degrades to Unknown instead
        // of failing the decode.
        assert_eq!(ConfigResourceType::from_i8(64), ConfigResourceType::Unknown);
    }

    #[test]
    fn request_encode_v0_is_tagged_fields_only() {
        // v0 has no fields; the resource types must not leak onto the wire.
        let req = ListConfigResourcesRequest::with_types(vec![ConfigResourceType::Topic]);
        let mut buf = BytesMut::new();
        req.encode_v0(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_u8(), 0);
        assert!(cur.is_empty());
    }

    #[test]
    fn request_encode_v1_emits_resource_types() {
        let req = ListConfigResourcesRequest::with_types(vec![
            ConfigResourceType::Topic,
            ConfigResourceType::Group,
        ]);
        let mut buf = BytesMut::new();
        req.encode_v1(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(varint::decode_unsigned_varint(&mut cur).unwrap(), 3); // 2 + 1
        assert_eq!(cur.get_i8(), 2); // Topic
        assert_eq!(cur.get_i8(), 32); // Group
        assert_eq!(cur.get_u8(), 0); // tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn request_encode_v1_empty_means_broker_default() {
        let req = ListConfigResourcesRequest::all();
        let mut buf = BytesMut::new();
        req.encode_v1(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(varint::decode_unsigned_varint(&mut cur).unwrap(), 1); // 0 + 1
        assert_eq!(cur.get_u8(), 0);
        assert!(cur.is_empty());
    }

    #[test]
    fn response_decode_v0_reports_client_metrics() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        varint::encode_unsigned_varint(3, &mut buf); // 2 resources
        put_compact_string(&mut buf, Some("metric-a"));
        put_tagged_fields(&mut buf);
        put_compact_string(&mut buf, Some("metric-b"));
        put_tagged_fields(&mut buf);
        put_tagged_fields(&mut buf); // top-level

        let resp = ListConfigResourcesResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 0);
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.config_resources.len(), 2);
        assert_eq!(resp.config_resources[0].name, "metric-a");
        // v0 predates KIP-1142 and can only list client metrics.
        assert_eq!(
            resp.config_resources[0].resource_type,
            ConfigResourceType::ClientMetrics
        );
        assert_eq!(resp.config_resources[1].name, "metric-b");
    }

    #[test]
    fn response_decode_v0_empty() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i16(0); // error_code
        varint::encode_unsigned_varint(1, &mut buf); // 0 resources
        put_tagged_fields(&mut buf);

        let resp = ListConfigResourcesResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        assert!(resp.config_resources.is_empty());
    }

    #[test]
    fn response_decode_v1_carries_resource_type() {
        let mut buf = BytesMut::new();
        buf.put_i32(7); // throttle_time_ms
        buf.put_i16(0); // error_code
        varint::encode_unsigned_varint(4, &mut buf); // 3 resources
        put_compact_string(&mut buf, Some("my-topic"));
        buf.put_i8(2); // Topic
        put_tagged_fields(&mut buf);
        put_compact_string(&mut buf, Some("my-group"));
        buf.put_i8(32); // Group
        put_tagged_fields(&mut buf);
        put_compact_string(&mut buf, Some("future"));
        buf.put_i8(64); // a type this client does not know
        put_tagged_fields(&mut buf);
        put_tagged_fields(&mut buf); // top-level

        let resp = ListConfigResourcesResponse::decode_v1(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 7);
        assert_eq!(resp.config_resources.len(), 3);
        assert_eq!(
            resp.config_resources[0].resource_type,
            ConfigResourceType::Topic
        );
        assert_eq!(
            resp.config_resources[1].resource_type,
            ConfigResourceType::Group
        );
        assert_eq!(
            resp.config_resources[2].resource_type,
            ConfigResourceType::Unknown
        );
    }

    #[test]
    fn versioned_dispatch_rejects_unknown_versions() {
        let req = ListConfigResourcesRequest::all();
        for v in [0, 1] {
            let mut buf = BytesMut::new();
            req.encode_versioned(v, &mut buf).unwrap();
            assert!(!buf.is_empty());
        }
        let mut buf = BytesMut::new();
        assert!(req.encode_versioned(2, &mut buf).is_err());

        let mut empty = BytesMut::new().freeze();
        assert!(ListConfigResourcesResponse::decode_versioned(2, &mut empty).is_err());
    }
}
