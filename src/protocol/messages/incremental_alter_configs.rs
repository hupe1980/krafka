use bytes::{Buf, BufMut};

use super::{ConfigResourceType, VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, encode_compact_array_len,
};

// ============================================================================
// IncrementalAlterConfigs API (Key 44)
// ============================================================================

/// Operation type for incremental config alteration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum AlterConfigOp {
    /// Set the config value.
    Set = 0,
    /// Delete the config (revert to default).
    Delete = 1,
    /// Append to a list config.
    Append = 2,
    /// Subtract from a list config.
    Subtract = 3,
}

impl AlterConfigOp {
    /// Convert from raw i8 value.
    ///
    /// Returns `None` for unrecognized operation codes.
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            0 => Some(Self::Set),
            1 => Some(Self::Delete),
            2 => Some(Self::Append),
            3 => Some(Self::Subtract),
            _ => None,
        }
    }

    /// Convert to raw i8 value.
    pub fn to_i8(self) -> i8 {
        self as i8
    }
}

/// IncrementalAlterConfigs request (API Key 44).
#[derive(Debug, Clone)]
pub struct IncrementalAlterConfigsRequest {
    /// Resources to alter incrementally.
    pub resources: Vec<IncrementalAlterConfigsResource>,
    /// If true, validate without actually changing configs.
    pub validate_only: bool,
}

/// Resource in IncrementalAlterConfigs request.
#[derive(Debug, Clone)]
pub struct IncrementalAlterConfigsResource {
    /// Resource type.
    pub resource_type: ConfigResourceType,
    /// Resource name (topic name or broker ID as string).
    pub resource_name: String,
    /// Configuration alterations.
    pub configs: Vec<AlterableConfig>,
}

/// A single config alteration in IncrementalAlterConfigs request.
#[derive(Debug, Clone)]
pub struct AlterableConfig {
    /// Config key name.
    pub name: String,
    /// Operation type (SET, DELETE, APPEND, SUBTRACT).
    pub config_operation: AlterConfigOp,
    /// Config value (null for DELETE).
    pub value: Option<String>,
}

impl IncrementalAlterConfigsRequest {
    /// Create a request to incrementally alter topic configs.
    pub fn for_topic(topic: impl Into<String>, configs: Vec<AlterableConfig>) -> Self {
        Self {
            resources: vec![IncrementalAlterConfigsResource {
                resource_type: ConfigResourceType::Topic,
                resource_name: topic.into(),
                configs,
            }],
            validate_only: false,
        }
    }

    /// Encode for version 0 (non-flexible).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        array_len_i32(self.resources.len())?.encode(buf);
        for resource in &self.resources {
            resource.resource_type.to_i8().encode(buf);
            KafkaString::new(&resource.resource_name).try_encode(buf)?;

            array_len_i32(resource.configs.len())?.encode(buf);
            for config in &resource.configs {
                KafkaString::new(&config.name).try_encode(buf)?;
                config.config_operation.to_i8().encode(buf);
                match &config.value {
                    Some(v) => KafkaString::new(v).try_encode(buf)?,
                    None => KafkaString::null().try_encode(buf)?,
                }
            }
        }
        buf.put_u8(u8::from(self.validate_only));
        Ok(())
    }

    /// Encode for version 1 (flexible encoding).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.resources.len(), buf)?;
        for resource in &self.resources {
            resource.resource_type.to_i8().encode(buf);
            KafkaString::new(&resource.resource_name).try_encode_compact(buf)?;

            encode_compact_array_len(resource.configs.len(), buf)?;
            for config in &resource.configs {
                KafkaString::new(&config.name).try_encode_compact(buf)?;
                config.config_operation.to_i8().encode(buf);
                match &config.value {
                    Some(v) => KafkaString(Some(v.clone())).try_encode_compact(buf)?,
                    None => KafkaString::null().try_encode_compact(buf)?,
                }
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        buf.put_u8(u8::from(self.validate_only));
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// IncrementalAlterConfigs response (API Key 44).
#[derive(Debug, Clone)]
pub struct IncrementalAlterConfigsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Results per resource.
    pub results: Vec<IncrementalAlterConfigsResult>,
}

/// Result for a resource in IncrementalAlterConfigs response.
#[derive(Debug, Clone)]
pub struct IncrementalAlterConfigsResult {
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// Resource type.
    pub resource_type: ConfigResourceType,
    /// Resource name.
    pub resource_name: String,
}

impl IncrementalAlterConfigsResponse {
    /// Decode from version 0 (non-flexible).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;
            let resource_type = ConfigResourceType::from_i8(i8::decode(buf)?);
            let resource_name = non_nullable_string("resource name", KafkaString::decode(buf)?.0)?;

            results.push(IncrementalAlterConfigsResult {
                error_code,
                error_message,
                resource_type,
                resource_name,
            });
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    /// Decode from version 1 (flexible encoding).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            let resource_type = ConfigResourceType::from_i8(i8::decode(buf)?);
            let resource_name =
                non_nullable_string("resource name", KafkaString::decode_compact(buf)?.0)?;
            let _ = TaggedFields::decode(buf)?;

            results.push(IncrementalAlterConfigsResult {
                error_code,
                error_message,
                resource_type,
                resource_name,
            });
        }

        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }
}

impl VersionedEncode for IncrementalAlterConfigsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            _ => return unsupported_encode!("IncrementalAlterConfigsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for IncrementalAlterConfigsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            _ => unsupported_decode!("IncrementalAlterConfigsResponse", version),
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
    fn test_incremental_alter_configs_request_encode_v0_round_trip() {
        let req = IncrementalAlterConfigsRequest {
            resources: vec![IncrementalAlterConfigsResource {
                resource_type: ConfigResourceType::Topic,
                resource_name: "t".to_string(),
                configs: vec![AlterableConfig {
                    name: "k".to_string(),
                    config_operation: AlterConfigOp::Set,
                    value: Some("v".to_string()),
                }],
            }],
            validate_only: false,
        };
        let mut buf = BytesMut::new();
        req.encode_v0(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_i32(), 1); // 1 resource
        assert_eq!(cur.get_i8(), 2); // Topic
        assert_eq!(cur.get_i16(), 1); // name len
        assert_eq!(cur.get_u8(), b't');
        assert_eq!(cur.get_i32(), 1); // 1 config
        assert_eq!(cur.get_i16(), 1); // key len
        assert_eq!(cur.get_u8(), b'k');
        assert_eq!(cur.get_i8(), 0); // config_operation = SET
        assert_eq!(cur.get_i16(), 1); // value len
        assert_eq!(cur.get_u8(), b'v');
        assert_eq!(cur.get_u8(), 0); // validate_only = false
        assert!(cur.is_empty());
    }

    #[test]
    fn test_incremental_alter_configs_request_encode_v1_flexible() {
        let req = IncrementalAlterConfigsRequest {
            resources: vec![IncrementalAlterConfigsResource {
                resource_type: ConfigResourceType::Topic,
                resource_name: "t".to_string(),
                configs: vec![AlterableConfig {
                    name: "k".to_string(),
                    config_operation: AlterConfigOp::Delete,
                    value: None,
                }],
            }],
            validate_only: true,
        };
        let mut buf = BytesMut::new();
        req.encode_v1(&mut buf).unwrap();

        let mut cur = &buf[..];
        let arr = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(arr, 2); // 1 + 1
        assert_eq!(cur.get_i8(), 2); // Topic
        let name_v = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(name_v, 2); // len("t") + 1
        assert_eq!(cur.get_u8(), b't');
        let cfg_arr = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(cfg_arr, 2); // 1 config + 1
        let key_v = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(key_v, 2);
        assert_eq!(cur.get_u8(), b'k');
        assert_eq!(cur.get_i8(), 1); // DELETE
        let val_v = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(val_v, 0); // null
        assert_eq!(cur.get_u8(), 0); // config tagged fields
        assert_eq!(cur.get_u8(), 0); // resource tagged fields
        assert_eq!(cur.get_u8(), 1); // validate_only = true
        assert_eq!(cur.get_u8(), 0); // top-level tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn test_incremental_alter_configs_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(100); // throttle_time_ms
        buf.put_i32(1); // 1 result
        buf.put_i16(0); // error_code NONE
        buf.put_i16(-1); // error_message null
        buf.put_i8(2); // resource_type = Topic
        buf.put_i16(5);
        buf.put_slice(b"topic");

        let resp = IncrementalAlterConfigsResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 100);
        assert_eq!(resp.results.len(), 1);
        assert!(resp.results[0].error_code.is_ok());
        assert!(resp.results[0].error_message.is_none());
        assert_eq!(resp.results[0].resource_name, "topic");
    }

    #[test]
    fn test_incremental_alter_configs_response_decode_v1_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(50); // throttle_time_ms
        varint::encode_unsigned_varint(2, &mut buf); // 1 result
        buf.put_i16(87); // error_code
        put_compact_string(&mut buf, Some("fail")); // error_message
        buf.put_i8(4); // resource_type = Broker
        put_compact_string(&mut buf, Some("0")); // resource_name
        put_tagged_fields(&mut buf); // result tagged fields
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = IncrementalAlterConfigsResponse::decode_v1(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 50);
        assert_eq!(resp.results.len(), 1);
        assert!(!resp.results[0].error_code.is_ok());
        assert_eq!(resp.results[0].error_message.as_deref(), Some("fail"));
        assert_eq!(resp.results[0].resource_name, "0");
    }

    #[test]
    fn test_incremental_alter_configs_full_frame_v1() {
        use crate::protocol::api::ApiKey;
        use crate::protocol::codec::Encoder;
        use crate::protocol::header::RequestHeader;

        let request = IncrementalAlterConfigsRequest::for_topic(
            "config-alter-topic",
            vec![AlterableConfig {
                name: "retention.ms".to_string(),
                config_operation: AlterConfigOp::Set,
                value: Some("3600000".to_string()),
            }],
        );

        let mut encoder = Encoder::new();
        let pos = encoder.start_message();
        let header =
            RequestHeader::new(ApiKey::IncrementalAlterConfigs, 1, 42).with_client_id("krafka");
        header.encode(encoder.buffer_mut()).unwrap();
        request.encode_v1(encoder.buffer_mut()).unwrap();
        encoder.finish_message(pos).unwrap();
        let bytes = encoder.take();

        let mut cur = &bytes[..];
        let frame_len = i32::decode(&mut cur).unwrap();
        assert_eq!(frame_len as usize, bytes.len() - 4);

        let api_key = i16::decode(&mut cur).unwrap();
        assert_eq!(api_key, 44);
        let api_version = i16::decode(&mut cur).unwrap();
        assert_eq!(api_version, 1);
        let correlation_id = i32::decode(&mut cur).unwrap();
        assert_eq!(correlation_id, 42);

        let client_id_len = i16::decode(&mut cur).unwrap();
        assert_eq!(client_id_len, 6);
        let mut client_id_bytes = vec![0u8; 6];
        cur.copy_to_slice(&mut client_id_bytes);
        assert_eq!(&client_id_bytes, b"krafka");

        let tags = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(tags, 0);

        let resources_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(resources_len, 2);

        let resource_type = cur.get_i8();
        assert_eq!(resource_type, 2);

        let name_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(name_len, 19);
        let mut name_bytes = vec![0u8; 18];
        cur.copy_to_slice(&mut name_bytes);
        assert_eq!(&name_bytes, b"config-alter-topic");

        let configs_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(configs_len, 2);

        let config_name_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(config_name_len, 13);
        let mut config_name = vec![0u8; 12];
        cur.copy_to_slice(&mut config_name);
        assert_eq!(&config_name, b"retention.ms");

        let config_op = cur.get_i8();
        assert_eq!(config_op, 0);

        let config_val_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(config_val_len, 8);
        let mut config_val = vec![0u8; 7];
        cur.copy_to_slice(&mut config_val);
        assert_eq!(&config_val, b"3600000");

        let config_tags = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(config_tags, 0);

        let resource_tags = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(resource_tags, 0);

        let validate_only = cur.get_u8();
        assert_eq!(validate_only, 0);

        let top_tags = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(top_tags, 0);

        assert!(cur.is_empty(), "should have consumed all bytes");
    }
}
