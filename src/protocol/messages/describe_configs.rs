use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, decode_capacity,
    encode_compact_array_len,
};

// ============================================================================
// DescribeConfigs API (Key 32)
// ============================================================================

/// Resource type for config operations.
///
/// Mirrors Kafka's `ConfigResource.Type`. The wire values are sparse rather
/// than sequential, so never assume a dense range when converting.
///
/// Used by `DescribeConfigs` / `IncrementalAlterConfigs` and by
/// `ListConfigResources` (API key 74, KIP-1142).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConfigResourceType {
    /// Unknown resource type. Also the fallback for a wire value this client
    /// does not recognise.
    Unknown = 0,
    /// Topic resource.
    Topic = 2,
    /// Broker resource.
    Broker = 4,
    /// Broker logger resource.
    BrokerLogger = 8,
    /// Client metrics subscription resource (KIP-714).
    ClientMetrics = 16,
    /// Consumer/share group resource (KIP-1142).
    Group = 32,
}

impl ConfigResourceType {
    /// Convert from i8. Unrecognised values become
    /// [`ConfigResourceType::Unknown`] rather than an error, so a newer broker
    /// never turns a response into a decode failure.
    #[inline]
    pub fn from_i8(value: i8) -> Self {
        match value {
            2 => Self::Topic,
            4 => Self::Broker,
            8 => Self::BrokerLogger,
            16 => Self::ClientMetrics,
            32 => Self::Group,
            _ => Self::Unknown,
        }
    }

    /// Convert to i8.
    #[inline]
    pub fn to_i8(self) -> i8 {
        self as i8
    }
}

/// DescribeConfigs request.
#[derive(Debug, Clone)]
pub struct DescribeConfigsRequest {
    /// Resources to describe.
    pub resources: Vec<DescribeConfigsResource>,
    /// Include synonyms in response.
    pub include_synonyms: bool,
    /// Include documentation in response.
    pub include_documentation: bool,
}

/// Resource in DescribeConfigs request.
#[derive(Debug, Clone)]
pub struct DescribeConfigsResource {
    /// Resource type.
    pub resource_type: ConfigResourceType,
    /// Resource name (topic name or broker ID as string).
    pub resource_name: String,
    /// Config names to describe (null for all).
    pub config_names: Option<Vec<String>>,
}

impl DescribeConfigsRequest {
    /// Create a request to describe topic configs.
    pub fn for_topic(topic: impl Into<String>) -> Self {
        Self {
            resources: vec![DescribeConfigsResource {
                resource_type: ConfigResourceType::Topic,
                resource_name: topic.into(),
                config_names: None,
            }],
            include_synonyms: false,
            include_documentation: false,
        }
    }

    /// Create a request to describe broker configs.
    pub fn for_broker(broker_id: i32) -> Self {
        Self {
            resources: vec![DescribeConfigsResource {
                resource_type: ConfigResourceType::Broker,
                resource_name: broker_id.to_string(),
                config_names: None,
            }],
            include_synonyms: false,
            include_documentation: false,
        }
    }

    /// Encode for version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        array_len_i32(self.resources.len())?.encode(buf);
        for resource in &self.resources {
            resource.resource_type.to_i8().encode(buf);
            KafkaString::new(&resource.resource_name).try_encode(buf)?;

            match &resource.config_names {
                None => (-1i32).encode(buf),
                Some(names) => {
                    array_len_i32(names.len())?.encode(buf);
                    for name in names {
                        KafkaString::new(name).try_encode(buf)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Encode for versions 1–2 (non-flexible; v1 adds include_synonyms).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        array_len_i32(self.resources.len())?.encode(buf);
        for resource in &self.resources {
            resource.resource_type.to_i8().encode(buf);
            KafkaString::new(&resource.resource_name).try_encode(buf)?;

            match &resource.config_names {
                None => (-1i32).encode(buf),
                Some(names) => {
                    array_len_i32(names.len())?.encode(buf);
                    for name in names {
                        KafkaString::new(name).try_encode(buf)?;
                    }
                }
            }
        }
        buf.put_u8(u8::from(self.include_synonyms));
        Ok(())
    }

    /// Encode for version 3 (non-flexible; adds include_documentation).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        array_len_i32(self.resources.len())?.encode(buf);
        for resource in &self.resources {
            resource.resource_type.to_i8().encode(buf);
            KafkaString::new(&resource.resource_name).try_encode(buf)?;

            match &resource.config_names {
                None => (-1i32).encode(buf),
                Some(names) => {
                    array_len_i32(names.len())?.encode(buf);
                    for name in names {
                        KafkaString::new(name).try_encode(buf)?;
                    }
                }
            }
        }
        buf.put_u8(u8::from(self.include_synonyms));
        buf.put_u8(u8::from(self.include_documentation));
        Ok(())
    }

    /// Encode for version 4 (flexible encoding).
    pub fn encode_v4(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.resources.len(), buf)?;
        for resource in &self.resources {
            resource.resource_type.to_i8().encode(buf);
            KafkaString::new(&resource.resource_name).try_encode_compact(buf)?;

            match &resource.config_names {
                None => {
                    // compact nullable array: 0 = null
                    crate::util::varint::encode_unsigned_varint(0, buf);
                }
                Some(names) => {
                    encode_compact_array_len(names.len(), buf)?;
                    for name in names {
                        KafkaString::new(name).try_encode_compact(buf)?;
                    }
                }
            }
            TaggedFields::default().try_encode(buf)?;
        }
        buf.put_u8(u8::from(self.include_synonyms));
        buf.put_u8(u8::from(self.include_documentation));
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

/// DescribeConfigs response.
#[derive(Debug, Clone)]
pub struct DescribeConfigsResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Results per resource.
    pub results: Vec<DescribeConfigsResult>,
}

/// Result for a resource in DescribeConfigs response.
#[derive(Debug, Clone)]
pub struct DescribeConfigsResult {
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// Resource type.
    pub resource_type: ConfigResourceType,
    /// Resource name.
    pub resource_name: String,
    /// Configuration entries.
    pub configs: Vec<DescribeConfigsEntry>,
}

/// Configuration entry in DescribeConfigs response.
#[derive(Debug, Clone)]
pub struct DescribeConfigsEntry {
    /// Config name.
    pub name: String,
    /// Config value.
    pub value: Option<String>,
    /// Whether the config is read-only.
    pub read_only: bool,
    /// Whether the config is the default value (v0 only; v1+ uses config_source).
    pub is_default: bool,
    /// Whether the config is sensitive.
    pub is_sensitive: bool,
    /// Configuration source (v1+). -1 if not available.
    pub config_source: i8,
    /// Synonyms for this configuration key (v1+).
    pub synonyms: Vec<ConfigSynonym>,
    /// Configuration data type (v3+). 0 = UNKNOWN.
    pub config_type: i8,
    /// Configuration documentation (v3+).
    pub documentation: Option<String>,
}

/// A synonym for a configuration key in DescribeConfigs response (v1+).
#[derive(Debug, Clone)]
pub struct ConfigSynonym {
    /// Synonym name.
    pub name: String,
    /// Synonym value.
    pub value: Option<String>,
    /// Synonym source.
    pub source: i8,
}

impl DescribeConfigsResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut results = Vec::with_capacity(decode_capacity(result_count, buf.remaining()));

        for _ in 0..result_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;
            let resource_type = ConfigResourceType::from_i8(i8::decode(buf)?);
            let resource_name = non_nullable_string("resource name", KafkaString::decode(buf)?.0)?;

            let config_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut configs = Vec::with_capacity(decode_capacity(config_count, buf.remaining()));

            for _ in 0..config_count {
                let name = non_nullable_string("config entry name", KafkaString::decode(buf)?.0)?;
                let value = KafkaString::decode(buf)?.0;
                let read_only = i8::decode(buf)? != 0;
                let is_default = i8::decode(buf)? != 0;
                let is_sensitive = i8::decode(buf)? != 0;

                configs.push(DescribeConfigsEntry {
                    name,
                    value,
                    read_only,
                    is_default,
                    is_sensitive,
                    config_source: -1,
                    synonyms: Vec::new(),
                    config_type: 0,
                    documentation: None,
                });
            }

            results.push(DescribeConfigsResult {
                error_code,
                error_message,
                resource_type,
                resource_name,
                configs,
            });
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    /// Decode from version 1–2 (non-flexible; adds config_source, synonyms).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut results = Vec::with_capacity(decode_capacity(result_count, buf.remaining()));

        for _ in 0..result_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;
            let resource_type = ConfigResourceType::from_i8(i8::decode(buf)?);
            let resource_name = non_nullable_string("resource name", KafkaString::decode(buf)?.0)?;

            let config_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut configs = Vec::with_capacity(decode_capacity(config_count, buf.remaining()));

            for _ in 0..config_count {
                let name = non_nullable_string("config entry name", KafkaString::decode(buf)?.0)?;
                let value = KafkaString::decode(buf)?.0;
                let read_only = i8::decode(buf)? != 0;
                let config_source = i8::decode(buf)?;
                let is_sensitive = i8::decode(buf)? != 0;

                // Decode synonyms array
                let synonym_count = check_decode_array_len(i32::decode(buf)?)?;
                let mut synonyms =
                    Vec::with_capacity(decode_capacity(synonym_count, buf.remaining()));
                for _ in 0..synonym_count {
                    let syn_name =
                        non_nullable_string("synonym name", KafkaString::decode(buf)?.0)?;
                    let syn_value = KafkaString::decode(buf)?.0;
                    let syn_source = i8::decode(buf)?;
                    synonyms.push(ConfigSynonym {
                        name: syn_name,
                        value: syn_value,
                        source: syn_source,
                    });
                }

                configs.push(DescribeConfigsEntry {
                    name,
                    value,
                    read_only,
                    is_default: false,
                    is_sensitive,
                    config_source,
                    synonyms,
                    config_type: 0,
                    documentation: None,
                });
            }

            results.push(DescribeConfigsResult {
                error_code,
                error_message,
                resource_type,
                resource_name,
                configs,
            });
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    /// Decode from version 3 (non-flexible; adds config_type, documentation).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut results = Vec::with_capacity(decode_capacity(result_count, buf.remaining()));

        for _ in 0..result_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;
            let resource_type = ConfigResourceType::from_i8(i8::decode(buf)?);
            let resource_name = non_nullable_string("resource name", KafkaString::decode(buf)?.0)?;

            let config_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut configs = Vec::with_capacity(decode_capacity(config_count, buf.remaining()));

            for _ in 0..config_count {
                let name = non_nullable_string("config entry name", KafkaString::decode(buf)?.0)?;
                let value = KafkaString::decode(buf)?.0;
                let read_only = i8::decode(buf)? != 0;
                let config_source = i8::decode(buf)?;
                let is_sensitive = i8::decode(buf)? != 0;

                let synonym_count = check_decode_array_len(i32::decode(buf)?)?;
                let mut synonyms =
                    Vec::with_capacity(decode_capacity(synonym_count, buf.remaining()));
                for _ in 0..synonym_count {
                    let syn_name =
                        non_nullable_string("synonym name", KafkaString::decode(buf)?.0)?;
                    let syn_value = KafkaString::decode(buf)?.0;
                    let syn_source = i8::decode(buf)?;
                    synonyms.push(ConfigSynonym {
                        name: syn_name,
                        value: syn_value,
                        source: syn_source,
                    });
                }

                let config_type = i8::decode(buf)?;
                let documentation = KafkaString::decode(buf)?.0;

                configs.push(DescribeConfigsEntry {
                    name,
                    value,
                    read_only,
                    is_default: false,
                    is_sensitive,
                    config_source,
                    synonyms,
                    config_type,
                    documentation,
                });
            }

            results.push(DescribeConfigsResult {
                error_code,
                error_message,
                resource_type,
                resource_name,
                configs,
            });
        }

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }

    /// Decode from version 4 (flexible encoding).
    pub fn decode_v4(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let result_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut results = Vec::with_capacity(decode_capacity(result_count, buf.remaining()));

        for _ in 0..result_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            let resource_type = ConfigResourceType::from_i8(i8::decode(buf)?);
            let resource_name =
                non_nullable_string("resource name", KafkaString::decode_compact(buf)?.0)?;

            let config_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut configs = Vec::with_capacity(decode_capacity(config_count, buf.remaining()));

            for _ in 0..config_count {
                let name =
                    non_nullable_string("config entry name", KafkaString::decode_compact(buf)?.0)?;
                let value = KafkaString::decode_compact(buf)?.0;
                let read_only = i8::decode(buf)? != 0;
                let config_source = i8::decode(buf)?;
                let is_sensitive = i8::decode(buf)? != 0;

                let synonym_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut synonyms =
                    Vec::with_capacity(decode_capacity(synonym_count, buf.remaining()));
                for _ in 0..synonym_count {
                    let syn_name =
                        non_nullable_string("synonym name", KafkaString::decode_compact(buf)?.0)?;
                    let syn_value = KafkaString::decode_compact(buf)?.0;
                    let syn_source = i8::decode(buf)?;
                    let _ = TaggedFields::decode(buf)?;
                    synonyms.push(ConfigSynonym {
                        name: syn_name,
                        value: syn_value,
                        source: syn_source,
                    });
                }

                let config_type = i8::decode(buf)?;
                let documentation = KafkaString::decode_compact(buf)?.0;
                let _ = TaggedFields::decode(buf)?;

                configs.push(DescribeConfigsEntry {
                    name,
                    value,
                    read_only,
                    is_default: false,
                    is_sensitive,
                    config_source,
                    synonyms,
                    config_type,
                    documentation,
                });
            }

            let _ = TaggedFields::decode(buf)?;

            results.push(DescribeConfigsResult {
                error_code,
                error_message,
                resource_type,
                resource_name,
                configs,
            });
        }

        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            results,
        })
    }
}

impl VersionedEncode for DescribeConfigsRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 | 2 => self.encode_v1(buf)?,
            3 => self.encode_v3(buf)?,
            4 => self.encode_v4(buf)?,
            _ => return unsupported_encode!("DescribeConfigsRequest", version),
        }
        Ok(())
    }
}

impl VersionedDecode for DescribeConfigsResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 | 2 => Self::decode_v1(buf),
            3 => Self::decode_v3(buf),
            4 => Self::decode_v4(buf),
            _ => unsupported_decode!("DescribeConfigsResponse", version),
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
    fn test_describe_configs_request_encode_v1_round_trip() {
        let req = DescribeConfigsRequest {
            resources: vec![DescribeConfigsResource {
                resource_type: ConfigResourceType::Topic,
                resource_name: "test-topic".to_string(),
                config_names: Some(vec!["retention.ms".to_string()]),
            }],
            include_synonyms: true,
            include_documentation: false,
        };
        let mut buf = BytesMut::new();
        req.encode_v1(&mut buf).unwrap();

        let mut cur = &buf[..];
        assert_eq!(cur.get_i32(), 1); // 1 resource
        assert_eq!(cur.get_i8(), 2); // Topic = 2
        let name_len = cur.get_i16() as usize;
        let mut name_bytes = vec![0u8; name_len];
        cur.copy_to_slice(&mut name_bytes);
        assert_eq!(name_bytes, b"test-topic");
        assert_eq!(cur.get_i32(), 1); // 1 config name
        let cfg_len = cur.get_i16() as usize;
        let mut cfg_bytes = vec![0u8; cfg_len];
        cur.copy_to_slice(&mut cfg_bytes);
        assert_eq!(cfg_bytes, b"retention.ms");
        assert_eq!(cur.get_u8(), 1); // include_synonyms = true
        assert!(cur.is_empty());
    }

    #[test]
    fn test_describe_configs_response_decode_v1_with_synonyms() {
        let mut buf = BytesMut::new();
        buf.put_i32(10); // throttle_time_ms
        buf.put_i32(1); // 1 result
        buf.put_i16(0); // error_code NONE
        buf.put_i16(-1); // error_message null
        buf.put_i8(2); // resource_type = Topic
        buf.put_i16(5);
        buf.put_slice(b"topic");
        buf.put_i32(1); // 1 config entry
        buf.put_i16(12);
        buf.put_slice(b"retention.ms");
        buf.put_i16(6);
        buf.put_slice(b"604800"); // value
        buf.put_i8(0); // read_only = false
        buf.put_i8(5); // config_source = DYNAMIC_TOPIC_CONFIG
        buf.put_i8(0); // is_sensitive = false
        buf.put_i32(1); // 1 synonym
        buf.put_i16(12);
        buf.put_slice(b"retention.ms");
        buf.put_i16(6);
        buf.put_slice(b"604800");
        buf.put_i8(5); // source

        let resp = DescribeConfigsResponse::decode_v1(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 10);
        assert_eq!(resp.results.len(), 1);
        let r = &resp.results[0];
        assert!(r.error_code.is_ok());
        assert_eq!(r.resource_name, "topic");
        assert_eq!(r.configs.len(), 1);
        let c = &r.configs[0];
        assert_eq!(c.name, "retention.ms");
        assert_eq!(c.value.as_deref(), Some("604800"));
        assert_eq!(c.config_source, 5);
        assert_eq!(c.synonyms.len(), 1);
        assert_eq!(c.synonyms[0].name, "retention.ms");
        assert_eq!(c.synonyms[0].source, 5);
        assert_eq!(c.config_type, 0);
        assert!(c.documentation.is_none());
    }

    #[test]
    fn test_describe_configs_response_decode_v3_with_type_and_docs() {
        let mut buf = BytesMut::new();
        buf.put_i32(5); // throttle_time_ms
        buf.put_i32(1); // 1 result
        buf.put_i16(0); // error_code NONE
        buf.put_i16(-1); // error_message null
        buf.put_i8(2); // resource_type = Topic
        buf.put_i16(1);
        buf.put_slice(b"t"); // resource_name
        buf.put_i32(1); // 1 config entry
        buf.put_i16(3);
        buf.put_slice(b"key"); // name
        buf.put_i16(3);
        buf.put_slice(b"val"); // value
        buf.put_i8(1); // read_only = true
        buf.put_i8(1); // config_source = DYNAMIC_TOPIC_CONFIG
        buf.put_i8(0); // is_sensitive = false
        buf.put_i32(0); // 0 synonyms
        buf.put_i8(3); // config_type = STRING
        buf.put_i16(4);
        buf.put_slice(b"docs"); // documentation

        let resp = DescribeConfigsResponse::decode_v3(&mut buf.freeze()).unwrap();
        let c = &resp.results[0].configs[0];
        assert_eq!(c.name, "key");
        assert!(c.read_only);
        assert_eq!(c.config_source, 1);
        assert_eq!(c.config_type, 3);
        assert_eq!(c.documentation.as_deref(), Some("docs"));
    }

    #[test]
    fn test_describe_configs_request_encode_v4_flexible() {
        let req = DescribeConfigsRequest {
            resources: vec![DescribeConfigsResource {
                resource_type: ConfigResourceType::Broker,
                resource_name: "0".to_string(),
                config_names: None,
            }],
            include_synonyms: true,
            include_documentation: true,
        };
        let mut buf = BytesMut::new();
        req.encode_v4(&mut buf).unwrap();

        let mut cur = &buf[..];
        let arr_varint = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(arr_varint, 2); // 1 resource + 1
        assert_eq!(cur.get_i8(), 4); // Broker = 4
        let name_varint = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(name_varint, 2); // len("0") + 1
        assert_eq!(cur.get_u8(), b'0');
        let null_varint = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(null_varint, 0);
        assert_eq!(cur.get_u8(), 0); // resource tagged fields
        assert_eq!(cur.get_u8(), 1); // include_synonyms
        assert_eq!(cur.get_u8(), 1); // include_documentation
        assert_eq!(cur.get_u8(), 0); // top-level tagged fields
        assert!(cur.is_empty());
    }

    #[test]
    fn test_describe_configs_response_decode_v4_flexible() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(2, &mut buf); // 1 result
        buf.put_i16(0); // error_code
        put_compact_string(&mut buf, None); // error_message null
        buf.put_i8(2); // resource_type = Topic
        put_compact_string(&mut buf, Some("tp")); // resource_name
        varint::encode_unsigned_varint(2, &mut buf); // 1 config
        put_compact_string(&mut buf, Some("k")); // name
        put_compact_string(&mut buf, Some("v")); // value
        buf.put_i8(0); // read_only
        buf.put_i8(2); // config_source
        buf.put_i8(0); // is_sensitive
        varint::encode_unsigned_varint(2, &mut buf); // 1 synonym
        put_compact_string(&mut buf, Some("k")); // synonym name
        put_compact_string(&mut buf, Some("v")); // synonym value
        buf.put_i8(2); // synonym source
        put_tagged_fields(&mut buf); // synonym tagged fields
        buf.put_i8(1); // config_type = BOOLEAN
        put_compact_string(&mut buf, Some("doc")); // documentation
        put_tagged_fields(&mut buf); // config entry tagged fields
        put_tagged_fields(&mut buf); // result tagged fields
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = DescribeConfigsResponse::decode_v4(&mut buf.freeze()).unwrap();
        assert_eq!(resp.results.len(), 1);
        let c = &resp.results[0].configs[0];
        assert_eq!(c.name, "k");
        assert_eq!(c.value.as_deref(), Some("v"));
        assert_eq!(c.config_source, 2);
        assert_eq!(c.synonyms.len(), 1);
        assert_eq!(c.config_type, 1);
        assert_eq!(c.documentation.as_deref(), Some("doc"));
    }

    // ── Regression: allocation amplification ───────────────────────────

    /// A tiny hostile body must not drive a large pre-allocation.
    ///
    /// This body is ~34 bytes and declares three nested counts at the
    /// `MAX_DECODE_ARRAY_LEN` ceiling. The length checks all pass — the counts
    /// are individually "legal" — but no element could possibly fit. Before the
    /// `decode_capacity` clamp, the three `Vec::with_capacity` calls reserved
    /// roughly 25 MB (~7e5 amplification) before the first inner decode failed,
    /// repeatable once per response per connection.
    ///
    /// The clamp bounds each reservation by the bytes remaining, so total
    /// allocation is now proportional to the response size. The observable
    /// assertion is that decoding fails promptly and returns an error rather
    /// than consuming memory.
    #[test]
    fn decode_v3_hostile_nested_counts_do_not_over_allocate() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i32(100_000); // result_count — at the safety ceiling
        // First result: enough header bytes to reach the nested counts.
        buf.put_i16(0); // error_code
        buf.put_i16(-1); // error_message: null
        buf.put_i8(2); // resource_type: topic
        buf.put_i16(0); // resource_name: ""
        buf.put_i32(100_000); // config_count — at the ceiling
        // First config entry, up to the synonyms count.
        buf.put_i16(0); // name: ""
        buf.put_i16(-1); // value: null
        buf.put_i8(0); // read_only
        buf.put_i8(0); // config_source
        buf.put_i8(0); // is_sensitive
        buf.put_i32(100_000); // synonym_count — at the ceiling
        // ...and the body simply ends here.

        let len = buf.len();
        assert!(len < 64, "hostile body should be tiny, was {len} bytes");

        let result = DescribeConfigsResponse::decode_v3(&mut buf.freeze());
        assert!(
            result.is_err(),
            "a body that ends mid-element must be rejected"
        );
    }

    /// The same shape in the flexible (v4) encoding.
    #[test]
    fn decode_v4_hostile_nested_counts_do_not_over_allocate() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        varint::encode_unsigned_varint(100_001, &mut buf); // result_count + 1
        buf.put_i16(0); // error_code
        varint::encode_unsigned_varint(0, &mut buf); // error_message: null
        buf.put_i8(2); // resource_type
        varint::encode_unsigned_varint(1, &mut buf); // resource_name: ""
        varint::encode_unsigned_varint(100_001, &mut buf); // config_count + 1

        let len = buf.len();
        assert!(len < 64, "hostile body should be tiny, was {len} bytes");

        assert!(DescribeConfigsResponse::decode_v4(&mut buf.freeze()).is_err());
    }
}
