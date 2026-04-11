use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, encode_compact_array_len,
};
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

// ============================================================================
// DescribeConfigs API (Key 32)
// ============================================================================

/// Resource type for config operations.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigResourceType {
    /// Unknown resource type.
    Unknown = 0,
    /// Topic resource.
    Topic = 2,
    /// Broker resource.
    Broker = 4,
    /// Broker logger resource.
    BrokerLogger = 8,
}

impl ConfigResourceType {
    /// Convert from i8.
    #[inline]
    pub fn from_i8(value: i8) -> Self {
        match value {
            2 => Self::Topic,
            4 => Self::Broker,
            8 => Self::BrokerLogger,
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
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;
            let resource_type = ConfigResourceType::from_i8(i8::decode(buf)?);
            let resource_name = non_nullable_string("resource name", KafkaString::decode(buf)?.0)?;

            let config_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut configs = Vec::with_capacity(config_count);

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
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;
            let resource_type = ConfigResourceType::from_i8(i8::decode(buf)?);
            let resource_name = non_nullable_string("resource name", KafkaString::decode(buf)?.0)?;

            let config_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut configs = Vec::with_capacity(config_count);

            for _ in 0..config_count {
                let name = non_nullable_string("config entry name", KafkaString::decode(buf)?.0)?;
                let value = KafkaString::decode(buf)?.0;
                let read_only = i8::decode(buf)? != 0;
                let config_source = i8::decode(buf)?;
                let is_sensitive = i8::decode(buf)? != 0;

                // Decode synonyms array
                let synonym_count = check_decode_array_len(i32::decode(buf)?)?;
                let mut synonyms = Vec::with_capacity(synonym_count);
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
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;
            let resource_type = ConfigResourceType::from_i8(i8::decode(buf)?);
            let resource_name = non_nullable_string("resource name", KafkaString::decode(buf)?.0)?;

            let config_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut configs = Vec::with_capacity(config_count);

            for _ in 0..config_count {
                let name = non_nullable_string("config entry name", KafkaString::decode(buf)?.0)?;
                let value = KafkaString::decode(buf)?.0;
                let read_only = i8::decode(buf)? != 0;
                let config_source = i8::decode(buf)?;
                let is_sensitive = i8::decode(buf)? != 0;

                let synonym_count = check_decode_array_len(i32::decode(buf)?)?;
                let mut synonyms = Vec::with_capacity(synonym_count);
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
        let mut results = Vec::with_capacity(result_count);

        for _ in 0..result_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;
            let resource_type = ConfigResourceType::from_i8(i8::decode(buf)?);
            let resource_name =
                non_nullable_string("resource name", KafkaString::decode_compact(buf)?.0)?;

            let config_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut configs = Vec::with_capacity(config_count);

            for _ in 0..config_count {
                let name =
                    non_nullable_string("config entry name", KafkaString::decode_compact(buf)?.0)?;
                let value = KafkaString::decode_compact(buf)?.0;
                let read_only = i8::decode(buf)? != 0;
                let config_source = i8::decode(buf)?;
                let is_sensitive = i8::decode(buf)? != 0;

                let synonym_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut synonyms = Vec::with_capacity(synonym_count);
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

    // -----------------------------------------------------------------------
    // DescribeConfigs / IncrementalAlterConfigs
    // -----------------------------------------------------------------------

    /// Helper: encode a compact string into `buf`.
    /// Non-null string: varint(len + 1) then bytes.
    /// Null string: varint(0).
    fn put_compact_string(buf: &mut BytesMut, s: Option<&str>) {
        match s {
            Some(val) => {
                // len + 1 fits in one byte for small strings
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

    // ── DescribeConfigs v1 (non-flexible, adds config_source + synonyms) ──

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
        // config entry
        buf.put_i16(12);
        buf.put_slice(b"retention.ms");
        buf.put_i16(6);
        buf.put_slice(b"604800"); // value
        buf.put_i8(0); // read_only = false
        buf.put_i8(5); // config_source = DYNAMIC_TOPIC_CONFIG
        buf.put_i8(0); // is_sensitive = false
        // 1 synonym
        buf.put_i32(1);
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
        // v1 doesn't have config_type or documentation
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
        // null compact nullable array = varint(0)
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

    // ── IncrementalAlterConfigs v0 (non-flexible) / v1 (flexible) ──

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
        // null compact string for value
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

        // Parse it back manually to verify correctness
        let mut cur = &bytes[..];
        // 4-byte length prefix
        let frame_len = i32::decode(&mut cur).unwrap();
        assert_eq!(frame_len as usize, bytes.len() - 4);

        // Request header v2 (flexible for IncrementalAlterConfigs v1)
        let api_key = i16::decode(&mut cur).unwrap();
        assert_eq!(api_key, 44);
        let api_version = i16::decode(&mut cur).unwrap();
        assert_eq!(api_version, 1);
        let correlation_id = i32::decode(&mut cur).unwrap();
        assert_eq!(correlation_id, 42);

        // client_id uses standard (non-compact) encoding per Kafka spec.
        // flexibleVersions: "none" — always uses old-style 2-byte length prefix.
        let client_id_len = i16::decode(&mut cur).unwrap();
        assert_eq!(client_id_len, 6); // "krafka" = 6 bytes
        let mut client_id_bytes = vec![0u8; 6];
        cur.copy_to_slice(&mut client_id_bytes);
        assert_eq!(&client_id_bytes, b"krafka");

        // tagged fields for header
        let tags = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(tags, 0);

        // Now the request body - compact array of resources
        let resources_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(resources_len, 2); // 1 + 1

        // First resource
        let resource_type = cur.get_i8();
        assert_eq!(resource_type, 2); // Topic

        let name_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(name_len, 19); // "config-alter-topic" = 18 + 1
        let mut name_bytes = vec![0u8; 18];
        cur.copy_to_slice(&mut name_bytes);
        assert_eq!(&name_bytes, b"config-alter-topic");

        // configs array
        let configs_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(configs_len, 2); // 1 + 1

        // First config
        let config_name_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(config_name_len, 13); // "retention.ms" = 12 + 1
        let mut config_name = vec![0u8; 12];
        cur.copy_to_slice(&mut config_name);
        assert_eq!(&config_name, b"retention.ms");

        let config_op = cur.get_i8();
        assert_eq!(config_op, 0); // SET

        let config_val_len = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(config_val_len, 8); // "3600000" = 7 + 1
        let mut config_val = vec![0u8; 7];
        cur.copy_to_slice(&mut config_val);
        assert_eq!(&config_val, b"3600000");

        // config tagged fields
        let config_tags = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(config_tags, 0);

        // resource tagged fields
        let resource_tags = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(resource_tags, 0);

        // validate_only
        let validate_only = cur.get_u8();
        assert_eq!(validate_only, 0);

        // top-level tagged fields
        let top_tags = varint::decode_unsigned_varint(&mut cur).unwrap();
        assert_eq!(top_tags, 0);

        assert!(cur.is_empty(), "should have consumed all bytes");
    }
}
