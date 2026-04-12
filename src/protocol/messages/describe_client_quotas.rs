use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, encode_compact_array_len,
};

// ============================================================================
// DescribeClientQuotas API (Key 48)
// ============================================================================

/// A component in a quota entity filter.
#[derive(Debug, Clone)]
pub struct QuotaFilterComponent {
    /// Entity type (e.g., `"user"`, `"client-id"`, `"ip"`).
    pub entity_type: String,
    /// Match type: `0` = exact, `1` = default, `2` = any specified.
    pub match_type: i8,
    /// Value to match (only used when `match_type` is exact).
    pub match_value: Option<String>,
}

/// DescribeClientQuotas request.
#[derive(Debug, Clone)]
pub struct DescribeClientQuotasRequest {
    /// Filter components. The broker returns entities matching all components.
    pub components: Vec<QuotaFilterComponent>,
    /// If `true`, the response includes all quota defaults that apply.
    pub strict: bool,
}

impl DescribeClientQuotasRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::DescribeClientQuotas
    }

    /// Encode for version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.components.len())?);
        for component in &self.components {
            KafkaString::new(&component.entity_type).try_encode(buf)?;
            component.match_type.encode(buf);
            match &component.match_value {
                None => KafkaString::null().try_encode(buf)?,
                Some(v) => KafkaString::new(v).try_encode(buf)?,
            }
        }
        self.strict.encode(buf);
        Ok(())
    }

    /// Encode for version 1 (flexible encoding).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.components.len(), buf)?;
        for component in &self.components {
            KafkaString::new(&component.entity_type).try_encode_compact(buf)?;
            component.match_type.encode(buf);
            KafkaString(component.match_value.clone()).try_encode_compact(buf)?;
            TaggedFields::default().try_encode(buf)?;
        }
        self.strict.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for DescribeClientQuotasRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            _ => return unsupported_encode!("DescribeClientQuotasRequest", version),
        }
        Ok(())
    }
}

/// An entity in a quota entry.
#[derive(Debug, Clone)]
pub struct QuotaEntity {
    /// Entity type (e.g., `"user"`, `"client-id"`, `"ip"`).
    pub entity_type: String,
    /// Entity name. `None` represents the default entity.
    pub entity_name: Option<String>,
}

/// A quota value in a quota entry.
#[derive(Debug, Clone)]
pub struct QuotaValue {
    /// Quota key (e.g., `"producer_byte_rate"`, `"consumer_byte_rate"`).
    pub key: String,
    /// Quota value.
    pub value: f64,
}

/// An entry returned by DescribeClientQuotas.
#[derive(Debug, Clone)]
pub struct QuotaEntry {
    /// Quota entity components.
    pub entity: Vec<QuotaEntity>,
    /// Quota values.
    pub values: Vec<QuotaValue>,
}

/// DescribeClientQuotas response.
#[derive(Debug, Clone)]
pub struct DescribeClientQuotasResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Error code.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// Quota entries matching the filter.
    pub entries: Option<Vec<QuotaEntry>>,
}

impl DescribeClientQuotasResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode(buf)?.0;

        let entry_count_raw = i32::decode(buf)?;
        let entries = if entry_count_raw == -1 {
            None
        } else {
            let entry_count = check_decode_array_len(entry_count_raw)?;
            let mut entries = Vec::with_capacity(entry_count);

            for _ in 0..entry_count {
                let entity_count = check_decode_array_len(i32::decode(buf)?)?;
                let mut entity = Vec::with_capacity(entity_count);
                for _ in 0..entity_count {
                    let entity_type =
                        non_nullable_string("entity_type", KafkaString::decode(buf)?.0)?;
                    let entity_name = KafkaString::decode(buf)?.0;
                    entity.push(QuotaEntity {
                        entity_type,
                        entity_name,
                    });
                }

                let value_count = check_decode_array_len(i32::decode(buf)?)?;
                let mut values = Vec::with_capacity(value_count);
                for _ in 0..value_count {
                    let key = non_nullable_string("quota key", KafkaString::decode(buf)?.0)?;
                    if buf.remaining() < 8 {
                        return Err(KrafkaError::protocol("not enough bytes for f64"));
                    }
                    let value = buf.get_f64();
                    values.push(QuotaValue { key, value });
                }

                entries.push(QuotaEntry { entity, values });
            }

            Some(entries)
        };

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            entries,
        })
    }

    /// Decode from version 1 (flexible encoding).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let error_message = KafkaString::decode_compact(buf)?.0;

        let entry_count_raw = crate::util::varint::decode_unsigned_varint(buf)?;
        let entries = if entry_count_raw == 0 {
            None
        } else {
            let entry_count = check_compact_array_len(entry_count_raw)?;
            let mut entries = Vec::with_capacity(entry_count);

            for _ in 0..entry_count {
                let entity_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut entity = Vec::with_capacity(entity_count);
                for _ in 0..entity_count {
                    let entity_type =
                        non_nullable_string("entity_type", KafkaString::decode_compact(buf)?.0)?;
                    let entity_name = KafkaString::decode_compact(buf)?.0;
                    let _ = TaggedFields::decode(buf)?;
                    entity.push(QuotaEntity {
                        entity_type,
                        entity_name,
                    });
                }

                let value_count =
                    check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
                let mut values = Vec::with_capacity(value_count);
                for _ in 0..value_count {
                    let key =
                        non_nullable_string("quota key", KafkaString::decode_compact(buf)?.0)?;
                    if buf.remaining() < 8 {
                        return Err(KrafkaError::protocol("not enough bytes for f64"));
                    }
                    let value = buf.get_f64();
                    let _ = TaggedFields::decode(buf)?;
                    values.push(QuotaValue { key, value });
                }

                let _ = TaggedFields::decode(buf)?;
                entries.push(QuotaEntry { entity, values });
            }

            Some(entries)
        };

        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            entries,
        })
    }
}

impl VersionedDecode for DescribeClientQuotasResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            _ => unsupported_decode!("DescribeClientQuotasResponse", version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::BytesMut;

    #[test]
    fn test_describe_client_quotas_request_roundtrip() {
        let request = DescribeClientQuotasRequest {
            components: vec![QuotaFilterComponent {
                entity_type: "user".to_string(),
                match_type: 0,
                match_value: Some("alice".to_string()),
            }],
            strict: false,
        };
        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_describe_client_quotas_response_roundtrip() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i16(-1); // error_message (null)
        buf.put_i32(1); // 1 entry
        // entry 0: entity
        buf.put_i32(1); // 1 entity component
        buf.put_i16(4);
        buf.put_slice(b"user"); // entity_type
        buf.put_i16(5);
        buf.put_slice(b"alice"); // entity_name
        // entry 0: values
        buf.put_i32(1); // 1 value
        buf.put_i16(18);
        buf.put_slice(b"producer_byte_rate"); // key
        buf.put_f64(1_048_576.0); // value

        let mut frozen = buf.freeze();
        let resp = DescribeClientQuotasResponse::decode_v0(&mut frozen).unwrap();
        assert!(resp.error_code.is_ok());
        let entries = resp.entries.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entity[0].entity_type, "user");
        assert_eq!(entries[0].values[0].key, "producer_byte_rate");
        assert!((entries[0].values[0].value - 1_048_576.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_describe_client_quotas_response_entry_with_no_values() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i16(-1); // error_message (null)
        buf.put_i32(1); // 1 entry
        // entry 0: entity
        buf.put_i32(1); // 1 entity component
        buf.put_i16(4);
        buf.put_slice(b"user");
        buf.put_i16(5);
        buf.put_slice(b"alice");
        // entry 0: zero values
        buf.put_i32(0);

        let mut frozen = buf.freeze();
        let resp = DescribeClientQuotasResponse::decode_v0(&mut frozen).unwrap();
        let entries = resp.entries.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entity[0].entity_type, "user");
        assert!(entries[0].values.is_empty());
    }

    #[test]
    fn test_describe_client_quotas_response_multiple_entries() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i16(-1); // error_message (null)
        buf.put_i32(2); // 2 entries
        // entry 0
        buf.put_i32(1);
        buf.put_i16(4);
        buf.put_slice(b"user");
        buf.put_i16(5);
        buf.put_slice(b"alice");
        buf.put_i32(1);
        buf.put_i16(18);
        buf.put_slice(b"producer_byte_rate");
        buf.put_f64(1_048_576.0);
        // entry 1
        buf.put_i32(1);
        buf.put_i16(9);
        buf.put_slice(b"client-id");
        buf.put_i16(6);
        buf.put_slice(b"my-app");
        buf.put_i32(1);
        buf.put_i16(18);
        buf.put_slice(b"consumer_byte_rate");
        buf.put_f64(2_097_152.0);

        let mut frozen = buf.freeze();
        let resp = DescribeClientQuotasResponse::decode_v0(&mut frozen).unwrap();
        let entries = resp.entries.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].entity[0].entity_name, Some("alice".to_string()));
        assert!((entries[0].values[0].value - 1_048_576.0).abs() < f64::EPSILON);
        assert_eq!(entries[1].entity[0].entity_type, "client-id");
        assert!((entries[1].values[0].value - 2_097_152.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_describe_client_quotas_response_null_entries() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i16(-1); // error_message (null)
        buf.put_i32(-1); // null entries

        let mut frozen = buf.freeze();
        let resp = DescribeClientQuotasResponse::decode_v0(&mut frozen).unwrap();
        assert!(resp.entries.is_none());
    }

    #[test]
    fn test_describe_client_quotas_response_rejects_invalid_negative() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_i16(-1); // error_message (null)
        buf.put_i32(-2); // invalid negative (only -1 means null)

        let mut frozen = buf.freeze();
        assert!(DescribeClientQuotasResponse::decode_v0(&mut frozen).is_err());
    }

    #[test]
    fn test_describe_client_quotas_v1_flexible() {
        let request = DescribeClientQuotasRequest {
            components: vec![QuotaFilterComponent {
                entity_type: "user".to_string(),
                match_type: 0,
                match_value: Some("alice".to_string()),
            }],
            strict: false,
        };
        let mut v0 = BytesMut::new();
        request.encode_v0(&mut v0).unwrap();
        let mut v1 = BytesMut::new();
        request.encode_v1(&mut v1).unwrap();
        assert_ne!(v0.len(), v1.len());
    }

    #[test]
    fn test_describe_client_quotas_response_v1_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // error_code
        buf.put_u8(0); // error_message: compact null
        buf.put_u8(0); // entries: compact nullable array null (varint 0)
        buf.put_u8(0); // top-level tagged fields

        let mut frozen = buf.freeze();
        let resp = DescribeClientQuotasResponse::decode_v1(&mut frozen).unwrap();
        assert!(resp.error_code.is_ok());
        assert!(resp.entries.is_none());
    }
}
