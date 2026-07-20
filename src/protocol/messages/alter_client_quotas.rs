use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, decode_capacity,
    encode_compact_array_len,
};

// ============================================================================
// AlterClientQuotas API (Key 49)
// ============================================================================

/// An entity component for identifying a quota entry to alter.
#[derive(Debug, Clone)]
pub struct AlterQuotaEntity {
    /// Entity type (e.g., `"user"`, `"client-id"`, `"ip"`).
    pub entity_type: String,
    /// Entity name. `None` represents the default entity.
    pub entity_name: Option<String>,
}

/// An operation to perform on a quota value.
#[derive(Debug, Clone)]
pub struct AlterQuotaOp {
    /// Quota key (e.g., `"producer_byte_rate"`).
    pub key: String,
    /// New quota value. Ignored when `remove` is `true`.
    pub value: f64,
    /// If `true`, remove this quota key rather than setting it.
    pub remove: bool,
}

/// A single quota entity alteration in the AlterClientQuotas request.
#[derive(Debug, Clone)]
pub struct AlterQuotaEntry {
    /// Quota entity to alter.
    pub entity: Vec<AlterQuotaEntity>,
    /// Operations to apply to the entity's quotas.
    pub ops: Vec<AlterQuotaOp>,
}

/// AlterClientQuotas request.
#[derive(Debug, Clone)]
pub struct AlterClientQuotasRequest {
    /// Quota alterations to apply.
    pub entries: Vec<AlterQuotaEntry>,
    /// If `true`, validate only — do not apply changes.
    pub validate_only: bool,
}

impl AlterClientQuotasRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::AlterClientQuotas
    }

    /// Encode for version 0.
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.entries.len())?);
        for entry in &self.entries {
            buf.put_i32(array_len_i32(entry.entity.len())?);
            for e in &entry.entity {
                KafkaString::new(&e.entity_type).try_encode(buf)?;
                match &e.entity_name {
                    None => KafkaString::null().try_encode(buf)?,
                    Some(v) => KafkaString::new(v).try_encode(buf)?,
                }
            }
            buf.put_i32(array_len_i32(entry.ops.len())?);
            for op in &entry.ops {
                KafkaString::new(&op.key).try_encode(buf)?;
                buf.put_f64(op.value);
                op.remove.encode(buf);
            }
        }
        self.validate_only.encode(buf);
        Ok(())
    }

    /// Encode for version 1 (flexible encoding).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.entries.len(), buf)?;
        for entry in &self.entries {
            encode_compact_array_len(entry.entity.len(), buf)?;
            for e in &entry.entity {
                KafkaString::new(&e.entity_type).try_encode_compact(buf)?;
                KafkaString(e.entity_name.clone()).try_encode_compact(buf)?;
                TaggedFields::default().try_encode(buf)?;
            }
            encode_compact_array_len(entry.ops.len(), buf)?;
            for op in &entry.ops {
                KafkaString::new(&op.key).try_encode_compact(buf)?;
                buf.put_f64(op.value);
                op.remove.encode(buf);
                TaggedFields::default().try_encode(buf)?;
            }
            TaggedFields::default().try_encode(buf)?;
        }
        self.validate_only.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for AlterClientQuotasRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            _ => return unsupported_encode!("AlterClientQuotasRequest", version),
        }
        Ok(())
    }
}

/// Per-entity result in AlterClientQuotas response.
#[derive(Debug, Clone)]
pub struct AlterQuotaEntityResult {
    /// Entity type.
    pub entity_type: String,
    /// Entity name.
    pub entity_name: Option<String>,
}

/// Per-entry result in AlterClientQuotas response.
#[derive(Debug, Clone)]
pub struct AlterQuotaEntryResult {
    /// Error code for this entity.
    pub error_code: ErrorCode,
    /// Error message.
    pub error_message: Option<String>,
    /// Entity that was altered.
    pub entity: Vec<AlterQuotaEntityResult>,
}

/// AlterClientQuotas response.
#[derive(Debug, Clone)]
pub struct AlterClientQuotasResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Per-entry results.
    pub entries: Vec<AlterQuotaEntryResult>,
}

impl AlterClientQuotasResponse {
    /// Decode from version 0.
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let entry_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut entries = Vec::with_capacity(decode_capacity(entry_count, buf.remaining()));

        for _ in 0..entry_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode(buf)?.0;

            let entity_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut entity = Vec::with_capacity(decode_capacity(entity_count, buf.remaining()));
            for _ in 0..entity_count {
                let entity_type = non_nullable_string("entity_type", KafkaString::decode(buf)?.0)?;
                let entity_name = KafkaString::decode(buf)?.0;
                entity.push(AlterQuotaEntityResult {
                    entity_type,
                    entity_name,
                });
            }

            entries.push(AlterQuotaEntryResult {
                error_code,
                error_message,
                entity,
            });
        }

        Ok(Self {
            throttle_time_ms,
            entries,
        })
    }

    /// Decode from version 1 (flexible encoding).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let entry_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut entries = Vec::with_capacity(decode_capacity(entry_count, buf.remaining()));

        for _ in 0..entry_count {
            let error_code = ErrorCode::from_i16(i16::decode(buf)?);
            let error_message = KafkaString::decode_compact(buf)?.0;

            let entity_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut entity = Vec::with_capacity(decode_capacity(entity_count, buf.remaining()));
            for _ in 0..entity_count {
                let entity_type =
                    non_nullable_string("entity_type", KafkaString::decode_compact(buf)?.0)?;
                let entity_name = KafkaString::decode_compact(buf)?.0;
                let _ = TaggedFields::decode(buf)?;
                entity.push(AlterQuotaEntityResult {
                    entity_type,
                    entity_name,
                });
            }

            let _ = TaggedFields::decode(buf)?;

            entries.push(AlterQuotaEntryResult {
                error_code,
                error_message,
                entity,
            });
        }

        let _ = TaggedFields::decode(buf)?;

        Ok(Self {
            throttle_time_ms,
            entries,
        })
    }
}

impl VersionedDecode for AlterClientQuotasResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 => Self::decode_v0(buf),
            1 => Self::decode_v1(buf),
            _ => unsupported_decode!("AlterClientQuotasResponse", version),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use bytes::BytesMut;

    #[test]
    fn test_alter_client_quotas_request_roundtrip() {
        let request = AlterClientQuotasRequest {
            entries: vec![AlterQuotaEntry {
                entity: vec![AlterQuotaEntity {
                    entity_type: "user".to_string(),
                    entity_name: Some("alice".to_string()),
                }],
                ops: vec![
                    AlterQuotaOp {
                        key: "producer_byte_rate".to_string(),
                        value: 1_048_576.0,
                        remove: false,
                    },
                    AlterQuotaOp {
                        key: "consumer_byte_rate".to_string(),
                        value: 0.0,
                        remove: true,
                    },
                ],
            }],
            validate_only: true,
        };
        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_alter_client_quotas_response_roundtrip() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i32(1); // 1 entry result
        // entry 0: error
        buf.put_i16(0); // error_code (None)
        buf.put_i16(-1); // error_message (null)
        // entry 0: entity
        buf.put_i32(1); // 1 entity
        buf.put_i16(4);
        buf.put_slice(b"user"); // entity_type
        buf.put_i16(5);
        buf.put_slice(b"alice"); // entity_name

        let mut frozen = buf.freeze();
        let resp = AlterClientQuotasResponse::decode_v0(&mut frozen).unwrap();
        assert_eq!(resp.entries.len(), 1);
        assert!(resp.entries[0].error_code.is_ok());
        assert_eq!(resp.entries[0].entity[0].entity_type, "user");
    }

    #[test]
    fn test_alter_client_quotas_v1_flexible() {
        let request = AlterClientQuotasRequest {
            entries: vec![AlterQuotaEntry {
                entity: vec![AlterQuotaEntity {
                    entity_type: "user".to_string(),
                    entity_name: Some("alice".to_string()),
                }],
                ops: vec![AlterQuotaOp {
                    key: "producer_byte_rate".to_string(),
                    value: 1048576.0,
                    remove: false,
                }],
            }],
            validate_only: false,
        };
        let mut v0 = BytesMut::new();
        request.encode_v0(&mut v0).unwrap();
        let mut v1 = BytesMut::new();
        request.encode_v1(&mut v1).unwrap();
        assert_ne!(v0.len(), v1.len());
    }

    #[test]
    fn test_alter_client_quotas_response_v1_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(2); // entries: compact array count=1+1=2
        buf.put_i16(0); // error_code
        buf.put_u8(0); // error_message: compact null
        buf.put_u8(2); // entity: compact array count=1+1=2
        buf.put_u8(5); // entity_type: compact string "user"
        buf.put_slice(b"user");
        buf.put_u8(6); // entity_name: compact string "alice"
        buf.put_slice(b"alice");
        buf.put_u8(0); // per-entity tagged fields
        buf.put_u8(0); // per-entry tagged fields
        buf.put_u8(0); // top-level tagged fields

        let mut frozen = buf.freeze();
        let resp = AlterClientQuotasResponse::decode_v1(&mut frozen).unwrap();
        assert_eq!(resp.entries.len(), 1);
        assert!(resp.entries[0].error_code.is_ok());
        assert_eq!(resp.entries[0].entity[0].entity_type, "user");
    }
}
