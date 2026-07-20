use std::fmt;

use bytes::{Buf, BufMut, Bytes};

use super::{VersionedDecode, VersionedEncode, non_nullable_bytes, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::api::ApiKey;
use crate::protocol::primitives::{
    Decode, Encode, KafkaBytes, KafkaString, TaggedFields, TryEncode,
};
use crate::protocol::{
    array_len_i32, check_compact_array_len, check_decode_array_len, decode_capacity,
    encode_compact_array_len,
};

// ============================================================================
// CreateDelegationToken API (Key 38)
// ============================================================================

/// A principal that can renew the delegation token.
#[derive(Debug, Clone)]
pub struct CreatableRenewer {
    /// Principal type (e.g., `"User"`).
    pub principal_type: String,
    /// Principal name.
    pub principal_name: String,
}

/// CreateDelegationToken request.
#[derive(Debug, Clone)]
pub struct CreateDelegationTokenRequest {
    /// Principals authorized to renew the token.
    pub renewers: Vec<CreatableRenewer>,
    /// Maximum lifetime in milliseconds. `-1` uses the server default.
    pub max_lifetime_ms: i64,
    /// Owner principal type override (v3+). `None` defaults to the requester.
    pub owner_principal_type: Option<String>,
    /// Owner principal name override (v3+). `None` defaults to the requester.
    pub owner_principal_name: Option<String>,
}

impl CreateDelegationTokenRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::CreateDelegationToken
    }

    /// Encode for version 1 (same wire format as v0).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i32(array_len_i32(self.renewers.len())?);
        for renewer in &self.renewers {
            KafkaString::new(&renewer.principal_type).try_encode(buf)?;
            KafkaString::new(&renewer.principal_name).try_encode(buf)?;
        }
        self.max_lifetime_ms.encode(buf);
        Ok(())
    }

    /// Encode for version 2 (flexible encoding, same fields as v1).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        encode_compact_array_len(self.renewers.len(), buf)?;
        for renewer in &self.renewers {
            KafkaString::new(&renewer.principal_type).try_encode_compact(buf)?;
            KafkaString::new(&renewer.principal_name).try_encode_compact(buf)?;
            TaggedFields::default().try_encode(buf)?;
        }
        self.max_lifetime_ms.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }

    /// Encode for version 3 (adds owner principal override fields).
    pub fn encode_v3(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaString(self.owner_principal_type.clone()).try_encode_compact(buf)?;
        KafkaString(self.owner_principal_name.clone()).try_encode_compact(buf)?;
        encode_compact_array_len(self.renewers.len(), buf)?;
        for renewer in &self.renewers {
            KafkaString::new(&renewer.principal_type).try_encode_compact(buf)?;
            KafkaString::new(&renewer.principal_name).try_encode_compact(buf)?;
            TaggedFields::default().try_encode(buf)?;
        }
        self.max_lifetime_ms.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for CreateDelegationTokenRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1 => self.encode_v1(buf)?,
            2 => self.encode_v2(buf)?,
            3 => self.encode_v3(buf)?,
            _ => return unsupported_encode!("CreateDelegationTokenRequest", version),
        }
        Ok(())
    }
}

/// CreateDelegationToken response.
#[derive(Clone)]
pub struct CreateDelegationTokenResponse {
    /// Error code.
    pub error_code: ErrorCode,
    /// Token owner principal type.
    pub principal_type: String,
    /// Token owner principal name.
    pub principal_name: String,
    /// Token requester principal type (v3+).
    pub token_requester_principal_type: Option<String>,
    /// Token requester principal name (v3+).
    pub token_requester_principal_name: Option<String>,
    /// When the token was issued (ms since epoch).
    pub issue_timestamp_ms: i64,
    /// When the token expires (ms since epoch).
    pub expiry_timestamp_ms: i64,
    /// Maximum timestamp at which the token can be renewed (ms since epoch).
    pub max_timestamp_ms: i64,
    /// Unique token ID (for logging/identification).
    pub token_id: String,
    /// HMAC of the delegation token (used for SASL authentication).
    pub hmac: Bytes,
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
}

impl CreateDelegationTokenResponse {
    /// Decode from version 1 (same wire format as v0).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let principal_type = non_nullable_string("principal_type", KafkaString::decode(buf)?.0)?;
        let principal_name = non_nullable_string("principal_name", KafkaString::decode(buf)?.0)?;
        let issue_timestamp_ms = i64::decode(buf)?;
        let expiry_timestamp_ms = i64::decode(buf)?;
        let max_timestamp_ms = i64::decode(buf)?;
        let token_id = non_nullable_string("token_id", KafkaString::decode(buf)?.0)?;
        let hmac = non_nullable_bytes("hmac", KafkaBytes::decode(buf)?.0)?;
        let throttle_time_ms = i32::decode(buf)?;
        Ok(Self {
            error_code,
            principal_type,
            principal_name,
            token_requester_principal_type: None,
            token_requester_principal_name: None,
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            hmac,
            throttle_time_ms,
        })
    }

    /// Decode from version 2 (flexible encoding, same fields as v1).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let principal_type =
            non_nullable_string("principal_type", KafkaString::decode_compact(buf)?.0)?;
        let principal_name =
            non_nullable_string("principal_name", KafkaString::decode_compact(buf)?.0)?;
        let issue_timestamp_ms = i64::decode(buf)?;
        let expiry_timestamp_ms = i64::decode(buf)?;
        let max_timestamp_ms = i64::decode(buf)?;
        let token_id = non_nullable_string("token_id", KafkaString::decode_compact(buf)?.0)?;
        let hmac = non_nullable_bytes("hmac", KafkaBytes::decode_compact(buf)?.0)?;
        let throttle_time_ms = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            error_code,
            principal_type,
            principal_name,
            token_requester_principal_type: None,
            token_requester_principal_name: None,
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            hmac,
            throttle_time_ms,
        })
    }

    /// Decode from version 3 (adds token requester fields).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let principal_type =
            non_nullable_string("principal_type", KafkaString::decode_compact(buf)?.0)?;
        let principal_name =
            non_nullable_string("principal_name", KafkaString::decode_compact(buf)?.0)?;
        let token_requester_principal_type =
            non_nullable_string("requester_type", KafkaString::decode_compact(buf)?.0)?;
        let token_requester_principal_name =
            non_nullable_string("requester_name", KafkaString::decode_compact(buf)?.0)?;
        let issue_timestamp_ms = i64::decode(buf)?;
        let expiry_timestamp_ms = i64::decode(buf)?;
        let max_timestamp_ms = i64::decode(buf)?;
        let token_id = non_nullable_string("token_id", KafkaString::decode_compact(buf)?.0)?;
        let hmac = non_nullable_bytes("hmac", KafkaBytes::decode_compact(buf)?.0)?;
        let throttle_time_ms = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            error_code,
            principal_type,
            principal_name,
            token_requester_principal_type: Some(token_requester_principal_type),
            token_requester_principal_name: Some(token_requester_principal_name),
            issue_timestamp_ms,
            expiry_timestamp_ms,
            max_timestamp_ms,
            token_id,
            hmac,
            throttle_time_ms,
        })
    }
}

impl VersionedDecode for CreateDelegationTokenResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 => Self::decode_v1(buf),
            2 => Self::decode_v2(buf),
            3 => Self::decode_v3(buf),
            _ => unsupported_decode!("CreateDelegationTokenResponse", version),
        }
    }
}

// ============================================================================
// RenewDelegationToken API (Key 39)
// ============================================================================

/// RenewDelegationToken request.
#[derive(Clone)]
pub struct RenewDelegationTokenRequest {
    /// HMAC of the delegation token to renew.
    pub hmac: Bytes,
    /// New renewal period in milliseconds.
    pub renew_period_ms: i64,
}

impl RenewDelegationTokenRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::RenewDelegationToken
    }

    /// Encode for version 1 (same wire format as v0).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaBytes::new(self.hmac.clone()).try_encode(buf)?;
        self.renew_period_ms.encode(buf);
        Ok(())
    }

    /// Encode for version 2 (flexible encoding).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaBytes::new(self.hmac.clone()).try_encode_compact(buf)?;
        self.renew_period_ms.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for RenewDelegationTokenRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1 => self.encode_v1(buf)?,
            2 => self.encode_v2(buf)?,
            _ => return unsupported_encode!("RenewDelegationTokenRequest", version),
        }
        Ok(())
    }
}

/// RenewDelegationToken response.
#[derive(Debug, Clone)]
pub struct RenewDelegationTokenResponse {
    /// Error code.
    pub error_code: ErrorCode,
    /// New expiry timestamp (ms since epoch).
    pub expiry_timestamp_ms: i64,
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
}

impl RenewDelegationTokenResponse {
    /// Decode from version 1 (same wire format as v0).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let expiry_timestamp_ms = i64::decode(buf)?;
        let throttle_time_ms = i32::decode(buf)?;
        Ok(Self {
            error_code,
            expiry_timestamp_ms,
            throttle_time_ms,
        })
    }

    /// Decode from version 2 (flexible encoding).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let expiry_timestamp_ms = i64::decode(buf)?;
        let throttle_time_ms = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            error_code,
            expiry_timestamp_ms,
            throttle_time_ms,
        })
    }
}

impl VersionedDecode for RenewDelegationTokenResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 => Self::decode_v1(buf),
            2 => Self::decode_v2(buf),
            _ => unsupported_decode!("RenewDelegationTokenResponse", version),
        }
    }
}

// ============================================================================
// ExpireDelegationToken API (Key 40)
// ============================================================================

/// ExpireDelegationToken request.
#[derive(Clone)]
pub struct ExpireDelegationTokenRequest {
    /// HMAC of the delegation token to expire.
    pub hmac: Bytes,
    /// New expiry period in milliseconds. Use `-1` to expire immediately.
    pub expiry_period_ms: i64,
}

impl ExpireDelegationTokenRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::ExpireDelegationToken
    }

    /// Encode for version 1 (same wire format as v0).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaBytes::new(self.hmac.clone()).try_encode(buf)?;
        self.expiry_period_ms.encode(buf);
        Ok(())
    }

    /// Encode for version 2 (flexible encoding).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        KafkaBytes::new(self.hmac.clone()).try_encode_compact(buf)?;
        self.expiry_period_ms.encode(buf);
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for ExpireDelegationTokenRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1 => self.encode_v1(buf)?,
            2 => self.encode_v2(buf)?,
            _ => return unsupported_encode!("ExpireDelegationTokenRequest", version),
        }
        Ok(())
    }
}

/// ExpireDelegationToken response.
#[derive(Debug, Clone)]
pub struct ExpireDelegationTokenResponse {
    /// Error code.
    pub error_code: ErrorCode,
    /// New expiry timestamp (ms since epoch).
    pub expiry_timestamp_ms: i64,
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
}

impl ExpireDelegationTokenResponse {
    /// Decode from version 1 (same wire format as v0).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let expiry_timestamp_ms = i64::decode(buf)?;
        let throttle_time_ms = i32::decode(buf)?;
        Ok(Self {
            error_code,
            expiry_timestamp_ms,
            throttle_time_ms,
        })
    }

    /// Decode from version 2 (flexible encoding).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let expiry_timestamp_ms = i64::decode(buf)?;
        let throttle_time_ms = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            error_code,
            expiry_timestamp_ms,
            throttle_time_ms,
        })
    }
}

impl VersionedDecode for ExpireDelegationTokenResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 => Self::decode_v1(buf),
            2 => Self::decode_v2(buf),
            _ => unsupported_decode!("ExpireDelegationTokenResponse", version),
        }
    }
}

// ============================================================================
// DescribeDelegationToken API (Key 41)
// ============================================================================

/// Owner filter for DescribeDelegationToken request.
#[derive(Debug, Clone)]
pub struct DescribeDelegationTokenOwner {
    /// Principal type (e.g., `"User"`).
    pub principal_type: String,
    /// Principal name.
    pub principal_name: String,
}

/// DescribeDelegationToken request.
#[derive(Debug, Clone)]
pub struct DescribeDelegationTokenRequest {
    /// Owners to filter by. `None` returns all tokens visible to the caller.
    pub owners: Option<Vec<DescribeDelegationTokenOwner>>,
}

impl DescribeDelegationTokenRequest {
    /// Get the API key.
    pub fn api_key() -> ApiKey {
        ApiKey::DescribeDelegationToken
    }

    /// Encode for version 1 (same wire format as v0).
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        match &self.owners {
            None => (-1i32).encode(buf),
            Some(owners) => {
                buf.put_i32(array_len_i32(owners.len())?);
                for owner in owners {
                    KafkaString::new(&owner.principal_type).try_encode(buf)?;
                    KafkaString::new(&owner.principal_name).try_encode(buf)?;
                }
            }
        }
        Ok(())
    }

    /// Encode for version 2–3 (flexible encoding).
    pub fn encode_v2(&self, buf: &mut impl BufMut) -> Result<()> {
        match &self.owners {
            None => crate::util::varint::encode_unsigned_varint(0, buf),
            Some(owners) => {
                encode_compact_array_len(owners.len(), buf)?;
                for owner in owners {
                    KafkaString::new(&owner.principal_type).try_encode_compact(buf)?;
                    KafkaString::new(&owner.principal_name).try_encode_compact(buf)?;
                    TaggedFields::default().try_encode(buf)?;
                }
            }
        }
        TaggedFields::default().try_encode(buf)?;
        Ok(())
    }
}

impl VersionedEncode for DescribeDelegationTokenRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            1 => self.encode_v1(buf)?,
            2 | 3 => self.encode_v2(buf)?,
            _ => return unsupported_encode!("DescribeDelegationTokenRequest", version),
        }
        Ok(())
    }
}

/// A principal that can renew a delegation token (in a describe response).
#[derive(Debug, Clone)]
pub struct DelegationTokenRenewer {
    /// Principal type (e.g., `"User"`).
    pub principal_type: String,
    /// Principal name.
    pub principal_name: String,
}

/// A delegation token returned by DescribeDelegationToken.
#[derive(Clone)]
pub struct DelegationTokenInfo {
    /// Token owner principal type.
    pub principal_type: String,
    /// Token owner principal name.
    pub principal_name: String,
    /// Token requester principal type (v3+).
    pub token_requester_principal_type: Option<String>,
    /// Token requester principal name (v3+).
    pub token_requester_principal_name: Option<String>,
    /// When the token was issued (ms since epoch).
    pub issue_timestamp_ms: i64,
    /// When the token expires (ms since epoch).
    pub expiry_timestamp_ms: i64,
    /// Maximum timestamp at which the token can be renewed (ms since epoch).
    pub max_timestamp_ms: i64,
    /// Unique token ID.
    pub token_id: String,
    /// HMAC of the delegation token.
    pub hmac: Bytes,
    /// Principals authorized to renew this token.
    pub renewers: Vec<DelegationTokenRenewer>,
}

/// DescribeDelegationToken response.
#[derive(Debug, Clone)]
pub struct DescribeDelegationTokenResponse {
    /// Error code.
    pub error_code: ErrorCode,
    /// Delegation tokens matching the request filters.
    pub tokens: Vec<DelegationTokenInfo>,
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
}

impl DescribeDelegationTokenResponse {
    /// Decode from version 1 (same wire format as v0).
    pub fn decode_v1(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let token_count = check_decode_array_len(i32::decode(buf)?)?;
        let mut tokens = Vec::with_capacity(decode_capacity(token_count, buf.remaining()));

        for _ in 0..token_count {
            let principal_type =
                non_nullable_string("principal_type", KafkaString::decode(buf)?.0)?;
            let principal_name =
                non_nullable_string("principal_name", KafkaString::decode(buf)?.0)?;
            let issue_timestamp_ms = i64::decode(buf)?;
            let expiry_timestamp_ms = i64::decode(buf)?;
            let max_timestamp_ms = i64::decode(buf)?;
            let token_id = non_nullable_string("token_id", KafkaString::decode(buf)?.0)?;
            let hmac = non_nullable_bytes("hmac", KafkaBytes::decode(buf)?.0)?;

            let renewer_count = check_decode_array_len(i32::decode(buf)?)?;
            let mut renewers = Vec::with_capacity(decode_capacity(renewer_count, buf.remaining()));
            for _ in 0..renewer_count {
                let renewer_type =
                    non_nullable_string("renewer_type", KafkaString::decode(buf)?.0)?;
                let renewer_name =
                    non_nullable_string("renewer_name", KafkaString::decode(buf)?.0)?;
                renewers.push(DelegationTokenRenewer {
                    principal_type: renewer_type,
                    principal_name: renewer_name,
                });
            }

            tokens.push(DelegationTokenInfo {
                principal_type,
                principal_name,
                token_requester_principal_type: None,
                token_requester_principal_name: None,
                issue_timestamp_ms,
                expiry_timestamp_ms,
                max_timestamp_ms,
                token_id,
                hmac,
                renewers,
            });
        }

        let throttle_time_ms = i32::decode(buf)?;
        Ok(Self {
            error_code,
            tokens,
            throttle_time_ms,
        })
    }

    /// Decode from version 2 (flexible encoding, same fields as v1).
    pub fn decode_v2(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let token_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut tokens = Vec::with_capacity(decode_capacity(token_count, buf.remaining()));

        for _ in 0..token_count {
            let principal_type =
                non_nullable_string("principal_type", KafkaString::decode_compact(buf)?.0)?;
            let principal_name =
                non_nullable_string("principal_name", KafkaString::decode_compact(buf)?.0)?;
            let issue_timestamp_ms = i64::decode(buf)?;
            let expiry_timestamp_ms = i64::decode(buf)?;
            let max_timestamp_ms = i64::decode(buf)?;
            let token_id = non_nullable_string("token_id", KafkaString::decode_compact(buf)?.0)?;
            let hmac = non_nullable_bytes("hmac", KafkaBytes::decode_compact(buf)?.0)?;

            let renewer_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut renewers = Vec::with_capacity(decode_capacity(renewer_count, buf.remaining()));
            for _ in 0..renewer_count {
                let renewer_type =
                    non_nullable_string("renewer_type", KafkaString::decode_compact(buf)?.0)?;
                let renewer_name =
                    non_nullable_string("renewer_name", KafkaString::decode_compact(buf)?.0)?;
                let _ = TaggedFields::decode(buf)?;
                renewers.push(DelegationTokenRenewer {
                    principal_type: renewer_type,
                    principal_name: renewer_name,
                });
            }

            let _ = TaggedFields::decode(buf)?;

            tokens.push(DelegationTokenInfo {
                principal_type,
                principal_name,
                token_requester_principal_type: None,
                token_requester_principal_name: None,
                issue_timestamp_ms,
                expiry_timestamp_ms,
                max_timestamp_ms,
                token_id,
                hmac,
                renewers,
            });
        }

        let throttle_time_ms = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            error_code,
            tokens,
            throttle_time_ms,
        })
    }

    /// Decode from version 3 (adds token requester fields per token).
    pub fn decode_v3(buf: &mut impl Buf) -> Result<Self> {
        let error_code = ErrorCode::from_i16(i16::decode(buf)?);
        let token_count =
            check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
        let mut tokens = Vec::with_capacity(decode_capacity(token_count, buf.remaining()));

        for _ in 0..token_count {
            let principal_type =
                non_nullable_string("principal_type", KafkaString::decode_compact(buf)?.0)?;
            let principal_name =
                non_nullable_string("principal_name", KafkaString::decode_compact(buf)?.0)?;
            let token_requester_principal_type =
                non_nullable_string("requester_type", KafkaString::decode_compact(buf)?.0)?;
            let token_requester_principal_name =
                non_nullable_string("requester_name", KafkaString::decode_compact(buf)?.0)?;
            let issue_timestamp_ms = i64::decode(buf)?;
            let expiry_timestamp_ms = i64::decode(buf)?;
            let max_timestamp_ms = i64::decode(buf)?;
            let token_id = non_nullable_string("token_id", KafkaString::decode_compact(buf)?.0)?;
            let hmac = non_nullable_bytes("hmac", KafkaBytes::decode_compact(buf)?.0)?;

            let renewer_count =
                check_compact_array_len(crate::util::varint::decode_unsigned_varint(buf)?)?;
            let mut renewers = Vec::with_capacity(decode_capacity(renewer_count, buf.remaining()));
            for _ in 0..renewer_count {
                let renewer_type =
                    non_nullable_string("renewer_type", KafkaString::decode_compact(buf)?.0)?;
                let renewer_name =
                    non_nullable_string("renewer_name", KafkaString::decode_compact(buf)?.0)?;
                let _ = TaggedFields::decode(buf)?;
                renewers.push(DelegationTokenRenewer {
                    principal_type: renewer_type,
                    principal_name: renewer_name,
                });
            }

            let _ = TaggedFields::decode(buf)?;

            tokens.push(DelegationTokenInfo {
                principal_type,
                principal_name,
                token_requester_principal_type: Some(token_requester_principal_type),
                token_requester_principal_name: Some(token_requester_principal_name),
                issue_timestamp_ms,
                expiry_timestamp_ms,
                max_timestamp_ms,
                token_id,
                hmac,
                renewers,
            });
        }

        let throttle_time_ms = i32::decode(buf)?;
        let _ = TaggedFields::decode(buf)?;
        Ok(Self {
            error_code,
            tokens,
            throttle_time_ms,
        })
    }
}

impl VersionedDecode for DescribeDelegationTokenResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            1 => Self::decode_v1(buf),
            2 => Self::decode_v2(buf),
            3 => Self::decode_v3(buf),
            _ => unsupported_decode!("DescribeDelegationTokenResponse", version),
        }
    }
}

// ---------------------------------------------------------------------------
// Redacted `Debug` impls for delegation-token types.
//
// The delegation-token HMAC *is* the bearer credential used for SASL
// authentication -- it is exactly as sensitive as a password. A derived
// `Debug` would write it into any log line, panic backtrace, or error report
// that formats the struct. The rest of the crate already hand-writes redacting
// `Debug` for `PlainCredentials`, `ScramCredentials`, `AwsMskIamCredentials`
// and `OAuthBearerToken`; these types were the remaining gap.
// ---------------------------------------------------------------------------

impl fmt::Debug for CreateDelegationTokenResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateDelegationTokenResponse")
            .field("error_code", &self.error_code)
            .field("principal_type", &self.principal_type)
            .field("principal_name", &self.principal_name)
            .field("issue_timestamp_ms", &self.issue_timestamp_ms)
            .field("expiry_timestamp_ms", &self.expiry_timestamp_ms)
            .field("max_timestamp_ms", &self.max_timestamp_ms)
            .field("token_id", &self.token_id)
            .field("hmac", &"[REDACTED]")
            .field("throttle_time_ms", &self.throttle_time_ms)
            .finish()
    }
}

impl fmt::Debug for RenewDelegationTokenRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RenewDelegationTokenRequest")
            .field("hmac", &"[REDACTED]")
            .field("renew_period_ms", &self.renew_period_ms)
            .finish()
    }
}

impl fmt::Debug for ExpireDelegationTokenRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ExpireDelegationTokenRequest")
            .field("hmac", &"[REDACTED]")
            .field("expiry_period_ms", &self.expiry_period_ms)
            .finish()
    }
}

impl fmt::Debug for DelegationTokenInfo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DelegationTokenInfo")
            .field("principal_type", &self.principal_type)
            .field("principal_name", &self.principal_name)
            .field("issue_timestamp_ms", &self.issue_timestamp_ms)
            .field("expiry_timestamp_ms", &self.expiry_timestamp_ms)
            .field("max_timestamp_ms", &self.max_timestamp_ms)
            .field("token_id", &self.token_id)
            .field("hmac", &"[REDACTED]")
            .field("renewers", &self.renewers)
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use bytes::BytesMut;

    // ── Delegation Token roundtrip tests ─────────────────────────────

    #[test]
    fn test_create_delegation_token_request_roundtrip() {
        let request = CreateDelegationTokenRequest {
            renewers: vec![CreatableRenewer {
                principal_type: "User".to_string(),
                principal_name: "alice".to_string(),
            }],
            max_lifetime_ms: 86_400_000,
            owner_principal_type: None,
            owner_principal_name: None,
        };
        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        assert!(!buf.is_empty());

        // Verify versioned dispatch
        let mut buf2 = BytesMut::new();
        request.encode_versioned(1, &mut buf2).unwrap();
        assert_eq!(buf, buf2);
    }

    #[test]
    fn test_create_delegation_token_request_empty_renewers() {
        let request = CreateDelegationTokenRequest {
            renewers: vec![],
            max_lifetime_ms: -1,
            owner_principal_type: None,
            owner_principal_name: None,
        };
        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        // 4-byte array length (0) + 8-byte i64
        assert_eq!(buf.len(), 4 + 8);
        assert_eq!(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]), 0);
    }

    #[test]
    fn test_create_delegation_token_response_roundtrip() {
        let mut buf = BytesMut::new();
        // error_code
        buf.put_i16(0);
        // principal_type
        buf.put_i16(4);
        buf.put_slice(b"User");
        // principal_name
        buf.put_i16(5);
        buf.put_slice(b"alice");
        // issue_timestamp_ms
        buf.put_i64(1000);
        // expiry_timestamp_ms
        buf.put_i64(2000);
        // max_timestamp_ms
        buf.put_i64(3000);
        // token_id
        buf.put_i16(8);
        buf.put_slice(b"token-01");
        // hmac
        buf.put_i32(4);
        buf.put_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        // throttle_time_ms
        buf.put_i32(0);

        let mut frozen = buf.freeze();
        let resp = CreateDelegationTokenResponse::decode_v1(&mut frozen).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.principal_name, "alice");
        assert_eq!(resp.token_id, "token-01");
        assert_eq!(&resp.hmac[..], &[0xDE, 0xAD, 0xBE, 0xEF]);
        assert_eq!(resp.issue_timestamp_ms, 1000);
        assert_eq!(resp.expiry_timestamp_ms, 2000);
    }

    #[test]
    fn test_delegation_token_v1_versioned_dispatch() {
        // Verify v1 versioned dispatch works for all delegation token APIs.
        let create_req = CreateDelegationTokenRequest {
            renewers: vec![CreatableRenewer {
                principal_type: "User".to_string(),
                principal_name: "alice".to_string(),
            }],
            max_lifetime_ms: 60_000,
            owner_principal_type: None,
            owner_principal_name: None,
        };
        let mut buf_direct = BytesMut::new();
        let mut buf_dispatch = BytesMut::new();
        create_req.encode_v1(&mut buf_direct).unwrap();
        create_req.encode_versioned(1, &mut buf_dispatch).unwrap();
        assert_eq!(buf_direct, buf_dispatch);

        let renew_req = RenewDelegationTokenRequest {
            hmac: Bytes::from_static(&[0x01, 0x02]),
            renew_period_ms: 30_000,
        };
        let mut buf_direct = BytesMut::new();
        let mut buf_dispatch = BytesMut::new();
        renew_req.encode_v1(&mut buf_direct).unwrap();
        renew_req.encode_versioned(1, &mut buf_dispatch).unwrap();
        assert_eq!(buf_direct, buf_dispatch);

        let expire_req = ExpireDelegationTokenRequest {
            hmac: Bytes::from_static(&[0xAB]),
            expiry_period_ms: -1,
        };
        let mut buf_direct = BytesMut::new();
        let mut buf_dispatch = BytesMut::new();
        expire_req.encode_v1(&mut buf_direct).unwrap();
        expire_req.encode_versioned(1, &mut buf_dispatch).unwrap();
        assert_eq!(buf_direct, buf_dispatch);

        let describe_req = DescribeDelegationTokenRequest { owners: None };
        let mut buf_direct = BytesMut::new();
        let mut buf_dispatch = BytesMut::new();
        describe_req.encode_v1(&mut buf_direct).unwrap();
        describe_req.encode_versioned(1, &mut buf_dispatch).unwrap();
        assert_eq!(buf_direct, buf_dispatch);

        // Verify response decode via versioned dispatch.
        let mut resp_buf = BytesMut::new();
        resp_buf.put_i16(0); // error_code
        resp_buf.put_i64(42_000); // expiry_timestamp_ms
        resp_buf.put_i32(5); // throttle_time_ms
        let frozen = resp_buf.freeze();
        let resp = RenewDelegationTokenResponse::decode_versioned(1, &mut frozen.clone()).unwrap();
        assert_eq!(resp.expiry_timestamp_ms, 42_000);
        assert_eq!(resp.throttle_time_ms, 5);
    }

    #[test]
    fn test_renew_delegation_token_request_roundtrip() {
        let request = RenewDelegationTokenRequest {
            hmac: Bytes::from_static(&[0x01, 0x02, 0x03]),
            renew_period_ms: 60_000,
        };
        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        // 4-byte length + 3 bytes hmac + 8-byte i64
        assert_eq!(buf.len(), 4 + 3 + 8);
    }

    #[test]
    fn test_renew_delegation_token_response_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i16(0); // error_code
        buf.put_i64(999_999); // expiry_timestamp_ms
        buf.put_i32(0); // throttle_time_ms

        let mut frozen = buf.freeze();
        let resp = RenewDelegationTokenResponse::decode_v1(&mut frozen).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.expiry_timestamp_ms, 999_999);
    }

    #[test]
    fn test_expire_delegation_token_request_roundtrip() {
        let request = ExpireDelegationTokenRequest {
            hmac: Bytes::from_static(&[0xAB]),
            expiry_period_ms: -1,
        };
        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        // 4-byte length + 1 byte hmac + 8-byte i64
        assert_eq!(buf.len(), 4 + 1 + 8);
    }

    #[test]
    fn test_expire_delegation_token_response_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i16(0); // error_code
        buf.put_i64(500_000); // expiry_timestamp_ms
        buf.put_i32(10); // throttle_time_ms

        let mut frozen = buf.freeze();
        let resp = ExpireDelegationTokenResponse::decode_v1(&mut frozen).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.expiry_timestamp_ms, 500_000);
        assert_eq!(resp.throttle_time_ms, 10);
    }

    #[test]
    fn test_describe_delegation_token_request_null_owners() {
        let request = DescribeDelegationTokenRequest { owners: None };
        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        // null array: -1 encoded as i32
        assert_eq!(buf.len(), 4);
        assert_eq!(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]), -1);
    }

    #[test]
    fn test_describe_delegation_token_request_with_owners() {
        let request = DescribeDelegationTokenRequest {
            owners: Some(vec![DescribeDelegationTokenOwner {
                principal_type: "User".to_string(),
                principal_name: "bob".to_string(),
            }]),
        };
        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        // 4 (array len) + 2+4 (string "User") + 2+3 (string "bob")
        assert_eq!(buf.len(), 4 + 6 + 5);
    }

    #[test]
    fn test_describe_delegation_token_request_empty_owners() {
        // Some(vec![]) encodes as array length 0, distinct from None (-1).
        let request = DescribeDelegationTokenRequest {
            owners: Some(vec![]),
        };
        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();
        assert_eq!(buf.len(), 4);
        assert_eq!(i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]), 0);
    }

    #[test]
    fn test_describe_delegation_token_response_roundtrip() {
        use bytes::BufMut;
        let mut buf = BytesMut::new();
        buf.put_i16(0); // error_code
        buf.put_i32(1); // token count
        // token 0
        buf.put_i16(4);
        buf.put_slice(b"User"); // principal_type
        buf.put_i16(3);
        buf.put_slice(b"bob"); // principal_name
        buf.put_i64(100); // issue_timestamp_ms
        buf.put_i64(200); // expiry_timestamp_ms
        buf.put_i64(300); // max_timestamp_ms
        buf.put_i16(2);
        buf.put_slice(b"t1"); // token_id
        buf.put_i32(2);
        buf.put_slice(&[0xAA, 0xBB]); // hmac
        buf.put_i32(2); // 2 renewers
        // renewer 0
        buf.put_i16(4);
        buf.put_slice(b"User"); // principal_type
        buf.put_i16(5);
        buf.put_slice(b"alice"); // principal_name
        // renewer 1
        buf.put_i16(4);
        buf.put_slice(b"User"); // principal_type
        buf.put_i16(3);
        buf.put_slice(b"eve"); // principal_name
        buf.put_i32(0); // throttle_time_ms

        let mut frozen = buf.freeze();
        let resp = DescribeDelegationTokenResponse::decode_v1(&mut frozen).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.tokens.len(), 1);
        assert_eq!(resp.tokens[0].principal_name, "bob");
        assert_eq!(resp.tokens[0].token_id, "t1");
        assert_eq!(&resp.tokens[0].hmac[..], &[0xAA, 0xBB]);
        assert_eq!(resp.tokens[0].renewers.len(), 2);
        assert_eq!(resp.tokens[0].renewers[0].principal_name, "alice");
        assert_eq!(resp.tokens[0].renewers[1].principal_name, "eve");
    }

    // ── DelegationToken flexible encoding (v2-v3) tests ──────────────

    #[test]
    fn test_create_delegation_token_v2_flexible() {
        let request = CreateDelegationTokenRequest {
            renewers: vec![CreatableRenewer {
                principal_type: "User".to_string(),
                principal_name: "alice".to_string(),
            }],
            max_lifetime_ms: 86_400_000,
            owner_principal_type: None,
            owner_principal_name: None,
        };
        let mut v1 = BytesMut::new();
        request.encode_v1(&mut v1).unwrap();
        let mut v2 = BytesMut::new();
        request.encode_v2(&mut v2).unwrap();
        assert_ne!(v1.len(), v2.len());
        assert!(!v2.is_empty());
    }

    #[test]
    fn test_create_delegation_token_v3_owner_override() {
        let request = CreateDelegationTokenRequest {
            renewers: vec![],
            max_lifetime_ms: -1,
            owner_principal_type: Some("User".to_string()),
            owner_principal_name: Some("admin".to_string()),
        };
        let mut buf = BytesMut::new();
        request.encode_v3(&mut buf).unwrap();
        assert!(!buf.is_empty());
        // v3 encodes owner fields before renewers
        // Verify it's longer than v2 (due to extra owner fields)
        let mut v2 = BytesMut::new();
        let req_no_owner = CreateDelegationTokenRequest {
            renewers: vec![],
            max_lifetime_ms: -1,
            owner_principal_type: None,
            owner_principal_name: None,
        };
        req_no_owner.encode_v2(&mut v2).unwrap();
        assert!(buf.len() > v2.len());
    }

    #[test]
    fn test_create_delegation_token_response_v2_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i16(0); // error_code
        buf.put_u8(5); // principal_type: compact string len=4+1=5
        buf.put_slice(b"User");
        buf.put_u8(6); // principal_name: compact string len=5+1=6
        buf.put_slice(b"alice");
        buf.put_i64(1000); // issue_timestamp_ms
        buf.put_i64(2000); // expiry_timestamp_ms
        buf.put_i64(3000); // max_timestamp_ms
        buf.put_u8(9); // token_id: compact string len=8+1=9
        buf.put_slice(b"token-01");
        buf.put_u8(5); // hmac: compact bytes len=4+1=5
        buf.put_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(0); // tagged fields

        let mut frozen = buf.freeze();
        let resp = CreateDelegationTokenResponse::decode_v2(&mut frozen).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.principal_name, "alice");
        assert_eq!(resp.token_id, "token-01");
        assert!(resp.token_requester_principal_type.is_none());
    }

    #[test]
    fn test_create_delegation_token_response_v3_with_requester() {
        let mut buf = BytesMut::new();
        buf.put_i16(0); // error_code
        buf.put_u8(5); // principal_type: "User"
        buf.put_slice(b"User");
        buf.put_u8(6); // principal_name: "admin"
        buf.put_slice(b"admin");
        buf.put_u8(5); // requester principal_type: "User"
        buf.put_slice(b"User");
        buf.put_u8(6); // requester principal_name: "alice"
        buf.put_slice(b"alice");
        buf.put_i64(1000); // issue_timestamp_ms
        buf.put_i64(2000); // expiry_timestamp_ms
        buf.put_i64(3000); // max_timestamp_ms
        buf.put_u8(4); // token_id: "t-1"
        buf.put_slice(b"t-1");
        buf.put_u8(3); // hmac: 2 bytes
        buf.put_slice(&[0xAB, 0xCD]);
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(0); // tagged fields

        let mut frozen = buf.freeze();
        let resp = CreateDelegationTokenResponse::decode_v3(&mut frozen).unwrap();
        assert_eq!(resp.principal_name, "admin");
        assert_eq!(resp.token_requester_principal_type.as_deref(), Some("User"));
        assert_eq!(
            resp.token_requester_principal_name.as_deref(),
            Some("alice")
        );
    }

    #[test]
    fn test_renew_delegation_token_v2_flexible() {
        let request = RenewDelegationTokenRequest {
            hmac: Bytes::from_static(&[0x01, 0x02, 0x03]),
            renew_period_ms: 60_000,
        };
        let mut v1 = BytesMut::new();
        request.encode_v1(&mut v1).unwrap();
        let mut v2 = BytesMut::new();
        request.encode_v2(&mut v2).unwrap();
        assert_ne!(v1.len(), v2.len());
    }

    #[test]
    fn test_renew_delegation_token_response_v2_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i16(0); // error_code
        buf.put_i64(42_000); // expiry_timestamp_ms
        buf.put_i32(5); // throttle_time_ms
        buf.put_u8(0); // tagged fields

        let mut frozen = buf.freeze();
        let resp = RenewDelegationTokenResponse::decode_v2(&mut frozen).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.expiry_timestamp_ms, 42_000);
    }

    #[test]
    fn test_expire_delegation_token_v2_flexible() {
        let request = ExpireDelegationTokenRequest {
            hmac: Bytes::from_static(&[0xAB]),
            expiry_period_ms: -1,
        };
        let mut v1 = BytesMut::new();
        request.encode_v1(&mut v1).unwrap();
        let mut v2 = BytesMut::new();
        request.encode_v2(&mut v2).unwrap();
        assert_ne!(v1.len(), v2.len());
    }

    #[test]
    fn test_expire_delegation_token_response_v2_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i16(0); // error_code
        buf.put_i64(500_000); // expiry_timestamp_ms
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(0); // tagged fields

        let mut frozen = buf.freeze();
        let resp = ExpireDelegationTokenResponse::decode_v2(&mut frozen).unwrap();
        assert!(resp.error_code.is_ok());
        assert_eq!(resp.expiry_timestamp_ms, 500_000);
    }

    #[test]
    fn test_describe_delegation_token_v2_flexible() {
        let request = DescribeDelegationTokenRequest { owners: None };
        let mut v1 = BytesMut::new();
        request.encode_v1(&mut v1).unwrap();
        let mut v2 = BytesMut::new();
        request.encode_v2(&mut v2).unwrap();
        // v1 null = 4 bytes (-1 i32), v2 null = 1 byte (varint 0) + 1 byte (tagged fields)
        assert_ne!(v1.len(), v2.len());
        // v2 and v3 use the same request wire format
        let mut v3 = BytesMut::new();
        request.encode_versioned(3, &mut v3).unwrap();
        assert_eq!(v2, v3);
    }

    #[test]
    fn test_describe_delegation_token_response_v2_roundtrip() {
        let mut buf = BytesMut::new();
        buf.put_i16(0); // error_code
        buf.put_u8(1); // tokens: compact array count=0+1=1 (empty)
        buf.put_i32(0); // throttle_time_ms
        buf.put_u8(0); // top-level tagged fields

        let mut frozen = buf.freeze();
        let resp = DescribeDelegationTokenResponse::decode_v2(&mut frozen).unwrap();
        assert!(resp.error_code.is_ok());
        assert!(resp.tokens.is_empty());
    }
}
