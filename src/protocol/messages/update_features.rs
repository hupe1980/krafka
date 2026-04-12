use bytes::{Buf, BufMut};

use super::{VersionedDecode, VersionedEncode, non_nullable_string};
use crate::error::{ErrorCode, Result};
use crate::protocol::primitives::{Decode, Encode, KafkaString, TaggedFields, TryEncode};
use crate::protocol::{check_compact_array_len, encode_compact_array_len};

// ============================================================================
// UpdateFeatures API (Key 57) — KIP-584
// ============================================================================

/// Upgrade type for feature updates (KIP-584, v1+).
///
/// Replaces the v0-only `AllowDowngrade` boolean.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i8)]
pub enum FeatureUpgradeType {
    /// Upgrade only — the default.
    Upgrade = 1,
    /// Safe (lossless) downgrade.
    SafeDowngrade = 2,
    /// Unsafe (lossy) downgrade.
    UnsafeDowngrade = 3,
}

impl FeatureUpgradeType {
    #[cfg(test)]
    fn from_i8(v: i8) -> Self {
        match v {
            2 => Self::SafeDowngrade,
            3 => Self::UnsafeDowngrade,
            _ => Self::Upgrade,
        }
    }
}

/// A single feature update entry in an [`UpdateFeaturesRequest`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct FeatureUpdateKey {
    /// Feature name.
    pub feature: String,
    /// New maximum version level (≥ 1 to set, < 1 to delete).
    pub max_version_level: i16,
    /// v1+: upgrade/downgrade strategy. Ignored when encoding v0.
    pub upgrade_type: FeatureUpgradeType,
}

impl FeatureUpdateKey {
    /// Create a new feature upgrade request.
    pub fn upgrade(feature: impl Into<String>, max_version_level: i16) -> Self {
        Self {
            feature: feature.into(),
            max_version_level,
            upgrade_type: FeatureUpgradeType::Upgrade,
        }
    }

    /// Create a safe downgrade request.
    pub fn safe_downgrade(feature: impl Into<String>, max_version_level: i16) -> Self {
        Self {
            feature: feature.into(),
            max_version_level,
            upgrade_type: FeatureUpgradeType::SafeDowngrade,
        }
    }

    /// Create an unsafe downgrade request.
    pub fn unsafe_downgrade(feature: impl Into<String>, max_version_level: i16) -> Self {
        Self {
            feature: feature.into(),
            max_version_level,
            upgrade_type: FeatureUpgradeType::UnsafeDowngrade,
        }
    }

    /// Create a feature deletion request (sets max_version_level to 0).
    pub fn delete(feature: impl Into<String>) -> Self {
        Self {
            feature: feature.into(),
            max_version_level: 0,
            upgrade_type: FeatureUpgradeType::SafeDowngrade,
        }
    }
}

/// UpdateFeatures request (API Key 57, KIP-584).
///
/// Manages cluster-wide finalized feature version levels.
/// All versions are flexible (compact strings/arrays + tagged fields).
#[derive(Debug, Clone)]
pub struct UpdateFeaturesRequest {
    /// How long to wait before timing out the request (milliseconds).
    pub timeout_ms: i32,
    /// Feature updates to apply.
    pub feature_updates: Vec<FeatureUpdateKey>,
    /// v1+: if true, validate the request without applying changes.
    pub validate_only: bool,
}

impl UpdateFeaturesRequest {
    /// Create a new request with default timeout.
    pub fn new(feature_updates: Vec<FeatureUpdateKey>) -> Self {
        Self {
            timeout_ms: 60_000,
            feature_updates,
            validate_only: false,
        }
    }

    /// Set the timeout in milliseconds.
    pub fn with_timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Enable validate-only mode (v1+, dry-run).
    pub fn with_validate_only(mut self, validate_only: bool) -> Self {
        self.validate_only = validate_only;
        self
    }

    /// Encode for version 0 (flexible).
    ///
    /// v0 uses `AllowDowngrade` bool (mapped from `upgrade_type`).
    pub fn encode_v0(&self, buf: &mut impl BufMut) -> Result<()> {
        self.timeout_ms.encode(buf);
        encode_compact_array_len(self.feature_updates.len(), buf)?;
        for update in &self.feature_updates {
            KafkaString::new(&update.feature).try_encode_compact(buf)?;
            update.max_version_level.encode(buf);
            // AllowDowngrade: true when downgrading
            let allow_downgrade = update.upgrade_type != FeatureUpgradeType::Upgrade;
            buf.put_u8(allow_downgrade as u8);
            TaggedFields::default().try_encode(buf)?; // per-entry tagged fields
        }
        TaggedFields::default().try_encode(buf)?; // top-level tagged fields
        Ok(())
    }

    /// Encode for version 1+ (flexible).
    ///
    /// v1 replaces `AllowDowngrade` with `UpgradeType` and adds `ValidateOnly`.
    pub fn encode_v1(&self, buf: &mut impl BufMut) -> Result<()> {
        self.timeout_ms.encode(buf);
        encode_compact_array_len(self.feature_updates.len(), buf)?;
        for update in &self.feature_updates {
            KafkaString::new(&update.feature).try_encode_compact(buf)?;
            update.max_version_level.encode(buf);
            buf.put_i8(update.upgrade_type as i8);
            TaggedFields::default().try_encode(buf)?; // per-entry tagged fields
        }
        buf.put_u8(self.validate_only as u8);
        TaggedFields::default().try_encode(buf)?; // top-level tagged fields
        Ok(())
    }
}

impl VersionedEncode for UpdateFeaturesRequest {
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()> {
        match version {
            0 => self.encode_v0(buf)?,
            1 => self.encode_v1(buf)?,
            _ => return unsupported_encode!("UpdateFeaturesRequest", version),
        }
        Ok(())
    }
}

/// Per-feature result in an [`UpdateFeaturesResponse`] (v0–v1 only).
#[derive(Debug, Clone)]
pub struct UpdatableFeatureResult {
    /// Feature name.
    pub feature: String,
    /// Error code (`0` if the update succeeded).
    pub error_code: ErrorCode,
    /// Error message, or `None` if the update succeeded.
    pub error_message: Option<String>,
}

/// UpdateFeatures response (API Key 57, KIP-584).
#[derive(Debug, Clone)]
pub struct UpdateFeaturesResponse {
    /// Throttle time in milliseconds.
    pub throttle_time_ms: i32,
    /// Top-level error code.
    pub error_code: ErrorCode,
    /// Top-level error message.
    pub error_message: Option<String>,
    /// Per-feature results (present in v0–v1 only).
    pub results: Vec<UpdatableFeatureResult>,
}

impl UpdateFeaturesResponse {
    /// Whether the top-level error code indicates success.
    pub fn is_ok(&self) -> bool {
        self.error_code.is_ok()
    }

    /// Decode from version 0–1 (flexible, includes per-feature results).
    pub fn decode_v0(buf: &mut impl Buf) -> Result<Self> {
        let throttle_time_ms = i32::decode(buf)?;
        let error_code = ErrorCode::from(i16::decode(buf)?);
        let error_message = KafkaString::decode_compact(buf)?.0;
        let raw_count = crate::util::varint::decode_unsigned_varint(buf)?;
        let items = check_compact_array_len(raw_count)?;
        let mut results = Vec::with_capacity(items);
        for _ in 0..items {
            let feature = non_nullable_string("feature", KafkaString::decode_compact(buf)?.0)?;
            let feature_error = ErrorCode::from(i16::decode(buf)?);
            let feature_msg = KafkaString::decode_compact(buf)?.0;
            let _ = TaggedFields::decode(buf)?; // per-entry tagged fields
            results.push(UpdatableFeatureResult {
                feature,
                error_code: feature_error,
                error_message: feature_msg,
            });
        }
        let _ = TaggedFields::decode(buf)?; // top-level tagged fields
        Ok(Self {
            throttle_time_ms,
            error_code,
            error_message,
            results,
        })
    }
}

impl VersionedDecode for UpdateFeaturesResponse {
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self> {
        match version {
            0 | 1 => Self::decode_v0(buf),
            _ => unsupported_decode!("UpdateFeaturesResponse", version),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::util::varint;
    use bytes::BytesMut;

    /// Helper: write empty tagged fields (varint 0).
    fn put_tagged_fields(buf: &mut BytesMut) {
        buf.put_u8(0);
    }

    #[test]
    fn test_update_features_request_v0_encode() {
        let request = UpdateFeaturesRequest::new(vec![
            FeatureUpdateKey::upgrade("metadata.version", 17),
            FeatureUpdateKey::delete("group.version"),
        ])
        .with_timeout_ms(30_000);

        let mut buf = BytesMut::new();
        request.encode_v0(&mut buf).unwrap();

        let mut cur = &buf[..];
        // timeout_ms
        assert_eq!(i32::decode(&mut cur).unwrap(), 30_000);
        // compact array: 2 entries → varint(3)
        assert_eq!(varint::decode_unsigned_varint(&mut cur).unwrap(), 3);
        // entry 0: "metadata.version", max_version_level=17, AllowDowngrade=false
        let name0 = KafkaString::decode_compact(&mut cur).unwrap().0.unwrap();
        assert_eq!(name0, "metadata.version");
        assert_eq!(i16::decode(&mut cur).unwrap(), 17);
        assert_eq!(cur.get_u8(), 0); // AllowDowngrade=false (upgrade)
        assert_eq!(cur.get_u8(), 0); // per-entry tagged fields
        // entry 1: "group.version", max_version_level=0, AllowDowngrade=true
        let name1 = KafkaString::decode_compact(&mut cur).unwrap().0.unwrap();
        assert_eq!(name1, "group.version");
        assert_eq!(i16::decode(&mut cur).unwrap(), 0);
        assert_eq!(cur.get_u8(), 1); // AllowDowngrade=true (delete → SafeDowngrade)
        assert_eq!(cur.get_u8(), 0); // per-entry tagged fields
        // top-level tagged fields
        assert_eq!(cur.get_u8(), 0);
        assert!(cur.is_empty());
    }

    #[test]
    fn test_update_features_request_v1_encode() {
        let request = UpdateFeaturesRequest::new(vec![FeatureUpdateKey::safe_downgrade(
            "metadata.version",
            15,
        )])
        .with_validate_only(true);

        let mut buf = BytesMut::new();
        request.encode_v1(&mut buf).unwrap();

        let mut cur = &buf[..];
        // timeout_ms
        assert_eq!(i32::decode(&mut cur).unwrap(), 60_000);
        // compact array: 1 entry → varint(2)
        assert_eq!(varint::decode_unsigned_varint(&mut cur).unwrap(), 2);
        // entry: "metadata.version", max_version_level=15, UpgradeType=2 (SafeDowngrade)
        let name = KafkaString::decode_compact(&mut cur).unwrap().0.unwrap();
        assert_eq!(name, "metadata.version");
        assert_eq!(i16::decode(&mut cur).unwrap(), 15);
        assert_eq!(cur.get_i8(), 2); // UpgradeType::SafeDowngrade
        assert_eq!(cur.get_u8(), 0); // per-entry tagged fields
        // validate_only=true
        assert_eq!(cur.get_u8(), 1);
        // top-level tagged fields
        assert_eq!(cur.get_u8(), 0);
        assert!(cur.is_empty());
    }

    #[test]
    fn test_update_features_request_versioned_dispatch() {
        let request = UpdateFeaturesRequest::new(vec![FeatureUpdateKey::upgrade("test", 1)]);

        let mut buf0 = BytesMut::new();
        let mut buf1 = BytesMut::new();
        request.encode_versioned(0, &mut buf0).unwrap();
        request.encode_versioned(1, &mut buf1).unwrap();
        // v0 and v1 have different wire formats (AllowDowngrade vs UpgradeType)
        assert_ne!(buf0, buf1);
        // v2 should fail
        let mut buf2 = BytesMut::new();
        assert!(request.encode_versioned(2, &mut buf2).is_err());
    }

    #[test]
    fn test_update_features_response_decode_v0() {
        let mut buf = BytesMut::new();
        buf.put_i32(100); // throttle_time_ms
        buf.put_i16(0); // error_code (None)
        varint::encode_unsigned_varint(0, &mut buf); // null error_message
        // Results array: 1 entry
        varint::encode_unsigned_varint(2, &mut buf); // count+1 = 2 → 1 entry
        // Feature name: "metadata.version"
        let name = "metadata.version";
        varint::encode_unsigned_varint((name.len() + 1) as u32, &mut buf);
        buf.put_slice(name.as_bytes());
        buf.put_i16(0); // error_code
        varint::encode_unsigned_varint(0, &mut buf); // null error_message
        put_tagged_fields(&mut buf); // per-entry tagged fields
        put_tagged_fields(&mut buf); // top-level tagged fields

        let resp = UpdateFeaturesResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert_eq!(resp.throttle_time_ms, 100);
        assert!(resp.is_ok());
        assert!(resp.error_message.is_none());
        assert_eq!(resp.results.len(), 1);
        assert_eq!(resp.results[0].feature, "metadata.version");
        assert!(resp.results[0].error_code.is_ok());
        assert!(resp.results[0].error_message.is_none());
    }

    #[test]
    fn test_update_features_response_decode_v0_with_error() {
        let mut buf = BytesMut::new();
        buf.put_i32(0); // throttle_time_ms
        buf.put_i16(0); // top-level error: None
        varint::encode_unsigned_varint(1, &mut buf); // empty error_message
        // Results: 1 entry with error
        varint::encode_unsigned_varint(2, &mut buf); // 1 result
        let name = "bad.feature";
        varint::encode_unsigned_varint((name.len() + 1) as u32, &mut buf);
        buf.put_slice(name.as_bytes());
        buf.put_i16(1); // error_code: UNKNOWN_SERVER_ERROR
        let msg = "not supported";
        varint::encode_unsigned_varint((msg.len() + 1) as u32, &mut buf);
        buf.put_slice(msg.as_bytes());
        put_tagged_fields(&mut buf);
        put_tagged_fields(&mut buf);

        let resp = UpdateFeaturesResponse::decode_v0(&mut buf.freeze()).unwrap();
        assert!(resp.is_ok()); // top-level OK
        assert_eq!(resp.results[0].feature, "bad.feature");
        assert!(!resp.results[0].error_code.is_ok());
        assert_eq!(
            resp.results[0].error_message.as_deref(),
            Some("not supported")
        );
    }

    #[test]
    fn test_feature_update_key_constructors() {
        let upgrade = FeatureUpdateKey::upgrade("f1", 5);
        assert_eq!(upgrade.upgrade_type, FeatureUpgradeType::Upgrade);
        assert_eq!(upgrade.max_version_level, 5);

        let safe = FeatureUpdateKey::safe_downgrade("f2", 3);
        assert_eq!(safe.upgrade_type, FeatureUpgradeType::SafeDowngrade);

        let unsf = FeatureUpdateKey::unsafe_downgrade("f3", 1);
        assert_eq!(unsf.upgrade_type, FeatureUpgradeType::UnsafeDowngrade);

        let del = FeatureUpdateKey::delete("f4");
        assert_eq!(del.max_version_level, 0);
        assert_eq!(del.upgrade_type, FeatureUpgradeType::SafeDowngrade);
    }

    #[test]
    fn test_feature_upgrade_type_from_i8() {
        assert_eq!(FeatureUpgradeType::from_i8(1), FeatureUpgradeType::Upgrade);
        assert_eq!(
            FeatureUpgradeType::from_i8(2),
            FeatureUpgradeType::SafeDowngrade
        );
        assert_eq!(
            FeatureUpgradeType::from_i8(3),
            FeatureUpgradeType::UnsafeDowngrade
        );
        // Unknown values default to Upgrade
        assert_eq!(FeatureUpgradeType::from_i8(99), FeatureUpgradeType::Upgrade);
    }
}
