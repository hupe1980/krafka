//! Kafka protocol implementation.
//!
//! This module implements the Kafka wire protocol, including:
//! - Request/response framing
//! - Message encoding and decoding
//! - API version negotiation
//! - Record batch handling
//!
//! # Version Negotiation
//!
//! Krafka supports automatic API version negotiation with Kafka brokers.
//! On connection, the client fetches supported API versions from the broker
//! and negotiates the highest mutually supported version for each API.
//!
//! ## Client Supported Versions
//!
//! | API | Min | Max | Notes |
//! |-----|-----|-----|-------|
//! | Produce | 0 | 3 | v3+ for transactions |
//! | Fetch | 0 | 11 | v0-4, v7-v11 (v5/v6 unsupported); v4 isolation level, v7 fetch sessions, v9 leader epoch fencing, v11 closest-replica fetching (KIP-392) |
//! | ListOffsets | 0 | 2 | v2 isolation level |
//! | Metadata | 0 | 8 | v1 controller + rack, v2 cluster_id, v3 throttle, v5 offline replicas, v7 leader epoch, v8 authorized-ops (encode/decode for v9-v13 exists but not yet activated) |
//! | OffsetCommit | 0 | 2 | v2+ for retention |
//! | OffsetFetch | 0 | 1 | v1+ for group coordinator |
//! | FindCoordinator | 0 | 1 | Group/txn coordinator lookup |
//! | JoinGroup | 0 | 5 | v5+ group instance id |
//! | Heartbeat | 0 | 3 | v3+ group instance id (KIP-345) |
//! | SyncGroup | 0 | 3 | v3+ group instance id |
//! | LeaveGroup | 0 | 3 | v3+ batch leave (KIP-345) |
//! | CreateTopics | 0 | 2 | v0 is baseline |
//! | DeleteTopics | 0 | 1 | v0 is baseline |
//! | CreatePartitions | 0 | 0 | v0 baseline |
//! | DescribeConfigs | 0 | 0 | v0 baseline |
//! | AlterConfigs | 0 | 0 | v0 baseline |
//! | DescribeAcls | 0 | 1 | v1 prefixed ACLs |
//! | CreateAcls | 0 | 1 | v1 prefixed ACLs |
//! | DeleteAcls | 0 | 1 | v1 prefixed ACLs |
//! | DescribeGroups | 0 | 1 | v0 is baseline |
//! | ListGroups | 0 | 1 | v0 is baseline |
//! | DeleteRecords | 0 | 0 | v0 baseline |
//! | OffsetForLeaderEpoch | 0 | 3 | v2 leader epoch fencing, v3 replica_id |
//! | InitProducerId | 0 | 0 | v0 baseline |
//! | CreateDelegationToken | 0 | 1 | v0–v1 same wire format |
//! | RenewDelegationToken | 0 | 1 | v0–v1 same wire format |
//! | ExpireDelegationToken | 0 | 1 | v0–v1 same wire format |
//! | DescribeDelegationToken | 0 | 1 | v0–v1 same wire format |
//! | DescribeClientQuotas | 0 | 0 | v0 baseline |
//! | AlterClientQuotas | 0 | 0 | v0 baseline |
//! | ConsumerGroupHeartbeat | 0 | 0 | KIP-848 baseline; v1 encode/decode for KIP-1082 (regex, client member-id) exists but is not activated yet |
//!
//! ## Example
//!
//! ```rust,ignore
//! use krafka::protocol::ApiKey;
//!
//! // Negotiate the best version for Fetch
//! // Prefer Fetch v7..=v11; fall back to v4 if the broker doesn't support v7+.
//! let fetch_version = match conn.negotiate_api_version(ApiKey::Fetch, 11, 7).await {
//!     Some(v) => v,
//!     None => conn.negotiate_api_version(ApiKey::Fetch, 4, 4).await
//!         .expect("broker does not support any usable Fetch version"),
//! };
//! println!("Using Fetch v{}", fetch_version);
//! ```

mod api;
mod codec;
mod header;
mod messages;
mod primitives;
mod record;

pub use api::{ApiKey, ApiVersionRange, ApiVersionsRequest, ApiVersionsResponse};
pub use codec::{Decoder, Encoder, MAX_MESSAGE_SIZE};
pub use header::{RequestHeader, ResponseHeader};
pub use messages::*;
pub use primitives::*;
pub use record::{
    Compression, LazyRecordBatch, LazyRecordIterator, Record, RecordBatch, RecordBatchBuilder,
    RecordHeader,
};

use crate::error::{KrafkaError, Result};

/// Maximum number of elements allowed in a single decoded array or loop.
///
/// Protects against malicious or corrupted broker responses that declare
/// extremely large array lengths. Without this cap, a crafted response with
/// `array_len = i32::MAX` would cause the decoder to spin billions of
/// iterations (each failing on an exhausted buffer) before returning an error.
///
/// The limit of 100,000 is generous for any realistic Kafka response while
/// preventing CPU-based denial-of-service amplification.
pub const MAX_DECODE_ARRAY_LEN: usize = 100_000;

/// Convert a collection length to i32, returning an error if it overflows.
#[inline]
pub(crate) fn array_len_i32(len: usize) -> Result<i32> {
    i32::try_from(len)
        .map_err(|_| KrafkaError::protocol(format!("array length {len} exceeds i32::MAX")))
}

/// Validate and convert a decoded array length from `i32` to `usize`.
///
/// Returns an error if the count is negative or exceeds [`MAX_DECODE_ARRAY_LEN`].
/// Use this before every inline decode loop to bound iteration count.
#[inline]
pub(crate) fn check_decode_array_len(len: i32) -> Result<usize> {
    if len < 0 {
        return Err(KrafkaError::protocol(format!(
            "negative array length {len} in decode (use check_decode_nullable_array_len for fields where -1 means null)"
        )));
    }
    let len = len as usize;
    if len > MAX_DECODE_ARRAY_LEN {
        return Err(KrafkaError::protocol(format!(
            "array length {len} exceeds safety limit {MAX_DECODE_ARRAY_LEN}"
        )));
    }
    Ok(len)
}

/// Like [`check_decode_array_len`], but treats `-1` as a null array (returns 0).
///
/// In the Kafka wire protocol, some array fields are "nullable": a length of
/// `-1` signals an absent/null array. Use this variant for those fields
/// (e.g. `aborted_transactions` in FetchResponse).
#[inline]
pub(crate) fn check_decode_nullable_array_len(len: i32) -> Result<usize> {
    if len == -1 {
        return Ok(0);
    }
    check_decode_array_len(len)
}

/// Validate a non-nullable compact array length (varint-encoded as `actual_len + 1`).
///
/// In flexible Kafka versions, compact arrays encode the element count plus one
/// as a varint. A raw value of `1` represents an empty array (`len == 0`).
/// A raw value of `0` represents a null array and is **invalid** for
/// non-nullable fields — use [`check_compact_nullable_array_len`] for fields
/// where null is permitted.
///
/// Values exceeding [`MAX_DECODE_ARRAY_LEN`] are rejected to prevent OOM from
/// malicious or corrupted broker responses.
#[inline]
pub(crate) fn check_compact_array_len(raw: u32) -> Result<usize> {
    if raw == 0 {
        return Err(KrafkaError::protocol(
            "compact array raw value 0 (null) is invalid for a non-nullable field; \
             use check_compact_nullable_array_len for nullable arrays",
        ));
    }
    let len = (raw - 1) as usize;
    if len > MAX_DECODE_ARRAY_LEN {
        return Err(KrafkaError::protocol(format!(
            "compact array length {len} exceeds safety limit {MAX_DECODE_ARRAY_LEN}"
        )));
    }
    Ok(len)
}

/// Like [`check_compact_array_len`], but treats a raw value of `0` as a null
/// array (returns `Ok(0)`).
///
/// In the Kafka wire protocol, some compact array fields are "nullable": a raw
/// varint of `0` signals an absent/null array. Use this variant for those
/// fields (e.g. `aborted_transactions` in FetchResponse v12+).
#[inline]
pub(crate) fn check_compact_nullable_array_len(raw: u32) -> Result<usize> {
    if raw == 0 {
        return Ok(0);
    }
    let len = (raw - 1) as usize;
    if len > MAX_DECODE_ARRAY_LEN {
        return Err(KrafkaError::protocol(format!(
            "compact array length {len} exceeds safety limit {MAX_DECODE_ARRAY_LEN}"
        )));
    }
    Ok(len)
}

/// Client-supported API version ranges.
///
/// This module defines the maximum version ranges that Krafka actually implements
/// encode/decode for. These are used for version negotiation with Kafka brokers.
///
/// **Important**: These must match the highest version with a working
/// encode+decode pair. Advertising a higher version than implemented
/// causes protocol parse failures.
pub mod versions {
    /// Maximum supported Produce version (v3 for transactions).
    ///
    /// Encode/decode for v9-v11 (flexible) exists but is not yet activated
    /// — the flexible paths need integration testing against a real broker.
    pub const PRODUCE_MAX: i16 = 3;
    /// Maximum supported Fetch version (v11 closest-replica fetching, KIP-392).
    ///
    /// Encode/decode for v12 (flexible) exists but is not yet activated
    /// — needs integration testing against a real broker.
    pub const FETCH_MAX: i16 = 11;
    /// Maximum supported Metadata version (v8 KRaft-aware metadata).
    ///
    /// Encode/decode for v9-v13 (flexible, topic UUIDs) exists but is not
    /// yet activated — the flexible paths need integration testing against
    /// a real broker. v10+ adds topic UUIDs required for KIP-848.
    pub const METADATA_MAX: i16 = 8;
    /// Maximum supported OffsetCommit version (v2 for retention).
    ///
    /// Encode/decode for v3-v9 (flexible, KIP-848) exists but is not yet
    /// activated — needs integration testing against a real broker.
    pub const OFFSET_COMMIT_MAX: i16 = 2;
    /// Maximum supported OffsetFetch version (v1 for group coordinator).
    ///
    /// Encode/decode for v2-v9 (flexible, KIP-848 batched + member epoch)
    /// exists but is not yet activated — needs integration testing.
    pub const OFFSET_FETCH_MAX: i16 = 1;
    /// Maximum supported FindCoordinator version (v1 adds key_type for txn).
    ///
    /// Encode/decode for v2-v4 (flexible, batched coordinators) exists but
    /// is not yet activated — needs integration testing against a real broker.
    pub const FIND_COORDINATOR_MAX: i16 = 1;
    /// Maximum supported JoinGroup version.
    pub const JOIN_GROUP_MAX: i16 = 5;
    /// Maximum supported Heartbeat version (v3 adds group_instance_id for KIP-345).
    pub const HEARTBEAT_MAX: i16 = 3;
    /// Maximum supported SyncGroup version.
    pub const SYNC_GROUP_MAX: i16 = 3;
    /// Maximum supported LeaveGroup version (v3 adds batch leave for KIP-345).
    pub const LEAVE_GROUP_MAX: i16 = 3;
    /// Maximum supported CreateTopics version.
    pub const CREATE_TOPICS_MAX: i16 = 2;
    /// Maximum supported DeleteTopics version.
    pub const DELETE_TOPICS_MAX: i16 = 1;
    /// Maximum supported CreatePartitions version.
    pub const CREATE_PARTITIONS_MAX: i16 = 0;
    /// Maximum supported DescribeConfigs version.
    pub const DESCRIBE_CONFIGS_MAX: i16 = 0;
    /// Maximum supported AlterConfigs version.
    pub const ALTER_CONFIGS_MAX: i16 = 0;
    /// Maximum supported DescribeAcls version (v1 adds pattern_type for prefixed ACLs).
    pub const DESCRIBE_ACLS_MAX: i16 = 1;
    /// Maximum supported CreateAcls version (v1 adds pattern_type for prefixed ACLs).
    pub const CREATE_ACLS_MAX: i16 = 1;
    /// Maximum supported DeleteAcls version (v1 adds pattern_type for prefixed ACLs).
    pub const DELETE_ACLS_MAX: i16 = 1;
    /// Maximum supported DescribeGroups version.
    pub const DESCRIBE_GROUPS_MAX: i16 = 1;
    /// Maximum supported ListGroups version.
    pub const LIST_GROUPS_MAX: i16 = 1;
    /// Maximum supported DeleteRecords version.
    pub const DELETE_RECORDS_MAX: i16 = 0;
    /// Maximum supported OffsetForLeaderEpoch version (v3 adds replica_id for consumer/follower fencing).
    pub const OFFSET_FOR_LEADER_EPOCH_MAX: i16 = 3;
    /// Maximum supported InitProducerId version.
    pub const INIT_PRODUCER_ID_MAX: i16 = 0;
    /// Maximum supported ListOffsets version (v2 encode/decode).
    pub const LIST_OFFSETS_MAX: i16 = 2;
    /// Maximum supported CreateDelegationToken version (v0–v1 same wire format).
    pub const CREATE_DELEGATION_TOKEN_MAX: i16 = 1;
    /// Maximum supported RenewDelegationToken version (v0–v1 same wire format).
    pub const RENEW_DELEGATION_TOKEN_MAX: i16 = 1;
    /// Maximum supported ExpireDelegationToken version (v0–v1 same wire format).
    pub const EXPIRE_DELEGATION_TOKEN_MAX: i16 = 1;
    /// Maximum supported DescribeDelegationToken version (v0–v1 same wire format).
    pub const DESCRIBE_DELEGATION_TOKEN_MAX: i16 = 1;
    /// Maximum supported DescribeClientQuotas version.
    pub const DESCRIBE_CLIENT_QUOTAS_MAX: i16 = 0;
    /// Maximum supported AlterClientQuotas version.
    pub const ALTER_CLIENT_QUOTAS_MAX: i16 = 0;
    /// Maximum supported ConsumerGroupHeartbeat version (KIP-848 next-gen consumer group).
    ///
    /// Capped at v0: v1 (KIP-1082) requires client-generated member IDs,
    /// which are not yet implemented.  Encode/decode for v1 exists and
    /// will activate once client-side member ID generation is added.
    pub const CONSUMER_GROUP_HEARTBEAT_MAX: i16 = 0;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_decode_array_len_valid() {
        assert_eq!(check_decode_array_len(0).unwrap(), 0);
        assert_eq!(check_decode_array_len(1).unwrap(), 1);
        assert_eq!(check_decode_array_len(100_000).unwrap(), 100_000);
    }

    #[test]
    fn check_decode_array_len_rejects_negative() {
        assert!(check_decode_array_len(-1).is_err());
        assert!(check_decode_array_len(i32::MIN).is_err());
    }

    #[test]
    fn check_decode_array_len_rejects_oversized() {
        assert!(check_decode_array_len(100_001).is_err());
        assert!(check_decode_array_len(i32::MAX).is_err());
    }

    #[test]
    fn check_decode_nullable_array_len_null() {
        assert_eq!(check_decode_nullable_array_len(-1).unwrap(), 0);
    }

    #[test]
    fn check_decode_nullable_array_len_valid() {
        assert_eq!(check_decode_nullable_array_len(0).unwrap(), 0);
        assert_eq!(check_decode_nullable_array_len(5).unwrap(), 5);
    }

    #[test]
    fn check_decode_nullable_array_len_rejects_other_negative() {
        assert!(check_decode_nullable_array_len(-2).is_err());
        assert!(check_decode_nullable_array_len(i32::MIN).is_err());
    }

    // --- compact array helpers (varint-encoded, raw = count + 1) ---

    #[test]
    fn compact_array_len_rejects_null() {
        // raw 0 means null — invalid for non-nullable fields
        assert!(check_compact_array_len(0).is_err());
    }

    #[test]
    fn compact_array_len_empty() {
        // raw 1 → actual length 0
        assert_eq!(check_compact_array_len(1).unwrap(), 0);
    }

    #[test]
    fn compact_array_len_valid() {
        assert_eq!(check_compact_array_len(2).unwrap(), 1);
        assert_eq!(check_compact_array_len(101).unwrap(), 100);
    }

    #[test]
    fn compact_array_len_rejects_oversized() {
        let over = (MAX_DECODE_ARRAY_LEN as u32) + 2; // raw = limit + 1 + 1
        assert!(check_compact_array_len(over).is_err());
    }

    #[test]
    fn compact_nullable_array_len_null() {
        // raw 0 → null → Ok(0)
        assert_eq!(check_compact_nullable_array_len(0).unwrap(), 0);
    }

    #[test]
    fn compact_nullable_array_len_empty() {
        // raw 1 → actual length 0
        assert_eq!(check_compact_nullable_array_len(1).unwrap(), 0);
    }

    #[test]
    fn compact_nullable_array_len_valid() {
        assert_eq!(check_compact_nullable_array_len(2).unwrap(), 1);
        assert_eq!(check_compact_nullable_array_len(101).unwrap(), 100);
    }

    #[test]
    fn compact_nullable_array_len_rejects_oversized() {
        let over = (MAX_DECODE_ARRAY_LEN as u32) + 2;
        assert!(check_compact_nullable_array_len(over).is_err());
    }
}
