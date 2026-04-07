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
//! | Produce | 0 | 11 | v3 transactions, v9 flexible, v10 CurrentLeader tagged |
//! | Fetch | 0 | 12 | v4 isolation, v7 sessions, v11 closest-replica, v12 flexible |
//! | ListOffsets | 0 | 2 | v2 isolation level |
//! | Metadata | 0 | 13 | v9 flexible, v10 topic_id, v13 top-level error code |
//! | OffsetCommit | 0 | 9 | v5 no retention, v6 leader_epoch, v7 instance_id, v8 flexible |
//! | OffsetFetch | 0 | 9 | v5 leader_epoch, v6 flexible, v8 batched groups, v9 KIP-848 |
//! | FindCoordinator | 0 | 4 | v3 flexible, v4 batched coordinator lookup (KIP-699) |
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
//! | ConsumerGroupHeartbeat | 0 | 1 | KIP-848 + KIP-1082 (v1 regex, client member-id) |
//!
//! ## Example
//!
//! ```rust,ignore
//! use krafka::protocol::ApiKey;
//!
//! // Negotiate the best version for Fetch
//! // Prefer Fetch v7..=v12; fall back to v4 if the broker doesn't support v7+.
//! let fetch_version = match conn.negotiate_api_version(ApiKey::Fetch, 12, 7).await {
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

/// Validate a compact array length (varint-encoded as `actual_len + 1`).
///
/// In flexible Kafka versions, compact arrays use a varint length equal to
/// the element count plus one. On the wire, a raw value of `0` represents a
/// null array, and a raw value of `1` represents an empty array (`len == 0`).
///
/// This helper intentionally collapses both cases to `Ok(0)`: `raw == 0`
/// returns `Ok(0)` directly, and `raw == 1` decodes to `len == 0` via
/// `raw - 1`. Values exceeding [`MAX_DECODE_ARRAY_LEN`] are rejected to
/// prevent OOM from malicious or corrupted broker responses.
#[inline]
pub(crate) fn check_compact_array_len(raw: u32) -> Result<usize> {
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
    /// Maximum supported Produce version (v0-v2 legacy, v3-v8 transactional, v9-v11 flexible).
    pub const PRODUCE_MAX: i16 = 11;
    /// Maximum supported Fetch version (v12 flexible encode/decode, KIP-227/KIP-320/KIP-392).
    pub const FETCH_MAX: i16 = 12;
    /// Maximum supported Metadata version (v13 top-level error code).
    pub const METADATA_MAX: i16 = 13;
    /// Maximum supported OffsetCommit version (v8-v9 flexible, KIP-848 STALE_MEMBER_EPOCH).
    pub const OFFSET_COMMIT_MAX: i16 = 9;
    /// Maximum supported OffsetFetch version (v8-v9 batched groups, KIP-848 MemberId/MemberEpoch).
    pub const OFFSET_FETCH_MAX: i16 = 9;
    /// Maximum supported FindCoordinator version (v3 flexible, v4 batched lookup KIP-699).
    pub const FIND_COORDINATOR_MAX: i16 = 4;
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
    /// Maximum supported ConsumerGroupHeartbeat version (v0–v1 — KIP-848 next-gen consumer group).
    pub const CONSUMER_GROUP_HEARTBEAT_MAX: i16 = 1;
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
}
