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
//! | Metadata | 0 | 8 | v1 controller + rack, v2 cluster_id, v3 throttle, v5 offline replicas, v7 leader epoch, v8 adds cluster/topic authorized-operations (decoded and discarded) |
//! | OffsetCommit | 0 | 2 | v2+ for retention |
//! | OffsetFetch | 0 | 1 | v1+ for group coordinator |
//! | FindCoordinator | 0 | 1 | Group/txn coordinator lookup |
//! | JoinGroup | 0 | 5 | v5+ group instance id |
//! | Heartbeat | 0 | 3 | v3+ group instance id (KIP-345) |
//! | SyncGroup | 0 | 3 | v3+ group instance id |
//! | LeaveGroup | 0 | 3 | v3+ batch leave (KIP-345) |
//! | CreateTopics | 0 | 2 | v0 is baseline |
//! | DeleteTopics | 0 | 1 | v0 is baseline |
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
            "negative array length {len} in decode"
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

/// Client-supported API version ranges.
///
/// This module defines the maximum version ranges that Krafka actually implements
/// encode/decode for. These are used for version negotiation with Kafka brokers.
///
/// **Important**: These must match the highest version with a working
/// encode+decode pair. Advertising a higher version than implemented
/// causes protocol parse failures.
pub mod versions {
    /// Maximum supported Produce version (v0 encode/decode + v3 encode).
    pub const PRODUCE_MAX: i16 = 3;
    /// Maximum supported Fetch version (v11 encode/decode — closest-replica fetching, KIP-392).
    pub const FETCH_MAX: i16 = 11;
    /// Maximum supported Metadata version (v8 encode/decode — KRaft-aware metadata).
    pub const METADATA_MAX: i16 = 8;
    /// Maximum supported OffsetCommit version.
    pub const OFFSET_COMMIT_MAX: i16 = 2;
    /// Maximum supported OffsetFetch version.
    pub const OFFSET_FETCH_MAX: i16 = 1;
    /// Maximum supported FindCoordinator version (v1 adds key_type for txn coordinators).
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
    /// Maximum supported DescribeAcls version.
    pub const DESCRIBE_ACLS_MAX: i16 = 0;
    /// Maximum supported CreateAcls version.
    pub const CREATE_ACLS_MAX: i16 = 0;
    /// Maximum supported DeleteAcls version.
    pub const DELETE_ACLS_MAX: i16 = 0;
    /// Maximum supported DescribeGroups version.
    pub const DESCRIBE_GROUPS_MAX: i16 = 1;
    /// Maximum supported ListGroups version.
    pub const LIST_GROUPS_MAX: i16 = 1;
    /// Maximum supported DeleteRecords version.
    pub const DELETE_RECORDS_MAX: i16 = 0;
    /// Maximum supported OffsetForLeaderEpoch version.
    pub const OFFSET_FOR_LEADER_EPOCH_MAX: i16 = 2;
    /// Maximum supported InitProducerId version.
    pub const INIT_PRODUCER_ID_MAX: i16 = 0;
    /// Maximum supported ListOffsets version (v2 encode/decode).
    pub const LIST_OFFSETS_MAX: i16 = 2;
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
