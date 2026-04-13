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
//! | Produce | 3 | 11 | v3+ for transactions, v9 flexible encoding, v11 ZStd compression |
//! | Fetch | 4 | 12 | v7 fetch sessions, v9 leader epoch fencing, v11 closest-replica (KIP-392), v12 flexible encoding |
//! | ListOffsets | 1 | 8 | v2 isolation level, v4 leader epoch, v6 flexible, v7 max_timestamp, v8 tiered-storage |
//! | Metadata | 1 | 13 | v9 flexible, v10 topic UUIDs, v12 topic_id works, v13 top-level error_code |
//! | OffsetCommit | 2 | 9 | v5 drops retention_time, v6 leader epoch, v8 flexible, v9 KIP-848 |
//! | OffsetFetch | 1 | 9 | v6 flexible, v8 batched groups, v9 member_epoch (KIP-848) |
//! | FindCoordinator | 1 | 6 | v3 flexible, v4 batched keys, v5 KIP-890, v6 share groups (KIP-932) |
//! | JoinGroup | 4 | 9 | v4 group_instance_id (KIP-345), v6 flexible, v8 reason (KIP-800) |
//! | Heartbeat | 3 | 4 | v3 group_instance_id (KIP-345), v4 flexible encoding |
//! | SyncGroup | 3 | 5 | v3 group_instance_id, v4 flexible, v5 protocol_type/protocol_name (KIP-559) |
//! | LeaveGroup | 3 | 5 | v3 batch leave (KIP-345), v4 flexible, v5 reason (KIP-800) |
//! | CreateTopics | 2 | 7 | v5 flexible, v7 topic_id in response (KIP-464, KIP-525) |
//! | DeleteTopics | 1 | 6 | v4 flexible, v6 topic-ID-based deletion |
//! | CreatePartitions | 0 | 3 | v2 flexible, v3 KIP-599 |
//! | DescribeConfigs | 0 | 4 | v1 config_source + synonyms, v3 config_type + documentation, v4 flexible |
//! | IncrementalAlterConfigs | 0 | 1 | v0 non-flexible, v1 flexible encoding |
//! | DescribeAcls | 1 | 3 | v2 flexible, v3 user resource type |
//! | CreateAcls | 1 | 3 | v2 flexible, v3 user resource type |
//! | DeleteAcls | 1 | 3 | v2 flexible, v3 user resource type |
//! | DescribeGroups | 1 | 6 | v3 authorized_operations, v4 static members, v5 flexible, v6 KIP-1043 |
//! | ListGroups | 1 | 5 | v3 flexible, v4 state filter (KIP-518), v5 type filter (KIP-848) |
//! | DeleteRecords | 0 | 2 | v2 flexible encoding |
//! | OffsetForLeaderEpoch | 2 | 4 | v2 leader epoch fencing, v3 replica_id, v4 flexible |
//! | InitProducerId | 0 | 5 | v2 flexible, v3 epoch recovery, v5 KIP-890 |
//! | AddPartitionsToTxn | 0 | 5 | v3 flexible, v4 broker batched, v5 KIP-890 |
//! | AddOffsetsToTxn | 0 | 4 | v3 flexible, v4 KIP-890 |
//! | EndTxn | 0 | 5 | v3 flexible, v5 KIP-890 epoch bump |
//! | TxnOffsetCommit | 0 | 5 | v2 leader epoch, v3 flexible + consumer fields, v5 KIP-890 |
//! | CreateDelegationToken | 1 | 3 | v2 flexible, v3 owner override |
//! | RenewDelegationToken | 1 | 2 | v2 flexible encoding |
//! | ExpireDelegationToken | 1 | 2 | v2 flexible encoding |
//! | DescribeDelegationToken | 1 | 3 | v2 flexible, v3 token requester |
//! | DescribeClientQuotas | 0 | 1 | v1 flexible encoding |
//! | AlterClientQuotas | 0 | 1 | v1 flexible encoding |
//! | ApiVersions | 0 | 4 | v3 flexible, v4 SupportedFeatures fix (KAFKA-17011) |
//! | ConsumerGroupHeartbeat | 0 | 1 | KIP-848 baseline; v1 KIP-1082 (regex, client member-id) |
//! | DeleteGroups | 0 | 2 | v2 flexible encoding |
//! | DescribeCluster | 0 | 2 | v0 flexible (KIP-700), v1 endpoint_type (KIP-919), v2 is_fenced (KIP-1073) |
//! | ConsumerGroupDescribe | 0 | 1 | v0 KIP-848, v1 member_type (KIP-1099) |
//! | DescribeTopicPartitions | 0 | 0 | v0 KIP-966 paginated partition describe |
//! | UpdateFeatures | 0 | 1 | v0 AllowDowngrade, v1 UpgradeType + ValidateOnly (KIP-584) |
//! | ListClientMetricsResources | 0 | 0 | v0 KIP-714 telemetry discovery |
//! | AlterReplicaLogDirs | 1 | 2 | v1 non-flexible, v2 flexible encoding |
//! | OffsetDelete | 0 | 0 | v0 non-flexible (flexibleVersions: none) |
//! | DescribeUserScramCredentials | 0 | 0 | v0 KIP-554 |
//! | AlterUserScramCredentials | 0 | 0 | v0 KIP-554 |
//! | DescribeProducers | 0 | 0 | v0 KIP-664 transaction debug |
//! | DescribeTransactions | 0 | 0 | v0 KIP-664 transaction debug |
//! | ListTransactions | 0 | 1 | v0 KIP-664, v1 DurationFilter (KIP-994) |
//! | ElectLeaders | 0 | 2 | v1 ElectionType (KIP-460), v2 flexible |
//! | AlterPartitionReassignments | 0 | 0 | v0 all flexible (KIP-455) |
//! | ListPartitionReassignments | 0 | 0 | v0 all flexible (KIP-455) |
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

pub use api::{
    ApiKey, ApiVersionRange, ApiVersionsRequest, ApiVersionsResponse, FinalizedFeature,
    SupportedFeature,
};
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

/// Encode a compact array length (Kafka flexible versions: `count + 1` as unsigned varint).
#[inline]
pub(crate) fn encode_compact_array_len(len: usize, buf: &mut impl bytes::BufMut) -> Result<()> {
    let wire =
        u32::try_from(len.checked_add(1).ok_or_else(|| {
            KrafkaError::protocol(format!("compact array length {len} overflows"))
        })?)
        .map_err(|_| {
            KrafkaError::protocol(format!("compact array length {len} exceeds u32::MAX"))
        })?;
    crate::util::varint::encode_unsigned_varint(wire, buf);
    Ok(())
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
/// This module defines the version ranges that Krafka implements encode/decode
/// for. `*_MIN` sets the floor (we never send versions below it), `*_MAX` sets
/// the ceiling. These drive version negotiation with Kafka brokers.
///
/// **MIN strategy:** Kafka 3.9+ floor. Our MINs match the versions Kafka 4.0
/// itself kept — Produce v3+, Fetch v4+, etc. Legacy encode/decode paths below
/// MIN are deleted.
///
/// **Important**: MAX must match the highest version with a working
/// encode+decode pair. Advertising a higher version than implemented
/// causes protocol parse failures.
pub mod versions {
    // ── Produce (API key 0) ──────────────────────────────────────────────
    /// Minimum supported Produce version. Kafka 4.0 removed v0–v2.
    pub const PRODUCE_MIN: i16 = 3;
    /// Maximum supported Produce version (v11 ZStd compression).
    ///
    /// v9 introduces flexible encoding, v11 adds ZStd compression support.
    /// Encode/decode for v12-v13 exists but is not yet activated (topic-UUID
    /// based producing needs end-to-end integration testing).
    pub const PRODUCE_MAX: i16 = 11;

    // ── Fetch (API key 1) ────────────────────────────────────────────────
    /// Minimum supported Fetch version. Kafka 4.0 removed v0–v3.
    pub const FETCH_MIN: i16 = 4;
    /// Maximum supported Fetch version (v12 flexible encoding).
    ///
    /// v12 adds flexible encoding. Encode/decode for v13+ exists but uses
    /// topic-UUID-based fetching which needs end-to-end integration testing.
    pub const FETCH_MAX: i16 = 12;

    // ── ListOffsets (API key 2) ──────────────────────────────────────────
    /// Minimum supported ListOffsets version. Kafka 4.0 removed v0.
    pub const LIST_OFFSETS_MIN: i16 = 1;
    /// Maximum supported ListOffsets version (v8 tiered-storage awareness).
    ///
    /// v4 adds leader epoch validation, v6 flexible encoding, v7 max_timestamp,
    /// v8 tiered-storage. Encode/decode for v9+ exists but is not yet activated.
    pub const LIST_OFFSETS_MAX: i16 = 8;

    // ── Metadata (API key 3) ─────────────────────────────────────────────
    /// Minimum supported Metadata version. v0 lacks essential fields.
    pub const METADATA_MIN: i16 = 1;
    /// Maximum supported Metadata version (v13 top-level error_code).
    ///
    /// v9 flexible encoding, v10 topic UUIDs (enables KIP-848 topic name
    /// resolution), v12 real topic_id-based lookup, v13 top-level error_code.
    pub const METADATA_MAX: i16 = 13;

    // ── OffsetCommit (API key 8) ─────────────────────────────────────────
    /// Minimum supported OffsetCommit version. Kafka 4.0 removed v0–v1.
    pub const OFFSET_COMMIT_MIN: i16 = 2;
    /// Maximum supported OffsetCommit version (v9 KIP-848 member_epoch).
    ///
    /// v5 drops retention_time, v6 leader epoch, v7 group_instance_id,
    /// v8 flexible encoding, v9 KIP-848 member_epoch.
    /// Encode/decode for v10 exists but is not yet activated.
    pub const OFFSET_COMMIT_MAX: i16 = 9;

    // ── OffsetFetch (API key 9) ──────────────────────────────────────────
    /// Minimum supported OffsetFetch version. Kafka 4.0 removed v0.
    pub const OFFSET_FETCH_MIN: i16 = 1;
    /// Maximum supported OffsetFetch version (v9 KIP-848 member_epoch).
    ///
    /// v2 top-level error, v6 flexible, v8 batched groups, v9 KIP-848.
    /// Encode/decode for v10 exists but is not yet activated.
    pub const OFFSET_FETCH_MAX: i16 = 9;

    // ── FindCoordinator (API key 10) ─────────────────────────────────────
    /// Minimum supported FindCoordinator version.
    pub const FIND_COORDINATOR_MIN: i16 = 1;
    /// Maximum supported FindCoordinator version (v6 share groups, KIP-932).
    ///
    /// v2 same wire as v1, v3 flexible, v4 batched keys (KIP-699),
    /// v5 KIP-890, v6 share groups (KIP-932).
    pub const FIND_COORDINATOR_MAX: i16 = 6;

    // ── JoinGroup (API key 11) ───────────────────────────────────────────
    /// Minimum supported JoinGroup version. v4+ adds group_instance_id (KIP-345).
    pub const JOIN_GROUP_MIN: i16 = 4;
    /// Maximum supported JoinGroup version (v9 latest).
    ///
    /// v6 flexible, v7 skip_assignment, v8 reason (KIP-800), v9 latest.
    pub const JOIN_GROUP_MAX: i16 = 9;

    // ── Heartbeat (API key 12) ───────────────────────────────────────────
    /// Minimum supported Heartbeat version. v3+ adds group_instance_id (KIP-345).
    pub const HEARTBEAT_MIN: i16 = 3;
    /// Maximum supported Heartbeat version (v4 flexible encoding).
    pub const HEARTBEAT_MAX: i16 = 4;

    // ── LeaveGroup (API key 13) ──────────────────────────────────────────
    /// Minimum supported LeaveGroup version. v3+ adds batch leave (KIP-345).
    pub const LEAVE_GROUP_MIN: i16 = 3;
    /// Maximum supported LeaveGroup version (v5 reason, KIP-800).
    pub const LEAVE_GROUP_MAX: i16 = 5;

    // ── SyncGroup (API key 14) ───────────────────────────────────────────
    /// Minimum supported SyncGroup version. v3+ adds group_instance_id (KIP-345).
    pub const SYNC_GROUP_MIN: i16 = 3;
    /// Maximum supported SyncGroup version (v5 protocol_type/name, KIP-559).
    pub const SYNC_GROUP_MAX: i16 = 5;

    // ── ApiVersions (API key 18) ─────────────────────────────────────────
    /// Minimum supported ApiVersions version.
    pub const API_VERSIONS_MIN: i16 = 0;
    /// Maximum supported ApiVersions version (v4 KAFKA-17011 SupportedFeatures fix).
    #[cfg(not(feature = "unstable-protocol"))]
    pub const API_VERSIONS_MAX: i16 = 4;
    /// Maximum supported ApiVersions version (v5 KIP-1242 ClusterId/NodeId).
    #[cfg(feature = "unstable-protocol")]
    pub const API_VERSIONS_MAX: i16 = 5;

    // ── CreateTopics (API key 19) ────────────────────────────────────────
    /// Minimum supported CreateTopics version. Kafka 4.0 removed v0–v1.
    pub const CREATE_TOPICS_MIN: i16 = 2;
    /// Maximum supported CreateTopics version (v7 topic_id in response).
    ///
    /// v5 flexible encoding, v7 topic_id (KIP-464, KIP-525).
    pub const CREATE_TOPICS_MAX: i16 = 7;

    // ── DeleteTopics (API key 20) ────────────────────────────────────────
    /// Minimum supported DeleteTopics version. Kafka 4.0 removed v0.
    pub const DELETE_TOPICS_MIN: i16 = 1;
    /// Maximum supported DeleteTopics version (v6 topic-ID-based deletion).
    ///
    /// v4 flexible, v5 error_message, v6 topic-ID-based deletion.
    pub const DELETE_TOPICS_MAX: i16 = 6;

    // ── CreatePartitions (API key 37) ────────────────────────────────────
    /// Minimum supported CreatePartitions version.
    pub const CREATE_PARTITIONS_MIN: i16 = 0;
    /// Maximum supported CreatePartitions version (v3 KIP-599).
    ///
    /// v2 flexible encoding, v3 KIP-599.
    pub const CREATE_PARTITIONS_MAX: i16 = 3;

    // ── DescribeConfigs (API key 32) ─────────────────────────────────────
    /// Minimum supported DescribeConfigs version.
    pub const DESCRIBE_CONFIGS_MIN: i16 = 0;
    /// Maximum supported DescribeConfigs version (v4 flexible encoding).
    ///
    /// v1 synonyms, v3 config_type + documentation, v4 flexible.
    pub const DESCRIBE_CONFIGS_MAX: i16 = 4;

    // ── DescribeAcls (API key 29) ────────────────────────────────────────
    /// Minimum supported DescribeAcls version. Kafka 4.0 removed v0.
    pub const DESCRIBE_ACLS_MIN: i16 = 1;
    /// Maximum supported DescribeAcls version (v3 user resource type).
    pub const DESCRIBE_ACLS_MAX: i16 = 3;

    // ── CreateAcls (API key 30) ──────────────────────────────────────────
    /// Minimum supported CreateAcls version. Kafka 4.0 removed v0.
    pub const CREATE_ACLS_MIN: i16 = 1;
    /// Maximum supported CreateAcls version (v3 user resource type).
    pub const CREATE_ACLS_MAX: i16 = 3;

    // ── DeleteAcls (API key 31) ──────────────────────────────────────────
    /// Minimum supported DeleteAcls version. Kafka 4.0 removed v0.
    pub const DELETE_ACLS_MIN: i16 = 1;
    /// Maximum supported DeleteAcls version (v3 user resource type).
    pub const DELETE_ACLS_MAX: i16 = 3;

    // ── DescribeGroups (API key 15) ──────────────────────────────────────
    /// Minimum supported DescribeGroups version.
    pub const DESCRIBE_GROUPS_MIN: i16 = 1;
    /// Maximum supported DescribeGroups version (v6 KIP-1043).
    ///
    /// v3 authorized_operations, v4 static members, v5 flexible, v6 KIP-1043.
    pub const DESCRIBE_GROUPS_MAX: i16 = 6;

    // ── ListGroups (API key 16) ──────────────────────────────────────────
    /// Minimum supported ListGroups version.
    pub const LIST_GROUPS_MIN: i16 = 1;
    /// Maximum supported ListGroups version (v5 type filter, KIP-848).
    ///
    /// v3 flexible, v4 state filter (KIP-518), v5 type filter (KIP-848).
    pub const LIST_GROUPS_MAX: i16 = 5;

    // ── DeleteRecords (API key 21) ───────────────────────────────────────
    /// Minimum supported DeleteRecords version.
    pub const DELETE_RECORDS_MIN: i16 = 0;
    /// Maximum supported DeleteRecords version (v2 flexible encoding).
    pub const DELETE_RECORDS_MAX: i16 = 2;

    // ── DescribeLogDirs (API key 35) ─────────────────────────────────────
    /// Minimum supported DescribeLogDirs version.
    /// v0 was removed in Kafka 4.0; v1 is the baseline.
    pub const DESCRIBE_LOG_DIRS_MIN: i16 = 1;
    /// Maximum supported DescribeLogDirs version (v4 adds TotalBytes/UsableBytes).
    ///
    /// v1 baseline, v2 flexible encoding, v3 top-level error_code,
    /// v4 TotalBytes + UsableBytes per log dir.
    pub const DESCRIBE_LOG_DIRS_MAX: i16 = 4;

    // ── OffsetForLeaderEpoch (API key 23) ────────────────────────────────
    /// Minimum supported OffsetForLeaderEpoch version. v2+ adds leader epoch.
    pub const OFFSET_FOR_LEADER_EPOCH_MIN: i16 = 2;
    /// Maximum supported OffsetForLeaderEpoch version (v4 flexible encoding).
    pub const OFFSET_FOR_LEADER_EPOCH_MAX: i16 = 4;

    // ── InitProducerId (API key 22) ──────────────────────────────────────
    /// Minimum supported InitProducerId version.
    pub const INIT_PRODUCER_ID_MIN: i16 = 0;
    /// Maximum supported InitProducerId version (v4 producer_id/epoch recovery).
    ///
    /// v2 flexible, v3 epoch recovery (producer_id + epoch for fencing),
    /// v4 latest stable. Encode/decode for v5-v6 (KIP-890) exists but is
    /// not yet activated.
    pub const INIT_PRODUCER_ID_MAX: i16 = 4;

    // ── AddPartitionsToTxn (API key 24) ──────────────────────────────────
    /// Minimum supported AddPartitionsToTxn version.
    pub const ADD_PARTITIONS_TO_TXN_MIN: i16 = 0;
    /// Maximum supported AddPartitionsToTxn version (v3 flexible encoding).
    ///
    /// v3 flexible. Encode/decode for v4-v5 (broker batched, KIP-890)
    /// exists but is not yet activated.
    pub const ADD_PARTITIONS_TO_TXN_MAX: i16 = 3;

    // ── AddOffsetsToTxn (API key 25) ─────────────────────────────────────
    /// Minimum supported AddOffsetsToTxn version.
    pub const ADD_OFFSETS_TO_TXN_MIN: i16 = 0;
    /// Maximum supported AddOffsetsToTxn version (v3 flexible encoding).
    ///
    /// v3 flexible. Encode/decode for v4 (KIP-890) exists but is not yet
    /// activated.
    pub const ADD_OFFSETS_TO_TXN_MAX: i16 = 3;

    // ── EndTxn (API key 26) ──────────────────────────────────────────────
    /// Minimum supported EndTxn version.
    pub const END_TXN_MIN: i16 = 0;
    /// Maximum supported EndTxn version (v3 flexible encoding).
    ///
    /// v3 flexible. Encode/decode for v4-v5 (KIP-890 epoch bump) exists
    /// but is not yet activated.
    pub const END_TXN_MAX: i16 = 3;

    // ── TxnOffsetCommit (API key 28) ─────────────────────────────────────
    /// Minimum supported TxnOffsetCommit version.
    pub const TXN_OFFSET_COMMIT_MIN: i16 = 0;
    /// Maximum supported TxnOffsetCommit version (v3 flexible + consumer fields).
    ///
    /// v2 leader epoch, v3 flexible + group_instance_id + generation_id.
    /// Encode/decode for v4-v5 (KIP-890) exists but is not yet activated.
    pub const TXN_OFFSET_COMMIT_MAX: i16 = 3;

    // ── Delegation Token APIs (38–41) ────────────────────────────────────
    /// Minimum supported CreateDelegationToken version. Kafka 4.0 removed v0.
    pub const CREATE_DELEGATION_TOKEN_MIN: i16 = 1;
    /// Maximum supported CreateDelegationToken version.
    /// v2 flexible encoding, v3 adds owner principal override.
    pub const CREATE_DELEGATION_TOKEN_MAX: i16 = 3;
    /// Minimum supported RenewDelegationToken version. Kafka 4.0 removed v0.
    pub const RENEW_DELEGATION_TOKEN_MIN: i16 = 1;
    /// Maximum supported RenewDelegationToken version.
    /// v2 flexible encoding.
    pub const RENEW_DELEGATION_TOKEN_MAX: i16 = 2;
    /// Minimum supported ExpireDelegationToken version. Kafka 4.0 removed v0.
    pub const EXPIRE_DELEGATION_TOKEN_MIN: i16 = 1;
    /// Maximum supported ExpireDelegationToken version.
    /// v2 flexible encoding.
    pub const EXPIRE_DELEGATION_TOKEN_MAX: i16 = 2;
    /// Minimum supported DescribeDelegationToken version. Kafka 4.0 removed v0.
    pub const DESCRIBE_DELEGATION_TOKEN_MIN: i16 = 1;
    /// Maximum supported DescribeDelegationToken version.
    /// v2 flexible encoding, v3 adds token requester fields.
    pub const DESCRIBE_DELEGATION_TOKEN_MAX: i16 = 3;

    // ── Client Quotas APIs (48–49) ───────────────────────────────────────
    /// Minimum supported DescribeClientQuotas version.
    pub const DESCRIBE_CLIENT_QUOTAS_MIN: i16 = 0;
    /// Maximum supported DescribeClientQuotas version (v1 flexible encoding).
    pub const DESCRIBE_CLIENT_QUOTAS_MAX: i16 = 1;
    /// Minimum supported AlterClientQuotas version.
    pub const ALTER_CLIENT_QUOTAS_MIN: i16 = 0;
    /// Maximum supported AlterClientQuotas version (v1 flexible encoding).
    pub const ALTER_CLIENT_QUOTAS_MAX: i16 = 1;

    // ── ConsumerGroupHeartbeat (API key 68) ──────────────────────────────
    /// Minimum supported ConsumerGroupHeartbeat version.
    pub const CONSUMER_GROUP_HEARTBEAT_MIN: i16 = 0;
    /// Maximum supported ConsumerGroupHeartbeat version (v1 KIP-1082 regex + client member-id).
    pub const CONSUMER_GROUP_HEARTBEAT_MAX: i16 = 1;

    // ── IncrementalAlterConfigs (API key 44) ─────────────────────────────
    /// Minimum supported IncrementalAlterConfigs version.
    pub const INCREMENTAL_ALTER_CONFIGS_MIN: i16 = 0;
    /// Maximum supported IncrementalAlterConfigs version.
    pub const INCREMENTAL_ALTER_CONFIGS_MAX: i16 = 1;

    // ── DeleteGroups (API key 42) ────────────────────────────────────────
    /// Minimum supported DeleteGroups version.
    pub const DELETE_GROUPS_MIN: i16 = 0;
    /// Maximum supported DeleteGroups version.
    pub const DELETE_GROUPS_MAX: i16 = 2;

    // ── DescribeCluster (API key 60) ─────────────────────────────────────
    /// Minimum supported DescribeCluster version.
    pub const DESCRIBE_CLUSTER_MIN: i16 = 0;
    /// Maximum supported DescribeCluster version.
    pub const DESCRIBE_CLUSTER_MAX: i16 = 2;

    // ── UpdateFeatures (API key 57) — KIP-584 ───────────────────────────
    /// Minimum supported UpdateFeatures version.
    pub const UPDATE_FEATURES_MIN: i16 = 0;
    /// Maximum supported UpdateFeatures version (v1 adds UpgradeType + ValidateOnly).
    pub const UPDATE_FEATURES_MAX: i16 = 1;

    // ── ElectLeaders (API key 43) ────────────────────────────────────────
    /// Minimum supported ElectLeaders version.
    /// v0 preferred-only election; v1 adds ElectionType (KIP-460);
    /// v2 flexible encoding.
    pub const ELECT_LEADERS_MIN: i16 = 0;
    /// Maximum supported ElectLeaders version.
    pub const ELECT_LEADERS_MAX: i16 = 2;

    // ── AlterPartitionReassignments (API key 45) ─────────────────────────
    /// Minimum supported AlterPartitionReassignments version.
    /// All versions use flexible encoding.
    pub const ALTER_PARTITION_REASSIGNMENTS_MIN: i16 = 0;
    /// Maximum supported AlterPartitionReassignments version.
    pub const ALTER_PARTITION_REASSIGNMENTS_MAX: i16 = 0;

    // ── ListPartitionReassignments (API key 46) ──────────────────────────
    /// Minimum supported ListPartitionReassignments version.
    /// All versions use flexible encoding.
    pub const LIST_PARTITION_REASSIGNMENTS_MIN: i16 = 0;
    /// Maximum supported ListPartitionReassignments version.
    pub const LIST_PARTITION_REASSIGNMENTS_MAX: i16 = 0;

    // ── AlterReplicaLogDirs (API key 34) ─────────────────────────────────
    /// Minimum supported AlterReplicaLogDirs version.
    /// v1 non-flexible; v2 flexible encoding.
    pub const ALTER_REPLICA_LOG_DIRS_MIN: i16 = 1;
    /// Maximum supported AlterReplicaLogDirs version.
    pub const ALTER_REPLICA_LOG_DIRS_MAX: i16 = 2;

    // ── OffsetDelete (API key 47) ────────────────────────────────────────
    /// Minimum supported OffsetDelete version.
    /// flexibleVersions: "none" — always non-flexible.
    pub const OFFSET_DELETE_MIN: i16 = 0;
    /// Maximum supported OffsetDelete version.
    pub const OFFSET_DELETE_MAX: i16 = 0;

    // ── DescribeUserScramCredentials (API key 50) ────────────────────────
    /// Minimum supported DescribeUserScramCredentials version.
    /// All versions use flexible encoding.
    pub const DESCRIBE_USER_SCRAM_CREDENTIALS_MIN: i16 = 0;
    /// Maximum supported DescribeUserScramCredentials version.
    pub const DESCRIBE_USER_SCRAM_CREDENTIALS_MAX: i16 = 0;

    // ── AlterUserScramCredentials (API key 51) ───────────────────────────
    /// Minimum supported AlterUserScramCredentials version.
    /// All versions use flexible encoding.
    pub const ALTER_USER_SCRAM_CREDENTIALS_MIN: i16 = 0;
    /// Maximum supported AlterUserScramCredentials version.
    pub const ALTER_USER_SCRAM_CREDENTIALS_MAX: i16 = 0;

    // ── DescribeProducers (API key 61) ───────────────────────────────────
    /// Minimum supported DescribeProducers version.
    /// All versions use flexible encoding.
    pub const DESCRIBE_PRODUCERS_MIN: i16 = 0;
    /// Maximum supported DescribeProducers version.
    pub const DESCRIBE_PRODUCERS_MAX: i16 = 0;

    // ── DescribeTransactions (API key 65) ────────────────────────────────
    /// Minimum supported DescribeTransactions version.
    /// All versions use flexible encoding.
    pub const DESCRIBE_TRANSACTIONS_MIN: i16 = 0;
    /// Maximum supported DescribeTransactions version.
    pub const DESCRIBE_TRANSACTIONS_MAX: i16 = 0;

    // ── ListTransactions (API key 66) ────────────────────────────────────
    /// Minimum supported ListTransactions version.
    /// All versions use flexible encoding.
    pub const LIST_TRANSACTIONS_MIN: i16 = 0;
    /// Maximum supported ListTransactions version.
    /// v1 adds DurationFilter (KIP-994).
    pub const LIST_TRANSACTIONS_MAX: i16 = 1;

    // ── ConsumerGroupDescribe (API key 69) ───────────────────────────────
    /// Minimum supported ConsumerGroupDescribe version.
    pub const CONSUMER_GROUP_DESCRIBE_MIN: i16 = 0;
    /// Maximum supported ConsumerGroupDescribe version.
    pub const CONSUMER_GROUP_DESCRIBE_MAX: i16 = 1;

    // ── DescribeTopicPartitions (API key 75) ─────────────────────────────
    /// Minimum supported DescribeTopicPartitions version.
    pub const DESCRIBE_TOPIC_PARTITIONS_MIN: i16 = 0;
    /// Maximum supported DescribeTopicPartitions version.
    pub const DESCRIBE_TOPIC_PARTITIONS_MAX: i16 = 0;

    // ── ListClientMetricsResources (API key 74) ──────────────────────────
    /// Minimum supported ListClientMetricsResources version.
    pub const LIST_CLIENT_METRICS_RESOURCES_MIN: i16 = 0;
    /// Maximum supported ListClientMetricsResources version.
    pub const LIST_CLIENT_METRICS_RESOURCES_MAX: i16 = 0;

    // ── WriteTxnMarkers (API key 27) ─────────────────────────────────────
    /// Minimum supported WriteTxnMarkers version.
    /// v0 was removed in Kafka 4.0; v1 is the baseline (flexible encoding).
    pub const WRITE_TXN_MARKERS_MIN: i16 = 1;
    /// Maximum supported WriteTxnMarkers version.
    /// v1 flexible baseline; v2 adds TransactionVersion (KIP-1228).
    pub const WRITE_TXN_MARKERS_MAX: i16 = 1;

    // ── DescribeQuorum (API key 55) ──────────────────────────────────────
    /// Minimum supported DescribeQuorum version.
    /// All versions use flexible encoding.
    pub const DESCRIBE_QUORUM_MIN: i16 = 0;
    /// Maximum supported DescribeQuorum version.
    /// v0 baseline; v1 adds timestamps (KIP-836); v2 adds Nodes (KIP-853).
    pub const DESCRIBE_QUORUM_MAX: i16 = 0;

    // ── GetTelemetrySubscriptions (API key 71) — KIP-714 ─────────────────
    /// Minimum supported GetTelemetrySubscriptions version.
    #[cfg(feature = "telemetry")]
    #[cfg_attr(docsrs, doc(cfg(feature = "telemetry")))]
    pub const GET_TELEMETRY_SUBSCRIPTIONS_MIN: i16 = 0;
    /// Maximum supported GetTelemetrySubscriptions version.
    #[cfg(feature = "telemetry")]
    #[cfg_attr(docsrs, doc(cfg(feature = "telemetry")))]
    pub const GET_TELEMETRY_SUBSCRIPTIONS_MAX: i16 = 0;

    // ── PushTelemetry (API key 72) — KIP-714 ────────────────────────────
    /// Minimum supported PushTelemetry version.
    #[cfg(feature = "telemetry")]
    #[cfg_attr(docsrs, doc(cfg(feature = "telemetry")))]
    pub const PUSH_TELEMETRY_MIN: i16 = 0;
    /// Maximum supported PushTelemetry version.
    #[cfg(feature = "telemetry")]
    #[cfg_attr(docsrs, doc(cfg(feature = "telemetry")))]
    pub const PUSH_TELEMETRY_MAX: i16 = 0;

    // ── ShareGroupHeartbeat (API key 76) — KIP-932 ──────────────────────
    /// Minimum supported ShareGroupHeartbeat version.
    #[cfg(feature = "unstable-protocol")]
    pub const SHARE_GROUP_HEARTBEAT_MIN: i16 = 1;
    /// Maximum supported ShareGroupHeartbeat version.
    #[cfg(feature = "unstable-protocol")]
    pub const SHARE_GROUP_HEARTBEAT_MAX: i16 = 1;

    // ── ShareGroupDescribe (API key 77) — KIP-932 ───────────────────────
    /// Minimum supported ShareGroupDescribe version.
    #[cfg(feature = "unstable-protocol")]
    pub const SHARE_GROUP_DESCRIBE_MIN: i16 = 1;
    /// Maximum supported ShareGroupDescribe version.
    #[cfg(feature = "unstable-protocol")]
    pub const SHARE_GROUP_DESCRIBE_MAX: i16 = 1;

    // ── ShareFetch (API key 78) — KIP-932 ───────────────────────────────
    /// Minimum supported ShareFetch version.
    #[cfg(feature = "unstable-protocol")]
    pub const SHARE_FETCH_MIN: i16 = 1;
    /// Maximum supported ShareFetch version (KIP-1206 + KIP-1222).
    #[cfg(feature = "unstable-protocol")]
    pub const SHARE_FETCH_MAX: i16 = 2;

    // ── ShareAcknowledge (API key 79) — KIP-932 ─────────────────────────
    /// Minimum supported ShareAcknowledge version.
    #[cfg(feature = "unstable-protocol")]
    pub const SHARE_ACKNOWLEDGE_MIN: i16 = 1;
    /// Maximum supported ShareAcknowledge version (KIP-1222).
    #[cfg(feature = "unstable-protocol")]
    pub const SHARE_ACKNOWLEDGE_MAX: i16 = 2;
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
