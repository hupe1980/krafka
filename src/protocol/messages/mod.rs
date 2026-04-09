//! Kafka protocol message types.
//!
//! This module defines the request and response types for all Kafka APIs.
//! Each request/response type implements [`VersionedEncode`] and/or
//! [`VersionedDecode`] for version-dispatched encoding and decoding.
//!
//! Types are organized by API category:
//! - [`metadata`] — Metadata request/response
//! - [`produce`] — Produce request/response
//! - [`fetch`] — Fetch request/response
//! - [`coordinator`] — FindCoordinator request/response
//! - [`group`] — JoinGroup, SyncGroup, Heartbeat, LeaveGroup
//! - [`offset`] — OffsetCommit, ListOffsets, OffsetFetch, OffsetForLeaderEpoch
//! - [`topic`] — CreateTopics, DeleteTopics, CreatePartitions, DescribeTopicPartitions
//! - [`config`] — DescribeConfigs, IncrementalAlterConfigs
//! - [`admin`] — DescribeGroups, ListGroups, DeleteGroups, DescribeCluster, ConsumerGroupDescribe
//! - [`sasl`] — SaslHandshake, SaslAuthenticate
//! - [`acl`] — ACL management (DescribeAcls, CreateAcls, DeleteAcls)
//! - [`txn`] — InitProducerId, AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit
//! - [`delete_records`] — DeleteRecords
//! - [`delegation_token`] — Delegation token management
//! - [`quota`] — DescribeClientQuotas, AlterClientQuotas
//! - [`consumer_group_heartbeat`] — KIP-848 consumer group heartbeat
//! - [`telemetry`] — KIP-714 telemetry (feature-gated)
//! - [`share`] — KIP-932 share groups (feature-gated)

use bytes::{Buf, BufMut, Bytes};

use crate::error::Result;

/// Trait for encoding a request/response at a specific protocol version.
///
/// Implementors dispatch to the appropriate `encode_vN` method based on
/// the version number, returning an error for unsupported versions.
/// All encoding is fallible — oversized inputs return an error instead of
/// panicking.
pub trait VersionedEncode {
    /// Encode this message for the given protocol version.
    fn encode_versioned(&self, version: i16, buf: &mut impl BufMut) -> Result<()>;
}

/// Trait for decoding a response/request from a specific protocol version.
///
/// Implementors dispatch to the appropriate `decode_vN` method based on
/// the version number, returning an error for unsupported versions.
pub trait VersionedDecode: Sized {
    /// Decode this message from the given protocol version.
    fn decode_versioned(version: i16, buf: &mut impl Buf) -> Result<Self>;
}

/// Reject a null value for a non-nullable string field.
///
/// In the Kafka wire protocol some string fields are declared as
/// non-nullable but the compact encoding still allows null.  This helper
/// turns `None` into a protocol error.
pub(crate) fn non_nullable_string(field: &str, opt: Option<String>) -> Result<String> {
    opt.ok_or_else(|| crate::error::KrafkaError::protocol(format!("{field} must not be null")))
}

/// Reject a null value for a non-nullable bytes field.
///
/// Same rationale as [`non_nullable_string`] but for `Bytes` payloads.
pub(crate) fn non_nullable_bytes(field: &str, opt: Option<Bytes>) -> Result<Bytes> {
    opt.ok_or_else(|| crate::error::KrafkaError::protocol(format!("{field} must not be null")))
}

mod acl;
pub use acl::*;

mod admin;
pub use admin::*;

mod config;
pub use config::*;

mod consumer_group_heartbeat;
pub use consumer_group_heartbeat::*;

mod coordinator;
pub use coordinator::*;

mod delegation_token;
pub use delegation_token::*;

mod delete_records;
pub use delete_records::*;

mod fetch;
pub use fetch::*;

mod group;
pub use group::*;

mod metadata;
pub use metadata::*;

mod offset;
pub use offset::*;

mod produce;
pub use produce::*;

mod quota;
pub use quota::*;

mod sasl;
pub use sasl::*;

#[cfg(feature = "unstable-protocol")]
mod share;
#[cfg(feature = "unstable-protocol")]
pub use share::*;

#[cfg(feature = "telemetry")]
mod telemetry;
#[cfg(feature = "telemetry")]
pub use telemetry::*;

mod topic;
pub use topic::*;

mod txn;
pub use txn::*;
