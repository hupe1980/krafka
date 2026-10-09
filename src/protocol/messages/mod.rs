//! Kafka protocol message types.
//!
//! This module defines the request and response types for all Kafka APIs.
//! Each request/response type implements [`VersionedEncode`] and/or
//! [`VersionedDecode`] for version-dispatched encoding and decoding.
//!
//! One submodule per API, named after it in snake case (`fetch`,
//! `join_group`, …). [`consumer_protocol`] holds the `ConsumerProtocol`
//! subscription and assignment blobs that classic groups carry.

use bytes::{Buf, BufMut, Bytes};

use crate::error::{ProtocolErrorKind, Result};

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
    opt.ok_or_else(|| {
        crate::error::KrafkaError::protocol_kind(
            ProtocolErrorKind::Malformed,
            format!("{field} must not be null"),
        )
    })
}

/// Reject a null value for a non-nullable bytes field.
///
/// Same rationale as [`non_nullable_string`] but for `Bytes` payloads.
pub(crate) fn non_nullable_bytes(field: &str, opt: Option<Bytes>) -> Result<Bytes> {
    opt.ok_or_else(|| {
        crate::error::KrafkaError::protocol_kind(
            ProtocolErrorKind::Malformed,
            format!("{field} must not be null"),
        )
    })
}

/// Generate a protocol error for an unsupported encode version.
///
/// Used in `VersionedEncode::encode_versioned` match arms for versions
/// outside the implemented range.
macro_rules! unsupported_encode {
    ($type:expr, $version:expr) => {
        Err($crate::error::KrafkaError::protocol_kind(
            $crate::error::ProtocolErrorKind::UnknownApiVersion,
            format!("unsupported {} encode version {}", $type, $version),
        ))
    };
}

/// Generate a protocol error for an unsupported decode version.
///
/// Used in `VersionedDecode::decode_versioned` match arms for versions
/// outside the implemented range.
macro_rules! unsupported_decode {
    ($type:expr, $version:expr) => {
        Err($crate::error::KrafkaError::protocol_kind(
            $crate::error::ProtocolErrorKind::UnknownApiVersion,
            format!("unsupported {} decode version {}", $type, $version),
        ))
    };
}

mod acl;
pub use acl::*;

mod consumer_group_describe;
pub use consumer_group_describe::*;

mod consumer_protocol;
pub use consumer_protocol::*;

mod delete_groups;
pub use delete_groups::*;

mod describe_cluster;
pub use describe_cluster::*;

mod describe_groups;
pub use describe_groups::*;

mod list_config_resources;
pub use list_config_resources::*;

mod list_groups;
pub use list_groups::*;

mod update_features;
pub use update_features::*;

mod alter_partition_reassignments;
pub use alter_partition_reassignments::*;

mod alter_replica_log_dirs;
pub use alter_replica_log_dirs::*;

mod describe_configs;
pub use describe_configs::*;

mod incremental_alter_configs;
pub use incremental_alter_configs::*;

mod consumer_group_heartbeat;
pub use consumer_group_heartbeat::*;

mod coordinator;
pub use coordinator::*;

mod delegation_token;
pub use delegation_token::*;

mod delete_records;
pub use delete_records::*;

mod describe_log_dirs;
pub use describe_log_dirs::*;

mod describe_producers;
pub use describe_producers::*;

mod describe_transactions;
pub use describe_transactions::*;

mod describe_quorum;
pub use describe_quorum::*;

mod elect_leaders;
pub use elect_leaders::*;

mod fetch;
pub use fetch::*;

mod heartbeat;
pub use heartbeat::*;

mod join_group;
pub use join_group::*;

mod leave_group;
pub use leave_group::*;

mod sync_group;
pub use sync_group::*;

mod list_partition_reassignments;
pub use list_partition_reassignments::*;

mod list_transactions;
pub use list_transactions::*;

mod metadata;
pub use metadata::*;

mod list_offsets;
pub use list_offsets::*;

mod offset_commit;
pub use offset_commit::*;

mod offset_fetch;
pub use offset_fetch::*;

mod offset_for_leader_epoch;
pub use offset_for_leader_epoch::*;

mod offset_delete;
pub use offset_delete::*;

mod produce;
pub use produce::*;

mod describe_client_quotas;
pub use describe_client_quotas::*;

mod alter_client_quotas;
pub use alter_client_quotas::*;

mod sasl;
pub use sasl::*;

mod share;
pub use share::*;

mod share_group_offsets;
pub use share_group_offsets::*;

mod streams_group_describe;
pub use streams_group_describe::*;

mod telemetry;
pub use telemetry::*;

mod create_topics;
pub use create_topics::*;

mod delete_topics;
pub use delete_topics::*;

mod create_partitions;
pub use create_partitions::*;

mod describe_topic_partitions;
pub use describe_topic_partitions::*;

mod add_offsets_to_txn;
pub use add_offsets_to_txn::*;

mod add_partitions_to_txn;
pub use add_partitions_to_txn::*;

mod end_txn;
pub use end_txn::*;

mod init_producer_id;
pub use init_producer_id::*;

mod txn_offset_commit;
pub use txn_offset_commit::*;

mod describe_user_scram_credentials;
pub use describe_user_scram_credentials::*;

mod alter_user_scram_credentials;
pub use alter_user_scram_credentials::*;

mod write_txn_markers;
pub use write_txn_markers::*;
