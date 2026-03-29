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
//! | Fetch | 0 | 7 | v4+ isolation level, v7 fetch sessions |
//! | Metadata | 0 | 1 | v1+ includes controller info |
//! | OffsetCommit | 0 | 2 | v2+ for retention |
//! | OffsetFetch | 0 | 1 | v1+ for group coordinator |
//! | FindCoordinator | 0 | 0 | Group coordinator lookup |
//! | JoinGroup | 0 | 5 | v5+ group instance id |
//! | Heartbeat | 0 | 1 | v0 is baseline |
//! | SyncGroup | 0 | 3 | v3+ group instance id |
//! | LeaveGroup | 0 | 1 | v0 is baseline |
//! | CreateTopics | 0 | 2 | v0 is baseline |
//! | DeleteTopics | 0 | 1 | v0 is baseline |
//!
//! ## Example
//!
//! ```rust,ignore
//! use krafka::protocol::ApiKey;
//!
//! // Negotiate the best version for Fetch
//! // Try v7 first (fetch sessions), fall back to v4.
//! let version = conn.negotiate_api_version(ApiKey::Fetch, 7, 7).await
//!     .unwrap_or(4);
//! println!("Using Fetch v{}", version);
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
    /// Maximum supported Fetch version (v7 encode/decode — fetch sessions, KIP-227).
    pub const FETCH_MAX: i16 = 7;
    /// Maximum supported Metadata version (v0 encode/decode).
    pub const METADATA_MAX: i16 = 1;
    /// Maximum supported OffsetCommit version.
    pub const OFFSET_COMMIT_MAX: i16 = 2;
    /// Maximum supported OffsetFetch version.
    pub const OFFSET_FETCH_MAX: i16 = 1;
    /// Maximum supported FindCoordinator version.
    pub const FIND_COORDINATOR_MAX: i16 = 0;
    /// Maximum supported JoinGroup version.
    pub const JOIN_GROUP_MAX: i16 = 5;
    /// Maximum supported Heartbeat version.
    pub const HEARTBEAT_MAX: i16 = 1;
    /// Maximum supported SyncGroup version.
    pub const SYNC_GROUP_MAX: i16 = 3;
    /// Maximum supported LeaveGroup version.
    pub const LEAVE_GROUP_MAX: i16 = 1;
    /// Maximum supported CreateTopics version.
    pub const CREATE_TOPICS_MAX: i16 = 2;
    /// Maximum supported DeleteTopics version.
    pub const DELETE_TOPICS_MAX: i16 = 1;
    /// Maximum supported DescribeConfigs version.
    pub const DESCRIBE_CONFIGS_MAX: i16 = 0;
    /// Maximum supported AlterConfigs version.
    pub const ALTER_CONFIGS_MAX: i16 = 0;
    /// Maximum supported InitProducerId version.
    pub const INIT_PRODUCER_ID_MAX: i16 = 0;
    /// Maximum supported ListOffsets version (v2 encode/decode).
    pub const LIST_OFFSETS_MAX: i16 = 2;
}
