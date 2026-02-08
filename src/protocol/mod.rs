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
//! | Produce | 0 | 9 | v3+ for transactions, v5+ for headers |
//! | Fetch | 0 | 12 | v4+ for leader epoch |
//! | Metadata | 0 | 12 | v1+ includes controller info |
//! | OffsetCommit | 0 | 8 | v2+ for retention |
//! | OffsetFetch | 0 | 8 | v1+ for group coordinator |
//! | FindCoordinator | 0 | 4 | v1+ for key type |
//! | JoinGroup | 0 | 9 | v1+ for rebalance timeout |
//! | Heartbeat | 0 | 4 | v0 is baseline |
//! | SyncGroup | 0 | 5 | v0 is baseline |
//! | LeaveGroup | 0 | 5 | v0 is baseline |
//! | CreateTopics | 0 | 7 | v0 is baseline |
//! | DeleteTopics | 0 | 6 | v0 is baseline |
//!
//! ## Example
//!
//! ```rust,ignore
//! use krafka::protocol::ApiKey;
//!
//! // Negotiate the best version for Fetch
//! let version = conn.negotiate_api_version(ApiKey::Fetch, 12, 4).await;
//! if let Some(v) = version {
//!     println!("Using Fetch v{}", v);
//! }
//! ```

mod api;
mod codec;
mod header;
mod messages;
mod primitives;
mod record;

pub use api::{ApiKey, ApiVersionRange, ApiVersionsRequest, ApiVersionsResponse};
pub use codec::{Decoder, Encoder};
pub use header::{RequestHeader, ResponseHeader};
pub use messages::*;
pub use primitives::*;
pub use record::{
    Compression, LazyRecordBatch, LazyRecordIterator, Record, RecordBatch, RecordBatchBuilder,
    RecordHeader,
};

/// Client-supported API version ranges.
///
/// This module defines the version ranges that Krafka supports for each Kafka API.
/// These are used for version negotiation with Kafka brokers.
pub mod versions {
    /// Maximum supported Produce version.
    pub const PRODUCE_MAX: i16 = 9;
    /// Maximum supported Fetch version.
    pub const FETCH_MAX: i16 = 12;
    /// Maximum supported Metadata version.
    pub const METADATA_MAX: i16 = 12;
    /// Maximum supported OffsetCommit version.
    pub const OFFSET_COMMIT_MAX: i16 = 8;
    /// Maximum supported OffsetFetch version.
    pub const OFFSET_FETCH_MAX: i16 = 8;
    /// Maximum supported FindCoordinator version.
    pub const FIND_COORDINATOR_MAX: i16 = 4;
    /// Maximum supported JoinGroup version.
    pub const JOIN_GROUP_MAX: i16 = 9;
    /// Maximum supported Heartbeat version.
    pub const HEARTBEAT_MAX: i16 = 4;
    /// Maximum supported SyncGroup version.
    pub const SYNC_GROUP_MAX: i16 = 5;
    /// Maximum supported LeaveGroup version.
    pub const LEAVE_GROUP_MAX: i16 = 5;
    /// Maximum supported CreateTopics version.
    pub const CREATE_TOPICS_MAX: i16 = 7;
    /// Maximum supported DeleteTopics version.
    pub const DELETE_TOPICS_MAX: i16 = 6;
    /// Maximum supported DescribeConfigs version.
    pub const DESCRIBE_CONFIGS_MAX: i16 = 4;
    /// Maximum supported AlterConfigs version.
    pub const ALTER_CONFIGS_MAX: i16 = 2;
    /// Maximum supported InitProducerId version.
    pub const INIT_PRODUCER_ID_MAX: i16 = 4;
}
