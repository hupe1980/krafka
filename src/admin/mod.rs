//! Admin client for Apache Kafka.
//!
//! This module provides administrative operations:
//! - Create/delete/describe topics
//! - Create additional partitions
//! - List topics and partitions
//! - Describe and alter configurations
//! - Manage ACLs
//! - Describe cluster and broker configs
//! - Manage delegation tokens (create, renew, expire, describe)
//! - Describe and alter client quotas
//! - Describe broker log directories
//! - Move replicas between log directories
//! - Elect partition leaders (preferred / unclean)
//! - Alter and list partition reassignments
//! - Delete committed offsets for consumer groups
//! - Describe and alter user SCRAM credentials
//! - Describe active producers on partitions
//! - Describe and list transactions
//! - List client metrics resources
//! - Write transaction markers / abort stuck transactions
//! - Describe KRaft quorum (voters, observers, leader)
//!
//! # Authentication
//!
//! The admin client supports all authentication mechanisms:
//! - PLAINTEXT (no auth)
//! - TLS/SSL
//! - SASL/PLAIN
//! - SASL/SCRAM-SHA-256/512
//! - SASL/AWS_MSK_IAM
//!
//! # Example
//!
//! ```rust,ignore
//! use krafka::admin::AdminClient;
//! use krafka::auth::AuthConfig;
//!
//! // With authentication
//! let client = AdminClient::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .auth(AuthConfig::sasl_plain("user", "password")?)
//!     .build()
//!     .await?;
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tracing::{info, warn};

use crate::auth::{AuthConfig, ScramMechanism};
use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::metadata::{ClusterMetadata, MetadataRecoveryStrategy, TopicInfo};
use crate::metrics::ConnectionMetrics;
use crate::network::{BrokerConnection, ConnectionPool};

use crate::protocol::{
    AclBinding, AclOperation, AclPatternType, AclPermissionType, AclResourceType, ApiKey,
    DeleteGroupsRequest, DeleteGroupsResponse, DescribeTopicPartitionsCursor,
    DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, FinalizedFeature,
    FindCoordinatorRequest, FindCoordinatorResponse, SupportedFeature, VersionedDecode,
    VersionedEncode, validate_topic_name, validate_topic_names, versions,
};

// Re-export for use by callers of `describe_configs`.
// All three types are required to build a `DescribeConfigsRequest` and are
// co-located here so callers can import exclusively from `krafka::admin`.
pub use crate::protocol::{ConfigResourceType, DescribeConfigsRequest, DescribeConfigsResource};
mod acls;
mod builder;
mod configs;
mod features;
mod group_offsets;
pub use group_offsets::OffsetVisibility;
mod groups;
pub use groups::GroupListing;
mod offsets;
mod partitions;
mod quotas;
mod scram;
mod share_group_offsets;
mod streams_groups;
pub use share_group_offsets::{
    DescribeShareGroupOffsetsResult, ShareGroupOffsetAlteration, ShareGroupOffsetDeletion,
    ShareGroupPartitionOffset,
};
mod tokens;
mod topics;
mod transactions;
pub use builder::AdminClientBuilder;

/// Default partition limit for DescribeTopicPartitions pagination.
const DEFAULT_RESPONSE_PARTITION_LIMIT: i32 = 2000;

/// Hard cap on DescribeTopicPartitions pagination iterations.
///
/// A broker that returns a non-advancing cursor (a bug, or a hostile peer)
/// would otherwise spin this loop forever while `all_topics` grows without
/// bound.
const MAX_DESCRIBE_TOPIC_PARTITIONS_PAGES: usize = 10_000;

/// Default number of attempts for a controller- or coordinator-routed request.
///
/// Mirrors the Java admin client's `retries`. Configurable per client through
/// [`AdminClientBuilder::retries`](crate::admin::AdminClientBuilder::retries);
/// this is only the default.
const DEFAULT_ADMIN_RETRIES: u32 = 5;

/// `true` when an error code means "you sent this to the wrong broker; find the
/// controller again".
#[inline]
fn is_controller_moved(code: crate::error::ErrorCode) -> bool {
    matches!(
        code,
        crate::error::ErrorCode::NotController | crate::error::ErrorCode::UnknownControllerId
    )
}

/// Outcome of one attempt at a controller-routed request.
///
/// Returned by the closure passed to [`AdminClient::with_controller`].
enum ControllerAttempt<T> {
    /// The request was served; this is the final result.
    Done(T),
    /// The broker reported that it is not (or no longer) the controller. The
    /// admin client refreshes metadata, re-resolves the controller, and retries.
    NotController(crate::error::ErrorCode),
}

/// Configuration for creating a topic.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct NewTopic {
    /// Topic name.
    pub name: String,
    /// Number of partitions.
    pub num_partitions: i32,
    /// Replication factor.
    pub replication_factor: i16,
    /// Topic configuration overrides.
    pub configs: HashMap<String, String>,
    /// Explicit replica placement: partition index → broker IDs, first entry
    /// being the preferred leader.
    ///
    /// Empty (the default) lets the controller place replicas, which is what
    /// most callers want. Set it when placement is the point:
    ///
    /// - **Rack-aware placement** the controller's own rule would not produce.
    /// - **Mirroring an existing topic's layout**, so a replacement topic
    ///   colocates with the one it replaces.
    /// - **Reproducing a broker's assignment in a test**, where "wherever the
    ///   controller feels like" is not an assertion.
    ///
    /// Kafka requires that `num_partitions` and `replication_factor` be `-1`
    /// when this is set, since the assignment already determines both.
    /// [`with_replica_assignment`](Self::with_replica_assignment) sets all
    /// three consistently and validates the shape.
    pub replica_assignments: HashMap<i32, Vec<i32>>,
}

impl NewTopic {
    /// Create a new topic configuration.
    ///
    /// # Arguments
    ///
    /// * `name` — Topic name. Must be non-empty and no longer than `i16::MAX`
    ///   bytes (the Kafka wire-format limit).
    /// * `num_partitions` — Must be positive or -1 (use broker default).
    /// * `replication_factor` — Must be positive or -1 (use broker default).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `name` is empty or exceeds the `i16::MAX`-byte wire-format limit, or
    /// - `num_partitions` or `replication_factor` is zero or less than -1.
    pub fn new(
        name: impl Into<String>,
        num_partitions: i32,
        replication_factor: i16,
    ) -> Result<Self> {
        let name = name.into();
        validate_topic_name(&name)?;
        if num_partitions == 0 || num_partitions < -1 {
            return Err(KrafkaError::config(format!(
                "num_partitions must be positive or -1, got {num_partitions}"
            )));
        }
        if replication_factor == 0 || replication_factor < -1 {
            return Err(KrafkaError::config(format!(
                "replication_factor must be positive or -1, got {replication_factor}"
            )));
        }
        Ok(Self {
            name,
            num_partitions,
            replication_factor,
            configs: HashMap::new(),
            replica_assignments: HashMap::new(),
        })
    }

    /// Create a topic with an explicit replica placement.
    ///
    /// `assignments` maps partition index to the broker IDs that should hold
    /// its replicas, first entry being the preferred leader. The partition
    /// count and replication factor follow from the map, so both are sent as
    /// `-1` — Kafka rejects a request that specifies an assignment *and* a
    /// count.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use krafka::admin::NewTopic;
    /// use std::collections::HashMap;
    ///
    /// # fn example() -> Result<(), krafka::error::KrafkaError> {
    /// // Three partitions, RF 2, pinned across racks the controller cannot see.
    /// let topic = NewTopic::with_replica_assignment(
    ///     "orders",
    ///     HashMap::from([
    ///         (0, vec![1, 4]),
    ///         (1, vec![2, 5]),
    ///         (2, vec![3, 6]),
    ///     ]),
    /// )?;
    /// # let _ = topic;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if `name` is invalid, if `assignments` is empty, if any
    /// partition has no replicas, or if the replica lists have differing
    /// lengths — Kafka requires a uniform replication factor across the
    /// partitions of one topic, and a broker rejects the request with
    /// `INVALID_REPLICA_ASSIGNMENT` rather than explaining which partition
    /// disagreed.
    pub fn with_replica_assignment(
        name: impl Into<String>,
        assignments: HashMap<i32, Vec<i32>>,
    ) -> Result<Self> {
        let name = name.into();
        validate_topic_name(&name)?;
        if assignments.is_empty() {
            return Err(KrafkaError::config(
                "replica assignment must name at least one partition",
            ));
        }

        let mut replication_factor = None;
        for (partition, brokers) in &assignments {
            if brokers.is_empty() {
                return Err(KrafkaError::config(format!(
                    "partition {partition} has no replicas"
                )));
            }
            match replication_factor {
                None => replication_factor = Some(brokers.len()),
                Some(expected) if expected != brokers.len() => {
                    return Err(KrafkaError::config(format!(
                        "every partition must have the same replication factor; \
                         partition {partition} has {} where an earlier one has {expected}",
                        brokers.len()
                    )));
                }
                Some(_) => {}
            }
        }

        Ok(Self {
            name,
            // Kafka rejects a request that carries both an assignment and a
            // count, because the assignment already determines both.
            num_partitions: -1,
            replication_factor: -1,
            configs: HashMap::new(),
            replica_assignments: assignments,
        })
    }

    /// Add a configuration option.
    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.insert(key.into(), value.into());
        self
    }
}

/// Result of topic creation.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct CreateTopicResult {
    /// Topic name.
    pub name: String,
    /// Error message if any.
    pub error: Option<String>,
}

/// Result of topic deletion.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DeleteTopicResult {
    /// Topic name.
    pub name: String,
    /// Error message if any.
    pub error: Option<String>,
}

/// Result of partition creation.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct CreatePartitionsResult {
    /// Topic name.
    pub topic: String,
    /// Error message if any.
    pub error: Option<String>,
}

/// The semantic value of a configuration entry.
///
/// Distinguishes between an explicit value, a broker-redacted sensitive value,
/// a key that uses the broker default, and a key that is not available at the
/// requested config source.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigValue {
    /// The config key has this explicit string value.
    Value(String),
    /// The broker redacted the value because it is sensitive (e.g. passwords).
    Sensitive,
    /// The config key has no explicitly set value; the broker default applies.
    Default,
    /// The config key is not available at the requested source.
    Unavailable,
}

impl ConfigValue {
    /// Returns the value as `&str` if it is [`ConfigValue::Value`], otherwise `None`.
    pub fn as_str(&self) -> Option<&str> {
        if let ConfigValue::Value(v) = self {
            Some(v.as_str())
        } else {
            None
        }
    }

    /// Returns `true` if this is an explicit [`ConfigValue::Value`].
    pub fn is_set(&self) -> bool {
        matches!(self, ConfigValue::Value(_))
    }

    /// Parse the value as type `T`.
    ///
    /// Returns `Err` if the value is not [`ConfigValue::Value`] or parsing fails.
    pub fn parse<T: std::str::FromStr>(&self) -> std::result::Result<T, ConfigParseError>
    where
        T::Err: std::fmt::Display,
    {
        match self {
            ConfigValue::Value(v) => v.parse::<T>().map_err(|e| ConfigParseError {
                message: e.to_string(),
            }),
            ConfigValue::Sensitive => Err(ConfigParseError {
                message: "config value is sensitive and cannot be parsed".to_string(),
            }),
            ConfigValue::Default => Err(ConfigParseError {
                message: "config value is the broker default and has no explicit value".to_string(),
            }),
            ConfigValue::Unavailable => Err(ConfigParseError {
                message: "config value is not available at the requested source".to_string(),
            }),
        }
    }
}

/// Error returned when [`ConfigValue::parse`] fails.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigParseError {
    /// Human-readable description of the parse failure.
    pub message: String,
}

impl std::fmt::Display for ConfigParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ConfigParseError {}

/// A configuration entry.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConfigEntry {
    /// Configuration name.
    pub name: String,
    /// Configuration value.
    pub value: Option<String>,
    /// Whether the config is read-only.
    pub read_only: bool,
    /// Whether this is the default value (v0 only; v1+ uses config_source).
    pub is_default: bool,
    /// Whether the config is sensitive (passwords, etc.).
    pub is_sensitive: bool,
    /// Configuration source (v1+). -1 if not available.
    pub config_source: i8,
    /// Synonyms for this configuration key (v1+).
    pub synonyms: Vec<ConfigSynonymEntry>,
    /// Configuration data type (v3+). 0 = UNKNOWN.
    pub config_type: i8,
    /// Configuration documentation (v3+).
    pub documentation: Option<String>,
}

impl ConfigEntry {
    /// Return the semantic [`ConfigValue`] for this entry.
    ///
    /// Interpretation priority:
    /// 1. If `is_sensitive` → [`ConfigValue::Sensitive`]
    /// 2. If `value` is `None` and `is_default` → [`ConfigValue::Default`]
    /// 3. If `value` is `Some` → [`ConfigValue::Value`]
    /// 4. Otherwise → [`ConfigValue::Unavailable`]
    pub fn config_value(&self) -> ConfigValue {
        if self.is_sensitive {
            return ConfigValue::Sensitive;
        }
        match &self.value {
            Some(v) => ConfigValue::Value(v.clone()),
            None if self.is_default => ConfigValue::Default,
            None => ConfigValue::Unavailable,
        }
    }
}

/// Per-resource result from
/// [`AdminClient::describe_configs_per_resource`].
///
/// Keeping the resource identity and its error alongside the entries is what
/// makes `TOPIC_AUTHORIZATION_FAILED` distinguishable from "this resource has
/// no config overrides" — flattening every resource into one entry list loses
/// both facts.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DescribeConfigsResourceResult {
    /// The type of resource described.
    pub resource_type: ConfigResourceType,
    /// The name of the resource described.
    pub resource_name: String,
    /// The raw per-resource error code. [`crate::error::ErrorCode::None`] on success.
    pub error_code: crate::error::ErrorCode,
    /// Human-readable per-resource error, or `None` on success.
    pub error: Option<String>,
    /// Configuration entries for this resource. Empty when `error` is set.
    pub configs: Vec<ConfigEntry>,
}

impl DescribeConfigsResourceResult {
    /// Whether this resource was described successfully.
    #[inline]
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.error_code.is_ok()
    }
}

/// A synonym for a configuration key.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConfigSynonymEntry {
    /// Synonym name.
    pub name: String,
    /// Synonym value.
    pub value: Option<String>,
    /// Synonym source.
    pub source: i8,
}

/// Result of config alteration.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct AlterConfigResult {
    /// Resource name.
    pub resource_name: String,
    /// Error message if any.
    pub error: Option<String>,
}

/// Result of describing ACLs.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DescribeAclsResult {
    /// Error message if any.
    pub error: Option<String>,
    /// List of ACL bindings found.
    pub bindings: Vec<AclBinding>,
}

/// Result of creating ACLs.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct CreateAclsResult {
    /// Results for each ACL creation.
    pub results: Vec<CreateAclResult>,
}

/// Result of a single ACL creation.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct CreateAclResult {
    /// Error message if any.
    pub error: Option<String>,
}

/// Result of deleting ACLs.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DeleteAclsResult {
    /// Results for each filter.
    pub filter_results: Vec<DeleteAclFilterResult>,
}

/// Result for a single ACL filter deletion.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DeleteAclFilterResult {
    /// Error message if any.
    pub error: Option<String>,
    /// Number of ACLs deleted by this filter.
    pub deleted_count: usize,
}

/// Consumer group type (classic vs. new consumer protocol from KIP-848).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupType {
    /// Classic consumer group protocol (JoinGroup/SyncGroup/Heartbeat).
    Classic,
    /// New consumer group protocol (KIP-848, ConsumerGroupHeartbeat).
    Consumer,
    /// Unknown or unrecognised group type.
    Unknown(String),
}

impl std::fmt::Display for GroupType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Classic => f.write_str("classic"),
            Self::Consumer => f.write_str("consumer"),
            Self::Unknown(s) => f.write_str(s),
        }
    }
}

/// Description of a consumer group.
///
/// This is a unified result type that covers both classic-protocol groups
/// (Key 15 — DescribeGroups) and KIP-848 consumer groups (Key 69 —
/// ConsumerGroupDescribe). The method [`AdminClient::describe_consumer_groups()`]
/// automatically detects each group's type and dispatches to the appropriate API.
///
/// Fields that are only available for one protocol type are wrapped in `Option`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescription {
    /// Group ID.
    pub group_id: String,
    /// Group type.
    pub group_type: GroupType,
    /// Group state (e.g., "Stable", "Empty", "Dead", "PreparingRebalance", "Assigning").
    pub state: String,
    /// Protocol type (classic groups only, e.g., "consumer").
    pub protocol_type: Option<String>,
    /// Protocol / assignor name. For classic groups, the partition assignment strategy
    /// (e.g., "range", "roundrobin"). For KIP-848 groups, the server-side assignor
    /// (e.g., "uniform").
    pub assignor: Option<String>,
    /// Group epoch (KIP-848 groups only).
    pub group_epoch: Option<i32>,
    /// Assignment epoch (KIP-848 groups only).
    pub assignment_epoch: Option<i32>,
    /// Group members.
    pub members: Vec<ConsumerGroupMember>,
    /// Authorized operations bitfield (KIP-848 groups only; -2^31 if not requested).
    pub authorized_operations: Option<i32>,
    /// Error message if any.
    pub error: Option<String>,
}

/// A member of a consumer group.
///
/// Fields that are only available for KIP-848 groups are wrapped in `Option`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConsumerGroupMember {
    /// Member ID.
    pub member_id: String,
    /// Group instance ID / instance ID (static membership).
    pub instance_id: Option<String>,
    /// Rack ID (KIP-848 groups only).
    pub rack_id: Option<String>,
    /// Current member epoch (KIP-848 groups only).
    pub member_epoch: Option<i32>,
    /// Client ID.
    pub client_id: String,
    /// Client host.
    pub client_host: String,
    /// Subscribed topic names (KIP-848 groups only).
    pub subscribed_topic_names: Option<Vec<String>>,
    /// Subscribed topic regex (KIP-848 groups only).
    pub subscribed_topic_regex: Option<String>,
    /// Current partition assignment (KIP-848 groups only).
    pub assignment: Option<Vec<TopicPartitionAssignment>>,
    /// Target partition assignment (KIP-848 groups only).
    pub target_assignment: Option<Vec<TopicPartitionAssignment>>,
    /// Member type (KIP-848 groups only). -1 = unknown, 0 = classic, 1 = consumer.
    pub member_type: Option<i8>,
}

/// Topic-partition assignment within a consumer group description.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct TopicPartitionAssignment {
    /// Topic ID (UUID).
    pub topic_id: [u8; 16],
    /// Topic name.
    pub topic_name: String,
    /// Assigned partition indices.
    pub partitions: Vec<i32>,
}

/// Listing entry for a consumer group.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConsumerGroupListing {
    /// Group ID.
    pub group_id: String,
    /// Protocol type (e.g., "consumer").
    pub protocol_type: String,
    /// Group type (Kafka 3.7+, KIP-848). `None` if the broker is too old.
    pub group_type: Option<GroupType>,
}

/// Result of [`AdminClient::describe_topic_partitions()`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DescribeTopicPartitionsResult {
    /// Described topics.
    ///
    /// Complete unless [`next_cursor_topic`](Self::next_cursor_topic) is set.
    pub topics: Vec<TopicPartitionDescription>,
    /// Pagination cursor topic name for the page that was **not** fetched.
    ///
    /// Normally `None`: the client drains every page before returning. It is
    /// populated only when pagination was abandoned because the broker returned
    /// a cursor that did not advance, or because the page cap was hit — in
    /// which case [`topics`](Self::topics) is a partial result.
    pub next_cursor_topic: Option<String>,
    /// Pagination cursor partition index matching
    /// [`next_cursor_topic`](Self::next_cursor_topic).
    pub next_cursor_partition: Option<i32>,
}

impl DescribeTopicPartitionsResult {
    /// Whether pagination completed. `false` means [`topics`](Self::topics)
    /// is truncated and a cursor is reported.
    #[inline]
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.next_cursor_topic.is_none()
    }
}

/// Per-topic result from [`AdminClient::describe_topic_partitions()`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct TopicPartitionDescription {
    /// Topic name.
    pub name: Option<String>,
    /// Topic ID (UUID).
    pub topic_id: [u8; 16],
    /// Whether the topic is internal.
    pub is_internal: bool,
    /// Partitions.
    pub partitions: Vec<PartitionDescription>,
    /// Authorized operations bitfield.
    pub topic_authorized_operations: i32,
    /// Error message if any.
    pub error: Option<String>,
}

/// Per-partition detail from [`AdminClient::describe_topic_partitions()`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct PartitionDescription {
    /// Partition index.
    pub partition_index: i32,
    /// Leader broker ID.
    pub leader_id: i32,
    /// Leader epoch.
    pub leader_epoch: i32,
    /// Replica broker IDs.
    pub replica_nodes: Vec<i32>,
    /// ISR broker IDs.
    pub isr_nodes: Vec<i32>,
    /// Eligible leader replicas (KIP-966).
    pub eligible_leader_replicas: Option<Vec<i32>>,
    /// Last known ELR (KIP-966).
    pub last_known_elr: Option<Vec<i32>>,
    /// Offline replica broker IDs.
    pub offline_replicas: Vec<i32>,
    /// Error message if any.
    pub error: Option<String>,
}

/// Result of deleting records from a partition.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DeleteRecordResult {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// The new log start offset (low watermark) after deletion.
    pub low_watermark: i64,
    /// Error message if any.
    pub error: Option<String>,
}

/// Result of an OffsetForLeaderEpoch request for a partition.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct LeaderEpochResult {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// The leader epoch.
    pub leader_epoch: i32,
    /// The end offset for this leader epoch.
    pub end_offset: i64,
    /// Error message if any.
    pub error: Option<String>,
}

/// A principal authorized to renew a delegation token.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DelegationTokenRenewer {
    /// Principal type (e.g., `"User"`).
    pub principal_type: String,
    /// Principal name.
    pub principal_name: String,
}

/// A delegation token returned by [`AdminClient::create_delegation_token()`] or
/// [`AdminClient::describe_delegation_token()`].
#[non_exhaustive]
#[derive(Clone)]
pub struct DelegationToken {
    /// Token owner principal type (e.g., `"User"`).
    pub principal_type: String,
    /// Token owner principal name.
    pub principal_name: String,
    /// When the token was issued (ms since epoch).
    pub issue_timestamp_ms: i64,
    /// When the token expires (ms since epoch).
    pub expiry_timestamp_ms: i64,
    /// Maximum timestamp at which the token can be renewed (ms since epoch).
    pub max_timestamp_ms: i64,
    /// Unique token ID.
    pub token_id: String,
    /// HMAC of the delegation token (used for SASL authentication).
    pub hmac: Bytes,
    /// Principals authorized to renew this token.
    ///
    /// Populated by [`AdminClient::describe_delegation_token()`]. Empty when
    /// returned from [`AdminClient::create_delegation_token()`] because the
    /// Create response does not include the renewer list.
    pub renewers: Vec<DelegationTokenRenewer>,
    /// Principal type of whoever *requested* the token, when it differs from
    /// the owner (KIP-373, `CreateDelegationToken` v3+).
    ///
    /// `None` on older brokers, and when the requester is the owner.
    ///
    /// KIP-373 lets one principal request a token *on behalf of* another —
    /// how a superuser provisions a token for a service account. The owner is
    /// who the token authenticates as; the requester is who asked for it, and
    /// that is the field an audit trail needs. Both were decoded from the
    /// response and dropped before reaching the caller, so the distinction the
    /// KIP exists to record was invisible.
    pub token_requester_principal_type: Option<String>,
    /// Principal name of whoever requested the token. See
    /// [`token_requester_principal_type`](Self::token_requester_principal_type).
    pub token_requester_principal_name: Option<String>,
}

impl std::fmt::Debug for DelegationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DelegationToken")
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

/// Result of creating a delegation token.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct CreateDelegationTokenResult {
    /// The created delegation token (present on success).
    pub token: Option<DelegationToken>,
    /// Error message if any.
    pub error: Option<String>,
}

/// Result of renewing a delegation token.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct RenewDelegationTokenResult {
    /// New expiry timestamp (ms since epoch).
    pub expiry_timestamp_ms: i64,
    /// Error message if any.
    pub error: Option<String>,
}

/// Result of expiring a delegation token.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ExpireDelegationTokenResult {
    /// New expiry timestamp (ms since epoch).
    pub expiry_timestamp_ms: i64,
    /// Error message if any.
    pub error: Option<String>,
}

/// A quota entity component describing who the quota applies to.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct QuotaEntityComponent {
    /// Entity type (e.g., `"user"`, `"client-id"`, `"ip"`).
    pub entity_type: String,
    /// Entity name. `None` represents the default entity.
    pub entity_name: Option<String>,
}

/// A quota configuration value.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct QuotaConfig {
    /// Quota key (e.g., `"producer_byte_rate"`, `"consumer_byte_rate"`,
    /// `"request_percentage"`).
    pub key: String,
    /// Quota value.
    pub value: f64,
}

/// A quota entry describing the quotas applied to an entity.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct QuotaDescription {
    /// Entity components (user, client-id, ip).
    pub entity: Vec<QuotaEntityComponent>,
    /// Quota configuration values.
    pub values: Vec<QuotaConfig>,
}

/// Result of describing client quotas.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DescribeClientQuotasResult {
    /// Quota entries matching the filter.
    pub entries: Vec<QuotaDescription>,
    /// Error message if any.
    pub error: Option<String>,
}

/// Result of altering a single quota entity.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct AlterClientQuotaResult {
    /// Entity components that were altered.
    pub entity: Vec<QuotaEntityComponent>,
    /// Error message if any.
    pub error: Option<String>,
}

/// Input for [`AdminClient::alter_client_quotas`].
///
/// Describes a set of quota operations (set or remove) to apply to a
/// single entity. An entity is identified by a list of (type, name) pairs —
/// for example `[("user", Some("alice")), ("client-id", None)]`.
#[derive(Debug, Clone)]
pub struct QuotaAlteration<'a> {
    /// Entity components (type, optional name). `None` name targets the
    /// default entity for that type.
    pub entity: Vec<(&'a str, Option<&'a str>)>,
    /// Quota operations. `Some(value)` sets the quota key;
    /// `None` removes it.
    pub ops: Vec<(&'a str, Option<f64>)>,
}

/// Filter for ACL operations (describe, delete).
///
/// This struct encapsulates all the filter parameters for ACL queries.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct AclFilter {
    /// Resource type to filter by.
    pub resource_type: AclResourceType,
    /// Resource name to filter by (None for any).
    pub resource_name: Option<String>,
    /// Pattern type for matching.
    pub pattern_type: AclPatternType,
    /// Principal to filter by (None for any).
    pub principal: Option<String>,
    /// Host to filter by (None for any).
    pub host: Option<String>,
    /// Operation to filter by.
    pub operation: AclOperation,
    /// Permission type to filter by.
    pub permission_type: AclPermissionType,
}

impl AclFilter {
    /// Create a new ACL filter that matches all ACLs.
    pub fn all() -> Self {
        Self::default()
    }

    /// Create a filter for a specific resource.
    pub fn for_resource(resource_type: AclResourceType, resource_name: impl Into<String>) -> Self {
        Self {
            resource_type,
            resource_name: Some(resource_name.into()),
            ..Default::default()
        }
    }

    /// Create a filter for a specific principal.
    pub fn for_principal(principal: impl Into<String>) -> Self {
        Self {
            principal: Some(principal.into()),
            ..Default::default()
        }
    }

    /// Set the resource type.
    pub fn resource_type(mut self, resource_type: AclResourceType) -> Self {
        self.resource_type = resource_type;
        self
    }

    /// Set the resource name.
    pub fn resource_name(mut self, name: impl Into<String>) -> Self {
        self.resource_name = Some(name.into());
        self
    }

    /// Set the pattern type.
    pub fn pattern_type(mut self, pattern_type: AclPatternType) -> Self {
        self.pattern_type = pattern_type;
        self
    }

    /// Set the principal.
    pub fn principal(mut self, principal: impl Into<String>) -> Self {
        self.principal = Some(principal.into());
        self
    }

    /// Set the host.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.host = Some(host.into());
        self
    }

    /// Set the operation.
    pub fn operation(mut self, operation: AclOperation) -> Self {
        self.operation = operation;
        self
    }

    /// Set the permission type.
    pub fn permission_type(mut self, permission_type: AclPermissionType) -> Self {
        self.permission_type = permission_type;
        self
    }
}

/// Admin client configuration.
///
/// Produced by [`AdminClient::builder()`], whose
/// [`build_config`](AdminClientBuilder::build_config) terminal returns it
/// without connecting. [`Default::default()`] also works.
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// Bootstrap servers.
    pub(crate) bootstrap_servers: String,
    /// Client ID.
    pub(crate) client_id: String,
    /// Request timeout.
    pub(crate) request_timeout: Duration,
    /// Time allowed for TCP establishment to one broker.
    pub(crate) connect_timeout: Duration,
    /// Metadata recovery strategy (KIP-899).
    pub(crate) metadata_recovery_strategy: MetadataRecoveryStrategy,
    /// Duration after which failing metadata refreshes trigger a rebootstrap
    /// (KIP-899). Only effective with
    /// [`MetadataRecoveryStrategy::Rebootstrap`]. Default: 300 s.
    pub(crate) metadata_recovery_rebootstrap_trigger: Duration,
    /// Authentication configuration (optional).
    pub(crate) auth: Option<AuthConfig>,
    /// Socket- and pool-level transport tuning.
    ///
    /// Defaults reproduce krafka's historical behaviour; see
    /// [`TransportConfig`](crate::network::TransportConfig).
    pub(crate) transport: crate::network::TransportConfig,
    /// Maximum age of cached cluster metadata before a refresh.
    ///
    /// Was hard-coded to 5 min at the one construction site, so an admin
    /// client on a fast-churning cluster could not shorten it.
    pub(crate) metadata_max_age: Duration,
    /// How many times a controller- or coordinator-routed request is re-issued
    /// after the broker answers `NOT_CONTROLLER`, `UNKNOWN_CONTROLLER_ID` or a
    /// retriable coordinator error.
    ///
    /// Counts *additional* attempts, as in the Java admin client and in this
    /// crate's own [`RetryPolicy`](crate::producer::RetryPolicy): `retries(0)`
    /// still makes one attempt. Default: 5, i.e. six attempts.
    pub(crate) retries: u32,
    /// Backoff between those attempts.
    ///
    /// Exponential with jitter, like every other retry in this crate — the
    /// admin client used to be the one place with a fixed 100 ms sleep and no
    /// jitter, so every admin client watching the same controller election
    /// retried in lockstep and hit the newly elected controller as one wave.
    ///
    /// Defaults to 100 ms initial, 10 s ceiling, 2× growth, 10 % jitter.
    pub(crate) retry_backoff: crate::util::BackoffPolicy,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            client_id: "krafka-admin".to_string(),
            request_timeout: Duration::from_secs(30),
            connect_timeout: crate::network::DEFAULT_CONNECT_TIMEOUT,
            metadata_recovery_strategy: MetadataRecoveryStrategy::Rebootstrap,
            metadata_recovery_rebootstrap_trigger: Duration::from_secs(300),
            auth: None,
            transport: crate::network::TransportConfig::default(),
            metadata_max_age: Duration::from_secs(300),
            retries: DEFAULT_ADMIN_RETRIES,
            retry_backoff: crate::util::BackoffPolicy::default(),
        }
    }
}

impl AdminConfig {
    /// Returns the bootstrap servers.
    #[inline]
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Returns the client ID.
    #[inline]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns how many times a controller- or coordinator-routed request is
    /// attempted before failing.
    #[inline]
    pub fn retries(&self) -> u32 {
        self.retries
    }

    /// Returns the backoff policy applied between those attempts.
    #[inline]
    pub fn retry_backoff(&self) -> &crate::util::BackoffPolicy {
        &self.retry_backoff
    }

    /// Returns the request timeout.
    #[inline]
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Returns the connect timeout.
    #[inline]
    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    /// Returns the metadata recovery strategy (KIP-899).
    #[inline]
    pub fn metadata_recovery_strategy(&self) -> MetadataRecoveryStrategy {
        self.metadata_recovery_strategy
    }

    /// Returns the rebootstrap trigger duration (KIP-899).
    #[inline]
    pub fn metadata_recovery_rebootstrap_trigger(&self) -> Duration {
        self.metadata_recovery_rebootstrap_trigger
    }

    /// Returns the authentication configuration, if set.
    #[inline]
    pub fn auth(&self) -> Option<&AuthConfig> {
        self.auth.as_ref()
    }
}

/// Result of deleting a single consumer group.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DeleteGroupResult {
    /// Group ID.
    pub group_id: String,
    /// Error message if any.
    pub error: Option<String>,
}

/// Cluster description returned by [`AdminClient::describe_cluster`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DescribeClusterResult {
    /// Cluster ID.
    pub cluster_id: String,
    /// Controller broker ID.
    pub controller_id: i32,
    /// Brokers in the cluster.
    pub brokers: Vec<DescribeClusterBrokerInfo>,
    /// Authorized operations bitfield (-2^31 if not requested).
    pub cluster_authorized_operations: i32,
}

/// Broker entry in [`DescribeClusterResult`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DescribeClusterBrokerInfo {
    /// Broker ID.
    pub broker_id: i32,
    /// Hostname.
    pub host: String,
    /// Port.
    pub port: i32,
    /// Rack (if assigned).
    pub rack: Option<String>,
}

/// Kafka admin client for cluster administration.
pub struct AdminClient {
    /// Configuration.
    config: AdminConfig,
    /// Cluster metadata.
    metadata: Arc<ClusterMetadata>,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Whether this admin client created the pool (and may therefore tear it
    /// down on [`close`](AdminClient::close)) or borrowed it from a
    /// [`KrafkaClient`](crate::client::KrafkaClient) via
    /// [`AdminClientBuilder::with_client`].
    ///
    /// Closing a *shared* pool would drop every producer and consumer
    /// connection on that client and fail all in-flight Produce/Fetch requests.
    pub(crate) pool_owned: bool,
    /// Whether the client has been closed.
    closed: std::sync::atomic::AtomicBool,
}

impl Drop for AdminClient {
    fn drop(&mut self) {
        // Warn when the client is dropped without an explicit `close()`:
        // in-flight RPCs are terminated abruptly and connections are not
        // cleanly shut down. Skip during panic unwinding.
        if !self.closed.load(std::sync::atomic::Ordering::SeqCst) && !std::thread::panicking() {
            warn!(
                "AdminClient dropped without close(); in-flight RPCs may fail abruptly. \
                 Call `AdminClient::close()` before drop."
            );
        }
    }
}

impl AdminClient {
    /// Create a new admin client builder.
    pub fn builder() -> AdminClientBuilder {
        AdminClientBuilder::default()
    }

    /// Return an error if the admin client has been closed.
    ///
    /// **Note:** This is a best-effort check. A concurrent call to [`close()`](Self::close)
    /// can race with the RPC that follows, in which case the RPC itself will fail
    /// with a network error rather than an "AdminClient is closed" message.
    #[inline]
    fn check_not_closed(&self) -> Result<()> {
        if self.is_closed() {
            return Err(KrafkaError::invalid_state("AdminClient is closed"));
        }
        Ok(())
    }

    /// Get a connection to any available broker.
    ///
    /// Checks the client is not closed, picks an available broker, and returns
    /// a connection from the pool. Suitable only for APIs that any broker can
    /// serve (reads such as `DescribeAcls`, `DescribeConfigs`, `ListGroups`).
    ///
    /// **Do not use this for controller-only or coordinator-only APIs** — see
    /// [`with_controller`](Self::with_controller) and
    /// [`find_group_coordinator`](Self::find_group_coordinator).
    async fn get_any_broker_connection(&self) -> Result<Arc<BrokerConnection>> {
        self.check_not_closed()?;
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::broker(
                crate::error::ErrorCode::UnknownServerError,
                "no brokers available",
            ));
        }
        let broker = &brokers[0];
        self.pool
            .get_connection_by_id(broker.id(), broker.address())
            .await
    }

    /// Get a connection to the cluster controller.
    ///
    /// Controller-only APIs must be routed here rather than to an arbitrary
    /// broker. A non-controller broker does forward such requests, but during a
    /// controller failover it answers `NOT_CONTROLLER` (41) instead — which the
    /// admin APIs surface only as a per-item error string, so a caller checking
    /// nothing but the `Result` concludes the operation succeeded.
    ///
    /// # Errors
    ///
    /// Returns [`crate::error::ErrorCode::UnknownControllerId`] if the cluster
    /// reports no controller even after a metadata refresh.
    pub async fn get_controller_connection(&self) -> Result<Arc<BrokerConnection>> {
        self.check_not_closed()?;
        self.metadata.get_controller_connection().await
    }

    /// Run a controller-only request with bounded `NOT_CONTROLLER` retries.
    ///
    /// The closure receives a connection to the current controller and returns
    /// either [`ControllerAttempt::Done`] with the final result, or
    /// [`ControllerAttempt::NotController`] to request re-resolution.
    ///
    /// Between attempts the admin client waits the configured
    /// [`retry_backoff`](crate::admin::AdminClientBuilder::retry_backoff) —
    /// exponential with jitter, so a fleet of admin clients watching the same
    /// election does not hit the new controller as one wave — then forces a
    /// metadata refresh (so the newly elected controller is discovered) and
    /// reconnects. After [`retries`](crate::admin::AdminClientBuilder::retries)
    /// unsuccessful attempts the last controller error is returned as a hard
    /// [`KrafkaError::Broker`], never as an `Ok` carrying a per-item error
    /// string.
    async fn with_controller<T, F, Fut>(&self, api: &str, mut op: F) -> Result<T>
    where
        F: FnMut(Arc<BrokerConnection>) -> Fut,
        Fut: std::future::Future<Output = Result<ControllerAttempt<T>>>,
    {
        self.check_not_closed()?;
        let mut last_code = crate::error::ErrorCode::NotController;

        // `retries` counts *additional* attempts, as it does in the Java admin
        // client and in this crate's own `RetryPolicy::max_retries`, so
        // `retries(0)` still gets one try.
        let attempts = self.config.retries.saturating_add(1);
        for attempt in 0..attempts {
            if attempt > 0 {
                tokio::time::sleep(self.config.retry_backoff.calculate_backoff(attempt)).await;
                // A full refresh re-reads `controller_id`; without it we would
                // reconnect to the same stale controller forever.
                if let Err(e) = self.metadata.refresh().await {
                    warn!("{api}: metadata refresh failed while re-resolving the controller: {e}");
                }
            }

            let conn = self.get_controller_connection().await?;
            match op(conn).await? {
                ControllerAttempt::Done(value) => return Ok(value),
                ControllerAttempt::NotController(code) => {
                    last_code = code;
                    warn!(
                        "{api}: broker reported {code:?} (attempt {}/{}); \
                         re-resolving the controller",
                        attempt + 1,
                        attempts
                    );
                }
            }
        }

        Err(KrafkaError::broker(
            last_code,
            format!(
                "{api}: the controller did not stabilise after {attempts} attempts; \
                 raise AdminClient::builder().retries(..) if elections on this cluster \
                 take longer"
            ),
        ))
    }

    /// Resolve the coordinator node for a group or transactional ID.
    ///
    /// Retries `FindCoordinator` with backoff while the broker reports a
    /// retriable error (`COORDINATOR_NOT_AVAILABLE`, `COORDINATOR_LOAD_IN_PROGRESS`,
    /// …). Unlike the previous behaviour, an unresolved coordinator is an
    /// **error** rather than a silent fallback to an arbitrary broker: that
    /// fallback guaranteed the follow-up request would fail with
    /// `NOT_COORDINATOR`, with the real cause already discarded.
    async fn find_coordinator_node(
        &self,
        key: &str,
        for_transaction: bool,
    ) -> Result<(i32, String)> {
        let kind = if for_transaction {
            "transaction"
        } else {
            "group"
        };
        let mut last_code = crate::error::ErrorCode::CoordinatorNotAvailable;

        // `retries` counts *additional* attempts, as it does in the Java admin
        // client and in this crate's own `RetryPolicy::max_retries`, so
        // `retries(0)` still gets one try.
        let attempts = self.config.retries.saturating_add(1);
        for attempt in 0..attempts {
            if attempt > 0 {
                tokio::time::sleep(self.config.retry_backoff.calculate_backoff(attempt)).await;
            }

            let conn = self.get_any_broker_connection().await?;
            let request = if for_transaction {
                FindCoordinatorRequest::for_transaction(key)
            } else {
                FindCoordinatorRequest::for_group(key)
            };

            let version = conn
                .negotiate_api_version(
                    ApiKey::FindCoordinator,
                    versions::FIND_COORDINATOR_MAX,
                    versions::FIND_COORDINATOR_MIN,
                )
                .ok_or_else(|| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::UnknownApiVersion,
                        "no mutually supported FindCoordinator API version",
                    )
                })?;

            let response_bytes = conn
                .send_request(ApiKey::FindCoordinator, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await?;
            let mut buf = response_bytes;
            let response = FindCoordinatorResponse::decode_versioned(version, &mut buf)?;

            if response.error_code.is_ok() {
                return Ok((
                    response.node_id,
                    format!("{}:{}", response.host, response.port),
                ));
            }

            last_code = response.error_code;
            if !response.error_code.is_retriable() {
                break;
            }
            warn!(
                "FindCoordinator for {kind} '{key}' returned {:?} (attempt {}/{})",
                response.error_code,
                attempt + 1,
                attempts
            );
        }

        Err(KrafkaError::broker(
            last_code,
            format!("could not resolve the coordinator for {kind} '{key}'"),
        ))
    }

    /// Refresh metadata for `topics` before a stale-leader retry.
    ///
    /// A bare `refresh_for_topics` can be suppressed by the `retry.backoff.ms`
    /// rate limiter, in which case the cache is untouched. Retrying against
    /// unchanged metadata reproduces the same `NotLeaderForPartition` and
    /// wastes the single retry the caller has. This helper waits out the
    /// backoff and re-issues so the retry sees genuinely new data.
    ///
    /// Refresh failures are logged rather than propagated: the caller still has
    /// a usable (if stale) cache and its own error reporting per partition.
    async fn refresh_topics_for_retry(&self, topics: &[&str], api: &str) {
        use crate::metadata::RefreshOutcome;

        match self.metadata.refresh_for_topics_outcome(Some(topics)).await {
            Ok(RefreshOutcome::Refreshed | RefreshOutcome::AlreadyFresh) => {}
            Ok(RefreshOutcome::RateLimited(remaining)) => {
                tokio::time::sleep(remaining).await;
                if let Err(e) = self.metadata.refresh_for_topics(Some(topics)).await {
                    warn!("{api}: metadata refresh failed before retry: {e}");
                }
            }
            Err(e) => warn!("{api}: metadata refresh failed before retry: {e}"),
        }
    }

    /// Resolve and connect to the group coordinator for `group_id`.
    ///
    /// # Errors
    ///
    /// Returns the broker's error code if the coordinator cannot be resolved.
    /// The request is **not** misrouted to an arbitrary broker.
    async fn find_group_coordinator(&self, group_id: &str) -> Result<Arc<BrokerConnection>> {
        let (node_id, addr) = self.find_coordinator_node(group_id, false).await?;
        self.pool.get_connection_by_id(node_id, &addr).await
    }

    /// Delete consumer groups by ID.
    ///
    /// Returns one [`DeleteGroupResult`] per group. Each result may contain
    /// an error if that particular group could not be deleted (e.g., it has
    /// active members).
    pub async fn delete_consumer_groups(
        &self,
        group_ids: Vec<String>,
    ) -> Result<Vec<DeleteGroupResult>> {
        self.check_not_closed()?;
        let conn = self.get_any_broker_connection().await?;

        let request = DeleteGroupsRequest::new(group_ids);
        let version = conn
            .negotiate_api_version(
                ApiKey::DeleteGroups,
                versions::DELETE_GROUPS_MAX,
                versions::DELETE_GROUPS_MIN,
            )
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported DeleteGroups API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DeleteGroups, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DeleteGroupsResponse::decode_versioned(version, &mut buf)?;

        let results = response
            .results
            .into_iter()
            .map(|r| DeleteGroupResult {
                group_id: r.group_id,
                error: if r.error_code.is_ok() {
                    None
                } else {
                    Some(format!("{:?}", r.error_code))
                },
            })
            .collect();

        Ok(results)
    }

    /// Describe topic partitions using the DescribeTopicPartitions API (Key 75).
    ///
    /// Returns detailed per-partition information including leader, replicas, ISR,
    /// eligible leader replicas (ELR), and offline replicas. Supports pagination
    /// for topics with many partitions.
    ///
    /// # Example
    /// ```ignore
    /// let result = admin
    ///     .describe_topic_partitions(vec!["my-topic".to_string()])
    ///     .await?;
    /// for topic in &result.topics {
    ///     println!("{}: {} partitions", topic.name.as_deref().unwrap_or("?"), topic.partitions.len());
    ///     for p in &topic.partitions {
    ///         println!("  partition {}: leader={}, isr={:?}", p.partition_index, p.leader_id, p.isr_nodes);
    ///     }
    /// }
    /// ```
    pub async fn describe_topic_partitions(
        &self,
        topics: Vec<String>,
    ) -> Result<DescribeTopicPartitionsResult> {
        self.check_not_closed()?;
        // H6: reject oversize topic names at ingress.
        validate_topic_names(topics.iter().map(String::as_str))?;
        let conn = self.get_any_broker_connection().await?;

        let version = conn
            .negotiate_api_version(
                ApiKey::DescribeTopicPartitions,
                versions::DESCRIBE_TOPIC_PARTITIONS_MAX,
                versions::DESCRIBE_TOPIC_PARTITIONS_MIN,
            )
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported DescribeTopicPartitions API version",
                )
            })?;

        // Collect all pages into a single result.
        let mut all_topics: Vec<TopicPartitionDescription> = Vec::new();
        let mut cursor = None;
        let mut pages = 0usize;
        // Set when the loop stops early with a cursor still outstanding, so the
        // caller can tell a truncated result from a complete one.
        let mut unfinished_cursor: Option<DescribeTopicPartitionsCursor> = None;

        loop {
            pages += 1;
            if pages > MAX_DESCRIBE_TOPIC_PARTITIONS_PAGES {
                warn!(
                    "DescribeTopicPartitions exceeded {MAX_DESCRIBE_TOPIC_PARTITIONS_PAGES} pages; \
                     the broker is returning a non-advancing cursor. Returning a partial result."
                );
                unfinished_cursor = cursor.clone();
                break;
            }

            let request = DescribeTopicPartitionsRequest {
                topics: topics.clone(),
                response_partition_limit: DEFAULT_RESPONSE_PARTITION_LIMIT,
                cursor: cursor.clone(),
            };

            let response_bytes = conn
                .send_request(ApiKey::DescribeTopicPartitions, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await?;

            let mut buf = response_bytes;
            let response = DescribeTopicPartitionsResponse::decode_versioned(version, &mut buf)?;

            for t in response.topics {
                // Find existing topic entry (pagination may split partitions across pages).
                // Use topic_id as merge key; Kafka always assigns a non-zero UUID.
                // Fall back to name comparison if topic_id is the null UUID (defensive).
                let null_uuid = [0u8; 16];
                let existing = if t.topic_id != null_uuid {
                    all_topics.iter_mut().find(|e| e.topic_id == t.topic_id)
                } else {
                    all_topics.iter_mut().find(|e| e.name == t.name)
                };
                let partitions: Vec<PartitionDescription> = t
                    .partitions
                    .into_iter()
                    .map(|p| PartitionDescription {
                        partition_index: p.partition_index,
                        leader_id: p.leader_id,
                        leader_epoch: p.leader_epoch,
                        replica_nodes: p.replica_nodes,
                        isr_nodes: p.isr_nodes,
                        eligible_leader_replicas: p.eligible_leader_replicas,
                        last_known_elr: p.last_known_elr,
                        offline_replicas: p.offline_replicas,
                        error: if p.error_code.is_ok() {
                            None
                        } else {
                            Some(format!("{:?}", p.error_code))
                        },
                    })
                    .collect();

                if let Some(entry) = existing {
                    entry.partitions.extend(partitions);
                } else {
                    all_topics.push(TopicPartitionDescription {
                        name: t.name,
                        topic_id: t.topic_id,
                        is_internal: t.is_internal,
                        partitions,
                        topic_authorized_operations: t.topic_authorized_operations,
                        error: if t.error_code.is_ok() {
                            None
                        } else {
                            Some(format!("{:?}", t.error_code))
                        },
                    });
                }
            }

            // Check for more pages.
            match response.next_cursor {
                Some(c) => {
                    let next = DescribeTopicPartitionsCursor {
                        topic_name: c.topic_name,
                        partition_index: c.partition_index,
                    };
                    // A cursor identical to the one we just sent means the
                    // broker is not making progress; stop rather than spin.
                    if cursor.as_ref().is_some_and(|prev| {
                        prev.topic_name == next.topic_name
                            && prev.partition_index == next.partition_index
                    }) {
                        warn!(
                            topic = %next.topic_name,
                            partition = next.partition_index,
                            "DescribeTopicPartitions returned a non-advancing cursor; \
                             stopping pagination with a partial result"
                        );
                        unfinished_cursor = Some(next);
                        break;
                    }
                    cursor = Some(next);
                }
                None => break,
            }
        }

        info!(
            "Described partitions for {} topics across {pages} page(s)",
            all_topics.len()
        );
        Ok(DescribeTopicPartitionsResult {
            topics: all_topics,
            next_cursor_topic: unfinished_cursor.as_ref().map(|c| c.topic_name.clone()),
            next_cursor_partition: unfinished_cursor.as_ref().map(|c| c.partition_index),
        })
    }

    /// Get access to the connection pool.
    pub fn pool(&self) -> &Arc<ConnectionPool> {
        &self.pool
    }

    /// Replace the bootstrap server list at runtime (KIP-899).
    ///
    /// The new addresses are used on the next metadata refresh that falls back
    /// to bootstrap servers. Does not close existing connections.
    ///
    /// # Errors
    ///
    /// Returns an error if `servers` is empty.
    pub fn update_seed_brokers(&self, servers: Vec<String>) -> Result<()> {
        self.metadata.update_seed_brokers(servers)
    }

    /// Re-read TLS certificate and key files from disk and atomically install
    /// the new material for all **future** connections (KIP-1288).
    ///
    /// Existing TLS sessions are unaffected: they keep the connector they
    /// handshaked with and are replaced naturally as connections cycle. On
    /// error the previously loaded certificates stay active, so a call made
    /// mid-rotation against a half-written PEM is safe to retry.
    ///
    /// No-op when TLS is not configured.
    ///
    /// Use this for event-driven rotation (an inotify watch, a sidecar
    /// signal). For unattended rotation set
    /// [`TransportConfig::tls_reload_interval`](crate::network::TransportConfig)
    /// instead and krafka reloads on a timer.
    ///
    /// # Errors
    ///
    /// Returns an error if the certificate or key files cannot be read or
    /// parsed.
    pub async fn refresh_tls(&self) -> Result<()> {
        self.pool.refresh_tls().await
    }

    /// Force a rebootstrap: close all connections, clear the metadata cache,
    /// and fall back to bootstrap servers (KIP-899).
    pub async fn rebootstrap(&self) {
        self.metadata.rebootstrap().await;
    }

    /// Close the admin client.
    ///
    /// Sets the closed flag so that subsequent operations fail fast.
    ///
    /// # Connection teardown depends on pool ownership
    ///
    /// - **Own pool** (the client was built from `bootstrap_servers`): all
    ///   broker connections are torn down. In-flight admin RPCs that have not
    ///   yet received a response will fail with a network error, so callers
    ///   should let long-running admin operations finish first.
    /// - **Shared pool** (built via
    ///   [`AdminClientBuilder::with_client`]): connections are left untouched.
    ///   The pool belongs to the [`KrafkaClient`](crate::client::KrafkaClient),
    ///   and closing it here would kill every producer and consumer connection
    ///   on that client and fail all in-flight Produce/Fetch requests. Close
    ///   the `KrafkaClient` to release those connections.
    ///
    /// Calling `close()` more than once is a no-op.
    pub async fn close(&self) {
        if self.closed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        if self.pool_owned {
            self.pool.close_all().await;
            info!("AdminClient closed (connection pool torn down)");
        } else {
            info!("AdminClient closed (shared connection pool left open)");
        }
    }

    /// Whether this admin client owns its connection pool.
    ///
    /// `false` when the pool was borrowed from a
    /// [`KrafkaClient`](crate::client::KrafkaClient) via
    /// [`AdminClientBuilder::with_client`]; in that case
    /// [`close`](Self::close) does not tear down connections.
    #[inline]
    #[must_use]
    pub fn owns_pool(&self) -> bool {
        self.pool_owned
    }

    /// Check if the admin client is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Get the shared connection metrics handle used by this admin client's broker pool.
    #[inline]
    pub fn connection_metrics(&self) -> Arc<ConnectionMetrics> {
        self.pool.metrics()
    }
}

/// Result from [`AdminClient::describe_features`] (KIP-584).
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DescribeFeaturesResult {
    /// Features supported by the responding broker.
    pub supported_features: Vec<SupportedFeature>,
    /// Cluster-wide finalized features.
    pub finalized_features: Vec<FinalizedFeature>,
    /// Monotonically increasing epoch for finalized features (−1 if unknown).
    pub finalized_features_epoch: i64,
}

/// Result from [`AdminClient::update_features`] (KIP-584).
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct UpdateFeaturesResult {
    /// Per-feature results.
    pub results: Vec<UpdateFeatureResult>,
}

/// Per-feature result from [`AdminClient::update_features`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct UpdateFeatureResult {
    /// Feature name.
    pub feature: String,
    /// Error message, or `None` if the update succeeded.
    pub error: Option<String>,
}

/// Information about a single broker log directory.
///
/// Returned by [`AdminClient::describe_log_dirs`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct LogDirInfo {
    /// Broker that owns this log directory.
    pub broker_id: i32,
    /// Absolute path of the log directory on the broker.
    pub log_dir: String,
    /// Per-directory error, or `None` on success.
    pub error: Option<String>,
    /// Topics and partitions stored in this directory.
    pub topics: Vec<LogDirTopicInfo>,
    /// Total bytes of the volume (-1 if unknown, v4+).
    pub total_bytes: i64,
    /// Usable bytes on the volume (-1 if unknown, v4+).
    pub usable_bytes: i64,
    /// Whether the directory is cordoned for decommissioning (KIP-1066,
    /// `DescribeLogDirs` v5+).
    ///
    /// A cordoned directory keeps serving its existing replicas but receives
    /// no new partition placements. `false` against brokers older than Kafka
    /// 4.3, where the field does not exist and no directory can be cordoned.
    pub is_cordoned: bool,
}

/// Per-topic partition details within a log directory.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct LogDirTopicInfo {
    /// Topic name.
    pub name: String,
    /// Partitions of this topic in the log directory.
    pub partitions: Vec<LogDirPartitionInfo>,
}

/// Per-partition details within a log directory.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct LogDirPartitionInfo {
    /// Partition index.
    pub partition_index: i32,
    /// Size of the log in bytes.
    pub partition_size: i64,
    /// Offset lag behind the high watermark.
    pub offset_lag: i64,
    /// Whether this is a future replica (reassignment in progress).
    pub is_future_key: bool,
}

/// Per-topic result from [`AdminClient::elect_leaders`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ElectLeadersResult {
    /// Topic name.
    pub topic: String,
    /// Per-partition election results.
    pub partitions: Vec<ElectLeadersPartitionInfo>,
}

/// Per-partition result from [`AdminClient::elect_leaders`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ElectLeadersPartitionInfo {
    /// Partition ID.
    pub partition_id: i32,
    /// Error message, or `None` if the election succeeded.
    pub error: Option<String>,
}

/// Result from [`AdminClient::alter_partition_reassignments`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct AlterReassignmentsResult {
    /// Top-level error, or `None` on success.
    pub error: Option<String>,
    /// Per-topic results.
    pub topics: Vec<ReassignmentTopicResult>,
}

/// Per-topic result from [`AdminClient::alter_partition_reassignments`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ReassignmentTopicResult {
    /// Topic name.
    pub name: String,
    /// Per-partition results.
    pub partitions: Vec<ReassignmentPartitionResult>,
}

/// Per-partition result from [`AdminClient::alter_partition_reassignments`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ReassignmentPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Error message, or `None` if the reassignment was accepted.
    pub error: Option<String>,
}

/// Per-topic ongoing reassignment info from [`AdminClient::list_partition_reassignments`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct PartitionReassignmentInfo {
    /// Topic name.
    pub name: String,
    /// Per-partition reassignment details.
    pub partitions: Vec<PartitionReassignmentPartitionInfo>,
}

/// Per-partition ongoing reassignment info from [`AdminClient::list_partition_reassignments`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct PartitionReassignmentPartitionInfo {
    /// Partition index.
    pub partition_index: i32,
    /// Current replica set.
    pub replicas: Vec<i32>,
    /// Replicas currently being added.
    pub adding_replicas: Vec<i32>,
    /// Replicas currently being removed.
    pub removing_replicas: Vec<i32>,
}

// ════════════════════════════════════════════════════════════════════════
// Result types for new admin APIs
// ════════════════════════════════════════════════════════════════════════

/// Per-topic result from [`AdminClient::alter_replica_log_dirs`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct AlterReplicaLogDirsResult {
    /// Broker that processed the request.
    pub broker_id: i32,
    /// Topic name.
    pub topic_name: String,
    /// Per-partition results.
    pub partitions: Vec<AlterReplicaLogDirsPartitionResult>,
}

/// Per-partition result from [`AdminClient::alter_replica_log_dirs`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct AlterReplicaLogDirsPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Error message, or `None` on success.
    pub error: Option<String>,
}

/// Result from `AdminClient::delete_offsets`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct OffsetDeleteResult {
    /// Top-level error, or `None` on success.
    pub error: Option<String>,
    /// Per-topic results.
    pub topics: Vec<OffsetDeleteTopicResult>,
}

/// Per-topic result from `AdminClient::delete_offsets`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct OffsetDeleteTopicResult {
    /// Topic name.
    pub name: String,
    /// Per-partition results.
    pub partitions: Vec<OffsetDeletePartitionResult>,
}

/// Per-partition result from `AdminClient::delete_offsets`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct OffsetDeletePartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Error message, or `None` on success.
    pub error: Option<String>,
}

/// Result from [`AdminClient::describe_user_scram_credentials`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DescribeUserScramCredentialsResult {
    /// Top-level error, or `None` on success.
    pub error: Option<String>,
    /// Per-user results.
    pub users: Vec<ScramCredentialUserResult>,
}

/// Per-user result from [`AdminClient::describe_user_scram_credentials`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ScramCredentialUserResult {
    /// User name.
    pub name: String,
    /// Error message, or `None` on success.
    pub error: Option<String>,
    /// Credential info entries.
    pub credential_infos: Vec<ScramCredentialInfoResult>,
}

/// SCRAM credential info for a user.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ScramCredentialInfoResult {
    /// SCRAM mechanism.
    pub mechanism: ScramMechanism,
    /// Number of iterations.
    pub iterations: i32,
}

/// Per-user result from [`AdminClient::alter_user_scram_credentials`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct AlterScramCredentialResult {
    /// User name.
    pub user: String,
    /// Error message, or `None` on success.
    pub error: Option<String>,
}

/// Per-topic result from [`AdminClient::describe_producers`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DescribeProducersTopicResult {
    /// Topic name.
    pub name: String,
    /// Per-partition results.
    pub partitions: Vec<DescribeProducersPartitionInfo>,
}

/// Per-partition result from [`AdminClient::describe_producers`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DescribeProducersPartitionInfo {
    /// Partition index.
    pub partition_index: i32,
    /// Error message, or `None` on success.
    pub error: Option<String>,
    /// Active producers on this partition.
    pub active_producers: Vec<ProducerStateInfo>,
}

/// Active producer state on a partition.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ProducerStateInfo {
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i32,
    /// Last sequence number sent. `-1` if unknown.
    pub last_sequence: i32,
    /// Last timestamp sent. `-1` if unknown.
    pub last_timestamp: i64,
    /// Coordinator epoch.
    pub coordinator_epoch: i32,
    /// Current transaction start offset. `-1` if not in a transaction.
    pub current_txn_start_offset: i64,
}

/// Transaction description from [`AdminClient::describe_transactions`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct TransactionDescription {
    /// Transactional ID.
    pub transactional_id: String,
    /// Error message, or `None` on success.
    pub error: Option<String>,
    /// Current state (e.g. "Ongoing", "PrepareCommit", "PrepareAbort").
    pub state: String,
    /// Transaction timeout in milliseconds.
    pub timeout_ms: i32,
    /// Transaction start time in milliseconds since epoch.
    pub start_time_ms: i64,
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Topic-partitions involved in the transaction.
    pub topics: Vec<TransactionTopicInfo>,
}

/// Topic-partitions involved in a transaction.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct TransactionTopicInfo {
    /// Topic name.
    pub topic: String,
    /// Partition indexes.
    pub partitions: Vec<i32>,
}

/// Result from [`AdminClient::list_transactions`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ListTransactionsResult {
    /// Top-level error, or `None` on success.
    pub error: Option<String>,
    /// State filters that were not recognized by the coordinator.
    pub unknown_state_filters: Vec<String>,
    /// Listed transactions.
    pub transactions: Vec<TransactionListEntry>,
}

/// A single transaction entry from [`AdminClient::list_transactions`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct TransactionListEntry {
    /// Transactional ID.
    pub transactional_id: String,
    /// Producer ID.
    pub producer_id: i64,
    /// Current transaction state.
    pub state: String,
}

/// Per-partition result from [`AdminClient::write_txn_markers`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct WriteTxnMarkersPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Error string, or `None` on success.
    pub error: Option<String>,
}

/// Per-topic result from [`AdminClient::write_txn_markers`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct WriteTxnMarkersTopicResult {
    /// Topic name.
    pub name: String,
    /// Per-partition results.
    pub partitions: Vec<WriteTxnMarkersPartitionResult>,
}

/// Result for one producer marker from [`AdminClient::write_txn_markers`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct WriteTxnMarkersResult {
    /// Producer ID this result pertains to.
    pub producer_id: i64,
    /// Per-topic results.
    pub topics: Vec<WriteTxnMarkersTopicResult>,
}

/// Replica (voter or observer) info from `AdminClient::describe_metadata_quorum`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct QuorumReplicaInfo {
    /// Replica broker ID.
    pub replica_id: i32,
    /// Last known log end offset, or -1 if unknown.
    pub log_end_offset: i64,
    /// Leader wall-clock time (epoch millis) of this replica's most recent
    /// fetch, or `-1` when unknown (KIP-836, `DescribeQuorum` v1+).
    ///
    /// `-1` for the leader's own entry, and for every replica when the broker
    /// only speaks v0. Subtract from the leader's clock to answer "how long
    /// has this voter been silent" — the check that distinguishes a slow
    /// follower from a dead one before a controller loses quorum.
    pub last_fetch_timestamp: i64,
    /// Leader wall-clock append time (epoch millis) of the offset this replica
    /// last fetched, or `-1` when unknown (KIP-836, `DescribeQuorum` v1+).
    ///
    /// The gap between this and the leader's current time is the replica's
    /// real replication lag in wall-clock terms, which `log_end_offset` alone
    /// cannot express on a low-traffic partition.
    pub last_caught_up_timestamp: i64,
    /// Directory UUID of this replica's log directory (KIP-853,
    /// `DescribeQuorum` v2+), or `None` against an older broker.
    ///
    /// From KIP-853 a KRaft voter is identified by `(replica_id,
    /// directory_id)`, not by ID alone. A reconfiguration tool that removes a
    /// voter by ID can otherwise remove a node rebuilt on a fresh disk while
    /// leaving the original in the quorum.
    pub replica_directory_id: Option<[u8; 16]>,
}

/// A listener endpoint of a quorum node (KIP-853, `DescribeQuorum` v2+).
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct QuorumListenerInfo {
    /// Listener name, e.g. `CONTROLLER`.
    pub name: String,
    /// Host name.
    pub host: String,
    /// Port.
    pub port: u16,
}

/// A node in the KRaft quorum and the endpoints it can be reached on
/// (KIP-853, `DescribeQuorum` v2+).
///
/// Below v2 `DescribeQuorum` reported replica IDs with no way to contact them,
/// leaving callers to cross-reference `DescribeCluster` and hope the two
/// agreed.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct QuorumNodeInfo {
    /// Node ID, matching a `replica_id` in the voter and observer lists.
    pub node_id: i32,
    /// Listener endpoints for this node.
    pub listeners: Vec<QuorumListenerInfo>,
}

/// Per-partition quorum info from `AdminClient::describe_metadata_quorum`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct QuorumPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition error, or `None` on success.
    ///
    /// Carries the broker's own message from `DescribeQuorum` v2+ (KIP-853)
    /// and falls back to the error code's name on older versions.
    pub error: Option<String>,
    /// Leader broker ID, or -1 if unknown.
    pub leader_id: i32,
    /// Latest known leader epoch.
    pub leader_epoch: i32,
    /// High watermark offset.
    pub high_watermark: i64,
    /// Current voters.
    pub current_voters: Vec<QuorumReplicaInfo>,
    /// Observers.
    pub observers: Vec<QuorumReplicaInfo>,
}

/// Per-topic quorum info from `AdminClient::describe_metadata_quorum`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct QuorumTopicResult {
    /// Topic name.
    pub topic_name: String,
    /// Per-partition quorum results.
    pub partitions: Vec<QuorumPartitionResult>,
}

/// Result from `AdminClient::describe_metadata_quorum`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DescribeQuorumResult {
    /// Top-level error, or `None` on success.
    pub error: Option<String>,
    /// Per-topic quorum data.
    pub topics: Vec<QuorumTopicResult>,
    /// Quorum nodes and their endpoints (KIP-853, `DescribeQuorum` v2+).
    /// Empty against an older broker.
    pub nodes: Vec<QuorumNodeInfo>,
}

/// A single per-partition result from [`AdminClient::list_offsets`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ListOffsetResult {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// The offset at the requested position (`-1` if unavailable).
    pub offset: i64,
    /// The timestamp associated with the offset (`-1` if not applicable).
    pub timestamp: i64,
    /// Per-partition error message, or `None` on success.
    pub error: Option<String>,
}

/// Offset specification for [`AdminClient::list_offsets`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetSpec {
    /// The earliest available offset in the partition (log start).
    Earliest,
    /// The end offset (high-watermark) of the partition.
    Latest,
    /// The first offset whose timestamp is ≥ the given milliseconds since
    /// the Unix epoch.
    Timestamp(i64),
    /// The offset of the record carrying the **largest timestamp** in the
    /// partition (KIP-734, `ListOffsets` v7+).
    ///
    /// Not the same as [`Latest`](Self::Latest) when producers write out of
    /// order — which they do whenever `CreateTime` timestamps come from
    /// application clocks, or when a partition is fed by several producers.
    /// This is the spec that answers "when was this partition last genuinely
    /// written to", which `Latest` cannot.
    MaxTimestamp,
    /// The earliest offset still held in the broker's **local** storage
    /// (KIP-405 tiered storage, `ListOffsets` v8+).
    ///
    /// Everything between [`Earliest`](Self::Earliest) and this offset lives in
    /// remote storage, where reads are typically orders of magnitude slower. A
    /// consumer or backfill job about to scan from the log start can use the
    /// gap to decide whether it is about to pull from object storage.
    EarliestLocal,
    /// The last offset that has been copied to **remote** storage
    /// (KIP-1005, `ListOffsets` v9+).
    ///
    /// The tiering frontier: everything at or below it survives local
    /// retention.
    LatestTiered,
}

impl OffsetSpec {
    /// Convert to the wire-format `timestamp` field used by `ListOffsets`.
    fn as_timestamp(self) -> i64 {
        match self {
            OffsetSpec::Earliest => -2,
            OffsetSpec::Latest => -1,
            OffsetSpec::MaxTimestamp => -3,
            OffsetSpec::EarliestLocal => -4,
            OffsetSpec::LatestTiered => -5,
            OffsetSpec::Timestamp(ts) => ts,
        }
    }

    /// Lowest `ListOffsets` version that understands this spec.
    ///
    /// The sentinels are negative timestamps, so a broker that predates one
    /// does not reject it — it treats the value as an ordinary timestamp and
    /// answers with the first offset at or after it, which for a negative
    /// number is the log start. The caller would get a plausible-looking
    /// answer to a question the broker never understood, so the version is
    /// checked before the request goes out rather than after.
    fn min_api_version(self) -> i16 {
        match self {
            OffsetSpec::Earliest | OffsetSpec::Latest | OffsetSpec::Timestamp(_) => {
                crate::protocol::versions::LIST_OFFSETS_MIN
            }
            OffsetSpec::MaxTimestamp => 7,
            OffsetSpec::EarliestLocal => 8,
            OffsetSpec::LatestTiered => 9,
        }
    }

    /// Human-readable name, for error messages.
    fn name(self) -> &'static str {
        match self {
            OffsetSpec::Earliest => "Earliest",
            OffsetSpec::Latest => "Latest",
            OffsetSpec::Timestamp(_) => "Timestamp",
            OffsetSpec::MaxTimestamp => "MaxTimestamp",
            OffsetSpec::EarliestLocal => "EarliestLocal",
            OffsetSpec::LatestTiered => "LatestTiered",
        }
    }
}

/// Per-partition consumer group lag from [`AdminClient::consumer_group_lag`].
///
/// # Unknown lag is `None`, never `0`
///
/// When `ListOffsets` fails for a partition, its end offset is unknown and both
/// [`end_offset`](Self::end_offset) and [`lag`](Self::lag) are `None`, with the
/// reason in [`end_offset_error`](Self::end_offset_error). Reporting `0` there
/// would hide a stalled consumer from lag alerting, which is exactly the
/// condition alerting exists to catch.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConsumerGroupLag {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Last committed offset for this group/partition, or `None` if no offset
    /// has been committed yet.
    pub committed_offset: Option<i64>,
    /// Current end offset (high-watermark) of the partition, or `None` if the
    /// `ListOffsets` request did not return a usable value for it.
    pub end_offset: Option<i64>,
    /// The per-partition `ListOffsets` error, if the end offset could not be
    /// determined. `None` when the end offset was fetched successfully.
    pub end_offset_error: Option<String>,
    /// Lag = `end_offset − committed_offset`.
    ///
    /// `None` when no offset was committed **or** when the end offset is
    /// unknown. Clamped to zero — a negative lag indicates the offset was
    /// committed ahead of the watermark (e.g. after a manual reset).
    pub lag: Option<i64>,
}

/// A single committed-offset entry from [`AdminClient::describe_consumer_group_offsets`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct GroupOffsetEntry {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Committed offset, or `-1` if none.
    pub committed_offset: i64,
    /// Optional metadata attached to the commit.
    pub metadata: Option<String>,
    /// Per-partition error, or `None` on success.
    pub error: Option<String>,
}

/// Per-partition result from [`AdminClient::alter_consumer_group_offsets`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct AlterGroupOffsetResult {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Error message, or `None` on success.
    pub error: Option<String>,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_new_topic() {
        let topic = NewTopic::new("test-topic", 3, 2)
            .unwrap()
            .with_config("cleanup.policy", "compact")
            .with_config("retention.ms", "86400000");

        assert_eq!(topic.name, "test-topic");
        assert_eq!(topic.num_partitions, 3);
        assert_eq!(topic.replication_factor, 2);
        assert_eq!(topic.configs.len(), 2);
    }

    #[test]
    fn test_new_topic_validation() {
        assert!(NewTopic::new("t", 1, 1).is_ok());
        assert!(NewTopic::new("t", -1, -1).is_ok());
        assert!(NewTopic::new("t", 0, 1).is_err());
        assert!(NewTopic::new("t", -2, 1).is_err());
        assert!(NewTopic::new("t", 1, 0).is_err());
        assert!(NewTopic::new("t", 1, -2).is_err());
    }

    /// H6: empty / oversize topic names must be rejected at `NewTopic::new`
    /// so the panicking `KafkaString::encode` path is unreachable from the
    /// public API.
    #[test]
    fn test_new_topic_name_validation_rejects_empty_and_oversize() {
        let empty = NewTopic::new("", 1, 1).unwrap_err().to_string();
        assert!(
            empty.contains("topic name cannot be empty"),
            "expected empty-name error, got: {empty}"
        );

        let oversize = "x".repeat(250);
        let err = NewTopic::new(oversize, 1, 1).unwrap_err().to_string();
        assert!(
            err.contains("exceeds maximum of 249"),
            "expected topic-name-length error, got: {err}"
        );

        // Boundary: exactly 249 bytes is accepted.
        let max_ok = "x".repeat(249);
        assert!(NewTopic::new(max_ok, 1, 1).is_ok());
    }

    #[test]
    fn test_admin_config_default() {
        let config = AdminConfig::default();
        assert_eq!(config.client_id, "krafka-admin");
        assert_eq!(config.request_timeout, Duration::from_secs(30));
        assert_eq!(
            config.metadata_recovery_strategy,
            MetadataRecoveryStrategy::Rebootstrap
        );
    }

    #[test]
    fn test_describe_acls_result() {
        let result = DescribeAclsResult {
            error: None,
            bindings: vec![
                AclBinding::allow_read_topic("my-topic", "User:alice"),
                AclBinding::allow_write_topic("my-topic", "User:bob"),
            ],
        };
        assert!(result.error.is_none());
        assert_eq!(result.bindings.len(), 2);
    }

    #[test]
    fn test_create_acls_result() {
        let result = CreateAclsResult {
            results: vec![
                CreateAclResult { error: None },
                CreateAclResult {
                    error: Some("ACL already exists".to_string()),
                },
            ],
        };
        assert!(result.results[0].error.is_none());
        assert!(result.results[1].error.is_some());
    }

    #[test]
    fn test_delete_acls_result() {
        let result = DeleteAclsResult {
            filter_results: vec![
                DeleteAclFilterResult {
                    error: None,
                    deleted_count: 3,
                },
                DeleteAclFilterResult {
                    error: None,
                    deleted_count: 0,
                },
            ],
        };
        assert_eq!(result.filter_results[0].deleted_count, 3);
        assert_eq!(result.filter_results[1].deleted_count, 0);
    }

    #[test]
    fn test_acl_filter_builder() {
        use crate::protocol::{AclOperation, AclPatternType, AclPermissionType, AclResourceType};

        // Test default filter (matches everything)
        let filter = AclFilter::all();
        assert_eq!(filter.resource_type, AclResourceType::Any);
        assert_eq!(filter.pattern_type, AclPatternType::Any);
        assert_eq!(filter.operation, AclOperation::Any);
        assert_eq!(filter.permission_type, AclPermissionType::Any);
        assert!(filter.resource_name.is_none());
        assert!(filter.principal.is_none());
        assert!(filter.host.is_none());

        // Test filter for specific resource
        let filter = AclFilter::for_resource(AclResourceType::Topic, "my-topic");
        assert_eq!(filter.resource_type, AclResourceType::Topic);
        assert_eq!(filter.resource_name, Some("my-topic".to_string()));

        // Test filter for specific principal
        let filter = AclFilter::for_principal("User:alice");
        assert_eq!(filter.principal, Some("User:alice".to_string()));

        // Test builder chain
        let filter = AclFilter::all()
            .resource_type(AclResourceType::Group)
            .resource_name("my-group")
            .pattern_type(AclPatternType::Literal)
            .principal("User:bob")
            .host("localhost")
            .operation(AclOperation::Read)
            .permission_type(AclPermissionType::Allow);

        assert_eq!(filter.resource_type, AclResourceType::Group);
        assert_eq!(filter.resource_name, Some("my-group".to_string()));
        assert_eq!(filter.pattern_type, AclPatternType::Literal);
        assert_eq!(filter.principal, Some("User:bob".to_string()));
        assert_eq!(filter.host, Some("localhost".to_string()));
        assert_eq!(filter.operation, AclOperation::Read);
        assert_eq!(filter.permission_type, AclPermissionType::Allow);
    }

    #[test]
    fn test_consumer_group_description() {
        let desc = ConsumerGroupDescription {
            group_id: "my-group".to_string(),
            group_type: GroupType::Classic,
            state: "Stable".to_string(),
            protocol_type: Some("consumer".to_string()),
            assignor: Some("range".to_string()),
            group_epoch: None,
            assignment_epoch: None,
            members: vec![
                ConsumerGroupMember {
                    member_id: "member-1".to_string(),
                    instance_id: Some("instance-1".to_string()),
                    rack_id: None,
                    member_epoch: None,
                    client_id: "my-client".to_string(),
                    client_host: "/127.0.0.1".to_string(),
                    subscribed_topic_names: None,
                    subscribed_topic_regex: None,
                    assignment: None,
                    target_assignment: None,
                    member_type: None,
                },
                ConsumerGroupMember {
                    member_id: "member-2".to_string(),
                    instance_id: None,
                    rack_id: None,
                    member_epoch: None,
                    client_id: "other-client".to_string(),
                    client_host: "/192.168.1.1".to_string(),
                    subscribed_topic_names: None,
                    subscribed_topic_regex: None,
                    assignment: None,
                    target_assignment: None,
                    member_type: None,
                },
            ],
            authorized_operations: None,
            error: None,
        };
        assert_eq!(desc.group_id, "my-group");
        assert_eq!(desc.group_type, GroupType::Classic);
        assert_eq!(desc.state, "Stable");
        assert_eq!(desc.members.len(), 2);
        assert!(desc.members[0].instance_id.is_some());
        assert!(desc.members[1].instance_id.is_none());
        assert!(desc.error.is_none());
    }

    #[test]
    fn test_consumer_group_listing() {
        let listing = ConsumerGroupListing {
            group_id: "my-group".to_string(),
            protocol_type: "consumer".to_string(),
            group_type: Some(GroupType::Consumer),
        };
        assert_eq!(listing.group_id, "my-group");
        assert_eq!(listing.protocol_type, "consumer");
        assert_eq!(listing.group_type, Some(GroupType::Consumer));
    }

    #[test]
    fn test_delete_record_result() {
        let result = DeleteRecordResult {
            topic: "my-topic".to_string(),
            partition: 0,
            low_watermark: 100,
            error: None,
        };
        assert_eq!(result.topic, "my-topic");
        assert_eq!(result.partition, 0);
        assert_eq!(result.low_watermark, 100);
        assert!(result.error.is_none());

        let result_err = DeleteRecordResult {
            topic: "my-topic".to_string(),
            partition: 1,
            low_watermark: -1,
            error: Some("NotLeaderOrFollower".to_string()),
        };
        assert!(result_err.error.is_some());
    }

    #[test]
    fn test_leader_epoch_result() {
        let result = LeaderEpochResult {
            topic: "my-topic".to_string(),
            partition: 0,
            leader_epoch: 5,
            end_offset: 1000,
            error: None,
        };
        assert_eq!(result.topic, "my-topic");
        assert_eq!(result.leader_epoch, 5);
        assert_eq!(result.end_offset, 1000);
        assert!(result.error.is_none());
    }

    #[test]
    fn test_admin_client_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AdminClient>();
    }

    /// An explicit replica placement must reach the wire, and must reject the
    /// shapes Kafka rejects — with a message naming the partition.
    ///
    /// Manual placement was unreachable: `NewTopic` had no way to express it
    /// and `create_topics` sent `assignments: Vec::new()` unconditionally. That
    /// rules out rack-aware placement the controller's own rule cannot
    /// produce, and mirroring an existing topic's layout.
    #[test]
    fn replica_assignment_is_expressible_and_validated() {
        use std::collections::HashMap;

        let topic = NewTopic::with_replica_assignment(
            "orders",
            HashMap::from([(0, vec![1, 4]), (1, vec![2, 5])]),
        )
        .expect("a uniform assignment is valid");

        // Kafka rejects a request carrying both an assignment and a count.
        assert_eq!(topic.num_partitions, -1);
        assert_eq!(topic.replication_factor, -1);
        assert_eq!(topic.replica_assignments.len(), 2);

        // A ragged replication factor is rejected here, where the message can
        // name the partition — the broker answers INVALID_REPLICA_ASSIGNMENT
        // without saying which one disagreed.
        let ragged = NewTopic::with_replica_assignment(
            "orders",
            HashMap::from([(0, vec![1, 4]), (1, vec![2])]),
        )
        .expect_err("a ragged replication factor must be rejected");
        assert!(
            ragged.to_string().contains("replication factor"),
            "got: {ragged}"
        );

        let empty_partition =
            NewTopic::with_replica_assignment("orders", HashMap::from([(0, Vec::new())]))
                .expect_err("a partition with no replicas must be rejected");
        assert!(
            empty_partition.to_string().contains("partition 0"),
            "the error must name the partition, got: {empty_partition}"
        );

        NewTopic::with_replica_assignment("orders", HashMap::new())
            .expect_err("an empty assignment names no partitions");
    }

    /// Every `OffsetSpec` must map to the wire sentinel Kafka defines for it,
    /// and must know the version that introduced it.
    ///
    /// The sentinels are negative timestamps. A broker too old to know one
    /// does not reject it — it answers as though the caller had asked for the
    /// first offset at or after a negative timestamp, i.e. the log start. So
    /// an unchecked spec does not fail; it returns a plausible wrong number.
    #[test]
    fn offset_spec_sentinels_match_the_protocol_and_carry_their_minimum_version() {
        use crate::protocol::versions;

        // KIP-734 and the tiered-storage specs (KIP-405, KIP-1005).
        assert_eq!(OffsetSpec::Earliest.as_timestamp(), -2);
        assert_eq!(OffsetSpec::Latest.as_timestamp(), -1);
        assert_eq!(OffsetSpec::MaxTimestamp.as_timestamp(), -3);
        assert_eq!(OffsetSpec::EarliestLocal.as_timestamp(), -4);
        assert_eq!(OffsetSpec::LatestTiered.as_timestamp(), -5);
        assert_eq!(
            OffsetSpec::Timestamp(1_700_000_000_000).as_timestamp(),
            1_700_000_000_000
        );

        assert_eq!(OffsetSpec::MaxTimestamp.min_api_version(), 7);
        assert_eq!(OffsetSpec::EarliestLocal.min_api_version(), 8);
        assert_eq!(OffsetSpec::LatestTiered.min_api_version(), 9);
        for spec in [
            OffsetSpec::Earliest,
            OffsetSpec::Latest,
            OffsetSpec::Timestamp(0),
        ] {
            assert_eq!(spec.min_api_version(), versions::LIST_OFFSETS_MIN);
        }

        // Every sentinel must be reachable at the version krafka negotiates,
        // or the API promises something the client cannot deliver.
        for spec in [
            OffsetSpec::MaxTimestamp,
            OffsetSpec::EarliestLocal,
            OffsetSpec::LatestTiered,
        ] {
            assert!(
                spec.min_api_version() <= versions::LIST_OFFSETS_MAX,
                "{} is unreachable at the negotiated ceiling",
                spec.name()
            );
        }
    }

    #[cfg(feature = "socks5")]
    #[test]
    fn test_admin_config_builder_proxy_round_trip() {
        let config = crate::admin::AdminClient::builder()
            .bootstrap_servers("localhost:9092")
            .proxy(crate::network::ProxyConfig::new("proxy:1080"))
            .build_config()
            .expect("config should build");
        let proxy = config
            .transport
            .proxy()
            .expect("proxy should reach the transport config");
        assert_eq!(proxy.address(), "proxy:1080");
    }

    // ══════════════════════════════════════════════════════════════════
    // Controller routing
    // ══════════════════════════════════════════════════════════════════

    /// The two codes that mean "you sent a controller-only request to a broker
    /// that is not (or is no longer) the controller".
    #[test]
    fn test_is_controller_moved_matches_exactly_the_controller_errors() {
        use crate::error::ErrorCode;

        assert!(is_controller_moved(ErrorCode::NotController));
        assert!(is_controller_moved(ErrorCode::UnknownControllerId));

        // Everything else must fall through to normal error handling —
        // treating unrelated failures as a controller move would retry
        // destructive operations that already failed for a different reason.
        for code in [
            ErrorCode::None,
            ErrorCode::TopicAlreadyExists,
            ErrorCode::ClusterAuthorizationFailed,
            ErrorCode::InvalidRequest,
            ErrorCode::NotCoordinator,
            ErrorCode::StaleControllerEpoch,
            ErrorCode::LeaderNotAvailable,
        ] {
            assert!(
                !is_controller_moved(code),
                "{code:?} must not be treated as a controller move"
            );
        }
    }

    /// NOT_CONTROLLER must be retriable so the controller loop is allowed to
    /// re-resolve; controller rerouting depends on this classification.
    #[test]
    fn test_not_controller_is_retriable_so_routing_can_recover() {
        assert!(crate::error::ErrorCode::NotController.is_retriable());
    }

    /// The default retry budget survives a controller failover without letting
    /// a destructive operation spin.
    #[test]
    fn test_controller_retry_budget_is_bounded() {
        let config = AdminConfig::default();
        assert!((2..=10).contains(&config.retries));

        // Worst case with jitter at its maximum, which is what a caller sizing
        // a request timeout has to budget for.
        let worst_case: Duration = (1..config.retries)
            .map(|attempt| config.retry_backoff.calculate_backoff(attempt))
            .sum();
        assert!(worst_case > Duration::ZERO);
        assert!(worst_case < Duration::from_secs(5), "got {worst_case:?}");
    }

    /// The admin client must back off with jitter like every other retry in
    /// the crate.
    ///
    /// It used to sleep a flat 100 ms, so every admin client watching one
    /// controller election retried in lockstep and arrived at the newly
    /// elected controller as a single wave — the thundering herd that
    /// `ClusterMetadata`'s rebootstrap jitter exists to avoid, in the one place
    /// that ignored the lesson.
    #[test]
    fn admin_retry_backoff_is_exponential_and_jittered() {
        let config = AdminConfig::default();
        assert!(
            config.retry_backoff.jitter_factor() > 0.0,
            "a fleet of admin clients must not retry in lockstep"
        );

        // Growth: a later attempt waits longer, even allowing for jitter in
        // both directions.
        let first = config.retry_backoff.calculate_backoff(1);
        let fourth = config.retry_backoff.calculate_backoff(4);
        assert!(
            fourth > first * 2,
            "backoff must grow across attempts, got {first:?} then {fourth:?}"
        );
    }

    /// The budget is configurable, which is what makes it usable on a cluster
    /// whose elections are slower than the default assumes.
    #[test]
    fn admin_retry_budget_is_configurable() {
        let config = AdminClient::builder()
            .bootstrap_servers("localhost:9092")
            .retries(20)
            .retry_backoff(Duration::from_millis(500))
            .build_config()
            .expect("a larger retry budget is valid");

        assert_eq!(config.retries(), 20);
        assert_eq!(
            config.retry_backoff().initial_backoff(),
            Duration::from_millis(500)
        );
    }

    /// Coordinator discovery retries only while the error is retriable; a
    /// terminal error must break out immediately instead of burning the budget.
    #[test]
    fn test_coordinator_retry_only_on_retriable_codes() {
        use crate::error::ErrorCode;

        assert!(ErrorCode::CoordinatorNotAvailable.is_retriable());
        assert!(ErrorCode::CoordinatorLoadInProgress.is_retriable());
        assert!(ErrorCode::NotCoordinator.is_retriable());

        assert!(!ErrorCode::GroupAuthorizationFailed.is_retriable());
        assert!(!ErrorCode::InvalidGroupId.is_retriable());
    }

    // ══════════════════════════════════════════════════════════════════
    // Close() must not tear down a shared pool
    // ══════════════════════════════════════════════════════════════════

    fn test_client(pool_owned: bool) -> AdminClient {
        let pool = Arc::new(ConnectionPool::new(
            crate::network::ConnectionConfig::default(),
        ));
        let metadata = Arc::new(ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            Arc::clone(&pool),
            Duration::from_secs(300),
        ));
        AdminClient {
            config: AdminConfig::default(),
            metadata,
            pool,
            pool_owned,
            closed: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// A client built from `bootstrap_servers` owns its pool and may close it.
    #[tokio::test]
    async fn test_close_tears_down_an_owned_pool() {
        let client = test_client(true);
        assert!(client.owns_pool());

        client.close().await;
        assert!(client.is_closed());
    }

    /// A client sharing a `KrafkaClient`'s pool must only mark itself closed.
    /// Calling `close_all()` here would drop every producer and consumer
    /// connection on that client and fail all in-flight Produce/Fetch requests.
    #[tokio::test]
    async fn test_close_leaves_a_shared_pool_open() {
        let client = test_client(false);
        assert!(!client.owns_pool());

        client.close().await;
        assert!(client.is_closed());

        // The shared pool is still usable by its real owner.
        let _ = client.pool().metrics();
    }

    #[tokio::test]
    async fn test_close_is_idempotent() {
        let client = test_client(true);
        client.close().await;
        client.close().await;
        assert!(client.is_closed());
    }

    #[tokio::test]
    async fn test_operations_fail_fast_after_close() {
        let client = test_client(true);
        client.close().await;

        let err = client.check_not_closed().unwrap_err();
        assert!(err.to_string().contains("closed"), "got: {err}");
    }

    // ══════════════════════════════════════════════════════════════════
    // DescribeTopicPartitions pagination
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn test_pagination_is_capped() {
        // An uncapped loop spins forever on a non-advancing cursor.
        assert!((1..=100_000).contains(&MAX_DESCRIBE_TOPIC_PARTITIONS_PAGES));
    }

    /// A complete result reports no cursor; a truncated one reports where it
    /// stopped, so callers can tell the two apart. Previously the cursor fields
    /// were always `None` and therefore dead.
    #[test]
    fn test_describe_topic_partitions_result_signals_completeness() {
        let complete = DescribeTopicPartitionsResult {
            topics: vec![],
            next_cursor_topic: None,
            next_cursor_partition: None,
        };
        assert!(complete.is_complete());

        let truncated = DescribeTopicPartitionsResult {
            topics: vec![],
            next_cursor_topic: Some("orders".into()),
            next_cursor_partition: Some(42),
        };
        assert!(!truncated.is_complete());
        assert_eq!(truncated.next_cursor_topic.as_deref(), Some("orders"));
        assert_eq!(truncated.next_cursor_partition, Some(42));
    }

    // ══════════════════════════════════════════════════════════════════
    // Unknown lag must never read as zero
    // ══════════════════════════════════════════════════════════════════

    #[test]
    fn test_lag_is_none_when_the_end_offset_is_unknown() {
        let stalled = ConsumerGroupLag {
            topic: "orders".into(),
            partition: 0,
            committed_offset: Some(100),
            end_offset: None,
            end_offset_error: Some("NotLeaderForPartition".into()),
            lag: None,
        };

        assert_eq!(
            stalled.lag, None,
            "an unknown end offset must report unknown lag, not 0 — \
             reporting 0 hides a stalled consumer from alerting"
        );
        assert!(stalled.end_offset_error.is_some());
    }

    #[test]
    fn test_lag_is_none_without_a_committed_offset() {
        let fresh = ConsumerGroupLag {
            topic: "orders".into(),
            partition: 0,
            committed_offset: None,
            end_offset: Some(500),
            end_offset_error: None,
            lag: None,
        };
        assert_eq!(fresh.lag, None);
    }

    #[test]
    fn test_lag_is_computed_and_clamped_when_both_ends_are_known() {
        let healthy = ConsumerGroupLag {
            topic: "orders".into(),
            partition: 0,
            committed_offset: Some(100),
            end_offset: Some(150),
            end_offset_error: None,
            lag: Some(50),
        };
        assert_eq!(healthy.lag, Some(50));

        // A commit ahead of the watermark (e.g. after a manual reset) clamps
        // to zero rather than reporting negative lag.
        let (committed, end) = (200i64, 150i64);
        assert_eq!((end - committed).max(0), 0);
    }

    // ══════════════════════════════════════════════════════════════════
    // DescribeConfigs per-resource attribution
    // ══════════════════════════════════════════════════════════════════

    /// An authorization failure must be distinguishable from "this resource
    /// has no config overrides"; flattening every resource into one entry list
    /// made the two identical.
    #[test]
    fn test_describe_configs_resource_result_preserves_error_attribution() {
        let denied = DescribeConfigsResourceResult {
            resource_type: ConfigResourceType::Topic,
            resource_name: "secret".into(),
            error_code: crate::error::ErrorCode::TopicAuthorizationFailed,
            error: Some("TopicAuthorizationFailed".into()),
            configs: vec![],
        };
        let empty = DescribeConfigsResourceResult {
            resource_type: ConfigResourceType::Topic,
            resource_name: "plain".into(),
            error_code: crate::error::ErrorCode::None,
            error: None,
            configs: vec![],
        };

        assert!(!denied.is_ok());
        assert!(empty.is_ok());
        assert_eq!(denied.configs.len(), empty.configs.len());
        assert_ne!(
            denied.is_ok(),
            empty.is_ok(),
            "both have zero configs, so only the error code tells them apart"
        );
        assert_eq!(denied.resource_name, "secret");
    }

    // ══════════════════════════════════════════════════════════════════
    // ConfigValue semantics
    // ══════════════════════════════════════════════════════════════════

    fn entry(value: Option<&str>, is_default: bool, is_sensitive: bool) -> ConfigEntry {
        ConfigEntry {
            name: "k".into(),
            value: value.map(str::to_string),
            read_only: false,
            is_default,
            is_sensitive,
            config_source: -1,
            synonyms: vec![],
            config_type: 0,
            documentation: None,
        }
    }

    #[test]
    fn test_config_value_classification() {
        assert_eq!(
            entry(Some("123"), false, false).config_value(),
            ConfigValue::Value("123".into())
        );
        // Sensitivity wins: a redacted value must never be exposed as Value.
        assert_eq!(
            entry(Some("hunter2"), false, true).config_value(),
            ConfigValue::Sensitive
        );
        assert_eq!(
            entry(None, true, false).config_value(),
            ConfigValue::Default
        );
        assert_eq!(
            entry(None, false, false).config_value(),
            ConfigValue::Unavailable
        );
    }

    #[test]
    fn test_config_value_parse_only_succeeds_for_explicit_values() {
        assert_eq!(
            ConfigValue::Value("86400000".into())
                .parse::<i64>()
                .unwrap(),
            86_400_000
        );
        assert!(ConfigValue::Value("nope".into()).parse::<i64>().is_err());
        assert!(ConfigValue::Sensitive.parse::<i64>().is_err());
        assert!(ConfigValue::Default.parse::<i64>().is_err());
        assert!(ConfigValue::Unavailable.parse::<i64>().is_err());

        assert!(ConfigValue::Value("x".into()).is_set());
        assert!(!ConfigValue::Default.is_set());
        assert_eq!(ConfigValue::Value("x".into()).as_str(), Some("x"));
        assert_eq!(ConfigValue::Sensitive.as_str(), None);
    }
}
