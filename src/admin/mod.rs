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
mod groups;
mod offsets;
mod partitions;
mod quotas;
mod scram;
mod tokens;
mod topics;
mod transactions;
pub use builder::AdminClientBuilder;

/// Default partition limit for DescribeTopicPartitions pagination.
const DEFAULT_RESPONSE_PARTITION_LIMIT: i32 = 2000;

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
    pub topics: Vec<TopicPartitionDescription>,
    /// Pagination cursor topic name for the next page, if more pages remain.
    pub next_cursor_topic: Option<String>,
    /// Pagination cursor partition index for the next page.
    pub next_cursor_partition: Option<i32>,
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
/// Use [`AdminConfig::builder()`] or [`Default::default()`] to construct.
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// Bootstrap servers.
    pub(crate) bootstrap_servers: String,
    /// Client ID.
    pub(crate) client_id: String,
    /// Request timeout.
    pub(crate) request_timeout: Duration,
    /// Metadata recovery strategy (KIP-899).
    pub(crate) metadata_recovery_strategy: MetadataRecoveryStrategy,
    /// Duration after which failing metadata refreshes trigger a rebootstrap
    /// (KIP-899). Only effective with
    /// [`MetadataRecoveryStrategy::Rebootstrap`]. Default: 300 s.
    pub(crate) metadata_recovery_rebootstrap_trigger: Duration,
    /// Authentication configuration (optional).
    pub(crate) auth: Option<AuthConfig>,
    /// SOCKS5 proxy configuration (optional).
    #[cfg(feature = "socks5")]
    pub(crate) proxy: Option<crate::network::ProxyConfig>,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            client_id: "krafka-admin".to_string(),
            request_timeout: Duration::from_secs(30),
            metadata_recovery_strategy: MetadataRecoveryStrategy::Rebootstrap,
            metadata_recovery_rebootstrap_trigger: Duration::from_secs(300),
            auth: None,
            #[cfg(feature = "socks5")]
            proxy: None,
        }
    }
}

impl AdminConfig {
    /// Create a new config builder.
    pub fn builder() -> AdminConfigBuilder {
        AdminConfigBuilder::default()
    }

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

    /// Returns the request timeout.
    #[inline]
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
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

    /// Returns the SOCKS5 proxy configuration, if set.
    #[cfg(feature = "socks5")]
    #[inline]
    pub fn proxy(&self) -> Option<&crate::network::ProxyConfig> {
        self.proxy.as_ref()
    }
}

/// Builder for AdminConfig.
#[must_use = "builders do nothing until .build() is called"]
#[derive(Debug, Default)]
pub struct AdminConfigBuilder {
    config: AdminConfig,
}

impl AdminConfigBuilder {
    /// Set bootstrap servers.
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    /// Set client ID.
    pub fn client_id(mut self, id: impl Into<String>) -> Self {
        self.config.client_id = id.into();
        self
    }

    /// Set request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set the metadata recovery strategy (KIP-899).
    pub fn metadata_recovery_strategy(mut self, strategy: MetadataRecoveryStrategy) -> Self {
        self.config.metadata_recovery_strategy = strategy;
        self
    }

    /// Set the rebootstrap trigger duration (KIP-899).
    ///
    /// Only effective when [`MetadataRecoveryStrategy::Rebootstrap`] is set.
    pub fn metadata_recovery_rebootstrap_trigger(mut self, duration: Duration) -> Self {
        self.config.metadata_recovery_rebootstrap_trigger = duration;
        self
    }

    /// Set authentication configuration.
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = Some(auth);
        self
    }

    /// Set SOCKS5 proxy configuration.
    #[cfg(feature = "socks5")]
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.config.proxy = Some(proxy);
        self
    }

    /// Build the AdminConfig.
    pub fn build(self) -> AdminConfig {
        self.config
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
    /// Checks the client is not closed, picks the first available broker, and
    /// returns a connection from the pool. Most admin commands can be sent to
    /// any broker (the broker will forward as needed).
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

    /// Resolve the group coordinator for `group_id`.
    async fn find_group_coordinator(&self, group_id: &str) -> Result<Arc<BrokerConnection>> {
        let any_conn = self.get_any_broker_connection().await?;
        let coord_request = FindCoordinatorRequest::for_group(group_id);
        let coord_version = any_conn
            .negotiate_api_version(
                ApiKey::FindCoordinator,
                versions::FIND_COORDINATOR_MAX,
                versions::FIND_COORDINATOR_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported FindCoordinator API version",
                )
            })?;
        let coord_response_bytes = any_conn
            .send_request(ApiKey::FindCoordinator, coord_version, |buf| {
                coord_request.encode_versioned(coord_version, buf)
            })
            .await?;
        let mut coord_buf = coord_response_bytes;
        let coord_response =
            FindCoordinatorResponse::decode_versioned(coord_version, &mut coord_buf)?;

        if coord_response.error_code.is_ok() {
            let addr = format!("{}:{}", coord_response.host, coord_response.port);
            self.pool
                .get_connection_by_id(coord_response.node_id, &addr)
                .await
        } else {
            warn!(
                "FindCoordinator failed for group '{}': {:?}, using any broker",
                group_id, coord_response.error_code
            );
            Ok(any_conn)
        }
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
            .await
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
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported DescribeTopicPartitions API version",
                )
            })?;

        // Collect all pages into a single result.
        let mut all_topics: Vec<TopicPartitionDescription> = Vec::new();
        let mut cursor = None;

        loop {
            let request = DescribeTopicPartitionsRequest {
                topics: topics.clone(),
                response_partition_limit: DEFAULT_RESPONSE_PARTITION_LIMIT,
                cursor,
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
                    cursor = Some(DescribeTopicPartitionsCursor {
                        topic_name: c.topic_name,
                        partition_index: c.partition_index,
                    });
                }
                None => break,
            }
        }

        info!("Described partitions for {} topics", all_topics.len());
        Ok(DescribeTopicPartitionsResult {
            topics: all_topics,
            next_cursor_topic: None,
            next_cursor_partition: None,
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

    /// Force a rebootstrap: close all connections, clear the metadata cache,
    /// and fall back to bootstrap servers (KIP-899).
    pub async fn rebootstrap(&self) {
        self.metadata.rebootstrap().await;
    }

    /// Close the admin client.
    ///
    /// Sets the closed flag and tears down all broker connections.
    /// In-flight RPCs that have not yet received a response will fail
    /// with a network error. Callers should ensure long-running admin
    /// operations have completed before calling `close()`.
    ///
    /// Calling `close()` more than once is a no-op.
    pub async fn close(&self) {
        if self.closed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return;
        }
        self.pool.close_all().await;
        info!("AdminClient closed");
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

/// Replica (voter or observer) info from `AdminClient::describe_quorum`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct QuorumReplicaInfo {
    /// Replica broker ID.
    pub replica_id: i32,
    /// Last known log end offset, or -1 if unknown.
    pub log_end_offset: i64,
}

/// Per-partition quorum info from `AdminClient::describe_quorum`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct QuorumPartitionResult {
    /// Partition index.
    pub partition_index: i32,
    /// Per-partition error, or `None` on success.
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

/// Per-topic quorum info from `AdminClient::describe_quorum`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct QuorumTopicResult {
    /// Topic name.
    pub topic_name: String,
    /// Per-partition quorum results.
    pub partitions: Vec<QuorumPartitionResult>,
}

/// Result from `AdminClient::describe_quorum`.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct DescribeQuorumResult {
    /// Top-level error, or `None` on success.
    pub error: Option<String>,
    /// Per-topic quorum data.
    pub topics: Vec<QuorumTopicResult>,
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
}

impl OffsetSpec {
    /// Convert to the wire-format `timestamp` field used by `ListOffsets`.
    fn as_timestamp(self) -> i64 {
        match self {
            OffsetSpec::Earliest => -2,
            OffsetSpec::Latest => -1,
            OffsetSpec::Timestamp(ts) => ts,
        }
    }
}

/// Per-partition consumer group lag from [`AdminClient::consumer_group_lag`].
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
    /// Current end offset (high-watermark) of the partition.
    pub end_offset: i64,
    /// Lag = `end_offset − committed_offset`, or `None` if no offset was
    /// committed.  Clamped to zero — a negative lag indicates the offset was
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

    #[cfg(feature = "socks5")]
    #[test]
    fn test_admin_config_builder_proxy_round_trip() {
        let config = AdminConfig::builder()
            .proxy(crate::network::ProxyConfig::new("proxy:1080"))
            .build();
        let proxy = config.proxy().expect("proxy should be set");
        assert_eq!(proxy.address(), "proxy:1080");
    }
}
