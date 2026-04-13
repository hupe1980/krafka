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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tracing::{info, warn};

use crate::auth::{AuthConfig, ScramMechanism};
use crate::error::{KrafkaError, Result};
use crate::metadata::{ClusterMetadata, MetadataRecoveryStrategy, TopicInfo};
use crate::network::{BrokerConnection, ConnectionConfig, ConnectionPool};

use crate::protocol::{
    AclBinding, AclBindingFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
    AlterClientQuotasRequest, AlterClientQuotasResponse, AlterConfigOp,
    AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse, AlterQuotaEntity,
    AlterQuotaEntry, AlterQuotaOp, AlterReplicaLogDir, AlterReplicaLogDirsRequest,
    AlterReplicaLogDirsResponse, AlterUserScramCredentialsRequest,
    AlterUserScramCredentialsResponse, AlterableConfig, ApiKey, ConsumerGroupDescribeRequest,
    ConsumerGroupDescribeResponse, CreatableRenewer, CreatableTopic, CreatableTopicConfig,
    CreateAclsRequest, CreateAclsResponse, CreateDelegationTokenRequest,
    CreateDelegationTokenResponse, CreatePartitionsRequest, CreatePartitionsResponse,
    CreatePartitionsTopic, CreateTopicsRequest, CreateTopicsResponse, DeleteAclsRequest,
    DeleteAclsResponse, DeleteGroupsRequest, DeleteGroupsResponse, DeleteRecordsPartition,
    DeleteRecordsRequest, DeleteRecordsResponse, DeleteRecordsTopic, DeleteTopicState,
    DeleteTopicsRequest, DeleteTopicsResponse, DescribableLogDirTopic, DescribeAclsRequest,
    DescribeAclsResponse, DescribeClientQuotasRequest, DescribeClientQuotasResponse,
    DescribeClusterRequest, DescribeClusterResponse, DescribeConfigsResponse,
    DescribeDelegationTokenOwner, DescribeDelegationTokenRequest, DescribeDelegationTokenResponse,
    DescribeGroupsRequest, DescribeGroupsResponse, DescribeLogDirsRequest, DescribeLogDirsResponse,
    DescribeProducersRequest, DescribeProducersResponse, DescribeProducersTopicRequest,
    DescribeQuorumPartitionRequest, DescribeQuorumRequest, DescribeQuorumResponse,
    DescribeQuorumTopicRequest, DescribeTopicPartitionsCursor, DescribeTopicPartitionsRequest,
    DescribeTopicPartitionsResponse, DescribeTransactionsRequest, DescribeTransactionsResponse,
    DescribeUserScramCredentialsRequest, DescribeUserScramCredentialsResponse, ElectLeadersRequest,
    ElectLeadersResponse, ElectLeadersTopicPartitions, ElectionType, ExpireDelegationTokenRequest,
    ExpireDelegationTokenResponse, FinalizedFeature, FindCoordinatorRequest,
    FindCoordinatorResponse, IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse,
    ListClientMetricsResourcesRequest, ListClientMetricsResourcesResponse, ListGroupsRequest,
    ListGroupsResponse, ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse,
    ListPartitionReassignmentsTopic, ListTransactionsRequest, ListTransactionsResponse,
    OffsetDeletePartitionRequest, OffsetDeleteRequest, OffsetDeleteResponse,
    OffsetDeleteTopicRequest, OffsetForLeaderEpochPartition, OffsetForLeaderEpochRequest,
    OffsetForLeaderEpochResponse, OffsetForLeaderEpochTopic, QuotaFilterComponent,
    ReassignableTopic, RenewDelegationTokenRequest, RenewDelegationTokenResponse,
    ScramCredentialDeletion, ScramCredentialUpsertion, SupportedFeature, UpdateFeaturesRequest,
    UpdateFeaturesResponse, VersionedDecode, VersionedEncode, WritableTxnMarker,
    WritableTxnMarkerTopic, WriteTxnMarkersRequest, WriteTxnMarkersResponse, versions,
};

// Re-export for use by callers of `describe_configs`.
pub use crate::protocol::DescribeConfigsRequest;

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
    /// * `num_partitions` — Must be positive or -1 (use broker default).
    /// * `replication_factor` — Must be positive or -1 (use broker default).
    ///
    /// # Errors
    ///
    /// Returns an error if `num_partitions` or `replication_factor` is zero or
    /// less than -1.
    pub fn new(
        name: impl Into<String>,
        num_partitions: i32,
        replication_factor: i16,
    ) -> Result<Self> {
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
            name: name.into(),
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
#[derive(Debug, Clone)]
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
            metadata_recovery_strategy: MetadataRecoveryStrategy::None,
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
            .get_connection_by_id(broker.id, broker.address())
            .await
    }

    /// Create topics.
    pub async fn create_topics(
        &self,
        topics: Vec<NewTopic>,
        timeout: Duration,
    ) -> Result<Vec<CreateTopicResult>> {
        let conn = self.get_any_broker_connection().await?;

        // Build request
        let request = CreateTopicsRequest {
            topics: topics
                .iter()
                .map(|t| CreatableTopic {
                    name: t.name.clone(),
                    num_partitions: t.num_partitions,
                    replication_factor: t.replication_factor,
                    assignments: Vec::new(),
                    configs: t
                        .configs
                        .iter()
                        .map(|(k, v)| CreatableTopicConfig {
                            name: k.clone(),
                            value: Some(v.clone()),
                        })
                        .collect(),
                })
                .collect(),
            timeout_ms: crate::util::duration_to_millis_i32(timeout),
            validate_only: false,
        };

        // Send request — negotiate API version with broker
        let version = conn
            .negotiate_api_version(
                ApiKey::CreateTopics,
                versions::CREATE_TOPICS_MAX,
                versions::CREATE_TOPICS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported CreateTopics API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::CreateTopics, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        // Decode response
        let mut buf = response_bytes;
        let response = CreateTopicsResponse::decode_versioned(version, &mut buf)?;

        // Convert to results
        let results = response
            .topics
            .into_iter()
            .map(|t| CreateTopicResult {
                name: t.name,
                error: if t.error_code.is_ok() {
                    None
                } else {
                    Some(
                        t.error_message
                            .unwrap_or_else(|| format!("{:?}", t.error_code)),
                    )
                },
            })
            .collect();

        info!("Created {} topics", topics.len());
        Ok(results)
    }

    /// Delete topics.
    pub async fn delete_topics(
        &self,
        topics: Vec<String>,
        timeout: Duration,
    ) -> Result<Vec<DeleteTopicResult>> {
        let conn = self.get_any_broker_connection().await?;

        // Build request — populate both fields so the correct one is used
        // regardless of the negotiated version (v1–v5 use topic_names, v6+ use topics).
        let delete_topic_states: Vec<DeleteTopicState> = topics
            .iter()
            .map(|name| DeleteTopicState {
                name: Some(name.clone()),
                // Null UUID: deletion by topic name, not UUID.
                topic_id: [0u8; 16],
            })
            .collect();
        let topic_count = topics.len();
        let request = DeleteTopicsRequest {
            topic_names: topics,
            topics: delete_topic_states,
            timeout_ms: crate::util::duration_to_millis_i32(timeout),
        };

        // Send request — negotiate API version with broker
        let version = conn
            .negotiate_api_version(
                ApiKey::DeleteTopics,
                versions::DELETE_TOPICS_MAX,
                versions::DELETE_TOPICS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported DeleteTopics API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DeleteTopics, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        // Decode response
        let mut buf = response_bytes;
        let response = DeleteTopicsResponse::decode_versioned(version, &mut buf)?;

        // Convert to results
        let results = response
            .responses
            .into_iter()
            .map(|r| DeleteTopicResult {
                name: r.name.unwrap_or_default(),
                error: if r.error_code.is_ok() {
                    None
                } else {
                    Some(
                        r.error_message
                            .unwrap_or_else(|| format!("{:?}", r.error_code)),
                    )
                },
            })
            .collect();

        info!("Deleted {} topics", topic_count);
        Ok(results)
    }

    /// Increase the number of partitions for a topic.
    ///
    /// Note: Partition count can only be increased, never decreased.
    pub async fn create_partitions(
        &self,
        topic: impl Into<String>,
        new_total_count: i32,
        timeout: Duration,
    ) -> Result<CreatePartitionsResult> {
        let topic_name = topic.into();
        let conn = self.get_any_broker_connection().await?;

        // Build request
        let request = CreatePartitionsRequest {
            topics: vec![CreatePartitionsTopic {
                name: topic_name.clone(),
                count: new_total_count,
                assignments: None,
            }],
            timeout_ms: crate::util::duration_to_millis_i32(timeout),
            validate_only: false,
        };

        // Send request — negotiate API version with broker
        let version = conn
            .negotiate_api_version(
                ApiKey::CreatePartitions,
                versions::CREATE_PARTITIONS_MAX,
                versions::CREATE_PARTITIONS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported CreatePartitions API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::CreatePartitions, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        // Decode response
        let mut buf = response_bytes;
        let response = CreatePartitionsResponse::decode_versioned(version, &mut buf)?;

        let result = response
            .results
            .into_iter()
            .next()
            .map(|r| CreatePartitionsResult {
                topic: r.name,
                error: if r.error_code.is_ok() {
                    None
                } else {
                    Some(
                        r.error_message
                            .unwrap_or_else(|| format!("{:?}", r.error_code)),
                    )
                },
            })
            .unwrap_or(CreatePartitionsResult {
                topic: topic_name.clone(),
                error: Some("no response received".to_string()),
            });

        if result.error.is_none() {
            info!(
                "Increased partitions for topic {} to {}",
                topic_name, new_total_count
            );
        }
        Ok(result)
    }

    /// Describe configuration for one or more resources (topics, brokers, etc.).
    ///
    /// Uses DescribeConfigs (API Key 32). Build a [`DescribeConfigsRequest`]
    /// via its convenience constructors (`for_topic`, `for_broker`) or manually
    /// populate the `resources` field for multi-resource queries.
    pub async fn describe_configs(
        &self,
        request: DescribeConfigsRequest,
    ) -> Result<Vec<ConfigEntry>> {
        let conn = self.get_any_broker_connection().await?;

        let version = conn
            .negotiate_api_version(
                ApiKey::DescribeConfigs,
                versions::DESCRIBE_CONFIGS_MAX,
                versions::DESCRIBE_CONFIGS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported DescribeConfigs API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DescribeConfigs, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DescribeConfigsResponse::decode_versioned(version, &mut buf)?;

        let entries = response
            .results
            .into_iter()
            .flat_map(|r| {
                if !r.error_code.is_ok() {
                    return Vec::new();
                }
                r.configs
                    .into_iter()
                    .map(|c| ConfigEntry {
                        name: c.name,
                        value: c.value,
                        read_only: c.read_only,
                        is_default: c.is_default,
                        is_sensitive: c.is_sensitive,
                        config_source: c.config_source,
                        synonyms: c
                            .synonyms
                            .into_iter()
                            .map(|s| ConfigSynonymEntry {
                                name: s.name,
                                value: s.value,
                                source: s.source,
                            })
                            .collect(),
                        config_type: c.config_type,
                        documentation: c.documentation,
                    })
                    .collect()
            })
            .collect();

        Ok(entries)
    }

    /// Alter configuration for a topic.
    ///
    /// Uses IncrementalAlterConfigs (API Key 44) to set individual config keys
    /// without replacing the entire config. Each key-value pair is applied as a
    /// SET operation.
    pub async fn alter_topic_config(
        &self,
        topic: &str,
        configs: HashMap<String, String>,
    ) -> Result<AlterConfigResult> {
        let conn = self.get_any_broker_connection().await?;

        let request = IncrementalAlterConfigsRequest::for_topic(
            topic,
            configs
                .into_iter()
                .map(|(name, value)| AlterableConfig {
                    name,
                    config_operation: AlterConfigOp::Set,
                    value: Some(value),
                })
                .collect(),
        );

        let version = conn
            .negotiate_api_version(
                ApiKey::IncrementalAlterConfigs,
                versions::INCREMENTAL_ALTER_CONFIGS_MAX,
                versions::INCREMENTAL_ALTER_CONFIGS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported IncrementalAlterConfigs API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::IncrementalAlterConfigs, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = IncrementalAlterConfigsResponse::decode_versioned(version, &mut buf)?;

        let result = response
            .results
            .into_iter()
            .next()
            .map(|r| AlterConfigResult {
                resource_name: r.resource_name,
                error: if r.error_code.is_ok() {
                    None
                } else {
                    Some(
                        r.error_message
                            .unwrap_or_else(|| format!("{:?}", r.error_code)),
                    )
                },
            })
            .unwrap_or(AlterConfigResult {
                resource_name: topic.to_string(),
                error: Some("no response received".to_string()),
            });

        if result.error.is_none() {
            info!("Altered config for topic {}", topic);
        }
        Ok(result)
    }

    /// List all topics.
    pub async fn list_topics(&self) -> Result<Vec<String>> {
        self.check_not_closed()?;
        self.metadata.refresh().await?;
        Ok(self.metadata.topics().into_iter().map(|t| t.name).collect())
    }

    /// Describe topics.
    pub async fn describe_topics(&self, topics: &[String]) -> Result<Vec<TopicInfo>> {
        self.check_not_closed()?;
        self.metadata.refresh().await?;
        let all_topics = self.metadata.topics();

        let mut result = Vec::new();
        for topic_name in topics {
            if let Some(info) = all_topics.iter().find(|t| &t.name == topic_name) {
                result.push(info.clone());
            }
        }
        Ok(result)
    }

    /// Describe the cluster using the DescribeCluster API (Key 60).
    ///
    /// Returns cluster metadata including cluster ID, controller, brokers,
    /// and authorized operations.
    pub async fn describe_cluster(&self) -> Result<DescribeClusterResult> {
        self.check_not_closed()?;
        let conn = self.get_any_broker_connection().await?;

        let request = DescribeClusterRequest::default();
        let version = conn
            .negotiate_api_version(
                ApiKey::DescribeCluster,
                versions::DESCRIBE_CLUSTER_MAX,
                versions::DESCRIBE_CLUSTER_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported DescribeCluster API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DescribeCluster, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DescribeClusterResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            let msg = response
                .error_message
                .unwrap_or_else(|| format!("{:?}", response.error_code));
            return Err(KrafkaError::protocol(msg));
        }

        Ok(DescribeClusterResult {
            cluster_id: response.cluster_id,
            controller_id: response.controller_id,
            brokers: response
                .brokers
                .into_iter()
                .map(|b| DescribeClusterBrokerInfo {
                    broker_id: b.broker_id,
                    host: b.host,
                    port: b.port,
                    rack: b.rack,
                })
                .collect(),
            cluster_authorized_operations: response.cluster_authorized_operations,
        })
    }

    /// Get partition count for a topic.
    pub async fn partition_count(&self, topic: &str) -> Result<Option<usize>> {
        self.check_not_closed()?;
        self.metadata.refresh().await?;
        Ok(self.metadata.partition_count(topic))
    }

    /// Get the client ID.
    #[inline]
    pub fn client_id(&self) -> &str {
        &self.config.client_id
    }

    /// Get the request timeout.
    #[inline]
    pub fn request_timeout(&self) -> Duration {
        self.config.request_timeout
    }

    /// Describe ACLs matching a filter.
    ///
    /// # Example
    /// ```ignore
    /// // Describe all ACLs for a specific topic
    /// let filter = AclFilter::for_resource(AclResourceType::Topic, "my-topic");
    /// let result = admin.describe_acls(filter).await?;
    /// ```
    pub async fn describe_acls(&self, filter: AclFilter) -> Result<DescribeAclsResult> {
        self.check_not_closed()?;
        let conn = self.get_any_broker_connection().await?;

        let request = DescribeAclsRequest {
            resource_type: filter.resource_type,
            resource_name: filter.resource_name,
            pattern_type: filter.pattern_type,
            principal: filter.principal,
            host: filter.host,
            operation: filter.operation,
            permission_type: filter.permission_type,
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::DescribeAcls,
                versions::DESCRIBE_ACLS_MAX,
                versions::DESCRIBE_ACLS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported DescribeAcls API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DescribeAcls, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DescribeAclsResponse::decode_versioned(version, &mut buf)?;

        let bindings = response
            .resources
            .into_iter()
            .flat_map(|res| {
                res.acls.into_iter().map(move |acl| AclBinding {
                    resource_type: res.resource_type,
                    resource_name: res.resource_name.clone(),
                    pattern_type: res.pattern_type,
                    principal: acl.principal,
                    host: acl.host,
                    operation: acl.operation,
                    permission_type: acl.permission_type,
                })
            })
            .collect();

        Ok(DescribeAclsResult {
            error: if response.error_code.is_ok() {
                None
            } else {
                Some(
                    response
                        .error_message
                        .unwrap_or_else(|| format!("{:?}", response.error_code)),
                )
            },
            bindings,
        })
    }

    /// Create ACLs.
    ///
    /// # Arguments
    /// * `acls` - List of ACL bindings to create
    ///
    /// # Example
    /// ```ignore
    /// let acl = AclBinding::allow_read_topic("my-topic", "User:alice");
    /// admin.create_acls(vec![acl]).await?;
    /// ```
    pub async fn create_acls(&self, acls: Vec<AclBinding>) -> Result<CreateAclsResult> {
        let conn = self.get_any_broker_connection().await?;

        let request = CreateAclsRequest {
            creations: acls.clone(),
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::CreateAcls,
                versions::CREATE_ACLS_MAX,
                versions::CREATE_ACLS_MIN,
            )
            .await
            .ok_or_else(|| KrafkaError::protocol("no mutually supported CreateAcls API version"))?;

        let response_bytes = conn
            .send_request(ApiKey::CreateAcls, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = CreateAclsResponse::decode_versioned(version, &mut buf)?;

        let results = response
            .results
            .into_iter()
            .map(|r| CreateAclResult {
                error: if r.error_code.is_ok() {
                    None
                } else {
                    Some(
                        r.error_message
                            .unwrap_or_else(|| format!("{:?}", r.error_code)),
                    )
                },
            })
            .collect();

        info!("Created {} ACLs", acls.len());
        Ok(CreateAclsResult { results })
    }

    /// Delete ACLs matching the specified filters.
    ///
    /// # Arguments
    /// * `filters` - List of ACL binding filters to match for deletion
    ///
    /// # Example
    /// ```ignore
    /// // Delete all ACLs for a specific topic
    /// let filter = AclBindingFilter {
    ///     resource_type: AclResourceType::Topic,
    ///     resource_name: Some("my-topic".to_string()),
    ///     pattern_type: AclPatternType::Literal,
    ///     principal: None,
    ///     host: None,
    ///     operation: AclOperation::Any,
    ///     permission_type: AclPermissionType::Any,
    /// };
    /// admin.delete_acls(vec![filter]).await?;
    /// ```
    pub async fn delete_acls(&self, filters: Vec<AclBindingFilter>) -> Result<DeleteAclsResult> {
        let conn = self.get_any_broker_connection().await?;

        let request = DeleteAclsRequest {
            filters: filters.clone(),
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::DeleteAcls,
                versions::DELETE_ACLS_MAX,
                versions::DELETE_ACLS_MIN,
            )
            .await
            .ok_or_else(|| KrafkaError::protocol("no mutually supported DeleteAcls API version"))?;

        let response_bytes = conn
            .send_request(ApiKey::DeleteAcls, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DeleteAclsResponse::decode_versioned(version, &mut buf)?;

        let filter_results = response
            .filter_results
            .into_iter()
            .map(|fr| DeleteAclFilterResult {
                error: if fr.error_code.is_ok() {
                    None
                } else {
                    Some(
                        fr.error_message
                            .unwrap_or_else(|| format!("{:?}", fr.error_code)),
                    )
                },
                deleted_count: fr.matching_acls.len(),
            })
            .collect();

        info!("Deleted ACLs with {} filters", filters.len());
        Ok(DeleteAclsResult { filter_results })
    }

    /// Describe consumer groups.
    ///
    /// Automatically detects whether each group uses the classic protocol or the
    /// new consumer protocol (KIP-848) and dispatches to the appropriate API:
    /// - **Classic groups** → DescribeGroups (Key 15)
    /// - **Consumer groups** → ConsumerGroupDescribe (Key 69)
    ///
    /// The returned [`ConsumerGroupDescription`] is a unified type.
    /// Fields specific to one protocol variant are `Option`-wrapped.
    ///
    /// # Example
    /// ```ignore
    /// let groups = admin
    ///     .describe_consumer_groups(vec!["my-group".to_string()])
    ///     .await?;
    /// for group in &groups {
    ///     println!("{}: type={}, state={}, members={}",
    ///         group.group_id, group.group_type, group.state, group.members.len());
    /// }
    /// ```
    pub async fn describe_consumer_groups(
        &self,
        group_ids: Vec<String>,
    ) -> Result<Vec<ConsumerGroupDescription>> {
        self.check_not_closed()?;
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::broker(
                crate::error::ErrorCode::UnknownServerError,
                "no brokers available",
            ));
        }

        // Route each group to its coordinator broker.
        let mut coordinator_groups: HashMap<i32, Vec<String>> = HashMap::new();
        let any_broker = &brokers[0];
        let any_conn = self
            .pool
            .get_connection_by_id(any_broker.id, any_broker.address())
            .await?;

        for group_id in &group_ids {
            let coord_request = FindCoordinatorRequest::for_group(group_id);
            let coord_version = any_conn
                .negotiate_api_version(
                    ApiKey::FindCoordinator,
                    versions::FIND_COORDINATOR_MAX,
                    versions::FIND_COORDINATOR_MIN,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol("no mutually supported FindCoordinator API version")
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
                coordinator_groups
                    .entry(coord_response.node_id)
                    .or_default()
                    .push(group_id.clone());
            } else {
                warn!(
                    "FindCoordinator failed for group '{}': {:?}, falling back to broker {}",
                    group_id, coord_response.error_code, any_broker.id
                );
                coordinator_groups
                    .entry(any_broker.id)
                    .or_default()
                    .push(group_id.clone());
            }
        }

        let mut all_results = Vec::new();

        for (broker_id, groups) in &coordinator_groups {
            let broker = brokers
                .iter()
                .find(|b| b.id == *broker_id)
                .unwrap_or(any_broker);
            let conn = self
                .pool
                .get_connection_by_id(broker.id, broker.address())
                .await?;

            // Try ConsumerGroupDescribe (Key 69) first for all groups on this broker.
            let kip848_version = conn
                .negotiate_api_version(
                    ApiKey::ConsumerGroupDescribe,
                    versions::CONSUMER_GROUP_DESCRIBE_MAX,
                    versions::CONSUMER_GROUP_DESCRIBE_MIN,
                )
                .await;

            let mut classic_fallback: Vec<String> = Vec::new();
            let mut maybe_classic: Vec<(String, ConsumerGroupDescription)> = Vec::new();

            if let Some(version) = kip848_version {
                let request = ConsumerGroupDescribeRequest::new(groups.clone());
                let response_bytes = conn
                    .send_request(ApiKey::ConsumerGroupDescribe, version, |buf| {
                        request.encode_versioned(version, buf)
                    })
                    .await?;

                let mut buf = response_bytes;
                let response = ConsumerGroupDescribeResponse::decode_versioned(version, &mut buf)?;

                // Collect groups that ConsumerGroupDescribe returned as "OK but
                // empty". On Kafka 3.7–3.9 (KIP-848 Early Access), this
                // happens for classic-protocol groups. We'll try the classic
                // DescribeGroups as well and use whichever has members.

                for g in response.groups {
                    if g.error_code == crate::error::ErrorCode::GroupIdNotFound {
                        // Classic-protocol group — fall back to DescribeGroups (Key 15).
                        classic_fallback.push(g.group_id);
                        continue;
                    }

                    let members_empty = g.members.is_empty() && g.error_code.is_ok();
                    let group_id_clone = g.group_id.clone();

                    let desc = ConsumerGroupDescription {
                        group_id: g.group_id,
                        group_type: GroupType::Consumer,
                        state: g.group_state,
                        protocol_type: None,
                        assignor: Some(g.assignor_name),
                        group_epoch: Some(g.group_epoch),
                        assignment_epoch: Some(g.assignment_epoch),
                        members: g
                            .members
                            .into_iter()
                            .map(|m| ConsumerGroupMember {
                                member_id: m.member_id,
                                instance_id: m.instance_id,
                                rack_id: m.rack_id,
                                member_epoch: Some(m.member_epoch),
                                client_id: m.client_id,
                                client_host: m.client_host,
                                subscribed_topic_names: Some(m.subscribed_topic_names),
                                subscribed_topic_regex: m.subscribed_topic_regex,
                                assignment: Some(
                                    m.assignment
                                        .topic_partitions
                                        .into_iter()
                                        .map(|tp| TopicPartitionAssignment {
                                            topic_id: tp.topic_id,
                                            topic_name: tp.topic_name,
                                            partitions: tp.partitions,
                                        })
                                        .collect(),
                                ),
                                target_assignment: Some(
                                    m.target_assignment
                                        .topic_partitions
                                        .into_iter()
                                        .map(|tp| TopicPartitionAssignment {
                                            topic_id: tp.topic_id,
                                            topic_name: tp.topic_name,
                                            partitions: tp.partitions,
                                        })
                                        .collect(),
                                ),
                                member_type: Some(m.member_type),
                            })
                            .collect(),
                        authorized_operations: Some(g.authorized_operations),
                        error: if g.error_code.is_ok() {
                            None
                        } else {
                            let msg = g
                                .error_message
                                .unwrap_or_else(|| format!("{:?}", g.error_code));
                            Some(msg)
                        },
                    };

                    // Kafka 3.7–3.9 (KIP-848 Early Access) may return OK
                    // with empty members for classic-protocol groups instead
                    // of GroupIdNotFound.  Try the classic DescribeGroups
                    // path as well; if it reports members, use that result.
                    if members_empty {
                        maybe_classic.push((group_id_clone.clone(), desc));
                        classic_fallback.push(group_id_clone);
                    } else {
                        all_results.push(desc);
                    }
                }
            } else {
                // Broker does not support Key 69 — all groups are classic.
                classic_fallback = groups.clone();
            }

            // Describe classic-protocol groups via DescribeGroups (Key 15).
            if !classic_fallback.is_empty() {
                let request = DescribeGroupsRequest {
                    groups: classic_fallback,
                    include_authorized_operations: false,
                };

                let version = conn
                    .negotiate_api_version(
                        ApiKey::DescribeGroups,
                        versions::DESCRIBE_GROUPS_MAX,
                        versions::DESCRIBE_GROUPS_MIN,
                    )
                    .await
                    .ok_or_else(|| {
                        KrafkaError::protocol("no mutually supported DescribeGroups API version")
                    })?;

                let response_bytes = conn
                    .send_request(ApiKey::DescribeGroups, version, |buf| {
                        request.encode_versioned(version, buf)
                    })
                    .await?;

                let mut buf = response_bytes;
                let response = DescribeGroupsResponse::decode_versioned(version, &mut buf)?;

                for g in response.groups {
                    let classic_desc = ConsumerGroupDescription {
                        group_id: g.group_id,
                        group_type: GroupType::Classic,
                        state: g.group_state,
                        protocol_type: Some(g.protocol_type),
                        assignor: Some(g.protocol_data),
                        group_epoch: None,
                        assignment_epoch: None,
                        members: g
                            .members
                            .into_iter()
                            .map(|m| ConsumerGroupMember {
                                member_id: m.member_id,
                                instance_id: m.group_instance_id,
                                rack_id: None,
                                member_epoch: None,
                                client_id: m.client_id,
                                client_host: m.client_host,
                                subscribed_topic_names: None,
                                subscribed_topic_regex: None,
                                assignment: None,
                                target_assignment: None,
                                member_type: None,
                            })
                            .collect(),
                        authorized_operations: None,
                        error: if g.error_code.is_ok() {
                            None
                        } else {
                            Some(format!("{:?}", g.error_code))
                        },
                    };

                    // If this group was a maybe_classic candidate from
                    // ConsumerGroupDescribe, prefer whichever path found
                    // members. Remove from maybe_classic so we don't
                    // double-add it later.
                    if let Some(idx) = maybe_classic
                        .iter()
                        .position(|(id, _)| *id == classic_desc.group_id)
                    {
                        let (_, consumer_desc) = maybe_classic.swap_remove(idx);
                        if classic_desc.members.is_empty() {
                            // Neither path found members — keep the consumer result.
                            all_results.push(consumer_desc);
                        } else {
                            all_results.push(classic_desc);
                        }
                    } else {
                        all_results.push(classic_desc);
                    }
                }
            }

            // Any remaining maybe_classic entries that weren't resolved
            // by the classic fallback (shouldn't happen, but be safe).
            for (_, desc) in maybe_classic {
                all_results.push(desc);
            }
        }

        info!("Described {} consumer groups", all_results.len());
        Ok(all_results)
    }

    /// List all consumer groups on the cluster.
    ///
    /// Returns a list of all consumer groups with their protocol types.
    ///
    /// # Example
    /// ```ignore
    /// let groups = admin.list_consumer_groups().await?;
    /// for group in &groups {
    ///     println!("{} ({})", group.group_id, group.protocol_type);
    /// }
    /// ```
    pub async fn list_consumer_groups(&self) -> Result<Vec<ConsumerGroupListing>> {
        self.check_not_closed()?;
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::broker(
                crate::error::ErrorCode::UnknownServerError,
                "no brokers available",
            ));
        }

        // ListGroups returns groups managed by each broker, so we query all brokers
        let mut all_groups = Vec::new();
        let mut seen_ids = HashSet::new();
        let mut broker_failures = 0usize;
        let broker_count = brokers.len();

        for broker in &brokers {
            let conn = match self
                .pool
                .get_connection_by_id(broker.id, broker.address())
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        "Failed to connect to broker {} for ListGroups, skipping: {}",
                        broker.id, e
                    );
                    broker_failures += 1;
                    continue;
                }
            };

            let request = ListGroupsRequest {
                states_filter: Vec::new(),
                types_filter: Vec::new(),
            };

            let version = match conn
                .negotiate_api_version(
                    ApiKey::ListGroups,
                    versions::LIST_GROUPS_MAX,
                    versions::LIST_GROUPS_MIN,
                )
                .await
            {
                Some(v) => v,
                None => {
                    warn!(
                        "No mutually supported ListGroups API version for broker {}, skipping",
                        broker.id
                    );
                    broker_failures += 1;
                    continue;
                }
            };

            let response_bytes = match conn
                .send_request(ApiKey::ListGroups, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    warn!("ListGroups RPC failed on broker {}: {}", broker.id, e);
                    broker_failures += 1;
                    continue;
                }
            };

            let mut buf = response_bytes;
            let response = match ListGroupsResponse::decode_versioned(version, &mut buf) {
                Ok(r) => r,
                Err(e) => {
                    warn!("ListGroups decode failed on broker {}: {}", broker.id, e);
                    broker_failures += 1;
                    continue;
                }
            };

            if !response.error_code.is_ok() {
                tracing::warn!(
                    "ListGroups error on broker {}: {:?}",
                    broker.id,
                    response.error_code
                );
                broker_failures += 1;
                continue;
            }

            for group in response.groups {
                if seen_ids.insert(group.group_id.clone()) {
                    let group_type = group.group_type.map(|t| match t.as_str() {
                        "classic" => GroupType::Classic,
                        "consumer" => GroupType::Consumer,
                        other => GroupType::Unknown(other.to_string()),
                    });
                    all_groups.push(ConsumerGroupListing {
                        group_id: group.group_id,
                        protocol_type: group.protocol_type,
                        group_type,
                    });
                }
            }
        }

        if broker_failures == broker_count {
            return Err(KrafkaError::invalid_state(
                "list_consumer_groups failed: all brokers returned errors",
            ));
        }

        if broker_failures > 0 {
            warn!(
                "list_consumer_groups: {broker_failures}/{broker_count} brokers failed; \
                 results may be incomplete"
            );
        }

        info!("Listed {} consumer groups", all_groups.len());
        Ok(all_groups)
    }

    /// Delete records from topic partitions before the specified offsets.
    ///
    /// Records with offsets less than the specified offset for each partition
    /// will be marked for deletion. This adjusts the log start offset.
    ///
    /// # Arguments
    /// * `offsets` - Map of (topic, partition) to the offset before which to delete
    /// * `timeout` - Operation timeout
    ///
    /// # Example
    /// ```ignore
    /// use std::collections::HashMap;
    /// let mut offsets = HashMap::new();
    /// offsets.insert(("my-topic".to_string(), 0), 100i64);
    /// let results = admin.delete_records(offsets, Duration::from_secs(30)).await?;
    /// ```
    pub async fn delete_records(
        &self,
        offsets: HashMap<(String, i32), i64>,
        timeout: Duration,
    ) -> Result<Vec<DeleteRecordResult>> {
        self.check_not_closed()?;

        for attempt in 0u8..2 {
            if attempt == 1 {
                let topics: Vec<&str> = offsets.keys().map(|(t, _)| t.as_str()).collect();
                let _ = self.metadata.refresh_for_topics(Some(&topics)).await;
            }

            let brokers = self.metadata.brokers();
            if brokers.is_empty() {
                return Err(KrafkaError::broker(
                    crate::error::ErrorCode::UnknownServerError,
                    "no brokers available",
                ));
            }

            // Group offsets by partition leader
            let mut leader_offsets: HashMap<i32, HashMap<String, Vec<DeleteRecordsPartition>>> =
                HashMap::new();
            let fallback_broker_id = brokers[0].id;

            for ((topic, partition), offset) in &offsets {
                let leader_id = self
                    .metadata
                    .leader(topic, *partition)
                    .unwrap_or(fallback_broker_id);
                leader_offsets
                    .entry(leader_id)
                    .or_default()
                    .entry(topic.clone())
                    .or_default()
                    .push(DeleteRecordsPartition {
                        partition_index: *partition,
                        offset: *offset,
                    });
            }

            let mut results = Vec::new();
            let mut has_stale_leader = false;

            for (broker_id, topics_map) in leader_offsets {
                let broker = brokers
                    .iter()
                    .find(|b| b.id == broker_id)
                    .unwrap_or(&brokers[0]);
                let conn = self
                    .pool
                    .get_connection_by_id(broker.id, broker.address())
                    .await?;

                let request = DeleteRecordsRequest {
                    topics: topics_map
                        .into_iter()
                        .map(|(name, partitions)| DeleteRecordsTopic { name, partitions })
                        .collect(),
                    timeout_ms: crate::util::duration_to_millis_i32(timeout),
                };

                let version = conn
                    .negotiate_api_version(
                        ApiKey::DeleteRecords,
                        versions::DELETE_RECORDS_MAX,
                        versions::DELETE_RECORDS_MIN,
                    )
                    .await
                    .ok_or_else(|| {
                        KrafkaError::protocol("no mutually supported DeleteRecords API version")
                    })?;

                let response_bytes = conn
                    .send_request(ApiKey::DeleteRecords, version, |buf| {
                        request.encode_versioned(version, buf)
                    })
                    .await?;

                let mut buf = response_bytes;
                let response = DeleteRecordsResponse::decode_versioned(version, &mut buf)?;

                for topic in response.topics {
                    let topic_name = topic.name;
                    for partition in topic.partitions {
                        if partition.error_code == crate::error::ErrorCode::NotLeaderForPartition {
                            has_stale_leader = true;
                        }
                        results.push(DeleteRecordResult {
                            topic: topic_name.clone(),
                            partition: partition.partition_index,
                            low_watermark: partition.low_watermark,
                            error: if partition.error_code.is_ok() {
                                None
                            } else {
                                Some(format!("{:?}", partition.error_code))
                            },
                        });
                    }
                }
            }

            if has_stale_leader && attempt == 0 {
                warn!(
                    "NotLeaderForPartition in DeleteRecords response, retrying with refreshed metadata"
                );
                continue;
            }

            info!("Deleted records from {} partition(s)", results.len());
            return Ok(results);
        }
        unreachable!()
    }

    /// Get the end offset for each partition at the given leader epoch.
    ///
    /// This is used to detect log truncation after a leader change. For each
    /// topic-partition, the broker returns the end offset for the requested
    /// leader epoch. If the epoch is no longer valid, the broker returns
    /// the epoch and offset where the log was truncated.
    ///
    /// # Arguments
    /// * `partitions` - List of (topic, partition, leader_epoch) tuples
    ///
    /// # Example
    /// ```ignore
    /// let results = admin.offset_for_leader_epoch(
    ///     vec![("my-topic".to_string(), 0, 5)]
    /// ).await?;
    /// for r in &results {
    ///     println!("{}:{} epoch={} end_offset={}", r.topic, r.partition, r.leader_epoch, r.end_offset);
    /// }
    /// ```
    pub async fn offset_for_leader_epoch(
        &self,
        partitions: Vec<(String, i32, i32)>,
    ) -> Result<Vec<LeaderEpochResult>> {
        self.check_not_closed()?;

        for attempt in 0u8..2 {
            if attempt == 1 {
                let topics: Vec<&str> = partitions.iter().map(|(t, _, _)| t.as_str()).collect();
                let _ = self.metadata.refresh_for_topics(Some(&topics)).await;
            }

            let brokers = self.metadata.brokers();
            if brokers.is_empty() {
                return Err(KrafkaError::broker(
                    crate::error::ErrorCode::UnknownServerError,
                    "no brokers available",
                ));
            }

            // Group partitions by their leader broker
            let fallback_broker_id = brokers[0].id;
            let mut leader_partitions: HashMap<
                i32,
                HashMap<String, Vec<OffsetForLeaderEpochPartition>>,
            > = HashMap::new();

            for (topic, partition, leader_epoch) in &partitions {
                let leader_id = self
                    .metadata
                    .leader(topic, *partition)
                    .unwrap_or(fallback_broker_id);
                leader_partitions
                    .entry(leader_id)
                    .or_default()
                    .entry(topic.clone())
                    .or_default()
                    .push(OffsetForLeaderEpochPartition {
                        partition: *partition,
                        current_leader_epoch: -1, // consumer perspective
                        leader_epoch: *leader_epoch,
                    });
            }

            let mut results = Vec::new();
            let mut has_stale_leader = false;

            for (broker_id, topics_map) in leader_partitions {
                let broker = brokers
                    .iter()
                    .find(|b| b.id == broker_id)
                    .unwrap_or(&brokers[0]);
                let conn = self
                    .pool
                    .get_connection_by_id(broker.id, broker.address())
                    .await?;

                let request = OffsetForLeaderEpochRequest {
                    replica_id: -1, // -1 for consumer
                    topics: topics_map
                        .into_iter()
                        .map(|(topic, partitions)| OffsetForLeaderEpochTopic { topic, partitions })
                        .collect(),
                };

                let version = conn
                    .negotiate_api_version(
                        ApiKey::OffsetForLeaderEpoch,
                        versions::OFFSET_FOR_LEADER_EPOCH_MAX,
                        versions::OFFSET_FOR_LEADER_EPOCH_MIN,
                    )
                    .await
                    .ok_or_else(|| {
                        KrafkaError::protocol(
                            "no mutually supported OffsetForLeaderEpoch API version",
                        )
                    })?;

                let response_bytes = conn
                    .send_request(ApiKey::OffsetForLeaderEpoch, version, |buf| {
                        request.encode_versioned(version, buf)
                    })
                    .await?;

                let mut buf = response_bytes;
                let response = OffsetForLeaderEpochResponse::decode_versioned(version, &mut buf)?;

                for topic in response.topics {
                    let topic_name = topic.topic;
                    for partition in topic.partitions {
                        if partition.error_code == crate::error::ErrorCode::NotLeaderForPartition {
                            has_stale_leader = true;
                        }
                        results.push(LeaderEpochResult {
                            topic: topic_name.clone(),
                            partition: partition.partition,
                            leader_epoch: partition.leader_epoch,
                            end_offset: partition.end_offset,
                            error: if partition.error_code.is_ok() {
                                None
                            } else {
                                Some(format!("{:?}", partition.error_code))
                            },
                        });
                    }
                }
            }

            if has_stale_leader && attempt == 0 {
                warn!(
                    "NotLeaderForPartition in OffsetForLeaderEpoch response, retrying with refreshed metadata"
                );
                continue;
            }

            info!(
                "Got leader epoch offsets for {} partition(s)",
                results.len()
            );
            return Ok(results);
        }
        unreachable!()
    }

    // ── Delegation Tokens ────────────────────────────────────────────────

    /// Create a delegation token.
    ///
    /// Delegation tokens allow a principal to delegate authentication to
    /// another principal without sharing credentials (KIP-48). The token
    /// HMAC can be used for SASL/SCRAM authentication.
    ///
    /// # Arguments
    ///
    /// * `renewers` - Principals authorized to renew the token (type, name pairs).
    ///   Pass an empty slice to allow only the token owner to renew.
    /// * `max_lifetime` - Maximum token lifetime. Use `None` for the server
    ///   default (typically 7 days).
    pub async fn create_delegation_token(
        &self,
        renewers: &[(&str, &str)],
        max_lifetime: Option<Duration>,
    ) -> Result<CreateDelegationTokenResult> {
        let conn = self.get_any_broker_connection().await?;

        let request = CreateDelegationTokenRequest {
            renewers: renewers
                .iter()
                .map(|(t, n)| CreatableRenewer {
                    principal_type: t.to_string(),
                    principal_name: n.to_string(),
                })
                .collect(),
            max_lifetime_ms: max_lifetime
                .map(crate::util::duration_to_millis_i64)
                .unwrap_or(-1),
            owner_principal_type: None,
            owner_principal_name: None,
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::CreateDelegationToken,
                versions::CREATE_DELEGATION_TOKEN_MAX,
                versions::CREATE_DELEGATION_TOKEN_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported CreateDelegationToken API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::CreateDelegationToken, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = CreateDelegationTokenResponse::decode_versioned(version, &mut buf)?;

        let result = if response.error_code.is_ok() {
            info!("Created delegation token");
            CreateDelegationTokenResult {
                token: Some(DelegationToken {
                    principal_type: response.principal_type,
                    principal_name: response.principal_name,
                    issue_timestamp_ms: response.issue_timestamp_ms,
                    expiry_timestamp_ms: response.expiry_timestamp_ms,
                    max_timestamp_ms: response.max_timestamp_ms,
                    token_id: response.token_id,
                    hmac: response.hmac,
                    renewers: Vec::new(),
                }),
                error: None,
            }
        } else {
            CreateDelegationTokenResult {
                token: None,
                error: Some(format!("{:?}", response.error_code)),
            }
        };

        Ok(result)
    }

    /// Renew a delegation token, extending its expiry time.
    ///
    /// # Arguments
    ///
    /// * `hmac` - HMAC of the token to renew (from [`DelegationToken::hmac`]).
    /// * `renew_period` - How long to extend the token's lifetime.
    pub async fn renew_delegation_token(
        &self,
        hmac: &[u8],
        renew_period: Duration,
    ) -> Result<RenewDelegationTokenResult> {
        let conn = self.get_any_broker_connection().await?;

        let request = RenewDelegationTokenRequest {
            hmac: Bytes::copy_from_slice(hmac),
            renew_period_ms: crate::util::duration_to_millis_i64(renew_period),
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::RenewDelegationToken,
                versions::RENEW_DELEGATION_TOKEN_MAX,
                versions::RENEW_DELEGATION_TOKEN_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported RenewDelegationToken API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::RenewDelegationToken, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = RenewDelegationTokenResponse::decode_versioned(version, &mut buf)?;

        if response.error_code.is_ok() {
            info!("Renewed delegation token");
        }

        Ok(RenewDelegationTokenResult {
            expiry_timestamp_ms: response.expiry_timestamp_ms,
            error: if response.error_code.is_ok() {
                None
            } else {
                Some(format!("{:?}", response.error_code))
            },
        })
    }

    /// Expire a delegation token, revoking it before its natural expiry.
    ///
    /// # Arguments
    ///
    /// * `hmac` - HMAC of the token to expire (from [`DelegationToken::hmac`]).
    /// * `expiry_period` - How long until the token expires. Pass `None` to
    ///   expire the token immediately (sends `-1` to the broker).
    pub async fn expire_delegation_token(
        &self,
        hmac: &[u8],
        expiry_period: Option<Duration>,
    ) -> Result<ExpireDelegationTokenResult> {
        let conn = self.get_any_broker_connection().await?;

        let request = ExpireDelegationTokenRequest {
            hmac: Bytes::copy_from_slice(hmac),
            expiry_period_ms: expiry_period
                .map(crate::util::duration_to_millis_i64)
                .unwrap_or(-1),
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::ExpireDelegationToken,
                versions::EXPIRE_DELEGATION_TOKEN_MAX,
                versions::EXPIRE_DELEGATION_TOKEN_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported ExpireDelegationToken API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::ExpireDelegationToken, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = ExpireDelegationTokenResponse::decode_versioned(version, &mut buf)?;

        if response.error_code.is_ok() {
            info!("Expired delegation token");
        }

        Ok(ExpireDelegationTokenResult {
            expiry_timestamp_ms: response.expiry_timestamp_ms,
            error: if response.error_code.is_ok() {
                None
            } else {
                Some(format!("{:?}", response.error_code))
            },
        })
    }

    /// Describe delegation tokens visible to the caller.
    ///
    /// # Arguments
    ///
    /// * `owners` - Filter by token owners (type, name pairs). Pass `None`
    ///   to return all tokens visible to the caller.
    pub async fn describe_delegation_token(
        &self,
        owners: Option<&[(&str, &str)]>,
    ) -> Result<Vec<DelegationToken>> {
        let conn = self.get_any_broker_connection().await?;

        let request = DescribeDelegationTokenRequest {
            owners: owners.map(|o| {
                o.iter()
                    .map(|(t, n)| DescribeDelegationTokenOwner {
                        principal_type: t.to_string(),
                        principal_name: n.to_string(),
                    })
                    .collect()
            }),
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::DescribeDelegationToken,
                versions::DESCRIBE_DELEGATION_TOKEN_MAX,
                versions::DESCRIBE_DELEGATION_TOKEN_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported DescribeDelegationToken API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DescribeDelegationToken, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DescribeDelegationTokenResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            return Err(KrafkaError::broker(
                response.error_code,
                "DescribeDelegationToken failed",
            ));
        }

        let tokens: Vec<DelegationToken> = response
            .tokens
            .into_iter()
            .map(|t| DelegationToken {
                principal_type: t.principal_type,
                principal_name: t.principal_name,
                issue_timestamp_ms: t.issue_timestamp_ms,
                expiry_timestamp_ms: t.expiry_timestamp_ms,
                max_timestamp_ms: t.max_timestamp_ms,
                token_id: t.token_id,
                hmac: t.hmac,
                renewers: t
                    .renewers
                    .into_iter()
                    .map(|r| DelegationTokenRenewer {
                        principal_type: r.principal_type,
                        principal_name: r.principal_name,
                    })
                    .collect(),
            })
            .collect();

        info!("Described {} delegation token(s)", tokens.len());
        Ok(tokens)
    }

    // ── Client Quotas ────────────────────────────────────────────────────

    /// Describe client quotas matching the given filter.
    ///
    /// # Arguments
    ///
    /// * `components` - Filter components. Each component specifies an entity
    ///   type and match criteria. The broker returns entities matching **all**
    ///   components.
    /// * `strict` - If `true`, exclude entities with unspecified entity types
    ///   (i.e., only return entities that exactly match all given component types).
    ///
    /// # Filter Match Types
    ///
    /// Each component has a `match_type`:
    /// - `0` (exact): match the entity with the given name
    /// - `1` (default): match the default entity for this type
    /// - `2` (any specified): match any entity with a name (non-default)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// // Describe all quotas for user "alice"
    /// let results = admin.describe_client_quotas(
    ///     &[("user", 0, Some("alice"))],
    ///     false,
    /// ).await?;
    /// ```
    pub async fn describe_client_quotas(
        &self,
        components: &[(&str, i8, Option<&str>)],
        strict: bool,
    ) -> Result<DescribeClientQuotasResult> {
        let conn = self.get_any_broker_connection().await?;

        let request = DescribeClientQuotasRequest {
            components: components
                .iter()
                .map(
                    |(entity_type, match_type, match_value)| QuotaFilterComponent {
                        entity_type: entity_type.to_string(),
                        match_type: *match_type,
                        match_value: match_value.map(|v| v.to_string()),
                    },
                )
                .collect(),
            strict,
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::DescribeClientQuotas,
                versions::DESCRIBE_CLIENT_QUOTAS_MAX,
                versions::DESCRIBE_CLIENT_QUOTAS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported DescribeClientQuotas API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DescribeClientQuotas, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DescribeClientQuotasResponse::decode_versioned(version, &mut buf)?;

        let entries = response
            .entries
            .unwrap_or_default()
            .into_iter()
            .map(|entry| QuotaDescription {
                entity: entry
                    .entity
                    .into_iter()
                    .map(|e| QuotaEntityComponent {
                        entity_type: e.entity_type,
                        entity_name: e.entity_name,
                    })
                    .collect(),
                values: entry
                    .values
                    .into_iter()
                    .map(|v| QuotaConfig {
                        key: v.key,
                        value: v.value,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        if response.error_code.is_ok() {
            info!("Described {} client quota entry(ies)", entries.len());
        }

        Ok(DescribeClientQuotasResult {
            entries,
            error: if response.error_code.is_ok() {
                None
            } else {
                let msg = response
                    .error_message
                    .unwrap_or_else(|| format!("{:?}", response.error_code));
                Some(msg)
            },
        })
    }

    /// Alter client quotas.
    ///
    /// Each entry specifies an entity (user, client-id, ip) and a set of
    /// quota operations (set or remove). Results are returned per-entity.
    ///
    /// # Arguments
    ///
    /// * `entries` - Quota alterations. Each entry has an entity and operations.
    /// * `validate_only` - If `true`, validate the request without applying changes.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use krafka::admin::QuotaAlteration;
    ///
    /// // Set producer byte rate quota for user "alice"
    /// let results = admin.alter_client_quotas(
    ///     &[QuotaAlteration {
    ///         entity: vec![("user", Some("alice"))],
    ///         ops: vec![("producer_byte_rate", Some(1_048_576.0))],
    ///     }],
    ///     false,
    /// ).await?;
    /// ```
    pub async fn alter_client_quotas(
        &self,
        entries: &[QuotaAlteration<'_>],
        validate_only: bool,
    ) -> Result<Vec<AlterClientQuotaResult>> {
        let conn = self.get_any_broker_connection().await?;

        let request = AlterClientQuotasRequest {
            entries: entries
                .iter()
                .map(|e| AlterQuotaEntry {
                    entity: e
                        .entity
                        .iter()
                        .map(|(t, n)| AlterQuotaEntity {
                            entity_type: t.to_string(),
                            entity_name: n.map(|v| v.to_string()),
                        })
                        .collect(),
                    ops: e
                        .ops
                        .iter()
                        .map(|(key, value)| AlterQuotaOp {
                            key: key.to_string(),
                            value: value.unwrap_or(0.0),
                            remove: value.is_none(),
                        })
                        .collect(),
                })
                .collect(),
            validate_only,
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::AlterClientQuotas,
                versions::ALTER_CLIENT_QUOTAS_MAX,
                versions::ALTER_CLIENT_QUOTAS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported AlterClientQuotas API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::AlterClientQuotas, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = AlterClientQuotasResponse::decode_versioned(version, &mut buf)?;

        let results: Vec<AlterClientQuotaResult> = response
            .entries
            .into_iter()
            .map(|entry| AlterClientQuotaResult {
                entity: entry
                    .entity
                    .into_iter()
                    .map(|e| QuotaEntityComponent {
                        entity_type: e.entity_type,
                        entity_name: e.entity_name,
                    })
                    .collect(),
                error: if entry.error_code.is_ok() {
                    None
                } else {
                    let msg = entry
                        .error_message
                        .unwrap_or_else(|| format!("{:?}", entry.error_code));
                    Some(msg)
                },
            })
            .collect();

        info!("Altered {} client quota entry(ies)", results.len());
        Ok(results)
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
                KrafkaError::protocol("no mutually supported DeleteGroups API version")
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
        let conn = self.get_any_broker_connection().await?;

        let version = conn
            .negotiate_api_version(
                ApiKey::DescribeTopicPartitions,
                versions::DESCRIBE_TOPIC_PARTITIONS_MAX,
                versions::DESCRIBE_TOPIC_PARTITIONS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported DescribeTopicPartitions API version")
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

    /// Describe broker-supported and cluster-finalized features (KIP-584).
    ///
    /// Sends an `ApiVersions` request (v3+) to any broker and extracts the
    /// feature information from the tagged fields. The response includes:
    /// - Features supported by the responding broker (per-broker)
    /// - Cluster-wide finalized features and their epoch (cluster-wide)
    ///
    /// # Example
    /// ```ignore
    /// let features = admin.describe_features().await?;
    /// for f in &features.supported_features {
    ///     println!("{}: v{}–v{}", f.name, f.min_version, f.max_version);
    /// }
    /// for f in &features.finalized_features {
    ///     println!("{}: v{}–v{} (finalized)", f.name, f.min_version_level, f.max_version_level);
    /// }
    /// ```
    pub async fn describe_features(&self) -> Result<DescribeFeaturesResult> {
        self.check_not_closed()?;
        let conn = self.get_any_broker_connection().await?;

        let request = crate::protocol::ApiVersionsRequest::new()
            .with_client_software("krafka", env!("CARGO_PKG_VERSION"));

        let version = conn
            .negotiate_api_version(
                ApiKey::ApiVersions,
                versions::API_VERSIONS_MAX,
                // Need v3+ for tagged feature fields
                3,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol(
                    "no mutually supported ApiVersions v3+; feature discovery requires v3+",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::ApiVersions, version, |buf| {
                if version >= 5 {
                    request.encode_v5(buf)
                } else {
                    request.encode_v3(buf)
                }
            })
            .await?;

        let mut buf = response_bytes;
        let response = crate::protocol::ApiVersionsResponse::decode_v3(&mut buf)?;

        if response.error_code != 0 {
            return Err(KrafkaError::broker(
                crate::error::ErrorCode::from(response.error_code),
                "ApiVersions request failed",
            ));
        }

        Ok(DescribeFeaturesResult {
            supported_features: response.supported_features,
            finalized_features: response.finalized_features,
            finalized_features_epoch: response.finalized_features_epoch,
        })
    }

    /// Update cluster-wide finalized feature version levels (KIP-584).
    ///
    /// This is a **destructive** operation — downgrades and deletions can be
    /// data-lossy. Only the controller broker serves this request; the client
    /// sends to any broker, which forwards to the controller.
    ///
    /// Requires `ALTER` permission on the cluster.
    ///
    /// # Example
    /// ```ignore
    /// use krafka::protocol::FeatureUpdateKey;
    ///
    /// let results = admin.update_features(
    ///     vec![FeatureUpdateKey::upgrade("metadata.version", 17)],
    ///     false, // validate_only
    /// ).await?;
    ///
    /// for result in &results.results {
    ///     if let Some(e) = &result.error {
    ///         eprintln!("Failed to update {}: {e}", result.feature);
    ///     }
    /// }
    /// ```
    pub async fn update_features(
        &self,
        feature_updates: Vec<crate::protocol::FeatureUpdateKey>,
        validate_only: bool,
    ) -> Result<UpdateFeaturesResult> {
        self.check_not_closed()?;
        let conn = self.get_any_broker_connection().await?;

        let request = UpdateFeaturesRequest::new(feature_updates).with_validate_only(validate_only);

        let version = conn
            .negotiate_api_version(
                ApiKey::UpdateFeatures,
                versions::UPDATE_FEATURES_MAX,
                versions::UPDATE_FEATURES_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported UpdateFeatures API version")
            })?;

        // validate_only requires v1+; reject early to avoid silently applying changes
        if validate_only && version < 1 {
            return Err(KrafkaError::protocol(
                "validate_only requires UpdateFeatures v1+, but broker only supports v0",
            ));
        }

        let response_bytes = conn
            .send_request(ApiKey::UpdateFeatures, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = UpdateFeaturesResponse::decode_versioned(version, &mut buf)?;

        if !response.is_ok() {
            let msg = response
                .error_message
                .unwrap_or_else(|| format!("{:?}", response.error_code));
            return Err(KrafkaError::protocol(msg));
        }

        Ok(UpdateFeaturesResult {
            results: response
                .results
                .into_iter()
                .map(|r| UpdateFeatureResult {
                    feature: r.feature,
                    error: if r.error_code.is_ok() {
                        None
                    } else {
                        Some(
                            r.error_message
                                .unwrap_or_else(|| format!("{:?}", r.error_code)),
                        )
                    },
                })
                .collect(),
        })
    }

    /// Describe log directories on all known brokers.
    ///
    /// Each broker maintains one or more log directories; this method queries
    /// every broker and returns per-directory information including sizes,
    /// partition assignments, and (v4+) volume capacity.
    ///
    /// Pass `None` for `topics` to describe **all** partitions on every
    /// broker, or pass a list of [`DescribableLogDirTopic`] to filter.
    ///
    /// # Example
    /// ```ignore
    /// // Describe all log dirs on every broker
    /// let dirs = admin.describe_log_dirs(None).await?;
    /// for dir in &dirs {
    ///     println!("broker {} dir {}: {:?}", dir.broker_id, dir.log_dir, dir.error);
    /// }
    ///
    /// // Describe specific topic partitions
    /// use krafka::protocol::DescribableLogDirTopic;
    /// let filter = vec![DescribableLogDirTopic {
    ///     topic: "my-topic".into(),
    ///     partitions: vec![0, 1, 2],
    /// }];
    /// let dirs = admin.describe_log_dirs(Some(filter)).await?;
    /// ```
    pub async fn describe_log_dirs(
        &self,
        topics: Option<Vec<DescribableLogDirTopic>>,
    ) -> Result<Vec<LogDirInfo>> {
        self.check_not_closed()?;
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::broker(
                crate::error::ErrorCode::UnknownServerError,
                "no brokers available",
            ));
        }

        let topic_scope = match &topics {
            None => "all".to_string(),
            Some(t) => format!("{} topic(s)", t.len()),
        };

        let request = match &topics {
            None => DescribeLogDirsRequest::all(),
            Some(t) => DescribeLogDirsRequest::for_topics(t.clone()),
        };

        let mut all_dirs = Vec::new();

        for broker in &brokers {
            let conn = self
                .pool
                .get_connection_by_id(broker.id, broker.address())
                .await?;

            let version = conn
                .negotiate_api_version(
                    ApiKey::DescribeLogDirs,
                    versions::DESCRIBE_LOG_DIRS_MAX,
                    versions::DESCRIBE_LOG_DIRS_MIN,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol("no mutually supported DescribeLogDirs API version")
                })?;

            let response_bytes = match conn
                .send_request(ApiKey::DescribeLogDirs, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await
            {
                Ok(bytes) => bytes,
                Err(e) => {
                    warn!(
                        "DescribeLogDirs request failed on broker {} ({}): {}",
                        broker.id, topic_scope, e
                    );
                    continue;
                }
            };

            let mut buf = response_bytes;
            let response = match DescribeLogDirsResponse::decode_versioned(version, &mut buf) {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "DescribeLogDirs decode failed on broker {} ({}): {}",
                        broker.id, topic_scope, e
                    );
                    continue;
                }
            };

            // v3+ top-level error code
            if !response.error_code.is_ok() {
                warn!(
                    "DescribeLogDirs top-level error on broker {} ({}): {:?}",
                    broker.id, topic_scope, response.error_code
                );
            }

            // Pre-v3 brokers lack a top-level error code; empty results typically
            // signal CLUSTER_AUTHORIZATION_FAILED (matches Java client heuristic).
            if response.results.is_empty() && version < 3 {
                warn!(
                    "DescribeLogDirs returned empty results on broker {} (v{}, {}); \
                     likely CLUSTER_AUTHORIZATION_FAILED",
                    broker.id, version, topic_scope
                );
            }

            for result in response.results {
                all_dirs.push(LogDirInfo {
                    broker_id: broker.id,
                    log_dir: result.log_dir,
                    error: if result.error_code.is_ok() {
                        None
                    } else {
                        Some(format!("{:?}", result.error_code))
                    },
                    topics: result
                        .topics
                        .into_iter()
                        .map(|t| LogDirTopicInfo {
                            name: t.name,
                            partitions: t
                                .partitions
                                .into_iter()
                                .map(|p| LogDirPartitionInfo {
                                    partition_index: p.partition_index,
                                    partition_size: p.partition_size,
                                    offset_lag: p.offset_lag,
                                    is_future_key: p.is_future_key,
                                })
                                .collect(),
                        })
                        .collect(),
                    total_bytes: result.total_bytes,
                    usable_bytes: result.usable_bytes,
                });
            }
        }

        info!(
            "Described {} log dir(s) across {} broker(s)",
            all_dirs.len(),
            brokers.len()
        );
        Ok(all_dirs)
    }

    /// Trigger leader election for the specified partitions.
    ///
    /// When `topic_partitions` is `None`, leaders for all partitions are
    /// elected. The `election_type` controls whether to perform a preferred
    /// or unclean leader election (requires broker v1+; v0 always does
    /// preferred election).
    ///
    /// Returns per-partition results — individual partitions may fail even
    /// when the RPC succeeds.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use krafka::protocol::ElectionType;
    /// use std::time::Duration;
    ///
    /// // Preferred election for all partitions
    /// let results = admin
    ///     .elect_leaders(ElectionType::Preferred, None, Duration::from_secs(60))
    ///     .await?;
    /// ```
    pub async fn elect_leaders(
        &self,
        election_type: ElectionType,
        topic_partitions: Option<Vec<ElectLeadersTopicPartitions>>,
        timeout: Duration,
    ) -> Result<Vec<ElectLeadersResult>> {
        let conn = self.get_any_broker_connection().await?;

        let request = ElectLeadersRequest {
            election_type,
            topic_partitions,
            timeout_ms: crate::util::duration_to_millis_i32(timeout),
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::ElectLeaders,
                versions::ELECT_LEADERS_MAX,
                versions::ELECT_LEADERS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported ElectLeaders API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::ElectLeaders, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = ElectLeadersResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            warn!("ElectLeaders top-level error: {:?}", response.error_code);
        }

        let results = response
            .replica_election_results
            .into_iter()
            .map(|topic| ElectLeadersResult {
                topic: topic.topic,
                partitions: topic
                    .partition_results
                    .into_iter()
                    .map(|p| ElectLeadersPartitionInfo {
                        partition_id: p.partition_id,
                        error: if p.error_code.is_ok() {
                            None
                        } else {
                            Some(
                                p.error_message
                                    .unwrap_or_else(|| format!("{:?}", p.error_code)),
                            )
                        },
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        info!("ElectLeaders completed for {} topic(s)", results.len());
        Ok(results)
    }

    /// Alter partition reassignments.
    ///
    /// Initiates or cancels partition reassignments. To cancel a pending
    /// reassignment, set `replicas` to `None` for that partition.
    ///
    /// **This is a destructive operation** — reassigning partitions moves data
    /// between brokers and can significantly impact cluster load.
    ///
    /// Returns per-partition results — individual partitions may fail even
    /// when the RPC succeeds.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use krafka::protocol::{ReassignableTopic, ReassignablePartition};
    /// use std::time::Duration;
    ///
    /// let results = admin.alter_partition_reassignments(
    ///     vec![ReassignableTopic {
    ///         name: "my-topic".into(),
    ///         partitions: vec![ReassignablePartition {
    ///             partition_index: 0,
    ///             replicas: Some(vec![1, 2, 3]),
    ///         }],
    ///     }],
    ///     Duration::from_secs(60),
    /// ).await?;
    /// ```
    pub async fn alter_partition_reassignments(
        &self,
        topics: Vec<ReassignableTopic>,
        timeout: Duration,
    ) -> Result<AlterReassignmentsResult> {
        let conn = self.get_any_broker_connection().await?;

        let request = AlterPartitionReassignmentsRequest {
            timeout_ms: crate::util::duration_to_millis_i32(timeout),
            topics,
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::AlterPartitionReassignments,
                versions::ALTER_PARTITION_REASSIGNMENTS_MAX,
                versions::ALTER_PARTITION_REASSIGNMENTS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol(
                    "no mutually supported AlterPartitionReassignments API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::AlterPartitionReassignments, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = AlterPartitionReassignmentsResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            warn!(
                "AlterPartitionReassignments top-level error: {:?} — {}",
                response.error_code,
                response.error_message.as_deref().unwrap_or("(no message)")
            );
        }

        let topic_results = response
            .responses
            .into_iter()
            .map(|t| ReassignmentTopicResult {
                name: t.name,
                partitions: t
                    .partitions
                    .into_iter()
                    .map(|p| ReassignmentPartitionResult {
                        partition_index: p.partition_index,
                        error: if p.error_code.is_ok() {
                            None
                        } else {
                            Some(
                                p.error_message
                                    .unwrap_or_else(|| format!("{:?}", p.error_code)),
                            )
                        },
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        info!(
            "AlterPartitionReassignments completed for {} topic(s)",
            topic_results.len()
        );

        Ok(AlterReassignmentsResult {
            error: if response.error_code.is_ok() {
                None
            } else {
                Some(
                    response
                        .error_message
                        .unwrap_or_else(|| format!("{:?}", response.error_code)),
                )
            },
            topics: topic_results,
        })
    }

    /// List ongoing partition reassignments.
    ///
    /// When `topics` is `None`, all ongoing reassignments are listed.
    /// Otherwise, only the specified topic-partitions are checked.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // List all ongoing reassignments
    /// let reassignments = admin
    ///     .list_partition_reassignments(None, Duration::from_secs(60))
    ///     .await?;
    /// for topic in &reassignments {
    ///     for p in &topic.partitions {
    ///         println!("{} p{}: adding {:?}, removing {:?}",
    ///             topic.name, p.partition_index, p.adding_replicas, p.removing_replicas);
    ///     }
    /// }
    /// ```
    pub async fn list_partition_reassignments(
        &self,
        topics: Option<Vec<ListPartitionReassignmentsTopic>>,
        timeout: Duration,
    ) -> Result<Vec<PartitionReassignmentInfo>> {
        let conn = self.get_any_broker_connection().await?;

        let request = ListPartitionReassignmentsRequest {
            timeout_ms: crate::util::duration_to_millis_i32(timeout),
            topics,
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::ListPartitionReassignments,
                versions::LIST_PARTITION_REASSIGNMENTS_MAX,
                versions::LIST_PARTITION_REASSIGNMENTS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol(
                    "no mutually supported ListPartitionReassignments API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::ListPartitionReassignments, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = ListPartitionReassignmentsResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            warn!(
                "ListPartitionReassignments top-level error: {:?} — {}",
                response.error_code,
                response.error_message.as_deref().unwrap_or("(no message)")
            );
        }

        let results = response
            .topics
            .into_iter()
            .map(|t| PartitionReassignmentInfo {
                name: t.name,
                partitions: t
                    .partitions
                    .into_iter()
                    .map(|p| PartitionReassignmentPartitionInfo {
                        partition_index: p.partition_index,
                        replicas: p.replicas,
                        adding_replicas: p.adding_replicas,
                        removing_replicas: p.removing_replicas,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        info!(
            "Listed {} topic(s) with ongoing reassignments",
            results.len()
        );
        Ok(results)
    }

    // ════════════════════════════════════════════════════════════════════
    // AlterReplicaLogDirs (API key 34)
    // ════════════════════════════════════════════════════════════════════

    /// Move partition replicas to a different log directory on the broker.
    ///
    /// **This is a destructive operation** — moving replicas between log
    /// directories triggers data copying and can impact broker I/O.
    ///
    /// This is a per-broker operation. The request is sent to every broker
    /// that currently hosts at least one replica for the specified
    /// topic-partitions.
    ///
    /// Returns per-partition results — individual partitions may fail even
    /// when the RPC succeeds.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use krafka::protocol::{AlterReplicaLogDir, AlterReplicaLogDirTopic};
    ///
    /// let results = admin.alter_replica_log_dirs(vec![
    ///     AlterReplicaLogDir {
    ///         path: "/data/kafka-logs-2".into(),
    ///         topics: vec![AlterReplicaLogDirTopic {
    ///             name: "my-topic".into(),
    ///             partitions: vec![0, 1],
    ///         }],
    ///     },
    /// ]).await?;
    /// ```
    pub async fn alter_replica_log_dirs(
        &self,
        dirs: Vec<AlterReplicaLogDir>,
    ) -> Result<Vec<AlterReplicaLogDirsResult>> {
        self.check_not_closed()?;
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::broker(
                crate::error::ErrorCode::UnknownServerError,
                "no brokers available",
            ));
        }

        let request = AlterReplicaLogDirsRequest { dirs };
        let mut all_results = Vec::new();

        for broker in &brokers {
            let conn = self
                .pool
                .get_connection_by_id(broker.id, broker.address())
                .await?;

            let version = conn
                .negotiate_api_version(
                    ApiKey::AlterReplicaLogDirs,
                    versions::ALTER_REPLICA_LOG_DIRS_MAX,
                    versions::ALTER_REPLICA_LOG_DIRS_MIN,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol("no mutually supported AlterReplicaLogDirs API version")
                })?;

            let response_bytes = match conn
                .send_request(ApiKey::AlterReplicaLogDirs, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await
            {
                Ok(bytes) => bytes,
                Err(e) => {
                    warn!(
                        "AlterReplicaLogDirs request failed on broker {} ({} dir(s)): {}",
                        broker.id,
                        request.dirs.len(),
                        e
                    );
                    continue;
                }
            };

            let mut buf = response_bytes;
            let response = match AlterReplicaLogDirsResponse::decode_versioned(version, &mut buf) {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "AlterReplicaLogDirs decode failed on broker {} ({} dir(s)): {}",
                        broker.id,
                        request.dirs.len(),
                        e
                    );
                    continue;
                }
            };

            for topic in response.results {
                all_results.push(AlterReplicaLogDirsResult {
                    broker_id: broker.id,
                    topic_name: topic.topic_name,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(|p| AlterReplicaLogDirsPartitionResult {
                            partition_index: p.partition_index,
                            error: if p.error_code.is_ok() {
                                None
                            } else {
                                Some(format!("{:?}", p.error_code))
                            },
                        })
                        .collect(),
                });
            }
        }

        info!(
            "AlterReplicaLogDirs completed for {} topic(s)",
            all_results.len()
        );
        Ok(all_results)
    }

    // ════════════════════════════════════════════════════════════════════
    // OffsetDelete (API key 47)
    // ════════════════════════════════════════════════════════════════════

    /// Delete committed offsets for a consumer group.
    ///
    /// **This is a destructive operation** — deleted offsets cannot be
    /// recovered. The consumer group must be in the `Empty` state.
    ///
    /// The request is sent to the group coordinator.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = admin.delete_offsets(
    ///     "my-group",
    ///     &[("my-topic", &[0, 1, 2])],
    /// ).await?;
    /// ```
    pub async fn delete_consumer_group_offsets(
        &self,
        group_id: &str,
        topic_partitions: &[(&str, &[i32])],
    ) -> Result<OffsetDeleteResult> {
        self.check_not_closed()?;

        // Find the group coordinator.
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
                KrafkaError::protocol("no mutually supported FindCoordinator API version")
            })?;
        let coord_response_bytes = any_conn
            .send_request(ApiKey::FindCoordinator, coord_version, |buf| {
                coord_request.encode_versioned(coord_version, buf)
            })
            .await?;
        let mut coord_buf = coord_response_bytes;
        let coord_response =
            FindCoordinatorResponse::decode_versioned(coord_version, &mut coord_buf)?;

        let coordinator = if coord_response.error_code.is_ok() {
            let addr = format!("{}:{}", coord_response.host, coord_response.port);
            self.pool
                .get_connection_by_id(coord_response.node_id, &addr)
                .await?
        } else {
            warn!(
                "FindCoordinator failed for group '{}': {:?}, using any broker",
                group_id, coord_response.error_code
            );
            any_conn
        };

        let topics = topic_partitions
            .iter()
            .map(|(name, partitions)| OffsetDeleteTopicRequest {
                name: (*name).to_string(),
                partitions: partitions
                    .iter()
                    .map(|&p| OffsetDeletePartitionRequest { partition_index: p })
                    .collect(),
            })
            .collect();

        let request = OffsetDeleteRequest {
            group_id: group_id.to_string(),
            topics,
        };

        let version = coordinator
            .negotiate_api_version(
                ApiKey::OffsetDelete,
                versions::OFFSET_DELETE_MAX,
                versions::OFFSET_DELETE_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported OffsetDelete API version")
            })?;

        let response_bytes = coordinator
            .send_request(ApiKey::OffsetDelete, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = OffsetDeleteResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            warn!("OffsetDelete top-level error: {:?}", response.error_code);
        }

        let topics = response
            .topics
            .into_iter()
            .map(|t| OffsetDeleteTopicResult {
                name: t.name,
                partitions: t
                    .partitions
                    .into_iter()
                    .map(|p| OffsetDeletePartitionResult {
                        partition_index: p.partition_index,
                        error: if p.error_code.is_ok() {
                            None
                        } else {
                            Some(format!("{:?}", p.error_code))
                        },
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        info!("OffsetDelete completed for group {group_id}");

        Ok(OffsetDeleteResult {
            error: if response.error_code.is_ok() {
                None
            } else {
                Some(format!("{:?}", response.error_code))
            },
            topics,
        })
    }

    // ════════════════════════════════════════════════════════════════════
    // DescribeUserScramCredentials (API key 50)
    // ════════════════════════════════════════════════════════════════════

    /// Describe SCRAM credentials for the specified users.
    ///
    /// When `users` is `None`, all SCRAM credentials are described.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // Describe all SCRAM credentials
    /// let results = admin.describe_user_scram_credentials(None).await?;
    /// for user in &results {
    ///     println!("{}: {:?}", user.name, user.credential_infos);
    /// }
    /// ```
    pub async fn describe_user_scram_credentials(
        &self,
        users: Option<Vec<String>>,
    ) -> Result<DescribeUserScramCredentialsResult> {
        let conn = self.get_any_broker_connection().await?;

        let request = DescribeUserScramCredentialsRequest { users };

        let version = conn
            .negotiate_api_version(
                ApiKey::DescribeUserScramCredentials,
                versions::DESCRIBE_USER_SCRAM_CREDENTIALS_MAX,
                versions::DESCRIBE_USER_SCRAM_CREDENTIALS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol(
                    "no mutually supported DescribeUserScramCredentials API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DescribeUserScramCredentials, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DescribeUserScramCredentialsResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            warn!(
                "DescribeUserScramCredentials top-level error: {:?} — {}",
                response.error_code,
                response.error_message.as_deref().unwrap_or("(no message)")
            );
        }

        let users = response
            .results
            .into_iter()
            .map(|r| ScramCredentialUserResult {
                name: r.user,
                error: if r.error_code.is_ok() {
                    None
                } else {
                    r.error_message
                        .or_else(|| Some(format!("{:?}", r.error_code)))
                },
                credential_infos: r
                    .credential_infos
                    .into_iter()
                    .map(|c| ScramCredentialInfoResult {
                        mechanism: c.mechanism,
                        iterations: c.iterations,
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        info!(
            "DescribeUserScramCredentials returned {} user(s)",
            users.len()
        );

        Ok(DescribeUserScramCredentialsResult {
            error: if response.error_code.is_ok() {
                None
            } else {
                response
                    .error_message
                    .or_else(|| Some(format!("{:?}", response.error_code)))
            },
            users,
        })
    }

    // ════════════════════════════════════════════════════════════════════
    // AlterUserScramCredentials (API key 51)
    // ════════════════════════════════════════════════════════════════════

    /// Alter (upsert or delete) SCRAM credentials for users.
    ///
    /// **This is a destructive operation** — deleting a SCRAM credential
    /// removes the user's ability to authenticate with that mechanism.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use krafka::protocol::{ScramCredentialDeletion, ScramCredentialUpsertion};
    /// use krafka::auth::ScramMechanism;
    /// use zeroize::Zeroizing;
    ///
    /// let results = admin.alter_user_scram_credentials(
    ///     vec![ScramCredentialDeletion {
    ///         name: "alice".into(),
    ///         mechanism: ScramMechanism::Sha512,
    ///     }],
    ///     vec![ScramCredentialUpsertion {
    ///         name: "bob".into(),
    ///         mechanism: ScramMechanism::Sha256,
    ///         iterations: 8192,
    ///         salt: Zeroizing::new(vec![1, 2, 3]),
    ///         salted_password: Zeroizing::new(vec![4, 5, 6]),
    ///     }],
    /// ).await?;
    /// ```
    pub async fn alter_user_scram_credentials(
        &self,
        deletions: Vec<ScramCredentialDeletion>,
        upsertions: Vec<ScramCredentialUpsertion>,
    ) -> Result<Vec<AlterScramCredentialResult>> {
        let conn = self.get_any_broker_connection().await?;

        let request = AlterUserScramCredentialsRequest {
            deletions,
            upsertions,
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::AlterUserScramCredentials,
                versions::ALTER_USER_SCRAM_CREDENTIALS_MAX,
                versions::ALTER_USER_SCRAM_CREDENTIALS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported AlterUserScramCredentials API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::AlterUserScramCredentials, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = AlterUserScramCredentialsResponse::decode_versioned(version, &mut buf)?;

        let results = response
            .results
            .into_iter()
            .map(|r| AlterScramCredentialResult {
                user: r.user,
                error: if r.error_code.is_ok() {
                    None
                } else {
                    r.error_message
                        .or_else(|| Some(format!("{:?}", r.error_code)))
                },
            })
            .collect::<Vec<_>>();

        info!(
            "AlterUserScramCredentials completed for {} user(s)",
            results.len()
        );
        Ok(results)
    }

    // ════════════════════════════════════════════════════════════════════
    // DescribeProducers (API key 61)
    // ════════════════════════════════════════════════════════════════════

    /// Describe active producers on the given topic-partitions.
    ///
    /// Routes each topic-partition to its leader broker via cached metadata
    /// for optimal performance. Falls back to any broker if the leader is
    /// unknown.
    ///
    /// Returns per-partition producer state useful for debugging
    /// transactional and idempotent producers.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = admin
    ///     .describe_producers(&[("my-topic", &[0, 1])])
    ///     .await?;
    /// ```
    pub async fn describe_producers(
        &self,
        topic_partitions: &[(&str, &[i32])],
    ) -> Result<Vec<DescribeProducersTopicResult>> {
        self.check_not_closed()?;

        for attempt in 0u8..2 {
            if attempt == 1 {
                // Refresh metadata after a stale-leader error.
                let topics: Vec<&str> = topic_partitions.iter().map(|&(t, _)| t).collect();
                let _ = self.metadata.refresh_for_topics(Some(&topics)).await;
            }

            let brokers = self.metadata.brokers();
            if brokers.is_empty() {
                return Err(KrafkaError::broker(
                    crate::error::ErrorCode::UnknownServerError,
                    "no brokers available",
                ));
            }

            let fallback_id = brokers[0].id;

            // Group topic-partitions by leader broker.
            let mut by_leader: HashMap<i32, HashMap<String, Vec<i32>>> = HashMap::new();
            for &(topic, partitions) in topic_partitions {
                for &pid in partitions {
                    let leader = self.metadata.leader(topic, pid).unwrap_or(fallback_id);
                    by_leader
                        .entry(leader)
                        .or_default()
                        .entry(topic.to_string())
                        .or_default()
                        .push(pid);
                }
            }

            let mut all_results: HashMap<String, DescribeProducersTopicResult> = HashMap::new();
            let mut has_stale_leader = false;

            for (broker_id, topic_map) in by_leader {
                let broker = brokers
                    .iter()
                    .find(|b| b.id == broker_id)
                    .unwrap_or(&brokers[0]);
                let conn = self
                    .pool
                    .get_connection_by_id(broker.id, broker.address())
                    .await?;

                let topics = topic_map
                    .into_iter()
                    .map(|(name, partition_indexes)| DescribeProducersTopicRequest {
                        name,
                        partition_indexes,
                    })
                    .collect();

                let request = DescribeProducersRequest { topics };

                let version = conn
                    .negotiate_api_version(
                        ApiKey::DescribeProducers,
                        versions::DESCRIBE_PRODUCERS_MAX,
                        versions::DESCRIBE_PRODUCERS_MIN,
                    )
                    .await
                    .ok_or_else(|| {
                        KrafkaError::protocol("no mutually supported DescribeProducers API version")
                    })?;

                let response_bytes = match conn
                    .send_request(ApiKey::DescribeProducers, version, |buf| {
                        request.encode_versioned(version, buf)
                    })
                    .await
                {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        warn!(
                            "DescribeProducers request failed on broker {}: {}",
                            broker.id, e
                        );
                        continue;
                    }
                };

                let mut buf = response_bytes;
                let response = match DescribeProducersResponse::decode_versioned(version, &mut buf)
                {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(
                            "DescribeProducers decode failed on broker {}: {}",
                            broker.id, e
                        );
                        continue;
                    }
                };

                for topic in response.topics {
                    let entry = all_results.entry(topic.name.clone()).or_insert_with(|| {
                        DescribeProducersTopicResult {
                            name: topic.name,
                            partitions: Vec::new(),
                        }
                    });
                    entry
                        .partitions
                        .extend(topic.partitions.into_iter().map(|p| {
                            if p.error_code == crate::error::ErrorCode::NotLeaderForPartition {
                                has_stale_leader = true;
                            }
                            DescribeProducersPartitionInfo {
                                partition_index: p.partition_index,
                                error: if p.error_code.is_ok() {
                                    None
                                } else {
                                    Some(
                                        p.error_message
                                            .unwrap_or_else(|| format!("{:?}", p.error_code)),
                                    )
                                },
                                active_producers: p
                                    .active_producers
                                    .into_iter()
                                    .map(|pr| ProducerStateInfo {
                                        producer_id: pr.producer_id,
                                        producer_epoch: pr.producer_epoch,
                                        last_sequence: pr.last_sequence,
                                        last_timestamp: pr.last_timestamp,
                                        coordinator_epoch: pr.coordinator_epoch,
                                        current_txn_start_offset: pr.current_txn_start_offset,
                                    })
                                    .collect(),
                            }
                        }));
                }
            }

            if has_stale_leader && attempt == 0 {
                warn!(
                    "NotLeaderForPartition in DescribeProducers response, retrying with refreshed metadata"
                );
                continue;
            }

            let results: Vec<DescribeProducersTopicResult> = all_results.into_values().collect();
            info!("DescribeProducers returned {} topic(s)", results.len());
            return Ok(results);
        }
        unreachable!()
    }

    // ════════════════════════════════════════════════════════════════════
    // DescribeTransactions (API key 65)
    // ════════════════════════════════════════════════════════════════════

    /// Describe the state of the given transactions.
    ///
    /// Routes each transactional ID to its transaction coordinator via
    /// `FindCoordinator`, groups by coordinator, and batches requests.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = admin
    ///     .describe_transactions(&["txn-1", "txn-2"])
    ///     .await?;
    /// ```
    pub async fn describe_transactions(
        &self,
        transactional_ids: &[&str],
    ) -> Result<Vec<TransactionDescription>> {
        self.check_not_closed()?;
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::broker(
                crate::error::ErrorCode::UnknownServerError,
                "no brokers available",
            ));
        }

        // Group transactional IDs by their coordinator broker.
        let any_broker = &brokers[0];
        let any_conn = self
            .pool
            .get_connection_by_id(any_broker.id, any_broker.address())
            .await?;

        let mut coordinator_txns: HashMap<i32, Vec<String>> = HashMap::new();

        for txn_id in transactional_ids {
            let coord_request = FindCoordinatorRequest::for_transaction(txn_id);
            let coord_version = any_conn
                .negotiate_api_version(
                    ApiKey::FindCoordinator,
                    versions::FIND_COORDINATOR_MAX,
                    versions::FIND_COORDINATOR_MIN,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol("no mutually supported FindCoordinator API version")
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
                coordinator_txns
                    .entry(coord_response.node_id)
                    .or_default()
                    .push((*txn_id).to_string());
            } else {
                warn!(
                    "FindCoordinator failed for txn '{}': {:?}, falling back to broker {}",
                    txn_id, coord_response.error_code, any_broker.id
                );
                coordinator_txns
                    .entry(any_broker.id)
                    .or_default()
                    .push((*txn_id).to_string());
            }
        }

        let mut all_results = Vec::new();

        for (broker_id, txn_ids) in coordinator_txns {
            let broker = brokers
                .iter()
                .find(|b| b.id == broker_id)
                .unwrap_or(any_broker);
            let conn = self
                .pool
                .get_connection_by_id(broker.id, broker.address())
                .await?;

            let request = DescribeTransactionsRequest {
                transactional_ids: txn_ids,
            };

            let version = conn
                .negotiate_api_version(
                    ApiKey::DescribeTransactions,
                    versions::DESCRIBE_TRANSACTIONS_MAX,
                    versions::DESCRIBE_TRANSACTIONS_MIN,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol("no mutually supported DescribeTransactions API version")
                })?;

            let response_bytes = match conn
                .send_request(ApiKey::DescribeTransactions, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await
            {
                Ok(bytes) => bytes,
                Err(e) => {
                    warn!(
                        "DescribeTransactions request failed on broker {}: {}",
                        broker.id, e
                    );
                    continue;
                }
            };

            let mut buf = response_bytes;
            let response = match DescribeTransactionsResponse::decode_versioned(version, &mut buf) {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "DescribeTransactions decode failed on broker {}: {}",
                        broker.id, e
                    );
                    continue;
                }
            };

            all_results.extend(response.transaction_states.into_iter().map(|s| {
                TransactionDescription {
                    transactional_id: s.transactional_id,
                    error: if s.error_code.is_ok() {
                        None
                    } else {
                        Some(format!("{:?}", s.error_code))
                    },
                    state: s.transaction_state,
                    timeout_ms: s.transaction_timeout_ms,
                    start_time_ms: s.transaction_start_time_ms,
                    producer_id: s.producer_id,
                    producer_epoch: s.producer_epoch,
                    topics: s
                        .topics
                        .into_iter()
                        .map(|t| TransactionTopicInfo {
                            topic: t.topic,
                            partitions: t.partitions,
                        })
                        .collect(),
                }
            }));
        }

        info!(
            "DescribeTransactions returned {} transaction(s)",
            all_results.len()
        );
        Ok(all_results)
    }

    // ════════════════════════════════════════════════════════════════════
    // ListTransactions (API key 66)
    // ════════════════════════════════════════════════════════════════════

    /// List transactions matching the given filters.
    ///
    /// Queries **all** brokers and merges results, because each broker
    /// only knows about transactions it coordinates.
    ///
    /// Pass empty slices for `state_filters` and `producer_id_filters` to
    /// list all transactions.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // List all ongoing transactions
    /// let txns = admin
    ///     .list_transactions(&["Ongoing"], &[], -1)
    ///     .await?;
    /// ```
    pub async fn list_transactions(
        &self,
        state_filters: &[&str],
        producer_id_filters: &[i64],
        duration_filter: i64,
    ) -> Result<ListTransactionsResult> {
        self.check_not_closed()?;
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::broker(
                crate::error::ErrorCode::UnknownServerError,
                "no brokers available",
            ));
        }

        let request = ListTransactionsRequest {
            state_filters: state_filters.iter().map(|s| (*s).to_string()).collect(),
            producer_id_filters: producer_id_filters.to_vec(),
            duration_filter,
        };

        let mut all_transactions = Vec::new();
        let mut all_unknown_state_filters = Vec::new();
        let mut last_error: Option<String> = None;

        for broker in &brokers {
            let conn = self
                .pool
                .get_connection_by_id(broker.id, broker.address())
                .await?;

            let version = conn
                .negotiate_api_version(
                    ApiKey::ListTransactions,
                    versions::LIST_TRANSACTIONS_MAX,
                    versions::LIST_TRANSACTIONS_MIN,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol("no mutually supported ListTransactions API version")
                })?;

            let response_bytes = match conn
                .send_request(ApiKey::ListTransactions, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await
            {
                Ok(bytes) => bytes,
                Err(e) => {
                    warn!(
                        "ListTransactions request failed on broker {}: {}",
                        broker.id, e
                    );
                    continue;
                }
            };

            let mut buf = response_bytes;
            let response = match ListTransactionsResponse::decode_versioned(version, &mut buf) {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "ListTransactions decode failed on broker {}: {}",
                        broker.id, e
                    );
                    continue;
                }
            };

            if !response.error_code.is_ok() {
                warn!(
                    "ListTransactions error on broker {}: {:?}",
                    broker.id, response.error_code
                );
                last_error = Some(format!("{:?}", response.error_code));
            }

            for filter in response.unknown_state_filters {
                if !all_unknown_state_filters.contains(&filter) {
                    all_unknown_state_filters.push(filter);
                }
            }

            all_transactions.extend(response.transaction_states.into_iter().map(|s| {
                TransactionListEntry {
                    transactional_id: s.transactional_id,
                    producer_id: s.producer_id,
                    state: s.transaction_state,
                }
            }));
        }

        info!(
            "ListTransactions returned {} transaction(s) across {} broker(s)",
            all_transactions.len(),
            brokers.len()
        );

        Ok(ListTransactionsResult {
            error: last_error,
            unknown_state_filters: all_unknown_state_filters,
            transactions: all_transactions,
        })
    }

    // ════════════════════════════════════════════════════════════════════
    // ListClientMetricsResources (API key 74)
    // ════════════════════════════════════════════════════════════════════

    /// List client metrics subscription resources (KIP-714).
    ///
    /// Returns the names of client metrics subscriptions configured on
    /// the cluster.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let names = admin.list_client_metrics_resources().await?;
    /// for name in &names {
    ///     println!("subscription: {name}");
    /// }
    /// ```
    pub async fn list_client_metrics_resources(&self) -> Result<Vec<String>> {
        let conn = self.get_any_broker_connection().await?;

        let request = ListClientMetricsResourcesRequest;

        let version = conn
            .negotiate_api_version(
                ApiKey::ListClientMetricsResources,
                versions::LIST_CLIENT_METRICS_RESOURCES_MAX,
                versions::LIST_CLIENT_METRICS_RESOURCES_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol(
                    "no mutually supported ListClientMetricsResources API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::ListClientMetricsResources, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = ListClientMetricsResourcesResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            warn!(
                "ListClientMetricsResources error: {:?}",
                response.error_code
            );
        }

        let names: Vec<String> = response
            .client_metrics_resources
            .into_iter()
            .map(|r| r.name)
            .collect();

        info!(
            "ListClientMetricsResources returned {} resource(s)",
            names.len()
        );
        Ok(names)
    }

    // ════════════════════════════════════════════════════════════════════
    // WriteTxnMarkers (API key 27)
    // ════════════════════════════════════════════════════════════════════

    /// Write transaction markers (COMMIT or ABORT) to the given topic-partitions.
    ///
    /// This is an inter-broker API used to finalize transactions.
    /// The admin client exposes it primarily for **aborting stuck transactions**
    /// (`abort_transaction`).
    ///
    /// Each marker is sent to **all** brokers since the partitions may be led
    /// by different brokers. Per-broker errors are logged and skipped so
    /// results from reachable brokers are still returned.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use krafka::protocol::{WritableTxnMarker, WritableTxnMarkerTopic};
    ///
    /// let results = admin
    ///     .write_txn_markers(&[WritableTxnMarker {
    ///         producer_id: 42,
    ///         producer_epoch: 5,
    ///         transaction_result: false, // ABORT
    ///         topics: vec![WritableTxnMarkerTopic {
    ///             name: "my-topic".into(),
    ///             partition_indexes: vec![0, 1],
    ///         }],
    ///         coordinator_epoch: 10,
    ///     }])
    ///     .await?;
    /// ```
    pub async fn write_txn_markers(
        &self,
        markers: &[WritableTxnMarker],
    ) -> Result<Vec<WriteTxnMarkersResult>> {
        self.check_not_closed()?;
        let conn = self.get_any_broker_connection().await?;

        let request = WriteTxnMarkersRequest {
            markers: markers.to_vec(),
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::WriteTxnMarkers,
                versions::WRITE_TXN_MARKERS_MAX,
                versions::WRITE_TXN_MARKERS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported WriteTxnMarkers API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::WriteTxnMarkers, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = WriteTxnMarkersResponse::decode_versioned(version, &mut buf)?;

        let results = response
            .markers
            .into_iter()
            .map(|m| WriteTxnMarkersResult {
                producer_id: m.producer_id,
                topics: m
                    .topics
                    .into_iter()
                    .map(|t| WriteTxnMarkersTopicResult {
                        name: t.name,
                        partitions: t
                            .partitions
                            .into_iter()
                            .map(|p| WriteTxnMarkersPartitionResult {
                                partition_index: p.partition_index,
                                error: if p.error_code.is_ok() {
                                    None
                                } else {
                                    Some(format!("{:?}", p.error_code))
                                },
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        info!(
            "WriteTxnMarkers returned {} marker result(s)",
            results.len()
        );
        Ok(results)
    }

    /// Abort a stuck transaction by writing an ABORT marker.
    ///
    /// This is the admin-friendly wrapper around [`write_txn_markers`](Self::write_txn_markers)
    /// that looks up the transaction coordinator, discovers the affected
    /// partitions via [`describe_transactions`](Self::describe_transactions),
    /// and writes an ABORT marker.
    ///
    /// # Example
    ///
    /// ```ignore
    /// admin.abort_transaction("my-transactional-id").await?;
    /// ```
    pub async fn abort_transaction(
        &self,
        transactional_id: &str,
    ) -> Result<Vec<WriteTxnMarkersResult>> {
        self.check_not_closed()?;

        // Describe the transaction to get producer_id, producer_epoch,
        // coordinator_epoch, and the affected topic-partitions.
        let descriptions = self.describe_transactions(&[transactional_id]).await?;
        let desc = descriptions
            .first()
            .ok_or_else(|| KrafkaError::protocol("no transaction description returned"))?;

        if let Some(ref err) = desc.error {
            return Err(KrafkaError::protocol(format!(
                "cannot abort transaction '{}': {}",
                transactional_id, err,
            )));
        }

        let topics: Vec<WritableTxnMarkerTopic> = desc
            .topics
            .iter()
            .map(|t| WritableTxnMarkerTopic {
                name: t.topic.clone(),
                partition_indexes: t.partitions.clone(),
            })
            .collect();

        let marker = WritableTxnMarker {
            producer_id: desc.producer_id,
            producer_epoch: desc.producer_epoch,
            transaction_result: false, // ABORT
            topics,
            coordinator_epoch: 0, // Use 0 — the broker will validate
        };

        self.write_txn_markers(&[marker]).await
    }

    // ════════════════════════════════════════════════════════════════════
    // DescribeQuorum (API key 55)
    // ════════════════════════════════════════════════════════════════════

    /// Describe the KRaft quorum for the given topic-partitions.
    ///
    /// In a KRaft-mode cluster this returns the current voters, observers,
    /// leader, leader epoch, and high watermark for each quorum partition.
    ///
    /// The primary use case is inspecting `__cluster_metadata` partition 0.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = admin
    ///     .describe_quorum(&[("__cluster_metadata", &[0])])
    ///     .await?;
    /// ```
    pub async fn describe_metadata_quorum(
        &self,
        topic_partitions: &[(&str, &[i32])],
    ) -> Result<DescribeQuorumResult> {
        self.check_not_closed()?;
        let conn = self.get_any_broker_connection().await?;

        let topics = topic_partitions
            .iter()
            .map(|(name, partitions)| DescribeQuorumTopicRequest {
                topic_name: (*name).to_string(),
                partitions: partitions
                    .iter()
                    .map(|&p| DescribeQuorumPartitionRequest { partition_index: p })
                    .collect(),
            })
            .collect();

        let request = DescribeQuorumRequest { topics };

        let version = conn
            .negotiate_api_version(
                ApiKey::DescribeQuorum,
                versions::DESCRIBE_QUORUM_MAX,
                versions::DESCRIBE_QUORUM_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported DescribeQuorum API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DescribeQuorum, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DescribeQuorumResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            warn!("DescribeQuorum top-level error: {:?}", response.error_code);
        }

        let topics = response
            .topics
            .into_iter()
            .map(|t| QuorumTopicResult {
                topic_name: t.topic_name,
                partitions: t
                    .partitions
                    .into_iter()
                    .map(|p| QuorumPartitionResult {
                        partition_index: p.partition_index,
                        error: if p.error_code.is_ok() {
                            None
                        } else {
                            Some(format!("{:?}", p.error_code))
                        },
                        leader_id: p.leader_id,
                        leader_epoch: p.leader_epoch,
                        high_watermark: p.high_watermark,
                        current_voters: p
                            .current_voters
                            .into_iter()
                            .map(|v| QuorumReplicaInfo {
                                replica_id: v.replica_id,
                                log_end_offset: v.log_end_offset,
                            })
                            .collect(),
                        observers: p
                            .observers
                            .into_iter()
                            .map(|o| QuorumReplicaInfo {
                                replica_id: o.replica_id,
                                log_end_offset: o.log_end_offset,
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        info!("DescribeQuorum returned {} topic(s)", topics.len());

        Ok(DescribeQuorumResult {
            error: if response.error_code.is_ok() {
                None
            } else {
                Some(format!("{:?}", response.error_code))
            },
            topics,
        })
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

/// Builder for AdminClient.
#[must_use = "builders do nothing until .build() is called"]
#[derive(Debug, Default)]
pub struct AdminClientBuilder {
    config: AdminConfig,
}

impl AdminClientBuilder {
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

    /// Set authentication configuration.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use krafka::admin::AdminClient;
    /// use krafka::auth::AuthConfig;
    ///
    /// let client = AdminClient::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .auth(AuthConfig::sasl_plain("user", "password")?)
    ///     .build()
    ///     .await?;
    /// ```
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = Some(auth);
        self
    }

    /// Set SOCKS5 proxy configuration.
    ///
    /// Routes all broker connections through the specified SOCKS5 proxy.
    #[cfg(feature = "socks5")]
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.config.proxy = Some(proxy);
        self
    }

    /// Configure SASL/PLAIN authentication.
    pub fn sasl_plain(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> crate::Result<Self> {
        self.config.auth = Some(AuthConfig::sasl_plain(username, password)?);
        Ok(self)
    }

    /// Configure SASL/SCRAM-SHA-256 authentication.
    pub fn sasl_scram_sha256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_scram_sha256(username, password));
        self
    }

    /// Configure SASL/SCRAM-SHA-512 authentication.
    pub fn sasl_scram_sha512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_scram_sha512(username, password));
        self
    }

    /// Configure SASL/OAUTHBEARER authentication with a static token.
    ///
    /// For automatic token refresh, use [`sasl_oauthbearer_provider()`](Self::sasl_oauthbearer_provider).
    /// For SASL extensions, use `.auth(AuthConfig::sasl_oauthbearer_token(...))`.
    pub fn sasl_oauthbearer(mut self, token: impl Into<String>) -> Self {
        self.config.auth = Some(AuthConfig::sasl_oauthbearer(token));
        self
    }

    /// Configure SASL/OAUTHBEARER authentication with an async token provider.
    ///
    /// The provider is called on every new broker connection, ensuring
    /// tokens are always fresh.
    pub fn sasl_oauthbearer_provider(
        mut self,
        provider: impl crate::auth::OAuthBearerTokenProvider + 'static,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_oauthbearer_provider(provider));
        self
    }

    /// Build the admin client.
    pub async fn build(self) -> Result<AdminClient> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap.servers is required"));
        }

        let bootstrap_servers =
            crate::util::parse_bootstrap_servers(&self.config.bootstrap_servers)?;

        // Create connection config with client ID and auth
        let mut conn_config_builder = ConnectionConfig::builder()
            .client_id(&self.config.client_id)
            .request_timeout(self.config.request_timeout);

        if let Some(ref auth) = self.config.auth {
            conn_config_builder = conn_config_builder.auth(auth.clone());
        }

        #[cfg(feature = "socks5")]
        if let Some(ref proxy) = self.config.proxy {
            conn_config_builder = conn_config_builder.proxy(proxy.clone());
        }

        let mut conn_config = conn_config_builder.build();
        conn_config.init_tls().await?;

        let pool = Arc::new(ConnectionPool::new(conn_config));
        let metadata = Arc::new(
            ClusterMetadata::new(bootstrap_servers, pool.clone(), Duration::from_secs(300))
                .with_recovery_strategy(self.config.metadata_recovery_strategy)
                .with_rebootstrap_trigger(self.config.metadata_recovery_rebootstrap_trigger),
        );

        metadata.refresh().await?;

        info!(
            "AdminClient initialized with auth: {}",
            if self.config.auth.is_some() {
                "configured"
            } else {
                "none"
            }
        );

        Ok(AdminClient {
            config: self.config,
            metadata,
            pool,
            closed: std::sync::atomic::AtomicBool::new(false),
        })
    }
}

#[cfg(test)]
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

    #[test]
    fn test_admin_config_default() {
        let config = AdminConfig::default();
        assert_eq!(config.client_id, "krafka-admin");
        assert_eq!(config.request_timeout, Duration::from_secs(30));
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
    fn test_admin_builder_with_auth() {
        use crate::auth::AuthConfig;

        let builder = AdminClient::builder()
            .bootstrap_servers("broker:9093")
            .auth(AuthConfig::sasl_plain("user", "pass").unwrap());

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(!auth.requires_tls());
        assert!(auth.plain_credentials.is_some());
    }

    #[test]
    fn test_admin_builder_sasl_plain() {
        let builder = AdminClient::builder()
            .bootstrap_servers("broker:9093")
            .sasl_plain("admin", "admin-secret")
            .unwrap();

        let auth = builder.config.auth.as_ref().unwrap();
        assert_eq!(
            auth.security_protocol,
            crate::auth::SecurityProtocol::SaslPlaintext
        );
        assert_eq!(auth.sasl_mechanism, Some(crate::auth::SaslMechanism::Plain));
        let creds = auth.plain_credentials.as_ref().unwrap();
        assert_eq!(creds.username, "admin");
    }

    #[test]
    fn test_admin_builder_sasl_scram() {
        let builder = AdminClient::builder()
            .bootstrap_servers("broker:9093")
            .sasl_scram_sha256("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert_eq!(
            auth.sasl_mechanism,
            Some(crate::auth::SaslMechanism::ScramSha256)
        );
        assert!(auth.scram_credentials.is_some());

        let builder = AdminClient::builder()
            .bootstrap_servers("broker:9093")
            .sasl_scram_sha512("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert_eq!(
            auth.sasl_mechanism,
            Some(crate::auth::SaslMechanism::ScramSha512)
        );
        assert!(auth.scram_credentials.is_some());
    }

    #[test]
    fn test_admin_builder_aws_msk_iam() {
        use crate::auth::AuthConfig;

        let auth = AuthConfig::aws_msk_iam("AKID", "secret", "us-east-1");
        let builder = AdminClient::builder()
            .bootstrap_servers("broker:9094")
            .auth(auth);

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_tls());
        assert!(auth.requires_sasl());
        assert_eq!(
            auth.sasl_mechanism,
            Some(crate::auth::SaslMechanism::AwsMskIam)
        );
        assert!(auth.aws_msk_iam_credentials.is_some());
        assert!(auth.tls_config.is_some());
    }

    #[test]
    fn test_admin_builder_no_auth_by_default() {
        let builder = AdminClient::builder().bootstrap_servers("broker:9092");

        assert!(builder.config.auth.is_none());
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
