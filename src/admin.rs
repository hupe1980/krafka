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
//!     .auth(AuthConfig::sasl_plain("user", "password"))
//!     .build()
//!     .await?;
//! ```

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tracing::{info, warn};

use crate::auth::AuthConfig;
use crate::error::{KrafkaError, Result};
use crate::metadata::{BrokerInfo, ClusterMetadata, TopicInfo};
use crate::network::{BrokerConnection, ConnectionConfig, ConnectionPool};

use crate::protocol::{
    AclBinding, AclBindingFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
    AlterClientQuotasRequest, AlterClientQuotasResponse, AlterConfigOp, AlterQuotaEntity,
    AlterQuotaEntry, AlterQuotaOp, AlterableConfig, ApiKey, ConsumerGroupDescribeRequest,
    ConsumerGroupDescribeResponse, CreatableRenewer, CreatableTopic, CreatableTopicConfig,
    CreateAclsRequest, CreateAclsResponse, CreateDelegationTokenRequest,
    CreateDelegationTokenResponse, CreatePartitionsRequest, CreatePartitionsResponse,
    CreatePartitionsTopic, CreateTopicsRequest, CreateTopicsResponse, DeleteAclsRequest,
    DeleteAclsResponse, DeleteGroupsRequest, DeleteGroupsResponse, DeleteRecordsPartition,
    DeleteRecordsRequest, DeleteRecordsResponse, DeleteRecordsTopic, DeleteTopicState,
    DeleteTopicsRequest, DeleteTopicsResponse, DescribeAclsRequest, DescribeAclsResponse,
    DescribeClientQuotasRequest, DescribeClientQuotasResponse, DescribeClusterRequest,
    DescribeClusterResponse, DescribeConfigsRequest, DescribeConfigsResponse,
    DescribeDelegationTokenOwner, DescribeDelegationTokenRequest, DescribeDelegationTokenResponse,
    DescribeGroupsRequest, DescribeGroupsResponse, DescribeTopicPartitionsCursor,
    DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse, ExpireDelegationTokenRequest,
    ExpireDelegationTokenResponse, FindCoordinatorRequest, FindCoordinatorResponse,
    IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse, ListGroupsRequest,
    ListGroupsResponse, OffsetForLeaderEpochPartition, OffsetForLeaderEpochRequest,
    OffsetForLeaderEpochResponse, OffsetForLeaderEpochTopic, QuotaFilterComponent,
    RenewDelegationTokenRequest, RenewDelegationTokenResponse, VersionedDecode, VersionedEncode,
    versions,
};

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
    pub fn new(name: impl Into<String>, num_partitions: i32, replication_factor: i16) -> Self {
        Self {
            name: name.into(),
            num_partitions,
            replication_factor,
            configs: HashMap::new(),
        }
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

/// Description of a consumer group.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConsumerGroupDescription {
    /// Group ID.
    pub group_id: String,
    /// Group state (e.g., "Stable", "Empty", "Dead", "PreparingRebalance").
    pub state: String,
    /// Protocol type (e.g., "consumer").
    pub protocol_type: String,
    /// Protocol (e.g., assignor name like "range", "roundrobin").
    pub protocol: String,
    /// Group members.
    pub members: Vec<ConsumerGroupMember>,
    /// Error message if any.
    pub error: Option<String>,
}

/// A member of a consumer group.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConsumerGroupMember {
    /// Member ID.
    pub member_id: String,
    /// Group instance ID (static membership).
    pub group_instance_id: Option<String>,
    /// Client ID.
    pub client_id: String,
    /// Client host.
    pub client_host: String,
}

/// Listing entry for a consumer group.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ConsumerGroupListing {
    /// Group ID.
    pub group_id: String,
    /// Protocol type (e.g., "consumer").
    pub protocol_type: String,
}

/// Description of a KIP-848 consumer group (from ConsumerGroupDescribe API).
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Kip848GroupDescription {
    /// Group ID.
    pub group_id: String,
    /// Group state (e.g., "Stable", "Empty", "Dead", "Assigning").
    pub state: String,
    /// Group epoch.
    pub group_epoch: i32,
    /// Assignment epoch.
    pub assignment_epoch: i32,
    /// Assignor name (e.g., "uniform").
    pub assignor_name: String,
    /// Group members.
    pub members: Vec<Kip848GroupMember>,
    /// Authorized operations bitfield (-2^31 if not requested).
    pub authorized_operations: i32,
    /// Error message if any.
    pub error: Option<String>,
}

/// A member of a KIP-848 consumer group.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Kip848GroupMember {
    /// Member ID.
    pub member_id: String,
    /// Instance ID (static membership).
    pub instance_id: Option<String>,
    /// Rack ID.
    pub rack_id: Option<String>,
    /// Current member epoch.
    pub member_epoch: i32,
    /// Client ID.
    pub client_id: String,
    /// Client host.
    pub client_host: String,
    /// Subscribed topic names.
    pub subscribed_topic_names: Vec<String>,
    /// Subscribed topic regex.
    pub subscribed_topic_regex: Option<String>,
    /// Current partition assignment.
    pub assignment: Vec<Kip848TopicPartition>,
    /// Target partition assignment.
    pub target_assignment: Vec<Kip848TopicPartition>,
    /// Member type. -1 = unknown, 0 = classic, 1 = consumer (v1+).
    pub member_type: i8,
}

/// Topic-partition assignment within a KIP-848 consumer group description.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct Kip848TopicPartition {
    /// Topic ID (UUID).
    pub topic_id: [u8; 16],
    /// Topic name.
    pub topic_name: String,
    /// Assigned partition indices.
    pub partitions: Vec<i32>,
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
/// [`AdminClient::describe_delegation_tokens()`].
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
    /// Populated by [`AdminClient::describe_delegation_tokens()`]. Empty when
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
        self.check_not_closed()?;
        // Get any broker connection
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::broker(
                crate::error::ErrorCode::UnknownServerError,
                "no brokers available",
            ));
        }

        let broker = &brokers[0];
        let conn = self
            .pool
            .get_connection_by_id(broker.id, broker.address())
            .await?;

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

    /// Describe configuration for a topic.
    pub async fn describe_topic_config(&self, topic: &str) -> Result<Vec<ConfigEntry>> {
        let conn = self.get_any_broker_connection().await?;

        let request = DescribeConfigsRequest::for_topic(topic);

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

    /// Describe configuration for a broker.
    pub async fn describe_broker_config(&self, broker_id: i32) -> Result<Vec<ConfigEntry>> {
        let conn = self.get_any_broker_connection().await?;

        let request = DescribeConfigsRequest::for_broker(broker_id);

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

    /// Describe the cluster.
    pub async fn describe_cluster(&self) -> Result<ClusterDescription> {
        self.check_not_closed()?;
        self.metadata.refresh().await?;
        let brokers = self.metadata.brokers();
        let controller = self.metadata.controller();

        Ok(ClusterDescription {
            controller_id: controller.map(|c| c.id),
            brokers,
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
    /// # Arguments
    /// * `resource_type` - Type of resource (Topic, Group, Cluster, etc.)
    /// * `resource_name` - Name of the resource (use None to match any)
    /// * `pattern_type` - Pattern type (Literal, Prefixed, Any)
    /// * `principal` - Principal (use None to match any)
    /// * `host` - Host (use None to match any)
    /// * `operation` - Operation (use Any to match all)
    /// * `permission_type` - Permission type (use Any to match all)
    ///
    /// # Example
    /// ```ignore
    /// // Describe all ACLs for a specific topic
    /// let result = admin.describe_acls(
    ///     AclResourceType::Topic,
    ///     Some("my-topic"),
    ///     AclPatternType::Literal,
    ///     None,
    ///     None,
    ///     AclOperation::Any,
    ///     AclPermissionType::Any,
    /// ).await?;
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub async fn describe_acls(
        &self,
        resource_type: AclResourceType,
        resource_name: Option<&str>,
        pattern_type: AclPatternType,
        principal: Option<&str>,
        host: Option<&str>,
        operation: AclOperation,
        permission_type: AclPermissionType,
    ) -> Result<DescribeAclsResult> {
        self.check_not_closed()?;
        self.describe_acls_with_filter(AclFilter {
            resource_type,
            resource_name: resource_name.map(|s| s.to_string()),
            pattern_type,
            principal: principal.map(|s| s.to_string()),
            host: host.map(|s| s.to_string()),
            operation,
            permission_type,
        })
        .await
    }

    /// Describe ACLs matching a filter.
    ///
    /// This is the preferred method for describing ACLs as it uses a structured
    /// filter object.
    ///
    /// # Example
    /// ```ignore
    /// // Describe all ACLs for a specific topic
    /// let filter = AclFilter::for_resource(AclResourceType::Topic, "my-topic");
    /// let result = admin.describe_acls_with_filter(filter).await?;
    /// ```
    pub async fn describe_acls_with_filter(&self, filter: AclFilter) -> Result<DescribeAclsResult> {
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
    /// Returns detailed information about each group including state, members,
    /// and partition assignments.
    ///
    /// # Example
    /// ```ignore
    /// let groups = admin.describe_groups(vec!["my-group".to_string()]).await?;
    /// for group in &groups {
    ///     println!("{}: state={}, members={}", group.group_id, group.state, group.members.len());
    /// }
    /// ```
    pub async fn describe_groups(
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

        // Group the group_ids by their coordinator broker
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
                // Fallback to first broker if coordinator lookup fails
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

        for (broker_id, groups) in coordinator_groups {
            // Find broker address
            let broker = brokers
                .iter()
                .find(|b| b.id == broker_id)
                .unwrap_or(any_broker);
            let conn = self
                .pool
                .get_connection_by_id(broker.id, broker.address())
                .await?;

            let request = DescribeGroupsRequest {
                groups: groups.clone(),
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
                all_results.push(ConsumerGroupDescription {
                    group_id: g.group_id,
                    state: g.group_state,
                    protocol_type: g.protocol_type,
                    protocol: g.protocol_data,
                    members: g
                        .members
                        .into_iter()
                        .map(|m| ConsumerGroupMember {
                            member_id: m.member_id,
                            group_instance_id: m.group_instance_id,
                            client_id: m.client_id,
                            client_host: m.client_host,
                        })
                        .collect(),
                    error: if g.error_code.is_ok() {
                        None
                    } else {
                        Some(format!("{:?}", g.error_code))
                    },
                });
            }
        }

        info!("Described {} groups", all_results.len());
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

        for broker in &brokers {
            let conn = match self
                .pool
                .get_connection_by_id(broker.id, broker.address())
                .await
            {
                Ok(c) => c,
                Err(_) => continue, // Skip unreachable brokers
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
                    continue;
                }
            };

            let mut buf = response_bytes;
            let response = match ListGroupsResponse::decode_versioned(version, &mut buf) {
                Ok(r) => r,
                Err(e) => {
                    warn!("ListGroups decode failed on broker {}: {}", broker.id, e);
                    continue;
                }
            };

            if !response.error_code.is_ok() {
                tracing::warn!(
                    "ListGroups error on broker {}: {:?}",
                    broker.id,
                    response.error_code
                );
                continue;
            }

            for group in response.groups {
                if seen_ids.insert(group.group_id.clone()) {
                    all_groups.push(ConsumerGroupListing {
                        group_id: group.group_id,
                        protocol_type: group.protocol_type,
                    });
                }
            }
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

        info!("Deleted records from {} partition(s)", results.len());
        Ok(results)
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
                    KrafkaError::protocol("no mutually supported OffsetForLeaderEpoch API version")
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

        info!(
            "Got leader epoch offsets for {} partition(s)",
            results.len()
        );
        Ok(results)
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
    pub async fn describe_delegation_tokens(
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
    pub async fn delete_groups(&self, group_ids: Vec<String>) -> Result<Vec<DeleteGroupResult>> {
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

    /// Describe the cluster using the DescribeCluster API (Key 60).
    ///
    /// Returns richer cluster info than [`describe_cluster`](Self::describe_cluster),
    /// including cluster ID and authorized operations.
    pub async fn describe_cluster_detailed(&self) -> Result<DescribeClusterResult> {
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

    /// Describe KIP-848 consumer groups using the ConsumerGroupDescribe API (Key 69).
    ///
    /// Returns rich group information including group epoch, assignment epoch,
    /// assignor name, and per-member target/current assignments with topic UUIDs.
    ///
    /// This API is available on Kafka 4.0+ and is the native way to describe
    /// consumer groups that use the new consumer group protocol (KIP-848).
    ///
    /// # Example
    /// ```ignore
    /// let groups = admin
    ///     .describe_consumer_groups(vec!["my-group".to_string()])
    ///     .await?;
    /// for group in &groups {
    ///     println!("{}: state={}, epoch={}", group.group_id, group.state, group.group_epoch);
    /// }
    /// ```
    pub async fn describe_consumer_groups(
        &self,
        group_ids: Vec<String>,
    ) -> Result<Vec<Kip848GroupDescription>> {
        self.check_not_closed()?;
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::broker(
                crate::error::ErrorCode::UnknownServerError,
                "no brokers available",
            ));
        }

        // Group the group_ids by their coordinator broker.
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

        for (broker_id, groups) in coordinator_groups {
            let broker = brokers
                .iter()
                .find(|b| b.id == broker_id)
                .unwrap_or(any_broker);
            let conn = self
                .pool
                .get_connection_by_id(broker.id, broker.address())
                .await?;

            let request = ConsumerGroupDescribeRequest::new(groups);

            let version = conn
                .negotiate_api_version(
                    ApiKey::ConsumerGroupDescribe,
                    versions::CONSUMER_GROUP_DESCRIBE_MAX,
                    versions::CONSUMER_GROUP_DESCRIBE_MIN,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol("no mutually supported ConsumerGroupDescribe API version")
                })?;

            let response_bytes = conn
                .send_request(ApiKey::ConsumerGroupDescribe, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await?;

            let mut buf = response_bytes;
            let response = ConsumerGroupDescribeResponse::decode_versioned(version, &mut buf)?;

            for g in response.groups {
                all_results.push(Kip848GroupDescription {
                    group_id: g.group_id,
                    state: g.group_state,
                    group_epoch: g.group_epoch,
                    assignment_epoch: g.assignment_epoch,
                    assignor_name: g.assignor_name,
                    members: g
                        .members
                        .into_iter()
                        .map(|m| Kip848GroupMember {
                            member_id: m.member_id,
                            instance_id: m.instance_id,
                            rack_id: m.rack_id,
                            member_epoch: m.member_epoch,
                            client_id: m.client_id,
                            client_host: m.client_host,
                            subscribed_topic_names: m.subscribed_topic_names,
                            subscribed_topic_regex: m.subscribed_topic_regex,
                            assignment: m
                                .assignment
                                .topic_partitions
                                .into_iter()
                                .map(|tp| Kip848TopicPartition {
                                    topic_id: tp.topic_id,
                                    topic_name: tp.topic_name,
                                    partitions: tp.partitions,
                                })
                                .collect(),
                            target_assignment: m
                                .target_assignment
                                .topic_partitions
                                .into_iter()
                                .map(|tp| Kip848TopicPartition {
                                    topic_id: tp.topic_id,
                                    topic_name: tp.topic_name,
                                    partitions: tp.partitions,
                                })
                                .collect(),
                            member_type: m.member_type,
                        })
                        .collect(),
                    authorized_operations: g.authorized_operations,
                    error: if g.error_code.is_ok() {
                        None
                    } else {
                        let msg = g
                            .error_message
                            .unwrap_or_else(|| format!("{:?}", g.error_code));
                        Some(msg)
                    },
                });
            }
        }

        info!("Described {} consumer groups (KIP-848)", all_results.len());
        Ok(all_results)
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

    /// Close the admin client.
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
}

/// Description of a Kafka cluster.
#[derive(Debug, Clone)]
pub struct ClusterDescription {
    /// Controller broker ID.
    pub controller_id: Option<i32>,
    /// List of brokers.
    pub brokers: Vec<BrokerInfo>,
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
    ///     .auth(AuthConfig::sasl_plain("user", "password"))
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
    pub fn sasl_plain(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.config.auth = Some(AuthConfig::sasl_plain(username, password));
        self
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

        let conn_config = conn_config_builder.build();

        let pool = Arc::new(ConnectionPool::new(conn_config));
        let metadata = Arc::new(ClusterMetadata::new(
            bootstrap_servers,
            pool.clone(),
            Duration::from_secs(300),
        ));

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
            .with_config("cleanup.policy", "compact")
            .with_config("retention.ms", "86400000");

        assert_eq!(topic.name, "test-topic");
        assert_eq!(topic.num_partitions, 3);
        assert_eq!(topic.replication_factor, 2);
        assert_eq!(topic.configs.len(), 2);
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
            .auth(AuthConfig::sasl_plain("user", "pass"));

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(!auth.requires_tls());
        assert!(auth.plain_credentials.is_some());
    }

    #[test]
    fn test_admin_builder_sasl_plain() {
        let builder = AdminClient::builder()
            .bootstrap_servers("broker:9093")
            .sasl_plain("admin", "admin-secret");

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
            state: "Stable".to_string(),
            protocol_type: "consumer".to_string(),
            protocol: "range".to_string(),
            members: vec![
                ConsumerGroupMember {
                    member_id: "member-1".to_string(),
                    group_instance_id: Some("instance-1".to_string()),
                    client_id: "my-client".to_string(),
                    client_host: "/127.0.0.1".to_string(),
                },
                ConsumerGroupMember {
                    member_id: "member-2".to_string(),
                    group_instance_id: None,
                    client_id: "other-client".to_string(),
                    client_host: "/192.168.1.1".to_string(),
                },
            ],
            error: None,
        };
        assert_eq!(desc.group_id, "my-group");
        assert_eq!(desc.state, "Stable");
        assert_eq!(desc.members.len(), 2);
        assert!(desc.members[0].group_instance_id.is_some());
        assert!(desc.members[1].group_instance_id.is_none());
        assert!(desc.error.is_none());
    }

    #[test]
    fn test_consumer_group_listing() {
        let listing = ConsumerGroupListing {
            group_id: "my-group".to_string(),
            protocol_type: "consumer".to_string(),
        };
        assert_eq!(listing.group_id, "my-group");
        assert_eq!(listing.protocol_type, "consumer");
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
