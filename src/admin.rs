//! Admin client for Apache Kafka.
//!
//! This module provides administrative operations:
//! - Create/delete/describe topics
//! - Create additional partitions
//! - List topics and partitions
//! - Describe and alter configurations
//! - Manage ACLs
//! - Describe cluster and broker configs
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

use tracing::{info, warn};

use crate::auth::AuthConfig;
use crate::error::{KrafkaError, Result};
use crate::metadata::{BrokerInfo, ClusterMetadata, TopicInfo};
use crate::network::{ConnectionConfig, ConnectionPool};

use crate::protocol::{
    AclBinding, AclBindingFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
    AlterConfigsRequest, AlterConfigsResponse, ApiKey, CreatableTopic, CreatableTopicConfig,
    CreateAclsRequest, CreateAclsResponse, CreatePartitionsRequest, CreatePartitionsResponse,
    CreatePartitionsTopic, CreateTopicsRequest, CreateTopicsResponse, DeleteAclsRequest,
    DeleteAclsResponse, DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsResponse,
    DeleteRecordsTopic, DeleteTopicsRequest, DeleteTopicsResponse, DescribeAclsRequest,
    DescribeAclsResponse, DescribeConfigsRequest, DescribeConfigsResponse, DescribeGroupsRequest,
    DescribeGroupsResponse, FindCoordinatorRequest, FindCoordinatorResponse, ListGroupsRequest,
    ListGroupsResponse, OffsetForLeaderEpochPartition, OffsetForLeaderEpochRequest,
    OffsetForLeaderEpochResponse, OffsetForLeaderEpochTopic, versions,
};

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
    /// Whether this is the default value.
    pub is_default: bool,
    /// Whether the config is sensitive (passwords, etc.).
    pub is_sensitive: bool,
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
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            client_id: "krafka-admin".to_string(),
            request_timeout: Duration::from_secs(30),
            auth: None,
        }
    }
}

impl AdminConfig {
    /// Create a new config builder.
    pub fn builder() -> AdminConfigBuilder {
        AdminConfigBuilder::default()
    }

    /// Returns the bootstrap servers.
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Returns the client ID.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the request timeout.
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }

    /// Returns the authentication configuration, if set.
    pub fn auth(&self) -> Option<&AuthConfig> {
        self.auth.as_ref()
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

    /// Build the AdminConfig.
    pub fn build(self) -> AdminConfig {
        self.config
    }
}

/// Kafka admin client for cluster administration.
pub struct AdminClient {
    /// Configuration.
    config: AdminConfig,
    /// Cluster metadata.
    metadata: Arc<ClusterMetadata>,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
}

impl AdminClient {
    /// Create a new admin client builder.
    pub fn builder() -> AdminClientBuilder {
        AdminClientBuilder::default()
    }

    /// Create topics.
    pub async fn create_topics(
        &self,
        topics: Vec<NewTopic>,
        timeout: Duration,
    ) -> Result<Vec<CreateTopicResult>> {
        // Get any broker connection (controller for leadership, but any broker forwards)
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
            .get_connection_by_id(broker.id, &broker.address())
            .await?;

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
            .negotiate_api_version_max(ApiKey::CreateTopics, versions::CREATE_TOPICS_MAX)
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported CreateTopics API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::CreateTopics, version, |buf| match version {
                0 => request.encode_v0(buf),
                1 | 2 => request.encode_v1(buf),
                _ => Err(KrafkaError::protocol(format!(
                    "unsupported CreateTopics version {version}"
                ))),
            })
            .await?;

        // Decode response
        let mut buf = response_bytes;
        let response = match version {
            0 => CreateTopicsResponse::decode_v0(&mut buf)?,
            1 => CreateTopicsResponse::decode_v1(&mut buf)?,
            2 => CreateTopicsResponse::decode_v2(&mut buf)?,
            _ => {
                return Err(KrafkaError::protocol(format!(
                    "unsupported CreateTopics version {version}"
                )));
            }
        };

        // Convert to results
        let results = response
            .topics
            .into_iter()
            .map(|t| CreateTopicResult {
                name: t.name,
                error: if t.error_code.is_ok() {
                    None
                } else {
                    Some(format!("{:?}", t.error_code))
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
            .get_connection_by_id(broker.id, &broker.address())
            .await?;

        // Build request
        let request = DeleteTopicsRequest {
            topic_names: topics.clone(),
            timeout_ms: crate::util::duration_to_millis_i32(timeout),
        };

        // Send request — negotiate API version with broker
        let version = conn
            .negotiate_api_version_max(ApiKey::DeleteTopics, versions::DELETE_TOPICS_MAX)
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported DeleteTopics API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DeleteTopics, version, |buf| request.encode_v0(buf))
            .await?;

        // Decode response
        let mut buf = response_bytes;
        let response = match version {
            0 => DeleteTopicsResponse::decode_v0(&mut buf)?,
            1 => DeleteTopicsResponse::decode_v1(&mut buf)?,
            _ => {
                return Err(KrafkaError::protocol(format!(
                    "unsupported DeleteTopics version {version}"
                )));
            }
        };

        // Convert to results
        let results = response
            .responses
            .into_iter()
            .map(|r| DeleteTopicResult {
                name: r.name.unwrap_or_default(),
                error: if r.error_code.is_ok() {
                    None
                } else {
                    Some(format!("{:?}", r.error_code))
                },
            })
            .collect();

        info!("Deleted {} topics", topics.len());
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
            .get_connection_by_id(broker.id, &broker.address())
            .await?;

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
            .negotiate_api_version_max(ApiKey::CreatePartitions, versions::CREATE_PARTITIONS_MAX)
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported CreatePartitions API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::CreatePartitions, version, |buf| {
                request.encode_v0(buf)
            })
            .await?;

        // Decode response
        let mut buf = response_bytes;
        let response = CreatePartitionsResponse::decode_v0(&mut buf)?;

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
            .get_connection_by_id(broker.id, &broker.address())
            .await?;

        let request = DescribeConfigsRequest::for_topic(topic);

        let version = conn
            .negotiate_api_version_max(ApiKey::DescribeConfigs, versions::DESCRIBE_CONFIGS_MAX)
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported DescribeConfigs API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DescribeConfigs, version, |buf| {
                request.encode_v0(buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DescribeConfigsResponse::decode_v0(&mut buf)?;

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
                    })
                    .collect()
            })
            .collect();

        Ok(entries)
    }

    /// Describe configuration for a broker.
    pub async fn describe_broker_config(&self, broker_id: i32) -> Result<Vec<ConfigEntry>> {
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
            .get_connection_by_id(broker.id, &broker.address())
            .await?;

        let request = DescribeConfigsRequest::for_broker(broker_id);

        let version = conn
            .negotiate_api_version_max(ApiKey::DescribeConfigs, versions::DESCRIBE_CONFIGS_MAX)
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported DescribeConfigs API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DescribeConfigs, version, |buf| {
                request.encode_v0(buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DescribeConfigsResponse::decode_v0(&mut buf)?;

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
                    })
                    .collect()
            })
            .collect();

        Ok(entries)
    }

    /// Alter configuration for a topic.
    ///
    /// Note: This replaces all dynamic configs. To modify a single config,
    /// first describe the topic config and then set all desired values.
    pub async fn alter_topic_config(
        &self,
        topic: &str,
        configs: HashMap<String, String>,
    ) -> Result<AlterConfigResult> {
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
            .get_connection_by_id(broker.id, &broker.address())
            .await?;

        let request = AlterConfigsRequest::for_topic(topic, configs.into_iter().collect());

        let version = conn
            .negotiate_api_version_max(ApiKey::AlterConfigs, versions::ALTER_CONFIGS_MAX)
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported AlterConfigs API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::AlterConfigs, version, |buf| request.encode_v0(buf))
            .await?;

        let mut buf = response_bytes;
        let response = AlterConfigsResponse::decode_v0(&mut buf)?;

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
        self.metadata.refresh().await?;
        Ok(self.metadata.topics().into_iter().map(|t| t.name).collect())
    }

    /// Describe topics.
    pub async fn describe_topics(&self, topics: &[String]) -> Result<Vec<TopicInfo>> {
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
        self.metadata.refresh().await?;
        Ok(self.metadata.partition_count(topic))
    }

    /// Get the client ID.
    pub fn client_id(&self) -> &str {
        &self.config.client_id
    }

    /// Get the request timeout.
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
            .get_connection_by_id(broker.id, &broker.address())
            .await?;

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
            .negotiate_api_version_max(ApiKey::DescribeAcls, versions::DESCRIBE_ACLS_MAX)
            .await
            .ok_or_else(|| {
                KrafkaError::protocol("no mutually supported DescribeAcls API version")
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DescribeAcls, version, |buf| request.encode_v0(buf))
            .await?;

        let mut buf = response_bytes;
        let response = DescribeAclsResponse::decode_v0(&mut buf)?;

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
            .get_connection_by_id(broker.id, &broker.address())
            .await?;

        let request = CreateAclsRequest {
            creations: acls.clone(),
        };

        let version = conn
            .negotiate_api_version_max(ApiKey::CreateAcls, versions::CREATE_ACLS_MAX)
            .await
            .ok_or_else(|| KrafkaError::protocol("no mutually supported CreateAcls API version"))?;

        let response_bytes = conn
            .send_request(ApiKey::CreateAcls, version, |buf| request.encode_v0(buf))
            .await?;

        let mut buf = response_bytes;
        let response = CreateAclsResponse::decode_v0(&mut buf)?;

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
            .get_connection_by_id(broker.id, &broker.address())
            .await?;

        let request = DeleteAclsRequest {
            filters: filters.clone(),
        };

        let version = conn
            .negotiate_api_version_max(ApiKey::DeleteAcls, versions::DELETE_ACLS_MAX)
            .await
            .ok_or_else(|| KrafkaError::protocol("no mutually supported DeleteAcls API version"))?;

        let response_bytes = conn
            .send_request(ApiKey::DeleteAcls, version, |buf| request.encode_v0(buf))
            .await?;

        let mut buf = response_bytes;
        let response = DeleteAclsResponse::decode_v0(&mut buf)?;

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
            .get_connection_by_id(any_broker.id, &any_broker.address())
            .await?;

        for group_id in &group_ids {
            let coord_request = FindCoordinatorRequest::for_group(group_id);
            let coord_version = any_conn
                .negotiate_api_version_max(ApiKey::FindCoordinator, versions::FIND_COORDINATOR_MAX)
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol("no mutually supported FindCoordinator API version")
                })?;

            let coord_response_bytes = any_conn
                .send_request(
                    ApiKey::FindCoordinator,
                    coord_version,
                    |buf| match coord_version {
                        0 => coord_request.encode_v0(buf),
                        1 => coord_request.encode_v1(buf),
                        _ => Err(KrafkaError::protocol(format!(
                            "unsupported FindCoordinator version {coord_version}"
                        ))),
                    },
                )
                .await?;
            let mut coord_buf = coord_response_bytes;
            let coord_response = match coord_version {
                0 => FindCoordinatorResponse::decode_v0(&mut coord_buf)?,
                1 => FindCoordinatorResponse::decode_v1(&mut coord_buf)?,
                _ => {
                    return Err(KrafkaError::protocol(format!(
                        "unsupported FindCoordinator version {coord_version}"
                    )));
                }
            };

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
                .get_connection_by_id(broker.id, &broker.address())
                .await?;

            let request = DescribeGroupsRequest {
                groups: groups.clone(),
            };

            let version = conn
                .negotiate_api_version_max(ApiKey::DescribeGroups, versions::DESCRIBE_GROUPS_MAX)
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol("no mutually supported DescribeGroups API version")
                })?;

            let response_bytes = conn
                .send_request(ApiKey::DescribeGroups, version, |buf| {
                    request.encode_v0(buf)
                })
                .await?;

            let mut buf = response_bytes;
            let response = match version {
                0 => DescribeGroupsResponse::decode_v0(&mut buf)?,
                1 => DescribeGroupsResponse::decode_v1(&mut buf)?,
                _ => {
                    return Err(KrafkaError::protocol(format!(
                        "unsupported DescribeGroups version {version}"
                    )));
                }
            };

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
                .get_connection_by_id(broker.id, &broker.address())
                .await
            {
                Ok(c) => c,
                Err(_) => continue, // Skip unreachable brokers
            };

            let request = ListGroupsRequest;

            let version = match conn
                .negotiate_api_version_max(ApiKey::ListGroups, versions::LIST_GROUPS_MAX)
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
                .send_request(ApiKey::ListGroups, version, |buf| request.encode_v0(buf))
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    warn!("ListGroups RPC failed on broker {}: {}", broker.id, e);
                    continue;
                }
            };

            let mut buf = response_bytes;
            let response = match version {
                0 => match ListGroupsResponse::decode_v0(&mut buf) {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("ListGroups decode failed on broker {}: {}", broker.id, e);
                        continue;
                    }
                },
                1 => match ListGroupsResponse::decode_v1(&mut buf) {
                    Ok(r) => r,
                    Err(e) => {
                        warn!("ListGroups decode failed on broker {}: {}", broker.id, e);
                        continue;
                    }
                },
                _ => {
                    warn!(
                        "Unsupported ListGroups version {version}, skipping broker {}",
                        broker.id
                    );
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
                .get_connection_by_id(broker.id, &broker.address())
                .await?;

            let request = DeleteRecordsRequest {
                topics: topics_map
                    .into_iter()
                    .map(|(name, partitions)| DeleteRecordsTopic { name, partitions })
                    .collect(),
                timeout_ms: crate::util::duration_to_millis_i32(timeout),
            };

            let version = conn
                .negotiate_api_version_max(ApiKey::DeleteRecords, versions::DELETE_RECORDS_MAX)
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol("no mutually supported DeleteRecords API version")
                })?;

            let response_bytes = conn
                .send_request(ApiKey::DeleteRecords, version, |buf| request.encode_v0(buf))
                .await?;

            let mut buf = response_bytes;
            let response = DeleteRecordsResponse::decode_v0(&mut buf)?;

            for topic in response.topics {
                for partition in topic.partitions {
                    results.push(DeleteRecordResult {
                        topic: topic.name.clone(),
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
                .get_connection_by_id(broker.id, &broker.address())
                .await?;

            let request = OffsetForLeaderEpochRequest {
                replica_id: -1, // -1 for consumer
                topics: topics_map
                    .into_iter()
                    .map(|(topic, partitions)| OffsetForLeaderEpochTopic { topic, partitions })
                    .collect(),
            };

            let version = conn
                .negotiate_api_version_max(
                    ApiKey::OffsetForLeaderEpoch,
                    versions::OFFSET_FOR_LEADER_EPOCH_MAX,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol("no mutually supported OffsetForLeaderEpoch API version")
                })?;

            let response_bytes = conn
                .send_request(ApiKey::OffsetForLeaderEpoch, version, |buf| match version {
                    0..=1 => request.encode_v0(buf),
                    2 => request.encode_v2(buf),
                    _ => Err(KrafkaError::protocol(format!(
                        "unsupported OffsetForLeaderEpoch encode version {version}"
                    ))),
                })
                .await?;

            let mut buf = response_bytes;
            let response = match version {
                0 => OffsetForLeaderEpochResponse::decode_v0(&mut buf)?,
                1 => OffsetForLeaderEpochResponse::decode_v1(&mut buf)?,
                2 => OffsetForLeaderEpochResponse::decode_v2(&mut buf)?,
                _ => {
                    return Err(KrafkaError::protocol(format!(
                        "unsupported OffsetForLeaderEpoch version {version}"
                    )));
                }
            };

            for topic in response.topics {
                for partition in topic.partitions {
                    results.push(LeaderEpochResult {
                        topic: topic.topic.clone(),
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

    /// Get access to the connection pool.
    pub fn pool(&self) -> &Arc<ConnectionPool> {
        &self.pool
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

    /// Configure SASL/OAUTHBEARER authentication.
    ///
    /// Uses a static OAuth 2.0 bearer token. For token refresh, reconnect
    /// with a new token. For SASL extensions, use `.auth(AuthConfig::sasl_oauthbearer_token(...))`.
    pub fn sasl_oauthbearer(mut self, token: impl Into<String>) -> Self {
        self.config.auth = Some(AuthConfig::sasl_oauthbearer(token));
        self
    }

    /// Build the admin client.
    pub async fn build(self) -> Result<AdminClient> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap_servers is required"));
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
}
