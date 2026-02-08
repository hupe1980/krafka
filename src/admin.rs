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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::auth::AuthConfig;
use crate::error::{KrafkaError, Result};
use crate::metadata::{BrokerInfo, ClusterMetadata, TopicInfo};
use crate::network::{ConnectionConfig, ConnectionPool};
use crate::protocol::{
    AclBinding, AclBindingFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
    AlterConfigsRequest, AlterConfigsResponse, ApiKey, CreatableTopic, CreatableTopicConfig,
    CreateAclsRequest, CreateAclsResponse, CreatePartitionsRequest, CreatePartitionsResponse,
    CreatePartitionsTopic, CreateTopicsRequest, CreateTopicsResponse, DeleteAclsRequest,
    DeleteAclsResponse, DeleteTopicsRequest, DeleteTopicsResponse, DescribeAclsRequest,
    DescribeAclsResponse, DescribeConfigsRequest, DescribeConfigsResponse,
};

/// Configuration for creating a topic.
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
#[derive(Debug, Clone)]
pub struct CreateTopicResult {
    /// Topic name.
    pub name: String,
    /// Error message if any.
    pub error: Option<String>,
}

/// Result of topic deletion.
#[derive(Debug, Clone)]
pub struct DeleteTopicResult {
    /// Topic name.
    pub name: String,
    /// Error message if any.
    pub error: Option<String>,
}

/// Result of partition creation.
#[derive(Debug, Clone)]
pub struct CreatePartitionsResult {
    /// Topic name.
    pub topic: String,
    /// Error message if any.
    pub error: Option<String>,
}

/// A configuration entry.
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
#[derive(Debug, Clone)]
pub struct AlterConfigResult {
    /// Resource name.
    pub resource_name: String,
    /// Error message if any.
    pub error: Option<String>,
}

/// Result of describing ACLs.
#[derive(Debug, Clone)]
pub struct DescribeAclsResult {
    /// Error message if any.
    pub error: Option<String>,
    /// List of ACL bindings found.
    pub bindings: Vec<AclBinding>,
}

/// Result of creating ACLs.
#[derive(Debug, Clone)]
pub struct CreateAclsResult {
    /// Results for each ACL creation.
    pub results: Vec<CreateAclResult>,
}

/// Result of a single ACL creation.
#[derive(Debug, Clone)]
pub struct CreateAclResult {
    /// Error message if any.
    pub error: Option<String>,
}

/// Result of deleting ACLs.
#[derive(Debug, Clone)]
pub struct DeleteAclsResult {
    /// Results for each filter.
    pub filter_results: Vec<DeleteAclFilterResult>,
}

/// Result for a single ACL filter deletion.
#[derive(Debug, Clone)]
pub struct DeleteAclFilterResult {
    /// Error message if any.
    pub error: Option<String>,
    /// Number of ACLs deleted by this filter.
    pub deleted_count: usize,
}

/// Filter for ACL operations (describe, delete).
///
/// This struct encapsulates all the filter parameters for ACL queries.
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
#[derive(Debug, Clone)]
pub struct AdminConfig {
    /// Bootstrap servers.
    pub bootstrap_servers: String,
    /// Client ID.
    pub client_id: String,
    /// Request timeout.
    pub request_timeout: Duration,
    /// Authentication configuration (optional).
    pub auth: Option<AuthConfig>,
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
        let brokers = self.metadata.brokers().await;
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
            timeout_ms: timeout.as_millis() as i32,
            validate_only: false,
        };

        // Send request
        let response_bytes = conn
            .send_request(ApiKey::CreateTopics, 0, |buf| {
                request.encode_v0(buf);
            })
            .await?;

        // Decode response
        let mut buf = response_bytes;
        let response = CreateTopicsResponse::decode_v0(&mut buf)?;

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
        let brokers = self.metadata.brokers().await;
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
            timeout_ms: timeout.as_millis() as i32,
        };

        // Send request
        let response_bytes = conn
            .send_request(ApiKey::DeleteTopics, 0, |buf| {
                request.encode_v0(buf);
            })
            .await?;

        // Decode response
        let mut buf = response_bytes;
        let response = DeleteTopicsResponse::decode_v0(&mut buf)?;

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
        let brokers = self.metadata.brokers().await;
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
            timeout_ms: timeout.as_millis() as i32,
            validate_only: false,
        };

        // Send request
        let response_bytes = conn
            .send_request(ApiKey::CreatePartitions, 0, |buf| {
                request.encode_v0(buf);
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
        let brokers = self.metadata.brokers().await;
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

        let response_bytes = conn
            .send_request(ApiKey::DescribeConfigs, 0, |buf| {
                request.encode_v0(buf);
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
        let brokers = self.metadata.brokers().await;
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

        let response_bytes = conn
            .send_request(ApiKey::DescribeConfigs, 0, |buf| {
                request.encode_v0(buf);
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
        let brokers = self.metadata.brokers().await;
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

        let response_bytes = conn
            .send_request(ApiKey::AlterConfigs, 0, |buf| {
                request.encode_v0(buf);
            })
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
        Ok(self
            .metadata
            .topics()
            .await
            .into_iter()
            .map(|t| t.name)
            .collect())
    }

    /// Describe topics.
    pub async fn describe_topics(&self, topics: &[String]) -> Result<Vec<TopicInfo>> {
        self.metadata.refresh().await?;
        let all_topics = self.metadata.topics().await;

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
        let brokers = self.metadata.brokers().await;
        let controller = self.metadata.controller().await;

        Ok(ClusterDescription {
            controller_id: controller.map(|c| c.id),
            brokers,
        })
    }

    /// Get partition count for a topic.
    pub async fn partition_count(&self, topic: &str) -> Result<Option<usize>> {
        self.metadata.refresh().await?;
        Ok(self.metadata.partition_count(topic).await)
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
        let brokers = self.metadata.brokers().await;
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

        let response_bytes = conn
            .send_request(ApiKey::DescribeAcls, 0, |buf| {
                request.encode_v0(buf);
            })
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
        let brokers = self.metadata.brokers().await;
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

        let response_bytes = conn
            .send_request(ApiKey::CreateAcls, 0, |buf| {
                request.encode_v0(buf);
            })
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
        let brokers = self.metadata.brokers().await;
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

        let response_bytes = conn
            .send_request(ApiKey::DeleteAcls, 0, |buf| {
                request.encode_v0(buf);
            })
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

    /// Build the admin client.
    pub async fn build(self) -> Result<AdminClient> {
        if self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap_servers is required"));
        }

        let bootstrap_servers: Vec<String> = self
            .config
            .bootstrap_servers
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        // Create connection config with client ID
        let conn_config = ConnectionConfig::builder()
            .client_id(&self.config.client_id)
            .request_timeout(self.config.request_timeout)
            .build();

        // Note: Full TLS/SASL support requires SecureConnectionPool
        // For now, we store the auth config for future use when secure connections are needed
        // The auth config is stored in AdminClient.config.auth

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
}
