//! AdminClient operation group: configs.

use std::collections::HashMap;
use std::time::Duration;

use tracing::info;

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    AlterConfigOp, AlterableConfig, ApiKey, DescribeClusterRequest, DescribeClusterResponse,
    DescribeConfigsResponse, IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse,
    VersionedDecode, VersionedEncode, versions,
};

#[allow(clippy::wildcard_imports)]
use super::*;

impl AdminClient {
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
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported DescribeConfigs API version",
                )
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
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported IncrementalAlterConfigs API version",
                )
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
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported DescribeCluster API version",
                )
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
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::Malformed,
                msg,
            ));
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
}
