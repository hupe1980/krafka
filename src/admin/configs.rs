//! AdminClient operation group: configs.

use std::collections::HashMap;
use std::time::Duration;

use tracing::{info, warn};

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    AlterConfigOp, AlterableConfig, ApiKey, DescribeClusterRequest, DescribeClusterResponse,
    DescribeConfigsResponse, IncrementalAlterConfigsRequest, IncrementalAlterConfigsResponse,
    VersionedDecode, VersionedEncode, versions,
};

#[allow(clippy::wildcard_imports)]
use super::*;

impl AdminClient {
    /// Describe configuration for one or more resources (topics, brokers, etc.),
    /// preserving per-resource errors and attribution.
    ///
    /// Uses DescribeConfigs (API Key 32). Build a [`DescribeConfigsRequest`]
    /// via its convenience constructors (`for_topic`, `for_broker`) or manually
    /// populate the `resources` field for multi-resource queries.
    ///
    /// Each returned [`DescribeConfigsResourceResult`] carries the resource it
    /// describes and that resource's error, if any. Use this rather than
    /// [`describe_configs`](Self::describe_configs) whenever more than one
    /// resource is requested, or whenever an authorization failure must be
    /// distinguishable from "this resource has no configs".
    pub async fn describe_configs_per_resource(
        &self,
        request: DescribeConfigsRequest,
    ) -> Result<Vec<DescribeConfigsResourceResult>> {
        self.check_not_closed()?;
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

        let results = response
            .results
            .into_iter()
            .map(|r| {
                if !r.error_code.is_ok() {
                    warn!(
                        resource = %r.resource_name,
                        "DescribeConfigs failed for resource: {:?}",
                        r.error_code
                    );
                }
                DescribeConfigsResourceResult {
                    resource_type: r.resource_type,
                    resource_name: r.resource_name,
                    error_code: r.error_code,
                    error: if r.error_code.is_ok() {
                        None
                    } else {
                        Some(
                            r.error_message
                                .unwrap_or_else(|| format!("{:?}", r.error_code)),
                        )
                    },
                    configs: r
                        .configs
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
                        .collect(),
                }
            })
            .collect();

        Ok(results)
    }

    /// Describe configuration for one or more resources, flattening every
    /// resource's entries into a single list.
    ///
    /// # Errors
    ///
    /// Unlike the previous behaviour, a per-resource error is **not** silently
    /// swallowed: if any requested resource failed (for example with
    /// `TOPIC_AUTHORIZATION_FAILED`), this returns that error rather than an
    /// empty or partial list that is indistinguishable from "no configs".
    ///
    /// Use [`describe_configs_per_resource`](Self::describe_configs_per_resource)
    /// to inspect partial results across a multi-resource request.
    pub async fn describe_configs(
        &self,
        request: DescribeConfigsRequest,
    ) -> Result<Vec<ConfigEntry>> {
        let results = self.describe_configs_per_resource(request).await?;

        if let Some(failed) = results.iter().find(|r| !r.error_code.is_ok()) {
            return Err(KrafkaError::broker(
                failed.error_code,
                format!(
                    "DescribeConfigs failed for {:?} '{}': {}",
                    failed.resource_type,
                    failed.resource_name,
                    failed.error.as_deref().unwrap_or("unknown error")
                ),
            ));
        }

        Ok(results.into_iter().flat_map(|r| r.configs).collect())
    }

    /// Alter configuration for a topic.
    ///
    /// Uses IncrementalAlterConfigs (API Key 44) to set individual config keys
    /// without replacing the entire config. Each key-value pair is applied as a
    /// SET operation.
    ///
    /// `IncrementalAlterConfigs` is a **controller-only** API: the request is
    /// routed to the current controller and re-issued against a freshly
    /// resolved controller on `NOT_CONTROLLER`.
    pub async fn alter_topic_config(
        &self,
        topic: &str,
        configs: HashMap<String, String>,
    ) -> Result<AlterConfigResult> {
        self.check_not_closed()?;

        let alterations: Vec<AlterableConfig> = configs
            .into_iter()
            .map(|(name, value)| AlterableConfig {
                name,
                config_operation: AlterConfigOp::Set,
                value: Some(value),
            })
            .collect();

        let results = self
            .with_controller("IncrementalAlterConfigs", |conn| {
                let alterations = &alterations;
                async move {
                    let request =
                        IncrementalAlterConfigsRequest::for_topic(topic, alterations.clone());

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
                    let response =
                        IncrementalAlterConfigsResponse::decode_versioned(version, &mut buf)?;

                    if let Some(r) = response
                        .results
                        .iter()
                        .find(|r| super::is_controller_moved(r.error_code))
                    {
                        return Ok(ControllerAttempt::NotController(r.error_code));
                    }

                    Ok(ControllerAttempt::Done(response.results))
                }
            })
            .await?;

        let result = results
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
    ///
    /// Forces a metadata refresh first, so this is safe to call immediately
    /// after [`create_topics`](Self::create_topics): the refresh waits out the
    /// `retry.backoff.ms` rate limiter rather than returning a stale cache as
    /// though it were current.
    pub async fn list_topics(&self) -> Result<Vec<String>> {
        self.check_not_closed()?;
        self.refresh_metadata_authoritative().await?;
        Ok(self
            .metadata
            .topics_arc()
            .into_iter()
            .map(|t| t.name.clone())
            .collect())
    }

    /// Force a metadata refresh that is guaranteed to have reached a broker.
    ///
    /// [`ClusterMetadata::refresh`](crate::metadata::ClusterMetadata::refresh)
    /// can be suppressed by the `retry.backoff.ms` rate limiter. For
    /// read-after-write callers (create a topic, then list topics) a suppressed
    /// refresh means reading a cache that predates the write. This waits out
    /// the backoff and re-issues.
    async fn refresh_metadata_authoritative(&self) -> Result<()> {
        use crate::metadata::RefreshOutcome;

        match self.metadata.refresh_for_topics_outcome(None).await? {
            RefreshOutcome::Refreshed | RefreshOutcome::AlreadyFresh => Ok(()),
            RefreshOutcome::RateLimited(remaining) => {
                tokio::time::sleep(remaining).await;
                self.metadata.refresh().await
            }
        }
    }

    /// Fetch specific config keys for a topic.
    ///
    /// A convenience wrapper around [`describe_configs`](Self::describe_configs)
    /// for the common case of reading a small set of well-known topic-level keys.
    /// Pass an empty `keys` slice to fetch all config entries for the topic.
    ///
    /// Returns a map of config key → [`ConfigValue`].
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// let cfg = admin.topic_config("my-topic", &["retention.ms", "cleanup.policy"]).await?;
    /// if let Some(krafka::admin::ConfigValue::Value(v)) = cfg.get("retention.ms") {
    ///     println!("retention.ms = {v}");
    /// }
    /// ```
    pub async fn topic_config(
        &self,
        topic: &str,
        keys: &[&str],
    ) -> Result<HashMap<String, super::ConfigValue>> {
        let request = DescribeConfigsRequest {
            resources: vec![super::DescribeConfigsResource {
                resource_type: super::ConfigResourceType::Topic,
                resource_name: topic.to_string(),
                config_names: if keys.is_empty() {
                    None
                } else {
                    Some(keys.iter().map(|k| k.to_string()).collect())
                },
            }],
            include_synonyms: false,
            include_documentation: false,
        };
        let entries = self.describe_configs(request).await?;
        Ok(entries
            .into_iter()
            .map(|e| {
                let cv = e.config_value();
                (e.name, cv)
            })
            .collect())
    }

    /// Describe topics by name, returning a map of topic name → [`TopicInfo`].
    ///
    /// Topics not found in cluster metadata are absent from the returned map;
    /// callers can detect missing topics by comparing request and response key sets.
    pub async fn describe_topics<S: AsRef<str>>(
        &self,
        topics: &[S],
    ) -> Result<HashMap<String, Arc<TopicInfo>>> {
        self.check_not_closed()?;
        self.refresh_metadata_authoritative().await?;
        // Use O(1) per-topic metadata look-up instead of a full Vec scan.
        let result = topics
            .iter()
            .filter_map(|name| {
                self.metadata
                    .topic_arc(name.as_ref())
                    .map(|info| (name.as_ref().to_owned(), info))
            })
            .collect();
        Ok(result)
    }

    /// Describe a single topic by name.
    ///
    /// Returns `None` if the topic does not exist or is not visible in cluster metadata.
    pub async fn describe_topic(&self, topic: &str) -> Result<Option<Arc<TopicInfo>>> {
        self.check_not_closed()?;
        self.refresh_metadata_authoritative().await?;
        Ok(self.metadata.topic_arc(topic))
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
            // Preserve the broker's error code so `ErrorCode::is_retriable()`
            // governs retries. Wrapping it in a `Malformed` protocol error
            // discarded the code into a string *and* made every failure look
            // retriable.
            return Err(KrafkaError::broker(response.error_code, msg));
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
        self.refresh_metadata_authoritative().await?;
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn test_describe_configs_request_encodes_named_keys_and_all_keys() {
        // Specific keys.
        let request = DescribeConfigsRequest {
            resources: vec![DescribeConfigsResource {
                resource_type: ConfigResourceType::Topic,
                resource_name: "orders".into(),
                config_names: Some(vec!["retention.ms".into(), "cleanup.policy".into()]),
            }],
            include_synonyms: true,
            include_documentation: true,
        };
        assert_eq!(request.resources[0].config_names.as_ref().unwrap().len(), 2);
        let mut buf = Vec::new();
        request
            .encode_versioned(versions::DESCRIBE_CONFIGS_MAX, &mut buf)
            .expect("DescribeConfigs must encode");
        assert!(!buf.is_empty());

        // `None` means "all keys" — a null array, not an empty one, which would
        // ask the broker for zero configs.
        let all = DescribeConfigsRequest {
            resources: vec![DescribeConfigsResource {
                resource_type: ConfigResourceType::Topic,
                resource_name: "orders".into(),
                config_names: None,
            }],
            include_synonyms: false,
            include_documentation: false,
        };
        assert!(all.resources[0].config_names.is_none());
        let mut buf = Vec::new();
        all.encode_versioned(versions::DESCRIBE_CONFIGS_MAX, &mut buf)
            .expect("DescribeConfigs(all) must encode");
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_incremental_alter_configs_uses_set_operations() {
        let request = IncrementalAlterConfigsRequest::for_topic(
            "orders",
            vec![AlterableConfig {
                name: "retention.ms".into(),
                config_operation: AlterConfigOp::Set,
                value: Some("86400000".into()),
            }],
        );

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::INCREMENTAL_ALTER_CONFIGS_MAX, &mut buf)
            .expect("IncrementalAlterConfigs must encode");
        assert!(!buf.is_empty());
    }

    /// The flattening `describe_configs` must surface a per-resource failure as
    /// an `Err`. Returning `Ok(vec![])` made `TOPIC_AUTHORIZATION_FAILED`
    /// indistinguishable from "this topic has no config overrides".
    #[test]
    fn test_failed_resource_produces_a_broker_error_not_an_empty_list() {
        let results = [
            DescribeConfigsResourceResult {
                resource_type: ConfigResourceType::Topic,
                resource_name: "ok".into(),
                error_code: ErrorCode::None,
                error: None,
                configs: vec![],
            },
            DescribeConfigsResourceResult {
                resource_type: ConfigResourceType::Topic,
                resource_name: "denied".into(),
                error_code: ErrorCode::TopicAuthorizationFailed,
                error: Some("TopicAuthorizationFailed".into()),
                configs: vec![],
            },
        ];

        let failed = results
            .iter()
            .find(|r| !r.error_code.is_ok())
            .expect("the denied resource must be detected");
        assert_eq!(failed.resource_name, "denied");
        assert_eq!(failed.error_code, ErrorCode::TopicAuthorizationFailed);

        // And the resulting error keeps the code, so retry policy is correct:
        // an authorization failure is terminal.
        let err = KrafkaError::broker(failed.error_code, "denied");
        assert!(!err.is_retriable());
    }

    #[test]
    fn test_all_ok_resources_flatten_into_one_entry_list() {
        let results = vec![
            DescribeConfigsResourceResult {
                resource_type: ConfigResourceType::Topic,
                resource_name: "a".into(),
                error_code: ErrorCode::None,
                error: None,
                configs: vec![ConfigEntry {
                    name: "k1".into(),
                    value: Some("v1".into()),
                    read_only: false,
                    is_default: false,
                    is_sensitive: false,
                    config_source: -1,
                    synonyms: vec![],
                    config_type: 0,
                    documentation: None,
                }],
            },
            DescribeConfigsResourceResult {
                resource_type: ConfigResourceType::Topic,
                resource_name: "b".into(),
                error_code: ErrorCode::None,
                error: None,
                configs: vec![ConfigEntry {
                    name: "k2".into(),
                    value: Some("v2".into()),
                    read_only: false,
                    is_default: false,
                    is_sensitive: false,
                    config_source: -1,
                    synonyms: vec![],
                    config_type: 0,
                    documentation: None,
                }],
            },
        ];

        assert!(results.iter().all(DescribeConfigsResourceResult::is_ok));
        let flattened: Vec<ConfigEntry> = results.into_iter().flat_map(|r| r.configs).collect();
        assert_eq!(flattened.len(), 2);
    }

    #[test]
    fn test_describe_cluster_error_keeps_its_broker_code() {
        // DescribeCluster failures must not be laundered into a `Malformed`
        // protocol error, which reads as retriable regardless of the cause.
        let err = KrafkaError::broker(ErrorCode::ClusterAuthorizationFailed, "denied");
        match err {
            KrafkaError::Broker { code, .. } => {
                assert_eq!(code, ErrorCode::ClusterAuthorizationFailed);
                assert!(!code.is_retriable());
            }
            other => panic!("expected Broker error, got {other:?}"),
        }
    }
}
