//! AdminClient operation group: topics.

use std::time::Duration;

use tracing::info;

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    ApiKey, CreatableTopic, CreatableTopicConfig, CreatePartitionsRequest,
    CreatePartitionsResponse, CreatePartitionsTopic, CreateTopicsRequest, CreateTopicsResponse,
    DeleteTopicState, DeleteTopicsRequest, DeleteTopicsResponse, VersionedDecode, VersionedEncode,
    validate_topic_name, validate_topic_names, versions,
};

#[allow(clippy::wildcard_imports)]
use super::*;

impl AdminClient {
    /// Create topics.
    ///
    /// Returns `Ok(results)` when the RPC succeeds.  **An `Ok` return does not
    /// mean every topic was created** — inspect each
    /// [`CreateTopicResult::error`] for per-topic failures.
    ///
    /// # Parameters
    ///
    /// * `topics` — Descriptions of the topics to create.
    /// * `timeout` — How long the broker should wait for the creation to complete.
    /// * `validate_only` — When `true`, the broker validates the request but does **not**
    ///   create any topics. Useful for pre-flight checks. Requires CreateTopics v2+
    ///   (Kafka 0.11+); all modern brokers support this.
    pub async fn create_topics(
        &self,
        topics: Vec<NewTopic>,
        timeout: Duration,
        validate_only: bool,
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
            validate_only,
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
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported CreateTopics API version",
                )
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
    ///
    /// Returns `Ok(results)` when the RPC succeeds.  **An `Ok` return does not
    /// mean every topic was deleted** — inspect each
    /// [`DeleteTopicResult::error`] for per-topic failures.
    pub async fn delete_topics(
        &self,
        topics: Vec<String>,
        timeout: Duration,
    ) -> Result<Vec<DeleteTopicResult>> {
        // H6: reject oversize topic names at ingress so we never reach the
        // panicking `KafkaString::encode` path.
        validate_topic_names(topics.iter().map(String::as_str))?;
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
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported DeleteTopics API version",
                )
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
        // H6: reject oversize topic names at ingress so we never reach the
        // panicking `KafkaString::encode` path.
        validate_topic_name(&topic_name)?;
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
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported CreatePartitions API version",
                )
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
}
