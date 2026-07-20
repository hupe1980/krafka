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
    /// `CreateTopics` is a **controller-only** API. The request is routed to the
    /// current controller and re-issued against a freshly resolved controller if
    /// the broker answers `NOT_CONTROLLER` — see
    /// [`get_controller_connection`](AdminClient::get_controller_connection).
    /// A `NOT_CONTROLLER` that survives every retry is returned as an `Err`, not
    /// as an `Ok` carrying a per-topic error string.
    ///
    /// Returns `Ok(results)` when the RPC succeeds.  **An `Ok` return does not
    /// mean every topic was created** — inspect each
    /// [`CreateTopicResult::error`] for per-topic failures. Every requested
    /// topic is guaranteed to appear in the result: a topic the broker omits
    /// from its response is reported with an explicit error rather than
    /// silently vanishing.
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
        self.check_not_closed()?;
        validate_topic_names(topics.iter().map(|t| t.name.as_str()))?;

        let requested: Vec<String> = topics.iter().map(|t| t.name.clone()).collect();
        let timeout_ms = crate::util::duration_to_millis_i32(timeout);

        let results = self
            .with_controller("CreateTopics", |conn| {
                let topics = &topics;
                async move {
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
                        timeout_ms,
                        validate_only,
                    };

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

                    let mut buf = response_bytes;
                    let response = CreateTopicsResponse::decode_versioned(version, &mut buf)?;

                    // If *any* topic reports a controller move, the whole batch
                    // must be re-sent to the new controller.
                    if let Some(t) = response
                        .topics
                        .iter()
                        .find(|t| super::is_controller_moved(t.error_code))
                    {
                        return Ok(ControllerAttempt::NotController(t.error_code));
                    }

                    Ok(ControllerAttempt::Done(response.topics))
                }
            })
            .await?;

        // Reconcile against the request so a topic the broker did not mention
        // surfaces as an explicit error instead of disappearing.
        let results = reconcile_topic_results(
            &requested,
            results.into_iter().map(|t| {
                let error = if t.error_code.is_ok() {
                    None
                } else {
                    Some(
                        t.error_message
                            .unwrap_or_else(|| format!("{:?}", t.error_code)),
                    )
                };
                (t.name, error)
            }),
            |name, error| CreateTopicResult { name, error },
        );

        let failed = results.iter().filter(|r| r.error.is_some()).count();
        let succeeded = results.len() - failed;
        if validate_only {
            info!(
                "Validated {succeeded}/{} topic(s) ({failed} rejected)",
                results.len()
            );
        } else {
            info!(
                "Created {succeeded}/{} topic(s) ({failed} failed)",
                results.len()
            );
        }
        Ok(results)
    }

    /// Delete topics.
    ///
    /// `DeleteTopics` is a **controller-only** API; see
    /// [`create_topics`](Self::create_topics) for how controller routing and
    /// `NOT_CONTROLLER` retries work.
    ///
    /// Returns `Ok(results)` when the RPC succeeds.  **An `Ok` return does not
    /// mean every topic was deleted** — inspect each
    /// [`DeleteTopicResult::error`] for per-topic failures. Topics omitted by
    /// the broker are reported with an explicit error.
    pub async fn delete_topics(
        &self,
        topics: Vec<String>,
        timeout: Duration,
    ) -> Result<Vec<DeleteTopicResult>> {
        self.check_not_closed()?;
        // H6: reject oversize topic names at ingress so we never reach the
        // panicking `KafkaString::encode` path.
        validate_topic_names(topics.iter().map(String::as_str))?;

        let timeout_ms = crate::util::duration_to_millis_i32(timeout);

        let responses = self
            .with_controller("DeleteTopics", |conn| {
                let topics = &topics;
                async move {
                    // Populate both fields so the correct one is used regardless
                    // of the negotiated version (v1–v5 use topic_names, v6+ use
                    // topics).
                    let request = DeleteTopicsRequest {
                        topic_names: topics.clone(),
                        topics: topics
                            .iter()
                            .map(|name| DeleteTopicState {
                                name: Some(name.clone()),
                                // Null UUID: deletion by topic name, not UUID.
                                topic_id: [0u8; 16],
                            })
                            .collect(),
                        timeout_ms,
                    };

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

                    let mut buf = response_bytes;
                    let response = DeleteTopicsResponse::decode_versioned(version, &mut buf)?;

                    if let Some(r) = response
                        .responses
                        .iter()
                        .find(|r| super::is_controller_moved(r.error_code))
                    {
                        return Ok(ControllerAttempt::NotController(r.error_code));
                    }

                    Ok(ControllerAttempt::Done(response.responses))
                }
            })
            .await?;

        let results = reconcile_topic_results(
            &topics,
            responses.into_iter().map(|r| {
                let error = if r.error_code.is_ok() {
                    None
                } else {
                    Some(
                        r.error_message
                            .unwrap_or_else(|| format!("{:?}", r.error_code)),
                    )
                };
                (r.name.unwrap_or_default(), error)
            }),
            |name, error| DeleteTopicResult { name, error },
        );

        let failed = results.iter().filter(|r| r.error.is_some()).count();
        info!(
            "Deleted {}/{} topic(s) ({failed} failed)",
            results.len() - failed,
            results.len()
        );
        Ok(results)
    }

    /// Increase the number of partitions for a topic.
    ///
    /// Note: Partition count can only be increased, never decreased.
    ///
    /// `CreatePartitions` is a **controller-only** API; see
    /// [`create_topics`](Self::create_topics) for how controller routing and
    /// `NOT_CONTROLLER` retries work.
    ///
    /// # Parameters
    ///
    /// * `topic` — Topic to expand.
    /// * `new_total_count` — The **total** partition count after the increase.
    /// * `timeout` — How long the broker should wait for the change to complete.
    /// * `validate_only` — When `true`, the broker validates the request but
    ///   does **not** create any partitions.
    pub async fn create_partitions(
        &self,
        topic: impl Into<String>,
        new_total_count: i32,
        timeout: Duration,
        validate_only: bool,
    ) -> Result<CreatePartitionsResult> {
        self.check_not_closed()?;
        let topic_name = topic.into();
        // H6: reject oversize topic names at ingress so we never reach the
        // panicking `KafkaString::encode` path.
        validate_topic_name(&topic_name)?;

        let timeout_ms = crate::util::duration_to_millis_i32(timeout);

        let results = self
            .with_controller("CreatePartitions", |conn| {
                let topic_name = &topic_name;
                async move {
                    let request = CreatePartitionsRequest {
                        topics: vec![CreatePartitionsTopic {
                            name: topic_name.clone(),
                            count: new_total_count,
                            assignments: None,
                        }],
                        timeout_ms,
                        validate_only,
                    };

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

                    let mut buf = response_bytes;
                    let response = CreatePartitionsResponse::decode_versioned(version, &mut buf)?;

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
            if validate_only {
                info!("Validated partition increase for topic {topic_name} to {new_total_count}");
            } else {
                info!("Increased partitions for topic {topic_name} to {new_total_count}");
            }
        }
        Ok(result)
    }
}

/// Reconcile a broker's per-topic results against the topics that were
/// requested.
///
/// Kafka is expected to echo one entry per requested topic, but nothing in the
/// protocol enforces it. Without this reconciliation a topic the broker omitted
/// would simply be missing from the returned `Vec`, and a caller that iterates
/// the results and finds no error would conclude the operation succeeded for
/// every topic it asked about.
///
/// Entries the broker returned for topics that were *not* requested are kept —
/// dropping them would hide information — and appended after the requested set.
fn reconcile_topic_results<T>(
    requested: &[String],
    responses: impl Iterator<Item = (String, Option<String>)>,
    build: impl Fn(String, Option<String>) -> T,
) -> Vec<T> {
    let mut by_name: std::collections::HashMap<String, Option<String>> =
        std::collections::HashMap::new();
    let mut extra: Vec<(String, Option<String>)> = Vec::new();

    for (name, error) in responses {
        if requested.contains(&name) {
            by_name.insert(name, error);
        } else {
            extra.push((name, error));
        }
    }

    let mut out: Vec<T> = requested
        .iter()
        .map(|name| {
            let error = by_name
                .remove(name)
                .unwrap_or_else(|| Some("broker returned no result for this topic".to_string()));
            build(name.clone(), error)
        })
        .collect();

    out.extend(extra.into_iter().map(|(name, error)| build(name, error)));
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn results(pairs: &[(&str, Option<&str>)]) -> Vec<(String, Option<String>)> {
        pairs
            .iter()
            .map(|(n, e)| ((*n).to_string(), e.map(str::to_string)))
            .collect()
    }

    fn reconcile(requested: &[&str], responded: &[(&str, Option<&str>)]) -> Vec<CreateTopicResult> {
        let requested: Vec<String> = requested.iter().map(|s| (*s).to_string()).collect();
        reconcile_topic_results(&requested, results(responded).into_iter(), |name, error| {
            CreateTopicResult { name, error }
        })
    }

    #[test]
    fn test_reconcile_passes_through_matching_results() {
        let out = reconcile(&["a", "b"], &[("a", None), ("b", Some("boom"))]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].name, "a");
        assert!(out[0].error.is_none());
        assert_eq!(out[1].name, "b");
        assert_eq!(out[1].error.as_deref(), Some("boom"));
    }

    /// The core defect: a topic the broker omits from its response must not
    /// silently vanish. A caller iterating the results and finding no error
    /// would otherwise believe every requested topic was created.
    #[test]
    fn test_reconcile_reports_topics_the_broker_omitted() {
        let out = reconcile(&["a", "b", "c"], &[("a", None), ("c", None)]);

        assert_eq!(out.len(), 3, "every requested topic must be represented");
        let b = out
            .iter()
            .find(|r| r.name == "b")
            .expect("b must be present");
        assert!(
            b.error.is_some(),
            "an omitted topic must carry an explicit error, not be dropped"
        );
        assert!(b.error.as_deref().unwrap().contains("no result"));
    }

    #[test]
    fn test_reconcile_preserves_request_order() {
        let out = reconcile(&["z", "y", "x"], &[("x", None), ("y", None), ("z", None)]);
        let names: Vec<&str> = out.iter().map(|r| r.name.as_str()).collect();
        assert_eq!(names, vec!["z", "y", "x"]);
    }

    /// Unexpected extras are surfaced rather than discarded — dropping them
    /// would hide information about what the broker actually did.
    #[test]
    fn test_reconcile_keeps_unrequested_extras() {
        let out = reconcile(&["a"], &[("a", None), ("surprise", Some("err"))]);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].name, "a");
        assert_eq!(out[1].name, "surprise");
        assert_eq!(out[1].error.as_deref(), Some("err"));
    }

    #[test]
    fn test_reconcile_empty_response_marks_all_missing() {
        let out = reconcile(&["a", "b"], &[]);
        assert_eq!(out.len(), 2);
        assert!(out.iter().all(|r| r.error.is_some()));
    }

    #[test]
    fn test_reconcile_empty_request_is_empty() {
        let out = reconcile(&[], &[]);
        assert!(out.is_empty());
    }

    #[test]
    fn test_reconcile_builds_delete_results_too() {
        let requested = vec!["t".to_string()];
        let out: Vec<DeleteTopicResult> =
            reconcile_topic_results(&requested, std::iter::empty(), |name, error| {
                DeleteTopicResult { name, error }
            });
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].name, "t");
        assert!(out[0].error.is_some());
    }

    // ── Request construction ──

    #[test]
    fn test_create_topics_request_carries_configs_and_validate_only() {
        let topic = NewTopic::new("orders", 6, 3)
            .unwrap()
            .with_config("cleanup.policy", "compact");

        let request = CreateTopicsRequest {
            topics: vec![CreatableTopic {
                name: topic.name.clone(),
                num_partitions: topic.num_partitions,
                replication_factor: topic.replication_factor,
                assignments: Vec::new(),
                configs: topic
                    .configs
                    .iter()
                    .map(|(k, v)| CreatableTopicConfig {
                        name: k.clone(),
                        value: Some(v.clone()),
                    })
                    .collect(),
            }],
            timeout_ms: 30_000,
            validate_only: true,
        };

        assert!(request.validate_only);
        assert_eq!(request.topics[0].num_partitions, 6);
        assert_eq!(request.topics[0].replication_factor, 3);
        assert_eq!(request.topics[0].configs.len(), 1);

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::CREATE_TOPICS_MAX, &mut buf)
            .expect("CreateTopics must encode at the max supported version");
        assert!(!buf.is_empty());
    }

    /// `create_partitions` must be able to express a dry run; hardcoding
    /// `validate_only: false` made the advertised pre-flight check unusable.
    #[test]
    fn test_create_partitions_request_supports_validate_only() {
        for validate_only in [false, true] {
            let request = CreatePartitionsRequest {
                topics: vec![CreatePartitionsTopic {
                    name: "orders".into(),
                    count: 12,
                    assignments: None,
                }],
                timeout_ms: 5_000,
                validate_only,
            };
            assert_eq!(request.validate_only, validate_only);

            let mut buf = Vec::new();
            request
                .encode_versioned(versions::CREATE_PARTITIONS_MAX, &mut buf)
                .expect("CreatePartitions must encode");
            assert!(!buf.is_empty());
        }
    }

    #[test]
    fn test_delete_topics_request_populates_both_name_forms() {
        let names = vec!["a".to_string(), "b".to_string()];
        let request = DeleteTopicsRequest {
            topic_names: names.clone(),
            topics: names
                .iter()
                .map(|n| DeleteTopicState {
                    name: Some(n.clone()),
                    topic_id: [0u8; 16],
                })
                .collect(),
            timeout_ms: 1_000,
        };

        // v1–v5 read `topic_names`; v6+ read `topics`. Both must be populated
        // so the negotiated version always finds its field.
        assert_eq!(request.topic_names.len(), 2);
        assert_eq!(request.topics.len(), 2);
        assert_eq!(request.topics[0].topic_id, [0u8; 16]);

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::DELETE_TOPICS_MAX, &mut buf)
            .expect("DeleteTopics must encode");
        assert!(!buf.is_empty());
    }
}
