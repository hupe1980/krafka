//! Topics: create, delete, add partitions, list, describe.

use std::collections::{HashMap, HashSet};

use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::metadata::TopicInfo;
use crate::protocol::{
    ApiKey, CreatableReplicaAssignment, CreatableTopic, CreatableTopicConfig,
    CreatePartitionsRequest, CreatePartitionsResponse, CreatePartitionsTopic, CreateTopicsRequest,
    CreateTopicsResponse, DeleteTopicState, DeleteTopicsRequest, DeleteTopicsResponse,
    DescribeTopicPartitionsCursor, DescribeTopicPartitionsRequest, DescribeTopicPartitionsResponse,
    validate_topic_name, versions,
};
use crate::{BrokerId, PartitionId};

use super::driver::{Mode, Target, answer, exchange, negotiate};
use super::{AdminClient, validate_topics};

/// Partitions per `DescribeTopicPartitions` page.
const RESPONSE_PARTITION_LIMIT: i32 = 2000;

/// Bound on `DescribeTopicPartitions` pages, so a broker returning a cursor
/// that never advances cannot keep the call looping.
const MAX_DESCRIBE_PAGES: usize = 10_000;

/// A topic to create.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct NewTopic {
    /// Topic name.
    pub name: String,
    /// Number of partitions; `-1` for the broker default.
    pub num_partitions: i32,
    /// Replication factor; `-1` for the broker default.
    pub replication_factor: i16,
    /// Topic configuration overrides.
    pub configs: HashMap<String, String>,
    /// Explicit replica placement: partition index → broker IDs, the first
    /// being the preferred leader. Empty lets the controller place replicas.
    pub replica_assignments: HashMap<i32, Vec<i32>>,
}

impl NewTopic {
    /// A topic with `num_partitions` partitions of `replication_factor`
    /// replicas each; `-1` for either means the broker default.
    ///
    /// # Errors
    ///
    /// Returns an error if `name` is empty or longer than 249 bytes, or if
    /// `num_partitions` or `replication_factor` is zero or less than -1.
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

    /// A topic with an explicit replica placement.
    ///
    /// `assignments` maps partition index to the broker IDs that hold its
    /// replicas, the first being the preferred leader. Partition count and
    /// replication factor follow from the map.
    ///
    /// ```rust,no_run
    /// use krafka::admin::NewTopic;
    /// use std::collections::HashMap;
    ///
    /// # fn example() -> Result<(), krafka::error::KrafkaError> {
    /// let topic = NewTopic::with_replica_assignment(
    ///     "orders",
    ///     HashMap::from([(0, vec![1, 4]), (1, vec![2, 5]), (2, vec![3, 6])]),
    /// )?;
    /// # let _ = topic;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error if `name` is invalid, if `assignments` is empty, if a
    /// partition has no replicas, or if partitions have different replica
    /// counts.
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
            // count.
            num_partitions: -1,
            replication_factor: -1,
            configs: HashMap::new(),
            replica_assignments: assignments,
        })
    }

    /// Add a configuration override.
    #[must_use]
    pub fn with_config(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.configs.insert(key.into(), value.into());
        self
    }

    fn to_wire(&self) -> CreatableTopic {
        let mut assignments: Vec<CreatableReplicaAssignment> = self
            .replica_assignments
            .iter()
            .map(
                |(&partition_index, broker_ids)| CreatableReplicaAssignment {
                    partition_index,
                    broker_ids: broker_ids.clone(),
                },
            )
            .collect();
        // Sorted so the request bytes are deterministic.
        assignments.sort_unstable_by_key(|a| a.partition_index);
        let mut configs: Vec<CreatableTopicConfig> = self
            .configs
            .iter()
            .map(|(name, value)| CreatableTopicConfig {
                name: name.clone(),
                value: Some(value.clone()),
            })
            .collect();
        configs.sort_unstable_by(|a, b| a.name.cmp(&b.name));
        CreatableTopic {
            name: self.name.clone(),
            num_partitions: self.num_partitions,
            replication_factor: self.replication_factor,
            assignments,
            configs,
        }
    }
}

admin_options! {
    /// Options for [`AdminClient::create_topics`].
    CreateTopicsOptions {
        /// Validate the request without creating anything.
        validate_only: bool,
    }
}

admin_options! {
    /// Options for [`AdminClient::delete_topics`].
    DeleteTopicsOptions {}
}

admin_options! {
    /// Options for [`AdminClient::create_partitions`].
    CreatePartitionsOptions {
        /// Validate the request without adding partitions.
        validate_only: bool,
    }
}

admin_options! {
    /// Options for [`AdminClient::list_topics`].
    ListTopicsOptions {
        /// Include internal topics such as `__consumer_offsets`.
        include_internal: bool,
    }
}

admin_options! {
    /// Options for [`AdminClient::describe_topics`].
    DescribeTopicsOptions {}
}

/// A described topic.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct TopicDescription {
    /// Topic name.
    pub name: String,
    /// Topic ID; all zeros when the broker reported none.
    pub topic_id: [u8; 16],
    /// Whether the topic is internal.
    pub is_internal: bool,
    /// Partitions, ordered by index.
    pub partitions: Vec<PartitionDescription>,
    /// Authorized operations bitfield, when the broker reported one.
    pub authorized_operations: Option<i32>,
}

/// A described partition.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct PartitionDescription {
    /// Partition index.
    pub partition: PartitionId,
    /// Leader broker; `None` while the partition has no leader.
    pub leader: Option<BrokerId>,
    /// Leader epoch; `None` when unknown.
    pub leader_epoch: Option<i32>,
    /// Replica broker IDs.
    pub replicas: Vec<BrokerId>,
    /// In-sync replica broker IDs.
    pub isr: Vec<BrokerId>,
    /// Offline replica broker IDs.
    pub offline_replicas: Vec<BrokerId>,
    /// Eligible leader replicas (KIP-966); `None` when the broker did not
    /// report them.
    pub eligible_leader_replicas: Option<Vec<BrokerId>>,
    /// Last known eligible leader replicas (KIP-966).
    pub last_known_elr: Option<Vec<BrokerId>>,
}

impl TopicDescription {
    fn from_metadata(info: &TopicInfo) -> Self {
        let mut partitions: Vec<PartitionDescription> = info
            .partitions_iter()
            .map(|p| PartitionDescription {
                partition: p.partition,
                leader: (p.leader >= 0).then_some(p.leader),
                leader_epoch: (p.leader_epoch >= 0).then_some(p.leader_epoch),
                replicas: p.replicas.clone(),
                isr: p.isr.clone(),
                offline_replicas: p.offline_replicas.clone(),
                eligible_leader_replicas: None,
                last_known_elr: None,
            })
            .collect();
        partitions.sort_unstable_by_key(|p| p.partition);
        Self {
            name: info.name.clone(),
            topic_id: info.topic_id,
            is_internal: info.is_internal,
            partitions,
            authorized_operations: None,
        }
    }
}

/// Reject a request naming the same item twice.
fn unique<'a>(what: &str, names: impl IntoIterator<Item = &'a str>) -> Result<()> {
    let mut seen = HashSet::new();
    for name in names {
        if !seen.insert(name) {
            return Err(KrafkaError::config(format!(
                "{what} '{name}' appears more than once"
            )));
        }
    }
    Ok(())
}

impl AdminClient {
    /// Create topics (controller).
    ///
    /// Returns a result per topic name. An existing topic is
    /// `Err(KrafkaError::Broker { code: ErrorCode::TopicAlreadyExists, .. })`.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client or a topic named twice.
    pub async fn create_topics(
        &self,
        topics: impl IntoIterator<Item = NewTopic>,
        options: CreateTopicsOptions,
    ) -> Result<HashMap<String, Result<()>>> {
        let topics: HashMap<String, NewTopic> = {
            let list: Vec<NewTopic> = topics.into_iter().collect();
            unique("topic", list.iter().map(|t| t.name.as_str()))?;
            list.into_iter().map(|t| (t.name.clone(), t)).collect()
        };
        let call = self.call("CreateTopics", Mode::Write, options.timeout)?;
        let validate_only = options.validate_only;
        let topics = &topics;
        let call_ref = &call;
        Ok(call
            .fan_out(
                topics.keys().cloned().collect(),
                |_| Target::Controller,
                |conn, names| async move {
                    let request = CreateTopicsRequest {
                        topics: names.iter().map(|n| topics[n].to_wire()).collect(),
                        timeout_ms: call_ref.remaining_ms(),
                        validate_only,
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::CreateTopics,
                        versions::CREATE_TOPICS_MIN,
                        versions::CREATE_TOPICS_MAX,
                    )?;
                    let response: CreateTopicsResponse =
                        exchange(&conn, ApiKey::CreateTopics, version, &request).await?;
                    Ok(response
                        .topics
                        .into_iter()
                        .map(|t| (t.name, answer(t.error_code, t.error_message)))
                        .collect())
                },
            )
            .await)
    }

    /// Delete topics (controller).
    ///
    /// Returns a result per topic name.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client or an invalid topic name.
    pub async fn delete_topics<I, S>(
        &self,
        topics: I,
        options: DeleteTopicsOptions,
    ) -> Result<HashMap<String, Result<()>>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let names: Vec<String> = topics.into_iter().map(|s| s.as_ref().to_string()).collect();
        validate_topics(names.iter().map(String::as_str))?;
        unique("topic", names.iter().map(String::as_str))?;
        let call = self.call("DeleteTopics", Mode::Write, options.timeout)?;
        let call_ref = &call;
        Ok(call
            .fan_out(
                names,
                |_| Target::Controller,
                |conn, names| async move {
                    // v1–v5 read `topic_names`, v6+ read `topics`.
                    let request = DeleteTopicsRequest {
                        topic_names: names.clone(),
                        topics: names
                            .iter()
                            .map(|name| DeleteTopicState {
                                name: Some(name.clone()),
                                topic_id: [0; 16],
                            })
                            .collect(),
                        timeout_ms: call_ref.remaining_ms(),
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::DeleteTopics,
                        versions::DELETE_TOPICS_MIN,
                        versions::DELETE_TOPICS_MAX,
                    )?;
                    let response: DeleteTopicsResponse =
                        exchange(&conn, ApiKey::DeleteTopics, version, &request).await?;
                    Ok(response
                        .responses
                        .into_iter()
                        .map(|r| {
                            (
                                r.name.unwrap_or_default(),
                                answer(r.error_code, r.error_message),
                            )
                        })
                        .collect())
                },
            )
            .await)
    }

    /// Raise topics' partition counts to the given **totals** (controller).
    ///
    /// Partition counts only grow. Returns a result per topic.
    ///
    /// ```rust,no_run
    /// # use krafka::admin::{AdminClient, CreatePartitionsOptions};
    /// # async fn example(admin: &AdminClient) -> Result<(), krafka::error::KrafkaError> {
    /// let results = admin
    ///     .create_partitions([("orders", 12)], CreatePartitionsOptions::default())
    ///     .await?;
    /// # let _ = results;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// The call fails for a closed client, an invalid topic name, or a topic
    /// named twice.
    pub async fn create_partitions<I, S>(
        &self,
        totals: I,
        options: CreatePartitionsOptions,
    ) -> Result<HashMap<String, Result<()>>>
    where
        I: IntoIterator<Item = (S, i32)>,
        S: AsRef<str>,
    {
        let totals: Vec<(String, i32)> = totals
            .into_iter()
            .map(|(name, count)| (name.as_ref().to_string(), count))
            .collect();
        validate_topics(totals.iter().map(|(name, _)| name.as_str()))?;
        unique("topic", totals.iter().map(|(name, _)| name.as_str()))?;
        let totals: HashMap<String, i32> = totals.into_iter().collect();
        let call = self.call("CreatePartitions", Mode::Write, options.timeout)?;
        let validate_only = options.validate_only;
        let totals = &totals;
        let call_ref = &call;
        Ok(call
            .fan_out(
                totals.keys().cloned().collect(),
                |_| Target::Controller,
                |conn, names| async move {
                    let request = CreatePartitionsRequest {
                        topics: names
                            .iter()
                            .map(|name| CreatePartitionsTopic {
                                name: name.clone(),
                                count: totals[name],
                                assignments: None,
                            })
                            .collect(),
                        timeout_ms: call_ref.remaining_ms(),
                        validate_only,
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::CreatePartitions,
                        versions::CREATE_PARTITIONS_MIN,
                        versions::CREATE_PARTITIONS_MAX,
                    )?;
                    let response: CreatePartitionsResponse =
                        exchange(&conn, ApiKey::CreatePartitions, version, &request).await?;
                    Ok(response
                        .results
                        .into_iter()
                        .map(|r| (r.name, answer(r.error_code, r.error_message)))
                        .collect())
                },
            )
            .await)
    }

    /// List the cluster's topic names from a fresh metadata fetch.
    ///
    /// # Errors
    ///
    /// Fails when the metadata fetch fails or the deadline passes.
    pub async fn list_topics(&self, options: ListTopicsOptions) -> Result<Vec<String>> {
        let call = self.call("ListTopics", Mode::Read, options.timeout)?;
        tokio::time::timeout(call.remaining(), self.metadata.refresh())
            .await
            .map_err(|_| KrafkaError::timeout("ListTopics"))??;
        let mut names: Vec<String> = self
            .metadata
            .topics_arc()
            .into_iter()
            .filter(|t| options.include_internal || !t.is_internal)
            .map(|t| t.name.clone())
            .collect();
        names.sort_unstable();
        Ok(names)
    }

    /// Describe topics: partitions, leaders, replicas, ISR and, on brokers
    /// with `DescribeTopicPartitions` (KIP-966), eligible leader replicas.
    ///
    /// Returns a result per topic; a missing topic is
    /// `Err(KrafkaError::Broker { code: ErrorCode::UnknownTopicOrPartition, .. })`.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client or an invalid topic name.
    pub async fn describe_topics<I, S>(
        &self,
        topics: I,
        options: DescribeTopicsOptions,
    ) -> Result<HashMap<String, Result<TopicDescription>>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut names: Vec<String> = topics.into_iter().map(|s| s.as_ref().to_string()).collect();
        validate_topics(names.iter().map(String::as_str))?;
        names.sort_unstable();
        names.dedup();
        let call = self.call("DescribeTopics", Mode::Read, options.timeout)?;
        Ok(call
            .fan_out(
                names,
                |_| Target::AnyBroker,
                |conn, names| async move {
                    match conn.negotiate_api_version(
                        ApiKey::DescribeTopicPartitions,
                        versions::DESCRIBE_TOPIC_PARTITIONS_MAX,
                        versions::DESCRIBE_TOPIC_PARTITIONS_MIN,
                    ) {
                        Some(version) => describe_topic_partitions(&conn, version, names).await,
                        None => self.describe_topics_from_metadata(names).await,
                    }
                },
            )
            .await)
    }

    /// Describe topics from a metadata fetch, for brokers without
    /// `DescribeTopicPartitions`.
    async fn describe_topics_from_metadata(
        &self,
        names: Vec<String>,
    ) -> Result<Vec<(String, Result<TopicDescription>)>> {
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        self.metadata.force_refresh(Some(&refs)).await?;
        Ok(names
            .into_iter()
            .map(|name| {
                let result = match self.metadata.topic_arc(&name) {
                    Some(info) => Ok(TopicDescription::from_metadata(&info)),
                    None => Err(KrafkaError::broker(
                        self.metadata
                            .topic_error(&name)
                            .unwrap_or(ErrorCode::UnknownTopicOrPartition),
                        format!("topic {name} is not in the cluster metadata"),
                    )),
                };
                (name, result)
            })
            .collect())
    }
}

/// Describe `names` with `DescribeTopicPartitions`, following the cursor
/// across pages.
async fn describe_topic_partitions(
    conn: &crate::network::BrokerConnection,
    version: i16,
    names: Vec<String>,
) -> Result<Vec<(String, Result<TopicDescription>)>> {
    let mut described: HashMap<String, Result<TopicDescription>> = HashMap::new();
    let mut cursor: Option<DescribeTopicPartitionsCursor> = None;

    for _ in 0..MAX_DESCRIBE_PAGES {
        let request = DescribeTopicPartitionsRequest {
            topics: names.clone(),
            response_partition_limit: RESPONSE_PARTITION_LIMIT,
            cursor: cursor.clone(),
        };
        let response: DescribeTopicPartitionsResponse =
            exchange(conn, ApiKey::DescribeTopicPartitions, version, &request).await?;

        for t in response.topics {
            let Some(name) = t.name else { continue };
            if !t.error_code.is_ok() {
                described.insert(
                    name,
                    Err(KrafkaError::broker(
                        t.error_code,
                        format!("{:?}", t.error_code),
                    )),
                );
                continue;
            }
            let entry = described.entry(name.clone()).or_insert_with(|| {
                Ok(TopicDescription {
                    name,
                    topic_id: t.topic_id,
                    is_internal: t.is_internal,
                    partitions: Vec::new(),
                    authorized_operations: (t.topic_authorized_operations != i32::MIN)
                        .then_some(t.topic_authorized_operations),
                })
            });
            if let Ok(description) = entry {
                description
                    .partitions
                    .extend(t.partitions.into_iter().map(|p| PartitionDescription {
                        partition: p.partition_index,
                        leader: (p.error_code.is_ok() && p.leader_id >= 0).then_some(p.leader_id),
                        leader_epoch: (p.leader_epoch >= 0).then_some(p.leader_epoch),
                        replicas: p.replica_nodes,
                        isr: p.isr_nodes,
                        offline_replicas: p.offline_replicas,
                        eligible_leader_replicas: p.eligible_leader_replicas,
                        last_known_elr: p.last_known_elr,
                    }));
            }
        }

        match response.next_cursor {
            None => {
                return Ok(names
                    .into_iter()
                    .map(|name| {
                        let result = described.remove(&name).unwrap_or_else(|| {
                            Err(KrafkaError::broker(
                                ErrorCode::UnknownTopicOrPartition,
                                format!("topic {name} was not described"),
                            ))
                        });
                        let result = result.map(|mut d| {
                            d.partitions.sort_unstable_by_key(|p| p.partition);
                            d
                        });
                        (name, result)
                    })
                    .collect());
            }
            Some(next) => {
                if cursor.as_ref().is_some_and(|prev| {
                    prev.topic_name == next.topic_name
                        && prev.partition_index == next.partition_index
                }) {
                    break;
                }
                cursor = Some(next);
            }
        }
    }

    Err(KrafkaError::protocol_kind(
        ProtocolErrorKind::Malformed,
        "DescribeTopicPartitions returned a cursor that does not advance",
    ))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::protocol::VersionedEncode;

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
    fn test_new_topic_name_validation_rejects_empty_and_oversize() {
        assert!(NewTopic::new("", 1, 1).is_err());
        assert!(NewTopic::new("x".repeat(250), 1, 1).is_err());
        assert!(NewTopic::new("x".repeat(249), 1, 1).is_ok());
    }

    #[test]
    fn replica_assignment_is_expressible_and_validated() {
        let topic = NewTopic::with_replica_assignment(
            "orders",
            HashMap::from([(1, vec![2, 5]), (0, vec![1, 4])]),
        )
        .expect("a uniform assignment is valid");
        assert_eq!(topic.num_partitions, -1);
        assert_eq!(topic.replication_factor, -1);
        let wire = topic.to_wire();
        assert_eq!(
            wire.assignments
                .iter()
                .map(|a| a.partition_index)
                .collect::<Vec<_>>(),
            vec![0, 1],
            "assignments are sorted so the request is deterministic"
        );

        let ragged = NewTopic::with_replica_assignment(
            "orders",
            HashMap::from([(0, vec![1, 4]), (1, vec![2])]),
        )
        .expect_err("a ragged replication factor must be rejected");
        assert!(ragged.to_string().contains("replication factor"));
        let empty = NewTopic::with_replica_assignment("orders", HashMap::from([(0, Vec::new())]))
            .expect_err("a partition with no replicas must be rejected");
        assert!(empty.to_string().contains("partition 0"));
        NewTopic::with_replica_assignment("orders", HashMap::new())
            .expect_err("an empty assignment names no partitions");
    }

    #[test]
    fn create_topics_request_encodes() {
        let topic = NewTopic::new("orders", 6, 3)
            .unwrap()
            .with_config("cleanup.policy", "compact");
        let request = CreateTopicsRequest {
            topics: vec![topic.to_wire()],
            timeout_ms: 30_000,
            validate_only: true,
        };
        let mut buf = Vec::new();
        request
            .encode_versioned(versions::CREATE_TOPICS_MAX, &mut buf)
            .unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn a_name_given_twice_is_an_invalid_argument() {
        assert!(unique("topic", ["a", "b"]).is_ok());
        let err = unique("topic", ["a", "b", "a"]).unwrap_err();
        assert!(matches!(err, KrafkaError::Config { .. }));
    }
}
