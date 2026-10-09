//! Replicas and leaders: log directories, leader election, reassignments.

use std::collections::{BTreeMap, HashMap};

use crate::BrokerId;
use crate::consumer::TopicPartition;
use crate::error::{ErrorCode, KrafkaError, Result};
use crate::protocol::{
    AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse, AlterReplicaLogDir,
    AlterReplicaLogDirTopic, AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse, ApiKey,
    DescribableLogDirTopic, DescribeLogDirsRequest, DescribeLogDirsResponse, ElectLeadersRequest,
    ElectLeadersResponse, ElectLeadersTopicPartitions, ElectionType,
    ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse,
    ListPartitionReassignmentsTopic, ReassignablePartition, ReassignableTopic, versions,
};

use super::driver::{Mode, Target, answer, exchange, negotiate};
use super::{AdminClient, validate_topics};

/// One replica of a partition: the partition and the broker holding it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub struct TopicPartitionReplica {
    /// Topic name.
    pub topic: String,
    /// Partition index.
    pub partition: i32,
    /// Broker holding the replica.
    pub broker_id: BrokerId,
}

impl TopicPartitionReplica {
    /// The replica of `topic`-`partition` on `broker_id`.
    pub fn new(topic: impl Into<String>, partition: i32, broker_id: BrokerId) -> Self {
        Self {
            topic: topic.into(),
            partition,
            broker_id,
        }
    }
}

/// A broker log directory.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct LogDirDescription {
    /// Replicas stored in the directory.
    pub replicas: HashMap<TopicPartition, LogDirReplica>,
    /// Volume size in bytes, when the broker reports it (v4+).
    pub total_bytes: Option<i64>,
    /// Usable bytes on the volume, when the broker reports it (v4+).
    pub usable_bytes: Option<i64>,
    /// Whether the directory is cordoned and takes no new replicas (KIP-1066,
    /// v5+); `false` on older brokers.
    pub is_cordoned: bool,
}

/// A replica in a [`LogDirDescription`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogDirReplica {
    /// Log size in bytes.
    pub size: i64,
    /// Offset lag behind the high watermark.
    pub offset_lag: i64,
    /// Whether this is the future replica of a move in progress.
    pub is_future: bool,
}

/// An ongoing reassignment.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionReassignment {
    /// Current replica set.
    pub replicas: Vec<BrokerId>,
    /// Replicas being added.
    pub adding_replicas: Vec<BrokerId>,
    /// Replicas being removed.
    pub removing_replicas: Vec<BrokerId>,
}

admin_options! {
    /// Options for [`AdminClient::describe_log_dirs`].
    DescribeLogDirsOptions {}
    optional {
        /// Only these partitions. Default: every partition.
        partitions: Vec<TopicPartition>,
    }
}

admin_options! {
    /// Options for [`AdminClient::elect_leaders`].
    ElectLeadersOptions {}
}

/// Options for [`AdminClient::alter_partition_reassignments`].
#[derive(Debug, Clone)]
#[must_use]
pub struct AlterPartitionReassignmentsOptions {
    timeout: Option<std::time::Duration>,
    allow_replication_factor_change: bool,
}

impl Default for AlterPartitionReassignmentsOptions {
    fn default() -> Self {
        Self {
            timeout: None,
            allow_replication_factor_change: true,
        }
    }
}

impl AlterPartitionReassignmentsOptions {
    /// Bound the whole call. Default: the client's
    /// [`default_api_timeout`](crate::admin::AdminClient::default_api_timeout).
    pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Whether a target replica set may change a partition's replication
    /// factor. Default: `true`. `false` needs `AlterPartitionReassignments`
    /// v1 (Kafka 4.1); against an older controller the call fails rather than
    /// send a request that would permit the change.
    pub fn allow_replication_factor_change(mut self, allow: bool) -> Self {
        self.allow_replication_factor_change = allow;
        self
    }
}

admin_options! {
    /// Options for [`AdminClient::list_partition_reassignments`].
    ListPartitionReassignmentsOptions {}
    optional {
        /// Only these partitions. Default: every partition with an ongoing
        /// reassignment.
        partitions: Vec<TopicPartition>,
    }
}

admin_options! {
    /// Options for [`AdminClient::alter_replica_log_dirs`].
    AlterReplicaLogDirsOptions {}
}

fn group_partitions<'a>(
    partitions: impl IntoIterator<Item = &'a TopicPartition>,
) -> BTreeMap<String, Vec<i32>> {
    let mut topics: BTreeMap<String, Vec<i32>> = BTreeMap::new();
    for tp in partitions {
        topics
            .entry(tp.topic.clone())
            .or_default()
            .push(tp.partition);
    }
    for partitions in topics.values_mut() {
        partitions.sort_unstable();
        partitions.dedup();
    }
    topics
}

impl AdminClient {
    /// Describe the log directories of the given brokers, each at that
    /// broker. Returns a result per broker, and inside it a result per
    /// directory path.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client or an invalid topic name.
    pub async fn describe_log_dirs(
        &self,
        brokers: impl IntoIterator<Item = BrokerId>,
        options: DescribeLogDirsOptions,
    ) -> Result<HashMap<BrokerId, Result<HashMap<String, Result<LogDirDescription>>>>> {
        let mut brokers: Vec<BrokerId> = brokers.into_iter().collect();
        brokers.sort_unstable();
        brokers.dedup();
        if let Some(partitions) = &options.partitions {
            validate_topics(partitions.iter().map(|tp| tp.topic.as_str()))?;
        }
        let call = self.call("DescribeLogDirs", Mode::Read, options.timeout)?;
        let request = match &options.partitions {
            None => DescribeLogDirsRequest::all(),
            Some(partitions) => DescribeLogDirsRequest::for_topics(
                group_partitions(partitions)
                    .into_iter()
                    .map(|(topic, partitions)| DescribableLogDirTopic { topic, partitions })
                    .collect(),
            ),
        };
        let request = &request;
        Ok(call
            .fan_out(
                brokers,
                |id| Target::Broker(*id),
                |conn, ids| async move {
                    let version = negotiate(
                        &conn,
                        ApiKey::DescribeLogDirs,
                        versions::DESCRIBE_LOG_DIRS_MIN,
                        versions::DESCRIBE_LOG_DIRS_MAX,
                    )?;
                    let response: DescribeLogDirsResponse =
                        exchange(&conn, ApiKey::DescribeLogDirs, version, request).await?;
                    let dirs = answer(response.error_code, None).and_then(|()| {
                        // Before v3 an unauthorized request is answered with no
                        // directories and no error code.
                        if version < 3 && response.results.is_empty() {
                            return Err(KrafkaError::broker(
                                ErrorCode::ClusterAuthorizationFailed,
                                "DescribeLogDirs returned no directories",
                            ));
                        }
                        Ok(response
                            .results
                            .into_iter()
                            .map(|dir| {
                                let description =
                                    answer(dir.error_code, None).map(|()| LogDirDescription {
                                        replicas: dir
                                            .topics
                                            .into_iter()
                                            .flat_map(|t| {
                                                let name = t.name;
                                                t.partitions.into_iter().map(move |p| {
                                                    (
                                                        TopicPartition::new(
                                                            name.clone(),
                                                            p.partition_index,
                                                        ),
                                                        LogDirReplica {
                                                            size: p.partition_size,
                                                            offset_lag: p.offset_lag,
                                                            is_future: p.is_future_key,
                                                        },
                                                    )
                                                })
                                            })
                                            .collect(),
                                        total_bytes: (dir.total_bytes >= 0)
                                            .then_some(dir.total_bytes),
                                        usable_bytes: (dir.usable_bytes >= 0)
                                            .then_some(dir.usable_bytes),
                                        is_cordoned: dir.is_cordoned,
                                    });
                                (dir.log_dir, description)
                            })
                            .collect())
                    });
                    Ok(ids.into_iter().map(|id| (id, dirs.clone())).collect())
                },
            )
            .await)
    }

    /// Elect leaders (controller): the preferred replica, or with
    /// [`ElectionType::Unclean`] any replica. `partitions = None` elects for
    /// every partition.
    ///
    /// Returns a result per partition. A partition whose preferred leader
    /// already leads (`ELECTION_NOT_NEEDED`) is `Ok`.
    ///
    /// # Errors
    ///
    /// A request-level error from the controller, a closed client, or an
    /// invalid topic name.
    pub async fn elect_leaders(
        &self,
        election_type: ElectionType,
        partitions: Option<Vec<TopicPartition>>,
        options: ElectLeadersOptions,
    ) -> Result<HashMap<TopicPartition, Result<()>>> {
        if let Some(partitions) = &partitions {
            validate_topics(partitions.iter().map(|tp| tp.topic.as_str()))?;
        }
        let call = self.call("ElectLeaders", Mode::Write, options.timeout)?;
        let topic_partitions = partitions.as_ref().map(|partitions| {
            group_partitions(partitions)
                .into_iter()
                .map(|(topic, partitions)| ElectLeadersTopicPartitions { topic, partitions })
                .collect::<Vec<_>>()
        });
        let topic_partitions = &topic_partitions;
        let call_ref = &call;
        call.single(Target::Controller, |conn| async move {
            let request = ElectLeadersRequest {
                election_type,
                topic_partitions: topic_partitions.clone(),
                timeout_ms: call_ref.remaining_ms(),
            };
            let version = negotiate(
                &conn,
                ApiKey::ElectLeaders,
                versions::ELECT_LEADERS_MIN,
                versions::ELECT_LEADERS_MAX,
            )?;
            let response: ElectLeadersResponse =
                exchange(&conn, ApiKey::ElectLeaders, version, &request).await?;
            answer(response.error_code, None)?;
            Ok(response
                .replica_election_results
                .into_iter()
                .flat_map(|t| {
                    let topic = t.topic;
                    t.partition_results.into_iter().map(move |p| {
                        let result = if p.error_code == ErrorCode::ElectionNotNeeded {
                            Ok(())
                        } else {
                            answer(p.error_code, p.error_message)
                        };
                        (TopicPartition::new(topic.clone(), p.partition_id), result)
                    })
                })
                .collect())
        })
        .await
    }

    /// Start or cancel partition reassignments (controller). A target of
    /// `None` cancels the partition's pending reassignment.
    ///
    /// Returns a result per partition; a request-level error applies to
    /// every partition.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client or an invalid topic name.
    pub async fn alter_partition_reassignments(
        &self,
        targets: impl IntoIterator<Item = (TopicPartition, Option<Vec<BrokerId>>)>,
        options: AlterPartitionReassignmentsOptions,
    ) -> Result<HashMap<TopicPartition, Result<()>>> {
        let targets: HashMap<TopicPartition, Option<Vec<BrokerId>>> = targets.into_iter().collect();
        validate_topics(targets.keys().map(|tp| tp.topic.as_str()))?;
        let call = self.call("AlterPartitionReassignments", Mode::Write, options.timeout)?;
        let allow = options.allow_replication_factor_change;
        let targets_ref = &targets;
        let call_ref = &call;
        Ok(call
            .fan_out(
                targets.keys().cloned().collect(),
                |_| Target::Controller,
                |conn, partitions| async move {
                    let version = negotiate(
                        &conn,
                        ApiKey::AlterPartitionReassignments,
                        versions::ALTER_PARTITION_REASSIGNMENTS_MIN,
                        versions::ALTER_PARTITION_REASSIGNMENTS_MAX,
                    )?;
                    if !allow && version < 1 {
                        return Err(KrafkaError::protocol_kind(
                            crate::error::ProtocolErrorKind::UnknownApiVersion,
                            "allow_replication_factor_change(false) needs \
                             AlterPartitionReassignments v1 (Kafka 4.1); the controller \
                             supports v0 only",
                        ));
                    }
                    let request = AlterPartitionReassignmentsRequest {
                        timeout_ms: call_ref.remaining_ms(),
                        allow_replication_factor_change: allow,
                        topics: group_partitions(&partitions)
                            .into_iter()
                            .map(|(name, indexes)| ReassignableTopic {
                                partitions: indexes
                                    .into_iter()
                                    .map(|partition_index| ReassignablePartition {
                                        partition_index,
                                        replicas: targets_ref
                                            [&TopicPartition::new(name.clone(), partition_index)]
                                            .clone(),
                                    })
                                    .collect(),
                                name,
                            })
                            .collect(),
                    };
                    let response: AlterPartitionReassignmentsResponse = exchange(
                        &conn,
                        ApiKey::AlterPartitionReassignments,
                        version,
                        &request,
                    )
                    .await?;
                    if let Err(e) = answer(response.error_code, response.error_message) {
                        return Ok(partitions
                            .into_iter()
                            .map(|tp| (tp, Err(e.clone())))
                            .collect());
                    }
                    Ok(response
                        .responses
                        .into_iter()
                        .flat_map(|t| {
                            let name = t.name;
                            t.partitions.into_iter().map(move |p| {
                                (
                                    TopicPartition::new(name.clone(), p.partition_index),
                                    answer(p.error_code, p.error_message),
                                )
                            })
                        })
                        .collect())
                },
            )
            .await)
    }

    /// List ongoing partition reassignments (any broker).
    ///
    /// # Errors
    ///
    /// The broker's error, a closed client, or an invalid topic name.
    pub async fn list_partition_reassignments(
        &self,
        options: ListPartitionReassignmentsOptions,
    ) -> Result<HashMap<TopicPartition, PartitionReassignment>> {
        if let Some(partitions) = &options.partitions {
            validate_topics(partitions.iter().map(|tp| tp.topic.as_str()))?;
        }
        let call = self.call("ListPartitionReassignments", Mode::Read, options.timeout)?;
        let topics = options.partitions.as_ref().map(|partitions| {
            group_partitions(partitions)
                .into_iter()
                .map(
                    |(name, partition_indexes)| ListPartitionReassignmentsTopic {
                        name,
                        partition_indexes,
                    },
                )
                .collect::<Vec<_>>()
        });
        let topics = &topics;
        let call_ref = &call;
        call.single(Target::AnyBroker, |conn| async move {
            let request = ListPartitionReassignmentsRequest {
                timeout_ms: call_ref.remaining_ms(),
                topics: topics.clone(),
            };
            let version = negotiate(
                &conn,
                ApiKey::ListPartitionReassignments,
                versions::LIST_PARTITION_REASSIGNMENTS_MIN,
                versions::LIST_PARTITION_REASSIGNMENTS_MAX,
            )?;
            let response: ListPartitionReassignmentsResponse =
                exchange(&conn, ApiKey::ListPartitionReassignments, version, &request).await?;
            answer(response.error_code, response.error_message)?;
            Ok(response
                .topics
                .into_iter()
                .flat_map(|t| {
                    let name = t.name;
                    t.partitions.into_iter().map(move |p| {
                        (
                            TopicPartition::new(name.clone(), p.partition_index),
                            PartitionReassignment {
                                replicas: p.replicas,
                                adding_replicas: p.adding_replicas,
                                removing_replicas: p.removing_replicas,
                            },
                        )
                    })
                })
                .collect())
        })
        .await
    }

    /// Move replicas to another log directory on the broker holding them.
    /// Each request goes to the replica's broker. Returns a result per
    /// replica.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client or an invalid topic name.
    pub async fn alter_replica_log_dirs(
        &self,
        moves: impl IntoIterator<Item = (TopicPartitionReplica, String)>,
        options: AlterReplicaLogDirsOptions,
    ) -> Result<HashMap<TopicPartitionReplica, Result<()>>> {
        let moves: HashMap<TopicPartitionReplica, String> = moves.into_iter().collect();
        validate_topics(moves.keys().map(|r| r.topic.as_str()))?;
        let call = self.call("AlterReplicaLogDirs", Mode::Write, options.timeout)?;
        let moves_ref = &moves;
        Ok(call
            .fan_out(
                moves.keys().cloned().collect(),
                |r| Target::Broker(r.broker_id),
                |conn, replicas| async move {
                    let broker_id = replicas.first().map_or(-1, |r| r.broker_id);
                    let mut dirs: BTreeMap<&str, BTreeMap<String, Vec<i32>>> = BTreeMap::new();
                    for r in &replicas {
                        dirs.entry(moves_ref[r].as_str())
                            .or_default()
                            .entry(r.topic.clone())
                            .or_default()
                            .push(r.partition);
                    }
                    let request = AlterReplicaLogDirsRequest {
                        dirs: dirs
                            .into_iter()
                            .map(|(path, topics)| AlterReplicaLogDir {
                                path: path.to_string(),
                                topics: topics
                                    .into_iter()
                                    .map(|(name, partitions)| AlterReplicaLogDirTopic {
                                        name,
                                        partitions,
                                    })
                                    .collect(),
                            })
                            .collect(),
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::AlterReplicaLogDirs,
                        versions::ALTER_REPLICA_LOG_DIRS_MIN,
                        versions::ALTER_REPLICA_LOG_DIRS_MAX,
                    )?;
                    let response: AlterReplicaLogDirsResponse =
                        exchange(&conn, ApiKey::AlterReplicaLogDirs, version, &request).await?;
                    Ok(response
                        .results
                        .into_iter()
                        .flat_map(|t| {
                            let name = t.topic_name;
                            t.partitions.into_iter().map(move |p| {
                                (
                                    TopicPartitionReplica::new(
                                        name.clone(),
                                        p.partition_index,
                                        broker_id,
                                    ),
                                    answer(p.error_code, None),
                                )
                            })
                        })
                        .collect())
                },
            )
            .await)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn reassignment_options_allow_a_replication_factor_change_by_default() {
        assert!(AlterPartitionReassignmentsOptions::default().allow_replication_factor_change);
        assert!(
            !AlterPartitionReassignmentsOptions::default()
                .allow_replication_factor_change(false)
                .allow_replication_factor_change
        );
    }

    #[test]
    fn partitions_group_sorted_by_topic() {
        let grouped = group_partitions(&[
            TopicPartition::new("b", 2),
            TopicPartition::new("a", 1),
            TopicPartition::new("b", 0),
        ]);
        assert_eq!(grouped["a"], vec![1]);
        assert_eq!(grouped["b"], vec![0, 2]);
    }
}
