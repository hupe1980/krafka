//! AdminClient operation group: partitions.

use std::collections::HashMap;
use std::time::Duration;

use tracing::{info, warn};

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse, AlterReplicaLogDir,
    AlterReplicaLogDirTopic, AlterReplicaLogDirsRequest, AlterReplicaLogDirsResponse, ApiKey,
    DescribableLogDirTopic, DescribeLogDirsRequest, DescribeLogDirsResponse, ElectLeadersRequest,
    ElectLeadersResponse, ElectLeadersTopicPartitions, ElectionType,
    ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse,
    ListPartitionReassignmentsTopic, ReassignableTopic, VersionedDecode, VersionedEncode,
    validate_topic_name, versions,
};

#[allow(clippy::wildcard_imports)]
use super::*;

impl AdminClient {
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
        // H6: reject oversize topic names at ingress.
        if let Some(ref ts) = topics {
            for t in ts {
                validate_topic_name(&t.topic)?;
            }
        }
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
                .get_connection_by_id(broker.id(), broker.address())
                .await?;

            let version = conn
                .negotiate_api_version(
                    ApiKey::DescribeLogDirs,
                    versions::DESCRIBE_LOG_DIRS_MAX,
                    versions::DESCRIBE_LOG_DIRS_MIN,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::UnknownApiVersion,
                        "no mutually supported DescribeLogDirs API version",
                    )
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
                        broker.id(),
                        topic_scope,
                        e
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
                        broker.id(),
                        topic_scope,
                        e
                    );
                    continue;
                }
            };

            // v3+ top-level error code
            if !response.error_code.is_ok() {
                warn!(
                    "DescribeLogDirs top-level error on broker {} ({}): {:?}",
                    broker.id(),
                    topic_scope,
                    response.error_code
                );
            }

            // Pre-v3 brokers lack a top-level error code; empty results typically
            // signal CLUSTER_AUTHORIZATION_FAILED (matches Java client heuristic).
            if response.results.is_empty() && version < 3 {
                warn!(
                    "DescribeLogDirs returned empty results on broker {} (v{}, {}); \
                     likely CLUSTER_AUTHORIZATION_FAILED",
                    broker.id(),
                    version,
                    topic_scope
                );
            }

            for result in response.results {
                all_dirs.push(LogDirInfo {
                    broker_id: broker.id(),
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
        self.check_not_closed()?;
        // H6: reject oversize topic names at ingress.
        if let Some(ref tps) = topic_partitions {
            for tp in tps {
                validate_topic_name(&tp.topic)?;
            }
        }

        let timeout_ms = crate::util::duration_to_millis_i32(timeout);

        // `ElectLeaders` is controller-only.
        let response = self
            .with_controller("ElectLeaders", |conn| {
                let topic_partitions = &topic_partitions;
                async move {
                    let request = ElectLeadersRequest {
                        election_type,
                        topic_partitions: topic_partitions.clone(),
                        timeout_ms,
                    };

                    let version = conn
                        .negotiate_api_version(
                            ApiKey::ElectLeaders,
                            versions::ELECT_LEADERS_MAX,
                            versions::ELECT_LEADERS_MIN,
                        )
                        .await
                        .ok_or_else(|| {
                            KrafkaError::protocol_kind(
                                ProtocolErrorKind::UnknownApiVersion,
                                "no mutually supported ElectLeaders API version",
                            )
                        })?;

                    let response_bytes = conn
                        .send_request(ApiKey::ElectLeaders, version, |buf| {
                            request.encode_versioned(version, buf)
                        })
                        .await?;

                    let mut buf = response_bytes;
                    let response = ElectLeadersResponse::decode_versioned(version, &mut buf)?;

                    if super::is_controller_moved(response.error_code) {
                        return Ok(ControllerAttempt::NotController(response.error_code));
                    }

                    Ok(ControllerAttempt::Done(response))
                }
            })
            .await?;

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
        self.check_not_closed()?;
        // H6: reject oversize topic names at ingress.
        for t in &topics {
            validate_topic_name(&t.name)?;
        }

        let timeout_ms = crate::util::duration_to_millis_i32(timeout);

        // `AlterPartitionReassignments` is controller-only.
        let response = self
            .with_controller("AlterPartitionReassignments", |conn| {
                let topics = &topics;
                async move {
                    let request = AlterPartitionReassignmentsRequest {
                        timeout_ms,
                        topics: topics.clone(),
                    };

                    let version = conn
                        .negotiate_api_version(
                            ApiKey::AlterPartitionReassignments,
                            versions::ALTER_PARTITION_REASSIGNMENTS_MAX,
                            versions::ALTER_PARTITION_REASSIGNMENTS_MIN,
                        )
                        .await
                        .ok_or_else(|| {
                            KrafkaError::protocol_kind(
                                ProtocolErrorKind::UnknownApiVersion,
                                "no mutually supported AlterPartitionReassignments API version",
                            )
                        })?;

                    let response_bytes = conn
                        .send_request(ApiKey::AlterPartitionReassignments, version, |buf| {
                            request.encode_versioned(version, buf)
                        })
                        .await?;

                    let mut buf = response_bytes;
                    let response =
                        AlterPartitionReassignmentsResponse::decode_versioned(version, &mut buf)?;

                    if super::is_controller_moved(response.error_code) {
                        return Ok(ControllerAttempt::NotController(response.error_code));
                    }

                    Ok(ControllerAttempt::Done(response))
                }
            })
            .await?;

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
        // H6: reject oversize topic names at ingress.
        if let Some(ref ts) = topics {
            for t in ts {
                validate_topic_name(&t.name)?;
            }
        }
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
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
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
    /// This is a per-broker operation. Each broker is sent **only the
    /// topic-partitions it actually hosts a replica for**, resolved from cached
    /// cluster metadata. Brokers that host none of the requested replicas are
    /// not contacted at all.
    ///
    /// This matters: broadcasting the full request to every broker asks each
    /// one to move replicas it does not own, producing a storm of
    /// `REPLICA_NOT_AVAILABLE` / `KAFKA_STORAGE_ERROR` results that are
    /// indistinguishable from genuine failures on the brokers that do own them.
    ///
    /// If metadata knows no replicas for a requested partition, the request is
    /// sent to the partition's leader when known, and reported as an error
    /// otherwise — it is never broadcast.
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
        // H6: reject oversize topic names at ingress.
        for d in &dirs {
            for t in &d.topics {
                validate_topic_name(&t.name)?;
            }
        }
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::broker(
                crate::error::ErrorCode::UnknownServerError,
                "no brokers available",
            ));
        }

        // Route each (log dir, topic, partition) triple to the brokers that
        // actually hold a replica of that partition.
        let (by_broker, unroutable) = plan_log_dir_moves(&dirs, |topic, partition| {
            let replicas: Vec<i32> = self
                .metadata
                .topic_arc(topic)
                .and_then(|t| {
                    t.partition(partition)
                        .map(|p| p.replicas.iter().copied().filter(|id| *id >= 0).collect())
                })
                .unwrap_or_default();
            if replicas.is_empty() {
                // Fall back to the leader if the replica set is unknown.
                self.metadata.leader(topic, partition).into_iter().collect()
            } else {
                replicas
            }
        });

        let mut all_results: Vec<AlterReplicaLogDirsResult> = unroutable
            .into_iter()
            .map(|(topic_name, partition_index)| {
                warn!(
                    topic = %topic_name,
                    partition = partition_index,
                    "AlterReplicaLogDirs: no replicas known in metadata; \
                     refusing to broadcast this partition to every broker"
                );
                AlterReplicaLogDirsResult {
                    broker_id: -1,
                    topic_name,
                    partitions: vec![AlterReplicaLogDirsPartitionResult {
                        partition_index,
                        error: Some(
                            "no replicas known in cluster metadata for this partition".to_string(),
                        ),
                    }],
                }
            })
            .collect();
        let contacted = by_broker.len();

        for (broker_id, dir_plan) in by_broker {
            let Some(broker) = brokers.iter().find(|b| b.id() == broker_id) else {
                warn!(
                    "AlterReplicaLogDirs: broker {broker_id} is not in cluster metadata; skipping"
                );
                continue;
            };

            // Build a request containing only this broker's replicas.
            let request = AlterReplicaLogDirsRequest {
                dirs: dir_plan
                    .into_iter()
                    .map(|(path, topics)| AlterReplicaLogDir {
                        path,
                        topics: topics
                            .into_iter()
                            .map(|(name, mut partitions)| {
                                partitions.sort_unstable();
                                partitions.dedup();
                                AlterReplicaLogDirTopic { name, partitions }
                            })
                            .collect(),
                    })
                    .collect(),
            };

            let conn = self
                .pool
                .get_connection_by_id(broker.id(), broker.address())
                .await?;

            let version = conn
                .negotiate_api_version(
                    ApiKey::AlterReplicaLogDirs,
                    versions::ALTER_REPLICA_LOG_DIRS_MAX,
                    versions::ALTER_REPLICA_LOG_DIRS_MIN,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::UnknownApiVersion,
                        "no mutually supported AlterReplicaLogDirs API version",
                    )
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
                        broker.id(),
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
                        broker.id(),
                        request.dirs.len(),
                        e
                    );
                    continue;
                }
            };

            for topic in response.results {
                all_results.push(AlterReplicaLogDirsResult {
                    broker_id: broker.id(),
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
            "AlterReplicaLogDirs completed for {} topic result(s) across {contacted} broker(s)",
            all_results.len()
        );
        Ok(all_results)
    }

    // ════════════════════════════════════════════════════════════════════
    // OffsetDelete (API key 47)
    // ════════════════════════════════════════════════════════════════════
}

/// Per-broker log-dir move plan: log dir path → topic name → partition indexes.
type DirPlan = HashMap<String, HashMap<String, Vec<i32>>>;

/// Route each `(log dir, topic, partition)` triple to the brokers that actually
/// hold a replica of that partition.
///
/// `replicas_of` returns the broker IDs hosting a replica of the given
/// partition (falling back to the leader when the full replica set is unknown).
/// Partitions for which it returns nothing are reported in the second tuple
/// element as `(topic, partition)` and are **not** sent anywhere.
///
/// Broadcasting the full request to every broker instead asks each one to move
/// replicas it does not own, producing a storm of `REPLICA_NOT_AVAILABLE` /
/// `KAFKA_STORAGE_ERROR` results indistinguishable from genuine failures on the
/// brokers that do own them.
fn plan_log_dir_moves(
    dirs: &[AlterReplicaLogDir],
    replicas_of: impl Fn(&str, i32) -> Vec<i32>,
) -> (HashMap<i32, DirPlan>, Vec<(String, i32)>) {
    let mut by_broker: HashMap<i32, DirPlan> = HashMap::new();
    let mut unroutable: Vec<(String, i32)> = Vec::new();

    for dir in dirs {
        for topic in &dir.topics {
            for &partition in &topic.partitions {
                let targets = replicas_of(&topic.name, partition);
                if targets.is_empty() {
                    unroutable.push((topic.name.clone(), partition));
                    continue;
                }
                for broker_id in targets {
                    by_broker
                        .entry(broker_id)
                        .or_default()
                        .entry(dir.path.clone())
                        .or_default()
                        .entry(topic.name.clone())
                        .or_default()
                        .push(partition);
                }
            }
        }
    }

    (by_broker, unroutable)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::protocol::{ReassignablePartition, ReassignableTopic};

    fn dir(path: &str, topics: &[(&str, &[i32])]) -> AlterReplicaLogDir {
        AlterReplicaLogDir {
            path: path.to_string(),
            topics: topics
                .iter()
                .map(|(name, partitions)| AlterReplicaLogDirTopic {
                    name: (*name).to_string(),
                    partitions: partitions.to_vec(),
                })
                .collect(),
        }
    }

    /// The destructive AlterReplicaLogDirs must reach only the brokers hosting
    /// a replica — never every broker in the cluster.
    #[test]
    fn test_log_dir_moves_only_reach_replica_holders() {
        let dirs = vec![dir("/data/2", &[("orders", &[0])])];
        // orders-0 lives on brokers 1 and 2 only; the cluster also has 3 and 4.
        let (plan, unroutable) = plan_log_dir_moves(&dirs, |_t, _p| vec![1, 2]);

        assert!(unroutable.is_empty());
        assert_eq!(plan.len(), 2, "only replica holders may be contacted");
        assert!(plan.contains_key(&1));
        assert!(plan.contains_key(&2));
        assert!(!plan.contains_key(&3), "broker 3 hosts no replica");
        assert!(!plan.contains_key(&4));
    }

    /// Each broker receives only its own partitions, not the whole request.
    #[test]
    fn test_each_broker_gets_only_its_own_partitions() {
        let dirs = vec![dir("/data/2", &[("orders", &[0, 1])])];
        // orders-0 -> broker 1, orders-1 -> broker 2.
        let (plan, _) = plan_log_dir_moves(&dirs, |_t, p| vec![p + 1]);

        assert_eq!(plan[&1]["/data/2"]["orders"], vec![0]);
        assert_eq!(plan[&2]["/data/2"]["orders"], vec![1]);
    }

    #[test]
    fn test_multiple_log_dirs_are_kept_separate() {
        let dirs = vec![
            dir("/data/a", &[("t", &[0])]),
            dir("/data/b", &[("t", &[1])]),
        ];
        let (plan, _) = plan_log_dir_moves(&dirs, |_t, _p| vec![1]);

        let broker = &plan[&1];
        assert_eq!(broker.len(), 2, "each target log dir stays distinct");
        assert_eq!(broker["/data/a"]["t"], vec![0]);
        assert_eq!(broker["/data/b"]["t"], vec![1]);
    }

    /// A partition with no known replicas must be reported, not broadcast.
    #[test]
    fn test_unknown_replicas_are_reported_not_broadcast() {
        let dirs = vec![dir("/data/2", &[("orders", &[0, 7])])];
        let (plan, unroutable) =
            plan_log_dir_moves(&dirs, |_t, p| if p == 0 { vec![1] } else { vec![] });

        assert_eq!(unroutable, vec![("orders".to_string(), 7)]);
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[&1]["/data/2"]["orders"], vec![0]);
    }

    #[test]
    fn test_empty_dirs_plan_nothing() {
        let (plan, unroutable) = plan_log_dir_moves(&[], |_t, _p| vec![1]);
        assert!(plan.is_empty());
        assert!(unroutable.is_empty());
    }

    // ── Request construction ──

    #[test]
    fn test_elect_leaders_request_encodes_both_election_types() {
        for election_type in [ElectionType::Preferred, ElectionType::Unclean] {
            let request = ElectLeadersRequest {
                election_type,
                topic_partitions: Some(vec![ElectLeadersTopicPartitions {
                    topic: "orders".into(),
                    partitions: vec![0, 1],
                }]),
                timeout_ms: 60_000,
            };

            let mut buf = Vec::new();
            request
                .encode_versioned(versions::ELECT_LEADERS_MAX, &mut buf)
                .expect("ElectLeaders must encode");
            assert!(!buf.is_empty());
        }
    }

    /// `None` topic_partitions means "elect for all partitions" — a null array
    /// on the wire, semantically different from an empty list.
    #[test]
    fn test_elect_leaders_all_partitions_is_null_not_empty() {
        let request = ElectLeadersRequest {
            election_type: ElectionType::Preferred,
            topic_partitions: None,
            timeout_ms: 1_000,
        };
        assert!(request.topic_partitions.is_none());

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::ELECT_LEADERS_MAX, &mut buf)
            .expect("ElectLeaders(all) must encode");
        assert!(!buf.is_empty());
    }

    /// `replicas: None` cancels a pending reassignment; `Some(..)` starts one.
    #[test]
    fn test_alter_reassignments_distinguishes_start_from_cancel() {
        let request = AlterPartitionReassignmentsRequest {
            timeout_ms: 60_000,
            topics: vec![ReassignableTopic {
                name: "orders".into(),
                partitions: vec![
                    ReassignablePartition {
                        partition_index: 0,
                        replicas: Some(vec![1, 2, 3]),
                    },
                    ReassignablePartition {
                        partition_index: 1,
                        replicas: None,
                    },
                ],
            }],
        };

        assert_eq!(
            request.topics[0].partitions[0].replicas.as_ref().unwrap(),
            &vec![1, 2, 3]
        );
        assert!(
            request.topics[0].partitions[1].replicas.is_none(),
            "None must mean 'cancel', not 'assign to no replicas'"
        );

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::ALTER_PARTITION_REASSIGNMENTS_MAX, &mut buf)
            .expect("AlterPartitionReassignments must encode");
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_describe_log_dirs_request_all_vs_filtered() {
        let all = DescribeLogDirsRequest::all();
        let mut buf = Vec::new();
        all.encode_versioned(versions::DESCRIBE_LOG_DIRS_MAX, &mut buf)
            .expect("DescribeLogDirs(all) must encode");
        assert!(!buf.is_empty());

        let filtered = DescribeLogDirsRequest::for_topics(vec![DescribableLogDirTopic {
            topic: "orders".into(),
            partitions: vec![0, 1, 2],
        }]);
        let mut buf = Vec::new();
        filtered
            .encode_versioned(versions::DESCRIBE_LOG_DIRS_MAX, &mut buf)
            .expect("DescribeLogDirs(filtered) must encode");
        assert!(!buf.is_empty());
    }
}
