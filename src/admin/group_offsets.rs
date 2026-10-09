//! Consumer group offsets: list, alter, delete, lag.

use std::collections::{BTreeMap, HashMap};

use crate::consumer::TopicPartition;
use crate::error::{KrafkaError, Result};
use crate::protocol::{
    ApiKey, OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    OffsetCommitResponse, OffsetDeletePartitionRequest, OffsetDeleteRequest, OffsetDeleteResponse,
    OffsetDeleteTopicRequest, OffsetFetchRequest, OffsetFetchRequestTopic, OffsetFetchResponse,
    versions,
};

use super::driver::{Mode, Target, answer, exchange, negotiate};
use super::offsets::{ListOffsetsOptions, OffsetSpec};
use super::{AdminClient, validate_topics};

/// A committed offset.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupOffset {
    /// Committed offset; `None` when the group has no commit for the
    /// partition.
    pub offset: Option<i64>,
    /// Leader epoch of the committed record, when known (KIP-320).
    pub leader_epoch: Option<i32>,
    /// Metadata attached to the commit.
    pub metadata: Option<String>,
}

/// A partition's consumer lag from [`AdminClient::consumer_group_lag`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumerGroupLag {
    /// Committed offset; `None` when the group has no commit.
    pub committed_offset: Option<i64>,
    /// End offset (high watermark).
    pub end_offset: i64,
    /// `end_offset − committed_offset`, clamped at zero; `None` without a
    /// committed offset.
    pub lag: Option<i64>,
}

admin_options! {
    /// Options for [`AdminClient::list_consumer_group_offsets`].
    ListConsumerGroupOffsetsOptions {
        /// Report only offsets no in-flight transaction can retract
        /// (KIP-447). A partition with an unresolved transactional commit is
        /// retried until it resolves or the deadline passes, then reported as
        /// `UNSTABLE_OFFSET_COMMIT`.
        require_stable: bool,
    }
    optional {
        /// Only these partitions. Default: every partition the group has
        /// committed.
        partitions: Vec<TopicPartition>,
    }
}

admin_options! {
    /// Options for [`AdminClient::alter_consumer_group_offsets`].
    AlterConsumerGroupOffsetsOptions {}
}

admin_options! {
    /// Options for [`AdminClient::delete_consumer_group_offsets`].
    DeleteConsumerGroupOffsetsOptions {}
}

admin_options! {
    /// Options for [`AdminClient::consumer_group_lag`].
    ConsumerGroupLagOptions {
        /// As [`ListConsumerGroupOffsetsOptions::require_stable`].
        require_stable: bool,
    }
    optional {
        /// Only these partitions. Default: every partition the group has
        /// committed.
        partitions: Vec<TopicPartition>,
    }
}

/// Group partitions by topic, sorted, for a request body.
fn by_topic<'a>(
    partitions: impl IntoIterator<Item = &'a TopicPartition>,
) -> BTreeMap<&'a str, Vec<i32>> {
    let mut topics: BTreeMap<&str, Vec<i32>> = BTreeMap::new();
    for tp in partitions {
        topics
            .entry(tp.topic.as_str())
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
    /// Fetch a consumer group's committed offsets from its coordinator.
    ///
    /// Returns a result per partition.
    ///
    /// # Errors
    ///
    /// A group-level error from the broker — for example
    /// `GROUP_AUTHORIZATION_FAILED`, or `COORDINATOR_LOAD_IN_PROGRESS` that
    /// outlasted the deadline — keeps its code as a
    /// [`KrafkaError::Broker`]. Also fails for a closed client.
    pub async fn list_consumer_group_offsets(
        &self,
        group_id: impl Into<String>,
        options: ListConsumerGroupOffsetsOptions,
    ) -> Result<HashMap<TopicPartition, Result<GroupOffset>>> {
        let group_id = group_id.into();
        if let Some(partitions) = &options.partitions {
            validate_topics(partitions.iter().map(|tp| tp.topic.as_str()))?;
        }
        let call = self.call("ListConsumerGroupOffsets", Mode::Read, options.timeout)?;
        let group = &group_id;
        let options = &options;
        call.single(
            Target::GroupCoordinator(group_id.clone()),
            |conn| async move {
                let request = OffsetFetchRequest {
                    group_id: group.clone(),
                    topics: options.partitions.as_ref().map(|partitions| {
                        by_topic(partitions)
                            .into_iter()
                            .map(|(name, partition_indexes)| OffsetFetchRequestTopic {
                                name: name.to_string(),
                                topic_id: None,
                                partition_indexes,
                            })
                            .collect()
                    }),
                    require_stable: options.require_stable,
                    member_id: None,
                    member_epoch: -1,
                };
                let version = negotiate(
                    &conn,
                    ApiKey::OffsetFetch,
                    versions::OFFSET_FETCH_MIN,
                    versions::OFFSET_FETCH_MAX,
                )?;
                let response: OffsetFetchResponse =
                    exchange(&conn, ApiKey::OffsetFetch, version, &request).await?;
                answer(response.error_code, None)?;

                let mut offsets = HashMap::new();
                for topic in response.topics {
                    for p in topic.partitions {
                        let tp = TopicPartition::new(topic.name.clone(), p.partition_index);
                        // UNSTABLE_OFFSET_COMMIT and the coordinator codes apply
                        // to the whole fetch: fail it so the driver retries.
                        if matches!(
                            p.error_code,
                            crate::error::ErrorCode::UnstableOffsetCommit
                                | crate::error::ErrorCode::CoordinatorLoadInProgress
                                | crate::error::ErrorCode::NotCoordinator
                        ) {
                            answer(p.error_code, None)?;
                        }
                        let result = answer(p.error_code, None).map(|()| GroupOffset {
                            offset: (p.committed_offset >= 0).then_some(p.committed_offset),
                            leader_epoch: (p.committed_leader_epoch >= 0)
                                .then_some(p.committed_leader_epoch),
                            metadata: p.metadata,
                        });
                        offsets.insert(tp, result);
                    }
                }
                Ok(offsets)
            },
        )
        .await
    }

    /// Set a consumer group's committed offsets at its coordinator. The group
    /// must have no active members. Returns a result per partition.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client or an invalid topic name.
    pub async fn alter_consumer_group_offsets(
        &self,
        group_id: impl Into<String>,
        offsets: impl IntoIterator<Item = (TopicPartition, i64)>,
        options: AlterConsumerGroupOffsetsOptions,
    ) -> Result<HashMap<TopicPartition, Result<()>>> {
        let group_id = group_id.into();
        let offsets: HashMap<TopicPartition, i64> = offsets.into_iter().collect();
        validate_topics(offsets.keys().map(|tp| tp.topic.as_str()))?;
        let call = self.call("AlterConsumerGroupOffsets", Mode::Write, options.timeout)?;
        let group = &group_id;
        let offsets_ref = &offsets;
        Ok(call
            .fan_out(
                offsets.keys().cloned().collect(),
                |_| Target::GroupCoordinator(group_id.clone()),
                |conn, partitions| async move {
                    let request = OffsetCommitRequest {
                        group_id: group.clone(),
                        generation_id: -1,
                        member_id: String::new(),
                        group_instance_id: None,
                        retention_time_ms: -1,
                        topics: by_topic(&partitions)
                            .into_iter()
                            .map(|(name, indexes)| OffsetCommitRequestTopic {
                                name: name.to_string(),
                                topic_id: None,
                                partitions: indexes
                                    .into_iter()
                                    .map(|partition| OffsetCommitRequestPartition {
                                        partition_index: partition,
                                        committed_offset: offsets_ref
                                            [&TopicPartition::new(name, partition)],
                                        // An administratively set offset has no
                                        // record behind it, so no leader epoch.
                                        committed_leader_epoch: -1,
                                        commit_timestamp: -1,
                                        committed_metadata: None,
                                    })
                                    .collect(),
                            })
                            .collect(),
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::OffsetCommit,
                        versions::OFFSET_COMMIT_MIN,
                        versions::OFFSET_COMMIT_MAX,
                    )?;
                    let response: OffsetCommitResponse =
                        exchange(&conn, ApiKey::OffsetCommit, version, &request).await?;
                    Ok(response
                        .topics
                        .into_iter()
                        .flat_map(|t| {
                            let name = t.name;
                            t.partitions.into_iter().map(move |p| {
                                (
                                    TopicPartition::new(name.clone(), p.partition_index),
                                    answer(p.error_code, None),
                                )
                            })
                        })
                        .collect())
                },
            )
            .await)
    }

    /// Delete a consumer group's committed offsets at its coordinator. The
    /// group must not be subscribed to the topics. Returns a result per
    /// partition; a group-level error applies to every partition.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client or an invalid topic name.
    pub async fn delete_consumer_group_offsets(
        &self,
        group_id: impl Into<String>,
        partitions: impl IntoIterator<Item = TopicPartition>,
        options: DeleteConsumerGroupOffsetsOptions,
    ) -> Result<HashMap<TopicPartition, Result<()>>> {
        let group_id = group_id.into();
        let mut partitions: Vec<TopicPartition> = partitions.into_iter().collect();
        validate_topics(partitions.iter().map(|tp| tp.topic.as_str()))?;
        partitions.dedup();
        let call = self.call("DeleteConsumerGroupOffsets", Mode::Write, options.timeout)?;
        let group = &group_id;
        Ok(call
            .fan_out(
                partitions,
                |_| Target::GroupCoordinator(group_id.clone()),
                |conn, partitions| async move {
                    let request = OffsetDeleteRequest {
                        group_id: group.clone(),
                        topics: by_topic(&partitions)
                            .into_iter()
                            .map(|(name, indexes)| OffsetDeleteTopicRequest {
                                name: name.to_string(),
                                partitions: indexes
                                    .into_iter()
                                    .map(|p| OffsetDeletePartitionRequest { partition_index: p })
                                    .collect(),
                            })
                            .collect(),
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::OffsetDelete,
                        versions::OFFSET_DELETE_MIN,
                        versions::OFFSET_DELETE_MAX,
                    )?;
                    let response: OffsetDeleteResponse =
                        exchange(&conn, ApiKey::OffsetDelete, version, &request).await?;
                    if let Err(e) = answer(response.error_code, None) {
                        return Ok(partitions
                            .into_iter()
                            .map(|tp| (tp, Err(e.clone())))
                            .collect());
                    }
                    Ok(response
                        .topics
                        .into_iter()
                        .flat_map(|t| {
                            let name = t.name;
                            t.partitions.into_iter().map(move |p| {
                                (
                                    TopicPartition::new(name.clone(), p.partition_index),
                                    answer(p.error_code, None),
                                )
                            })
                        })
                        .collect())
                },
            )
            .await)
    }

    /// A consumer group's lag per partition: the end offset minus the
    /// committed offset.
    ///
    /// A partition whose end offset could not be fetched is an `Err` with the
    /// `ListOffsets` error, never a lag of zero that would hide a stalled
    /// consumer.
    ///
    /// # Errors
    ///
    /// As [`list_consumer_group_offsets`](Self::list_consumer_group_offsets).
    pub async fn consumer_group_lag(
        &self,
        group_id: impl Into<String>,
        options: ConsumerGroupLagOptions,
    ) -> Result<HashMap<TopicPartition, Result<ConsumerGroupLag>>> {
        let call = self.call("ConsumerGroupLag", Mode::Read, options.timeout)?;
        let mut fetch = ListConsumerGroupOffsetsOptions::default()
            .require_stable(options.require_stable)
            .timeout(call.remaining());
        if let Some(partitions) = options.partitions {
            fetch = fetch.partitions(partitions);
        }
        let committed = self.list_consumer_group_offsets(group_id, fetch).await?;

        let end_offsets = self
            .list_offsets(
                committed.keys().map(|tp| (tp.clone(), OffsetSpec::Latest)),
                ListOffsetsOptions::default().timeout(call.remaining()),
            )
            .await?;

        Ok(committed
            .into_iter()
            .map(|(tp, committed)| {
                let lag = committed.and_then(|committed| {
                    let end = end_offsets.get(&tp).cloned().unwrap_or_else(|| {
                        Err(KrafkaError::timeout("ListOffsets returned no end offset"))
                    })?;
                    Ok(ConsumerGroupLag {
                        committed_offset: committed.offset,
                        end_offset: end.offset,
                        lag: committed.offset.map(|c| (end.offset - c).max(0)),
                    })
                });
                (tp, lag)
            })
            .collect())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn partitions_are_grouped_by_topic_sorted_and_deduplicated() {
        let partitions = [
            TopicPartition::new("b", 1),
            TopicPartition::new("a", 2),
            TopicPartition::new("a", 0),
            TopicPartition::new("a", 2),
        ];
        let grouped = by_topic(&partitions);
        assert_eq!(
            grouped.into_iter().collect::<Vec<_>>(),
            vec![("a", vec![0, 2]), ("b", vec![1])]
        );
    }
}
