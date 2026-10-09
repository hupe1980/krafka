//! Share-group offsets (KIP-932, KIP-1226): describe, alter, delete.

use std::collections::{BTreeMap, HashMap};

use crate::consumer::TopicPartition;
use crate::error::{KrafkaError, Result};
use crate::protocol::{
    AlterShareGroupOffsetsRequest, AlterShareGroupOffsetsRequestPartition,
    AlterShareGroupOffsetsRequestTopic, AlterShareGroupOffsetsResponse, ApiKey,
    DeleteShareGroupOffsetsRequest, DeleteShareGroupOffsetsResponse,
    DescribeShareGroupOffsetsRequest, DescribeShareGroupOffsetsRequestGroup,
    DescribeShareGroupOffsetsRequestTopic, DescribeShareGroupOffsetsResponse, versions,
};

use super::driver::{Mode, Target, answer, exchange, negotiate};
use super::{AdminClient, validate_topics};

/// One share partition's offset state.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SharePartitionOffset {
    /// Share-partition start offset: the earliest offset the group may still
    /// deliver.
    pub start_offset: i64,
    /// Leader epoch of the partition.
    pub leader_epoch: i32,
    /// Share-partition lag; `None` when the coordinator does not report it
    /// (`DescribeShareGroupOffsets` v0, Kafka 4.2 and earlier).
    pub lag: Option<i64>,
}

admin_options! {
    /// Options for [`AdminClient::describe_share_group_offsets`].
    DescribeShareGroupOffsetsOptions {}
    optional {
        /// Only these partitions. Default: every partition the group holds
        /// state for.
        partitions: Vec<TopicPartition>,
    }
}

admin_options! {
    /// Options for [`AdminClient::alter_share_group_offsets`].
    AlterShareGroupOffsetsOptions {}
}

admin_options! {
    /// Options for [`AdminClient::delete_share_group_offsets`].
    DeleteShareGroupOffsetsOptions {}
}

impl AdminClient {
    /// Read a share group's share-partition start offsets from its
    /// coordinator. Returns a result per partition.
    ///
    /// # Errors
    ///
    /// A group-level broker error keeps its code; also a closed client or an
    /// invalid topic name.
    pub async fn describe_share_group_offsets(
        &self,
        group_id: impl Into<String>,
        options: DescribeShareGroupOffsetsOptions,
    ) -> Result<HashMap<TopicPartition, Result<SharePartitionOffset>>> {
        let group_id = group_id.into();
        if let Some(partitions) = &options.partitions {
            validate_topics(partitions.iter().map(|tp| tp.topic.as_str()))?;
        }
        let call = self.call("DescribeShareGroupOffsets", Mode::Read, options.timeout)?;
        let topics = options.partitions.as_ref().map(|partitions| {
            let mut topics: BTreeMap<String, Vec<i32>> = BTreeMap::new();
            for tp in partitions {
                topics
                    .entry(tp.topic.clone())
                    .or_default()
                    .push(tp.partition);
            }
            topics
                .into_iter()
                .map(
                    |(topic_name, partitions)| DescribeShareGroupOffsetsRequestTopic {
                        topic_name,
                        partitions,
                    },
                )
                .collect::<Vec<_>>()
        });
        let group = &group_id;
        let topics = &topics;
        call.single(
            Target::GroupCoordinator(group_id.clone()),
            |conn| async move {
                let request = DescribeShareGroupOffsetsRequest {
                    groups: vec![DescribeShareGroupOffsetsRequestGroup {
                        group_id: group.clone(),
                        topics: topics.clone(),
                    }],
                };
                let version = negotiate(
                    &conn,
                    ApiKey::DescribeShareGroupOffsets,
                    versions::DESCRIBE_SHARE_GROUP_OFFSETS_MIN,
                    versions::DESCRIBE_SHARE_GROUP_OFFSETS_MAX,
                )?;
                let response: DescribeShareGroupOffsetsResponse =
                    exchange(&conn, ApiKey::DescribeShareGroupOffsets, version, &request).await?;
                let group = response.groups.into_iter().next().ok_or_else(|| {
                    KrafkaError::protocol_kind(
                        crate::error::ProtocolErrorKind::Malformed,
                        "DescribeShareGroupOffsets returned no group",
                    )
                })?;
                answer(group.error_code, group.error_message)?;
                // `lag` exists from v1; below, the field holds a -1 sentinel.
                let lag_supported = version >= 1;
                Ok(group
                    .topics
                    .into_iter()
                    .flat_map(|t| {
                        let name = t.topic_name;
                        t.partitions.into_iter().map(move |p| {
                            (
                                TopicPartition::new(name.clone(), p.partition_index),
                                answer(p.error_code, p.error_message).map(|()| {
                                    SharePartitionOffset {
                                        start_offset: p.start_offset,
                                        leader_epoch: p.leader_epoch,
                                        lag: (lag_supported && p.lag >= 0).then_some(p.lag),
                                    }
                                }),
                            )
                        })
                    })
                    .collect())
            },
        )
        .await
    }

    /// Set a share group's share-partition start offsets at its coordinator.
    /// The group must be empty. Returns a result per partition; a group-level
    /// error such as `NON_EMPTY_GROUP` applies to every partition.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client or an invalid topic name.
    pub async fn alter_share_group_offsets(
        &self,
        group_id: impl Into<String>,
        offsets: impl IntoIterator<Item = (TopicPartition, i64)>,
        options: AlterShareGroupOffsetsOptions,
    ) -> Result<HashMap<TopicPartition, Result<()>>> {
        let group_id = group_id.into();
        let offsets: HashMap<TopicPartition, i64> = offsets.into_iter().collect();
        validate_topics(offsets.keys().map(|tp| tp.topic.as_str()))?;
        let call = self.call("AlterShareGroupOffsets", Mode::Write, options.timeout)?;
        let group = &group_id;
        let offsets_ref = &offsets;
        Ok(call
            .fan_out(
                offsets.keys().cloned().collect(),
                |_| Target::GroupCoordinator(group_id.clone()),
                |conn, partitions| async move {
                    let mut topics: BTreeMap<String, Vec<AlterShareGroupOffsetsRequestPartition>> =
                        BTreeMap::new();
                    for tp in &partitions {
                        topics.entry(tp.topic.clone()).or_default().push(
                            AlterShareGroupOffsetsRequestPartition {
                                partition_index: tp.partition,
                                start_offset: offsets_ref[tp],
                            },
                        );
                    }
                    let request = AlterShareGroupOffsetsRequest {
                        group_id: group.clone(),
                        topics: topics
                            .into_iter()
                            .map(
                                |(topic_name, partitions)| AlterShareGroupOffsetsRequestTopic {
                                    topic_name,
                                    partitions,
                                },
                            )
                            .collect(),
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::AlterShareGroupOffsets,
                        versions::ALTER_SHARE_GROUP_OFFSETS_MIN,
                        versions::ALTER_SHARE_GROUP_OFFSETS_MAX,
                    )?;
                    let response: AlterShareGroupOffsetsResponse =
                        exchange(&conn, ApiKey::AlterShareGroupOffsets, version, &request).await?;
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
                            let name = t.topic_name;
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

    /// Delete a share group's offset state for whole topics at its
    /// coordinator. The group must be empty. Returns a result per topic; a
    /// group-level error applies to every topic.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client or an invalid topic name.
    pub async fn delete_share_group_offsets<I, S>(
        &self,
        group_id: impl Into<String>,
        topics: I,
        options: DeleteShareGroupOffsetsOptions,
    ) -> Result<HashMap<String, Result<()>>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let group_id = group_id.into();
        let mut topics: Vec<String> = topics.into_iter().map(|s| s.as_ref().to_string()).collect();
        validate_topics(topics.iter().map(String::as_str))?;
        topics.sort_unstable();
        topics.dedup();
        let call = self.call("DeleteShareGroupOffsets", Mode::Write, options.timeout)?;
        let group = &group_id;
        Ok(call
            .fan_out(
                topics,
                |_| Target::GroupCoordinator(group_id.clone()),
                |conn, topics| async move {
                    let request = DeleteShareGroupOffsetsRequest {
                        group_id: group.clone(),
                        topics: topics.clone(),
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::DeleteShareGroupOffsets,
                        versions::DELETE_SHARE_GROUP_OFFSETS_MIN,
                        versions::DELETE_SHARE_GROUP_OFFSETS_MAX,
                    )?;
                    let response: DeleteShareGroupOffsetsResponse =
                        exchange(&conn, ApiKey::DeleteShareGroupOffsets, version, &request).await?;
                    if let Err(e) = answer(response.error_code, response.error_message) {
                        return Ok(topics.into_iter().map(|t| (t, Err(e.clone()))).collect());
                    }
                    Ok(response
                        .responses
                        .into_iter()
                        .map(|t| (t.topic_name, answer(t.error_code, t.error_message)))
                        .collect())
                },
            )
            .await)
    }
}
