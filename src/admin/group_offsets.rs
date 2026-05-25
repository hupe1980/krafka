//! AdminClient operation group: group_offsets.

use tracing::{debug, info, warn};

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    ApiKey, OffsetCommitRequest, OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    OffsetCommitResponse, OffsetDeletePartitionRequest, OffsetDeleteRequest, OffsetDeleteResponse,
    OffsetDeleteTopicRequest, OffsetFetchRequest, OffsetFetchRequestTopic, OffsetFetchResponse,
    VersionedDecode, VersionedEncode, versions,
};

#[allow(clippy::wildcard_imports)]
use super::*;

impl AdminClient {
    /// Delete committed offsets for a consumer group.
    ///
    /// **This is a destructive operation** — deleted offsets cannot be
    /// recovered. The consumer group must be in the `Empty` state.
    ///
    /// The request is sent to the group coordinator.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = admin.delete_offsets(
    ///     "my-group",
    ///     &[("my-topic", &[0, 1, 2])],
    /// ).await?;
    /// ```
    pub async fn delete_consumer_group_offsets(
        &self,
        group_id: &str,
        topic_partitions: &[(&str, &[i32])],
    ) -> Result<OffsetDeleteResult> {
        self.check_not_closed()?;

        // Find the group coordinator.
        let coordinator = self.find_group_coordinator(group_id).await?;

        let topics = topic_partitions
            .iter()
            .map(|(name, partitions)| OffsetDeleteTopicRequest {
                name: (*name).to_string(),
                partitions: partitions
                    .iter()
                    .map(|&p| OffsetDeletePartitionRequest { partition_index: p })
                    .collect(),
            })
            .collect();

        let request = OffsetDeleteRequest {
            group_id: group_id.to_string(),
            topics,
        };

        let version = coordinator
            .negotiate_api_version(
                ApiKey::OffsetDelete,
                versions::OFFSET_DELETE_MAX,
                versions::OFFSET_DELETE_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported OffsetDelete API version",
                )
            })?;

        let response_bytes = coordinator
            .send_request(ApiKey::OffsetDelete, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = OffsetDeleteResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            warn!("OffsetDelete top-level error: {:?}", response.error_code);
        }

        let topics = response
            .topics
            .into_iter()
            .map(|t| OffsetDeleteTopicResult {
                name: t.name,
                partitions: t
                    .partitions
                    .into_iter()
                    .map(|p| OffsetDeletePartitionResult {
                        partition_index: p.partition_index,
                        error: if p.error_code.is_ok() {
                            None
                        } else {
                            Some(format!("{:?}", p.error_code))
                        },
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        info!("OffsetDelete completed for group {group_id}");

        Ok(OffsetDeleteResult {
            error: if response.error_code.is_ok() {
                None
            } else {
                Some(format!("{:?}", response.error_code))
            },
            topics,
        })
    }

    // ════════════════════════════════════════════════════════════════════
    // DescribeUserScramCredentials (API key 50)
    // ════════════════════════════════════════════════════════════════════

    /// Fetch committed offsets for a consumer group.
    ///
    /// Pass `topic_partitions` to fetch offsets for specific partitions,
    /// or `None` to fetch all committed offsets for the group.
    ///
    /// The request is sent to the group coordinator.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let offsets = admin
    ///     .describe_consumer_group_offsets("my-group", None)
    ///     .await?;
    /// for entry in &offsets {
    ///     println!("{}/{}: {}", entry.topic, entry.partition, entry.committed_offset);
    /// }
    /// ```
    pub async fn describe_consumer_group_offsets(
        &self,
        group_id: &str,
        topic_partitions: Option<&[(&str, &[i32])]>,
    ) -> Result<Vec<GroupOffsetEntry>> {
        self.check_not_closed()?;

        let coordinator = self.find_group_coordinator(group_id).await?;

        let topics = topic_partitions.map(|tps| {
            tps.iter()
                .map(|(name, partitions)| OffsetFetchRequestTopic {
                    name: (*name).to_string(),
                    topic_id: None,
                    partition_indexes: partitions.to_vec(),
                })
                .collect::<Vec<_>>()
        });

        let request = OffsetFetchRequest {
            group_id: group_id.to_string(),
            topics,
            require_stable: false,
            member_id: None,
            member_epoch: -1,
        };

        let version = coordinator
            .negotiate_api_version(
                ApiKey::OffsetFetch,
                versions::OFFSET_FETCH_MAX,
                versions::OFFSET_FETCH_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported OffsetFetch API version",
                )
            })?;

        let response_bytes = coordinator
            .send_request(ApiKey::OffsetFetch, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = OffsetFetchResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::Other,
                format!("OffsetFetch top-level error: {:?}", response.error_code),
            ));
        }

        let mut entries = Vec::new();
        for topic in response.topics {
            for partition in topic.partitions {
                entries.push(GroupOffsetEntry {
                    topic: topic.name.clone(),
                    partition: partition.partition_index,
                    committed_offset: partition.committed_offset,
                    metadata: partition.metadata,
                    error: if partition.error_code.is_ok() {
                        None
                    } else {
                        Some(format!("{:?}", partition.error_code))
                    },
                });
            }
        }

        debug!(
            "OffsetFetch for group {group_id}: {} entries",
            entries.len()
        );
        Ok(entries)
    }

    /// Alter committed offsets for a consumer group.
    ///
    /// Sets each specified partition's committed offset. The consumer group
    /// must be in the `Empty` state (no active members).
    ///
    /// The request is sent to the group coordinator.
    ///
    /// # Example
    ///
    /// ```ignore
    /// admin
    ///     .alter_consumer_group_offsets(
    ///         "my-group",
    ///         &[("my-topic", &[(0, 100), (1, 200)])],
    ///     )
    ///     .await?;
    /// ```
    pub async fn alter_consumer_group_offsets(
        &self,
        group_id: &str,
        topic_offsets: &[(&str, &[(i32, i64)])],
    ) -> Result<Vec<AlterGroupOffsetResult>> {
        self.check_not_closed()?;

        let coordinator = self.find_group_coordinator(group_id).await?;

        let topics = topic_offsets
            .iter()
            .map(|(name, partitions)| OffsetCommitRequestTopic {
                name: (*name).to_string(),
                topic_id: None,
                partitions: partitions
                    .iter()
                    .map(|&(partition, offset)| OffsetCommitRequestPartition {
                        partition_index: partition,
                        committed_offset: offset,
                        committed_leader_epoch: -1,
                        commit_timestamp: -1,
                        committed_metadata: None,
                    })
                    .collect(),
            })
            .collect();

        let request = OffsetCommitRequest {
            group_id: group_id.to_string(),
            generation_id: -1,
            member_id: String::new(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics,
        };

        let version = coordinator
            .negotiate_api_version(
                ApiKey::OffsetCommit,
                versions::OFFSET_COMMIT_MAX,
                versions::OFFSET_COMMIT_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported OffsetCommit API version",
                )
            })?;

        let response_bytes = coordinator
            .send_request(ApiKey::OffsetCommit, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = OffsetCommitResponse::decode_versioned(version, &mut buf)?;

        let mut results = Vec::new();
        for topic in response.topics {
            for partition in topic.partitions {
                results.push(AlterGroupOffsetResult {
                    topic: topic.name.clone(),
                    partition: partition.partition_index,
                    error: if partition.error_code.is_ok() {
                        None
                    } else {
                        Some(format!("{:?}", partition.error_code))
                    },
                });
            }
        }

        info!(
            "OffsetCommit for group {group_id}: {} partitions updated",
            results.len()
        );
        Ok(results)
    }

    // ════════════════════════════════════════════════════════════════════
    // DescribeUserScramCredentials (API key 50)
    // ════════════════════════════════════════════════════════════════════
}
