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
                        // `-1` is correct here, unlike on the consumer's own
                        // commit path. An administratively set offset is not
                        // derived from a record this client consumed, so there
                        // is no leader epoch it can honestly vouch for;
                        // inventing one would defeat the KIP-320 check it is
                        // supposed to feed.
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_delete_request_maps_topic_partitions() {
        let request = OffsetDeleteRequest {
            group_id: "my-group".into(),
            topics: vec![OffsetDeleteTopicRequest {
                name: "orders".into(),
                partitions: [0, 1, 2]
                    .iter()
                    .map(|&p| OffsetDeletePartitionRequest { partition_index: p })
                    .collect(),
            }],
        };

        assert_eq!(request.group_id, "my-group");
        assert_eq!(request.topics[0].partitions.len(), 3);

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::OFFSET_DELETE_MAX, &mut buf)
            .expect("OffsetDelete must encode");
        assert!(!buf.is_empty());
    }

    /// `topics: None` fetches every committed offset for the group; an empty
    /// vec would fetch none. The two must not be conflated.
    #[test]
    fn test_offset_fetch_request_none_means_all_partitions() {
        let all = OffsetFetchRequest {
            group_id: "g".into(),
            topics: None,
            require_stable: false,
            member_id: None,
            member_epoch: -1,
        };
        assert!(all.topics.is_none());

        let specific = OffsetFetchRequest {
            group_id: "g".into(),
            topics: Some(vec![OffsetFetchRequestTopic {
                name: "orders".into(),
                topic_id: None,
                partition_indexes: vec![0, 1],
            }]),
            require_stable: false,
            member_id: None,
            member_epoch: -1,
        };
        assert_eq!(
            specific.topics.as_ref().unwrap()[0].partition_indexes.len(),
            2
        );

        let mut buf = Vec::new();
        specific
            .encode_versioned(versions::OFFSET_FETCH_MAX, &mut buf)
            .expect("OffsetFetch must encode");
        assert!(!buf.is_empty());
    }

    /// An admin offset reset acts outside any group membership, so it must send
    /// the "no member" sentinels — a real generation/member ID would be fenced.
    #[test]
    fn test_admin_offset_commit_uses_no_member_sentinels() {
        let request = OffsetCommitRequest {
            group_id: "g".into(),
            generation_id: -1,
            member_id: String::new(),
            group_instance_id: None,
            retention_time_ms: -1,
            topics: vec![OffsetCommitRequestTopic {
                name: "orders".into(),
                topic_id: None,
                partitions: vec![OffsetCommitRequestPartition {
                    partition_index: 0,
                    committed_offset: 100,
                    committed_leader_epoch: -1,
                    commit_timestamp: -1,
                    committed_metadata: None,
                }],
            }],
        };

        assert_eq!(request.generation_id, -1);
        assert!(request.member_id.is_empty());
        assert_eq!(request.topics[0].partitions[0].committed_offset, 100);

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::OFFSET_COMMIT_MAX, &mut buf)
            .expect("OffsetCommit must encode");
        assert!(!buf.is_empty());
    }

    /// `-1` is Kafka's "no offset committed" sentinel and must not be read as
    /// a real offset of -1.
    #[test]
    fn test_committed_offset_sentinel_is_distinguished_from_a_real_offset() {
        let none = GroupOffsetEntry {
            topic: "orders".into(),
            partition: 0,
            committed_offset: -1,
            metadata: None,
            error: None,
        };
        let real = GroupOffsetEntry {
            topic: "orders".into(),
            partition: 1,
            committed_offset: 0,
            metadata: None,
            error: None,
        };

        let interpret = |e: &GroupOffsetEntry| {
            if e.committed_offset == -1 {
                None
            } else {
                Some(e.committed_offset)
            }
        };

        assert_eq!(interpret(&none), None);
        assert_eq!(
            interpret(&real),
            Some(0),
            "offset 0 is a real committed position, not 'no commit'"
        );
    }
}
