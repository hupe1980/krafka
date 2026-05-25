//! AdminClient operation group: groups.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    ApiKey, ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse, DeleteRecordsPartition,
    DeleteRecordsRequest, DeleteRecordsResponse, DeleteRecordsTopic, DescribeGroupsRequest,
    DescribeGroupsResponse, FindCoordinatorRequest, FindCoordinatorResponse, ListGroupsRequest,
    ListGroupsResponse, OffsetForLeaderEpochPartition, OffsetForLeaderEpochRequest,
    OffsetForLeaderEpochResponse, OffsetForLeaderEpochTopic, VersionedDecode, VersionedEncode,
    validate_topic_name, versions,
};

#[allow(clippy::wildcard_imports)]
use super::*;

impl AdminClient {
    /// Describe consumer groups.
    ///
    /// Automatically detects whether each group uses the classic protocol or the
    /// new consumer protocol (KIP-848) and dispatches to the appropriate API:
    /// - **Classic groups** → DescribeGroups (Key 15)
    /// - **Consumer groups** → ConsumerGroupDescribe (Key 69)
    ///
    /// The returned [`ConsumerGroupDescription`] is a unified type.
    /// Fields specific to one protocol variant are `Option`-wrapped.
    ///
    /// # Example
    /// ```ignore
    /// let groups = admin
    ///     .describe_consumer_groups(vec!["my-group".to_string()])
    ///     .await?;
    /// for group in &groups {
    ///     println!("{}: type={}, state={}, members={}",
    ///         group.group_id, group.group_type, group.state, group.members.len());
    /// }
    /// ```
    pub async fn describe_consumer_groups(
        &self,
        group_ids: Vec<String>,
    ) -> Result<Vec<ConsumerGroupDescription>> {
        self.check_not_closed()?;
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::broker(
                crate::error::ErrorCode::UnknownServerError,
                "no brokers available",
            ));
        }

        // Route each group to its coordinator broker.
        let mut coordinator_groups: HashMap<i32, Vec<String>> = HashMap::new();
        let any_broker = &brokers[0];
        let any_conn = self
            .pool
            .get_connection_by_id(any_broker.id(), any_broker.address())
            .await?;

        for group_id in &group_ids {
            let coord_request = FindCoordinatorRequest::for_group(group_id);
            let coord_version = any_conn
                .negotiate_api_version(
                    ApiKey::FindCoordinator,
                    versions::FIND_COORDINATOR_MAX,
                    versions::FIND_COORDINATOR_MIN,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::UnknownApiVersion,
                        "no mutually supported FindCoordinator API version",
                    )
                })?;

            let coord_response_bytes = any_conn
                .send_request(ApiKey::FindCoordinator, coord_version, |buf| {
                    coord_request.encode_versioned(coord_version, buf)
                })
                .await?;
            let mut coord_buf = coord_response_bytes;
            let coord_response =
                FindCoordinatorResponse::decode_versioned(coord_version, &mut coord_buf)?;

            if coord_response.error_code.is_ok() {
                coordinator_groups
                    .entry(coord_response.node_id)
                    .or_default()
                    .push(group_id.clone());
            } else {
                warn!(
                    "FindCoordinator failed for group '{}': {:?}, falling back to broker {}",
                    group_id,
                    coord_response.error_code,
                    any_broker.id()
                );
                coordinator_groups
                    .entry(any_broker.id())
                    .or_default()
                    .push(group_id.clone());
            }
        }

        let mut all_results = Vec::new();

        for (broker_id, groups) in &coordinator_groups {
            let broker = brokers
                .iter()
                .find(|b| b.id() == *broker_id)
                .unwrap_or(any_broker);
            let conn = self
                .pool
                .get_connection_by_id(broker.id(), broker.address())
                .await?;

            // Try ConsumerGroupDescribe (Key 69) first for all groups on this broker.
            let kip848_version = conn
                .negotiate_api_version(
                    ApiKey::ConsumerGroupDescribe,
                    versions::CONSUMER_GROUP_DESCRIBE_MAX,
                    versions::CONSUMER_GROUP_DESCRIBE_MIN,
                )
                .await;

            let mut classic_fallback: Vec<String> = Vec::new();
            let mut maybe_classic: Vec<(String, ConsumerGroupDescription)> = Vec::new();

            if let Some(version) = kip848_version {
                let request = ConsumerGroupDescribeRequest::new(groups.clone());
                let response_bytes = conn
                    .send_request(ApiKey::ConsumerGroupDescribe, version, |buf| {
                        request.encode_versioned(version, buf)
                    })
                    .await?;

                let mut buf = response_bytes;
                let response = ConsumerGroupDescribeResponse::decode_versioned(version, &mut buf)?;

                // ConsumerGroupDescribe (Key 69) returns per-group error codes
                // that tell us which groups need the classic DescribeGroups path:
                //
                //  • GroupIdNotFound  — classic group (Kafka 3.7–3.8 or 4.0+
                //                       with a group that was never a consumer group)
                //  • UnsupportedVersion — classic group (Kafka 3.9)
                //  • OK + empty members — ambiguous on 3.7–3.8; we try the
                //                         classic path too and prefer whichever
                //                         reports members.

                for g in response.groups {
                    debug!(
                        "ConsumerGroupDescribe for '{}': error={:?}, state='{}', members={}",
                        g.group_id,
                        g.error_code,
                        g.group_state,
                        g.members.len()
                    );
                    if g.error_code == crate::error::ErrorCode::GroupIdNotFound
                        || g.error_code == crate::error::ErrorCode::UnsupportedVersion
                    {
                        // Classic-protocol group — fall back to DescribeGroups (Key 15).
                        debug!(
                            "ConsumerGroupDescribe for '{}' returned {:?}, \
                             will retry with DescribeGroups (Key 15)",
                            g.group_id, g.error_code
                        );
                        classic_fallback.push(g.group_id);
                        continue;
                    }

                    let members_empty = g.members.is_empty() && g.error_code.is_ok();
                    let group_id_clone = g.group_id.clone();

                    let desc = ConsumerGroupDescription {
                        group_id: g.group_id,
                        group_type: GroupType::Consumer,
                        state: g.group_state,
                        protocol_type: None,
                        assignor: Some(g.assignor_name),
                        group_epoch: Some(g.group_epoch),
                        assignment_epoch: Some(g.assignment_epoch),
                        members: g
                            .members
                            .into_iter()
                            .map(|m| ConsumerGroupMember {
                                member_id: m.member_id,
                                instance_id: m.instance_id,
                                rack_id: m.rack_id,
                                member_epoch: Some(m.member_epoch),
                                client_id: m.client_id,
                                client_host: m.client_host,
                                subscribed_topic_names: Some(m.subscribed_topic_names),
                                subscribed_topic_regex: m.subscribed_topic_regex,
                                assignment: Some(
                                    m.assignment
                                        .topic_partitions
                                        .into_iter()
                                        .map(|tp| TopicPartitionAssignment {
                                            topic_id: tp.topic_id,
                                            topic_name: tp.topic_name,
                                            partitions: tp.partitions,
                                        })
                                        .collect(),
                                ),
                                target_assignment: Some(
                                    m.target_assignment
                                        .topic_partitions
                                        .into_iter()
                                        .map(|tp| TopicPartitionAssignment {
                                            topic_id: tp.topic_id,
                                            topic_name: tp.topic_name,
                                            partitions: tp.partitions,
                                        })
                                        .collect(),
                                ),
                                member_type: Some(m.member_type),
                            })
                            .collect(),
                        authorized_operations: Some(g.authorized_operations),
                        error: if g.error_code.is_ok() {
                            None
                        } else {
                            let msg = g
                                .error_message
                                .unwrap_or_else(|| format!("{:?}", g.error_code));
                            Some(msg)
                        },
                    };

                    // Kafka 3.7–3.8 (KIP-848 Early Access) may return OK
                    // with empty members for classic-protocol groups instead
                    // of GroupIdNotFound / UnsupportedVersion.  Try the
                    // classic DescribeGroups path and prefer whichever has
                    // members.
                    if members_empty {
                        maybe_classic.push((group_id_clone.clone(), desc));
                        classic_fallback.push(group_id_clone);
                    } else {
                        all_results.push(desc);
                    }
                }
            } else {
                // Broker does not support Key 69 — all groups are classic.
                classic_fallback = groups.clone();
            }

            // Describe classic-protocol groups via DescribeGroups (Key 15).
            if !classic_fallback.is_empty() {
                let request = DescribeGroupsRequest {
                    groups: classic_fallback,
                    include_authorized_operations: false,
                };

                let version = conn
                    .negotiate_api_version(
                        ApiKey::DescribeGroups,
                        versions::DESCRIBE_GROUPS_MAX,
                        versions::DESCRIBE_GROUPS_MIN,
                    )
                    .await
                    .ok_or_else(|| {
                        KrafkaError::protocol_kind(
                            ProtocolErrorKind::UnknownApiVersion,
                            "no mutually supported DescribeGroups API version",
                        )
                    })?;

                let response_bytes = conn
                    .send_request(ApiKey::DescribeGroups, version, |buf| {
                        request.encode_versioned(version, buf)
                    })
                    .await?;

                let mut buf = response_bytes;
                let response = DescribeGroupsResponse::decode_versioned(version, &mut buf)?;

                for g in response.groups {
                    debug!(
                        "DescribeGroups (classic) for '{}': error={:?}, state='{}', members={}",
                        g.group_id,
                        g.error_code,
                        g.group_state,
                        g.members.len()
                    );
                    let classic_desc = ConsumerGroupDescription {
                        group_id: g.group_id,
                        group_type: GroupType::Classic,
                        state: g.group_state,
                        protocol_type: Some(g.protocol_type),
                        assignor: Some(g.protocol_data),
                        group_epoch: None,
                        assignment_epoch: None,
                        members: g
                            .members
                            .into_iter()
                            .map(|m| ConsumerGroupMember {
                                member_id: m.member_id,
                                instance_id: m.group_instance_id,
                                rack_id: None,
                                member_epoch: None,
                                client_id: m.client_id,
                                client_host: m.client_host,
                                subscribed_topic_names: None,
                                subscribed_topic_regex: None,
                                assignment: None,
                                target_assignment: None,
                                member_type: None,
                            })
                            .collect(),
                        authorized_operations: None,
                        error: if g.error_code.is_ok() {
                            None
                        } else {
                            Some(format!("{:?}", g.error_code))
                        },
                    };

                    // If this group was a maybe_classic candidate from
                    // ConsumerGroupDescribe, prefer whichever path found
                    // members. Remove from maybe_classic so we don't
                    // double-add it later.
                    if let Some(idx) = maybe_classic
                        .iter()
                        .position(|(id, _)| *id == classic_desc.group_id)
                    {
                        let (_, consumer_desc) = maybe_classic.swap_remove(idx);
                        if classic_desc.members.is_empty() {
                            // Neither path found members — keep the consumer result.
                            all_results.push(consumer_desc);
                        } else {
                            all_results.push(classic_desc);
                        }
                    } else {
                        all_results.push(classic_desc);
                    }
                }
            }

            // Any remaining maybe_classic entries that weren't resolved
            // by the classic fallback (shouldn't happen, but be safe).
            for (_, desc) in maybe_classic {
                all_results.push(desc);
            }
        }

        info!("Described {} consumer groups", all_results.len());
        Ok(all_results)
    }

    /// List all consumer groups on the cluster.
    ///
    /// Returns a list of all consumer groups with their protocol types.
    ///
    /// # Example
    /// ```ignore
    /// let groups = admin.list_consumer_groups().await?;
    /// for group in &groups {
    ///     println!("{} ({})", group.group_id, group.protocol_type);
    /// }
    /// ```
    pub async fn list_consumer_groups(&self) -> Result<Vec<ConsumerGroupListing>> {
        self.check_not_closed()?;
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::broker(
                crate::error::ErrorCode::UnknownServerError,
                "no brokers available",
            ));
        }

        // ListGroups returns groups managed by each broker, so we query all brokers
        let mut all_groups = Vec::new();
        let mut seen_ids = HashSet::new();
        let mut broker_failures = 0usize;
        let broker_count = brokers.len();

        for broker in &brokers {
            let conn = match self
                .pool
                .get_connection_by_id(broker.id(), broker.address())
                .await
            {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        "Failed to connect to broker {} for ListGroups, skipping: {}",
                        broker.id(),
                        e
                    );
                    broker_failures += 1;
                    continue;
                }
            };

            let request = ListGroupsRequest {
                states_filter: Vec::new(),
                types_filter: Vec::new(),
            };

            let version = match conn
                .negotiate_api_version(
                    ApiKey::ListGroups,
                    versions::LIST_GROUPS_MAX,
                    versions::LIST_GROUPS_MIN,
                )
                .await
            {
                Some(v) => v,
                None => {
                    warn!(
                        "No mutually supported ListGroups API version for broker {}, skipping",
                        broker.id()
                    );
                    broker_failures += 1;
                    continue;
                }
            };

            let response_bytes = match conn
                .send_request(ApiKey::ListGroups, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    warn!("ListGroups RPC failed on broker {}: {}", broker.id(), e);
                    broker_failures += 1;
                    continue;
                }
            };

            let mut buf = response_bytes;
            let response = match ListGroupsResponse::decode_versioned(version, &mut buf) {
                Ok(r) => r,
                Err(e) => {
                    warn!("ListGroups decode failed on broker {}: {}", broker.id(), e);
                    broker_failures += 1;
                    continue;
                }
            };

            if !response.error_code.is_ok() {
                tracing::warn!(
                    "ListGroups error on broker {}: {:?}",
                    broker.id(),
                    response.error_code
                );
                broker_failures += 1;
                continue;
            }

            for group in response.groups {
                if seen_ids.insert(group.group_id.clone()) {
                    let group_type = group.group_type.map(|t| match t.as_str() {
                        "classic" => GroupType::Classic,
                        "consumer" => GroupType::Consumer,
                        other => GroupType::Unknown(other.to_string()),
                    });
                    all_groups.push(ConsumerGroupListing {
                        group_id: group.group_id,
                        protocol_type: group.protocol_type,
                        group_type,
                    });
                }
            }
        }

        if broker_failures == broker_count {
            return Err(KrafkaError::invalid_state(
                "list_consumer_groups failed: all brokers returned errors",
            ));
        }

        if broker_failures > 0 {
            warn!(
                "list_consumer_groups: {broker_failures}/{broker_count} brokers failed; \
                 results may be incomplete"
            );
        }

        info!("Listed {} consumer groups", all_groups.len());
        Ok(all_groups)
    }

    /// Delete records from topic partitions before the specified offsets.
    ///
    /// Records with offsets less than the specified offset for each partition
    /// will be marked for deletion. This adjusts the log start offset.
    ///
    /// # Arguments
    /// * `offsets` - Map of (topic, partition) to the offset before which to delete
    /// * `timeout` - Operation timeout
    ///
    /// # Example
    /// ```ignore
    /// use std::collections::HashMap;
    /// let mut offsets = HashMap::new();
    /// offsets.insert(("my-topic".to_string(), 0), 100i64);
    /// let results = admin.delete_records(offsets, Duration::from_secs(30)).await?;
    /// ```
    pub async fn delete_records(
        &self,
        offsets: HashMap<(String, i32), i64>,
        timeout: Duration,
    ) -> Result<Vec<DeleteRecordResult>> {
        self.check_not_closed()?;
        // H6: reject oversize topic names before any encoder reaches them.
        for (topic, _) in offsets.keys() {
            validate_topic_name(topic)?;
        }

        for attempt in 0u8..2 {
            if attempt == 1 {
                let topics: Vec<&str> = offsets.keys().map(|(t, _)| t.as_str()).collect();
                let _ = self.metadata.refresh_for_topics(Some(&topics)).await;
            }

            let brokers = self.metadata.brokers();
            if brokers.is_empty() {
                return Err(KrafkaError::broker(
                    crate::error::ErrorCode::UnknownServerError,
                    "no brokers available",
                ));
            }

            // Group offsets by partition leader
            let mut leader_offsets: HashMap<i32, HashMap<String, Vec<DeleteRecordsPartition>>> =
                HashMap::new();
            let fallback_broker_id = brokers[0].id();

            for ((topic, partition), offset) in &offsets {
                let leader_id = self
                    .metadata
                    .leader(topic, *partition)
                    .unwrap_or(fallback_broker_id);
                leader_offsets
                    .entry(leader_id)
                    .or_default()
                    .entry(topic.clone())
                    .or_default()
                    .push(DeleteRecordsPartition {
                        partition_index: *partition,
                        offset: *offset,
                    });
            }

            let mut results = Vec::new();
            let mut has_stale_leader = false;

            for (broker_id, topics_map) in leader_offsets {
                let broker = brokers
                    .iter()
                    .find(|b| b.id() == broker_id)
                    .unwrap_or(&brokers[0]);
                let conn = self
                    .pool
                    .get_connection_by_id(broker.id(), broker.address())
                    .await?;

                let request = DeleteRecordsRequest {
                    topics: topics_map
                        .into_iter()
                        .map(|(name, partitions)| DeleteRecordsTopic { name, partitions })
                        .collect(),
                    timeout_ms: crate::util::duration_to_millis_i32(timeout),
                };

                let version = conn
                    .negotiate_api_version(
                        ApiKey::DeleteRecords,
                        versions::DELETE_RECORDS_MAX,
                        versions::DELETE_RECORDS_MIN,
                    )
                    .await
                    .ok_or_else(|| {
                        KrafkaError::protocol_kind(
                            ProtocolErrorKind::UnknownApiVersion,
                            "no mutually supported DeleteRecords API version",
                        )
                    })?;

                let response_bytes = conn
                    .send_request(ApiKey::DeleteRecords, version, |buf| {
                        request.encode_versioned(version, buf)
                    })
                    .await?;

                let mut buf = response_bytes;
                let response = DeleteRecordsResponse::decode_versioned(version, &mut buf)?;

                for topic in response.topics {
                    let topic_name = topic.name;
                    for partition in topic.partitions {
                        if partition.error_code == crate::error::ErrorCode::NotLeaderForPartition {
                            has_stale_leader = true;
                        }
                        results.push(DeleteRecordResult {
                            topic: topic_name.clone(),
                            partition: partition.partition_index,
                            low_watermark: partition.low_watermark,
                            error: if partition.error_code.is_ok() {
                                None
                            } else {
                                Some(format!("{:?}", partition.error_code))
                            },
                        });
                    }
                }
            }

            if has_stale_leader && attempt == 0 {
                warn!(
                    "NotLeaderForPartition in DeleteRecords response, retrying with refreshed metadata"
                );
                continue;
            }

            info!("Deleted records from {} partition(s)", results.len());
            return Ok(results);
        }
        Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::Malformed,
            "DeleteRecords retry loop exhausted after metadata refresh",
        ))
    }

    /// Get the end offset for each partition at the given leader epoch.
    ///
    /// This is used to detect log truncation after a leader change. For each
    /// topic-partition, the broker returns the end offset for the requested
    /// leader epoch. If the epoch is no longer valid, the broker returns
    /// the epoch and offset where the log was truncated.
    ///
    /// # Arguments
    /// * `partitions` - List of (topic, partition, leader_epoch) tuples
    ///
    /// # Example
    /// ```ignore
    /// let results = admin.offset_for_leader_epoch(
    ///     vec![("my-topic".to_string(), 0, 5)]
    /// ).await?;
    /// for r in &results {
    ///     println!("{}:{} epoch={} end_offset={}", r.topic, r.partition, r.leader_epoch, r.end_offset);
    /// }
    /// ```
    pub async fn offset_for_leader_epoch(
        &self,
        partitions: Vec<(String, i32, i32)>,
    ) -> Result<Vec<LeaderEpochResult>> {
        self.check_not_closed()?;
        // H6: reject oversize topic names at ingress.
        for (topic, _, _) in &partitions {
            validate_topic_name(topic)?;
        }

        for attempt in 0u8..2 {
            if attempt == 1 {
                let topics: Vec<&str> = partitions.iter().map(|(t, _, _)| t.as_str()).collect();
                let _ = self.metadata.refresh_for_topics(Some(&topics)).await;
            }

            let brokers = self.metadata.brokers();
            if brokers.is_empty() {
                return Err(KrafkaError::broker(
                    crate::error::ErrorCode::UnknownServerError,
                    "no brokers available",
                ));
            }

            // Group partitions by their leader broker
            let fallback_broker_id = brokers[0].id();
            let mut leader_partitions: HashMap<
                i32,
                HashMap<String, Vec<OffsetForLeaderEpochPartition>>,
            > = HashMap::new();

            for (topic, partition, leader_epoch) in &partitions {
                let leader_id = self
                    .metadata
                    .leader(topic, *partition)
                    .unwrap_or(fallback_broker_id);
                leader_partitions
                    .entry(leader_id)
                    .or_default()
                    .entry(topic.clone())
                    .or_default()
                    .push(OffsetForLeaderEpochPartition {
                        partition: *partition,
                        current_leader_epoch: -1, // consumer perspective
                        leader_epoch: *leader_epoch,
                    });
            }

            let mut results = Vec::new();
            let mut has_stale_leader = false;

            for (broker_id, topics_map) in leader_partitions {
                let broker = brokers
                    .iter()
                    .find(|b| b.id() == broker_id)
                    .unwrap_or(&brokers[0]);
                let conn = self
                    .pool
                    .get_connection_by_id(broker.id(), broker.address())
                    .await?;

                let request = OffsetForLeaderEpochRequest {
                    replica_id: -1, // -1 for consumer
                    topics: topics_map
                        .into_iter()
                        .map(|(topic, partitions)| OffsetForLeaderEpochTopic { topic, partitions })
                        .collect(),
                };

                let version = conn
                    .negotiate_api_version(
                        ApiKey::OffsetForLeaderEpoch,
                        versions::OFFSET_FOR_LEADER_EPOCH_MAX,
                        versions::OFFSET_FOR_LEADER_EPOCH_MIN,
                    )
                    .await
                    .ok_or_else(|| {
                        KrafkaError::protocol_kind(
                            ProtocolErrorKind::UnknownApiVersion,
                            "no mutually supported OffsetForLeaderEpoch API version",
                        )
                    })?;

                let response_bytes = conn
                    .send_request(ApiKey::OffsetForLeaderEpoch, version, |buf| {
                        request.encode_versioned(version, buf)
                    })
                    .await?;

                let mut buf = response_bytes;
                let response = OffsetForLeaderEpochResponse::decode_versioned(version, &mut buf)?;

                for topic in response.topics {
                    let topic_name = topic.topic;
                    for partition in topic.partitions {
                        if partition.error_code == crate::error::ErrorCode::NotLeaderForPartition {
                            has_stale_leader = true;
                        }
                        results.push(LeaderEpochResult {
                            topic: topic_name.clone(),
                            partition: partition.partition,
                            leader_epoch: partition.leader_epoch,
                            end_offset: partition.end_offset,
                            error: if partition.error_code.is_ok() {
                                None
                            } else {
                                Some(format!("{:?}", partition.error_code))
                            },
                        });
                    }
                }
            }

            if has_stale_leader && attempt == 0 {
                warn!(
                    "NotLeaderForPartition in OffsetForLeaderEpoch response, retrying with refreshed metadata"
                );
                continue;
            }

            info!(
                "Got leader epoch offsets for {} partition(s)",
                results.len()
            );
            return Ok(results);
        }
        Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::Malformed,
            "OffsetForLeaderEpoch retry loop exhausted after metadata refresh",
        ))
    }

    // ── Delegation Tokens ────────────────────────────────────────────────
}
