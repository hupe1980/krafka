//! AdminClient operation group: groups.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use tracing::{debug, info, warn};

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    ApiKey, CONSUMER_PROTOCOL_TYPE, ConsumerGroupDescribeRequest, ConsumerGroupDescribeResponse,
    DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsResponse, DeleteRecordsTopic,
    DescribeGroupsRequest, DescribeGroupsResponse, ListGroupsRequest, ListGroupsResponse,
    OffsetForLeaderEpochPartition, OffsetForLeaderEpochRequest, OffsetForLeaderEpochResponse,
    OffsetForLeaderEpochTopic, VersionedDecode, VersionedEncode,
    decode_consumer_protocol_assignment, decode_consumer_protocol_subscription,
    validate_topic_name, versions,
};

#[allow(clippy::wildcard_imports)]
use super::*;

/// Server-side filter for [`AdminClient::list_consumer_groups`].
///
/// Both filters are applied by the broker, which is the point: a cluster can
/// hold tens of thousands of consumer groups, and listing all of them to keep
/// the three that are `Empty` transfers the entire group registry over the
/// network on every call. Filtering client-side is correct and does not scale.
///
/// An empty filter means "no restriction". Brokers that predate the filter
/// ignore it — `states_filter` needs `ListGroups` v4 (KIP-518) and
/// `types_filter` needs v5 (KIP-848) — so a filtered call against an old
/// broker returns more than asked for rather than failing.
#[non_exhaustive]
#[derive(Debug, Clone, Default)]
pub struct GroupListing {
    /// Group states to include, e.g. `"Empty"`, `"Stable"`, `"Dead"`.
    pub states: Vec<String>,
    /// Group types to include, e.g. `"consumer"`, `"classic"`, `"share"`.
    pub types: Vec<String>,
}

impl GroupListing {
    /// No restriction — every group the broker knows about.
    #[must_use]
    pub fn all() -> Self {
        Self::default()
    }

    /// Restrict to the given group states.
    ///
    /// The canonical spellings are Kafka's own: `PreparingRebalance`,
    /// `CompletingRebalance`, `Stable`, `Dead`, `Empty`. They are passed
    /// through verbatim, so a state a future broker adds needs no crate
    /// release.
    #[must_use]
    pub fn in_states<I, S>(mut self, states: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.states = states.into_iter().map(Into::into).collect();
        self
    }

    /// Restrict to the given group types (KIP-848): `classic`, `consumer`,
    /// `share`, `streams`.
    #[must_use]
    pub fn of_types<I, S>(mut self, types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.types = types.into_iter().map(Into::into).collect();
        self
    }
}

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
        //
        // Coordinator resolution retries on retriable errors and errors out if
        // it cannot be resolved. The previous fallback to an arbitrary broker
        // guaranteed the follow-up DescribeGroups would answer NOT_COORDINATOR
        // while the real cause had already been discarded into a log line.
        let mut coordinator_groups: HashMap<(i32, String), Vec<String>> = HashMap::new();

        for group_id in &group_ids {
            let (node_id, addr) = self.find_coordinator_node(group_id, false).await?;
            coordinator_groups
                .entry((node_id, addr))
                .or_default()
                .push(group_id.clone());
        }

        let mut all_results = Vec::new();

        for ((broker_id, addr), groups) in &coordinator_groups {
            let conn = self.pool.get_connection_by_id(*broker_id, addr).await?;

            // Try ConsumerGroupDescribe (Key 69) first for all groups on this broker.
            let kip848_version = conn.negotiate_api_version(
                ApiKey::ConsumerGroupDescribe,
                versions::CONSUMER_GROUP_DESCRIBE_MAX,
                versions::CONSUMER_GROUP_DESCRIBE_MIN,
            );

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
                    let group_id = g.group_id;
                    let protocol_type = g.protocol_type;
                    let classic_desc = ConsumerGroupDescription {
                        members: g
                            .members
                            .into_iter()
                            .map(|m| {
                                let decoded = classic_member_protocol(
                                    &group_id,
                                    &m.member_id,
                                    &protocol_type,
                                    &m,
                                );
                                ConsumerGroupMember {
                                    member_id: m.member_id,
                                    instance_id: m.group_instance_id,
                                    rack_id: decoded.rack_id,
                                    member_epoch: None,
                                    client_id: m.client_id,
                                    client_host: m.client_host,
                                    subscribed_topic_names: decoded.subscribed_topic_names,
                                    subscribed_topic_regex: None,
                                    assignment: decoded.assignment,
                                    target_assignment: None,
                                    member_type: None,
                                }
                            })
                            .collect(),
                        group_id,
                        group_type: GroupType::Classic,
                        state: g.group_state,
                        protocol_type: Some(protocol_type),
                        assignor: Some(g.protocol_data),
                        group_epoch: None,
                        assignment_epoch: None,
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
    /// let groups = admin.list_consumer_groups(&GroupListing::all()).await?;
    /// for group in &groups {
    ///     println!("{} ({})", group.group_id, group.protocol_type);
    /// }
    /// ```
    ///
    /// `filter` is applied by the **broker**. On a cluster with thousands of
    /// groups that is the difference between transferring the whole registry
    /// and transferring the handful you asked about — see [`GroupListing`].
    pub async fn list_consumer_groups(
        &self,
        filter: &GroupListing,
    ) -> Result<Vec<ConsumerGroupListing>> {
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
                states_filter: filter.states.clone(),
                types_filter: filter.types.clone(),
            };

            let version = match conn.negotiate_api_version(
                ApiKey::ListGroups,
                versions::LIST_GROUPS_MAX,
                versions::LIST_GROUPS_MIN,
            ) {
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
                // Wait out `retry.backoff.ms` if the refresh is rate-limited;
                // retrying against an unchanged cache reproduces the same
                // NotLeaderForPartition and burns the only retry.
                let topics: Vec<&str> = offsets.keys().map(|(t, _)| t.as_str()).collect();
                self.refresh_topics_for_retry(&topics, "DeleteRecords")
                    .await;
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
                // See `delete_records`: a rate-limited refresh must be awaited,
                // not treated as success.
                let topics: Vec<&str> = partitions.iter().map(|(t, _, _)| t.as_str()).collect();
                self.refresh_topics_for_retry(&topics, "OffsetForLeaderEpoch")
                    .await;
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

/// Decode one classic group member's embedded protocol blobs.
///
/// `DescribeGroups` (Key 15) returns a classic member's subscription and
/// assignment as opaque `bytes`: the coordinator stores what the member and the
/// group leader wrote and never parses either.
///
/// Both results are `None` when the value is *unknown*, which is not the same
/// as empty:
///
/// * The group's embedded protocol is not `consumer`. Connect and Streams put
///   their own formats in these fields, and reading one as a consumer
///   subscription would invent topic names out of unrelated bytes.
/// * The blob is malformed. It is written by another client, so a bad one is
///   logged and skipped rather than failing the describe for the whole group.
///
/// `Some(vec![])` means the member subscribes to nothing, or owns no partitions
/// right now — it has joined but not yet completed a rebalance.
fn classic_member_protocol(
    group_id: &str,
    member_id: &str,
    protocol_type: &str,
    member: &crate::protocol::DescribeGroupMember,
) -> ClassicMemberProtocol {
    if protocol_type != CONSUMER_PROTOCOL_TYPE {
        return ClassicMemberProtocol::default();
    }

    let subscription = match decode_consumer_protocol_subscription(&member.member_metadata) {
        Ok(subscription) => Some(subscription),
        Err(e) => {
            warn!(
                "failed to decode subscription for member '{}' of classic group '{}': {}",
                member_id, group_id, e
            );
            None
        }
    };

    let assignment = match decode_consumer_protocol_assignment(&member.member_assignment) {
        Ok(assignment) => Some(
            assignment
                .assigned_partitions
                .into_iter()
                .map(topic_partition_assignment)
                .collect(),
        ),
        Err(e) => {
            warn!(
                "failed to decode assignment for member '{}' of classic group '{}': {}",
                member_id, group_id, e
            );
            None
        }
    };

    ClassicMemberProtocol {
        rack_id: subscription.as_ref().and_then(|s| s.rack_id.clone()),
        subscribed_topic_names: subscription.map(|s| s.topics),
        assignment,
    }
}

/// What [`classic_member_protocol`] recovered from a member's blobs.
#[derive(Default)]
struct ClassicMemberProtocol {
    /// Topics from the `ConsumerProtocolSubscription`.
    subscribed_topic_names: Option<Vec<String>>,
    /// Rack from the subscription (v3+, KIP-881).
    rack_id: Option<String>,
    /// Partitions from the `ConsumerProtocolAssignment`.
    assignment: Option<Vec<TopicPartitionAssignment>>,
}

/// The classic protocol carries topic names only, so the topic ID is all-zero.
fn topic_partition_assignment(
    tp: crate::protocol::ConsumerProtocolTopicPartitions,
) -> TopicPartitionAssignment {
    TopicPartitionAssignment {
        topic_id: [0u8; 16],
        topic_name: tp.topic,
        partitions: tp.partitions,
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use bytes::BufMut;

    use super::*;
    use crate::error::ErrorCode;
    use crate::protocol::{
        ConsumerProtocolAssignment, ConsumerProtocolSubscription, ConsumerProtocolTopicPartitions,
    };

    #[test]
    fn test_list_groups_request_encodes_empty_filters_as_no_filter() {
        let request = ListGroupsRequest {
            states_filter: Vec::new(),
            types_filter: Vec::new(),
        };
        let mut buf = Vec::new();
        request
            .encode_versioned(versions::LIST_GROUPS_MAX, &mut buf)
            .expect("ListGroups must encode");
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_describe_groups_request_encodes_group_ids() {
        let request = DescribeGroupsRequest {
            groups: vec!["a".into(), "b".into()],
            include_authorized_operations: false,
        };
        assert_eq!(request.groups.len(), 2);

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::DESCRIBE_GROUPS_MAX, &mut buf)
            .expect("DescribeGroups must encode");
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_consumer_group_describe_request_encodes_group_ids() {
        let request = ConsumerGroupDescribeRequest::new(vec!["a".into()]);
        let mut buf = Vec::new();
        request
            .encode_versioned(versions::CONSUMER_GROUP_DESCRIBE_MAX, &mut buf)
            .expect("ConsumerGroupDescribe must encode");
        assert!(!buf.is_empty());
    }

    /// KIP-848 `ConsumerGroupDescribe` reports classic-protocol groups with one
    /// of two error codes depending on broker version; both must fall back to
    /// the classic `DescribeGroups` path rather than surfacing as an error.
    #[test]
    fn test_classic_group_fallback_error_codes() {
        let needs_fallback = |code: ErrorCode| {
            code == ErrorCode::GroupIdNotFound || code == ErrorCode::UnsupportedVersion
        };

        // Kafka 3.7–3.8 / 4.0 with a never-consumer group.
        assert!(needs_fallback(ErrorCode::GroupIdNotFound));
        // Kafka 3.9.
        assert!(needs_fallback(ErrorCode::UnsupportedVersion));

        // A genuine failure must not be mistaken for "this is a classic group".
        assert!(!needs_fallback(ErrorCode::GroupAuthorizationFailed));
        assert!(!needs_fallback(ErrorCode::CoordinatorNotAvailable));
        assert!(!needs_fallback(ErrorCode::None));
    }

    /// Build the member the way the coordinator stores one: a subscription it
    /// was given at JoinGroup, an assignment the leader wrote at SyncGroup.
    fn classic_member(
        subscription: &ConsumerProtocolSubscription,
        assigned: &[(&str, &[i32])],
    ) -> crate::protocol::DescribeGroupMember {
        let mut metadata = bytes::BytesMut::new();
        crate::protocol::encode_consumer_protocol_subscription(subscription, &mut metadata)
            .expect("subscription must encode");

        let assignment = ConsumerProtocolAssignment::new(
            0,
            assigned
                .iter()
                .map(|(topic, partitions)| ConsumerProtocolTopicPartitions {
                    topic: (*topic).to_string(),
                    partitions: partitions.to_vec(),
                })
                .collect(),
        );
        let mut assignment_bytes = bytes::BytesMut::new();
        crate::protocol::encode_consumer_protocol_assignment(&assignment, &mut assignment_bytes)
            .expect("assignment must encode");

        crate::protocol::DescribeGroupMember {
            member_id: "m1".to_string(),
            group_instance_id: None,
            client_id: "c1".to_string(),
            client_host: "/127.0.0.1".to_string(),
            member_metadata: metadata.freeze(),
            member_assignment: assignment_bytes.freeze(),
        }
    }

    /// DescribeGroups hands a classic member's subscription and assignment back
    /// as opaque bytes. Both must be decoded here, or every caller has to
    /// reimplement two wire formats to learn what the group is doing.
    #[test]
    fn a_classic_member_reports_its_subscription_and_its_assignment() {
        let subscription =
            ConsumerProtocolSubscription::new(vec!["orders".to_string(), "payments".to_string()])
                .with_rack_id("us-east-1a");
        let member = classic_member(&subscription, &[("orders", &[0, 3]), ("payments", &[1])]);

        let decoded = classic_member_protocol("g1", "m1", "consumer", &member);

        assert_eq!(
            decoded.subscribed_topic_names.as_deref(),
            Some(["orders".to_string(), "payments".to_string()].as_slice())
        );
        assert_eq!(decoded.rack_id.as_deref(), Some("us-east-1a"));

        let assignment = decoded.assignment.expect("the assignment must decode");
        assert_eq!(assignment.len(), 2);
        assert_eq!(assignment[0].topic_name, "orders");
        assert_eq!(assignment[0].partitions, vec![0, 3]);
        assert_eq!(assignment[1].topic_name, "payments");
        assert_eq!(assignment[1].partitions, vec![1]);
        // The classic protocol carries no topic IDs.
        assert_eq!(assignment[0].topic_id, [0u8; 16]);
    }

    /// Connect and Streams put their own formats in the same two fields.
    /// Reading one as a consumer blob would invent topic names out of unrelated
    /// bytes, so an unknown embedded protocol stays `None` — unknown.
    #[test]
    fn a_non_consumer_protocol_is_not_decoded() {
        let subscription = ConsumerProtocolSubscription::new(vec!["orders".to_string()]);
        let member = classic_member(&subscription, &[("orders", &[0])]);

        for protocol_type in ["connect", ""] {
            let decoded = classic_member_protocol("g1", "m1", protocol_type, &member);
            assert!(decoded.assignment.is_none());
            assert!(decoded.subscribed_topic_names.is_none());
        }
    }

    /// A member that owns nothing must be distinguishable from one whose
    /// assignment could not be read: `Some(vec![])` versus `None`.
    #[test]
    fn a_member_mid_rebalance_owns_nothing_rather_than_unknown() {
        let subscription = ConsumerProtocolSubscription::new(vec!["orders".to_string()]);
        let mut member = classic_member(&subscription, &[]);
        // What the coordinator stores for a member that has joined but not yet
        // been given an assignment.
        member.member_assignment = bytes::Bytes::new();

        let decoded = classic_member_protocol("g1", "m1", "consumer", &member);

        assert!(
            decoded.assignment.is_some_and(|a| a.is_empty()),
            "an empty assignment blob means the member owns nothing, not that \
             we could not read it"
        );
        assert_eq!(
            decoded.subscribed_topic_names.as_deref(),
            Some(["orders".to_string()].as_slice())
        );
    }

    /// The blobs are written by other clients, so one bad member must not fail
    /// the describe for the whole group — and must not be reported as empty
    /// either.
    #[test]
    fn a_malformed_blob_degrades_to_unknown() {
        let subscription = ConsumerProtocolSubscription::new(vec!["orders".to_string()]);
        let mut member = classic_member(&subscription, &[("orders", &[0])]);
        // A topic name that is not valid UTF-8.
        let mut bad = bytes::BytesMut::new();
        bad.put_i16(0);
        bad.put_i32(1);
        bad.put_i16(2);
        bad.put_slice(&[0xff, 0xfe]);
        bad.put_i32(0);
        member.member_assignment = bad.freeze();

        let decoded = classic_member_protocol("g1", "m1", "consumer", &member);

        assert!(decoded.assignment.is_none());
        // The subscription is still readable, and still reported.
        assert!(decoded.subscribed_topic_names.is_some());
    }

    #[test]
    fn test_group_type_parsing() {
        let parse = |s: &str| match s {
            "classic" => GroupType::Classic,
            "consumer" => GroupType::Consumer,
            other => GroupType::Unknown(other.to_string()),
        };

        assert_eq!(parse("classic"), GroupType::Classic);
        assert_eq!(parse("consumer"), GroupType::Consumer);
        assert_eq!(
            parse("share"),
            GroupType::Unknown("share".to_string()),
            "an unrecognised type must be preserved, not silently dropped"
        );
        assert_eq!(GroupType::Classic.to_string(), "classic");
        assert_eq!(GroupType::Unknown("share".into()).to_string(), "share");
    }

    #[test]
    fn test_delete_records_request_maps_offsets_per_partition() {
        let request = DeleteRecordsRequest {
            topics: vec![DeleteRecordsTopic {
                name: "orders".into(),
                partitions: vec![
                    DeleteRecordsPartition {
                        partition_index: 0,
                        offset: 100,
                    },
                    DeleteRecordsPartition {
                        partition_index: 1,
                        offset: 250,
                    },
                ],
            }],
            timeout_ms: 30_000,
        };

        assert_eq!(request.topics[0].partitions[0].offset, 100);
        assert_eq!(request.topics[0].partitions[1].offset, 250);

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::DELETE_RECORDS_MAX, &mut buf)
            .expect("DeleteRecords must encode");
        assert!(!buf.is_empty());
    }

    /// The admin client queries from the consumer's perspective, so
    /// `replica_id` and `current_leader_epoch` use the consumer sentinels.
    #[test]
    fn test_offset_for_leader_epoch_request_uses_consumer_sentinels() {
        let request = OffsetForLeaderEpochRequest {
            replica_id: -1,
            topics: vec![OffsetForLeaderEpochTopic {
                topic: "orders".into(),
                partitions: vec![OffsetForLeaderEpochPartition {
                    partition: 0,
                    current_leader_epoch: -1,
                    leader_epoch: 5,
                }],
            }],
        };

        assert_eq!(
            request.replica_id, -1,
            "-1 identifies a consumer, not a broker"
        );
        assert_eq!(request.topics[0].partitions[0].leader_epoch, 5);

        let mut buf = Vec::new();
        request
            .encode_versioned(versions::OFFSET_FOR_LEADER_EPOCH_MAX, &mut buf)
            .expect("OffsetForLeaderEpoch must encode");
        assert!(!buf.is_empty());
    }

    /// `list_consumer_groups` merges results from every broker and must
    /// deduplicate group IDs seen on more than one.
    #[test]
    fn test_listed_groups_are_deduplicated_across_brokers() {
        let mut seen = HashSet::new();
        let mut kept = Vec::new();
        for id in ["g1", "g2", "g1", "g3", "g2"] {
            if seen.insert(id.to_string()) {
                kept.push(id);
            }
        }
        assert_eq!(kept, vec!["g1", "g2", "g3"]);
    }
}
