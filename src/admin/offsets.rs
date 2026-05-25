//! AdminClient operations: ListOffsets and consumer group lag.

use std::collections::HashMap;

use tracing::{debug, warn};

use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    ApiKey, ListOffsetsRequest, ListOffsetsRequestPartition, ListOffsetsRequestTopic,
    ListOffsetsResponse, VersionedDecode, VersionedEncode, validate_topic_names, versions,
};

#[allow(clippy::wildcard_imports)]
use super::*;

impl AdminClient {
    /// List offsets for one or more topic-partitions.
    ///
    /// Each request is routed to the partition's current leader.  Metadata
    /// is refreshed once on `NotLeaderForPartition` errors before retrying.
    ///
    /// # Arguments
    ///
    /// * `topic_partitions` — slice of `(topic_name, partition_ids)` pairs.
    /// * `spec` — which offset to fetch (`Earliest`, `Latest`, or `Timestamp`).
    ///
    /// # Example
    ///
    /// ```ignore
    /// use krafka::admin::{AdminClient, OffsetSpec};
    ///
    /// let results = admin
    ///     .list_offsets(&[("my-topic", &[0, 1, 2])], OffsetSpec::Latest)
    ///     .await?;
    /// for r in &results {
    ///     println!("{}/{}: offset={}", r.topic, r.partition, r.offset);
    /// }
    /// ```
    pub async fn list_offsets(
        &self,
        topic_partitions: &[(&str, &[i32])],
        spec: OffsetSpec,
    ) -> Result<Vec<ListOffsetResult>> {
        self.check_not_closed()?;

        let topics: Vec<&str> = topic_partitions.iter().map(|(t, _)| *t).collect();
        validate_topic_names(topics.iter().copied())?;

        let timestamp = spec.as_timestamp();

        for attempt in 0u8..2 {
            if attempt == 1 {
                let _ = self.metadata.refresh_for_topics(Some(&topics)).await;
            }

            let brokers = self.metadata.brokers();
            if brokers.is_empty() {
                return Err(KrafkaError::broker(
                    ErrorCode::UnknownServerError,
                    "no brokers available",
                ));
            }

            // Group partitions by their leader broker.
            let mut leader_map: HashMap<i32, HashMap<String, Vec<i32>>> = HashMap::new();
            let fallback_broker_id = brokers[0].id();

            for &(topic, partitions) in topic_partitions {
                for &partition in partitions {
                    let leader_id = self
                        .metadata
                        .leader(topic, partition)
                        .unwrap_or(fallback_broker_id);
                    leader_map
                        .entry(leader_id)
                        .or_default()
                        .entry(topic.to_string())
                        .or_default()
                        .push(partition);
                }
            }

            let mut results: Vec<ListOffsetResult> = Vec::new();
            let mut has_stale_leader = false;

            for (broker_id, topics_map) in leader_map {
                let broker = brokers
                    .iter()
                    .find(|b| b.id() == broker_id)
                    .unwrap_or(&brokers[0]);
                let conn = self
                    .pool
                    .get_connection_by_id(broker.id(), broker.address())
                    .await?;

                let request = ListOffsetsRequest {
                    replica_id: -1,     // -1 = consumer
                    isolation_level: 0, // read_uncommitted
                    topics: topics_map
                        .into_iter()
                        .map(|(name, partitions)| ListOffsetsRequestTopic {
                            name,
                            partitions: partitions
                                .into_iter()
                                .map(|p| ListOffsetsRequestPartition {
                                    partition_index: p,
                                    current_leader_epoch: -1,
                                    timestamp,
                                })
                                .collect(),
                        })
                        .collect(),
                    timeout_ms: None,
                };

                let version = conn
                    .negotiate_api_version(
                        ApiKey::ListOffsets,
                        versions::LIST_OFFSETS_MAX,
                        versions::LIST_OFFSETS_MIN,
                    )
                    .await
                    .ok_or_else(|| {
                        KrafkaError::protocol_kind(
                            ProtocolErrorKind::UnknownApiVersion,
                            "no mutually supported ListOffsets API version",
                        )
                    })?;

                let response_bytes = conn
                    .send_request(ApiKey::ListOffsets, version, |buf| {
                        request.encode_versioned(version, buf)
                    })
                    .await?;

                let mut buf = response_bytes;
                let response = ListOffsetsResponse::decode_versioned(version, &mut buf)?;

                for topic in response.topics {
                    for partition in topic.partitions {
                        if partition.error_code == ErrorCode::NotLeaderForPartition {
                            has_stale_leader = true;
                        }
                        results.push(ListOffsetResult {
                            topic: topic.name.clone(),
                            partition: partition.partition_index,
                            offset: partition.offset,
                            timestamp: partition.timestamp,
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
                    "NotLeaderForPartition in ListOffsets response, retrying with refreshed metadata"
                );
                continue;
            }

            debug!("ListOffsets returned {} partition result(s)", results.len());
            return Ok(results);
        }

        Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::Malformed,
            "ListOffsets retry loop exhausted after metadata refresh",
        ))
    }

    /// Compute consumer group lag for the specified topics.
    ///
    /// Lag is defined as `end_offset − committed_offset` for each
    /// topic-partition.  Partitions with no committed offset have
    /// `committed_offset = None` and `lag = None`.
    ///
    /// This method issues two parallel-ish requests:
    /// 1. [`describe_consumer_group_offsets`] for the committed positions.
    /// 2. [`list_offsets`] with [`OffsetSpec::Latest`] for the end offsets.
    ///
    /// The consumer group does **not** need to be stopped.
    ///
    /// # Arguments
    ///
    /// * `group_id` — consumer group ID.
    /// * `topic_partitions` — which partitions to measure; pass `None` to
    ///   measure all partitions that the group has committed offsets for.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let lag = admin
    ///     .consumer_group_lag("my-group", Some(&[("my-topic", &[0, 1, 2])]))
    ///     .await?;
    /// for entry in &lag {
    ///     println!(
    ///         "{}/{}: lag={:?}",
    ///         entry.topic, entry.partition, entry.lag
    ///     );
    /// }
    /// ```
    ///
    /// [`describe_consumer_group_offsets`]: AdminClient::describe_consumer_group_offsets
    /// [`list_offsets`]: AdminClient::list_offsets
    pub async fn consumer_group_lag(
        &self,
        group_id: &str,
        topic_partitions: Option<&[(&str, &[i32])]>,
    ) -> Result<Vec<ConsumerGroupLag>> {
        self.check_not_closed()?;

        // 1. Fetch committed offsets.
        let committed = self
            .describe_consumer_group_offsets(group_id, topic_partitions)
            .await?;

        if committed.is_empty() {
            return Ok(Vec::new());
        }

        // 2. Build the (topic, partitions) list for list_offsets.
        //    Group by topic, collect unique partition IDs.
        let mut by_topic: HashMap<String, Vec<i32>> = HashMap::new();
        for entry in &committed {
            by_topic
                .entry(entry.topic.clone())
                .or_default()
                .push(entry.partition);
        }

        // Deduplicate partition lists (describe_consumer_group_offsets may
        // return duplicate entries if the same partition appears multiple times
        // in a group's state, which is rare but possible during rebalance).
        for partitions in by_topic.values_mut() {
            partitions.sort_unstable();
            partitions.dedup();
        }

        let topic_partition_refs: Vec<(&str, &[i32])> = by_topic
            .iter()
            .map(|(t, ps)| (t.as_str(), ps.as_slice()))
            .collect();

        // 3. Fetch end offsets.
        let end_offsets = self
            .list_offsets(&topic_partition_refs, OffsetSpec::Latest)
            .await?;

        // 4. Build a lookup map: (topic, partition) → end_offset.
        let mut end_map: HashMap<(&str, i32), i64> = HashMap::new();
        for r in &end_offsets {
            if r.error.is_none() {
                end_map.insert((r.topic.as_str(), r.partition), r.offset);
            }
        }

        // 5. Compute lag for each committed entry.
        let mut lag_results = Vec::with_capacity(committed.len());
        for entry in &committed {
            // Treat -1 as "no committed offset" (Kafka wire sentinel).
            let committed_offset = if entry.committed_offset == -1 {
                None
            } else {
                Some(entry.committed_offset)
            };

            let end_offset = end_map
                .get(&(entry.topic.as_str(), entry.partition))
                .copied()
                .unwrap_or(-1);

            let lag = committed_offset.map(|co| (end_offset - co).max(0));

            lag_results.push(ConsumerGroupLag {
                topic: entry.topic.clone(),
                partition: entry.partition,
                committed_offset,
                end_offset,
                lag,
            });
        }

        debug!(
            "consumer_group_lag for group {group_id}: {} partition(s)",
            lag_results.len()
        );
        Ok(lag_results)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_offset_spec_as_timestamp() {
        assert_eq!(OffsetSpec::Earliest.as_timestamp(), -2);
        assert_eq!(OffsetSpec::Latest.as_timestamp(), -1);
        assert_eq!(
            OffsetSpec::Timestamp(1_705_276_800).as_timestamp(),
            1_705_276_800
        );
    }

    #[test]
    fn test_consumer_group_lag_struct_fields() {
        let lag = ConsumerGroupLag {
            topic: "test".to_string(),
            partition: 0,
            committed_offset: Some(100),
            end_offset: 150,
            lag: Some(50),
        };
        assert_eq!(lag.lag, Some(50));
        assert_eq!(lag.end_offset, 150);
        assert_eq!(lag.committed_offset, Some(100));
    }

    #[test]
    fn test_list_offset_result_struct() {
        let r = ListOffsetResult {
            topic: "my-topic".to_string(),
            partition: 1,
            offset: 42,
            timestamp: -1,
            error: None,
        };
        assert_eq!(r.offset, 42);
        assert!(r.error.is_none());
    }
}
