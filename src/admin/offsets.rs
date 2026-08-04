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
                // Await a *real* refresh before retrying. A rate-limited
                // refresh returns `RateLimited` without contacting a broker; if
                // that were treated as success the retry would re-issue against
                // byte-identical stale metadata and reproduce the same
                // NotLeaderForPartition forever.
                self.refresh_topics_for_retry(&topics, "ListOffsets").await;
            }

            let brokers = self.metadata.brokers();
            if brokers.is_empty() {
                return Err(KrafkaError::broker(
                    ErrorCode::UnknownServerError,
                    "no brokers available",
                ));
            }

            // Group partitions by their leader broker, carrying each
            // partition's cached leader epoch alongside its index.
            let mut leader_map: HashMap<i32, HashMap<String, Vec<(i32, i32)>>> = HashMap::new();
            let fallback_broker_id = brokers[0].id();

            for &(topic, partitions) in topic_partitions {
                for &partition in partitions {
                    let leader_id = self
                        .metadata
                        .leader(topic, partition)
                        .unwrap_or(fallback_broker_id);
                    // Send the epoch we believe is current so the broker can
                    // fence the request (KIP-320). `-1` disables fencing
                    // entirely and is used only when the epoch is unknown
                    // (Metadata < v7, or the partition is not in the cache).
                    let current_leader_epoch =
                        self.metadata.leader_epoch(topic, partition).unwrap_or(-1);
                    leader_map
                        .entry(leader_id)
                        .or_default()
                        .entry(topic.to_string())
                        .or_default()
                        .push((partition, current_leader_epoch));
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
                                .map(|(partition_index, current_leader_epoch)| {
                                    ListOffsetsRequestPartition {
                                        partition_index,
                                        current_leader_epoch,
                                        timestamp,
                                    }
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
                        // Now that a real `current_leader_epoch` is sent, the
                        // broker can also reject the request with a fenced or
                        // unknown epoch. All three mean "your metadata is
                        // stale" and are cured by the same refresh.
                        if matches!(
                            partition.error_code,
                            ErrorCode::NotLeaderForPartition
                                | ErrorCode::FencedLeaderEpoch
                                | ErrorCode::UnknownLeaderEpoch
                        ) {
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
                warn!("stale leader metadata in ListOffsets response, retrying after a refresh");
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
    /// Partitions whose end offset could not be fetched report
    /// `end_offset = None`, `lag = None`, and the reason in
    /// [`ConsumerGroupLag::end_offset_error`] — never `lag = 0`, which would
    /// make a stalled consumer look healthy.
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

        // 4. Build a lookup map: (topic, partition) → Ok(end_offset) | Err(reason).
        //    Both the per-partition error and a negative sentinel offset mean
        //    the end offset is unknown; neither may be silently coerced to 0.
        let mut end_map: HashMap<(&str, i32), std::result::Result<i64, String>> = HashMap::new();
        for r in &end_offsets {
            let value = match &r.error {
                Some(e) => Err(e.clone()),
                None if r.offset < 0 => {
                    Err(format!("ListOffsets returned sentinel offset {}", r.offset))
                }
                None => Ok(r.offset),
            };
            end_map.insert((r.topic.as_str(), r.partition), value);
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

            let (end_offset, end_offset_error) =
                match end_map.get(&(entry.topic.as_str(), entry.partition)) {
                    Some(Ok(offset)) => (Some(*offset), None),
                    Some(Err(reason)) => (None, Some(reason.clone())),
                    None => (
                        None,
                        Some("no ListOffsets result for this partition".to_string()),
                    ),
                };

            // Lag is only meaningful when *both* ends are known. An unknown end
            // offset must not report lag 0 — that hides a stalled consumer from
            // alerting.
            let lag = match (committed_offset, end_offset) {
                (Some(co), Some(eo)) => Some((eo - co).max(0)),
                _ => None,
            };

            if let Some(ref reason) = end_offset_error {
                warn!(
                    topic = %entry.topic,
                    partition = entry.partition,
                    "end offset unknown, lag cannot be computed: {reason}"
                );
            }

            lag_results.push(ConsumerGroupLag {
                topic: entry.topic.clone(),
                partition: entry.partition,
                committed_offset,
                end_offset,
                end_offset_error,
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
            end_offset: Some(150),
            end_offset_error: None,
            lag: Some(50),
        };
        assert_eq!(lag.lag, Some(50));
        assert_eq!(lag.end_offset, Some(150));
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

    /// `end_offset` previously defaulted to -1 when ListOffsets failed,
    /// and `lag = (end_offset - committed).max(0)` then reported 0 — making a
    /// stalled consumer look perfectly healthy to alerting.
    #[test]
    fn test_unknown_end_offset_reports_unknown_lag_not_zero() {
        let committed = 100i64;

        // What the old code did.
        let old_end_offset = -1i64;
        let old_lag = (old_end_offset - committed).max(0);
        assert_eq!(old_lag, 0, "this is the bug being fixed");

        // What the new code does: no end offset means no lag.
        let end_offset: Option<i64> = None;
        let new_lag = match (Some(committed), end_offset) {
            (Some(co), Some(eo)) => Some((eo - co).max(0)),
            _ => None,
        };
        assert_eq!(new_lag, None);
    }

    #[test]
    fn test_lag_is_computed_when_both_ends_are_known() {
        let lag = match (Some(100i64), Some(150i64)) {
            (Some(co), Some(eo)) => Some((eo - co).max(0)),
            _ => None,
        };
        assert_eq!(lag, Some(50));
    }

    /// A commit ahead of the watermark (e.g. after a manual offset reset)
    /// clamps to zero rather than reporting negative lag.
    #[test]
    fn test_lag_clamps_negative_to_zero() {
        let lag = match (Some(200i64), Some(150i64)) {
            (Some(co), Some(eo)) => Some((eo - co).max(0)),
            _ => None,
        };
        assert_eq!(lag, Some(0));
    }

    /// A negative offset from ListOffsets is a sentinel, not a position, and
    /// must be treated as "unknown" just like an explicit error.
    #[test]
    fn test_negative_sentinel_offset_counts_as_unknown() {
        let classify = |error: Option<&str>, offset: i64| -> std::result::Result<i64, String> {
            match error {
                Some(e) => Err(e.to_string()),
                None if offset < 0 => Err(format!("ListOffsets returned sentinel offset {offset}")),
                None => Ok(offset),
            }
        };

        assert_eq!(classify(None, 150), Ok(150));
        assert!(
            classify(None, -1).is_err(),
            "sentinel must not be a position"
        );
        assert!(classify(Some("NotLeaderForPartition"), 150).is_err());
    }

    /// A partition missing entirely from the ListOffsets response must also
    /// report unknown, not silently inherit a default.
    #[test]
    fn test_missing_partition_reports_unknown_end_offset() {
        let end_map: HashMap<(&str, i32), std::result::Result<i64, String>> = HashMap::new();

        let (end_offset, err) = match end_map.get(&("orders", 0)) {
            Some(Ok(o)) => (Some(*o), None),
            Some(Err(e)) => (None, Some(e.clone())),
            None => (
                None,
                Some("no ListOffsets result for this partition".to_string()),
            ),
        };

        assert_eq!(end_offset, None);
        assert!(err.is_some());
    }

    /// The cached leader epoch must be sent so the broker can fence a stale
    /// request (KIP-320). Pinning -1 disabled that protection entirely.
    #[test]
    fn test_list_offsets_request_carries_the_leader_epoch() {
        let request = ListOffsetsRequest {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsRequestTopic {
                name: "orders".into(),
                partitions: vec![
                    ListOffsetsRequestPartition {
                        partition_index: 0,
                        current_leader_epoch: 42,
                        timestamp: OffsetSpec::Latest.as_timestamp(),
                    },
                    ListOffsetsRequestPartition {
                        partition_index: 1,
                        // -1 only when the epoch is genuinely unknown.
                        current_leader_epoch: -1,
                        timestamp: OffsetSpec::Latest.as_timestamp(),
                    },
                ],
            }],
            timeout_ms: None,
        };

        assert_eq!(request.topics[0].partitions[0].current_leader_epoch, 42);
        assert_eq!(request.topics[0].partitions[1].current_leader_epoch, -1);

        let mut buf = Vec::new();
        assert!(
            request
                .encode_versioned(versions::LIST_OFFSETS_MAX, &mut buf)
                .is_ok(),
            "ListOffsets must encode"
        );
        assert!(!buf.is_empty());
    }

    /// Now that a real epoch is sent, the broker can reject with a fenced or
    /// unknown epoch; all three codes mean "metadata is stale" and must trigger
    /// the same refresh-and-retry.
    #[test]
    fn test_stale_leader_detection_covers_epoch_errors() {
        let is_stale = |c: ErrorCode| {
            matches!(
                c,
                ErrorCode::NotLeaderForPartition
                    | ErrorCode::FencedLeaderEpoch
                    | ErrorCode::UnknownLeaderEpoch
            )
        };

        assert!(is_stale(ErrorCode::NotLeaderForPartition));
        assert!(is_stale(ErrorCode::FencedLeaderEpoch));
        assert!(is_stale(ErrorCode::UnknownLeaderEpoch));

        assert!(!is_stale(ErrorCode::None));
        assert!(!is_stale(ErrorCode::OffsetOutOfRange));
        assert!(!is_stale(ErrorCode::TopicAuthorizationFailed));
    }
}
