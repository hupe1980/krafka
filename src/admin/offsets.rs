//! Partition offsets: list, delete records, end offset per leader epoch.

use std::collections::{BTreeMap, HashMap};

use crate::consumer::TopicPartition;
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    ApiKey, DeleteRecordsPartition, DeleteRecordsRequest, DeleteRecordsResponse,
    DeleteRecordsTopic, ListOffsetsRequest, ListOffsetsRequestPartition, ListOffsetsRequestTopic,
    ListOffsetsResponse, OffsetForLeaderEpochPartition, OffsetForLeaderEpochRequest,
    OffsetForLeaderEpochResponse, OffsetForLeaderEpochTopic, versions,
};

use super::driver::{Mode, Target, answer, exchange, negotiate};
use super::{AdminClient, validate_topics};

/// Which offset [`AdminClient::list_offsets`] looks up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum OffsetSpec {
    /// The earliest offset (log start).
    Earliest,
    /// The end offset (high watermark, or last stable offset with
    /// `read_committed`).
    Latest,
    /// The first offset whose timestamp is at or after these milliseconds
    /// since the Unix epoch.
    Timestamp(i64),
    /// The offset of the record with the largest timestamp (KIP-734,
    /// `ListOffsets` v7+).
    MaxTimestamp,
    /// The earliest offset still in local storage (KIP-405, `ListOffsets`
    /// v8+).
    EarliestLocal,
    /// The last offset copied to remote storage (KIP-1005, `ListOffsets` v9+).
    LatestTiered,
    /// The earliest offset not yet copied to remote storage (KIP-1023,
    /// `ListOffsets` v11+).
    EarliestPendingUpload,
}

impl OffsetSpec {
    /// The wire `timestamp` field for this spec.
    fn as_timestamp(self) -> i64 {
        match self {
            OffsetSpec::Earliest => -2,
            OffsetSpec::Latest => -1,
            OffsetSpec::MaxTimestamp => -3,
            OffsetSpec::EarliestLocal => -4,
            OffsetSpec::LatestTiered => -5,
            OffsetSpec::EarliestPendingUpload => -6,
            OffsetSpec::Timestamp(ts) => ts,
        }
    }

    /// Lowest `ListOffsets` version that understands this spec. A broker
    /// below it would read the negative sentinel as a timestamp and answer
    /// with the log start, so such a partition fails instead.
    fn min_api_version(self) -> i16 {
        match self {
            OffsetSpec::Earliest | OffsetSpec::Latest | OffsetSpec::Timestamp(_) => {
                versions::LIST_OFFSETS_MIN
            }
            OffsetSpec::MaxTimestamp => 7,
            OffsetSpec::EarliestLocal => 8,
            OffsetSpec::LatestTiered => 9,
            OffsetSpec::EarliestPendingUpload => 11,
        }
    }
}

/// An offset from [`AdminClient::list_offsets`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListedOffset {
    /// The offset.
    pub offset: i64,
    /// The timestamp of the record at `offset`, when the spec has one.
    pub timestamp: Option<i64>,
    /// The leader epoch of the record at `offset`, when known.
    pub leader_epoch: Option<i32>,
}

/// The end of a leader epoch, from [`AdminClient::offset_for_leader_epoch`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpochEndOffset {
    /// The epoch the end offset belongs to: the requested one, or the largest
    /// epoch below it when the log was truncated.
    pub leader_epoch: i32,
    /// The end offset of that epoch.
    pub end_offset: i64,
}

admin_options! {
    /// Options for [`AdminClient::list_offsets`].
    ListOffsetsOptions {
        /// Report `Latest` as the last stable offset, below any open
        /// transaction.
        read_committed: bool,
    }
}

admin_options! {
    /// Options for [`AdminClient::delete_records`].
    DeleteRecordsOptions {}
}

admin_options! {
    /// Options for [`AdminClient::offset_for_leader_epoch`].
    OffsetForLeaderEpochOptions {}
}

/// Group partition keys by topic, keeping each key's value.
fn by_topic<V: Copy>(
    keys: &[TopicPartition],
    value: impl Fn(&TopicPartition) -> V,
) -> BTreeMap<String, Vec<(i32, V)>> {
    let mut topics: BTreeMap<String, Vec<(i32, V)>> = BTreeMap::new();
    for tp in keys {
        topics
            .entry(tp.topic.clone())
            .or_default()
            .push((tp.partition, value(tp)));
    }
    topics
}

impl AdminClient {
    /// Look up offsets per partition at each partition's leader.
    ///
    /// Returns a result per partition: one partition with no reachable leader
    /// does not fail the others.
    ///
    /// ```rust,no_run
    /// # use krafka::admin::{AdminClient, ListOffsetsOptions, OffsetSpec, TopicPartition};
    /// # async fn example(admin: &AdminClient) -> Result<(), krafka::error::KrafkaError> {
    /// let ends = admin
    ///     .list_offsets(
    ///         (0..3).map(|p| (TopicPartition::new("orders", p), OffsetSpec::Latest)),
    ///         ListOffsetsOptions::default(),
    ///     )
    ///     .await?;
    /// # let _ = ends;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// The call fails for a closed client or an invalid topic name.
    pub async fn list_offsets(
        &self,
        specs: impl IntoIterator<Item = (TopicPartition, OffsetSpec)>,
        options: ListOffsetsOptions,
    ) -> Result<HashMap<TopicPartition, Result<ListedOffset>>> {
        let specs: HashMap<TopicPartition, OffsetSpec> = specs.into_iter().collect();
        validate_topics(specs.keys().map(|tp| tp.topic.as_str()))?;
        let call = self.call("ListOffsets", Mode::Read, options.timeout)?;
        let isolation_level = i8::from(options.read_committed);
        let specs_ref = &specs;
        Ok(call
            .fan_out(
                specs.keys().cloned().collect(),
                |tp| Target::Leader(tp.clone()),
                |conn, partitions| async move {
                    let version = negotiate(
                        &conn,
                        ApiKey::ListOffsets,
                        versions::LIST_OFFSETS_MIN,
                        versions::LIST_OFFSETS_MAX,
                    )?;
                    let (supported, unsupported): (Vec<TopicPartition>, Vec<TopicPartition>) =
                        partitions
                            .into_iter()
                            .partition(|tp| version >= specs_ref[tp].min_api_version());
                    let mut results: Vec<(TopicPartition, Result<ListedOffset>)> = unsupported
                        .into_iter()
                        .map(|tp| {
                            let error = KrafkaError::protocol_kind(
                                ProtocolErrorKind::UnknownApiVersion,
                                format!(
                                    "{:?} needs ListOffsets v{}, the broker negotiated v{version}",
                                    specs_ref[&tp],
                                    specs_ref[&tp].min_api_version()
                                ),
                            );
                            (tp, Err(error))
                        })
                        .collect();
                    if supported.is_empty() {
                        return Ok(results);
                    }

                    let metadata = &self.metadata;
                    let request = ListOffsetsRequest {
                        replica_id: -1,
                        isolation_level,
                        topics: by_topic(&supported, |tp| {
                            (
                                specs_ref[tp].as_timestamp(),
                                metadata.leader_epoch(&tp.topic, tp.partition).unwrap_or(-1),
                            )
                        })
                        .into_iter()
                        .map(|(name, partitions)| ListOffsetsRequestTopic {
                            name,
                            partitions: partitions
                                .into_iter()
                                .map(|(partition_index, (timestamp, current_leader_epoch))| {
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
                    let response: ListOffsetsResponse =
                        exchange(&conn, ApiKey::ListOffsets, version, &request).await?;
                    for topic in response.topics {
                        for p in topic.partitions {
                            let result = answer(p.error_code, None).and_then(|()| {
                                if p.offset < 0 {
                                    Err(KrafkaError::broker(
                                        ErrorCode::OffsetNotAvailable,
                                        "the broker reported no offset for this spec",
                                    ))
                                } else {
                                    Ok(ListedOffset {
                                        offset: p.offset,
                                        timestamp: (p.timestamp >= 0).then_some(p.timestamp),
                                        leader_epoch: (p.leader_epoch >= 0)
                                            .then_some(p.leader_epoch),
                                    })
                                }
                            });
                            results.push((
                                TopicPartition::new(topic.name.clone(), p.partition_index),
                                result,
                            ));
                        }
                    }
                    Ok(results)
                },
            )
            .await)
    }

    /// Delete the records below the given offsets, at each partition's leader.
    ///
    /// Returns the new log start offset (low watermark) per partition.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client or an invalid topic name.
    pub async fn delete_records(
        &self,
        before: impl IntoIterator<Item = (TopicPartition, i64)>,
        options: DeleteRecordsOptions,
    ) -> Result<HashMap<TopicPartition, Result<i64>>> {
        let before: HashMap<TopicPartition, i64> = before.into_iter().collect();
        validate_topics(before.keys().map(|tp| tp.topic.as_str()))?;
        let call = self.call("DeleteRecords", Mode::Write, options.timeout)?;
        let before_ref = &before;
        let call_ref = &call;
        Ok(call
            .fan_out(
                before.keys().cloned().collect(),
                |tp| Target::Leader(tp.clone()),
                |conn, partitions| async move {
                    let request = DeleteRecordsRequest {
                        topics: by_topic(&partitions, |tp| before_ref[tp])
                            .into_iter()
                            .map(|(name, partitions)| DeleteRecordsTopic {
                                name,
                                partitions: partitions
                                    .into_iter()
                                    .map(|(partition_index, offset)| DeleteRecordsPartition {
                                        partition_index,
                                        offset,
                                    })
                                    .collect(),
                            })
                            .collect(),
                        timeout_ms: call_ref.remaining_ms(),
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::DeleteRecords,
                        versions::DELETE_RECORDS_MIN,
                        versions::DELETE_RECORDS_MAX,
                    )?;
                    let response: DeleteRecordsResponse =
                        exchange(&conn, ApiKey::DeleteRecords, version, &request).await?;
                    Ok(response
                        .topics
                        .into_iter()
                        .flat_map(|t| {
                            let name = t.name;
                            t.partitions.into_iter().map(move |p| {
                                (
                                    TopicPartition::new(name.clone(), p.partition_index),
                                    answer(p.error_code, None).map(|()| p.low_watermark),
                                )
                            })
                        })
                        .collect())
                },
            )
            .await)
    }

    /// Find where each given leader epoch ends, at each partition's leader —
    /// how a consumer detects log truncation after a leader change.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client or an invalid topic name.
    pub async fn offset_for_leader_epoch(
        &self,
        epochs: impl IntoIterator<Item = (TopicPartition, i32)>,
        options: OffsetForLeaderEpochOptions,
    ) -> Result<HashMap<TopicPartition, Result<EpochEndOffset>>> {
        let epochs: HashMap<TopicPartition, i32> = epochs.into_iter().collect();
        validate_topics(epochs.keys().map(|tp| tp.topic.as_str()))?;
        let call = self.call("OffsetForLeaderEpoch", Mode::Read, options.timeout)?;
        let epochs_ref = &epochs;
        Ok(call
            .fan_out(
                epochs.keys().cloned().collect(),
                |tp| Target::Leader(tp.clone()),
                |conn, partitions| async move {
                    let request = OffsetForLeaderEpochRequest {
                        replica_id: -1,
                        topics: by_topic(&partitions, |tp| epochs_ref[tp])
                            .into_iter()
                            .map(|(topic, partitions)| OffsetForLeaderEpochTopic {
                                topic,
                                partitions: partitions
                                    .into_iter()
                                    .map(|(partition, leader_epoch)| {
                                        OffsetForLeaderEpochPartition {
                                            partition,
                                            current_leader_epoch: -1,
                                            leader_epoch,
                                        }
                                    })
                                    .collect(),
                            })
                            .collect(),
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::OffsetForLeaderEpoch,
                        versions::OFFSET_FOR_LEADER_EPOCH_MIN,
                        versions::OFFSET_FOR_LEADER_EPOCH_MAX,
                    )?;
                    let response: OffsetForLeaderEpochResponse =
                        exchange(&conn, ApiKey::OffsetForLeaderEpoch, version, &request).await?;
                    Ok(response
                        .topics
                        .into_iter()
                        .flat_map(|t| {
                            let name = t.topic;
                            t.partitions.into_iter().map(move |p| {
                                (
                                    TopicPartition::new(name.clone(), p.partition),
                                    answer(p.error_code, None).map(|()| EpochEndOffset {
                                        leader_epoch: p.leader_epoch,
                                        end_offset: p.end_offset,
                                    }),
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
    fn offset_spec_sentinels_match_the_protocol_and_carry_their_minimum_version() {
        assert_eq!(OffsetSpec::Earliest.as_timestamp(), -2);
        assert_eq!(OffsetSpec::Latest.as_timestamp(), -1);
        assert_eq!(OffsetSpec::MaxTimestamp.as_timestamp(), -3);
        assert_eq!(OffsetSpec::EarliestLocal.as_timestamp(), -4);
        assert_eq!(OffsetSpec::LatestTiered.as_timestamp(), -5);
        assert_eq!(OffsetSpec::EarliestPendingUpload.as_timestamp(), -6);
        assert_eq!(OffsetSpec::Timestamp(1_700).as_timestamp(), 1_700);

        assert_eq!(OffsetSpec::MaxTimestamp.min_api_version(), 7);
        assert_eq!(OffsetSpec::EarliestLocal.min_api_version(), 8);
        assert_eq!(OffsetSpec::LatestTiered.min_api_version(), 9);
        assert_eq!(OffsetSpec::EarliestPendingUpload.min_api_version(), 11);
        for spec in [
            OffsetSpec::Earliest,
            OffsetSpec::Latest,
            OffsetSpec::Timestamp(0),
            OffsetSpec::MaxTimestamp,
            OffsetSpec::EarliestLocal,
            OffsetSpec::LatestTiered,
            OffsetSpec::EarliestPendingUpload,
        ] {
            assert!(spec.min_api_version() <= versions::LIST_OFFSETS_MAX);
        }
    }

    #[test]
    fn partitions_group_by_topic_with_their_values() {
        let keys = [
            TopicPartition::new("b", 0),
            TopicPartition::new("a", 1),
            TopicPartition::new("a", 0),
        ];
        let grouped = by_topic(&keys, |tp| tp.partition * 10);
        assert_eq!(grouped["a"].len(), 2);
        assert_eq!(grouped["b"], vec![(0, 0)]);
    }
}
