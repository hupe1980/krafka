//! The consumer's one `ListOffsets` path, and how a partition without a
//! position gets one: the group's committed offset, else a configured initial
//! offset, else `auto_offset_reset`.

use tokio::time::Instant;

use ahash::AHashMap as HashMap;
use tracing::{debug, warn};

use crate::consumer::Consumer;
use crate::consumer::state::{OffsetReset, PartitionKey, PendingPosition};
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    ApiKey, ListOffsetsRequest, ListOffsetsRequestPartition, ListOffsetsRequestTopic,
    ListOffsetsResponse, VersionedDecode, VersionedEncode, versions,
};
use crate::{BrokerId, Offset, PartitionId};

/// Errors after which the client's metadata for the topic is known to be wrong
/// and has to be refreshed before a retry can succeed.
pub(in crate::consumer) fn needs_metadata_refresh(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::FencedLeaderEpoch
            | ErrorCode::UnknownLeaderEpoch
            | ErrorCode::NotLeaderForPartition
            | ErrorCode::LeaderNotAvailable
            | ErrorCode::UnknownTopicOrPartition
    )
}

/// Build one broker's `ListOffsets` request.
///
/// Every partition carries the leader epoch the client believes current
/// (KIP-320), or `-1` when metadata has none, so a stale leader fences the
/// request instead of answering from a log this client's view of leadership
/// says nothing about. The encoder only writes the field from v4.
fn build_request(
    partitions: &[PartitionKey],
    timestamp: i64,
    isolation_level: i8,
    leader_epoch: impl Fn(&str, PartitionId) -> Option<i32>,
) -> ListOffsetsRequest {
    let mut topics: HashMap<String, Vec<ListOffsetsRequestPartition>> = HashMap::new();
    for (topic, partition) in partitions {
        topics
            .entry(topic.clone())
            .or_default()
            .push(ListOffsetsRequestPartition {
                partition_index: *partition,
                current_leader_epoch: leader_epoch(topic, *partition).unwrap_or(-1),
                timestamp,
            });
    }
    ListOffsetsRequest {
        replica_id: -1,
        isolation_level,
        topics: topics
            .into_iter()
            .map(|(name, partitions)| ListOffsetsRequestTopic { name, partitions })
            .collect(),
        timeout_ms: None,
    }
}

/// Fold a `ListOffsets` response into `result`, returning the topics whose
/// metadata has to be refreshed before a retry can succeed.
fn apply_response(
    response: &ListOffsetsResponse,
    result: &mut HashMap<PartitionKey, Result<Offset>>,
) -> Vec<String> {
    let mut refresh: Vec<String> = Vec::new();
    for topic in &response.topics {
        for partition in &topic.partitions {
            let key = (topic.name.clone(), partition.partition_index);
            if partition.error_code.is_ok() {
                result.insert(key, Ok(partition.offset));
                continue;
            }
            debug!(
                "ListOffsets error for {}-{}: {:?}",
                topic.name, partition.partition_index, partition.error_code
            );
            if needs_metadata_refresh(partition.error_code) && !refresh.contains(&topic.name) {
                refresh.push(topic.name.clone());
            }
            result.insert(
                key,
                Err(KrafkaError::broker(
                    partition.error_code,
                    format!(
                        "ListOffsets error for {}-{}",
                        topic.name, partition.partition_index
                    ),
                )),
            );
        }
    }
    refresh
}

impl Consumer {
    /// Resolve `timestamp` (`-1` latest, `-2` earliest, else a time in ms) to
    /// an offset for each partition, one request per leader.
    ///
    /// Every requested partition is in the result. A fenced or unknown leader
    /// epoch, or an error that means the client's leadership view is wrong,
    /// forces a metadata refresh for the topic, so the caller's retry goes to
    /// the right leader with the right epoch.
    pub(in crate::consumer) async fn list_offsets(
        &self,
        partitions: &HashMap<String, Vec<PartitionId>>,
        timestamp: i64,
    ) -> HashMap<PartitionKey, Result<Offset>> {
        let mut result: HashMap<PartitionKey, Result<Offset>> = HashMap::new();
        if partitions.is_empty() {
            return result;
        }

        let mut by_leader: std::collections::BTreeMap<BrokerId, Vec<PartitionKey>> =
            std::collections::BTreeMap::new();
        let mut leaderless: Vec<PartitionKey> = Vec::new();
        for (topic, parts) in partitions {
            for &p in parts {
                result.insert(
                    (topic.clone(), p),
                    Err(KrafkaError::broker(
                        ErrorCode::LeaderNotAvailable,
                        format!("no leader found for {topic}-{p}"),
                    )),
                );
                match self.metadata.leader(topic, p) {
                    Some(leader) => by_leader
                        .entry(leader)
                        .or_default()
                        .push((topic.clone(), p)),
                    None => leaderless.push((topic.clone(), p)),
                }
            }
        }

        if !leaderless.is_empty() {
            let mut topics: Vec<&str> = leaderless.iter().map(|(t, _)| t.as_str()).collect();
            topics.sort_unstable();
            topics.dedup();
            if let Err(e) = self.metadata.force_refresh(Some(&topics)).await {
                warn!(
                    "Metadata refresh for leaderless topics {:?} failed: {}",
                    topics, e
                );
            }
            for (topic, partition) in leaderless {
                if let Some(leader) = self.metadata.leader(&topic, partition) {
                    by_leader
                        .entry(leader)
                        .or_default()
                        .push((topic, partition));
                }
            }
        }

        let mut refresh: Vec<String> = Vec::new();
        for (leader_id, leader_partitions) in &by_leader {
            let request = build_request(
                leader_partitions,
                timestamp,
                self.config.isolation_level.to_i8(),
                |topic, partition| self.metadata.leader_epoch(topic, partition),
            );
            match self.send_list_offsets(*leader_id, &request).await {
                Ok(response) => {
                    for topic in apply_response(&response, &mut result) {
                        if !refresh.contains(&topic) {
                            refresh.push(topic);
                        }
                    }
                }
                Err(e) => {
                    warn!("ListOffsets to broker {} failed: {}", leader_id, e);
                    for key in leader_partitions {
                        result.insert(key.clone(), Err(e.clone()));
                    }
                }
            }
        }

        for topic in refresh {
            if let Err(e) = self.metadata.force_refresh(Some(&[&topic])).await {
                debug!(topic = %topic, error = %e, "metadata refresh after ListOffsets failed");
            }
        }
        result
    }

    async fn send_list_offsets(
        &self,
        leader_id: BrokerId,
        request: &ListOffsetsRequest,
    ) -> Result<ListOffsetsResponse> {
        let broker = self.metadata.broker(leader_id).ok_or_else(|| {
            KrafkaError::unavailable(format!("broker {leader_id} not found in metadata"))
        })?;
        let conn = self
            .pool
            .get_connection_by_id(leader_id, broker.address())
            .await?;
        let version = conn
            .negotiate_api_version(
                ApiKey::ListOffsets,
                versions::LIST_OFFSETS_MAX,
                versions::LIST_OFFSETS_MIN,
            )
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    format!("no mutually supported ListOffsets API version for broker {leader_id}"),
                )
            })?;
        let response = conn
            .send_request(ApiKey::ListOffsets, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;
        let mut buf = response;
        ListOffsetsResponse::decode_versioned(version, &mut buf)
    }

    /// [`list_offsets`](Self::list_offsets) for one partition.
    pub(in crate::consumer) async fn list_offset(
        &self,
        topic: &str,
        partition: PartitionId,
        timestamp: i64,
    ) -> Result<Offset> {
        let mut partitions = HashMap::new();
        partitions.insert(topic.to_string(), vec![partition]);
        self.list_offsets(&partitions, timestamp)
            .await
            .remove(&(topic.to_string(), partition))
            .unwrap_or_else(|| {
                Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::Malformed,
                    format!("no offset returned for {topic}-{partition}"),
                ))
            })
    }

    /// Give every assigned partition without a position one: the group's
    /// committed offset, else a configured initial offset, else the reset
    /// `auto_offset_reset` asks for; then resolve pending resets.
    ///
    /// `only` limits the work to one partition, for `position()`. Lookups
    /// that fail back the partition off (100 ms doubling to 30 s); results for
    /// a partition that moved or was reassigned meanwhile are discarded.
    ///
    /// # Errors
    ///
    /// [`KrafkaError::NoOffset`] for partitions that have no committed offset
    /// while `auto_offset_reset` is `None`.
    pub(in crate::consumer) async fn update_positions(
        &self,
        only: Option<&PartitionKey>,
    ) -> Result<()> {
        let now = Instant::now();
        let needing: Vec<PendingPosition> = self
            .state
            .lock()
            .partitions_needing_position(now)
            .into_iter()
            .filter(|p| only.is_none_or(|key| *key == p.key))
            .collect();

        let mut missing: Vec<PartitionKey> = Vec::new();
        if !needing.is_empty() {
            let without_committed = self.apply_committed_positions(&needing).await;
            let reset = OffsetReset::from_auto(self.config.auto_offset_reset);
            let mut state = self.state.lock();
            for pending in without_committed {
                if let Some(&initial) = self.config.initial_offsets.get(&pending.key) {
                    state.set_initial_position(&pending.key, pending.version, initial, None);
                } else if let Some(reset) = reset {
                    state.set_pending_reset(&pending.key, pending.version, reset);
                } else {
                    missing.push(pending.key);
                }
            }
        }

        let awaiting: Vec<(PendingPosition, OffsetReset)> = self
            .state
            .lock()
            .partitions_awaiting_reset(now)
            .into_iter()
            .filter(|(p, _)| only.is_none_or(|key| *key == p.key))
            .collect();
        let mut resets: Vec<OffsetReset> = Vec::new();
        for (_, reset) in &awaiting {
            if !resets.contains(reset) {
                resets.push(*reset);
            }
        }
        for reset in resets {
            let batch: Vec<&PendingPosition> = awaiting
                .iter()
                .filter(|(_, r)| *r == reset)
                .map(|(p, _)| p)
                .collect();
            let mut by_topic: HashMap<String, Vec<PartitionId>> = HashMap::new();
            for p in &batch {
                by_topic.entry(p.key.0.clone()).or_default().push(p.key.1);
            }
            let mut resolved = self.list_offsets(&by_topic, reset.timestamp()).await;
            if matches!(reset, OffsetReset::ByDuration(_)) {
                self.resolve_unmatched_to_latest(&mut resolved).await;
            }
            let now = Instant::now();
            let mut state = self.state.lock();
            for p in batch {
                match resolved.get(&p.key) {
                    Some(Ok(offset)) if *offset >= 0 => {
                        state.set_initial_position(&p.key, p.version, *offset, None);
                    }
                    other => {
                        debug!(
                            topic = %p.key.0,
                            partition = p.key.1,
                            result = ?other.map(|r| r.as_ref().map_err(ToString::to_string)),
                            "offset reset failed; backing off"
                        );
                        state.back_off(&p.key, Some(p.version), now);
                    }
                }
            }
        }

        self.update_gauges();
        if missing.is_empty() {
            Ok(())
        } else {
            Err(KrafkaError::no_offset(missing))
        }
    }

    /// A timestamp lookup answers offset `-1` for a partition with no record
    /// at or after the timestamp; such a partition resets to the end of the
    /// log (KIP-1106).
    async fn resolve_unmatched_to_latest(
        &self,
        resolved: &mut HashMap<PartitionKey, Result<Offset>>,
    ) {
        let mut by_topic: HashMap<String, Vec<PartitionId>> = HashMap::new();
        for (key, result) in resolved.iter() {
            if matches!(result, Ok(offset) if *offset < 0) {
                by_topic.entry(key.0.clone()).or_default().push(key.1);
            }
        }
        if by_topic.is_empty() {
            return;
        }
        resolved.extend(
            self.list_offsets(&by_topic, OffsetReset::Latest.timestamp())
                .await,
        );
    }

    /// Install the group's committed offsets for `needing`; returns the
    /// partitions the group has never committed.
    ///
    /// The committed leader epoch is installed with the offset (KIP-320), so
    /// the first fetch from it can be checked for divergence.
    async fn apply_committed_positions(&self, needing: &[PendingPosition]) -> Vec<PendingPosition> {
        let Some(coordinator) = self.group_coordinator.as_ref() else {
            return needing.to_vec();
        };
        let mut by_topic: HashMap<String, Vec<PartitionId>> = HashMap::new();
        for p in needing {
            by_topic.entry(p.key.0.clone()).or_default().push(p.key.1);
        }
        match coordinator.fetch_committed_offsets(&by_topic).await {
            Ok(committed) => {
                let mut state = self.state.lock();
                let mut without = Vec::new();
                for p in needing {
                    match committed.get(&p.key) {
                        Some(position) if position.offset >= 0 => {
                            let epoch =
                                (position.leader_epoch >= 0).then_some(position.leader_epoch);
                            state.set_initial_position(&p.key, p.version, position.offset, epoch);
                        }
                        _ => without.push(p.clone()),
                    }
                }
                without
            }
            Err(e) => {
                warn!("Fetching committed offsets failed; retrying with backoff: {e}");
                let now = Instant::now();
                let mut state = self.state.lock();
                for p in needing {
                    state.back_off(&p.key, Some(p.version), now);
                }
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::protocol::{ListOffsetsResponsePartition, ListOffsetsResponseTopic};

    fn response(partitions: Vec<(i32, ErrorCode, i64)>) -> ListOffsetsResponse {
        ListOffsetsResponse {
            topics: vec![ListOffsetsResponseTopic {
                name: "t".to_string(),
                partitions: partitions
                    .into_iter()
                    .map(
                        |(partition_index, error_code, offset)| ListOffsetsResponsePartition {
                            partition_index,
                            error_code,
                            timestamp: -1,
                            offset,
                            leader_epoch: -1,
                        },
                    )
                    .collect(),
            }],
        }
    }

    #[test]
    fn every_request_carries_the_leader_epoch_from_metadata() {
        let request = build_request(
            &[("t".to_string(), 0), ("t".to_string(), 1)],
            -2,
            1,
            |_, p| (p == 0).then_some(7),
        );
        let t = &request.topics[0];
        let epoch = |p: i32| {
            t.partitions
                .iter()
                .find(|x| x.partition_index == p)
                .unwrap()
                .current_leader_epoch
        };
        assert_eq!(epoch(0), 7);
        assert_eq!(epoch(1), -1, "no epoch in metadata means -1");
        assert!(t.partitions.iter().all(|p| p.timestamp == -2));
        assert_eq!(request.isolation_level, 1);
    }

    #[test]
    fn a_partial_failure_keeps_the_successes() {
        let mut result = HashMap::new();
        let refresh = apply_response(
            &response(vec![
                (0, ErrorCode::None, 42),
                (1, ErrorCode::NotLeaderForPartition, -1),
                (2, ErrorCode::None, 99),
            ]),
            &mut result,
        );
        assert_eq!(*result[&("t".to_string(), 0)].as_ref().unwrap(), 42);
        assert_eq!(*result[&("t".to_string(), 2)].as_ref().unwrap(), 99);
        assert!(result[&("t".to_string(), 1)].is_err());
        assert_eq!(
            refresh,
            vec!["t".to_string()],
            "a leader error needs new metadata"
        );
    }

    #[test]
    fn a_fenced_epoch_asks_for_a_refresh_and_other_errors_do_not() {
        let mut result = HashMap::new();
        let refresh = apply_response(
            &response(vec![(0, ErrorCode::FencedLeaderEpoch, -1)]),
            &mut result,
        );
        assert_eq!(refresh, vec!["t".to_string()]);
        let mut result = HashMap::new();
        let refresh = apply_response(
            &response(vec![(0, ErrorCode::OffsetNotAvailable, -1)]),
            &mut result,
        );
        assert!(refresh.is_empty());
        assert!(result[&("t".to_string(), 0)].is_err());
    }
}
