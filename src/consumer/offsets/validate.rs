//! Leader-epoch validation of positions (KIP-320): before fetching from a
//! position that was just set, ask the leader whether its log still contains
//! it, and rewind if it does not.

use tokio::time::Instant;

use tracing::{debug, warn};

use super::reset::needs_metadata_refresh;
use crate::consumer::Consumer;
use crate::consumer::state::PartitionKey;
use crate::error::{KrafkaError, Result};
use crate::protocol::{
    ApiKey, OffsetForLeaderEpochPartition, OffsetForLeaderEpochRequest,
    OffsetForLeaderEpochResponse, OffsetForLeaderEpochTopic, VersionedDecode, VersionedEncode,
    versions,
};
use crate::{Offset, PartitionId};

/// What validating one position found.
#[derive(Debug, PartialEq, Eq)]
enum Validation {
    /// The position exists in the leader's log, or cannot be checked (no
    /// leader epoch known, or the broker does not serve the API).
    Valid,
    /// The leader's log ends at this offset for the position's epoch; the
    /// position is beyond it.
    Truncated(Offset),
    /// The broker answered with an error for the partition.
    Rejected(crate::error::ErrorCode),
}

impl Consumer {
    /// Validate every positioned, unvalidated partition that is not backing
    /// off, concurrently.
    ///
    /// A position beyond the leader's log is rewound to where the logs part
    /// (no `auto_offset_reset`: that offset is valid). An error answer counts
    /// as a failed attempt and backs the partition off; a fenced or unknown
    /// epoch also forces a metadata refresh. A transport failure leaves the
    /// partition unvalidated for the next poll without blocking its fetch.
    pub(in crate::consumer) async fn validate_positions(&self) {
        let pending = self.state.lock().unvalidated(Instant::now());
        if pending.is_empty() {
            return;
        }
        let results = futures::future::join_all(
            pending
                .iter()
                .map(|(key, _, position, epoch)| self.validate_position(key, *position, *epoch)),
        )
        .await;

        let mut refresh: Vec<String> = Vec::new();
        {
            let now = Instant::now();
            let mut state = self.state.lock();
            for ((key, version, position, _), result) in pending.into_iter().zip(results) {
                match result {
                    Ok(Validation::Valid) => state.mark_validated(&key, version),
                    Ok(Validation::Truncated(end_offset)) => {
                        warn!(
                            topic = %key.0,
                            partition = key.1,
                            old_position = position,
                            new_position = end_offset,
                            "Log truncation detected: the position is past the end of its \
                             leader epoch; rewinding"
                        );
                        if state.truncate(&key, Some(version), end_offset).is_some() {
                            self.metrics.record_seek(1);
                        }
                    }
                    Ok(Validation::Rejected(code)) => {
                        debug!(
                            topic = %key.0,
                            partition = key.1,
                            ?code,
                            "OffsetForLeaderEpoch rejected; backing off"
                        );
                        if needs_metadata_refresh(code) && !refresh.contains(&key.0) {
                            refresh.push(key.0.clone());
                        }
                        state.back_off(&key, Some(version), now);
                    }
                    Err(e) => {
                        debug!(
                            topic = %key.0,
                            partition = key.1,
                            error = %e,
                            "Offset validation failed; retrying on the next poll"
                        );
                    }
                }
            }
        }
        for topic in refresh {
            if let Err(e) = self.metadata.force_refresh(Some(&[&topic])).await {
                debug!(topic = %topic, error = %e, "metadata refresh after validation failed");
            }
        }
        self.update_gauges();
    }

    /// Ask the leader where the epoch of the position ends.
    ///
    /// The request pairs the epoch the position came from (falling back to
    /// the current leader epoch when nothing has been consumed yet) with the
    /// current leader epoch.
    async fn validate_position(
        &self,
        key: &PartitionKey,
        position: Offset,
        position_epoch: Option<i32>,
    ) -> Result<Validation> {
        let (topic, partition): (&str, PartitionId) = (&key.0, key.1);
        let Some(leader_epoch) = self.metadata.leader_epoch(topic, partition) else {
            return Ok(Validation::Valid);
        };
        let leader_id = self.metadata.leader(topic, partition).ok_or_else(|| {
            KrafkaError::broker(
                crate::error::ErrorCode::LeaderNotAvailable,
                format!("no leader for {topic}-{partition}"),
            )
        })?;
        let broker = self
            .metadata
            .broker(leader_id)
            .ok_or_else(|| KrafkaError::unavailable(format!("broker {leader_id} not found")))?;
        let conn = self
            .pool
            .get_connection_by_id(leader_id, broker.address())
            .await?;
        // A broker that does not serve the API cannot validate; fetching
        // still carries `last_fetched_epoch`, which catches divergence.
        let Some(version) = conn.negotiate_api_version(
            ApiKey::OffsetForLeaderEpoch,
            versions::OFFSET_FOR_LEADER_EPOCH_MAX,
            versions::OFFSET_FOR_LEADER_EPOCH_MIN,
        ) else {
            return Ok(Validation::Valid);
        };

        let request = OffsetForLeaderEpochRequest {
            replica_id: -1,
            topics: vec![OffsetForLeaderEpochTopic {
                topic: topic.to_string(),
                partitions: vec![OffsetForLeaderEpochPartition {
                    partition,
                    current_leader_epoch: leader_epoch,
                    leader_epoch: position_epoch.filter(|e| *e >= 0).unwrap_or(leader_epoch),
                }],
            }],
        };
        let response = conn
            .send_request(ApiKey::OffsetForLeaderEpoch, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;
        let mut buf = response;
        let response = OffsetForLeaderEpochResponse::decode_versioned(version, &mut buf)?;

        for topic_result in response.topics {
            for result in topic_result.partitions {
                if result.partition != partition {
                    continue;
                }
                if !result.error_code.is_ok() {
                    return Ok(Validation::Rejected(result.error_code));
                }
                if result.end_offset >= 0 && position > result.end_offset {
                    return Ok(Validation::Truncated(result.end_offset));
                }
                return Ok(Validation::Valid);
            }
        }
        Ok(Validation::Valid)
    }
}
