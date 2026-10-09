//! Fetching: one round of concurrent per-broker `Fetch` requests, decoded and
//! applied to the subscription state.
//!
//! A round fetches only partitions that are positioned, unpaused, not backing
//! off and have nothing buffered. Each response is installed only if the
//! partition is still at the position and version the request was built from.

mod decode;
mod plan;

use std::collections::VecDeque;
use std::time::Duration;
use tokio::time::Instant;

use ahash::AHashMap as HashMap;
use bytes::Bytes;
use tracing::{debug, trace, warn};

use crate::consumer::Consumer;
use crate::consumer::fetch_session::FetchSessionClose;
use crate::consumer::state::{CompletedFetch, FetchTarget, OffsetReset, PartitionKey};
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::metadata::broker_info_for_node;
use crate::protocol::{
    AbortedTransaction, ApiKey, DivergingEpoch, FetchPartitionRequest, FetchRequest, FetchResponse,
    FetchTopicRequest, LeaderIdAndEpoch, NodeEndpoint, VersionedDecode, VersionedEncode, versions,
};
use crate::{BrokerId, Offset};

use crate::consumer::offsets::needs_metadata_refresh;
use decode::decode_partition_batches;
use plan::build_fetch_routing_plan;

/// A partition whose batch at the fetch position could not be decoded, so it
/// cannot advance. Re-fetching returns the same bytes, so it is reported to
/// the application rather than retried in a loop.
#[derive(Debug)]
pub(super) struct FetchFault {
    key: PartitionKey,
    offset: Offset,
    error: KrafkaError,
}

impl FetchFault {
    /// The error `poll()` returns, naming the partition, the offset and both
    /// remedies.
    pub(super) fn into_error(self, total_faults: usize) -> KrafkaError {
        let (topic, partition) = self.key;
        let others = match total_faults {
            0 | 1 => String::new(),
            n => format!(" ({n} partitions affected in this poll)"),
        };
        KrafkaError::protocol_kind(
            self.error
                .protocol_error_kind()
                .unwrap_or(ProtocolErrorKind::Malformed),
            format!(
                "undecodable record batch at {topic}-{partition} offset {}{others}; \
                 the partition cannot advance past it and re-fetching returns the same \
                 bytes. Either seek({topic}-{partition}) past the offset to skip the \
                 corrupt data, or pause({topic}-{partition}) to keep consuming every \
                 other partition while you investigate. Underlying error: {}",
                self.offset, self.error
            ),
        )
    }
}

/// What a fetch round produced besides the records it installed.
#[derive(Debug, Default)]
pub(super) struct FetchRound {
    /// Partitions that cannot decode at their position.
    pub(super) faults: Vec<FetchFault>,
    /// Partitions answered out of range while `auto_offset_reset` is `None`.
    pub(super) no_offset: Vec<PartitionKey>,
    /// Whether any fetch request was sent.
    pub(super) requested: bool,
}

/// One partition of a fetch response, matched to the request it answers.
#[derive(Debug)]
struct FetchedPartition {
    target: FetchTarget,
    error_code: ErrorCode,
    high_watermark: Offset,
    last_stable_offset: Offset,
    log_start_offset: Offset,
    /// `None` below Fetch v11, where the field is not on the wire.
    preferred_read_replica: Option<BrokerId>,
    diverging_epoch: Option<DivergingEpoch>,
    current_leader: Option<LeaderIdAndEpoch>,
    records: Option<Bytes>,
    aborted_transactions: Vec<AbortedTransaction>,
}

/// One broker's response, matched to its targets.
#[derive(Debug)]
struct BrokerFetch {
    partitions: Vec<FetchedPartition>,
    node_endpoints: Vec<NodeEndpoint>,
}

/// What decoding one partition produced.
enum Decoded {
    Fetch(CompletedFetch),
    Fault(FetchFault),
    Nothing,
}

impl Consumer {
    /// Run one fetch round against every broker that leads (or is the
    /// preferred replica of) a fetchable partition, concurrently.
    ///
    /// `budget` bounds how many records the round decodes; `None` is
    /// unlimited. `wakeup()` interrupts the round with
    /// [`KrafkaError::Wakeup`]; nothing has been installed at that point.
    pub(super) async fn fetch_round(
        &self,
        max_wait: Duration,
        budget: Option<usize>,
    ) -> Result<FetchRound> {
        let targets = self.state.lock().fetch_targets(Instant::now());
        if targets.is_empty() {
            return Ok(FetchRound::default());
        }
        let leaders: HashMap<PartitionKey, BrokerId> = targets
            .iter()
            .filter_map(|t| {
                self.metadata
                    .leader(&t.key.0, t.key.1)
                    .map(|leader| (t.key.clone(), leader))
            })
            .collect();
        let plan = build_fetch_routing_plan(targets, &leaders);
        if !plan.skipped.is_empty() {
            let mut topics: Vec<&str> = plan.skipped.iter().map(|(t, _)| t.as_str()).collect();
            topics.sort_unstable();
            topics.dedup();
            debug!(?topics, "Partitions without a leader; refreshing metadata");
            let _ =
                tokio::time::timeout(max_wait, self.metadata.force_refresh(Some(&topics))).await;
        }
        if plan.by_broker.is_empty() {
            return Ok(FetchRound::default());
        }

        let fetches = plan
            .by_broker
            .iter()
            .map(|(broker_id, targets)| self.fetch_from_broker(*broker_id, targets, max_wait));
        let responses = tokio::select! {
            biased;
            () = self.wakeup_notify.notified() => return Err(KrafkaError::Wakeup),
            responses = futures::future::join_all(fetches) => responses,
        };

        let mut fetched: Vec<FetchedPartition> = Vec::new();
        let mut endpoints: Vec<NodeEndpoint> = Vec::new();
        for ((broker_id, targets), response) in plan.by_broker.iter().zip(responses) {
            match response {
                Ok(fetch) => {
                    fetched.extend(fetch.partitions);
                    endpoints.extend(fetch.node_endpoints);
                }
                Err(e) => {
                    self.metrics.record_error();
                    warn!("Fetch from broker {} failed: {}", broker_id, e);
                    // A dead preferred replica must not keep attracting
                    // fetches until it expires, and the partitions back off:
                    // a broker that refuses at once must not be asked again
                    // in a tight loop.
                    let now = Instant::now();
                    let mut state = self.state.lock();
                    for target in targets {
                        if target.preferred_replica.is_some() {
                            state.set_preferred_replica(&target.key, None);
                        }
                        state.back_off(&target.key, Some(target.version), now);
                    }
                }
            }
        }
        let mut round = self.apply_fetched(fetched, &endpoints, budget).await;
        round.requested = true;
        Ok(round)
    }

    /// Decode and install a round's responses, and act on partition errors.
    async fn apply_fetched(
        &self,
        fetched: Vec<FetchedPartition>,
        endpoints: &[NodeEndpoint],
        mut budget: Option<usize>,
    ) -> FetchRound {
        let mut round = FetchRound::default();

        // Decode outside the lock.
        let mut decoded: Vec<Decoded> = Vec::with_capacity(fetched.len());
        for p in &fetched {
            let records = match (&p.records, p.error_code.is_ok(), &p.diverging_epoch) {
                (Some(records), true, None) if budget != Some(0) => records.clone(),
                _ => {
                    decoded.push(Decoded::Nothing);
                    continue;
                }
            };
            let mut out = Vec::new();
            let outcome = decode_partition_batches(
                &p.target.key.0,
                p.target.key.1,
                records,
                p.target.offset,
                p.aborted_transactions.clone(),
                budget.as_mut(),
                self.config.max_decompressed_size,
                &mut out,
            );
            if outcome.corrupt {
                self.metrics.record_batch_decode_error();
            }
            decoded.push(match (outcome.error, outcome.last_offset) {
                (Some(error), _) => Decoded::Fault(FetchFault {
                    key: p.target.key.clone(),
                    offset: p.target.offset,
                    error,
                }),
                (None, Some(last)) => Decoded::Fetch(CompletedFetch {
                    records: VecDeque::from(out),
                    next_offset: last.saturating_add(1),
                    next_epoch: (outcome.last_epoch >= 0).then_some(outcome.last_epoch),
                }),
                (None, None) => Decoded::Nothing,
            });
        }

        let reset = OffsetReset::from_auto(self.config.auto_offset_reset);
        let mut refresh: Vec<String> = Vec::new();
        let mut hints: Vec<(PartitionKey, LeaderIdAndEpoch)> = Vec::new();
        {
            let now = Instant::now();
            let replica_expiry = now + self.metadata.max_age();
            let mut state = self.state.lock();
            for (p, decoded) in fetched.into_iter().zip(decoded) {
                let key = &p.target.key;
                state.update_watermarks(
                    key,
                    (p.high_watermark >= 0).then_some(p.high_watermark),
                    (p.last_stable_offset >= 0).then_some(p.last_stable_offset),
                    (p.log_start_offset >= 0).then_some(p.log_start_offset),
                    now,
                );
                if let Some(replica) = p.preferred_read_replica {
                    state.set_preferred_replica(
                        key,
                        (replica >= 0).then_some((replica, replica_expiry)),
                    );
                }

                // The broker compared our (position, epoch) with its log and
                // they diverge: everything from `end_offset` on was never in
                // the leader's log. Rewind there; it is a valid offset, so no
                // `auto_offset_reset`.
                if let Some(diverging) = p.diverging_epoch {
                    if let Some(old) =
                        state.truncate(key, Some(p.target.version), diverging.end_offset)
                    {
                        warn!(
                            topic = %key.0,
                            partition = key.1,
                            old_position = old,
                            new_position = diverging.end_offset,
                            diverging_epoch = diverging.epoch,
                            "Log truncation detected; rewinding the position"
                        );
                        self.metrics.record_seek(1);
                    }
                    continue;
                }

                if !p.error_code.is_ok() {
                    if p.target.preferred_replica.is_some() {
                        state.set_preferred_replica(key, None);
                    }
                    match p.error_code {
                        ErrorCode::OffsetOutOfRange => match reset {
                            Some(reset) => {
                                warn!(
                                    "Offset {} out of range for {}-{}; resetting",
                                    p.target.offset, key.0, key.1
                                );
                                state.reset_if_current(key, p.target.version, reset);
                            }
                            None => round.no_offset.push(key.clone()),
                        },
                        code if needs_metadata_refresh(code) => {
                            // A usable leader hint (KIP-951) routes the next
                            // fetch at once; without one, refresh and back off.
                            match p.current_leader.filter(|l| l.leader_id >= 0) {
                                Some(leader) => hints.push((key.clone(), leader)),
                                None => {
                                    debug!(
                                        "Fetch for {}-{} answered {:?}; refreshing metadata \
                                         and backing off",
                                        key.0, key.1, code
                                    );
                                    if matches!(
                                        code,
                                        ErrorCode::FencedLeaderEpoch
                                            | ErrorCode::UnknownLeaderEpoch
                                    ) {
                                        state.mark_unvalidated(key);
                                    }
                                    state.back_off(key, Some(p.target.version), now);
                                    if !refresh.contains(&key.0) {
                                        refresh.push(key.0.clone());
                                    }
                                }
                            }
                        }
                        code => {
                            warn!("Fetch error for {}-{}: {:?}", key.0, key.1, code);
                            state.back_off(key, Some(p.target.version), now);
                        }
                    }
                    continue;
                }

                match decoded {
                    Decoded::Fetch(fetch) => {
                        if !state.install_fetch(&p.target, fetch) {
                            trace!(
                                topic = %key.0,
                                partition = key.1,
                                "Discarding a fetch for a partition that moved meanwhile"
                            );
                        }
                    }
                    Decoded::Fault(fault) => round.faults.push(fault),
                    Decoded::Nothing => state.clear_backoff(key, p.target.version),
                }
            }
        }

        for (key, leader) in hints {
            debug!(
                "{}-{} is now led by node {} (epoch {})",
                key.0, key.1, leader.leader_id, leader.leader_epoch
            );
            self.metadata.apply_leader_hint(
                &key.0,
                key.1,
                leader.leader_id,
                leader.leader_epoch,
                broker_info_for_node(endpoints, leader.leader_id),
            );
        }
        for topic in refresh {
            if let Err(e) = self.metadata.force_refresh(Some(&[&topic])).await {
                debug!(topic = %topic, error = %e, "metadata refresh after a fetch error failed");
            }
        }
        self.update_gauges();
        round
    }

    /// Send one broker its fetch request and match the response to the
    /// targets.
    async fn fetch_from_broker(
        &self,
        broker_id: BrokerId,
        targets: &[FetchTarget],
        max_wait: Duration,
    ) -> Result<BrokerFetch> {
        self.metrics.record_fetch();
        let _timer = self.metrics.fetch_latency.start();

        // A leader learned from a fetch response (KIP-951) is registered in
        // the metadata cache with the endpoint the broker advertised.
        let address = self
            .metadata
            .broker(broker_id)
            .map(|b| b.address().to_string())
            .ok_or_else(|| KrafkaError::unavailable(format!("broker {broker_id} not found")))?;
        let conn = self.pool.get_connection_by_id(broker_id, &address).await?;

        let mut topic_order: Vec<String> = Vec::new();
        let mut by_topic: HashMap<String, Vec<FetchPartitionRequest>> = HashMap::new();
        for target in targets {
            let (topic, partition) = &target.key;
            if !by_topic.contains_key(topic) {
                topic_order.push(topic.clone());
            }
            by_topic
                .entry(topic.clone())
                .or_default()
                .push(FetchPartitionRequest {
                    partition: *partition,
                    // Fences reads from a stale leader (KIP-320).
                    current_leader_epoch: self
                        .metadata
                        .leader_epoch(topic, *partition)
                        .unwrap_or(-1),
                    fetch_offset: target.offset,
                    // Lets the broker detect a diverged log; -1 when the epoch
                    // at the position is unknown.
                    last_fetched_epoch: target.last_fetched_epoch,
                    log_start_offset: -1,
                    partition_max_bytes: self
                        .config
                        .topic_fetch_max_bytes
                        .get(topic.as_str())
                        .copied()
                        .unwrap_or(self.config.max_partition_fetch_bytes),
                    replica_directory_id: None,
                    high_watermark: None,
                });
        }
        let mut fetch_topics: Vec<FetchTopicRequest> = topic_order
            .into_iter()
            .map(|topic| FetchTopicRequest {
                partitions: by_topic.remove(&topic).unwrap_or_default(),
                topic,
                topic_id: None,
            })
            .collect();

        // v7 fetch sessions (KIP-227), v9 leader-epoch fencing (KIP-320), v11
        // rack id (KIP-392), v13 topic ids (KIP-516). v17/v18 add only
        // follower fields, so negotiating them costs nothing.
        let mut fetch_version = conn
            .negotiate_api_version(ApiKey::Fetch, versions::FETCH_MAX, 7)
            .unwrap_or(4);
        if fetch_version >= 13 {
            let all_resolved = fetch_topics.iter_mut().all(|t| {
                t.topic_id = self.metadata.topic_id_for_name(&t.topic);
                t.topic_id.is_some()
            });
            if !all_resolved {
                fetch_version = 12;
            }
        }

        let (session_id, session_epoch, request_topics, forgotten_topics) = if fetch_version >= 7 {
            let mut state = self.state.lock();
            let session = state.fetch_sessions.get_or_create(broker_id);
            let session_req = session.build_request(&fetch_topics);
            trace!(
                broker_id,
                session_id = session_req.session_id,
                epoch = session_req.session_epoch,
                full = session_req.is_full_fetch,
                "Fetch request"
            );
            let mut topics = session_req.topics;
            let mut forgotten = session_req.forgotten_topics;
            if fetch_version >= 13 {
                for t in &mut topics {
                    if t.topic_id.is_none() {
                        t.topic_id = self.metadata.topic_id_for_name(&t.topic);
                    }
                }
                for t in &mut forgotten {
                    if t.topic_id.is_none() {
                        t.topic_id = self.metadata.topic_id_for_name(&t.topic);
                    }
                }
            }
            (
                session_req.session_id,
                session_req.session_epoch,
                topics,
                forgotten,
            )
        } else {
            (0, -1, fetch_topics.clone(), Vec::new())
        };

        let request = FetchRequest {
            replica_id: -1,
            max_wait_ms: crate::util::duration_to_millis_i32(max_wait),
            min_bytes: self.config.fetch_min_bytes,
            max_bytes: self.config.fetch_max_bytes,
            isolation_level: self.config.isolation_level.to_i8(),
            session_id,
            session_epoch,
            topics: request_topics,
            forgotten_topics,
            rack_id: self.config.client_rack.clone().unwrap_or_default(),
        };

        // On any send or decode failure the session is reset, so the next
        // fetch is a full one instead of hitting INVALID_FETCH_SESSION_EPOCH.
        let response = conn
            .send_request(ApiKey::Fetch, fetch_version, |buf| {
                request.encode_versioned(fetch_version, buf)
            })
            .await
            .inspect_err(|_| self.reset_fetch_session(broker_id, fetch_version))?;
        let mut buf = response;
        let mut response = FetchResponse::decode_versioned(fetch_version, &mut buf)
            .inspect_err(|_| self.reset_fetch_session(broker_id, fetch_version))?;

        // KIP-219.
        conn.notify_throttle(response.throttle_time_ms);

        // v13+ responses carry topic ids; one that cannot be mapped back to a
        // name is dropped rather than recorded under an empty topic.
        if fetch_version >= 13 {
            response.responses.retain_mut(|topic_response| {
                if !topic_response.topic.is_empty() {
                    return true;
                }
                match topic_response
                    .topic_id
                    .and_then(|id| self.metadata.topic_name_for_id(&id))
                {
                    Some(name) => {
                        topic_response.topic = name;
                        true
                    }
                    None => {
                        warn!("Fetch response names an unknown topic id; discarding it");
                        false
                    }
                }
            });
        }

        if fetch_version >= 7 {
            if matches!(
                response.error_code,
                ErrorCode::FetchSessionIdNotFound | ErrorCode::InvalidFetchSessionEpoch
            ) {
                warn!(
                    "Fetch session error for broker {}: {:?}, resetting session",
                    broker_id, response.error_code
                );
                self.reset_fetch_session(broker_id, fetch_version);
                return Ok(BrokerFetch {
                    partitions: Vec::new(),
                    node_endpoints: Vec::new(),
                });
            }
            self.state
                .lock()
                .fetch_sessions
                .get_or_create(broker_id)
                .update_from_response(response.session_id, &fetch_topics);
        }

        let by_key: HashMap<PartitionKey, &FetchTarget> =
            targets.iter().map(|t| (t.key.clone(), t)).collect();
        let mut partitions = Vec::new();
        for topic_response in response.responses {
            for p in topic_response.partitions {
                let key = (topic_response.topic.clone(), p.partition);
                let Some(target) = by_key.get(&key) else {
                    debug!(
                        topic = %key.0,
                        partition = key.1,
                        "Fetch response for a partition that was not requested; ignoring"
                    );
                    continue;
                };
                partitions.push(FetchedPartition {
                    target: (*target).clone(),
                    error_code: p.error_code,
                    high_watermark: p.high_watermark,
                    last_stable_offset: p.last_stable_offset,
                    log_start_offset: p.log_start_offset,
                    preferred_read_replica: (fetch_version >= 11)
                        .then_some(p.preferred_read_replica),
                    diverging_epoch: p.diverging_epoch,
                    current_leader: p.current_leader,
                    records: p.records,
                    aborted_transactions: p.aborted_transactions,
                });
            }
        }
        Ok(BrokerFetch {
            partitions,
            node_endpoints: std::mem::take(&mut response.node_endpoints),
        })
    }

    fn reset_fetch_session(&self, broker_id: BrokerId, fetch_version: i16) {
        if fetch_version >= 7 {
            self.state.lock().fetch_sessions.reset_broker(broker_id);
        }
    }

    /// Tell every broker this consumer is done with its fetch session, so
    /// the broker frees the slot instead of waiting for its LRU to evict it.
    /// Failures are ignored: the local state is cleared regardless.
    pub(super) async fn close_fetch_sessions(&self) {
        let closes = self.state.lock().fetch_sessions.close_all();
        if closes.is_empty() {
            return;
        }
        futures::future::join_all(
            closes
                .into_iter()
                .map(|close| self.send_fetch_session_close(close)),
        )
        .await;
    }

    async fn send_fetch_session_close(&self, close: FetchSessionClose) {
        let Some(broker) = self.metadata.broker(close.broker_id) else {
            return;
        };
        let Ok(conn) = self
            .pool
            .get_connection_by_id(close.broker_id, broker.address())
            .await
        else {
            return;
        };
        let Some(version) = conn.negotiate_api_version(ApiKey::Fetch, versions::FETCH_MAX, 7)
        else {
            return;
        };
        let request = fetch_session_close_request(&self.config, &close);
        if let Err(e) = conn
            .send_request(ApiKey::Fetch, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await
        {
            debug!(
                broker_id = close.broker_id,
                session_id = close.session_id,
                "Failed to close fetch session: {e}"
            );
        }
    }
}

/// The final-epoch `Fetch` that closes a session. It carries the configured
/// wait and size limits as the Java client does: Redpanda never answers a
/// close with `max_wait_ms = 0`, which would block every later response on the
/// connection.
fn fetch_session_close_request(
    config: &crate::consumer::ConsumerConfig,
    close: &FetchSessionClose,
) -> FetchRequest {
    FetchRequest {
        replica_id: -1,
        max_wait_ms: crate::util::duration_to_millis_i32(config.fetch_max_wait),
        min_bytes: config.fetch_min_bytes,
        max_bytes: config.fetch_max_bytes,
        isolation_level: config.isolation_level.to_i8(),
        session_id: close.session_id,
        session_epoch: close.session_epoch,
        topics: Vec::new(),
        forgotten_topics: Vec::new(),
        rack_id: config.client_rack.clone().unwrap_or_default(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_session_close_carries_the_configured_fetch_limits() {
        let config = crate::Kafka::detached()
            .consumer("g")
            .fetch_max_wait(Duration::from_millis(321))
            .fetch_min_bytes(7)
            .build_config()
            .unwrap();
        let close = FetchSessionClose {
            broker_id: 1,
            session_id: 42,
            session_epoch: -1,
        };
        let request = fetch_session_close_request(&config, &close);
        assert_eq!(request.max_wait_ms, 321);
        assert_eq!(request.min_bytes, 7);
        assert_eq!(request.session_id, 42);
        assert_eq!(request.session_epoch, -1);
        assert!(request.topics.is_empty());
    }

    #[test]
    fn a_decode_fault_names_the_partition_and_the_remedies() {
        let fault = FetchFault {
            key: ("t".to_string(), 3),
            offset: 17,
            error: KrafkaError::protocol_kind(ProtocolErrorKind::CrcMismatch, "bad crc"),
        };
        let error = fault.into_error(2);
        let text = error.to_string();
        assert!(text.contains("t-3 offset 17"), "{text}");
        assert!(text.contains("2 partitions affected"), "{text}");
        assert_eq!(
            error.protocol_error_kind(),
            Some(ProtocolErrorKind::CrcMismatch)
        );
    }
}
