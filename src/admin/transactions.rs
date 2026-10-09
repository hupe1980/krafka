//! Producers, transactions and the KRaft quorum.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use tracing::{info, warn};

use crate::BrokerId;
use crate::consumer::TopicPartition;
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    ApiKey, DescribeProducersRequest, DescribeProducersResponse, DescribeProducersTopicRequest,
    DescribeQuorumPartitionRequest, DescribeQuorumRequest, DescribeQuorumResponse,
    DescribeQuorumTopicRequest, DescribeTransactionsRequest, DescribeTransactionsResponse,
    ListTransactionsRequest, ListTransactionsResponse, QuorumReplicaState, WritableTxnMarker,
    WritableTxnMarkerTopic, WriteTxnMarkersRequest, WriteTxnMarkersResponse, versions,
};

use super::driver::{Mode, Target, answer, exchange, negotiate};
use super::{AdminClient, validate_topics};

/// The KRaft metadata log partition.
const CLUSTER_METADATA_TOPIC: &str = "__cluster_metadata";

/// An active producer on a partition.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProducerState {
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i32,
    /// Last sequence number written; `-1` if unknown.
    pub last_sequence: i32,
    /// Timestamp of the last write; `-1` if unknown.
    pub last_timestamp: i64,
    /// Coordinator epoch of the producer's transaction coordinator.
    pub coordinator_epoch: i32,
    /// Start offset of the open transaction, if any.
    pub current_transaction_start_offset: Option<i64>,
}

/// A transaction as its coordinator describes it.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct TransactionDescription {
    /// State (`Ongoing`, `PrepareCommit`, `CompleteAbort`, …).
    pub state: String,
    /// Transaction timeout.
    pub timeout: Duration,
    /// Start time in milliseconds since the epoch; `-1` without an open
    /// transaction.
    pub start_time_ms: i64,
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Partitions in the transaction.
    pub partitions: Vec<TopicPartition>,
}

/// A transaction from [`AdminClient::list_transactions`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionListing {
    /// Transactional ID.
    pub transactional_id: String,
    /// Producer ID.
    pub producer_id: i64,
    /// State.
    pub state: String,
}

/// The KRaft quorum of the cluster metadata log.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct QuorumInfo {
    /// Leader node ID; `-1` when there is none.
    pub leader_id: BrokerId,
    /// Leader epoch.
    pub leader_epoch: i32,
    /// High watermark.
    pub high_watermark: i64,
    /// Voters.
    pub voters: Vec<QuorumReplica>,
    /// Observers.
    pub observers: Vec<QuorumReplica>,
    /// Quorum nodes and their endpoints (KIP-853, v2+); empty on older
    /// brokers.
    pub nodes: Vec<QuorumNode>,
}

/// A voter or observer of the quorum.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumReplica {
    /// Replica node ID.
    pub replica_id: BrokerId,
    /// Directory ID of the replica's log directory (KIP-853, v2+).
    pub directory_id: Option<[u8; 16]>,
    /// Log end offset; `-1` if unknown.
    pub log_end_offset: i64,
    /// Leader time of the replica's last fetch, in epoch milliseconds
    /// (KIP-836, v1+); `None` when unknown.
    pub last_fetch_timestamp: Option<i64>,
    /// Leader time of the offset the replica last caught up to (KIP-836,
    /// v1+); `None` when unknown.
    pub last_caught_up_timestamp: Option<i64>,
}

/// A quorum node and its listeners (KIP-853).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumNode {
    /// Node ID.
    pub node_id: BrokerId,
    /// Listener endpoints.
    pub listeners: Vec<QuorumListener>,
}

/// A listener endpoint of a [`QuorumNode`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuorumListener {
    /// Listener name, e.g. `CONTROLLER`.
    pub name: String,
    /// Host.
    pub host: String,
    /// Port.
    pub port: u16,
}

admin_options! {
    /// Options for [`AdminClient::describe_producers`].
    DescribeProducersOptions {}
}

admin_options! {
    /// Options for [`AdminClient::describe_transactions`].
    DescribeTransactionsOptions {}
}

admin_options! {
    /// Options for [`AdminClient::list_transactions`]. Empty filters match
    /// everything.
    ListTransactionsOptions {
        /// Only transactions in these states.
        states: Vec<String>,
        /// Only transactions of these producer IDs.
        producer_ids: Vec<i64>,
    }
    optional {
        /// Only transactions open at least this long.
        min_duration: Duration,
        /// Only transactional IDs matching this regular expression
        /// (KIP-1152, `ListTransactions` v2, Kafka 4.1+).
        transactional_id_pattern: String,
    }
}

admin_options! {
    /// Options for [`AdminClient::abort_transaction`].
    AbortTransactionOptions {}
    optional {
        /// The coordinator epoch to write the markers with. Default: read
        /// from the producer state on the transaction's partitions.
        coordinator_epoch: i32,
    }
}

admin_options! {
    /// Options for [`AdminClient::describe_metadata_quorum`].
    DescribeMetadataQuorumOptions {}
}

fn quorum_replica(state: QuorumReplicaState) -> QuorumReplica {
    QuorumReplica {
        replica_id: state.replica_id,
        directory_id: state.replica_directory_id,
        log_end_offset: state.log_end_offset,
        last_fetch_timestamp: (state.last_fetch_timestamp >= 0)
            .then_some(state.last_fetch_timestamp),
        last_caught_up_timestamp: (state.last_caught_up_timestamp >= 0)
            .then_some(state.last_caught_up_timestamp),
    }
}

impl AdminClient {
    /// Describe the active producers of partitions, at each partition's
    /// leader. Returns a result per partition.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client or an invalid topic name.
    pub async fn describe_producers(
        &self,
        partitions: impl IntoIterator<Item = TopicPartition>,
        options: DescribeProducersOptions,
    ) -> Result<HashMap<TopicPartition, Result<Vec<ProducerState>>>> {
        let mut partitions: Vec<TopicPartition> = partitions.into_iter().collect();
        validate_topics(partitions.iter().map(|tp| tp.topic.as_str()))?;
        partitions.dedup();
        let call = self.call("DescribeProducers", Mode::Read, options.timeout)?;
        Ok(call
            .fan_out(
                partitions,
                |tp| Target::Leader(tp.clone()),
                |conn, partitions| async move {
                    let mut topics: BTreeMap<String, Vec<i32>> = BTreeMap::new();
                    for tp in &partitions {
                        topics
                            .entry(tp.topic.clone())
                            .or_default()
                            .push(tp.partition);
                    }
                    let request = DescribeProducersRequest {
                        topics: topics
                            .into_iter()
                            .map(|(name, partition_indexes)| DescribeProducersTopicRequest {
                                name,
                                partition_indexes,
                            })
                            .collect(),
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::DescribeProducers,
                        versions::DESCRIBE_PRODUCERS_MIN,
                        versions::DESCRIBE_PRODUCERS_MAX,
                    )?;
                    let response: DescribeProducersResponse =
                        exchange(&conn, ApiKey::DescribeProducers, version, &request).await?;
                    Ok(response
                        .topics
                        .into_iter()
                        .flat_map(|t| {
                            let name = t.name;
                            t.partitions.into_iter().map(move |p| {
                                let producers = answer(p.error_code, p.error_message).map(|()| {
                                    p.active_producers
                                        .into_iter()
                                        .map(|s| ProducerState {
                                            producer_id: s.producer_id,
                                            producer_epoch: s.producer_epoch,
                                            last_sequence: s.last_sequence,
                                            last_timestamp: s.last_timestamp,
                                            coordinator_epoch: s.coordinator_epoch,
                                            current_transaction_start_offset: (s
                                                .current_txn_start_offset
                                                >= 0)
                                                .then_some(s.current_txn_start_offset),
                                        })
                                        .collect()
                                });
                                (
                                    TopicPartition::new(name.clone(), p.partition_index),
                                    producers,
                                )
                            })
                        })
                        .collect())
                },
            )
            .await)
    }

    /// Describe transactions at their coordinators. Returns a result per
    /// transactional ID.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client.
    pub async fn describe_transactions<I, S>(
        &self,
        transactional_ids: I,
        options: DescribeTransactionsOptions,
    ) -> Result<HashMap<String, Result<TransactionDescription>>>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut ids: Vec<String> = transactional_ids
            .into_iter()
            .map(|s| s.as_ref().to_string())
            .collect();
        ids.sort_unstable();
        ids.dedup();
        let call = self.call("DescribeTransactions", Mode::Read, options.timeout)?;
        Ok(call
            .fan_out(
                ids,
                |id| Target::TransactionCoordinator(id.clone()),
                |conn, ids| async move {
                    let request = DescribeTransactionsRequest {
                        transactional_ids: ids,
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::DescribeTransactions,
                        versions::DESCRIBE_TRANSACTIONS_MIN,
                        versions::DESCRIBE_TRANSACTIONS_MAX,
                    )?;
                    let response: DescribeTransactionsResponse =
                        exchange(&conn, ApiKey::DescribeTransactions, version, &request).await?;
                    Ok(response
                        .transaction_states
                        .into_iter()
                        .map(|s| {
                            let description =
                                answer(s.error_code, None).map(|()| TransactionDescription {
                                    state: s.transaction_state,
                                    timeout: Duration::from_millis(
                                        u64::try_from(s.transaction_timeout_ms).unwrap_or(0),
                                    ),
                                    start_time_ms: s.transaction_start_time_ms,
                                    producer_id: s.producer_id,
                                    producer_epoch: s.producer_epoch,
                                    partitions: s
                                        .topics
                                        .into_iter()
                                        .flat_map(|t| {
                                            let topic = t.topic;
                                            t.partitions
                                                .into_iter()
                                                .map(move |p| TopicPartition::new(topic.clone(), p))
                                        })
                                        .collect(),
                                });
                            (s.transactional_id, description)
                        })
                        .collect())
                },
            )
            .await)
    }

    /// List transactions on every broker. Each broker knows the transactions
    /// it coordinates, so the result is per broker.
    ///
    /// # Errors
    ///
    /// The call fails for a closed client, or when the broker list cannot be
    /// fetched. A broker too old for `transactional_id_pattern` is an `Err`
    /// in its entry rather than an unfiltered listing.
    pub async fn list_transactions(
        &self,
        options: ListTransactionsOptions,
    ) -> Result<HashMap<BrokerId, Result<Vec<TransactionListing>>>> {
        let call = self.call("ListTransactions", Mode::Read, options.timeout)?;
        let brokers = self.broker_ids(&call).await?;
        let request = ListTransactionsRequest {
            state_filters: options.states.clone(),
            producer_id_filters: options.producer_ids.clone(),
            duration_filter: options
                .min_duration
                .map_or(-1, crate::util::duration_to_millis_i64),
            transactional_id_pattern: options.transactional_id_pattern.clone(),
        };
        let request = &request;
        Ok(call
            .fan_out(
                brokers,
                |id| Target::Broker(*id),
                |conn, ids| async move {
                    let version = negotiate(
                        &conn,
                        ApiKey::ListTransactions,
                        versions::LIST_TRANSACTIONS_MIN,
                        versions::LIST_TRANSACTIONS_MAX,
                    )?;
                    let listing = if request.transactional_id_pattern.is_some() && version < 2 {
                        Err(KrafkaError::protocol_kind(
                            ProtocolErrorKind::UnknownApiVersion,
                            format!(
                                "transactional_id_pattern needs ListTransactions v2 (KIP-1152); \
                             the broker negotiated v{version}"
                            ),
                        ))
                    } else {
                        let response: ListTransactionsResponse =
                            exchange(&conn, ApiKey::ListTransactions, version, request).await?;
                        if !response.unknown_state_filters.is_empty() {
                            warn!(
                                unknown = ?response.unknown_state_filters,
                                "ListTransactions: the broker does not know these state filters"
                            );
                        }
                        answer(response.error_code, None).map(|()| {
                            response
                                .transaction_states
                                .into_iter()
                                .map(|s| TransactionListing {
                                    transactional_id: s.transactional_id,
                                    producer_id: s.producer_id,
                                    state: s.transaction_state,
                                })
                                .collect()
                        })
                    };
                    Ok(ids.into_iter().map(|id| (id, listing.clone())).collect())
                },
            )
            .await)
    }

    /// Abort a hanging transaction by writing ABORT markers to each of its
    /// partitions' leaders.
    ///
    /// The partitions and producer come from the transaction's coordinator.
    /// The coordinator epoch comes from
    /// [`AbortTransactionOptions::coordinator_epoch`], or else from the
    /// producer state on those partitions; a partition leader with no cached
    /// epoch accepts any epoch, so one is never invented. Returns a result per
    /// partition; a transaction with no partitions returns none.
    ///
    /// # Errors
    ///
    /// The transaction's describe error; a `Config` error when no epoch was
    /// given and the producer state does not reveal one; a retriable
    /// `CONCURRENT_TRANSACTIONS` when the partitions disagree on the epoch,
    /// which means the transaction is mid-transition.
    pub async fn abort_transaction(
        &self,
        transactional_id: impl Into<String>,
        options: AbortTransactionOptions,
    ) -> Result<HashMap<TopicPartition, Result<()>>> {
        let transactional_id = transactional_id.into();
        let call = self.call("AbortTransaction", Mode::Write, options.timeout)?;

        let description = self
            .describe_transactions(
                [transactional_id.clone()],
                DescribeTransactionsOptions::default().timeout(call.remaining()),
            )
            .await?
            .remove(&transactional_id)
            .unwrap_or_else(|| Err(KrafkaError::timeout("DescribeTransactions")))?;
        if description.partitions.is_empty() {
            return Ok(HashMap::new());
        }

        let coordinator_epoch = match options.coordinator_epoch {
            Some(epoch) => epoch,
            None => {
                let producers = self
                    .describe_producers(
                        description.partitions.clone(),
                        DescribeProducersOptions::default().timeout(call.remaining()),
                    )
                    .await?;
                coordinator_epoch_of(&transactional_id, description.producer_id, producers)?
            }
        };

        info!(
            transactional_id,
            producer_id = description.producer_id,
            coordinator_epoch,
            "aborting transaction by writing ABORT markers"
        );
        let description = &description;
        Ok(call
            .fan_out(
                description.partitions.clone(),
                |tp| Target::Leader(tp.clone()),
                |conn, partitions| async move {
                    let mut topics: BTreeMap<String, Vec<i32>> = BTreeMap::new();
                    for tp in &partitions {
                        topics
                            .entry(tp.topic.clone())
                            .or_default()
                            .push(tp.partition);
                    }
                    let request = WriteTxnMarkersRequest {
                        markers: vec![WritableTxnMarker {
                            producer_id: description.producer_id,
                            producer_epoch: description.producer_epoch,
                            transaction_result: false,
                            topics: topics
                                .into_iter()
                                .map(|(name, partition_indexes)| WritableTxnMarkerTopic {
                                    name,
                                    partition_indexes,
                                })
                                .collect(),
                            coordinator_epoch,
                            // TV2 markers are written by the coordinator; an
                            // administrative abort uses the legacy encoding.
                            transaction_version: WritableTxnMarker::legacy_transaction_version(),
                        }],
                    };
                    let version = negotiate(
                        &conn,
                        ApiKey::WriteTxnMarkers,
                        versions::WRITE_TXN_MARKERS_MIN,
                        versions::WRITE_TXN_MARKERS_MAX,
                    )?;
                    let response: WriteTxnMarkersResponse =
                        exchange(&conn, ApiKey::WriteTxnMarkers, version, &request).await?;
                    Ok(response
                        .markers
                        .into_iter()
                        .flat_map(|m| m.topics)
                        .flat_map(|t| {
                            let name = t.name;
                            t.partitions.into_iter().map(move |p| {
                                (
                                    TopicPartition::new(name.clone(), p.partition_index),
                                    answer(p.error_code, None),
                                )
                            })
                        })
                        .collect())
                },
            )
            .await)
    }

    /// Describe the KRaft quorum of the cluster metadata log (any broker).
    ///
    /// # Errors
    ///
    /// The broker's error, a closed client, or the deadline.
    pub async fn describe_metadata_quorum(
        &self,
        options: DescribeMetadataQuorumOptions,
    ) -> Result<QuorumInfo> {
        let call = self.call("DescribeQuorum", Mode::Read, options.timeout)?;
        call.single(Target::AnyBroker, |conn| async move {
            let request = DescribeQuorumRequest {
                topics: vec![DescribeQuorumTopicRequest {
                    topic_name: CLUSTER_METADATA_TOPIC.to_string(),
                    partitions: vec![DescribeQuorumPartitionRequest { partition_index: 0 }],
                }],
            };
            let version = negotiate(
                &conn,
                ApiKey::DescribeQuorum,
                versions::DESCRIBE_QUORUM_MIN,
                versions::DESCRIBE_QUORUM_MAX,
            )?;
            let response: DescribeQuorumResponse =
                exchange(&conn, ApiKey::DescribeQuorum, version, &request).await?;
            answer(response.error_code, response.error_message)?;
            let partition = response
                .topics
                .into_iter()
                .flat_map(|t| t.partitions)
                .next()
                .ok_or_else(|| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::Malformed,
                        "DescribeQuorum returned no partition",
                    )
                })?;
            answer(partition.error_code, partition.error_message)?;
            Ok(QuorumInfo {
                leader_id: partition.leader_id,
                leader_epoch: partition.leader_epoch,
                high_watermark: partition.high_watermark,
                voters: partition
                    .current_voters
                    .into_iter()
                    .map(quorum_replica)
                    .collect(),
                observers: partition
                    .observers
                    .into_iter()
                    .map(quorum_replica)
                    .collect(),
                nodes: response
                    .nodes
                    .into_iter()
                    .map(|n| QuorumNode {
                        node_id: n.node_id,
                        listeners: n
                            .listeners
                            .into_iter()
                            .map(|l| QuorumListener {
                                name: l.name,
                                host: l.host,
                                port: l.port,
                            })
                            .collect(),
                    })
                    .collect(),
            })
        })
        .await
    }
}

/// The one coordinator epoch the partitions' producer state reports for
/// `producer_id`.
fn coordinator_epoch_of(
    transactional_id: &str,
    producer_id: i64,
    producers: HashMap<TopicPartition, Result<Vec<ProducerState>>>,
) -> Result<i32> {
    let mut found: Option<i32> = None;
    for (tp, states) in producers {
        for state in states? {
            if state.producer_id != producer_id {
                continue;
            }
            match found {
                None => found = Some(state.coordinator_epoch),
                Some(epoch) if epoch == state.coordinator_epoch => {}
                Some(epoch) => {
                    return Err(KrafkaError::broker(
                        ErrorCode::ConcurrentTransactions,
                        format!(
                            "transaction '{transactional_id}' reports coordinator epochs {epoch} \
                             and {} (on {}-{}); it is mid-transition",
                            state.coordinator_epoch, tp.topic, tp.partition
                        ),
                    ));
                }
            }
        }
    }
    found.ok_or_else(|| {
        KrafkaError::config(format!(
            "no producer state for producer {producer_id} on the partitions of transaction \
             '{transactional_id}'; pass AbortTransactionOptions::coordinator_epoch"
        ))
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn state(producer_id: i64, coordinator_epoch: i32) -> ProducerState {
        ProducerState {
            producer_id,
            producer_epoch: 0,
            last_sequence: 0,
            last_timestamp: 0,
            coordinator_epoch,
            current_transaction_start_offset: Some(0),
        }
    }

    #[test]
    fn the_coordinator_epoch_is_read_from_the_producer_state() {
        let producers = HashMap::from([
            (
                TopicPartition::new("t", 0),
                Ok(vec![state(7, 3), state(8, 9)]),
            ),
            (TopicPartition::new("t", 1), Ok(vec![state(7, 3)])),
        ]);
        assert_eq!(coordinator_epoch_of("tx", 7, producers).unwrap(), 3);
    }

    #[test]
    fn disagreeing_epochs_are_a_retriable_conflict() {
        let producers = HashMap::from([
            (TopicPartition::new("t", 0), Ok(vec![state(7, 3)])),
            (TopicPartition::new("t", 1), Ok(vec![state(7, 4)])),
        ]);
        let err = coordinator_epoch_of("tx", 7, producers).unwrap_err();
        assert!(matches!(
            err,
            KrafkaError::Broker {
                code: ErrorCode::ConcurrentTransactions,
                ..
            }
        ));
        assert!(err.is_retriable());
    }

    #[test]
    fn no_producer_state_never_invents_an_epoch() {
        let producers = HashMap::from([(TopicPartition::new("t", 0), Ok(vec![state(8, 1)]))]);
        let err = coordinator_epoch_of("tx", 7, producers).unwrap_err();
        assert!(matches!(err, KrafkaError::Config { .. }));
    }
}
