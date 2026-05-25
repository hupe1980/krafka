//! AdminClient operation group: transactions.

use std::collections::HashMap;

use tracing::{info, warn};

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    ApiKey, DescribeProducersRequest, DescribeProducersResponse, DescribeProducersTopicRequest,
    DescribeQuorumPartitionRequest, DescribeQuorumRequest, DescribeQuorumResponse,
    DescribeQuorumTopicRequest, DescribeTransactionsRequest, DescribeTransactionsResponse,
    FindCoordinatorRequest, FindCoordinatorResponse, ListClientMetricsResourcesRequest,
    ListClientMetricsResourcesResponse, ListTransactionsRequest, ListTransactionsResponse,
    VersionedDecode, VersionedEncode, WritableTxnMarker, WritableTxnMarkerTopic,
    WriteTxnMarkersRequest, WriteTxnMarkersResponse, versions,
};

#[allow(clippy::wildcard_imports)]
use super::*;

impl AdminClient {
    /// Describe active producers on the given topic-partitions.
    ///
    /// Routes each topic-partition to its leader broker via cached metadata
    /// for optimal performance. Falls back to any broker if the leader is
    /// unknown.
    ///
    /// Returns per-partition producer state useful for debugging
    /// transactional and idempotent producers.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = admin
    ///     .describe_producers(&[("my-topic", &[0, 1])])
    ///     .await?;
    /// ```
    pub async fn describe_producers(
        &self,
        topic_partitions: &[(&str, &[i32])],
    ) -> Result<Vec<DescribeProducersTopicResult>> {
        self.check_not_closed()?;

        for attempt in 0u8..2 {
            if attempt == 1 {
                // Refresh metadata after a stale-leader error.
                let topics: Vec<&str> = topic_partitions.iter().map(|&(t, _)| t).collect();
                let _ = self.metadata.refresh_for_topics(Some(&topics)).await;
            }

            let brokers = self.metadata.brokers();
            if brokers.is_empty() {
                return Err(KrafkaError::broker(
                    crate::error::ErrorCode::UnknownServerError,
                    "no brokers available",
                ));
            }

            let fallback_id = brokers[0].id();

            // Group topic-partitions by leader broker.
            let mut by_leader: HashMap<i32, HashMap<String, Vec<i32>>> = HashMap::new();
            for &(topic, partitions) in topic_partitions {
                for &pid in partitions {
                    let leader = self.metadata.leader(topic, pid).unwrap_or(fallback_id);
                    by_leader
                        .entry(leader)
                        .or_default()
                        .entry(topic.to_string())
                        .or_default()
                        .push(pid);
                }
            }

            let mut all_results: HashMap<String, DescribeProducersTopicResult> = HashMap::new();
            let mut has_stale_leader = false;

            for (broker_id, topic_map) in by_leader {
                let broker = brokers
                    .iter()
                    .find(|b| b.id() == broker_id)
                    .unwrap_or(&brokers[0]);
                let conn = self
                    .pool
                    .get_connection_by_id(broker.id(), broker.address())
                    .await?;

                let topics = topic_map
                    .into_iter()
                    .map(|(name, partition_indexes)| DescribeProducersTopicRequest {
                        name,
                        partition_indexes,
                    })
                    .collect();

                let request = DescribeProducersRequest { topics };

                let version = conn
                    .negotiate_api_version(
                        ApiKey::DescribeProducers,
                        versions::DESCRIBE_PRODUCERS_MAX,
                        versions::DESCRIBE_PRODUCERS_MIN,
                    )
                    .await
                    .ok_or_else(|| {
                        KrafkaError::protocol_kind(
                            ProtocolErrorKind::UnknownApiVersion,
                            "no mutually supported DescribeProducers API version",
                        )
                    })?;

                let response_bytes = match conn
                    .send_request(ApiKey::DescribeProducers, version, |buf| {
                        request.encode_versioned(version, buf)
                    })
                    .await
                {
                    Ok(bytes) => bytes,
                    Err(e) => {
                        warn!(
                            "DescribeProducers request failed on broker {}: {}",
                            broker.id(),
                            e
                        );
                        continue;
                    }
                };

                let mut buf = response_bytes;
                let response = match DescribeProducersResponse::decode_versioned(version, &mut buf)
                {
                    Ok(r) => r,
                    Err(e) => {
                        warn!(
                            "DescribeProducers decode failed on broker {}: {}",
                            broker.id(),
                            e
                        );
                        continue;
                    }
                };

                for topic in response.topics {
                    let entry = all_results.entry(topic.name.clone()).or_insert_with(|| {
                        DescribeProducersTopicResult {
                            name: topic.name,
                            partitions: Vec::new(),
                        }
                    });
                    entry
                        .partitions
                        .extend(topic.partitions.into_iter().map(|p| {
                            if p.error_code == crate::error::ErrorCode::NotLeaderForPartition {
                                has_stale_leader = true;
                            }
                            DescribeProducersPartitionInfo {
                                partition_index: p.partition_index,
                                error: if p.error_code.is_ok() {
                                    None
                                } else {
                                    Some(
                                        p.error_message
                                            .unwrap_or_else(|| format!("{:?}", p.error_code)),
                                    )
                                },
                                active_producers: p
                                    .active_producers
                                    .into_iter()
                                    .map(|pr| ProducerStateInfo {
                                        producer_id: pr.producer_id,
                                        producer_epoch: pr.producer_epoch,
                                        last_sequence: pr.last_sequence,
                                        last_timestamp: pr.last_timestamp,
                                        coordinator_epoch: pr.coordinator_epoch,
                                        current_txn_start_offset: pr.current_txn_start_offset,
                                    })
                                    .collect(),
                            }
                        }));
                }
            }

            if has_stale_leader && attempt == 0 {
                warn!(
                    "NotLeaderForPartition in DescribeProducers response, retrying with refreshed metadata"
                );
                continue;
            }

            let results: Vec<DescribeProducersTopicResult> = all_results.into_values().collect();
            info!("DescribeProducers returned {} topic(s)", results.len());
            return Ok(results);
        }
        Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::Malformed,
            "DescribeProducers retry loop exhausted after metadata refresh",
        ))
    }

    // ════════════════════════════════════════════════════════════════════
    // DescribeTransactions (API key 65)
    // ════════════════════════════════════════════════════════════════════

    /// Describe the state of the given transactions.
    ///
    /// Routes each transactional ID to its transaction coordinator via
    /// `FindCoordinator`, groups by coordinator, and batches requests.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let results = admin
    ///     .describe_transactions(&["txn-1", "txn-2"])
    ///     .await?;
    /// ```
    pub async fn describe_transactions(
        &self,
        transactional_ids: &[&str],
    ) -> Result<Vec<TransactionDescription>> {
        self.check_not_closed()?;
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::broker(
                crate::error::ErrorCode::UnknownServerError,
                "no brokers available",
            ));
        }

        // Group transactional IDs by their coordinator broker.
        let any_broker = &brokers[0];
        let any_conn = self
            .pool
            .get_connection_by_id(any_broker.id(), any_broker.address())
            .await?;

        let mut coordinator_txns: HashMap<i32, Vec<String>> = HashMap::new();

        for txn_id in transactional_ids {
            let coord_request = FindCoordinatorRequest::for_transaction(txn_id);
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
                coordinator_txns
                    .entry(coord_response.node_id)
                    .or_default()
                    .push((*txn_id).to_string());
            } else {
                warn!(
                    "FindCoordinator failed for txn '{}': {:?}, falling back to broker {}",
                    txn_id,
                    coord_response.error_code,
                    any_broker.id()
                );
                coordinator_txns
                    .entry(any_broker.id())
                    .or_default()
                    .push((*txn_id).to_string());
            }
        }

        let mut all_results = Vec::new();

        for (broker_id, txn_ids) in coordinator_txns {
            let broker = brokers
                .iter()
                .find(|b| b.id() == broker_id)
                .unwrap_or(any_broker);
            let conn = self
                .pool
                .get_connection_by_id(broker.id(), broker.address())
                .await?;

            let request = DescribeTransactionsRequest {
                transactional_ids: txn_ids,
            };

            let version = conn
                .negotiate_api_version(
                    ApiKey::DescribeTransactions,
                    versions::DESCRIBE_TRANSACTIONS_MAX,
                    versions::DESCRIBE_TRANSACTIONS_MIN,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::UnknownApiVersion,
                        "no mutually supported DescribeTransactions API version",
                    )
                })?;

            let response_bytes = match conn
                .send_request(ApiKey::DescribeTransactions, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await
            {
                Ok(bytes) => bytes,
                Err(e) => {
                    warn!(
                        "DescribeTransactions request failed on broker {}: {}",
                        broker.id(),
                        e
                    );
                    continue;
                }
            };

            let mut buf = response_bytes;
            let response = match DescribeTransactionsResponse::decode_versioned(version, &mut buf) {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "DescribeTransactions decode failed on broker {}: {}",
                        broker.id(),
                        e
                    );
                    continue;
                }
            };

            all_results.extend(response.transaction_states.into_iter().map(|s| {
                TransactionDescription {
                    transactional_id: s.transactional_id,
                    error: if s.error_code.is_ok() {
                        None
                    } else {
                        Some(format!("{:?}", s.error_code))
                    },
                    state: s.transaction_state,
                    timeout_ms: s.transaction_timeout_ms,
                    start_time_ms: s.transaction_start_time_ms,
                    producer_id: s.producer_id,
                    producer_epoch: s.producer_epoch,
                    topics: s
                        .topics
                        .into_iter()
                        .map(|t| TransactionTopicInfo {
                            topic: t.topic,
                            partitions: t.partitions,
                        })
                        .collect(),
                }
            }));
        }

        info!(
            "DescribeTransactions returned {} transaction(s)",
            all_results.len()
        );
        Ok(all_results)
    }

    // ════════════════════════════════════════════════════════════════════
    // ListTransactions (API key 66)
    // ════════════════════════════════════════════════════════════════════

    /// List transactions matching the given filters.
    ///
    /// Queries **all** brokers and merges results, because each broker
    /// only knows about transactions it coordinates.
    ///
    /// Pass empty slices for `state_filters` and `producer_id_filters` to
    /// list all transactions.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // List all ongoing transactions
    /// let txns = admin
    ///     .list_transactions(&["Ongoing"], &[], -1)
    ///     .await?;
    /// ```
    pub async fn list_transactions(
        &self,
        state_filters: &[&str],
        producer_id_filters: &[i64],
        duration_filter: i64,
    ) -> Result<ListTransactionsResult> {
        self.check_not_closed()?;
        let brokers = self.metadata.brokers();
        if brokers.is_empty() {
            return Err(KrafkaError::broker(
                crate::error::ErrorCode::UnknownServerError,
                "no brokers available",
            ));
        }

        let request = ListTransactionsRequest {
            state_filters: state_filters.iter().map(|s| (*s).to_string()).collect(),
            producer_id_filters: producer_id_filters.to_vec(),
            duration_filter,
        };

        let mut all_transactions = Vec::new();
        let mut all_unknown_state_filters = Vec::new();
        let mut last_error: Option<String> = None;

        for broker in &brokers {
            let conn = self
                .pool
                .get_connection_by_id(broker.id(), broker.address())
                .await?;

            let version = conn
                .negotiate_api_version(
                    ApiKey::ListTransactions,
                    versions::LIST_TRANSACTIONS_MAX,
                    versions::LIST_TRANSACTIONS_MIN,
                )
                .await
                .ok_or_else(|| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::UnknownApiVersion,
                        "no mutually supported ListTransactions API version",
                    )
                })?;

            let response_bytes = match conn
                .send_request(ApiKey::ListTransactions, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await
            {
                Ok(bytes) => bytes,
                Err(e) => {
                    warn!(
                        "ListTransactions request failed on broker {}: {}",
                        broker.id(),
                        e
                    );
                    continue;
                }
            };

            let mut buf = response_bytes;
            let response = match ListTransactionsResponse::decode_versioned(version, &mut buf) {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "ListTransactions decode failed on broker {}: {}",
                        broker.id(),
                        e
                    );
                    continue;
                }
            };

            if !response.error_code.is_ok() {
                warn!(
                    "ListTransactions error on broker {}: {:?}",
                    broker.id(),
                    response.error_code
                );
                last_error = Some(format!("{:?}", response.error_code));
            }

            for filter in response.unknown_state_filters {
                if !all_unknown_state_filters.contains(&filter) {
                    all_unknown_state_filters.push(filter);
                }
            }

            all_transactions.extend(response.transaction_states.into_iter().map(|s| {
                TransactionListEntry {
                    transactional_id: s.transactional_id,
                    producer_id: s.producer_id,
                    state: s.transaction_state,
                }
            }));
        }

        info!(
            "ListTransactions returned {} transaction(s) across {} broker(s)",
            all_transactions.len(),
            brokers.len()
        );

        Ok(ListTransactionsResult {
            error: last_error,
            unknown_state_filters: all_unknown_state_filters,
            transactions: all_transactions,
        })
    }

    // ════════════════════════════════════════════════════════════════════
    // ListClientMetricsResources (API key 74)
    // ════════════════════════════════════════════════════════════════════

    /// List client metrics subscription resources (KIP-714).
    ///
    /// Returns the names of client metrics subscriptions configured on
    /// the cluster.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let names = admin.list_client_metrics_resources().await?;
    /// for name in &names {
    ///     println!("subscription: {name}");
    /// }
    /// ```
    pub async fn list_client_metrics_resources(&self) -> Result<Vec<String>> {
        let conn = self.get_any_broker_connection().await?;

        let request = ListClientMetricsResourcesRequest;

        let version = conn
            .negotiate_api_version(
                ApiKey::ListClientMetricsResources,
                versions::LIST_CLIENT_METRICS_RESOURCES_MAX,
                versions::LIST_CLIENT_METRICS_RESOURCES_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported ListClientMetricsResources API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::ListClientMetricsResources, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = ListClientMetricsResourcesResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            warn!(
                "ListClientMetricsResources error: {:?}",
                response.error_code
            );
        }

        let names: Vec<String> = response
            .client_metrics_resources
            .into_iter()
            .map(|r| r.name)
            .collect();

        info!(
            "ListClientMetricsResources returned {} resource(s)",
            names.len()
        );
        Ok(names)
    }

    // ════════════════════════════════════════════════════════════════════
    // WriteTxnMarkers (API key 27)
    // ════════════════════════════════════════════════════════════════════

    /// Write transaction markers (COMMIT or ABORT) to the given topic-partitions.
    ///
    /// This is an inter-broker API used to finalize transactions.
    /// The admin client exposes it primarily for **aborting stuck transactions**
    /// (`abort_transaction`).
    ///
    /// Each marker is sent to **all** brokers since the partitions may be led
    /// by different brokers. Per-broker errors are logged and skipped so
    /// results from reachable brokers are still returned.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use krafka::protocol::{WritableTxnMarker, WritableTxnMarkerTopic};
    ///
    /// let results = admin
    ///     .write_txn_markers(&[WritableTxnMarker {
    ///         producer_id: 42,
    ///         producer_epoch: 5,
    ///         transaction_result: false, // ABORT
    ///         topics: vec![WritableTxnMarkerTopic {
    ///             name: "my-topic".into(),
    ///             partition_indexes: vec![0, 1],
    ///         }],
    ///         coordinator_epoch: 10,
    ///     }])
    ///     .await?;
    /// ```
    pub async fn write_txn_markers(
        &self,
        markers: &[WritableTxnMarker],
    ) -> Result<Vec<WriteTxnMarkersResult>> {
        self.check_not_closed()?;
        let conn = self.get_any_broker_connection().await?;

        let request = WriteTxnMarkersRequest {
            markers: markers.to_vec(),
        };

        let version = conn
            .negotiate_api_version(
                ApiKey::WriteTxnMarkers,
                versions::WRITE_TXN_MARKERS_MAX,
                versions::WRITE_TXN_MARKERS_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported WriteTxnMarkers API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::WriteTxnMarkers, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = WriteTxnMarkersResponse::decode_versioned(version, &mut buf)?;

        let results = response
            .markers
            .into_iter()
            .map(|m| WriteTxnMarkersResult {
                producer_id: m.producer_id,
                topics: m
                    .topics
                    .into_iter()
                    .map(|t| WriteTxnMarkersTopicResult {
                        name: t.name,
                        partitions: t
                            .partitions
                            .into_iter()
                            .map(|p| WriteTxnMarkersPartitionResult {
                                partition_index: p.partition_index,
                                error: if p.error_code.is_ok() {
                                    None
                                } else {
                                    Some(format!("{:?}", p.error_code))
                                },
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        info!(
            "WriteTxnMarkers returned {} marker result(s)",
            results.len()
        );
        Ok(results)
    }

    /// Abort a stuck transaction by writing an ABORT marker.
    ///
    /// This is the admin-friendly wrapper around [`write_txn_markers`](Self::write_txn_markers)
    /// that looks up the transaction coordinator, discovers the affected
    /// partitions via [`describe_transactions`](Self::describe_transactions),
    /// and writes an ABORT marker.
    ///
    /// # Example
    ///
    /// ```ignore
    /// admin.abort_transaction("my-transactional-id").await?;
    /// ```
    pub async fn abort_transaction(
        &self,
        transactional_id: &str,
    ) -> Result<Vec<WriteTxnMarkersResult>> {
        self.check_not_closed()?;

        // Describe the transaction to get producer_id, producer_epoch,
        // coordinator_epoch, and the affected topic-partitions.
        let descriptions = self.describe_transactions(&[transactional_id]).await?;
        let desc = descriptions.first().ok_or_else(|| {
            KrafkaError::protocol_kind(
                ProtocolErrorKind::Malformed,
                "no transaction description returned",
            )
        })?;

        if let Some(ref err) = desc.error {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::Malformed,
                format!("cannot abort transaction '{}': {}", transactional_id, err,),
            ));
        }

        let topics: Vec<WritableTxnMarkerTopic> = desc
            .topics
            .iter()
            .map(|t| WritableTxnMarkerTopic {
                name: t.topic.clone(),
                partition_indexes: t.partitions.clone(),
            })
            .collect();

        let marker = WritableTxnMarker {
            producer_id: desc.producer_id,
            producer_epoch: desc.producer_epoch,
            transaction_result: false, // ABORT
            topics,
            coordinator_epoch: 0, // Use 0 — the broker will validate
        };

        self.write_txn_markers(&[marker]).await
    }

    // ════════════════════════════════════════════════════════════════════
    // DescribeQuorum (API key 55)
    // ════════════════════════════════════════════════════════════════════

    /// Describe the KRaft quorum for the given topic-partitions.
    ///
    /// In a KRaft-mode cluster this returns the current voters, observers,
    /// leader, leader epoch, and high watermark for each quorum partition.
    ///
    /// The primary use case is inspecting `__cluster_metadata` partition 0.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = admin
    ///     .describe_quorum(&[("__cluster_metadata", &[0])])
    ///     .await?;
    /// ```
    pub async fn describe_metadata_quorum(
        &self,
        topic_partitions: &[(&str, &[i32])],
    ) -> Result<DescribeQuorumResult> {
        self.check_not_closed()?;
        let conn = self.get_any_broker_connection().await?;

        let topics = topic_partitions
            .iter()
            .map(|(name, partitions)| DescribeQuorumTopicRequest {
                topic_name: (*name).to_string(),
                partitions: partitions
                    .iter()
                    .map(|&p| DescribeQuorumPartitionRequest { partition_index: p })
                    .collect(),
            })
            .collect();

        let request = DescribeQuorumRequest { topics };

        let version = conn
            .negotiate_api_version(
                ApiKey::DescribeQuorum,
                versions::DESCRIBE_QUORUM_MAX,
                versions::DESCRIBE_QUORUM_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported DescribeQuorum API version",
                )
            })?;

        let response_bytes = conn
            .send_request(ApiKey::DescribeQuorum, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = DescribeQuorumResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            warn!("DescribeQuorum top-level error: {:?}", response.error_code);
        }

        let topics = response
            .topics
            .into_iter()
            .map(|t| QuorumTopicResult {
                topic_name: t.topic_name,
                partitions: t
                    .partitions
                    .into_iter()
                    .map(|p| QuorumPartitionResult {
                        partition_index: p.partition_index,
                        error: if p.error_code.is_ok() {
                            None
                        } else {
                            Some(format!("{:?}", p.error_code))
                        },
                        leader_id: p.leader_id,
                        leader_epoch: p.leader_epoch,
                        high_watermark: p.high_watermark,
                        current_voters: p
                            .current_voters
                            .into_iter()
                            .map(|v| QuorumReplicaInfo {
                                replica_id: v.replica_id,
                                log_end_offset: v.log_end_offset,
                            })
                            .collect(),
                        observers: p
                            .observers
                            .into_iter()
                            .map(|o| QuorumReplicaInfo {
                                replica_id: o.replica_id,
                                log_end_offset: o.log_end_offset,
                            })
                            .collect(),
                    })
                    .collect(),
            })
            .collect::<Vec<_>>();

        info!("DescribeQuorum returned {} topic(s)", topics.len());

        Ok(DescribeQuorumResult {
            error: if response.error_code.is_ok() {
                None
            } else {
                Some(format!("{:?}", response.error_code))
            },
            topics,
        })
    }
}
