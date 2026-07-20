//! AdminClient operation group: transactions.

use std::collections::HashMap;

use tracing::{info, warn};

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{
    ApiKey, ConfigResourceType, DescribeProducersRequest, DescribeProducersResponse,
    DescribeProducersTopicRequest, DescribeQuorumPartitionRequest, DescribeQuorumRequest,
    DescribeQuorumResponse, DescribeQuorumTopicRequest, DescribeTransactionsRequest,
    DescribeTransactionsResponse, ListConfigResourcesRequest, ListConfigResourcesResponse,
    ListTransactionsRequest, ListTransactionsResponse, ListedConfigResource, VersionedDecode,
    VersionedEncode, WritableTxnMarker, WritableTxnMarkerTopic, WriteTxnMarkersRequest,
    WriteTxnMarkersResponse, versions,
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
                // Refresh metadata after a stale-leader error. A rate-limited
                // refresh returns without contacting a broker; retrying against
                // that unchanged cache would reproduce the same
                // NotLeaderForPartition, so wait out the backoff first.
                let topics: Vec<&str> = topic_partitions.iter().map(|&(t, _)| t).collect();
                self.refresh_topics_for_retry(&topics, "DescribeProducers")
                    .await;
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
        //
        // Coordinator resolution retries on retriable errors and fails loudly
        // if it cannot be resolved. Falling back to an arbitrary broker (the
        // previous behaviour) guaranteed the follow-up DescribeTransactions
        // would answer NOT_COORDINATOR, with the real cause already discarded.
        let mut coordinator_txns: HashMap<(i32, String), Vec<String>> = HashMap::new();

        for txn_id in transactional_ids {
            let (node_id, addr) = self.find_coordinator_node(txn_id, true).await?;
            coordinator_txns
                .entry((node_id, addr))
                .or_default()
                .push((*txn_id).to_string());
        }

        let mut all_results = Vec::new();

        for ((broker_id, addr), txn_ids) in coordinator_txns {
            let conn = self.pool.get_connection_by_id(broker_id, &addr).await?;

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
                    warn!("DescribeTransactions request failed on broker {broker_id}: {e}");
                    continue;
                }
            };

            let mut buf = response_bytes;
            let response = match DescribeTransactionsResponse::decode_versioned(version, &mut buf) {
                Ok(r) => r,
                Err(e) => {
                    warn!("DescribeTransactions decode failed on broker {broker_id}: {e}");
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
    /// Pass empty slices for `state_filters` and `producer_id_filters`, `-1`
    /// for `duration_filter` and `None` for `transactional_id_pattern` to list
    /// all transactions.
    ///
    /// `transactional_id_pattern` requires ListTransactions v2 (KIP-1152,
    /// Kafka 4.1+). Against an older broker the negotiated version is lower and
    /// the pattern cannot be sent, so this method rejects the call rather than
    /// silently returning unfiltered results.
    ///
    /// # Example
    ///
    /// ```ignore
    /// // List all ongoing transactions
    /// let txns = admin
    ///     .list_transactions(&["Ongoing"], &[], -1, None)
    ///     .await?;
    ///
    /// // Only transactions whose transactional ID starts with "orders-"
    /// let txns = admin
    ///     .list_transactions(&[], &[], -1, Some("orders-.*"))
    ///     .await?;
    /// ```
    pub async fn list_transactions(
        &self,
        state_filters: &[&str],
        producer_id_filters: &[i64],
        duration_filter: i64,
        transactional_id_pattern: Option<&str>,
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
            transactional_id_pattern: transactional_id_pattern.map(str::to_string),
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

            // Silently dropping the pattern would return every transaction on
            // this broker, which reads as "nothing matched the filter" only by
            // accident. Surface the version gap instead.
            if transactional_id_pattern.is_some() && version < 2 {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    format!(
                        "transactional_id_pattern requires ListTransactions v2 (KIP-1152); \
                         broker {} negotiated v{version}",
                        broker.id()
                    ),
                ));
            }

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
    // ListConfigResources (API key 74)
    // ════════════════════════════════════════════════════════════════════

    /// List config resources known to the cluster (KIP-1142).
    ///
    /// Kafka 4.1 renamed API key 74 from `ListClientMetricsResources` to
    /// `ListConfigResources` and generalised it: v0 could only enumerate
    /// client-metrics subscriptions (KIP-714), while v1 enumerates any config
    /// resource type — topics, brokers, broker loggers, groups and client
    /// metrics.
    ///
    /// Pass an empty `resource_types` slice to get whichever types the broker
    /// lists by default.
    ///
    /// # Version behaviour
    ///
    /// Against a pre-4.1 broker only v0 is available, which ignores
    /// `resource_types` and returns client-metrics subscriptions. Requesting a
    /// type other than [`ConfigResourceType::ClientMetrics`] there would
    /// silently return the wrong set, so this method rejects the call instead.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use krafka::protocol::ConfigResourceType;
    ///
    /// // Everything the broker lists by default.
    /// let all = admin.list_config_resources(&[]).await?;
    ///
    /// // Just the topics.
    /// let topics = admin
    ///     .list_config_resources(&[ConfigResourceType::Topic])
    ///     .await?;
    /// ```
    pub async fn list_config_resources(
        &self,
        resource_types: &[ConfigResourceType],
    ) -> Result<Vec<ListedConfigResource>> {
        let conn = self.get_any_broker_connection().await?;

        let version = conn
            .negotiate_api_version(
                ApiKey::ListConfigResources,
                versions::LIST_CONFIG_RESOURCES_MAX,
                versions::LIST_CONFIG_RESOURCES_MIN,
            )
            .await
            .ok_or_else(|| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported ListConfigResources API version",
                )
            })?;

        if version < 1
            && resource_types
                .iter()
                .any(|t| *t != ConfigResourceType::ClientMetrics)
        {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::UnknownApiVersion,
                format!(
                    "listing config resource types other than ClientMetrics requires \
                     ListConfigResources v1 (KIP-1142); broker negotiated v{version}"
                ),
            ));
        }

        let request = ListConfigResourcesRequest::with_types(resource_types.to_vec());

        let response_bytes = conn
            .send_request(ApiKey::ListConfigResources, version, |buf| {
                request.encode_versioned(version, buf)
            })
            .await?;

        let mut buf = response_bytes;
        let response = ListConfigResourcesResponse::decode_versioned(version, &mut buf)?;

        if !response.error_code.is_ok() {
            warn!("ListConfigResources error: {:?}", response.error_code);
        }

        info!(
            "ListConfigResources returned {} resource(s)",
            response.config_resources.len()
        );
        Ok(response.config_resources)
    }

    /// List client metrics subscription names (KIP-714).
    ///
    /// Convenience wrapper over [`list_config_resources`](Self::list_config_resources)
    /// that asks for [`ConfigResourceType::ClientMetrics`] and returns only the
    /// names. Works against any broker: this is exactly what API key 74 did
    /// before KIP-1142 renamed it.
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
        let resources = self
            .list_config_resources(&[ConfigResourceType::ClientMetrics])
            .await?;
        Ok(resources
            .into_iter()
            .filter(|r| r.resource_type == ConfigResourceType::ClientMetrics)
            .map(|r| r.name)
            .collect())
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
    /// # Routing
    ///
    /// `WriteTxnMarkers` must reach the **leader of each partition**: only the
    /// leader can append the marker to the log. This method groups each
    /// marker's topic-partitions by their current leader and sends one
    /// `WriteTxnMarkers` request per leader, then merges the per-partition
    /// results back together.
    ///
    /// Sending the whole marker set to a single broker instead would leave a
    /// transaction that spans several brokers only partially finalised: the
    /// other leaders answer `NOT_LEADER_OR_FOLLOWER`, the transaction stays
    /// open, and `read_committed` consumers block on the last stable offset
    /// indefinitely.
    ///
    /// # Errors
    ///
    /// Returns `Err` if a partition's leader is unknown, or if the request to a
    /// leader fails outright — a marker that was never written must not be
    /// reported as success. Per-partition broker errors are surfaced in
    /// [`WriteTxnMarkersPartitionResult::error`].
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
    ///         transaction_version: 0,
    ///     }])
    ///     .await?;
    /// ```
    pub async fn write_txn_markers(
        &self,
        markers: &[WritableTxnMarker],
    ) -> Result<Vec<WriteTxnMarkersResult>> {
        self.check_not_closed()?;

        // Refresh so leader resolution below is not based on a stale snapshot;
        // a marker sent to a former leader is silently ineffective.
        let topic_names: Vec<&str> = markers
            .iter()
            .flat_map(|m| m.topics.iter().map(|t| t.name.as_str()))
            .collect();
        if !topic_names.is_empty() {
            self.refresh_topics_for_retry(&topic_names, "WriteTxnMarkers")
                .await;
        }

        // Group every (marker, topic, partition) triple by the partition's
        // current leader.
        let by_leader = plan_markers_by_leader(markers, |topic, partition| {
            self.metadata.leader(topic, partition)
        })?;

        // Accumulate per-producer, per-topic, per-partition results across all
        // leaders. Keyed by producer_id → topic → partition.
        let mut merged: HashMap<i64, HashMap<String, Vec<WriteTxnMarkersPartitionResult>>> =
            HashMap::new();

        for (leader_id, plan) in by_leader {
            let conn = self.metadata.get_broker_connection(leader_id).await?;

            let request = WriteTxnMarkersRequest {
                markers: plan
                    .into_iter()
                    .map(|(idx, topics)| {
                        let src = &markers[idx];
                        WritableTxnMarker {
                            producer_id: src.producer_id,
                            producer_epoch: src.producer_epoch,
                            transaction_result: src.transaction_result,
                            coordinator_epoch: src.coordinator_epoch,
                            transaction_version: src.transaction_version,
                            topics: topics
                                .into_iter()
                                .map(|(name, mut partition_indexes)| {
                                    partition_indexes.sort_unstable();
                                    partition_indexes.dedup();
                                    WritableTxnMarkerTopic {
                                        name,
                                        partition_indexes,
                                    }
                                })
                                .collect(),
                        }
                    })
                    .collect(),
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

            // A transport failure to one leader means that leader's partitions
            // were definitely not marked. Fail loudly instead of returning a
            // partial success that leaves the transaction stuck.
            let response_bytes = conn
                .send_request(ApiKey::WriteTxnMarkers, version, |buf| {
                    request.encode_versioned(version, buf)
                })
                .await
                .map_err(|e| {
                    KrafkaError::network(std::io::Error::other(format!(
                        "WriteTxnMarkers failed on leader {leader_id}: {e}"
                    )))
                })?;

            let mut buf = response_bytes;
            let response = WriteTxnMarkersResponse::decode_versioned(version, &mut buf)?;

            for m in response.markers {
                let per_topic = merged.entry(m.producer_id).or_default();
                for t in m.topics {
                    let entries = per_topic.entry(t.name).or_default();
                    for p in t.partitions {
                        if !p.error_code.is_ok() {
                            warn!(
                                leader = leader_id,
                                partition = p.partition_index,
                                "WriteTxnMarkers partition error: {:?}",
                                p.error_code
                            );
                        }
                        entries.push(WriteTxnMarkersPartitionResult {
                            partition_index: p.partition_index,
                            error: if p.error_code.is_ok() {
                                None
                            } else {
                                Some(format!("{:?}", p.error_code))
                            },
                        });
                    }
                }
            }
        }

        let results: Vec<WriteTxnMarkersResult> = merged
            .into_iter()
            .map(|(producer_id, topics)| WriteTxnMarkersResult {
                producer_id,
                topics: topics
                    .into_iter()
                    .map(|(name, partitions)| WriteTxnMarkersTopicResult { name, partitions })
                    .collect(),
            })
            .collect();

        let failed: usize = results
            .iter()
            .flat_map(|r| r.topics.iter())
            .flat_map(|t| t.partitions.iter())
            .filter(|p| p.error.is_some())
            .count();
        if failed > 0 {
            warn!(
                "WriteTxnMarkers: {failed} partition(s) failed; the transaction may remain open \
                 and read_committed consumers will block on the LSO until it is resolved"
            );
        }
        info!(
            "WriteTxnMarkers returned {} marker result(s), {failed} partition failure(s)",
            results.len()
        );
        Ok(results)
    }

    /// Abort a stuck transaction by writing an ABORT marker.
    ///
    /// This is the admin-friendly wrapper around
    /// [`write_txn_markers`](Self::write_txn_markers) that discovers the
    /// affected partitions via
    /// [`describe_transactions`](Self::describe_transactions), determines the
    /// owning coordinator epoch via
    /// [`describe_producers`](Self::describe_producers), and writes an ABORT
    /// marker to each partition's leader.
    ///
    /// # The coordinator epoch is discovered, never assumed
    ///
    /// The partition leader validates the marker's `coordinator_epoch` against
    /// the epoch it has cached for that producer, and **accepts any epoch when
    /// it has none cached**. Passing a fabricated `0` therefore succeeds
    /// exactly on the partitions where validation cannot protect you — aborting
    /// a transaction that a live, newer coordinator still owns and discarding
    /// committed exactly-once data.
    ///
    /// This method instead reads the real epoch from the active producer state
    /// on the target partitions, mirroring Java's `AbortTransactionSpec`.
    /// [`abort_transaction_with_epoch`](Self::abort_transaction_with_epoch)
    /// takes the epoch explicitly when the caller already knows it.
    ///
    /// # Errors
    ///
    /// Returns an error when the transaction cannot be described, when no
    /// active producer state exposes a coordinator epoch (so no safe epoch can
    /// be determined), or when the discovered producer state disagrees across
    /// partitions.
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

        let desc = self
            .describe_transaction_for_abort(transactional_id)
            .await?;

        // Resolve the coordinator epoch from the live producer state on the
        // partitions this transaction touches.
        let topic_partitions: Vec<(String, Vec<i32>)> = desc
            .topics
            .iter()
            .map(|t| (t.topic.clone(), t.partitions.clone()))
            .collect();
        let borrowed: Vec<(&str, &[i32])> = topic_partitions
            .iter()
            .map(|(t, ps)| (t.as_str(), ps.as_slice()))
            .collect();

        let coordinator_epoch = self
            .resolve_coordinator_epoch(&borrowed, desc.producer_id, transactional_id)
            .await?;

        self.abort_transaction_with_epoch(transactional_id, coordinator_epoch)
            .await
    }

    /// Abort a stuck transaction using a caller-supplied coordinator epoch.
    ///
    /// Use this when the epoch is already known (for example from a previous
    /// [`describe_producers`](Self::describe_producers) call); otherwise prefer
    /// [`abort_transaction`](Self::abort_transaction), which discovers it.
    ///
    /// # Warning
    ///
    /// Supplying an epoch older than the one the partition leader has cached
    /// is rejected by the broker, but supplying an arbitrary epoch to a leader
    /// with *no* cached epoch is **accepted**. Never invent a value here.
    pub async fn abort_transaction_with_epoch(
        &self,
        transactional_id: &str,
        coordinator_epoch: i32,
    ) -> Result<Vec<WriteTxnMarkersResult>> {
        self.check_not_closed()?;

        let desc = self
            .describe_transaction_for_abort(transactional_id)
            .await?;

        let topics: Vec<WritableTxnMarkerTopic> = desc
            .topics
            .iter()
            .map(|t| WritableTxnMarkerTopic {
                name: t.topic.clone(),
                partition_indexes: t.partitions.clone(),
            })
            .collect();

        if topics.is_empty() {
            return Err(KrafkaError::invalid_state(format!(
                "transaction '{transactional_id}' has no partitions to abort"
            )));
        }

        let marker = WritableTxnMarker {
            producer_id: desc.producer_id,
            producer_epoch: desc.producer_epoch,
            transaction_result: false, // ABORT
            topics,
            coordinator_epoch,
            // TV2 markers are written by the coordinator itself; an admin-driven
            // abort always uses the legacy encoding the broker accepts on any
            // WriteTxnMarkers version.
            transaction_version: WritableTxnMarker::legacy_transaction_version(),
        };

        info!(
            transactional_id,
            producer_id = desc.producer_id,
            coordinator_epoch,
            "aborting transaction by writing ABORT markers"
        );
        self.write_txn_markers(&[marker]).await
    }

    /// Describe a transaction and validate that it is abortable.
    async fn describe_transaction_for_abort(
        &self,
        transactional_id: &str,
    ) -> Result<TransactionDescription> {
        let descriptions = self.describe_transactions(&[transactional_id]).await?;
        let desc = descriptions.into_iter().next().ok_or_else(|| {
            KrafkaError::protocol_kind(
                ProtocolErrorKind::Malformed,
                format!("no transaction description returned for '{transactional_id}'"),
            )
        })?;

        if let Some(ref err) = desc.error {
            return Err(KrafkaError::invalid_state(format!(
                "cannot abort transaction '{transactional_id}': {err}"
            )));
        }

        Ok(desc)
    }

    /// Determine the coordinator epoch that currently owns `producer_id` on the
    /// given partitions.
    ///
    /// Reads active producer state via `DescribeProducers` and takes the
    /// coordinator epoch from the entry matching `producer_id`. All partitions
    /// reporting this producer must agree; disagreement means the transaction
    /// is mid-transition and aborting it is unsafe.
    async fn resolve_coordinator_epoch(
        &self,
        topic_partitions: &[(&str, &[i32])],
        producer_id: i64,
        transactional_id: &str,
    ) -> Result<i32> {
        if topic_partitions.is_empty() {
            return Err(KrafkaError::invalid_state(format!(
                "transaction '{transactional_id}' lists no partitions; \
                 cannot determine its coordinator epoch"
            )));
        }

        let producers = self.describe_producers(topic_partitions).await?;

        let mut found: Option<i32> = None;
        for topic in &producers {
            for partition in &topic.partitions {
                for state in &partition.active_producers {
                    if state.producer_id != producer_id {
                        continue;
                    }
                    match found {
                        None => found = Some(state.coordinator_epoch),
                        Some(existing) if existing == state.coordinator_epoch => {}
                        Some(existing) => {
                            return Err(KrafkaError::invalid_state(format!(
                                "transaction '{transactional_id}' reports conflicting coordinator \
                                 epochs ({existing} vs {}) on {}-{}; the transaction is mid-transition \
                                 and aborting it now could discard committed data",
                                state.coordinator_epoch, topic.name, partition.partition_index
                            )));
                        }
                    }
                }
            }
        }

        found.ok_or_else(|| {
            KrafkaError::invalid_state(format!(
                "no active producer state for producer_id {producer_id} on the partitions of \
                 transaction '{transactional_id}'; cannot determine the coordinator epoch. \
                 Fabricating one risks aborting a transaction owned by a live coordinator — \
                 pass a known epoch to `abort_transaction_with_epoch` if you are certain."
            ))
        })
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

/// A per-leader plan: marker index → topic name → partition indexes.
type MarkerPlan = HashMap<usize, HashMap<String, Vec<i32>>>;

/// Group every `(marker, topic, partition)` triple by the partition's current
/// leader, so that one `WriteTxnMarkers` request can be built per leader.
///
/// `WriteTxnMarkers` is only meaningful at the partition leader — it appends
/// the control record to the log. Sending a whole marker set to one broker
/// leaves partitions led by other brokers unmarked, so the transaction stays
/// open and `read_committed` consumers block on the last stable offset.
///
/// # Errors
///
/// Returns [`crate::error::ErrorCode::LeaderNotAvailable`] if any partition's
/// leader is unknown. Writing markers for the rest and reporting success would
/// leave the transaction partially finalised, which is the exact failure this
/// grouping exists to prevent.
fn plan_markers_by_leader(
    markers: &[WritableTxnMarker],
    leader_of: impl Fn(&str, i32) -> Option<i32>,
) -> Result<HashMap<i32, MarkerPlan>> {
    let mut by_leader: HashMap<i32, MarkerPlan> = HashMap::new();

    for (idx, marker) in markers.iter().enumerate() {
        for topic in &marker.topics {
            for &partition in &topic.partition_indexes {
                let leader = leader_of(&topic.name, partition).ok_or_else(|| {
                    KrafkaError::broker(
                        crate::error::ErrorCode::LeaderNotAvailable,
                        format!(
                            "WriteTxnMarkers: no known leader for {}-{partition}; \
                             refusing to write a marker that would not be applied",
                            topic.name
                        ),
                    )
                })?;
                by_leader
                    .entry(leader)
                    .or_default()
                    .entry(idx)
                    .or_default()
                    .entry(topic.name.clone())
                    .or_default()
                    .push(partition);
            }
        }
    }

    Ok(by_leader)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn marker(
        producer_id: i64,
        coordinator_epoch: i32,
        topics: &[(&str, &[i32])],
    ) -> WritableTxnMarker {
        WritableTxnMarker {
            producer_id,
            producer_epoch: 7,
            transaction_result: false,
            coordinator_epoch,
            transaction_version: 0,
            topics: topics
                .iter()
                .map(|(name, partitions)| WritableTxnMarkerTopic {
                    name: (*name).to_string(),
                    partition_indexes: partitions.to_vec(),
                })
                .collect(),
        }
    }

    /// A transaction spanning three brokers must produce three requests, each
    /// carrying only that leader's partitions. Sending everything to one broker
    /// is what left transactions stuck.
    #[test]
    fn test_markers_are_split_across_partition_leaders() {
        let m = marker(42, 10, &[("orders", &[0, 1, 2])]);
        // orders-0 -> broker 1, orders-1 -> broker 2, orders-2 -> broker 3
        let plan = plan_markers_by_leader(&[m], |_topic, p| Some(p + 1)).unwrap();

        assert_eq!(plan.len(), 3, "expected one request per leader");
        for (leader, marker_plan) in &plan {
            let partitions = &marker_plan[&0]["orders"];
            assert_eq!(
                partitions,
                &vec![leader - 1],
                "leader {leader} must only receive the partitions it leads"
            );
        }
    }

    /// Partitions sharing a leader are coalesced into one request.
    #[test]
    fn test_markers_sharing_a_leader_are_batched() {
        let m = marker(42, 10, &[("orders", &[0, 1, 2, 3])]);
        let plan = plan_markers_by_leader(&[m], |_t, _p| Some(9)).unwrap();

        assert_eq!(plan.len(), 1);
        let mut partitions = plan[&9][&0]["orders"].clone();
        partitions.sort_unstable();
        assert_eq!(partitions, vec![0, 1, 2, 3]);
    }

    /// Multiple markers (producers) hitting the same leader stay distinct, so
    /// each producer's marker keeps its own producer_id/epoch on the wire.
    #[test]
    fn test_multiple_markers_stay_separate_per_producer() {
        let markers = vec![marker(1, 10, &[("a", &[0])]), marker(2, 11, &[("a", &[0])])];
        let plan = plan_markers_by_leader(&markers, |_t, _p| Some(5)).unwrap();

        assert_eq!(plan.len(), 1);
        let marker_plan = &plan[&5];
        assert_eq!(marker_plan.len(), 2, "each marker must stay separate");
        assert!(marker_plan.contains_key(&0));
        assert!(marker_plan.contains_key(&1));
    }

    /// An unknown leader must abort the whole write rather than silently
    /// skipping that partition — a partially written marker set leaves the
    /// transaction open forever.
    #[test]
    fn test_unknown_leader_is_a_hard_error() {
        let m = marker(42, 10, &[("orders", &[0, 1])]);
        let err = plan_markers_by_leader(&[m], |_t, p| if p == 0 { Some(1) } else { None })
            .expect_err("unknown leader must fail");

        match err {
            KrafkaError::Broker { code, ref message } => {
                assert_eq!(code, crate::error::ErrorCode::LeaderNotAvailable);
                assert!(message.contains("orders-1"), "got: {message}");
            }
            other => panic!("expected Broker error, got {other:?}"),
        }
    }

    /// Grouping preserves the coordinator epoch supplied by the caller; it must
    /// never be defaulted to 0 anywhere in this path.
    #[test]
    fn test_coordinator_epoch_is_carried_through_untouched() {
        let m = marker(42, 1234, &[("a", &[0])]);
        assert_eq!(m.coordinator_epoch, 1234);
        let plan = plan_markers_by_leader(std::slice::from_ref(&m), |_t, _p| Some(1)).unwrap();
        // The plan indexes back into the original markers, so the epoch is
        // taken from the caller's value rather than reconstructed.
        assert!(plan[&1].contains_key(&0));
        assert_eq!(m.coordinator_epoch, 1234);
    }

    #[test]
    fn test_empty_marker_set_plans_nothing() {
        let plan = plan_markers_by_leader(&[], |_t, _p| Some(1)).unwrap();
        assert!(plan.is_empty());
    }
}
