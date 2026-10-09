//! Kafka consumer.
//!
//! [`Consumer`] reads records from assigned partitions — through a consumer
//! group (classic or KIP-848 protocol) or a manual [`assign`](Consumer::assign)
//! — and commits offsets.
//!
//! # Position and delivery
//!
//! A partition's position is the offset of the next record the consumer will
//! hand to the application. It advances only when records are returned from
//! [`poll`](Consumer::poll), [`recv`](Consumer::recv) or the
//! [stream](Consumer::stream),
//! after deserialization. Records fetched ahead of that stay buffered and do
//! not move it. [`commit`](Consumer::commit) commits the positions;
//! committing never moves them.
//!
//! `poll`, `recv` and the [stream](Consumer::stream) are cancel safe; each
//! method's `# Cancel safety` section says what dropping it does.
//!
//! # Delivery semantics
//!
//! At-least-once by default: auto-commit commits the positions, which cover
//! the records already returned — not necessarily processed. To commit only
//! processed records, disable auto-commit and call [`Consumer::commit`] after
//! processing.

mod assignor;
mod builder;
mod config;
mod fetch_session;
mod fetcher;
mod group;
mod group_metadata;
mod offset;
mod offsets;
mod rebalance;
mod record;
mod state;
mod stream;

pub mod compacted;

pub use builder::ConsumerBuilder;
pub use compacted::{
    CompactedEntry, CompactedTable, CompactedTableClearListener, CompactedTableSnapshot,
    CompactedTopicConsumer, TableChange,
};
pub(crate) use config::ConsumerConfig;
pub use config::{AutoOffsetReset, GroupProtocol, IsolationLevel, PartitionAssignmentStrategy};
pub(crate) use group::{COORDINATOR_REDISCOVERY_MAX_ATTEMPTS, is_coordinator_retriable};
pub use group_metadata::ConsumerGroupMetadata;
pub use offset::OffsetAndMetadata;
pub use rebalance::{ConsumerRebalanceListener, NoOpRebalanceListener};
pub(crate) use record::headers_from_wire;
pub use record::{ConsumerRecord, TimestampType, TopicPartition};
pub use stream::ConsumerStream;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::Instant;

use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use bytes::Bytes;
use parking_lot::Mutex as SyncMutex;
use std::borrow::Borrow;
use std::collections::{HashMap as StdHashMap, HashSet as StdHashSet};
use tracing::{Instrument, debug, info, warn};

use crate::client::{CloseOptions, GroupMembershipOperation, Kafka};
use crate::error::{KrafkaError, Result};
use crate::metadata::{BrokerInfo, ClusterMetadata, TopicInfo};
use crate::metrics::{ClientInstanceId, ConsumerRecorder, Metrics, MetricsSource};
use crate::network::ConnectionPool;
use crate::protocol::{validate_topic_name, validate_topic_names};
use crate::telemetry::{ClientType, Telemetry};
use crate::{Offset, PartitionId};

use group::GroupCoordinator;
use offsets::CommitRequestOffsets;
use rebalance::ErasedRebalanceListener;
use state::{DeliveryGuard, OffsetReset, PartitionKey, SubscriptionState};

/// Cluster metadata snapshot returned by [`Consumer::fetch_metadata`].
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct FetchMetadataResult {
    /// All brokers known to the cluster.
    pub brokers: Vec<BrokerInfo>,
    /// The requested topic if found, or every cached topic when called with
    /// `None`.
    pub topics: Vec<TopicInfo>,
}

/// What [`Consumer::lag`] reports for one assigned partition, from the
/// watermarks cached by fetch responses (no request is sent).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionLag {
    /// The offset of the next record the consumer will hand out.
    pub position: Option<Offset>,
    /// The log start offset.
    pub log_start_offset: Option<Offset>,
    /// The high watermark.
    pub high_watermark: Option<Offset>,
    /// The last stable offset (Fetch v4+); the readable end under
    /// `read_committed`.
    pub last_stable_offset: Option<Offset>,
    /// Records between the position and the readable end offset (the last
    /// stable offset under `read_committed`, else the high watermark).
    /// `None` until both are known.
    pub lag: Option<u64>,
    /// Whether the cached watermarks are older than
    /// [`lag_staleness_threshold`](ConsumerBuilder::lag_staleness_threshold).
    pub stale: bool,
}

/// A position being committed for one partition.
///
/// The leader epoch is stored with the offset and returned by `OffsetFetch`,
/// so the next owner of the partition can check the log still contains that
/// `(offset, epoch)` pair (KIP-320).
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommitPosition {
    /// Next offset the group should read from.
    pub offset: Offset,
    /// Leader epoch of the record before `offset`, or `-1` when unknown (a
    /// position from a seek or a reset).
    pub leader_epoch: i32,
    /// Optional application metadata stored with the offset.
    pub metadata: Option<String>,
}

/// How long `Consumer::close` may take without a timeout of its own.
const DEFAULT_CLOSE_TIMEOUT: Duration = Duration::from_secs(30);

/// A committed position read back from the coordinator.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct CommittedPosition {
    /// The committed offset.
    pub offset: Offset,
    /// Leader epoch stored with it, or `-1` if the group never committed one.
    pub leader_epoch: i32,
}

/// A Kafka consumer, built with [`Kafka::consumer`].
///
/// `Consumer` is `Send + Sync`; share it across tasks with `Arc`. All its
/// state sits behind one mutex that is never held across an `.await`.
pub struct Consumer {
    config: ConsumerConfig,
    metadata: Arc<ClusterMetadata>,
    pool: Arc<ConnectionPool>,
    /// Subscription, assignment and per-partition positions and buffers.
    state: Arc<SyncMutex<SubscriptionState>>,
    /// Serialises offset commits, retries included, so they leave in call
    /// order. The one lock held across an `.await`; it guards no state.
    commit_gate: Arc<tokio::sync::Mutex<()>>,
    closed: AtomicBool,
    /// Set by [`Consumer::wakeup`], cleared by the `poll()` that observes it.
    /// A flag as well as a `Notify`, so a `wakeup()` that lands before
    /// `poll()` is called still takes effect.
    wakeup_flag: AtomicBool,
    /// Wakes a `poll()` parked on a fetch or an idle wait.
    wakeup_notify: tokio::sync::Notify,
    group_coordinator: Option<Arc<GroupCoordinator>>,
    metrics: Arc<ConsumerRecorder>,
    /// Where `metrics()` and the KIP-714 reporter read from.
    metrics_source: Arc<MetricsSource>,
    /// The KIP-714 reporter.
    telemetry: Telemetry,
    rebalance_listener: Arc<dyn ErasedRebalanceListener>,
    interceptor: Arc<dyn crate::interceptor::ConsumerInterceptor>,
    /// Applied to every record key before it is returned (`key.deserializer`).
    key_deserializer: Option<Arc<dyn crate::serdes::Deserializer>>,
    /// Applied to every record value before it is returned
    /// (`value.deserializer`).
    value_deserializer: Option<Arc<dyn crate::serdes::Deserializer>>,
}

// Accessors that read the state mutex keep their `async` signature; the
// public API shape is settled separately.
#[allow(clippy::unused_async)]
impl Consumer {
    /// Create a consumer on `kafka`'s pool. Contacts no broker.
    fn new(kafka: &Kafka, config: ConsumerConfig) -> Self {
        let pool = Arc::clone(kafka.pool());
        let metadata = Arc::clone(kafka.metadata());
        let group_coordinator = config.group_id.as_ref().map(|group_id| {
            Arc::new(
                GroupCoordinator::new(
                    group_id.clone(),
                    pool.clone(),
                    metadata.clone(),
                    config.session_timeout,
                    config.heartbeat_interval,
                    // The rebalance timeout is max.poll.interval.ms, as in Java.
                    config.max_poll_interval,
                )
                .with_assignor_strategies(config.partition_assignment_strategies.clone())
                .with_group_instance_id(config.group_instance_id.clone())
                .with_client_rack(config.client_rack.clone())
                .with_isolation_level(config.isolation_level.to_i8())
                .with_group_protocol(config.group_protocol)
                .with_server_assignor(config.group_remote_assignor.clone()),
            )
        });

        info!(group_id = ?config.group_id, "consumer created");
        let metrics = Arc::new(ConsumerRecorder::default());
        let metrics_source = MetricsSource::consumer(kafka, Arc::clone(&metrics));
        let telemetry = Telemetry::start(
            config.metrics_push,
            kafka,
            ClientType::Consumer,
            Arc::clone(&metrics_source),
        );

        Self {
            config,
            metadata,
            pool,
            state: Arc::new(SyncMutex::new(SubscriptionState::default())),
            commit_gate: Arc::new(tokio::sync::Mutex::new(())),
            closed: AtomicBool::new(false),
            wakeup_flag: AtomicBool::new(false),
            wakeup_notify: tokio::sync::Notify::new(),
            group_coordinator,
            metrics,
            metrics_source,
            telemetry,
            rebalance_listener: Arc::new(NoOpRebalanceListener),
            interceptor: Arc::new(crate::interceptor::NoOpConsumerInterceptor),
            key_deserializer: None,
            value_deserializer: None,
        }
    }

    /// Subscribe to topics, replacing the current subscription.
    ///
    /// With a `group_id` the next [`poll`](Self::poll) joins the group (or
    /// rejoins it with the new subscription). Without one, the consumer
    /// assigns itself every partition of the topics and keeps following
    /// cluster metadata: topics created later and partitions added later are
    /// picked up.
    ///
    /// Takes any collection of topic names: `["a", "b"]`, `&topics`,
    /// `vec![String::from("a")]`.
    pub async fn subscribe(&self, topics: impl IntoIterator<Item = impl AsRef<str>>) -> Result<()> {
        let topics: Vec<String> = topics.into_iter().map(|t| t.as_ref().to_string()).collect();
        validate_topic_names(topics.iter().map(String::as_str))?;
        {
            let mut state = self.state.lock();
            state.subscription = topics.iter().cloned().collect();
        }
        let names: Vec<&str> = topics.iter().map(String::as_str).collect();
        self.metadata.refresh_for_topics(Some(&names)).await?;

        if let Some(ref coordinator) = self.group_coordinator {
            coordinator.note_poll();
            coordinator.set_subscription(topics.clone());
            // A classic member that owns nothing has nothing to revoke first,
            // so its join can start now, on its own task; poll applies it.
            if !coordinator.is_consumer_protocol()
                && self.state.lock().assigned_count() == 0
                && coordinator.needs_rejoin()
            {
                coordinator.spawn_rejoin(true);
            }
        } else {
            {
                let mut state = self.state.lock();
                state.standalone_topics = topics.iter().cloned().collect();
                state.standalone_resolved = None;
            }
            self.resolve_standalone_subscription(true).await?;
        }
        debug!("Subscribed to topics: {:?}", topics);
        Ok(())
    }

    /// Assign partitions of one topic manually, replacing that topic's
    /// previous manual assignment. Not available with a `group_id`.
    ///
    /// New partitions start at a configured initial offset, else at the reset
    /// `auto_offset_reset` asks for, resolved by the next poll.
    pub async fn assign(&self, topic: &str, partitions: Vec<PartitionId>) -> Result<()> {
        validate_topic_name(topic)?;
        if self.group_coordinator.is_some() {
            return Err(KrafkaError::illegal_state(
                "cannot use manual partition assignment with consumer group subscription",
            ));
        }
        self.metadata.refresh_for_topics(Some(&[topic])).await?;
        {
            let mut state = self.state.lock();
            let keep: HashSet<PartitionId> = partitions.iter().copied().collect();
            let dropped: Vec<PartitionKey> = state
                .assigned_keys()
                .into_iter()
                .filter(|(t, p)| t == topic && !keep.contains(p))
                .collect();
            state.remove_partitions(&dropped);
            state.add_partitions(partitions.iter().map(|&p| (topic.to_string(), p)));
            state.subscription.insert(topic.to_string());
            // The caller owns this topic's partition list from here on; the
            // metadata-driven resolver must not widen it again.
            state.standalone_topics.remove(topic);
        }
        self.update_gauges();
        debug!("Assigned partitions for {}: {:?}", topic, partitions);
        Ok(())
    }

    /// Move an assigned partition's position to `offset`. Buffered records of
    /// the partition are dropped; the next poll fetches from `offset`.
    ///
    /// # Errors
    ///
    /// [`KrafkaError::IllegalState`] when the partition is not assigned;
    /// nothing is stored.
    pub async fn seek(&self, topic: &str, partition: PartitionId, offset: Offset) -> Result<()> {
        self.state
            .lock()
            .seek(&(topic.to_string(), partition), offset)?;
        self.metrics.record_seek(1);
        self.update_gauges();
        debug!("Seek to offset {} for {}-{}", offset, topic, partition);
        Ok(())
    }

    /// Seek several partitions at once. Either every partition is assigned
    /// and all are moved, or none is.
    ///
    /// # Errors
    ///
    /// [`KrafkaError::IllegalState`] when any partition is not assigned.
    pub async fn seek_many<P, O>(&self, offsets: impl IntoIterator<Item = (P, O)>) -> Result<()>
    where
        P: Borrow<TopicPartition>,
        O: Borrow<Offset>,
    {
        let offsets: Vec<(PartitionKey, Offset)> = offsets
            .into_iter()
            .map(|(tp, offset)| {
                let tp = tp.borrow();
                ((tp.topic.clone(), tp.partition), *offset.borrow())
            })
            .collect();
        {
            let mut state = self.state.lock();
            if let Some(((topic, partition), _)) =
                offsets.iter().find(|(key, _)| !state.is_assigned(key))
            {
                return Err(KrafkaError::illegal_state(format!(
                    "no current assignment for partition {topic}-{partition}"
                )));
            }
            for (key, offset) in &offsets {
                state.seek(key, *offset)?;
            }
        }
        self.metrics.record_seek(offsets.len() as u64);
        self.update_gauges();
        Ok(())
    }

    /// Move an assigned partition to the start of its retained log. The
    /// offset is resolved with `ListOffsets` (earliest) by the next poll, or
    /// by [`position`](Self::position).
    ///
    /// # Errors
    ///
    /// [`KrafkaError::IllegalState`] when the partition is not assigned.
    pub async fn seek_to_beginning(&self, topic: &str, partition: PartitionId) -> Result<()> {
        self.state
            .lock()
            .request_reset(&(topic.to_string(), partition), OffsetReset::Earliest)?;
        self.metrics.record_seek(1);
        Ok(())
    }

    /// Move an assigned partition to the end of its log, so only records
    /// produced afterwards are delivered. Resolved with `ListOffsets` (latest)
    /// by the next poll, or by [`position`](Self::position).
    ///
    /// # Errors
    ///
    /// [`KrafkaError::IllegalState`] when the partition is not assigned.
    pub async fn seek_to_end(&self, topic: &str, partition: PartitionId) -> Result<()> {
        self.state
            .lock()
            .request_reset(&(topic.to_string(), partition), OffsetReset::Latest)?;
        self.metrics.record_seek(1);
        Ok(())
    }

    /// Seek an assigned partition to the first record whose timestamp is at
    /// or after `timestamp_ms`.
    ///
    /// # Errors
    ///
    /// [`KrafkaError::IllegalState`] when the partition is not assigned;
    /// [`KrafkaError::NoOffset`] when no record is that new; lookup failures.
    pub async fn seek_to_timestamp(
        &self,
        topic: &str,
        partition: PartitionId,
        timestamp_ms: i64,
    ) -> Result<()> {
        if !self
            .state
            .lock()
            .is_assigned(&(topic.to_string(), partition))
        {
            return Err(KrafkaError::illegal_state(format!(
                "no current assignment for partition {topic}-{partition}"
            )));
        }
        let offset = self.list_offset(topic, partition, timestamp_ms).await?;
        if offset < 0 {
            return Err(KrafkaError::no_offset(vec![(topic.to_string(), partition)]));
        }
        self.seek(topic, partition, offset).await
    }

    /// The earliest offset whose timestamp is at or after `timestamp`, for
    /// each partition. Every partition is in the result: `Ok(-1)` when no
    /// record is that new, `Err` when it could not be resolved.
    pub async fn offsets_for_times(
        &self,
        partitions: &[(&str, PartitionId)],
        timestamp: i64,
    ) -> StdHashMap<(String, PartitionId), Result<Offset>> {
        let mut result: StdHashMap<(String, PartitionId), Result<Offset>> = StdHashMap::new();
        let mut valid: Vec<PartitionKey> = Vec::with_capacity(partitions.len());
        for &(topic, partition) in partitions {
            match validate_topic_name(topic) {
                Ok(()) => valid.push((topic.to_string(), partition)),
                Err(e) => {
                    result.insert((topic.to_string(), partition), Err(e));
                }
            }
        }
        if !valid.is_empty() {
            result.extend(self.list_offsets(&Self::by_topic(&valid), timestamp).await);
        }
        result
    }

    /// [`offsets_for_times`](Self::offsets_for_times) for every partition of
    /// one topic, after refreshing its metadata.
    ///
    /// # Errors
    ///
    /// Returns an error if the topic is unknown after the refresh.
    pub async fn offsets_for_times_for_topic(
        &self,
        topic: &str,
        timestamp: i64,
    ) -> Result<StdHashMap<PartitionId, Result<Offset>>> {
        validate_topic_name(topic)?;
        self.metadata.refresh_for_topics(Some(&[topic])).await?;
        let info = self
            .metadata
            .topic(topic)
            .ok_or_else(|| KrafkaError::unknown_topic(topic))?;
        let mut grouped = HashMap::new();
        grouped.insert(
            topic.to_string(),
            info.partitions.values().map(|p| p.partition).collect(),
        );
        Ok(self
            .list_offsets(&grouped, timestamp)
            .await
            .into_iter()
            .map(|((_, p), result)| (p, result))
            .collect())
    }

    /// The low (log start) and high watermarks of a partition, from two
    /// concurrent `ListOffsets` lookups.
    pub async fn fetch_watermarks(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> Result<(Offset, Offset)> {
        validate_topic_name(topic)?;
        let (low, high) = tokio::join!(
            self.list_offset(topic, partition, -2),
            self.list_offset(topic, partition, -1),
        );
        Ok((low?, high?))
    }

    /// A snapshot of cluster metadata. With `Some(topic)` the topic is
    /// refreshed first; with `None` the cache is returned as is.
    pub async fn fetch_metadata(&self, topic: Option<&str>) -> Result<FetchMetadataResult> {
        if let Some(name) = topic {
            validate_topic_name(name)?;
            self.metadata.refresh_for_topics(Some(&[name])).await?;
        }
        let topics = match topic {
            Some(name) => self.metadata.topic(name).into_iter().collect(),
            None => self.metadata.topics(),
        };
        Ok(FetchMetadataResult {
            brokers: self.metadata.brokers(),
            topics,
        })
    }

    /// Poll for records, waiting up to `timeout`.
    ///
    /// Returns as soon as records are available — buffered ones at once,
    /// without a round trip — at most
    /// [`max_poll_records`](ConsumerBuilder::max_poll_records) of them, or an
    /// empty batch after `timeout`. Group membership is maintained here:
    /// rebalances are applied, listener callbacks run, and auto-commit
    /// happens.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. Records leave the buffer, and positions
    /// move, only at the synchronous point where the call returns them: a
    /// dropped call returns nothing and loses nothing, and the next call
    /// returns the records it would have.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # async fn example(kafka: krafka::Kafka) -> krafka::Result<()> {
    /// let consumer = kafka.consumer("my-group").build().await?;
    /// consumer.subscribe(["my-topic"]).await?;
    ///
    /// loop {
    ///     for record in consumer.poll(std::time::Duration::from_secs(1)).await? {
    ///         println!("Received: {record:?}");
    ///     }
    /// }
    /// # }
    /// ```
    pub async fn poll(&self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        let max = if self.config.max_poll_records > 0 {
            self.config.max_poll_records as usize
        } else {
            usize::MAX
        };
        let span = self.poll_span();
        let result = self
            .poll_records(max, timeout)
            .instrument(span.clone())
            .await;
        crate::tracing_ext::record_poll_outcome(&span, result.as_ref().map(Vec::len));
        result
    }

    /// The `poll` span of one `poll`/`recv`, named after the subscription's
    /// topic when it has exactly one.
    fn poll_span(&self) -> tracing::Span {
        let span = crate::tracing_ext::poll_span(
            self.config.group_id.as_deref(),
            self.metrics_source.client_id(),
        );
        if !span.is_disabled() {
            let state = self.state.lock();
            crate::tracing_ext::record_destination(
                &span,
                "poll",
                state.subscription.iter().map(String::as_str),
            );
        }
        span
    }

    /// Poll for at most `max` records.
    async fn poll_records(&self, max: usize, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(KrafkaError::closed("consumer is closed"));
        }
        if self.wakeup_flag.swap(false, Ordering::AcqRel) {
            return Err(KrafkaError::Wakeup);
        }
        let _poll_timer = self.metrics.poll_latency.start();
        self.metrics.polls.inc();
        let deadline = Instant::now() + timeout;

        if self.maintain_group(timeout).await? {
            self.metrics.empty_polls.inc();
            return Ok(Vec::new());
        }

        loop {
            let records = self.deliver(max)?;
            if !records.is_empty() {
                return Ok(records);
            }

            self.update_positions(None).await?;
            self.validate_positions().await;

            let buffered = self.state.lock().buffered_count();
            let cap = self.config.max_buffered_records;
            if cap > 0 && buffered >= cap as usize {
                // Only records of paused partitions can be left buffered here.
                debug!(buffered, cap, "Buffer cap reached, skipping fetch");
                break;
            }
            // Decode one delivery's worth plus what the buffer has room for,
            // so the next poll is often served without a round trip and a
            // large response is not decoded only to be thrown away.
            let budget = (max != usize::MAX).then(|| {
                let headroom = if cap > 0 {
                    (cap as usize).saturating_sub(buffered)
                } else {
                    max
                };
                max.saturating_add(headroom)
            });
            let remaining = deadline.saturating_duration_since(Instant::now());
            let round = match self
                .fetch_round(self.config.fetch_max_wait.min(remaining), budget)
                .await
            {
                Err(KrafkaError::Wakeup) => {
                    self.wakeup_flag.store(false, Ordering::Release);
                    return Err(KrafkaError::Wakeup);
                }
                other => other?,
            };
            if !round.no_offset.is_empty() {
                return Err(KrafkaError::no_offset(round.no_offset));
            }
            if let Some(fault) = round.faults.into_iter().next() {
                self.metrics.record_error();
                return Err(fault.into_error(1));
            }
            if self.state.lock().has_deliverable() {
                continue;
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            if !round.requested {
                // Nothing was fetchable (paused, backing off, no leader):
                // wait a little rather than spin.
                let idle = self.config.idle_poll_backoff.max(Duration::from_millis(1));
                tokio::select! {
                    () = self.wakeup_notify.notified() => {
                        self.wakeup_flag.store(false, Ordering::Release);
                        return Err(KrafkaError::Wakeup);
                    }
                    () = tokio::time::sleep(idle.min(remaining)) => {}
                }
            }
        }
        self.metrics.empty_polls.inc();
        Ok(Vec::new())
    }

    /// Hand out up to `max` buffered records.
    ///
    /// The records leave the buffer inside a [`DeliveryGuard`]; deserializers
    /// run on copies of their keys and values; positions advance only in
    /// [`DeliveryGuard::finish`], after the last `.await`. A dropped future
    /// drops the guard, which puts the records back. A deserializer failure
    /// hands out the records before the failing one and puts the rest back,
    /// so the failure is reported again by the next call.
    fn deliver(&self, max: usize) -> Result<Vec<ConsumerRecord>> {
        let Some(delivery) = self.state.lock().take_delivery(max) else {
            return Ok(Vec::new());
        };
        let mut guard = DeliveryGuard::new(&self.state, delivery);
        let mut handed_out = guard.records_mut().len();
        let mut failure = None;

        if self.key_deserializer.is_some() || self.value_deserializer.is_some() {
            let mut decoded: Vec<(Option<Bytes>, Option<Bytes>)> = Vec::new();
            for record in guard.records_mut().iter() {
                match self.deserialize(record) {
                    Ok(pair) => decoded.push(pair),
                    Err(e) => {
                        failure = Some(e);
                        break;
                    }
                }
            }
            handed_out = decoded.len();
            for (record, (key, value)) in guard.records_mut().iter_mut().zip(decoded) {
                record.key = key;
                record.value = value;
            }
        }

        let records = guard.finish(handed_out);
        self.update_gauges();
        if let Some(error) = failure
            && records.is_empty()
        {
            self.metrics.record_error();
            return Err(error);
        }
        if !records.is_empty() {
            let bytes: u64 = records
                .iter()
                .map(|r| r.value.as_ref().map_or(0, |v| v.len() as u64))
                .sum();
            self.metrics.record_receive(records.len() as u64, bytes);
            crate::interceptor::safe_on_consume(&*self.interceptor, &records);
        }
        Ok(records)
    }

    /// Run the configured deserializers over one record's key and value.
    fn deserialize(&self, record: &ConsumerRecord) -> Result<(Option<Bytes>, Option<Bytes>)> {
        let decode =
            |decoder: &Arc<dyn crate::serdes::Deserializer>, payload: &Bytes, is_key: bool| {
                decoder
                    .deserialize(&record.topic, &record.headers, payload.clone(), is_key)
                    .map_err(|e| {
                        KrafkaError::record_deserialization(
                            &*record.topic,
                            record.partition,
                            record.offset,
                            if is_key { "key" } else { "value" },
                            e.to_string(),
                        )
                    })
            };
        let value = match (&self.value_deserializer, &record.value) {
            (Some(decoder), Some(value)) => Some(decode(decoder, value, false)?),
            (_, value) => value.clone(),
        };
        let key = match (&self.key_deserializer, &record.key) {
            (Some(decoder), Some(key)) => Some(decode(decoder, key, true)?),
            (_, key) => key.clone(),
        };
        Ok((key, value))
    }

    /// Receive the next record; `Ok(None)` once the consumer is closed.
    ///
    /// ```rust,no_run
    /// # async fn example(consumer: krafka::consumer::Consumer) -> krafka::Result<()> {
    /// while let Some(record) = consumer.recv().await? {
    ///     println!("{record:?}");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// A clean close — from this task or another — is `Ok(None)`, never an
    /// error.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. As for [`poll`](Self::poll): a dropped call
    /// loses no record and moves no position; the next call returns the record
    /// it would have.
    pub async fn recv(&self) -> Result<Option<ConsumerRecord>> {
        let span = self.poll_span();
        let result = self.recv_one().instrument(span.clone()).await;
        crate::tracing_ext::record_poll_outcome(
            &span,
            result.as_ref().map(|record| usize::from(record.is_some())),
        );
        result
    }

    async fn recv_one(&self) -> Result<Option<ConsumerRecord>> {
        loop {
            if self.closed.load(Ordering::SeqCst) {
                return Ok(None);
            }
            match self.poll_records(1, Duration::from_secs(1)).await {
                Ok(mut records) if !records.is_empty() => return Ok(Some(records.swap_remove(0))),
                Ok(_) => continue,
                Err(_) if self.closed.load(Ordering::SeqCst) => return Ok(None),
                Err(e) => return Err(e),
            }
        }
    }

    /// An async [`Stream`](futures_core::Stream) of records, driven by
    /// [`recv`](Self::recv). It ends when the consumer is closed.
    #[must_use = "stream does nothing unless polled"]
    pub fn stream(&self) -> ConsumerStream<'_> {
        ConsumerStream::new(self)
    }

    /// Commit the position of every assigned partition.
    ///
    /// Commits from one consumer leave one at a time in call order (manual
    /// and auto-commit alike), so a retried older commit never lands after a
    /// newer one. Committing does not move any position.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel safe. A dropped commit may or may not have
    /// been applied by the coordinator. Calling `commit` again is safe: it
    /// commits the positions as they are then.
    pub async fn commit(&self) -> Result<()> {
        self.committer().commit_positions().await
    }

    /// Commit the given offsets as given — offset, leader epoch and
    /// metadata — whether or not the partitions are still assigned; the
    /// coordinator decides. No position moves.
    ///
    /// ```rust,no_run
    /// use std::collections::HashMap;
    /// use krafka::consumer::{OffsetAndMetadata, TopicPartition};
    ///
    /// # async fn example(consumer: krafka::consumer::Consumer) -> krafka::Result<()> {
    /// let mut offsets = HashMap::new();
    /// offsets.insert(
    ///     TopicPartition::new("my-topic", 0),
    ///     OffsetAndMetadata::with_metadata(100, "checkpoint-abc123"),
    /// );
    /// consumer.commit_offsets(&offsets).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel safe. A dropped call may or may not have been
    /// applied by the coordinator. Calling it again with the same offsets is
    /// safe.
    pub async fn commit_offsets<P, O>(
        &self,
        offsets: impl IntoIterator<Item = (P, O)>,
    ) -> Result<()>
    where
        P: Borrow<TopicPartition>,
        O: Borrow<OffsetAndMetadata>,
    {
        let offsets: CommitRequestOffsets = offsets
            .into_iter()
            .map(|(tp, meta)| {
                let (tp, meta) = (tp.borrow(), meta.borrow());
                (
                    (tp.topic.clone(), tp.partition),
                    CommitPosition {
                        offset: meta.offset,
                        leader_epoch: meta.leader_epoch.unwrap_or(-1),
                        metadata: meta.metadata.clone(),
                    },
                )
            })
            .collect();
        self.committer().commit_offsets(offsets).await
    }

    /// The offset of the next record this partition will hand out — the
    /// offset a commit writes.
    ///
    /// A partition waiting for its committed offset or a reset is resolved
    /// first. `None` when the partition is not assigned or its position
    /// cannot be resolved yet.
    pub async fn position(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        let key = (topic.to_string(), partition);
        {
            let state = self.state.lock();
            let record = state.partition(&key)?;
            if record.position.is_some() {
                return record.position;
            }
        }
        if let Err(e) = self.update_positions(Some(&key)).await {
            debug!("resolving the position of {topic}-{partition} failed: {e}");
        }
        self.state.lock().partition(&key)?.position
    }

    /// The offset the next fetch for this partition starts at. Runs ahead of
    /// [`position`](Self::position) by the records buffered for it.
    pub async fn fetch_position(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        self.state
            .lock()
            .partition(&(topic.to_string(), partition))?
            .fetch_position()
    }

    /// A snapshot of the current assignment.
    pub async fn assignment(&self) -> StdHashMap<String, Vec<PartitionId>> {
        self.state.lock().assignment().into_iter().collect()
    }

    /// The subscribed (or manually assigned) topics.
    pub async fn subscription(&self) -> StdHashSet<String> {
        self.state.lock().subscription.iter().cloned().collect()
    }

    /// Position, cached watermarks and lag of every assigned partition. No
    /// request is sent: watermarks come from fetch responses, and a partition
    /// not fetched within
    /// [`lag_staleness_threshold`](ConsumerBuilder::lag_staleness_threshold)
    /// is marked [`stale`](PartitionLag::stale). For a live end offset use
    /// [`fetch_watermarks`](Self::fetch_watermarks).
    pub async fn lag(&self) -> StdHashMap<TopicPartition, PartitionLag> {
        self.state
            .lock()
            .lag_report(
                self.config.isolation_level,
                Instant::now(),
                self.config.lag_staleness_threshold,
            )
            .into_iter()
            .collect()
    }

    /// The cached readable end offset of a partition: the last stable offset
    /// under `read_committed`, else the high watermark.
    pub(crate) fn cached_end_offset(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        self.state
            .lock()
            .partition(&(topic.to_string(), partition))?
            .readable_end_offset(self.config.isolation_level)
    }

    /// Unsubscribe from all topics: commit (with auto-commit), report the
    /// partitions to `on_partitions_revoked`, leave the group, and clear all
    /// partition state.
    ///
    /// The leave-group error, if any, is returned after the state is cleared.
    pub async fn unsubscribe(&self) -> Result<()> {
        self.revoke_all().await;
        let leave_result = match self.group_coordinator {
            Some(ref coordinator) => {
                coordinator.set_subscription(Vec::new());
                coordinator.leave_group().await
            }
            None => Ok(()),
        };
        self.clear_state().await;
        debug!("Unsubscribed from all topics");
        leave_result
    }

    /// Pause fetching and delivery of assigned partitions. Buffered records
    /// stay buffered and positions do not move until resumed. Partitions
    /// that are not assigned are ignored.
    pub async fn pause(&self, topic: &str, partitions: &[PartitionId]) {
        let applied = self.state.lock().set_paused(topic, partitions, true);
        if applied < partitions.len() {
            warn!("pause({topic}, {partitions:?}): some partitions are not assigned");
        }
        self.update_gauges();
    }

    /// Resume paused partitions.
    pub async fn resume(&self, topic: &str, partitions: &[PartitionId]) {
        self.state.lock().set_paused(topic, partitions, false);
        self.update_gauges();
    }

    /// The paused partitions.
    pub async fn paused_partitions(&self) -> StdHashSet<(String, PartitionId)> {
        self.state.lock().paused().into_iter().collect()
    }

    /// The close-time commit error wins over the leave-group error, unless it
    /// only says the member already lost the group.
    fn select_close_result(commit_result: Result<()>, leave_result: Result<()>) -> Result<()> {
        match commit_result {
            Err(KrafkaError::Broker {
                code:
                    crate::error::ErrorCode::UnknownMemberId
                    | crate::error::ErrorCode::IllegalGeneration
                    | crate::error::ErrorCode::RebalanceInProgress
                    | crate::error::ErrorCode::FencedMemberEpoch
                    | crate::error::ErrorCode::StaleMemberEpoch,
                ..
            })
            | Ok(()) => leave_result,
            Err(error) => Err(error),
        }
    }

    /// Close the consumer: commit (with auto-commit), report the partitions
    /// still assigned to `on_partitions_revoked`, and leave the group as
    /// [`GroupMembershipOperation::Default`] says, within 30 s. A commit in flight is waited for. A `recv()` waiting in another
    /// task returns `Ok(None)`. Calling `close()` again is a no-op.
    ///
    /// The first cleanup error is returned.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel safe. Once polled, the consumer is closed even
    /// if the future is then dropped: `recv` returns `Ok(None)` and calling
    /// `close` again returns at once, but the final commit and leaving the
    /// group may not have happened, in which case the group rebalances when the
    /// member's session times out.
    pub async fn close(&self) -> Result<()> {
        self.close_with(CloseOptions::new()).await
    }

    /// [`close`](Self::close) within `options`' timeout (default 30 s), leaving
    /// or keeping the group membership as
    /// [`group_membership_operation`](CloseOptions::group_membership_operation)
    /// says (KIP-1092). On timeout the call returns [`KrafkaError::Timeout`]
    /// and the consumer stays closed.
    ///
    /// ```rust,no_run
    /// # async fn f(consumer: krafka::consumer::Consumer) -> krafka::Result<()> {
    /// use krafka::{CloseOptions, GroupMembershipOperation};
    ///
    /// // A quick redeploy: keep the membership so the group does not rebalance.
    /// consumer
    ///     .close_with(
    ///         CloseOptions::new()
    ///             .group_membership_operation(GroupMembershipOperation::RemainInGroup),
    ///     )
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn close_with(&self, options: CloseOptions) -> Result<()> {
        let timeout = options.timeout.unwrap_or(DEFAULT_CLOSE_TIMEOUT);
        tokio::time::timeout(
            timeout,
            self.close_inner(options.group_membership_operation),
        )
        .await
        .unwrap_or_else(|_| Err(KrafkaError::timeout("consumer close")))
    }

    async fn close_inner(&self, membership: GroupMembershipOperation) -> Result<()> {
        if self.closed.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let commit_result = if self.config.enable_auto_commit {
            self.commit().await
        } else {
            Ok(())
        };
        let assigned = self.state.lock().assigned_keys();
        if !assigned.is_empty() {
            let partitions: Vec<TopicPartition> = assigned
                .into_iter()
                .map(|(t, p)| TopicPartition::new(t, p))
                .collect();
            self.rebalance_listener
                .on_partitions_revoked_erased(&partitions)
                .await;
        }
        let leave_result = match self.group_coordinator {
            Some(ref coordinator) => coordinator.leave_group_with(membership).await,
            None => Ok(()),
        };
        self.clear_state().await;
        crate::interceptor::safe_consumer_close(&*self.interceptor);
        // Bounded by `close_with`'s timeout around this whole function.
        self.telemetry.close(DEFAULT_CLOSE_TIMEOUT).await;
        info!("consumer closed");
        Self::select_close_result(commit_result, leave_result)
    }

    /// Commit (with auto-commit) and revoke every assigned partition.
    async fn revoke_all(&self) {
        match self.group_coordinator.clone() {
            Some(coordinator) => self.revoke_all_in_group(&coordinator).await,
            None => {
                let assigned = self.state.lock().assigned_keys();
                if !assigned.is_empty() {
                    let partitions: Vec<TopicPartition> = assigned
                        .iter()
                        .map(|(t, p)| TopicPartition::new(t.clone(), *p))
                        .collect();
                    self.rebalance_listener
                        .on_partitions_revoked_erased(&partitions)
                        .await;
                    self.state.lock().remove_partitions(&assigned);
                }
            }
        }
    }

    /// Drop subscription, assignment, buffers and fetch sessions.
    async fn clear_state(&self) {
        {
            let mut state = self.state.lock();
            state.subscription.clear();
            state.standalone_topics.clear();
            state.clear_assignment();
        }
        self.close_fetch_sessions().await;
        self.update_gauges();
    }

    /// Whether the consumer is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// The group's committed offsets for the given partitions, read from the
    /// coordinator. A partition the group never committed is absent.
    ///
    /// # Errors
    ///
    /// [`KrafkaError::IllegalState`] without a `group_id`.
    pub async fn committed(
        &self,
        partitions: &[(&str, PartitionId)],
    ) -> Result<StdHashMap<(String, PartitionId), CommittedPosition>> {
        if self.is_closed() {
            return Err(KrafkaError::closed("consumer is closed"));
        }
        let Some(ref coordinator) = self.group_coordinator else {
            return Err(KrafkaError::illegal_state(
                "committed() requires a group_id; an assign-only consumer has no \
                 coordinator to read committed offsets from",
            ));
        };
        if partitions.is_empty() {
            return Ok(StdHashMap::new());
        }
        let mut by_topic: HashMap<String, Vec<PartitionId>> = HashMap::new();
        for (topic, partition) in partitions {
            validate_topic_name(topic)?;
            by_topic
                .entry((*topic).to_string())
                .or_default()
                .push(*partition);
        }
        Ok(coordinator
            .fetch_committed_offsets(&by_topic)
            .await?
            .into_iter()
            .collect())
    }

    /// Interrupt a [`poll`](Self::poll) / [`recv`](Self::recv) from another
    /// task: a parked poll returns [`KrafkaError::Wakeup`] at once, and one
    /// that has not started yet returns it immediately. The consumer stays
    /// usable.
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use std::time::Duration;
    /// # async fn f(
    /// #     consumer: Arc<krafka::consumer::Consumer>,
    /// #     shutdown: tokio::sync::oneshot::Receiver<()>,
    /// # ) {
    /// let c = Arc::clone(&consumer);
    /// tokio::spawn(async move {
    ///     let _ = shutdown.await;
    ///     c.wakeup();
    /// });
    ///
    /// while let Ok(records) = consumer.poll(Duration::from_secs(30)).await {
    ///     for record in records {
    ///         let _ = record;
    ///     }
    /// }
    /// # }
    /// ```
    #[inline]
    pub fn wakeup(&self) {
        self.wakeup_flag.store(true, Ordering::Release);
        self.wakeup_notify.notify_waiters();
    }

    /// This consumer's identity within its group, for
    /// [`TransactionalProducer::send_offsets`](crate::producer::TransactionalProducer::send_offsets). Re-read it for every transaction: the
    /// generation changes on every rebalance. `None` without a `group_id` or
    /// before the first join.
    pub async fn group_metadata(&self) -> Option<ConsumerGroupMetadata> {
        self.group_coordinator.as_ref()?.group_metadata()
    }

    /// This consumer's [`Metrics`]: its consumer counters and the
    /// connection counters of the pool it shares. An owned snapshot, read
    /// without blocking.
    pub fn metrics(&self) -> Metrics {
        self.metrics_source.snapshot()
    }

    /// The id the cluster assigned this consumer for KIP-714 telemetry,
    /// waiting at most `timeout` for it (Java `clientInstanceId`). `None`
    /// when the cluster does not support client telemetry.
    ///
    /// # Errors
    ///
    /// [`KrafkaError::IllegalState`] when
    /// [`metrics_push`](ConsumerBuilder::metrics_push) is off;
    /// [`KrafkaError::Timeout`] when no broker answered in time.
    pub async fn client_instance_id(&self, timeout: Duration) -> Result<Option<ClientInstanceId>> {
        self.telemetry.client_instance_id(timeout).await
    }

    /// Refresh the buffered, assigned, paused and lag gauges.
    fn update_gauges(&self) {
        let (buffered, assigned, paused, (lag, lag_max)) = {
            let state = self.state.lock();
            (
                state.buffered_count(),
                state.assigned_count(),
                state.paused().len(),
                state.aggregate_lag(self.config.isolation_level),
            )
        };
        self.metrics.buffered_records.set(buffered as u64);
        self.metrics.assigned_partitions.set(assigned as u64);
        self.metrics.paused_partitions.set(paused as u64);
        self.metrics.lag.set(lag);
        self.metrics.lag_max.set(lag_max);
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        if !self.closed.load(Ordering::SeqCst) && !std::thread::panicking() {
            warn!(
                "Consumer dropped without close(); group rebalance will be delayed \
                 until session.timeout.ms. Call `Consumer::close()` before drop."
            );
        }
    }
}

#[cfg(test)]
mod tests;
