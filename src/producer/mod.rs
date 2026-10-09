//! Kafka producer implementation.
//!
//! This module provides:
//! - [`Producer`]: batches records per partition and sends them through one
//!   engine task per producer, which coalesces the ready partitions of each
//!   broker into one Produce request
//! - [`TransactionalProducer`]: atomic writes across partitions
//! - [`TypedProducer`]: typed keys and values through a [`Serializer`](crate::serdes::Serializer)
//! - partitioning (KIP-794, KIP-1123), idempotence (KIP-360) and compression

mod accumulator;
mod batch;
mod config;
mod engine;
mod gate;
mod identity;
mod partitioner;
mod record;
mod retry;
mod transaction;
mod typed;

pub use accumulator::DeliveryHandle;
pub use config::Acks;
pub(crate) use config::ProducerConfig;
pub use partitioner::{Partitioner, RoundRobinPartitioner, murmur2};
pub use record::{DeliveryConfirmation, Record, RecordMetadata, UNKNOWN_PARTITION};
pub use transaction::{
    PreparedTxnState, TopicPartitionOffset, TransactionOutcome, TransactionState,
    TransactionVersion, TransactionalProducer,
};
pub use typed::TypedProducer;

use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use bytes::Bytes;
use tracing::{debug, info, warn};

use crate::PartitionId;
use crate::client::{CloseOptions, Kafka};
use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::metadata::ClusterMetadata;
use crate::metrics::{ClientInstanceId, Metrics, MetricsSource, ProducerRecorder};
use crate::protocol::{ApiKey, Compression, ProduceRequest, ProduceResponse, VersionedEncode};
use crate::telemetry::{ClientType, Telemetry};
use accumulator::Accumulator;
use gate::TxnGate;
use partitioner::Partitioning;
use record::TopicHandle;

use crate::barrier::InFlightBarrier;

/// Resolve the partition a record is routed to, fetching topic metadata when
/// the cache does not have it.
///
/// Shared by both producers' `enqueue`. Mirrors
/// `KafkaProducer.waitOnMetadata`: a cache miss fetches, bounded by `max_wait`
/// (what remains of `max_block`), and the partition — given with the record or
/// chosen by a custom partitioner — is range-checked so a partition the topic
/// does not have fails here rather than as an unroutable batch.
pub(crate) async fn resolve_partition(
    metadata: &ClusterMetadata,
    partitioning: &Partitioning,
    topic: &TopicHandle,
    key: Option<&[u8]>,
    requested: Option<PartitionId>,
    record_size: usize,
    max_wait: Duration,
) -> Result<PartitionId> {
    let partition_count = metadata.ensure_partition_count(topic, max_wait).await?;

    let (partition, source) = match requested {
        Some(partition) => (partition, "invalid partition given with record"),
        None => (
            partitioning.partition(metadata, topic, key, record_size, partition_count),
            "the partitioner returned partition",
        ),
    };
    if partition < 0 || partition as usize >= partition_count {
        return Err(KrafkaError::config(format!(
            "{source} {partition}, which is not in the range [0, {partition_count}) for topic \
             {topic}"
        )));
    }
    Ok(partition)
}

/// The `on_acknowledgement` a record owes once `on_send` has observed it.
///
/// `on_send` runs at the very top of the send path, before validation,
/// partitioning and the wait for buffer memory — every one of which can reject
/// the record and return early. The guard makes the obligation a value that has
/// to be spent: discharged by [`fail`](Self::fail), which fires the terminal
/// callback with the error, or by [`take_context`](Self::take_context), which
/// hands the context to the engine so the callback fires there.
///
/// A dropped obligation still reports. `send()` is an ordinary future, so a
/// caller may drop it — `tokio::time::timeout(d, producer.send(..))` is the
/// obvious way — and a record whose future was dropped before it was queued
/// will never be delivered. `Drop` therefore fires the terminal callback too.
/// Reaching `Drop` with no `.await` in flight is a krafka bug, and only that
/// case asserts.
pub(crate) struct SendObligation<'a> {
    interceptor: &'a dyn crate::interceptor::ProducerInterceptor,
    /// The topic as the interceptor chain left it, interned once and shared
    /// with the routing path.
    topic: TopicHandle,
    /// `None` once discharged.
    pub(crate) context: Option<crate::interceptor::RecordContext>,
    /// Set while the send future is parked on an `.await` this obligation
    /// wraps. It distinguishes a cancelled send from a code path in this crate
    /// that returned without reporting.
    suspended: bool,
    /// The record's `send` span; handed to the engine with the context.
    pub(crate) span: tracing::Span,
}

impl<'a> SendObligation<'a> {
    /// Open the obligation and run `on_send`.
    ///
    /// A panicking interceptor fails the send: the error is returned together
    /// with the discharged obligation's callback already fired.
    fn on_send(
        interceptor: &'a dyn crate::interceptor::ProducerInterceptor,
        record: &mut Record,
        client_id: &str,
    ) -> std::result::Result<Self, KrafkaError> {
        let mut context = crate::interceptor::RecordContext::new();
        let outcome = crate::interceptor::safe_on_send(interceptor, record, &mut context);
        let mut obligation = Self {
            interceptor,
            // Read after `on_send`, so an interceptor that rewrites the topic
            // is reported against the topic it chose.
            topic: TopicHandle::from(record.topic.as_str()),
            context: Some(context),
            suspended: false,
            span: crate::tracing_ext::send_span(
                &record.topic,
                client_id,
                record.key.as_deref(),
                record.value.is_none(),
            ),
        };
        match outcome {
            Ok(()) => Ok(obligation),
            Err(error) => Err(obligation.fail(UNKNOWN_PARTITION, &record.headers, error)),
        }
    }

    /// The interned topic, for the routing path to reuse.
    pub(crate) fn topic(&self) -> TopicHandle {
        TopicHandle::clone(&self.topic)
    }

    /// Await `fut` with the obligation marked as suspended.
    ///
    /// If the caller drops the `send()` future while it is parked here, `Drop`
    /// reports a cancellation instead of asserting — and still fires
    /// `on_acknowledgement`, because the record will never be delivered.
    pub(crate) async fn suspend<T>(&mut self, fut: impl std::future::Future<Output = T>) -> T {
        self.suspended = true;
        let output = fut.await;
        self.suspended = false;
        output
    }

    /// Discharge by reporting a terminal failure, returning `error` so call
    /// sites read `return Err(obligation.fail(..))`.
    ///
    /// Pass [`UNKNOWN_PARTITION`] when the record failed before it was routed.
    pub(crate) fn fail(
        &mut self,
        partition: PartitionId,
        headers: &crate::Headers,
        error: KrafkaError,
    ) -> KrafkaError {
        crate::tracing_ext::record_error(&self.span, &error);
        if let Some(mut context) = self.context.take() {
            crate::interceptor::safe_on_acknowledgement(
                self.interceptor,
                &self.topic,
                partition,
                Err(&error),
                headers,
                &mut context,
            );
        }
        error
    }

    /// Discharge by handing the context to the engine, which owes the
    /// terminal callback from here on.
    pub(crate) fn take_context(&mut self) -> crate::interceptor::RecordContext {
        self.context.take().unwrap_or_default()
    }
}

impl Drop for SendObligation<'_> {
    fn drop(&mut self) {
        // Unwinding already: firing a callback into a half-torn-down stack is
        // worse than the missed report.
        let (Some(mut context), false) = (self.context.take(), std::thread::panicking()) else {
            return;
        };

        let error = if self.suspended {
            // The caller dropped the `send()` future while it was parked — a
            // `timeout` elapsing, a `select!` branch losing. The record was
            // never queued and never will be, so the callback is owed here.
            KrafkaError::closed("the send was cancelled before the record was queued")
        } else {
            debug_assert!(
                false,
                "krafka bug: a record ran on_send but no on_acknowledgement was reported",
            );
            tracing::error!(
                "krafka bug: a record ran on_send but no on_acknowledgement was reported",
            );
            KrafkaError::illegal_state(
                "krafka bug: a record ran on_send but no on_acknowledgement was reported",
            )
        };
        crate::tracing_ext::record_error(&self.span, &error);

        crate::interceptor::safe_on_acknowledgement(
            self.interceptor,
            &self.topic,
            UNKNOWN_PARTITION,
            Err(&error),
            &Vec::new(),
            &mut context,
        );
    }
}

/// The send path both producers share: interceptors, validation, routing,
/// hand-off.
///
/// `admit` runs at the hand-off, synchronously, after the buffer memory is
/// reserved: the transactional producer passes its gate there so a send is
/// counted in the transaction exactly when it is queued. `register` runs after
/// routing and before the reservation, for TV1 partition registration.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn enqueue_record<R, F>(
    accumulator: &Accumulator,
    metadata: &ClusterMetadata,
    partitioning: &Partitioning,
    interceptor: &dyn crate::interceptor::ProducerInterceptor,
    client_id: &str,
    max_block: Duration,
    record: Record,
    register: R,
    admit: impl FnOnce() -> Result<Option<gate::TxnTicket>>,
) -> Result<DeliveryHandle>
where
    R: FnOnce(TopicHandle, PartitionId) -> F,
    F: std::future::Future<Output = Result<()>>,
{
    // `max_block` covers everything from here: metadata, registration and the
    // wait for buffer memory.
    let send_started_at = Instant::now();
    let mut record = record;
    let mut obligation = SendObligation::on_send(interceptor, &mut record, client_id)?;

    if let Err(error) = record.validate() {
        return Err(obligation.fail(UNKNOWN_PARTITION, &record.headers, error));
    }

    let record_size = record.estimated_size();
    let routed = record.into_routed_parts_with_topic(obligation.topic());
    let topic = routed.topic;
    let record = routed.record;

    let partition = match obligation
        .suspend(resolve_partition(
            metadata,
            partitioning,
            &topic,
            record.key_bytes(),
            routed.partition,
            record_size,
            max_block.saturating_sub(send_started_at.elapsed()),
        ))
        .await
    {
        Ok(partition) => partition,
        Err(error) => return Err(obligation.fail(UNKNOWN_PARTITION, &record.headers, error)),
    };
    obligation.span.record(
        "messaging.destination.partition.id",
        tracing::field::display(partition),
    );

    if let Err(error) = obligation
        .suspend(register(TopicHandle::clone(&topic), partition))
        .await
    {
        return Err(obligation.fail(partition, &record.headers, error));
    }

    match accumulator
        .enqueue(
            topic,
            record,
            record_size,
            partition,
            send_started_at,
            &mut obligation,
            admit,
        )
        .await
    {
        Ok(handle) => Ok(handle),
        // The context comes back rather than being dropped, so the obligation
        // is re-opened and discharged with the partition already chosen.
        Err(rejected) => {
            obligation.context = Some(rejected.context);
            Err(obligation.fail(partition, &rejected.record.headers, rejected.error))
        }
    }
}

/// A Kafka producer.
///
/// Built with [`Kafka::producer`]. `Send + Sync`: share it across tasks with
/// an `Arc`.
pub struct Producer {
    /// The handle this producer was built from: pool, metadata, client id.
    kafka: Kafka,
    /// Producer configuration.
    config: ProducerConfig,
    /// How keyless and keyed records choose a partition.
    partitioning: Arc<Partitioning>,
    /// The send engine. Every send goes through it.
    accumulator: Accumulator,
    /// Generation-counted sends, for `flush` and `close`.
    barrier: Arc<InFlightBarrier>,
    /// Where `metrics()` and the KIP-714 reporter read from.
    metrics_source: Arc<MetricsSource>,
    /// The KIP-714 reporter.
    telemetry: Telemetry,
    /// Producer interceptor.
    interceptor: Arc<dyn crate::interceptor::ProducerInterceptor>,
}

/// Fold a leader named in a produce response into the metadata cache (KIP-951).
///
/// A broker that rejects a produce with `NOT_LEADER_OR_FOLLOWER` /
/// `FENCED_LEADER_EPOCH` also reports which node now leads the partition, and
/// advertises that node's endpoint. Applying it lets the retry go straight to
/// the new leader without a metadata round trip.
///
/// Returns `true` when the cache changed. Any other error code is left alone.
fn apply_produce_leader_hint(
    metadata: &ClusterMetadata,
    topic: &str,
    partition: PartitionId,
    response: &ProduceResponse,
    partition_response: &crate::protocol::ProducePartitionResponse,
) -> bool {
    use crate::error::ErrorCode;
    if !matches!(
        partition_response.error_code,
        ErrorCode::NotLeaderForPartition | ErrorCode::FencedLeaderEpoch
    ) {
        return false;
    }
    let Some(leader) = partition_response.current_leader else {
        return false;
    };

    let applied = metadata.apply_leader_hint(
        topic,
        partition,
        leader.leader_id,
        leader.leader_epoch,
        crate::metadata::broker_info_for_node(&response.node_endpoints, leader.leader_id),
    );
    if applied {
        debug!(
            topic,
            partition,
            leader_id = leader.leader_id,
            leader_epoch = leader.leader_epoch,
            "broker named a new leader; retrying there without a metadata refresh (KIP-951)"
        );
    }
    applied
}

fn request_header_size(api_key: ApiKey, api_version: i16, client_id: &str) -> Result<usize> {
    // 2 (api_key) + 2 (api_version) + 4 (correlation_id) + 2+len (client_id standard string)
    if client_id.len() > i16::MAX as usize {
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::InvalidLength,
            format!(
                "client_id length {} exceeds protocol limit of {}",
                client_id.len(),
                i16::MAX
            ),
        ));
    }
    let base = 2 + 2 + 4 + 2 + client_id.len();
    match crate::protocol::RequestHeader::header_version(api_key, api_version) {
        1 => Ok(base),
        2 => Ok(base + 1), // +1 for empty tagged-fields byte
        version => Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::UnknownApiVersion,
            format!("unsupported request header version {version}"),
        )),
    }
}

/// Encode the request body, validate the total wire frame size, and return
/// the encoded body bytes.
///
/// The single source of truth for produce frame sizing: it uses the real
/// encoder rather than a separate size computation.
fn encode_and_validate_produce_request(
    client_id: &str,
    max_request_size: usize,
    api_version: i16,
    request: &ProduceRequest,
) -> Result<Bytes> {
    let mut body = bytes::BytesMut::new();
    request.encode_versioned(api_version, &mut body)?;
    let frame_size = 4 + request_header_size(ApiKey::Produce, api_version, client_id)? + body.len();
    if frame_size > max_request_size {
        // `FrameTooLarge`, not `InvalidLength`: the split-and-resend recovery
        // keys off this kind, and it must never be triggered by a response
        // that merely failed to decode.
        return Err(KrafkaError::protocol_kind(
            ProtocolErrorKind::FrameTooLarge,
            format!(
                "produce request size {frame_size} exceeds max_request_size {max_request_size}"
            ),
        ));
    }
    Ok(body.freeze())
}

/// Populate `topic_id` fields for Produce v13+ (KIP-516).
///
/// Returns `true` if **all** topic IDs were resolved; `false` means the caller
/// should cap the wire version to v12 and send topic names instead.
pub(crate) fn fill_produce_topic_ids(
    request: &mut ProduceRequest,
    metadata: &ClusterMetadata,
) -> bool {
    let mut all_resolved = true;
    for topic_data in &mut request.topic_data {
        if topic_data.topic_id.is_none() {
            if let Some(id) = metadata.topic_id_for_name(&topic_data.name) {
                topic_data.topic_id = Some(id);
            } else {
                all_resolved = false;
            }
        }
    }
    all_resolved
}

/// Hand-written: several fields are trait objects.
impl std::fmt::Debug for Producer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("client_id", &self.kafka.client_id())
            .field("idempotent", &self.config.idempotent)
            .finish_non_exhaustive()
    }
}

/// Collapse the builder's interceptor list into one interceptor.
pub(crate) fn interceptor_chain(
    mut interceptors: Vec<Arc<dyn crate::interceptor::ProducerInterceptor>>,
) -> Arc<dyn crate::interceptor::ProducerInterceptor> {
    match interceptors.len() {
        0 => Arc::new(crate::interceptor::NoOpProducerInterceptor),
        1 => interceptors
            .pop()
            .unwrap_or_else(|| Arc::new(crate::interceptor::NoOpProducerInterceptor)),
        _ => Arc::new(crate::interceptor::ProducerInterceptorChain::new(
            interceptors,
        )),
    }
}

/// The transactional half of a producer's engine: its id and its gate.
pub(crate) struct Transactional {
    pub(crate) transactional_id: String,
    pub(crate) gate: Arc<TxnGate>,
}

impl Producer {
    /// Create a producer from validated settings.
    async fn new(
        kafka: Kafka,
        config: ProducerConfig,
        interceptor: Arc<dyn crate::interceptor::ProducerInterceptor>,
        partitioner: Option<Arc<dyn Partitioner>>,
        transactional: Option<Transactional>,
    ) -> Result<Self> {
        let backoff = retry::Backoff::new(config.retry_backoff);

        // An idempotent producer starts with a producer id; later epochs are
        // local bumps (KIP-360). A transactional producer gets its id from
        // the coordinator instead.
        let identity = if config.idempotent && transactional.is_none() {
            let deadline = tokio::time::Instant::now() + config.delivery_timeout;
            Some(engine::init_producer_id(kafka.metadata(), &backoff, deadline).await?)
        } else {
            None
        };

        let partitioning = Arc::new(Partitioning::new(
            partitioner,
            config.batch_size,
            config
                .partitioner_rack_aware
                .then(|| config.client_rack.clone())
                .flatten(),
        ));

        let metrics = Arc::new(ProducerRecorder::default());
        let metrics_source = MetricsSource::producer(&kafka, Arc::clone(&metrics));
        let barrier = Arc::new(InFlightBarrier::new());
        let (transactional_id, gate, mode) = match transactional {
            Some(txn) => (
                Some(txn.transactional_id),
                Some(txn.gate),
                identity::Mode::Transactional { tv2: false },
            ),
            None if config.idempotent => (None, None, identity::Mode::Idempotent),
            None => (None, None, identity::Mode::Plain),
        };

        let accumulator = Accumulator::spawn(
            engine::EngineConfig {
                batch_size: config.batch_size,
                linger: config.linger,
                delivery_timeout: config.delivery_timeout,
                request_timeout: kafka.request_timeout(),
                max_request_size: config.max_request_size,
                acks: config.acks.to_i16(),
                compression: config.compression,
                compression_level: config.compression_level,
                topic_compression: config.topic_compression.clone().into_iter().collect(),
                client_id: kafka.client_id().to_string(),
                transactional_id,
                backoff,
                interceptor: Arc::clone(&interceptor),
                mode,
                identity,
                gate,
            },
            config.buffer_memory,
            config.max_block,
            Arc::clone(kafka.metadata()),
            Arc::clone(&metrics),
            Arc::clone(&barrier),
        );

        info!(client_id = kafka.client_id(), "producer started");
        let telemetry = Telemetry::start(
            config.metrics_push,
            &kafka,
            ClientType::Producer,
            Arc::clone(&metrics_source),
        );

        Ok(Self {
            kafka,
            config,
            partitioning,
            accumulator,
            barrier,
            metrics_source,
            telemetry,
            interceptor,
        })
    }

    /// Send a record and wait for the broker to acknowledge it:
    /// `enqueue(record).await?.await`.
    ///
    /// ```rust,no_run
    /// # async fn example(producer: &krafka::producer::Producer) -> krafka::Result<()> {
    /// use krafka::Record;
    ///
    /// let metadata = producer.send(Record::new("orders", "hello").key("k")).await?;
    /// println!("partition {} offset {}", metadata.partition, metadata.offset);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel safe. Dropped before the record is queued,
    /// nothing is sent and nothing stays reserved. Dropped after, the record is
    /// still delivered and only its acknowledgement is lost, so calling `send`
    /// again may write it twice. To bound the wait without that risk,
    /// [`enqueue`](Self::enqueue) the record and put the timeout around
    /// awaiting its [`DeliveryHandle`].
    pub async fn send(&self, record: Record) -> Result<RecordMetadata> {
        self.enqueue(record).await?.await
    }

    /// Queue a record and return as soon as it is **queued**; the returned
    /// [`DeliveryHandle`] resolves to the broker's answer.
    ///
    /// # Ordering
    ///
    /// **Produce order is enqueue order.** If `enqueue(a)` returns before
    /// `enqueue(b)` is called, `a` reaches its partition before `b` — whatever
    /// order the two handles are polled in, and whether or not they are polled
    /// at all.
    ///
    /// ```rust,no_run
    /// use futures::stream::{FuturesUnordered, StreamExt};
    /// use krafka::Record;
    ///
    /// # async fn example(producer: &krafka::producer::Producer) -> krafka::Result<()> {
    /// let mut acks = FuturesUnordered::new();
    /// for i in 0..1000u32 {
    ///     acks.push(producer.enqueue(Record::new("events", i.to_be_bytes().to_vec())).await?);
    /// }
    /// while let Some(result) = acks.next().await {
    ///     result?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. The record is queued at one synchronous
    /// point after its buffer memory is reserved, and the wait for that memory
    /// is the only `.await` before it: a dropped call queued nothing and holds
    /// nothing, and a call that returned a handle queued the record exactly
    /// once. The interceptor's `on_send` runs before the wait; a dropped call
    /// reports the record to `on_acknowledgement` as failed.
    ///
    /// # Errors
    ///
    /// The outer `Result` covers everything up to and including the enqueue:
    /// interceptors, record validation, topic resolution, and the wait for
    /// buffer memory, all within `max_block`. The handle covers delivery,
    /// within `delivery_timeout`.
    pub async fn enqueue(&self, record: Record) -> Result<DeliveryHandle> {
        enqueue_record(
            &self.accumulator,
            self.kafka.metadata(),
            &self.partitioning,
            &*self.interceptor,
            self.kafka.client_id(),
            self.config.max_block,
            record,
            |_, _| async { Ok(()) },
            || Ok(None),
        )
        .await
    }

    /// Partition metadata for `topic`, fetching it if the cache does not have
    /// it (Java `partitionsFor`), in ascending partition order. Bounded by
    /// [`max_block`](ProducerBuilder::max_block).
    ///
    /// # Errors
    ///
    /// Fails when the topic does not exist or its metadata cannot be fetched
    /// in time.
    pub async fn partitions_for(&self, topic: &str) -> Result<Vec<crate::PartitionInfo>> {
        let metadata = self.kafka.metadata();
        metadata
            .ensure_partition_count(topic, self.config.max_block)
            .await?;
        let mut partitions: Vec<_> = metadata
            .topic_arc(topic)
            .map(|info| info.partitions_iter().cloned().collect())
            .unwrap_or_default();
        partitions.sort_by_key(|p| p.partition);
        Ok(partitions)
    }

    /// Send every record queued before this call and wait for their outcomes.
    ///
    /// Covers exactly the sends that were queued when `flush` was called: a
    /// send queued afterwards neither holds it up nor, by completing, ends it
    /// early. Buffered batches go out at once instead of waiting for `linger`.
    ///
    /// # Cancel safety
    ///
    /// This method is cancel safe. Dropping it stops the wait, not the sends:
    /// the records it covered are still delivered, and a later `flush` or
    /// `close` waits for them.
    pub async fn flush(&self) -> Result<()> {
        let generation = self.barrier.snapshot();
        self.accumulator.flush(generation);
        self.barrier.wait_for(generation).await;
        Ok(())
    }

    /// Close the producer: refuse new sends, send every queued record, wait
    /// for their outcomes and close the interceptors. No time bound; see
    /// [`close_with`](Self::close_with). Calling it again is a no-op.
    ///
    /// Dropping a producer without `close()` still sends what is buffered, in
    /// the background, but nothing waits for it.
    ///
    /// # Cancel safety
    ///
    /// This method is not cancel safe. Once polled, the producer is closed even
    /// if the future is then dropped: new sends are refused and the queued
    /// records are still delivered in the background, but nothing waits for
    /// them, and calling `close` again returns at once. To bound the wait, use
    /// [`close_with`](Self::close_with) with a timeout instead of dropping the
    /// future.
    pub async fn close(&self) -> Result<()> {
        self.close_with(CloseOptions::new()).await
    }

    /// Close the producer within `options`' timeout, if any. Records still
    /// without an outcome when it expires fail with
    /// [`KrafkaError::Closed`], and the call returns
    /// [`KrafkaError::Timeout`].
    pub async fn close_with(&self, options: CloseOptions) -> Result<()> {
        let Some(generation) = self.barrier.begin_close() else {
            return Ok(());
        };
        self.finish_close(generation, options.timeout).await
    }

    /// The second half of a close whose generation `begin_close` returned:
    /// drain, then close the interceptors.
    async fn finish_close(&self, generation: u64, timeout: Option<Duration>) -> Result<()> {
        let started = Instant::now();
        self.accumulator.flush(generation);
        let drained = self.barrier.wait_for(generation);
        let result = match timeout {
            Some(timeout) => tokio::time::timeout(timeout, drained).await.map_err(|_| {
                warn!("producer close timed out; failing the records still queued");
                self.accumulator
                    .terminate(KrafkaError::closed("the producer was closed"));
                KrafkaError::timeout("producer close")
            }),
            None => {
                drained.await;
                Ok(())
            }
        };
        crate::interceptor::safe_producer_close(&*self.interceptor);
        // The terminating push carries the final counters, so it goes last.
        let telemetry_budget = timeout.map_or(self.kafka.request_timeout(), |t| {
            t.saturating_sub(started.elapsed())
        });
        self.telemetry.close(telemetry_budget).await;
        info!("producer closed");
        result
    }

    /// Whether [`close`](Self::close) was called.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.barrier.is_closing()
    }

    /// This producer's [`Metrics`]: its producer counters and the
    /// connection counters of the pool it shares. An owned snapshot, read
    /// without blocking.
    pub fn metrics(&self) -> Metrics {
        self.metrics_source.snapshot()
    }

    /// The id the cluster assigned this producer for KIP-714 telemetry,
    /// waiting at most `timeout` for it (Java `clientInstanceId`). `None`
    /// when the cluster does not support client telemetry.
    ///
    /// # Errors
    ///
    /// [`KrafkaError::IllegalState`] when
    /// [`metrics_push`](ProducerBuilder::metrics_push) is off;
    /// [`KrafkaError::Timeout`] when no broker answered in time.
    pub async fn client_instance_id(&self, timeout: Duration) -> Result<Option<ClientInstanceId>> {
        self.telemetry.client_instance_id(timeout).await
    }
}

impl Drop for Producer {
    /// A producer dropped without [`close()`](Producer::close) still sends what
    /// is buffered: the engine keeps running until every queued batch has an
    /// outcome. Nothing waits for it, and it races the runtime's shutdown.
    fn drop(&mut self) {
        if !self.barrier.is_closing() && !std::thread::panicking() {
            warn!(
                "Producer dropped without close(); buffered records are still being sent in \
                 the background with nobody waiting for them. Call `close()` before drop."
            );
            crate::interceptor::safe_producer_close(&*self.interceptor);
        }
    }
}

/// Builder for a [`Producer`] or a [`TransactionalProducer`]: producer
/// settings only. Obtain with [`Kafka::producer`].
#[must_use = "builders do nothing until .build() is called"]
pub struct ProducerBuilder {
    kafka: Kafka,
    config: ProducerConfig,
    interceptors: Vec<Arc<dyn crate::interceptor::ProducerInterceptor>>,
    partitioner: Option<Arc<dyn Partitioner>>,
}

impl std::fmt::Debug for ProducerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl ProducerBuilder {
    pub(crate) fn new(kafka: Kafka) -> Self {
        Self {
            kafka,
            config: ProducerConfig::default(),
            interceptors: Vec::new(),
            partitioner: None,
        }
    }

    /// Set the required acknowledgments. Default: [`Acks::All`].
    pub fn acks(mut self, acks: Acks) -> Self {
        self.config.acks = acks;
        self
    }

    /// Set the compression codec. Default: none.
    ///
    /// [`Compression::Zstd`] needs the `zstd` Cargo feature to *encode*;
    /// `build()` rejects it without. Every codec decodes in every build.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.config.compression = compression;
        self
    }

    /// Override the compression codec's default level.
    ///
    /// `None` (the default) uses the codec's own default: zlib 6 for `Gzip`,
    /// 3 for `Zstd`. Only `Gzip` and `Zstd` take a level; setting one
    /// alongside `Snappy` or `Lz4` (or a per-topic override using them) is
    /// rejected by `build()`. Zstd levels above roughly 9 cost CPU far faster
    /// than they save bytes.
    pub fn compression_level(mut self, level: Option<i32>) -> Self {
        self.config.compression_level = level;
        self
    }

    /// Override the compression codec for one topic.
    pub fn topic_compression(mut self, topic: impl Into<String>, compression: Compression) -> Self {
        self.config
            .topic_compression
            .insert(topic.into(), compression);
        self
    }

    /// Bytes per batch, and how many bytes of keyless records stick to one
    /// partition before the built-in partitioner moves on (KIP-794).
    /// Default: 16 KiB.
    pub fn batch_size(mut self, bytes: usize) -> Self {
        self.config.batch_size = bytes;
        self
    }

    /// How long a batch may wait for more records. Default: 5 ms.
    pub fn linger(mut self, linger: Duration) -> Self {
        self.config.linger = linger;
        self
    }

    /// Total time a record may spend queued and retried before it fails
    /// (`delivery.timeout.ms`); the only bound on retries. Must be at least
    /// `linger` plus the handle's `request_timeout`. Default: 120 s.
    pub fn delivery_timeout(mut self, timeout: Duration) -> Self {
        self.config.delivery_timeout = timeout;
        self
    }

    /// First retry delay; it doubles per retry up to 1 s, with jitter.
    /// Default: 100 ms.
    pub fn retry_backoff(mut self, backoff: Duration) -> Self {
        self.config.retry_backoff = backoff;
        self
    }

    /// Largest encoded Produce request, in bytes. Default: 100 MiB.
    pub fn max_request_size(mut self, bytes: usize) -> Self {
        self.config.max_request_size = bytes;
        self
    }

    /// Idempotent production (KIP-679): the broker de-duplicates retries
    /// within this producer's session. Default: on; requires
    /// [`Acks::All`].
    ///
    /// Idempotence does not fence a second instance of the application; a
    /// [`TransactionalProducer`] with a stable transactional id does.
    ///
    /// On a cluster that cannot give the producer an id (no
    /// `InitProducerId`, or it is refused), `build` fails with an error
    /// naming this setting; turn it off there.
    pub fn idempotent(mut self, enable: bool) -> Self {
        self.config.idempotent = enable;
        self
    }

    /// One budget for everything `send()`/`enqueue()` may block on — topic
    /// metadata and buffer memory — and for each transaction coordinator call
    /// (`max.block.ms`). Default: 60 s.
    pub fn max_block(mut self, duration: Duration) -> Self {
        self.config.max_block = duration;
        self
    }

    /// Bytes the producer may hold for unsent records before `send()` waits
    /// (`buffer.memory`). Default: 32 MiB.
    pub fn buffer_memory(mut self, bytes: usize) -> Self {
        self.config.buffer_memory = bytes;
        self
    }

    /// The rack this producer runs in (Java `client.rack`), read by
    /// [`partitioner_rack_aware`](Self::partitioner_rack_aware).
    pub fn client_rack(mut self, rack: impl Into<String>) -> Self {
        self.config.client_rack = Some(rack.into());
        self
    }

    /// Send keyless records only to partitions led from
    /// [`client_rack`](Self::client_rack) (KIP-1123). Default: off. Keyed
    /// records always follow their key's hash; refused with a custom
    /// [`partitioner`](Self::partitioner).
    pub fn partitioner_rack_aware(mut self, enable: bool) -> Self {
        self.config.partitioner_rack_aware = enable;
        self
    }

    /// Choose partitions with `partitioner` instead of the built-in one
    /// (murmur2 for keyed records, sticky batches for keyless ones). A
    /// partition outside `[0, partition_count)` fails the send with
    /// [`KrafkaError::Config`].
    pub fn partitioner(mut self, partitioner: impl Partitioner + 'static) -> Self {
        self.partitioner = Some(Arc::new(partitioner));
        self
    }

    /// Append an interceptor to the chain. Interceptors run in the order they
    /// were added, each panic-isolated; `on_send` sees the record as the
    /// previous interceptor left it.
    pub fn interceptor(
        mut self,
        interceptor: impl crate::interceptor::ProducerInterceptor + 'static,
    ) -> Self {
        self.interceptors.push(Arc::new(interceptor));
        self
    }

    /// How long the coordinator lets a transaction stay open before it aborts
    /// it (`transaction.timeout.ms`). Default: 60 s.
    /// [`build_transactional`](Self::build_transactional) only; contradicts
    /// [`two_phase_commit`](Self::two_phase_commit).
    pub fn transaction_timeout(mut self, timeout: Duration) -> Self {
        self.config.transaction_timeout = Some(timeout);
        self
    }

    /// Take part in an external two-phase commit (KIP-939): the coordinator
    /// holds a prepared transaction until
    /// [`complete`](TransactionalProducer::complete) or a commit/abort, with
    /// no timeout. [`build_transactional`](Self::build_transactional) only;
    /// needs `transaction.version` 3 on the broker (`InitProducerId` v6,
    /// hence the `unstable-protocol` feature) and the `TWO_PHASE_COMMIT` ACL.
    pub fn two_phase_commit(mut self, enable: bool) -> Self {
        self.config.two_phase_commit = enable;
        self
    }

    /// Push this producer's metrics to the brokers when a cluster operator
    /// subscribes to them (KIP-714, Java `enable.metrics.push`). Default: on.
    /// Nothing is sent to a cluster without a client-telemetry plugin.
    pub fn metrics_push(mut self, enable: bool) -> Self {
        self.config.metrics_push = enable;
        self
    }

    fn validate(&self, transactional: bool) -> Result<()> {
        config::validate(&self.config, self.kafka.request_timeout(), transactional)?;
        config::validate_partitioning(
            self.config.partitioner_rack_aware,
            self.config.client_rack.as_deref(),
            self.partitioner.is_some(),
        )
    }

    /// The validated settings, without starting a producer.
    #[cfg(test)]
    pub(crate) fn build_config(self) -> Result<ProducerConfig> {
        self.validate(false)?;
        Ok(self.config)
    }

    /// Build the producer. An idempotent producer fetches its producer id
    /// here.
    ///
    /// # Errors
    ///
    /// [`KrafkaError::Config`] naming the setting for an invalid
    /// configuration; a broker error if the producer id cannot be obtained.
    pub async fn build(self) -> Result<Producer> {
        self.validate(false)?;
        let interceptor = interceptor_chain(self.interceptors);
        Producer::new(self.kafka, self.config, interceptor, self.partitioner, None).await
    }

    /// Build a [`TransactionalProducer`] for `transactional_id`, and
    /// initialise it with the transaction coordinator (`InitProducerId`),
    /// which fences any earlier instance with the same id and aborts the
    /// transaction it left open.
    ///
    /// With [`two_phase_commit`](Self::two_phase_commit), a transaction an
    /// earlier instance left prepared is kept instead; see
    /// [`TransactionalProducer::prepared_transaction`].
    ///
    /// # Errors
    ///
    /// [`KrafkaError::Config`] for an invalid configuration (an empty
    /// `transactional_id`, …); [`KrafkaError::Fenced`] or
    /// [`KrafkaError::Auth`] when the coordinator refuses the id.
    pub async fn build_transactional(
        self,
        transactional_id: impl Into<String>,
    ) -> Result<TransactionalProducer> {
        let transactional_id = transactional_id.into();
        if transactional_id.is_empty() {
            return Err(KrafkaError::config("transactional_id must not be empty"));
        }
        if transactional_id.len() > i16::MAX as usize {
            return Err(KrafkaError::config(format!(
                "transactional_id is {} bytes, exceeding the Kafka wire limit of {}",
                transactional_id.len(),
                i16::MAX
            )));
        }
        self.validate(true)?;
        let gate = Arc::new(TxnGate::new());
        let interceptor = interceptor_chain(self.interceptors);
        let producer = Producer::new(
            self.kafka,
            self.config,
            interceptor,
            self.partitioner,
            Some(Transactional {
                transactional_id: transactional_id.clone(),
                gate: Arc::clone(&gate),
            }),
        )
        .await?;
        TransactionalProducer::start(producer, transactional_id, gate).await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use crate::protocol::{ProducePartitionData, ProduceTopicData, versions};

    #[test]
    fn test_validate_produce_request_size_rejects_oversized_frame() {
        let request = ProduceRequest {
            transactional_id: None,
            acks: Acks::All.to_i16(),
            timeout_ms: 30_000,
            topic_data: vec![ProduceTopicData {
                name: "topic".to_string(),
                topic_id: None,
                partition_data: vec![ProducePartitionData {
                    index: 0,
                    records: Bytes::from(vec![0; 512]),
                }],
            }],
        };

        let error =
            encode_and_validate_produce_request("client", 128, versions::PRODUCE_MIN, &request)
                .expect_err("oversized frame should be rejected");

        assert!(error.to_string().contains("max_request_size"));
    }

    #[test]
    fn test_validate_produce_request_size_uses_exact_flexible_encoding_size() {
        let request = ProduceRequest {
            transactional_id: Some("txn-123".to_string()),
            acks: Acks::All.to_i16(),
            timeout_ms: 30_000,
            topic_data: vec![ProduceTopicData {
                name: "topic".to_string(),
                // PRODUCE_MAX is v13 which requires topic_id on the wire.
                topic_id: Some([0u8; 16]),
                partition_data: vec![ProducePartitionData {
                    index: 0,
                    records: Bytes::from(vec![1; 32]),
                }],
            }],
        };

        // Encode with a permissive limit to recover the actual frame size.
        let encoded = encode_and_validate_produce_request(
            "client",
            usize::MAX,
            versions::PRODUCE_MAX,
            &request,
        )
        .unwrap();
        let exact_size = 4
            + request_header_size(ApiKey::Produce, versions::PRODUCE_MAX, "client").unwrap()
            + encoded.len();

        encode_and_validate_produce_request("client", exact_size, versions::PRODUCE_MAX, &request)
            .unwrap();

        let error = encode_and_validate_produce_request(
            "client",
            exact_size.saturating_sub(1),
            versions::PRODUCE_MAX,
            &request,
        )
        .unwrap_err();

        assert!(error.to_string().contains("max_request_size"));
    }

    #[test]
    fn test_validate_produce_request_size_v13_requires_topic_id() {
        let request = ProduceRequest {
            transactional_id: None,
            acks: Acks::All.to_i16(),
            timeout_ms: 30_000,
            topic_data: vec![ProduceTopicData {
                name: "topic".to_string(),
                topic_id: None,
                partition_data: vec![ProducePartitionData {
                    index: 0,
                    records: Bytes::from_static(b"payload"),
                }],
            }],
        };

        let error = encode_and_validate_produce_request("client", 1024, 13, &request).unwrap_err();
        assert!(error.to_string().contains("topic_id is required"));
    }

    /// A record larger than `buffer_memory` is rejected before it can block.
    ///
    /// It could never be admitted — the byte-granular permit pool never
    /// accumulates that many permits — so blocking for `max_block` and then
    /// timing out would report the wrong cause. Admission is checked up front
    /// on the one send path, in `check_record_admission`.
    #[test]
    fn a_record_larger_than_buffer_memory_is_rejected_up_front() {
        let err = accumulator::check_record_admission(1024, 16, usize::MAX)
            .expect_err("a record larger than buffer_memory must be rejected");
        assert!(
            err.to_string().contains("buffer_memory"),
            "the error must name the setting to raise, got: {err}"
        );
    }

    #[test]
    fn test_producer_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Producer>();
    }

    /// `TransactionVersion` must be nameable by callers: it is the return type
    /// of the public `TransactionalProducer::transaction_version()`, and a type
    /// that cannot be written down cannot be matched on or stored.
    #[test]
    fn test_transaction_version_is_publicly_nameable() {
        let version: crate::producer::TransactionVersion = TransactionVersion::V2;
        assert_eq!(version, TransactionVersion::V2);
    }

    #[tokio::test]
    async fn interceptors_append_in_order() {
        use crate::interceptor::ProducerInterceptor;

        #[derive(Debug)]
        struct A;
        impl ProducerInterceptor for A {}

        let builder = crate::Kafka::detached()
            .producer()
            .interceptor(A)
            .interceptor(Arc::new(A));
        assert_eq!(builder.interceptors.len(), 2);
    }
}
