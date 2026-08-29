//! Kafka producer implementation.
//!
//! This module provides:
//! - Async producer for sending messages
//! - Batching for performance with linger timer
//! - Compression support
//! - Partitioning strategies
//! - Retry handling with exponential backoff
//! - Idempotent production for exactly-once semantics
//! - Transactional production for exactly-once delivery

mod accumulator;
mod config;
mod idempotent;
mod partitioner;
mod record;
mod retry;
mod transaction;

pub use accumulator::{
    AccumulatorConfig, DeliveryHandle, RecordAccumulator, RecordAccumulatorHandle,
};
pub use config::{Acks, ProducerConfig};
pub use idempotent::{
    PartitionSequenceSnapshot, ProducerIdentity, ProducerIdentitySnapshot, ProducerStateStore,
    RollbackOutcome,
};
pub use partitioner::{
    DefaultPartitioner, HashPartitioner, Partitioner, RoundRobinPartitioner, StickyPartitioner,
    UniformStickyPartitioner, murmur2,
};
pub use record::{
    DeliveryConfirmation, NO_TIMESTAMP, ProducerRecord, RecordHeaders, RecordMetadata,
    UNKNOWN_PARTITION,
};
pub use retry::{RetryContext, RetryPolicy};
pub use transaction::{
    PreparedTxnState, TopicPartitionOffset, TransactionOutcome, TransactionState,
    TransactionVersion, TransactionalProducer, TransactionalProducerBuilder,
    TransactionalProducerConfig,
};

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tracing::{debug, info, warn};

use crate::PartitionId;
use crate::auth::AuthConfig;
use crate::error::{ErrorCode, KrafkaError, ProtocolErrorKind, Result};
use crate::metadata::ClusterMetadata;
use crate::metrics::{ConnectionMetrics, ProducerMetrics as ProducerMetricsInner};
use crate::network::{ConnectionConfig, ConnectionPool};
use crate::protocol::{
    ApiKey, Compression, InitProducerIdRequest, InitProducerIdResponse, ProduceRequest,
    ProduceResponse, VersionedDecode, VersionedEncode, versions,
};
use crate::serdes::Serializer;

use self::idempotent::ErasedProducerStateStore;
use crate::barrier::InFlightBarrier;

/// Build a [`ProducerRecord`] from the borrowed `(topic, key, value)` triple
/// the `send` convenience methods take.
///
/// Shared by [`Producer::send`] and
/// [`TransactionalProducer::send`](transaction::TransactionalProducer::send).
/// `None` is preserved as Kafka's null, never widened to an empty slice.
pub(crate) fn build_record(
    topic: &str,
    key: Option<&[u8]>,
    value: Option<&[u8]>,
) -> ProducerRecord {
    let mut record = ProducerRecord::new(topic, Bytes::new());
    record.value = value.map(Bytes::copy_from_slice);
    record.key = key.map(Bytes::copy_from_slice);
    record
}

/// Apply the configured key/value serializers to a record, in place.
///
/// Shared by [`Producer::enqueue`] and
/// [`TransactionalProducer::enqueue`](transaction::TransactionalProducer::enqueue)
/// so the two send paths cannot drift apart.
///
/// A `None` key or value is passed through untouched — see
/// [`ProducerBuilder::value_serializer`] for why.
pub(crate) async fn apply_serializers(
    record: &mut ProducerRecord,
    key_serializer: Option<&dyn Serializer>,
    value_serializer: Option<&dyn Serializer>,
) -> Result<()> {
    if let (Some(enc), Some(value)) = (value_serializer, record.value.as_ref()) {
        record.value = Some(
            enc.serialize(
                value.clone(),
                &record.topic,
                record.record_name.as_deref(),
                false,
            )
            .await?,
        );
    }
    if let (Some(enc), Some(key)) = (key_serializer, record.key.as_ref()) {
        record.key = Some(
            enc.serialize(
                key.clone(),
                &record.topic,
                record.record_name.as_deref(),
                true,
            )
            .await?,
        );
    }
    Ok(())
}

/// The `on_acknowledgement` a record owes once `on_send` has observed it.
///
/// `on_send` runs at the very top of the send path, before serialization,
/// validation, partitioning and the wait for buffer memory — every one of which
/// can reject the record and return early. Without this guard each of those
/// early returns is a record whose terminal callback never fires, which with
/// [`RecordContext`](crate::interceptor::RecordContext) in the picture leaks
/// whatever the interceptor parked there.
///
/// The guard makes the obligation a value that has to be spent: discharged by
/// [`fail`](Self::fail), which fires the terminal callback with the error, or
/// by [`take_context`](Self::take_context), which hands the context to the
/// accumulator so the callback fires there. Dropping it undischarged is a bug
/// in this crate, and `Drop` says so.
struct SendObligation<'a> {
    interceptor: &'a dyn crate::interceptor::ProducerInterceptor,
    /// `None` once discharged.
    context: Option<crate::interceptor::RecordContext>,
}

impl<'a> SendObligation<'a> {
    /// Open the obligation and run `on_send`.
    ///
    /// The context is created here rather than at the first `insert`, so from
    /// this point on every failure path has one to hand back. An untouched
    /// context allocates nothing.
    fn on_send(
        interceptor: &'a dyn crate::interceptor::ProducerInterceptor,
        record: &mut ProducerRecord,
    ) -> Self {
        let mut context = crate::interceptor::RecordContext::new();
        crate::interceptor::safe_on_send(interceptor, record, &mut context);
        Self {
            interceptor,
            context: Some(context),
        }
    }

    /// Discharge by reporting a terminal failure, returning `error` so call
    /// sites read `return Err(obligation.fail(..))`.
    ///
    /// Pass [`UNKNOWN_PARTITION`] when the record failed before it was routed.
    /// `headers` are the record's as of the failure — however far the chain and
    /// the serializers got before it was rejected.
    fn fail(
        &mut self,
        topic: &str,
        partition: PartitionId,
        headers: &RecordHeaders,
        error: KrafkaError,
    ) -> KrafkaError {
        if let Some(mut context) = self.context.take() {
            let metadata = RecordMetadata::failed(topic.to_owned(), partition);
            crate::interceptor::safe_on_acknowledgement(
                self.interceptor,
                &metadata,
                Some(&error),
                headers,
                &mut context,
            );
        }
        error
    }

    /// Discharge by handing the context to the accumulator, which owes the
    /// terminal callback from here on.
    fn take_context(&mut self) -> crate::interceptor::RecordContext {
        self.context.take().unwrap_or_default()
    }
}

impl Drop for SendObligation<'_> {
    fn drop(&mut self) {
        if self.context.is_some() && !std::thread::panicking() {
            // A send path grew an early return that forgot to report. Loud in
            // this crate's own tests, logged in production — never a panic in a
            // user's process.
            debug_assert!(
                false,
                "krafka bug: a record ran on_send but no on_acknowledgement was reported",
            );
            tracing::error!(
                "krafka bug: a record ran on_send but no on_acknowledgement was reported; \
                 per-record interceptor state was dropped",
            );
        }
    }
}

/// A Kafka producer.
pub struct Producer {
    /// Producer configuration.
    config: ProducerConfig,
    /// Cluster metadata.
    metadata: Arc<ClusterMetadata>,
    /// Connection pool.
    pool: Arc<ConnectionPool>,
    /// Partitioner.
    partitioner: Arc<dyn Partitioner>,
    /// Record accumulator.
    ///
    /// Every send goes through it, at every `linger` setting including zero —
    /// see [`ProducerBuilder::linger`]. There is deliberately no second,
    /// unbatched send path: it duplicated the retry, sequence-recovery,
    /// leader-hint and dead-letter logic, and it had no per-partition dispatch
    /// FIFO, so concurrent sends to one partition could reach the broker out of
    /// sequence order and fail an idempotent producer permanently.
    accumulator: RecordAccumulatorHandle,
    /// Barrier over all started send operations and shutdown state.
    in_flight_barrier: Arc<InFlightBarrier>,
    /// Shared metrics.
    metrics: Arc<ProducerMetricsInner>,
    /// Producer interceptor.
    interceptor: Arc<dyn crate::interceptor::ProducerInterceptor>,
    /// Producer identity for idempotent production (PID, epoch, sequences).
    identity: Option<Arc<ProducerIdentity>>,
    /// Optional key encoder applied transparently in `send_record`.
    ///
    /// When set, the record key is passed through this encoder (schema
    /// registration + Confluent wire framing) on every `send_record` call,
    /// before partitioning or batching.  Equivalent to `key.serializer` in
    /// the Java `KafkaProducer`.
    key_serializer: Option<Arc<dyn Serializer>>,
    /// Optional value encoder applied transparently in `send_record`.
    ///
    /// When set, the record value is passed through this encoder on every
    /// `send_record` call.  Equivalent to `value.serializer` in the Java
    /// `KafkaProducer`.
    value_serializer: Option<Arc<dyn Serializer>>,
    /// Whether this client owns its connection pool.
    ///
    /// `false` when the pool was borrowed from a
    /// [`KrafkaClient`](crate::client::KrafkaClient) via `with_client`.
    ///
    /// Closing a borrowed pool would tear down every sibling client's
    /// connections and fail their in-flight requests — which is what happened
    /// until `AdminClient`'s handling of this was extended to its siblings.
    pool_owned: bool,
}

async fn init_idempotent_producer_id(
    identity: &ProducerIdentity,
    metadata: &ClusterMetadata,
    retry_policy: &RetryPolicy,
) -> Result<()> {
    let started_at = Instant::now();

    for attempt in 0..=retry_policy.max_retries {
        if let Some(deadline) = retry_policy.delivery_timeout
            && started_at.elapsed() >= deadline
        {
            return Err(KrafkaError::timeout("InitProducerId"));
        }

        if attempt > 0 {
            let mut backoff = retry_policy.calculate_backoff(attempt);
            if let Some(deadline) = retry_policy.delivery_timeout {
                let elapsed = started_at.elapsed();
                if elapsed >= deadline {
                    return Err(KrafkaError::timeout("InitProducerId"));
                }
                backoff = backoff.min(deadline.saturating_sub(elapsed));
            }
            if !backoff.is_zero() {
                tokio::time::sleep(backoff).await;
            }
        }

        if let Some(deadline) = retry_policy.delivery_timeout
            && started_at.elapsed() >= deadline
        {
            return Err(KrafkaError::timeout("InitProducerId"));
        }

        let brokers = metadata.brokers();
        if brokers.is_empty() {
            if attempt < retry_policy.max_retries {
                warn!(attempt, "No brokers available for InitProducerId, retrying");
                continue;
            }
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::Malformed,
                "no brokers available for InitProducerId",
            ));
        }

        let broker = &brokers[attempt as usize % brokers.len()];
        let conn = match metadata.get_broker_connection(broker.id()).await {
            Ok(connection) => connection,
            Err(error) if error.is_retriable() && attempt < retry_policy.max_retries => {
                warn!(
                    attempt,
                    error = %error,
                    "Connection failed for InitProducerId, retrying"
                );
                continue;
            }
            Err(error) => return Err(error),
        };

        let ip_version = match conn.negotiate_api_version(
            ApiKey::InitProducerId,
            versions::INIT_PRODUCER_ID_MAX,
            versions::INIT_PRODUCER_ID_MIN,
        ) {
            Some(version) => version,
            None => {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::UnknownApiVersion,
                    "no mutually supported InitProducerId API version",
                ));
            }
        };

        let request = InitProducerIdRequest::idempotent();
        let response_bytes = match conn
            .send_request(ApiKey::InitProducerId, ip_version, |buf| {
                request.encode_versioned(ip_version, buf)
            })
            .await
        {
            Ok(bytes) => bytes,
            Err(error) if error.is_retriable() && attempt < retry_policy.max_retries => {
                warn!(
                    attempt,
                    error = %error,
                    "InitProducerId request failed, retrying"
                );
                continue;
            }
            Err(error) => return Err(error),
        };

        let mut buf = response_bytes;
        let response = InitProducerIdResponse::decode_versioned(ip_version, &mut buf)?;

        if response.is_ok() {
            identity.initialize(response.producer_id, response.producer_epoch);
            info!(
                "Idempotent producer initialized: PID={}, epoch={}",
                response.producer_id, response.producer_epoch
            );
            return Ok(());
        }

        if response.error_code.is_retriable() && attempt < retry_policy.max_retries {
            warn!(
                error_code = ?response.error_code,
                attempt,
                "InitProducerId returned retriable error, retrying"
            );
        } else {
            return Err(KrafkaError::broker(
                response.error_code,
                "failed to initialize producer ID",
            ));
        }
    }

    Err(KrafkaError::protocol_kind(
        ProtocolErrorKind::Malformed,
        format!(
            "InitProducerId retry loop exhausted after {} retries",
            retry_policy.max_retries
        ),
    ))
}

/// Ensure the idempotent producer has a usable PID/epoch before a batch is
/// dispatched.
///
/// Honours a pending re-init request on [`ProducerIdentity`]: when the
/// local sequence space was invalidated — for example because a failed batch no
/// longer owned the tail of its allocated range — the current PID and all
/// partition sequences are discarded here and a fresh `InitProducerId` is
/// performed. This replaces the previous "poisoned" behaviour, which
/// permanently bricked the producer and required the application to rebuild it.
async fn ensure_idempotent_producer_id_initialized(
    identity: &ProducerIdentity,
    metadata: &ClusterMetadata,
    retry_policy: &RetryPolicy,
) -> Result<()> {
    if identity.take_reinit_request() {
        warn!(
            "Producer identity invalidated; obtaining a fresh producer ID and \
             restarting all partition sequences"
        );
    } else if identity.is_initialized() {
        return Ok(());
    }

    init_idempotent_producer_id(identity, metadata, retry_policy).await
}

async fn recover_unknown_producer_id(
    identity: &ProducerIdentity,
    metadata: &ClusterMetadata,
    retry_policy: &RetryPolicy,
    topic: &str,
    partition: PartitionId,
    base_sequence: i32,
    record_count: i32,
) -> Result<i32> {
    if identity.needs_reinit() {
        return Err(KrafkaError::broker(
            ErrorCode::UnknownProducerId,
            "producer identity is awaiting re-initialisation; retry the send",
        ));
    }

    // Use the atomic check-and-reset to avoid the TOCTOU window between a
    // separate can_retry_unknown_producer_id() (read lock) + reset() (write
    // lock) pair: no concurrent thread can allocate sequences against the
    // current PID between the retryability check and the state reset.
    //
    // This check effectively always succeeds: the per-partition dispatch FIFO
    // guarantees this batch is the only one in flight for its partition, so no
    // newer allocation can exist. It is kept because the invariant is the
    // reason the in-place recovery below is sound, and a future change that
    // broke it would otherwise corrupt sequences silently.
    if !identity.check_and_reset_if_retryable(topic, partition, base_sequence, record_count)? {
        // Previously this poisoned the identity, permanently bricking the
        // producer whenever more than one batch was outstanding. Instead,
        // request a re-initialisation: the next dispatch obtains a fresh PID
        // and restarts every partition at sequence 0. Concurrent batches still
        // using the old PID fail and are retried under the new one.
        warn!(
            topic,
            partition,
            "UnknownProducerId while newer batches for this partition are still in flight; \
             requesting a producer-ID re-init instead of failing permanently"
        );
        identity.request_reinit();
        return Err(KrafkaError::broker(
            ErrorCode::UnknownProducerId,
            format!(
                "UnknownProducerId for {topic}-{partition} could not be resolved in place \
                 while newer batches were in flight; the producer ID will be re-initialised \
                 and this send should be retried"
            ),
        ));
    }

    init_idempotent_producer_id(identity, metadata, retry_policy).await?;
    identity.allocate_sequence(topic, partition, record_count)
}

/// Build the fatal error raised when `OUT_OF_ORDER_SEQUENCE_NUMBER` cannot be
/// resolved by a local sequence rewind.
///
/// Deliberately **not** a `Broker` error: broker errors are classified
/// retriable, and retrying is precisely the bug — it papers over records the
/// broker never durably stored. Matches librdkafka, which raises a fatal error
/// when the failing request is head-of-line.
pub(crate) fn out_of_order_data_loss_error(
    topic: &str,
    partition: PartitionId,
    base_sequence: i32,
) -> KrafkaError {
    KrafkaError::invalid_state(format!(
        "fatal OUT_OF_ORDER_SEQUENCE_NUMBER for {topic}-{partition} at base sequence \
         {base_sequence}: the broker expected a different sequence, which means an earlier \
         batch was never durably stored (log truncation or unclean leader election). \
         Retrying would silently write this batch into the resulting gap, so the send is \
         failed instead. Recreate the producer to resume."
    ))
}

/// Fold a leader named in a produce response into the metadata cache (KIP-951).
///
/// A broker that rejects a produce with `NOT_LEADER_OR_FOLLOWER` /
/// `FENCED_LEADER_EPOCH` also reports which node now leads the partition, and
/// advertises that node's endpoint. Applying it lets the retry go straight to
/// the new leader; the alternative is a forced metadata refresh — an extra
/// round trip on the failover path, on top of the produce that just failed.
///
/// Returns `true` when the cache changed, i.e. when the caller can retry
/// immediately instead of refreshing metadata first. Any other error code is
/// left alone: only these two mean "you sent this to the wrong broker".
fn apply_produce_leader_hint(
    metadata: &ClusterMetadata,
    topic: &str,
    partition: PartitionId,
    response: &ProduceResponse,
    partition_response: &crate::protocol::ProducePartitionResponse,
) -> bool {
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
/// Returning the bytes avoids a second encode in the send path — callers
/// write the pre-encoded body directly into the connection's I/O buffer.
///
/// This is the single source of truth for produce frame sizing — it uses the
/// real encoder rather than a separate analytical size-computation path,
/// which would otherwise need to be kept in sync with every encoding change.
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
        // `FrameTooLarge`, not `InvalidLength`: the accumulator's
        // split-and-resend recovery keys off this kind, and it must never be
        // triggered by a response that merely failed to decode.
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
/// Looks up each topic's UUID from the metadata cache and fills it in.
/// Returns `true` if **all** topic IDs were resolved; returns `false` if any
/// UUID was missing (caller should cap the wire version to v12 and retry with
/// topic names instead).
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

/// Hand-written rather than derived, for two reasons.
///
/// The mechanical one: several fields are `Arc<dyn Trait>` — the partitioner,
/// the interceptor, the DLQ, the schema encoders — and a derive would require
/// `Debug` on every one of those traits.
///
/// The one that matters: [`DeadLetterQueue`](crate::dlq::DeadLetterQueue)
/// requires `Debug`, and the obvious implementation of it owns a `Producer` to
/// write dead letters back into Kafka. Without this impl that natural design
/// does not compile, and the documented example for the crate's own DLQ trait
/// was wrong for exactly that reason.
///
/// Only non-secret, non-`dyn` state is printed. `ProducerConfig` is
/// deliberately excluded — it can carry SASL credentials, and the crate's
/// `secret-debug` CI check exists to keep credential-bearing types out of
/// `Debug` output.
impl std::fmt::Debug for Producer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("client_id", &self.config.client_id)
            .field("idempotent", &self.identity.is_some())
            .field("connections", &self.pool.len())
            .field("owns_pool", &self.owns_pool())
            .finish_non_exhaustive()
    }
}

impl Producer {
    /// Create a new producer builder.
    pub fn builder() -> ProducerBuilder {
        ProducerBuilder::default()
    }

    /// Create a new producer with the given configuration.
    async fn new(
        config: ProducerConfig,
        interceptor: Arc<dyn crate::interceptor::ProducerInterceptor>,
        partitioner: Option<Arc<dyn Partitioner>>,
        key_serializer: Option<Arc<dyn Serializer>>,
        value_serializer: Option<Arc<dyn Serializer>>,
        shared: Option<(Arc<ConnectionPool>, Arc<crate::metadata::ClusterMetadata>)>,
        state_store: Option<Arc<dyn ErasedProducerStateStore>>,
    ) -> Result<Self> {
        let pool_owned = shared.is_none();
        let (pool, metadata) = if let Some((pool, metadata)) = shared {
            // Use the pre-built shared pool and metadata from a KrafkaClient.
            // No need to construct a new pool or perform an initial metadata
            // fetch — the KrafkaClient already did that at build time.
            (pool, metadata)
        } else {
            let mut pool_config_builder = config.transport.apply(
                ConnectionConfig::builder()
                    .client_id(&config.client_id)
                    .request_timeout(config.request_timeout)
                    .connect_timeout(config.connect_timeout),
            );

            if let Some(ref auth) = config.auth {
                pool_config_builder = pool_config_builder.auth(auth.clone());
            }

            let mut pool_config = pool_config_builder.build()?;
            pool_config.init_tls().await?;

            // Every client builds its pool through `TransportConfig::build_pool`,
            // which applies the pool-level settings and starts the background
            // tasks (idle eviction, OAUTHBEARER refresh, KIP-1288 TLS reload).
            // Routing all construction sites through one function is what stops
            // them drifting apart again.
            let pool = config.transport.build_pool(pool_config);

            let bootstrap_servers =
                crate::util::parse_bootstrap_servers(&config.bootstrap_servers)?;

            let metadata = Arc::new({
                let mut meta =
                    ClusterMetadata::new(bootstrap_servers, pool.clone(), config.metadata_max_age)
                        .with_recovery_strategy(config.metadata_recovery_strategy)
                        .with_rebootstrap_trigger(config.metadata_recovery_rebootstrap_trigger);
                if let Some(ttl) = config.metadata_topic_cache_ttl {
                    meta = meta.with_topic_cache_ttl(ttl);
                } else {
                    meta = meta.with_topic_cache_ttl_disabled();
                }
                meta
            });

            // Initial metadata fetch
            metadata.refresh().await?;

            info!(
                "Producer initialized with {} brokers",
                metadata.brokers().len()
            );

            (pool, metadata)
        };

        let init_retry_policy = RetryPolicy::new()
            .with_max_retries(config.retries)
            .with_initial_backoff(config.retry_backoff)
            .with_max_backoff(Duration::from_secs(10))
            .with_delivery_timeout(Some(config.delivery_timeout));

        // Initialize idempotent producer identity (PID + epoch) if enabled.
        let identity = if config.idempotent {
            let identity = Arc::new(ProducerIdentity::new());
            init_idempotent_producer_id(&identity, &metadata, &init_retry_policy).await?;

            // If a state store is configured, attempt to load and restore a
            // previous snapshot.  Restoration only succeeds when both
            // `producer_id` and `producer_epoch` in the snapshot match the
            // broker-assigned values — this occurs exclusively for transactional
            // producers whose `transactional.id` the broker recognises.
            if let Some(ref store) = state_store {
                match store.load_erased().await {
                    Ok(Some(snapshot))
                        if snapshot.producer_id == identity.producer_id()
                            && snapshot.producer_epoch == identity.producer_epoch() =>
                    {
                        identity.restore_from_snapshot(&snapshot);
                        info!(
                            pid = identity.producer_id(),
                            epoch = identity.producer_epoch(),
                            partitions = snapshot.partition_sequences.len(),
                            "Producer identity restored from state store"
                        );
                    }
                    Ok(Some(_)) => {
                        debug!(
                            "State store snapshot PID/epoch mismatch — sequences not restored \
                             (expected for new transactional sessions or plain idempotent producers)"
                        );
                    }
                    Ok(None) => {
                        debug!("No previous producer state found in state store");
                    }
                    Err(err) => {
                        warn!(error = %err, "Failed to load producer state from store; continuing with fresh state");
                    }
                }
            }

            Some(identity)
        } else {
            None
        };

        // KIP-480: the default partitioner is the uniform sticky partitioner,
        // unconditionally.
        //
        // Previously this fell back to round-robin whenever `linger == 0` —
        // which is the *default* linger — on the theory that there are no batch
        // boundaries to stick to. That is pre-KIP-480 behaviour and is actively
        // harmful: round-robin spreads consecutive records across every
        // partition, so a topic with N partitions produces N separate Produce
        // requests where sticky produces one. Sticky partitioning helps at
        // `linger = 0` too, because records that arrive while a request is in
        // flight coalesce into the batch behind it — see
        // `RecordAccumulator::dispatch_unblocked_partitions`.
        let partitioner: Arc<dyn Partitioner> =
            partitioner.unwrap_or_else(|| Arc::new(UniformStickyPartitioner::new()));

        // Build retry policy from config
        let retry_policy = RetryPolicy::new()
            .with_max_retries(config.retries)
            .with_initial_backoff(config.retry_backoff)
            .with_max_backoff(Duration::from_secs(30))
            .with_delivery_timeout(Some(config.delivery_timeout));

        // Shared metrics
        let metrics = Arc::new(ProducerMetricsInner::default());

        if config.buffer_memory == 0 {
            warn!(
                "buffer_memory=0 disables producer backpressure; \
                 memory usage is unbounded. Not recommended for production."
            );
        }

        let in_flight_barrier = Arc::new(InFlightBarrier::new());

        // One send path, always. See `Producer::accumulator`.
        let acc_config = accumulator::AccumulatorConfig {
            batch_size: config.batch_size,
            linger: config.linger,
            compression: config.compression,
            compression_level: config.compression_level,
            topic_compression: config.topic_compression.clone().into_iter().collect(),
            acks: config.acks.to_i16(),
            client_id: config.client_id.clone(),
            request_timeout: config.request_timeout,
            max_request_size: config.max_request_size,
            buffer_memory: config.buffer_memory,
            max_block_ms: config.max_block,
            interceptor: interceptor.clone(),
            identity: identity.clone(),
            partitioner: partitioner.clone(),
            state_store: state_store.clone(),
            transactional_id: None,
            dead_letter_queue: config.dead_letter_queue.clone(),
        };
        let accumulator = accumulator::RecordAccumulator::spawn(
            acc_config,
            metadata.clone(),
            retry_policy.clone(),
            metrics.clone(),
            in_flight_barrier.clone(),
        );

        Ok(Self {
            config: config.clone(),
            metadata,
            pool,
            partitioner,
            accumulator,
            in_flight_barrier,
            metrics,
            interceptor,
            identity,
            key_serializer,
            value_serializer,
            pool_owned,
        })
    }

    /// Send a record to a topic.
    ///
    /// This is the main method for producing messages.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use krafka::producer::Producer;
    /// # async fn example() -> Result<(), krafka::error::KrafkaError> {
    /// let producer = Producer::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .build()
    ///     .await?;
    ///
    /// let metadata = producer.send("my-topic", Some(b"key"), Some(b"value")).await?;
    /// println!("Sent to partition {} at offset {}", metadata.partition, metadata.offset);
    ///
    /// // A null value is a tombstone: on a compacted topic it deletes the key.
    /// producer.send("my-topic", Some(b"key"), None).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Both arguments are `Option` because Kafka distinguishes *absent* from
    /// *empty*: a `None` key is keyless (the default partitioner spreads it
    /// rather than hashing), and a `None` value is a
    /// [tombstone](ProducerRecord::tombstone). `Some(&[])` is a zero-length
    /// field, which is ordinary data.
    pub async fn send(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
    ) -> Result<RecordMetadata> {
        self.send_record(build_record(topic, key, value)).await
    }

    /// Send a record with headers.
    ///
    /// `key`, `value` and each header value are `Option` so that Kafka's
    /// null-versus-empty distinction survives; see [`send`](Self::send).
    pub async fn send_with_headers(
        &self,
        topic: &str,
        key: Option<&[u8]>,
        value: Option<&[u8]>,
        headers: Vec<(String, Option<Bytes>)>,
    ) -> Result<RecordMetadata> {
        let mut record = build_record(topic, key, value);
        record.headers = headers;
        self.send_record(record).await
    }

    /// Send a producer record and wait for the broker to acknowledge it.
    ///
    /// Equivalent to `enqueue(record).await?.await`. Use
    /// [`enqueue`](Self::enqueue) when you need several records in flight at
    /// once — awaiting each acknowledgement before sending the next costs a
    /// full round trip per record.
    pub async fn send_record(&self, record: ProducerRecord) -> Result<RecordMetadata> {
        self.enqueue(record).await?.await
    }

    /// Queue a record for sending and return as soon as it is **queued**.
    ///
    /// The returned [`DeliveryHandle`] resolves to the broker's answer. This is
    /// the shape of Java's `Producer.send()`, and it exists for the same
    /// reason: separating "this record has taken its place in the stream" from
    /// "this record is durable" is what lets a caller pipeline without giving
    /// up ordering.
    ///
    /// # Ordering
    ///
    /// **Produce order is enqueue order.** If `enqueue(a)` returns before
    /// `enqueue(b)` is called, `a` reaches its partition before `b` — whatever
    /// order the two handles are polled in, and whether or not they are polled
    /// at all.
    ///
    /// That guarantee is why this method exists. [`send_record`](Self::send_record)
    /// fuses the enqueue and the acknowledgement into one future, so a caller
    /// who builds N of them and polls them concurrently gets records appended in
    /// *poll* order, not call order — and under buffer-memory backpressure the
    /// two diverge, because a send that cannot get its permit yields and lets a
    /// later one append first. Pipelining on top of the fused future therefore
    /// requires polling every outstanding future in submission order on every
    /// wake, which is both subtle and O(window) per wake.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use futures::stream::{FuturesUnordered, StreamExt};
    /// use krafka::producer::{Producer, ProducerRecord};
    ///
    /// # async fn example(producer: &Producer) -> Result<(), krafka::error::KrafkaError> {
    /// let mut acks = FuturesUnordered::new();
    /// for i in 0..1000 {
    ///     // Ordering is fixed here, in loop order.
    ///     acks.push(producer.enqueue(ProducerRecord::new("events", vec![i as u8])).await?);
    /// }
    /// // Completion order is irrelevant to what the broker stored.
    /// while let Some(result) = acks.next().await {
    ///     let metadata = result?;
    ///     let _ = metadata.offset;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// The outer `Result` covers everything up to and including the enqueue:
    /// interceptors, serializers, record validation, unknown topics, and the
    /// up-to-`max_block` wait for buffer memory. The handle covers delivery.
    pub async fn enqueue(&self, record: ProducerRecord) -> Result<DeliveryHandle> {
        // `delivery.timeout.ms` covers everything from `send()` entry, so
        // the clock starts here — before schema encoding, partition lookup and,
        // critically, before the up-to-`max_block` wait for buffer memory.
        // Starting it at the first network attempt silently excluded up to 60 s
        // of blocking from the delivery budget.
        let send_started_at = Instant::now();
        let operation_guard = self.in_flight_barrier.start("producer")?;

        // Invoke interceptor before send.
        //
        // Everything from here to the handoff is inside the obligation: each
        // early return goes through `obligation.fail(..)`, so a record that
        // `on_send` observed always reaches `on_acknowledgement`.
        let mut record = record;
        let mut obligation = SendObligation::on_send(&*self.interceptor, &mut record);

        // Transparently apply producer-level schema encoders if configured.
        // Runs after the interceptor (which may set topic/key/value) but before
        // validation, so oversized encoded payloads are still caught.
        if let Err(e) = apply_serializers(
            &mut record,
            self.key_serializer.as_deref(),
            self.value_serializer.as_deref(),
        )
        .await
        {
            return Err(obligation.fail(&record.topic, UNKNOWN_PARTITION, &record.headers, e));
        }

        // Validate record fields against Kafka protocol wire-format limits.
        // Runs after the interceptor since interceptors can mutate the record.
        if let Err(e) = record.validate() {
            return Err(obligation.fail(&record.topic, UNKNOWN_PARTITION, &record.headers, e));
        }

        let record_size = record.estimated_size();
        let routed = record.into_routed_parts();
        let topic = routed.topic;
        let record = routed.record;

        // Determine partition
        let partition = match routed.partition {
            Some(p) => p,
            None => match self.metadata.partition_count(topic.as_ref()) {
                Some(partition_count) => {
                    self.partitioner
                        .partition(topic.as_ref(), record.key_bytes(), partition_count)
                }
                None => {
                    let error = KrafkaError::invalid_state(format!("unknown topic: {topic}"));
                    return Err(obligation.fail(
                        topic.as_ref(),
                        UNKNOWN_PARTITION,
                        &record.headers,
                        error,
                    ));
                }
            },
        };

        match self
            .accumulator
            .enqueue_routed_with_guard(
                topic.clone(),
                record,
                record_size,
                partition,
                operation_guard,
                send_started_at,
                obligation.take_context(),
            )
            .await
        {
            Ok(handle) => Ok(handle),
            // The accumulator hands the context back rather than dropping it,
            // so the obligation is re-opened here and discharged as a failure
            // with the partition the record had already been routed to.
            Err(rejected) => {
                obligation.context = Some(rejected.context);
                Err(obligation.fail(
                    topic.as_ref(),
                    partition,
                    &rejected.record.headers,
                    rejected.error,
                ))
            }
        }
    }

    /// Flush all pending records.
    pub async fn flush(&self) -> Result<()> {
        let target = self.in_flight_barrier.snapshot();
        self.accumulator.flush().await?;

        self.in_flight_barrier.wait_for(target).await;
        Ok(())
    }

    /// Replace the bootstrap server list at runtime (KIP-899).
    ///
    /// The new addresses are used on the next metadata refresh that falls back
    /// to bootstrap servers. Does not close existing connections.
    ///
    /// # Errors
    ///
    /// Returns an error if `servers` is empty.
    pub fn update_seed_brokers(&self, servers: Vec<String>) -> Result<()> {
        self.metadata.update_seed_brokers(servers)
    }

    /// Re-read TLS certificate and key files from disk and atomically install
    /// the new material for all **future** connections (KIP-1288).
    ///
    /// Existing TLS sessions are unaffected: they keep the connector they
    /// handshaked with and are replaced naturally as connections cycle. On
    /// error the previously loaded certificates stay active, so a call made
    /// mid-rotation against a half-written PEM is safe to retry.
    ///
    /// No-op when TLS is not configured.
    ///
    /// Use this for event-driven rotation (an inotify watch, a sidecar
    /// signal). For unattended rotation set
    /// [`TransportConfig::tls_reload_interval`](crate::network::TransportConfig)
    /// instead and krafka reloads on a timer.
    ///
    /// # Errors
    ///
    /// Returns an error if the certificate or key files cannot be read or
    /// parsed.
    pub async fn refresh_tls(&self) -> Result<()> {
        self.pool.refresh_tls().await
    }

    /// Force a rebootstrap: close all connections, clear the metadata cache,
    /// and fall back to bootstrap servers (KIP-899).
    pub async fn rebootstrap(&self) {
        self.metadata.rebootstrap().await;
    }

    /// Close the producer, flushing every buffered record first.
    ///
    /// **Calling this is mandatory.** [`Drop`] cannot flush — it is not async —
    /// so a producer that goes out of scope without `close()` discards whatever
    /// is still sitting in the accumulator or in retry backoff, and the
    /// corresponding `send()` futures never complete. `Drop` makes a
    /// best-effort attempt to drain (see the `Drop` impl) but it can only do so
    /// when a Tokio runtime is still alive and it cannot wait for completion.
    ///
    /// Flushes pending records, notifies interceptors, and tears down
    /// connections. Calling `close()` more than once is a no-op. Use
    /// [`close_with_timeout`](Self::close_with_timeout) to bound the wait.
    pub async fn close(&self) {
        let _ = self.close_inner(None).await;
    }

    /// Close the producer, giving up on graceful shutdown once `timeout` expires.
    ///
    /// If the timeout elapses before all started sends cross the acknowledgment
    /// boundary, remaining work is failed by tearing down the connection pool.
    /// Batches in retry backoff will be aborted without notification.
    pub async fn close_with_timeout(&self, timeout: Duration) -> Result<()> {
        self.close_inner(Some(timeout)).await
    }

    async fn close_inner(&self, timeout: Option<Duration>) -> Result<()> {
        let Some(target) = self.in_flight_barrier.begin_close() else {
            return Ok(());
        };

        let graceful_close = async {
            // Shutdown accumulator first to flush pending records.
            if let Err(e) = self.accumulator.shutdown().await {
                warn!("Accumulator shutdown error during close: {e}");
            }

            self.in_flight_barrier.wait_for(target).await;
        };

        let close_result = if let Some(timeout) = timeout {
            tokio::time::timeout(timeout, graceful_close)
                .await
                .map_err(|_| {
                    warn!("Producer close timed out; batches in retry backoff may be lost");
                    KrafkaError::timeout("producer close")
                })
        } else {
            graceful_close.await;
            Ok(())
        };

        // Notify interceptor of shutdown
        crate::interceptor::safe_producer_close(&*self.interceptor);

        // A pool borrowed from a `KrafkaClient` belongs to that client: tearing
        // it down here would kill every sibling consumer, admin client and
        // producer sharing it and fail their in-flight requests. `AdminClient`
        // already got this right; its siblings did not.
        if self.pool_owned {
            self.pool.close_all().await;
            info!("Producer closed (connection pool torn down)");
        } else {
            info!("Producer closed (shared connection pool left open)");
        }

        close_result
    }

    /// Whether this client owns its connection pool.
    ///
    /// `false` when the pool was borrowed from a
    /// [`KrafkaClient`](crate::client::KrafkaClient) via `with_client`. In that
    /// case [`close`](Self::close) leaves the connections untouched — closing
    /// them would tear down every sibling client on that `KrafkaClient` and
    /// fail their in-flight requests. Close the `KrafkaClient` to release them.
    #[inline]
    #[must_use]
    pub fn owns_pool(&self) -> bool {
        self.pool_owned
    }

    /// Check if the producer is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.in_flight_barrier.is_closing()
    }

    /// Get producer metrics.
    ///
    /// Synchronous, like every other metrics accessor in this crate: the
    /// counters are atomics and the connection count is a lock-free read. It
    /// used to be `async` with no `await`, which meant a Prometheus scrape
    /// handler or a signal handler could read `Consumer::metrics()` but not
    /// this one.
    pub fn metrics(&self) -> ProducerMetricsSnapshot {
        ProducerMetricsSnapshot {
            connections: self.pool.len(),
            records_sent: self.metrics.records_sent.get(),
            bytes_sent: self.metrics.bytes_sent.get(),
            errors: self.metrics.errors.get(),
            retries: self.metrics.retries.get(),
            buffered_records: self.metrics.buffered_records.get(),
        }
    }

    /// Get the shared metrics handle (for external monitoring).
    #[inline]
    pub fn metrics_handle(&self) -> Arc<ProducerMetricsInner> {
        self.metrics.clone()
    }

    /// Get the shared connection metrics handle used by this producer's broker pool.
    #[inline]
    pub fn connection_metrics(&self) -> Arc<ConnectionMetrics> {
        self.pool.metrics()
    }
}

impl Drop for Producer {
    /// Best-effort rescue for a producer dropped without
    /// [`close()`](Producer::close).
    ///
    /// `Drop` is synchronous, so it cannot await the flush and cannot report
    /// whether it succeeded. What it *can* do is stop the accumulator from
    /// being torn down with records still buffered: if a Tokio runtime is still
    /// available, the accumulator handle is moved into a detached shutdown task
    /// which drains and dispatches the remaining batches. That task races the
    /// runtime's own shutdown, so delivery is **not** guaranteed.
    ///
    /// `close()` remains mandatory for any producer whose records matter.
    fn drop(&mut self) {
        if self.in_flight_barrier.is_closing() {
            return;
        }

        // Clone the handle so the accumulator task is not dropped along with
        // `self`; the spawned task keeps it alive until the flush completes.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let accumulator = self.accumulator.clone();
            drop(runtime.spawn(async move {
                if let Err(err) = accumulator.shutdown().await {
                    warn!(error = %err, "Best-effort flush on Producer drop failed");
                }
            }));
        }

        // Skip the warning during panic unwinding — the panic is the story.
        if !std::thread::panicking() {
            warn!(
                "Producer dropped without close(); buffered batches are being flushed on a \
                 detached task with no completion guarantee and may still be lost. Call \
                 `Producer::close()` (or `close_with_timeout`) before drop."
            );
        }
    }
}

/// Producer metrics snapshot.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ProducerMetricsSnapshot {
    /// Number of active connections.
    pub connections: usize,
    /// Total records sent.
    pub records_sent: u64,
    /// Total bytes sent.
    pub bytes_sent: u64,
    /// Total errors.
    pub errors: u64,
    /// Total retries.
    pub retries: u64,
    /// Records currently admitted under the producer memory budget.
    pub buffered_records: u64,
}

/// Builder for creating producers.
#[derive(Default)]
#[must_use = "builders do nothing until .build() is called"]
pub struct ProducerBuilder {
    config: ProducerConfig,
    interceptors: Vec<Arc<dyn crate::interceptor::ProducerInterceptor>>,
    partitioner: Option<Arc<dyn Partitioner>>,
    key_serializer: Option<Arc<dyn Serializer>>,
    value_serializer: Option<Arc<dyn Serializer>>,
    /// Pre-built pool and metadata from a [`KrafkaClient`](crate::client::KrafkaClient).
    shared: Option<(Arc<ConnectionPool>, Arc<crate::metadata::ClusterMetadata>)>,
    /// Optional pluggable persistence hook for producer identity state.
    state_store: Option<Arc<dyn ErasedProducerStateStore>>,
}

impl ProducerBuilder {
    /// Set the bootstrap servers.
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    /// Set the client ID.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.config.client_id = client_id.into();
        self
    }

    /// Set the required acknowledgments.
    pub fn acks(mut self, acks: Acks) -> Self {
        self.config.acks = acks;
        self
    }

    /// Override the compression codec for one topic.
    ///
    /// Topics without an override use the producer-wide
    /// [`compression`](Self::compression) setting. Useful when one high-volume
    /// topic warrants a heavier codec than the rest of the traffic.
    pub fn topic_compression(mut self, topic: impl Into<String>, compression: Compression) -> Self {
        self.config
            .topic_compression
            .insert(topic.into(), compression);
        self
    }

    /// Set the total bytes the producer may buffer for unsent records.
    ///
    /// This is the backpressure budget: once it is exhausted, `send()` blocks
    /// for up to [`max_block`](Self::max_block) rather than growing without
    /// bound.
    pub fn buffer_memory(mut self, bytes: usize) -> Self {
        self.config.buffer_memory = bytes;
        self
    }

    /// How long `send()` may block when the buffer is full before failing.
    pub fn max_block(mut self, duration: Duration) -> Self {
        self.config.max_block = duration;
        self
    }

    /// Route permanently failed records to a dead-letter queue.
    ///
    /// Each record is handed to the DLQ once, after its retry budget is
    /// exhausted or on a non-retriable error, immediately before the failure is
    /// returned to the caller. `send()` still returns the error — the DLQ
    /// preserves the payload, it does not swallow the failure.
    ///
    /// # Scope
    ///
    /// Every send, at every `linger` setting, and on the
    /// `TransactionalProducer` too — there is one send path. The accumulator
    /// keeps each record's key, value, headers and timestamp for the lifetime
    /// of its batch, which is what makes the record reconstructable at the
    /// point of permanent failure.
    pub fn dead_letter_queue(mut self, dlq: Arc<dyn crate::dlq::DeadLetterQueue>) -> Self {
        self.config.dead_letter_queue = Some(dlq);
        self
    }

    /// Set the metadata recovery strategy (KIP-1102).
    ///
    /// Controls what the client does when every known broker becomes
    /// unreachable: keep retrying the cached broker set, or fall back to the
    /// original bootstrap servers.
    pub fn metadata_recovery_strategy(
        mut self,
        strategy: crate::metadata::MetadataRecoveryStrategy,
    ) -> Self {
        self.config.metadata_recovery_strategy = strategy;
        self
    }

    /// How long metadata must stay unrefreshable before a rebootstrap fires.
    ///
    /// Only effective with
    /// [`MetadataRecoveryStrategy::Rebootstrap`](crate::metadata::MetadataRecoveryStrategy::Rebootstrap).
    pub fn metadata_recovery_rebootstrap_trigger(mut self, duration: Duration) -> Self {
        self.config.metadata_recovery_rebootstrap_trigger = duration;
        self
    }

    /// Route all broker connections through a SOCKS5 proxy.
    #[cfg(feature = "socks5")]
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.config.transport.proxy = Some(proxy);
        self
    }

    /// Set socket- and pool-level transport tuning.
    ///
    /// Covers TCP keepalive and nodelay, the per-connection response ceiling
    /// and in-flight cap, the priority-channel depths, the Happy Eyeballs
    /// stagger, idle-connection eviction, a total-connection cap, and the
    /// KIP-1288 automatic TLS reload interval.
    ///
    /// Omitting this call keeps krafka's historical defaults, which
    /// [`TransportConfig::default`](crate::network::TransportConfig) reproduces
    /// exactly.
    pub fn transport(mut self, transport: crate::network::TransportConfig) -> Self {
        self.config.transport = transport;
        self
    }

    /// Set the compression type.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.config.compression = compression;
        self
    }

    /// Override the compression codec's default level.
    ///
    /// `None` (the default) uses the codec's own default: zlib 6 for `Gzip`,
    /// 3 for `Zstd` — the same defaults the Java client applies.
    ///
    /// Only `Gzip` and `Zstd` take a level. `Snappy` has none in its format,
    /// and krafka encodes LZ4 with `lz4_flex`, whose frame encoder exposes
    /// none. Setting a level alongside either is rejected at build time rather
    /// than ignored, because a silently-ignored tuning knob is how a
    /// deployment ships believing it was tuned.
    ///
    /// # Choosing a value
    ///
    /// Zstd's range is what the linked libzstd reports — negative "fast"
    /// levels through 22. Level 3 already compresses Kafka payloads well; the
    /// levels above roughly 9 cost CPU far faster than they save bytes, and on
    /// a producer that is throughput-bound rather than bandwidth-bound they
    /// are usually a net loss. Measure against your own payloads.
    ///
    /// ```no_run
    /// # use krafka::producer::Producer;
    /// # use krafka::protocol::Compression;
    /// # async fn f() -> krafka::error::Result<()> {
    /// let producer = Producer::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .compression(Compression::Zstd)
    ///     .compression_level(Some(1)) // favour throughput over ratio
    ///     .build()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    pub fn compression_level(mut self, level: Option<i32>) -> Self {
        self.config.compression_level = level;
        self
    }

    /// Set the batch size.
    pub fn batch_size(mut self, size: usize) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Set the linger time.
    pub fn linger(mut self, duration: Duration) -> Self {
        self.config.linger = duration;
        self
    }

    /// Set the request timeout: how long one in-flight request may wait for its
    /// response. Default: 30 s.
    ///
    /// Must be at least [`connect_timeout`](Self::connect_timeout), whose
    /// default is 10 s — a request's clock covers establishing the connection
    /// it is sent over, so a shorter value would expire every request before
    /// the handshake could finish. To go below 10 s, lower `connect_timeout`
    /// as well; `build()` returns a config error otherwise.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set the connect timeout: how long TCP establishment to one broker may
    /// take. Default: 10 s.
    ///
    /// This also acts as the floor on
    /// [`request_timeout`](Self::request_timeout), so lowering it is what makes
    /// a sub-10-second request timeout possible.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    /// Set the total delivery timeout.
    ///
    /// Bounds the total time a record may spend queued and retried before it
    /// fails locally. Default: 120 seconds.
    pub fn delivery_timeout(mut self, timeout: Duration) -> Self {
        self.config.delivery_timeout = timeout;
        self
    }

    /// Set the number of retries.
    pub fn retries(mut self, retries: u32) -> Self {
        self.config.retries = retries;
        self
    }

    /// Set the retry backoff.
    pub fn retry_backoff(mut self, backoff: Duration) -> Self {
        self.config.retry_backoff = backoff;
        self
    }

    /// Set the maximum encoded Kafka request frame size in bytes.
    pub fn max_request_size(mut self, bytes: usize) -> Self {
        self.config.max_request_size = bytes;
        self
    }

    /// Set metadata max age before refresh.
    pub fn metadata_max_age(mut self, duration: Duration) -> Self {
        self.config.metadata_max_age = duration;
        self
    }

    /// Set the topic cache TTL for partial metadata refreshes.
    ///
    /// During partial refreshes, cached topics that have not been refreshed
    /// within this duration are evicted to prevent unbounded cache growth.
    ///
    /// Default: 5 minutes (matching Java's `metadata.max.idle.ms`).
    pub fn metadata_topic_cache_ttl(mut self, ttl: Duration) -> Self {
        self.config.metadata_topic_cache_ttl = Some(ttl);
        self
    }

    /// Disable topic cache TTL eviction for partial metadata refreshes.
    ///
    /// By default, cached topics are evicted after 5 minutes to prevent
    /// unbounded growth on topic churn. Call this to opt out of TTL eviction;
    /// entries will then persist across partial refreshes indefinitely.
    pub fn disable_metadata_topic_cache_ttl(mut self) -> Self {
        self.config.metadata_topic_cache_ttl = None;
        self
    }

    /// Enable or disable idempotent production.
    ///
    /// Idempotent production is enabled by default (matching KIP-679 / Kafka 3.0+).
    /// When enabled, the producer obtains a Producer ID from the broker and
    /// attaches sequence numbers to every batch, allowing the broker to
    /// de-duplicate retries **within a single producer session**.
    ///
    /// # Zombie fencing limitation
    ///
    /// Idempotent producers do **not** fence zombie producers. If this producer
    /// crashes and a new instance starts, both may produce to the same partition
    /// concurrently — the broker cannot distinguish them because plain idempotent
    /// producers get a fresh PID on every init (no stable identity across restarts).
    ///
    /// For true zombie fencing and exactly-once semantics across process restarts,
    /// use [`TransactionalProducer`] with
    /// a stable `transactional_id`. The broker uses the `transactional_id` to bump
    /// the producer epoch on each new init, fencing any previous instance with the
    /// same ID. (KIP-360 / Kafka 2.5+)
    ///
    /// Requires `acks = All`. Unlike the Java client there is no
    /// `max.in.flight ≤ 5` rule to observe: the record accumulator keeps
    /// exactly one batch per partition on the wire, so sequence order and wire
    /// order cannot diverge in the first place.
    pub fn idempotent(mut self, enable: bool) -> Self {
        self.config.idempotent = enable;
        self
    }

    /// Set authentication configuration.
    ///
    /// Enables TLS and/or SASL authentication for all broker connections.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use krafka::producer::Producer;
    /// use krafka::auth::AuthConfig;
    ///
    /// let producer = Producer::builder()
    ///     .bootstrap_servers("broker:9093")
    ///     .auth(AuthConfig::sasl_plain("user", "password")?)
    ///     .build()
    ///     .await?;
    /// ```
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = Some(auth);
        self
    }

    /// Configure SASL/PLAIN authentication.
    pub fn sasl_plain(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> crate::Result<Self> {
        self.config.auth = Some(AuthConfig::sasl_plain(username, password)?);
        Ok(self)
    }

    /// Configure SASL/SCRAM-SHA-256 authentication.
    pub fn sasl_scram_sha256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_scram_sha256(username, password));
        self
    }

    /// Configure SASL/SCRAM-SHA-512 authentication.
    pub fn sasl_scram_sha512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_scram_sha512(username, password));
        self
    }

    /// Configure SASL/OAUTHBEARER authentication with a static token.
    ///
    /// For automatic token refresh, use [`sasl_oauthbearer_provider()`](Self::sasl_oauthbearer_provider).
    /// For SASL extensions, use `.auth(AuthConfig::sasl_oauthbearer_token(...))`.
    pub fn sasl_oauthbearer(mut self, token: impl Into<String>) -> Self {
        self.config.auth = Some(AuthConfig::sasl_oauthbearer(token));
        self
    }

    /// Configure SASL/OAUTHBEARER authentication with an async token provider.
    ///
    /// The provider is called on every new broker connection, ensuring
    /// tokens are always fresh.
    pub fn sasl_oauthbearer_provider(
        mut self,
        provider: impl crate::auth::OAuthBearerTokenProvider + 'static,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_oauthbearer_provider(provider));
        self
    }

    /// Set a custom partitioner.
    ///
    /// The partitioner determines which partition a record is sent to when
    /// [`ProducerRecord::partition`] is `None`.
    ///
    /// The default is [`UniformStickyPartitioner`]: murmur2 hash for keyed
    /// records, and a sticky partition for null keys that advances on batch
    /// boundaries. This matches the Java Kafka client's post-KIP-480 default.
    /// [`DefaultPartitioner`] (round-robin for null keys) is the pre-KIP-480
    /// behaviour and is available for explicit opt-in, but it produces one
    /// Produce request per record on topics with many partitions.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use krafka::producer::{Producer, Partitioner, RoundRobinPartitioner};
    ///
    /// let producer = Producer::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .partitioner(RoundRobinPartitioner::new())
    ///     .build()
    ///     .await?;
    /// ```
    pub fn partitioner(mut self, partitioner: impl Partitioner + 'static) -> Self {
        self.partitioner = Some(Arc::new(partitioner));
        self
    }

    /// Set a producer interceptor, replacing any previously added interceptors.
    ///
    /// The interceptor's `on_send` method is called before each record is sent,
    /// and `on_acknowledgement` is called after a send succeeds or fails.
    ///
    /// To register multiple interceptors as an ordered chain, use
    /// [`add_interceptor`](Self::add_interceptor) instead.
    pub fn interceptor(
        mut self,
        interceptor: Arc<dyn crate::interceptor::ProducerInterceptor>,
    ) -> Self {
        self.interceptors = vec![interceptor];
        self
    }

    /// Append a producer interceptor to the chain.
    ///
    /// Interceptors execute in the order they are added. Each interceptor is
    /// individually panic-isolated — a panic in one will not prevent the
    /// remaining interceptors from running.
    ///
    /// For `on_send`, each interceptor sees the record as modified by all
    /// preceding interceptors.
    pub fn add_interceptor(
        mut self,
        interceptor: Arc<dyn crate::interceptor::ProducerInterceptor>,
    ) -> Self {
        self.interceptors.push(interceptor);
        self
    }

    /// Attach a key encoder applied automatically on every [`send_record`](Producer::send_record) call.
    ///
    /// The encoder runs after the interceptor and before partitioning, so the
    /// partitioner sees the Confluent-wire-framed key bytes.
    ///
    /// This is the Rust equivalent of `key.serializer` in the Java
    /// `KafkaProducer`. Configure it once here and encoding is transparent
    /// on every send.
    ///
    /// For per-record subject-name strategies (`RecordName`, `TopicRecordName`),
    /// set [`ProducerRecord::record_name`] (via
    /// [`with_record_name`](ProducerRecord::with_record_name)) on each record
    /// before sending.
    ///
    /// A **null key is not encoded** — it is passed through as `None`, for the
    /// reason given on [`value_serializer`](Self::value_serializer).
    pub fn key_serializer(mut self, encoder: Arc<dyn Serializer>) -> Self {
        self.key_serializer = Some(encoder);
        self
    }

    /// Attach a value encoder applied automatically on every [`send_record`](Producer::send_record) call.
    ///
    /// This is the Rust equivalent of `value.serializer` in the Java
    /// `KafkaProducer`. Configure it once here and encoding is transparent
    /// on every send.
    ///
    /// A **tombstone is not encoded** — a `None` value is passed through as
    /// `None`, since framing it would emit a short record that log compaction
    /// keeps, and the key would never be deleted. The same applies to a null
    /// key, which would otherwise move onto the partitioner's hash path.
    pub fn value_serializer(mut self, encoder: Arc<dyn Serializer>) -> Self {
        self.value_serializer = Some(encoder);
        self
    }

    /// Share a [`KrafkaClient`](crate::client::KrafkaClient)'s connection pool
    /// and metadata cache instead of creating a new one.
    ///
    /// When multiple producers, consumers, or admin clients are created in the
    /// same process you should create a single
    /// [`crate::client::KrafkaClient`] and pass it to each builder. All clients
    /// will then multiplex over the same TCP
    /// connections, reducing the total connection count from `N × brokers` to
    /// `brokers`.
    ///
    /// When this method is called, `bootstrap_servers` is optional on the
    /// builder (the client was already connected at `KrafkaClient::build` time).
    pub fn with_client(mut self, client: &crate::client::KrafkaClient) -> Self {
        self.shared = Some((client.pool().clone(), client.metadata().clone()));
        self
    }

    /// Attach a pluggable state store for producer identity persistence.
    ///
    /// When set:
    /// - `load()` is called once during [`build()`](Self::build). If the
    ///   stored snapshot's `producer_id` and `producer_epoch` match what the
    ///   broker returns from `InitProducerId`, per-partition sequences are
    ///   restored (only meaningful for transactional producers with a stable
    ///   `transactional.id`).
    /// - `store()` is called asynchronously (fire-and-forget) after each
    ///   successful batch acknowledgement. Errors are logged at `warn!` and
    ///   do not fail the produce operation.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use std::sync::Arc;
    /// use krafka::producer::{Producer, ProducerStateStore, ProducerIdentitySnapshot};
    ///
    /// struct MyStore;
    /// impl ProducerStateStore for MyStore {
    ///     async fn load(&self) -> krafka::Result<Option<ProducerIdentitySnapshot>> { Ok(None) }
    ///     async fn store(&self, _: &ProducerIdentitySnapshot) -> krafka::Result<()> { Ok(()) }
    /// }
    ///
    /// let producer = Producer::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .state_store(Arc::new(MyStore))
    ///     .build()
    ///     .await?;
    /// ```
    pub fn state_store(
        mut self,
        store: impl crate::producer::ProducerStateStore + 'static,
    ) -> Self {
        self.state_store = Some(Arc::new(store));
        self
    }

    /// Validate the configuration and return it, without connecting.
    ///
    /// Runs exactly the checks [`build`](Self::build) runs — they call the same
    /// validator — so a config that passes here will not be rejected later for
    /// a configuration reason. Useful for validating settings at startup, in a
    /// test, or in a config-linting tool, none of which want a broker.
    ///
    /// Note that validation also *normalises* — for example clamping a
    /// compression level into the selected codec's range — so the returned
    /// config may differ from what was set.
    ///
    /// # Errors
    ///
    /// Returns [`KrafkaError::Config`] for any invalid combination — an empty
    /// `bootstrap_servers`, a zero `batch_size`, a compression codec whose
    /// Cargo feature is not enabled, `acks != All` with idempotence, and so on.
    pub fn build_config(self) -> Result<ProducerConfig> {
        let has_shared_pool = self.shared.is_some();
        let mut config = self.config;
        config::validate(&mut config, has_shared_pool)?;
        Ok(config)
    }

    /// Build the producer.
    ///
    /// Validates the configuration through the same validator the synchronous
    /// [`build_config`](Self::build_config) uses, then connects.
    ///
    /// # Errors
    ///
    /// Returns [`KrafkaError::Config`] for an invalid configuration, or a
    /// network error if the initial metadata fetch fails.
    pub async fn build(mut self) -> Result<Producer> {
        // One validator, shared with `build_config`. This used to be a second,
        // hand-maintained copy that had silently drifted: it skipped the
        // client-id length limit, the infinite-retry-loop guard and — most
        // visibly — the compression-codec availability checks, so
        // `.compression(Zstd)` without the `zstd` feature built a producer that
        // failed on its first send.
        config::validate(&mut self.config, self.shared.is_some())?;

        let interceptor: Arc<dyn crate::interceptor::ProducerInterceptor> =
            if self.interceptors.is_empty() {
                Arc::new(crate::interceptor::NoOpProducerInterceptor)
            } else if self.interceptors.len() == 1 {
                // infallible: len == 1 guaranteed by the surrounding else-if
                let Some(single) = self.interceptors.into_iter().next() else {
                    unreachable!("len == 1 verified above");
                };
                single
            } else {
                Arc::new(crate::interceptor::ProducerInterceptorChain::new(
                    self.interceptors,
                ))
            };
        let producer = Producer::new(
            self.config,
            interceptor,
            self.partitioner,
            self.key_serializer,
            self.value_serializer,
            self.shared,
            self.state_store,
        )
        .await?;
        Ok(producer)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use crate::metadata::ClusterMetadata;
    use crate::network::{ConnectionConfig, ConnectionPool};
    use crate::protocol::{ProducePartitionData, ProduceTopicData};

    #[test]
    fn test_producer_builder() {
        let builder = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .client_id("test")
            .acks(Acks::All)
            .compression(Compression::Gzip)
            .batch_size(32768)
            .max_request_size(65536)
            .linger(Duration::from_millis(10));

        assert_eq!(builder.config.bootstrap_servers, "localhost:9092");
        assert_eq!(builder.config.client_id, "test");
        assert_eq!(builder.config.acks, Acks::All);
        assert_eq!(builder.config.compression, Compression::Gzip);
        assert_eq!(builder.config.batch_size, 32768);
        assert_eq!(builder.config.max_request_size, 65536);
        assert_eq!(builder.config.linger, Duration::from_millis(10));
        assert!(builder.config.auth.is_none());
    }

    #[test]
    fn test_producer_builder_with_auth() {
        let builder = Producer::builder()
            .bootstrap_servers("broker:9093")
            .auth(AuthConfig::sasl_plain("user", "pass").unwrap());

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(!auth.requires_tls());
        assert_eq!(
            auth.security_protocol,
            crate::auth::SecurityProtocol::SaslPlaintext
        );
        assert_eq!(auth.sasl_mechanism, Some(crate::auth::SaslMechanism::Plain));
    }

    #[test]
    fn test_producer_builder_aws_msk_iam() {
        let auth = AuthConfig::aws_msk_iam("AKID", "secret", "us-east-1");
        let builder = Producer::builder()
            .bootstrap_servers("broker:9094")
            .auth(auth);

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_tls());
        assert!(auth.requires_sasl());
        assert_eq!(
            auth.sasl_mechanism,
            Some(crate::auth::SaslMechanism::AwsMskIam)
        );
        assert!(auth.aws_msk_iam_credentials.is_some());
        assert!(auth.tls_config.is_some());
    }

    #[test]
    fn test_producer_builder_no_auth_by_default() {
        let builder = Producer::builder().bootstrap_servers("broker:9092");

        assert!(builder.config.auth.is_none());
    }

    #[test]
    fn test_producer_builder_sasl_plain() {
        let builder = Producer::builder()
            .bootstrap_servers("broker:9093")
            .sasl_plain("user", "pass")
            .unwrap();

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.plain_credentials.is_some());
    }

    #[test]
    fn test_producer_builder_sasl_scram() {
        let builder = Producer::builder()
            .bootstrap_servers("broker:9093")
            .sasl_scram_sha256("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.scram_credentials.is_some());

        let builder = Producer::builder()
            .bootstrap_servers("broker:9093")
            .sasl_scram_sha512("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.scram_credentials.is_some());
    }

    #[tokio::test]
    async fn test_producer_builder_no_servers() {
        let result = Producer::builder().build().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_producer_builder_retry_config() {
        let builder = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .retries(5)
            .retry_backoff(Duration::from_millis(200));

        assert_eq!(builder.config.retries, 5);
        assert_eq!(builder.config.retry_backoff, Duration::from_millis(200));
    }

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

    /// An `UnknownProducerId` that cannot be resolved in place must no
    /// longer permanently brick the producer.
    ///
    /// The old behaviour poisoned the identity whenever more than one batch was
    /// outstanding, and every subsequent send failed forever. Now a re-initialisation is
    /// requested instead: the send fails and is retriable, and the next
    /// dispatch obtains a fresh PID with all partitions restarted at 0.
    #[tokio::test]
    async fn test_recover_unknown_producer_id_requests_reinit_instead_of_poisoning() {
        let identity = ProducerIdentity::new();
        identity.initialize(7, 1);
        assert_eq!(identity.allocate_sequence("topic", 0, 2).unwrap(), 0);
        assert_eq!(identity.allocate_sequence("topic", 0, 1).unwrap(), 2);

        let pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
        let metadata = ClusterMetadata::new(
            vec!["localhost:9092".to_string()],
            pool,
            Duration::from_secs(300),
        );
        let retry_policy = RetryPolicy::default();

        let error =
            recover_unknown_producer_id(&identity, &metadata, &retry_policy, "topic", 0, 0, 2)
                .await
                .unwrap_err();

        // Reported as a broker UnknownProducerId, which is retriable — not an
        // InvalidState "poisoned, recreate the producer" dead end.
        assert!(
            matches!(
                error,
                KrafkaError::Broker {
                    code: ErrorCode::UnknownProducerId,
                    ..
                }
            ),
            "expected a retriable broker error, got: {error:?}"
        );
        assert!(
            !error.to_string().contains("poisoned"),
            "the producer must not be described as permanently poisoned"
        );

        // A re-init is pending; state is untouched until it is consumed.
        assert!(identity.needs_reinit());
        assert_eq!(identity.producer_id(), 7);
        assert_eq!(identity.peek_sequence("topic", 0), 3);

        // Consuming the request clears the identity so a fresh PID can be
        // fetched and every partition restarts at sequence 0.
        assert!(identity.take_reinit_request());
        assert!(!identity.is_initialized());
        assert_eq!(identity.peek_sequence("topic", 0), 0);

        // And the producer is usable again once re-initialised.
        identity.initialize(8, 0);
        assert!(identity.is_initialized());
        assert_eq!(identity.allocate_sequence("topic", 0, 1).unwrap(), 0);
    }

    #[test]
    fn test_producer_metrics_snapshot() {
        let snapshot = ProducerMetricsSnapshot {
            connections: 3,
            records_sent: 100,
            bytes_sent: 50000,
            errors: 2,
            retries: 5,
            buffered_records: 7,
        };
        assert_eq!(snapshot.connections, 3);
        assert_eq!(snapshot.records_sent, 100);
        assert_eq!(snapshot.bytes_sent, 50000);
        assert_eq!(snapshot.errors, 2);
        assert_eq!(snapshot.retries, 5);
        assert_eq!(snapshot.buffered_records, 7);
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
    fn test_retry_policy_from_config() {
        let policy = RetryPolicy::new()
            .with_max_retries(10)
            .with_initial_backoff(Duration::from_millis(50))
            .with_max_backoff(Duration::from_secs(30));

        assert_eq!(policy.max_retries, 10);
        assert_eq!(policy.initial_backoff(), Duration::from_millis(50));
        assert_eq!(policy.max_backoff(), Duration::from_secs(30));
    }

    #[test]
    fn test_acks_none_returns_fire_and_forget_metadata() {
        // Verify that acks=0 configuration is correctly set.
        // The actual fire-and-forget behavior is tested via integration tests,
        // but we can verify the config pipeline here.
        let builder = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .acks(Acks::None);

        assert_eq!(builder.config.acks, Acks::None);
        assert_eq!(builder.config.acks.to_i16(), 0);
    }

    #[test]
    fn test_idempotent_builder() {
        // Idempotent is enabled by default
        let builder = Producer::builder().bootstrap_servers("broker:9092");
        assert!(builder.config.idempotent);

        // Can be explicitly disabled
        let builder = Producer::builder()
            .bootstrap_servers("broker:9092")
            .idempotent(false);
        assert!(!builder.config.idempotent);
    }

    #[tokio::test]
    async fn test_idempotent_requires_acks_all() {
        let builder = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .acks(Acks::Leader)
            .idempotent(true);

        let result = builder.build().await;
        match result {
            Err(e) => assert!(e.to_string().contains("acks")),
            Ok(_) => panic!("expected config error for idempotent with acks != All"),
        }
    }

    #[tokio::test]
    async fn test_producer_builder_rejects_zero_batch_size() {
        let mut builder = Producer::builder().bootstrap_servers("localhost:9092");
        builder.config.batch_size = 0;
        let result = builder.build().await;

        match result {
            Err(e) => assert!(e.to_string().contains("batch_size")),
            Ok(_) => panic!("expected error for batch_size=0"),
        }
    }

    #[test]
    fn test_producer_builder_interceptor() {
        use crate::interceptor::{InterceptorResult, ProducerInterceptor};

        #[derive(Debug)]
        struct TestInterceptor;
        impl ProducerInterceptor for TestInterceptor {
            fn on_send(
                &self,
                _record: &mut ProducerRecord,
                _ctx: &mut crate::interceptor::RecordContext,
            ) -> InterceptorResult {
                Ok(())
            }
        }

        let builder = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .interceptor(Arc::new(TestInterceptor));

        assert_eq!(builder.interceptors.len(), 1);
    }

    #[test]
    fn test_producer_builder_add_interceptor() {
        use crate::interceptor::ProducerInterceptor;

        #[derive(Debug)]
        struct A;
        impl ProducerInterceptor for A {}

        #[derive(Debug)]
        struct B;
        impl ProducerInterceptor for B {}

        // add_interceptor appends to chain
        let builder = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .add_interceptor(Arc::new(A))
            .add_interceptor(Arc::new(B));
        assert_eq!(builder.interceptors.len(), 2);
    }

    #[test]
    fn test_producer_builder_interceptor_replaces_chain() {
        use crate::interceptor::ProducerInterceptor;

        #[derive(Debug)]
        struct A;
        impl ProducerInterceptor for A {}

        #[derive(Debug)]
        struct B;
        impl ProducerInterceptor for B {}

        // interceptor() replaces any previously added interceptors
        let builder = Producer::builder()
            .bootstrap_servers("localhost:9092")
            .add_interceptor(Arc::new(A))
            .add_interceptor(Arc::new(A))
            .interceptor(Arc::new(B));
        assert_eq!(builder.interceptors.len(), 1);
    }

    #[test]
    fn test_producer_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Producer>();
    }

    /// A 2 s request timeout is only reachable because `connect_timeout` is
    /// settable; the connection layer rejects a request timeout below it.
    #[tokio::test]
    async fn test_a_sub_ten_second_request_timeout_is_reachable() {
        let err = Producer::builder()
            .bootstrap_servers("127.0.0.1:1")
            .request_timeout(Duration::from_secs(2))
            .build()
            .await
            .expect_err("request_timeout below the default connect_timeout must be rejected");
        assert!(
            err.to_string().contains("connect_timeout"),
            "the error should name the setter to change: {err}"
        );

        // Lowering connect_timeout gets past validation; the build then fails
        // only because there is no broker at that address.
        let err = Producer::builder()
            .bootstrap_servers("127.0.0.1:1")
            .request_timeout(Duration::from_secs(2))
            .connect_timeout(Duration::from_secs(2))
            .build()
            .await
            .expect_err("no broker is listening on port 1");
        assert!(
            !err.to_string().contains("connect_timeout"),
            "config validation should have passed, got {err}"
        );
    }

    /// `TransactionVersion` must be nameable by callers: it is the return type
    /// of the public `TransactionalProducer::transaction_version()`, and a type
    /// that cannot be written down cannot be matched on or stored.
    #[test]
    fn test_transaction_version_is_publicly_nameable() {
        let version: crate::producer::TransactionVersion = TransactionVersion::V2;
        assert_eq!(version, TransactionVersion::V2);
    }

    // ── Serializers must not resurrect a null ─────────────────────

    /// A serializer that frames its input the way a schema-registry serializer
    /// does: five bytes of envelope in front of the payload.
    #[derive(Debug)]
    struct Framing;

    impl crate::serdes::Serializer for Framing {
        fn serialize(
            &self,
            payload: Bytes,
            _topic: &str,
            _record_name: Option<&str>,
            _is_key: bool,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Bytes>> + Send + '_>>
        {
            Box::pin(async move {
                let mut out = Vec::with_capacity(5 + payload.len());
                out.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x01]);
                out.extend_from_slice(&payload);
                Ok(Bytes::from(out))
            })
        }
    }

    /// A tombstone must reach the wire null. Framing it would produce a
    /// five-byte record, which compaction treats as ordinary data — the key
    /// would never be deleted, and the caller would have no way to tell.
    #[tokio::test]
    async fn value_serializer_skips_a_tombstone() {
        let framing = Framing;
        let mut record = ProducerRecord::tombstone("users", "user-42");

        apply_serializers(&mut record, None, Some(&framing))
            .await
            .expect("serialization should succeed");

        assert_eq!(record.value, None, "a tombstone must not be framed");
        assert!(record.is_tombstone());
    }

    /// The negative control: an ordinary value *is* framed, so the skip above
    /// is about nullness and not about the serializer being inert.
    #[tokio::test]
    async fn value_serializer_runs_on_a_present_value() {
        let framing = Framing;
        let mut record = ProducerRecord::new("users", b"v".to_vec());

        apply_serializers(&mut record, None, Some(&framing))
            .await
            .expect("serialization should succeed");

        assert_eq!(
            record.value,
            Some(Bytes::from_static(&[0x00, 0x00, 0x00, 0x00, 0x01, b'v']))
        );
    }

    /// A null key must stay null. Serializing it into an empty key moves the
    /// record off the partitioner's keyless path — every keyless record would
    /// hash to the same partition instead of being spread.
    #[tokio::test]
    async fn key_serializer_skips_a_null_key() {
        let framing = Framing;
        let mut record = ProducerRecord::new("users", b"v".to_vec());

        apply_serializers(&mut record, Some(&framing), None)
            .await
            .expect("serialization should succeed");

        assert_eq!(record.key, None, "a null key must not become an empty key");
    }

    /// The negative control for the key path.
    #[tokio::test]
    async fn key_serializer_runs_on_a_present_key() {
        let framing = Framing;
        let mut record = ProducerRecord::new("users", b"v".to_vec()).with_key(b"k".to_vec());

        apply_serializers(&mut record, Some(&framing), None)
            .await
            .expect("serialization should succeed");

        assert_eq!(
            record.key,
            Some(Bytes::from_static(&[0x00, 0x00, 0x00, 0x00, 0x01, b'k']))
        );
    }

    /// `build_record` must carry `None` through as Kafka's null rather than
    /// widening it to an empty slice.
    #[test]
    fn build_record_preserves_null_key_and_value() {
        let tombstone = build_record("t", Some(b"k"), None);
        assert_eq!(tombstone.value, None);
        assert!(tombstone.is_tombstone());

        let keyless = build_record("t", None, Some(b"v"));
        assert_eq!(keyless.key, None);

        let empty = build_record("t", Some(b"k"), Some(b""));
        assert_eq!(empty.value, Some(Bytes::new()), "empty is not null");
        assert!(!empty.is_tombstone());
    }
}
