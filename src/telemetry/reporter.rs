//! KIP-714 background telemetry reporter.
//!
//! The [`TelemetryReporter`] runs as a Tokio task that:
//!
//! 1. Sends `GetTelemetrySubscriptions` to obtain a `client_instance_id`,
//!    subscription ID, push interval, and the broker's metric preferences.
//! 2. Periodically collects metrics from the client's [`KrafkaMetrics`]
//!    registry, serialises them as OTLP protobuf, and sends a
//!    `PushTelemetry` request.
//! 3. On shutdown (via the cancellation token), sends a final push with
//!    `terminating = true`.
//!
//! The reporter prefers an existing broker connection and sticks to the
//! same broker for the lifetime of the subscription, switching only when
//! the connection drops (per KIP-714 § Connection Selection).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use rand::Rng;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::error::ErrorCode;
use crate::metrics::{KrafkaMetrics, LatencySnapshot, MetricsExporter};
use crate::network::{BrokerConnection, ConnectionPool};
use crate::protocol::{
    ApiKey, Compression, GetTelemetrySubscriptionsRequest, GetTelemetrySubscriptionsResponse,
    PushTelemetryRequest, PushTelemetryResponse, VersionedDecode,
};

use super::otlp::OtlpExporter;

/// Maximum retry attempts for transient failures (subscription / push).
const MAX_RETRIES: u32 = 3;

/// Base backoff duration for retries.
const RETRY_BACKOFF_BASE: Duration = Duration::from_secs(1);

/// Minimum telemetry push interval accepted from the broker.
const MIN_PUSH_INTERVAL_MS: i32 = 100;

/// Maximum telemetry push interval accepted from the broker.
const MAX_PUSH_INTERVAL_MS: i32 = 60 * 60 * 1000;

fn retry_backoff(attempt: u32) -> Duration {
    debug_assert!(attempt > 0, "retry_backoff expects attempts starting at 1");
    RETRY_BACKOFF_BASE * 2u32.saturating_pow(attempt.saturating_sub(1))
}

fn clamp_push_interval_ms(raw_ms: i32) -> i32 {
    raw_ms.clamp(MIN_PUSH_INTERVAL_MS, MAX_PUSH_INTERVAL_MS)
}

/// Configuration for the telemetry reporter.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TelemetryConfig {
    /// Whether to enable telemetry push. Corresponds to KIP-714's
    /// `enable.metrics.push` (default: `true`).
    pub enabled: bool,
    /// Metric name prefix used when serialising to OTLP.
    pub metrics_prefix: String,
    /// Resource attributes to attach to every OTLP payload
    /// (e.g., `("client_rack", "us-east-1a")`).
    ///
    /// **Privacy**: these key-value pairs are sent to the broker verbatim
    /// on every telemetry push. Do not include personally identifiable
    /// information (PII) such as `user_id`, `email`, or `ip_address`.
    pub resource_attributes: Vec<(String, String)>,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            metrics_prefix: "org.apache.kafka".to_string(),
            resource_attributes: Vec::new(),
        }
    }
}

/// Active subscription state obtained from the broker.
#[derive(Debug, Clone)]
struct Subscription {
    /// Broker-assigned client instance ID (UUID bytes).
    client_instance_id: [u8; 16],
    /// CRC32C-based subscription identifier.
    subscription_id: i32,
    /// Broker-requested push interval.
    push_interval: Duration,
    /// Whether counters should use delta temporality.
    delta_temporality: bool,
    /// Broker-advertised telemetry compression codecs, ordered by preference.
    accepted_compression_types: Vec<Compression>,
    /// Maximum payload size the broker accepts.
    telemetry_max_bytes: i32,
    /// Metric name prefix patterns the broker subscribes to.
    /// Empty means no metrics desired (but keep polling).
    /// A single `"*"` entry means all metrics.
    requested_metrics: Vec<String>,
}

impl Subscription {
    /// Returns `true` if any metrics should be emitted for this subscription.
    fn has_metrics(&self) -> bool {
        !self.requested_metrics.is_empty()
    }

    /// Returns `true` if all metrics are requested (wildcard `"*"`).
    fn wants_all_metrics(&self) -> bool {
        self.requested_metrics.len() == 1 && self.requested_metrics[0] == "*"
    }
}

#[derive(Debug, Clone)]
enum CollectedMetricEntry {
    Counter {
        name: String,
        help: String,
        value: u64,
    },
    Gauge {
        name: String,
        help: String,
        value: u64,
    },
    Latency {
        name: String,
        help: String,
        snapshot: LatencySnapshot,
    },
}

#[derive(Debug, Default)]
struct CollectingExporter {
    entries: Vec<CollectedMetricEntry>,
}

impl MetricsExporter for CollectingExporter {
    fn export_counter(&mut self, name: &str, help: &str, value: u64) {
        self.entries.push(CollectedMetricEntry::Counter {
            name: name.to_string(),
            help: help.to_string(),
            value,
        });
    }

    fn export_gauge(&mut self, name: &str, help: &str, value: u64) {
        self.entries.push(CollectedMetricEntry::Gauge {
            name: name.to_string(),
            help: help.to_string(),
            value,
        });
    }

    fn export_latency(&mut self, name: &str, help: &str, snapshot: &LatencySnapshot) {
        self.entries.push(CollectedMetricEntry::Latency {
            name: name.to_string(),
            help: help.to_string(),
            snapshot: snapshot.clone(),
        });
    }
}

#[derive(Debug)]
struct PreparedMetric {
    bytes: Vec<u8>,
    counter_update: Option<(String, u64)>,
}

#[derive(Debug)]
struct PreparedTelemetryChunk {
    metric_bytes: Vec<Vec<u8>>,
    counter_updates: Vec<(String, u64)>,
}

#[derive(Debug)]
struct PendingPushWindow {
    window_start_nanos: u64,
    chunks: Vec<PreparedTelemetryChunk>,
}

struct ChunkPreparationContext<'a> {
    subscription: &'a Subscription,
    resource_attributes: &'a [(String, String)],
    max_bytes: usize,
    prefer_uncompressed_chunking: bool,
    can_bound_encoded_payload_by_uncompressed: bool,
    unsupported_compression_types: &'a mut HashSet<Compression>,
}

#[derive(Default)]
struct ChunkBuilderState {
    metric_bytes: Vec<Vec<u8>>,
    counter_updates: Vec<(String, u64)>,
    metric_entries_len: usize,
}

#[derive(Debug)]
enum TelemetryChunkingError {
    SingleMetricTooLarge {
        payload_bytes: usize,
        max_bytes: usize,
    },
    NoUsableCompressionCodec {
        accepted_compression_types: Vec<Compression>,
    },
}

/// A background KIP-714 telemetry reporter.
///
/// Create via [`TelemetryReporter::new`] and spawn with [`TelemetryReporter::run`].
/// The task runs until the shutdown signal is sent.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use krafka::metrics::KrafkaMetrics;
/// use krafka::telemetry::reporter::{TelemetryReporter, TelemetryConfig};
/// use krafka::network::BrokerConnection;
///
/// # async fn example(conn: Arc<BrokerConnection>) {
/// let metrics = Arc::new(KrafkaMetrics::new());
/// let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
///
/// let reporter = TelemetryReporter::new(
///     conn,
///     metrics,
///     TelemetryConfig::default(),
///     shutdown_rx,
/// );
///
/// // Spawn the reporter as a background task
/// let handle = tokio::spawn(reporter.run());
///
/// // ... later, trigger shutdown ...
/// let _ = shutdown_tx.send(true);
/// let _ = handle.await;
/// # }
/// ```
pub struct TelemetryReporter {
    connection: Arc<BrokerConnection>,
    /// Connection pool — used to reconnect when `connection` drops.
    pool: Arc<ConnectionPool>,
    /// Broker addresses to try when reconnecting, in preference order.
    broker_addresses: Vec<String>,
    metrics: Arc<KrafkaMetrics>,
    config: TelemetryConfig,
    shutdown: watch::Receiver<bool>,
    /// Tracks previous counter values for KIP-714 delta temporality.
    delta_tracker: DeltaTracker,
    /// Compression codecs that failed locally and should be skipped.
    unsupported_compression_types: HashSet<Compression>,
    /// Last observed `delta_temporality` flag — reset tracker on change.
    last_delta_temporality: bool,
    /// Remaining chunks from a partially accepted collection window.
    pending_push_window: Option<PendingPushWindow>,
}

impl TelemetryReporter {
    /// Create a new telemetry reporter.
    ///
    /// * `connection` — initial broker connection to use for telemetry RPCs.
    /// * `pool` — connection pool for reconnection when the current broker drops.
    /// * `broker_addresses` — ordered list of broker addresses to try on reconnect.
    /// * `metrics` — the shared metrics registry to read from.
    /// * `config` — telemetry configuration.
    /// * `shutdown` — a watch channel; set to `true` to stop the reporter.
    pub fn new(
        connection: Arc<BrokerConnection>,
        pool: Arc<ConnectionPool>,
        broker_addresses: Vec<String>,
        metrics: Arc<KrafkaMetrics>,
        config: TelemetryConfig,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            connection,
            pool,
            broker_addresses,
            metrics,
            config,
            shutdown,
            delta_tracker: DeltaTracker::new(),
            unsupported_compression_types: HashSet::new(),
            last_delta_temporality: false,
            pending_push_window: None,
        }
    }

    /// Run the reporter loop. Returns when the shutdown signal is received
    /// (after sending the terminating push).
    pub async fn run(mut self) {
        if !self.config.enabled {
            debug!("Telemetry push disabled by configuration");
            return;
        }

        info!("KIP-714 telemetry reporter starting");

        // Step 1: Get initial subscription (with null UUID) — retry on transient errors
        let mut subscription = match self.get_subscription_with_retry([0u8; 16]).await {
            Some(s) => s,
            None => {
                warn!("Failed to obtain telemetry subscription after retries; reporter exiting");
                return;
            }
        };

        info!(
            client_instance_id = ?subscription.client_instance_id,
            push_interval_ms = subscription.push_interval.as_millis(),
            requested_metrics = ?subscription.requested_metrics,
            "Telemetry subscription acquired"
        );

        // KIP-714: First push randomised between 0.5 × interval … 1.5 × interval.
        let jitter_factor: f64 = rand::rng().random_range(0.5..1.5);
        let first_delay = subscription.push_interval.mul_f64(jitter_factor);
        let collection_start = Self::nanos_since_epoch();

        // Wait for first interval (or shutdown)
        if self.wait_or_shutdown(first_delay).await {
            self.send_terminating_push(&subscription, collection_start)
                .await;
            return;
        }

        // Main push loop
        let mut window_start = collection_start;
        loop {
            // KIP-714: only push when the broker has subscribed to metrics.
            // When requested_metrics is empty, skip the push entirely and
            // re-poll for subscription changes after the interval.
            let had_metrics = subscription.has_metrics();
            let mut push_result = None;
            if had_metrics {
                let result = self.push_metrics(&subscription, window_start, false).await;
                match result {
                    PushResult::Ok => {}
                    PushResult::ReSubscribe => {
                        debug!("Subscription invalidated; re-subscribing");
                        let preserved_pending_window = self.pending_push_window.is_some();
                        match self
                            .get_subscription_with_retry(subscription.client_instance_id)
                            .await
                        {
                            Some(s) => {
                                if preserved_pending_window
                                    && !can_reuse_pending_window(&subscription, &s)
                                {
                                    debug!(
                                        "Telemetry subscription changed; dropping preserved pending window"
                                    );
                                    self.pending_push_window = None;
                                    self.delta_tracker.reset();
                                    window_start = Self::nanos_since_epoch();
                                } else if !preserved_pending_window {
                                    self.delta_tracker.reset();
                                }
                                subscription = s;
                            }
                            None => {
                                warn!("Re-subscription failed after retries; reporter exiting");
                                return;
                            }
                        }
                    }
                    PushResult::Transient => {
                        // Logged already; we'll just retry on the next interval.
                    }
                    PushResult::Throttled => {
                        // The broker did not accept this interval's payload.
                        // Preserve the collection window so the next interval
                        // retries the same telemetry slice.
                    }
                    PushResult::Fatal => {
                        warn!("Fatal telemetry push error; attempting reconnection");
                        if self.reconnect().await {
                            match self
                                .get_subscription_with_retry(subscription.client_instance_id)
                                .await
                            {
                                Some(s) => {
                                    self.delta_tracker.reset();
                                    subscription = s;
                                }
                                None => {
                                    warn!(
                                        "Re-subscription failed after reconnect; reporter exiting"
                                    );
                                    return;
                                }
                            }
                        } else {
                            warn!("All broker connections failed; reporter exiting");
                            return;
                        }
                    }
                }
                push_result = Some(result);
            }

            if should_advance_window(had_metrics, push_result) {
                window_start = Self::nanos_since_epoch();
            }

            // Wait for next interval (or shutdown)
            if self.wait_or_shutdown(subscription.push_interval).await {
                self.send_terminating_push(&subscription, window_start)
                    .await;
                return;
            }

            // KIP-714 § empty RequestedMetrics: re-poll subscription for changes
            // so the reporter picks up new subscriptions without a client restart.
            if !subscription.has_metrics() {
                debug!("No metrics subscribed; re-checking subscription");
                match self
                    .get_subscription_with_retry(subscription.client_instance_id)
                    .await
                {
                    Some(s) => {
                        self.delta_tracker.reset();
                        subscription = s;
                    }
                    None => {
                        warn!("Re-subscription failed after retries; reporter exiting");
                        return;
                    }
                }
            }
        }
    }

    /// Try to reconnect to any known broker after a fatal connection error.
    ///
    /// Iterates `broker_addresses` in order and replaces `self.connection` on
    /// the first success. Returns `true` if reconnected, `false` if all fail.
    async fn reconnect(&mut self) -> bool {
        for addr in &self.broker_addresses.clone() {
            match self.pool.get_connection(addr).await {
                Ok(conn) => {
                    info!(broker = %addr, "Telemetry reporter reconnected to broker");
                    self.connection = conn;
                    // Reset per-connection state on reconnect.
                    self.unsupported_compression_types.clear();
                    return true;
                }
                Err(err) => {
                    warn!(broker = %addr, %err, "Telemetry reconnection attempt failed");
                }
            }
        }
        false
    }

    /// Try to obtain a subscription, retrying transient failures with backoff.
    async fn get_subscription_with_retry(
        &mut self,
        client_instance_id: [u8; 16],
    ) -> Option<Subscription> {
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff = retry_backoff(attempt);
                debug!(
                    attempt,
                    backoff_ms = backoff.as_millis(),
                    "Retrying GetTelemetrySubscriptions"
                );
                if self.wait_or_shutdown(backoff).await {
                    return None; // shutdown requested
                }
            }

            match self.get_subscription(client_instance_id).await {
                SubscriptionResult::Ok(sub) => return Some(sub),
                SubscriptionResult::Transient => continue,
                SubscriptionResult::Fatal => return None,
            }
        }

        None
    }

    /// Send `GetTelemetrySubscriptions` and parse the response.
    async fn get_subscription(&self, client_instance_id: [u8; 16]) -> SubscriptionResult {
        let req = GetTelemetrySubscriptionsRequest { client_instance_id };

        let response_bytes: Bytes = match self
            .connection
            .send_request(ApiKey::GetTelemetrySubscriptions, 0, |buf| {
                req.encode_v0(buf)
            })
            .await
        {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "GetTelemetrySubscriptions request failed");
                return SubscriptionResult::Transient;
            }
        };

        let resp = match GetTelemetrySubscriptionsResponse::decode_versioned(
            0,
            &mut response_bytes.as_ref(),
        ) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Failed to decode GetTelemetrySubscriptionsResponse");
                return SubscriptionResult::Transient;
            }
        };

        if resp.throttle_time_ms > 0 {
            debug!(throttle_ms = resp.throttle_time_ms, "Throttled by broker");
        }

        if resp.error_code != ErrorCode::None {
            warn!(
                error_code = ?resp.error_code,
                "GetTelemetrySubscriptions returned error"
            );
            return if resp.error_code.is_retriable() {
                SubscriptionResult::Transient
            } else {
                SubscriptionResult::Fatal
            };
        }

        let clamped_push_interval_ms = clamp_push_interval_ms(resp.push_interval_ms);
        if clamped_push_interval_ms != resp.push_interval_ms {
            debug!(
                raw_push_interval_ms = resp.push_interval_ms,
                clamped_push_interval_ms,
                "Clamped broker telemetry push interval to supported bounds"
            );
        }
        let push_interval = Duration::from_millis(clamped_push_interval_ms as u64);

        let effective_id = if client_instance_id == [0u8; 16] {
            resp.client_instance_id
        } else {
            client_instance_id
        };

        SubscriptionResult::Ok(Subscription {
            client_instance_id: effective_id,
            subscription_id: resp.subscription_id,
            push_interval,
            delta_temporality: resp.delta_temporality,
            accepted_compression_types: Self::accepted_telemetry_compression_types(
                &resp.accepted_compression_types,
            ),
            telemetry_max_bytes: resp.telemetry_max_bytes,
            requested_metrics: resp.requested_metrics,
        })
    }

    fn accepted_telemetry_compression_types(raw_types: &[i8]) -> Vec<Compression> {
        let mut codecs = Vec::with_capacity(raw_types.len());
        for raw in raw_types {
            match Compression::from_i8(*raw) {
                Some(codec) => codecs.push(codec),
                None => {
                    warn!(
                        compression_type = *raw,
                        "Ignoring unknown telemetry compression type advertised by broker"
                    );
                }
            }
        }
        codecs
    }

    /// Collect metrics and send `PushTelemetry`.
    async fn push_metrics(
        &mut self,
        subscription: &Subscription,
        window_start_nanos: u64,
        terminating: bool,
    ) -> PushResult {
        let chunks = if let Some(pending_window) = self.take_pending_push_window(window_start_nanos)
        {
            pending_window.chunks
        } else {
            if subscription.delta_temporality != self.last_delta_temporality {
                debug!(
                    old = self.last_delta_temporality,
                    new = subscription.delta_temporality,
                    "Delta temporality changed; resetting tracker"
                );
                self.delta_tracker.reset();
                self.last_delta_temporality = subscription.delta_temporality;
            }

            let entries = self.collect_metrics(subscription);
            let push_time_nanos = Self::nanos_since_epoch();

            match Self::prepare_push_chunks(
                subscription,
                window_start_nanos,
                push_time_nanos,
                &entries,
                &self.config.resource_attributes,
                &self.delta_tracker,
                &mut self.unsupported_compression_types,
            ) {
                Ok(chunks) => chunks,
                Err(TelemetryChunkingError::SingleMetricTooLarge {
                    payload_bytes,
                    max_bytes,
                }) => {
                    warn!(
                        payload_bytes,
                        max_bytes,
                        "Telemetry payload chunk exceeds broker TelemetryMaxBytes; re-subscribing"
                    );
                    return PushResult::ReSubscribe;
                }
                Err(TelemetryChunkingError::NoUsableCompressionCodec {
                    accepted_compression_types,
                }) => {
                    warn!(
                        ?accepted_compression_types,
                        "Broker advertised no telemetry compression codec that is locally usable; stopping telemetry reporter"
                    );
                    return PushResult::Fatal;
                }
            }
        };

        let chunk_count = chunks.len();
        let mut committed_counter_updates = Vec::new();
        let mut chunk_iter = chunks.into_iter().enumerate();

        while let Some((index, chunk)) = chunk_iter.next() {
            let chunk_terminating = terminating && index + 1 == chunk_count;
            let (payload, compression) = match Self::encode_prepared_chunk(
                subscription,
                &self.config.resource_attributes,
                &chunk,
                &mut self.unsupported_compression_types,
            ) {
                Ok(encoded) => encoded,
                Err(TelemetryChunkingError::SingleMetricTooLarge {
                    payload_bytes,
                    max_bytes,
                }) => {
                    warn!(
                        payload_bytes,
                        max_bytes,
                        "Telemetry payload chunk exceeds broker TelemetryMaxBytes; re-subscribing"
                    );
                    return PushResult::ReSubscribe;
                }
                Err(TelemetryChunkingError::NoUsableCompressionCodec {
                    accepted_compression_types,
                }) => {
                    warn!(
                        ?accepted_compression_types,
                        "Broker advertised no telemetry compression codec that is locally usable; stopping telemetry reporter"
                    );
                    return PushResult::Fatal;
                }
            };

            let chunk_result = self
                .push_payload_with_retry(subscription, chunk_terminating, payload, compression)
                .await;
            if chunk_result != PushResult::Ok {
                self.delta_tracker
                    .commit_updates(&committed_counter_updates);
                if should_preserve_pending_window(chunk_result) {
                    let mut remaining_chunks = vec![chunk];
                    remaining_chunks.extend(chunk_iter.map(|(_, pending_chunk)| pending_chunk));
                    self.pending_push_window = Some(PendingPushWindow {
                        window_start_nanos,
                        chunks: remaining_chunks,
                    });
                }
                return chunk_result;
            }
            committed_counter_updates.extend(chunk.counter_updates);
        }

        self.delta_tracker
            .commit_updates(&committed_counter_updates);

        PushResult::Ok
    }

    fn take_pending_push_window(&mut self, window_start_nanos: u64) -> Option<PendingPushWindow> {
        match self.pending_push_window.take() {
            Some(pending_window) if pending_window.window_start_nanos == window_start_nanos => {
                Some(pending_window)
            }
            Some(pending_window) => {
                self.pending_push_window = Some(pending_window);
                None
            }
            None => None,
        }
    }

    async fn push_payload_once(
        &mut self,
        subscription: &Subscription,
        terminating: bool,
        payload: Vec<u8>,
        compression: Compression,
    ) -> PushResult {
        let req = PushTelemetryRequest {
            client_instance_id: subscription.client_instance_id,
            subscription_id: subscription.subscription_id,
            terminating,
            compression_type: compression as i8,
            metrics: Bytes::from(payload),
        };

        let response_bytes: Bytes = match self
            .connection
            .send_request(ApiKey::PushTelemetry, 0, |buf| req.encode_v0(buf))
            .await
        {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "PushTelemetry request failed (transient)");
                return PushResult::Transient;
            }
        };

        let resp = match PushTelemetryResponse::decode_versioned(0, &mut response_bytes.as_ref()) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Failed to decode PushTelemetryResponse");
                return PushResult::Transient;
            }
        };

        if resp.throttle_time_ms > 0 {
            debug!(
                throttle_ms = resp.throttle_time_ms,
                "PushTelemetry throttled"
            );
        }

        match resp.error_code {
            ErrorCode::None => {
                debug!(
                    terminating,
                    payload_bytes = req.metrics.len(),
                    "PushTelemetry accepted"
                );
                PushResult::Ok
            }
            ErrorCode::UnknownSubscriptionId => {
                debug!("Broker returned UNKNOWN_SUBSCRIPTION_ID");
                PushResult::ReSubscribe
            }
            ErrorCode::UnsupportedCompressionType => {
                debug!("Broker returned UNSUPPORTED_COMPRESSION_TYPE");
                PushResult::ReSubscribe
            }
            ErrorCode::TelemetryTooLarge => {
                warn!(
                    payload_bytes = req.metrics.len(),
                    "Broker returned TELEMETRY_TOO_LARGE; re-subscribing for updated limits"
                );
                PushResult::ReSubscribe
            }
            ErrorCode::InvalidRequest | ErrorCode::InvalidRecord => {
                warn!(
                    error_code = ?resp.error_code,
                    "PushTelemetry rejected with non-retriable error; stopping"
                );
                PushResult::Fatal
            }
            ErrorCode::ThrottlingQuotaExceeded => {
                debug!("PushTelemetry throttled; will retry next interval");
                PushResult::Throttled
            }
            other => {
                warn!(error_code = ?other, "PushTelemetry returned unexpected error");
                PushResult::Transient
            }
        }
    }

    /// Retry a single telemetry payload chunk with the same bounded exponential
    /// backoff used for subscription acquisition.
    async fn push_payload_with_retry(
        &mut self,
        subscription: &Subscription,
        terminating: bool,
        payload: Vec<u8>,
        compression: Compression,
    ) -> PushResult {
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff = retry_backoff(attempt);
                debug!(
                    attempt,
                    backoff_ms = backoff.as_millis(),
                    terminating,
                    "Retrying PushTelemetry chunk"
                );
                if self.wait_or_shutdown(backoff).await {
                    return PushResult::Transient;
                }
            }

            match self
                .push_payload_once(subscription, terminating, payload.clone(), compression)
                .await
            {
                PushResult::Transient if attempt < MAX_RETRIES => continue,
                result => return result,
            }
        }

        PushResult::Transient
    }

    fn collect_metrics(&self, subscription: &Subscription) -> Vec<CollectedMetricEntry> {
        let mut collector = CollectingExporter::default();

        if subscription.has_metrics() {
            if subscription.wants_all_metrics() {
                self.metrics
                    .export_all_with_prefix(&self.config.metrics_prefix, &mut collector);
            } else {
                let mut filter =
                    PrefixFilterExporter::new(&subscription.requested_metrics, &mut collector);
                self.metrics
                    .export_all_with_prefix(&self.config.metrics_prefix, &mut filter);
            }
        }

        collector.entries
    }

    fn build_payload_from_metrics(
        resource_attributes: &[(String, String)],
        metric_bytes: &[Vec<u8>],
    ) -> Vec<u8> {
        let mut exporter = OtlpExporter::new(false, 0);
        for (k, v) in resource_attributes {
            exporter.add_resource_attribute(k.as_str(), v.as_str());
        }
        for metric in metric_bytes {
            exporter.push_metric_bytes(metric.clone());
        }
        exporter.finish()
    }

    fn choose_compression(
        subscription: &Subscription,
        unsupported_compression_types: &mut HashSet<Compression>,
        payload: &[u8],
    ) -> Result<(Vec<u8>, Compression), TelemetryChunkingError> {
        let mut fallback_compression = None;

        for compression in &subscription.accepted_compression_types {
            if *compression == Compression::None {
                return Ok((payload.to_vec(), Compression::None));
            }

            if unsupported_compression_types.contains(compression) {
                continue;
            }

            if !compression.is_available() {
                unsupported_compression_types.insert(*compression);
                continue;
            }

            match compression.compress(payload) {
                Ok(compressed) => {
                    if compressed.len() >= payload.len() {
                        if fallback_compression.is_none() {
                            fallback_compression = Some((compressed.to_vec(), *compression));
                        }
                        continue;
                    }
                    debug!(
                        ?compression,
                        original_payload_bytes = payload.len(),
                        compressed_payload_bytes = compressed.len(),
                        "Compressed telemetry payload"
                    );
                    return Ok((compressed.to_vec(), *compression));
                }
                Err(error) => {
                    unsupported_compression_types.insert(*compression);
                    warn!(
                        ?compression,
                        error = %error,
                        "Telemetry compression failed locally; falling back to the next supported codec"
                    );
                }
            }
        }

        if let Some((compressed, compression)) = fallback_compression {
            debug!(
                ?compression,
                original_payload_bytes = payload.len(),
                compressed_payload_bytes = compressed.len(),
                "Using broker-advertised telemetry compression despite no size reduction"
            );
            return Ok((compressed, compression));
        }

        Err(TelemetryChunkingError::NoUsableCompressionCodec {
            accepted_compression_types: subscription.accepted_compression_types.clone(),
        })
    }

    fn encode_payload(
        subscription: &Subscription,
        resource_attributes: &[(String, String)],
        metric_bytes: &[Vec<u8>],
        unsupported_compression_types: &mut HashSet<Compression>,
    ) -> Result<(Vec<u8>, Compression), TelemetryChunkingError> {
        let payload = Self::build_payload_from_metrics(resource_attributes, metric_bytes);
        Self::choose_compression(subscription, unsupported_compression_types, &payload)
    }

    fn encode_prepared_chunk(
        subscription: &Subscription,
        resource_attributes: &[(String, String)],
        chunk: &PreparedTelemetryChunk,
        unsupported_compression_types: &mut HashSet<Compression>,
    ) -> Result<(Vec<u8>, Compression), TelemetryChunkingError> {
        Self::encode_payload(
            subscription,
            resource_attributes,
            &chunk.metric_bytes,
            unsupported_compression_types,
        )
    }

    fn varint_len(mut value: usize) -> usize {
        let mut len = 1;
        while value >= 0x80 {
            value >>= 7;
            len += 1;
        }
        len
    }

    fn len_delimited_field_len(payload_len: usize) -> usize {
        1 + Self::varint_len(payload_len) + payload_len
    }

    fn string_field_len(value: &str) -> usize {
        if value.is_empty() {
            0
        } else {
            Self::len_delimited_field_len(value.len())
        }
    }

    fn resource_attributes_payload_len(resource_attributes: &[(String, String)]) -> usize {
        resource_attributes
            .iter()
            .map(|(key, value)| {
                let any_value_len = Self::string_field_len(value);
                let key_value_len =
                    Self::string_field_len(key) + Self::len_delimited_field_len(any_value_len);
                Self::len_delimited_field_len(key_value_len)
            })
            .sum()
    }

    fn uncompressed_payload_len(
        resource_attributes: &[(String, String)],
        metric_entries_len: usize,
    ) -> usize {
        let scope_len =
            Self::string_field_len("krafka") + Self::string_field_len(env!("CARGO_PKG_VERSION"));
        let scope_metrics_len = Self::len_delimited_field_len(scope_len) + metric_entries_len;

        let mut resource_metrics_len = Self::len_delimited_field_len(scope_metrics_len);
        let resource_len = Self::resource_attributes_payload_len(resource_attributes);
        if resource_len > 0 {
            resource_metrics_len += Self::len_delimited_field_len(resource_len);
        }

        Self::len_delimited_field_len(resource_metrics_len)
    }

    fn take_current_chunk(current_chunk: &mut ChunkBuilderState) -> PreparedTelemetryChunk {
        PreparedTelemetryChunk {
            metric_bytes: std::mem::take(&mut current_chunk.metric_bytes),
            counter_updates: std::mem::take(&mut current_chunk.counter_updates),
        }
    }

    fn start_chunk_with_metric(
        context: &mut ChunkPreparationContext<'_>,
        current_chunk: &mut ChunkBuilderState,
        bytes: Vec<u8>,
        counter_update: Option<(String, u64)>,
    ) -> Result<Option<PreparedTelemetryChunk>, TelemetryChunkingError> {
        let metric_entry_len = Self::len_delimited_field_len(bytes.len());
        current_chunk.metric_bytes.push(bytes);
        current_chunk.metric_entries_len = metric_entry_len;

        if context.can_bound_encoded_payload_by_uncompressed
            && Self::uncompressed_payload_len(context.resource_attributes, metric_entry_len)
                <= context.max_bytes
        {
            current_chunk.counter_updates.extend(counter_update);
            return Ok(None);
        }

        let single_metric_chunk = PreparedTelemetryChunk {
            metric_bytes: std::mem::take(&mut current_chunk.metric_bytes),
            counter_updates: counter_update.into_iter().collect(),
        };
        let (payload, _) = Self::encode_prepared_chunk(
            context.subscription,
            context.resource_attributes,
            &single_metric_chunk,
            context.unsupported_compression_types,
        )?;
        if payload.len() > context.max_bytes {
            return Err(TelemetryChunkingError::SingleMetricTooLarge {
                payload_bytes: payload.len(),
                max_bytes: context.max_bytes,
            });
        }

        if context.prefer_uncompressed_chunking {
            current_chunk.metric_entries_len = 0;
            return Ok(Some(single_metric_chunk));
        }

        current_chunk.metric_entries_len = metric_entry_len;
        current_chunk.metric_bytes = single_metric_chunk.metric_bytes;
        current_chunk.counter_updates = single_metric_chunk.counter_updates;
        Ok(None)
    }

    fn start_chunk_with_metric_or_skip(
        context: &mut ChunkPreparationContext<'_>,
        current_chunk: &mut ChunkBuilderState,
        chunks: &mut Vec<PreparedTelemetryChunk>,
        bytes: Vec<u8>,
        counter_update: Option<(String, u64)>,
    ) -> Result<(), TelemetryChunkingError> {
        match Self::start_chunk_with_metric(context, current_chunk, bytes, counter_update) {
            Ok(Some(single_metric_chunk)) => chunks.push(single_metric_chunk),
            Ok(None) => {}
            Err(TelemetryChunkingError::SingleMetricTooLarge {
                payload_bytes,
                max_bytes,
            }) => {
                current_chunk.metric_entries_len = 0;
                current_chunk.metric_bytes.clear();
                current_chunk.counter_updates.clear();
                warn!(
                    payload_bytes,
                    max_bytes, "Skipping telemetry metric that exceeds broker TelemetryMaxBytes"
                );
            }
            Err(error @ TelemetryChunkingError::NoUsableCompressionCodec { .. }) => {
                return Err(error);
            }
        }

        Ok(())
    }

    fn prepare_push_chunks(
        subscription: &Subscription,
        start_time_nanos: u64,
        time_nanos: u64,
        entries: &[CollectedMetricEntry],
        resource_attributes: &[(String, String)],
        delta_tracker: &DeltaTracker,
        unsupported_compression_types: &mut HashSet<Compression>,
    ) -> Result<Vec<PreparedTelemetryChunk>, TelemetryChunkingError> {
        let mut prepared_metrics = Vec::new();
        for entry in entries {
            prepared_metrics.extend(entry.encode(
                subscription.delta_temporality,
                start_time_nanos,
                time_nanos,
                delta_tracker,
            ));
        }

        let max_bytes = if subscription.telemetry_max_bytes > 0 {
            subscription.telemetry_max_bytes as usize
        } else {
            usize::MAX
        };
        let prefer_uncompressed_chunking = prefers_uncompressed_chunking(subscription);
        let can_bound_encoded_payload_by_uncompressed =
            supports_uncompressed_fallback(subscription);

        if prepared_metrics.is_empty() {
            let empty_chunk = PreparedTelemetryChunk {
                metric_bytes: Vec::new(),
                counter_updates: Vec::new(),
            };
            let (payload, _) = Self::encode_prepared_chunk(
                subscription,
                resource_attributes,
                &empty_chunk,
                unsupported_compression_types,
            )?;
            if payload.len() > max_bytes {
                return Err(TelemetryChunkingError::SingleMetricTooLarge {
                    payload_bytes: payload.len(),
                    max_bytes,
                });
            }
            return Ok(vec![empty_chunk]);
        }

        if max_bytes == usize::MAX {
            let mut chunk = PreparedTelemetryChunk {
                metric_bytes: Vec::with_capacity(prepared_metrics.len()),
                counter_updates: Vec::new(),
            };
            for prepared_metric in prepared_metrics {
                chunk.metric_bytes.push(prepared_metric.bytes);
                chunk.counter_updates.extend(prepared_metric.counter_update);
            }

            // Unlimited broker budgets still need one encode pass here so we
            // surface unusable broker codec combinations before enqueueing the window.
            let _ = Self::encode_prepared_chunk(
                subscription,
                resource_attributes,
                &chunk,
                unsupported_compression_types,
            )?;

            return Ok(vec![chunk]);
        }

        let mut chunks = Vec::new();
        let mut current_chunk = ChunkBuilderState::default();
        let mut context = ChunkPreparationContext {
            subscription,
            resource_attributes,
            max_bytes,
            prefer_uncompressed_chunking,
            can_bound_encoded_payload_by_uncompressed,
            unsupported_compression_types,
        };

        for prepared_metric in prepared_metrics {
            let PreparedMetric {
                bytes,
                counter_update,
            } = prepared_metric;

            if current_chunk.metric_bytes.is_empty() {
                Self::start_chunk_with_metric_or_skip(
                    &mut context,
                    &mut current_chunk,
                    &mut chunks,
                    bytes,
                    counter_update,
                )?;
                continue;
            }

            let metric_entry_len = Self::len_delimited_field_len(bytes.len());
            let candidate_metric_entries_len = current_chunk.metric_entries_len + metric_entry_len;
            let fits_current_chunk = if context.can_bound_encoded_payload_by_uncompressed
                && Self::uncompressed_payload_len(resource_attributes, candidate_metric_entries_len)
                    <= max_bytes
            {
                true
            } else if prefer_uncompressed_chunking {
                Self::uncompressed_payload_len(resource_attributes, candidate_metric_entries_len)
                    <= max_bytes
            } else {
                current_chunk.metric_bytes.push(bytes.clone());
                let fits = Self::encode_payload(
                    subscription,
                    resource_attributes,
                    &current_chunk.metric_bytes,
                    context.unsupported_compression_types,
                )?
                .0
                .len()
                    <= max_bytes;
                current_chunk.metric_bytes.pop();
                fits
            };

            if fits_current_chunk {
                current_chunk.metric_entries_len += metric_entry_len;
                current_chunk.metric_bytes.push(bytes);
                current_chunk.counter_updates.extend(counter_update);
                continue;
            }

            chunks.push(Self::take_current_chunk(&mut current_chunk));
            current_chunk.metric_entries_len = 0;

            Self::start_chunk_with_metric_or_skip(
                &mut context,
                &mut current_chunk,
                &mut chunks,
                bytes,
                counter_update,
            )?;
        }

        if !current_chunk.metric_bytes.is_empty() {
            chunks.push(Self::take_current_chunk(&mut current_chunk));
        }

        Ok(chunks)
    }

    /// Retry transient push failures.
    ///
    /// `push_metrics()` now retries each payload chunk independently so we do
    /// not re-send chunks that were already accepted.
    async fn push_metrics_with_retry(
        &mut self,
        subscription: &Subscription,
        window_start_nanos: u64,
        terminating: bool,
    ) -> PushResult {
        self.push_metrics(subscription, window_start_nanos, terminating)
            .await
    }

    /// Send a final push with `terminating = true`.
    ///
    /// Per KIP-714 § Client termination: if the push fails with
    /// `UNKNOWN_SUBSCRIPTION_ID`, re-subscribe once and retry.
    async fn send_terminating_push(&mut self, subscription: &Subscription, window_start: u64) {
        if !subscription.has_metrics() {
            debug!("No metrics subscribed; skipping terminating push");
            return;
        }

        info!("Sending terminating telemetry push");
        if let PushResult::ReSubscribe = self
            .push_metrics_with_retry(subscription, window_start, true)
            .await
        {
            debug!("Terminating push returned re-subscribe; attempting one re-subscribe");
            if let SubscriptionResult::Ok(new_sub) =
                self.get_subscription(subscription.client_instance_id).await
            {
                let _ = self
                    .push_metrics_with_retry(&new_sub, window_start, true)
                    .await;
            }
        }
    }

    /// Wait for the given duration or until shutdown is signalled.
    /// Returns `true` if shutdown was signalled.
    async fn wait_or_shutdown(&mut self, duration: Duration) -> bool {
        tokio::select! {
            _ = tokio::time::sleep(duration) => false,
            result = self.shutdown.changed() => {
                // Channel closed or value changed to true → shutdown
                result.is_err() || *self.shutdown.borrow()
            }
        }
    }

    /// Current time as nanoseconds since Unix epoch.
    fn nanos_since_epoch() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}

/// Result of a `GetTelemetrySubscriptions` attempt.
enum SubscriptionResult {
    /// Successfully obtained a subscription.
    Ok(Subscription),
    /// Transient error (network, decode) — worth retrying.
    Transient,
    /// Non-retriable error — stop the reporter.
    Fatal,
}

/// Result of a `PushTelemetry` attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PushResult {
    /// Accepted normally.
    Ok,
    /// Broker invalidated subscription — need to re-subscribe.
    ReSubscribe,
    /// Transient error — skip this push, retry next interval.
    Transient,
    /// Broker throttled the push; keep the current collection window.
    Throttled,
    /// Non-retriable error — stop the reporter.
    Fatal,
}

fn should_preserve_pending_window(result: PushResult) -> bool {
    matches!(
        result,
        PushResult::Transient | PushResult::Throttled | PushResult::ReSubscribe
    )
}

fn requested_metrics_match(current: &[String], next: &[String]) -> bool {
    current.iter().map(String::as_str).collect::<HashSet<_>>()
        == next.iter().map(String::as_str).collect::<HashSet<_>>()
}

fn prefers_uncompressed_chunking(subscription: &Subscription) -> bool {
    matches!(
        subscription.accepted_compression_types.first(),
        Some(Compression::None)
    )
}

fn supports_uncompressed_fallback(subscription: &Subscription) -> bool {
    subscription
        .accepted_compression_types
        .contains(&Compression::None)
}

fn can_reuse_pending_window(current: &Subscription, next: &Subscription) -> bool {
    current.delta_temporality == next.delta_temporality
        && current.telemetry_max_bytes == next.telemetry_max_bytes
        && current.accepted_compression_types == next.accepted_compression_types
        && requested_metrics_match(&current.requested_metrics, &next.requested_metrics)
}

fn should_advance_window(has_metrics: bool, push_result: Option<PushResult>) -> bool {
    if !has_metrics {
        return true;
    }

    matches!(push_result, Some(PushResult::Ok))
}

// ---------------------------------------------------------------------------
// DeltaTracker — KIP-714 delta temporality computation
// ---------------------------------------------------------------------------

/// Tracks previous counter values to compute deltas for KIP-714 delta temporality.
///
/// When `DeltaTemporality` is `true` in the broker's subscription response,
/// counter metrics must be sent as increments since the last push rather than
/// absolute totals. This tracker stores the last reported value for each
/// counter and computes the difference on each push.
#[derive(Debug, Clone)]
struct DeltaTracker {
    prev: HashMap<String, u64>,
}

impl DeltaTracker {
    fn new() -> Self {
        Self {
            prev: HashMap::new(),
        }
    }

    /// Return the delta for a counter metric. Stores `value` as the new
    /// baseline for subsequent calls.
    #[cfg(test)]
    fn delta(&mut self, name: &str, value: u64) -> u64 {
        if let Some(prev_val) = self.prev.get_mut(name) {
            let prev = *prev_val;
            *prev_val = value;
            value.saturating_sub(prev)
        } else {
            self.prev.insert(name.to_string(), value);
            value
        }
    }

    /// Clear all stored baselines (e.g., on temporality change).
    fn reset(&mut self) {
        self.prev.clear();
    }

    fn preview_delta(&self, name: &str, value: u64) -> u64 {
        self.prev
            .get(name)
            .map_or(value, |prev| value.saturating_sub(*prev))
    }

    fn commit_updates(&mut self, updates: &[(String, u64)]) {
        for (name, value) in updates {
            self.prev.insert(name.clone(), *value);
        }
    }
}

impl CollectedMetricEntry {
    fn encode(
        &self,
        delta_temporality: bool,
        start_time_nanos: u64,
        time_nanos: u64,
        delta_tracker: &DeltaTracker,
    ) -> Vec<PreparedMetric> {
        let mut exporter =
            OtlpExporter::with_timestamps(delta_temporality, start_time_nanos, time_nanos);

        match self {
            Self::Counter { name, help, value } => {
                let encoded_value = if delta_temporality {
                    delta_tracker.preview_delta(name, *value)
                } else {
                    *value
                };
                exporter.export_counter(name, help, encoded_value);
                exporter
                    .into_metric_bytes()
                    .into_iter()
                    .map(|bytes| PreparedMetric {
                        bytes,
                        counter_update: Some((name.clone(), *value)),
                    })
                    .collect()
            }
            Self::Gauge { name, help, value } => {
                exporter.export_gauge(name, help, *value);
                exporter
                    .into_metric_bytes()
                    .into_iter()
                    .map(|bytes| PreparedMetric {
                        bytes,
                        counter_update: None,
                    })
                    .collect()
            }
            Self::Latency {
                name,
                help,
                snapshot,
            } => {
                exporter.export_latency(name, help, snapshot);
                exporter
                    .into_metric_bytes()
                    .into_iter()
                    .map(|bytes| PreparedMetric {
                        bytes,
                        counter_update: None,
                    })
                    .collect()
            }
        }
    }
}

/// Wraps a [`MetricsExporter`] to convert counter values to deltas.
///
/// Gauge and latency metrics pass through unchanged since they represent
/// point-in-time values that are independent of temporality.
#[cfg(test)]
struct DeltaExporter<'a> {
    inner: &'a mut dyn MetricsExporter,
    tracker: &'a mut DeltaTracker,
}

#[cfg(test)]
impl<'a> DeltaExporter<'a> {
    fn new(inner: &'a mut dyn MetricsExporter, tracker: &'a mut DeltaTracker) -> Self {
        Self { inner, tracker }
    }
}

#[cfg(test)]
impl MetricsExporter for DeltaExporter<'_> {
    fn export_counter(&mut self, name: &str, help: &str, value: u64) {
        let delta = self.tracker.delta(name, value);
        self.inner.export_counter(name, help, delta);
    }

    fn export_gauge(&mut self, name: &str, help: &str, value: u64) {
        self.inner.export_gauge(name, help, value);
    }

    fn export_latency(
        &mut self,
        name: &str,
        help: &str,
        snapshot: &crate::metrics::LatencySnapshot,
    ) {
        self.inner.export_latency(name, help, snapshot);
    }
}

// ---------------------------------------------------------------------------
// PrefixFilterExporter — KIP-714 metric name prefix matching
// ---------------------------------------------------------------------------

/// Wraps an inner [`MetricsExporter`] and only forwards metrics whose names
/// match at least one of the broker's requested metric prefixes.
struct PrefixFilterExporter<'a> {
    prefixes: &'a [String],
    inner: &'a mut dyn MetricsExporter,
}

impl<'a> PrefixFilterExporter<'a> {
    fn new(prefixes: &'a [String], inner: &'a mut dyn MetricsExporter) -> Self {
        Self { prefixes, inner }
    }

    fn matches(&self, name: &str) -> bool {
        self.prefixes.iter().any(|p| name.starts_with(p.as_str()))
    }
}

impl MetricsExporter for PrefixFilterExporter<'_> {
    fn export_counter(&mut self, name: &str, help: &str, value: u64) {
        if self.matches(name) {
            self.inner.export_counter(name, help, value);
        }
    }

    fn export_gauge(&mut self, name: &str, help: &str, value: u64) {
        if self.matches(name) {
            self.inner.export_gauge(name, help, value);
        }
    }

    fn export_latency(
        &mut self,
        name: &str,
        help: &str,
        snapshot: &crate::metrics::LatencySnapshot,
    ) {
        if self.matches(name) {
            self.inner.export_latency(name, help, snapshot);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_telemetry_config_defaults() {
        let config = TelemetryConfig::default();
        assert!(config.enabled);
        assert_eq!(config.metrics_prefix, "org.apache.kafka");
        assert!(config.resource_attributes.is_empty());
    }

    #[test]
    fn test_nanos_since_epoch_is_reasonable() {
        let nanos = TelemetryReporter::nanos_since_epoch();
        // Should be after 2020-01-01
        let year_2020_nanos = 1_577_836_800_000_000_000u64;
        assert!(nanos > year_2020_nanos);
    }

    #[test]
    fn test_subscription_has_metrics() {
        let sub = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 0,
            push_interval: Duration::from_secs(300),
            delta_temporality: false,
            accepted_compression_types: Vec::new(),
            telemetry_max_bytes: 1_048_576,
            requested_metrics: vec![],
        };
        assert!(!sub.has_metrics());
        assert!(!sub.wants_all_metrics());
    }

    #[test]
    fn test_subscription_wants_all_metrics() {
        let sub = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 0,
            push_interval: Duration::from_secs(300),
            delta_temporality: false,
            accepted_compression_types: Vec::new(),
            telemetry_max_bytes: 1_048_576,
            requested_metrics: vec!["*".to_string()],
        };
        assert!(sub.has_metrics());
        assert!(sub.wants_all_metrics());
    }

    #[test]
    fn test_subscription_prefix_metrics() {
        let sub = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 0,
            push_interval: Duration::from_secs(300),
            delta_temporality: false,
            accepted_compression_types: Vec::new(),
            telemetry_max_bytes: 1_048_576,
            requested_metrics: vec!["org.apache.kafka.producer.".to_string()],
        };
        assert!(sub.has_metrics());
        assert!(!sub.wants_all_metrics());
    }

    #[test]
    fn test_prefix_filter_exporter() {
        let prefixes = vec![
            "org.apache.kafka.producer.".to_string(),
            "org.apache.kafka.consumer.lag".to_string(),
        ];

        let mut otlp = OtlpExporter::new(false, 0);
        {
            let mut filter = PrefixFilterExporter::new(&prefixes, &mut otlp);

            // Should match
            filter.export_counter("org.apache.kafka.producer.records_sent", "help", 10);
            filter.export_gauge("org.apache.kafka.consumer.lag", "help", 5);
            filter.export_latency(
                "org.apache.kafka.producer.send_latency",
                "help",
                &crate::metrics::LatencySnapshot {
                    count: 1,
                    sum: Duration::from_millis(50),
                    min: Some(Duration::from_millis(50)),
                    max: Some(Duration::from_millis(50)),
                    avg: Some(Duration::from_millis(50)),
                    p50: Some(Duration::from_millis(50)),
                    p95: Some(Duration::from_millis(50)),
                    p99: Some(Duration::from_millis(50)),
                },
            );

            // Should NOT match
            filter.export_counter("org.apache.kafka.consumer.polls", "help", 99);
            filter.export_gauge("org.apache.kafka.connection.active", "help", 3);
            filter.export_latency(
                "org.apache.kafka.connection.latency",
                "help",
                &crate::metrics::LatencySnapshot {
                    count: 1,
                    sum: Duration::from_millis(10),
                    min: Some(Duration::from_millis(10)),
                    max: Some(Duration::from_millis(10)),
                    avg: Some(Duration::from_millis(10)),
                    p50: Some(Duration::from_millis(10)),
                    p95: Some(Duration::from_millis(10)),
                    p99: Some(Duration::from_millis(10)),
                },
            );
        }

        // 2 direct metrics + 5 from matching latency (count, sum, min, max, avg)
        assert_eq!(otlp.finish_metric_count(), 7);
    }

    #[test]
    fn test_subscription_push_interval_clamped_to_supported_bounds() {
        let check = |raw: i32, expected_ms: u64| {
            let clamped = clamp_push_interval_ms(raw) as u64;
            assert_eq!(clamped, expected_ms);
        };
        check(0, 100);
        check(-1, 100);
        check(50, 100);
        check(100, 100);
        check(300_000, 300_000);
        check(MAX_PUSH_INTERVAL_MS + 1, MAX_PUSH_INTERVAL_MS as u64);
        check(i32::MAX, MAX_PUSH_INTERVAL_MS as u64);
    }

    #[test]
    fn test_retry_backoff_exponential() {
        assert_eq!(retry_backoff(1), Duration::from_secs(1));
        assert_eq!(retry_backoff(2), Duration::from_secs(2));
        assert_eq!(retry_backoff(3), Duration::from_secs(4));
    }

    #[test]
    fn test_should_advance_window_only_after_successful_push() {
        assert!(should_advance_window(false, None));
        assert!(should_advance_window(true, Some(PushResult::Ok)));
        assert!(!should_advance_window(true, Some(PushResult::Transient)));
        assert!(!should_advance_window(true, Some(PushResult::Throttled)));
        assert!(!should_advance_window(true, Some(PushResult::ReSubscribe)));
    }

    #[test]
    fn test_preserve_pending_window_for_retries_and_resubscribe() {
        assert!(should_preserve_pending_window(PushResult::Transient));
        assert!(should_preserve_pending_window(PushResult::Throttled));
        assert!(should_preserve_pending_window(PushResult::ReSubscribe));
        assert!(!should_preserve_pending_window(PushResult::Ok));
        assert!(!should_preserve_pending_window(PushResult::Fatal));
    }

    #[test]
    fn test_prefers_uncompressed_chunking_only_when_none_is_first() {
        let mut subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::None],
            telemetry_max_bytes: 1_024,
            requested_metrics: vec!["*".to_string()],
        };

        assert!(prefers_uncompressed_chunking(&subscription));

        subscription.accepted_compression_types = vec![Compression::Gzip, Compression::None];
        assert!(!prefers_uncompressed_chunking(&subscription));
    }

    #[test]
    fn test_supports_uncompressed_fallback_when_none_is_advertised() {
        let mut subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::Gzip],
            telemetry_max_bytes: 1_024,
            requested_metrics: vec!["*".to_string()],
        };

        assert!(!supports_uncompressed_fallback(&subscription));

        subscription.accepted_compression_types = vec![Compression::Gzip, Compression::None];
        assert!(supports_uncompressed_fallback(&subscription));
    }

    #[test]
    fn test_reuse_pending_window_requires_matching_subscription_shape() {
        let base = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: true,
            accepted_compression_types: vec![Compression::Gzip, Compression::None],
            telemetry_max_bytes: 1024,
            requested_metrics: vec!["foo".to_string(), "bar".to_string()],
        };

        let mut same = base.clone();
        same.subscription_id = 2;
        same.push_interval = Duration::from_secs(2);
        assert!(can_reuse_pending_window(&base, &same));

        let mut changed_filter = base.clone();
        changed_filter.requested_metrics = vec!["foo".to_string()];
        assert!(!can_reuse_pending_window(&base, &changed_filter));

        let mut changed_delta = base.clone();
        changed_delta.delta_temporality = false;
        assert!(!can_reuse_pending_window(&base, &changed_delta));

        let mut changed_max_bytes = base.clone();
        changed_max_bytes.telemetry_max_bytes = 2048;
        assert!(!can_reuse_pending_window(&base, &changed_max_bytes));

        let mut changed_compression = base.clone();
        changed_compression.accepted_compression_types = vec![Compression::None];
        assert!(!can_reuse_pending_window(&base, &changed_compression));
    }

    #[test]
    fn test_requested_metrics_match_compares_effective_prefix_sets() {
        assert!(requested_metrics_match(
            &["a".to_string(), "a".to_string()],
            &["a".to_string()]
        ));
        assert!(!requested_metrics_match(
            &["a".to_string(), "a".to_string()],
            &["a".to_string(), "b".to_string()]
        ));
    }

    #[test]
    fn test_prepare_push_chunks_splits_oversized_payload() {
        let start_time_nanos = 1;
        let time_nanos = 2;
        let tracker = DeltaTracker::new();
        let entries = vec![
            CollectedMetricEntry::Counter {
                name: "org.apache.kafka.producer.records_sent_total".to_string(),
                help: "help".to_string(),
                value: 10,
            },
            CollectedMetricEntry::Gauge {
                name: "org.apache.kafka.consumer.lag".to_string(),
                help: "help".to_string(),
                value: 5,
            },
        ];

        let first_metric = entries[0].encode(false, start_time_nanos, time_nanos, &tracker);
        let max_bytes =
            TelemetryReporter::build_payload_from_metrics(&[], &[first_metric[0].bytes.clone()])
                .len();

        let subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::None],
            telemetry_max_bytes: max_bytes as i32,
            requested_metrics: vec!["*".to_string()],
        };
        let mut unsupported = HashSet::new();

        let chunks = TelemetryReporter::prepare_push_chunks(
            &subscription,
            start_time_nanos,
            time_nanos,
            &entries,
            &[],
            &tracker,
            &mut unsupported,
        )
        .expect("payload should split into multiple chunks");

        assert_eq!(chunks.len(), 2);
        assert!(chunks.iter().all(|chunk| {
            let mut unsupported = HashSet::new();
            TelemetryReporter::encode_prepared_chunk(&subscription, &[], chunk, &mut unsupported)
                .expect("chunk should encode")
                .0
                .len()
                <= max_bytes
        }));
    }

    #[test]
    fn test_prepare_push_chunks_skips_single_metric_too_large() {
        let start_time_nanos = 1;
        let time_nanos = 2;
        let tracker = DeltaTracker::new();
        let entries = vec![
            CollectedMetricEntry::Gauge {
                name: "small_metric_a".to_string(),
                help: "help".to_string(),
                value: 1,
            },
            CollectedMetricEntry::Gauge {
                name: "oversized_metric".to_string(),
                help: "x".repeat(8_192),
                value: 2,
            },
            CollectedMetricEntry::Gauge {
                name: "small_metric_b".to_string(),
                help: "help".to_string(),
                value: 3,
            },
        ];

        let first_metric = entries[0].encode(false, start_time_nanos, time_nanos, &tracker);
        let max_bytes =
            TelemetryReporter::build_payload_from_metrics(&[], &[first_metric[0].bytes.clone()])
                .len();
        let oversized_metric = entries[1].encode(false, start_time_nanos, time_nanos, &tracker);
        let oversized_payload_len = TelemetryReporter::build_payload_from_metrics(
            &[],
            &[oversized_metric[0].bytes.clone()],
        )
        .len();
        assert!(oversized_payload_len > max_bytes);

        let subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::None],
            telemetry_max_bytes: max_bytes as i32,
            requested_metrics: vec!["*".to_string()],
        };
        let mut unsupported = HashSet::new();

        let chunks = TelemetryReporter::prepare_push_chunks(
            &subscription,
            start_time_nanos,
            time_nanos,
            &entries,
            &[],
            &tracker,
            &mut unsupported,
        )
        .expect("oversized metrics should be skipped instead of stalling telemetry");

        let emitted_metric_count: usize = chunks.iter().map(|chunk| chunk.metric_bytes.len()).sum();
        assert_eq!(emitted_metric_count, 2);
        assert!(chunks.iter().all(|chunk| {
            let mut unsupported = HashSet::new();
            TelemetryReporter::encode_prepared_chunk(&subscription, &[], chunk, &mut unsupported)
                .expect("chunk should encode")
                .0
                .len()
                <= max_bytes
        }));
    }

    #[test]
    fn test_prepare_push_chunks_keeps_compressible_payload_together_when_none_is_fallback() {
        if !Compression::Gzip.is_available() {
            return;
        }

        let start_time_nanos = 1;
        let time_nanos = 2;
        let tracker = DeltaTracker::new();
        let repeated_help = "compressible".repeat(512);
        let entries = vec![
            CollectedMetricEntry::Gauge {
                name: "metric_a".to_string(),
                help: repeated_help.clone(),
                value: 1,
            },
            CollectedMetricEntry::Gauge {
                name: "metric_b".to_string(),
                help: repeated_help,
                value: 2,
            },
        ];

        let prepared_metrics: Vec<_> = entries
            .iter()
            .flat_map(|entry| entry.encode(false, start_time_nanos, time_nanos, &tracker))
            .collect();
        let metric_bytes: Vec<_> = prepared_metrics
            .iter()
            .map(|metric| metric.bytes.clone())
            .collect();
        let payload = TelemetryReporter::build_payload_from_metrics(&[], &metric_bytes);
        let compressed = Compression::Gzip
            .compress(&payload)
            .expect("gzip compression should succeed in this test");

        assert!(payload.len() > compressed.len());
        let max_bytes = compressed.len();

        let subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::Gzip, Compression::None],
            telemetry_max_bytes: max_bytes as i32,
            requested_metrics: vec!["*".to_string()],
        };
        let mut unsupported = HashSet::new();

        let chunks = TelemetryReporter::prepare_push_chunks(
            &subscription,
            start_time_nanos,
            time_nanos,
            &entries,
            &[],
            &tracker,
            &mut unsupported,
        )
        .expect("compressible payload should fit in one chunk with gzip-first subscriptions");

        assert_eq!(chunks.len(), 1);
        let encoded = TelemetryReporter::encode_prepared_chunk(
            &subscription,
            &[],
            &chunks[0],
            &mut unsupported,
        )
        .expect("chunk should encode");
        assert_eq!(encoded.1, Compression::Gzip);
        assert!(encoded.0.len() <= max_bytes);
    }

    #[test]
    fn test_prepare_push_chunks_unbounded_max_bytes_still_validates_codec() {
        let start_time_nanos = 1;
        let time_nanos = 2;
        let tracker = DeltaTracker::new();
        let entries = vec![CollectedMetricEntry::Gauge {
            name: "metric_a".to_string(),
            help: "help".to_string(),
            value: 1,
        }];
        let subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: Vec::new(),
            telemetry_max_bytes: 0,
            requested_metrics: vec!["*".to_string()],
        };
        let mut unsupported = HashSet::new();

        let error = TelemetryReporter::prepare_push_chunks(
            &subscription,
            start_time_nanos,
            time_nanos,
            &entries,
            &[],
            &tracker,
            &mut unsupported,
        )
        .expect_err("unbounded chunking must still reject missing broker codecs");

        assert!(matches!(
            error,
            TelemetryChunkingError::NoUsableCompressionCodec { .. }
        ));
    }

    #[test]
    fn test_delta_tracker_commits_only_successful_chunk_updates() {
        let start_time_nanos = 1;
        let time_nanos = 2;
        let mut tracker = DeltaTracker {
            prev: HashMap::from([("counter_a".to_string(), 10), ("counter_b".to_string(), 20)]),
        };
        let entries = vec![
            CollectedMetricEntry::Counter {
                name: "counter_a".to_string(),
                help: "help".to_string(),
                value: 15,
            },
            CollectedMetricEntry::Counter {
                name: "counter_b".to_string(),
                help: "help".to_string(),
                value: 30,
            },
        ];

        let first_metric = entries[0].encode(true, start_time_nanos, time_nanos, &tracker);
        let max_bytes =
            TelemetryReporter::build_payload_from_metrics(&[], &[first_metric[0].bytes.clone()])
                .len();

        let subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: true,
            accepted_compression_types: vec![Compression::None],
            telemetry_max_bytes: max_bytes as i32,
            requested_metrics: vec!["*".to_string()],
        };
        let mut unsupported = HashSet::new();

        let chunks = TelemetryReporter::prepare_push_chunks(
            &subscription,
            start_time_nanos,
            time_nanos,
            &entries,
            &[],
            &tracker,
            &mut unsupported,
        )
        .expect("counter payload should split into two chunks");

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].counter_updates.len(), 1);
        assert_eq!(chunks[1].counter_updates.len(), 1);

        let first_update = chunks[0].counter_updates[0].clone();
        let second_update = chunks[1].counter_updates[0].clone();

        tracker.commit_updates(std::slice::from_ref(&first_update));

        assert_eq!(tracker.preview_delta(&first_update.0, first_update.1), 0);
        assert_eq!(tracker.preview_delta(&second_update.0, second_update.1), 10);
    }

    #[test]
    fn test_uncompressed_payload_len_matches_encoded_payload() {
        let start_time_nanos = 1;
        let time_nanos = 2;
        let tracker = DeltaTracker::new();
        let entries = vec![
            CollectedMetricEntry::Counter {
                name: "counter_a".to_string(),
                help: "help".to_string(),
                value: 15,
            },
            CollectedMetricEntry::Gauge {
                name: "gauge_b".to_string(),
                help: "help".to_string(),
                value: 2,
            },
        ];
        let mut metric_bytes = Vec::new();
        let mut metric_entries_len = 0;

        for entry in entries {
            for prepared in entry.encode(false, start_time_nanos, time_nanos, &tracker) {
                metric_entries_len +=
                    TelemetryReporter::len_delimited_field_len(prepared.bytes.len());
                metric_bytes.push(prepared.bytes);
            }
        }

        let resource_attributes = vec![("service.name".to_string(), "krafka".to_string())];

        assert_eq!(
            TelemetryReporter::uncompressed_payload_len(&resource_attributes, metric_entries_len,),
            TelemetryReporter::build_payload_from_metrics(&resource_attributes, &metric_bytes)
                .len()
        );
    }

    #[test]
    fn test_choose_compression_prefers_first_supported_broker_codec() {
        let subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::Gzip, Compression::None],
            telemetry_max_bytes: 1_048_576,
            requested_metrics: vec!["*".to_string()],
        };
        let payload = vec![b'a'; 1024];
        let mut unsupported = HashSet::new();

        let (_, compression) =
            TelemetryReporter::choose_compression(&subscription, &mut unsupported, &payload)
                .expect("gzip or none should be usable");

        if Compression::Gzip.is_available() {
            assert_eq!(compression, Compression::Gzip);
        } else {
            assert_eq!(compression, Compression::None);
            assert!(unsupported.contains(&Compression::Gzip));
        }
    }

    #[test]
    fn test_choose_compression_skips_cached_unsupported_codec() {
        let subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: vec![
                Compression::Zstd,
                Compression::Gzip,
                Compression::None,
            ],
            telemetry_max_bytes: 1_048_576,
            requested_metrics: vec!["*".to_string()],
        };
        let payload = vec![b'a'; 1024];
        let mut unsupported = HashSet::from([Compression::Zstd]);

        let (_, compression) =
            TelemetryReporter::choose_compression(&subscription, &mut unsupported, &payload)
                .expect(
                    "a cached unsupported codec should fall through to the next advertised option",
                );

        assert!(
            !unsupported.remove(&Compression::Zstd)
                || unsupported.is_empty()
                || unsupported.contains(&Compression::Gzip)
        );
        if Compression::Gzip.is_available() {
            assert_eq!(compression, Compression::Gzip);
        } else {
            assert_eq!(compression, Compression::None);
            assert!(unsupported.contains(&Compression::Gzip));
        }
    }

    #[test]
    fn test_choose_compression_uses_broker_codec_when_none_is_not_advertised() {
        let subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::Gzip],
            telemetry_max_bytes: 1_048_576,
            requested_metrics: vec!["*".to_string()],
        };
        let payload = vec![b'a'];
        let mut unsupported = HashSet::new();

        let result =
            TelemetryReporter::choose_compression(&subscription, &mut unsupported, &payload);

        if Compression::Gzip.is_available() {
            let (_, compression) = result.expect("gzip should remain the only legal fallback");
            assert_eq!(compression, Compression::Gzip);
        } else {
            let error =
                result.expect_err("without gzip or none, compression selection should fail");
            assert!(matches!(
                error,
                TelemetryChunkingError::NoUsableCompressionCodec { .. }
            ));
            assert!(unsupported.contains(&Compression::Gzip));
        }
    }

    #[test]
    fn test_choose_compression_prefers_none_over_expanding_fallback() {
        let subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::Gzip, Compression::None],
            telemetry_max_bytes: 1_048_576,
            requested_metrics: vec!["*".to_string()],
        };
        let payload = vec![b'a'];
        let mut unsupported = HashSet::new();

        let (_, compression) =
            TelemetryReporter::choose_compression(&subscription, &mut unsupported, &payload)
                .expect("gzip or none should be usable");

        assert_eq!(compression, Compression::None);
        if !Compression::Gzip.is_available() {
            assert!(unsupported.contains(&Compression::Gzip));
        }
    }

    #[test]
    fn test_choose_compression_errors_without_usable_broker_codec() {
        let subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::Gzip],
            telemetry_max_bytes: 1_048_576,
            requested_metrics: vec!["*".to_string()],
        };
        let payload = vec![b'a'; 1024];
        let mut unsupported = HashSet::from([Compression::Gzip]);

        let error = TelemetryReporter::choose_compression(
            &subscription,
            &mut unsupported,
            &payload,
        )
        .expect_err(
            "without an advertised none codec, selection must fail when all codecs are unusable",
        );

        match error {
            TelemetryChunkingError::NoUsableCompressionCodec {
                accepted_compression_types,
            } => assert_eq!(accepted_compression_types, vec![Compression::Gzip]),
            other => panic!("unexpected compression selection error: {other:?}"),
        }
    }

    #[test]
    fn test_subscription_multiple_prefixes() {
        let sub = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 0,
            push_interval: Duration::from_secs(300),
            delta_temporality: false,
            accepted_compression_types: Vec::new(),
            telemetry_max_bytes: 1_048_576,
            requested_metrics: vec![
                "org.apache.kafka.producer.".to_string(),
                "org.apache.kafka.consumer.".to_string(),
            ],
        };
        assert!(sub.has_metrics());
        assert!(!sub.wants_all_metrics());
    }

    #[test]
    fn test_delta_tracker_computes_deltas() {
        let mut tracker = DeltaTracker::new();

        // First call: no previous value → delta equals the absolute value.
        assert_eq!(tracker.delta("counter_a", 10), 10);
        assert_eq!(tracker.delta("counter_b", 5), 5);

        // Second call: returns the increment since last call.
        assert_eq!(tracker.delta("counter_a", 25), 15);
        assert_eq!(tracker.delta("counter_b", 5), 0); // no change

        // Third call: counter wraps (value < previous) → saturating_sub → 0.
        assert_eq!(tracker.delta("counter_a", 20), 0);
    }

    #[test]
    fn test_delta_tracker_reset() {
        let mut tracker = DeltaTracker::new();
        tracker.delta("c", 100);
        tracker.reset();

        // After reset, previous baseline is gone → full value returned.
        assert_eq!(tracker.delta("c", 50), 50);
    }

    #[test]
    fn test_delta_exporter_converts_counters_only() {
        let mut otlp = OtlpExporter::new(true, 0);
        let mut tracker = DeltaTracker::new();

        // First push: absolute 100 → delta is 100.
        {
            let mut dexp = DeltaExporter::new(&mut otlp, &mut tracker);
            dexp.export_counter("c", "help", 100);
            dexp.export_gauge("g", "help", 42);
        }
        assert_eq!(otlp.finish_metric_count(), 2);

        // Second push with a fresh exporter: counter went to 130 → delta is 30.
        let mut otlp2 = OtlpExporter::new(true, 0);
        {
            let mut dexp = DeltaExporter::new(&mut otlp2, &mut tracker);
            dexp.export_counter("c", "help", 130);
            dexp.export_gauge("g", "help", 99);
        }
        assert_eq!(otlp2.finish_metric_count(), 2);

        // Verify the finished protobuf is different (delta 30 vs absolute 130).
        // Just check the bytes differ — full structural tests are in otlp.rs.
        let data1 = otlp.finish();
        let data2 = otlp2.finish();
        assert_ne!(data1, data2);
    }

    #[test]
    fn test_delta_exporter_with_prefix_filter() {
        let prefixes = vec!["prod.".to_string()];
        let mut otlp = OtlpExporter::new(true, 0);
        let mut tracker = DeltaTracker::new();

        // Chain: PrefixFilter → DeltaExporter → OtlpExporter
        {
            let mut dexp = DeltaExporter::new(&mut otlp, &mut tracker);
            let mut filter = PrefixFilterExporter::new(&prefixes, &mut dexp);

            filter.export_counter("prod.sent", "help", 50);
            filter.export_counter("cons.recv", "help", 99); // filtered out
        }
        assert_eq!(otlp.finish_metric_count(), 1); // only prod.sent passed

        // Second pass: delta for prod.sent should be 30.
        let mut otlp2 = OtlpExporter::new(true, 0);
        {
            let mut dexp = DeltaExporter::new(&mut otlp2, &mut tracker);
            let mut filter = PrefixFilterExporter::new(&prefixes, &mut dexp);

            filter.export_counter("prod.sent", "help", 80);
        }
        assert_eq!(otlp2.finish_metric_count(), 1);
    }
}
