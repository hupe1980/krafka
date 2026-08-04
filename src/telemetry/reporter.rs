//! KIP-714 background telemetry reporter.
//!
//! The [`TelemetryReporter`] runs as a Tokio task that:
//!
//! 1. Sends `GetTelemetrySubscriptions` to obtain a `client_instance_id`,
//!    subscription ID, push interval, and the broker's metric preferences.
//! 2. Periodically collects metrics from the client's [`KrafkaMetrics`]
//!    registry, serialises them as OTLP protobuf, and sends a
//!    `PushTelemetry` request.
//! 3. On shutdown (via the cancellation token), pushes any remaining
//!    telemetry and then sends exactly one dedicated, minimal push with
//!    `terminating = true` so the broker can release subscription state
//!    immediately instead of waiting for it to expire.
//!
//! The reporter prefers an existing broker connection and sticks to the
//! same broker for the lifetime of the subscription, switching only when
//! the connection drops (per KIP-714 § Connection Selection).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::error::ErrorCode;
use crate::metrics::{KrafkaMetrics, LatencySnapshot, MetricsExporter};
use crate::network::{BrokerConnection, ConnectionPool};
use crate::protocol::{
    ApiKey, Compression, GetTelemetrySubscriptionsRequest, GetTelemetrySubscriptionsResponse,
    PushTelemetryRequest, PushTelemetryResponse, VersionedDecode, versions,
};

use super::otlp::OtlpExporter;

/// Maximum retry attempts for transient failures (subscription / push).
const MAX_RETRIES: u32 = 3;

/// Payload budget used when the broker advertises a non-positive
/// `TelemetryMaxBytes`.
///
/// Treating a non-positive limit as "unbounded" is unsafe: the broker still
/// enforces *its* limit and answers with `TELEMETRY_TOO_LARGE`, which makes the
/// reporter re-subscribe and then re-encode the very same oversized payload on
/// every interval — a permanent livelock. Falling back to a conservative 1 MiB
/// keeps chunking active so an oversized collection is split instead.
const DEFAULT_TELEMETRY_MAX_BYTES: usize = 1024 * 1024;

/// Upper bound on the number of distinct strings held by the persistent
/// [`MetricStringInterner`].
///
/// Metric names and help texts come from a fixed set of string literals, so
/// this ceiling is never reached in practice; it exists purely so a
/// pathological (e.g. label-cardinality-driven) name space cannot grow the
/// cache without limit for the lifetime of the reporter.
const MAX_INTERNED_METRIC_STRINGS: usize = 4096;

/// Base backoff duration for retries.
const RETRY_BACKOFF_BASE: Duration = Duration::from_secs(1);

/// Minimum telemetry push interval accepted from the broker.
const MIN_PUSH_INTERVAL_MS: i32 = 100;

/// Maximum telemetry push interval accepted from the broker.
const MAX_PUSH_INTERVAL_MS: i32 = 60 * 60 * 1000;

fn retry_backoff(attempt: u32) -> Duration {
    debug_assert!(attempt > 0, "retry_backoff expects attempts starting at 1");
    let base = RETRY_BACKOFF_BASE * 2u32.saturating_pow(attempt.saturating_sub(1));
    // Add ±25% jitter to prevent thundering herd on concurrent reconnects.
    let jitter = rand::random_range(0.75..1.25);
    base.mul_f64(jitter)
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
    ///
    /// Per KIP-714 an **empty** list means no metrics are desired (the reporter
    /// keeps polling for subscription changes), while a `"*"` entry means *all*
    /// metrics.
    requested_metrics: Vec<String>,
}

impl Subscription {
    /// Returns `true` if any metrics should be emitted for this subscription.
    ///
    /// An empty `RequestedMetrics` list means "no metrics" per KIP-714.
    fn has_metrics(&self) -> bool {
        !self.requested_metrics.is_empty()
    }

    /// Returns `true` if all metrics are requested.
    ///
    /// The wildcard `"*"` matches everything wherever it appears in the list —
    /// including alongside other prefixes. Requiring it to be the *sole* entry
    /// would send a mixed list such as `["*", "org.apache.kafka.producer."]`
    /// down the prefix-matching path, where `"*"` matches nothing (no metric
    /// name starts with `*`) and nearly every metric is silently dropped.
    fn wants_all_metrics(&self) -> bool {
        self.requested_metrics.iter().any(|metric| metric == "*")
    }
}

#[derive(Debug, Clone)]
enum CollectedMetricEntry {
    Counter {
        /// Interned metric name — `Arc<str>` avoids repeated allocation across
        /// 100 ms push intervals; `Arc::clone` is a single atomic increment.
        name: Arc<str>,
        help: Arc<str>,
        value: u64,
    },
    Gauge {
        name: Arc<str>,
        help: Arc<str>,
        value: u64,
    },
    Latency {
        name: Arc<str>,
        help: Arc<str>,
        snapshot: LatencySnapshot,
    },
}

/// Intern table for metric names and help strings.
///
/// The telemetry push loop calls `export_counter` / `export_gauge` /
/// `export_latency` at (potentially) 100 ms intervals with the same name and
/// help strings every time. The table is owned by the [`TelemetryReporter`] and
/// **reused across every collection**, so each unique string is allocated
/// exactly once for the lifetime of the reporter and later collections only pay
/// an atomic increment for the `Arc::clone`. Rebuilding it per collection would
/// re-allocate every string on every push and defeat the whole purpose.
///
/// The table is bounded by [`MAX_INTERNED_METRIC_STRINGS`]; once full, further
/// strings are returned uninterned rather than growing the cache without limit.
#[derive(Default)]
struct MetricStringInterner {
    cache: HashMap<String, Arc<str>>,
}

impl MetricStringInterner {
    fn intern(&mut self, s: &str) -> Arc<str> {
        if let Some(arc) = self.cache.get(s) {
            return arc.clone();
        }
        let arc: Arc<str> = s.into();
        if self.cache.len() < MAX_INTERNED_METRIC_STRINGS {
            self.cache.insert(s.to_owned(), arc.clone());
        }
        arc
    }

    /// Number of distinct strings currently interned.
    #[cfg(test)]
    fn len(&self) -> usize {
        self.cache.len()
    }
}

/// Collects exported metrics into [`CollectedMetricEntry`] values, interning
/// names and help texts through a caller-owned (and therefore long-lived)
/// [`MetricStringInterner`].
struct CollectingExporter<'a> {
    entries: Vec<CollectedMetricEntry>,
    interner: &'a mut MetricStringInterner,
}

impl<'a> CollectingExporter<'a> {
    fn new(interner: &'a mut MetricStringInterner) -> Self {
        Self {
            entries: Vec::new(),
            interner,
        }
    }
}

impl MetricsExporter for CollectingExporter<'_> {
    fn export_counter(&mut self, name: &str, help: &str, value: u64) {
        let name = self.interner.intern(name);
        let help = self.interner.intern(help);
        self.entries
            .push(CollectedMetricEntry::Counter { name, help, value });
    }

    fn export_gauge(&mut self, name: &str, help: &str, value: u64) {
        let name = self.interner.intern(name);
        let help = self.interner.intern(help);
        self.entries
            .push(CollectedMetricEntry::Gauge { name, help, value });
    }

    fn export_latency(&mut self, name: &str, help: &str, snapshot: &LatencySnapshot) {
        let name = self.interner.intern(name);
        let help = self.interner.intern(help);
        self.entries.push(CollectedMetricEntry::Latency {
            name,
            help,
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

/// Immutable-per-window inputs shared by the chunking helpers.
struct ChunkPreparationContext<'a> {
    subscription: &'a Subscription,
    resource_attributes: &'a [(String, String)],
    /// Effective payload budget (see [`telemetry_max_bytes`]).
    max_bytes: usize,
    /// The broker lists `Compression::None` first, so payloads go out
    /// uncompressed and the uncompressed size is exactly the wire size.
    prefer_uncompressed_chunking: bool,
    /// The broker advertises `Compression::None` somewhere, so the wire size is
    /// never larger than the uncompressed size.
    can_bound_encoded_payload_by_uncompressed: bool,
    unsupported_compression_types: &'a mut HashSet<Compression>,
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
/// use krafka::network::{BrokerConnection, ConnectionPool};
///
/// # async fn example(conn: Arc<BrokerConnection>, pool: Arc<ConnectionPool>) {
/// let metrics = Arc::new(KrafkaMetrics::new());
/// let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
/// let broker_addresses = vec!["localhost:9092".to_string()];
///
/// let reporter = TelemetryReporter::new(
///     conn,
///     pool,
///     broker_addresses,
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
    /// Persistent metric name/help intern table, reused across collections so
    /// repeated pushes do not re-allocate the same strings every interval.
    metric_string_interner: MetricStringInterner,
    /// Last observed [`KrafkaMetrics::reset_generation`].
    ///
    /// `KrafkaMetrics::reset()` rewinds counters that [`DeltaTracker`] assumes
    /// are monotonic; a bumped generation means the baselines are stale.
    last_reset_generation: u64,
    /// Index into `broker_addresses` at which the next reconnect sweep starts.
    ///
    /// Randomised at construction and advanced on every reconnect so a fleet of
    /// clients does not pile its telemetry onto `broker_addresses[0]` after a
    /// rolling restart.
    reconnect_start_index: usize,
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
        let reconnect_start_index = random_start_index(broker_addresses.len());
        let metrics_reset_generation = metrics.reset_generation();
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
            metric_string_interner: MetricStringInterner::default(),
            last_reset_generation: metrics_reset_generation,
            reconnect_start_index,
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
        let jitter_factor: f64 = rand::random_range(0.5..1.5);
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
                let result = self.push_metrics(&subscription, window_start).await;
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
                        let preserved_fatal_window = self.pending_push_window.is_some();
                        if self.reconnect().await {
                            match self
                                .get_subscription_with_retry(subscription.client_instance_id)
                                .await
                            {
                                Some(s) => {
                                    // Resetting the delta baselines while a
                                    // pending window still holds deltas
                                    // computed against the *old* baselines
                                    // would re-send those deltas against a
                                    // zeroed baseline and silently lose the
                                    // accrued increment. Drop the window
                                    // whenever it can no longer be reused —
                                    // the same guard the ReSubscribe arm uses.
                                    if preserved_fatal_window
                                        && !can_reuse_pending_window(&subscription, &s)
                                    {
                                        debug!(
                                            "Telemetry subscription changed across reconnect; dropping preserved pending window"
                                        );
                                        self.pending_push_window = None;
                                        self.delta_tracker.reset();
                                        window_start = Self::nanos_since_epoch();
                                    } else if !preserved_fatal_window {
                                        self.delta_tracker.reset();
                                    }
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
    /// Tries every entry of `broker_addresses` exactly once, starting at
    /// `reconnect_start_index` and wrapping around, and replaces
    /// `self.connection` on the first success. Returns `true` if reconnected,
    /// `false` if all fail.
    ///
    /// The start index is randomised at construction and advanced round-robin
    /// on every sweep: always starting at `broker_addresses[0]` would
    /// concentrate the telemetry traffic of an entire client fleet on a single
    /// broker after a rolling restart.
    async fn reconnect(&mut self) -> bool {
        let broker_count = self.broker_addresses.len();
        if broker_count == 0 {
            return false;
        }

        let start = self.reconnect_start_index % broker_count;
        // Advance so a subsequent reconnect sweep starts elsewhere.
        self.reconnect_start_index = start.wrapping_add(1) % broker_count;

        for offset in 0..broker_count {
            let idx = (start + offset) % broker_count;
            let addr = self.broker_addresses[idx].clone();
            match self.pool.get_connection(&addr).await {
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
    ///
    /// The request version is negotiated against the broker's `ApiVersions`
    /// response rather than hardcoded, so a broker that does not support the
    /// KIP-714 API (or supports only versions outside our range) is treated as
    /// a permanent condition instead of being retried forever.
    async fn get_subscription(&self, client_instance_id: [u8; 16]) -> SubscriptionResult {
        let Some(api_version) = self.connection.negotiate_api_version(
            ApiKey::GetTelemetrySubscriptions,
            versions::GET_TELEMETRY_SUBSCRIPTIONS_MAX,
            versions::GET_TELEMETRY_SUBSCRIPTIONS_MIN,
        ) else {
            warn!(
                "Broker does not support a compatible GetTelemetrySubscriptions version; stopping telemetry reporter"
            );
            return SubscriptionResult::Fatal;
        };

        let req = GetTelemetrySubscriptionsRequest { client_instance_id };

        let response_bytes: Bytes = match self
            .connection
            .send_request(ApiKey::GetTelemetrySubscriptions, api_version, |buf| {
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
            api_version,
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
    /// Collect metrics and send them as one or more `PushTelemetry` requests.
    ///
    /// Data chunks are **never** marked `terminating`. The terminating flag is
    /// carried by a dedicated final request sent from
    /// [`Self::send_terminating_push`], so a mid-window failure cannot swallow
    /// the shutdown notification and a retried chunk cannot send
    /// `terminating = true` twice.
    async fn push_metrics(
        &mut self,
        subscription: &Subscription,
        window_start_nanos: u64,
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

            self.drop_baselines_on_metrics_reset();

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

        let mut committed_counter_updates = Vec::new();
        let mut chunk_iter = chunks.into_iter();

        while let Some(chunk) = chunk_iter.next() {
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
                .push_payload_with_retry(subscription, false, payload, compression)
                .await;
            if chunk_result != PushResult::Ok {
                self.delta_tracker
                    .commit_updates(&committed_counter_updates);
                if should_preserve_pending_window(chunk_result) {
                    let mut remaining_chunks = vec![chunk];
                    remaining_chunks.extend(chunk_iter);
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

    /// Send a single `PushTelemetry` request and classify the outcome.
    ///
    /// The request version is negotiated against the broker's `ApiVersions`
    /// response instead of being hardcoded to `0`.
    async fn push_payload_once(
        &mut self,
        subscription: &Subscription,
        terminating: bool,
        payload: Vec<u8>,
        compression: Compression,
    ) -> PushResult {
        let Some(api_version) = self.connection.negotiate_api_version(
            ApiKey::PushTelemetry,
            versions::PUSH_TELEMETRY_MAX,
            versions::PUSH_TELEMETRY_MIN,
        ) else {
            warn!(
                "Broker does not support a compatible PushTelemetry version; stopping telemetry reporter"
            );
            return PushResult::Fatal;
        };

        let req = PushTelemetryRequest {
            client_instance_id: subscription.client_instance_id,
            subscription_id: subscription.subscription_id,
            terminating,
            compression_type: compression as i8,
            metrics: Bytes::from(payload),
        };

        let response_bytes: Bytes = match self
            .connection
            .send_request(ApiKey::PushTelemetry, api_version, |buf| req.encode_v0(buf))
            .await
        {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "PushTelemetry request failed (transient)");
                return PushResult::Transient;
            }
        };

        let resp = match PushTelemetryResponse::decode_versioned(
            api_version,
            &mut response_bytes.as_ref(),
        ) {
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
            ErrorCode::InvalidRequest
            | ErrorCode::InvalidRecord
            // UNSUPPORTED_VERSION can never be resolved by retrying the same
            // request: the broker rejected the negotiated request version
            // itself, so classifying it as transient would spin forever at the
            // push interval.
            | ErrorCode::UnsupportedVersion => {
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

    /// Drop the [`DeltaTracker`] baselines when the metrics registry has been
    /// reset since the last collection.
    ///
    /// [`KrafkaMetrics::reset`] rewinds counters that the tracker assumes are
    /// monotonic. Without this, `preview_delta` would `saturating_sub` a
    /// now-larger baseline and report `0` for every counter until each one
    /// climbed back past its pre-reset value — silently hiding real traffic.
    /// Comparing the registry's reset generation detects that in O(1).
    fn drop_baselines_on_metrics_reset(&mut self) {
        let generation = self.metrics.reset_generation();
        if generation != self.last_reset_generation {
            debug!(
                previous_generation = self.last_reset_generation,
                generation, "Metrics registry was reset; dropping delta baselines"
            );
            self.delta_tracker.reset();
            self.last_reset_generation = generation;
        }
    }

    /// Collect the metrics selected by `subscription`.
    ///
    /// Takes `&mut self` because the metric-name intern table lives on the
    /// reporter and is reused across every collection.
    fn collect_metrics(&mut self, subscription: &Subscription) -> Vec<CollectedMetricEntry> {
        // Bind the fields individually so the interner can be borrowed mutably
        // while `metrics`/`config` stay borrowed immutably.
        let metrics = &self.metrics;
        let metrics_prefix = self.config.metrics_prefix.as_str();
        let mut collector = CollectingExporter::new(&mut self.metric_string_interner);

        if subscription.has_metrics() {
            if subscription.wants_all_metrics() {
                metrics.export_all_with_prefix(metrics_prefix, &mut collector);
            } else {
                let mut filter =
                    PrefixFilterExporter::new(&subscription.requested_metrics, &mut collector);
                metrics.export_all_with_prefix(metrics_prefix, &mut filter);
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

            match compression.compress_with_level(payload, None) {
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

    /// Encode a contiguous run of prepared metrics into a wire payload.
    fn encode_prepared_metric_range(
        context: &mut ChunkPreparationContext<'_>,
        metrics: &[PreparedMetric],
    ) -> Result<(Vec<u8>, Compression), TelemetryChunkingError> {
        let mut exporter = OtlpExporter::new(false, 0);
        for (key, value) in context.resource_attributes {
            exporter.add_resource_attribute(key.as_str(), value.as_str());
        }
        for metric in metrics {
            exporter.push_metric_bytes(metric.bytes.clone());
        }
        let payload = exporter.finish();

        Self::choose_compression(
            context.subscription,
            context.unsupported_compression_types,
            &payload,
        )
    }

    /// Decide whether a contiguous run of prepared metrics fits the broker's
    /// payload budget, spending **at most one** compression pass.
    ///
    /// `metric_entries_len` is the pre-computed sum of the run's length-delimited
    /// entry sizes, so the uncompressed size is arithmetic rather than a re-encode.
    ///
    /// Three cases, cheapest first:
    ///
    /// 1. The broker advertises `Compression::None`, so
    ///    [`Self::choose_compression`] never returns something *larger* than the
    ///    uncompressed encoding — an uncompressed fit therefore proves a wire
    ///    fit, with no compression at all.
    /// 2. The broker lists `Compression::None` first, so the payload goes out
    ///    uncompressed verbatim and the uncompressed size *is* the wire size.
    /// 3. Otherwise compression decides, and exactly one pass is spent.
    fn range_fits(
        context: &mut ChunkPreparationContext<'_>,
        metrics: &[PreparedMetric],
        metric_entries_len: usize,
    ) -> Result<bool, TelemetryChunkingError> {
        let uncompressed_len =
            Self::uncompressed_payload_len(context.resource_attributes, metric_entries_len);

        if context.can_bound_encoded_payload_by_uncompressed
            && uncompressed_len <= context.max_bytes
        {
            return Ok(true);
        }

        if context.prefer_uncompressed_chunking {
            return Ok(uncompressed_len <= context.max_bytes);
        }

        let (payload, _) = Self::encode_prepared_metric_range(context, metrics)?;
        Ok(payload.len() <= context.max_bytes)
    }

    /// Split a collection window into payload chunks that each fit the broker's
    /// `TelemetryMaxBytes`.
    ///
    /// # Algorithm
    ///
    /// The metric run is split by **binary subdivision with verification**:
    /// test the whole run, and on overflow split it in half and test each half.
    /// Uncompressed sizes come from a prefix-sum table (O(1) per test), and a
    /// compression pass is spent only on runs that are not already provably
    /// within budget — one pass per tested run, never one per candidate metric.
    ///
    /// The earlier design appended metrics one at a time and re-encoded *and*
    /// re-compressed the entire accumulated chunk after every single candidate,
    /// which is O(n²) work with an O(n) number of gzip passes — repeated at a
    /// push interval that may be as short as 100 ms.
    ///
    /// Chunk boundaries may land slightly earlier than a perfectly greedy pack
    /// would place them; that costs at most an extra request and never
    /// correctness — **no emitted chunk exceeds `max_bytes`**, because every
    /// emitted chunk was verified by [`Self::range_fits`]. A single metric that
    /// cannot fit on its own is dropped with a warning rather than stalling the
    /// whole window.
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

        let max_bytes = telemetry_max_bytes(subscription);
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

        // Prefix sums of the length-delimited entry sizes: the uncompressed
        // size of any contiguous run is a single subtraction.
        let mut entry_len_prefix_sums = Vec::with_capacity(prepared_metrics.len() + 1);
        let mut running = 0usize;
        entry_len_prefix_sums.push(running);
        for metric in &prepared_metrics {
            running += Self::len_delimited_field_len(metric.bytes.len());
            entry_len_prefix_sums.push(running);
        }

        let mut context = ChunkPreparationContext {
            subscription,
            resource_attributes,
            max_bytes,
            prefer_uncompressed_chunking,
            can_bound_encoded_payload_by_uncompressed,
            unsupported_compression_types,
        };

        let mut chunks = Vec::new();
        // LIFO work list of half-open ranges. Halves are pushed right-then-left
        // so ranges are always visited (and chunks emitted) in metric order.
        let mut pending_ranges = vec![(0usize, prepared_metrics.len())];

        while let Some((start, end)) = pending_ranges.pop() {
            debug_assert!(start < end, "empty ranges are never pushed");

            let metric_entries_len = entry_len_prefix_sums[end] - entry_len_prefix_sums[start];
            if Self::range_fits(
                &mut context,
                &prepared_metrics[start..end],
                metric_entries_len,
            )? {
                let mut metric_bytes = Vec::with_capacity(end - start);
                let mut counter_updates = Vec::new();
                // Each metric is emitted exactly once (ranges are disjoint and
                // visited left to right), so moving the buffers out is safe.
                for metric in &mut prepared_metrics[start..end] {
                    metric_bytes.push(std::mem::take(&mut metric.bytes));
                    counter_updates.extend(metric.counter_update.take());
                }
                chunks.push(PreparedTelemetryChunk {
                    metric_bytes,
                    counter_updates,
                });
                continue;
            }

            if end - start == 1 {
                warn!(
                    metric_bytes = prepared_metrics[start].bytes.len(),
                    max_bytes, "Skipping telemetry metric that exceeds broker TelemetryMaxBytes"
                );
                continue;
            }

            let mid = start + (end - start) / 2;
            pending_ranges.push((mid, end));
            pending_ranges.push((start, mid));
        }

        Ok(chunks)
    }

    /// Send the dedicated final `PushTelemetry` carrying `terminating = true`.
    ///
    /// The payload is a minimal, metric-free OTLP envelope: the flag exists so
    /// the broker can release subscription state immediately, and piggybacking
    /// it on a data chunk is what made it possible to lose it (a mid-window
    /// failure) or to send it twice (a retried final chunk).
    async fn push_terminating_notification(&mut self, subscription: &Subscription) -> PushResult {
        let empty_chunk = PreparedTelemetryChunk {
            metric_bytes: Vec::new(),
            counter_updates: Vec::new(),
        };

        let (payload, compression) = match Self::encode_prepared_chunk(
            subscription,
            &self.config.resource_attributes,
            &empty_chunk,
            &mut self.unsupported_compression_types,
        ) {
            Ok(encoded) => encoded,
            Err(error) => {
                warn!(
                    ?error,
                    "Failed to encode terminating telemetry push; skipping it"
                );
                return PushResult::Fatal;
            }
        };

        self.push_payload_with_retry(subscription, true, payload, compression)
            .await
    }

    /// Flush the last collection window and then send exactly one terminating
    /// push.
    ///
    /// Per KIP-714 § Client termination the terminating push is what lets the
    /// broker drop subscription state right away instead of waiting for it to
    /// expire, so it is sent **unconditionally** — even when the data push
    /// above failed mid-window, and even when no metrics were subscribed. If it
    /// comes back with `UNKNOWN_SUBSCRIPTION_ID`, re-subscribe once and send it
    /// one final time.
    async fn send_terminating_push(&mut self, subscription: &Subscription, window_start: u64) {
        if subscription.has_metrics() {
            // Best-effort flush of the outstanding window. Its outcome must not
            // decide whether the broker learns that we are going away.
            let data_result = self.push_metrics(subscription, window_start).await;
            if data_result != PushResult::Ok {
                debug!(
                    ?data_result,
                    "Final telemetry data push did not fully succeed; sending terminating push anyway"
                );
            }
        } else {
            debug!("No metrics subscribed; sending terminating push only");
        }

        info!("Sending terminating telemetry push");
        if self.push_terminating_notification(subscription).await == PushResult::ReSubscribe {
            debug!("Terminating push returned re-subscribe; attempting one re-subscribe");
            if let SubscriptionResult::Ok(new_sub) =
                self.get_subscription(subscription.client_instance_id).await
            {
                let _ = self.push_terminating_notification(&new_sub).await;
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

/// Effective per-request payload budget for a subscription.
///
/// A non-positive broker-supplied `TelemetryMaxBytes` falls back to
/// [`DEFAULT_TELEMETRY_MAX_BYTES`] rather than disabling chunking: an
/// "unlimited" budget lets a large collection go out whole, the broker answers
/// `TELEMETRY_TOO_LARGE`, the reporter re-subscribes and re-encodes the very
/// same oversized payload — livelocking at the push interval forever.
fn telemetry_max_bytes(subscription: &Subscription) -> usize {
    if subscription.telemetry_max_bytes > 0 {
        subscription.telemetry_max_bytes as usize
    } else {
        debug!(
            telemetry_max_bytes = subscription.telemetry_max_bytes,
            default_max_bytes = DEFAULT_TELEMETRY_MAX_BYTES,
            "Broker advertised a non-positive TelemetryMaxBytes; using the default payload budget"
        );
        DEFAULT_TELEMETRY_MAX_BYTES
    }
}

/// Pick the index at which the first reconnect sweep starts.
///
/// Randomising it spreads a client fleet's telemetry across the broker list
/// instead of stacking it all on `broker_addresses[0]`.
fn random_start_index(broker_count: usize) -> usize {
    if broker_count <= 1 {
        0
    } else {
        rand::random_range(0..broker_count)
    }
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
                        counter_update: Some((name.to_string(), *value)),
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

    /// Forward labeled counters as labeled counters so labels survive the
    /// wrapper, converting the value to a delta keyed by name *and* labels.
    ///
    /// Relying on the trait's default implementation would flatten the labels
    /// into the metric name before the inner exporter ever sees them.
    fn export_labeled_counter(
        &mut self,
        name: &str,
        help: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) {
        let key = delta_key(name, labels);
        let delta = self.tracker.delta(&key, value);
        self.inner.export_labeled_counter(name, help, labels, delta);
    }

    /// Labeled gauges pass through unchanged (with their labels intact): a
    /// gauge is a point-in-time value and is independent of temporality.
    fn export_labeled_gauge(
        &mut self,
        name: &str,
        help: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) {
        self.inner.export_labeled_gauge(name, help, labels, value);
    }
}

/// Build the [`DeltaTracker`] key for a labeled counter.
///
/// Each label combination is an independent time series, so the labels must be
/// part of the key or two series would share (and corrupt) one baseline.
#[cfg(test)]
fn delta_key(name: &str, labels: &[(&str, &str)]) -> String {
    if labels.is_empty() {
        return name.to_string();
    }
    let mut key = String::with_capacity(name.len() + labels.len() * 16);
    key.push_str(name);
    for (label_key, label_value) in labels {
        key.push('\u{1}');
        key.push_str(label_key);
        key.push('\u{2}');
        key.push_str(label_value);
    }
    key
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

    /// Filter on the *base* metric name and forward labels intact.
    ///
    /// The trait default would flatten labels into the name first, so the
    /// subscription prefixes would be matched against a label-decorated name
    /// and the inner exporter would lose the labels entirely.
    fn export_labeled_counter(
        &mut self,
        name: &str,
        help: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) {
        if self.matches(name) {
            self.inner.export_labeled_counter(name, help, labels, value);
        }
    }

    /// Filter on the *base* metric name and forward labels intact, for the
    /// same reason as [`Self::export_labeled_counter`].
    fn export_labeled_gauge(
        &mut self,
        name: &str,
        help: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) {
        if self.matches(name) {
            self.inner.export_labeled_gauge(name, help, labels, value);
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

        // 2 direct metrics + 8 from matching latency
        // (count, sum, min, max, avg, p50, p95, p99)
        assert_eq!(otlp.finish_metric_count(), 10);
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
        // With ±25% jitter the actual duration falls within [0.75×base, 1.25×base].
        let b1 = retry_backoff(1);
        assert!(b1 >= Duration::from_millis(750) && b1 <= Duration::from_millis(1250));
        let b2 = retry_backoff(2);
        assert!(b2 >= Duration::from_millis(1500) && b2 <= Duration::from_millis(2500));
        let b3 = retry_backoff(3);
        assert!(b3 >= Duration::from_millis(3000) && b3 <= Duration::from_millis(5000));
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
                name: "org.apache.kafka.producer.records_sent_total".into(),
                help: "help".into(),
                value: 10,
            },
            CollectedMetricEntry::Gauge {
                name: "org.apache.kafka.consumer.lag".into(),
                help: "help".into(),
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
            telemetry_max_bytes: i32::try_from(max_bytes).unwrap(),
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
                name: "small_metric_a".into(),
                help: "help".into(),
                value: 1,
            },
            CollectedMetricEntry::Gauge {
                name: "oversized_metric".into(),
                help: "x".repeat(8_192).into(),
                value: 2,
            },
            CollectedMetricEntry::Gauge {
                name: "small_metric_b".into(),
                help: "help".into(),
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
            telemetry_max_bytes: i32::try_from(max_bytes).unwrap(),
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
                name: "metric_a".into(),
                help: repeated_help.clone().into(),
                value: 1,
            },
            CollectedMetricEntry::Gauge {
                name: "metric_b".into(),
                help: repeated_help.into(),
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
            .compress_with_level(&payload, None)
            .expect("gzip compression should succeed in this test");

        assert!(payload.len() > compressed.len());
        let max_bytes = compressed.len();

        let subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::Gzip, Compression::None],
            telemetry_max_bytes: i32::try_from(max_bytes).unwrap(),
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
            name: "metric_a".into(),
            help: "help".into(),
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
                name: "counter_a".into(),
                help: "help".into(),
                value: 15,
            },
            CollectedMetricEntry::Counter {
                name: "counter_b".into(),
                help: "help".into(),
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
            telemetry_max_bytes: i32::try_from(max_bytes).unwrap(),
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
                name: "counter_a".into(),
                help: "help".into(),
                value: 15,
            },
            CollectedMetricEntry::Gauge {
                name: "gauge_b".into(),
                help: "help".into(),
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

    // -----------------------------------------------------------------
    // Wildcard metric filtering
    // -----------------------------------------------------------------

    /// A `"*"` alongside other prefixes must still mean "all metrics".
    ///
    /// Requiring `"*"` to be the sole entry sent mixed lists down the
    /// prefix-matching path, where nothing starts with `*`, dropping
    /// essentially every metric.
    #[test]
    fn test_wildcard_in_mixed_requested_metrics_list_matches_all() {
        let subscription = |requested: Vec<&str>| Subscription {
            client_instance_id: [0; 16],
            subscription_id: 0,
            push_interval: Duration::from_secs(300),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::None],
            telemetry_max_bytes: 1_048_576,
            requested_metrics: requested.into_iter().map(str::to_string).collect(),
        };

        // Sole wildcard: unchanged behaviour.
        assert!(subscription(vec!["*"]).wants_all_metrics());
        // Wildcard mixed with prefixes, in any position.
        assert!(subscription(vec!["*", "org.apache.kafka.producer."]).wants_all_metrics());
        assert!(subscription(vec!["org.apache.kafka.producer.", "*"]).wants_all_metrics());
        assert!(subscription(vec!["a.", "*", "b."]).wants_all_metrics());
        // No wildcard: prefix matching.
        assert!(!subscription(vec!["org.apache.kafka.producer."]).wants_all_metrics());
        // KIP-714 empty list semantics are preserved: no metrics at all.
        let empty = subscription(vec![]);
        assert!(!empty.has_metrics());
        assert!(!empty.wants_all_metrics());
    }

    // -----------------------------------------------------------------
    // TelemetryMaxBytes fallback
    // -----------------------------------------------------------------

    /// A non-positive `TelemetryMaxBytes` must fall back to 1 MiB, not to an
    /// unbounded budget that livelocks on `TELEMETRY_TOO_LARGE`.
    #[test]
    fn test_non_positive_telemetry_max_bytes_falls_back_to_default() {
        let with_max = |max: i32| Subscription {
            client_instance_id: [0; 16],
            subscription_id: 0,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::None],
            telemetry_max_bytes: max,
            requested_metrics: vec!["*".to_string()],
        };

        assert_eq!(DEFAULT_TELEMETRY_MAX_BYTES, 1024 * 1024);
        assert_eq!(
            telemetry_max_bytes(&with_max(0)),
            DEFAULT_TELEMETRY_MAX_BYTES
        );
        assert_eq!(
            telemetry_max_bytes(&with_max(-1)),
            DEFAULT_TELEMETRY_MAX_BYTES
        );
        assert_eq!(
            telemetry_max_bytes(&with_max(i32::MIN)),
            DEFAULT_TELEMETRY_MAX_BYTES
        );
        // A positive limit is used verbatim.
        assert_eq!(telemetry_max_bytes(&with_max(4_096)), 4_096);
        assert_ne!(telemetry_max_bytes(&with_max(0)), usize::MAX);
    }

    /// With a non-positive `TelemetryMaxBytes`, chunking must stay *enabled*:
    /// a collection larger than the 1 MiB default has to be split.
    #[test]
    fn test_non_positive_max_bytes_still_chunks_oversized_collection() {
        let start_time_nanos = 1;
        let time_nanos = 2;
        let tracker = DeltaTracker::new();

        // Roughly 3 MiB of gauges — comfortably over the 1 MiB default.
        let entries: Vec<_> = (0..48)
            .map(|i| CollectedMetricEntry::Gauge {
                name: format!("metric_{i}").into(),
                help: "h".repeat(64 * 1024).into(),
                value: i,
            })
            .collect();

        let subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::None],
            telemetry_max_bytes: 0,
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
        .expect("a non-positive max-bytes must chunk, not produce one unbounded payload");

        assert!(
            chunks.len() > 1,
            "expected the oversized collection to be split across chunks"
        );
        for chunk in &chunks {
            let mut unsupported = HashSet::new();
            let encoded = TelemetryReporter::encode_prepared_chunk(
                &subscription,
                &[],
                chunk,
                &mut unsupported,
            )
            .expect("chunk should encode");
            assert!(encoded.0.len() <= DEFAULT_TELEMETRY_MAX_BYTES);
        }
    }

    // -----------------------------------------------------------------
    // Fatal-path pending-window invalidation
    // -----------------------------------------------------------------

    /// Both the ReSubscribe and the Fatal/reconnect paths must decide with the
    /// *same* rule whether a preserved pending window survives.
    ///
    /// The Fatal path used to reset the delta baselines unconditionally while
    /// keeping the pending window, so stale deltas were re-sent against a
    /// zeroed baseline and the accrued increment was lost.
    #[test]
    fn test_fatal_reconnect_invalidates_pending_window_like_resubscribe() {
        let base = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: true,
            accepted_compression_types: vec![Compression::None],
            telemetry_max_bytes: 1024,
            requested_metrics: vec!["foo".to_string()],
        };

        // Shape-compatible: the window may be reused, baselines must be kept.
        let mut compatible = base.clone();
        compatible.subscription_id = 7;
        assert!(can_reuse_pending_window(&base, &compatible));

        // Any shape change (temporality, budget, codecs, filter) invalidates it.
        for mutate in [
            (|s: &mut Subscription| s.delta_temporality = false) as fn(&mut Subscription),
            |s: &mut Subscription| s.telemetry_max_bytes = 2048,
            |s: &mut Subscription| s.accepted_compression_types = vec![Compression::Gzip],
            |s: &mut Subscription| s.requested_metrics = vec!["bar".to_string()],
        ] {
            let mut changed = base.clone();
            mutate(&mut changed);
            assert!(
                !can_reuse_pending_window(&base, &changed),
                "a changed subscription shape must drop the preserved window"
            );
        }
    }

    /// A pending window whose deltas were computed against baselines that are
    /// then reset must not be re-sent: doing so loses the accrued increment.
    #[test]
    fn test_dropping_pending_window_with_baselines_preserves_accrued_delta() {
        let mut tracker = DeltaTracker::new();
        tracker.commit_updates(&[("counter_a".to_string(), 100)]);

        // A window prepared now encodes delta 20 (120 - 100).
        assert_eq!(tracker.preview_delta("counter_a", 120), 20);

        // Resetting the baselines while keeping that window would re-send the
        // stale 20 even though the correct value against a zeroed baseline is
        // the full 120 — 100 counts would silently vanish. Dropping the window
        // together with the baselines is what keeps the total intact.
        tracker.reset();
        assert_eq!(tracker.preview_delta("counter_a", 120), 120);
    }

    // -----------------------------------------------------------------
    // Exactly-once terminating push
    // -----------------------------------------------------------------

    /// The terminating flag must live on its own dedicated request, never on a
    /// data chunk.
    ///
    /// `push_metrics` no longer takes a `terminating` parameter at all, so it
    /// is structurally impossible for a data chunk to carry the flag — which
    /// is what previously made a mid-window failure drop the terminating push
    /// entirely and a retried final chunk send it twice.
    #[test]
    fn test_data_chunks_never_carry_the_terminating_flag() {
        let start_time_nanos = 1;
        let time_nanos = 2;
        let tracker = DeltaTracker::new();
        let entries = vec![
            CollectedMetricEntry::Gauge {
                name: "metric_a".into(),
                help: "help".into(),
                value: 1,
            },
            CollectedMetricEntry::Gauge {
                name: "metric_b".into(),
                help: "help".into(),
                value: 2,
            },
        ];

        let first = entries[0].encode(false, start_time_nanos, time_nanos, &tracker);
        let max_bytes =
            TelemetryReporter::build_payload_from_metrics(&[], &[first[0].bytes.clone()]).len();

        let subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::None],
            telemetry_max_bytes: i32::try_from(max_bytes).unwrap(),
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
        .expect("two metrics should split into two chunks");

        // Multiple data chunks exist, and none of them is distinguished as
        // "the terminating one" — the flag is not part of chunk state.
        assert_eq!(chunks.len(), 2);
    }

    /// The dedicated terminating request carries a valid, metric-free payload,
    /// so it can be sent even when the preceding data push failed mid-window.
    #[test]
    fn test_terminating_push_payload_is_minimal_and_independent_of_data() {
        let subscription = Subscription {
            client_instance_id: [7; 16],
            subscription_id: 42,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::None],
            telemetry_max_bytes: 64,
            requested_metrics: vec!["*".to_string()],
        };

        let empty_chunk = PreparedTelemetryChunk {
            metric_bytes: Vec::new(),
            counter_updates: Vec::new(),
        };
        let mut unsupported = HashSet::new();
        let (payload, compression) = TelemetryReporter::encode_prepared_chunk(
            &subscription,
            &[],
            &empty_chunk,
            &mut unsupported,
        )
        .expect("the terminating payload must always encode");

        assert_eq!(compression, Compression::None);
        // Minimal: an envelope only, well within even a tiny broker budget.
        assert!(payload.len() <= telemetry_max_bytes(&subscription));
        assert!(empty_chunk.counter_updates.is_empty());
    }

    /// A mid-window chunk failure preserves the window for a later retry but
    /// must not consume the shutdown notification: the terminating push is a
    /// separate request that is still sent afterwards.
    #[test]
    fn test_mid_window_failure_preserves_window_without_consuming_terminating() {
        // A failed data chunk parks the window …
        assert!(should_preserve_pending_window(PushResult::Transient));
        assert!(should_preserve_pending_window(PushResult::Throttled));
        assert!(should_preserve_pending_window(PushResult::ReSubscribe));
        // … and never advances the collection window.
        assert!(!should_advance_window(true, Some(PushResult::Transient)));
        assert!(!should_advance_window(true, Some(PushResult::Throttled)));
        // The terminating push is not represented in either decision, because
        // it is emitted unconditionally by `send_terminating_push`.
        assert!(!should_preserve_pending_window(PushResult::Ok));
    }

    // -----------------------------------------------------------------
    // UNSUPPORTED_VERSION classification
    // -----------------------------------------------------------------

    /// `UNSUPPORTED_VERSION` is permanent and must never be retried.
    #[test]
    fn test_unsupported_version_is_not_retriable() {
        // The push classifier maps it to `Fatal` (see `push_payload_once`);
        // the shared error table agrees that it is non-retriable, which is what
        // routes the subscription path to `SubscriptionResult::Fatal` too.
        assert!(!ErrorCode::UnsupportedVersion.is_retriable());
        assert!(!ErrorCode::InvalidRequest.is_retriable());
        // Genuinely transient codes stay retriable.
        assert!(ErrorCode::RequestTimedOut.is_retriable());
    }

    /// Both telemetry RPCs negotiate their version from `ApiVersions` within
    /// the ranges published by the protocol module, instead of hardcoding `0`.
    #[test]
    fn test_telemetry_api_version_ranges_are_taken_from_protocol_constants() {
        const {
            assert!(
                versions::GET_TELEMETRY_SUBSCRIPTIONS_MIN
                    <= versions::GET_TELEMETRY_SUBSCRIPTIONS_MAX
            );
            assert!(versions::PUSH_TELEMETRY_MIN <= versions::PUSH_TELEMETRY_MAX);
            assert!(versions::GET_TELEMETRY_SUBSCRIPTIONS_MIN >= 0);
            assert!(versions::PUSH_TELEMETRY_MIN >= 0);
        }
    }

    // -----------------------------------------------------------------
    // Linear chunker
    // -----------------------------------------------------------------

    /// The chunker must be linear-ish *and* correct: every emitted chunk fits
    /// the budget, metric order is preserved, and no metric is duplicated.
    #[test]
    fn test_linear_chunker_emits_bounded_ordered_chunks() {
        let start_time_nanos = 1;
        let time_nanos = 2;
        let tracker = DeltaTracker::new();

        let entries: Vec<_> = (0..64u64)
            .map(|i| CollectedMetricEntry::Gauge {
                name: format!("org.apache.kafka.metric_{i:03}").into(),
                help: "help text".into(),
                value: i,
            })
            .collect();

        let single = entries[0].encode(false, start_time_nanos, time_nanos, &tracker);
        let one_metric_payload =
            TelemetryReporter::build_payload_from_metrics(&[], &[single[0].bytes.clone()]).len();
        // Budget for roughly five metrics per chunk.
        let max_bytes = one_metric_payload * 5;

        let subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_millis(100),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::None],
            telemetry_max_bytes: i32::try_from(max_bytes).unwrap(),
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
        .expect("chunking should succeed");

        // Every emitted chunk is within budget — the correctness invariant.
        for chunk in &chunks {
            let mut unsupported = HashSet::new();
            let encoded = TelemetryReporter::encode_prepared_chunk(
                &subscription,
                &[],
                chunk,
                &mut unsupported,
            )
            .expect("chunk should encode");
            assert!(
                encoded.0.len() <= max_bytes,
                "chunk of {} bytes exceeds the {max_bytes} byte budget",
                encoded.0.len()
            );
            assert!(!chunk.metric_bytes.is_empty());
        }

        // Nothing lost, nothing duplicated, order preserved.
        let emitted: Vec<_> = chunks
            .iter()
            .flat_map(|chunk| chunk.metric_bytes.iter().cloned())
            .collect();
        let expected: Vec<_> = entries
            .iter()
            .flat_map(|entry| entry.encode(false, start_time_nanos, time_nanos, &tracker))
            .map(|metric| metric.bytes)
            .collect();
        assert_eq!(emitted, expected);
        assert!(chunks.len() > 1);
    }

    /// The chunker must not compress at all when the broker prefers
    /// uncompressed payloads — the uncompressed size *is* the wire size.
    #[test]
    fn test_chunker_skips_compression_when_uncompressed_bounds_the_payload() {
        let subscription = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 1,
            push_interval: Duration::from_secs(1),
            delta_temporality: false,
            accepted_compression_types: vec![Compression::None, Compression::Gzip],
            telemetry_max_bytes: 4_096,
            requested_metrics: vec!["*".to_string()],
        };
        assert!(prefers_uncompressed_chunking(&subscription));
        assert!(supports_uncompressed_fallback(&subscription));

        let mut unsupported = HashSet::new();
        let mut context = ChunkPreparationContext {
            subscription: &subscription,
            resource_attributes: &[],
            max_bytes: telemetry_max_bytes(&subscription),
            prefer_uncompressed_chunking: true,
            can_bound_encoded_payload_by_uncompressed: true,
            unsupported_compression_types: &mut unsupported,
        };

        // Small run: accepted on the arithmetic fast path.
        assert!(TelemetryReporter::range_fits(&mut context, &[], 128).unwrap());
        // Oversized run: rejected, still without a compression pass.
        assert!(!TelemetryReporter::range_fits(&mut context, &[], 1_000_000).unwrap());
        assert!(
            unsupported.is_empty(),
            "no codec should have been exercised"
        );
    }

    // -----------------------------------------------------------------
    // Interner reuse
    // -----------------------------------------------------------------

    /// The interner is reused across collections, so a repeated collection
    /// hands back the *same* allocations instead of fresh ones.
    #[test]
    fn test_metric_string_interner_is_reused_across_collections() {
        let mut interner = MetricStringInterner::default();

        let first_name = {
            let mut collector = CollectingExporter::new(&mut interner);
            collector.export_counter("org.apache.kafka.producer.records", "help", 1);
            collector.export_gauge("org.apache.kafka.consumer.lag", "help", 2);
            match &collector.entries[0] {
                CollectedMetricEntry::Counter { name, .. } => name.clone(),
                other => panic!("unexpected entry: {other:?}"),
            }
        };
        assert_eq!(interner.len(), 3); // two names + one shared help

        // Second collection through the same interner: no new entries, and the
        // returned `Arc` points at the very same allocation.
        let second_name = {
            let mut collector = CollectingExporter::new(&mut interner);
            collector.export_counter("org.apache.kafka.producer.records", "help", 5);
            collector.export_gauge("org.apache.kafka.consumer.lag", "help", 6);
            match &collector.entries[0] {
                CollectedMetricEntry::Counter { name, .. } => name.clone(),
                other => panic!("unexpected entry: {other:?}"),
            }
        };
        assert_eq!(interner.len(), 3, "a reused interner must not re-grow");
        assert!(
            Arc::ptr_eq(&first_name, &second_name),
            "a reused interner must return the same allocation"
        );
    }

    /// The interner is bounded so an unbounded metric name space cannot grow
    /// it without limit.
    #[test]
    fn test_metric_string_interner_is_bounded() {
        let mut interner = MetricStringInterner::default();
        for i in 0..(MAX_INTERNED_METRIC_STRINGS + 100) {
            let _ = interner.intern(&format!("metric_{i}"));
        }
        assert_eq!(interner.len(), MAX_INTERNED_METRIC_STRINGS);
        // Strings past the ceiling still work, they are just not cached.
        assert_eq!(
            &*interner.intern("beyond_the_ceiling"),
            "beyond_the_ceiling"
        );
    }

    // -----------------------------------------------------------------
    // Reconnect start-index spread
    // -----------------------------------------------------------------

    /// Reconnect attempts must not all begin at `broker_addresses[0]`.
    #[test]
    fn test_reconnect_start_index_is_spread_across_brokers() {
        assert_eq!(random_start_index(0), 0);
        assert_eq!(random_start_index(1), 0);

        // Over many draws across a 5-broker list, more than one start index
        // must appear (the probability of a false failure is 5 * (1/5)^200).
        let observed: HashSet<usize> = (0..200).map(|_| random_start_index(5)).collect();
        assert!(
            observed.len() > 1,
            "reconnect start index must not be constant"
        );
        assert!(observed.iter().all(|idx| *idx < 5));
    }

    /// A reconnect sweep starting at any index still visits every broker
    /// exactly once, wrapping around the end of the list.
    #[test]
    fn test_reconnect_sweep_visits_every_broker_once_from_any_start() {
        let brokers = ["b0", "b1", "b2", "b3"];
        for start in 0..brokers.len() {
            let visited: Vec<_> = (0..brokers.len())
                .map(|offset| brokers[(start + offset) % brokers.len()])
                .collect();
            assert_eq!(visited.len(), brokers.len());
            assert_eq!(
                visited.iter().collect::<HashSet<_>>().len(),
                brokers.len(),
                "every broker must be tried exactly once"
            );
            assert_eq!(visited[0], brokers[start]);
        }
    }

    // -----------------------------------------------------------------
    // Labeled metric forwarding through the wrapper exporters
    // -----------------------------------------------------------------

    /// Labels must survive the filtering and delta wrappers.
    #[test]
    fn test_wrapper_exporters_forward_labeled_counters() {
        let prefixes = vec!["prod.".to_string()];
        let mut otlp = OtlpExporter::new(true, 0);
        let mut tracker = DeltaTracker::new();

        {
            let mut dexp = DeltaExporter::new(&mut otlp, &mut tracker);
            let mut filter = PrefixFilterExporter::new(&prefixes, &mut dexp);
            filter.export_labeled_counter("prod.sent", "help", &[("topic", "t1")], 10);
            filter.export_labeled_counter("prod.sent", "help", &[("topic", "t2")], 40);
            // Filtered out by the subscription prefixes.
            filter.export_labeled_counter("cons.recv", "help", &[("topic", "t1")], 99);
        }
        assert_eq!(otlp.finish_metric_count(), 2);

        // Each label combination keeps its own delta baseline.
        assert_eq!(
            tracker.delta(&delta_key("prod.sent", &[("topic", "t1")]), 25),
            15
        );
        assert_eq!(
            tracker.delta(&delta_key("prod.sent", &[("topic", "t2")]), 40),
            0
        );
    }

    /// Distinct label sets must not collide on a single delta baseline.
    #[test]
    fn test_delta_key_separates_label_combinations() {
        assert_eq!(delta_key("m", &[]), "m");
        assert_ne!(delta_key("m", &[("a", "1")]), delta_key("m", &[("a", "2")]));
        assert_ne!(delta_key("m", &[("a", "1")]), delta_key("m", &[("b", "1")]));
        assert_ne!(delta_key("ma", &[]), delta_key("m", &[("a", "")]));
    }

    // -----------------------------------------------------------------
    // Metrics-registry reset generation
    // -----------------------------------------------------------------

    /// A `KrafkaMetrics::reset()` rewinds counters the tracker assumes are
    /// monotonic, so the reporter must drop its baselines when the registry's
    /// reset generation changes.
    #[test]
    fn test_metrics_reset_generation_invalidates_delta_baselines() {
        let metrics = KrafkaMetrics::new();
        let generation = metrics.reset_generation();

        let mut tracker = DeltaTracker::new();
        tracker.commit_updates(&[("counter_a".to_string(), 500)]);

        // Registry reset: counters rewind, the generation moves.
        metrics.reset();
        let new_generation = metrics.reset_generation();
        assert_ne!(new_generation, generation);

        // Without dropping the baselines, a rewound counter reads as zero
        // delta and real traffic would stay invisible until it passed 500.
        assert_eq!(tracker.preview_delta("counter_a", 20), 0);

        // Observing the generation change drops them, so the post-reset value
        // is reported in full.
        tracker.reset();
        assert_eq!(tracker.preview_delta("counter_a", 20), 20);
    }

    /// The reset generation only moves on an actual reset, so a stable
    /// registry must not churn the baselines every interval.
    #[test]
    fn test_metrics_reset_generation_is_stable_without_reset() {
        let metrics = KrafkaMetrics::new();
        let generation = metrics.reset_generation();
        assert_eq!(metrics.reset_generation(), generation);
        assert_eq!(metrics.reset_generation(), generation);
    }

    /// Labeled gauges must keep their labels through both wrapper exporters.
    #[test]
    fn test_wrapper_exporters_forward_labeled_gauges() {
        /// One recorded `export_labeled_gauge` call.
        #[derive(Debug, PartialEq, Eq)]
        struct RecordedGauge {
            name: String,
            labels: Vec<(String, String)>,
            value: u64,
        }

        #[derive(Default)]
        struct RecordingExporter {
            labeled_gauges: Vec<RecordedGauge>,
        }

        impl MetricsExporter for RecordingExporter {
            fn export_counter(&mut self, _: &str, _: &str, _: u64) {}
            fn export_gauge(&mut self, _: &str, _: &str, _: u64) {}
            fn export_latency(&mut self, _: &str, _: &str, _: &LatencySnapshot) {}
            fn export_labeled_gauge(
                &mut self,
                name: &str,
                _help: &str,
                labels: &[(&str, &str)],
                value: u64,
            ) {
                self.labeled_gauges.push(RecordedGauge {
                    name: name.to_string(),
                    labels: labels
                        .iter()
                        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                        .collect(),
                    value,
                });
            }
        }

        let prefixes = vec!["prod.".to_string()];
        let mut recorder = RecordingExporter::default();
        let mut tracker = DeltaTracker::new();
        {
            let mut dexp = DeltaExporter::new(&mut recorder, &mut tracker);
            let mut filter = PrefixFilterExporter::new(&prefixes, &mut dexp);
            filter.export_labeled_gauge("prod.lag", "help", &[("partition", "3")], 17);
            // Filtered out by the subscription prefixes.
            filter.export_labeled_gauge("cons.lag", "help", &[("partition", "3")], 99);
        }

        // Labels survive both wrappers, and the gauge value is not deltified.
        assert_eq!(
            recorder.labeled_gauges,
            vec![RecordedGauge {
                name: "prod.lag".to_string(),
                labels: vec![("partition".to_string(), "3".to_string())],
                value: 17,
            }]
        );
    }
}
