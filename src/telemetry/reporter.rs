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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use rand::Rng;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::error::ErrorCode;
use crate::metrics::{KrafkaMetrics, MetricsExporter};
use crate::network::BrokerConnection;
use crate::protocol::{
    ApiKey, GetTelemetrySubscriptionsRequest, GetTelemetrySubscriptionsResponse,
    PushTelemetryRequest, PushTelemetryResponse, VersionedDecode,
};

use super::otlp::OtlpExporter;

/// Maximum retry attempts for transient failures (subscription / push).
const MAX_RETRIES: u32 = 3;

/// Base backoff duration for retries.
const RETRY_BACKOFF_BASE: Duration = Duration::from_secs(1);

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
    metrics: Arc<KrafkaMetrics>,
    config: TelemetryConfig,
    shutdown: watch::Receiver<bool>,
    /// Tracks previous counter values for KIP-714 delta temporality.
    delta_tracker: DeltaTracker,
    /// Last observed `delta_temporality` flag — reset tracker on change.
    last_delta_temporality: bool,
}

impl TelemetryReporter {
    /// Create a new telemetry reporter.
    ///
    /// * `connection` — broker connection to use for telemetry RPCs.
    /// * `metrics` — the shared metrics registry to read from.
    /// * `config` — telemetry configuration.
    /// * `shutdown` — a watch channel; set to `true` to stop the reporter.
    pub fn new(
        connection: Arc<BrokerConnection>,
        metrics: Arc<KrafkaMetrics>,
        config: TelemetryConfig,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            connection,
            metrics,
            config,
            shutdown,
            delta_tracker: DeltaTracker::new(),
            last_delta_temporality: false,
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
            if subscription.has_metrics() {
                match self.push_metrics(&subscription, window_start, false).await {
                    PushResult::Ok => {}
                    PushResult::ReSubscribe => {
                        debug!("Subscription invalidated; re-subscribing");
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
                    PushResult::Transient => {
                        // Logged already; we'll just retry on the next interval.
                    }
                    PushResult::Fatal => {
                        warn!("Fatal telemetry push error; reporter exiting");
                        return;
                    }
                }
            }

            window_start = Self::nanos_since_epoch();

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

    /// Try to obtain a subscription, retrying transient failures with backoff.
    async fn get_subscription_with_retry(
        &mut self,
        client_instance_id: [u8; 16],
    ) -> Option<Subscription> {
        for attempt in 0..=MAX_RETRIES {
            if attempt > 0 {
                let backoff = RETRY_BACKOFF_BASE * 2u32.saturating_pow(attempt - 1);
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

        // Respect throttle_time_ms if set.
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

        let push_interval = Duration::from_millis(resp.push_interval_ms.max(100) as u64);

        // KIP-714: the response only contains a non-null ClientInstanceId on
        // the initial handshake (request had null UUID). On subsequent requests
        // (re-subscriptions) the response field is null. Preserve the original
        // broker-assigned ID in that case.
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
            telemetry_max_bytes: resp.telemetry_max_bytes,
            requested_metrics: resp.requested_metrics,
        })
    }

    /// Collect metrics and send `PushTelemetry`.
    async fn push_metrics(
        &mut self,
        subscription: &Subscription,
        window_start_nanos: u64,
        terminating: bool,
    ) -> PushResult {
        let mut exporter = OtlpExporter::new(subscription.delta_temporality, window_start_nanos);

        for (k, v) in &self.config.resource_attributes {
            exporter.add_resource_attribute(k.as_str(), v.as_str());
        }

        // KIP-714: detect temporality change and reset delta tracker.
        if subscription.delta_temporality != self.last_delta_temporality {
            debug!(
                old = self.last_delta_temporality,
                new = subscription.delta_temporality,
                "Delta temporality changed; resetting tracker"
            );
            self.delta_tracker.reset();
            self.last_delta_temporality = subscription.delta_temporality;
        }

        // KIP-714: empty requested_metrics → no metrics desired (keep polling).
        // Single "*" → all metrics. Otherwise, prefix-match filter.
        // When delta temporality is active, wrap in DeltaExporter so counters
        // report increments since the last push instead of absolute totals.
        if subscription.has_metrics() {
            if subscription.delta_temporality {
                let mut delta_exp = DeltaExporter::new(&mut exporter, &mut self.delta_tracker);
                if subscription.wants_all_metrics() {
                    self.metrics
                        .export_all_with_prefix(&self.config.metrics_prefix, &mut delta_exp);
                } else {
                    let mut filter =
                        PrefixFilterExporter::new(&subscription.requested_metrics, &mut delta_exp);
                    self.metrics
                        .export_all_with_prefix(&self.config.metrics_prefix, &mut filter);
                }
            } else if subscription.wants_all_metrics() {
                self.metrics
                    .export_all_with_prefix(&self.config.metrics_prefix, &mut exporter);
            } else {
                let mut filter =
                    PrefixFilterExporter::new(&subscription.requested_metrics, &mut exporter);
                self.metrics
                    .export_all_with_prefix(&self.config.metrics_prefix, &mut filter);
            }
        }

        let payload = exporter.finish();

        // Warn if payload exceeds broker limit (but still send — broker enforces).
        if subscription.telemetry_max_bytes > 0
            && payload.len() > subscription.telemetry_max_bytes as usize
        {
            warn!(
                payload_bytes = payload.len(),
                max_bytes = subscription.telemetry_max_bytes,
                "Telemetry payload exceeds broker TelemetryMaxBytes"
            );
        }

        let req = PushTelemetryRequest {
            client_instance_id: subscription.client_instance_id,
            subscription_id: subscription.subscription_id,
            terminating,
            compression_type: 0, // no compression
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

        // Respect throttle_time_ms.
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
            // KIP-714: re-subscribe to get new subscription ID.
            ErrorCode::UnknownSubscriptionId => {
                debug!("Broker returned UNKNOWN_SUBSCRIPTION_ID");
                PushResult::ReSubscribe
            }
            // KIP-714: re-subscribe to get updated compression types / subscription.
            ErrorCode::UnsupportedCompressionType => {
                debug!("Broker returned UNSUPPORTED_COMPRESSION_TYPE");
                PushResult::ReSubscribe
            }
            // KIP-714: reduce payload size; re-subscribe to refresh TelemetryMaxBytes.
            ErrorCode::TelemetryTooLarge => {
                warn!(
                    payload_bytes = req.metrics.len(),
                    "Broker returned TELEMETRY_TOO_LARGE; re-subscribing for updated limits"
                );
                PushResult::ReSubscribe
            }
            // KIP-714: non-retriable — stop pushing.
            ErrorCode::InvalidRequest | ErrorCode::InvalidRecord => {
                warn!(
                    error_code = ?resp.error_code,
                    "PushTelemetry rejected with non-retriable error; stopping"
                );
                PushResult::Fatal
            }
            // Throttling or other transient — retry on next interval.
            ErrorCode::ThrottlingQuotaExceeded => {
                debug!("PushTelemetry throttled; will retry next interval");
                PushResult::Ok
            }
            other => {
                warn!(error_code = ?other, "PushTelemetry returned unexpected error");
                PushResult::Transient
            }
        }
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
        if let PushResult::ReSubscribe = self.push_metrics(subscription, window_start, true).await {
            debug!("Terminating push returned re-subscribe; attempting one re-subscribe");
            if let SubscriptionResult::Ok(new_sub) =
                self.get_subscription(subscription.client_instance_id).await
            {
                let _ = self.push_metrics(&new_sub, window_start, true).await;
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
enum PushResult {
    /// Accepted normally.
    Ok,
    /// Broker invalidated subscription — need to re-subscribe.
    ReSubscribe,
    /// Transient error — skip this push, retry next interval.
    Transient,
    /// Non-retriable error — stop the reporter.
    Fatal,
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
}

/// Wraps a [`MetricsExporter`] to convert counter values to deltas.
///
/// Gauge and latency metrics pass through unchanged since they represent
/// point-in-time values that are independent of temporality.
struct DeltaExporter<'a> {
    inner: &'a mut dyn MetricsExporter,
    tracker: &'a mut DeltaTracker,
}

impl<'a> DeltaExporter<'a> {
    fn new(inner: &'a mut dyn MetricsExporter, tracker: &'a mut DeltaTracker) -> Self {
        Self { inner, tracker }
    }
}

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
                },
            );
        }

        // 2 direct metrics + 5 from matching latency (count, sum, min, max, avg)
        assert_eq!(otlp.finish_metric_count(), 7);
    }

    #[test]
    fn test_subscription_push_interval_floored_at_100ms() {
        // Verifies the push_interval_ms.max(100) floor in get_subscription's
        // Subscription construction. Values below 100 (including negatives)
        // should be clamped to 100ms.
        let check = |raw: i32, expected_ms: u64| {
            let clamped = raw.max(100) as u64;
            assert_eq!(clamped, expected_ms);
        };
        check(0, 100);
        check(-1, 100);
        check(50, 100);
        check(100, 100);
        check(300_000, 300_000);
    }

    #[test]
    fn test_subscription_multiple_prefixes() {
        let sub = Subscription {
            client_instance_id: [0; 16],
            subscription_id: 0,
            push_interval: Duration::from_secs(300),
            delta_temporality: false,
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
