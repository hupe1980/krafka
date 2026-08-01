//! Metrics and observability for Krafka clients.
//!
//! This module provides metrics collection for producers and consumers,
//! including counters, gauges, and latency tracking.
//!
//! # Pluggable Metrics Export
//!
//! Krafka supports pluggable metrics export through the [`MetricsExporter`] trait.
//! Built-in exporters include [`PrometheusExporter`] and [`JsonExporter`].
//! Implement the trait to add custom backends (StatsD, OpenTelemetry, etc.).
//!
//! ```rust
//! use krafka::metrics::{ProducerMetrics, PrometheusExporter, MetricsVisitable};
//!
//! let metrics = ProducerMetrics::new();
//! metrics.record_send(100);
//! metrics.record_batch(5);
//!
//! // Export in Prometheus text format
//! let mut exporter = PrometheusExporter::new();
//! metrics.export_metrics("krafka_producer", &mut exporter);
//! let prometheus_output = exporter.finish();
//! println!("{}", prometheus_output);
//! ```
//!
//! Or use the convenience method:
//! ```rust
//! use krafka::metrics::{ProducerMetrics, MetricsVisitable};
//!
//! let metrics = ProducerMetrics::new();
//! let output = metrics.to_prometheus_text("krafka_producer");
//! ```
//!
//! # JSON Export
//!
//! ```rust
//! use krafka::metrics::{ProducerMetrics, JsonExporter, MetricsVisitable};
//!
//! let metrics = ProducerMetrics::new();
//! metrics.record_send(100);
//!
//! let mut exporter = JsonExporter::new();
//! metrics.export_metrics("krafka_producer", &mut exporter);
//! let json_output = exporter.finish();
//! ```
//!
//! # All-in-One Export
//!
//! Use [`KrafkaMetrics`] to collect and export all metrics from multiple components:
//!
//! ```rust
//! use std::sync::Arc;
//! use krafka::metrics::{KrafkaMetrics, ProducerMetrics, ConsumerMetrics};
//!
//! let krafka_metrics = KrafkaMetrics::new();
//!
//! // Register components
//! let producer = krafka_metrics.producer_metrics();
//! let consumer = krafka_metrics.consumer_metrics();
//!
//! // Later, export all metrics
//! let all_metrics = krafka_metrics.to_prometheus_text();
//! ```

use ahash::AHashMap;
use std::fmt::Write;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Atomic counter for tracking counts.
#[derive(Debug, Default)]
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    /// Create a new counter with value 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment the counter by 1.
    #[inline]
    pub fn inc(&self) {
        self.add(1);
    }

    /// Add a value to the counter.
    #[inline]
    pub fn add(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Get the current value.
    #[inline]
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Reset the counter to 0.
    pub fn reset(&self) {
        self.value.store(0, Ordering::Relaxed);
    }
}

/// Atomic gauge for tracking current values.
#[derive(Debug, Default)]
pub struct Gauge {
    value: AtomicU64,
    /// Counts how many times `dec()` has been called when the gauge was already
    /// 0.  Every underflow emits a `warn!` log that includes the cumulative
    /// count, so repeated underflows in a buggy dec-loop remain visible rather
    /// than being silenced after the first occurrence.  The count itself is
    /// also a diagnostic signal: `underflow_count == 1` suggests a one-off
    /// timing edge-case, while `underflow_count == 10_000` clearly signals a
    /// systematic inc/dec imbalance.
    underflow_count: AtomicU64,
}

impl Gauge {
    /// Create a new gauge with value 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the gauge value.
    #[inline]
    pub fn set(&self, value: u64) {
        self.value.store(value, Ordering::Relaxed);
    }

    /// Get the current value.
    #[inline]
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Returns the cumulative number of times `dec()` has underflowed.
    ///
    /// A non-zero value indicates a mismatched `inc()`/`dec()` pair somewhere
    /// in the call path for this gauge.
    #[inline]
    pub fn underflow_count(&self) -> u64 {
        self.underflow_count.load(Ordering::Relaxed)
    }

    /// Increment the gauge by 1.
    #[inline]
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the gauge by 1 (saturates at 0 to prevent underflow).
    ///
    /// Logs a `warn!` **on every underflow** including the cumulative underflow
    /// count so repeated bugs remain visible — not silenced after the first
    /// occurrence.  Uses a CAS loop to atomically prevent underflow: two
    /// concurrent `dec()` calls on a gauge at value 1 are guaranteed to have
    /// exactly one succeed and one warn — never silently wrapping to `u64::MAX`.
    #[inline]
    pub fn dec(&self) {
        let result = self
            .value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                if v == 0 { None } else { Some(v - 1) }
            });
        if result.is_err() {
            let count = self.underflow_count.fetch_add(1, Ordering::Relaxed) + 1;
            tracing::warn!(
                underflow_count = count,
                "Gauge::dec() called when value was already 0 — possible inc/dec mismatch"
            );
        }
    }
}

/// Latency tracker using atomic min/max/sum/count plus a 64-bucket
/// power-of-2 histogram for p50/p95/p99 estimates.
///
/// # Bucket layout
///
/// Bucket `i` counts samples whose nanosecond value satisfies `2^i ≤ ns < 2^(i+1)`.
/// Bucket 0 contains both `0 ns` and the range `[1 ns, 2 ns)`.
///
/// # Accuracy
///
/// Each power-of-2 band is divided into 8 equal sub-buckets of width
/// `2^(i-3)`, giving a **relative error of ≤ 6.25 %** (midpoint of the
/// containing sub-bucket is at most half a sub-bucket from the true value):
///
/// | Sample range     | Sub-bucket width | Max relative error |
/// |------------------|------------------|--------------------|
/// | 1 ms – 2 ms      | ~131 μs          | 6.25 %             |
/// | 8 ms – 16 ms     | ~1 ms            | 6.25 %             |
/// | 64 ms – 128 ms   | ~8 ms            | 6.25 %             |
///
/// **For tight SLO contracts** (p99 < 50 ms alerts), ±6.25 % gives
/// sub-millisecond slack at typical latency ranges.  For zero-error
/// requirements, replace or supplement with a T-Digest or HDR histogram.
///
/// For capacity-planning and order-of-magnitude alerting ("are we above 1 s?")
/// the precision is more than adequate and comes at zero allocation cost.
///
/// This approach requires no external dependencies and no heap allocation.
#[derive(Debug)]
pub struct LatencyTracker {
    count: AtomicU64,
    sum_nanos: AtomicU64,
    min_nanos: AtomicU64,
    max_nanos: AtomicU64,
    /// Sub-divided power-of-2 histogram: 512 buckets.
    /// See [`Self::bucket_for`] and [`Self::estimate_nanos_for_bucket`]
    /// for the encoding details.
    histogram: [AtomicU64; 512],
}

impl Default for LatencyTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl LatencyTracker {
    /// Create a new latency tracker.
    pub fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum_nanos: AtomicU64::new(0),
            min_nanos: AtomicU64::new(u64::MAX),
            max_nanos: AtomicU64::new(0),
            histogram: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// Map a nanosecond value to a histogram bucket index (0–511).
    ///
    /// Encoding:
    /// - Bucket 0  : `nanos == 0`
    /// - Bucket 1  : `nanos == 1`
    /// - Bucket 2  : `nanos ∈ [2, 4)` (nanos ∈ {2, 3})
    /// - Bucket 3  : `nanos ∈ [4, 8)` (nanos ∈ {4, 5, 6, 7})
    /// - Bucket `(i-3)*8 + 4 + j` : band `i` (i ≥ 3), sub-bucket `j` (0–7),
    ///   where `j = (nanos >> (i-3)) & 7`.
    ///
    /// Each of the 61 bands `[2^i, 2^(i+1))` for `i ∈ [3, 63]` is split into
    /// 8 equal sub-buckets of width `2^(i-3)`, giving ≤ 6.25 % relative error.
    #[inline]
    fn bucket_for(nanos: u64) -> usize {
        if nanos == 0 {
            return 0;
        }
        let i = (63 - nanos.leading_zeros()) as usize; // floor(log2(nanos))
        match i {
            0 => 1, // nanos == 1
            1 => 2, // nanos ∈ {2, 3}
            2 => 3, // nanos ∈ {4, 5, 6, 7}
            _ => {
                // i >= 3: split band [2^i, 2^(i+1)) into 8 sub-buckets.
                let sub = ((nanos >> (i - 3)) & 7) as usize;
                ((i - 3) * 8 + 4 + sub).min(511)
            }
        }
    }

    /// Return the midpoint nanosecond estimate for a bucket index.
    ///
    /// For band `i ≥ 3` and sub-bucket `j`:
    ///   midpoint = `2^i + j × 2^(i-3) + 2^(i-4)` (half a sub-bucket width)
    ///
    /// For `i == 3` (sub-bucket width = 1 ns) the midpoint equals the lower
    /// bound exactly (no fractional ns).
    ///
    /// The maximum relative error of this estimate is ≤ 6.25 %.
    #[inline]
    fn estimate_nanos_for_bucket(bucket: usize) -> u64 {
        match bucket {
            0 => 0,
            1 => 1, // exact: only nanos == 1 maps here
            2 => 3, // midpoint of [2, 4): (2 + 3) / 2 = 2.5 → round up to 3
            3 => 6, // midpoint of [4, 8): (4 + 7) / 2 = 5.5 → round up to 6
            _ => {
                let idx = bucket - 4;
                let band = idx / 8 + 3; // i ≥ 3
                let sub = idx % 8;
                let lower = 1u64 << band;
                let sub_width = 1u64 << (band - 3);
                // half_sub_width: 0 for i==3 (width=1, no fractional midpoint),
                // 2^(i-4) for i>=4.
                let half_sub = if band >= 4 { 1u64 << (band - 4) } else { 0 };
                lower + sub as u64 * sub_width + half_sub
            }
        }
    }

    /// Record a latency value.
    ///
    /// Durations longer than `u64::MAX` nanoseconds (~584 years) saturate
    /// rather than wrapping — a wrapped value would silently record a bogus
    /// small latency and corrupt `min`/`sum`/the histogram.
    #[inline]
    pub fn record(&self, duration: Duration) {
        let nanos = duration.as_nanos().min(u64::MAX as u128) as u64;
        self.sum_nanos.fetch_add(nanos, Ordering::Relaxed);
        self.min_nanos.fetch_min(nanos, Ordering::Relaxed);
        self.max_nanos.fetch_max(nanos, Ordering::Relaxed);
        self.histogram[Self::bucket_for(nanos)].fetch_add(1, Ordering::Relaxed);
        // Increment count last so snapshots never observe count > histogram sum.
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Start timing an operation. Returns a guard that records when dropped.
    #[inline]
    pub fn start(&self) -> LatencyGuard<'_> {
        LatencyGuard {
            tracker: self,
            start: Instant::now(),
        }
    }

    /// Get the number of recorded samples.
    #[inline]
    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Get the sum of all recorded latencies.
    pub fn sum(&self) -> Duration {
        Duration::from_nanos(self.sum_nanos.load(Ordering::Relaxed))
    }

    /// Get the minimum recorded latency.
    pub fn min(&self) -> Option<Duration> {
        let min = self.min_nanos.load(Ordering::Relaxed);
        if min == u64::MAX {
            None
        } else {
            Some(Duration::from_nanos(min))
        }
    }

    /// Get the maximum recorded latency.
    ///
    /// Returns `None` if no samples have been recorded.
    ///
    /// Note: `Some(Duration::ZERO)` is a valid return value when the maximum
    /// observed latency was zero (e.g. all samples were sub-nanosecond).
    pub fn max(&self) -> Option<Duration> {
        if self.count() == 0 {
            None
        } else {
            Some(Duration::from_nanos(self.max_nanos.load(Ordering::Relaxed)))
        }
    }

    /// Get the average latency.
    pub fn avg(&self) -> Option<Duration> {
        let count = self.count();
        let sum = self.sum_nanos.load(Ordering::Relaxed);
        sum.checked_div(count).map(Duration::from_nanos)
    }

    /// Estimate the p-th percentile latency from the histogram.
    ///
    /// `percentile` must be in `[0.0, 100.0]`. Returns `None` if no samples
    /// have been recorded.
    ///
    /// # Accuracy
    ///
    /// The estimate uses the sub-bucket midpoint for the bucket containing the
    /// target rank.  The **maximum relative error is ≤ 6.25 %** — each
    /// power-of-2 band is split into 8 equal sub-buckets of width `2^(i-3)`,
    /// so the midpoint of the containing sub-bucket is at most 6.25 % above
    /// the true value.
    pub fn percentile(&self, percentile: f64) -> Option<Duration> {
        let total = self.count();
        if total == 0 {
            return None;
        }
        // Clamp percentile to [0, 100].
        let p = percentile.clamp(0.0, 100.0);
        // Number of samples that must be at or below the target percentile.
        // Ensure target >= 1 so p=0 returns min, not bucket 0 artificially.
        let target = ((p / 100.0) * total as f64).ceil().max(1.0) as u64;
        let mut cumulative: u64 = 0;
        for (i, bucket) in self.histogram.iter().enumerate() {
            cumulative += bucket.load(Ordering::Relaxed);
            if cumulative >= target {
                return Some(Duration::from_nanos(Self::estimate_nanos_for_bucket(i)));
            }
        }
        // All counts are in the histogram; return max as fallback.
        self.max()
    }

    /// Get a snapshot of the latency statistics.
    pub fn snapshot(&self) -> LatencySnapshot {
        LatencySnapshot {
            count: self.count(),
            sum: self.sum(),
            min: self.min(),
            max: self.max(),
            avg: self.avg(),
            p50: self.percentile(50.0),
            p95: self.percentile(95.0),
            p99: self.percentile(99.0),
        }
    }

    /// Reset all statistics.
    ///
    /// # Consistency note
    ///
    /// This reset is **best-effort and not snapshot-consistent**. A concurrent
    /// [`snapshot()`](Self::snapshot) may observe any mix of pre- and
    /// post-reset values (e.g., `count = 0` with `sum_nanos > 0`). For most
    /// production use cases — periodic metric reporting windows — this is
    /// acceptable. If consistency is required, quiesce all recorders before
    /// calling `reset()`.
    pub fn reset(&self) {
        self.count.store(0, Ordering::Relaxed);
        self.sum_nanos.store(0, Ordering::Relaxed);
        self.min_nanos.store(u64::MAX, Ordering::Relaxed);
        self.max_nanos.store(0, Ordering::Relaxed);
        for bucket in &self.histogram {
            bucket.store(0, Ordering::Relaxed);
        }
    }
}

/// Guard that records latency when dropped.
pub struct LatencyGuard<'a> {
    tracker: &'a LatencyTracker,
    start: Instant,
}

impl Drop for LatencyGuard<'_> {
    fn drop(&mut self) {
        self.tracker.record(self.start.elapsed());
    }
}

/// Snapshot of latency statistics.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct LatencySnapshot {
    /// Number of recorded samples.
    pub count: u64,
    /// Sum of all recorded latencies.
    pub sum: Duration,
    /// Minimum recorded latency.
    pub min: Option<Duration>,
    /// Maximum recorded latency.
    pub max: Option<Duration>,
    /// Average latency.
    pub avg: Option<Duration>,
    /// 50th-percentile (median) latency estimate.
    ///
    /// Derived from a 512-bucket sub-divided power-of-2 histogram.  The
    /// relative error is **at most 6.25 %** (midpoint of the containing
    /// sub-bucket is at most half a sub-bucket above the true value).  Returns
    /// `None` when no samples have been recorded.
    pub p50: Option<Duration>,
    /// 95th-percentile latency estimate (same accuracy as `p50`, ≤ 6.25 %).
    pub p95: Option<Duration>,
    /// 99th-percentile latency estimate (same accuracy as `p50`, ≤ 6.25 %).
    pub p99: Option<Duration>,
}

/// Trait for exporting metrics to a pluggable backend.
///
/// Implement this trait to create custom metrics exporters for any
/// monitoring system (Prometheus, StatsD, OpenTelemetry, Datadog, etc.).
///
/// Each method receives the metric's fully-qualified name (with prefix),
/// a human-readable help string, and the current value.
///
/// # Example
///
/// ```rust
/// use krafka::metrics::{MetricsExporter, LatencySnapshot};
///
/// struct StdoutExporter;
///
/// impl MetricsExporter for StdoutExporter {
///     fn export_counter(&mut self, name: &str, help: &str, value: u64) {
///         println!("COUNTER {name} = {value} ({help})");
///     }
///     fn export_gauge(&mut self, name: &str, help: &str, value: u64) {
///         println!("GAUGE {name} = {value} ({help})");
///     }
///     fn export_latency(&mut self, name: &str, help: &str, snapshot: &LatencySnapshot) {
///         println!("LATENCY {name} count={} ({help})", snapshot.count);
///     }
/// }
/// ```
pub trait MetricsExporter {
    /// Export a monotonically increasing counter metric.
    fn export_counter(&mut self, name: &str, help: &str, value: u64);

    /// Export a gauge metric (current value that can go up or down).
    fn export_gauge(&mut self, name: &str, help: &str, value: u64);

    /// Export a latency tracker metric with count, sum, min, max, and avg.
    fn export_latency(&mut self, name: &str, help: &str, snapshot: &LatencySnapshot);

    /// Export a counter with attached key-value labels.
    ///
    /// The canonical form in Prometheus is:
    /// `metric_name_total{label_key="label_value"} <value>`.
    ///
    /// # Default implementation
    ///
    /// The default implementation embeds label values in the metric name
    /// (e.g. `name_<value>`) so the sample is at least *visible* in exporters
    /// that have no concept of labels.
    ///
    /// **This fallback is lossy and should be overridden by any exporter whose
    /// backend supports dimensions.** Mangling label values into the name means
    /// the backend cannot aggregate across label values, and an unbounded label
    /// domain (e.g. a topic name) mints an unbounded number of metric names.
    /// All exporters shipped with Krafka override it.
    fn export_labeled_counter(
        &mut self,
        name: &str,
        help: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) {
        self.export_counter(&mangle_labels_into_name(name, labels), help, value);
    }

    /// Export a gauge with attached key-value labels.
    ///
    /// The canonical form in Prometheus is:
    /// `metric_name{label_key="label_value"} <value>`.
    ///
    /// # Default implementation
    ///
    /// Mirrors [`export_labeled_counter`](Self::export_labeled_counter): the
    /// default embeds label values in the metric name and delegates to
    /// [`export_gauge`](Self::export_gauge). It carries the same caveats and
    /// should be overridden by exporters whose backend supports dimensions.
    ///
    /// This method has a default implementation so that adding it is not a
    /// breaking change for out-of-tree [`MetricsExporter`] implementations.
    fn export_labeled_gauge(
        &mut self,
        name: &str,
        help: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) {
        self.export_gauge(&mangle_labels_into_name(name, labels), help, value);
    }
}

/// Fallback name mangling for exporters that cannot represent labels.
///
/// Appends each label *value* to the metric name, separated by `_`. Returns
/// `name` unchanged when `labels` is empty.
///
/// Used only by the default [`MetricsExporter::export_labeled_counter`] /
/// [`MetricsExporter::export_labeled_gauge`] implementations — every exporter
/// in this crate overrides them with proper labeled output.
fn mangle_labels_into_name(name: &str, labels: &[(&str, &str)]) -> String {
    if labels.is_empty() {
        return name.to_string();
    }
    let mut out = String::with_capacity(name.len() + 16);
    out.push_str(name);
    for (_, v) in labels {
        out.push('_');
        out.push_str(v);
    }
    out
}

/// Trait for types that can export their metrics through a [`MetricsExporter`].
///
/// Each metrics struct (producer, consumer, connection) implements this trait
/// to emit its metrics to any exporter backend.
pub trait MetricsVisitable {
    /// Export all metrics to the given exporter using the provided prefix.
    ///
    /// The prefix is prepended to each metric name (e.g. `"krafka_producer"`),
    /// separated by an underscore.
    fn export_metrics(&self, prefix: &str, exporter: &mut dyn MetricsExporter);

    /// Convenience: export metrics as Prometheus text format.
    fn to_prometheus_text(&self, prefix: &str) -> String {
        let mut exporter = PrometheusExporter::new();
        self.export_metrics(prefix, &mut exporter);
        exporter.finish()
    }

    /// Convenience: export metrics as JSON.
    fn to_json(&self, prefix: &str) -> String {
        let mut exporter = JsonExporter::new();
        self.export_metrics(prefix, &mut exporter);
        exporter.finish()
    }
}

// ---------------------------------------------------------------------------
// PrometheusExporter
// ---------------------------------------------------------------------------

/// Exports metrics in Prometheus text exposition format.
///
/// Produces output compatible with Prometheus, Grafana Agent, and
/// OpenTelemetry's Prometheus receiver.
///
/// Each counter is emitted with a `_total` suffix. Latency trackers emit an
/// OpenMetrics-valid `_seconds` **summary** family (`_seconds_count`,
/// `_seconds_sum` and `{quantile="…"}` samples) plus separate `_min_seconds`
/// and `_max_seconds` **gauge** families — min/max are not legal members of a
/// summary family.
///
/// Labeled counters and gauges (e.g. per-topic metrics) are emitted as:
/// `metric_name_total{label_key="label_value"} <value>` and
/// `metric_name{label_key="label_value"} <value>` respectively.
pub struct PrometheusExporter {
    output: String,
    /// Tracks metric family names for which the `# HELP` / `# TYPE` header
    /// has already been emitted, ensuring each family header appears exactly
    /// once per scrape per the Prometheus text format specification.
    declared_families: std::collections::HashSet<String>,
}

impl PrometheusExporter {
    /// Create a new Prometheus exporter.
    pub fn new() -> Self {
        Self {
            output: String::with_capacity(4096),
            declared_families: std::collections::HashSet::new(),
        }
    }

    /// Consume the exporter and return the Prometheus text output.
    pub fn finish(self) -> String {
        self.output
    }
}

impl Default for PrometheusExporter {
    fn default() -> Self {
        Self::new()
    }
}

/// Sanitize a metric name to conform to Prometheus naming conventions.
///
/// Replaces any character that is not `[a-zA-Z0-9_:]` with `_`.
/// Ensures the name starts with a letter or underscore.
fn sanitize_prometheus_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == ':' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    // Ensure it starts with a letter or underscore.
    if out.starts_with(|c: char| c.is_ascii_digit()) {
        out.insert(0, '_');
    }
    out
}

/// Escape a Prometheus label value: backslash and double-quote must be escaped.
fn escape_prometheus_label_value(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for ch in v.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

impl MetricsExporter for PrometheusExporter {
    fn export_counter(&mut self, name: &str, help: &str, value: u64) {
        let name = sanitize_prometheus_name(name);
        let _ = writeln!(self.output, "# HELP {}_total {}", name, help);
        let _ = writeln!(self.output, "# TYPE {}_total counter", name);
        let _ = writeln!(self.output, "{}_total {}", name, value);
    }

    fn export_gauge(&mut self, name: &str, help: &str, value: u64) {
        let name = sanitize_prometheus_name(name);
        let _ = writeln!(self.output, "# HELP {} {}", name, help);
        let _ = writeln!(self.output, "# TYPE {} gauge", name);
        let _ = writeln!(self.output, "{} {}", name, value);
    }

    /// Export a labeled counter in proper Prometheus text format.
    ///
    /// Emits `# HELP` and `# TYPE` exactly once per metric family name
    /// (across all calls to this exporter instance), then emits one sample
    /// line per call:
    /// ```text
    /// krafka_producer_topic_records_sent_total{topic="orders"} 42
    /// ```
    fn export_labeled_counter(
        &mut self,
        name: &str,
        help: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) {
        let name = sanitize_prometheus_name(name);
        let full_name = format!("{name}_total");
        if !self.declared_families.contains(&full_name) {
            let _ = writeln!(self.output, "# HELP {} {}", full_name, help);
            let _ = writeln!(self.output, "# TYPE {} counter", full_name);
            self.declared_families.insert(full_name.clone());
        }
        if labels.is_empty() {
            let _ = writeln!(self.output, "{} {}", full_name, value);
        } else {
            let label_str = labels
                .iter()
                .map(|(k, v)| format!("{}=\"{}\"", k, escape_prometheus_label_value(v)))
                .collect::<Vec<_>>()
                .join(",");
            let _ = writeln!(self.output, "{}{{{}}} {}", full_name, label_str, value);
        }
    }

    /// Export a labeled gauge in proper Prometheus text format.
    ///
    /// Identical to [`export_labeled_counter`](Self::export_labeled_counter)
    /// except the family is typed `gauge` and carries no `_total` suffix:
    /// ```text
    /// krafka_producer_topic_buffered_records{topic="orders"} 7
    /// ```
    fn export_labeled_gauge(
        &mut self,
        name: &str,
        help: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) {
        let full_name = sanitize_prometheus_name(name);
        if !self.declared_families.contains(&full_name) {
            let _ = writeln!(self.output, "# HELP {} {}", full_name, help);
            let _ = writeln!(self.output, "# TYPE {} gauge", full_name);
            self.declared_families.insert(full_name.clone());
        }
        if labels.is_empty() {
            let _ = writeln!(self.output, "{} {}", full_name, value);
        } else {
            let label_str = labels
                .iter()
                .map(|(k, v)| format!("{}=\"{}\"", k, escape_prometheus_label_value(v)))
                .collect::<Vec<_>>()
                .join(",");
            let _ = writeln!(self.output, "{}{{{}}} {}", full_name, label_str, value);
        }
    }

    /// Export a latency tracker as an OpenMetrics-valid summary plus two gauges.
    ///
    /// The `{name}_seconds` **summary** family carries only the members the
    /// specification allows: `_count`, `_sum`, and `{quantile="…"}` samples.
    ///
    /// `min` and `max` are *not* valid members of a summary family — strict
    /// OpenMetrics parsers reject `{name}_seconds_min` / `{name}_seconds_max`
    /// under a `# TYPE {name}_seconds summary` header. They are therefore
    /// emitted as their own `gauge` families with distinct names that do not
    /// collide with the summary's member namespace:
    /// `{name}_min_seconds` and `{name}_max_seconds`.
    fn export_latency(&mut self, name: &str, help: &str, snapshot: &LatencySnapshot) {
        let name = sanitize_prometheus_name(name);
        let _ = writeln!(
            self.output,
            "# HELP {}_seconds {} (quantiles estimated from 512-bucket histogram, 8 sub-buckets per band; relative error ≤6.25%)",
            name, help
        );
        let _ = writeln!(self.output, "# TYPE {}_seconds summary", name);
        let _ = writeln!(self.output, "{}_seconds_count {}", name, snapshot.count);
        let _ = writeln!(
            self.output,
            "{}_seconds_sum {:.9}",
            name,
            snapshot.sum.as_secs_f64()
        );

        if let Some(p50) = snapshot.p50 {
            let _ = writeln!(
                self.output,
                "{}_seconds{{quantile=\"0.5\"}} {:.9}",
                name,
                p50.as_secs_f64()
            );
        }
        if let Some(p95) = snapshot.p95 {
            let _ = writeln!(
                self.output,
                "{}_seconds{{quantile=\"0.95\"}} {:.9}",
                name,
                p95.as_secs_f64()
            );
        }
        if let Some(p99) = snapshot.p99 {
            let _ = writeln!(
                self.output,
                "{}_seconds{{quantile=\"0.99\"}} {:.9}",
                name,
                p99.as_secs_f64()
            );
        }

        // min / max live in their own gauge families — see the doc comment.
        if let Some(min) = snapshot.min {
            let _ = writeln!(
                self.output,
                "# HELP {}_min_seconds {} (minimum observed)",
                name, help
            );
            let _ = writeln!(self.output, "# TYPE {}_min_seconds gauge", name);
            let _ = writeln!(self.output, "{}_min_seconds {:.9}", name, min.as_secs_f64());
        }
        if let Some(max) = snapshot.max {
            let _ = writeln!(
                self.output,
                "# HELP {}_max_seconds {} (maximum observed)",
                name, help
            );
            let _ = writeln!(self.output, "# TYPE {}_max_seconds gauge", name);
            let _ = writeln!(self.output, "{}_max_seconds {:.9}", name, max.as_secs_f64());
        }
    }
}

// ---------------------------------------------------------------------------
// JsonExporter
// ---------------------------------------------------------------------------

/// Exports metrics as a JSON array of metric objects.
///
/// Each metric becomes a JSON object with `name`, `type`, `help`, and
/// type-specific value fields. No `serde` dependency required.
///
/// # Output Format
///
/// ```json
/// [
///   {"name":"krafka_producer_records_sent","type":"counter","help":"Total records sent","value":42},
///   {"name":"krafka_producer_connections","type":"gauge","help":"Active connections","value":3},
///   {"name":"krafka_producer_send_latency","type":"latency","help":"Send latency","count":10,"sum_seconds":0.5,"min_seconds":0.01,"max_seconds":0.1,"avg_seconds":0.05}
/// ]
/// ```
pub struct JsonExporter {
    entries: Vec<String>,
}

impl JsonExporter {
    /// Create a new JSON exporter.
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// Consume the exporter and return the JSON output.
    pub fn finish(self) -> String {
        let mut output = String::with_capacity(self.entries.iter().map(|e| e.len() + 1).sum());
        output.push('[');
        for (i, entry) in self.entries.iter().enumerate() {
            if i > 0 {
                output.push(',');
            }
            output.push_str(entry);
        }
        output.push(']');
        output
    }

    /// Push a labeled metric entry with a nested `"labels"` object.
    fn push_labeled(
        &mut self,
        name: &str,
        kind: &str,
        help: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) {
        let labels_json = labels
            .iter()
            .map(|(k, v)| format!("\"{}\":\"{}\"", json_escape(k), json_escape(v)))
            .collect::<Vec<_>>()
            .join(",");
        self.entries.push(format!(
            "{{\"name\":\"{}\",\"type\":\"{}\",\"help\":\"{}\",\"labels\":{{{}}},\"value\":{}}}",
            json_escape(name),
            kind,
            json_escape(help),
            labels_json,
            value,
        ));
    }
}

impl Default for JsonExporter {
    fn default() -> Self {
        Self::new()
    }
}

/// Escape a string for JSON output (handles `"`, `\`, and control characters).
fn json_escape(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            c if c.is_control() => {
                let _ = write!(escaped, "\\u{:04x}", c as u32);
            }
            c => escaped.push(c),
        }
    }
    escaped
}

impl MetricsExporter for JsonExporter {
    fn export_counter(&mut self, name: &str, help: &str, value: u64) {
        self.entries.push(format!(
            "{{\"name\":\"{}\",\"type\":\"counter\",\"help\":\"{}\",\"value\":{}}}",
            json_escape(name),
            json_escape(help),
            value,
        ));
    }

    fn export_gauge(&mut self, name: &str, help: &str, value: u64) {
        self.entries.push(format!(
            "{{\"name\":\"{}\",\"type\":\"gauge\",\"help\":\"{}\",\"value\":{}}}",
            json_escape(name),
            json_escape(help),
            value,
        ));
    }

    /// Emit a labeled counter with the labels in a nested `"labels"` object.
    ///
    /// The metric `name` is emitted verbatim — label values are never folded
    /// into it.
    fn export_labeled_counter(
        &mut self,
        name: &str,
        help: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) {
        self.push_labeled(name, "counter", help, labels, value);
    }

    /// Emit a labeled gauge with the labels in a nested `"labels"` object.
    ///
    /// Mirrors [`export_labeled_counter`](Self::export_labeled_counter) with
    /// `"type":"gauge"`.
    fn export_labeled_gauge(
        &mut self,
        name: &str,
        help: &str,
        labels: &[(&str, &str)],
        value: u64,
    ) {
        self.push_labeled(name, "gauge", help, labels, value);
    }

    fn export_latency(&mut self, name: &str, help: &str, snapshot: &LatencySnapshot) {
        let min_str = snapshot
            .min
            .map(|d| format!("{:.9}", d.as_secs_f64()))
            .unwrap_or_else(|| "null".to_string());
        let max_str = snapshot
            .max
            .map(|d| format!("{:.9}", d.as_secs_f64()))
            .unwrap_or_else(|| "null".to_string());
        let avg_str = snapshot
            .avg
            .map(|d| format!("{:.9}", d.as_secs_f64()))
            .unwrap_or_else(|| "null".to_string());
        let p50_str = snapshot
            .p50
            .map(|d| format!("{:.9}", d.as_secs_f64()))
            .unwrap_or_else(|| "null".to_string());
        let p95_str = snapshot
            .p95
            .map(|d| format!("{:.9}", d.as_secs_f64()))
            .unwrap_or_else(|| "null".to_string());
        let p99_str = snapshot
            .p99
            .map(|d| format!("{:.9}", d.as_secs_f64()))
            .unwrap_or_else(|| "null".to_string());

        self.entries.push(format!(
            "{{\"name\":\"{}\",\"type\":\"latency\",\"help\":\"{}\",\"count\":{},\"sum_seconds\":{:.9},\"min_seconds\":{},\"max_seconds\":{},\"avg_seconds\":{},\"p50_seconds\":{},\"p95_seconds\":{},\"p99_seconds\":{}}}",
            json_escape(name),
            json_escape(help),
            snapshot.count,
            snapshot.sum.as_secs_f64(),
            min_str,
            max_str,
            avg_str,
            p50_str,
            p95_str,
            p99_str,
        ));
    }
}

// ---------------------------------------------------------------------------
// MetricsVisitable — ProducerMetrics
// ---------------------------------------------------------------------------

impl MetricsVisitable for ProducerMetrics {
    fn export_metrics(&self, prefix: &str, exporter: &mut dyn MetricsExporter) {
        exporter.export_counter(
            &format!("{prefix}_records_sent"),
            "Total number of records sent",
            self.records_sent.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_bytes_sent"),
            "Total bytes sent",
            self.bytes_sent.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_batches_sent"),
            "Total batches sent",
            self.batches_sent.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_errors"),
            "Total send errors",
            self.errors.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_retries"),
            "Total retries",
            self.retries.get(),
        );
        exporter.export_gauge(
            &format!("{prefix}_connections"),
            "Current active connections",
            self.connections.get(),
        );
        exporter.export_gauge(
            &format!("{prefix}_buffered_records"),
            "Producer records currently admitted under the memory budget",
            self.buffered_records.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_compressed_bytes"),
            "Total compressed bytes written for compressed batches",
            self.compressed_bytes.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_uncompressed_bytes"),
            "Total uncompressed bytes for the same compressed batches",
            self.uncompressed_bytes.get(),
        );
        exporter.export_latency(
            &format!("{prefix}_send_latency"),
            "Send latency",
            &self.send_latency.snapshot(),
        );
        // Per-topic counters: take a snapshot (a lock-free `ArcSwap` load)
        // before calling the exporter, so no producer hot-path call is ever
        // blocked by slow exporter I/O.  Topics are sorted so the output is
        // deterministic.  Metric names are label-free; the topic travels as a
        // `topic` label/attribute so backends can aggregate across topics.
        // Topics beyond the tracking cap appear once under `__other__`.
        let name_records = format!("{prefix}_topic_records_sent");
        let name_bytes = format!("{prefix}_topic_bytes_sent");
        let name_errors = format!("{prefix}_topic_errors");
        for snap in self.topic_snapshot() {
            let labels = [("topic", snap.topic.as_str())];
            exporter.export_labeled_counter(
                &name_records,
                "Records sent to this topic",
                &labels,
                snap.records_sent,
            );
            exporter.export_labeled_counter(
                &name_bytes,
                "Bytes sent to this topic",
                &labels,
                snap.bytes_sent,
            );
            exporter.export_labeled_counter(
                &name_errors,
                "Send errors for this topic",
                &labels,
                snap.errors,
            );
        }
    }
}

// ---------------------------------------------------------------------------
// MetricsVisitable — ConsumerMetrics
// ---------------------------------------------------------------------------

impl MetricsVisitable for ConsumerMetrics {
    fn export_metrics(&self, prefix: &str, exporter: &mut dyn MetricsExporter) {
        exporter.export_counter(
            &format!("{prefix}_records_received"),
            "Total records received",
            self.records_received.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_bytes_received"),
            "Total bytes received",
            self.bytes_received.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_fetches"),
            "Total fetch requests",
            self.fetches.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_polls"),
            "Total poll operations",
            self.polls.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_empty_polls"),
            "Total empty polls",
            self.empty_polls.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_commits"),
            "Total commit operations",
            self.commits.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_errors"),
            "Total errors",
            self.errors.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_rebalances"),
            "Total rebalances",
            self.rebalances.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_seeks"),
            "Total seek operations (seek + seek_many partition count)",
            self.seeks.get(),
        );
        exporter.export_gauge(
            &format!("{prefix}_lag"),
            "Total consumer lag across all assigned partitions",
            self.lag.get(),
        );
        exporter.export_gauge(
            &format!("{prefix}_lag_max"),
            "Maximum per-partition consumer lag",
            self.lag_max.get(),
        );
        exporter.export_gauge(
            &format!("{prefix}_assigned_partitions"),
            "Currently assigned partitions",
            self.assigned_partitions.get(),
        );
        exporter.export_gauge(
            &format!("{prefix}_paused_partitions"),
            "Currently paused partitions",
            self.paused_partitions.get(),
        );
        exporter.export_gauge(
            &format!("{prefix}_buffered_records"),
            "Currently buffered records in recv() buffer",
            self.buffered_records.get(),
        );
        exporter.export_latency(
            &format!("{prefix}_poll_latency"),
            "Poll latency",
            &self.poll_latency.snapshot(),
        );
        exporter.export_latency(
            &format!("{prefix}_fetch_latency"),
            "Fetch latency",
            &self.fetch_latency.snapshot(),
        );
    }
}

// ---------------------------------------------------------------------------
// MetricsVisitable — ConnectionMetrics
// ---------------------------------------------------------------------------

impl MetricsVisitable for ConnectionMetrics {
    fn export_metrics(&self, prefix: &str, exporter: &mut dyn MetricsExporter) {
        exporter.export_counter(
            &format!("{prefix}_connections_created"),
            "Total connections created",
            self.connections_created.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_connections_closed"),
            "Total connections closed",
            self.connections_closed.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_connection_errors"),
            "Total connection errors",
            self.connection_errors.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_high_priority_requests"),
            "Total high-priority requests sent",
            self.high_priority_requests.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_normal_priority_requests"),
            "Total normal-priority requests sent",
            self.normal_priority_requests.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_high_priority_bypasses"),
            "High-priority requests processed ahead of normal-priority work",
            self.high_priority_bypasses.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_high_priority_bypass_yields"),
            "Forced normal-priority drain steps after exhausting the high-priority bypass budget",
            self.high_priority_bypass_yields.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_throttle_delays"),
            "Normal-priority requests delayed due to broker throttling",
            self.throttle_delays.get(),
        );
        exporter.export_counter(
            &format!("{prefix}_throttle_delay_ms"),
            "Total broker-throttle delay applied to normal-priority requests in milliseconds",
            self.throttle_delay_ms.get(),
        );
        exporter.export_gauge(
            &format!("{prefix}_active_connections"),
            "Current active connections",
            self.active_connections.get(),
        );
        exporter.export_latency(
            &format!("{prefix}_connect_latency"),
            "Connection establishment latency",
            &self.connect_latency.snapshot(),
        );
        exporter.export_latency(
            &format!("{prefix}_tls_handshake_latency"),
            "TLS handshake latency (TLS connections only)",
            &self.tls_handshake_latency.snapshot(),
        );
    }
}

/// Aggregated metrics registry for all Krafka components.
///
/// This provides a convenient way to collect and export metrics from
/// multiple producers, consumers, and connections through any
/// [`MetricsExporter`] backend.
///
/// # Example
///
/// ```rust
/// use krafka::metrics::KrafkaMetrics;
///
/// let metrics = KrafkaMetrics::new();
///
/// // Get shared metrics handles
/// let producer_metrics = metrics.producer_metrics();
/// let consumer_metrics = metrics.consumer_metrics();
///
/// // Record some metrics
/// producer_metrics.record_send(100);
/// consumer_metrics.record_poll(5);
///
/// // Export all metrics in Prometheus format
/// let output = metrics.to_prometheus_text();
/// println!("{}", output);
///
/// // Export all metrics as JSON
/// let json = metrics.to_json();
/// println!("{}", json);
/// ```
#[derive(Debug, Clone)]
pub struct KrafkaMetrics {
    /// Producer metrics.
    producer: Arc<ProducerMetrics>,
    /// Consumer metrics.
    consumer: Arc<ConsumerMetrics>,
    /// Connection metrics.
    connection: Arc<ConnectionMetrics>,
    /// Monotonically increasing counter bumped by [`KrafkaMetrics::reset`].
    ///
    /// Shared across clones so every observer sees the same generation.
    reset_generation: Arc<AtomicU64>,
}

impl Default for KrafkaMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl KrafkaMetrics {
    /// Create a new metrics registry.
    ///
    /// Producer per-topic metrics use [`DEFAULT_MAX_TRACKED_TOPICS`].
    pub fn new() -> Self {
        Self::with_max_tracked_topics(DEFAULT_MAX_TRACKED_TOPICS)
    }

    /// Create a new metrics registry with a custom producer per-topic cap.
    ///
    /// See [`ProducerMetrics::with_max_tracked_topics`].
    pub fn with_max_tracked_topics(max_tracked_topics: usize) -> Self {
        Self {
            producer: Arc::new(ProducerMetrics::with_max_tracked_topics(max_tracked_topics)),
            consumer: Arc::new(ConsumerMetrics::new()),
            connection: Arc::new(ConnectionMetrics::new()),
            reset_generation: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Number of times [`reset`](Self::reset) has been called on this registry.
    ///
    /// Counters exposed by this registry are **not** monotonic across a
    /// `reset()` — they rewind to zero. Consumers that compute deltas (such as
    /// the KIP-714 telemetry reporter, which does `current - previous` with a
    /// saturating subtraction) must therefore treat a change in this value as
    /// an instruction to drop their cached baselines; otherwise every delta
    /// reads as `0` until the counters climb back past their pre-reset values.
    ///
    /// The generation itself is monotonic and never reset. Poll it alongside
    /// each collection:
    ///
    /// ```rust
    /// use krafka::metrics::KrafkaMetrics;
    ///
    /// let metrics = KrafkaMetrics::new();
    /// let mut seen_generation = metrics.reset_generation();
    ///
    /// // ... later, on each reporting tick:
    /// let generation = metrics.reset_generation();
    /// if generation != seen_generation {
    ///     // drop delta baselines here
    ///     seen_generation = generation;
    /// }
    /// # let _ = seen_generation;
    /// ```
    #[must_use]
    pub fn reset_generation(&self) -> u64 {
        self.reset_generation.load(Ordering::Relaxed)
    }

    /// Get shared producer metrics handle.
    #[must_use]
    pub fn producer_metrics(&self) -> Arc<ProducerMetrics> {
        self.producer.clone()
    }

    /// Get shared consumer metrics handle.
    #[must_use]
    pub fn consumer_metrics(&self) -> Arc<ConsumerMetrics> {
        self.consumer.clone()
    }

    /// Get shared connection metrics handle.
    #[must_use]
    pub fn connection_metrics(&self) -> Arc<ConnectionMetrics> {
        self.connection.clone()
    }

    /// Export all metrics through a custom [`MetricsExporter`].
    ///
    /// Uses the standard `"krafka"` prefix (`krafka_producer_*`,
    /// `krafka_consumer_*`, `krafka_connection_*`).
    pub fn export_all(&self, exporter: &mut dyn MetricsExporter) {
        self.export_all_with_prefix("krafka", exporter);
    }

    /// Export all metrics through a custom [`MetricsExporter`] with a custom prefix.
    pub fn export_all_with_prefix(&self, prefix: &str, exporter: &mut dyn MetricsExporter) {
        self.producer
            .export_metrics(&format!("{prefix}_producer"), exporter);
        self.consumer
            .export_metrics(&format!("{prefix}_consumer"), exporter);
        self.connection
            .export_metrics(&format!("{prefix}_connection"), exporter);
    }

    /// Export all metrics in Prometheus text format.
    ///
    /// Uses the standard "krafka_" prefix for all metric names.
    pub fn to_prometheus_text(&self) -> String {
        self.to_prometheus_text_with_prefix("krafka")
    }

    /// Export all metrics in Prometheus text format with custom prefix.
    pub fn to_prometheus_text_with_prefix(&self, prefix: &str) -> String {
        let mut exporter = PrometheusExporter::new();
        self.export_all_with_prefix(prefix, &mut exporter);
        exporter.finish()
    }

    /// Export all metrics as JSON.
    ///
    /// Uses the standard "krafka_" prefix for all metric names.
    pub fn to_json(&self) -> String {
        self.to_json_with_prefix("krafka")
    }

    /// Export all metrics as JSON with custom prefix.
    pub fn to_json_with_prefix(&self, prefix: &str) -> String {
        let mut exporter = JsonExporter::new();
        self.export_all_with_prefix(prefix, &mut exporter);
        exporter.finish()
    }

    /// Reset all metrics.
    ///
    /// This rewinds counters that are otherwise monotonic, and bumps
    /// [`reset_generation`](Self::reset_generation) so delta-computing
    /// consumers can invalidate their baselines. The generation is bumped
    /// *before* the counters are cleared, so an observer that reads the
    /// generation after reading a counter can never pair a post-reset counter
    /// value with a pre-reset generation.
    pub fn reset(&self) {
        self.reset_generation.fetch_add(1, Ordering::Relaxed);

        self.producer.records_sent.reset();
        self.producer.bytes_sent.reset();
        self.producer.batches_sent.reset();
        self.producer.errors.reset();
        self.producer.retries.reset();
        self.producer.send_latency.reset();

        self.consumer.records_received.reset();
        self.consumer.bytes_received.reset();
        self.consumer.fetches.reset();
        self.consumer.polls.reset();
        self.consumer.empty_polls.reset();
        self.consumer.commits.reset();
        self.consumer.errors.reset();
        self.consumer.rebalances.reset();
        self.consumer.seeks.reset();
        self.consumer.poll_latency.reset();
        self.consumer.fetch_latency.reset();
        self.consumer.lag.set(0);
        self.consumer.lag_max.set(0);
        self.consumer.assigned_partitions.set(0);
        self.consumer.paused_partitions.set(0);
        self.consumer.buffered_records.set(0);

        self.producer.connections.set(0);
        self.producer.buffered_records.set(0);
        self.producer.compressed_bytes.reset();
        self.producer.uncompressed_bytes.reset();
        self.producer.clear_topic_metrics();

        self.connection.connections_created.reset();
        self.connection.connections_closed.reset();
        self.connection.connection_errors.reset();
        self.connection.high_priority_requests.reset();
        self.connection.normal_priority_requests.reset();
        self.connection.high_priority_bypasses.reset();
        self.connection.high_priority_bypass_yields.reset();
        self.connection.throttle_delays.reset();
        self.connection.throttle_delay_ms.reset();
        self.connection.active_connections.set(0);
        self.connection.connect_latency.reset();
        self.connection.tls_handshake_latency.reset();
    }
}

/// Default maximum number of distinct topics tracked by [`ProducerMetrics`].
///
/// See [`ProducerMetrics::with_max_tracked_topics`] for how to change it and
/// [`OVERFLOW_TOPIC_KEY`] for what happens beyond the cap.
pub const DEFAULT_MAX_TRACKED_TOPICS: usize = 1000;

/// Topic key under which all metrics beyond the per-topic cap are aggregated.
///
/// Once [`ProducerMetrics`] tracks [`DEFAULT_MAX_TRACKED_TOPICS`] (or the value
/// passed to [`ProducerMetrics::with_max_tracked_topics`]) distinct topics,
/// every further topic is folded into a single bucket under this key instead of
/// allocating a new entry. This bounds both memory and the number of exported
/// time series for workloads with an unbounded topic domain (e.g. a CDC/outbox
/// producer writing `cdc.tenant_<uuid>.events`).
///
/// The overflow bucket does not itself count against the cap.
pub const OVERFLOW_TOPIC_KEY: &str = "__other__";

/// Per-topic counters tracked inside [`ProducerMetrics`].
///
/// Accessed via [`ProducerMetrics::topic_snapshot`].
#[derive(Debug, Default)]
pub struct TopicProducerMetrics {
    /// Number of records sent successfully to this topic.
    pub records_sent: Counter,
    /// Number of bytes sent to this topic.
    pub bytes_sent: Counter,
    /// Number of send errors for this topic.
    pub errors: Counter,
}

/// Snapshot of per-topic producer metrics.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TopicProducerMetricsSnapshot {
    /// Topic name.
    pub topic: String,
    /// Number of records sent successfully.
    pub records_sent: u64,
    /// Number of bytes sent.
    pub bytes_sent: u64,
    /// Number of send errors.
    pub errors: u64,
}

/// Producer metrics.
///
/// # Per-topic metrics
///
/// Per-topic counters are stored in a lock-free [`arc_swap::ArcSwap`] map: the
/// per-record hot path performs a `load()` plus a hash lookup and bumps
/// atomics, never taking an exclusive lock. Only registering a *new* topic
/// takes a short write mutex and publishes a copy-on-write replacement map.
///
/// The number of distinct topics is capped (see
/// [`DEFAULT_MAX_TRACKED_TOPICS`] and
/// [`with_max_tracked_topics`](Self::with_max_tracked_topics)); topics beyond
/// the cap are aggregated under [`OVERFLOW_TOPIC_KEY`].
#[derive(Debug)]
pub struct ProducerMetrics {
    /// Number of records sent successfully.
    pub records_sent: Counter,
    /// Number of bytes sent (record values only).
    pub bytes_sent: Counter,
    /// Number of batches sent.
    pub batches_sent: Counter,
    /// Number of send errors.
    pub errors: Counter,
    /// Number of retries.
    pub retries: Counter,
    /// Send latency (time from send call to ack).
    pub send_latency: LatencyTracker,
    /// Current number of active connections.
    pub connections: Gauge,
    /// Producer records currently admitted under the memory budget.
    pub buffered_records: Gauge,
    /// Estimated compressed bytes written to the wire for compressed batches
    /// (numerator for compression ratio).
    ///
    /// These values are derived from batch-size estimates and are best-effort
    /// rather than exact protocol-frame byte counts.
    pub compressed_bytes: Counter,
    /// Estimated uncompressed bytes for the same compressed batches
    /// (denominator for compression ratio).
    ///
    /// These values are derived from batch-size estimates and are best-effort
    /// rather than exact protocol-frame byte counts.
    pub uncompressed_bytes: Counter,
    /// Per-topic counters (records_sent, bytes_sent, errors).
    ///
    /// Populated by [`record_send_for_topic`](Self::record_send_for_topic) and
    /// [`record_error_for_topic`](Self::record_error_for_topic).
    /// Exported via [`export_metrics`](MetricsVisitable::export_metrics) as
    /// `{prefix}_topic_records_sent_total{topic="<name>"}`,
    /// `{prefix}_topic_bytes_sent_total{topic="<name>"}`, and
    /// `{prefix}_topic_errors_total{topic="<name>"}`.
    ///
    /// Read lock-free via `ArcSwap`; replaced copy-on-write under
    /// `topic_write_lock` when a new topic is registered.
    topic_metrics: arc_swap::ArcSwap<AHashMap<String, Arc<TopicProducerMetrics>>>,
    /// Serialises copy-on-write publication of `topic_metrics`. Taken only on
    /// the rare new-topic path, never per record.
    topic_write_lock: parking_lot::Mutex<()>,
    /// Maximum number of distinct topics tracked individually before falling
    /// back to the [`OVERFLOW_TOPIC_KEY`] bucket.
    max_tracked_topics: usize,
}

impl Default for ProducerMetrics {
    fn default() -> Self {
        Self::with_max_tracked_topics(DEFAULT_MAX_TRACKED_TOPICS)
    }
}

impl ProducerMetrics {
    /// Create new producer metrics tracking up to
    /// [`DEFAULT_MAX_TRACKED_TOPICS`] distinct topics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create new producer metrics with a custom per-topic cap.
    ///
    /// At most `max_tracked_topics` distinct topics get their own counters;
    /// every topic seen after the cap is reached is aggregated into a single
    /// bucket keyed [`OVERFLOW_TOPIC_KEY`] (which does not itself count against
    /// the cap). This bounds memory and exported series cardinality for
    /// producers with an unbounded topic domain.
    ///
    /// A cap of `0` routes *all* per-topic metrics into the overflow bucket.
    pub fn with_max_tracked_topics(max_tracked_topics: usize) -> Self {
        Self {
            records_sent: Counter::default(),
            bytes_sent: Counter::default(),
            batches_sent: Counter::default(),
            errors: Counter::default(),
            retries: Counter::default(),
            send_latency: LatencyTracker::new(),
            connections: Gauge::default(),
            buffered_records: Gauge::default(),
            compressed_bytes: Counter::default(),
            uncompressed_bytes: Counter::default(),
            topic_metrics: arc_swap::ArcSwap::from_pointee(AHashMap::new()),
            topic_write_lock: parking_lot::Mutex::new(()),
            max_tracked_topics,
        }
    }

    /// The configured maximum number of individually tracked topics.
    ///
    /// See [`with_max_tracked_topics`](Self::with_max_tracked_topics).
    #[must_use]
    pub fn max_tracked_topics(&self) -> usize {
        self.max_tracked_topics
    }

    /// Number of topic buckets currently tracked, including the
    /// [`OVERFLOW_TOPIC_KEY`] bucket if it exists.
    #[must_use]
    pub fn tracked_topic_count(&self) -> usize {
        self.topic_metrics.load().len()
    }

    /// Drop all per-topic counters.
    ///
    /// Publishes a fresh empty map; concurrent recorders holding a previously
    /// loaded snapshot may still bump the old entries, whose values are then
    /// discarded.
    pub fn clear_topic_metrics(&self) {
        let _write = self.topic_write_lock.lock();
        self.topic_metrics.store(Arc::new(AHashMap::new()));
    }

    /// Run `f` against the counters for `topic`, registering the topic if it is
    /// new.
    ///
    /// Hot path: a lock-free `ArcSwap::load()` plus a hash lookup. Once the cap
    /// is reached, unknown topics resolve to the already-published overflow
    /// bucket — also lock-free. Only the first sighting of a topic that still
    /// fits under the cap falls through to [`Self::register_topic`], which takes
    /// the write mutex.
    #[inline]
    fn with_topic<F: FnOnce(&TopicProducerMetrics)>(&self, topic: &str, f: F) {
        let guard = self.topic_metrics.load();
        if let Some(m) = guard.get(topic) {
            f(m);
            return;
        }
        // Cap already reached and the overflow bucket is published: no lock.
        if Self::named_topic_count(&guard) >= self.max_tracked_topics
            && let Some(m) = guard.get(OVERFLOW_TOPIC_KEY)
        {
            f(m);
            return;
        }
        drop(guard);
        f(&self.register_topic(topic));
    }

    /// Number of entries in `map` excluding the overflow bucket.
    #[inline]
    fn named_topic_count(map: &AHashMap<String, Arc<TopicProducerMetrics>>) -> usize {
        map.len() - usize::from(map.contains_key(OVERFLOW_TOPIC_KEY))
    }

    /// Register `topic` (or resolve the overflow bucket) under the write lock.
    ///
    /// Re-checks the map after acquiring the lock so two threads racing on the
    /// same new topic end up sharing one entry rather than clobbering each
    /// other's copy-on-write publication.
    #[cold]
    fn register_topic(&self, topic: &str) -> Arc<TopicProducerMetrics> {
        let _write = self.topic_write_lock.lock();
        let current = self.topic_metrics.load();

        // Another thread may have registered it while we waited for the lock.
        if let Some(m) = current.get(topic) {
            return Arc::clone(m);
        }

        // Enforce the cap: beyond it, everything lands in the overflow bucket.
        let key: &str = if Self::named_topic_count(&current) >= self.max_tracked_topics {
            OVERFLOW_TOPIC_KEY
        } else {
            topic
        };
        if let Some(m) = current.get(key) {
            return Arc::clone(m);
        }

        let entry = Arc::new(TopicProducerMetrics::default());
        let mut next = (**current).clone();
        next.insert(key.to_string(), Arc::clone(&entry));
        self.topic_metrics.store(Arc::new(next));
        entry
    }

    /// Record a successful send.
    #[inline]
    pub fn record_send(&self, bytes: u64) {
        self.records_sent.inc();
        self.bytes_sent.add(bytes);
    }

    /// Record a batch send.
    #[inline]
    pub fn record_batch(&self, records: u64) {
        self.batches_sent.inc();
        self.records_sent.add(records);
    }

    /// Record an error.
    #[inline]
    pub fn record_error(&self) {
        self.errors.inc();
    }

    /// Record a send error for a specific topic.
    ///
    /// Updates both the global error counter and the per-topic error counter.
    /// Topics beyond the tracking cap are aggregated under
    /// [`OVERFLOW_TOPIC_KEY`].
    #[inline]
    pub fn record_error_for_topic(&self, topic: &str) {
        self.errors.inc();
        self.with_topic(topic, |m| m.errors.inc());
    }

    /// Record a successful send and update per-topic counters.
    ///
    /// This method updates both the global counters and the per-topic
    /// `records_sent` and `bytes_sent` for the given topic name. Topics beyond
    /// the tracking cap are aggregated under [`OVERFLOW_TOPIC_KEY`].
    #[inline]
    pub fn record_send_for_topic(&self, topic: &str, bytes: u64) {
        self.records_sent.inc();
        self.bytes_sent.add(bytes);
        self.with_topic(topic, |m| {
            m.records_sent.inc();
            m.bytes_sent.add(bytes);
        });
    }

    /// Record a successful batch send and update per-topic counters.
    ///
    /// Increments the global `batches_sent`, `records_sent` (by `records`), and
    /// `bytes_sent` (by `bytes`) counters, and mirrors `records_sent` and
    /// `bytes_sent` into the per-topic entry for `topic`. Topics beyond the
    /// tracking cap are aggregated under [`OVERFLOW_TOPIC_KEY`].
    #[inline]
    pub fn record_batch_for_topic(&self, topic: &str, records: u64, bytes: u64) {
        self.batches_sent.inc();
        self.records_sent.add(records);
        self.bytes_sent.add(bytes);
        self.with_topic(topic, |m| {
            m.records_sent.add(records);
            m.bytes_sent.add(bytes);
        });
    }

    /// Return per-topic metric snapshots sorted by topic name.
    ///
    /// Includes the [`OVERFLOW_TOPIC_KEY`] bucket (aggregating every topic seen
    /// beyond the cap) when it exists.
    pub fn topic_snapshot(&self) -> Vec<TopicProducerMetricsSnapshot> {
        let map = self.topic_metrics.load();
        let mut out: Vec<TopicProducerMetricsSnapshot> = map
            .iter()
            .map(|(topic, m)| TopicProducerMetricsSnapshot {
                topic: topic.clone(),
                records_sent: m.records_sent.get(),
                bytes_sent: m.bytes_sent.get(),
                errors: m.errors.get(),
            })
            .collect();
        out.sort_unstable_by(|a, b| a.topic.cmp(&b.topic));
        out
    }

    /// Record a retry.
    #[inline]
    pub fn record_retry(&self) {
        self.retries.inc();
    }

    /// Record estimated bytes before and after compression for a batch.
    ///
    /// Only call this for batches that actually used compression
    /// (`compression != Compression::None`). Values are based on size
    /// estimates used by the accumulator and are best-effort. Passing equal
    /// values (e.g. for an incompressible batch) is valid and contributes a
    /// ratio of 1.0 to the running average.
    #[inline]
    pub fn record_compression(&self, compressed: u64, uncompressed: u64) {
        self.compressed_bytes.add(compressed);
        self.uncompressed_bytes.add(uncompressed);
    }

    /// Get a snapshot of all metrics.
    pub fn snapshot(&self) -> ProducerMetricsSnapshot {
        let compressed = self.compressed_bytes.get();
        let uncompressed = self.uncompressed_bytes.get();
        let compression_ratio_avg = if uncompressed > 0 {
            Some(compressed as f64 / uncompressed as f64)
        } else {
            None
        };
        ProducerMetricsSnapshot {
            records_sent: self.records_sent.get(),
            bytes_sent: self.bytes_sent.get(),
            batches_sent: self.batches_sent.get(),
            errors: self.errors.get(),
            retries: self.retries.get(),
            send_latency: self.send_latency.snapshot(),
            connections: self.connections.get(),
            buffered_records: self.buffered_records.get(),
            compressed_bytes: compressed,
            uncompressed_bytes: uncompressed,
            compression_ratio_avg,
            topic_metrics: self.topic_snapshot(),
        }
    }
}

/// Snapshot of producer metrics.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ProducerMetricsSnapshot {
    /// Number of records sent successfully.
    pub records_sent: u64,
    /// Number of bytes sent.
    pub bytes_sent: u64,
    /// Number of batches sent.
    pub batches_sent: u64,
    /// Number of errors.
    pub errors: u64,
    /// Number of retries.
    pub retries: u64,
    /// Send latency statistics.
    pub send_latency: LatencySnapshot,
    /// Current connections.
    pub connections: u64,
    /// Records currently admitted under the producer memory budget.
    pub buffered_records: u64,
    /// Total estimated compressed bytes for compressed batches.
    pub compressed_bytes: u64,
    /// Total estimated uncompressed bytes for the same compressed batches.
    pub uncompressed_bytes: u64,
    /// Average estimated compression ratio (`compressed_bytes / uncompressed_bytes`).
    ///
    /// Values `< 1.0` indicate net compression; values `> 1.0` indicate
    /// expansion (possible for incompressible or already-compressed inputs).
    /// `None` when no compressed batches have been sent yet.
    pub compression_ratio_avg: Option<f64>,
    /// Per-topic metric snapshots sorted by topic name.
    ///
    /// Populated from [`ProducerMetrics::topic_snapshot`]. Empty until at least
    /// one call to [`record_send_for_topic`](ProducerMetrics::record_send_for_topic)
    /// or [`record_error_for_topic`](ProducerMetrics::record_error_for_topic).
    pub topic_metrics: Vec<TopicProducerMetricsSnapshot>,
}

/// Consumer metrics.
#[derive(Debug, Default)]
pub struct ConsumerMetrics {
    /// Number of records received.
    pub records_received: Counter,
    /// Number of bytes received (record values only).
    pub bytes_received: Counter,
    /// Number of fetch requests.
    pub fetches: Counter,
    /// Number of poll calls.
    pub polls: Counter,
    /// Number of empty polls (no records).
    pub empty_polls: Counter,
    /// Number of commit operations.
    pub commits: Counter,
    /// Number of errors.
    pub errors: Counter,
    /// Number of rebalances.
    pub rebalances: Counter,
    /// Poll latency.
    pub poll_latency: LatencyTracker,
    /// Fetch latency.
    pub fetch_latency: LatencyTracker,
    /// Total consumer lag across all assigned partitions (records behind).
    pub lag: Gauge,
    /// Maximum per-partition lag across all assigned partitions.
    pub lag_max: Gauge,
    /// Current number of assigned partitions.
    pub assigned_partitions: Gauge,
    /// Current number of paused partitions.
    pub paused_partitions: Gauge,
    /// Current number of records buffered in the recv() buffer.
    pub buffered_records: Gauge,
    /// Total number of seek operations (seek + seek_many).
    pub seeks: Counter,
    /// Record batches the consumer could not decode.
    ///
    /// Counts CRC mismatches, unsupported magic bytes and out-of-range fields
    /// in fetched record batches — i.e. on-the-wire or on-disk corruption.
    /// Trailing batches cut short by the fetch size limit are **not** counted:
    /// those are expected and the next fetch re-requests them.
    ///
    /// Any sustained non-zero rate here means a partition is failing to
    /// advance; correlate with the `warn!` that accompanies each increment,
    /// which names the topic, partition and offset.
    pub batch_decode_errors: Counter,
}

impl ConsumerMetrics {
    /// Create new consumer metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a seek operation.
    ///
    /// Pass `n = 1` for a single-partition seek, or the number of partitions for `seek_many`.
    #[inline]
    pub fn record_seek(&self, n: u64) {
        self.seeks.add(n);
    }

    /// Record records received.
    #[inline]
    pub fn record_receive(&self, records: u64, bytes: u64) {
        self.records_received.add(records);
        self.bytes_received.add(bytes);
    }

    /// Record a poll operation.
    #[inline]
    pub fn record_poll(&self, records: u64) {
        self.polls.inc();
        if records == 0 {
            self.empty_polls.inc();
        }
    }

    /// Record a fetch.
    #[inline]
    pub fn record_fetch(&self) {
        self.fetches.inc();
    }

    /// Record a record batch that failed to decode because it was corrupt.
    ///
    /// See [`ConsumerMetrics::batch_decode_errors`] for what does and does not
    /// count.
    #[inline]
    pub fn record_batch_decode_error(&self) {
        self.batch_decode_errors.inc();
    }

    /// Record a commit.
    #[inline]
    pub fn record_commit(&self) {
        self.commits.inc();
    }

    /// Record an error.
    #[inline]
    pub fn record_error(&self) {
        self.errors.inc();
    }

    /// Record a rebalance.
    #[inline]
    pub fn record_rebalance(&self) {
        self.rebalances.inc();
    }

    /// Get a snapshot of all metrics.
    pub fn snapshot(&self) -> ConsumerMetricsSnapshot {
        ConsumerMetricsSnapshot {
            records_received: self.records_received.get(),
            bytes_received: self.bytes_received.get(),
            fetches: self.fetches.get(),
            polls: self.polls.get(),
            empty_polls: self.empty_polls.get(),
            commits: self.commits.get(),
            errors: self.errors.get(),
            rebalances: self.rebalances.get(),
            poll_latency: self.poll_latency.snapshot(),
            fetch_latency: self.fetch_latency.snapshot(),
            lag: self.lag.get(),
            lag_max: self.lag_max.get(),
            assigned_partitions: self.assigned_partitions.get(),
            paused_partitions: self.paused_partitions.get(),
            buffered_records: self.buffered_records.get(),
            seeks: self.seeks.get(),
            batch_decode_errors: self.batch_decode_errors.get(),
        }
    }
}

/// Snapshot of consumer metrics.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ConsumerMetricsSnapshot {
    /// Number of records received.
    pub records_received: u64,
    /// Number of bytes received.
    pub bytes_received: u64,
    /// Number of fetches.
    pub fetches: u64,
    /// Number of polls.
    pub polls: u64,
    /// Number of empty polls.
    pub empty_polls: u64,
    /// Number of commits.
    pub commits: u64,
    /// Number of errors.
    pub errors: u64,
    /// Number of rebalances.
    pub rebalances: u64,
    /// Poll latency statistics.
    pub poll_latency: LatencySnapshot,
    /// Fetch latency statistics.
    pub fetch_latency: LatencySnapshot,
    /// Total consumer lag across all assigned partitions.
    pub lag: u64,
    /// Maximum per-partition lag.
    pub lag_max: u64,
    /// Assigned partitions.
    pub assigned_partitions: u64,
    /// Paused partitions.
    pub paused_partitions: u64,
    /// Buffered records in recv() buffer.
    pub buffered_records: u64,
    /// Total seek operations (seek + seek_many partition count).
    pub seeks: u64,
    /// Record batches that failed to decode because they were corrupt.
    ///
    /// See [`ConsumerMetrics::batch_decode_errors`].
    pub batch_decode_errors: u64,
}

/// Connection pool metrics.
#[derive(Debug, Default)]
pub struct ConnectionMetrics {
    /// Number of connections created.
    pub connections_created: Counter,
    /// Number of connections closed.
    pub connections_closed: Counter,
    /// Number of connection errors.
    pub connection_errors: Counter,
    /// Number of high-priority requests sent.
    pub high_priority_requests: Counter,
    /// Number of normal-priority requests sent.
    pub normal_priority_requests: Counter,
    /// Number of high-priority requests processed ahead of normal-priority work.
    pub high_priority_bypasses: Counter,
    /// Number of fairness yields that forced one normal-priority drain after the
    /// high-priority bypass budget was exhausted.
    pub high_priority_bypass_yields: Counter,
    /// Number of normal-priority requests delayed by broker throttling.
    pub throttle_delays: Counter,
    /// Total broker-throttle delay applied to normal-priority requests, in milliseconds.
    pub throttle_delay_ms: Counter,
    /// Current active connections.
    pub active_connections: Gauge,
    /// Connection establishment latency.
    pub connect_latency: LatencyTracker,
    /// TLS handshake latency (only populated for TLS-secured connections).
    pub tls_handshake_latency: LatencyTracker,
}

impl ConnectionMetrics {
    /// Create new connection metrics.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a new connection.
    #[inline]
    pub fn record_connect(&self) {
        self.connections_created.inc();
        self.active_connections.inc();
    }

    /// Record a connection close.
    #[inline]
    pub fn record_close(&self) {
        self.connections_closed.inc();
        self.active_connections.dec();
    }

    /// Record a connection error.
    #[inline]
    pub fn record_error(&self) {
        self.connection_errors.inc();
    }

    /// Record a high-priority request.
    #[inline]
    pub fn record_high_priority_request(&self) {
        self.high_priority_requests.inc();
    }

    /// Record a normal-priority request.
    #[inline]
    pub fn record_normal_priority_request(&self) {
        self.normal_priority_requests.inc();
    }

    /// Record a high-priority request processed ahead of normal-priority work.
    #[inline]
    pub fn record_high_priority_bypass(&self) {
        self.high_priority_bypasses.inc();
    }

    /// Record a fairness yield after exhausting the high-priority bypass budget.
    #[inline]
    pub fn record_high_priority_bypass_yield(&self) {
        self.high_priority_bypass_yields.inc();
    }

    /// Record a normal-priority delay caused by broker throttling.
    #[inline]
    pub fn record_throttle_delay(&self, delay: Duration) {
        self.throttle_delays.inc();
        let millis = delay.as_millis().min(u64::MAX as u128) as u64;
        self.throttle_delay_ms.add(millis);
    }

    /// Record a completed TLS handshake duration.
    #[inline]
    pub fn record_tls_handshake(&self, duration: Duration) {
        self.tls_handshake_latency.record(duration);
    }

    /// Get a snapshot of all metrics.
    pub fn snapshot(&self) -> ConnectionMetricsSnapshot {
        ConnectionMetricsSnapshot {
            connections_created: self.connections_created.get(),
            connections_closed: self.connections_closed.get(),
            connection_errors: self.connection_errors.get(),
            high_priority_requests: self.high_priority_requests.get(),
            normal_priority_requests: self.normal_priority_requests.get(),
            high_priority_bypasses: self.high_priority_bypasses.get(),
            high_priority_bypass_yields: self.high_priority_bypass_yields.get(),
            throttle_delays: self.throttle_delays.get(),
            throttle_delay_ms: self.throttle_delay_ms.get(),
            active_connections: self.active_connections.get(),
            connect_latency: self.connect_latency.snapshot(),
            tls_handshake_latency: self.tls_handshake_latency.snapshot(),
        }
    }
}

/// Snapshot of connection metrics.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ConnectionMetricsSnapshot {
    /// Total connections created.
    pub connections_created: u64,
    /// Total connections closed.
    pub connections_closed: u64,
    /// Connection errors.
    pub connection_errors: u64,
    /// Total high-priority requests sent.
    pub high_priority_requests: u64,
    /// Total normal-priority requests sent.
    pub normal_priority_requests: u64,
    /// High-priority requests processed ahead of normal-priority work.
    pub high_priority_bypasses: u64,
    /// Forced normal-priority drain steps after the high-priority bypass budget was exhausted.
    pub high_priority_bypass_yields: u64,
    /// Normal-priority requests delayed by broker throttling.
    pub throttle_delays: u64,
    /// Total broker-throttle delay applied to normal-priority requests, in milliseconds.
    pub throttle_delay_ms: u64,
    /// Current active connections.
    pub active_connections: u64,
    /// Connection latency statistics.
    pub connect_latency: LatencySnapshot,
    /// TLS handshake latency statistics (only populated for TLS connections).
    pub tls_handshake_latency: LatencySnapshot,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let counter = Counter::new();
        assert_eq!(counter.get(), 0);

        counter.inc();
        assert_eq!(counter.get(), 1);

        counter.add(5);
        assert_eq!(counter.get(), 6);

        counter.reset();
        assert_eq!(counter.get(), 0);
    }

    #[test]
    fn test_gauge() {
        let gauge = Gauge::new();
        assert_eq!(gauge.get(), 0);

        gauge.set(10);
        assert_eq!(gauge.get(), 10);

        gauge.inc();
        assert_eq!(gauge.get(), 11);

        gauge.dec();
        assert_eq!(gauge.get(), 10);
    }

    #[test]
    fn test_gauge_dec_saturates_at_zero() {
        let gauge = Gauge::new();
        assert_eq!(gauge.get(), 0);
        assert_eq!(gauge.underflow_count(), 0);

        // Decrementing from 0 should not underflow
        gauge.dec();
        assert_eq!(
            gauge.get(),
            0,
            "Gauge::dec() should saturate at 0, not underflow"
        );
        assert_eq!(
            gauge.underflow_count(),
            1,
            "first underflow must be counted"
        );

        // Each subsequent underflow increments the count
        gauge.dec();
        gauge.dec();
        assert_eq!(gauge.get(), 0);
        assert_eq!(
            gauge.underflow_count(),
            3,
            "all underflows must be counted, not silenced after the first"
        );

        // Set to 1, dec to 0, then dec again
        gauge.set(1);
        gauge.dec();
        assert_eq!(gauge.get(), 0);
        gauge.dec();
        assert_eq!(gauge.get(), 0, "Gauge should not wrap around u64::MAX");
        assert_eq!(gauge.underflow_count(), 4);
    }

    #[test]
    fn test_gauge_dec_concurrent_no_underflow() {
        use std::sync::Arc;
        // Two threads both dec a gauge starting at 1. Exactly one should
        // succeed and bring the value to 0; the other should warn and leave
        // it at 0. The invariant is that the gauge never wraps to u64::MAX.
        let gauge = Arc::new(Gauge::new());
        gauge.set(1);

        let g1 = Arc::clone(&gauge);
        let g2 = Arc::clone(&gauge);

        let t1 = std::thread::spawn(move || g1.dec());
        let t2 = std::thread::spawn(move || g2.dec());

        t1.join().unwrap();
        t2.join().unwrap();

        assert_eq!(
            gauge.get(),
            0,
            "concurrent dec() must not underflow to u64::MAX"
        );
    }

    #[test]
    fn test_latency_tracker() {
        let tracker = LatencyTracker::new();
        assert_eq!(tracker.count(), 0);
        assert!(tracker.min().is_none());
        assert!(tracker.max().is_none());
        assert!(tracker.avg().is_none());

        tracker.record(Duration::from_millis(100));
        tracker.record(Duration::from_millis(200));
        tracker.record(Duration::from_millis(300));

        assert_eq!(tracker.count(), 3);
        assert_eq!(tracker.min(), Some(Duration::from_millis(100)));
        assert_eq!(tracker.max(), Some(Duration::from_millis(300)));
        assert_eq!(tracker.avg(), Some(Duration::from_millis(200)));
    }

    #[test]
    fn test_latency_tracker_zero_duration_sample() {
        // Recording a zero-duration sample must produce Some(ZERO) from max(),
        // not None.  The previous implementation returned None when max == 0,
        // conflating "no samples" with "all zero-duration samples".
        let tracker = LatencyTracker::new();
        tracker.record(Duration::ZERO);
        assert_eq!(tracker.count(), 1);
        assert_eq!(
            tracker.max(),
            Some(Duration::ZERO),
            "max() must return Some(ZERO) for a zero-duration sample, not None"
        );
        assert_eq!(
            tracker.min(),
            Some(Duration::ZERO),
            "min() must return Some(ZERO) for a zero-duration sample, not None"
        );
    }

    #[test]
    fn test_latency_guard() {
        let tracker = LatencyTracker::new();

        {
            let _guard = tracker.start();
            std::thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(tracker.count(), 1);
        assert!(tracker.min().unwrap() >= Duration::from_millis(10));
    }

    #[test]
    fn test_producer_metrics() {
        let metrics = ProducerMetrics::new();

        metrics.record_send(100);
        metrics.record_send(200);
        metrics.record_batch(5);
        metrics.record_error();
        metrics.record_retry();
        metrics.connections.set(3);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.records_sent, 7); // 2 sends + 5 batch
        assert_eq!(snapshot.bytes_sent, 300);
        assert_eq!(snapshot.batches_sent, 1);
        assert_eq!(snapshot.errors, 1);
        assert_eq!(snapshot.retries, 1);
        assert_eq!(snapshot.connections, 3);
    }

    #[test]
    fn test_consumer_metrics() {
        let metrics = ConsumerMetrics::new();

        metrics.record_receive(10, 1000);
        metrics.record_poll(10);
        metrics.record_poll(0); // empty
        metrics.record_fetch();
        metrics.record_commit();
        metrics.record_rebalance();
        metrics.assigned_partitions.set(4);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.records_received, 10);
        assert_eq!(snapshot.bytes_received, 1000);
        assert_eq!(snapshot.polls, 2);
        assert_eq!(snapshot.empty_polls, 1);
        assert_eq!(snapshot.fetches, 1);
        assert_eq!(snapshot.commits, 1);
        assert_eq!(snapshot.rebalances, 1);
        assert_eq!(snapshot.assigned_partitions, 4);
    }

    #[test]
    fn test_connection_metrics() {
        let metrics = ConnectionMetrics::new();

        metrics.record_connect();
        metrics.record_connect();
        metrics.record_close();
        metrics.record_error();
        metrics.record_high_priority_request();
        metrics.record_normal_priority_request();
        metrics.record_high_priority_bypass();
        metrics.record_high_priority_bypass_yield();
        metrics.record_throttle_delay(Duration::from_millis(25));

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.connections_created, 2);
        assert_eq!(snapshot.connections_closed, 1);
        assert_eq!(snapshot.active_connections, 1);
        assert_eq!(snapshot.connection_errors, 1);
        assert_eq!(snapshot.high_priority_requests, 1);
        assert_eq!(snapshot.normal_priority_requests, 1);
        assert_eq!(snapshot.high_priority_bypasses, 1);
        assert_eq!(snapshot.high_priority_bypass_yields, 1);
        assert_eq!(snapshot.throttle_delays, 1);
        assert_eq!(snapshot.throttle_delay_ms, 25);
    }

    #[test]
    fn test_latency_reset() {
        let tracker = LatencyTracker::new();
        tracker.record(Duration::from_millis(100));
        assert_eq!(tracker.count(), 1);

        tracker.reset();
        assert_eq!(tracker.count(), 0);
        assert!(tracker.min().is_none());
    }

    #[test]
    fn test_producer_prometheus_export() {
        let metrics = ProducerMetrics::new();
        metrics.record_send(100);
        metrics.record_batch(5);
        metrics.record_error();

        let output = metrics.to_prometheus_text("krafka_producer");

        assert!(output.contains("# TYPE krafka_producer_records_sent_total counter"));
        assert!(output.contains("krafka_producer_records_sent_total 6"));
        assert!(output.contains("krafka_producer_bytes_sent_total 100"));
        assert!(output.contains("krafka_producer_errors_total 1"));
    }

    #[test]
    fn test_consumer_prometheus_export() {
        let metrics = ConsumerMetrics::new();
        metrics.record_receive(10, 500);
        metrics.record_poll(10);
        metrics.record_seek(3);
        metrics.assigned_partitions.set(3);

        let output = metrics.to_prometheus_text("krafka_consumer");

        assert!(output.contains("# TYPE krafka_consumer_records_received_total counter"));
        assert!(output.contains("krafka_consumer_records_received_total 10"));
        assert!(output.contains("krafka_consumer_bytes_received_total 500"));
        assert!(output.contains("krafka_consumer_seeks_total 3"));
        assert!(output.contains("krafka_consumer_assigned_partitions 3"));
    }

    #[test]
    fn test_connection_prometheus_export() {
        let metrics = ConnectionMetrics::new();
        metrics.record_connect();
        metrics.record_connect();
        metrics.record_close();

        let output = metrics.to_prometheus_text("krafka_connection");

        assert!(output.contains("krafka_connection_connections_created_total 2"));
        assert!(output.contains("krafka_connection_connections_closed_total 1"));
        assert!(output.contains("krafka_connection_active_connections 1"));
    }

    #[test]
    fn test_connection_priority_prometheus_export() {
        let metrics = ConnectionMetrics::new();
        metrics.record_high_priority_request();
        metrics.record_normal_priority_request();
        metrics.record_high_priority_bypass();
        metrics.record_high_priority_bypass_yield();
        metrics.record_throttle_delay(Duration::from_millis(75));

        let output = metrics.to_prometheus_text("krafka_connection");

        assert!(output.contains("krafka_connection_high_priority_requests_total 1"));
        assert!(output.contains("krafka_connection_normal_priority_requests_total 1"));
        assert!(output.contains("krafka_connection_high_priority_bypasses_total 1"));
        assert!(output.contains("krafka_connection_high_priority_bypass_yields_total 1"));
        assert!(output.contains("krafka_connection_throttle_delays_total 1"));
        assert!(output.contains("krafka_connection_throttle_delay_ms_total 75"));
    }

    #[test]
    fn test_krafka_metrics_registry() {
        let registry = KrafkaMetrics::new();

        // Get handles and record metrics
        let producer = registry.producer_metrics();
        let consumer = registry.consumer_metrics();

        producer.record_send(100);
        consumer.record_poll(5);

        // Export all metrics
        let output = registry.to_prometheus_text();

        assert!(output.contains("krafka_producer_records_sent_total 1"));
        assert!(output.contains("krafka_consumer_polls_total 1"));
    }

    #[test]
    fn test_krafka_metrics_reset() {
        let registry = KrafkaMetrics::new();
        let producer = registry.producer_metrics();
        let consumer = registry.consumer_metrics();
        let connection = registry.connection_metrics();

        producer.record_send(100);
        consumer.record_seek(2);
        connection.record_high_priority_request();
        connection.record_high_priority_bypass_yield();
        connection.record_throttle_delay(Duration::from_millis(10));
        assert_eq!(producer.records_sent.get(), 1);
        assert_eq!(consumer.seeks.get(), 2);
        assert_eq!(connection.high_priority_requests.get(), 1);
        assert_eq!(connection.high_priority_bypass_yields.get(), 1);
        assert_eq!(connection.throttle_delay_ms.get(), 10);

        registry.reset();
        assert_eq!(producer.records_sent.get(), 0);
        assert_eq!(consumer.seeks.get(), 0);
        assert_eq!(connection.high_priority_requests.get(), 0);
        assert_eq!(connection.high_priority_bypass_yields.get(), 0);
        assert_eq!(connection.throttle_delay_ms.get(), 0);
    }

    #[test]
    fn test_latency_prometheus_format() {
        let metrics = ProducerMetrics::new();
        metrics.send_latency.record(Duration::from_millis(50));
        metrics.send_latency.record(Duration::from_millis(100));

        let output = metrics.to_prometheus_text("test");

        assert!(output.contains("# TYPE test_send_latency_seconds summary"));
        assert!(output.contains("test_send_latency_seconds_count 2"));
        assert!(output.contains("test_send_latency_seconds_sum"));
        // min/max are their own gauge families — they are not legal members of
        // a summary family. See `export_latency` on `PrometheusExporter`.
        assert!(output.contains("# TYPE test_send_latency_min_seconds gauge"));
        assert!(output.contains("# TYPE test_send_latency_max_seconds gauge"));
        assert!(!output.contains("test_send_latency_seconds_min"));
        assert!(!output.contains("test_send_latency_seconds_max"));
    }

    #[test]
    fn test_consumer_lag_metrics() {
        let metrics = ConsumerMetrics::new();

        // Initially zero
        assert_eq!(metrics.lag.get(), 0);
        assert_eq!(metrics.lag_max.get(), 0);

        // Set lag values
        metrics.lag.set(42);
        metrics.lag_max.set(15);

        assert_eq!(metrics.lag.get(), 42);
        assert_eq!(metrics.lag_max.get(), 15);

        // Snapshot captures lag values
        let snap = metrics.snapshot();
        assert_eq!(snap.lag, 42);
        assert_eq!(snap.lag_max, 15);
    }

    #[test]
    fn test_consumer_lag_prometheus_export() {
        let metrics = ConsumerMetrics::new();
        metrics.lag.set(100);
        metrics.lag_max.set(30);

        let output = metrics.to_prometheus_text("c");

        assert!(output.contains("# HELP c_lag Total consumer lag across all assigned partitions"));
        assert!(output.contains("# TYPE c_lag gauge"));
        assert!(output.contains("c_lag 100"));

        assert!(output.contains("# HELP c_lag_max Maximum per-partition consumer lag"));
        assert!(output.contains("# TYPE c_lag_max gauge"));
        assert!(output.contains("c_lag_max 30"));
    }

    #[test]
    fn test_json_exporter_counter() {
        let metrics = ProducerMetrics::new();
        metrics.record_send(100);
        metrics.record_batch(5);

        let json = metrics.to_json("p");
        assert!(json.starts_with('['));
        assert!(json.ends_with(']'));
        assert!(json.contains("\"name\":\"p_records_sent\""));
        assert!(json.contains("\"type\":\"counter\""));
        assert!(json.contains("\"value\":6"));
    }

    #[test]
    fn test_json_exporter_gauge() {
        let metrics = ConsumerMetrics::new();
        metrics.assigned_partitions.set(4);

        let json = metrics.to_json("c");
        assert!(json.contains("\"name\":\"c_assigned_partitions\""));
        assert!(json.contains("\"type\":\"gauge\""));
        assert!(json.contains("\"value\":4"));
    }

    #[test]
    fn test_json_exporter_latency() {
        let metrics = ProducerMetrics::new();
        metrics.send_latency.record(Duration::from_millis(50));
        metrics.send_latency.record(Duration::from_millis(100));

        let json = metrics.to_json("p");
        assert!(json.contains("\"name\":\"p_send_latency\""));
        assert!(json.contains("\"type\":\"latency\""));
        assert!(json.contains("\"count\":2"));
        assert!(json.contains("\"sum_seconds\":"));
    }

    #[test]
    fn test_json_exporter_empty() {
        let exporter = JsonExporter::new();
        assert_eq!(exporter.finish(), "[]");
    }

    #[test]
    fn test_krafka_metrics_json() {
        let registry = KrafkaMetrics::new();
        let producer = registry.producer_metrics();
        producer.record_send(42);

        let json = registry.to_json();
        assert!(json.contains("\"name\":\"krafka_producer_records_sent\""));
        assert!(json.contains("\"value\":1"));
    }

    #[test]
    fn test_krafka_metrics_export_all() {
        let registry = KrafkaMetrics::new();
        let producer = registry.producer_metrics();
        producer.record_send(100);

        let mut exporter = PrometheusExporter::new();
        registry.export_all(&mut exporter);
        let output = exporter.finish();

        assert!(output.contains("krafka_producer_records_sent_total 1"));
        assert!(output.contains("krafka_consumer_polls_total 0"));
        assert!(output.contains("krafka_connection_connections_created_total 0"));
    }

    #[test]
    fn test_custom_exporter() {
        struct CountingExporter {
            counters: usize,
            gauges: usize,
            latencies: usize,
        }

        impl MetricsExporter for CountingExporter {
            fn export_counter(&mut self, _name: &str, _help: &str, _value: u64) {
                self.counters += 1;
            }
            fn export_gauge(&mut self, _name: &str, _help: &str, _value: u64) {
                self.gauges += 1;
            }
            fn export_latency(&mut self, _name: &str, _help: &str, _snapshot: &LatencySnapshot) {
                self.latencies += 1;
            }
        }

        let metrics = ProducerMetrics::new();
        let mut exporter = CountingExporter {
            counters: 0,
            gauges: 0,
            latencies: 0,
        };
        metrics.export_metrics("test", &mut exporter);

        // ProducerMetrics has 7 counters, 2 gauges, 1 latency
        assert_eq!(exporter.counters, 7);
        assert_eq!(exporter.gauges, 2);
        assert_eq!(exporter.latencies, 1);
    }

    #[test]
    fn test_json_escape() {
        assert_eq!(json_escape("hello"), "hello");
        assert_eq!(json_escape("he\"llo"), "he\\\"llo");
        assert_eq!(json_escape("he\\llo"), "he\\\\llo");
        assert_eq!(json_escape("he\nllo"), "he\\nllo");
    }

    #[test]
    fn test_sanitize_prometheus_name_valid() {
        assert_eq!(
            sanitize_prometheus_name("krafka_requests_total"),
            "krafka_requests_total"
        );
    }

    #[test]
    fn test_sanitize_prometheus_name_dots_hyphens() {
        assert_eq!(
            sanitize_prometheus_name("kafka.producer-send.rate"),
            "kafka_producer_send_rate"
        );
    }

    #[test]
    fn test_sanitize_prometheus_name_leading_digit() {
        assert_eq!(sanitize_prometheus_name("9lives"), "_9lives");
    }

    #[test]
    fn test_sanitize_prometheus_name_colons_preserved() {
        assert_eq!(
            sanitize_prometheus_name("namespace:metric"),
            "namespace:metric"
        );
    }

    #[test]
    fn test_latency_fetch_min_max() {
        let tracker = LatencyTracker::new();
        tracker.record(Duration::from_millis(100));
        tracker.record(Duration::from_millis(50));
        tracker.record(Duration::from_millis(200));
        let snapshot = tracker.snapshot();
        assert_eq!(snapshot.count, 3);
        assert_eq!(snapshot.min, Some(Duration::from_millis(50)));
        assert_eq!(snapshot.max, Some(Duration::from_millis(200)));
    }

    // -----------------------------------------------------------------------
    // Sub-bucket histogram accuracy tests (F-26)
    // -----------------------------------------------------------------------

    #[test]
    fn test_bucket_for_zero_and_small() {
        assert_eq!(LatencyTracker::bucket_for(0), 0);
        assert_eq!(LatencyTracker::bucket_for(1), 1);
        // i=1: nanos ∈ {2, 3} → bucket 2
        assert_eq!(LatencyTracker::bucket_for(2), 2);
        assert_eq!(LatencyTracker::bucket_for(3), 2);
        // i=2: nanos ∈ [4, 8) → bucket 3
        assert_eq!(LatencyTracker::bucket_for(4), 3);
        assert_eq!(LatencyTracker::bucket_for(5), 3);
        assert_eq!(LatencyTracker::bucket_for(6), 3);
        assert_eq!(LatencyTracker::bucket_for(7), 3);
        // i=3: [8, 16) → 8 sub-buckets, sub = (nanos >> 0) & 7 = nanos-8
        // bucket = (3-3)*8+4+sub = 4+sub
        assert_eq!(LatencyTracker::bucket_for(8), 4); // sub=0
        assert_eq!(LatencyTracker::bucket_for(9), 5); // sub=1
        assert_eq!(LatencyTracker::bucket_for(10), 6); // sub=2
        assert_eq!(LatencyTracker::bucket_for(14), 10); // sub=6
        assert_eq!(LatencyTracker::bucket_for(15), 11); // sub=7
        // i=4: [16, 32) → sub = (nanos >> 1) & 7 → bucket = 1*8+4+sub = 12+sub
        assert_eq!(LatencyTracker::bucket_for(16), 12); // sub=0
        assert_eq!(LatencyTracker::bucket_for(18), 13); // sub=1
        assert_eq!(LatencyTracker::bucket_for(24), 16); // sub=4
        assert_eq!(LatencyTracker::bucket_for(28), 18); // sub=6
        assert_eq!(LatencyTracker::bucket_for(31), 19); // sub=7
    }

    #[test]
    fn test_estimate_nanos_for_bucket_correctness() {
        assert_eq!(LatencyTracker::estimate_nanos_for_bucket(0), 0);
        assert_eq!(LatencyTracker::estimate_nanos_for_bucket(1), 1);
        assert_eq!(LatencyTracker::estimate_nanos_for_bucket(2), 3); // midpoint of [2,4)
        assert_eq!(LatencyTracker::estimate_nanos_for_bucket(3), 6); // midpoint of [4,8)
        // Band i=3 (width=1 ns, half_sub=0): lower=8 + sub*1
        assert_eq!(LatencyTracker::estimate_nanos_for_bucket(4), 8); // [8,9)
        assert_eq!(LatencyTracker::estimate_nanos_for_bucket(5), 9); // [9,10)
        assert_eq!(LatencyTracker::estimate_nanos_for_bucket(6), 10); // [10,11)
        assert_eq!(LatencyTracker::estimate_nanos_for_bucket(11), 15); // [15,16)
        // Band i=4 (width=2 ns, half_sub=1): lower=16 + sub*2 + 1
        assert_eq!(LatencyTracker::estimate_nanos_for_bucket(12), 17); // [16,18) midpoint=17
        assert_eq!(LatencyTracker::estimate_nanos_for_bucket(13), 19); // [18,20) midpoint=19
        assert_eq!(LatencyTracker::estimate_nanos_for_bucket(14), 21); // [20,22) midpoint=21
        assert_eq!(LatencyTracker::estimate_nanos_for_bucket(19), 31); // [30,32) midpoint=31
    }

    #[test]
    fn test_sub_bucket_relative_error_within_12_5_percent() {
        // For values in the 8-sub-bucket range (nanos >= 8), verify the estimate
        // is within 6.25% of the true value. Values < 8 use coarser single-bucket
        // coverage (exact for 0, 1; [2,4) and [4,8) as single buckets) and are
        // tested separately via test_bucket_for_zero_and_small.
        let test_nanos: &[u64] = &[
            8,
            16,
            32,
            64,
            128,
            256,
            512,
            1_000,
            2_000,
            4_000,
            8_000,
            16_000,
            32_000,
            64_000,
            100_000,
            1_000_000,
            10_000_000,
            50_000_000,
            100_000_000,
        ];
        for &nanos in test_nanos {
            let bucket = LatencyTracker::bucket_for(nanos);
            let estimate = LatencyTracker::estimate_nanos_for_bucket(bucket) as f64;
            let relative_error = (estimate - nanos as f64) / nanos as f64;
            assert!(
                (-0.0625 - 1e-9..=0.0625 + 1e-9).contains(&relative_error),
                "nanos={nanos}: estimate={estimate}, relative_error={relative_error:.4} > 6.25%"
            );
        }
    }

    #[test]
    fn test_percentile_accuracy_for_known_values() {
        // Record exactly 100 samples at 10 ms (= 10_000_000 ns).
        // p50, p95, p99 should all land in the same sub-bucket.
        // Verify the estimate is within 6.25% of 10 ms.
        let tracker = LatencyTracker::new();
        for _ in 0..100 {
            tracker.record(Duration::from_millis(10));
        }
        let exact_nanos = 10_000_000u64;
        for pct in [50.0_f64, 95.0, 99.0] {
            let est = tracker.percentile(pct).unwrap().as_nanos() as u64;
            let err = (est as f64 - exact_nanos as f64) / exact_nanos as f64;
            assert!(
                err.abs() <= 0.0625 + 1e-9,
                "p{pct}: estimate {est} ns, true {exact_nanos} ns, err={err:.4}"
            );
        }
    }

    // -----------------------------------------------------------------------
    // Labeled metrics carry labels, not mangled names
    // -----------------------------------------------------------------------

    /// Minimal exporter implementing only the three required methods, so the
    /// trait's *default* labeled implementations are exercised.
    #[derive(Default)]
    struct MinimalExporter {
        lines: Vec<String>,
    }

    impl MetricsExporter for MinimalExporter {
        fn export_counter(&mut self, name: &str, _help: &str, value: u64) {
            self.lines.push(format!("counter {name} {value}"));
        }
        fn export_gauge(&mut self, name: &str, _help: &str, value: u64) {
            self.lines.push(format!("gauge {name} {value}"));
        }
        fn export_latency(&mut self, name: &str, _help: &str, snapshot: &LatencySnapshot) {
            self.lines
                .push(format!("latency {name} {}", snapshot.count));
        }
    }

    #[test]
    fn test_default_labeled_fallbacks_mangle_names() {
        let mut exporter = MinimalExporter::default();
        exporter.export_labeled_counter("m", "h", &[("topic", "orders")], 1);
        exporter.export_labeled_gauge("g", "h", &[("topic", "orders")], 2);
        // No labels: the name must be untouched.
        exporter.export_labeled_counter("m", "h", &[], 3);
        exporter.export_labeled_gauge("g", "h", &[], 4);

        assert_eq!(
            exporter.lines,
            vec![
                "counter m_orders 1".to_string(),
                "gauge g_orders 2".to_string(),
                "counter m 3".to_string(),
                "gauge g 4".to_string(),
            ]
        );
    }

    #[test]
    fn test_mangle_labels_into_name() {
        assert_eq!(mangle_labels_into_name("m", &[]), "m");
        assert_eq!(
            mangle_labels_into_name("m", &[("topic", "orders")]),
            "m_orders"
        );
        assert_eq!(
            mangle_labels_into_name("m", &[("topic", "orders"), ("broker", "1")]),
            "m_orders_1"
        );
    }

    #[test]
    fn test_prometheus_labeled_gauge_emits_labels() {
        let mut exporter = PrometheusExporter::new();
        exporter.export_labeled_gauge("krafka_topic_lag", "Lag", &[("topic", "orders")], 5);
        exporter.export_labeled_gauge("krafka_topic_lag", "Lag", &[("topic", "events")], 9);
        let out = exporter.finish();

        assert!(out.contains("# TYPE krafka_topic_lag gauge"));
        assert!(out.contains("krafka_topic_lag{topic=\"orders\"} 5"));
        assert!(out.contains("krafka_topic_lag{topic=\"events\"} 9"));
        // The name must not be mangled with the label value.
        assert!(!out.contains("krafka_topic_lag_orders"));
        // The family header appears exactly once.
        assert_eq!(out.matches("# TYPE krafka_topic_lag gauge").count(), 1);
    }

    #[test]
    fn test_prometheus_labeled_gauge_without_labels() {
        let mut exporter = PrometheusExporter::new();
        exporter.export_labeled_gauge("krafka_thing", "Thing", &[], 3);
        let out = exporter.finish();
        assert!(out.contains("krafka_thing 3"));
    }

    #[test]
    fn test_json_labeled_gauge_emits_labels() {
        let mut exporter = JsonExporter::new();
        exporter.export_labeled_gauge("krafka_topic_lag", "Lag", &[("topic", "orders")], 5);
        let out = exporter.finish();

        assert!(out.contains("\"name\":\"krafka_topic_lag\""));
        assert!(out.contains("\"type\":\"gauge\""));
        assert!(out.contains("\"labels\":{\"topic\":\"orders\"}"));
        assert!(!out.contains("krafka_topic_lag_orders"));
    }

    #[test]
    fn test_json_labeled_counter_keeps_name_unmangled() {
        let mut exporter = JsonExporter::new();
        exporter.export_labeled_counter("krafka_topic_sent", "Sent", &[("topic", "orders")], 5);
        let out = exporter.finish();
        assert!(out.contains("\"name\":\"krafka_topic_sent\""));
        assert!(out.contains("\"type\":\"counter\""));
        assert!(out.contains("\"labels\":{\"topic\":\"orders\"}"));
    }

    #[test]
    fn test_producer_export_uses_topic_labels_not_mangled_names() {
        let metrics = ProducerMetrics::new();
        metrics.record_send_for_topic("orders", 100);
        metrics.record_error_for_topic("orders");

        let out = metrics.to_prometheus_text("krafka_producer");
        assert!(out.contains("krafka_producer_topic_records_sent_total{topic=\"orders\"} 1"));
        assert!(out.contains("krafka_producer_topic_errors_total{topic=\"orders\"} 1"));
        assert!(!out.contains("krafka_producer_topic_records_sent_orders"));
    }

    // -----------------------------------------------------------------------
    // OpenMetrics: min/max must not live inside the summary family
    // -----------------------------------------------------------------------

    #[test]
    fn test_prometheus_latency_min_max_are_separate_gauge_families() {
        let tracker = LatencyTracker::new();
        tracker.record(Duration::from_millis(10));
        tracker.record(Duration::from_millis(100));

        let mut exporter = PrometheusExporter::new();
        exporter.export_latency("krafka_send", "Send latency", &tracker.snapshot());
        let out = exporter.finish();

        // Summary family keeps only count/sum/quantiles.
        assert!(out.contains("# TYPE krafka_send_seconds summary"));
        assert!(out.contains("krafka_send_seconds_count 2"));
        assert!(out.contains("krafka_send_seconds_sum "));
        assert!(out.contains("krafka_send_seconds{quantile=\"0.5\"}"));

        // The illegal summary members are gone.
        assert!(
            !out.contains("krafka_send_seconds_min"),
            "_seconds_min is not a legal summary member:\n{out}"
        );
        assert!(
            !out.contains("krafka_send_seconds_max"),
            "_seconds_max is not a legal summary member:\n{out}"
        );

        // ...and reappear as their own gauge families.
        assert!(out.contains("# TYPE krafka_send_min_seconds gauge"));
        assert!(out.contains("# TYPE krafka_send_max_seconds gauge"));
        assert!(out.contains("krafka_send_min_seconds "));
        assert!(out.contains("krafka_send_max_seconds "));

        // Every emitted family declares a TYPE exactly once.
        assert_eq!(out.matches("# TYPE krafka_send_seconds summary").count(), 1);
        assert_eq!(
            out.matches("# TYPE krafka_send_min_seconds gauge").count(),
            1
        );
        assert_eq!(
            out.matches("# TYPE krafka_send_max_seconds gauge").count(),
            1
        );
    }

    #[test]
    fn test_prometheus_latency_empty_snapshot_emits_no_min_max_families() {
        let tracker = LatencyTracker::new();
        let mut exporter = PrometheusExporter::new();
        exporter.export_latency("krafka_send", "Send latency", &tracker.snapshot());
        let out = exporter.finish();

        assert!(out.contains("# TYPE krafka_send_seconds summary"));
        assert!(!out.contains("min_seconds"));
        assert!(!out.contains("max_seconds"));
    }

    // -----------------------------------------------------------------------
    // Nanosecond saturation
    // -----------------------------------------------------------------------

    #[test]
    fn test_latency_record_saturates_instead_of_truncating() {
        let tracker = LatencyTracker::new();
        // Duration::MAX is ~584 billion years — far more than u64::MAX nanos.
        tracker.record(Duration::MAX);

        // A wrapping `as u64` cast would produce a small bogus value here
        // (`Duration::MAX.as_nanos() as u64` == u64::MAX only by luck of the
        // low bits, so assert against the saturated value explicitly).
        assert_eq!(tracker.count(), 1);
        assert_eq!(tracker.max(), Some(Duration::from_nanos(u64::MAX)));
        assert_eq!(tracker.sum(), Duration::from_nanos(u64::MAX));
        // `min()` uses u64::MAX as its "no samples yet" sentinel, so a single
        // saturated sample is indistinguishable from an empty tracker there.
        // That sentinel behaviour predates the saturation fix.
        assert_eq!(tracker.min(), None);
    }

    #[test]
    fn test_latency_record_saturates_just_above_u64_nanos() {
        let tracker = LatencyTracker::new();
        // 2^64 ns + 1 s: comfortably past u64::MAX nanos, but nowhere near
        // Duration::MAX, so a truncating cast would wrap to a tiny value.
        let over = Duration::from_nanos(u64::MAX) + Duration::from_secs(1);
        tracker.record(over);

        assert_eq!(tracker.count(), 1);
        assert_eq!(tracker.max(), Some(Duration::from_nanos(u64::MAX)));
        assert_eq!(tracker.sum(), Duration::from_nanos(u64::MAX));
    }

    #[test]
    fn test_latency_record_normal_values_unchanged() {
        let tracker = LatencyTracker::new();
        tracker.record(Duration::from_millis(5));
        assert_eq!(tracker.max(), Some(Duration::from_millis(5)));
    }

    // -----------------------------------------------------------------------
    // Per-topic cap and the `__other__` overflow bucket
    // -----------------------------------------------------------------------

    #[test]
    fn test_default_topic_cap() {
        assert_eq!(DEFAULT_MAX_TRACKED_TOPICS, 1000);
        assert_eq!(
            ProducerMetrics::new().max_tracked_topics(),
            DEFAULT_MAX_TRACKED_TOPICS
        );
        assert_eq!(
            ProducerMetrics::default().max_tracked_topics(),
            DEFAULT_MAX_TRACKED_TOPICS
        );
    }

    #[test]
    fn test_topic_cap_routes_overflow_to_other_bucket() {
        let metrics = ProducerMetrics::with_max_tracked_topics(3);
        for i in 0..3 {
            metrics.record_send_for_topic(&format!("topic-{i}"), 10);
        }
        assert_eq!(metrics.tracked_topic_count(), 3);

        // Three more distinct topics all fold into `__other__`.
        for i in 3..6 {
            metrics.record_send_for_topic(&format!("topic-{i}"), 10);
        }
        // 3 named + 1 overflow bucket.
        assert_eq!(metrics.tracked_topic_count(), 4);

        let snapshot = metrics.topic_snapshot();
        let topics: Vec<&str> = snapshot.iter().map(|s| s.topic.as_str()).collect();
        assert_eq!(
            topics,
            vec![OVERFLOW_TOPIC_KEY, "topic-0", "topic-1", "topic-2"]
        );

        let other = snapshot
            .iter()
            .find(|s| s.topic == OVERFLOW_TOPIC_KEY)
            .unwrap();
        assert_eq!(other.records_sent, 3);
        assert_eq!(other.bytes_sent, 30);

        // Global counters still see every record.
        assert_eq!(metrics.records_sent.get(), 6);
        assert_eq!(metrics.bytes_sent.get(), 60);
    }

    #[test]
    fn test_topic_cap_does_not_grow_with_unbounded_topic_domain() {
        let metrics = ProducerMetrics::with_max_tracked_topics(10);
        for i in 0..5_000 {
            metrics.record_send_for_topic(&format!("cdc.tenant_{i}.events"), 1);
        }
        // 10 named topics + `__other__`, regardless of how many were seen.
        assert_eq!(metrics.tracked_topic_count(), 11);

        let snapshot = metrics.topic_snapshot();
        assert_eq!(snapshot.len(), 11);
        let other = snapshot
            .iter()
            .find(|s| s.topic == OVERFLOW_TOPIC_KEY)
            .unwrap();
        assert_eq!(other.records_sent, 4_990);
    }

    #[test]
    fn test_topic_cap_zero_routes_everything_to_overflow() {
        let metrics = ProducerMetrics::with_max_tracked_topics(0);
        metrics.record_send_for_topic("orders", 1);
        metrics.record_error_for_topic("events");
        metrics.record_batch_for_topic("payments", 2, 20);

        let snapshot = metrics.topic_snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].topic, OVERFLOW_TOPIC_KEY);
        assert_eq!(snapshot[0].records_sent, 3);
        assert_eq!(snapshot[0].bytes_sent, 21);
        assert_eq!(snapshot[0].errors, 1);
    }

    #[test]
    fn test_overflow_bucket_is_exported_with_a_topic_label() {
        let metrics = ProducerMetrics::with_max_tracked_topics(1);
        metrics.record_send_for_topic("orders", 1);
        metrics.record_send_for_topic("events", 1);

        let out = metrics.to_prometheus_text("krafka_producer");
        assert!(out.contains("krafka_producer_topic_records_sent_total{topic=\"orders\"} 1"));
        assert!(out.contains("krafka_producer_topic_records_sent_total{topic=\"__other__\"} 1"));
    }

    #[test]
    fn test_known_topics_still_recorded_after_cap_reached() {
        let metrics = ProducerMetrics::with_max_tracked_topics(1);
        metrics.record_send_for_topic("orders", 1);
        metrics.record_send_for_topic("events", 1); // overflow
        // The already-tracked topic keeps its own bucket.
        metrics.record_send_for_topic("orders", 1);

        let snapshot = metrics.topic_snapshot();
        let orders = snapshot.iter().find(|s| s.topic == "orders").unwrap();
        assert_eq!(orders.records_sent, 2);
    }

    #[test]
    fn test_clear_topic_metrics() {
        let metrics = ProducerMetrics::new();
        metrics.record_send_for_topic("orders", 1);
        assert_eq!(metrics.tracked_topic_count(), 1);
        metrics.clear_topic_metrics();
        assert_eq!(metrics.tracked_topic_count(), 0);
        assert!(metrics.topic_snapshot().is_empty());
    }

    // -----------------------------------------------------------------------
    // Concurrent per-record recording without a global exclusive lock
    // -----------------------------------------------------------------------

    #[test]
    fn test_concurrent_recording_is_correct() {
        use std::thread;

        const THREADS: u64 = 8;
        const ITERS: u64 = 2_000;

        let metrics = Arc::new(ProducerMetrics::new());
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let metrics = Arc::clone(&metrics);
            handles.push(thread::spawn(move || {
                for _ in 0..ITERS {
                    // A topic every thread contends on...
                    metrics.record_send_for_topic("shared", 10);
                    // ...and one private to this thread.
                    metrics.record_send_for_topic(&format!("thread-{t}"), 1);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        let snapshot = metrics.topic_snapshot();
        let shared = snapshot.iter().find(|s| s.topic == "shared").unwrap();
        assert_eq!(shared.records_sent, THREADS * ITERS);
        assert_eq!(shared.bytes_sent, THREADS * ITERS * 10);

        // Every thread's private topic was registered exactly once.
        for t in 0..THREADS {
            let name = format!("thread-{t}");
            let per_thread = snapshot.iter().find(|s| s.topic == name).unwrap();
            assert_eq!(per_thread.records_sent, ITERS);
        }
        // 1 shared + THREADS private, no duplicates.
        assert_eq!(snapshot.len(), THREADS as usize + 1);
        assert_eq!(metrics.records_sent.get(), THREADS * ITERS * 2);
    }

    #[test]
    fn test_concurrent_registration_of_the_same_new_topic() {
        use std::thread;

        const THREADS: u64 = 16;
        let metrics = Arc::new(ProducerMetrics::new());
        let mut handles = Vec::new();
        for _ in 0..THREADS {
            let metrics = Arc::clone(&metrics);
            handles.push(thread::spawn(move || {
                metrics.record_send_for_topic("race", 1);
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // The copy-on-write publication must not lose any thread's increment.
        let snapshot = metrics.topic_snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].topic, "race");
        assert_eq!(snapshot[0].records_sent, THREADS);
    }

    #[test]
    fn test_concurrent_recording_respects_the_cap() {
        use std::thread;

        const THREADS: u64 = 8;
        let metrics = Arc::new(ProducerMetrics::with_max_tracked_topics(20));
        let mut handles = Vec::new();
        for t in 0..THREADS {
            let metrics = Arc::clone(&metrics);
            handles.push(thread::spawn(move || {
                for i in 0..200 {
                    metrics.record_send_for_topic(&format!("t-{t}-{i}"), 1);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }

        // At most 20 named topics plus the overflow bucket.
        assert!(
            metrics.tracked_topic_count() <= 21,
            "cap not enforced: {} buckets",
            metrics.tracked_topic_count()
        );
        // Nothing was dropped: every record landed somewhere.
        let total: u64 = metrics
            .topic_snapshot()
            .iter()
            .map(|s| s.records_sent)
            .sum();
        assert_eq!(total, THREADS * 200);
    }

    #[test]
    fn test_batch_and_error_paths_share_topic_buckets() {
        let metrics = ProducerMetrics::new();
        metrics.record_send_for_topic("orders", 10);
        metrics.record_batch_for_topic("orders", 5, 50);
        metrics.record_error_for_topic("orders");

        let snapshot = metrics.topic_snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].records_sent, 6);
        assert_eq!(snapshot[0].bytes_sent, 60);
        assert_eq!(snapshot[0].errors, 1);
    }

    // -----------------------------------------------------------------------
    // Reset generation
    // -----------------------------------------------------------------------

    #[test]
    fn test_reset_generation_starts_at_zero_and_increments() {
        let metrics = KrafkaMetrics::new();
        assert_eq!(metrics.reset_generation(), 0);

        metrics.reset();
        assert_eq!(metrics.reset_generation(), 1);

        metrics.reset();
        metrics.reset();
        assert_eq!(metrics.reset_generation(), 3);
    }

    #[test]
    fn test_reset_generation_is_shared_across_clones() {
        let metrics = KrafkaMetrics::new();
        let clone = metrics.clone();
        metrics.reset();
        assert_eq!(clone.reset_generation(), 1);
        assert_eq!(metrics.reset_generation(), 1);
    }

    #[test]
    fn test_reset_generation_signals_counter_rewind() {
        let metrics = KrafkaMetrics::new();
        let producer = metrics.producer_metrics();
        producer.record_send_for_topic("orders", 100);
        assert_eq!(producer.records_sent.get(), 1);

        let before = metrics.reset_generation();
        metrics.reset();

        // Counters rewound...
        assert_eq!(producer.records_sent.get(), 0);
        assert!(producer.topic_snapshot().is_empty());
        // ...and the generation changed so delta consumers can drop baselines.
        assert_ne!(metrics.reset_generation(), before);
    }

    #[test]
    fn test_krafka_metrics_with_max_tracked_topics() {
        let metrics = KrafkaMetrics::with_max_tracked_topics(2);
        let producer = metrics.producer_metrics();
        assert_eq!(producer.max_tracked_topics(), 2);
        producer.record_send_for_topic("a", 1);
        producer.record_send_for_topic("b", 1);
        producer.record_send_for_topic("c", 1);
        assert_eq!(producer.tracked_topic_count(), 3); // a, b, __other__
    }
}
