//! Metrics and observability for Krafka clients.
//!
//! This module provides metrics collection for producers and consumers,
//! including counters, gauges, and latency tracking.
//!
//! # Metrics Export
//!
//! Krafka provides built-in Prometheus text format export without external dependencies.
//! This allows easy integration with Prometheus, Grafana, and other monitoring tools.
//!
//! ```rust
//! use krafka::metrics::{ProducerMetrics, MetricsExport};
//!
//! let metrics = ProducerMetrics::new();
//! metrics.record_send(100);
//! metrics.record_batch(5);
//!
//! // Export in Prometheus text format
//! let prometheus_output = metrics.to_prometheus_text("krafka_producer");
//! println!("{}", prometheus_output);
//! ```
//!
//! Example output:
//! ```text
//! # HELP krafka_producer_records_sent_total Total number of records sent
//! # TYPE krafka_producer_records_sent_total counter
//! krafka_producer_records_sent_total 6
//! # HELP krafka_producer_bytes_sent_total Total bytes sent
//! # TYPE krafka_producer_bytes_sent_total counter
//! krafka_producer_bytes_sent_total 100
//! ...
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

    /// Increment the gauge by 1.
    #[inline]
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the gauge by 1 (saturates at 0 to prevent underflow).
    #[inline]
    pub fn dec(&self) {
        let _ = self
            .value
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
    }
}

/// Latency tracker using atomic min/max/sum/count.
#[derive(Debug, Default)]
pub struct LatencyTracker {
    count: AtomicU64,
    sum_nanos: AtomicU64,
    min_nanos: AtomicU64,
    max_nanos: AtomicU64,
}

impl LatencyTracker {
    /// Create a new latency tracker.
    pub fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            sum_nanos: AtomicU64::new(0),
            min_nanos: AtomicU64::new(u64::MAX),
            max_nanos: AtomicU64::new(0),
        }
    }

    /// Record a latency value.
    #[inline]
    pub fn record(&self, duration: Duration) {
        let nanos = duration.as_nanos() as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_nanos.fetch_add(nanos, Ordering::Relaxed);

        // Update min (compare-and-swap loop)
        let mut current_min = self.min_nanos.load(Ordering::Relaxed);
        while nanos < current_min {
            match self.min_nanos.compare_exchange_weak(
                current_min,
                nanos,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_min = x,
            }
        }

        // Update max (compare-and-swap loop)
        let mut current_max = self.max_nanos.load(Ordering::Relaxed);
        while nanos > current_max {
            match self.max_nanos.compare_exchange_weak(
                current_max,
                nanos,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(x) => current_max = x,
            }
        }
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
    pub fn max(&self) -> Option<Duration> {
        let max = self.max_nanos.load(Ordering::Relaxed);
        if max == 0 && self.count() == 0 {
            None
        } else {
            Some(Duration::from_nanos(max))
        }
    }

    /// Get the average latency.
    pub fn avg(&self) -> Option<Duration> {
        let count = self.count();
        if count == 0 {
            None
        } else {
            let sum = self.sum_nanos.load(Ordering::Relaxed);
            Some(Duration::from_nanos(sum / count))
        }
    }

    /// Get a snapshot of the latency statistics.
    pub fn snapshot(&self) -> LatencySnapshot {
        LatencySnapshot {
            count: self.count(),
            sum: self.sum(),
            min: self.min(),
            max: self.max(),
            avg: self.avg(),
        }
    }

    /// Reset all statistics.
    pub fn reset(&self) {
        self.count.store(0, Ordering::Relaxed);
        self.sum_nanos.store(0, Ordering::Relaxed);
        self.min_nanos.store(u64::MAX, Ordering::Relaxed);
        self.max_nanos.store(0, Ordering::Relaxed);
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
}

/// Trait for exporting metrics in various formats.
pub trait MetricsExport {
    /// Export metrics in Prometheus text format.
    ///
    /// # Arguments
    ///
    /// * `prefix` - Prefix for metric names (e.g., "krafka_producer")
    ///
    /// # Example
    ///
    /// ```rust
    /// use krafka::metrics::{ProducerMetrics, MetricsExport};
    ///
    /// let metrics = ProducerMetrics::new();
    /// let output = metrics.to_prometheus_text("krafka_producer");
    /// ```
    fn to_prometheus_text(&self, prefix: &str) -> String;
}

impl MetricsExport for ProducerMetrics {
    fn to_prometheus_text(&self, prefix: &str) -> String {
        let mut output = String::with_capacity(2048);

        write_prometheus_counter(
            &mut output,
            prefix,
            "records_sent",
            "Total number of records sent",
            self.records_sent.get(),
        );
        write_prometheus_counter(
            &mut output,
            prefix,
            "bytes_sent",
            "Total bytes sent",
            self.bytes_sent.get(),
        );
        write_prometheus_counter(
            &mut output,
            prefix,
            "batches_sent",
            "Total batches sent",
            self.batches_sent.get(),
        );
        write_prometheus_counter(
            &mut output,
            prefix,
            "errors",
            "Total send errors",
            self.errors.get(),
        );
        write_prometheus_counter(
            &mut output,
            prefix,
            "retries",
            "Total retries",
            self.retries.get(),
        );
        write_prometheus_gauge(
            &mut output,
            prefix,
            "connections",
            "Current active connections",
            self.connections.get(),
        );
        write_prometheus_gauge(
            &mut output,
            prefix,
            "buffered_records",
            "Currently buffered records",
            self.buffered_records.get(),
        );
        write_prometheus_latency(
            &mut output,
            prefix,
            "send_latency",
            "Send latency",
            &self.send_latency,
        );

        output
    }
}

impl MetricsExport for ConsumerMetrics {
    fn to_prometheus_text(&self, prefix: &str) -> String {
        let mut output = String::with_capacity(2048);

        write_prometheus_counter(
            &mut output,
            prefix,
            "records_received",
            "Total records received",
            self.records_received.get(),
        );
        write_prometheus_counter(
            &mut output,
            prefix,
            "bytes_received",
            "Total bytes received",
            self.bytes_received.get(),
        );
        write_prometheus_counter(
            &mut output,
            prefix,
            "fetches",
            "Total fetch requests",
            self.fetches.get(),
        );
        write_prometheus_counter(
            &mut output,
            prefix,
            "polls",
            "Total poll operations",
            self.polls.get(),
        );
        write_prometheus_counter(
            &mut output,
            prefix,
            "empty_polls",
            "Total empty polls",
            self.empty_polls.get(),
        );
        write_prometheus_counter(
            &mut output,
            prefix,
            "commits",
            "Total commit operations",
            self.commits.get(),
        );
        write_prometheus_counter(
            &mut output,
            prefix,
            "errors",
            "Total errors",
            self.errors.get(),
        );
        write_prometheus_counter(
            &mut output,
            prefix,
            "rebalances",
            "Total rebalances",
            self.rebalances.get(),
        );
        write_prometheus_gauge(
            &mut output,
            prefix,
            "lag",
            "Total consumer lag across all assigned partitions",
            self.lag.get(),
        );
        write_prometheus_gauge(
            &mut output,
            prefix,
            "lag_max",
            "Maximum per-partition consumer lag",
            self.lag_max.get(),
        );
        write_prometheus_gauge(
            &mut output,
            prefix,
            "assigned_partitions",
            "Currently assigned partitions",
            self.assigned_partitions.get(),
        );
        write_prometheus_gauge(
            &mut output,
            prefix,
            "paused_partitions",
            "Currently paused partitions",
            self.paused_partitions.get(),
        );
        write_prometheus_latency(
            &mut output,
            prefix,
            "poll_latency",
            "Poll latency",
            &self.poll_latency,
        );
        write_prometheus_latency(
            &mut output,
            prefix,
            "fetch_latency",
            "Fetch latency",
            &self.fetch_latency,
        );

        output
    }
}

impl MetricsExport for ConnectionMetrics {
    fn to_prometheus_text(&self, prefix: &str) -> String {
        let mut output = String::with_capacity(1024);

        write_prometheus_counter(
            &mut output,
            prefix,
            "connections_created",
            "Total connections created",
            self.connections_created.get(),
        );
        write_prometheus_counter(
            &mut output,
            prefix,
            "connections_closed",
            "Total connections closed",
            self.connections_closed.get(),
        );
        write_prometheus_counter(
            &mut output,
            prefix,
            "connection_errors",
            "Total connection errors",
            self.connection_errors.get(),
        );
        write_prometheus_gauge(
            &mut output,
            prefix,
            "active_connections",
            "Current active connections",
            self.active_connections.get(),
        );
        write_prometheus_latency(
            &mut output,
            prefix,
            "connect_latency",
            "Connection establishment latency",
            &self.connect_latency,
        );

        output
    }
}

/// Write a counter metric in Prometheus format.
fn write_prometheus_counter(output: &mut String, prefix: &str, name: &str, help: &str, value: u64) {
    let _ = writeln!(output, "# HELP {}_{}_total {}", prefix, name, help);
    let _ = writeln!(output, "# TYPE {}_{}_total counter", prefix, name);
    let _ = writeln!(output, "{}_{}_total {}", prefix, name, value);
}

/// Write a gauge metric in Prometheus format.
fn write_prometheus_gauge(output: &mut String, prefix: &str, name: &str, help: &str, value: u64) {
    let _ = writeln!(output, "# HELP {}_{} {}", prefix, name, help);
    let _ = writeln!(output, "# TYPE {}_{} gauge", prefix, name);
    let _ = writeln!(output, "{}_{} {}", prefix, name, value);
}

/// Write latency metrics in Prometheus format (as a summary).
fn write_prometheus_latency(
    output: &mut String,
    prefix: &str,
    name: &str,
    help: &str,
    tracker: &LatencyTracker,
) {
    let count = tracker.count();
    let sum_seconds = tracker.sum().as_secs_f64();

    let _ = writeln!(output, "# HELP {}_{}_seconds {}", prefix, name, help);
    let _ = writeln!(output, "# TYPE {}_{}_seconds summary", prefix, name);
    let _ = writeln!(output, "{}_{}_seconds_count {}", prefix, name, count);
    let _ = writeln!(output, "{}_{}_seconds_sum {:.9}", prefix, name, sum_seconds);

    if let Some(min) = tracker.min() {
        let _ = writeln!(
            output,
            "{}_{}_seconds_min {:.9}",
            prefix,
            name,
            min.as_secs_f64()
        );
    }
    if let Some(max) = tracker.max() {
        let _ = writeln!(
            output,
            "{}_{}_seconds_max {:.9}",
            prefix,
            name,
            max.as_secs_f64()
        );
    }
}

/// Aggregated metrics registry for all Krafka components.
///
/// This provides a convenient way to collect and export metrics from
/// multiple producers, consumers, and connections.
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
/// ```
#[derive(Debug, Clone)]
pub struct KrafkaMetrics {
    /// Producer metrics.
    producer: Arc<ProducerMetrics>,
    /// Consumer metrics.
    consumer: Arc<ConsumerMetrics>,
    /// Connection metrics.
    connection: Arc<ConnectionMetrics>,
}

impl Default for KrafkaMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl KrafkaMetrics {
    /// Create a new metrics registry.
    pub fn new() -> Self {
        Self {
            producer: Arc::new(ProducerMetrics::new()),
            consumer: Arc::new(ConsumerMetrics::new()),
            connection: Arc::new(ConnectionMetrics::new()),
        }
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

    /// Export all metrics in Prometheus text format.
    ///
    /// Uses the standard "krafka_" prefix for all metric names.
    pub fn to_prometheus_text(&self) -> String {
        self.to_prometheus_text_with_prefix("krafka")
    }

    /// Export all metrics in Prometheus text format with custom prefix.
    pub fn to_prometheus_text_with_prefix(&self, prefix: &str) -> String {
        let mut output = String::with_capacity(8192);

        output.push_str(
            &self
                .producer
                .to_prometheus_text(&format!("{}_producer", prefix)),
        );
        output.push_str(
            &self
                .consumer
                .to_prometheus_text(&format!("{}_consumer", prefix)),
        );
        output.push_str(
            &self
                .connection
                .to_prometheus_text(&format!("{}_connection", prefix)),
        );

        output
    }

    /// Reset all metrics.
    pub fn reset(&self) {
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
        self.consumer.poll_latency.reset();
        self.consumer.fetch_latency.reset();
        self.consumer.lag.set(0);
        self.consumer.lag_max.set(0);
        self.consumer.assigned_partitions.set(0);
        self.consumer.paused_partitions.set(0);

        self.producer.connections.set(0);
        self.producer.buffered_records.set(0);

        self.connection.connections_created.reset();
        self.connection.connections_closed.reset();
        self.connection.connection_errors.reset();
        self.connection.active_connections.set(0);
        self.connection.connect_latency.reset();
    }
}

/// Producer metrics.
#[derive(Debug, Default)]
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
    /// Number of records currently buffered.
    pub buffered_records: Gauge,
}

impl ProducerMetrics {
    /// Create new producer metrics.
    pub fn new() -> Self {
        Self::default()
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

    /// Record a retry.
    #[inline]
    pub fn record_retry(&self) {
        self.retries.inc();
    }

    /// Get a snapshot of all metrics.
    pub fn snapshot(&self) -> ProducerMetricsSnapshot {
        ProducerMetricsSnapshot {
            records_sent: self.records_sent.get(),
            bytes_sent: self.bytes_sent.get(),
            batches_sent: self.batches_sent.get(),
            errors: self.errors.get(),
            retries: self.retries.get(),
            send_latency: self.send_latency.snapshot(),
            connections: self.connections.get(),
            buffered_records: self.buffered_records.get(),
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
    /// Currently buffered records.
    pub buffered_records: u64,
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
}

impl ConsumerMetrics {
    /// Create new consumer metrics.
    pub fn new() -> Self {
        Self::default()
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
    /// Current active connections.
    pub active_connections: Gauge,
    /// Connection establishment latency.
    pub connect_latency: LatencyTracker,
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

    /// Get a snapshot of all metrics.
    pub fn snapshot(&self) -> ConnectionMetricsSnapshot {
        ConnectionMetricsSnapshot {
            connections_created: self.connections_created.get(),
            connections_closed: self.connections_closed.get(),
            connection_errors: self.connection_errors.get(),
            active_connections: self.active_connections.get(),
            connect_latency: self.connect_latency.snapshot(),
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
    /// Current active connections.
    pub active_connections: u64,
    /// Connection latency statistics.
    pub connect_latency: LatencySnapshot,
}

#[cfg(test)]
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

        // Decrementing from 0 should not underflow
        gauge.dec();
        assert_eq!(
            gauge.get(),
            0,
            "Gauge::dec() should saturate at 0, not underflow"
        );

        // Multiple decrements from 0 should all stay at 0
        gauge.dec();
        gauge.dec();
        assert_eq!(gauge.get(), 0);

        // Set to 1, dec to 0, then dec again
        gauge.set(1);
        gauge.dec();
        assert_eq!(gauge.get(), 0);
        gauge.dec();
        assert_eq!(gauge.get(), 0, "Gauge should not wrap around u64::MAX");
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

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.connections_created, 2);
        assert_eq!(snapshot.connections_closed, 1);
        assert_eq!(snapshot.active_connections, 1);
        assert_eq!(snapshot.connection_errors, 1);
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
        metrics.assigned_partitions.set(3);

        let output = metrics.to_prometheus_text("krafka_consumer");

        assert!(output.contains("# TYPE krafka_consumer_records_received_total counter"));
        assert!(output.contains("krafka_consumer_records_received_total 10"));
        assert!(output.contains("krafka_consumer_bytes_received_total 500"));
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

        producer.record_send(100);
        assert_eq!(producer.records_sent.get(), 1);

        registry.reset();
        assert_eq!(producer.records_sent.get(), 0);
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
        assert!(output.contains("test_send_latency_seconds_min"));
        assert!(output.contains("test_send_latency_seconds_max"));
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
}
