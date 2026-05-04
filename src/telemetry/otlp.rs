//! Minimal OTLP MetricsData v1 protobuf encoder.
//!
//! This module hand-encodes the subset of the OpenTelemetry protobuf schema
//! required by KIP-714 — no code-generated `.proto` dependency. Only the
//! `MetricsData → ResourceMetrics → ScopeMetrics → Metric` path is supported,
//! using `Sum` (for counters) and `Gauge` (for gauges / latency values) data
//! types with `NumberDataPoint` carrying `as_int` or `as_double` values.
//!
//! Wire-format reference: <https://protobuf.dev/programming-guides/encoding/>
//! Proto schema: opentelemetry-proto v0.19.0 `metrics/v1/metrics.proto`
//!
//! # Field-number reference (from the .proto files)
//!
//! | Message               | Field                    | Number | Wire |
//! |-----------------------|--------------------------|--------|------|
//! | MetricsData           | resource_metrics         | 1      | LEN  |
//! | ResourceMetrics       | resource                 | 1      | LEN  |
//! | ResourceMetrics       | scope_metrics            | 2      | LEN  |
//! | Resource              | attributes               | 1      | LEN  |
//! | ScopeMetrics          | scope                    | 1      | LEN  |
//! | ScopeMetrics          | metrics                  | 2      | LEN  |
//! | InstrumentationScope  | name                     | 1      | LEN  |
//! | InstrumentationScope  | version                  | 2      | LEN  |
//! | Metric                | name                     | 1      | LEN  |
//! | Metric                | description              | 2      | LEN  |
//! | Metric                | unit                     | 3      | LEN  |
//! | Metric                | gauge                    | 5      | LEN  |
//! | Metric                | sum                      | 7      | LEN  |
//! | Gauge                 | data_points              | 1      | LEN  |
//! | Sum                   | data_points              | 1      | LEN  |
//! | Sum                   | aggregation_temporality  | 2      | VINT |
//! | Sum                   | is_monotonic             | 3      | VINT |
//! | NumberDataPoint       | start_time_unix_nano     | 2      | I64  |
//! | NumberDataPoint       | time_unix_nano           | 3      | I64  |
//! | NumberDataPoint       | as_double                | 4      | I64  |
//! | NumberDataPoint       | as_int                   | 6      | I64  |
//! | NumberDataPoint       | attributes               | 7      | LEN  |
//! | KeyValue              | key                      | 1      | LEN  |
//! | KeyValue              | value                    | 2      | LEN  |
//! | AnyValue              | string_value             | 1      | LEN  |

use std::time::{SystemTime, UNIX_EPOCH};

use crate::metrics::{LatencySnapshot, MetricsExporter};

// ---------------------------------------------------------------------------
// Protobuf primitive helpers
// ---------------------------------------------------------------------------

/// Encode a protobuf varint (unsigned LEB128).
fn encode_varint(mut value: u64, buf: &mut Vec<u8>) {
    loop {
        let byte = (value & 0x7F) as u8;
        value >>= 7;
        if value == 0 {
            buf.push(byte);
            return;
        }
        buf.push(byte | 0x80);
    }
}

/// Encode a protobuf field tag: `(field_number << 3) | wire_type`.
fn encode_tag(field: u32, wire_type: u8, buf: &mut Vec<u8>) {
    encode_varint(((field as u64) << 3) | wire_type as u64, buf);
}

/// Encode a length-delimited field (wire type 2).
fn encode_len_field(field: u32, data: &[u8], buf: &mut Vec<u8>) {
    encode_tag(field, 2, buf);
    encode_varint(data.len() as u64, buf);
    buf.extend_from_slice(data);
}

/// Encode a string field (wire type 2).
fn encode_string_field(field: u32, s: &str, buf: &mut Vec<u8>) {
    if !s.is_empty() {
        encode_len_field(field, s.as_bytes(), buf);
    }
}

/// Encode a varint field (wire type 0).
fn encode_varint_field(field: u32, value: u64, buf: &mut Vec<u8>) {
    if value != 0 {
        encode_tag(field, 0, buf);
        encode_varint(value, buf);
    }
}

/// Encode a fixed64 field (wire type 1).
fn encode_fixed64_field(field: u32, value: u64, buf: &mut Vec<u8>) {
    encode_tag(field, 1, buf);
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Encode a `bool` field as varint (wire type 0).
fn encode_bool_field(field: u32, value: bool, buf: &mut Vec<u8>) {
    if value {
        encode_varint_field(field, 1, buf);
    }
}

// ---------------------------------------------------------------------------
// OTLP message builders
// ---------------------------------------------------------------------------

/// Current wall-clock time as nanoseconds since Unix epoch.
fn now_nanos() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// `AnyValue { string_value = 1 }`
fn encode_any_value_string(s: &str, buf: &mut Vec<u8>) {
    encode_string_field(1, s, buf);
}

/// `KeyValue { key = 1, value = 2 }`
fn encode_key_value_string(key: &str, value: &str, buf: &mut Vec<u8>) {
    encode_string_field(1, key, buf);
    // value field is AnyValue message at field 2
    let mut av = Vec::new();
    encode_any_value_string(value, &mut av);
    encode_len_field(2, &av, buf);
}

/// `Resource { attributes = 1 }`
fn encode_resource(attrs: &[(&str, &str)], buf: &mut Vec<u8>) {
    for &(k, v) in attrs {
        let mut kv_buf = Vec::new();
        encode_key_value_string(k, v, &mut kv_buf);
        encode_len_field(1, &kv_buf, buf);
    }
}

/// `InstrumentationScope { name = 1, version = 2 }`
fn encode_instrumentation_scope(name: &str, version: &str, buf: &mut Vec<u8>) {
    encode_string_field(1, name, buf);
    encode_string_field(2, version, buf);
}

/// A `NumberDataPoint` with an `as_int` (sfixed64) value.
fn encode_number_data_point_int(
    value: i64,
    time_nanos: u64,
    start_time_nanos: u64,
    buf: &mut Vec<u8>,
) {
    encode_fixed64_field(2, start_time_nanos, buf);
    encode_fixed64_field(3, time_nanos, buf);
    // as_int = field 6, wire type 1 (sfixed64)
    encode_fixed64_field(6, value as u64, buf);
}

/// A `NumberDataPoint` with an `as_double` value.
fn encode_number_data_point_double(
    value: f64,
    time_nanos: u64,
    start_time_nanos: u64,
    buf: &mut Vec<u8>,
) {
    encode_fixed64_field(2, start_time_nanos, buf);
    encode_fixed64_field(3, time_nanos, buf);
    // as_double = field 4, wire type 1
    encode_fixed64_field(4, value.to_bits(), buf);
}

/// AggregationTemporality enum values.
const AGGREGATION_TEMPORALITY_DELTA: u64 = 1;
const AGGREGATION_TEMPORALITY_CUMULATIVE: u64 = 2;

/// Encode a complete `Metric` message for a counter (Sum, monotonic).
fn encode_metric_counter(
    name: &str,
    description: &str,
    value: i64,
    delta: bool,
    time_nanos: u64,
    start_time_nanos: u64,
    buf: &mut Vec<u8>,
) {
    encode_string_field(1, name, buf); // name
    encode_string_field(2, description, buf); // description

    // Sum message at field 7
    let mut sum = Vec::new();
    // data_points = field 1
    let mut dp = Vec::new();
    encode_number_data_point_int(value, time_nanos, start_time_nanos, &mut dp);
    encode_len_field(1, &dp, &mut sum);
    // aggregation_temporality = field 2
    let temporality = if delta {
        AGGREGATION_TEMPORALITY_DELTA
    } else {
        AGGREGATION_TEMPORALITY_CUMULATIVE
    };
    encode_varint_field(2, temporality, &mut sum);
    // is_monotonic = field 3
    encode_bool_field(3, true, &mut sum);

    encode_len_field(7, &sum, buf);
}

/// Encode a complete `Metric` message for a gauge (int value).
fn encode_metric_gauge_int(
    name: &str,
    description: &str,
    value: i64,
    time_nanos: u64,
    start_time_nanos: u64,
    buf: &mut Vec<u8>,
) {
    encode_string_field(1, name, buf);
    encode_string_field(2, description, buf);

    // Gauge message at field 5
    let mut gauge = Vec::new();
    let mut dp = Vec::new();
    encode_number_data_point_int(value, time_nanos, start_time_nanos, &mut dp);
    encode_len_field(1, &dp, &mut gauge);

    encode_len_field(5, &gauge, buf);
}

/// Encode a complete `Metric` message for a gauge (double value).
fn encode_metric_gauge_double(
    name: &str,
    description: &str,
    value: f64,
    time_nanos: u64,
    start_time_nanos: u64,
    buf: &mut Vec<u8>,
) {
    encode_string_field(1, name, buf);
    encode_string_field(2, description, buf);

    let mut gauge = Vec::new();
    let mut dp = Vec::new();
    encode_number_data_point_double(value, time_nanos, start_time_nanos, &mut dp);
    encode_len_field(1, &dp, &mut gauge);

    encode_len_field(5, &gauge, buf);
}

// ---------------------------------------------------------------------------
// OtlpExporter — implements MetricsExporter
// ---------------------------------------------------------------------------

/// Collects metrics in OTLP `MetricsData` v1 protobuf wire format.
///
/// This exporter implements [`MetricsExporter`] so it can be used with any
/// `MetricsVisitable` type. After calling `export_metrics()`, call
/// [`finish()`](OtlpExporter::finish) to obtain the serialised protobuf bytes.
///
/// Counter metrics are encoded as `Sum` (monotonic) data points.
/// Gauge metrics are encoded as `Gauge` data points.
/// Latency trackers map to multiple gauges: `*_count`, `*_sum_seconds`,
/// `*_min_seconds`, `*_max_seconds`, `*_avg_seconds`.
pub struct OtlpExporter {
    /// Whether to emit delta temporality for counters.
    delta: bool,
    /// Timestamp for all data points (nanoseconds since epoch).
    time_nanos: u64,
    /// Start time for this collection window.
    start_time_nanos: u64,
    /// Encoded Metric messages (not yet wrapped in ScopeMetrics).
    metrics: Vec<Vec<u8>>,
    /// Resource attributes (key-value pairs).
    resource_attrs: Vec<(String, String)>,
}

impl OtlpExporter {
    /// Create a new OTLP exporter.
    ///
    /// * `delta` — if `true`, counters use `DELTA` temporality; otherwise `CUMULATIVE`.
    /// * `start_time_nanos` — the start of the collection window (nanos since epoch).
    pub fn new(delta: bool, start_time_nanos: u64) -> Self {
        Self {
            delta,
            time_nanos: now_nanos(),
            start_time_nanos,
            metrics: Vec::with_capacity(32),
            resource_attrs: Vec::new(),
        }
    }

    pub(crate) fn with_timestamps(delta: bool, start_time_nanos: u64, time_nanos: u64) -> Self {
        Self {
            delta,
            time_nanos,
            start_time_nanos,
            metrics: Vec::with_capacity(32),
            resource_attrs: Vec::new(),
        }
    }

    /// Add a resource attribute (e.g., `"service.name"` → `"krafka"`).
    pub fn add_resource_attribute(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.resource_attrs.push((key.into(), value.into()));
    }

    pub(crate) fn push_metric_bytes(&mut self, metric: Vec<u8>) {
        self.metrics.push(metric);
    }

    pub(crate) fn into_metric_bytes(self) -> Vec<Vec<u8>> {
        self.metrics
    }

    /// Return the number of metric entries collected so far (for testing).
    #[cfg(test)]
    pub(crate) fn finish_metric_count(&self) -> usize {
        self.metrics.len()
    }

    /// Consume the exporter and produce the serialised `MetricsData` protobuf.
    pub fn finish(self) -> Vec<u8> {
        // Build ScopeMetrics
        let mut scope_metrics = Vec::new();
        // scope = field 1
        let mut scope = Vec::new();
        encode_instrumentation_scope("krafka", env!("CARGO_PKG_VERSION"), &mut scope);
        encode_len_field(1, &scope, &mut scope_metrics);
        // metrics = field 2 (repeated)
        for m in &self.metrics {
            encode_len_field(2, m, &mut scope_metrics);
        }

        // Build ResourceMetrics
        let mut resource_metrics = Vec::new();
        // resource = field 1
        if !self.resource_attrs.is_empty() {
            let attrs: Vec<(&str, &str)> = self
                .resource_attrs
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();
            let mut res = Vec::new();
            encode_resource(&attrs, &mut res);
            encode_len_field(1, &res, &mut resource_metrics);
        }
        // scope_metrics = field 2
        encode_len_field(2, &scope_metrics, &mut resource_metrics);

        // Build MetricsData
        let mut metrics_data = Vec::new();
        // resource_metrics = field 1
        encode_len_field(1, &resource_metrics, &mut metrics_data);

        metrics_data
    }
}

impl MetricsExporter for OtlpExporter {
    fn export_counter(&mut self, name: &str, help: &str, value: u64) {
        let mut buf = Vec::new();
        encode_metric_counter(
            name,
            help,
            value as i64,
            self.delta,
            self.time_nanos,
            self.start_time_nanos,
            &mut buf,
        );
        self.metrics.push(buf);
    }

    fn export_gauge(&mut self, name: &str, help: &str, value: u64) {
        let mut buf = Vec::new();
        encode_metric_gauge_int(
            name,
            help,
            value as i64,
            self.time_nanos,
            self.start_time_nanos,
            &mut buf,
        );
        self.metrics.push(buf);
    }

    fn export_latency(&mut self, name: &str, help: &str, snapshot: &LatencySnapshot) {
        // Pre-build name/help variants once to avoid repeated format! allocations.
        let name_count = format!("{name}_count");
        let name_sum = format!("{name}_sum_seconds");

        let mut buf = Vec::new();
        encode_metric_gauge_int(
            &name_count,
            help,
            snapshot.count as i64,
            self.time_nanos,
            self.start_time_nanos,
            &mut buf,
        );
        self.metrics.push(buf);

        let mut buf = Vec::new();
        encode_metric_gauge_double(
            &name_sum,
            help,
            snapshot.sum.as_secs_f64(),
            self.time_nanos,
            self.start_time_nanos,
            &mut buf,
        );
        self.metrics.push(buf);

        if let Some(min) = snapshot.min {
            let mut buf = Vec::new();
            encode_metric_gauge_double(
                &format!("{name}_min_seconds"),
                help,
                min.as_secs_f64(),
                self.time_nanos,
                self.start_time_nanos,
                &mut buf,
            );
            self.metrics.push(buf);
        }
        if let Some(max) = snapshot.max {
            let mut buf = Vec::new();
            encode_metric_gauge_double(
                &format!("{name}_max_seconds"),
                help,
                max.as_secs_f64(),
                self.time_nanos,
                self.start_time_nanos,
                &mut buf,
            );
            self.metrics.push(buf);
        }
        if let Some(avg) = snapshot.avg {
            let mut buf = Vec::new();
            encode_metric_gauge_double(
                &format!("{name}_avg_seconds"),
                help,
                avg.as_secs_f64(),
                self.time_nanos,
                self.start_time_nanos,
                &mut buf,
            );
            self.metrics.push(buf);
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_encode_varint_single_byte() {
        let mut buf = Vec::new();
        encode_varint(0, &mut buf);
        assert_eq!(buf, vec![0]);

        buf.clear();
        encode_varint(127, &mut buf);
        assert_eq!(buf, vec![127]);
    }

    #[test]
    fn test_encode_varint_multi_byte() {
        let mut buf = Vec::new();
        encode_varint(300, &mut buf);
        // 300 = 0b100101100 → [0xAC, 0x02]
        assert_eq!(buf, vec![0xAC, 0x02]);
    }

    #[test]
    fn test_encode_string_field() {
        let mut buf = Vec::new();
        encode_string_field(1, "hello", &mut buf);
        // tag = (1 << 3) | 2 = 0x0A, len = 5, "hello"
        assert_eq!(&buf[0..2], &[0x0A, 5]);
        assert_eq!(&buf[2..], b"hello");
    }

    #[test]
    fn test_otlp_exporter_counter() {
        let start = now_nanos().saturating_sub(1_000_000_000);
        let mut exporter = OtlpExporter::new(false, start);
        exporter.export_counter("test_counter", "A test counter", 42);

        let data = exporter.finish();
        // Should produce non-empty protobuf
        assert!(!data.is_empty());
        // First byte should be field 1, wire type 2 (LEN) = 0x0A
        assert_eq!(data[0], 0x0A);
    }

    #[test]
    fn test_otlp_exporter_gauge() {
        let start = now_nanos().saturating_sub(1_000_000_000);
        let mut exporter = OtlpExporter::new(false, start);
        exporter.export_gauge("test_gauge", "A test gauge", 7);

        let data = exporter.finish();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_otlp_exporter_latency() {
        let start = now_nanos().saturating_sub(1_000_000_000);
        let mut exporter = OtlpExporter::new(true, start);
        let snapshot = LatencySnapshot {
            count: 10,
            sum: Duration::from_millis(500),
            min: Some(Duration::from_millis(10)),
            max: Some(Duration::from_millis(100)),
            avg: Some(Duration::from_millis(50)),
        };
        exporter.export_latency("test_latency", "A test latency", &snapshot);

        let data = exporter.finish();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_otlp_exporter_with_resource_attrs() {
        let start = now_nanos();
        let mut exporter = OtlpExporter::new(false, start);
        exporter.add_resource_attribute("service.name", "krafka");
        exporter.add_resource_attribute("client_rack", "us-east-1a");
        exporter.export_counter("c", "counter", 1);

        let data = exporter.finish();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_otlp_exporter_empty() {
        let exporter = OtlpExporter::new(false, now_nanos());
        let data = exporter.finish();
        // Even with no metrics, should produce a valid (but minimal) protobuf
        assert!(!data.is_empty());
    }

    #[test]
    fn test_delta_vs_cumulative() {
        let start = now_nanos();

        let mut delta_exporter = OtlpExporter::new(true, start);
        delta_exporter.export_counter("c", "counter", 5);
        let delta_data = delta_exporter.finish();

        let mut cumul_exporter = OtlpExporter::new(false, start);
        cumul_exporter.export_counter("c", "counter", 5);
        let cumul_data = cumul_exporter.finish();

        // Should produce different encodings (temporality field differs)
        assert_ne!(delta_data, cumul_data);
    }

    #[test]
    fn test_otlp_exporter_latency_sparse() {
        let start = now_nanos().saturating_sub(1_000_000_000);
        let mut exporter = OtlpExporter::new(false, start);
        let snapshot = LatencySnapshot {
            count: 0,
            sum: Duration::ZERO,
            min: None,
            max: None,
            avg: None,
        };
        exporter.export_latency("test_latency", "A sparse latency", &snapshot);

        // With no samples, only count and sum are emitted (min/max/avg are None)
        assert_eq!(exporter.finish_metric_count(), 2);
    }
}
