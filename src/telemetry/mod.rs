//! KIP-714 client telemetry: subscription polling and metric push (feature-gated).
//!
//! When a broker supports `GetTelemetrySubscriptions` (API Key 71), the
//! [`TelemetryReporter`](reporter::TelemetryReporter) background task periodically:
//!
//! 1. Fetches the broker's metrics subscription via `GetTelemetrySubscriptions`.
//! 2. Serialises matching metrics as OTLP `MetricsData` v1 protobuf.
//! 3. Pushes them via `PushTelemetry` at the broker-specified interval.
//!
//! On shutdown the reporter sends a final push with `terminating = true`.

pub mod otlp;
pub mod reporter;
