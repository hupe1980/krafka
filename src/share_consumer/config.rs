//! Share consumer configuration (KIP-932).

use std::time::Duration;

/// Acknowledgement mode for share consumers (KIP-932).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AcknowledgementMode {
    /// The records a `poll()`/`recv()` returned are accepted when the next
    /// `poll()`/`recv()` starts, or by `commit()`/`close()`.
    #[default]
    Implicit,
    /// The application settles every delivered record with
    /// [`ack`](super::ShareConsumer::ack), [`release`](super::ShareConsumer::release)
    /// or [`reject`](super::ShareConsumer::reject) before the next `poll()`.
    Explicit,
}

/// How the broker bounds the records one `ShareFetch` acquires (KIP-1206).
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AcquireMode {
    /// The broker may finish a record batch beyond `max_poll_records`
    /// (`ShareAcquireMode` 0).
    #[default]
    BatchOptimized,
    /// The broker acquires at most `max_poll_records` records per `ShareFetch`
    /// (`ShareAcquireMode` 1). Needs `ShareFetch` v2 (Kafka 4.2+); against an
    /// older broker the first `poll()` fails with a `Config` error.
    RecordLimit,
}

impl AcquireMode {
    /// The wire value of `ShareAcquireMode`.
    pub(crate) fn to_i8(self) -> i8 {
        match self {
            Self::BatchOptimized => 0,
            Self::RecordLimit => 1,
        }
    }
}

/// Share consumer settings, as the builder collected them.
#[derive(Debug, Clone)]
pub(crate) struct ShareConsumerConfig {
    /// Share group ID (required).
    pub(crate) group_id: String,
    /// Acknowledgement mode.
    pub(crate) acknowledgement_mode: AcknowledgementMode,
    /// How the broker bounds acquisition per `ShareFetch` (KIP-1206).
    pub(crate) acquire_mode: AcquireMode,
    /// Minimum bytes to fetch.
    pub(crate) fetch_min_bytes: i32,
    /// Maximum bytes to fetch.
    pub(crate) fetch_max_bytes: i32,
    /// Maximum records returned to the application per `poll()`, and the
    /// `MaxRecords` of every `ShareFetch`. Must be >= 1. Defaults to 500.
    pub(crate) max_poll_records: i32,
    /// Batch-size hint sent with every `ShareFetch`, capped at
    /// `max_poll_records`.
    pub(crate) batch_size: i32,
    /// How long a broker may hold a `ShareFetch` waiting for
    /// [`fetch_min_bytes`](Self::fetch_min_bytes) to accumulate.
    pub(crate) fetch_max_wait: Duration,
    /// The handle's request timeout, copied at build time.
    pub(crate) request_timeout: Duration,
    /// Client rack ID for closest-replica fetching (KIP-392).
    pub(crate) client_rack: Option<String>,
    /// Maximum decompressed size for record batches (compression bomb protection).
    /// Defaults to 128 MiB.
    pub(crate) max_decompressed_size: usize,
    /// Push this client's metrics to brokers that subscribe to them (KIP-714,
    /// Java `enable.metrics.push`).
    pub(crate) metrics_push: bool,
}

impl Default for ShareConsumerConfig {
    fn default() -> Self {
        Self {
            group_id: String::new(),
            acknowledgement_mode: AcknowledgementMode::Implicit,
            acquire_mode: AcquireMode::BatchOptimized,
            fetch_min_bytes: 1,
            fetch_max_bytes: 52_428_800, // 50 MiB
            max_poll_records: 500,
            batch_size: 500,
            fetch_max_wait: Duration::from_millis(500),
            request_timeout: Duration::from_secs(30),
            client_rack: None,
            max_decompressed_size: crate::protocol::RecordBatch::MAX_DECOMPRESSED_SIZE,
            metrics_push: true,
        }
    }
}
