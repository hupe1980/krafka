//! Building a [`ShareConsumer`].

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU64};
use std::time::Duration;

use ahash::AHashMap as HashMap;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tracing::info;

use super::ShareConsumer;
use super::commit::{AcknowledgementCommit, AcknowledgementCommitCallback};
use super::config::{AcknowledgementMode, AcquireMode, ShareConsumerConfig};
use super::membership::MemberState;
use super::state::{Inner, State};
use crate::client::Kafka;
use crate::error::{KrafkaError, Result};

/// Builder for a [`ShareConsumer`]: share-consumer settings only. Obtain
/// with [`Kafka::share_consumer`].
#[must_use = "builders do nothing until .build() is called"]
pub struct ShareConsumerBuilder {
    kafka: Kafka,
    config: ShareConsumerConfig,
    /// Optional decoder applied to every consumed record's key.
    key_deserializer: Option<Arc<dyn crate::serdes::Deserializer>>,
    /// Optional decoder applied to every consumed record's value.
    value_deserializer: Option<Arc<dyn crate::serdes::Deserializer>>,
    callback: Option<AcknowledgementCommitCallback>,
}

impl std::fmt::Debug for ShareConsumerBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShareConsumerBuilder")
            .field("group_id", &self.config.group_id)
            .finish_non_exhaustive()
    }
}

impl ShareConsumerBuilder {
    pub(crate) fn new(kafka: Kafka, group_id: String) -> Self {
        let config = ShareConsumerConfig {
            group_id,
            request_timeout: kafka.request_timeout(),
            ..ShareConsumerConfig::default()
        };
        Self {
            kafka,
            config,
            key_deserializer: None,
            value_deserializer: None,
            callback: None,
        }
    }

    /// Decode every record key before it is returned; see
    /// [`ConsumerBuilder::key_deserializer`](crate::consumer::ConsumerBuilder::key_deserializer).
    /// A record that fails to decode is released for redelivery, after the
    /// records before it are returned.
    pub fn key_deserializer(
        mut self,
        deserializer: impl crate::serdes::Deserializer + 'static,
    ) -> Self {
        self.key_deserializer = Some(Arc::new(deserializer));
        self
    }

    /// Decode every record value before it is returned; see
    /// [`key_deserializer`](Self::key_deserializer).
    pub fn value_deserializer(
        mut self,
        deserializer: impl crate::serdes::Deserializer + 'static,
    ) -> Self {
        self.value_deserializer = Some(Arc::new(deserializer));
        self
    }

    /// Set the acknowledgement mode.
    pub fn acknowledgement_mode(mut self, mode: AcknowledgementMode) -> Self {
        self.config.acknowledgement_mode = mode;
        self
    }

    /// Set the maximum number of records returned per `poll()` call.
    ///
    /// Also the `MaxRecords` of every `ShareFetch`, so it bounds how many
    /// records this member holds acquisition locks on from one fetch. Must be
    /// >= 1. Defaults to 500.
    pub fn max_poll_records(mut self, max: i32) -> Self {
        self.config.max_poll_records = max;
        self
    }

    /// Set how the broker bounds acquisition per `ShareFetch` (KIP-1206).
    ///
    /// [`AcquireMode::RecordLimit`] makes `max_poll_records` a hard limit; it
    /// needs Kafka 4.2+ and fails the first `poll()` with a `Config` error on
    /// an older broker. Defaults to [`AcquireMode::BatchOptimized`].
    pub fn acquire_mode(mut self, mode: AcquireMode) -> Self {
        self.config.acquire_mode = mode;
        self
    }

    /// Set a callback that receives the outcome of every acknowledgement
    /// request, per partition: explicit and implicit acknowledgements,
    /// those piggybacked on fetches, and those sent by `commit()` and
    /// `close()`.
    ///
    /// It runs on a background task between requests, so it must be quick;
    /// a panic in it is caught and logged.
    pub fn acknowledgement_commit_callback(
        mut self,
        callback: impl Fn(&AcknowledgementCommit) + Send + Sync + 'static,
    ) -> Self {
        self.callback = Some(Arc::new(callback));
        self
    }

    /// Set how long a broker may hold a `ShareFetch` waiting for
    /// [`fetch_min_bytes`](Self::fetch_min_bytes) to accumulate.
    ///
    /// Capped by the `poll()` timeout, so a short poll is never made to wait
    /// for a long fetch. Defaults to 500 ms — the same trade as the regular
    /// consumer's [`fetch_max_wait`](crate::consumer::ConsumerBuilder::fetch_max_wait).
    pub fn fetch_max_wait(mut self, wait: Duration) -> Self {
        self.config.fetch_max_wait = wait;
        self
    }

    /// Set the minimum bytes a broker must have before answering a
    /// `ShareFetch`.
    ///
    /// Raising it trades latency for fewer, fuller responses; the broker still
    /// answers after [`fetch_max_wait`](Self::fetch_max_wait) regardless.
    /// Defaults to 1 (answer as soon as anything is available).
    pub fn fetch_min_bytes(mut self, bytes: i32) -> Self {
        self.config.fetch_min_bytes = bytes;
        self
    }

    /// Set the maximum bytes one `ShareFetch` response may carry.
    ///
    /// Defaults to 50 MiB, matching the regular consumer's `fetch_max_bytes`.
    pub fn fetch_max_bytes(mut self, bytes: i32) -> Self {
        self.config.fetch_max_bytes = bytes;
        self
    }

    /// Set the batch size the broker should aim for when acquiring records
    /// (KIP-932 `BatchSize`).
    ///
    /// A hint, not a limit, capped at `max_poll_records`. Defaults to 500.
    pub fn batch_size(mut self, size: i32) -> Self {
        self.config.batch_size = size;
        self
    }

    /// Set the client rack ID.
    pub fn client_rack(mut self, rack: impl Into<String>) -> Self {
        self.config.client_rack = Some(rack.into());
        self
    }

    /// Set the maximum decompressed size for record batches.
    ///
    /// Compressed payloads that decompress beyond this limit are rejected as
    /// potential compression bombs. Defaults to 128 MiB.
    pub fn max_decompressed_size(mut self, size: usize) -> Self {
        self.config.max_decompressed_size = size;
        self
    }

    /// Push this consumer's metrics to the brokers when a cluster operator
    /// subscribes to them (KIP-714, Java `enable.metrics.push`). Default: on.
    /// Nothing is sent to a cluster without a client-telemetry plugin.
    pub fn metrics_push(mut self, enable: bool) -> Self {
        self.config.metrics_push = enable;
        self
    }

    /// Build the share consumer. Contacts no broker: the group is joined by
    /// [`subscribe`](ShareConsumer::subscribe).
    ///
    /// # Errors
    ///
    /// [`KrafkaError::Config`] naming the setting for an invalid
    /// configuration.
    // Async like every other client's `build`.
    #[allow(clippy::unused_async)]
    pub async fn build(self) -> Result<ShareConsumer> {
        if self.config.group_id.is_empty() {
            return Err(KrafkaError::config(
                "group_id is required for share consumers",
            ));
        }
        if self.config.max_poll_records < 1 {
            return Err(KrafkaError::config(format!(
                "max_poll_records ({}) must be >= 1",
                self.config.max_poll_records,
            )));
        }
        let ShareConsumerBuilder {
            kafka,
            config,
            key_deserializer,
            value_deserializer,
            callback,
        } = self;
        info!(group_id = %config.group_id, "share consumer created");
        let metrics = Arc::new(crate::metrics::ConsumerRecorder::default());
        let metrics_source = crate::metrics::MetricsSource::consumer(&kafka, Arc::clone(&metrics));
        let telemetry = crate::telemetry::Telemetry::start(
            config.metrics_push,
            &kafka,
            crate::telemetry::ClientType::Consumer,
            Arc::clone(&metrics_source),
        );
        Ok(ShareConsumer(Arc::new(Inner {
            config,
            metadata: Arc::clone(kafka.metadata()),
            pool: Arc::clone(kafka.pool()),
            metrics,
            metrics_source,
            telemetry,
            key_deserializer,
            value_deserializer,
            callback,
            member: Mutex::new(MemberState::new()),
            state: Mutex::new(State::default()),
            records_ready: Notify::new(),
            poll_lock: tokio::sync::Mutex::new(()),
            closed: AtomicBool::new(false),
            shut_down: AtomicBool::new(false),
            wakeup: AtomicBool::new(false),
            session_generation: AtomicU64::new(0),
            acquisition_lock_timeout_ms: AtomicI32::new(-1),
            nodes: Mutex::new(HashMap::new()),
            heartbeat_task: Mutex::new(None),
        })))
    }
}
