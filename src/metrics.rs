//! Client metrics.
//!
//! Every client returns its counters as one owned [`Metrics`] snapshot from
//! `metrics()`, and [`Kafka::metrics`](crate::Kafka::metrics) sums the clients
//! of a handle. A snapshot is plain data: render it with
//! [`Metrics::prometheus_text`], or read its fields and render any other
//! format. The KIP-714 reporter pushes the same snapshot to the broker.
//!
//! ```rust,no_run
//! # async fn example(kafka: krafka::Kafka) -> krafka::Result<()> {
//! let producer = kafka.producer().build().await?;
//! // ... send ...
//! let metrics = producer.metrics();
//! println!("sent {} records over {} connections",
//!     metrics.producer.records_sent, metrics.connections.connections_created);
//!
//! // Every client of the handle, connections counted once.
//! let scrape: String = kafka.metrics().prometheus_text();
//! # Ok(())
//! # }
//! ```
//!
//! Counters are monotonic for a client's lifetime. A snapshot carries every
//! section; the sections a client does not use read zero.

use std::fmt::Write as _;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::time::Instant;

use ahash::AHashMap;

use crate::network::ConnectionPool;

/// Default maximum number of distinct topics a producer tracks in
/// [`ProducerMetrics::topics`].
pub const DEFAULT_MAX_TRACKED_TOPICS: usize = 1000;

/// Topic name under which every topic beyond [`DEFAULT_MAX_TRACKED_TOPICS`]
/// is aggregated in [`ProducerMetrics::topics`]. It bounds memory and the
/// number of exported series for producers with an unbounded topic domain.
pub const OVERFLOW_TOPIC_KEY: &str = "__other__";

// ── The snapshot ────────────────────────────────────────────────────────────

/// A client's metrics at one instant: an owned copy, unaffected by later
/// activity.
///
/// Returned by `metrics()` on every client and on [`Kafka`](crate::Kafka).
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct Metrics {
    /// The `client_id` of the client this snapshot belongs to; `None` for the
    /// sum [`Kafka::metrics`](crate::Kafka::metrics) returns.
    pub client_id: Option<String>,
    /// Producer counters; zero on a consumer or an admin client.
    pub producer: ProducerMetrics,
    /// Consumer counters, for both the consumer and the share consumer; zero
    /// on a producer or an admin client.
    pub consumer: ConsumerMetrics,
    /// Counters of the connection pool the client shares with every client of
    /// its [`Kafka`](crate::Kafka) handle.
    pub connections: ConnectionMetrics,
}

/// Count, total and maximum of a timed operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct Latency {
    /// Number of samples.
    pub count: u64,
    /// Sum of every sample.
    pub sum: Duration,
    /// Largest sample.
    pub max: Duration,
}

impl Latency {
    /// Mean sample, or `None` without samples.
    #[must_use]
    pub fn mean(&self) -> Option<Duration> {
        (self.count > 0).then(|| {
            let nanos = self.sum.as_nanos() / u128::from(self.count);
            Duration::from_nanos(u64::try_from(nanos).unwrap_or(u64::MAX))
        })
    }

    fn add(&mut self, other: &Self) {
        self.count += other.count;
        self.sum += other.sum;
        self.max = self.max.max(other.max);
    }
}

/// Producer counters.
#[derive(Debug, Clone, Default, PartialEq)]
#[non_exhaustive]
pub struct ProducerMetrics {
    /// Records the broker acknowledged.
    pub records_sent: u64,
    /// Estimated bytes of the acknowledged records.
    pub bytes_sent: u64,
    /// Batches the broker acknowledged.
    pub batches_sent: u64,
    /// Batches that failed with a terminal error.
    pub errors: u64,
    /// Batch retries.
    pub retries: u64,
    /// Times the broker reported a batch of this producer lost
    /// (`OUT_OF_ORDER_SEQUENCE_NUMBER`, or `UNKNOWN_PRODUCER_ID` while the log
    /// still held the producer's last acknowledged batch).
    pub data_loss_detected: u64,
    /// Estimated encoded bytes of compressed batches.
    pub compressed_bytes: u64,
    /// Estimated bytes of the same batches before compression.
    pub uncompressed_bytes: u64,
    /// Records currently held under `buffer_memory` (a gauge).
    pub buffered_records: u64,
    /// Time from a batch's first record to its acknowledgement.
    pub send_latency: Latency,
    /// Per-topic counters, sorted by topic. At most
    /// [`DEFAULT_MAX_TRACKED_TOPICS`] topics, plus [`OVERFLOW_TOPIC_KEY`].
    pub topics: Vec<TopicMetrics>,
}

impl ProducerMetrics {
    /// `compressed_bytes / uncompressed_bytes`, or `None` before the first
    /// compressed batch. Below `1.0` the codec saves bytes.
    #[must_use]
    pub fn compression_ratio(&self) -> Option<f64> {
        (self.uncompressed_bytes > 0)
            .then(|| self.compressed_bytes as f64 / self.uncompressed_bytes as f64)
    }

    fn add(&mut self, other: &Self) {
        self.records_sent += other.records_sent;
        self.bytes_sent += other.bytes_sent;
        self.batches_sent += other.batches_sent;
        self.errors += other.errors;
        self.retries += other.retries;
        self.data_loss_detected += other.data_loss_detected;
        self.compressed_bytes += other.compressed_bytes;
        self.uncompressed_bytes += other.uncompressed_bytes;
        self.buffered_records += other.buffered_records;
        self.send_latency.add(&other.send_latency);
        for theirs in &other.topics {
            match self.topics.iter_mut().find(|t| t.topic == theirs.topic) {
                Some(ours) => {
                    ours.records_sent += theirs.records_sent;
                    ours.bytes_sent += theirs.bytes_sent;
                    ours.errors += theirs.errors;
                }
                None => self.topics.push(theirs.clone()),
            }
        }
        self.topics.sort_unstable_by(|a, b| a.topic.cmp(&b.topic));
    }
}

/// One topic's producer counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TopicMetrics {
    /// Topic name, or [`OVERFLOW_TOPIC_KEY`].
    pub topic: String,
    /// Records the broker acknowledged.
    pub records_sent: u64,
    /// Estimated bytes of the acknowledged records.
    pub bytes_sent: u64,
    /// Batches that failed with a terminal error.
    pub errors: u64,
}

/// Consumer and share-consumer counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConsumerMetrics {
    /// Records handed to the application.
    pub records_received: u64,
    /// Value bytes of the records handed to the application.
    pub bytes_received: u64,
    /// Fetch requests sent.
    pub fetches: u64,
    /// `poll`/`recv` rounds.
    pub polls: u64,
    /// Rounds that returned no record.
    pub empty_polls: u64,
    /// Offset commits (share consumer: acknowledgement commits).
    pub commits: u64,
    /// Errors returned from `poll`/`recv`.
    pub errors: u64,
    /// Assignment changes applied.
    pub rebalances: u64,
    /// Partitions repositioned by a seek.
    pub seeks: u64,
    /// Record batches that failed to decode: CRC mismatch, unknown magic,
    /// out-of-range fields. Each increment also logs a `warn!` naming the
    /// partition and offset.
    pub batch_decode_errors: u64,
    /// Records behind the end of the log, summed over assigned partitions (a
    /// gauge).
    pub lag: u64,
    /// Largest per-partition lag (a gauge).
    pub lag_max: u64,
    /// Assigned partitions (a gauge).
    pub assigned_partitions: u64,
    /// Paused partitions (a gauge).
    pub paused_partitions: u64,
    /// Records fetched and not yet handed out (a gauge).
    pub buffered_records: u64,
    /// Duration of `poll` rounds.
    pub poll_latency: Latency,
    /// Duration of fetch round trips.
    pub fetch_latency: Latency,
}

impl ConsumerMetrics {
    fn add(&mut self, other: &Self) {
        self.records_received += other.records_received;
        self.bytes_received += other.bytes_received;
        self.fetches += other.fetches;
        self.polls += other.polls;
        self.empty_polls += other.empty_polls;
        self.commits += other.commits;
        self.errors += other.errors;
        self.rebalances += other.rebalances;
        self.seeks += other.seeks;
        self.batch_decode_errors += other.batch_decode_errors;
        self.lag += other.lag;
        self.lag_max = self.lag_max.max(other.lag_max);
        self.assigned_partitions += other.assigned_partitions;
        self.paused_partitions += other.paused_partitions;
        self.buffered_records += other.buffered_records;
        self.poll_latency.add(&other.poll_latency);
        self.fetch_latency.add(&other.fetch_latency);
    }
}

/// Connection-pool counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct ConnectionMetrics {
    /// Connections opened.
    pub connections_created: u64,
    /// Connections closed.
    pub connections_closed: u64,
    /// Connections that failed.
    pub connection_errors: u64,
    /// Connections open now (a gauge).
    pub active_connections: u64,
    /// Requests held back by a broker throttle (KIP-219).
    pub throttle_delays: u64,
    /// Total time requests were held back by broker throttles, in
    /// milliseconds.
    pub throttle_delay_ms: u64,
    /// Connections closed because a request on them timed out.
    pub stalled_connections: u64,
    /// Coordination requests sent on the shared data connection because the
    /// connection cap left no room for a coordination connection.
    pub coordination_fallbacks: u64,
    /// TLS handshakes.
    pub tls_handshake_latency: Latency,
    /// SASL/OAUTHBEARER token fetches attempted, by a token provider.
    pub oauth_token_fetches: u64,
    /// SASL/OAUTHBEARER token fetches that failed — what a misconfigured
    /// token endpoint shows as.
    pub oauth_token_fetch_failures: u64,
    /// Successful SASL/OAUTHBEARER token fetches.
    pub oauth_token_fetch_latency: Latency,
    /// Expiry of the cached OAUTHBEARER token, in milliseconds since the Unix
    /// epoch; `0` when unknown (a gauge).
    pub oauth_token_expiry_epoch_ms: u64,
}

impl Metrics {
    /// Add `other`'s client counters (producer and consumer sections) to
    /// these. Connection counters are left alone: clients of one handle share
    /// one pool.
    pub(crate) fn add_client(&mut self, other: &Self) {
        self.producer.add(&other.producer);
        self.consumer.add(&other.consumer);
    }

    /// The snapshot in the Prometheus text exposition format, under
    /// `krafka_*` names.
    ///
    /// A client's snapshot labels every series with `client_id`; the
    /// [`Kafka::metrics`](crate::Kafka::metrics) sum has no client label.
    /// Two clients with the same `client_id` rendered into one scrape
    /// collide: scrape the `Kafka` sum, or give the handles distinct ids.
    #[must_use]
    pub fn prometheus_text(&self) -> String {
        let client = self
            .client_id
            .as_deref()
            .map(|id| format!("client_id=\"{}\"", escape_label(id)));
        let labels = |extra: Option<String>| -> String {
            let parts: Vec<String> = client.iter().cloned().chain(extra).collect();
            if parts.is_empty() {
                String::new()
            } else {
                format!("{{{}}}", parts.join(","))
            }
        };
        let plain = labels(None);
        let mut out = String::with_capacity(8 * 1024);

        for m in self.scalars() {
            let kind = match m.kind {
                Kind::Counter => "counter",
                Kind::Gauge => "gauge",
            };
            let _ = writeln!(out, "# HELP {} {}", m.prometheus, m.help);
            let _ = writeln!(out, "# TYPE {} {kind}", m.prometheus);
            let _ = writeln!(out, "{}{plain} {}", m.prometheus, m.value);
        }
        for m in self.latencies() {
            let name = m.prometheus;
            let _ = writeln!(out, "# HELP {name}_seconds {}", m.help);
            let _ = writeln!(out, "# TYPE {name}_seconds summary");
            let _ = writeln!(
                out,
                "{name}_seconds_sum{plain} {}",
                m.value.sum.as_secs_f64()
            );
            let _ = writeln!(out, "{name}_seconds_count{plain} {}", m.value.count);
            let _ = writeln!(out, "# HELP {name}_max_seconds Largest sample: {}", m.help);
            let _ = writeln!(out, "# TYPE {name}_max_seconds gauge");
            let _ = writeln!(
                out,
                "{name}_max_seconds{plain} {}",
                m.value.max.as_secs_f64()
            );
        }
        if !self.producer.topics.is_empty() {
            type Field = fn(&TopicMetrics) -> u64;
            let families: [(&str, &str, Field); 3] = [
                (
                    "krafka_producer_topic_records_sent_total",
                    "Records the broker acknowledged, per topic.",
                    |t| t.records_sent,
                ),
                (
                    "krafka_producer_topic_bytes_sent_total",
                    "Estimated bytes of the acknowledged records, per topic.",
                    |t| t.bytes_sent,
                ),
                (
                    "krafka_producer_topic_errors_total",
                    "Batches that failed with a terminal error, per topic.",
                    |t| t.errors,
                ),
            ];
            for (name, help, field) in families {
                let _ = writeln!(out, "# HELP {name} {help}");
                let _ = writeln!(out, "# TYPE {name} counter");
                for topic in &self.producer.topics {
                    let topic_labels =
                        labels(Some(format!("topic=\"{}\"", escape_label(&topic.topic))));
                    let _ = writeln!(out, "{name}{topic_labels} {}", field(topic));
                }
            }
        }
        out
    }

    /// Every counter and gauge, with its Prometheus name and its KIP-714 name.
    pub(crate) fn scalars(&self) -> Vec<Scalar> {
        let p = &self.producer;
        let c = &self.consumer;
        let n = &self.connections;
        use Kind::{Counter, Gauge};
        use Section::{Connection, Consumer, Producer};
        let s = |section, kind, prometheus, push, help, value| Scalar {
            section,
            kind,
            prometheus,
            push,
            help,
            value,
        };
        vec![
            s(
                Producer,
                Counter,
                "krafka_producer_records_sent_total",
                "record.send.total",
                "Records the broker acknowledged.",
                p.records_sent,
            ),
            s(
                Producer,
                Counter,
                "krafka_producer_bytes_sent_total",
                "record.byte.total",
                "Estimated bytes of the acknowledged records.",
                p.bytes_sent,
            ),
            s(
                Producer,
                Counter,
                "krafka_producer_batches_sent_total",
                "batch.send.total",
                "Batches the broker acknowledged.",
                p.batches_sent,
            ),
            s(
                Producer,
                Counter,
                "krafka_producer_errors_total",
                "batch.error.total",
                "Batches that failed with a terminal error.",
                p.errors,
            ),
            s(
                Producer,
                Counter,
                "krafka_producer_retries_total",
                "batch.retry.total",
                "Batch retries.",
                p.retries,
            ),
            s(
                Producer,
                Counter,
                "krafka_producer_data_loss_detected_total",
                "data.loss.detected.total",
                "Times the broker reported a batch of this producer lost.",
                p.data_loss_detected,
            ),
            s(
                Producer,
                Counter,
                "krafka_producer_compressed_bytes_total",
                "batch.compressed.byte.total",
                "Estimated encoded bytes of compressed batches.",
                p.compressed_bytes,
            ),
            s(
                Producer,
                Counter,
                "krafka_producer_uncompressed_bytes_total",
                "batch.uncompressed.byte.total",
                "Estimated bytes of the compressed batches before compression.",
                p.uncompressed_bytes,
            ),
            s(
                Producer,
                Gauge,
                "krafka_producer_buffered_records",
                "buffered.records",
                "Records currently held under buffer_memory.",
                p.buffered_records,
            ),
            s(
                Consumer,
                Counter,
                "krafka_consumer_records_received_total",
                "fetch.manager.records.consumed.total",
                "Records handed to the application.",
                c.records_received,
            ),
            s(
                Consumer,
                Counter,
                "krafka_consumer_bytes_received_total",
                "fetch.manager.bytes.consumed.total",
                "Value bytes of the records handed to the application.",
                c.bytes_received,
            ),
            s(
                Consumer,
                Counter,
                "krafka_consumer_fetches_total",
                "fetch.manager.fetch.total",
                "Fetch requests sent.",
                c.fetches,
            ),
            s(
                Consumer,
                Counter,
                "krafka_consumer_polls_total",
                "poll.total",
                "poll/recv rounds.",
                c.polls,
            ),
            s(
                Consumer,
                Counter,
                "krafka_consumer_empty_polls_total",
                "poll.empty.total",
                "Rounds that returned no record.",
                c.empty_polls,
            ),
            s(
                Consumer,
                Counter,
                "krafka_consumer_commits_total",
                "coordinator.commit.total",
                "Offset or acknowledgement commits.",
                c.commits,
            ),
            s(
                Consumer,
                Counter,
                "krafka_consumer_errors_total",
                "error.total",
                "Errors returned from poll/recv.",
                c.errors,
            ),
            s(
                Consumer,
                Counter,
                "krafka_consumer_rebalances_total",
                "coordinator.rebalance.total",
                "Assignment changes applied.",
                c.rebalances,
            ),
            s(
                Consumer,
                Counter,
                "krafka_consumer_seeks_total",
                "seek.total",
                "Partitions repositioned by a seek.",
                c.seeks,
            ),
            s(
                Consumer,
                Counter,
                "krafka_consumer_batch_decode_errors_total",
                "fetch.manager.batch.decode.error.total",
                "Record batches that failed to decode.",
                c.batch_decode_errors,
            ),
            s(
                Consumer,
                Gauge,
                "krafka_consumer_lag",
                "fetch.manager.records.lag.total",
                "Records behind the end of the log, summed over assigned partitions.",
                c.lag,
            ),
            s(
                Consumer,
                Gauge,
                "krafka_consumer_lag_max",
                "fetch.manager.records.lag.max",
                "Largest per-partition lag.",
                c.lag_max,
            ),
            s(
                Consumer,
                Gauge,
                "krafka_consumer_assigned_partitions",
                "coordinator.assigned.partitions",
                "Assigned partitions.",
                c.assigned_partitions,
            ),
            s(
                Consumer,
                Gauge,
                "krafka_consumer_paused_partitions",
                "paused.partitions",
                "Paused partitions.",
                c.paused_partitions,
            ),
            s(
                Consumer,
                Gauge,
                "krafka_consumer_buffered_records",
                "buffered.records",
                "Records fetched and not yet handed out.",
                c.buffered_records,
            ),
            s(
                Connection,
                Counter,
                "krafka_connections_created_total",
                "connection.creation.total",
                "Connections opened.",
                n.connections_created,
            ),
            s(
                Connection,
                Counter,
                "krafka_connections_closed_total",
                "connection.close.total",
                "Connections closed.",
                n.connections_closed,
            ),
            s(
                Connection,
                Counter,
                "krafka_connection_errors_total",
                "connection.error.total",
                "Connections that failed.",
                n.connection_errors,
            ),
            s(
                Connection,
                Gauge,
                "krafka_connections_active",
                "connection.count",
                "Connections open now.",
                n.active_connections,
            ),
            s(
                Connection,
                Counter,
                "krafka_throttle_delays_total",
                "throttle.delay.total",
                "Requests held back by a broker throttle.",
                n.throttle_delays,
            ),
            s(
                Connection,
                Counter,
                "krafka_throttle_delay_ms_total",
                "throttle.delay.ms.total",
                "Milliseconds requests were held back by broker throttles.",
                n.throttle_delay_ms,
            ),
            s(
                Connection,
                Counter,
                "krafka_connections_stalled_total",
                "connection.stalled.total",
                "Connections closed because a request on them timed out.",
                n.stalled_connections,
            ),
            s(
                Connection,
                Counter,
                "krafka_coordination_fallbacks_total",
                "coordination.fallback.total",
                "Coordination requests sent on the data connection at the connection cap.",
                n.coordination_fallbacks,
            ),
            s(
                Connection,
                Counter,
                "krafka_oauth_token_fetches_total",
                "oauth.token.fetch.total",
                "SASL/OAUTHBEARER token fetches attempted.",
                n.oauth_token_fetches,
            ),
            s(
                Connection,
                Counter,
                "krafka_oauth_token_fetch_failures_total",
                "oauth.token.fetch.failure.total",
                "SASL/OAUTHBEARER token fetches that failed.",
                n.oauth_token_fetch_failures,
            ),
            s(
                Connection,
                Gauge,
                "krafka_oauth_token_expiry_epoch_ms",
                "oauth.token.expiry.epoch.ms",
                "Expiry of the cached OAUTHBEARER token, ms since the Unix epoch; 0 when unknown.",
                n.oauth_token_expiry_epoch_ms,
            ),
        ]
    }

    /// Every timed operation, with its Prometheus base name and its KIP-714
    /// name.
    pub(crate) fn latencies(&self) -> Vec<Timed> {
        use Section::{Connection, Consumer, Producer};
        let t = |section, prometheus, push, help, value| Timed {
            section,
            prometheus,
            push,
            help,
            value,
        };
        vec![
            t(
                Producer,
                "krafka_producer_send_latency",
                "record.send.latency",
                "Time from a batch's first record to its acknowledgement.",
                self.producer.send_latency,
            ),
            t(
                Consumer,
                "krafka_consumer_poll_latency",
                "poll.latency",
                "Duration of poll rounds.",
                self.consumer.poll_latency,
            ),
            t(
                Consumer,
                "krafka_consumer_fetch_latency",
                "fetch.manager.fetch.latency",
                "Duration of fetch round trips.",
                self.consumer.fetch_latency,
            ),
            t(
                Connection,
                "krafka_tls_handshake_latency",
                "tls.handshake.latency",
                "TLS handshakes.",
                self.connections.tls_handshake_latency,
            ),
            t(
                Connection,
                "krafka_oauth_token_fetch_latency",
                "oauth.token.fetch.latency",
                "Successful SASL/OAUTHBEARER token fetches.",
                self.connections.oauth_token_fetch_latency,
            ),
        ]
    }
}

/// Which part of a snapshot a metric comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Section {
    Producer,
    Consumer,
    Connection,
}

/// Counter (monotonic) or gauge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Counter,
    Gauge,
}

/// One counter or gauge of a snapshot.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Scalar {
    pub(crate) section: Section,
    pub(crate) kind: Kind,
    pub(crate) prometheus: &'static str,
    /// The KIP-714 name below `<prefix>.<client type>.`.
    pub(crate) push: &'static str,
    pub(crate) help: &'static str,
    pub(crate) value: u64,
}

/// One timed operation of a snapshot.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Timed {
    pub(crate) section: Section,
    pub(crate) prometheus: &'static str,
    /// The KIP-714 name below `<prefix>.<client type>.`; `.avg` and `.max`
    /// are appended.
    pub(crate) push: &'static str,
    pub(crate) help: &'static str,
    pub(crate) value: Latency,
}

/// Escape a Prometheus label value: backslash, double quote and newline.
fn escape_label(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// The id a broker assigns a client for KIP-714 telemetry, as Java's
/// `clientInstanceId(Duration)` returns it. Operators find a client's pushed
/// metrics by it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ClientInstanceId([u8; 16]);

impl ClientInstanceId {
    pub(crate) fn new(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// The 16 bytes of the UUID.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

/// The canonical UUID form, `8-4-4-4-12` lowercase hex digits.
impl std::fmt::Display for ClientInstanceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        for (i, byte) in self.0.iter().enumerate() {
            if matches!(i, 4 | 6 | 8 | 10) {
                f.write_str("-")?;
            }
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

// ── Where a client's snapshot comes from ────────────────────────────────────

/// Everything one client's [`Metrics`] is read from. `metrics()` and the
/// KIP-714 reporter both call [`snapshot`](Self::snapshot).
pub(crate) struct MetricsSource {
    client_id: String,
    pool: Arc<ConnectionPool>,
    producer: Option<Arc<ProducerRecorder>>,
    consumer: Option<Arc<ConsumerRecorder>>,
}

impl MetricsSource {
    /// A producer's source, registered with `kafka` for
    /// [`Kafka::metrics`](crate::Kafka::metrics).
    pub(crate) fn producer(kafka: &crate::Kafka, recorder: Arc<ProducerRecorder>) -> Arc<Self> {
        Self::register(kafka, Some(recorder), None)
    }

    /// A consumer's or share consumer's source, registered with `kafka`.
    pub(crate) fn consumer(kafka: &crate::Kafka, recorder: Arc<ConsumerRecorder>) -> Arc<Self> {
        Self::register(kafka, None, Some(recorder))
    }

    /// An admin client's source: connection counters only.
    pub(crate) fn admin(kafka: &crate::Kafka) -> Arc<Self> {
        Arc::new(Self {
            client_id: kafka.client_id().to_string(),
            pool: Arc::clone(kafka.pool()),
            producer: None,
            consumer: None,
        })
    }

    fn register(
        kafka: &crate::Kafka,
        producer: Option<Arc<ProducerRecorder>>,
        consumer: Option<Arc<ConsumerRecorder>>,
    ) -> Arc<Self> {
        let source = Arc::new(Self {
            client_id: kafka.client_id().to_string(),
            pool: Arc::clone(kafka.pool()),
            producer,
            consumer,
        });
        kafka.register_metrics(&source);
        source
    }

    /// The client's `client_id`.
    pub(crate) fn client_id(&self) -> &str {
        &self.client_id
    }

    /// The client's own counters: no connections, no client id.
    pub(crate) fn client_counters(&self) -> Metrics {
        Metrics {
            client_id: None,
            producer: self
                .producer
                .as_ref()
                .map(|p| p.snapshot())
                .unwrap_or_default(),
            consumer: self
                .consumer
                .as_ref()
                .map(|c| c.snapshot())
                .unwrap_or_default(),
            connections: ConnectionMetrics::default(),
        }
    }

    /// The client's [`Metrics`].
    pub(crate) fn snapshot(&self) -> Metrics {
        let mut metrics = self.client_counters();
        metrics.client_id = Some(self.client_id.clone());
        metrics.connections = self.pool.metrics();
        metrics
    }
}

impl std::fmt::Debug for MetricsSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsSource")
            .field("client_id", &self.client_id)
            .finish_non_exhaustive()
    }
}

// ── Recorders: what the clients write ───────────────────────────────────────

/// A monotonic counter.
#[derive(Debug, Default)]
pub(crate) struct Counter(AtomicU64);

impl Counter {
    #[inline]
    pub(crate) fn inc(&self) {
        self.add(1);
    }

    #[inline]
    pub(crate) fn add(&self, n: u64) {
        self.0.fetch_add(n, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }
}

/// A value that goes up and down; `dec` saturates at zero.
#[derive(Debug, Default)]
pub(crate) struct Gauge(AtomicU64);

impl Gauge {
    #[inline]
    pub(crate) fn set(&self, value: u64) {
        self.0.store(value, Ordering::Relaxed);
    }

    #[inline]
    pub(crate) fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    #[inline]
    pub(crate) fn inc(&self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement, never below zero. Reaching zero early is a krafka bug: an
    /// unmatched `inc`/`dec` pair, logged at `warn`.
    #[inline]
    pub(crate) fn dec(&self) {
        let result = self
            .0
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |v| v.checked_sub(1));
        if result.is_err() {
            tracing::warn!("krafka bug: a gauge was decremented below zero");
        }
    }
}

/// Count, total and maximum of a timed operation.
#[derive(Debug, Default)]
pub(crate) struct LatencyRecorder {
    count: AtomicU64,
    sum_nanos: AtomicU64,
    max_nanos: AtomicU64,
}

impl LatencyRecorder {
    #[inline]
    pub(crate) fn record(&self, duration: Duration) {
        let nanos = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_nanos.fetch_add(nanos, Ordering::Relaxed);
        self.max_nanos.fetch_max(nanos, Ordering::Relaxed);
    }

    /// Time from now until the returned guard is dropped.
    #[inline]
    pub(crate) fn start(&self) -> LatencyTimer<'_> {
        LatencyTimer {
            recorder: self,
            started: Instant::now(),
        }
    }

    pub(crate) fn snapshot(&self) -> Latency {
        Latency {
            count: self.count.load(Ordering::Relaxed),
            sum: Duration::from_nanos(self.sum_nanos.load(Ordering::Relaxed)),
            max: Duration::from_nanos(self.max_nanos.load(Ordering::Relaxed)),
        }
    }
}

/// Records the time since [`LatencyRecorder::start`] when dropped.
pub(crate) struct LatencyTimer<'a> {
    recorder: &'a LatencyRecorder,
    started: Instant,
}

impl Drop for LatencyTimer<'_> {
    fn drop(&mut self) {
        self.recorder.record(self.started.elapsed());
    }
}

/// Per-topic producer counters.
#[derive(Debug, Default)]
struct TopicRecorder {
    records_sent: Counter,
    bytes_sent: Counter,
    errors: Counter,
}

/// What a producer writes.
///
/// Per-topic counters sit in a lock-free [`arc_swap::ArcSwap`] map: the hot
/// path is a `load()` and a hash lookup. Only a new topic takes the write
/// mutex and publishes a copy-on-write map. Topics beyond the cap fold into
/// [`OVERFLOW_TOPIC_KEY`].
#[derive(Debug)]
pub(crate) struct ProducerRecorder {
    pub(crate) records_sent: Counter,
    pub(crate) bytes_sent: Counter,
    pub(crate) batches_sent: Counter,
    pub(crate) errors: Counter,
    pub(crate) retries: Counter,
    pub(crate) data_loss_detected: Counter,
    pub(crate) compressed_bytes: Counter,
    pub(crate) uncompressed_bytes: Counter,
    pub(crate) buffered_records: Gauge,
    pub(crate) send_latency: LatencyRecorder,
    topics: arc_swap::ArcSwap<AHashMap<String, Arc<TopicRecorder>>>,
    topic_write_lock: parking_lot::Mutex<()>,
    max_tracked_topics: usize,
}

impl Default for ProducerRecorder {
    fn default() -> Self {
        Self::with_max_tracked_topics(DEFAULT_MAX_TRACKED_TOPICS)
    }
}

impl ProducerRecorder {
    pub(crate) fn with_max_tracked_topics(max_tracked_topics: usize) -> Self {
        Self {
            records_sent: Counter::default(),
            bytes_sent: Counter::default(),
            batches_sent: Counter::default(),
            errors: Counter::default(),
            retries: Counter::default(),
            data_loss_detected: Counter::default(),
            compressed_bytes: Counter::default(),
            uncompressed_bytes: Counter::default(),
            buffered_records: Gauge::default(),
            send_latency: LatencyRecorder::default(),
            topics: arc_swap::ArcSwap::from_pointee(AHashMap::new()),
            topic_write_lock: parking_lot::Mutex::new(()),
            max_tracked_topics,
        }
    }

    #[inline]
    fn with_topic(&self, topic: &str, f: impl FnOnce(&TopicRecorder)) {
        let map = self.topics.load();
        if let Some(m) = map.get(topic) {
            f(m);
            return;
        }
        if Self::named_topic_count(&map) >= self.max_tracked_topics
            && let Some(m) = map.get(OVERFLOW_TOPIC_KEY)
        {
            f(m);
            return;
        }
        drop(map);
        f(&self.register_topic(topic));
    }

    #[inline]
    fn named_topic_count(map: &AHashMap<String, Arc<TopicRecorder>>) -> usize {
        map.len() - usize::from(map.contains_key(OVERFLOW_TOPIC_KEY))
    }

    /// Register `topic` (or resolve the overflow bucket) under the write lock,
    /// re-checking the map so two threads racing on one new topic share it.
    #[cold]
    fn register_topic(&self, topic: &str) -> Arc<TopicRecorder> {
        let _write = self.topic_write_lock.lock();
        let current = self.topics.load();
        if let Some(m) = current.get(topic) {
            return Arc::clone(m);
        }
        let key = if Self::named_topic_count(&current) >= self.max_tracked_topics {
            OVERFLOW_TOPIC_KEY
        } else {
            topic
        };
        if let Some(m) = current.get(key) {
            return Arc::clone(m);
        }
        let entry = Arc::new(TopicRecorder::default());
        let mut next = (**current).clone();
        next.insert(key.to_string(), Arc::clone(&entry));
        self.topics.store(Arc::new(next));
        entry
    }

    /// An acknowledged batch of `records` records and `bytes` bytes.
    #[inline]
    pub(crate) fn record_batch_for_topic(&self, topic: &str, records: u64, bytes: u64) {
        self.batches_sent.inc();
        self.records_sent.add(records);
        self.bytes_sent.add(bytes);
        self.with_topic(topic, |m| {
            m.records_sent.add(records);
            m.bytes_sent.add(bytes);
        });
    }

    /// A batch that failed with a terminal error.
    #[inline]
    pub(crate) fn record_error_for_topic(&self, topic: &str) {
        self.errors.inc();
        self.with_topic(topic, |m| m.errors.inc());
    }

    #[inline]
    pub(crate) fn record_retry(&self) {
        self.retries.inc();
    }

    /// Estimated bytes before and after compression of one compressed batch.
    #[inline]
    pub(crate) fn record_compression(&self, compressed: u64, uncompressed: u64) {
        self.compressed_bytes.add(compressed);
        self.uncompressed_bytes.add(uncompressed);
    }

    pub(crate) fn snapshot(&self) -> ProducerMetrics {
        let mut topics: Vec<TopicMetrics> = self
            .topics
            .load()
            .iter()
            .map(|(topic, m)| TopicMetrics {
                topic: topic.clone(),
                records_sent: m.records_sent.get(),
                bytes_sent: m.bytes_sent.get(),
                errors: m.errors.get(),
            })
            .collect();
        topics.sort_unstable_by(|a, b| a.topic.cmp(&b.topic));
        ProducerMetrics {
            records_sent: self.records_sent.get(),
            bytes_sent: self.bytes_sent.get(),
            batches_sent: self.batches_sent.get(),
            errors: self.errors.get(),
            retries: self.retries.get(),
            data_loss_detected: self.data_loss_detected.get(),
            compressed_bytes: self.compressed_bytes.get(),
            uncompressed_bytes: self.uncompressed_bytes.get(),
            buffered_records: self.buffered_records.get(),
            send_latency: self.send_latency.snapshot(),
            topics,
        }
    }
}

/// What a consumer or share consumer writes.
#[derive(Debug, Default)]
pub(crate) struct ConsumerRecorder {
    pub(crate) records_received: Counter,
    pub(crate) bytes_received: Counter,
    pub(crate) fetches: Counter,
    pub(crate) polls: Counter,
    pub(crate) empty_polls: Counter,
    pub(crate) commits: Counter,
    pub(crate) errors: Counter,
    pub(crate) rebalances: Counter,
    pub(crate) seeks: Counter,
    pub(crate) batch_decode_errors: Counter,
    pub(crate) lag: Gauge,
    pub(crate) lag_max: Gauge,
    pub(crate) assigned_partitions: Gauge,
    pub(crate) paused_partitions: Gauge,
    pub(crate) buffered_records: Gauge,
    pub(crate) poll_latency: LatencyRecorder,
    pub(crate) fetch_latency: LatencyRecorder,
}

impl ConsumerRecorder {
    /// `n` partitions repositioned.
    #[inline]
    pub(crate) fn record_seek(&self, n: u64) {
        self.seeks.add(n);
    }

    /// Records handed to the application.
    #[inline]
    pub(crate) fn record_receive(&self, records: u64, bytes: u64) {
        self.records_received.add(records);
        self.bytes_received.add(bytes);
    }

    #[inline]
    pub(crate) fn record_fetch(&self) {
        self.fetches.inc();
    }

    #[inline]
    pub(crate) fn record_batch_decode_error(&self) {
        self.batch_decode_errors.inc();
    }

    #[inline]
    pub(crate) fn record_commit(&self) {
        self.commits.inc();
    }

    #[inline]
    pub(crate) fn record_error(&self) {
        self.errors.inc();
    }

    pub(crate) fn snapshot(&self) -> ConsumerMetrics {
        ConsumerMetrics {
            records_received: self.records_received.get(),
            bytes_received: self.bytes_received.get(),
            fetches: self.fetches.get(),
            polls: self.polls.get(),
            empty_polls: self.empty_polls.get(),
            commits: self.commits.get(),
            errors: self.errors.get(),
            rebalances: self.rebalances.get(),
            seeks: self.seeks.get(),
            batch_decode_errors: self.batch_decode_errors.get(),
            lag: self.lag.get(),
            lag_max: self.lag_max.get(),
            assigned_partitions: self.assigned_partitions.get(),
            paused_partitions: self.paused_partitions.get(),
            buffered_records: self.buffered_records.get(),
            poll_latency: self.poll_latency.snapshot(),
            fetch_latency: self.fetch_latency.snapshot(),
        }
    }
}

/// What a connection pool writes.
#[derive(Debug, Default)]
pub(crate) struct ConnectionRecorder {
    pub(crate) connections_created: Counter,
    pub(crate) connections_closed: Counter,
    pub(crate) connection_errors: Counter,
    pub(crate) active_connections: Gauge,
    pub(crate) throttle_delays: Counter,
    pub(crate) throttle_delay_ms: Counter,
    pub(crate) stalled_connections: Counter,
    pub(crate) coordination_fallbacks: Counter,
    pub(crate) tls_handshake_latency: LatencyRecorder,
    pub(crate) oauth_token_fetches: Counter,
    pub(crate) oauth_token_fetch_failures: Counter,
    pub(crate) oauth_token_fetch_latency: LatencyRecorder,
    pub(crate) oauth_token_expiry_epoch_ms: Gauge,
}

impl ConnectionRecorder {
    #[inline]
    pub(crate) fn record_connect(&self) {
        self.connections_created.inc();
        self.active_connections.inc();
    }

    #[inline]
    pub(crate) fn record_close(&self) {
        self.connections_closed.inc();
        self.active_connections.dec();
    }

    #[inline]
    pub(crate) fn record_error(&self) {
        self.connection_errors.inc();
    }

    #[inline]
    pub(crate) fn record_throttle_delay(&self, delay: Duration) {
        self.throttle_delays.inc();
        self.throttle_delay_ms
            .add(u64::try_from(delay.as_millis()).unwrap_or(u64::MAX));
    }

    #[inline]
    pub(crate) fn record_stalled_connection(&self) {
        self.stalled_connections.inc();
    }

    #[inline]
    pub(crate) fn record_coordination_fallback(&self) {
        self.coordination_fallbacks.inc();
    }

    #[inline]
    pub(crate) fn record_tls_handshake(&self, duration: Duration) {
        self.tls_handshake_latency.record(duration);
    }

    /// A successful SASL/OAUTHBEARER token fetch; `expiry_epoch_ms` is `None`
    /// when the identity provider returned no `expires_in`.
    #[inline]
    pub(crate) fn record_oauth_token_fetch(
        &self,
        duration: Duration,
        expiry_epoch_ms: Option<i64>,
    ) {
        self.oauth_token_fetches.inc();
        self.oauth_token_fetch_latency.record(duration);
        self.oauth_token_expiry_epoch_ms
            .set(expiry_epoch_ms.unwrap_or(0).max(0) as u64);
    }

    /// A failed token fetch. The cached expiry stays: the previous token may
    /// still be valid.
    #[inline]
    pub(crate) fn record_oauth_token_fetch_failure(&self) {
        self.oauth_token_fetches.inc();
        self.oauth_token_fetch_failures.inc();
    }

    pub(crate) fn snapshot(&self) -> ConnectionMetrics {
        ConnectionMetrics {
            connections_created: self.connections_created.get(),
            connections_closed: self.connections_closed.get(),
            connection_errors: self.connection_errors.get(),
            active_connections: self.active_connections.get(),
            throttle_delays: self.throttle_delays.get(),
            throttle_delay_ms: self.throttle_delay_ms.get(),
            stalled_connections: self.stalled_connections.get(),
            coordination_fallbacks: self.coordination_fallbacks.get(),
            tls_handshake_latency: self.tls_handshake_latency.snapshot(),
            oauth_token_fetches: self.oauth_token_fetches.get(),
            oauth_token_fetch_failures: self.oauth_token_fetch_failures.get(),
            oauth_token_fetch_latency: self.oauth_token_fetch_latency.snapshot(),
            oauth_token_expiry_epoch_ms: self.oauth_token_expiry_epoch_ms.get(),
        }
    }
}

#[cfg(all(test, feature = "test-broker"))]
mod broker_tests;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn producer_snapshot(records: u64, client_id: Option<&str>) -> Metrics {
        let recorder = ProducerRecorder::default();
        recorder.record_batch_for_topic("orders", records, records * 10);
        Metrics {
            client_id: client_id.map(str::to_string),
            producer: recorder.snapshot(),
            ..Metrics::default()
        }
    }

    /// The value of the unlabelled-or-client-labelled series `name`.
    fn series(text: &str, name: &str) -> Option<String> {
        text.lines()
            .filter(|l| !l.starts_with('#'))
            .find(|l| {
                l.strip_prefix(name)
                    .is_some_and(|rest| rest.starts_with(' ') || rest.starts_with('{'))
            })
            .map(|l| l.rsplit(' ').next().unwrap_or_default().to_string())
    }

    #[test]
    fn gauge_dec_saturates_at_zero() {
        let gauge = Gauge::default();
        gauge.dec();
        assert_eq!(gauge.get(), 0);
        gauge.inc();
        gauge.inc();
        gauge.dec();
        assert_eq!(gauge.get(), 1);
    }

    #[test]
    fn latency_keeps_count_sum_and_max() {
        let recorder = LatencyRecorder::default();
        recorder.record(Duration::from_millis(10));
        recorder.record(Duration::from_millis(30));
        let latency = recorder.snapshot();
        assert_eq!(latency.count, 2);
        assert_eq!(latency.sum, Duration::from_millis(40));
        assert_eq!(latency.max, Duration::from_millis(30));
        assert_eq!(latency.mean(), Some(Duration::from_millis(20)));
        assert_eq!(Latency::default().mean(), None);
    }

    #[test]
    fn a_snapshot_is_owned() {
        let recorder = ProducerRecorder::default();
        recorder.record_batch_for_topic("t", 5, 50);
        let before = recorder.snapshot();
        recorder.record_batch_for_topic("t", 100, 1000);
        let after = recorder.snapshot();
        assert_eq!(before.records_sent, 5);
        assert_eq!(after.records_sent - before.records_sent, 100);
    }

    #[test]
    fn prometheus_text_renders_the_snapshot_under_the_client_label() {
        let metrics = producer_snapshot(100, Some("orders-service"));
        let text = metrics.prometheus_text();
        assert!(
            text.contains("krafka_producer_records_sent_total{client_id=\"orders-service\"} 100"),
            "{text}"
        );
        assert!(text.contains(
            "krafka_producer_topic_records_sent_total{client_id=\"orders-service\",topic=\"orders\"} 100"
        ));
        assert!(text.contains("# TYPE krafka_producer_records_sent_total counter"));
        assert!(text.contains("# TYPE krafka_producer_send_latency_seconds summary"));
    }

    #[test]
    fn two_clients_render_distinct_series() {
        let a = producer_snapshot(1, Some("a")).prometheus_text();
        let b = producer_snapshot(1, Some("b")).prometheus_text();
        let sample = |text: &str| -> Vec<String> {
            text.lines()
                .filter(|l| !l.starts_with('#'))
                .map(|l| l.rsplit_once(' ').map(|(k, _)| k.to_string()).unwrap())
                .collect()
        };
        let a_keys = sample(&a);
        assert!(sample(&b).iter().all(|k| !a_keys.contains(k)));
    }

    #[test]
    fn the_sum_has_no_client_label() {
        let mut total = Metrics::default();
        total.add_client(&producer_snapshot(3, Some("a")));
        total.add_client(&producer_snapshot(4, Some("b")));
        let text = total.prometheus_text();
        assert_eq!(
            series(&text, "krafka_producer_records_sent_total").as_deref(),
            Some("7")
        );
        assert!(!text.contains("client_id"));
        assert_eq!(total.producer.topics.len(), 1);
        assert_eq!(total.producer.topics[0].records_sent, 7);
    }

    #[test]
    fn a_client_instance_id_displays_as_a_uuid() {
        let id = ClientInstanceId::new(
            *b"\x01\x23\x45\x67\x89\xab\xcd\xef\x01\x23\x45\x67\x89\xab\xcd\xef",
        );
        assert_eq!(id.to_string(), "01234567-89ab-cdef-0123-456789abcdef");
    }

    #[test]
    fn label_values_are_escaped() {
        assert_eq!(escape_label("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }

    #[test]
    fn every_name_is_unique_and_well_formed() {
        let metrics = Metrics::default();
        let mut prometheus: Vec<&str> = metrics.scalars().iter().map(|m| m.prometheus).collect();
        prometheus.extend(metrics.latencies().iter().map(|m| m.prometheus));
        let mut push: Vec<(Section, &str)> = metrics
            .scalars()
            .iter()
            .map(|m| (m.section, m.push))
            .collect();
        push.extend(metrics.latencies().iter().map(|m| (m.section, m.push)));
        for name in &prometheus {
            assert!(name.starts_with("krafka_"), "{name}");
            assert!(name.chars().all(|c| c.is_ascii_lowercase() || c == '_'));
        }
        for (_, name) in &push {
            assert!(
                name.split('.')
                    .all(|seg| !seg.is_empty() && seg.chars().all(|c| c.is_ascii_lowercase())),
                "{name}"
            );
        }
        let count = prometheus.len();
        prometheus.sort_unstable();
        prometheus.dedup();
        assert_eq!(prometheus.len(), count);
        let count = push.len();
        push.sort_unstable_by_key(|(s, n)| (*s as u8, *n));
        push.dedup();
        assert_eq!(push.len(), count);
    }

    #[test]
    fn topics_beyond_the_cap_fold_into_the_overflow_bucket() {
        let recorder = ProducerRecorder::with_max_tracked_topics(3);
        for i in 0..6 {
            recorder.record_batch_for_topic(&format!("topic-{i}"), 1, 10);
        }
        recorder.record_batch_for_topic("topic-0", 1, 10);
        let snapshot = recorder.snapshot();
        let topics: Vec<&str> = snapshot.topics.iter().map(|t| t.topic.as_str()).collect();
        assert_eq!(
            topics,
            vec![OVERFLOW_TOPIC_KEY, "topic-0", "topic-1", "topic-2"]
        );
        let other = &snapshot.topics[0];
        assert_eq!(other.records_sent, 3);
        assert_eq!(snapshot.topics[1].records_sent, 2);
        assert_eq!(snapshot.records_sent, 7);
    }

    #[test]
    fn concurrent_registration_of_one_topic_shares_its_counters() {
        let recorder = Arc::new(ProducerRecorder::default());
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let recorder = Arc::clone(&recorder);
                std::thread::spawn(move || {
                    for _ in 0..1000 {
                        recorder.record_batch_for_topic("hot", 1, 1);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().unwrap();
        }
        let snapshot = recorder.snapshot();
        assert_eq!(snapshot.topics.len(), 1);
        assert_eq!(snapshot.topics[0].records_sent, snapshot.records_sent);
        assert_eq!(snapshot.records_sent, 8000);
    }

    #[test]
    fn oauth_expiry_is_unknown_without_an_expiry() {
        let recorder = ConnectionRecorder::default();
        recorder.record_oauth_token_fetch(Duration::from_millis(5), Some(1_700_000_000_000));
        recorder.record_oauth_token_fetch_failure();
        let snapshot = recorder.snapshot();
        assert_eq!(snapshot.oauth_token_fetches, 2);
        assert_eq!(snapshot.oauth_token_fetch_failures, 1);
        assert_eq!(snapshot.oauth_token_expiry_epoch_ms, 1_700_000_000_000);
        recorder.record_oauth_token_fetch(Duration::from_millis(5), None);
        assert_eq!(recorder.snapshot().oauth_token_expiry_epoch_ms, 0);
    }
}
