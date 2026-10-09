//! End to end: the spans a recording `tracing` layer sees, and
//! that a subscriber not interested in krafka gets none constructed.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing_subscriber::layer::{Context, SubscriberExt};

use crate::Kafka;
use crate::consumer::{AutoOffsetReset, Consumer, GroupProtocol};
use crate::error::ErrorCode;
use crate::testing::{ApiKey, Control, FakeBroker};

/// A recorded span: its name, its fields as strings, whether it closed.
#[derive(Debug, Clone, Default)]
struct Recorded {
    name: &'static str,
    target: String,
    fields: HashMap<String, String>,
    closed: bool,
}

#[derive(Clone, Default)]
struct Recorder(Arc<parking_lot::Mutex<HashMap<u64, Recorded>>>);

struct Fields<'a>(&'a mut HashMap<String, String>);

impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
}

impl<S> tracing_subscriber::Layer<S> for Recorder
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, _: Context<'_, S>) {
        let mut span = Recorded {
            name: attrs.metadata().name(),
            target: attrs.metadata().target().to_string(),
            ..Recorded::default()
        };
        attrs.record(&mut Fields(&mut span.fields));
        self.0.lock().insert(id.into_u64(), span);
    }

    fn on_record(&self, id: &Id, values: &Record<'_>, _: Context<'_, S>) {
        if let Some(span) = self.0.lock().get_mut(&id.into_u64()) {
            values.record(&mut Fields(&mut span.fields));
        }
    }

    fn on_close(&self, id: Id, _: Context<'_, S>) {
        if let Some(span) = self.0.lock().get_mut(&id.into_u64()) {
            span.closed = true;
        }
    }
}

impl Recorder {
    fn named(&self, name: &str) -> Vec<Recorded> {
        self.0
            .lock()
            .values()
            .filter(|s| s.name == name && s.target.starts_with("krafka"))
            .cloned()
            .collect()
    }

    fn all(&self) -> Vec<Recorded> {
        self.0.lock().values().cloned().collect()
    }
}

fn record() -> (Recorder, tracing::subscriber::DefaultGuard) {
    let recorder = Recorder::default();
    let guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(recorder.clone()));
    (recorder, guard)
}

async fn connect(broker: &FakeBroker) -> Kafka {
    Kafka::builder(broker.bootstrap_servers())
        .client_id("svc")
        .request_timeout(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(1))
        .connect()
        .await
        .unwrap()
}

async fn poll_some(consumer: &Consumer) -> usize {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        let n = consumer
            .poll(Duration::from_millis(200))
            .await
            .unwrap()
            .len();
        if n > 0 {
            return n;
        }
    }
    panic!("no record arrived");
}

fn field<'a>(span: &'a Recorded, name: &str) -> Option<&'a str> {
    span.fields.get(name).map(String::as_str)
}

/// Names semantic conventions v1.44.0 deprecated, and what replaced the key.
const DEPRECATED: &[&str] = &[
    "messaging.operation",
    "messaging.kafka.destination.partition",
    "messaging.kafka.message.offset",
    "messaging.kafka.consumer.group",
    "messaging.client_id",
    "error.message",
    "krafka.message.key.sha256",
];

/// No deprecated name, no key bytes, no key hash, no un-namespaced key.
fn assert_clean(spans: &[Recorded], key: &str) {
    use sha2::Digest;
    let hash: String = sha2::Sha256::digest(key.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    for span in spans.iter().filter(|s| s.target.starts_with("krafka")) {
        for (name, value) in &span.fields {
            assert!(!DEPRECATED.contains(&name.as_str()), "{name} on {span:?}");
            assert!(
                ["otel.", "messaging.", "error.", "krafka."]
                    .iter()
                    .any(|ns| name.starts_with(ns)),
                "un-namespaced {name} on {span:?}"
            );
            assert!(!value.contains(key), "key bytes in {name} on {span:?}");
            assert!(!value.contains(&hash), "key hash in {name} on {span:?}");
        }
    }
}

/// One `send` span per record, kind producer, v1.44.0
/// attributes, ended at the acknowledgement; no key, no hash.
///
/// Reverted-line controls: without the span in `SendObligation::on_send`
/// no `send orders` span exists; with `messaging.operation` added
/// or the key hashed onto it `assert_clean` fails.
#[tokio::test]
async fn a_send_records_one_producer_span_per_record() {
    let (recorder, _guard) = record();
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);
    let producer = connect(&broker).await.producer().build().await.unwrap();
    let key = "customer-42-secret-key";
    let metadata = producer
        .send(crate::Record::new("orders", "v").key(key))
        .await
        .unwrap();
    let _ = producer
        .send(crate::Record::tombstone("orders", "k2"))
        .await
        .unwrap();

    let spans = recorder.named("send");
    assert_eq!(spans.len(), 2, "{spans:?}");
    let span = spans
        .iter()
        .find(|s| field(s, "krafka.message.key.size") == Some(&key.len().to_string()))
        .expect("the keyed record's span");
    assert_eq!(field(span, "otel.name"), Some("send orders"));
    assert_eq!(field(span, "otel.kind"), Some("producer"));
    assert_eq!(field(span, "messaging.system"), Some("kafka"));
    assert_eq!(field(span, "messaging.operation.name"), Some("send"));
    assert_eq!(field(span, "messaging.operation.type"), Some("send"));
    assert_eq!(field(span, "messaging.destination.name"), Some("orders"));
    assert_eq!(field(span, "messaging.destination.partition.id"), Some("0"));
    assert_eq!(
        field(span, "messaging.kafka.offset"),
        Some(metadata.offset.to_string().as_str())
    );
    assert_eq!(field(span, "messaging.client.id"), Some("svc"));
    assert_eq!(field(span, "error.type"), None);
    assert_eq!(field(span, "messaging.kafka.message.tombstone"), None);
    assert!(span.closed, "the span ends at the acknowledgement");
    let tombstone = spans.iter().find(|s| !std::ptr::eq(*s, span)).unwrap();
    assert_eq!(
        field(tombstone, "messaging.kafka.message.tombstone"),
        Some("true")
    );
    assert_clean(&recorder.all(), key);
    producer.close().await.unwrap();
}

/// A failed send types its error and sets the error status.
#[tokio::test]
async fn a_failed_send_carries_the_kafka_error_name() {
    let (recorder, _guard) = record();
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);
    let producer = connect(&broker).await.producer().build().await.unwrap();
    broker.on(ApiKey::Produce, |_| {
        Control::Error(ErrorCode::InvalidRecord)
    });
    let _ = producer
        .send(crate::Record::new("orders", "v"))
        .await
        .expect_err("the broker refuses the record");

    let spans = recorder.named("send");
    assert_eq!(spans.len(), 1);
    assert_eq!(field(&spans[0], "error.type"), Some("INVALID_RECORD"));
    assert_eq!(field(&spans[0], "otel.status_code"), Some("error"));
    assert!(spans[0].closed);
    broker.clear_hooks();
    producer.close().await.unwrap();
}

/// On the classic protocol: `poll` spans with the
/// batch size, a `commit` span, and a `rebalance` span.
#[tokio::test]
async fn poll_commit_and_rebalance_spans_on_the_classic_protocol() {
    poll_commit_and_rebalance(GroupProtocol::Classic).await;
}

/// The same on the KIP-848 protocol.
#[tokio::test]
async fn poll_commit_and_rebalance_spans_on_the_kip848_protocol() {
    poll_commit_and_rebalance(GroupProtocol::Consumer).await;
}

async fn poll_commit_and_rebalance(protocol: GroupProtocol) {
    let (recorder, _guard) = record();
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 2);
    broker.create_topic("audit", 1);
    let kafka = connect(&broker).await;
    let producer = kafka.producer().build().await.unwrap();
    let _ = producer
        .send(crate::Record::new("orders", "v"))
        .await
        .unwrap();
    let _ = producer
        .send(crate::Record::new("audit", "v"))
        .await
        .unwrap();

    let consumer = kafka
        .consumer("g")
        .group_protocol(protocol)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    consumer.subscribe(["orders"]).await.unwrap();
    let returned = poll_some(&consumer).await;
    consumer.commit().await.unwrap();

    let polls = recorder.named("poll");
    let span = polls
        .iter()
        .find(|s| field(s, "messaging.batch.message_count") == Some(&returned.to_string()))
        .expect("the poll that returned records");
    assert_eq!(field(span, "otel.name"), Some("poll orders"));
    assert_eq!(field(span, "otel.kind"), Some("client"));
    assert_eq!(field(span, "messaging.operation.name"), Some("poll"));
    assert_eq!(field(span, "messaging.operation.type"), Some("receive"));
    assert_eq!(field(span, "messaging.destination.name"), Some("orders"));
    assert_eq!(field(span, "messaging.consumer.group.name"), Some("g"));
    assert_eq!(field(span, "messaging.client.id"), Some("svc"));
    assert!(span.closed);

    let commits = recorder.named("commit");
    assert_eq!(commits.len(), 1, "{commits:?}");
    assert_eq!(
        field(&commits[0], "messaging.operation.type"),
        Some("settle")
    );
    assert_eq!(field(&commits[0], "otel.kind"), Some("client"));
    assert_eq!(field(&commits[0], "otel.name"), Some("commit orders"));
    assert_eq!(
        field(&commits[0], "messaging.consumer.group.name"),
        Some("g")
    );
    assert_eq!(field(&commits[0], "error.type"), None);

    let rebalances = recorder.named("rebalance");
    assert!(!rebalances.is_empty());
    let rebalance = &rebalances[0];
    assert_eq!(field(rebalance, "otel.kind"), Some("internal"));
    assert_eq!(field(rebalance, "messaging.consumer.group.name"), Some("g"));
    assert_eq!(field(rebalance, "krafka.rebalance.partitions"), Some("2"));
    assert!(field(rebalance, "krafka.rebalance.generation").is_some());

    // Two topics: the poll span names no destination.
    consumer.subscribe(["orders", "audit"]).await.unwrap();
    let _ = consumer.poll(Duration::from_millis(200)).await.unwrap();
    let last = recorder
        .named("poll")
        .into_iter()
        .filter(|s| field(s, "otel.name") == Some("poll"))
        .count();
    assert!(last >= 1, "a two-topic poll is named `poll`");
    assert!(
        recorder
            .named("poll")
            .iter()
            .filter(|s| field(s, "otel.name") == Some("poll"))
            .all(|s| field(s, "messaging.destination.name").is_none())
    );

    assert_clean(&recorder.all(), "no-key-was-sent");
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
}

/// A subscriber that disables every krafka target and counts the spans it
/// is asked to construct anyway.
struct Disinterested {
    krafka_spans: Arc<AtomicUsize>,
    other_spans: Arc<AtomicUsize>,
    next: AtomicU64,
    want_krafka: bool,
}

impl tracing::Subscriber for Disinterested {
    fn register_callsite(
        &self,
        _: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        tracing::subscriber::Interest::sometimes()
    }
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        self.want_krafka || !metadata.target().starts_with("krafka")
    }
    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        if attrs.metadata().target().starts_with("krafka") {
            self.krafka_spans.fetch_add(1, Ordering::SeqCst);
        } else {
            self.other_spans.fetch_add(1, Ordering::SeqCst);
        }
        Id::from_u64(self.next.fetch_add(1, Ordering::SeqCst) + 1)
    }
    fn record(&self, _: &Id, _: &Record<'_>) {}
    fn record_follows_from(&self, _: &Id, _: &Id) {}
    fn event(&self, _: &tracing::Event<'_>) {}
    fn enter(&self, _: &Id) {}
    fn exit(&self, _: &Id) {}
}

async fn spans_constructed(want_krafka: bool) -> usize {
    let krafka_spans = Arc::new(AtomicUsize::new(0));
    let _guard = tracing::subscriber::set_default(Disinterested {
        krafka_spans: Arc::clone(&krafka_spans),
        other_spans: Arc::default(),
        next: AtomicU64::new(0),
        want_krafka,
    });
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);
    let kafka = connect(&broker).await;
    let producer = kafka.producer().build().await.unwrap();
    for _ in 0..10 {
        let _ = producer
            .send(crate::Record::new("orders", "v"))
            .await
            .unwrap();
    }
    let consumer = kafka
        .consumer("g")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    consumer.subscribe(["orders"]).await.unwrap();
    poll_some(&consumer).await;
    consumer.commit().await.unwrap();
    consumer.close().await.unwrap();
    producer.close().await.unwrap();
    krafka_spans.load(Ordering::SeqCst)
}

/// A subscriber that filters krafka out gets no krafka span
/// constructed; the same run with krafka enabled constructs them (the
/// positive control that the counter sees spans at all).
///
/// Reverted-line control: building the send span with
/// `Span::new` ahead of the callsite's interest check fails the first
/// assertion.
#[tokio::test]
async fn a_subscriber_without_interest_constructs_no_span() {
    assert_eq!(spans_constructed(false).await, 0);
    assert!(spans_constructed(true).await >= 13);
}
