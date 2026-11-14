//! What a produced record costs besides its bytes: the `send` span only for a
//! subscriber that asked for `DEBUG`; buffer memory, flush/close accounting
//! and the buffered gauge released once per batch, after every caller of the
//! batch was answered, on every path a batch can end; and a drop warning
//! only when records are at risk.

#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use krafka::error::{ErrorCode, KrafkaError};
use krafka::interceptor::{InterceptorResult, ProducerInterceptor, RecordContext};
use krafka::producer::{Producer, Record, RecordMetadata};
use krafka::testing::{ApiKey, Control, FakeBroker};
use krafka::{Headers, PartitionId};
use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record as SpanRecord};
use tracing::{Event, Level, Subscriber};
use tracing_subscriber::layer::{Context, SubscriberExt};
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::{EnvFilter, Layer};

// ── A capturing subscriber ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
struct SpanSeen {
    name: &'static str,
    fields: HashMap<String, String>,
}

#[derive(Debug, Clone)]
struct EventSeen {
    level: Level,
    target: String,
    message: String,
}

#[derive(Default)]
struct Captured {
    spans: Vec<SpanSeen>,
    by_id: HashMap<u64, usize>,
    events: Vec<EventSeen>,
}

#[derive(Clone, Default)]
struct Capture(Arc<Mutex<Captured>>);

struct Fields<'a>(&'a mut HashMap<String, String>);

impl Visit for Fields<'_> {
    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_owned(), value.to_owned());
    }
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0.insert(field.name().to_owned(), format!("{value:?}"));
    }
}

impl<S: Subscriber + for<'a> LookupSpan<'a>> Layer<S> for Capture {
    fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, _ctx: Context<'_, S>) {
        let mut fields = HashMap::new();
        attrs.record(&mut Fields(&mut fields));
        let mut c = self.0.lock().unwrap();
        let index = c.spans.len();
        c.spans.push(SpanSeen {
            name: attrs.metadata().name(),
            fields,
        });
        c.by_id.insert(id.into_u64(), index);
    }

    fn on_record(&self, id: &Id, values: &SpanRecord<'_>, _ctx: Context<'_, S>) {
        let mut c = self.0.lock().unwrap();
        if let Some(&index) = c.by_id.get(&id.into_u64()) {
            values.record(&mut Fields(&mut c.spans[index].fields));
        }
    }

    fn on_event(&self, event: &Event<'_>, _ctx: Context<'_, S>) {
        let mut fields = HashMap::new();
        event.record(&mut Fields(&mut fields));
        self.0.lock().unwrap().events.push(EventSeen {
            level: *event.metadata().level(),
            target: event.metadata().target().to_owned(),
            message: fields.remove("message").unwrap_or_default(),
        });
    }
}

impl Capture {
    /// Install on this thread, behind `filter` as a per-layer filter.
    ///
    /// With one live dispatcher, `tracing-core` caches a new callsite's
    /// interest from the default of whichever thread hits it first — in
    /// parallel tests often a thread without a subscriber, which caches it
    /// `never` for this one too. A second dispatcher kept for the process
    /// keeps every registered subscriber consulted.
    fn install(filter: &str) -> (Self, tracing::subscriber::DefaultGuard) {
        static SECOND: std::sync::OnceLock<tracing::Dispatch> = std::sync::OnceLock::new();
        SECOND.get_or_init(|| tracing::Dispatch::new(tracing::subscriber::NoSubscriber::default()));
        let capture = Self::default();
        let subscriber = tracing_subscriber::registry()
            .with(capture.clone().with_filter(EnvFilter::new(filter)));
        let guard = tracing::subscriber::set_default(subscriber);
        (capture, guard)
    }

    fn spans(&self, name: &str) -> Vec<SpanSeen> {
        let c = self.0.lock().unwrap();
        c.spans.iter().filter(|s| s.name == name).cloned().collect()
    }

    fn events(&self, level: Level, target_prefix: &str, contains: &str) -> Vec<EventSeen> {
        let c = self.0.lock().unwrap();
        c.events
            .iter()
            .filter(|e| {
                e.level == level
                    && e.target.starts_with(target_prefix)
                    && e.message.contains(contains)
            })
            .cloned()
            .collect()
    }
}

// ── Helpers ─────────────────────────────────────────────────────────────────

fn rec(topic: &str, partition: i32, value: &str) -> Record {
    Record::new(topic, value.as_bytes().to_vec()).partition(partition)
}

async fn kafka(broker: &FakeBroker) -> krafka::Kafka {
    krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(Duration::from_secs(1))
        .connect_timeout(Duration::from_secs(1))
        .connect()
        .await
        .unwrap()
}

/// The gauge's underflow warning: a release counted twice.
const UNDERFLOW: &str = "below zero";

/// A record whose admission size is just under `budget`.
fn near_budget_record(topic: &str, budget: usize) -> Record {
    let probe = rec(topic, 0, "");
    let overhead = probe.estimated_size();
    rec(topic, 0, &"x".repeat(budget - overhead - 16))
}

/// The memory budget is whole: a record of almost the whole budget is
/// admitted at once.
async fn assert_budget_whole(p: &Producer, topic: &str, budget: usize) {
    let handle = tokio::time::timeout(
        Duration::from_millis(500),
        p.enqueue(near_budget_record(topic, budget)),
    )
    .await
    .expect("buffer memory was not fully released")
    .unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle)
        .await
        .expect("the near-budget record resolves");
}

// ── US1: span level ─────────────────────────────────────────────────────────

/// Acceptance 1 and 3: at `info`, sends build no `send` span; a consumer
/// `poll` still builds its span.
#[tokio::test]
async fn span_send_is_not_built_at_info() {
    let (capture, _guard) = Capture::install("info");
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let kafka = kafka(&broker).await;
    let p = kafka.producer().build().await.unwrap();
    for i in 0..100 {
        let _ = p.send(rec("t", 0, &format!("v{i}"))).await.unwrap();
    }
    assert_eq!(capture.spans("send").len(), 0, "no send span at info");

    let consumer = kafka.consumer_without_group().build().await.unwrap();
    consumer.assign("t", vec![0]).await.unwrap();
    let _ = consumer.poll(Duration::from_millis(200)).await.unwrap();
    assert!(!capture.spans("poll").is_empty(), "poll span still at info");
    consumer.close().await.unwrap();
    p.close().await.unwrap();
}

/// Acceptance 2: with `krafka::producer=debug`, each send has one `send`
/// span with its attributes and its outcome.
#[tokio::test]
async fn span_send_is_built_at_debug_with_its_outcome() {
    let (capture, _guard) = Capture::install("info,krafka::producer=debug");
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let p = kafka(&broker).await.producer().build().await.unwrap();
    let ok = p.send(rec("t", 0, "ok")).await.unwrap();
    let failed = p.send(rec("t", 7, "no such partition")).await;
    assert!(failed.is_err());

    let spans = capture.spans("send");
    assert_eq!(spans.len(), 2, "{spans:?}");
    for span in &spans {
        assert_eq!(
            span.fields.get("otel.name").map(String::as_str),
            Some("send t")
        );
        assert_eq!(
            span.fields
                .get("messaging.destination.name")
                .map(String::as_str),
            Some("t")
        );
    }
    assert_eq!(
        spans[0].fields.get("messaging.kafka.offset"),
        Some(&ok.offset.to_string())
    );
    assert!(spans[1].fields.contains_key("error.type"), "{:?}", spans[1]);
    assert_eq!(
        spans[1].fields.get("otel.status_code").map(String::as_str),
        Some("error")
    );
    p.close().await.unwrap();
}

// ── US2: per-batch release ─────────────────────────────────────────────────

/// Counts acknowledgements, slowly, so a flush that ends early sees a
/// partial count. Optionally panics on one record.
#[derive(Debug, Clone, Default)]
struct SlowAcks {
    acked: Arc<AtomicUsize>,
    delay: Duration,
    panic_on: Option<usize>,
}

impl ProducerInterceptor for SlowAcks {
    fn on_acknowledgement(
        &self,
        _topic: &str,
        _partition: PartitionId,
        _result: Result<&RecordMetadata, &KrafkaError>,
        _headers: &Headers,
        _ctx: &mut RecordContext,
    ) -> InterceptorResult {
        std::thread::sleep(self.delay);
        let n = self.acked.fetch_add(1, Ordering::SeqCst) + 1;
        if self.panic_on == Some(n) {
            panic!("interceptor panic on record {n}");
        }
        Ok(())
    }
}

/// FR-009, acceptance 2: `flush()` returns only after every covered
/// record's caller was answered. Multi-threaded, so a flush released early
/// would observe the batch half answered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn settle_flush_waits_for_every_caller() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let acks = SlowAcks {
        delay: Duration::from_millis(2),
        ..SlowAcks::default()
    };
    let p = kafka(&broker)
        .await
        .producer()
        .linger(Duration::from_millis(50))
        .interceptor(acks.clone())
        .build()
        .await
        .unwrap();
    let _ = p.send(rec("t", 0, "warm")).await.unwrap();
    let before = acks.acked.load(Ordering::SeqCst);
    let mut handles = Vec::new();
    for i in 0..64 {
        handles.push(p.enqueue(rec("t", 0, &format!("v{i}"))).await.unwrap());
    }
    p.flush().await.unwrap();
    assert_eq!(
        acks.acked.load(Ordering::SeqCst) - before,
        64,
        "flush returned before every caller was answered"
    );
    for handle in handles {
        let _ = tokio::time::timeout(Duration::from_millis(10), handle)
            .await
            .expect("every handle is answered when flush returns")
            .unwrap();
    }
    p.close().await.unwrap();
}

/// Acceptance 3: a sender parked on buffer memory is woken by a completed
/// batch, and times out at `max_block` while nothing completes.
#[tokio::test]
async fn settle_parked_sender_is_woken() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let budget = 8 * 1024;
    let p = kafka(&broker)
        .await
        .producer()
        .buffer_memory(budget)
        .batch_size(budget / 2)
        .max_block(Duration::from_millis(600))
        .build()
        .await
        .unwrap();
    let _ = p.send(rec("t", 0, "warm")).await.unwrap();
    broker.on(ApiKey::Produce, |_| {
        Control::Delay(Duration::from_millis(200))
    });
    let half = budget / 2 - 64;
    let first = p.enqueue(near_budget_record("t", half)).await.unwrap();
    let second = p.enqueue(near_budget_record("t", half)).await.unwrap();
    // Parks until the first batch completes, ~200 ms, well inside max_block.
    let started = Instant::now();
    let third = p.enqueue(near_budget_record("t", half)).await.unwrap();
    assert!(started.elapsed() >= Duration::from_millis(50), "it parked");
    for h in [first, second, third] {
        let _ = h.await.unwrap();
    }

    // Nothing completes while the broker is silent: the parked send times out.
    broker.clear_hooks();
    broker.on(ApiKey::Produce, |_| Control::Silence);
    let _a = p.enqueue(near_budget_record("t", half)).await.unwrap();
    let _b = p.enqueue(near_budget_record("t", half)).await.unwrap();
    let parked = p.enqueue(near_budget_record("t", half)).await;
    assert!(
        matches!(parked, Err(KrafkaError::Timeout { .. })),
        "{parked:?}"
    );
    broker.clear_hooks();
}

/// After a batch ends on `path`, everything it held is back: gauge at zero,
/// no double release, flush returns, the whole budget admissible.
async fn assert_all_released(p: &Producer, capture: &Capture, path: &str, budget: usize) {
    tokio::time::timeout(Duration::from_secs(2), p.flush())
        .await
        .unwrap_or_else(|_| panic!("{path}: flush did not return"))
        .unwrap();
    assert_eq!(
        p.metrics().producer.buffered_records,
        0,
        "{path}: buffered gauge"
    );
    assert!(
        capture.events(Level::WARN, "krafka", UNDERFLOW).is_empty(),
        "{path}: released twice"
    );
    assert_budget_whole(p, "t", budget).await;
}

/// Edge cases, FR-010: every way a batch can fail releases exactly once.
#[tokio::test]
async fn settle_on_every_failure_path() {
    let (capture, _guard) = Capture::install("warn");
    let budget = 64 * 1024;

    // Delivery timeout while queued behind a silent broker.
    {
        let broker = FakeBroker::start().await.unwrap();
        broker.create_topic("t", 1);
        let p = kafka(&broker)
            .await
            .producer()
            .buffer_memory(budget)
            .linger(Duration::ZERO)
            .delivery_timeout(Duration::from_millis(1200))
            .build()
            .await
            .unwrap();
        let _ = p.send(rec("t", 0, "warm")).await.unwrap();
        broker.on(ApiKey::Produce, |_| Control::Silence);
        let handles: Vec<_> = {
            let mut v = Vec::new();
            for i in 0..20 {
                v.push(p.enqueue(rec("t", 0, &format!("q{i}"))).await.unwrap());
            }
            v
        };
        for h in handles {
            assert!(h.await.is_err(), "queued: timed out");
        }
        broker.clear_hooks();
        assert_all_released(&p, &capture, "queued timeout", budget).await;
        drop(p);
    }

    // Delivery timeout on the wire, then the late response lands.
    {
        let broker = FakeBroker::start().await.unwrap();
        broker.create_topic("t", 1);
        let p = kafka(&broker)
            .await
            .producer()
            .buffer_memory(budget)
            .linger(Duration::ZERO)
            // Equal to `request_timeout`: the batch expires on the wire,
            // with no time left for a retry.
            .delivery_timeout(Duration::from_secs(1))
            .build()
            .await
            .unwrap();
        let _ = p.send(rec("t", 0, "warm")).await.unwrap();
        broker.on_once(ApiKey::Produce, |_| {
            Control::Delay(Duration::from_millis(1500))
        });
        let h = p.enqueue(rec("t", 0, "late")).await.unwrap();
        assert!(h.await.is_err(), "on the wire: timed out");
        // Let the late response arrive and resolve the batch again.
        tokio::time::sleep(Duration::from_millis(600)).await;
        assert_all_released(&p, &capture, "wire timeout", budget).await;
        p.close().await.unwrap();
    }

    // A fatal answer: every record of the producer fails.
    {
        let broker = FakeBroker::start().await.unwrap();
        broker.create_topic("t", 1);
        let p = kafka(&broker)
            .await
            .producer()
            .buffer_memory(budget)
            .linger(Duration::from_millis(20))
            .build()
            .await
            .unwrap();
        let _ = p.send(rec("t", 0, "warm")).await.unwrap();
        broker.on(ApiKey::Produce, |_| {
            Control::Error(ErrorCode::ClusterAuthorizationFailed)
        });
        let mut handles = Vec::new();
        for i in 0..20 {
            handles.push(p.enqueue(rec("t", 0, &format!("f{i}"))).await.unwrap());
        }
        for h in handles {
            assert!(h.await.is_err(), "fatal: failed");
        }
        tokio::time::timeout(Duration::from_secs(2), p.flush())
            .await
            .expect("fatal: flush returns")
            .unwrap();
        assert_eq!(
            p.metrics().producer.buffered_records,
            0,
            "fatal: buffered gauge"
        );
        assert!(capture.events(Level::WARN, "krafka", UNDERFLOW).is_empty());
    }

    // A transaction abort fails what is still buffered.
    {
        let broker = FakeBroker::start().await.unwrap();
        broker.create_topic("t", 1);
        let p = kafka(&broker)
            .await
            .producer()
            .buffer_memory(budget)
            .linger(Duration::from_secs(5))
            .delivery_timeout(Duration::from_secs(60))
            .build_transactional("settle-abort")
            .await
            .unwrap();
        p.begin().unwrap();
        let mut handles = Vec::new();
        for i in 0..20 {
            handles.push(p.enqueue(rec("t", 0, &format!("a{i}"))).await.unwrap());
        }
        p.abort().await.unwrap();
        for h in handles {
            assert!(h.await.is_err(), "abort: failed");
        }
        assert_eq!(
            p.metrics().producer.buffered_records,
            0,
            "abort: buffered gauge"
        );
        assert!(capture.events(Level::WARN, "krafka", UNDERFLOW).is_empty());
        p.begin().unwrap();
        let h = tokio::time::timeout(
            Duration::from_millis(500),
            p.enqueue(near_budget_record("t", budget)),
        )
        .await
        .expect("abort: buffer memory released")
        .unwrap();
        p.commit().await.unwrap();
        let _ = h.await.unwrap();
        p.close().await.unwrap();
    }
}

/// A batch split after `MESSAGE_TOO_LARGE` releases each record once.
#[tokio::test]
async fn settle_after_split() {
    let (capture, _guard) = Capture::install("warn");
    let budget = 64 * 1024;
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let p = kafka(&broker)
        .await
        .producer()
        .buffer_memory(budget)
        .linger(Duration::from_millis(50))
        .build()
        .await
        .unwrap();
    let _ = p.send(rec("t", 0, "warm")).await.unwrap();
    broker.on_once(ApiKey::Produce, |_| {
        Control::Error(ErrorCode::MessageTooLarge)
    });
    let mut handles = Vec::new();
    for i in 0..16 {
        handles.push(p.enqueue(rec("t", 0, &format!("s{i}"))).await.unwrap());
    }
    for h in handles {
        let _ = h.await.unwrap();
    }
    assert_all_released(&p, &capture, "split", budget).await;
    p.close().await.unwrap();
}

/// Two flush generations in one batch: each flush waits for its own records.
#[tokio::test]
async fn settle_two_generations_in_one_batch() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let p = Arc::new(
        kafka(&broker)
            .await
            .producer()
            .linger(Duration::from_secs(1))
            .build()
            .await
            .unwrap(),
    );
    let _ = p.send(rec("t", 0, "warm")).await.unwrap();
    broker.on(ApiKey::Produce, |_| {
        Control::Delay(Duration::from_millis(300))
    });
    let a = p.enqueue(rec("t", 0, "a")).await.unwrap();
    let flush_a = {
        let p = Arc::clone(&p);
        tokio::spawn(async move { p.flush().await })
    };
    tokio::task::yield_now().await;
    let b = p.enqueue(rec("t", 0, "b")).await.unwrap();
    let flush_b = {
        let p = Arc::clone(&p);
        tokio::spawn(async move { p.flush().await })
    };
    let started = Instant::now();
    flush_a.await.unwrap().unwrap();
    assert!(
        started.elapsed() >= Duration::from_millis(200),
        "the first flush waited for its record's outcome"
    );
    let _ = tokio::time::timeout(Duration::from_millis(10), a)
        .await
        .expect("a has its outcome when its flush returns")
        .unwrap();
    flush_b.await.unwrap().unwrap();
    let _ = tokio::time::timeout(Duration::from_millis(10), b)
        .await
        .expect("b has its outcome when its flush returns")
        .unwrap();
    broker.clear_hooks();
    Arc::into_inner(p).unwrap().close().await.unwrap();
}

/// A panicking interceptor on one record does not stop the batch's release.
#[tokio::test]
async fn settle_survives_interceptor_panic() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let acks = SlowAcks {
        panic_on: Some(3),
        ..SlowAcks::default()
    };
    let p = kafka(&broker)
        .await
        .producer()
        .linger(Duration::from_millis(50))
        .interceptor(acks.clone())
        .build()
        .await
        .unwrap();
    let mut handles = Vec::new();
    for i in 0..10 {
        handles.push(p.enqueue(rec("t", 0, &format!("p{i}"))).await.unwrap());
    }
    for h in handles {
        let _ = h.await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(2), p.flush())
        .await
        .expect("flush returns")
        .unwrap();
    assert_eq!(p.metrics().producer.buffered_records, 0);
    assert_eq!(acks.acked.load(Ordering::SeqCst), 10);
    p.close().await.unwrap();
}

/// FR-007: the records of one batch share one topic name.
#[tokio::test]
async fn metadata_topic_is_shared_not_copied() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let p = kafka(&broker)
        .await
        .producer()
        .linger(Duration::from_millis(50))
        .build()
        .await
        .unwrap();
    let a = p.enqueue(rec("t", 0, "a")).await.unwrap();
    let b = p.enqueue(rec("t", 0, "b")).await.unwrap();
    let (a, b) = (a.await.unwrap(), b.await.unwrap());
    assert_eq!(&*a.topic, "t");
    assert!(Arc::ptr_eq(&a.topic, &b.topic), "one shared topic name");
    p.close().await.unwrap();
}

// ── US4: the drop warning ──────────────────────────────────────────────────

const DROPPED: &str = "dropped without close()";

/// (a) never sent, (b) everything answered: no warning; (c) a record without
/// an outcome: exactly one.
#[tokio::test]
async fn drop_warns_only_with_records_at_risk() {
    let (capture, _guard) = Capture::install("debug");
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let kafka = kafka(&broker).await;
    let warnings = |c: &Capture| c.events(Level::WARN, "krafka::producer", DROPPED).len();

    let idle = kafka.producer().build().await.unwrap();
    drop(idle);
    assert_eq!(warnings(&capture), 0, "a producer that never sent");
    assert_eq!(
        capture
            .events(Level::DEBUG, "krafka::producer", DROPPED)
            .len(),
        1,
        "the drop is still recorded at debug"
    );

    let answered = kafka.producer().build().await.unwrap();
    let _ = answered.send(rec("t", 0, "x")).await.unwrap();
    drop(answered);
    assert_eq!(warnings(&capture), 0, "every record already answered");

    let at_risk = kafka.producer().build().await.unwrap();
    let _ = at_risk.send(rec("t", 0, "warm")).await.unwrap();
    broker.on(ApiKey::Produce, |_| Control::Silence);
    let _pending = at_risk.enqueue(rec("t", 0, "y")).await.unwrap();
    drop(at_risk);
    assert_eq!(warnings(&capture), 1, "a record without an outcome");
    broker.clear_hooks();

    // The transactional producer drops through the same check.
    let txn = kafka
        .producer()
        .build_transactional("drop-idle")
        .await
        .unwrap();
    drop(txn);
    assert_eq!(warnings(&capture), 1, "an idle transactional producer");
}
