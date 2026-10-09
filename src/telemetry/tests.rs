//! KIP-714 end to end, against the fake broker.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use super::otlp::{self, Value};
use crate::Kafka;
use crate::error::KrafkaError;
use crate::testing::{ApiKey, Control, FakeBroker, TelemetryPush, TelemetrySubscription};

const INTERVAL: Duration = Duration::from_millis(200);

async fn broker_with(subscription: Option<TelemetrySubscription>) -> FakeBroker {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);
    broker.set_telemetry(subscription);
    broker
}

async fn connect(broker: &FakeBroker) -> Kafka {
    Kafka::builder(broker.bootstrap_servers())
        .request_timeout(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(1))
        .connect()
        .await
        .unwrap()
}

/// Wait until `pred` holds for the pushes received, or fail after `within`.
async fn pushes_until(
    broker: &FakeBroker,
    within: Duration,
    pred: impl Fn(&[TelemetryPush]) -> bool,
) -> Vec<TelemetryPush> {
    let deadline = Instant::now() + within;
    loop {
        let pushes = broker.telemetry_pushes();
        if pred(&pushes) {
            return pushes;
        }
        assert!(
            Instant::now() < deadline,
            "no matching push within {within:?}; got {pushes:?}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn telemetry_requests(broker: &FakeBroker) -> usize {
    broker.request_count(ApiKey::GetTelemetrySubscriptions)
        + broker.request_count(ApiKey::PushTelemetry)
}

fn names(push: &TelemetryPush) -> Vec<(String, Value)> {
    otlp::decode(&push.metrics)
}

/// A default producer subscribes, pushes
/// within 1.5 intervals, and `close()` ends with exactly one terminating
/// push.
///
/// Reverted-line control: with `Telemetry::start` removed from
/// `Producer::new`, no `GetTelemetrySubscriptions` arrives and this fails.
#[tokio::test]
async fn a_default_producer_pushes_and_terminates_on_close() {
    let broker = broker_with(Some(TelemetrySubscription::new(["*"], INTERVAL))).await;
    let kafka = connect(&broker).await;
    let producer = kafka.producer().build().await.unwrap();
    let _ = producer
        .send(crate::Record::new("orders", "v").key("k"))
        .await
        .unwrap();

    let pushes = pushes_until(
        &broker,
        INTERVAL * 3 / 2 + Duration::from_millis(500),
        |p| !p.is_empty(),
    )
    .await;
    assert!(!pushes[0].terminating, "data pushes are not terminating");
    let pushed = names(&pushes[0]);
    assert!(
        pushed
            .iter()
            .any(|(n, _)| n == "org.apache.kafka.producer.record.send.total"),
        "{pushed:?}"
    );
    assert_eq!(broker.request_count(ApiKey::GetTelemetrySubscriptions), 1);

    producer.close().await.unwrap();
    let pushes = broker.telemetry_pushes();
    assert_eq!(pushes.iter().filter(|p| p.terminating).count(), 1);
    assert!(
        pushes.last().unwrap().terminating,
        "the last push terminates"
    );

    // A second close sends nothing more.
    producer.close().await.unwrap();
    assert_eq!(broker.telemetry_pushes().len(), pushes.len());
}

/// A subscription to the dotted producer prefix
/// selects the real pushed names, with the counters the producer's
/// `metrics()` reports.
///
/// Reverted-line controls: joining name segments with `_` leaves the
/// push empty of matching names; giving the reporter `Metrics::default()`
/// instead of the client's snapshot leaves every counter at zero.
#[tokio::test]
async fn a_dotted_prefix_subscription_receives_the_producers_counters() {
    let prefix = "org.apache.kafka.producer.";
    let broker = broker_with(Some(TelemetrySubscription::new([prefix], INTERVAL))).await;
    let kafka = connect(&broker).await;
    let producer = kafka.producer().build().await.unwrap();
    for i in 0..3 {
        let _ = producer
            .send(crate::Record::new("orders", format!("v{i}")))
            .await
            .unwrap();
    }

    let pushes = pushes_until(&broker, Duration::from_secs(3), |p| !p.is_empty()).await;
    let pushed = names(&pushes[0]);
    assert!(!pushed.is_empty());
    assert!(
        pushed.iter().all(|(n, _)| n.starts_with(prefix)),
        "{pushed:?}"
    );
    let sent = pushed
        .iter()
        .find(|(n, _)| n == "org.apache.kafka.producer.record.send.total")
        .map(|(_, v)| *v);
    assert_eq!(
        sent,
        Some(Value::Sum(producer.metrics().producer.records_sent))
    );
    assert_eq!(sent, Some(Value::Sum(3)));
    producer.close().await.unwrap();
}

/// Delta temporality: each push carries what changed since the last
/// accepted one.
#[tokio::test]
async fn delta_temporality_pushes_the_change_since_the_last_push() {
    let mut subscription =
        TelemetrySubscription::new(["org.apache.kafka.producer.record.send."], INTERVAL);
    subscription.delta_temporality = true;
    let broker = broker_with(Some(subscription)).await;
    let kafka = connect(&broker).await;
    let producer = kafka.producer().build().await.unwrap();
    let _ = producer
        .send(crate::Record::new("orders", "a"))
        .await
        .unwrap();
    let _ = producer
        .send(crate::Record::new("orders", "b"))
        .await
        .unwrap();
    pushes_until(&broker, Duration::from_secs(3), |p| !p.is_empty()).await;
    let _ = producer
        .send(crate::Record::new("orders", "c"))
        .await
        .unwrap();
    let pushes = pushes_until(&broker, Duration::from_secs(3), |p| p.len() >= 2).await;

    let sent = |push: &TelemetryPush| {
        names(push)
            .into_iter()
            .find(|(n, _)| n.ends_with("record.send.total"))
            .map(|(_, v)| v)
    };
    assert_eq!(sent(&pushes[0]), Some(Value::Sum(2)));
    assert_eq!(sent(&pushes[1]), Some(Value::Sum(1)));
    producer.close().await.unwrap();
}

/// The consumer and the share consumer push by default
/// under the consumer client type; the transactional producer as a
/// producer.
#[tokio::test]
async fn every_default_on_client_pushes() {
    let broker = broker_with(Some(TelemetrySubscription::new(["*"], INTERVAL))).await;
    let kafka = connect(&broker).await;

    let consumer = kafka.consumer("g").build().await.unwrap();
    let pushes = pushes_until(&broker, Duration::from_secs(3), |p| !p.is_empty()).await;
    assert!(
        names(&pushes[0])
            .iter()
            .any(|(n, _)| n.starts_with("org.apache.kafka.consumer.fetch.manager."))
    );
    consumer.close().await.unwrap();

    let before = broker.telemetry_pushes().len();
    let share = kafka.share_consumer("s").build().await.unwrap();
    pushes_until(&broker, Duration::from_secs(3), |p| {
        p[before..].iter().any(|p| !p.terminating)
    })
    .await;
    share.close().await.unwrap();

    let before = broker.telemetry_pushes().len();
    let txn = kafka.producer().build_transactional("t").await.unwrap();
    let pushes = pushes_until(&broker, Duration::from_secs(3), |p| {
        p[before..].iter().any(|p| !p.terminating)
    })
    .await;
    assert!(
        names(&pushes[before])
            .iter()
            .any(|(n, _)| n.starts_with("org.apache.kafka.producer."))
    );
    txn.close().await.unwrap();
}

/// With the switch off — and for a default admin client — no
/// request with API key 71 or 72 is sent, closing included.
///
/// Reverted-line control: with `Telemetry::start` ignoring
/// `enabled`, the producer's subscription request fails this.
#[tokio::test]
async fn the_switch_off_sends_nothing_and_neither_does_a_default_admin() {
    let broker = broker_with(Some(TelemetrySubscription::new(["*"], INTERVAL))).await;
    let kafka = connect(&broker).await;

    let producer = kafka.producer().metrics_push(false).build().await.unwrap();
    let _ = producer
        .send(crate::Record::new("orders", "v"))
        .await
        .unwrap();
    let consumer = kafka
        .consumer("g")
        .metrics_push(false)
        .build()
        .await
        .unwrap();
    let share = kafka
        .share_consumer("s")
        .metrics_push(false)
        .build()
        .await
        .unwrap();
    let admin = kafka.admin();
    admin
        .list_topics(crate::admin::ListTopicsOptions::default())
        .await
        .unwrap();

    tokio::time::sleep(INTERVAL * 3).await;
    producer.close().await.unwrap();
    consumer.close().await.unwrap();
    share.close().await.unwrap();
    admin.close().await.unwrap();

    assert_eq!(telemetry_requests(&broker), 0);
    assert!(matches!(
        producer.client_instance_id(Duration::from_secs(1)).await,
        Err(KrafkaError::IllegalState { .. })
    ));
    assert!(matches!(
        admin.client_instance_id(Duration::from_secs(1)).await,
        Err(KrafkaError::IllegalState { .. })
    ));
}

/// The admin client pushes once its switch is on.
#[tokio::test]
async fn an_admin_client_pushes_when_switched_on() {
    let broker = broker_with(Some(TelemetrySubscription::new(["*"], INTERVAL))).await;
    let admin = connect(&broker).await.admin().metrics_push(true);
    let pushes = pushes_until(&broker, Duration::from_secs(3), |p| !p.is_empty()).await;
    assert!(
        names(&pushes[0])
            .iter()
            .all(|(n, _)| n.starts_with("org.apache.kafka.admin."))
    );
    admin.close().await.unwrap();
}

/// Records every event's level and target.
#[derive(Clone, Default)]
struct Events(Arc<parking_lot::Mutex<Vec<(tracing::Level, String)>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Events {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        let meta = event.metadata();
        self.0
            .lock()
            .push((*meta.level(), meta.target().to_string()));
    }
}

/// Against a cluster without a telemetry plugin a
/// default client sends no telemetry request, the instance id is `None`, and
/// the telemetry component logs nothing above `debug`.
///
/// Reverted-line control: logging the missing API at `warn` fails the
/// event assertion.
#[tokio::test]
async fn a_cluster_without_telemetry_sees_nothing_and_logs_nothing() {
    use tracing_subscriber::layer::SubscriberExt;
    let events = Events::default();
    let _guard =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(events.clone()));

    let broker = broker_with(None).await;
    let kafka = connect(&broker).await;
    let producer = kafka.producer().build().await.unwrap();
    let _ = producer
        .send(crate::Record::new("orders", "v"))
        .await
        .unwrap();
    assert_eq!(
        producer
            .client_instance_id(Duration::from_secs(2))
            .await
            .unwrap(),
        None
    );
    producer.close().await.unwrap();

    assert_eq!(telemetry_requests(&broker), 0);
    let telemetry_events: Vec<_> = events
        .0
        .lock()
        .iter()
        .filter(|(level, target)| {
            target.starts_with("krafka::telemetry") && *level <= tracing::Level::INFO
        })
        .cloned()
        .collect();
    assert!(telemetry_events.is_empty(), "{telemetry_events:?}");
    assert!(
        events
            .0
            .lock()
            .iter()
            .any(|(_, target)| target.starts_with("krafka::telemetry")),
        "the reporter ran and logged at debug"
    );
}

/// An empty subscription is re-polled once per interval and
/// never pushed to, not even at close.
#[tokio::test]
async fn an_empty_subscription_is_polled_not_pushed() {
    let none: [&str; 0] = [];
    let broker = broker_with(Some(TelemetrySubscription::new(none, INTERVAL))).await;
    let kafka = connect(&broker).await;
    let started = Instant::now();
    let producer = kafka.producer().build().await.unwrap();
    tokio::time::sleep(INTERVAL * 4).await;
    producer.close().await.unwrap();

    assert!(broker.telemetry_pushes().is_empty());
    assert_eq!(broker.request_count(ApiKey::PushTelemetry), 0);
    let polls = broker.request_count(ApiKey::GetTelemetrySubscriptions);
    let bound = (started.elapsed().as_millis() / INTERVAL.as_millis()) as usize + 1;
    assert!(polls >= 2, "the subscription is re-polled, got {polls}");
    assert!(polls <= bound, "{polls} polls in {:?}", started.elapsed());
}

/// The instance id is the broker's; a broker that never answers is a
/// timeout, not a hang.
#[tokio::test]
async fn the_client_instance_id_is_the_brokers_and_bounded_by_the_timeout() {
    let subscription = TelemetrySubscription::new(["*"], INTERVAL);
    let assigned = subscription.client_instance_id;
    let broker = broker_with(Some(subscription)).await;
    let kafka = connect(&broker).await;
    let consumer = kafka.consumer("g").build().await.unwrap();
    let id = consumer
        .client_instance_id(Duration::from_secs(2))
        .await
        .unwrap()
        .expect("the broker assigned one");
    assert_eq!(id.as_bytes(), &assigned);
    consumer.close().await.unwrap();

    let silent = broker_with(Some(TelemetrySubscription::new(["*"], INTERVAL))).await;
    silent.on(ApiKey::GetTelemetrySubscriptions, |_| Control::Silence);
    let kafka = connect(&silent).await;
    let producer = kafka.producer().metrics_push(true).build().await.unwrap();
    let started = Instant::now();
    let result = producer
        .client_instance_id(Duration::from_millis(300))
        .await;
    assert!(
        matches!(result, Err(KrafkaError::Timeout { .. })),
        "{result:?}"
    );
    assert!(started.elapsed() < Duration::from_secs(1));
    producer
        .close_with(crate::CloseOptions::new().timeout(Duration::from_millis(500)))
        .await
        .ok();
}
