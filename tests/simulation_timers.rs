//! Client timers under a paused clock, against the in-memory fake broker.
//!
//! Each test checks a timer one millisecond before its configured duration
//! (nothing happened: the negative control) and at or just after it (it
//! fired). Simulated time costs no wall time, so a 5 s linger takes
//! milliseconds.

#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::time::Duration;

use krafka::Kafka;
use krafka::consumer::{AutoOffsetReset, Consumer, ConsumerBuilder};
use krafka::error::{ErrorCode, KrafkaError};
use krafka::producer::Record;
use krafka::testing::{ApiKey, Control, FakeBroker, RecordedRequest};
use tokio::time::Instant;

const MS: Duration = Duration::from_millis(1);

fn broker() -> FakeBroker {
    let broker = FakeBroker::start_in_memory(1);
    broker.create_topic("t", 1);
    broker
}

async fn kafka(broker: &FakeBroker, request_timeout: Duration) -> Kafka {
    broker
        .kafka()
        .request_timeout(request_timeout)
        .connect_timeout(request_timeout)
        .connect()
        .await
        .unwrap()
}

fn record() -> Record {
    Record::new("t", b"v".to_vec()).partition(0)
}

fn requests(broker: &FakeBroker, api: ApiKey) -> Vec<RecordedRequest> {
    broker
        .requests()
        .into_iter()
        .filter(|r| r.api_key == api)
        .collect()
}

/// Sleep until `start + offset` on the paused clock.
async fn at(start: Instant, offset: Duration) {
    tokio::time::sleep_until(start + offset).await;
}

/// A batch is sent when its linger has passed, not before.
#[tokio::test(start_paused = true)]
async fn linger() {
    let wall = std::time::Instant::now();
    let broker = broker();
    let producer = kafka(&broker, Duration::from_secs(30))
        .await
        .producer()
        .linger(Duration::from_secs(5))
        .build()
        .await
        .unwrap();
    let start = Instant::now();
    let handle = producer.enqueue(record()).await.unwrap();

    at(start, Duration::from_millis(4900)).await;
    assert_eq!(broker.request_count(ApiKey::Produce), 0, "sent at 4.9 s");
    at(start, Duration::from_secs(5) - MS).await;
    assert_eq!(broker.request_count(ApiKey::Produce), 0, "sent 1 ms early");
    at(start, Duration::from_secs(5) + MS).await;
    assert_eq!(broker.request_count(ApiKey::Produce), 1, "not sent at 5 s");
    let _ = handle.await.unwrap();
    producer.close().await.unwrap();
    assert!(
        wall.elapsed() < Duration::from_secs(1),
        "{:?}",
        wall.elapsed()
    );
}

/// A retriable produce error is retried `retry_backoff` (plus up to 20 %
/// jitter) after the error was handled, and handling it includes the
/// metadata refresh it triggers, 100 ms on this cluster.
#[tokio::test(start_paused = true)]
async fn retry_backoff() {
    krafka::testing::seed_rng(1);
    let backoff = Duration::from_millis(400);
    let refresh = Duration::from_millis(100);
    let broker = broker();
    broker.on_once(ApiKey::Produce, |_| {
        Control::Error(ErrorCode::NotEnoughReplicas)
    });
    let producer = kafka(&broker, Duration::from_secs(30))
        .await
        .producer()
        .linger(Duration::ZERO)
        .retry_backoff(backoff)
        .build()
        .await
        .unwrap();
    // Linger is zero: the first attempt goes out, and fails, at `start`.
    let start = Instant::now();
    let handle = producer.enqueue(record()).await.unwrap();

    at(start, refresh + backoff - MS).await;
    assert_eq!(broker.request_count(ApiKey::Produce), 1, "retried early");
    let _ = handle.await.unwrap();
    let produce = requests(&broker, ApiKey::Produce);
    let gap = produce[1].at - produce[0].at - refresh;
    assert!(
        gap >= backoff && gap <= backoff.mul_f64(1.2) + MS,
        "retried {gap:?} after the refresh, backoff {backoff:?}"
    );
    producer.close().await.unwrap();
}

/// A request without an answer times out after `request_timeout`: its
/// connection is closed then, not before, and the batch is retried on a new
/// one.
#[tokio::test(start_paused = true)]
async fn request_timeout() {
    let timeout = Duration::from_secs(2);
    let broker = broker();
    broker.on_once(ApiKey::Produce, |_| Control::Silence);
    let producer = kafka(&broker, timeout)
        .await
        .producer()
        .linger(Duration::ZERO)
        .build()
        .await
        .unwrap();
    let stalled = || producer.metrics().connections.stalled_connections;
    let start = Instant::now();
    let handle = producer.enqueue(record()).await.unwrap();

    at(start, timeout - MS).await;
    assert_eq!(stalled(), 0, "timed out early");
    at(start, timeout + MS).await;
    assert_eq!(stalled(), 1, "not timed out");
    let _ = handle.await.unwrap();
    let produce = requests(&broker, ApiKey::Produce);
    assert_ne!(
        produce[0].connection, produce[1].connection,
        "same connection"
    );
    producer.close().await.unwrap();
}

/// A record with no answer fails at `delivery_timeout` after it was
/// enqueued, not before.
#[tokio::test(start_paused = true)]
async fn delivery_timeout() {
    let delivery = Duration::from_secs(3);
    let broker = broker();
    broker.on(ApiKey::Produce, |_| Control::Silence);
    let producer = kafka(&broker, Duration::from_secs(1))
        .await
        .producer()
        .linger(Duration::ZERO)
        .delivery_timeout(delivery)
        .build()
        .await
        .unwrap();
    let start = Instant::now();
    let mut handle = producer.enqueue(record()).await.unwrap();

    at(start, delivery - MS).await;
    assert!(
        futures::FutureExt::now_or_never(&mut handle).is_none(),
        "failed before delivery_timeout"
    );
    at(start, delivery + MS).await;
    let error = futures::FutureExt::now_or_never(&mut handle)
        .expect("no outcome at delivery_timeout")
        .unwrap_err();
    assert!(
        matches!(
            error,
            KrafkaError::DeliveryTimeout {
                possibly_written: true,
                ..
            }
        ),
        "{error:?}"
    );
    broker.clear_hooks();
    producer.close().await.unwrap();
}

/// Cached metadata for a topic is reused for `metadata_max_age` and fetched
/// again after it.
#[tokio::test(start_paused = true)]
async fn metadata_max_age() {
    let max_age = Duration::from_secs(10);
    let broker = broker();
    let consumer = broker
        .kafka()
        .metadata_max_age(max_age)
        .connect()
        .await
        .unwrap()
        .consumer_without_group()
        .build()
        .await
        .unwrap();
    consumer.fetch_metadata(Some("t")).await.unwrap();
    let start = Instant::now();
    let fetched = broker.request_count(ApiKey::Metadata);

    at(start, max_age - MS).await;
    consumer.fetch_metadata(Some("t")).await.unwrap();
    assert_eq!(
        broker.request_count(ApiKey::Metadata),
        fetched,
        "refetched early"
    );
    at(start, max_age + MS).await;
    consumer.fetch_metadata(Some("t")).await.unwrap();
    assert_eq!(
        broker.request_count(ApiKey::Metadata),
        fetched + 1,
        "not refetched"
    );
    consumer.close().await.unwrap();
}

/// A consumer in group `g` subscribed to `t`, after its first poll, and the
/// time that poll was called.
async fn group_consumer(
    broker: &FakeBroker,
    configure: impl FnOnce(ConsumerBuilder) -> ConsumerBuilder,
) -> (Consumer, Instant) {
    let builder = broker
        .kafka()
        .connect()
        .await
        .unwrap()
        .consumer("g")
        .auto_offset_reset(AutoOffsetReset::Earliest);
    let consumer = configure(builder).build().await.unwrap();
    consumer.subscribe(["t"]).await.unwrap();
    let polled = Instant::now();
    consumer.poll(Duration::from_millis(100)).await.unwrap();
    (consumer, polled)
}

/// Heartbeats go out every `heartbeat_interval`, the client side of the
/// session timeout.
#[tokio::test(start_paused = true)]
async fn heartbeat_interval() {
    let interval = Duration::from_millis(700);
    let broker = broker();
    let (consumer, _) = group_consumer(&broker, |b| b.heartbeat_interval(interval)).await;
    let start = requests(&broker, ApiKey::Heartbeat).len();
    tokio::time::sleep(interval * 4 + MS).await;
    let beats = &requests(&broker, ApiKey::Heartbeat)[start..];
    assert!(beats.len() >= 3, "{} heartbeats", beats.len());
    for pair in beats.windows(2) {
        assert_eq!(pair[1].at - pair[0].at, interval, "{beats:?}");
    }
    consumer.close().await.unwrap();
}

/// A member that stops polling leaves the group once `max_poll_interval`
/// has passed since its last poll (checked on its next heartbeat), not
/// before.
#[tokio::test(start_paused = true)]
async fn max_poll_interval() {
    let interval = Duration::from_secs(5);
    let heartbeat = Duration::from_millis(100);
    let broker = broker();
    let (consumer, polled) = group_consumer(&broker, |b| {
        b.max_poll_interval(interval).heartbeat_interval(heartbeat)
    })
    .await;

    at(polled, interval - MS).await;
    assert_eq!(broker.request_count(ApiKey::LeaveGroup), 0, "left early");
    at(polled, interval + heartbeat + MS).await;
    assert_eq!(broker.request_count(ApiKey::LeaveGroup), 1, "did not leave");
    drop(consumer);
}

/// A poll whose partition leader refuses connections waits out its timeout
/// on the clock instead of retrying the fetch in a loop: the partition backs
/// off after the failed fetch. A loop that never yields to a timer stops the
/// paused clock, so the poll runs on its own thread under a wall-clock limit.
#[test]
fn a_refused_fetch_backs_off_instead_of_spinning() {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .start_paused(true)
            .build()
            .unwrap();
        let result = runtime.block_on(async {
            let broker = broker();
            let consumer = kafka(&broker, Duration::from_secs(2))
                .await
                .consumer_without_group()
                .auto_offset_reset(AutoOffsetReset::Earliest)
                .build()
                .await
                .unwrap();
            consumer.assign("t", vec![0]).await.unwrap();
            let _ = consumer.poll(Duration::from_millis(100)).await.unwrap();
            broker.crash(0);
            let start = Instant::now();
            let polled = consumer.poll(Duration::from_secs(1)).await;
            (polled.map(|r| r.len()), start.elapsed())
        });
        let _ = tx.send(result);
    });
    let (polled, elapsed) = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("poll spun without yielding to the clock");
    assert_eq!(polled.unwrap(), 0);
    assert!(elapsed >= Duration::from_secs(1), "{elapsed:?}");
}
