//! An acknowledged send is written exactly once; a stamped sequence range is
//! never reused.
//!
//! Every test drives an idempotent producer through a fault on the fake
//! broker, which de-duplicates by `(producer id, epoch, sequence range)` as a
//! Kafka leader does, and then checks the send history against the log.

#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use std::time::Duration;

use krafka::error::{ErrorCode, KrafkaError};
use krafka::producer::{Producer, Record};
use krafka::testing::ApiKey;
use krafka::testing::{Control, FakeBroker};

use support::{History, batch_identities};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(3);

async fn producer(broker: &FakeBroker) -> Producer {
    krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(REQUEST_TIMEOUT)
        .connect_timeout(REQUEST_TIMEOUT)
        .connect()
        .await
        .expect("producer connects")
        .producer()
        .delivery_timeout(DELIVERY_TIMEOUT)
        .build()
        .await
        .expect("producer connects")
}

/// Send `value` to partition 0 of `topic` and record the answer.
async fn send(producer: &Producer, history: &mut History, topic: &str, value: &str) {
    let result = producer
        .send(Record::new(topic, value.as_bytes().to_vec()).partition(0))
        .await;
    history.record(value, result);
}

/// A produce that is appended but answered after `delivery_timeout` fails
/// record A; record B, sent afterwards, must then be in the log.
async fn appended_then_lost(fault: Control) -> History {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let producer = producer(&broker).await;
    let mut history = History::new();

    broker.on(ApiKey::Produce, move |_| fault.clone());
    send(&producer, &mut history, "t", "A").await;
    assert!(
        history.sends()[0].result.is_err(),
        "A must fail on delivery_timeout: {:?}",
        history.sends()[0].result
    );

    // Let every late append land, then serve normally.
    tokio::time::sleep(Duration::from_secs(3)).await;
    broker.clear_hooks();
    send(&producer, &mut history, "t", "B").await;
    send(&producer, &mut history, "t", "C").await;

    history.assert_holds(&broker, "t");
    producer.close().await.unwrap();
    history
}

/// P1: the failed batch's sequence range must not be handed to the next
/// batch, or the broker answers B as a duplicate of A.
#[tokio::test]
async fn an_appended_batch_whose_answer_is_lost_does_not_swallow_the_next() {
    let history =
        appended_then_lost(Control::Delay(REQUEST_TIMEOUT + Duration::from_millis(500))).await;
    let a = history.sends()[0].result.as_ref().unwrap_err();
    assert!(
        matches!(
            a,
            KrafkaError::DeliveryTimeout {
                possibly_written: true,
                ..
            }
        ),
        "a batch written to the connection reports possibly_written: {a:?}"
    );
}

/// The same with the leader answering `NOT_ENOUGH_REPLICAS_AFTER_APPEND`
/// after it appended: the error means "written".
#[tokio::test]
async fn not_enough_replicas_after_append_counts_as_written() {
    let history = appended_then_lost(Control::ApplyThen(Box::new(Control::Error(
        ErrorCode::NotEnoughReplicasAfterAppend,
    ))))
    .await;
    let a = history.sends()[0].result.as_ref().unwrap_err();
    assert!(
        matches!(
            a,
            KrafkaError::DeliveryTimeout {
                possibly_written: true,
                ..
            }
        ),
        "NOT_ENOUGH_REPLICAS_AFTER_APPEND is not a definitive non-append: {a:?}"
    );
}

/// Negative control: without a fault the checker passes.
#[tokio::test]
async fn without_a_fault_every_send_is_written_once() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let producer = producer(&broker).await;
    let mut history = History::new();
    for value in ["A", "B", "C"] {
        send(&producer, &mut history, "t", value).await;
    }
    assert!(history.sends().iter().all(|s| s.result.is_ok()));
    history.assert_holds(&broker, "t");
    producer.close().await.unwrap();
}

/// Negative control for the broker model: with de-duplication off, a retry
/// of an appended batch is written twice and the checker says so.
#[tokio::test]
async fn the_checker_reports_a_duplicate_when_the_broker_does_not_dedup() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    broker.set_idempotence(false);
    let producer = producer(&broker).await;
    let mut history = History::new();

    // Appended, then the connection drops: the client retries the batch.
    broker.on_once(ApiKey::Produce, |_| {
        Control::ApplyThen(Box::new(Control::Disconnect))
    });
    send(&producer, &mut history, "t", "A").await;

    let violations = history.violations(&broker, "t");
    assert!(
        violations.iter().any(|v| v.contains("2 times")),
        "the checker must see the duplicate: {violations:?} / {:?}",
        history.sends()
    );
    producer.close().await.unwrap();
}

/// No two batches the broker stored for one partition carry the same
/// `(producer id, epoch, base sequence)`.
fn assert_identities_unique(broker: &FakeBroker, topic: &str) {
    let identities = batch_identities(broker, topic, 0);
    let mut seen = std::collections::HashSet::new();
    for (pid, epoch, seq, _) in &identities {
        assert!(
            seen.insert((*pid, *epoch, *seq)),
            "two stored batches share ({pid}, {epoch}, {seq}): {identities:?}"
        );
    }
}

/// P2: `OUT_OF_ORDER_SEQUENCE_NUMBER` fails the head batch with a non-fatal
/// `OutOfOrderSequence`, bumps the epoch, and the next batch is accepted at
/// sequence 0 — the identical sequence is never resent.
#[tokio::test]
async fn out_of_order_sequence_fails_the_head_batch_and_bumps() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let producer = producer(&broker).await;
    let mut history = History::new();
    send(&producer, &mut history, "t", "warm").await;

    broker.on_once(ApiKey::Produce, |_| {
        Control::Error(ErrorCode::OutOfOrderSequenceNumber)
    });
    broker.clear_requests();
    send(&producer, &mut history, "t", "x").await;
    let error = history.sends()[1].result.as_ref().unwrap_err();
    assert!(
        matches!(error, KrafkaError::OutOfOrderSequence { .. }),
        "OOSN fails the head batch with OutOfOrderSequence: {error:?}"
    );
    assert!(!error.is_fatal(), "OutOfOrderSequence is not fatal");
    assert_eq!(
        broker.request_count(ApiKey::Produce),
        1,
        "the identical sequence is not resent"
    );

    send(&producer, &mut history, "t", "y").await;
    assert!(
        history.sends()[2].result.is_ok(),
        "{:?}",
        history.sends()[2]
    );
    let identities = batch_identities(&broker, "t", 0);
    let (_, warm_epoch, _, _) = identities[0];
    let (_, epoch, seq, _) = *identities.last().unwrap();
    assert_eq!(
        seq, 0,
        "the next batch restarts at sequence 0: {identities:?}"
    );
    assert!(epoch > warm_epoch, "under a bumped epoch: {identities:?}");
    history.assert_holds(&broker, "t");
    assert_identities_unique(&broker, "t");
    producer.close().await.unwrap();
}

/// `UNKNOWN_PRODUCER_ID` with the log start past the last acknowledged
/// offset is benign: retention removed the producer's state, so the producer
/// bumps and the record is delivered.
#[tokio::test]
async fn unknown_producer_id_after_retention_is_retried_under_a_new_epoch() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    // A leader that answers UNKNOWN_PRODUCER_ID for a producer it has no
    // state for (InitProducerId below v3, before KIP-360).
    broker.set_api_versions(ApiKey::InitProducerId, 0, 2);
    let producer = producer(&broker).await;
    let mut history = History::new();
    send(&producer, &mut history, "t", "warm").await;

    // Retention removed everything the producer wrote, and its state.
    broker.with_state(|s| {
        let p = s.partition_mut("t", 0).unwrap();
        p.log_start_offset = p.next_offset;
    });
    assert!(broker.clear_producer_state("t", 0));
    send(&producer, &mut history, "t", "after-retention").await;
    assert!(
        history.sends()[1].result.is_ok(),
        "benign UNKNOWN_PRODUCER_ID is not reported: {:?}",
        history.sends()[1].result
    );
    let (_, epoch, seq, _) = *batch_identities(&broker, "t", 0).last().unwrap();
    assert_eq!(
        (epoch, seq),
        (1, 0),
        "re-stamped at sequence 0 under epoch 1"
    );
    producer.close().await.unwrap();
}

/// `UNKNOWN_PRODUCER_ID` while the log still holds the acknowledged batch is
/// data loss: it is handled as OOSN.
#[tokio::test]
async fn unknown_producer_id_without_retention_is_handled_as_out_of_order() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let producer = producer(&broker).await;
    let mut history = History::new();
    send(&producer, &mut history, "t", "warm").await;

    broker.on_once(ApiKey::Produce, |_| {
        Control::Error(ErrorCode::UnknownProducerId)
    });
    send(&producer, &mut history, "t", "lost").await;
    let error = history.sends()[1].result.as_ref().unwrap_err();
    assert!(
        matches!(error, KrafkaError::OutOfOrderSequence { .. }),
        "{error:?}"
    );
    send(&producer, &mut history, "t", "next").await;
    assert!(history.sends()[2].result.is_ok());
    history.assert_holds(&broker, "t");
    producer.close().await.unwrap();
}

/// A batch rejected with a definitive non-append error is reported with that
/// error, and its range is still spent: the next batch moves to a new epoch.
#[tokio::test]
async fn a_definitive_rejection_spends_the_range() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let producer = producer(&broker).await;
    let mut history = History::new();
    send(&producer, &mut history, "t", "warm").await;

    broker.on_once(ApiKey::Produce, |_| {
        Control::Error(ErrorCode::InvalidRecord)
    });
    send(&producer, &mut history, "t", "bad").await;
    let error = history.sends()[1].result.as_ref().unwrap_err();
    assert!(
        matches!(
            error,
            KrafkaError::Broker {
                code: ErrorCode::InvalidRecord,
                ..
            }
        ),
        "{error:?}"
    );
    send(&producer, &mut history, "t", "good").await;
    let identities = batch_identities(&broker, "t", 0);
    let (_, epoch, seq, _) = *identities.last().unwrap();
    assert_eq!((epoch, seq), (1, 0), "{identities:?}");
    history.assert_holds(&broker, "t");
    producer.close().await.unwrap();
}

/// A batch that expires before it is ever written reports
/// `possibly_written: false`; one that was written reports `true`.
#[tokio::test]
async fn only_a_written_batch_is_possibly_written() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let producer = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(REQUEST_TIMEOUT)
        .connect_timeout(REQUEST_TIMEOUT)
        .connect()
        .await
        .unwrap()
        .producer()
        .delivery_timeout(DELIVERY_TIMEOUT)
        .linger(Duration::ZERO)
        .batch_size(1)
        .build()
        .await
        .unwrap();
    let _ = producer
        .send(Record::new("t", b"warm".to_vec()).partition(0))
        .await
        .unwrap();

    // The first batch goes on the wire and is never answered; the second
    // waits behind it for its partition's turn until it expires.
    broker.on(ApiKey::Produce, |_| Control::Silence);
    let first = producer
        .enqueue(Record::new("t", b"first".to_vec()).partition(0))
        .await
        .unwrap();
    let second = producer
        .enqueue(Record::new("t", b"second".to_vec()).partition(0))
        .await
        .unwrap();
    let (first, second) = (first.await, second.await);
    assert!(
        matches!(
            first,
            Err(KrafkaError::DeliveryTimeout {
                possibly_written: true,
                ..
            })
        ),
        "{first:?}"
    );
    assert!(
        matches!(
            second,
            Err(KrafkaError::DeliveryTimeout {
                possibly_written: false,
                ..
            })
        ),
        "{second:?}"
    );
    broker.clear_hooks();
    producer.close().await.unwrap();
}

/// Two partitions whose batches fail at once cause one bump, not two.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_failures_bump_once() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 2);
    let producer = producer(&broker).await;
    for partition in 0..2 {
        let _ = producer
            .send(Record::new("t", b"warm".to_vec()).partition(partition))
            .await
            .unwrap();
    }
    broker.on_times(ApiKey::Produce, 1, |_| {
        Control::Error(ErrorCode::OutOfOrderSequenceNumber)
    });
    let (a, b) = tokio::join!(
        producer.send(Record::new("t", b"a".to_vec()).partition(0)),
        producer.send(Record::new("t", b"b".to_vec()).partition(1)),
    );
    assert!(a.is_err() != b.is_err() || a.is_err(), "{a:?} {b:?}");
    for partition in 0..2 {
        let _ = producer
            .send(Record::new("t", b"after".to_vec()).partition(partition))
            .await
            .unwrap();
    }
    let epochs: std::collections::BTreeSet<i16> = (0..2)
        .flat_map(|p| batch_identities(&broker, "t", p))
        .map(|(_, epoch, _, _)| epoch)
        .collect();
    assert_eq!(
        epochs.into_iter().collect::<Vec<_>>(),
        vec![0, 1],
        "one bump for the whole producer"
    );
    assert_eq!(
        broker.request_count(ApiKey::InitProducerId),
        1,
        "the bump is local"
    );
    producer.close().await.unwrap();
}
