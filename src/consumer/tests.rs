#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::atomic::AtomicUsize;

use super::state::CompletedFetch;
use super::*;

fn test_consumer() -> Consumer {
    let config = ConsumerConfig::default();
    let pool = Arc::new(ConnectionPool::new(
        crate::network::ConnectionConfig::default(),
    ));
    let metadata = Arc::new(
        ClusterMetadata::new(
            vec!["127.0.0.1:9092".to_string()],
            pool.clone(),
            Duration::from_secs(300),
        )
        .with_topic_cache_ttl_disabled(),
    );
    let metrics = Arc::new(ConsumerRecorder::default());
    let metrics_source = MetricsSource::consumer(&Kafka::detached(), Arc::clone(&metrics));
    Consumer {
        config,
        metadata,
        pool,
        state: Arc::new(SyncMutex::new(SubscriptionState::default())),
        commit_gate: Arc::new(tokio::sync::Mutex::new(())),
        closed: AtomicBool::new(false),
        wakeup_flag: AtomicBool::new(false),
        wakeup_notify: tokio::sync::Notify::new(),
        group_coordinator: None,
        metrics,
        metrics_source,
        telemetry: Telemetry::disabled(),
        rebalance_listener: Arc::new(NoOpRebalanceListener),
        interceptor: Arc::new(crate::interceptor::NoOpConsumerInterceptor),
        key_deserializer: None,
        value_deserializer: None,
    }
}

/// Assign `orders-0` at position 0 with `count` records buffered.
fn buffer_records(consumer: &Consumer, count: i64) {
    let mut state = consumer.state.lock();
    let key = ("orders".to_string(), 0);
    state.add_partitions([key.clone()]);
    state.seek(&key, 0).unwrap();
    let target = state.fetch_targets(Instant::now()).remove(0);
    let records = (0..count)
        .map(|o| ConsumerRecord::new("orders", 0, o, None, Some(Bytes::from(vec![o as u8]))))
        .collect();
    assert!(state.install_fetch(
        &target,
        CompletedFetch {
            records,
            next_offset: count,
            next_epoch: None,
        },
    ));
}

fn position(consumer: &Consumer) -> Option<Offset> {
    consumer
        .state
        .lock()
        .partition(&("orders".to_string(), 0))
        .and_then(|r| r.position)
}

#[test]
fn the_consumer_is_send_and_sync_and_its_stream_is_send() {
    fn assert_send_sync<T: Send + Sync>() {}
    fn assert_send<T: Send>() {}
    assert_send_sync::<Consumer>();
    assert_send::<ConsumerStream<'_>>();
}

#[tokio::test]
async fn deliver_advances_the_position_past_what_it_returns() {
    let consumer = test_consumer();
    buffer_records(&consumer, 10);
    let records = consumer.deliver(4).unwrap();
    assert_eq!(
        records.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    assert_eq!(position(&consumer), Some(4));
    assert_eq!(consumer.metrics.buffered_records.get(), 6);
}

/// Fails on the value of the record at `fail_at`; uppercases nothing,
/// prefixes every other value with `d:`.
struct FailAt {
    fail_at: u8,
    calls: Arc<AtomicUsize>,
}

impl crate::serdes::Deserializer for FailAt {
    fn deserialize(
        &self,
        topic: &str,
        _headers: &crate::Headers,
        payload: Bytes,
        _is_key: bool,
    ) -> Result<Bytes> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if payload.first() == Some(&self.fail_at) {
            Err(KrafkaError::serialization(format!("bad record in {topic}")))
        } else {
            let mut out = b"d:".to_vec();
            out.extend_from_slice(&payload);
            Ok(Bytes::from(out))
        }
    }
}

/// A deserializer failure hands out the records before the failing one,
/// leaves the position at the failing record, and reports the failure on the
/// next call without double-decoding anything.
#[tokio::test]
async fn a_deserializer_failure_returns_the_records_before_it() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut consumer = test_consumer();
    consumer.value_deserializer = Some(Arc::new(FailAt {
        fail_at: 4,
        calls: calls.clone(),
    }));
    buffer_records(&consumer, 10);

    let first = consumer.deliver(10).unwrap();
    assert_eq!(
        first.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![0, 1, 2, 3]
    );
    assert!(
        first
            .iter()
            .all(|r| r.value.as_deref().unwrap().starts_with(b"d:"))
    );
    assert_eq!(position(&consumer), Some(4));

    let error = consumer.deliver(10).unwrap_err();
    assert!(
        matches!(error, KrafkaError::RecordDeserialization { .. }),
        "{error:?}"
    );
    assert_eq!(position(&consumer), Some(4));

    consumer.seek("orders", 0, 5).await.unwrap();
    assert_eq!(
        consumer.state.lock().buffered_count(),
        0,
        "a seek drops the buffer"
    );
}

#[tokio::test]
async fn seek_on_an_unassigned_partition_is_rejected() {
    let consumer = test_consumer();
    assert!(consumer.seek("orders", 0, 7).await.is_err());
    assert!(consumer.seek_to_beginning("orders", 0).await.is_err());
    assert!(consumer.seek_to_end("orders", 0).await.is_err());
    let many = [(TopicPartition::new("orders", 0), 3)];
    assert!(consumer.seek_many(many).await.is_err());
    assert!(consumer.seek_to_timestamp("orders", 0, 0).await.is_err());
    assert!(consumer.state.lock().assigned_keys().is_empty());
}

#[tokio::test]
async fn recv_returns_none_once_closed() {
    let consumer = test_consumer();
    consumer.closed.store(true, Ordering::SeqCst);
    assert!(consumer.recv().await.unwrap().is_none());
}

#[tokio::test]
async fn close_clears_the_state_and_is_idempotent() {
    let consumer = test_consumer();
    buffer_records(&consumer, 3);
    consumer.state.lock().subscription.insert("orders".into());
    consumer.pause("orders", &[0]).await;
    consumer.close().await.unwrap();
    assert!(consumer.is_closed());
    assert!(consumer.assignment().await.is_empty());
    assert!(consumer.subscription().await.is_empty());
    assert_eq!(consumer.metrics.buffered_records.get(), 0);
    assert_eq!(consumer.metrics.paused_partitions.get(), 0);
    consumer.close().await.unwrap();
}

#[tokio::test]
async fn unsubscribe_clears_the_state_and_is_idempotent() {
    let consumer = test_consumer();
    buffer_records(&consumer, 3);
    consumer.unsubscribe().await.unwrap();
    assert!(!consumer.is_closed());
    assert!(consumer.assignment().await.is_empty());
    assert_eq!(consumer.metrics.assigned_partitions.get(), 0);
    consumer.unsubscribe().await.unwrap();
}

#[test]
fn a_benign_close_commit_error_yields_to_the_leave_result() {
    let rebalance = || {
        Err(KrafkaError::broker(
            crate::error::ErrorCode::RebalanceInProgress,
            "rebalance",
        ))
    };
    assert!(Consumer::select_close_result(rebalance(), Ok(())).is_ok());
    let leave =
        Consumer::select_close_result(rebalance(), Err(KrafkaError::illegal_state("leave")));
    assert!(leave.unwrap_err().to_string().contains("leave"));
    let commit = Consumer::select_close_result(
        Err(KrafkaError::illegal_state("commit")),
        Err(KrafkaError::illegal_state("leave")),
    );
    assert!(commit.unwrap_err().to_string().contains("commit"));
}
