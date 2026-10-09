//! Every `# Cancel safety` claim on the data path, checked by dropping the
//! future at every point it can be dropped.
//!
//! Each case builds a fresh in-memory cluster under a paused clock, polls the
//! method's future `k` times and drops it, for every `k` from 0 (never
//! polled) to `k_max` (the pending polls an undisturbed call takes), then
//! continues as a caller would and checks what the method's section
//! promises. `k_max` is recorded per case, must be the same on two runs, and
//! must not be 0 for a method that does I/O: a case that never drops its
//! future mid-flight proves nothing.
//!
//! `xtask/cancel_safety.py` requires every method documented cancel safe to
//! be named by a case here, by its `Type::method` string.

#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::Duration;

use futures::StreamExt as _;

use krafka::consumer::{AutoOffsetReset, Consumer, ConsumerRecord};
use krafka::error::KrafkaError;
use krafka::interceptor::{InterceptorResult, ProducerInterceptor, RecordContext};
use krafka::producer::{
    DeliveryHandle, Producer, Record, RecordMetadata, TopicPartitionOffset, TransactionState,
    TransactionalProducer,
};
use krafka::share_consumer::{AcknowledgementMode, ShareConsumer};
use krafka::testing::{ApiKey, Control, FakeBroker, ShareAckType};
use krafka::{Headers, PartitionId};

/// A method's future, borrowing the case's state.
type Call<'a, O> = Pin<Box<dyn Future<Output = O> + 'a>>;

/// Longest simulated time a check may take; past it the check is reported
/// as not completing (a lost wake-up, a futurelock).
const CHECK_LIMIT: Duration = Duration::from_secs(120);

/// Let every other task run; on the paused clock this also lets timers due
/// within the next millisecond fire.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(1)).await;
}

/// Poll to completion, counting the pending polls.
async fn run_to_end<O>(mut fut: Call<'_, O>) -> (usize, O) {
    let mut pending = 0;
    loop {
        match futures::poll!(fut.as_mut()) {
            Poll::Ready(out) => return (pending, out),
            Poll::Pending => {
                pending += 1;
                settle().await;
            }
        }
    }
}

/// Poll `k` times, then drop the future unless it finished first.
async fn poll_then_drop<O>(mut fut: Call<'_, O>, k: usize) -> Option<O> {
    for _ in 0..k {
        if let Poll::Ready(out) = futures::poll!(fut.as_mut()) {
            return Some(out);
        }
        settle().await;
    }
    None
}

/// Run `call` on fresh state, dropping its future before the first poll and
/// after every pending poll, and `check` the state after each run (with the
/// output when the call finished). Returns `k_max`.
async fn drop_at_every_poll<S, O>(
    case: &str,
    setup: impl AsyncFn() -> S,
    call: impl for<'a> Fn(&'a S) -> Call<'a, O>,
    check: impl AsyncFn(S, Option<O>),
) -> usize {
    let checked = async |state: S, out: Option<O>, k: &str| {
        println!("{case}: checking after {k}");
        tokio::time::timeout(CHECK_LIMIT, check(state, out))
            .await
            .unwrap_or_else(|_| panic!("{case}: the caller's next steps hang after {k}"));
    };
    let measure = async || {
        let state = setup().await;
        let (k_max, out) = run_to_end(call(&state)).await;
        checked(state, Some(out), "an undisturbed call").await;
        k_max
    };
    let k_max = measure().await;
    assert_eq!(measure().await, k_max, "{case}: k_max differs between runs");
    assert!(
        k_max > 0,
        "{case}: completed on its first poll, so no drop happened mid-flight"
    );
    for k in 0..=k_max {
        let state = setup().await;
        let out = poll_then_drop(call(&state), k).await;
        checked(state, out, &format!("a drop at k = {k}")).await;
    }
    println!("{case}: k_max = {k_max}");
    k_max
}

// ── Producers ──────────────────────────────────────────────────────────────

/// Counts the interceptor's two callbacks: every `on_send` owes exactly one
/// `on_acknowledgement`.
#[derive(Debug, Default)]
struct Calls {
    sent: AtomicUsize,
    acknowledged: AtomicUsize,
}

#[derive(Debug, Clone, Default)]
struct Counter(Arc<Calls>);

impl ProducerInterceptor for Counter {
    fn on_send(&self, _: &mut Record, _: &mut RecordContext) -> InterceptorResult {
        self.0.sent.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    fn on_acknowledgement(
        &self,
        _: &str,
        _: PartitionId,
        _: Result<&RecordMetadata, &KrafkaError>,
        _: &Headers,
        _: &mut RecordContext,
    ) -> InterceptorResult {
        self.0.acknowledged.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

impl Counter {
    fn assert_paired(&self) {
        assert_eq!(
            self.0.sent.load(Ordering::SeqCst),
            self.0.acknowledged.load(Ordering::SeqCst),
            "every on_send gets exactly one on_acknowledgement"
        );
    }
}

const PAD: usize = 400;

/// A record of `PAD` bytes whose value starts with `name`.
fn record(name: &str) -> Record {
    let mut value = name.as_bytes().to_vec();
    value.resize(PAD, b'.');
    Record::new("t", value).partition(0)
}

fn count(broker: &FakeBroker, name: &str, committed: bool) -> usize {
    let records = if committed {
        broker.committed_records("t")
    } else {
        broker.all_records("t")
    };
    records
        .unwrap()
        .iter()
        .filter(|r| {
            r.value
                .as_deref()
                .is_some_and(|v| v.starts_with(name.as_bytes()))
        })
        .count()
}

/// A producer whose buffer holds two records, with both already queued and
/// every produce answered after 30 ms: the next enqueue waits for memory.
struct Produce {
    broker: FakeBroker,
    producer: Producer,
    counter: Counter,
    queued: Vec<DeliveryHandle>,
    /// A handle for the `DeliveryHandle` case to take.
    x: parking_lot::Mutex<Option<DeliveryHandle>>,
}

async fn produce_setup() -> Produce {
    let broker = FakeBroker::start_in_memory(1);
    broker.create_topic("t", 1);
    broker.on(ApiKey::Produce, |_| {
        Control::Delay(Duration::from_millis(30))
    });
    let counter = Counter::default();
    let producer = broker
        .kafka()
        .connect()
        .await
        .unwrap()
        .producer()
        .linger(Duration::ZERO)
        .batch_size(PAD + 100)
        .buffer_memory(2 * PAD + 200)
        .interceptor(counter.clone())
        .build()
        .await
        .unwrap();
    let mut queued = Vec::new();
    for name in ["a", "b"] {
        queued.push(producer.enqueue(record(name)).await.unwrap());
    }
    Produce {
        broker,
        producer,
        counter,
        queued,
        x: parking_lot::Mutex::new(None),
    }
}

/// After the call: the queued records are delivered, flush and close
/// complete, and the callbacks pair up. Returns how often `x` was written.
async fn produce_finish(state: Produce) -> usize {
    for handle in state.queued {
        let _ = handle.await.unwrap();
    }
    state.producer.flush().await.unwrap();
    state.producer.close().await.unwrap();
    state.counter.assert_paired();
    assert_eq!(count(&state.broker, "a", false), 1);
    assert_eq!(count(&state.broker, "b", false), 1);
    count(&state.broker, "x", false)
}

/// `enqueue` resolves exactly at the enqueue: a dropped call queued nothing,
/// a finished one is delivered once.
#[tokio::test(start_paused = true)]
async fn producer_enqueue() {
    drop_at_every_poll(
        "Producer::enqueue",
        produce_setup,
        |s| Box::pin(s.producer.enqueue(record("x"))),
        async |state, out| {
            let handle = out.map(Result::unwrap);
            let enqueued = handle.is_some();
            if let Some(handle) = handle {
                let _ = handle.await.unwrap();
            }
            let written = produce_finish(state).await;
            assert_eq!(
                written,
                usize::from(enqueued),
                "x written {written} times, enqueued: {enqueued}"
            );
        },
    )
    .await;
}

/// `send` is not cancel safe: a drop before the record is queued sends
/// nothing; a drop after it does not cancel the delivery.
#[tokio::test(start_paused = true)]
async fn producer_send() {
    let k_max = drop_at_every_poll(
        "Producer::send",
        produce_setup,
        |s| Box::pin(s.producer.send(record("x"))),
        async |state, out| {
            let finished = out.map(Result::unwrap).is_some();
            let written = produce_finish(state).await;
            assert!(written <= 1, "x written {written} times");
            if finished {
                assert_eq!(written, 1);
            }
        },
    )
    .await;
    // Dropped on its last pending poll the record is long queued: it is
    // still delivered.
    let state = produce_setup().await;
    assert!(
        poll_then_drop(Box::pin(state.producer.send(record("x"))), k_max)
            .await
            .is_none()
    );
    assert_eq!(
        produce_finish(state).await,
        1,
        "a dropped send cancelled its delivery"
    );
}

/// `flush` is cancel safe: a later flush and close complete and every
/// record is delivered once.
#[tokio::test(start_paused = true)]
async fn producer_flush() {
    drop_at_every_poll(
        "Producer::flush",
        produce_setup,
        |s| Box::pin(s.producer.flush()),
        async |state, out| {
            if let Some(result) = out {
                result.unwrap();
            }
            assert_eq!(produce_finish(state).await, 0);
        },
    )
    .await;
}

/// `close` is not cancel safe: dropped, the producer is closed and its
/// records are still delivered; calling close again returns at once.
#[tokio::test(start_paused = true)]
async fn producer_close() {
    drop_at_every_poll(
        "Producer::close",
        produce_setup,
        |s| Box::pin(s.producer.close()),
        async |state, out| {
            if let Some(result) = out {
                result.unwrap();
            }
            state.producer.close().await.unwrap();
            assert!(state.producer.is_closed());
            for handle in state.queued {
                let _ = handle.await.unwrap();
            }
            state.counter.assert_paired();
        },
    )
    .await;
}

/// A `DeliveryHandle` is cancel safe: dropping it discards the outcome, not
/// the record.
#[tokio::test(start_paused = true)]
async fn delivery_handle() {
    let enqueued = async || {
        let state = produce_setup().await;
        // Queued behind `a` and `b`: its answer comes after theirs.
        let x = state.producer.enqueue(record("x")).await.unwrap();
        *state.x.lock() = Some(x);
        state
    };
    drop_at_every_poll(
        "DeliveryHandle",
        enqueued,
        |s| {
            let x = s.x.lock().take().expect("x was enqueued");
            Box::pin(x)
        },
        async |state, out| {
            if let Some(result) = out {
                let _ = result.unwrap();
            }
            assert_eq!(produce_finish(state).await, 1, "x delivered once");
        },
    )
    .await;
}

/// A transactional producer with the same memory-bound buffer, a
/// transaction open and `AddPartitionsToTxn` answered after 30 ms.
struct Txn {
    broker: FakeBroker,
    producer: TransactionalProducer,
    counter: Counter,
    queued: Vec<DeliveryHandle>,
}

async fn txn_setup() -> Txn {
    let broker = FakeBroker::start_in_memory(1);
    broker.create_topic("t", 1);
    broker.on(ApiKey::Produce, |_| {
        Control::Delay(Duration::from_millis(30))
    });
    broker.on(ApiKey::AddPartitionsToTxn, |_| {
        Control::Delay(Duration::from_millis(30))
    });
    let counter = Counter::default();
    let producer = broker
        .kafka()
        .connect()
        .await
        .unwrap()
        .producer()
        .linger(Duration::ZERO)
        .batch_size(PAD + 100)
        .buffer_memory(2 * PAD + 200)
        .interceptor(counter.clone())
        .build_transactional("tx")
        .await
        .unwrap();
    producer.begin().unwrap();
    Txn {
        broker,
        producer,
        counter,
        queued: Vec::new(),
    }
}

/// [`txn_setup`] with two records queued, so the next enqueue waits for
/// memory.
async fn txn_full_setup() -> Txn {
    let mut state = txn_setup().await;
    for name in ["a", "b"] {
        state
            .queued
            .push(state.producer.enqueue(record(name)).await.unwrap());
    }
    state
}

/// Commit what the transaction holds; returns the cluster and how often `x`
/// is visible.
async fn txn_finish(state: Txn) -> (FakeBroker, usize) {
    for handle in state.queued {
        let _ = handle.await.unwrap();
    }
    if matches!(
        state.producer.state(),
        TransactionState::Open | TransactionState::CommitUnknown
    ) {
        state.producer.commit().await.unwrap();
    }
    state.producer.close().await.unwrap();
    state.counter.assert_paired();
    let x = count(&state.broker, "x", true);
    (state.broker, x)
}

/// `enqueue` on a transactional producer: the same contract.
#[tokio::test(start_paused = true)]
async fn transactional_enqueue() {
    drop_at_every_poll(
        "TransactionalProducer::enqueue",
        txn_full_setup,
        |s| Box::pin(s.producer.enqueue(record("x"))),
        async |state, out| {
            let handle = out.map(Result::unwrap);
            let enqueued = handle.is_some();
            if let Some(handle) = handle {
                let _ = handle.await.unwrap();
            }
            let written = txn_finish(state).await.1;
            assert_eq!(
                written,
                usize::from(enqueued),
                "x written {written} times, enqueued: {enqueued}"
            );
        },
    )
    .await;
}

/// `send` dropped anywhere, including inside `AddPartitionsToTxn`: the next
/// send to the partition proceeds and the transaction commits.
#[tokio::test(start_paused = true)]
async fn transactional_send() {
    drop_at_every_poll(
        "TransactionalProducer::send",
        txn_setup,
        |s| Box::pin(s.producer.send(record("x"))),
        async |state, out| {
            let finished = out.map(Result::unwrap).is_some();
            let y = state.producer.send(record("y")).await.unwrap();
            assert_eq!(y.partition, 0);
            let (broker, written) = txn_finish(state).await;
            assert!(written <= 1, "x written {written} times");
            if finished {
                assert_eq!(written, 1);
            }
            assert_eq!(count(&broker, "y", true), 1);
        },
    )
    .await;
}

/// `flush` on a transactional producer is cancel safe.
#[tokio::test(start_paused = true)]
async fn transactional_flush() {
    drop_at_every_poll(
        "TransactionalProducer::flush",
        txn_full_setup,
        |s| Box::pin(s.producer.flush()),
        async |state, out| {
            if let Some(result) = out {
                result.unwrap();
            }
            state.producer.flush().await.unwrap();
            assert_eq!(txn_finish(state).await.1, 0);
        },
    )
    .await;
}

/// A transaction with one record written, its `EndTxn` answered after
/// 30 ms.
async fn txn_written_setup() -> Txn {
    let state = txn_setup().await;
    let _ = state.producer.send(record("x")).await.unwrap();
    state.broker.on(ApiKey::EndTxn, |_| {
        Control::Delay(Duration::from_millis(30))
    });
    state
}

/// `commit` is not cancel safe; the documented recovery, committing again,
/// commits the transaction exactly once.
#[tokio::test(start_paused = true)]
async fn transactional_commit() {
    drop_at_every_poll(
        "TransactionalProducer::commit",
        txn_written_setup,
        |s| Box::pin(s.producer.commit()),
        async |state, out| {
            match out {
                Some(result) => result.unwrap(),
                None => state.producer.commit().await.unwrap(),
            }
            assert_eq!(state.producer.state(), TransactionState::Ready);
            state.producer.begin().unwrap();
            let _ = state.producer.send(record("z")).await.unwrap();
            state.producer.commit().await.unwrap();
            let (broker, x) = txn_finish(state).await;
            assert_eq!(x, 1, "x committed once");
            assert_eq!(count(&broker, "z", true), 1);
        },
    )
    .await;
}

/// `abort` is not cancel safe; the documented recovery, aborting again,
/// leaves nothing visible and the producer usable.
#[tokio::test(start_paused = true)]
async fn transactional_abort() {
    drop_at_every_poll(
        "TransactionalProducer::abort",
        txn_written_setup,
        |s| Box::pin(s.producer.abort()),
        async |state, out| {
            match out {
                Some(result) => result.unwrap(),
                None => state.producer.abort().await.unwrap(),
            }
            state.producer.begin().unwrap();
            let _ = state.producer.send(record("z")).await.unwrap();
            let (broker, x) = txn_finish(state).await;
            assert_eq!(x, 0, "the aborted x is not visible");
            assert_eq!(count(&broker, "z", true), 1);
        },
    )
    .await;
}

/// A transaction with a record written and a group member to send offsets
/// for, `TxnOffsetCommit` answered after 30 ms.
struct Offsets {
    txn: Txn,
    consumer: Consumer,
}

async fn offsets_setup() -> Offsets {
    let txn = txn_setup().await;
    let _ = txn.producer.send(record("x")).await.unwrap();
    let consumer = txn
        .broker
        .kafka()
        .connect()
        .await
        .unwrap()
        .consumer("g")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    consumer.subscribe(["t"]).await.unwrap();
    consumer.poll(Duration::from_millis(100)).await.unwrap();
    txn.broker.on(ApiKey::TxnOffsetCommit, |_| {
        Control::Delay(Duration::from_millis(30))
    });
    Offsets { txn, consumer }
}

fn offsets() -> Vec<TopicPartitionOffset> {
    vec![TopicPartitionOffset::new("t", 0, 7)]
}

/// `send_offsets` is not cancel safe: dropped after it started, the
/// transaction can no longer commit, and the documented recovery, aborting,
/// leaves neither the records nor the offsets committed.
#[tokio::test(start_paused = true)]
async fn transactional_send_offsets() {
    drop_at_every_poll(
        "TransactionalProducer::send_offsets",
        offsets_setup,
        |s| {
            Box::pin(async move {
                let group = s.consumer.group_metadata().await.unwrap();
                s.txn.producer.send_offsets(&offsets(), &group).await
            })
        },
        async |state, out| {
            let sent = matches!(out, Some(Ok(())));
            let committed = match state.txn.producer.commit().await {
                Ok(()) => true,
                Err(error) => {
                    assert!(!sent, "{error}");
                    assert!(error.requires_abort(), "{error}");
                    state.txn.producer.abort().await.unwrap();
                    false
                }
            };
            let (broker, x) = txn_finish(state.txn).await;
            assert_eq!(x, usize::from(committed));
            if sent {
                assert_eq!(broker.committed_offset("g", "t", 0), Some(7));
            } else if !committed {
                assert_eq!(broker.committed_offset("g", "t", 0), None);
            }
            state.consumer.close().await.unwrap();
        },
    )
    .await;
}

/// `close` on a transactional producer is not cancel safe: dropped, the
/// producer is closed; calling it again returns.
#[tokio::test(start_paused = true)]
async fn transactional_close() {
    drop_at_every_poll(
        "TransactionalProducer::close",
        txn_full_setup,
        |s| Box::pin(s.producer.close()),
        async |state, out| {
            if let Some(result) = out {
                result.unwrap();
            }
            state.producer.close().await.unwrap();
            assert!(state.producer.is_closed());
            for handle in state.queued {
                let _ = handle.await;
            }
            state.counter.assert_paired();
        },
    )
    .await;
}

// ── Consumer ───────────────────────────────────────────────────────────────

const RECORDS: i64 = 10;

/// A consumer assigned partition 0 of a topic holding `RECORDS` records,
/// every fetch answered after 20 ms, at most three records per poll.
struct Consume {
    broker: FakeBroker,
    consumer: Consumer,
}

async fn consume_setup(group: Option<&str>) -> Consume {
    let broker = FakeBroker::start_in_memory(1);
    broker.create_topic("t", 1);
    let kafka = broker.kafka().connect().await.unwrap();
    let producer = kafka.producer().build().await.unwrap();
    for i in 0..RECORDS {
        let _ = producer.send(record(&format!("r{i}"))).await.unwrap();
    }
    producer.close().await.unwrap();
    broker.on(ApiKey::Fetch, |_| Control::Delay(Duration::from_millis(20)));
    let builder = match group {
        Some(group) => kafka.consumer(group),
        None => kafka.consumer_without_group(),
    };
    let consumer = builder
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .max_poll_records(3)
        .build()
        .await
        .unwrap();
    match group {
        Some(_) => consumer.subscribe(["t"]).await.unwrap(),
        None => consumer.assign("t", vec![0]).await.unwrap(),
    }
    Consume { broker, consumer }
}

async fn assigned() -> Consume {
    consume_setup(None).await
}

/// Read the rest after the call: the records every call returned, in
/// order, are offsets `0..RECORDS` exactly once, and the position never
/// passed a record not yet returned.
async fn consume_finish(state: Consume, returned: Vec<ConsumerRecord>) {
    let mut offsets: Vec<i64> = returned.iter().map(|r| r.offset).collect();
    let position = state.consumer.position("t", 0).await.unwrap_or(0);
    assert!(
        position <= offsets.len() as i64,
        "position {position} passed records not returned ({offsets:?})"
    );
    while (offsets.len() as i64) < RECORDS {
        let record = state.consumer.recv().await.unwrap().unwrap();
        offsets.push(record.offset);
    }
    assert_eq!(
        offsets,
        (0..RECORDS).collect::<Vec<_>>(),
        "a gap or a duplicate"
    );
    state.consumer.close().await.unwrap();
}

/// `Consumer::poll` is cancel safe.
#[tokio::test(start_paused = true)]
async fn consumer_poll() {
    drop_at_every_poll(
        "Consumer::poll",
        assigned,
        |s| Box::pin(s.consumer.poll(Duration::from_secs(5))),
        async |state, out| consume_finish(state, out.map(Result::unwrap).unwrap_or_default()).await,
    )
    .await;
}

/// `Consumer::recv` is cancel safe.
#[tokio::test(start_paused = true)]
async fn consumer_recv() {
    drop_at_every_poll(
        "Consumer::recv",
        assigned,
        |s| Box::pin(s.consumer.recv()),
        async |state, out| {
            let returned = out.map(|r| r.unwrap().unwrap()).into_iter().collect();
            consume_finish(state, returned).await;
        },
    )
    .await;
}

/// `ConsumerStream` is cancel safe.
#[tokio::test(start_paused = true)]
async fn consumer_stream() {
    drop_at_every_poll(
        "ConsumerStream",
        assigned,
        |s| Box::pin(async move { s.consumer.stream().next().await }),
        async |state, out| {
            let returned = out.map(|r| r.unwrap().unwrap()).into_iter().collect();
            consume_finish(state, returned).await;
        },
    )
    .await;
}

/// A group member that has read three records, its `OffsetCommit` answered
/// after 30 ms.
async fn committing() -> Consume {
    let state = consume_setup(Some("g")).await;
    let mut got = 0;
    while got < 3 {
        got += state
            .consumer
            .poll(Duration::from_secs(1))
            .await
            .unwrap()
            .len();
    }
    state.broker.on(ApiKey::OffsetCommit, |_| {
        Control::Delay(Duration::from_millis(30))
    });
    state
}

/// `Consumer::commit` is not cancel safe; the recovery, committing again,
/// commits the position.
#[tokio::test(start_paused = true)]
async fn consumer_commit() {
    drop_at_every_poll(
        "Consumer::commit",
        committing,
        |s| Box::pin(s.consumer.commit()),
        async |state, out| {
            if !matches!(out, Some(Ok(()))) {
                state.consumer.commit().await.unwrap();
            }
            let position = state.consumer.position("t", 0).await.unwrap();
            assert_eq!(state.broker.committed_offset("g", "t", 0), Some(position));
            state.consumer.close().await.unwrap();
        },
    )
    .await;
}

/// `Consumer::commit_offsets` is not cancel safe; committing again commits
/// the offsets.
#[tokio::test(start_paused = true)]
async fn consumer_commit_offsets() {
    let offsets = || {
        let mut offsets = ahash::AHashMap::new();
        offsets.insert(
            krafka::consumer::TopicPartition::new("t", 0),
            krafka::consumer::OffsetAndMetadata::new(2),
        );
        offsets
    };
    drop_at_every_poll(
        "Consumer::commit_offsets",
        committing,
        |s| Box::pin(async move { s.consumer.commit_offsets(&offsets()).await }),
        async |state, out| {
            if !matches!(out, Some(Ok(()))) {
                state.consumer.commit_offsets(&offsets()).await.unwrap();
            }
            assert_eq!(state.broker.committed_offset("g", "t", 0), Some(2));
            state.consumer.close().await.unwrap();
        },
    )
    .await;
}

/// `Consumer::close` is not cancel safe: dropped, the consumer is closed;
/// calling it again returns.
#[tokio::test(start_paused = true)]
async fn consumer_close() {
    drop_at_every_poll(
        "Consumer::close",
        committing,
        |s| Box::pin(s.consumer.close()),
        async |state, out| {
            if let Some(result) = out {
                result.unwrap();
            }
            state.consumer.close().await.unwrap();
            assert!(state.consumer.is_closed());
            assert!(state.consumer.recv().await.unwrap().is_none());
        },
    )
    .await;
}

// ── Share consumer ─────────────────────────────────────────────────────────

const SHARE_RECORDS: i64 = 6;

struct Share {
    broker: FakeBroker,
    consumer: ShareConsumer,
    returned: Vec<ConsumerRecord>,
}

async fn share_setup(mode: AcknowledgementMode) -> Share {
    let broker = FakeBroker::start_in_memory(1);
    broker.create_topic("t", 1);
    let kafka = broker.kafka().connect().await.unwrap();
    let producer = kafka.producer().build().await.unwrap();
    for i in 0..SHARE_RECORDS {
        let _ = producer.send(record(&format!("s{i}"))).await.unwrap();
    }
    producer.close().await.unwrap();
    broker.on(ApiKey::ShareFetch, |_| {
        Control::Delay(Duration::from_millis(20))
    });
    broker.on(ApiKey::ShareAcknowledge, |_| {
        Control::Delay(Duration::from_millis(20))
    });
    let consumer = kafka
        .share_consumer("sg")
        .acknowledgement_mode(mode)
        // Explicit mode settles everything one poll returned before the
        // next: one poll returns the whole topic.
        .max_poll_records(if mode == AcknowledgementMode::Explicit {
            6
        } else {
            2
        })
        .build()
        .await
        .unwrap();
    consumer.subscribe(["t"]).await.unwrap();
    Share {
        broker,
        consumer,
        returned: Vec::new(),
    }
}

async fn implicit() -> Share {
    share_setup(AcknowledgementMode::Implicit).await
}

fn acks(broker: &FakeBroker) -> BTreeMap<i64, Vec<ShareAckType>> {
    broker.share_acknowledgements("sg", "t", 0)
}

/// Read the rest in implicit mode, then close: every record was returned to
/// the application exactly once and accepted exactly once, and none was
/// accepted without having been returned.
async fn share_finish(state: Share, mut returned: Vec<ConsumerRecord>) {
    while (returned.len() as i64) < SHARE_RECORDS {
        returned.extend(state.consumer.poll(Duration::from_secs(5)).await.unwrap());
    }
    let mut offsets: Vec<i64> = returned.iter().map(|r| r.offset).collect();
    offsets.sort_unstable();
    assert_eq!(
        offsets,
        (0..SHARE_RECORDS).collect::<Vec<_>>(),
        "records returned to the application: {offsets:?}"
    );
    state.consumer.close().await.unwrap();
    assert_accepted_once(&acks(&state.broker));
}

/// Every record of the topic was accepted exactly once, and nothing else
/// was applied to it.
fn assert_accepted_once(log: &BTreeMap<i64, Vec<ShareAckType>>) {
    for offset in 0..SHARE_RECORDS {
        assert_eq!(
            log.get(&offset).map(Vec::as_slice),
            Some(&[ShareAckType::Accept][..]),
            "offset {offset}: {log:?}"
        );
    }
}

/// `ShareConsumer::poll` is cancel safe: a dropped poll returns nothing and
/// accepts nothing; its records come with a later call.
#[tokio::test(start_paused = true)]
async fn share_poll() {
    drop_at_every_poll(
        "ShareConsumer::poll",
        implicit,
        |s| Box::pin(s.consumer.poll(Duration::from_secs(5))),
        async |state, out| share_finish(state, out.map(Result::unwrap).unwrap_or_default()).await,
    )
    .await;
}

/// `ShareConsumer::recv` is cancel safe.
#[tokio::test(start_paused = true)]
async fn share_recv() {
    drop_at_every_poll(
        "ShareConsumer::recv",
        implicit,
        |s| Box::pin(s.consumer.recv()),
        async |state, out| {
            let returned = out.map(|r| r.unwrap().unwrap()).into_iter().collect();
            share_finish(state, returned).await;
        },
    )
    .await;
}

/// `ShareConsumerStream` is cancel safe.
#[tokio::test(start_paused = true)]
async fn share_stream() {
    drop_at_every_poll(
        "ShareConsumerStream",
        implicit,
        |s| Box::pin(async move { s.consumer.stream().next().await }),
        async |state, out| {
            let returned = out.map(|r| r.unwrap().unwrap()).into_iter().collect();
            share_finish(state, returned).await;
        },
    )
    .await;
}

/// Explicit mode with every record returned by one poll and settled —
/// accepted, rejected, released or renewed — and not yet committed.
async fn settled() -> Share {
    let mut state = share_setup(AcknowledgementMode::Explicit).await;
    while state.returned.is_empty() {
        let batch = state.consumer.poll(Duration::from_secs(5)).await.unwrap();
        assert!(
            batch.is_empty() || batch.len() as i64 == SHARE_RECORDS,
            "{batch:?}"
        );
        for record in &batch {
            match record.offset {
                2 => state.consumer.reject(record).unwrap(),
                3 => state.consumer.release(record).unwrap(),
                4 => state.consumer.renew(record).unwrap(),
                _ => state.consumer.ack(record).unwrap(),
            }
        }
        state.returned.extend(batch);
    }
    state
}

/// `ShareConsumer::commit` with pending `ack`, `release`, `reject` and
/// `renew` is cancel safe: every acknowledgement the application made
/// reaches the broker exactly once, whether the commit finished, was
/// dropped and retried, or was dropped and left to the next commit.
#[tokio::test(start_paused = true)]
async fn share_commit() {
    drop_at_every_poll(
        "ShareConsumer::commit, ShareConsumer::ack, ShareConsumer::release, \
         ShareConsumer::reject, ShareConsumer::renew",
        settled,
        |s| Box::pin(s.consumer.commit()),
        async |state, out| {
            if let Some(results) = out {
                assert!(results.unwrap().values().all(Result::is_ok));
            }
            // The renewed record is settled after the renewal went out.
            let renewed = state.returned.iter().find(|r| r.offset == 4).unwrap();
            state.consumer.commit().await.unwrap();
            state.consumer.ack(renewed).unwrap();
            state.consumer.close().await.unwrap();
            let log = acks(&state.broker);
            use ShareAckType::{Accept, Reject, Release, Renew};
            for (offset, expected) in [
                (0, &[Accept][..]),
                (1, &[Accept]),
                (2, &[Reject]),
                (4, &[Renew, Accept]),
                (5, &[Accept]),
            ] {
                assert_eq!(
                    log.get(&offset).map(Vec::as_slice),
                    Some(expected),
                    "{log:?}"
                );
            }
            // Released, it may be fetched again and released again on close;
            // never accepted.
            let three = log.get(&3).map(Vec::as_slice).unwrap_or_default();
            assert!(
                three.first() == Some(&Release) && three.iter().all(|a| *a == Release),
                "{log:?}"
            );
        },
    )
    .await;
}

/// `ShareConsumer::close_with` is not cancel safe: dropped, the consumer is
/// closed; calling it again returns.
#[tokio::test(start_paused = true)]
async fn share_close() {
    drop_at_every_poll(
        "ShareConsumer::close_with",
        settled,
        |s| Box::pin(s.consumer.close_with(krafka::CloseOptions::new())),
        async |state, out| {
            if let Some(result) = out {
                result.unwrap();
            }
            state.consumer.close().await.unwrap();
            assert!(state.consumer.is_closed());
            assert!(state.consumer.recv().await.unwrap().is_none());
        },
    )
    .await;
}

/// Negative control for the share checks: a log with a record accepted
/// twice, or one never accepted, fails them.
#[test]
fn a_planted_double_acknowledgement_fails_the_check() {
    use ShareAckType::Accept;
    let once: BTreeMap<i64, Vec<ShareAckType>> =
        (0..SHARE_RECORDS).map(|o| (o, vec![Accept])).collect();
    assert_accepted_once(&once);
    let mut twice = once.clone();
    twice.insert(3, vec![Accept, Accept]);
    assert!(std::panic::catch_unwind(|| assert_accepted_once(&twice)).is_err());
    let mut missing = once;
    missing.remove(&3);
    assert!(std::panic::catch_unwind(|| assert_accepted_once(&missing)).is_err());
}
