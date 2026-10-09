//! A transaction commits all of its sends or none, and an unknown commit
//! stays unknown. Every scenario runs under TV1 and TV2 unless it is about a
//! TV1-only mechanism.

#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use krafka::error::{ErrorCode, KrafkaError};
use krafka::interceptor::{InterceptorResult, ProducerInterceptor};
use krafka::producer::{Record, TransactionState, TransactionalProducer};
use krafka::testing::ApiKey;
use krafka::testing::{Control, FakeBroker};

use support::{all_values, committed_values};

const TV: [i16; 2] = [1, 2];

async fn broker(tv: i16) -> FakeBroker {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);
    if tv >= 2 {
        broker.set_transaction_version(tv);
    }
    broker
}

async fn txn_producer(broker: &FakeBroker, id: &str) -> TransactionalProducer {
    krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(Duration::from_secs(2))
        .connect_timeout(Duration::from_secs(2))
        .connect()
        .await
        .expect("connect")
        .producer()
        .max_block(Duration::from_secs(10))
        .build_transactional(id)
        .await
        .expect("connect")
}

fn rec(value: &str) -> Record {
    Record::new("orders", value.as_bytes().to_vec()).partition(0)
}

/// T1: a send answered `INVALID_RECORD` makes the commit refuse with
/// `TransactionAbortable`; after the abort nothing of the transaction is
/// visible. The same whether or not the failed handle was awaited.
#[tokio::test]
async fn a_failed_send_makes_the_commit_refuse() {
    for tv in TV {
        for await_handle in [true, false] {
            let broker = broker(tv).await;
            let p = txn_producer(&broker, "t1").await;
            p.begin().unwrap();
            let _ = p.send(rec("r1")).await.expect("r1");
            broker.on_once(ApiKey::Produce, |_| {
                Control::Error(ErrorCode::InvalidRecord)
            });
            let r2 = p.enqueue(rec("r2")).await.expect("r2 queued");
            if await_handle {
                assert!(r2.await.is_err(), "TV{tv}: r2 fails");
            } else {
                drop(r2);
                p.flush().await.unwrap();
            }
            let refused = p.send(rec("r3")).await.unwrap_err();
            assert!(
                refused.requires_abort(),
                "TV{tv}: later sends refused: {refused}"
            );

            let commit = p.commit().await.unwrap_err();
            assert!(commit.requires_abort(), "TV{tv}: {commit}");
            assert!(
                commit.to_string().contains("InvalidRecord"),
                "carries r2's error: {commit}"
            );
            assert_eq!(p.state(), TransactionState::Open);
            p.abort().await.expect("abort");
            assert!(committed_values(&broker, "orders").is_empty(), "TV{tv}");

            // The failure is per transaction.
            p.begin().unwrap();
            let _ = p.send(rec("next")).await.expect("next");
            p.commit().await.expect("the next transaction commits");
            assert_eq!(committed_values(&broker, "orders"), vec!["next"], "TV{tv}");
            p.close().await.unwrap();
        }
    }
}

/// T1b: five consecutive `NOT_ENOUGH_REPLICAS` on defaults are retried until
/// they succeed; nothing counts retries.
#[tokio::test]
async fn transient_errors_are_retried_within_the_delivery_timeout() {
    for tv in TV {
        let broker = broker(tv).await;
        let p = txn_producer(&broker, "t1b").await;
        p.begin().unwrap();
        let _ = p.send(rec("r1")).await.unwrap();
        broker.on_times(ApiKey::Produce, 5, |_| {
            Control::Error(ErrorCode::NotEnoughReplicas)
        });
        let _ = p.send(rec("r2")).await.expect("r2 succeeds after retries");
        p.commit().await.unwrap();
        assert_eq!(
            committed_values(&broker, "orders"),
            vec!["r1", "r2"],
            "TV{tv}"
        );
        p.close().await.unwrap();
    }
}

/// X1: a send refused because the transaction is committing does not let the
/// commit overtake an earlier send that is still in flight.
#[tokio::test]
async fn a_refused_send_does_not_let_the_commit_overtake_an_in_flight_one() {
    for tv in TV {
        for race in [true, false] {
            let broker = FakeBroker::start_cluster(2).await.unwrap();
            broker.create_topic("orders", 1);
            broker.set_leader("orders", 0, 0);
            broker.set_txn_coordinator("x1", 1);
            if tv >= 2 {
                broker.set_transaction_version(tv);
            }
            let p = krafka::Kafka::builder(broker.bootstrap_servers())
                .request_timeout(Duration::from_secs(5))
                .connect_timeout(Duration::from_secs(2))
                .connect()
                .await
                .unwrap()
                .producer()
                .build_transactional("x1")
                .await
                .unwrap();
            p.begin().unwrap();
            let _ = p.send(rec("r0")).await.unwrap();
            broker.on(ApiKey::Produce, |info| {
                if info.node_id == 0 {
                    Control::Delay(Duration::from_millis(2000))
                } else {
                    Control::Pass
                }
            });
            let r1 = p.enqueue(rec("r1")).await.unwrap();
            let started = Instant::now();
            let done = AtomicBool::new(false);
            let commit = async {
                let result = p.commit().await;
                done.store(true, Ordering::SeqCst);
                (result, started.elapsed())
            };
            let interferer = async {
                if !race {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
                while !done.load(Ordering::SeqCst) {
                    let _ = p.send(rec("late")).await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            };
            let ((result, elapsed), ()) = tokio::join!(commit, interferer);
            result.expect("commit");
            assert!(
                elapsed >= Duration::from_millis(1900),
                "TV{tv} race={race}: the commit waited for r1 ({elapsed:?})"
            );
            let _ = r1.await.expect("r1 acknowledged");
            assert_eq!(
                committed_values(&broker, "orders"),
                vec!["r0", "r1"],
                "TV{tv} race={race}"
            );
            broker.clear_hooks();
            p.close().await.unwrap();
        }
    }
}

/// Abort fails buffered records instead of sending them; only records on
/// the wire are awaited.
#[tokio::test]
async fn abort_fails_buffered_records_without_sending_them() {
    for tv in TV {
        let broker = broker(tv).await;
        let p = krafka::Kafka::builder(broker.bootstrap_servers())
            .connect()
            .await
            .unwrap()
            .producer()
            .linger(Duration::from_secs(5))
            .delivery_timeout(Duration::from_secs(60))
            .build_transactional("abort-unsent")
            .await
            .unwrap();
        p.begin().unwrap();
        let buffered = p.enqueue(rec("buffered")).await.unwrap();
        let started = Instant::now();
        p.abort().await.expect("abort");
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "abort did not wait for linger"
        );
        let error = buffered.await.unwrap_err();
        assert!(error.requires_abort(), "TV{tv}: {error}");
        assert!(
            all_values(&broker, "orders").is_empty(),
            "TV{tv}: nothing was sent"
        );
        p.close().await.unwrap();
    }
}

/// X7/X8: a commit sends buffered records itself instead of waiting out
/// `linger`.
#[tokio::test]
async fn commit_does_not_wait_out_linger() {
    for tv in TV {
        let broker = broker(tv).await;
        let p = krafka::Kafka::builder(broker.bootstrap_servers())
            .connect()
            .await
            .unwrap()
            .producer()
            .linger(Duration::from_secs(2))
            .build_transactional("x8")
            .await
            .unwrap();
        p.begin().unwrap();
        let handle = p.enqueue(rec("r1")).await.unwrap();
        let started = Instant::now();
        p.commit().await.unwrap();
        let took = started.elapsed();
        let _ = handle.await.unwrap();
        assert!(
            took < Duration::from_millis(500),
            "TV{tv}: commit took {took:?}"
        );
        assert_eq!(committed_values(&broker, "orders"), vec!["r1"]);
        p.close().await.unwrap();
    }
}

/// T2: the first `EndTxn(commit)` is applied but answered after the request
/// timeout; every later attempt fails without an answer from `EndTxn`. The
/// commit reports `CommitUnknown`, and abort is refused without a request.
#[tokio::test]
async fn an_unanswered_commit_stays_unknown_and_refuses_abort() {
    for tv in TV {
        for later in [
            ErrorCode::CoordinatorNotAvailable,
            ErrorCode::NotCoordinator,
        ] {
            let broker = broker(tv).await;
            let p = krafka::Kafka::builder(broker.bootstrap_servers())
                .request_timeout(Duration::from_secs(2))
                .connect_timeout(Duration::from_secs(2))
                .connect()
                .await
                .unwrap()
                .producer()
                .max_block(Duration::from_secs(4))
                .build_transactional("t2")
                .await
                .unwrap();
            p.begin().unwrap();
            let _ = p.send(rec("r1")).await.unwrap();

            broker.on_once(ApiKey::EndTxn, |_| {
                Control::Delay(Duration::from_millis(2300))
            });
            if later == ErrorCode::CoordinatorNotAvailable {
                let seen = broker.request_count(ApiKey::FindCoordinator) as u64;
                broker.on(ApiKey::FindCoordinator, move |info| {
                    if info.api_call_index >= seen {
                        Control::Error(later)
                    } else {
                        Control::Pass
                    }
                });
            } else {
                broker.on(ApiKey::EndTxn, move |_| Control::Error(later));
            }

            let commit = p.commit().await;
            assert!(commit.is_err(), "TV{tv} {later:?}");
            assert_eq!(
                p.state(),
                TransactionState::CommitUnknown,
                "TV{tv} {later:?}"
            );
            assert_eq!(
                committed_values(&broker, "orders"),
                vec!["r1"],
                "it was applied"
            );

            broker.clear_hooks();
            let end_txn_before = broker.request_count(ApiKey::EndTxn);
            let abort = p.abort().await;
            assert!(abort.is_err(), "TV{tv}: abort refused");
            assert_eq!(
                broker.request_count(ApiKey::EndTxn),
                end_txn_before,
                "nothing sent"
            );

            // A retried commit resolves it.
            p.commit().await.expect("the retried commit succeeds");
            assert_eq!(p.state(), TransactionState::Ready);
            assert_eq!(
                committed_values(&broker, "orders"),
                vec!["r1"],
                "exactly once"
            );
            p.close().await.unwrap();
        }
    }
}

/// US4.3 control: every attempt answered with a definitive error and none
/// unanswered — the transaction stays open with the error.
#[tokio::test]
async fn a_definitively_refused_commit_reverts_to_open() {
    for tv in TV {
        let broker = broker(tv).await;
        let p = krafka::Kafka::builder(broker.bootstrap_servers())
            .connect()
            .await
            .unwrap()
            .producer()
            .max_block(Duration::from_millis(800))
            .build_transactional("us43")
            .await
            .unwrap();
        p.begin().unwrap();
        let _ = p.send(rec("r1")).await.unwrap();
        broker.on(ApiKey::EndTxn, |_| {
            Control::Error(ErrorCode::ConcurrentTransactions)
        });
        assert!(p.commit().await.is_err());
        assert_eq!(p.state(), TransactionState::Open, "TV{tv}");
        broker.clear_hooks();
        p.abort().await.expect("abort is allowed");
        p.close().await.unwrap();
    }
}

/// US4.5 (TV1): after an `EndTxn` attempt went unanswered and a later one
/// succeeded, the next transaction starts on a bumped epoch, so the stale
/// attempt — released by the coordinator afterwards — is fenced instead of
/// committing the new transaction early.
#[tokio::test]
async fn tv1_bumps_the_epoch_after_an_unanswered_end_txn() {
    let broker = broker(1).await;
    let p = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(Duration::from_secs(1))
        .connect_timeout(Duration::from_secs(1))
        .connect()
        .await
        .unwrap()
        .producer()
        .build_transactional("us45")
        .await
        .unwrap();
    let epoch_before = p.producer_epoch();
    p.begin().unwrap();
    let _ = p.send(rec("first")).await.unwrap();

    // Attempt 1 is applied, then its answer is lost.
    broker.on_once(ApiKey::EndTxn, |_| {
        Control::ApplyThen(Box::new(Control::Disconnect))
    });
    p.commit().await.expect("the retry succeeds");
    assert_eq!(p.state(), TransactionState::Ready);

    p.begin().unwrap();
    let _ = p.send(rec("second")).await.unwrap();
    assert!(
        p.producer_epoch() > epoch_before,
        "the next transaction runs under a bumped epoch ({} -> {})",
        epoch_before,
        p.producer_epoch()
    );
    let (_, coordinator_epoch) = broker.transactional_producer("us45").unwrap();
    assert_eq!(coordinator_epoch, p.producer_epoch());
    // The stale commit, carrying the old epoch, cannot end this transaction.
    assert!(broker.transaction_is_open("us45"));
    assert_eq!(committed_values(&broker, "orders"), vec!["first"]);
    p.commit().await.unwrap();
    assert_eq!(committed_values(&broker, "orders"), vec!["first", "second"]);
    p.close().await.unwrap();
}

/// Under TV1 the client never negotiates `EndTxn` v5, which a coordinator
/// reads as TV2.
#[tokio::test]
async fn tv1_never_sends_end_txn_v5() {
    let broker = broker(1).await;
    let p = txn_producer(&broker, "tv1-v5").await;
    p.begin().unwrap();
    let _ = p.send(rec("r")).await.unwrap();
    p.commit().await.unwrap();
    let versions: Vec<i16> = broker
        .requests()
        .iter()
        .filter(|r| r.api_key == ApiKey::EndTxn)
        .map(|r| r.api_version)
        .collect();
    assert!(
        !versions.is_empty() && versions.iter().all(|v| *v < 5),
        "{versions:?}"
    );
    p.close().await.unwrap();
}

#[derive(Debug, Default)]
struct CloseCounter(AtomicUsize);

impl ProducerInterceptor for CloseCounter {
    fn close(&self) -> InterceptorResult {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

/// X6: both producers close their interceptors exactly once.
#[tokio::test]
async fn both_producers_close_their_interceptors_once() {
    let broker = FakeBroker::start().await.unwrap();
    let txn_counter = Arc::new(CloseCounter::default());
    let p = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap()
        .producer()
        .interceptor(txn_counter.clone())
        .build_transactional("x6")
        .await
        .unwrap();
    p.close().await.unwrap();
    p.close().await.unwrap();
    assert_eq!(
        txn_counter.0.load(Ordering::SeqCst),
        1,
        "close() closes them"
    );
    drop(p);
    assert_eq!(
        txn_counter.0.load(Ordering::SeqCst),
        1,
        "and drop does not again"
    );

    let plain_counter = Arc::new(CloseCounter::default());
    let plain = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap()
        .producer()
        .interceptor(plain_counter.clone())
        .build()
        .await
        .unwrap();
    plain.close().await.unwrap();
    plain.close().await.unwrap();
    assert_eq!(plain_counter.0.load(Ordering::SeqCst), 1);
    drop(plain);
    assert_eq!(plain_counter.0.load(Ordering::SeqCst), 1);
}

/// A fenced producer ends: its next send fails as `Fenced`.
#[tokio::test]
async fn a_fenced_producer_reports_fenced() {
    let broker = broker(1).await;
    let zombie = txn_producer(&broker, "fenced").await;
    let successor = txn_producer(&broker, "fenced").await;
    zombie.begin().unwrap();
    let outcome = match zombie.send(rec("zombie")).await {
        Err(error) => error,
        Ok(_) => zombie.commit().await.unwrap_err(),
    };
    assert!(
        matches!(outcome, KrafkaError::Fenced { .. }) || outcome.is_fatal(),
        "{outcome}"
    );
    successor.close().await.unwrap();
}
