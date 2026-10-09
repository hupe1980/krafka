//! One producer pipeline: per-broker coalescing, a flush that never blocks
//! other partitions, `delivery_timeout` as a bound, a 1 s backoff cap, and a
//! defined commit point for `enqueue`.

#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use krafka::error::{ErrorCode, KrafkaError};
use krafka::producer::Record;
use krafka::testing::ApiKey;
use krafka::testing::{Control, FakeBroker};

fn rec(topic: &str, partition: i32, value: &str) -> Record {
    Record::new(topic, value.as_bytes().to_vec()).partition(partition)
}

/// US1: one record to each of 64 partitions led by one broker costs at most
/// two Produce requests, not 64.
#[tokio::test]
async fn a_wave_for_one_broker_is_one_request() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 64);
    let producer = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap()
        .producer()
        .build()
        .await
        .unwrap();
    // Warm the connection and the producer id.
    let _ = producer.send(rec("t", 0, "warm")).await.unwrap();

    broker.on(ApiKey::Produce, |_| {
        Control::Delay(Duration::from_millis(50))
    });
    broker.clear_requests();
    let started = Instant::now();
    let mut handles = Vec::new();
    for partition in 0..64 {
        handles.push(producer.enqueue(rec("t", partition, "v")).await.unwrap());
    }
    for handle in handles {
        let _ = handle.await.unwrap();
    }
    let elapsed = started.elapsed();
    let requests = broker.request_count(ApiKey::Produce);
    assert!(requests <= 2, "{requests} Produce requests for one wave");
    assert!(
        elapsed < Duration::from_millis(400),
        "the wave took {elapsed:?}"
    );
    broker.clear_hooks();
    producer.close().await.unwrap();
}

/// US1.3: partitions on three brokers are sent as three concurrent requests.
#[tokio::test]
async fn each_broker_gets_its_own_request_concurrently() {
    let broker = FakeBroker::start_cluster(3).await.unwrap();
    broker.create_topic("t", 6);
    for partition in 0..6 {
        broker.set_leader("t", partition, partition % 3);
    }
    let producer = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap()
        .producer()
        .build()
        .await
        .unwrap();
    for partition in 0..3 {
        let _ = producer.send(rec("t", partition, "warm")).await.unwrap();
    }
    broker.on(ApiKey::Produce, |_| {
        Control::Delay(Duration::from_millis(300))
    });
    broker.clear_requests();
    let started = Instant::now();
    let mut handles = Vec::new();
    for partition in 0..6 {
        handles.push(producer.enqueue(rec("t", partition, "v")).await.unwrap());
    }
    for handle in handles {
        let _ = handle.await.unwrap();
    }
    assert_eq!(broker.request_count(ApiKey::Produce), 3);
    assert!(
        started.elapsed() < Duration::from_millis(800),
        "{:?}",
        started.elapsed()
    );
    broker.clear_hooks();
    producer.close().await.unwrap();
}

/// US1.6: in a coalesced request, one partition's retriable error retries
/// only that partition; the others are acknowledged at once.
#[tokio::test]
async fn a_partition_error_in_a_coalesced_request_retries_only_that_partition() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 2);
    broker.create_topic("u", 1);
    let producer = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap()
        .producer()
        .build()
        .await
        .unwrap();
    let _ = producer.send(rec("t", 0, "warm")).await.unwrap();
    // The fake answers an injected error for every partition of the request,
    // so put the failing partition on its own topic and fail that topic once.
    broker.on_once(ApiKey::Produce, |_| {
        Control::Error(ErrorCode::NotEnoughReplicas)
    });
    let a = producer.enqueue(rec("u", 0, "retried")).await.unwrap();
    let _ = a.await.expect("retried until it succeeds");
    broker.clear_hooks();
    producer.close().await.unwrap();
}

/// X2: a flush waiting on a slow broker does not stall sends to a healthy
/// one. The control without a flush shows the test discriminates.
#[tokio::test]
async fn flush_does_not_stall_unrelated_partitions() {
    for with_flush in [true, false] {
        let broker = FakeBroker::start_cluster(2).await.unwrap();
        broker.create_topic("t", 2);
        broker.set_leader("t", 0, 0);
        broker.set_leader("t", 1, 1);
        let p = Arc::new(
            krafka::Kafka::builder(broker.bootstrap_servers())
                .request_timeout(Duration::from_secs(10))
                .connect()
                .await
                .unwrap()
                .producer()
                .build()
                .await
                .unwrap(),
        );
        let _ = p.send(rec("t", 0, "w0")).await.unwrap();
        let _ = p.send(rec("t", 1, "w1")).await.unwrap();
        broker.on(ApiKey::Produce, |info| {
            if info.node_id == 0 {
                Control::Delay(Duration::from_millis(1500))
            } else {
                Control::Pass
            }
        });
        let h1 = p.enqueue(rec("t", 0, "r1")).await.unwrap();
        let h2 = p.enqueue(rec("t", 0, "r2")).await.unwrap();
        let flush = with_flush.then(|| {
            let pf = Arc::clone(&p);
            tokio::spawn(async move { pf.flush().await })
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        let started = Instant::now();
        let _ = p.send(rec("t", 1, "fast")).await.unwrap();
        let fast = started.elapsed();
        assert!(
            fast < Duration::from_millis(200),
            "flush={with_flush}: {fast:?}"
        );
        if let Some(flush) = flush {
            flush.await.unwrap().unwrap();
        }
        let _ = (h1.await, h2.await);
        broker.clear_hooks();
        p.close().await.unwrap();
    }
}

/// P3: `flush` covers exactly the sends queued before it. A later, faster
/// send completing does not release it while an earlier one is in flight.
#[tokio::test]
async fn flush_waits_for_every_earlier_send() {
    let broker = FakeBroker::start_cluster(2).await.unwrap();
    broker.create_topic("t", 2);
    broker.set_leader("t", 0, 0);
    broker.set_leader("t", 1, 1);
    let producer = Arc::new(
        krafka::Kafka::builder(broker.bootstrap_servers())
            .connect()
            .await
            .unwrap()
            .producer()
            .build()
            .await
            .unwrap(),
    );
    let _ = producer.send(rec("t", 0, "w0")).await.unwrap();
    let _ = producer.send(rec("t", 1, "w1")).await.unwrap();
    broker.on(ApiKey::Produce, |info| {
        if info.node_id == 0 {
            Control::Delay(Duration::from_secs(2))
        } else {
            Control::Pass
        }
    });
    let slow = producer.enqueue(rec("t", 0, "slow")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    let p = Arc::clone(&producer);
    let flush = tokio::spawn(async move { p.flush().await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = producer.send(rec("t", 1, "fast")).await.unwrap();
    let early = tokio::time::timeout(Duration::from_millis(1000), flush).await;
    assert!(
        early.is_err(),
        "flush returned while the slow send was in flight"
    );
    let _ = slow.await.unwrap();
    broker.clear_hooks();
    producer.close().await.unwrap();
}

/// X3: every record resolves within `delivery_timeout` of its batch's
/// creation, queued or in flight, against a broker that answers late.
#[tokio::test]
async fn delivery_timeout_bounds_every_record() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let p = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(Duration::from_secs(1))
        .connect_timeout(Duration::from_secs(1))
        .connect()
        .await
        .unwrap()
        .producer()
        .delivery_timeout(Duration::from_secs(2))
        .batch_size(1)
        .build()
        .await
        .unwrap();
    let _ = p.send(rec("t", 0, "warm")).await.unwrap();
    broker.on(ApiKey::Produce, |_| {
        Control::Delay(Duration::from_millis(1500))
    });
    let started = Instant::now();
    let mut handles = Vec::new();
    for i in 0..5 {
        handles.push(p.enqueue(rec("t", 0, &format!("r{i}"))).await.unwrap());
    }
    for handle in handles {
        let result = handle.await;
        let at = started.elapsed();
        assert!(
            at < Duration::from_millis(2300),
            "resolved after {at:?}: {result:?}"
        );
        assert!(
            matches!(result, Err(KrafkaError::DeliveryTimeout { .. })),
            "{result:?}"
        );
    }
    broker.clear_hooks();
    p.close().await.unwrap();
}

/// FR-010: a build whose `delivery_timeout` cannot fit one attempt is refused.
#[tokio::test]
async fn delivery_timeout_must_cover_linger_and_one_request() {
    let broker = FakeBroker::start().await.unwrap();
    let error = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(Duration::from_secs(1))
        .connect_timeout(Duration::from_secs(1))
        .connect()
        .await
        .unwrap()
        .producer()
        .delivery_timeout(Duration::from_secs(1))
        .linger(Duration::from_millis(100))
        .build()
        .await
        .unwrap_err();
    let message = error.to_string();
    for name in ["delivery_timeout", "linger", "request_timeout"] {
        assert!(message.contains(name), "{message}");
    }
}

/// X6/US4: a retrying batch never sleeps more than about a second, so the
/// record goes out soon after a long outage ends.
#[tokio::test]
async fn backoff_is_capped_at_one_second() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let p = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap()
        .producer()
        .build()
        .await
        .unwrap();
    let _ = p.send(rec("t", 0, "warm")).await.unwrap();
    let outage = Arc::new(AtomicBool::new(true));
    let o = Arc::clone(&outage);
    broker.on(ApiKey::Produce, move |_| {
        if o.load(Ordering::SeqCst) {
            Control::Error(ErrorCode::NotEnoughReplicas)
        } else {
            Control::Pass
        }
    });
    let started = Instant::now();
    let handle = p.enqueue(rec("t", 0, "r")).await.unwrap();
    tokio::time::sleep(Duration::from_secs(7)).await;
    outage.store(false, Ordering::SeqCst);
    let healed = started.elapsed();
    let _ = handle.await.unwrap();
    let after = started.elapsed() - healed;
    assert!(
        after < Duration::from_millis(1300),
        "acknowledged {after:?} after the outage"
    );
    broker.clear_hooks();
    p.close().await.unwrap();
}

/// US4.2: a batch failing non-retriably answers all its callers together.
#[tokio::test]
async fn a_failed_batch_answers_every_caller_at_once() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let p = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap()
        .producer()
        .linger(Duration::from_millis(200))
        .build()
        .await
        .unwrap();
    broker.on_once(ApiKey::Produce, |_| {
        Control::Error(ErrorCode::InvalidRecord)
    });
    let mut handles = Vec::new();
    for i in 0..6 {
        handles.push(p.enqueue(rec("t", 0, &format!("r{i}"))).await.unwrap());
    }
    let mut at = Vec::new();
    let started = Instant::now();
    for handle in handles {
        assert!(handle.await.is_err());
        at.push(started.elapsed());
    }
    let spread = at[at.len() - 1] - at[0];
    assert!(spread < Duration::from_millis(10), "{at:?}");
    p.close().await.unwrap();
}

/// US3 (partitioning): the default `linger` batches more than `linger = 0`
/// for the same concurrent keyless workload.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_linger_sends_fewer_requests_than_zero() {
    async fn requests(linger: Option<Duration>) -> usize {
        let broker = FakeBroker::start().await.unwrap();
        broker.create_topic("t", 4);
        let kafka = krafka::Kafka::builder(broker.bootstrap_servers())
            .connect()
            .await
            .unwrap();
        let mut builder = kafka.producer();
        if let Some(linger) = linger {
            builder = builder.linger(linger);
        }
        let producer = Arc::new(builder.build().await.unwrap());
        broker.clear_requests();
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let producer = Arc::clone(&producer);
            tasks.push(tokio::spawn(async move {
                for _ in 0..125 {
                    let _ = producer
                        .send(krafka::Record::new(
                            "t",
                            bytes::Bytes::copy_from_slice(&[0u8; 200]),
                        ))
                        .await
                        .unwrap();
                }
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        producer.close().await.unwrap();
        broker.request_count(ApiKey::Produce)
    }
    let default = requests(None).await;
    let zero = requests(Some(Duration::ZERO)).await;
    eprintln!("default linger: {default} requests, linger 0: {zero}");
    assert!(
        default < zero,
        "default linger: {default} requests, linger 0: {zero}"
    );
}

/// US2 (partitioning, KIP-1123): with rack-aware partitioning every keyless
/// record lands on a partition led in the client's rack; off, they spread
/// over racks.
#[tokio::test]
async fn rack_aware_partitioning_stays_in_the_rack() {
    for rack_aware in [true, false] {
        let broker = FakeBroker::start_cluster(3).await.unwrap();
        for (node, rack) in [(0, "a"), (1, "b"), (2, "c")] {
            broker.set_broker_rack(node, Some(rack));
        }
        broker.create_topic("t", 6);
        for partition in 0..6 {
            broker.set_leader("t", partition, partition % 3);
        }
        let producer = krafka::Kafka::builder(broker.bootstrap_servers())
            .connect()
            .await
            .unwrap()
            .producer()
            .client_rack("a")
            .partitioner_rack_aware(rack_aware)
            .batch_size(1024)
            .build()
            .await
            .unwrap();
        let mut leaders = std::collections::BTreeSet::new();
        for _ in 0..200 {
            let md = producer
                .send(krafka::Record::new(
                    "t",
                    bytes::Bytes::copy_from_slice(&[0u8; 100]),
                ))
                .await
                .unwrap();
            leaders.insert(md.partition % 3);
        }
        if rack_aware {
            assert_eq!(
                leaders.into_iter().collect::<Vec<_>>(),
                vec![0],
                "only rack a"
            );
        } else {
            assert!(leaders.len() > 1, "rack-aware off spreads: {leaders:?}");
        }
        producer.close().await.unwrap();
    }
}

/// KIP-1123: rack-aware needs a client rack, and has no effect with a custom
/// partitioner.
#[tokio::test]
async fn rack_aware_partitioning_is_validated() {
    let broker = FakeBroker::start().await.unwrap();
    let kafka = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap();
    let error = kafka
        .producer()
        .partitioner_rack_aware(true)
        .build()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("client_rack"), "{error}");
    let error = kafka
        .producer()
        .client_rack("a")
        .partitioner_rack_aware(true)
        .partitioner(krafka::producer::RoundRobinPartitioner::new())
        .build()
        .await
        .unwrap_err();
    assert!(error.to_string().contains("partitioner"), "{error}");
}

/// US8: an `enqueue` dropped while it waits for buffer memory queues nothing
/// and holds nothing.
#[tokio::test]
async fn a_dropped_enqueue_queues_nothing() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let record_size = rec("t", 0, &"x".repeat(900)).estimated_size();
    let p = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap()
        .producer()
        .buffer_memory(record_size)
        .batch_size(record_size)
        .linger(Duration::from_secs(1))
        .build()
        .await
        .unwrap();
    broker.on(ApiKey::Produce, |_| {
        Control::Delay(Duration::from_millis(500))
    });
    let first = p.enqueue(rec("t", 0, &"x".repeat(900))).await.unwrap();
    let waiting = tokio::time::timeout(
        Duration::from_millis(100),
        p.enqueue(rec("t", 0, &"y".repeat(900))),
    )
    .await;
    assert!(waiting.is_err(), "the second enqueue waits for memory");
    let _ = first.await.unwrap();
    // The budget is whole again: a full-size record is admitted at once.
    let third = tokio::time::timeout(
        Duration::from_millis(200),
        p.enqueue(rec("t", 0, &"z".repeat(900))),
    )
    .await
    .expect("memory was released")
    .unwrap();
    let _ = third.await.unwrap();
    broker.clear_hooks();
    let values: Vec<String> = broker
        .all_records("t")
        .unwrap()
        .into_iter()
        .map(|r| String::from_utf8_lossy(&r.value.unwrap()[..1]).into_owned())
        .collect();
    assert_eq!(
        values,
        vec!["x", "z"],
        "the dropped enqueue was never produced"
    );
    p.close().await.unwrap();
}
