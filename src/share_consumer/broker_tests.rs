//! Share consumer behaviour against the in-process fake broker, which
//! validates share-session epochs, rejects acknowledgements at epoch 0 and of
//! records the member does not hold (`INVALID_RECORD_STATE`), rejects a join
//! without a subscription, and serves `ShareFetch`/`ShareAcknowledge` v2.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use parking_lot::Mutex;

use super::*;
use crate::consumer::TopicPartition;
use crate::error::ErrorCode;
use crate::producer::{Producer, Record};
use crate::protocol::ApiKey;
use crate::testing::{Control, FakeBroker, SharePartitionState};

const T: Duration = Duration::from_secs(2);
const SETTLE: Duration = Duration::from_secs(10);

async fn producer(broker: &FakeBroker) -> Producer {
    crate::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .producer()
        .linger(Duration::from_millis(50))
        .build()
        .await
        .unwrap()
}

/// Produce `values` to partition 0 as one record batch.
async fn produce_one_batch(broker: &FakeBroker, topic: &str, values: &[&[u8]]) {
    let p = producer(broker).await;
    let mut handles = Vec::new();
    for v in values {
        let record = Record::new(topic, Bytes::copy_from_slice(v)).partition(0);
        handles.push(p.enqueue(record).await.unwrap());
    }
    for h in handles {
        let _ = h.await.unwrap();
    }
    p.close().await.unwrap();
}

async fn builder(
    broker: &FakeBroker,
    group: &str,
    mode: AcknowledgementMode,
) -> ShareConsumerBuilder {
    crate::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .share_consumer(group)
        .acknowledgement_mode(mode)
}

async fn share(broker: &FakeBroker, group: &str, mode: AcknowledgementMode) -> ShareConsumer {
    builder(broker, group, mode).await.build().await.unwrap()
}

async fn drain(c: &ShareConsumer, want: usize) -> Vec<ConsumerRecord> {
    let deadline = tokio::time::Instant::now() + SETTLE;
    let mut got = Vec::new();
    while got.len() < want && tokio::time::Instant::now() < deadline {
        got.extend(c.poll(Duration::from_millis(200)).await.unwrap());
    }
    got
}

fn partition_state(broker: &FakeBroker, group: &str, topic: &str, p: i32) -> SharePartitionState {
    broker.with_state(|s| {
        s.share_groups
            .get(group)
            .and_then(|g| g.partitions.get(&(topic.to_string(), p)))
            .cloned()
            .unwrap_or_default()
    })
}

async fn eventually(what: &str, mut check: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + SETTLE;
    while !check() {
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn tp(topic: &str, partition: i32) -> TopicPartition {
    TopicPartition::new(topic, partition)
}

// ── only acquired records reach the application ───────────────────────

/// The broker returns the whole batch 0..=9 on the second fetch, but only
/// 5..=9 are acquired; 0..=4 were accepted. Control: without the
/// acquired-range filter, 0..=4 are redelivered and accepting them fails
/// with `INVALID_RECORD_STATE`.
#[tokio::test]
async fn only_records_inside_acquired_ranges_are_delivered() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let vals: Vec<Vec<u8>> = (0..10u8).map(|i| vec![b'v', i]).collect();
    let refs: Vec<&[u8]> = vals.iter().map(Vec::as_slice).collect();
    produce_one_batch(&broker, "t", &refs).await;

    let c = share(&broker, "g1", AcknowledgementMode::Explicit).await;
    c.subscribe(&["t"]).await.unwrap();
    let first = drain(&c, 10).await;
    assert_eq!(first.len(), 10);
    for r in &first {
        if r.offset < 5 { c.ack(r) } else { c.release(r) }.unwrap();
    }
    let results = c.commit().await.unwrap();
    assert!(results[&tp("t", 0)].is_ok());
    assert_eq!(partition_state(&broker, "g1", "t", 0).start_offset, 5);

    let second = drain(&c, 5).await;
    let offsets: Vec<i64> = second.iter().map(|r| r.offset).collect();
    assert_eq!(offsets, vec![5, 6, 7, 8, 9]);
    assert!(second.iter().all(|r| r.delivery_count == Some(2)));
    for r in &second {
        c.ack(r).unwrap();
    }
    let results = c.commit().await.unwrap();
    assert!(
        results[&tp("t", 0)].is_ok(),
        "no acknowledgement of an unacquired offset: {results:?}"
    );
    assert_eq!(partition_state(&broker, "g1", "t", 0).start_offset, 10);
    c.close().await.unwrap();
}

/// A transaction's commit marker is not delivered; its offset is
/// GAP-acknowledged, so the share-partition start offset moves past it.
/// Control: delivering control records hands the application a second
/// record here.
#[tokio::test]
async fn transaction_markers_are_skipped_and_gap_acknowledged() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let p = crate::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .producer()
        .build_transactional("tx")
        .await
        .unwrap();
    p.begin().unwrap();
    let _ = p.send(crate::Record::new("t", "payload")).await.unwrap();
    p.commit().await.unwrap();
    p.close().await.unwrap();

    let c = share(&broker, "g8", AcknowledgementMode::Implicit).await;
    c.subscribe(&["t"]).await.unwrap();
    let recs = drain(&c, 1).await;
    // Give a wrongly delivered marker the chance to show up.
    let more = c.poll(Duration::from_millis(300)).await.unwrap();
    assert_eq!(
        recs.len() + more.len(),
        1,
        "only the data record is delivered"
    );
    assert_eq!(recs[0].value.as_deref(), Some(&b"payload"[..]));

    let results = c.commit().await.unwrap();
    assert!(results.values().all(Result::is_ok), "{results:?}");
    eventually("the marker is archived", || {
        partition_state(&broker, "g8", "t", 0).start_offset == 2
    })
    .await;
    c.close().await.unwrap();
}

// ── acknowledgements are sent once, at a valid epoch, to their node ──

/// A connection reset with an acknowledgement pending: the session is
/// reopened by a `ShareFetch` without acknowledgements, and the
/// acknowledgement follows at epoch 1. Control: piggybacking it on the
/// opening request makes the broker answer `INVALID_REQUEST` and the commit
/// report it.
#[tokio::test]
async fn acknowledgements_wait_for_a_reopened_session() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    produce_one_batch(&broker, "t", &[b"x"]).await;

    let c = share(&broker, "g-reset", AcknowledgementMode::Explicit).await;
    c.subscribe(&["t"]).await.unwrap();
    let recs = drain(&c, 1).await;

    broker.on_once(ApiKey::ShareAcknowledge, |_| Control::Disconnect);
    c.ack(&recs[0]).unwrap();
    let results = c.commit().await.unwrap();
    assert!(results[&tp("t", 0)].is_ok(), "{results:?}");
    assert_eq!(partition_state(&broker, "g-reset", "t", 0).start_offset, 1);
    c.close().await.unwrap();
}

/// Records acquired from node 0, leadership moved to node 1: the
/// acknowledgement fails locally with `NOT_LEADER_OR_FOLLOWER` and is never
/// sent to node 1. Control: routing by current leader sends it to node 1,
/// which answers `INVALID_RECORD_STATE`.
#[tokio::test]
async fn acknowledgements_of_a_moved_partition_fail_locally() {
    let broker = FakeBroker::start_cluster(2).await.unwrap();
    broker.create_topic("t", 1);
    broker.set_leader("t", 0, 0);
    broker.set_group_coordinator("g-move", 0);
    produce_one_batch(&broker, "t", &[b"x"]).await;

    let c = share(&broker, "g-move", AcknowledgementMode::Explicit).await;
    c.subscribe(&["t"]).await.unwrap();
    let recs = drain(&c, 1).await;

    broker.set_leader("t", 0, 1);
    c.0.metadata.force_refresh(Some(&["t"])).await.unwrap();
    broker.clear_requests();
    c.ack(&recs[0]).unwrap();
    let results = c.commit().await.unwrap();
    assert!(
        matches!(
            results[&tp("t", 0)],
            Err(KrafkaError::Broker {
                code: ErrorCode::NotLeaderForPartition,
                ..
            })
        ),
        "{results:?}"
    );
    assert!(
        broker.request_nodes(ApiKey::ShareAcknowledge).is_empty(),
        "nothing was sent to either node"
    );
    c.close().await.unwrap();
}

/// `INVALID_RECORD_STATE` on one partition is reported for that partition
/// once, not retried, and the other partition commits. Later
/// acknowledgements on the failed partition succeed.
#[tokio::test]
async fn a_refused_acknowledgement_is_reported_per_partition_and_dropped() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 2);
    let p = producer(&broker).await;
    for partition in 0..2 {
        let record = Record::new("t", Bytes::from_static(b"v")).partition(partition);
        let _ = p.send(record).await.unwrap();
    }
    p.close().await.unwrap();

    let outcomes: Arc<Mutex<Vec<AcknowledgementCommit>>> = Arc::default();
    let seen = Arc::clone(&outcomes);
    let c = builder(&broker, "g-irs", AcknowledgementMode::Explicit)
        .await
        .acknowledgement_commit_callback(move |c| seen.lock().push(c.clone()))
        .build()
        .await
        .unwrap();
    c.subscribe(&["t"]).await.unwrap();
    let recs = drain(&c, 2).await;
    assert_eq!(recs.len(), 2);

    // The broker forgets partition 1's acquisition, as after lock expiry.
    broker.with_state(|s| {
        if let Some(p) = s
            .share_groups
            .get_mut("g-irs")
            .and_then(|g| g.partitions.get_mut(&("t".to_string(), 1)))
        {
            p.acquired.clear();
        }
    });
    broker.clear_requests();
    for r in &recs {
        c.ack(r).unwrap();
    }
    let results = c.commit().await.unwrap();
    assert_eq!(results.len(), 2, "{results:?}");
    assert!(results[&tp("t", 0)].is_ok());
    assert!(matches!(
        results[&tp("t", 1)],
        Err(KrafkaError::Broker {
            code: ErrorCode::InvalidRecordState,
            ..
        })
    ));
    assert_eq!(
        broker.request_count(ApiKey::ShareAcknowledge),
        1,
        "a refused acknowledgement is not retried"
    );
    {
        let outcomes = outcomes.lock();
        let failed: Vec<_> = outcomes.iter().filter(|o| o.result.is_err()).collect();
        assert_eq!(failed.len(), 1, "{outcomes:?}");
        assert_eq!(failed[0].partition, 1);
        assert_eq!(failed[0].offsets.len(), 1);
        assert_eq!(*failed[0].offsets[0].start(), 0);
        assert_eq!(*failed[0].offsets[0].end(), 0);
    }

    // The released record comes back and can be acknowledged.
    let again = drain(&c, 1).await;
    assert_eq!(again[0].partition, 1);
    c.ack(&again[0]).unwrap();
    let results = c.commit().await.unwrap();
    assert!(results[&tp("t", 1)].is_ok(), "{results:?}");
    assert!(
        c.commit().await.unwrap().is_empty(),
        "nothing left to commit"
    );
    c.close().await.unwrap();
}

/// `commit()` and `poll()` running against one broker never share a session
/// epoch: the broker would answer `INVALID_SHARE_SESSION_EPOCH`. Control: not
/// advancing the epoch after a `ShareAcknowledge` fails it.
#[tokio::test]
async fn concurrent_commit_and_poll_never_reuse_an_epoch() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let p = producer(&broker).await;
    for i in 0..40u8 {
        let _ = p
            .send(crate::Record::new("t", bytes::Bytes::copy_from_slice(&[i])))
            .await
            .unwrap();
    }
    p.close().await.unwrap();

    let c = builder(&broker, "g-race", AcknowledgementMode::Implicit)
        .await
        .max_poll_records(3)
        .build()
        .await
        .unwrap();
    c.subscribe(&["t"]).await.unwrap();
    let mut delivered = 0;
    let deadline = tokio::time::Instant::now() + SETTLE;
    while delivered < 40 && tokio::time::Instant::now() < deadline {
        let (polled, committed) = tokio::join!(c.poll(Duration::from_millis(100)), c.commit());
        delivered += polled.unwrap().len();
        let committed = committed.unwrap();
        assert!(committed.values().all(Result::is_ok), "{committed:?}");
    }
    assert_eq!(delivered, 40);
    let results = c.commit().await.unwrap();
    assert!(results.values().all(Result::is_ok), "{results:?}");
    assert_eq!(partition_state(&broker, "g-race", "t", 0).start_offset, 40);
    c.close().await.unwrap();
}

/// A `poll()` cancelled while its fetch is in flight: the release is applied
/// once and the record is acquired once more, then delivered from the
/// buffer. Control: a per-poll fetch applies the release twice (delivery
/// count 3) and orphans the acquisition.
#[tokio::test]
async fn a_cancelled_poll_neither_resends_nor_orphans() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    produce_one_batch(&broker, "t", &[b"x"]).await;

    let c = share(&broker, "g6", AcknowledgementMode::Explicit).await;
    c.subscribe(&["t"]).await.unwrap();
    let recs = drain(&c, 1).await;
    assert_eq!(recs[0].delivery_count, Some(1));
    c.release(&recs[0]).unwrap();
    assert!(c.commit().await.unwrap()[&tp("t", 0)].is_ok());

    broker.on_once(ApiKey::ShareFetch, |_| {
        Control::Delay(Duration::from_millis(800))
    });
    let r = tokio::time::timeout(Duration::from_millis(200), c.poll(Duration::from_secs(1))).await;
    assert!(r.is_err(), "poll cancelled by the timeout");
    tokio::time::sleep(Duration::from_millis(1000)).await;

    let again = drain(&c, 1).await;
    assert_eq!(again.len(), 1);
    assert_eq!(again[0].offset, 0);
    assert_eq!(again[0].delivery_count, Some(2));
    c.close().await.unwrap();
}

// ── a deserialization failure does not take its neighbours with it ───

struct FailOn(&'static [u8]);

impl crate::serdes::Deserializer for FailOn {
    fn deserialize(
        &self,
        _topic: &str,
        _headers: &crate::Headers,
        payload: Bytes,
        _is_key: bool,
    ) -> Result<Bytes> {
        if payload.as_ref() == self.0 {
            Err(KrafkaError::illegal_state("poison"))
        } else {
            Ok(payload)
        }
    }
}

const FIVE: [&[u8]; 5] = [b"r0", b"r1", b"bad", b"r3", b"r4"];

async fn poll_offsets(c: &ShareConsumer) -> Result<Vec<i64>> {
    let deadline = tokio::time::Instant::now() + SETTLE;
    loop {
        let records = c.poll(Duration::from_millis(200)).await?;
        if !records.is_empty() || tokio::time::Instant::now() > deadline {
            return Ok(records.iter().map(|r| r.offset).collect());
        }
    }
}

/// Implicit mode: records 0–1, then the error for offset 2, then 3–4. No
/// record is accepted before it was delivered. Control: accepting before
/// deserializing accepts 3–4 undelivered.
#[tokio::test]
async fn implicit_mode_returns_the_records_around_a_poison_record() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    produce_one_batch(&broker, "t", &FIVE).await;
    let c = builder(&broker, "g3", AcknowledgementMode::Implicit)
        .await
        .value_deserializer(Arc::new(FailOn(b"bad")))
        .build()
        .await
        .unwrap();
    c.subscribe(&["t"]).await.unwrap();

    assert_eq!(poll_offsets(&c).await.unwrap(), vec![0, 1]);
    let state = partition_state(&broker, "g3", "t", 0);
    assert!(
        state.archived.is_empty() && state.start_offset == 0,
        "{state:?}"
    );

    match c.poll(Duration::from_millis(200)).await {
        Err(KrafkaError::RecordDeserialization {
            partition, offset, ..
        }) => assert_eq!((partition, offset), (0, 2)),
        other => panic!("expected the poison record's error, got {other:?}"),
    }
    let state = partition_state(&broker, "g3", "t", 0);
    assert!(
        !state.archived.contains(&3) && !state.archived.contains(&4),
        "3 and 4 must not be accepted before they are delivered: {state:?}"
    );

    assert_eq!(poll_offsets(&c).await.unwrap(), vec![3, 4]);
    let _ = c.commit().await.unwrap();
    eventually("0, 1, 3 and 4 accepted; 2 released", || {
        let s = partition_state(&broker, "g3", "t", 0);
        s.start_offset == 2 && s.archived.contains(&3) && s.archived.contains(&4)
    })
    .await;
    c.close().await.unwrap();
}

/// Explicit mode: after acknowledging 0–1 and seeing the error, `poll()`
/// returns 3–4; it is not wedged on the poison record.
#[tokio::test]
async fn explicit_mode_is_not_wedged_by_a_poison_record() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    produce_one_batch(&broker, "t", &FIVE).await;
    let c = builder(&broker, "g4", AcknowledgementMode::Explicit)
        .await
        .value_deserializer(Arc::new(FailOn(b"bad")))
        .build()
        .await
        .unwrap();
    c.subscribe(&["t"]).await.unwrap();

    let deadline = tokio::time::Instant::now() + SETTLE;
    let first = loop {
        let records = c.poll(Duration::from_millis(200)).await.unwrap();
        if !records.is_empty() || tokio::time::Instant::now() > deadline {
            break records;
        }
    };
    assert_eq!(
        first.iter().map(|r| r.offset).collect::<Vec<_>>(),
        vec![0, 1]
    );
    for r in &first {
        c.ack(r).unwrap();
    }
    assert!(matches!(
        c.poll(Duration::from_millis(200)).await,
        Err(KrafkaError::RecordDeserialization { offset: 2, .. })
    ));
    assert_eq!(poll_offsets(&c).await.unwrap(), vec![3, 4]);
    c.close().await.unwrap();
}

/// Negative control: a deserializer that never fails delivers all five and
/// releases nothing.
#[tokio::test]
async fn without_a_poison_record_all_five_are_delivered_and_accepted() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    produce_one_batch(&broker, "t", &FIVE).await;
    let c = builder(&broker, "g3n", AcknowledgementMode::Implicit)
        .await
        .value_deserializer(Arc::new(FailOn(b"never")))
        .build()
        .await
        .unwrap();
    c.subscribe(&["t"]).await.unwrap();
    assert_eq!(poll_offsets(&c).await.unwrap(), vec![0, 1, 2, 3, 4]);
    let results = c.commit().await.unwrap();
    assert!(results[&tp("t", 0)].is_ok());
    assert_eq!(partition_state(&broker, "g3n", "t", 0).start_offset, 5);
    c.close().await.unwrap();
}

// ── membership ─────────────────────────────────────────────────────────

/// A member the coordinator forgot (`UNKNOWN_MEMBER_ID`) rejoins at epoch 0
/// and is assigned again. Control: without the handling it heartbeats
/// forever with its old epoch.
#[tokio::test]
async fn an_unknown_member_rejoins() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let c = share(&broker, "gm", AcknowledgementMode::Implicit).await;
    c.subscribe(&["t"]).await.unwrap();
    assert!(c.member_epoch() > 0);

    broker.with_state(|s| s.share_groups.get_mut("gm").unwrap().members.clear());
    eventually("the member rejoins", || {
        broker.with_state(|s| !s.share_groups["gm"].members.is_empty())
    })
    .await;
    eventually("an assignment arrives", || {
        !c.0.state.lock().assigned.is_empty()
    })
    .await;
    assert!(c.member_epoch() > 0);
    c.close().await.unwrap();
}

/// A fenced member rejoins at epoch 0 with its full subscription; the fake
/// broker rejects a join without one. Records flow again afterwards.
#[tokio::test]
async fn a_fenced_member_rejoins_with_its_subscription() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let c = share(&broker, "gf", AcknowledgementMode::Implicit).await;
    c.subscribe(&["t"]).await.unwrap();

    broker.on_once(ApiKey::ShareGroupHeartbeat, |_| {
        Control::Error(ErrorCode::FencedMemberEpoch)
    });
    let hb = broker.request_count(ApiKey::ShareGroupHeartbeat);
    eventually("the fence is seen and the member rejoins", || {
        broker.request_count(ApiKey::ShareGroupHeartbeat) >= hb + 2 && c.member_epoch() > 0
    })
    .await;
    produce_one_batch(&broker, "t", &[b"after"]).await;
    assert_eq!(drain(&c, 1).await.len(), 1);
    c.close().await.unwrap();
}

/// A partition whose leader moved: metadata is refreshed from the
/// `NOT_LEADER_OR_FOLLOWER` answer and fetching resumes on the new leader,
/// without a hot loop. Control: ignoring the error stalls the partition.
#[tokio::test]
async fn fetching_follows_a_leader_move() {
    let broker = FakeBroker::start_cluster(2).await.unwrap();
    broker.create_topic("t", 1);
    broker.set_leader("t", 0, 0);
    broker.set_group_coordinator("gl", 0);
    let c = share(&broker, "gl", AcknowledgementMode::Implicit).await;
    c.subscribe(&["t"]).await.unwrap();
    let _ = c.poll(Duration::from_millis(200)).await.unwrap();

    broker.set_leader("t", 0, 1);
    let p = producer(&broker).await;
    let _ = p.send(crate::Record::new("t", "after-move")).await.unwrap();
    p.close().await.unwrap();

    broker.clear_requests();
    let got = drain(&c, 1).await;
    assert_eq!(got.len(), 1, "the record on the new leader is delivered");
    let nodes = broker.request_nodes(ApiKey::ShareFetch);
    assert!(nodes.contains(&1), "fetched from the new leader: {nodes:?}");
    assert!(
        nodes.iter().filter(|&&n| n == 0).count() <= 20,
        "the old leader is not hammered: {nodes:?}"
    );
    c.close().await.unwrap();
}

/// A REJECT queued before a subscription change reaches the broker. (The
/// request manager may send it before the change lands; the revocation rule
/// itself is pinned by `an_assignment_change_drops_only_revoked_partitions`.)
#[tokio::test]
async fn an_assignment_change_keeps_a_queued_reject() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("a", 1);
    broker.create_topic("b", 1);
    produce_one_batch(&broker, "a", &[b"x"]).await;

    let c = share(&broker, "g5", AcknowledgementMode::Explicit).await;
    c.subscribe(&["a"]).await.unwrap();
    let recs = drain(&c, 1).await;
    c.reject(&recs[0]).unwrap();
    c.subscribe(&["a", "b"]).await.unwrap();
    let results = c.commit().await.unwrap();
    assert!(results.values().all(Result::is_ok), "{results:?}");
    eventually("the reject reached the broker", || {
        partition_state(&broker, "g5", "a", 0).start_offset == 1
    })
    .await;
    c.close().await.unwrap();
}

/// `poll()` does not heartbeat: only the background loop does, at the
/// coordinator's interval (1 s on the fake broker).
#[tokio::test]
async fn polling_does_not_heartbeat() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let c = share(&broker, "gh", AcknowledgementMode::Implicit).await;
    c.subscribe(&["t"]).await.unwrap();
    let before = broker.request_count(ApiKey::ShareGroupHeartbeat);
    for _ in 0..30 {
        let _ = c.poll(Duration::from_millis(10)).await.unwrap();
    }
    let heartbeats = broker.request_count(ApiKey::ShareGroupHeartbeat) - before;
    assert!(heartbeats <= 2, "{heartbeats} heartbeats during 30 polls");
    c.close().await.unwrap();
}

// ── acquisition bound, KIP-1206 and renew ─────────────────────────────

/// `max_poll_records(1)`: one `recv()` acquires one record of ten. Control:
/// a fixed `max_records = 5000` acquires all ten.
#[tokio::test]
async fn acquisition_is_bounded_by_max_poll_records() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let p = producer(&broker).await;
    for i in 0..10u8 {
        let _ = p
            .send(crate::Record::new("t", bytes::Bytes::copy_from_slice(&[i])))
            .await
            .unwrap();
    }
    p.close().await.unwrap();

    let c = builder(&broker, "g7", AcknowledgementMode::Implicit)
        .await
        .max_poll_records(1)
        .build()
        .await
        .unwrap();
    c.subscribe(&["t"]).await.unwrap();
    let r = tokio::time::timeout(SETTLE, c.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(r.is_some());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(partition_state(&broker, "g7", "t", 0).acquired.len(), 1);
    c.close().await.unwrap();
}

async fn acquired_after_one_poll(mode: AcquireMode) -> (usize, usize, Vec<i16>) {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let vals: Vec<Vec<u8>> = (0..10u8).map(|i| vec![i]).collect();
    let refs: Vec<&[u8]> = vals.iter().map(Vec::as_slice).collect();
    produce_one_batch(&broker, "t", &refs).await;

    let c = builder(&broker, "g-limit", AcknowledgementMode::Implicit)
        .await
        .max_poll_records(3)
        .acquire_mode(mode)
        .build()
        .await
        .unwrap();
    c.subscribe(&["t"]).await.unwrap();
    let got = drain(&c, 1).await.len();
    let acquired = partition_state(&broker, "g-limit", "t", 0).acquired.len();
    let versions = broker
        .requests()
        .iter()
        .filter(|r| r.api_key == ApiKey::ShareFetch)
        .map(|r| r.api_version)
        .collect();
    c.close().await.unwrap();
    (got, acquired, versions)
}

/// `RecordLimit` acquires exactly `max_poll_records` of a ten-record batch
/// with `ShareFetch` v2; the batch-optimized default finishes the batch.
#[tokio::test]
async fn record_limit_bounds_acquisition_exactly() {
    let (got, acquired, versions) = acquired_after_one_poll(AcquireMode::RecordLimit).await;
    assert_eq!((got, acquired), (3, 3));
    assert!(versions.iter().all(|&v| v == 2), "{versions:?}");

    let (got, acquired, _) = acquired_after_one_poll(AcquireMode::BatchOptimized).await;
    assert_eq!(got, 3, "one poll still returns max_poll_records");
    assert_eq!(
        acquired, 10,
        "batch-optimized acquisition finishes the batch"
    );
}

/// `RecordLimit` against a broker without `ShareFetch` v2 fails the first
/// poll with a `Config` error naming KIP-1206; it does not fall back.
#[tokio::test]
async fn record_limit_needs_share_fetch_v2() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    broker.set_api_versions(ApiKey::ShareFetch, 1, 1);
    produce_one_batch(&broker, "t", &[b"x"]).await;
    let c = builder(&broker, "g-v1", AcknowledgementMode::Implicit)
        .await
        .acquire_mode(AcquireMode::RecordLimit)
        .build()
        .await
        .unwrap();
    c.subscribe(&["t"]).await.unwrap();
    let deadline = tokio::time::Instant::now() + SETTLE;
    let error = loop {
        match c.poll(Duration::from_millis(200)).await {
            Err(error) => break error,
            Ok(records) => assert!(records.is_empty(), "no fallback to batch-optimized"),
        }
        assert!(tokio::time::Instant::now() < deadline);
    };
    assert!(
        matches!(&error, KrafkaError::Config { message } if message.contains("KIP-1206")),
        "{error:?}"
    );
    c.close().await.unwrap();
}

/// `renew` sends `IsRenewAck=true`, keeps the record pending, and a later
/// accept succeeds. Control: completing the record on renew rejects the
/// accept.
#[tokio::test]
async fn renew_then_accept() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    produce_one_batch(&broker, "t", &[b"a"]).await;
    let c = share(&broker, "g2", AcknowledgementMode::Explicit).await;
    c.subscribe(&["t"]).await.unwrap();
    let recs = drain(&c, 1).await;

    c.renew(&recs[0]).unwrap();
    let results = c.commit().await.unwrap();
    assert!(results[&tp("t", 0)].is_ok(), "{results:?}");
    assert_eq!(
        partition_state(&broker, "g2", "t", 0).acquired.len(),
        1,
        "a renewed record stays acquired"
    );
    assert!(
        c.poll(Duration::from_millis(50)).await.is_err(),
        "a renewed record still needs settling"
    );
    c.ack(&recs[0]).unwrap();
    let results = c.commit().await.unwrap();
    assert!(results[&tp("t", 0)].is_ok(), "{results:?}");
    assert_eq!(partition_state(&broker, "g2", "t", 0).start_offset, 1);
    assert!(c.poll(Duration::from_millis(50)).await.is_ok());
    c.close().await.unwrap();
}

/// Negative control: `renew` on a broker without KIP-1222 fails with an
/// error naming it, and the record stays pending.
#[tokio::test]
async fn renew_needs_kip_1222() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    broker.set_api_versions(ApiKey::ShareAcknowledge, 1, 1);
    broker.set_api_versions(ApiKey::ShareFetch, 1, 1);
    produce_one_batch(&broker, "t", &[b"a"]).await;
    let c = share(&broker, "g2v1", AcknowledgementMode::Explicit).await;
    c.subscribe(&["t"]).await.unwrap();
    let recs = drain(&c, 1).await;
    let error = c.renew(&recs[0]).unwrap_err();
    assert!(error.to_string().contains("KIP-1222"), "{error}");
    c.ack(&recs[0]).unwrap();
    assert!(c.commit().await.unwrap()[&tp("t", 0)].is_ok());
    c.close().await.unwrap();
}

// ── close ──────────────────────────────────────────────────────────────

/// Close in implicit mode accepts the last delivery and sends GAP acks as
/// GAP: the marker's offset is archived, not released. Control: rewriting
/// pending acks to RELEASE on close leaves the start offset at 1.
#[tokio::test]
async fn close_keeps_gap_acknowledgements() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("t", 1);
    let p = crate::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .producer()
        .build_transactional("tx-close")
        .await
        .unwrap();
    p.begin().unwrap();
    let _ = p.send(crate::Record::new("t", "payload")).await.unwrap();
    p.commit().await.unwrap();
    p.close().await.unwrap();

    let c = share(&broker, "g-close", AcknowledgementMode::Implicit).await;
    c.subscribe(&["t"]).await.unwrap();
    assert_eq!(drain(&c, 1).await.len(), 1);
    c.close().await.unwrap();
    assert_eq!(partition_state(&broker, "g-close", "t", 0).start_offset, 2);
    assert_eq!(broker.share_session_closes().len(), 1);
}

/// An assignment change drops the buffered records and delivered records of
/// revoked partitions only, releasing the buffered ones; a queued REJECT of
/// a partition that stays assigned is kept. Control: clearing the whole ack
/// book on a change drops the REJECT.
#[tokio::test]
async fn an_assignment_change_drops_only_revoked_partitions() {
    use super::acks::{AckRange, AckType};
    use super::completed_fetch::CompletedFetch;
    use super::state::{Acquisition, Assigned};

    let broker = FakeBroker::start().await.unwrap();
    let c = share(&broker, "g-revoke", AcknowledgementMode::Explicit).await;
    let assigned = |p: i32| Assigned {
        partition: tp("t", p),
        topic_id: [9; 16],
    };
    c.0.install_assignment(vec![assigned(0), assigned(1)], Vec::new());
    {
        let mut state = c.0.state.lock();
        state
            .book
            .add(0, tp("t", 0), [9; 16], AckRange::one(3, AckType::Reject));
        for p in [0, 1] {
            state.buffer.push_back(CompletedFetch {
                topic: "t".to_string(),
                topic_id: [9; 16],
                partition: p,
                node: 0,
                records: [ConsumerRecord::new("t", p, 7, None, None)].into(),
            });
            state.outstanding.entry(tp("t", p)).or_default().insert(
                4,
                Acquisition {
                    node: 0,
                    topic_id: [9; 16],
                },
            );
        }
    }

    assert!(c.0.install_assignment(vec![assigned(0)], Vec::new()));

    {
        let state = c.0.state.lock();
        let pending: Vec<_> = state
            .book
            .pending_for(0)
            .map(|(p, e)| (p.partition, e.pending.clone()))
            .collect();
        assert!(
            pending.contains(&(0, vec![AckRange::one(3, AckType::Reject)])),
            "the REJECT of the kept partition stays queued: {pending:?}"
        );
        assert!(
            pending.contains(&(1, vec![AckRange::one(7, AckType::Release)])),
            "the revoked partition's buffered record is released: {pending:?}"
        );
        assert_eq!(state.buffer.len(), 1);
        assert_eq!(state.buffer[0].partition, 0);
        assert!(state.outstanding.contains_key(&tp("t", 0)));
        assert!(!state.outstanding.contains_key(&tp("t", 1)));
    }
    c.close().await.unwrap();
}
