//! Consumer guarantees on their failure paths, against the fake broker:
//! cancelled polls, commits, seeks, rebalance listener contract, poll-interval
//! expiry on both group protocols, leader-epoch fencing, and ordering of
//! eager revocation and commits.
//!
//! Run: `cargo test --features test-broker --test consumer_core`
#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use krafka::consumer::{
    AutoOffsetReset, Consumer, ConsumerRebalanceListener, GroupProtocol, OffsetAndMetadata,
    TopicPartition,
};
use krafka::error::ErrorCode;
use krafka::testing::ApiKey;
use krafka::testing::{Control, FakeBroker};

const T: Duration = Duration::from_secs(2);

async fn produce(broker: &FakeBroker, topic: &str, n: usize) {
    let producer = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .producer()
        .build()
        .await
        .unwrap();
    for i in 0..n {
        let value = [b'v', (i % 256) as u8];
        let _ = producer
            .send(krafka::Record::new(
                topic,
                bytes::Bytes::copy_from_slice(&value),
            ))
            .await
            .unwrap();
    }
    let _ = producer.close().await;
}

async fn wait_assigned(consumer: &Consumer) {
    for _ in 0..50 {
        let _ = consumer.poll(Duration::from_millis(200)).await;
        if !consumer.assignment().await.is_empty() {
            return;
        }
    }
    panic!("never assigned");
}

async fn drain(consumer: &Consumer, polls: usize) -> Vec<i64> {
    let mut seen = Vec::new();
    for _ in 0..polls {
        seen.extend(
            consumer
                .poll(Duration::from_millis(200))
                .await
                .unwrap()
                .into_iter()
                .map(|r| r.offset),
        );
    }
    seen
}

/// Records the listener callbacks in order.
#[derive(Default, Clone)]
struct Events(Arc<Mutex<Vec<(&'static str, usize)>>>);

impl Events {
    fn push(&self, kind: &'static str, n: usize) {
        self.0.lock().unwrap().push((kind, n));
    }
    fn kinds(&self) -> Vec<&'static str> {
        self.0.lock().unwrap().iter().map(|(k, _)| *k).collect()
    }
    fn count(&self, kind: &str) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(k, _)| *k == kind)
            .count()
    }
}

impl ConsumerRebalanceListener for Events {
    async fn on_partitions_assigned(&self, p: &[TopicPartition]) {
        self.push("assigned", p.len());
    }
    async fn on_partitions_revoked(&self, p: &[TopicPartition]) {
        self.push("revoked", p.len());
    }
    async fn on_partitions_lost(&self, p: &[TopicPartition]) {
        self.push("lost", p.len());
    }
}

// ── US1: a cancelled poll loses nothing (F1) ────────────────────────────

/// `recv()` dropped while its rejoin is in flight: the join finishes on its
/// own task and the next `recv()` receives records.
#[tokio::test]
async fn a_recv_dropped_during_a_rejoin_does_not_strand_the_consumer() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    produce(&broker, "events", 3).await;
    broker.on_once(ApiKey::JoinGroup, |_| {
        Control::Delay(Duration::from_millis(800))
    });

    let consumer = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .consumer("g-rejoin-cancel")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    consumer.subscribe(&["events"]).await.unwrap();

    let dropped = tokio::time::timeout(Duration::from_millis(200), consumer.recv()).await;
    assert!(dropped.is_err(), "setup: recv is dropped mid-join");

    let record = tokio::time::timeout(Duration::from_secs(10), consumer.recv())
        .await
        .expect("the consumer recovers from a dropped rejoin")
        .unwrap()
        .expect("the consumer is open");
    assert_eq!(record.offset, 0);
    let _ = consumer.close().await;
}

// ── US2: commit never moves the position; commits land in order (F2, F10) ──

#[tokio::test]
async fn committing_does_not_move_the_position() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    produce(&broker, "events", 10).await;

    let consumer = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .consumer("g-f2")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .enable_auto_commit(false)
        .build()
        .await
        .unwrap();
    consumer.subscribe(&["events"]).await.unwrap();

    let mut first = Vec::new();
    for _ in 0..20 {
        first.extend(consumer.poll(Duration::from_millis(200)).await.unwrap());
        if first.len() >= 10 {
            break;
        }
    }
    assert_eq!(first.len(), 10);

    let mut offsets = ahash::AHashMap::new();
    offsets.insert(
        TopicPartition::new("events", 0),
        OffsetAndMetadata::with_metadata(3, "ckpt"),
    );
    consumer.commit_offsets(&offsets).await.unwrap();
    assert_eq!(broker.committed_offset("g-f2", "events", 0), Some(3));
    assert_eq!(consumer.position("events", 0).await, Some(10));
    assert!(
        drain(&consumer, 3).await.is_empty(),
        "nothing is re-delivered"
    );
    let _ = consumer.close().await;
}

/// A first commit whose first attempt fails retriably, and a second commit
/// made while it waits to retry: the broker ends at the second one.
#[tokio::test]
async fn a_retried_commit_never_lands_after_a_newer_one() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let consumer = Arc::new(
        krafka::Kafka::builder(broker.bootstrap_servers())
            .request_timeout(T)
            .connect_timeout(T)
            .connect()
            .await
            .unwrap()
            .consumer("g-order")
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .build()
            .await
            .unwrap(),
    );
    consumer.subscribe(&["events"]).await.unwrap();
    wait_assigned(&consumer).await;

    broker.on_once(ApiKey::OffsetCommit, |_| {
        Control::Error(ErrorCode::CoordinatorLoadInProgress)
    });
    let commit = |offset: i64| {
        let consumer = Arc::clone(&consumer);
        async move {
            let mut offsets = ahash::AHashMap::new();
            offsets.insert(
                TopicPartition::new("events", 0),
                OffsetAndMetadata::new(offset),
            );
            consumer.commit_offsets(&offsets).await
        }
    };
    let older = tokio::spawn(commit(100));
    tokio::time::sleep(Duration::from_millis(30)).await;
    let newer = tokio::spawn(commit(200));
    older.await.unwrap().unwrap();
    newer.await.unwrap().unwrap();
    assert_eq!(broker.committed_offset("g-order", "events", 0), Some(200));
    let _ = consumer.close().await;
}

// ── US5: seek (F7, F8) ──────────────────────────────────────────────────

#[tokio::test]
async fn seek_to_beginning_lands_at_the_log_start_offset() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    produce(&broker, "events", 10).await;
    broker.with_state(|s| s.partition_mut("events", 0).unwrap().log_start_offset = 5);

    let consumer = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .consumer_without_group()
        .build()
        .await
        .unwrap();
    consumer.assign("events", vec![0]).await.unwrap();
    let _ = consumer.poll(Duration::from_millis(100)).await;
    consumer.seek_to_beginning("events", 0).await.unwrap();
    assert_eq!(consumer.position("events", 0).await, Some(5));
    assert_eq!(drain(&consumer, 5).await, (5..10).collect::<Vec<_>>());
    let _ = consumer.close().await;
}

#[tokio::test]
async fn seek_on_an_unowned_partition_is_rejected_and_not_stored() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    produce(&broker, "events", 10).await;

    let consumer = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .consumer("g-f8")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    assert!(consumer.seek("events", 0, 7).await.is_err());
    consumer.subscribe(&["events"]).await.unwrap();
    let mut seen = Vec::new();
    for _ in 0..15 {
        seen.extend(drain(&consumer, 1).await);
        if seen.len() >= 10 {
            break;
        }
    }
    assert_eq!(
        seen,
        (0..10).collect::<Vec<_>>(),
        "the group's reset position wins"
    );
    let _ = consumer.close().await;
}

// ── US6: the listener contract (F3, F9) ─────────────────────────────────

struct SlowRevoke {
    finished: Arc<AtomicBool>,
}

impl ConsumerRebalanceListener for SlowRevoke {
    async fn on_partitions_assigned(&self, _p: &[TopicPartition]) {}
    async fn on_partitions_revoked(&self, _p: &[TopicPartition]) {
        tokio::time::sleep(Duration::from_secs(6)).await;
        self.finished.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn a_slow_revocation_callback_runs_to_completion() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 1);
    let finished = Arc::new(AtomicBool::new(false));
    let consumer = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .consumer("g-f3")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .rebalance_listener(SlowRevoke {
            finished: finished.clone(),
        })
        .build()
        .await
        .unwrap();
    consumer.subscribe(&["events"]).await.unwrap();
    wait_assigned(&consumer).await;

    let t0 = Instant::now();
    consumer.unsubscribe().await.unwrap();
    assert!(t0.elapsed() >= Duration::from_secs(6));
    assert!(
        finished.load(Ordering::SeqCst),
        "the callback was not cut off"
    );
    let _ = consumer.close().await;
}

/// Classic expiry: one `lost`, no error, an automatic rejoin, and `close()`
/// revokes only what was assigned after the rejoin.
#[tokio::test]
async fn classic_expiry_reports_the_loss_once_and_rejoins() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 2);
    let events = Events::default();
    let consumer = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .consumer("g-f9")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .max_poll_interval(Duration::from_secs(1))
        .heartbeat_interval(Duration::from_millis(200))
        .session_timeout(Duration::from_secs(6))
        .rebalance_listener(events.clone())
        .build()
        .await
        .unwrap();
    consumer.subscribe(&["events"]).await.unwrap();
    wait_assigned(&consumer).await;
    let joins_before = broker.request_count(ApiKey::JoinGroup);

    tokio::time::sleep(Duration::from_secs(3)).await;
    assert!(
        broker.request_count(ApiKey::LeaveGroup) >= 1,
        "the member left on expiry"
    );
    for _ in 0..3 {
        consumer.poll(Duration::from_millis(200)).await.unwrap();
    }
    assert_eq!(events.count("lost"), 1, "the loss is reported once");
    assert!(
        broker.request_count(ApiKey::JoinGroup) > joins_before,
        "it rejoined"
    );
    assert!(
        !consumer.assignment().await.is_empty(),
        "and was assigned again"
    );

    let commits = broker.request_count(ApiKey::OffsetCommit);
    let _ = consumer.close().await;
    assert_eq!(
        events.kinds(),
        vec!["assigned", "lost", "assigned", "revoked"]
    );
    assert!(broker.request_count(ApiKey::OffsetCommit) >= commits);
}

// ── KIP-848 poll-interval enforcement ────────────────────

async fn stall(protocol: GroupProtocol, heartbeat: ApiKey, group: &str) {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 2);
    let events = Events::default();
    let consumer = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .consumer(group)
        .group_protocol(protocol)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .max_poll_interval(Duration::from_secs(2))
        .heartbeat_interval(Duration::from_millis(500))
        .rebalance_listener(events.clone())
        .build()
        .await
        .unwrap();
    consumer.subscribe(&["events"]).await.unwrap();
    wait_assigned(&consumer).await;

    tokio::time::sleep(Duration::from_secs(4)).await;
    let heartbeats = broker.request_count(heartbeat);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(
        broker.request_count(heartbeat),
        heartbeats,
        "{protocol:?}: no heartbeat after the member left"
    );
    let members_after_stall = broker.with_state(|s| {
        s.groups
            .get(group)
            .map_or(0, |g| g.members.len() + g.consumer_members.len())
    });
    assert_eq!(
        members_after_stall, 0,
        "{protocol:?}: the stalled member left the group"
    );

    consumer
        .poll(Duration::from_millis(200))
        .await
        .expect("the poll after expiry returns no error");
    assert_eq!(events.count("lost"), 1, "{protocol:?}");
    wait_assigned(&consumer).await;
    assert_eq!(events.count("lost"), 1, "{protocol:?}: reported once");
    let _ = consumer.close().await;
}

#[tokio::test]
async fn a_stalled_kip848_member_leaves_and_rejoins() {
    stall(
        GroupProtocol::Consumer,
        ApiKey::ConsumerGroupHeartbeat,
        "g-c1",
    )
    .await;
}

#[tokio::test]
async fn a_stalled_classic_member_leaves_and_rejoins() {
    stall(GroupProtocol::Classic, ApiKey::Heartbeat, "g-c1-classic").await;
}

/// A consumer that polls within the interval never leaves (negative
/// control for spurious expiry).
#[tokio::test]
async fn a_polling_kip848_member_does_not_expire() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 2);
    let events = Events::default();
    let consumer = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .consumer("g-c1-ok")
        .group_protocol(GroupProtocol::Consumer)
        .max_poll_interval(Duration::from_secs(2))
        .rebalance_listener(events.clone())
        .build()
        .await
        .unwrap();
    consumer.subscribe(&["events"]).await.unwrap();
    wait_assigned(&consumer).await;
    for _ in 0..8 {
        consumer.poll(Duration::from_millis(500)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert_eq!(events.count("lost"), 0);
    let _ = consumer.close().await;
}

/// `STALE_MEMBER_EPOCH` on a heartbeat is a loss: reported once, then the
/// member rejoins at epoch 0 (F13).
#[tokio::test]
async fn a_stale_member_epoch_on_the_heartbeat_loses_the_partitions_once() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("events", 2);
    let events = Events::default();
    let consumer = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .consumer("g-f13")
        .group_protocol(GroupProtocol::Consumer)
        .rebalance_listener(events.clone())
        .build()
        .await
        .unwrap();
    consumer.subscribe(&["events"]).await.unwrap();
    wait_assigned(&consumer).await;
    broker.on_once(ApiKey::ConsumerGroupHeartbeat, |_| {
        Control::Error(ErrorCode::StaleMemberEpoch)
    });
    for _ in 0..40 {
        consumer.poll(Duration::from_millis(200)).await.unwrap();
        if events.count("lost") == 1 && !consumer.assignment().await.is_empty() {
            break;
        }
    }
    assert_eq!(events.count("lost"), 1);
    assert!(
        !consumer.assignment().await.is_empty(),
        "rejoined and reassigned"
    );
    let _ = consumer.close().await;
}

// ── US4: eager revoke before assign (F4) ────────────────────────────────

/// Notes the JoinGroup count the broker had seen when the revocation ran.
struct RevokeProbe {
    broker: Weak<FakeBroker>,
    joins_at_revoke: Arc<AtomicUsize>,
}

impl ConsumerRebalanceListener for RevokeProbe {
    async fn on_partitions_assigned(&self, _p: &[TopicPartition]) {}
    async fn on_partitions_revoked(&self, _p: &[TopicPartition]) {
        if let Some(b) = self.broker.upgrade() {
            self.joins_at_revoke
                .store(b.request_count(ApiKey::JoinGroup), Ordering::SeqCst);
        }
    }
}

#[tokio::test]
async fn an_eager_member_commits_and_revokes_before_it_rejoins() {
    let broker = Arc::new(FakeBroker::start().await.unwrap());
    broker.create_topic("events", 1);
    produce(&broker, "events", 5).await;
    let joins_at_revoke = Arc::new(AtomicUsize::new(usize::MAX));
    let consumer = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .consumer("g-f4")
        .partition_assignment_strategy(krafka::consumer::PartitionAssignmentStrategy::Range)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .heartbeat_interval(Duration::from_millis(100))
        .rebalance_listener(RevokeProbe {
            broker: Arc::downgrade(&broker),
            joins_at_revoke: joins_at_revoke.clone(),
        })
        .build()
        .await
        .unwrap();
    consumer.subscribe(&["events"]).await.unwrap();
    let mut got = 0;
    for _ in 0..20 {
        got += consumer
            .poll(Duration::from_millis(200))
            .await
            .unwrap()
            .len();
        if got >= 5 {
            break;
        }
    }
    let joins = broker.request_count(ApiKey::JoinGroup);

    broker.on_once(ApiKey::Heartbeat, |_| {
        Control::Error(ErrorCode::RebalanceInProgress)
    });
    tokio::time::sleep(Duration::from_secs(1)).await;
    assert_eq!(
        broker.request_count(ApiKey::JoinGroup),
        joins,
        "no JoinGroup from the heartbeat task under an eager protocol"
    );

    let commits = broker.requests();
    let _ = consumer.poll(Duration::from_secs(1)).await.unwrap();
    assert_eq!(
        joins_at_revoke.load(Ordering::SeqCst),
        joins,
        "on_partitions_revoked ran before the rejoin"
    );
    let log = broker.requests();
    let new = &log[commits.len()..];
    let commit_at = new.iter().position(|r| r.api_key == ApiKey::OffsetCommit);
    let join_at = new.iter().position(|r| r.api_key == ApiKey::JoinGroup);
    assert!(
        matches!((commit_at, join_at), (Some(c), Some(j)) if c < j),
        "commit before join: {commit_at:?} {join_at:?}"
    );
    assert_eq!(broker.committed_offset("g-f4", "events", 0), Some(5));
    let _ = consumer.close().await;
}

// ── Leader epoch on every ListOffsets; fenced-partition backoff ──

#[tokio::test]
async fn a_group_reset_carries_the_leader_epoch_and_refreshes_when_fenced() {
    let broker = Arc::new(FakeBroker::start().await.unwrap());
    broker.create_topic("events", 1);
    let consumer = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .consumer("g-c2")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    let weak = Arc::downgrade(&broker);
    broker.on_once(ApiKey::ListOffsets, move |_| {
        if let Some(b) = weak.upgrade() {
            b.bump_leader_epoch("events", 0);
        }
        Control::Pass
    });
    consumer.subscribe(&["events"]).await.unwrap();
    wait_assigned(&consumer).await;
    for _ in 0..5 {
        let _ = consumer.poll(Duration::from_millis(200)).await;
    }
    assert!(
        broker.request_count(ApiKey::ListOffsets) >= 2,
        "fenced, then retried"
    );
    assert!(
        broker.request_count(ApiKey::Metadata) >= 2,
        "with a metadata refresh between"
    );
    assert!(
        broker.request_count(ApiKey::ListOffsets) <= 20,
        "and no hot loop"
    );
    let _ = consumer.close().await;
}

/// A partition whose every fetch is fenced draws a bounded number of
/// requests, and a healthy partition on another broker keeps flowing.
#[tokio::test]
async fn a_permanently_fenced_partition_backs_off() {
    let broker = FakeBroker::start_cluster(2).await.unwrap();
    broker.create_topic("fenced", 1);
    broker.create_topic("healthy", 1);
    broker.set_leader("fenced", 0, 0);
    broker.set_leader("healthy", 0, 1);
    produce(&broker, "healthy", 200).await;

    let consumer = krafka::Kafka::builder(broker.bootstrap_servers())
        .request_timeout(T)
        .connect_timeout(T)
        .connect()
        .await
        .unwrap()
        .consumer_without_group()
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .unwrap();
    consumer.assign("fenced", vec![0]).await.unwrap();
    consumer.assign("healthy", vec![0]).await.unwrap();

    broker.on(ApiKey::Fetch, |info| {
        if info.node_id == 0 {
            Control::Error(ErrorCode::FencedLeaderEpoch)
        } else {
            Control::Pass
        }
    });
    let fetches_before = broker
        .request_nodes(ApiKey::Fetch)
        .iter()
        .filter(|n| **n == 0)
        .count();
    let metadata_before = broker.request_count(ApiKey::Metadata);
    let mut healthy = 0;
    let t0 = Instant::now();
    while t0.elapsed() < Duration::from_secs(2) {
        healthy += consumer
            .poll(Duration::from_millis(100))
            .await
            .unwrap()
            .iter()
            .filter(|r| &*r.topic == "healthy")
            .count();
    }
    let fenced_fetches = broker
        .request_nodes(ApiKey::Fetch)
        .iter()
        .filter(|n| **n == 0)
        .count()
        - fetches_before;
    // The 100 ms doubling schedule allows about five attempts in 2 s; the
    // spec's ceiling is 20. Without the backoff the metadata refresh rate
    // limiter alone still lets about 19 through, so 10 is the bound tested.
    assert!(
        fenced_fetches <= 10,
        "{fenced_fetches} fetches of a fenced partition in 2 s"
    );
    assert!(fenced_fetches >= 2, "it is retried");
    assert!(
        broker.request_count(ApiKey::Metadata) > metadata_before,
        "metadata is refreshed"
    );
    assert_eq!(healthy, 200, "the healthy partition is delivered in full");
    let _ = consumer.close().await;
}
