//! Deterministic simulation: client workloads against an in-memory fake
//! cluster under a paused clock, with faults chosen from a seed, judged by
//! six invariants over the history.
//!
//! Every test runs a seed budget: `KRAFKA_SIM_SEEDS=<a>..<b>` widens it (`just
//! sim-long`), `KRAFKA_SIM_SEED=<n>` replays one seed. A failure prints its
//! seed and the command that replays it. Build with
//! `RUSTFLAGS='--cfg tokio_unstable'` (`just sim`) so Tokio's own RNG, which
//! orders `select!` branches, is seeded too.

#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

#[path = "simulation/checker.rs"]
mod checker;
#[path = "simulation/harness.rs"]
mod harness;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt as _;
use parking_lot::Mutex;
use rand::Rng;
use rand::rngs::SmallRng;

use krafka::consumer::{AutoOffsetReset, GroupProtocol, IsolationLevel};
use krafka::error::ErrorCode;
use krafka::producer::{DeliveryHandle, Producer, Record, TransactionState, TransactionalProducer};
use krafka::testing::{ApiKey, Control};

use checker::{Invariant, Kind, Op, OpId, Outcome, Stamp, Violation};
use harness::{Run, Sim, assert_seeds, seeds, simulate};

/// Seeds every pull request runs, per workload.
const PR_SEEDS: std::ops::Range<u64> = 0..48;

const TOPIC: &str = "sim";
const PARTITIONS: i32 = 3;
const BROKERS: usize = 3;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(6);

// ── Faults ──────────────────────────────────────────────────────────────────

/// A fault for one `Produce`, drawn from the seed: latency, an append whose
/// answer is late or lost, a drop before the append, retriable errors and a
/// definitive rejection.
fn produce_fault(rng: &mut SmallRng) -> Control {
    let late = REQUEST_TIMEOUT + Duration::from_millis(rng.random_range(1..1500));
    match rng.random_range(0..100) {
        0..70 => Control::Pass,
        70..78 => Control::Delay(Duration::from_millis(rng.random_range(1..300))),
        78..82 => Control::ApplyThen(Box::new(Control::Delay(late))),
        82..86 => Control::ApplyThen(Box::new(Control::Disconnect)),
        86..89 => Control::Disconnect,
        89..93 => Control::Error(ErrorCode::NotLeaderForPartition),
        93..96 => Control::ApplyThen(Box::new(Control::Error(
            ErrorCode::NotEnoughReplicasAfterAppend,
        ))),
        96..98 => Control::Delay(late),
        _ => Control::Error(ErrorCode::InvalidRecord),
    }
}

/// A fault for one `EndTxn`: an answer that is late (the request is applied
/// after the client gave up and dropped the connection) or lost, and
/// coordinator errors.
fn end_txn_fault(rng: &mut SmallRng) -> Control {
    let late = REQUEST_TIMEOUT + Duration::from_millis(rng.random_range(1..1500));
    match rng.random_range(0..100) {
        0..65 => Control::Pass,
        65..75 => Control::Delay(late),
        75..83 => Control::ApplyThen(Box::new(Control::Disconnect)),
        83..90 => Control::ApplyThen(Box::new(Control::Delay(late))),
        90..95 => Control::Error(ErrorCode::NotCoordinator),
        _ => Control::Error(ErrorCode::CoordinatorLoadInProgress),
    }
}

/// Between two operations: maybe crash a broker for a while, or move a
/// partition's leader.
async fn cluster_event(sim: &Sim) {
    match sim.pick(0..100) {
        0..4 => {
            let node = i32::try_from(sim.pick(1..BROKERS as u64)).unwrap();
            sim.broker.crash(node);
            tokio::time::sleep(Duration::from_millis(sim.pick(100..3000))).await;
            sim.broker.restart(node);
        }
        4..8 => {
            let partition = i32::try_from(sim.pick(0..PARTITIONS as u64)).unwrap();
            let node = i32::try_from(sim.pick(0..BROKERS as u64)).unwrap();
            sim.broker.set_leader(TOPIC, partition, node);
        }
        8..30 => tokio::time::sleep(Duration::from_millis(sim.pick(1..500))).await,
        _ => {}
    }
}

fn record(value: &str, partition: u64) -> Record {
    Record::new(TOPIC, value.as_bytes().to_vec()).partition(i32::try_from(partition).unwrap())
}

// ── Producer workloads ──────────────────────────────────────────────────────

async fn producer(sim: &Sim) -> Producer {
    sim.broker
        .kafka()
        .request_timeout(REQUEST_TIMEOUT)
        .connect_timeout(REQUEST_TIMEOUT)
        .connect()
        .await
        .expect("connects")
        .producer()
        .linger(Duration::from_millis(sim.pick(0..50)))
        .delivery_timeout(DELIVERY_TIMEOUT)
        .build()
        .await
        .expect("builds")
}

/// Sends enqueued and not yet resolved, by history op.
type Pending = Arc<Mutex<Vec<(OpId, DeliveryHandle)>>>;

/// Record every pending send that has its outcome now, without waiting.
fn collect_resolved(sim: &Sim, pending: &Pending) {
    pending
        .lock()
        .retain_mut(|(op, handle)| match handle.now_or_never() {
            Some(result) => {
                sim.history.complete_send(*op, &result);
                false
            }
            None => true,
        });
}

/// Enqueue `value` and record the send as invoked once it is enqueued.
async fn enqueue(
    sim: &Sim,
    producer: &Producer,
    pending: &Pending,
    value: String,
    txn: Option<usize>,
) {
    let partition = sim.pick(0..PARTITIONS as u64);
    match producer.enqueue(record(&value, partition)).await {
        Ok(handle) => {
            let op = sim.history.invoke(Kind::Send { value, txn });
            pending.lock().push((op, handle));
        }
        Err(error) => {
            sim.history.instant(
                Kind::Send { value, txn },
                Outcome::Failed(error.to_string()),
            );
        }
    }
}

/// Flush, then record which earlier sends had their outcome when it
/// returned.
async fn flush(sim: &Sim, producer: &Producer, pending: &Pending) {
    let op = sim.history.invoke(Kind::Flush);
    let result = producer.flush().await;
    collect_resolved(sim, pending);
    sim.history.complete(op, outcome(&result));
}

/// Close, check completeness as for flush, then wait for the rest.
async fn close(sim: &Sim, producer: &Producer, pending: &Pending) {
    let op = sim.history.invoke(Kind::Close);
    let result = producer.close().await;
    collect_resolved(sim, pending);
    sim.history.complete(op, outcome(&result));
    let rest: Vec<_> = std::mem::take(&mut *pending.lock());
    for (op, handle) in rest {
        let result = handle.await;
        sim.history.complete_send(op, &result);
    }
}

fn outcome<T>(result: &krafka::error::Result<T>) -> Outcome {
    match result {
        Ok(_) => Outcome::Ok,
        Err(error) => Outcome::Failed(error.to_string()),
    }
}

/// An idempotent producer (`acks=all`) sending to three partitions through
/// produce faults, broker crashes and leader moves, flushing now and then.
async fn idempotent_workload(sim: &mut Sim) {
    let faults = sim.faults();
    sim.broker
        .on(ApiKey::Produce, move |_| produce_fault(&mut faults.lock()));
    let producer = producer(sim).await;
    let pending = Pending::default();
    for i in 0..24 {
        enqueue(sim, &producer, &pending, format!("v{i}"), None).await;
        cluster_event(sim).await;
        collect_resolved(sim, &pending);
        if sim.chance(0.15) {
            flush(sim, &producer, &pending).await;
        }
    }
    close(sim, &producer, &pending).await;
}

/// Several tasks enqueue concurrently while the main task flushes.
async fn concurrent_workload(sim: &mut Sim) {
    let faults = sim.faults();
    sim.broker
        .on(ApiKey::Produce, move |_| produce_fault(&mut faults.lock()));
    let producer = Arc::new(producer(sim).await);
    let pending = Pending::default();
    let sim = &*sim;
    let tasks = (0..3).map(|task| {
        let producer = Arc::clone(&producer);
        let pending = Arc::clone(&pending);
        async move {
            for i in 0..8 {
                enqueue(sim, &producer, &pending, format!("w{task}-{i}"), None).await;
                tokio::time::sleep(Duration::from_millis(sim.pick(0..400))).await;
            }
        }
    });
    let flusher = async {
        for _ in 0..4 {
            tokio::time::sleep(Duration::from_millis(sim.pick(100..1200))).await;
            flush(sim, &producer, &pending).await;
        }
    };
    futures::join!(futures::future::join_all(tasks), flusher);
    close(sim, &producer, &pending).await;
}

// ── Transactional workload ─────────────────────────────────────────────────

async fn txn_producer(sim: &Sim) -> TransactionalProducer {
    sim.broker
        .kafka()
        .request_timeout(REQUEST_TIMEOUT)
        .connect_timeout(REQUEST_TIMEOUT)
        .connect()
        .await
        .expect("connects")
        .producer()
        .linger(Duration::from_millis(sim.pick(0..50)))
        .delivery_timeout(DELIVERY_TIMEOUT)
        .max_block(Duration::from_secs(8))
        .build_transactional("sim-txn")
        .await
        .expect("builds")
}

/// Commit as the documentation says: retry while the outcome is unknown,
/// abort after a definite failure.
async fn end_txn(sim: &Sim, producer: &TransactionalProducer, txn: usize, commit: bool) {
    if commit {
        for _ in 0..4 {
            let op = sim.history.invoke(Kind::Commit { txn });
            let result = producer.commit().await;
            let unknown = producer.state() == TransactionState::CommitUnknown;
            sim.history.complete(
                op,
                match &result {
                    Ok(()) => Outcome::Ok,
                    Err(e) if unknown => Outcome::Unknown(e.to_string()),
                    Err(e) => Outcome::Failed(e.to_string()),
                },
            );
            if result.is_ok() || !unknown {
                break;
            }
        }
    }
    if matches!(
        producer.state(),
        TransactionState::Open | TransactionState::Prepared
    ) {
        let op = sim.history.invoke(Kind::Abort { txn });
        let result = producer.abort().await;
        sim.history.complete(op, outcome(&result));
    }
}

/// Transactions of one to four sends under produce and `EndTxn` faults,
/// each committed or aborted, then read back by a `read_committed` reader.
async fn transactional_workload(sim: &mut Sim, tv: i16) {
    sim.broker.set_transaction_version(tv);
    let faults = sim.faults();
    sim.broker.on(ApiKey::Produce, move |_| {
        let mut rng = faults.lock();
        // Fewer produce faults than the idempotent workload: one in a
        // transaction already dooms it.
        if rng.random_bool(0.5) {
            Control::Pass
        } else {
            produce_fault(&mut rng)
        }
    });
    let faults = sim.faults();
    sim.broker
        .on(ApiKey::EndTxn, move |_| end_txn_fault(&mut faults.lock()));

    let producer = txn_producer(sim).await;
    for t in 0..5 {
        if producer.begin().is_err() {
            break;
        }
        let txn = sim.history.begin_txn();
        let pending = Pending::default();
        for i in 0..sim.pick(1..5) {
            let partition = sim.pick(0..PARTITIONS as u64);
            let value = format!("t{t}-{i}");
            match producer.enqueue(record(&value, partition)).await {
                Ok(handle) => {
                    let op = sim.history.invoke(Kind::Send {
                        value,
                        txn: Some(txn),
                    });
                    pending.lock().push((op, handle));
                }
                Err(error) => {
                    sim.history.instant(
                        Kind::Send {
                            value,
                            txn: Some(txn),
                        },
                        Outcome::Failed(error.to_string()),
                    );
                }
            }
            cluster_event(sim).await;
        }
        end_txn(sim, &producer, txn, sim.chance(0.8)).await;
        let rest: Vec<_> = std::mem::take(&mut *pending.lock());
        for (op, handle) in rest {
            let result = handle.await;
            sim.history.complete_send(op, &result);
        }
        if producer.state() == TransactionState::CommitUnknown {
            break;
        }
    }
    let _ = producer.close().await;
    sim.broker.clear_hooks();
    read_committed(sim).await;
}

/// Read every partition to its end with a `read_committed` consumer.
async fn read_committed(sim: &Sim) {
    let kafka = sim.broker.kafka().connect().await.expect("connects");
    let consumer = kafka
        .consumer_without_group()
        .isolation_level(IsolationLevel::ReadCommitted)
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await
        .expect("builds");
    consumer
        .assign(TOPIC, (0..PARTITIONS).collect())
        .await
        .expect("assigns");
    let op = sim.history.invoke(Kind::Read { values: Vec::new() });
    let mut values = Vec::new();
    for _ in 0..20 {
        for record in consumer
            .poll(Duration::from_millis(500))
            .await
            .unwrap_or_default()
        {
            values.push(
                String::from_utf8_lossy(record.value.as_deref().unwrap_or_default()).into_owned(),
            );
        }
    }
    sim.history.set_read(op, values);
    let _ = consumer.close().await;
}

// ── Consumer-group workload ─────────────────────────────────────────────────

const GROUP: &str = "sim-group";
const GROUP_RECORDS: usize = 24;
const GROUP_MEMBERS: usize = 2;
/// Polls one member incarnation makes at most before it leaves.
const ROUNDS: usize = 60;
/// Incarnations one member goes through at most. A dropped KIP-848 member
/// holds its partitions until the coordinator's 45 s session timeout, so the
/// budget leaves room for several of those.
const INCARNATIONS: usize = 30;

/// A fault for one `OffsetCommit`: latency, a commit applied whose answer is
/// late or lost, a drop before it is applied, and coordinator errors.
fn offset_commit_fault(rng: &mut SmallRng) -> Control {
    let late = REQUEST_TIMEOUT + Duration::from_millis(rng.random_range(1..1500));
    match rng.random_range(0..100) {
        0..70 => Control::Pass,
        70..78 => Control::Delay(Duration::from_millis(rng.random_range(1..300))),
        78..83 => Control::ApplyThen(Box::new(Control::Disconnect)),
        83..87 => Control::Disconnect,
        87..91 => Control::ApplyThen(Box::new(Control::Delay(late))),
        91..96 => Control::Error(ErrorCode::NotCoordinator),
        _ => Control::Error(ErrorCode::CoordinatorLoadInProgress),
    }
}

/// Write the records the group reads, without faults.
async fn seed_group_records(sim: &Sim) {
    let producer = producer(sim).await;
    let pending = Pending::default();
    for i in 0..GROUP_RECORDS {
        enqueue(sim, &producer, &pending, format!("c{i}"), None).await;
    }
    close(sim, &producer, &pending).await;
}

/// One member of the group: polls, processes what it got (records it in the
/// history), commits now and then, and every so often leaves — cleanly, or by
/// being dropped — to come back as a new incarnation. It stops when the group
/// as a whole has processed every record.
async fn group_member(
    sim: &Sim,
    protocol: GroupProtocol,
    member: usize,
    processed: &Mutex<HashSet<String>>,
) {
    let done = || processed.lock().len() >= GROUP_RECORDS;
    let kafka = sim
        .broker
        .kafka()
        .request_timeout(REQUEST_TIMEOUT)
        .connect_timeout(REQUEST_TIMEOUT)
        .connect()
        .await
        .expect("connects");
    for _incarnation in 0..INCARNATIONS {
        if done() {
            return;
        }
        let consumer = kafka
            .consumer(GROUP)
            .group_protocol(protocol)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .max_poll_records(i32::try_from(sim.pick(1..6)).unwrap())
            .session_timeout(Duration::from_secs(10))
            .heartbeat_interval(Duration::from_secs(1))
            .build()
            .await
            .expect("builds");
        if consumer.subscribe([TOPIC]).await.is_err() {
            continue;
        }
        for _ in 0..ROUNDS {
            let Ok(records) = consumer.poll(Duration::from_millis(500)).await else {
                continue;
            };
            if !records.is_empty() {
                let values: Vec<String> = records
                    .iter()
                    .map(|r| String::from_utf8_lossy(r.value.as_deref().unwrap_or_default()).into())
                    .collect();
                processed.lock().extend(values.iter().cloned());
                sim.history
                    .instant(Kind::Consume { member, values }, Outcome::Ok);
            }
            if sim.chance(0.4) {
                let op = sim.history.invoke(Kind::CommitOffsets { member });
                let result = consumer.commit().await;
                sim.history.complete(op, outcome(&result));
            }
            cluster_event(sim).await;
            if done() || sim.chance(0.05) {
                break;
            }
        }
        if sim.chance(0.5) {
            let _ = consumer.close().await;
        }
    }
}

/// Two members of one group read a topic through `OffsetCommit` faults,
/// broker crashes and leader moves, committing now and then and leaving and
/// rejoining, until the group has processed every record.
async fn consumer_group_workload(sim: &mut Sim, protocol: GroupProtocol) {
    seed_group_records(sim).await;
    sim.history.consumer_group(GROUP);
    let faults = sim.faults();
    sim.broker.on(ApiKey::OffsetCommit, move |_| {
        offset_commit_fault(&mut faults.lock())
    });
    let processed = Mutex::new(HashSet::new());
    let sim = &*sim;
    let members = (0..GROUP_MEMBERS).map(|member| group_member(sim, protocol, member, &processed));
    futures::future::join_all(members).await;
}

// ── Tests ───────────────────────────────────────────────────────────────────

fn run_idempotent(seed: u64) -> Run {
    simulate(seed, BROKERS, &[(TOPIC, PARTITIONS)], idempotent_workload)
}

fn run_concurrent(seed: u64) -> Run {
    simulate(seed, BROKERS, &[(TOPIC, PARTITIONS)], concurrent_workload)
}

fn run_transactional(seed: u64, tv: i16) -> Run {
    simulate(
        seed,
        BROKERS,
        &[(TOPIC, PARTITIONS)],
        async move |sim: &mut Sim| {
            transactional_workload(sim, tv).await;
        },
    )
}

fn run_consumer_group(seed: u64, protocol: GroupProtocol) -> Run {
    simulate(
        seed,
        BROKERS,
        &[(TOPIC, PARTITIONS)],
        async move |sim: &mut Sim| {
            consumer_group_workload(sim, protocol).await;
        },
    )
}

#[test]
fn idempotent_producer() {
    assert_seeds("idempotent_producer", &seeds(PR_SEEDS), run_idempotent);
}

#[test]
fn concurrent_producer() {
    assert_seeds("concurrent_producer", &seeds(PR_SEEDS), run_concurrent);
}

#[test]
fn transactions_tv1() {
    assert_seeds("transactions_tv1", &seeds(PR_SEEDS), |seed| {
        run_transactional(seed, 1)
    });
}

#[test]
fn transactions_tv2() {
    assert_seeds("transactions_tv2", &seeds(PR_SEEDS), |seed| {
        run_transactional(seed, 2)
    });
}

#[test]
fn consumer_group_classic() {
    assert_seeds("consumer_group_classic", &seeds(PR_SEEDS), |seed| {
        run_consumer_group(seed, GroupProtocol::Classic)
    });
}

#[test]
fn consumer_group_kip848() {
    assert_seeds("consumer_group_kip848", &seeds(PR_SEEDS), |seed| {
        run_consumer_group(seed, GroupProtocol::Consumer)
    });
}

/// One workload's run for a seed.
type Workload = fn(u64) -> Run;

/// The same seed gives the same trace: 100 seeds of the transactional
/// workload (producer, coordinator faults, a `read_committed` consumer) and
/// 25 of each other workload, the consumer groups among them.
#[test]
#[cfg_attr(
    not(tokio_unstable),
    ignore = "needs Tokio's RNG seeded: --cfg tokio_unstable (just sim)"
)]
fn a_run_is_a_function_of_its_seed() {
    let runs: [(&str, u64, Workload); 6] = [
        ("transactions_tv1", 100, |seed| run_transactional(seed, 1)),
        ("transactions_tv2", 25, |seed| run_transactional(seed, 2)),
        ("idempotent_producer", 25, run_idempotent),
        ("concurrent_producer", 25, run_concurrent),
        ("consumer_group_classic", 25, |seed| {
            run_consumer_group(seed, GroupProtocol::Classic)
        }),
        ("consumer_group_kip848", 25, |seed| {
            run_consumer_group(seed, GroupProtocol::Consumer)
        }),
    ];
    let mut differing = Vec::new();
    let mut first_difference = None;
    for (name, count, run) in runs {
        for seed in seeds(0..count) {
            let first = run(seed).trace;
            let second = run(seed).trace;
            if first != second {
                differing.push(format!("{name}/{seed}"));
                first_difference.get_or_insert_with(|| divergence(seed, &first, &second));
            }
        }
    }
    assert!(
        differing.is_empty(),
        "traces differ for {differing:?}\n{}",
        first_difference.unwrap_or_default()
    );
}

/// The first line where two traces of `seed` part.
fn divergence(seed: u64, a: &str, b: &str) -> String {
    let (line, (x, y)) = a
        .lines()
        .zip(b.lines())
        .enumerate()
        .find(|(_, (x, y))| x != y)
        .unwrap_or((a.lines().count().min(b.lines().count()), ("<end>", "<end>")));
    format!("seed {seed}, line {line}:\n  first:  {x}\n  second: {y}")
}

// ── Negative controls ──────────────────────────────────────────────────────

/// A planted unbiased `select!` between two ready branches, recorded into
/// the history: the branch order is Tokio's RNG's.
async fn select_workload(sim: &mut Sim) {
    for _ in 0..64 {
        let winner = tokio::select! {
            () = std::future::ready(()) => "a",
            () = std::future::ready(()) => "b",
        };
        sim.history.instant(
            Kind::Send {
                value: winner.to_owned(),
                txn: None,
            },
            Outcome::Unknown("planted".to_owned()),
        );
    }
}

/// Negative control for the determinism check: with Tokio's RNG unseeded, a
/// planted unbiased `select!` makes two runs of one seed differ; seeded
/// (`--cfg tokio_unstable`), they agree.
#[test]
fn an_unseeded_select_makes_runs_differ() {
    let unseeded =
        |_| harness::simulate_with(harness::Seeding::Unseeded, 0, 1, &[], select_workload).trace;
    assert_ne!(unseeded(0), unseeded(0), "64 unseeded coin flips agreed");
    #[cfg(tokio_unstable)]
    {
        let seeded = || simulate(7, 1, &[], select_workload).trace;
        assert_eq!(seeded(), seeded());
    }
}

fn op(seq: usize, kind: Kind, completed: Option<(usize, Outcome)>) -> Op {
    let stamp = |seq| Stamp {
        seq,
        at: Duration::from_millis(seq as u64),
    };
    Op {
        kind,
        invoked: stamp(seq),
        completed: completed.map(|(seq, outcome)| (stamp(seq), outcome)),
    }
}

fn send(value: &str, txn: Option<usize>) -> Kind {
    Kind::Send {
        value: value.to_owned(),
        txn,
    }
}

fn judge(ops: &[Op], log: &[&str], committed: &[&str]) -> Vec<Invariant> {
    let mut counts = HashMap::new();
    for value in log {
        *counts.entry((*value).to_owned()).or_default() += 1;
    }
    let committed: HashSet<String> = committed.iter().map(|v| (*v).to_owned()).collect();
    checker::check(ops, None, &counts, &committed)
        .into_iter()
        .map(|v: Violation| v.invariant)
        .collect()
}

/// Each invariant fails on a planted history that breaks it, and holds on
/// the same history with the break removed.
#[test]
fn each_invariant_fails_on_a_planted_history() {
    let failed = || Outcome::Failed("rejected".to_owned());

    // 1. Acknowledged, not in the log.
    let acked = [op(0, send("a", None), Some((1, Outcome::Ok)))];
    assert_eq!(judge(&acked, &[], &[]), [Invariant::LostWrite]);
    assert!(judge(&acked, &["a"], &["a"]).is_empty());

    // 2. In the log twice.
    assert_eq!(judge(&acked, &["a", "a"], &["a"]), [Invariant::Duplicate]);

    // 3. A read_committed reader returned an uncommitted record.
    let read = [op(
        0,
        Kind::Read {
            values: vec!["x".to_owned()],
        },
        Some((1, Outcome::Ok)),
    )];
    assert_eq!(judge(&read, &["x"], &[]), [Invariant::AbortedRead]);
    assert!(judge(&read, &["x"], &["x"]).is_empty());

    // 4. A committed transaction with a failed send (T1); a commit reported
    // as a definite failure whose records are visible (T2); a torn
    // transaction; a definite send failure that took effect.
    let t1 = [
        op(0, send("r1", Some(1)), Some((3, Outcome::Ok))),
        op(1, send("r2", Some(1)), Some((4, failed()))),
        op(2, Kind::Commit { txn: 1 }, Some((5, Outcome::Ok))),
    ];
    assert_eq!(judge(&t1, &["r1"], &["r1"]), [Invariant::PartialCommit]);
    let t2 = [
        op(0, send("r1", Some(1)), Some((3, Outcome::Ok))),
        op(1, Kind::Commit { txn: 1 }, Some((2, failed()))),
        op(4, Kind::Abort { txn: 1 }, Some((5, Outcome::Ok))),
    ];
    assert_eq!(judge(&t2, &["r1"], &["r1"]), [Invariant::PartialCommit]);
    assert!(judge(&t2, &["r1"], &[]).is_empty());
    let torn = [
        op(0, send("r1", Some(1)), Some((3, Outcome::Ok))),
        op(1, send("r2", Some(1)), Some((4, Outcome::Ok))),
        op(
            2,
            Kind::Commit { txn: 1 },
            Some((5, Outcome::Unknown("timeout".to_owned()))),
        ),
    ];
    assert_eq!(
        judge(&torn, &["r1", "r2"], &["r1"]),
        [Invariant::PartialCommit]
    );
    assert!(judge(&torn, &["r1", "r2"], &["r1", "r2"]).is_empty());
    assert!(judge(&torn, &["r1", "r2"], &[]).is_empty());
    let took_effect = [op(0, send("a", None), Some((1, failed())))];
    assert_eq!(judge(&took_effect, &["a"], &[]), [Invariant::PartialCommit]);
    assert!(judge(&took_effect, &[], &[]).is_empty());
    let maybe = [op(
        0,
        send("a", None),
        Some((1, Outcome::Unknown("timeout".to_owned()))),
    )];
    assert!(judge(&maybe, &["a"], &[]).is_empty());
    assert!(judge(&maybe, &[], &[]).is_empty());

    // 5. A flush returned before an earlier send had its outcome (P3).
    let early = [
        op(0, send("a", None), Some((3, Outcome::Ok))),
        op(1, Kind::Flush, Some((2, Outcome::Ok))),
    ];
    assert_eq!(judge(&early, &["a"], &["a"]), [Invariant::Incomplete]);
    let in_time = [
        op(0, send("a", None), Some((2, Outcome::Ok))),
        op(1, Kind::Flush, Some((3, Outcome::Ok))),
    ];
    assert!(judge(&in_time, &["a"], &["a"]).is_empty());
    let unresolved = [
        op(0, send("a", None), None),
        op(1, Kind::Close, Some((2, Outcome::Ok))),
    ];
    assert_eq!(judge(&unresolved, &[], &[]), [Invariant::Incomplete]);

    // 6. A record nobody processed: below the committed offset, or never
    // reached at all.
    let log = |offset, value: &str| checker::LogRecord {
        topic: TOPIC.to_owned(),
        partition: 0,
        offset,
        value: value.to_owned(),
    };
    let records = [log(0, "a"), log(1, "b")];
    let consumed = |values: &[&str]| {
        [op(
            0,
            Kind::Consume {
                member: 0,
                values: values.iter().map(|v| (*v).to_owned()).collect(),
            },
            Some((1, Outcome::Ok)),
        )]
    };
    let committed: HashMap<(String, i32), i64> = [((TOPIC.to_owned(), 0), 2)].into();
    let uncommitted = HashMap::new();
    let at_least_once = |ops: &[Op], committed| {
        checker::check_at_least_once(ops, &records, committed)
            .into_iter()
            .map(|v| v.invariant)
            .collect::<Vec<_>>()
    };
    assert_eq!(
        at_least_once(&consumed(&["a"]), &committed),
        [Invariant::AtLeastOnce]
    );
    assert_eq!(
        at_least_once(&consumed(&["a"]), &uncommitted),
        [Invariant::AtLeastOnce]
    );
    assert!(at_least_once(&consumed(&["a", "b"]), &committed).is_empty());
    assert!(at_least_once(&consumed(&["b", "a", "b"]), &uncommitted).is_empty());
}
