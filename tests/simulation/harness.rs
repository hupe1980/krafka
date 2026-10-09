//! Runs one workload as a function of its seed.
//!
//! A run is a current-thread runtime with Tokio's clock paused, an in-memory
//! fake cluster, the client's random draws seeded from the seed and, when
//! built with `--cfg tokio_unstable`, Tokio's own RNG (the `select!` branch
//! order) seeded too. The workload's fault choices draw from a second stream
//! of the same seed. Nothing reads the wall clock or the network, so the
//! same seed gives the same trace.

use std::fmt::Write as _;
use std::ops::Range;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use krafka::testing::FakeBroker;

use super::checker::{History, Violation};

/// Longest simulated time a run may take. A workload still running at this
/// point is reported as not terminating (a `flush` or `close` that never
/// returns, or a futurelock).
pub const RUN_LIMIT: Duration = Duration::from_secs(600);

/// Mixed into the seed for the workload's own fault choices, so they draw
/// from a different stream than the client.
const FAULT_STREAM: u64 = 0x9E37_79B9_7F4A_7C15;

/// The seeds a test runs: `KRAFKA_SIM_SEED=<n>` replays one,
/// `KRAFKA_SIM_SEEDS=<a>..<b>` runs a range, otherwise `default`.
pub fn seeds(default: Range<u64>) -> Vec<u64> {
    if let Ok(one) = std::env::var("KRAFKA_SIM_SEED") {
        return vec![one.trim().parse().expect("KRAFKA_SIM_SEED is an integer")];
    }
    if let Ok(range) = std::env::var("KRAFKA_SIM_SEEDS") {
        let (a, b) = range
            .split_once("..")
            .expect("KRAFKA_SIM_SEEDS is <start>..<end>");
        let a: u64 = a.trim().parse().expect("range start is an integer");
        let b: u64 = b.trim().parse().expect("range end is an integer");
        return (a..b).collect();
    }
    default.collect()
}

/// What one workload sees: the seed's fault stream, the cluster and the
/// history it records into.
pub struct Sim {
    pub broker: FakeBroker,
    pub history: History,
    faults: Arc<Mutex<SmallRng>>,
}

impl Sim {
    /// A draw from the fault stream, shareable with broker hooks.
    pub fn faults(&self) -> Arc<Mutex<SmallRng>> {
        Arc::clone(&self.faults)
    }

    /// `true` with probability `p`, from the fault stream.
    pub fn chance(&self, p: f64) -> bool {
        self.faults.lock().random_bool(p)
    }

    /// A value in `range`, from the fault stream.
    pub fn pick(&self, range: Range<u64>) -> u64 {
        self.faults.lock().random_range(range)
    }
}

/// The result of one run.
pub struct Run {
    /// Every operation, every request the cluster received, and the final
    /// logs, one line each, with simulated times. Equal seeds give equal
    /// traces.
    pub trace: String,
    pub violations: Vec<Violation>,
}

/// Whether Tokio's own RNG is seeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Seeding {
    /// From the run's seed (only possible with `--cfg tokio_unstable`).
    Seeded,
    /// From the operating system: the negative control.
    Unseeded,
}

/// Build the runtime for `seed`.
pub fn runtime(seeding: Seeding, seed: u64) -> tokio::runtime::Runtime {
    let mut builder = tokio::runtime::Builder::new_current_thread();
    builder.enable_time().start_paused(true);
    #[cfg(tokio_unstable)]
    if seeding == Seeding::Seeded {
        builder.rng_seed(tokio::runtime::RngSeed::from_bytes(&seed.to_le_bytes()));
    }
    #[cfg(not(tokio_unstable))]
    let _ = (seeding, seed);
    builder.build().expect("runtime builds")
}

/// Wall-clock budget of one run. Simulated time only advances when every
/// task is idle, so a task that never yields to a timer (a busy loop) stops
/// the clock; this catches it.
pub const WALL_LIMIT: Duration = Duration::from_secs(30);

/// Run `workload` on a fresh cluster of `brokers` brokers under `seed`, then
/// check the history against the cluster's final state.
///
/// The run gets its own thread; one that exceeds [`WALL_LIMIT`] is reported
/// as not terminating and left behind.
pub fn simulate<W>(seed: u64, brokers: usize, topics: &[(&str, i32)], workload: W) -> Run
where
    W: AsyncFnOnce(&mut Sim) + Send + 'static,
{
    simulate_with(Seeding::Seeded, seed, brokers, topics, workload)
}

/// [`simulate`], choosing whether Tokio's RNG is seeded.
pub fn simulate_with<W>(
    seeding: Seeding,
    seed: u64,
    brokers: usize,
    topics: &[(&str, i32)],
    workload: W,
) -> Run
where
    W: AsyncFnOnce(&mut Sim) + Send + 'static,
{
    let topics: Vec<(String, i32)> = topics.iter().map(|(t, p)| ((*t).to_owned(), *p)).collect();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name(format!("sim-seed-{seed}"))
        .spawn(move || {
            let _ = tx.send(run_on_this_thread(
                seeding, seed, brokers, &topics, workload,
            ));
        })
        .expect("thread spawns");
    rx.recv_timeout(WALL_LIMIT).unwrap_or_else(|_| Run {
        trace: format!("seed {seed}: no result within {WALL_LIMIT:?} of wall time\n"),
        violations: vec![Violation {
            invariant: super::checker::Invariant::Incomplete,
            detail: format!(
                "the run did not finish within {WALL_LIMIT:?} of wall time: a task spins \
                 without yielding to the clock"
            ),
        }],
    })
}

fn run_on_this_thread<W>(
    seeding: Seeding,
    seed: u64,
    brokers: usize,
    topics: &[(String, i32)],
    workload: W,
) -> Run
where
    W: AsyncFnOnce(&mut Sim),
{
    runtime(seeding, seed).block_on(async {
        krafka::testing::seed_rng(seed);
        let broker = FakeBroker::start_in_memory(brokers);
        for (topic, partitions) in topics {
            broker.create_topic(topic, *partitions);
        }
        let mut sim = Sim {
            broker,
            history: History::default(),
            faults: Arc::new(Mutex::new(SmallRng::seed_from_u64(seed ^ FAULT_STREAM))),
        };
        let history = sim.history.clone();
        let finished = tokio::time::timeout(RUN_LIMIT, workload(&mut sim)).await;
        if finished.is_err() {
            history.not_terminated(RUN_LIMIT);
        }
        sim.broker.clear_hooks();
        for node in 0..i32::try_from(brokers).expect("broker count fits i32") {
            sim.broker.restart(node);
        }
        let topics: Vec<&str> = topics.iter().map(|(t, _)| t.as_str()).collect();
        let violations = history.check(&sim.broker, &topics);
        let trace = trace(&sim, &history, &topics);
        Run { trace, violations }
    })
}

fn trace(sim: &Sim, history: &History, topics: &[&str]) -> String {
    let mut out = history.render();
    for r in sim.broker.requests() {
        let _ = writeln!(
            out,
            "{:>10.3?} request #{} {:?} v{} node {} connection {}",
            r.at, r.sequence, r.api_key, r.api_version, r.node_id, r.connection
        );
    }
    for topic in topics {
        let values: Vec<String> = sim
            .broker
            .all_records(topic)
            .expect("log decodes")
            .into_iter()
            .map(|r| {
                format!(
                    "{}@{}:{}",
                    String::from_utf8_lossy(r.value.as_deref().unwrap_or_default()),
                    r.partition,
                    r.offset
                )
            })
            .collect();
        let _ = writeln!(out, "log {topic}: {}", values.join(" "));
    }
    out
}

/// Run `seeds` and panic with every failing seed and the command that
/// replays it.
pub fn assert_seeds<W>(test: &str, seeds: &[u64], mut run: W)
where
    W: FnMut(u64) -> Run,
{
    let mut failures = String::new();
    for &seed in seeds {
        let result = run(seed);
        if std::env::var_os("KRAFKA_SIM_TRACE").is_some() {
            println!("── seed {seed}\n{}", result.trace);
        }
        if !result.violations.is_empty() {
            let _ = writeln!(failures, "seed {seed}:");
            for v in &result.violations {
                let _ = writeln!(failures, "  {v}");
            }
            let _ = writeln!(failures, "  replay: {}", replay_command(test, seed));
        }
    }
    assert!(
        failures.is_empty(),
        "invariant violations (KRAFKA_SIM_TRACE=1 --nocapture prints each trace):\n{failures}"
    );
}

/// The one command that replays `seed` of `test`.
pub fn replay_command(test: &str, seed: u64) -> String {
    format!("just sim-replay {test} {seed}")
}
