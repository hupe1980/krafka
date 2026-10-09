//! The six invariants, judged over a history and the cluster's final state.
//!
//! This extends the send-history checker in `tests/support` (an acknowledged
//! record is in the log exactly once; a failed one only when its error said it
//! may have been written) from one send at a time to a history of concurrent
//! operations with transactions, reads, flushes and closes:
//!
//! 1. **lost acknowledged write**: a send acknowledged under `acks=all` is in
//!    the log;
//! 2. **duplicate**: with idempotence on, no value is in the log twice;
//! 3. **aborted read**: a `read_committed` reader returns only committed
//!    records;
//! 4. **partial commit**: a transaction reported committed has every send
//!    succeeded and every record visible; one reported aborted, or whose
//!    commit failed definitely, has none visible; one whose commit outcome is
//!    unknown has all or none visible; a send outside a transaction that
//!    failed definitely is not in the log;
//! 5. **flush/close completeness**: `flush()` or `close()` returns only after
//!    every send enqueued before it was called has an outcome;
//! 6. **at-least-once**: every record a consumer group reads is processed by
//!    some member, and the group's committed offset never covers a record no
//!    member processed.
//!
//! An indefinite outcome may complete at any later time, so it is accepted
//! either way. Record timestamps are never compared.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt::{self, Write as _};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use krafka::error::{KrafkaError, Result};
use krafka::producer::RecordMetadata;
use krafka::testing::FakeBroker;

/// Which of the six invariants a violation breaks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Invariant {
    LostWrite,
    Duplicate,
    AbortedRead,
    PartialCommit,
    Incomplete,
    AtLeastOnce,
}

impl fmt::Display for Invariant {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::LostWrite => "lost acknowledged write",
            Self::Duplicate => "duplicate",
            Self::AbortedRead => "aborted read",
            Self::PartialCommit => "partial commit",
            Self::Incomplete => "flush/close completeness",
            Self::AtLeastOnce => "at-least-once",
        })
    }
}

/// One broken invariant, with the history position it was found at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Violation {
    pub invariant: Invariant,
    pub detail: String,
}

impl fmt::Display for Violation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.invariant, self.detail)
    }
}

/// What the application was told.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Ok,
    /// A definite failure: the operation did not take effect.
    Failed(String),
    /// Indefinite: it may take effect at any later time.
    Unknown(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    /// One record, `txn` naming its transaction.
    Send {
        value: String,
        txn: Option<usize>,
    },
    Flush,
    Close,
    Commit {
        txn: usize,
    },
    Abort {
        txn: usize,
    },
    /// A `read_committed` reader's records.
    Read {
        values: Vec<String>,
    },
    /// Records a consumer-group member returned from `poll` and processed.
    Consume {
        member: usize,
        values: Vec<String>,
    },
    /// A consumer-group member's `commit()`.
    CommitOffsets {
        member: usize,
    },
}

/// A point in the history: its position and the simulated time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamp {
    pub seq: usize,
    pub at: Duration,
}

#[derive(Debug, Clone)]
pub struct Op {
    pub kind: Kind,
    pub invoked: Stamp,
    pub completed: Option<(Stamp, Outcome)>,
}

pub type OpId = usize;

#[derive(Debug)]
struct Inner {
    started: tokio::time::Instant,
    next_seq: usize,
    txns: usize,
    ops: Vec<Op>,
    not_terminated: Option<Duration>,
    /// The consumer group whose reads the at-least-once invariant judges.
    group: Option<String>,
}

impl Default for Inner {
    fn default() -> Self {
        Self {
            started: tokio::time::Instant::now(),
            next_seq: 0,
            txns: 0,
            ops: Vec::new(),
            not_terminated: None,
            group: None,
        }
    }
}

/// The history of one run, shared by every task of the workload.
#[derive(Debug, Clone, Default)]
pub struct History(Arc<Mutex<Inner>>);

impl Inner {
    fn stamp(&mut self) -> Stamp {
        let seq = self.next_seq;
        self.next_seq += 1;
        Stamp {
            seq,
            at: self.started.elapsed(),
        }
    }
}

impl History {
    /// Record that `kind` was invoked.
    pub fn invoke(&self, kind: Kind) -> OpId {
        let mut inner = self.0.lock();
        let invoked = inner.stamp();
        inner.ops.push(Op {
            kind,
            invoked,
            completed: None,
        });
        inner.ops.len() - 1
    }

    /// Record the outcome of `op`.
    pub fn complete(&self, op: OpId, outcome: Outcome) {
        let mut inner = self.0.lock();
        let stamp = inner.stamp();
        inner.ops[op].completed = Some((stamp, outcome));
    }

    /// Record an operation that completed at once.
    pub fn instant(&self, kind: Kind, outcome: Outcome) -> OpId {
        let op = self.invoke(kind);
        self.complete(op, outcome);
        op
    }

    /// Record a send's answer: an error that says the record may have been
    /// written is indefinite.
    pub fn complete_send(&self, op: OpId, result: &Result<RecordMetadata>) {
        self.complete(op, send_outcome(result));
    }

    /// Complete a read with the values it returned.
    pub fn set_read(&self, op: OpId, values: Vec<String>) {
        self.0.lock().ops[op].kind = Kind::Read { values };
        self.complete(op, Outcome::Ok);
    }

    /// Number a new transaction.
    pub fn begin_txn(&self) -> usize {
        let mut inner = self.0.lock();
        inner.txns += 1;
        inner.txns
    }

    /// Judge `group`'s reads and commits by the at-least-once invariant.
    pub fn consumer_group(&self, group: &str) {
        self.0.lock().group = Some(group.to_owned());
    }

    /// The run hit the harness limit before the workload returned.
    pub fn not_terminated(&self, limit: Duration) {
        self.0.lock().not_terminated = Some(limit);
    }

    /// One line per event, in history order.
    pub fn render(&self) -> String {
        let inner = self.0.lock();
        let mut events: Vec<(Stamp, String)> = Vec::new();
        for (id, op) in inner.ops.iter().enumerate() {
            events.push((op.invoked, format!("invoke #{id} {:?}", op.kind)));
            if let Some((stamp, outcome)) = &op.completed {
                events.push((*stamp, format!("return #{id} {outcome:?}")));
            }
        }
        events.sort_by_key(|(s, _)| s.seq);
        let mut out = String::new();
        for (stamp, line) in events {
            let _ = writeln!(out, "{:>10.3?} {line}", stamp.at);
        }
        if let Some(limit) = inner.not_terminated {
            let _ = writeln!(out, "not terminated within {limit:?}");
        }
        out
    }

    /// Judge the history against the cluster's final state of `topics`.
    pub fn check(&self, broker: &FakeBroker, topics: &[&str]) -> Vec<Violation> {
        let mut log: HashMap<String, usize> = HashMap::new();
        let mut committed: HashSet<String> = HashSet::new();
        for topic in topics {
            for record in broker.all_records(topic).expect("log decodes") {
                *log.entry(value_of(record.value.as_deref())).or_default() += 1;
            }
            for record in broker.committed_records(topic).expect("log decodes") {
                committed.insert(value_of(record.value.as_deref()));
            }
        }
        let inner = self.0.lock();
        let mut violations = check(&inner.ops, inner.not_terminated, &log, &committed);
        if let Some(group) = &inner.group {
            let mut records = Vec::new();
            let mut offsets = HashMap::new();
            for topic in topics {
                for record in broker.all_records(topic).expect("log decodes") {
                    let key = ((*topic).to_owned(), record.partition);
                    if let Some(offset) = broker.committed_offset(group, topic, record.partition) {
                        offsets.insert(key.clone(), offset);
                    }
                    records.push(LogRecord {
                        topic: key.0,
                        partition: key.1,
                        offset: record.offset,
                        value: value_of(record.value.as_deref()),
                    });
                }
            }
            violations.extend(check_at_least_once(&inner.ops, &records, &offsets));
        }
        violations
    }
}

/// One record of the final log.
#[derive(Debug, Clone)]
pub struct LogRecord {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub value: String,
}

/// Invariant 6: every record in the log was processed by some member, and
/// none below a partition's committed offset went unprocessed.
pub fn check_at_least_once(
    ops: &[Op],
    records: &[LogRecord],
    committed: &HashMap<(String, i32), i64>,
) -> Vec<Violation> {
    let processed: HashSet<&str> = ops
        .iter()
        .filter_map(|op| match &op.kind {
            Kind::Consume { values, .. } => Some(values),
            _ => None,
        })
        .flatten()
        .map(String::as_str)
        .collect();
    let mut violations = Vec::new();
    for record in records {
        if processed.contains(record.value.as_str()) {
            continue;
        }
        let at = format!(
            "{} at {}-{}@{}",
            record.value, record.topic, record.partition, record.offset
        );
        let detail = match committed.get(&(record.topic.clone(), record.partition)) {
            Some(offset) if record.offset < *offset => {
                format!("{at} is below the committed offset {offset} but no member processed it")
            }
            _ => format!("{at} was never processed"),
        };
        violations.push(Violation {
            invariant: Invariant::AtLeastOnce,
            detail,
        });
    }
    violations
}

fn value_of(bytes: Option<&[u8]>) -> String {
    String::from_utf8_lossy(bytes.unwrap_or_default()).into_owned()
}

/// The outcome a send's answer reports. Only a delivery timeout says whether
/// the record may have been written; any other error leaves it open, since
/// an earlier attempt of the batch may have been appended before the one
/// that failed.
pub fn send_outcome(result: &Result<RecordMetadata>) -> Outcome {
    match result {
        Ok(_) => Outcome::Ok,
        Err(
            error @ KrafkaError::DeliveryTimeout {
                possibly_written: false,
                ..
            },
        ) => Outcome::Failed(error.to_string()),
        Err(error) => Outcome::Unknown(error.to_string()),
    }
}

/// The five invariants over `ops`, given how often each value is in the log
/// and which values a `read_committed` reader can see.
pub fn check(
    ops: &[Op],
    not_terminated: Option<Duration>,
    log: &HashMap<String, usize>,
    committed: &HashSet<String>,
) -> Vec<Violation> {
    let mut violations = Vec::new();
    let mut push = |invariant, detail: String| violations.push(Violation { invariant, detail });

    // 2. No value twice.
    let mut duplicates: Vec<_> = log.iter().filter(|(_, n)| **n > 1).collect();
    duplicates.sort();
    for (value, n) in duplicates {
        push(
            Invariant::Duplicate,
            format!("{value} is in the log {n} times"),
        );
    }

    let mut txns: BTreeMap<usize, Vec<(OpId, &Op)>> = BTreeMap::new();
    for (id, op) in ops.iter().enumerate() {
        let outcome = op.completed.as_ref().map(|(_, o)| o);
        match (&op.kind, outcome) {
            // 1. An acknowledged write outside a transaction is in the log.
            (Kind::Send { value, txn: None }, Some(Outcome::Ok)) => {
                if log.get(value).copied().unwrap_or(0) == 0 {
                    push(
                        Invariant::LostWrite,
                        format!("#{id} {value} acknowledged but not in the log"),
                    );
                }
            }
            // 4. A definite failure did not take effect.
            (Kind::Send { value, txn: None }, Some(Outcome::Failed(error))) => {
                if log.get(value).copied().unwrap_or(0) > 0 {
                    push(
                        Invariant::PartialCommit,
                        format!("#{id} {value} failed definitely ({error}) but is in the log"),
                    );
                }
            }
            (Kind::Send { txn: Some(txn), .. } | Kind::Commit { txn } | Kind::Abort { txn }, _) => {
                txns.entry(*txn).or_default().push((id, op))
            }
            // 3. A read_committed reader sees only committed records.
            (Kind::Read { values }, _) => {
                for value in values {
                    if !committed.contains(value) {
                        push(
                            Invariant::AbortedRead,
                            format!("#{id} read {value}, which is not committed"),
                        );
                    }
                }
            }
            // 5. Every send enqueued before a flush or close has an outcome
            // when it returns.
            (Kind::Flush | Kind::Close, Some(_)) => {
                let Some((returned, _)) = &op.completed else {
                    continue;
                };
                for (send_id, send) in ops.iter().enumerate() {
                    let Kind::Send { value, .. } = &send.kind else {
                        continue;
                    };
                    if send.invoked.seq >= op.invoked.seq {
                        continue;
                    }
                    let resolved_before = send
                        .completed
                        .as_ref()
                        .is_some_and(|(s, _)| s.seq < returned.seq);
                    if !resolved_before {
                        push(
                            Invariant::Incomplete,
                            format!(
                                "#{id} {:?} returned at {:?} before #{send_id} {value} had an \
                                 outcome",
                                op.kind, returned.at
                            ),
                        );
                    }
                }
            }
            _ => {}
        }
    }

    for (txn, members) in txns {
        check_txn(txn, &members, committed, &mut push);
    }

    if let Some(limit) = not_terminated {
        push(
            Invariant::Incomplete,
            format!("the workload did not return within {limit:?} of simulated time"),
        );
    }
    violations
}

/// What the application was told about one transaction.
#[derive(Debug, PartialEq, Eq)]
enum Claim {
    Committed,
    /// Aborted, or a commit failed definitely with nothing indefinite.
    NotCommitted,
    /// A commit's outcome was unknown and nothing resolved it.
    Unknown,
    /// Never ended (the run stopped first).
    Open,
}

fn check_txn(
    txn: usize,
    members: &[(OpId, &Op)],
    committed: &HashSet<String>,
    push: &mut impl FnMut(Invariant, String),
) {
    let mut claim = Claim::Open;
    for (_, op) in members {
        let outcome = op.completed.as_ref().map(|(_, o)| o);
        claim = match (&op.kind, outcome, claim) {
            (Kind::Commit { .. }, Some(Outcome::Ok), _) => Claim::Committed,
            (_, _, Claim::Committed) => Claim::Committed,
            (Kind::Commit { .. } | Kind::Abort { .. }, Some(Outcome::Unknown(_)), _) => {
                Claim::Unknown
            }
            (Kind::Commit { .. }, None, _) => Claim::Unknown,
            (Kind::Abort { .. }, Some(Outcome::Ok), Claim::Unknown) => Claim::Unknown,
            (Kind::Abort { .. }, Some(Outcome::Ok), _) => Claim::NotCommitted,
            (Kind::Commit { .. }, Some(Outcome::Failed(_)), Claim::Open) => Claim::NotCommitted,
            (_, _, claim) => claim,
        };
    }

    let sends: Vec<_> = members
        .iter()
        .filter_map(|(id, op)| match &op.kind {
            Kind::Send { value, .. } => Some((*id, value, op.completed.as_ref().map(|(_, o)| o))),
            _ => None,
        })
        .collect();
    let visible: Vec<_> = sends
        .iter()
        .filter(|(_, value, _)| committed.contains(*value))
        .collect();

    match claim {
        Claim::Committed => {
            for (id, value, outcome) in &sends {
                match outcome {
                    Some(Outcome::Ok) if committed.contains(*value) => {}
                    Some(Outcome::Ok) => push(
                        Invariant::PartialCommit,
                        format!("txn {txn} committed but #{id} {value} is not visible"),
                    ),
                    other => push(
                        Invariant::PartialCommit,
                        format!("txn {txn} committed although #{id} {value} ended {other:?}"),
                    ),
                }
            }
        }
        Claim::NotCommitted => {
            for (id, value, _) in &visible {
                push(
                    Invariant::PartialCommit,
                    format!("txn {txn} was not committed but #{id} {value} is visible"),
                );
            }
        }
        Claim::Unknown | Claim::Open => {
            if !visible.is_empty() && visible.len() != sends.len() {
                push(
                    Invariant::PartialCommit,
                    format!(
                        "txn {txn} is torn: {} of its {} records are visible",
                        visible.len(),
                        sends.len()
                    ),
                );
            }
        }
    }
}
