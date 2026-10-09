//! Commit results and the acknowledgement-commit callback.

use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use ahash::AHashMap as HashMap;
use parking_lot::Mutex;
use tokio::sync::Notify;
use tracing::warn;

use super::acks::Resolved;
use super::state::Inner;
use crate::consumer::TopicPartition;
use crate::error::{KrafkaError, Result};
use crate::{Offset, PartitionId};

/// Per-partition outcome of [`commit`](super::ShareConsumer::commit).
///
/// Holds one entry for every partition the commit sent acknowledgements
/// for, and only those.
pub type CommitResults = HashMap<TopicPartition, Result<()>>;

/// The outcome of one acknowledgement request for one partition, as passed
/// to the callback set with
/// [`acknowledgement_commit_callback`](super::ShareConsumerBuilder::acknowledgement_commit_callback).
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct AcknowledgementCommit {
    /// Topic of the acknowledged records.
    pub topic: String,
    /// Partition of the acknowledged records.
    pub partition: PartitionId,
    /// The acknowledged offsets, as inclusive ranges in offset order.
    pub offsets: Vec<RangeInclusive<Offset>>,
    /// `Ok` when the broker applied every acknowledgement of the request,
    /// otherwise the error that failed them.
    pub result: Result<()>,
}

/// Callback invoked with the outcome of every acknowledgement request.
pub type AcknowledgementCommitCallback = Arc<dyn Fn(&AcknowledgementCommit) + Send + Sync>;

impl AcknowledgementCommit {
    pub(crate) fn from_resolved(resolved: &Resolved) -> Self {
        let mut ranges = resolved.ranges.clone();
        ranges.sort_unstable_by_key(|r| r.first);
        let mut offsets: Vec<RangeInclusive<Offset>> = Vec::with_capacity(ranges.len());
        for range in ranges {
            match offsets.last_mut() {
                Some(prev) if prev.end().checked_add(1) == Some(range.first) => {
                    *prev = *prev.start()..=range.last;
                }
                _ => offsets.push(range.first..=range.last),
            }
        }
        Self {
            topic: resolved.partition.topic.clone(),
            partition: resolved.partition.partition,
            offsets,
            result: resolved.result.clone(),
        }
    }
}

/// Invoke the callback for each resolution, isolating panics.
pub(crate) fn report(callback: Option<&AcknowledgementCommitCallback>, resolved: &[Resolved]) {
    let Some(callback) = callback else {
        return;
    };
    for resolution in resolved {
        let commit = AcknowledgementCommit::from_resolved(resolution);
        let call = std::panic::AssertUnwindSafe(|| callback(&commit));
        if std::panic::catch_unwind(call).is_err() {
            warn!(
                topic = %commit.topic,
                partition = commit.partition,
                "the acknowledgement-commit callback panicked"
            );
        }
    }
}

/// A `commit()` or `close()` waiting for the ack-book entries it attached to.
#[derive(Debug, Default)]
pub(crate) struct CommitWaiter {
    state: Mutex<WaiterState>,
    done: Notify,
}

#[derive(Debug, Default)]
struct WaiterState {
    remaining: usize,
    results: CommitResults,
}

impl CommitWaiter {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Wait for `entries` more ack-book entries. Called under the state lock
    /// that attached them, so none can finish first.
    pub(crate) fn expect(&self, entries: usize) {
        self.state.lock().remaining += entries;
    }

    /// Record a resolution; an error wins over `Ok` for the partition.
    pub(crate) fn record(&self, partition: &TopicPartition, result: &Result<()>) {
        let mut state = self.state.lock();
        let slot = state
            .results
            .entry(partition.clone())
            .or_insert_with(|| Ok(()));
        if slot.is_ok() {
            *slot = result.clone();
        }
    }

    /// One attached entry is empty.
    pub(crate) fn entry_done(&self) {
        let mut state = self.state.lock();
        state.remaining = state.remaining.saturating_sub(1);
        if state.remaining == 0 {
            self.done.notify_waiters();
        }
    }

    /// Wait until every attached entry is empty, or `deadline`.
    pub(crate) async fn wait(&self, deadline: tokio::time::Instant) -> bool {
        loop {
            let done = self.done.notified();
            tokio::pin!(done);
            done.as_mut().enable();
            if self.state.lock().remaining == 0 {
                return true;
            }
            if tokio::time::timeout_at(deadline, done).await.is_err() {
                return self.state.lock().remaining == 0;
            }
        }
    }

    /// Mark every partition still without a result as timed out, and take the
    /// results.
    pub(crate) fn finish(&self, unresolved: &[TopicPartition]) -> CommitResults {
        let mut state = self.state.lock();
        for partition in unresolved {
            state.results.insert(
                partition.clone(),
                Err(KrafkaError::timeout(format!(
                    "acknowledging {}-{}",
                    partition.topic, partition.partition
                ))),
            );
        }
        std::mem::take(&mut state.results)
    }
}

/// Detaches a waiter from the ack book when `commit()` returns or is
/// dropped.
struct Attached<'a> {
    inner: &'a Inner,
    waiter: Arc<CommitWaiter>,
}

impl Drop for Attached<'_> {
    fn drop(&mut self) {
        self.inner.state.lock().book.detach(&self.waiter);
    }
}

/// Send every acknowledgement now and wait for the outcome, per partition,
/// until `deadline`. Acknowledgements still unanswered at the deadline are
/// dropped from the book and reported as timed out.
pub(crate) async fn commit(
    inner: &Arc<Inner>,
    deadline: tokio::time::Instant,
) -> Result<CommitResults> {
    if inner.closed.load(Ordering::Acquire) {
        return Err(KrafkaError::closed("share consumer is closed"));
    }
    inner.accept_delivered();
    let waiter = CommitWaiter::new();
    {
        let mut state = inner.state.lock();
        let entries = state.book.attach(&waiter);
        waiter.expect(entries);
    }
    let attached = Attached {
        inner,
        waiter: Arc::clone(&waiter),
    };
    inner.wake_nodes();

    let unresolved = if waiter.wait(deadline).await {
        Vec::new()
    } else {
        let (timed_out, unresolved) = {
            let mut state = inner.state.lock();
            let timed_out = state.book.fail_pending(
                |_, entry| entry.waiters.iter().any(|w| Arc::ptr_eq(w, &waiter)),
                |(node, tp)| {
                    KrafkaError::timeout(format!(
                        "acknowledging {}-{} on node {node}",
                        tp.topic, tp.partition
                    ))
                },
            );
            let unresolved = state.book.waited_by(&waiter);
            (timed_out, unresolved)
        };
        inner.report(&timed_out);
        unresolved
    };
    drop(attached);

    let results = waiter.finish(&unresolved);
    if results.values().all(Result::is_ok) {
        inner.metrics.record_commit();
    }
    Ok(results)
}
