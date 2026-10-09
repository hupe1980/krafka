//! The ack book: acknowledgements waiting for, or riding on, a request.
//!
//! Acknowledgements are kept per partition and per **acquiring node**. A
//! record's acquisition belongs to the broker that handed it out, so its
//! acknowledgement is only ever sent there; when that broker no longer leads
//! the partition the acknowledgement fails locally instead of being re-routed
//! to a broker that never acquired the record.

use std::sync::Arc;
use tokio::time::Instant;

use ahash::AHashMap as HashMap;

use super::commit::CommitWaiter;
use crate::consumer::TopicPartition;
use crate::error::{ErrorCode, KrafkaError};
use crate::protocol::ShareAcknowledgementBatch;
use crate::{BrokerId, Offset};

/// KIP-932 acknowledgement types, by wire value (KIP-1222 adds `Renew`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AckType {
    /// The client takes no delivery of an acquired offset that holds no
    /// deliverable record (a control record, or an offset compacted away).
    Gap = 0,
    Accept = 1,
    Release = 2,
    Reject = 3,
    /// Extend the acquisition lock without completing the record.
    Renew = 4,
}

/// A run of offsets acknowledged with one type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct AckRange {
    pub first: Offset,
    pub last: Offset,
    pub kind: AckType,
}

impl AckRange {
    pub(crate) fn one(offset: Offset, kind: AckType) -> Self {
        Self {
            first: offset,
            last: offset,
            kind,
        }
    }
}

/// How a failed acknowledgement is handled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AckFailure {
    /// The request did not reach a usable session: send it again to the same
    /// node once its session is re-established.
    Retry,
    /// The node no longer leads the partition: report it and refresh metadata.
    NotLeader,
    /// The broker refused it: report it and drop it.
    Permanent,
}

/// Classify an acknowledgement error.
pub(crate) fn classify(error: &KrafkaError) -> AckFailure {
    match error {
        KrafkaError::Network(_) | KrafkaError::Timeout { .. } => AckFailure::Retry,
        KrafkaError::Broker { code, .. } => classify_code(*code),
        _ => AckFailure::Permanent,
    }
}

/// Classify a broker error code on a share request or partition.
pub(crate) fn classify_code(code: ErrorCode) -> AckFailure {
    if is_session_error(code) || code == ErrorCode::RequestTimedOut {
        AckFailure::Retry
    } else if is_leader_error(code) {
        AckFailure::NotLeader
    } else {
        AckFailure::Permanent
    }
}

/// A code that means the node's share session is gone or out of step.
pub(crate) fn is_session_error(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::ShareSessionNotFound
            | ErrorCode::InvalidShareSessionEpoch
            | ErrorCode::ShareSessionLimitReached
    )
}

/// A code that means the node does not lead the partition (any more).
pub(crate) fn is_leader_error(code: ErrorCode) -> bool {
    matches!(
        code,
        ErrorCode::NotLeaderForPartition
            | ErrorCode::FencedLeaderEpoch
            | ErrorCode::UnknownLeaderEpoch
            | ErrorCode::UnknownTopicOrPartition
            | ErrorCode::UnknownTopicId
    )
}

/// The acknowledgements of one partition acquired from one node.
#[derive(Debug)]
pub(crate) struct AckEntry {
    pub topic_id: [u8; 16],
    /// Waiting for the node's next request.
    pub pending: Vec<AckRange>,
    /// Carried by the node's request in flight.
    pub in_flight: Vec<AckRange>,
    /// When the oldest pending acknowledgement was queued.
    pub queued_at: Instant,
    /// `commit()`/`close()` calls waiting for this entry to empty.
    pub waiters: Vec<Arc<CommitWaiter>>,
}

impl AckEntry {
    fn new(topic_id: [u8; 16]) -> Self {
        Self {
            topic_id,
            pending: Vec::new(),
            in_flight: Vec::new(),
            queued_at: Instant::now(),
            waiters: Vec::new(),
        }
    }

    fn is_empty(&self) -> bool {
        self.pending.is_empty() && self.in_flight.is_empty()
    }
}

/// Key of an [`AckEntry`]: the acquiring node and the partition.
pub(crate) type AckKey = (BrokerId, TopicPartition);

/// The outcome of a resolved acknowledgement, for reporting.
#[derive(Debug)]
pub(crate) struct Resolved {
    pub partition: TopicPartition,
    pub ranges: Vec<AckRange>,
    pub result: Result<(), KrafkaError>,
}

/// Every acknowledgement the consumer has not resolved yet.
#[derive(Debug, Default)]
pub(crate) struct AckBook {
    entries: HashMap<AckKey, AckEntry>,
}

impl AckBook {
    /// Queue `range` for the partition's acquiring `node`. A later
    /// acknowledgement of an offset replaces a pending earlier one, so a
    /// renewed record that is then accepted is sent once, as accepted.
    pub(crate) fn add(
        &mut self,
        node: BrokerId,
        partition: TopicPartition,
        topic_id: [u8; 16],
        range: AckRange,
    ) {
        let entry = self
            .entries
            .entry((node, partition))
            .or_insert_with(|| AckEntry::new(topic_id));
        if entry.pending.is_empty() {
            entry.queued_at = Instant::now();
        }
        insert_range(&mut entry.pending, range);
    }

    /// Whether `node` has acknowledgements waiting to be sent.
    pub(crate) fn has_pending(&self, node: BrokerId) -> bool {
        self.entries
            .iter()
            .any(|((n, _), e)| *n == node && !e.pending.is_empty())
    }

    /// The nodes that have acknowledgements.
    pub(crate) fn nodes(&self) -> Vec<BrokerId> {
        let mut nodes: Vec<BrokerId> = self.entries.keys().map(|(n, _)| *n).collect();
        nodes.sort_unstable();
        nodes.dedup();
        nodes
    }

    /// Iterate the pending entries of `node`.
    pub(crate) fn pending_for(
        &self,
        node: BrokerId,
    ) -> impl Iterator<Item = (&TopicPartition, &AckEntry)> {
        self.entries
            .iter()
            .filter(move |((n, _), e)| *n == node && !e.pending.is_empty())
            .map(|((_, tp), e)| (tp, e))
    }

    /// Move the pending acknowledgements of `node` for `partitions` (all of
    /// them when `None`) in flight, returning what to put on the wire.
    pub(crate) fn take(
        &mut self,
        node: BrokerId,
        partitions: Option<&[TopicPartition]>,
    ) -> Vec<(TopicPartition, [u8; 16], Vec<AckRange>)> {
        let mut out = Vec::new();
        for ((n, tp), entry) in &mut self.entries {
            if *n != node || entry.pending.is_empty() || !entry.in_flight.is_empty() {
                continue;
            }
            if partitions.is_some_and(|ps| !ps.contains(tp)) {
                continue;
            }
            entry.in_flight = std::mem::take(&mut entry.pending);
            out.push((tp.clone(), entry.topic_id, entry.in_flight.clone()));
        }
        out.sort_by(|a, b| (&a.0.topic, a.0.partition).cmp(&(&b.0.topic, b.0.partition)));
        out
    }

    /// Settle the in-flight acknowledgements of one entry.
    ///
    /// `Ok`, and failures that are not retried, resolve them. A failure of the
    /// [`AckFailure::Retry`] class puts them back in front of anything queued
    /// since, provided `retry_cutoff` is given and they were queued after it;
    /// otherwise it resolves them as timed out.
    pub(crate) fn settle(
        &mut self,
        node: BrokerId,
        partition: &TopicPartition,
        result: Result<(), KrafkaError>,
        retry_cutoff: Option<Instant>,
    ) -> Option<Resolved> {
        let key = (node, partition.clone());
        let entry = self.entries.get_mut(&key)?;
        let in_flight = std::mem::take(&mut entry.in_flight);
        if in_flight.is_empty() {
            return None;
        }
        let result = match result {
            Err(error) if classify(&error) == AckFailure::Retry => {
                if retry_cutoff.is_some_and(|cutoff| entry.queued_at >= cutoff) {
                    let mut retried = in_flight;
                    for range in std::mem::take(&mut entry.pending) {
                        insert_range(&mut retried, range);
                    }
                    entry.pending = retried;
                    return None;
                }
                Err(KrafkaError::timeout(format!(
                    "acknowledging {}-{} on node {node} ({error})",
                    partition.topic, partition.partition
                )))
            }
            other => other,
        };
        let resolved = Resolved {
            partition: partition.clone(),
            ranges: in_flight,
            result,
        };
        self.finish(&key, &resolved);
        Some(resolved)
    }

    /// Resolve every pending acknowledgement of the entries `select` picks
    /// with `error`, without sending them.
    pub(crate) fn fail_pending(
        &mut self,
        mut select: impl FnMut(&AckKey, &AckEntry) -> bool,
        error: impl Fn(&AckKey) -> KrafkaError,
    ) -> Vec<Resolved> {
        let keys: Vec<AckKey> = self
            .entries
            .iter()
            .filter(|(k, e)| !e.pending.is_empty() && select(k, e))
            .map(|(k, _)| k.clone())
            .collect();
        let mut out = Vec::new();
        for key in keys {
            let Some(entry) = self.entries.get_mut(&key) else {
                continue;
            };
            let resolved = Resolved {
                partition: key.1.clone(),
                ranges: std::mem::take(&mut entry.pending),
                result: Err(error(&key)),
            };
            self.finish(&key, &resolved);
            out.push(resolved);
        }
        out
    }

    /// Attach `waiter` to every entry, returning how many it waits for.
    pub(crate) fn attach(&mut self, waiter: &Arc<CommitWaiter>) -> usize {
        for entry in self.entries.values_mut() {
            entry.waiters.push(Arc::clone(waiter));
        }
        self.entries.len()
    }

    /// Detach `waiter` from every entry.
    pub(crate) fn detach(&mut self, waiter: &Arc<CommitWaiter>) {
        for entry in self.entries.values_mut() {
            entry.waiters.retain(|w| !Arc::ptr_eq(w, waiter));
        }
    }

    /// The partitions of the entries `waiter` is attached to.
    pub(crate) fn waited_by(&self, waiter: &Arc<CommitWaiter>) -> Vec<TopicPartition> {
        self.entries
            .iter()
            .filter(|(_, e)| e.waiters.iter().any(|w| Arc::ptr_eq(w, waiter)))
            .map(|((_, tp), _)| tp.clone())
            .collect()
    }

    /// Whether any pending entry of `node` has a waiter attached.
    pub(crate) fn has_waiters(&self, node: BrokerId) -> bool {
        self.pending_for(node).any(|(_, e)| !e.waiters.is_empty())
    }

    /// Record a resolution with the entry's waiters and drop the entry once
    /// nothing of it is left.
    fn finish(&mut self, key: &AckKey, resolved: &Resolved) {
        let Some(entry) = self.entries.get(key) else {
            return;
        };
        for waiter in &entry.waiters {
            waiter.record(&resolved.partition, &resolved.result);
        }
        if entry.is_empty()
            && let Some(entry) = self.entries.remove(key)
        {
            for waiter in entry.waiters {
                waiter.entry_done();
            }
        }
    }
}

/// Insert `range` into `ranges`, replacing whatever it overlaps.
fn insert_range(ranges: &mut Vec<AckRange>, range: AckRange) {
    if let Some(last) = ranges.last_mut() {
        if last.last < range.first {
            if last.kind == range.kind && last.last + 1 == range.first {
                last.last = range.last;
            } else {
                ranges.push(range);
            }
            return;
        }
    } else {
        ranges.push(range);
        return;
    }
    let mut out = Vec::with_capacity(ranges.len() + 1);
    for existing in ranges.drain(..) {
        if existing.last < range.first || existing.first > range.last {
            out.push(existing);
            continue;
        }
        if existing.first < range.first {
            out.push(AckRange {
                last: range.first - 1,
                ..existing
            });
        }
        if existing.last > range.last {
            out.push(AckRange {
                first: range.last + 1,
                ..existing
            });
        }
    }
    out.push(range);
    out.sort_unstable_by_key(|r| r.first);
    *ranges = out;
}

/// The wire form of `ranges`: sorted by offset, contiguous runs of one type
/// merged, which is the form the Java client sends.
pub(crate) fn to_batches(ranges: &[AckRange]) -> Vec<ShareAcknowledgementBatch> {
    let mut sorted = ranges.to_vec();
    sorted.sort_unstable_by_key(|r| r.first);
    let mut batches: Vec<ShareAcknowledgementBatch> = Vec::with_capacity(sorted.len());
    for range in sorted {
        let kind = range.kind as i8;
        match batches.last_mut() {
            Some(prev)
                if prev.last_offset.checked_add(1) == Some(range.first)
                    && prev.acknowledge_types == [kind] =>
            {
                prev.last_offset = range.last;
            }
            _ => batches.push(ShareAcknowledgementBatch {
                first_offset: range.first,
                last_offset: range.last,
                acknowledge_types: vec![kind],
            }),
        }
    }
    batches
}

/// Whether any range renews a lock.
pub(crate) fn has_renew(ranges: &[AckRange]) -> bool {
    ranges.iter().any(|r| r.kind == AckType::Renew)
}
