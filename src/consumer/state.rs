//! The consumer's subscription state: what it is subscribed to, which
//! partitions it owns, and where each of them stands.
//!
//! One [`SubscriptionState`] per consumer, behind one `parking_lot::Mutex`
//! that is never held across an `.await`. Network work happens outside the
//! lock; its result is applied in one short critical section, and only if the
//! partition's [`version`](PartitionRecord::version) still matches the one the
//! request was issued against.
//!
//! # Position
//!
//! A partition's position is the offset of the next record to hand to the
//! application. It advances only when records leave the consumer — in
//! [`SubscriptionState::complete_delivery`], after deserialization — never
//! when they are fetched and never when an offset is committed. Records that
//! have been fetched but not handed out sit in the partition's
//! [`CompletedFetch`], and a partition is fetched again only once that buffer
//! is empty, so the next fetch offset is always the buffered fetch's
//! [`next_offset`](CompletedFetch::next_offset) or, with nothing buffered, the
//! position.

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;
use tokio::time::Instant;

use ahash::{AHashMap as HashMap, AHashSet as HashSet};

use super::PartitionLag;
use super::config::{AutoOffsetReset, IsolationLevel};
use super::fetch_session::FetchSessionCache;
use super::record::{ConsumerRecord, TopicPartition};
use crate::error::{KrafkaError, Result};
use crate::{BrokerId, Offset, PartitionId};

/// A `(topic, partition)` key.
pub(super) type PartitionKey = (String, PartitionId);

/// First wait after a failed or fenced attempt on a partition.
const BACKOFF_INITIAL: Duration = Duration::from_millis(100);
/// Longest wait between attempts on a failing partition.
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Where a partition's position is reset to when it has none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OffsetReset {
    /// The log start offset.
    Earliest,
    /// The end of the log.
    Latest,
    /// The first record at or after now minus this duration; the end of the
    /// log when there is none.
    ByDuration(Duration),
}

impl OffsetReset {
    /// The `ListOffsets` timestamp that resolves this reset, reading the clock
    /// for [`ByDuration`](Self::ByDuration).
    pub(super) fn timestamp(self) -> i64 {
        match self {
            Self::Earliest => -2,
            Self::Latest => -1,
            Self::ByDuration(duration) => {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default();
                i64::try_from(now.saturating_sub(duration).as_millis()).unwrap_or(i64::MAX)
            }
        }
    }

    /// The reset `auto_offset_reset` asks for, or `None` when it forbids one.
    pub(super) fn from_auto(reset: AutoOffsetReset) -> Option<Self> {
        match reset {
            AutoOffsetReset::Earliest => Some(Self::Earliest),
            AutoOffsetReset::Latest => Some(Self::Latest),
            AutoOffsetReset::ByDuration(duration) => Some(Self::ByDuration(duration)),
            AutoOffsetReset::None => None,
        }
    }
}

/// One partition's fetched records that have not been handed out yet.
#[derive(Debug)]
pub(super) struct CompletedFetch {
    /// Decoded records, in offset order.
    pub(super) records: VecDeque<ConsumerRecord>,
    /// Where the next fetch starts once `records` is empty. It can lie beyond
    /// the last record: control batches, aborted transactions and batches the
    /// log cleaner emptied are walked through without producing records.
    pub(super) next_offset: Offset,
    /// Leader epoch of the batch `next_offset` sits just past.
    pub(super) next_epoch: Option<i32>,
}

/// Everything the consumer tracks for one assigned partition.
#[derive(Debug, Default)]
pub(super) struct PartitionRecord {
    /// Changes on every reposition and on (re)assignment, drawn from one
    /// consumer-wide counter so a partition that is revoked and assigned again
    /// never reuses a value. Results of requests issued against an older
    /// version are discarded.
    pub(super) version: u64,
    /// Offset of the next record to hand to the application. `None` while the
    /// partition waits for its committed offset or for a reset.
    pub(super) position: Option<Offset>,
    /// Leader epoch of the record just before `position` (KIP-320), sent as
    /// `last_fetched_epoch` and committed with the offset. `None` when the
    /// position came from a seek or a reset.
    pub(super) epoch: Option<i32>,
    /// A pending reset; set by `seek_to_beginning`, `seek_to_end`, an
    /// out-of-range fetch, or a new partition with no committed offset.
    pub(super) reset: Option<OffsetReset>,
    /// Whether `position` has been checked against the leader's log with
    /// `OffsetForLeaderEpoch` since it was last set.
    pub(super) validated: bool,
    /// Set by `pause()`; a paused partition is neither fetched nor delivered.
    pub(super) paused: bool,
    /// Records of this partition are on their way out in a [`Delivery`].
    pub(super) delivering: bool,
    /// Fetched records not handed out yet.
    pub(super) buffered: Option<CompletedFetch>,
    /// Latest high watermark from a fetch response.
    pub(super) high_watermark: Option<Offset>,
    /// When `high_watermark` was last updated.
    pub(super) watermark_updated_at: Option<Instant>,
    /// Latest last stable offset from a fetch response (Fetch v4+).
    pub(super) last_stable_offset: Option<Offset>,
    /// Latest log start offset from a fetch response (Fetch v5+).
    pub(super) log_start_offset: Option<Offset>,
    /// KIP-392 preferred read replica and when it expires.
    pub(super) preferred_replica: Option<(BrokerId, Instant)>,
    /// Earliest time of the next attempt on this partition and the current
    /// interval, after a failed offset lookup, a fenced fetch or a failed
    /// validation. Cleared by the first attempt that succeeds.
    pub(super) backoff: Option<(Instant, Duration)>,
}

impl PartitionRecord {
    /// The highest offset this consumer may read: the last stable offset
    /// under `read_committed` when the broker reported one, else the high
    /// watermark.
    pub(super) fn readable_end_offset(&self, isolation_level: IsolationLevel) -> Option<Offset> {
        match isolation_level {
            IsolationLevel::ReadCommitted => self.last_stable_offset.or(self.high_watermark),
            IsolationLevel::ReadUncommitted => self.high_watermark,
        }
    }

    fn backoff_elapsed(&self, now: Instant) -> bool {
        self.backoff.is_none_or(|(next, _)| now >= next)
    }

    /// The next offset a fetch for this partition would start from.
    pub(super) fn fetch_position(&self) -> Option<Offset> {
        match &self.buffered {
            Some(fetch) => Some(fetch.next_offset),
            None => self.position,
        }
    }

    fn lag(&self, isolation_level: IsolationLevel) -> Option<u64> {
        let end = self.readable_end_offset(isolation_level)?;
        let position = self.position?;
        Some((end - position).max(0) as u64)
    }
}

/// A partition about to be fetched, captured with the version the response
/// will be checked against.
#[derive(Debug, Clone)]
pub(super) struct FetchTarget {
    pub(super) key: PartitionKey,
    pub(super) version: u64,
    /// The offset the fetch starts at.
    pub(super) offset: Offset,
    /// Sent as `last_fetched_epoch`; `-1` when unknown.
    pub(super) last_fetched_epoch: i32,
    /// A live KIP-392 preferred replica, if any.
    pub(super) preferred_replica: Option<BrokerId>,
}

/// A partition whose position has to be (re)established.
#[derive(Debug, Clone)]
pub(super) struct PendingPosition {
    pub(super) key: PartitionKey,
    pub(super) version: u64,
}

/// Per-partition bookkeeping of a [`Delivery`].
#[derive(Debug)]
struct DeliveredPartition {
    key: PartitionKey,
    version: u64,
    /// How many of the delivery's records belong to this partition. Records
    /// are grouped by partition in the delivery, in this order.
    count: usize,
}

/// Records taken out of the buffered fetches, on their way to the
/// application. Positions have not moved yet.
#[derive(Debug, Default)]
pub(super) struct Delivery {
    pub(super) records: Vec<ConsumerRecord>,
    partitions: Vec<DeliveredPartition>,
}

/// The consumer's mutable state. See the module documentation.
#[derive(Debug)]
pub(super) struct SubscriptionState {
    /// Topics subscribed to (group or group-less `subscribe`) or assigned
    /// with `assign`.
    pub(super) subscription: HashSet<String>,
    /// Topics a group-less `subscribe()` resolves from cluster metadata.
    pub(super) standalone_topics: HashSet<String>,
    /// When the group-less subscription was last resolved.
    pub(super) standalone_resolved: Option<Instant>,
    /// Assigned partitions; the key set is the assignment.
    partitions: BTreeMap<PartitionKey, PartitionRecord>,
    next_version: u64,
    /// KIP-227 fetch sessions, one per broker.
    pub(super) fetch_sessions: FetchSessionCache,
    /// Round-robin cursor over the fetchable partitions, advanced per fetch.
    rotation: usize,
    /// When the last auto-commit was attempted.
    pub(super) last_auto_commit: Instant,
}

impl Default for SubscriptionState {
    fn default() -> Self {
        Self {
            subscription: HashSet::new(),
            standalone_topics: HashSet::new(),
            standalone_resolved: None,
            partitions: BTreeMap::new(),
            next_version: 0,
            fetch_sessions: FetchSessionCache::new(),
            rotation: 0,
            last_auto_commit: Instant::now(),
        }
    }
}

impl SubscriptionState {
    fn bump(&mut self) -> u64 {
        self.next_version += 1;
        self.next_version
    }

    fn not_assigned(topic: &str, partition: PartitionId) -> KrafkaError {
        KrafkaError::illegal_state(format!(
            "no current assignment for partition {topic}-{partition}"
        ))
    }

    // ── assignment ──────────────────────────────────────────────────────

    /// The current assignment, partitions sorted.
    pub(super) fn assignment(&self) -> HashMap<String, Vec<PartitionId>> {
        let mut out: HashMap<String, Vec<PartitionId>> = HashMap::new();
        for (topic, partition) in self.partitions.keys() {
            out.entry(topic.clone()).or_default().push(*partition);
        }
        out
    }

    /// Every assigned partition, in key order.
    pub(super) fn assigned_keys(&self) -> Vec<PartitionKey> {
        self.partitions.keys().cloned().collect()
    }

    pub(super) fn assigned_count(&self) -> usize {
        self.partitions.len()
    }

    pub(super) fn is_assigned(&self, key: &PartitionKey) -> bool {
        self.partitions.contains_key(key)
    }

    pub(super) fn partition(&self, key: &PartitionKey) -> Option<&PartitionRecord> {
        self.partitions.get(key)
    }

    /// Add partitions with no position. Already assigned partitions keep
    /// their state.
    pub(super) fn add_partitions(&mut self, keys: impl IntoIterator<Item = PartitionKey>) {
        for key in keys {
            if self.partitions.contains_key(&key) {
                continue;
            }
            let version = self.bump();
            self.partitions.insert(
                key,
                PartitionRecord {
                    version,
                    ..PartitionRecord::default()
                },
            );
        }
    }

    /// Drop partitions and everything known about them, buffered records
    /// included.
    pub(super) fn remove_partitions<'a>(
        &mut self,
        keys: impl IntoIterator<Item = &'a PartitionKey>,
    ) {
        for key in keys {
            self.partitions.remove(key);
        }
    }

    /// Drop every partition.
    pub(super) fn clear_assignment(&mut self) {
        self.partitions.clear();
    }

    // ── positions ───────────────────────────────────────────────────────

    /// Move an assigned partition to `offset`, dropping what is buffered for
    /// it.
    pub(super) fn seek(&mut self, key: &PartitionKey, offset: Offset) -> Result<()> {
        if !self.partitions.contains_key(key) {
            return Err(Self::not_assigned(&key.0, key.1));
        }
        let version = self.bump();
        if let Some(record) = self.partitions.get_mut(key) {
            record.version = version;
            record.position = Some(offset);
            record.epoch = None;
            record.reset = None;
            record.validated = false;
            record.buffered = None;
            record.delivering = false;
            record.backoff = None;
        }
        Ok(())
    }

    /// Ask for an assigned partition's position to be reset, dropping its
    /// position and what is buffered for it.
    pub(super) fn request_reset(&mut self, key: &PartitionKey, reset: OffsetReset) -> Result<()> {
        if !self.partitions.contains_key(key) {
            return Err(Self::not_assigned(&key.0, key.1));
        }
        let version = self.bump();
        if let Some(record) = self.partitions.get_mut(key) {
            record.version = version;
            record.position = None;
            record.epoch = None;
            record.reset = Some(reset);
            record.validated = false;
            record.buffered = None;
            record.delivering = false;
            record.backoff = None;
        }
        Ok(())
    }

    /// Reset a partition whose fetch was answered out of range, if nothing
    /// moved it since the fetch was issued.
    pub(super) fn reset_if_current(
        &mut self,
        key: &PartitionKey,
        version: u64,
        reset: OffsetReset,
    ) {
        if self
            .partitions
            .get(key)
            .is_some_and(|r| r.version == version)
        {
            let _ = self.request_reset(key, reset);
        }
    }

    /// Partitions that have neither a position nor a pending reset and are
    /// not backing off: newly assigned ones waiting for their committed
    /// offset.
    pub(super) fn partitions_needing_position(&self, now: Instant) -> Vec<PendingPosition> {
        self.partitions
            .iter()
            .filter(|(_, r)| r.position.is_none() && r.reset.is_none() && r.backoff_elapsed(now))
            .map(|(key, r)| PendingPosition {
                key: key.clone(),
                version: r.version,
            })
            .collect()
    }

    /// Partitions with a pending reset that are not backing off.
    pub(super) fn partitions_awaiting_reset(
        &self,
        now: Instant,
    ) -> Vec<(PendingPosition, OffsetReset)> {
        self.partitions
            .iter()
            .filter(|(_, r)| r.backoff_elapsed(now))
            .filter_map(|(key, r)| {
                r.reset.map(|reset| {
                    (
                        PendingPosition {
                            key: key.clone(),
                            version: r.version,
                        },
                        reset,
                    )
                })
            })
            .collect()
    }

    /// Install a position found for a partition that has none, if the
    /// partition is still the one the lookup was made for.
    pub(super) fn set_initial_position(
        &mut self,
        key: &PartitionKey,
        version: u64,
        offset: Offset,
        epoch: Option<i32>,
    ) -> bool {
        match self.partitions.get_mut(key) {
            Some(r) if r.version == version && r.position.is_none() => {
                r.position = Some(offset);
                r.epoch = epoch;
                r.reset = None;
                r.validated = false;
                r.backoff = None;
                true
            }
            _ => false,
        }
    }

    /// Record that a partition without a committed offset falls back to
    /// `reset`.
    pub(super) fn set_pending_reset(
        &mut self,
        key: &PartitionKey,
        version: u64,
        reset: OffsetReset,
    ) {
        if let Some(r) = self.partitions.get_mut(key)
            && r.version == version
            && r.position.is_none()
        {
            r.reset = Some(reset);
        }
    }

    /// Back off a partition after a failed attempt (100 ms doubling to 30 s).
    pub(super) fn back_off(&mut self, key: &PartitionKey, version: Option<u64>, now: Instant) {
        if let Some(r) = self.partitions.get_mut(key)
            && version.is_none_or(|v| v == r.version)
        {
            let previous = r.backoff.map(|(_, d)| d).unwrap_or(Duration::ZERO);
            let next = (previous * 2).clamp(BACKOFF_INITIAL, BACKOFF_MAX);
            r.backoff = Some((now + next, next));
        }
    }

    /// Rewind a partition to `end_offset` after the broker reported that its
    /// log diverged (KIP-320). Buffered records came from the discarded log.
    pub(super) fn truncate(
        &mut self,
        key: &PartitionKey,
        version: Option<u64>,
        end_offset: Offset,
    ) -> Option<Offset> {
        let current = self.partitions.get(key)?;
        if version.is_some_and(|v| v != current.version) {
            return None;
        }
        let old = current.position;
        let new_version = self.bump();
        let r = self.partitions.get_mut(key)?;
        r.version = new_version;
        r.position = Some(end_offset);
        r.epoch = None;
        r.reset = None;
        r.validated = true;
        r.buffered = None;
        r.delivering = false;
        old
    }

    /// Mark a validated position.
    pub(super) fn mark_validated(&mut self, key: &PartitionKey, version: u64) {
        if let Some(r) = self.partitions.get_mut(key)
            && r.version == version
        {
            r.validated = true;
        }
    }

    /// Mark a position as needing validation, e.g. after a fenced fetch.
    pub(super) fn mark_unvalidated(&mut self, key: &PartitionKey) {
        if let Some(r) = self.partitions.get_mut(key) {
            r.validated = false;
        }
    }

    /// Positioned partitions whose position has not been validated, with the
    /// epoch to validate against.
    pub(super) fn unvalidated(
        &self,
        now: Instant,
    ) -> Vec<(PartitionKey, u64, Offset, Option<i32>)> {
        self.partitions
            .iter()
            .filter(|(_, r)| !r.validated && !r.paused && r.backoff_elapsed(now))
            .filter_map(|(key, r)| r.position.map(|p| (key.clone(), r.version, p, r.epoch)))
            .collect()
    }

    // ── pause ───────────────────────────────────────────────────────────

    /// Pause or resume assigned partitions; unassigned ones are ignored.
    /// Returns how many assigned partitions it applied to.
    pub(super) fn set_paused(
        &mut self,
        topic: &str,
        partitions: &[PartitionId],
        paused: bool,
    ) -> usize {
        let mut applied = 0;
        for &partition in partitions {
            if let Some(r) = self.partitions.get_mut(&(topic.to_string(), partition)) {
                r.paused = paused;
                applied += 1;
            }
        }
        applied
    }

    pub(super) fn paused(&self) -> HashSet<PartitionKey> {
        self.partitions
            .iter()
            .filter(|(_, r)| r.paused)
            .map(|(key, _)| key.clone())
            .collect()
    }

    // ── fetching ────────────────────────────────────────────────────────

    /// Number of fetched records not handed out yet.
    pub(super) fn buffered_count(&self) -> usize {
        self.partitions
            .values()
            .filter_map(|r| r.buffered.as_ref())
            .map(|f| f.records.len())
            .sum()
    }

    /// Whether any unpaused partition has records ready to hand out.
    pub(super) fn has_deliverable(&self) -> bool {
        self.partitions.values().any(|r| {
            !r.paused && !r.delivering && r.buffered.as_ref().is_some_and(|f| !f.records.is_empty())
        })
    }

    /// The partitions a fetch may be issued for, rotated by one per call so
    /// that the head of the list changes from fetch to fetch (the response
    /// size limits are consumed in request order).
    ///
    /// A partition is fetchable when it is positioned, unpaused, not backing
    /// off, and has nothing buffered or in delivery.
    pub(super) fn fetch_targets(&mut self, now: Instant) -> Vec<FetchTarget> {
        let mut targets: Vec<FetchTarget> = Vec::new();
        for (key, r) in self.partitions.iter_mut() {
            if let Some((_, expiry)) = r.preferred_replica
                && now >= expiry
            {
                r.preferred_replica = None;
            }
            let Some(offset) = r.position else { continue };
            if r.paused
                || r.delivering
                || r.buffered.is_some()
                || r.reset.is_some()
                || !r.backoff_elapsed(now)
            {
                continue;
            }
            targets.push(FetchTarget {
                key: key.clone(),
                version: r.version,
                offset,
                last_fetched_epoch: r.epoch.unwrap_or(-1),
                preferred_replica: r.preferred_replica.map(|(id, _)| id),
            });
        }
        if !targets.is_empty() {
            let turn = self.rotation % targets.len();
            self.rotation = self.rotation.wrapping_add(1);
            targets.rotate_left(turn);
        }
        targets
    }

    /// Install a fetch result, if the partition is still where the fetch was
    /// issued from. A result with no records but a later next offset (control
    /// batches, compaction gaps) moves the position directly. Returns whether
    /// it was applied.
    pub(super) fn install_fetch(&mut self, target: &FetchTarget, fetch: CompletedFetch) -> bool {
        let Some(r) = self.partitions.get_mut(&target.key) else {
            return false;
        };
        if r.version != target.version
            || r.position != Some(target.offset)
            || r.buffered.is_some()
            || r.delivering
        {
            return false;
        }
        r.backoff = None;
        if fetch.records.is_empty() {
            if fetch.next_offset > target.offset {
                r.position = Some(fetch.next_offset);
                if fetch.next_epoch.is_some() {
                    r.epoch = fetch.next_epoch;
                }
            }
        } else {
            r.buffered = Some(fetch);
        }
        true
    }

    /// Clear a partition's backoff after an unfenced fetch.
    pub(super) fn clear_backoff(&mut self, key: &PartitionKey, version: u64) {
        if let Some(r) = self.partitions.get_mut(key)
            && r.version == version
        {
            r.backoff = None;
        }
    }

    /// Record the watermarks a fetch response reported.
    pub(super) fn update_watermarks(
        &mut self,
        key: &PartitionKey,
        high_watermark: Option<Offset>,
        last_stable_offset: Option<Offset>,
        log_start_offset: Option<Offset>,
        now: Instant,
    ) {
        if let Some(r) = self.partitions.get_mut(key) {
            if let Some(hw) = high_watermark {
                r.high_watermark = Some(hw);
                r.watermark_updated_at = Some(now);
            }
            if last_stable_offset.is_some() {
                r.last_stable_offset = last_stable_offset;
            }
            if log_start_offset.is_some() {
                r.log_start_offset = log_start_offset;
            }
        }
    }

    /// Set or clear a partition's preferred read replica (KIP-392).
    pub(super) fn set_preferred_replica(
        &mut self,
        key: &PartitionKey,
        replica: Option<(BrokerId, Instant)>,
    ) {
        if let Some(r) = self.partitions.get_mut(key) {
            r.preferred_replica = replica;
        }
    }

    // ── delivery ────────────────────────────────────────────────────────

    /// Take up to `max` buffered records out for delivery, without moving any
    /// position. The partitions involved are marked as delivering until
    /// [`complete_delivery`](Self::complete_delivery) or
    /// [`abort_delivery`](Self::abort_delivery).
    pub(super) fn take_delivery(&mut self, max: usize) -> Option<Delivery> {
        if max == 0 {
            return None;
        }
        let mut delivery = Delivery::default();
        let ready: Vec<PartitionKey> = self
            .partitions
            .iter()
            .filter(|(_, r)| {
                !r.paused
                    && !r.delivering
                    && r.buffered.as_ref().is_some_and(|f| !f.records.is_empty())
            })
            .map(|(key, _)| key.clone())
            .collect();
        if ready.is_empty() {
            return None;
        }
        let start = self.rotation % ready.len();
        self.rotation = self.rotation.wrapping_add(1);
        for key in ready.iter().cycle().skip(start).take(ready.len()) {
            let remaining = max - delivery.records.len();
            if remaining == 0 {
                break;
            }
            let Some(r) = self.partitions.get_mut(key) else {
                continue;
            };
            let Some(fetch) = r.buffered.as_mut() else {
                continue;
            };
            let take = remaining.min(fetch.records.len());
            delivery.records.extend(fetch.records.drain(..take));
            r.delivering = true;
            delivery.partitions.push(DeliveredPartition {
                key: key.clone(),
                version: r.version,
                count: take,
            });
        }
        Some(delivery)
    }

    /// Hand out the first `handed_out` records of `delivery` and put the rest
    /// back. Positions advance past what is handed out, in this one step.
    ///
    /// Records of a partition that was repositioned or revoked since the
    /// delivery was taken are dropped: they describe a place in the log the
    /// consumer has left.
    pub(super) fn complete_delivery(
        &mut self,
        delivery: Delivery,
        handed_out: usize,
    ) -> Vec<ConsumerRecord> {
        let Delivery {
            records,
            partitions,
        } = delivery;
        let mut records = records.into_iter();
        let mut out = Vec::with_capacity(handed_out.min(records.len()));
        let mut remaining_out = handed_out;
        for part in partitions {
            let chunk: Vec<ConsumerRecord> = records.by_ref().take(part.count).collect();
            let deliver = remaining_out.min(chunk.len());
            remaining_out -= deliver;

            let Some(r) = self.partitions.get_mut(&part.key) else {
                continue;
            };
            if r.version != part.version {
                continue;
            }
            r.delivering = false;
            let mut chunk = chunk.into_iter();
            let delivered: Vec<ConsumerRecord> = chunk.by_ref().take(deliver).collect();
            let put_back: Vec<ConsumerRecord> = chunk.collect();

            if let Some(last) = delivered.last() {
                r.position = Some(last.offset.saturating_add(1));
                if last.leader_epoch.is_some() {
                    r.epoch = last.leader_epoch;
                }
            }
            if let Some(fetch) = r.buffered.as_mut() {
                for record in put_back.into_iter().rev() {
                    fetch.records.push_front(record);
                }
                if fetch.records.is_empty() {
                    let next = fetch.next_offset;
                    let next_epoch = fetch.next_epoch;
                    r.buffered = None;
                    if r.position.is_none_or(|p| next > p) {
                        r.position = Some(next);
                        if next_epoch.is_some() {
                            r.epoch = next_epoch;
                        }
                    }
                }
            }
            out.extend(delivered);
        }
        out
    }

    /// Put every record of `delivery` back where it was taken from.
    pub(super) fn abort_delivery(&mut self, delivery: Delivery) {
        let _ = self.complete_delivery(delivery, 0);
    }

    // ── reading ─────────────────────────────────────────────────────────

    /// Positions to commit: every assigned, positioned partition.
    pub(super) fn committable(&self) -> Vec<(PartitionKey, Offset, Option<i32>)> {
        self.partitions
            .iter()
            .filter_map(|(key, r)| r.position.map(|p| (key.clone(), p, r.epoch)))
            .collect()
    }

    /// `(total, max)` lag over the partitions whose position and end offset
    /// are known.
    pub(super) fn aggregate_lag(&self, isolation_level: IsolationLevel) -> (u64, u64) {
        let mut total: u64 = 0;
        let mut max: u64 = 0;
        for r in self.partitions.values() {
            if let Some(lag) = r.lag(isolation_level) {
                total = total.saturating_add(lag);
                max = max.max(lag);
            }
        }
        (total, max)
    }

    /// Per-partition lag and the partitions whose watermark is older than
    /// `threshold`.
    pub(super) fn lag_report(
        &self,
        isolation_level: IsolationLevel,
        now: Instant,
        threshold: Duration,
    ) -> HashMap<TopicPartition, PartitionLag> {
        self.partitions
            .iter()
            .map(|((topic, partition), r)| {
                let stale = r
                    .watermark_updated_at
                    .is_none_or(|t| now.saturating_duration_since(t) > threshold);
                (
                    TopicPartition::new(topic.clone(), *partition),
                    PartitionLag {
                        position: r.position,
                        log_start_offset: r.log_start_offset,
                        high_watermark: r.high_watermark,
                        last_stable_offset: r.last_stable_offset,
                        lag: r.lag(isolation_level),
                        stale,
                    },
                )
            })
            .collect()
    }
}

/// A [`Delivery`] that puts its records back when dropped unfinished.
///
/// This is what makes a dropped `poll()` future lose nothing: the records
/// travel out of the buffer inside the guard, and only
/// [`finish`](Self::finish) — synchronous, after deserialization — advances
/// the positions.
pub(super) struct DeliveryGuard<'a> {
    state: &'a parking_lot::Mutex<SubscriptionState>,
    delivery: Option<Delivery>,
}

impl<'a> DeliveryGuard<'a> {
    pub(super) fn new(
        state: &'a parking_lot::Mutex<SubscriptionState>,
        delivery: Delivery,
    ) -> Self {
        Self {
            state,
            delivery: Some(delivery),
        }
    }

    /// The records on their way out.
    pub(super) fn records_mut(&mut self) -> &mut [ConsumerRecord] {
        match self.delivery.as_mut() {
            Some(delivery) => &mut delivery.records,
            None => &mut [],
        }
    }

    /// Hand out the first `handed_out` records and put the rest back.
    pub(super) fn finish(mut self, handed_out: usize) -> Vec<ConsumerRecord> {
        match self.delivery.take() {
            Some(delivery) => self.state.lock().complete_delivery(delivery, handed_out),
            None => Vec::new(),
        }
    }
}

impl Drop for DeliveryGuard<'_> {
    fn drop(&mut self) {
        if let Some(delivery) = self.delivery.take() {
            self.state.lock().abort_delivery(delivery);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn key(topic: &str, partition: PartitionId) -> PartitionKey {
        (topic.to_string(), partition)
    }

    fn record(topic: &str, partition: PartitionId, offset: Offset) -> ConsumerRecord {
        let mut record = ConsumerRecord::new(topic, partition, offset, None, None);
        record.leader_epoch = Some(3);
        record
    }

    fn fetched(
        topic: &str,
        partition: PartitionId,
        offsets: std::ops::Range<Offset>,
    ) -> CompletedFetch {
        CompletedFetch {
            next_offset: offsets.end,
            records: offsets.map(|o| record(topic, partition, o)).collect(),
            next_epoch: Some(3),
        }
    }

    /// A state with `t-0` at position 0 and records 0..10 buffered.
    fn buffered_state() -> SubscriptionState {
        let mut state = SubscriptionState::default();
        state.add_partitions([key("t", 0)]);
        state.seek(&key("t", 0), 0).unwrap();
        let targets = state.fetch_targets(Instant::now());
        assert_eq!(targets.len(), 1);
        assert!(state.install_fetch(&targets[0], fetched("t", 0, 0..10)));
        state
    }

    #[test]
    fn fetching_does_not_move_the_position() {
        let state = buffered_state();
        let r = state.partition(&key("t", 0)).unwrap();
        assert_eq!(r.position, Some(0));
        assert_eq!(r.fetch_position(), Some(10));
    }

    #[test]
    fn delivery_advances_the_position_only_when_finished() {
        let mut state = buffered_state();
        let delivery = state.take_delivery(4).unwrap();
        assert_eq!(delivery.records.len(), 4);
        assert_eq!(state.partition(&key("t", 0)).unwrap().position, Some(0));

        let out = state.complete_delivery(delivery, 4);
        assert_eq!(
            out.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );
        let r = state.partition(&key("t", 0)).unwrap();
        assert_eq!(r.position, Some(4));
        assert_eq!(r.epoch, Some(3));
    }

    #[test]
    fn an_aborted_delivery_puts_every_record_back() {
        let mut state = buffered_state();
        let delivery = state.take_delivery(4).unwrap();
        state.abort_delivery(delivery);
        let r = state.partition(&key("t", 0)).unwrap();
        assert_eq!(r.position, Some(0));
        let again = state.take_delivery(100).unwrap();
        assert_eq!(
            again.records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            (0..10).collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_dropped_guard_puts_the_records_back() {
        let state = parking_lot::Mutex::new(buffered_state());
        let delivery = state.lock().take_delivery(5).unwrap();
        drop(DeliveryGuard::new(&state, delivery));
        let mut state = state.into_inner();
        assert_eq!(state.partition(&key("t", 0)).unwrap().position, Some(0));
        assert_eq!(state.take_delivery(100).unwrap().records.len(), 10);
    }

    #[test]
    fn a_partial_hand_out_keeps_the_rest_in_order() {
        let mut state = buffered_state();
        let delivery = state.take_delivery(6).unwrap();
        let out = state.complete_delivery(delivery, 2);
        assert_eq!(out.len(), 2);
        assert_eq!(state.partition(&key("t", 0)).unwrap().position, Some(2));
        let next = state.take_delivery(100).unwrap();
        assert_eq!(
            next.records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            (2..10).collect::<Vec<_>>()
        );
    }

    #[test]
    fn draining_the_buffer_moves_to_the_next_fetch_offset() {
        let mut state = SubscriptionState::default();
        state.add_partitions([key("t", 0)]);
        state.seek(&key("t", 0), 0).unwrap();
        let target = state.fetch_targets(Instant::now()).remove(0);
        // Records 0..3; the batch walk reached 7 (a compacted tail).
        let mut fetch = fetched("t", 0, 0..3);
        fetch.next_offset = 7;
        assert!(state.install_fetch(&target, fetch));
        let delivery = state.take_delivery(10).unwrap();
        let _ = state.complete_delivery(delivery, 3);
        let r = state.partition(&key("t", 0)).unwrap();
        assert_eq!(r.position, Some(7));
        assert!(r.buffered.is_none());
    }

    #[test]
    fn a_partition_with_buffered_records_is_not_fetched() {
        let mut state = buffered_state();
        assert!(state.fetch_targets(Instant::now()).is_empty());
        let delivery = state.take_delivery(100).unwrap();
        assert!(
            state.fetch_targets(Instant::now()).is_empty(),
            "nor while delivering"
        );
        let _ = state.complete_delivery(delivery, 10);
        let targets = state.fetch_targets(Instant::now());
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].offset, 10);
    }

    #[test]
    fn a_seek_drops_the_buffer_and_a_stale_delivery() {
        let mut state = buffered_state();
        let delivery = state.take_delivery(3).unwrap();
        state.seek(&key("t", 0), 100).unwrap();
        let out = state.complete_delivery(delivery, 3);
        assert!(
            out.is_empty(),
            "records from before the seek are not handed out"
        );
        let r = state.partition(&key("t", 0)).unwrap();
        assert_eq!(r.position, Some(100));
        assert!(r.buffered.is_none());
    }

    #[test]
    fn a_stale_fetch_is_not_installed() {
        let mut state = SubscriptionState::default();
        state.add_partitions([key("t", 0)]);
        state.seek(&key("t", 0), 0).unwrap();
        let target = state.fetch_targets(Instant::now()).remove(0);
        state.seek(&key("t", 0), 50).unwrap();
        assert!(!state.install_fetch(&target, fetched("t", 0, 0..10)));
        assert_eq!(state.partition(&key("t", 0)).unwrap().position, Some(50));
    }

    #[test]
    fn a_fetch_for_a_revoked_and_reassigned_partition_is_not_installed() {
        let mut state = SubscriptionState::default();
        state.add_partitions([key("t", 0)]);
        state.seek(&key("t", 0), 0).unwrap();
        let target = state.fetch_targets(Instant::now()).remove(0);
        state.remove_partitions([&key("t", 0)]);
        state.add_partitions([key("t", 0)]);
        assert!(!state.install_fetch(&target, fetched("t", 0, 0..10)));
    }

    #[test]
    fn seek_rejects_an_unassigned_partition_and_stores_nothing() {
        let mut state = SubscriptionState::default();
        assert!(state.seek(&key("t", 0), 7).is_err());
        assert!(
            state
                .request_reset(&key("t", 0), OffsetReset::Earliest)
                .is_err()
        );
        state.add_partitions([key("t", 0)]);
        assert_eq!(state.partition(&key("t", 0)).unwrap().position, None);
    }

    #[test]
    fn a_new_partition_waits_for_its_position() {
        let mut state = SubscriptionState::default();
        state.add_partitions([key("t", 0)]);
        let pending = state.partitions_needing_position(Instant::now());
        assert_eq!(pending.len(), 1);
        assert!(state.fetch_targets(Instant::now()).is_empty());
        assert!(state.set_initial_position(&key("t", 0), pending[0].version, 42, Some(5)));
        let r = state.partition(&key("t", 0)).unwrap();
        assert_eq!((r.position, r.epoch), (Some(42), Some(5)));
    }

    #[test]
    fn a_reset_lookup_for_a_moved_partition_is_discarded() {
        let mut state = SubscriptionState::default();
        state.add_partitions([key("t", 0)]);
        state
            .request_reset(&key("t", 0), OffsetReset::Earliest)
            .unwrap();
        let (pending, reset) = state.partitions_awaiting_reset(Instant::now()).remove(0);
        assert_eq!(reset, OffsetReset::Earliest);
        state.seek(&key("t", 0), 9).unwrap();
        assert!(!state.set_initial_position(&pending.key, pending.version, 0, None));
        assert_eq!(state.partition(&key("t", 0)).unwrap().position, Some(9));
    }

    #[test]
    fn a_paused_partition_keeps_its_buffer_and_position() {
        let mut state = buffered_state();
        state.set_paused("t", &[0], true);
        assert!(state.take_delivery(10).is_none());
        assert_eq!(state.buffered_count(), 10);
        state.set_paused("t", &[0], false);
        assert_eq!(state.take_delivery(10).unwrap().records.len(), 10);
    }

    #[test]
    fn backoff_doubles_and_is_capped() {
        let mut state = SubscriptionState::default();
        state.add_partitions([key("t", 0)]);
        let now = Instant::now();
        let mut waits = Vec::new();
        for _ in 0..12 {
            state.back_off(&key("t", 0), None, now);
            waits.push(state.partition(&key("t", 0)).unwrap().backoff.unwrap().1);
        }
        assert_eq!(waits[0], Duration::from_millis(100));
        assert_eq!(waits[1], Duration::from_millis(200));
        assert_eq!(*waits.last().unwrap(), Duration::from_secs(30));
        assert!(state.partitions_needing_position(now).is_empty());
    }

    #[test]
    fn lag_is_measured_from_the_delivered_position() {
        let mut state = buffered_state();
        let now = Instant::now();
        state.update_watermarks(&key("t", 0), Some(10), None, None, now);
        let lag = |state: &SubscriptionState| {
            state.lag_report(
                IsolationLevel::ReadUncommitted,
                now,
                Duration::from_secs(60),
            )[&TopicPartition::new("t", 0)]
                .clone()
        };
        assert_eq!(lag(&state).lag, Some(10));
        assert_eq!(lag(&state).high_watermark, Some(10));
        assert!(!lag(&state).stale);
        let delivery = state.take_delivery(4).unwrap();
        let _ = state.complete_delivery(delivery, 4);
        assert_eq!(lag(&state).lag, Some(6));
        assert_eq!(lag(&state).position, Some(4));
    }

    #[test]
    fn readable_end_offset_follows_the_isolation_level() {
        let record = PartitionRecord {
            high_watermark: Some(100),
            last_stable_offset: Some(80),
            ..PartitionRecord::default()
        };
        assert_eq!(
            record.readable_end_offset(IsolationLevel::ReadCommitted),
            Some(80)
        );
        assert_eq!(
            record.readable_end_offset(IsolationLevel::ReadUncommitted),
            Some(100)
        );
        let no_lso = PartitionRecord {
            high_watermark: Some(100),
            ..PartitionRecord::default()
        };
        assert_eq!(
            no_lso.readable_end_offset(IsolationLevel::ReadCommitted),
            Some(100)
        );
    }

    #[test]
    fn delivery_rotates_across_partitions() {
        let mut state = SubscriptionState::default();
        state.add_partitions([key("t", 0), key("t", 1)]);
        for p in 0..2 {
            state.seek(&key("t", p), 0).unwrap();
        }
        for target in state.fetch_targets(Instant::now()) {
            let p = target.key.1;
            assert!(state.install_fetch(&target, fetched("t", p, 0..5)));
        }
        let delivery = state.take_delivery(5).unwrap();
        let first: HashSet<PartitionId> = delivery.records.iter().map(|r| r.partition).collect();
        assert_eq!(first.len(), 1, "the cap is filled from one partition first");
        let _ = state.complete_delivery(delivery, 5);
        let next = state.take_delivery(5).unwrap();
        assert!(next.records.iter().all(|r| !first.contains(&r.partition)));
    }

    #[test]
    fn truncation_rewinds_and_drops_the_buffer() {
        let mut state = buffered_state();
        let delivery = state.take_delivery(10).unwrap();
        let _ = state.complete_delivery(delivery, 10);
        assert_eq!(state.truncate(&key("t", 0), None, 6), Some(10));
        let r = state.partition(&key("t", 0)).unwrap();
        assert_eq!(r.position, Some(6));
        assert!(r.validated);
        assert!(r.epoch.is_none());
    }
}
