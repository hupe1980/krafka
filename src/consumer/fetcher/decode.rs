//! The batch walk: turning one partition's fetched bytes into records and the
//! offset the next fetch starts at.

use ahash::AHashSet as HashSet;
use tracing::{trace, warn};

use crate::Offset;
use crate::PartitionId;
use crate::consumer::ConsumerRecord;
use crate::error::{KrafkaError, ProtocolErrorKind};
use crate::protocol::{RecordBatch, RecordBatchHeader};

/// Whether a control batch carries an **abort** marker (as opposed to a commit
/// marker, or a control record type this client does not know).
///
/// The control record's key is `[version: i16][type: i16]`, where type `0` is
/// ABORT and `1` is COMMIT (`ControlRecordType` in the Java client). The
/// `read_committed` filter deactivates a producer's aborted-transaction state
/// only on an abort marker; treating a commit marker as an end-of-abort would
/// let the data records of a still-aborted transaction through.
///
/// Returns `false` for a control batch whose key is missing or too short —
/// the conservative answer, since it leaves the filter engaged.
pub(super) fn control_batch_is_abort(batch: &RecordBatch) -> bool {
    const CONTROL_TYPE_ABORT: i16 = 0;
    batch.records.first().is_some_and(|record| {
        record.key.as_ref().is_some_and(|key| {
            key.len() >= 4 && i16::from_be_bytes([key[2], key[3]]) == CONTROL_TYPE_ABORT
        })
    })
}

/// What walking one partition's fetched batches produced, besides the records
/// themselves (those are appended to the caller's shared output vector).
pub(super) struct PartitionDecodeOutcome {
    /// Highest offset the position advanced through: the last record pushed,
    /// or the end of the last batch that was skipped or drained in full.
    /// `None` when nothing in the payload could move the position.
    pub(super) last_offset: Option<Offset>,
    /// Leader epoch of the batch `last_offset` came from, reported back on
    /// the next fetch so the broker can detect divergence (KIP-320).
    pub(super) last_epoch: i32,
    /// The batch at the fetch position could not be decoded, so the partition
    /// cannot advance. The caller reports it as a partition-level fault.
    pub(super) error: Option<KrafkaError>,
    /// A batch failed to decode for a reason other than the benign
    /// truncated-tail case; the caller records it on the metrics sink.
    pub(super) corrupt: bool,
}

/// Advance a partition's pending position bookkeeping through `batch_end`.
///
/// Every batch-level advance — control batches, aborted transactions,
/// compaction-emptied batches, and batches drained to their last record —
/// goes through here so one place enforces that a fetch response can only
/// move the position *forward*: a batch lying entirely below the fetch
/// position is ignored (its offsets were delivered by earlier polls), and a
/// batch end at or below what this response already advanced through leaves
/// the bookkeeping untouched. Batches arrive in offset order, so both guards
/// are expected to be inert; they exist so a misbehaving broker degrades to a
/// visible stall instead of silently rewinding and re-delivering records.
pub(super) fn advance_through_batch(
    last_offset: &mut Option<Offset>,
    last_epoch: &mut i32,
    fetch_offset: Offset,
    batch_end: Offset,
    batch_epoch: i32,
) {
    if batch_end < fetch_offset {
        return;
    }
    if last_offset.is_none_or(|advanced| batch_end > advanced) {
        *last_offset = Some(batch_end);
        *last_epoch = batch_epoch;
    }
}

/// Walk one partition's fetched record bytes, appending deliverable records
/// to `records` and tracking how far the partition's position may advance.
///
/// The walk is where every "this offset range carries nothing to deliver"
/// case is decided — control batches, aborted transactions, batches the log
/// cleaner emptied, batches whose surviving records all predate the fetch
/// offset — and each of those cases must still advance the position past the
/// range it covers, or the partition re-fetches the same bytes forever. It is
/// a free function, welded to no socket, precisely so those decisions can be
/// pinned down by unit tests feeding it hand-built batches.
///
/// `partition_fetch_offset` is the position the fetch was issued from;
/// records below it were delivered by an earlier poll and are skipped.
/// `record_budget`, when present, is how many records this call may still
/// decode; it is decremented by what is decoded.
#[allow(clippy::too_many_arguments)]
pub(super) fn decode_partition_batches(
    topic_name: &str,
    partition: PartitionId,
    mut batch_buf: bytes::Bytes,
    partition_fetch_offset: Offset,
    mut aborted_txns: Vec<crate::protocol::AbortedTransaction>,
    mut record_budget: Option<&mut usize>,
    max_decompressed_size: usize,
    records: &mut Vec<ConsumerRecord>,
) -> PartitionDecodeOutcome {
    let mut outcome = PartitionDecodeOutcome {
        last_offset: None,
        last_epoch: -1,
        error: None,
        corrupt: false,
    };

    // For READ_COMMITTED, the broker still includes data batches from aborted
    // transactions in the FetchResponse but lists their (producer_id,
    // first_offset) pairs in `aborted_transactions` so the client can filter
    // them. Control batches (abort/commit markers) are filtered below either
    // way; this state machine handles the data records themselves.
    //
    // Sort by first_offset so entries can be activated in-order as the walk
    // reaches them.
    aborted_txns.sort_unstable_by_key(|at| at.first_offset);
    // One allocation per partition response; every record shares it.
    let topic: std::sync::Arc<str> = std::sync::Arc::from(topic_name);
    let mut aborted_txns_iter = aborted_txns.iter().peekable();
    // Producer IDs currently inside an open aborted transaction.
    let mut aborted_producers: HashSet<i64> = HashSet::new();

    // Decode fetched batches, bounded by the poll's remaining record budget.
    // The position advances only over records that are actually pushed and
    // batches that are walked in full, so a batch cut short by the budget is
    // re-fetched rather than skipped.
    let mut budget_exhausted = false;
    while batch_buf.len() >= 12 {
        // Decoding a batch only to discard it is the waste the budget bounds.
        if record_budget.as_deref().is_some_and(|b| *b == 0) {
            break;
        }
        // Header first: a data batch of an aborted transaction is skipped
        // without decompressing or decoding its records.
        if let Ok(header) = RecordBatchHeader::peek(&batch_buf)
            && header.attributes.is_transactional
            && !header.attributes.is_control_batch
            && batch_buf.len() >= header.total_size()
        {
            while aborted_txns_iter
                .peek()
                .is_some_and(|at| at.first_offset <= header.base_offset)
            {
                if let Some(at) = aborted_txns_iter.next() {
                    aborted_producers.insert(at.producer_id);
                }
            }
            if aborted_producers.contains(&header.producer_id) {
                advance_through_batch(
                    &mut outcome.last_offset,
                    &mut outcome.last_epoch,
                    partition_fetch_offset,
                    header.last_offset(),
                    header.partition_leader_epoch,
                );
                let _ = batch_buf.split_to(header.total_size());
                continue;
            }
        }
        match RecordBatch::decode_with_limit(&mut batch_buf, max_decompressed_size) {
            Ok(batch) => {
                let batch_epoch = batch.partition_leader_epoch;
                // The offset range this batch spans, whether or not any
                // record inside it survives to delivery. `last_offset_delta`
                // is preserved by the log cleaner even when the records it
                // once counted are gone, so this is the only trustworthy
                // measure of how far the batch reaches.
                let batch_end = batch
                    .base_offset
                    .saturating_add(batch.last_offset_delta as i64);

                // Advance the aborted-transaction state machine. Activate any
                // AbortedTransaction entries whose first_offset has been
                // reached. The list is sorted by first_offset so we only peek
                // at the front.
                while aborted_txns_iter
                    .peek()
                    .is_some_and(|at| at.first_offset <= batch.base_offset)
                {
                    if let Some(at) = aborted_txns_iter.next() {
                        aborted_producers.insert(at.producer_id);
                    }
                }

                // Skip transaction control batches (commit/abort markers).
                // These are internal Kafka bookkeeping records that must not
                // be surfaced to consumers.  When the control batch belongs
                // to a tracked aborted producer we also deactivate it so
                // subsequent transactions from the same producer are not
                // incorrectly filtered.  The offset must still be advanced
                // past them so that subsequent fetches do not re-process them.
                if batch.attributes.is_control_batch {
                    // Only an ABORT marker ends an aborted
                    // transaction. Clearing on any control
                    // batch (including COMMIT markers) meant
                    // the client trusted the marker *type* it
                    // never looked at, so a coordinator that
                    // wrote a commit marker for a producer the
                    // broker had listed as aborted would stop
                    // the filter early and surface aborted
                    // records to a `read_committed` consumer.
                    // Mirrors the Java client's
                    // `CompletedFetch.containsAbortMarker`.
                    if control_batch_is_abort(&batch) {
                        aborted_producers.remove(&batch.producer_id);
                    }
                    advance_through_batch(
                        &mut outcome.last_offset,
                        &mut outcome.last_epoch,
                        partition_fetch_offset,
                        batch_end,
                        batch_epoch,
                    );
                    continue;
                }

                // Skip data records from aborted transactions.
                // Once the abort marker (control batch) is seen,
                // the producer_id is removed from the set, so
                // later committed transactions from the same
                // producer are not affected.
                if batch.attributes.is_transactional
                    && aborted_producers.contains(&batch.producer_id)
                {
                    advance_through_batch(
                        &mut outcome.last_offset,
                        &mut outcome.last_epoch,
                        partition_fetch_offset,
                        batch_end,
                        batch_epoch,
                    );
                    continue;
                }

                // The log cleaner keeps a batch's header (to preserve the
                // producer's last sequence number for idempotence) even after
                // compaction has removed every record it originally held.
                // Such a batch carries nothing to deliver, but the offset
                // range it spans is real and must still be skipped —
                // otherwise the position never leaves it and every subsequent
                // fetch re-reads the same empty batch forever. Worse, the
                // budget check below would read an empty batch as "budget
                // exhausted" and throw away every later batch in the response.
                if batch.records.is_empty() {
                    advance_through_batch(
                        &mut outcome.last_offset,
                        &mut outcome.last_epoch,
                        partition_fetch_offset,
                        batch_end,
                        batch_epoch,
                    );
                    continue;
                }

                // Records skipped as already delivered do not consume the
                // budget.
                let allowed = record_budget.as_deref().copied().unwrap_or(usize::MAX);
                let mut used = 0usize;

                for record in batch.records.into_iter() {
                    // Use offset_delta for correct offset in compacted topics
                    // where records may have been deleted (log compaction awareness).
                    let record_offset =
                        batch.base_offset.saturating_add(record.offset_delta as i64);

                    // Skip records below the fetch offset — these were
                    // already delivered in a prior poll but are included
                    // because Kafka returns whole batches.
                    if record_offset < partition_fetch_offset {
                        continue;
                    }

                    if used == allowed {
                        budget_exhausted = true;
                        break;
                    }
                    used += 1;

                    records.push(ConsumerRecord {
                        topic: std::sync::Arc::clone(&topic),
                        partition,
                        offset: record_offset,
                        timestamp: batch.base_timestamp.saturating_add(record.timestamp_delta),
                        timestamp_type: batch.attributes.timestamp_type,
                        key: record.key,
                        value: record.value,
                        headers: crate::consumer::headers_from_wire(record.headers),
                        leader_epoch: Some(batch_epoch),
                        delivery_count: None,
                    });
                    outcome.last_offset = Some(record_offset);
                    outcome.last_epoch = batch_epoch;
                }

                if let Some(budget) = record_budget.as_deref_mut() {
                    *budget -= used;
                }
                if budget_exhausted {
                    // The batch was cut short mid-delivery: the position must
                    // stop at the last record pushed so the remainder is
                    // re-fetched, not skipped. Deliberately no batch-level
                    // advance here.
                    break;
                }

                // The batch was walked to its end, so the position moves to
                // the end of the *offset range* it spans — not merely past
                // its last surviving record. The two differ on compacted
                // topics: when the cleaner has removed a batch's trailing
                // records, `batch_end` reaches beyond the last record, and a
                // position parked between them re-fetches this same batch,
                // finds every record below the fetch offset, delivers
                // nothing, and never advances again. Mirrors the Java
                // client's `CompletedFetch.nextFetchOffset`, which jumps to
                // `batch.nextOffset()` once a batch is drained.
                advance_through_batch(
                    &mut outcome.last_offset,
                    &mut outcome.last_epoch,
                    partition_fetch_offset,
                    batch_end,
                    batch_epoch,
                );
            }
            Err(e) => {
                // Two very different situations arrive here.
                //
                // 1. The broker cut the response at
                //    `partition_max_bytes`, so the trailing
                //    batch is incomplete. Expected and benign:
                //    the prefix that did decode advances the
                //    position and the next fetch re-requests
                //    the rest.
                //
                // 2. The bytes are corrupt (CRC mismatch,
                //    unsupported magic, an out-of-range field).
                //    When the *first* batch is the bad one,
                //    nothing decodes and no offset update is
                //    produced, so breaking silently would
                //    stall the partition at that offset.
                //
                // A decode error that leaves the partition
                // unable to advance is therefore reported to
                // the caller. Whether it is truncation or
                // corruption, a partition that cannot get past
                // its current offset is a stall, and a stall
                // the application cannot see is worse than one
                // it can.
                let made_progress = outcome.last_offset.is_some();
                let truncated_tail =
                    e.protocol_error_kind() == Some(ProtocolErrorKind::TruncatedFrame);

                if made_progress && truncated_tail {
                    trace!(
                        topic = %topic_name,
                        partition,
                        "Trailing record batch truncated by the fetch size limit"
                    );
                    break;
                }

                outcome.corrupt = true;

                if made_progress {
                    // Deliver the good prefix, but do not let
                    // the corruption pass unremarked — the next
                    // fetch will start at the bad batch and
                    // surface it through the branch below.
                    warn!(
                        topic = %topic_name,
                        partition,
                        fetch_offset = partition_fetch_offset,
                        error = %e,
                        "Corrupt record batch after a decodable prefix; \
                         delivering the prefix and stopping here"
                    );
                    break;
                }

                // Nothing decoded: this partition is stuck at
                // `partition_fetch_offset`. Report it as a
                // partition-level fault so the caller can move on to the
                // next partition — failing the whole request
                // here would throw away records from every
                // healthy partition that shares this leader,
                // which is a far larger blast radius than the
                // fault deserves.
                warn!(
                    topic = %topic_name,
                    partition,
                    fetch_offset = partition_fetch_offset,
                    error = %e,
                    "Record batch at the fetch position could not be decoded; \
                     the partition cannot advance"
                );
                outcome.error = Some(e);
                break;
            }
        }
    }

    outcome
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    // ── only ABORT markers end an aborted transaction ──

    #[test]
    fn control_batch_type_is_read_from_the_marker_key() {
        use crate::protocol::{Record, RecordBatch};

        let marker = |control_type: i16| {
            let mut key = bytes::BytesMut::new();
            bytes::BufMut::put_i16(&mut key, 0); // control-record version
            bytes::BufMut::put_i16(&mut key, control_type);
            let mut batch = RecordBatch::new();
            batch.attributes.is_control_batch = true;
            batch.add_record(Record {
                attributes: 0,
                timestamp_delta: 0,
                offset_delta: 0,
                key: Some(key.freeze()),
                value: Some(bytes::Bytes::new()),
                headers: Vec::new(),
            });
            batch
        };

        assert!(control_batch_is_abort(&marker(0)), "type 0 is ABORT");
        assert!(!control_batch_is_abort(&marker(1)), "type 1 is COMMIT");

        // A malformed or absent key leaves the filter engaged — the
        // conservative answer, since releasing it would surface aborted data.
        let mut short = RecordBatch::new();
        short.attributes.is_control_batch = true;
        short.add_record(Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key: Some(bytes::Bytes::from_static(&[0, 0])),
            value: None,
            headers: Vec::new(),
        });
        assert!(!control_batch_is_abort(&short));

        let mut keyless = RecordBatch::new();
        keyless.attributes.is_control_batch = true;
        assert!(!control_batch_is_abort(&keyless));
    }

    // ── decode_partition_batches: the batch walk that advances the position ──
    //
    // These pin down the cases where a batch's offset range carries nothing to
    // deliver but must still be advanced through, or the partition re-fetches
    // the same bytes forever. All of them are the log cleaner's doing: on a
    // compacted topic a batch can outlive its records (the header is retained
    // for producer idempotence) or keep a `last_offset_delta` that reaches
    // beyond its last surviving record.

    /// Leader epoch stamped on every test batch, so tests can assert the
    /// epoch is reported alongside the position advance (KIP-320).
    const WALK_EPOCH: i32 = 7;

    fn walk_record(offset_delta: i32) -> crate::protocol::Record {
        crate::protocol::Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta,
            key: None,
            value: Some(bytes::Bytes::from_static(b"v")),
            headers: Vec::new(),
        }
    }

    /// A data batch spanning `base..=base + last_offset_delta` whose surviving
    /// records sit at the given deltas — the shape the log cleaner leaves
    /// behind when compaction removes records from a batch. An empty `deltas`
    /// is the fully-emptied batch the cleaner retains for producer state.
    fn walk_batch(base_offset: i64, last_offset_delta: i32, deltas: &[i32]) -> RecordBatch {
        let mut batch = RecordBatch::new();
        batch.base_offset = base_offset;
        batch.last_offset_delta = last_offset_delta;
        batch.partition_leader_epoch = WALK_EPOCH;
        for &delta in deltas {
            batch.add_record(walk_record(delta));
        }
        batch
    }

    /// Encode `batches` back-to-back — the wire shape of a fetch response
    /// payload — and walk them from `fetch_offset`.
    fn walk(
        batches: &[RecordBatch],
        fetch_offset: Offset,
        budget: Option<&mut usize>,
    ) -> (Vec<ConsumerRecord>, PartitionDecodeOutcome) {
        let mut buf = bytes::BytesMut::new();
        for batch in batches {
            buf.extend_from_slice(&batch.encode().expect("encode batch"));
        }
        let mut records = Vec::new();
        let outcome = decode_partition_batches(
            "walk-topic",
            0,
            buf.freeze(),
            fetch_offset,
            Vec::new(),
            budget,
            RecordBatch::MAX_DECOMPRESSED_SIZE,
            &mut records,
        );
        (records, outcome)
    }

    #[test]
    fn compaction_emptied_batch_is_skipped_not_a_stall() {
        // The cleaner kept only the header: base 1000, spanning 1000..=1004,
        // zero records. The walk must advance through the whole span.
        let (records, outcome) = walk(&[walk_batch(1000, 4, &[])], 1000, None);

        assert!(records.is_empty(), "an emptied batch delivers nothing");
        assert_eq!(
            outcome.last_offset,
            Some(1004),
            "the position must advance through the emptied batch's span, \
             or the next fetch re-reads it forever"
        );
        assert_eq!(outcome.last_epoch, WALK_EPOCH);
        assert!(outcome.error.is_none());
        assert!(!outcome.corrupt);
    }

    #[test]
    fn compaction_emptied_batch_does_not_discard_the_rest_of_the_response() {
        // An empty batch must not read as "budget exhausted": the walk
        // continues to every later batch in the response, with and without a
        // budget configured.
        let batches = [walk_batch(1000, 4, &[]), walk_batch(1005, 1, &[0, 1])];

        for mut budget in [None, Some(512usize)] {
            let (records, outcome) = walk(&batches, 1000, budget.as_mut());

            let offsets: Vec<Offset> = records.iter().map(|r| r.offset).collect();
            assert_eq!(offsets, vec![1005, 1006]);
            assert_eq!(outcome.last_offset, Some(1006));
            if let Some(budget) = budget {
                assert_eq!(
                    budget, 510,
                    "the emptied batch must not consume budget slots"
                );
            }
        }
    }

    #[test]
    fn drained_compacted_batch_advances_past_its_removed_tail() {
        // Compaction removed the records at deltas 1, 2, 4, and 6..=9; the
        // batch still spans 100..=109. Draining it must move the position to
        // the end of the *span*, not to the last surviving record — parking
        // at 106 would re-fetch this same batch and deliver nothing.
        let (records, outcome) = walk(&[walk_batch(100, 9, &[0, 3, 5])], 100, None);

        let offsets: Vec<Offset> = records.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, vec![100, 103, 105]);
        assert!(records.iter().all(|r| r.leader_epoch == Some(WALK_EPOCH)));
        assert_eq!(
            outcome.last_offset,
            Some(109),
            "a drained batch advances to its span end (Java client's nextFetchOffset)"
        );
    }

    #[test]
    fn position_inside_a_compacted_batch_escapes_it() {
        // The consumer delivered through 105 and parked at 106 — inside the
        // batch's span, above its last surviving record. The broker returns
        // the same batch; every record is below the fetch offset, and the
        // position must still advance past the batch.
        let (records, outcome) = walk(&[walk_batch(100, 9, &[0, 3, 5])], 106, None);

        assert!(
            records.is_empty(),
            "everything in the batch was already delivered"
        );
        assert_eq!(
            outcome.last_offset,
            Some(109),
            "the position must escape the straddling batch"
        );
        assert_eq!(outcome.last_epoch, WALK_EPOCH);
    }

    #[test]
    fn budget_cut_batch_re_fetches_its_remainder() {
        // The batch-span advance applies only to batches walked in full: a
        // batch cut short by `max_poll_records` must leave the position at
        // the last delivered record so the remainder is re-fetched.
        let mut budget = 2usize;
        let (records, outcome) = walk(&[walk_batch(100, 2, &[0, 1, 2])], 100, Some(&mut budget));

        let offsets: Vec<Offset> = records.iter().map(|r| r.offset).collect();
        assert_eq!(offsets, vec![100, 101]);
        assert_eq!(
            outcome.last_offset,
            Some(101),
            "a budget-cut batch must not be skipped over"
        );
        assert_eq!(budget, 0);
    }

    #[test]
    fn batch_below_the_fetch_position_cannot_rewind_it() {
        // A batch lying entirely below the fetch position describes offsets
        // already delivered. Advancing "through" it would compute a next
        // position *behind* the current one and re-deliver records. No broker
        // should send this shape; the guard keeps a buggy one duplicate-free.
        let (records, outcome) = walk(&[walk_batch(100, 4, &[])], 200, None);

        assert!(records.is_empty());
        assert_eq!(
            outcome.last_offset, None,
            "no position update at all beats a backwards one"
        );
    }

    #[test]
    fn truncated_tail_after_a_skipped_batch_is_benign() {
        // The broker cut the response mid-batch (partition_max_bytes). When
        // the walk already advanced — even if only through an emptied batch,
        // with nothing delivered — the truncation is the expected benign case,
        // not a fault: the advance is what proves the next fetch will make
        // progress past it.
        let mut buf = bytes::BytesMut::new();
        buf.extend_from_slice(&walk_batch(1000, 4, &[]).encode().expect("encode"));
        let tail = walk_batch(1005, 1, &[0, 1]).encode().expect("encode");
        buf.extend_from_slice(&tail[..tail.len() - 5]);

        let mut records = Vec::new();
        let outcome = decode_partition_batches(
            "walk-topic",
            0,
            buf.freeze(),
            1000,
            Vec::new(),
            None,
            RecordBatch::MAX_DECOMPRESSED_SIZE,
            &mut records,
        );

        assert!(records.is_empty());
        assert_eq!(outcome.last_offset, Some(1004));
        assert!(outcome.error.is_none(), "a truncated tail is not a fault");
        assert!(!outcome.corrupt);
    }

    #[test]
    fn corrupt_first_batch_reports_a_fault() {
        // Nothing decoded and nothing advanced: the partition is stuck at the
        // fetch offset, and the walk must say so rather than stall silently.
        let mut bad = bytes::BytesMut::from(
            walk_batch(1000, 1, &[0, 1])
                .encode()
                .expect("encode")
                .as_ref(),
        );
        bad[30] ^= 0xFF; // flip a CRC-covered byte

        let mut records = Vec::new();
        let outcome = decode_partition_batches(
            "walk-topic",
            0,
            bad.freeze(),
            1000,
            Vec::new(),
            None,
            RecordBatch::MAX_DECOMPRESSED_SIZE,
            &mut records,
        );

        assert!(records.is_empty());
        assert_eq!(outcome.last_offset, None);
        assert!(
            outcome.error.is_some(),
            "an undecodable position is a reported fault"
        );
        assert!(outcome.corrupt);
    }

    #[test]
    fn aborted_transaction_batches_are_filtered_but_advance_the_position() {
        // producer 9's transaction at 100..=102 was aborted; its abort marker
        // sits at 103; a later batch from the same producer at 104..=105 is
        // committed. READ_COMMITTED must deliver only the latter, while the
        // position still advances through everything it skipped.
        let mut aborted_data = walk_batch(100, 2, &[0, 1, 2]);
        aborted_data.attributes.is_transactional = true;
        aborted_data.producer_id = 9;

        let mut marker_key = bytes::BytesMut::new();
        bytes::BufMut::put_i16(&mut marker_key, 0); // control-record version
        bytes::BufMut::put_i16(&mut marker_key, 0); // type 0 = ABORT
        let mut abort_marker = walk_batch(103, 0, &[]);
        abort_marker.attributes.is_control_batch = true;
        abort_marker.producer_id = 9;
        abort_marker.add_record(crate::protocol::Record {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key: Some(marker_key.freeze()),
            value: Some(bytes::Bytes::new()),
            headers: Vec::new(),
        });

        let mut committed_data = walk_batch(104, 1, &[0, 1]);
        committed_data.attributes.is_transactional = true;
        committed_data.producer_id = 9;

        let mut buf = bytes::BytesMut::new();
        for batch in [&aborted_data, &abort_marker, &committed_data] {
            buf.extend_from_slice(&batch.encode().expect("encode"));
        }

        let mut records = Vec::new();
        let outcome = decode_partition_batches(
            "walk-topic",
            0,
            buf.freeze(),
            100,
            vec![crate::protocol::AbortedTransaction {
                producer_id: 9,
                first_offset: 100,
            }],
            None,
            RecordBatch::MAX_DECOMPRESSED_SIZE,
            &mut records,
        );

        let offsets: Vec<Offset> = records.iter().map(|r| r.offset).collect();
        assert_eq!(
            offsets,
            vec![104, 105],
            "only the committed transaction is delivered"
        );
        assert_eq!(outcome.last_offset, Some(105));
    }

    #[test]
    fn advance_through_batch_never_moves_backwards() {
        let mut last_offset = Some(500);
        let mut last_epoch = 3;

        // A batch end behind what this response already advanced through
        // leaves the bookkeeping untouched — epoch included.
        advance_through_batch(&mut last_offset, &mut last_epoch, 100, 400, 9);
        assert_eq!(last_offset, Some(500));
        assert_eq!(last_epoch, 3);

        // A batch end below the fetch position is ignored outright.
        advance_through_batch(&mut last_offset, &mut last_epoch, 700, 600, 9);
        assert_eq!(last_offset, Some(500));

        // Forward movement takes the new end and its epoch.
        advance_through_batch(&mut last_offset, &mut last_epoch, 100, 600, 9);
        assert_eq!(last_offset, Some(600));
        assert_eq!(last_epoch, 9);
    }
    /// An aborted transaction's data batch is dropped from its header: its
    /// records are never decompressed. Under `read_uncommitted` (no aborted
    /// list) the same batch is decompressed and delivered.
    ///
    /// Reverted-line control: without the header-first check, the batch is
    /// decompressed before the aborted-producer test and the first
    /// `DECOMPRESSIONS` assertion fails.
    #[test]
    fn aborted_compressed_batch_is_skipped_without_decompressing() {
        let mut aborted = walk_batch(100, 2, &[0, 1, 2]);
        aborted.attributes.is_transactional = true;
        aborted.attributes.compression = crate::protocol::Compression::Gzip;
        aborted.producer_id = 9;
        let wire = aborted.encode().expect("encode");
        let decompressions = || crate::protocol::DECOMPRESSIONS.with(std::cell::Cell::get);

        let before = decompressions();
        let mut records = Vec::new();
        let outcome = decode_partition_batches(
            "walk-topic",
            0,
            wire.clone(),
            100,
            vec![crate::protocol::AbortedTransaction {
                producer_id: 9,
                first_offset: 100,
            }],
            None,
            RecordBatch::MAX_DECOMPRESSED_SIZE,
            &mut records,
        );
        assert!(records.is_empty());
        assert_eq!(outcome.last_offset, Some(102), "the span is still skipped");
        assert_eq!(decompressions() - before, 0, "never decompressed");

        let before = decompressions();
        let mut records = Vec::new();
        decode_partition_batches(
            "walk-topic",
            0,
            wire,
            100,
            Vec::new(),
            None,
            RecordBatch::MAX_DECOMPRESSED_SIZE,
            &mut records,
        );
        assert_eq!(records.len(), 3, "read_uncommitted delivers the records");
        assert_eq!(decompressions() - before, 1);
    }
}
