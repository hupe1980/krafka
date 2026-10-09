//! Unit tests of the share consumer's pure parts.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use bytes::{Bytes, BytesMut};

use super::acks::{self, AckBook, AckFailure, AckRange, AckType};
use super::commit::{self, AcknowledgementCommit, CommitWaiter};
use super::completed_fetch;
use super::*;
use crate::consumer::TopicPartition;
use crate::error::ErrorCode;
use crate::protocol::{Record, RecordBatch, ShareAcquiredRecords};

fn tp(partition: PartitionId) -> TopicPartition {
    TopicPartition::new("t", partition)
}

fn acquired(first: i64, last: i64, delivery_count: i16) -> ShareAcquiredRecords {
    ShareAcquiredRecords {
        first_offset: first,
        last_offset: last,
        delivery_count,
    }
}

/// A record batch at `base` holding `count` records.
fn batch(base: i64, count: i32, control: bool) -> Bytes {
    let mut batch = RecordBatch::new();
    batch.base_offset = base;
    batch.attributes.is_control_batch = control;
    batch.last_offset_delta = count - 1;
    for i in 0..count {
        let mut record = Record::new(None, Some(Bytes::from(vec![i as u8])));
        record.offset_delta = i;
        batch.add_record(record);
    }
    batch.encode().unwrap()
}

fn concat(parts: &[Bytes]) -> Bytes {
    let mut out = BytesMut::new();
    for part in parts {
        out.extend_from_slice(part);
    }
    out.freeze()
}

fn offsets(processed: &completed_fetch::Processed) -> Vec<i64> {
    processed.fetch.records.iter().map(|r| r.offset).collect()
}

fn process(records: &Bytes, ranges: &[ShareAcquiredRecords]) -> completed_fetch::Processed {
    completed_fetch::process(
        "t",
        [1; 16],
        0,
        7,
        Some(records),
        ranges,
        RecordBatch::MAX_DECOMPRESSED_SIZE,
    )
}

// ── Completed fetch ─────────────────────────────────────────────────────────

#[test]
fn only_acquired_offsets_of_a_whole_batch_are_delivered() {
    // The broker returns the whole batch 0..=9 but acquired only 5..=9.
    let processed = process(&batch(0, 10, false), &[acquired(5, 9, 2)]);
    assert_eq!(offsets(&processed), vec![5, 6, 7, 8, 9]);
    assert!(
        processed
            .fetch
            .records
            .iter()
            .all(|r| r.delivery_count == Some(2))
    );
    assert!(
        processed.acks.is_empty(),
        "every acquired offset carried a record"
    );
    assert_eq!(processed.fetch.node, 7);
}

#[test]
fn a_control_batch_is_skipped_and_gap_acknowledged() {
    let records = concat(&[batch(0, 1, false), batch(1, 1, true)]);
    let processed = process(&records, &[acquired(0, 1, 1)]);
    assert_eq!(offsets(&processed), vec![0]);
    assert_eq!(
        processed.acks,
        vec![AckRange {
            first: 1,
            last: 1,
            kind: AckType::Gap
        }]
    );
}

#[test]
fn acquired_offsets_without_records_are_gap_acknowledged() {
    // Acquired 10..=19, records only for 10..=14 (compacted away).
    let processed = process(&batch(10, 5, false), &[acquired(10, 19, 1)]);
    assert_eq!(offsets(&processed), vec![10, 11, 12, 13, 14]);
    assert_eq!(
        processed.acks,
        vec![AckRange {
            first: 15,
            last: 19,
            kind: AckType::Gap
        }]
    );
}

#[test]
fn offsets_from_an_undecodable_batch_on_are_released() {
    let mut bad = BytesMut::from(batch(3, 3, false).as_ref());
    let last = bad.len() - 1;
    bad[last] ^= 0xff; // breaks the CRC
    let records = concat(&[batch(0, 3, false), bad.freeze()]);
    let processed = process(&records, &[acquired(0, 5, 1)]);
    assert_eq!(offsets(&processed), vec![0, 1, 2]);
    assert_eq!(
        processed.acks,
        vec![AckRange {
            first: 3,
            last: 5,
            kind: AckType::Release
        }]
    );
}

#[test]
fn a_huge_acquired_range_costs_one_gap_range() {
    let processed = process(&batch(0, 2, false), &[acquired(0, i64::MAX - 1, 1)]);
    assert_eq!(offsets(&processed), vec![0, 1]);
    assert_eq!(processed.acks.len(), 1);
    assert_eq!(processed.acks[0].first, 2);
    assert_eq!(processed.acks[0].kind, AckType::Gap);
}

#[test]
fn inverted_ranges_acquire_nothing() {
    let processed = process(&batch(0, 3, false), &[acquired(2, 1, 1)]);
    assert!(processed.fetch.records.is_empty());
    assert!(processed.acks.is_empty());
}

#[test]
fn unsorted_ranges_are_walked_in_offset_order() {
    let processed = process(&batch(0, 6, false), &[acquired(4, 5, 3), acquired(0, 1, 1)]);
    assert_eq!(offsets(&processed), vec![0, 1, 4, 5]);
    let counts: Vec<_> = processed
        .fetch
        .records
        .iter()
        .map(|r| r.delivery_count)
        .collect();
    assert_eq!(counts, vec![Some(1), Some(1), Some(3), Some(3)]);
}

// ── Ack book ────────────────────────────────────────────────────────────────

#[test]
fn wire_batches_are_sorted_and_merged() {
    let ranges = [
        AckRange::one(3, AckType::Accept),
        AckRange::one(1, AckType::Accept),
        AckRange::one(2, AckType::Accept),
        AckRange::one(4, AckType::Release),
    ];
    let batches = acks::to_batches(&ranges);
    let wire: Vec<(i64, i64, Vec<i8>)> = batches
        .iter()
        .map(|b| (b.first_offset, b.last_offset, b.acknowledge_types.clone()))
        .collect();
    assert_eq!(wire, vec![(1, 3, vec![1]), (4, 4, vec![2])]);
}

#[test]
fn a_later_acknowledgement_replaces_a_pending_renew() {
    let mut book = AckBook::default();
    book.add(1, tp(0), [1; 16], AckRange::one(5, AckType::Renew));
    book.add(1, tp(0), [1; 16], AckRange::one(5, AckType::Accept));
    let taken = book.take(1, None);
    assert_eq!(taken.len(), 1);
    assert_eq!(taken[0].2, vec![AckRange::one(5, AckType::Accept)]);
}

#[test]
fn acknowledgements_stay_with_the_acquiring_node() {
    let mut book = AckBook::default();
    book.add(1, tp(0), [1; 16], AckRange::one(1, AckType::Accept));
    book.add(2, tp(0), [1; 16], AckRange::one(2, AckType::Accept));
    let taken = book.take(2, None);
    assert_eq!(taken.len(), 1);
    assert_eq!(taken[0].2, vec![AckRange::one(2, AckType::Accept)]);
    assert!(book.has_pending(1));
    assert!(!book.has_pending(2));
}

#[test]
fn errors_are_classified() {
    let broker = |code| KrafkaError::broker(code, "x");
    assert_eq!(
        acks::classify(&KrafkaError::timeout("t")),
        AckFailure::Retry
    );
    for code in [
        ErrorCode::InvalidShareSessionEpoch,
        ErrorCode::ShareSessionNotFound,
        ErrorCode::ShareSessionLimitReached,
        ErrorCode::RequestTimedOut,
    ] {
        assert_eq!(acks::classify(&broker(code)), AckFailure::Retry, "{code:?}");
    }
    for code in [
        ErrorCode::NotLeaderForPartition,
        ErrorCode::FencedLeaderEpoch,
        ErrorCode::UnknownTopicOrPartition,
    ] {
        assert_eq!(
            acks::classify(&broker(code)),
            AckFailure::NotLeader,
            "{code:?}"
        );
    }
    for code in [ErrorCode::InvalidRecordState, ErrorCode::InvalidRequest] {
        assert_eq!(
            acks::classify(&broker(code)),
            AckFailure::Permanent,
            "{code:?}"
        );
    }
}

#[test]
fn a_retried_failure_goes_back_in_front_and_a_permanent_one_resolves() {
    let now = tokio::time::Instant::now();
    let mut book = AckBook::default();
    book.add(1, tp(0), [1; 16], AckRange::one(1, AckType::Accept));
    let _ = book.take(1, None);
    book.add(1, tp(0), [1; 16], AckRange::one(2, AckType::Accept));

    let retried = book.settle(
        1,
        &tp(0),
        Err(KrafkaError::timeout("t")),
        Some(now - Duration::from_secs(30)),
    );
    assert!(retried.is_none(), "a session-class failure is retried");
    let taken = book.take(1, None);
    assert_eq!(
        taken[0].2,
        vec![AckRange {
            first: 1,
            last: 2,
            kind: AckType::Accept
        }]
    );

    let resolved = book
        .settle(
            1,
            &tp(0),
            Err(KrafkaError::broker(ErrorCode::InvalidRecordState, "x")),
            Some(now - Duration::from_secs(30)),
        )
        .expect("a permanent failure resolves");
    assert!(resolved.result.is_err());
    assert!(!book.has_pending(1));
    assert!(book.nodes().is_empty(), "the book is empty");
}

#[test]
fn a_retry_past_its_cutoff_resolves_as_a_timeout() {
    let mut book = AckBook::default();
    book.add(1, tp(0), [1; 16], AckRange::one(1, AckType::Accept));
    let _ = book.take(1, None);
    let resolved = book
        .settle(
            1,
            &tp(0),
            Err(KrafkaError::timeout("t")),
            Some(tokio::time::Instant::now() + Duration::from_secs(1)),
        )
        .expect("past the cutoff it resolves");
    assert!(matches!(resolved.result, Err(KrafkaError::Timeout { .. })));
}

// ── Commit results and callback ─────────────────────────────────────────────

#[tokio::test]
async fn a_commit_waiter_collects_one_result_per_partition() {
    let mut book = AckBook::default();
    book.add(1, tp(0), [1; 16], AckRange::one(1, AckType::Accept));
    book.add(1, tp(1), [1; 16], AckRange::one(1, AckType::Accept));
    let waiter = CommitWaiter::new();
    waiter.expect(book.attach(&waiter));
    let _ = book.take(1, None);
    let _ = book.settle(1, &tp(0), Ok(()), None);
    let _ = book.settle(
        1,
        &tp(1),
        Err(KrafkaError::broker(ErrorCode::InvalidRecordState, "x")),
        None,
    );
    assert!(waiter.wait(tokio::time::Instant::now()).await);
    let results = waiter.finish(&[]);
    assert_eq!(results.len(), 2);
    assert!(results[&tp(0)].is_ok());
    assert!(matches!(
        results[&tp(1)],
        Err(KrafkaError::Broker {
            code: ErrorCode::InvalidRecordState,
            ..
        })
    ));
}

#[tokio::test]
async fn a_commit_with_nothing_attached_is_done_and_empty() {
    let waiter = CommitWaiter::new();
    assert!(waiter.wait(tokio::time::Instant::now()).await);
    assert!(waiter.finish(&[]).is_empty());
}

#[test]
fn a_panicking_callback_is_contained() {
    let calls = Arc::new(AtomicUsize::new(0));
    let seen = Arc::clone(&calls);
    let callback: AcknowledgementCommitCallback = Arc::new(move |_c: &AcknowledgementCommit| {
        seen.fetch_add(1, Ordering::SeqCst);
        panic!("application bug");
    });
    let mut book = AckBook::default();
    book.add(1, tp(0), [1; 16], AckRange::one(1, AckType::Accept));
    book.add(1, tp(1), [1; 16], AckRange::one(1, AckType::Accept));
    let _ = book.take(1, None);
    let resolved: Vec<_> = [tp(0), tp(1)]
        .iter()
        .filter_map(|p| book.settle(1, p, Ok(()), None))
        .collect();
    commit::report(Some(&callback), &resolved);
    assert_eq!(calls.load(Ordering::SeqCst), 2, "every outcome is reported");
}

#[test]
fn acknowledged_offsets_are_reported_as_ranges() {
    let mut book = AckBook::default();
    for offset in [1, 2, 3, 7] {
        book.add(1, tp(0), [1; 16], AckRange::one(offset, AckType::Accept));
    }
    let _ = book.take(1, None);
    let resolved = book.settle(1, &tp(0), Ok(()), None).unwrap();
    let commit = AcknowledgementCommit::from_resolved(&resolved);
    assert_eq!(commit.offsets, vec![1..=3, 7..=7]);
    assert!(commit.result.is_ok());
}

// ── Configuration ───────────────────────────────────────────────────────────

#[test]
fn defaults() {
    let config = super::config::ShareConsumerConfig::default();
    assert_eq!(config.acknowledgement_mode, AcknowledgementMode::Implicit);
    assert_eq!(config.acquire_mode, AcquireMode::BatchOptimized);
    assert_eq!(config.max_poll_records, 500);
    assert_eq!(AcquireMode::BatchOptimized.to_i8(), 0);
    assert_eq!(AcquireMode::RecordLimit.to_i8(), 1);
    assert_eq!(CloseOptions::default().timeout, None);
    assert_eq!(
        CloseOptions::new().timeout(Duration::from_secs(1)).timeout,
        Some(Duration::from_secs(1))
    );
}

#[tokio::test]
async fn the_builder_validates() {
    let kafka = crate::Kafka::detached();
    let missing_group = kafka.share_consumer("").build().await;
    assert!(matches!(missing_group, Err(KrafkaError::Config { .. })));

    let zero_poll = kafka.share_consumer("g").max_poll_records(0).build().await;
    assert!(matches!(zero_poll, Err(KrafkaError::Config { .. })));
}
