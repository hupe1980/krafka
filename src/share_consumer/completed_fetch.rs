//! Turning one partition of a `ShareFetch` response into deliverable records.
//!
//! A broker returns whole record batches, but only the offsets inside the
//! response's acquired ranges belong to this member. Records outside them may
//! already be accepted, or held by another member; delivering them would
//! process them twice, and acknowledging them fails the partition's whole
//! acknowledgement request with `INVALID_RECORD_STATE`.

use std::collections::VecDeque;

use bytes::Bytes;
use tracing::debug;

use super::acks::{AckRange, AckType};
use crate::consumer::ConsumerRecord;
use crate::protocol::{RecordBatch, ShareAcquiredRecords};
use crate::{BrokerId, Offset, PartitionId};

/// The deliverable records of one partition response, in offset order.
#[derive(Debug)]
pub(crate) struct CompletedFetch {
    pub topic: String,
    pub topic_id: [u8; 16],
    pub partition: PartitionId,
    /// The node that acquired the records.
    pub node: BrokerId,
    pub records: VecDeque<ConsumerRecord>,
}

/// What processing a partition response produced.
#[derive(Debug)]
pub(crate) struct Processed {
    pub fetch: CompletedFetch,
    /// Acknowledgements the client owes for acquired offsets it will not
    /// deliver: GAP for control records and missing offsets, RELEASE for
    /// offsets in batches that could not be decoded.
    pub acks: Vec<AckRange>,
}

/// Process one partition response.
///
/// Walks the decoded records against the acquired ranges, sorted by offset.
/// A record is delivered only when its offset is acquired, with that range's
/// delivery count. Control batches are skipped. Acquired offsets that carry
/// no delivered record are acknowledged as GAP, except those at or after a
/// batch that failed to decode, which are released for redelivery (the
/// broker archives a record once its delivery count reaches
/// `group.share.delivery.count.limit`).
///
/// Nothing is allocated per acquired offset: a malformed range such as
/// `0..=i64::MAX` costs one GAP range, which the broker rejects.
#[allow(clippy::too_many_arguments)]
pub(crate) fn process(
    topic: &str,
    topic_id: [u8; 16],
    partition: PartitionId,
    node: BrokerId,
    records: Option<&Bytes>,
    acquired: &[ShareAcquiredRecords],
    max_decompressed_size: usize,
) -> Processed {
    let mut ranges: Vec<&ShareAcquiredRecords> = acquired
        .iter()
        .filter(|r| r.last_offset >= r.first_offset)
        .collect();
    ranges.sort_unstable_by_key(|r| r.first_offset);

    let mut delivered: VecDeque<ConsumerRecord> = VecDeque::new();
    // One allocation per partition response; every record shares it.
    let topic_name: std::sync::Arc<str> = std::sync::Arc::from(topic);
    // Offsets that carry a record, delivered or skipped as control, sorted.
    let mut covered: Vec<(Offset, Offset, AckType)> = Vec::new();
    let mut release_from: Option<Offset> = None;
    // Records arrive in offset order, so the range lookup only moves forward.
    let mut next_range = 0;

    if let Some(raw) = records {
        let mut cursor = raw.clone();
        while !cursor.is_empty() {
            let base = (cursor.len() >= 8).then(|| {
                let mut prefix = [0u8; 8];
                prefix.copy_from_slice(&cursor[..8]);
                i64::from_be_bytes(prefix)
            });
            let batch = match RecordBatch::decode_with_limit(&mut cursor, max_decompressed_size) {
                Ok(batch) => batch,
                Err(error) => {
                    debug!("undecodable record batch in {topic}-{partition}: {error}");
                    release_from = Some(base.unwrap_or(Offset::MIN));
                    break;
                }
            };
            if batch.attributes.is_control_batch {
                let last = batch
                    .base_offset
                    .saturating_add(i64::from(batch.last_offset_delta));
                covered.push((batch.base_offset, last, AckType::Gap));
                continue;
            }
            for record in batch.records {
                let offset = batch
                    .base_offset
                    .saturating_add(i64::from(record.offset_delta));
                if covered.last().is_some_and(|&(_, last, _)| offset <= last) {
                    continue;
                }
                while ranges
                    .get(next_range)
                    .is_some_and(|r| r.last_offset < offset)
                {
                    next_range += 1;
                }
                let Some(range) = ranges.get(next_range).filter(|r| r.first_offset <= offset)
                else {
                    continue;
                };
                covered.push((offset, offset, AckType::Accept));
                delivered.push_back(ConsumerRecord {
                    topic: std::sync::Arc::clone(&topic_name),
                    partition,
                    offset,
                    timestamp: batch.base_timestamp.saturating_add(record.timestamp_delta),
                    timestamp_type: batch.attributes.timestamp_type,
                    key: record.key,
                    value: record.value,
                    headers: crate::consumer::headers_from_wire(record.headers),
                    leader_epoch: Some(batch.partition_leader_epoch),
                    delivery_count: Some(range.delivery_count),
                });
            }
        }
    }

    // Every acquired offset not delivered is acknowledged here.
    covered.sort_unstable_by_key(|c| c.0);
    let mut acks: Vec<AckRange> = Vec::new();
    let mut push = |first: Offset, last: Offset, kind: AckType| {
        if first > last {
            return;
        }
        match acks.last_mut() {
            Some(prev) if prev.kind == kind && prev.last.checked_add(1) == Some(first) => {
                prev.last = last;
            }
            _ => acks.push(AckRange { first, last, kind }),
        }
    };
    // Both lists are sorted, so one cursor walks them together.
    let mut start = 0;
    for range in ranges {
        while start < covered.len() && covered[start].1 < range.first_offset {
            start += 1;
        }
        let mut next = range.first_offset;
        for &(first, last, kind) in covered[start..]
            .iter()
            .take_while(|c| c.0 <= range.last_offset)
        {
            if last < next {
                continue;
            }
            if first > next {
                uncovered(next, first - 1, release_from, &mut push);
            }
            if kind == AckType::Gap {
                push(first.max(next), last.min(range.last_offset), AckType::Gap);
            }
            next = last.saturating_add(1);
        }
        if next <= range.last_offset {
            uncovered(next, range.last_offset, release_from, &mut push);
        }
    }

    Processed {
        fetch: CompletedFetch {
            topic: topic.to_string(),
            topic_id,
            partition,
            node,
            records: delivered,
        },
        acks,
    }
}

/// Acknowledge acquired offsets `first..=last` that carry no record: GAP
/// before the first undecodable batch, RELEASE from it on.
fn uncovered(
    first: Offset,
    last: Offset,
    release_from: Option<Offset>,
    push: &mut impl FnMut(Offset, Offset, AckType),
) {
    match release_from {
        Some(from) if from <= first => push(first, last, AckType::Release),
        Some(from) if from <= last => {
            push(first, from - 1, AckType::Gap);
            push(from, last, AckType::Release);
        }
        _ => push(first, last, AckType::Gap),
    }
}
