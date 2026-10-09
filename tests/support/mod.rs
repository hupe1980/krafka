//! Shared helpers for the fake-broker producer tests.
//!
//! The centre piece is [`History`], the send-history checker: it records what
//! the client reported for every send and compares it with the partition logs
//! the fake broker holds.

#![allow(dead_code, clippy::expect_used, clippy::panic)]

use std::collections::HashMap;

use krafka::error::{KrafkaError, Result};
use krafka::producer::RecordMetadata;
use krafka::testing::FakeBroker;

/// What the client reported for one send.
#[derive(Debug)]
pub struct Sent {
    /// The record's value; every value in a history is unique.
    pub value: String,
    /// The client's answer.
    pub result: Result<RecordMetadata>,
}

/// The client-side record of every send of a test, checked against the log.
#[derive(Debug, Default)]
pub struct History {
    sends: Vec<Sent>,
}

impl History {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record the answer the client gave for the send of `value`.
    pub fn record(&mut self, value: impl Into<String>, result: Result<RecordMetadata>) {
        self.sends.push(Sent {
            value: value.into(),
            result,
        });
    }

    pub fn sends(&self) -> &[Sent] {
        &self.sends
    }

    /// Check the history against every record of `topic` in the broker's log.
    ///
    /// - every acknowledged record is in the log exactly once;
    /// - a failed record is in the log only when its error said it may have
    ///   been written.
    ///
    /// Returns the violations, one line each; empty means the history holds.
    pub fn violations(&self, broker: &FakeBroker, topic: &str) -> Vec<String> {
        let log = broker.all_records(topic).expect("fake broker log decodes");
        let mut counts: HashMap<String, usize> = HashMap::new();
        for record in &log {
            let value = record
                .value
                .as_deref()
                .map(|v| String::from_utf8_lossy(v).into_owned())
                .unwrap_or_default();
            *counts.entry(value).or_default() += 1;
        }

        let mut violations = Vec::new();
        for sent in &self.sends {
            let in_log = counts.get(&sent.value).copied().unwrap_or(0);
            match &sent.result {
                Ok(_) if in_log == 1 => {}
                Ok(metadata) => violations.push(format!(
                    "{} acknowledged (offset {}) but in the log {in_log} times",
                    sent.value, metadata.offset
                )),
                Err(error) if in_log == 0 => {
                    let _ = error;
                }
                Err(error) if possibly_written(error) && in_log == 1 => {}
                Err(error) => violations.push(format!(
                    "{} failed with `{error}` but is in the log {in_log} times",
                    sent.value
                )),
            }
        }
        violations
    }

    /// Panic with every violation, if there is one.
    pub fn assert_holds(&self, broker: &FakeBroker, topic: &str) {
        let violations = self.violations(broker, topic);
        assert!(
            violations.is_empty(),
            "send history does not match the log:\n{}",
            violations.join("\n")
        );
    }
}

/// Whether the error says the broker may have appended the record.
pub fn possibly_written(error: &KrafkaError) -> bool {
    matches!(
        error,
        KrafkaError::DeliveryTimeout {
            possibly_written: true,
            ..
        }
    )
}

/// Every value `read_committed` sees on `topic`, in log order.
pub fn committed_values(broker: &FakeBroker, topic: &str) -> Vec<String> {
    broker
        .committed_records(topic)
        .expect("fake broker log decodes")
        .into_iter()
        .map(|r| String::from_utf8_lossy(r.value.as_deref().unwrap_or_default()).into_owned())
        .collect()
}

/// Every value on `topic`, aborted and open transactions included.
pub fn all_values(broker: &FakeBroker, topic: &str) -> Vec<String> {
    broker
        .all_records(topic)
        .expect("fake broker log decodes")
        .into_iter()
        .map(|r| String::from_utf8_lossy(r.value.as_deref().unwrap_or_default()).into_owned())
        .collect()
}

/// `(producer_id, epoch, base_sequence, record_count)` of every batch the
/// partition's log holds, read from the v2 batch header.
pub fn batch_identities(
    broker: &FakeBroker,
    topic: &str,
    partition: i32,
) -> Vec<(i64, i16, i32, i32)> {
    broker.with_state(|state| {
        let Some(p) = state.partition_mut(topic, partition) else {
            return Vec::new();
        };
        p.log
            .iter()
            .filter(|b| b.len() >= 61)
            .map(|b| {
                let i64_at =
                    |o: usize| i64::from_be_bytes(b[o..o + 8].try_into().expect("8 bytes"));
                let i32_at =
                    |o: usize| i32::from_be_bytes(b[o..o + 4].try_into().expect("4 bytes"));
                let i16_at =
                    |o: usize| i16::from_be_bytes(b[o..o + 2].try_into().expect("2 bytes"));
                (i64_at(43), i16_at(51), i32_at(53), i32_at(57))
            })
            .collect()
    })
}
