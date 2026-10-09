//! Producer identity, batch stamps, and what a produce outcome means for them.
//!
//! An idempotent or transactional producer writes every batch under a
//! `(producer id, epoch, base sequence)` stamp. The rules this module encodes:
//!
//! - A batch is stamped once, when it is first drained, and keeps that stamp
//!   across every retry ([`BatchIdentity`]).
//! - A stamped sequence range is never handed out again. A terminal failure of
//!   a stamped batch moves the whole producer to the next epoch instead
//!   (KIP-360); every partition then restarts at sequence 0.
//! - [`classify`] decides, from one partition's produce outcome, whether the
//!   batch is acknowledged, retried, re-stamped, split or failed, and whether
//!   the producer must bump its epoch.

use crate::PartitionId;
use crate::error::ErrorCode;

/// The producer's current `(producer id, epoch)`.
///
/// `generation` counts identity changes; a partition whose stored generation
/// differs restarts at sequence 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Identity {
    pub(crate) producer_id: i64,
    pub(crate) epoch: i16,
    pub(crate) generation: u64,
}

impl Identity {
    /// The identity after a local epoch bump, under `generation`, or `None`
    /// when the epoch is exhausted and a new producer id is needed.
    pub(crate) fn bumped(self, generation: u64) -> Option<Self> {
        (self.epoch < i16::MAX - 1).then(|| Self {
            producer_id: self.producer_id,
            epoch: self.epoch + 1,
            generation,
        })
    }
}

/// The stamp a batch carries on the wire. Never changed once assigned.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BatchIdentity {
    pub(crate) producer_id: i64,
    pub(crate) epoch: i16,
    pub(crate) base_sequence: i32,
    pub(crate) generation: u64,
}

/// One partition's sequence position under one identity generation.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct PartitionSeq {
    generation: u64,
    next: i32,
}

impl PartitionSeq {
    /// Stamp a batch of `count` records under `identity`, advancing past it.
    ///
    /// A partition last used under another generation starts at 0. Sequences
    /// wrap at `i32::MAX` as Kafka's do.
    pub(crate) fn stamp(&mut self, identity: Identity, count: i32) -> BatchIdentity {
        if self.generation != identity.generation {
            self.generation = identity.generation;
            self.next = 0;
        }
        let base_sequence = self.next;
        self.next = next_sequence(base_sequence, count);
        BatchIdentity {
            producer_id: identity.producer_id,
            epoch: identity.epoch,
            base_sequence,
            generation: identity.generation,
        }
    }
}

/// The sequence after a batch of `count` records starting at `base`.
#[inline]
fn next_sequence(base: i32, count: i32) -> i32 {
    // Kafka wraps sequences to 0 after i32::MAX.
    let next = i64::from(base) + i64::from(count);
    (next % (i64::from(i32::MAX) + 1)) as i32
}

/// How the producer writes, for [`classify`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Mode {
    /// No producer id: `idempotent(false)`.
    Plain,
    /// Idempotent, no transactional id.
    Idempotent,
    /// Transactional; `tv2` once KIP-890 was negotiated.
    Transactional { tv2: bool },
}

impl Mode {
    #[inline]
    pub(crate) fn has_identity(self) -> bool {
        !matches!(self, Self::Plain)
    }
}

/// One partition's answer in a produce response.
#[derive(Debug, Clone, Copy)]
pub(crate) struct PartitionAnswer {
    pub(crate) code: ErrorCode,
    pub(crate) log_start_offset: i64,
}

/// What [`classify`] decided for a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Action {
    /// Written. `duplicate` when the broker answered
    /// `DUPLICATE_SEQUENCE_NUMBER` without an offset.
    Ack { duplicate: bool },
    /// Retry with the same stamp after a backoff.
    Retry,
    /// Bump the epoch and send the batch again under a new stamp. Only for
    /// a rejection that proves the batch was not appended.
    Restamp,
    /// Too large: split it in two and send the halves as new batches.
    Split { bump: bool },
    /// Fail the batch's records with `failure`.
    Fail { failure: Failure, bump: bool },
    /// The producer cannot continue; fail this batch and every later send.
    Fatal,
}

/// Which error a failed batch reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Failure {
    /// The broker's code, as a `Broker` error.
    Broker,
    /// `OutOfOrderSequence`: the broker lost an earlier batch.
    OutOfOrder,
    /// The transaction must be aborted.
    Abortable,
}

/// What a terminal batch failure costs the producer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Context {
    pub(crate) mode: Mode,
    pub(crate) record_count: usize,
    /// Highest offset acknowledged on this partition by this producer.
    pub(crate) last_acked_offset: Option<i64>,
    /// Whether `INVALID_PRODUCER_EPOCH` already caused one re-stamp.
    pub(crate) epoch_retry_used: bool,
}

/// Decide what a partition's produce answer means for its batch.
///
/// Transport failures (timeout, disconnect, an undecodable response) are not
/// answers; the caller retries them with the same stamp until the deadline.
pub(crate) fn classify(ctx: Context, answer: PartitionAnswer) -> Action {
    use ErrorCode as E;

    let identity = ctx.mode.has_identity();
    let transactional = matches!(ctx.mode, Mode::Transactional { .. });
    let tv2 = matches!(ctx.mode, Mode::Transactional { tv2: true });
    // A failed stamped batch spends its range: the idempotent producer bumps
    // at once, the transactional one when the transaction is aborted.
    let bump = matches!(ctx.mode, Mode::Idempotent);
    let fail = |failure| Action::Fail {
        failure: if transactional {
            Failure::Abortable
        } else {
            failure
        },
        bump,
    };

    match answer.code {
        E::None => Action::Ack { duplicate: false },
        E::DuplicateSequenceNumber if identity => Action::Ack { duplicate: true },
        E::MessageTooLarge | E::RecordListTooLarge if ctx.record_count > 1 => {
            if transactional {
                fail(Failure::Broker)
            } else {
                Action::Split { bump }
            }
        }
        E::OutOfOrderSequenceNumber if identity => fail(Failure::OutOfOrder),
        E::UnknownProducerId if identity => {
            let retention_removed_it = ctx
                .last_acked_offset
                .is_none_or(|acked| answer.log_start_offset > acked);
            if transactional {
                fail(Failure::Abortable)
            } else if retention_removed_it {
                Action::Restamp
            } else {
                fail(Failure::OutOfOrder)
            }
        }
        E::InvalidProducerEpoch if identity => match ctx.mode {
            Mode::Transactional { tv2: false } => Action::Fatal,
            Mode::Transactional { tv2: true } => fail(Failure::Abortable),
            _ if ctx.epoch_retry_used => Action::Fatal,
            _ => Action::Restamp,
        },
        E::InvalidProducerIdMapping if transactional && !tv2 => fail(Failure::Abortable),
        E::ProducerFenced
        | E::TransactionalIdAuthorizationFailed
        | E::ClusterAuthorizationFailed
        | E::InvalidProducerIdMapping
        | E::UnsupportedVersion
            if identity =>
        {
            Action::Fatal
        }
        E::TransactionAbortable if transactional => fail(Failure::Abortable),
        code if code.is_retriable() => Action::Retry,
        _ => fail(Failure::Broker),
    }
}

/// Whether `code` proves the broker did not append the batch.
///
/// A written batch answered by anything else — a timeout, a lost connection,
/// `NOT_ENOUGH_REPLICAS_AFTER_APPEND`, `REQUEST_TIMED_OUT` — may be in the
/// log, so its failure reports `possibly_written: true`.
pub(crate) fn is_definitive_non_append(code: ErrorCode) -> bool {
    use ErrorCode as E;
    matches!(
        code,
        E::MessageTooLarge
            | E::RecordListTooLarge
            | E::InvalidRecord
            | E::CorruptMessage
            | E::NotLeaderForPartition
            | E::UnknownTopicOrPartition
            | E::TopicAuthorizationFailed
            | E::InvalidRequiredAcks
            | E::UnsupportedCompressionType
            | E::InvalidTimestamp
    )
}

/// Topic-partition key used by the engine's per-partition state.
pub(crate) type PartitionKey = (std::sync::Arc<str>, PartitionId);

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use ErrorCode as E;

    fn ctx(mode: Mode) -> Context {
        Context {
            mode,
            record_count: 3,
            last_acked_offset: Some(10),
            epoch_retry_used: false,
        }
    }

    fn answer(code: ErrorCode) -> PartitionAnswer {
        PartitionAnswer {
            code,
            log_start_offset: 0,
        }
    }

    const IDEMPOTENT: Mode = Mode::Idempotent;
    const TV1: Mode = Mode::Transactional { tv2: false };
    const TV2: Mode = Mode::Transactional { tv2: true };

    #[test]
    fn success_and_duplicate_acknowledge() {
        for mode in [Mode::Plain, IDEMPOTENT, TV1, TV2] {
            assert_eq!(
                classify(ctx(mode), answer(E::None)),
                Action::Ack { duplicate: false }
            );
        }
        assert_eq!(
            classify(ctx(IDEMPOTENT), answer(E::DuplicateSequenceNumber)),
            Action::Ack { duplicate: true }
        );
    }

    #[test]
    fn retriable_codes_retry_with_the_same_stamp() {
        for code in [
            E::NotLeaderForPartition,
            E::NotEnoughReplicas,
            E::NotEnoughReplicasAfterAppend,
            E::RequestTimedOut,
            E::KafkaStorageException,
        ] {
            for mode in [Mode::Plain, IDEMPOTENT, TV1, TV2] {
                assert_eq!(classify(ctx(mode), answer(code)), Action::Retry, "{code:?}");
            }
        }
    }

    #[test]
    fn too_large_splits_or_aborts() {
        assert_eq!(
            classify(ctx(IDEMPOTENT), answer(E::MessageTooLarge)),
            Action::Split { bump: true }
        );
        assert_eq!(
            classify(ctx(Mode::Plain), answer(E::RecordListTooLarge)),
            Action::Split { bump: false }
        );
        assert!(matches!(
            classify(ctx(TV1), answer(E::MessageTooLarge)),
            Action::Fail {
                failure: Failure::Abortable,
                ..
            }
        ));
        let single = Context {
            record_count: 1,
            ..ctx(IDEMPOTENT)
        };
        assert_eq!(
            classify(single, answer(E::MessageTooLarge)),
            Action::Fail {
                failure: Failure::Broker,
                bump: true
            }
        );
    }

    #[test]
    fn a_non_retriable_record_error_fails_and_spends_the_range() {
        for code in [
            E::InvalidRecord,
            E::TopicAuthorizationFailed,
            E::InvalidTimestamp,
        ] {
            assert_eq!(
                classify(ctx(IDEMPOTENT), answer(code)),
                Action::Fail {
                    failure: Failure::Broker,
                    bump: true
                },
                "{code:?}"
            );
            assert_eq!(
                classify(ctx(Mode::Plain), answer(code)),
                Action::Fail {
                    failure: Failure::Broker,
                    bump: false
                }
            );
            assert_eq!(
                classify(ctx(TV2), answer(code)),
                Action::Fail {
                    failure: Failure::Abortable,
                    bump: false
                }
            );
        }
    }

    /// OOSN never resends the identical sequence: it fails the head batch
    /// non-fatally and bumps.
    #[test]
    fn out_of_order_fails_the_head_batch_and_bumps() {
        assert_eq!(
            classify(ctx(IDEMPOTENT), answer(E::OutOfOrderSequenceNumber)),
            Action::Fail {
                failure: Failure::OutOfOrder,
                bump: true
            }
        );
        assert!(matches!(
            classify(ctx(TV1), answer(E::OutOfOrderSequenceNumber)),
            Action::Fail {
                failure: Failure::Abortable,
                ..
            }
        ));
    }

    #[test]
    fn unknown_producer_id_is_benign_only_past_the_last_acknowledged_offset() {
        let benign = PartitionAnswer {
            code: E::UnknownProducerId,
            log_start_offset: 11,
        };
        assert_eq!(classify(ctx(IDEMPOTENT), benign), Action::Restamp);
        let lost = PartitionAnswer {
            code: E::UnknownProducerId,
            log_start_offset: 10,
        };
        assert_eq!(
            classify(ctx(IDEMPOTENT), lost),
            Action::Fail {
                failure: Failure::OutOfOrder,
                bump: true
            }
        );
        assert!(matches!(
            classify(ctx(TV1), benign),
            Action::Fail {
                failure: Failure::Abortable,
                ..
            }
        ));
    }

    #[test]
    fn invalid_epoch_restamps_once_then_is_fatal() {
        assert_eq!(
            classify(ctx(IDEMPOTENT), answer(E::InvalidProducerEpoch)),
            Action::Restamp
        );
        let used = Context {
            epoch_retry_used: true,
            ..ctx(IDEMPOTENT)
        };
        assert_eq!(
            classify(used, answer(E::InvalidProducerEpoch)),
            Action::Fatal
        );
        assert_eq!(
            classify(ctx(TV1), answer(E::InvalidProducerEpoch)),
            Action::Fatal
        );
        assert!(matches!(
            classify(ctx(TV2), answer(E::InvalidProducerEpoch)),
            Action::Fail {
                failure: Failure::Abortable,
                ..
            }
        ));
    }

    #[test]
    fn fencing_and_authorization_are_fatal() {
        for code in [
            E::ProducerFenced,
            E::TransactionalIdAuthorizationFailed,
            E::ClusterAuthorizationFailed,
            E::UnsupportedVersion,
        ] {
            for mode in [IDEMPOTENT, TV1, TV2] {
                assert_eq!(classify(ctx(mode), answer(code)), Action::Fatal, "{code:?}");
            }
        }
        assert_eq!(
            classify(ctx(TV2), answer(E::InvalidProducerIdMapping)),
            Action::Fatal
        );
        assert!(matches!(
            classify(ctx(TV1), answer(E::InvalidProducerIdMapping)),
            Action::Fail {
                failure: Failure::Abortable,
                ..
            }
        ));
    }

    #[test]
    fn transaction_abortable_is_abortable() {
        assert!(matches!(
            classify(ctx(TV2), answer(E::TransactionAbortable)),
            Action::Fail {
                failure: Failure::Abortable,
                ..
            }
        ));
    }

    #[test]
    fn the_allow_list_is_exactly_the_definitive_non_appends() {
        for code in [
            E::NotEnoughReplicasAfterAppend,
            E::RequestTimedOut,
            E::NotEnoughReplicas,
        ] {
            assert!(
                !is_definitive_non_append(code),
                "{code:?} may have been written"
            );
        }
        for code in [
            E::InvalidRecord,
            E::NotLeaderForPartition,
            E::MessageTooLarge,
        ] {
            assert!(is_definitive_non_append(code), "{code:?}");
        }
    }

    #[test]
    fn a_partition_restarts_at_zero_under_a_new_generation() {
        let first = Identity {
            producer_id: 7,
            epoch: 0,
            generation: 1,
        };
        let mut seq = PartitionSeq::default();
        assert_eq!(seq.stamp(first, 3).base_sequence, 0);
        assert_eq!(seq.stamp(first, 2).base_sequence, 3);
        let bumped = first.bumped(2).unwrap();
        let stamp = seq.stamp(bumped, 1);
        assert_eq!((stamp.epoch, stamp.base_sequence), (1, 0));
    }

    #[test]
    fn the_epoch_is_exhausted_one_below_the_maximum() {
        let last = Identity {
            producer_id: 1,
            epoch: i16::MAX - 1,
            generation: 0,
        };
        assert!(last.bumped(1).is_none());
    }

    #[test]
    fn sequences_wrap_after_i32_max() {
        assert_eq!(next_sequence(i32::MAX, 1), 0);
        assert_eq!(next_sequence(i32::MAX - 1, 3), 1);
    }
}
