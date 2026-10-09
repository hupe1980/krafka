//! Share session state of one broker node (KIP-932).
//!
//! A share session is identified by `(group, member)` on one broker and
//! numbered by an epoch the client sends with every `ShareFetch` and
//! `ShareAcknowledge`:
//!
//! - `0` opens a session. Only a `ShareFetch` may open one, and it must not
//!   carry acknowledgements.
//! - `1..=i32::MAX` continue it; each answered request advances the epoch by
//!   one, wrapping from `i32::MAX` to `1`.
//! - `-1` closes it. The broker applies the request's acknowledgements and
//!   then releases every record the member still holds.
//!
//! The broker remembers which partitions the session fetches, so a request
//! after the first names only partitions to add (`Topics`) and to drop
//! (`ForgottenTopicsData`).

use ahash::AHashSet as HashSet;

use crate::PartitionId;

/// Epoch that opens a share session.
pub(crate) const INITIAL_EPOCH: i32 = 0;

/// Epoch that closes a share session.
pub(crate) const FINAL_EPOCH: i32 = -1;

/// A partition as the share APIs name it.
pub(crate) type SessionPartition = ([u8; 16], PartitionId);

/// The client's view of its share session on one node.
#[derive(Debug, Default)]
pub(crate) struct ShareSession {
    epoch: i32,
    /// Partitions the broker fetches for this session.
    partitions: HashSet<SessionPartition>,
}

/// What the next `ShareFetch` must say about the session's partitions.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct SessionDelta {
    /// Partitions to list in `Topics`.
    pub added: Vec<SessionPartition>,
    /// Partitions to list in `ForgottenTopicsData`.
    pub forgotten: Vec<SessionPartition>,
}

impl ShareSession {
    /// The epoch to send with the next request.
    pub(crate) fn epoch(&self) -> i32 {
        self.epoch
    }

    /// Whether the broker holds a session for this node.
    pub(crate) fn is_established(&self) -> bool {
        self.epoch != INITIAL_EPOCH
    }

    /// How a `ShareFetch` for `wanted` differs from the session. An opening
    /// request lists every wanted partition.
    pub(crate) fn delta(&self, wanted: &[SessionPartition]) -> SessionDelta {
        if !self.is_established() {
            return SessionDelta {
                added: wanted.to_vec(),
                forgotten: Vec::new(),
            };
        }
        let wanted_set: HashSet<SessionPartition> = wanted.iter().copied().collect();
        let mut forgotten: Vec<SessionPartition> = self
            .partitions
            .iter()
            .filter(|p| !wanted_set.contains(p))
            .copied()
            .collect();
        forgotten.sort_unstable();
        SessionDelta {
            added: wanted
                .iter()
                .filter(|p| !self.partitions.contains(p))
                .copied()
                .collect(),
            forgotten,
        }
    }

    /// Record an answered `ShareFetch` that applied `delta`.
    pub(crate) fn on_fetch(&mut self, delta: &SessionDelta) {
        if !self.is_established() {
            self.partitions.clear();
        }
        self.partitions.extend(delta.added.iter().copied());
        for partition in &delta.forgotten {
            self.partitions.remove(partition);
        }
        self.advance();
    }

    /// Record an answered `ShareAcknowledge`.
    pub(crate) fn on_acknowledge(&mut self) {
        self.advance();
    }

    /// Forget the session; the next request opens a new one.
    pub(crate) fn reset(&mut self) {
        self.epoch = INITIAL_EPOCH;
        self.partitions.clear();
    }

    fn advance(&mut self) {
        self.epoch = if self.epoch == i32::MAX {
            1
        } else {
            self.epoch + 1
        };
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    const A: SessionPartition = ([1; 16], 0);
    const B: SessionPartition = ([1; 16], 1);

    #[test]
    fn an_opening_fetch_lists_every_partition() {
        let session = ShareSession::default();
        assert_eq!(session.epoch(), INITIAL_EPOCH);
        let delta = session.delta(&[A, B]);
        assert_eq!(delta.added, vec![A, B]);
        assert!(delta.forgotten.is_empty());
    }

    #[test]
    fn a_later_fetch_names_only_the_difference() {
        let mut session = ShareSession::default();
        let delta = session.delta(&[A, B]);
        session.on_fetch(&delta);
        assert_eq!(session.epoch(), 1);

        let delta = session.delta(&[A]);
        assert!(delta.added.is_empty());
        assert_eq!(delta.forgotten, vec![B]);
        session.on_fetch(&delta);
        assert_eq!(session.epoch(), 2);
        assert_eq!(session.delta(&[A]), SessionDelta::default());
    }

    #[test]
    fn the_epoch_wraps_to_one() {
        let mut session = ShareSession {
            epoch: i32::MAX,
            partitions: HashSet::new(),
        };
        session.on_acknowledge();
        assert_eq!(session.epoch(), 1);
    }

    #[test]
    fn a_reset_reopens_with_every_partition() {
        let mut session = ShareSession::default();
        session.on_fetch(&session.delta(&[A]));
        session.reset();
        assert!(!session.is_established());
        assert_eq!(session.delta(&[A]).added, vec![A]);
    }
}
