//! Share session management (KIP-932).
//!
//! Share sessions are similar to fetch sessions (KIP-227) but operate on
//! share group partitions. Each broker maintains a share session per
//! (group_id, member_id) pair. The client tracks session epochs:
//!
//! - Epoch `0`: open a new session (full fetch)
//! - Epoch `1..=i32::MAX`: incremental fetches
//! - Epoch `-1`: close the session
//!
//! A share session also carries piggybacked acknowledgements: the client
//! reports accepted/released/rejected offsets alongside the next fetch
//! request, reducing round trips.

use std::collections::HashMap;

use crate::BrokerId;

/// Epoch value for opening a new share session.
pub const INITIAL_EPOCH: i32 = 0;

/// Epoch value for closing a share session.
///
/// Used when sending the final `ShareFetch` to a broker to tear down
/// the session (not yet wired into `close()`).
#[allow(dead_code)]
pub(crate) const FINAL_EPOCH: i32 = -1;

/// Per-broker share session state.
#[derive(Debug)]
pub struct ShareSessionState {
    /// Current epoch. Starts at 0 (open), incremented after each successful
    /// response. Set to -1 to close.
    epoch: i32,
    /// Whether a session is established (at least one successful exchange).
    established: bool,
}

impl ShareSessionState {
    /// Create a new share session for a broker. Starts with no session.
    fn new() -> Self {
        Self {
            epoch: INITIAL_EPOCH,
            established: false,
        }
    }

    /// Returns the current epoch to send in the next request.
    pub fn epoch(&self) -> i32 {
        self.epoch
    }

    /// Update after a successful share fetch response.
    /// Bumps the epoch for the next incremental fetch.
    pub fn on_success(&mut self) {
        self.established = true;
        // Wraps from MAX to 1 (0 is "open new session").
        self.epoch = if self.epoch == i32::MAX {
            1
        } else {
            self.epoch + 1
        };
    }

    /// Reset the session (e.g., on error or rebalance).
    /// The next fetch will open a new session (epoch 0).
    pub fn reset(&mut self) {
        self.epoch = INITIAL_EPOCH;
        self.established = false;
    }
}

/// Cache of share sessions, one per broker.
#[derive(Debug, Default)]
pub struct ShareSessionCache {
    sessions: HashMap<BrokerId, ShareSessionState>,
}

impl ShareSessionCache {
    /// Create a new empty cache.
    pub fn new() -> Self {
        Self::default()
    }

    /// Get or create the session state for a broker.
    pub fn get_or_create(&mut self, broker_id: BrokerId) -> &mut ShareSessionState {
        self.sessions
            .entry(broker_id)
            .or_insert_with(ShareSessionState::new)
    }

    /// Get the session state for a broker (read-only).
    pub fn get(&self, broker_id: BrokerId) -> Option<&ShareSessionState> {
        self.sessions.get(&broker_id)
    }

    /// Reset the session for a specific broker.
    pub fn reset_broker(&mut self, broker_id: BrokerId) {
        if let Some(session) = self.sessions.get_mut(&broker_id) {
            session.reset();
        }
    }

    /// Reset all sessions (e.g., on group leave).
    pub fn reset_all(&mut self) {
        for session in self.sessions.values_mut() {
            session.reset();
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn new_session_starts_at_epoch_zero() {
        let state = ShareSessionState::new();
        assert_eq!(state.epoch(), INITIAL_EPOCH);
        assert!(!state.established);
    }

    #[test]
    fn on_success_bumps_epoch() {
        let mut state = ShareSessionState::new();
        state.on_success();
        assert_eq!(state.epoch(), 1);
        assert!(state.established);

        state.on_success();
        assert_eq!(state.epoch(), 2);
    }

    #[test]
    fn epoch_wraps_at_max() {
        let mut state = ShareSessionState::new();
        state.epoch = i32::MAX;
        state.on_success();
        assert_eq!(state.epoch(), 1);
    }

    #[test]
    fn reset_returns_to_initial() {
        let mut state = ShareSessionState::new();
        state.on_success();
        state.on_success();
        assert_eq!(state.epoch(), 2);
        assert!(state.established);

        state.reset();
        assert_eq!(state.epoch(), INITIAL_EPOCH);
        assert!(!state.established);
    }

    #[test]
    fn final_epoch_is_minus_one() {
        assert_eq!(FINAL_EPOCH, -1);
    }

    #[test]
    fn cache_get_or_create_returns_same_session() {
        let mut cache = ShareSessionCache::new();
        cache.get_or_create(1).on_success();
        assert_eq!(cache.get_or_create(1).epoch(), 1);
    }

    #[test]
    fn cache_reset_broker() {
        let mut cache = ShareSessionCache::new();
        cache.get_or_create(1).on_success();
        cache.get_or_create(1).on_success();
        assert_eq!(cache.get(1).unwrap().epoch(), 2);

        cache.reset_broker(1);
        assert_eq!(cache.get(1).unwrap().epoch(), INITIAL_EPOCH);
    }

    #[test]
    fn cache_reset_all() {
        let mut cache = ShareSessionCache::new();
        cache.get_or_create(1).on_success();
        cache.get_or_create(2).on_success();
        cache.get_or_create(2).on_success();

        cache.reset_all();
        assert_eq!(cache.get(1).unwrap().epoch(), INITIAL_EPOCH);
        assert_eq!(cache.get(2).unwrap().epoch(), INITIAL_EPOCH);
    }
}
