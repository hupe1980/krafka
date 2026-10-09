//! Liveness bookkeeping shared by both group protocols: the poll tracker
//! behind `max_poll_interval`, and the flags the heartbeat tasks raise for the
//! poll path.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Tracks how long it has been since the application last called `poll()`.
///
/// # Why the consumer needs this
///
/// Heartbeats are sent by a background task, so they keep flowing whether or
/// not the application is making progress. An application that stops calling
/// `poll()` — deadlocked, stuck on a slow downstream call, or looping forever
/// on one record — therefore looks perfectly healthy to the coordinator. It
/// holds its partitions indefinitely while consuming nothing from them, and
/// because the group never rebalances, no other member can take over. Nothing
/// in the system reports an error; the partitions simply stop advancing.
///
/// `max.poll.interval.ms` is the bound that makes that failure visible. The
/// heartbeat task compares the elapsed time against it and, once exceeded,
/// stops heartbeating so the coordinator can reassign the partitions to a
/// member that is actually consuming.
#[derive(Debug)]
pub(crate) struct PollTracker {
    /// When `poll()` was last entered.
    last_poll: parking_lot::Mutex<tokio::time::Instant>,
    /// Maximum permitted gap between `poll()` calls.
    max_poll_interval: Duration,
    /// Set once the interval has been exceeded, so `poll()` can report it.
    exceeded: std::sync::atomic::AtomicBool,
}

impl PollTracker {
    /// Create a tracker armed from now.
    pub(crate) fn new(max_poll_interval: Duration) -> Self {
        Self {
            last_poll: parking_lot::Mutex::new(tokio::time::Instant::now()),
            max_poll_interval,
            exceeded: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// Record that the application has just called `poll()`.
    pub(crate) fn note_poll(&self) {
        *self.last_poll.lock() = tokio::time::Instant::now();
    }

    /// Time since the last `poll()`.
    pub(crate) fn elapsed(&self) -> Duration {
        self.last_poll.lock().elapsed()
    }

    /// The configured maximum poll interval.
    pub(crate) fn max_poll_interval(&self) -> Duration {
        self.max_poll_interval
    }

    /// Whether the application has exceeded the maximum poll interval.
    pub(crate) fn is_expired(&self) -> bool {
        self.elapsed() > self.max_poll_interval
    }

    /// Latch the expired state. Returns `true` the first time it is set, so
    /// the caller can act on the transition exactly once.
    pub(crate) fn mark_exceeded(&self) -> bool {
        !self
            .exceeded
            .swap(true, std::sync::atomic::Ordering::SeqCst)
    }

    /// Whether the tracker has been latched as exceeded.
    pub(crate) fn exceeded(&self) -> bool {
        self.exceeded.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Clear the latch and restart the timer, e.g. after rejoining the group.
    pub(crate) fn reset(&self) {
        self.exceeded
            .store(false, std::sync::atomic::Ordering::SeqCst);
        self.note_poll();
    }
}

/// Flags the heartbeat task raises for the poll path, and whether it runs.
#[derive(Debug, Default)]
pub(crate) struct HeartbeatController {
    /// Whether a heartbeat task is running.
    running: AtomicBool,
    /// The coordinator asked for a rebalance.
    rebalance_needed: AtomicBool,
    /// The coordinator no longer knows this member (unknown member, illegal
    /// generation, fenced epoch): its partitions are lost.
    member_invalidated: AtomicBool,
}

impl HeartbeatController {
    pub(crate) fn is_running(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    pub(crate) fn start(&self) {
        self.running.store(true, Ordering::Release);
    }

    pub(crate) fn stop(&self) {
        self.running.store(false, Ordering::Release);
    }

    pub(crate) fn signal_rebalance(&self) {
        self.rebalance_needed.store(true, Ordering::Release);
    }

    pub(crate) fn take_rebalance_needed(&self) -> bool {
        self.rebalance_needed.swap(false, Ordering::AcqRel)
    }

    /// Record that the member was invalidated; also requests a rebalance.
    pub(crate) fn signal_member_invalidated(&self) {
        self.member_invalidated.store(true, Ordering::Release);
        self.rebalance_needed.store(true, Ordering::Release);
    }

    pub(crate) fn take_member_invalidated(&self) -> bool {
        self.member_invalidated.swap(false, Ordering::AcqRel)
    }
}

/// Commands for the heartbeat background task.
#[derive(Debug)]
pub(crate) enum HeartbeatCommand {
    /// Stop the heartbeat task.
    Stop,
    /// Send a full heartbeat now, reporting the owned partitions the
    /// consumer just acknowledged (KIP-848).
    AcknowledgeRevocation,
}
