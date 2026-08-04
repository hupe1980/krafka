//! Type-labeled async read-write locks with compile-time acquisition ordering.
//!
//! # Lock Hierarchy
//!
//! The `Consumer` struct holds five async `RwLock`s, each wrapped in
//! `LeveledRwLock<L, T>` where `L` is the lock's 1-based position in the
//! acquisition order defined in `consumer/mod.rs`:
//!
//! | Level | Field            | Type                                          |
//! |------:|------------------|-----------------------------------------------|
//! | 1     | `subscriptions`  | `HashSet<String>`                             |
//! | 2     | `assignments`    | `HashMap<String, Vec<PartitionId>>`           |
//! | 3     | `offsets`        | `HashMap<(String, PartitionId), Offset>`      |
//! | 4     | `paused`         | `HashSet<(String, PartitionId)>`              |
//! | 5     | `partition_state`| `HashMap<(String, PartitionId), PartitionState>` |
//!
//! Always acquire locks in strictly increasing level order. The `L` constant
//! appears in compiler diagnostics and documentation, making order inversions
//! visible during code review.
//!
//! # What the background heartbeat task may touch
//!
//! Since the classic-protocol heartbeat task performs `JoinGroup`/`SyncGroup`
//! on the application's behalf, two tasks can be inside the consumer at once.
//! The hierarchy above is not what keeps them apart — the division of state is:
//!
//! - The five locks above are the consumer's **data plane** and are reached
//!   **only from the poll path**. The heartbeat task never acquires any of
//!   them. That is what lets rebalance callbacks, offset bookkeeping and the
//!   receive buffer stay free of concurrency: they still only ever run on the
//!   task the application drives.
//! - The heartbeat task confines itself to `GroupCoordinator` state — group
//!   identity, the coordinator connection, and the parked assignment. Those
//!   locks live inside `GroupCoordinator`, are always the outermost thing a
//!   task acquires, and are released before any consumer-level lock is taken,
//!   so they cannot participate in a cycle with levels 1–5.
//!
//! Three `GroupCoordinator` fields carry the hand-off between the two tasks
//! and are deliberately *not* levelled async locks, because each is read or
//! written in a single operation with no `.await` under it:
//!
//! - `pending_rebalance` (`parking_lot::Mutex`) — the assignment itself,
//!   moved across with one `take`/`replace`.
//! - `rejoin_in_flight` (`watch::Sender<bool>`) — whether a rebalance is
//!   running. A watch rather than a flag so `poll()` can wait on it instead of
//!   busy-returning empty results.
//! - `heartbeat_epoch` (`AtomicU64`) — which heartbeat task is current, so a
//!   task still shutting down does not reset state belonging to its successor.
//!
//! Keeping all three out of the hierarchy is what stops the hand-off from
//! needing a position in it at all.
//!
//! No lock at any level may be held across an `.await` that performs network
//! I/O. The join/sync path in particular clones what it needs out of
//! `GroupCoordinator` and drops the guard before sending a request.
//!
//! # Runtime Checks (debug builds)
//!
//! In `debug_assertions` builds, each [`LeveledRwLock`] checks the task-local
//! level tracker before acquiring. An out-of-order acquisition panics with a
//! clear message identifying the violation.

use std::cell::Cell;
use tokio::sync::{RwLockReadGuard, RwLockWriteGuard};

tokio::task_local! {
    /// Tracks the maximum lock level currently held in this async task.
    ///
    /// Initialized to `0` (no lock held) at the start of each tracked scope.
    /// Acquiring level `L` asserts `L > current`, then sets the tracker to `L`.
    /// Releasing restores the previous value via [`LevelGuard`].
    ///
    /// Only meaningful inside a `LOCK_LEVEL.scope(…)` block. Outside such a
    /// scope the task-local is not set; reads will panic, so the guard only
    /// performs checks when the scope is active.
    #[cfg(debug_assertions)]
    pub(crate) static LOCK_LEVEL: Cell<usize>;
}

/// RAII guard that restores the previous lock level when dropped.
#[cfg(debug_assertions)]
pub(crate) struct LevelGuard {
    prev: usize,
}

#[cfg(debug_assertions)]
impl LevelGuard {
    /// Record that level `l` is now held and return a guard that will
    /// restore the previous maximum on drop.
    ///
    /// Panics if `l` is not strictly greater than the currently recorded
    /// maximum (i.e., the acquisition order is violated).
    pub(crate) fn acquire(l: usize) -> Option<Self> {
        // Only check when inside a tracked scope.
        LOCK_LEVEL
            .try_with(|cell| {
                let prev = cell.get();
                assert!(
                    l > prev,
                    "Lock ordering violation: tried to acquire level-{l} lock \
                     while level-{prev} is already held. \
                     See the LOCK ORDER comment in consumer/mod.rs."
                );
                cell.set(l);
                LevelGuard { prev }
            })
            .ok()
    }
}

#[cfg(debug_assertions)]
impl Drop for LevelGuard {
    fn drop(&mut self) {
        // Only restore if we're still inside a tracked scope.
        let _ = LOCK_LEVEL.try_with(|cell| cell.set(self.prev));
    }
}

/// A `tokio::sync::RwLock` labeled with its position in the lock hierarchy.
///
/// `L` is the 1-based level from the lock ordering table above. All methods
/// delegate transparently to the inner [`tokio::sync::RwLock`] and return the
/// same guard types, so existing call sites require no modification.
///
/// In `debug_assertions` builds the level is checked against the task-local
/// tracker before each acquisition; an ordering violation panics immediately
/// rather than silently risking a deadlock.
pub(crate) struct LeveledRwLock<const L: usize, T>(tokio::sync::RwLock<T>);

impl<const L: usize, T> LeveledRwLock<L, T> {
    /// Wrap `val` in a new leveled lock.
    #[inline]
    pub(crate) fn new(val: T) -> Self {
        Self(tokio::sync::RwLock::new(val))
    }

    /// Acquire a shared read guard.
    ///
    /// In `debug_assertions` builds this asserts the level ordering before
    /// blocking on the inner lock.
    #[inline]
    pub(crate) async fn read(&self) -> LeveledReadGuard<'_, L, T> {
        // Level check must happen *before* the .await so it runs synchronously
        // on the calling task.
        #[cfg(debug_assertions)]
        let _level_guard = LevelGuard::acquire(L);

        let guard = self.0.read().await;

        LeveledReadGuard {
            guard,
            #[cfg(debug_assertions)]
            _level_guard,
        }
    }

    /// Acquire an exclusive write guard.
    ///
    /// In `debug_assertions` builds this asserts the level ordering before
    /// blocking on the inner lock.
    #[inline]
    pub(crate) async fn write(&self) -> LeveledWriteGuard<'_, L, T> {
        #[cfg(debug_assertions)]
        let _level_guard = LevelGuard::acquire(L);

        let guard = self.0.write().await;

        LeveledWriteGuard {
            guard,
            #[cfg(debug_assertions)]
            _level_guard,
        }
    }

    /// Non-blocking shared read attempt. Returns `Err` if the lock is
    /// currently held exclusively (matches `tokio::sync::RwLock::try_read`).
    ///
    /// In `debug_assertions` builds this also checks the lock-ordering
    /// invariant, ensuring the same guarantee as the async `read()` path.
    #[inline]
    pub(crate) fn try_read(&self) -> Result<LeveledReadGuard<'_, L, T>, tokio::sync::TryLockError> {
        #[cfg(debug_assertions)]
        let _level_guard = LevelGuard::acquire(L);

        let guard = self.0.try_read()?;
        Ok(LeveledReadGuard {
            guard,
            #[cfg(debug_assertions)]
            _level_guard,
        })
    }

    /// Non-blocking exclusive write attempt. Returns `Err` if any guard
    /// is currently held (matches `tokio::sync::RwLock::try_write`).
    ///
    /// In `debug_assertions` builds this also checks the lock-ordering
    /// invariant, ensuring the same guarantee as the async `write()` path.
    // Deliberately kept despite having no production caller today.
    //
    // The point of `LeveledRwLock` is that *every* acquisition passes through
    // the ordering check. A missing `try_write` would push the next caller who
    // needs one to reach for the inner `tokio::sync::RwLock` directly and
    // silently bypass the tracker — which is the failure this type exists to
    // prevent. `try_read`, its counterpart, does have callers.
    //
    // Covered by `test_try_write_returns_guard`, so it cannot rot unnoticed.
    #[allow(dead_code)]
    #[inline]
    pub(crate) fn try_write(
        &self,
    ) -> Result<LeveledWriteGuard<'_, L, T>, tokio::sync::TryLockError> {
        #[cfg(debug_assertions)]
        let _level_guard = LevelGuard::acquire(L);

        let guard = self.0.try_write()?;
        Ok(LeveledWriteGuard {
            guard,
            #[cfg(debug_assertions)]
            _level_guard,
        })
    }
}

// ── Guard wrappers ─────────────────────────────────────────────────────────

/// Read guard returned by [`LeveledRwLock::read`].
///
/// Derefs to `T` via the inner [`RwLockReadGuard`].
pub(crate) struct LeveledReadGuard<'a, const L: usize, T> {
    guard: RwLockReadGuard<'a, T>,
    #[cfg(debug_assertions)]
    _level_guard: Option<LevelGuard>,
}

impl<const L: usize, T> std::ops::Deref for LeveledReadGuard<'_, L, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.guard
    }
}

/// Write guard returned by [`LeveledRwLock::write`].
///
/// Derefs to `T` (mutably and immutably) via the inner [`RwLockWriteGuard`].
pub(crate) struct LeveledWriteGuard<'a, const L: usize, T> {
    guard: RwLockWriteGuard<'a, T>,
    #[cfg(debug_assertions)]
    _level_guard: Option<LevelGuard>,
}

impl<const L: usize, T> std::ops::Deref for LeveledWriteGuard<'_, L, T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T {
        &self.guard
    }
}

impl<const L: usize, T> std::ops::DerefMut for LeveledWriteGuard<'_, L, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut T {
        &mut self.guard
    }
}

/// Run `fut` inside a lock-order tracking scope.
///
/// The ordering assertion in [`LevelGuard::acquire`] only fires while the
/// task-local level tracker is set, which means it does nothing at all unless
/// some entry point establishes the scope. Wrap the consumer's top-level
/// operations — poll, commit, rebalance — so acquisitions inside them are
/// actually checked in debug builds.
///
/// In release builds this compiles away to just awaiting `fut`.
#[inline]
pub(crate) async fn with_lock_tracking<F: std::future::Future>(fut: F) -> F::Output {
    #[cfg(debug_assertions)]
    {
        LOCK_LEVEL.scope(Cell::new(0), fut).await
    }
    #[cfg(not(debug_assertions))]
    {
        fut.await
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// The ordering assertion is only live inside a tracking scope. This test
    /// pins that fact down: if `with_lock_tracking` ever stops establishing
    /// the scope, `acquire` silently returns `None` and every ordering check
    /// in the consumer becomes dead code without any test failing.
    #[tokio::test]
    async fn test_level_tracking_is_active_inside_scope() {
        let active = with_lock_tracking(async { LevelGuard::acquire(1).is_some() }).await;
        assert!(
            active,
            "lock-level tracking must be active inside with_lock_tracking; \
             otherwise the ordering assertions never run"
        );
    }

    #[test]
    fn test_level_tracking_is_inactive_outside_scope() {
        // Outside a scope the guard degrades to a no-op rather than panicking.
        assert!(LevelGuard::acquire(1).is_none());
    }

    #[tokio::test]
    async fn test_in_order_acquisition_is_accepted() {
        with_lock_tracking(async {
            let a = LeveledRwLock::<1, u32>::new(1);
            let b = LeveledRwLock::<3, u32>::new(3);
            let ga = a.read().await;
            let gb = b.read().await;
            assert_eq!((*ga, *gb), (1, 3));
        })
        .await;
    }

    #[tokio::test]
    async fn test_level_is_restored_after_guard_drops() {
        with_lock_tracking(async {
            let high = LeveledRwLock::<5, u32>::new(5);
            {
                let _g = high.read().await;
            }
            // After the level-5 guard drops, a level-2 acquisition is legal
            // again; it would panic if the tracker had not been restored.
            let low = LeveledRwLock::<2, u32>::new(2);
            let g = low.read().await;
            assert_eq!(*g, 2);
        })
        .await;
    }

    #[cfg(debug_assertions)]
    #[tokio::test]
    #[should_panic(expected = "Lock ordering violation")]
    async fn test_out_of_order_acquisition_panics() {
        with_lock_tracking(async {
            let high = LeveledRwLock::<5, u32>::new(5);
            let low = LeveledRwLock::<2, u32>::new(2);
            let _g_high = high.read().await;
            // Acquiring a lower level while a higher one is held is the
            // inversion that risks deadlock.
            let _g_low = low.read().await;
        })
        .await;
    }

    #[test]
    fn test_leveled_lock_new_and_basic_access() {
        // LeveledRwLock::new() compiles and holds the correct value.
        // (Async read/write tested separately with a tokio runtime.)
        let lock = LeveledRwLock::<3, u32>::new(42);
        // try_read works synchronously when no writer holds the lock.
        let guard = lock.try_read().expect("try_read should succeed");
        assert_eq!(*guard, 42);
    }

    #[test]
    fn test_try_write_returns_guard() {
        let lock = LeveledRwLock::<2, Vec<i32>>::new(vec![1, 2, 3]);
        let mut guard = lock.try_write().expect("try_write should succeed");
        guard.push(4);
        drop(guard);
        assert_eq!(*lock.try_read().unwrap(), vec![1, 2, 3, 4]);
    }

    #[tokio::test]
    async fn test_async_read_and_write() {
        let lock = LeveledRwLock::<1, String>::new("hello".to_string());
        {
            let r = lock.read().await;
            assert_eq!(*r, "hello");
        }
        {
            let mut w = lock.write().await;
            w.push_str(" world");
        }
        let r = lock.read().await;
        assert_eq!(*r, "hello world");
    }
}
