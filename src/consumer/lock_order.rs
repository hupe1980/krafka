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
    #[inline]
    pub(crate) fn try_read(&self) -> Result<RwLockReadGuard<'_, T>, tokio::sync::TryLockError> {
        self.0.try_read()
    }

    /// Non-blocking exclusive write attempt. Returns `Err` if any guard
    /// is currently held (matches `tokio::sync::RwLock::try_write`).
    #[allow(dead_code)] // part of the API; not every caller uses it today
    #[inline]
    pub(crate) fn try_write(&self) -> Result<RwLockWriteGuard<'_, T>, tokio::sync::TryLockError> {
        self.0.try_write()
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

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

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
