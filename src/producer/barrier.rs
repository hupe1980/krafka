use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use tokio::sync::Notify;

use crate::error::{KrafkaError, Result};

/// Shared barrier for producer operations that must complete before shutdown.
pub(crate) struct InFlightBarrier {
    closing: AtomicBool,
    started: AtomicU64,
    completed: AtomicU64,
    notify: Notify,
}

impl InFlightBarrier {
    pub(crate) fn new() -> Self {
        Self {
            closing: AtomicBool::new(false),
            started: AtomicU64::new(0),
            completed: AtomicU64::new(0),
            notify: Notify::new(),
        }
    }

    #[inline]
    pub(crate) fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    /// Register a new operation unless shutdown has already started.
    pub(crate) fn start(self: &Arc<Self>, owner: &str) -> Result<InFlightOpGuard> {
        if self.closing.load(Ordering::Acquire) {
            return Err(KrafkaError::invalid_state(format!("{owner} is closed")));
        }

        self.started.fetch_add(1, Ordering::SeqCst);

        // SeqCst on both `started` and `closing` prevents the store-buffering
        // hazard: either `begin_close` observes our incremented `started`,
        // or we observe `closing = true` below — at least one must hold.

        if self.closing.load(Ordering::SeqCst) {
            self.complete_one();
            return Err(KrafkaError::invalid_state(format!("{owner} is closed")));
        }

        Ok(InFlightOpGuard {
            barrier: Some(self.clone()),
        })
    }

    /// Capture a flush snapshot without blocking new operations.
    #[inline]
    pub(crate) fn snapshot(&self) -> u64 {
        self.started.load(Ordering::Relaxed)
    }

    /// Begin shutdown and capture the final target count.
    pub(crate) fn begin_close(&self) -> Option<u64> {
        if self.closing.swap(true, Ordering::SeqCst) {
            return None;
        }

        // SeqCst on `closing` and `started` pairs with `start()` to prevent
        // store-buffering; see comment there.
        Some(self.started.load(Ordering::SeqCst))
    }

    pub(crate) async fn wait_for(&self, target: u64) {
        loop {
            // Relaxed is sufficient: `Notify` provides the synchronization
            // barrier between `complete_one` and this loop.
            if self.completed.load(Ordering::Relaxed) >= target {
                return;
            }

            let notified = self.notify.notified();
            if self.completed.load(Ordering::Relaxed) >= target {
                return;
            }
            notified.await;
        }
    }

    fn complete_one(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        self.notify.notify_waiters();
    }
}

impl Default for InFlightBarrier {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for InFlightBarrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InFlightBarrier")
            .field("closing", &self.closing.load(Ordering::Relaxed))
            .field("started", &self.started.load(Ordering::Relaxed))
            .field("completed", &self.completed.load(Ordering::Relaxed))
            .finish()
    }
}

#[derive(Debug)]
pub(crate) struct InFlightOpGuard {
    barrier: Option<Arc<InFlightBarrier>>,
}

impl Drop for InFlightOpGuard {
    fn drop(&mut self) {
        if let Some(barrier) = self.barrier.take() {
            barrier.complete_one();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_wait_for_snapshot_ignores_later_operations() {
        let barrier = Arc::new(InFlightBarrier::new());
        let first = barrier.start("producer").unwrap();
        let target = barrier.snapshot();
        let second = barrier.start("producer").unwrap();

        drop(first);
        tokio::time::timeout(std::time::Duration::from_secs(1), barrier.wait_for(target))
            .await
            .expect("snapshot wait should ignore later operations");

        drop(second);
    }

    #[tokio::test]
    async fn test_close_blocks_until_all_started_operations_finish() {
        let barrier = Arc::new(InFlightBarrier::new());
        let first = barrier.start("producer").unwrap();
        let second = barrier.start("producer").unwrap();
        let target = barrier.begin_close().unwrap();

        assert!(barrier.start("producer").is_err());

        drop(first);
        let wait_result = tokio::time::timeout(
            std::time::Duration::from_millis(25),
            barrier.wait_for(target),
        )
        .await;
        assert!(
            wait_result.is_err(),
            "shutdown should wait for remaining work"
        );

        drop(second);
        tokio::time::timeout(std::time::Duration::from_secs(1), barrier.wait_for(target))
            .await
            .expect("shutdown wait should complete once all work finishes");
    }

    /// Simulates `close_with_timeout` behavior: timeout elapses before
    /// in-flight work completes → returns timeout error, but cleanup
    /// (pool teardown) still runs unconditionally.
    #[tokio::test]
    async fn test_close_with_timeout_returns_timeout_on_incomplete_work() {
        let barrier = Arc::new(InFlightBarrier::new());
        let _in_flight = barrier.start("producer").unwrap();
        let target = barrier.begin_close().unwrap();

        // Mimic close_inner: wrap the graceful wait in a timeout.
        let close_result = tokio::time::timeout(
            std::time::Duration::from_millis(25),
            barrier.wait_for(target),
        )
        .await;

        // Timeout should fire because _in_flight is still held.
        assert!(close_result.is_err(), "should timeout with in-flight work");

        // Cleanup code (interceptor close, pool.close_all) runs unconditionally
        // after the timeout — verify that is_closing is true so new sends are
        // rejected even though the timeout fired.
        assert!(barrier.is_closing());
        assert!(barrier.start("producer").is_err());
    }

    /// After `begin_close` + timeout, dropping the in-flight guard still
    /// completes the barrier (no leaked state).
    #[tokio::test]
    async fn test_close_with_timeout_guard_drop_still_completes() {
        let barrier = Arc::new(InFlightBarrier::new());
        let in_flight = barrier.start("producer").unwrap();
        let target = barrier.begin_close().unwrap();

        // Timeout fires while work is in-flight.
        let _ = tokio::time::timeout(
            std::time::Duration::from_millis(10),
            barrier.wait_for(target),
        )
        .await;

        // Now drop the guard (simulating pool teardown killing the connection).
        drop(in_flight);

        // The barrier should be fully drained.
        tokio::time::timeout(
            std::time::Duration::from_millis(10),
            barrier.wait_for(target),
        )
        .await
        .expect("barrier should be drained after guard drop");
    }

    /// `begin_close` is idempotent — second call returns None.
    #[tokio::test]
    async fn test_begin_close_is_idempotent() {
        let barrier = Arc::new(InFlightBarrier::new());
        let _first = barrier.begin_close();
        assert!(_first.is_some());
        assert!(barrier.begin_close().is_none());
    }

    /// Multiple tasks racing to close — exactly one gets `Some(target)`,
    /// the rest get `None`.
    #[tokio::test]
    async fn test_concurrent_begin_close_exactly_one_wins() {
        let barrier = Arc::new(InFlightBarrier::new());
        let _guard = barrier.start("producer").unwrap();

        let mut handles = Vec::new();
        for _ in 0..10 {
            let b = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move { b.begin_close() }));
        }

        let mut winners = 0u32;
        for handle in handles {
            if handle.await.unwrap().is_some() {
                winners += 1;
            }
        }

        assert_eq!(winners, 1, "exactly one task should win begin_close");
        assert!(barrier.is_closing());
    }

    /// `start` after `begin_close` returns an error, even from another task.
    #[tokio::test]
    async fn test_start_after_close_from_another_task() {
        let barrier = Arc::new(InFlightBarrier::new());
        let b = Arc::clone(&barrier);
        tokio::spawn(async move {
            b.begin_close();
        })
        .await
        .unwrap();

        assert!(barrier.start("producer").is_err());
    }
}
