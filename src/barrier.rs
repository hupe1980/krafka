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

        // Why SeqCst and not AcqRel:
        //
        // This is the store-buffering (SB) litmus test.  Thread A writes
        // `started` then reads `closing`; thread B (begin_close) writes
        // `closing` then reads `started`.  Under AcqRel both reads may
        // return the pre-write values (each thread's store is only
        // visible when the *other* thread performs an acquire load of
        // the *same* variable).  Only SeqCst establishes a total order
        // that guarantees at least one thread sees the other's write.
        //
        // This cannot be safely weakened to AcqRel without adding a
        // separate fence or restructuring the algorithm.

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

        // SeqCst pairs with `start()` — see the SB litmus-test comment
        // there.  Cannot be weakened without breaking the invariant that
        // at least one side observes the other's write.
        Some(self.started.load(Ordering::SeqCst))
    }

    pub(crate) async fn wait_for(&self, target: u64) {
        loop {
            if self.completed.load(Ordering::Acquire) >= target {
                return;
            }

            let notified = self.notify.notified();
            if self.completed.load(Ordering::Acquire) >= target {
                return;
            }
            notified.await;
        }
    }

    fn complete_one(&self) {
        self.completed.fetch_add(1, Ordering::Release);
        // `notify_waiters` (broadcast) is intentional: concurrent `flush()`
        // and `close_inner()` can wait on different targets simultaneously,
        // so `notify_one()` could leave the other waiter stuck.
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
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    // Both tests below exist because `cargo mutants` showed the suite could not
    // tell the real implementations from stubs: `is_closing` could return a
    // constant `true` and the `Debug` impl could render nothing, and every
    // assertion still held.

    /// `is_closing` must track the flag, not answer a constant.
    ///
    /// It gates `start()`, so a stub that always answered `true` would refuse
    /// every operation on a healthy barrier — and a stub answering `false`
    /// would admit work into a closing one, which is the case the barrier
    /// exists to prevent.
    #[tokio::test]
    async fn is_closing_tracks_the_flag() {
        let barrier = Arc::new(InFlightBarrier::new());
        assert!(!barrier.is_closing(), "a fresh barrier is open");

        let guard = barrier.start("producer").unwrap();
        assert!(
            !barrier.is_closing(),
            "an in-flight operation does not close it"
        );

        barrier.begin_close();
        assert!(barrier.is_closing(), "begin_close must be observable");
        drop(guard);
        assert!(barrier.is_closing(), "completing work does not reopen it");
    }

    /// The `Debug` impl must render the counters it claims to.
    ///
    /// This is the type three shutdown paths block on (the transactional
    /// producer's commit and abort, and the share consumer's acknowledgement
    /// flush). When one of them appears to hang, this output is the first thing
    /// anyone reads — a `Debug` that silently rendered nothing would hide
    /// exactly the state needed to tell "waiting for real work" from "leaked a
    /// guard".
    #[tokio::test]
    async fn debug_renders_the_counters() {
        let barrier = Arc::new(InFlightBarrier::new());
        let guard = barrier.start("producer").unwrap();

        let rendered = format!("{barrier:?}");
        assert!(rendered.contains("InFlightBarrier"), "got: {rendered}");
        assert!(rendered.contains("closing: false"), "got: {rendered}");
        assert!(rendered.contains("started: 1"), "got: {rendered}");
        assert!(rendered.contains("completed: 0"), "got: {rendered}");

        drop(guard);
        let rendered = format!("{barrier:?}");
        assert!(
            rendered.contains("completed: 1"),
            "the counters must move, not just be present: {rendered}"
        );
    }

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

    /// Concurrent `flush()` + `close()` can wait on distinct targets simultaneously.
    ///
    /// `flush()` captures `snapshot()` (current `started` count) as its target.
    /// `close()` captures `begin_close()` as its target (same count or higher).
    /// After all in-flight ops complete, `notify_waiters()` is broadcast and
    /// both waiters must wake, not just one.
    #[tokio::test]
    async fn test_concurrent_flush_and_close_both_wake() {
        let barrier = Arc::new(InFlightBarrier::new());

        // Start two in-flight ops.
        let op1 = barrier.start("producer").unwrap();
        let op2 = barrier.start("producer").unwrap();

        // `flush()` snapshot — targets the current started count (2).
        let flush_target = barrier.snapshot();

        // `close()` — also targets the current started count.
        let close_target = barrier.begin_close().unwrap();

        // Both targets should be the same (2) since nothing extra was started.
        assert_eq!(flush_target, close_target);

        let b_flush = Arc::clone(&barrier);
        let b_close = Arc::clone(&barrier);

        // Spawn both waiters concurrently.
        let flush_handle = tokio::spawn(async move { b_flush.wait_for(flush_target).await });
        let close_handle = tokio::spawn(async move { b_close.wait_for(close_target).await });

        // Neither should finish yet.
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        // Complete the first op — still below target.
        drop(op1);
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;

        // Complete the second op — both waiters should now wake.
        drop(op2);

        let timeout = std::time::Duration::from_secs(1);
        tokio::time::timeout(timeout, flush_handle)
            .await
            .expect("flush waiter should complete")
            .expect("flush task should not panic");
        tokio::time::timeout(timeout, close_handle)
            .await
            .expect("close waiter should complete")
            .expect("close task should not panic");
    }

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
