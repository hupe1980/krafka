use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::error::{KrafkaError, Result};

/// Counts operations by the generation they started in, so a flush or a close
/// waits for exactly the operations that started before it.
///
/// Each [`register`](Self::register) counts one operation in the current
/// generation; [`complete_many`](Self::complete_many) uncounts any number of
/// them, of any generations, under one lock. [`snapshot`](Self::snapshot) and
/// [`begin_close`](Self::begin_close) end the current generation and return
/// it; [`wait_for`](Self::wait_for) then waits until no operation of that
/// generation or an earlier one is still running. An operation registered
/// afterwards belongs to a later generation, so it can neither hold the wait
/// up nor, by completing, release it early.
pub(crate) struct InFlightBarrier {
    closing: AtomicBool,
    state: Mutex<Generations>,
    notify: Notify,
}

#[derive(Debug, Default)]
struct Generations {
    current: u64,
    /// Running operations per generation; a generation with none is absent.
    running: BTreeMap<u64, u64>,
}

impl InFlightBarrier {
    pub(crate) fn new() -> Self {
        Self {
            closing: AtomicBool::new(false),
            state: Mutex::new(Generations::default()),
            notify: Notify::new(),
        }
    }

    #[inline]
    pub(crate) fn is_closing(&self) -> bool {
        self.closing.load(Ordering::Acquire)
    }

    /// Count a new operation in the current generation, unless shutdown has
    /// already begun, and return that generation. The caller owes exactly one
    /// [`complete_many`](Self::complete_many) count for it.
    pub(crate) fn register(&self, owner: &str) -> Result<u64> {
        let mut state = self.state.lock();
        // Checked under the lock `begin_close` takes, so an operation is
        // either counted in a generation the close waits for or refused.
        if self.closing.load(Ordering::Acquire) {
            return Err(KrafkaError::closed(format!("{owner} is closed")));
        }
        let generation = state.current;
        *state.running.entry(generation).or_default() += 1;
        Ok(generation)
    }

    /// Uncount `count` operations of each `(generation, count)`.
    ///
    /// Waiters are woken only when a generation has no operation left: a
    /// [`wait_for`](Self::wait_for) can end at no other moment.
    pub(crate) fn complete_many(&self, generations: &[(u64, u32)]) {
        let drained = {
            let mut state = self.state.lock();
            let mut drained = false;
            for &(generation, count) in generations {
                if let Some(running) = state.running.get_mut(&generation) {
                    *running = running.saturating_sub(u64::from(count));
                    if *running == 0 {
                        state.running.remove(&generation);
                        drained = true;
                    }
                }
            }
            drained
        };
        if drained {
            // Broadcast: a flush and a close can wait on different generations.
            self.notify.notify_waiters();
        }
    }

    /// Whether no registered operation is still running.
    pub(crate) fn is_idle(&self) -> bool {
        self.state.lock().running.is_empty()
    }

    /// End the current generation and return it, for [`wait_for`](Self::wait_for).
    pub(crate) fn snapshot(&self) -> u64 {
        let mut state = self.state.lock();
        let generation = state.current;
        state.current += 1;
        generation
    }

    /// Begin shutdown and return the last generation to wait for, or `None`
    /// if shutdown had already begun.
    pub(crate) fn begin_close(&self) -> Option<u64> {
        let mut state = self.state.lock();
        if self.closing.swap(true, Ordering::AcqRel) {
            return None;
        }
        let generation = state.current;
        state.current += 1;
        Some(generation)
    }

    /// Wait until no operation of `generation` or an earlier one is running.
    pub(crate) async fn wait_for(&self, generation: u64) {
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_drained(generation) {
                return;
            }
            notified.await;
        }
    }

    fn is_drained(&self, generation: u64) -> bool {
        self.state
            .lock()
            .running
            .range(..=generation)
            .next()
            .is_none()
    }
}

impl Default for InFlightBarrier {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for InFlightBarrier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.state.lock();
        f.debug_struct("InFlightBarrier")
            .field("closing", &self.closing.load(Ordering::Relaxed))
            .field("generation", &state.current)
            .field("running", &state.running.values().sum::<u64>())
            .finish()
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use std::sync::Arc;
    use std::task::Poll;

    /// One registered operation, uncounted on drop.
    struct Op {
        barrier: Arc<InFlightBarrier>,
        generation: u64,
    }

    impl Drop for Op {
        fn drop(&mut self) {
            self.barrier.complete_many(&[(self.generation, 1)]);
        }
    }

    trait Start {
        fn start(&self, owner: &str) -> Result<Op>;
    }

    impl Start for Arc<InFlightBarrier> {
        fn start(&self, owner: &str) -> Result<Op> {
            let generation = self.register(owner)?;
            Ok(Op {
                barrier: Arc::clone(self),
                generation,
            })
        }
    }

    // The tests below tell the real implementations from stubs (a constant
    // `is_closing`, an empty `Debug`).

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
        assert!(rendered.contains("running: 1"), "got: {rendered}");

        drop(guard);
        let rendered = format!("{barrier:?}");
        assert!(
            rendered.contains("running: 0"),
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

    /// Simulates a `close_with` whose timeout elapses before in-flight work
    /// completes: a timeout error, while the cleanup still runs.
    #[tokio::test]
    async fn test_close_with_timeout_returns_timeout_on_incomplete_work() {
        let barrier = Arc::new(InFlightBarrier::new());
        let _in_flight = barrier.start("producer").unwrap();
        let target = barrier.begin_close().unwrap();

        // Mimic the close: wrap the graceful wait in a timeout.
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
    /// `flush()` waits on the generation `snapshot()` closed, `close()` on the
    /// one `begin_close()` closed. When the last operation completes both
    /// waiters must wake, not just one.
    #[tokio::test]
    async fn test_concurrent_flush_and_close_both_wake() {
        let barrier = Arc::new(InFlightBarrier::new());

        // Start two in-flight ops.
        let op1 = barrier.start("producer").unwrap();
        let op2 = barrier.start("producer").unwrap();

        let flush_target = barrier.snapshot();

        let close_target = barrier.begin_close().unwrap();

        // The close covers a later generation than the flush.
        assert!(close_target > flush_target);

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

    /// An operation that starts after a snapshot can neither hold up nor
    /// release the wait for that snapshot. With one shared completion counter
    /// a later, faster operation released `flush()` while an earlier one was
    /// still running.
    #[tokio::test]
    async fn a_later_completion_does_not_release_an_earlier_wait() {
        let barrier = Arc::new(InFlightBarrier::new());
        let slow = barrier.start("producer").unwrap();
        let target = barrier.snapshot();
        let fast = barrier.start("producer").unwrap();
        drop(fast);

        let early = tokio::time::timeout(
            std::time::Duration::from_millis(25),
            barrier.wait_for(target),
        )
        .await;
        assert!(early.is_err(), "the slow operation is still running");

        drop(slow);
        tokio::time::timeout(std::time::Duration::from_secs(1), barrier.wait_for(target))
            .await
            .expect("drained once the slow operation completes");
    }

    /// `complete_many` across two generations releases the older wait only
    /// when that generation's count reaches zero.
    #[tokio::test]
    async fn complete_many_releases_a_wait_only_when_its_generation_drains() {
        let barrier = Arc::new(InFlightBarrier::new());
        let g0a = barrier.register("producer").unwrap();
        let g0b = barrier.register("producer").unwrap();
        let target = barrier.snapshot();
        let g1 = barrier.register("producer").unwrap();
        assert_eq!((g0a, g0b, g1), (0, 0, 1));

        barrier.complete_many(&[(0, 1), (1, 1)]);
        let early = tokio::time::timeout(
            std::time::Duration::from_millis(25),
            barrier.wait_for(target),
        )
        .await;
        assert!(early.is_err(), "one operation of generation 0 still runs");

        barrier.complete_many(&[(0, 1)]);
        tokio::time::timeout(std::time::Duration::from_secs(1), barrier.wait_for(target))
            .await
            .expect("generation 0 drained");
        assert!(barrier.is_idle());
    }

    /// A completion that drains no generation wakes nobody.
    #[tokio::test]
    async fn a_completion_that_drains_nothing_wakes_nobody() {
        let barrier = Arc::new(InFlightBarrier::new());
        barrier.register("producer").unwrap();
        barrier.register("producer").unwrap();
        let notified = barrier.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        barrier.complete_many(&[(0, 1)]);
        assert_eq!(
            futures_poll(notified.as_mut()),
            Poll::Pending,
            "one operation still runs: no wake"
        );
        barrier.complete_many(&[(0, 1)]);
        assert_eq!(
            futures_poll(notified.as_mut()),
            Poll::Ready(()),
            "the generation drained: wake"
        );
    }

    fn futures_poll<F: std::future::Future>(fut: std::pin::Pin<&mut F>) -> Poll<F::Output> {
        let waker = std::task::Waker::noop();
        fut.poll(&mut std::task::Context::from_waker(waker))
    }

    #[test]
    fn is_idle_follows_registration() {
        let barrier = InFlightBarrier::new();
        assert!(barrier.is_idle(), "fresh");
        let generation = barrier.register("producer").unwrap();
        assert!(!barrier.is_idle(), "one registered");
        barrier.complete_many(&[(generation, 1)]);
        assert!(barrier.is_idle(), "completed");
    }

    #[test]
    fn register_after_begin_close_refuses() {
        let barrier = InFlightBarrier::new();
        barrier.begin_close();
        assert!(barrier.register("producer").is_err());
        assert!(barrier.is_idle());
    }
}
