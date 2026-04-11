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
        self.closing.load(Ordering::SeqCst)
    }

    /// Register a new operation unless shutdown has already started.
    pub(crate) fn start(self: &Arc<Self>, owner: &str) -> Result<InFlightOpGuard> {
        if self.is_closing() {
            return Err(KrafkaError::invalid_state(format!("{owner} is closed")));
        }

        self.started.fetch_add(1, Ordering::SeqCst);

        if self.is_closing() {
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
        self.started.load(Ordering::SeqCst)
    }

    /// Begin shutdown and capture the final target count.
    pub(crate) fn begin_close(&self) -> Option<u64> {
        if self.closing.swap(true, Ordering::SeqCst) {
            return None;
        }
        Some(self.snapshot())
    }

    pub(crate) async fn wait_for(&self, target: u64) {
        loop {
            if self.completed.load(Ordering::SeqCst) >= target {
                return;
            }

            let notified = self.notify.notified();
            if self.completed.load(Ordering::SeqCst) >= target {
                return;
            }
            notified.await;
        }
    }

    fn complete_one(&self) {
        self.completed.fetch_add(1, Ordering::SeqCst);
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
            .field("closing", &self.is_closing())
            .field("started", &self.started.load(Ordering::SeqCst))
            .field("completed", &self.completed.load(Ordering::SeqCst))
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
}
