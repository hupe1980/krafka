//! The transaction gate: which operations the transaction state admits, how
//! many of its sends are unresolved, and the first one that failed.
//!
//! A send is admitted, and counted, under the same lock that `commit` and
//! `abort` take to leave [`TransactionState::Open`]. So a send is either
//! counted before the commit starts waiting — and the commit waits for it — or
//! refused. A refused send touches nothing.

use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;

use super::TransactionState;
use crate::error::{KrafkaError, Result};

#[derive(Debug)]
struct Inner {
    state: TransactionState,
    /// Admitted sends of the current transaction without an outcome yet.
    pending: u64,
    /// The first send of the current transaction that failed.
    failed: Option<KrafkaError>,
}

/// Per transactional producer: state, pending sends, first failure.
#[derive(Debug)]
pub(crate) struct TxnGate {
    inner: Mutex<Inner>,
    drained: Notify,
}

impl TxnGate {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                state: TransactionState::Uninitialized,
                pending: 0,
                failed: None,
            }),
            drained: Notify::new(),
        }
    }

    pub(crate) fn state(&self) -> TransactionState {
        self.inner.lock().state
    }

    /// Set the state unconditionally.
    pub(crate) fn set(&self, state: TransactionState) {
        self.inner.lock().state = state;
    }

    /// Move from any of `from` to `to`, returning the state left.
    pub(crate) fn transition(
        &self,
        from: &[TransactionState],
        to: TransactionState,
    ) -> std::result::Result<TransactionState, TransactionState> {
        let mut inner = self.inner.lock();
        if from.contains(&inner.state) {
            let left = inner.state;
            inner.state = to;
            Ok(left)
        } else {
            Err(inner.state)
        }
    }

    /// `Ready → Open`: a new transaction with no failure and nothing pending.
    pub(crate) fn begin(&self) -> std::result::Result<(), TransactionState> {
        let mut inner = self.inner.lock();
        if inner.state != TransactionState::Ready {
            return Err(inner.state);
        }
        inner.state = TransactionState::Open;
        inner.failed = None;
        Ok(())
    }

    /// Refuse an operation unless the transaction is open and nothing in it
    /// has failed. Counts nothing.
    pub(crate) fn check_open(&self, operation: &str) -> Result<()> {
        let inner = self.inner.lock();
        Self::open_or_refuse(&inner, operation)
    }

    fn open_or_refuse(inner: &Inner, operation: &str) -> Result<()> {
        if let Some(failed) = &inner.failed {
            return Err(abortable(operation, failed));
        }
        match inner.state {
            TransactionState::Open => Ok(()),
            TransactionState::Fatal => Err(KrafkaError::fenced(format!(
                "cannot {operation}: the transactional producer hit a fatal error"
            ))),
            state => Err(KrafkaError::illegal_state(format!(
                "cannot {operation} in transaction state {state}"
            ))),
        }
    }

    /// Admit one send into the open transaction and count it.
    pub(crate) fn admit(self: &Arc<Self>) -> Result<TxnTicket> {
        let mut inner = self.inner.lock();
        Self::open_or_refuse(&inner, "send")?;
        inner.pending += 1;
        Ok(TxnTicket {
            gate: Some(Arc::clone(self)),
        })
    }

    /// The first failure of the current transaction, as the error `commit`
    /// and later sends report.
    pub(crate) fn failure(&self, operation: &str) -> Option<KrafkaError> {
        self.inner
            .lock()
            .failed
            .as_ref()
            .map(|failed| abortable(operation, failed))
    }

    /// Whether a send of the current transaction failed.
    pub(crate) fn has_failed(&self) -> bool {
        self.inner.lock().failed.is_some()
    }

    /// Record `error` as the transaction's failure unless one is recorded.
    pub(crate) fn fail(&self, error: KrafkaError) {
        let mut inner = self.inner.lock();
        if inner.failed.is_none() {
            inner.failed = Some(error);
        }
    }

    /// Wait until every admitted send has an outcome.
    pub(crate) async fn drained(&self) {
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.inner.lock().pending == 0 {
                return;
            }
            notified.await;
        }
    }

    fn resolve(&self, failure: Option<KrafkaError>) {
        let mut inner = self.inner.lock();
        inner.pending = inner.pending.saturating_sub(1);
        if let Some(error) = failure
            && inner.failed.is_none()
        {
            inner.failed = Some(error);
        }
        if inner.pending == 0 {
            self.drained.notify_waiters();
        }
    }
}

fn abortable(operation: &str, failed: &KrafkaError) -> KrafkaError {
    KrafkaError::transaction_abortable(format!(
        "cannot {operation}: a send in this transaction failed ({failed}); abort the transaction"
    ))
}

/// One admitted send's place in the transaction's pending count.
///
/// Resolved exactly once: by [`complete`](Self::complete) with the send's
/// outcome, or on drop as a failure, so a lost record can never let a commit
/// through.
#[derive(Debug)]
pub(crate) struct TxnTicket {
    gate: Option<Arc<TxnGate>>,
}

impl TxnTicket {
    /// Report the send's outcome.
    pub(crate) fn complete(mut self, result: std::result::Result<(), &KrafkaError>) {
        if let Some(gate) = self.gate.take() {
            gate.resolve(result.err().cloned());
        }
    }
}

impl Drop for TxnTicket {
    fn drop(&mut self) {
        if let Some(gate) = self.gate.take() {
            gate.resolve(Some(KrafkaError::illegal_state(
                "a transactional send was dropped before its outcome was known",
            )));
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use std::time::Duration;

    fn open_gate() -> Arc<TxnGate> {
        let gate = Arc::new(TxnGate::new());
        gate.set(TransactionState::Ready);
        gate.begin().unwrap();
        gate
    }

    /// A send refused because the transaction is committing does not count:
    /// it cannot release the commit's wait for an earlier send.
    #[tokio::test]
    async fn a_refused_send_does_not_release_the_wait() {
        let gate = open_gate();
        let in_flight = gate.admit().unwrap();
        gate.transition(&[TransactionState::Open], TransactionState::Committing)
            .unwrap();
        assert!(gate.admit().is_err(), "committing refuses sends");
        let wait = tokio::time::timeout(Duration::from_millis(20), gate.drained()).await;
        assert!(wait.is_err(), "the admitted send is still pending");
        in_flight.complete(Ok(()));
        tokio::time::timeout(Duration::from_secs(1), gate.drained())
            .await
            .unwrap();
    }

    #[test]
    fn the_first_failure_refuses_later_sends_and_commit() {
        let gate = open_gate();
        let a = gate.admit().unwrap();
        let b = gate.admit().unwrap();
        a.complete(Err(&KrafkaError::config("first")));
        b.complete(Err(&KrafkaError::config("second")));
        let refused = gate.admit().unwrap_err();
        assert!(refused.requires_abort(), "{refused}");
        assert!(refused.to_string().contains("first"), "{refused}");
        assert!(
            gate.failure("commit")
                .unwrap()
                .to_string()
                .contains("first")
        );
    }

    #[test]
    fn a_dropped_ticket_counts_as_a_failure() {
        let gate = open_gate();
        drop(gate.admit().unwrap());
        assert!(gate.has_failed());
    }

    #[test]
    fn begin_clears_the_failure() {
        let gate = open_gate();
        gate.admit()
            .unwrap()
            .complete(Err(&KrafkaError::config("x")));
        gate.set(TransactionState::Ready);
        gate.begin().unwrap();
        assert!(!gate.has_failed());
        assert!(gate.admit().is_ok());
    }
}
