//! Offset commits, serialised through one commit gate.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use ahash::AHashMap as HashMap;
use tracing::{Instrument, debug};

use crate::consumer::group::GroupCoordinator;
use crate::consumer::state::{PartitionKey, SubscriptionState};
use crate::consumer::{CommitPosition, Consumer};
use crate::error::Result;
use crate::interceptor::ConsumerInterceptor;
use crate::metrics::ConsumerRecorder;

/// Offsets to commit, per partition.
pub(in crate::consumer) type CommitRequestOffsets = HashMap<PartitionKey, CommitPosition>;

/// Everything a commit needs, cloneable onto a background task.
#[derive(Clone)]
pub(in crate::consumer) struct Committer {
    state: Arc<parking_lot::Mutex<SubscriptionState>>,
    /// Serialises OffsetCommit round trips, retries included, so commits leave
    /// in the order they were made and a retried older commit can never land
    /// after a newer one. It is the one lock in the consumer held across an
    /// `.await`, and it guards no state: positions are read from the state
    /// mutex inside it.
    gate: Arc<tokio::sync::Mutex<()>>,
    coordinator: Option<Arc<GroupCoordinator>>,
    interceptor: Arc<dyn ConsumerInterceptor>,
    metrics: Arc<ConsumerRecorder>,
    metrics_source: Arc<crate::metrics::MetricsSource>,
}

impl Committer {
    /// Commit every assigned partition's position.
    ///
    /// The positions are read after the gate is acquired, so a commit queued
    /// behind another sends what the application has received by the time it
    /// goes out, never an older value.
    pub(in crate::consumer) async fn commit_positions(&self) -> Result<()> {
        let _gate = self.gate.lock().await;
        let offsets: CommitRequestOffsets = self
            .state
            .lock()
            .committable()
            .into_iter()
            .map(|(key, offset, epoch)| {
                (
                    key,
                    CommitPosition {
                        offset,
                        leader_epoch: epoch.unwrap_or(-1),
                        metadata: None,
                    },
                )
            })
            .collect();
        if offsets.is_empty() {
            debug!("No positions to commit");
            return Ok(());
        }
        self.send(offsets).await
    }

    /// Commit `offsets` as given. Positions do not move.
    pub(in crate::consumer) async fn commit_offsets(
        &self,
        offsets: CommitRequestOffsets,
    ) -> Result<()> {
        if offsets.is_empty() {
            return Ok(());
        }
        let _gate = self.gate.lock().await;
        self.send(offsets).await
    }

    async fn send(&self, offsets: CommitRequestOffsets) -> Result<()> {
        self.metrics.commits.inc();
        let span = crate::tracing_ext::commit_span(
            self.coordinator.as_ref().map(|c| c.group_id()),
            self.metrics_source.client_id(),
        );
        if !span.is_disabled() {
            let mut topics: Vec<&str> = offsets.keys().map(|(topic, _)| topic.as_str()).collect();
            topics.sort_unstable();
            topics.dedup();
            crate::tracing_ext::record_destination(&span, "commit", topics);
        }
        let result = self
            .send_to_coordinator(offsets)
            .instrument(span.clone())
            .await;
        if let Err(error) = &result {
            crate::tracing_ext::record_error(&span, error);
        }
        result
    }

    async fn send_to_coordinator(&self, offsets: CommitRequestOffsets) -> Result<()> {
        let Some(coordinator) = self.coordinator.as_ref() else {
            // Without a group there is nowhere to store offsets.
            debug!("{} partition offsets committed locally only", offsets.len());
            return Ok(());
        };
        let committed: crate::interceptor::CommitOffsets = offsets
            .iter()
            .map(|(key, position)| (key.clone(), position.offset))
            .collect();
        let result = retry_commit_with(|| coordinator.commit_offsets(&offsets)).await;
        crate::interceptor::safe_on_commit(&*self.interceptor, &committed, result.as_ref().err());
        result
    }
}

/// Run `commit_once`, retrying retriable errors twice (100 ms, 250 ms).
async fn retry_commit_with<F, Fut>(mut commit_once: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut last_error = match commit_once().await {
        Ok(()) => return Ok(()),
        Err(error) if error.is_retriable() => error,
        Err(error) => return Err(error),
    };
    for delay in [Duration::from_millis(100), Duration::from_millis(250)] {
        debug!("Commit failed with retriable error, retrying in {delay:?}: {last_error}");
        tokio::time::sleep(delay).await;
        match commit_once().await {
            Ok(()) => return Ok(()),
            Err(error) if error.is_retriable() => last_error = error,
            Err(error) => return Err(error),
        }
    }
    Err(last_error)
}

impl Consumer {
    pub(in crate::consumer) fn committer(&self) -> Committer {
        Committer {
            state: Arc::clone(&self.state),
            gate: Arc::clone(&self.commit_gate),
            coordinator: self.group_coordinator.clone(),
            interceptor: Arc::clone(&self.interceptor),
            metrics: Arc::clone(&self.metrics),
            metrics_source: Arc::clone(&self.metrics_source),
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::error::{ErrorCode, KrafkaError};

    async fn run(errors: Vec<ErrorCode>) -> (Result<()>, usize) {
        let attempts = Arc::new(AtomicUsize::new(0));
        let errors = Arc::new(errors);
        let result = retry_commit_with({
            let attempts = attempts.clone();
            move || {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                let errors = errors.clone();
                async move {
                    match errors.get(attempt) {
                        Some(&code) => Err(KrafkaError::broker(code, "commit")),
                        None => Ok(()),
                    }
                }
            }
        })
        .await;
        (result, attempts.load(Ordering::SeqCst))
    }

    #[tokio::test(start_paused = true)]
    async fn retriable_errors_are_retried_until_success() {
        let (result, attempts) = run(vec![
            ErrorCode::CoordinatorLoadInProgress,
            ErrorCode::CoordinatorLoadInProgress,
        ])
        .await;
        result.unwrap();
        assert_eq!(attempts, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn exhausted_retries_return_the_last_error() {
        let (result, attempts) = run(vec![ErrorCode::CoordinatorLoadInProgress; 5]).await;
        assert!(matches!(
            result,
            Err(KrafkaError::Broker {
                code: ErrorCode::CoordinatorLoadInProgress,
                ..
            })
        ));
        assert_eq!(attempts, 3);
    }

    #[tokio::test(start_paused = true)]
    async fn a_non_retriable_error_stops_at_once() {
        let (result, attempts) = run(vec![ErrorCode::GroupAuthorizationFailed]).await;
        assert!(result.is_err());
        assert_eq!(attempts, 1);
    }
}
