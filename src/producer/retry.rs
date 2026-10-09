//! The one retry backoff every producer retry loop uses.
//!
//! Produce retries, `InitProducerId` and the transaction coordinator RPCs all
//! back off the same way: exponentially from `retry_backoff`, with ±20 %
//! jitter, capped at one second (Java's `retry.backoff.max.ms`, KIP-580), and
//! bounded by a deadline rather than a count.

use std::future::Future;
use std::time::Duration;

use tokio::time::Instant;
use tracing::debug;

use crate::error::{KrafkaError, Result};
use crate::util::BackoffPolicy;

/// The cap on any single producer retry delay.
pub(crate) const MAX_BACKOFF: Duration = Duration::from_secs(1);

/// Exponential backoff from `retry_backoff`, ±20 % jitter, capped at
/// [`MAX_BACKOFF`].
#[derive(Debug, Clone)]
pub(crate) struct Backoff {
    policy: BackoffPolicy,
}

impl Backoff {
    /// Backoff starting at `initial` (the producer's `retry_backoff`).
    pub(crate) fn new(initial: Duration) -> Self {
        Self {
            policy: BackoffPolicy {
                initial_backoff: initial.min(MAX_BACKOFF),
                max_backoff: MAX_BACKOFF,
                backoff_multiplier: 2.0,
                jitter_factor: 0.2,
            },
        }
    }

    /// The delay before retry number `retry` (the first retry is 1).
    #[inline]
    pub(crate) fn delay(&self, retry: u32) -> Duration {
        self.policy.calculate_backoff(retry.max(1)).min(MAX_BACKOFF)
    }
}

/// Run `attempt` until it succeeds, fails with a non-retriable error, or
/// `deadline` passes.
///
/// Each attempt is itself bounded by what is left of the deadline. `attempt`
/// receives the zero-based attempt number. When the deadline passes the last
/// error is returned, or a timeout naming `what` if no attempt finished.
pub(crate) async fn until_deadline<T, F, Fut>(
    backoff: &Backoff,
    deadline: Instant,
    what: &str,
    mut attempt: F,
) -> Result<T>
where
    F: FnMut(u32) -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut last_error: Option<KrafkaError> = None;
    for number in 0u32.. {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, attempt(number)).await {
            Ok(Ok(value)) => return Ok(value),
            Ok(Err(error)) if !error.is_retriable() => return Err(error),
            Ok(Err(error)) => {
                debug!(what, attempt = number, %error, "retriable failure; backing off");
                last_error = Some(error);
            }
            Err(_) => {
                last_error = Some(KrafkaError::timeout(what));
                break;
            }
        }
        let delay = backoff
            .delay(number + 1)
            .min(deadline.saturating_duration_since(Instant::now()));
        tokio::time::sleep(delay).await;
    }
    Err(last_error.unwrap_or_else(|| KrafkaError::timeout(what)))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    use crate::error::ErrorCode;

    /// No delay exceeds one second, however many retries came before.
    #[test]
    fn the_delay_is_capped_at_one_second() {
        let backoff = Backoff::new(Duration::from_millis(100));
        for retry in 1..64 {
            assert!(backoff.delay(retry) <= MAX_BACKOFF, "retry {retry}");
        }
        assert!(
            backoff.delay(20) >= Duration::from_millis(800),
            "it does grow"
        );
    }

    /// The first retry waits about `retry_backoff`, within the jitter.
    #[test]
    fn the_first_delay_is_the_configured_backoff() {
        let backoff = Backoff::new(Duration::from_millis(100));
        let first = backoff.delay(1);
        assert!(
            first >= Duration::from_millis(100) && first <= Duration::from_millis(120),
            "{first:?}"
        );
    }

    /// A `retry_backoff` above the cap is held to the cap.
    #[test]
    fn a_large_initial_backoff_is_held_to_the_cap() {
        let backoff = Backoff::new(Duration::from_secs(5));
        assert!(backoff.delay(1) <= MAX_BACKOFF);
    }

    #[tokio::test]
    async fn retries_a_retriable_error_until_it_succeeds() {
        let backoff = Backoff::new(Duration::from_millis(1));
        let deadline = Instant::now() + Duration::from_secs(5);
        let result = until_deadline(&backoff, deadline, "op", |n| async move {
            if n < 3 {
                Err(KrafkaError::broker(ErrorCode::NotCoordinator, "moving"))
            } else {
                Ok(n)
            }
        })
        .await;
        assert_eq!(result.unwrap(), 3);
    }

    #[tokio::test]
    async fn a_non_retriable_error_stops_at_once() {
        let backoff = Backoff::new(Duration::from_millis(1));
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut calls = 0;
        let result: Result<()> = until_deadline(&backoff, deadline, "op", |_| {
            calls += 1;
            async { Err(KrafkaError::config("no")) }
        })
        .await;
        assert!(result.is_err());
        assert_eq!(calls, 1);
    }

    /// The deadline, not a count, ends the loop; the last error is kept.
    #[tokio::test]
    async fn the_deadline_ends_the_loop_with_the_last_error() {
        let backoff = Backoff::new(Duration::from_millis(5));
        let started = Instant::now();
        let result: Result<()> = until_deadline(
            &backoff,
            started + Duration::from_millis(100),
            "op",
            |_| async { Err(KrafkaError::broker(ErrorCode::NotCoordinator, "moving")) },
        )
        .await;
        assert!(started.elapsed() < Duration::from_millis(500));
        assert!(
            matches!(
                result,
                Err(KrafkaError::Broker {
                    code: ErrorCode::NotCoordinator,
                    ..
                })
            ),
            "{result:?}"
        );
    }

    /// An attempt that outlives the deadline is cut off there.
    #[tokio::test]
    async fn a_hanging_attempt_is_bounded_by_the_deadline() {
        let backoff = Backoff::new(Duration::from_millis(5));
        let started = Instant::now();
        let result: Result<()> = until_deadline(
            &backoff,
            started + Duration::from_millis(50),
            "hang",
            |_| std::future::pending(),
        )
        .await;
        assert!(started.elapsed() < Duration::from_millis(500));
        assert!(
            matches!(result, Err(KrafkaError::Timeout { .. })),
            "{result:?}"
        );
    }
}
