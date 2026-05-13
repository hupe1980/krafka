//! Producer retry policy with exponential backoff.

use std::time::{Duration, Instant};

use tracing::{debug, warn};

use crate::error::KrafkaError;
use crate::util::BackoffPolicy;

/// Configuration for retry behavior with exponential backoff.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retries (0 = no retries).
    pub(crate) max_retries: u32,
    /// Shared exponential-backoff parameters.
    pub(crate) backoff: BackoffPolicy,
    /// Total time budget for all retries.
    ///
    /// When set, retries stop once the elapsed time since the first attempt
    /// exceeds this duration, even if `max_retries` has not been reached.
    /// Similar to Kafka's `delivery.timeout.ms`.
    pub(crate) delivery_timeout: Option<Duration>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            backoff: BackoffPolicy::default(),
            delivery_timeout: Some(Duration::from_secs(120)),
        }
    }
}

impl RetryPolicy {
    /// Create a new retry policy with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a retry policy that performs no retries.
    pub fn no_retries() -> Self {
        Self {
            max_retries: 0,
            ..Self::default()
        }
    }

    /// Maximum number of retries (0 = no retries).
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// Initial backoff duration.
    pub fn initial_backoff(&self) -> Duration {
        self.backoff.initial_backoff
    }

    /// Maximum backoff duration (caps exponential growth).
    pub fn max_backoff(&self) -> Duration {
        self.backoff.max_backoff
    }

    /// Backoff multiplier for exponential growth.
    pub fn backoff_multiplier(&self) -> f64 {
        self.backoff.backoff_multiplier
    }

    /// Jitter factor (0.0-1.0) applied to backoff.
    pub fn jitter_factor(&self) -> f64 {
        self.backoff.jitter_factor
    }

    /// Total time budget for all retries, or `None` if unlimited.
    pub fn delivery_timeout(&self) -> Option<Duration> {
        self.delivery_timeout
    }

    /// Set the maximum number of retries.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the initial backoff duration.
    pub fn with_initial_backoff(mut self, duration: Duration) -> Self {
        self.backoff.initial_backoff = duration;
        self
    }

    /// Set the maximum backoff duration.
    pub fn with_max_backoff(mut self, duration: Duration) -> Self {
        self.backoff.max_backoff = duration;
        self
    }

    /// Set the backoff multiplier.
    ///
    /// Must be finite and ≥ 1.0. Values below 1.0 are clamped to 1.0 (no
    /// shrinkage). Non-finite values (NaN, ±Inf) are replaced with 2.0.
    pub fn with_backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.backoff.backoff_multiplier = if multiplier.is_finite() {
            multiplier.max(1.0)
        } else {
            warn!("backoff_multiplier is not finite ({multiplier}); using default 2.0");
            2.0
        };
        self
    }

    /// Set the jitter factor (0.0-1.0).
    pub fn with_jitter_factor(mut self, factor: f64) -> Self {
        self.backoff.jitter_factor = factor.clamp(0.0, 1.0);
        self
    }

    /// Set the total delivery timeout.
    ///
    /// When set, retries stop once this much time has elapsed since the
    /// first attempt, regardless of `max_retries`. Pass `None` to disable.
    /// Default: 120 seconds.
    pub fn with_delivery_timeout(mut self, timeout: Option<Duration>) -> Self {
        self.delivery_timeout = timeout;
        self
    }

    /// Calculate the backoff duration for a given attempt number.
    ///
    /// Delegates to the embedded [`BackoffPolicy`].
    #[inline]
    pub fn calculate_backoff(&self, attempt: u32) -> Duration {
        self.backoff.calculate_backoff(attempt)
    }

    /// Check if an error is retriable and we haven't exceeded max retries.
    #[inline]
    pub fn should_retry(&self, error: &KrafkaError, attempt: u32) -> bool {
        attempt < self.max_retries && error.is_retriable()
    }

    /// Check if the maximum number of retries has been reached.
    #[inline]
    pub fn max_retries_reached(&self, attempt: u32) -> bool {
        attempt >= self.max_retries
    }
}

/// Retry context for tracking retry state.
#[derive(Debug)]
pub struct RetryContext {
    /// The retry policy.
    policy: RetryPolicy,
    /// Number of retries attempted so far (0 = no retries yet, only the initial attempt).
    attempt: u32,
    /// The operation being retried.
    operation: String,
    /// When the first attempt started (for delivery_timeout).
    started_at: Instant,
}

impl RetryContext {
    /// Create a new retry context.
    pub fn new(policy: RetryPolicy, operation: impl Into<String>) -> Self {
        Self {
            policy,
            attempt: 0,
            operation: operation.into(),
            started_at: Instant::now(),
        }
    }

    /// Create a retry context with a custom start time.
    ///
    /// Use this when the delivery timeout should cover time spent waiting
    /// in a buffer (e.g., the accumulator's linger window) rather than
    /// starting from the first send attempt.
    pub fn new_with_start(
        policy: RetryPolicy,
        operation: impl Into<String>,
        started_at: Instant,
    ) -> Self {
        Self {
            policy,
            attempt: 0,
            operation: operation.into(),
            started_at,
        }
    }

    /// Get the current attempt number.
    #[inline]
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Get the operation name.
    #[inline]
    pub fn operation(&self) -> &str {
        &self.operation
    }

    /// Record a failed attempt and determine if we should retry.
    ///
    /// Returns `Some(backoff_duration)` if we should retry, `None` if we should give up.
    pub fn record_failure(&mut self, error: &KrafkaError) -> Option<Duration> {
        let elapsed = self.started_at.elapsed();

        // 1. Delivery timeout — elapsed time trumps everything else.
        if let Some(deadline) = self.policy.delivery_timeout
            && elapsed >= deadline
        {
            warn!(
                operation = %self.operation,
                attempt = self.attempt,
                elapsed_ms = elapsed.as_millis(),
                error = %error,
                "Delivery timeout exceeded, giving up"
            );
            return None;
        }

        // 2. Delegate retriability + count check to RetryPolicy.
        if !self.policy.should_retry(error, self.attempt) {
            if !error.is_retriable() {
                debug!(
                    operation = %self.operation,
                    error = %error,
                    "Non-retriable error, not retrying"
                );
            } else {
                warn!(
                    operation = %self.operation,
                    attempt = self.attempt,
                    max_retries = self.policy.max_retries,
                    error = %error,
                    "Max retries reached, giving up"
                );
            }
            return None;
        }

        // 3. Retry — increment *after* all give-up checks pass.
        self.attempt += 1;
        let backoff = self.policy.calculate_backoff(self.attempt);

        // Clamp backoff so it doesn't exceed remaining delivery budget.
        // The `elapsed >= deadline` check above already handles the
        // zero-remaining case, so `remaining` is always positive here.
        let backoff = if let Some(deadline) = self.policy.delivery_timeout {
            let remaining = deadline.saturating_sub(elapsed);
            backoff.min(remaining)
        } else {
            backoff
        };

        debug!(
            operation = %self.operation,
            attempt = self.attempt,
            max_retries = self.policy.max_retries,
            backoff_ms = backoff.as_millis(),
            error = %error,
            "Retrying after failure"
        );
        Some(backoff)
    }

    /// Record a successful attempt.
    pub fn record_success(&self) {
        if self.attempt > 0 {
            debug!(
                operation = %self.operation,
                attempt = self.attempt,
                "Succeeded after retries"
            );
        }
    }

    /// Wait for the next retry with the given backoff.
    pub async fn wait(&self, backoff: Duration) {
        if !backoff.is_zero() {
            tokio::time::sleep(backoff).await;
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries, 3);
        assert_eq!(policy.initial_backoff(), Duration::from_millis(100));
        assert_eq!(policy.max_backoff(), Duration::from_secs(10));
        assert_eq!(policy.backoff_multiplier(), 2.0);
    }

    #[test]
    fn test_retry_policy_no_retries() {
        let policy = RetryPolicy::no_retries();
        assert_eq!(policy.max_retries, 0);
    }

    #[test]
    fn test_retry_policy_builder() {
        let policy = RetryPolicy::new()
            .with_max_retries(5)
            .with_initial_backoff(Duration::from_millis(50))
            .with_max_backoff(Duration::from_secs(5))
            .with_backoff_multiplier(3.0)
            .with_jitter_factor(0.2);

        assert_eq!(policy.max_retries, 5);
        assert_eq!(policy.initial_backoff(), Duration::from_millis(50));
        assert_eq!(policy.max_backoff(), Duration::from_secs(5));
        assert_eq!(policy.backoff_multiplier(), 3.0);
        assert_eq!(policy.jitter_factor(), 0.2);
    }

    #[test]
    fn test_calculate_backoff_exponential() {
        let policy = RetryPolicy::new()
            .with_initial_backoff(Duration::from_millis(100))
            .with_backoff_multiplier(2.0)
            .with_jitter_factor(0.0); // Disable jitter for testing

        // Attempt 0 = no backoff (first try)
        assert_eq!(policy.calculate_backoff(0), Duration::ZERO);

        // Attempt 1 = initial backoff
        assert_eq!(policy.calculate_backoff(1), Duration::from_millis(100));

        // Attempt 2 = initial * 2
        assert_eq!(policy.calculate_backoff(2), Duration::from_millis(200));

        // Attempt 3 = initial * 4
        assert_eq!(policy.calculate_backoff(3), Duration::from_millis(400));
    }

    #[test]
    fn test_calculate_backoff_capped() {
        let policy = RetryPolicy::new()
            .with_initial_backoff(Duration::from_secs(1))
            .with_max_backoff(Duration::from_secs(5))
            .with_backoff_multiplier(10.0)
            .with_jitter_factor(0.0);

        // Attempt 2 would be 10 seconds, but capped at 5
        assert_eq!(policy.calculate_backoff(2), Duration::from_secs(5));
    }

    #[test]
    fn test_calculate_backoff_handles_max_attempt() {
        let policy = RetryPolicy::new()
            .with_initial_backoff(Duration::from_millis(100))
            .with_max_backoff(Duration::from_secs(10))
            .with_jitter_factor(0.0);

        assert_eq!(policy.calculate_backoff(u32::MAX), Duration::from_secs(10));
    }

    #[test]
    fn test_should_retry() {
        let policy = RetryPolicy::new().with_max_retries(3);

        // Retriable error, under limit
        let retriable_error = KrafkaError::timeout("test");
        assert!(policy.should_retry(&retriable_error, 0));
        assert!(policy.should_retry(&retriable_error, 1));
        assert!(policy.should_retry(&retriable_error, 2));
        assert!(!policy.should_retry(&retriable_error, 3)); // At limit
        assert!(!policy.should_retry(&retriable_error, 4)); // Over limit

        // Non-retriable error
        let non_retriable = KrafkaError::config("test");
        assert!(!policy.should_retry(&non_retriable, 0));
    }

    #[test]
    fn test_retry_context() {
        // max_retries=3 → 3 retries (4 total attempts including the initial).
        // should_retry is checked before incrementing attempt, so
        // attempt 0, 1, 2 all pass the `< 3` gate.
        let policy = RetryPolicy::new().with_max_retries(3);
        let mut ctx = RetryContext::new(policy, "test_operation");

        assert_eq!(ctx.attempt(), 0);
        assert_eq!(ctx.operation(), "test_operation");

        // First failure — should_retry(0) = true, then attempt becomes 1
        let error = KrafkaError::timeout("test");
        let backoff = ctx.record_failure(&error);
        assert!(backoff.is_some());
        assert_eq!(ctx.attempt(), 1);

        // Second failure — should_retry(1) = true, then attempt becomes 2
        let backoff = ctx.record_failure(&error);
        assert!(backoff.is_some());
        assert_eq!(ctx.attempt(), 2);

        // Third failure — should_retry(2) = true, then attempt becomes 3
        let backoff = ctx.record_failure(&error);
        assert!(backoff.is_some());
        assert_eq!(ctx.attempt(), 3);

        // Fourth failure — should_retry(3) = false, max retries exhausted
        let backoff = ctx.record_failure(&error);
        assert!(backoff.is_none());
        assert_eq!(ctx.attempt(), 3);
    }

    #[test]
    fn test_retry_context_non_retriable() {
        let policy = RetryPolicy::new().with_max_retries(5);
        let mut ctx = RetryContext::new(policy, "test");

        // Non-retriable error should not retry even on first attempt
        let error = KrafkaError::config("invalid config");
        let backoff = ctx.record_failure(&error);
        assert!(backoff.is_none());
    }

    #[test]
    fn test_jitter_factor_clamped() {
        let policy = RetryPolicy::new().with_jitter_factor(2.0); // Over 1.0, should clamp
        assert_eq!(policy.jitter_factor(), 1.0);

        let policy = RetryPolicy::new().with_jitter_factor(-0.5); // Negative, should clamp
        assert_eq!(policy.jitter_factor(), 0.0);
    }

    #[test]
    fn test_calculate_backoff_never_below_initial_backoff() {
        // Even with maximum jitter (factor=1.0), no backoff result should fall
        // below `initial_backoff`. Previously `.max(0.0)` allowed near-zero delays.
        let policy = RetryPolicy::new()
            .with_initial_backoff(Duration::from_millis(100))
            .with_backoff_multiplier(2.0)
            .with_jitter_factor(1.0); // maximum — ±100% of backoff

        let floor = policy.initial_backoff();
        for attempt in 1..=5 {
            let backoff = policy.calculate_backoff(attempt);
            assert!(
                backoff >= floor,
                "attempt {attempt}: backoff {backoff:?} fell below initial_backoff {floor:?}"
            );
        }
    }

    #[test]
    fn test_calculate_backoff_jitter_produces_varying_results() {
        let policy = RetryPolicy::new()
            .with_initial_backoff(Duration::from_millis(100))
            .with_backoff_multiplier(2.0)
            .with_jitter_factor(0.5); // 50% jitter

        // Collect multiple backoff values for the same attempt
        let backoffs: Vec<Duration> = (0..50).map(|_| policy.calculate_backoff(2)).collect();

        // With 50% jitter on a 200ms base, values should range from 100ms to 300ms.
        // Check that not all values are identical (i.e., jitter is actually applied).
        let unique_count = {
            let mut unique: Vec<u128> = backoffs.iter().map(|d| d.as_nanos()).collect();
            unique.sort();
            unique.dedup();
            unique.len()
        };

        assert!(
            unique_count > 1,
            "with jitter_factor > 0, calculate_backoff should produce varying results, but got {} unique values",
            unique_count
        );
    }

    #[test]
    fn test_delivery_timeout_gives_up_when_exceeded() {
        let policy = RetryPolicy::new()
            .with_max_retries(10) // Plenty of retries remaining
            .with_delivery_timeout(Some(Duration::from_millis(50)));

        // Start 100ms in the past — elapsed already exceeds the 50ms deadline.
        let started_at = Instant::now() - Duration::from_millis(100);
        let mut ctx = RetryContext::new_with_start(policy, "test_timeout", started_at);

        let error = KrafkaError::timeout("test");
        let result = ctx.record_failure(&error);

        assert!(
            result.is_none(),
            "should give up when delivery timeout exceeded"
        );
        // Attempt counter is not incremented on timeout (failure is immediate).
        assert_eq!(ctx.attempt(), 0);
    }

    #[test]
    fn test_delivery_timeout_clamps_backoff_to_remaining_budget() {
        let policy = RetryPolicy::new()
            .with_max_retries(5)
            .with_initial_backoff(Duration::from_secs(10)) // Very large backoff
            .with_jitter_factor(0.0)
            .with_delivery_timeout(Some(Duration::from_secs(1)));

        // 900ms elapsed — 100ms remaining in the 1s budget.
        let started_at = Instant::now() - Duration::from_millis(900);
        let mut ctx = RetryContext::new_with_start(policy, "test_clamp", started_at);

        let error = KrafkaError::timeout("test");
        let backoff = ctx
            .record_failure(&error)
            .expect("should still retry within budget");

        // The raw backoff would be 10s, but it must be clamped to ≤ remaining (~100ms).
        assert!(
            backoff <= Duration::from_millis(150),
            "backoff ({backoff:?}) should be clamped to remaining delivery budget"
        );
    }

    #[test]
    fn test_delivery_timeout_disabled_does_not_limit_retries() {
        let policy = RetryPolicy::new()
            .with_max_retries(2)
            .with_delivery_timeout(None);

        // Even with a very old start time, timeout never fires.
        let started_at = Instant::now() - Duration::from_secs(3600);
        let mut ctx = RetryContext::new_with_start(policy, "test_no_timeout", started_at);

        let error = KrafkaError::timeout("test");
        let backoff = ctx.record_failure(&error);
        assert!(
            backoff.is_some(),
            "should retry when delivery_timeout is None"
        );
    }
}
