//! Producer retry policy with exponential backoff.

use std::time::{Duration, Instant};

use tracing::{debug, warn};

use crate::error::KrafkaError;

/// Configuration for retry behavior with exponential backoff.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retries (0 = no retries).
    pub max_retries: u32,
    /// Initial backoff duration.
    pub initial_backoff: Duration,
    /// Maximum backoff duration (caps exponential growth).
    pub max_backoff: Duration,
    /// Backoff multiplier for exponential growth (typically 2.0).
    pub backoff_multiplier: f64,
    /// Jitter factor (0.0-1.0) to add randomness to backoff.
    pub jitter_factor: f64,
    /// Total time budget for all retries.
    ///
    /// When set, retries stop once the elapsed time since the first attempt
    /// exceeds this duration, even if `max_retries` has not been reached.
    /// Similar to Kafka's `delivery.timeout.ms`.
    pub delivery_timeout: Option<Duration>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
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

    /// Set the maximum number of retries.
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the initial backoff duration.
    pub fn with_initial_backoff(mut self, duration: Duration) -> Self {
        self.initial_backoff = duration;
        self
    }

    /// Set the maximum backoff duration.
    pub fn with_max_backoff(mut self, duration: Duration) -> Self {
        self.max_backoff = duration;
        self
    }

    /// Set the backoff multiplier.
    pub fn with_backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.backoff_multiplier = multiplier;
        self
    }

    /// Set the jitter factor (0.0-1.0).
    pub fn with_jitter_factor(mut self, factor: f64) -> Self {
        self.jitter_factor = factor.clamp(0.0, 1.0);
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
    /// Attempt 0 returns `Duration::ZERO` (no retry yet). Attempt 1 = first
    /// retry = `initial_backoff`. Subsequent attempts grow exponentially.
    #[inline]
    pub fn calculate_backoff(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }

        // Exponential backoff: initial * multiplier^(attempt-1)
        let base_backoff =
            self.initial_backoff.as_secs_f64() * self.backoff_multiplier.powi((attempt - 1) as i32);

        // Cap at max backoff
        let capped_backoff = base_backoff.min(self.max_backoff.as_secs_f64());

        // Add jitter: ±jitter_factor * backoff (randomized to prevent thundering herd)
        let jitter_range = capped_backoff * self.jitter_factor;
        let jitter = if self.jitter_factor > 0.0 {
            use rand::Rng;
            let mut rng = rand::rng();
            rng.random_range(-jitter_range..=jitter_range)
        } else {
            0.0
        };

        let final_backoff = (capped_backoff + jitter).max(0.0);
        Duration::from_secs_f64(final_backoff)
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
    /// Current attempt number (0 = first attempt).
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

        // Check delivery timeout first — elapsed time trumps retry count.
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

        // Check retriability *before* incrementing so that `max_retries = N`
        // yields exactly N retries (matching the transaction.rs retry loops
        // which use `for attempt in 0..=max_retries`).
        if self.policy.should_retry(error, self.attempt) {
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
        } else {
            if self.policy.max_retries_reached(self.attempt + 1) {
                warn!(
                    operation = %self.operation,
                    attempt = self.attempt,
                    max_retries = self.policy.max_retries,
                    error = %error,
                    "Max retries reached, giving up"
                );
            } else {
                debug!(
                    operation = %self.operation,
                    error = %error,
                    "Non-retriable error, not retrying"
                );
            }
            None
        }
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
mod tests {
    use super::*;

    #[test]
    fn test_retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries, 3);
        assert_eq!(policy.initial_backoff, Duration::from_millis(100));
        assert_eq!(policy.max_backoff, Duration::from_secs(10));
        assert_eq!(policy.backoff_multiplier, 2.0);
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
        assert_eq!(policy.initial_backoff, Duration::from_millis(50));
        assert_eq!(policy.max_backoff, Duration::from_secs(5));
        assert_eq!(policy.backoff_multiplier, 3.0);
        assert_eq!(policy.jitter_factor, 0.2);
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
        assert_eq!(policy.jitter_factor, 1.0);

        let policy = RetryPolicy::new().with_jitter_factor(-0.5); // Negative, should clamp
        assert_eq!(policy.jitter_factor, 0.0);
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
}
