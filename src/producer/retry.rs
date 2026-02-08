//! Producer retry policy with exponential backoff.

use std::time::Duration;

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
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
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

    /// Calculate the backoff duration for a given attempt number (0-indexed).
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

        // Add jitter: ±jitter_factor * backoff
        let jitter_range = capped_backoff * self.jitter_factor;
        let jitter = if self.jitter_factor > 0.0 {
            // Simple deterministic "jitter" based on attempt number for reproducibility
            // In production, consider using rand crate
            let jitter_sign = if attempt % 2 == 0 { 1.0 } else { -1.0 };
            jitter_sign * jitter_range * 0.5
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
}

impl RetryContext {
    /// Create a new retry context.
    pub fn new(policy: RetryPolicy, operation: impl Into<String>) -> Self {
        Self {
            policy,
            attempt: 0,
            operation: operation.into(),
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
        self.attempt += 1;

        if self.policy.should_retry(error, self.attempt) {
            let backoff = self.policy.calculate_backoff(self.attempt);
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
            if self.policy.max_retries_reached(self.attempt) {
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
        // max_retries=3 means: initial try + up to 3 retries = 4 total attempts
        // But we check attempt < max_retries, so we get 3 retries after initial
        let policy = RetryPolicy::new().with_max_retries(3);
        let mut ctx = RetryContext::new(policy, "test_operation");

        assert_eq!(ctx.attempt(), 0);
        assert_eq!(ctx.operation(), "test_operation");

        // First failure (attempt becomes 1, 1 < 3 = true)
        let error = KrafkaError::timeout("test");
        let backoff = ctx.record_failure(&error);
        assert!(backoff.is_some());
        assert_eq!(ctx.attempt(), 1);

        // Second failure (attempt becomes 2, 2 < 3 = true)
        let backoff = ctx.record_failure(&error);
        assert!(backoff.is_some());
        assert_eq!(ctx.attempt(), 2);

        // Third failure (attempt becomes 3, 3 < 3 = false)
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
}
