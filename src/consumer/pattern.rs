//! Topic patterns for [`Consumer::subscribe_pattern`](super::Consumer::subscribe_pattern)
//! and [`Consumer::subscribe_broker_pattern`](super::Consumer::subscribe_broker_pattern).

use crate::error::{KrafkaError, Result};

/// A topic pattern the consumer matches itself, against the cluster's topic
/// list.
///
/// The syntax is Rust regular-expression syntax without Unicode classes
/// (the [`regex-lite`](https://docs.rs/regex-lite) dialect). A pattern matches
/// a topic only when it matches the **whole** name: `orders-.*` matches
/// `orders-eu` but not `old-orders-eu`.
///
/// ```
/// use krafka::consumer::TopicPattern;
///
/// let pattern = TopicPattern::new("orders-.*")?;
/// assert!(pattern.matches("orders-eu"));
/// assert!(!pattern.matches("old-orders-eu"));
/// # Ok::<(), krafka::KrafkaError>(())
/// ```
#[derive(Clone)]
pub struct TopicPattern {
    source: String,
    matcher: regex_lite::Regex,
}

impl TopicPattern {
    /// Compile `pattern`.
    ///
    /// # Errors
    ///
    /// [`KrafkaError::Config`] naming the pattern when it does not compile.
    pub fn new(pattern: &str) -> Result<Self> {
        let matcher = regex_lite::Regex::new(&format!("^(?:{pattern})$"))
            .map_err(|e| KrafkaError::config(format!("invalid topic pattern {pattern:?}: {e}")))?;
        Ok(Self {
            source: pattern.to_string(),
            matcher,
        })
    }

    /// The pattern as written.
    pub fn as_str(&self) -> &str {
        &self.source
    }

    /// Whether the pattern matches the whole of `topic`.
    pub fn matches(&self, topic: &str) -> bool {
        self.matcher.is_match(topic)
    }
}

impl std::fmt::Debug for TopicPattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("TopicPattern").field(&self.source).finish()
    }
}

impl PartialEq for TopicPattern {
    fn eq(&self, other: &Self) -> bool {
        self.source == other.source
    }
}

impl Eq for TopicPattern {}

/// A topic pattern the group coordinator evaluates (KIP-848).
///
/// Sent to the broker as written and matched there with RE2J syntax against
/// whole topic names; the client does not parse it. Usable only with
/// [`GroupProtocol::Consumer`](super::GroupProtocol::Consumer) on a broker
/// that supports `ConsumerGroupHeartbeat` v1 (Kafka 4.0+).
///
/// ```
/// use krafka::consumer::SubscriptionPattern;
///
/// let pattern = SubscriptionPattern::new("orders-.*")?;
/// assert_eq!(pattern.as_str(), "orders-.*");
/// # Ok::<(), krafka::KrafkaError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionPattern {
    source: String,
}

impl SubscriptionPattern {
    /// Wrap `pattern`.
    ///
    /// # Errors
    ///
    /// [`KrafkaError::Config`] when `pattern` is empty.
    pub fn new(pattern: impl Into<String>) -> Result<Self> {
        let source = pattern.into();
        if source.is_empty() {
            return Err(KrafkaError::config(
                "subscription pattern must not be empty",
            ));
        }
        Ok(Self { source })
    }

    /// The pattern as written.
    pub fn as_str(&self) -> &str {
        &self.source
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn a_topic_pattern_matches_whole_names_only() {
        let pattern = TopicPattern::new("pat-.*").unwrap();
        assert!(pattern.matches("pat-a"));
        assert!(pattern.matches("pat-a-x"));
        assert!(!pattern.matches("xpat-a"));
        assert!(!pattern.matches("pat"));
    }

    #[test]
    fn alternation_stays_whole_name() {
        let pattern = TopicPattern::new("a|b").unwrap();
        assert!(pattern.matches("a"));
        assert!(pattern.matches("b"));
        assert!(!pattern.matches("ab"));
        assert!(!pattern.matches("xa"));
    }

    #[test]
    fn an_invalid_topic_pattern_is_a_config_error_naming_it() {
        let err = TopicPattern::new("(").unwrap_err();
        assert!(matches!(err, KrafkaError::Config { .. }), "{err:?}");
        assert!(err.to_string().contains("\"(\""), "{err}");
    }

    #[test]
    fn topic_patterns_compare_and_print_by_source() {
        let a = TopicPattern::new("x.*").unwrap();
        assert_eq!(a, TopicPattern::new("x.*").unwrap());
        assert_ne!(a, TopicPattern::new("y.*").unwrap());
        assert_eq!(a.as_str(), "x.*");
        assert_eq!(format!("{a:?}"), "TopicPattern(\"x.*\")");
    }

    #[test]
    fn an_empty_subscription_pattern_is_rejected() {
        let err = SubscriptionPattern::new("").unwrap_err();
        assert!(matches!(err, KrafkaError::Config { .. }), "{err:?}");
        assert_eq!(SubscriptionPattern::new("a.*").unwrap().as_str(), "a.*");
    }
}
