//! Tracing extensions for observability.
//!
//! This module provides OpenTelemetry-compatible tracing utilities:
//! - Span helpers for producer and consumer operations
//! - Semantic conventions for Kafka messaging
//! - Error recording utilities
//!
//! # OpenTelemetry Semantic Conventions
//!
//! This module follows the [OpenTelemetry Semantic Conventions for Messaging](https://opentelemetry.io/docs/specs/semconv/messaging/).
//!
//! # Example
//!
//! ```ignore
//! use krafka::tracing_ext::{kafka_producer_span, kafka_consumer_span, record_error};
//! use tracing::{Instrument, info_span};
//!
//! async fn produce() {
//!     let span = kafka_producer_span("my-topic", Some(0), Some(b"key"));
//!     async {
//!         // produce message
//!     }.instrument(span).await;
//! }
//! ```

use crate::PartitionId;
use tracing::{Level, Span};

/// OpenTelemetry semantic convention: messaging system.
pub const MESSAGING_SYSTEM: &str = "messaging.system";
/// OpenTelemetry semantic convention: messaging destination.
pub const MESSAGING_DESTINATION: &str = "messaging.destination.name";
/// OpenTelemetry semantic convention: messaging operation.
pub const MESSAGING_OPERATION: &str = "messaging.operation";
/// OpenTelemetry semantic convention: Kafka partition.
pub const MESSAGING_KAFKA_PARTITION: &str = "messaging.kafka.destination.partition";
/// OpenTelemetry semantic convention: Kafka message offset.
pub const MESSAGING_KAFKA_OFFSET: &str = "messaging.kafka.message.offset";
/// OpenTelemetry semantic convention: Kafka consumer group.
pub const MESSAGING_KAFKA_CONSUMER_GROUP: &str = "messaging.kafka.consumer.group";
/// OpenTelemetry semantic convention: message key.
pub const MESSAGING_MESSAGE_KEY: &str = "messaging.message.id";
/// OpenTelemetry semantic convention: message body size.
pub const MESSAGING_MESSAGE_BODY_SIZE: &str = "messaging.message.body.size";
/// OpenTelemetry semantic convention: batch message count.
pub const MESSAGING_BATCH_MESSAGE_COUNT: &str = "messaging.batch.message_count";
/// Krafka-specific: correlation ID.
pub const KRAFKA_CORRELATION_ID: &str = "krafka.correlation_id";
/// Krafka-specific: compression type.
pub const KRAFKA_COMPRESSION: &str = "krafka.compression";
/// Krafka-specific: acks.
pub const KRAFKA_ACKS: &str = "krafka.acks";

/// Create a span for a Kafka producer send operation.
///
/// This span follows OpenTelemetry semantic conventions for messaging.
///
/// # Arguments
///
/// * `topic` - The destination topic name
/// * `partition` - Optional partition ID (if known before send)
/// * `key` - Optional message key (for logging)
///
/// # Returns
///
/// A tracing `Span` configured with Kafka producer attributes.
#[inline]
pub fn kafka_producer_span(
    topic: &str,
    partition: Option<PartitionId>,
    key: Option<&[u8]>,
) -> Span {
    let span = tracing::span!(
        Level::INFO,
        "kafka.produce",
        { MESSAGING_SYSTEM } = tracing::field::Empty,
        { MESSAGING_OPERATION } = tracing::field::Empty,
        { MESSAGING_DESTINATION } = tracing::field::Empty,
        { MESSAGING_KAFKA_PARTITION } = tracing::field::Empty,
        { MESSAGING_MESSAGE_KEY } = tracing::field::Empty,
    );

    span.record(MESSAGING_SYSTEM, "kafka");
    span.record(MESSAGING_OPERATION, "publish");
    span.record(MESSAGING_DESTINATION, topic);

    if let Some(p) = partition {
        span.record(MESSAGING_KAFKA_PARTITION, p);
    }

    if let Some(k) = key
        && let Ok(key_str) = std::str::from_utf8(k)
    {
        span.record(MESSAGING_MESSAGE_KEY, key_str);
    }

    span
}

/// Create a span for a Kafka consumer poll operation.
///
/// # Arguments
///
/// * `group_id` - Optional consumer group ID
/// * `topics` - Topics being polled
///
/// # Returns
///
/// A tracing `Span` configured with Kafka consumer attributes.
#[inline]
pub fn kafka_consumer_poll_span(group_id: Option<&str>, topics: &[String]) -> Span {
    let topics_str = topics.join(",");
    let span = tracing::span!(
        Level::INFO,
        "kafka.poll",
        { MESSAGING_SYSTEM } = "kafka",
        { MESSAGING_OPERATION } = "receive",
        topics = %topics_str,
    );

    if let Some(gid) = group_id {
        span.record(MESSAGING_KAFKA_CONSUMER_GROUP, gid);
    }

    span
}

/// Create a span for a Kafka consumer fetch operation on a specific partition.
///
/// # Arguments
///
/// * `topic` - The topic name
/// * `partition` - The partition ID
/// * `offset` - The starting offset for the fetch
///
/// # Returns
///
/// A tracing `Span` configured with Kafka fetch attributes.
#[inline]
pub fn kafka_fetch_span(topic: &str, partition: PartitionId, offset: i64) -> Span {
    tracing::span!(
        Level::DEBUG,
        "kafka.fetch",
        { MESSAGING_SYSTEM } = "kafka",
        { MESSAGING_OPERATION } = "receive",
        { MESSAGING_DESTINATION } = topic,
        { MESSAGING_KAFKA_PARTITION } = partition,
        { MESSAGING_KAFKA_OFFSET } = offset,
    )
}

/// Create a span for a Kafka consumer commit operation.
///
/// # Arguments
///
/// * `group_id` - Optional consumer group ID
/// * `topic` - The topic name
/// * `partition` - The partition ID
/// * `offset` - The offset being committed
///
/// # Returns
///
/// A tracing `Span` configured with Kafka commit attributes.
#[inline]
pub fn kafka_commit_span(
    group_id: Option<&str>,
    topic: &str,
    partition: PartitionId,
    offset: i64,
) -> Span {
    let span = tracing::span!(
        Level::DEBUG,
        "kafka.commit",
        { MESSAGING_SYSTEM } = "kafka",
        { MESSAGING_OPERATION } = "settle",
        { MESSAGING_DESTINATION } = topic,
        { MESSAGING_KAFKA_PARTITION } = partition,
        { MESSAGING_KAFKA_OFFSET } = offset,
    );

    if let Some(gid) = group_id {
        span.record(MESSAGING_KAFKA_CONSUMER_GROUP, gid);
    }

    span
}

/// Create a span for a Kafka admin operation.
///
/// # Arguments
///
/// * `operation` - The admin operation name (e.g., "create_topic", "delete_topic")
/// * `resource` - Optional resource name (e.g., topic name)
///
/// # Returns
///
/// A tracing `Span` configured with Kafka admin attributes.
#[inline]
pub fn kafka_admin_span(operation: &str, resource: Option<&str>) -> Span {
    let span = tracing::span!(
        Level::INFO,
        "kafka.admin",
        { MESSAGING_SYSTEM } = "kafka",
        operation = operation,
    );

    if let Some(res) = resource {
        span.record("resource", res);
    }

    span
}

/// Create a span for a Kafka connection operation.
///
/// # Arguments
///
/// * `broker_address` - The broker address (host:port)
/// * `operation` - The connection operation (e.g., "connect", "disconnect")
///
/// # Returns
///
/// A tracing `Span` configured with connection attributes.
#[inline]
pub fn kafka_connection_span(broker_address: &str, operation: &str) -> Span {
    tracing::span!(
        Level::DEBUG,
        "kafka.connection",
        { MESSAGING_SYSTEM } = "kafka",
        broker = broker_address,
        operation = operation,
    )
}

/// Create a span for a Kafka request/response cycle.
///
/// # Arguments
///
/// * `api_key` - The Kafka API key name
/// * `correlation_id` - The correlation ID for the request
///
/// # Returns
///
/// A tracing `Span` configured with request attributes.
#[inline]
pub fn kafka_request_span(api_key: &str, correlation_id: i32) -> Span {
    tracing::span!(
        Level::DEBUG,
        "kafka.request",
        { MESSAGING_SYSTEM } = "kafka",
        api_key = api_key,
        { KRAFKA_CORRELATION_ID } = correlation_id,
    )
}

/// Create a span for consumer group coordination operations.
///
/// # Arguments
///
/// * `group_id` - The consumer group ID
/// * `operation` - The operation (e.g., "join", "sync", "heartbeat", "leave")
///
/// # Returns
///
/// A tracing `Span` configured with group coordination attributes.
#[inline]
pub fn kafka_group_span(group_id: &str, operation: &str) -> Span {
    tracing::span!(
        Level::DEBUG,
        "kafka.group",
        { MESSAGING_SYSTEM } = "kafka",
        { MESSAGING_KAFKA_CONSUMER_GROUP } = group_id,
        operation = operation,
    )
}

/// Create a span for consumer rebalance operations.
///
/// # Arguments
///
/// * `group_id` - The consumer group ID
/// * `event` - The rebalance event type (e.g., "assigned", "revoked", "lost")
/// * `partition_count` - Number of partitions affected
///
/// # Returns
///
/// A tracing `Span` configured with rebalance attributes.
#[inline]
pub fn kafka_rebalance_span(group_id: &str, event: &str, partition_count: usize) -> Span {
    tracing::span!(
        Level::INFO,
        "kafka.rebalance",
        { MESSAGING_SYSTEM } = "kafka",
        { MESSAGING_KAFKA_CONSUMER_GROUP } = group_id,
        event = event,
        partition_count = partition_count,
    )
}

/// Record an error on the current span.
///
/// This function records error information following OpenTelemetry conventions.
///
/// # Arguments
///
/// * `error` - The error to record
#[inline]
pub fn record_error(error: &dyn std::error::Error) {
    let span = Span::current();
    span.record("otel.status_code", "ERROR");
    span.record("error.message", error.to_string().as_str());
}

/// Record an error message on the current span.
///
/// # Arguments
///
/// * `message` - The error message
#[inline]
pub fn record_error_message(message: &str) {
    let span = Span::current();
    span.record("otel.status_code", "ERROR");
    span.record("error.message", message);
}

/// Record success on the current span.
#[inline]
pub fn record_success() {
    let span = Span::current();
    span.record("otel.status_code", "OK");
}

/// Record the number of records in a batch.
///
/// # Arguments
///
/// * `count` - The number of records
#[inline]
pub fn record_batch_count(count: usize) {
    let span = Span::current();
    span.record(MESSAGING_BATCH_MESSAGE_COUNT, count);
}

/// Record the message body size.
///
/// # Arguments
///
/// * `size` - The message body size in bytes
#[inline]
pub fn record_message_size(size: usize) {
    let span = Span::current();
    span.record(MESSAGING_MESSAGE_BODY_SIZE, size);
}

/// Record the offset after a successful produce or fetch.
///
/// # Arguments
///
/// * `offset` - The Kafka offset
#[inline]
pub fn record_offset(offset: i64) {
    let span = Span::current();
    span.record(MESSAGING_KAFKA_OFFSET, offset);
}

/// Record the partition.
///
/// # Arguments
///
/// * `partition` - The partition ID
#[inline]
pub fn record_partition(partition: PartitionId) {
    let span = Span::current();
    span.record(MESSAGING_KAFKA_PARTITION, partition);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Without a tracing subscriber, spans may be disabled.
    // These tests verify the span creation functions don't panic
    // and return valid Span objects.

    #[test]
    fn test_kafka_producer_span() {
        let _span = kafka_producer_span("test-topic", Some(0), Some(b"key"));
        // Also test without partition and key
        let _span2 = kafka_producer_span("test-topic", None, None);
    }

    #[test]
    fn test_kafka_consumer_poll_span() {
        let topics = vec!["topic1".to_string(), "topic2".to_string()];
        let _span = kafka_consumer_poll_span(Some("my-group"), &topics);
        // Also test without group ID
        let _span2 = kafka_consumer_poll_span(None, &topics);
    }

    #[test]
    fn test_kafka_fetch_span() {
        let _span = kafka_fetch_span("test-topic", 0, 100);
    }

    #[test]
    fn test_kafka_commit_span() {
        let _span = kafka_commit_span(Some("my-group"), "test-topic", 0, 100);
        let _span2 = kafka_commit_span(None, "test-topic", 1, 200);
    }

    #[test]
    fn test_kafka_admin_span() {
        let _span = kafka_admin_span("create_topic", Some("new-topic"));
        let _span2 = kafka_admin_span("list_topics", None);
    }

    #[test]
    fn test_kafka_connection_span() {
        let _span = kafka_connection_span("localhost:9092", "connect");
    }

    #[test]
    fn test_kafka_request_span() {
        let _span = kafka_request_span("Produce", 42);
    }

    #[test]
    fn test_kafka_group_span() {
        let _span = kafka_group_span("my-group", "join");
    }

    #[test]
    fn test_kafka_rebalance_span() {
        let _span = kafka_rebalance_span("my-group", "assigned", 3);
    }

    #[test]
    fn test_record_helpers() {
        // These should not panic even with no active span
        record_batch_count(10);
        record_message_size(1024);
        record_offset(12345);
        record_partition(0);
        record_success();
        record_error_message("test error");
    }

    #[test]
    fn test_semantic_conventions() {
        // Verify constants have expected values
        assert_eq!(MESSAGING_SYSTEM, "messaging.system");
        assert_eq!(MESSAGING_DESTINATION, "messaging.destination.name");
        assert_eq!(MESSAGING_OPERATION, "messaging.operation");
        assert_eq!(
            MESSAGING_KAFKA_PARTITION,
            "messaging.kafka.destination.partition"
        );
        assert_eq!(MESSAGING_KAFKA_OFFSET, "messaging.kafka.message.offset");
        assert_eq!(
            MESSAGING_KAFKA_CONSUMER_GROUP,
            "messaging.kafka.consumer.group"
        );
    }
}
