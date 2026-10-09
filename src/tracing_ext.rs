//! The spans the clients emit, on OpenTelemetry messaging semantic
//! conventions [`OTEL_SEMCONV_VERSION`].
//!
//! Every span goes through `tracing`. The name the conventions want,
//! `{operation} {destination}`, and the span kind travel in the `otel.name`
//! and `otel.kind` fields, which a `tracing` → OpenTelemetry bridge reads.
//! Span macros evaluate no field when no subscriber is interested, so a
//! process without one pays a callsite check per operation and nothing more.
//!
//! No span records record keys or values, or anything derived from a key
//! but its size.

use std::borrow::Cow;

use tracing::Span;

use crate::error::KrafkaError;

/// The OpenTelemetry semantic-conventions version krafka's span names and
/// attributes follow. The messaging conventions are still marked
/// *Development*; krafka moves to a newer version deliberately, as a
/// breaking change.
pub const OTEL_SEMCONV_VERSION: &str = "1.44.0";

/// One `send` span per record, from `send`/`enqueue` to the record's outcome.
/// The partition and offset are recorded as they become known.
#[inline]
pub(crate) fn send_span(topic: &str, client_id: &str, key: Option<&[u8]>, tombstone: bool) -> Span {
    tracing::info_span!(
        target: "krafka::producer",
        "send",
        otel.name = %format_args!("send {topic}"),
        otel.kind = "producer",
        otel.status_code = tracing::field::Empty,
        messaging.system = "kafka",
        messaging.operation.name = "send",
        messaging.operation.type = "send",
        messaging.destination.name = topic,
        messaging.destination.partition.id = tracing::field::Empty,
        messaging.kafka.offset = tracing::field::Empty,
        messaging.kafka.message.tombstone = tombstone.then_some(true),
        messaging.client.id = client_id,
        krafka.message.key.size = key.map(|k| k.len() as u64),
        error.type = tracing::field::Empty,
    )
}

/// One `poll` span per `poll`/`recv` of a consumer or share consumer. Name
/// it after its topic with [`record_destination`].
#[inline]
pub(crate) fn poll_span(group: Option<&str>, client_id: &str) -> Span {
    tracing::info_span!(
        target: "krafka::consumer",
        "poll",
        otel.name = "poll",
        otel.kind = "client",
        otel.status_code = tracing::field::Empty,
        messaging.system = "kafka",
        messaging.operation.name = "poll",
        messaging.operation.type = "receive",
        messaging.destination.name = tracing::field::Empty,
        messaging.consumer.group.name = group,
        messaging.client.id = client_id,
        messaging.batch.message_count = tracing::field::Empty,
        error.type = tracing::field::Empty,
    )
}

/// One `commit` span per offset or acknowledgement commit. Name it after its
/// topic with [`record_destination`].
#[inline]
pub(crate) fn commit_span(group: Option<&str>, client_id: &str) -> Span {
    tracing::info_span!(
        target: "krafka::consumer",
        "commit",
        otel.name = "commit",
        otel.kind = "client",
        otel.status_code = tracing::field::Empty,
        messaging.system = "kafka",
        messaging.operation.name = "commit",
        messaging.operation.type = "settle",
        messaging.destination.name = tracing::field::Empty,
        messaging.consumer.group.name = group,
        messaging.client.id = client_id,
        error.type = tracing::field::Empty,
    )
}

/// One `rebalance` span per applied assignment change, classic or KIP-848.
/// Not a semantic-conventions operation: internal kind, `krafka.*` fields.
#[inline]
pub(crate) fn rebalance_span(group: &str, protocol: &str, generation: Option<i32>) -> Span {
    tracing::info_span!(
        target: "krafka::consumer",
        "rebalance",
        otel.name = %format_args!("rebalance {group}"),
        otel.kind = "internal",
        messaging.system = "kafka",
        messaging.consumer.group.name = group,
        krafka.rebalance.protocol = protocol,
        krafka.rebalance.generation = generation,
        krafka.rebalance.assigned = tracing::field::Empty,
        krafka.rebalance.revoked = tracing::field::Empty,
        krafka.rebalance.partitions = tracing::field::Empty,
    )
}

/// Name a `poll` or `commit` span after `topics` when there is exactly one;
/// with several the name stays the bare operation. Callers skip computing
/// `topics` for a span that [`is_disabled`](Span::is_disabled).
pub(crate) fn record_destination<'a>(
    span: &Span,
    operation: &'static str,
    topics: impl IntoIterator<Item = &'a str>,
) {
    let mut topics = topics.into_iter();
    if let (Some(topic), None) = (topics.next(), topics.next()) {
        span.record(
            "otel.name",
            tracing::field::display(OperationName(operation, Some(topic))),
        );
        span.record("messaging.destination.name", topic);
    }
}

/// `{operation} {topic}`, or the operation alone without a single topic.
struct OperationName<'a>(&'static str, Option<&'a str>);

impl std::fmt::Display for OperationName<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.1 {
            Some(topic) => write!(f, "{} {topic}", self.0),
            None => f.write_str(self.0),
        }
    }
}

/// Close out a `poll` span: the number of records returned, or the error.
pub(crate) fn record_poll_outcome(span: &Span, outcome: std::result::Result<usize, &KrafkaError>) {
    match outcome {
        Ok(count) => {
            span.record("messaging.batch.message_count", count as u64);
        }
        Err(error) => record_error(span, error),
    }
}

/// Mark `span` failed with `error`'s type.
pub(crate) fn record_error(span: &Span, error: &KrafkaError) {
    if !span.is_disabled() {
        span.record("error.type", error_type(error).as_ref());
        span.record("otel.status_code", "error");
    }
}

/// The `error.type` of `error`: the Kafka error-code name where the broker
/// sent one, else a short name of krafka's error kind.
pub(crate) fn error_type(error: &KrafkaError) -> Cow<'static, str> {
    match error {
        KrafkaError::Broker { code, .. } => Cow::Owned(screaming_snake(&format!("{code:?}"))),
        KrafkaError::OutOfOrderSequence { .. } => "OUT_OF_ORDER_SEQUENCE_NUMBER".into(),
        KrafkaError::UnknownTopic { .. } => "UNKNOWN_TOPIC_OR_PARTITION".into(),
        KrafkaError::Network(_) => "network".into(),
        KrafkaError::Protocol { .. } => "protocol".into(),
        KrafkaError::Auth { .. } => "auth".into(),
        KrafkaError::Timeout { .. } => "timeout".into(),
        KrafkaError::DeliveryTimeout { .. } => "delivery_timeout".into(),
        KrafkaError::Config { .. } => "config".into(),
        KrafkaError::Compression { .. } => "compression".into(),
        KrafkaError::Serialization { .. } => "serialization".into(),
        KrafkaError::RecordDeserialization { .. } => "record_deserialization".into(),
        KrafkaError::Closed { .. } => "closed".into(),
        KrafkaError::Wakeup => "wakeup".into(),
        KrafkaError::Fenced { .. } => "fenced".into(),
        KrafkaError::TransactionAbortable { .. } => "transaction_abortable".into(),
        KrafkaError::NoOffset { .. } => "no_offset".into(),
        KrafkaError::IllegalState { .. } => "illegal_state".into(),
    }
}

/// `NotEnoughReplicas` → `NOT_ENOUGH_REPLICAS`; `Unknown(42)` → `UNKNOWN`.
fn screaming_snake(camel: &str) -> String {
    let name = camel.split('(').next().unwrap_or(camel);
    let mut out = String::with_capacity(name.len() + 8);
    for (i, ch) in name.chars().enumerate() {
        if ch.is_ascii_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(ch.to_ascii_uppercase());
    }
    out
}

#[cfg(all(test, feature = "test-broker"))]
mod broker_tests;

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::error::ErrorCode;

    #[test]
    fn broker_errors_are_typed_by_their_kafka_name() {
        let error = KrafkaError::broker(ErrorCode::NotEnoughReplicas, "x");
        assert_eq!(error_type(&error), "NOT_ENOUGH_REPLICAS");
        let error = KrafkaError::broker(ErrorCode::UnknownTopicOrPartition, "x");
        assert_eq!(error_type(&error), "UNKNOWN_TOPIC_OR_PARTITION");
        assert_eq!(error_type(&KrafkaError::closed("x")), "closed");
    }

    #[test]
    fn the_operation_name_omits_a_missing_destination() {
        assert_eq!(
            OperationName("poll", Some("orders")).to_string(),
            "poll orders"
        );
        assert_eq!(OperationName("poll", None).to_string(), "poll");
    }
}
