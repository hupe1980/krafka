//! Producer record types.

use std::sync::Arc;

use bytes::Bytes;

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::protocol::{MAX_RECORD_HEADERS, RecordBatchBuilder, validate_topic_name};
use crate::{PartitionId, Timestamp};

/// A record to be sent to Kafka.
#[non_exhaustive]
#[must_use]
#[derive(Debug, Clone)]
pub struct ProducerRecord {
    /// Target topic.
    pub topic: String,
    /// Target partition (optional, will be computed if not set).
    pub partition: Option<PartitionId>,
    /// Record key (optional, zero-copy via `Bytes`).
    ///
    /// `None` is Kafka's *null key*: the record is keyless, so the default
    /// partitioner spreads it instead of hashing. `Some(Bytes::new())` is a
    /// zero-length key, which hashes like any other.
    pub key: Option<Bytes>,
    /// Record value (zero-copy via `Bytes`), or `None` for a **tombstone**.
    ///
    /// `None` encodes Kafka's *null value* (a `-1` length prefix on the wire),
    /// which on a `cleanup.policy=compact` topic marks the key for deletion.
    /// `Some(Bytes::new())` is a zero-length value — an ordinary record that
    /// compaction preserves. See [`tombstone`](Self::tombstone).
    pub value: Option<Bytes>,
    /// Record timestamp (optional, will use current time if not set).
    pub timestamp: Option<Timestamp>,
    /// Record headers.
    ///
    /// Duplicate keys are permitted and preserved in order, matching the Kafka
    /// record format. A `None` value is a *null* header value, which the wire
    /// format distinguishes from a zero-length one.
    pub headers: Vec<(String, Option<Bytes>)>,
    /// Optional type name forwarded to the
    /// [`Serializer`](crate::serdes::Serializer).
    ///
    /// krafka never interprets it. It exists because a serializer often needs
    /// to name the record's *type* as well as its topic — a schema-registry
    /// serializer deriving a subject from a record name, for instance. Leave it
    /// `None` unless your serializer documents that it reads it.
    pub record_name: Option<String>,
}

impl ProducerRecord {
    /// Create a new producer record carrying `value`.
    ///
    /// For a record with a *null* value — a tombstone — use
    /// [`tombstone`](Self::tombstone), or [`without_value`](Self::without_value)
    /// on a record built here.
    pub fn new(topic: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            topic: topic.into(),
            partition: None,
            key: None,
            value: Some(value.into()),
            timestamp: None,
            headers: Vec::new(),
            record_name: None,
        }
    }

    /// Create a **tombstone**: a record with `key` and a null value.
    ///
    /// On a topic configured with `cleanup.policy=compact`, a tombstone marks
    /// the key for deletion. Log compaction then removes every earlier record
    /// for that key, and finally the tombstone itself once
    /// `delete.retention.ms` has elapsed.
    ///
    /// The key is required, because a tombstone is a statement *about a key* —
    /// a null value on a keyless record deletes nothing. Build that with
    /// [`without_value`](Self::without_value) if you need it.
    ///
    /// A configured
    /// [`value_serializer`](crate::producer::ProducerBuilder::value_serializer)
    /// is **not** applied here; see its documentation.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use krafka::producer::ProducerRecord;
    ///
    /// # async fn example(producer: &krafka::producer::Producer) -> krafka::Result<()> {
    /// let record = ProducerRecord::tombstone("users", "user-42")
    ///     .with_header("X-Reason", &b"gdpr-erasure"[..]);
    /// assert!(record.is_tombstone());
    /// producer.send_record(record).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn tombstone(topic: impl Into<String>, key: impl Into<Bytes>) -> Self {
        Self {
            topic: topic.into(),
            partition: None,
            key: Some(key.into()),
            value: None,
            timestamp: None,
            headers: Vec::new(),
            record_name: None,
        }
    }

    /// Set the partition.
    pub fn with_partition(mut self, partition: PartitionId) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Set the key.
    pub fn with_key(mut self, key: impl Into<Bytes>) -> Self {
        self.key = Some(key.into());
        self
    }

    /// Clear the key (set it to `None`).
    pub fn without_key(mut self) -> Self {
        self.key = None;
        self
    }

    /// Set the value.
    pub fn with_value(mut self, value: impl Into<Bytes>) -> Self {
        self.value = Some(value.into());
        self
    }

    /// Clear the value, turning this record into a **tombstone**.
    ///
    /// See [`tombstone`](Self::tombstone) for what that means on a compacted
    /// topic. Note that a tombstone with no key deletes nothing.
    pub fn without_value(mut self) -> Self {
        self.value = None;
        self
    }

    /// Set the timestamp.
    pub fn with_timestamp(mut self, timestamp: Timestamp) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Add a header.
    ///
    /// Duplicate keys are allowed and preserved in insertion order. Use
    /// [`with_null_header`](Self::with_null_header) for a header whose value is
    /// *null* rather than zero-length.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push((key.into(), Some(value.into())));
        self
    }

    /// Add a header with a **null** value.
    ///
    /// The Kafka record format distinguishes a null header value from a
    /// zero-length one, and some ecosystems use a null-valued header as a bare
    /// flag. `with_header(k, Bytes::new())` produces the zero-length form
    /// instead.
    pub fn with_null_header(mut self, key: impl Into<String>) -> Self {
        self.headers.push((key.into(), None));
        self
    }

    /// Set the type name passed to the
    /// [`Serializer`](crate::serdes::Serializer).
    ///
    /// Only needed when the configured serializer reads it — for example a
    /// schema-registry serializer whose subject-name strategy is derived from
    /// the record name rather than the topic.
    pub fn with_record_name(mut self, name: impl Into<String>) -> Self {
        self.record_name = Some(name.into());
        self
    }

    /// Get the key as a string (if valid UTF-8).
    #[inline]
    pub fn key_str(&self) -> Option<&str> {
        self.key.as_ref().and_then(|k| std::str::from_utf8(k).ok())
    }

    /// Get the value as a string (if present and valid UTF-8).
    ///
    /// Returns `None` both for a tombstone and for a non-UTF-8 value; use
    /// [`is_tombstone`](Self::is_tombstone) or inspect
    /// [`value`](Self::value) directly to tell the two apart.
    #[inline]
    pub fn value_str(&self) -> Option<&str> {
        self.value
            .as_ref()
            .and_then(|v| std::str::from_utf8(v).ok())
    }

    /// Returns `true` if this record is a tombstone (a delete marker).
    ///
    /// A tombstone has a key and a null value. A record with neither key nor
    /// value is *not* a tombstone — it has no key to delete. This mirrors
    /// [`ConsumerRecord::is_tombstone`](crate::consumer::ConsumerRecord::is_tombstone)
    /// so a record can be classified identically on both sides of the wire.
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        self.key.is_some() && self.value.is_none()
    }

    /// Get the estimated size in bytes.
    ///
    /// Returns a conservative upper-bound on the wire-encoded size of this
    /// record within a RecordBatch v2 frame.  The estimate is used for both
    /// batch size-gating and memory backpressure; an undercount can cause
    /// batches to exceed `max_request_size` and trigger broker-side
    /// `MESSAGE_TOO_LARGE` errors.
    ///
    /// # Wire layout (RecordBatch v2 per-record)
    ///
    /// ```text
    /// signed_varint(body_size)      — record length prefix (exact)
    /// i8 attributes                 — 1 byte (fixed)
    /// signed_varlong(ts_delta)      — ≤ 5 bytes (conservative for typical batch windows)
    /// signed_varint(off_delta)      — ≤ 2 bytes (covers batches up to ~16 k records)
    /// signed_varint(key_len) + key  — exact varint + bytes; -1 when null
    /// signed_varint(val_len) + val  — exact varint + bytes; -1 for a tombstone
    /// signed_varint(hdr_count)      — exact varint
    ///   per header: varint(k_len) + k + varint(v_len) + v; v_len -1 when null
    /// ```
    ///
    /// An additional per-record batch-overhead allowance is added to amortise
    /// the RecordBatch fixed header (61 bytes) and per-topic produce-request
    /// framing across records.
    #[inline]
    pub fn estimated_size(&self) -> usize {
        use crate::util::varint;

        // Unknowns at this point; conservative fixed estimates:
        //   timestamp_delta ≤ 5 bytes (covers ~67 s at ms resolution — typical batch window)
        //   offset_delta    ≤ 2 bytes (covers batches up to 16383 records)
        const TIMESTAMP_DELTA_BYTES: usize = 5;
        const OFFSET_DELTA_BYTES: usize = 2;

        let key_bytes = self.key.as_ref().map_or(0, |k| k.len());
        let val_bytes = self.value.as_ref().map_or(0, |v| v.len());

        let key_varint = match &self.key {
            Some(k) => varint::signed_varint_size(k.len() as i32),
            None => varint::signed_varint_size(-1), // null sentinel
        };
        let val_varint = match &self.value {
            Some(v) => varint::signed_varint_size(v.len() as i32),
            None => varint::signed_varint_size(-1), // null sentinel (tombstone)
        };
        let hdr_count_varint = varint::signed_varint_size(self.headers.len() as i32);

        let headers_wire: usize = self
            .headers
            .iter()
            .map(|(k, v)| {
                let value_wire = match v {
                    Some(v) => varint::signed_varint_size(v.len() as i32) + v.len(),
                    None => varint::signed_varint_size(-1), // null sentinel
                };
                varint::signed_varint_size(k.len() as i32) + k.len() + value_wire
            })
            .sum();

        let body_size = 1 // attributes byte
            + TIMESTAMP_DELTA_BYTES
            + OFFSET_DELTA_BYTES
            + key_varint
            + key_bytes
            + val_varint
            + val_bytes
            + hdr_count_varint
            + headers_wire;

        // Record framing: body_size is itself encoded as a signed varint prefix.
        let framing = varint::signed_varint_size(body_size as i32);

        // Amortised batch-level overhead: RecordBatch fixed header (61 bytes),
        // produce-request topic/partition framing (~20 bytes), topic String heap.
        let batch_overhead = self.topic.len() + 64;

        framing + body_size + batch_overhead
    }

    /// Validate that this record's fields do not exceed Kafka wire-format limits.
    ///
    /// Checks:
    /// - Key length fits in `i32` (Kafka bytes encoding limit of 2 GiB)
    /// - Value length fits in `i32`
    /// - Each header key fits in `i32` (record batch v2 uses varint/i32 length prefix)
    /// - Each header value fits in `i32`
    /// - Topic name fits in `i16` (Kafka string encoding limit of 32 KiB)
    pub fn validate(&self) -> Result<()> {
        // Topic name must be non-empty and fit the KafkaString (i16) length prefix.
        // Shared with admin-path ingress so the error message is stable across
        // the client.
        validate_topic_name(&self.topic)?;

        // Key is encoded as KafkaBytes (i32 length prefix)
        if let Some(ref key) = self.key
            && key.len() > i32::MAX as usize
        {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                format!(
                    "record key length {} exceeds protocol limit of {}",
                    key.len(),
                    i32::MAX
                ),
            ));
        }

        // Value is encoded as KafkaBytes (i32 length prefix). A tombstone
        // (`None`) is encoded as the -1 sentinel and has no length to check.
        if let Some(ref value) = self.value
            && value.len() > i32::MAX as usize
        {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                format!(
                    "record value length {} exceeds protocol limit of {}",
                    value.len(),
                    i32::MAX
                ),
            ));
        }

        // Header keys and values are encoded with varint i32 length prefixes
        // in the record batch v2 format. Limit header count to prevent
        // excessively large batches from bypassing max_request_size.
        if self.headers.len() > MAX_RECORD_HEADERS {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                format!(
                    "record has {} headers, exceeding limit of {MAX_RECORD_HEADERS}",
                    self.headers.len()
                ),
            ));
        }
        for (i, (key, value)) in self.headers.iter().enumerate() {
            if key.len() > i32::MAX as usize {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::InvalidLength,
                    format!(
                        "header[{}] key length {} exceeds protocol limit of {}",
                        i,
                        key.len(),
                        i32::MAX
                    ),
                ));
            }
            if let Some(value) = value
                && value.len() > i32::MAX as usize
            {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::InvalidLength,
                    format!(
                        "header[{}] value length {} exceeds protocol limit of {}",
                        i,
                        value.len(),
                        i32::MAX
                    ),
                ));
            }
        }

        Ok(())
    }

    /// Split the public record into an interned topic handle and routed payload.
    pub(crate) fn into_routed_parts(self) -> RoutedRecordParts {
        let topic = Arc::<str>::from(self.topic.as_str());
        self.into_routed_parts_with_topic(topic)
    }

    /// As [`into_routed_parts`](Self::into_routed_parts), reusing a topic
    /// handle the caller already holds.
    ///
    /// The send path interns the topic once, when the interceptor chain has
    /// finished with the record, and both the routing and the failure-reporting
    /// sides share that one handle.
    pub(crate) fn into_routed_parts_with_topic(self, topic: TopicHandle) -> RoutedRecordParts {
        let Self {
            topic: _,
            partition,
            key,
            value,
            timestamp,
            headers,
            record_name: _,
        } = self;

        RoutedRecordParts {
            topic,
            partition,
            record: RoutedRecord {
                key,
                value,
                timestamp,
                headers,
            },
        }
    }
}

/// The record headers handed to
/// [`ProducerInterceptor::on_acknowledgement`](crate::interceptor::ProducerInterceptor::on_acknowledgement).
///
/// Named rather than spelled out for the same reason as
/// [`CommitOffsets`](crate::interceptor::CommitOffsets): a trait signature that
/// wrote `&[(String, Option<Bytes>)]` inline would make every implementor
/// repeat it, and would pin the representation in place the first time someone
/// did.
///
/// `Vec<(String, Option<Bytes>)>` — the type of
/// [`ProducerRecord::headers`] — derefs to this, so `&record.headers` is
/// already a `&RecordHeaders`.
pub type RecordHeaders = [(String, Option<Bytes>)];

/// Interned topic handle reused across the producer routing path.
pub(crate) type TopicHandle = Arc<str>;

/// Internal payload retained after partition routing strips the topic.
#[derive(Debug, Clone)]
pub(crate) struct RoutedRecord {
    pub key: Option<Bytes>,
    /// `None` is the Kafka null value — a tombstone.
    pub value: Option<Bytes>,
    pub timestamp: Option<Timestamp>,
    pub headers: Vec<(String, Option<Bytes>)>,
}

impl RoutedRecord {
    /// A record with no fields set.
    ///
    /// Only for the unreachable arm where a channel hands back something other
    /// than the message it was given: the terminal callback still has to fire,
    /// and it needs *some* header slice to report.
    pub(crate) fn empty() -> Self {
        Self {
            key: None,
            value: None,
            timestamp: None,
            headers: Vec::new(),
        }
    }

    #[inline]
    pub(crate) fn key_bytes(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    pub(crate) fn append_to_batch_builder(
        &self,
        batch_builder: RecordBatchBuilder,
    ) -> RecordBatchBuilder {
        if self.headers.is_empty() {
            batch_builder.add_record(self.key.clone(), self.value.clone())
        } else {
            batch_builder.add_record_with_headers(
                self.key.clone(),
                self.value.clone(),
                self.headers.clone(),
            )
        }
    }
}

/// Internal representation of a routed record after topic extraction.
pub(crate) struct RoutedRecordParts {
    pub topic: TopicHandle,
    pub partition: Option<PartitionId>,
    pub record: RoutedRecord,
}

/// How the broker confirmed (or did not confirm) a record.
///
/// # Why this is not derived from `offset`
///
/// `offset == -1` is ambiguous: it is produced both when the broker
/// deduplicated an idempotent batch (**the data is durably in Kafka**) and when
/// `acks = 0` was configured (**the broker never confirmed anything**). A
/// caller that treated `-1` as "deduplicated" would get the exact opposite of
/// the durability guarantee it believed it had. This enum is the explicit
/// discriminator; `offset` is only meaningful for
/// [`DeliveryConfirmation::Offset`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryConfirmation {
    /// The broker acknowledged the record and returned a real log offset.
    Offset,
    /// The broker answered `DuplicateSequenceNumber`: an earlier attempt of
    /// this idempotent batch was already committed. The data **is** in Kafka,
    /// but the original offset is no longer recoverable.
    Deduplicated,
    /// `acks = 0` — the request was written to the socket and the broker sends
    /// no response. There is **no** durability guarantee whatsoever; the record
    /// may never have been stored.
    Unacknowledged,
    /// The send failed permanently. Present only on the metadata handed to
    /// [`ProducerInterceptor::on_acknowledgement`](crate::interceptor::ProducerInterceptor::on_acknowledgement)
    /// alongside the error; it is never returned as `Ok`.
    Failed,
}

/// Partition value reported when a record failed before it was routed.
///
/// A record rejected by serialization, validation or topic lookup never
/// reaches the partitioner, so the
/// [`RecordMetadata`] handed to
/// [`ProducerInterceptor::on_acknowledgement`](crate::interceptor::ProducerInterceptor::on_acknowledgement)
/// carries this instead of a real partition. Mirrors the Java client's
/// `RecordMetadata.UNKNOWN_PARTITION`.
pub const UNKNOWN_PARTITION: PartitionId = -1;

/// Timestamp reported when the broker never assigned one.
///
/// Mirrors the Java client's `RecordBatch.NO_TIMESTAMP`. Only meaningful
/// alongside [`DeliveryConfirmation::Failed`] or an `acks = 0` send — a
/// successful append always carries a real timestamp.
pub const NO_TIMESTAMP: Timestamp = -1;

/// Metadata returned after successfully sending a record.
///
/// Always check [`delivery`](Self::delivery) (or the
/// [`is_success`](Self::is_success) / [`is_deduplicated`](Self::is_deduplicated)
/// / [`is_unacknowledged`](Self::is_unacknowledged) helpers) before relying on
/// [`offset`](Self::offset): a `-1` offset alone does not tell you whether the
/// record is durably stored.
#[non_exhaustive]
#[must_use = "contains the result of a send operation"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordMetadata {
    /// Topic the record was sent to.
    pub topic: String,
    /// Partition the record was sent to.
    pub partition: PartitionId,
    /// Log offset of the committed record, or `-1` when no offset is available.
    ///
    /// Only meaningful when [`delivery`](Self::delivery) is
    /// [`DeliveryConfirmation::Offset`].
    pub offset: i64,
    /// Broker-assigned timestamp of the record, or `-1`/`0` when unavailable.
    pub timestamp: Timestamp,
    /// What the broker actually confirmed. See [`DeliveryConfirmation`].
    pub delivery: DeliveryConfirmation,
}

impl RecordMetadata {
    /// Terminal metadata for a record that failed and therefore has no offset.
    ///
    /// The single constructor for the failure case, so every path that reports
    /// a terminal failure to
    /// [`ProducerInterceptor::on_acknowledgement`](crate::interceptor::ProducerInterceptor::on_acknowledgement)
    /// — pre-enqueue rejection, batch failure, dead-letter hand-off — describes
    /// it identically. Pass [`UNKNOWN_PARTITION`] when the record failed before
    /// it was routed.
    pub(crate) fn failed(topic: String, partition: PartitionId) -> Self {
        Self {
            topic,
            partition,
            offset: -1,
            timestamp: NO_TIMESTAMP,
            delivery: DeliveryConfirmation::Failed,
        }
    }

    /// Returns `true` if the record was committed with a known log offset.
    ///
    /// Deduplicated records return `false` even though their data *is* in
    /// Kafka — use [`is_deduplicated`](Self::is_deduplicated) to tell those
    /// apart, or match on [`delivery`](Self::delivery) directly.
    #[inline]
    pub fn is_success(&self) -> bool {
        self.delivery == DeliveryConfirmation::Offset
    }

    /// Returns `true` when the broker deduplicated this record.
    ///
    /// An idempotent producer receives `DuplicateSequenceNumber` when the
    /// broker has already committed the batch from an earlier attempt. The data
    /// **is** in Kafka, but the original log offset is not available.
    ///
    /// This is now driven by an explicit discriminator rather than
    /// `offset == -1`, which also matched the `acks = 0` path and therefore
    /// reported "deduplicated" for records with no durability guarantee at all.
    #[inline]
    pub fn is_deduplicated(&self) -> bool {
        self.delivery == DeliveryConfirmation::Deduplicated
    }

    /// Returns `true` when the producer is configured with `acks = 0` and the
    /// broker never confirmed the write.
    ///
    /// There is no durability guarantee for such a record.
    #[inline]
    pub fn is_unacknowledged(&self) -> bool {
        self.delivery == DeliveryConfirmation::Unacknowledged
    }

    /// Returns `true` when the record is durably stored in Kafka, whether or
    /// not its offset is known.
    ///
    /// True for [`DeliveryConfirmation::Offset`] and
    /// [`DeliveryConfirmation::Deduplicated`].
    #[inline]
    pub fn is_persisted(&self) -> bool {
        matches!(
            self.delivery,
            DeliveryConfirmation::Offset | DeliveryConfirmation::Deduplicated
        )
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_record_new() {
        let record = ProducerRecord::new("test-topic", b"hello".to_vec());
        assert_eq!(record.topic, "test-topic");
        assert_eq!(record.value.as_deref(), Some(&b"hello"[..]));
        assert!(record.key.is_none());
        assert!(record.partition.is_none());
    }

    #[test]
    fn test_producer_record_with_key() {
        let record =
            ProducerRecord::new("test-topic", b"hello".to_vec()).with_key(b"my-key".to_vec());

        assert_eq!(record.key, Some(Bytes::from_static(b"my-key")));
        assert_eq!(record.key_str(), Some("my-key"));
    }

    #[test]
    fn test_producer_record_with_partition() {
        let record = ProducerRecord::new("test-topic", b"hello".to_vec()).with_partition(5);

        assert_eq!(record.partition, Some(5));
    }

    #[test]
    fn test_producer_record_with_headers() {
        let record = ProducerRecord::new("test-topic", b"hello".to_vec())
            .with_header("h1", b"v1".to_vec())
            .with_header("h2", b"v2".to_vec());

        assert_eq!(record.headers.len(), 2);
        assert_eq!(record.headers[0].0, "h1");
        assert_eq!(record.headers[1].0, "h2");
    }

    #[test]
    fn test_producer_record_estimated_size() {
        let record =
            ProducerRecord::new("test-topic", b"hello world".to_vec()).with_key(b"key".to_vec());

        let size = record.estimated_size();
        // Must include at least key + value bytes, the varint framing overhead,
        // and the batch-level header overhead.
        assert!(size > 3 + 11 + 8, "estimated_size={size} too small");

        // Must not be unreasonably large (< 512 for this tiny record).
        assert!(size < 512, "estimated_size={size} unexpectedly large");

        // A record with no key should still estimate correctly.
        let no_key = ProducerRecord::new("test-topic", b"hello world".to_vec());
        let no_key_size = no_key.estimated_size();
        // No key → slightly smaller than with key (only null sentinel varint, no key bytes).
        assert!(no_key_size < size, "no-key estimate should be smaller");

        // A record with headers should be larger than one without.
        let with_headers = ProducerRecord::new("test-topic", b"hello world".to_vec())
            .with_header("h1", b"v1".to_vec())
            .with_header("h2", b"v2".to_vec());
        assert!(
            with_headers.estimated_size() > no_key_size,
            "headers should increase estimate"
        );
    }

    #[test]
    fn test_producer_record_into_routed_parts() {
        let record = ProducerRecord::new("test-topic", b"hello".to_vec())
            .with_partition(2)
            .with_key(b"key".to_vec())
            .with_timestamp(1234)
            .with_header("h1", b"v1".to_vec());

        let routed = record.into_routed_parts();

        assert_eq!(routed.topic.as_ref(), "test-topic");
        assert_eq!(routed.partition, Some(2));
        assert_eq!(routed.record.key, Some(Bytes::from_static(b"key")));
        assert_eq!(routed.record.value, Some(Bytes::from_static(b"hello")));
        assert_eq!(routed.record.timestamp, Some(1234));
        assert_eq!(routed.record.headers.len(), 1);
        assert_eq!(routed.record.headers[0].0, "h1");
    }

    // ── Tombstones: null value is not empty value ────────────────

    /// `tombstone()` produces a keyed record with a null value, which is what
    /// log compaction reads as "delete this key".
    #[test]
    fn tombstone_has_key_and_null_value() {
        let record = ProducerRecord::tombstone("users", "user-42");

        assert_eq!(record.topic, "users");
        assert_eq!(record.key, Some(Bytes::from_static(b"user-42")));
        assert_eq!(record.value, None);
        assert!(record.is_tombstone());
        assert_eq!(record.value_str(), None);
    }

    /// A zero-length value is an ordinary record that compaction *keeps*;
    /// only a null value deletes. Collapsing the two loses the difference.
    #[test]
    fn empty_value_is_not_a_tombstone() {
        let empty = ProducerRecord::new("users", Bytes::new()).with_key("user-42");

        assert_eq!(empty.value, Some(Bytes::new()));
        assert!(!empty.is_tombstone());
        assert_ne!(
            empty.value,
            ProducerRecord::tombstone("users", "user-42").value
        );
    }

    /// A null value without a key deletes nothing, so it is not a tombstone —
    /// matching `ConsumerRecord::is_tombstone`, so a record classifies the same
    /// on both sides of the wire.
    #[test]
    fn keyless_null_value_is_not_a_tombstone() {
        let record = ProducerRecord::new("t", b"v".to_vec()).without_value();

        assert_eq!(record.value, None);
        assert!(!record.is_tombstone(), "no key means nothing to delete");
    }

    #[test]
    fn with_value_and_without_value_round_trip() {
        let record = ProducerRecord::tombstone("t", "k").with_value(b"back".to_vec());
        assert_eq!(record.value, Some(Bytes::from_static(b"back")));
        assert!(!record.is_tombstone());

        let record = record.without_value();
        assert_eq!(record.value, None);
        assert!(record.is_tombstone());
    }

    /// A null value costs the `-1` sentinel varint and no payload bytes, so a
    /// tombstone must estimate *smaller* than the same record with an empty
    /// value — which occupies a `0` varint plus nothing. Both are one byte, so
    /// the meaningful assertion is that neither over-counts and both stay
    /// under the batch budget.
    #[test]
    fn tombstone_estimated_size_accounts_for_the_null_sentinel() {
        let tombstone = ProducerRecord::tombstone("test-topic", "key");
        let valued = ProducerRecord::new("test-topic", b"hello world".to_vec()).with_key("key");

        assert!(
            tombstone.estimated_size() < valued.estimated_size(),
            "a tombstone carries no value bytes"
        );
        // The estimate must still cover the fixed framing and the key.
        assert!(tombstone.estimated_size() > 3);
    }

    /// A null header value is also a `-1` sentinel, not a zero-length payload.
    #[test]
    fn null_header_value_is_distinct_from_empty() {
        let record = ProducerRecord::new("t", b"v".to_vec())
            .with_null_header("flag")
            .with_header("empty", Bytes::new());

        assert_eq!(record.headers[0], ("flag".to_string(), None));
        assert_eq!(record.headers[1], ("empty".to_string(), Some(Bytes::new())));
        assert_ne!(record.headers[0].1, record.headers[1].1);
    }

    /// A tombstone must survive validation — it is a legal record, and an
    /// over-eager length check on an absent value would reject it.
    #[test]
    fn tombstone_validates() {
        ProducerRecord::tombstone("t", "k")
            .with_null_header("h")
            .validate()
            .expect("a tombstone is a legal record");
    }

    /// Routing must not resurrect the null: the value that reaches the batch
    /// builder is still `None`.
    #[test]
    fn tombstone_survives_routing() {
        let routed = ProducerRecord::tombstone("t", "k")
            .with_null_header("h")
            .into_routed_parts();

        assert_eq!(routed.record.value, None);
        assert_eq!(routed.record.key, Some(Bytes::from_static(b"k")));
        assert_eq!(routed.record.headers[0].1, None);
    }

    /// The end of the produce path: a routed tombstone must encode as a `-1`
    /// value length and decode back to `None`, not to an empty buffer.
    #[test]
    fn tombstone_encodes_as_null_on_the_wire() {
        use crate::protocol::{RecordBatch, RecordBatchBuilder};

        let routed = ProducerRecord::tombstone("t", "k")
            .with_null_header("flag")
            .with_header("kept", b"1".to_vec())
            .into_routed_parts();

        let builder = RecordBatchBuilder::new();
        let batch = routed.record.append_to_batch_builder(builder).build();
        let mut encoded = batch.encode().expect("batch should encode");

        let decoded = RecordBatch::decode(&mut encoded).expect("batch should decode");
        let record = &decoded.records[0];

        assert_eq!(record.key, Some(Bytes::from_static(b"k")));
        assert_eq!(record.value, None, "the tombstone must stay null");
        assert_eq!(record.headers[0].value, None, "null header stays null");
        assert_eq!(record.headers[1].value, Some(Bytes::from_static(b"1")));
    }

    /// The negative control for the test above: an empty value must *not*
    /// decode as null, or the two would be indistinguishable on the wire and
    /// compaction would delete keys their producer meant to keep.
    #[test]
    fn empty_value_encodes_as_zero_length_not_null() {
        use crate::protocol::{RecordBatch, RecordBatchBuilder};

        let routed = ProducerRecord::new("t", Bytes::new())
            .with_key("k")
            .into_routed_parts();

        let builder = RecordBatchBuilder::new();
        let batch = routed.record.append_to_batch_builder(builder).build();
        let mut encoded = batch.encode().expect("batch should encode");

        let decoded = RecordBatch::decode(&mut encoded).expect("batch should decode");
        assert_eq!(decoded.records[0].value, Some(Bytes::new()));
    }

    #[test]
    fn test_record_metadata() {
        let metadata = RecordMetadata {
            topic: "test".to_string(),
            partition: 0,
            offset: 42,
            timestamp: 1234567890000,
            delivery: DeliveryConfirmation::Offset,
        };

        assert!(metadata.is_success());
        assert!(metadata.is_persisted());
        assert!(!metadata.is_deduplicated());
        assert!(!metadata.is_unacknowledged());
        assert_eq!(metadata.offset, 42);
    }

    // ── `offset == -1` is not a durability discriminator ──────────

    fn meta_with(delivery: DeliveryConfirmation, offset: i64) -> RecordMetadata {
        RecordMetadata {
            topic: "t".to_string(),
            partition: 0,
            offset,
            timestamp: -1,
            delivery,
        }
    }

    /// A deduplicated record carries no offset but **is** durably stored.
    #[test]
    fn test_deduplicated_metadata_is_persisted_without_offset() {
        let m = meta_with(DeliveryConfirmation::Deduplicated, -1);
        assert!(m.is_deduplicated());
        assert!(m.is_persisted(), "deduplicated data is in Kafka");
        assert!(!m.is_success(), "no log offset is available");
        assert!(!m.is_unacknowledged());
    }

    /// The `acks = 0` path also returns `offset == -1`, but has **no**
    /// durability guarantee. Under the old `offset == -1` definition it was
    /// reported as deduplicated — the exact opposite of the truth.
    #[test]
    fn test_acks_none_metadata_is_not_reported_as_deduplicated() {
        let m = meta_with(DeliveryConfirmation::Unacknowledged, -1);
        assert!(m.is_unacknowledged());
        assert!(
            !m.is_deduplicated(),
            "acks=0 must never be reported as broker-deduplicated"
        );
        assert!(
            !m.is_persisted(),
            "acks=0 gives no durability guarantee whatsoever"
        );
        assert!(!m.is_success());
    }

    /// Both `-1` cases are distinguishable despite the identical offset.
    #[test]
    fn test_minus_one_offset_is_disambiguated_by_delivery() {
        let dedup = meta_with(DeliveryConfirmation::Deduplicated, -1);
        let unacked = meta_with(DeliveryConfirmation::Unacknowledged, -1);
        assert_eq!(dedup.offset, unacked.offset);
        assert_ne!(dedup.delivery, unacked.delivery);
        assert_ne!(dedup.is_persisted(), unacked.is_persisted());
    }

    #[test]
    fn test_failed_metadata_is_neither_persisted_nor_successful() {
        let m = meta_with(DeliveryConfirmation::Failed, -1);
        assert!(!m.is_success());
        assert!(!m.is_persisted());
        assert!(!m.is_deduplicated());
        assert!(!m.is_unacknowledged());
    }

    #[test]
    fn test_validate_valid_record() {
        let record = ProducerRecord::new("topic", b"value".to_vec())
            .with_key(b"key".to_vec())
            .with_header("h1", b"v1".to_vec());
        assert!(record.validate().is_ok());
    }

    #[test]
    fn test_validate_rejects_oversized_topic() {
        let record = ProducerRecord::new("x".repeat(i16::MAX as usize + 1), b"v".to_vec());
        let err = record.validate().unwrap_err().to_string();
        assert!(err.contains("topic name length"), "unexpected: {err}");
    }

    #[test]
    fn test_validate_accepts_header_key_within_i32_limit() {
        // Header keys use varint i32 length prefix in record batch v2,
        // so i16::MAX + 1 must be accepted (previously rejected).
        let record = ProducerRecord::new("topic", b"v".to_vec())
            .with_header("x".repeat(i16::MAX as usize + 1), b"v".to_vec());
        assert!(record.validate().is_ok());
    }

    #[test]
    fn test_validate_accepts_max_valid_sizes() {
        // Topic name max is 249 bytes (Kafka protocol limit).
        let record = ProducerRecord::new("a".repeat(249), b"v".to_vec());
        assert!(record.validate().is_ok());
    }

    #[test]
    fn test_without_key_clears_key() {
        let record = ProducerRecord::new("topic", b"value".to_vec())
            .with_key("my-key")
            .without_key();
        assert!(record.key.is_none());
    }

    #[test]
    fn test_validate_rejects_empty_topic() {
        let record = ProducerRecord::new("", b"value".to_vec());
        let err = record.validate().unwrap_err().to_string();
        assert!(err.contains("empty"), "unexpected: {err}");
    }

    #[test]
    fn test_record_metadata_equality() {
        let a = RecordMetadata {
            topic: "t".to_string(),
            partition: 0,
            offset: 1,
            timestamp: 100,
            delivery: DeliveryConfirmation::Offset,
        };
        let b = RecordMetadata {
            topic: "t".to_string(),
            partition: 0,
            offset: 1,
            timestamp: 100,
            delivery: DeliveryConfirmation::Offset,
        };
        assert_eq!(a, b);
    }
}
