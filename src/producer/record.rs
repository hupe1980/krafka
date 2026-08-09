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
    pub key: Option<Bytes>,
    /// Record value (zero-copy via `Bytes`).
    pub value: Bytes,
    /// Record timestamp (optional, will use current time if not set).
    pub timestamp: Option<Timestamp>,
    /// Record headers.
    pub headers: Vec<(String, Bytes)>,
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
    /// Create a new producer record.
    pub fn new(topic: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            topic: topic.into(),
            partition: None,
            key: None,
            value: value.into(),
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

    /// Set the timestamp.
    pub fn with_timestamp(mut self, timestamp: Timestamp) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Add a header.
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        self.headers.push((key.into(), value.into()));
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

    /// Get the value as a string (if valid UTF-8).
    #[inline]
    pub fn value_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.value).ok()
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
    /// signed_varint(key_len) + key  — exact varint + bytes
    /// signed_varint(val_len) + val  — exact varint + bytes
    /// signed_varint(hdr_count)      — exact varint
    ///   per header: varint(k_len) + k + varint(v_len) + v
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
        let val_bytes = self.value.len();

        let key_varint = match &self.key {
            Some(k) => varint::signed_varint_size(k.len() as i32),
            None => varint::signed_varint_size(-1), // null sentinel
        };
        let val_varint = varint::signed_varint_size(val_bytes as i32);
        let hdr_count_varint = varint::signed_varint_size(self.headers.len() as i32);

        let headers_wire: usize = self
            .headers
            .iter()
            .map(|(k, v)| {
                varint::signed_varint_size(k.len() as i32)
                    + k.len()
                    + varint::signed_varint_size(v.len() as i32)
                    + v.len()
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

        // Value is encoded as KafkaBytes (i32 length prefix)
        if self.value.len() > i32::MAX as usize {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                format!(
                    "record value length {} exceeds protocol limit of {}",
                    self.value.len(),
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
            if value.len() > i32::MAX as usize {
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
        let Self {
            topic,
            partition,
            key,
            value,
            timestamp,
            headers,
            record_name: _,
        } = self;

        RoutedRecordParts {
            topic: Arc::<str>::from(topic),
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

/// Interned topic handle reused across the producer routing path.
pub(crate) type TopicHandle = Arc<str>;

/// Internal payload retained after partition routing strips the topic.
#[derive(Debug, Clone)]
pub(crate) struct RoutedRecord {
    pub key: Option<Bytes>,
    pub value: Bytes,
    pub timestamp: Option<Timestamp>,
    pub headers: Vec<(String, Bytes)>,
}

impl RoutedRecord {
    #[inline]
    pub(crate) fn key_bytes(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    pub(crate) fn append_to_batch_builder(
        &self,
        batch_builder: RecordBatchBuilder,
    ) -> RecordBatchBuilder {
        if self.headers.is_empty() {
            batch_builder.add_record(self.key.clone(), Some(self.value.clone()))
        } else {
            batch_builder.add_record_with_headers(
                self.key.clone(),
                Some(self.value.clone()),
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
        assert_eq!(record.value.as_ref(), b"hello");
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
        assert_eq!(routed.record.value, Bytes::from_static(b"hello"));
        assert_eq!(routed.record.timestamp, Some(1234));
        assert_eq!(routed.record.headers.len(), 1);
        assert_eq!(routed.record.headers[0].0, "h1");
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
