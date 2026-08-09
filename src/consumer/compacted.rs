//! Key→value table for log-compacted Kafka topics.
//!
//! The primary type is [`CompactedTable`] — a standalone, Kafka-agnostic
//! data structure that maintains an in-memory key→value snapshot from a
//! stream of [`ConsumerRecord`]s. It handles tombstones (records with null
//! values) automatically and tracks changes via [`TableChange`].
//!
//! Because `CompactedTable` is decoupled from the consumer, it composes
//! with any [`Consumer`] — group-coordinated, standalone, or manually
//! assigned:
//!
//! ```rust,ignore
//! use krafka::consumer::{Consumer, CompactedTable};
//! use std::time::Duration;
//!
//! let consumer = Consumer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .group_id("my-group")
//!     .build()
//!     .await?;
//! consumer.subscribe(&["user-profiles"]).await?;
//!
//! let mut table = CompactedTable::new();
//! loop {
//!     let records = consumer.poll(Duration::from_secs(1)).await?;
//!     let changes = table.apply(&records);
//!     for change in &changes {
//!         println!("{:?}", change);
//!     }
//! }
//! ```
//!
//! For the common case of scanning an entire compacted topic from the
//! beginning, [`CompactedTopicConsumer`] bundles a [`Consumer`] and
//! [`CompactedTable`] together with built-in caught-up detection:
//!
//! ```rust,ignore
//! use krafka::consumer::{CompactedTopicConsumer, Consumer};
//! use std::time::Duration;
//!
//! let mut ctc = CompactedTopicConsumer::from_consumer_builder(
//!     Consumer::builder().bootstrap_servers("localhost:9092"),
//!     "user-profiles",
//! )
//! .await?;
//!
//! ctc.scan(Duration::from_secs(1)).await?;
//!
//! if let Some(value) = ctc.table().get(b"user-123") {
//!     println!("User: {:?}", value);
//! }
//! ```

use ahash::AHashMap as HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::Mutex;
use tracing::{debug, info};

use super::record::ConsumerRecord;
use super::{
    AutoOffsetReset, Consumer, ConsumerBuilder, ConsumerRebalanceListener, IsolationLevel,
    TopicPartition,
};
use crate::error::{KrafkaError, Result};
use crate::{Offset, PartitionId, Timestamp};

/// A single entry in a [`CompactedTable`], carrying the value together with
/// the provenance metadata (offset and broker timestamp) of the most recent
/// record that wrote it.
///
/// Accessing the timestamp lets callers implement freshness policies such as
/// "reject state older than 24 h" without coupling to a separate metadata
/// store.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactedEntry {
    /// The current value for the key.
    pub value: Bytes,
    /// Broker-assigned append timestamp (milliseconds since epoch) of the
    /// record that last wrote this key.  Matches the `timestamp` field of
    /// the originating [`ConsumerRecord`].
    pub timestamp_ms: Timestamp,
    /// Log offset of the record that last wrote this key.
    pub offset: Offset,
    /// Partition the record came from.
    pub partition: PartitionId,
}

impl CompactedEntry {
    /// Returns `true` if the entry's timestamp is older than `max_age`.
    #[inline]
    pub fn is_stale(&self, max_age: std::time::Duration) -> bool {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(i64::MAX);
        now_ms.saturating_sub(self.timestamp_ms) > max_age.as_millis() as i64
    }
}

/// A change to a [`CompactedTable`].
///
/// Returned by [`CompactedTable::apply()`] to describe how the table
/// was modified after processing a record.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableChange {
    /// The record key.
    pub key: Bytes,
    /// The previous value (`None` if the key was not in the table).
    pub old_value: Option<Bytes>,
    /// The new value (`None` for a tombstone / deletion).
    pub new_value: Option<Bytes>,
    /// Partition the record came from.
    pub partition: PartitionId,
    /// Offset of the record within the partition.
    pub offset: Offset,
    /// Record timestamp.
    pub timestamp: Timestamp,
}

impl TableChange {
    /// Returns `true` if this change is a deletion (tombstone).
    #[inline]
    pub fn is_delete(&self) -> bool {
        self.new_value.is_none()
    }

    /// Returns `true` if this is an insert (key was not previously in the table).
    #[inline]
    pub fn is_insert(&self) -> bool {
        self.old_value.is_none() && self.new_value.is_some()
    }

    /// Returns `true` if this is an update (key existed with a previous value).
    #[inline]
    pub fn is_update(&self) -> bool {
        self.old_value.is_some() && self.new_value.is_some()
    }
}

/// Metrics snapshot for a [`CompactedTable`] or [`CompactedTopicConsumer`].
///
/// Returned by [`CompactedTable::metrics_snapshot()`] and
/// [`CompactedTopicConsumer::metrics_snapshot()`]. All counts are
/// monotonically increasing since the table was created.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactedTableSnapshot {
    /// Number of distinct keys currently held in the table.
    pub entry_count: u64,
    /// Total records processed (including tombstones and keyless records).
    pub records_processed: u64,
    /// Total tombstone records processed (keys removed from the table).
    pub tombstones_processed: u64,
    /// `true` once all assigned partitions have been read up to the
    /// high-water mark at scan time.
    ///
    /// Always `false` for a bare [`CompactedTable`] (which has no consumer
    /// attached); use [`CompactedTopicConsumer::metrics_snapshot()`] to
    /// obtain an accurate `caught_up` value.
    pub caught_up: bool,
}

/// Rewinds a partition to the start of its log.
///
/// Exists so the rebalance listener can be exercised without a live broker
/// connection; [`Consumer`] is the only production implementation.
trait PartitionRewinder: Send + Sync {
    fn rewind_to_beginning<'a>(
        &'a self,
        topic: &'a str,
        partition: PartitionId,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>>;
}

impl PartitionRewinder for Consumer {
    fn rewind_to_beginning<'a>(
        &'a self,
        topic: &'a str,
        partition: PartitionId,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(self.seek_to_beginning(topic, partition))
    }
}

/// A [`ConsumerRebalanceListener`] that keeps a shared [`CompactedTable`] in
/// sync with the consumer's assignment across rebalances.
///
/// This is the recommended way to integrate a [`CompactedTable`] with a
/// group-coordinated [`Consumer`]: wrap the table in an
/// `Arc<Mutex<CompactedTable>>`, share a clone with this listener, and
/// register the listener on the consumer before subscribing.
///
/// # Behaviour
///
/// - **Revoked / lost partitions** — only the entries that came from those
///   partitions are removed. Entries from partitions this consumer kept are
///   preserved, because their committed position has already moved past the
///   records that produced them and they would otherwise never be re-read.
/// - **Assigned partitions** — the entries of each newly assigned partition
///   are dropped and the partition is rewound to the beginning of the log.
///   A compacted-topic table is only correct if every owned partition is
///   replayed from its start; resuming from a committed offset would surface
///   just the keys written after the rebalance and report `None` for every
///   other live key.
///
/// Rewinding requires a handle to the consumer, which does not exist yet when
/// the listener is registered on the builder. Call
/// [`attach_consumer()`](Self::attach_consumer) once the consumer is built —
/// until then, assignments are pruned but not rewound, and a warning is
/// logged.
///
/// Because [`CompactedTable`] entries record a partition but not a topic,
/// pruning matches on partition id across all topics. Use one table per topic
/// with multi-topic consumers.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use tokio::sync::Mutex;
/// use krafka::consumer::{Consumer, CompactedTable, CompactedTableClearListener};
///
/// let table = Arc::new(Mutex::new(CompactedTable::new()));
/// let listener = CompactedTableClearListener::new(Arc::clone(&table));
///
/// let consumer = Arc::new(
///     Consumer::builder()
///         .bootstrap_servers("localhost:9092")
///         .group_id("my-group")
///         .rebalance_listener(listener.clone())
///         .build()
///         .await?,
/// );
/// // Lets the listener seek newly assigned partitions to the beginning.
/// listener.attach_consumer(Arc::clone(&consumer));
/// consumer.subscribe(&["config-topic"]).await?;
///
/// loop {
///     let records = consumer.poll(Duration::from_secs(1)).await?;
///     let mut t = table.lock().await;
///     t.ingest(&records);
/// }
/// ```
#[derive(Clone)]
pub struct CompactedTableClearListener {
    table: Arc<Mutex<CompactedTable>>,
    /// Set once via `attach_consumer()`; shared by every clone of the listener
    /// so attaching after registration is visible to the registered copy.
    rewinder: Arc<std::sync::OnceLock<Arc<dyn PartitionRewinder>>>,
}

impl fmt::Debug for CompactedTableClearListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompactedTableClearListener")
            .field("consumer_attached", &self.rewinder.get().is_some())
            .finish()
    }
}

impl CompactedTableClearListener {
    /// Create a new listener that prunes `table` as partitions change owner.
    ///
    /// Attach the consumer with
    /// [`attach_consumer()`](Self::attach_consumer) so newly assigned
    /// partitions can be replayed from the beginning of the log.
    pub fn new(table: Arc<Mutex<CompactedTable>>) -> Self {
        Self {
            table,
            rewinder: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Give the listener the consumer it belongs to, enabling it to seek
    /// newly assigned partitions to the beginning of the log.
    ///
    /// Call this once, right after the consumer is built and before
    /// subscribing. The handle is shared by every clone of this listener, so
    /// it takes effect for the copy already registered on the consumer.
    /// Subsequent calls are ignored.
    pub fn attach_consumer(&self, consumer: Arc<Consumer>) {
        if self.rewinder.set(consumer).is_err() {
            tracing::warn!(
                "CompactedTableClearListener: consumer already attached; ignoring repeat call"
            );
        }
    }

    /// Create a listener with an arbitrary rewind target (test seam).
    #[cfg(test)]
    fn with_rewinder(
        table: Arc<Mutex<CompactedTable>>,
        rewinder: Arc<dyn PartitionRewinder>,
    ) -> Self {
        let cell = std::sync::OnceLock::new();
        let _ = cell.set(rewinder);
        Self {
            table,
            rewinder: Arc::new(cell),
        }
    }

    /// Drop the entries produced by `partitions` from the shared table.
    async fn prune(&self, partitions: &[TopicPartition], reason: &str) {
        if partitions.is_empty() {
            return;
        }
        let ids: Vec<PartitionId> = partitions.iter().map(|tp| tp.partition).collect();
        let removed = self.table.lock().await.remove_partitions(&ids);
        debug!(
            partitions = ?ids,
            removed,
            reason,
            "CompactedTableClearListener pruned table entries"
        );
    }
}

impl ConsumerRebalanceListener for CompactedTableClearListener {
    /// Drops any stale state for the newly assigned partitions and rewinds
    /// each of them to the beginning of the log, so the table is rebuilt from
    /// the partition's full history rather than from the committed offset.
    async fn on_partitions_assigned(&self, partitions: &[TopicPartition]) {
        if partitions.is_empty() {
            return;
        }

        // A gained partition is replayed in full, so any leftover entries for
        // it would only be duplicated work at best and stale at worst.
        self.prune(partitions, "assigned").await;

        let Some(rewinder) = self.rewinder.get() else {
            tracing::warn!(
                partitions = partitions.len(),
                "CompactedTableClearListener: no consumer attached, cannot rewind newly \
                 assigned partitions; the table will only observe keys written after this \
                 rebalance. Call attach_consumer() after building the consumer."
            );
            return;
        };

        for tp in partitions {
            match rewinder.rewind_to_beginning(&tp.topic, tp.partition).await {
                Ok(()) => debug!(
                    topic = %tp.topic,
                    partition = tp.partition,
                    "CompactedTableClearListener rewound newly assigned partition"
                ),
                Err(e) => tracing::warn!(
                    topic = %tp.topic,
                    partition = tp.partition,
                    error = %e,
                    "CompactedTableClearListener failed to rewind newly assigned partition; \
                     the table may be missing keys for it"
                ),
            }
        }
    }

    /// Removes only the entries belonging to the revoked partitions, leaving
    /// the state of retained partitions intact.
    async fn on_partitions_revoked(&self, partitions: &[TopicPartition]) {
        self.prune(partitions, "revoked").await;
    }

    /// Removes only the entries belonging to the lost partitions, leaving the
    /// state of retained partitions intact.
    async fn on_partitions_lost(&self, partitions: &[TopicPartition]) {
        self.prune(partitions, "lost").await;
    }
}

/// In-memory key→value table built from log-compacted Kafka records.
///
/// `CompactedTable` is a standalone data structure with no dependency on
/// [`Consumer`] or Kafka networking. Feed it [`ConsumerRecord`]s via
/// [`apply()`](Self::apply) and it maintains a snapshot that reflects
/// the latest value for each key, handling tombstones automatically.
///
/// # Composability
///
/// Because it is decoupled from the consumer, `CompactedTable` works with
/// **any** consumer setup — group, standalone, or manual assignment:
///
/// ```rust,ignore
/// let consumer = Consumer::builder()
///     .bootstrap_servers("localhost:9092")
///     .group_id("my-group")
///     .build()
///     .await?;
/// consumer.subscribe(&["config-topic"]).await?;
///
/// let mut table = CompactedTable::new();
/// loop {
///     let records = consumer.poll(Duration::from_secs(1)).await?;
///     let changes = table.apply(&records);
///     for change in &changes {
///         println!("{:?}", change);
///     }
/// }
/// ```
///
/// # Record handling
///
/// - Records **without a key** are skipped (compacted topics require keys).
/// - **Tombstones** (key present, value absent) remove the key from the table.
/// - All other records insert or update the table entry for that key.
///
/// # Cross-partition keys
///
/// The table is keyed purely by record key, not by (partition, key). If the
/// same key appears in multiple partitions (e.g., due to a custom partitioner
/// or producer misconfiguration), entries will be conflated with last-write-wins
/// ordering across partitions. This matches the common single-writer pattern
/// for compacted topics; if partition-scoped dedup is required, encode the
/// partition into the key before feeding records to the table.
///
/// # Equality
///
/// Two tables are equal if they contain the same key→value entries,
/// regardless of processing history (`records_processed`,
/// `tombstones_processed`). This follows the `std` convention where
/// collection equality reflects contents, not metadata.
#[derive(Default, Clone)]
pub struct CompactedTable {
    /// The key→entry map (value + provenance metadata).
    entries: HashMap<Bytes, CompactedEntry>,
    /// Total records processed (including tombstones and keyless records).
    records_processed: u64,
    /// Total tombstones processed.
    tombstones_processed: u64,
}

impl CompactedTable {
    /// Create an empty table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a table pre-allocated for the expected number of keys.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: HashMap::with_capacity(capacity),
            records_processed: 0,
            tombstones_processed: 0,
        }
    }

    /// Apply a batch of consumer records to the table.
    ///
    /// Returns a list of [`TableChange`]s — one per keyed record applied.
    /// Records without a key are counted but produce no change.
    /// Tombstones always produce a change for their key, even if the key was
    /// not present in the table; in that case both `old_value` and `new_value`
    /// are `None`.
    #[must_use = "use ingest() if changes are not needed"]
    pub fn apply(&mut self, records: &[ConsumerRecord]) -> Vec<TableChange> {
        // Don't pre-allocate records.len(): keyless records are skipped and
        // produce no change, so that would over-allocate for mixed batches.
        let mut changes = Vec::new();

        for record in records {
            self.records_processed += 1;

            // Compacted topics require keys; skip keyless records.
            let Some(ref key) = record.key else {
                continue;
            };

            let change = self.apply_keyed_record(key, record);
            changes.push(change);
        }

        changes
    }

    /// Get the current entry for a key, including value, timestamp, and offset.
    ///
    /// Returns `None` if the key is not in the table (never seen or deleted
    /// by a tombstone).
    pub fn get(&self, key: &[u8]) -> Option<&CompactedEntry> {
        self.entries.get(key)
    }

    /// Get just the value bytes for a key, without provenance metadata.
    ///
    /// Equivalent to `get(key).map(|e| &e.value)`. Use [`get`](Self::get)
    /// when you also need the timestamp or offset.
    pub fn get_value(&self, key: &[u8]) -> Option<&Bytes> {
        self.entries.get(key).map(|e| &e.value)
    }

    /// Check if the table contains the given key.
    pub fn contains_key(&self, key: &[u8]) -> bool {
        self.entries.contains_key(key)
    }

    /// Get the number of keys currently in the table.
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the table has no keys.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterate over all key→entry pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &CompactedEntry)> {
        self.entries.iter()
    }

    /// Iterate over all keys in the table.
    pub fn keys(&self) -> impl Iterator<Item = &Bytes> {
        self.entries.keys()
    }

    /// Iterate over all entries in the table.
    pub fn values(&self) -> impl Iterator<Item = &CompactedEntry> {
        self.entries.values()
    }

    /// Get a snapshot (clone) of the key→entry map.
    ///
    /// Returns all entries including provenance metadata. Use
    /// [`Clone::clone()`] if you need a full copy including counters.
    #[must_use]
    pub fn snapshot(&self) -> HashMap<Bytes, CompactedEntry> {
        self.entries.clone()
    }

    /// Total records processed (including tombstones and keyless records).
    pub fn records_processed(&self) -> u64 {
        self.records_processed
    }

    /// Total tombstones processed.
    pub fn tombstones_processed(&self) -> u64 {
        self.tombstones_processed
    }

    /// Return a metrics snapshot for this table.
    ///
    /// The `caught_up` field is always `false` for a bare `CompactedTable`.
    /// Use [`CompactedTopicConsumer::metrics_snapshot()`] for a snapshot that
    /// includes the caught-up status.
    #[must_use]
    pub fn metrics_snapshot(&self) -> CompactedTableSnapshot {
        CompactedTableSnapshot {
            entry_count: self.entries.len() as u64,
            records_processed: self.records_processed,
            tombstones_processed: self.tombstones_processed,
            caught_up: false,
        }
    }

    /// Apply records to the table without tracking changes.
    ///
    /// This is semantically identical to [`apply()`](Self::apply) but skips
    /// building the [`TableChange`] list, avoiding allocations and `Bytes`
    /// ref-count churn. Prefer this during bulk loads (e.g., initial scan)
    /// where only the final table state matters.
    pub fn ingest(&mut self, records: &[ConsumerRecord]) {
        for record in records {
            self.records_processed += 1;

            let Some(ref key) = record.key else {
                continue;
            };

            self.ingest_keyed_record(key, record);
        }
    }

    /// Shared mutation logic for a single keyed record, returning a
    /// [`TableChange`] describing the modification.
    fn apply_keyed_record(&mut self, key: &Bytes, record: &ConsumerRecord) -> TableChange {
        if record.is_tombstone() {
            self.tombstones_processed += 1;
            let old_entry = self.entries.remove(key.as_ref());
            TableChange {
                key: key.clone(),
                old_value: old_entry.map(|e| e.value),
                new_value: None,
                partition: record.partition,
                offset: record.offset,
                timestamp: record.timestamp,
            }
        } else {
            // value must be Some here because is_tombstone() returned false and key is Some
            let Some(value) = record.value.clone() else {
                unreachable!("non-tombstone compacted record must have a value");
            };
            let key_owned = key.clone();
            let new_entry = CompactedEntry {
                value: value.clone(),
                timestamp_ms: record.timestamp,
                offset: record.offset,
                partition: record.partition,
            };
            // Warn if the same key arrives from a different partition — this is
            // almost always a producer misconfiguration or unexpected custom partitioner
            // and will silently produce last-write-wins semantics across partitions.
            if let Some(existing) = self.entries.get(key_owned.as_ref())
                && existing.partition != record.partition
            {
                tracing::warn!(
                    existing_partition = existing.partition,
                    new_partition = record.partition,
                    "CompactedTable: key appears in multiple partitions; \
                     entries will be conflated with last-write-wins semantics. \
                     If partition-scoped dedup is required, encode the partition \
                     into the key before ingesting records."
                );
            }
            let old_entry = self.entries.insert(key_owned.clone(), new_entry);
            TableChange {
                key: key_owned,
                old_value: old_entry.map(|e| e.value),
                new_value: Some(value),
                partition: record.partition,
                offset: record.offset,
                timestamp: record.timestamp,
            }
        }
    }

    /// Shared mutation logic for a single keyed record without producing a
    /// [`TableChange`]. Avoids extra clones needed for the change struct.
    fn ingest_keyed_record(&mut self, key: &Bytes, record: &ConsumerRecord) {
        if record.is_tombstone() {
            self.tombstones_processed += 1;
            self.entries.remove(key.as_ref());
        } else {
            // value must be Some here because is_tombstone() returned false and key is Some
            let Some(value) = record.value.clone() else {
                unreachable!("non-tombstone compacted record must have a value");
            };
            // Warn if the same key arrives from a different partition — this is
            // almost always a producer misconfiguration or unexpected custom partitioner
            // and will silently produce last-write-wins semantics across partitions.
            if let Some(existing) = self.entries.get(key.as_ref())
                && existing.partition != record.partition
            {
                tracing::warn!(
                    existing_partition = existing.partition,
                    new_partition = record.partition,
                    "CompactedTable: key appears in multiple partitions; \
                     entries will be conflated with last-write-wins semantics. \
                     If partition-scoped dedup is required, encode the partition \
                     into the key before ingesting records."
                );
            }
            self.entries.insert(
                key.clone(),
                CompactedEntry {
                    value,
                    timestamp_ms: record.timestamp,
                    offset: record.offset,
                    partition: record.partition,
                },
            );
        }
    }

    /// Clear the table, removing all entries and resetting counters.
    ///
    /// Useful when the table needs to be rebuilt from scratch. When only some
    /// partitions change ownership during a rebalance, prefer
    /// [`remove_partitions()`](Self::remove_partitions), which keeps the state
    /// of the partitions that are still owned.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.records_processed = 0;
        self.tombstones_processed = 0;
    }

    /// Remove every entry that was last written by one of `partitions`,
    /// leaving entries from all other partitions untouched.
    ///
    /// Returns the number of entries removed.
    ///
    /// This is the correct reaction to a partial rebalance: dropping only the
    /// partitions this consumer no longer owns preserves the state of the
    /// partitions it kept. Clearing the whole table instead would discard
    /// retained state that the consumer will never re-read, because its
    /// committed position on those partitions has already advanced past the
    /// records that built it.
    ///
    /// Entries are matched on partition id alone — the table does not record
    /// the topic a key came from — so with a multi-topic consumer this also
    /// removes same-numbered partitions of other topics. Use one table per
    /// topic if that matters.
    ///
    /// The `records_processed` and `tombstones_processed` counters are
    /// deliberately left unchanged: they are lifetime totals of ingest work
    /// performed, not a description of current table contents.
    pub fn remove_partitions(&mut self, partitions: &[PartitionId]) -> usize {
        if partitions.is_empty() {
            return 0;
        }
        let before = self.entries.len();
        self.entries
            .retain(|_, entry| !partitions.contains(&entry.partition));
        before - self.entries.len()
    }
}

impl<'a> IntoIterator for &'a CompactedTable {
    type Item = (&'a Bytes, &'a CompactedEntry);
    type IntoIter = std::collections::hash_map::Iter<'a, Bytes, CompactedEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

impl IntoIterator for CompactedTable {
    type Item = (Bytes, CompactedEntry);
    type IntoIter = std::collections::hash_map::IntoIter<Bytes, CompactedEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl fmt::Debug for CompactedTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompactedTable")
            .field("len", &self.entries.len())
            .field("records_processed", &self.records_processed)
            .field("tombstones_processed", &self.tombstones_processed)
            .finish()
    }
}

impl PartialEq for CompactedTable {
    /// Compares entries only — processing counters are ignored.
    /// Two tables with the same key→entry content are equal regardless
    /// of how many records were processed to reach that state.
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries
    }
}

impl Eq for CompactedTable {}

// ---------------------------------------------------------------------------
// CompactedTopicConsumer — convenience wrapper
// ---------------------------------------------------------------------------

/// Default upper bound on how long [`CompactedTopicConsumer::scan()`] waits
/// for every partition to reach its target offset before giving up.
const DEFAULT_SCAN_TIMEOUT: Duration = Duration::from_secs(300);

/// The operations `scan()` needs from a consumer.
///
/// Abstracting them keeps the scan loop — including its deadline handling —
/// testable without a live broker. [`Consumer`] is the only production
/// implementation.
trait ScanSource: Sync {
    fn poll_records(
        &self,
        timeout: Duration,
    ) -> impl std::future::Future<Output = Result<Vec<ConsumerRecord>>> + Send;

    fn assigned_partitions(
        &self,
        topic: &str,
    ) -> impl std::future::Future<Output = Vec<PartitionId>> + Send;

    fn partition_position(
        &self,
        topic: &str,
        partition: PartitionId,
    ) -> impl std::future::Future<Output = Option<Offset>> + Send;

    fn end_offsets(
        &self,
        topic: &str,
    ) -> impl std::future::Future<Output = Result<HashMap<PartitionId, Result<Offset>>>> + Send;
}

impl ScanSource for Consumer {
    async fn poll_records(&self, timeout: Duration) -> Result<Vec<ConsumerRecord>> {
        self.poll(timeout).await
    }

    async fn assigned_partitions(&self, topic: &str) -> Vec<PartitionId> {
        self.assignment()
            .await
            .get(topic)
            .cloned()
            .unwrap_or_default()
    }

    async fn partition_position(&self, topic: &str, partition: PartitionId) -> Option<Offset> {
        self.position(topic, partition).await
    }

    async fn end_offsets(&self, topic: &str) -> Result<HashMap<PartitionId, Result<Offset>>> {
        self.offsets_for_times_for_topic(topic, -1).await
    }
}

/// A partition that has not yet reached its scan target offset.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LaggingPartition {
    partition: PartitionId,
    /// Consumer position, or `None` when no position is known yet (e.g. the
    /// partition has no leader and was never fetched).
    position: Option<Offset>,
    target: Offset,
}

impl fmt::Display for LaggingPartition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.position {
            Some(pos) => write!(
                f,
                "partition {} (position {}, target {})",
                self.partition, pos, self.target
            ),
            None => write!(
                f,
                "partition {} (position unknown, target {})",
                self.partition, self.target
            ),
        }
    }
}

/// Build the error returned when a scan runs out of time, naming every
/// partition that is still behind together with its position and target so
/// the stall can be attributed to a specific partition.
fn scan_timeout_error(topic: &str, timeout: Duration, lagging: &[LaggingPartition]) -> KrafkaError {
    let detail = lagging
        .iter()
        .map(|l| l.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    KrafkaError::timeout(format!(
        "scan of compacted topic '{topic}' did not catch up within {timeout:?}; \
         partitions still behind their target offset: [{detail}]"
    ))
}

/// Partitions that have not yet reached their target offset.
///
/// Only currently assigned partitions are considered. A partition whose
/// position is unknown counts as lagging, as does one whose target is missing
/// from the current assignment — the latter means the assignment changed
/// mid-scan and the snapshot can no longer be satisfied.
async fn lagging_partitions<S: ScanSource>(
    source: &S,
    topic: &str,
    targets: &HashMap<PartitionId, Offset>,
) -> Vec<LaggingPartition> {
    let assigned = source.assigned_partitions(topic).await;

    if assigned.is_empty() {
        // Nothing assigned means nothing can advance; report every target.
        let mut all: Vec<LaggingPartition> = targets
            .iter()
            .map(|(&partition, &target)| LaggingPartition {
                partition,
                position: None,
                target,
            })
            .collect();
        all.sort_by_key(|l| l.partition);
        return all;
    }

    let mut lagging = Vec::new();
    for partition in assigned {
        let Some(&target) = targets.get(&partition) else {
            // Not part of the snapshot (e.g. added after the scan started).
            continue;
        };
        // A target of 0 or -1 means the partition is empty; nothing to consume.
        if target <= 0 {
            continue;
        }

        let position = source.partition_position(topic, partition).await;
        if position.is_none_or(|pos| pos < target) {
            lagging.push(LaggingPartition {
                partition,
                position,
                target,
            });
        }
    }
    lagging.sort_by_key(|l| l.partition);
    lagging
}

/// Drive a bounded scan of `topic` into `table`.
///
/// Polls until every assigned partition reaches the high-watermark snapshot
/// taken at the start, or until `timeout` elapses — in which case the returned
/// error names the partitions that never got there.
async fn run_scan<S: ScanSource>(
    source: &S,
    topic: &str,
    table: &mut CompactedTable,
    poll_timeout: Duration,
    timeout: Duration,
) -> Result<()> {
    // Fail fast if no partitions are assigned — avoids a scan that can only
    // ever time out (especially when using from_consumer() without assign()).
    if source.assigned_partitions(topic).await.is_empty() {
        return Err(KrafkaError::invalid_state(format!(
            "no partitions assigned for topic '{topic}'; \
             assign partitions before calling scan()"
        )));
    }

    // Snapshot the latest offsets (high-water marks) **before** starting
    // the poll loop.  Comparing against a fixed snapshot means the scan
    // terminates even when new records arrive during the scan, avoiding
    // the HWM-chasing race that makes scans on active topics non-terminating.
    let hwm_results = source.end_offsets(topic).await?;

    // Fail fast on any per-partition error: a missing partition in the
    // target map causes the caught-up check to silently skip it, which
    // would prematurely declare the scan complete without having consumed
    // that partition's data.
    let mut scan_target_hwms: HashMap<PartitionId, Offset> =
        HashMap::with_capacity(hwm_results.len());
    for (partition, result) in hwm_results {
        let offset = result.map_err(|e| {
            KrafkaError::invalid_state(format!(
                "failed to fetch high-watermark for '{topic}' partition {partition}: {e}"
            ))
        })?;
        scan_target_hwms.insert(partition, offset);
    }

    if scan_target_hwms.values().all(|&hwm| hwm <= 0) {
        // All partitions are empty (HWM = 0) — nothing to scan.
        info!("Compacted topic '{topic}' has no data yet (all partition HWMs = 0); scan complete");
        return Ok(());
    }

    // Keep only non-empty partitions in the target map; empty partitions
    // are satisfied immediately and don't need to be polled.
    scan_target_hwms.retain(|_, &mut hwm| hwm > 0);

    info!(
        topic = %topic,
        partitions = scan_target_hwms.len(),
        timeout = ?timeout,
        "Starting compacted topic scan (HWM snapshot taken)"
    );

    let deadline = std::time::Instant::now() + timeout;

    loop {
        let mut records = source.poll_records(poll_timeout).await?;
        let before_len = records.len();
        records.retain(|r| r.topic == topic);
        let filtered = before_len - records.len();
        if filtered > 0 {
            debug!("Filtered out {filtered} record(s) from other topics during scan for '{topic}'");
        }
        table.ingest(&records);

        let lagging = lagging_partitions(source, topic, &scan_target_hwms).await;
        if lagging.is_empty() {
            info!(
                "Compacted topic scan complete for '{}': {} keys, {} records processed, \
                 {} tombstones",
                topic,
                table.len(),
                table.records_processed(),
                table.tombstones_processed(),
            );
            return Ok(());
        }

        // A partition can stall indefinitely (no leader, offsets that never
        // resolve), so the loop is bounded by wall-clock time rather than
        // trusting every partition to eventually converge.
        if std::time::Instant::now() >= deadline {
            return Err(scan_timeout_error(topic, timeout, &lagging));
        }
    }
}

/// Convenience wrapper that pairs a [`Consumer`] with a [`CompactedTable`]
/// for the common pattern of scanning an entire compacted topic.
///
/// When constructed via
/// [`from_consumer_builder()`](Self::from_consumer_builder), it creates a
/// standalone (no group) consumer, assigns every partition, reads from the
/// earliest offset at `read_committed` isolation, and provides
/// [`scan()`](Self::scan) to block until the table is fully populated.
///
/// [`from_consumer()`](Self::from_consumer) uses the caller-provided consumer
/// configuration and assignment as-is.
///
/// For fully custom consumer setups, you can also use [`CompactedTable`]
/// directly with your own [`Consumer`].
///
/// # Example
///
/// ```rust,ignore
/// use krafka::consumer::{CompactedTopicConsumer, Consumer};
/// use std::time::Duration;
///
/// let mut ctc = CompactedTopicConsumer::from_consumer_builder(
///     Consumer::builder().bootstrap_servers("localhost:9092"),
///     "user-profiles",
/// )
/// .await?;
///
/// // Build the initial snapshot
/// ctc.scan(Duration::from_secs(1)).await?;
///
/// // Read a key
/// if let Some(value) = ctc.table().get(b"user-123") {
///     println!("User profile: {:?}", value);
/// }
///
/// // Continue tailing for live updates
/// loop {
///     let changes = ctc.poll(Duration::from_secs(1)).await?;
///     for change in &changes {
///         println!("{:?}", change);
///     }
/// }
/// ```
pub struct CompactedTopicConsumer {
    /// The underlying Kafka consumer.
    consumer: Consumer,
    /// Topic name.
    topic: String,
    /// In-memory key→value table.
    table: CompactedTable,
    /// Whether the initial scan has completed (caught up to high watermarks).
    caught_up: bool,
}

impl fmt::Debug for CompactedTopicConsumer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CompactedTopicConsumer")
            .field("topic", &self.topic)
            .field("caught_up", &self.caught_up)
            .field("table", &self.table)
            .finish()
    }
}

impl CompactedTopicConsumer {
    /// Create a `CompactedTopicConsumer` from an already-configured [`Consumer`].
    ///
    /// The consumer should already have partitions assigned for the given
    /// topic (e.g., via [`Consumer::assign()`]). This constructor performs no
    /// partition discovery and does not modify the consumer's subscription
    /// or assignment.
    ///
    /// If the consumer is subscribed/assigned to additional topics, records
    /// from those topics are filtered out (logged at debug level) — only
    /// records matching the given `topic` are applied to the table.
    ///
    /// This constructor is the best fit for consumers with stable, explicit
    /// assignments. If you wrap a group-coordinated consumer whose assignment
    /// can change over time, note that the table owned by this wrapper is not
    /// pruned when partitions are revoked: keys loaded from partitions that
    /// are no longer assigned remain in it. Drop them from a rebalance
    /// callback with
    /// [`table_mut().remove_partitions()`](CompactedTable::remove_partitions),
    /// or share an `Arc<Mutex<CompactedTable>>` with a
    /// [`CompactedTableClearListener`], which does that bookkeeping (and the
    /// matching rewind of newly assigned partitions) for you.
    ///
    /// # Finding the partitions to assign
    ///
    /// [`Consumer::fetch_metadata`] answers exactly this, and refreshes first —
    /// there is no need for a second `AdminClient` and a second auth handshake
    /// to enumerate them:
    ///
    /// ```rust,no_run
    /// use krafka::consumer::{CompactedTopicConsumer, Consumer};
    ///
    /// # async fn example(consumer: Consumer, topic: &str) -> Result<(), krafka::error::KrafkaError> {
    /// let metadata = consumer.fetch_metadata(Some(topic)).await?;
    /// let partitions: Vec<i32> = metadata
    ///     .topics
    ///     .iter()
    ///     .find(|t| t.name == topic)
    ///     .map(|t| t.partitions.keys().copied().collect())
    ///     .unwrap_or_default();
    /// consumer.assign(topic, partitions).await?;
    /// let compacted = CompactedTopicConsumer::from_consumer(consumer, topic);
    /// # let _ = compacted;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// Assign **every** partition, or the table is silently partial.
    /// [`from_consumer_builder`](Self::from_consumer_builder) does this step
    /// for you and is the better choice unless you need a `Consumer` you have
    /// already built for other reasons.
    ///
    /// Use this when you need full control over the consumer configuration —
    /// including an isolation level other than the `ReadCommitted` that
    /// `from_consumer_builder` imposes.
    pub fn from_consumer(consumer: Consumer, topic: impl Into<String>) -> Self {
        Self {
            consumer,
            topic: topic.into(),
            table: CompactedTable::new(),
            caught_up: false,
        }
    }

    /// Scan the topic from the consumer's current position until all
    /// partitions are caught up.
    ///
    /// This method does not seek to the beginning of the topic and does not
    /// clear or reset the current table state before polling. It repeatedly
    /// polls using the given `poll_timeout` until the consumer's position on
    /// every assigned partition reaches or exceeds the latest known high
    /// watermark.
    ///
    /// When this scanner is created via the builder, the internal consumer is
    /// initialized with [`AutoOffsetReset::Earliest`], so an initial `scan()`
    /// will typically read from the beginning of the topic. When using
    /// [`from_consumer()`](Self::from_consumer), however, scanning starts from
    /// whatever position that consumer already has.
    ///
    /// `scan()` takes a **point-in-time snapshot** of the latest offsets
    /// (high-water marks) for all assigned partitions at the moment it is
    /// called, then reads until the consumer's position on every partition
    /// reaches or exceeds those snapshot offsets. Records written to the
    /// topic *after* the snapshot is taken are not counted towards the
    /// caught-up condition — this bounds the scan to a deterministic target
    /// rather than chasing a continuously advancing watermark.
    ///
    /// The scan is bounded: if the partitions have not all reached their
    /// target within five minutes, it returns a timeout error instead of
    /// polling forever. Use [`scan_with_timeout()`](Self::scan_with_timeout)
    /// to choose a different bound.
    ///
    /// If this call returns `Ok`, [`is_caught_up()`](Self::is_caught_up) is
    /// `true` and the table contains the latest value for every live key
    /// observed up to the snapshot watermark.
    ///
    /// # Errors
    ///
    /// Returns an error if any poll fails unrecoverably, if the initial
    /// watermark snapshot cannot be obtained, or if the scan does not catch
    /// up in time — the timeout error names each partition that is still
    /// behind along with its position and target offset.
    pub async fn scan(&mut self, poll_timeout: Duration) -> Result<()> {
        self.scan_with_timeout(poll_timeout, DEFAULT_SCAN_TIMEOUT)
            .await
    }

    /// Like [`scan()`](Self::scan), but with an explicit upper bound on how
    /// long the scan may run.
    ///
    /// A partition can fail to make progress indefinitely — it may have no
    /// leader, or its offsets may never resolve — so the scan loop is bounded
    /// by wall-clock time. When `timeout` elapses with partitions still short
    /// of their target, the returned error lists those partitions with their
    /// last known position and the target they were expected to reach, which
    /// identifies the stalled partition directly.
    ///
    /// `timeout` bounds the whole scan, not an individual poll; a single poll
    /// may still block for up to `poll_timeout` past the deadline before the
    /// timeout is detected.
    ///
    /// # Errors
    ///
    /// Returns an error if no partitions are assigned, if any poll fails
    /// unrecoverably, if the watermark snapshot cannot be obtained, or if
    /// `timeout` elapses before every partition reaches its target.
    pub async fn scan_with_timeout(
        &mut self,
        poll_timeout: Duration,
        timeout: Duration,
    ) -> Result<()> {
        run_scan(
            &self.consumer,
            &self.topic,
            &mut self.table,
            poll_timeout,
            timeout,
        )
        .await?;
        self.caught_up = true;
        Ok(())
    }

    /// Poll for new records and update the table.
    ///
    /// Returns a list of [`TableChange`]s describing how the table was
    /// modified. An empty list means no new records were received.
    ///
    /// Also updates the [`is_caught_up()`](Self::is_caught_up) flag if the
    /// consumer reaches the high watermarks for the first time.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying consumer poll fails.
    pub async fn poll(&mut self, timeout: Duration) -> Result<Vec<TableChange>> {
        let mut records = self.consumer.poll(timeout).await?;
        let before_len = records.len();
        records.retain(|r| r.topic == self.topic);
        let filtered = before_len - records.len();
        if filtered > 0 {
            debug!(
                "Filtered out {} record(s) from other topics during poll for '{}'",
                filtered, self.topic
            );
        }
        let changes = self.table.apply(&records);

        if !self.caught_up && self.check_caught_up().await {
            self.caught_up = true;
            debug!(
                "CompactedTopicConsumer for '{}' caught up via poll()",
                self.topic
            );
        }

        Ok(changes)
    }

    /// Returns a reference to the underlying [`CompactedTable`].
    pub fn table(&self) -> &CompactedTable {
        &self.table
    }

    /// Returns a mutable reference to the underlying [`CompactedTable`].
    pub fn table_mut(&mut self) -> &mut CompactedTable {
        &mut self.table
    }

    /// Returns `true` after the consumer has caught up to the topic's high
    /// watermarks. Becomes `true` when [`scan()`](Self::scan) completes or
    /// when [`poll()`](Self::poll) naturally reaches the end.
    pub fn is_caught_up(&self) -> bool {
        self.caught_up
    }

    /// Return a metrics snapshot for this consumer, including table statistics
    /// and the caught-up flag.
    #[must_use]
    pub fn metrics_snapshot(&self) -> CompactedTableSnapshot {
        let mut snap = self.table.metrics_snapshot();
        snap.caught_up = self.caught_up;
        snap
    }

    /// Returns the topic name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Returns a reference to the underlying [`Consumer`].
    ///
    /// Useful for calling consumer operations not exposed on this wrapper,
    /// such as seek, pause, commit, or reading assignment/metrics.
    pub fn consumer(&self) -> &Consumer {
        &self.consumer
    }

    /// Returns a mutable reference to the underlying [`Consumer`].
    pub fn consumer_mut(&mut self) -> &mut Consumer {
        &mut self.consumer
    }

    /// Unwrap this wrapper and return the underlying [`Consumer`] and
    /// [`CompactedTable`].
    pub fn into_parts(self) -> (Consumer, CompactedTable) {
        (self.consumer, self.table)
    }

    /// Close the underlying consumer and surface shutdown errors.
    pub async fn close(&self) -> Result<()> {
        self.consumer.close().await
    }

    /// Check if all assigned partitions have reached their live cached high
    /// watermarks.
    ///
    /// This is used by [`poll()`](Self::poll) for best-effort post-scan
    /// caught-up detection. The bounded [`scan()`](Self::scan) operation
    /// compares against a pre-scan HWM snapshot instead.
    async fn check_caught_up(&self) -> bool {
        let assignments = self.consumer.assignment().await;
        let Some(partitions) = assignments.get(&self.topic) else {
            return false;
        };

        for &partition in partitions {
            let position = self.consumer.position(&self.topic, partition).await;
            let high_watermark = self
                .consumer
                .cached_end_offset(&self.topic, partition)
                .await;

            match (position, high_watermark) {
                // Position at or past the high watermark — caught up.
                (Some(pos), Some(hw)) if pos >= hw => continue,
                // High watermark is 0 — empty partition, nothing to consume.
                (_, Some(0)) => continue,
                // Position or high watermark not yet known, or still behind.
                _ => return false,
            }
        }

        true
    }
}

impl CompactedTopicConsumer {
    /// Build from a configured [`ConsumerBuilder`], discovering and assigning
    /// every partition of `topic`.
    ///
    /// This replaces the curated `CompactedTopicConsumerBuilder` that used to
    /// live here. That builder owned a hand-picked subset of the consumer's
    /// settings — nine of them — and every setting it omitted was unreachable
    /// through it. Two of the omissions mattered:
    ///
    /// - **`isolation_level`.** The type most likely to be pointed at
    ///   transactional data was the one whose builder could not ask for
    ///   `read_committed`, so a table could be materialised from records that
    ///   were later aborted — wrong in a way the caller cannot see. This
    ///   constructor sets `ReadCommitted` (see below).
    /// - **`connect_timeout`.** `build()` rejects `request_timeout <
    ///   connect_timeout`, so a caller wanting a tight request budget was
    ///   refused with an error naming a value the builder gave them no way to
    ///   change.
    ///
    /// Taking the real `ConsumerBuilder` removes the whole class: everything a
    /// consumer can be configured with is available, and nothing has to be
    /// mirrored here as the consumer grows settings.
    ///
    /// # Settings this constructor imposes
    ///
    /// Three, and they are requirements of materialising a table rather than
    /// preferences, so they override whatever the builder carried:
    ///
    /// | Setting | Value | Why |
    /// |---|---|---|
    /// | `auto_offset_reset` | `Earliest` | A table built from anything later is a partial table. |
    /// | `enable_auto_commit` | `false` | A scan is a materialisation, not group progress. |
    /// | `isolation_level` | `ReadCommitted` | A table must not contain records that were aborted. |
    ///
    /// `ReadCommitted` costs nothing on a topic with no transactions — the last
    /// stable offset equals the high watermark, in the same fetch response, so
    /// there is no extra round trip — and is the difference between a correct
    /// and a corrupt table on one that has them.
    ///
    /// Use [`from_consumer`](Self::from_consumer) with a `Consumer` you built
    /// and assigned yourself if you need different values, including reading
    /// uncommitted state deliberately.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use krafka::consumer::{CompactedTopicConsumer, Consumer};
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), krafka::error::KrafkaError> {
    /// let compacted = CompactedTopicConsumer::from_consumer_builder(
    ///     Consumer::builder()
    ///         .bootstrap_servers("localhost:9092")
    ///         .client_id("state-reader")
    ///         // The whole consumer surface is available here, including the
    ///         // settings the old builder could not express.
    ///         .connect_timeout(Duration::from_secs(2))
    ///         .request_timeout(Duration::from_secs(5)),
    ///     "state-topic",
    /// )
    /// .await?;
    /// # let _ = compacted;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Fails if the consumer cannot be built, if `topic` is not present in
    /// cluster metadata, or if its partition count does not fit in a
    /// [`PartitionId`].
    pub async fn from_consumer_builder(
        builder: ConsumerBuilder,
        topic: impl Into<String>,
    ) -> Result<Self> {
        let topic = topic.into();

        let consumer = builder
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .isolation_level(IsolationLevel::ReadCommitted)
            .build()
            .await?;

        // Refresh metadata to get the latest partition count for the topic.
        // Consumer::build() fetches an initial snapshot, but it may already
        // be slightly stale if the topic was recently expanded.
        consumer
            .metadata
            .refresh_for_topics(Some(&[&topic]))
            .await?;

        let partition_count = consumer.metadata.partition_count(&topic).ok_or_else(|| {
            KrafkaError::config(format!("topic '{topic}' not found in cluster metadata"))
        })?;

        let partition_count = PartitionId::try_from(partition_count).map_err(|_| {
            KrafkaError::config(format!(
                "topic '{topic}' has too many partitions to fit in PartitionId"
            ))
        })?;

        // Assigning every partition is the point: a partial assignment
        // silently materialises a partial table, which is the mistake
        // `run_scan`'s empty-assignment guard exists to catch one step later.
        let partitions: Vec<PartitionId> = (0..partition_count).collect();
        consumer.assign(&topic, partitions).await?;

        debug!(
            "CompactedTopicConsumer initialized for '{}' with {} partitions",
            topic, partition_count
        );

        Ok(CompactedTopicConsumer::from_consumer(consumer, topic))
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;

    fn make_record(
        key: Option<&str>,
        value: Option<&str>,
        partition: PartitionId,
        offset: Offset,
    ) -> ConsumerRecord {
        ConsumerRecord {
            topic: "test-topic".to_string(),
            partition,
            offset,
            timestamp: offset * 1000,
            timestamp_type: 0,
            key: key.map(|k| Bytes::from(k.to_string())),
            value: value.map(|v| Bytes::from(v.to_string())),
            headers: Vec::new(),
            leader_epoch: None,
            delivery_count: None,
        }
    }

    // -----------------------------------------------------------------------
    // CompactedTable tests
    // -----------------------------------------------------------------------

    #[test]
    fn test_table_insert() {
        let mut table = CompactedTable::new();
        let records = vec![
            make_record(Some("k1"), Some("v1"), 0, 0),
            make_record(Some("k2"), Some("v2"), 0, 1),
        ];

        let changes = table.apply(&records);

        assert_eq!(table.len(), 2);
        assert_eq!(table.get_value(b"k1"), Some(&Bytes::from("v1")));
        assert_eq!(table.get_value(b"k2"), Some(&Bytes::from("v2")));
        assert_eq!(changes.len(), 2);
        assert!(changes[0].is_insert());
        assert!(changes[1].is_insert());
        assert_eq!(table.records_processed(), 2);
        assert_eq!(table.tombstones_processed(), 0);
    }

    #[test]
    fn test_table_update() {
        let mut table = CompactedTable::new();
        table.ingest(&[make_record(Some("k1"), Some("old"), 0, 0)]);

        let changes = table.apply(&[make_record(Some("k1"), Some("new"), 0, 5)]);

        assert_eq!(table.len(), 1);
        assert_eq!(table.get_value(b"k1"), Some(&Bytes::from("new")));
        assert_eq!(changes.len(), 1);
        assert!(changes[0].is_update());
        assert_eq!(changes[0].old_value, Some(Bytes::from("old")));
        assert_eq!(changes[0].new_value, Some(Bytes::from("new")));
    }

    #[test]
    fn test_table_tombstone() {
        let mut table = CompactedTable::new();
        table.ingest(&[
            make_record(Some("k1"), Some("v1"), 0, 0),
            make_record(Some("k2"), Some("v2"), 0, 1),
        ]);

        let changes = table.apply(&[make_record(Some("k1"), None, 0, 10)]);

        assert_eq!(table.len(), 1);
        assert!(!table.contains_key(b"k1"));
        assert_eq!(table.get_value(b"k2"), Some(&Bytes::from("v2")));
        assert_eq!(changes.len(), 1);
        assert!(changes[0].is_delete());
        assert_eq!(changes[0].old_value, Some(Bytes::from("v1")));
        assert_eq!(changes[0].new_value, None);
        assert_eq!(table.tombstones_processed(), 1);
    }

    #[test]
    fn test_table_tombstone_for_missing_key() {
        let mut table = CompactedTable::new();
        let changes = table.apply(&[make_record(Some("missing"), None, 0, 0)]);

        assert!(table.is_empty());
        assert_eq!(changes.len(), 1);
        assert!(changes[0].is_delete());
        assert_eq!(changes[0].old_value, None);
        assert_eq!(table.tombstones_processed(), 1);
    }

    #[test]
    fn test_table_skips_keyless() {
        let mut table = CompactedTable::new();
        let records = vec![
            make_record(None, Some("value-without-key"), 0, 0),
            make_record(Some("k1"), Some("v1"), 0, 1),
        ];

        let changes = table.apply(&records);

        assert_eq!(table.len(), 1);
        assert_eq!(changes.len(), 1);
        assert_eq!(table.records_processed(), 2);
    }

    #[test]
    fn test_table_full_lifecycle() {
        let mut table = CompactedTable::new();

        // Insert
        let changes = table.apply(&[
            make_record(Some("user-1"), Some("Alice"), 0, 0),
            make_record(Some("user-2"), Some("Bob"), 0, 1),
        ]);
        assert_eq!(table.len(), 2);
        assert!(changes.iter().all(|c| c.is_insert()));

        // Update
        let changes = table.apply(&[make_record(Some("user-1"), Some("Alice V2"), 0, 2)]);
        assert_eq!(table.get_value(b"user-1"), Some(&Bytes::from("Alice V2")));
        assert!(changes[0].is_update());

        // Delete
        let changes = table.apply(&[make_record(Some("user-2"), None, 0, 3)]);
        assert_eq!(table.len(), 1);
        assert!(changes[0].is_delete());

        // Re-insert deleted key
        let changes = table.apply(&[make_record(Some("user-2"), Some("Bob V2"), 0, 4)]);
        assert_eq!(table.len(), 2);
        assert!(changes[0].is_insert());
    }

    #[test]
    fn test_table_empty_input() {
        let mut table = CompactedTable::new();
        table.ingest(&[make_record(Some("k1"), Some("v1"), 0, 0)]);

        let changes = table.apply(&[]);

        assert_eq!(table.len(), 1);
        assert!(changes.is_empty());
    }

    #[test]
    fn test_table_multiple_partitions() {
        let mut table = CompactedTable::new();
        let records = vec![
            make_record(Some("k1"), Some("v1"), 0, 0),
            make_record(Some("k2"), Some("v2"), 1, 0),
            make_record(Some("k1"), Some("v1-updated"), 0, 1),
        ];

        let changes = table.apply(&records);

        assert_eq!(table.len(), 2);
        assert_eq!(table.get_value(b"k1"), Some(&Bytes::from("v1-updated")));
        assert_eq!(changes.len(), 3);
        assert!(changes[0].is_insert());
        assert!(changes[1].is_insert());
        assert!(changes[2].is_update());
        assert_eq!(changes[0].partition, 0);
        assert_eq!(changes[1].partition, 1);
    }

    #[test]
    fn test_table_with_capacity() {
        let table = CompactedTable::with_capacity(100);
        assert!(table.is_empty());
        assert_eq!(table.records_processed(), 0);
    }

    #[test]
    fn test_table_iter() {
        let mut table = CompactedTable::new();
        table.ingest(&[
            make_record(Some("a"), Some("1"), 0, 0),
            make_record(Some("b"), Some("2"), 0, 1),
        ]);

        let items: HashMap<&Bytes, &Bytes> = table.iter().map(|(k, e)| (k, &e.value)).collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items[&Bytes::from("a")], &Bytes::from("1"));
        assert_eq!(items[&Bytes::from("b")], &Bytes::from("2"));
    }

    #[test]
    fn test_table_snapshot() {
        let mut table = CompactedTable::new();
        table.ingest(&[
            make_record(Some("k1"), Some("v1"), 0, 0),
            make_record(Some("k2"), Some("v2"), 0, 1),
        ]);

        let snap = table.snapshot();
        assert_eq!(snap.len(), 2);
        assert_eq!(
            snap.get(&Bytes::from("k1")).map(|e| &e.value),
            Some(&Bytes::from("v1"))
        );
    }

    #[test]
    fn test_table_debug() {
        let mut table = CompactedTable::new();
        table.ingest(&[
            make_record(Some("k1"), Some("v1"), 0, 0),
            make_record(Some("k2"), None, 0, 1),
        ]);
        let debug = format!("{table:?}");
        assert!(debug.contains("len: 1"));
        assert!(debug.contains("records_processed: 2"));
        assert!(debug.contains("tombstones_processed: 1"));
    }

    #[test]
    fn test_table_clear() {
        let mut table = CompactedTable::new();
        table.ingest(&[
            make_record(Some("k1"), Some("v1"), 0, 0),
            make_record(Some("k2"), None, 0, 1),
        ]);
        assert_eq!(table.len(), 1);
        assert_eq!(table.records_processed(), 2);
        assert_eq!(table.tombstones_processed(), 1);

        table.clear();

        assert!(table.is_empty());
        assert_eq!(table.records_processed(), 0);
        assert_eq!(table.tombstones_processed(), 0);
    }

    #[test]
    fn test_table_into_iterator() {
        let mut table = CompactedTable::new();
        table.ingest(&[
            make_record(Some("a"), Some("1"), 0, 0),
            make_record(Some("b"), Some("2"), 0, 1),
        ]);

        let items: HashMap<&Bytes, &Bytes> =
            (&table).into_iter().map(|(k, e)| (k, &e.value)).collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items[&Bytes::from("a")], &Bytes::from("1"));
    }

    #[test]
    fn test_table_change_classification() {
        // Insert: old=None, new=Some
        let insert = TableChange {
            key: Bytes::from("k"),
            old_value: None,
            new_value: Some(Bytes::from("v")),
            partition: 0,
            offset: 0,
            timestamp: 0,
        };
        assert!(insert.is_insert());
        assert!(!insert.is_update());
        assert!(!insert.is_delete());

        // Update: old=Some, new=Some
        let update = TableChange {
            key: Bytes::from("k"),
            old_value: Some(Bytes::from("old")),
            new_value: Some(Bytes::from("new")),
            partition: 0,
            offset: 1,
            timestamp: 1000,
        };
        assert!(!update.is_insert());
        assert!(update.is_update());
        assert!(!update.is_delete());

        // Delete: new=None
        let delete = TableChange {
            key: Bytes::from("k"),
            old_value: Some(Bytes::from("v")),
            new_value: None,
            partition: 0,
            offset: 2,
            timestamp: 2000,
        };
        assert!(!delete.is_insert());
        assert!(!delete.is_update());
        assert!(delete.is_delete());
    }

    #[test]
    fn test_table_keys() {
        let mut table = CompactedTable::new();
        table.ingest(&[
            make_record(Some("a"), Some("1"), 0, 0),
            make_record(Some("b"), Some("2"), 0, 1),
        ]);

        let mut keys: Vec<&Bytes> = table.keys().collect();
        keys.sort();
        assert_eq!(keys, vec![&Bytes::from("a"), &Bytes::from("b")]);
    }

    #[test]
    fn test_table_values() {
        let mut table = CompactedTable::new();
        table.ingest(&[
            make_record(Some("a"), Some("1"), 0, 0),
            make_record(Some("b"), Some("2"), 0, 1),
        ]);

        let mut values: Vec<&Bytes> = table.values().map(|e| &e.value).collect();
        values.sort();
        assert_eq!(values, vec![&Bytes::from("1"), &Bytes::from("2")]);
    }

    #[test]
    fn test_table_owned_into_iterator() {
        let mut table = CompactedTable::new();
        table.ingest(&[
            make_record(Some("a"), Some("1"), 0, 0),
            make_record(Some("b"), Some("2"), 0, 1),
        ]);

        let items: HashMap<Bytes, Bytes> = table.into_iter().map(|(k, e)| (k, e.value)).collect();
        assert_eq!(items.len(), 2);
        assert_eq!(items.get(&Bytes::from("a")), Some(&Bytes::from("1")));
        assert_eq!(items.get(&Bytes::from("b")), Some(&Bytes::from("2")));
    }

    #[test]
    fn test_table_clone_preserves_state() {
        let mut table = CompactedTable::new();
        table.ingest(&[
            make_record(Some("k1"), Some("v1"), 0, 0),
            make_record(Some("k2"), Some("v2"), 0, 1),
            make_record(Some("k3"), None, 0, 2), // tombstone (key never existed)
        ]);

        let cloned = table.clone();

        assert_eq!(cloned.len(), table.len());
        assert_eq!(cloned.get(b"k1"), table.get(b"k1"));
        assert_eq!(cloned.get(b"k2"), table.get(b"k2"));
        assert_eq!(cloned.records_processed(), table.records_processed());
        assert_eq!(cloned.tombstones_processed(), table.tombstones_processed());
    }

    #[test]
    fn test_table_ingest() {
        let mut table = CompactedTable::new();
        let records = vec![
            make_record(Some("k1"), Some("v1"), 0, 0),
            make_record(Some("k2"), Some("v2"), 0, 1),
            make_record(None, Some("no-key"), 0, 2),
            make_record(Some("k1"), None, 0, 3), // tombstone
        ];

        table.ingest(&records);

        assert_eq!(table.len(), 1);
        assert!(!table.contains_key(b"k1"));
        assert_eq!(table.get_value(b"k2"), Some(&Bytes::from("v2")));
        assert_eq!(table.records_processed(), 4);
        assert_eq!(table.tombstones_processed(), 1);
    }

    #[test]
    fn test_table_ingest_matches_apply_state() {
        let records = vec![
            make_record(Some("a"), Some("1"), 0, 0),
            make_record(Some("b"), Some("2"), 1, 0),
            make_record(Some("a"), Some("3"), 0, 1),
            make_record(Some("b"), None, 1, 1),
        ];

        let mut via_apply = CompactedTable::new();
        let _ = via_apply.apply(&records);

        let mut via_ingest = CompactedTable::new();
        via_ingest.ingest(&records);

        // Both methods must produce identical table state (entries + counters).
        assert_eq!(via_apply, via_ingest);
        assert_eq!(
            via_apply.records_processed(),
            via_ingest.records_processed()
        );
        assert_eq!(
            via_apply.tombstones_processed(),
            via_ingest.tombstones_processed()
        );
    }

    #[test]
    fn test_table_equality_ignores_counters() {
        // Two tables built from different batches but with identical final entries
        // (same key, value, offset, timestamp) must compare equal regardless of
        // how many total records were processed.
        let mut t1 = CompactedTable::new();
        t1.ingest(&[make_record(Some("k"), Some("v"), 0, 5)]);

        let mut t2 = CompactedTable::new();
        t2.ingest(&[
            make_record(None, Some("noise"), 0, 0), // keyless — skipped, but counted
            make_record(Some("k"), Some("v"), 0, 5), // same key/value/offset/ts
        ]);

        // Same entries, different counters.
        assert_eq!(t1, t2);
        assert_ne!(t1.records_processed(), t2.records_processed());
    }

    #[test]
    fn test_table_same_key_lifecycle_in_single_batch() {
        let mut table = CompactedTable::new();
        let records = vec![
            make_record(Some("x"), Some("v1"), 0, 0), // insert
            make_record(Some("x"), Some("v2"), 0, 1), // update
            make_record(Some("x"), None, 0, 2),       // delete
            make_record(Some("x"), Some("v3"), 0, 3), // re-insert
        ];

        let changes = table.apply(&records);

        assert_eq!(table.len(), 1);
        assert_eq!(table.get_value(b"x"), Some(&Bytes::from("v3")));
        assert_eq!(changes.len(), 4);

        // Insert: no previous value
        assert!(changes[0].is_insert());
        assert_eq!(changes[0].old_value, None);
        assert_eq!(changes[0].new_value, Some(Bytes::from("v1")));

        // Update: old_value reflects in-batch state, not just pre-batch
        assert!(changes[1].is_update());
        assert_eq!(changes[1].old_value, Some(Bytes::from("v1")));
        assert_eq!(changes[1].new_value, Some(Bytes::from("v2")));

        // Delete: old_value is the most recent in-batch value
        assert!(changes[2].is_delete());
        assert_eq!(changes[2].old_value, Some(Bytes::from("v2")));

        // Re-insert after in-batch delete: treated as fresh insert
        assert!(changes[3].is_insert());
        assert_eq!(changes[3].old_value, None);
        assert_eq!(changes[3].new_value, Some(Bytes::from("v3")));

        assert_eq!(table.records_processed(), 4);
        assert_eq!(table.tombstones_processed(), 1);
    }

    #[test]
    fn test_all_public_types_are_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CompactedTable>();
        assert_send_sync::<TableChange>();
        assert_send_sync::<CompactedTopicConsumer>();
    }

    /// Required-field validation now comes from `ConsumerBuilder` itself
    /// rather than being duplicated by a second builder, which is the point of
    /// taking one.
    #[tokio::test]
    async fn from_consumer_builder_inherits_consumer_validation() {
        let result =
            CompactedTopicConsumer::from_consumer_builder(Consumer::builder(), "test").await;
        let err = result.expect_err("a builder with no brokers cannot build");
        assert!(
            err.to_string().contains("bootstrap_servers"),
            "expected the consumer's own validation, got: {err}"
        );
    }

    // -----------------------------------------------------------------------
    // Partition-scoped pruning
    // -----------------------------------------------------------------------

    #[test]
    fn test_remove_partitions_removes_only_listed_partitions() {
        let mut table = CompactedTable::new();
        table.ingest(&[
            make_record(Some("p0-a"), Some("v"), 0, 0),
            make_record(Some("p1-a"), Some("v"), 1, 0),
            make_record(Some("p1-b"), Some("v"), 1, 1),
            make_record(Some("p2-a"), Some("v"), 2, 0),
        ]);

        let removed = table.remove_partitions(&[1]);

        assert_eq!(removed, 2);
        assert_eq!(table.len(), 2);
        assert!(table.contains_key(b"p0-a"));
        assert!(table.contains_key(b"p2-a"));
        assert!(!table.contains_key(b"p1-a"));
        assert!(!table.contains_key(b"p1-b"));
        // Lifetime counters describe work done, not current contents.
        assert_eq!(table.records_processed(), 4);
    }

    #[test]
    fn test_remove_partitions_empty_list_is_noop() {
        let mut table = CompactedTable::new();
        table.ingest(&[make_record(Some("k"), Some("v"), 0, 0)]);

        assert_eq!(table.remove_partitions(&[]), 0);
        assert_eq!(table.len(), 1);
    }

    // -----------------------------------------------------------------------
    // CompactedTableClearListener
    // -----------------------------------------------------------------------

    #[derive(Default)]
    struct RecordingRewinder {
        calls: std::sync::Mutex<Vec<(String, PartitionId)>>,
    }

    impl RecordingRewinder {
        fn calls(&self) -> Vec<(String, PartitionId)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl PartitionRewinder for RecordingRewinder {
        fn rewind_to_beginning<'a>(
            &'a self,
            topic: &'a str,
            partition: PartitionId,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
            self.calls
                .lock()
                .unwrap()
                .push((topic.to_string(), partition));
            Box::pin(async { Ok(()) })
        }
    }

    fn populated_table() -> Arc<Mutex<CompactedTable>> {
        let mut table = CompactedTable::new();
        table.ingest(&[
            make_record(Some("p0"), Some("v"), 0, 0),
            make_record(Some("p1"), Some("v"), 1, 0),
            make_record(Some("p2"), Some("v"), 2, 0),
        ]);
        Arc::new(Mutex::new(table))
    }

    #[tokio::test]
    async fn test_listener_revocation_prunes_only_revoked_partitions() {
        let table = populated_table();
        let listener = CompactedTableClearListener::new(Arc::clone(&table));

        listener
            .on_partitions_revoked(&[TopicPartition::new("test-topic", 1)])
            .await;

        let t = table.lock().await;
        assert_eq!(t.len(), 2, "retained partitions must survive revocation");
        assert!(t.contains_key(b"p0"));
        assert!(t.contains_key(b"p2"));
        assert!(!t.contains_key(b"p1"));
    }

    #[tokio::test]
    async fn test_listener_loss_prunes_only_lost_partitions() {
        let table = populated_table();
        let listener = CompactedTableClearListener::new(Arc::clone(&table));

        listener
            .on_partitions_lost(&[TopicPartition::new("test-topic", 2)])
            .await;

        let t = table.lock().await;
        assert_eq!(t.len(), 2);
        assert!(t.contains_key(b"p0"));
        assert!(t.contains_key(b"p1"));
        assert!(!t.contains_key(b"p2"));
    }

    #[tokio::test]
    async fn test_listener_assignment_rewinds_new_partitions() {
        let table = populated_table();
        let rewinder = Arc::new(RecordingRewinder::default());
        let listener = CompactedTableClearListener::with_rewinder(
            Arc::clone(&table),
            Arc::clone(&rewinder) as Arc<dyn PartitionRewinder>,
        );

        listener
            .on_partitions_assigned(&[
                TopicPartition::new("test-topic", 1),
                TopicPartition::new("test-topic", 3),
            ])
            .await;

        assert_eq!(
            rewinder.calls(),
            vec![("test-topic".to_string(), 1), ("test-topic".to_string(), 3)],
            "newly assigned partitions must be replayed from the start"
        );

        // Stale state for a re-gained partition is dropped; untouched
        // partitions keep theirs.
        let t = table.lock().await;
        assert!(!t.contains_key(b"p1"));
        assert!(t.contains_key(b"p0"));
        assert!(t.contains_key(b"p2"));
    }

    #[tokio::test]
    async fn test_listener_assignment_without_consumer_does_not_panic() {
        let table = populated_table();
        let listener = CompactedTableClearListener::new(Arc::clone(&table));

        listener
            .on_partitions_assigned(&[TopicPartition::new("test-topic", 0)])
            .await;

        let t = table.lock().await;
        assert!(!t.contains_key(b"p0"));
        assert_eq!(t.len(), 2);
    }

    #[tokio::test]
    async fn test_listener_empty_rebalance_is_noop() {
        let table = populated_table();
        let listener = CompactedTableClearListener::new(Arc::clone(&table));

        listener.on_partitions_revoked(&[]).await;
        listener.on_partitions_assigned(&[]).await;

        assert_eq!(table.lock().await.len(), 3);
    }

    // -----------------------------------------------------------------------
    // Bounded scan
    // -----------------------------------------------------------------------

    /// A `ScanSource` whose partition positions advance by a fixed amount per
    /// poll, so a test can model both a converging and a permanently stalled
    /// partition.
    struct FakeScanSource {
        assigned: Vec<PartitionId>,
        end_offsets: HashMap<PartitionId, Offset>,
        positions: std::sync::Mutex<HashMap<PartitionId, Offset>>,
        /// Per-partition position increment applied on every poll.
        advance: HashMap<PartitionId, Offset>,
        first_poll_records: std::sync::Mutex<Vec<ConsumerRecord>>,
    }

    impl FakeScanSource {
        fn new(
            assigned: &[PartitionId],
            end_offsets: &[(PartitionId, Offset)],
            positions: &[(PartitionId, Offset)],
            advance: &[(PartitionId, Offset)],
        ) -> Self {
            Self {
                assigned: assigned.to_vec(),
                end_offsets: end_offsets.iter().copied().collect(),
                positions: std::sync::Mutex::new(positions.iter().copied().collect()),
                advance: advance.iter().copied().collect(),
                first_poll_records: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn with_records(self, records: Vec<ConsumerRecord>) -> Self {
            *self.first_poll_records.lock().unwrap() = records;
            self
        }
    }

    impl ScanSource for FakeScanSource {
        async fn poll_records(&self, _timeout: Duration) -> Result<Vec<ConsumerRecord>> {
            let mut positions = self.positions.lock().unwrap();
            for (partition, step) in &self.advance {
                *positions.entry(*partition).or_insert(0) += *step;
            }
            drop(positions);
            Ok(std::mem::take(
                &mut *self.first_poll_records.lock().unwrap(),
            ))
        }

        async fn assigned_partitions(&self, _topic: &str) -> Vec<PartitionId> {
            self.assigned.clone()
        }

        async fn partition_position(&self, _topic: &str, partition: PartitionId) -> Option<Offset> {
            self.positions.lock().unwrap().get(&partition).copied()
        }

        async fn end_offsets(&self, _topic: &str) -> Result<HashMap<PartitionId, Result<Offset>>> {
            Ok(self.end_offsets.iter().map(|(&p, &o)| (p, Ok(o))).collect())
        }
    }

    #[tokio::test]
    async fn test_scan_times_out_and_names_lagging_partitions() {
        // Partition 0 converges immediately; partition 1 never moves and
        // partition 2 has no position at all (e.g. no leader).
        let source = FakeScanSource::new(
            &[0, 1, 2],
            &[(0, 10), (1, 5), (2, 7)],
            &[(0, 10), (1, 2)],
            &[],
        );
        let mut table = CompactedTable::new();

        let err = run_scan(
            &source,
            "test-topic",
            &mut table,
            Duration::from_millis(1),
            Duration::from_millis(30),
        )
        .await
        .expect_err("scan must not run forever when a partition never converges");

        let msg = err.to_string();
        assert!(
            msg.contains("partition 1 (position 2, target 5)"),
            "error must name the stalled partition and its lag: {msg}"
        );
        assert!(
            msg.contains("partition 2 (position unknown, target 7)"),
            "error must name partitions with no known position: {msg}"
        );
        assert!(
            !msg.contains("partition 0"),
            "caught-up partitions must not be reported as lagging: {msg}"
        );
    }

    #[tokio::test]
    async fn test_scan_completes_when_partitions_reach_targets() {
        let source = FakeScanSource::new(&[0], &[(0, 3)], &[(0, 0)], &[(0, 3)])
            .with_records(vec![make_record(Some("k1"), Some("v1"), 0, 0)]);
        let mut table = CompactedTable::new();

        run_scan(
            &source,
            "test-topic",
            &mut table,
            Duration::from_millis(1),
            Duration::from_secs(5),
        )
        .await
        .unwrap();

        assert_eq!(table.get_value(b"k1"), Some(&Bytes::from("v1")));
    }

    #[tokio::test]
    async fn test_scan_requires_an_assignment() {
        let source = FakeScanSource::new(&[], &[], &[], &[]);
        let mut table = CompactedTable::new();

        let err = run_scan(
            &source,
            "test-topic",
            &mut table,
            Duration::from_millis(1),
            Duration::from_millis(10),
        )
        .await
        .unwrap_err();

        assert!(err.to_string().contains("no partitions assigned"));
    }

    #[tokio::test]
    async fn test_scan_returns_early_for_empty_topic() {
        let source = FakeScanSource::new(&[0], &[(0, 0)], &[], &[]);
        let mut table = CompactedTable::new();

        run_scan(
            &source,
            "test-topic",
            &mut table,
            Duration::from_millis(1),
            Duration::from_millis(10),
        )
        .await
        .unwrap();

        assert!(table.is_empty());
    }

    #[tokio::test]
    async fn test_lagging_partitions_ignores_empty_and_unknown_targets() {
        // Partition 0 is empty (target 0), partition 9 is assigned but was not
        // in the snapshot; neither may hold the scan back.
        let source = FakeScanSource::new(&[0, 9], &[], &[], &[]);
        let targets: HashMap<PartitionId, Offset> = [(0, 0)].into_iter().collect();

        assert!(
            lagging_partitions(&source, "test-topic", &targets)
                .await
                .is_empty()
        );
    }

    #[test]
    fn test_scan_timeout_error_lists_every_lagging_partition() {
        let err = scan_timeout_error(
            "cfg",
            Duration::from_secs(2),
            &[
                LaggingPartition {
                    partition: 3,
                    position: Some(7),
                    target: 42,
                },
                LaggingPartition {
                    partition: 4,
                    position: None,
                    target: 1,
                },
            ],
        );

        let msg = err.to_string();
        assert!(msg.contains("cfg"));
        assert!(msg.contains("partition 3 (position 7, target 42)"));
        assert!(msg.contains("partition 4 (position unknown, target 1)"));
    }
}
