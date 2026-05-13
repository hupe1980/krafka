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
//! use krafka::consumer::CompactedTopicConsumer;
//! use std::time::Duration;
//!
//! let mut ctc = CompactedTopicConsumer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .topic("user-profiles")
//!     .build()
//!     .await?;
//!
//! ctc.scan(Duration::from_secs(1)).await?;
//!
//! if let Some(value) = ctc.table().get(b"user-123") {
//!     println!("User: {:?}", value);
//! }
//! ```

use std::collections::HashMap;
use std::fmt;
use std::time::Duration;

use bytes::Bytes;
use tracing::{debug, info};

use super::record::ConsumerRecord;
use super::{AutoOffsetReset, Consumer};
use crate::auth::AuthConfig;
use crate::error::{KrafkaError, Result};
use crate::{Offset, PartitionId, Timestamp};

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
    /// The key→value entries.
    entries: HashMap<Bytes, Bytes>,
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

    /// Get the current value for a key.
    ///
    /// Returns `None` if the key is not in the table (never seen or deleted
    /// by a tombstone).
    pub fn get(&self, key: &[u8]) -> Option<&Bytes> {
        self.entries.get(key)
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

    /// Iterate over all key→value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&Bytes, &Bytes)> {
        self.entries.iter()
    }

    /// Iterate over all keys in the table.
    pub fn keys(&self) -> impl Iterator<Item = &Bytes> {
        self.entries.keys()
    }

    /// Iterate over all values in the table.
    pub fn values(&self) -> impl Iterator<Item = &Bytes> {
        self.entries.values()
    }

    /// Get a snapshot (clone) of the key→value entries.
    ///
    /// Returns only the entries, without counters. Use
    /// [`Clone::clone()`] if you need a full copy including
    /// `records_processed` and `tombstones_processed`.
    #[must_use]
    pub fn snapshot(&self) -> HashMap<Bytes, Bytes> {
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
            let old_value = self.entries.remove(key.as_ref());
            TableChange {
                key: key.clone(),
                old_value,
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
            let old_value = self.entries.insert(key_owned.clone(), value.clone());
            TableChange {
                key: key_owned,
                old_value,
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
            self.entries.insert(key.clone(), value);
        }
    }

    /// Clear the table, removing all entries and resetting counters.
    ///
    /// Useful when partitions are revoked during a consumer group rebalance
    /// and the table needs to be rebuilt from scratch.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.records_processed = 0;
        self.tombstones_processed = 0;
    }
}

impl<'a> IntoIterator for &'a CompactedTable {
    type Item = (&'a Bytes, &'a Bytes);
    type IntoIter = std::collections::hash_map::Iter<'a, Bytes, Bytes>;

    fn into_iter(self) -> Self::IntoIter {
        self.entries.iter()
    }
}

impl IntoIterator for CompactedTable {
    type Item = (Bytes, Bytes);
    type IntoIter = std::collections::hash_map::IntoIter<Bytes, Bytes>;

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
    fn eq(&self, other: &Self) -> bool {
        self.entries == other.entries
    }
}

impl Eq for CompactedTable {}

// ---------------------------------------------------------------------------
// CompactedTopicConsumer — convenience wrapper
// ---------------------------------------------------------------------------

/// Convenience wrapper that pairs a [`Consumer`] with a [`CompactedTable`]
/// for the common pattern of scanning an entire compacted topic.
///
/// When constructed via [`builder()`](Self::builder), it creates a
/// standalone (no group) consumer, assigns all partitions from the earliest
/// offset, and provides [`scan()`](Self::scan) to block until the table is
/// fully populated.
///
/// Other constructors, such as [`from_consumer()`](Self::from_consumer),
/// use the caller-provided consumer configuration and assignment as-is.
///
/// For fully custom consumer setups, you can also use [`CompactedTable`]
/// directly with your own [`Consumer`].
///
/// # Example
///
/// ```rust,ignore
/// use krafka::consumer::CompactedTopicConsumer;
/// use std::time::Duration;
///
/// let mut ctc = CompactedTopicConsumer::builder()
///     .bootstrap_servers("localhost:9092")
///     .topic("user-profiles")
///     .build()
///     .await?;
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
    /// Create a new builder.
    pub fn builder() -> CompactedTopicConsumerBuilder {
        CompactedTopicConsumerBuilder::default()
    }

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
    /// can change over time, note that [`CompactedTable`] is not
    /// automatically pruned when partitions are revoked. Keys loaded from
    /// partitions that are no longer assigned will remain in the table until
    /// you clear or rebuild it (e.g., call [`table_mut().clear()`](CompactedTable::clear)
    /// from a rebalance callback when assignments change).
    ///
    /// Use this when you need full control over the consumer configuration
    /// (TLS, auth, timeouts, etc.) beyond what the builder exposes.
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
    /// Because the high watermark is refreshed on each fetch, it can keep
    /// advancing while this scan is in progress. On actively written topics,
    /// `scan()` may therefore block indefinitely and should be treated as a
    /// best-effort catch-up operation rather than a bounded snapshot scan.
    ///
    /// If this call returns, [`is_caught_up()`](Self::is_caught_up) is `true`
    /// and the table contains the latest value for every live key observed up
    /// to the point where the consumer determined it had caught up.
    ///
    /// # Errors
    ///
    /// Returns an error if any poll fails unrecoverably.
    pub async fn scan(&mut self, poll_timeout: Duration) -> Result<()> {
        // Fail fast if no partitions are assigned — avoids an infinite loop
        // of empty polls (especially when using from_consumer() without assign()).
        let assignments = self.consumer.assignment().await;
        if assignments.get(&self.topic).is_none_or(|p| p.is_empty()) {
            return Err(KrafkaError::invalid_state(format!(
                "no partitions assigned for topic '{}'; \
                 assign partitions before calling scan()",
                self.topic
            )));
        }

        info!("Starting compacted topic scan for '{}'", self.topic);

        loop {
            let mut records = self.consumer.poll(poll_timeout).await?;
            let before_len = records.len();
            records.retain(|r| r.topic == self.topic);
            let filtered = before_len - records.len();
            if filtered > 0 {
                debug!(
                    "Filtered out {} record(s) from other topics during scan for '{}'",
                    filtered, self.topic
                );
            }
            self.table.ingest(&records);

            if self.check_caught_up().await {
                self.caught_up = true;
                info!(
                    "Compacted topic scan complete for '{}': {} keys, {} records processed, \
                     {} tombstones",
                    self.topic,
                    self.table.len(),
                    self.table.records_processed(),
                    self.table.tombstones_processed(),
                );
                return Ok(());
            }
        }
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

    /// Check if all assigned partitions have reached their high watermarks.
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

/// Builder for [`CompactedTopicConsumer`].
#[derive(Default)]
pub struct CompactedTopicConsumerBuilder {
    bootstrap_servers: Option<String>,
    topic: Option<String>,
    client_id: Option<String>,
    request_timeout: Option<Duration>,
    fetch_max_bytes: Option<i32>,
    max_partition_fetch_bytes: Option<i32>,
    max_poll_records: Option<i32>,
    auth: Option<AuthConfig>,
    #[cfg(feature = "socks5")]
    proxy: Option<crate::network::ProxyConfig>,
}

impl CompactedTopicConsumerBuilder {
    /// Set the Kafka bootstrap servers (required).
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.bootstrap_servers = Some(servers.into());
        self
    }

    /// Set the compacted topic to consume (required).
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.topic = Some(topic.into());
        self
    }

    /// Set the client ID sent to the broker.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Set the request timeout for broker RPCs.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = Some(timeout);
        self
    }

    /// Set the maximum bytes to fetch per request.
    pub fn fetch_max_bytes(mut self, bytes: i32) -> Self {
        self.fetch_max_bytes = Some(bytes);
        self
    }

    /// Set the maximum bytes to fetch per partition per request.
    pub fn max_partition_fetch_bytes(mut self, bytes: i32) -> Self {
        self.max_partition_fetch_bytes = Some(bytes);
        self
    }

    /// Set the maximum number of records returned per poll.
    pub fn max_poll_records(mut self, max: i32) -> Self {
        self.max_poll_records = Some(max);
        self
    }

    /// Set authentication configuration.
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.auth = Some(auth);
        self
    }

    /// Set SOCKS5 proxy configuration.
    ///
    /// Routes all broker connections through the specified SOCKS5 proxy.
    #[cfg(feature = "socks5")]
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.proxy = Some(proxy);
        self
    }

    /// Build the [`CompactedTopicConsumer`].
    ///
    /// Creates an internal [`Consumer`] in standalone mode (no consumer group),
    /// discovers all partitions for the topic, and assigns them starting from
    /// the earliest available offset.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `bootstrap_servers` or `topic` is not set.
    /// - The broker is unreachable or the topic does not exist.
    pub async fn build(self) -> Result<CompactedTopicConsumer> {
        let bootstrap_servers = self
            .bootstrap_servers
            .ok_or_else(|| KrafkaError::config("bootstrap_servers is required"))?;
        let topic = self
            .topic
            .ok_or_else(|| KrafkaError::config("topic is required for CompactedTopicConsumer"))?;

        let mut consumer_builder = Consumer::builder()
            .bootstrap_servers(&bootstrap_servers)
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false);

        if let Some(client_id) = self.client_id {
            consumer_builder = consumer_builder.client_id(client_id);
        }
        if let Some(timeout) = self.request_timeout {
            consumer_builder = consumer_builder.request_timeout(timeout);
        }
        if let Some(bytes) = self.fetch_max_bytes {
            consumer_builder = consumer_builder.fetch_max_bytes(bytes);
        }
        if let Some(bytes) = self.max_partition_fetch_bytes {
            consumer_builder = consumer_builder.max_partition_fetch_bytes(bytes);
        }
        if let Some(max) = self.max_poll_records {
            consumer_builder = consumer_builder.max_poll_records(max);
        }
        if let Some(auth) = self.auth {
            consumer_builder = consumer_builder.auth(auth);
        }
        #[cfg(feature = "socks5")]
        if let Some(proxy) = self.proxy {
            consumer_builder = consumer_builder.proxy(proxy);
        }

        let consumer = consumer_builder.build().await?;

        // Refresh metadata to get the latest partition count for the topic.
        // Consumer::build() fetches an initial snapshot, but it may already
        // be slightly stale if the topic was recently expanded.
        consumer
            .metadata
            .refresh_for_topics(Some(&[&topic]))
            .await?;

        // Discover partitions and assign all of them
        let partition_count = consumer.metadata.partition_count(&topic).ok_or_else(|| {
            KrafkaError::config(format!("topic '{topic}' not found in cluster metadata"))
        })?;

        let partition_count = PartitionId::try_from(partition_count).map_err(|_| {
            KrafkaError::config(format!(
                "topic '{topic}' has too many partitions to fit in PartitionId"
            ))
        })?;

        let partitions: Vec<PartitionId> = (0..partition_count).collect();
        consumer.assign(&topic, partitions).await?;

        info!(
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
        assert_eq!(table.get(b"k1"), Some(&Bytes::from("v1")));
        assert_eq!(table.get(b"k2"), Some(&Bytes::from("v2")));
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
        assert_eq!(table.get(b"k1"), Some(&Bytes::from("new")));
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
        assert_eq!(table.get(b"k2"), Some(&Bytes::from("v2")));
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
        assert_eq!(table.get(b"user-1"), Some(&Bytes::from("Alice V2")));
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
        assert_eq!(table.get(b"k1"), Some(&Bytes::from("v1-updated")));
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

        let items: HashMap<&Bytes, &Bytes> = table.iter().collect();
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
        assert_eq!(snap.get(&Bytes::from("k1")), Some(&Bytes::from("v1")));
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

        let items: HashMap<&Bytes, &Bytes> = (&table).into_iter().collect();
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

        let mut values: Vec<&Bytes> = table.values().collect();
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

        let items: HashMap<Bytes, Bytes> = table.into_iter().collect();
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
        assert_eq!(table.get(b"k2"), Some(&Bytes::from("v2")));
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
        let mut t1 = CompactedTable::new();
        t1.ingest(&[make_record(Some("k"), Some("v"), 0, 0)]);

        let mut t2 = CompactedTable::new();
        t2.ingest(&[
            make_record(None, Some("noise"), 0, 0), // keyless — skipped, but counted
            make_record(Some("k"), Some("v"), 0, 1),
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
        assert_eq!(table.get(b"x"), Some(&Bytes::from("v3")));
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
        assert_send_sync::<CompactedTopicConsumerBuilder>();
    }

    #[tokio::test]
    async fn test_builder_missing_bootstrap_servers() {
        let result = CompactedTopicConsumerBuilder::default()
            .topic("test")
            .build()
            .await;
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("bootstrap_servers")
        );
    }

    #[tokio::test]
    async fn test_builder_missing_topic() {
        let result = CompactedTopicConsumerBuilder::default()
            .bootstrap_servers("localhost:9092")
            .build()
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("topic"));
    }
}
