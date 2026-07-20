//! Share consumer configuration (KIP-932).

use std::time::Duration;

use crate::auth::AuthConfig;
use crate::metadata::MetadataRecoveryStrategy;

/// Acknowledgement mode for share consumers (KIP-932).
///
/// Controls whether record acknowledgements are sent explicitly by the
/// application or implicitly when the next `poll()` is called.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AcknowledgementMode {
    /// Records are automatically acknowledged (accepted) on the next
    /// `poll()` call. This is the default and simplest mode.
    #[default]
    Implicit,
    /// The application must explicitly acknowledge each record via
    /// [`ShareConsumer::acknowledge`](super::ShareConsumer::acknowledge)
    /// before calling `commit_sync()` or awaiting `commit_async()`.
    Explicit,
}

/// Acknowledgement type for share group records (KIP-932).
///
/// Used with [`ShareConsumer::acknowledge`](super::ShareConsumer::acknowledge)
/// in explicit acknowledgement mode.
///
/// The discriminants are the wire values from KIP-932 (extended by KIP-1222):
/// `1 = Accept`, `2 = Release`, `3 = Reject`, `4 = Renew`. Wire value `0` is
/// "gap", which applications never choose — the client emits it on their behalf
/// for offsets it acquired but could not decode, so the broker archives them
/// instead of redelivering them forever.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcknowledgeType {
    /// Accept the record (mark as successfully processed). Wire value: 1.
    Accept = 1,
    /// Release the record (return to the pool for redelivery). Wire value: 2.
    Release = 2,
    /// Reject the record (move to dead-letter topic or discard). Wire value: 3.
    Reject = 3,
    /// Renew the acquisition lock, extending the delivery deadline without
    /// completing the record (KIP-1222, Kafka 4.2+). Wire value: 4.
    ///
    /// Use this for long-running processing that would otherwise exceed
    /// `group.share.record.lock.duration.ms` and have the record redelivered
    /// to another consumer.
    ///
    /// Brokers older than Kafka 4.2 do not understand this type and reject the
    /// **entire** acknowledgement batch with `INVALID_REQUEST`. The client
    /// therefore drops `Renew` acknowledgements when the negotiated
    /// `ShareFetch`/`ShareAcknowledge` version is below 2, logging a warning;
    /// the acquisition lock then simply expires, which is the same outcome as
    /// not renewing at all.
    Renew = 4,
}

impl AcknowledgeType {
    /// Convert to the wire protocol value.
    #[inline]
    pub fn to_i8(self) -> i8 {
        self as i8
    }
}

/// Share consumer configuration.
///
/// Constructed via [`ShareConsumer::builder()`](super::ShareConsumer::builder).
/// Fields are deliberately `pub(crate)` to enforce construction through the
/// builder, which validates required fields.
#[non_exhaustive]
#[derive(Debug, Clone)]
pub struct ShareConsumerConfig {
    /// Bootstrap servers (comma-separated).
    pub(crate) bootstrap_servers: String,
    /// Share group ID (required).
    pub(crate) group_id: String,
    /// Client ID.
    pub(crate) client_id: String,
    /// Acknowledgement mode.
    pub(crate) acknowledgement_mode: AcknowledgementMode,
    /// Minimum bytes to fetch.
    pub(crate) fetch_min_bytes: i32,
    /// Maximum bytes to fetch.
    pub(crate) fetch_max_bytes: i32,
    /// Maximum records returned to the application per `poll()`. Must be >= 1.
    /// Defaults to 500.
    pub(crate) max_poll_records: i32,
    /// Maximum records buffered internally by [`recv()`](super::ShareConsumer::recv).
    ///
    /// This is a soft threshold: when the buffer is at/above this value,
    /// `poll()` skips new fetches until the buffer drains. A single `recv()`
    /// call may buffer more records than the threshold due to batched fetch
    /// responses. Set to `0` to disable the cap (unlimited). Defaults to 500.
    pub(crate) max_buffered_records: i32,
    /// Maximum records to acquire per fetch request.
    pub(crate) max_records: i32,
    /// Optimal batch size for acquired records.
    pub(crate) batch_size: i32,
    /// Maximum wait time for fetch requests.
    pub(crate) fetch_max_wait_ms: i32,
    /// Request timeout.
    pub(crate) request_timeout: Duration,
    /// Session timeout for share group membership.
    pub(crate) session_timeout: Duration,
    /// Heartbeat interval.
    pub(crate) heartbeat_interval: Duration,
    /// Client rack ID for closest-replica fetching (KIP-392).
    pub(crate) client_rack: Option<String>,
    /// Metadata max age.
    pub(crate) metadata_max_age: Duration,
    /// Metadata recovery strategy (KIP-899).
    pub(crate) metadata_recovery_strategy: MetadataRecoveryStrategy,
    /// Rebootstrap trigger duration (KIP-899).
    pub(crate) metadata_recovery_rebootstrap_trigger: Duration,
    /// Maximum age of cached topic entries during partial metadata refreshes.
    /// Topics not refreshed within this duration are evicted to prevent
    /// unbounded cache growth. Defaults to 5 minutes, matching the Java
    /// client's `metadata.max.idle.ms`. `None` disables TTL eviction.
    pub(crate) metadata_topic_cache_ttl: Option<Duration>,
    /// Authentication configuration.
    pub(crate) auth: Option<AuthConfig>,
    /// Maximum decompressed size for record batches (compression bomb protection).
    /// Defaults to [`RecordBatch::MAX_DECOMPRESSED_SIZE`](crate::protocol::RecordBatch::MAX_DECOMPRESSED_SIZE) (128 MiB).
    pub(crate) max_decompressed_size: usize,
    /// SOCKS5 proxy configuration.
    #[cfg(feature = "socks5")]
    pub(crate) proxy: Option<crate::network::ProxyConfig>,
}

impl Default for ShareConsumerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: String::new(),
            group_id: String::new(),
            client_id: "krafka".to_string(),
            acknowledgement_mode: AcknowledgementMode::Implicit,
            fetch_min_bytes: 1,
            fetch_max_bytes: 52_428_800, // 50 MiB
            max_poll_records: 500,
            max_buffered_records: 500,
            max_records: 5000,
            batch_size: 500,
            fetch_max_wait_ms: 500,
            request_timeout: Duration::from_secs(30),
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(5),
            client_rack: None,
            metadata_max_age: Duration::from_secs(300),
            metadata_recovery_strategy: MetadataRecoveryStrategy::Rebootstrap,
            metadata_recovery_rebootstrap_trigger: Duration::from_secs(300),
            metadata_topic_cache_ttl: Some(Duration::from_secs(300)),
            auth: None,
            max_decompressed_size: crate::protocol::RecordBatch::MAX_DECOMPRESSED_SIZE,
            #[cfg(feature = "socks5")]
            proxy: None,
        }
    }
}

impl ShareConsumerConfig {
    /// Returns the bootstrap servers.
    #[inline]
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Returns the share group ID.
    #[inline]
    pub fn group_id(&self) -> &str {
        &self.group_id
    }

    /// Returns the client ID.
    #[inline]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Returns the acknowledgement mode.
    #[inline]
    pub fn acknowledgement_mode(&self) -> AcknowledgementMode {
        self.acknowledgement_mode
    }

    /// Returns the session timeout.
    #[inline]
    pub fn session_timeout(&self) -> Duration {
        self.session_timeout
    }

    /// Returns the heartbeat interval.
    #[inline]
    pub fn heartbeat_interval(&self) -> Duration {
        self.heartbeat_interval
    }
}
