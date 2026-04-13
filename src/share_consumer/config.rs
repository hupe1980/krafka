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
    /// before calling `commit_sync()` or `commit_async()`.
    Explicit,
}

/// Acknowledgement type for share group records (KIP-932).
///
/// Used with [`ShareConsumer::acknowledge`](super::ShareConsumer::acknowledge)
/// in explicit acknowledgement mode.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AcknowledgeType {
    /// Accept the record (mark as successfully processed). Wire value: 1.
    Accept = 1,
    /// Release the record (return to the pool for redelivery). Wire value: 2.
    Release = 2,
    /// Reject the record (move to dead-letter topic or discard). Wire value: 3.
    Reject = 3,
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
    /// Maximum poll records returned to the application per `poll()`.
    pub(crate) max_poll_records: i32,
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
    /// When set, topics not refreshed within this duration are evicted to
    /// prevent unbounded cache growth. `None` disables TTL eviction (default).
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
            max_records: 5000,
            batch_size: 500,
            fetch_max_wait_ms: 500,
            request_timeout: Duration::from_secs(30),
            session_timeout: Duration::from_secs(45),
            heartbeat_interval: Duration::from_secs(5),
            client_rack: None,
            metadata_max_age: Duration::from_secs(300),
            metadata_recovery_strategy: MetadataRecoveryStrategy::None,
            metadata_recovery_rebootstrap_trigger: Duration::from_secs(300),
            metadata_topic_cache_ttl: None,
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
