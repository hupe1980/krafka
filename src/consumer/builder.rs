//! Consumer builder.
//!
//! This module provides [`ConsumerBuilder`], which is the primary entry point
//! for constructing a [`Consumer`](super::Consumer).  Obtain a builder via
//! [`Consumer::builder()`](super::Consumer::builder).

use std::sync::Arc;
use std::time::Duration;

use ahash::AHashMap as HashMap;

use super::group::ErasedRebalanceListener;
use super::{
    AutoOffsetReset, Consumer, ConsumerConfig, ConsumerRebalanceListener, IsolationLevel,
    PartitionAssignmentStrategy,
};
use crate::auth::AuthConfig;
use crate::error::{KrafkaError, Result};
use crate::metadata::ClusterMetadata;
use crate::network::ConnectionPool;
use crate::{Offset, PartitionId};

/// Builder for creating consumers.
#[derive(Default)]
#[must_use = "builders do nothing until .build() is called"]
pub struct ConsumerBuilder {
    config: ConsumerConfig,
    rebalance_listener: Option<Arc<dyn ErasedRebalanceListener>>,
    interceptors: Vec<Arc<dyn crate::interceptor::ConsumerInterceptor>>,
    key_decoder: Option<Arc<dyn crate::schema_registry::SchemaDecoder>>,
    value_decoder: Option<Arc<dyn crate::schema_registry::SchemaDecoder>>,
    /// Pre-built pool and metadata from a [`KrafkaClient`](crate::client::KrafkaClient).
    shared: Option<(Arc<ConnectionPool>, Arc<ClusterMetadata>)>,
}

impl ConsumerBuilder {
    /// Set the bootstrap servers.
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.config.bootstrap_servers = servers.into();
        self
    }

    /// Set the group ID.
    pub fn group_id(mut self, group_id: impl Into<String>) -> Self {
        self.config.group_id = Some(group_id.into());
        self
    }

    /// Set the client ID.
    pub fn client_id(mut self, client_id: impl Into<String>) -> Self {
        self.config.client_id = client_id.into();
        self
    }

    /// Set auto offset reset behavior.
    pub fn auto_offset_reset(mut self, reset: AutoOffsetReset) -> Self {
        self.config.auto_offset_reset = reset;
        self
    }

    /// Enable auto commit.
    pub fn enable_auto_commit(mut self, enable: bool) -> Self {
        self.config.enable_auto_commit = enable;
        self
    }

    /// Set auto commit interval.
    pub fn auto_commit_interval(mut self, interval: Duration) -> Self {
        self.config.auto_commit_interval = interval;
        self
    }

    /// Set fetch minimum bytes.
    pub fn fetch_min_bytes(mut self, bytes: i32) -> Self {
        self.config.fetch_min_bytes = bytes;
        self
    }

    /// Set fetch maximum bytes.
    pub fn fetch_max_bytes(mut self, bytes: i32) -> Self {
        self.config.fetch_max_bytes = bytes;
        self
    }

    /// Set max partition fetch bytes.
    pub fn max_partition_fetch_bytes(mut self, bytes: i32) -> Self {
        self.config.max_partition_fetch_bytes = bytes;
        self
    }

    /// Override the per-partition fetch byte limit for a specific topic.
    pub fn topic_fetch_max_bytes(mut self, topic: impl Into<String>, bytes: i32) -> Self {
        self.config
            .topic_fetch_max_bytes
            .insert(topic.into(), bytes);
        self
    }

    /// Set maximum poll records per poll() call.
    pub fn max_poll_records(mut self, max: i32) -> Self {
        self.config.max_poll_records = max;
        self
    }

    /// Set the maximum number of records buffered internally by
    /// [`recv()`](super::Consumer::recv).
    ///
    /// When the buffer reaches this limit, `poll()` stops fetching until it
    /// drains, bounding memory when the application consumes more slowly than
    /// the broker delivers. `0` disables the cap. Defaults to 500.
    pub fn max_buffered_records(mut self, max: i32) -> Self {
        self.config.max_buffered_records = max;
        self
    }

    /// Set how long the broker may hold a fetch request waiting for
    /// `fetch_min_bytes` to accumulate.
    ///
    /// Independent of the [`poll()`](super::Consumer::poll) timeout: `poll()`
    /// issues fetches in a loop until its own deadline, so a short value here
    /// still supports long polling. Defaults to 500 ms, matching Java's
    /// `fetch.max.wait.ms`.
    pub fn fetch_max_wait(mut self, wait: Duration) -> Self {
        self.config.fetch_max_wait = wait;
        self
    }

    /// Set maximum poll interval before consumer is considered dead.
    pub fn max_poll_interval(mut self, interval: Duration) -> Self {
        self.config.max_poll_interval = interval;
        self
    }

    /// Set the request timeout: how long one in-flight request may wait for its
    /// response. Default: 30 s.
    ///
    /// Must be at least [`connect_timeout`](Self::connect_timeout), whose
    /// default is 10 s — a request's clock covers establishing the connection
    /// it is sent over, so a shorter value would expire every request before
    /// the handshake could finish. To go below 10 s, lower `connect_timeout`
    /// as well; `build()` returns a config error otherwise.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
        self
    }

    /// Set the connect timeout: how long TCP establishment to one broker may
    /// take. Default: 10 s.
    ///
    /// This also acts as the floor on
    /// [`request_timeout`](Self::request_timeout), so lowering it is what makes
    /// a sub-10-second request timeout possible.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.config.connect_timeout = timeout;
        self
    }

    /// Set session timeout for consumer groups.
    pub fn session_timeout(mut self, timeout: Duration) -> Self {
        self.config.session_timeout = timeout;
        self
    }

    /// Set heartbeat interval.
    pub fn heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    /// Set isolation level.
    pub fn isolation_level(mut self, level: IsolationLevel) -> Self {
        self.config.isolation_level = level;
        self
    }

    /// Set a single partition assignment strategy for consumer groups,
    /// replacing the default preference list.
    ///
    /// Pinning the group to one protocol means it cannot be migrated to a
    /// different rebalance protocol without a full group restart; prefer
    /// [`partition_assignment_strategies`](Self::partition_assignment_strategies)
    /// where that matters.
    pub fn partition_assignment_strategy(mut self, strategy: PartitionAssignmentStrategy) -> Self {
        self.config.partition_assignment_strategies = vec![strategy];
        self
    }

    /// Set the partition assignment strategies in order of preference.
    ///
    /// All are advertised in JoinGroup; the coordinator selects the
    /// most-preferred protocol that every member of the group supports. The
    /// default is `[Range, CooperativeSticky]`, which allows a group to move
    /// from the eager to the cooperative protocol in a single rolling bounce.
    pub fn partition_assignment_strategies(
        mut self,
        strategies: impl IntoIterator<Item = PartitionAssignmentStrategy>,
    ) -> Self {
        self.config.partition_assignment_strategies = strategies.into_iter().collect();
        self
    }

    /// Set the static group membership instance ID (KIP-345).
    ///
    /// When configured, the consumer uses static group membership. The broker
    /// preserves partition assignments across restarts as long as the same
    /// instance ID is used, avoiding unnecessary rebalances.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let consumer = Consumer::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .group_id("my-group")
    ///     .group_instance_id("instance-1")
    ///     .build()
    ///     .await?;
    /// ```
    pub fn group_instance_id(mut self, id: impl Into<String>) -> Self {
        self.config.group_instance_id = Some(id.into());
        self
    }

    /// Set metadata max age before forcing a refresh.
    pub fn metadata_max_age(mut self, age: Duration) -> Self {
        self.config.metadata_max_age = age;
        self
    }

    /// Set the high-watermark staleness threshold used by [`Consumer::lag`](super::Consumer::lag).
    ///
    /// A partition's high watermark is considered stale when it has not been
    /// refreshed within this duration. Stale partitions are reported in
    /// [`LagResult::stale_partitions`](super::LagResult::stale_partitions) so callers can decide whether to trust
    /// the lag value.
    ///
    /// Default: 60 seconds.
    pub fn lag_staleness_threshold(mut self, threshold: Duration) -> Self {
        self.config.lag_staleness_threshold = threshold;
        self
    }

    /// Set the client rack ID for closest-replica fetching (KIP-392).
    ///
    /// When configured, the consumer includes its rack in fetch requests.
    /// The broker may return a preferred read replica in the same rack,
    /// reducing cross-rack network traffic.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let consumer = Consumer::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .group_id("my-group")
    ///     .client_rack("us-east-1a")
    ///     .build()
    ///     .await?;
    /// ```
    pub fn client_rack(mut self, rack: impl Into<String>) -> Self {
        self.config.client_rack = Some(rack.into());
        self
    }

    /// Select the consumer group protocol.
    ///
    /// [`GroupProtocol::Consumer`](super::GroupProtocol::Consumer) — the
    /// KIP-848 protocol, where the coordinator computes assignments
    /// server-side and `ConsumerGroupHeartbeat` is the sole membership channel
    /// — is the **recommended** choice. It has been production ready since
    /// Apache Kafka 4.0.
    ///
    /// [`GroupProtocol::Classic`](super::GroupProtocol::Classic) remains the
    /// default so that upgrading krafka is never itself a protocol migration,
    /// but Apache Kafka 4.3 has begun deprecating it (KIP-1274) and krafka
    /// logs a one-time warning when a group starts on it.
    pub fn group_protocol(mut self, protocol: super::GroupProtocol) -> Self {
        self.config.group_protocol = protocol;
        self
    }

    /// Set the maximum decompressed size for a single record batch.
    ///
    /// Compressed payloads that decompress beyond this limit are rejected as
    /// potential compression bombs. Lower it when consuming from a topic whose
    /// producers are not fully trusted; the default is 128 MiB.
    pub fn max_decompressed_size(mut self, size: usize) -> Self {
        self.config.max_decompressed_size = size;
        self
    }

    /// Set the metadata recovery strategy (KIP-1102).
    ///
    /// Controls what the client does when every known broker becomes
    /// unreachable: keep retrying the cached broker set, or fall back to the
    /// original bootstrap servers.
    pub fn metadata_recovery_strategy(
        mut self,
        strategy: crate::metadata::MetadataRecoveryStrategy,
    ) -> Self {
        self.config.metadata_recovery_strategy = strategy;
        self
    }

    /// How long metadata must stay unrefreshable before a rebootstrap fires.
    ///
    /// Only effective with
    /// [`MetadataRecoveryStrategy::Rebootstrap`](crate::metadata::MetadataRecoveryStrategy::Rebootstrap).
    pub fn metadata_recovery_rebootstrap_trigger(mut self, duration: Duration) -> Self {
        self.config.metadata_recovery_rebootstrap_trigger = duration;
        self
    }

    /// Set the maximum number of cooperative-rebalance rejoin rounds per poll.
    ///
    /// Bounds the work one `poll()` will do converging a cooperative
    /// rebalance. Default: 10; values below 1 are clamped to 1.
    pub fn max_cooperative_rebalance_rounds(mut self, rounds: usize) -> Self {
        self.config.max_cooperative_rebalance_rounds = rounds.max(1);
        self
    }

    /// Set how long `poll()` sleeps when there is nothing to do.
    ///
    /// Smaller values reduce latency when records arrive during the sleep
    /// window, at the cost of CPU under sustained idle. Default: 10 ms.
    pub fn idle_poll_backoff(mut self, backoff: Duration) -> Self {
        self.config.idle_poll_backoff = backoff;
        self
    }

    /// Set the maximum time allowed for the `on_partitions_revoked` callback.
    ///
    /// If the callback exceeds this duration the consumer logs a warning and
    /// proceeds with the rebalance rather than stalling the group. Default: 5 s.
    pub fn revocation_timeout(mut self, timeout: Duration) -> Self {
        self.config.revocation_timeout = timeout;
        self
    }

    /// Set a rebalance listener to be notified of partition assignment changes.
    pub fn rebalance_listener(
        mut self,
        listener: impl ConsumerRebalanceListener + 'static,
    ) -> Self {
        self.rebalance_listener = Some(Arc::new(listener));
        self
    }

    /// Set authentication configuration.
    ///
    /// Enables TLS and/or SASL authentication for all broker connections.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use krafka::consumer::Consumer;
    /// use krafka::auth::AuthConfig;
    ///
    /// let consumer = Consumer::builder()
    ///     .bootstrap_servers("broker:9093")
    ///     .group_id("my-group")
    ///     .auth(AuthConfig::sasl_plain("user", "password")?)
    ///     .build()
    ///     .await?;
    /// ```
    pub fn auth(mut self, auth: AuthConfig) -> Self {
        self.config.auth = Some(auth);
        self
    }

    /// Set SOCKS5 proxy configuration.
    ///
    /// Routes all broker connections through the specified SOCKS5 proxy.
    #[cfg(feature = "socks5")]
    pub fn proxy(mut self, proxy: crate::network::ProxyConfig) -> Self {
        self.config.proxy = Some(proxy);
        self
    }

    /// Set socket- and pool-level transport tuning.
    ///
    /// Covers TCP keepalive and nodelay, the per-connection response ceiling
    /// and in-flight cap, the priority-channel depths, the Happy Eyeballs
    /// stagger, idle-connection eviction, a total-connection cap, and the
    /// KIP-1288 automatic TLS reload interval.
    ///
    /// Omitting this call keeps krafka's historical defaults, which
    /// [`TransportConfig::default`](crate::network::TransportConfig) reproduces
    /// exactly.
    pub fn transport(mut self, transport: crate::network::TransportConfig) -> Self {
        self.config.transport = transport;
        self
    }

    /// Configure SASL/PLAIN authentication.
    pub fn sasl_plain(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> crate::Result<Self> {
        self.config.auth = Some(AuthConfig::sasl_plain(username, password)?);
        Ok(self)
    }

    /// Configure SASL/SCRAM-SHA-256 authentication.
    pub fn sasl_scram_sha256(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_scram_sha256(username, password));
        self
    }

    /// Configure SASL/SCRAM-SHA-512 authentication.
    pub fn sasl_scram_sha512(
        mut self,
        username: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_scram_sha512(username, password));
        self
    }

    /// Configure SASL/OAUTHBEARER authentication with a static token.
    ///
    /// For automatic token refresh, use [`sasl_oauthbearer_provider()`](Self::sasl_oauthbearer_provider).
    /// For SASL extensions, use `.auth(AuthConfig::sasl_oauthbearer_token(...))`.
    pub fn sasl_oauthbearer(mut self, token: impl Into<String>) -> Self {
        self.config.auth = Some(AuthConfig::sasl_oauthbearer(token));
        self
    }

    /// Configure SASL/OAUTHBEARER authentication with an async token provider.
    ///
    /// The provider is called on every new broker connection, ensuring
    /// tokens are always fresh.
    pub fn sasl_oauthbearer_provider(
        mut self,
        provider: impl crate::auth::OAuthBearerTokenProvider + 'static,
    ) -> Self {
        self.config.auth = Some(AuthConfig::sasl_oauthbearer_provider(provider));
        self
    }

    /// Set a consumer interceptor, replacing any previously added interceptors.
    ///
    /// The interceptor's `on_consume` method is called after records are fetched
    /// but before they are returned from `poll()`, and `on_commit` is called
    /// after offsets are committed.
    ///
    /// To register multiple interceptors as an ordered chain, use
    /// [`add_interceptor`](Self::add_interceptor) instead.
    pub fn interceptor(
        mut self,
        interceptor: Arc<dyn crate::interceptor::ConsumerInterceptor>,
    ) -> Self {
        self.interceptors = vec![interceptor];
        self
    }

    /// Append a consumer interceptor to the chain.
    ///
    /// Interceptors execute in the order they are added. Each interceptor is
    /// individually panic-isolated — a panic in one will not prevent the
    /// remaining interceptors from running.
    pub fn add_interceptor(
        mut self,
        interceptor: Arc<dyn crate::interceptor::ConsumerInterceptor>,
    ) -> Self {
        self.interceptors.push(interceptor);
        self
    }

    /// Set the topic cache TTL for partial metadata refreshes.
    ///
    /// During partial refreshes, cached topics that have not been refreshed
    /// within this duration are evicted to prevent unbounded cache growth.
    ///
    /// Default: 5 minutes (matching Java's `metadata.max.idle.ms`).
    pub fn metadata_topic_cache_ttl(mut self, ttl: Duration) -> Self {
        self.config.metadata_topic_cache_ttl = Some(ttl);
        self
    }

    /// Disable topic cache TTL eviction for partial metadata refreshes.
    ///
    /// By default, cached topics are evicted after 5 minutes to prevent
    /// unbounded growth on topic churn. Call this to opt out of TTL eviction;
    /// entries will then persist across partial refreshes indefinitely.
    pub fn disable_metadata_topic_cache_ttl(mut self) -> Self {
        self.config.metadata_topic_cache_ttl = None;
        self
    }

    /// Set per-partition initial offsets applied before auto-offset-reset.
    ///
    /// When a partition is first assigned and has no committed group offset,
    /// the consumer starts fetching from the given offset instead of applying
    /// `auto_offset_reset`. Useful for exactly-once recovery.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use ahash::AHashMap;
    ///
    /// Consumer::builder()
    ///     .bootstrap_servers("localhost:9092")
    ///     .initial_offsets(AHashMap::from_iter([
    ///         (("orders".to_string(), 0), 1_000),
    ///         (("orders".to_string(), 1), 2_000),
    ///     ]))
    ///     .build()
    ///     .await?;
    /// ```
    pub fn initial_offsets(mut self, offsets: HashMap<(String, PartitionId), Offset>) -> Self {
        self.config.initial_offsets = offsets;
        self
    }

    /// Set a key decoder applied transparently after each `poll()` / `recv()`.
    ///
    /// When set, every consumed record's key bytes are passed through this
    /// decoder before being returned to the caller.  The decoder runs after
    /// the interceptor.  Equivalent to `key.deserializer` in the Java
    /// `KafkaConsumer`.
    pub fn key_decoder(mut self, decoder: Arc<dyn crate::schema_registry::SchemaDecoder>) -> Self {
        self.key_decoder = Some(decoder);
        self
    }

    /// Set a value decoder applied transparently after each `poll()` / `recv()`.
    ///
    /// When set, every consumed record's value bytes are passed through this
    /// decoder before being returned to the caller.  The decoder runs after
    /// the interceptor.  Equivalent to `value.deserializer` in the Java
    /// `KafkaConsumer`.
    pub fn value_decoder(
        mut self,
        decoder: Arc<dyn crate::schema_registry::SchemaDecoder>,
    ) -> Self {
        self.value_decoder = Some(decoder);
        self
    }

    /// Share a [`KrafkaClient`](crate::client::KrafkaClient)'s connection pool
    /// and metadata cache instead of creating a new one.
    ///
    /// When multiple clients are created in the same process you should create
    /// a single [`crate::client::KrafkaClient`] and pass it to each builder.
    /// All clients will then multiplex over the same TCP connections.
    ///
    /// When this method is called, `bootstrap_servers` is optional on the
    /// builder (the client was already connected at `KrafkaClient::build` time).
    pub fn with_client(mut self, client: &crate::client::KrafkaClient) -> Self {
        self.shared = Some((client.pool().clone(), client.metadata().clone()));
        self
    }

    /// Validate the configuration and return it, without connecting.
    ///
    /// Runs exactly the checks [`build`](Self::build) runs — they call the same
    /// validator — so a config that passes here will not be rejected later for
    /// a configuration reason. Useful for validating settings at startup, in a
    /// test, or in a config-linting tool, none of which want a broker.
    ///
    /// # Errors
    ///
    /// Returns [`KrafkaError::Config`](crate::error::KrafkaError::Config) for
    /// any invalid combination; see
    /// the consumer configuration validator for the full list.
    pub fn build_config(self) -> Result<ConsumerConfig> {
        // A shared pool supplies the connection, so an empty bootstrap list is
        // legitimate there. Mirror what `build` does rather than duplicating
        // the reasoning.
        if self.shared.is_some() && self.config.bootstrap_servers.is_empty() {
            let mut probe = self.config.clone();
            probe.bootstrap_servers = "<provided-by-client>".to_string();
            super::config::validate(&probe)?;
            return Ok(self.config);
        }
        super::config::validate(&self.config)?;
        Ok(self.config)
    }

    /// Build the consumer.
    ///
    /// # Errors
    ///
    /// All configuration constraints are enforced here, via the same
    /// consumer configuration validator. See its documentation for the full
    /// list.
    ///
    pub async fn build(self) -> Result<Consumer> {
        // `bootstrap_servers` is optional when a pre-built client supplies the
        // connection pool, so that one check is done here rather than in the
        // shared validator, which has no visibility into `shared`.
        if self.shared.is_none() && self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap.servers is required"));
        }
        if self.config.enable_auto_commit && self.config.group_id.is_none() {
            tracing::warn!(
                "enable_auto_commit=true has no effect without group_id; \
                 offsets will not be persisted to the broker"
            );
        }

        // Run the shared validator so that constraints such as
        // `max_poll_records != 0` are enforced on this path too. Without this
        // the only entry point that checked them was unreachable, and
        // `max_poll_records(0)` produced a consumer that silently returned no
        // records forever.
        if self.shared.is_some() && self.config.bootstrap_servers.is_empty() {
            // Satisfy the validator's non-empty check without mutating the
            // caller's config semantics; the pool is already connected.
            let mut probe = self.config.clone();
            probe.bootstrap_servers = "<provided-by-client>".to_string();
            crate::consumer::config::validate(&probe)?;
        } else {
            crate::consumer::config::validate(&self.config)?;
        }

        // `session_timeout` and `max_poll_interval` bound two independent
        // failure modes — coordinator liveness versus application progress —
        // so neither has to be smaller than the other. A session timeout
        // larger than the poll interval is unusual enough to flag, but it is
        // a legitimate configuration and must not block startup.
        if self.config.session_timeout > self.config.max_poll_interval {
            tracing::warn!(
                session_timeout = ?self.config.session_timeout,
                max_poll_interval = ?self.config.max_poll_interval,
                "session_timeout exceeds max_poll_interval; a stalled application \
                 will be removed from the group by the poll-interval check before \
                 the coordinator's session timer would notice"
            );
        }

        let mut consumer = Consumer::new(self.config, self.shared).await?;
        if let Some(listener) = self.rebalance_listener {
            consumer.rebalance_listener = listener;
        }
        if !self.interceptors.is_empty() {
            consumer.interceptor = if self.interceptors.len() == 1 {
                // infallible: len == 1 guaranteed by the surrounding if
                let Some(single) = self.interceptors.into_iter().next() else {
                    unreachable!("len == 1 verified above");
                };
                single
            } else {
                Arc::new(crate::interceptor::ConsumerInterceptorChain::new(
                    self.interceptors,
                ))
            };
        }
        if let Some(dec) = self.key_decoder {
            consumer.key_decoder = Some(dec);
        }
        if let Some(dec) = self.value_decoder {
            consumer.value_decoder = Some(dec);
        }
        Ok(consumer)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use crate::auth::AuthConfig;
    use crate::consumer::{
        AutoOffsetReset, Consumer, ConsumerRebalanceListener, PartitionAssignmentStrategy,
        TopicPartition,
    };
    use std::sync::Arc;
    use std::time::Duration;

    #[test]
    fn test_consumer_builder() {
        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .client_id("test")
            .auto_offset_reset(AutoOffsetReset::Earliest)
            .enable_auto_commit(false)
            .max_poll_records(100)
            .max_poll_interval(Duration::from_secs(600));

        assert_eq!(builder.config.bootstrap_servers, "localhost:9092");
        assert_eq!(builder.config.group_id, Some("test-group".to_string()));
        assert_eq!(builder.config.client_id, "test");
        assert_eq!(builder.config.auto_offset_reset, AutoOffsetReset::Earliest);
        assert!(!builder.config.enable_auto_commit);
        assert_eq!(builder.config.max_poll_records, 100);
        assert_eq!(builder.config.max_poll_interval, Duration::from_secs(600));
        assert!(builder.config.auth.is_none());
    }

    #[test]
    fn test_consumer_builder_with_auth() {
        let builder = Consumer::builder()
            .bootstrap_servers("broker:9093")
            .group_id("secure-group")
            .auth(AuthConfig::sasl_plain("user", "pass").unwrap());

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(!auth.requires_tls());
        assert_eq!(
            auth.security_protocol,
            crate::auth::SecurityProtocol::SaslPlaintext
        );
        assert_eq!(auth.sasl_mechanism, Some(crate::auth::SaslMechanism::Plain));
    }

    #[test]
    fn test_consumer_builder_aws_msk_iam() {
        let auth = AuthConfig::aws_msk_iam("AKID", "secret", "us-east-1");
        let builder = Consumer::builder()
            .bootstrap_servers("broker:9094")
            .group_id("msk-group")
            .auth(auth);

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_tls());
        assert!(auth.requires_sasl());
        assert_eq!(
            auth.sasl_mechanism,
            Some(crate::auth::SaslMechanism::AwsMskIam)
        );
        assert!(auth.aws_msk_iam_credentials.is_some());
        assert!(auth.tls_config.is_some());
    }

    #[test]
    fn test_consumer_builder_no_auth_by_default() {
        let builder = Consumer::builder()
            .bootstrap_servers("broker:9092")
            .group_id("group");

        assert!(builder.config.auth.is_none());
    }

    #[test]
    fn test_consumer_builder_sasl_plain() {
        let builder = Consumer::builder()
            .bootstrap_servers("broker:9093")
            .sasl_plain("user", "pass")
            .unwrap();

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.plain_credentials.is_some());
    }

    #[test]
    fn test_consumer_builder_sasl_scram() {
        let builder = Consumer::builder()
            .bootstrap_servers("broker:9093")
            .sasl_scram_sha256("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.scram_credentials.is_some());

        let builder = Consumer::builder()
            .bootstrap_servers("broker:9093")
            .sasl_scram_sha512("user", "pass");

        let auth = builder.config.auth.as_ref().unwrap();
        assert!(auth.requires_sasl());
        assert!(auth.scram_credentials.is_some());
    }

    #[tokio::test]
    async fn test_consumer_builder_no_servers() {
        let result = Consumer::builder().build().await;
        assert!(result.is_err());
    }

    #[test]
    fn test_consumer_builder_partition_assignment_strategy() {
        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .partition_assignment_strategy(PartitionAssignmentStrategy::RoundRobin);

        assert_eq!(
            builder.config.partition_assignment_strategy(),
            PartitionAssignmentStrategy::RoundRobin
        );
    }

    #[test]
    fn test_consumer_builder_with_rebalance_listener() {
        use std::sync::atomic::AtomicBool;
        use std::sync::atomic::Ordering;

        struct TestListener {
            assigned: AtomicBool,
        }
        impl ConsumerRebalanceListener for TestListener {
            async fn on_partitions_assigned(&self, _: &[TopicPartition]) {
                self.assigned.store(true, Ordering::SeqCst);
            }
            async fn on_partitions_revoked(&self, _: &[TopicPartition]) {}
        }

        let listener = Arc::new(TestListener {
            assigned: AtomicBool::new(false),
        });

        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .rebalance_listener(listener.clone());

        assert!(builder.rebalance_listener.is_some());
    }

    #[test]
    fn test_consumer_builder_group_instance_id() {
        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .group_instance_id("my-instance");

        assert_eq!(
            builder.config.group_instance_id,
            Some("my-instance".to_string())
        );
    }

    #[test]
    fn test_consumer_builder_interceptor() {
        use crate::interceptor::ConsumerInterceptor;
        use crate::interceptor::InterceptorResult;

        #[derive(Debug)]
        struct TestInterceptor;
        impl ConsumerInterceptor for TestInterceptor {
            fn on_consume(
                &self,
                _records: &[crate::consumer::ConsumerRecord],
            ) -> InterceptorResult {
                Ok(())
            }
        }

        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .interceptor(Arc::new(TestInterceptor));

        assert_eq!(builder.interceptors.len(), 1);
    }

    #[test]
    fn test_consumer_builder_add_interceptor() {
        use crate::interceptor::ConsumerInterceptor;

        #[derive(Debug)]
        struct A;
        impl ConsumerInterceptor for A {}

        #[derive(Debug)]
        struct B;
        impl ConsumerInterceptor for B {}

        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .add_interceptor(Arc::new(A))
            .add_interceptor(Arc::new(B));
        assert_eq!(builder.interceptors.len(), 2);
    }

    #[test]
    fn test_consumer_builder_interceptor_replaces_chain() {
        use crate::interceptor::ConsumerInterceptor;

        #[derive(Debug)]
        struct A;
        impl ConsumerInterceptor for A {}

        #[derive(Debug)]
        struct B;
        impl ConsumerInterceptor for B {}

        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group")
            .add_interceptor(Arc::new(A))
            .add_interceptor(Arc::new(A))
            .interceptor(Arc::new(B));
        assert_eq!(builder.interceptors.len(), 1);
    }

    // assign() is rejected when group coordinator is active.
    #[test]
    fn test_assign_with_group_id_configured() {
        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group");

        // When group_id is set, group_coordinator will be Some after new().
        // We verify the config at builder level.
        assert!(builder.config.group_id.is_some());
    }

    // group field removed — only group_coordinator accessor exists.
    #[test]
    fn test_no_legacy_group_field() {
        let builder = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("test-group");
        // The builder should have no group field; only group_coordinator is used
        assert!(builder.config.group_id.is_some());
    }

    // ── Builder validation ───────────────────────────────────────────────
    //
    // These constraints previously lived only in the deleted `ConsumerConfigBuilder::build`,
    // which no public API could reach, so `Consumer::builder()` accepted values
    // that produce a broken consumer.

    #[tokio::test]
    async fn test_builder_rejects_zero_max_poll_records() {
        // 0 truncates every fetched batch to nothing: the consumer reads from
        // the broker and returns no records, forever, with no error.
        let result = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .max_poll_records(0)
            .build()
            .await;

        let err = result.err().expect("max_poll_records(0) must be rejected");
        assert!(
            err.to_string().contains("max_poll_records"),
            "error should name the offending setting, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_builder_rejects_max_poll_records_below_minus_one() {
        let result = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .max_poll_records(-2)
            .build()
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_builder_rejects_negative_max_buffered_records() {
        let result = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .max_buffered_records(-1)
            .build()
            .await;

        let err = result.err().expect("negative buffer cap must be rejected");
        assert!(err.to_string().contains("max_buffered_records"));
    }

    #[tokio::test]
    async fn test_builder_rejects_fetch_min_above_fetch_max() {
        let result = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .fetch_min_bytes(1000)
            .fetch_max_bytes(100)
            .build()
            .await;

        let err = result.err().expect("min above max must be rejected");
        assert!(err.to_string().contains("fetch_min_bytes"));
    }

    #[tokio::test]
    async fn test_builder_rejects_empty_group_id() {
        let result = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .group_id("")
            .build()
            .await;

        let err = result.err().expect("empty group id must be rejected");
        assert!(err.to_string().contains("group_id"));
    }

    #[tokio::test]
    async fn test_builder_rejects_empty_assignment_strategy_list() {
        let result = Consumer::builder()
            .bootstrap_servers("localhost:9092")
            .partition_assignment_strategies(Vec::new())
            .build()
            .await;

        let err = result.err().expect("empty strategy list must be rejected");
        assert!(err.to_string().contains("partition_assignment_strategies"));
    }

    #[tokio::test]
    async fn test_builder_accepts_session_timeout_above_max_poll_interval() {
        // These bound two independent failure modes — coordinator liveness
        // versus application progress — so neither has to be smaller than the
        // other. This is a warning, not a rejection. The build still fails
        // here because there is no broker to connect to, but it must not fail
        // with a *config* error.
        let result = Consumer::builder()
            .bootstrap_servers("localhost:1")
            .session_timeout(Duration::from_secs(120))
            .max_poll_interval(Duration::from_secs(60))
            .heartbeat_interval(Duration::from_secs(3))
            .build()
            .await;

        if let Err(e) = result {
            let msg = e.to_string();
            assert!(
                !msg.contains("must be <= max_poll_interval"),
                "session_timeout > max_poll_interval must not be a config error, got: {msg}"
            );
        }
    }

    #[test]
    fn test_builder_default_strategies_allow_protocol_migration() {
        // Advertising both is what lets a group move from the eager to the
        // cooperative protocol in one rolling bounce.
        let builder = Consumer::builder();
        assert_eq!(
            builder.config.partition_assignment_strategies(),
            &[
                PartitionAssignmentStrategy::Range,
                PartitionAssignmentStrategy::CooperativeSticky
            ]
        );
    }

    #[test]
    fn test_builder_single_strategy_replaces_list() {
        let builder =
            Consumer::builder().partition_assignment_strategy(PartitionAssignmentStrategy::Sticky);
        assert_eq!(
            builder.config.partition_assignment_strategies(),
            &[PartitionAssignmentStrategy::Sticky]
        );
    }
}
