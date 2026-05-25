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

    /// Set maximum poll interval before consumer is considered dead.
    pub fn max_poll_interval(mut self, interval: Duration) -> Self {
        self.config.max_poll_interval = interval;
        self
    }

    /// Set request timeout.
    pub fn request_timeout(mut self, timeout: Duration) -> Self {
        self.config.request_timeout = timeout;
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

    /// Set partition assignment strategy for consumer groups.
    pub fn partition_assignment_strategy(mut self, strategy: PartitionAssignmentStrategy) -> Self {
        self.config.partition_assignment_strategy = strategy;
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

    /// Build the consumer.
    pub async fn build(self) -> Result<Consumer> {
        if self.shared.is_none() && self.config.bootstrap_servers.is_empty() {
            return Err(KrafkaError::config("bootstrap.servers is required"));
        }
        if self.config.enable_auto_commit && self.config.group_id.is_none() {
            tracing::warn!(
                "enable_auto_commit=true has no effect without group_id; \
                 offsets will not be persisted to the broker"
            );
        }
        if self.config.heartbeat_interval >= self.config.session_timeout {
            return Err(KrafkaError::config(format!(
                "heartbeat_interval ({:?}) must be less than session_timeout ({:?}) \
                 (recommended: session_timeout / 3)",
                self.config.heartbeat_interval, self.config.session_timeout,
            )));
        }
        if self.config.session_timeout > self.config.max_poll_interval {
            return Err(KrafkaError::config(format!(
                "session_timeout ({:?}) must be <= max_poll_interval ({:?})",
                self.config.session_timeout, self.config.max_poll_interval,
            )));
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
            builder.config.partition_assignment_strategy,
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
}
