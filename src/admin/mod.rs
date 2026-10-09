//! Admin client for Apache Kafka.
//!
//! [`AdminClient`] manages topics, partitions, configurations, ACLs, consumer
//! and share groups, offsets, transactions, quotas, SCRAM credentials,
//! delegation tokens, features, log directories and the KRaft quorum.
//!
//! # Shape of every operation
//!
//! Each operation is one method that takes its arguments as krafka types and
//! an options struct (`CreateTopicsOptions`, `DescribeConfigsOptions`, …)
//! whose [`Default`] is the usual choice:
//!
//! ```rust,no_run
//! use krafka::Kafka;
//! use krafka::admin::{CreateTopicsOptions, NewTopic};
//!
//! # async fn example() -> krafka::Result<()> {
//! let kafka = Kafka::builder("localhost:9092").connect().await?;
//! let admin = kafka.admin();
//!
//! let results = admin
//!     .create_topics([NewTopic::new("orders", 6, 3)?], CreateTopicsOptions::default())
//!     .await?;
//! for (topic, result) in &results {
//!     match result {
//!         Ok(()) => println!("created {topic}"),
//!         Err(e) => eprintln!("{topic}: {e}"),
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! An operation over several items returns one `Result` per item, keyed by
//! the item. The outer `Result` fails only for what applies to the whole call:
//! a closed client or invalid arguments. A broker's error code stays a
//! [`KrafkaError::Broker`] with that code, so "create if not exists" is
//! `matches!(e, KrafkaError::Broker { code: ErrorCode::TopicAlreadyExists, .. })`.
//!
//! # Routing, retries and the deadline
//!
//! Each request goes to the node that can answer it: the controller, a group
//! or transaction coordinator, a partition leader, a named broker, or any
//! broker — another one when the first fails. Items whose attempt can succeed
//! elsewhere or later are retried with backoff. Reads are retried on any
//! retriable error; writes only when they were not applied. Every call is
//! bounded by its options' `timeout`, by default the client's
//! [`default_api_timeout`](AdminClient::default_api_timeout) (60 s).
//! An item still failing at the deadline reports the broker's last answer, or
//! a [`KrafkaError::Timeout`].
//!
//! # Cancel safety
//!
//! No admin method is cancel safe. A call dropped before it returns may
//! already have been applied by the broker, and the call says nothing about
//! which items were. Retrying is safe for idempotent operations — every
//! describe and list, and alterations that set a fixed value — and not for
//! the others: a retried `create_topics` reports `TOPIC_ALREADY_EXISTS` for a
//! topic the dropped call created. To bound a call and still learn every
//! item's outcome, set its options' `timeout` instead of dropping it.
//!
//! [`KrafkaError::Broker`]: crate::error::KrafkaError::Broker
//! [`KrafkaError::Timeout`]: crate::error::KrafkaError::Timeout

use std::sync::Arc;
use std::time::Duration;

use tracing::info;

use crate::client::Kafka;
use crate::error::Result;
use crate::metadata::ClusterMetadata;
use crate::metrics::{ClientInstanceId, Metrics, MetricsSource};
use crate::network::ConnectionPool;
use crate::telemetry::{ClientType, Telemetry};

pub use crate::consumer::TopicPartition;
pub use crate::protocol::{
    AclBinding, AclBindingFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
    ConfigResourceType, DescribedStreamsGroup, ElectionType, FeatureUpdateKey, FeatureUpgradeType,
    FinalizedFeature, ListedConfigResource, ScramCredentialDeletion, ScramCredentialUpsertion,
    StreamsAssignment, StreamsEndpoint, StreamsGroupMember, StreamsKeyValue, StreamsSubtopology,
    StreamsTaskIds, StreamsTaskOffset, StreamsTopicInfo, StreamsTopology, SupportedFeature,
};

/// Declare an operation's options struct: a `timeout` plus the listed
/// fields, each with a setter of the same name. Fields in the `optional`
/// block are `Option`s whose setter takes the inner value.
macro_rules! admin_options {
    (
        $(#[$meta:meta])*
        $name:ident {
            $( $(#[$field_meta:meta])* $field:ident : $ty:ty ),* $(,)?
        }
        $( optional {
            $( $(#[$opt_meta:meta])* $opt:ident : $opt_ty:ty ),* $(,)?
        } )?
    ) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Default)]
        #[must_use]
        pub struct $name {
            timeout: Option<std::time::Duration>,
            $( $field: $ty, )*
            $( $( $opt: Option<$opt_ty>, )* )?
        }

        impl $name {
            /// Bound the whole call: lookups, attempts and backoff. Default:
            /// the client's
            /// [`default_api_timeout`](crate::admin::AdminClient::default_api_timeout).
            pub fn timeout(mut self, timeout: std::time::Duration) -> Self {
                self.timeout = Some(timeout);
                self
            }

            $(
                $(#[$field_meta])*
                pub fn $field(mut self, value: $ty) -> Self {
                    self.$field = value;
                    self
                }
            )*

            $( $(
                $(#[$opt_meta])*
                pub fn $opt(mut self, value: $opt_ty) -> Self {
                    self.$opt = Some(value);
                    self
                }
            )* )?
        }
    };
}

mod acls;
mod configs;
mod driver;
mod features;
mod group_offsets;
mod groups;
mod offsets;
mod partitions;
mod quotas;
mod scram;
mod share_group_offsets;
mod streams_groups;
mod tokens;
mod topics;
mod transactions;

pub use acls::{
    AclFilter, CreateAclsOptions, DeleteAclsOptions, DeleteAclsResult, DescribeAclsOptions,
};
pub use configs::{
    ClusterBroker, ClusterDescription, ConfigEntry, ConfigOp, ConfigParseError, ConfigResource,
    ConfigSynonymEntry, ConfigValue, DescribeClusterOptions, DescribeConfigsOptions,
    IncrementalAlterConfigsOptions, ListConfigResourcesOptions,
};
pub use features::{DescribeFeaturesOptions, FeatureMetadata, UpdateFeaturesOptions};
pub use group_offsets::{
    AlterConsumerGroupOffsetsOptions, ConsumerGroupLag, ConsumerGroupLagOptions,
    DeleteConsumerGroupOffsetsOptions, GroupOffset, ListConsumerGroupOffsetsOptions,
};
pub use groups::{
    ConsumerGroupDescription, ConsumerGroupListing, ConsumerGroupMember,
    DeleteConsumerGroupsOptions, DescribeConsumerGroupsOptions, GroupType,
    ListConsumerGroupsOptions, TopicPartitionAssignment,
};
pub use offsets::{
    DeleteRecordsOptions, EpochEndOffset, ListOffsetsOptions, ListedOffset,
    OffsetForLeaderEpochOptions, OffsetSpec,
};
pub use partitions::{
    AlterPartitionReassignmentsOptions, AlterReplicaLogDirsOptions, DescribeLogDirsOptions,
    ElectLeadersOptions, ListPartitionReassignmentsOptions, LogDirDescription, LogDirReplica,
    PartitionReassignment, TopicPartitionReplica,
};
pub use quotas::{
    AlterClientQuotasOptions, ClientQuotaAlteration, ClientQuotaEntity, ClientQuotaFilter,
    DescribeClientQuotasOptions, QuotaMatch,
};
pub use scram::{
    AlterUserScramCredentialsOptions, DescribeUserScramCredentialsOptions, ScramCredentialInfo,
};
pub use share_group_offsets::{
    AlterShareGroupOffsetsOptions, DeleteShareGroupOffsetsOptions,
    DescribeShareGroupOffsetsOptions, SharePartitionOffset,
};
pub use streams_groups::DescribeStreamsGroupsOptions;
pub use tokens::{
    CreateDelegationTokenOptions, DelegationToken, DelegationTokenPrincipal,
    DescribeDelegationTokenOptions, ExpireDelegationTokenOptions, RenewDelegationTokenOptions,
};
pub use topics::{
    CreatePartitionsOptions, CreateTopicsOptions, DeleteTopicsOptions, DescribeTopicsOptions,
    ListTopicsOptions, NewTopic, PartitionDescription, TopicDescription,
};
pub use transactions::{
    AbortTransactionOptions, DescribeMetadataQuorumOptions, DescribeProducersOptions,
    DescribeTransactionsOptions, ListTransactionsOptions, ProducerState, QuorumInfo,
    QuorumListener, QuorumNode, QuorumReplica, TransactionDescription, TransactionListing,
};

/// Default bound on one admin call, matching Java's `default.api.timeout.ms`.
const DEFAULT_API_TIMEOUT: Duration = Duration::from_secs(60);

/// Admin client settings.
#[derive(Debug, Clone)]
pub(crate) struct AdminConfig {
    /// Bound on one admin call when its options set no `timeout`.
    pub(crate) default_api_timeout: Duration,
    /// Backoff between attempts inside one call: 100 ms doubling to 1 s,
    /// ±20% jitter.
    pub(crate) retry_backoff: crate::util::BackoffPolicy,
    /// Push this client's metrics to brokers that subscribe to them (KIP-714,
    /// Java `enable.metrics.push`).
    pub(crate) metrics_push: bool,
}

impl Default for AdminConfig {
    fn default() -> Self {
        Self {
            default_api_timeout: DEFAULT_API_TIMEOUT,
            retry_backoff: crate::util::BackoffPolicy {
                initial_backoff: Duration::from_millis(100),
                max_backoff: Duration::from_secs(1),
                backoff_multiplier: 2.0,
                jitter_factor: 0.2,
            },
            metrics_push: false,
        }
    }
}

/// Kafka admin client for cluster administration, from [`Kafka::admin`].
///
/// See the [module documentation](self) for the shape of every operation.
pub struct AdminClient {
    config: AdminConfig,
    kafka: Kafka,
    metadata: Arc<ClusterMetadata>,
    pool: Arc<ConnectionPool>,
    closed: std::sync::atomic::AtomicBool,
    /// Where `metrics()` and the KIP-714 reporter read from.
    metrics_source: Arc<MetricsSource>,
    /// The KIP-714 reporter, when `metrics_push` is on.
    telemetry: Telemetry,
}

impl std::fmt::Debug for AdminClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdminClient")
            .field("config", &self.config)
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}

impl AdminClient {
    pub(crate) fn new(kafka: Kafka) -> Self {
        Self {
            config: AdminConfig::default(),
            metadata: Arc::clone(kafka.metadata()),
            pool: Arc::clone(kafka.pool()),
            closed: std::sync::atomic::AtomicBool::new(false),
            metrics_source: MetricsSource::admin(&kafka),
            telemetry: Telemetry::disabled(),
            kafka,
        }
    }

    /// Bound every call whose options set no `timeout`: lookups, attempts
    /// and backoff together (Java `default.api.timeout.ms`). Default: 60 s.
    pub fn default_api_timeout(mut self, timeout: Duration) -> Self {
        self.config.default_api_timeout = timeout;
        self
    }

    /// First backoff between attempts inside one call; it doubles up to 1 s,
    /// with 20 % jitter. Default: 100 ms.
    pub fn retry_backoff(mut self, backoff: Duration) -> Self {
        self.config.retry_backoff.initial_backoff = backoff;
        self
    }

    /// Push this client's metrics to the brokers when a cluster operator
    /// subscribes to them (KIP-714, Java `enable.metrics.push`). Default: off,
    /// as in Java. Turning it on starts the reporter, which needs a Tokio
    /// runtime.
    pub fn metrics_push(mut self, enable: bool) -> Self {
        self.config.metrics_push = enable;
        self.telemetry = Telemetry::start(
            enable,
            &self.kafka,
            ClientType::Admin,
            Arc::clone(&self.metrics_source),
        );
        self
    }

    /// Close the admin client: later calls fail with
    /// [`KrafkaError::Closed`](crate::error::KrafkaError::Closed). Calls in
    /// flight finish. Idempotent.
    pub async fn close(&self) -> Result<()> {
        if !self.closed.swap(true, std::sync::atomic::Ordering::SeqCst) {
            self.telemetry.close(self.config.default_api_timeout).await;
            info!("admin client closed");
        }
        Ok(())
    }

    /// Check if the admin client is closed.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// This client's [`Metrics`]: the connection counters of the pool it
    /// shares. An owned snapshot, read without blocking.
    pub fn metrics(&self) -> Metrics {
        self.metrics_source.snapshot()
    }

    /// The id the cluster assigned this client for KIP-714 telemetry,
    /// waiting at most `timeout` for it (Java `clientInstanceId`). `None`
    /// when the cluster does not support client telemetry.
    ///
    /// # Errors
    ///
    /// [`KrafkaError::IllegalState`](crate::error::KrafkaError::IllegalState)
    /// when [`metrics_push`](Self::metrics_push) is off (the default);
    /// [`KrafkaError::Timeout`](crate::error::KrafkaError::Timeout) when no
    /// broker answered in time.
    pub async fn client_instance_id(&self, timeout: Duration) -> Result<Option<ClientInstanceId>> {
        self.telemetry.client_instance_id(timeout).await
    }
}

/// Reject invalid topic names before anything is encoded.
fn validate_topics<'a>(names: impl IntoIterator<Item = &'a str>) -> Result<()> {
    crate::protocol::validate_topic_names(names)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use crate::error::KrafkaError;

    #[test]
    fn test_admin_client_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<AdminClient>();
    }

    #[test]
    fn admin_retry_backoff_is_exponential_and_jittered() {
        let policy = AdminConfig::default().retry_backoff;
        assert_eq!(policy.initial_backoff, Duration::from_millis(100));
        assert_eq!(policy.max_backoff, Duration::from_secs(1));
        assert!(policy.backoff_multiplier > 1.0);
        assert!(policy.jitter_factor() > 0.0);
    }

    #[tokio::test]
    async fn admin_settings_reach_the_config() {
        let admin = Kafka::detached()
            .admin()
            .default_api_timeout(Duration::from_secs(5))
            .retry_backoff(Duration::from_millis(7));
        assert_eq!(admin.config.default_api_timeout, Duration::from_secs(5));
        assert_eq!(
            admin.config.retry_backoff.initial_backoff,
            Duration::from_millis(7)
        );
    }

    #[tokio::test]
    async fn close_is_idempotent_and_operations_fail_fast_after_it() {
        let client = Kafka::detached().admin();
        client.close().await.unwrap();
        client.close().await.unwrap();
        assert!(client.is_closed());
        let err = client
            .list_topics(ListTopicsOptions::default())
            .await
            .unwrap_err();
        assert!(matches!(err, KrafkaError::Closed { .. }), "got: {err:?}");
    }
}
