//! # Krafka
//!
//! A pure Rust, async-native Apache Kafka client.
//!
//! Producer, transactional producer, consumer, share consumer and admin
//! clients for Apache Kafka.
//!
//! ## Features
//!
//! - **No C library, no system dependency**: the default build needs a Rust
//!   toolchain and the C compiler `cc` finds for `ring`, nothing else. Every
//!   codec decodes in pure Rust; the opt-in `zstd`, `rustls-aws-lc-rs` and
//!   `aws-msk` features compile more C
//! - **Async-native**: built on Tokio
//! - **Shared buffers**: decoded record keys, values and header values are slices of
//!   the fetched response (one allocation per uncompressed batch, for the
//!   record list), and each response frame is read into one allocation
//! - **Safe**: no `unsafe` code; `panic`, `unwrap` and `expect` are denied
//! - **Cloud-ready**: TLS, SASL PLAIN/SCRAM/OAUTHBEARER, a built-in OIDC
//!   provider and AWS MSK IAM
//!
//! ## Quick start
//!
//! ```rust,no_run
//! # async fn example() -> krafka::Result<()> {
//! use krafka::{Kafka, Record};
//!
//! let kafka = Kafka::builder("localhost:9092").connect().await?;
//! let producer = kafka.producer().build().await?;
//! producer.send(Record::new("orders", "hello").key("k")).await?;
//!
//! let consumer = kafka.consumer("my-group").build().await?;
//! consumer.subscribe(["orders"]).await?;
//! while let Some(rec) = consumer.recv().await? { println!("{rec:?}"); }
//! # Ok(())
//! # }
//! ```
//!
//! [`Kafka`] holds every connection setting — bootstrap servers, client id,
//! TLS and SASL, transport, proxy, metadata — and one connection pool. Every
//! client is built from it and shares the pool: [`Kafka::producer`],
//! [`Kafka::consumer`], [`Kafka::share_consumer`], [`Kafka::admin`]. A
//! separate pool is a second `Kafka`.
//!
//! Every client is `Send + Sync`; share one across tasks with an `Arc`, and
//! end it with `close().await`.
//!
//! ## Errors
//!
//! Every fallible call returns [`KrafkaError`], which answers three
//! questions: [`is_retriable`](KrafkaError::is_retriable) (the same call may
//! succeed), [`is_fatal`](KrafkaError::is_fatal) (rebuild the client) and
//! [`requires_abort`](KrafkaError::requires_abort) (abort the transaction;
//! the producer stays usable).
//!
//! ## Observability
//!
//! - **Metrics.** Every client's `metrics()` returns one owned
//!   [`Metrics`](metrics::Metrics) snapshot, including the connection
//!   counters of its pool; [`Kafka::metrics`] sums a handle's clients.
//!   [`Metrics::prometheus_text`](metrics::Metrics::prometheus_text) renders
//!   one for a scrape endpoint.
//! - **Spans.** The clients emit spans through `tracing`, on OpenTelemetry
//!   messaging semantic conventions [`OTEL_SEMCONV_VERSION`] (v1.44.0, whose
//!   messaging conventions are still *Development*): a `send` span per
//!   record from `send`/`enqueue` to its outcome (kind `producer`), a `poll`
//!   span per `poll`/`recv` and a `commit` span per commit (kind `client`),
//!   and a krafka-specific `rebalance` span per assignment change (kind
//!   `internal`). Record keys and values, and hashes of keys, are never
//!   recorded; a key's size is. krafka depends on no OpenTelemetry crate:
//!   bridge `tracing` to OpenTelemetry in the application.
//! - **KIP-714 telemetry.** Every client pushes its metrics to brokers whose
//!   operator subscribed to them, as Java clients do with
//!   `enable.metrics.push`: on by default for producers and consumers, off
//!   for the admin client, switched by `metrics_push` on each role builder.
//!   A cluster without a client-telemetry plugin receives nothing.
//!
//! ## Stability
//!
//! The public API follows semver. Outside that promise:
//! [`testing`] (the fake broker, behind `test-broker`), the
//! `unstable-protocol` feature, and the hidden `__private` module behind the
//! tooling-only `internal` feature, which exists for krafka's own benches and
//! fuzz targets.
//!
//! ## Cargo Features
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `ring` | **yes** | rustls crypto backend using `ring`. |
//! | `rustls-aws-lc-rs` | no | rustls crypto backend using `aws-lc-rs`; offers post-quantum X25519MLKEM768 key exchange first. Compiles C and needs CMake. |
//! | `zstd` | no | Zstd *encoding* via `zstd` (compiles C). Zstd decoding is always available. |
//! | `aws-msk` | no | AWS MSK IAM authentication with the SDK credential chain (compiles C and needs CMake, via `aws-lc-sys`). |
//! | `oauth-oidc` | no | Built-in OIDC token provider for SASL/OAUTHBEARER: the `client_credentials` grant (KIP-768) and RFC 7523 client assertions (KIP-1258). Adds no cryptography dependency — assertions are supplied pre-signed. |
//! | `native-tls-roots` | no | Load platform-native root certificates via `rustls-native-certs`. |
//! | `tls-encrypted-keys` | no | Passphrase-encrypted PKCS#8 client keys (`ssl.key.password`) via the RustCrypto `pkcs8` crate. |
//! | `unstable-protocol` | no | Enables protocol versions Kafka marks `latestVersionUnstable` — a released broker does not advertise them without `unstable.api.versions.enable=true`. Covers `ApiVersions` v5 (KIP-1242) and `InitProducerId` v6 (KIP-939). APIs under this feature may change without semver notice. |
//! | `test-broker` | no | In-process fake Kafka broker for testing your own code against a real client. Not for production builds; outside semver. |
//!
//! Always compiled in: gzip, Snappy and LZ4 compression, the KIP-932 share
//! consumer (needs a Kafka 4.2+ broker), SOCKS5 proxy support, and KIP-714
//! client telemetry.
//!
//! ## TLS crypto backend
//!
//! Exactly one rustls crypto backend is used at runtime. `ring` is the default;
//! `rustls-aws-lc-rs` selects `aws-lc-rs` instead.
//!
//! These features are **additive**, as Cargo requires: if two crates in your
//! dependency graph each select a different backend, the build still succeeds
//! and `rustls-aws-lc-rs` wins. To pin a specific backend regardless of what
//! your dependency graph enabled, install it as the process default before
//! constructing any krafka client:
//!
//! ```rust,ignore
//! rustls::crypto::ring::default_provider().install_default().ok();
//! ```
//!
//! Enabling **neither** backend is a compile error — rustls cannot build a
//! `ClientConfig` without a crypto provider.
//!
//! To disable the default features and pick only what you need, remember that
//! `default-features = false` also drops `ring`:
//!
//! ```sh
//! # `ring` (or `rustls-aws-lc-rs`) is required — without it the build fails.
//! cargo add krafka --no-default-features --features rustls-aws-lc-rs
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(unsafe_code)]
#![deny(private_interfaces, private_bounds, unnameable_types)]

// The backends are additive (Cargo unifies features across the graph);
// `rustls-aws-lc-rs` wins when both are on — see
// `auth::tls::resolve_crypto_provider`. At least one is required: rustls
// cannot construct a `ClientConfig` without a crypto provider.
#[cfg(not(any(feature = "ring", feature = "rustls-aws-lc-rs")))]
compile_error!(
    "krafka requires a rustls crypto backend, but neither `ring` nor \
     `rustls-aws-lc-rs` is enabled. The default `ring` backend is disabled by \
     `default-features = false`; re-enable it with `features = [\"ring\"]`, or \
     select `features = [\"rustls-aws-lc-rs\"]` instead."
);

// krafka's metrics layer relies on 64-bit atomic operations (AtomicU64).
// 32-bit targets without hardware AtomicU64 support (e.g. Cortex-M3) are not
// supported.
#[cfg(not(target_has_atomic = "64"))]
compile_error!(
    "krafka requires 64-bit atomic support (`target_has_atomic = \"64\"`). \
     32-bit targets without AtomicU64 (e.g. ARMv6-M, Cortex-M3) are not supported."
);

pub mod admin;
pub mod auth;
/// Tracks in-flight operations so a flush or a close can wait for work that has
/// started but not yet reached the structure it will land in.
///
/// Shared by the producer (draining a transaction before `EndTxn`) and the
/// share consumer (draining acknowledgements before `close`), which is why it
/// sits at the crate root rather than inside either.
mod barrier;
mod client;
pub mod consumer;
pub mod dlq;
pub mod error;
/// Minimal async HTTP/1.1 client used by the OIDC token provider.
///
/// Compiled only when `oauth-oidc` is enabled. This is an implementation
/// detail and not part of the stable public API.
#[cfg(feature = "oauth-oidc")]
mod http;
pub mod interceptor;
/// Cluster metadata cache and refresh logic.
// Items only the fake broker, tests or `__private` use are dead without
// `internal`.
#[cfg_attr(not(feature = "internal"), allow(dead_code))]
pub(crate) mod metadata;
pub mod metrics;
/// Network connection pool and transport layer.
#[cfg_attr(not(feature = "internal"), allow(dead_code))]
pub(crate) mod network;
pub mod producer;
/// Kafka wire-protocol encode/decode layer. Request decoders and response
/// encoders serve the fake broker in `testing`.
#[cfg_attr(not(feature = "internal"), allow(dead_code))]
pub(crate) mod protocol;
pub mod serdes;
pub mod share_consumer;
/// KIP-714 client telemetry: the reporter every client starts.
pub(crate) mod telemetry;
/// In-process fake Kafka broker for deterministic client tests.
///
/// Enabled by the `test-broker` feature. Not compiled into production builds,
/// and not covered by semver: its API may change in any release.
#[cfg(feature = "test-broker")]
#[cfg_attr(docsrs, doc(cfg(feature = "test-broker")))]
pub mod testing;
/// The spans the clients emit.
mod tracing_ext;
#[cfg_attr(not(feature = "internal"), allow(dead_code))]
pub(crate) mod util;

/// Internals for krafka's own benches and fuzz targets. Not part of the
/// public API: no semver promise, enabled only by the tooling-only
/// `internal` feature.
#[cfg(feature = "internal")]
#[doc(hidden)]
pub mod __private {
    /// The wire-protocol layer.
    pub mod protocol {
        pub use crate::protocol::*;
    }
    /// Connections and the pool.
    pub mod network {
        pub use crate::network::*;
    }
    /// The cluster-metadata cache.
    pub mod metadata {
        pub use crate::metadata::*;
    }
    /// SCRAM client.
    pub mod scram {
        pub use crate::auth::scram::*;
    }
    /// TLS connector construction.
    pub mod tls {
        pub use crate::auth::tls::*;
    }
    /// The metadata cache a [`Kafka`](crate::Kafka) handle routes with.
    pub fn kafka_metadata(
        kafka: &crate::Kafka,
    ) -> &std::sync::Arc<crate::metadata::ClusterMetadata> {
        kafka.metadata()
    }
    /// The connection pool a [`Kafka`](crate::Kafka) handle owns.
    pub fn kafka_pool(kafka: &crate::Kafka) -> &std::sync::Arc<crate::network::ConnectionPool> {
        kafka.pool()
    }
    /// Shared helpers: CRC32C, varints, backoff.
    pub mod util {
        pub use crate::util::*;
    }
    /// The OIDC token client's HTTP/1.1 response parser.
    #[cfg(feature = "oauth-oidc")]
    pub mod http {
        pub use crate::http::{MAX_HEADERS, MAX_LINE_BYTES, read_response_from_bytes};
    }
}

/// The types you need to write a producer, a consumer or an admin client.
///
/// ```rust
/// use krafka::prelude::*;
/// ```
///
/// A glob import, so an explicit `use` of the same type shadows it rather than
/// colliding. Everything here is re-exported from its own module; the prelude
/// exists so that the common case is one line instead of eight, and so that the
/// documentation snippets have a single import that keeps them honest.
///
/// Deliberately excluded: the share consumer, the fake broker, and anything a
/// caller is unlikely to name more than once per program (the metrics
/// snapshot types).
/// A prelude that pulls in names you did not ask for is worse than no prelude.
pub mod prelude {
    pub use crate::admin::{AdminClient, NewTopic};
    pub use crate::auth::{AuthConfig, TlsConfig};
    pub use crate::client::{Kafka, KafkaBuilder};
    pub use crate::consumer::{
        AutoOffsetReset, Consumer, ConsumerRecord, IsolationLevel, OffsetAndMetadata,
        TopicPartition,
    };
    // `Result` is deliberately absent: krafka's alias takes one parameter, so
    // a glob that shadowed `std::result::Result` would break every
    // `Result<T, E>` in the importing module. Write `krafka::Result<T>`.
    pub use crate::error::KrafkaError;
    pub use crate::producer::{Producer, Record, TransactionalProducer};
    pub use crate::protocol::Compression;
}

pub use client::{CloseOptions, GroupMembershipOperation, Kafka, KafkaBuilder};
pub use consumer::{ConsumerRecord, TimestampType};
pub use error::{KrafkaError, ProtocolErrorKind, Result};
pub use metadata::{BrokerInfo, MetadataRecoveryStrategy, PartitionInfo, TopicInfo};
pub use network::ProxyConfig;
pub use producer::Record;
pub use protocol::Compression;
pub use tracing_ext::OTEL_SEMCONV_VERSION;

/// Kafka partition ID.
pub type PartitionId = i32;

/// Kafka broker ID.
pub type BrokerId = i32;

/// Kafka offset.
pub type Offset = i64;

/// Kafka timestamp (milliseconds since epoch).
pub type Timestamp = i64;

/// Record headers, the same type on produce and consume: keys in order,
/// duplicates kept, and `None` for a null value (distinct from an empty one).
pub type Headers = Vec<(String, Option<bytes::Bytes>)>;

/// Doctest-only guard: each snippet names an item that is not part of the
/// public API and must not compile.
///
/// ```compile_fail
/// let _ = krafka::client::KrafkaClient::builder("b:9092");
/// ```
///
/// ```compile_fail
/// let _ = krafka::producer::Producer::builder();
/// ```
///
/// ```compile_fail
/// let _ = krafka::consumer::Consumer::builder();
/// ```
///
/// ```compile_fail
/// let _ = krafka::share_consumer::ShareConsumer::builder();
/// ```
///
/// ```compile_fail
/// let _ = krafka::admin::AdminClient::builder();
/// ```
///
/// ```compile_fail
/// let _ = krafka::producer::TransactionalProducer::builder();
/// ```
///
/// ```compile_fail
/// fn f(kafka: &krafka::Kafka) { let _ = kafka.producer().client_id("x"); }
/// ```
///
/// ```compile_fail
/// fn f(kafka: &krafka::Kafka) { let _ = kafka.producer().bootstrap_servers("b:9092"); }
/// ```
///
/// ```compile_fail
/// fn f(kafka: &krafka::Kafka) { let _ = kafka.consumer("g").sasl_plain("u", "p"); }
/// ```
///
/// ```compile_fail
/// fn f(kafka: &krafka::Kafka) { let _ = kafka.share_consumer("g").request_timeout(std::time::Duration::ZERO); }
/// ```
///
/// ```compile_fail
/// fn f(kafka: &krafka::Kafka, other: &krafka::Kafka) { let _ = kafka.producer().with_client(other); }
/// ```
///
/// ```compile_fail
/// let _ = krafka::auth::AuthConfig::sasl_plain_ssl("u", "p", krafka::auth::TlsConfig::new());
/// ```
///
/// ```compile_fail
/// let _ = krafka::auth::AuthConfig::sasl_plain("u", "p").unwrap();
/// ```
///
/// ```compile_fail
/// fn f(p: &krafka::producer::Producer) -> bool { p.owns_pool() }
/// ```
///
/// ```compile_fail
/// async fn f(p: &krafka::producer::Producer) { let _ = p.send("t", None, Some(b"v")).await; }
/// ```
///
/// ```compile_fail
/// async fn f(p: &krafka::producer::Producer) { let _ = p.send_record(krafka::Record::new("t", "v")).await; }
/// ```
///
/// ```compile_fail
/// async fn f(p: &krafka::producer::Producer) { let _ = p.send_with_headers("t", None, None, vec![]).await; }
/// ```
///
/// ```compile_fail
/// let _ = krafka::producer::ProducerRecord::new("t", "v");
/// ```
///
/// ```compile_fail
/// fn f(p: &krafka::producer::Producer) { let _ = p.begin(); }
/// ```
///
/// ```compile_fail
/// async fn f(p: &krafka::producer::Producer) { p.close_with_timeout(std::time::Duration::ZERO).await; }
/// ```
///
/// ```compile_fail
/// let _: Option<krafka::RecvError> = None;
/// ```
///
/// ```compile_fail
/// async fn f(c: &krafka::consumer::Consumer) { let _ = c.batch_recv(1, std::time::Duration::ZERO).await; }
/// ```
///
/// ```compile_fail
/// let _: Option<krafka::consumer::BatchRecvOutcome> = None;
/// ```
///
/// ```compile_fail
/// async fn f(c: &krafka::consumer::Consumer) { let _ = c.commit_sync().await; }
/// ```
///
/// ```compile_fail
/// fn f(c: &krafka::consumer::Consumer) { let _ = c.commit_async(); }
/// ```
///
/// ```compile_fail
/// let _: Option<krafka::consumer::OffsetCommitHandle> = None;
/// ```
///
/// ```compile_fail
/// async fn f(c: &krafka::consumer::Consumer) { let _ = c.cached_end_offset("t", 0).await; }
/// ```
///
/// ```compile_fail
/// fn f(c: &krafka::share_consumer::ShareConsumer) { let _ = c.acknowledge_by_offset("t", 0, 0); }
/// ```
///
/// ```compile_fail
/// let _: Option<krafka::share_consumer::ShareConsumerConfig> = None;
/// ```
///
/// ```compile_fail
/// use krafka::protocol::ApiKey;
/// ```
///
/// ```compile_fail
/// use krafka::network::TransportConfig;
/// ```
///
/// ```compile_fail
/// use krafka::metadata::ClusterMetadata;
/// ```
///
/// ```compile_fail
/// use krafka::auth::OAuthBearerTokenProvider;
/// ```
#[cfg(doctest)]
pub struct RemovedApi;
