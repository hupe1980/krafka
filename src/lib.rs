//! # Krafka
//!
//! A pure Rust, async-native Apache Kafka client.
//!
//! Krafka provides high-performance, safe, and idiomatic Rust APIs for
//! producing and consuming messages from Apache Kafka clusters.
//!
//! ## Features
//!
//! - **Pure Rust by default**: No librdkafka or C bindings; the optional `zstd`
//!   compression feature links against `zstd-sys` and requires a C toolchain
//! - **Async-native**: Built on Tokio for non-blocking I/O
//! - **High-performance**: Zero-copy buffers, minimal allocations
//! - **Safe**: No unsafe code by default
//! - **Cloud-native**: First-class AWS MSK support including IAM auth
//!
//! ## Thread Safety
//!
//! All main types in Krafka implement `Send + Sync`:
//!
//! - [`Producer`](producer::Producer) - can be shared across tasks with `Arc`
//! - [`Consumer`](consumer::Consumer) - can be shared across tasks with `Arc`
//! - [`AdminClient`](admin::AdminClient) - can be shared across tasks with `Arc`
//!
//! This allows safe concurrent access from multiple Tokio tasks:
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use krafka::producer::Producer;
//!
//! # async fn example() -> Result<(), krafka::error::KrafkaError> {
//! let producer = Arc::new(Producer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .build()
//!     .await?);
//!
//! // Spawn multiple tasks sharing the producer
//! for i in 0..10 {
//!     let producer = producer.clone();
//!     tokio::spawn(async move {
//!         let _ = producer.send("topic", None, b"message").await;
//!     });
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Quick Start
//!
//! ### Producer
//!
//! ```rust,no_run
//! use krafka::producer::Producer;
//!
//! # async fn example() -> Result<(), krafka::error::KrafkaError> {
//! let producer = Producer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .build()
//!     .await?;
//!
//! producer.send("my-topic", Some(b"key"), b"value").await?;
//! # Ok(())
//! # }
//! ```
//!
//! ### Consumer
//!
//! ```rust,no_run
//! use krafka::consumer::Consumer;
//!
//! # async fn example() -> Result<(), krafka::error::KrafkaError> {
//! let consumer = Consumer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .group_id("my-group")
//!     .build()
//!     .await?;
//!
//! consumer.subscribe(&["my-topic"]).await?;
//!
//! loop {
//!     match consumer.recv().await {
//!         Ok(msg)                          => println!("{:?}", msg),
//!         Err(krafka::RecvError::Closed)   => break,
//!         Err(krafka::RecvError::Error(e)) => return Err(e),
//!         Err(_)                           => break,
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Cargo Features
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `compression` | **yes** | Enables pure-Rust compression codecs (`gzip` + `snappy` + `lz4`). |
//! | `compression-all` | no | Enables all compression codecs, including `zstd`. |
//! | `gzip` | via `compression` | Gzip record batch compression via `flate2`. |
//! | `snappy` | via `compression` | Snappy compression via `snap`. |
//! | `lz4` | via `compression` | LZ4 compression via `lz4_flex`. |
//! | `zstd` | no | Zstd compression via `zstd` (requires C toolchain). |
//! | `aws-msk` | no | AWS MSK IAM authentication with SDK credential chain. |
//! | `schema-registry` | no | Confluent Schema Registry HTTP client. |
//! | `oauth-oidc` | no | Built-in OIDC token provider for SASL/OAUTHBEARER: the `client_credentials` grant (KIP-768) and RFC 7523 client assertions (KIP-1258). Adds no cryptography dependency — assertions are supplied pre-signed. |
//! | `aws-glue-schema-registry` | no | AWS Glue Schema Registry SDK client. |
//! | `socks5` | no | SOCKS5 proxy support via `tokio-socks`. |
//! | `telemetry` | no | OpenTelemetry exporter for producer/consumer metrics. |
//! | `unstable-protocol` | no | Enables protocol versions Kafka marks `latestVersionUnstable` — a released broker does not advertise them without `unstable.api.versions.enable=true`. Covers `ApiVersions` v5 (KIP-1242), `InitProducerId` v6 (KIP-939) and the Share Consumer (KIP-932). APIs under this feature may change without semver notice. |
//! | `ring` | **yes** | rustls crypto backend using `ring` (pure Rust). |
//! | `rustls-aws-lc-rs` | no | rustls crypto backend using `aws-lc-rs`. Preferred on AWS Graviton and for FIPS deployments. |
//! | `native-tls-roots` | no | Load platform-native root certificates via `rustls-native-certs`. |
//! | `test-broker` | no | In-process fake Kafka broker for testing your own code against a real client. Not for production builds. |
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
//! ```toml
//! [dependencies]
//! # `ring` (or `rustls-aws-lc-rs`) is required — without it the build fails.
//! krafka = { version = "0.15.0", default-features = false, features = ["lz4", "ring"] }
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(unsafe_code)]

// Cargo features must be *additive*: if crate A depends on krafka with `ring`
// and crate B depends on krafka with `rustls-aws-lc-rs`, Cargo unifies the two
// feature sets and both are enabled.  Rejecting that combination at compile
// time would make the two crates impossible to use together, with no recourse
// for the application author.  Instead the backends are additive and
// `rustls-aws-lc-rs` deterministically wins when both are active — see
// `auth::tls::resolve_crypto_provider`.
//
// At least one backend is required, because rustls cannot construct a
// `ClientConfig` without a crypto provider.
#[cfg(not(any(feature = "ring", feature = "rustls-aws-lc-rs")))]
compile_error!(
    "krafka requires a rustls crypto backend, but neither `ring` nor \
     `rustls-aws-lc-rs` is enabled. The default `ring` backend is disabled by \
     `default-features = false`; re-enable it with `features = [\"ring\"]`, or \
     select `features = [\"rustls-aws-lc-rs\"]` instead."
);

// krafka's metrics layer relies on 64-bit atomic operations (AtomicU64).
// 32-bit targets without hardware AtomicU64 support (e.g. Cortex-M3) are not
// supported.  Fail fast with a clear diagnostic rather than a confusing link
// error or silent correctness bug.
#[cfg(not(target_has_atomic = "64"))]
compile_error!(
    "krafka requires 64-bit atomic support (`target_has_atomic = \"64\"`). \
     32-bit targets without AtomicU64 (e.g. ARMv6-M, Cortex-M3) are not supported."
);

pub mod admin;
pub mod auth;
pub mod client;
pub mod consumer;
pub mod dlq;
pub mod error;
/// Minimal async HTTP/1.1 client shared by the OIDC token provider and the
/// Confluent Schema Registry client.
///
/// Compiled only when a feature needs it (`oauth-oidc` or `schema-registry`).
/// This is an implementation detail and not part of the stable public API.
#[cfg(any(feature = "oauth-oidc", feature = "schema-registry"))]
mod http;
pub mod interceptor;
/// Cluster metadata cache and refresh logic.
///
/// This is an implementation detail of the consumer and producer. Types are
/// accessible for advanced use but are **not** part of the stable public API.
#[doc(hidden)]
pub mod metadata;
pub mod metrics;
/// Network connection pool and transport layer.
///
/// This is an implementation detail. Types are accessible for advanced use
/// (e.g. custom authentication) but are **not** part of the stable public API.
#[doc(hidden)]
pub mod network;
pub mod producer;
/// Kafka wire-protocol encode/decode layer.
///
/// This is an implementation detail. Types are accessible for advanced use
/// (e.g. benchmarks, raw record batch construction) but are **not** part of
/// the stable public API.
#[doc(hidden)]
pub mod protocol;
pub mod schema_registry;
#[cfg(feature = "unstable-protocol")]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable-protocol")))]
pub mod share_consumer;
#[cfg(feature = "telemetry")]
#[cfg_attr(docsrs, doc(cfg(feature = "telemetry")))]
pub mod telemetry;
/// In-process fake Kafka broker for deterministic client tests.
///
/// Enabled by the `test-broker` feature. Not compiled into production builds.
#[cfg(feature = "test-broker")]
#[cfg_attr(docsrs, doc(cfg(feature = "test-broker")))]
pub mod testing;
pub mod tracing_ext;
pub mod util;

pub use error::{KrafkaError, ProtocolErrorKind, RecvError, Result};
pub use metadata::MetadataRecoveryStrategy;
// Re-export user-facing protocol types at a stable path so callers do not
// need to reach into the hidden `protocol` module.
pub use protocol::{
    Compression, LazyRecordBatch, LazyRecordIterator, Record, RecordBatch, RecordBatchBuilder,
    RecordHeader,
};
// Re-export user-facing network/auth types at a stable path so callers do
// not need to reach into the hidden `network` module.
pub use network::{
    ChallengeResponse, SaslAuthenticator, SecureConnectionConfig, SecureConnectionConfigBuilder,
};

/// Kafka protocol API version.
pub type ApiVersion = i16;

/// Kafka correlation ID for request/response matching.
pub type CorrelationId = i32;

/// Kafka partition ID.
pub type PartitionId = i32;

/// Kafka broker ID.
pub type BrokerId = i32;

/// Kafka offset.
pub type Offset = i64;

/// Kafka timestamp (milliseconds since epoch).
pub type Timestamp = i64;
