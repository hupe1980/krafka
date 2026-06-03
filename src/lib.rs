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
//! | `aws-glue-schema-registry` | no | AWS Glue Schema Registry SDK client. |
//! | `socks5` | no | SOCKS5 proxy support via `tokio-socks`. |
//! | `danger-insecure-tls` | no | Allow disabling TLS certificate verification (MITM risk!). |
//! | `telemetry` | no | OpenTelemetry exporter for producer/consumer metrics. |
//! | `unstable-protocol` | no | Enables experimental protocol APIs (Share Consumer, KIP-932). APIs under this feature may change without semver notice. |
//!
//! To disable the default compression codecs and pick only what you need:
//!
//! ```toml
//! [dependencies]
//! krafka = { version = "0.9.1", default-features = false, features = ["lz4"] }
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]
#![deny(unsafe_code)]

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
