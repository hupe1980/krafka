//! # Krafka
//!
//! A pure Rust, async-native Apache Kafka client.
//!
//! Krafka provides high-performance, safe, and idiomatic Rust APIs for
//! producing and consuming messages from Apache Kafka clusters.
//!
//! ## Features
//!
//! - **Pure Rust**: No librdkafka or C bindings
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
//! while let Some(msg) = consumer.recv().await? {
//!     println!("{:?}", msg);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! ## Cargo Features
//!
//! | Feature | Default | Description |
//! |---------|---------|-------------|
//! | `compression` | **yes** | Enables all compression codecs (`gzip` + `snappy` + `lz4` + `zstd`). |
//! | `gzip` | via `compression` | Gzip record batch compression via `flate2`. |
//! | `snappy` | via `compression` | Snappy compression via `snap`. |
//! | `lz4` | via `compression` | LZ4 compression via `lz4_flex`. |
//! | `zstd` | via `compression` | Zstd compression via `zstd` (requires C toolchain). |
//! | `aws-msk` | no | AWS MSK IAM authentication with SDK credential chain. |
//! | `schema-registry` | no | Confluent Schema Registry HTTP client. |
//! | `aws-glue-schema-registry` | no | AWS Glue Schema Registry SDK client. |
//! | `socks5` | no | SOCKS5 proxy support via `tokio-socks`. |
//! | `danger-insecure-tls` | no | Allow disabling TLS certificate verification (MITM risk!). |
//!
//! To disable the default compression codecs and pick only what you need:
//!
//! ```toml
//! [dependencies]
//! krafka = { version = "0.4", default-features = false, features = ["lz4"] }
//! ```

#![warn(missing_docs)]
#![warn(rust_2018_idioms)]
#![deny(unsafe_code)]
#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod admin;
pub mod auth;
pub mod consumer;
pub mod error;
pub mod interceptor;
pub mod metadata;
pub mod metrics;
pub mod network;
pub mod producer;
pub mod protocol;
pub mod schema_registry;
pub mod tracing_ext;
pub mod util;

pub use error::{KrafkaError, Result};

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
