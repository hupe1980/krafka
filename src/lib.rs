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
//!
//! To disable the default compression codecs and pick only what you need:
//!
//! ```toml
//! [dependencies]
//! krafka = { version = "0.6", default-features = false, features = ["lz4"] }
//! ```

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
#[cfg(feature = "unstable-protocol")]
#[cfg_attr(docsrs, doc(cfg(feature = "unstable-protocol")))]
pub mod share_consumer;
#[cfg(feature = "telemetry")]
#[cfg_attr(docsrs, doc(cfg(feature = "telemetry")))]
pub mod telemetry;
pub mod tracing_ext;
pub mod util;

pub use error::{KrafkaError, ProtocolErrorKind, Result};
pub use metadata::MetadataRecoveryStrategy;

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

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod cached_schema_registry_inherent_methods_tests {
    use std::future::Future;
    use std::pin::Pin;

    use crate::Result;
    use crate::schema_registry::glue::{
        CachedGlueSchemaRegistry, GlueDataFormat, GlueSchema, GlueSchemaVersionId,
    };
    use crate::schema_registry::{
        CachedSchemaRegistry, Schema, SchemaReference, SchemaType, SchemaVersion,
    };

    struct MockRegistry;

    impl crate::schema_registry::SchemaRegistryClient for MockRegistry {
        fn get_schema_by_id(
            &self,
            id: u32,
        ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
            Box::pin(async move { Ok(Schema::new(id, SchemaType::Avro, r#"{"type":"string"}"#)) })
        }

        fn get_latest_schema(
            &self,
            subject: &str,
        ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
            let subject = subject.to_string();
            Box::pin(async move {
                Ok(Schema::new(7, SchemaType::Avro, r#"{"type":"string"}"#)
                    .with_subject(subject, 1))
            })
        }

        fn get_schema_by_version(
            &self,
            subject: &str,
            version: SchemaVersion,
        ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
            let subject = subject.to_string();
            Box::pin(async move {
                Ok(Schema::new(9, SchemaType::Avro, r#"{"type":"string"}"#)
                    .with_subject(subject, version))
            })
        }

        fn register_schema(
            &self,
            _subject: &str,
            _schema: &str,
            _schema_type: SchemaType,
            _references: &[SchemaReference],
        ) -> Pin<Box<dyn Future<Output = Result<u32>> + Send + '_>> {
            Box::pin(async { Ok(42) })
        }
    }

    struct MockGlueRegistry;

    impl crate::schema_registry::glue::GlueSchemaRegistryClient for MockGlueRegistry {
        fn get_schema_by_version_id(
            &self,
            id: GlueSchemaVersionId,
        ) -> Pin<Box<dyn Future<Output = Result<GlueSchema>> + Send + '_>> {
            Box::pin(async move {
                Ok(GlueSchema::new(
                    id,
                    GlueDataFormat::Avro,
                    r#"{"type":"string"}"#,
                ))
            })
        }

        fn register_schema(
            &self,
            _schema_name: &str,
            _schema: &str,
            _data_format: GlueDataFormat,
        ) -> Pin<Box<dyn Future<Output = Result<GlueSchemaVersionId>> + Send + '_>> {
            let id = "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
            Box::pin(async move { Ok(id) })
        }
    }

    #[tokio::test]
    async fn cached_schema_registry_methods_work_without_trait_import() {
        let cached = CachedSchemaRegistry::new(MockRegistry);

        let by_id = cached.get_schema_by_id(1).await.unwrap();
        assert_eq!(by_id.id, 1);

        let latest = cached.get_latest_schema("orders-value").await.unwrap();
        assert_eq!(latest.id, 7);

        let by_version = cached
            .get_schema_by_version("orders-value", 3)
            .await
            .unwrap();
        assert_eq!(by_version.id, 9);

        let registered = cached
            .register_schema(
                "orders-value",
                r#"{"type":"string"}"#,
                SchemaType::Avro,
                &[],
            )
            .await
            .unwrap();
        assert_eq!(registered, 42);
    }

    #[tokio::test]
    async fn cached_glue_registry_methods_work_without_trait_import() {
        let cached = CachedGlueSchemaRegistry::new(MockGlueRegistry);
        let version_id: GlueSchemaVersionId =
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();

        let schema = cached.get_schema_by_version_id(version_id).await.unwrap();
        assert_eq!(schema.schema_version_id, version_id);

        let registered = cached
            .register_schema("orders-value", r#"{"type":"string"}"#, GlueDataFormat::Avro)
            .await
            .unwrap();
        assert_eq!(registered, version_id);
    }
}
