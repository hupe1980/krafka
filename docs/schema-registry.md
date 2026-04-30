---
layout: default
title: Schema Registry
nav_order: 9
description: "Schema registry integration for Avro, Protobuf, and JSON Schema workflows"
---

# Schema Registry Guide

This guide covers Krafka's schema registry integration, including the Confluent wire format, subject naming strategies, caching, and the built-in HTTP client.

## Overview

Krafka provides schema registry support at two levels:

- **Always available (no extra dependencies):** Wire format encode/decode, subject name strategies, the `SchemaRegistryClient` trait, `CachedSchemaRegistry`, the Glue wire format, the `GlueSchemaRegistryClient` trait, and `CachedGlueSchemaRegistry`.
- **Feature-gated (`schema-registry`):** `ConfluentSchemaRegistry` HTTP client for the [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/).
- **Feature-gated (`aws-glue-schema-registry`):** `AwsGlueSchemaRegistry` SDK client for the [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html).

Krafka handles the **wire format framing** and **registry communication**. Actual serialization (Avro, Protobuf, JSON Schema) is left to your preferred library — this keeps the dependency tree lean and gives you full control over serde.

## Wire Format

The [Confluent wire format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format) prepends a 5-byte header to every serialized payload:

```text
┌──────────┬────────────────────┬──────────────────┐
│ 0x00 (1B)│ Schema ID (4B, BE) │ Payload (N bytes)│
└──────────┴────────────────────┴──────────────────┘
```

Use `encode_wire_format()` and `decode_wire_format()`:

```rust
use krafka::schema_registry::{encode_wire_format, decode_wire_format};

// Encoding: prepend wire format header to serialized data
let avro_bytes: Vec<u8> = serialize_with_avro(&my_record);
let framed = encode_wire_format(schema_id, &avro_bytes);
// framed is ready to use as a Kafka record value

// Decoding: strip the header to get schema ID + raw payload
let (schema_id, payload) = decode_wire_format(&record.value.unwrap())?;
// Use schema_id to look up the schema, then deserialize payload
```

### Zero-Copy Decoding with `Bytes`

When working with `Bytes` values (e.g., from `CompactedTable`), use `decode_wire_format_bytes()` for zero-copy slicing — the returned payload shares the same backing allocation:

```rust
use krafka::schema_registry::decode_wire_format_bytes;

// value is &Bytes from CompactedTable::get()
let (schema_id, payload) = decode_wire_format_bytes(value)?;
// payload is a Bytes slice — no copy, no allocation
```

## Subject Name Strategies

A **subject** determines where a schema is registered and looked up in the registry. Krafka supports three strategies matching the Confluent conventions:

| Strategy | Subject format | Best for |
|----------|---------------|----------|
| `TopicName` (default) | `{topic}-key` / `{topic}-value` | One schema per topic |
| `RecordName` | `{record_name}` | Same type across multiple topics |
| `TopicRecordName` | `{topic}-{record_name}` | Per-topic evolution of shared types |

```rust
use krafka::schema_registry::SubjectNameStrategy;

let strategy = SubjectNameStrategy::TopicName;
let subject = strategy.subject_name("orders", None, false)?;
assert_eq!(subject, "orders-value");

let strategy = SubjectNameStrategy::RecordName;
let subject = strategy.subject_name("orders", Some("com.example.Order"), false)?;
assert_eq!(subject, "com.example.Order");
```

## Compatible Registries

The `ConfluentSchemaRegistry` HTTP client uses the standard Confluent REST API and works with any registry that implements it:

| Registry | Notes |
|----------|-------|
| [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/) | The reference implementation |
| [Karapace](https://github.com/Aiven-Open/karapace) (Aiven, Apache 2.0) | Drop-in replacement; compatible with Confluent SR API level 6.1.1 |
| [Apicurio Registry](https://www.apicur.io/registry/) (Red Hat, Apache 2.0) | Enable its [Confluent-compatible API](https://www.apicur.io/registry/docs/apicurio-registry/3.0.x/getting-started/assembly-configuring-the-registry.html) mode |

No code changes are needed — just point `ConfluentSchemaRegistry` at the compatible URL.

For AWS environments, the `AwsGlueSchemaRegistry` SDK client communicates with the [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html) via the AWS SDK.

## Schema Registry Client Trait

The `SchemaRegistryClient` trait allows pluggable registry backends:

```rust
use krafka::schema_registry::{SchemaRegistryClient, Schema, SchemaId, SchemaType, SchemaVersion, SchemaReference};
use krafka::error::Result;
use std::future::Future;
use std::pin::Pin;

struct MyRegistry { /* ... */ }

impl SchemaRegistryClient for MyRegistry {
    fn get_schema_by_id(
        &self,
        id: SchemaId,
    ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
        Box::pin(async move {
            // Fetch from your registry backend
            Ok(Schema::new(id, SchemaType::Avro, r#"{"type":"string"}"#))
        })
    }

    fn get_latest_schema(
        &self,
        subject: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
        // ...
        # todo!()
    }

    fn get_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
        // ...
        # todo!()
    }

    fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Pin<Box<dyn Future<Output = Result<SchemaId>> + Send + '_>> {
        // ...
        # todo!()
    }
}
```

## Caching

`CachedSchemaRegistry` wraps any `SchemaRegistryClient` with an in-memory ID-to-schema cache. Schema IDs are immutable in the registry, so cached entries never expire unless you opt into bounded eviction with `with_max_entries()`. Concurrent cold misses for the same schema ID are also coalesced, so only one upstream request runs per ID at a time:

```rust
use krafka::schema_registry::CachedSchemaRegistry;

let cached = CachedSchemaRegistry::new(my_registry);

// First call fetches from the registry
let schema = cached.get_schema_by_id(1).await?;

// Second call is served from cache (no network request)
let same = cached.get_schema_by_id(1).await?;

// get_latest_schema always forwards but populates the ID cache
let latest = cached.get_latest_schema("orders-value").await?;
let by_id = cached.get_schema_by_id(latest.id).await?; // cache hit

// Inspect or clear the cache
println!("Cached schemas: {}", cached.cache_len());
cached.clear_cache();

// Optional: bound cache growth by evicting the oldest inserted IDs
let bounded = CachedSchemaRegistry::with_max_entries(other_registry, 1024);
```

`CachedGlueSchemaRegistry` follows the same rules for AWS Glue schema version IDs: immutable-ID caching, concurrent miss coalescing, and optional bounded eviction via `with_max_entries()`.

## Confluent Schema Registry HTTP Client

Enable the `schema-registry` feature to use the built-in HTTP client:

```toml
[dependencies]
krafka = { version = "0.7.0", features = ["schema-registry"] }
```

### Basic Usage

```rust
use krafka::schema_registry::{
    ConfluentSchemaRegistry, CachedSchemaRegistry, SchemaType,
    encode_wire_format, decode_wire_format,
};

// Create and cache the client
let client = ConfluentSchemaRegistry::new("http://localhost:8081");
let registry = CachedSchemaRegistry::new(client);

// Register a schema
let schema_id = registry.register_schema(
    "orders-value",
    r#"{"type":"record","name":"Order","fields":[{"name":"id","type":"string"}]}"#,
    SchemaType::Avro,
    &[],
).await?;

// Encode with wire format
let avro_bytes = serialize_order(&order);
let wire_bytes = encode_wire_format(schema_id, &avro_bytes);
producer.send("orders", Some(b"key"), &wire_bytes).await?;

// Decode from wire format
let records = consumer.poll(Duration::from_secs(1)).await?;
for record in &records {
    if let Some(value) = &record.value {
        let (id, payload) = decode_wire_format(value)?;
        let schema = registry.get_schema_by_id(id).await?;
        let order = deserialize_order(payload, &schema.schema);
    }
}
```

### Authentication

```rust
use krafka::schema_registry::ConfluentSchemaRegistry;

// Basic auth
let client = ConfluentSchemaRegistry::builder()
    .url("https://registry.example.com")
    .basic_auth("user", "password")
    .build()?;

// Bearer token
let client = ConfluentSchemaRegistry::builder()
    .url("https://registry.example.com")
    .bearer_token("my-jwt-token")
    .build()?;

// Custom timeout
let client = ConfluentSchemaRegistry::builder()
    .url("http://localhost:8081")
    .request_timeout(Duration::from_secs(10))
    .build()?;
```

### Additional Operations

The HTTP client provides extra methods beyond the trait:

```rust
// Check schema compatibility (supports references)
let compatible = client.check_compatibility(
    "orders-value",
    &new_schema,
    SchemaType::Avro,
    &[],  // pass SchemaReference values if the schema has dependencies
).await?;

// List all subjects
let subjects = client.get_subjects().await?;

// List all versions of a subject
let versions = client.get_versions("orders-value").await?;

// Delete a subject (soft-delete)
let deleted = client.delete_subject("orders-value", false).await?;

// Delete a subject (permanent hard-delete)
let deleted = client.delete_subject("orders-value", true).await?;
```

## Schema References

For schemas with dependencies (e.g., Protobuf imports, Avro references), pass `SchemaReference` values when registering:

```rust
use krafka::schema_registry::{SchemaReference, SchemaType};

let refs = vec![
    SchemaReference::new("com.example.Address", "address-value", 1),
];

let id = registry.register_schema(
    "order-value",
    &order_schema,
    SchemaType::Avro,
    &refs,
).await?;
```

## Using with CompactedTable

`CompactedTable` stores key-value pairs as `Bytes`. When the values are Confluent wire-format encoded, use `decode_wire_format_bytes()` for zero-copy decoding:

```rust
use krafka::consumer::CompactedTopicConsumer;
use krafka::schema_registry::{
    decode_wire_format_bytes, CachedSchemaRegistry, ConfluentSchemaRegistry,
};

// Set up the schema registry client with caching
let registry = CachedSchemaRegistry::new(
    ConfluentSchemaRegistry::new("http://localhost:8081"),
);

// Build and start the compacted topic consumer
let ctc = CompactedTopicConsumer::builder()
    .bootstrap_servers("localhost:9092")
    .topic("user-profiles")
    .build()
    .await?;

// Look up a single key
if let Some(value) = ctc.table().get(b"user-42") {
    let (schema_id, payload) = decode_wire_format_bytes(value)?;
    let schema = registry.get_schema_by_id(schema_id).await?;
    let user = deserialize_avro(&payload, &schema.schema);
}

// Iterate all entries
for (key, value) in ctc.table() {
    let (schema_id, payload) = decode_wire_format_bytes(value)?;
    let schema = registry.get_schema_by_id(schema_id).await?;
    // schema_id lookups are cached after the first fetch
}
```

Since schema IDs are immutable, `CachedSchemaRegistry` ensures you only make one HTTP round-trip per schema ID, even when iterating thousands of table entries.

## AWS Glue Schema Registry

For AWS MSK users, Krafka provides first-class [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html) support. Glue uses a completely different wire format and UUID-based schema version IDs.

### Glue Wire Format

The Glue wire format uses an 18-byte header (vs Confluent's 5-byte header):

```text
┌──────────┬─────────────┬──────────────────────┬──────────────────┐
│ 0x03 (1B)│ Compr. (1B) │ Schema Version UUID  │ Payload (N bytes)│
│          │             │      (16B, BE)       │                  │
└──────────┴─────────────┴──────────────────────┴──────────────────┘
```

- **Byte 0**: Header version byte (`0x03`)
- **Byte 1**: Compression indicator (`0x00` = none, `0x05` = ZLIB)
- **Bytes 2–17**: Schema version ID as a 128-bit UUID (big-endian)
- **Bytes 18+**: Payload (ZLIB-compressed if byte 1 is `0x05`)

Encode and decode with the Glue-specific functions:

```rust
use krafka::schema_registry::glue::{
    encode_glue_wire_format, decode_glue_wire_format,
    GlueSchemaVersionId, GlueCompression,
};

// Encoding
let uuid: GlueSchemaVersionId = "550e8400-e29b-41d4-a716-446655440000".parse()?;
let framed = encode_glue_wire_format(uuid, &avro_bytes, GlueCompression::None)?;
producer.send("my-topic", Some(b"key"), &framed).await?;

// Decoding
let (version_id, payload) = decode_glue_wire_format(&record_bytes)?;
```

ZLIB compression is supported out of the box:

```rust
// Encode with ZLIB compression
let framed = encode_glue_wire_format(uuid, &payload, GlueCompression::Zlib)?;

// Decode automatically decompresses
let (version_id, original) = decode_glue_wire_format(&framed)?;
```

> **Note:** ZLIB decompression output is capped at 128 MiB to protect against decompression bombs, matching the limit used by record-batch decompression.

For `Bytes` values (e.g., from `CompactedTable`), use `decode_glue_wire_format_bytes()` for zero-copy slicing on uncompressed payloads.

### Glue Client Trait

The `GlueSchemaRegistryClient` trait allows pluggable backends (always available, no feature required):

```rust
use krafka::schema_registry::glue::{
    GlueSchemaRegistryClient, GlueSchema, GlueSchemaVersionId, GlueDataFormat,
};
```

### AWS SDK Client

Enable the `aws-glue-schema-registry` feature to use the built-in SDK client:

```toml
[dependencies]
krafka = { version = "0.7.0", features = ["aws-glue-schema-registry"] }
```

```rust
use krafka::schema_registry::glue::{
    AwsGlueSchemaRegistry, CachedGlueSchemaRegistry,
    decode_glue_wire_format, GlueSchemaRegistryClient,
};

// Create from AWS config
let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
    .load()
    .await;
let glue_client = aws_sdk_glue::Client::new(&config);

let registry = CachedGlueSchemaRegistry::new(
    AwsGlueSchemaRegistry::new(glue_client, "my-registry"),
);

// Decode and look up schema
let (version_id, payload) = decode_glue_wire_format(&record_bytes)?;
let schema = registry.get_schema_by_version_id(version_id).await?;
// Deserialize payload using schema.schema_definition
```

Advanced configuration via the builder:

```rust
let registry = AwsGlueSchemaRegistry::builder(glue_client)
    .registry_name("my-custom-registry")
    .auto_register(true)  // auto-create schemas on first register
    .poll_max_attempts(15)
    .poll_interval(Duration::from_secs(2))
    .build();
```

### Confluent vs Glue: Quick Comparison

| Aspect | Confluent | AWS Glue |
|--------|-----------|----------|
| Wire format header | 5 bytes | 18 bytes |
| Schema identifier | `u32` (integer ID) | UUID (128-bit) |
| Compression | Not in wire format | ZLIB in header |
| API | HTTP REST | AWS SDK |
| Feature flag | `schema-registry` | `aws-glue-schema-registry` |
| Trait | `SchemaRegistryClient` | `GlueSchemaRegistryClient` |
| Caching wrapper | `CachedSchemaRegistry` | `CachedGlueSchemaRegistry` |

## Next Steps

- [Producer Guide](producer.md) — sending schema-encoded records
- [Consumer Guide](consumer.md) — consuming and decoding records
- [Authentication Guide](authentication.md) — securing connections
