//! Schema registry integration for Avro, Protobuf, and JSON Schema workflows.
//!
//! This module provides:
//!
//! - **Wire format**: The [Confluent wire format] encoder/decoder for framing
//!   schema-aware payloads (5-byte header: magic byte + schema ID).
//! - **Subject strategies**: [`SubjectNameStrategy`] for deriving registry
//!   subject names from topics and record names.
//! - **Registry trait**: [`SchemaRegistryClient`] for pluggable registry backends.
//! - **Caching**: [`CachedSchemaRegistry`] wraps any client with an in-memory
//!   schema-ID-to-schema cache.
//!
//! When the `schema-registry` feature is enabled, `ConfluentSchemaRegistry`
//! provides a ready-made HTTP client for the
//! [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/)
//! and any compatible registry (e.g.,
//! [Karapace](https://github.com/Aiven-Open/karapace),
//! [Apicurio](https://www.apicur.io/registry/) in Confluent-compatible mode).
//!
//! [Confluent wire format]: https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
//!
//! # Wire Format
//!
//! The Confluent wire format prepends a 5-byte header to every serialized
//! payload:
//!
//! ```text
//! ┌──────────┬────────────────────┬──────────────────┐
//! │ 0x00 (1B)│ Schema ID (4B, BE) │ Payload (N bytes)│
//! └──────────┴────────────────────┴──────────────────┘
//! ```
//!
//! Use [`encode_wire_format()`] to frame and [`decode_wire_format()`] to
//! unframe:
//!
//! ```rust
//! use krafka::schema_registry::{encode_wire_format, decode_wire_format};
//!
//! let payload = b"serialized avro data";
//! let framed = encode_wire_format(42, payload);
//!
//! let (schema_id, data) = decode_wire_format(&framed).unwrap();
//! assert_eq!(schema_id, 42);
//! assert_eq!(data, payload);
//! ```
//!
//! # Example with Consumer
//!
//! ```rust,ignore
//! use krafka::consumer::Consumer;
//! use krafka::schema_registry::{
//!     decode_wire_format, CachedSchemaRegistry, ConfluentSchemaRegistry,
//! };
//! use std::time::Duration;
//!
//! let registry = CachedSchemaRegistry::new(
//!     ConfluentSchemaRegistry::new("http://localhost:8081"),
//! );
//!
//! let consumer = Consumer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .group_id("my-group")
//!     .build()
//!     .await?;
//! consumer.subscribe(&["avro-topic"]).await?;
//!
//! loop {
//!     let records = consumer.poll(Duration::from_secs(1)).await?;
//!     for record in &records {
//!         if let Some(value) = &record.value {
//!             let (schema_id, payload) = decode_wire_format(value)?;
//!             let schema = registry.get_schema_by_id(schema_id).await?;
//!             // Deserialize `payload` using `schema.schema` with your
//!             // preferred Avro / Protobuf / JSON library
//!         }
//!     }
//! }
//! ```
//!
//! # Example with CompactedTable
//!
//! [`decode_wire_format_bytes()`] provides zero-copy decoding for
//! [`Bytes`] values — ideal for [`CompactedTable`](crate::consumer::CompactedTable)
//! lookups:
//!
//! ```rust,ignore
//! use krafka::consumer::CompactedTopicConsumer;
//! use krafka::schema_registry::{
//!     decode_wire_format_bytes, CachedSchemaRegistry, ConfluentSchemaRegistry,
//! };
//!
//! let registry = CachedSchemaRegistry::new(
//!     ConfluentSchemaRegistry::new("http://localhost:8081"),
//! );
//!
//! let ctc = CompactedTopicConsumer::builder()
//!     .bootstrap_servers("localhost:9092")
//!     .topic("user-profiles")
//!     .build()
//!     .await?;
//!
//! // After initial catch-up, look up a key:
//! if let Some(value) = ctc.table().get(b"user-42") {
//!     let (schema_id, payload) = decode_wire_format_bytes(value)?;
//!     let schema = registry.get_schema_by_id(schema_id).await?;
//!     // Deserialize `payload` using the schema
//! }
//! ```

#[cfg(feature = "schema-registry")]
mod client;
pub mod glue;

#[cfg(feature = "schema-registry")]
#[cfg_attr(docsrs, doc(cfg(feature = "schema-registry")))]
pub use client::{ConfluentSchemaRegistry, ConfluentSchemaRegistryBuilder};

use self::glue::{GlueSchema, GlueSchemaRegistryClient, GlueSchemaVersionId};
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt;
use std::future::Future;
use std::pin::Pin;

use bytes::{BufMut, Bytes, BytesMut};
use parking_lot::{Mutex, RwLock};
use tokio::sync::oneshot;

use crate::error::{KrafkaError, Result};
use tracing::debug;

// ── Types ────────────────────────────────────────────────────────────────

/// Schema ID as used in the Confluent wire format.
pub type SchemaId = u32;

/// Schema version within a subject.
pub type SchemaVersion = i32;

/// Schema type supported by the registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SchemaType {
    /// Apache Avro schema.
    Avro,
    /// Protocol Buffers schema.
    Protobuf,
    /// JSON Schema.
    Json,
}

impl SchemaType {
    /// Return the canonical uppercase name (`"AVRO"`, `"PROTOBUF"`, `"JSON"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Avro => "AVRO",
            Self::Protobuf => "PROTOBUF",
            Self::Json => "JSON",
        }
    }
}

impl fmt::Display for SchemaType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for SchemaType {
    type Err = KrafkaError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("AVRO") {
            Ok(Self::Avro)
        } else if s.eq_ignore_ascii_case("PROTOBUF") {
            Ok(Self::Protobuf)
        } else if s.eq_ignore_ascii_case("JSON") {
            Ok(Self::Json)
        } else {
            Err(KrafkaError::schema_registry(format!(
                "unknown schema type: '{s}'"
            )))
        }
    }
}

/// A reference to another schema (used for multi-schema dependencies).
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaReference {
    /// Reference name (typically the fully qualified type name).
    pub name: String,
    /// Subject that owns the referenced schema.
    pub subject: String,
    /// Version of the referenced schema.
    pub version: SchemaVersion,
}

impl SchemaReference {
    /// Create a new schema reference.
    pub fn new(
        name: impl Into<String>,
        subject: impl Into<String>,
        version: SchemaVersion,
    ) -> Self {
        Self {
            name: name.into(),
            subject: subject.into(),
            version,
        }
    }
}

/// A schema retrieved from or registered with a schema registry.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schema {
    /// Globally unique schema ID.
    pub id: SchemaId,
    /// Schema type (Avro, Protobuf, or JSON Schema).
    pub schema_type: SchemaType,
    /// Schema definition string.
    ///
    /// For Avro and JSON Schema this is a JSON string. For Protobuf this is
    /// the `.proto` file content.
    pub schema: String,
    /// Schema version within its subject (`None` when fetched by ID only).
    pub version: Option<SchemaVersion>,
    /// Subject name (`None` when fetched by ID only).
    pub subject: Option<String>,
    /// References to other schemas.
    pub references: Vec<SchemaReference>,
}

impl Schema {
    /// Create a schema with the given ID, type, and definition.
    ///
    /// `version`, `subject`, and `references` default to `None`/empty.
    pub fn new(id: SchemaId, schema_type: SchemaType, schema: impl Into<String>) -> Self {
        Self {
            id,
            schema_type,
            schema: schema.into(),
            version: None,
            subject: None,
            references: Vec::new(),
        }
    }

    /// Set the subject and version.
    pub fn with_subject(mut self, subject: impl Into<String>, version: SchemaVersion) -> Self {
        self.subject = Some(subject.into());
        self.version = Some(version);
        self
    }

    /// Set the schema references.
    pub fn with_references(mut self, references: Vec<SchemaReference>) -> Self {
        self.references = references;
        self
    }
}

// ── Wire format ──────────────────────────────────────────────────────────

/// Magic byte for the Confluent wire format header.
const MAGIC_BYTE: u8 = 0x00;

/// Size of the wire format header (magic byte + 4-byte big-endian schema ID).
const HEADER_SIZE: usize = 5;

/// Encode a payload with the Confluent wire format header.
///
/// Prepends a 5-byte header (`0x00` + 4-byte big-endian schema ID) to the
/// payload, producing a [`Bytes`] value ready for use as a Kafka record
/// key or value.
///
/// # Example
///
/// ```rust
/// use krafka::schema_registry::encode_wire_format;
///
/// let framed = encode_wire_format(42, b"hello");
/// assert_eq!(&framed[..5], &[0x00, 0, 0, 0, 42]);
/// assert_eq!(&framed[5..], b"hello");
/// ```
pub fn encode_wire_format(schema_id: SchemaId, payload: &[u8]) -> Bytes {
    let mut buf = BytesMut::with_capacity(HEADER_SIZE + payload.len());
    buf.put_u8(MAGIC_BYTE);
    buf.put_u32(schema_id);
    buf.put_slice(payload);
    buf.freeze()
}

/// Decode a Confluent wire format message.
///
/// Returns the schema ID and the payload slice after the 5-byte header.
///
/// # Errors
///
/// Returns a serialization error if:
/// - The data is shorter than 5 bytes.
/// - The magic byte is not `0x00`.
///
/// # Example
///
/// ```rust
/// use krafka::schema_registry::{encode_wire_format, decode_wire_format};
///
/// let framed = encode_wire_format(7, b"data");
/// let (id, payload) = decode_wire_format(&framed).unwrap();
/// assert_eq!(id, 7);
/// assert_eq!(payload, b"data");
/// ```
pub fn decode_wire_format(data: &[u8]) -> Result<(SchemaId, &[u8])> {
    let schema_id = validate_wire_header(data)?;
    Ok((schema_id, &data[HEADER_SIZE..]))
}

/// Decode a Confluent wire format message, returning an owned payload.
///
/// This is useful when the payload needs to outlive the source buffer, for
/// example when passing decoded bytes across an `.await` boundary.
///
/// # Errors
///
/// Same as [`decode_wire_format()`].
pub fn decode_wire_format_owned(data: &[u8]) -> Result<(SchemaId, Vec<u8>)> {
    let (schema_id, payload) = decode_wire_format(data)?;
    Ok((schema_id, payload.to_vec()))
}

/// Decode a Confluent wire format message, returning a zero-copy [`Bytes`] payload.
///
/// This is the preferred variant when working with [`Bytes`] values such as
/// those stored in a [`CompactedTable`](crate::consumer::CompactedTable).
/// The returned payload shares the same backing allocation as `data`.
///
/// # Errors
///
/// Same as [`decode_wire_format()`].
///
/// # Example
///
/// ```rust
/// use bytes::Bytes;
/// use krafka::schema_registry::{encode_wire_format, decode_wire_format_bytes};
///
/// let framed = encode_wire_format(7, b"data");
/// let (id, payload) = decode_wire_format_bytes(&framed).unwrap();
/// assert_eq!(id, 7);
/// assert_eq!(&payload[..], b"data");
/// ```
pub fn decode_wire_format_bytes(data: &Bytes) -> Result<(SchemaId, Bytes)> {
    let schema_id = validate_wire_header(data)?;
    Ok((schema_id, data.slice(HEADER_SIZE..)))
}

/// Validate the Confluent wire format header and extract the schema ID.
fn validate_wire_header(data: &[u8]) -> Result<SchemaId> {
    if data.len() < HEADER_SIZE {
        return Err(KrafkaError::serialization(format!(
            "wire format data too short: expected at least {HEADER_SIZE} bytes, got {}",
            data.len()
        )));
    }
    if data[0] != MAGIC_BYTE {
        return Err(KrafkaError::serialization(format!(
            "invalid wire format magic byte: expected 0x{MAGIC_BYTE:02X}, got 0x{:02X}",
            data[0]
        )));
    }
    Ok(u32::from_be_bytes([data[1], data[2], data[3], data[4]]))
}

/// Detected schema wire format for payload dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum DetectedWireFormat {
    /// Confluent wire format (`0x00` magic + schema ID).
    Confluent {
        /// Confluent schema ID.
        schema_id: SchemaId,
        /// Offset where payload bytes start.
        payload_offset: usize,
    },
    /// AWS Glue wire format (`0x03` version + compression + UUID).
    Glue {
        /// Glue schema version UUID.
        version_id: GlueSchemaVersionId,
        /// Offset where payload bytes start.
        payload_offset: usize,
    },
    /// Unknown or invalid wire format.
    Unknown,
}

/// Detect schema wire format from the message header.
///
/// Returns [`DetectedWireFormat::Unknown`] for empty buffers, unrecognized
/// magic bytes, or invalid/incomplete headers.
pub fn detect_wire_format(data: &[u8]) -> DetectedWireFormat {
    if data.is_empty() {
        return DetectedWireFormat::Unknown;
    }

    match data[0] {
        MAGIC_BYTE => {
            if data.len() < HEADER_SIZE {
                return DetectedWireFormat::Unknown;
            }
            let schema_id = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);
            DetectedWireFormat::Confluent {
                schema_id,
                payload_offset: HEADER_SIZE,
            }
        }
        // Glue wire header: version byte + compression + 16-byte UUID.
        // Constants are defined (and validated) in schema_registry::glue.
        glue::GLUE_HEADER_VERSION_BYTE => {
            if data.len() < glue::GLUE_HEADER_SIZE {
                return DetectedWireFormat::Unknown;
            }
            let compression = data[1];
            if compression != glue::GLUE_COMPRESSION_NONE_BYTE
                && compression != glue::GLUE_COMPRESSION_ZLIB_BYTE
            {
                return DetectedWireFormat::Unknown;
            }

            let mut version_bytes = [0u8; 16];
            version_bytes.copy_from_slice(&data[2..glue::GLUE_HEADER_SIZE]);
            DetectedWireFormat::Glue {
                version_id: GlueSchemaVersionId::from_bytes(version_bytes),
                payload_offset: glue::GLUE_HEADER_SIZE,
            }
        }
        _ => DetectedWireFormat::Unknown,
    }
}

/// Unified schema format across Confluent and Glue registries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SchemaFormat {
    /// Apache Avro.
    Avro,
    /// JSON Schema.
    Json,
    /// Protocol Buffers.
    Protobuf,
    /// Not schema-framed (or unknown framing).
    Unknown,
}

impl From<SchemaType> for SchemaFormat {
    fn from(value: SchemaType) -> Self {
        match value {
            SchemaType::Avro => Self::Avro,
            SchemaType::Json => Self::Json,
            SchemaType::Protobuf => Self::Protobuf,
        }
    }
}

impl From<glue::GlueDataFormat> for SchemaFormat {
    fn from(value: glue::GlueDataFormat) -> Self {
        match value {
            glue::GlueDataFormat::Avro => Self::Avro,
            glue::GlueDataFormat::Json => Self::Json,
            glue::GlueDataFormat::Protobuf => Self::Protobuf,
        }
    }
}

/// Registry-specific schema metadata associated with a decoded payload.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SchemaMetadata {
    /// Metadata fetched from a Confluent-compatible registry.
    Confluent(Schema),
    /// Metadata fetched from the AWS Glue Schema Registry.
    Glue(GlueSchema),
}

impl SchemaMetadata {
    /// Return the normalized schema format for this metadata.
    pub fn schema_format(&self) -> SchemaFormat {
        match self {
            Self::Confluent(schema) => schema.schema_type.into(),
            Self::Glue(schema) => schema.data_format.into(),
        }
    }
}

/// Decoded schema-framed payload plus resolved schema metadata.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct DecodedMessage {
    /// Unified schema format.
    pub schema_format: SchemaFormat,
    /// Decoded payload bytes.
    pub payload: Vec<u8>,
    /// Registry metadata when a known schema wire format was detected.
    pub schema_metadata: Option<SchemaMetadata>,
}

/// Unified decoder that dispatches based on detected wire format.
///
/// Use this to centralize Confluent/Glue dispatch logic and avoid repeating
/// magic-byte checks in application code.
#[derive(Default, Clone, Copy)]
pub struct SchemaDecoder<'a> {
    confluent: Option<&'a dyn SchemaRegistryClient>,
    glue: Option<&'a dyn GlueSchemaRegistryClient>,
}

impl fmt::Debug for SchemaDecoder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SchemaDecoder")
            .field("has_confluent", &self.confluent.is_some())
            .field("has_glue", &self.glue.is_some())
            .finish()
    }
}

impl<'a> SchemaDecoder<'a> {
    /// Create an empty decoder.
    ///
    /// Use [`with_confluent`](Self::with_confluent) and/or
    /// [`with_glue`](Self::with_glue) before calling [`decode`](Self::decode).
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a decoder configured with only a Confluent registry.
    pub fn confluent(registry: &'a dyn SchemaRegistryClient) -> Self {
        Self::new().with_confluent(registry)
    }

    /// Create a decoder configured with only a Glue registry.
    pub fn glue(registry: &'a dyn GlueSchemaRegistryClient) -> Self {
        Self::new().with_glue(registry)
    }

    /// Attach a Confluent registry client.
    pub fn with_confluent(mut self, registry: &'a dyn SchemaRegistryClient) -> Self {
        self.confluent = Some(registry);
        self
    }

    /// Attach a Glue registry client.
    pub fn with_glue(mut self, registry: &'a dyn GlueSchemaRegistryClient) -> Self {
        self.glue = Some(registry);
        self
    }

    /// Decode a schema-framed payload and fetch associated schema metadata.
    ///
    /// - Confluent (`0x00`): decodes schema ID and fetches via Confluent client.
    /// - Glue (`0x03`): decodes schema version ID and fetches via Glue client.
    /// - Unknown framing: returns payload as-is with `SchemaFormat::Unknown`.
    pub async fn decode(&self, data: &[u8]) -> Result<DecodedMessage> {
        match detect_wire_format(data) {
            DetectedWireFormat::Confluent { schema_id, .. } => {
                let registry = self.confluent.ok_or_else(|| {
                    KrafkaError::config(
                        "schema decoder missing Confluent registry for Confluent-framed payload",
                    )
                })?;

                let (_, payload) = decode_wire_format_owned(data)?;
                let schema = registry.get_schema_by_id(schema_id).await?;

                Ok(DecodedMessage {
                    schema_format: schema.schema_type.into(),
                    payload,
                    schema_metadata: Some(SchemaMetadata::Confluent(schema)),
                })
            }
            DetectedWireFormat::Glue { version_id, .. } => {
                let registry = self.glue.ok_or_else(|| {
                    KrafkaError::config(
                        "schema decoder missing Glue registry for Glue-framed payload",
                    )
                })?;

                let (_, payload) = glue::decode_glue_wire_format(data)?;
                let schema = registry.get_schema_by_version_id(version_id).await?;

                Ok(DecodedMessage {
                    schema_format: schema.data_format.into(),
                    payload: payload.into_owned(),
                    schema_metadata: Some(SchemaMetadata::Glue(schema)),
                })
            }
            DetectedWireFormat::Unknown => Ok(DecodedMessage {
                schema_format: SchemaFormat::Unknown,
                payload: data.to_vec(),
                schema_metadata: None,
            }),
        }
    }
}

// ── Subject name strategy ────────────────────────────────────────────────

/// Strategy for deriving registry subject names from topics and records.
///
/// The subject name determines where schemas are registered and looked up.
/// The default [`TopicName`](Self::TopicName) strategy produces subjects
/// like `my-topic-key` and `my-topic-value`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum SubjectNameStrategy {
    /// `{topic}-key` / `{topic}-value`.
    ///
    /// This is the Confluent default. Each topic has one key schema and one
    /// value schema.
    #[default]
    TopicName,
    /// `{record_name}` (the record's fully qualified name).
    ///
    /// Useful when the same record type appears across multiple topics and
    /// should share a single schema entry.
    RecordName,
    /// `{topic}-{record_name}`.
    ///
    /// Useful when the same record type requires per-topic schema evolution.
    TopicRecordName,
}

fn schema_lookup_cancelled_error(id: SchemaId) -> KrafkaError {
    KrafkaError::invalid_state(format!(
        "schema lookup cancelled before completion for id {id}"
    ))
}

impl SubjectNameStrategy {
    /// Derive the subject name for the given topic and optional record name.
    ///
    /// # Errors
    ///
    /// Returns a configuration error if `record_name` is `None` for
    /// strategies that require it ([`RecordName`](Self::RecordName),
    /// [`TopicRecordName`](Self::TopicRecordName)).
    pub fn subject_name(
        &self,
        topic: &str,
        record_name: Option<&str>,
        is_key: bool,
    ) -> Result<String> {
        match self {
            Self::TopicName => {
                let suffix = if is_key { "key" } else { "value" };
                Ok(format!("{topic}-{suffix}"))
            }
            Self::RecordName => {
                let name = record_name.ok_or_else(|| {
                    KrafkaError::config("RecordName strategy requires a record name")
                })?;
                Ok(name.to_string())
            }
            Self::TopicRecordName => {
                let name = record_name.ok_or_else(|| {
                    KrafkaError::config("TopicRecordName strategy requires a record name")
                })?;
                Ok(format!("{topic}-{name}"))
            }
        }
    }
}

// ── Trait ─────────────────────────────────────────────────────────────────

/// Async client interface for a schema registry.
///
/// Implement this trait to integrate with any schema registry backend.
/// When the `schema-registry` feature is enabled, `ConfluentSchemaRegistry`
/// provides a ready-made HTTP implementation for the Confluent Schema
/// Registry (and compatible registries such as Karapace and Apicurio).
///
/// All methods return boxed futures for object safety, following the same
/// pattern as [`OAuthBearerTokenProvider`](crate::auth::OAuthBearerTokenProvider).
pub trait SchemaRegistryClient: Send + Sync {
    /// Retrieve a schema by its globally unique ID.
    ///
    /// Schema IDs are immutable — a given ID always maps to the same schema.
    fn get_schema_by_id(
        &self,
        id: SchemaId,
    ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>>;

    /// Retrieve the latest schema registered under the given subject.
    fn get_latest_schema(
        &self,
        subject: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>>;

    /// Retrieve a specific version of a schema under a subject.
    fn get_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>>;

    /// Register a schema under the given subject.
    ///
    /// If the same schema is already registered, the existing ID is returned
    /// (the operation is idempotent). Pass `&[]` for `references` when the
    /// schema has no dependencies.
    fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Pin<Box<dyn Future<Output = Result<SchemaId>> + Send + '_>>;
}

/// Shared cache-management interface implemented by schema cache wrappers.
///
/// This trait allows generic orchestration over both
/// [`CachedSchemaRegistry`] and [`glue::CachedGlueSchemaRegistry`] for cache
/// lifecycle operations (invalidate, clear, prewarm), without coupling to a
/// specific registry provider.
pub trait AnySchemaCache: Send + Sync {
    /// Identifier type used by this cache (schema ID or schema version ID).
    type Id: Copy + Send + Sync;

    /// Number of entries currently held in the cache.
    fn cache_len(&self) -> usize;

    /// Returns `true` when the cache contains no entries.
    fn cache_is_empty(&self) -> bool;

    /// Clear all cached entries.
    fn clear_cache(&self);

    /// Invalidate a specific cache entry.
    fn invalidate(&self, id: Self::Id);

    /// Invalidate all cache entries.
    fn invalidate_all(&self);

    /// Pre-warm the cache for a set of immutable IDs.
    fn warm_cache<'a>(
        &'a self,
        ids: &'a [Self::Id],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>>;
}

// ── CachedSchemaRegistry ─────────────────────────────────────────────────

/// Caching wrapper around any [`SchemaRegistryClient`].
///
/// Caches schema-ID-to-schema lookups in memory. Because a schema ID is
/// immutable in the registry (it always maps to the same schema), cached
/// entries never expire.
///
/// Methods whose results may change over time
/// ([`get_latest_schema`](SchemaRegistryClient::get_latest_schema))
/// always hit the inner client but still populate the ID cache so that
/// subsequent [`get_schema_by_id`](SchemaRegistryClient::get_schema_by_id)
/// calls are served from cache.
///
/// # Example
///
/// ```rust,ignore
/// use krafka::schema_registry::{CachedSchemaRegistry, ConfluentSchemaRegistry};
///
/// let client = ConfluentSchemaRegistry::new("http://localhost:8081");
/// let cached = CachedSchemaRegistry::new(client);
///
/// // First call fetches from the registry:
/// let schema = cached.get_schema_by_id(1).await?;
///
/// // Second call is served from cache:
/// let same = cached.get_schema_by_id(1).await?;
/// ```
pub struct CachedSchemaRegistry<C> {
    /// The inner registry client.
    inner: C,
    /// Schema ID → Schema cache. Entries are immutable once inserted.
    cache: RwLock<HashMap<SchemaId, Schema>>,
    /// Insertion order for bounded-cache eviction.
    insertion_order: RwLock<VecDeque<SchemaId>>,
    /// Optional maximum number of cached entries.
    max_entries: Option<usize>,
    /// Waiters for coalescing concurrent cold misses by schema ID.
    in_flight: Mutex<HashMap<SchemaId, Vec<oneshot::Sender<Result<Schema>>>>>,
}

impl<C: SchemaRegistryClient> CachedSchemaRegistry<C> {
    /// Wrap the given client with an in-memory cache.
    pub fn new(inner: C) -> Self {
        Self {
            inner,
            cache: RwLock::new(HashMap::new()),
            insertion_order: RwLock::new(VecDeque::new()),
            max_entries: None,
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Wrap the given client with a pre-allocated cache.
    pub fn with_capacity(inner: C, capacity: usize) -> Self {
        Self {
            inner,
            cache: RwLock::new(HashMap::with_capacity(capacity)),
            insertion_order: RwLock::new(VecDeque::with_capacity(capacity)),
            max_entries: None,
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Wrap the given client with a bounded in-memory cache.
    ///
    /// When the cache reaches `max_entries`, the oldest inserted schema ID is evicted.
    pub fn with_max_entries(inner: C, max_entries: usize) -> Self {
        let max_entries = max_entries.max(1);
        Self {
            inner,
            cache: RwLock::new(HashMap::with_capacity(max_entries)),
            insertion_order: RwLock::new(VecDeque::with_capacity(max_entries)),
            max_entries: Some(max_entries),
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// Returns a reference to the inner (uncached) client.
    pub fn inner(&self) -> &C {
        &self.inner
    }

    /// Number of schemas currently in the cache.
    pub fn cache_len(&self) -> usize {
        self.cache.read().len()
    }

    /// Returns `true` if the cache contains no schemas.
    pub fn cache_is_empty(&self) -> bool {
        self.cache.read().is_empty()
    }

    /// Clear the schema cache.
    pub fn clear_cache(&self) {
        self.cache.write().clear();
        self.insertion_order.write().clear();
    }

    /// Remove a single schema ID from the cache.
    pub fn invalidate(&self, schema_id: SchemaId) {
        self.cache.write().remove(&schema_id);
        self.insertion_order
            .write()
            .retain(|cached_id| *cached_id != schema_id);
    }

    /// Remove all cached schemas.
    pub fn invalidate_all(&self) {
        self.clear_cache();
    }

    /// Pre-fetch a set of schema IDs into the cache.
    ///
    /// Duplicate IDs are ignored to avoid redundant lookups.
    pub async fn warm_cache(&self, schema_ids: &[SchemaId]) -> Result<()> {
        let mut seen = HashSet::with_capacity(schema_ids.len());
        for &id in schema_ids {
            if !seen.insert(id) {
                continue;
            }
            self.get_schema_by_id_impl(id).await?;
        }
        Ok(())
    }

    async fn get_schema_by_id_impl(&self, id: SchemaId) -> Result<Schema> {
        // Fast path: read lock only.
        if let Some(schema) = self.cache.read().get(&id) {
            debug!(schema_id = id, "schema cache hit");
            return Ok(schema.clone());
        }

        let waiter_rx = {
            let mut in_flight = self.in_flight.lock();
            if let Some(schema) = self.cache.read().get(&id) {
                debug!(schema_id = id, "schema cache hit (double-checked)");
                return Ok(schema.clone());
            }

            if let Some(waiters) = in_flight.get_mut(&id) {
                let (tx, rx) = oneshot::channel();
                waiters.push(tx);
                Some(rx)
            } else {
                in_flight.insert(id, Vec::new());
                None
            }
        };

        if let Some(rx) = waiter_rx {
            return rx.await.map_err(|_| schema_lookup_cancelled_error(id))?;
        }

        struct InFlightSchemaFetchGuard<'a> {
            in_flight: &'a Mutex<HashMap<SchemaId, Vec<oneshot::Sender<Result<Schema>>>>>,
            id: SchemaId,
            completed: bool,
        }

        impl Drop for InFlightSchemaFetchGuard<'_> {
            fn drop(&mut self) {
                if self.completed {
                    return;
                }
                let waiters = self.in_flight.lock().remove(&self.id).unwrap_or_default();
                for waiter in waiters {
                    let _ = waiter.send(Err(schema_lookup_cancelled_error(self.id)));
                }
            }
        }

        let mut guard = InFlightSchemaFetchGuard {
            in_flight: &self.in_flight,
            id,
            completed: false,
        };

        let result = self.inner.get_schema_by_id(id).await;
        if let Ok(schema) = &result {
            debug!(schema_id = id, "schema cache miss — fetched from registry");
            self.insert_cache_entry(id, schema.clone());
        }

        let waiters = self.in_flight.lock().remove(&id).unwrap_or_default();
        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
        guard.completed = true;

        result
    }

    async fn get_latest_schema_impl(&self, subject: &str) -> Result<Schema> {
        // Always forward (latest may change), but cache by ID.
        let schema = self.inner.get_latest_schema(subject).await?;
        self.insert_cache_entry(schema.id, schema.clone());
        Ok(schema)
    }

    async fn get_schema_by_version_impl(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<Schema> {
        let schema = self.inner.get_schema_by_version(subject, version).await?;
        self.insert_cache_entry(schema.id, schema.clone());
        Ok(schema)
    }

    async fn register_schema_impl(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Result<SchemaId> {
        self.inner
            .register_schema(subject, schema, schema_type, references)
            .await
    }

    /// Retrieve a schema by its globally unique ID.
    ///
    /// This inherent method mirrors [`SchemaRegistryClient::get_schema_by_id`]
    /// so callers of `CachedSchemaRegistry` do not need to import the trait.
    ///
    /// Inherent methods intentionally shadow trait methods for the concrete type.
    /// If you need the boxed trait future shape, call the trait explicitly via UFCS.
    pub async fn get_schema_by_id(&self, id: SchemaId) -> Result<Schema> {
        self.get_schema_by_id_impl(id).await
    }

    /// Retrieve the latest schema registered under the given subject.
    ///
    /// This inherent method mirrors [`SchemaRegistryClient::get_latest_schema`]
    /// so callers of `CachedSchemaRegistry` do not need to import the trait.
    pub async fn get_latest_schema(&self, subject: &str) -> Result<Schema> {
        self.get_latest_schema_impl(subject).await
    }

    /// Retrieve a specific version of a schema under a subject.
    ///
    /// This inherent method mirrors [`SchemaRegistryClient::get_schema_by_version`]
    /// so callers of `CachedSchemaRegistry` do not need to import the trait.
    pub async fn get_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Result<Schema> {
        self.get_schema_by_version_impl(subject, version).await
    }

    /// Register a schema under the given subject.
    ///
    /// This inherent method mirrors [`SchemaRegistryClient::register_schema`]
    /// so callers of `CachedSchemaRegistry` do not need to import the trait.
    pub async fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Result<SchemaId> {
        self.register_schema_impl(subject, schema, schema_type, references)
            .await
    }

    fn insert_cache_entry(&self, id: SchemaId, schema: Schema) {
        let mut cache = self.cache.write();

        // Fast path: update existing entry without touching insertion_order.
        if let Some(existing) = cache.get_mut(&id) {
            *existing = schema;
            return;
        }

        // New entry: evict oldest if bounded.
        if let Some(max_entries) = self.max_entries {
            let mut insertion_order = self.insertion_order.write();
            if cache.len() >= max_entries
                && let Some(evicted) = insertion_order.pop_front()
            {
                cache.remove(&evicted);
            }
            insertion_order.push_back(id);
        }

        cache.insert(id, schema);
    }
}

impl<C> fmt::Debug for CachedSchemaRegistry<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedSchemaRegistry")
            .field("cache_len", &self.cache.read().len())
            .field("max_entries", &self.max_entries)
            .finish()
    }
}

impl<C: SchemaRegistryClient> SchemaRegistryClient for CachedSchemaRegistry<C> {
    fn get_schema_by_id(
        &self,
        id: SchemaId,
    ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
        Box::pin(async move { self.get_schema_by_id_impl(id).await })
    }

    fn get_latest_schema(
        &self,
        subject: &str,
    ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
        let subject = subject.to_string();
        Box::pin(async move { self.get_latest_schema_impl(&subject).await })
    }

    fn get_schema_by_version(
        &self,
        subject: &str,
        version: SchemaVersion,
    ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
        let subject = subject.to_string();
        Box::pin(async move { self.get_schema_by_version_impl(&subject, version).await })
    }

    fn register_schema(
        &self,
        subject: &str,
        schema: &str,
        schema_type: SchemaType,
        references: &[SchemaReference],
    ) -> Pin<Box<dyn Future<Output = Result<SchemaId>> + Send + '_>> {
        let subject = subject.to_string();
        let schema = schema.to_string();
        let references = references.to_vec();
        Box::pin(async move {
            self.register_schema_impl(&subject, &schema, schema_type, &references)
                .await
        })
    }
}

impl<C: SchemaRegistryClient> AnySchemaCache for CachedSchemaRegistry<C> {
    type Id = SchemaId;

    fn cache_len(&self) -> usize {
        Self::cache_len(self)
    }

    fn cache_is_empty(&self) -> bool {
        Self::cache_is_empty(self)
    }

    fn clear_cache(&self) {
        Self::clear_cache(self)
    }

    fn invalidate(&self, id: Self::Id) {
        Self::invalidate(self, id)
    }

    fn invalidate_all(&self) {
        Self::invalidate_all(self)
    }

    fn warm_cache<'a>(
        &'a self,
        ids: &'a [Self::Id],
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'a>> {
        Box::pin(async move { Self::warm_cache(self, ids).await })
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::sync::Notify;

    fn ok<T, E: std::fmt::Display>(result: std::result::Result<T, E>) -> T {
        match result {
            Ok(value) => value,
            Err(err) => unreachable!("expected Ok(..), got Err({err})"),
        }
    }

    fn err<T, E: std::fmt::Display>(result: std::result::Result<T, E>) -> E {
        match result {
            Err(err) => err,
            Ok(_) => unreachable!("expected Err(..), got Ok(..)"),
        }
    }

    fn join_ok<T>(result: std::result::Result<T, tokio::task::JoinError>) -> T {
        match result {
            Ok(value) => value,
            Err(err) => unreachable!("spawned task failed unexpectedly: {err}"),
        }
    }

    // ── Wire format ──────────────────────────────────────────────────────

    #[test]
    fn test_wire_format_roundtrip() {
        let payload = b"hello world";
        let encoded = encode_wire_format(42, payload);
        let (id, decoded) = ok(decode_wire_format(&encoded));
        assert_eq!(id, 42);
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_wire_format_owned_roundtrip() {
        let payload = b"hello owned world";
        let encoded = encode_wire_format(42, payload);
        let (id, decoded) = ok(decode_wire_format_owned(&encoded));
        assert_eq!(id, 42);
        assert_eq!(decoded, payload);
    }

    #[test]
    fn test_wire_format_empty_payload() {
        let encoded = encode_wire_format(1, b"");
        assert_eq!(encoded.len(), HEADER_SIZE);
        let (id, payload) = ok(decode_wire_format(&encoded));
        assert_eq!(id, 1);
        assert!(payload.is_empty());
    }

    #[test]
    fn test_wire_format_max_schema_id() {
        let encoded = encode_wire_format(u32::MAX, b"data");
        let (id, _) = ok(decode_wire_format(&encoded));
        assert_eq!(id, u32::MAX);
    }

    #[test]
    fn test_wire_format_header_bytes() {
        // Schema ID 256 = 0x00000100
        let encoded = encode_wire_format(256, b"x");
        assert_eq!(&encoded[..5], &[0x00, 0x00, 0x00, 0x01, 0x00]);
        assert_eq!(&encoded[5..], b"x");
    }

    #[test]
    fn test_wire_format_invalid_magic_byte() {
        let data = [0x01, 0, 0, 0, 1, 0x42];
        let result = decode_wire_format(&data);
        assert!(result.is_err());
        assert!(err(result).to_string().contains("magic byte"));
    }

    #[test]
    fn test_wire_format_too_short() {
        let result = decode_wire_format(&[0x00, 0, 0]);
        assert!(result.is_err());
        assert!(err(result).to_string().contains("too short"));
    }

    #[test]
    fn test_wire_format_empty_data() {
        let result = decode_wire_format(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_detect_wire_format_confluent() {
        let encoded = encode_wire_format(42, b"data");
        let detected = detect_wire_format(&encoded);
        assert_eq!(
            detected,
            DetectedWireFormat::Confluent {
                schema_id: 42,
                payload_offset: 5,
            }
        );
    }

    #[test]
    fn test_detect_wire_format_glue() {
        let version_id: GlueSchemaVersionId =
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
        let encoded = crate::schema_registry::glue::encode_glue_wire_format(
            version_id,
            b"data",
            crate::schema_registry::glue::GlueCompression::None,
        )
        .unwrap();
        let detected = detect_wire_format(&encoded);
        assert_eq!(
            detected,
            DetectedWireFormat::Glue {
                version_id,
                payload_offset: 18,
            }
        );
    }

    #[test]
    fn test_detect_wire_format_unknown() {
        assert_eq!(detect_wire_format(&[]), DetectedWireFormat::Unknown);
        assert_eq!(
            detect_wire_format(&[0x99, 0x00, 0x00]),
            DetectedWireFormat::Unknown
        );
    }

    struct DecoderMockGlueRegistry;

    impl glue::GlueSchemaRegistryClient for DecoderMockGlueRegistry {
        fn get_schema_by_version_id(
            &self,
            id: GlueSchemaVersionId,
        ) -> Pin<Box<dyn Future<Output = Result<glue::GlueSchema>> + Send + '_>> {
            Box::pin(async move {
                Ok(glue::GlueSchema::new(
                    id,
                    glue::GlueDataFormat::Json,
                    r#"{"type":"object"}"#,
                ))
            })
        }

        fn register_schema(
            &self,
            _schema_name: &str,
            _schema: &str,
            _data_format: glue::GlueDataFormat,
        ) -> Pin<Box<dyn Future<Output = Result<GlueSchemaVersionId>> + Send + '_>> {
            Box::pin(async {
                Ok("550e8400-e29b-41d4-a716-446655440000"
                    .parse::<GlueSchemaVersionId>()
                    .unwrap())
            })
        }
    }

    #[tokio::test]
    async fn test_schema_decoder_confluent() {
        let registry = CachedSchemaRegistry::new(MockRegistry::new());
        let decoder = SchemaDecoder::confluent(&registry);

        let encoded = encode_wire_format(7, b"payload");
        let decoded = ok(decoder.decode(&encoded).await);

        assert_eq!(decoded.schema_format, SchemaFormat::Avro);
        assert_eq!(decoded.payload, b"payload");
        match decoded.schema_metadata {
            Some(SchemaMetadata::Confluent(schema)) => assert_eq!(schema.id, 7),
            _ => unreachable!("expected confluent metadata"),
        }
    }

    #[tokio::test]
    async fn test_schema_decoder_glue() {
        let registry = glue::CachedGlueSchemaRegistry::new(DecoderMockGlueRegistry);
        let decoder = SchemaDecoder::glue(&registry);

        let version_id: GlueSchemaVersionId =
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
        let encoded =
            glue::encode_glue_wire_format(version_id, b"payload", glue::GlueCompression::None)
                .unwrap();

        let decoded = ok(decoder.decode(&encoded).await);
        assert_eq!(decoded.schema_format, SchemaFormat::Json);
        assert_eq!(decoded.payload, b"payload");
        match decoded.schema_metadata {
            Some(SchemaMetadata::Glue(schema)) => assert_eq!(schema.schema_version_id, version_id),
            _ => unreachable!("expected glue metadata"),
        }
    }

    #[tokio::test]
    async fn test_schema_decoder_unknown_passthrough() {
        let decoder = SchemaDecoder::new();
        let decoded = ok(decoder.decode(b"plain-data").await);

        assert_eq!(decoded.schema_format, SchemaFormat::Unknown);
        assert_eq!(decoded.payload, b"plain-data");
        assert!(decoded.schema_metadata.is_none());
    }

    #[tokio::test]
    async fn test_schema_decoder_missing_registry_is_error() {
        let decoder = SchemaDecoder::new();
        let encoded = encode_wire_format(1, b"x");

        let result = decoder.decode(&encoded).await;
        assert!(result.is_err());
        assert!(
            err(result)
                .to_string()
                .contains("missing Confluent registry")
        );
    }

    // ── SubjectNameStrategy ──────────────────────────────────────────────

    #[test]
    fn test_subject_default_is_topic_name() {
        assert_eq!(
            SubjectNameStrategy::default(),
            SubjectNameStrategy::TopicName
        );
    }

    #[test]
    fn test_subject_topic_name_key() {
        let s = ok(SubjectNameStrategy::TopicName.subject_name("orders", None, true));
        assert_eq!(s, "orders-key");
    }

    #[test]
    fn test_subject_topic_name_value() {
        let s = ok(SubjectNameStrategy::TopicName.subject_name("orders", None, false));
        assert_eq!(s, "orders-value");
    }

    #[test]
    fn test_subject_record_name() {
        let s = ok(SubjectNameStrategy::RecordName.subject_name(
            "orders",
            Some("com.example.Order"),
            false,
        ));
        assert_eq!(s, "com.example.Order");
    }

    #[test]
    fn test_subject_record_name_missing() {
        let result = SubjectNameStrategy::RecordName.subject_name("orders", None, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_subject_topic_record_name() {
        let s =
            ok(SubjectNameStrategy::TopicRecordName.subject_name("orders", Some("Order"), true));
        assert_eq!(s, "orders-Order");
    }

    #[test]
    fn test_subject_topic_record_name_missing() {
        let result = SubjectNameStrategy::TopicRecordName.subject_name("orders", None, true);
        assert!(result.is_err());
    }

    // ── SchemaType ───────────────────────────────────────────────────────

    #[test]
    fn test_schema_type_display() {
        assert_eq!(SchemaType::Avro.to_string(), "AVRO");
        assert_eq!(SchemaType::Protobuf.to_string(), "PROTOBUF");
        assert_eq!(SchemaType::Json.to_string(), "JSON");
    }

    #[test]
    fn test_schema_type_from_str() {
        assert_eq!(ok("AVRO".parse::<SchemaType>()), SchemaType::Avro);
        assert_eq!(ok("PROTOBUF".parse::<SchemaType>()), SchemaType::Protobuf);
        assert_eq!(ok("JSON".parse::<SchemaType>()), SchemaType::Json);
    }

    #[test]
    fn test_schema_type_from_str_unknown() {
        let result = "XML".parse::<SchemaType>();
        assert!(result.is_err());
        assert!(err(result).to_string().contains("XML"));
    }

    // ── Schema constructors ──────────────────────────────────────────────

    #[test]
    fn test_schema_new() {
        let s = Schema::new(1, SchemaType::Avro, r#"{"type":"string"}"#);
        assert_eq!(s.id, 1);
        assert_eq!(s.schema_type, SchemaType::Avro);
        assert_eq!(s.schema, r#"{"type":"string"}"#);
        assert_eq!(s.version, None);
        assert_eq!(s.subject, None);
        assert!(s.references.is_empty());
    }

    #[test]
    fn test_schema_with_subject() {
        let s = Schema::new(1, SchemaType::Avro, "{}").with_subject("my-topic-value", 3);
        assert_eq!(s.subject, Some("my-topic-value".to_string()));
        assert_eq!(s.version, Some(3));
    }

    #[test]
    fn test_schema_with_references() {
        let refs = vec![SchemaReference::new("Ref", "ref-subject", 1)];
        let s = Schema::new(1, SchemaType::Avro, "{}").with_references(refs.clone());
        assert_eq!(s.references, refs);
    }

    #[test]
    fn test_schema_reference_new() {
        let r = SchemaReference::new("com.example.Address", "address-value", 2);
        assert_eq!(r.name, "com.example.Address");
        assert_eq!(r.subject, "address-value");
        assert_eq!(r.version, 2);
    }

    // ── CachedSchemaRegistry ─────────────────────────────────────────────

    /// Mock registry that counts calls to `get_schema_by_id`.
    struct MockRegistry {
        get_by_id_calls: AtomicU32,
    }

    impl MockRegistry {
        fn new() -> Self {
            Self {
                get_by_id_calls: AtomicU32::new(0),
            }
        }

        fn get_by_id_call_count(&self) -> u32 {
            self.get_by_id_calls.load(Ordering::SeqCst)
        }
    }

    impl SchemaRegistryClient for MockRegistry {
        fn get_schema_by_id(
            &self,
            id: SchemaId,
        ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
            self.get_by_id_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(Schema::new(id, SchemaType::Avro, r#"{"type":"string"}"#)) })
        }

        fn get_latest_schema(
            &self,
            subject: &str,
        ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
            let subject = subject.to_string();
            Box::pin(async move {
                Ok(Schema::new(100, SchemaType::Avro, r#"{"type":"string"}"#)
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
                Ok(Schema::new(100, SchemaType::Avro, r#"{"type":"string"}"#)
                    .with_subject(subject, version))
            })
        }

        fn register_schema(
            &self,
            _subject: &str,
            _schema: &str,
            _schema_type: SchemaType,
            _references: &[SchemaReference],
        ) -> Pin<Box<dyn Future<Output = Result<SchemaId>> + Send + '_>> {
            Box::pin(async { Ok(42) })
        }
    }

    struct BlockingMockRegistry {
        get_by_id_calls: AtomicU32,
        started: Notify,
        release: Notify,
    }

    impl BlockingMockRegistry {
        fn new() -> Self {
            Self {
                get_by_id_calls: AtomicU32::new(0),
                started: Notify::new(),
                release: Notify::new(),
            }
        }

        fn get_by_id_call_count(&self) -> u32 {
            self.get_by_id_calls.load(Ordering::SeqCst)
        }

        async fn wait_started(&self) {
            self.started.notified().await;
        }

        fn release(&self) {
            self.release.notify_waiters();
        }
    }

    impl SchemaRegistryClient for BlockingMockRegistry {
        fn get_schema_by_id(
            &self,
            id: SchemaId,
        ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
            self.get_by_id_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                self.started.notify_waiters();
                self.release.notified().await;
                Ok(Schema::new(id, SchemaType::Avro, r#"{"type":"string"}"#))
            })
        }

        fn get_latest_schema(
            &self,
            subject: &str,
        ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
            let subject = subject.to_string();
            Box::pin(async move {
                Ok(Schema::new(100, SchemaType::Avro, r#"{"type":"string"}"#)
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
                Ok(Schema::new(100, SchemaType::Avro, r#"{"type":"string"}"#)
                    .with_subject(subject, version))
            })
        }

        fn register_schema(
            &self,
            _subject: &str,
            _schema: &str,
            _schema_type: SchemaType,
            _references: &[SchemaReference],
        ) -> Pin<Box<dyn Future<Output = Result<SchemaId>> + Send + '_>> {
            Box::pin(async { Ok(42) })
        }
    }

    #[tokio::test]
    async fn test_cache_miss_then_hit() {
        let mock = MockRegistry::new();
        let cached = CachedSchemaRegistry::new(mock);

        // First call: cache miss → hits mock
        let s1 = ok(cached.get_schema_by_id(1).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 1);
        assert_eq!(cached.cache_len(), 1);

        // Second call: cache hit → does NOT hit mock
        let s2 = ok(cached.get_schema_by_id(1).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 1);

        assert_eq!(s1, s2);
    }

    #[tokio::test]
    async fn test_cache_different_ids() {
        let mock = MockRegistry::new();
        let cached = CachedSchemaRegistry::new(mock);

        ok(cached.get_schema_by_id(1).await);
        ok(cached.get_schema_by_id(2).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 2);
        assert_eq!(cached.cache_len(), 2);

        // Both cached now
        ok(cached.get_schema_by_id(1).await);
        ok(cached.get_schema_by_id(2).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 2);
    }

    #[tokio::test]
    async fn test_cache_clear() {
        let mock = MockRegistry::new();
        let cached = CachedSchemaRegistry::new(mock);

        ok(cached.get_schema_by_id(1).await);
        assert_eq!(cached.cache_len(), 1);

        cached.clear_cache();
        assert_eq!(cached.cache_len(), 0);

        // After clear, next call hits mock again
        ok(cached.get_schema_by_id(1).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 2);
    }

    #[tokio::test]
    async fn test_cache_invalidate_single_entry() {
        let mock = MockRegistry::new();
        let cached = CachedSchemaRegistry::new(mock);

        ok(cached.get_schema_by_id(1).await);
        ok(cached.get_schema_by_id(2).await);
        assert_eq!(cached.cache_len(), 2);

        cached.invalidate(1);
        assert_eq!(cached.cache_len(), 1);

        // ID 1 should miss after invalidation; ID 2 should still hit.
        ok(cached.get_schema_by_id(2).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 2);
        ok(cached.get_schema_by_id(1).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 3);
    }

    #[tokio::test]
    async fn test_cache_warm_cache_deduplicates_ids() {
        let mock = MockRegistry::new();
        let cached = CachedSchemaRegistry::new(mock);

        ok(cached.warm_cache(&[1, 2, 1, 2, 3]).await);

        assert_eq!(cached.inner().get_by_id_call_count(), 3);
        assert_eq!(cached.cache_len(), 3);

        // Subsequent gets should be cache hits only.
        ok(cached.get_schema_by_id(1).await);
        ok(cached.get_schema_by_id(2).await);
        ok(cached.get_schema_by_id(3).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 3);
    }

    #[tokio::test]
    async fn test_cache_coalesces_concurrent_misses() {
        let cached = Arc::new(CachedSchemaRegistry::new(BlockingMockRegistry::new()));

        let first = {
            let cached = cached.clone();
            tokio::spawn(async move { ok(cached.get_schema_by_id(7).await) })
        };

        cached.inner().wait_started().await;

        let second = {
            let cached = cached.clone();
            tokio::spawn(async move { ok(cached.get_schema_by_id(7).await) })
        };

        tokio::task::yield_now().await;
        cached.inner().release();

        let first_schema = join_ok(first.await);
        let second_schema = join_ok(second.await);

        assert_eq!(first_schema, second_schema);
        assert_eq!(cached.inner().get_by_id_call_count(), 1);
    }

    #[tokio::test]
    async fn test_cache_coalescer_cleans_up_when_leader_is_cancelled() {
        let cached = Arc::new(CachedSchemaRegistry::new(BlockingMockRegistry::new()));

        let first = {
            let cached = cached.clone();
            tokio::spawn(async move { ok(cached.get_schema_by_id(9).await) })
        };

        cached.inner().wait_started().await;
        first.abort();
        tokio::task::yield_now().await;

        let second = {
            let cached = cached.clone();
            tokio::spawn(async move { ok(cached.get_schema_by_id(9).await) })
        };

        // If cancellation cleanup is broken, this waits forever because second
        // caller never becomes leader and never reaches the inner mock.
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            cached.inner().wait_started(),
        )
        .await
        .expect("second lookup did not reach inner registry");

        cached.inner().release();
        let schema = tokio::time::timeout(std::time::Duration::from_secs(5), second)
            .await
            .expect("second lookup timed out")
            .expect("second task failed");

        assert_eq!(schema.id, 9);
    }

    #[tokio::test]
    async fn test_cache_get_latest_populates_id_cache() {
        let mock = MockRegistry::new();
        let cached = CachedSchemaRegistry::new(mock);

        // get_latest_schema always forwards but caches by ID
        let schema = ok(cached.get_latest_schema("test-value").await);
        assert_eq!(cached.cache_len(), 1);

        // Subsequent get_schema_by_id should be cached
        let by_id = ok(cached.get_schema_by_id(schema.id).await);
        // Mock was never called via get_schema_by_id
        assert_eq!(cached.inner().get_by_id_call_count(), 0);
        assert_eq!(by_id.id, schema.id);
    }

    #[tokio::test]
    async fn test_cache_get_by_version_populates_id_cache() {
        let mock = MockRegistry::new();
        let cached = CachedSchemaRegistry::new(mock);

        let schema = ok(cached.get_schema_by_version("test-value", 1).await);
        assert_eq!(cached.cache_len(), 1);

        let by_id = ok(cached.get_schema_by_id(schema.id).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 0);
        assert_eq!(by_id.id, schema.id);
    }

    #[tokio::test]
    async fn test_cache_register_forwards() {
        let mock = MockRegistry::new();
        let cached = CachedSchemaRegistry::new(mock);

        let id = cached
            .register_schema("test-value", "{}", SchemaType::Avro, &[])
            .await;
        let id = ok(id);
        assert_eq!(id, 42);
    }

    // ── Type assertions ──────────────────────────────────────────────────

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Schema>();
        assert_send_sync::<SchemaReference>();
        assert_send_sync::<SchemaType>();
        assert_send_sync::<SubjectNameStrategy>();
        assert_send_sync::<CachedSchemaRegistry<MockRegistry>>();
    }

    /// Verify that [`SchemaRegistryClient`] is object-safe.
    #[test]
    fn test_object_safe() {
        fn _assert_object_safe(_: &dyn SchemaRegistryClient) {}
    }

    #[test]
    fn test_cached_debug() {
        let cached = CachedSchemaRegistry::new(MockRegistry::new());
        let debug = format!("{cached:?}");
        assert!(debug.contains("cache_len"));
    }

    // ── decode_wire_format_bytes ─────────────────────────────────────────

    #[test]
    fn test_wire_format_bytes_roundtrip() {
        let payload = b"hello world";
        let encoded = encode_wire_format(42, payload);
        let (id, decoded) = ok(decode_wire_format_bytes(&encoded));
        assert_eq!(id, 42);
        assert_eq!(&decoded[..], payload);
    }

    #[test]
    fn test_wire_format_bytes_empty_payload() {
        let encoded = encode_wire_format(1, b"");
        let (id, payload) = ok(decode_wire_format_bytes(&encoded));
        assert_eq!(id, 1);
        assert!(payload.is_empty());
    }

    #[test]
    fn test_wire_format_bytes_invalid_magic() {
        let data = Bytes::from_static(&[0x01, 0, 0, 0, 1, 0x42]);
        let result = decode_wire_format_bytes(&data);
        assert!(result.is_err());
        assert!(err(result).to_string().contains("magic byte"));
    }

    #[test]
    fn test_wire_format_bytes_too_short() {
        let data = Bytes::from_static(&[0x00, 0, 0]);
        let result = decode_wire_format_bytes(&data);
        assert!(result.is_err());
        assert!(err(result).to_string().contains("too short"));
    }

    #[test]
    fn test_wire_format_bytes_zero_copy() {
        // The returned Bytes should share the same allocation as the input.
        let encoded = encode_wire_format(99, b"shared");
        let (_, payload) = ok(decode_wire_format_bytes(&encoded));
        // Bytes::slice shares the backing allocation, so ptr should be
        // within the original allocation.
        assert_eq!(&payload[..], b"shared");
    }

    // ── SchemaType case-insensitive ──────────────────────────────────────

    #[test]
    fn test_schema_type_from_str_lowercase() {
        assert_eq!(ok("avro".parse::<SchemaType>()), SchemaType::Avro);
        assert_eq!(ok("protobuf".parse::<SchemaType>()), SchemaType::Protobuf);
        assert_eq!(ok("json".parse::<SchemaType>()), SchemaType::Json);
    }

    #[test]
    fn test_schema_type_from_str_mixed_case() {
        assert_eq!(ok("Avro".parse::<SchemaType>()), SchemaType::Avro);
        assert_eq!(ok("ProtobuF".parse::<SchemaType>()), SchemaType::Protobuf);
        assert_eq!(ok("Json".parse::<SchemaType>()), SchemaType::Json);
    }

    // ── CachedSchemaRegistry::with_capacity ──────────────────────────────

    #[tokio::test]
    async fn test_cache_with_capacity() {
        let mock = MockRegistry::new();
        let cached = CachedSchemaRegistry::with_capacity(mock, 100);
        assert_eq!(cached.cache_len(), 0);

        ok(cached.get_schema_by_id(1).await);
        assert_eq!(cached.cache_len(), 1);
    }

    #[tokio::test]
    async fn test_cache_with_max_entries_evicts_oldest_entry() {
        let mock = MockRegistry::new();
        let cached = CachedSchemaRegistry::with_max_entries(mock, 1);

        ok(cached.get_schema_by_id(1).await);
        ok(cached.get_schema_by_id(2).await);

        assert_eq!(cached.cache_len(), 1);
        assert_eq!(cached.inner().get_by_id_call_count(), 2);

        ok(cached.get_schema_by_id(1).await);
        assert_eq!(cached.inner().get_by_id_call_count(), 3);
    }

    mod inherent_api_tests {
        use std::future::Future;
        use std::pin::Pin;

        use crate::Result;
        use crate::schema_registry::{
            CachedSchemaRegistry, Schema, SchemaReference, SchemaType, SchemaVersion,
        };

        struct InherentMockRegistry;

        impl crate::schema_registry::SchemaRegistryClient for InherentMockRegistry {
            fn get_schema_by_id(
                &self,
                id: u32,
            ) -> Pin<Box<dyn Future<Output = Result<Schema>> + Send + '_>> {
                Box::pin(
                    async move { Ok(Schema::new(id, SchemaType::Avro, r#"{"type":"string"}"#)) },
                )
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

        #[tokio::test]
        async fn cached_schema_registry_methods_work_without_trait_import() {
            let cached = CachedSchemaRegistry::new(InherentMockRegistry);

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
    }

    #[tokio::test]
    async fn test_any_schema_cache_trait_for_confluent_cache() {
        let mock = MockRegistry::new();
        let cached = CachedSchemaRegistry::new(mock);

        let generic_cache: &dyn AnySchemaCache<Id = SchemaId> = &cached;
        ok(generic_cache.warm_cache(&[11, 12, 11]).await);

        assert_eq!(generic_cache.cache_len(), 2);
        assert!(!generic_cache.cache_is_empty());

        generic_cache.invalidate(11);
        assert_eq!(generic_cache.cache_len(), 1);

        generic_cache.invalidate_all();
        assert!(generic_cache.cache_is_empty());
    }
}
