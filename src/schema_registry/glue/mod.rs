//! AWS Glue Schema Registry wire format and client types.
//!
//! This module is always available — no extra dependencies required.
//! It provides the Glue wire format encoder/decoder, types, a pluggable
//! client trait, and a caching wrapper.
//!
//! When the `aws-glue-schema-registry` feature is enabled,
//! `AwsGlueSchemaRegistry` provides a ready-made AWS SDK client.
//!
//! # Wire Format
//!
//! The AWS Glue wire format uses an 18-byte header:
//!
//! ```text
//! ┌──────────┬─────────────┬──────────────────────┬──────────────────┐
//! │ 0x03 (1B)│ Compr. (1B) │ Schema Version UUID  │ Payload (N bytes)│
//! │          │             │      (16B, BE)        │                  │
//! └──────────┴─────────────┴──────────────────────┴──────────────────┘
//! ```
//!
//! - **Byte 0**: Header version byte (`0x03`)
//! - **Byte 1**: Compression indicator (`0x00` = none, `0x05` = ZLIB)
//! - **Bytes 2–17**: Schema version ID as a 128-bit UUID (big-endian)
//! - **Bytes 18+**: Payload (ZLIB-compressed if byte 1 is `0x05`)
//!
//! # Example
//!
//! ```rust
//! use krafka::schema_registry::glue::{
//!     encode_glue_wire_format, decode_glue_wire_format,
//!     GlueSchemaVersionId, GlueCompression,
//! };
//!
//! let uuid: GlueSchemaVersionId = "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
//! let framed = encode_glue_wire_format(uuid, b"avro data", GlueCompression::None).unwrap();
//!
//! let (id, payload) = decode_glue_wire_format(&framed).unwrap();
//! assert_eq!(id, uuid);
//! assert_eq!(&payload, b"avro data");
//! ```

#[cfg(feature = "aws-glue-schema-registry")]
mod glue_client;

#[cfg(feature = "aws-glue-schema-registry")]
#[cfg_attr(docsrs, doc(cfg(feature = "aws-glue-schema-registry")))]
pub use glue_client::{AwsGlueSchemaRegistry, AwsGlueSchemaRegistryBuilder};

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::io::{Read, Write};
use std::pin::Pin;
use std::str::FromStr;

use bytes::{BufMut, Bytes, BytesMut};
use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use parking_lot::RwLock;
use tokio::sync::{Mutex as AsyncMutex, oneshot};

use crate::error::{KrafkaError, Result};

// ── Constants ────────────────────────────────────────────────────────────

/// Header version byte for the AWS Glue wire format.
const GLUE_HEADER_VERSION_BYTE: u8 = 0x03;

/// Compression indicator: no compression.
const GLUE_COMPRESSION_NONE_BYTE: u8 = 0x00;

/// Compression indicator: ZLIB compression.
const GLUE_COMPRESSION_ZLIB_BYTE: u8 = 0x05;

/// Size of the Glue wire format header (version + compression + 16-byte UUID).
const GLUE_HEADER_SIZE: usize = 18;

/// Size of the UUID field in the Glue wire format.
const UUID_SIZE: usize = 16;

// ── Types ────────────────────────────────────────────────────────────────

/// Schema version identifier used by the AWS Glue Schema Registry.
///
/// Internally a 128-bit UUID stored as big-endian bytes matching the
/// Glue wire format layout. Display and parse use the standard
/// `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` format.
///
/// # Example
///
/// ```rust
/// use krafka::schema_registry::glue::GlueSchemaVersionId;
///
/// let id: GlueSchemaVersionId = "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
/// assert_eq!(id.to_string(), "550e8400-e29b-41d4-a716-446655440000");
/// assert_eq!(id.as_bytes().len(), 16);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct GlueSchemaVersionId([u8; UUID_SIZE]);

impl GlueSchemaVersionId {
    /// Create from raw big-endian UUID bytes.
    pub fn from_bytes(bytes: [u8; UUID_SIZE]) -> Self {
        Self(bytes)
    }

    /// Return the raw big-endian UUID bytes.
    pub fn as_bytes(&self) -> &[u8; UUID_SIZE] {
        &self.0
    }
}

impl From<[u8; UUID_SIZE]> for GlueSchemaVersionId {
    fn from(bytes: [u8; UUID_SIZE]) -> Self {
        Self(bytes)
    }
}

impl From<GlueSchemaVersionId> for [u8; UUID_SIZE] {
    fn from(id: GlueSchemaVersionId) -> Self {
        id.0
    }
}

impl fmt::Display for GlueSchemaVersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let b = &self.0;
        write!(
            f,
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0],
            b[1],
            b[2],
            b[3],
            b[4],
            b[5],
            b[6],
            b[7],
            b[8],
            b[9],
            b[10],
            b[11],
            b[12],
            b[13],
            b[14],
            b[15]
        )
    }
}

impl fmt::Debug for GlueSchemaVersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GlueSchemaVersionId({self})")
    }
}

impl FromStr for GlueSchemaVersionId {
    type Err = KrafkaError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        let s = s.as_bytes();
        if s.len() != 36 {
            return Err(KrafkaError::schema_registry(format!(
                "invalid UUID: expected 36 characters, got {}",
                s.len()
            )));
        }
        if s[8] != b'-' || s[13] != b'-' || s[18] != b'-' || s[23] != b'-' {
            return Err(KrafkaError::schema_registry(
                "invalid UUID format: expected dashes at positions 8, 13, 18, 23",
            ));
        }
        let hex_positions: [usize; UUID_SIZE] =
            [0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34];
        let mut bytes = [0u8; UUID_SIZE];
        for (i, &pos) in hex_positions.iter().enumerate() {
            bytes[i] = parse_hex_byte(s[pos], s[pos + 1]).ok_or_else(|| {
                KrafkaError::schema_registry("invalid UUID: non-hexadecimal character")
            })?;
        }
        Ok(Self(bytes))
    }
}

/// Parse two ASCII hex digits into a byte.
fn parse_hex_byte(hi: u8, lo: u8) -> Option<u8> {
    Some((hex_digit(hi)? << 4) | hex_digit(lo)?)
}

/// Convert a single ASCII hex digit to its numeric value.
fn hex_digit(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

/// Compression type used in the AWS Glue wire format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[non_exhaustive]
pub enum GlueCompression {
    /// No compression (wire format byte `0x00`).
    #[default]
    None,
    /// ZLIB compression (wire format byte `0x05`).
    Zlib,
}

impl GlueCompression {
    /// Return the canonical uppercase name (`"NONE"`, `"ZLIB"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::None => "NONE",
            Self::Zlib => "ZLIB",
        }
    }
}

impl fmt::Display for GlueCompression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Data format supported by the AWS Glue Schema Registry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum GlueDataFormat {
    /// Apache Avro.
    Avro,
    /// JSON Schema.
    Json,
    /// Protocol Buffers.
    Protobuf,
}

impl GlueDataFormat {
    /// Return the canonical uppercase name (`"AVRO"`, `"JSON"`, `"PROTOBUF"`).
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Avro => "AVRO",
            Self::Json => "JSON",
            Self::Protobuf => "PROTOBUF",
        }
    }
}

impl fmt::Display for GlueDataFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for GlueDataFormat {
    type Err = KrafkaError;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if s.eq_ignore_ascii_case("AVRO") {
            Ok(Self::Avro)
        } else if s.eq_ignore_ascii_case("JSON") {
            Ok(Self::Json)
        } else if s.eq_ignore_ascii_case("PROTOBUF") {
            Ok(Self::Protobuf)
        } else {
            Err(KrafkaError::schema_registry(format!(
                "unknown Glue data format: '{s}'"
            )))
        }
    }
}

/// A schema retrieved from the AWS Glue Schema Registry.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlueSchema {
    /// Globally unique schema version ID (UUID).
    pub schema_version_id: GlueSchemaVersionId,
    /// Data format (Avro, JSON, Protobuf).
    pub data_format: GlueDataFormat,
    /// Schema definition string.
    ///
    /// For Avro and JSON Schema this is a JSON string. For Protobuf this is
    /// the `.proto` file content.
    pub schema_definition: String,
    /// Schema ARN (present when fetched from the registry).
    pub schema_arn: Option<String>,
    /// Schema version number within its schema (1-based, present when fetched).
    pub version_number: Option<i64>,
}

impl GlueSchema {
    /// Create a schema with the given version ID, data format, and definition.
    pub fn new(
        schema_version_id: GlueSchemaVersionId,
        data_format: GlueDataFormat,
        schema_definition: impl Into<String>,
    ) -> Self {
        Self {
            schema_version_id,
            data_format,
            schema_definition: schema_definition.into(),
            schema_arn: None,
            version_number: None,
        }
    }

    /// Set the schema ARN and version number.
    pub fn with_metadata(mut self, schema_arn: impl Into<String>, version_number: i64) -> Self {
        self.schema_arn = Some(schema_arn.into());
        self.version_number = Some(version_number);
        self
    }
}

// ── Wire format ──────────────────────────────────────────────────────────

/// Encode a payload with the AWS Glue wire format header.
///
/// Prepends an 18-byte header (version byte `0x03` + compression byte +
/// 16-byte UUID) to the payload. When `compression` is
/// [`GlueCompression::Zlib`], the payload is ZLIB-compressed before framing.
///
/// # Errors
///
/// Returns a serialization error if ZLIB compression fails.
///
/// # Example
///
/// ```rust
/// use krafka::schema_registry::glue::{
///     encode_glue_wire_format, GlueSchemaVersionId, GlueCompression,
/// };
///
/// let uuid: GlueSchemaVersionId = "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
/// let framed = encode_glue_wire_format(uuid, b"hello", GlueCompression::None).unwrap();
/// assert_eq!(framed[0], 0x03);
/// assert_eq!(framed[1], 0x00);
/// assert_eq!(framed.len(), 18 + 5);
/// ```
pub fn encode_glue_wire_format(
    schema_version_id: GlueSchemaVersionId,
    payload: &[u8],
    compression: GlueCompression,
) -> Result<Bytes> {
    let compressed;
    let (compression_byte, payload_bytes): (u8, &[u8]) = match compression {
        GlueCompression::None => (GLUE_COMPRESSION_NONE_BYTE, payload),
        GlueCompression::Zlib => {
            compressed = compress_zlib(payload)?;
            (GLUE_COMPRESSION_ZLIB_BYTE, &compressed)
        }
    };
    let mut buf = BytesMut::with_capacity(GLUE_HEADER_SIZE + payload_bytes.len());
    buf.put_u8(GLUE_HEADER_VERSION_BYTE);
    buf.put_u8(compression_byte);
    buf.put_slice(schema_version_id.as_bytes());
    buf.put_slice(payload_bytes);
    Ok(buf.freeze())
}

/// Decode an AWS Glue wire format message.
///
/// Validates the 18-byte header, extracts the schema version ID, and
/// decompresses the payload if ZLIB-compressed. Always returns an owned
/// `Vec<u8>` — for zero-copy decoding of uncompressed [`Bytes`] payloads,
/// use [`decode_glue_wire_format_bytes()`] instead.
///
/// # Errors
///
/// Returns a serialization error if:
/// - The data is shorter than 18 bytes.
/// - The header version byte is not `0x03`.
/// - The compression byte is unrecognized.
/// - ZLIB decompression fails.
/// - The decompressed payload exceeds 128 MiB (decompression bomb protection).
///
/// # Example
///
/// ```rust
/// use krafka::schema_registry::glue::{
///     encode_glue_wire_format, decode_glue_wire_format, GlueCompression,
/// };
///
/// let uuid = "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
/// let framed = encode_glue_wire_format(uuid, b"data", GlueCompression::None).unwrap();
/// let (id, payload) = decode_glue_wire_format(&framed).unwrap();
/// assert_eq!(id, uuid);
/// assert_eq!(&payload, b"data");
/// ```
pub fn decode_glue_wire_format(data: &[u8]) -> Result<(GlueSchemaVersionId, Vec<u8>)> {
    let (schema_version_id, compression) = validate_glue_wire_header(data)?;
    let raw = &data[GLUE_HEADER_SIZE..];
    let payload = match compression {
        GlueCompression::None => raw.to_vec(),
        GlueCompression::Zlib => decompress_zlib(raw)?,
    };
    Ok((schema_version_id, payload))
}

/// Decode an AWS Glue wire format message, returning a [`Bytes`] payload.
///
/// For uncompressed payloads, the returned [`Bytes`] shares the same
/// backing allocation as `data` (zero-copy). For ZLIB-compressed payloads,
/// the data is decompressed into a new allocation.
///
/// # Errors
///
/// Same as [`decode_glue_wire_format()`].
///
/// # Example
///
/// ```rust
/// use bytes::Bytes;
/// use krafka::schema_registry::glue::{
///     encode_glue_wire_format, decode_glue_wire_format_bytes, GlueCompression,
/// };
///
/// let uuid = "550e8400-e29b-41d4-a716-446655440000".parse().unwrap();
/// let framed = encode_glue_wire_format(uuid, b"data", GlueCompression::None).unwrap();
/// let (id, payload) = decode_glue_wire_format_bytes(&framed).unwrap();
/// assert_eq!(id, uuid);
/// assert_eq!(&payload[..], b"data");
/// ```
pub fn decode_glue_wire_format_bytes(data: &Bytes) -> Result<(GlueSchemaVersionId, Bytes)> {
    let (schema_version_id, compression) = validate_glue_wire_header(data)?;
    let payload = match compression {
        GlueCompression::None => data.slice(GLUE_HEADER_SIZE..),
        GlueCompression::Zlib => {
            let decompressed = decompress_zlib(&data[GLUE_HEADER_SIZE..])?;
            Bytes::from(decompressed)
        }
    };
    Ok((schema_version_id, payload))
}

/// Validate the Glue wire format header and extract the schema version ID
/// and compression type.
fn validate_glue_wire_header(data: &[u8]) -> Result<(GlueSchemaVersionId, GlueCompression)> {
    if data.len() < GLUE_HEADER_SIZE {
        return Err(KrafkaError::serialization(format!(
            "Glue wire format data too short: expected at least {GLUE_HEADER_SIZE} bytes, got {}",
            data.len()
        )));
    }
    if data[0] != GLUE_HEADER_VERSION_BYTE {
        return Err(KrafkaError::serialization(format!(
            "invalid Glue wire format header version byte: expected 0x{GLUE_HEADER_VERSION_BYTE:02X}, got 0x{:02X}",
            data[0]
        )));
    }
    let compression = match data[1] {
        GLUE_COMPRESSION_NONE_BYTE => GlueCompression::None,
        GLUE_COMPRESSION_ZLIB_BYTE => GlueCompression::Zlib,
        other => {
            return Err(KrafkaError::serialization(format!(
                "unknown Glue wire format compression byte: 0x{other:02X}"
            )));
        }
    };
    let mut uuid_bytes = [0u8; UUID_SIZE];
    uuid_bytes.copy_from_slice(&data[2..GLUE_HEADER_SIZE]);
    Ok((GlueSchemaVersionId(uuid_bytes), compression))
}

/// ZLIB-compress data.
fn compress_zlib(data: &[u8]) -> Result<Vec<u8>> {
    let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(data)
        .map_err(|e| KrafkaError::serialization(format!("ZLIB compression failed: {e}")))?;
    encoder
        .finish()
        .map_err(|e| KrafkaError::serialization(format!("ZLIB compression failed: {e}")))
}

/// Maximum decompressed size (128 MiB) to protect against decompression bombs.
///
/// Matches the limit used by record-batch decompression in `protocol::record`.
const MAX_DECOMPRESSED_SIZE: usize = 128 * 1024 * 1024;

/// ZLIB-decompress data with a size limit to prevent decompression bombs.
fn decompress_zlib(data: &[u8]) -> Result<Vec<u8>> {
    let decoder = ZlibDecoder::new(data);
    let mut limited = decoder.take(MAX_DECOMPRESSED_SIZE as u64 + 1);
    let mut decompressed = Vec::new();
    limited
        .read_to_end(&mut decompressed)
        .map_err(|e| KrafkaError::serialization(format!("ZLIB decompression failed: {e}")))?;
    if decompressed.len() > MAX_DECOMPRESSED_SIZE {
        return Err(KrafkaError::serialization(format!(
            "ZLIB decompressed size {} exceeds maximum {} bytes (possible decompression bomb)",
            decompressed.len(),
            MAX_DECOMPRESSED_SIZE
        )));
    }
    Ok(decompressed)
}

// ── Trait ─────────────────────────────────────────────────────────────────

/// Async client interface for the AWS Glue Schema Registry.
///
/// Implement this trait to integrate with the Glue backend. When the
/// `aws-glue-schema-registry` feature is enabled,
/// `AwsGlueSchemaRegistry` provides a ready-made AWS SDK implementation.
///
/// All methods return boxed futures for object safety, following the same
/// pattern as [`SchemaRegistryClient`](super::SchemaRegistryClient).
pub trait GlueSchemaRegistryClient: Send + Sync {
    /// Retrieve a schema by its version ID (the UUID from the wire format).
    ///
    /// Schema version IDs are immutable — a given UUID always maps to the
    /// same schema definition.
    fn get_schema_by_version_id(
        &self,
        id: GlueSchemaVersionId,
    ) -> Pin<Box<dyn Future<Output = Result<GlueSchema>> + Send + '_>>;

    /// Register a schema version (idempotent).
    ///
    /// If the same schema definition is already registered under
    /// `schema_name`, the existing version ID is returned.
    fn register_schema(
        &self,
        schema_name: &str,
        schema: &str,
        data_format: GlueDataFormat,
    ) -> Pin<Box<dyn Future<Output = Result<GlueSchemaVersionId>> + Send + '_>>;
}

// ── CachedGlueSchemaRegistry ─────────────────────────────────────────────

/// Caching wrapper around any [`GlueSchemaRegistryClient`].
///
/// Caches schema-version-ID-to-schema lookups in memory. Because a schema
/// version ID (UUID) always maps to the same definition, cached entries
/// never expire.
///
/// # Example
///
/// ```rust,ignore
/// use krafka::schema_registry::glue::{
///     CachedGlueSchemaRegistry, AwsGlueSchemaRegistry,
/// };
///
/// let client = AwsGlueSchemaRegistry::new(glue_client, "my-registry");
/// let cached = CachedGlueSchemaRegistry::new(client);
///
/// // First call fetches from the registry:
/// let schema = cached.get_schema_by_version_id(uuid).await?;
///
/// // Second call is served from cache:
/// let same = cached.get_schema_by_version_id(uuid).await?;
/// ```
pub struct CachedGlueSchemaRegistry<C> {
    /// The inner registry client.
    inner: C,
    /// Schema version ID → GlueSchema cache.
    cache: RwLock<HashMap<GlueSchemaVersionId, GlueSchema>>,
    /// Insertion order for bounded-cache eviction.
    insertion_order: RwLock<VecDeque<GlueSchemaVersionId>>,
    /// Optional maximum number of cached entries.
    max_entries: Option<usize>,
    /// Waiters for coalescing concurrent cold misses by schema version ID.
    in_flight: AsyncMutex<HashMap<GlueSchemaVersionId, Vec<oneshot::Sender<Result<GlueSchema>>>>>,
}

impl<C: GlueSchemaRegistryClient> CachedGlueSchemaRegistry<C> {
    /// Wrap the given client with an in-memory cache.
    pub fn new(inner: C) -> Self {
        Self {
            inner,
            cache: RwLock::new(HashMap::new()),
            insertion_order: RwLock::new(VecDeque::new()),
            max_entries: None,
            in_flight: AsyncMutex::new(HashMap::new()),
        }
    }

    /// Wrap the given client with a pre-allocated cache.
    pub fn with_capacity(inner: C, capacity: usize) -> Self {
        Self {
            inner,
            cache: RwLock::new(HashMap::with_capacity(capacity)),
            insertion_order: RwLock::new(VecDeque::with_capacity(capacity)),
            max_entries: None,
            in_flight: AsyncMutex::new(HashMap::new()),
        }
    }

    /// Wrap the given client with a bounded in-memory cache.
    ///
    /// When the cache reaches `max_entries`, the oldest inserted schema version ID is evicted.
    pub fn with_max_entries(inner: C, max_entries: usize) -> Self {
        let max_entries = max_entries.max(1);
        Self {
            inner,
            cache: RwLock::new(HashMap::with_capacity(max_entries)),
            insertion_order: RwLock::new(VecDeque::with_capacity(max_entries)),
            max_entries: Some(max_entries),
            in_flight: AsyncMutex::new(HashMap::new()),
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

    fn insert_cache_entry(&self, id: GlueSchemaVersionId, schema: GlueSchema) {
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

impl<C> fmt::Debug for CachedGlueSchemaRegistry<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CachedGlueSchemaRegistry")
            .field("cache_len", &self.cache.read().len())
            .field("max_entries", &self.max_entries)
            .finish()
    }
}

impl<C: GlueSchemaRegistryClient> GlueSchemaRegistryClient for CachedGlueSchemaRegistry<C> {
    fn get_schema_by_version_id(
        &self,
        id: GlueSchemaVersionId,
    ) -> Pin<Box<dyn Future<Output = Result<GlueSchema>> + Send + '_>> {
        Box::pin(async move {
            // Fast path: read lock only.
            if let Some(schema) = self.cache.read().get(&id) {
                return Ok(schema.clone());
            }

            let mut in_flight = self.in_flight.lock().await;
            if let Some(schema) = self.cache.read().get(&id) {
                return Ok(schema.clone());
            }

            if let Some(waiters) = in_flight.get_mut(&id) {
                let (tx, rx) = oneshot::channel();
                waiters.push(tx);
                drop(in_flight);
                return rx.await.map_err(|_| {
                    KrafkaError::invalid_state("glue schema lookup coalescer dropped")
                })?;
            }

            in_flight.insert(id, Vec::new());
            drop(in_flight);

            let result = self.inner.get_schema_by_version_id(id).await;
            if let Ok(schema) = &result {
                self.insert_cache_entry(id, schema.clone());
            }

            let waiters = self.in_flight.lock().await.remove(&id).unwrap_or_default();
            for waiter in waiters {
                let _ = waiter.send(result.clone());
            }

            result
        })
    }

    fn register_schema(
        &self,
        schema_name: &str,
        schema: &str,
        data_format: GlueDataFormat,
    ) -> Pin<Box<dyn Future<Output = Result<GlueSchemaVersionId>> + Send + '_>> {
        let schema_name = schema_name.to_string();
        let schema = schema.to_string();
        Box::pin(async move {
            self.inner
                .register_schema(&schema_name, &schema, data_format)
                .await
        })
    }
}

// ── Tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU32, Ordering};
    use tokio::sync::Notify;

    const TEST_UUID_STR: &str = "550e8400-e29b-41d4-a716-446655440000";
    const TEST_UUID_BYTES: [u8; 16] = [
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00,
        0x00,
    ];

    // ── GlueSchemaVersionId ──────────────────────────────────────────────

    #[test]
    fn test_uuid_from_str() {
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        assert_eq!(id.as_bytes(), &TEST_UUID_BYTES);
    }

    #[test]
    fn test_uuid_display() {
        let id = GlueSchemaVersionId::from_bytes(TEST_UUID_BYTES);
        assert_eq!(id.to_string(), TEST_UUID_STR);
    }

    #[test]
    fn test_uuid_roundtrip() {
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        assert_eq!(id.to_string(), TEST_UUID_STR);
    }

    #[test]
    fn test_uuid_debug() {
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        let debug = format!("{id:?}");
        assert!(debug.contains(TEST_UUID_STR));
        assert!(debug.contains("GlueSchemaVersionId"));
    }

    #[test]
    fn test_uuid_from_bytes_into_bytes() {
        let id = GlueSchemaVersionId::from(TEST_UUID_BYTES);
        let bytes: [u8; 16] = id.into();
        assert_eq!(bytes, TEST_UUID_BYTES);
    }

    #[test]
    fn test_uuid_equality() {
        let a = GlueSchemaVersionId::from_bytes(TEST_UUID_BYTES);
        let b: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        assert_eq!(a, b);
    }

    #[test]
    fn test_uuid_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        set.insert(id);
        assert!(set.contains(&id));
    }

    #[test]
    fn test_uuid_invalid_length() {
        let result = "550e8400-e29b-41d4-a716".parse::<GlueSchemaVersionId>();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("36 characters"));
    }

    #[test]
    fn test_uuid_invalid_dashes() {
        let result = "550e8400xe29b-41d4-a716-446655440000".parse::<GlueSchemaVersionId>();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("dashes"));
    }

    #[test]
    fn test_uuid_invalid_hex() {
        let result = "550e8400-e29b-41d4-a716-44665544000g".parse::<GlueSchemaVersionId>();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("hex"));
    }

    #[test]
    fn test_uuid_uppercase_hex() {
        let id: GlueSchemaVersionId = "550E8400-E29B-41D4-A716-446655440000".parse().unwrap();
        assert_eq!(id.as_bytes(), &TEST_UUID_BYTES);
    }

    #[test]
    fn test_uuid_all_zeros() {
        let id: GlueSchemaVersionId = "00000000-0000-0000-0000-000000000000".parse().unwrap();
        assert_eq!(id.as_bytes(), &[0u8; 16]);
    }

    #[test]
    fn test_uuid_all_ones() {
        let id: GlueSchemaVersionId = "ffffffff-ffff-ffff-ffff-ffffffffffff".parse().unwrap();
        assert_eq!(id.as_bytes(), &[0xffu8; 16]);
    }

    // ── GlueCompression ──────────────────────────────────────────────────

    #[test]
    fn test_compression_default() {
        assert_eq!(GlueCompression::default(), GlueCompression::None);
    }

    #[test]
    fn test_compression_display() {
        assert_eq!(GlueCompression::None.to_string(), "NONE");
        assert_eq!(GlueCompression::Zlib.to_string(), "ZLIB");
    }

    // ── GlueDataFormat ───────────────────────────────────────────────────

    #[test]
    fn test_data_format_display() {
        assert_eq!(GlueDataFormat::Avro.to_string(), "AVRO");
        assert_eq!(GlueDataFormat::Json.to_string(), "JSON");
        assert_eq!(GlueDataFormat::Protobuf.to_string(), "PROTOBUF");
    }

    #[test]
    fn test_data_format_from_str() {
        assert_eq!(
            "AVRO".parse::<GlueDataFormat>().unwrap(),
            GlueDataFormat::Avro
        );
        assert_eq!(
            "JSON".parse::<GlueDataFormat>().unwrap(),
            GlueDataFormat::Json
        );
        assert_eq!(
            "PROTOBUF".parse::<GlueDataFormat>().unwrap(),
            GlueDataFormat::Protobuf
        );
    }

    #[test]
    fn test_data_format_from_str_case_insensitive() {
        assert_eq!(
            "avro".parse::<GlueDataFormat>().unwrap(),
            GlueDataFormat::Avro
        );
        assert_eq!(
            "Json".parse::<GlueDataFormat>().unwrap(),
            GlueDataFormat::Json
        );
        assert_eq!(
            "protobuf".parse::<GlueDataFormat>().unwrap(),
            GlueDataFormat::Protobuf
        );
    }

    #[test]
    fn test_data_format_from_str_unknown() {
        let result = "XML".parse::<GlueDataFormat>();
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("XML"));
    }

    // ── GlueSchema ───────────────────────────────────────────────────────

    #[test]
    fn test_glue_schema_new() {
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        let s = GlueSchema::new(id, GlueDataFormat::Avro, r#"{"type":"string"}"#);
        assert_eq!(s.schema_version_id, id);
        assert_eq!(s.data_format, GlueDataFormat::Avro);
        assert_eq!(s.schema_definition, r#"{"type":"string"}"#);
        assert_eq!(s.schema_arn, None);
        assert_eq!(s.version_number, None);
    }

    #[test]
    fn test_glue_schema_with_metadata() {
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        let s = GlueSchema::new(id, GlueDataFormat::Avro, "{}")
            .with_metadata("arn:aws:glue:us-east-1:123:schema/default-registry/test", 3);
        assert_eq!(
            s.schema_arn,
            Some("arn:aws:glue:us-east-1:123:schema/default-registry/test".to_string())
        );
        assert_eq!(s.version_number, Some(3));
    }

    // ── Wire format (uncompressed) ───────────────────────────────────────

    #[test]
    fn test_wire_format_roundtrip_uncompressed() {
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        let payload = b"hello world";
        let encoded = encode_glue_wire_format(id, payload, GlueCompression::None).unwrap();
        let (decoded_id, decoded_payload) = decode_glue_wire_format(&encoded).unwrap();
        assert_eq!(decoded_id, id);
        assert_eq!(&decoded_payload, payload);
    }

    #[test]
    fn test_wire_format_header_bytes_uncompressed() {
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        let encoded = encode_glue_wire_format(id, b"x", GlueCompression::None).unwrap();
        assert_eq!(encoded[0], 0x03); // header version byte
        assert_eq!(encoded[1], 0x00); // no compression
        assert_eq!(&encoded[2..18], &TEST_UUID_BYTES); // UUID
        assert_eq!(&encoded[18..], b"x"); // payload
        assert_eq!(encoded.len(), GLUE_HEADER_SIZE + 1);
    }

    #[test]
    fn test_wire_format_empty_payload() {
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        let encoded = encode_glue_wire_format(id, b"", GlueCompression::None).unwrap();
        assert_eq!(encoded.len(), GLUE_HEADER_SIZE);
        let (_, payload) = decode_glue_wire_format(&encoded).unwrap();
        assert!(payload.is_empty());
    }

    // ── Wire format (ZLIB compressed) ────────────────────────────────────

    #[test]
    fn test_wire_format_roundtrip_zlib() {
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        let payload = b"hello world compressed data that benefits from compression";
        let encoded = encode_glue_wire_format(id, payload, GlueCompression::Zlib).unwrap();
        assert_eq!(encoded[1], 0x05); // ZLIB compression byte
        let (decoded_id, decoded_payload) = decode_glue_wire_format(&encoded).unwrap();
        assert_eq!(decoded_id, id);
        assert_eq!(&decoded_payload, payload);
    }

    #[test]
    fn test_wire_format_zlib_empty_payload() {
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        let encoded = encode_glue_wire_format(id, b"", GlueCompression::Zlib).unwrap();
        let (_, payload) = decode_glue_wire_format(&encoded).unwrap();
        assert!(payload.is_empty());
    }

    // ── Wire format (Bytes variant) ──────────────────────────────────────

    #[test]
    fn test_wire_format_bytes_roundtrip_uncompressed() {
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        let payload = b"hello world";
        let encoded = encode_glue_wire_format(id, payload, GlueCompression::None).unwrap();
        let (decoded_id, decoded_payload) = decode_glue_wire_format_bytes(&encoded).unwrap();
        assert_eq!(decoded_id, id);
        assert_eq!(&decoded_payload[..], payload);
    }

    #[test]
    fn test_wire_format_bytes_roundtrip_zlib() {
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        let payload = b"compressed bytes payload";
        let encoded = encode_glue_wire_format(id, payload, GlueCompression::Zlib).unwrap();
        let (decoded_id, decoded_payload) = decode_glue_wire_format_bytes(&encoded).unwrap();
        assert_eq!(decoded_id, id);
        assert_eq!(&decoded_payload[..], payload);
    }

    #[test]
    fn test_wire_format_bytes_zero_copy_uncompressed() {
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        let encoded = encode_glue_wire_format(id, b"shared", GlueCompression::None).unwrap();
        let (_, payload) = decode_glue_wire_format_bytes(&encoded).unwrap();
        assert_eq!(&payload[..], b"shared");
    }

    // ── Wire format (error cases) ────────────────────────────────────────

    #[test]
    fn test_wire_format_too_short() {
        let result = decode_glue_wire_format(&[0x03, 0x00, 0x01]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too short"));
    }

    #[test]
    fn test_wire_format_empty_data() {
        let result = decode_glue_wire_format(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_wire_format_invalid_header_version() {
        let mut data = [0u8; GLUE_HEADER_SIZE];
        data[0] = 0x00; // Wrong header version (this is Confluent's magic byte)
        let result = decode_glue_wire_format(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("header version"));
    }

    #[test]
    fn test_wire_format_unknown_compression() {
        let mut data = [0u8; GLUE_HEADER_SIZE];
        data[0] = 0x03;
        data[1] = 0xFF; // Unknown compression
        let result = decode_glue_wire_format(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("compression"));
    }

    #[test]
    fn test_wire_format_bytes_too_short() {
        let data = Bytes::from_static(&[0x03, 0x00]);
        let result = decode_glue_wire_format_bytes(&data);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too short"));
    }

    #[test]
    fn test_wire_format_bytes_invalid_header() {
        let data = Bytes::from_static(&[0x01; GLUE_HEADER_SIZE]);
        let result = decode_glue_wire_format_bytes(&data);
        assert!(result.is_err());
    }

    // ── ZLIB helpers ─────────────────────────────────────────────────────

    #[test]
    fn test_zlib_roundtrip() {
        let original = b"test data for ZLIB compression roundtrip";
        let compressed = compress_zlib(original).unwrap();
        let decompressed = decompress_zlib(&compressed).unwrap();
        assert_eq!(&decompressed, original);
    }

    #[test]
    fn test_zlib_empty() {
        let compressed = compress_zlib(b"").unwrap();
        let decompressed = decompress_zlib(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_zlib_invalid_data() {
        let result = decompress_zlib(&[0xFF, 0xFE, 0xFD]);
        assert!(result.is_err());
    }

    // ── CachedGlueSchemaRegistry ─────────────────────────────────────────

    struct MockGlueRegistry {
        get_calls: AtomicU32,
    }

    impl MockGlueRegistry {
        fn new() -> Self {
            Self {
                get_calls: AtomicU32::new(0),
            }
        }

        fn get_call_count(&self) -> u32 {
            self.get_calls.load(Ordering::SeqCst)
        }
    }

    impl GlueSchemaRegistryClient for MockGlueRegistry {
        fn get_schema_by_version_id(
            &self,
            id: GlueSchemaVersionId,
        ) -> Pin<Box<dyn Future<Output = Result<GlueSchema>> + Send + '_>> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
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
            Box::pin(async { Ok(TEST_UUID_STR.parse().unwrap()) })
        }
    }

    struct BlockingMockGlueRegistry {
        get_calls: AtomicU32,
        started: Notify,
        release: Notify,
    }

    impl BlockingMockGlueRegistry {
        fn new() -> Self {
            Self {
                get_calls: AtomicU32::new(0),
                started: Notify::new(),
                release: Notify::new(),
            }
        }

        fn get_call_count(&self) -> u32 {
            self.get_calls.load(Ordering::SeqCst)
        }

        async fn wait_started(&self) {
            self.started.notified().await;
        }

        fn release(&self) {
            self.release.notify_waiters();
        }
    }

    impl GlueSchemaRegistryClient for BlockingMockGlueRegistry {
        fn get_schema_by_version_id(
            &self,
            id: GlueSchemaVersionId,
        ) -> Pin<Box<dyn Future<Output = Result<GlueSchema>> + Send + '_>> {
            self.get_calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                self.started.notify_waiters();
                self.release.notified().await;
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
            Box::pin(async { Ok(TEST_UUID_STR.parse().unwrap()) })
        }
    }

    #[tokio::test]
    async fn test_cache_miss_then_hit() {
        let mock = MockGlueRegistry::new();
        let cached = CachedGlueSchemaRegistry::new(mock);
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();

        // First call: cache miss → hits mock
        let s1 = cached.get_schema_by_version_id(id).await.unwrap();
        assert_eq!(cached.inner().get_call_count(), 1);
        assert_eq!(cached.cache_len(), 1);

        // Second call: cache hit → does NOT hit mock
        let s2 = cached.get_schema_by_version_id(id).await.unwrap();
        assert_eq!(cached.inner().get_call_count(), 1);

        assert_eq!(s1, s2);
    }

    #[tokio::test]
    async fn test_cache_different_ids() {
        let mock = MockGlueRegistry::new();
        let cached = CachedGlueSchemaRegistry::new(mock);
        let id1: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        let id2: GlueSchemaVersionId = "00000000-0000-0000-0000-000000000001".parse().unwrap();

        cached.get_schema_by_version_id(id1).await.unwrap();
        cached.get_schema_by_version_id(id2).await.unwrap();
        assert_eq!(cached.inner().get_call_count(), 2);
        assert_eq!(cached.cache_len(), 2);

        // Both cached now
        cached.get_schema_by_version_id(id1).await.unwrap();
        cached.get_schema_by_version_id(id2).await.unwrap();
        assert_eq!(cached.inner().get_call_count(), 2);
    }

    #[tokio::test]
    async fn test_cache_clear() {
        let mock = MockGlueRegistry::new();
        let cached = CachedGlueSchemaRegistry::new(mock);
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();

        cached.get_schema_by_version_id(id).await.unwrap();
        assert_eq!(cached.cache_len(), 1);

        cached.clear_cache();
        assert_eq!(cached.cache_len(), 0);
        assert!(cached.cache_is_empty());

        // After clear, next call hits mock again
        cached.get_schema_by_version_id(id).await.unwrap();
        assert_eq!(cached.inner().get_call_count(), 2);
    }

    #[tokio::test]
    async fn test_cache_coalesces_concurrent_misses() {
        let cached = Arc::new(CachedGlueSchemaRegistry::new(
            BlockingMockGlueRegistry::new(),
        ));
        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();

        let first = {
            let cached = cached.clone();
            tokio::spawn(async move { cached.get_schema_by_version_id(id).await.unwrap() })
        };

        cached.inner().wait_started().await;

        let second = {
            let cached = cached.clone();
            tokio::spawn(async move { cached.get_schema_by_version_id(id).await.unwrap() })
        };

        tokio::task::yield_now().await;
        cached.inner().release();

        let first_schema = first.await.unwrap();
        let second_schema = second.await.unwrap();

        assert_eq!(first_schema, second_schema);
        assert_eq!(cached.inner().get_call_count(), 1);
    }

    #[tokio::test]
    async fn test_cache_register_forwards() {
        let mock = MockGlueRegistry::new();
        let cached = CachedGlueSchemaRegistry::new(mock);

        let id = cached
            .register_schema("my-schema", "{}", GlueDataFormat::Avro)
            .await
            .unwrap();
        assert_eq!(id.to_string(), TEST_UUID_STR);
    }

    #[tokio::test]
    async fn test_cache_with_capacity() {
        let mock = MockGlueRegistry::new();
        let cached = CachedGlueSchemaRegistry::with_capacity(mock, 100);
        assert!(cached.cache_is_empty());

        let id: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        cached.get_schema_by_version_id(id).await.unwrap();
        assert_eq!(cached.cache_len(), 1);
    }

    #[tokio::test]
    async fn test_cache_with_max_entries_evicts_oldest_entry() {
        let mock = MockGlueRegistry::new();
        let cached = CachedGlueSchemaRegistry::with_max_entries(mock, 1);
        let id1: GlueSchemaVersionId = TEST_UUID_STR.parse().unwrap();
        let id2: GlueSchemaVersionId = "00000000-0000-0000-0000-000000000001".parse().unwrap();

        cached.get_schema_by_version_id(id1).await.unwrap();
        cached.get_schema_by_version_id(id2).await.unwrap();

        assert_eq!(cached.cache_len(), 1);
        assert_eq!(cached.inner().get_call_count(), 2);

        cached.get_schema_by_version_id(id1).await.unwrap();
        assert_eq!(cached.inner().get_call_count(), 3);
    }

    // ── Type assertions ──────────────────────────────────────────────────

    #[test]
    fn test_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<GlueSchema>();
        assert_send_sync::<GlueSchemaVersionId>();
        assert_send_sync::<GlueDataFormat>();
        assert_send_sync::<GlueCompression>();
        assert_send_sync::<CachedGlueSchemaRegistry<MockGlueRegistry>>();
    }

    /// Verify that [`GlueSchemaRegistryClient`] is object-safe.
    #[test]
    fn test_object_safe() {
        fn _assert_object_safe(_: &dyn GlueSchemaRegistryClient) {}
    }

    #[test]
    fn test_cached_debug() {
        let cached = CachedGlueSchemaRegistry::new(MockGlueRegistry::new());
        let debug = format!("{cached:?}");
        assert!(debug.contains("cache_len"));
    }
}
