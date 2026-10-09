//! Kafka record batch implementation.
//!
//! This module implements the Kafka record batch format (v2),
//! which is used for both producing and consuming messages.

use super::decode_capacity;
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::util::{crc32c, varint};

/// Compression codec.
///
/// All variants are always available because they represent wire-format values
/// (bits 0–2 of the record batch attributes field). Every codec decodes in
/// every build. Gzip, Snappy and LZ4 also encode in every build; Zstd encodes
/// only with the `zstd` Cargo feature.
///
/// Use [`Compression::is_available`] to check whether the codec can encode.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum Compression {
    /// No compression.
    #[default]
    None = 0,
    /// Gzip compression.
    Gzip = 1,
    /// Snappy compression.
    Snappy = 2,
    /// LZ4 compression.
    Lz4 = 3,
    /// Zstd compression.
    ///
    /// Decoding is pure Rust and always available. Encoding requires the
    /// `zstd` Cargo feature, which pulls in `zstd-sys` and needs a C toolchain.
    Zstd = 4,
}

impl Compression {
    /// Create from a raw telemetry / protocol compression identifier.
    #[inline]
    #[must_use]
    pub const fn from_i8(value: i8) -> Option<Self> {
        match value {
            0 => Some(Self::None),
            1 => Some(Self::Gzip),
            2 => Some(Self::Snappy),
            3 => Some(Self::Lz4),
            4 => Some(Self::Zstd),
            _ => None,
        }
    }

    /// Create from the lower 3 bits of a record batch attributes field.
    ///
    /// Returns `None` for unknown discriminants (values 5–7). Callers should
    /// propagate `None` as a `ProtocolErrorKind::InvalidValue` rather than
    /// silently falling back to `Compression::None`, which would attempt to
    /// interpret compressed bytes as raw data and produce garbage records.
    #[inline]
    #[must_use]
    pub const fn from_u8(value: u8) -> Option<Self> {
        match value & 0x07 {
            0 => Some(Self::None),
            1 => Some(Self::Gzip),
            2 => Some(Self::Snappy),
            3 => Some(Self::Lz4),
            4 => Some(Self::Zstd),
            _ => None,
        }
    }

    /// Returns `true` if the codec can encode in this build.
    ///
    /// Only `Compression::Zstd` can be missing: encoding it needs the `zstd`
    /// Cargo feature. Every codec decodes in every build.
    ///
    /// # Examples
    ///
    /// ```
    /// use krafka::Compression;
    ///
    /// assert!(Compression::None.is_available());
    /// ```
    #[inline]
    #[must_use]
    pub const fn is_available(&self) -> bool {
        match self {
            Self::Zstd => cfg!(feature = "zstd"),
            Self::None | Self::Gzip | Self::Snappy | Self::Lz4 => true,
        }
    }

    /// Whether this codec accepts a compression level.
    ///
    /// `Snappy` has no level in its format at all. `Lz4` does in principle,
    /// but krafka encodes LZ4 with `lz4_flex`, whose frame encoder exposes no
    /// level — so accepting one here would be a setting that silently does
    /// nothing, which is worse than not offering it.
    #[must_use]
    pub const fn supports_level(&self) -> bool {
        matches!(self, Self::Gzip | Self::Zstd)
    }

    /// Inclusive range of levels this codec accepts, or `None` when it takes
    /// no level.
    ///
    /// `Gzip` is zlib's 0–9. `Zstd`'s range comes from the linked libzstd
    /// rather than a hard-coded constant, because it has widened over time:
    /// negative "fast" levels were added in 1.3.4 and the floor has moved
    /// since.
    #[must_use]
    pub fn level_range(&self) -> Option<std::ops::RangeInclusive<i32>> {
        match self {
            Self::Gzip => Some(0..=9),
            #[cfg(feature = "zstd")]
            Self::Zstd => Some(zstd::compression_level_range()),
            #[cfg(not(feature = "zstd"))]
            Self::Zstd => Some(-131_072..=22),
            _ => None,
        }
    }

    /// Compress an arbitrary payload, optionally overriding the codec's
    /// default level.
    ///
    /// `level` is `None` for the codec default — zlib 6 for `Gzip`, 3 for
    /// `Zstd`, matching the Java client. A `Some` value for a codec that takes
    /// no level is a caller bug that configuration validation should already
    /// have rejected; it is ignored here rather than failing a send that is
    /// already on the hot path.
    pub(crate) fn compress_with_level(&self, payload: &[u8], level: Option<i32>) -> Result<Bytes> {
        let _ = level;
        match self {
            Self::None => Ok(Bytes::copy_from_slice(payload)),
            Self::Gzip => {
                use flate2::write::GzEncoder;
                use std::io::Write;

                let flate_level = match level {
                    // Clamped rather than rejected: validation has already
                    // bounded this, and clamping keeps a hot-path send from
                    // failing on an out-of-range value that slipped through.
                    Some(l) => flate2::Compression::new(l.clamp(0, 9) as u32),
                    None => flate2::Compression::default(),
                };
                let mut encoder = GzEncoder::new(Vec::new(), flate_level);
                encoder
                    .write_all(payload)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                let compressed = encoder
                    .finish()
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                Ok(Bytes::from(compressed))
            }
            // snappy-java's stream format, as the Java client writes it.
            Self::Snappy => compress_snappy_xerial(payload),
            Self::Lz4 => {
                use std::io::Write;

                // Kafka RecordBatch v2 requires LZ4 **Frame Format** (magic
                // 0x184D2204), which is what `lz4_flex::frame::FrameEncoder`
                // produces. Do NOT switch to block-level encoding
                // (`lz4_flex::block`); that would produce an incompatible wire
                // format and cause decoding failures on any Kafka broker or
                // Java client.
                let mut compressed = Vec::new();
                let mut encoder = lz4_flex::frame::FrameEncoder::new(&mut compressed);
                encoder
                    .write_all(payload)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                encoder
                    .finish()
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                Ok(Bytes::from(compressed))
            }
            #[cfg(feature = "zstd")]
            Self::Zstd => {
                // 3 is libzstd's default and the Java client's.
                let compressed = zstd::encode_all(payload, level.unwrap_or(3))
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                Ok(Bytes::from(compressed))
            }
            #[cfg(not(feature = "zstd"))]
            Self::Zstd => Err(KrafkaError::compression(
                "zstd compression requires the `zstd` Cargo feature",
            )),
        }
    }
}

/// Timestamp type.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum TimestampType {
    /// Create time.
    #[default]
    CreateTime = 0,
    /// Log append time.
    LogAppendTime = 1,
}

impl TimestampType {
    /// Create from attributes byte.
    #[inline]
    pub fn from_attributes(attributes: i16) -> Self {
        if attributes & 0x08 != 0 {
            Self::LogAppendTime
        } else {
            Self::CreateTime
        }
    }
}

/// A Kafka record header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordHeader {
    /// Header key — raw bytes, not necessarily UTF-8.
    ///
    /// Kafka does not mandate UTF-8 for header keys at the wire level.
    /// Storing as `Bytes` avoids an unnecessary UTF-8 validation on every
    /// fetch response. Use [`key_str()`](Self::key_str) when you need a `&str`.
    pub key: Bytes,
    /// Header value.
    pub value: Option<Bytes>,
}

impl RecordHeader {
    /// Create a new record header with a present (possibly zero-length) value.
    pub fn new(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
        }
    }

    /// Create a header whose value is **null**.
    ///
    /// The wire format distinguishes a null header value (a `-1` length
    /// prefix) from a zero-length one, so this is not the same as
    /// `RecordHeader::new(key, Bytes::new())`.
    pub fn null(key: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: None,
        }
    }

    /// Return the key as a `&str` if it is valid UTF-8.
    #[inline]
    pub fn key_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.key).ok()
    }

    /// Encode the header.
    #[inline]
    pub fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        let key_len = i32::try_from(self.key.len()).map_err(|_| {
            KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                "record header key too large for i32 length",
            )
        })?;
        varint::encode_signed_varint(key_len, buf);
        buf.put_slice(&self.key);
        match &self.value {
            Some(v) => {
                let val_len = i32::try_from(v.len()).map_err(|_| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::InvalidLength,
                        "record header value too large for i32 length",
                    )
                })?;
                varint::encode_signed_varint(val_len, buf);
                buf.put_slice(v);
            }
            None => varint::encode_signed_varint(-1, buf),
        }
        Ok(())
    }

    /// Decode a header.
    #[inline]
    pub fn decode(buf: &mut impl Buf) -> Result<Self> {
        let key_len = varint::decode_signed_varint(buf)?;
        if key_len < 0 || buf.remaining() < key_len as usize {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidValue,
                "invalid header key length",
            ));
        }
        let key = buf.copy_to_bytes(key_len as usize);

        let value_len = varint::decode_signed_varint(buf)?;
        let value = if value_len < 0 {
            None
        } else {
            if buf.remaining() < value_len as usize {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::InvalidValue,
                    "invalid header value length",
                ));
            }
            Some(buf.copy_to_bytes(value_len as usize))
        };

        Ok(Self { key, value })
    }
}

/// A Kafka record within a batch.
#[must_use = "contains record key, value and headers"]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Record attributes (currently unused in v2).
    pub attributes: i8,
    /// Timestamp delta from batch base timestamp.
    pub timestamp_delta: i64,
    /// Offset delta from batch base offset.
    pub offset_delta: i32,
    /// Record key.
    pub key: Option<Bytes>,
    /// Record value.
    pub value: Option<Bytes>,
    /// Record headers.
    pub headers: Vec<RecordHeader>,
}

impl Record {
    /// Create a new record with key and value.
    pub fn new(key: Option<Bytes>, value: Option<Bytes>) -> Self {
        Self {
            attributes: 0,
            timestamp_delta: 0,
            offset_delta: 0,
            key,
            value,
            headers: Vec::new(),
        }
    }

    /// Add a header to the record.
    pub fn with_header(mut self, key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        self.headers.push(RecordHeader::new(key, value));
        self
    }

    /// Set timestamp delta.
    pub fn with_timestamp_delta(mut self, delta: i64) -> Self {
        self.timestamp_delta = delta;
        self
    }

    /// Set offset delta.
    pub fn with_offset_delta(mut self, delta: i32) -> Self {
        self.offset_delta = delta;
        self
    }

    /// Encode the record to a buffer.
    ///
    /// Pre-computes the body size analytically so no intermediate allocation
    /// is needed — the length varint is written first, then the body is
    /// encoded directly into the output buffer.
    #[inline]
    pub fn encode(&self, buf: &mut impl BufMut) -> Result<()> {
        let body_size = self.record_body_size()?;
        let record_len = i32::try_from(body_size).map_err(|_| {
            KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                "record too large for i32 length prefix",
            )
        })?;
        varint::encode_signed_varint(record_len, buf);
        self.encode_body(buf)?;
        Ok(())
    }

    /// Compute the wire-encoded size of the record body (everything after the
    /// length prefix). Used by [`encode`](Self::encode) to avoid a temporary
    /// allocation.
    #[inline]
    pub fn record_body_size(&self) -> Result<usize> {
        let mut size: usize = 0;
        // attributes: i8
        size += 1;
        // timestamp_delta: signed varlong
        size += varint::signed_varlong_size(self.timestamp_delta);
        // offset_delta: signed varint
        size += varint::signed_varint_size(self.offset_delta);
        // key: length varint + bytes
        match &self.key {
            Some(k) => {
                let key_len = i32::try_from(k.len()).map_err(|_| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::InvalidLength,
                        "record key too large for i32 length",
                    )
                })?;
                size += varint::signed_varint_size(key_len);
                size += k.len();
            }
            None => {
                size += varint::signed_varint_size(-1);
            }
        }
        // value: length varint + bytes
        match &self.value {
            Some(v) => {
                let val_len = i32::try_from(v.len()).map_err(|_| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::InvalidLength,
                        "record value too large for i32 length",
                    )
                })?;
                size += varint::signed_varint_size(val_len);
                size += v.len();
            }
            None => {
                size += varint::signed_varint_size(-1);
            }
        }
        // headers: count varint + each header
        let headers_len = i32::try_from(self.headers.len()).map_err(|_| {
            KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                "record headers count exceeds i32 limit",
            )
        })?;
        size += varint::signed_varint_size(headers_len);
        for header in &self.headers {
            let key_len = i32::try_from(header.key.len()).map_err(|_| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::InvalidLength,
                    "record header key too large for i32 length",
                )
            })?;
            size += varint::signed_varint_size(key_len);
            size += header.key.len();
            match &header.value {
                Some(v) => {
                    let val_len = i32::try_from(v.len()).map_err(|_| {
                        KrafkaError::protocol_kind(
                            ProtocolErrorKind::InvalidLength,
                            "record header value too large for i32 length",
                        )
                    })?;
                    size += varint::signed_varint_size(val_len);
                    size += v.len();
                }
                None => {
                    size += varint::signed_varint_size(-1);
                }
            }
        }
        Ok(size)
    }

    #[inline]
    fn encode_body(&self, buf: &mut impl BufMut) -> Result<()> {
        buf.put_i8(self.attributes);
        varint::encode_signed_varlong(self.timestamp_delta, buf);
        varint::encode_signed_varint(self.offset_delta, buf);

        // Key
        match &self.key {
            Some(k) => {
                let key_len = i32::try_from(k.len()).map_err(|_| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::InvalidLength,
                        "record key too large for i32 length",
                    )
                })?;
                varint::encode_signed_varint(key_len, buf);
                buf.put_slice(k);
            }
            None => varint::encode_signed_varint(-1, buf),
        }

        // Value
        match &self.value {
            Some(v) => {
                let val_len = i32::try_from(v.len()).map_err(|_| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::InvalidLength,
                        "record value too large for i32 length",
                    )
                })?;
                varint::encode_signed_varint(val_len, buf);
                buf.put_slice(v);
            }
            None => varint::encode_signed_varint(-1, buf),
        }

        // Headers
        let headers_len = i32::try_from(self.headers.len()).map_err(|_| {
            KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                "record headers count exceeds i32 limit",
            )
        })?;
        varint::encode_signed_varint(headers_len, buf);
        for header in &self.headers {
            header.encode(buf)?;
        }
        Ok(())
    }

    /// Decode a record from a buffer.
    #[inline]
    pub fn decode(buf: &mut impl Buf) -> Result<Self> {
        let length = varint::decode_signed_varint(buf)?;
        if length < 0 {
            return Err(KrafkaError::protocol_kind(
                crate::error::ProtocolErrorKind::InvalidValue,
                format!("invalid record length: {length}"),
            ));
        }
        let length = usize::try_from(length).map_err(|_| {
            KrafkaError::protocol_kind(
                crate::error::ProtocolErrorKind::InvalidLength,
                format!("record length {length} overflows usize on this target"),
            )
        })?;
        if buf.remaining() < length {
            return Err(KrafkaError::protocol_kind(
                crate::error::ProtocolErrorKind::TruncatedFrame,
                format!(
                    "record body truncated: need {length} bytes, have {}",
                    buf.remaining()
                ),
            ));
        }

        // Slice the buffer to exactly `length` bytes so that fields can never
        // read past the declared record boundary into the next record's bytes.
        // This matches the Java client's ByteBuffer.slice() approach and
        // prevents silent data corruption when `length` < actual field payload.
        let mut rbuf = buf.copy_to_bytes(length);

        let attributes = if rbuf.has_remaining() {
            rbuf.get_i8()
        } else {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::Malformed,
                "missing record attributes",
            ));
        };

        let timestamp_delta = varint::decode_signed_varlong(&mut rbuf)?;
        let offset_delta = varint::decode_signed_varint(&mut rbuf)?;

        // Key
        let key_len = varint::decode_signed_varint(&mut rbuf)?;
        let key = if key_len < 0 {
            None
        } else {
            if rbuf.remaining() < key_len as usize {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::InvalidValue,
                    "invalid record key length",
                ));
            }
            Some(rbuf.copy_to_bytes(key_len as usize))
        };

        // Value
        let value_len = varint::decode_signed_varint(&mut rbuf)?;
        let value = if value_len < 0 {
            None
        } else {
            if rbuf.remaining() < value_len as usize {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::InvalidValue,
                    "invalid record value length",
                ));
            }
            Some(rbuf.copy_to_bytes(value_len as usize))
        };

        // Headers
        let header_count = varint::decode_signed_varint(&mut rbuf)?;
        if header_count < 0 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidValue,
                format!("negative header count {header_count} in record"),
            ));
        }
        let header_count = header_count as usize;
        if header_count > super::MAX_DECODE_ARRAY_LEN {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                format!(
                    "header count {header_count} exceeds safety limit {}",
                    super::MAX_DECODE_ARRAY_LEN
                ),
            ));
        }
        // Bound the pre-allocation by the bytes left in *this record*, not by
        // the rest of the batch. `buf` was already advanced past this record,
        // so its `remaining()` describes bytes that can never hold these
        // headers — using it silently widened the clamp that
        // `decode_capacity` exists to apply, letting a batch of tiny records
        // each declaring `MAX_DECODE_ARRAY_LEN` headers allocate megabytes
        // apiece before the first header byte is read.
        let mut headers = Vec::with_capacity(decode_capacity(header_count, rbuf.remaining()));
        for _ in 0..header_count {
            headers.push(RecordHeader::decode(&mut rbuf)?);
        }

        Ok(Self {
            attributes,
            timestamp_delta,
            offset_delta,
            key,
            value,
            headers,
        })
    }
}

/// Record batch attributes.
#[derive(Debug, Clone, Copy, Default)]
pub struct RecordBatchAttributes {
    /// Compression type.
    pub compression: Compression,
    /// Timestamp type.
    pub timestamp_type: TimestampType,
    /// Is transactional.
    pub is_transactional: bool,
    /// Is control batch.
    pub is_control_batch: bool,
}

impl RecordBatchAttributes {
    /// Create from raw attributes value.
    ///
    /// Returns an error if the compression discriminant (bits 0–2) is not a
    /// recognised Kafka codec. This prevents silently decoding compressed
    /// bytes as uncompressed data when a new codec is added to the protocol.
    #[inline]
    pub fn from_i16(value: i16) -> Result<Self> {
        let compression_bits = (value & 0x07) as u8;
        let compression = Compression::from_u8(compression_bits).ok_or_else(|| {
            KrafkaError::protocol_kind(
                crate::error::ProtocolErrorKind::InvalidValue,
                format!("unknown compression codec discriminant: {compression_bits}"),
            )
        })?;
        Ok(Self {
            compression,
            timestamp_type: TimestampType::from_attributes(value),
            is_transactional: value & 0x10 != 0,
            is_control_batch: value & 0x20 != 0,
        })
    }

    /// Convert to raw attributes value.
    #[inline]
    pub fn to_i16(self) -> i16 {
        let mut value = self.compression as i16;
        if matches!(self.timestamp_type, TimestampType::LogAppendTime) {
            value |= 0x08;
        }
        if self.is_transactional {
            value |= 0x10;
        }
        if self.is_control_batch {
            value |= 0x20;
        }
        value
    }
}

/// A Kafka record batch (v2 format).
#[derive(Debug, Clone)]
pub struct RecordBatch {
    /// Base offset.
    pub base_offset: i64,
    /// Partition leader epoch.
    pub partition_leader_epoch: i32,
    /// Magic byte (2 for current format).
    pub magic: i8,
    /// Batch attributes.
    pub attributes: RecordBatchAttributes,
    /// Last offset delta.
    pub last_offset_delta: i32,
    /// Base timestamp.
    pub base_timestamp: i64,
    /// Max timestamp.
    pub max_timestamp: i64,
    /// Producer ID for idempotent/transactional producers.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Base sequence number.
    pub base_sequence: i32,
    /// Records in the batch.
    pub records: Vec<Record>,
    /// Compression level used when encoding, or `None` for the codec default.
    ///
    /// Deliberately not `pub`: this is an encode-time knob, not part of the
    /// decoded wire representation. A batch decoded from the wire carries the
    /// codec but not the level the producer used, because the format does not
    /// record it — making this public would imply it round-trips.
    pub(crate) compression_level: Option<i32>,
}

impl RecordBatch {
    /// Create a new empty record batch.
    pub fn new() -> Self {
        Self {
            base_offset: 0,
            partition_leader_epoch: 0,
            magic: 2,
            attributes: RecordBatchAttributes::default(),
            last_offset_delta: 0,
            base_timestamp: 0,
            max_timestamp: 0,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            records: Vec::new(),
            compression_level: None,
        }
    }

    /// Set the compression type.
    pub fn with_compression(mut self, compression: Compression) -> Self {
        self.attributes.compression = compression;
        self
    }

    /// Add a record to the batch.
    pub fn add_record(&mut self, record: Record) {
        self.records.push(record);
    }

    /// Encode the batch to bytes.
    ///
    /// # Layout
    ///
    /// ```text
    /// [0..8)   base_offset              (i64)
    /// [8..12)  batch_length             (i32)  — total bytes from offset 12 to end
    /// [12..16) partition_leader_epoch   (i32)
    /// [16..17) magic                    (i8)
    /// [17..21) crc                      (u32)  — CRC32C of buf[21..]
    /// [21..)   CRC-covered region:
    ///            attributes             (i16)
    ///            last_offset_delta      (i32)
    ///            base_timestamp         (i64)
    ///            max_timestamp          (i64)
    ///            producer_id            (i64)
    ///            producer_epoch         (i16)
    ///            base_sequence          (i32)
    ///            records_count          (i32)
    ///            records                (variable)
    /// ```
    pub fn encode(&self) -> Result<Bytes> {
        // Fixed header offsets used for in-place patching.
        const BATCH_LENGTH_POS: usize = 8;
        const CRC_POS: usize = 17;
        const CRC_REGION_START: usize = 21;
        // Fixed-field bytes that count toward batch_length (everything after the
        // batch_length field itself, up to but not including the records payload).
        const FIXED_OVERHEAD: usize = 49; // 4+1+4+2+4+8+8+8+2+4+4

        let records_count = i32::try_from(self.records.len()).map_err(|_| {
            KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                "record batch record count exceeds i32 limit",
            )
        })?;

        // Estimated payload size: key + value bytes plus a conservative per-record
        // overhead for varint framing, attributes, and timestamp/offset deltas.
        let estimated_records_size: usize = self
            .records
            .iter()
            .map(|r| {
                r.key.as_ref().map_or(0, Bytes::len) + r.value.as_ref().map_or(0, Bytes::len) + 25
            })
            .sum();

        if matches!(self.attributes.compression, Compression::None) {
            // Fast path: write records directly into the output buffer, eliminating
            // the intermediate `records_buf` and the extra copy used by the general
            // path.  `batch_length` and CRC are unknown until after the records are
            // written, so they are written as zero and patched in place at the end.
            // base_offset(8) + batch_length(4) + FIXED_OVERHEAD(49) = 61 bytes
            const HEADER_SIZE: usize = 12 + FIXED_OVERHEAD;

            let mut buf = BytesMut::with_capacity(HEADER_SIZE + estimated_records_size);

            buf.put_i64(self.base_offset);
            buf.put_i32(0); // batch_length — patched below
            buf.put_i32(self.partition_leader_epoch);
            buf.put_i8(self.magic);
            buf.put_u32(0); // CRC — patched below
            // CRC-covered region starts here (offset 21).
            buf.put_i16(self.attributes.to_i16());
            buf.put_i32(self.last_offset_delta);
            buf.put_i64(self.base_timestamp);
            buf.put_i64(self.max_timestamp);
            buf.put_i64(self.producer_id);
            buf.put_i16(self.producer_epoch);
            buf.put_i32(self.base_sequence);
            buf.put_i32(records_count);

            for record in &self.records {
                record.encode(&mut buf)?;
            }

            // Patch batch_length: every byte after the batch_length field (offset 12).
            let batch_length = i32::try_from(buf.len() - 12).map_err(|_| {
                KrafkaError::protocol_kind(
                    ProtocolErrorKind::InvalidLength,
                    "record batch too large for i32 length prefix",
                )
            })?;
            buf[BATCH_LENGTH_POS..BATCH_LENGTH_POS + 4]
                .copy_from_slice(&batch_length.to_be_bytes());

            // Patch CRC: CRC32C of everything from the attributes field to the end.
            let crc = crc32c(&buf[CRC_REGION_START..]);
            buf[CRC_POS..CRC_POS + 4].copy_from_slice(&crc.to_be_bytes());

            Ok(buf.freeze())
        } else {
            // Compressed path: encode records into a staging buffer, compress, then
            // write the complete batch (header + compressed payload) in one pass.
            let mut records_buf = BytesMut::with_capacity(estimated_records_size);
            for record in &self.records {
                record.encode(&mut records_buf)?;
            }

            let compressed_records = self.compress_records(&records_buf)?;

            let batch_length =
                i32::try_from(FIXED_OVERHEAD + compressed_records.len()).map_err(|_| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::InvalidLength,
                        "record batch too large for i32 length prefix",
                    )
                })?;

            let mut buf = BytesMut::with_capacity(12 + batch_length as usize);

            buf.put_i64(self.base_offset);
            buf.put_i32(batch_length);
            buf.put_i32(self.partition_leader_epoch);
            buf.put_i8(self.magic);
            buf.put_u32(0); // CRC placeholder
            // CRC-covered region starts here (offset 21).
            buf.put_i16(self.attributes.to_i16());
            buf.put_i32(self.last_offset_delta);
            buf.put_i64(self.base_timestamp);
            buf.put_i64(self.max_timestamp);
            buf.put_i64(self.producer_id);
            buf.put_i16(self.producer_epoch);
            buf.put_i32(self.base_sequence);
            buf.put_i32(records_count);
            buf.put_slice(&compressed_records);

            let crc = crc32c(&buf[CRC_REGION_START..]);
            buf[CRC_POS..CRC_POS + 4].copy_from_slice(&crc.to_be_bytes());

            Ok(buf.freeze())
        }
    }

    fn compress_records(&self, records: &[u8]) -> Result<Bytes> {
        self.attributes
            .compression
            .compress_with_level(records, self.compression_level)
    }

    /// Decode a record batch.
    ///
    /// Uses [`MAX_DECOMPRESSED_SIZE`](Self::MAX_DECOMPRESSED_SIZE) as the
    /// decompression limit. For a configurable limit, use
    /// [`decode_with_limit`](Self::decode_with_limit).
    pub fn decode(buf: &mut Bytes) -> Result<Self> {
        Self::decode_with_limit(buf, Self::MAX_DECOMPRESSED_SIZE)
    }

    /// Decode a record batch with a custom decompression size limit.
    ///
    /// The header is parsed first ([`RecordBatchHeader::peek`]), then the CRC
    /// is checked, then the records are decompressed and decoded. On success
    /// `buf` is advanced past the batch; on error it is left untouched.
    ///
    /// Record keys, values and header values are slices of `buf` (or of the
    /// decompressed buffer), not copies: a record that is kept alive keeps the
    /// buffer it came from alive.
    ///
    /// Compressed payloads that decompress beyond `max_decompressed_size` bytes
    /// are rejected as potential compression bombs. The number of records is
    /// bounded by the bytes that hold them, not by a constant.
    pub fn decode_with_limit(buf: &mut Bytes, max_decompressed_size: usize) -> Result<Self> {
        let header = RecordBatchHeader::peek(buf)?;
        let total_size = header.total_size();
        if buf.len() < total_size {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::TruncatedFrame,
                format!(
                    "not enough bytes for record batch: need {total_size}, have {}",
                    buf.len()
                ),
            ));
        }

        // The CRC covers everything after the CRC field, computed over the raw
        // wire bytes so reserved attribute bits are included as written.
        let computed_crc = crc32c(&buf[RecordBatchHeader::CRC_COVERED_START..total_size]);
        if computed_crc != header.crc {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::CrcMismatch,
                format!(
                    "CRC mismatch: expected {:08x}, got {computed_crc:08x}",
                    header.crc
                ),
            ));
        }

        let compressed = buf.slice(RecordBatchHeader::SIZE..total_size);
        let mut records_buf = decompress_records(
            header.attributes.compression,
            &compressed,
            max_decompressed_size,
        )?;

        let records_count = header.records_count as usize;
        if records_count > records_buf.len() / MIN_RECORD_SIZE {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                format!(
                    "record batch declares {records_count} records but holds only {} record bytes \
                     (at least {MIN_RECORD_SIZE} bytes per record)",
                    records_buf.len()
                ),
            ));
        }
        let mut records = Vec::with_capacity(records_count);
        for _ in 0..records_count {
            records.push(Record::decode(&mut records_buf)?);
        }

        buf.advance(total_size);
        Ok(Self {
            base_offset: header.base_offset,
            partition_leader_epoch: header.partition_leader_epoch,
            magic: header.magic,
            attributes: header.attributes,
            last_offset_delta: header.last_offset_delta,
            base_timestamp: header.base_timestamp,
            max_timestamp: header.max_timestamp,
            producer_id: header.producer_id,
            producer_epoch: header.producer_epoch,
            base_sequence: header.base_sequence,
            records,
            compression_level: None,
        })
    }

    /// Maximum decompressed size to protect against compression bombs.
    ///
    /// Set to 128 MiB. Records exceeding this limit after decompression are rejected.
    /// Kafka's `max.message.bytes` defaults to 1 MiB; this is much higher to
    /// accommodate edge cases. The consumer's `max_decompressed_size` setting
    /// overrides it per client.
    pub const MAX_DECOMPRESSED_SIZE: usize = 128 * 1024 * 1024;
}

/// Smallest encoding of one record: length, attributes, timestamp delta, offset
/// delta, key length, value length and header count, one byte each.
const MIN_RECORD_SIZE: usize = 7;

/// The fixed 61-byte header of a v2 record batch.
///
/// [`peek`](Self::peek) parses it without decompressing or copying, so a
/// reader can decide to skip a batch (a control batch, or a batch of an
/// aborted transaction) before paying for its records.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct RecordBatchHeader {
    /// Offset of the first record.
    pub base_offset: i64,
    /// Bytes in the batch after this field.
    pub batch_length: i32,
    /// Partition leader epoch.
    pub partition_leader_epoch: i32,
    /// Magic byte (always 2).
    pub magic: i8,
    /// CRC-32C of everything after the CRC field.
    pub crc: u32,
    /// Batch attributes.
    pub attributes: RecordBatchAttributes,
    /// Offset of the last record relative to `base_offset`.
    pub last_offset_delta: i32,
    /// Base timestamp.
    pub base_timestamp: i64,
    /// Max timestamp.
    pub max_timestamp: i64,
    /// Producer ID.
    pub producer_id: i64,
    /// Producer epoch.
    pub producer_epoch: i16,
    /// Base sequence.
    pub base_sequence: i32,
    /// Number of records the batch declares.
    pub records_count: i32,
}

impl RecordBatchHeader {
    /// Size of the v2 record batch header in bytes.
    pub const SIZE: usize = 61;

    /// Offset of the first byte covered by the CRC (the attributes field).
    const CRC_COVERED_START: usize = 21;

    /// Parse the header at the start of `buf` without consuming it.
    ///
    /// Fails if `buf` is shorter than the header, the batch length is too
    /// small to hold a header, the magic is not 2, the compression codec is
    /// unknown or the record count is negative. Does not check that the whole
    /// batch is present or that its CRC matches; [`RecordBatch::decode`] does.
    pub fn peek(buf: &[u8]) -> Result<Self> {
        if buf.len() < 12 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::TruncatedFrame,
                "not enough bytes for record batch header",
            ));
        }
        let mut b = buf;
        let base_offset = b.get_i64();
        let batch_length = b.get_i32();
        if batch_length < (Self::SIZE - 12) as i32 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidValue,
                format!("invalid record batch length: {batch_length}"),
            ));
        }
        if buf.len() < Self::SIZE {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::TruncatedFrame,
                "not enough bytes for record batch header",
            ));
        }
        let partition_leader_epoch = b.get_i32();
        let magic = b.get_i8();
        if magic != 2 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::UnsupportedMagic,
                format!("unsupported record batch magic: {magic}"),
            ));
        }
        let crc = b.get_u32();
        let attributes = RecordBatchAttributes::from_i16(b.get_i16())?;
        let last_offset_delta = b.get_i32();
        let base_timestamp = b.get_i64();
        let max_timestamp = b.get_i64();
        let producer_id = b.get_i64();
        let producer_epoch = b.get_i16();
        let base_sequence = b.get_i32();
        let records_count = b.get_i32();
        if records_count < 0 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidValue,
                format!("invalid negative records count: {records_count}"),
            ));
        }
        Ok(Self {
            base_offset,
            batch_length,
            partition_leader_epoch,
            magic,
            crc,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records_count,
        })
    }

    /// Size of the whole batch on the wire, header included.
    #[inline]
    #[must_use]
    pub fn total_size(&self) -> usize {
        12 + self.batch_length as usize
    }

    /// Offset of the last record in the batch.
    #[inline]
    #[must_use]
    pub fn last_offset(&self) -> i64 {
        self.base_offset
            .saturating_add(i64::from(self.last_offset_delta))
    }
}

/// The 8-byte magic that starts a snappy-java (xerial) stream.
const XERIAL_MAGIC: [u8; 8] = [0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0];

/// Magic, version and minimum compatible version: the xerial stream header.
const XERIAL_HEADER_LEN: usize = 16;

/// Uncompressed bytes per xerial chunk, snappy-java's default block size.
const XERIAL_BLOCK_SIZE: usize = 32 * 1024;

#[cfg(test)]
thread_local! {
    /// Times `decompress_records` ran a codec on this thread; lets tests prove
    /// a batch was skipped without being decompressed.
    pub(crate) static DECOMPRESSIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Decompress the record section of a batch.
///
/// The uncompressed case shares `data` instead of copying it.
fn decompress_records(
    compression: Compression,
    data: &Bytes,
    max_decompressed_size: usize,
) -> Result<Bytes> {
    #[cfg(test)]
    if compression != Compression::None {
        DECOMPRESSIONS.with(|c| c.set(c.get() + 1));
    }

    let compressed: &[u8] = data.as_ref();
    let result: Vec<u8> = match compression {
        Compression::None => return Ok(data.clone()),
        Compression::Gzip => read_limited(
            flate2::read::GzDecoder::new(compressed),
            compressed.len().saturating_mul(3),
            max_decompressed_size,
        )?,
        Compression::Snappy => decompress_snappy(compressed, max_decompressed_size)?,
        Compression::Lz4 => read_limited(
            lz4_flex::frame::FrameDecoder::new(compressed),
            compressed.len().saturating_mul(4),
            max_decompressed_size,
        )?,
        Compression::Zstd => decompress_zstd(compressed, max_decompressed_size)?,
    };

    if result.len() > max_decompressed_size {
        return Err(KrafkaError::compression(format!(
            "decompressed size {} exceeds maximum {max_decompressed_size} bytes (possible compression bomb)",
            result.len(),
        )));
    }
    Ok(Bytes::from(result))
}

/// Decompress zstd with the pure-Rust decoder. A batch may hold several
/// concatenated frames; all are decoded, bounded by `max` in total.
fn decompress_zstd(data: &[u8], max: usize) -> Result<Vec<u8>> {
    use std::io::Read;
    let mut out = Vec::with_capacity(data.len().saturating_mul(3).min(max));
    let mut input = data;
    while !input.is_empty() && out.len() <= max {
        let decoder = ruzstd::decoding::StreamingDecoder::new(&mut input)
            .map_err(|e| KrafkaError::compression(format!("zstd: {e}")))?;
        let budget = (max as u64 + 1) - out.len() as u64;
        decoder
            .take(budget)
            .read_to_end(&mut out)
            .map_err(|e| KrafkaError::compression(format!("zstd: {e}")))?;
    }
    Ok(out)
}

/// Read a decoder to the end, stopping one byte past `max` so an oversized
/// stream is detected without being materialised.
fn read_limited(decoder: impl std::io::Read, capacity_hint: usize, max: usize) -> Result<Vec<u8>> {
    use std::io::Read;
    let mut out = Vec::with_capacity(capacity_hint.min(max));
    decoder
        .take(max as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|e| KrafkaError::compression(e.to_string()))?;
    Ok(out)
}

/// Decompress snappy as Kafka writes it: xerial-framed (snappy-java's
/// `SnappyOutputStream`, what the Java client and a broker recompressing for
/// `compression.type=snappy` write) or raw.
///
/// The output is bounded by `max` across all chunks before each chunk is
/// allocated.
fn decompress_snappy(data: &[u8], max: usize) -> Result<Vec<u8>> {
    let mut decoder = snap::raw::Decoder::new();
    let mut out = Vec::new();
    if data.len() > XERIAL_HEADER_LEN && data.starts_with(&XERIAL_MAGIC) {
        let mut rest = &data[XERIAL_HEADER_LEN..];
        while !rest.is_empty() {
            let Some((len_bytes, tail)) = rest.split_first_chunk::<4>() else {
                return Err(xerial_error(format!(
                    "truncated chunk length ({} bytes left)",
                    rest.len()
                )));
            };
            let len = u32::from_be_bytes(*len_bytes) as usize;
            if len == 0 {
                return Err(xerial_error("zero-length chunk".to_string()));
            }
            if len > tail.len() {
                return Err(xerial_error(format!(
                    "chunk of {len} bytes but only {} left",
                    tail.len()
                )));
            }
            let (block, next) = tail.split_at(len);
            append_snappy_block(&mut decoder, block, &mut out, max)?;
            rest = next;
        }
    } else {
        append_snappy_block(&mut decoder, data, &mut out, max)?;
    }
    Ok(out)
}

fn xerial_error(detail: String) -> KrafkaError {
    KrafkaError::protocol_kind(
        ProtocolErrorKind::Malformed,
        format!("xerial snappy stream: {detail}"),
    )
}

/// Decompress one raw snappy block onto the end of `out`, refusing before
/// allocation if the block's declared length would take `out` past `max`.
fn append_snappy_block(
    decoder: &mut snap::raw::Decoder,
    block: &[u8],
    out: &mut Vec<u8>,
    max: usize,
) -> Result<()> {
    let declared =
        snap::raw::decompress_len(block).map_err(|e| KrafkaError::compression(e.to_string()))?;
    let start = out.len();
    if declared > max.saturating_sub(start) {
        return Err(KrafkaError::compression(format!(
            "snappy decompressed size {} exceeds maximum {max} bytes (possible compression bomb)",
            start.saturating_add(declared)
        )));
    }
    out.resize(start + declared, 0);
    let written = decoder
        .decompress(block, &mut out[start..])
        .map_err(|e| KrafkaError::compression(e.to_string()))?;
    out.truncate(start + written);
    Ok(())
}

/// Compress with snappy in snappy-java's stream format, as the Java client
/// does: the 16-byte xerial header, then `[u32 BE length][raw block]` per
/// 32 KiB of input.
fn compress_snappy_xerial(payload: &[u8]) -> Result<Bytes> {
    let mut encoder = snap::raw::Encoder::new();
    let blocks = payload.len().div_ceil(XERIAL_BLOCK_SIZE).max(1);
    let mut out = BytesMut::with_capacity(
        XERIAL_HEADER_LEN + snap::raw::max_compress_len(payload.len()) + 4 * blocks,
    );
    out.put_slice(&XERIAL_MAGIC);
    out.put_i32(1); // stream version
    out.put_i32(1); // minimum compatible version
    let mut block = vec![0u8; snap::raw::max_compress_len(payload.len().min(XERIAL_BLOCK_SIZE))];
    let mut put_block = |chunk: &[u8]| -> Result<()> {
        let n = encoder
            .compress(chunk, &mut block)
            .map_err(|e| KrafkaError::compression(e.to_string()))?;
        let len =
            u32::try_from(n).map_err(|_| KrafkaError::compression("snappy block too large"))?;
        out.put_u32(len);
        out.put_slice(&block[..n]);
        Ok(())
    };
    for chunk in payload.chunks(XERIAL_BLOCK_SIZE) {
        put_block(chunk)?;
    }
    if payload.is_empty() {
        put_block(payload)?;
    }
    Ok(out.freeze())
}

impl Default for RecordBatch {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for creating record batches.
#[must_use = "builders do nothing until .build() is called"]
#[derive(Debug, Default)]
pub struct RecordBatchBuilder {
    compression: Compression,
    compression_level: Option<i32>,
    records: Vec<Record>,
    base_timestamp: Option<i64>,
    producer_id: i64,
    producer_epoch: i16,
    base_sequence: i32,
    is_transactional: bool,
}

impl RecordBatchBuilder {
    /// Create a new record batch builder.
    pub fn new() -> Self {
        Self {
            compression: Compression::None,
            compression_level: None,
            records: Vec::new(),
            base_timestamp: None,
            producer_id: -1,
            producer_epoch: -1,
            base_sequence: -1,
            is_transactional: false,
        }
    }

    /// Set the compression type.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = compression;
        self
    }

    /// Override the codec's default compression level.
    ///
    /// `None` uses the codec default: zlib 6 for `Gzip`, 3 for `Zstd` — the
    /// same defaults the Java client uses. Ignored by codecs that take no
    /// level; see [`Compression::supports_level`].
    pub fn compression_level(mut self, level: Option<i32>) -> Self {
        self.compression_level = level;
        self
    }

    /// Set producer information for idempotent/transactional production.
    pub fn producer(mut self, id: i64, epoch: i16, sequence: i32) -> Self {
        self.producer_id = id;
        self.producer_epoch = epoch;
        self.base_sequence = sequence;
        self
    }

    /// Mark this batch as transactional.
    ///
    /// Transactional batches are part of a Kafka transaction and will only
    /// be visible to consumers after the transaction is committed.
    pub fn transactional(mut self, is_transactional: bool) -> Self {
        self.is_transactional = is_transactional;
        self
    }

    /// Set the base timestamp.
    pub fn base_timestamp(mut self, timestamp: i64) -> Self {
        self.base_timestamp = Some(timestamp);
        self
    }

    /// Add a record with key and value.
    pub fn add_record(
        mut self,
        key: Option<impl Into<Bytes>>,
        value: Option<impl Into<Bytes>>,
    ) -> Self {
        debug_assert!(
            self.records.len() < i32::MAX as usize,
            "batch record count would overflow i32"
        );
        let offset_delta = self.records.len() as i32;
        let record =
            Record::new(key.map(Into::into), value.map(Into::into)).with_offset_delta(offset_delta);
        self.records.push(record);
        self
    }

    /// Add a record with headers.
    ///
    /// A `None` `value` is Kafka's null value (a tombstone); a `None` header
    /// value is a null header value. Both are encoded as the `-1` length
    /// sentinel, which the wire format distinguishes from zero-length.
    pub fn add_record_with_headers(
        mut self,
        key: Option<impl Into<Bytes>>,
        value: Option<impl Into<Bytes>>,
        headers: Vec<(impl Into<Bytes>, Option<impl Into<Bytes>>)>,
    ) -> Self {
        debug_assert!(
            self.records.len() < i32::MAX as usize,
            "batch record count would overflow i32"
        );
        let offset_delta = self.records.len() as i32;
        let mut record =
            Record::new(key.map(Into::into), value.map(Into::into)).with_offset_delta(offset_delta);
        for (k, v) in headers {
            record.headers.push(RecordHeader {
                key: k.into(),
                value: v.map(Into::into),
            });
        }
        self.records.push(record);
        self
    }

    /// Build the record batch.
    pub fn build(self) -> RecordBatch {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        let base_timestamp = self.base_timestamp.unwrap_or(now);
        let last_offset_delta = self.records.len().saturating_sub(1) as i32;

        RecordBatch {
            base_offset: 0,
            partition_leader_epoch: 0,
            magic: 2,
            attributes: RecordBatchAttributes {
                compression: self.compression,
                timestamp_type: TimestampType::CreateTime,
                is_transactional: self.is_transactional,
                is_control_batch: false,
            },
            last_offset_delta,
            base_timestamp,
            max_timestamp: base_timestamp,
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            base_sequence: self.base_sequence,
            records: self.records,
            compression_level: self.compression_level,
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {

    /// A payload that is structured enough to compress but varied enough that
    /// match-finding effort actually matters.
    ///
    /// The first attempt at these tests used `(i % 7) as u8`, which zstd
    /// reduces to 24 bytes at *every* level — so the assertion compared 24
    /// against 24 and the test failed for a reason that had nothing to do with
    /// the code. Kafka payloads are records, so the fixture is records.
    fn compressible_payload() -> Vec<u8> {
        let mut out = String::new();
        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        for i in 0..2_000 {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            out.push_str(&format!(
                "{{\"id\":{i},\"user\":\"u{}\",\"ts\":{},\"evt\":\"click\",\"v\":{}}}\n",
                x % 100_000,
                1_700_000_000_000u64 + (x % 1_000_000),
                x % 997
            ));
        }
        out.into_bytes()
    }

    /// A compression level must change the bytes on the wire.
    ///
    /// Comparing the *encoded output* at two levels is the only assertion
    /// that fails if `compress_with_level` stops threading the level through.
    #[cfg(feature = "zstd")]
    #[test]
    fn zstd_compression_level_changes_the_encoded_bytes() {
        let payload = compressible_payload();

        let fast = Compression::Zstd
            .compress_with_level(&payload, Some(1))
            .expect("level 1 must encode");
        let dense = Compression::Zstd
            .compress_with_level(&payload, Some(19))
            .expect("level 19 must encode");
        let default = Compression::Zstd
            .compress_with_level(&payload, None)
            .expect("default must encode");

        // Only the extremes are asserted. zstd's mid levels are not monotonic
        // in output size — on this fixture level 3 is *larger* than level 1,
        // because they use different match-finding strategies rather than the
        // same one turned up. Asserting a monotonic ladder would encode a
        // property zstd does not promise.
        assert!(
            dense.len() < fast.len(),
            "level 19 must compress better than level 1, got {} vs {}",
            dense.len(),
            fast.len()
        );
        assert_eq!(
            default,
            Compression::Zstd
                .compress_with_level(&payload, Some(3))
                .expect("level 3 must encode"),
            "the documented default is 3; if that changes, the docs are wrong"
        );
    }

    #[test]
    fn gzip_compression_level_changes_the_encoded_bytes() {
        let payload = compressible_payload();

        let none = Compression::Gzip
            .compress_with_level(&payload, Some(0))
            .expect("level 0 must encode");
        let best = Compression::Gzip
            .compress_with_level(&payload, Some(9))
            .expect("level 9 must encode");

        assert!(
            best.len() < none.len(),
            "level 9 must compress better than level 0 (store), got {} vs {}",
            best.len(),
            none.len()
        );
        assert_eq!(
            Compression::Gzip
                .compress_with_level(&payload, None)
                .expect("default must encode"),
            Compression::Gzip
                .compress_with_level(&payload, Some(6))
                .expect("level 6 must encode"),
            "zlib's default is 6; if that changes, the docs are wrong"
        );
    }

    /// The level must survive the whole batch-encode path, not just the codec.
    #[cfg(feature = "zstd")]
    #[test]
    fn record_batch_carries_the_compression_level_to_the_wire() {
        let payload = compressible_payload();

        let encode_at = |level: Option<i32>| {
            RecordBatchBuilder::new()
                .compression(Compression::Zstd)
                .compression_level(level)
                .add_record(None::<Bytes>, Some(Bytes::from(payload.clone())))
                .build()
                .encode()
                .expect("batch must encode")
        };

        let fast = encode_at(Some(1));
        let dense = encode_at(Some(19));
        assert!(
            dense.len() < fast.len(),
            "the level must reach the codec through RecordBatch::encode, got {} vs {}",
            dense.len(),
            fast.len()
        );
    }

    /// Codecs that take no level must report so, so validation can reject a
    /// setting that would otherwise be silently ignored.
    #[test]
    fn only_gzip_and_zstd_accept_a_level() {
        assert!(Compression::Gzip.supports_level());
        assert!(Compression::Zstd.supports_level());
        assert!(!Compression::Snappy.supports_level());
        assert!(!Compression::Lz4.supports_level());
        assert!(!Compression::None.supports_level());

        assert_eq!(Compression::Gzip.level_range(), Some(0..=9));
        assert!(Compression::Snappy.level_range().is_none());
    }
    use super::*;

    #[test]
    fn test_record_encode_decode() {
        let record = Record::new(Some(Bytes::from("key")), Some(Bytes::from("value")))
            .with_timestamp_delta(100)
            .with_offset_delta(0)
            .with_header("header1", Bytes::from("value1"));

        let mut buf = BytesMut::new();
        record.encode(&mut buf).unwrap();

        let decoded = Record::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded.key, Some(Bytes::from("key")));
        assert_eq!(decoded.value, Some(Bytes::from("value")));
        assert_eq!(decoded.timestamp_delta, 100);
        assert_eq!(decoded.offset_delta, 0);
        assert_eq!(decoded.headers.len(), 1);
        assert_eq!(decoded.headers[0].key, "header1");
    }

    #[test]
    fn test_record_null_key_value() {
        let record = Record::new(None, Some(Bytes::from("value")));

        let mut buf = BytesMut::new();
        record.encode(&mut buf).unwrap();

        let decoded = Record::decode(&mut buf.freeze()).unwrap();
        assert!(decoded.key.is_none());
        assert_eq!(decoded.value, Some(Bytes::from("value")));
    }

    #[test]
    fn test_record_batch_builder() {
        let batch = RecordBatchBuilder::new()
            .compression(Compression::None)
            .add_record(Some("key1"), Some("value1"))
            .add_record(Some("key2"), Some("value2"))
            .build();

        assert_eq!(batch.records.len(), 2);
        assert_eq!(batch.last_offset_delta, 1);
    }

    #[test]
    fn test_record_batch_encode_decode() {
        let batch = RecordBatchBuilder::new()
            .base_timestamp(1234567890000)
            .add_record(Some("key"), Some("value"))
            .build();

        let encoded = batch.encode().unwrap();
        let decoded = RecordBatch::decode(&mut encoded.clone()).unwrap();

        assert_eq!(decoded.base_offset, 0);
        assert_eq!(decoded.base_timestamp, 1234567890000);
        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].key, Some(Bytes::from("key")));
        assert_eq!(decoded.records[0].value, Some(Bytes::from("value")));
    }

    #[test]
    fn test_record_batch_compression_gzip() {
        let batch = RecordBatchBuilder::new()
            .compression(Compression::Gzip)
            .base_timestamp(1234567890000)
            .add_record(Some("key"), Some("value"))
            .build();

        let encoded = batch.encode().unwrap();
        let decoded = RecordBatch::decode(&mut encoded.clone()).unwrap();

        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].key, Some(Bytes::from("key")));
    }

    #[test]
    fn test_record_batch_compression_snappy() {
        let batch = RecordBatchBuilder::new()
            .compression(Compression::Snappy)
            .base_timestamp(1234567890000)
            .add_record(Some("key"), Some("value"))
            .build();

        let encoded = batch.encode().unwrap();
        let decoded = RecordBatch::decode(&mut encoded.clone()).unwrap();

        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].key, Some(Bytes::from("key")));
    }

    #[test]
    fn test_record_batch_compression_lz4() {
        let batch = RecordBatchBuilder::new()
            .compression(Compression::Lz4)
            .base_timestamp(1234567890000)
            .add_record(Some("key"), Some("value"))
            .build();

        let encoded = batch.encode().unwrap();
        let decoded = RecordBatch::decode(&mut encoded.clone()).unwrap();

        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].key, Some(Bytes::from("key")));
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn test_record_batch_compression_zstd() {
        let batch = RecordBatchBuilder::new()
            .compression(Compression::Zstd)
            .base_timestamp(1234567890000)
            .add_record(Some("key"), Some("value"))
            .build();

        let encoded = batch.encode().unwrap();
        let decoded = RecordBatch::decode(&mut encoded.clone()).unwrap();

        assert_eq!(decoded.records.len(), 1);
        assert_eq!(decoded.records[0].key, Some(Bytes::from("key")));
    }

    #[test]
    fn test_compression_is_available() {
        // None is always available.
        assert!(Compression::None.is_available());

        assert!(Compression::Gzip.is_available());
        assert!(Compression::Snappy.is_available());
        assert!(Compression::Lz4.is_available());
        assert_eq!(Compression::Zstd.is_available(), cfg!(feature = "zstd"));
    }

    #[cfg(not(feature = "zstd"))]
    #[test]
    fn test_zstd_without_its_feature_returns_error() {
        let batch = RecordBatchBuilder::new()
            .compression(Compression::Zstd)
            .add_record(Some("k"), Some("v"))
            .build();
        let msg = batch.encode().unwrap_err().to_string();
        assert!(msg.contains("zstd"), "got: {msg}");
    }

    #[test]
    fn test_compression_roundtrip() {
        #[allow(clippy::single_element_loop)]
        for compression in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            #[cfg(feature = "zstd")]
            Compression::Zstd,
        ] {
            let batch = RecordBatchBuilder::new()
                .compression(compression)
                .base_timestamp(1234567890000)
                .add_record(Some("key1"), Some("value1"))
                .add_record(Some("key2"), Some("value2"))
                .add_record(Some("key3"), Some("value3"))
                .build();

            let encoded = batch.encode().unwrap();
            let decoded = RecordBatch::decode(&mut encoded.clone()).unwrap();

            assert_eq!(
                decoded.records.len(),
                3,
                "Failed for compression {compression:?}"
            );
        }
    }

    #[test]
    fn test_record_batch_attributes() {
        let attrs = RecordBatchAttributes {
            compression: Compression::Lz4,
            timestamp_type: TimestampType::LogAppendTime,
            is_transactional: true,
            is_control_batch: false,
        };

        let raw = attrs.to_i16();
        let decoded = RecordBatchAttributes::from_i16(raw).unwrap();

        assert_eq!(decoded.compression, Compression::Lz4);
        assert_eq!(decoded.timestamp_type, TimestampType::LogAppendTime);
        assert!(decoded.is_transactional);
        assert!(!decoded.is_control_batch);
    }

    #[test]
    fn test_record_batch_attributes_rejects_unknown_compression_discriminant() {
        // Compression discriminant lives in bits 0..=2. Values 5..=7 are unknown.
        let err = RecordBatchAttributes::from_i16(0x0005).unwrap_err();
        match err {
            KrafkaError::Protocol { kind, .. } => {
                assert_eq!(kind, crate::error::ProtocolErrorKind::InvalidValue)
            }
            other => panic!("expected protocol invalid-value error, got: {other}"),
        }
    }

    #[test]
    fn test_decompress_normal_data_within_limit() {
        // A normally compressed batch should be well under the 128 MiB limit
        let batch = RecordBatchBuilder::new()
            .compression(Compression::Gzip)
            .add_record(Some("key"), Some("value"))
            .build();

        let encoded = batch.encode().unwrap();
        let decoded = RecordBatch::decode(&mut encoded.clone()).unwrap();
        assert_eq!(decoded.records.len(), 1);
    }

    #[test]
    fn test_max_decompressed_size_constant() {
        // Verify the constant is 128 MiB
        assert_eq!(RecordBatch::MAX_DECOMPRESSED_SIZE, 128 * 1024 * 1024);
    }

    #[test]
    fn test_snappy_decompression_bomb_rejected() {
        // Craft a snappy frame with a declared uncompressed length exceeding MAX_DECOMPRESSED_SIZE.
        // The snappy format stores the uncompressed length as a varint at the start.
        // We create a minimal frame claiming 256 MiB uncompressed size.
        let huge_size: u64 = 256 * 1024 * 1024;
        // Encode as varint: 256 MiB = 0x10000000
        let mut fake_snappy = Vec::new();
        let mut val = huge_size;
        while val >= 0x80 {
            fake_snappy.push((val as u8) | 0x80);
            val >>= 7;
        }
        fake_snappy.push(val as u8);
        // Append some garbage bytes (won't be decompressed)
        fake_snappy.extend_from_slice(&[0u8; 16]);

        let result = decompress_records(
            Compression::Snappy,
            &Bytes::from(fake_snappy),
            RecordBatch::MAX_DECOMPRESSED_SIZE,
        );
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("compression bomb") || err_msg.contains("exceeds maximum"),
            "Error should mention size limit: {err_msg}"
        );
    }

    #[test]
    #[cfg(feature = "zstd")]
    fn test_zstd_decompression_uses_streaming_limit() {
        // Verify that zstd uses a streaming decoder with size limit
        // by compressing normal data and ensuring it round-trips correctly
        let batch = RecordBatchBuilder::new()
            .compression(Compression::Zstd)
            .add_record(Some("key"), Some("value"))
            .build();

        let encoded = batch.encode().unwrap();
        let decoded = RecordBatch::decode(&mut encoded.clone()).unwrap();
        assert_eq!(decoded.records.len(), 1);
    }

    #[test]
    fn test_record_batch_builder_transactional_flag() {
        let batch = RecordBatchBuilder::new()
            .transactional(true)
            .add_record(Some("key"), Some("value"))
            .build();

        assert!(batch.attributes.is_transactional);

        // Verify it round-trips through encode/decode
        let encoded = batch.encode().unwrap();
        let decoded = RecordBatch::decode(&mut encoded.clone()).unwrap();
        assert!(decoded.attributes.is_transactional);
    }

    #[test]
    fn test_record_batch_builder_non_transactional_default() {
        let batch = RecordBatchBuilder::new()
            .add_record(Some("key"), Some("value"))
            .build();

        assert!(!batch.attributes.is_transactional);
    }

    #[test]
    fn test_record_batch_builder_producer_identity() {
        let batch = RecordBatchBuilder::new()
            .producer(12345, 7, 42)
            .transactional(true)
            .add_record(Some("key"), Some("value"))
            .build();

        assert_eq!(batch.producer_id, 12345);
        assert_eq!(batch.producer_epoch, 7);
        assert_eq!(batch.base_sequence, 42);
        assert!(batch.attributes.is_transactional);

        // Verify producer identity round-trips
        let encoded = batch.encode().unwrap();
        let decoded = RecordBatch::decode(&mut encoded.clone()).unwrap();
        assert_eq!(decoded.producer_id, 12345);
        assert_eq!(decoded.producer_epoch, 7);
        assert_eq!(decoded.base_sequence, 42);
    }

    #[test]
    fn test_record_batch_attributes_transactional_bit() {
        // Verify the transactional bit (0x10) is correctly set/read
        let attrs = RecordBatchAttributes::from_i16(0x10).unwrap();
        assert!(attrs.is_transactional);
        assert!(!attrs.is_control_batch);

        let raw = attrs.to_i16();
        assert_eq!(raw & 0x10, 0x10);

        // Non-transactional
        let attrs = RecordBatchAttributes::from_i16(0x00).unwrap();
        assert!(!attrs.is_transactional);
    }

    #[test]
    fn test_record_batch_decode_rejects_negative_batch_length() {
        // Negative batch_length (i32 = -1) must not wrap to huge usize
        let mut buf = BytesMut::new();
        buf.put_i64(0); // base_offset
        buf.put_i32(-1); // batch_length — negative!

        let result = RecordBatch::decode(&mut buf.freeze());
        assert!(result.is_err(), "negative batch_length should be rejected");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("invalid record batch length"),
            "error should mention invalid length: {err_msg}"
        );
    }

    #[test]
    fn test_record_batch_decode_rejects_too_small_batch_length() {
        // batch_length < 49 (minimum for fixed fields) should be rejected
        let mut buf = BytesMut::new();
        buf.put_i64(0); // base_offset
        buf.put_i32(10); // batch_length — too small for header

        let result = RecordBatch::decode(&mut buf.freeze());
        assert!(result.is_err(), "batch_length < 49 should be rejected");
    }

    #[test]
    fn test_record_batch_decode_rejects_negative_records_count() {
        // F-54: A negative records_count must not wrap to ~4 billion via `as usize`
        // Build a minimal valid batch but with records_count = -1
        let mut batch = RecordBatch::new();
        batch
            .records
            .push(Record::new(Some(Bytes::from("k")), Some(Bytes::from("v"))));
        let encoded = batch.encode().unwrap();

        // Tamper: overwrite records_count (last i32 before record data) with -1
        let mut tampered = BytesMut::from(encoded.as_ref());
        // records_count is at offset: 8(base_offset) + 4(batch_length) + 4(leader_epoch)
        // + 1(magic) + 4(crc) + 2(attributes) + 4(last_offset_delta)
        // + 8(base_timestamp) + 8(max_timestamp) + 8(producer_id)
        // + 2(producer_epoch) + 4(base_sequence) = 57
        let rc_offset = 57;
        tampered[rc_offset..rc_offset + 4].copy_from_slice(&(-1i32).to_be_bytes());

        // Also fix CRC so we test the records_count check, not CRC mismatch
        // CRC covers bytes from attributes onwards (offset 21 to end)
        let crc_data = &tampered[21..];
        let new_crc = crate::util::crc32c(crc_data);
        tampered[17..21].copy_from_slice(&new_crc.to_be_bytes());

        let result = RecordBatch::decode(&mut tampered.freeze());
        assert!(result.is_err(), "negative records_count should be rejected");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("negative records count"),
            "error should mention negative records count: {err_msg}"
        );
    }

    #[test]
    fn test_kafka_bytes_encode_normal_size() {
        // F-55: Verify KafkaBytes encode works for normal-sized values
        use crate::protocol::primitives::{KafkaBytes, TryEncode};
        let b = KafkaBytes::new(vec![1, 2, 3]);
        let mut buf = BytesMut::new();
        b.try_encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 4 + 3); // 4-byte i32 length + 3 bytes data
    }

    // ── Uncompressed decompress path is zero-copy ──────────────────────

    /// For `Compression::None` the returned `Bytes` must share the caller's
    /// allocation rather than being a fresh copy.
    #[test]
    fn decompress_none_is_zero_copy() {
        let src = Bytes::from(vec![7u8; 4096]);
        let out = decompress_records(Compression::None, &src, RecordBatch::MAX_DECOMPRESSED_SIZE)
            .unwrap();
        assert_eq!(out, src);
        assert_eq!(
            out.as_ptr(),
            src.as_ptr(),
            "uncompressed path must not copy the record payload"
        );
    }

    // ── Snappy as the Java client writes it, zstd without C, byte bounds ──

    /// Re-pack the records of an uncompressed v2 batch under `compression`,
    /// with `records` as the record section (header kept, length and CRC fixed).
    fn repack(uncompressed_batch: &[u8], compression: Compression, records: &[u8]) -> Bytes {
        let mut b = BytesMut::new();
        b.put_slice(&uncompressed_batch[..21]);
        let attrs = i16::from_be_bytes([uncompressed_batch[21], uncompressed_batch[22]]);
        b.put_i16((attrs & !0x07) | compression as i16);
        b.put_slice(&uncompressed_batch[23..RecordBatchHeader::SIZE]);
        b.put_slice(records);
        let batch_length = (b.len() - 12) as i32;
        b[8..12].copy_from_slice(&batch_length.to_be_bytes());
        let crc = crc32c(&b[21..]);
        b[17..21].copy_from_slice(&crc.to_be_bytes());
        b.freeze()
    }

    /// snappy-java's stream format, written independently of the encoder
    /// under test: header, then `[len][raw block]` per 32 KiB.
    fn xerial(raw: &[u8]) -> Vec<u8> {
        let mut out = XERIAL_MAGIC.to_vec();
        out.extend_from_slice(&1i32.to_be_bytes());
        out.extend_from_slice(&1i32.to_be_bytes());
        for chunk in raw.chunks(32 * 1024) {
            let block = snap::raw::Encoder::new().compress_vec(chunk).unwrap();
            out.extend_from_slice(&(block.len() as u32).to_be_bytes());
            out.extend_from_slice(&block);
        }
        out
    }

    fn plain_batch(n: usize, value_len: usize) -> Bytes {
        let mut b = RecordBatchBuilder::new();
        for i in 0..n {
            b = b.add_record(Some(format!("k{i}")), Some(vec![b'v'; value_len]));
        }
        b.build().encode().unwrap()
    }

    fn records_of(batch: &Bytes) -> &[u8] {
        &batch[RecordBatchHeader::SIZE..]
    }

    #[test]
    fn xerial_snappy_spanning_several_chunks_decodes() {
        let plain = plain_batch(200, 1000); // > 32 KiB of records: several chunks
        let framed = xerial(records_of(&plain));
        let mut java = repack(&plain, Compression::Snappy, &framed);
        let decoded = RecordBatch::decode(&mut java).unwrap();
        assert_eq!(decoded.records.len(), 200);
        assert_eq!(decoded.records[199].key, Some(Bytes::from("k199")));
        assert!(java.is_empty(), "the buffer is advanced past the batch");
    }

    #[test]
    fn raw_snappy_still_decodes() {
        let plain = plain_batch(3, 10);
        let raw = snap::raw::Encoder::new()
            .compress_vec(records_of(&plain))
            .unwrap();
        let mut batch = repack(&plain, Compression::Snappy, &raw);
        assert_eq!(RecordBatch::decode(&mut batch).unwrap().records.len(), 3);
    }

    #[test]
    fn snappy_encode_writes_the_xerial_stream_java_writes() {
        let payload = compressible_payload(); // > 32 KiB
        let encoded = Compression::Snappy
            .compress_with_level(&payload, None)
            .unwrap();
        assert_eq!(&encoded[..8], &XERIAL_MAGIC);
        assert_eq!(&encoded[8..16], &[0, 0, 0, 1, 0, 0, 0, 1]);
        // Chunk layout: each `[len][block]` decompresses to at most 32 KiB.
        let mut rest = &encoded[16..];
        let mut sizes = Vec::new();
        while !rest.is_empty() {
            let len = u32::from_be_bytes(rest[..4].try_into().unwrap()) as usize;
            let block = &rest[4..4 + len];
            sizes.push(snap::raw::decompress_len(block).unwrap());
            rest = &rest[4 + len..];
        }
        assert!(sizes.len() > 1);
        assert!(
            sizes[..sizes.len() - 1]
                .iter()
                .all(|&n| n == XERIAL_BLOCK_SIZE)
        );
        assert_eq!(sizes.iter().sum::<usize>(), payload.len());
        assert_eq!(
            decompress_snappy(&encoded, usize::MAX).unwrap(),
            payload,
            "round trip"
        );
    }

    #[test]
    fn xerial_output_is_bounded_across_chunks() {
        // Every chunk is small; only their sum exceeds the limit.
        let framed = xerial(&vec![0u8; 100 * 1024]);
        let err = decompress_snappy(&framed, 64 * 1024)
            .unwrap_err()
            .to_string();
        assert!(err.contains("exceeds maximum"), "{err}");
        assert_eq!(
            decompress_snappy(&framed, 100 * 1024).unwrap().len(),
            100 * 1024
        );
    }

    #[test]
    fn malformed_xerial_streams_fail_with_protocol_errors() {
        let mut header = XERIAL_MAGIC.to_vec();
        header.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 1]);
        let block = snap::raw::Encoder::new().compress_vec(b"hello").unwrap();

        let mut zero_len = header.clone();
        zero_len.extend_from_slice(&0u32.to_be_bytes());
        let mut truncated_len = header.clone();
        truncated_len.extend_from_slice(&[0, 0]);
        let mut overlong = header.clone();
        overlong.extend_from_slice(&((block.len() + 1) as u32).to_be_bytes());
        overlong.extend_from_slice(&block);

        for (name, input) in [
            ("zero-length chunk", zero_len),
            ("truncated chunk length", truncated_len),
            ("chunk longer than the data", overlong),
        ] {
            match decompress_snappy(&input, usize::MAX) {
                Err(KrafkaError::Protocol { .. }) => {}
                other => panic!("{name}: expected a protocol error, got {other:?}"),
            }
        }
    }

    #[test]
    fn a_magic_prefixed_buffer_of_16_bytes_or_less_is_raw_snappy() {
        let mut header = XERIAL_MAGIC.to_vec();
        header.extend_from_slice(&[0, 0, 0, 1, 0, 0, 0, 1]);
        // Read as raw snappy, which it is not: a codec error, not a panic and
        // not a xerial parse.
        assert!(matches!(
            decompress_snappy(&header, usize::MAX),
            Err(KrafkaError::Compression { .. })
        ));
    }

    #[test]
    fn zstd_decodes_in_every_build() {
        let plain = plain_batch(50, 100);
        let one = ruzstd::encoding::compress_to_vec(
            records_of(&plain),
            ruzstd::encoding::CompressionLevel::Fastest,
        );
        let mut batch = repack(&plain, Compression::Zstd, &one);
        assert_eq!(RecordBatch::decode(&mut batch).unwrap().records.len(), 50);

        // Two concatenated frames are one record section.
        let records = records_of(&plain);
        let (a, b) = records.split_at(records.len() / 2);
        let mut two =
            ruzstd::encoding::compress_to_vec(a, ruzstd::encoding::CompressionLevel::Fastest);
        two.extend(ruzstd::encoding::compress_to_vec(
            b,
            ruzstd::encoding::CompressionLevel::Fastest,
        ));
        let mut batch = repack(&plain, Compression::Zstd, &two);
        assert_eq!(RecordBatch::decode(&mut batch).unwrap().records.len(), 50);
    }

    #[test]
    fn zstd_decode_is_bounded() {
        let frame = ruzstd::encoding::compress_to_vec(
            &vec![0u8; 1024 * 1024][..],
            ruzstd::encoding::CompressionLevel::Fastest,
        );
        // Decoding stops one byte past the limit instead of inflating it all.
        assert_eq!(
            decompress_zstd(&frame, 64 * 1024).unwrap().len(),
            64 * 1024 + 1
        );
        let err = decompress_records(Compression::Zstd, &Bytes::from(frame), 64 * 1024)
            .unwrap_err()
            .to_string();
        assert!(err.contains("exceeds maximum"), "{err}");
    }

    /// The record count is bounded by the bytes, not by a constant.
    #[test]
    fn a_declared_count_the_bytes_cannot_hold_is_rejected_before_allocating() {
        let plain = plain_batch(3, 1);
        let mut tampered = BytesMut::from(plain.as_ref());
        tampered[57..61].copy_from_slice(&1_000_000i32.to_be_bytes());
        let crc = crc32c(&tampered[21..]);
        tampered[17..21].copy_from_slice(&crc.to_be_bytes());
        let err = RecordBatch::decode(&mut tampered.freeze()).unwrap_err();
        assert!(
            matches!(
                err,
                KrafkaError::Protocol {
                    kind: ProtocolErrorKind::InvalidLength,
                    ..
                }
            ),
            "{err}"
        );
    }

    /// A count the bytes could hold but the records do not fill is truncated.
    #[test]
    fn a_batch_with_fewer_records_than_declared_is_rejected() {
        let plain = plain_batch(3, 20); // ~30 bytes per record
        let mut tampered = BytesMut::from(plain.as_ref());
        tampered[57..61].copy_from_slice(&4i32.to_be_bytes());
        let crc = crc32c(&tampered[21..]);
        tampered[17..21].copy_from_slice(&crc.to_be_bytes());
        assert!(RecordBatch::decode(&mut tampered.freeze()).is_err());
    }

    #[test]
    fn decoded_records_share_the_input_buffer() {
        let encoded = plain_batch(10, 100);
        let base = encoded.as_ptr() as usize;
        let batch = RecordBatch::decode(&mut encoded.clone()).unwrap();
        for record in &batch.records {
            let v = record.value.as_ref().unwrap().as_ptr() as usize;
            assert!(
                v >= base && v < base + encoded.len(),
                "value is a view, not a copy"
            );
        }
    }

    #[test]
    fn header_peek_reads_the_fixed_fields_without_consuming() {
        let encoded = RecordBatchBuilder::new()
            .producer(42, 3, 7)
            .transactional(true)
            .add_record(Some("k"), Some("v"))
            .add_record(Some("k"), Some("v"))
            .build()
            .encode()
            .unwrap();
        let header = RecordBatchHeader::peek(&encoded).unwrap();
        assert_eq!(header.total_size(), encoded.len());
        assert_eq!(header.producer_id, 42);
        assert!(header.attributes.is_transactional);
        assert_eq!(header.records_count, 2);
        assert_eq!(header.last_offset(), 1);
        assert!(RecordBatchHeader::peek(&encoded[..60]).is_err());
    }

    #[test]
    fn a_failed_decode_leaves_the_buffer_untouched() {
        let encoded = plain_batch(2, 5);
        let mut short = encoded.slice(..encoded.len() - 1);
        let before = short.len();
        assert!(RecordBatch::decode(&mut short).is_err());
        assert_eq!(short.len(), before);
    }
}
