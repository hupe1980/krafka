//! Kafka record batch implementation.
//!
//! This module implements the Kafka record batch format (v2),
//! which is used for both producing and consuming messages.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{KrafkaError, ProtocolErrorKind, Result};
use crate::util::{crc32c, varint};

/// Compression codec.
///
/// All variants are always available because they represent wire-format values
/// (bits 0–2 of the record batch attributes field). The actual
/// compress/decompress implementation for each codec is gated behind
/// its Cargo feature (`gzip`, `snappy`, `lz4`, `zstd`). All four are
/// enabled by the `compression` convenience feature (on by default).
///
/// Use [`Compression::is_available`] to check at runtime whether the
/// underlying codec implementation was compiled in.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(u8)]
pub enum Compression {
    /// No compression.
    #[default]
    None = 0,
    /// Gzip compression.
    ///
    /// Requires the `gzip` Cargo feature for compression/decompression.
    Gzip = 1,
    /// Snappy compression.
    ///
    /// Requires the `snappy` Cargo feature for compression/decompression.
    Snappy = 2,
    /// LZ4 compression.
    ///
    /// Requires the `lz4` Cargo feature for compression/decompression.
    Lz4 = 3,
    /// Zstd compression.
    ///
    /// Requires the `zstd` Cargo feature for compression/decompression.
    /// Note: `zstd` pulls in `zstd-sys` which requires a C toolchain.
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

    /// Returns `true` if the codec's feature was enabled at compile time.
    ///
    /// `Compression::None` is always available. Other codecs require their
    /// corresponding Cargo feature (`gzip`, `snappy`, `lz4`, `zstd`).
    ///
    /// # Examples
    ///
    /// ```
    /// use krafka::protocol::Compression;
    ///
    /// assert!(Compression::None.is_available());
    /// ```
    #[inline]
    #[must_use]
    pub const fn is_available(&self) -> bool {
        match self {
            Self::None => true,
            Self::Gzip => cfg!(feature = "gzip"),
            Self::Snappy => cfg!(feature = "snappy"),
            Self::Lz4 => cfg!(feature = "lz4"),
            Self::Zstd => cfg!(feature = "zstd"),
        }
    }

    /// Returns the Cargo feature name required for this codec, if any.
    ///
    /// Returns `None` for `Compression::None` (always available).
    #[inline]
    #[must_use]
    pub const fn required_feature(&self) -> Option<&'static str> {
        match self {
            Self::None => Option::None,
            Self::Gzip => Option::Some("gzip"),
            Self::Snappy => Option::Some("snappy"),
            Self::Lz4 => Option::Some("lz4"),
            Self::Zstd => Option::Some("zstd"),
        }
    }

    /// Compress an arbitrary payload with this codec.
    pub(crate) fn compress(&self, payload: &[u8]) -> Result<Bytes> {
        match self {
            Self::None => Ok(Bytes::copy_from_slice(payload)),
            #[cfg(feature = "gzip")]
            Self::Gzip => {
                use flate2::write::GzEncoder;
                use std::io::Write;

                let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder
                    .write_all(payload)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                let compressed = encoder
                    .finish()
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                Ok(Bytes::from(compressed))
            }
            #[cfg(not(feature = "gzip"))]
            Self::Gzip => Err(KrafkaError::compression(
                "gzip compression requires the `gzip` Cargo feature",
            )),
            #[cfg(feature = "snappy")]
            Self::Snappy => {
                // Kafka RecordBatch v2 uses *raw* Snappy (RFC-defined stream format),
                // NOT the older "framed Snappy" that Kafka message format v0/v1 used.
                // `snap::raw::Encoder` produces the correct raw format for v2 batches.
                // Do NOT switch to `snap::write::FrameEncoder`, which would produce
                // framed Snappy and cause broker decode failures on v2 batches.
                let mut encoder = snap::raw::Encoder::new();
                let compressed = encoder
                    .compress_vec(payload)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                Ok(Bytes::from(compressed))
            }
            #[cfg(not(feature = "snappy"))]
            Self::Snappy => Err(KrafkaError::compression(
                "snappy compression requires the `snappy` Cargo feature",
            )),
            #[cfg(feature = "lz4")]
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
            #[cfg(not(feature = "lz4"))]
            Self::Lz4 => Err(KrafkaError::compression(
                "lz4 compression requires the `lz4` Cargo feature",
            )),
            #[cfg(feature = "zstd")]
            Self::Zstd => {
                let compressed = zstd::encode_all(payload, 3)
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
    /// Create a new record header.
    pub fn new(key: impl Into<Bytes>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
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
        let mut headers = Vec::with_capacity(header_count);
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
    pub fn encode(&self) -> Result<Bytes> {
        let mut buf = BytesMut::new();

        // First, encode the records
        let mut records_buf = BytesMut::new();
        for record in &self.records {
            record.encode(&mut records_buf)?;
        }

        // Compress if needed
        let compressed_records = self.compress_records(&records_buf)?;

        // Calculate batch length (everything after batch_length field)
        // 4 (partition_leader_epoch) + 1 (magic) + 4 (crc) + 2 (attributes) +
        // 4 (last_offset_delta) + 8 (base_timestamp) + 8 (max_timestamp) +
        // 8 (producer_id) + 2 (producer_epoch) + 4 (base_sequence) +
        // 4 (records_count) + records
        let batch_length =
            i32::try_from(4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + compressed_records.len())
                .map_err(|_| {
                    KrafkaError::protocol_kind(
                        ProtocolErrorKind::InvalidLength,
                        "record batch too large for i32 length prefix",
                    )
                })?;

        // Write header
        buf.put_i64(self.base_offset);
        buf.put_i32(batch_length);
        buf.put_i32(self.partition_leader_epoch);
        buf.put_i8(self.magic);

        // Calculate CRC position
        let crc_pos = buf.len();
        buf.put_u32(0); // Placeholder for CRC

        // Write everything that goes into the CRC calculation
        let crc_start = buf.len();
        buf.put_i16(self.attributes.to_i16());
        buf.put_i32(self.last_offset_delta);
        buf.put_i64(self.base_timestamp);
        buf.put_i64(self.max_timestamp);
        buf.put_i64(self.producer_id);
        buf.put_i16(self.producer_epoch);
        buf.put_i32(self.base_sequence);
        buf.put_i32(i32::try_from(self.records.len()).map_err(|_| {
            KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                "record batch record count exceeds i32 limit",
            )
        })?);
        buf.put_slice(&compressed_records);

        // Calculate and write CRC
        let crc = crc32c(&buf[crc_start..]);
        buf[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());

        Ok(buf.freeze())
    }

    fn compress_records(&self, records: &[u8]) -> Result<Bytes> {
        self.attributes.compression.compress(records)
    }

    /// Decode a record batch from bytes.
    ///
    /// Uses [`MAX_DECOMPRESSED_SIZE`](Self::MAX_DECOMPRESSED_SIZE) as the
    /// decompression limit. For a configurable limit, use
    /// [`decode_with_limit`](Self::decode_with_limit).
    pub fn decode(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_with_limit(buf, Self::MAX_DECOMPRESSED_SIZE)
    }

    /// Decode a record batch from bytes with a custom decompression size limit.
    ///
    /// Compressed payloads that decompress beyond `max_decompressed_size` bytes
    /// are rejected as potential compression bombs.
    pub fn decode_with_limit(buf: &mut impl Buf, max_decompressed_size: usize) -> Result<Self> {
        if buf.remaining() < 12 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::TruncatedFrame,
                "not enough bytes for record batch header",
            ));
        }

        let base_offset = buf.get_i64();
        let batch_length_i32 = buf.get_i32();

        if batch_length_i32 < 49 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidValue,
                format!("invalid record batch length: {batch_length_i32}"),
            ));
        }

        let batch_length = batch_length_i32 as usize;

        if buf.remaining() < batch_length {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::TruncatedFrame,
                "not enough bytes for record batch",
            ));
        }

        let partition_leader_epoch = buf.get_i32();
        let magic = buf.get_i8();

        if magic != 2 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::UnsupportedMagic,
                format!("unsupported record batch magic: {magic}"),
            ));
        }

        let crc = buf.get_u32();

        // Capture the CRC-covered region as raw wire bytes BEFORE decoding
        // individual fields.  The CRC covers everything after the 4-byte CRC
        // field itself; within `batch_length` we have already consumed:
        //   partition_leader_epoch (4) + magic (1) + crc (4) = 9 bytes.
        // So the CRC-covered region is `batch_length - 9` bytes.
        //
        // Computing the CRC over raw bytes (rather than re-encoding decoded
        // fields) is semantically correct: it preserves reserved attribute
        // bits 6–13 that `to_i16()` would silently drop, making the check
        // immune to future broker extensions that set those bits.
        //
        // This also eliminates the per-batch `BytesMut` allocation of the
        // previous implementation.  `buf.remaining() >= batch_length` was
        // verified above; after consuming 9 bytes we have at least
        // `batch_length - 9` bytes remaining.
        let crc_covered_len = batch_length - 9;
        let crc_covered = buf.copy_to_bytes(crc_covered_len);

        let computed_crc = crc32c(&crc_covered);
        if computed_crc != crc {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::CrcMismatch,
                format!("CRC mismatch: expected {crc:08x}, got {computed_crc:08x}"),
            ));
        }

        // Decode the fixed header fields from the CRC-covered slice.
        // We consume `crc_covered` in place — no clone needed.
        let mut cbuf = crc_covered;
        let attributes = RecordBatchAttributes::from_i16(cbuf.get_i16())?;
        let last_offset_delta = cbuf.get_i32();
        let base_timestamp = cbuf.get_i64();
        let max_timestamp = cbuf.get_i64();
        let producer_id = cbuf.get_i64();
        let producer_epoch = cbuf.get_i16();
        let base_sequence = cbuf.get_i32();
        let records_count = cbuf.get_i32();

        if records_count < 0 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidValue,
                format!("invalid negative records count: {records_count}"),
            ));
        }

        // The remaining bytes in `cbuf` are the (possibly compressed) records.
        // records_len = (batch_length - 9) - 40 fixed-field bytes = batch_length - 49.
        let compressed_records = cbuf;

        // Decompress records
        let decompressed = Self::decompress_records(
            attributes.compression,
            &compressed_records,
            max_decompressed_size,
        )?;
        let mut records_buf = decompressed.as_ref();

        // Decode records — records_count already validated as non-negative above;
        // apply upper-bound check before looping.
        let records_len = records_count as usize;
        if records_len > super::MAX_DECODE_ARRAY_LEN {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                format!(
                    "records count {records_len} exceeds safety limit {}",
                    super::MAX_DECODE_ARRAY_LEN
                ),
            ));
        }
        let mut records = Vec::with_capacity(records_len);
        for _ in 0..records_len {
            records.push(Record::decode(&mut records_buf)?);
        }

        Ok(Self {
            base_offset,
            partition_leader_epoch,
            magic,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records,
        })
    }

    /// Maximum decompressed size to protect against compression bombs.
    ///
    /// Set to 128 MiB. Records exceeding this limit after decompression are rejected.
    /// Kafka's `max.message.bytes` defaults to 1 MiB; this is much higher to
    /// accommodate edge cases. Future versions may make this runtime-configurable.
    pub const MAX_DECOMPRESSED_SIZE: usize = 128 * 1024 * 1024;

    fn decompress_records(
        compression: Compression,
        data: &[u8],
        max_decompressed_size: usize,
    ) -> Result<Bytes> {
        #[allow(unused_variables)]
        let result: Vec<u8> = match compression {
            Compression::None => return Ok(Bytes::copy_from_slice(data)),
            #[cfg(feature = "gzip")]
            Compression::Gzip => {
                use flate2::read::GzDecoder;
                use std::io::Read;

                let decoder = GzDecoder::new(data);
                let mut limited = decoder.take(max_decompressed_size as u64 + 1);
                let capacity = data.len().saturating_mul(3).min(max_decompressed_size);
                let mut decompressed = Vec::with_capacity(capacity);
                limited
                    .read_to_end(&mut decompressed)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                decompressed
            }
            #[cfg(not(feature = "gzip"))]
            Compression::Gzip => {
                return Err(KrafkaError::compression(
                    "gzip decompression requires the `gzip` Cargo feature",
                ));
            }
            #[cfg(feature = "snappy")]
            Compression::Snappy => {
                // Pre-check decompressed length from snappy header before allocating.
                // snap::raw::decompress_len reads the varint length prefix without decompressing.
                let declared_len = snap::raw::decompress_len(data)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                if declared_len > max_decompressed_size {
                    return Err(KrafkaError::compression(format!(
                        "snappy declared decompressed size {} exceeds maximum {} bytes (possible compression bomb)",
                        declared_len, max_decompressed_size
                    )));
                }
                let mut decoder = snap::raw::Decoder::new();
                decoder
                    .decompress_vec(data)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?
            }
            #[cfg(not(feature = "snappy"))]
            Compression::Snappy => {
                return Err(KrafkaError::compression(
                    "snappy decompression requires the `snappy` Cargo feature",
                ));
            }
            #[cfg(feature = "lz4")]
            Compression::Lz4 => {
                use std::io::Read;
                let decoder = lz4_flex::frame::FrameDecoder::new(data);
                let mut limited = decoder.take(max_decompressed_size as u64 + 1);
                let capacity = data.len().saturating_mul(4).min(max_decompressed_size);
                let mut decompressed = Vec::with_capacity(capacity);
                limited
                    .read_to_end(&mut decompressed)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                decompressed
            }
            #[cfg(not(feature = "lz4"))]
            Compression::Lz4 => {
                return Err(KrafkaError::compression(
                    "lz4 decompression requires the `lz4` Cargo feature",
                ));
            }
            #[cfg(feature = "zstd")]
            Compression::Zstd => {
                // Use streaming decoder with size limit instead of decode_all
                // to prevent decompression bombs from causing OOM.
                use std::io::Read;
                let decoder = zstd::Decoder::new(data)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                let mut limited = decoder.take(max_decompressed_size as u64 + 1);
                let capacity = data.len().saturating_mul(3).min(max_decompressed_size);
                let mut decompressed = Vec::with_capacity(capacity);
                limited
                    .read_to_end(&mut decompressed)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                decompressed
            }
            #[cfg(not(feature = "zstd"))]
            Compression::Zstd => {
                return Err(KrafkaError::compression(
                    "zstd decompression requires the `zstd` Cargo feature",
                ));
            }
        };

        // When all compression features are disabled, every non-None arm
        // diverges via `return Err(...)`, making this code unreachable.
        #[allow(unreachable_code)]
        {
            if result.len() > max_decompressed_size {
                return Err(KrafkaError::compression(format!(
                    "decompressed size {} exceeds maximum {} bytes (possible compression bomb)",
                    result.len(),
                    max_decompressed_size
                )));
            }

            Ok(Bytes::from(result))
        }
    }
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
    pub fn add_record_with_headers(
        mut self,
        key: Option<impl Into<Bytes>>,
        value: Option<impl Into<Bytes>>,
        headers: Vec<(impl Into<Bytes>, impl Into<Bytes>)>,
    ) -> Self {
        debug_assert!(
            self.records.len() < i32::MAX as usize,
            "batch record count would overflow i32"
        );
        let offset_delta = self.records.len() as i32;
        let mut record =
            Record::new(key.map(Into::into), value.map(Into::into)).with_offset_delta(offset_delta);
        for (k, v) in headers {
            record.headers.push(RecordHeader::new(k, v));
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
        }
    }
}

/// A lazily-decoded record batch for improved performance.
///
/// This struct stores the decompressed record bytes and metadata,
/// deferring individual record parsing until iteration. This is
/// useful when filtering records based on offset before accessing
/// the key/value, avoiding unnecessary deserialization.
///
/// # Example
///
/// ```rust,ignore
/// let lazy = LazyRecordBatch::decode(&mut buf)?;
/// for result in lazy.records() {
///     let record = result?;
///     println!("Key: {:?}", record.key);
/// }
/// ```
#[must_use = "contains lazily-decoded record batch data"]
#[derive(Debug, Clone)]
pub struct LazyRecordBatch {
    /// Base offset.
    pub base_offset: i64,
    /// Partition leader epoch.
    pub partition_leader_epoch: i32,
    /// Batch attributes.
    pub attributes: RecordBatchAttributes,
    /// Last offset delta.
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
    /// Number of records.
    pub records_count: i32,
    /// Raw (decompressed) record bytes.
    raw_records: Bytes,
}

impl LazyRecordBatch {
    /// Decode a lazy record batch from bytes.
    ///
    /// This performs decompression but defers record parsing.
    /// Uses [`RecordBatch::MAX_DECOMPRESSED_SIZE`] as the decompression limit.
    /// For a configurable limit, use [`decode_with_limit`](Self::decode_with_limit).
    pub fn decode(buf: &mut impl Buf) -> Result<Self> {
        Self::decode_with_limit(buf, RecordBatch::MAX_DECOMPRESSED_SIZE)
    }

    /// Decode a lazy record batch with a custom decompression size limit.
    ///
    /// Compressed payloads that decompress beyond `max_decompressed_size` bytes
    /// are rejected as potential compression bombs.
    pub fn decode_with_limit(buf: &mut impl Buf, max_decompressed_size: usize) -> Result<Self> {
        if buf.remaining() < 12 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::TruncatedFrame,
                "not enough bytes for record batch header",
            ));
        }

        let base_offset = buf.get_i64();
        let batch_length_i32 = buf.get_i32();

        if batch_length_i32 < 49 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidValue,
                format!("invalid record batch length: {batch_length_i32}"),
            ));
        }

        let batch_length = batch_length_i32 as usize;

        if buf.remaining() < batch_length {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::TruncatedFrame,
                "not enough bytes for record batch",
            ));
        }

        let partition_leader_epoch = buf.get_i32();
        let magic = buf.get_i8();

        if magic != 2 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::UnsupportedMagic,
                format!("unsupported record batch magic: {magic}"),
            ));
        }

        let crc = buf.get_u32();

        // Same raw-bytes CRC strategy as `RecordBatch::decode_with_limit`:
        // capture the CRC-covered region before decoding fields to avoid
        // re-encoding lossy and to eliminate the per-batch BytesMut allocation.
        let crc_covered_len = batch_length - 9;
        let crc_covered = buf.copy_to_bytes(crc_covered_len);

        let computed_crc = crc32c(&crc_covered);
        if computed_crc != crc {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::CrcMismatch,
                format!("CRC mismatch: expected {crc:08x}, got {computed_crc:08x}"),
            ));
        }

        let mut cbuf = crc_covered;
        let attributes = RecordBatchAttributes::from_i16(cbuf.get_i16())?;
        let last_offset_delta = cbuf.get_i32();
        let base_timestamp = cbuf.get_i64();
        let max_timestamp = cbuf.get_i64();
        let producer_id = cbuf.get_i64();
        let producer_epoch = cbuf.get_i16();
        let base_sequence = cbuf.get_i32();
        let records_count = cbuf.get_i32();

        if records_count < 0 {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidValue,
                format!("invalid negative records count: {records_count}"),
            ));
        }
        if records_count as usize > super::MAX_DECODE_ARRAY_LEN {
            return Err(KrafkaError::protocol_kind(
                ProtocolErrorKind::InvalidLength,
                format!(
                    "records count {} exceeds safety limit {}",
                    records_count,
                    super::MAX_DECODE_ARRAY_LEN
                ),
            ));
        }

        // Remaining bytes in cbuf are the (possibly compressed) records.
        let compressed_records = cbuf;

        // Decompress but don't parse records
        let raw_records = RecordBatch::decompress_records(
            attributes.compression,
            &compressed_records,
            max_decompressed_size,
        )?;

        Ok(Self {
            base_offset,
            partition_leader_epoch,
            attributes,
            last_offset_delta,
            base_timestamp,
            max_timestamp,
            producer_id,
            producer_epoch,
            base_sequence,
            records_count,
            raw_records,
        })
    }

    /// Get the number of records in the batch.
    #[inline]
    pub fn len(&self) -> usize {
        self.records_count as usize
    }

    /// Check if the batch is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.records_count == 0
    }

    /// Iterate over records, decoding each on demand.
    ///
    /// This returns an iterator that yields `Result<Record>` for each record.
    #[inline]
    pub fn records(&self) -> LazyRecordIterator {
        LazyRecordIterator {
            buf: self.raw_records.clone(),
            remaining: self.records_count as usize,
        }
    }

    /// Eagerly decode all records into a Vec.
    ///
    /// This is equivalent to `records().collect()` but with proper error handling.
    pub fn decode_all(&self) -> Result<Vec<Record>> {
        let mut records =
            Vec::with_capacity((self.records_count as usize).min(super::MAX_DECODE_ARRAY_LEN));
        for result in self.records() {
            records.push(result?);
        }
        Ok(records)
    }

    /// Convert to an eager `RecordBatch` by decoding all records.
    pub fn into_record_batch(self) -> Result<RecordBatch> {
        Ok(RecordBatch {
            base_offset: self.base_offset,
            partition_leader_epoch: self.partition_leader_epoch,
            magic: 2,
            attributes: self.attributes,
            last_offset_delta: self.last_offset_delta,
            base_timestamp: self.base_timestamp,
            max_timestamp: self.max_timestamp,
            producer_id: self.producer_id,
            producer_epoch: self.producer_epoch,
            base_sequence: self.base_sequence,
            records: self.decode_all()?,
        })
    }
}

/// Iterator that decodes records on demand from raw bytes.
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct LazyRecordIterator {
    buf: Bytes,
    remaining: usize,
}

impl Iterator for LazyRecordIterator {
    type Item = Result<Record>;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 || self.buf.is_empty() {
            return None;
        }
        self.remaining -= 1;
        Some(Record::decode(&mut self.buf))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

impl ExactSizeIterator for LazyRecordIterator {}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
mod tests {
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
    #[cfg(feature = "gzip")]
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
    #[cfg(feature = "snappy")]
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
    #[cfg(feature = "lz4")]
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

        // The other codecs depend on their features.
        assert_eq!(Compression::Gzip.is_available(), cfg!(feature = "gzip"));
        assert_eq!(Compression::Snappy.is_available(), cfg!(feature = "snappy"));
        assert_eq!(Compression::Lz4.is_available(), cfg!(feature = "lz4"));
        assert_eq!(Compression::Zstd.is_available(), cfg!(feature = "zstd"));
    }

    #[test]
    fn test_compression_required_feature() {
        assert_eq!(Compression::None.required_feature(), None);
        assert_eq!(Compression::Gzip.required_feature(), Some("gzip"));
        assert_eq!(Compression::Snappy.required_feature(), Some("snappy"));
        assert_eq!(Compression::Lz4.required_feature(), Some("lz4"));
        assert_eq!(Compression::Zstd.required_feature(), Some("zstd"));
    }

    #[test]
    fn test_disabled_codec_returns_error() {
        // For every codec whose feature is disabled, verify that encoding
        // produces a descriptive error mentioning the Cargo feature.
        for compression in [
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            if compression.is_available() {
                continue;
            }
            let batch = RecordBatchBuilder::new()
                .compression(compression)
                .add_record(Some("k"), Some("v"))
                .build();
            let err = batch.encode().unwrap_err();
            let msg = err.to_string();
            let feature = compression.required_feature().unwrap();
            assert!(
                msg.contains(feature),
                "error for {compression:?} should mention feature `{feature}`, got: {msg}"
            );
        }
    }

    #[test]
    fn test_compression_roundtrip() {
        #[allow(clippy::single_element_loop)]
        for compression in [
            Compression::None,
            #[cfg(feature = "gzip")]
            Compression::Gzip,
            #[cfg(feature = "snappy")]
            Compression::Snappy,
            #[cfg(feature = "lz4")]
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
    fn test_lazy_record_batch_decode() {
        let batch = RecordBatchBuilder::new()
            .compression(Compression::None)
            .base_timestamp(1234567890000)
            .add_record(Some("key1"), Some("value1"))
            .add_record(Some("key2"), Some("value2"))
            .add_record(Some("key3"), Some("value3"))
            .build();

        let encoded = batch.encode().unwrap();
        let lazy = LazyRecordBatch::decode(&mut encoded.clone()).unwrap();

        assert_eq!(lazy.len(), 3);
        assert!(!lazy.is_empty());
        assert_eq!(lazy.base_timestamp, 1234567890000);

        // Iterate and decode on demand
        let records: Vec<Record> = lazy.records().map(|r| r.unwrap()).collect();
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].key, Some(Bytes::from("key1")));
        assert_eq!(records[1].key, Some(Bytes::from("key2")));
        assert_eq!(records[2].key, Some(Bytes::from("key3")));
    }

    #[test]
    #[cfg(feature = "lz4")]
    fn test_lazy_record_batch_into_eager() {
        let batch = RecordBatchBuilder::new()
            .compression(Compression::Lz4)
            .base_timestamp(1234567890000)
            .add_record(Some("key"), Some("value"))
            .build();

        let encoded = batch.encode().unwrap();
        let lazy = LazyRecordBatch::decode(&mut encoded.clone()).unwrap();
        let eager = lazy.into_record_batch().unwrap();

        assert_eq!(eager.records.len(), 1);
        assert_eq!(eager.records[0].key, Some(Bytes::from("key")));
        assert_eq!(eager.base_timestamp, 1234567890000);
    }

    #[test]
    fn test_lazy_record_batch_with_compression() {
        #[allow(clippy::single_element_loop)]
        for compression in [
            Compression::None,
            #[cfg(feature = "gzip")]
            Compression::Gzip,
            #[cfg(feature = "snappy")]
            Compression::Snappy,
            #[cfg(feature = "lz4")]
            Compression::Lz4,
            #[cfg(feature = "zstd")]
            Compression::Zstd,
        ] {
            let batch = RecordBatchBuilder::new()
                .compression(compression)
                .base_timestamp(1234567890000)
                .add_record(Some("key1"), Some("value1"))
                .add_record(Some("key2"), Some("value2"))
                .build();

            let encoded = batch.encode().unwrap();
            let lazy = LazyRecordBatch::decode(&mut encoded.clone()).unwrap();

            assert_eq!(lazy.len(), 2, "Failed for compression {compression:?}");

            let records: Result<Vec<_>> = lazy.records().collect();
            let records = records.unwrap();
            assert_eq!(records.len(), 2, "Failed for compression {compression:?}");
        }
    }

    #[test]
    #[cfg(feature = "gzip")]
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
    #[cfg(feature = "snappy")]
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

        let result = RecordBatch::decompress_records(
            Compression::Snappy,
            &fake_snappy,
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
    fn test_lazy_record_batch_decode_rejects_negative_batch_length() {
        let mut buf = BytesMut::new();
        buf.put_i64(0); // base_offset
        buf.put_i32(-100); // batch_length — negative!

        let result = LazyRecordBatch::decode(&mut buf.freeze());
        assert!(result.is_err(), "negative batch_length should be rejected");
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
    fn test_lazy_record_batch_decode_rejects_negative_records_count() {
        // Same F-54 test for LazyRecordBatch
        let mut batch = RecordBatch::new();
        batch
            .records
            .push(Record::new(Some(Bytes::from("k")), Some(Bytes::from("v"))));
        let encoded = batch.encode().unwrap();

        let mut tampered = BytesMut::from(encoded.as_ref());
        let rc_offset = 57;
        tampered[rc_offset..rc_offset + 4].copy_from_slice(&(-1i32).to_be_bytes());

        // Fix CRC
        let crc_data = &tampered[21..];
        let new_crc = crate::util::crc32c(crc_data);
        tampered[17..21].copy_from_slice(&new_crc.to_be_bytes());

        let result = LazyRecordBatch::decode(&mut tampered.freeze());
        assert!(result.is_err(), "negative records_count should be rejected");
    }

    #[test]
    fn test_kafka_bytes_encode_normal_size() {
        // F-55: Verify KafkaBytes encode works for normal-sized values
        // (the overflow guard panics for >i32::MAX, which can't be tested due to memory limits)
        use crate::protocol::primitives::{Encode, KafkaBytes};
        let b = KafkaBytes::new(vec![1, 2, 3]);
        let mut buf = BytesMut::new();
        b.encode(&mut buf);
        assert_eq!(buf.len(), 4 + 3); // 4-byte i32 length + 3 bytes data
    }
}
