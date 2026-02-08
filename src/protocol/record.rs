//! Kafka record batch implementation.
//!
//! This module implements the Kafka record batch format (v2),
//! which is used for both producing and consuming messages.

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{KrafkaError, Result};
use crate::util::{crc32c, varint};

/// Compression codec.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
    Zstd = 4,
}

impl Compression {
    /// Create from a raw value.
    #[inline]
    pub fn from_u8(value: u8) -> Self {
        match value & 0x07 {
            0 => Self::None,
            1 => Self::Gzip,
            2 => Self::Snappy,
            3 => Self::Lz4,
            4 => Self::Zstd,
            _ => Self::None,
        }
    }
}

/// Timestamp type.
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
    /// Header key.
    pub key: String,
    /// Header value.
    pub value: Option<Bytes>,
}

impl RecordHeader {
    /// Create a new record header.
    pub fn new(key: impl Into<String>, value: impl Into<Bytes>) -> Self {
        Self {
            key: key.into(),
            value: Some(value.into()),
        }
    }

    /// Encode the header.
    #[inline]
    pub fn encode(&self, buf: &mut impl BufMut) {
        varint::encode_signed_varint(self.key.len() as i32, buf);
        buf.put_slice(self.key.as_bytes());
        match &self.value {
            Some(v) => {
                varint::encode_signed_varint(v.len() as i32, buf);
                buf.put_slice(v);
            }
            None => varint::encode_signed_varint(-1, buf),
        }
    }

    /// Decode a header.
    #[inline]
    pub fn decode(buf: &mut impl Buf) -> Result<Self> {
        let key_len = varint::decode_signed_varint(buf)?;
        if key_len < 0 || buf.remaining() < key_len as usize {
            return Err(KrafkaError::protocol("invalid header key length"));
        }
        let key = String::from_utf8(buf.copy_to_bytes(key_len as usize).to_vec())
            .map_err(|e| KrafkaError::protocol(format!("invalid header key: {}", e)))?;

        let value_len = varint::decode_signed_varint(buf)?;
        let value = if value_len < 0 {
            None
        } else {
            if buf.remaining() < value_len as usize {
                return Err(KrafkaError::protocol("invalid header value length"));
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
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<Bytes>) -> Self {
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
    #[inline]
    pub fn encode(&self, buf: &mut impl BufMut) {
        // First encode to a temporary buffer to get the size
        let mut record_buf = BytesMut::new();
        self.encode_body(&mut record_buf);

        // Write length + body
        varint::encode_signed_varint(record_buf.len() as i32, buf);
        buf.put_slice(&record_buf);
    }

    #[inline]
    fn encode_body(&self, buf: &mut impl BufMut) {
        buf.put_i8(self.attributes);
        varint::encode_signed_varlong(self.timestamp_delta, buf);
        varint::encode_signed_varint(self.offset_delta, buf);

        // Key
        match &self.key {
            Some(k) => {
                varint::encode_signed_varint(k.len() as i32, buf);
                buf.put_slice(k);
            }
            None => varint::encode_signed_varint(-1, buf),
        }

        // Value
        match &self.value {
            Some(v) => {
                varint::encode_signed_varint(v.len() as i32, buf);
                buf.put_slice(v);
            }
            None => varint::encode_signed_varint(-1, buf),
        }

        // Headers
        varint::encode_signed_varint(self.headers.len() as i32, buf);
        for header in &self.headers {
            header.encode(buf);
        }
    }

    /// Decode a record from a buffer.
    #[inline]
    pub fn decode(buf: &mut impl Buf) -> Result<Self> {
        let length = varint::decode_signed_varint(buf)?;
        if length < 0 || buf.remaining() < length as usize {
            return Err(KrafkaError::protocol("invalid record length"));
        }

        let attributes = if buf.has_remaining() {
            buf.get_i8()
        } else {
            return Err(KrafkaError::protocol("missing record attributes"));
        };

        let timestamp_delta = varint::decode_signed_varlong(buf)?;
        let offset_delta = varint::decode_signed_varint(buf)?;

        // Key
        let key_len = varint::decode_signed_varint(buf)?;
        let key = if key_len < 0 {
            None
        } else {
            if buf.remaining() < key_len as usize {
                return Err(KrafkaError::protocol("invalid record key length"));
            }
            Some(buf.copy_to_bytes(key_len as usize))
        };

        // Value
        let value_len = varint::decode_signed_varint(buf)?;
        let value = if value_len < 0 {
            None
        } else {
            if buf.remaining() < value_len as usize {
                return Err(KrafkaError::protocol("invalid record value length"));
            }
            Some(buf.copy_to_bytes(value_len as usize))
        };

        // Headers
        let header_count = varint::decode_signed_varint(buf)?;
        let mut headers = Vec::with_capacity(header_count.max(0) as usize);
        for _ in 0..header_count {
            headers.push(RecordHeader::decode(buf)?);
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
    #[inline]
    pub fn from_i16(value: i16) -> Self {
        Self {
            compression: Compression::from_u8((value & 0x07) as u8),
            timestamp_type: TimestampType::from_attributes(value),
            is_transactional: value & 0x10 != 0,
            is_control_batch: value & 0x20 != 0,
        }
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
            record.encode(&mut records_buf);
        }

        // Compress if needed
        let compressed_records = self.compress_records(&records_buf)?;

        // Calculate batch length (everything after batch_length field)
        // 4 (partition_leader_epoch) + 1 (magic) + 4 (crc) + 2 (attributes) +
        // 4 (last_offset_delta) + 8 (base_timestamp) + 8 (max_timestamp) +
        // 8 (producer_id) + 2 (producer_epoch) + 4 (base_sequence) +
        // 4 (records_count) + records
        let batch_length = 4 + 1 + 4 + 2 + 4 + 8 + 8 + 8 + 2 + 4 + 4 + compressed_records.len();

        // Write header
        buf.put_i64(self.base_offset);
        buf.put_i32(batch_length as i32);
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
        buf.put_i32(self.records.len() as i32);
        buf.put_slice(&compressed_records);

        // Calculate and write CRC
        let crc = crc32c(&buf[crc_start..]);
        buf[crc_pos..crc_pos + 4].copy_from_slice(&crc.to_be_bytes());

        Ok(buf.freeze())
    }

    fn compress_records(&self, records: &[u8]) -> Result<Bytes> {
        match self.attributes.compression {
            Compression::None => Ok(Bytes::copy_from_slice(records)),
            Compression::Gzip => {
                use flate2::write::GzEncoder;
                use std::io::Write;

                let mut encoder = GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder
                    .write_all(records)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                let compressed = encoder
                    .finish()
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                Ok(Bytes::from(compressed))
            }
            Compression::Snappy => {
                let mut encoder = snap::raw::Encoder::new();
                let compressed = encoder
                    .compress_vec(records)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                Ok(Bytes::from(compressed))
            }
            Compression::Lz4 => {
                use std::io::Write;
                let mut compressed = Vec::new();
                let mut encoder = lz4_flex::frame::FrameEncoder::new(&mut compressed);
                encoder
                    .write_all(records)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                encoder
                    .finish()
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                Ok(Bytes::from(compressed))
            }
            Compression::Zstd => {
                let compressed = zstd::encode_all(records, 3)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                Ok(Bytes::from(compressed))
            }
        }
    }

    /// Decode a record batch from bytes.
    pub fn decode(buf: &mut impl Buf) -> Result<Self> {
        if buf.remaining() < 12 {
            return Err(KrafkaError::protocol(
                "not enough bytes for record batch header",
            ));
        }

        let base_offset = buf.get_i64();
        let batch_length = buf.get_i32() as usize;

        if buf.remaining() < batch_length {
            return Err(KrafkaError::protocol("not enough bytes for record batch"));
        }

        let partition_leader_epoch = buf.get_i32();
        let magic = buf.get_i8();

        if magic != 2 {
            return Err(KrafkaError::protocol(format!(
                "unsupported record batch magic: {}",
                magic
            )));
        }

        let crc = buf.get_u32();
        let attributes = RecordBatchAttributes::from_i16(buf.get_i16());
        let last_offset_delta = buf.get_i32();
        let base_timestamp = buf.get_i64();
        let max_timestamp = buf.get_i64();
        let producer_id = buf.get_i64();
        let producer_epoch = buf.get_i16();
        let base_sequence = buf.get_i32();
        let records_count = buf.get_i32();

        // Remaining bytes are the (possibly compressed) records
        let records_len = batch_length - 49; // 49 bytes for fixed fields after batch_length
        if buf.remaining() < records_len {
            return Err(KrafkaError::protocol("not enough bytes for records"));
        }

        let compressed_records = buf.copy_to_bytes(records_len);

        // Verify CRC
        let mut crc_data = BytesMut::new();
        crc_data.put_i16(attributes.to_i16());
        crc_data.put_i32(last_offset_delta);
        crc_data.put_i64(base_timestamp);
        crc_data.put_i64(max_timestamp);
        crc_data.put_i64(producer_id);
        crc_data.put_i16(producer_epoch);
        crc_data.put_i32(base_sequence);
        crc_data.put_i32(records_count);
        crc_data.put_slice(&compressed_records);

        let computed_crc = crc32c(&crc_data);
        if computed_crc != crc {
            return Err(KrafkaError::protocol(format!(
                "CRC mismatch: expected {:08x}, got {:08x}",
                crc, computed_crc
            )));
        }

        // Decompress records
        let decompressed = Self::decompress_records(attributes.compression, &compressed_records)?;
        let mut records_buf = decompressed.as_ref();

        // Decode records
        let mut records = Vec::with_capacity(records_count as usize);
        for _ in 0..records_count {
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

    fn decompress_records(compression: Compression, data: &[u8]) -> Result<Bytes> {
        match compression {
            Compression::None => Ok(Bytes::copy_from_slice(data)),
            Compression::Gzip => {
                use flate2::read::GzDecoder;
                use std::io::Read;

                let mut decoder = GzDecoder::new(data);
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                Ok(Bytes::from(decompressed))
            }
            Compression::Snappy => {
                let mut decoder = snap::raw::Decoder::new();
                let decompressed = decoder
                    .decompress_vec(data)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                Ok(Bytes::from(decompressed))
            }
            Compression::Lz4 => {
                use std::io::Read;
                let mut decoder = lz4_flex::frame::FrameDecoder::new(data);
                let mut decompressed = Vec::new();
                decoder
                    .read_to_end(&mut decompressed)
                    .map_err(|e| KrafkaError::compression(e.to_string()))?;
                Ok(Bytes::from(decompressed))
            }
            Compression::Zstd => {
                let decompressed =
                    zstd::decode_all(data).map_err(|e| KrafkaError::compression(e.to_string()))?;
                Ok(Bytes::from(decompressed))
            }
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
        headers: Vec<(impl Into<String>, impl Into<Bytes>)>,
    ) -> Self {
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
                is_transactional: false,
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
    pub fn decode(buf: &mut impl Buf) -> Result<Self> {
        if buf.remaining() < 12 {
            return Err(KrafkaError::protocol(
                "not enough bytes for record batch header",
            ));
        }

        let base_offset = buf.get_i64();
        let batch_length = buf.get_i32() as usize;

        if buf.remaining() < batch_length {
            return Err(KrafkaError::protocol("not enough bytes for record batch"));
        }

        let partition_leader_epoch = buf.get_i32();
        let magic = buf.get_i8();

        if magic != 2 {
            return Err(KrafkaError::protocol(format!(
                "unsupported record batch magic: {}",
                magic
            )));
        }

        let crc = buf.get_u32();
        let attributes = RecordBatchAttributes::from_i16(buf.get_i16());
        let last_offset_delta = buf.get_i32();
        let base_timestamp = buf.get_i64();
        let max_timestamp = buf.get_i64();
        let producer_id = buf.get_i64();
        let producer_epoch = buf.get_i16();
        let base_sequence = buf.get_i32();
        let records_count = buf.get_i32();

        // Remaining bytes are the (possibly compressed) records
        let records_len = batch_length - 49;
        if buf.remaining() < records_len {
            return Err(KrafkaError::protocol("not enough bytes for records"));
        }

        let compressed_records = buf.copy_to_bytes(records_len);

        // Verify CRC
        let mut crc_data = BytesMut::new();
        crc_data.put_i16(attributes.to_i16());
        crc_data.put_i32(last_offset_delta);
        crc_data.put_i64(base_timestamp);
        crc_data.put_i64(max_timestamp);
        crc_data.put_i64(producer_id);
        crc_data.put_i16(producer_epoch);
        crc_data.put_i32(base_sequence);
        crc_data.put_i32(records_count);
        crc_data.put_slice(&compressed_records);

        let computed_crc = crc32c(&crc_data);
        if computed_crc != crc {
            return Err(KrafkaError::protocol(format!(
                "CRC mismatch: expected {:08x}, got {:08x}",
                crc, computed_crc
            )));
        }

        // Decompress but don't parse records
        let raw_records =
            RecordBatch::decompress_records(attributes.compression, &compressed_records)?;

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
        let mut records = Vec::with_capacity(self.records_count as usize);
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
mod tests {
    use super::*;

    #[test]
    fn test_record_encode_decode() {
        let record = Record::new(Some(Bytes::from("key")), Some(Bytes::from("value")))
            .with_timestamp_delta(100)
            .with_offset_delta(0)
            .with_header("header1", Bytes::from("value1"));

        let mut buf = BytesMut::new();
        record.encode(&mut buf);

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
        record.encode(&mut buf);

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
    fn test_compression_roundtrip() {
        for compression in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
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
        let decoded = RecordBatchAttributes::from_i16(raw);

        assert_eq!(decoded.compression, Compression::Lz4);
        assert_eq!(decoded.timestamp_type, TimestampType::LogAppendTime);
        assert!(decoded.is_transactional);
        assert!(!decoded.is_control_batch);
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
        for compression in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
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
}
