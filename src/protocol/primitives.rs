//! Kafka protocol primitive types.
//!
//! This module provides encoding and decoding for Kafka protocol primitive types:
//! - Integers (i8, i16, i32, i64)
//! - Unsigned integers (u32 for varints)
//! - Strings (nullable and non-nullable)
//! - Bytes (nullable and non-nullable)
//! - Arrays (nullable and non-nullable)
//! - Compact variants (varint-encoded lengths)

use bytes::{Buf, BufMut, Bytes};

use crate::error::{KrafkaError, Result};
use crate::util::varint;

/// Trait for encoding values to the Kafka wire format.
pub trait Encode {
    /// Encode this value to the buffer.
    fn encode(&self, buf: &mut impl BufMut);

    /// Encode this value using the compact format.
    fn encode_compact(&self, buf: &mut impl BufMut) {
        self.encode(buf);
    }
}

/// Trait for decoding values from the Kafka wire format.
pub trait Decode: Sized {
    /// Decode a value from the buffer.
    fn decode(buf: &mut impl Buf) -> Result<Self>;

    /// Decode a value using the compact format.
    fn decode_compact(buf: &mut impl Buf) -> Result<Self> {
        Self::decode(buf)
    }
}

// Primitive integer implementations

impl Encode for i8 {
    #[inline]
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_i8(*self);
    }
}

impl Decode for i8 {
    #[inline]
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        if buf.remaining() < 1 {
            return Err(KrafkaError::protocol("not enough bytes for i8"));
        }
        Ok(buf.get_i8())
    }
}

impl Encode for i16 {
    #[inline]
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_i16(*self);
    }
}

impl Decode for i16 {
    #[inline]
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        if buf.remaining() < 2 {
            return Err(KrafkaError::protocol("not enough bytes for i16"));
        }
        Ok(buf.get_i16())
    }
}

impl Encode for i32 {
    #[inline]
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_i32(*self);
    }

    #[inline]
    fn encode_compact(&self, buf: &mut impl BufMut) {
        varint::encode_signed_varint(*self, buf);
    }
}

impl Decode for i32 {
    #[inline]
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        if buf.remaining() < 4 {
            return Err(KrafkaError::protocol("not enough bytes for i32"));
        }
        Ok(buf.get_i32())
    }

    #[inline]
    fn decode_compact(buf: &mut impl Buf) -> Result<Self> {
        varint::decode_signed_varint(buf)
    }
}

impl Encode for u32 {
    #[inline]
    fn encode(&self, buf: &mut impl BufMut) {
        varint::encode_unsigned_varint(*self, buf);
    }
}

impl Decode for u32 {
    #[inline]
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        varint::decode_unsigned_varint(buf)
    }
}

impl Encode for i64 {
    #[inline]
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_i64(*self);
    }

    #[inline]
    fn encode_compact(&self, buf: &mut impl BufMut) {
        varint::encode_signed_varlong(*self, buf);
    }
}

impl Decode for i64 {
    #[inline]
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        if buf.remaining() < 8 {
            return Err(KrafkaError::protocol("not enough bytes for i64"));
        }
        Ok(buf.get_i64())
    }

    #[inline]
    fn decode_compact(buf: &mut impl Buf) -> Result<Self> {
        varint::decode_signed_varlong(buf)
    }
}

impl Encode for bool {
    #[inline]
    fn encode(&self, buf: &mut impl BufMut) {
        buf.put_u8(if *self { 1 } else { 0 });
    }
}

impl Decode for bool {
    #[inline]
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        if buf.remaining() < 1 {
            return Err(KrafkaError::protocol("not enough bytes for bool"));
        }
        Ok(buf.get_u8() != 0)
    }
}

// String implementations

/// A Kafka protocol string (length-prefixed).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KafkaString(pub Option<String>);

impl KafkaString {
    /// Create a new non-null string.
    pub fn new(s: impl Into<String>) -> Self {
        Self(Some(s.into()))
    }

    /// Create a null string.
    pub fn null() -> Self {
        Self(None)
    }

    /// Get the string value.
    pub fn as_str(&self) -> Option<&str> {
        self.0.as_deref()
    }

    /// Check if string is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        self.0.is_none()
    }
}

impl From<String> for KafkaString {
    fn from(s: String) -> Self {
        Self(Some(s))
    }
}

impl From<&str> for KafkaString {
    fn from(s: &str) -> Self {
        Self(Some(s.to_string()))
    }
}

impl From<Option<String>> for KafkaString {
    fn from(s: Option<String>) -> Self {
        Self(s)
    }
}

impl Encode for KafkaString {
    fn encode(&self, buf: &mut impl BufMut) {
        match &self.0 {
            None => buf.put_i16(-1),
            Some(s) => {
                let len = i16::try_from(s.len()).unwrap_or_else(|_| {
                    panic!(
                        "KafkaString length {} exceeds protocol limit of {}",
                        s.len(),
                        i16::MAX
                    )
                });
                buf.put_i16(len);
                buf.put_slice(s.as_bytes());
            }
        }
    }

    fn encode_compact(&self, buf: &mut impl BufMut) {
        match &self.0 {
            None => varint::encode_unsigned_varint(0, buf),
            Some(s) => {
                let len_plus_one = u32::try_from(s.len().saturating_add(1)).unwrap_or_else(|_| {
                    panic!("compact KafkaString length {} exceeds u32 limit", s.len())
                });
                varint::encode_unsigned_varint(len_plus_one, buf);
                buf.put_slice(s.as_bytes());
            }
        }
    }
}

impl Decode for KafkaString {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let len = i16::decode(buf)?;
        if len < 0 {
            return Ok(Self(None));
        }

        let len = len as usize;
        if buf.remaining() < len {
            return Err(KrafkaError::protocol("not enough bytes for string"));
        }

        let bytes = buf.copy_to_bytes(len);
        let s = String::from_utf8(bytes.to_vec())
            .map_err(|e| KrafkaError::protocol(format!("invalid UTF-8 string: {}", e)))?;
        Ok(Self(Some(s)))
    }

    fn decode_compact(buf: &mut impl Buf) -> Result<Self> {
        let len = varint::decode_unsigned_varint(buf)?;
        if len == 0 {
            return Ok(Self(None));
        }

        let len = (len - 1) as usize;
        if buf.remaining() < len {
            return Err(KrafkaError::protocol("not enough bytes for compact string"));
        }

        let bytes = buf.copy_to_bytes(len);
        let s = String::from_utf8(bytes.to_vec())
            .map_err(|e| KrafkaError::protocol(format!("invalid UTF-8 string: {}", e)))?;
        Ok(Self(Some(s)))
    }
}

// Bytes implementations

/// Kafka protocol bytes (length-prefixed).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KafkaBytes(pub Option<Bytes>);

impl KafkaBytes {
    /// Create new non-null bytes.
    pub fn new(bytes: impl Into<Bytes>) -> Self {
        Self(Some(bytes.into()))
    }

    /// Create null bytes.
    pub fn null() -> Self {
        Self(None)
    }

    /// Get the bytes value.
    pub fn as_bytes(&self) -> Option<&Bytes> {
        self.0.as_ref()
    }

    /// Check if bytes is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        self.0.is_none()
    }
}

impl From<Bytes> for KafkaBytes {
    fn from(bytes: Bytes) -> Self {
        Self(Some(bytes))
    }
}

impl From<Vec<u8>> for KafkaBytes {
    fn from(bytes: Vec<u8>) -> Self {
        Self(Some(Bytes::from(bytes)))
    }
}

impl From<&[u8]> for KafkaBytes {
    fn from(bytes: &[u8]) -> Self {
        Self(Some(Bytes::copy_from_slice(bytes)))
    }
}

impl Encode for KafkaBytes {
    fn encode(&self, buf: &mut impl BufMut) {
        match &self.0 {
            None => buf.put_i32(-1),
            Some(bytes) => {
                let len = i32::try_from(bytes.len()).unwrap_or_else(|_| {
                    panic!(
                        "KafkaBytes length {} exceeds protocol limit of {}",
                        bytes.len(),
                        i32::MAX
                    )
                });
                buf.put_i32(len);
                buf.put_slice(bytes);
            }
        }
    }

    fn encode_compact(&self, buf: &mut impl BufMut) {
        match &self.0 {
            None => varint::encode_unsigned_varint(0, buf),
            Some(bytes) => {
                let len_plus_one =
                    u32::try_from(bytes.len().saturating_add(1)).unwrap_or_else(|_| {
                        panic!(
                            "compact KafkaBytes length {} exceeds u32 limit",
                            bytes.len()
                        )
                    });
                varint::encode_unsigned_varint(len_plus_one, buf);
                buf.put_slice(bytes);
            }
        }
    }
}

impl Decode for KafkaBytes {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let len = i32::decode(buf)?;
        if len < 0 {
            return Ok(Self(None));
        }

        let len = len as usize;
        if buf.remaining() < len {
            return Err(KrafkaError::protocol("not enough bytes for bytes field"));
        }

        Ok(Self(Some(buf.copy_to_bytes(len))))
    }

    fn decode_compact(buf: &mut impl Buf) -> Result<Self> {
        let len = varint::decode_unsigned_varint(buf)?;
        if len == 0 {
            return Ok(Self(None));
        }

        let len = (len - 1) as usize;
        if buf.remaining() < len {
            return Err(KrafkaError::protocol(
                "not enough bytes for compact bytes field",
            ));
        }

        Ok(Self(Some(buf.copy_to_bytes(len))))
    }
}

// Array implementations

/// Kafka protocol array (length-prefixed).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KafkaArray<T>(pub Option<Vec<T>>);

impl<T> KafkaArray<T> {
    /// Create a new non-null array.
    pub fn new(items: Vec<T>) -> Self {
        Self(Some(items))
    }

    /// Create a null array.
    pub fn null() -> Self {
        Self(None)
    }

    /// Get the array items.
    pub fn items(&self) -> Option<&[T]> {
        self.0.as_deref()
    }

    /// Check if array is null.
    #[inline]
    pub fn is_null(&self) -> bool {
        self.0.is_none()
    }

    /// Get the length of the array (0 if null).
    #[inline]
    pub fn len(&self) -> usize {
        self.0.as_ref().map(|v| v.len()).unwrap_or(0)
    }

    /// Check if array is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<T> From<Vec<T>> for KafkaArray<T> {
    fn from(items: Vec<T>) -> Self {
        Self(Some(items))
    }
}

impl<T: Encode> Encode for KafkaArray<T> {
    fn encode(&self, buf: &mut impl BufMut) {
        match &self.0 {
            None => buf.put_i32(-1),
            Some(items) => {
                let len = i32::try_from(items.len()).unwrap_or_else(|_| {
                    panic!(
                        "KafkaArray length {} exceeds protocol limit of {}",
                        items.len(),
                        i32::MAX
                    )
                });
                buf.put_i32(len);
                for item in items {
                    item.encode(buf);
                }
            }
        }
    }

    fn encode_compact(&self, buf: &mut impl BufMut) {
        match &self.0 {
            None => varint::encode_unsigned_varint(0, buf),
            Some(items) => {
                let len_plus_one =
                    u32::try_from(items.len().saturating_add(1)).unwrap_or_else(|_| {
                        panic!(
                            "compact KafkaArray length {} exceeds u32 limit",
                            items.len()
                        )
                    });
                varint::encode_unsigned_varint(len_plus_one, buf);
                for item in items {
                    item.encode_compact(buf);
                }
            }
        }
    }
}

impl<T: Decode> Decode for KafkaArray<T> {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let len = i32::decode(buf)?;
        if len < 0 {
            return Ok(Self(None));
        }

        let len = len as usize;
        let mut items = Vec::with_capacity(len.min(10_000));
        for _ in 0..len {
            items.push(T::decode(buf)?);
        }
        Ok(Self(Some(items)))
    }

    fn decode_compact(buf: &mut impl Buf) -> Result<Self> {
        let len = varint::decode_unsigned_varint(buf)?;
        if len == 0 {
            return Ok(Self(None));
        }

        let len = (len - 1) as usize;
        let mut items = Vec::with_capacity(len.min(10_000));
        for _ in 0..len {
            items.push(T::decode_compact(buf)?);
        }
        Ok(Self(Some(items)))
    }
}

/// Tagged fields for flexible versions.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TaggedFields(pub Vec<TaggedField>);

/// A single tagged field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaggedField {
    /// The tag ID.
    pub tag: u32,
    /// The field data.
    pub data: Bytes,
}

impl Encode for TaggedFields {
    fn encode(&self, buf: &mut impl BufMut) {
        let count = u32::try_from(self.0.len())
            .unwrap_or_else(|_| panic!("TaggedFields count {} exceeds u32 limit", self.0.len()));
        varint::encode_unsigned_varint(count, buf);
        for field in &self.0 {
            varint::encode_unsigned_varint(field.tag, buf);
            let data_len = u32::try_from(field.data.len()).unwrap_or_else(|_| {
                panic!(
                    "TaggedField data length {} exceeds u32 limit",
                    field.data.len()
                )
            });
            varint::encode_unsigned_varint(data_len, buf);
            buf.put_slice(&field.data);
        }
    }
}

impl Decode for TaggedFields {
    fn decode(buf: &mut impl Buf) -> Result<Self> {
        let count = varint::decode_unsigned_varint(buf)?;
        let mut fields = Vec::with_capacity((count as usize).min(10_000));

        for _ in 0..count {
            let tag = varint::decode_unsigned_varint(buf)?;
            let len = varint::decode_unsigned_varint(buf)? as usize;
            if buf.remaining() < len {
                return Err(KrafkaError::protocol("not enough bytes for tagged field"));
            }
            let data = buf.copy_to_bytes(len);
            fields.push(TaggedField { tag, data });
        }

        Ok(Self(fields))
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn test_i8_encode_decode() {
        let mut buf = BytesMut::new();
        42i8.encode(&mut buf);
        assert_eq!(i8::decode(&mut buf.freeze()).unwrap(), 42);
    }

    #[test]
    fn test_i16_encode_decode() {
        let mut buf = BytesMut::new();
        1234i16.encode(&mut buf);
        assert_eq!(i16::decode(&mut buf.freeze()).unwrap(), 1234);
    }

    #[test]
    fn test_i32_encode_decode() {
        let mut buf = BytesMut::new();
        123456i32.encode(&mut buf);
        assert_eq!(i32::decode(&mut buf.freeze()).unwrap(), 123456);
    }

    #[test]
    fn test_i64_encode_decode() {
        let mut buf = BytesMut::new();
        123456789i64.encode(&mut buf);
        assert_eq!(i64::decode(&mut buf.freeze()).unwrap(), 123456789);
    }

    #[test]
    fn test_bool_encode_decode() {
        let mut buf = BytesMut::new();
        true.encode(&mut buf);
        assert!(bool::decode(&mut buf.freeze()).unwrap());

        let mut buf = BytesMut::new();
        false.encode(&mut buf);
        assert!(!bool::decode(&mut buf.freeze()).unwrap());
    }

    #[test]
    fn test_kafka_string_encode_decode() {
        // Non-null string
        let mut buf = BytesMut::new();
        let s = KafkaString::new("hello");
        s.encode(&mut buf);
        let decoded = KafkaString::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded.as_str(), Some("hello"));

        // Null string
        let mut buf = BytesMut::new();
        let s = KafkaString::null();
        s.encode(&mut buf);
        let decoded = KafkaString::decode(&mut buf.freeze()).unwrap();
        assert!(decoded.is_null());
    }

    #[test]
    fn test_kafka_string_compact_encode_decode() {
        // Non-null string
        let mut buf = BytesMut::new();
        let s = KafkaString::new("hello");
        s.encode_compact(&mut buf);
        let decoded = KafkaString::decode_compact(&mut buf.freeze()).unwrap();
        assert_eq!(decoded.as_str(), Some("hello"));

        // Null string
        let mut buf = BytesMut::new();
        let s = KafkaString::null();
        s.encode_compact(&mut buf);
        let decoded = KafkaString::decode_compact(&mut buf.freeze()).unwrap();
        assert!(decoded.is_null());
    }

    #[test]
    fn test_kafka_bytes_encode_decode() {
        // Non-null bytes
        let mut buf = BytesMut::new();
        let b = KafkaBytes::new(vec![1, 2, 3, 4]);
        b.encode(&mut buf);
        let decoded = KafkaBytes::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded.as_bytes(), Some(&Bytes::from_static(&[1, 2, 3, 4])));

        // Null bytes
        let mut buf = BytesMut::new();
        let b = KafkaBytes::null();
        b.encode(&mut buf);
        let decoded = KafkaBytes::decode(&mut buf.freeze()).unwrap();
        assert!(decoded.is_null());
    }

    #[test]
    fn test_kafka_array_encode_decode() {
        // Non-null array
        let mut buf = BytesMut::new();
        let arr = KafkaArray::new(vec![1i32, 2, 3]);
        arr.encode(&mut buf);
        let decoded = KafkaArray::<i32>::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded.items(), Some([1i32, 2, 3].as_slice()));

        // Null array
        let mut buf = BytesMut::new();
        let arr: KafkaArray<i32> = KafkaArray::null();
        arr.encode(&mut buf);
        let decoded = KafkaArray::<i32>::decode(&mut buf.freeze()).unwrap();
        assert!(decoded.is_null());
    }

    #[test]
    fn test_tagged_fields_encode_decode() {
        let mut buf = BytesMut::new();
        let fields = TaggedFields(vec![
            TaggedField {
                tag: 0,
                data: Bytes::from_static(b"test"),
            },
            TaggedField {
                tag: 1,
                data: Bytes::from_static(b"data"),
            },
        ]);
        fields.encode(&mut buf);
        let decoded = TaggedFields::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded.0.len(), 2);
        assert_eq!(decoded.0[0].tag, 0);
        assert_eq!(decoded.0[0].data.as_ref(), b"test");
    }

    #[test]
    #[should_panic(expected = "KafkaString length")]
    fn test_kafka_string_encode_rejects_oversized() {
        // Strings > i16::MAX bytes must not silently truncate
        let big = "x".repeat(i16::MAX as usize + 1);
        let ks = KafkaString::from(big);
        let mut buf = BytesMut::new();
        ks.encode(&mut buf);
    }

    #[test]
    fn test_kafka_string_encode_max_valid_length() {
        // Strings exactly at i16::MAX should encode fine
        let s = "a".repeat(i16::MAX as usize);
        let ks = KafkaString::from(s.clone());
        let mut buf = BytesMut::new();
        ks.encode(&mut buf);
        // Verify: 2-byte length prefix + string bytes
        assert_eq!(buf.len(), 2 + s.len());
        let decoded = KafkaString::decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded.0.unwrap(), s);
    }
}
