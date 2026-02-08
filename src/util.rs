//! Utility functions for Krafka.\n//!\n//! This module provides low-level utilities used throughout the crate:\n//!\n//! - **Correlation ID generation**: Thread-safe ID generation for request/response matching\n//! - **CRC32C**: Checksum calculation for Kafka record validation\n//! - **Varint encoding**: Variable-length integer encoding for compact protocols

use std::sync::atomic::{AtomicI32, Ordering};

/// Thread-safe correlation ID generator.
pub struct CorrelationIdGenerator {
    counter: AtomicI32,
}

impl CorrelationIdGenerator {
    /// Create a new correlation ID generator.
    pub const fn new() -> Self {
        Self {
            counter: AtomicI32::new(1),
        }
    }

    /// Generate the next correlation ID.
    #[inline]
    pub fn next(&self) -> i32 {
        self.counter.fetch_add(1, Ordering::Relaxed)
    }
}

impl Default for CorrelationIdGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// CRC32C calculation for Kafka records.
#[inline]
pub fn crc32c(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

/// Varint encoding utilities for compact protocol.
pub mod varint {
    use bytes::{Buf, BufMut};

    use crate::error::{KrafkaError, Result};

    /// Encode a signed 32-bit integer as a varint.
    #[inline]
    pub fn encode_signed_varint(value: i32, buf: &mut impl BufMut) {
        let unsigned = ((value << 1) ^ (value >> 31)) as u32;
        encode_unsigned_varint(unsigned, buf);
    }

    /// Encode an unsigned 32-bit integer as a varint.
    #[inline]
    pub fn encode_unsigned_varint(mut value: u32, buf: &mut impl BufMut) {
        while value >= 0x80 {
            buf.put_u8((value as u8) | 0x80);
            value >>= 7;
        }
        buf.put_u8(value as u8);
    }

    /// Encode a signed 64-bit integer as a varlong.
    #[inline]
    pub fn encode_signed_varlong(value: i64, buf: &mut impl BufMut) {
        let unsigned = ((value << 1) ^ (value >> 63)) as u64;
        encode_unsigned_varlong(unsigned, buf);
    }

    /// Encode an unsigned 64-bit integer as a varlong.
    #[inline]
    pub fn encode_unsigned_varlong(mut value: u64, buf: &mut impl BufMut) {
        while value >= 0x80 {
            buf.put_u8((value as u8) | 0x80);
            value >>= 7;
        }
        buf.put_u8(value as u8);
    }

    /// Decode a signed 32-bit varint.
    #[inline]
    pub fn decode_signed_varint(buf: &mut impl Buf) -> Result<i32> {
        let unsigned = decode_unsigned_varint(buf)?;
        Ok(((unsigned >> 1) as i32) ^ -((unsigned & 1) as i32))
    }

    /// Decode an unsigned 32-bit varint.
    #[inline]
    pub fn decode_unsigned_varint(buf: &mut impl Buf) -> Result<u32> {
        let mut result: u32 = 0;
        let mut shift = 0;

        loop {
            if !buf.has_remaining() {
                return Err(KrafkaError::protocol("unexpected end of varint"));
            }

            let byte = buf.get_u8();
            result |= ((byte & 0x7F) as u32) << shift;

            if byte & 0x80 == 0 {
                break;
            }

            shift += 7;
            if shift >= 35 {
                return Err(KrafkaError::protocol("varint too long"));
            }
        }

        Ok(result)
    }

    /// Decode a signed 64-bit varlong.
    #[inline]
    pub fn decode_signed_varlong(buf: &mut impl Buf) -> Result<i64> {
        let unsigned = decode_unsigned_varlong(buf)?;
        Ok(((unsigned >> 1) as i64) ^ -((unsigned & 1) as i64))
    }

    /// Decode an unsigned 64-bit varlong.
    #[inline]
    pub fn decode_unsigned_varlong(buf: &mut impl Buf) -> Result<u64> {
        let mut result: u64 = 0;
        let mut shift = 0;

        loop {
            if !buf.has_remaining() {
                return Err(KrafkaError::protocol("unexpected end of varlong"));
            }

            let byte = buf.get_u8();
            result |= ((byte & 0x7F) as u64) << shift;

            if byte & 0x80 == 0 {
                break;
            }

            shift += 7;
            if shift >= 70 {
                return Err(KrafkaError::protocol("varlong too long"));
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;

    use super::*;

    #[test]
    fn test_correlation_id_generator() {
        let generator = CorrelationIdGenerator::new();
        assert_eq!(generator.next(), 1);
        assert_eq!(generator.next(), 2);
        assert_eq!(generator.next(), 3);
    }

    #[test]
    fn test_varint_encode_decode() {
        let test_values = [0, 1, 127, 128, 255, 300, 16383, 16384, i32::MAX, i32::MIN];

        for value in test_values {
            let mut buf = BytesMut::new();
            varint::encode_signed_varint(value, &mut buf);
            let decoded = varint::decode_signed_varint(&mut buf.freeze()).unwrap();
            assert_eq!(decoded, value, "Failed for value {value}");
        }
    }

    #[test]
    fn test_varlong_encode_decode() {
        let test_values = [
            0i64,
            1,
            127,
            128,
            255,
            300,
            16383,
            16384,
            i64::MAX,
            i64::MIN,
        ];

        for value in test_values {
            let mut buf = BytesMut::new();
            varint::encode_signed_varlong(value, &mut buf);
            let decoded = varint::decode_signed_varlong(&mut buf.freeze()).unwrap();
            assert_eq!(decoded, value, "Failed for value {value}");
        }
    }

    #[test]
    fn test_crc32c() {
        let data = b"hello world";
        let crc = crc32c(data);
        assert_eq!(crc, 0xc99465aa);
    }
}
