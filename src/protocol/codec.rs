//! Protocol codec for framing Kafka messages.
//!
//! Kafka protocol uses a simple length-prefix framing:
//! - 4-byte big-endian length prefix
//! - followed by the message payload

use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::error::{KrafkaError, Result};

/// Maximum message size (default 100MB, configurable).
pub const MAX_MESSAGE_SIZE: usize = 100 * 1024 * 1024;

/// Encoder for Kafka protocol messages.
#[derive(Debug, Default)]
pub struct Encoder {
    buffer: BytesMut,
}

impl Encoder {
    /// Create a new encoder.
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(1024),
        }
    }

    /// Create a new encoder with a specific capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
        }
    }

    /// Get mutable access to the underlying buffer.
    pub fn buffer_mut(&mut self) -> &mut BytesMut {
        &mut self.buffer
    }

    /// Get read access to the underlying buffer.
    pub fn buffer(&self) -> &BytesMut {
        &self.buffer
    }

    /// Get the current length of the buffer.
    #[inline]
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the buffer is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Reset the buffer for reuse.
    pub fn reset(&mut self) {
        self.buffer.clear();
    }

    /// Start encoding a size-prefixed message.
    /// Returns the position where the size should be written.
    pub fn start_message(&mut self) -> usize {
        let pos = self.buffer.len();
        // Reserve space for the 4-byte size prefix
        self.buffer.put_i32(0);
        pos
    }

    /// Finish encoding a size-prefixed message.
    /// Updates the size at the given position.
    pub fn finish_message(&mut self, size_pos: usize) -> Result<()> {
        let message_size = i32::try_from(self.buffer.len() - size_pos - 4)
            .map_err(|_| KrafkaError::protocol("message size exceeds i32::MAX"))?;
        let size_bytes = message_size.to_be_bytes();
        self.buffer[size_pos..size_pos + 4].copy_from_slice(&size_bytes);
        Ok(())
    }

    /// Take the completed message as Bytes.
    pub fn take(&mut self) -> Bytes {
        self.buffer.split().freeze()
    }
}

/// Decoder for Kafka protocol messages.
#[derive(Debug, Default)]
pub struct Decoder {
    buffer: BytesMut,
    max_size: usize,
}

impl Decoder {
    /// Create a new decoder.
    pub fn new() -> Self {
        Self {
            buffer: BytesMut::with_capacity(8192),
            max_size: MAX_MESSAGE_SIZE,
        }
    }

    /// Create a new decoder with a specific max message size.
    pub fn with_max_size(max_size: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(8192),
            max_size,
        }
    }

    /// Add data to the decoder buffer.
    pub fn extend(&mut self, data: &[u8]) {
        self.buffer.extend_from_slice(data);
    }

    /// Try to decode the next message.
    ///
    /// Returns:
    /// - `Ok(Some(bytes))` if a complete message is available
    /// - `Ok(None)` if more data is needed
    /// - `Err(...)` if the message is invalid
    pub fn decode(&mut self) -> Result<Option<Bytes>> {
        // Need at least 4 bytes for the size
        if self.buffer.len() < 4 {
            return Ok(None);
        }

        // Read the message size (without consuming)
        let size_i32 = i32::from_be_bytes([
            self.buffer[0],
            self.buffer[1],
            self.buffer[2],
            self.buffer[3],
        ]);

        if size_i32 < 0 {
            return Err(KrafkaError::protocol(format!(
                "negative message size: {size_i32}"
            )));
        }

        let size = size_i32 as usize;

        // Validate the size
        if size > self.max_size {
            return Err(KrafkaError::protocol(format!(
                "message size {} exceeds maximum {}",
                size, self.max_size
            )));
        }

        // Check if we have the complete message
        let total_size = 4 + size;
        if self.buffer.len() < total_size {
            return Ok(None);
        }

        // Extract the complete message
        self.buffer.advance(4); // Skip size prefix
        let message = self.buffer.split_to(size).freeze();
        Ok(Some(message))
    }

    /// Get the current buffer length.
    pub fn buffered(&self) -> usize {
        self.buffer.len()
    }

    /// Clear the decoder buffer.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encoder_basic() {
        let mut encoder = Encoder::new();
        let pos = encoder.start_message();
        encoder.buffer_mut().put_slice(b"hello");
        encoder.finish_message(pos).unwrap();

        let bytes = encoder.take();
        assert_eq!(bytes.len(), 9); // 4 bytes size + 5 bytes data

        let size = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(size, 5);
        assert_eq!(&bytes[4..], b"hello");
    }

    #[test]
    fn test_encoder_reset() {
        let mut encoder = Encoder::new();
        encoder.buffer_mut().put_slice(b"test");
        assert!(!encoder.is_empty());
        encoder.reset();
        assert!(encoder.is_empty());
    }

    #[test]
    fn test_decoder_complete_message() {
        let mut decoder = Decoder::new();

        // Create a message with size prefix
        let mut msg = BytesMut::new();
        msg.put_i32(5);
        msg.put_slice(b"hello");

        decoder.extend(&msg);

        let result = decoder.decode().unwrap();
        assert!(result.is_some());
        assert_eq!(result.unwrap().as_ref(), b"hello");
        assert_eq!(decoder.buffered(), 0);
    }

    #[test]
    fn test_decoder_incomplete_header() {
        let mut decoder = Decoder::new();
        decoder.extend(&[0, 0]); // Only 2 bytes

        let result = decoder.decode().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decoder_incomplete_message() {
        let mut decoder = Decoder::new();

        // Create an incomplete message
        let mut msg = BytesMut::new();
        msg.put_i32(10); // Claims 10 bytes
        msg.put_slice(b"hello"); // Only 5 bytes

        decoder.extend(&msg);

        let result = decoder.decode().unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_decoder_multiple_messages() {
        let mut decoder = Decoder::new();

        // Create two messages
        let mut msg = BytesMut::new();
        msg.put_i32(5);
        msg.put_slice(b"hello");
        msg.put_i32(5);
        msg.put_slice(b"world");

        decoder.extend(&msg);

        let result1 = decoder.decode().unwrap();
        assert_eq!(result1.unwrap().as_ref(), b"hello");

        let result2 = decoder.decode().unwrap();
        assert_eq!(result2.unwrap().as_ref(), b"world");

        let result3 = decoder.decode().unwrap();
        assert!(result3.is_none());
    }

    #[test]
    fn test_decoder_message_too_large() {
        let mut decoder = Decoder::with_max_size(100);

        // Create a message claiming to be too large
        let mut msg = BytesMut::new();
        msg.put_i32(1000); // Claims 1000 bytes
        msg.put_slice(b"test");

        decoder.extend(&msg);

        let result = decoder.decode();
        assert!(result.is_err());
    }

    #[test]
    fn test_decoder_streaming() {
        let mut decoder = Decoder::new();

        // Add data in chunks
        let mut msg = BytesMut::new();
        msg.put_i32(10);
        msg.put_slice(b"0123456789");

        // First chunk - size only
        decoder.extend(&msg[..4]);
        assert!(decoder.decode().unwrap().is_none());

        // Second chunk - partial data
        decoder.extend(&msg[4..8]);
        assert!(decoder.decode().unwrap().is_none());

        // Third chunk - complete
        decoder.extend(&msg[8..]);
        let result = decoder.decode().unwrap();
        assert_eq!(result.unwrap().as_ref(), b"0123456789");
    }

    #[test]
    fn test_decoder_negative_size() {
        // F-53: A negative message size should produce a clear error, not wrap to usize::MAX
        let mut decoder = Decoder::new();
        let mut msg = BytesMut::new();
        msg.put_i32(-1); // negative size
        msg.put_slice(b"junk");

        decoder.extend(&msg);

        let result = decoder.decode();
        assert!(result.is_err(), "negative message size should be rejected");
        let err_msg = format!("{}", result.unwrap_err());
        assert!(
            err_msg.contains("negative message size"),
            "error should mention negative size: {err_msg}"
        );
    }
}
