//! Utility functions for Krafka.\n//!\n//! This module provides low-level utilities used throughout the crate:\n//!\n//! - **Correlation ID generation**: Thread-safe ID generation for request/response matching\n//! - **CRC32C**: Checksum calculation for Kafka record validation\n//! - **Varint encoding**: Variable-length integer encoding for compact protocols\n//! - **SNI hostname extraction**: Parse hostnames from address strings for TLS SNI

use std::sync::atomic::{AtomicI32, Ordering};
use std::time::Duration;

use crate::error::{KrafkaError, Result};

/// Convert a `Duration` to milliseconds as `i32`, capping at `i32::MAX`.
///
/// `Duration::as_millis()` returns `u128`, which would silently truncate
/// when cast to `i32`. This function caps the value at `i32::MAX` (~24.8 days)
/// to prevent silent wraparound.
#[inline]
pub fn duration_to_millis_i32(d: Duration) -> i32 {
    d.as_millis().min(i32::MAX as u128) as i32
}

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

    #[test]
    fn test_duration_to_millis_i32_normal() {
        assert_eq!(duration_to_millis_i32(Duration::from_millis(100)), 100);
        assert_eq!(duration_to_millis_i32(Duration::from_secs(30)), 30_000);
        assert_eq!(duration_to_millis_i32(Duration::ZERO), 0);
    }

    #[test]
    fn test_duration_to_millis_i32_caps_at_max() {
        // 25 days in millis exceeds i32::MAX (~24.8 days)
        let huge = Duration::from_secs(25 * 24 * 3600);
        assert_eq!(duration_to_millis_i32(huge), i32::MAX);
    }

    #[test]
    fn test_duration_to_millis_i32_exact_max() {
        // Duration exactly at i32::MAX millis
        let exact = Duration::from_millis(i32::MAX as u64);
        assert_eq!(duration_to_millis_i32(exact), i32::MAX);
    }
}

/// Parse a comma-separated bootstrap servers string into individual addresses.
///
/// Trims whitespace, filters empty entries, and returns an error if no
/// valid servers remain.
pub fn parse_bootstrap_servers(servers: &str) -> Result<Vec<String>> {
    let addrs: Vec<String> = servers
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect();

    if addrs.is_empty() {
        return Err(KrafkaError::config("no bootstrap servers specified"));
    }

    Ok(addrs)
}

/// Extract the hostname from an address string for TLS SNI.
///
/// Handles bracketed IPv6 (`[::1]:port`), bare IPv6 (`2001:db8::1`),
/// and IPv4/hostname with optional port (`host:port`). Returns the bare
/// hostname without port or brackets.
///
/// Returns an error if the address is empty, contains mismatched brackets,
/// has empty brackets, or has an invalid bracketed format.
pub fn extract_sni_hostname(address: &str) -> Result<&str> {
    if address.is_empty() {
        return Err(KrafkaError::config("empty address"));
    }

    let has_open = address.contains('[');
    let close_pos = address.find(']');

    match (has_open, close_pos) {
        // Bracketed: [host]:port or [host]
        (true, Some(end)) => {
            // '[' must be at position 0
            if !address.starts_with('[') {
                return Err(KrafkaError::config(format!(
                    "malformed address ('[' not at start): {address}"
                )));
            }
            let hostname = &address[1..end];
            if hostname.is_empty() {
                return Err(KrafkaError::config(format!(
                    "empty hostname in brackets: {address}"
                )));
            }
            // After ']' must be empty or a well-formed ':port' with no extra brackets
            let after = &address[end + 1..];
            if after.contains('[') || after.contains(']') {
                return Err(KrafkaError::config(format!(
                    "unexpected bracket characters after closing ']': {address}"
                )));
            }
            if !after.is_empty() {
                if !after.starts_with(':') {
                    return Err(KrafkaError::config(format!(
                        "unexpected characters after closing ']': {address}"
                    )));
                }
                let port_str = &after[1..];
                if port_str.is_empty() || !port_str.chars().all(|c| c.is_ascii_digit()) {
                    return Err(KrafkaError::config(format!(
                        "invalid port after closing ']': {address}"
                    )));
                }
            }
            Ok(hostname)
        }
        // Mismatched brackets
        (true, None) => Err(KrafkaError::config(format!(
            "malformed address (missing closing ']'): {address}"
        ))),
        (false, Some(_)) => Err(KrafkaError::config(format!(
            "malformed address (unexpected ']' without '['): {address}"
        ))),
        // No brackets: bare IPv6, IPv4, or hostname
        (false, None) => {
            if address.parse::<std::net::Ipv6Addr>().is_ok() {
                Ok(address)
            } else {
                Ok(address.rsplit_once(':').map_or(address, |(host, _)| host))
            }
        }
    }
}

#[cfg(test)]
mod bootstrap_tests {
    use super::*;

    #[test]
    fn test_parse_bootstrap_servers_basic() {
        let result = parse_bootstrap_servers("localhost:9092,broker:9093").unwrap();
        assert_eq!(result, vec!["localhost:9092", "broker:9093"]);
    }

    #[test]
    fn test_parse_bootstrap_servers_trims_whitespace() {
        let result = parse_bootstrap_servers(" localhost:9092 , broker:9093 ").unwrap();
        assert_eq!(result, vec!["localhost:9092", "broker:9093"]);
    }

    #[test]
    fn test_parse_bootstrap_servers_filters_empty() {
        let result = parse_bootstrap_servers(" , ,localhost:9092, , broker:9093, ").unwrap();
        assert_eq!(result, vec!["localhost:9092", "broker:9093"]);
    }

    #[test]
    fn test_parse_bootstrap_servers_empty_string() {
        assert!(parse_bootstrap_servers("").is_err());
    }

    #[test]
    fn test_parse_bootstrap_servers_only_whitespace() {
        assert!(parse_bootstrap_servers(" , , ").is_err());
    }
}

#[cfg(test)]
mod sni_tests {
    use super::*;

    #[test]
    fn test_extract_sni_bracketed_ipv6_with_port() {
        assert_eq!(extract_sni_hostname("[::1]:9092").unwrap(), "::1");
    }

    #[test]
    fn test_extract_sni_bracketed_ipv6_no_port() {
        assert_eq!(extract_sni_hostname("[::1]").unwrap(), "::1");
    }

    #[test]
    fn test_extract_sni_bare_ipv6() {
        assert_eq!(extract_sni_hostname("2001:db8::1").unwrap(), "2001:db8::1");
    }

    #[test]
    fn test_extract_sni_bare_ipv6_loopback() {
        assert_eq!(extract_sni_hostname("::1").unwrap(), "::1");
    }

    #[test]
    fn test_extract_sni_ipv4_with_port() {
        assert_eq!(
            extract_sni_hostname("192.168.1.1:9092").unwrap(),
            "192.168.1.1"
        );
    }

    #[test]
    fn test_extract_sni_hostname_with_port() {
        assert_eq!(
            extract_sni_hostname("broker.example.com:9092").unwrap(),
            "broker.example.com"
        );
    }

    #[test]
    fn test_extract_sni_hostname_no_port() {
        assert_eq!(
            extract_sni_hostname("broker.example.com").unwrap(),
            "broker.example.com"
        );
    }

    #[test]
    fn test_extract_sni_bracketed_ipv6_full() {
        assert_eq!(
            extract_sni_hostname("[2001:db8::1]:9092").unwrap(),
            "2001:db8::1"
        );
    }

    #[test]
    fn test_extract_sni_ipv6_ambiguous_port() {
        // `2001:db8::1:9092` is a valid 8-group IPv6 address, so the function
        // correctly returns it as-is. Use bracket notation to separate host from port.
        assert_eq!(
            extract_sni_hostname("2001:db8::1:9092").unwrap(),
            "2001:db8::1:9092"
        );
        // When the string is NOT a valid IPv6 address, the last :segment
        // is stripped as a port.
        assert_eq!(
            extract_sni_hostname("2001:db8::zz:9092").unwrap(),
            "2001:db8::zz"
        );
    }

    #[test]
    fn test_extract_sni_malformed_bracket_returns_error() {
        // Missing closing ']'
        assert!(extract_sni_hostname("[::1").is_err());
        assert!(extract_sni_hostname("[host").is_err());
        assert!(extract_sni_hostname("[host:9092").is_err());
        // Stray closing ']' without opening '['
        assert!(extract_sni_hostname("::1]:9092").is_err());
        assert!(extract_sni_hostname("host]").is_err());
        assert!(extract_sni_hostname("host]:9092").is_err());
        // '[' not at start
        assert!(extract_sni_hostname("foo[::1]:9092").is_err());
        // Trailing garbage after ']'
        assert!(extract_sni_hostname("[::1]extra").is_err());
        // Extra closing bracket in port section
        assert!(extract_sni_hostname("[::1]:9092]").is_err());
        // Invalid port (non-numeric)
        assert!(extract_sni_hostname("[::1]:abc").is_err());
        // Empty port after colon
        assert!(extract_sni_hostname("[::1]:").is_err());
    }

    #[test]
    fn test_extract_sni_empty_input_returns_error() {
        assert!(extract_sni_hostname("").is_err());
        assert!(extract_sni_hostname("[]").is_err());
        assert!(extract_sni_hostname("[]:9092").is_err());
    }
}
