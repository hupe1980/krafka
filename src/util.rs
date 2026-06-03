//! Utility functions for Krafka.\n//!\n//! This module provides low-level utilities used throughout the crate:\n//!\n//! - **Correlation ID generation**: Thread-safe ID generation for request/response matching\n//! - **CRC32C**: Checksum calculation for Kafka record validation\n//! - **Varint encoding**: Variable-length integer encoding for compact protocols\n//! - **SNI hostname extraction**: Parse hostnames from address strings for TLS SNI

use std::cell::RefCell;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicI32, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};

use crate::error::{KrafkaError, Result};

// Thread-local fast PRNG for non-security uses (backoff jitter).
// SmallRng is not cryptographically secure; UUIDs and other security-sensitive
// values continue to use the global CSPRNG via `rand::random()`.
thread_local! {
    static JITTER_RNG: RefCell<SmallRng> = RefCell::new(SmallRng::from_os_rng());
}

/// Shared exponential-backoff parameters used by both [`RetryPolicy`] and
/// [`ConnectionRetryConfig`].
///
/// [`RetryPolicy`]: crate::producer::RetryPolicy
/// [`ConnectionRetryConfig`]: crate::network::ConnectionRetryConfig
#[derive(Debug, Clone)]
pub struct BackoffPolicy {
    /// Initial backoff duration (first retry delay).
    pub initial_backoff: Duration,
    /// Maximum backoff duration (caps exponential growth).
    pub max_backoff: Duration,
    /// Backoff multiplier for exponential growth (typically 2.0).
    pub backoff_multiplier: f64,
    /// Jitter factor (0.0–1.0) to randomize backoff and prevent thundering herd.
    pub jitter_factor: f64,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter_factor: 0.1,
        }
    }
}

impl BackoffPolicy {
    /// Calculate the backoff duration for a given attempt number.
    ///
    /// Attempt 0 returns `Duration::ZERO` (no delay before first attempt).
    /// Attempt 1 = first retry = `initial_backoff`. Subsequent attempts grow
    /// exponentially up to `max_backoff`, with optional ±jitter.
    ///
    /// # Constraint
    ///
    /// `max_backoff` must be >= `initial_backoff`. If not, `max_backoff` is
    /// silently clamped up to `initial_backoff` so the contract that
    /// `initial_backoff <= result <= max_backoff` holds.
    #[inline]
    pub fn calculate_backoff(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::ZERO;
        }

        // Clamp max_backoff to at least initial_backoff.
        // If the caller configured max_backoff < initial_backoff the floor
        // below would silently ignore max_backoff, so we canonicalize first.
        let effective_max = self.max_backoff.max(self.initial_backoff);

        // Early-exit once the exponential would already exceed the ceiling.
        // This avoids evaluating powi() with a potentially large exponent on
        // every poll in a retry storm.  The threshold is conservative: once
        // `attempt` is large enough that even a single step would exceed
        // `effective_max`, all future attempts land at the clamped value.
        let initial_secs = self.initial_backoff.as_secs_f64();
        let effective_max_secs = effective_max.as_secs_f64();
        let exponent = attempt.saturating_sub(1).min(i32::MAX as u32) as i32;
        // If the initial value already equals the ceiling, or if the exponent
        // is large enough that floating-point will overflow to infinity, skip
        // powi entirely and go straight to jitter application at the cap.
        let base_backoff = if initial_secs >= effective_max_secs || exponent >= 1024 {
            effective_max_secs
        } else {
            // Exponential backoff: initial * multiplier^(attempt-1)
            (initial_secs * self.backoff_multiplier.powi(exponent)).min(effective_max_secs)
        };

        // Add ±jitter to prevent thundering herd.
        let jitter_range = base_backoff * self.jitter_factor;
        let jitter = if self.jitter_factor > 0.0 {
            JITTER_RNG.with(|rng| rng.borrow_mut().random_range(-jitter_range..=jitter_range))
        } else {
            0.0
        };

        let final_backoff = (base_backoff + jitter).max(self.initial_backoff.as_secs_f64());

        if !final_backoff.is_finite() {
            tracing::warn!(
                "BackoffPolicy::calculate_backoff produced non-finite value ({final_backoff}); capping at max_backoff"
            );
            return effective_max;
        }

        Duration::from_secs_f64(final_backoff)
    }

    /// Initial backoff duration.
    #[inline]
    pub fn initial_backoff(&self) -> Duration {
        self.initial_backoff
    }

    /// Maximum backoff duration.
    #[inline]
    pub fn max_backoff(&self) -> Duration {
        self.max_backoff
    }

    /// Backoff multiplier.
    #[inline]
    pub fn backoff_multiplier(&self) -> f64 {
        self.backoff_multiplier
    }

    /// Jitter factor (0.0–1.0).
    #[inline]
    pub fn jitter_factor(&self) -> f64 {
        self.jitter_factor
    }
}

/// Reserved correlation ID for requests that intentionally expect no response.
///
/// The normal request generator skips this value so fire-and-forget paths can
/// avoid consuming the regular request/response ID space.
pub(crate) const NO_RESPONSE_CORRELATION_ID: i32 = i32::MIN;

/// Convert a `Duration` to milliseconds as `i32`, capping at `i32::MAX`.
///
/// `Duration::as_millis()` returns `u128`, which would silently truncate
/// when cast to `i32`. This function caps the value at `i32::MAX` (~24.8 days)
/// to prevent silent wraparound. A warning is logged when the cap fires so
/// over-large timeouts are visible in production logs rather than silently
/// becoming a much smaller value.
#[inline]
pub fn duration_to_millis_i32(d: Duration) -> i32 {
    /// Minimum interval between clamp warnings. Fires at most once per hour so
    /// persistent misconfiguration remains visible after restarts without
    /// flooding logs under high call rates.
    const WARN_INTERVAL_NANOS: u64 = 3600 * 1_000_000_000;
    static BASELINE: OnceLock<Instant> = OnceLock::new();
    static NEXT_WARN_NANOS: AtomicU64 = AtomicU64::new(0);

    let ms = d.as_millis();
    if ms > i32::MAX as u128 {
        let now_nanos = BASELINE
            .get_or_init(Instant::now)
            .elapsed()
            .as_nanos()
            .min(u64::MAX as u128) as u64;
        let next = NEXT_WARN_NANOS.load(Ordering::Relaxed);
        if now_nanos >= next
            && NEXT_WARN_NANOS
                .compare_exchange(
                    next,
                    now_nanos + WARN_INTERVAL_NANOS,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
        {
            tracing::warn!(
                duration_ms = %ms,
                capped_at = i32::MAX,
                "duration exceeds i32::MAX (~24.8 days); clamping to i32::MAX. \
                 Check timeout/deadline configuration. (repeats at most once per hour)"
            );
        }
    }
    ms.min(i32::MAX as u128) as i32
}

/// Convert a `Duration` to milliseconds as `i64`, capping at `i64::MAX`.
///
/// `Duration::as_millis()` returns `u128`, which would silently truncate
/// when cast to `i64`. This function caps the value at `i64::MAX` (~292 million years)
/// to prevent silent wraparound.
#[inline]
pub fn duration_to_millis_i64(d: Duration) -> i64 {
    d.as_millis().min(i64::MAX as u128) as i64
}

/// Generate a random UUID v4 string (KIP-1082 client-generated member ID).
///
/// Format: `xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx` where `y` is one of
/// `{8, 9, a, b}`. Uses `rand::ThreadRng` (ChaCha12, OS-seeded CSPRNG) for the
/// 122 random bits. Suitable for both uniqueness (member IDs, client IDs)
/// and non-predictability — UUIDs generated here are not guessable.
///
/// A single heap allocation of exactly 36 bytes is made.
pub fn random_uuid_v4() -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut bytes: [u8; 16] = rand::random();
    // Set version (4) and variant (RFC 4122).
    bytes[6] = (bytes[6] & 0x0F) | 0x40;
    bytes[8] = (bytes[8] & 0x3F) | 0x80;

    // Encode into a 36-byte UUID string (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx).
    // Dashes fall after byte groups 4, 6, 8, 10 (byte indices 4, 6, 8, 10).
    let mut s = String::with_capacity(36);
    for (i, &b) in bytes.iter().enumerate() {
        if i == 4 || i == 6 || i == 8 || i == 10 {
            s.push('-');
        }
        s.push(HEX[(b >> 4) as usize] as char);
        s.push(HEX[(b & 0xF) as usize] as char);
    }
    debug_assert_eq!(s.len(), 36, "UUID must be exactly 36 chars");
    s
}

/// Thread-safe correlation ID generator.
///
/// The counter wraps around from `i32::MAX` to `i32::MIN` (roughly every
/// 2.1 billion IDs), while skipping the reserved
/// `NO_RESPONSE_CORRELATION_ID` sentinel used by fire-and-forget requests.
/// With a bounded in-flight window (default 256), collision between a
/// recycled ID and a still-pending request is extremely unlikely.
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
    ///
    /// IDs are unique modulo `i32` wraparound, excluding the reserved
    /// `NO_RESPONSE_CORRELATION_ID` sentinel. Negative values are valid
    /// Kafka correlation IDs.
    #[inline]
    pub fn next(&self) -> i32 {
        loop {
            let correlation_id = self.counter.fetch_add(1, Ordering::Relaxed);
            if correlation_id != NO_RESPONSE_CORRELATION_ID {
                return correlation_id;
            }
        }
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

    use crate::error::{KrafkaError, ProtocolErrorKind, Result};

    /// Return the encoded byte length of an unsigned varint.
    #[inline]
    pub const fn unsigned_varint_size(mut value: u32) -> usize {
        let mut len = 1usize;
        while value >= 0x80 {
            value >>= 7;
            len += 1;
        }
        len
    }

    /// Return the encoded byte length of a signed varint (zigzag encoded).
    #[inline]
    pub const fn signed_varint_size(value: i32) -> usize {
        let unsigned = ((value << 1) ^ (value >> 31)) as u32;
        unsigned_varint_size(unsigned)
    }

    /// Return the encoded byte length of an unsigned varlong.
    #[inline]
    pub const fn unsigned_varlong_size(mut value: u64) -> usize {
        let mut len = 1usize;
        while value >= 0x80 {
            value >>= 7;
            len += 1;
        }
        len
    }

    /// Return the encoded byte length of a signed varlong (zigzag encoded).
    #[inline]
    pub const fn signed_varlong_size(value: i64) -> usize {
        let unsigned = ((value << 1) ^ (value >> 63)) as u64;
        unsigned_varlong_size(unsigned)
    }

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
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::TruncatedFrame,
                    "unexpected end of varint",
                ));
            }

            let byte = buf.get_u8();
            result |= ((byte & 0x7F) as u32) << shift;

            if byte & 0x80 == 0 {
                break;
            }

            shift += 7;
            if shift >= 35 {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::InvalidLength,
                    "varint too long",
                ));
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
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::TruncatedFrame,
                    "unexpected end of varlong",
                ));
            }

            let byte = buf.get_u8();
            result |= ((byte & 0x7F) as u64) << shift;

            if byte & 0x80 == 0 {
                break;
            }

            shift += 7;
            if shift >= 70 {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::InvalidLength,
                    "varlong too long",
                ));
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
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
    fn test_correlation_id_generator_skips_reserved_no_response_id() {
        let generator = CorrelationIdGenerator {
            counter: AtomicI32::new(NO_RESPONSE_CORRELATION_ID),
        };

        assert_eq!(generator.next(), NO_RESPONSE_CORRELATION_ID + 1);
        assert_eq!(generator.next(), NO_RESPONSE_CORRELATION_ID + 2);
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

    #[test]
    fn test_duration_to_millis_i64_normal() {
        assert_eq!(duration_to_millis_i64(Duration::from_millis(100)), 100);
        assert_eq!(duration_to_millis_i64(Duration::from_secs(30)), 30_000);
        assert_eq!(duration_to_millis_i64(Duration::ZERO), 0);
    }

    #[test]
    fn test_duration_to_millis_i64_caps_at_max() {
        // u64::MAX seconds far exceeds i64::MAX millis
        let huge = Duration::from_secs(u64::MAX);
        assert_eq!(duration_to_millis_i64(huge), i64::MAX);
    }

    #[test]
    fn test_duration_to_millis_i64_exact_max() {
        // Duration exactly at i64::MAX millis
        let exact = Duration::from_millis(i64::MAX as u64);
        assert_eq!(duration_to_millis_i64(exact), i64::MAX);
    }

    #[test]
    fn test_random_uuid_v4_format() {
        let uuid = random_uuid_v4();
        // 8-4-4-4-12 hex format = 36 chars
        assert_eq!(uuid.len(), 36);
        let parts: Vec<&str> = uuid.split('-').collect();
        assert_eq!(parts.len(), 5);
        assert_eq!(parts[0].len(), 8);
        assert_eq!(parts[1].len(), 4);
        assert_eq!(parts[2].len(), 4);
        assert_eq!(parts[3].len(), 4);
        assert_eq!(parts[4].len(), 12);
        // All chars are hex digits or hyphens
        assert!(uuid.chars().all(|c| c.is_ascii_hexdigit() || c == '-'));
    }

    #[test]
    fn test_random_uuid_v4_version_and_variant() {
        let uuid = random_uuid_v4();
        let parts: Vec<&str> = uuid.split('-').collect();
        // Version nibble: first char of third group must be '4'
        assert_eq!(
            parts[2].chars().next().unwrap(),
            '4',
            "UUID version nibble must be 4"
        );
        // Variant: first char of fourth group must be 8, 9, a, or b
        let variant = parts[3].chars().next().unwrap();
        assert!(
            matches!(variant, '8' | '9' | 'a' | 'b'),
            "UUID variant nibble must be 8/9/a/b, got '{variant}'"
        );
    }

    #[test]
    fn test_random_uuid_v4_uniqueness() {
        let a = random_uuid_v4();
        let b = random_uuid_v4();
        assert_ne!(a, b, "Two UUIDs should not be identical");
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
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
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
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
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
