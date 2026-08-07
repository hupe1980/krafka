//! Utility functions for Krafka.
//!
//! This module provides low-level utilities used throughout the crate:
//!
//! - **Correlation ID generation**: Thread-safe ID generation for request/response matching
//! - **CRC32C**: Checksum calculation for Kafka record validation
//! - **Varint encoding**: Variable-length integer encoding for compact protocols
//! - **SNI hostname extraction**: Parse hostnames from address strings for TLS SNI

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
    /// Jitter factor to randomize backoff and prevent thundering herd.
    ///
    /// **Valid range: `0.0..=1.0`.** The field is public for struct-literal
    /// construction, so an out-of-range or non-finite value can be written
    /// directly. It is never trusted: every read goes through
    /// [`BackoffPolicy::jitter_factor`], which clamps to `0.0..=1.0` and maps
    /// `NaN` to `0.0`. Prefer [`BackoffPolicy::with_jitter_factor`] or
    /// [`BackoffPolicy::set_jitter_factor`], which normalize the stored value
    /// eagerly so the invariant also holds for anything reading the field.
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
    /// The jitter is sampled uniformly from `[-jitter_range, +jitter_range]`
    /// where `jitter_range = base_backoff × jitter_factor`. The result is then
    /// clamped from below to `initial_backoff`, so the **minimum** delay is
    /// always `initial_backoff` even when jitter would otherwise push it lower.
    /// In other words, negative jitter cannot reduce the backoff below the
    /// configured floor — jitter only increases scatter above `initial_backoff`.
    ///
    /// # Constraint
    ///
    /// `max_backoff` must be >= `initial_backoff`. If not, `max_backoff` is
    /// silently clamped up to `initial_backoff` so the contract that
    /// `initial_backoff <= result <= max_backoff + jitter_range` holds.
    ///
    /// `jitter_factor` is read through [`Self::jitter_factor`], which clamps it
    /// to `0.0..=1.0` and maps `NaN` to `0.0`. A negative or non-finite
    /// `jitter_factor` therefore disables jitter instead of panicking on an
    /// empty sampling range.
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
        // Exponential backoff: initial * multiplier^(attempt-1), clamped.
        //
        // `min` carries every edge case, which is why there is no
        // large-exponent special case here:
        //
        // * a growing multiplier overflows `powi` to `+inf`, and
        //   `inf.min(ceiling)` is the ceiling — the intended answer;
        // * a shrinking multiplier underflows to `0.0`, and the floor applied
        //   below lifts it back to `initial_backoff`;
        // * a non-finite multiplier makes the product `NaN`, and `f64::min`
        //   returns the non-`NaN` operand rather than propagating it.
        //
        // A previous version short-circuited to the ceiling whenever
        // `multiplier > 1.0 && exponent >= 1024`, to avoid "evaluating powi with
        // a large exponent". That guard was both unnecessary and wrong:
        // `powi` is repeated squaring, so even `i32::MAX` costs ~31
        // multiplications, and for a multiplier just above 1.0 the exponential
        // is nowhere near the ceiling at attempt 1025 — `multiplier = 1.0000001`
        // jumped from 100 ms straight to `max_backoff` instead of the 100.01 ms
        // it had actually reached. Mutation testing surfaced it: negating the
        // `||` changed nothing any test could see.
        let base_backoff = if initial_secs >= effective_max_secs {
            effective_max_secs
        } else {
            (initial_secs * self.backoff_multiplier.powi(exponent)).min(effective_max_secs)
        };

        // Add ±jitter to prevent thundering herd.
        // The final .max(initial_backoff) clamps the floor so negative jitter
        // cannot reduce the delay below the configured initial_backoff.
        // Read through the accessor so a directly-assigned negative/NaN
        // jitter_factor cannot produce an empty (panicking) sampling range.
        //
        // `cargo mutants` reports four surviving mutants across the jitter
        // block below. All four are *equivalent* — no test can distinguish
        // them, and none should be written to try:
        //
        // * `jitter_factor > 0.0` → `>= 0.0`, and `&&` → `||`: `jitter_range`
        //   is `base_backoff * jitter_factor`, so the two conditions cannot
        //   disagree except when `base_backoff` is zero — and there
        //   `random_range(-0.0..=0.0)` returns `0.0` regardless.
        // * `base_backoff + jitter` → `-`: the jitter is sampled symmetrically
        //   from `[-range, +range]`, so negating it yields the same
        //   distribution.
        //
        // Recorded here so the next person to run mutation testing does not
        // spend the afternoon contorting a test around them.
        let jitter_factor = self.jitter_factor();
        let jitter_range = base_backoff * jitter_factor;
        let jitter = if jitter_factor > 0.0 && jitter_range > 0.0 {
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

    /// Jitter factor, always normalized into `0.0..=1.0`.
    ///
    /// The backing field is public and may hold an out-of-range or non-finite
    /// value if it was assigned directly. This accessor is the authoritative
    /// read: values below `0.0` clamp to `0.0`, values above `1.0` clamp to
    /// `1.0`, and `NaN` maps to `0.0` (jitter disabled). Clamping here is what
    /// keeps [`Self::calculate_backoff`] from sampling an empty range.
    #[inline]
    pub fn jitter_factor(&self) -> f64 {
        Self::clamp_jitter_factor(self.jitter_factor)
    }

    /// Normalize a raw jitter factor into `0.0..=1.0`, mapping `NaN` to `0.0`.
    #[inline]
    fn clamp_jitter_factor(factor: f64) -> f64 {
        if factor.is_nan() {
            0.0
        } else {
            factor.clamp(0.0, 1.0)
        }
    }

    /// Set the jitter factor, clamping it into `0.0..=1.0`.
    ///
    /// Negative values become `0.0`, values above `1.0` become `1.0`, and
    /// `NaN` becomes `0.0`. Use this instead of assigning the public field so
    /// the stored value already satisfies the invariant.
    #[inline]
    pub fn set_jitter_factor(&mut self, factor: f64) {
        self.jitter_factor = Self::clamp_jitter_factor(factor);
    }

    /// Builder-style variant of [`Self::set_jitter_factor`].
    ///
    /// The factor is clamped into `0.0..=1.0` (`NaN` becomes `0.0`).
    #[inline]
    #[must_use]
    pub fn with_jitter_factor(mut self, factor: f64) -> Self {
        self.set_jitter_factor(factor);
        self
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
///
/// # A note for anyone running `cargo mutants` here
///
/// Eight mutants survive in this function, all inside the warning's rate
/// limiter — the interval constant, the `ms > i32::MAX` warn trigger, and the
/// `compare_exchange` bookkeeping. **None of them change the returned value**,
/// which is `ms.min(i32::MAX as u128) as i32` on the last line and is covered
/// by its own tests.
///
/// They are left alone on purpose. Pinning them would mean asserting on
/// `tracing` output gated by a `static` that lives for the whole process, so
/// the test's result would depend on whether some earlier test in the binary
/// had already consumed the hour-long window. An order-dependent test is worse
/// than no test, and the thing it would protect is a log line.
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

/// Stamp the RFC 4122 version (4) and variant bits into a random 16-byte
/// buffer.
///
/// Split out of [`random_uuid_v4`] so the bit manipulation can be checked
/// against every possible input byte rather than against whatever the RNG
/// happened to produce. The previous test asserted the version and variant
/// nibbles of a single random UUID, which caught a corrupted variant mask only
/// half the time — a coin flip dressed up as an assertion.
///
/// `cargo mutants` reports the two `|` operators below as surviving mutants
/// (`| 0x40` → `^ 0x40`, `| 0x80` → `^ 0x80`). Both are *equivalent*: the
/// preceding mask has already cleared exactly the bits being set, so `|` and
/// `^` agree for all 256 inputs — checked exhaustively in
/// `stamp_uuid_v4_bits_is_correct_for_every_input`. The mutants that are *not*
/// equivalent, notably `& 0x3F` → `| 0x3F`, are killed by that same test.
#[inline]
fn stamp_uuid_v4_bits(bytes: &mut [u8; 16]) {
    // Version 4: high nibble of byte 6 becomes 0b0100.
    bytes[6] = (bytes[6] & 0x0F) | 0x40;
    // Variant RFC 4122: top two bits of byte 8 become 0b10.
    bytes[8] = (bytes[8] & 0x3F) | 0x80;
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
    stamp_uuid_v4_bits(&mut bytes);

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

    /// Decode a signed 32-bit varint (zigzag encoded).
    ///
    /// Inherits the strictness of [`decode_unsigned_varint`]: encodings whose
    /// fifth byte carries bits above the 32-bit value range are rejected rather
    /// than silently truncated.
    #[inline]
    pub fn decode_signed_varint(buf: &mut impl Buf) -> Result<i32> {
        let unsigned = decode_unsigned_varint(buf)?;
        Ok(((unsigned >> 1) as i32) ^ -((unsigned & 1) as i32))
    }

    /// Decode an unsigned 32-bit varint.
    ///
    /// At most five bytes are consumed. The fifth (final) byte may only carry
    /// the four remaining value bits, so any payload above `0x0F` in that
    /// position would overflow `u32`. Such encodings are **rejected** with
    /// [`ProtocolErrorKind::InvalidLength`] rather than silently truncated —
    /// matching the Java client's `readUnsignedVarint`, which throws. This
    /// keeps `FF FF FF FF 7F` (non-canonical) distinguishable from
    /// `FF FF FF FF 0F` (canonical `u32::MAX`); previously both decoded to
    /// `u32::MAX`.
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

            // The 5th byte (shift == 28) holds only bits 28..=31; anything
            // above 0x0F would be shifted out of the u32 entirely.
            if shift == 28 && byte & 0x7F > 0x0F {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::InvalidLength,
                    "varint overflows u32",
                ));
            }

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

    /// Decode a signed 64-bit varlong (zigzag encoded).
    ///
    /// Inherits the strictness of [`decode_unsigned_varlong`]: encodings whose
    /// tenth byte carries bits above the 64-bit value range are rejected rather
    /// than silently truncated.
    #[inline]
    pub fn decode_signed_varlong(buf: &mut impl Buf) -> Result<i64> {
        let unsigned = decode_unsigned_varlong(buf)?;
        Ok(((unsigned >> 1) as i64) ^ -((unsigned & 1) as i64))
    }

    /// Decode an unsigned 64-bit varlong.
    ///
    /// At most ten bytes are consumed. The tenth (final) byte may only carry
    /// the single remaining value bit, so any payload above `0x01` in that
    /// position would overflow `u64`. Such encodings are **rejected** with
    /// [`ProtocolErrorKind::InvalidLength`] rather than silently truncated,
    /// mirroring [`decode_unsigned_varint`] and the Java client's `readVarlong`.
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

            // The 10th byte (shift == 63) holds only bit 63; anything above
            // 0x01 would be shifted out of the u64 entirely.
            if shift == 63 && byte & 0x7F > 0x01 {
                return Err(KrafkaError::protocol_kind(
                    ProtocolErrorKind::InvalidLength,
                    "varlong overflows u64",
                ));
            }

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

    // --- Varint overflow / non-canonical encoding rejection ---

    #[test]
    fn test_decode_unsigned_varint_max_canonical() {
        // FF FF FF FF 0F is the canonical 5-byte encoding of u32::MAX.
        let mut buf = &[0xFFu8, 0xFF, 0xFF, 0xFF, 0x0F][..];
        assert_eq!(varint::decode_unsigned_varint(&mut buf).unwrap(), u32::MAX);
    }

    #[test]
    fn test_decode_unsigned_varint_rejects_overflowing_fifth_byte() {
        // FF FF FF FF 7F sets bits 32-34, which do not fit in a u32. It used
        // to decode to u32::MAX (silent truncation); it must now be rejected.
        let mut buf = &[0xFFu8, 0xFF, 0xFF, 0xFF, 0x7F][..];
        let err = varint::decode_unsigned_varint(&mut buf).unwrap_err();
        assert!(
            err.to_string().contains("overflows u32"),
            "unexpected error: {err}"
        );

        // Smallest rejected fifth byte: 0x10 (bit 32).
        let mut buf = &[0xFFu8, 0xFF, 0xFF, 0xFF, 0x10][..];
        assert!(varint::decode_unsigned_varint(&mut buf).is_err());
    }

    #[test]
    fn test_decode_unsigned_varint_still_rejects_too_long() {
        // A 6th continuation byte is still reported as "varint too long".
        let mut buf = &[0xFFu8, 0xFF, 0xFF, 0xFF, 0x8F, 0x01][..];
        let err = varint::decode_unsigned_varint(&mut buf).unwrap_err();
        assert!(
            err.to_string().contains("too long"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_decode_signed_varint_rejects_overflowing_fifth_byte() {
        let mut buf = &[0xFFu8, 0xFF, 0xFF, 0xFF, 0x7F][..];
        assert!(varint::decode_signed_varint(&mut buf).is_err());
    }

    #[test]
    fn test_decode_unsigned_varlong_max_canonical() {
        // Ten-byte canonical encoding of u64::MAX ends in 0x01.
        let mut buf = &[0xFFu8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01][..];
        assert_eq!(varint::decode_unsigned_varlong(&mut buf).unwrap(), u64::MAX);
    }

    #[test]
    fn test_decode_unsigned_varlong_rejects_overflowing_tenth_byte() {
        // 0x7F in the tenth byte sets bits 64-69, which do not fit in a u64.
        let mut buf = &[0xFFu8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F][..];
        let err = varint::decode_unsigned_varlong(&mut buf).unwrap_err();
        assert!(
            err.to_string().contains("overflows u64"),
            "unexpected error: {err}"
        );

        // Smallest rejected tenth byte: 0x02 (bit 64).
        let mut buf = &[0xFFu8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x02][..];
        assert!(varint::decode_unsigned_varlong(&mut buf).is_err());
    }

    // --- BackoffPolicy jitter_factor clamping ---

    #[test]
    fn test_jitter_factor_accessor_clamps_negative() {
        let policy = BackoffPolicy {
            jitter_factor: -0.5,
            ..BackoffPolicy::default()
        };
        assert_eq!(policy.jitter_factor(), 0.0);
    }

    #[test]
    fn test_jitter_factor_accessor_clamps_above_one() {
        let policy = BackoffPolicy {
            jitter_factor: 7.5,
            ..BackoffPolicy::default()
        };
        assert_eq!(policy.jitter_factor(), 1.0);
    }

    #[test]
    fn test_jitter_factor_accessor_maps_nan_to_zero() {
        let policy = BackoffPolicy {
            jitter_factor: f64::NAN,
            ..BackoffPolicy::default()
        };
        assert_eq!(policy.jitter_factor(), 0.0);
    }

    #[test]
    fn test_set_and_with_jitter_factor_clamp_stored_value() {
        let mut policy = BackoffPolicy::default();
        policy.set_jitter_factor(-1.0);
        assert_eq!(policy.jitter_factor, 0.0);
        policy.set_jitter_factor(3.0);
        assert_eq!(policy.jitter_factor, 1.0);
        policy.set_jitter_factor(f64::NAN);
        assert_eq!(policy.jitter_factor, 0.0);

        let policy = BackoffPolicy::default().with_jitter_factor(-2.0);
        assert_eq!(policy.jitter_factor, 0.0);
        let policy = BackoffPolicy::default().with_jitter_factor(0.25);
        assert_eq!(policy.jitter_factor, 0.25);
    }

    #[test]
    fn test_calculate_backoff_does_not_panic_on_negative_jitter_factor() {
        // A negative jitter_factor previously produced an empty sampling range
        // (`random_range(0.05..=-0.05)`), which panics. It must now behave
        // exactly like jitter_factor == 0.0.
        let policy = BackoffPolicy {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter_factor: -0.5,
        };
        let no_jitter = BackoffPolicy {
            jitter_factor: 0.0,
            ..policy.clone()
        };
        for attempt in 1..=6 {
            assert_eq!(
                policy.calculate_backoff(attempt),
                no_jitter.calculate_backoff(attempt),
                "negative jitter_factor must behave like 0.0 (attempt {attempt})"
            );
        }
    }

    #[test]
    fn test_calculate_backoff_does_not_panic_on_nan_jitter_factor() {
        let policy = BackoffPolicy {
            jitter_factor: f64::NAN,
            ..BackoffPolicy::default()
        };
        // NaN disables jitter, so the result is the deterministic exponential.
        assert_eq!(policy.calculate_backoff(1), Duration::from_millis(100));
        assert_eq!(policy.calculate_backoff(2), Duration::from_millis(200));
    }

    #[test]
    fn test_calculate_backoff_clamps_jitter_factor_above_one() {
        // jitter_factor > 1.0 is clamped to 1.0, so jitter never exceeds ±base.
        let policy = BackoffPolicy {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter_factor: 100.0,
        };
        for _ in 0..64 {
            let d = policy.calculate_backoff(4).as_secs_f64();
            // base = 0.8s, jitter in [-0.8, 0.8], floored at initial (0.1s).
            assert!((0.1..=1.6).contains(&d), "backoff out of range: {d}");
        }
    }

    #[test]
    fn test_calculate_backoff_grows_exponentially_and_caps() {
        let policy = BackoffPolicy {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(1000),
            backoff_multiplier: 2.0,
            jitter_factor: 0.0,
        };
        assert_eq!(policy.calculate_backoff(0), Duration::ZERO);
        assert_eq!(policy.calculate_backoff(1), Duration::from_millis(100));
        assert_eq!(policy.calculate_backoff(2), Duration::from_millis(200));
        assert_eq!(policy.calculate_backoff(3), Duration::from_millis(400));
        assert_eq!(policy.calculate_backoff(4), Duration::from_millis(800));
        // Capped from here on.
        assert_eq!(policy.calculate_backoff(5), Duration::from_millis(1000));
        assert_eq!(policy.calculate_backoff(50), Duration::from_millis(1000));
    }

    #[test]
    fn test_calculate_backoff_flat_multiplier_stays_flat_for_huge_attempts() {
        // A multiplier of 1.0 describes a flat series. A large attempt number
        // must not be short-circuited to `max_backoff`.
        let policy = BackoffPolicy {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 1.0,
            jitter_factor: 0.0,
        };
        assert_eq!(policy.calculate_backoff(1), Duration::from_millis(100));
        assert_eq!(policy.calculate_backoff(5_000), Duration::from_millis(100));
        assert_eq!(
            policy.calculate_backoff(u32::MAX),
            Duration::from_millis(100)
        );
    }

    #[test]
    fn test_calculate_backoff_shrinking_multiplier_floors_at_initial() {
        // A multiplier below 1.0 shrinks; the floor is `initial_backoff`, not
        // the ceiling.
        let policy = BackoffPolicy {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 0.5,
            jitter_factor: 0.0,
        };
        assert_eq!(policy.calculate_backoff(10_000), Duration::from_millis(100));
    }

    #[test]
    fn test_calculate_backoff_is_jittered_within_bounds() {
        let policy = BackoffPolicy {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_millis(1000),
            backoff_multiplier: 2.0,
            jitter_factor: 0.2,
        };
        // Attempt 3 has a base of 400 ms and ±20% jitter, so results land in
        // [320, 480] ms and must not all be identical.
        let mut seen = std::collections::HashSet::new();
        for _ in 0..256 {
            let d = policy.calculate_backoff(3);
            assert!(
                d >= Duration::from_millis(320) && d <= Duration::from_millis(480),
                "jittered backoff out of bounds: {d:?}"
            );
            seen.insert(d.as_nanos());
        }
        assert!(seen.len() > 1, "jitter must actually randomize the delay");
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

    // ── Findings from `cargo mutants` ─────────────────────────────────

    /// The version and variant bits, checked against every possible input.
    ///
    /// This replaces asserting the nibbles of one random UUID, which caught a
    /// corrupted variant mask only half the time — the surviving mutant
    /// (`& 0x3F` → `| 0x3F`) leaves bit 6 random, so the old test passed on a
    /// coin flip.
    #[test]
    fn stamp_uuid_v4_bits_is_correct_for_every_input() {
        for byte in 0u8..=255 {
            let mut bytes = [byte; 16];
            stamp_uuid_v4_bits(&mut bytes);

            assert_eq!(
                bytes[6] >> 4,
                4,
                "version nibble must be 4 for input byte {byte:#04x}"
            );
            assert_eq!(
                bytes[6] & 0x0F,
                byte & 0x0F,
                "the low nibble of byte 6 must survive untouched"
            );
            assert_eq!(
                bytes[8] >> 6,
                0b10,
                "variant bits must be 0b10 for input byte {byte:#04x}"
            );
            assert_eq!(
                bytes[8] & 0x3F,
                byte & 0x3F,
                "the low six bits of byte 8 must survive untouched"
            );
            // Every other byte is left alone.
            for i in [0, 1, 2, 3, 4, 5, 7, 9, 10, 11, 12, 13, 14, 15] {
                assert_eq!(bytes[i], byte, "byte {i} must not be modified");
            }
        }
    }

    /// A multiplier just above 1.0 must keep growing, not jump to the ceiling.
    ///
    /// The previous implementation short-circuited to `max_backoff` whenever
    /// `multiplier > 1.0 && exponent >= 1024`, on the theory that such an
    /// exponent would overflow. It does not for a multiplier this close to 1:
    /// at attempt 1025 the series has reached ~100.01 ms, and the old code
    /// returned the full 10 s ceiling — a 100× jump.
    #[test]
    fn calculate_backoff_does_not_jump_to_the_ceiling_for_slow_growth() {
        let policy = BackoffPolicy {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 1.000_000_1,
            jitter_factor: 0.0,
        };

        let backoff = policy.calculate_backoff(1025);
        assert!(
            backoff < Duration::from_millis(101),
            "a multiplier of 1.0000001 has barely grown by attempt 1025, got {backoff:?}"
        );
        assert!(backoff >= Duration::from_millis(100));
    }

    /// A genuinely growing multiplier still saturates at the ceiling, which is
    /// what the removed short-circuit was there for — `powi` overflowing to
    /// `inf` and `inf.min(ceiling)` gives the same answer without the guard.
    #[test]
    fn calculate_backoff_saturates_when_the_exponential_overflows() {
        let policy = BackoffPolicy {
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            backoff_multiplier: 2.0,
            jitter_factor: 0.0,
        };
        assert_eq!(policy.calculate_backoff(2_000), Duration::from_secs(10));
        assert_eq!(policy.calculate_backoff(u32::MAX), Duration::from_secs(10));
    }

    /// The accessors report the configured values rather than defaults.
    #[test]
    fn backoff_accessors_return_the_configured_values() {
        let policy = BackoffPolicy {
            initial_backoff: Duration::from_millis(250),
            max_backoff: Duration::from_secs(30),
            backoff_multiplier: 3.5,
            jitter_factor: 0.25,
        };
        assert_eq!(policy.initial_backoff(), Duration::from_millis(250));
        assert_eq!(policy.max_backoff(), Duration::from_secs(30));
        assert!((policy.backoff_multiplier() - 3.5).abs() < f64::EPSILON);
        assert!((policy.jitter_factor() - 0.25).abs() < f64::EPSILON);
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
