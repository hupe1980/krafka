#![no_main]
//! Fuzz the framing header and the standalone protocol primitives.
//!
//! `fuzz_response_decode` only reaches primitives through a message decoder, so
//! any primitive path that a message never happens to take was untested. These
//! are the very first bytes read off a socket — `ResponseHeader::decode_*` runs
//! before the client knows anything about the payload — so a panic here is
//! reachable by any broker (or anything impersonating one) on the very first
//! response of a connection.
//!
//! Beyond "must not panic", this target asserts two structural invariants that
//! a decoder must uphold on hostile input:
//!
//! 1. a successful decode never consumes more than the buffer held;
//! 2. a successful decode of a length-prefixed value never yields a value
//!    longer than the bytes that were available to it — i.e. the declared
//!    length was validated against the buffer, not trusted.

use bytes::{Buf, Bytes};
use libfuzzer_sys::fuzz_target;

use krafka::protocol::{Decode, KafkaArray, KafkaBytes, KafkaString, ResponseHeader, TaggedFields};

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let selector = data[0];
    let payload = &data[1..];
    let available = payload.len();

    match selector % 12 {
        // ── Framing header ────────────────────────────────────────────────
        0 => {
            let mut buf = Bytes::copy_from_slice(payload);
            if ResponseHeader::decode_v0(&mut buf).is_ok() {
                assert!(buf.remaining() <= available);
            }
        }
        1 => {
            let mut buf = Bytes::copy_from_slice(payload);
            if ResponseHeader::decode_v1(&mut buf).is_ok() {
                assert!(buf.remaining() <= available);
            }
        }

        // ── KafkaString ───────────────────────────────────────────────────
        2 => {
            let mut buf = Bytes::copy_from_slice(payload);
            if let Ok(s) = KafkaString::decode(&mut buf) {
                assert!(buf.remaining() <= available);
                if let Some(s) = s.0 {
                    assert!(s.len() <= available, "string outgrew its buffer");
                }
            }
        }
        3 => {
            let mut buf = Bytes::copy_from_slice(payload);
            if let Ok(s) = KafkaString::decode_compact(&mut buf) {
                assert!(buf.remaining() <= available);
                if let Some(s) = s.0 {
                    assert!(s.len() <= available, "compact string outgrew its buffer");
                }
            }
        }

        // ── KafkaBytes ────────────────────────────────────────────────────
        4 => {
            let mut buf = Bytes::copy_from_slice(payload);
            if let Ok(b) = KafkaBytes::decode(&mut buf) {
                assert!(buf.remaining() <= available);
                if let Some(b) = b.0 {
                    assert!(b.len() <= available, "bytes outgrew its buffer");
                }
            }
        }
        5 => {
            let mut buf = Bytes::copy_from_slice(payload);
            if let Ok(b) = KafkaBytes::decode_compact(&mut buf) {
                assert!(buf.remaining() <= available);
                if let Some(b) = b.0 {
                    assert!(b.len() <= available, "compact bytes outgrew its buffer");
                }
            }
        }

        // ── TaggedFields ──────────────────────────────────────────────────
        6 => {
            let mut buf = Bytes::copy_from_slice(payload);
            if let Ok(tf) = TaggedFields::decode(&mut buf) {
                assert!(buf.remaining() <= available);
                // Each field costs at least a tag varint and a length varint,
                // so the count can never exceed the byte budget.
                assert!(tf.0.len() <= available, "more tagged fields than bytes");
                let total: usize = tf.0.iter().map(|f| f.data.len()).sum();
                assert!(total <= available, "tagged field data outgrew its buffer");
            }
        }

        // ── KafkaArray, both element widths and both length encodings ─────
        7 => {
            let mut buf = Bytes::copy_from_slice(payload);
            if let Ok(a) = KafkaArray::<i32>::decode(&mut buf) {
                assert!(buf.remaining() <= available);
                if let Some(items) = a.0 {
                    assert!(items.len() <= available);
                }
            }
        }
        8 => {
            let mut buf = Bytes::copy_from_slice(payload);
            if let Ok(a) = KafkaArray::<i32>::decode_compact(&mut buf) {
                assert!(buf.remaining() <= available);
                if let Some(items) = a.0 {
                    assert!(items.len() <= available);
                }
            }
        }
        9 => {
            let mut buf = Bytes::copy_from_slice(payload);
            if let Ok(a) = KafkaArray::<i64>::decode(&mut buf) {
                assert!(buf.remaining() <= available);
                if let Some(items) = a.0 {
                    assert!(items.len() <= available);
                }
            }
        }
        10 => {
            let mut buf = Bytes::copy_from_slice(payload);
            if let Ok(a) = KafkaArray::<KafkaString>::decode(&mut buf) {
                assert!(buf.remaining() <= available);
                if let Some(items) = a.0 {
                    assert!(items.len() <= available);
                }
            }
        }
        11 => {
            let mut buf = Bytes::copy_from_slice(payload);
            if let Ok(a) = KafkaArray::<KafkaString>::decode_compact(&mut buf) {
                assert!(buf.remaining() <= available);
                if let Some(items) = a.0 {
                    assert!(items.len() <= available);
                }
            }
        }
        _ => unreachable!(),
    }
});
