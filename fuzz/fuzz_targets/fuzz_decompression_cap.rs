#![no_main]

//! Record-batch decompression under a small explicit cap, in every codec.
//!
//! Two modes, chosen by byte 0:
//!
//! - **Arbitrary compressed bytes** wrapped in a valid v2 batch header (length
//!   and CRC fixed up, so the input reaches the codec): decoding never panics,
//!   and an accepted batch holds no more record bytes than the cap.
//! - **A batch the crate itself compressed** from a payload longer than the
//!   cap: decoding must fail. This is the decompression-bomb case — a highly
//!   repetitive payload compresses to a few bytes and must still be refused.
//!
//! Byte 1 picks the codec, byte 2 the cap (64 B steps up to 16 KiB).

use bytes::{BufMut, Bytes, BytesMut};
use libfuzzer_sys::fuzz_target;

use krafka::__private::protocol::{Compression, RecordBatch, RecordBatchBuilder};
use krafka::__private::util::crc32c;

const CODECS: [Compression; 5] = [
    Compression::None,
    Compression::Gzip,
    Compression::Snappy,
    Compression::Lz4,
    Compression::Zstd,
];

/// A v2 batch header (61 bytes) in front of `records`, declaring `count`
/// records compressed with `codec`.
fn wrap(codec: Compression, count: i32, records: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(61 + records.len());
    b.put_i64(0); // base offset
    b.put_i32((49 + records.len()) as i32); // batch length
    b.put_i32(0); // partition leader epoch
    b.put_i8(2); // magic
    b.put_u32(0); // CRC, patched below
    b.put_i16(codec as i16); // attributes
    b.put_i32(count.saturating_sub(1).max(0)); // last offset delta
    b.put_i64(0); // base timestamp
    b.put_i64(0); // max timestamp
    b.put_i64(-1); // producer id
    b.put_i16(-1); // producer epoch
    b.put_i32(-1); // base sequence
    b.put_i32(count);
    b.put_slice(records);
    let crc = crc32c(&b[21..]);
    b[17..21].copy_from_slice(&crc.to_be_bytes());
    b.freeze()
}

fuzz_target!(|data: &[u8]| {
    let [mode, codec, cap, payload @ ..] = data else {
        return;
    };
    let codec = CODECS[usize::from(*codec) % CODECS.len()];
    let cap = (usize::from(*cap) + 1) * 64;

    if mode & 1 == 0 {
        let count = i32::from(*mode >> 1);
        let mut buf = wrap(codec, count, payload);
        if let Ok(batch) = RecordBatch::decode_with_limit(&mut buf, cap) {
            let held: usize = batch
                .records
                .iter()
                .map(|r| {
                    r.key.as_ref().map_or(0, |k| k.len())
                        + r.value.as_ref().map_or(0, |v| v.len())
                        + r.headers
                            .iter()
                            .map(|h| h.key.len() + h.value.as_ref().map_or(0, |v| v.len()))
                            .sum::<usize>()
                })
                .sum();
            assert!(held <= cap, "{held} record bytes decoded under a {cap}-byte cap");
        }
    } else {
        // Repeat the payload past the cap: the uncompressed record section is
        // longer than the value alone, so this is over the cap in every codec.
        let seed = if payload.is_empty() { &[0u8][..] } else { payload };
        let value: Vec<u8> = seed.iter().copied().cycle().take(cap + 1).collect();
        let batch = RecordBatchBuilder::new()
            .compression(codec)
            .add_record(None::<Bytes>, Some(value))
            .build();
        let Ok(mut buf) = batch.encode() else {
            return;
        };
        if codec == Compression::None {
            // Uncompressed records are not decompressed, so no cap applies.
            return;
        }
        assert!(
            RecordBatch::decode_with_limit(&mut buf, cap).is_err(),
            "{codec:?} batch decompressing past a {cap}-byte cap was accepted"
        );
    }
});
