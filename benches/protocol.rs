//! Protocol layer benchmarks.
//!
//! Run with: cargo bench --bench protocol
//!
//! These benchmarks measure the performance of protocol primitives,
//! header encoding/decoding, and metadata lookups - all critical hot paths.

use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

use krafka::util::varint::{
    decode_signed_varint, decode_unsigned_varint, encode_signed_varint, encode_unsigned_varint,
};

/// Benchmark primitive encoding operations (hot path).
fn bench_primitives_encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitives_encode");

    // i32 encoding (most common)
    group.bench_function("i32", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(4);
            buf.extend_from_slice(&black_box(12345678_i32).to_be_bytes());
            black_box(buf)
        });
    });

    // i64 encoding
    group.bench_function("i64", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(8);
            buf.extend_from_slice(&black_box(1234567890123456_i64).to_be_bytes());
            black_box(buf)
        });
    });

    // bool encoding
    group.bench_function("bool", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(1);
            buf.extend_from_slice(&[if black_box(true) { 1 } else { 0 }]);
            black_box(buf)
        });
    });

    group.finish();
}

/// Benchmark primitive decoding operations (hot path).
fn bench_primitives_decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("primitives_decode");

    // i32 decoding - use static byte array
    static I32_BYTES: [u8; 4] = 12345678_i32.to_be_bytes();
    group.bench_function("i32", |b| {
        b.iter(|| {
            let buf = &I32_BYTES;
            let val = i32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
            black_box(val)
        });
    });

    // i64 decoding - use static byte array
    static I64_BYTES: [u8; 8] = 1234567890123456_i64.to_be_bytes();
    group.bench_function("i64", |b| {
        b.iter(|| {
            let buf = &I64_BYTES;
            let val = i64::from_be_bytes([
                buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
            ]);
            black_box(val)
        });
    });

    group.finish();
}

/// Benchmark varint encoding/decoding (used extensively in protocol).
fn bench_varint_detailed(c: &mut Criterion) {
    let mut group = c.benchmark_group("varint_detailed");

    // Test different value ranges
    let test_cases: Vec<(&str, i32, u32)> = vec![
        ("1_byte", 63, 127),
        ("2_byte", 8191, 16383),
        ("3_byte", 1048575, 2097151),
        ("5_byte_max", i32::MAX, u32::MAX),
    ];

    for (name, signed, unsigned) in test_cases {
        // Signed encoding
        group.bench_with_input(
            BenchmarkId::new("encode_signed", name),
            &signed,
            |b, &value| {
                b.iter(|| {
                    let mut buf = BytesMut::with_capacity(5);
                    encode_signed_varint(black_box(value), &mut buf);
                    black_box(buf)
                });
            },
        );

        // Unsigned encoding
        group.bench_with_input(
            BenchmarkId::new("encode_unsigned", name),
            &unsigned,
            |b, &value| {
                b.iter(|| {
                    let mut buf = BytesMut::with_capacity(5);
                    encode_unsigned_varint(black_box(value), &mut buf);
                    black_box(buf)
                });
            },
        );

        // Signed decoding
        let mut encoded = BytesMut::new();
        encode_signed_varint(signed, &mut encoded);
        let encoded_signed = encoded.freeze();
        group.bench_with_input(
            BenchmarkId::new("decode_signed", name),
            &encoded_signed,
            |b, encoded| {
                b.iter(|| {
                    let mut buf = encoded.clone();
                    let val = decode_signed_varint(&mut buf).unwrap();
                    black_box(val)
                });
            },
        );

        // Unsigned decoding
        let mut encoded = BytesMut::new();
        encode_unsigned_varint(unsigned, &mut encoded);
        let encoded_unsigned = encoded.freeze();
        group.bench_with_input(
            BenchmarkId::new("decode_unsigned", name),
            &encoded_unsigned,
            |b, encoded| {
                b.iter(|| {
                    let mut buf = encoded.clone();
                    let val = decode_unsigned_varint(&mut buf).unwrap();
                    black_box(val)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark CRC32C checksum (used in record batch validation).
fn bench_crc32c(c: &mut Criterion) {
    use krafka::util::crc32c;

    let mut group = c.benchmark_group("crc32c");

    for size in [64, 256, 1024, 4096, 16384] {
        let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();

        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("bytes", size), &data, |b, data| {
            b.iter(|| {
                let checksum = crc32c(black_box(data));
                black_box(checksum)
            });
        });
    }

    group.finish();
}

/// Benchmark request header encoding (called for every request).
fn bench_request_header(c: &mut Criterion) {
    use krafka::protocol::{ApiKey, RequestHeader};

    let mut group = c.benchmark_group("request_header");

    let header = RequestHeader::new(ApiKey::Produce, 3, 12345).with_client_id("krafka-benchmark");

    // v0 header (minimal)
    group.bench_function("encode_v0", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(32);
            header.encode_v0(&mut buf).unwrap();
            black_box(buf)
        });
    });

    // v1 header (with client_id)
    group.bench_function("encode_v1", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            header.encode_v1(&mut buf).unwrap();
            black_box(buf)
        });
    });

    // v2 header (with tagged fields)
    group.bench_function("encode_v2", |b| {
        b.iter(|| {
            let mut buf = BytesMut::with_capacity(64);
            header.encode_v2(&mut buf).unwrap();
            black_box(buf)
        });
    });

    group.finish();
}

/// Benchmark error code conversions (called on every response).
fn bench_error_code(c: &mut Criterion) {
    use krafka::error::ErrorCode;

    let mut group = c.benchmark_group("error_code");

    // Common codes
    let codes: Vec<i16> = vec![0, 1, 3, 6, 7, 36, 50];

    group.bench_function("from_i16", |b| {
        b.iter(|| {
            for &code in &codes {
                let ec = ErrorCode::from_i16(black_box(code));
                black_box(ec);
            }
        });
    });

    group.bench_function("to_i16", |b| {
        let error_codes: Vec<ErrorCode> = codes.iter().map(|&c| ErrorCode::from_i16(c)).collect();
        b.iter(|| {
            for ec in &error_codes {
                let code = ec.to_i16();
                black_box(code);
            }
        });
    });

    group.bench_function("is_retriable", |b| {
        let error_codes: Vec<ErrorCode> = codes.iter().map(|&c| ErrorCode::from_i16(c)).collect();
        b.iter(|| {
            for ec in &error_codes {
                let retriable = ec.is_retriable();
                black_box(retriable);
            }
        });
    });

    group.finish();
}

/// Benchmark API key conversions.
fn bench_api_key(c: &mut Criterion) {
    use krafka::protocol::ApiKey;

    let mut group = c.benchmark_group("api_key");

    let api_keys: Vec<i16> = vec![0, 1, 2, 3, 8, 9, 10, 11, 18, 19, 32];

    group.bench_function("from_i16", |b| {
        b.iter(|| {
            for &key in &api_keys {
                let ak = ApiKey::from_i16(black_box(key));
                black_box(ak);
            }
        });
    });

    group.bench_function("to_i16", |b| {
        let keys: Vec<ApiKey> = api_keys.iter().map(|&k| ApiKey::from_i16(k)).collect();
        b.iter(|| {
            for ak in &keys {
                let code = ak.to_i16();
                black_box(code);
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_primitives_encode,
    bench_primitives_decode,
    bench_varint_detailed,
    bench_crc32c,
    bench_request_header,
    bench_error_code,
    bench_api_key
);
criterion_main!(benches);
