//! Producer benchmarks.
//!
//! Run with: cargo bench --bench producer

use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

use krafka::protocol::{Compression, RecordBatch, RecordBatchBuilder};

/// Benchmark record batch encoding with different sizes.
fn bench_record_batch_encoding(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch_encoding");

    for batch_size in [1, 10, 100, 1000] {
        group.throughput(Throughput::Elements(batch_size as u64));

        group.bench_with_input(
            BenchmarkId::new("records", batch_size),
            &batch_size,
            |b, &size| {
                b.iter(|| {
                    let mut builder = RecordBatchBuilder::new();
                    for i in 0..size {
                        let key = format!("key-{}", i);
                        let value = format!("value-{}-with-some-additional-payload", i);
                        builder = builder.add_record(
                            Some(key.as_bytes().to_vec()),
                            Some(value.as_bytes().to_vec()),
                        );
                    }
                    let batch = builder.build();
                    black_box(batch)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark compression performance.
fn bench_compression(c: &mut Criterion) {
    let mut group = c.benchmark_group("compression");

    // Create a batch with 100 records
    let records: Vec<(String, String)> = (0..100)
        .map(|i| {
            (
                format!("key-{}", i),
                format!(
                    "value-{}-with-realistic-message-payload-that-is-compressible",
                    i
                ),
            )
        })
        .collect();

    for compression in [
        Compression::None,
        Compression::Gzip,
        Compression::Snappy,
        Compression::Lz4,
        Compression::Zstd,
    ] {
        group.throughput(Throughput::Elements(100));

        group.bench_with_input(
            BenchmarkId::new("codec", format!("{:?}", compression)),
            &compression,
            |b, &compression| {
                b.iter(|| {
                    let mut builder = RecordBatchBuilder::new().compression(compression);
                    for (key, value) in &records {
                        builder = builder.add_record(
                            Some(key.as_bytes().to_vec()),
                            Some(value.as_bytes().to_vec()),
                        );
                    }
                    let batch = builder.build();
                    black_box(batch)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark murmur2 hashing (used for partitioning).
fn bench_murmur2(c: &mut Criterion) {
    use krafka::producer::murmur2;

    let mut group = c.benchmark_group("murmur2");

    for key_size in [8, 32, 128, 512] {
        let key: Vec<u8> = (0..key_size).map(|i| (i % 256) as u8).collect();

        group.throughput(Throughput::Bytes(key_size as u64));
        group.bench_with_input(BenchmarkId::new("key_bytes", key_size), &key, |b, key| {
            b.iter(|| {
                let hash = murmur2(black_box(key));
                black_box(hash)
            });
        });
    }

    group.finish();
}

/// Benchmark varint encoding (hot path in protocol layer).
fn bench_varint(c: &mut Criterion) {
    use krafka::util::varint::{encode_signed_varint, encode_unsigned_varint};

    let mut group = c.benchmark_group("varint");

    let test_values: Vec<i32> = vec![0, 127, 16383, 2097151, i32::MAX];

    for &value in &test_values {
        group.bench_with_input(BenchmarkId::new("signed", value), &value, |b, &value| {
            b.iter(|| {
                let mut buf = BytesMut::with_capacity(8);
                encode_signed_varint(black_box(value), &mut buf);
                black_box(buf)
            });
        });
    }

    let unsigned_values: Vec<u32> = vec![0, 127, 16383, 2097151, u32::MAX];
    for &value in &unsigned_values {
        group.bench_with_input(BenchmarkId::new("unsigned", value), &value, |b, &value| {
            b.iter(|| {
                let mut buf = BytesMut::with_capacity(8);
                encode_unsigned_varint(black_box(value), &mut buf);
                black_box(buf)
            });
        });
    }

    group.finish();
}

/// Benchmark encode/decode roundtrip latency.
fn bench_roundtrip_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("roundtrip_latency");

    // Test with realistic message sizes
    for msg_size in [100, 1000, 10000] {
        let key = format!("key-{}", msg_size);
        let value: String = (0..msg_size)
            .map(|i| (b'a' + (i % 26) as u8) as char)
            .collect();

        group.throughput(Throughput::Bytes(msg_size as u64));
        group.bench_with_input(
            BenchmarkId::new("single_record_bytes", msg_size),
            &(key, value),
            |b, (key, value)| {
                b.iter(|| {
                    // Encode
                    let batch = RecordBatchBuilder::new()
                        .add_record(
                            Some(key.as_bytes().to_vec()),
                            Some(value.as_bytes().to_vec()),
                        )
                        .build();
                    let encoded = batch.encode().expect("encode failed");

                    // Decode
                    let mut buf = encoded;
                    let decoded = RecordBatch::decode(&mut buf).expect("decode failed");
                    black_box(decoded)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark partitioner performance (called for every message).
fn bench_partitioners(c: &mut Criterion) {
    use krafka::producer::{
        DefaultPartitioner, HashPartitioner, Partitioner, RoundRobinPartitioner, StickyPartitioner,
    };

    let mut group = c.benchmark_group("partitioners");

    let keys: Vec<Vec<u8>> = (0..1000)
        .map(|i| format!("key-{}", i).into_bytes())
        .collect();
    let partition_count = 32;

    // DefaultPartitioner with keys
    let default_partitioner = DefaultPartitioner::new();
    group.throughput(Throughput::Elements(1000));
    group.bench_function("default_keyed", |b| {
        b.iter(|| {
            for key in &keys {
                let p = default_partitioner.partition("topic", Some(key), partition_count);
                black_box(p);
            }
        });
    });

    // RoundRobinPartitioner
    let round_robin = RoundRobinPartitioner::new();
    group.bench_function("round_robin", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let p = round_robin.partition("topic", None, partition_count);
                black_box(p);
            }
        });
    });

    // StickyPartitioner
    let sticky = StickyPartitioner::new();
    group.bench_function("sticky", |b| {
        b.iter(|| {
            for _ in 0..1000 {
                let p = sticky.partition("topic", None, partition_count);
                black_box(p);
            }
        });
    });

    // HashPartitioner
    let hash_partitioner = HashPartitioner::new();
    group.bench_function("hash_keyed", |b| {
        b.iter(|| {
            for key in &keys {
                let p = hash_partitioner.partition("topic", Some(key), partition_count);
                black_box(p);
            }
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_record_batch_encoding,
    bench_compression,
    bench_murmur2,
    bench_varint,
    bench_roundtrip_latency,
    bench_partitioners
);
criterion_main!(benches);
