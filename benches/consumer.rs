//! Consumer benchmarks.
//!
//! Run with: cargo bench --bench consumer

#![allow(missing_docs, clippy::panic)]

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

use krafka::protocol::{Compression, LazyRecordBatch, RecordBatch};

fn ok<T, E: std::fmt::Display>(result: Result<T, E>) -> T {
    match result {
        Ok(value) => value,
        Err(err) => unreachable!("benchmark setup should not fail: {err}"),
    }
}

fn some<T>(value: Option<T>) -> T {
    match value {
        Some(value) => value,
        None => unreachable!("benchmark should contain at least one record"),
    }
}

/// Benchmark record batch decoding with different batch sizes.
fn bench_record_batch_decoding(c: &mut Criterion) {
    use krafka::protocol::RecordBatchBuilder;

    let mut group = c.benchmark_group("record_batch_decoding");

    for batch_size in [1, 10, 100, 500] {
        // Create a batch to decode
        let mut builder = RecordBatchBuilder::new().compression(Compression::None);
        for i in 0..batch_size {
            let key = format!("key-{i}");
            let value = format!("value-{i}-with-some-payload-data");
            builder = builder.add_record(
                Some(key.as_bytes().to_vec()),
                Some(value.as_bytes().to_vec()),
            );
        }
        let batch = builder.build();
        let encoded = ok(batch.encode());

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("records", batch_size),
            &encoded,
            |b, encoded| {
                b.iter(|| {
                    let mut buf = encoded.clone();
                    let decoded = ok(RecordBatch::decode(&mut buf));
                    black_box(decoded)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark decompression performance.
fn bench_decompression(c: &mut Criterion) {
    use krafka::protocol::RecordBatchBuilder;

    let mut group = c.benchmark_group("decompression");

    // Create records with compressible content
    let records: Vec<(String, String)> = (0..100)
        .map(|i| {
            (
                format!("key-{i}"),
                format!("value-{i}-with-realistic-compressible-message-payload-that-repeats"),
            )
        })
        .collect();

    for compression in [
        #[cfg(feature = "gzip")]
        Compression::Gzip,
        #[cfg(feature = "snappy")]
        Compression::Snappy,
        #[cfg(feature = "lz4")]
        Compression::Lz4,
        #[cfg(feature = "zstd")]
        Compression::Zstd,
    ] {
        let mut builder = RecordBatchBuilder::new().compression(compression);
        for (key, value) in &records {
            builder = builder.add_record(
                Some(key.as_bytes().to_vec()),
                Some(value.as_bytes().to_vec()),
            );
        }
        let batch = builder.build();
        let encoded = ok(batch.encode());

        group.throughput(Throughput::Elements(100));
        group.bench_with_input(
            BenchmarkId::new("codec", format!("{compression:?}")),
            &encoded,
            |b, encoded| {
                b.iter(|| {
                    let mut buf = encoded.clone();
                    let decoded = ok(RecordBatch::decode(&mut buf));
                    black_box(decoded)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark record iteration (simulates consumer record processing).
fn bench_record_iteration(c: &mut Criterion) {
    use krafka::protocol::RecordBatchBuilder;

    let mut group = c.benchmark_group("record_iteration");

    for batch_size in [10, 100, 1000] {
        let mut builder = RecordBatchBuilder::new();
        for i in 0..batch_size {
            let key = format!("key-{i}");
            let value = format!("value-{i}");
            builder = builder.add_record(
                Some(key.as_bytes().to_vec()),
                Some(value.as_bytes().to_vec()),
            );
        }
        let batch = builder.build();
        let encoded = ok(batch.encode());

        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("records", batch_size),
            &encoded,
            |b, encoded| {
                b.iter(|| {
                    let mut buf = encoded.clone();
                    let decoded = ok(RecordBatch::decode(&mut buf));
                    let mut count = 0;
                    for record in &decoded.records {
                        black_box(&record.key);
                        black_box(&record.value);
                        count += 1;
                    }
                    black_box(count)
                });
            },
        );
    }

    group.finish();
}

/// Benchmark lazy vs eager record batch decoding.
fn bench_lazy_vs_eager_decoding(c: &mut Criterion) {
    use krafka::protocol::RecordBatchBuilder;

    let mut group = c.benchmark_group("lazy_vs_eager");

    for batch_size in [10, 100, 500] {
        #[cfg(feature = "lz4")]
        let compression = Compression::Lz4;
        #[cfg(not(feature = "lz4"))]
        let compression = Compression::None;

        let mut builder = RecordBatchBuilder::new().compression(compression);
        for i in 0..batch_size {
            let key = format!("key-{i}");
            let value = format!("value-{i}-with-payload-data");
            builder = builder.add_record(
                Some(key.as_bytes().to_vec()),
                Some(value.as_bytes().to_vec()),
            );
        }
        let batch = builder.build();
        let encoded = ok(batch.encode());

        // Eager: decode all records immediately
        group.bench_with_input(
            BenchmarkId::new("eager", batch_size),
            &encoded,
            |b, encoded| {
                b.iter(|| {
                    let mut buf = encoded.clone();
                    let decoded = ok(RecordBatch::decode(&mut buf));
                    black_box(decoded.records.len())
                });
            },
        );

        // Lazy: decode only header (records parsed on demand)
        group.bench_with_input(
            BenchmarkId::new("lazy_header_only", batch_size),
            &encoded,
            |b, encoded| {
                b.iter(|| {
                    let mut buf = encoded.clone();
                    let lazy = ok(LazyRecordBatch::decode(&mut buf));
                    black_box(lazy.len())
                });
            },
        );

        // Lazy with first record access
        group.bench_with_input(
            BenchmarkId::new("lazy_first_record", batch_size),
            &encoded,
            |b, encoded| {
                b.iter(|| {
                    let mut buf = encoded.clone();
                    let lazy = ok(LazyRecordBatch::decode(&mut buf));
                    let first = ok(some(lazy.records().next()));
                    black_box(first.key)
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_record_batch_decoding,
    bench_decompression,
    bench_record_iteration,
    bench_lazy_vs_eager_decoding
);
criterion_main!(benches);
