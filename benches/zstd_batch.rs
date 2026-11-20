//! Zstd record batch encode (produce) and decode (fetch) at the codec
//! default level, the producer's default.
//!
//! Run with: cargo bench --features internal,zstd --bench zstd_batch

#![allow(missing_docs, clippy::panic)]

use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};

use krafka::__private::protocol::{Compression, RecordBatch, RecordBatchBuilder};

/// Uncompressed payload sizes (keys plus values) to benchmark.
const SIZES: [(&str, usize); 3] = [
    ("1KiB", 1 << 10),
    ("16KiB", 16 << 10),
    ("256KiB", 256 << 10),
];

fn ok<T, E: std::fmt::Display>(result: Result<T, E>) -> T {
    match result {
        Ok(value) => value,
        Err(err) => unreachable!("benchmark setup should not fail: {err}"),
    }
}

/// Xorshift64 with a fixed seed, so every run sees the same records.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn pick<T: Copy>(&mut self, items: &[T]) -> T {
        items[self.below(items.len() as u64) as usize]
    }
}

/// JSON event records keyed by user, until keys plus values reach `target`
/// bytes.
fn event_records(target: usize) -> Vec<(Bytes, Bytes)> {
    const EVENTS: [&str; 7] = [
        "page_view",
        "click",
        "add_to_cart",
        "checkout",
        "login",
        "logout",
        "search",
    ];
    const PATHS: [&str; 7] = [
        "/",
        "/products",
        "/products/detail",
        "/cart",
        "/checkout",
        "/account/settings",
        "/search",
    ];
    const REGIONS: [&str; 5] = [
        "us-east-1",
        "us-west-2",
        "eu-west-1",
        "eu-central-1",
        "ap-southeast-1",
    ];
    const AGENTS: [&str; 4] = [
        "Mozilla/5.0 (X11; Linux x86_64)",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_5)",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_5 like Mac OS X)",
        "okhttp/4.12.0",
    ];
    const STATUS: [u16; 10] = [200, 200, 200, 200, 200, 201, 304, 400, 404, 500];

    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    let mut ts = 1_700_000_000_000u64;
    let mut records = Vec::new();
    let mut total = 0;
    while total < target {
        ts += rng.below(50);
        let user = rng.below(50_000);
        let key = format!("user-{user}");
        let value = format!(
            "{{\"ts\":{ts},\"user_id\":{user},\"session\":\"{:016x}\",\"event\":\"{}\",\
             \"path\":\"{}\",\"item_id\":{},\"status\":{},\"latency_ms\":{},\
             \"region\":\"{}\",\"user_agent\":\"{}\"}}",
            rng.next(),
            rng.pick(&EVENTS),
            rng.pick(&PATHS),
            rng.below(10_000),
            rng.pick(&STATUS),
            5 + rng.below(400),
            rng.pick(&REGIONS),
            rng.pick(&AGENTS),
        );
        total += key.len() + value.len();
        records.push((Bytes::from(key), Bytes::from(value)));
    }
    records
}

fn zstd_batch(records: &[(Bytes, Bytes)]) -> RecordBatch {
    let mut builder = RecordBatchBuilder::new()
        .compression(Compression::Zstd)
        .base_timestamp(1_700_000_000_000);
    for (key, value) in records {
        builder = builder.add_record(Some(key.clone()), Some(value.clone()));
    }
    builder.build()
}

fn bench_zstd_batch(c: &mut Criterion) {
    let mut encode = c.benchmark_group("krafka_zstd_encode");
    let mut cases = Vec::new();
    for (label, size) in SIZES {
        let records = event_records(size);
        let raw: usize = records.iter().map(|(k, v)| k.len() + v.len()).sum();
        let batch = zstd_batch(&records);

        let encoded = ok(batch.encode());
        eprintln!(
            "krafka zstd {label}: {} records, {raw} B keys+values -> {} B record batch",
            records.len(),
            encoded.len()
        );
        let decoded = ok(RecordBatch::decode(&mut encoded.clone()));
        assert_eq!(decoded.records.len(), records.len());

        encode.throughput(Throughput::Bytes(raw as u64));
        encode.bench_with_input(BenchmarkId::from_parameter(label), &batch, |b, batch| {
            b.iter(|| black_box(ok(batch.encode())));
        });
        cases.push((label, raw, encoded));
    }
    encode.finish();

    let mut decode = c.benchmark_group("krafka_zstd_decode");
    for (label, raw, encoded) in &cases {
        decode.throughput(Throughput::Bytes(*raw as u64));
        decode.bench_with_input(BenchmarkId::from_parameter(label), encoded, |b, encoded| {
            b.iter(|| black_box(ok(RecordBatch::decode(&mut encoded.clone()))));
        });
    }
    decode.finish();
}

criterion_group!(benches, bench_zstd_batch);
criterion_main!(benches);
