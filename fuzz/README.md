# Fuzz Testing for Krafka

This directory contains fuzz testing targets for the krafka protocol layer
using [cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) (libFuzzer).

## Prerequisites

```sh
cargo install cargo-fuzz
```

Requires nightly Rust:

```sh
rustup install nightly
```

## Available Targets

| Target | Description |
|--------|-------------|
| `fuzz_kafka_array` | `KafkaArray` decode and decode_compact with random bytes |
| `fuzz_record_batch` | `RecordBatch` and `LazyRecordBatch` decode with random bytes |
| `fuzz_response_decode` | Key response types (`ProduceResponse`, `FetchResponse`, `MetadataResponse`, `CreateTopicsResponse`, `DeleteTopicsResponse`) across multiple protocol versions |

## Running

Run a specific target (default: runs until stopped with Ctrl+C):

```sh
cd fuzz
cargo +nightly fuzz run fuzz_kafka_array
cargo +nightly fuzz run fuzz_record_batch
cargo +nightly fuzz run fuzz_response_decode
```

Run with a time limit:

```sh
cargo +nightly fuzz run fuzz_kafka_array -- -max_total_time=300
```

Run all targets sequentially (5 minutes each):

```sh
for target in fuzz_kafka_array fuzz_record_batch fuzz_response_decode; do
    echo "=== Running $target ==="
    cargo +nightly fuzz run "$target" -- -max_total_time=300
done
```

## Corpus

Fuzzer corpus data is stored in `fuzz/corpus/<target>/`. The corpus is
reused across runs and grows as the fuzzer discovers new code paths.

## Interpreting Results

If a crash is found, the reproducing input is saved to `fuzz/artifacts/<target>/`.
Reproduce it with:

```sh
cargo +nightly fuzz run fuzz_kafka_array fuzz/artifacts/fuzz_kafka_array/<crash-file>
```

## Design

These targets exercise the protocol decode paths — the primary attack surface
for a Kafka client. A malicious or buggy broker could send arbitrary bytes in
response frames; the decoder must never panic, hang, or consume unbounded
resources.

The `MAX_DECODE_ARRAY_LEN` constant (100,000) bounds all decode loops, and
these fuzz targets verify that bound is effective under adversarial input.
