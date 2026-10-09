# Fuzz Testing for Krafka

cargo-fuzz (libFuzzer) targets for the parsers that read untrusted input: what
a broker, a token endpoint or another group member sends.

## Prerequisites

```sh
cargo install cargo-fuzz
rustup install nightly
```

The fuzz crate builds krafka with `unstable-protocol`, `internal` (for
`krafka::__private`), `oauth-oidc` and `zstd`.

## Targets

| Target | What it fuzzes |
|--------|----------------|
| `fuzz_response_decode` | Every response decoder at every version in `api_versions!` (versions come from `SUPPORTED_API_VERSIONS`), the pinned SASL handshake/authenticate decoders, and the embedded consumer-protocol subscription and assignment blobs |
| `fuzz_record_batch` | `RecordBatchHeader::peek` and `RecordBatch::decode` |
| `fuzz_decompression_cap` | Record-batch decompression under a small cap in every codec: arbitrary compressed bytes behind a valid header never yield more than the cap, and a batch the crate compressed from a payload over the cap is refused |
| `fuzz_oidc_http` | The OIDC token client's HTTP/1.1 response parser: an accepted response has no head line over the line cap, no more headers than the header cap, and no body over the body cap |
| `fuzz_header_primitives` | Response headers and the protocol primitives (varints, strings, bytes, tagged fields) |
| `fuzz_kafka_array` | `KafkaArray` decode and compact decode |
| `fuzz_request_encode` | Request encoding is total and deterministic |
| `fuzz_scram` | SCRAM server-first parsing, which runs before authentication |

`just fuzz-list` prints the targets; `just fuzz <target> [seconds]` runs one
(default 60 s).

## Seeds and corpus

`fuzz/seeds/<target>/` holds the committed seed inputs, read on every run.
`fuzz_response_decode` has one seed per (API, version) pair, named
`<Api>-v<N>`, whose first two bytes select that pair; after a change to
`api_versions!` regenerate them with
`python3 xtask/fuzz_coverage.py --write-seeds`. Add a crash reproducer as a
seed once the crash is fixed. Seeds grow only by reviewed change.

`fuzz/corpus/<target>/` is the working corpus the fuzzer grows; it is not
committed.

## Checks

`just fuzz-coverage` (part of `just ci`) fails when an API in `api_versions!`
has no arm in `fuzz_response_decode`, when a (API, version) pair has no seed,
or when a target has no `[[bin]]` or no seeds. CI runs every target for 60 s
on each pull request, and `fuzz-nightly.yml` for 30 minutes per target.

## Crashes

A crash, timeout or out-of-memory exits non-zero and leaves the input in
`fuzz/artifacts/<target>/`. Reproduce it with:

```sh
cd fuzz
cargo +nightly fuzz run <target> artifacts/<target>/<crash-file>
```

## Bounds under test

A broker can send arbitrary bytes, so no decoder may panic, hang or allocate
without bound. Array decode loops are bounded by `MAX_DECODE_ARRAY_LEN` and by
the bytes available, record counts by the bytes that hold them, decompression
by `RecordBatch::decode_with_limit`'s cap, and the OIDC HTTP parser by its
line, header-count, trailer and body caps.
