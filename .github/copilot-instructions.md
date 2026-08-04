# Krafka — Copilot Instructions

## Project

Pure-Rust async Kafka client. Tokio runtime, edition 2024, MSRV 1.88.

## Build & Test

```sh
cargo fmt && cargo clippy --all-targets && cargo test --lib
```

Run this after every change. All three must be clean before considering work done.

## Architecture

Layered: `protocol` → `network` → `metadata` → `consumer`/`producer`/`admin`.
See `site/content/docs/architecture.md` for internals, `site/content/docs/consumer.md` and `site/content/docs/producer.md` for client behavior.

Key types: `Offset = i64`, `PartitionId = i32`, `BrokerId = i32`.
`Consumer` requires a live broker — unit tests cannot construct one; test logic via extracted helpers or data-structure-level assertions.

## Self-Review Checklist

After writing or modifying code, do a second pass on every item below.
This is not optional — apply it before declaring a task complete.

- Doc comment on every changed field/method matches the new semantics
- Markdown docs in `site/content/docs/` updated in the same commit when behavior changes
- No `unwrap()` in non-test code unless immediately after an infallible check with a comment
- Adding a field to a public struct → mark it `#[non_exhaustive]` or use accessors
- Changing a return type → check all call sites (grep, not memory)
- `reset()` / `clear()` methods updated for all new fields
- Cleanup paths (revocation, unsubscribe, close) handle all new state
- All early-return paths return the correct shape (tuple arity, default values)
- No repeated `.to_string()` / `.clone()` on the same value — build once, reuse
- Credentials never logged, even at `debug!` level

## Rust Conventions

- `warn!` for recoverable errors, `debug!` for expected flow, `info!` for lifecycle events
- Prefer `if let` / `let else` over match for single-variant checks
- Error types via `thiserror`, not manual `impl Display`

## PR Review Readiness

Before submitting, verify:
1. `cargo fmt && cargo clippy --all-targets` — zero warnings
2. `cargo test --lib` — all pass
3. Changed public API → update `site/content/docs/` in the same commit
4. New metric → present in struct, Prometheus export, snapshot, reset, and `site/content/docs/metrics.md`
