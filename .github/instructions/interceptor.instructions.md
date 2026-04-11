---
applyTo: "src/interceptor.rs"
description: "Use when editing interceptors: panic safety wrappers, credential exposure risk, and performance constraints."
---

# Interceptor Rules

## Panic Safety

All interceptor calls must go through the `safe_*` wrappers (`safe_on_send`, `safe_on_acknowledgement`, `safe_on_consume`, `safe_on_commit`).
Panics are caught and logged — they must **never** crash the producer or consumer.

**Log levels** (matching Java Kafka client convention):
- Callback panics (`on_send`, `on_acknowledgement`, `on_consume`, `on_commit`) → `warn!` (recoverable, chain continues)
- `close()` panics → `error!` (lifecycle failure)

Interceptor chains (`ProducerInterceptorChain`, `ConsumerInterceptorChain`) provide per-interceptor panic isolation: a panic in one interceptor is caught and logged, and the remaining interceptors still execute. The outer `safe_*` wrapper provides belt-and-suspenders protection.

## Credential Exposure

- `on_send()` receives `&mut ProducerRecord` with **all headers** — headers may contain auth tokens.
- `on_acknowledgement()` error messages from auth failures may contain broker-echoed details.
- Never log full record contents in interceptor implementations without sanitization.
- **Never log `interceptor = ?interceptor`** — user-provided `Debug` implementations may expose secrets (API keys, tokens, endpoints). Log `chain_index` and `chain_len` instead to identify the panicking interceptor.
- Add contextual domain data (topic, partition) where available, matching Java's approach.

## Performance

- `on_send()` and `on_consume()` are called on the hot path — they must not block.
- When chaining, each interceptor invocation includes a `catch_unwind` boundary; keep chains short (< 10 interceptors) to avoid measurable overhead.

## Chain Semantics

- Interceptors execute in registration order (insertion order of `add_interceptor()` calls).
- For `on_send()`, each interceptor sees the record as modified by preceding interceptors.
- `interceptor()` replaces the chain with a single interceptor; `add_interceptor()` appends.
- A single interceptor is stored directly (no chain wrapper); chains of 2+ are wrapped in `ProducerInterceptorChain` / `ConsumerInterceptorChain`.
