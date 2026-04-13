---
applyTo: "src/interceptor.rs"
description: "Use when editing interceptors: fallible return types, panic safety wrappers, credential exposure risk, and performance constraints."
---

# Interceptor Rules

## Error Contract

All interceptor trait methods return `InterceptorResult` (`Result<(), Box<dyn Error + Send + Sync>>`).
This is the **primary** error channel — interceptor authors should return `Err` for expected failures
(e.g. a metrics backend is down, a serialization error).

Reserve panics for genuine bugs. `catch_unwind` is a **safety net**, not the normal error path.

Three severity levels, all non-fatal (chain always continues):

| Outcome | Log level | Meaning |
|---------|-----------|---------|
| `Ok(())` | — | Normal |
| `Err(e)` | `warn!` | Expected failure — error message is logged |
| panic | `error!` (payload **redacted**) | Bug — `catch_unwind` catches it |

## Panic Safety

All interceptor calls must go through the `safe_*` wrappers (`safe_on_send`, `safe_on_acknowledgement`, `safe_on_consume`, `safe_on_commit`).
The wrappers use a three-arm `match catch_unwind(...)` pattern: `Ok(Ok(()))` → normal, `Ok(Err(e))` → `warn!`, `Err(_)` → `error!`.
Panics must **never** crash the producer or consumer.

**Log levels** (matching Java Kafka client convention):
- Callback errors (`on_send`, `on_acknowledgement`, `on_consume`, `on_commit`) → `warn!` (recoverable, chain continues)
- Callback panics → `error!` (payload redacted, chain continues)
- `close()` errors → `warn!`; `close()` panics → `error!` (lifecycle failure)

Interceptor chains (`ProducerInterceptorChain`, `ConsumerInterceptorChain`) provide per-interceptor error and panic isolation: a failure in one interceptor is caught and logged, and the remaining interceptors still execute. The outer `safe_*` wrapper provides belt-and-suspenders protection.

## Credential Exposure

- `on_send()` receives `&mut ProducerRecord` with **all headers** — headers may contain auth tokens.
- `on_acknowledgement()` error messages from auth failures may contain broker-echoed details.
- Never log full record contents in interceptor implementations without sanitization.
- **Never log `interceptor = ?interceptor`** — user-provided `Debug` implementations may expose secrets (API keys, tokens, endpoints). Log `chain_index` and `chain_len` instead to identify the failing interceptor.
- **Never log panic payloads** — they may contain arbitrary data. Log a fixed "interceptor panicked" message with chain index only.
- Add contextual domain data (topic, partition) where available, matching Java's approach.

## Performance

- `on_send()` and `on_consume()` are called on the hot path — they must not block.
- When chaining, each interceptor invocation includes a `catch_unwind` boundary; keep chains short (< 10 interceptors) to avoid measurable overhead.

## Chain Semantics

- Interceptors execute in registration order (insertion order of `add_interceptor()` calls).
- For `on_send()`, each interceptor sees the record as modified by preceding interceptors.
- `interceptor()` replaces the chain with a single interceptor; `add_interceptor()` appends.
- A single interceptor is stored directly (no chain wrapper); chains of 2+ are wrapped in `ProducerInterceptorChain` / `ConsumerInterceptorChain`.
