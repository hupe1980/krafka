---
applyTo: "src/interceptor.rs"
description: "Use when editing interceptors: panic safety wrappers, credential exposure risk, and performance constraints."
---

# Interceptor Rules

## Panic Safety

All interceptor calls must go through the `safe_*` wrappers (`safe_on_send`, `safe_on_acknowledgement`, `safe_on_consume`, `safe_on_commit`).
Panics are caught and logged at `error!` — they must **never** crash the producer or consumer.

## Credential Exposure

- `on_send()` receives `&mut ProducerRecord` with **all headers** — headers may contain auth tokens.
- `on_acknowledgement()` error messages from auth failures may contain broker-echoed details.
- Never log full record contents in interceptor implementations without sanitization.

## Performance

- `on_send()` and `on_consume()` are called on the hot path — they must not block.
- Single interceptor per producer/consumer (not a chain); combine logic in one implementation.
