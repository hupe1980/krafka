---
applyTo: "src/auth/**"
description: "Use when editing auth code: credential safety, SASL handshake flows, SCRAM state machine, zeroization, and constant-time comparison."
---

# Auth Module Rules

## Credential Safety (Non-Negotiable)

- Every field holding a secret (`password`, `secret_access_key`, `session_token`, `token_value`, `salted_password`, `server_signature`) must derive `Zeroize` + `ZeroizeOnDrop`.
- All credential structs must override `fmt::Debug` to print `[REDACTED]` instead of the secret.
- Never log credentials at **any** level — not even `debug!` or `trace!`.
- Temporary buffers containing secrets (e.g., SASL auth bytes) must be wrapped in `Zeroizing<Vec<u8>>`.

When adding a new auth mechanism or field that holds sensitive data, verify all four rules above.

## SCRAM State Machine

States: `Initial → WaitingServerFirst → WaitingClientFinal → WaitingServerFinal → Complete | Failed`

- Invalid transitions must set `state = Failed` immediately — never silently ignored.
- Iteration count bounds: **min 4096, max 1,000,000** (prevents downgrade and DoS).
- Server nonce must start with client nonce (validated, not assumed).
- Signature verification uses `subtle::ConstantTimeEq` — never use `==` for HMAC comparison.

## TLS

- `rustls` only (no OpenSSL / native-tls).
- Insecure mode (`verify_server_cert = false`) requires the `danger-insecure-tls` crate feature. Without the feature, it is **rejected at runtime**. With the feature, it emits a `warn!` log and uses `NoServerCertVerifier` via `dangerous()` builder — intended only for local development. For production, use `with_ca_cert()`.
- File I/O for certs: use `load_certs_async()` / `spawn_blocking` in async contexts; sync variants only in tests.
- SNI hostname extraction must handle IPv6 brackets (`[::1]:9092`).

## General Auth Patterns

- Auth mechanisms use **enum dispatch** (no trait objects).
- All auth errors are `KrafkaError::auth(message)` — no custom auth error variants.
- Auth failures are **not retried** internally; the network layer handles reconnection.
- AWS MSK IAM signing uses system clock — no built-in skew tolerance.
