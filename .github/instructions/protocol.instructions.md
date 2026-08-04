---
applyTo: "src/protocol/**"
description: "Use when editing Kafka protocol encode/decode: version dispatch, wire format safety, and TryEncode validation."
---

# Protocol Module Rules

## Version Dispatch

All request/response types implement `VersionedEncode` and `VersionedDecode`.
When adding a new version (`encode_vN` / `decode_vN`):
- Add the version to the `match` arm in both traits
- Update `*_MAX` constant in `src/protocol/mod.rs`
- Unsupported versions must return a descriptive protocol error, not panic

## Wire Format Safety

- `TryEncode` for fallible encoding; `Encode` delegates to `try_encode().expect(...)`
- `ProducerRecord::validate()` checks wire-format limits at the API boundary
- Never `panic!()` in encode/decode paths — return `Err` instead

## Field Layout Changes

When adding fields to a request/response struct, check which versions include that field.
Fields not present in older versions must use sentinel defaults (typically `-1` for i32/i64).
See `site/content/docs/protocol.md` for version-to-field mapping.
