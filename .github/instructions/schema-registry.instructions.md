---
applyTo: "src/schema_registry/**"
description: "Use when editing schema registry code: wire format safety, credential redaction, cache semantics, feature gating, and two-registry symmetry."
---

# Schema Registry Module Rules

## Feature Gating

- **Always available (no feature):** Wire format encode/decode, types (`Schema`, `SchemaType`, `SubjectNameStrategy`, `GlueSchemaVersionId`, `GlueSchema`, `GlueCompression`, `GlueDataFormat`), traits (`SchemaRegistryClient`, `GlueSchemaRegistryClient`), caching wrappers (`CachedSchemaRegistry`, `CachedGlueSchemaRegistry`).
- **`schema-registry` feature:** `ConfluentSchemaRegistry` HTTP client, `ConfluentSchemaRegistryBuilder` (gates `reqwest`, `serde`, `serde_json`).
- **`aws-glue-schema-registry` feature:** `AwsGlueSchemaRegistry`, `AwsGlueSchemaRegistryBuilder` (gates `aws-sdk-glue`, `aws-config`).

When adding types, decide whether they belong in always-available core or behind a feature gate. Wire format and trait definitions must never require optional dependencies.

## Credential Safety

- `RegistryAuth` must **not** derive or implement `Debug` — credentials would leak.
- `ConfluentSchemaRegistry` and `AwsGlueSchemaRegistry` use custom `fmt::Debug` impls that redact secrets (print `basic(***)` / `bearer(***)`, never the actual values).
- Never log auth fields at any level, even `debug!` or `trace!`.
- When adding a new auth variant or client field holding secrets, verify all three rules above.

## Wire Format Safety

Both wire formats follow the same pattern: validate header → extract ID → return payload.

- **Confluent**: 5-byte header — magic byte `0x00` + 4-byte BE schema ID.
- **Glue**: 18-byte header — version byte `0x03` + compression byte + 16-byte UUID.

Validation rules (non-negotiable):
1. Check minimum length **before** any byte access.
2. Validate magic/version byte — reject unknown values.
3. For Glue: validate compression byte — reject unknown values.
4. `decode_*_bytes()` variants use `Bytes::slice()` for zero-copy on uncompressed payloads.
5. ZLIB decompression must be bounded (`MAX_DECOMPRESSED_SIZE = 128 MiB`) to prevent decompression bombs — use `.take()` on the decoder, matching the pattern in `protocol::record`.

When adding a new wire format variant, follow the `validate_*_header()` → decode pattern.

## Cache Semantics

Both `CachedSchemaRegistry` and `CachedGlueSchemaRegistry` follow identical patterns:

- **Immutable ID lookups** (`get_schema_by_id`, `get_schema_by_version_id`): Fast-path read lock, slow-path write lock. Once cached, entries never expire (IDs are immutable in both registries).
- **Mutable lookups** (`get_latest_schema`, `get_schema_by_version`): Always forward to inner client, but populate the ID cache so subsequent `get_schema_by_id` calls hit cache.
- **Mutations** (`register_schema`): Always forward, never cache (the returned ID can be cached by the caller via a subsequent get).
- **Lock discipline**: `parking_lot::RwLock` — read lock for cache hits, write lock only for inserts/clears. Never hold a lock across an `.await` point.

When adding a new trait method, decide whether its result is immutable (cache) or mutable (forward + optionally populate cache).

## Two-Registry Symmetry

Confluent and Glue follow parallel designs. When adding a feature to one, check if the other needs it too:

| Confluent | Glue |
|-----------|------|
| `SchemaRegistryClient` | `GlueSchemaRegistryClient` |
| `CachedSchemaRegistry<C>` | `CachedGlueSchemaRegistry<C>` |
| `encode_wire_format()` / `decode_wire_format()` | `encode_glue_wire_format()` / `decode_glue_wire_format()` |
| `decode_wire_format_bytes()` | `decode_glue_wire_format_bytes()` |
| `ConfluentSchemaRegistry` (HTTP) | `AwsGlueSchemaRegistry` (SDK) |

Both cached wrappers expose: `new()`, `with_capacity()`, `inner()`, `cache_len()`, `cache_is_empty()`, `clear_cache()`, custom `Debug`.

## URL & Subject Encoding (Confluent)

- Always strip trailing slash from base URL (both `new()` and builder).
- Always strip userinfo (`user:pass@`) from URLs via `sanitize_url()` to prevent credential leakage through `Debug` or logs. Log a `warn!` when stripping.
- Percent-encode subject names in URL path segments (`%`, `/`, ` `, `#`, `?`).
- Use the `SCHEMA_REGISTRY_CONTENT_TYPE` constant for all requests.

## Glue Registration Flow

The 4-step registration in `AwsGlueSchemaRegistry::register_schema` handles race conditions:

1. Check if definition already registered (`get_schema_by_definition`).
2. Try `register_schema_version` (schema exists, new version).
3. If step 2 fails and `auto_register` → `create_schema` (new schema).
4. If step 3 fails (race) → retry `register_schema_version`.

After registration, poll for `AVAILABLE` status via `wait_for_available()`. When modifying this flow, preserve all four steps and the race-condition fallback.

## Testing

- Both HTTP and SDK clients require external services — cannot be unit-tested directly.
- Test via mock implementations of the traits (see `MockRegistry`, `MockGlueRegistry` in tests).
- Wire format tests: roundtrip, empty payload, boundary values, error cases (too short, wrong magic byte, unknown compression).
- Cache tests: miss-then-hit, different IDs, clear, forward-and-populate pattern.
- Type assertions: `Send + Sync` on all public types, object safety on traits.

## Public API Checklist

- All public structs with fields → `#[non_exhaustive]`
- Builder pattern for clients with optional configuration
- `impl Into<String>` for string parameters in constructors/builders
- Custom `Debug` on client structs (redact secrets, show cache size)
