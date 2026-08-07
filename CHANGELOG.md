# Changelog

All notable changes to krafka are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).
Versioning is [Semantic Versioning](https://semver.org/spec/v2.0.0.html); while
the crate is pre-1.0, a **minor** bump may carry breaking changes and the
`Breaking` section of each release lists them exhaustively.

Entries before 0.17.0 were reconstructed from the release history and the
`Upgrading` sections that previously lived in `README.md`. They are summaries,
not a complete record.

## [Unreleased]

Nothing yet.

## [0.18.0] — 2026-08-07

Schema-registry support is removed from krafka and replaced by a generic
serialization hook. This is a scope change, not a capability loss: the registry
client now lives in [`schemreg`](https://crates.io/crates/schemreg), which also
has native Apicurio support and real Avro / Protobuf / JSON codecs that krafka
never had.

### Testing

- **`just mutants`** — scoped mutation testing over the invariant-dense modules
  (sequence arithmetic, the in-flight barrier, varint codecs, fetch sessions).
  Deliberately not part of `ci`: 8 200 mutants across the crate is roughly
  45 CPU-hours, and the timing-based fake-broker tests turn many mutants into
  timeouts rather than failures.

  It found that **four separate corruptions of `is_newer_sequence` passed the
  whole test suite**. That function decides whether a broker acknowledgement
  moves `last_acked_sequence` forward, which feeds `can_reset_after_out_of_order`
  and `reset_sequence`. Every existing test used `candidate == 0`, `last == 0`,
  or values small enough that subtracting, adding and dividing all landed on the
  same side of the half-sequence-space threshold — so the assertions held for
  arithmetic that was wrong. Three new tests pin the forward branch, the wrap
  branch and the exact-half-space boundary, chosen so each mutation flips the
  answer rather than just an intermediate.

  It also caught a missing assertion: `take_reinit_request` resets the producer
  epoch alongside the ID, but `is_initialized()` only inspects the ID, so a
  stale epoch was invisible to the test.

  Running it over the rest of the scoped set found five more gaps, all in code
  that fails *quietly* when it is wrong — which is why nothing had noticed:

  - **`SessionKey::from_request` never tested a zero UUID.** A zero topic ID
    means "no topic ID" (Fetch v12 and below), not "the topic whose ID is
    zero". Three separate corruptions of that guard survived; any of them would
    have collapsed every pre-v13 topic onto the single key `Uuid([0; 16])`, so
    one topic's partition state would overwrite another's and the incremental
    diff would describe the wrong topic.
  - **`update_from_response` stored the UUID under the same rule, also
    untested.** It is what `FetchForgottenTopic` entries carry, so a zero UUID
    stored as real would tell the broker to forget topic `0`.
  - **`next_epoch` could have returned a constant.** The fetch-session epoch is
    how the broker detects a desynchronised session; a constant one draws
    `INVALID_FETCH_SESSION_EPOCH`, the client resets, and every fetch silently
    degrades to a full one — KIP-227's entire benefit lost with no error
    surfaced.
  - **`InFlightBarrier::is_closing` and its `Debug` impl** were both
    unasserted. The barrier is what three shutdown paths block on, so its
    `Debug` output is the first thing read when one appears to hang.
  - **The UUID version/variant test was a coin flip.** It asserted the nibbles
    of one random UUID, so a corrupted variant mask (`& 0x3F` → `| 0x3F`, which
    leaves bit 6 random) passed half the time. The bit-stamping is now a
    separate function checked against all 256 input bytes.

  Not every surviving mutant is a defect. Three of them were provably
  *equivalent* — `base + jitter` vs `base - jitter` over a symmetric jitter
  range, and two `|` → `^` swaps on bits the preceding mask had already
  cleared, identical for all 256 inputs. Those are left alone deliberately;
  mutation score is a diagnostic, not a target.

### Fixed

- **`BackoffPolicy::calculate_backoff` jumped to `max_backoff` for a
  slow-growing multiplier.** It short-circuited whenever
  `multiplier > 1.0 && exponent >= 1024`, to avoid "evaluating `powi` with a
  large exponent". Both halves of that were wrong: `powi` is repeated squaring,
  so even `i32::MAX` costs ~31 multiplications, and for a multiplier just above
  1.0 the series is nowhere near the ceiling at attempt 1025 — with
  `multiplier = 1.0000001` a 100 ms initial backoff jumped straight to the
  full 10 s ceiling instead of the ~100.01 ms it had actually reached.

  The guard is removed rather than corrected: `.min(ceiling)` already carries
  every case it was covering. A growing multiplier overflows `powi` to `+inf`
  and `inf.min(ceiling)` is the ceiling; a shrinking one underflows to `0.0`
  and the existing floor lifts it back to `initial_backoff`; a non-finite one
  yields `NaN`, and `f64::min` returns the non-`NaN` operand. Mutation testing
  found it: negating the `||` changed nothing any test could observe.

- **A share-consumer `commit_sync()` or `close()` could strand
  acknowledgements.** `poll()` drains every entry out of `pending_acks` into a
  guard for the duration of its `ShareFetch`, so during that window the map is
  empty. A concurrent flush took nothing, reported success, and left the
  acknowledgements to be restored by the guard a moment later with nobody left
  to send them — so records the application had explicitly acknowledged were
  redelivered anyway.

  This is the documented shutdown sequence, not an exotic race: `wakeup()`
  followed by `close()`, where `wakeup()` does not wait for the poll it
  interrupts to unwind. Both flush paths now wait on an in-flight barrier
  first — the same fix as the transactional producer's, and the same barrier
  type, which moved to `crate::barrier` now that two subsystems use it.

### Breaking

- **`krafka::schema_registry` is gone**, along with the `schema-registry` and
  `aws-glue-schema-registry` features and the `aws-sdk-glue` dependency. 6 487
  lines of Confluent + AWS Glue registry client, wire formats, caching and
  subject strategies are no longer part of this crate.

  A schema registry is a different service with a different protocol, auth model
  and release cadence, and every comparable client draws the line in the same
  place: Java's `kafka-clients` has no registry support (`kafka-avro-serializer`
  is a separate artifact), librdkafka has none (`libschemaregistry` is a
  separate library), and franz-go keeps `pkg/sr` out of `kgo`. Carrying it here
  meant a registry API change could force a Kafka client release.

  Move to `schemreg` and bridge it with a newtype — the adapter is about twenty
  lines and is written out in the
  [Cookbook](https://hupe1980.github.io/krafka/docs/cookbook/#use-a-schema-registry).

- **`SchemaEncoder` / `SchemaDecoder` are now `serdes::Serializer` /
  `serdes::Deserializer`**, in the new `krafka::serdes` module. The trait shapes
  are unchanged apart from the method names (`encode` → `serialize`,
  `decode` → `deserialize`); only the naming and the home moved. They are the
  equivalent of Java's `key.serializer` / `value.serializer`: krafka owns the
  place the transformation happens, the ecosystem owns the transformations.

  Because the traits are plain `Bytes -> Bytes`, they now also cover envelope
  encryption, application-level compression, or a bare `serde_json` round-trip.

- **Builder setters renamed** to match: `key_encoder` → `key_serializer`,
  `value_encoder` → `value_serializer` (both producers), `key_decoder` →
  `key_deserializer`, `value_decoder` → `value_deserializer` (consumer).

- **`KrafkaError::SchemaRegistry` is now `KrafkaError::Http`**, with
  `schema_registry()` / `schema_registry_with_source()` becoming `http()` /
  `http_with_source()`. The variant only ever described failures from the
  built-in HTTP client, which now serves the OIDC token provider alone.

### Changed

- The built-in HTTP/1.1 client (`src/http.rs`) is compiled for `oauth-oidc`
  only. It stays because the OIDC token provider uses it — removing the registry
  client did not remove the need for an HTTP client, and claiming otherwise
  would have been the easy overstatement here.


## [0.17.0] — 2026-08-07

Ten defects fixed in the consumer's fetch-to-delivery path, and the read path
turned into a real prefetch pipeline. Two breaking changes, both in the
consumer's offset accessors — see **Breaking** below before upgrading.

### Breaking

- **`Consumer::cached_end_offset` is now isolation-aware.** Under
  `IsolationLevel::ReadCommitted` it returns the **last stable offset** rather
  than the high watermark, because the broker will not deliver a record at or
  above the LSO — measuring against the high watermark reported lag the
  consumer could never close. Callers that specifically want the log-end offset
  should use the new `Consumer::cached_high_watermark`. Behaviour under
  `ReadUncommitted` (the default) is unchanged.

- **`Consumer::position()` reports the *delivered* offset, not the fetch
  position.** The consumer reads ahead of delivery, so the two differ by
  whatever is parked in the receive buffer. `position()` now returns the offset
  of the next record that will be handed to the application — the same value a
  commit writes — so `position()` and `commit()` can never disagree. The
  read-ahead value is available as the new `Consumer::fetch_position()`.

### Fixed

- **A `seek()` could move the consumer group's committed offset *backwards*.**
  `seek`, `seek_many`, `seek_to_beginning`, `seek_to_end`, `seek_to_timestamp`
  and the `auto.offset.reset` path left already-fetched records in the receive
  buffer. Because a commit is clamped down to the lowest still-buffered offset
  — correct, and what stops an undelivered record from being acknowledged — a
  stale buffered record dragged the commit back to *before* the seek. After
  `seek_to_end()` on a partition with buffered offset 100 and a new position of
  5 000, the next commit wrote **100**, re-delivering 4 900 records on the next
  rebalance or restart. Via `auto.offset.reset` the clamped offset could be one
  the log no longer holds, producing a reset → commit → `OFFSET_OUT_OF_RANGE`
  loop that never converged. Every reposition path now discards the affected
  partitions' buffered records.

- **`read_committed` consumers reported permanent phantom lag.**
  `last_stable_offset` was decoded from every Fetch response and never read.
  `lag()`, `current_lag()`, `is_caught_up()` and the `consumer_lag` /
  `consumer_lag_max` metrics all compared the position against the high
  watermark, so an open transaction on a partition kept a fully drained
  consumer reporting a backlog the size of that transaction — and
  `is_caught_up()` could never return `true`, breaking every "drain then
  proceed" pattern including `CompactedTopicConsumer::scan()`. Lag-based
  autoscalers wired to these metrics would scale out against an idle consumer.

- **`pause()` was bypassed by `recv()` and `batch_recv()`.** `poll()` withheld
  records for partitions paused since the fetch was issued; the buffer drain
  did not, so the same client gave two different answers to "does `pause()`
  stop delivery?" depending on which read API was used. Paused records are now
  withheld — and deliberately *kept* in the buffer rather than discarded, since
  the fetch position has already advanced past them.

- **A transactional commit could orphan a record into the next transaction.**
  `commit_transaction()` drained the accumulator *before* transitioning out of
  `InTransaction`. Since `send_record` admits a record whenever it observes
  `InTransaction`, a concurrent send in the window between the flush completing
  and the state changing was accepted — and its record was still buffered when
  `EndTxn` went out. It would either be rejected by the broker as
  `INVALID_TXN_STATE` or, once `begin_transaction()` had been called again,
  silently join the *next* transaction: a record the application was told had
  been committed could disappear when a later transaction aborted. The
  transition now happens first, mirroring `abort_transaction()`, which always
  did it in that order.

  Draining now also waits on the in-flight barrier before flushing. Emptying the
  batch queue is not enough on its own: a `send_record` that passed its state
  check moments earlier may still be running interceptors or encoders and has
  not reached the accumulator yet, so the flush would miss exactly the records
  closest to the transition. `abort_transaction()` had the same gap and gets the
  same fix, which is also what stops those callers' futures hanging after the
  transaction is torn down.

- **A commit could write the `EndTxn` marker while `send_offsets_to_transaction`
  was still on the wire.** That call is the join between the consumer's position
  and the producer's output — the whole point of consume-transform-produce is
  that the two commit atomically — but it never registered with the producer's
  in-flight barrier. A concurrent `commit_transaction()` therefore could not see
  it: the commit found the barrier idle, flushed, and sent `EndTxn` while the
  `TxnOffsetCommit` was still in flight, leaving the offsets **outside** the
  transaction. The output records stayed atomic with each other but not with the
  consumer's position, which is the one guarantee the API exists to provide.
  `send_offsets_to_transaction` now takes a barrier guard before reading the
  state, which makes the two orderings exhaustive: either the commit waits for
  it, or it started after the commit transitioned and is refused.

- **A commit marker could end a `read_committed` abort filter early.** The
  aborted-transaction filter deactivated a producer on **any** control batch,
  never reading the marker's type field (`0` = ABORT, `1` = COMMIT). Any
  sequence in which a commit marker for an aborted producer preceded that
  producer's abort marker would release the filter and surface aborted records
  to a `read_committed` consumer. The type field is now read; a control batch
  with a missing or malformed key leaves the filter engaged.

- **The HTTP `Host` header omitted the port and did not bracket IPv6
  literals.** `https://registry.example.com:8081/subjects` — the Confluent
  Schema Registry's own default port, so the most common configuration — sent
  `Host: registry.example.com`, violating RFC 9110 §7.2. Name-based virtual
  hosting, nginx `server_name`, Envoy virtual hosts and ALB host-header rules
  all failed to match, and an OIDC token endpoint validating the request
  authority against its issuer rejected the token request. An IPv6 registry
  sent `Host: ::1`, which is not a parseable authority. Affects the Schema
  Registry client (`schema-registry`) and the OIDC token provider
  (`oauth-oidc`).

- **`assign()` leaked state for partitions it dropped.** Narrowing a manual
  assignment — `assign("t", vec![0, 1])` then `assign("t", vec![0])` — left
  partition 1's position, cached watermarks, paused flag and buffered records
  behind. The buffered records were the damaging part: commits are clamped to
  the lowest buffered offset, so a partition the caller had stopped consuming
  kept dragging back the commit for one it had not. A narrower `assign()` now
  revokes what it drops, exactly as a rebalance does.

### Changed

- **Lag counts records read ahead into the buffer.** `lag()`, `current_lag()`,
  `is_caught_up()` and the `lag` / `lag_max` metrics measure from the delivered
  position rather than the fetch position, because a record that has been
  fetched but not returned is still backlog from the application's point of
  view. All five, plus `position()` and `commit()`, are now derived from one
  boundary — the lowest offset still awaiting delivery.

### Performance

- **Fetch responses are read ahead into a prefetch buffer.** Each fetch decodes
  one delivery's worth (`max_poll_records`) plus the receive buffer's free
  capacity and parks the surplus, so the next `poll()` is served from memory
  with no Fetch on the wire. In steady state this halves Fetch round trips and
  takes network latency out of every other poll. Nothing is dropped and nothing
  is decoded twice; the commit is held behind whatever is still parked, so a
  crash cannot skip it.

- **Fetch decoding is now bounded by `max_poll_records`.** A response could
  carry up to `fetch_max_bytes` (50 MB by default) while a poll may return only
  `max_poll_records` (500). Every record in the response was fully
  materialised — `ConsumerRecord` allocated, key and value sliced, headers
  built, topic cloned — and the surplus was then dropped and re-decoded on the
  next poll. With 50 partitions of 1 KiB records that is roughly **100× the
  necessary decode work per poll**, all of it garbage, plus the allocator
  pressure of ~50 000 short-lived records. A poll-wide budget, shared across
  the concurrent per-broker fetches and claimed one CAS *per batch* rather than
  per record, now stops the decode at the delivery cap plus the buffer's free
  capacity — and the surplus is parked rather than discarded, so it is neither
  re-fetched nor re-decoded.

- **Partition fetch order is now a real round robin.** Both the broker's
  `fetch.max.bytes` accounting and the client's `max_poll_records` cap consume
  partitions in request order, so a fixed order starves the tail. The order
  previously came from `HashMap` iteration, which is unspecified — fairness was
  a property of the standard library's per-instance hash seed rather than of
  the code. Partitions are now sorted (making the order deterministic) and
  rotated by one position per poll, matching the Java client's
  `PartitionStates.moveToEnd`.

### Added

- **`KafkaDeadLetterQueue`** — the `DeadLetterQueue` implementation that routes
  dead letters back into a Kafka topic. It shipped as a trait with no
  implementation, so every user wrote the same twenty-five lines. The built-in
  version attaches `__krafka.dlq.original.topic` and
  `__krafka.dlq.exception.message`, drops the source partition index (the
  dead-letter topic has its own partition count), and counts what it routed and
  what it lost — `failures()` is the only signal that the safety net itself is
  failing, since the original error reaches the caller either way.
- **`krafka::prelude`** — the types needed to write a producer, consumer or
  admin client, as one glob import. `Result` is deliberately excluded: krafka's
  alias takes one type parameter, so a glob that shadowed
  `std::result::Result` would break every `Result<T, E>` in the importing
  module.
- **`krafka::interceptor::CommitOffsets`** — the map type `on_commit` receives.
  The trait signature previously leaked `ahash::AHashMap`, so an implementor had
  to add `ahash` to their own manifest just to name the parameter.
- **DLQ header constants** — `HEADER_ORIGINAL_TOPIC`,
  `HEADER_ORIGINAL_PARTITION`, `HEADER_ORIGINAL_OFFSET`,
  `HEADER_EXCEPTION_MESSAGE`, so the producer-side and consumer-side paths
  cannot drift from one wire contract.

### Documentation

- **`just docs-test` now exists and runs in CI.** `xtask/doc_api.py` had
  documented it as the stronger guarantee for two releases; the recipe was never
  written. It compiles every snippet marked ```` ```rust,compile ```` against
  the real crate. 196 of 343 Rust blocks across the README and the guides are
  now compile-checked; the rest are deliberate fragments naming
  reader-supplied helpers.
- **It immediately found broken snippets in the first two pages anyone reads.**
  The README and Getting Started admin examples chained `.with_config()` onto
  `NewTopic::new()`, which returns a `Result` because it validates the topic
  name; the Getting Started consumer example passed `&Option<Bytes>` to
  `String::from_utf8_lossy`. Neither compiled.
- The documented `ConsumerInterceptor::on_commit` implementation used
  `std::collections::HashMap` where the trait takes an `ahash::AHashMap` — a
  different type, so it never compiled either.
- The documented `DeadLetterQueue` implementation never compiled. `Debug` is a
  supertrait of `DeadLetterQueue` and the natural implementation owns a
  `Producer`, which did not implement `Debug` — so the example in the error
  guide, and any code copied from it, failed to build. `Producer` now
  implements `Debug` (hand-written, excluding credential-bearing config), and
  the pattern is kept honest by a compiled doctest on the trait itself.
- New **Cookbook** guide: task-oriented recipes for exactly-once
  consume-transform-produce, at-least-once commits, backpressure, replay from a
  timestamp, compacted-topic tables, dead-letter routing, shared connection
  pools, TLS rotation, Prometheus export and testing without Docker.
- The README module table listed 6 of the crate's 16 public modules; it now
  lists all of them and marks the three `#[doc(hidden)]` ones as unstable.
- `ProducerStateStore` had no coverage outside its rustdoc despite being a
  public trait with a builder setter; documented under Producer.
- Corrected the `lag()` snippet in the consumer guide, which iterated the
  `LagResult` as if it were a map, and the async-commit section, which awaited
  `commit_async()` inline and so never showed what the handle is for.
- Documented read-ahead and partition-fetch fairness in the performance guide.
- The `dlq` module's own examples were wrong in two ways: one passed a
  `KrafkaError` where `send` takes a `String`, and the header table named raw
  strings rather than the constants that define them.

### Added

- `Consumer::cached_high_watermark` — the partition's log-end offset regardless
  of isolation level, for callers that want the value `cached_end_offset` used
  to return under `read_committed`.
- `Consumer::cached_last_stable_offset` — the first offset belonging to an open
  transaction. The gap between it and the high watermark is the volume of
  in-flight transactional data.
- `Consumer::fetch_position` — the offset the next fetch starts from, for
  callers that want the read-ahead value rather than the delivered one.

## [0.16.0]

### Breaking

- `AwsMskIamCredentials::with_session_token` is a builder method rather than a
  four-argument constructor:
  `AwsMskIamCredentials::new(id, secret, region).with_session_token(token)`.
  The old form fails to compile rather than silently changing meaning.

### Fixed

- **`compression_level` was dropped on the batching path.** It applied only at
  `linger = 0`, so the throughput-tuned configuration — and every
  `TransactionalProducer`, which always batches — encoded at the codec default.
- **`dead_letter_queue` was direct-send only.** Configuring a DLQ alongside any
  batching silently disabled it.
- **`close()` tore down a *shared* connection pool.** A client built with
  `.with_client(..)` called `close_all()` unconditionally, killing every
  sibling client's connections. Clients now report `owns_pool()` and leave a
  borrowed pool to its `KrafkaClient`.
- `SecureConnectionConfigBuilder::tls()` lost its TLS configuration when called
  before a SASL setter; ordering no longer matters.

### Added

- `AuthConfig::with_tls(TlsConfig)` — every SASL mechanism composes with TLS
  through one method. `SASL_SSL` + SCRAM, the default secured listener on most
  managed Kafka offerings, was previously unreachable from outside the crate.
  `sasl_scram_sha256_ssl` / `sasl_scram_sha512_ssl` added for symmetry;
  `AuthConfig::from_env` gained `KAFKA_SSL_*` material plus the `OAUTHBEARER`
  and `AWS_MSK_IAM` mechanisms.
- `AwsMskIamCredentials::with_region` and `from_env_with_region`.
- `TransactionalProducerBuilder` reaches parity with `ProducerBuilder`:
  `build_config()`, `compression_level`, `topic_compression`,
  `delivery_timeout`, `dead_letter_queue`, `interceptor` / `add_interceptor`,
  `state_store`, `with_client`, the metadata cache TTLs and
  `sasl_oauthbearer_provider`. `acks` and `idempotent` stay excluded because
  transactions fix both.
- `TransactionalProducer::flush()`.
- `ShareConsumerBuilder::with_client`.
- OAUTHBEARER token-lifecycle metrics on `ConnectionMetrics`:
  `oauth_token_fetches`, `oauth_token_fetch_failures`,
  `oauth_token_fetch_latency`, `oauth_token_expiry_epoch_ms`, plus a `WARN` on
  every failed fetch. A misconfigured `token_endpoint` is no longer
  indistinguishable from an unreachable broker.
- The in-process fake broker serves the full transaction protocol — KIP-360
  fencing, commit/abort control batches, `read_committed` isolation, TV1 and
  KIP-890 TV2. Exactly-once is testable without Docker.

## [0.15.0]

### Added

- Transactional producer and consumer test coverage for KIP-98, KIP-360,
  KIP-447 and KIP-890.
- Additional CI checks for code quality and documentation consistency.

## [0.14.0]

### Changed

- Improved `Metadata` and `ConsumerGroupHeartbeat` request handling.

## [0.13.0]

### Breaking

- Correctness overhaul across the protocol layer; legacy encode/decode paths
  below the Kafka 3.9 floor were removed.

### Added

- KIP-320 (truncation detection), KIP-447 (zombie fencing), KIP-890
  (transaction version 2) and KIP-951 (leader hints) support.
- The in-process test broker (`test-broker` feature).

## [0.12.0]

### Changed

- Optimised topic-metadata retrieval.
- Improved debug output for AWS IAM credentials.
- CI gained feature-specific checks for the `rustls-aws-lc-rs` and `ring`
  backends.

## [0.11.0]

### Changed

- Improved metadata refresh handling; the in-flight request ceiling is bounded.
- Per-topic metrics error handling and batch recording.

## [0.10.0]

### Changed

- Dual-licensed MIT OR Apache-2.0.
- Removed the deprecated `ring` dependency path; improved the duration-clamping
  warning.

### Added

- `established_broker_ids`; improved backoff jitter handling.
- `InFlightBarrier` supports concurrent flush and close.

## [0.9.x and earlier]

Initial development: wire protocol, producer, consumer, admin client,
authentication (SASL PLAIN / SCRAM / OAUTHBEARER / AWS MSK IAM), TLS,
compression codecs, schema registry integration and the metrics layer.

[Unreleased]: https://github.com/hupe1980/krafka/compare/v0.18.0...HEAD
[0.18.0]: https://github.com/hupe1980/krafka/compare/v0.17.0...v0.18.0
[0.17.0]: https://github.com/hupe1980/krafka/compare/v0.16.0...v0.17.0
[0.16.0]: https://github.com/hupe1980/krafka/compare/v0.15.0...v0.16.0
[0.15.0]: https://github.com/hupe1980/krafka/compare/v0.14.0...v0.15.0
[0.14.0]: https://github.com/hupe1980/krafka/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/hupe1980/krafka/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/hupe1980/krafka/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/hupe1980/krafka/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/hupe1980/krafka/compare/v0.9.2...v0.10.0
