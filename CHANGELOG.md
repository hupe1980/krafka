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

[Unreleased]: https://github.com/hupe1980/krafka/compare/v0.17.0...HEAD
[0.17.0]: https://github.com/hupe1980/krafka/compare/v0.16.0...v0.17.0
[0.16.0]: https://github.com/hupe1980/krafka/compare/v0.15.0...v0.16.0
[0.15.0]: https://github.com/hupe1980/krafka/compare/v0.14.0...v0.15.0
[0.14.0]: https://github.com/hupe1980/krafka/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/hupe1980/krafka/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/hupe1980/krafka/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/hupe1980/krafka/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/hupe1980/krafka/compare/v0.9.2...v0.10.0
