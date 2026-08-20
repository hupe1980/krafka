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

## [0.20.0] — 2026-08-20

krafka could read a Kafka tombstone but not write one. `ProducerRecord::value`
was `Bytes`, with no way to express the protocol's **null** — so an empty value
encoded as a zero-length payload, which log compaction treats as ordinary data
rather than as a delete marker. This release makes null representable
end to end on the produce path, and fixes the three places the missing
representation was already causing silent damage.

### Breaking

- **`ProducerRecord::value` is now `Option<Bytes>`.** `None` is Kafka's null
  value — a tombstone. `ProducerRecord::new(topic, value)` is unchanged and
  stores `Some(value)`; the new `ProducerRecord::tombstone(topic, key)`,
  `with_value()` and `without_value()` build and clear the null.
- **`ProducerRecord::headers` is now `Vec<(String, Option<Bytes>)>`**, matching
  `ConsumerRecord`. The record format distinguishes a null header value from a
  zero-length one. `with_header(key, value)` still takes a plain value;
  `with_null_header(key)` is new. Pushing to the field directly now needs
  `Some(...)`.
- **`Producer::send`, `Producer::send_with_headers` and
  `TransactionalProducer::send` take `value: Option<&[u8]>`**, mirroring `key`.
  Wrap existing values in `Some(...)`; pass `None` to write a tombstone.
- **`RecordBatchBuilder::add_record_with_headers`** takes
  `Vec<(impl Into<Bytes>, Option<impl Into<Bytes>>)>` for headers.
  `RecordHeader::null(key)` builds a null-valued header.
- `build_dlq_record` returns a record whose `value` and header values are
  `Option<Bytes>`.

### Fixed

- **A tombstone routed through a dead-letter queue is no longer flattened.**
  `build_dlq_record` collapsed a null value, and a null header value, to
  zero-length — documented at the time as unfixable without this type change.
  A tombstone reaching a compacted DLQ topic therefore arrived as an ordinary
  empty record and never deleted its key.
- **A configured `value_serializer` no longer runs on a tombstone.** A
  schema-registry serializer prepends a magic byte and schema ID; applied to a
  null value it produced a five-byte record that compaction preserves, so the
  key was never deleted and the caller had no way to see it. Null values are
  now passed through unserialized, matching the Confluent serializers and the
  consumer's deserializers, which already skipped absent fields.
- **A configured `key_serializer` no longer invents a key for a keyless
  record.** It serialized `Bytes::new()` and stored the result, moving the
  record off the default partitioner's keyless path — every keyless record on
  that producer hashed to a single partition instead of being spread.
- The plain and transactional producers now share one serializer application
  path (`apply_serializers`) instead of two copies that had already drifted.

### Added

- `ProducerRecord::tombstone`, `with_value`, `without_value`, `is_tombstone`
  and `with_null_header`. `is_tombstone` uses the same rule as
  `ConsumerRecord::is_tombstone` — a key and no value — so a record classifies
  identically on both sides of the wire.
- `RecordHeader::null`.
- Tests covering the whole path: wire-level encode/decode of a null value and a
  null header (with a zero-length negative control), serializer skipping,
  DLQ preservation, a fake-broker round trip through a real producer, tombstone
  partition co-location with its key, a produced tombstone deleting a key from
  `CompactedTable`, and a Docker integration test against a real
  `cleanup.policy=compact` topic.
- Guide coverage: **Tombstones and Compacted Topics** in the producer guide,
  a *Delete a key from a compacted topic* cookbook recipe, and a null-vs-empty
  note in the consumer guide's tombstone section.

## [0.19.1] — 2026-08-20

### Fixed

- **A compacted partition could stall `poll()` forever**, returning empty
  `Vec`s long before the end of the log. A batch the log cleaner had emptied
  entirely (its header retained for producer idempotence, zero records left)
  made the budget claim return `0`, which was misread as "budget exhausted":
  the walk broke out, discarded every later batch in the response, and never
  advanced the position, so the next fetch re-read the same empty batch. A
  batch drained in full had the mirror-image problem — it advanced the position
  only to its last *surviving* record + 1, which still lay inside the batch's
  offset span when compaction had removed its trailing records. The batch walk
  now advances through the full offset span of any batch it skips or drains in
  full, mirroring the Java client's `CompletedFetch.nextFetchOffset`, while a
  batch cut short by `max_poll_records` still parks at the last delivered
  record so its remainder is re-fetched. Every batch-level advance goes through
  one guarded helper that refuses to move the position backwards, so a
  misbehaving broker degrades to a visible stall rather than silent
  re-delivery.

### Changed

- Dependencies refreshed in `Cargo.lock`; the ignored-advisory list in
  `deny.toml` cleared.

## [0.19.0] — 2026-08-10

A correctness release from a three-round deep audit against the reference
implementations (Java client, librdkafka, franz-go) and the broker source.
Six real defects fixed — three in the transactional producer, one in
cooperative rebalancing, two in the share consumer / KIP-848 paths — plus a
Redpanda integration suite and a Kafka 3.9 → 4.3 version-matrix recipe.
Validated against real brokers: 46/46 Apache Kafka and 3/3 Redpanda Docker
integration tests.

### Fixed

- **Cooperative rebalancing now withholds transferring partitions for one
  generation** (KIP-429), closing a double-consumption window. The leader
  used to hand a partition moving from member A to member B directly to B in
  the same generation; B — whose own revocation diff is empty — finalized
  immediately and began consuming while A was still consuming the same
  partition, and B's committed-offset fetch could race A's pre-revocation
  commit (re-processing from a stale offset). The leader now removes any
  partition whose previous owner is a *different live member* from the new
  owner's assignment, exactly as the Java client's
  `CooperativeStickyAssignor#adjustAssignment` does; the previous owner
  revokes it and the follow-up rebalance delivers it. Verified against the
  member-side flow: the withheld partition reaches its new owner in the next
  generation and the assignment is stable from there.

- **The KIP-848 revocation-ack heartbeat now fires immediately.** After the
  consumer finished its revocation callbacks, the heartbeat task called
  `Interval::reset()` under a comment claiming the next tick fires
  immediately — but `reset()` schedules the next tick one *full period* from
  now, so the acknowledgement the coordinator waits for before continuing
  reconciliation sat out up to an entire heartbeat interval per revocation
  round. The task now installs a fresh interval, whose first tick completes
  immediately.

- **KIP-932 acknowledgement batches are now sorted, merged, and
  deduplicated per partition.** Explicit acks were sent in application call
  order, and duplicate offsets in the implicit path produced overlapping
  single-offset ranges — which reference record state a preceding batch
  already transitioned and fail the partition with `INVALID_RECORD_STATE`.
  Batches are now ascending by first offset (the wire form the Java client
  always produces), contiguous same-type ranges collapse into one batch, and
  implicit-ack offsets are deduplicated — so N sequential `acknowledge()`
  calls cost one wire batch instead of N.

- **A produce response that failed to decode can no longer trigger a batch
  split-and-resend.** `is_batch_too_large` matched every
  `ProtocolErrorKind::InvalidLength`, which is also what dozens of *response
  decode* paths raise — so a truncated or corrupt `ProduceResponse` counted as
  "batch too large" and resubmitted (as two halves, with reallocated
  sequences) a batch the broker may already have committed: duplicates for a
  plain producer, a wedged sequence space for an idempotent one. The local
  frame-size guard now raises the new dedicated
  `ProtocolErrorKind::FrameTooLarge`, and the split path matches exactly that
  plus the two broker size rejections.

- **A failed transactional commit or abort now reverts to the state it was
  entered from**, instead of unconditionally to `InTransaction`. Two origins
  were mishandled:
  - a **`Prepared`** transaction (KIP-939 two-phase commit) whose completion
    failed retriably was reopened as `InTransaction`, re-admitting `send()`
    into content whose `(producer_id, epoch)` had already been handed to the
    external coordinator;
  - a **`CommitIndeterminate`** commit retry that failed again (flush
    failure, or a definitive-looking broker error on the *retry*) reverted to
    `InTransaction`, re-enabling `abort_transaction` — the exact
    abort-after-possible-commit tear (KAFKA-17754) the state exists to
    prevent. An indeterminate commit now stays indeterminate until a commit
    succeeds or the coordinator answers `TransactionAbortable`.

### Added

- `ProtocolErrorKind::FrameTooLarge` — raised only by the client's own
  pre-send frame-size guard, never by inbound decoding. (`ProtocolErrorKind`
  is `#[non_exhaustive]`, so this is not a breaking change.)
- **Redpanda integration suite** (`just integration-redpanda`): pins
  produce/consume, admin, and the KIP-890 TV1 transaction fallback against a
  real Redpanda container. Redpanda works with krafka out of the box via API
  version negotiation; the docs' new *Broker Compatibility* section spells
  out what applies and what fails fast (`ShareConsumer`, log-dir admin).
- **Kafka version-matrix recipe** (`just integration-matrix`): runs the
  Docker integration suite against every supported Kafka minor (3.9.0 →
  4.3.0), or any subset (`just integration-matrix "4.2.0 4.3.0"`).
- **CI now runs all of it.** The integration matrix covers Kafka 3.9.0
  through 4.3.0 (previously 3.9.0 and 4.0.0 only), the Redpanda suite has
  its own job, and the SASL suite — runnable locally since it was written but
  never wired into a workflow — finally runs in CI too. `just ci-full`, the
  release gate, includes the same three suites.

### Removed

- The unreachable `"record too large for batch size"` accumulator error
  path. An empty batch accepts any first record by design (oversized records
  form a single-record batch, as in the Java client), so the branch could
  never execute; `max_request_size` admission already rejects genuinely
  oversized records at enqueue time.

## [0.18.0] — 2026-08-08

The producer's second, unbatched send path is deleted. It was the **default**
configuration (`linger = 0`), it issued one Produce request per record, and it
had no per-partition dispatch order — so concurrent sends to one partition
could fail an idempotent producer permanently. Every send now goes through the
record accumulator.

Plus a consumer ordering defect (`recv()` could hand back a partition's records
out of order) and a silent data-loss path (a deserializer error skipped the
records it rejected).

### Breaking

- **`CompactedTopicConsumerBuilder` is removed**, replaced by
  `CompactedTopicConsumer::from_consumer_builder(ConsumerBuilder, topic)`.

  The old builder owned a hand-picked subset of nine consumer settings, and
  every setting it omitted was unreachable through it. Two of the omissions
  mattered: `isolation_level`, so the type most likely to be pointed at
  transactional data could not ask for `read_committed` and could materialise a
  table from records that were later aborted; and `connect_timeout`, which
  `build()` validates `request_timeout` against, so a caller wanting a tight
  request budget was refused with an error naming a value they had no way to
  change.

  Taking the real `ConsumerBuilder` removes the class rather than the two
  instances. The constructor imposes three settings, which are requirements of
  materialising a table rather than preferences: `auto_offset_reset = Earliest`,
  `enable_auto_commit = false`, and — new — **`isolation_level = ReadCommitted`**.
  That last one is a behaviour change for existing callers, and deliberately so:
  anyone relying on the old default was reading aborted records into a table.
  It costs nothing on a topic with no transactions, where the last stable offset
  equals the high watermark in the same fetch response. Use `from_consumer` with
  a hand-built `Consumer` to read uncommitted state on purpose.

- **The SOCKS5 proxy moved onto `TransportConfig`.** `ProducerConfig::proxy`,
  `ConsumerConfig::proxy`, `AdminConfig::proxy`, `ShareConsumerConfig::proxy`
  and their accessors are gone; `TransportConfig::proxy` replaces them. Every
  client builder keeps its `.proxy(..)` shorthand, which now writes into the
  builder's transport config, so there is exactly one storage location and no
  precedence rule.

  Reported by a downstream project that mapped its own transport settings onto
  `TransportConfig` — the obvious thing to do with a type of that name — and
  shipped a producer that silently bypassed the proxy its deployment required.
  Where the brokers were reachable directly, traffic left by the wrong egress
  path and nothing said so. What made it a trap rather than an omission is that
  this module's own documentation described a `TransportConfig` as carrying
  "the SOCKS5 route", and warned that a client left on the default transport
  gets "no proxy" — describing a capability the type did not have.

- **`ProducerConfig::max_in_flight` / `ProducerBuilder::max_in_flight` removed**
  from both the plain and the transactional producer, along with the
  `max_in_flight()` accessor and the automatic "cap to 5 when idempotent"
  normalisation.

  The knob existed to bound a concurrency the accumulator does not have: it
  keeps exactly one batch per partition on the wire, in the order batches were
  sealed, so sequence order and wire order cannot diverge and KIP-679's
  `max.in.flight ≤ 5` rule has nothing to protect. The per-connection ceiling
  is `TransportConfig::max_in_flight_requests`, which is where a per-connection
  setting belongs. Delete the call; there is no replacement.

  It was also doing real harm: the accumulator gated *global* batch dispatch on
  it, so a producer writing to 100 partitions could have at most five Produce
  requests outstanding across the whole cluster.

- **`ShareConsumerBuilder::fetch_max_wait_ms(i32)` is now
  `fetch_max_wait(Duration)`**, and `ShareConsumerConfig::fetch_max_wait_ms`
  is `fetch_max_wait: Duration`. It was the only timeout in the crate taking
  raw milliseconds.

- **`TransactionalProducerConfig::transaction_timeout_ms` is now
  `transaction_timeout: Duration`** internally. The public setter and accessor
  were already `Duration`-based and are unchanged; only the field name and the
  struct's `Debug` output differ.

- **`KrafkaError::RecordDeserialization { topic, partition, offset, part,
  message }`** is a new variant. Exhaustive matches over `KrafkaError` need a
  new arm. It replaces the generic error a failing key/value deserializer used
  to surface and carries the coordinates needed to seek past a poison record —
  the Java client's `RecordDeserializationException`.

- **Consumer deserialization now runs before the consumer interceptor**, so
  `ConsumerInterceptor::on_consume` observes deserialized values rather than
  wire bytes. This mirrors the producer, where `on_send` runs before
  serialization, and matches the Java client. An interceptor that parsed the
  raw framing itself must move that logic into a `Deserializer`.

### Added

- **`Producer::enqueue` and `TransactionalProducer::enqueue`**, returning a
  `DeliveryHandle` / `TransactionalDeliveryHandle` instead of fusing the append
  and the acknowledgement into one future — the shape of Java's
  `Producer.send()`.

  **Produce order is enqueue order.** If `enqueue(a)` returns before
  `enqueue(b)` is called, `a` reaches its partition first, whatever order the
  handles are polled in or whether they are polled at all. `send_record` cannot
  offer that: it does its append somewhere inside its own polling, so N of them
  polled concurrently append in *poll* order — and under buffer-memory
  backpressure the two diverge, because a send that cannot get its permit
  yields and lets a later one append first.

  Pipelining on top of the fused future was therefore possible but delicate: it
  required polling every outstanding future in submission order on every wake,
  which is O(window) per wake and where the sweep *is* the ordering guarantee.
  Reported by a downstream project that had built exactly that, and measured
  35× on the alternative of awaiting each acknowledgement.

  `send_record` remains, and is now `enqueue(record).await?.await`.

- **KIP-939 two-phase commit** (`unstable-protocol`), the largest remaining
  functional gap the previous review named. Kafka transactions are atomic
  within Kafka and with nothing else, so a service that must write to Kafka
  **and** a database — either both or neither — could not express that.

  - `TransactionalProducerBuilder::two_phase_commit(true)` sends `enable2Pc` on
    `InitProducerId`, which stops the coordinator applying
    `transaction.max.timeout.ms`. Without it "prepared" is a promise krafka
    cannot keep, because the broker would abort the transaction out from under
    the external coordinator. Combining it with an explicit
    `transaction_timeout` is a configuration error rather than a silently
    ignored setting.
  - `prepare_transaction() -> PreparedTxnState` flushes every buffered record
    and closes the transaction to new ones. It sends **no request**: there is no
    prepare in the Kafka protocol, and the prepare *is* the flush — once every
    record is durably written with no commit marker following, the transaction
    is in doubt exactly as a prepared one should be.
  - `init_transactions_keeping_prepared() -> Option<PreparedTxnState>` is the
    recovery entry point. Where `init_transactions()` tells the coordinator to
    abort whatever the previous incarnation left open, this tells it to hold.
  - `complete_transaction(stored) -> TransactionOutcome` resolves it: if the
    stored state matches the transaction the coordinator still holds, the
    prepare was durably recorded externally and this side commits to match;
    otherwise the stored value names an older transaction, the external side
    rolled back, and this side aborts. A mismatch is the *normal* outcome of a
    crash in that window, not an error.
  - `PreparedTxnState` renders as `producer_id:epoch` through `Display` and
    parses back through `FromStr`, so storing it in the external coordinator
    needs no bespoke serialisation.

  `EndTxn` for a recovered transaction carries the *ongoing* producer ID and
  epoch the coordinator reported, not the fresh pair `InitProducerId` just
  issued — sending the fresh pair would fence the very transaction the call is
  resolving.

  `TransactionVersion::V3` was added alongside it, and this is the part worth
  reading twice. `from_feature_level` collapsed every level ≥ 2 to `V2`, and
  `is_v2()` was an equality test — so adding `V3` without changing `is_v2()`
  would have sent a TV3 cluster back to TV1 semantics: `AddPartitionsToTxn` per
  partition and the wrong epoch handling, both perfectly legal requests, so the
  regression would have been silent. `is_v2()` now means *at least* TV2, which
  is what the name always described.

  TV3 also requires the same kind of evidence TV2 does: the finalized feature
  level **and** an `InitProducerId` that can actually carry `enable2Pc` (v6). A
  broker too old for the field does not reject it — the field is simply absent,
  and the coordinator applies `transaction.max.timeout.ms` to a transaction the
  caller believes is exempt. The cluster level is the minimum across brokers, so
  a rolling upgrade cannot enable 2PC before every broker can honour it.

  `two_phase_commit(true)` on a cluster below TV3 now fails at
  `init_transactions()` naming the feature level, the API version and the ACL
  required, instead of surfacing the broker's bare `UNSUPPORTED_VERSION` behind
  "failed to initialize producer ID".

  This retires the two `ongoing_txn_producer_*` entries from
  `protocol-reachability`'s exemption list, leaving two.

- **`NewTopic::with_replica_assignment`.** Manual replica placement —
  partition index → broker IDs, first entry the preferred leader — was
  unreachable: `NewTopic` could not express it and `create_topics` sent
  `assignments: Vec::new()` unconditionally. That rules out rack-aware
  placement the controller's own rule cannot produce, and mirroring an existing
  topic's layout. A ragged replication factor is rejected here, where the error
  can name the partition; the broker answers `INVALID_REPLICA_ASSIGNMENT`
  without saying which one disagreed.

- **`AdminClient::list_consumer_groups` takes a `GroupListing` filter.** The
  `states_filter` (KIP-518) and `types_filter` (KIP-848) fields were sent
  empty, so there was no way to ask the broker for only the `Empty` groups or
  only the KIP-848 ones. On a cluster with tens of thousands of groups that is
  the difference between transferring the entire group registry on every call
  and transferring the handful you asked about — filtering client-side is
  correct and does not scale. Older brokers ignore the filter and return a
  superset rather than failing.

- **`AdminClient::create_delegation_token` takes an `owner`.** KIP-373's
  on-behalf-of half was missing: the previous release surfaced the *requester*
  fields the broker sends back, while the request could not name an owner, so
  the distinction was observable and not producible. A superuser can now
  provision a token for a service account that never authenticates
  interactively.

- **`ShareConsumer::acquisition_lock_timeout()`.** The broker reports how long
  an acquisition lock lasts on every `ShareFetch` (KIP-1222), and krafka
  decoded the field and dropped it. `AcknowledgeType::Renew` exists to extend
  that lock and its own documentation says to renew before the deadline — while
  the deadline comes from `group.share.record.lock.duration.ms`, a broker-side
  setting no client can read from its own configuration. `Renew` was therefore
  documented, reachable, and impossible to schedule correctly; the only
  workable strategy was a timer tuned by guesswork.

- **`DelegationToken::token_requester_principal_type` / `_name`.** KIP-373 lets
  one principal request a delegation token *on behalf of* another — how a
  superuser provisions a token for a service account. The owner is who the
  token authenticates as; the requester is who asked for it, and that is the
  field an audit trail needs. Both were decoded from the response and dropped
  before reaching the caller, so the distinction the KIP exists to record was
  invisible.

- **`just protocol-reachability`** (`xtask/protocol_reachability.py`), a new CI
  gate and the mirror image of `config-reachability`: every `pub` field of every
  response struct must be read by client code outside the protocol layer and
  outside tests, or carry a documented reason for being decode-only. 103 fields,
  4 documented exceptions.

  This is the shape of the two most severe defects in this project's history.
  `FetchResponsePartition::last_stable_offset` was decoded for every Fetch
  version from v4 up, asserted in the codec's own tests, and read by not one
  line of consumer code — so `read_committed` consumers reported permanent
  phantom lag. `acquisition_lock_timeout_ms` above is the same. Both look
  finished from the codec's side; from the client's side the information simply
  never arrives, and nothing in the type system can tell the difference.

- **`OffsetSpec::MaxTimestamp`, `EarliestLocal` and `LatestTiered`.** krafka
  negotiates `ListOffsets` v11 but `OffsetSpec` exposed only three of the five
  specs Java has, so three questions the wire could already answer were
  unreachable:

  - `MaxTimestamp` (`-3`, KIP-734) — the offset of the record with the largest
    timestamp. Not the same as `Latest` on any topic whose producers write out
    of order, which is any topic with application-clock timestamps or more than
    one producer. This is the spec a staleness alert actually wants.
  - `EarliestLocal` (`-4`, KIP-405) and `LatestTiered` (`-5`, KIP-1005) — the
    boundary between local and remote storage. Without them a client cannot
    tell whether a scan from the log start is about to pull from object
    storage.

  All three are negative timestamps on the wire, so a broker too old to know
  one does not reject it — it answers as though the value were an ordinary
  timestamp, i.e. the log start. krafka checks the negotiated version before
  sending and names the version required, rather than returning a
  plausible-looking wrong answer.

- **`AdminClient::describe_consumer_group_offsets` takes an
  `OffsetVisibility`.** The admin counterpart of the KIP-447 fix above:
  `StableOnly` refuses to report an offset an in-flight transaction can still
  retract, `IncludeUnstable` reports the freshest value. `consumer_group_lag`
  uses `IncludeUnstable`, because lag is a monitoring signal and a number that
  dips when a transaction aborts is the honest shape of the data.

- **`AdminClientBuilder::retries` / `retry_backoff` / `retry_backoff_policy`.**
  The admin client's controller- and coordinator-routing retries were
  compile-time constants: five attempts spaced by a flat 100 ms. Two problems.
  The budget is about a second of real time, which is short for a KRaft
  controller election — `create_topics` during a rolling controller restart
  failed with "the controller did not stabilise" when waiting longer would have
  worked. And the flat sleep had **no jitter**, so every admin client watching
  one election retried in lockstep and arrived at the newly elected controller
  as a single wave, which is the thundering herd `ClusterMetadata`'s rebootstrap
  jitter already exists to prevent.

  Both are now configurable and routed through the crate's shared
  `BackoffPolicy` — exponential with jitter, like every other retry here. The
  docstring claimed the gap was `retry.backoff.ms`, a setting that did not
  exist; the failure message now names the setting that does.

- **`FakeBroker::committed_records` / `all_records`.** Every record on a topic
  as a `read_committed` consumer would see it, and as a `read_uncommitted` one
  would — read from the broker's own log, so no consumer, no bounded poll loop
  and no iteration count that becomes flaky when someone tunes it. The
  *difference* between the two is what an exactly-once test is actually
  asserting.

- **Five `ShareConsumer` settings that existed but could not be set.**
  `fetch_min_bytes`, `fetch_max_bytes`, `max_records` and `batch_size` — the
  four knobs KIP-932 exposes for tuning a share fetch — were declared on
  `ShareConsumerConfig` and read when the `ShareFetch` request was built, with
  no builder setter anywhere. Every krafka share consumer in existence sent the
  same four numbers. `metadata_recovery_rebootstrap_trigger` was the fifth.

- **`ShareConsumerConfig` gained 17 accessors.** It had 6 where `ConsumerConfig`
  has 34, so `build_config()` — documented as the way to validate a
  configuration without a broker — handed back something largely unreadable.

- **`ShareConsumer` accepts `key_deserializer` / `value_deserializer`.** It
  returns the same `ConsumerRecord` as the subscription consumer, so it now
  takes the same `Deserializer` hook; a share-group application previously had
  to decode schema framing by hand. Deserialization runs *after* the record is
  registered for acknowledgement, because a share consumer's remedy for a
  poison record is `acknowledge_by_offset(.., Reject)` and that call requires
  the offset to be pending — there is no `seek()` to skip it with.

- **`TransportConfig::socket_send_buffer` / `socket_receive_buffer`** —
  `SO_SNDBUF` and `SO_RCVBUF` for every broker socket, the Java client's
  `send.buffer.bytes` / `receive.buffer.bytes`. They were already declared on
  `ConnectionConfig`, already had public accessors, and were already applied to
  the real socket by `happy_eyeballs.rs` via `socket2` — with no setter
  anywhere, so every krafka connection took the OS default. On a high
  bandwidth-delay-product link that is the throughput ceiling. Found by the new
  reachability gate below, on its first extension to the transport configs.

- **`just config-reachability`** (`xtask/config_reachability.py`), a new CI
  gate: every field of every config struct must have a builder setter and a
  public accessor, or an entry in an exception list with a reason. It walks the
  *fields*, so unlike `tests/builder_surface.rs` it can prove nothing was
  forgotten. 151 fields across 8 configs, 25 documented exceptions.

- **`rustdoc::broken_intra_doc_links` and `private_intra_doc_links` are
  denied**, and `just doc` now runs a second pass with
  `--document-private-items`. Fifteen documentation links resolved to nothing
  and rendered as plain text — including one to `ProducerConfigBuilder`, a type
  deleted several releases ago. The lint is allow-by-default for items rustdoc
  does not render, so `RUSTDOCFLAGS: -Dwarnings` alone never saw them.

### Fixed

- **`OffsetFetch` never asked for stable offsets (KIP-447).** `require_stable`
  was hardcoded `false`, so a `read_committed` consumer resuming after a crash
  could read a committed offset that a transaction had *staged but not
  committed*. If that transaction then aborted, the offset it staged was
  retracted — but the consumer had already resumed past those records, and they
  were never reprocessed. Silent data loss on the exactly-once recovery path,
  in the window a crash is most likely to land in.

  The flag now follows the isolation level. `read_uncommitted` consumers keep
  asking for the unstable value deliberately: they already read uncommitted
  data, and blocking their startup on an unrelated producer's open transaction
  would be worse.

  The second half was more dangerous and only becomes reachable once the flag
  is set: a partition answering `UNSTABLE_OFFSET_COMMIT` fell through the
  result-building loop, which keeps partitions whose `error_code.is_ok()`. The
  partition was simply absent from the map, and every caller reads a missing
  entry as "this group has never committed here" — which means
  `auto.offset.reset`. A few hundred milliseconds of waiting would have become
  a rewind to the start of the topic, or a jump to its end. It is now surfaced
  and retried, with jittered backoff (a rebalancing group calls this from every
  member against one coordinator at the same instant).

- **Three KIP-219 unit tests could not fail.** They built their own
  `parking_lot::Mutex<Instant>` and asserted over `checked_duration_since` —
  exercising `std::time` arithmetic while never touching `BrokerConnection`,
  which is why they were green throughout the defect above. Replaced by
  `broker_throttle_is_honoured_and_counted`, which drives a real connection and
  covers what they claimed to: the deadline is recorded, a shorter later
  throttle does not cut a longer window short, waiting both sleeps and counts,
  and an un-throttled connection does neither.

- **The producer's throttle wait was never counted.** `throttle_delays` and
  `throttle_delay_ms` (KIP-219) are recorded where the client sleeps out a
  broker-imposed throttle — but the producer pre-emptively slept on
  `conn.throttle_remaining()` one layer above the request path, to avoid
  negotiating an API version inside the quota window. That sleep consumed the
  window, so the request path's own check then found nothing left to wait for
  and recorded nothing.

  The single metric that answers "is the broker throttling us" therefore read
  **zero on the path most likely to be throttled** — produce quotas being the
  common case. A zero that means "not measured" is worse than a missing metric:
  an operator looking at the dashboard concludes throttling is not the problem.

  Both sites now go through `BrokerConnection::await_throttle`, which sleeps
  *and* records, so it is no longer possible to wait without counting.

- **KIP-814 `skip_assignment` was decoded and ignored.** When a static member
  rejoins and the coordinator still holds a valid assignment, it sets
  `skip_assignment` on the leader's `JoinGroup` response and sends no member
  metadata; the leader must send an empty assignment and let the coordinator's
  stand. krafka ran its assignor whenever it was the leader.

  Ignoring the flag happened to be harmless only because the member list
  arrives empty in that case, so the assignor produced nothing to send. That is
  obedience by accident: a leader that assigns whenever it is the leader has
  taken authority the coordinator explicitly reclaimed, and any response
  pairing `skip_assignment` with a non-empty member list would have it overwrite
  the coordinator's decision.

- **`ProxyConfig`'s rustdoc example led to a type no client accepts.** It built
  a `ConnectionConfig`, which no builder takes. Replaced with the two paths that
  work, both compile-checked.

- **`CompactedTopicConsumer::from_consumer` did not say how to find the
  partitions to assign.** It named `Consumer::assign` and stopped, so a reader
  could reasonably build a second `AdminClient` — a second connection, a second
  auth handshake — to enumerate what the consumer already knew. It now shows
  `Consumer::fetch_metadata`, and says that a partial assignment silently
  materialises a partial table.

- **`bootstrap_servers` was reported under two different names.** Six error
  sites said `bootstrap.servers is required`, three said `bootstrap_servers
  must not be empty`; a user grepping logs saw two spellings for one setting.
  All nine now name the builder method.

- **The share-consumer guide documented three wrong defaults.** `max_records`
  was listed as `-1` (actually `5000`), `batch_size` as `0` (actually `500`)
  and `client_id` as `"krafka-share-consumer"` (actually `"krafka"`), and the
  table omitted eight settings entirely.

- **A deserializer error silently dropped the records it rejected.** The fetch
  position is advanced before records are handed to the deserializers, so
  failing the poll there skipped the whole batch permanently — no commit, lag
  metric or log line would show it. The batch is now put back at the front of
  the receive buffer, where the commit clamp holds the committed offset behind
  it, and the error names the exact record.

- **`recv()` could deliver a partition's records out of order.** `poll()` parks
  its undelivered surplus at the *back* of the receive buffer. `recv()` took
  one record and appended the rest to the back as well — behind records from
  higher offsets in the same partitions. A fetch yielding more than
  `max_poll_records` for one partition therefore delivered offsets 501+ before
  offsets 2–500. The remainder is now reinserted at the front, which is what
  `batch_recv()` already did.

- **Concurrent sends to one partition could permanently fail an idempotent
  producer.** At the default `linger = 0` the removed direct-send path allowed
  up to `max_in_flight` produce requests to race onto the wire with no
  per-partition serialization, while sequence numbers were allocated in a
  different order. The broker answered `OUT_OF_ORDER_SEQUENCE_NUMBER`, which
  krafka correctly refuses to paper over, so the producer failed with
  "recreate the producer to resume". Reachable from the documented
  `Arc<Producer>`-across-tasks pattern with no configuration at all.

- **A Fetch v13+ response carrying an unresolvable topic UUID was logged as
  discarded but processed anyway.** Its partitions kept an empty topic name, so
  the high watermark, log-start offset, last-stable offset and preferred
  replica were all recorded under `("", partition)` — state that belongs to no
  topic, is never read back, and collides across topics.

- **A producer-ID reset racing a batch could disable idempotence silently.**
  The accumulator allocated sequences through the unchecked path, so a reset
  landing between the caller's initialisation check and the allocation left the
  batch stamped with producer ID `-1`: a non-idempotent write, with no error
  raised anywhere. It now allocates through `checked_allocate_sequence`, which
  verifies the identity under the same lock, and re-initialises once on a race.

- **`delivery_timeout` no longer excludes backpressure.** The budget is charged
  from `send()` entry — including the up-to-`max_block` wait for buffer memory
  — by pulling a batch's deadline back to its earliest record's entry time. The
  batched path previously started the clock when the batch was created, after
  that wait.

- **Steady-state logging demoted from `info!` to `debug!`**: every
  `ConsumerGroupHeartbeat`, every offset commit, every committed-offset fetch
  and every `SyncGroup`. An idle consumer group emitted a line every few
  seconds per member at the default subscriber level.

### Performance

- **`linger = 0` batches.** It always meant "do not *wait* for more records",
  not "do not batch" — the reading the deleted send path implemented. The
  accumulator dispatches immediately when a partition has nothing on the wire,
  and coalesces the records that arrive during that round trip into the next
  batch, dispatched the instant the acknowledgement lands (a completion wakes
  the dispatch loop directly; it does not wait for a timer tick). Latency for
  an idle producer is unchanged, because the first record never waits.

  Measured against the in-process fake broker: 200 concurrent sends to one
  partition, default configuration → **3** Produce requests, down from 200.
  Pinned by `the_default_producer_batches_concurrent_sends_to_one_partition`.

- **An idle producer no longer wakes the runtime 1 000 times a second.** The
  accumulator's run loop drove a fixed 1 ms `interval`, which was affordable
  while only `linger > 0` producers had an accumulator. Now that every producer
  has one, the loop sleeps until something is actually due — the earliest open
  batch's linger deadline, or a 1 s housekeeping floor when nothing is open.
  Dispatch is event-driven either way, so the timer is a deadline rather than a
  poll.

- **No per-record `String` allocation on the delivery hot path.** `pause()`
  filtering, the stale-response filter and receive-buffer purges probed a
  `HashSet<(String, PartitionId)>`, which cannot be keyed by a borrowed name,
  by allocating an owned key for every record. They now scan borrowed names
  over a set that is empty in the common case.

### Removed

- ~560 lines of duplicated producer send logic: `Producer::send_to_partition`,
  `do_send`, `build_produce_request`, `release_failed_sequence`,
  `reserve_send_memory` and the second memory-permit pool that shadowed the
  accumulator's. Retries, sequence recovery, KIP-951 leader hints, the DLQ and
  the interceptor callbacks now exist in exactly one place, so they cannot
  drift apart again — which is how `compression_level` and `dead_letter_queue`
  each came to work on only one of the two paths.

## [0.17.0] — 2026-08-07

Ten defects fixed in the consumer's fetch-to-delivery path, the read path turned
into a real prefetch pipeline, and schema-registry support replaced by a generic
serialization hook.

Dropping the registry is a scope change, not a capability loss: the client now
lives in [`schemreg`](https://crates.io/crates/schemreg), which also has native
Apicurio support and real Avro / Protobuf / JSON codecs that krafka never had.

Six breaking changes — four from that removal, two in the consumer's offset
accessors. See **Breaking** below before upgrading.

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
  literals.** A token endpoint on a non-default port —
  `https://idp.example.com:8443/oauth2/token` — was sent
  `Host: idp.example.com`, violating RFC 9110 §7.2. Name-based virtual
  hosting, nginx `server_name`, Envoy virtual hosts and ALB host-header rules
  all failed to match, and an OIDC token endpoint validating the request
  authority against its issuer rejected the token request. An IPv6 endpoint
  was sent `Host: ::1`, which is not a parseable authority. Affects the OIDC
  token provider (`oauth-oidc`), now the built-in HTTP client's only caller.

- **`assign()` leaked state for partitions it dropped.** Narrowing a manual
  assignment — `assign("t", vec![0, 1])` then `assign("t", vec![0])` — left
  partition 1's position, cached watermarks, paused flag and buffered records
  behind. The buffered records were the damaging part: commits are clamped to
  the lowest buffered offset, so a partition the caller had stopped consuming
  kept dragging back the commit for one it had not. A narrower `assign()` now
  revokes what it drops, exactly as a rebalance does.

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

### Changed

- **Lag counts records read ahead into the buffer.** `lag()`, `current_lag()`,
  `is_caught_up()` and the `lag` / `lag_max` metrics measure from the delivered
  position rather than the fetch position, because a record that has been
  fetched but not returned is still backlog from the application's point of
  view. All five, plus `position()` and `commit()`, are now derived from one
  boundary — the lowest offset still awaiting delivery.

- The built-in HTTP/1.1 client (`src/http.rs`) is compiled for `oauth-oidc`
  only. It stays because the OIDC token provider uses it — removing the registry
  client did not remove the need for an HTTP client, and claiming otherwise
  would have been the easy overstatement here.

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

- `Consumer::cached_high_watermark` — the partition's log-end offset regardless
  of isolation level, for callers that want the value `cached_end_offset` used
  to return under `read_committed`.
- `Consumer::cached_last_stable_offset` — the first offset belonging to an open
  transaction. The gap between it and the high watermark is the volume of
  in-flight transactional data.
- `Consumer::fetch_position` — the offset the next fetch starts from, for
  callers that want the read-ahead value rather than the delivered one.

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

### Documentation

- **`just docs-test` now exists and runs in CI.** `xtask/doc_api.py` had
  documented it as the stronger guarantee for two releases; the recipe was never
  written. It compiles every snippet marked ```` ```rust,compile ```` against
  the real crate. 192 of 321 Rust blocks across the README and the guides are
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

[Unreleased]: https://github.com/hupe1980/krafka/compare/v0.20.0...HEAD
[0.20.0]: https://github.com/hupe1980/krafka/compare/v0.19.1...v0.20.0
[0.19.1]: https://github.com/hupe1980/krafka/compare/v0.19.0...v0.19.1
[0.19.0]: https://github.com/hupe1980/krafka/compare/v0.18.0...v0.19.0
[0.18.0]: https://github.com/hupe1980/krafka/compare/v0.17.0...v0.18.0
[0.17.0]: https://github.com/hupe1980/krafka/compare/v0.16.0...v0.17.0
[0.16.0]: https://github.com/hupe1980/krafka/compare/v0.15.0...v0.16.0
[0.15.0]: https://github.com/hupe1980/krafka/compare/v0.14.0...v0.15.0
[0.14.0]: https://github.com/hupe1980/krafka/compare/v0.13.0...v0.14.0
[0.13.0]: https://github.com/hupe1980/krafka/compare/v0.12.0...v0.13.0
[0.12.0]: https://github.com/hupe1980/krafka/compare/v0.11.0...v0.12.0
[0.11.0]: https://github.com/hupe1980/krafka/compare/v0.10.0...v0.11.0
[0.10.0]: https://github.com/hupe1980/krafka/compare/v0.9.2...v0.10.0
