# 🦀 krafka

[![CI](https://github.com/hupe1980/krafka/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/krafka/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/krafka.svg)](https://crates.io/crates/krafka)
[![Documentation](https://docs.rs/krafka/badge.svg)](https://docs.rs/krafka)
[![MSRV](https://img.shields.io/badge/MSRV-1.88-blue.svg)](https://github.com/rust-lang/rust/releases/tag/1.88.0)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

A pure-Rust, async-native Apache Kafka client. No librdkafka, no C toolchain,
no `unsafe`, no panics — enforced by the compiler, not by convention. Protocol
parity with Apache Kafka 4.3, checked in CI against Kafka's own schemas.

## ✨ Why krafka

**Pure Rust, and it stays that way.** No librdkafka, no C toolchain, no
cross-compilation surprises. The optional `zstd` feature is the single
exception, and it is opt-in.

**The safety posture is enforced by the compiler.** `unsafe_code = "deny"`
crate-wide, plus `panic`, `unwrap` and `expect` denied across the whole crate.
A malformed broker response cannot panic the process, and every allocation
from untrusted input is bounded twice — by the declared count *and* by the
bytes actually available.

**Protocol currency is a build failure, not a bug report.** Every API version
is tracked against **Apache Kafka 4.3** — Fetch v18 (KIP-1166), Produce v13,
Metadata v13, DescribeLogDirs v5 (KIP-1066), DescribeQuorum v2
(KIP-836/853) — and CI diffs krafka's version table against Kafka's own
message schemas. `ApiVersions` is negotiated rather than pinned, so a 3.9
broker gets 3.9-era versions with no configuration.

**Correctness where it is hardest.** KIP-320 truncation detection on all three
legs — Fetch *and* ListOffsets *and* persisted through OffsetCommit, so it
survives restarts and rebalances. KIP-447 zombie fencing with a transaction
state machine that refuses the KAFKA-17754 abort-after-commit-timeout hazard.
`OUT_OF_ORDER_SEQUENCE_NUMBER` verified head-of-line before any rewind, so a
silent gap raises a fatal error instead of reporting success.

**Per-partition ordering is structural, not a setting you can get wrong.** The
producer has exactly one send path, and it keeps exactly one batch per
partition on the wire, dispatched in the order batches were sealed. Sequence
order and wire order cannot diverge, so there is no
`max.in.flight.requests.per.connection ≤ 5` rule to remember, a retry cannot
reorder a partition, and `Arc<Producer>` shared across a hundred tasks is
simply correct. Different partitions still proceed concurrently.

### What is in the box

| | |
|---|---|
| **Clients** | Producer — one send path that batches at every `linger` setting including `0`, with compression, idempotence, transactions and tombstones (null keys and values are `Option` end to end) · Consumer (classic **and** KIP-848 server-side assignment with validated revoke-before-assign reconciliation) · `ShareConsumer` (KIP-932 at Kafka 4.2 parity, incl. KIP-1222 `Renew` and KIP-1206 `ShareAcquireMode`) · full `AdminClient` |
| **Security** | rustls TLS/mTLS with **hot certificate reload** (KIP-1288) · SASL PLAIN, SCRAM-SHA-256/512, OAUTHBEARER · built-in OIDC provider for `client_credentials` (KIP-768) and RFC 7523 client assertions (KIP-1258), with no cryptography dependency added · AWS MSK IAM · every mechanism composes with TLS through one `with_tls`, asserted reachable over both `SASL_PLAINTEXT` and `SASL_SSL` at compile time |
| **Consistency** | Every client shares one configuration surface and one operational surface (`close`, `rebootstrap`, `update_seed_brokers`, `refresh_tls`, `metrics`) — asserted at compile time, builders included. One builder per client, with `build_config()` to validate without a broker and `build()` to validate and connect, both through the same validator. The transactional producer mirrors the plain one setter for setter, minus the two settings transactions fix (`acks`, `idempotent`) |
| **Tuning** | Per-codec compression levels (Gzip 0–9, Zstd through 22) validated against the selected codec at build time, so a level set on a codec that has none is rejected rather than ignored — on the plain and the transactional producer alike |
| **Transport** | One `TransportConfig` on every builder — pass the same instance to every client that shares a network path: TCP keepalive, response ceiling, in-flight cap, idle eviction, file-descriptor cap · KIP-227 incremental fetch sessions · SOCKS5 |
| **Observability** | Lock-free counters, gauges and latency histograms with bounded per-topic cardinality · Prometheus export · producer interceptors and dead-letter queues on **both** producers, over the single send path, with a per-record `RecordContext` carrying a span or timer from `on_send` to a terminal callback that fires for **every** record `on_send` saw · OAUTHBEARER token-fetch counters and expiry gauge · OpenTelemetry semantic conventions |
| **Hardening** | Secret zeroization · constant-time comparison (`subtle`) · decompression-bomb limits · decode-loop bounds · RFC 3986 path encoding on every outbound HTTP target · CI forbids any credential-bearing type from deriving `Debug` |
| **Testing** | 2 350+ tests · 6 cargo-fuzz targets · proptest round-trips across the protocol layer · an in-process fake broker with fault injection that serves the **full transaction protocol** — KIP-360 fencing, commit/abort markers, `read_committed` isolation, TV1 and KIP-890 TV2 — so even exactly-once tests need no Docker |

> **Broker versions:** krafka requires **Apache Kafka 3.9+**; protocol versions below that baseline have been removed. Features needing a newer broker (KIP-848 consumer groups, KIP-932 share groups, KIP-1066 cordoned log dirs) say so where they are documented and fail with a clear `UnknownApiVersion` rather than silently degrading. The Docker integration suite runs against every supported minor in one command (`just integration-matrix`, Kafka 3.9 → 4.3).
>
> **Redpanda** works out of the box: every API version is negotiated, and transactions fall back to KIP-890 TV1 automatically (Redpanda has no server-side TV2) — pinned by a dedicated smoke suite (`just integration-redpanda`). APIs Redpanda does not implement (share groups, log-dir admin) fail fast with `UnknownApiVersion`, same as against an older Kafka.

## 🚀 Quick Start

```sh
cargo add krafka
cargo add tokio --features full

# For AWS MSK IAM authentication with the full SDK credential chain:
cargo add krafka --features aws-msk
```

### Producer

```rust,compile
use krafka::producer::Producer;
use krafka::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let producer = Producer::builder()
        .bootstrap_servers("localhost:9092")
        .client_id("my-producer")
        .build()
        .await?;

    // Send a message
    let metadata = producer
        .send("my-topic", Some(b"key"), Some(b"Hello, Kafka!"))
        .await?;

    // A null value is a tombstone: on a compacted topic it deletes the key.
    producer.send("my-topic", Some(b"key"), None).await?;
    
    println!("Sent to partition {} at offset {}", 
             metadata.partition, metadata.offset);

    producer.close().await;
    Ok(())
}
```

### Consumer

```rust,compile
use krafka::consumer::{Consumer, AutoOffsetReset};
use krafka::error::Result;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let consumer = Consumer::builder()
        .bootstrap_servers("localhost:9092")
        .group_id("my-consumer-group")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await?;

    consumer.subscribe(&["my-topic"]).await?;

    loop {
        let records = consumer.poll(Duration::from_secs(1)).await?;
        for record in records {
            if let Some(ref value) = record.value {
                println!(
                    "Received: topic={}, partition={}, offset={}, value={:?}",
                    record.topic,
                    record.partition,
                    record.offset,
                    String::from_utf8_lossy(value)
                );
            }
        }
    }
}
```

### Admin Client

```rust,compile
use krafka::admin::{AdminClient, NewTopic};
use krafka::error::Result;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    let admin = AdminClient::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .await?;

    // Create a topic
    // `new` validates the topic name, so it returns a Result.
    let topic = NewTopic::new("new-topic", 6, 3)?
        .with_config("retention.ms", "604800000");

    admin.create_topics(vec![topic], Duration::from_secs(30), false).await?;

    // List topics
    let topics = admin.list_topics().await?;
    println!("Topics: {:?}", topics);

    Ok(())
}
```

### Transactional Producer

For exactly-once semantics across multiple partitions:

```rust,compile
use krafka::producer::TransactionalProducer;
use krafka::error::Result;

#[tokio::main]
async fn main() -> Result<()> {
    let producer = TransactionalProducer::builder()
        .bootstrap_servers("localhost:9092")
        .transactional_id("my-transaction")
        .build()
        .await?;

    // Initialize transactions (once per producer)
    producer.init_transactions().await?;

    // Atomic transaction
    producer.begin_transaction()?;
    producer.send("topic-a", Some(b"key"), Some(b"value1")).await?;
    producer.send("topic-b", Some(b"key"), Some(b"value2")).await?;
    producer.commit_transaction().await?;

    Ok(())
}
```

### Authentication

Connect to secured Kafka clusters with SASL, SCRAM, OAUTHBEARER, or AWS MSK IAM — available on all client types:

```rust
use krafka::producer::Producer;
use krafka::consumer::Consumer;
use krafka::AdminClient;

// Producer with SASL_SSL + SCRAM-SHA-512 (the usual managed-Kafka listener)
use krafka::auth::{AuthConfig, TlsConfig};
let producer = Producer::builder()
    .bootstrap_servers("broker:9093")
    .auth(AuthConfig::sasl_scram_sha512_ssl("username", "password", TlsConfig::new()))
    .build()
    .await?;

// Any mechanism composes with TLS through `with_tls`
let auth = AuthConfig::sasl_scram_sha256("username", "password")
    .with_tls(TlsConfig::new().with_ca_cert("/etc/kafka/ca.pem"));

// Consumer with SASL/PLAIN
let consumer = Consumer::builder()
    .bootstrap_servers("broker:9092")
    .group_id("secure-group")
    .sasl_plain("username", "password")
    .build()
    .await?;

// Producer with SASL/OAUTHBEARER
let producer = Producer::builder()
    .bootstrap_servers("broker:9093")
    .sasl_oauthbearer("your-jwt-token")
    .build()
    .await?;

// Admin with AWS MSK IAM
let auth = AuthConfig::aws_msk_iam("access_key", "secret_key", "us-east-1");
let admin = AdminClient::builder()
    .bootstrap_servers("broker:9094")
    .auth(auth)
    .build()
    .await?;
```

## 📦 Modules

| Module | Description |
|--------|-------------|
| `producer` | Batching, compression, idempotence, transactions, partitioners |
| `consumer` | Consumer groups (classic + KIP-848), offsets, rebalancing, compacted-topic tables |
| `share_consumer` | KIP-932 share groups — queue semantics on a Kafka topic *(`share-groups`, on by default; needs a Kafka 4.2+ broker)* |
| `admin` | Cluster administration: topics, partitions, groups, configs, ACLs, quotas, tokens |
| `client` | `KrafkaClient` — one connection pool and metadata cache shared by several clients |
| `auth` | SASL PLAIN / SCRAM / OAUTHBEARER / AWS MSK IAM, TLS and mTLS |
| `serdes` | `Serializer` / `Deserializer` hooks applied on the way to and from the wire |
| `interceptor` | Producer and consumer hooks for tracing and enrichment |
| `dlq` | Dead-letter queues for records that exhaust their retries |
| `metrics` | Lock-free counters, gauges and latency histograms; Prometheus export |
| `telemetry` | KIP-714 broker-driven client telemetry and OTLP export *(`telemetry`)* |
| `tracing_ext` | OpenTelemetry semantic-convention fields for `tracing` spans |
| `testing` | In-process fake broker with fault injection *(`test-broker`)* |
| `error` | `KrafkaError`, `ErrorCode`, `ProtocolErrorKind` and retriability classification |
| `util` | Backoff policy, varint codecs, CRC32C, bootstrap-server parsing |
| `prelude` | One glob import for the common types (`use krafka::prelude::*`) |

Three more modules are public but `#[doc(hidden)]` — `protocol`, `network` and
`metadata`. They are reachable for advanced use (custom authenticators, raw
record batches, benchmarks) but are **not** part of the stable API surface.

## 🗜️ Compression

krafka supports all Kafka compression codecs, individually feature-gated:

```rust,compile
use krafka::producer::Producer;
use krafka::protocol::Compression;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .compression(Compression::Lz4)  // Fast compression
    .build()
    .await?;
```

| Codec | Cargo Feature | Crate | Characteristics |
|-------|---------------|-------|-----------------|
| `Compression::Gzip` | `gzip` | flate2 | Best ratio, slower |
| `Compression::Snappy` | `snappy` | snap | Good balance |
| `Compression::Lz4` | `lz4` | lz4_flex | Fastest |
| `Compression::Zstd` | `zstd` | zstd | Best modern choice (requires C toolchain) |

The default `compression` feature enables the pure-Rust codecs: gzip, snappy,
and LZ4. Zstd remains available through the explicit `zstd` or
`compression-all` feature because it requires a C toolchain via `zstd-sys`.
To select only what you need:

```sh
# Only the codecs you need. `--no-default-features` also drops the default
# `ring` TLS backend, so a crypto backend must be named explicitly.
cargo add krafka --no-default-features --features lz4,snappy,ring

# Or every codec, including zstd:
cargo add krafka --features compression-all
```

### TLS crypto backend

krafka uses `rustls` and needs exactly one crypto backend. `ring` is the
default; `rustls-aws-lc-rs` selects aws-lc-rs instead, which is the better
choice on AWS Graviton and in FIPS-oriented deployments:

```sh
cargo add krafka --no-default-features --features rustls-aws-lc-rs,compression
```

The two backends are **additive**, not mutually exclusive — a transitive
dependency may well enable the other one, and Cargo would have no way to
resolve a conflict if they were exclusive. When both are compiled in,
aws-lc-rs deterministically wins. krafka always selects the provider
explicitly rather than letting `rustls` infer it from crate features, so the
combination cannot produce a runtime panic. Installing a process-wide provider
with `CryptoProvider::install_default()` overrides the choice for the whole
application, krafka included.

## 🛠️ Development

Tasks are driven by [`just`](https://just.systems). The `justfile` is the single
source of truth for what the checks are — CI calls the same recipes, so a check
cannot pass locally and fail in CI because the two drifted apart.

```bash
just              # list every recipe
just ci           # everything CI runs, except the Docker-backed suites
just ci-full      # ci + supply-chain audit + Docker integration tests
just pre-commit   # the fast subset (fmt, clippy, check)
just install-hooks  # wire pre-commit into .git/hooks
just t <pattern>  # run one test by name, with output

just bench-baseline  # record the performance reference
just bench-check     # fail if the send path regressed >10% since then
just semver-check    # classify API changes against the last published release
```

Individual recipes mirror one CI job each: `fmt-check`, `clippy`, `check`,
`protocol-parity`, `secret-debug`, `test`, `test-ring`,
`test-cross-platform`, `minimal-features`, `doc`, `deny`, `integration`, `msrv`.
`just ci-job-parity` asserts that pairing holds.

`bench-check` and `semver-check` sit outside `just ci`: both are slow and noisy
on a shared runner, and pre-1.0 a detected API break is allowed rather than
fatal.

### Checks that exist because a review found what they now catch

**`tests/builder_surface.rs`** — a compile-time assertion that every client
builder accepts a `TransportConfig`, offers a synchronous `build_config()`
alongside the async `build()`, exposes `refresh_tls()`, and keeps metrics and
version negotiation callable without an async context. Every line fails to
compile if the method it names disappears.

It also asserts two matrices that a per-client check could not see. Every
`SaslMechanism` must be constructible under **both** `SASL_PLAINTEXT` and
`SASL_SSL` from the public API alone — `SASL_SSL` + SCRAM, the default secured
listener on most managed Kafka offerings, was unreachable from outside the crate
because the `_ssl` constructors were a hand-maintained list and SCRAM was missing
from it. And both producer builders must expose the same configuration surface —
the transactional one was missing seventeen setters, including the
`build_config()` this file already promised for every client.

This replaced a Python parity script. krafka used to have two builders per
client — 72 hand-maintained forwarding methods whose config half nothing outside
the crate's own tests ever called — and the script checked that they stayed in
sync. It found two real defects, then missed a third: both producer builders had
a `compression` method, but only the unused one *validated* it, so
`.compression(Zstd)` without the `zstd` feature built a producer that failed on
its first send. A parity check compares surfaces; the divergence had moved
underneath it. Deleting the duplication removed both the defect class and the
need for the script, and Rust checks reachability better than a regex can.

**`just protocol-parity`** — the API version table is diffed against Apache
Kafka's own message schemas: names and keys agree, MIN is still a version Kafka
accepts, MAX neither overstates (claiming a version marked
`latestVersionUnstable`) nor understates (declining a stable one), and the
flexible-version boundary matches. This is how `Fetch` v17/v18 sat implemented,
documented and unreachable for two Kafka releases.

It reads a vendored snapshot, so it needs no network and cannot flake. Track a
newer Kafka release deliberately:

```bash
just refresh-protocol-snapshot 4.3   # rewrite the snapshot; review the diff
just protocol-parity                 # see what krafka must do about it
```

**`just ci-job-parity`** — every recipe in `just ci` has a CI job, one required
check (`ci-success`) gates every job, and that check carries `if: always()`.

The justfile being the source of truth has a blind spot: a recipe wired to no
workflow is invisible from both sides, because each list is internally complete
and neither is compared to the other. `just integration-sasl` ran in no workflow
for its entire life, and `just docs-test` gated every compiled documentation
snippet while no pull request ran it.

`if: always()` is not cosmetic: GitHub leaves a required check pending forever
when its job never runs, and reads a **skipped** required check as success.

**`just bench-check`** — the send path has not regressed more than 10% against
the recorded baseline, measured against the in-process fake broker.

These are krafka-vs-krafka numbers. The harness holds a single lock and keeps
its log in memory, so **no figure it produces is quotable** and none appears in
this README. It is still the right tool for detecting a regression: a constant
overhead cancels when you subtract two runs of it.

**`just secret-debug`** — no credential-bearing type may derive `Debug`.
`Debug` is the quiet way secrets reach a log aggregator: a `tracing` field, an
error context or a panic message that formats the enclosing struct is enough,
and nobody has to log the secret deliberately. Two instances shipped before this
check existed — the OIDC client secret, and `SaslAuthenticateRequest.auth_bytes`,
which for SASL/PLAIN is `\0username\0password` in cleartext.

## ⚡ Performance Tuning

### High Throughput Producer

```rust,compile
use krafka::producer::{Producer, Acks};
use krafka::protocol::Compression;
use std::time::Duration;

let producer = Producer::builder()
    .bootstrap_servers("localhost:9092")
    .acks(Acks::Leader)
    .compression(Compression::Lz4)
    .batch_size(1048576)                  // 1MB batches
    .linger(Duration::from_millis(10))    // Allow batching
    .build()
    .await?;
```

### Low Latency Consumer

```rust,compile
use krafka::consumer::Consumer;
use std::time::Duration;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("low-latency")
    .fetch_min_bytes(1)
    .fetch_max_wait(Duration::from_millis(10))
    .build()
    .await?;
```

### Transport tuning

Socket- and pool-level settings live on one `TransportConfig`, accepted by every
builder (`Producer`, `Consumer`, `AdminClient`, `TransactionalProducer`,
`ShareConsumer`, `KrafkaClient`). The defaults reproduce krafka's historical
behaviour exactly, so this is opt-in.

```rust,compile
use krafka::network::TransportConfig;
use krafka::consumer::Consumer;
use std::time::Duration;

let transport = TransportConfig::builder()
    // Beat the idle timeout of whatever NAT gateway or load balancer sits
    // between you and the brokers — the usual cause of "the consumer stops
    // receiving after exactly N minutes".
    .tcp_keepalive(Some(Duration::from_secs(30)))
    // Kafka returns at least one full record batch per partition even when it
    // exceeds fetch.max.bytes. Raise this above the topic's max.message.bytes
    // or that partition stalls permanently.
    .max_response_size(200 * 1024 * 1024)
    // Bound worst-case memory: the per-connection ceiling is
    // max_response_size × max_in_flight_requests.
    .max_in_flight_requests(5)
    // Bound file descriptors on a cluster whose broker count can jump.
    .max_connections(Some(64))
    // Re-read certificates from disk hourly (KIP-1288).
    .tls_reload_interval(Some(Duration::from_secs(3600)))
    .build()?;

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("tuned")
    .transport(transport)
    .build()
    .await?;
```

### TLS certificate rotation (KIP-1288)

Two paths, because rotation happens two ways:

```rust,compile
// Event-driven: an inotify watch or a sidecar signal fired.
producer.refresh_tls().await?;

// Unattended: set `tls_reload_interval` above and krafka reloads on a timer.
```

Existing TLS sessions keep the certificates they handshaked with and are
replaced as connections cycle. A reload that fails — a half-written PEM caught
mid-rotation — is logged and the previous material stays active, so a
non-atomic rotation converges on the next attempt instead of breaking every new
connection in between.

## 🎯 Delivery Semantics

Pick the mode that matches what your data is worth.

| Mode | Guarantee | Cost |
|------|-----------|------|
| `acks=0` | At-most-once. No durability — the record may never reach the log. | Lowest latency |
| `acks=all` + idempotence (**default**) | At-least-once, ordered and gap-free per partition. Batches for a partition are serialised in seal order; sequence numbers are monotonic and never reused. | One round trip to the ISR |
| Transactions + `send_offsets_to_transaction` | Exactly-once across a read-process-write cycle. | Transaction coordinator round trips |

Under `acks=0`, `RecordMetadata::confirmation` reports `Unacknowledged` — do not
mistake the returned metadata for a durability guarantee.

### Exactly-once

`send_offsets_to_transaction` takes a [`ConsumerGroupMetadata`], not a bare group
ID. That metadata is what lets the group coordinator fence a **zombie**: an
instance that was partitioned away, lost its partitions to a rebalance, and came
back still holding a transaction. Without it the coordinator accepts the zombie's
commit and it overwrites the position of the member that now owns the partition.

```rust
// Re-read for every transaction. The generation changes on every rebalance,
// so a cached value stops fencing at exactly the moment it matters.
let group_metadata = consumer
    .group_metadata()
    .await
    .ok_or("consumer has not joined the group yet")?;

producer.begin_transaction()?;
producer.send("out-topic", Some(b"key"), Some(b"value")).await?;
producer.send_offsets_to_transaction(&offsets, &group_metadata).await?;
producer.commit_transaction().await?;
```

See [`examples/exactly_once.rs`](examples/exactly_once.rs) for the full
read-process-write loop.

## 🧩 Consumer Groups

Both rebalance protocols are supported, but they are no longer equals.

**Prefer `GroupProtocol::Consumer` (KIP-848).** It has been production ready
since Apache Kafka 4.0: the coordinator computes assignments server-side, a
rebalance reconciles incrementally instead of stopping every member, and a slow
member affects only its own partitions. Apache Kafka 4.3 began *deprecating*
the classic protocol (KIP-1274 phase 1 — warn in 4.3, default flips in 5.0,
removed in 6.0), and krafka logs the same warning once per process when a group
starts on it.

```rust,compile
use krafka::consumer::{Consumer, GroupProtocol};

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("my-group")
    .group_protocol(GroupProtocol::Consumer)   // KIP-848
    .build()
    .await?;
```

`Classic` remains the default for now, so that upgrading krafka is never itself
a protocol migration: krafka supports Kafka 3.9 brokers, and KIP-848 needs 4.0
(or 3.7–3.9 with `group.coordinator.new.enable=true`). The two protocols cannot
mix within one group on pre-4.0 brokers — move every member together, or
upgrade the cluster first.


Four assignors ship: `Range`, `RoundRobin`, `Sticky` (eager), and
`CooperativeSticky`. The default is the preference list
`[Range, CooperativeSticky]`, matching the Java client — every member advertises
both, so a group moves from eager to cooperative rebalancing in a single rolling
bounce rather than a full stop-the-world restart.

Cooperative rebalances enforce revoke-before-assign structurally: a partition
moving between two live members is withheld from its new owner for one
generation — the previous owner revokes it, then the follow-up rebalance
delivers it — so two members of one group can never consume the same partition
concurrently, and the new owner's committed-offset fetch cannot race the old
owner's final commit.

Rebalances do not wait on your poll loop. The background group task keeps
heartbeating through a rebalance and sends `JoinGroup`/`SyncGroup` itself, so a
consumer that is idle or busy between `poll()` calls does not hold the rest of
the group up. The new assignment is applied — and your rebalance listener
called — on the next `poll()`, so callbacks and record delivery stay on one
thread and an offset commit cannot race a revocation.

`max.poll.interval.ms` is still enforced: an application that genuinely stops
polling leaves the group so its partitions are reassigned promptly, rather than
holding them while a background task vouches for it. Static members
(`group.instance.id`) instead keep their assignment until the session expires,
so a restart can reclaim it.

## 📐 Scope

krafka speaks the **client** side of the Kafka protocol: 60+ API keys covering
produce, fetch, group coordination, transactions, share groups, and
administration, tracked against Apache Kafka 4.3.

Broker-internal APIs (`LeaderAndIsr`, `UpdateMetadata`, `Vote`, `FetchSnapshot`,
`BrokerHeartbeat`, the share-group state persister, …) are deliberately absent
— a client does not speak them. They are still *named* in the `ApiKey` enum, so
an `ApiVersions` response from a modern broker decodes to something readable
rather than `Unknown(87)`.

Not implemented: `StreamsGroupHeartbeat` (KIP-1071, key 88). Its request carries
the Streams application topology, which is group-wide state — a client with no
Streams runtime cannot send a truthful one. Its sibling `StreamsGroupDescribe`
(key 89) *is* implemented, as `AdminClient::describe_streams_groups`.

**Schema registries are out of scope**, as they are for every comparable client
— Java's `kafka-clients` has none, librdkafka has none, franz-go keeps `pkg/sr`
out of `kgo`. A registry is a different service with a different protocol, auth
model and release cadence. krafka provides the *hook* (`serdes::Serializer` /
`Deserializer`, the equivalent of Java's `key.serializer`); pair it with
[`schemreg`](https://crates.io/crates/schemreg) for Confluent, AWS Glue or
Apicurio, and Avro / Protobuf / JSON codecs. See the
[Cookbook](https://hupe1980.github.io/krafka/docs/cookbook/#use-a-schema-registry).

Tokio is the async runtime.

### Authentication

SASL/PLAIN, SASL/SCRAM-SHA-256/512 (with RFC 5929 channel binding), SASL/OAUTHBEARER
(with proactive token refresh, a built-in OIDC `client_credentials` provider and
KIP-1258 client assertions behind the `oauth-oidc` feature), AWS MSK IAM, and mTLS.

GSSAPI/Kerberos is outside the scope of this client.

## 🧪 Testing Against a Fake Broker

Enable the `test-broker` feature to get an in-process Kafka broker your tests can
drive directly. Real `Producer`/`Consumer`/`AdminClient` instances connect to it
over a real TCP socket, so you exercise the actual client — no Docker, no
containers, and failure modes you cannot reproduce against a healthy cluster.

```rust,compile
use krafka::testing::{Control, FakeBroker};
use krafka::protocol::ApiKey;
use krafka::error::ErrorCode;

let broker = FakeBroker::start().await?;

// Make the next CreateTopics land on a non-controller and assert the client
// refreshes metadata and retries instead of surfacing the error.
broker.on(ApiKey::CreateTopics, |_| Control::Error(ErrorCode::NotController));

let admin = AdminClient::builder()
    .bootstrap_servers(broker.bootstrap_servers())
    .build()
    .await?;
```

`Control` covers `Error`, `Delay`, `DelayThen`, `Disconnect`, `Silence`,
`CorruptRecords` and pass-through. The cluster is mutable mid-test
(`set_leader`, `bump_leader_epoch`, `set_group_coordinator`, `set_controller`,
`set_broker_online`), and `set_api_versions` makes the broker advertise an
*older* API range so the client's degradation branches are reachable.

It serves the produce/fetch path, both group protocols — classic and KIP-848
with real revoke-before-assign reconciliation — KIP-932 share groups with the
share-partition state machine, KIP-584 feature administration, and the full
transaction protocol.

Transactions are modelled end to end, which is what makes exactly-once testable
without a cluster: `InitProducerId` returns a stable producer ID per
transactional ID with an epoch that rises on every re-initialisation (KIP-360
fencing), `EndTxn` writes real commit and abort control batches, offsets staged
by `TxnOffsetCommit` apply only on commit, and a `read_committed` fetch stops at
the last stable offset and reports aborted transactions so the consumer's own
filtering runs for real. `set_transaction_version(2)` finalizes the
`transaction.version` feature and the client negotiates KIP-890 TV2 from it —
the same route a real cluster takes — so both protocols are reachable.

```rust
broker.set_transaction_version(2);
// ...run a transaction through a TransactionalProducer...
assert_eq!(broker.request_count(ApiKey::AddPartitionsToTxn), 0);
```

See the **[Testing guide](https://hupe1980.github.io/krafka/docs/testing/)** for
what it does and, just as importantly, what it deliberately does not model.


## ⬆️ Upgrading

krafka is pre-1.0: a **minor** bump may carry breaking changes, and every one
is listed in the `Breaking` section of that release in
[CHANGELOG.md](CHANGELOG.md).

## 📚 Documentation

Full documentation: **[hupe1980.github.io/krafka](https://hupe1980.github.io/krafka)** ·
API reference: **[docs.rs/krafka](https://docs.rs/krafka)** ·
Release history: **[CHANGELOG.md](CHANGELOG.md)**

| Start here | Clients | Integration | Operations | Reference |
|---|---|---|---|---|
| [Getting Started](https://hupe1980.github.io/krafka/docs/getting-started/) | [Producer](https://hupe1980.github.io/krafka/docs/producer/) | [Authentication](https://hupe1980.github.io/krafka/docs/authentication/) | [Metrics](https://hupe1980.github.io/krafka/docs/metrics/) | [Protocol Support](https://hupe1980.github.io/krafka/docs/protocol/) |
| [Cookbook](https://hupe1980.github.io/krafka/docs/cookbook/) | [Consumer](https://hupe1980.github.io/krafka/docs/consumer/) | | [Performance](https://hupe1980.github.io/krafka/docs/performance/) | [Architecture](https://hupe1980.github.io/krafka/docs/architecture/) |
| [Configuration](https://hupe1980.github.io/krafka/docs/configuration/) | [Share Consumer](https://hupe1980.github.io/krafka/docs/share-consumer/) | [Interceptors](https://hupe1980.github.io/krafka/docs/interceptors/) | [Testing](https://hupe1980.github.io/krafka/docs/testing/) | |
| | [Admin Client](https://hupe1980.github.io/krafka/docs/admin/) | | [Error Handling](https://hupe1980.github.io/krafka/docs/errors/) | |

The site is built with [Zola](https://www.getzola.org) from `site/`. Run
`just site-serve` for a local preview with live reload.

## 🎮 Examples

Run the examples with:

```bash
# Producer example
cargo run --example producer

# Consumer example
cargo run --example consumer

# Advanced consumer example (pause/resume, seek, manual commits)
cargo run --example consumer_advanced

# Admin client example
cargo run --example admin

# Transactional producer example
cargo run --example transactional_producer

# Exactly-once read-process-write (KIP-447 zombie fencing)
cargo run --example exactly_once

# Authentication examples (SASL, SCRAM, MSK IAM)
cargo run --example authentication
```

## 🤝 Contributing

Contributions are welcome!

## 📄 License

Licensed under either the [MIT License](LICENSE-MIT) or the [Apache License 2.0](LICENSE-APACHE), at your option.
