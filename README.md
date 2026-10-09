# 🦀 krafka

[![CI](https://github.com/hupe1980/krafka/actions/workflows/ci.yml/badge.svg)](https://github.com/hupe1980/krafka/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/krafka.svg)](https://crates.io/crates/krafka)
[![Documentation](https://docs.rs/krafka/badge.svg)](https://docs.rs/krafka)
[![MSRV](https://img.shields.io/badge/MSRV-1.95-blue.svg)](https://github.com/rust-lang/rust/releases/tag/1.95.0)
[![License](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE-MIT)

A pure-Rust, async-native Apache Kafka client: producer, transactions,
consumer groups, share groups and a full admin client, on Tokio. No C library
and no system dependency in the default build, no `unsafe`, no panics on a
malformed response. Protocol versions tracked against
<!-- generated:kips:kafka-ref -->Apache Kafka 4.3<!-- /generated -->, checked in CI against Kafka's own schemas.

## 🚀 Quick start

```sh
cargo add krafka
cargo add tokio --features full
```

```rust,compile
use krafka::{Kafka, Record};

let kafka = Kafka::builder("localhost:9092").connect().await?;
let producer = kafka.producer().build().await?;
producer.send(Record::new("orders", "hello").key("k")).await?;

let consumer = kafka.consumer("my-group").build().await?;
consumer.subscribe(["orders"]).await?;
while let Some(rec) = consumer.recv().await? { println!("{rec:?}"); }
```

`Kafka` holds every connection setting — bootstrap servers, client id, TLS and
SASL, transport, proxy, metadata — and one connection pool. Every client is
built from it and shares the pool: `kafka.producer()`, `kafka.consumer(group)`,
`kafka.consumer_without_group()`, `kafka.share_consumer(group)`,
`kafka.admin()`. A second pool is a second `Kafka`. Every client is
`Send + Sync` and ends with `close().await`.

## ✨ Why krafka

**No C library, no system dependency.** The default build needs a Rust
toolchain and the C compiler `cc` finds for `ring`, and nothing else: no
OpenSSL, no librdkafka, no CMake, no pkg-config. `ring` is the only crate in
the default graph that compiles C; the features that compile more (`zstd`,
`rustls-aws-lc-rs`, `aws-msk`) say so below. `just no-c` checks this for six
targets.

**A broker response is untrusted input.** `unsafe_code`, `panic`, `unwrap` and
`expect` are denied crate-wide. A malformed frame surfaces as a `KrafkaError`,
and every allocation derived from a response is bounded by its declared count
and by the bytes actually available. Credential-bearing types do not derive
`Debug`.

**Acknowledged means written.** The idempotent producer is the default; a
sequence range is never reused, a batch whose outcome is unknown moves the
producer to the next epoch, and `DeliveryTimeout { possibly_written }` tells
you whether a timed-out record may be in the log. A transaction with a failed
send cannot commit. Per-partition order is structural: one send path, at most
five requests in flight per broker, and a retry cannot reorder a partition.

**Protocol currency is mechanical.** The API version table is diffed against
Kafka's message schemas (`just protocol-parity`), every version is negotiated
through `ApiVersions`, and a broker from 3.9 onwards gets the versions it
supports with no configuration.

**Your code is testable without Docker.** The `test-broker` feature ships an
in-process fake Kafka cluster with fault injection; see
[Testing](#-testing-against-a-fake-broker).

### What is in the box

| | |
|---|---|
| **Clients** | `Producer` (`send` waits, `enqueue` pipelines) and `TypedProducer<K, V>` · `TransactionalProducer` with KIP-447 offset commits and two-phase `prepare`/`complete` · `Consumer`, classic and KIP-848 groups · `ShareConsumer` (KIP-932 share groups) · `AdminClient` |
| **Security** | rustls TLS and mTLS with certificate reload (KIP-1288) · SASL PLAIN, SCRAM-SHA-256/512, OAUTHBEARER · built-in OIDC provider (`client_credentials`, RFC 7523 client assertions) · AWS MSK IAM · every mechanism composes with TLS through `with_tls` |
| **Observability** | `metrics()` on every client and on `Kafka` returns one `Metrics` snapshot; `prometheus_text()` renders it · spans through `tracing` on OpenTelemetry messaging conventions · KIP-714 client telemetry to brokers that subscribe · producer and consumer interceptors |
| **Transport** | Set once on the `Kafka` builder: TCP keepalive, response ceiling, in-flight cap, idle eviction, connection cap · a separate coordination connection per broker · KIP-227 incremental fetch sessions · SOCKS5 |
| **Testing** | `krafka::testing::FakeBroker`: real clients over a real or in-memory socket, fault injection per request, the full transaction protocol, both group protocols and share groups *(`test-broker`; unstable, outside semver)* |

> **Broker versions:** Apache Kafka **3.9+**. KIP-848 groups need 4.0, share
> groups 4.2; a cluster without a feature fails with an error naming the
> feature and the setting that avoids it. The Docker suite runs against every
> supported minor (`just integration-matrix`). **Redpanda** works: versions are
> negotiated and transactions use transaction version 1 there
> (`just integration-redpanda`).

## 📦 Cargo features

| Feature | Default | What it adds |
|---|---|---|
| `ring` | **yes** | rustls crypto backend on `ring` (compiles C with `cc`). |
| `rustls-aws-lc-rs` | no | rustls crypto backend on `aws-lc-rs`; offers post-quantum `X25519MLKEM768` first. Needs CMake. |
| `zstd` | no | Zstd *encoding* (`zstd-sys`, compiles C). Zstd decoding is always available. |
| `aws-msk` | no | MSK IAM with the AWS SDK credential chain (pulls `aws-lc-sys`, needs CMake). |
| `oauth-oidc` | no | Built-in OIDC token provider for OAUTHBEARER. No cryptography dependency. |
| `native-tls-roots` | no | The platform's root certificates. |
| `tls-encrypted-keys` | no | Passphrase-encrypted PKCS#8 client keys (`ssl.key.password`). |
| `unstable-protocol` | no | Protocol versions Kafka marks unstable; outside semver. |
| `test-broker` | no | `krafka::testing`, the in-process fake broker; outside semver. |

Always compiled in: gzip, Snappy and LZ4, the share consumer, SOCKS5 and
KIP-714 telemetry. The two TLS backends are additive; with both, `aws-lc-rs`
wins, and a process-default rustls provider overrides either.
`default-features = false` also drops `ring`, so name a backend:

```sh
cargo add krafka --no-default-features --features rustls-aws-lc-rs
```

## 📤 Producer

```rust,compile
use krafka::{Kafka, Record};

let kafka = Kafka::builder("localhost:9092").client_id("my-producer").connect().await?;
let producer = kafka.producer().build().await?;

// `send` waits for the acknowledgement.
let meta = producer
    .send(Record::new("my-topic", "Hello, Kafka!").key("key").header("trace", "abc"))
    .await?;
println!("partition {} offset {}", meta.partition, meta.offset);

// `enqueue` returns once the record is buffered; the handle resolves to the
// acknowledgement. Produce order is enqueue order.
let ack = producer.enqueue(Record::new("my-topic", "pipelined")).await?;
ack.await?;

// A null value is a tombstone.
producer.send(Record::tombstone("my-topic", "key")).await?;
producer.close().await?;
```

## 📥 Consumer

```rust,compile
use krafka::Kafka;
use krafka::consumer::{AutoOffsetReset, GroupProtocol};

let kafka = Kafka::builder("localhost:9092").connect().await?;
let consumer = kafka
    .consumer("my-group")
    .group_protocol(GroupProtocol::Consumer) // KIP-848; needs Kafka 4.0+
    .auto_offset_reset(AutoOffsetReset::Earliest)
    .build()
    .await?;
consumer.subscribe(["my-topic"]).await?;

// `None` means the consumer was closed.
while let Some(record) = consumer.recv().await? {
    println!("{}/{}@{}: {:?}", record.topic, record.partition, record.offset, record.value_str());
    consumer.commit().await?;
}
```

`poll(timeout)` returns a batch. A partition's position is the next record to
hand out; `commit()` commits the positions, `commit_offsets(offsets)` the
offsets you choose, and `lag()` reports position, watermarks and lag per
partition. `GroupProtocol::Classic` is the default and works with every
supported broker; Apache Kafka 4.3 deprecates it in the Java client
(KIP-1274), and krafka logs the same warning once per process. The assignors
are `Range`, `RoundRobin` and `CooperativeSticky`.

## 🔁 Transactions

```rust,compile
use krafka::{Kafka, Record};

let kafka = Kafka::builder("localhost:9092").connect().await?;
// Registers the transactional id, fencing an earlier instance with the same id.
let producer = kafka.producer().build_transactional("my-transaction").await?;

producer.begin()?;
producer.send(Record::new("topic-a", "value1").key("key")).await?;
producer.send(Record::new("topic-b", "value2").key("key")).await?;
if let Err(e) = producer.commit().await {
    if e.requires_abort() {
        producer.abort().await?;
    }
}
producer.close().await?;
```

For consume-transform-produce, `send_offsets(&offsets, &group_metadata)` adds
the consumer's offsets to the transaction. Read `consumer.group_metadata()`
for every transaction: the generation it carries is what lets the coordinator
fence a zombie. See [`examples/exactly_once.rs`](examples/exactly_once.rs).

## 🛠️ Admin client

```rust,compile
use krafka::Kafka;
use krafka::admin::{CreateTopicsOptions, ListTopicsOptions, NewTopic};

let kafka = Kafka::builder("localhost:9092").connect().await?;
let admin = kafka.admin();

let topic = NewTopic::new("new-topic", 6, 3)?.with_config("retention.ms", "604800000");
// One result per topic.
for (name, result) in admin.create_topics([topic], CreateTopicsOptions::default()).await? {
    if let Err(e) = result {
        eprintln!("{name}: {e}");
    }
}
println!("{:?}", admin.list_topics(ListTopicsOptions::default()).await?);
admin.close().await?;
```

Every operation takes an `*Options` struct with a `timeout`; multi-item
operations return one `Result` per item.

## 🔐 Authentication

Security is a connection setting, set once on the handle:

```rust,compile
use krafka::Kafka;
use krafka::auth::{AuthConfig, TlsConfig};

// SASL_SSL with SCRAM-SHA-512.
let kafka = Kafka::builder("broker:9093")
    .security(AuthConfig::sasl_scram_sha512("username", "password").with_tls(TlsConfig::new()))
    .connect()
    .await?;

// Any mechanism composes with TLS through `with_tls`.
let _ = AuthConfig::sasl_plain("username", "password")
    .with_tls(TlsConfig::new().with_ca_cert("/etc/kafka/ca.pem"));
let _ = AuthConfig::sasl_oauthbearer("your-jwt-token");
let _ = AuthConfig::aws_msk_iam("access_key", "secret_key", "us-east-1");
```

Certificates rotate with `kafka.refresh_tls().await?` or on a timer with
`tls_reload_interval`; a reload that fails keeps the previous material.
Recipes for Azure Event Hubs, Google Managed Kafka, Amazon MSK and Confluent
Cloud are in [Cloud Platforms](https://hupe1980.github.io/krafka/docs/cloud/).

## 📈 Observability

```rust,compile
use krafka::Kafka;

let kafka = Kafka::builder("localhost:9092").connect().await?;
let producer = kafka.producer().build().await?;

// One owned snapshot per client; `kafka.metrics()` sums the handle's clients.
let snapshot = producer.metrics();
println!("{}", snapshot.prometheus_text());
```

- **Metrics.** Producer, consumer and connection counters, latencies as
  count/sum/max, Prometheus series named `krafka_*` with a `client_id` label.
- **Spans.** `send`, `poll`, `commit` and `rebalance` spans through `tracing`,
  on OpenTelemetry messaging semantic conventions `krafka::OTEL_SEMCONV_VERSION`.
  Record keys and values are never recorded. krafka depends on no
  OpenTelemetry crate; bridge `tracing` in the application.
- **KIP-714.** Producers and consumers push their metrics to brokers whose
  operator subscribed to them, like Java's `enable.metrics.push`;
  `metrics_push(false)` on the role builder turns it off. The admin client
  pushes only when `metrics_push(true)` is set.

## 🧪 Testing against a fake broker

With the `test-broker` feature, `krafka::testing::FakeBroker` is an
in-process Kafka cluster that real clients talk to. Inject a fault per request,
move leaders, crash brokers, and assert what the client sent:

```rust,compile
use krafka::error::ErrorCode;
use krafka::testing::{ApiKey, Control, FakeBroker};
use krafka::Kafka;

let broker = FakeBroker::start().await?;
broker.on(ApiKey::CreateTopics, |_| Control::Error(ErrorCode::NotController));
let admin = Kafka::builder(broker.bootstrap_servers()).connect().await?.admin();
```

It serves produce and fetch, both group protocols, share groups and the full
transaction protocol, and runs on Tokio's paused clock with
`FakeBroker::start_in_memory`. `krafka::testing` is **unstable**: it is outside
the semver promise. The [Testing guide](https://hupe1980.github.io/krafka/docs/testing/)
shows how to test your own code with it and with testcontainers.

## 📐 Scope

krafka speaks the **client** side of the Kafka protocol, tracked against
<!-- generated:kips:kafka-ref -->Apache Kafka 4.3<!-- /generated -->.

<!-- generated:kips:summary -->
**KIPs named in this documentation:** 58 implemented · 8 partial (KIP-368, KIP-525, KIP-794, KIP-800, KIP-853, KIP-932, KIP-1071, KIP-1242). Each with its status, evidence and reason: [KIP support](https://hupe1980.github.io/krafka/docs/protocol/#kip-support).
<!-- /generated -->

Not implemented, or implemented in part:

<!-- generated:kips:not-implemented -->
- **SASL/GSSAPI (Kerberos) authentication** (GSSAPI) — No mature pure-Rust GSSAPI implementation exists, and linking system Kerberos libraries would add a C dependency for every user.
- **Schema registry client** (schema-registry) — A schema registry is a separate service with its own protocol; krafka provides the serdes::Serializer and Deserializer hooks instead.
- **Async runtimes other than Tokio** (runtime-agnostic) — krafka is built on Tokio and does not abstract over the async runtime.
- **Kafka Streams runtime** (streams-runtime) — krafka is a client library with no stream-processing runtime, which is also why StreamsGroupHeartbeat is not implemented.
- **Broker-, controller- and KRaft-internal APIs** (broker-internal-apis) — APIs such as LeaderAndIsr, UpdateMetadata, Vote and the share-group state persister are spoken between brokers, not by clients.
- **KIP-368 Allow SASL connections to periodically re-authenticate**, partly — The broker-reported session lifetime is honoured by replacing a pooled connection before it expires; in-band re-authentication of a live connection is not implemented.
- **KIP-525 Return topic metadata and configs in CreateTopics response**, partly — CreateTopics v5+ is negotiated, but create_topics returns only per-topic success, so the partition count, replication factor and configs in the response are not surfaced.
- **KIP-794 Strictly uniform sticky partitioner**, partly — Keyless records stick to a partition for batch_size bytes and then switch at random, but the next partition is not weighted by per-broker queue size (partitioner.adaptive.partitioning.enable) and slow brokers are not avoided (partitioner.availability.timeout.ms).
- **KIP-800 Add reason to JoinGroupRequest and LeaveGroupRequest**, partly — JoinGroup v8 and LeaveGroup v5 are negotiated, but the reason field is always sent as null.
- **KIP-853 KRaft controller membership changes**, partly — Quorum membership can be described, but the AddRaftVoter and RemoveRaftVoter admin RPCs are not implemented.
- **KIP-932 Queues for Kafka**, partly — The share consumer and share-group offset administration are implemented, but ShareGroupDescribe is never sent, so no admin call describes or lists share groups' members.
- **KIP-1071 Streams rebalance protocol**, partly — Streams groups can be described, but StreamsGroupHeartbeat is not implemented because its request carries an application topology that only a Streams runtime can supply.
- **KIP-1242 Detection and handling of misrouted connections**, partly — ApiVersions v5 is encoded behind unstable-protocol, but the ClusterId and NodeId fields are never populated and REBOOTSTRAP_REQUIRED from ApiVersions does not trigger a rebootstrap.
- `AlterConfigs` is implemented below Kafka's ceiling — superseded by IncrementalAlterConfigs, which krafka uses instead; the legacy whole-config replace is not exposed
- `SaslHandshake` is implemented below Kafka's ceiling — pinned at v1 by the handshake path; v0 has no mechanism list
- `SaslAuthenticate` is implemented below Kafka's ceiling — pinned at v1: v2 only adds flexible encoding, and the pre-auth reader is deliberately version-pinned so an unauthenticated peer cannot steer it
- Not spoken by a client (broker-, controller- and KRaft-internal): `LeaderAndIsr`, `StopReplica`, `UpdateMetadata`, `ControlledShutdown`, `Vote`, `BeginQuorumEpoch`, `EndQuorumEpoch`, `AlterPartition`, `Envelope`, `FetchSnapshot`, `BrokerRegistration`, `BrokerHeartbeat`, `UnregisterBroker`, `AllocateProducerIds`, `ControllerRegistration`, `AssignReplicasToDirs`, `UpdateRaftVoter`, `InitializeShareGroupState`, `ReadShareGroupState`, `WriteShareGroupState`, `DeleteShareGroupState`, `ReadShareGroupStateSummary`
<!-- /generated -->

Schema registries are a separate service; krafka provides the
`serdes::Serializer` and `Deserializer` hooks. See the
[Cookbook](https://hupe1980.github.io/krafka/docs/cookbook/).

## 🎮 Examples

Each example has a header comment saying what it shows and how to run it.
Every one reads `KAFKA_BOOTSTRAP_SERVERS` (default `localhost:9092`) except
`fake_broker`, which needs no broker.

| Example | What it shows | Run |
|---|---|---|
| [`producer`](examples/producer.rs) | `send`, then `enqueue` with the delivery handles awaited later | `cargo run --example producer` |
| [`consumer`](examples/consumer.rs) | a group member that commits after each batch, then reports lag | `cargo run --example consumer` |
| [`share_consumer`](examples/share_consumer.rs) | KIP-932: ack or reject each record, check the commit results (Kafka 4.2+) | `cargo run --example share_consumer` |
| [`exactly_once`](examples/exactly_once.rs) | consume-transform-produce with `send_offsets`; abort and seek back on failure | `cargo run --example exactly_once` |
| [`admin`](examples/admin.rs) | describe the cluster; create, describe and delete a topic | `cargo run --example admin` |
| [`metrics`](examples/metrics.rs) | read `Metrics` fields and print Prometheus text | `cargo run --example metrics` |
| [`tracing`](examples/tracing.rs) | print the clients' spans with `tracing-subscriber` | `cargo run --example tracing` |
| [`authentication`](examples/authentication.rs) | TLS and SASL from environment variables (`AuthConfig::from_env`) | `cargo run --example authentication` |
| [`fake_broker`](examples/fake_broker.rs) | test your own code against `FakeBroker` under injected faults | `cargo run --example fake_broker --features test-broker` |
| [`oauth_oidc`](examples/oauth_oidc.rs) | the OIDC token provider with a client secret or an assertion file | `cargo run --example oauth_oidc --features oauth-oidc` |
| [`msk_iam`](examples/msk_iam.rs) | MSK IAM with the AWS SDK default credential chain | `cargo run --example msk_iam --features aws-msk` |

## 📚 Documentation

Guides: **[hupe1980.github.io/krafka](https://hupe1980.github.io/krafka)** ·
API reference: **[docs.rs/krafka](https://docs.rs/krafka)** ·
Release history: **[CHANGELOG.md](CHANGELOG.md)**

| Start here | Clients | Integration | Operations | Reference |
|---|---|---|---|---|
| [Getting Started](https://hupe1980.github.io/krafka/docs/getting-started/) | [Producer](https://hupe1980.github.io/krafka/docs/producer/) | [Authentication](https://hupe1980.github.io/krafka/docs/authentication/) | [Metrics](https://hupe1980.github.io/krafka/docs/metrics/) | [Protocol Support](https://hupe1980.github.io/krafka/docs/protocol/) |
| [Cookbook](https://hupe1980.github.io/krafka/docs/cookbook/) | [Consumer](https://hupe1980.github.io/krafka/docs/consumer/) | [Cloud Platforms](https://hupe1980.github.io/krafka/docs/cloud/) | [Performance](https://hupe1980.github.io/krafka/docs/performance/) | [Architecture](https://hupe1980.github.io/krafka/docs/architecture/) |
| [Configuration](https://hupe1980.github.io/krafka/docs/configuration/) | [Share Consumer](https://hupe1980.github.io/krafka/docs/share-consumer/) | [Interceptors](https://hupe1980.github.io/krafka/docs/interceptors/) | [Testing](https://hupe1980.github.io/krafka/docs/testing/) | [Project and Support](https://hupe1980.github.io/krafka/docs/governance/) |
| [Upgrading to 0.27](https://hupe1980.github.io/krafka/docs/upgrading/) | [Admin Client](https://hupe1980.github.io/krafka/docs/admin/) | | [Error Handling](https://hupe1980.github.io/krafka/docs/errors/) | |
| [Migrating from rdkafka](https://hupe1980.github.io/krafka/docs/migrating-from-rdkafka/) | | | | |

krafka is pre-1.0: a **minor** release may carry breaking changes, and each
one is listed under `Breaking` in [CHANGELOG.md](CHANGELOG.md).
[Upgrading to 0.27](https://hupe1980.github.io/krafka/docs/upgrading/) maps
every removed name to its replacement.

## 🛠️ Development

Tasks run through [`just`](https://just.systems); CI calls the same recipes.

```bash
just              # list every recipe
just ci           # everything CI runs, except the Docker-backed suites
just ci-full      # ci + supply-chain audit + Docker integration suites
just pre-commit   # fmt, clippy, check
just t <pattern>  # one test by name, with output
```

`just ci` includes the API-surface and boundary checks
(`tests/builder_surface.rs`, `private_interfaces` denied), `protocol-parity`,
`claims-check` (the capability lists above are generated from a registry),
`no-c`, `secret-debug`, `cancel-safety`, `docs-test` (every `rust,compile`
block in this README and the guides is built), the deterministic simulation
(`sim`) and the test suites under both TLS backends. How the project is
maintained, supported and checked:
[Project and Support](https://hupe1980.github.io/krafka/docs/governance/).
Security reports: [SECURITY.md](SECURITY.md).

## 📄 License

Licensed under either the [MIT License](LICENSE-MIT) or the
[Apache License 2.0](LICENSE-APACHE), at your option.
