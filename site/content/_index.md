+++
title = "krafka — a pure-Rust Apache Kafka client"
description = "krafka is a pure-Rust, async-native Apache Kafka client. No C library, no system dependency, no unsafe, no panics on malformed input."
template = "index.html"

[extra]
tagline = "An async Apache Kafka client in pure Rust."
lede = """
Pure Rust on Tokio. No C library and no system dependency in the default \
build, no `unsafe`, no panics on a malformed response. Protocol versions \
tracked against Apache Kafka 4.3 and checked in CI against Kafka's own schemas.
"""

[[extra.pillars]]
title = "Nothing to link"
body = """
`cargo add krafka` needs a Rust toolchain and the C compiler `ring` uses — \
no librdkafka, no OpenSSL, no CMake, no pkg-config, no system library. \
`ring` is the only crate in the default build that compiles C; `zstd` \
encoding, the aws-lc-rs backend and MSK IAM are opt-in features that add more.
"""

[[extra.pillars]]
title = "No unsafe, no panics"
body = """
Unsafe code is denied crate-wide, as are panic, unwrap and expect. A \
malformed broker response surfaces as an error, never a panic; every \
allocation from untrusted input is bounded twice, by the declared count and \
by the bytes actually available.
"""

[[extra.pillars]]
title = "Current protocol"
body = """
Apache Kafka 4.3 API versions, including KIP-848 consumer groups and KIP-932 \
share groups. A CI job diffs krafka's version table against Kafka's own \
message schemas.
"""

[[extra.pillars]]
title = "Acknowledged means written"
body = """
An idempotent producer by default that never reuses a sequence range, a \
timeout that says whether the record may have been written, a transaction \
that cannot commit after a failed send, and KIP-320 truncation detection on \
Fetch, ListOffsets and OffsetCommit.
"""

[[extra.highlights]]
label = "Protocol"
value = "Kafka 4.3"
note = "CI-diffed against Kafka's schemas"

[[extra.highlights]]
label = "Unsafe blocks"
value = "0"
note = "denied crate-wide"

[[extra.highlights]]
label = "C libraries"
value = "0"
note = "only ring compiles C by default"

[[extra.highlights]]
label = "Fake broker"
value = "built in"
note = "test your code without Docker"
+++

## Install

```sh
cargo add krafka
cargo add tokio --features full
```

## Produce

```rust,compile
use krafka::{Kafka, Record};

#[tokio::main]
async fn main() -> krafka::Result<()> {
    let kafka = Kafka::builder("localhost:9092").connect().await?;
    let producer = kafka.producer().build().await?;

    producer.send(Record::new("orders", "hello").key("key")).await?;
    producer.close().await
}
```

## Consume

```rust,compile
use krafka::Kafka;
use krafka::consumer::AutoOffsetReset;

#[tokio::main]
async fn main() -> krafka::Result<()> {
    let kafka = Kafka::builder("localhost:9092").connect().await?;
    let consumer = kafka
        .consumer("order-processor")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await?;

    consumer.subscribe(["orders"]).await?;
    while let Some(record) = consumer.recv().await? {
        println!("{}-{} @ {}", record.topic, record.partition, record.offset);
    }
    Ok(())
}
```

## What you get

**Clients.** A [producer](@/docs/producer.md) with batching, compression,
idempotence and exactly-once transactions. A [consumer](@/docs/consumer.md)
supporting both group protocols — classic and KIP-848 server-side assignment. A
[share consumer](@/docs/share-consumer.md) for queue-like per-record
acknowledgement (KIP-932). An [admin client](@/docs/admin.md): topics, partitions,
configs, ACLs, quotas, groups, delegation tokens, SCRAM credentials and cluster
features.

**Security.** TLS and mTLS over rustls with hot certificate reload (KIP-1288).
SASL PLAIN, SCRAM-SHA-256/512 and OAUTHBEARER, including a built-in OIDC token
provider covering both the client-secret flow (KIP-768) and RFC 7523 client
assertions (KIP-1258), with no JWT or RSA dependency. Secrets are zeroized,
credential comparison is constant-time, and no credential-bearing type derives
`Debug`. See
[Authentication](@/docs/authentication.md) and
[Cloud Platforms](@/docs/cloud.md).

**Operability.** `metrics()` on every client returns one
[metrics snapshot](@/docs/metrics.md) with Prometheus text output; spans go
through `tracing` on OpenTelemetry messaging conventions; KIP-714 pushes
client metrics to brokers that subscribe. [Interceptors](@/docs/interceptors.md)
for redaction and auditing. Connection settings — keepalive, the
response ceiling, the in-flight cap, the connection cap — are set once on the
`Kafka` handle and shared by every client built from it.

**Testing.** An in-process fake broker behind the `test-broker` feature
speaks the wire protocol with fault injection (leader moves, coordinator
failover, late responses, controller churn), needs no Docker and runs on
Tokio's paused clock. See [Testing](@/docs/testing.md). Coming from rdkafka? Read
[Migrating from rdkafka](@/docs/migrating-from-rdkafka.md).

## Limits

- **No GSSAPI/Kerberos.** The full list of what is not implemented is on the
  [Protocol](@/docs/protocol.md) page.
- **Tokio only.**
- **No published throughput number.** The benchmarks are micro-benchmarks and
  a send-path regression gate; see [Performance](@/docs/performance.md).
- **Assertion signing is your job.** krafka sources a signed JWT from a file or
  a callback rather than choosing an RSA implementation for you.
