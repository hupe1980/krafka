+++
title = "krafka — a pure-Rust Apache Kafka client"
description = "krafka is a pure-Rust, async-native Apache Kafka client. No librdkafka, no C toolchain, no unsafe, no panics. Protocol parity with Apache Kafka 4.3, enforced in CI."
template = "index.html"

[extra]
tagline = "The Apache Kafka client that Rust should have had."
lede = """
Pure Rust on Tokio. No librdkafka, no C toolchain, no `unsafe`, no panics — \
enforced by the compiler, not by convention. Protocol parity with Apache \
Kafka 4.3, checked in CI against Kafka's own schemas.
"""

[[extra.pillars]]
title = "Nothing to link, nothing to build"
body = """
`cargo add krafka` and you are done. No librdkafka, no cmake, no C compiler, \
no cross-compilation surprises, no ~5 MiB of resident C library. Optional \
zstd is the single exception, and it is opt-in.
"""

[[extra.pillars]]
title = "The safety posture is enforced, not claimed"
body = """
Unsafe code is denied crate-wide, as are panic, unwrap and expect, across \
~133 000 lines. A malformed broker response cannot panic the process; every \
allocation from untrusted input is bounded twice, by the declared count and \
by the bytes actually available.
"""

[[extra.pillars]]
title = "Current, and provably so"
body = """
Apache Kafka 4.3 API versions, including KIP-848 consumer groups and KIP-932 \
share groups. A CI job diffs krafka's version table against Kafka's own \
message schemas, so falling behind is a failed build rather than a bug report.
"""

[[extra.pillars]]
title = "Correctness where it is hardest"
body = """
KIP-320 truncation detection on all three legs — Fetch, ListOffsets and \
persisted through OffsetCommit. A transaction state machine that refuses \
KAFKA-17754 by construction. Out-of-order sequence numbers verified \
head-of-line before any rewind, so a silent gap is a fatal error rather than \
a reported success.
"""

[[extra.highlights]]
label = "Protocol"
value = "Kafka 4.3"
note = "64 APIs, CI-diffed against Kafka's schemas"

[[extra.highlights]]
label = "Unsafe blocks"
value = "0"
note = "denied crate-wide"

[[extra.highlights]]
label = "C dependencies"
value = "0"
note = "zstd optional, opt-in"

[[extra.highlights]]
label = "Tests"
value = "2400+"
note = "incl. an in-process fake broker"
+++

## Why another Kafka client?

Because the Rust ecosystem's practical choice has been a binding.
`rust-rdkafka` is a mature, well-maintained wrapper over librdkafka — and it
inherits C's memory model, C's build requirements and C's failure modes
wholesale. A cross-compilation toolchain, a multi-megabyte resident footprint
in a container that exists to move bytes, and a safety story that stops at the
FFI boundary.

krafka is the other trade: a native implementation, so the guarantees Rust can
make actually reach the wire.

## Install

```sh
cargo add krafka
cargo add tokio --features full
```

## Produce

```rust,compile
use krafka::producer::Producer;

#[tokio::main]
async fn main() -> krafka::error::Result<()> {
    let producer = Producer::builder()
        .bootstrap_servers("localhost:9092")
        .build()
        .await?;

    producer.send("orders", Some(b"key"), Some(b"hello")).await?;
    producer.close().await;
    Ok(())
}
```

## Consume

```rust,compile
use krafka::consumer::{AutoOffsetReset, Consumer};
use std::time::Duration;

#[tokio::main]
async fn main() -> krafka::error::Result<()> {
    let consumer = Consumer::builder()
        .bootstrap_servers("localhost:9092")
        .group_id("order-processor")
        .auto_offset_reset(AutoOffsetReset::Earliest)
        .build()
        .await?;

    consumer.subscribe(&["orders"]).await?;

    loop {
        for record in consumer.poll(Duration::from_secs(1)).await? {
            println!("{}-{} @ {}", record.topic, record.partition, record.offset);
        }
    }
}
```

## What you get

**Clients.** A [producer](@/docs/producer.md) with batching, compression,
idempotence and exactly-once transactions. A [consumer](@/docs/consumer.md)
supporting both group protocols — classic and KIP-848 server-side assignment. A
[share consumer](@/docs/share-consumer.md) for queue-like per-record
acknowledgement (KIP-932), at Kafka 4.2 parity with the offset administration
to operate it. A full [admin client](@/docs/admin.md): topics, partitions,
configs, ACLs, quotas, groups, delegation tokens, SCRAM credentials and cluster
features.

**Security.** TLS and mTLS over rustls with hot certificate reload (KIP-1288).
SASL PLAIN, SCRAM-SHA-256/512 and OAUTHBEARER, including a built-in OIDC token
provider covering both the client-secret flow (KIP-768) and RFC 7523 client
assertions (KIP-1258) — without pulling in a JWT or RSA crate. Secrets are
zeroized, credential comparison is constant-time, and no credential-bearing
type may derive `Debug`. That last one is a CI job, because it had already
happened twice. See [Authentication](@/docs/authentication.md).

**Operability.** Built-in [counters, gauges and histograms](@/docs/metrics.md)
with Prometheus export. [Interceptors](@/docs/interceptors.md) for tracing,
redaction and auditing. OpenTelemetry semantic conventions. A `TransportConfig`
on every builder covering keepalive, the response ceiling, the in-flight cap
and the file-descriptor cap.

**Testing.** An in-process fake broker behind the `test-broker` feature,
speaking the real wire protocol with fault injection — leader moves,
coordinator failover, late responses, controller churn — so client tests need
no Docker. It can also advertise *older* API versions on demand, which is how
you reach the branches that degrade gracefully against an old cluster.

## Honest limits

A page that lists only strengths is a page you cannot calibrate against, so:

- **No GSSAPI/Kerberos.** A deliberate boundary, and the one SASL mechanism
  librdkafka has that krafka does not.
- **Tokio only.** `rskafka` is runtime-agnostic; krafka is not.
- **No published end-to-end throughput benchmark.** There are criterion
  micro-benchmarks for the protocol layer, but no measured number against
  `rust-rdkafka` on a real cluster. Until there is, treat performance claims
  here as architectural reasoning rather than evidence — which is why the word
  "fastest" appears nowhere on this page. See
  [Performance](@/docs/performance.md) for what is and is not measured.
- **Assertion signing is your job.** krafka sources a signed JWT from a file or
  a callback rather than choosing an RSA implementation for you.
