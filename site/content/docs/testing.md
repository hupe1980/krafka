+++
title = "Testing"
description = "Test your Kafka code against an in-process fake broker with injected faults, or against a real broker in a container."
weight = 115

[extra]
slug_id = "testing"
+++

`krafka::testing::FakeBroker` is an in-process broker speaking the Kafka wire
protocol, over a TCP socket or over in-memory streams. Your code gets a real
`Producer`, `Consumer`, `ShareConsumer` or `AdminClient`, and the test decides
what the broker does: move a leader mid-produce, fail over a coordinator, lose
the answer to a write that landed, corrupt a batch's CRC.

Enable it for tests only:

```sh
cargo add --dev krafka --features test-broker
```

`krafka::testing` is **unstable and outside semver**: the fake broker follows
the client's internals, and any release may change it.

## A first test

```rust,compile
use krafka::testing::FakeBroker;
use krafka::producer::Producer;

#[tokio::test]
async fn records_reach_the_broker() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);

    let producer = krafka::Kafka::builder(broker.bootstrap_servers())
        .connect()
        .await
        .unwrap()
        .producer()
        .build()
        .await
        .unwrap();

    producer.send(krafka::Record::new("orders", "payload")).await.unwrap();

    assert_eq!(broker.next_offset("orders", 0), Some(1));
}
```

`start()` binds a single broker to an ephemeral loopback port. `start_cluster(n)`
binds `n`, each on its own port, which is what makes leader moves and
coordinator failover expressible.

## Testing your code under a fault

Hand your function the client built from the fake broker, install a fault,
and assert on what your function returned and what reached the log. Here the
broker appends the first batch and then drops the connection before
answering.

```rust,compile
use krafka::producer::Producer;
use krafka::testing::{ApiKey, Control, FakeBroker};
use krafka::{Kafka, Record};

// Application code: publish an order, keyed by its id.
async fn publish_order(producer: &Producer, order_id: &str) -> krafka::Result<i64> {
    let metadata = producer
        .send(Record::new("orders", format!("order {order_id} created")).key(order_id.to_owned()))
        .await?;
    Ok(metadata.offset)
}

// The test: the body of a `#[tokio::test] async fn … -> krafka::Result<()>`.
let broker = FakeBroker::start().await?;
broker.create_topic("orders", 1);
let producer = Kafka::builder(broker.bootstrap_servers())
    .connect()
    .await?
    .producer()
    .build()
    .await?;

// Apply the Produce, then lose its answer.
broker.on_once(ApiKey::Produce, |_| Control::ApplyThen(Box::new(Control::Disconnect)));

let offset = publish_order(&producer, "1001").await?;

assert_eq!(offset, 0);
assert_eq!(broker.request_count(ApiKey::Produce), 2, "the producer retried");
assert_eq!(broker.next_offset("orders", 0), Some(1), "the order is in the log once");
```

The idempotent producer retries with the same producer id and sequence, and
the broker acknowledges the retry without writing it again, as Kafka does.
`examples/fake_broker.rs` runs this and a retried `NOT_LEADER_OR_FOLLOWER`
to completion, with no broker to install:

```sh
cargo run --example fake_broker --features test-broker
```

The same shape tests a consumer loop (fail an `OffsetCommit`, move the group
coordinator), a share-group worker (inspect `share_acknowledgements` for what
it accepted, released and rejected) or an admin workflow.

## Timers in milliseconds: the in-memory cluster

`FakeBroker::start_in_memory(n)` starts the same cluster without sockets:
clients built from `broker.kafka()` reach it over in-memory streams. With no
real I/O left, the test can run on Tokio's paused clock, where a timer fires
the moment every task is idle — a five-second linger takes microseconds, and
fires at exactly five seconds:

```rust,compile
use std::time::Duration;
use krafka::testing::{ApiKey, FakeBroker};

#[tokio::test(start_paused = true)]
async fn a_batch_waits_for_its_linger() {
    let broker = FakeBroker::start_in_memory(1);
    broker.create_topic("orders", 1);
    let producer = broker
        .kafka()
        .connect()
        .await
        .unwrap()
        .producer()
        .linger(Duration::from_secs(5))
        .build()
        .await
        .unwrap();
    let start = tokio::time::Instant::now();
    let handle = producer
        .enqueue(krafka::Record::new("orders", "payload").partition(0))
        .await
        .unwrap();

    tokio::time::sleep_until(start + Duration::from_millis(4999)).await;
    assert_eq!(broker.request_count(ApiKey::Produce), 0);
    tokio::time::sleep_until(start + Duration::from_millis(5001)).await;
    assert_eq!(broker.request_count(ApiKey::Produce), 1);
    handle.await.unwrap();
}
```

`#[tokio::test(start_paused = true)]` needs Tokio's `test-util` feature in your
dev-dependencies. `bootstrap_servers()` of an in-memory cluster names
`broker-<id>:9092`, which only `broker.kafka()` can reach; TLS and SASL are not
available over it. Every request the broker records carries `at`, its arrival
time on Tokio's clock since the cluster started.

Under the paused clock a task that loops without awaiting a timer stops time:
nothing else is idle, so nothing fires. A test that hangs there has found a
busy loop.

`krafka::testing::seed_rng(seed)` seeds the client's non-cryptographic random
draws on the calling thread — backoff jitter, keyless partition choice, broker
order and generated member ids — so a test on a current-thread runtime with
the clock paused sends the same requests at the same simulated times on every
run.

## Injecting faults

`on_once`, `on_times(api, n, hook)` and `on` (until `clear_hooks`) install a
hook per API key. The hook receives a `RequestInfo` — the API, version, node
and `api_call_index`, how many requests for that API came before — and returns
a `Control` describing what the broker does instead of answering normally.

```rust,compile
use krafka::admin::{CreateTopicsOptions, NewTopic};
use krafka::error::ErrorCode;
use krafka::testing::{ApiKey, Control, FakeBroker};

let broker = FakeBroker::start().await?;
let admin = krafka::Kafka::builder(broker.bootstrap_servers()).connect().await?.admin();

// Exactly one CreateTopics lands on a non-controller; the rest are served.
broker.on_once(ApiKey::CreateTopics, |_| Control::Error(ErrorCode::NotController));

let results = admin
    .create_topics([NewTopic::new("orders", 3, 1)?], CreateTopicsOptions::default())
    .await?;

assert!(results["orders"].is_ok(), "the client should have retried");
assert_eq!(broker.request_count(ApiKey::CreateTopics), 2);
```

| `Control` | What the broker does |
|---|---|
| `Pass` | Falls through to the default handler |
| `Error(code)` | Answers with a **structurally valid** response carrying `code` in whatever field that API actually has — top level, per topic or per partition — so the client runs its normal error handling rather than its malformed-frame path |
| `Delay(d)` | Waits, then answers normally. With `d` above the client's request timeout, the response arrives after the client gave up, on a connection still open |
| `DelayThen(d, ctrl)` | Waits, then applies the nested control |
| `Disconnect` | Drops the connection without answering |
| `Silence` | Never answers but holds the connection open. Because Kafka responses are ordered per connection, this also blocks every later request on that connection |
| `CorruptRecords` | Answers a `Fetch` normally but flips a byte inside the CRC-covered region, so the batch fails its checksum while the surrounding response still parses. `Fetch` only; on any other API it is an error |
| `ApplyThen(ctrl)` | Serves the request — its effects happen — then answers as the nested control says. `ApplyThen(Disconnect)` is the "outcome unknown" fault: the produce is written, the transaction committed or the share records acknowledged, and the client never hears so |

`Error` works on every API the broker serves, the transaction and share APIs
included. For `ShareFetch` and `ShareAcknowledge`, partition-scoped codes
(`NOT_LEADER_OR_FOLLOWER`, `INVALID_RECORD_STATE`, …) go on each partition and
the rest at the top level.

`set_throttle(api, duration)` reports a `throttle_time_ms` on every response to
that API (KIP-219); the response is still sent at once.
`require_sasl_plain(username, password)` makes every broker demand SASL/PLAIN
with those credentials.

## Moving the cluster around

The cluster is mutable mid-test:

```rust,compile
use krafka::testing::FakeBroker;

let broker = FakeBroker::start_cluster(3).await?;
broker.create_topic("orders", 1);

// Partition 0's leader moves from broker 0 to broker 1.
broker.set_leader("orders", 0, 1);
broker.bump_leader_epoch("orders", 0);

// The group coordinator fails over.
broker.set_group_coordinator("my-group", 2);

// The controller is lost entirely.
broker.set_controller(-1);

// A broker is up but out of the cluster's view.
broker.set_broker_online(1, false);

// The topic grows to 4 partitions, as a `CreatePartitions` admin call would.
// Returns how many partitions were added; Kafka never removes them.
broker.add_partitions("orders", 4);

// The topic is deleted and re-created: a new topic ID, leader epochs from 0.
broker.delete_topic("orders");
broker.create_topic("orders", 1);

// A broker advertises a rack in Metadata.
broker.set_broker_rack(1, Some("eu-west-1b"));

// A broker crashes: its connections drop and new ones are refused; its logs
// survive. Then it comes back.
broker.crash(1);
broker.restart(1);
```

`set_txn_coordinator` does the same for a transactional ID, and `with_state`
gives direct access for anything not covered by a named setter.

## Transactions and exactly-once

The broker implements the full transactional path — `InitProducerId` with
KIP-360 fencing, `AddPartitionsToTxn`, `AddOffsetsToTxn`, `TxnOffsetCommit`,
`EndTxn`, real commit and abort control batches, and `read_committed`
isolation. A complete consume-transform-produce cycle runs in-process:

```rust,compile
use krafka::testing::FakeBroker;

let broker = FakeBroker::start().await?;
broker.create_topic("orders", 1);

let producer = krafka::Kafka::builder(broker.bootstrap_servers())
    .connect()
    .await?
    .producer()
    .build_transactional("checkout-0")
    .await?;

producer.begin()?;
producer.send(krafka::Record::new("orders", "payload")).await?;
producer.flush().await?;

// Until the commit lands, a read_committed consumer may not read past the
// transaction's first record.
assert_eq!(broker.last_stable_offset("orders", 0), Some(0));

producer.commit().await?;
```

### Choosing the transaction protocol

`set_transaction_version` finalizes the cluster's `transaction.version`
feature. The client reads it out of `ApiVersions` and negotiates from it, so
the protocol is chosen the same way a real cluster chooses it:

| Level | Protocol | What the client does |
|---|---|---|
| `0` or `1` *(default)* | TV1 | Registers partitions with `AddPartitionsToTxn` and the offsets topic with `AddOffsetsToTxn` before writing |
| `2` | TV2 (KIP-890) | Sends neither — `Produce` and `TxnOffsetCommit` carry the transactional ID, and `EndTxn` returns a bumped epoch |

```rust,compile
use krafka::testing::{ApiKey, FakeBroker};

let broker = FakeBroker::start().await?;
broker.create_topic("orders", 1);
broker.set_transaction_version(2);

let producer = krafka::Kafka::builder(broker.bootstrap_servers())
    .connect()
    .await?
    .producer()
    .build_transactional("checkout-0")
    .await?;
producer.begin()?;
producer.send(krafka::Record::new("orders", "payload")).await?;
producer.commit().await?;

assert_eq!(
    broker.request_count(ApiKey::AddPartitionsToTxn), 0,
    "under TV2 the client registers no partitions",
);
```

### Inspecting transaction state

```rust
broker.transactional_producer("checkout-0");   // (producer_id, epoch)
broker.transaction_is_open("checkout-0");      // is a transaction in flight?
broker.transaction_status("checkout-0");       // the coordinator's TxnStatus
broker.last_stable_offset("orders", 0);        // the read_committed ceiling
broker.aborted_transactions("orders", 0);      // (producer_id, first_offset) pairs
broker.committed_offset("etl-group", "orders", 0);
broker.committed_records("orders")?;           // what a read_committed reader sees
broker.all_records("orders")?;                 // every data record in the log
```

`transactional_producer` is how you assert fencing: re-initialising the same
transactional ID must return the **same** producer ID with a **higher** epoch,
and under TV2 every completed transaction bumps it again.

`transaction_status` returns the coordinator's state — `Empty`, `Ongoing`,
`PrepareCommit`, `PrepareAbort`, `CompleteCommit` or `CompleteAbort`. Two
setters reach the states a real coordinator passes through:

```rust
// EndTxn leaves the transaction prepared; the producer's next InitProducerId,
// AddPartitionsToTxn and TV2 Produce see CONCURRENT_TRANSACTIONS until
// the markers are written.
broker.hold_transaction_markers(true);
// ...
broker.hold_transaction_markers(false); // write them now

// The coordinator times the transaction out: epoch bumped, abort markers.
broker.abort_transaction("checkout-0");
```

### Idempotence

Each partition leader keeps producer state as Kafka does: the epoch and the
last five batches per producer ID. A retried batch that matches one of them is
acknowledged at the offset it was first written at and not written again; a
sequence gap is `OUT_OF_ORDER_SEQUENCE_NUMBER`; an epoch below the stored one
is `INVALID_PRODUCER_EPOCH`, and a higher one must restart at sequence 0. A
transactional write must carry the coordinator's producer ID and epoch, and
its partition must be in the transaction — added by `AddPartitionsToTxn` below
`Produce` v12, by the write itself at v12.

`set_idempotence(false)` turns all of that off, which is the negative control
for a test that relies on de-duplication. `clear_producer_state(topic,
partition)` forgets a partition's producer state, as retention does; a broker
whose `InitProducerId` is overridden below v3 then answers a continuing
sequence with `UNKNOWN_PRODUCER_ID`, as brokers before KIP-360 do.

## Being an older broker

`set_api_versions` overrides the range the broker advertises for one API, to
test how the client behaves against a broker too old for a feature:

```rust,compile
use krafka::admin::{FeatureUpdateKey, UpdateFeaturesOptions};
use krafka::testing::{ApiKey, FakeBroker};

let broker = FakeBroker::start().await?;
let admin = krafka::Kafka::builder(broker.bootstrap_servers()).connect().await?.admin();

// A broker predating KIP-584's `ValidateOnly` field.
broker.set_api_versions(ApiKey::UpdateFeatures, 0, 0);

let outcome = admin
    .update_features(
        vec![FeatureUpdateKey::upgrade("transaction.version", 2)],
        UpdateFeaturesOptions::default().validate_only(true),
    )
    .await;

assert!(outcome.is_err());
assert_eq!(
    broker.request_count(ApiKey::UpdateFeatures), 0,
    "the dry run is refused before anything is sent",
);
```

An override for an API the broker has no handler for is still advertised, so a
test can observe where the client sends a request this broker cannot serve.

## Asserting on what the client did


```rust
broker.request_count(ApiKey::Metadata);          // how many times
broker.request_nodes(ApiKey::UpdateFeatures);    // which brokers, in order
broker.requests();                               // every RecordedRequest: API, version, node, connection, `at`
broker.clear_requests();                         // reset between phases
broker.open_connections();                       // connections open right now
broker.share_session_closes();                   // final-epoch share session closes, and via which API
broker.share_acknowledgements("group", "orders", 0); // every applied share ack (ShareAckType), per offset, in order
broker.list_offsets_lookups();                   // every ListOffsets lookup, with its isolation level
broker.leave_group_members();                    // members that left a classic group, and how
broker.consumer_group_heartbeats();              // every KIP-848 heartbeat

// Wait for the client to act, rather than sleeping and hoping.
broker.wait_for_requests(ApiKey::Fetch, 3, Duration::from_secs(5)).await;
broker.wait_for_request_on_node(ApiKey::Produce, 1, Duration::from_secs(5)).await;
```

Prefer `wait_for_requests` over a `sleep`, which is timing-dependent on a
loaded CI runner.

### Client telemetry

The broker answers KIP-714 `GetTelemetrySubscriptions` and `PushTelemetry`
once `set_telemetry` installs a subscription, and records every push:

```rust,compile
use std::time::Duration;
use krafka::testing::{FakeBroker, TelemetrySubscription};

let broker = FakeBroker::start().await?;
broker.set_telemetry(Some(TelemetrySubscription::new(
    ["org.apache.kafka.producer."],
    Duration::from_secs(1),
)));

let producer = krafka::Kafka::builder(broker.bootstrap_servers())
    .connect()
    .await?
    .producer()
    .build()
    .await?;
producer.close().await?;

// The last push before a client closes is marked terminating.
assert!(broker.telemetry_pushes().iter().any(|push| push.terminating));
```

## What the broker implements

The produce and fetch path, `ListOffsets` v5–v11 (latest is the last stable
offset for `read_committed`; a timestamp lookup answers the first record at or
after it), `Metadata` v12 (topic UUIDs, and `allow.auto.create.topics` — a
request naming a topic the cluster does not have creates it),
`FindCoordinator`, `CreateTopics`/`DeleteTopics`, `UpdateFeatures` with
controller routing, KIP-714 telemetry, the full transaction protocol, and
**both** consumer group protocols plus share groups:

- **Fetch long-poll** — a `Fetch` with nothing to return is held for its
  `max_wait_ms` and answered as soon as an append brings `min_bytes`; a
  `ShareFetch` with nothing to acquire waits the same way.
- **Transactions** — `InitProducerId` v0–v5 (v6 when advertised) with KIP-360
  fencing: a stable producer ID per transactional ID, the next epoch on every
  re-initialisation, a named `(producer_id, epoch)` bumped once and its retry
  answered with the same epoch, anything else `PRODUCER_FENCED`. An open
  transaction is aborted — markers at a bumped epoch — before a new
  incarnation gets its epoch. `AddPartitionsToTxn` and `AddOffsetsToTxn` under
  TV1, `TxnOffsetCommit` with KIP-447 generation-and-member fencing, and
  `EndTxn` writing real commit and abort control batches. The coordinator reads
  the protocol from the `EndTxn` version, as Kafka does: v5 bumps the epoch,
  v4 and below do not; a retried `EndTxn` for a completed transaction succeeds.
  Offsets staged by a transaction are applied only on commit. `read_committed`
  fetches stop at the last stable offset and report aborted transactions, so
  the consumer's own filtering runs for real.
- **Classic** — `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`,
  `OffsetCommit`, `OffsetFetch`, and `DescribeGroups` v4. The describe returns
  the subscription and assignment blobs exactly as the members and the group
  leader wrote them, so a client decoding them is decoding bytes a real broker
  would have handed back unchanged.
- **KIP-848** — `ConsumerGroupHeartbeat` v1 with real revoke-before-assign
  reconciliation: a partition moves to its new owner strictly after the
  previous owner confirms releasing it, so no two members ever believe they own
  it at once. The server assignors are `uniform` and `range`; any other is
  `UNSUPPORTED_ASSIGNOR`. A member that stops heartbeating is removed after the
  45 s session timeout. `OffsetCommit` v9 validates a member's commit as
  Kafka's consumer group does: an unknown member is `UNKNOWN_MEMBER_ID`, an
  epoch behind the member's is `STALE_MEMBER_EPOCH`, one ahead of it
  `FENCED_MEMBER_EPOCH`, and a commit below v9 `UNSUPPORTED_VERSION`.
- **KIP-932 share groups** — `ShareGroupHeartbeat` v1 (a joining member must
  send its subscription), `ShareFetch` and `ShareAcknowledge` v1–v2 with share
  sessions validated per broker, backed by the share-partition state machine
  that replaces committed offsets: a start offset, the member holding each
  acquired record and a per-record delivery count. Acknowledging a record the
  member does not hold is `INVALID_RECORD_STATE`. `Accept`, `Reject` and `Gap`
  archive the record, `Release` returns it with a higher delivery count,
  `Renew` keeps it, and records a member holds come back when it leaves or
  closes its session. v2's record-limit acquire mode (KIP-1206) acquires
  exactly `max_records`.

`StreamsGroupDescribe` (KIP-1071, key 89) is served from a fixture a test
populates directly via `with_state`. krafka cannot *join* a Streams group — that
needs `StreamsGroupHeartbeat` and an application topology.

## What it does not implement


- **Acquisition-lock expiry.** A share record returns to the pool when it is
  released or when its holder leaves — never on a timer. Tests must not be read
  as validating lock timeouts or `group.share.delivery.attempts`.
- **Multi-member classic rebalancing.** The classic protocol's coordinator side
  is modelled far more shallowly than KIP-848's.
- **Transaction timeouts.** `transaction.timeout.ms` is validated but no timer
  runs: a transaction stays open until the client ends it or the test calls
  `abort_transaction`.
- **Producer-ID exhaustion.** Epochs are bumped without the overflow to a new
  producer ID at `i16::MAX`.
- **Replication, retention, compaction and quotas.** There is one in-memory log
  per partition and no background machinery at all.
- **Performance.** Benchmarks against it measure the fake; see
  [Performance](@/docs/performance.md#benchmarking).

For any of these, test against a real broker.

## Testing against a real broker with testcontainers

The [`testcontainers`](https://crates.io/crates/testcontainers) crate starts a
Kafka container from a test. The image's default configuration is a
single-node KRaft broker advertising `localhost:9092`, so the container's port
9092 is published on the same host port:

```toml
[dev-dependencies]
testcontainers = "0.27"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

```rust,compile
use std::time::Duration;
use krafka::admin::{CreateTopicsOptions, NewTopic};
use krafka::consumer::AutoOffsetReset;
use krafka::{Kafka, Record};
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

// One Kafka broker; the container is stopped and removed when the handle drops.
async fn start_kafka() -> ContainerAsync<GenericImage> {
    GenericImage::new("apache/kafka", "4.3.1")
        .with_exposed_port(ContainerPort::Tcp(9092))
        .with_wait_for(WaitFor::message_on_stdout("Kafka Server started"))
        .with_mapped_port(9092, ContainerPort::Tcp(9092))
        .start()
        .await
        .expect("start the Kafka container")
}

// The test: the body of a `#[tokio::test] async fn … -> krafka::Result<()>`.
let _container = start_kafka().await;
let kafka = Kafka::builder("localhost:9092").connect().await?;

kafka
    .admin()
    .create_topics([NewTopic::new("orders", 1, 1)?], CreateTopicsOptions::default())
    .await?;

let producer = kafka.producer().build().await?;
producer.send(Record::new("orders", "order 1001 created")).await?;

let consumer = kafka
    .consumer("orders-test")
    .auto_offset_reset(AutoOffsetReset::Earliest)
    .build()
    .await?;
consumer.subscribe(["orders"]).await?;
let records = consumer.poll(Duration::from_secs(10)).await?;
assert_eq!(records[0].value_str(), Some("order 1001 created"));
```

As of 2026-10-09 this uses `testcontainers` 0.27
([crates.io](https://crates.io/crates/testcontainers)) and the
`apache/kafka:4.3.1` image ([Docker Hub](https://hub.docker.com/r/apache/kafka)).
The fixed host port allows one such container per machine at a time; to run
several, give each its own port and set `KAFKA_ADVERTISED_LISTENERS` to match,
which with this image means supplying the full KRaft configuration through
`KAFKA_*` environment variables.

## Negative controls

Check a test by breaking the code under test on purpose and confirming the
test fails. The fake broker supplies controls on its side: `set_idempotence(false)`
for de-duplication, `set_transaction_version` for the transaction protocol,
`set_api_versions` for version negotiation.

## How krafka itself is tested

For contributors. `just ci` is the local gate; each of its recipes is a job in
`.github/workflows/ci.yml` behind the required check `CI`.

| Gate | What it checks | Blocks merge |
|---|---|---|
| `just test` | Unit tests, doc tests and the fake-broker integration tests | yes |
| `just sim` | The deterministic simulation (`tests/simulation.rs`): seeded faults on an in-memory cluster with a paused clock, judged against six invariants (no lost acknowledged write, no duplicate, no aborted read, no partial commit, flush/close completeness, at-least-once). `xtask/determinism.py` keeps the library inside what a seed controls | yes |
| `just sim-replay <workload> <seed>` | Replays one seed and prints its trace | — |
| `just sim-nightly` | 5 000 seeds per workload, then the planted defects in `tests/plants/` (`xtask/plants.py`), each of which must be caught | no (nightly) |
| `just cancel-safety` | Every data-path method has a well-formed `# Cancel safety` section; `tests/cancel_safety.rs` tests each claim by dropping the future at every pending poll | yes |
| `just no-c` | The default build compiles no C beyond `ring` and links no system library | yes |
| `just fuzz` | The fuzz targets, 60 s each per pull request; a longer nightly run does not block | yes |
| `just semver-check`, `just minimal-versions`, `just cross-build` | API compatibility, minimal dependency versions, musl and windows-gnu builds | yes |
| `just integration`, `just integration-matrix` | Apache Kafka in Docker (`apache/kafka-native:3.9.0` by default), every supported minor 3.9 → 4.3 | yes |
| `just integration-sasl`, `just integration-sasl-matrix` | PLAIN, SCRAM-SHA-256/512 and OAUTHBEARER over `SASL_PLAINTEXT` and `SASL_SSL`, Kafka 3.9.0 and 4.3.1. AWS MSK IAM is covered by unit tests only | yes |
| `just integration-redpanda` | The Redpanda release pinned in `tests/redpanda/Dockerfile`; `REDPANDA_VERSION=latest` runs the current one weekly | pinned: yes; `latest`: no |
| `just mutants`, `just mutants-diff origin/main` | cargo-mutants over sequence arithmetic, the in-flight barrier, varint codecs and fetch sessions; per pull request only the mutants its diff touches. `--re <fn>` scopes a run to one function | no |
| `just bench-check` | Send- and consume-path regression against a saved baseline | local only |
| `just ci-job-parity` | Every workflow job is inside `CI` or declared non-blocking | yes |
