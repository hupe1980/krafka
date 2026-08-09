+++
title = "Testing"
description = "Test clients against an in-process Kafka broker with fault injection — leader moves, coordinator failover and corrupt batches, without Docker."
weight = 115

[extra]
slug_id = "testing"
+++

The failure modes worth testing in a Kafka client are the ones that are hard to
cause on purpose. A leader moving mid-produce. A coordinator disappearing
between a heartbeat and a commit. A response arriving after the client gave up
on it. A batch that fails its CRC.

Reproducing those against a real cluster means orchestrating containers and
then hoping the timing lands. `krafka::testing::FakeBroker` causes them
directly: an in-process broker speaking the real wire protocol over a real TCP
socket, so the client under test is the actual `Producer`, `Consumer` or
`AdminClient`, exercising its real network path.

```sh
cargo add --dev krafka --features test-broker
```

## A first test

```rust,compile
use krafka::testing::FakeBroker;
use krafka::producer::Producer;

#[tokio::test]
async fn records_reach_the_broker() {
    let broker = FakeBroker::start().await.unwrap();
    broker.create_topic("orders", 1);

    let producer = Producer::builder()
        .bootstrap_servers(broker.bootstrap_servers())
        .build()
        .await
        .unwrap();

    producer.send("orders", None, b"payload").await.unwrap();

    assert_eq!(broker.next_offset("orders", 0), Some(1));
}
```

`start()` binds a single broker to an ephemeral loopback port. `start_cluster(n)`
binds `n`, each on its own port, which is what makes leader moves and
coordinator failover expressible.

## Injecting faults

`on`, `on_once` and `on_times` install a hook per API key. The hook returns a
`Control` describing what the broker should do instead of answering normally.

```rust
use krafka::{error::ErrorCode, protocol::ApiKey, testing::Control};

// Exactly one CreateTopics lands on a non-controller; the rest are served.
broker.on_once(ApiKey::CreateTopics, |_| Control::Error(ErrorCode::NotController));

let results = admin.create_topics(topics, timeout, false).await?;

assert_eq!(results[0].error, None, "the client should have retried");
assert_eq!(broker.request_count(ApiKey::CreateTopics), 2);
```

| `Control` | What the broker does |
|---|---|
| `Pass` | Falls through to the default handler |
| `Error(code)` | Answers with a **structurally valid** response carrying `code` in whatever field that API actually has — top level, per topic or per partition — so the client runs its normal error handling rather than its malformed-frame path |
| `Delay(d)` | Waits, then answers normally. Pushes the response past the client's request timeout while leaving the connection open, which is how you test "does the client survive a response it no longer wants?" |
| `DelayThen(d, ctrl)` | Waits, then applies the nested control |
| `Disconnect` | Drops the connection without answering |
| `Silence` | Never answers but holds the connection open. Because Kafka responses are ordered per connection, this also blocks every later request on that connection |
| `CorruptRecords` | Answers a `Fetch` normally but flips a byte inside the CRC-covered region, so the batch fails its checksum while the surrounding response still parses. `Fetch` only — applying it elsewhere is an error rather than a silent pass, so a test cannot quietly assert nothing |

## Moving the cluster around

The cluster is mutable mid-test, which is what turns rare production events
into ordinary unit tests:

```rust
// Partition 0's leader moves from broker 0 to broker 1.
broker.set_leader("orders", 0, 1);
broker.bump_leader_epoch("orders", 0);

// The group coordinator fails over.
broker.set_group_coordinator("my-group", 2);

// The controller is lost entirely.
broker.set_controller(-1);

// A broker is up but out of the cluster's view.
broker.set_broker_online(1, false);
```

`set_txn_coordinator` does the same for a transactional ID, and `with_state`
gives direct access for anything not covered by a named setter.

## Transactions and exactly-once

The broker implements the full transactional path — `InitProducerId` with
KIP-360 fencing, `AddPartitionsToTxn`, `AddOffsetsToTxn`, `TxnOffsetCommit`,
`EndTxn`, real commit and abort control batches, and `read_committed`
isolation. A complete consume-transform-produce cycle runs in-process:

```rust,compile
use krafka::consumer::IsolationLevel;
use krafka::producer::TransactionalProducer;
use krafka::testing::FakeBroker;

let broker = FakeBroker::start().await?;
broker.create_topic("orders", 1);

let producer = TransactionalProducer::builder()
    .bootstrap_servers(broker.bootstrap_servers())
    .transactional_id("checkout-0")
    .build()
    .await?;

producer.init_transactions().await?;
producer.begin_transaction()?;
producer.send("orders", None, b"payload").await?;
producer.flush().await?;

// Until the commit lands, a read_committed consumer may not read past the
// transaction's first record.
assert_eq!(broker.last_stable_offset("orders", 0), Some(0));

producer.commit_transaction().await?;
```

### Choosing the transaction protocol

`set_transaction_version` finalizes the cluster's `transaction.version`
feature. The client reads it out of `ApiVersions` and negotiates from it, so
the protocol is chosen the same way a real cluster chooses it:

| Level | Protocol | What the client does |
|---|---|---|
| `0` or `1` *(default)* | TV1 | Registers partitions with `AddPartitionsToTxn` and the offsets topic with `AddOffsetsToTxn` before writing |
| `2` | TV2 (KIP-890) | Sends neither — `Produce` and `TxnOffsetCommit` carry the transactional ID, and `EndTxn` returns a bumped epoch |

```rust
broker.set_transaction_version(2);
// ...run a transaction...
assert_eq!(
    broker.request_count(ApiKey::AddPartitionsToTxn), 0,
    "TV2 exists to remove that round trip; a client that still sends it is wrong",
);
```

### Inspecting transaction state

```rust
broker.transactional_producer("checkout-0");   // (producer_id, epoch)
broker.transaction_is_open("checkout-0");      // is a transaction in flight?
broker.last_stable_offset("orders", 0);        // the read_committed ceiling
broker.aborted_transactions("orders", 0);      // (producer_id, first_offset) pairs
broker.committed_offset("etl-group", "orders", 0);
```

`transactional_producer` is how you assert fencing: re-initialising the same
transactional ID must return the **same** producer ID with a **higher** epoch,
and under TV2 every completed transaction bumps it again. A broker that minted
a fresh producer ID instead would fence nothing, and a test written against it
would pass with the client's fencing deleted.

## Being an older broker

`set_api_versions` overrides the range the broker advertises for one API. This
reaches the branches where the client *degrades* — the code paths that refuse
or strip a feature the cluster is too old for, which otherwise need a real
cluster of the right vintage to reach at all:

```rust
// A broker predating KIP-584's `ValidateOnly` field.
broker.set_api_versions(ApiKey::UpdateFeatures, 0, 0);

let outcome = admin.update_features(updates, true /* validate_only */).await;

assert!(outcome.is_err());
assert_eq!(
    broker.request_count(ApiKey::UpdateFeatures), 0,
    "a dry run that reaches the controller has already stopped being one",
);
```

The second assertion is the load-bearing one. An implementation that sent the
request and *then* complained would pass an error-is-returned check having
already applied a data-lossy change.

## Asserting on what the client did

The point of these tests is the client's behaviour, not the broker's internals:

```rust
broker.request_count(ApiKey::Metadata);          // how many times
broker.request_nodes(ApiKey::UpdateFeatures);    // which brokers, in order
broker.requests();                               // every recorded request
broker.clear_requests();                         // reset between phases

// Wait for the client to act, rather than sleeping and hoping.
broker.wait_for_requests(ApiKey::Fetch, 3, Duration::from_secs(5)).await;
broker.wait_for_request_on_node(ApiKey::Produce, 1, Duration::from_secs(5)).await;
```

Prefer `wait_for_requests` over a `sleep`. A sleep that is long enough on a
developer laptop is a flake on a loaded CI runner, and a flaky test is worse
than no test because it trains people to re-run.

## What the broker implements

The produce and fetch path, `ListOffsets`, `Metadata` v12 (topic UUIDs),
`FindCoordinator`, `CreateTopics`/`DeleteTopics`, `UpdateFeatures` with
controller routing, the full transaction protocol, and **both** group
protocols:

- **Transactions** — `InitProducerId` with KIP-360 fencing (a stable producer
  ID per transactional ID, epoch bumped on every re-initialisation),
  `AddPartitionsToTxn` and `AddOffsetsToTxn` under TV1, `TxnOffsetCommit` with
  KIP-447 generation-and-member fencing, and `EndTxn` writing real commit and
  abort control batches. Offsets staged by a transaction are applied only on
  commit. `read_committed` fetches stop at the last stable offset and report
  aborted transactions, so the consumer's own filtering runs for real.
  KIP-890's TV2 is selected by finalizing `transaction.version` — see above.

- **Classic** — `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`,
  `OffsetCommit`, `OffsetFetch`.
- **KIP-848** — `ConsumerGroupHeartbeat` v1 with real revoke-before-assign
  reconciliation: a partition moves to its new owner strictly after the
  previous owner confirms releasing it, so no two members ever believe they own
  it at once.
- **KIP-932 share groups** — `ShareGroupHeartbeat`, `ShareFetch` and
  `ShareAcknowledge` v1, backed by the share-partition state machine that
  replaces committed offsets: a start offset, an acquisition cursor and a
  per-record delivery count. `Accept` and `Reject` advance the start offset,
  `Release` returns the record to the pool with a higher delivery count, and
  records left in flight come back when the member holding them leaves.

`StreamsGroupDescribe` (KIP-1071, key 89) is served from a fixture a test
populates directly via `with_state`. krafka cannot *join* a Streams group — that
needs `StreamsGroupHeartbeat` and an application topology — so there is nothing
for the broker to derive group state from. It exists to exercise the decoder,
whose response carries two nullable structs behind presence bytes, a nullable
array nested inside one of them, and a `uint16` port.

## What it deliberately does not implement

A fake that quietly pretends is worse than one that refuses, so the boundaries
are explicit:

- **Acquisition-lock expiry.** A share record returns to the pool when it is
  released or when its holder leaves — never on a timer. Tests must not be read
  as validating lock timeouts, the archived state, or
  `group.share.delivery.attempts`.
- **`ShareFetch`/`ShareAcknowledge` v2.** Not advertised, because neither
  `ShareAcquireMode` (KIP-1206) nor renew-ack (KIP-1222) is modelled. A broker
  advertising a version whose semantics it does not implement would make tests
  pass for the wrong reason.
- **Multi-member classic rebalancing.** The classic protocol's coordinator side
  is modelled far more shallowly than KIP-848's.
- **Transaction timeouts.** `transaction.timeout.ms` is accepted and ignored:
  a transaction stays open until the client ends it. Tests must not be read as
  validating coordinator-side timeout behaviour.
- **Replication, retention, compaction and quotas.** There is one in-memory log
  per partition and no background machinery at all.
- **Performance.** The fake broker is an excellent correctness harness and a
  poor performance one — see [Performance](@/docs/performance.md) for why
  benchmarking against it measures the fake.

## Writing a test that can actually fail

Every test worth having should be checked with a **negative control**: break
the production code on purpose and confirm the test goes red. It is the only
evidence a test works, and it is cheap.

This matters more than it sounds. krafka's own share-group model shipped a
first draft where deleting the `Accept` handling changed nothing observable —
the field it wrote was never read back, so an accepted record and a dropped one
produced identical broker state. The test looked like proof and was not. The
fix was to model the event that makes the state load-bearing, and the negative
control is what found it.

If a test passes with the feature deleted, it is not testing the feature.

### Automating the negative control

Doing that by hand catches the tests you thought to check. `just mutants` runs
[cargo-mutants](https://mutants.rs), which does it exhaustively: it corrupts the
code in small ways — flip a `<` to `<=`, swap a `-` for a `+`, replace a
function body with a constant — and reports every corruption the suite still
passes. A surviving mutant is a missing assertion.

```bash
just mutants                       # the scoped set
just mutants -- --re is_newer_sequence   # one function, when verifying a fix
```

It is **not** part of `just ci`, and that is deliberate: the crate has ~8 200
mutants, which is roughly 45 CPU-hours, and the timing-based fake-broker tests
turn many of them into timeouts rather than failures. The recipe is scoped to
pure, fast, high-consequence code — sequence arithmetic, the in-flight barrier,
varint codecs, fetch sessions — where a surviving mutant is unambiguous.

It has earned that place. On its first run it showed that **four separate
corruptions of `is_newer_sequence` passed the entire suite**. That function
decides whether a broker acknowledgement moves the producer's
`last_acked_sequence` forward. Every existing test used `candidate == 0`,
`last == 0`, or values small enough that subtracting, adding *and* dividing all
landed on the same side of the half-sequence-space threshold — so the
assertions held for arithmetic that was wrong.

The pattern in every gap it found is the same: **the code fails quietly.** A
fetch-session epoch stuck at a constant does not error, it just makes the broker
reject every incremental fetch so the client silently falls back to full ones. A
zero topic UUID treated as a real identifier does not error, it collapses every
pre-v13 topic onto one session key. Tests catch loud failures on their own;
mutation testing is for the quiet ones.

When you verify a fix, scope it with `--re` to the function you changed rather
than reading a whole-file tally — it answers "did my new tests kill *these*
mutants?" in minutes instead of half an hour.
