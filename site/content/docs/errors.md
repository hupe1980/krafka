+++
title = "Error Handling"
description = "Error taxonomy, which errors are retriable, and how to build a dead-letter path."
weight = 120

[extra]
slug_id = "errors"
+++

## Error Types

krafka uses a single error enum for all error conditions. It is
`#[non_exhaustive]`, so a `match` needs a wildcard arm.

```rust
pub enum KrafkaError {
    /// Connecting, I/O, or a broker that is not reachable. Retriable.
    Network(Arc<io::Error>),
    /// Encoding/decoding; see `ProtocolErrorKind`.
    Protocol { kind: ProtocolErrorKind, message: String },
    /// SASL, TLS certificate or OIDC token-endpoint failure. Fatal.
    Auth { message: String, source: Option<ArcError> },
    /// An operation exceeded its time bound. Retriable.
    Timeout { operation: String },
    /// A record was not acknowledged within `delivery_timeout`.
    DeliveryTimeout { possibly_written: bool, message: String },
    /// An error code returned by a broker.
    Broker { code: ErrorCode, message: String },
    Config { message: String },
    Compression { message: String },
    Serialization { message: String },
    /// A consumed record could not be deserialized.
    RecordDeserialization { topic: String, partition: i32, offset: i64, part: &'static str, message: String },
    /// The client was closed. Fatal.
    Closed { message: String },
    /// A blocking consumer call was interrupted by `wakeup()`.
    Wakeup,
    /// Another producer took over this identity. Fatal.
    Fenced { message: String },
    /// The open transaction must be aborted.
    TransactionAbortable { message: String },
    /// No committed offset and no reset policy.
    NoOffset { partitions: Vec<(String, i32)> },
    UnknownTopic { topic: String },
    /// The broker rejected a batch's sequence number.
    OutOfOrderSequence { topic: String, partition: i32, message: String },
    /// A call that is not valid in the client's current state.
    IllegalState { message: String },
}
```

Errors keep their cause: `std::error::Error::source()` walks from a
`KrafkaError` to the I/O or TLS error underneath it.

### Where each kind comes from

| Kind | Raised when | Predicate |
|------|-------------|-----------|
| `Network` | Connecting, I/O, a reset during a TLS handshake, the connection cap reached | retriable |
| `Timeout` | A request or other operation exceeded its bound | retriable |
| `Auth` | SASL failed, a certificate was rejected, the OIDC endpoint failed | fatal |
| `Closed` | A call on a client (or a part it needs) that has been closed; records still unresolved when `close_with` times out | fatal |
| `Fenced` | Another producer with the same transactional id, or the same idempotent identity, took over | fatal |
| `TransactionAbortable` | A send or `send_offsets` of the open transaction failed; `commit()` and later sends refuse with it, and `abort()` fails the records it drops with it | requires abort |
| `DeliveryTimeout { possibly_written }` | A record was not acknowledged within `delivery_timeout`; `possibly_written` says whether the broker may hold it | — |
| `OutOfOrderSequence` | The broker lost an earlier batch of an idempotent producer's partition; the batch fails and the producer continues on a new epoch | — |
| `NoOffset` | `auto_offset_reset(AutoOffsetReset::None)` and a partition has no committed offset; `seek_to_timestamp` past the newest record | — |
| `Wakeup` | `Consumer::wakeup()` interrupted a blocking consumer call | — |
| `UnknownTopic` | The consumer has no metadata for the topic after a refresh (`offsets_for_times_for_topic`) | — |
| `IllegalState` | A call that is not valid now: `send` before `begin`, `seek` on an unassigned partition, `committed` without a group | — |
| `Broker { code }` | The broker answered with an error code; `is_retriable()` and `is_fatal()` follow the code | by code |
| `Protocol { kind }` | Encoding or decoding failed; see [ProtocolErrorKind](#protocolerrorkind) | by kind |
| `Config` | An invalid builder setting, named in the message | — |
| `Serialization` / `RecordDeserialization` | A serializer or deserializer failed | — |

### What to do next

Three predicates decide what to do without matching on variants:

| Predicate | Meaning |
|-----------|---------|
| `is_retriable()` | The same operation may succeed if tried again. |
| `requires_abort()` | The open transaction must be aborted; the producer stays usable. |
| `is_fatal()` | The client cannot continue; build a new one. |

`requires_abort()` and `is_fatal()` are never both true. A connection reset
during a TLS handshake is `Network` (retriable); a certificate the client or
broker rejects is `Auth` (fatal).

## Kafka Error Codes

`ErrorCode` names every Kafka error code (`ErrorCode::NotLeaderForPartition`,
`ErrorCode::TopicAlreadyExists`, …); a code krafka does not know is
`ErrorCode::Unknown(i16)`. `OffsetOutOfRange` during a fetch never reaches the
application: the consumer applies `auto_offset_reset`.

```rust,compile
use krafka::error::ErrorCode;

// Convert from raw i16
let code = ErrorCode::from_i16(6);
assert_eq!(code, ErrorCode::NotLeaderForPartition);
assert!(code.is_retriable());

// Check if an error code indicates success
assert!(ErrorCode::from_i16(0).is_ok());
```

## Matching on errors

```rust,compile
use krafka::error::KrafkaError;

fn handle_error(error: KrafkaError) {
    match error {
        KrafkaError::Timeout { operation } => {
            eprintln!("Operation timed out: {}", operation);
        }
        KrafkaError::Broker { code, message } => {
            eprintln!("Broker error {:?}: {}", code, message);
        }
        KrafkaError::Auth { message, .. } => {
            eprintln!("Authentication failed: {}", message); // fatal: check credentials
        }
        KrafkaError::DeliveryTimeout { possibly_written, .. } => {
            eprintln!("Not acknowledged in time (possibly written: {possibly_written})");
        }
        KrafkaError::Config { message } => {
            eprintln!("Configuration error: {}", message);
        }
        _ => {
            eprintln!("Other error: {}", error);
        }
    }
}
```

The producer, the consumer and `commit()` already retry retriable failures;
use `is_retriable()` to retry your own calls around them, such as admin
operations.

## ProtocolErrorKind

`KrafkaError::Protocol` carries a `ProtocolErrorKind` next to the message, so
callers can decide without matching on text.

```rust,compile
use krafka::{KrafkaError, ProtocolErrorKind};

fn handle_protocol_error(err: &KrafkaError) {
    match err.protocol_error_kind() {
        Some(ProtocolErrorKind::TruncatedFrame) => {
            // Retriable — transient short read, reconnect and retry.
        }
        Some(ProtocolErrorKind::CrcMismatch) => {
            // NOT retriable — re-fetching the same offset returns the same
            // bytes. Treat as data corruption and escalate.
        }
        Some(ProtocolErrorKind::UnknownApiVersion) => {
            // Not retriable — permanent client/broker version mismatch.
        }
        Some(ProtocolErrorKind::InvalidLength) => {
            // Not retriable — malformed response or misconfigured safety cap.
        }
        Some(kind) => {
            eprintln!("Protocol error ({kind:?}): {err}");
        }
        None => { /* not a Protocol variant */ }
    }
}
```

The display format for `KrafkaError::Protocol` is:

```text
protocol error (CrcMismatch): record batch CRC check failed
```

### ProtocolErrorKind variants

| Variant | Retriable | Meaning |
|---|---|---|
| `TruncatedFrame` | ✓ | Buffer exhausted before a complete frame could be read |
| `CrcMismatch` | ✗ | Record batch CRC32C mismatch — corruption; re-fetching the same offset yields the same bytes |
| `FrameTooLarge` | ✗ | A request frame larger than `max_request_size`, rejected before sending |
| `Malformed` | ✓ | Structurally malformed response (often transient) |
| `UnknownApiVersion` | ✗ | No mutually supported API version — permanent mismatch. When the cluster lacks a feature, the message names it and the setting that avoids it ([table](@/docs/protocol.md#a-cluster-without-a-feature)) |
| `InvalidLength` | ✗ | Encoded length exceeds protocol maximum or safety cap |
| `InvalidUtf8` | ✗ | Bytes decoded as UTF-8 string were not valid UTF-8 |
| `UnsupportedMagic` | ✗ | Record batch magic byte is not version 2 |
| `InvalidValue` | ✗ | Field value outside allowed range or malformed varint |
| `Other` | ✗ | Catch-all; inspect the message for details |

## Consumer Error Handling

### `AutoOffsetReset::None` Error

With `AutoOffsetReset::None`, a partition without a committed offset makes `poll()` return `KrafkaError::NoOffset` naming the partitions:

```rust,compile
use krafka::consumer::{Consumer, AutoOffsetReset};

let consumer = krafka::Kafka::builder("localhost:9092")
    .connect()
    .await?
    .consumer("strict-group")
    .auto_offset_reset(AutoOffsetReset::None)
    .build()
    .await?;

// This will error if any assigned partition has no committed offset
match consumer.poll(Duration::from_secs(1)).await {
    Err(KrafkaError::NoOffset { partitions }) => {
        for (topic, partition) in partitions {
            // Choose a position explicitly, e.g. with seek_to_beginning.
            eprintln!("no committed offset for {topic}-{partition}");
        }
    }
    Err(e) => return Err(e),
    Ok(records) => { /* process */ }
}
```

### Handling Poll Errors

`poll()` returns an empty batch when its timeout passes, not an error:

```rust,compile
use krafka::consumer::{Consumer, ConsumerRecord};
use krafka::error::KrafkaError;
use std::time::Duration;

async fn process_record(record: &ConsumerRecord) -> krafka::Result<()> {
    Ok(())
}

async fn consume_safely(consumer: &Consumer) -> krafka::Result<()> {
    loop {
        match consumer.poll(Duration::from_secs(1)).await {
            Ok(records) => {
                for record in records {
                    if let Err(e) = process_record(&record).await {
                        eprintln!("Failed to process record: {}", e);
                        // Decide: skip, retry, or dead-letter
                    }
                }
            }
            // wakeup() from another task: stop cleanly.
            Err(KrafkaError::Wakeup) => return Ok(()),
            // A record that cannot be deserialized: skip past it.
            Err(KrafkaError::RecordDeserialization { topic, partition, offset, .. }) => {
                consumer.seek(&topic, partition, offset + 1).await?;
            }
            Err(e) if e.is_retriable() => {
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

## Producer Error Handling

A send error is final for that record: the producer has already retried
until `delivery_timeout`. Decide by kind:

```rust,compile
use krafka::Record;
use krafka::producer::Producer;
use krafka::error::{ErrorCode, KrafkaError};

async fn send_critical_message(producer: &Producer, record: Record) -> krafka::Result<()> {
    match producer.send(record).await {
        Ok(metadata) => {
            println!("Message sent to {}:{}", metadata.partition, metadata.offset);
            Ok(())
        }
        Err(KrafkaError::Broker { code: ErrorCode::MessageTooLarge, .. }) => {
            // The record is larger than the topic accepts; resending cannot help.
            Err(KrafkaError::config("record exceeds the topic's max.message.bytes"))
        }
        Err(KrafkaError::DeliveryTimeout { possibly_written, .. }) => {
            // possibly_written == false: never reached the broker, safe to resend.
            // possibly_written == true: a resend may duplicate it.
            Err(KrafkaError::timeout(format!(
                "send not acknowledged (possibly written: {possibly_written})"
            )))
        }
        Err(e) if e.is_fatal() => {
            // Closed, fenced or not authorized: build a new producer.
            Err(e)
        }
        Err(e) => Err(e),
    }
}
```

## Admin Error Handling

### Create Topic Errors

`create_topics` returns a result per topic:

```rust,compile
use krafka::admin::{AdminClient, CreateTopicsOptions, NewTopic};
use krafka::error::{ErrorCode, KrafkaError};

async fn ensure_topic_exists(
    admin: &AdminClient,
    name: &str,
    partitions: i32,
    replication_factor: i16,
) -> krafka::Result<()> {
    let topic = NewTopic::new(name, partitions, replication_factor)?;
    let results = admin
        .create_topics([topic], CreateTopicsOptions::default())
        .await?;
    for (name, result) in results {
        match result {
            Ok(()) => println!("Created topic: {name}"),
            Err(KrafkaError::Broker { code: ErrorCode::TopicAlreadyExists, .. }) => {
                println!("Topic {name} already exists");
            }
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
```

## Dead Letter Queue

A _dead-letter queue_ (DLQ) receives consumed records that cannot be
processed, so one poison pill does not block the partition. Send the record
to a dead-letter topic with an ordinary `Producer`; `krafka::dlq::record_for`
builds the record and attaches provenance headers:

| Header | Value |
|--------|-------|
| `__krafka.dlq.original.topic` | original topic name |
| `__krafka.dlq.original.partition` | partition number |
| `__krafka.dlq.original.offset` | record offset |
| `__krafka.dlq.exception.message` | error description |

```rust,compile
use krafka::consumer::ConsumerRecord;
use krafka::producer::Producer;

async fn dead_letter(
    dlq: &Producer,
    record: &ConsumerRecord,
    error: &str,
) -> krafka::Result<()> {
    dlq.send(krafka::dlq::record_for("my-topic.DLQ", record, &error))
        .await?;
    Ok(())
}
```

The record keeps the original key, value and headers, with the provenance
headers appended after them, and no partition: the dead-letter topic chooses
its own. `record_for` preserves nulls: a tombstone stays a tombstone and a null
header value stays null, which matters when the dead-letter topic is itself
compacted. See
[Tombstones and Compacted Topics](@/docs/producer.md#tombstones-and-compacted-topics).

A failed *send* returns its error to the caller; `send` takes the record by
value, so clone it first to route it elsewhere on failure.

## Guidelines

- **Do not wrap sends in a retry loop.** The producer retries until
  `delivery_timeout`; an error from `send()` is what is left after that.
  Resend only a `DeliveryTimeout { possibly_written: false }`, or accept
  duplicates. Raise `delivery_timeout` rather than adding a loop.
- **Stop on fatal errors.** `is_fatal()` means the client cannot continue:
  close it and build a new one, or exit.
- **Log with context**: topic, partition and offset alongside the error.

## Next Steps

- [Configuration Reference](@/docs/configuration.md) - Timeout and retry settings
