---
layout: default
title: Error Handling
nav_order: 9
description: "Error types and handling strategies"
---

# Error Handling Guide

This guide covers error handling in Krafka, including error types, strategies, and best practices.

## Error Types

Krafka uses a single error enum for all error conditions:

```rust
pub enum KrafkaError {
    /// Network-related errors (connection, I/O)
    Network(Arc<io::Error>),

    /// Protocol encoding/decoding errors
    /// Display: "protocol error ({kind:?}): {message}"
    Protocol { kind: ProtocolErrorKind, message: String },

    /// Authentication failures
    Auth { message: String },

    /// Operation timeouts
    Timeout { operation: String },

    /// Kafka broker errors (with error code)
    Broker { code: ErrorCode, message: String },

    /// Configuration errors
    Config { message: String },

    /// Compression/decompression errors
    Compression { message: String },

    /// Invalid state errors
    InvalidState { message: String },

    /// Serialization errors
    Serialization { message: String },
}
```

## Kafka Error Codes

Krafka defines all Kafka error codes in the `ErrorCode` enum:

```rust
use krafka::error::ErrorCode;

// Common error codes
ErrorCode::None                      // 0: No error
ErrorCode::UnknownServerError        // -1: Unknown error
ErrorCode::OffsetOutOfRange          // 1: Offset out of range
ErrorCode::NotLeaderForPartition     // 6: Not leader for partition
ErrorCode::RequestTimedOut           // 7: Request timed out
ErrorCode::MessageTooLarge           // 10: Message too large
ErrorCode::UnknownTopicOrPartition   // 3: Unknown topic
ErrorCode::LeaderNotAvailable        // 5: Leader not available
ErrorCode::TopicAlreadyExists        // 36: Topic already exists
ErrorCode::InvalidTopic              // 17: Invalid topic
ErrorCode::GroupAuthorizationFailed  // 30: Group auth failed
ErrorCode::SaslAuthenticationFailed  // 58: SASL auth failed
ErrorCode::UnknownProducerId         // 59: Unknown producer ID
ErrorCode::FencedInstanceId          // 82: Fenced instance ID
ErrorCode::UnstableOffsetCommit      // 88: Unstable offset commit
ErrorCode::ProducerFenced            // 90: Producer fenced (zombie)
ErrorCode::UnknownTopicId            // 100: Unknown topic ID (KIP-516)
ErrorCode::InconsistentTopicId       // 103: Topic ID mismatch
ErrorCode::FetchSessionTopicIdError  // 106: Fetch session topic ID error
ErrorCode::OffsetMovedToTieredStorage // 109: Offset in tiered storage (KIP-405)
ErrorCode::FencedMemberEpoch         // 110: Fenced member epoch (KIP-848)
ErrorCode::StaleMemberEpoch          // 113: Stale member epoch (KIP-848)
ErrorCode::TransactionAbortable      // 120: Transaction abortable (KIP-890)
```

> **Note:** `OffsetOutOfRange` errors during fetch are automatically handled by the consumer — it applies the configured `auto_offset_reset` policy to recover the affected partition without returning an error to the application.

### Checking Error Codes

```rust
use krafka::error::ErrorCode;

// Check if error code indicates success
if error_code.is_ok() {
    println!("Success!");
}

// Convert from raw i16
let code = ErrorCode::from_i16(6);
assert_eq!(code, ErrorCode::NotLeaderForPartition);
```

## Error Handling Patterns

### Basic Error Handling

```rust
use krafka::error::{KrafkaError, Result};
use krafka::producer::Producer;

async fn send_message(producer: &Producer) -> Result<()> {
    match producer.send("topic", Some(b"key"), b"value").await {
        Ok(metadata) => {
            println!("Sent to partition {} offset {}", 
                     metadata.partition, metadata.offset);
            Ok(())
        }
        Err(e) => {
            eprintln!("Send failed: {}", e);
            Err(e)
        }
    }
}
```

### Pattern Matching on Errors

```rust
use krafka::error::KrafkaError;

fn handle_error(error: KrafkaError) {
    match error {
        KrafkaError::Timeout { operation } => {
            eprintln!("Operation timed out: {}", operation);
            // Consider retrying
        }
        KrafkaError::Broker { code, message } => {
            eprintln!("Broker error {:?}: {}", code, message);
            // Check if retriable
        }
        KrafkaError::Auth { message } => {
            eprintln!("Authentication failed: {}", message);
            // Likely not retriable - check credentials
        }
        KrafkaError::Config { message } => {
            eprintln!("Configuration error: {}", message);
            // Fix configuration and restart
        }
        _ => {
            eprintln!("Other error: {}", error);
        }
    }
}
```

### Retry Logic

`KrafkaError` exposes a built-in `.is_retriable()` method so callers don't need
to duplicate retry-classification logic. Protocol errors are further classified
by `ProtocolErrorKind` (see [below](#protokollerrorkind)) so you can distinguish
a transient truncated frame from a permanent API-version mismatch.

```rust
use krafka::error::KrafkaError;
use std::time::Duration;

async fn send_with_retry<F, T>(
    mut operation: F,
    max_retries: u32,
    backoff: Duration,
) -> Result<T, KrafkaError>
where
    F: FnMut() -> futures::future::BoxFuture<'static, Result<T, KrafkaError>>,
{
    let mut attempts = 0;

    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(e) if e.is_retriable() && attempts < max_retries => {
                attempts += 1;
                let delay = backoff * attempts;
                eprintln!(
                    "Retriable error (attempt {}/{}): {}. Retrying in {:?}",
                    attempts, max_retries, e, delay
                );
                tokio::time::sleep(delay).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

## ProtocolErrorKind

`KrafkaError::Protocol` carries a structured [`ProtocolErrorKind`] classification
alongside the human-readable message. This lets callers make retry decisions
without substring-matching the message text.

```rust
use krafka::{KrafkaError, ProtocolErrorKind};

fn handle_protocol_error(err: &KrafkaError) {
    match err.protocol_error_kind() {
        Some(ProtocolErrorKind::TruncatedFrame) => {
            // Retriable — transient short read, reconnect and retry.
        }
        Some(ProtocolErrorKind::CrcMismatch) => {
            // Retriable — on-wire corruption, reconnect and retry.
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

```
protocol error (CrcMismatch): record batch CRC check failed
```

### ProtocolErrorKind variants

| Variant | Retriable | Meaning |
|---|---|---|
| `TruncatedFrame` | ✓ | Buffer exhausted before a complete frame could be read |
| `CrcMismatch` | ✓ | Record batch CRC32C mismatch — on-wire corruption |
| `Malformed` | ✓ | Structurally malformed response (often transient) |
| `UnknownApiVersion` | ✗ | No mutually supported API version — permanent mismatch |
| `InvalidLength` | ✗ | Encoded length exceeds protocol maximum or safety cap |
| `InvalidUtf8` | ✗ | Bytes decoded as UTF-8 string were not valid UTF-8 |
| `UnsupportedMagic` | ✗ | Record batch magic byte is not version 2 |
| `InvalidValue` | ✗ | Field value outside allowed range or malformed varint |
| `Other` | ✗ | Catch-all; inspect the message for details |

### Error Context

Add context to errors for better debugging:

```rust
use krafka::error::{KrafkaError, Result};

async fn process_topic(producer: &Producer, topic: &str) -> Result<()> {
    producer
        .send(topic, None, b"message")
        .await
        .map_err(|e| {
            eprintln!("Failed to send to topic {}: {}", topic, e);
            e
        })?;
    
    Ok(())
}
```

## Consumer Error Handling

### `AutoOffsetReset::None` Error

When `auto_offset_reset` is set to `None` and a partition has no committed offset, `poll()` will return an error:

```rust
use krafka::consumer::{Consumer, AutoOffsetReset};

let consumer = Consumer::builder()
    .bootstrap_servers("localhost:9092")
    .group_id("strict-group")
    .auto_offset_reset(AutoOffsetReset::None)
    .build()
    .await?;

// This will error if any assigned partition has no committed offset
match consumer.poll(Duration::from_secs(1)).await {
    Err(e) => eprintln!("No committed offset: {}", e),
    Ok(records) => { /* process */ }
}
```

### Handling Poll Errors

```rust
use krafka::consumer::Consumer;
use krafka::error::KrafkaError;
use std::time::Duration;

async fn consume_safely(consumer: &Consumer) {
    loop {
        match consumer.poll(Duration::from_secs(1)).await {
            Ok(records) => {
                for record in records {
                    if let Err(e) = process_record(&record).await {
                        eprintln!("Failed to process record: {}", e);
                        // Decide: skip, retry, or stop
                    }
                }
            }
            Err(KrafkaError::Timeout { .. }) => {
                // Normal - no messages available
                continue;
            }
            Err(KrafkaError::Broker { code, message }) => {
                eprintln!("Broker error {:?}: {}", code, message);
                // May need to refresh metadata or reconnect
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
            Err(e) => {
                eprintln!("Fatal error: {}", e);
                break;
            }
        }
    }
}
```

### Commit Error Handling

```rust
use krafka::error::KrafkaError;

async fn commit_with_retry(consumer: &Consumer, retries: u32) -> Result<(), KrafkaError> {
    let mut attempts = 0;
    
    loop {
        match consumer.commit().await {
            Ok(()) => return Ok(()),
            Err(e) if attempts < retries => {
                attempts += 1;
                eprintln!("Commit failed (attempt {}): {}", attempts, e);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => return Err(e),
        }
    }
}
```

## Producer Error Handling

### Send Error Handling

```rust
use krafka::producer::Producer;
use krafka::error::{KrafkaError, ErrorCode};

async fn send_critical_message(
    producer: &Producer,
    topic: &str,
    key: &[u8],
    value: &[u8],
) -> Result<(), KrafkaError> {
    const MAX_RETRIES: u32 = 3;
    
    for attempt in 1..=MAX_RETRIES {
        match producer.send(topic, Some(key), value).await {
            Ok(metadata) => {
                println!("Message sent to {}:{}", metadata.partition, metadata.offset);
                return Ok(());
            }
            Err(KrafkaError::Broker { code: ErrorCode::MessageTooLarge, .. }) => {
                // Not retriable - message is too large
                return Err(KrafkaError::config("Message exceeds max size"));
            }
            Err(e) if e.is_retriable() && attempt < MAX_RETRIES => {
                eprintln!("Send failed (attempt {}): {}. Retrying...", attempt, e);
                tokio::time::sleep(Duration::from_millis(100 * attempt as u64)).await;
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
    
    Err(KrafkaError::timeout("send after retries"))
}
```

## Admin Error Handling

### Create Topic Errors

```rust
use krafka::admin::{AdminClient, NewTopic};
use krafka::error::KrafkaError;

async fn ensure_topic_exists(
    admin: &AdminClient,
    name: &str,
    partitions: i32,
    replication_factor: i16,
) -> Result<(), KrafkaError> {
    let topic = NewTopic::new(name, partitions, replication_factor);
    
    match admin.create_topics(vec![topic], Duration::from_secs(30)).await {
        Ok(results) => {
            for result in results {
                match &result.error {
                    None => println!("Created topic: {}", result.name),
                    Some(e) if e.contains("TOPIC_ALREADY_EXISTS") => {
                        println!("Topic {} already exists", result.name);
                    }
                    Some(e) => {
                        return Err(KrafkaError::broker(
                            ErrorCode::UnknownServerError,
                            e.clone(),
                        ));
                    }
                }
            }
            Ok(())
        }
        Err(e) => Err(e),
    }
}
```

## Best Practices

### 1. Always Handle Errors

```rust
// ❌ Bad: ignoring errors
let _ = producer.send("topic", None, b"value").await;

// ✅ Good: handling errors
if let Err(e) = producer.send("topic", None, b"value").await {
    log::error!("Send failed: {}", e);
}
```

### 2. Use Appropriate Retry Strategies

```rust
// ❌ Bad: infinite retries
loop {
    if producer.send(...).await.is_ok() {
        break;
    }
}

// ✅ Good: bounded retries with backoff
let mut attempts = 0;
while attempts < 3 {
    match producer.send(...).await {
        Ok(_) => break,
        Err(e) if e.is_retriable() => {
            attempts += 1;
            tokio::time::sleep(Duration::from_millis(100 << attempts)).await;
        }
        Err(e) => return Err(e),
    }
}
```

### 3. Log Errors with Context

```rust
// ❌ Bad: minimal logging
log::error!("Error: {}", e);

// ✅ Good: contextual logging
log::error!(
    topic = %topic,
    partition = %partition,
    offset = %offset,
    "Failed to process message: {}",
    e
);
```

### 4. Graceful Degradation

```rust
async fn process_with_fallback(record: &ConsumerRecord) -> Result<()> {
    match primary_processing(record).await {
        Ok(()) => Ok(()),
        Err(e) => {
            log::warn!("Primary processing failed: {}. Using fallback.", e);
            fallback_processing(record).await
        }
    }
}
```

### 5. Circuit Breaker Pattern

```rust
use std::sync::atomic::{AtomicU32, AtomicBool, Ordering};
use std::time::{Duration, Instant};

struct CircuitBreaker {
    failures: AtomicU32,
    threshold: u32,
    open: AtomicBool,
    opened_at: std::sync::Mutex<Option<Instant>>,
    reset_timeout: Duration,
}

impl CircuitBreaker {
    fn record_failure(&self) {
        let failures = self.failures.fetch_add(1, Ordering::SeqCst) + 1;
        if failures >= self.threshold {
            self.open.store(true, Ordering::SeqCst);
            *self.opened_at.lock().unwrap() = Some(Instant::now());
        }
    }

    fn record_success(&self) {
        self.failures.store(0, Ordering::SeqCst);
        self.open.store(false, Ordering::SeqCst);
    }

    fn is_open(&self) -> bool {
        if !self.open.load(Ordering::SeqCst) {
            return false;
        }
        // Check if we should try again
        if let Some(opened_at) = *self.opened_at.lock().unwrap() {
            if opened_at.elapsed() > self.reset_timeout {
                return false;  // Allow retry
            }
        }
        true
    }
}
```

## Next Steps

- [Configuration Reference](configuration.md) - Timeout and retry settings
- [Architecture Overview](architecture.md) - How errors flow through the system
