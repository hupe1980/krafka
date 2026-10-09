//! The network layer behaves as Kafka's own clients do: snappy as Java writes
//! it, record batches bounded by bytes, one dial per call with per-address
//! backoff, a connection cap that never blocks a replacement, KIP-219 muting,
//! and close on the first request timeout.
//!
//! Run: `cargo test --features test-broker --test network_correctness`
#![cfg(all(feature = "test-broker", feature = "internal"))]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use krafka::__private::network::{BrokerConnection, ConnectionConfig, ConnectionPool};
use krafka::__private::protocol::{ApiKey, Compression, RecordBatch, RecordBatchBuilder};
use krafka::error::KrafkaError;
use krafka::testing::{Control, FakeBroker};

/// The error of a call expected to fail (the `Ok` type need not be `Debug`).
fn expect_err<T>(result: Result<T, KrafkaError>) -> KrafkaError {
    match result {
        Ok(_) => panic!("expected an error"),
        Err(e) => e,
    }
}

// ---------------------------------------------------------------------------
// Snappy written by the Java client (xerial framing) decodes.
// ---------------------------------------------------------------------------

/// Wrap `raw` in xerial snappy-java framing exactly as Kafka's
/// `SnappyCompression.wrapForOutput` (`org.xerial.snappy.SnappyOutputStream`)
/// does: 8-byte magic, version=1, compatible=1, then [len BE][raw snappy block].
fn xerial_frame(raw: &[u8]) -> Vec<u8> {
    let mut out = vec![0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0];
    out.extend_from_slice(&1i32.to_be_bytes());
    out.extend_from_slice(&1i32.to_be_bytes());
    for chunk in raw.chunks(32 * 1024) {
        let block = snap::raw::Encoder::new().compress_vec(chunk).unwrap();
        out.extend_from_slice(&(block.len() as i32).to_be_bytes());
        out.extend_from_slice(&block);
    }
    out
}

/// Re-pack an uncompressed v2 batch as a snappy batch whose payload uses the
/// given compressed record bytes (header fields kept, length and CRC recomputed).
fn repack_as_snappy(uncompressed_batch: &[u8], compressed_records: &[u8]) -> Bytes {
    const RECORDS_START: usize = 61;
    let mut b = BytesMut::new();
    b.put_slice(&uncompressed_batch[..21]); // base_offset .. crc (patched)
    let attrs = i16::from_be_bytes([uncompressed_batch[21], uncompressed_batch[22]]);
    b.put_i16((attrs & !0x07) | 2); // codec bits = snappy
    b.put_slice(&uncompressed_batch[23..RECORDS_START]);
    b.put_slice(compressed_records);
    let batch_length = (b.len() - 12) as i32;
    b[8..12].copy_from_slice(&batch_length.to_be_bytes());
    let crc = krafka::__private::util::crc32c(&b[21..]);
    b[17..21].copy_from_slice(&crc.to_be_bytes());
    b.freeze()
}

/// Reverted-line control: with the xerial branch of `decompress_snappy`
/// removed (raw only), the second decode fails with
/// `snappy: corrupt input` and this test fails.
#[test]
fn n1_java_client_snappy_batch_decodes() {
    let plain = RecordBatchBuilder::new()
        .add_record(Some("k"), Some("written by a Java producer"))
        .build()
        .encode()
        .unwrap();
    let records = &plain[61..];

    // Control: raw snappy decodes.
    let raw = snap::raw::Encoder::new().compress_vec(records).unwrap();
    let mut ok = repack_as_snappy(&plain, &raw);
    let decoded = RecordBatch::decode(&mut ok).expect("raw snappy control must decode");
    assert_eq!(decoded.records.len(), 1);
    assert_eq!(decoded.attributes.compression, Compression::Snappy);

    // Java/xerial-framed snappy: what Kafka's own client and a broker that
    // recompresses for `compression.type=snappy` write.
    let mut java = repack_as_snappy(&plain, &xerial_frame(records));
    let result = RecordBatch::decode(&mut java);
    assert!(
        result.is_ok(),
        "xerial-framed snappy (Java client / broker recompression) must decode, got {result:?}"
    );
    assert_eq!(
        result.unwrap().records[0].value.as_deref(),
        Some(&b"written by a Java producer"[..])
    );
}

/// What krafka encodes with snappy is what Java writes.
#[test]
fn snappy_batches_krafka_writes_use_the_xerial_stream() {
    let encoded = RecordBatchBuilder::new()
        .compression(Compression::Snappy)
        .add_record(Some("k"), Some("v"))
        .build()
        .encode()
        .unwrap();
    assert_eq!(
        &encoded[61..69],
        &[0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0]
    );
    let mut buf = encoded;
    assert_eq!(RecordBatch::decode(&mut buf).unwrap().records.len(), 1);
}

// ---------------------------------------------------------------------------
// A legal batch with more than 100 000 records decodes.
// ---------------------------------------------------------------------------

#[test]
fn n2_batch_with_more_than_100k_records_decodes() {
    let mut builder = RecordBatchBuilder::new();
    for _ in 0..100_001 {
        builder = builder.add_record(None::<Bytes>, Some(Bytes::new()));
    }
    let mut encoded = builder.build().encode().unwrap();
    // Under the broker's default max.message.bytes (1 048 588), uncompressed.
    assert!(
        encoded.len() < 1_048_588,
        "batch is a legal size for a default broker"
    );
    let result = RecordBatch::decode(&mut encoded);
    assert!(
        result.is_ok(),
        "a 100 001-record batch under max.message.bytes must decode, got {:?}",
        result.err()
    );
    assert_eq!(result.unwrap().records.len(), 100_001);
}

// ---------------------------------------------------------------------------
// The connection cap never blocks a replacement, and growth fails retriably.
// ---------------------------------------------------------------------------

/// Reverted-line control: keeping the stale entry in the pool and counting
/// it (no `by_key.remove(&key)` before the cap check, `by_key.len()` as the
/// count) makes the second call fail with the cap error.
#[tokio::test]
async fn n3_dead_connection_is_replaced_at_the_connection_cap() {
    let broker = FakeBroker::start().await.unwrap();
    let addr = broker.bootstrap_servers();
    let pool =
        Arc::new(ConnectionPool::new(ConnectionConfig::default()).with_max_total_connections(1));

    let first = pool.get_connection(&addr).await.unwrap();
    first.close().await;
    let deadline = Instant::now() + Duration::from_secs(5);
    while first.is_alive() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !first.is_alive(),
        "precondition: the first connection is closed"
    );

    // One broker, one connection: the cap is not exceeded by replacing it.
    let second = pool.get_connection(&addr).await;
    assert!(
        second.is_ok(),
        "replacing the only (dead) connection must not trip a cap of 1: {:?}",
        second.err()
    );
    assert!(!Arc::ptr_eq(&first, &second.unwrap()));
}

/// Negative control for the cap: growth beyond it is refused, retriably.
#[tokio::test]
async fn growth_beyond_the_cap_fails_retriably_naming_the_cap() {
    let cluster = FakeBroker::start_cluster(2).await.unwrap();
    let a = cluster.broker_addr(0).unwrap().to_string();
    let b = cluster.broker_addr(1).unwrap().to_string();
    let pool = ConnectionPool::new(ConnectionConfig::default()).with_max_total_connections(1);

    pool.get_connection(&a).await.unwrap();
    let err = expect_err(pool.get_connection(&b).await);
    assert!(matches!(err, KrafkaError::Network(_)), "got {err:?}");
    assert!(err.is_retriable());
    let msg = err.to_string();
    assert!(msg.contains("limit") && msg.contains(&b), "{msg}");
}

// ---------------------------------------------------------------------------
// A coordinator gets its own connection, and shares the data one at the cap.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_coordinator_gets_its_own_connection() {
    let broker = FakeBroker::start().await.unwrap();
    let addr = broker.bootstrap_servers();
    let pool = ConnectionPool::new(ConnectionConfig::default());

    let data = pool.get_connection(&addr).await.unwrap();
    let coordination = pool.get_coordinator_connection(&addr).await.unwrap();
    assert!(!Arc::ptr_eq(&data, &coordination), "a separate socket");
    assert!(Arc::ptr_eq(
        &coordination,
        &pool.get_coordinator_connection(&addr).await.unwrap()
    ));
    assert_eq!(pool.metrics().connections_created, 2);

    pool.close_all().await;
    let deadline = Instant::now() + Duration::from_secs(5);
    while (data.is_alive() || coordination.is_alive()) && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        !coordination.is_alive(),
        "close_all closes the coordination connection"
    );
    assert_eq!(pool.metrics().active_connections, 0);
}

#[tokio::test]
async fn at_the_cap_coordination_falls_back_to_the_data_connection() {
    let broker = FakeBroker::start().await.unwrap();
    let addr = broker.bootstrap_servers();
    let pool = ConnectionPool::new(ConnectionConfig::default()).with_max_total_connections(1);

    let data = pool.get_connection(&addr).await.unwrap();
    let coordination = pool.get_coordinator_connection(&addr).await.unwrap();
    assert!(
        Arc::ptr_eq(&data, &coordination),
        "shares the data connection"
    );
    pool.get_coordinator_connection(&addr).await.unwrap();
    assert_eq!(pool.metrics().coordination_fallbacks, 2);
    assert_eq!(pool.metrics().connections_created, 1);
}

// ---------------------------------------------------------------------------
// A throttle mutes the connection; a written request never times out on it.
// ---------------------------------------------------------------------------

fn config(connect: Duration, request: Duration) -> ConnectionConfig {
    ConnectionConfig::builder()
        .connect_timeout(connect)
        .request_timeout(request)
        .build()
        .unwrap()
}

/// The throttle (800 ms) is longer than the request budget (500 ms): the
/// request waits unwritten, is written after the mute, and succeeds.
#[tokio::test]
async fn n5_throttle_longer_than_budget_does_not_send_and_time_out() {
    let broker = FakeBroker::start().await.unwrap();
    let conn = BrokerConnection::connect(
        &broker.bootstrap_servers(),
        config(Duration::from_millis(500), Duration::from_millis(500)),
    )
    .await
    .unwrap();
    broker.clear_requests();

    conn.notify_throttle(800);
    let started = Instant::now();
    let result = conn.send_request(ApiKey::ApiVersions, 0, |_| Ok(())).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    let reached_broker = broker.request_count(ApiKey::ApiVersions);
    assert!(
        !(result.is_err() && reached_broker == 1),
        "the caller was told the request timed out, yet it was written to the broker"
    );
    assert!(
        result.is_ok(),
        "written after the mute, within its budget: {result:?}"
    );
    assert!(
        started.elapsed() >= Duration::from_millis(750),
        "waited for the mute"
    );
}

/// A caller deadline that passes during the mute fails the call, and the
/// broker never sees the request.
///
/// Reverted-line control: without the `is_abandoned` check in the event loop,
/// the request is written when the mute ends and the broker counts one.
#[tokio::test]
async fn a_caller_deadline_inside_the_mute_fails_unsent() {
    let broker = FakeBroker::start().await.unwrap();
    let conn = BrokerConnection::connect(
        &broker.bootstrap_servers(),
        config(Duration::from_millis(500), Duration::from_millis(500)),
    )
    .await
    .unwrap();
    broker.clear_requests();

    conn.notify_throttle(800);
    let result = tokio::time::timeout(
        Duration::from_millis(500),
        conn.send_request(ApiKey::ApiVersions, 0, |_| Ok(())),
    )
    .await;
    assert!(result.is_err(), "the caller's deadline fires");
    tokio::time::sleep(Duration::from_millis(600)).await; // past the mute
    assert_eq!(
        broker.request_count(ApiKey::ApiVersions),
        0,
        "never written"
    );

    // The connection is still usable afterwards.
    conn.send_request(ApiKey::ApiVersions, 0, |_| Ok(()))
        .await
        .unwrap();
}

/// Control: without a throttle, nothing waits.
#[tokio::test]
async fn without_a_throttle_nothing_waits() {
    let broker = FakeBroker::start().await.unwrap();
    let conn = BrokerConnection::connect(&broker.bootstrap_servers(), ConnectionConfig::default())
        .await
        .unwrap();
    let started = Instant::now();
    conn.send_request(ApiKey::ApiVersions, 0, |_| Ok(()))
        .await
        .unwrap();
    assert!(started.elapsed() < Duration::from_millis(200));
    assert_eq!(conn.throttle_remaining(), None);
}

// ---------------------------------------------------------------------------
// A request timeout closes the connection; the next request gets a new one.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_request_timeout_closes_the_connection_and_the_pool_replaces_it() {
    let broker = FakeBroker::start().await.unwrap();
    let addr = broker.bootstrap_servers();
    let pool = Arc::new(ConnectionPool::new(config(
        Duration::from_millis(300),
        Duration::from_millis(300),
    )));
    let conn = pool.get_connection(&addr).await.unwrap();
    // The next request is never answered, which blocks every response
    // behind it on this connection.
    broker.on_once(ApiKey::ApiVersions, |_| Control::Silence);

    let mut calls = Vec::new();
    for _ in 0..3 {
        let conn = conn.clone();
        calls.push(tokio::spawn(async move {
            conn.send_request(ApiKey::ApiVersions, 0, |_| Ok(())).await
        }));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let mut outcomes = Vec::new();
    for call in calls {
        outcomes.push(call.await.unwrap());
    }
    assert!(
        matches!(outcomes[0], Err(KrafkaError::Timeout { .. })),
        "{:?}",
        outcomes[0]
    );
    for other in &outcomes[1..] {
        let err = other.as_ref().unwrap_err();
        assert!(matches!(err, KrafkaError::Network(_)), "{err:?}");
        assert!(err.is_retriable());
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!conn.is_alive(), "closed at the first timeout");

    let next = pool.get_connection(&addr).await.unwrap();
    assert!(!Arc::ptr_eq(&conn, &next), "a new connection");
    next.send_request(ApiKey::ApiVersions, 0, |_| Ok(()))
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// One dial per call, bounded by connect_timeout and the caller's deadline;
// per-address backoff between calls.
// ---------------------------------------------------------------------------

/// A listener that accepts TCP and never answers; returns its address and a
/// count of accepted connections.
async fn silent_broker() -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let accepted = Arc::new(AtomicUsize::new(0));
    let count = accepted.clone();
    let task = tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            let (s, _) = listener.accept().await.unwrap();
            count.fetch_add(1, Ordering::SeqCst);
            held.push(s); // accept, never answer
        }
    });
    (addr, accepted, task)
}

#[tokio::test]
async fn n6_get_connection_to_a_silent_broker_is_bounded_by_one_attempt() {
    let (addr, accepted, task) = silent_broker().await;
    let pool = ConnectionPool::new(config(
        Duration::from_millis(300),
        Duration::from_millis(300),
    ));
    let start = Instant::now();
    let result = pool.get_connection(&addr).await;
    let elapsed = start.elapsed();
    task.abort();
    assert!(result.is_err());
    assert!(
        elapsed < Duration::from_millis(600),
        "one connection attempt is bounded by connect_timeout (300 ms); took {elapsed:?}"
    );
    assert_eq!(accepted.load(Ordering::SeqCst), 1, "exactly one dial");
}

#[tokio::test]
async fn the_callers_deadline_bounds_get_connection() {
    let (addr, _accepted, task) = silent_broker().await;
    let pool = ConnectionPool::new(config(Duration::from_secs(10), Duration::from_secs(10)));
    let start = Instant::now();
    let result = tokio::time::timeout(Duration::from_millis(100), pool.get_connection(&addr)).await;
    let elapsed = start.elapsed();
    task.abort();
    assert!(result.is_err(), "the caller's deadline fires");
    assert!(elapsed < Duration::from_millis(300), "{elapsed:?}");
}

/// After a failed dial, a call inside the backoff window fails at once
/// without dialing; after the window it dials again.
///
/// Reverted-line control: without the `reconnect` check in `start_dial`, the
/// second call dials and `accepted` reaches 2 before the window passes.
#[tokio::test]
async fn a_failed_dial_backs_off_per_address() {
    let (addr, accepted, task) = silent_broker().await;
    let pool = ConnectionPool::new(config(
        Duration::from_millis(200),
        Duration::from_millis(200),
    ));
    expect_err(pool.get_connection(&addr).await);
    assert_eq!(accepted.load(Ordering::SeqCst), 1);

    let start = Instant::now();
    let err = expect_err(pool.get_connection(&addr).await);
    assert!(start.elapsed() < Duration::from_millis(20), "fails fast");
    assert!(err.is_retriable(), "{err:?}");
    assert!(err.to_string().contains("not reconnecting"), "{err}");
    assert_eq!(
        accepted.load(Ordering::SeqCst),
        1,
        "no dial inside the window"
    );

    // First backoff is 50 ms (+/- 20 %).
    tokio::time::sleep(Duration::from_millis(100)).await;
    expect_err(pool.get_connection(&addr).await);
    assert_eq!(accepted.load(Ordering::SeqCst), 2, "dials after the window");
    task.abort();
}

/// Concurrent callers share one dial.
#[tokio::test]
async fn concurrent_callers_share_one_dial() {
    let broker = FakeBroker::start().await.unwrap();
    let addr = broker.bootstrap_servers();
    let pool = Arc::new(ConnectionPool::new(ConnectionConfig::default()));
    let mut calls = Vec::new();
    for _ in 0..8 {
        let pool = pool.clone();
        let addr = addr.clone();
        calls.push(tokio::spawn(
            async move { pool.get_connection(&addr).await },
        ));
    }
    let first = calls.remove(0).await.unwrap().unwrap();
    for call in calls {
        assert!(Arc::ptr_eq(&first, &call.await.unwrap().unwrap()));
    }
    assert_eq!(pool.metrics().connections_created, 1);
}
