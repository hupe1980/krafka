//! Heap allocations per record on the send path, end to end, measured with a
//! counting global allocator that only counts the current thread.
//!
//! Everything — caller, send engine, fake broker — runs on one current-thread
//! runtime, so the count covers the whole path of a streaming caller: enqueue,
//! batching, the produce round trip and completion.
//!
//! Run: `cargo test --features test-broker --test send_allocations -- --nocapture`
#![cfg(feature = "test-broker")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::VecDeque;

use krafka::producer::{Producer, Record};
use krafka::testing::FakeBroker;

struct Counting;

thread_local! {
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
}

fn count() {
    // `try_with`: the allocator may run during thread teardown.
    let _ = ALLOCS.try_with(|a| a.set(a.get() + 1));
}

// SAFETY: every method forwards to `System` with the caller's arguments; the
// counting touches only a thread-local `Cell` and never allocates.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        count();
        // SAFETY: forwarded unchanged from the caller, who upholds `GlobalAlloc`'s contract.
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        // SAFETY: forwarded unchanged from the caller, who upholds `GlobalAlloc`'s contract.
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        count();
        // SAFETY: forwarded unchanged from the caller, who upholds `GlobalAlloc`'s contract.
        unsafe { System.realloc(p, l, n) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

const VALUE: &[u8] = br#"{"op":"c","table":"orders","id":123456,"amount":"129.95"}"#;

/// Records sent while counting.
const RECORDS: usize = 10_000;
/// Outstanding handles before the oldest is awaited.
const WINDOW: usize = 256;

async fn stream(producer: &Producer, keys: &[Vec<u8>]) {
    let mut window = VecDeque::with_capacity(WINDOW);
    for key in keys {
        if window.len() == WINDOW {
            let handle: krafka::producer::DeliveryHandle = window.pop_front().unwrap();
            let _ = handle.await.unwrap();
        }
        let record = Record::new("alloc", VALUE).key(key.clone());
        window.push_back(producer.enqueue(record).await.unwrap());
    }
    for handle in window {
        let _ = handle.await.unwrap();
    }
}

/// Allocations per send for a streaming caller. The keys are built before
/// counting starts, so the figure is the client's, not the caller's — apart
/// from the one `Vec` clone per key handed to `Record::key`.
fn allocations_per_send() -> f64 {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let broker = FakeBroker::start().await.unwrap();
        broker.create_topic("alloc", 4);
        let producer = krafka::Kafka::builder(broker.bootstrap_servers())
            .connect()
            .await
            .unwrap()
            .producer()
            .build()
            .await
            .unwrap();
        let warm: Vec<Vec<u8>> = (0..1_000).map(|i| format!("w{i}").into_bytes()).collect();
        stream(&producer, &warm).await;
        producer.flush().await.unwrap();

        let keys: Vec<Vec<u8>> = (0..RECORDS).map(|i| format!("k{i}").into_bytes()).collect();
        let before = ALLOCS.with(Cell::get);
        stream(&producer, &keys).await;
        producer.flush().await.unwrap();
        let allocs = ALLOCS.with(Cell::get) - before;
        producer.close().await.unwrap();
        allocs as f64 / RECORDS as f64
    })
}

/// Allocations per streamed send, end to end, on 0.31.1 (`04cf388`): the
/// topic name was copied into every acknowledgement and every send allocated
/// its own topic handle.
const BEFORE: f64 = 6.47;

/// The ceiling: 4.47 measured, rounded up to the next quarter. Of those, two
/// are the caller's (the key copy and the `Record`'s topic `String`) and one is
/// the handle's reply channel.
const CEILING: f64 = 4.5;

#[test]
fn a_streamed_send_allocates_within_its_budget() {
    let per_send = allocations_per_send();
    eprintln!("send path, {RECORDS} streamed records: {per_send:.2} allocations/record");
    assert!(
        per_send <= CEILING,
        "{per_send:.2} allocations/record, over the ceiling of {CEILING}"
    );
    assert!(
        BEFORE - per_send > 1.99,
        "the two per-send topic allocations are back"
    );
}
