//! Allocation counts on the consume path, measured with a counting global
//! allocator that only counts the current thread.
//!
//! - Decoding 1000 uncompressed 100-byte records allocates fewer than one
//!   block per record, and a decoded value is a view into the fetched buffer.
//! - Reading a 16 MiB response frame reserves the frame once.
//!
//! Run: `cargo test --test record_decode_allocations -- --nocapture`
#![cfg(feature = "internal")]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, unsafe_code)]

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use bytes::Bytes;
use krafka::__private::protocol::{Decoder, RecordBatch, RecordBatchBuilder};

struct Counting;

thread_local! {
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
    static BYTES: Cell<usize> = const { Cell::new(0) };
    static LARGE: Cell<usize> = const { Cell::new(0) };
}

/// Allocations at least this large are counted separately: the frame-sized
/// ones.
const LARGE_ALLOC: usize = 1024 * 1024;

fn count(size: usize) {
    // `try_with`: the allocator may run during thread teardown.
    let _ = ALLOCS.try_with(|a| a.set(a.get() + 1));
    let _ = BYTES.try_with(|b| b.set(b.get() + size));
    if size >= LARGE_ALLOC {
        let _ = LARGE.try_with(|l| l.set(l.get() + 1));
    }
}

// SAFETY: every method forwards to `System` with the caller's arguments; the
// counting touches only thread-local `Cell`s and never allocates.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        count(l.size());
        // SAFETY: forwarded unchanged from the caller, who upholds `GlobalAlloc`'s contract.
        unsafe { System.alloc(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        // SAFETY: forwarded unchanged from the caller, who upholds `GlobalAlloc`'s contract.
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        count(n);
        // SAFETY: forwarded unchanged from the caller, who upholds `GlobalAlloc`'s contract.
        unsafe { System.realloc(p, l, n) }
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Run `f` and return its output with the allocations and bytes it requested
/// on this thread.
fn measure<T>(f: impl FnOnce() -> T) -> (T, usize, usize) {
    let a0 = ALLOCS.with(Cell::get);
    let b0 = BYTES.with(Cell::get);
    let out = f();
    (out, ALLOCS.with(Cell::get) - a0, BYTES.with(Cell::get) - b0)
}

#[test]
fn uncompressed_decode_allocates_less_than_once_per_record_and_shares_the_buffer() {
    const N: usize = 1000;
    let mut b = RecordBatchBuilder::new();
    for i in 0..N {
        b = b.add_record(Some(format!("key-{i}")), Some(vec![b'v'; 100]));
    }
    let encoded: Bytes = b.build().encode().unwrap();
    let wire_len = encoded.len();

    let (batch, allocs, bytes) = measure(|| {
        let mut buf = encoded.clone();
        RecordBatch::decode(&mut buf).unwrap()
    });
    assert_eq!(batch.records.len(), N);
    eprintln!(
        "RecordBatch::decode, {N} uncompressed records ({wire_len} B): \
         {allocs} allocations ({:.2}/record), {bytes} B allocated ({:.2}x wire size)",
        allocs as f64 / N as f64,
        bytes as f64 / wire_len as f64
    );
    assert!(
        (allocs as f64 / N as f64) < 1.0,
        "{allocs} allocations for {N} records"
    );

    let base = encoded.as_ptr() as usize;
    let v = batch.records[0].value.as_ref().unwrap().as_ptr() as usize;
    assert!(
        v >= base && v < base + encoded.len(),
        "a decoded value is a view into the fetched buffer"
    );
}

#[test]
fn a_large_frame_is_reserved_once() {
    const FRAME: usize = 16 * 1024 * 1024;
    let mut wire = Vec::with_capacity(FRAME + 4);
    wire.extend_from_slice(&(FRAME as i32).to_be_bytes());
    wire.resize(FRAME + 4, 7u8);

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    let large_before = LARGE.with(Cell::get);
    let (frame, allocs, bytes) = measure(|| {
        runtime.block_on(async {
            let mut decoder = Decoder::with_max_size(100 * 1024 * 1024);
            // 64 KiB per read, like a socket.
            let mut reader = tokio::io::BufReader::with_capacity(64 * 1024, &wire[..]);
            decoder.read_frame(&mut reader).await.unwrap().unwrap()
        })
    });
    assert_eq!(frame.len(), FRAME);
    eprintln!(
        "Decoder::read_frame, one {} MiB frame: {allocs} (re)allocations, \
         {bytes} B requested ({:.2}x frame size)",
        FRAME / (1024 * 1024),
        bytes as f64 / FRAME as f64
    );
    let large = LARGE.with(Cell::get) - large_before;
    assert_eq!(large, 1, "one frame-sized allocation, no doubling");
    assert!(
        bytes < FRAME + FRAME / 4,
        "{bytes} B requested for a {FRAME} B frame"
    );
}
