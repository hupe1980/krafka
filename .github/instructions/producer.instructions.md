---
applyTo: "src/producer/**"
description: "Use when editing producer code: batching/accumulator flow, memory backpressure, retry semantics, close/flush ordering, and transactional state machine."
---

# Producer Module Rules

## Batching & Accumulator

- **linger > 0** → records routed through background `RecordAccumulator` task (mpsc channel, not mutex)
- **linger == 0** → direct send path, accumulator bypassed
- One batch per (topic, partition) at a time in the accumulator
- Linger timer fires at `max(1ms, linger/10)` intervals; actual delay ≈ linger + one tick

When touching batch or accumulator logic, verify both the **direct-send** and **accumulator** code paths.

## Memory Backpressure

Total memory = `memory_used` (buffered) + `in_flight_memory` (extracted, being sent).
Always check **both** components against `buffer_memory` limit.

- `BufferFull` returns the record **by move** (zero-copy); caller retries with same instance
- `InFlightGuard` (RAII) frees `in_flight_memory` on drop and calls `memory_freed.notify_waiters()`
- Callers pre-register `memory_freed.notified()` **before** sending append to avoid missed wakes

## Retry & Error Handling

- Exponential backoff with jitter (`RetryContext`); check `error.is_retriable()` before retrying
- Metadata refresh is **awaited** on retriable errors before retry
- `acks=0` uses `send_fire_and_forget()` — returns offset = -1, no delivery confirmation
- Record batch is encoded **once** and reused across retries (timestamp frozen at first encode)

## Close / Flush Ordering

```
close()
  0. closed.swap(true)       ← idempotent guard; second call returns early
  1. accumulator.shutdown()   ← flushes all pending batches, blocks until done
  2. interceptor close hook
  3. pool.close_all()         ← tears down broker connections
```

Adding state to `Producer` → verify it is drained or cleaned up in steps 1–4.

## Transactional Producer

- State machine: `Uninitialized → Ready → InTransaction → Committing/Aborting → Ready | FatalError`
- Transitions via CAS on `AtomicU8`; `FatalError` is a hard-set (no CAS)
- Sequence numbers wrap at `i32::MAX` → 0; guarantees hold only within one PID epoch
- `PendingAddGuard` prevents race in `AddPartitionsToTxn`; don't drop it before the RPC completes

## Partitioner Atomics

`StickyPartitioner::next_partition` uses `fetch_add` (not load+store) to avoid races under concurrent sends.

## Concurrency

- `max_in_flight` semaphore is **per-producer**, not per-partition — out-of-order delivery possible when > 1
- Batch sends are concurrent via `JoinSet`; within-partition ordering relies on single-batch-per-partition invariant
