---
applyTo: "src/consumer/**"
description: "Use when editing consumer code: async lock discipline, rebalance cleanup paths, offset/high-watermark tracking, and cooperative rebalance correctness."
---

# Consumer Module Rules

## Lock & Async Discipline

- Never hold a write lock across an `.await` for another lock — split into write-then-drop, then reacquire as read
- Lock scope is minimal: insert data, drop lock, then compute
- Don't acquire `RwLock` write when only reads are needed afterward
- Lock ordering: `assignments` → `offsets` → `high_watermarks`. Verify new code doesn't invert this

## Cleanup Paths

Every per-partition cache must be handled in **all three** cleanup paths:

| Path | Method | Trigger |
|------|--------|---------|
| Full reset | `clear_partition_state()` | Eager rebalance, unsubscribe |
| Partial revocation | `apply_partition_revocations()` | Cooperative rebalance |
| Shutdown | `close()` | Consumer shutdown |

When adding a new `HashMap<(String, PartitionId), _>` field: add `.clear()` in the first, `.remove()` loop in the second, and verify the third calls the first.

## Offset Arithmetic

- `Offset = i64` but `Gauge` stores `u64` — always clamp: `(hw - pos).max(0) as u64`
- Use `saturating_add` when summing per-partition values into an aggregate
- `high_watermark` and `log_start_offset` from fetch responses are valid even on error partitions — always cache them

## Testing Constraints

`Consumer` requires a live broker and cannot be constructed in unit tests.
Test logic via:
- Extracted helper methods that take plain data structures
- Data-structure-level assertions (HashMap operations matching poll() logic)
- Use the exact types and arithmetic as production code (u64 + saturating_add, not i64 + sum)

## Rebalance Protocol

See `docs/consumer.md` for cooperative vs eager rebalance semantics.
Key invariant: `on_partitions_revoked` fires only for moved partitions in cooperative mode; `on_partitions_lost` fires for unexpectedly vanished ones (session timeout, fencing).

## Buffer Cap (`max_buffered_records`)

- `recv()` buffers excess records from `poll()` in `recv_buffer: RwLock<VecDeque<ConsumerRecord>>`
- When `max_buffered_records > 0` and the buffer reaches the limit, `poll()` skips fetching
- Auto-commit and rebalance handling still run — only the fetch is skipped
- The `buffered_records` gauge must be updated at **every** `recv_buffer` mutation: `pop_front`, `extend`, `clear`, and `retain`
- Set to `0` to disable (unlimited buffering)
- For single-caller `recv()` the buffer is naturally bounded by `max_poll_records`; the cap primarily guards mixed `poll()`/`recv()` and concurrent `recv()` callers
