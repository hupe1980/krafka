---
applyTo: "src/metadata.rs"
description: "Use when editing metadata: cache refresh coalescing, lock ordering, staleness semantics, and error filtering."
---

# Metadata Module Rules

## Cache Refresh Coalescing

- A `Mutex` (`refresh_lock`) serializes concurrent refresh requests.
- After acquiring the lock, check if cache was updated within 100 ms by another task — if so, skip the redundant request.
- Never remove this check; it prevents thundering-herd on stale cache.

## Lock Ordering

1. `refresh_lock` (Mutex) — held only during the refresh RPC
2. `cache` (ArcSwap) — lock-free reads via `load()`, atomic writes via `store()`

`ArcSwap` eliminates lock-ordering concerns: readers never block writers and
vice-versa. `refresh_lock` serializes RPC calls; the cache swap is a single
atomic pointer store at the end.

## Error Filtering

- Topics with error codes are **not** inserted into the cache (deleted/unauthorized topics filtered out).
- Partitions with error codes are **excluded** from their topic's partition list.
- `leader_epoch` of `-1` means unknown — treat accordingly.

## Staleness

- `needs_refresh()` checks `last_updated.elapsed() > max_age` — it does **not** trigger a refresh.
- Callers (consumer, producer) are responsible for refreshing when `needs_refresh()` returns true or when a `NotLeaderForPartition` error occurs.
