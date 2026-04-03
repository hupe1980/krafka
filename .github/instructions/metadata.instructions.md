---
applyTo: "src/metadata.rs"
description: "Use when editing metadata: cache refresh coalescing, lock ordering, staleness semantics, and error filtering."
---

# Metadata Module Rules

## Cache Refresh Coalescing

- A `Mutex` (`refresh_lock`) serializes concurrent refresh requests.
- After acquiring the lock, check if cache was updated within 100 ms by another task — if so, skip **only** when all requested topics are already present in the cache. A partial refresh for a missing topic must never be skipped.
- Never remove this check; it prevents thundering-herd on stale cache.

## Lock Ordering

1. `refresh_lock` (Mutex) — held only during the refresh RPC
2. `cache` (ArcSwap) — lock-free reads via `load()`, atomic writes via `store()`

`ArcSwap` eliminates lock-ordering concerns: readers never block writers and
vice-versa. `refresh_lock` serializes RPC calls; the cache swap is a single
atomic pointer store at the end.

## Error Filtering

- Topics with error codes are **not** inserted into the cache (deleted/unauthorized topics filtered out).
- On partial refresh, error topics are **removed** from the cache (may have been deleted).
- Partitions with error codes are **excluded** from their topic's partition list.
- `leader_epoch` of `-1` means unknown — treat accordingly.

## Full vs. Partial Refresh

- **Full refresh** (`refresh_for_topics(None)`): response is authoritative — brokers and topics are rebuilt from scratch. Deleted topics and decommissioned brokers are automatically purged.
- **Partial refresh** (`refresh_for_topics(Some(&[...]))`): response is delta-merged into the existing cache. Topics and brokers not in the request are preserved so that preserved topics cannot reference missing brokers.

## Staleness

- `needs_refresh()` checks `last_updated.elapsed() > max_age` — it does **not** trigger a refresh.
- Callers (consumer, producer) are responsible for refreshing when `needs_refresh()` returns true or when a `NotLeaderForPartition` error occurs.
