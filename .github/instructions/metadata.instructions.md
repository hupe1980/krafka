---
applyTo: "src/metadata.rs"
description: "Use when editing metadata: cache refresh coalescing, lock ordering, staleness semantics, and error filtering."
---

# Metadata Module Rules

## Cache Refresh Coalescing

- Concurrent refresh calls are coalesced via a `parking_lot::Mutex<RefreshCoalescingState>` subscriber list.
- The first caller claims the `Idle → InFlight` transition and becomes the refresher.
- Subsequent callers register a `oneshot::Sender` in the `InFlight` list and `await` its receiver; they never acquire the lock after registration.
- The `RefreshGuard` RAII type ensures that all subscribers are notified and the state is reset to `Idle` even if the refresher task is cancelled or panics.
- The `parking_lot::Mutex` is held for at most a few microseconds (just long enough to push/drain the subscriber list) and is **never** held across an `.await` point.
- If the refresher is cancelled, subscribers receive `Err(RecvError)` and retry as a new refresher via `Box::pin(self.refresh_for_topics(topics)).await`.
- After acquiring the refresher role, check the staleness/backoff guards (retry_backoff + max_age) before issuing any RPCs — subsequent callers that were coalesced get the refresher's result directly.

## Lock Ordering

1. `refresh_state` (`parking_lot::Mutex<RefreshCoalescingState>`) — held for microseconds only, never across await
2. `cache` (ArcSwap) — lock-free reads via `load()`, atomic writes via `store()`
3. `last_refresh_completed` / `metadata_attempt_start` (`parking_lot::Mutex<Option<...>>`) — held briefly for reads/writes

`ArcSwap` eliminates lock-ordering concerns: readers never block writers and
vice-versa. `refresh_state` gates the refresher role; the cache swap is a single
atomic pointer store at the end.

## Error Filtering

- Topics with **permanent** error codes (UnknownTopicOrPartition, TopicAuthorizationFailed, InvalidTopic, etc.) are **removed** from the cache.
- Topics with **transient** error codes (LeaderNotAvailable, RequestTimedOut, etc.) are **kept** as stale entries so callers don't lose visibility. Use `ErrorCode::is_retriable()` to distinguish.
- Partitions with error codes are **excluded** from their topic's partition list.
- `leader_epoch` of `-1` means unknown — treat accordingly.

## Full vs. Partial Refresh

- **Full refresh** (`refresh_for_topics(None)`): response is authoritative — brokers and topics are rebuilt from scratch. Deleted topics and decommissioned brokers are automatically purged.
- **Partial refresh** (`refresh_for_topics(Some(&[...]))`): response is delta-merged into the existing cache. Topics and brokers not in the request are preserved so that preserved topics cannot reference missing brokers.

## Staleness

- `needs_refresh()` checks `last_updated.elapsed() > max_age` — it does **not** trigger a refresh.
- Callers (consumer, producer) are responsible for refreshing when `needs_refresh()` returns true or when a `NotLeaderForPartition` error occurs.

## KIP-899 Rebootstrap

- `bootstrap_servers` is wrapped in `ArcSwap<Vec<String>>` — lock-free reads, atomic swap via `update_seed_brokers()`.
- `MetadataRecoveryStrategy::Rebootstrap` enables automatic recovery when metadata refresh fails.
- `metadata_attempt_start` (`std::sync::Mutex<Option<Instant>>`) tracks the failure streak start:
  - Set via `get_or_insert_with(Instant::now)` at the **start** of every `refresh_for_topics` call.
  - Cleared to `None` on successful refresh.
  - Set to `Some(Instant::now())` after rebootstrap (matches Java's `metadataAttemptStartMs = Optional.of(now)`) so the next cycle starts timing immediately.
- `needs_rebootstrap()` is a pure predicate — returns `bool`, no side effects. The caller (`refresh_for_topics`) awaits `rebootstrap()` if it returns `true`.
- `rebootstrap()` is the async public method — awaits `pool.close_all()`, resets cache, sets timer to now.
- `refresh_for_topics` handles both client-initiated rebootstrap (connection failure + trigger exceeded) and server-initiated rebootstrap (`ErrorCode::RebootstrapRequired` = 124, KIP-899) via a loop with a single-retry guard.
- `update_seed_brokers()` only swaps the address list; it does not close connections or trigger a refresh.
