---
applyTo: "src/admin.rs"
description: "Use when editing admin client: API version constraints, result error handling, and destructive operation awareness."
---

# Admin Client Rules

## Result Error Handling

Admin operations return **per-resource results** (e.g., `Vec<CreateTopicResult>`), each with an `error: Option<String>`.
Callers must check individual results — a successful RPC does not mean every resource succeeded.

## API Versions

Admin requests currently use **v0** of each API (lowest common denominator).
When bumping a version, update the corresponding encode/decode path in `src/protocol/` **and** verify the new fields here.

## Destructive Operations

- `alter_topic_config()` **replaces all dynamic configs** — always fetch-modify-update.
- `create_partitions()` can only **increase** count; never decreases.
- `delete_topics()` and `delete_acls()` are irreversible.

## Connection Strategy

Admin commands are sent to **any** broker (not necessarily the controller).
No built-in retry — callers must retry on retriable errors.
