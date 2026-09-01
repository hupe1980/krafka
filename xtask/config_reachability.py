#!/usr/bin/env python3
"""Every configuration field must be settable and readable from outside the crate.

# Why this exists

This crate's review history contains the same defect three times, in three
different modules: a configuration field that is declared, documented, wired
all the way to the wire protocol — and reachable from no public builder. It
looks finished from the inside. From the outside it is a constant.

  - `ConnectionConfig`'s eleven socket- and pool-level settings. Documented in
    detail; every client built its connection config from four fields and took
    the defaults for the rest. Fixed by `TransportConfig`.
  - `AccumulatorConfig::compression_level` and `dead_letter_queue`. Present on
    one of the producer's two send paths, silently ignored on the other.
  - `ShareConsumerConfig`'s `fetch_min_bytes`, `fetch_max_bytes`, `max_records`
    and `batch_size` — the four knobs KIP-932 exposes for tuning a share fetch.
    All four are read when the `ShareFetch` request is built. None of them had
    a builder setter, so every krafka share consumer in existence sent the same
    four numbers.

`tests/builder_surface.rs` proves that specific named methods exist, which is
the right tool for a cross-client invariant ("every builder takes a
`TransportConfig`"). It cannot prove the *absence* of a gap, because a field
nobody remembered to expose is also a field nobody remembered to add a line
for. That is what this checks: it starts from the fields, not from the methods.

# What it checks

For each config struct below, every private field must have

  1. a builder setter — `pub fn <field>(mut self, ..)` on the builder, and
  2. a public accessor — `pub fn <field>(&self)` on the config,

unless the field is listed in EXEMPT with a reason.

Accessors matter as much as setters: `build_config()` is documented as the way
to validate a configuration without a broker, which is only useful if the
result can be inspected. `ConsumerConfig` had 34 accessors and
`ShareConsumerConfig` had 6, while the README promised "every client shares one
configuration surface".

Naming is the whole mechanism, so it is enforced: the setter, the accessor and
the field must all share a name. That is already the convention everywhere
except one setter (`fetch_max_wait_ms`, since renamed), and it is what makes a
field-driven check possible at all.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# struct name -> (file defining it, files that may define its builder)
CONFIGS = {
    "ConsumerConfig": ("src/consumer/config.rs", ["src/consumer/builder.rs"]),
    "ProducerConfig": ("src/producer/config.rs", ["src/producer/mod.rs"]),
    "AdminConfig": ("src/admin/mod.rs", ["src/admin/builder.rs", "src/admin/mod.rs"]),
    "ShareConsumerConfig": (
        "src/share_consumer/config.rs",
        ["src/share_consumer/mod.rs"],
    ),
    "TransactionalProducerConfig": (
        "src/producer/transaction.rs",
        ["src/producer/transaction.rs"],
    ),
    # Transport-level configs. `ConnectionConfig` is where the fourth instance
    # of this defect was found: `send_buffer_size` / `recv_buffer_size` were
    # declared, had public accessors, and were applied to the real socket in
    # `happy_eyeballs.rs` via `socket2` — with no setter anywhere, so `SO_SNDBUF`
    # and `SO_RCVBUF` tuning was permanently off.
    "TransportConfig": ("src/network/transport.rs", ["src/network/transport.rs"]),
    "ConnectionConfig": ("src/network/connection.rs", ["src/network/connection.rs"]),
    "ConnectionRetryConfig": ("src/network/pool.rs", ["src/network/pool.rs"]),
}

# (struct, field, kind) -> reason. `kind` is "setter" or "accessor".
#
# Keep this list short and keep the reasons specific. An entry that says
# "not needed" is how the next uncallable field gets in.
EXEMPT = {
    # Composite façades: the setter takes the whole struct, and handing the
    # inner map/handle back out by reference would leak a type the caller
    # cannot usefully act on.
    ("ConsumerConfig", "transport", "accessor"): "TransportConfig is set as a unit; no per-field read-back",
    ("ProducerConfig", "transport", "accessor"): "TransportConfig is set as a unit; no per-field read-back",
    ("AdminConfig", "transport", "accessor"): "TransportConfig is set as a unit; no per-field read-back",
    ("ShareConsumerConfig", "transport", "accessor"): "TransportConfig is set as a unit; no per-field read-back",
    ("TransactionalProducerConfig", "transport", "accessor"): "TransportConfig is set as a unit; no per-field read-back",
    # Trait objects: an accessor would hand back an `Arc<dyn ..>` whose only
    # use is to call it, which the client already does.
    ("ProducerConfig", "dead_letter_queue", "accessor"): "Arc<dyn DeadLetterQueue> has no read-back use",
    ("TransactionalProducerConfig", "dead_letter_queue", "accessor"): "Arc<dyn DeadLetterQueue> has no read-back use",
    # Per-topic override maps: the setter adds one entry at a time, so the
    # field is not the unit the API is expressed in.
    ("ConsumerConfig", "topic_fetch_max_bytes", "accessor"): "populated one topic at a time by topic_fetch_max_bytes()",
    ("ProducerConfig", "topic_compression", "accessor"): "populated one topic at a time by topic_compression()",
    ("TransactionalProducerConfig", "topic_compression", "accessor"): "populated one topic at a time by topic_compression()",
    # Bulk seeds, meaningful only at build time.
    ("ConsumerConfig", "initial_offsets", "accessor"): "seed map consumed during assignment; position() is the read side",
    # Set through a differently named accessor.
    ("AdminConfig", "metadata_max_age", "accessor"): "AdminClient exposes it through metadata_max_age_duration()",
    # `ConnectionConfig`'s field names predate `TransportConfig`, which is the
    # façade every client builder actually takes. The setters carry the public
    # names; the fields keep the socket-level ones.
    ("ConnectionConfig", "send_buffer_size", "setter"): "set via socket_send_buffer()",
    ("ConnectionConfig", "recv_buffer_size", "setter"): "set via socket_receive_buffer()",
    ("ConnectionConfig", "tcp_keepalive", "accessor"): "read back as keepalive()",
    ("ConnectionConfig", "max_high_priority_bypasses_per_round", "accessor"): "event-loop tuning, surfaced on TransportConfig",
    # Derived at build time from the auth config, not set directly.
    ("ConnectionConfig", "tls_connector", "setter"): "built from auth/TLS config by init_tls()",
    ("ConnectionConfig", "tls_connector", "accessor"): "built from auth/TLS config by init_tls()",
    ("ConnectionConfig", "msk_iam_clock_offset_secs", "setter"): "learned from broker clock skew at handshake time",
    ("ConnectionConfig", "msk_iam_clock_offset_secs", "accessor"): "learned from broker clock skew at handshake time",
    # A nested settings struct with its own setters, flattened onto the builder.
    ("ConnectionRetryConfig", "backoff", "setter"): "flattened: initial_backoff/max_backoff/backoff_multiplier/jitter_factor",
    ("ConnectionRetryConfig", "backoff", "accessor"): "flattened: initial_backoff()/max_backoff()/backoff_multiplier()/jitter_factor()",
    # Internal ceilings on a retry loop, deliberately not part of the surface.
    ("ConsumerConfig", "max_cooperative_rebalance_rounds", "accessor"): "loop bound, not a tuning knob",
    ("ConsumerConfig", "lag_staleness_threshold", "accessor"): "reported through the lag metrics, not read back",
}


def config_fields(source: str, struct: str) -> list[str] | None:
    """Private field names of `struct`, in declaration order."""
    match = re.search(rf"pub struct {struct} \{{(.*?)\n\}}", source, re.S)
    if match is None:
        return None
    body = match.group(1)
    # `pub(crate) name:` and bare `name:` are both private outside the crate.
    # `pub name:` is already reachable and needs no setter.
    return re.findall(r"^\s+(?:pub\(crate\)\s+)?([a-z_][a-z0-9_]*):", body, re.M)


def main() -> int:
    failures: list[str] = []
    checked = 0

    for struct, (config_path, builder_paths) in CONFIGS.items():
        config_src = (ROOT / config_path).read_text()
        fields = config_fields(config_src, struct)
        if fields is None:
            failures.append(
                f"  - {struct} was not found in {config_path}. Update CONFIGS in\n"
                "    xtask/config_reachability.py, or this check silently stops "
                "covering it."
            )
            continue

        builder_src = "\n".join((ROOT / p).read_text() for p in builder_paths)
        setters = set(re.findall(r"pub fn (\w+)\(\s*mut self", builder_src, re.S))
        accessors = set(re.findall(r"pub fn (\w+)\(\s*&self", config_src, re.S))

        for field in fields:
            checked += 1
            for kind, present in (("setter", field in setters), ("accessor", field in accessors)):
                if present or (struct, field, kind) in EXEMPT:
                    continue
                where = builder_paths[0] if kind == "setter" else config_path
                shape = (
                    f"pub fn {field}(mut self, ..) -> Self"
                    if kind == "setter"
                    else f"pub fn {field}(&self) -> ..."
                )
                failures.append(
                    f"  - {struct}::{field} has no {kind}.\n"
                    f"    Add `{shape}` in {where}, or add an entry to EXEMPT in\n"
                    "    xtask/config_reachability.py explaining why the field is "
                    "deliberately\n    unreachable."
                )

    if failures:
        print("Configuration reachability check FAILED\n", file=sys.stderr)
        for failure in failures:
            print(failure + "\n", file=sys.stderr)
        return 1

    print(
        f"✓ Configuration reachability: {checked} fields across {len(CONFIGS)} "
        f"configs, every one settable and readable ({len(EXEMPT)} documented exceptions)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
