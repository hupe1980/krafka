"""Every configuration field must be settable from outside the crate.

# Why this exists

A configuration field that is declared, documented and wired all the way to
the wire protocol, but reachable from no public builder, looks finished from
the inside and is a constant from the outside.

# What it checks

Connection settings live on `KafkaBuilder`; role settings on the role
builders (`ProducerBuilder`, `ConsumerBuilder`, `ShareConsumerBuilder`) and on
`AdminClient`. For each struct below, every field must have a setter of the
same name — `pub fn <field>(mut self, ..)` — in one of the listed files,
unless the field is listed in EXEMPT with a reason. `TransportConfig`'s fields
are flattened onto `KafkaBuilder`, so they are checked against it.

`tests/builder_surface.rs` proves named methods exist; only a field-driven
check can prove nothing was forgotten.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# struct name -> (file defining it, files that may define its setters)
CONFIGS = {
    "KafkaBuilder": ("src/client.rs", ["src/client.rs"]),
    "TransportConfig": ("src/network/transport.rs", ["src/client.rs"]),
    "ProducerConfig": ("src/producer/config.rs", ["src/producer/mod.rs"]),
    "ConsumerConfig": ("src/consumer/config.rs", ["src/consumer/builder.rs"]),
    "ShareConsumerConfig": (
        "src/share_consumer/config.rs",
        ["src/share_consumer/builder.rs"],
    ),
    "AdminConfig": ("src/admin/mod.rs", ["src/admin/mod.rs"]),
    # The connection layer's own builder: every field the pool needs must be
    # settable on it, or the `KafkaBuilder` setter has nothing to reach.
    "ConnectionConfig": ("src/network/connection.rs", ["src/network/connection.rs"]),
}

# (struct, field) -> reason.
#
# Keep this list short and keep the reasons specific. An entry that says
# "not needed" is how the next uncallable field gets in.
EXEMPT = {
    ("KafkaBuilder", "bootstrap_servers"): "the argument of Kafka::builder(..)",
    ("KafkaBuilder", "transport"): "flattened: TransportConfig's fields are KafkaBuilder setters",
    ("ConsumerConfig", "group_id"): "the argument of Kafka::consumer(..)",
    ("ConsumerConfig", "partition_assignment_strategies"): "set by partition_assignment_strategies()/partition_assignment_strategy()",
    ("ShareConsumerConfig", "group_id"): "the argument of Kafka::share_consumer(..)",
    ("ShareConsumerConfig", "request_timeout"): "copied from the Kafka handle",
    ("ConnectionConfig", "send_buffer_size"): "set via socket_send_buffer()",
    ("ConnectionConfig", "recv_buffer_size"): "set via socket_receive_buffer()",
    ("ConnectionConfig", "tls_connector"): "built from auth/TLS config by init_tls()",
    ("ConnectionConfig", "msk_iam_clock_offset_secs"): "learned from broker clock skew at handshake time",
    ("KafkaBuilder", "connector"): "test seam (test-broker): installed by testing::FakeBroker::kafka(), not a setting",
    ("ConnectionConfig", "connector"): "test seam (test-broker): copied from KafkaBuilder, not a setting",
    ("ConnectionConfig", "connection_metrics"): "each pool owns its recorder; read through the clients' metrics()",
}


def config_fields(source: str, struct: str) -> list[str] | None:
    """Private field names of `struct`, in declaration order."""
    match = re.search(rf"pub(?:\(crate\))? struct {struct} \{{(.*?)\n\}}", source, re.S)
    if match is None:
        return None
    body = match.group(1)
    # `pub(crate) name:` and bare `name:` are both private outside the crate.
    # `pub name:` is already reachable and needs no setter.
    return re.findall(r"^\s+(?:pub\(crate\)\s+)?([a-z_][a-z0-9_]*):", body, re.M)


def main() -> int:
    failures: list[str] = []
    checked = 0

    for struct, (config_path, setter_paths) in CONFIGS.items():
        config_src = (ROOT / config_path).read_text()
        fields = config_fields(config_src, struct)
        if fields is None:
            failures.append(
                f"  - {struct} was not found in {config_path}. Update CONFIGS in\n"
                "    xtask/config_reachability.py, or this check silently stops "
                "covering it."
            )
            continue

        setter_src = "\n".join((ROOT / p).read_text() for p in setter_paths)
        setters = set(re.findall(r"pub fn (\w+)(?:<[^>]*>)?\(\s*mut self", setter_src, re.S))

        for field in fields:
            checked += 1
            if field in setters or (struct, field) in EXEMPT:
                continue
            failures.append(
                f"  - {struct}::{field} has no setter.\n"
                f"    Add `pub fn {field}(mut self, ..) -> Self` in {setter_paths[0]}, "
                "or an entry to\n    EXEMPT in xtask/config_reachability.py explaining "
                "why the field is\n    deliberately unreachable."
            )

    if failures:
        print("Configuration reachability check FAILED\n", file=sys.stderr)
        for failure in failures:
            print(failure + "\n", file=sys.stderr)
        return 1

    print(
        f"✓ Configuration reachability: {checked} fields across {len(CONFIGS)} "
        f"configs, every one settable ({len(EXEMPT)} documented exceptions)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
