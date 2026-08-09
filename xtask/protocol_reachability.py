#!/usr/bin/env python3
"""A protocol field the client decodes must be a field the client reads.

# Why this exists

`xtask/config_reachability.py` catches a *configuration* field nobody can set.
This catches its mirror image on the wire: a *response* field the codec decodes
correctly, tests round-trip, and no client code ever looks at.

The two most severe defects in this project's review history were both this
shape, and neither the type system nor any existing check could see them,
because a decoded-and-ignored field is indistinguishable from a
decoded-and-used one until you go looking:

  - `FetchResponsePartition::last_stable_offset` was decoded for every Fetch
    version from v4 up, asserted in the codec's own tests, and read by not one
    line of consumer code. Every lag and progress computation used the high
    watermark instead, so a `read_committed` consumer reported permanent
    phantom lag and `is_caught_up()` could never return `true`.
  - `ShareFetchResponse::acquisition_lock_timeout_ms` (KIP-1222) was decoded
    and dropped. `AcknowledgeType::Renew` exists to extend that lock and is
    documented as the tool for long-running processing — while the deadline it
    extends is a broker-side setting the application had no way to learn.

Both look finished from the codec's side. From the client's side the
information simply never arrives.

# What it checks

Every `pub` field of every `*Response*` struct under `src/protocol/messages/`
must be *named* somewhere outside the protocol layer — in the consumer,
producer, admin, share-consumer or telemetry code — with test modules stripped,
so a field kept alive only by its own round-trip test does not count as read.

Fields that are legitimately decode-only are listed in ALLOW with a reason.
Keep that list short and keep the reasons specific: "not needed yet" is how the
next `last_stable_offset` gets in.

# What it deliberately does not check

Request fields encoded from a constant. `require_stable: false` was exactly
that defect, but the honest cases vastly outnumber the dishonest ones — a
consumer's `replica_id: -1` and a non-transactional producer's
`transactional_id: None` are correct constants, and an allowlist covering them
would be longer than the check. That class still needs a human.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# field name -> reason it is decoded but never read by client code.
ALLOW = {
    # Echoes of a request field. The client already knows what it asked for,
    # and the broker is required to reflect it, so reading it back proves
    # nothing the client did not already have.
    "allow_replication_factor_change": "echo of the AlterPartitionReassignments request field",
    # KIP-919: distinguishes a broker endpoint from a controller endpoint in
    # DescribeCluster. krafka never issues cluster-metadata requests against
    # the controller quorum directly — every admin operation is routed to the
    # controller by node id, not by endpoint type — so the discriminator has
    # nothing to select between. Revisit if direct quorum access is ever added.
    "endpoint_type": "krafka routes to the controller by node id, never by endpoint type",
}

MESSAGES = "src/protocol/messages"


def response_fields() -> dict[str, set[str]]:
    """`pub` fields of every `*Response*` struct, mapped to where they appear."""
    found: dict[str, set[str]] = {}
    for path in sorted((ROOT / MESSAGES).glob("*.rs")):
        source = path.read_text()
        for match in re.finditer(r"pub struct (\w*Response\w*) \{(.*?)\n\}", source, re.S):
            struct, body = match.group(1), match.group(2)
            for field in re.findall(r"^\s+pub (\w+):", body, re.M):
                found.setdefault(field, set()).add(f"{path.name}::{struct}")
    return found


def client_source() -> str:
    """Every non-test line outside the protocol layer and the test broker.

    The fake broker is excluded on purpose: it *serves* these fields, so a
    field only it touches is still one no client reads.
    """
    parts = []
    for path in sorted((ROOT / "src").rglob("*.rs")):
        rel = path.relative_to(ROOT)
        if "protocol" in rel.parts or "testing" in rel.parts:
            continue
        source = path.read_text()
        cut = source.find("\n#[cfg(test)]")
        if cut > 0:
            source = source[:cut]
        parts.append(source)
    return "\n".join(parts)


def main() -> int:
    fields = response_fields()
    client = client_source()

    unread = [
        (field, sites)
        for field, sites in sorted(fields.items())
        if field not in ALLOW and not re.search(r"\b" + re.escape(field) + r"\b", client)
    ]

    stale = sorted(set(ALLOW) - set(fields))
    if stale:
        print("Protocol reachability check FAILED\n", file=sys.stderr)
        for field in stale:
            print(
                f"  - ALLOW names `{field}`, which is no longer a response field.\n"
                "    Remove the entry from xtask/protocol_reachability.py.\n",
                file=sys.stderr,
            )
        return 1

    if unread:
        print("Protocol reachability check FAILED\n", file=sys.stderr)
        for field, sites in unread:
            where = ", ".join(sorted(sites))
            print(
                f"  - `{field}` is decoded by {where} and read by no client code.\n"
                "    Either use it, or add it to ALLOW in "
                "xtask/protocol_reachability.py with the\n    reason it is "
                "decode-only. A field the broker sends and the client throws away\n"
                "    is information the application cannot get any other way.\n",
                file=sys.stderr,
            )
        return 1

    print(
        f"✓ Protocol reachability: {len(fields)} response fields, "
        f"every one read by client code ({len(ALLOW)} documented decode-only)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
