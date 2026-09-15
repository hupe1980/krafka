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

Every `pub` field of every response struct under `src/protocol/messages/` must
be *named* somewhere outside the protocol layer — in the consumer,
producer, admin, share-consumer or telemetry code — with test modules stripped,
so a field kept alive only by its own round-trip test does not count as read.

A response struct here means a `*Response*` struct **or any struct it contains**.
Nesting was the hole this check shipped with: `DescribeGroupMember::member_assignment`
held every classic consumer group's partition assignment, was decoded for every
DescribeGroups version, and was dropped on the floor by the admin mapping — one
level below where the check was looking.

Types the crate returns to callers verbatim are exempt, and listed in
PASSTHROUGH. For those the field is not dropped: it is the caller's to read, and
naming it in client code would prove nothing. The exemption is transitive, since
handing back a struct hands back everything inside it.

Fields that are legitimately decode-only are listed in ALLOW with a reason.
Keep both lists short and keep the reasons specific: "not needed yet" is how the
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

# struct name -> the public API that returns it to callers unchanged.
PASSTHROUGH = {
    # `AdminClient::describe_streams_groups` returns the decoded groups as they
    # came off the wire — topology, tasks, offsets and all. Nothing is mapped,
    # so nothing can be dropped.
    "DescribedStreamsGroup": "returned verbatim by AdminClient::describe_streams_groups",
}

MESSAGES = "src/protocol/messages"


def structs() -> dict[str, tuple[str, str]]:
    """Every `pub struct` under `src/protocol/messages/`: name -> (file, body)."""
    found: dict[str, tuple[str, str]] = {}
    for path in sorted((ROOT / MESSAGES).glob("*.rs")):
        source = path.read_text()
        for match in re.finditer(r"pub struct (\w+) \{(.*?)\n\}", source, re.S):
            found[match.group(1)] = (path.name, match.group(2))
    return found


def contained(roots: list[str], defs: dict[str, tuple[str, str]]) -> set[str]:
    """`roots` plus every struct reachable through their field types."""
    seen: set[str] = set()
    stack = list(roots)
    while stack:
        name = stack.pop()
        if name in seen or name not in defs:
            continue
        seen.add(name)
        for referenced in re.findall(r"\b([A-Z]\w+)\b", defs[name][1]):
            if referenced in defs and referenced not in seen:
                stack.append(referenced)
    return seen


def response_fields() -> dict[str, set[str]]:
    """`pub` fields of every response struct, mapped to where they appear.

    A response struct is a `*Response*` struct or anything one contains, minus
    the types the crate hands to callers whole.
    """
    defs = structs()
    reachable = contained([name for name in defs if "Response" in name], defs)
    passthrough = contained([name for name in PASSTHROUGH if name in defs], defs)

    found: dict[str, set[str]] = {}
    for struct in sorted(reachable - passthrough):
        file_name, body = defs[struct]
        for field in re.findall(r"^\s+pub (\w+):", body, re.M):
            found.setdefault(field, set()).add(f"{file_name}::{struct}")
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
    stale += sorted(name for name in PASSTHROUGH if name not in structs())
    if stale:
        print("Protocol reachability check FAILED\n", file=sys.stderr)
        for field in stale:
            print(
                f"  - `{field}` is named by ALLOW or PASSTHROUGH but is no longer "
                "a response field or struct.\n"
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
        f"every one read by client code ({len(ALLOW)} documented decode-only, "
        f"{len(PASSTHROUGH)} type(s) returned verbatim)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
