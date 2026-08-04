#!/usr/bin/env python3
"""Check krafka's API version table against Apache Kafka's own message schemas.

# Why this exists

A review found krafka three Kafka releases behind in four APIs, and a feature
flag documented as gating `Fetch` v17/v18 that gated nothing — the encoders
existed, were tested, and were unreachable in every build configuration.

Both were "the world moved and the table did not". That is not a defect a
reviewer should be finding by hand once a year; it is a diff between two
machine-readable files, and this is the diff.

# What it checks

Against the vendored snapshot of `apache/kafka`'s message schemas:

1. **Name and key agree.** A protocol rename applied to one place and not the
   other (`ListClientMetricsResources` → `ListConfigResources` in Kafka 4.1) is
   caught here.
2. **MIN is still valid.** Kafka removes old versions in major releases; a MIN
   below Kafka's floor means every request is rejected.
3. **MAX does not overstate.** A version Kafka marks `latestVersionUnstable` is
   not advertised by a released broker. Claiming it in an *ungated* row is a
   promise the client cannot keep — and, if a future unstable version is not
   wire-identical to its predecessor, an actual bug against a test cluster
   started with `unstable.api.versions.enable=true`.
4. **MAX does not understate.** A stable Kafka version the client does not
   negotiate is a capability silently left on the table. This is the check that
   would have caught `Fetch` v17/v18 the day Kafka 4.0 shipped.
5. **Flexible-version boundary matches.** `ApiKey::flexible_version()` decides
   header v1-vs-v2 and compact-vs-standard encoding for every field. Off by one
   and the broker cannot parse the request at all.

# Hermetic by default

CI must not depend on GitHub being reachable, so the schema facts are vendored
in `kafka_protocol_snapshot.json` and this script reads them offline. Refresh
the snapshot deliberately:

    python3 xtask/protocol_parity.py --refresh --ref 4.3

which rewrites the snapshot; the resulting diff is then a reviewable commit that
says exactly what Kafka changed. Running the check afterwards reports what
krafka must do about it.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SNAPSHOT = Path(__file__).resolve().parent / "kafka_protocol_snapshot.json"

VERSIONS_RS = ROOT / "src/protocol/mod.rs"
API_RS = ROOT / "src/protocol/api.rs"

GITHUB_API = "https://api.github.com/repos/apache/kafka/contents/clients/src/main/resources/common/message"
GITHUB_RAW = "https://raw.githubusercontent.com/apache/kafka/{ref}/clients/src/main/resources/common/message/{name}"

# APIs krafka deliberately does not carry to Kafka's stable ceiling.
#
# Every entry needs a reason, because an unexplained entry here is how check 4
# stops working. `None` for the version means "not implemented at all".
DELIBERATE_GAPS: dict[str, str] = {
    # Broker- and controller-internal APIs. A client does not speak them; they
    # are present in `ApiKey` only so an `ApiVersions` response from a modern
    # broker decodes to something readable.
    "LeaderAndIsr": "broker-internal",
    "StopReplica": "broker-internal",
    "UpdateMetadata": "broker-internal",
    "ControlledShutdown": "broker-internal",
    "Vote": "KRaft-internal",
    "BeginQuorumEpoch": "KRaft-internal",
    "EndQuorumEpoch": "KRaft-internal",
    "AlterPartition": "broker-internal",
    "Envelope": "broker-internal",
    "FetchSnapshot": "KRaft-internal",
    "BrokerRegistration": "broker-internal",
    "BrokerHeartbeat": "broker-internal",
    "UnregisterBroker": "broker-internal",
    "AllocateProducerIds": "broker-internal",
    "ControllerRegistration": "controller-internal",
    "AssignReplicasToDirs": "broker-internal",
    "AddRaftVoter": "KRaft admin; not yet implemented",
    "RemoveRaftVoter": "KRaft admin; not yet implemented",
    "UpdateRaftVoter": "controller-internal",
    "InitializeShareGroupState": "share-group state persister; broker-internal",
    "ReadShareGroupState": "share-group state persister; broker-internal",
    "WriteShareGroupState": "share-group state persister; broker-internal",
    "DeleteShareGroupState": "share-group state persister; broker-internal",
    "ReadShareGroupStateSummary": "share-group state persister; broker-internal",
    # Only the heartbeat half remains a gap. Its request carries the Streams
    # application topology — subtopologies, repartition and changelog topics —
    # so a client without a Streams runtime cannot send a truthful one, and a
    # fabricated topology would corrupt the metadata every real member of the
    # group shares. `StreamsGroupDescribe` (key 89) is implemented: it is
    # observational and is what an operator actually needs.
    "StreamsGroupHeartbeat": (
        "KIP-1071 Streams *runtime* protocol; the request carries the application "
        "topology, which a client with no Streams layer cannot truthfully send"
    ),
    # Client APIs krafka implements, but not to Kafka's ceiling, on purpose.
    "AlterConfigs": (
        "superseded by IncrementalAlterConfigs, which krafka uses instead; "
        "the legacy whole-config replace is not exposed"
    ),
    "SaslHandshake": "pinned at v1 by the handshake path; v0 has no mechanism list",
    "SaslAuthenticate": (
        "pinned at v1: v2 only adds flexible encoding, and the pre-auth reader "
        "is deliberately version-pinned so an unauthenticated peer cannot steer it"
    ),
}


# ── Snapshot -----------------------------------------------------------------


def refresh_snapshot(ref: str) -> None:
    """Rewrite the vendored snapshot from `apache/kafka@<ref>`."""
    print(f"fetching schema listing for apache/kafka@{ref} …", file=sys.stderr)
    listing = json.loads(_get(f"{GITHUB_API}?ref={ref}"))
    names = sorted(
        entry["name"]
        for entry in listing
        if entry["name"].endswith("Request.json")
    )

    apis: dict[str, dict[str, object]] = {}
    for name in names:
        raw = _get(GITHUB_RAW.format(ref=ref, name=name)).decode("utf-8")
        schema = _parse_schema(raw)
        api_name = schema["name"].removesuffix("Request")
        apis[api_name] = {
            "key": schema["apiKey"],
            "min_version": schema["min_version"],
            "max_version": schema["max_version"],
            "latest_version_unstable": schema["latest_version_unstable"],
            "flexible_from": schema["flexible_from"],
        }

    SNAPSHOT.write_text(
        json.dumps({"kafka_ref": ref, "apis": apis}, indent=2, sort_keys=True) + "\n"
    )
    print(f"wrote {SNAPSHOT.relative_to(ROOT)} — {len(apis)} APIs", file=sys.stderr)


def _get(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "krafka-protocol-parity"})
    with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
        return response.read()


def _parse_schema(raw: str) -> dict[str, object]:
    """Extract the fields we need from a Kafka message schema.

    Kafka's schema files are JSON *with* `//` comments, which `json` rejects, so
    the few fields that matter are read with targeted patterns rather than by
    parsing the whole document. Every one of them is a flat scalar at the top
    level, so this is exact rather than approximate.
    """

    def scalar(field: str) -> str | None:
        match = re.search(rf'"{field}"\s*:\s*"?([^",\n]+)"?', raw)
        return match.group(1).strip() if match else None

    valid = scalar("validVersions") or ""
    if valid == "none":
        # An API Kafka has fully removed (e.g. LeaderAndIsr in 4.0).
        lo = hi = None
    elif "-" in valid:
        lo_s, hi_s = valid.split("-", 1)
        lo, hi = int(lo_s), int(hi_s)
    else:
        lo = hi = int(valid)

    flexible = scalar("flexibleVersions") or "none"
    flexible_from = None if flexible == "none" else int(flexible.rstrip("+"))

    name = scalar("name")
    api_key = scalar("apiKey")
    return {
        "name": name,
        "apiKey": int(api_key) if api_key is not None else -1,
        "min_version": lo,
        "max_version": hi,
        "latest_version_unstable": '"latestVersionUnstable": true' in raw,
        "flexible_from": flexible_from,
    }


# ── krafka's side ------------------------------------------------------------

# One `api_versions!` row:
#   "Name" [key] (cfg(...))? => NAME_MIN = n ..= NAME_MAX = m, "notes"
ROW = re.compile(
    r'"(?P<api>\w+)"\s*\[(?P<key>\d+)\]\s*'
    r'(?:cfg\((?P<cfg>[^)]*)\)\s*)?'
    r'=>\s*\w+\s*=\s*(?P<min>\d+)\s*\.\.=\s*\w+\s*=\s*(?P<max>\d+)\s*,'
)


def krafka_rows() -> list[dict[str, object]]:
    source = VERSIONS_RS.read_text()
    start = source.index("api_versions! {")
    body = source[start:]
    rows = []
    for match in ROW.finditer(body):
        cfg = match.group("cfg") or ""
        rows.append(
            {
                "api": match.group("api"),
                "key": int(match.group("key")),
                "min": int(match.group("min")),
                "max": int(match.group("max")),
                # A row gated on `feature = "unstable-protocol"` is allowed to
                # name a version Kafka has not stabilised. The `not(...)` row of
                # the same pair is the ungated one and is held to the stable
                # ceiling.
                "unstable_gated": 'feature = "unstable-protocol"' in cfg
                and "not(" not in cfg,
                # A row gated on any other feature is conditional but still held
                # to the stable ceiling.
                "gated": bool(cfg),
            }
        )
    if not rows:
        raise SystemExit("could not parse any rows from api_versions!")
    return rows


def krafka_flexible_versions() -> dict[str, int]:
    """`ApiKey::flexible_version()`'s arm values, keyed by variant name."""
    source = API_RS.read_text()
    start = source.index("pub fn flexible_version(self) -> i16 {")
    end = source.index("\n    }\n", start)
    body = source[start:end]

    out: dict[str, int] = {}
    for line in body.splitlines():
        # `Self::Produce => 9,` or a `|`-separated group ending in `=> 0,`
        match = re.search(r"=>\s*(i16::MAX|\d+)\s*,", line)
        if not match:
            continue
        value = 32767 if match.group(1) == "i16::MAX" else int(match.group(1))
        # Collect every `Self::Variant` mentioned in this arm and the `|` lines
        # immediately above it that have no `=>` of their own.
        for variant in re.findall(r"Self::(\w+)", line):
            out.setdefault(variant, value)
    # Multi-line `|` groups: walk again, remembering pending variants.
    pending: list[str] = []
    for line in body.splitlines():
        variants = re.findall(r"Self::(\w+)", line)
        match = re.search(r"=>\s*(i16::MAX|\d+)\s*,", line)
        if match:
            value = 32767 if match.group(1) == "i16::MAX" else int(match.group(1))
            for variant in pending + variants:
                out[variant] = value
            pending = []
        else:
            pending.extend(variants)
    return out


# ── Checks -------------------------------------------------------------------


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="re-fetch the vendored snapshot from GitHub instead of checking",
    )
    parser.add_argument(
        "--ref",
        default="4.3",
        help="apache/kafka git ref to snapshot (default: %(default)s)",
    )
    args = parser.parse_args()

    if args.refresh:
        refresh_snapshot(args.ref)
        return 0

    if not SNAPSHOT.exists():
        raise SystemExit(
            f"{SNAPSHOT} is missing; run `python3 xtask/protocol_parity.py --refresh`"
        )

    snapshot = json.loads(SNAPSHOT.read_text())
    kafka: dict[str, dict] = snapshot["apis"]
    kafka_ref = snapshot["kafka_ref"]

    errors: list[str] = []
    by_name = {row["api"]: row for row in krafka_rows()}

    # Rows come in pairs for APIs with an unstable ceiling; keep the widest.
    merged: dict[str, dict] = {}
    for row in krafka_rows():
        existing = merged.get(row["api"])
        if existing is None or row["max"] > existing["max"]:
            merged[row["api"]] = row
        if existing is not None:
            # Remember whether *any* row for this API is ungated, and what its
            # ceiling is — that is what check 3 holds to the stable maximum.
            pass
    ungated_max: dict[str, int] = {}
    for row in krafka_rows():
        if not row["unstable_gated"]:
            ungated_max[row["api"]] = max(ungated_max.get(row["api"], -1), row["max"])

    for api, row in merged.items():
        spec = kafka.get(api)
        if spec is None:
            errors.append(
                f"{api} (key {row['key']}) is in krafka's table but not in the "
                f"Kafka {kafka_ref} schemas — renamed or removed upstream?"
            )
            continue

        # 1. key agreement
        if spec["key"] != row["key"]:
            errors.append(
                f"{api}: krafka uses API key {row['key']}, Kafka {kafka_ref} says {spec['key']}"
            )

        if spec["max_version"] is None:
            errors.append(
                f"{api}: Kafka {kafka_ref} has removed this API entirely "
                f"(validVersions: none), but krafka still negotiates v{row['min']}..={row['max']}"
            )
            continue

        stable_max = spec["max_version"] - 1 if spec["latest_version_unstable"] else spec["max_version"]

        # 2. MIN is still valid
        if row["min"] < spec["min_version"]:
            errors.append(
                f"{api}: krafka MIN is v{row['min']} but Kafka {kafka_ref} removed "
                f"everything below v{spec['min_version']} — those requests are rejected outright"
            )

        # 3. MAX does not overstate (ungated rows only)
        ungated = ungated_max.get(api)
        if ungated is not None and ungated > stable_max:
            unstable_note = (
                f" (v{spec['max_version']} is marked latestVersionUnstable, so a released "
                f"broker does not advertise it)"
                if spec["latest_version_unstable"]
                else ""
            )
            errors.append(
                f"{api}: krafka advertises up to v{ungated} without gating it behind "
                f"`unstable-protocol`, but Kafka {kafka_ref}'s stable ceiling is "
                f"v{stable_max}{unstable_note}"
            )

        # 4. MAX does not understate
        if row["max"] < stable_max and api not in DELIBERATE_GAPS:
            errors.append(
                f"{api}: krafka stops at v{row['max']} but Kafka {kafka_ref} has "
                f"stable v{stable_max} — a released broker offers capability this "
                f"client will not negotiate"
            )

        # 5. flexible boundary
        flexible = krafka_flexible_versions().get(api)
        expected = spec["flexible_from"]
        if flexible is not None:
            expected_value = 32767 if expected is None else expected
            if flexible != expected_value:
                errors.append(
                    f"{api}: ApiKey::flexible_version() says v{flexible}, Kafka "
                    f"{kafka_ref} says {'never' if expected is None else 'v' + str(expected)}"
                    " — the header version and every compact field depend on this"
                )

    # APIs Kafka has that krafka's table does not mention at all.
    missing = sorted(
        name
        for name, spec in kafka.items()
        if name not in by_name
        and name not in DELIBERATE_GAPS
        and spec["max_version"] is not None
    )
    for name in missing:
        errors.append(
            f"{name} (key {kafka[name]['key']}) exists in Kafka {kafka_ref} but has no "
            f"row in krafka's api_versions! table, and is not listed in DELIBERATE_GAPS"
        )

    if errors:
        print(f"Protocol parity check FAILED against Kafka {kafka_ref}\n", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        print(
            "\nEither update the version table (and the codec arms behind it), or —\n"
            "for a deliberate omission — add the API to DELIBERATE_GAPS in this\n"
            "script with the reason. Refresh the snapshot with `--refresh --ref <tag>`\n"
            "when tracking a newer Kafka release.",
            file=sys.stderr,
        )
        return 1

    print(
        f"✓ Protocol parity: {len(merged)} APIs match Kafka {kafka_ref} "
        f"(+{len(DELIBERATE_GAPS)} deliberate omissions)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
