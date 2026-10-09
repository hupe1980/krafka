#!/usr/bin/env python3
"""krafka's normal dependency graph contains no OpenTelemetry crate.

krafka emits spans through `tracing` and leaves the bridge and the SDK to the
application. An `opentelemetry*` or `tracing-opentelemetry` crate in krafka's
own graph would pin a version of a fast-moving stack on every user.

The graph is resolved with `cargo tree --offline --locked` (normal and build
edges, no dev-dependencies) under the default features and under
`--all-features`, so no feature can bring one in.

Run: python3 xtask/no_otel.py
     python3 xtask/no_otel.py --self-test   # the planted-violation control
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def banned(crate: str) -> bool:
    return crate.startswith("opentelemetry") or crate == "tracing-opentelemetry"


def violations(lines: list[str]) -> list[str]:
    """Crate names (first word of each `cargo tree` line) that are banned."""
    found = set()
    for line in lines:
        name = line.strip().split(" ")[0] if line.strip() else ""
        if banned(name):
            found.add(name)
    return sorted(found)


def graph(features: list[str]) -> list[str]:
    out = subprocess.run(
        ["cargo", "tree", "--offline", "--locked", "--color", "never", "-e", "normal,build",
         "--prefix", "none", "--format", "{p}", *features],
        cwd=ROOT, check=True, capture_output=True, text=True,
    ).stdout
    return out.splitlines()


def self_test() -> int:
    planted = ["krafka v0.26.0", "opentelemetry v0.31.0", "tracing-opentelemetry v0.32.0", "tracing v0.1.41"]
    found = violations(planted)
    if found != ["opentelemetry", "tracing-opentelemetry"]:
        print(f"self-test FAILED: planted crates not caught, got {found}", file=sys.stderr)
        return 1
    if violations(["tracing v0.1.41", "tracing-subscriber v0.3.20"]):
        print("self-test FAILED: a clean graph was flagged", file=sys.stderr)
        return 1
    print("✓ no-otel self-test: planted crates caught, clean graph passes")
    return 0


def main() -> int:
    if "--self-test" in sys.argv:
        return self_test()
    failed = False
    for features in ([], ["--all-features"]):
        found = violations(graph(features))
        if found:
            label = " ".join(features) or "default features"
            print(f"✗ OpenTelemetry crates in krafka's graph ({label}): {', '.join(found)}", file=sys.stderr)
            failed = True
    if failed:
        print("  krafka emits spans through `tracing` only; the bridge belongs to the application.", file=sys.stderr)
        return 1
    print("✓ No OpenTelemetry crate in krafka's dependency graph (default and all features)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
