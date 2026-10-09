#!/usr/bin/env python3
"""Library code reads time, the network and randomness only where a simulation controls them.

The simulation (`just sim`) runs the client on a paused Tokio clock, over
in-memory streams and with seeded random draws. That holds only while the
library outside `src/testing/` keeps to three rules, which this checks:

  1. Elapsed time and deadlines use `tokio::time::Instant`, never
     `std::time::Instant` (the paused clock does not move it).
  2. Every outbound connection and name resolution goes through
     `src/network/connector.rs` (and `happy_eyeballs.rs`, which only it
     calls): no `TcpStream::connect`, `TcpSocket`, `lookup_host` elsewhere.
  3. Random draws that shape behaviour go through `util::with_rng`, which a
     simulation seeds: no `rand::rng()`, `rand::random`, `thread_rng`
     elsewhere. ALLOWED_RANDOM names the cryptographic draws that must stay
     on the OS-seeded CSPRNG.

Comments and `#[cfg(test)]` modules are not checked.

Run: python3 xtask/determinism.py
     python3 xtask/determinism.py --self-test   # the planted-violation controls
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"

STD_INSTANT = re.compile(r"std::time::Instant\b|use std::time::\{[^}]*\bInstant\b")
DIAL = re.compile(r"\bTcpStream::connect\b|\bTcpSocket\b|\blookup_host\b|\bUdpSocket\b")
RANDOM = re.compile(r"\brand::rng\(\)|\brand::random\b|\bthread_rng\b|\bfrom_os_rng\b|\bOsRng\b")

# Files allowed to dial: the connector seam and the Happy Eyeballs racer it calls.
ALLOWED_DIAL = {
    "src/network/connector.rs": "the connector seam",
    "src/network/happy_eyeballs.rs": "called only from the connector seam",
}

# Files allowed to draw from the OS-seeded RNG, with the reason.
ALLOWED_RANDOM = {
    "src/util.rs": "seeds the thread RNG behind `with_rng`; UUIDs stay CSPRNG-drawn",
    "src/auth/scram.rs": "RFC 5802 nonces must come from a CSPRNG",
}


def code_lines(text: str) -> list[tuple[int, str]]:
    """Numbered lines outside comments and `#[cfg(test)]` modules."""
    lines = text.splitlines()
    out: list[tuple[int, str]] = []
    skip_depth: int | None = None
    depth = 0
    pending_test = False
    for number, line in enumerate(lines, 1):
        stripped = line.strip()
        code = line.split("//", 1)[0]
        if skip_depth is None:
            if stripped.startswith("#[cfg(test)]"):
                pending_test = True
            elif pending_test and re.match(r"(pub(\([^)]*\))?\s+)?mod\s+\w+\s*\{", stripped):
                skip_depth = depth
                pending_test = False
            elif stripped and not stripped.startswith("#[") and not stripped.startswith("//"):
                pending_test = False
                out.append((number, code))
        depth += code.count("{") - code.count("}")
        if skip_depth is not None and depth <= skip_depth and "}" in code:
            skip_depth = None
    return out


def check_text(path: str, text: str) -> list[str]:
    problems = []
    for number, code in code_lines(text):
        where = f"{path}:{number}"
        if STD_INSTANT.search(code):
            problems.append(f"{where}: std::time::Instant; use tokio::time::Instant")
        if DIAL.search(code) and path not in ALLOWED_DIAL:
            problems.append(f"{where}: a dial or name lookup outside src/network/connector.rs")
        if RANDOM.search(code) and path not in ALLOWED_RANDOM:
            problems.append(f"{where}: an OS-seeded random draw; use crate::util::with_rng")
    return problems


def check_tree() -> list[str]:
    problems = []
    for file in sorted(SRC.rglob("*.rs")):
        rel = file.relative_to(ROOT).as_posix()
        if rel.startswith("src/testing/"):
            continue
        problems.extend(check_text(rel, file.read_text(encoding="utf-8")))
    for allowed in [*ALLOWED_DIAL, *ALLOWED_RANDOM]:
        if not (ROOT / allowed).is_file():
            problems.append(f"{allowed}: allow-listed but missing")
    return problems


def self_test() -> int:
    plants = {
        "std Instant": ("src/x.rs", "fn f() { let t = std::time::Instant::now(); }"),
        "std Instant import": ("src/x.rs", "use std::time::{Duration, Instant};"),
        "direct dial": ("src/x.rs", "let s = TcpStream::connect(addr).await?;"),
        "name lookup": ("src/http.rs", "let a = tokio::net::lookup_host(h).await?;"),
        "unseeded draw": ("src/metadata.rs", "addrs.shuffle(&mut rand::rng());"),
    }
    clean = {
        "tokio Instant": ("src/x.rs", "use tokio::time::Instant;"),
        "comment": ("src/x.rs", "/// std::time::Instant in a doc example"),
        "test module": ("src/x.rs", "#[cfg(test)]\nmod tests {\n    fn f() { let _ = std::time::Instant::now(); }\n}"),
        "seam": ("src/network/connector.rs", "TcpStream::connect((host, port)).await"),
        "seeded draw": ("src/x.rs", "crate::util::with_rng(|rng| addrs.shuffle(rng));"),
    }
    failed = False
    for name, (path, text) in plants.items():
        if not check_text(path, text):
            print(f"✗ self-test: planted {name} not reported", file=sys.stderr)
            failed = True
    for name, (path, text) in clean.items():
        if found := check_text(path, text):
            print(f"✗ self-test: clean {name} reported: {found}", file=sys.stderr)
            failed = True
    if failed:
        return 1
    print(f"✓ determinism self-test: {len(plants)} plants reported, {len(clean)} clean cases pass")
    return 0


def main() -> int:
    if "--self-test" in sys.argv[1:]:
        return self_test()
    problems = check_tree()
    if problems:
        print("\n".join(f"✗ {p}" for p in problems), file=sys.stderr)
        return 1
    print("✓ library time, dials and random draws go through what a simulation controls")
    return 0


if __name__ == "__main__":
    sys.exit(main())
