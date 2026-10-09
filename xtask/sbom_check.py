#!/usr/bin/env python3
"""The release SBOM names every package of the default-feature graph.

Reads a CycloneDX JSON SBOM (from `cargo cyclonedx`) and fails if any package
`cargo tree -e normal,build` reports for the default features on `--target` is
missing from its components (matched by name and version).

Run: python3 xtask/sbom_check.py <sbom.json> [--target x86_64-unknown-linux-gnu]
     python3 xtask/sbom_check.py <sbom.json> --self-test   # a direct dependency removed must fail
"""

from __future__ import annotations

import argparse
import copy
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def tree_packages(target: str) -> set[tuple[str, str]]:
    out = subprocess.run(
        ["cargo", "tree", "--locked", "--color", "never", "-e", "normal,build", "--target", target,
         "--prefix", "none", "--format", "{p}"],
        cwd=ROOT, capture_output=True, text=True, check=True,
    ).stdout
    pkgs = set()
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[1].startswith("v"):
            pkgs.add((parts[0], parts[1][1:]))
    return pkgs


def direct_dependencies(target: str) -> set[str]:
    out = subprocess.run(
        ["cargo", "tree", "--locked", "--color", "never", "-e", "normal", "--target", target, "--depth", "1",
         "--prefix", "none", "--format", "{p}"],
        cwd=ROOT, capture_output=True, text=True, check=True,
    ).stdout
    return {line.split()[0] for line in out.splitlines()[1:] if line.strip()}


def sbom_packages(sbom: dict) -> set[tuple[str, str]]:
    found = set()

    def walk(components: list[dict]) -> None:
        for c in components:
            found.add((c.get("name", ""), c.get("version", "")))
            walk(c.get("components", []))

    walk(sbom.get("components", []))
    meta = sbom.get("metadata", {}).get("component")
    if meta:
        found.add((meta.get("name", ""), meta.get("version", "")))
    return found


def missing(sbom: dict, expected: set[tuple[str, str]]) -> list[str]:
    return sorted(f"{n} {v}" for n, v in expected - sbom_packages(sbom))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("sbom", type=Path)
    ap.add_argument("--target", default="x86_64-unknown-linux-gnu")
    ap.add_argument("--self-test", action="store_true")
    args = ap.parse_args()
    sbom = json.loads(args.sbom.read_text())
    expected = tree_packages(args.target)

    if args.self_test:
        gaps = missing(sbom, expected)
        print(("✓" if not gaps else "✗") + f" the SBOM as generated: {len(gaps)} missing")
        dep = sorted(direct_dependencies(args.target))[0]
        planted = copy.deepcopy(sbom)
        planted["components"] = [c for c in planted.get("components", []) if c.get("name") != dep]
        gaps_planted = missing(planted, expected)
        print(("✓" if gaps_planted else "✗") + f" plant: direct dependency `{dep}` removed -> missing {gaps_planted}")
        return 0 if (not gaps and gaps_planted) else 1

    gaps = missing(sbom, expected)
    if gaps:
        print(f"✗ SBOM lacks {len(gaps)} package(s) of the default graph:", file=sys.stderr)
        for g in gaps:
            print(f"  {g}", file=sys.stderr)
        return 1
    print(f"✓ SBOM names all {len(expected)} packages of the default graph on {args.target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
