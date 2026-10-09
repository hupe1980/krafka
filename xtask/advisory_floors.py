#!/usr/bin/env python3
"""No declared dependency requirement may admit a version with an advisory.

`cargo deny check advisories` audits the versions in *our* lockfile. A
downstream lockfile resolves `rustls = "0.23"` to whatever it already has, and
0.23.13–0.23.44 carry RUSTSEC-2026-0285. This check reads every version
requirement in Cargo.toml and fails when the range it admits contains a
version that a RustSec advisory marks affected.

Affectedness is piecewise constant between the versions an advisory names,
so testing the requirement's floor and every named version (and its next
patch) inside the range is exact for release versions.

The advisory database is the one `cargo deny fetch` maintains under
`~/.cargo/advisory-db*` (or `$ADVISORY_DB`). Without one the check is skipped
locally and fails under CI (`$CI` set).
"""

from __future__ import annotations

import glob
import os
import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

Version = tuple[int, int, int]


def parse_version(text: str) -> Version:
    parts = [int(p) for p in re.match(r"\d+(?:\.\d+){0,2}", text.strip()).group(0).split(".")]
    return tuple(parts + [0] * (3 - len(parts)))  # type: ignore[return-value]


def caret_upper(text: str) -> Version:
    parts = [int(p) for p in text.strip().split(".")]
    major, minor, patch = (parts + [0, 0])[:3]
    if major > 0 or len(parts) == 1:
        return (major + 1, 0, 0)
    if minor > 0 or len(parts) == 2:
        return (0, minor + 1, 0)
    return (0, 0, patch + 1)


def comparator_matches(comp: str, v: Version) -> bool:
    m = re.match(r"\s*(>=|<=|>|<|=|\^|~)?\s*([\d.]+)", comp)
    if not m:
        return False
    op, ver = m.group(1) or "^", m.group(2)
    bound = parse_version(ver)
    return {
        ">=": v >= bound,
        "<=": v <= bound,
        ">": v > bound,
        "<": v < bound,
        "=": v == bound,
        "^": bound <= v < caret_upper(ver),
        "~": bound <= v < ((bound[0], bound[1] + 1, 0) if ver.count(".") else (bound[0] + 1, 0, 0)),
    }[op]


def req_matches(req: str, v: Version) -> bool:
    return all(comparator_matches(c, v) for c in req.split(","))


def requirement_range(req: str) -> tuple[Version, Version] | None:
    """[floor, ceiling) of a caret or bare requirement; None for others."""
    req = req.strip()
    if not re.fullmatch(r"\^?\d+(\.\d+){0,2}", req):
        return None
    text = req.lstrip("^")
    return parse_version(text), caret_upper(text)


def declared_requirements(manifest: dict) -> list[tuple[str, str]]:
    tables = [manifest]
    tables += list(manifest.get("target", {}).values())
    found = []
    for table in tables:
        for section in ("dependencies", "dev-dependencies", "build-dependencies"):
            for name, spec in table.get(section, {}).items():
                if isinstance(spec, str):
                    found.append((name, spec))
                elif isinstance(spec, dict) and "version" in spec:
                    found.append((spec.get("package", name), spec["version"]))
    return found


def advisories(db: Path, crate: str) -> list[dict]:
    out = []
    for path in sorted((db / "crates" / crate).glob("*.md")):
        text = path.read_text()
        m = re.search(r"```toml\n(.*?)```", text, re.S)
        if m:
            out.append(tomllib.loads(m.group(1)))
    return out


def affected(advisory: dict, v: Version) -> bool:
    versions = advisory.get("versions", {})
    safe = versions.get("patched", []) + versions.get("unaffected", [])
    return not any(req_matches(r, v) for r in safe)


def first_affected(advisory: dict, low: Version, high: Version) -> Version | None:
    versions = advisory.get("versions", {})
    points = {low}
    for req in versions.get("patched", []) + versions.get("unaffected", []):
        for ver in re.findall(r"\d+(?:\.\d+){0,2}", req):
            v = parse_version(ver)
            points |= {v, (v[0], v[1], v[2] + 1)}
    for v in sorted(points):
        if low <= v < high and affected(advisory, v):
            return v
    return None


def find_db() -> Path | None:
    candidates = [os.environ.get("ADVISORY_DB", "")]
    candidates += glob.glob(os.path.expanduser("~/.cargo/advisory-db*/**/crates"), recursive=True)
    for c in candidates:
        path = Path(c)
        if path.name == "crates":
            path = path.parent
        if c and (path / "crates").is_dir():
            return path
    return None


def check(manifest_path: Path, db: Path) -> list[str]:
    manifest = tomllib.loads(manifest_path.read_text())
    failures = []
    for crate, req in declared_requirements(manifest):
        bounds = requirement_range(req)
        if bounds is None:
            continue
        for advisory in advisories(db, crate):
            meta = advisory.get("advisory", {})
            if meta.get("withdrawn") or meta.get("informational"):
                continue
            hit = first_affected(advisory, *bounds)
            if hit:
                failures.append(
                    f'{crate} = "{req}" admits {".".join(map(str, hit))}, affected by '
                    f"{meta.get('id')} ({meta.get('url', '')}); raise the floor to a "
                    f"patched version: {', '.join(advisory['versions'].get('patched', []))}"
                )
    return failures


def main() -> int:
    args = sys.argv[1:]
    require_db = bool(os.environ.get("CI"))
    manifests = [Path(a) for a in args if not a.startswith("--")] or [ROOT / "Cargo.toml"]
    db = find_db()
    if db is None:
        msg = "no RustSec advisory database found (run `cargo deny fetch`)"
        if require_db:
            print(f"✗ advisory-floors: {msg}", file=sys.stderr)
            return 1
        print(f"⊘ advisory-floors: {msg} — skipping.")
        return 0

    failures = [f for m in manifests for f in check(m, db)]
    if failures:
        print("Dependency floors admit advised versions:\n", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        return 1
    print(f"✓ advisory-floors: no declared requirement admits an advised version ({db})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
