#!/usr/bin/env python3
"""An API break against the last release is either declared or fails.

Runs `cargo semver-checks check-release` over the stable surface — every
feature except EXCLUDED (and `unstable-*`), named explicitly for both the
baseline and the current crate — and then:

  * no break found                         -> pass
  * break found, version < 1.0, and the CHANGELOG.md section for this release
    (`## [Unreleased]`, or `## [X.Y.Z]` matching Cargo.toml once the release is
    cut) has a non-empty `### Breaking` list -> pass, classification printed
  * break found otherwise                  -> fail (from 1.0, a break in a
    non-major bump fails whatever the CHANGELOG says)
  * the tool could not complete            -> fail

Before 1.0 the comparison runs as a patch release, so a break is reported even
when Cargo.toml already carries the next minor version.

Run: python3 xtask/semver_gate.py
     python3 xtask/semver_gate.py --self-test   # the planted-violation controls
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_tomllib  # noqa: E402

ensure_tomllib()
import tomllib  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
CRATE = "krafka"

# Features outside the semver promise: `krafka::testing` is documented
# unstable; `internal` exposes `__private` for benches and fuzzing.
EXCLUDED = {"test-broker", "internal"}

BREAK = 100  # cargo-semver-checks: semver violation found
INCOMPLETE = 101  # cargo-semver-checks: the check could not run


def stable(features: list[str]) -> list[str]:
    return sorted(
        f for f in features if f != "default" and f not in EXCLUDED and not f.startswith("unstable-")
    )


def index_entries() -> list[dict]:
    name = CRATE.lower()
    url = f"https://index.crates.io/{name[:2]}/{name[2:4]}/{name}"
    with urllib.request.urlopen(url, timeout=30) as resp:  # noqa: S310 (fixed https URL)
        return [json.loads(line) for line in resp.read().decode().splitlines() if line.strip()]


def baseline() -> tuple[str, list[str]]:
    """The newest non-yanked release and its feature names."""
    entries = [e for e in index_entries() if not e.get("yanked")]
    if not entries:
        raise SystemExit("✗ semver-check: no published baseline on crates.io")
    entry = max(entries, key=lambda e: tuple(int(p) for p in re.findall(r"\d+", e["vers"])[:3]))
    features = list(entry.get("features", {})) + list(entry.get("features2", {}) or {})
    return entry["vers"], features


def release_breaking(changelog: str, version: str) -> list[str]:
    """Bullets under `### Breaking` in `## [Unreleased]` or `## [<version>]`."""
    entries: list[str] = []
    for heading in ("Unreleased", re.escape(version)):
        m = re.search(rf"^## \[{heading}\][^\n]*\n(.*?)(?=^## |\Z)", changelog, re.M | re.S)
        if not m:
            continue
        b = re.search(r"^### Breaking[^\n]*\n(.*?)(?=^### |\Z)", m.group(1), re.M | re.S)
        if b:
            entries += [line for line in b.group(1).splitlines() if line.lstrip().startswith(("- ", "* "))]
    return entries


def decide(code: int, version: str, changelog: str) -> tuple[bool, str]:
    major = int(version.split(".")[0])
    if code == 0:
        return True, "no API break against the baseline"
    if code != BREAK:
        return False, f"cargo-semver-checks could not complete (exit {code})"
    if major >= 1:
        return False, f"API break in a non-major release of {version}"
    entries = release_breaking(changelog, version)
    if not entries:
        return False, (
            f"API break with no `### Breaking` entries under `## [Unreleased]` or `## [{version}]` in CHANGELOG.md"
        )
    return True, f"API break declared: {len(entries)} Breaking entries for {version}"


def semver_command(current: list[str], base: list[str], version: str) -> list[str]:
    cmd = ["cargo", "semver-checks", "check-release", "--only-explicit-features"]
    if current:
        cmd += ["--current-features", ",".join(current)]
    if base:
        cmd += ["--baseline-features", ",".join(base)]
    if int(version.split(".")[0]) == 0:
        cmd += ["--release-type", "patch"]
    return cmd


def main() -> int:
    if "--self-test" in sys.argv:
        return self_test()

    manifest = tomllib.loads((ROOT / "Cargo.toml").read_text())
    version = manifest["package"]["version"]
    current = stable(list(manifest.get("features", {})))
    base_version, base_features = baseline()
    base = stable(base_features)

    cmd = semver_command(current, base, version)
    print(f"▶ {CRATE} {version} against {base_version}; features {','.join(current) or '(none)'}")
    print(f"  {' '.join(cmd)}", flush=True)
    code = subprocess.run(cmd, cwd=ROOT).returncode

    ok, why = decide(code, version, (ROOT / "CHANGELOG.md").read_text())
    print(("✓ semver-check: " if ok else "✗ semver-check: ") + why, file=sys.stdout if ok else sys.stderr)
    return 0 if ok else 1


# ---------------------------------------------------------------------------
# Negative controls.
# ---------------------------------------------------------------------------

CHANGELOG_WITH = "# Changelog\n\n## [Unreleased]\n\n### Breaking\n\n- `planted` removed.\n\n## [0.26.0]\n"
CHANGELOG_WITHOUT = "# Changelog\n\n## [Unreleased]\n\n### Added\n\n- x\n\n## [0.26.0]\n\n### Breaking\n\n- old\n"
CHANGELOG_RELEASED = "# Changelog\n\n## [Unreleased]\n\n## [0.27.0] — 2026-10-09\n\n### Breaking\n\n- `planted` removed.\n\n## [0.26.0]\n"


def self_test() -> int:
    import shutil
    import tempfile

    failures = 0

    def expect(name: str, got: bool, want: bool, why: str, words: tuple[str, str] = ("pass", "fail")) -> None:
        nonlocal failures
        mark = "✓" if got == want else "✗"
        if got != want:
            failures += 1
        print(f"{mark} {name}: {words[0] if got else words[1]} (expected {words[0] if want else words[1]}) — {why}")

    # The decision, for each outcome the tool can report.
    for name, args, want in [
        ("(a) break, no Breaking entry", (BREAK, "0.27.0", CHANGELOG_WITHOUT), False),
        ("(b) break, Breaking entry", (BREAK, "0.27.0", CHANGELOG_WITH), True),
        ("(b2) break, Breaking entry in the cut release section", (BREAK, "0.27.0", CHANGELOG_RELEASED), True),
        ("(b3) break, Breaking only in an older release section", (BREAK, "0.28.0", CHANGELOG_RELEASED), False),
        ("(d) tool could not complete", (INCOMPLETE, "0.27.0", CHANGELOG_WITH), False),
        ("(e) break at 1.0.1 with Breaking entry", (BREAK, "1.0.1", CHANGELOG_WITH), False),
        ("no break", (0, "0.27.0", CHANGELOG_WITHOUT), True),
    ]:
        ok, why = decide(*args)
        expect(name, ok, want, why)

    # The scope: a removal confined to an excluded feature is not reported,
    # and the same removal in the stable surface is. The baseline is a copy of
    # this tree with one planted public function; the current crate lacks it.
    manifest = tomllib.loads((ROOT / "Cargo.toml").read_text())
    version = manifest["package"]["version"]
    features = stable(list(manifest.get("features", {})))
    plants = {
        "(c) removal under `unstable-protocol`": ('#[cfg(feature = "unstable-protocol")]\n', False),
        "(f) removal under `test-broker` (krafka::testing)": ('#[cfg(feature = "test-broker")]\n', False),
        "(h) removal under `internal` (krafka::__private)": ('#[cfg(feature = "internal")]\n', False),
        "(g) the same removal in the stable surface": ("", True),
    }
    for name, (gate, reported) in plants.items():
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp) / "baseline"
            shutil.copytree(ROOT, base, ignore=shutil.ignore_patterns("target", ".git", "site", "fuzz", "concepts", "specs"))
            lib = base / "src" / "lib.rs"
            lib.write_text(lib.read_text() + f"\n/// Planted.\n{gate}pub fn semver_gate_plant() {{}}\n")
            flags = semver_command(features, features, version)[3:]
            cmd = ["cargo", "semver-checks", "check-release", "--baseline-root", str(base), *flags]
            code = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True).returncode
            expect(name, code == BREAK, reported, f"cargo-semver-checks exit {code}", ("reported", "not reported"))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
