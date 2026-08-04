#!/usr/bin/env python3
"""Every place that names krafka's own version must agree with Cargo.toml.

# Why this exists

A version bump is a search-and-replace, and search-and-replace has a blind
spot that bit this project: you grep for the *old* version, so anywhere already
stale is invisible. Bumping 0.14 → 0.15 could not find `fuzz/Cargo.lock`,
which had been pinned at **0.12.0** for two releases — the grep was looking for
"0.14" and that file did not contain it.

The fix is to stop searching for a value and start asserting an invariant:
enumerate every location that declares krafka's version and require it to equal
`Cargo.toml`'s, whatever that is.

# What it checks

  1. `Cargo.lock` and every nested lockfile (`fuzz/Cargo.lock`, …) — the
     resolved version of the `krafka` package itself, not its dependencies.
  2. `site/zola.toml`'s `extra.version`, which the docs footer renders.
  3. Every `krafka = "X"` / `krafka = { version = "X", … }` dependency snippet
     in Markdown and in `//!` doc comments — the lines a reader copies.

Snippets may abbreviate to `MAJOR.MINOR` (`"0.15"`), which is what Cargo
resolves anyway; a snippet naming a *different* minor is an error.
"""

from __future__ import annotations

import re
import sys
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Files whose dependency snippets are user-facing.
SNIPPET_GLOBS = ("README.md", "site/content/**/*.md", "src/lib.rs")


def main() -> int:
    cargo = tomllib.loads((ROOT / "Cargo.toml").read_text())
    expected = cargo["package"]["version"]
    major_minor = ".".join(expected.split(".")[:2])

    errors: list[str] = []
    checked = 0

    # ── 1. Lockfiles ────────────────────────────────────────────────────
    #
    # A lockfile lists every dependency, so the `krafka` entry has to be
    # located by name rather than by scanning for version strings.
    for lock in sorted(ROOT.glob("**/Cargo.lock")):
        if "target" in lock.parts:
            continue
        text = lock.read_text()
        m = re.search(r'^name = "krafka"\nversion = "([^"]+)"', text, re.M)
        if not m:
            continue
        checked += 1
        if m.group(1) != expected:
            line = text[: m.start()].count("\n") + 2
            errors.append(
                f"{lock.relative_to(ROOT)}:{line}  krafka is locked at "
                f"{m.group(1)!r}, but Cargo.toml says {expected!r}.\n"
                f"    Refresh it: cargo update --manifest-path "
                f"{lock.parent.relative_to(ROOT) or '.'}/Cargo.toml --package krafka"
            )

    # ── 2. Site config ──────────────────────────────────────────────────
    zola = ROOT / "site" / "zola.toml"
    if zola.exists():
        site_version = tomllib.loads(zola.read_text()).get("extra", {}).get("version")
        checked += 1
        if site_version != expected:
            errors.append(
                f"site/zola.toml  extra.version is {site_version!r}, "
                f"but Cargo.toml says {expected!r}"
            )

    # ── 3. Dependency snippets ──────────────────────────────────────────
    #
    # `krafka = "0.15"`, `krafka = { version = "0.15.0", features = [...] }`,
    # and the same inside `//!` doc comments.
    snippet = re.compile(r'krafka\s*=\s*(?:\{[^}]*?version\s*=\s*)?"([^"]+)"')
    paths: list[Path] = []
    for pattern in SNIPPET_GLOBS:
        paths.extend(ROOT.glob(pattern))
    for path in sorted(set(paths)):
        text = path.read_text()
        for m in snippet.finditer(text):
            found = m.group(1)
            checked += 1
            # A snippet may abbreviate to major.minor; anything else must match
            # exactly.
            if found in (expected, major_minor):
                continue
            line = text[: m.start()].count("\n") + 1
            errors.append(
                f"{path.relative_to(ROOT)}:{line}  snippet says krafka = "
                f"{found!r}; expected {expected!r} or {major_minor!r}"
            )

    if errors:
        print("Version consistency check FAILED\n", file=sys.stderr)
        for e in errors:
            print(f"  - {e}\n", file=sys.stderr)
        return 1

    print(f"✓ Version consistency: {checked} declarations all agree on {expected}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
