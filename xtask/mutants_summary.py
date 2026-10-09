#!/usr/bin/env python3
"""Summarise a cargo-mutants run: counts, then every survivor and timeout.

Prints to stdout and, under GitHub Actions, appends the same text to the job
summary (`$GITHUB_STEP_SUMMARY`).

Run: python3 xtask/mutants_summary.py [mutants.out]
"""

from __future__ import annotations

import os
import sys
from pathlib import Path


def lines(path: Path) -> list[str]:
    return [l for l in path.read_text().splitlines() if l.strip()] if path.exists() else []


def main() -> int:
    out = Path(sys.argv[1] if len(sys.argv) > 1 else "mutants.out")
    if not out.is_dir():
        print(f"no cargo-mutants output at {out}")
        return 0
    caught = lines(out / "caught.txt")
    missed = lines(out / "missed.txt")
    timeout = lines(out / "timeout.txt")
    unviable = lines(out / "unviable.txt")
    tested = len(caught) + len(missed) + len(timeout) + len(unviable)

    text = [
        "## Mutation testing",
        "",
        f"{tested} mutants tested: {len(caught)} caught, {len(missed)} survived, "
        f"{len(timeout)} timed out, {len(unviable)} unviable.",
    ]
    if missed:
        text += ["", "### Survivors (no test failed)", "", *[f"- `{m}`" for m in missed]]
    if timeout:
        text += ["", "### Timeouts", "", *[f"- `{m}`" for m in timeout]]
    body = "\n".join(text) + "\n"
    print(body)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as f:
            f.write(body)
    return 0


if __name__ == "__main__":
    sys.exit(main())
