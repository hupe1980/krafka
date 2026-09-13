#!/usr/bin/env python3
"""Every check in `just ci` has a CI job, and the required check gates them all.

# Why this exists

The `justfile` is the single source of truth for what the checks are, and CI
calls the same recipes, so a check cannot pass locally and fail in CI because
the two drifted apart. That arrangement has one blind spot, and this project
sat in it twice:

  * `just integration-sasl` was runnable from the day it was written and ran in
    no workflow for its entire life, so a SASL handshake regression could only
    ever be caught by hand.
  * `just docs-test` — the gate behind every ```rust,compile block in the
    README and the guides — was in `just ci`, and therefore in the release
    recipe, while having no job in any workflow. A pull request could break
    every documented sample and stay green.

Both are the same defect: a gate that is real, correct, and attached to
nothing. It is invisible from both sides — the recipe list looks complete, and
the workflow looks complete, because neither is compared to the other.

The second failure mode is subtler and is about the *required* check. `ci.yml`
had 19 jobs and no `needs:` anywhere, so branch protection had to name each one
by hand, and a newly added job was not required by default: it ran, it could
fail, and the merge button stayed green until somebody remembered to add it.

# What it checks

  1. Every recipe named in `just ci` has a job in `ci.yml` that runs it —
     except the recipes in ALLOWED_WITHOUT_JOB below, each with a reason.
  2. Every job defined in `ci.yml` is listed in the `ci-success` aggregator's
     `needs:`, so the single required check genuinely gates all of them.
  3. `ci-success` carries `if: always()`. Without it the job is skipped when a
     dependency fails, and GitHub reads a skipped required check as SUCCESS —
     which inverts the rule the job exists to enforce.

Run: python3 xtask/ci_job_parity.py
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
JUSTFILE = ROOT / "justfile"
CI = ROOT / ".github" / "workflows" / "ci.yml"
AGGREGATOR = "ci-success"

# Recipes in `just ci` that deliberately have no dedicated CI job.
ALLOWED_WITHOUT_JOB = {
    "site-check": (
        "runs in pages.yml, which is path-filtered to site/** and src/** — the "
        "only inputs it reads"
    ),
}


def fail(messages: list[str]) -> None:
    print("✗ CI job parity", file=sys.stderr)
    for m in messages:
        print(f"  {m}", file=sys.stderr)
    sys.exit(1)


def ci_recipe_dependencies() -> list[str]:
    """The recipe names on the `ci:` line of the justfile."""
    for line in JUSTFILE.read_text().splitlines():
        m = re.match(r"^ci:\s*(.*)$", line)
        if m:
            return m.group(1).split()
    fail(["no `ci:` recipe found in justfile"])
    return []


def ci_yaml() -> str:
    return CI.read_text()


def main() -> int:
    problems: list[str] = []
    yaml = ci_yaml()

    # Job names are the two-space-indented keys under `jobs:` — and only
    # there. Scanning the whole file also matches the trigger names under
    # `on:` (`push`, `pull_request`), which are not jobs.
    jobs_section = re.search(r"^jobs:\s*$(.*)\Z", yaml, re.M | re.S)
    if not jobs_section:
        fail(["no `jobs:` section found in ci.yml"])
    jobs = re.findall(r"^  ([a-z][a-z0-9-]*):\s*$", jobs_section.group(1), re.M)
    if not jobs:
        fail(["no jobs parsed from ci.yml"])
    job_set = set(jobs)

    # ---- 1. every `just ci` recipe is run by some job -----------------------
    for recipe in ci_recipe_dependencies():
        if recipe in ALLOWED_WITHOUT_JOB:
            continue
        if not re.search(rf"run:\s*just {re.escape(recipe)}\s*$", yaml, re.M):
            problems.append(
                f"`just {recipe}` is in `just ci` but no job in ci.yml runs it. "
                f"Add a job, or add it to ALLOWED_WITHOUT_JOB with a reason."
            )

    # ---- 2. the aggregator needs every job ---------------------------------
    if AGGREGATOR not in job_set:
        problems.append(
            f"no `{AGGREGATOR}` job in ci.yml — there is no single required "
            f"status check, so branch protection must name every job by hand "
            f"and a new job is not required by default."
        )
    else:
        block = re.search(
            rf"^  {AGGREGATOR}:\s*$(.*?)(?=^  [a-z][a-z0-9-]*:\s*$|\Z)",
            yaml,
            re.M | re.S,
        )
        body = block.group(1) if block else ""
        needs_block = re.search(r"^    needs:\s*$((?:\s*^      - .*$)+)", body, re.M)
        listed = set(re.findall(r"^      - ([a-z][a-z0-9-]*)\s*$", needs_block.group(1), re.M)) if needs_block else set()

        for missing in sorted(job_set - listed - {AGGREGATOR}):
            problems.append(
                f"job `{missing}` is not in `{AGGREGATOR}`'s needs: — it can "
                f"fail without blocking a merge."
            )
        for ghost in sorted(listed - job_set):
            problems.append(
                f"`{AGGREGATOR}` needs `{ghost}`, which is not a job in ci.yml. "
                f"A needs: entry naming nothing leaves the check pending forever."
            )

        # ---- 3. if: always() ------------------------------------------------
        if not re.search(r"^    if:\s*always\(\)\s*$", body, re.M):
            problems.append(
                f"`{AGGREGATOR}` lacks `if: always()`. Without it the job is "
                f"skipped when a dependency fails — and GitHub reads a skipped "
                f"required check as SUCCESS, inverting the rule it enforces."
            )

    if problems:
        fail(problems)

    gated = len(job_set) - 1
    print(
        f"✓ CI job parity: {gated} jobs, all gated by `{AGGREGATOR}`; "
        f"every `just ci` recipe has a job "
        f"({len(ALLOWED_WITHOUT_JOB)} documented exception)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
