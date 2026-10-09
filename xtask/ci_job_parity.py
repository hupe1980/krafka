#!/usr/bin/env python3
"""Every check runs in CI, and every job either gates `CI` or is declared not to.

The `justfile` is the single source of truth for what the checks are and CI
calls its recipes. This asserts the wiring between the two, and that no job or
workflow sits outside the single required check (`ci-success`, named `CI`)
without a declared reason.

# What it checks

  1. Every recipe named in `just ci` has a job in `ci.yml` that runs it, or an
     ALLOWED_WITHOUT_JOB entry naming another workflow that runs it on every
     pull request with no path filter.
  2. Every `ci.yml` job is in `ci-success`'s `needs:` or in NON_BLOCKING_JOBS,
     not both. A declared non-blocking job must exist, must not be needed by a
     gated job (its failure would skip that job and fail `CI`), and a gated job
     must not carry `continue-on-error: true` (it could not fail `CI`).
  3. Every job in MUST_BLOCK exists and is gated.
  4. Every workflow file under `.github/workflows/` other than `ci.yml` is in
     WORKFLOWS with a class and a reason, and every declared workflow exists.
  5. `ci-success` carries `if: always()` and fails on any non-success result.
     Without `if: always()` it is skipped when a dependency fails, and GitHub
     reads a skipped required check as success.

Run: python3 xtask/ci_job_parity.py
     python3 xtask/ci_job_parity.py --self-test   # the planted-violation controls
"""

from __future__ import annotations

import re
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
AGGREGATOR = "ci-success"

# `just ci` recipes that run in a workflow other than ci.yml. Each must name
# that workflow, and the workflow must run `just <recipe>` on every pull
# request (no `paths:` filter), or the exemption is a gap.
ALLOWED_WITHOUT_JOB: dict[str, dict[str, str]] = {}

# ci.yml jobs that report without gating `CI`. Wiring: absent from
# `ci-success`'s needs; GitHub shows their result on the pull request.
NON_BLOCKING_JOBS: dict[str, str] = {}

# Jobs that must gate `CI`: a red result in them is a krafka regression.
MUST_BLOCK = {
    "integration-redpanda": "the pinned Redpanda suite; only krafka changes between two runs",
    "integration-sasl": "the SASL suite at the supported floor and the newest 4.3",
    "fuzz": "a panic on broker input violates the untrusted-input guarantee",
}

# Every other workflow, classed as a non-blocking check (scheduled or
# event-driven evidence whose failure is a reason to look, not to refuse a
# merge) or as not a check (release, deploy, report).
WORKFLOWS = {
    "publish.yml": ("not-a-check", "release: packages, attests and publishes a tagged commit whose CI passed"),
    "pages.yml": ("not-a-check", "deploy: publishes the site from main; CI's `site` job builds and checks it"),
    "fuzz-nightly.yml": ("non-blocking", "nightly 30-minute fuzz campaign that grows the corpus"),
    "sim-nightly.yml": (
        "non-blocking",
        "nightly simulation seed budget and planted-defect controls; the per-PR budget gates in `sim`",
    ),
    "mutants-weekly.yml": ("non-blocking", "weekly full mutation run over the scoped files; records the survivor count"),
    "redpanda-latest.yml": (
        "non-blocking",
        "third-party latest image; its changes must not block a krafka merge",
    ),
}
CLASSES = {"non-blocking", "not-a-check"}


def jobs_in(yaml: str) -> dict[str, str]:
    """Job name -> its block text, for the two-space-indented keys under `jobs:`."""
    section = re.search(r"^jobs:\s*$(.*)\Z", yaml, re.M | re.S)
    if not section:
        return {}
    body = section.group(1)
    names = list(re.finditer(r"^  ([a-z][a-z0-9_-]*):\s*$", body, re.M))
    blocks = {}
    for i, m in enumerate(names):
        end = names[i + 1].start() if i + 1 < len(names) else len(body)
        blocks[m.group(1)] = body[m.end():end]
    return blocks


def needs_of(block: str) -> set[str]:
    inline = re.search(r"^    needs:\s*\[([^\]]*)\]\s*$", block, re.M)
    if inline:
        return {n.strip() for n in inline.group(1).split(",") if n.strip()}
    scalar = re.search(r"^    needs:\s*([a-z][a-z0-9_-]*)\s*$", block, re.M)
    if scalar:
        return {scalar.group(1)}
    listed = re.search(r"^    needs:\s*$((?:\n      - .*)+)", block, re.M)
    if listed:
        return set(re.findall(r"^      - ([a-z][a-z0-9_-]*)\s*$", listed.group(1), re.M))
    return set()


def ci_recipes(justfile: str) -> list[str]:
    for line in justfile.splitlines():
        m = re.match(r"^ci:\s*(.*)$", line)
        if m:
            return m.group(1).split()
    return []


def runs_recipe(yaml: str, recipe: str) -> bool:
    return re.search(rf"run:\s*just {re.escape(recipe)}(\s|$)", yaml, re.M) is not None


def runs_on_every_pull_request(yaml: str) -> bool:
    on = re.search(r"^'?on'?:\s*$(.*?)(?=^\S)", yaml, re.M | re.S)
    if not on:
        return False
    pr = re.search(r"^  pull_request:\s*$(.*?)(?=^  \S|\Z)", on.group(1), re.M | re.S)
    if not pr:
        return re.search(r"^  pull_request:\s*\{\}\s*$", on.group(1), re.M) is not None
    return "paths:" not in pr.group(1) and "paths-ignore:" not in pr.group(1)


def check(root: Path) -> tuple[list[str], str]:
    problems: list[str] = []
    workflows = root / ".github" / "workflows"
    ci_path = workflows / "ci.yml"
    yaml = ci_path.read_text()
    justfile = (root / "justfile").read_text()
    jobs = jobs_in(yaml)
    if not jobs:
        return ["no jobs parsed from ci.yml"], ""

    # ---- 1. every `just ci` recipe runs somewhere on every PR ---------------
    recipes = ci_recipes(justfile)
    if not recipes:
        problems.append("no `ci:` recipe found in justfile")
    for recipe in recipes:
        if runs_recipe(yaml, recipe):
            continue
        exemption = ALLOWED_WITHOUT_JOB.get(recipe)
        if exemption is None:
            problems.append(
                f"`just {recipe}` is in `just ci` but no job in ci.yml runs it. "
                f"Add a job, or declare it in ALLOWED_WITHOUT_JOB."
            )
            continue
        other = workflows / exemption.get("runs_in", "")
        if not other.is_file():
            problems.append(f"ALLOWED_WITHOUT_JOB[{recipe}] names `{exemption.get('runs_in')}`, which does not exist")
        elif not runs_recipe(other.read_text(), recipe):
            problems.append(f"ALLOWED_WITHOUT_JOB[{recipe}]: `{other.name}` does not run `just {recipe}`")
        elif not runs_on_every_pull_request(other.read_text()):
            problems.append(
                f"ALLOWED_WITHOUT_JOB[{recipe}]: `{other.name}` does not run on every pull request "
                f"(missing trigger or a path filter), so a change to an input it does not list goes unchecked"
            )

    # ---- 2. every job is gated or declared non-blocking ---------------------
    gated_count = 0
    if AGGREGATOR not in jobs:
        problems.append(f"no `{AGGREGATOR}` job in ci.yml: there is no single required status check")
    else:
        agg = jobs[AGGREGATOR]
        needed = needs_of(agg)
        for job in sorted(set(jobs) - {AGGREGATOR}):
            declared = job in NON_BLOCKING_JOBS
            if job in needed and declared:
                problems.append(f"job `{job}` is declared non-blocking but is in `{AGGREGATOR}`'s needs: it can fail `CI`")
            elif job not in needed and not declared:
                problems.append(f"job `{job}` is not in `{AGGREGATOR}`'s needs and not declared non-blocking")
            if job in needed and re.search(r"^    continue-on-error:\s*true\s*$", jobs[job], re.M):
                problems.append(f"job `{job}` is gated but has `continue-on-error: true`, so it cannot fail `CI`")
        for ghost in sorted(needed - set(jobs)):
            problems.append(f"`{AGGREGATOR}` needs `{ghost}`, which is not a job in ci.yml")
        for job in sorted(NON_BLOCKING_JOBS):
            if job not in jobs:
                problems.append(f"NON_BLOCKING_JOBS declares `{job}`, which is not a job in ci.yml")
                continue
            for other, block in jobs.items():
                if other in needed and job in needs_of(block):
                    problems.append(
                        f"non-blocking job `{job}` is needed by gated job `{other}`: its failure skips "
                        f"`{other}` and fails `CI`"
                    )
        # ---- 3. jobs that must gate ------------------------------------------
        for job, why in MUST_BLOCK.items():
            if job not in jobs:
                problems.append(f"`{job}` must exist and gate `CI` ({why}), but ci.yml has no such job")
            elif job not in needed or job in NON_BLOCKING_JOBS:
                problems.append(f"`{job}` must gate `CI` ({why})")
        # ---- 5. the aggregator itself ----------------------------------------
        if not re.search(r"^    if:\s*always\(\)\s*$", agg, re.M):
            problems.append(
                f"`{AGGREGATOR}` lacks `if: always()`: it is skipped when a dependency fails, "
                f"and GitHub reads a skipped required check as success"
            )
        if "needs.*.result" not in agg:
            problems.append(f"`{AGGREGATOR}` does not inspect `needs.*.result`, so a failed job cannot fail it")
        gated_count = len(needed & set(jobs))

    # ---- 4. every workflow declared ------------------------------------------
    present = {p.name for p in workflows.glob("*.y*ml")} - {"ci.yml"}
    for name in sorted(present - set(WORKFLOWS)):
        problems.append(f"workflow `{name}` is not declared in WORKFLOWS (non-blocking check or not a check, with a reason)")
    for name in sorted(set(WORKFLOWS) - present):
        problems.append(f"WORKFLOWS declares `{name}`, which does not exist")
    for name, (cls, reason) in WORKFLOWS.items():
        if cls not in CLASSES or not reason.strip():
            problems.append(f"WORKFLOWS[{name}] needs a class in {sorted(CLASSES)} and a reason")

    summary = (
        f"{gated_count} jobs gated by `{AGGREGATOR}`, {len(NON_BLOCKING_JOBS)} non-blocking, "
        f"{len(WORKFLOWS)} other workflows declared; every `just ci` recipe has a job"
    )
    return problems, summary


def main() -> int:
    if "--self-test" in sys.argv:
        return self_test()
    problems, summary = check(ROOT)
    if problems:
        print("✗ CI job parity", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        return 1
    print(f"✓ CI job parity: {summary}")
    return 0


# ---------------------------------------------------------------------------
# Negative controls: each plant in a scratch copy must produce a failure.
# ---------------------------------------------------------------------------

def _drop_need(yaml: str, job: str) -> str:
    return re.sub(rf"^      - {re.escape(job)}\n", "", yaml, count=1, flags=re.M)


def self_test() -> int:
    global ALLOWED_WITHOUT_JOB, NON_BLOCKING_JOBS
    plants = {
        "job neither gated nor declared": (
            lambda y: _drop_need(y, "clippy"), None, None, "`clippy` is not in",
        ),
        "declared non-blocking job absent": (
            None, None, {"ghost-job": "x"}, "`ghost-job`, which is not a job",
        ),
        "declared non-blocking job gates CI": (
            None, None, {"clippy": "x"},
            "`clippy` is declared non-blocking but is in",
        ),
        "gated job wired not to block": (
            lambda y: y.replace("  msrv:\n    name: MSRV (1.95)\n", "  msrv:\n    name: MSRV (1.95)\n    continue-on-error: true\n", 1),
            None, None, "`msrv` is gated but has `continue-on-error: true`",
        ),
        "non-blocking job needed by a gated job": (
            lambda y: _drop_need(y, "clippy").replace("  msrv:\n    name: MSRV (1.95)\n", "  msrv:\n    name: MSRV (1.95)\n    needs: clippy\n", 1),
            None, {"clippy": "x"}, "needed by gated job `msrv`",
        ),
        "pinned Redpanda job dropped from needs": (
            lambda y: _drop_need(y, "integration-redpanda"), None, {**NON_BLOCKING_JOBS, "integration-redpanda": "x"},
            "`integration-redpanda` must gate",
        ),
        "SASL job removed": (
            lambda y: _drop_need(y, "integration-sasl"), None, None, "`integration-sasl` must gate",
        ),
        "undeclared workflow file": (
            None, ("extra.yml", "name: x\non: push\njobs: {}\n"), None, "workflow `extra.yml` is not declared",
        ),
        "declared workflow missing": (
            None, ("-mutants-weekly.yml", ""), None, "`mutants-weekly.yml`, which does not exist",
        ),
        "recipe in `just ci` with no job": (
            lambda y: y.replace("      - run: just no-c\n", "", 1), None, None, "`just no-c` is in `just ci`",
        ),
        "aggregator without if: always()": (
            lambda y: y.replace("    if: always()\n", "", 1), None, None, "lacks `if: always()`",
        ),
        "site-check exemption whose workflow is path-filtered": (
            lambda y: y.replace("      - run: just site-check\n", "", 1),
            ("pages-check.yml", "on:\n  pull_request:\n    paths:\n      - 'site/**'\njobs:\n  b:\n    steps:\n      - run: just site-check\n"),
            "exempt-site-check", "does not run on every pull request",
        ),
    }
    failures = 0
    saved = (dict(ALLOWED_WITHOUT_JOB), dict(NON_BLOCKING_JOBS))
    for name, (edit_ci, extra_file, override, expect) in plants.items():
        ALLOWED_WITHOUT_JOB, NON_BLOCKING_JOBS = dict(saved[0]), dict(saved[1])
        with tempfile.TemporaryDirectory() as tmp:
            copy = Path(tmp)
            shutil.copytree(ROOT / ".github", copy / ".github")
            shutil.copy(ROOT / "justfile", copy / "justfile")
            ci = copy / ".github" / "workflows" / "ci.yml"
            if edit_ci:
                before = ci.read_text()
                after = edit_ci(before)
                if after == before:
                    print(f"✗ plant `{name}`: the edit did not apply", file=sys.stderr)
                    failures += 1
                    continue
                ci.write_text(after)
            if extra_file:
                fname, content = extra_file
                if fname.startswith("-"):
                    (copy / ".github" / "workflows" / fname[1:]).unlink()
                else:
                    (copy / ".github" / "workflows" / fname).write_text(content)
                    if override == "exempt-site-check":
                        WORKFLOWS[fname] = ("not-a-check", "plant")
            if override == "exempt-site-check":
                ALLOWED_WITHOUT_JOB = {"site-check": {"runs_in": "pages-check.yml", "reason": "plant"}}
            elif isinstance(override, dict):
                NON_BLOCKING_JOBS = override
            problems, _ = check(copy)
            WORKFLOWS.pop("pages-check.yml", None)
            hit = [p for p in problems if expect in p]
            if hit:
                print(f"✓ plant `{name}`: {hit[0]}")
            else:
                print(f"✗ plant `{name}`: NOT detected (got {problems})", file=sys.stderr)
                failures += 1
    ALLOWED_WITHOUT_JOB, NON_BLOCKING_JOBS = saved
    problems, summary = check(ROOT)
    if problems:
        print(f"✗ the real tree fails: {problems}", file=sys.stderr)
        failures += 1
    else:
        print(f"✓ real tree: {summary}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
