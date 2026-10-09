#!/usr/bin/env python3
"""Fail when a benchmark regresses against the recorded baseline.

# Why this exists

A regression gate compares krafka against krafka. The fake broker these
benchmarks run against is a poor harness for an absolute or comparative
figure, but its overhead is a constant, and a constant cancels when two runs
of it are subtracted. So nothing this produces is quotable, and none of it
belongs in the documentation; what it does is fail a build that got slower.

# How it works

    just bench-baseline     # save the reference as the named baseline
    just bench-check        # re-run and compare against that baseline

`baseline` runs the gated benchmarks with criterion's `--save-baseline`;
`check` runs them with `--baseline`, which compares against the saved
reference without replacing it, so a regression fails every check until it
is fixed or deliberately re-baselined.

Only measurements the current run produced are judged: a benchmark counts
when its `new/benchmark.json` was written by this run, and its
`change/estimates.json` must have been written by this run too. Output left
by `just bench`, another baseline, or a benchmark that no longer exists can
neither fail nor pass the check.

A measurement fails when its mean slowed by more than THRESHOLD and the 95%
confidence interval excludes zero, so noise alone cannot fail a build.

Run: python3 xtask/bench_check.py baseline
     python3 xtask/bench_check.py check
     python3 xtask/bench_check.py --self-test   # the planted-input controls
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# The benchmarks the gate runs: end-to-end send and consume paths against the
# fake broker.
BENCHES = ("send_path", "consume_path")
FEATURES = "test-broker"
BASELINE = "gate"
CRITERION_ARGS = ("--warm-up-time", "1", "--measurement-time", "5")

# A mean slowdown beyond this fails the build.
#
# 10% is loose on purpose: the harness is a lock-bound in-memory broker on
# whatever machine runs it, so the noise floor is real. The gate exists to
# catch a 2× regression, not 3% drift.
THRESHOLD = 0.10


def criterion_dir() -> Path:
    target = os.environ.get("CARGO_TARGET_DIR")
    return (Path(target) if target else ROOT / "target") / "criterion"


def run_benches(mode_flag: str) -> float:
    """Run the gated benchmarks; return the wall-clock time they started at."""
    cmd = ["cargo", "bench", "--features", FEATURES]
    for bench in BENCHES:
        cmd += ["--bench", bench]
    cmd += ["--", mode_flag, BASELINE, *CRITERION_ARGS]
    # Criterion stamps files with the filesystem clock; a one-second margin
    # absorbs coarse mtime resolution without admitting an earlier run.
    started = time.time() - 1
    subprocess.run(cmd, cwd=ROOT, check=True)
    return started


def judge(criterion: Path, since: float) -> int:
    """Judge the measurements written at or after `since`."""
    regressions: list[tuple[str, float, float, float]] = []
    missing: list[str] = []
    improvements: list[tuple[str, float]] = []
    compared = 0

    for bench_json in sorted(criterion.glob("**/new/benchmark.json")):
        if bench_json.stat().st_mtime < since:
            continue
        case = bench_json.parent.parent
        name = str(case.relative_to(criterion))
        estimates = case / "change" / "estimates.json"
        if not estimates.is_file() or estimates.stat().st_mtime < since:
            missing.append(name)
            continue
        try:
            mean = json.loads(estimates.read_text())["mean"]
            point = mean["point_estimate"]
            lower = mean["confidence_interval"]["lower_bound"]
            upper = mean["confidence_interval"]["upper_bound"]
        except (OSError, KeyError, TypeError, json.JSONDecodeError):
            missing.append(name)
            continue

        compared += 1
        # Regression only when the whole interval is on the slow side: a point
        # estimate above the threshold whose interval straddles zero is noise.
        if point > THRESHOLD and lower > 0:
            regressions.append((name, point, lower, upper))
        elif point < -THRESHOLD and upper < 0:
            improvements.append((name, point))

    if missing:
        print(
            f"✗ No comparison against baseline `{BASELINE}` for: "
            + ", ".join(missing)
            + "\n  Record one with `just bench-baseline` on a known-good commit.",
            file=sys.stderr,
        )
        return 1
    if compared == 0:
        print("✗ This run produced no measurements.", file=sys.stderr)
        return 1

    for name, point in improvements:
        print(f"  ↓ {name}: {point:+.1%} (faster)")

    if regressions:
        print(
            f"\n✗ Benchmark regression: {len(regressions)} of {compared} "
            f"measurements slower than baseline `{BASELINE}` by more than "
            f"{THRESHOLD:.0%}\n",
            file=sys.stderr,
        )
        for name, point, lower, upper in regressions:
            print(
                f"  - {name}: {point:+.1%} (95% CI {lower:+.1%} … {upper:+.1%})",
                file=sys.stderr,
            )
        print(
            "\nThese are krafka-vs-krafka numbers from a fake broker: not quotable,\n"
            "but a change this size is real. Investigate, or re-baseline\n"
            "deliberately with `just bench-baseline` if the cost is intended.",
            file=sys.stderr,
        )
        return 1

    print(
        f"✓ Benchmark check: {compared} measurements, none slower than "
        f"{THRESHOLD:.0%} vs baseline `{BASELINE}`"
    )
    return 0


# ── Self-test --------------------------------------------------------------


def _plant(criterion: Path, case: str, change: float | None, bench_age: float, change_age: float) -> None:
    """Write one case's criterion output; ages are seconds before now."""
    d = criterion / case
    (d / "new").mkdir(parents=True, exist_ok=True)
    bench = d / "new" / "benchmark.json"
    bench.write_text(json.dumps({"full_id": case}))
    now = time.time()
    os.utime(bench, (now - bench_age, now - bench_age))
    if change is not None:
        (d / "change").mkdir(parents=True, exist_ok=True)
        est = d / "change" / "estimates.json"
        ci = {"lower_bound": change - 0.02, "upper_bound": change + 0.02}
        est.write_text(json.dumps({"mean": {"point_estimate": change, "confidence_interval": ci}}))
        os.utime(est, (now - change_age, now - change_age))


def self_test() -> int:
    hour = 3600
    cases = [
        # (description, [(case, change, bench_age, change_age)], expected exit)
        ("unchanged run passes", [("a/x", 0.01, 0, 0)], 0),
        ("2x regression fails", [("a/x", 1.0, 0, 0)], 1),
        ("stale regression of a case not in the run is ignored",
         [("a/x", 0.0, 0, 0), ("gone/y", 1.0, hour, hour)], 0),
        ("measurement without a comparison fails", [("a/x", None, 0, 0)], 1),
        ("stale comparison beside a fresh measurement fails", [("a/x", 0.0, 0, hour)], 1),
        ("a run with no measurements fails", [("old/x", 0.0, hour, hour)], 1),
    ]
    failed = 0
    for desc, plants, expected in cases:
        with tempfile.TemporaryDirectory() as tmp:
            criterion = Path(tmp)
            for plant in plants:
                _plant(criterion, *plant)
            since = time.time() - 60
            devnull = open(os.devnull, "w")
            out, err = sys.stdout, sys.stderr
            sys.stdout = sys.stderr = devnull
            try:
                got = judge(criterion, since)
            finally:
                sys.stdout, sys.stderr = out, err
                devnull.close()
        ok = got == expected
        failed += not ok
        print(f"  {'✓' if ok else '✗'} {desc} (exit {got}, expected {expected})")
    if failed:
        print(f"✗ bench_check self-test: {failed} control(s) misjudged", file=sys.stderr)
        return 1
    print(f"✓ bench_check self-test: {len(cases)} controls judged correctly")
    return 0


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return self_test()
    if argv == ["baseline"]:
        run_benches("--save-baseline")
        print(f"✓ Saved baseline `{BASELINE}` for {', '.join(BENCHES)}")
        return 0
    if argv == ["check"]:
        since = run_benches("--baseline")
        return judge(criterion_dir(), since)
    print(__doc__.split("Run:", 1)[1].rstrip(), file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
