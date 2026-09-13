#!/usr/bin/env python3
"""Fail when a benchmark regresses against the baseline.

# Why this exists

krafka is a performance-shaped product, and until this gate there was no
throughput or latency measurement anywhere in it: a change that halved producer
throughput passed every check in the repository.

The absence was reasoned, not careless. `site/content/docs/performance.md` sets
a high bar for a credible *public* benchmark — real broker, published hardware
and configuration on both sides, raw artifacts, verified histograms — and the
in-process fake broker meets none of it: single lock, in-memory log. franz-go
withdrew its own "4x faster" claim; publishing a number from a bad harness is
worse than publishing none.

That reasoning is about *comparison*, and it does not extend to *regression*:

    A regression gate compares krafka against krafka.

The harness contributes a large constant, and a constant cancels when you
subtract two runs of it. A harness that measures throughput badly still detects
a change that doubled the client's own work.

So nothing this produces is quotable, and none of it should reach the README.
What it does is fail a build that got slower.

# How it works

Criterion stores the relative change between the current run and the previous
one, with a confidence interval, in
`target/criterion/<group>/<id>/change/estimates.json`. This fails when a mean
regression exceeds THRESHOLD *and* the interval excludes zero, so noise alone
cannot fail a build.

    just bench-baseline     # record the reference
    just bench-check        # re-run and compare

A first run has nothing to compare against and passes with a notice.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CRITERION = ROOT / "target" / "criterion"

# A mean slowdown beyond this fails the build.
#
# 10% is loose on purpose. These benchmarks run against a lock-bound in-memory
# fake broker on whatever machine CI happened to allocate, so the noise floor is
# real. The gate exists to catch a 2× regression in the accumulator, not to
# police 3% drift — and a gate that cries wolf is a gate people start passing
# with `--no-verify`.
THRESHOLD = 0.10


def main() -> int:
    if not CRITERION.is_dir():
        print(
            "⊘ No criterion data. Run `just bench-baseline` first.",
            file=sys.stderr,
        )
        return 0

    regressions: list[tuple[str, float, float, float]] = []
    improvements: list[tuple[str, float]] = []
    compared = 0

    for estimates in sorted(CRITERION.glob("**/change/estimates.json")):
        name = str(estimates.parent.parent.relative_to(CRITERION))
        try:
            mean = json.loads(estimates.read_text())["mean"]
        except (OSError, KeyError, json.JSONDecodeError):
            continue

        compared += 1
        point = mean["point_estimate"]
        lower = mean["confidence_interval"]["lower_bound"]
        upper = mean["confidence_interval"]["upper_bound"]

        # Regression only when the whole interval is on the slow side: a
        # point estimate above the threshold whose interval still straddles
        # zero is noise, and failing on it teaches people to ignore this.
        if point > THRESHOLD and lower > 0:
            regressions.append((name, point, lower, upper))
        elif point < -THRESHOLD and upper < 0:
            improvements.append((name, point))

    if compared == 0:
        print(
            "⊘ No comparisons yet — criterion needs two runs. "
            "Run `just bench-baseline`, then `just bench-check`."
        )
        return 0

    for name, point in improvements:
        print(f"  ↓ {name}: {point:+.1%} (faster)")

    if regressions:
        print(
            f"\n✗ Benchmark regression: {len(regressions)} of {compared} "
            f"measurements slower by more than {THRESHOLD:.0%}\n",
            file=sys.stderr,
        )
        for name, point, lower, upper in regressions:
            print(
                f"  - {name}: {point:+.1%} "
                f"(95% CI {lower:+.1%} … {upper:+.1%})",
                file=sys.stderr,
            )
        print(
            "\nThese are krafka-vs-krafka numbers from a fake broker: they are "
            "not quotable\nas absolute performance, but a change this size is "
            "real. Investigate, or\nre-baseline deliberately with "
            "`just bench-baseline` if the cost is intended.",
            file=sys.stderr,
        )
        return 1

    print(
        f"✓ Benchmark check: {compared} measurements, none slower than "
        f"{THRESHOLD:.0%} vs the previous run"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
