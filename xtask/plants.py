#!/usr/bin/env python3
"""The simulation and the cancel-safety harness find every defect planted back.

Each patch in tests/plants/ reverts one fix or breaks one guard. For each,
this applies the patch to a scratch copy of the tree, runs the test that must
catch it, and checks that the test fails the way PLANTS says. A simulation
plant must also replay: its first failing seed, run alone, reports the same
violations. The check fails when a patch no longer applies (regenerate it
against the current code), when the planted tree passes (the harness lost the
ability to find that defect), or when a seed does not replay.

Simulation plants, each a confirmed defect class: P1 (a failed batch's
sequence range reused), P3 (`flush()` returning before an earlier send has an
outcome), T1 (a commit after a failed send), T2 (an unanswered commit reported
as a definite failure), F2 (under TV1, no epoch bump after an unanswered
`EndTxn`), and a send-engine busy loop the simulation found.

Cancel-safety plants: the send path's drop guard not reporting a cancelled
send, an `enqueue` that appends before its last await, a consumer `poll` that
moves the position before returning, a share `poll` that takes records before
its last await, the pending `AddPartitionsToTxn` rollback removed, and a
dropped `commit`/`abort` leaving the transaction wedged.

The tree itself is never modified.

Run: python3 xtask/plants.py [plant ...]
     KRAFKA_SIM_SEEDS=0..400 python3 xtask/plants.py
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PATCHES = ROOT / "tests" / "plants"

DEFAULT_SEEDS = "0..200"


@dataclass(frozen=True)
class Plant:
    suite: str  # the test target
    test: str  # the test that must fail
    expect: str  # text its failure must contain
    seeds: str | None = None  # simulation seed range, when not the default


PLANTS = {
    "p1": Plant("simulation", "idempotent_producer", "[lost acknowledged write]"),
    "p3": Plant("simulation", "concurrent_producer", "[flush/close completeness]"),
    "t1": Plant("simulation", "transactions_tv2", "[partial commit]"),
    "t2": Plant("simulation", "transactions_tv1", "[partial commit]"),
    "f2": Plant("simulation", "transactions_tv1", "[partial commit]"),
    # A busy loop stops the simulated clock and is caught by the wall-clock
    # limit; each failing seed costs the whole limit, so one seed.
    "spin": Plant("simulation", "concurrent_producer", "[flush/close completeness]", "8..9"),
    "send-obligation": Plant("cancel_safety", "producer_enqueue", "on_acknowledgement"),
    "enqueue-append": Plant("cancel_safety", "producer_enqueue", "enqueued: false"),
    "consumer-position": Plant("cancel_safety", "consumer_poll", "passed records not returned"),
    "share-poll": Plant("cancel_safety", "share_poll", "ShareConsumer::poll: the caller's next steps hang"),
    "pending-add": Plant("cancel_safety", "transactional_send", "hang"),
    "txn-ending": Plant("cancel_safety", "transactional_commit", "cannot commit in state Committing"),
}


def cargo_test(copy: Path, env: dict[str, str], plant: Plant) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["cargo", "test", "--quiet", "--features", "test-broker", "--test", plant.suite,
         "--", "--exact", plant.test, "--nocapture"],
        cwd=copy,
        env=env,
        capture_output=True,
        text=True,
    )


def violations(output: str, seed: str) -> list[str]:
    """The violation lines a simulation reported for `seed`."""
    lines = output.splitlines()
    try:
        start = lines.index(f"seed {seed}:")
    except ValueError:
        return []
    found = []
    for line in lines[start + 1 :]:
        if not line.startswith("  ["):
            break
        found.append(line.strip())
    return found


def run_plant(name: str, target_dir: Path, seeds: str) -> str | None:
    """Return None when the plant is caught, else why not."""
    plant = PLANTS[name]
    patch = PATCHES / f"{name}.patch"
    with tempfile.TemporaryDirectory(prefix=f"krafka-plant-{name}-") as tmp:
        copy = Path(tmp) / "tree"
        shutil.copytree(
            ROOT,
            copy,
            ignore=shutil.ignore_patterns("target", ".git", ".claude", "fuzz"),
        )
        applied = subprocess.run(
            ["patch", "-p1", "--forward", "--silent", "-i", str(patch)],
            cwd=copy,
            capture_output=True,
            text=True,
        )
        if applied.returncode != 0:
            return f"{patch.name} no longer applies; regenerate it:\n{applied.stdout}{applied.stderr}"
        env = dict(os.environ)
        env["CARGO_TARGET_DIR"] = str(target_dir)
        # A plant is a defect, not polished code: its warnings must not stop
        # the build.
        env["RUSTFLAGS"] = "--cfg tokio_unstable"
        env.pop("KRAFKA_SIM_SEED", None)
        env["KRAFKA_SIM_SEEDS"] = plant.seeds or seeds
        result = cargo_test(copy, env, plant)
        output = result.stdout + result.stderr
        if result.returncode == 0:
            return f"{plant.suite}::{plant.test} passed with {name} planted"
        if plant.expect not in output:
            tail = "\n".join(output.splitlines()[-30:])
            return f"{plant.test} failed, but not with `{plant.expect}`:\n{tail}"
        if plant.suite != "simulation":
            print(f"✓ {name}: {plant.suite}::{plant.test} fails with `{plant.expect}`")
            return None
        failing = re.findall(r"^seed (\d+):", output, re.MULTILINE)
        first = failing[0]
        env.pop("KRAFKA_SIM_SEEDS")
        env["KRAFKA_SIM_SEED"] = first
        replay = cargo_test(copy, env, plant)
        if violations(replay.stdout + replay.stderr, first) != violations(output, first):
            return f"seed {first} does not replay to the same violations"
        print(
            f"✓ {name}: {plant.test} reports {plant.expect} for seeds {', '.join(failing)}; "
            f"seed {first} replays"
        )
        return None


def main() -> int:
    names = sys.argv[1:] or list(PLANTS)
    unknown = [n for n in names if n not in PLANTS]
    if unknown:
        print(f"unknown plant(s): {unknown}; known: {list(PLANTS)}", file=sys.stderr)
        return 2
    patches = {p.stem for p in PATCHES.glob("*.patch")}
    if strays := sorted(patches - set(PLANTS)):
        print(f"patches without an entry in PLANTS: {strays}", file=sys.stderr)
        return 2
    seeds = os.environ.get("KRAFKA_SIM_SEEDS", DEFAULT_SEEDS)
    target_dir = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")) / "plants"
    failures = []
    for name in names:
        print(f"▶ {name}: {PLANTS[name].suite}::{PLANTS[name].test}")
        if problem := run_plant(name, target_dir, seeds):
            failures.append(f"✗ {name}: {problem}")
    if failures:
        print("\n".join(failures), file=sys.stderr)
        return 1
    print(f"✓ every planted defect is caught ({len(names)})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
