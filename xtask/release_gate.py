#!/usr/bin/env python3
"""A release may proceed only from a green, tagged commit on `main`.

Fails unless all hold:

  * the ref is a tag `v<version>` and `<version>` equals Cargo.toml's;
  * the tagged commit is reachable from `origin/main`;
  * the most recent `CI` check run on that commit concluded `success`.

It fails closed: a check run that is queued, in progress, failed, cancelled,
skipped, timed out or absent stops the release, and the message names which.
Re-running the release after `CI` turns green proceeds; nothing polls.

Run (GitHub Actions supplies these):
  GITHUB_REF=refs/tags/v0.27.0 GITHUB_SHA=<sha> GITHUB_REPOSITORY=owner/repo \\
  GITHUB_TOKEN=<token> python3 xtask/release_gate.py
  python3 xtask/release_gate.py --check-runs <file.json>   # offline, from a saved API answer
  python3 xtask/release_gate.py --self-test                # the planted-state controls
"""

from __future__ import annotations

import json
import os
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
CHECK_NAME = "CI"  # the `ci-success` job's name: the single required check


def latest_run(check_runs: dict) -> dict | None:
    runs = [r for r in check_runs.get("check_runs", []) if r.get("name") == CHECK_NAME]
    if not runs:
        return None
    return max(runs, key=lambda r: (r.get("started_at") or "", r.get("id") or 0))


def ci_verdict(check_runs: dict) -> tuple[bool, str]:
    run = latest_run(check_runs)
    if run is None:
        return False, f"no `{CHECK_NAME}` check run on this commit: CI never ran"
    if run.get("status") != "completed":
        return False, f"`{CHECK_NAME}` has not concluded (status `{run.get('status')}`); re-run the release once it is green"
    if run.get("conclusion") != "success":
        return False, f"`{CHECK_NAME}` concluded `{run.get('conclusion')}`"
    return True, f"`{CHECK_NAME}` concluded `success` ({run.get('html_url', 'check run')})"


def tag_verdict(ref: str, cargo_version: str) -> tuple[bool, str]:
    m = re.fullmatch(r"refs/tags/v(.+)", ref)
    if not m:
        return False, f"`{ref}` is not a `v*` tag"
    if m.group(1) != cargo_version:
        return False, f"tag version {m.group(1)} differs from Cargo.toml's {cargo_version}"
    return True, f"tag v{cargo_version} matches Cargo.toml"


def on_main_verdict(sha: str, main_ref: str = "origin/main", repo: Path = ROOT) -> tuple[bool, str]:
    r = subprocess.run(["git", "merge-base", "--is-ancestor", sha, main_ref], cwd=repo)
    if r.returncode == 0:
        return True, f"{sha[:12]} is reachable from {main_ref}"
    if r.returncode == 1:
        return False, f"{sha[:12]} is not reachable from {main_ref}"
    return False, f"could not determine whether {sha[:12]} is on {main_ref} (git exit {r.returncode})"


def fetch_check_runs(repo: str, sha: str, token: str) -> dict:
    url = f"https://api.github.com/repos/{repo}/commits/{sha}/check-runs?check_name={CHECK_NAME}&filter=all&per_page=100"
    req = urllib.request.Request(
        url,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310 (fixed https URL)
        return json.load(resp)


def cargo_version() -> str:
    return tomllib.loads((ROOT / "Cargo.toml").read_text())["package"]["version"]


def main() -> int:
    if "--self-test" in sys.argv:
        return self_test()
    ref = os.environ.get("GITHUB_REF", "")
    sha = os.environ.get("GITHUB_SHA", "")
    if "--check-runs" in sys.argv:
        runs = json.loads(Path(sys.argv[sys.argv.index("--check-runs") + 1]).read_text())
    else:
        runs = fetch_check_runs(os.environ["GITHUB_REPOSITORY"], sha, os.environ["GITHUB_TOKEN"])

    verdicts = [tag_verdict(ref, cargo_version()), on_main_verdict(sha), ci_verdict(runs)]
    for ok, why in verdicts:
        print(("✓ " if ok else "✗ ") + why)
    if all(ok for ok, _ in verdicts):
        print("✓ release gate: proceed")
        return 0
    print("✗ release gate: stop before packaging", file=sys.stderr)
    return 1


def self_test() -> int:
    def run(conclusion: str | None, status: str = "completed", started: str = "2026-10-08T10:00:00Z") -> dict:
        return {"name": CHECK_NAME, "status": status, "conclusion": conclusion, "started_at": started, "id": 1}

    cases = [
        ("(a) CI passed", {"check_runs": [run("success")]}, True),
        ("(b) CI failed", {"check_runs": [run("failure")]}, False),
        ("(b) CI cancelled", {"check_runs": [run("cancelled")]}, False),
        ("(b) CI skipped", {"check_runs": [run("skipped")]}, False),
        ("(d) CI still running", {"check_runs": [run(None, status="in_progress")]}, False),
        ("CI never ran", {"check_runs": []}, False),
        ("re-run green after a failure", {"check_runs": [run("failure"), {**run("success", started="2026-10-08T11:00:00Z"), "id": 2}]}, True),
        ("re-run red after a success", {"check_runs": [run("success"), {**run("failure", started="2026-10-08T11:00:00Z"), "id": 2}]}, False),
    ]
    failures = 0
    for name, runs, want in cases:
        ok, why = ci_verdict(runs)
        failures += ok != want
        print(f"{'✓' if ok == want else '✗'} {name}: {'proceed' if ok else 'stop'} — {why}")
    version = cargo_version()
    for name, ref, want in [
        ("tag matches Cargo.toml", f"refs/tags/v{version}", True),
        ("tag differs from Cargo.toml", "refs/tags/v99.0.0", False),
        ("a branch, not a tag", "refs/heads/main", False),
    ]:
        ok, why = tag_verdict(ref, version)
        failures += ok != want
        print(f"{'✓' if ok == want else '✗'} {name}: {'proceed' if ok else 'stop'} — {why}")
    # (c) not on main, in a scratch repository: `main` and a side branch.
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        env = {**os.environ, "GIT_AUTHOR_NAME": "x", "GIT_AUTHOR_EMAIL": "x@x", "GIT_COMMITTER_NAME": "x", "GIT_COMMITTER_EMAIL": "x@x"}

        def git(*args: str) -> str:
            return subprocess.run(["git", *args], cwd=tmp, env=env, capture_output=True, text=True, check=True).stdout.strip()

        git("init", "-q", "-b", "main")
        git("commit", "-q", "--allow-empty", "-m", "on main")
        on_main = git("rev-parse", "HEAD")
        git("checkout", "-q", "-b", "side")
        git("commit", "-q", "--allow-empty", "-m", "off main")
        off_main = git("rev-parse", "HEAD")
        for name, sha, want in [("(c) commit not on main", off_main, False), ("commit on main", on_main, True)]:
            ok, why = on_main_verdict(sha, "main", Path(tmp))
            failures += ok != want
            print(f"{'✓' if ok == want else '✗'} {name}: {'proceed' if ok else 'stop'} — {why}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
