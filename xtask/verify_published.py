#!/usr/bin/env python3
"""crates.io serves exactly the `.crate` this repository built and attested.

Compares up to four sha256 digests of `krafka-<version>.crate` and fails
unless all agree, naming the two that disagree:

  * local    — the `.crate` given on the command line (the release artifact);
  * attested — the subject digest of its provenance attestation (`--attested`);
  * served   — the file https://static.crates.io serves for that version;
  * index    — the `cksum` the crates.io sparse index records for that version.

The CDN and the index can lag a publish, so the download and the index read
are retried for a bounded time (`--wait`, default 300 s) before failing. A
failure after publishing is an alarm to yank and investigate: the version is
already permanent.

Run: python3 xtask/verify_published.py --version 0.26.0 --crate krafka-0.26.0.crate
         [--attested <sha256>] [--index-version <v>] [--wait <seconds>]
     python3 xtask/verify_published.py --self-test   # positive and planted controls (network)
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

CRATE = "krafka"
STATIC = "https://static.crates.io/crates/{name}/{name}-{version}.crate"
INDEX = "https://index.crates.io/{a}/{b}/{name}"
HEADERS = {"User-Agent": "krafka-release-verify (https://github.com/hupe1980/krafka)"}


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def get(url: str) -> bytes:
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=60) as resp:  # noqa: S310 (fixed https URLs)
        return resp.read()


def with_retry(what: str, fn, wait: float):
    deadline = time.monotonic() + wait
    delay = 5.0
    while True:
        try:
            return fn()
        except (urllib.error.URLError, LookupError) as e:
            if time.monotonic() + delay > deadline:
                raise SystemExit(f"✗ {what}: still unavailable after {wait:.0f} s ({e})")
            print(f"  {what} not available yet ({e}); retrying in {delay:.0f} s", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 60.0)


def served_digest(version: str, wait: float) -> str:
    url = STATIC.format(name=CRATE, version=version)
    return sha256(with_retry(f"download {url}", lambda: get(url), wait))


def index_cksum(version: str, wait: float) -> str:
    name = CRATE.lower()
    url = INDEX.format(a=name[:2], b=name[2:4], name=name)

    def read() -> str:
        for line in get(url).decode().splitlines():
            if line.strip():
                entry = json.loads(line)
                if entry.get("vers") == version:
                    return entry["cksum"]
        raise LookupError(f"version {version} not in the index yet")

    return with_retry(f"index entry for {version}", read, wait)


def compare(digests: dict[str, str]) -> list[str]:
    """Every pair that disagrees, named."""
    names = list(digests)
    return [
        f"{a} {digests[a]} != {b} {digests[b]}"
        for i, a in enumerate(names)
        for b in names[i + 1:]
        if digests[a] != digests[b]
    ]


def verify(version: str, local: Path, attested: str | None, index_version: str | None, wait: float) -> list[str]:
    digests = {"local": sha256(local.read_bytes())}
    if attested:
        digests["attested"] = attested.removeprefix("sha256:").lower()
    digests["served"] = served_digest(version, wait)
    digests["index"] = index_cksum(index_version or version, wait)
    for name, value in digests.items():
        print(f"  {name:<9}{value}")
    return compare(digests)


def main() -> int:
    if "--self-test" in sys.argv:
        return self_test()
    ap = argparse.ArgumentParser()
    ap.add_argument("--version", required=True)
    ap.add_argument("--crate", required=True, type=Path)
    ap.add_argument("--attested")
    ap.add_argument("--index-version", help="read the index cksum of another version (negative control)")
    ap.add_argument("--wait", type=float, default=300.0)
    args = ap.parse_args()
    print(f"▶ {CRATE} {args.version}")
    mismatches = verify(args.version, args.crate, args.attested, args.index_version, args.wait)
    if mismatches:
        print("✗ published crate does not match:", file=sys.stderr)
        for m in mismatches:
            print(f"  {m}", file=sys.stderr)
        return 1
    print("✓ served, index and local digests agree" + (" with the attestation" if args.attested else ""))
    return 0


# ---------------------------------------------------------------------------
# Controls: the published 0.26.0 against `cargo package` of the commit its tag
# names (positive), and four planted violations.
# ---------------------------------------------------------------------------

REFERENCE_VERSION = "0.26.0"
REFERENCE_COMMIT = "532d781022fb"


def package_at(commit: str, workdir: Path, recommit: bool) -> Path:
    """`cargo package --no-verify` of `commit`, optionally re-committed under another SHA."""
    import subprocess

    root = Path(__file__).resolve().parent.parent
    clone = workdir / ("recommitted" if recommit else "tagged")
    env_id = {"GIT_AUTHOR_NAME": "x", "GIT_AUTHOR_EMAIL": "x@x", "GIT_COMMITTER_NAME": "x", "GIT_COMMITTER_EMAIL": "x@x"}
    import os

    env = {**os.environ, **env_id}
    subprocess.run(["git", "clone", "-q", "--no-checkout", str(root), str(clone)], check=True)
    subprocess.run(["git", "checkout", "-q", "--detach", commit], cwd=clone, check=True)
    if recommit:
        subprocess.run(["git", "commit", "-q", "--allow-empty", "--amend", "-m", "same tree, other commit"], cwd=clone, check=True, env=env)
    subprocess.run(
        ["cargo", "package", "--no-verify", "--locked", "--quiet", "--target-dir", str(workdir / f"target-{clone.name}")],
        cwd=clone, check=True,
    )
    return workdir / f"target-{clone.name}" / "package" / f"{CRATE}-{REFERENCE_VERSION}.crate"


def self_test() -> int:
    import tempfile

    failures = 0

    def expect(name: str, mismatches: list[str], want_ok: bool) -> None:
        nonlocal failures
        ok = not mismatches
        failures += ok != want_ok
        detail = "all digests agree" if ok else mismatches[0]
        print(f"{'✓' if ok == want_ok else '✗'} {name}: {'pass' if ok else 'fail'} — {detail}")

    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        tagged = package_at(REFERENCE_COMMIT, work, recommit=False)
        recommitted = package_at(REFERENCE_COMMIT, work, recommit=True)
        good = sha256(tagged.read_bytes())

        expect("positive: published 0.26.0 vs repackage of 532d781", verify(REFERENCE_VERSION, tagged, good, None, 30), True)

        flipped = work / "flipped.crate"
        data = bytearray(tagged.read_bytes())
        data[len(data) // 2] ^= 0x01
        flipped.write_bytes(bytes(data))
        expect("plant: local .crate with one byte changed", verify(REFERENCE_VERSION, flipped, None, None, 30), False)
        expect("plant: same tree packaged from a different commit", verify(REFERENCE_VERSION, recommitted, None, None, 30), False)
        expect("plant: index cksum read for a different version", verify(REFERENCE_VERSION, tagged, None, "0.25.0", 30), False)
        expect("plant: attestation subject digest differs", verify(REFERENCE_VERSION, tagged, "0" * 64, None, 30), False)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
