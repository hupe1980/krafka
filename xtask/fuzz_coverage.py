#!/usr/bin/env python3
"""Every (API, version) pair krafka negotiates has a fuzz path, and a seed.

`fuzz_response_decode` takes its versions from `SUPPORTED_API_VERSIONS`, the
runtime form of the `api_versions!` table, so version ranges cannot go stale
in the target. What the table cannot supply is the API → response-type
mapping; that is written in the target, and an API with no arm panics at run
time. This check makes the same gap a build failure.

# What it checks

  1. Every API in `api_versions!`, under the fuzz crate's feature set, has an
     arm in `fuzz_response_decode`'s dispatch; every arm names a table API.
  2. The target draws its versions from `SUPPORTED_API_VERSIONS` and states no
     version range of its own.
  3. Every fuzz target is a `[[bin]]` in `fuzz/Cargo.toml` with a committed,
     non-empty seed directory `fuzz/seeds/<target>/` within the size budget.
  4. `fuzz/seeds/fuzz_response_decode/` holds one seed per (API, version)
     pair, named `<Api>-v<N>`, whose first two bytes select that pair.

Failures name each unreached pair. `--write-seeds` (re)writes the check-4
seeds after a table change.

Run: python3 xtask/fuzz_coverage.py
     python3 xtask/fuzz_coverage.py --write-seeds
     python3 xtask/fuzz_coverage.py --self-test   # the planted-violation controls
"""

from __future__ import annotations

import re
import shutil
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_tomllib  # noqa: E402

ensure_tomllib()

import tomllib  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent

RESPONSE_TARGET = "fuzz_response_decode"
# Committed seeds stay small; the working corpus is not committed.
SEED_BUDGET_BYTES = 256 * 1024
# Body bytes after the two selector bytes in a generated seed.
SEED_BODY = bytes(32)

ROW = re.compile(
    r'"(?P<api>\w+)"\s*\[(?P<key>\d+)\]\s*(?:cfg\((?P<cfg>[^)]*\)?)\))?\s*=>\s*'
    r"\w+\s*=\s*(?P<min>\d+)\s*\.\.=\s*\w+\s*=\s*(?P<max>\d+)"
)


def cfg_active(cfg: str, features: set[str]) -> bool:
    """Evaluate the `feature = "x"` / `not(feature = "x")` gates the table uses."""
    cfg = cfg.strip()
    if not cfg:
        return True
    m = re.fullmatch(r'not\(feature\s*=\s*"([\w-]+)"\)', cfg)
    if m:
        return m.group(1) not in features
    m = re.fullmatch(r'feature\s*=\s*"([\w-]+)"', cfg)
    if m:
        return m.group(1) in features
    raise SystemExit(f"fuzz_coverage: cannot evaluate cfg({cfg}) in api_versions!")


def fuzz_features(root: Path) -> set[str]:
    manifest = tomllib.loads((root / "fuzz/Cargo.toml").read_text())
    return set(manifest["dependencies"]["krafka"].get("features", []))


def table_rows(root: Path) -> list[tuple[str, int, int]]:
    """Active rows, in table order: what `SUPPORTED_API_VERSIONS` holds."""
    source = (root / "src/protocol/mod.rs").read_text()
    body = source[source.index("api_versions! {") :]
    features = fuzz_features(root)
    rows = [
        (m.group("api"), int(m.group("min")), int(m.group("max")))
        for m in ROW.finditer(body)
        if cfg_active(m.group("cfg") or "", features)
    ]
    if not rows:
        raise SystemExit("fuzz_coverage: no rows parsed from api_versions!")
    return rows


def dispatch_arms(target: str) -> set[str]:
    block = re.search(r"dispatch!\s*\{(.*?)\n\s*\}", target, re.S)
    arms = set(re.findall(r"^\s*(\w+)\s*=>", block.group(1), re.M)) if block else set()
    arms |= set(re.findall(r"ApiKey::(\w+)\s*=>", target))
    return arms


def seed_name(api: str, version: int) -> str:
    return f"{api}-v{version}"


def seed_bytes(index: int, row_count: int, min_v: int, max_v: int, version: int) -> bytes:
    # The target selects row `selector % (rows + 4)` and version
    # `min + ver_byte % span`.
    assert index < row_count <= 252
    return bytes([index, version - min_v]) + SEED_BODY


def check(root: Path) -> list[str]:
    errors: list[str] = []
    rows = table_rows(root)
    target_path = root / "fuzz/fuzz_targets" / f"{RESPONSE_TARGET}.rs"
    target = target_path.read_text()

    # 1. Every table API has an arm, every arm a table API.
    arms = dispatch_arms(target)
    apis = {api for api, _, _ in rows}
    for api, lo, hi in rows:
        if api not in arms:
            errors.append(
                f"{api} v{lo}–v{hi}: no fuzz path — add `{api} => {api}Response` "
                f"to the dispatch in {target_path.relative_to(root)}"
            )
    for arm in sorted(arms - apis):
        errors.append(f"{target_path.relative_to(root)}: arm `{arm}` names no api_versions! row")

    # 2. Versions come from the table.
    if "SUPPORTED_API_VERSIONS" not in target:
        errors.append(f"{target_path.relative_to(root)}: does not draw versions from SUPPORTED_API_VERSIONS")
    for m in re.finditer(r"decode_versioned\(\s*(\d+)|\b\d+\s*\.\.=\s*\d+\s*=>.*decode_versioned", target):
        errors.append(f"{target_path.relative_to(root)}: hand-written version `{m.group(0)}`")

    # 3. Every target is a bin with seeds.
    manifest = tomllib.loads((root / "fuzz/Cargo.toml").read_text())
    bins = {b["name"] for b in manifest.get("bin", [])}
    for path in sorted((root / "fuzz/fuzz_targets").glob("*.rs")):
        name = path.stem
        if name not in bins:
            errors.append(f"fuzz/fuzz_targets/{name}.rs: no [[bin]] in fuzz/Cargo.toml")
        seeds = root / "fuzz/seeds" / name
        files = [f for f in seeds.glob("*") if f.is_file()] if seeds.is_dir() else []
        if not files:
            errors.append(f"fuzz/seeds/{name}/: no committed seeds")
        size = sum(f.stat().st_size for f in files)
        if size > SEED_BUDGET_BYTES:
            errors.append(f"fuzz/seeds/{name}/: {size} bytes, over the {SEED_BUDGET_BYTES}-byte budget")

    # 4. One seed per (API, version).
    seeds = root / "fuzz/seeds" / RESPONSE_TARGET
    for index, (api, lo, hi) in enumerate(rows):
        for version in range(lo, hi + 1):
            path = seeds / seed_name(api, version)
            want = seed_bytes(index, len(rows), lo, hi, version)[:2]
            if not path.is_file():
                errors.append(f"{api} v{version}: no seed {path.relative_to(root)} (run --write-seeds)")
            elif path.read_bytes()[:2] != want:
                errors.append(f"{path.relative_to(root)}: selects another pair (run --write-seeds)")
    return errors


def write_seeds(root: Path) -> int:
    rows = table_rows(root)
    seeds = root / "fuzz/seeds" / RESPONSE_TARGET
    keep = set()
    for index, (api, lo, hi) in enumerate(rows):
        for version in range(lo, hi + 1):
            name = seed_name(api, version)
            keep.add(name)
            (seeds / name).parent.mkdir(parents=True, exist_ok=True)
            (seeds / name).write_bytes(seed_bytes(index, len(rows), lo, hi, version))
    for stale in seeds.glob("*-v*"):
        if stale.name not in keep:
            stale.unlink()
    print(f"✓ wrote {len(keep)} seeds to {seeds.relative_to(root)}")
    return 0


# ── Self-test --------------------------------------------------------------


def _copy(tmp: Path) -> Path:
    root = tmp / "repo"
    for rel in ("fuzz/Cargo.toml", "src/protocol/mod.rs"):
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / rel, root / rel)
    shutil.copytree(ROOT / "fuzz/fuzz_targets", root / "fuzz/fuzz_targets")
    shutil.copytree(ROOT / "fuzz/seeds", root / "fuzz/seeds")
    return root


def _edit(path: Path, old: str, new: str) -> None:
    text = path.read_text()
    if old not in text:
        raise SystemExit(f"self-test: plant anchor not found in {path.name}: {old!r}")
    path.write_text(text.replace(old, new, 1))


def self_test() -> int:
    target = f"fuzz/fuzz_targets/{RESPONSE_TARGET}.rs"
    plants = [
        ("deleted dispatch arm", "DescribeQuorum v0",
         lambda r: _edit(r / target, "        DescribeQuorum => DescribeQuorumResponse,\n", "")),
        ("new api_versions! row with no path", "Bogus v0",
         lambda r: _edit(r / "src/protocol/mod.rs", "api_versions! {\n",
                         'api_versions! {\n    "Bogus" [99] => BOGUS_MIN = 0 ..= BOGUS_MAX = 0, "x";\n')),
        ("raised MAX without a seed", "DescribeQuorum v3",
         lambda r: _edit(r / "src/protocol/mod.rs", "DESCRIBE_QUORUM_MAX = 2", "DESCRIBE_QUORUM_MAX = 3")),
        ("hand-written version range", "hand-written version",
         lambda r: _edit(r / target, "let _ = <$ty>::decode_versioned(version, buf);",
                         "let _ = <$ty>::decode_versioned(3, buf);")),
        ("target without a [[bin]]", "no [[bin]]",
         lambda r: (r / "fuzz/fuzz_targets/fuzz_orphan.rs").write_text("")),
        ("target without seeds", "fuzz/seeds/fuzz_scram/: no committed seeds",
         lambda r: shutil.rmtree(r / "fuzz/seeds/fuzz_scram")),
    ]
    failed = 0
    with tempfile.TemporaryDirectory() as tmp:
        clean = check(_copy(Path(tmp) / "clean"))
        if clean:
            print("  ✗ unplanted copy fails:\n    " + "\n    ".join(clean))
            failed += 1
    for desc, expect, plant in plants:
        with tempfile.TemporaryDirectory() as tmp:
            root = _copy(Path(tmp))
            plant(root)
            errors = check(root)
        caught = any(expect in e for e in errors)
        failed += not caught
        print(f"  {'✓' if caught else '✗'} {desc}" + ("" if caught else f" — not caught: {errors}"))
    if failed:
        print(f"✗ fuzz_coverage self-test: {failed} control(s) failed", file=sys.stderr)
        return 1
    print(f"✓ fuzz_coverage self-test: {len(plants)} plants caught, clean copy passes")
    return 0


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return self_test()
    if argv == ["--write-seeds"]:
        return write_seeds(ROOT)
    errors = check(ROOT)
    if errors:
        print(f"✗ fuzz coverage: {len(errors)} problem(s)", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    rows = table_rows(ROOT)
    pairs = sum(hi - lo + 1 for _, lo, hi in rows)
    print(f"✓ fuzz coverage: {len(rows)} APIs, {pairs} (API, version) pairs dispatched and seeded")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
