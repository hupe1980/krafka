#!/usr/bin/env python3
"""Every workflow holds only what it uses, pins what it runs, and holds no registry token.

Runs zizmor (config: `.github/zizmor.yml`) over `.github/workflows/`, then the
repository's own rules, which hold whatever zizmor's defaults are:

  * every `uses:` names a full 40-character commit SHA (a version comment
    beside it is fine);
  * no workflow grants a write permission at workflow level (`write-all`, or
    any `<scope>: write` in the top-level `permissions:`);
  * no workflow references a registry token secret (`secrets.CARGO_REGISTRY_TOKEN`
    or any `secrets.*` naming CARGO/CRATES and TOKEN): publishing goes through
    crates.io Trusted Publishing only.

zizmor is taken from PATH, else run through `uvx zizmor==ZIZMOR_VERSION` when uv
is installed. Missing both, the zizmor half is skipped locally and fails in CI.

Run: python3 xtask/workflow_lint.py
     python3 xtask/workflow_lint.py --self-test   # the planted-violation controls
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ZIZMOR_VERSION = "1.30.1"


def zizmor_command() -> list[str] | None:
    if shutil.which("zizmor"):
        return ["zizmor"]
    if shutil.which("uvx"):
        return ["uvx", f"zizmor@{ZIZMOR_VERSION}"]
    return None


def run_zizmor(root: Path) -> tuple[bool | None, str]:
    """(passed, output); passed is None when zizmor is unavailable."""
    cmd = zizmor_command()
    if cmd is None:
        return None, ""
    out = subprocess.run(
        [*cmd, "--offline", "--no-progress", "--config", str(root / ".github" / "zizmor.yml"),
         str(root / ".github" / "workflows")],
        capture_output=True, text=True,
    )
    return out.returncode == 0, out.stdout + out.stderr


USES = re.compile(r"^\s*(?:-\s+)?uses:\s*([^\s#]+)", re.M)
TOKEN_SECRET = re.compile(r"secrets\.(CARGO_REGISTRY_TOKEN|[A-Z0-9_]*(?:CARGO|CRATES)[A-Z0-9_]*TOKEN[A-Z0-9_]*)")


def own_rules(root: Path) -> list[str]:
    problems: list[str] = []
    for path in sorted((root / ".github" / "workflows").glob("*.y*ml")):
        text = path.read_text()
        name = path.name
        for m in USES.finditer(text):
            ref = m.group(1)
            if ref.startswith("./") or ref.startswith("docker://"):
                continue
            if not re.fullmatch(r"[^@\s]+@[0-9a-f]{40}", ref):
                line = text.count("\n", 0, m.start()) + 1
                problems.append(f"{name}:{line}: `{ref}` is not pinned to a full commit SHA")
        top = re.search(r"^permissions:(.*?)(?=^\S)", text, re.M | re.S)
        if top:
            block = top.group(1)
            if "write-all" in block or re.search(r"^\s+[a-z-]+:\s*write\s*$", block, re.M):
                problems.append(f"{name}: workflow-level `permissions:` grants a write; grant it on the job that needs it")
        elif re.search(r"^permissions:\s*write-all\s*$", text, re.M):
            problems.append(f"{name}: workflow-level `permissions: write-all`")
        if not re.search(r"^permissions:", text, re.M):
            problems.append(f"{name}: no workflow-level `permissions:`; set it to read-only or `{{}}`")
        for m in TOKEN_SECRET.finditer(text):
            line = text.count("\n", 0, m.start()) + 1
            problems.append(f"{name}:{line}: references `{m.group(0)}`; publishing uses Trusted Publishing only")
    return problems


def lint(root: Path) -> tuple[list[str], bool | None]:
    problems = own_rules(root)
    passed, output = run_zizmor(root)
    if passed is False:
        problems.append("zizmor reported findings:\n" + output.strip())
    return problems, passed


def main() -> int:
    if "--self-test" in sys.argv:
        return self_test()
    problems, zizmor = lint(ROOT)
    if zizmor is None:
        if os.environ.get("CI"):
            problems.append("zizmor is not installed (required in CI); install it or uv")
        else:
            print("⊘ zizmor not installed (nor uv) — zizmor audits skipped. Install with: cargo install --locked zizmor")
    if problems:
        print("✗ workflow lint", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        return 1
    count = len(list((ROOT / ".github" / "workflows").glob("*.y*ml")))
    print(f"✓ workflow lint: {count} workflows" + (", zizmor clean" if zizmor else ""))
    return 0


def self_test() -> int:
    ci = "ci.yml"
    plants = {
        "tag-pinned action": (
            ci, lambda t: re.sub(r"actions/checkout@[0-9a-f]{40}", "actions/checkout@v4", t, count=1),
            "not pinned to a full commit SHA",
        ),
        "workflow-level contents: write": (
            ci, lambda t: t.replace("permissions:\n  contents: read\n", "permissions:\n  contents: write\n", 1),
            "workflow-level `permissions:` grants a write",
        ),
        "run step interpolating the PR title": (
            ci, lambda t: t.replace("      - run: just fmt-check\n", "      - run: echo \"${{ github.event.pull_request.title }}\"\n      - run: just fmt-check\n", 1),
            "template-injection",
        ),
        "checkout without persist-credentials: false": (
            ci, lambda t: t.replace("        with:\n          persist-credentials: false\n", "", 1),
            "artipacked",
        ),
        "registry token secret": (
            "publish.yml", lambda t: t.replace("${{ steps.auth.outputs.token }}", "${{ secrets.CARGO_REGISTRY_TOKEN }}", 1),
            "CARGO_REGISTRY_TOKEN",
        ),
    }
    failures = 0
    if zizmor_command() is None:
        print("✗ the self-test needs zizmor (or uv)", file=sys.stderr)
        return 1
    for name, (file, edit, expect) in plants.items():
        with tempfile.TemporaryDirectory() as tmp:
            copy = Path(tmp)
            shutil.copytree(ROOT / ".github", copy / ".github")
            target = copy / ".github" / "workflows" / file
            before = target.read_text()
            after = edit(before)
            if after == before:
                print(f"✗ plant `{name}`: the edit did not apply", file=sys.stderr)
                failures += 1
                continue
            target.write_text(after)
            problems, _ = lint(copy)
            hit = [p for p in problems if expect in p]
            if hit:
                first = next(l for l in hit[0].splitlines() if expect in l)
                print(f"✓ plant `{name}`: {first.strip()}")
            else:
                print(f"✗ plant `{name}`: NOT detected", file=sys.stderr)
                failures += 1
    problems, _ = lint(ROOT)
    if problems:
        print(f"✗ the real tree fails: {problems}", file=sys.stderr)
        failures += 1
    else:
        print("✓ real tree: clean")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
