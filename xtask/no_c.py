#!/usr/bin/env python3
"""The default build compiles no C except `ring`'s, and links no system library.

For every target in TARGETS the default-feature graph (normal and build edges,
no dev-dependencies) is resolved with `cargo tree --offline --locked` — no
build, no network — and the check fails when:

  * `cmake`, `bindgen`, `pkg-config` or `openssl-sys` is anywhere in it;
  * a `*-sys` crate is in it that SYS_ALLOWLIST does not name;
  * a crate other than `ring` reaches `cc` through its build-dependencies.

Each violation names the crate and the dependency path from krafka.

It then resolves each named feature added to the defaults (on the host
target) and fails when the C it needs differs from FEATURE_C.

Run: python3 xtask/no_c.py
     python3 xtask/no_c.py --self-test   # the planted-violation controls
"""

from __future__ import annotations

import re
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

TARGETS = (
    "x86_64-unknown-linux-gnu",
    "x86_64-unknown-linux-musl",
    "aarch64-unknown-linux-musl",
    "x86_64-pc-windows-msvc",
    "x86_64-pc-windows-gnu",
    "aarch64-apple-darwin",
)

# The one crate allowed to compile C in the default build.
C_ALLOWED = {"ring"}

BANNED = {
    "cmake": "needs CMake installed",
    "bindgen": "needs libclang installed",
    "pkg-config": "probes for a system library",
    "openssl-sys": "links the system OpenSSL",
}

# `*-sys` crates that compile and link nothing: pure-Rust bindings.
SYS_ALLOWLIST = {
    "windows-sys": "Rust declarations of the Win32 API; links only system DLLs every Windows has",
    "linux-raw-sys": "pure-Rust Linux syscall definitions; no C",
    "core-foundation-sys": "declarations of a framework every macOS has; compiles nothing",
    "security-framework-sys": "declarations of a framework every macOS has; compiles nothing",
}

# What each named feature adds over the defaults: None (pure Rust), "cc"
# (a C compiler), or "cmake" (a C compiler and CMake), with the crate that
# causes it.
FEATURE_C: dict[str, tuple[str | None, str | None]] = {
    "zstd": ("cc", "zstd-sys"),
    "rustls-aws-lc-rs": ("cmake", "aws-lc-sys"),
    "aws-msk": ("cmake", "aws-lc-sys"),
    "oauth-oidc": (None, None),
    "tls-encrypted-keys": (None, None),
    "native-tls-roots": (None, None),
    "unstable-protocol": (None, None),
    "test-broker": (None, None),
}


@dataclass
class Graph:
    root: str
    # child -> set of (parent, kind); kind is "normal" or "build"
    parents: dict[str, set[tuple[str, str]]] = field(default_factory=dict)
    children: dict[str, set[tuple[str, str]]] = field(default_factory=dict)

    def add(self, parent: str, child: str, kind: str) -> None:
        self.children.setdefault(parent, set()).add((child, kind))
        self.parents.setdefault(child, set()).add((parent, kind))
        self.children.setdefault(child, set())

    def nodes(self) -> set[str]:
        return set(self.children)

    def path_to(self, node: str) -> str:
        """A shortest dependency path from the root to `node`."""
        prev: dict[str, str | None] = {self.root: None}
        queue = [self.root]
        while queue:
            cur = queue.pop(0)
            if cur == node:
                break
            for child, _ in sorted(self.children.get(cur, ())):
                if child not in prev:
                    prev[child] = cur
                    queue.append(child)
        chain: list[str] = []
        cur: str | None = node
        while cur is not None:
            chain.append(name_of(cur))
            cur = prev.get(cur)
        return " -> ".join(reversed(chain))

    def reaches(self, start: str, target_name: str) -> bool:
        seen = {start}
        stack = [start]
        while stack:
            cur = stack.pop()
            if name_of(cur) == target_name:
                return True
            for child, _ in self.children.get(cur, ()):
                if child not in seen:
                    seen.add(child)
                    stack.append(child)
        return False


def name_of(node: str) -> str:
    return node.split(" ", 1)[0]


LINE = re.compile(r"^((?:[|`]-- |\|   |    )*)(.*)$")


def parse_tree(text: str) -> Graph:
    """Parse `cargo tree --charset ascii` output into typed edges."""
    lines = [l for l in text.splitlines() if l.strip()]
    if not lines:
        raise SystemExit("✗ no-c: empty `cargo tree` output")
    root = node_id(lines[0])
    graph = Graph(root=root)
    graph.children.setdefault(root, set())
    stack: list[str] = [root]  # stack[level] = node at that depth
    kinds: dict[int, str] = {}  # child level -> edge kind currently in effect
    for line in lines[1:]:
        m = LINE.match(line)
        assert m is not None
        prefix, content = m.group(1), m.group(2)
        if content.startswith("["):
            level = len(prefix) // 4 + 1
            kinds[level] = "build" if content == "[build-dependencies]" else "other"
            continue
        level = len(prefix) // 4
        node = node_id(content)
        del stack[level:]
        for deeper in [k for k in kinds if k > level]:
            del kinds[deeper]
        parent = stack[level - 1]
        kind = kinds.get(level, "normal")
        if kind != "other":
            graph.add(parent, node, kind)
        stack.append(node)
    return graph


def node_id(content: str) -> str:
    # "ring v0.17.14", "krafka v0.26.0 (/path)", "x v1 (*)", "y v1 (proc-macro)"
    parts = content.split()
    return f"{parts[0]} {parts[1]}"


def cargo_tree(target: str, features: str | None, manifest: Path) -> Graph:
    cmd = [
        "cargo", "tree", "--offline", "--locked", "--charset", "ascii", "--color", "never",
        "-e", "normal,build", "--target", target,
        "--manifest-path", str(manifest),
    ]
    if features:
        cmd += ["--features", features]
    out = subprocess.run(cmd, cwd=manifest.parent, capture_output=True, text=True)
    if out.returncode != 0:
        raise SystemExit(f"✗ no-c: `{' '.join(cmd)}` failed:\n{out.stderr}")
    return parse_tree(out.stdout)


def violations(graph: Graph) -> list[str]:
    found: list[str] = []
    for node in sorted(graph.nodes()):
        name = name_of(node)
        if name in BANNED:
            found.append(f"`{name}` ({BANNED[name]}): {graph.path_to(node)}")
        elif name.endswith("-sys") and name not in SYS_ALLOWLIST:
            found.append(f"`{name}` is a -sys crate outside the allowlist: {graph.path_to(node)}")
        if name in C_ALLOWED:
            continue
        for child, kind in graph.children.get(node, ()):
            if kind == "build" and graph.reaches(child, "cc"):
                found.append(f"`{name}` compiles C (`cc` in its build-dependencies): {graph.path_to(node)}")
                break
    return found


def c_requirement(graph: Graph, baseline: Graph) -> tuple[str | None, str | None]:
    """The C a feature adds over the default graph, and the crate causing it."""
    new = {name_of(n) for n in graph.nodes()} - {name_of(n) for n in baseline.nodes()}
    if "cmake" in new:
        return "cmake", culprit(graph, "cmake")
    for node in sorted(graph.nodes()):
        name = name_of(node)
        if name in C_ALLOWED or name not in new:
            continue
        for child, kind in graph.children.get(node, ()):
            if kind == "build" and graph.reaches(child, "cc"):
                return "cc", name
    return None, None


def culprit(graph: Graph, tool: str) -> str | None:
    for node in sorted(graph.nodes()):
        for child, kind in graph.children.get(node, ()):
            if kind == "build" and name_of(child) == tool:
                return name_of(node)
    return None


def host_target() -> str:
    out = subprocess.run(["rustc", "-vV"], capture_output=True, text=True, check=True).stdout
    return re.search(r"^host: (\S+)$", out, re.M).group(1)  # type: ignore[union-attr]


def check(manifest: Path, check_features: bool = True) -> list[str]:
    problems: list[str] = []
    for target in TARGETS:
        for v in violations(cargo_tree(target, None, manifest)):
            problems.append(f"[{target}] {v}")
    if check_features:
        host = host_target()
        baseline = cargo_tree(host, None, manifest)
        for feature, expected in FEATURE_C.items():
            got = c_requirement(cargo_tree(host, feature, manifest), baseline)
            if got != expected:
                problems.append(
                    f"feature `{feature}`: graph needs {describe(got)}, FEATURE_C records {describe(expected)}"
                )
    return problems


def describe(req: tuple[str | None, str | None]) -> str:
    kind, crate = req
    if kind is None:
        return "no C"
    what = "a C compiler and CMake" if kind == "cmake" else "a C compiler"
    return f"{what} (via `{crate}`)"


def main() -> int:
    if "--self-test" in sys.argv:
        return self_test()
    problems = check(ROOT / "Cargo.toml")
    if problems:
        print("✗ no-c: the default build compiles C beyond ring's, or a feature's C changed", file=sys.stderr)
        for p in problems:
            print(f"  {p}", file=sys.stderr)
        return 1
    print(
        f"✓ no-c: default graph on {len(TARGETS)} targets has no C outside `ring`; "
        f"{len(FEATURE_C)} features match their recorded C requirement"
    )
    return 0


# ---------------------------------------------------------------------------
# Negative controls: plant a violation in a scratch copy, expect a failure.
# ---------------------------------------------------------------------------

def self_test() -> int:
    import shutil
    import tempfile

    plants = {
        # A default dependency whose build script compiles C (zstd-sys).
        "cc-build-dep": ("default = [", "default = [\"zstd\", ", "compiles C"),
        # cmake as a build dependency of the crate itself.
        "cmake-build-dep": ("[dev-dependencies]\n", "[build-dependencies]\ncmake = \"0.1\"\n\n[dev-dependencies]\n", "cmake"),
    }
    failures = 0
    for name, (anchor, replacement, expect) in plants.items():
        with tempfile.TemporaryDirectory() as tmp:
            copy = Path(tmp) / "krafka"
            shutil.copytree(ROOT, copy, ignore=shutil.ignore_patterns("target", ".git", "site", "fuzz", "concepts"))
            toml = copy / "Cargo.toml"
            text = toml.read_text()
            assert anchor in text, anchor
            toml.write_text(text.replace(anchor, replacement, 1))
            # Re-lock offline against the local registry cache.
            lock = subprocess.run(
                ["cargo", "generate-lockfile", "--offline"], cwd=copy, capture_output=True, text=True
            )
            if lock.returncode != 0:
                print(f"✗ plant {name}: could not lock offline:\n{lock.stderr}", file=sys.stderr)
                failures += 1
                continue
            problems = [p for p in check(copy / "Cargo.toml", check_features=False) if expect in p]
            if problems:
                print(f"✓ plant {name}: detected — {problems[0]}")
            else:
                print(f"✗ plant {name}: NOT detected", file=sys.stderr)
                failures += 1
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
