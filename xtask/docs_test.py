#!/usr/bin/env python3
"""Compile the guide snippets that claim to be compilable.

# Why this exists

`xtask/doc_api.py` checks that every krafka name a guide mentions is a name the
crate defines. That catches a renamed API. It does not catch a *fabricated*
one used with the wrong shape, because a plausible snippet reaches only for
names that exist:

    KrafkaMetrics::new().with_consumer(m).to_prometheus()   # no such methods
    KafkaDeadLetterQueue::builder()                          # no such type at the time
    #[derive(Debug)] struct Dlq { producer: Producer }        # Producer had no Debug

All three passed `doc_api.py` and none of them compiled. The last one shipped
in the error-handling guide for two releases: `Debug` is a supertrait of
`DeadLetterQueue`, so the documented way to implement the crate's own trait did
not build.

`doc_api.py`'s own docstring already promised that "`just docs-test` compiles
the snippets that are marked compilable". That recipe did not exist. This is it.

# The contract

A fenced block is compiled when its info string is exactly:

    ```rust,compile

Everything else — plain ```rust, ```rust,ignore, ```toml, ```text — is left
alone, because most guide snippets are deliberate fragments that name helpers
the reader supplies (`transform`, `sink`, `write_to_database`).

Marked snippets are assembled into one generated example and built with
`cargo build --all-features --example`, so they see exactly the API a user
sees. Each snippet becomes its own module, so two snippets may define the same
type without colliding.

A snippet is wrapped like this:

  * lines that are items (`use`, `struct`, `enum`, `impl`, `fn`, `#[...]`,
    `trait`, `const`, `static`, `type`, `mod`) stay at module level;
  * everything else becomes the body of an `async fn` returning
    `krafka::Result<()>`, so `?` works and `.await` is legal.

That covers both shapes guides actually use — a builder chain, and a trait
implementation — without the author having to think about it.

Coverage is reported rather than enforced: the marker is opt-in, so a page with
no marked snippets is not a failure. Mark the snippets people copy.
"""

from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
GENERATED = ROOT / "examples" / "__docs_test.rs"

# Files whose snippets are user-facing.
SOURCES = ["README.md", "site/content/**/*.md"]

FENCE = re.compile(r"^```rust,compile\s*$")
FENCE_END = re.compile(r"^```\s*$")
ANY_FENCE = re.compile(r"^```")

# A line that must stay at module level rather than move into the async body.
ITEM_START = re.compile(
    r"^\s*(?:#\[|#!\[|use\s|pub\s|struct\s|enum\s|trait\s|impl\b|mod\s"
    r"|const\s|static\s|type\s|fn\s|async\s+fn\s|unsafe\s|extern\s)"
)


def collect() -> list[tuple[str, int, str]]:
    """Every marked snippet, as (path, line number, body)."""
    found: list[tuple[str, int, str]] = []
    seen: set[Path] = set()
    for pattern in SOURCES:
        for path in sorted(ROOT.glob(pattern)):
            if path in seen:
                continue
            seen.add(path)
            lines = path.read_text().splitlines()
            i = 0
            while i < len(lines):
                if FENCE.match(lines[i]):
                    start = i + 1
                    body: list[str] = []
                    i += 1
                    while i < len(lines) and not FENCE_END.match(lines[i]):
                        body.append(lines[i])
                        i += 1
                    found.append(
                        (str(path.relative_to(ROOT)), start, "\n".join(body))
                    )
                i += 1
    return found


def total_rust_blocks() -> int:
    """All Rust blocks, marked or not — the denominator for coverage."""
    count = 0
    seen: set[Path] = set()
    for pattern in SOURCES:
        for path in sorted(ROOT.glob(pattern)):
            if path in seen:
                continue
            seen.add(path)
            for line in path.read_text().splitlines():
                if ANY_FENCE.match(line) and line.startswith("```rust"):
                    count += 1
    return count


def wrap(index: int, source: str, line: int, body: str) -> str:
    """Turn one snippet into a self-contained module.

    Guides omit imports and setup on purpose — a snippet that spends four lines
    on `use` before showing the one call it is about is a worse snippet. So the
    harness supplies both:

      * **Imports**, as *glob* imports only. A glob does not collide with an
        explicit `use` of the same type, so a snippet that spells out its own
        imports still compiles.
      * **Ambient bindings** — `producer`, `consumer`, `admin`, `client` — as
        parameters. A snippet that builds its own shadows the parameter.

    What is left is exactly what the check is for: whether the API calls are
    real and correctly shaped.
    """
    items: list[str] = []
    stmts: list[str] = []
    in_item = False
    depth = 0

    for raw in body.split("\n"):
        stripped = raw.strip()
        if not in_item and ITEM_START.match(raw):
            in_item = True
            depth = 0
        if in_item:
            items.append(raw)
            depth += raw.count("{") - raw.count("}")
            if (depth <= 0 and stripped.endswith(";")) or (
                depth == 0 and stripped.endswith("}")
            ):
                in_item = False
        else:
            stmts.append(raw)

    indented = "\n".join(f"        {line}" for line in stmts)
    return (
        f"// {source}:{line}\n"
        f"mod snippet_{index} {{\n"
        f"    #![allow(unused, unreachable_code, clippy::all)]\n"
        f"    use krafka::prelude::*;\n"
        f"    use std::sync::*;\n"
        f"    use std::time::*;\n"
        f"    use std::collections::*;\n"
        + "\n".join(f"    {line}" for line in items)
        + "\n"
        f"    pub async fn __check(\n"
        f"        producer: &Producer,\n"
        f"        consumer: &Consumer,\n"
        f"        admin: &AdminClient,\n"
        f"        client: &KrafkaClient,\n"
        f"    ) -> krafka::Result<()> {{\n"
        f"{indented}\n"
        f"        Ok(())\n"
        f"    }}\n"
        f"}}\n"
    )


def main() -> int:
    snippets = collect()
    total = total_rust_blocks()

    if not snippets:
        print(
            "No snippets are marked `rust,compile`. Mark the ones readers copy;\n"
            "see xtask/docs_test.py for the contract."
        )
        return 0

    generated = (
        "// GENERATED by xtask/docs_test.py — do not edit.\n"
        "//\n"
        "// Each module below is a documentation snippet marked ```rust,compile.\n"
        "// If this file fails to build, a guide is telling readers to write code\n"
        "// that does not compile. Fix the guide, not this file.\n"
        # The crate denies `missing_docs`, which applies to examples too.
        "//! Generated documentation snippets.\n"
        "#![allow(unused, clippy::all)]\n"
        "#![allow(missing_docs)]\n\n"
        + "\n".join(wrap(i, s, l, b) for i, (s, l, b) in enumerate(snippets))
        + "\nfn main() {}\n"
    )
    GENERATED.parent.mkdir(parents=True, exist_ok=True)
    GENERATED.write_text(generated)

    result = subprocess.run(
        ["cargo", "build", "--all-features", "--example", "__docs_test"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        print("Documentation snippets failed to compile\n")
        print(result.stderr.rstrip())
        print(
            f"\nThe generated crate is at {GENERATED.relative_to(ROOT)}; each module\n"
            "is headed with the guide and line it came from."
        )
        return 1

    GENERATED.unlink(missing_ok=True)
    pct = 100 * len(snippets) / total if total else 0
    print(
        f"✓ Documentation snippets: {len(snippets)} of {total} Rust blocks "
        f"compile-checked ({pct:.0f}%)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
