#!/usr/bin/env python3
"""Forbid documentation that names an API the crate does not have.

# Why this exists

The guides under `site/content/docs/` are the only part of this project the
compiler never sees. Rustdoc examples are at least parsed; a Markdown fence is
inert. So a rename lands in the crate, the tests and the doc comments, and
silently rots every guide that named the old thing — and nobody finds out until
a reader copies the snippet.

Three had already rotted when this check was written, all in the admin guide:

  - `describe_delegation_tokens(..)` — the method is singular,
    `describe_delegation_token`. Three call sites.
  - `describe_quorum(..)` — the method is `describe_metadata_quorum`. This one
    was also wrong in the crate's **own rustdoc**, in five places, so following
    the API reference instead of the guide would not have saved you.
  - `describe_metadata_quorum()` — called with no arguments, where the method
    requires `&[(&str, &[i32])]`.

# What it checks

For every Markdown file under `site/content/`:

  1. `krafka::a::b::Type` — the final segment must be a type the crate defines
     or re-exports.
  2. `.method(` at the start of a line — a builder-chain or receiver call. The
     name must be a function the crate defines, or be in ALLOWLIST.
  3. Any mention at all — prose or code — of a name in REMOVED.

Rule 3 exists because rules 1 and 2 both missed a stale `max_in_flight` in the
producer guide's prose: it was inside backticks with no `.` and no `(`, so
neither pattern saw it. A general "backticked snake_case must resolve" rule is
not viable — the guides legitimately name 90-odd metric names, protocol fields
and broker settings that are not crate symbols. Naming the removals explicitly
is precise, costs one line per removal, and is exactly the case that rots:
nobody greps the guides for a name they just deleted.

Arity and shape are not checked here; that needs a compiler. `just docs-test`
(xtask/docs_test.py) compiles every snippet marked ```rust,compile, which is
the stronger guarantee where it applies — this check still covers the prose and
the fragments that cannot be compiled.

The allowlist exists because the guides legitimately show `std`, `tokio`,
`axum` and `rustls` calls in the same chains as krafka's. Keep it to names that
are unambiguously not ours.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Methods from outside the crate that appear in documented chains.
ALLOWLIST = {
    # std / core
    "unwrap", "expect", "unwrap_err", "unwrap_or", "unwrap_or_else",
    "unwrap_or_default", "map_err", "map", "and_then", "ok_or_else", "clone",
    "to_string", "into", "iter", "collect", "push", "insert", "get", "len",
    "await", "as_str", "as_ref", "parse", "join", "spawn", "lock", "read",
    "write", "send", "recv", "next", "filter", "for_each", "unwrap_or_err",
    "as_millis", "as_secs", "duration_since", "tick", "extend", "is_empty",
    "as_deref", "unwrap_or_default", "from_utf8_lossy", "retain", "contains",
    # tokio / futures
    "run", "serve", "bind", "block_on", "sleep", "timeout", "abort",
    # axum / hyper / tower
    "route", "layer", "with_state", "into_make_service", "nest",
    # rustls / webpki
    "install_default", "load", "add_parsable_certificates", "with_root_certificates",
    "with_no_client_auth", "with_safe_defaults", "builder",
    # serde / misc
    "to_owned", "from", "try_into", "deserialize", "serialize",
    # Illustrative database calls in the two-phase-commit example. The external
    # coordinator is the point of KIP-939, so the guide has to show one; these
    # are `postgres`-shaped placeholders, not krafka API.
    "query_one", "execute",
}


# Public API that was deliberately removed, checked everywhere — prose and code
# fences alike, since a stale mention is stale wherever it sits.
#
# Only add a name that **no other crate in the guides can legitimately own**.
# `SchemaEncoder` and `SchemaDecoder` were tried here and had to be taken out
# again: krafka dropped them in 0.17, but the cookbook correctly names
# `schemreg::traits::SchemaEncoder` when showing how to bridge the two crates.
# A removal that shares a name with a live third-party type does not belong in
# this set.
REMOVED = {
    # 0.18: the producer's second, unbatched send path was deleted. Ordering is
    # now structural (one batch per partition on the wire), so the setting has
    # no invariant left to protect; the per-connection ceiling is
    # `TransportConfig::max_in_flight_requests`.
    "max_in_flight",
}


def crate_symbols() -> set[str]:
    """Every type, trait, function and enum variant the crate defines."""
    src = "\n".join(p.read_text() for p in ROOT.glob("src/**/*.rs"))
    names: set[str] = set()
    for pattern in (
        r"\bpub(?:\([^)]*\))?\s+(?:struct|enum|trait|type|const|static)\s+(\w+)",
        r"\bpub(?:\([^)]*\))?\s+(?:const\s+)?(?:async\s+)?fn\s+(\w+)",
        r"pub use [^;]*?\b(\w+)\s*(?:,|\}|;|\s+as\b)",
    ):
        names.update(re.findall(pattern, src))
    # Enum variants: a bare `Name,` or `Name = 3,` at one indent level.
    names.update(re.findall(r"^\s{4}(\w+)\s*(?:=\s*-?\d+)?\s*,\s*$", src, re.M))
    return names


def main() -> int:
    defined = crate_symbols() | ALLOWLIST
    removed_pattern = re.compile(
        r"\b(" + "|".join(sorted(map(re.escape, REMOVED))) + r")\b"
    )
    failures: list[tuple[str, str, int]] = []
    scanned = 0

    for doc in sorted(ROOT.glob("site/content/**/*.md")):
        text = doc.read_text()
        scanned += 1
        rel = str(doc.relative_to(ROOT))

        for m in re.finditer(r"\bkrafka::(?:[a-z_]+::)*([A-Z]\w+)", text):
            if m.group(1) not in defined:
                failures.append((rel, m.group(1), text[: m.start()].count("\n") + 1))

        for m in re.finditer(r"^\s*\.(\w+)\(", text, re.M):
            if m.group(1) not in defined:
                failures.append((rel, m.group(1), text[: m.start()].count("\n") + 1))

        for m in removed_pattern.finditer(text):
            failures.append((rel, m.group(1), text[: m.start()].count("\n") + 1))

    if failures:
        print("Documentation API check FAILED\n", file=sys.stderr)
        seen = set()
        for rel, name, line in failures:
            if (rel, name) in seen:
                continue
            seen.add((rel, name))
            if name in REMOVED:
                print(
                    f"  - {rel}:{line}  `{name}` was removed from the public API.\n"
                    "    Rewrite the passage; see REMOVED in xtask/doc_api.py for what\n"
                    "    replaced it.\n",
                    file=sys.stderr,
                )
            else:
                print(
                    f"  - {rel}:{line}  `{name}` is not defined by the crate.\n"
                    "    Either the guide names a renamed or removed API, or the name "
                    "belongs to\n    another crate and should be added to ALLOWLIST in "
                    "xtask/doc_api.py.\n",
                    file=sys.stderr,
                )
        return 1

    print(f"✓ Documentation API: {scanned} files scanned, every krafka name resolves")
    return 0


if __name__ == "__main__":
    sys.exit(main())
