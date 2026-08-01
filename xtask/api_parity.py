#!/usr/bin/env python3
"""Check that every config-builder setter is reachable from the public builder.

# Why this exists

krafka has two builders per client: an internal `*ConfigBuilder` that owns the
full option surface, and the public `*Builder` returned by `Client::builder()`
that every example and every doc page uses. Options added to the former and not
mirrored on the latter are *implemented, documented, and uncallable*.

That is not hypothetical. A review found:

  - `ConsumerBuilder` missing `group_protocol`, so KIP-848 could not be selected
    from the documented API at all, and `max_decompressed_size`, the
    decompression-bomb limit — a security control.
  - `ProducerBuilder` missing `dead_letter_queue`, an entire feature, with a
    worked example in `docs/errors.md` that did not compile.

Both were found by hand. This script finds them mechanically.

# What it does

Parses `impl <Name> { ... }` blocks with brace matching (no regex-only
guessing), collects `pub fn` names, and reports options present on the config
builder but absent from the public one.

Exits non-zero on any gap, so it can gate CI.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# (public builder, its file) -> (config builder, its file)
#
# Only pairs where the public builder is genuinely meant to be a complete
# façade over the config builder. `ConnectionConfigBuilder` is excluded: it is
# the public API in its own right and has no separate façade.
PAIRS = [
    (
        ("ConsumerBuilder", "src/consumer/builder.rs"),
        ("ConsumerConfigBuilder", "src/consumer/config.rs"),
    ),
    (
        ("ProducerBuilder", "src/producer/mod.rs"),
        ("ProducerConfigBuilder", "src/producer/config.rs"),
    ),
    (
        ("AdminClientBuilder", "src/admin/builder.rs"),
        ("AdminConfigBuilder", "src/admin/mod.rs"),
    ),
]

# Names that are not options and must not be mirrored.
#
# Keep this list short and justified: every entry is a place the check is
# deliberately blind, so an unexplained addition here can hide a real gap.
NOT_OPTIONS = {
    # Terminal / constructor methods, not settings.
    "build",
    "builder",
    "new",
    "default",
    # Config-builder plumbing that the public builder supplies itself.
    "pool",
    "owns_pool",
    "connection_metrics",
    # Runtime accessors that live on the built client, not on its builder.
    "is_closed",
    "update_seed_brokers",
}


def impl_block(source: str, type_name: str) -> str:
    """Return the body of `impl <type_name> { ... }`, matched by braces.

    Regex alone cannot find the end of an impl block; counting braces can.
    String and char literals containing braces would break a naive counter, so
    they are stripped first — comments too, since a `}` in prose is common.
    """
    stripped = strip_literals_and_comments(source)
    match = re.search(rf"\bimpl\s+{re.escape(type_name)}\s*(?:<[^>]*>)?\s*\{{", stripped)
    if not match:
        raise SystemExit(f"could not find `impl {type_name}`")

    start = match.end() - 1
    depth = 0
    for i in range(start, len(stripped)):
        if stripped[i] == "{":
            depth += 1
        elif stripped[i] == "}":
            depth -= 1
            if depth == 0:
                return stripped[start + 1 : i]
    raise SystemExit(f"unbalanced braces in `impl {type_name}`")


def strip_literals_and_comments(source: str) -> str:
    """Blank out comments and string/char literals, preserving offsets.

    Replacing rather than deleting keeps byte offsets stable, so a brace count
    over the result still describes the original text.
    """
    out = list(source)
    i = 0
    n = len(source)
    while i < n:
        ch = source[i]
        if ch == "/" and i + 1 < n and source[i + 1] == "/":
            while i < n and source[i] != "\n":
                out[i] = " "
                i += 1
        elif ch == "/" and i + 1 < n and source[i + 1] == "*":
            depth = 1
            out[i] = out[i + 1] = " "
            i += 2
            while i < n and depth:
                if source.startswith("/*", i):
                    depth += 1
                    out[i] = out[i + 1] = " "
                    i += 2
                elif source.startswith("*/", i):
                    depth -= 1
                    out[i] = out[i + 1] = " "
                    i += 2
                else:
                    out[i] = " " if source[i] != "\n" else "\n"
                    i += 1
        elif ch == '"':
            out[i] = " "
            i += 1
            while i < n:
                if source[i] == "\\":
                    out[i] = " "
                    if i + 1 < n:
                        out[i + 1] = " "
                    i += 2
                    continue
                if source[i] == '"':
                    out[i] = " "
                    i += 1
                    break
                out[i] = " " if source[i] != "\n" else "\n"
                i += 1
        else:
            i += 1
    return "".join(out)


def setters(body: str) -> set[str]:
    """Public method names declared directly in an impl body."""
    return {m.group(1) for m in re.finditer(r"\bpub\s+fn\s+(\w+)", body)}


def main() -> int:
    failures: list[str] = []

    for (public_name, public_path), (config_name, config_path) in PAIRS:
        public = setters(impl_block((ROOT / public_path).read_text(), public_name))
        config = setters(impl_block((ROOT / config_path).read_text(), config_name))

        missing = sorted(config - public - NOT_OPTIONS)
        if missing:
            failures.append(
                f"{public_name} ({public_path}) is missing {len(missing)} option(s) "
                f"that {config_name} exposes:\n"
                + "".join(f"    - {name}\n" for name in missing)
            )

    if failures:
        print("API surface parity check FAILED\n", file=sys.stderr)
        for failure in failures:
            print(failure, file=sys.stderr)
        print(
            "An option on the config builder that is absent from the public builder\n"
            "is implemented, documentable, and uncallable. Either mirror it onto the\n"
            "public builder, or add it to NOT_OPTIONS in this script with a reason.",
            file=sys.stderr,
        )
        return 1

    total = sum(
        len(setters(impl_block((ROOT / cfg_path).read_text(), cfg_name)))
        for (_, _), (cfg_name, cfg_path) in PAIRS
    )
    print(f"✓ API surface parity: {len(PAIRS)} builder pairs, {total} options all reachable")
    return 0


if __name__ == "__main__":
    sys.exit(main())
