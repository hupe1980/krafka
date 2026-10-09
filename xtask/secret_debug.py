#!/usr/bin/env python3
"""Forbid `#[derive(Debug)]` on types that hold credential material.

`Debug` is the quiet way credentials reach a log aggregator. Nothing has to log
the secret deliberately: a `tracing` field, an error context, a panic message or
a test assertion that formats the enclosing value is enough.

# What it checks

Every `struct` (braced or tuple) and every `enum` under `src/` that derives
`Debug` fails when it carries something that looks like a credential:

  - a field or enum variant whose *name* contains a credential word
    (`password`, `passwd`, `passphrase`, `secret`, `token`, `credential`,
    `private_key`, `access_key`, `secret_key`, `signing_key`, `auth_bytes`,
    `hmac`) and does not end in a metadata suffix (`_endpoint`, `_id`,
    `_type`, ...);
  - a field whose *type* is `Zeroizing<..>`, whose `Debug` delegates to the
    inner value, or a tuple field whose type name contains a credential word.

A field whose type is in SELF_REDACTING_TYPES (a manual `Debug` that redacts)
passes, as does a (type, field) pair in ALLOWLIST, each with a reason.

Names are the main signal rather than types, because the leak is about what
the value means, not how it is stored: `auth_bytes: Vec<u8>` is a password.

# Self-test

Before scanning `src/`, the check runs over the planted violations in
`xtask/secret_debug_fixtures/`: every `*.rs` file there except `clean.rs` must
fail, and `clean.rs` must pass. A guard that stops seeing one of those shapes
fails the run, so its negative control runs on every invocation.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
FIXTURES = ROOT / "xtask" / "secret_debug_fixtures"

# Words that denote credential material, matched anywhere in a snake_case name.
SECRET_WORD = re.compile(
    r"password|passwd|passphrase|secret|token|credential|private_key|access_key|"
    r"secret_key|signing_key|auth_bytes|hmac"
)

# A name ending in one of these describes a credential rather than holding one.
METADATA_SUFFIX = re.compile(
    r"(_endpoint|_id|_ids|_path|_type|_name|_principal_name|_principal_type|"
    r"_fetches|_failures|_latency|_epoch_ms|_count|_len|_infos)$"
)

# (type, field-or-variant) pairs that look secret-bearing but are not.
#
# Keep this short. Every entry is a place the check is deliberately blind.
ALLOWLIST: dict[tuple[str, str], str] = {
    ("AckObservation", "token"): (
        "test-only: the interceptor's correlation token, not a credential"
    ),
    ("ScramCredentialUserResult", "credential_infos"): (
        "SCRAM credential *metadata* (mechanism + iteration count); Kafka never "
        "returns the salt or stored key over this API"
    ),
    ("DescribeUserScramCredentialsResultEntry", "credential_infos"): (
        "same: mechanism and iterations only, no secret material"
    ),
}

# Types with a manual `Debug` that redacts, so a derive over them is safe.
SELF_REDACTING_TYPES = (
    "AssertionSource",
    "AwsMskIamCredentialProviderHandle",
    "AwsMskIamCredentials",
    "ClientCredentials",
    "DelegationToken",
    "OAuthBearerToken",
    "OAuthBearerTokenProviderHandle",
    "PlainCredentials",
    "ScramCredentialInfo",
    "ScramCredentials",
    "TlsConfig",
)


def strip_comments(text: str) -> str:
    """Blank out comments, preserving offsets so line numbers stay correct."""
    out = list(text)
    i, n = 0, len(text)
    while i < n:
        if text.startswith("//", i):
            while i < n and text[i] != "\n":
                out[i] = " "
                i += 1
        elif text.startswith("/*", i):
            depth = 1
            out[i] = out[i + 1] = " "
            i += 2
            while i < n and depth:
                if text.startswith("/*", i):
                    depth += 1
                    out[i] = out[i + 1] = " "
                    i += 2
                elif text.startswith("*/", i):
                    depth -= 1
                    out[i] = out[i + 1] = " "
                    i += 2
                else:
                    out[i] = " " if text[i] != "\n" else "\n"
                    i += 1
        else:
            i += 1
    return "".join(out)


ITEM_RE = re.compile(
    r"#\[derive\(([^)]*)\)\]\s*(?:#\[[^\]]*\]\s*)*"
    r"(?:pub(?:\([^)]*\))?\s+)?(struct|enum)\s+(\w+)\s*(?:<[^>{(;]*>)?\s*"
    r"(?:where[^{(;]*)?([{(;])"
)
OPEN = {"{": "}", "(": ")", "<": ">", "[": "]"}


def snake(name: str) -> str:
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", name).lower()


def is_secret_name(name: str) -> bool:
    name = snake(name)
    return bool(SECRET_WORD.search(name)) and not METADATA_SUFFIX.search(name)


def matching(text: str, start: int) -> int | None:
    """Index of the bracket closing the one at `start`."""
    stack = []
    for i in range(start, len(text)):
        c = text[i]
        if c in OPEN:
            stack.append(OPEN[c])
        elif stack and c == stack[-1]:
            stack.pop()
            if not stack:
                return i
        elif c in ")}]" and stack:
            return None
    return None


def split_top(body: str) -> list[str]:
    """Split on commas that are not nested in brackets."""
    parts, depth, cur = [], 0, []
    for c in body:
        if c in "{(<[":
            depth += 1
        elif c in "})>]":
            depth -= 1
        if c == "," and depth == 0:
            parts.append("".join(cur))
            cur = []
        else:
            cur.append(c)
    parts.append("".join(cur))
    return [p.strip() for p in parts if p.strip()]


ATTR_RE = re.compile(r"#\[[^\]]*\]")
VIS_RE = re.compile(r"^pub(?:\([^)]*\))?\s+")


def clean(part: str) -> str:
    return VIS_RE.sub("", ATTR_RE.sub("", part).strip()).strip()


def secret_type(ty: str, by_name: bool) -> bool:
    """`Zeroizing<..>` always; a credential-named type only when `by_name`.

    Type names count only for tuple fields, which have no field name. In a
    braced field a type such as `Vec<ScramCredentialUserResult>` is a
    Debug-deriving type that this check scans on its own.
    """
    if any(t in ty for t in SELF_REDACTING_TYPES):
        return False
    if "Zeroizing" in ty:
        return True
    return by_name and any(is_secret_name(i) for i in re.findall(r"[A-Z]\w*", ty))


def findings(item: str, body: str, kind: str) -> list[tuple[str, str]]:
    """(member, reason) for every credential-looking member of `body`."""
    out = []

    def named_fields(fields: str, prefix: str = "") -> None:
        for part in split_top(fields):
            part = clean(part)
            if ":" not in part:
                continue
            name, ty = (s.strip() for s in part.split(":", 1))
            if any(t in ty for t in SELF_REDACTING_TYPES):
                continue
            if is_secret_name(name) or secret_type(ty, by_name=False):
                out.append((prefix + name, f"{name}: {ty}"))

    def tuple_fields(fields: str, owner: str) -> None:
        for i, part in enumerate(split_top(fields)):
            ty = clean(part)
            if secret_type(ty, by_name=True) or (
                is_secret_name(owner) and not any(t in ty for t in SELF_REDACTING_TYPES)
            ):
                out.append((f"{owner}.{i}", f"{owner}({ty})"))

    if kind == "{":
        named_fields(body)
    elif kind == "(":
        tuple_fields(body, item)
    elif kind == "enum":
        i = 0
        while i < len(body):
            m = re.compile(r"\s*(?:#\[[^\]]*\]\s*)*(\w+)\s*").match(body, i)
            if not m:
                break
            variant, j = m.group(1), m.end()
            payload = ""
            if j < len(body) and body[j] in "{(":
                end = matching(body, j)
                if end is None:
                    break
                payload, opener, j = body[j + 1 : end], body[j], end + 1
                if opener == "(":
                    tuple_fields(payload, variant)
                else:
                    named_fields(payload, prefix=f"{variant}::")
                    if is_secret_name(variant) and payload.strip() and not any(
                        t in payload for t in SELF_REDACTING_TYPES
                    ):
                        out.append((variant, f"variant {variant} {{ .. }}"))
            nxt = body.find(",", j)
            if nxt == -1:
                break
            i = nxt + 1
    return out


def scan(path: Path) -> tuple[int, list[str]]:
    raw = path.read_text()
    text = strip_comments(raw)
    failures: list[str] = []
    checked = 0
    for m in ITEM_RE.finditer(text):
        derives, keyword, name, opener = m.groups()
        if "Debug" not in [d.strip() for d in derives.split(",")]:
            continue
        if opener == ";":
            continue
        start = m.end() - 1
        end = matching(text, start)
        if end is None:
            continue
        checked += 1
        kind = "enum" if keyword == "enum" else opener
        for member, what in findings(name, text[start + 1 : end], kind):
            field = member.split("::")[-1]
            if (name, field) in ALLOWLIST or (name, member) in ALLOWLIST:
                continue
            line = raw[: m.start()].count("\n") + 1
            try:
                shown = path.relative_to(ROOT)
            except ValueError:
                shown = path
            failures.append(
                f"{shown}:{line}  `{name}` derives Debug and carries `{what}`.\n"
                "    A credential in a derived Debug reaches every log line, error\n"
                "    context and panic message that formats the enclosing value.\n"
                "    Write a manual `Debug` that reports a length or `[REDACTED]`,\n"
                "    or add the pair to ALLOWLIST in this script with a reason.\n"
            )
    return checked, failures


def self_test() -> list[str]:
    """Every planted fixture must fail; `clean.rs` must pass."""
    problems = []
    fixtures = sorted(FIXTURES.glob("*.rs"))
    if not any(f.name != "clean.rs" for f in fixtures):
        return [f"no planted fixtures found in {FIXTURES}"]
    for fixture in fixtures:
        _, failures = scan(fixture)
        if fixture.name == "clean.rs" and failures:
            problems.append(f"{fixture.name}: flagged a type with a redacting Debug")
        elif fixture.name != "clean.rs" and not failures:
            problems.append(f"{fixture.name}: planted violation not detected")
    return problems


def main() -> int:
    problems = self_test()
    if problems:
        print("Secret-in-Debug self-test FAILED: the guard is blind\n", file=sys.stderr)
        for p in problems:
            print(f"  - {p}", file=sys.stderr)
        return 1

    checked, failures = 0, []
    for path in sorted(ROOT.glob("src/**/*.rs")):
        n, f = scan(path)
        checked += n
        failures += f

    if failures:
        print("Secret-in-Debug check FAILED\n", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    planted = sum(1 for f in FIXTURES.glob("*.rs") if f.name != "clean.rs")
    print(
        f"✓ Secret-in-Debug: {planted} planted fixtures rejected; {checked} "
        f"Debug-deriving types scanned, {len(ALLOWLIST)} documented exceptions"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
