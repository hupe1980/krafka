#!/usr/bin/env python3
"""Forbid `#[derive(Debug)]` on types that hold credential material.

# Why this exists

`Debug` is the quiet way credentials reach a log aggregator. Nothing has to log
the secret deliberately: a `tracing` field, an error context, a panic message or
a test assertion that formats the enclosing struct is enough.

Two instances shipped before this check existed:

  - `ClientCredentials` (the OIDC token provider) derived `Debug`.
    `Zeroizing<String>` scrubs memory on drop, but its own `Debug` delegates to
    the inner `String`, so the derive printed the OAuth client secret.
  - `SaslAuthenticateRequest` and `SaslAuthenticateResponse` derived `Debug`
    over `auth_bytes` — the raw SASL payload. For `PLAIN` that is
    `\\0username\\0password` in cleartext; for `OAUTHBEARER` it is the bearer
    token verbatim; for `SCRAM-SHA-*` it includes the client proof.

The first was caught by its own redaction test before commit. The second had
been on the public API for the crate's whole life, waiting for anyone to write
`debug!(?request)`.

Both are now manual `Debug` impls that report a length and nothing else. This
script is what stops the third.

# What it checks

Any `struct` that both

  1. derives `Debug`, and
  2. has a field whose *name* matches a credential pattern,

fails, unless the field's type is a wrapper this file knows redacts on its own,
or the (struct, field) pair is in ALLOWLIST with a reason.

Field *names* are the signal rather than types, because the leak is about what
the value means, not how it is stored — `auth_bytes: Vec<u8>` is a password.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# Field names that denote credential material.
SECRET_FIELD = re.compile(
    r"^(password|passwd|secret|client_secret|sasl_password|credential|credentials|"
    r"private_key|passphrase|session_token|access_key|secret_key|api_token|"
    r"auth_bytes|hmac|token_value|bearer_token)$"
)

# (struct, field) pairs that look secret-bearing but are not, each with a reason.
#
# Keep this short. Every entry is a place the check is deliberately blind.
ALLOWLIST: dict[tuple[str, str], str] = {
    # Kafka's `api_key` is the numeric API identifier (Produce = 0), not a
    # credential. The regex no longer matches it, but these were the historical
    # false positives and are recorded so the intent is clear.
    ("CreateDelegationTokenResult", "token"): (
        "holds a DelegationToken, which has its own redacting Debug impl"
    ),
    ("DescribeDelegationTokenResponse", "tokens"): (
        "holds DelegationTokenDetail values, which redact their own hmac"
    ),
    ("OidcTokenProviderBuilder", "credentials"): (
        "holds ClientCredentials, whose manual Debug redacts the secret"
    ),
    ("ScramCredentialUserResult", "credential_infos"): (
        "SCRAM credential *metadata* (mechanism + iteration count); Kafka never "
        "returns the salt or stored key over this API"
    ),
    ("DescribeUserScramCredentialsResultEntry", "credential_infos"): (
        "same: mechanism and iterations only, no secret material"
    ),
}

# Field types that redact themselves, so a derive over them is safe.
SELF_REDACTING_TYPES = (
    "DelegationToken",
    "ClientCredentials",
    "AssertionSource",
    "ScramCredentialInfo",
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


STRUCT_RE = re.compile(
    r"#\[derive\(([^)]*)\)\]\s*(?:#\[[^\]]*\]\s*)*"
    r"(?:pub(?:\([^)]*\))?\s+)?struct\s+(\w+)\s*(?:<[^>]*>)?\s*\{"
)
FIELD_RE = re.compile(r"(?:pub(?:\([^)]*\))?\s+)?(\w+)\s*:\s*([^,\n]+)")


def main() -> int:
    failures: list[str] = []
    checked = 0

    for path in sorted(ROOT.glob("src/**/*.rs")):
        raw = path.read_text()
        text = strip_comments(raw)
        for m in STRUCT_RE.finditer(text):
            derives, name = m.group(1), m.group(2)
            if "Debug" not in [d.strip() for d in derives.split(",")]:
                continue

            depth, start, end = 0, m.end() - 1, None
            for i in range(start, len(text)):
                if text[i] == "{":
                    depth += 1
                elif text[i] == "}":
                    depth -= 1
                    if depth == 0:
                        end = i
                        break
            if end is None:
                continue

            checked += 1
            for fm in FIELD_RE.finditer(text[start + 1 : end]):
                field, ty = fm.group(1), fm.group(2).strip()
                if not SECRET_FIELD.match(field):
                    continue
                if (name, field) in ALLOWLIST:
                    continue
                if any(t in ty for t in SELF_REDACTING_TYPES):
                    continue
                line = raw[: m.start()].count("\n") + 1
                failures.append(
                    f"{path.relative_to(ROOT)}:{line}  "
                    f"`{name}` derives Debug and has field `{field}: {ty}`.\n"
                    "    A credential in a derived Debug reaches every log line, error\n"
                    "    context and panic message that formats the enclosing value.\n"
                    "    Write a manual `Debug` that reports a length or `[REDACTED]`,\n"
                    "    or add the pair to ALLOWLIST in this script with a reason.\n"
                )

    if failures:
        print("Secret-in-Debug check FAILED\n", file=sys.stderr)
        for failure in failures:
            print(f"  - {failure}", file=sys.stderr)
        return 1

    print(
        f"✓ Secret-in-Debug: {checked} Debug-deriving structs scanned, "
        f"{len(ALLOWLIST)} documented exceptions"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
