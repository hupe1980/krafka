"""Shared interpreter bootstrap for the xtask checks.

# Why this exists

Two checks parse TOML, which means `tomllib` — standard library, but only
since Python 3.11. macOS ships 3.9 as `python3`, so `just ci` died on a
`ModuleNotFoundError` traceback halfway through the run on a developer machine
while passing in CI, which is the worst of both worlds: the check is not run
locally, and the failure says nothing about why.

`ensure_tomllib()` re-executes the calling script under a newer interpreter if
one is on `PATH`, and otherwise fails with a sentence that names the problem
and the fix. It is a no-op on any interpreter that already has `tomllib`, which
includes every one CI uses.
"""

from __future__ import annotations

import os
import shutil
import sys

# Newest first: the check only reads TOML, so any of these behaves identically.
CANDIDATES = ("python3.14", "python3.13", "python3.12", "python3.11")


def ensure_tomllib() -> None:
    """Guarantee `import tomllib` works, re-executing this script if needed."""
    try:
        import tomllib  # noqa: F401
    except ModuleNotFoundError:
        pass
    else:
        return

    # Guard against an interpreter that satisfies the search but not the
    # import, which would otherwise re-exec forever.
    if os.environ.get("_KRAFKA_XTASK_REEXEC"):
        _die("re-executed interpreter still has no `tomllib`")

    for name in CANDIDATES:
        found = shutil.which(name)
        if not found:
            continue
        env = dict(os.environ, _KRAFKA_XTASK_REEXEC="1")
        os.execve(found, [found, *sys.argv], env)

    _die(f"no interpreter with `tomllib` found (tried: {', '.join(CANDIDATES)})")


def _die(detail: str) -> None:
    print(
        f"{sys.argv[0]}: {detail}.\n"
        f"  This check parses TOML, which needs Python 3.11+ "
        f"(running {sys.version.split()[0]}).\n"
        f"  Install a newer Python, or run it directly: "
        f"python3.13 {sys.argv[0]}",
        file=sys.stderr,
    )
    raise SystemExit(2)
