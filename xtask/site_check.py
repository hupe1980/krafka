#!/usr/bin/env python3
"""Structural invariants for the documentation site.

# Why this exists

The Jekyll site this replaced had three faults that no build step could see,
because Jekyll happily serves a broken site:

  - `performance.md` had **no front matter at all**, so Jekyll treated it as a
    static file and shipped raw Markdown. It was linked from the README and the
    home page.
  - Four pairs of pages shared a `nav_order`: 5, 6, 8 and 9 each appeared
    twice, leaving the order of half the navigation up to the sort's tie-break.
  - `schema-registry.md` — 711 lines — was reachable from no index at all.

Zola catches the first (a page it cannot parse fails the build) and, with
`internal_level = "error"`, catches broken cross-links. It cannot catch the
other two, because a duplicate weight and an unlisted page are both valid
sites. Hence this.

# What it checks

  1. Every page has a title, a description and a weight.
  2. Weights are unique, so navigation order is total rather than tie-broken.
  3. Every page appears in exactly one `extra.nav` group, and every group entry
     names a page that exists — no orphans in either direction.
  4. `slug_id` matches the filename, since the nav joins on it.
  5. Descriptions fit a search result: 50–160 characters.
  6. (Version consistency moved to `xtask/version_check.py`, which covers the
     lockfiles and dependency snippets too.)
  7. The protocol guide's API table matches `src/protocol/mod.rs` exactly — no
     missing API, no stale version range.
  8. Every guide is linked from the README, at the path the site serves.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _bootstrap import ensure_tomllib  # noqa: E402

ensure_tomllib()

import tomllib  # noqa: E402

ROOT = Path(__file__).resolve().parent.parent
SITE = ROOT / "site"
DOCS = SITE / "content" / "docs"


def front_matter(path: Path) -> dict:
    text = path.read_text()
    if not text.startswith("+++\n"):
        raise ValueError(f"{path.name} has no TOML front matter")
    end = text.index("\n+++", 3)
    return tomllib.loads(text[4:end])


def main() -> int:
    errors: list[str] = []

    config = tomllib.loads((SITE / "zola.toml").read_text())
    pages: dict[str, dict] = {}

    for path in sorted(DOCS.glob("*.md")):
        if path.stem == "_index":
            continue
        try:
            fm = front_matter(path)
        except ValueError as e:
            errors.append(str(e))
            continue
        pages[path.stem] = fm

        for key in ("title", "description", "weight"):
            if key not in fm:
                errors.append(f"{path.name}: missing `{key}`")

        slug = fm.get("extra", {}).get("slug_id")
        if slug != path.stem:
            errors.append(
                f"{path.name}: slug_id is {slug!r}, expected {path.stem!r} — "
                "the sidebar joins pages to nav groups on this field"
            )

        desc = fm.get("description", "")
        if desc and not 50 <= len(desc) <= 160:
            errors.append(
                f"{path.name}: description is {len(desc)} chars; search results "
                "truncate past ~160 and under ~50 says too little"
            )

    weights: dict[int, list[str]] = {}
    for slug, fm in pages.items():
        weights.setdefault(fm.get("weight", -1), []).append(slug)
    for weight, slugs in sorted(weights.items()):
        if len(slugs) > 1:
            errors.append(
                f"weight {weight} is shared by {', '.join(sorted(slugs))} — "
                "navigation order between them is undefined"
            )

    listed: list[str] = []
    for group in config.get("extra", {}).get("nav", []):
        for slug in group["pages"]:
            listed.append(slug)
            if slug not in pages:
                errors.append(
                    f"nav group {group['title']!r} lists {slug!r}, which has no page"
                )
    for slug in sorted(set(listed)):
        if listed.count(slug) > 1:
            errors.append(f"{slug} appears in more than one nav group")
    for slug in sorted(pages):
        if slug not in listed:
            errors.append(
                f"{slug}.md is in no nav group, so nothing links to it — "
                "this is how schema-registry.md went unreachable"
            )

    # Version consistency lives in `xtask/version_check.py`, which covers the
    # lockfiles and every documented dependency snippet as well as this file.
    site_version = config.get("extra", {}).get("version")

    # The README is the front door and is not built by Zola, so nothing else
    # checks it. Every one of its thirteen documentation links was a 404: they
    # pointed at `/krafka/producer`, but the site serves `/krafka/docs/producer/`.
    # Schema Registry was missing from the list entirely.
    base = config["base_url"].rstrip("/")
    readme = (ROOT / "README.md").read_text()
    linked = set(re.findall(re.escape(base) + r"/docs/([a-z0-9-]+)/", readme))
    for slug in sorted(re.findall(re.escape(base) + r"/(?!docs/)([a-z0-9-]+)/?\)", readme)):
        if slug in pages:
            errors.append(
                f"README links to {base}/{slug} but the site serves "
                f"{base}/docs/{slug}/ — the page path is missing `/docs/`"
            )
    for slug in sorted(linked - set(pages)):
        errors.append(f"README links to /docs/{slug}/, which has no page")
    for slug in sorted(set(pages) - linked):
        errors.append(
            f"README does not link to {slug} — this is how a 711-line "
            "schema-registry guide went unlisted"
        )

    # The protocol guide is the doc most likely to rot silently: the crate's
    # version table moves whenever Kafka does (`just protocol-parity`), and
    # nothing tied the guide to it. Seven implemented APIs were missing from the
    # table when this was added — the *versions* were all correct, which is why
    # a hand review had not caught it.
    protocol_md = DOCS / "protocol.md"
    if protocol_md.exists():
        src = (ROOT / "src" / "protocol" / "mod.rs").read_text()
        # An entry may carry a `cfg(..)` clause, and a feature-gated API appears
        # twice with different ceilings — `InitProducerId` is v0–v5 normally and
        # v0–v6 under `unstable-protocol`. Collect every declared range per name
        # and accept the guide naming any of them, since the table documents the
        # default feature set.
        crate_apis: dict[str, set[tuple[int, int]]] = {}
        for m in re.finditer(
            r'"(\w+)"\s*\[(\d+)\]\s*(?:cfg\(.*?\)\s*)?=>\s*\w+_MIN\s*=\s*(\d+)'
            r"\s*\.\.=\s*\w+_MAX\s*=\s*(\d+)",
            src,
        ):
            crate_apis.setdefault(m.group(1), set()).add(
                (int(m.group(3)), int(m.group(4)))
            )
        # Names may carry a footnote marker in the table.
        # The max cell may carry a feature-gated ceiling in the guide's own
        # notation — `4 (5¹)` means "v4 by default, v5 under
        # `unstable-protocol`" — so collect every number it names.
        doc_apis: dict[str, tuple[int, set[int]]] = {}
        for m in re.finditer(
            r"^\|\s*`?([A-Za-z][A-Za-z ]*?)`?[¹²³*†]*\s*\|\s*(\d+)\s*\|([^|]+)\|",
            protocol_md.read_text(),
            re.M,
        ):
            maxes = {int(v) for v in re.findall(r"\d+", m.group(3))}
            doc_apis[m.group(1).replace(" ", "")] = (int(m.group(2)), maxes)
        for name, ranges in sorted(crate_apis.items()):
            if name not in doc_apis:
                shown = " or ".join(f"v{lo}–v{hi}" for lo, hi in sorted(ranges))
                errors.append(
                    f"protocol.md does not list {name} ({shown}), "
                    "which the crate implements"
                )
            else:
                doc_min, doc_maxes = doc_apis[name]
                crate_min = min(lo for lo, _ in ranges)
                crate_maxes = {hi for _, hi in ranges}
                if doc_min != crate_min:
                    errors.append(
                        f"protocol.md says {name} starts at v{doc_min}, "
                        f"but the crate says v{crate_min}"
                    )
                for hi in sorted(crate_maxes - doc_maxes):
                    errors.append(
                        f"protocol.md does not mention {name} v{hi}, which the crate "
                        "can advertise — a feature-gated ceiling is written `5 (6¹)`"
                    )
        for name in sorted(set(doc_apis) - set(crate_apis)):
            errors.append(f"protocol.md lists {name}, which the crate does not implement")

    if errors:
        print("Site structure check FAILED\n", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1

    print(
        f"✓ Site structure: {len(pages)} pages, unique weights, all reachable "
        f"from the sidebar and the README, protocol table matches the crate, "
        f"version {site_version}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
