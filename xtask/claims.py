#!/usr/bin/env python3
"""Generate the capability claims from data, and fail when they drift.

A negative claim ("X is not implemented") names nothing a compiler or a link
checker can resolve, so when X ships the sentence survives every gate; a
positive claim can name a type that never existed. The fix is to write neither
by hand: the published KIP table, the not-implemented list, the Cargo-feature
table and the tracked Kafka release are generated from registries, and the
registries are checked against the code.

# Sources

  - `xtask/kips.toml` — one entry per KIP named in published prose (status,
    evidence, reason), scope exclusions, Cargo feature descriptions, and the
    `DELIBERATE_GAPS` APIs that are version ceilings rather than absences.
  - `Cargo.toml` `[features]` — names and the `default` set.
  - the `api_versions!` table in `src/protocol/mod.rs`.
  - `xtask/kafka_protocol_snapshot.json` — `kafka_ref`.
  - `DELIBERATE_GAPS` in `xtask/protocol_parity.py`, reasons included.

# What it checks

  1. Every KIP entry is well formed: a known status, a reason unless
     `implemented`, evidence unless not implemented or out of scope.
  2. Evidence resolves: `api:Name@v` names an `api_versions!` row reaching v
     (a row gated on a feature also needs that `feature:`), `feature:x` is
     declared, `symbol:krafka::…` names an item defined in `src/`. Symbols are
     also written to `tests/kip_evidence.rs`, which the compiler resolves
     exactly in every `cargo check`/`cargo test`.
  3. A `not-implemented` entry's API evidence has no row; every `absent` API
     is in `DELIBERATE_GAPS` and has no row; no `DELIBERATE_GAPS` API has a
     row unless declared a ceiling.
  4. Every Cargo feature except `default` has a description, and every
     description names a declared feature.
  5. Every generated region equals what the registries produce now.
  6. The highest Kafka version in CI's integration matrix and in
     `just integration-matrix`'s default has the snapshot's minor.

Regions are `<!-- generated:kips:NAME -->` … `<!-- /generated -->` in
Markdown, inline or spanning lines; nothing outside them is read or written.

Run: python3 xtask/claims.py            # check (just claims-check)
     python3 xtask/claims.py --write    # regenerate (just gen-claims)
     python3 xtask/claims.py --self-test   # the planted-violation controls
"""

from __future__ import annotations

import json
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
FIX = "edit xtask/kips.toml (or the registry named), then run `just gen-claims`"
STATUSES = ("implemented", "partial", "not-implemented", "out-of-scope")
SITE_KIPS = "https://hupe1980.github.io/krafka/docs/protocol/#kip-support"

# Files that may hold generated regions.
REGION_FILES = (
    "README.md",
    "site/content/docs/protocol.md",
    "site/content/docs/configuration.md",
)
EVIDENCE_RS = "tests/kip_evidence.rs"

REGION = re.compile(
    r"(?P<open><!-- generated:kips:(?P<name>[\w-]+) -->)(?P<body>.*?)(?P<close><!-- /generated -->)",
    re.S,
)
ROW = re.compile(
    r'"(?P<api>\w+)"\s*\[(?P<key>\d+)\]\s*(?:cfg\((?P<cfg>[^)]*\)?)\))?\s*=>\s*'
    r"\w+\s*=\s*(?P<min>\d+)\s*\.\.=\s*\w+\s*=\s*(?P<max>\d+)"
)


class Invalid(Exception):
    pass


# ── Registries --------------------------------------------------------------


def load(root: Path) -> dict:
    kips = tomllib.loads((root / "xtask/kips.toml").read_text())
    cargo = tomllib.loads((root / "Cargo.toml").read_text())
    snapshot = json.loads((root / "xtask/kafka_protocol_snapshot.json").read_text())
    source = (root / "src/protocol/mod.rs").read_text()
    body = source[source.index("api_versions! {") :]
    rows = []
    for m in ROW.finditer(body):
        cfg = (m.group("cfg") or "").strip()
        gate = re.fullmatch(r'feature\s*=\s*"([\w-]+)"', cfg)
        rows.append(
            {
                "api": m.group("api"),
                "min": int(m.group("min")),
                "max": int(m.group("max")),
                # The feature the row needs, or None for an ungated row and for
                # the `not(feature)` fallback row, which every build has.
                "feature": gate.group(1) if gate else None,
            }
        )
    return {
        "kips": kips.get("kip", []),
        "scope": kips.get("scope", []),
        "features_doc": {k: v["description"] for k, v in kips.get("feature", {}).items()},
        "ceilings": kips.get("gaps", {}).get("ceiling", []),
        "features": cargo["features"],
        "kafka_ref": str(snapshot["kafka_ref"]),
        "rows": rows,
        "gaps": deliberate_gaps(root),
        "src": "\n".join(p.read_text() for p in sorted((root / "src").rglob("*.rs"))),
    }


def deliberate_gaps(root: Path) -> dict[str, str]:
    """`DELIBERATE_GAPS` as protocol_parity.py declares it, without importing it."""
    text = (root / "xtask/protocol_parity.py").read_text()
    m = re.search(r"^DELIBERATE_GAPS[^=]*=\s*(\{.*?^\})", text, re.S | re.M)
    if not m:
        raise SystemExit("claims: DELIBERATE_GAPS not found in xtask/protocol_parity.py")
    return eval(m.group(1), {})  # noqa: S307 — a literal from this repository


def default_closure(features: dict) -> set[str]:
    seen: set[str] = set()
    todo = list(features.get("default", []))
    while todo:
        f = todo.pop()
        if f in features and f not in seen:
            seen.add(f)
            todo.extend(features[f])
    return seen


# ── Validation --------------------------------------------------------------


def parse_evidence(item: str) -> tuple[str, str, int | None]:
    kind, _, rest = item.partition(":")
    if kind == "api":
        name, _, version = rest.partition("@")
        return kind, name, int(version) if version else 0
    if kind in ("feature", "symbol"):
        return kind, rest, None
    raise Invalid(f"unknown evidence kind {item!r}")


def defined_names(src: str) -> set[str]:
    """Every name `src/` defines or re-exports: items, enum variants, and
    struct fields (the admin_options! macro names its setters after them)."""
    names = set(re.findall(r"\b(?:fn|struct|enum|trait|type|const|static|mod)\s+(\w+)", src))
    names |= set(re.findall(r"^\s*(?:pub )?(\w+)\s*(?:[:,({=]|$)", src, re.M))
    for group in re.findall(r"\bpub use ([^;]*);", src):
        names |= set(re.findall(r"\w+", group))
    return names


def symbol_defined(path: str, names: set[str]) -> bool:
    """A coarse, fast pre-check; `tests/kip_evidence.rs` is the exact one."""
    return path.startswith("krafka::") and path.rsplit("::", 1)[1] in names


def validate(d: dict) -> list[str]:
    errors: list[str] = []
    rows_by_api: dict[str, list[dict]] = {}
    for r in d["rows"]:
        rows_by_api.setdefault(r["api"], []).append(r)
    declared = set(d["features"]) - {"default"}
    seen: set[str] = set()
    names = defined_names(d["src"])

    for entry in d["kips"] + d["scope"]:
        eid = entry.get("id", "<no id>")
        where = f"xtask/kips.toml {eid}"
        if eid in seen:
            errors.append(f"{where}: duplicate entry")
        seen.add(eid)
        status = entry.get("status")
        if status not in STATUSES:
            errors.append(f"{where}: status {status!r} is not one of {', '.join(STATUSES)}")
            continue
        if not entry.get("title"):
            errors.append(f"{where}: no title")
        if status != "implemented" and not entry.get("reason"):
            errors.append(f"{where}: status {status} needs a reason")
        evidence = entry.get("evidence", [])
        if status in ("implemented", "partial") and not evidence:
            errors.append(f"{where}: status {status} needs evidence")
        features = {e.split(":", 1)[1] for e in evidence if e.startswith("feature:")}
        for item in evidence:
            try:
                kind, name, version = parse_evidence(item)
            except (Invalid, ValueError) as e:
                errors.append(f"{where}: {e}")
                continue
            if kind == "api":
                rows = rows_by_api.get(name, [])
                if status == "not-implemented":
                    if rows:
                        errors.append(
                            f"{where}: not-implemented, but `{name}` has an api_versions! row"
                        )
                    continue
                reaching = [r for r in rows if r["max"] >= version]
                if not reaching:
                    errors.append(f"{where}: evidence {item} — no api_versions! row reaches v{version}")
                elif all(r["feature"] and r["feature"] not in features for r in reaching):
                    errors.append(
                        f"{where}: evidence {item} — only the `{reaching[0]['feature']}` row reaches "
                        f"v{version}; add feature:{reaching[0]['feature']}"
                    )
            elif kind == "feature" and name not in declared:
                errors.append(f"{where}: evidence {item} — no such Cargo feature")
            elif kind == "symbol" and not symbol_defined(name, names):
                errors.append(f"{where}: evidence {item} — no item `{name.rsplit('::', 1)[-1]}` in src/")
        for api in entry.get("absent", []):
            if api not in d["gaps"]:
                errors.append(f"{where}: absent API {api} is not in DELIBERATE_GAPS")
            if api in rows_by_api:
                errors.append(f"{where}: absent API {api} has an api_versions! row")

    for api in d["gaps"]:
        if api in rows_by_api and api not in d["ceilings"]:
            errors.append(
                f"src/protocol/mod.rs: api_versions! has a row for {api}, which DELIBERATE_GAPS "
                "lists as absent — remove the gap, or declare it under [gaps] ceiling"
            )
    for api in d["ceilings"]:
        if api not in d["gaps"]:
            errors.append(f"xtask/kips.toml [gaps]: ceiling {api} is not in DELIBERATE_GAPS")

    for f in sorted(declared - set(d["features_doc"])):
        errors.append(f"Cargo.toml: feature `{f}` has no description in xtask/kips.toml [feature.{f}]")
    for f in sorted(set(d["features_doc"]) - declared):
        errors.append(f"xtask/kips.toml [feature.{f}]: describes a feature Cargo.toml does not declare")
    return errors


# ── Rendering ---------------------------------------------------------------


def render_evidence(item: str) -> str:
    kind, name, version = parse_evidence(item)
    if kind == "api":
        return f"`{name}` v{version}+"
    if kind == "feature":
        return f"feature `{name}`"
    return f"`{'::'.join(name.split('::')[-2:])}`"


def status_label(status: str) -> str:
    return status.replace("-", " ")


def gated_versions(d: dict, feature: str) -> str:
    out = []
    for r in d["rows"]:
        if r["feature"] != feature:
            continue
        fallback = max(
            (x["max"] for x in d["rows"] if x["api"] == r["api"] and x["feature"] is None),
            default=r["min"] - 1,
        )
        lo = fallback + 1
        out.append(f"`{r['api']}` v{lo}" if lo == r["max"] else f"`{r['api']}` v{lo}–v{r['max']}")
    return ", ".join(out)


def kip_number(entry: dict) -> int:
    return int(entry["id"].split("-")[1])


def render(d: dict) -> dict[str, str]:
    kips = sorted(d["kips"], key=kip_number)
    by_status = {s: [k for k in kips if k["status"] == s] for s in STATUSES}

    table = ["", "| KIP | Title | Status | Evidence | Note |", "|-----|-------|--------|----------|------|"]
    for k in kips:
        evidence = ", ".join(render_evidence(e) for e in k.get("evidence", [])) or "—"
        table.append(
            f"| {k['id']} | {k['title']} | {status_label(k['status'])} | {evidence} | {k.get('reason', '')} |"
        )
    table.append("")

    counts = []
    for s, entries in by_status.items():
        if entries:
            ids = f" ({', '.join(k['id'] for k in entries)})" if s != "implemented" else ""
            counts.append(f"{len(entries)} {status_label(s)}{ids}")
    summary = [
        "",
        f"**KIPs named in this documentation:** {' · '.join(counts)}. Each with its "
        f"status, evidence and reason: [KIP support]({SITE_KIPS}).",
        "",
    ]

    gaps = [""]
    for k in by_status["not-implemented"] + by_status["out-of-scope"] + d["scope"]:
        gaps.append(f"- **{k['title']}** ({k['id']}) — {k['reason']}")
    for k in by_status["partial"]:
        gaps.append(f"- **{k['id']} {k['title']}**, partly — {k['reason']}")
    for api in d["ceilings"]:
        gaps.append(f"- `{api}` is implemented below Kafka's ceiling — {d['gaps'][api]}")
    absent = {a for k in kips for a in k.get("absent", [])}
    internal = [a for a in d["gaps"] if a not in absent and a not in d["ceilings"]]
    if internal:
        gaps.append(
            "- Not spoken by a client (broker-, controller- and KRaft-internal): "
            + ", ".join(f"`{a}`" for a in internal)
        )
    gaps.append("")

    defaults = default_closure(d["features"])
    features = [
        "",
        "| Feature | Default | Gates API versions | Description |",
        "|---------|---------|--------------------|-------------|",
    ]
    for name in d["features"]:
        if name == "default":
            continue
        features.append(
            f"| `{name}` | {'**yes**' if name in defaults else 'no'} | "
            f"{gated_versions(d, name) or '—'} | {d['features_doc'].get(name, '')} |"
        )
    features.append("")

    return {
        "kafka-ref": f"Apache Kafka {d['kafka_ref']}",
        "summary": "\n".join(summary),
        "kip-table": "\n".join(table),
        "not-implemented": "\n".join(gaps),
        "features": "\n".join(features),
    }


def render_evidence_rs(d: dict) -> str:
    lines = [
        "//! Generated by `just gen-claims` from `xtask/kips.toml`; do not edit.",
        "//!",
        "//! Every `symbol:` evidence in the KIP registry, named so the compiler",
        "//! resolves it. A KIP that cites an item the crate does not have fails",
        "//! `cargo check --all-targets` here.",
        "// `{ .. }` names a variant whatever its shape, so a unit variant gets one too.",
        "#![allow(dead_code, unused_imports, clippy::unneeded_struct_pattern)]",
        "",
    ]
    body: list[str] = []
    for entry in sorted(d["kips"], key=kip_number):
        features = sorted(e.split(":", 1)[1] for e in entry.get("evidence", []) if e.startswith("feature:"))
        gates = [f'feature = "{f}"' for f in features]
        cfg = None
        if len(gates) == 1:
            cfg = f"#[cfg({gates[0]})]"
        elif gates:
            cfg = "#[cfg(all(" + ", ".join(gates) + "))]"
        for item in entry.get("evidence", []):
            if not item.startswith("symbol:"):
                continue
            path = item.split(":", 1)[1]
            parent, last = path.rsplit("::", 1)
            owner = parent.rsplit("::", 1)[-1]
            line_cfg = f"    {cfg}\n" if cfg else ""
            if owner[:1].isupper() and last[:1].isupper():
                stmt = f"let _ = |v: {parent}| matches!(v, {path} {{ .. }});"
            elif owner[:1].isupper():
                stmt = f"let _ = {path};"
            else:
                stmt = f"{{ use {path} as _; }}"
            body.append(f"    // {entry['id']}\n{line_cfg}    {stmt}")
    lines.append("#[rustfmt::skip]")
    lines.append("fn evidence() {")
    lines.extend(body)
    lines.append("}")
    return "\n".join(lines) + "\n"


# ── Regions -----------------------------------------------------------------


def apply_regions(text: str, rendered: dict[str, str], path: str, errors: list[str]) -> str:
    def sub(m: re.Match) -> str:
        name = m.group("name")
        if name not in rendered:
            errors.append(f"{path}: unknown region generated:kips:{name}")
            return m.group(0)
        return m.group("open") + rendered[name] + m.group("close")

    return REGION.sub(sub, text)


def matrix_errors(root: Path, kafka_ref: str) -> list[str]:
    errors = []
    minor = tuple(int(x) for x in kafka_ref.split(".")[:2])
    sources = {
        ".github/workflows/ci.yml": r'kafka-version:\s*"(\d+\.\d+\.\d+)"',
        "justfile": r'^integration-matrix versions="([^"]+)"',
    }
    for rel, pattern in sources.items():
        text = (root / rel).read_text()
        found = []
        for m in re.finditer(pattern, text, re.M):
            line = text.count("\n", 0, m.start()) + 1
            for v in m.group(1).split():
                found.append((tuple(int(x) for x in v.split(".")[:2]), v, line))
        if not found:
            errors.append(f"{rel}: no Kafka integration matrix found")
            continue
        top = max(found)
        if top[0] != minor:
            errors.append(
                f"{rel}:{top[2]}: highest integration-matrix Kafka is {top[1]}, but the protocol "
                f"snapshot tracks {kafka_ref} — move the matrix to the tracked release"
            )
    return errors


def run(root: Path, write: bool) -> list[str]:
    try:
        d = load(root)
    except (KeyError, ValueError, tomllib.TOMLDecodeError) as e:
        return [f"claims: cannot load registries: {e}"]
    errors = validate(d)
    if errors:
        return errors + [f"→ {FIX}"]
    rendered = render(d)
    outputs = {rel: None for rel in REGION_FILES}
    for rel in REGION_FILES:
        text = (root / rel).read_text()
        new = apply_regions(text, rendered, rel, errors)
        if new != text:
            if write:
                (root / rel).write_text(new)
            else:
                for m_old, m_new in zip(REGION.finditer(text), REGION.finditer(new)):
                    if m_old.group(0) != m_new.group(0):
                        line = text.count("\n", 0, m_old.start()) + 1
                        errors.append(
                            f"{rel}:{line}: region generated:kips:{m_old.group('name')} differs "
                            f"from the registries — do not edit it by hand; {FIX}"
                        )
        outputs[rel] = new
    evidence = render_evidence_rs(d)
    current = (root / EVIDENCE_RS).read_text() if (root / EVIDENCE_RS).is_file() else None
    if evidence != current:
        if write:
            (root / EVIDENCE_RS).write_text(evidence)
        else:
            errors.append(f"{EVIDENCE_RS}: out of date — {FIX}")
    errors += matrix_errors(root, d["kafka_ref"])
    return errors


# ── Self-test --------------------------------------------------------------


def _copy(tmp: Path) -> Path:
    root = tmp / "repo"
    for rel in (*REGION_FILES, EVIDENCE_RS, "Cargo.toml", "justfile", ".github/workflows/ci.yml",
                "xtask/kips.toml", "xtask/kafka_protocol_snapshot.json", "xtask/protocol_parity.py"):
        (root / rel).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(ROOT / rel, root / rel)
    shutil.copytree(ROOT / "src", root / "src")
    return root


def _edit(path: Path, old: str, new: str, count: int = 1) -> None:
    text = path.read_text()
    if old not in text:
        raise SystemExit(f"self-test: plant anchor not found in {path}: {old!r}")
    path.write_text(text.replace(old, new, count))


def _kip(root: Path, kip: str, old: str, new: str) -> None:
    """Edit one line of one entry in kips.toml."""
    p = root / "xtask/kips.toml"
    text = p.read_text()
    start = text.index(f'id = "{kip}"')
    end = text.find("[[", start)
    end = len(text) if end < 0 else end
    block = text[start:end]
    if old not in block:
        raise SystemExit(f"self-test: {old!r} not in {kip}")
    p.write_text(text[:start] + block.replace(old, new, 1) + text[end:])


def _region_body(root: Path, rel: str, name: str, suffix: str) -> None:
    p = root / rel
    marker = f"<!-- generated:kips:{name} -->"
    _edit(p, marker, marker + suffix)


def self_test() -> int:
    plants = [
        ("hand edit in the README KIP summary", "README.md", lambda r: _region_body(r, "README.md", "summary", "x")),
        ("hand edit in the README not-implemented list", "generated:kips:not-implemented",
         lambda r: _region_body(r, "README.md", "not-implemented", "\n- GSSAPI is supported\n")),
        ("hand edit in an inline Kafka-release region", "generated:kips:kafka-ref",
         lambda r: _region_body(r, "README.md", "kafka-ref", "x")),
        ("hand edit in the site KIP table", "site/content/docs/protocol.md",
         lambda r: _region_body(r, "site/content/docs/protocol.md", "kip-table", "x")),
        ("hand edit in the site feature table", "site/content/docs/configuration.md",
         lambda r: _region_body(r, "site/content/docs/configuration.md", "features", "x")),
        ("KIP-1206 evidenced by a type that does not exist", "ShareAcquireMode",
         lambda r: _kip(r, "KIP-1206", "symbol:krafka::share_consumer::AcquireMode::RecordLimit",
                        "symbol:krafka::share_consumer::ShareAcquireMode")),
        ("a not-implemented KIP whose API has a row", "not-implemented, but `ShareFetch`",
         lambda r: _kip(r, "KIP-1206", 'status = "implemented"',
                        'status = "not-implemented"\nreason = "planted"')),
        ("an api_versions! row for a DELIBERATE_GAPS API", "row for StreamsGroupHeartbeat",
         lambda r: _edit(r / "src/protocol/mod.rs", "api_versions! {\n",
                         'api_versions! {\n    "StreamsGroupHeartbeat" [88] => SGH_MIN = 0 ..= SGH_MAX = 0, "x";\n')),
        ("a Cargo feature with no description", "feature `planted` has no description",
         lambda r: _edit(r / "Cargo.toml", "[features]\n", "[features]\nplanted = []\n")),
        ("kafka_ref moved to 4.4.0 without regenerating", "generated:kips:kafka-ref differs",
         lambda r: _edit(r / "xtask/kafka_protocol_snapshot.json", '"kafka_ref": "4.3"', '"kafka_ref": "4.4.0"')),
        ("CI matrix left at 4.3 after regenerating for 4.4.0", ".github/workflows/ci.yml",
         lambda r: (_edit(r / "xtask/kafka_protocol_snapshot.json", '"kafka_ref": "4.3"', '"kafka_ref": "4.4.0"'),
                    run(r, write=True))),
        ("a stale kip_evidence.rs", EVIDENCE_RS,
         lambda r: _edit(r / EVIDENCE_RS, "fn evidence() {", "fn evidence() {\n    // edited")),
    ]
    failed = 0
    with tempfile.TemporaryDirectory() as tmp:
        clean = run(_copy(Path(tmp)), write=False)
    if clean:
        print("  ✗ unplanted copy fails:\n    " + "\n    ".join(clean))
        failed += 1
    for desc, expect, plant in plants:
        with tempfile.TemporaryDirectory() as tmp:
            root = _copy(Path(tmp))
            plant(root)
            errors = run(root, write=False)
        caught = any(expect in e for e in errors)
        failed += not caught
        print(f"  {'✓' if caught else '✗'} {desc}" + ("" if caught else f" — got: {errors}"))
    if failed:
        print(f"✗ claims self-test: {failed} control(s) failed", file=sys.stderr)
        return 1
    print(f"✓ claims self-test: {len(plants)} plants caught, clean copy passes")
    return 0


def main(argv: list[str]) -> int:
    if argv == ["--self-test"]:
        return self_test()
    write = argv == ["--write"]
    if argv and not write:
        print(__doc__.split("Run:", 1)[1].rstrip(), file=sys.stderr)
        return 2
    errors = run(ROOT, write)
    if errors:
        print(f"✗ claims: {len(errors)} problem(s)", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1
    d = load(ROOT)
    print(
        f"✓ claims {'written' if write else 'current'}: {len(d['kips'])} KIPs, {len(d['scope'])} scope "
        f"entries, {len(d['features']) - 1} features, Kafka {d['kafka_ref']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
