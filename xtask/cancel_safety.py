#!/usr/bin/env python3
"""Every data-path method says what dropping it does, and every "cancel safe" is tested.

For each item in IN_SCOPE the check reads its rustdoc and fails when:

  1. it has no `# Cancel safety` section, or more than one;
  2. the section's first sentence is not "This method is cancel safe." or
     "This method is not cancel safe." (for a type: "This type is …");
  3. a "not cancel safe" section stops at that sentence, saying neither what
     a drop leaves nor what to do next;
  4. a "cancel safe" claim is not named by a case in tests/cancel_safety.rs
     (the item's `Type::method` string, or the type's name).

It also requires the admin module's `# Cancel safety` paragraph, which stands
in for per-method sections on single-request admin calls.

Run: python3 xtask/cancel_safety.py
     python3 xtask/cancel_safety.py --self-test   # the planted-violation controls
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
HARNESS = ROOT / "tests" / "cancel_safety.rs"
ADMIN = ROOT / "src" / "admin" / "mod.rs"

# file -> [(type, method or None for the type itself)]
IN_SCOPE: dict[str, list[tuple[str, str | None]]] = {
    "src/producer/mod.rs": [
        ("Producer", "send"),
        ("Producer", "enqueue"),
        ("Producer", "flush"),
        ("Producer", "close"),
    ],
    "src/producer/transaction.rs": [
        ("TransactionalProducer", "send"),
        ("TransactionalProducer", "enqueue"),
        ("TransactionalProducer", "send_offsets"),
        ("TransactionalProducer", "commit"),
        ("TransactionalProducer", "abort"),
        ("TransactionalProducer", "flush"),
        ("TransactionalProducer", "close"),
    ],
    "src/producer/accumulator.rs": [("DeliveryHandle", None)],
    "src/consumer/mod.rs": [
        ("Consumer", "recv"),
        ("Consumer", "poll"),
        ("Consumer", "commit"),
        ("Consumer", "commit_offsets"),
        ("Consumer", "close"),
    ],
    "src/consumer/stream.rs": [("ConsumerStream", None)],
    "src/share_consumer/mod.rs": [
        ("ShareConsumer", "recv"),
        ("ShareConsumer", "poll"),
        ("ShareConsumer", "ack"),
        ("ShareConsumer", "release"),
        ("ShareConsumer", "reject"),
        ("ShareConsumer", "renew"),
        ("ShareConsumer", "commit"),
        ("ShareConsumer", "close_with"),
    ],
    "src/share_consumer/stream.rs": [("ShareConsumerStream", None)],
}

HEADING = "# Cancel safety"


def doc_of(text: str, type_name: str, method: str | None) -> list[str] | None:
    """The rustdoc lines of `type_name::method` (or of the type), or None."""
    lines = text.splitlines()
    if method is None:
        target = re.compile(rf"^\s*pub struct {re.escape(type_name)}\b")
        candidates = [i for i, line in enumerate(lines) if target.match(line)]
    else:
        target = re.compile(rf"^\s*pub (async )?fn {re.escape(method)}\b")
        impl = re.compile(rf"^impl(<[^>]*>)? {re.escape(type_name)}(<[^>]*>)? \{{")
        any_impl = re.compile(r"^impl\b")
        candidates = []
        inside = False
        for i, line in enumerate(lines):
            if any_impl.match(line):
                inside = bool(impl.match(line))
            elif inside and target.match(line):
                candidates.append(i)
    if len(candidates) != 1:
        return None
    j = candidates[0] - 1
    while j >= 0 and lines[j].strip().startswith("#["):
        j -= 1
    doc = []
    while j >= 0 and lines[j].strip().startswith("///"):
        doc.append(lines[j].strip()[3:].strip())
        j -= 1
    return list(reversed(doc))


def sections(doc: list[str]) -> list[str]:
    """The text of every `# Cancel safety` section."""
    found = []
    current: list[str] | None = None
    for line in doc:
        if line.startswith("# "):
            if current is not None:
                found.append(" ".join(current).strip())
            current = [] if line == HEADING else None
        elif current is not None:
            current.append(line)
    if current is not None:
        found.append(" ".join(current).strip())
    return found


def case_names(harness: str) -> set[str]:
    """Every name in a string literal of the harness: the cases' names."""
    names: set[str] = set()
    for literal in re.findall(r'"((?:[^"\\]|\\.)*)"', harness, re.S):
        names.update(part for part in re.split(r"[,\s\\]+", literal) if part)
    return names


def check_item(name: str, doc: list[str] | None, harness: str, is_type: bool) -> list[str]:
    if doc is None:
        return [f"{name}: not found (or found more than once)"]
    found = sections(doc)
    if not found:
        return [f"{name}: no `{HEADING}` section"]
    if len(found) > 1:
        return [f"{name}: {len(found)} `{HEADING}` sections, expected one"]
    subject = "type" if is_type else "method"
    safe = f"This {subject} is cancel safe."
    unsafe = f"This {subject} is not cancel safe."
    section = found[0]
    if section.startswith(safe):
        if name not in case_names(harness):
            return [f"{name}: documented cancel safe, but no case in tests/cancel_safety.rs names it"]
        return []
    if section.startswith(unsafe):
        if len(section) <= len(unsafe) + 20:
            return [f"{name}: not cancel safe, but the section says neither what a drop leaves nor how to recover"]
        return []
    return [f"{name}: the section must start with `{safe}` or `{unsafe}`"]


def check_admin(text: str) -> list[str]:
    module_doc = [line[3:].strip() for line in text.splitlines() if line.startswith("//!")]
    found = sections(module_doc)
    if len(found) != 1:
        return [f"src/admin/mod.rs: expected one module-level `{HEADING}` paragraph, found {len(found)}"]
    if "may already have been applied" not in found[0] or "idempotent" not in found[0]:
        return ["src/admin/mod.rs: the paragraph must say a dropped call may already have been applied and that only idempotent operations are safe to retry"]
    return []


def check_tree() -> list[str]:
    harness = HARNESS.read_text(encoding="utf-8")
    problems = []
    for file, items in IN_SCOPE.items():
        text = (ROOT / file).read_text(encoding="utf-8")
        for type_name, method in items:
            name = type_name if method is None else f"{type_name}::{method}"
            problems.extend(check_item(name, doc_of(text, type_name, method), harness, method is None))
    problems.extend(check_admin(ADMIN.read_text(encoding="utf-8")))
    return problems


def self_test() -> int:
    harness = 'drop_at_every_poll("Thing::poll", ...)'

    def src(doc: str) -> str:
        body = "\n".join(f"    /// {line}" if line else "    ///" for line in doc.splitlines())
        return f"pub struct Thing;\n\nimpl Thing {{\n{body}\n    pub async fn poll(&self) {{}}\n}}\n"

    def run(doc: str, harness_text: str = harness) -> list[str]:
        return check_item("Thing::poll", doc_of(src(doc), "Thing", "poll"), harness_text, False)

    good = "Poll.\n\n# Cancel safety\n\nThis method is cancel safe. Nothing is lost."
    plants = {
        "missing section": "Poll.",
        "two sections": good + "\n\n# Cancel safety\n\nThis method is cancel safe.",
        "malformed sentence": "Poll.\n\n# Cancel safety\n\nCancel-safe: see the module.",
        "unproven claim": (good, "nothing names it"),
        "bare not-safe": "Poll.\n\n# Cancel safety\n\nThis method is not cancel safe.",
    }
    failed = False
    if problems := run(good):
        print(f"✗ self-test: a correct section was reported: {problems}", file=sys.stderr)
        failed = True
    for name, plant in plants.items():
        doc, harness_text = plant if isinstance(plant, tuple) else (plant, harness)
        problems = run(doc, harness_text)
        if not problems:
            print(f"✗ self-test: planted {name} not reported", file=sys.stderr)
            failed = True
        elif "Thing::poll" not in problems[0]:
            print(f"✗ self-test: {name} reported without naming the method: {problems}", file=sys.stderr)
            failed = True
    if not check_admin("//! Admin.\n"):
        print("✗ self-test: a missing admin paragraph was not reported", file=sys.stderr)
        failed = True
    if failed:
        return 1
    print(f"✓ cancel-safety self-test: {len(plants)} plants and a missing admin paragraph reported")
    return 0


def main() -> int:
    if "--self-test" in sys.argv[1:]:
        return self_test()
    problems = check_tree()
    if problems:
        print("\n".join(f"✗ {p}" for p in problems), file=sys.stderr)
        return 1
    count = sum(len(items) for items in IN_SCOPE.values())
    print(f"✓ {count} data-path items carry a well-formed `{HEADING}` section; every claim is tested")
    return 0


if __name__ == "__main__":
    sys.exit(main())
