#!/usr/bin/env python3
"""Fail-closed Bazel migration ledger completeness check (#6).

Every Cargo workspace target must appear in the checked-in target map as either
`mapped` (with a Bazel label) or `exception` (with a justified exception id).
Unmapped rows and stub retained-tool exceptions fail the gate.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

SCHEMA = "graphforge.bazel-migration-target-map.v1"
DEFAULT_MAP = Path("tools/bazel/parity/migration_target_map.json")
DEFAULT_LEDGER = Path("docs/development/bazel-migration-ledger.md")
ALLOWED_EXCEPTION_STATUS = frozenset({"justified", "handoff", "closed", "mapped", "excluded"})
KIND_TO_CLASS = {
    "lib": "lib",
    "rlib": "lib",
    "dylib": "lib",
    "cdylib": "cdylib",
    "bin": "bin",
    "test": "integration-test",
    "example": "example",
    "custom-build": "custom-build",
}


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def cargo_targets(root: Path) -> list[tuple[str, str, str, str]]:
    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--format-version=1", "--no-deps", "--locked"],
            cwd=root,
            text=True,
        )
    )
    workspace_members = set(metadata["workspace_members"])
    rows: list[tuple[str, str, str, str]] = []
    for package in metadata["packages"]:
        if package["id"] not in workspace_members:
            continue
        for target in package["targets"]:
            kind = (target.get("kind") or ["?"])[0]
            cls = KIND_TO_CLASS.get(kind, kind)
            src = target["src_path"]
            prefix = str(root) + "/"
            if src.startswith(prefix):
                src = src[len(prefix) :]
            rows.append((package["name"], target["name"], cls, src))
    rows.sort()
    return rows


def load_map(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("schema") != SCHEMA:
        raise SystemExit(f"unexpected target-map schema: {payload.get('schema')!r}")
    return payload


def markdown_has_unmapped(ledger: Path) -> list[str]:
    if not ledger.is_file():
        return [f"missing ledger markdown: {ledger}"]
    text = ledger.read_text(encoding="utf-8")
    hits: list[str] = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        if re.search(r"\| `unmapped` \|", line):
            hits.append(f"{ledger}:{lineno}: unmapped ledger row")
    return hits


def check(
    *,
    root: Path,
    map_path: Path,
    ledger_path: Path,
) -> list[str]:
    errors: list[str] = []
    payload = load_map(map_path)
    mapped_entries = {(entry["package"], entry["target"]): entry for entry in payload["targets"]}
    exceptions = {entry["id"]: entry for entry in payload.get("exceptions", [])}

    cargo_rows = cargo_targets(root)
    if len(cargo_rows) != payload.get("cargo_target_count"):
        errors.append(
            "cargo_target_count mismatch: "
            f"map={payload.get('cargo_target_count')} cargo={len(cargo_rows)}"
        )

    cargo_keys = {(pkg, tgt) for pkg, tgt, _cls, _src in cargo_rows}
    map_keys = set(mapped_entries)
    for missing in sorted(cargo_keys - map_keys):
        errors.append(f"cargo target missing from map: {missing[0]}::{missing[1]}")
    for extra in sorted(map_keys - cargo_keys):
        errors.append(f"map entry not in cargo metadata: {extra[0]}::{extra[1]}")

    for pkg, tgt, cls, src in cargo_rows:
        entry = mapped_entries.get((pkg, tgt))
        if entry is None:
            continue
        status = entry.get("status")
        if status == "unmapped":
            errors.append(f"unmapped target fails ledger: {pkg}::{tgt}")
            continue
        if status == "mapped":
            label = entry.get("bazel_label")
            if not label or not str(label).startswith("//"):
                errors.append(f"mapped target missing bazel_label: {pkg}::{tgt}")
            if entry.get("class") != cls:
                # Allow ledger class names that match cargo normalization.
                errors.append(
                    f"class mismatch for {pkg}::{tgt}: map={entry.get('class')} cargo={cls}"
                )
            if entry.get("source") != src:
                errors.append(
                    f"source mismatch for {pkg}::{tgt}: map={entry.get('source')} cargo={src}"
                )
            continue
        if status == "exception":
            exception_id = entry.get("exception_id")
            if not exception_id or exception_id not in exceptions:
                errors.append(
                    f"exception target missing justified id: {pkg}::{tgt} ({exception_id!r})"
                )
                continue
            exc_status = exceptions[exception_id].get("status")
            if exc_status not in ALLOWED_EXCEPTION_STATUS:
                errors.append(
                    f"unjustified retained exception {exception_id}: status={exc_status!r}"
                )
            continue
        errors.append(f"unknown status {status!r} for {pkg}::{tgt}")

    for exception_id, exc in sorted(exceptions.items()):
        status = exc.get("status")
        if status == "stub":
            errors.append(f"unjustified retained exception stub: {exception_id}")
        elif status not in ALLOWED_EXCEPTION_STATUS:
            errors.append(f"retained exception {exception_id} has invalid status {status!r}")
        if not (exc.get("justification") or "").strip():
            errors.append(f"retained exception {exception_id} missing justification")

    errors.extend(markdown_has_unmapped(ledger_path))
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=None)
    parser.add_argument("--map", type=Path, default=DEFAULT_MAP)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    args = parser.parse_args(argv)

    root = args.root.resolve() if args.root else repo_root()
    map_path = args.map if args.map.is_absolute() else root / args.map
    ledger_path = args.ledger if args.ledger.is_absolute() else root / args.ledger

    errors = check(root=root, map_path=map_path, ledger_path=ledger_path)
    if errors:
        print("bazel migration ledger check FAILED:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    payload = load_map(map_path)
    print(
        "bazel migration ledger check OK: "
        f"targets={payload['cargo_target_count']} "
        f"exceptions={len(payload.get('exceptions', []))}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
