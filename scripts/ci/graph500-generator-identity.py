#!/usr/bin/env python3
"""Check or regenerate current-source Graph500 generator identity bindings."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys
from typing import Any

HISTORICAL_COMMIT = "6255f9393362c996b5840566d255569761e881d7"
HISTORICAL_GENERATOR_IDENTITY = (
    "sha256:3b4bdf5ae4f41523f911dc1998db2328dadd2fa9e5f95e49478f148e81a14ce2"
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def generator_identity(root: Path) -> str:
    source = root / "benchmarks/runners/graph500-generator/src/main.rs"
    return f"sha256:{hashlib.sha256(source.read_bytes()).hexdigest()}"


def load_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: root must be an object")
    return value


def generator_action(value: dict[str, Any], path: Path) -> dict[str, Any]:
    actions = [
        phase.get("action")
        for phase in value.get("phases", [])
        if isinstance(phase, dict)
        and isinstance(phase.get("action"), dict)
        and phase["action"].get("interface") == "benchmark_generator"
    ]
    if len(actions) != 1:
        raise ValueError(f"{path}: expected exactly one benchmark_generator action")
    return actions[0]


def current_documents(root: Path) -> list[tuple[Path, list[dict[str, Any]]]]:
    documents: list[tuple[Path, list[dict[str, Any]]]] = []
    profile_dir = root / "benchmarks/profiles/graph500"
    for path in sorted(profile_dir.glob("*.json")):
        value = load_object(path)
        generator = value.get("generator")
        if not isinstance(generator, dict):
            raise ValueError(f"{path}: generator must be an object")
        documents.append((path, [generator, generator_action(value, path)]))
    fixture = root / "benchmarks/fixtures/progressive/tiny-executable.json"
    value = load_object(fixture)
    documents.append((fixture, [generator_action(value, fixture)]))
    return documents


def validate_historical_evidence(root: Path) -> None:
    path = root / "benchmarks/fixtures/parity/ladder-bundle/manifest.json"
    manifest = load_object(path)
    if manifest.get("commit") != HISTORICAL_COMMIT:
        raise ValueError(f"{path}: historical commit identity changed")
    if manifest.get("generator_identity") != HISTORICAL_GENERATOR_IDENTITY:
        raise ValueError(f"{path}: historical generator identity changed")


def update_document(path: Path, bindings: list[dict[str, Any]], expected: str) -> int:
    stale = [binding.get("identity") for binding in bindings if binding.get("identity") != expected]
    if not all(isinstance(value, str) for value in stale):
        raise ValueError(f"{path}: current generator identity must be a string")
    text = path.read_text(encoding="utf-8")
    for observed, count in Counter(stale).items():
        quoted = json.dumps(observed)
        if text.count(quoted) != count:
            raise ValueError(f"{path}: generator identity occurs outside bound fields")
        text = text.replace(quoted, json.dumps(expected))
    if stale:
        path.write_text(text, encoding="utf-8")
    return len(stale)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=repo_root())
    parser.add_argument("--write", action="store_true")
    args = parser.parse_args(argv)

    root = args.root.resolve()
    validate_historical_evidence(root)
    expected = generator_identity(root)
    stale: list[tuple[Path, list[dict[str, Any]]]] = []
    for path, bindings in current_documents(root):
        if any(binding.get("identity") != expected for binding in bindings):
            stale.append((path, bindings))
    if stale and not args.write:
        print(f"current Graph500 generator identity is stale: {expected}", file=sys.stderr)
        return 1
    updated = sum(update_document(path, bindings, expected) for path, bindings in stale)
    print(f"current Graph500 generator identity verified: {expected} ({updated} updated)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
