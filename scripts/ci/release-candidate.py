#!/usr/bin/env python3
"""Validate and query the immutable M1 release-candidate artifact bundle."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import sys
from typing import Any

SCHEMA = "graphforge-release-record-v1"
SHA_RE = re.compile(r"[0-9a-f]{40}")
HASH_RE = re.compile(r"[0-9a-f]{64}")
CRATES = (
    "graphforge-core",
    "graphforge-ast",
    "graphforge-knowledge",
    "graphforge-ontology",
    "graphforge-provenance",
    "graphforge-ir",
    "graphforge-plan",
    "graphforge-storage",
    "graphforge-io",
    "graphforge-rel",
    "graphforge-search",
    "graphforge-cypher",
    "graphforge-exec",
    "graphforge-api",
    "graphforge-cli",
)
NPM_PACKAGES = (
    "@curatelabs/graphforge-darwin-arm64",
    "@curatelabs/graphforge-darwin-x64",
    "@curatelabs/graphforge-linux-arm64-gnu",
    "@curatelabs/graphforge-linux-x64-gnu",
    "@curatelabs/graphforge-win32-x64-msvc",
    "@curatelabs/graphforge",
    "@curatelabs/graphforge-cli",
    "@curatelabs/graphforge-agent-skills",
)


class CandidateError(ValueError):
    """The candidate bundle does not satisfy the release contract."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise CandidateError(f"cannot read release record {path}: {error}") from error
    if not isinstance(value, dict):
        raise CandidateError("release record must be a JSON object")
    return value


def validate(
    record_path: Path, artifacts_dir: Path, expected_sha: str, version: str
) -> dict[str, Any]:
    if SHA_RE.fullmatch(expected_sha) is None:
        raise CandidateError("expected SHA must be 40 lowercase hexadecimal characters")
    record = _load(record_path)
    if record.get("schema") != SCHEMA:
        raise CandidateError(f"unexpected release record schema: {record.get('schema')!r}")
    if record.get("version") != version or record.get("tag") != f"v{version}":
        raise CandidateError("release record version/tag does not match the requested version")
    if record.get("commit_sha") != expected_sha:
        raise CandidateError("release record commit does not match the requested SHA")

    items = record.get("artifacts")
    if not isinstance(items, list) or not items:
        raise CandidateError("release record has no artifacts")

    seen_paths: set[str] = set()
    names_by_surface: dict[str, set[str]] = {"pypi": set(), "npm": set(), "crates": set()}
    wheel_count = 0
    sdist_count = 0
    addon_count = 0
    for index, item in enumerate(items):
        if not isinstance(item, dict):
            raise CandidateError(f"artifacts[{index}] must be an object")
        relative = item.get("path")
        digest = item.get("sha256")
        if not isinstance(relative, str) or not relative or relative.startswith(("/", "../")):
            raise CandidateError(f"artifacts[{index}] has an unsafe path")
        if relative in seen_paths:
            raise CandidateError(f"duplicate artifact path: {relative}")
        seen_paths.add(relative)
        path = artifacts_dir / relative
        if not path.is_file():
            raise CandidateError(f"recorded artifact is missing: {relative}")
        if not isinstance(digest, str) or HASH_RE.fullmatch(digest) is None:
            raise CandidateError(f"artifacts[{index}] has an invalid SHA-256")
        if _sha256(path) != digest:
            raise CandidateError(f"artifact checksum mismatch: {relative}")
        if item.get("version") != version:
            raise CandidateError(f"artifact version mismatch: {relative}")
        surface = item.get("surface")
        name = item.get("name")
        if surface in names_by_surface and isinstance(name, str):
            names_by_surface[surface].add(name)
        if item.get("class") == "python-wheel":
            wheel_count += 1
        elif item.get("class") == "python-sdist":
            sdist_count += 1
        elif item.get("class") == "node-addon":
            addon_count += 1

    actual_files = {
        str(path.relative_to(artifacts_dir))
        for path in artifacts_dir.rglob("*")
        if path.is_file() and not path.name.startswith(".")
    }
    if actual_files != seen_paths:
        missing = sorted(seen_paths - actual_files)
        extra = sorted(actual_files - seen_paths)
        raise CandidateError(f"record/file inventory drift: missing={missing} extra={extra}")
    if wheel_count != 3 or sdist_count != 1 or names_by_surface["pypi"] != {"graphforge"}:
        raise CandidateError("candidate must contain three graphforge wheels plus its sdist")
    if addon_count != 5:
        raise CandidateError("candidate must contain the five tested Node addons")
    if names_by_surface["npm"] != set(NPM_PACKAGES):
        raise CandidateError(
            "candidate npm set mismatch: "
            f"expected={list(NPM_PACKAGES)} actual={sorted(names_by_surface['npm'])}"
        )
    if names_by_surface["crates"] != set(CRATES):
        raise CandidateError(
            f"candidate crates set mismatch: expected={list(CRATES)} "
            f"actual={sorted(names_by_surface['crates'])}"
        )
    return record


def npm_paths(record: dict[str, Any]) -> list[str]:
    by_name = {
        item["name"]: item["path"] for item in record["artifacts"] if item.get("surface") == "npm"
    }
    return [by_name[name] for name in NPM_PACKAGES]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("validate", "npm-paths"))
    parser.add_argument("--record", type=Path, required=True)
    parser.add_argument("--artifacts-dir", type=Path, required=True)
    parser.add_argument("--expected-sha", required=True)
    parser.add_argument("--version", required=True)
    args = parser.parse_args(argv)
    try:
        record = validate(args.record, args.artifacts_dir, args.expected_sha, args.version)
        if args.command == "npm-paths":
            print("\n".join(npm_paths(record)))
        else:
            print(
                f"release-candidate: valid version={args.version} "
                f"sha={args.expected_sha} artifacts={len(record['artifacts'])}"
            )
        return 0
    except CandidateError as error:
        print(f"release-candidate: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
