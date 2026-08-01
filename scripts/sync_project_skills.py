#!/usr/bin/env python3
"""Generate and verify packaged copies of canonical project-local skills."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "project-skills"
MANIFEST = SOURCE / "manifest.json"
DISTRIBUTION_COPIES = (
    ROOT / "crates" / "graphforge-bindings-py" / "python" / "graphforge" / "_project_skills",
    ROOT / "packages" / "cli" / "project-skills",
)
SKILL_NAMES = ("graphforge-bootstrap", "graphforge-build-knowledge")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def payload_paths() -> list[Path]:
    return sorted(
        path for name in SKILL_NAMES for path in (SOURCE / name).rglob("*") if path.is_file()
    )


def expected_manifest() -> dict[str, object]:
    files = [
        {
            "path": path.relative_to(SOURCE).as_posix(),
            "sha256": sha256(path),
        }
        for path in payload_paths()
    ]
    return {
        "schema_version": 1,
        "bundle_version": 1,
        "graphforge_compatibility": ">=0.5.0 <0.6.0",
        "skills": list(SKILL_NAMES),
        "files": files,
    }


def encoded_manifest() -> bytes:
    return (json.dumps(expected_manifest(), indent=2) + "\n").encode()


def source_files() -> list[Path]:
    return [SOURCE / "README.md", *payload_paths(), MANIFEST]


def write() -> None:
    MANIFEST.write_bytes(encoded_manifest())
    for destination in DISTRIBUTION_COPIES:
        if destination.exists():
            shutil.rmtree(destination)
        for source in source_files():
            target = destination / source.relative_to(SOURCE)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, target)


def check() -> list[str]:
    errors: list[str] = []
    expected = encoded_manifest()
    if not MANIFEST.exists() or MANIFEST.read_bytes() != expected:
        errors.append("project-skills/manifest.json is stale; run with --write")
    for destination in DISTRIBUTION_COPIES:
        expected_paths = {path.relative_to(SOURCE).as_posix() for path in source_files()}
        actual_paths = (
            {
                path.relative_to(destination).as_posix()
                for path in destination.rglob("*")
                if path.is_file()
            }
            if destination.exists()
            else set()
        )
        if actual_paths != expected_paths:
            errors.append(f"{destination.relative_to(ROOT)} file set differs from canonical source")
            continue
        for source in source_files():
            relative = source.relative_to(SOURCE)
            if (destination / relative).read_bytes() != source.read_bytes():
                errors.append(
                    f"{(destination / relative).relative_to(ROOT)} differs from canonical source"
                )
    return errors


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write",
        action="store_true",
        help="regenerate the canonical manifest and distribution copies",
    )
    args = parser.parse_args()
    if args.write:
        write()
    errors = check()
    for error in errors:
        print(f"project-skills: {error}", file=sys.stderr)
    if errors:
        return 1
    print("project-skills: canonical and packaged assets are byte-identical")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
