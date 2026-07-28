#!/usr/bin/env python3
"""Validate that napi placed each native addon in its declared target package."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

PACKAGE_BY_TARGET = {
    "aarch64-apple-darwin": "darwin-arm64",
    "x86_64-apple-darwin": "darwin-x64",
    "aarch64-unknown-linux-gnu": "linux-arm64-gnu",
    "x86_64-unknown-linux-gnu": "linux-x64-gnu",
    "x86_64-pc-windows-msvc": "win32-x64-msvc",
}


def validate(npm_dir: Path, targets: list[str]) -> None:
    unknown = sorted(set(targets) - PACKAGE_BY_TARGET.keys())
    if unknown:
        raise ValueError(f"unsupported napi targets: {unknown}")

    addons = sorted(npm_dir.rglob("*.node"))
    if len(addons) != len(targets):
        raise ValueError(
            f"expected exactly one addon for each of {len(targets)} targets, found {len(addons)}: "
            f"{[str(path) for path in addons]}"
        )

    expected_dirs = {npm_dir / PACKAGE_BY_TARGET[target] for target in targets}
    misplaced = [path for path in addons if path.parent not in expected_dirs]
    if misplaced:
        raise ValueError(f"addons placed in the wrong target package: {misplaced}")

    for target in targets:
        expected_dir = npm_dir / PACKAGE_BY_TARGET[target]
        packaged = sorted(expected_dir.glob("*.node"))
        if len(packaged) != 1:
            raise ValueError(
                f"{target}: expected exactly one addon in {expected_dir}, found {len(packaged)}"
            )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--npm-dir", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--target", action="append")
    args = parser.parse_args()

    manifest = json.loads(args.manifest.read_text(encoding="utf-8"))
    declared_targets = manifest.get("napi", {}).get("targets")
    if not isinstance(declared_targets, list) or not declared_targets:
        raise SystemExit("manifest must declare non-empty napi.targets")
    targets = args.target or declared_targets
    undeclared = sorted(set(targets) - set(declared_targets))
    if undeclared:
        raise SystemExit(f"requested targets are not declared by the manifest: {undeclared}")
    try:
        validate(args.npm_dir, targets)
    except ValueError as error:
        raise SystemExit(f"napi artifact validation failed: {error}") from error


if __name__ == "__main__":
    main()
