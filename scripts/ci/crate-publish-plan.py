#!/usr/bin/env python3
"""Compute and validate crates.io publish order for GraphForge workspace crates.

Usage:
    python3 scripts/ci/crate-publish-plan.py list
    python3 scripts/ci/crate-publish-plan.py check
    python3 scripts/ci/crate-publish-plan.py dry-run-commands

Language-binding implementation crates are not published to crates.io; their
public distributions ship through PyPI and npm. The Rust CLI is part of the
crates.io surface.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[2]
CRATES_DIR = ROOT / "crates"

# Implementation crates published through language registries, not crates.io.
CRATES_IO_EXCLUDED = frozenset(
    {
        "graphforge-bindings-py",
        "graphforge-bindings-node",
    }
)

PATH_DEP_RE = re.compile(
    r"^(?P<name>graphforge-[a-z0-9-]+)\s*=\s*\{(?P<body>[^}]*)\}",
    re.MULTILINE,
)
PACKAGE_NAME_RE = re.compile(r'^name\s*=\s*"(?P<name>[^"]+)"', re.MULTILINE)


def _dependencies_section(text: str) -> str:
    """Return the primary [dependencies] block (exclude dev-dependencies)."""
    parts = re.split(r"^\[dev-dependencies\]", text, maxsplit=1, flags=re.MULTILINE)
    return parts[0]


def parse_crate(manifest: Path) -> tuple[str, set[str], list[str]]:
    """Return (name, workspace path deps, path-deps missing version=)."""
    text = manifest.read_text(encoding="utf-8")
    name_match = PACKAGE_NAME_RE.search(text)
    if not name_match:
        raise ValueError(f"missing package name in {manifest}")
    name = name_match.group("name")
    deps: set[str] = set()
    missing_version: list[str] = []
    for match in PATH_DEP_RE.finditer(_dependencies_section(text)):
        body = match.group("body")
        if "path" not in body:
            continue
        dep = match.group("name")
        deps.add(dep)
        if not re.search(r"\bversion\s*=", body):
            missing_version.append(dep)
    return name, deps, missing_version


def load_workspace() -> dict[str, set[str]]:
    """Map crate name → set of workspace path dependencies."""
    crates: dict[str, set[str]] = {}
    for manifest in sorted(CRATES_DIR.glob("*/Cargo.toml")):
        name, deps, _ = parse_crate(manifest)
        crates[name] = deps
    return crates


def topological_publish_order(crates: dict[str, set[str]]) -> list[str]:
    """Return crates.io candidates in dependency order (bindings excluded)."""
    remaining = {
        name: {dep for dep in deps if dep in crates and dep not in CRATES_IO_EXCLUDED}
        for name, deps in crates.items()
        if name not in CRATES_IO_EXCLUDED
    }
    # Drop edges to excluded crates from remaining dependency sets.
    for name in list(remaining):
        remaining[name] = {dep for dep in remaining[name] if dep in remaining}

    order: list[str] = []
    while remaining:
        ready = sorted(name for name, deps in remaining.items() if not deps)
        if not ready:
            cycle = ", ".join(sorted(remaining))
            raise SystemExit(f"dependency cycle among crates.io candidates: {cycle}")
        for name in ready:
            order.append(name)
            del remaining[name]
        for deps in remaining.values():
            deps.difference_update(ready)
    return order


def path_deps_missing_versions() -> dict[str, list[str]]:
    """Return crate → path deps that lack version= (blocks cargo publish)."""
    missing: dict[str, list[str]] = {}
    for manifest in sorted(CRATES_DIR.glob("*/Cargo.toml")):
        name, _, missing_version = parse_crate(manifest)
        if name in CRATES_IO_EXCLUDED:
            continue
        if missing_version:
            missing[name] = sorted(missing_version)
    return missing


def cmd_list(_: argparse.Namespace) -> int:
    order = topological_publish_order(load_workspace())
    for name in order:
        print(name)
    return 0


def cmd_check(_: argparse.Namespace) -> int:
    crates = load_workspace()
    order = topological_publish_order(crates)
    errors: list[str] = []

    unexpected = sorted(name for name in order if not name.startswith("graphforge-"))
    for name in unexpected:
        errors.append(f"{name}: publishable package must use the graphforge-* namespace")

    missing = path_deps_missing_versions()
    for name, deps in missing.items():
        errors.append(
            f"{name}: path dependencies missing version= for cargo publish: " + ", ".join(deps)
        )

    if errors:
        print("crate publish plan check FAILED:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        print(
            "See docs/development/publication-order.md (crates.io publication plan).",
            file=sys.stderr,
        )
        return 1

    print(f"crate publish plan OK ({len(order)} crates)")
    for name in order:
        print(f"  {name}")
    return 0


def cmd_dry_run_commands(_: argparse.Namespace) -> int:
    order = topological_publish_order(load_workspace())
    for name in order:
        print(f"cargo publish -p {name} --dry-run --locked")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="Print crates.io publish order").set_defaults(func=cmd_list)
    sub.add_parser(
        "check",
        help="Fail on name conflicts, cycles, or missing path+version deps",
    ).set_defaults(func=cmd_check)
    sub.add_parser(
        "dry-run-commands",
        help="Print cargo publish --dry-run commands when unblocked",
    ).set_defaults(func=cmd_dry_run_commands)

    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
