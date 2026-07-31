#!/usr/bin/env python3
"""Set or check the GraphForge release version across publish surfaces.

Surfaces:
- Cargo workspace ``[workspace.package].version``
- Cargo lockfile entries for workspace packages
- Python ``crates/gf-bindings-py/pyproject.toml`` (PEP 440)
- Node ``crates/gf-bindings-node/package.json``
- NPX lifecycle CLI ``packages/cli/package.json``
- NPX skills ``packages/agent-skills/package.json``
- NPX skills ``packages/agent-skills/compatibility.json``

Usage:
    python3 scripts/set_release_version.py --check
    python3 scripts/set_release_version.py 0.5.0 --dry-run
    python3 scripts/set_release_version.py 0.5.0
    python3 scripts/set_release_version.py 0.5.0-dev

Dev forms:
- Cargo / Node / skills: ``0.5.0-dev`` (Cargo) and ``0.5.0-dev.0`` (npm)
- Python PEP 440: ``0.5.0.dev0``
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
CARGO_TOML = ROOT / "Cargo.toml"
CARGO_LOCK = ROOT / "Cargo.lock"
PYPROJECT = ROOT / "crates" / "gf-bindings-py" / "pyproject.toml"
NODE_PACKAGE = ROOT / "crates" / "gf-bindings-node" / "package.json"
CLI_PACKAGE = ROOT / "packages" / "cli" / "package.json"
SKILLS_PACKAGE = ROOT / "packages" / "agent-skills" / "package.json"
SKILLS_COMPATIBILITY = ROOT / "packages" / "agent-skills" / "compatibility.json"


def cargo_lock_versions() -> dict[str, str]:
    """Return versions for local gf-* packages recorded in Cargo.lock."""
    text = CARGO_LOCK.read_text(encoding="utf-8")
    return dict(re.findall(r'(?m)^name = "(gf-[^"]+)"\nversion = "([^"]+)"$', text))


def parse_base(version: str) -> tuple[str, bool]:
    """Return (MAJOR.MINOR.PATCH, is_dev)."""
    raw = version.strip()
    if not raw:
        raise ValueError("version must be non-empty")
    dev = False
    if raw.endswith("-dev") or raw.endswith(".dev0") or re.search(r"-dev\.\d+$", raw):
        dev = True
        raw = re.sub(r"(-dev(\.\d+)?)|(\.dev0)$", "", raw)
    if not re.fullmatch(r"\d+\.\d+\.\d+", raw):
        raise ValueError(f"unsupported version '{version}' (expected X.Y.Z or X.Y.Z-dev)")
    return raw, dev


def cargo_version(base: str, *, dev: bool) -> str:
    return f"{base}-dev" if dev else base


def python_version(base: str, *, dev: bool) -> str:
    return f"{base}.dev0" if dev else base


def npm_version(base: str, *, dev: bool) -> str:
    return f"{base}-dev.0" if dev else base


def read_current() -> dict[str, str]:
    cargo = re.search(
        r'(?m)^version\s*=\s*"([^"]+)"',
        CARGO_TOML.read_text(encoding="utf-8"),
    )
    py = re.search(
        r'(?m)^version\s*=\s*"([^"]+)"',
        PYPROJECT.read_text(encoding="utf-8"),
    )
    node = json.loads(NODE_PACKAGE.read_text(encoding="utf-8"))["version"]
    cli = json.loads(CLI_PACKAGE.read_text(encoding="utf-8"))["version"]
    skills = json.loads(SKILLS_PACKAGE.read_text(encoding="utf-8"))["version"]
    if not cargo or not py:
        raise ValueError("could not read Cargo or Python version")
    return {
        "cargo": cargo.group(1),
        "python": py.group(1),
        "node": node,
        "cli": cli,
        "skills": skills,
    }


def expected_for(base: str, *, dev: bool) -> dict[str, str]:
    return {
        "cargo": cargo_version(base, dev=dev),
        "python": python_version(base, dev=dev),
        "node": npm_version(base, dev=dev),
        "cli": npm_version(base, dev=dev),
        "skills": npm_version(base, dev=dev),
    }


def check_aligned() -> list[str]:
    """Return drift errors if surfaces disagree on base/dev."""
    current = read_current()
    errors: list[str] = []
    try:
        base, dev = parse_base(current["cargo"])
    except ValueError as exc:
        return [f"cargo version unusable: {exc}"]
    expected = expected_for(base, dev=dev)
    for key, want in expected.items():
        got = current[key]
        if got != want:
            errors.append(f"{key}: got {got!r}, expected {want!r} for cargo base {base} dev={dev}")
    lock_versions = cargo_lock_versions()
    for package, got in sorted(lock_versions.items()):
        if got != expected["cargo"]:
            errors.append(f"Cargo.lock {package}: got {got!r}, expected {expected['cargo']!r}")
    compatibility = json.loads(SKILLS_COMPATIBILITY.read_text(encoding="utf-8"))
    if compatibility.get("package_version") != expected["skills"]:
        errors.append(
            "skills compatibility package_version: got "
            f"{compatibility.get('package_version')!r}, expected {expected['skills']!r}"
        )
    return errors


def apply_version(base: str, *, dev: bool, dry_run: bool) -> dict[str, str]:
    expected = expected_for(base, dev=dev)
    if dry_run:
        return expected

    cargo_text = CARGO_TOML.read_text(encoding="utf-8")
    cargo_text, n = re.subn(
        r'(?m)^(version\s*=\s*")[^"]+(")',
        rf"\g<1>{expected['cargo']}\2",
        cargo_text,
        count=1,
    )
    if n != 1:
        raise ValueError("failed to update Cargo.toml workspace version")
    CARGO_TOML.write_text(cargo_text, encoding="utf-8")

    lock_text = CARGO_LOCK.read_text(encoding="utf-8")

    def update_lock(match: re.Match[str]) -> str:
        return f"{match.group(1)}{expected['cargo']}{match.group(2)}"

    lock_text, lock_count = re.subn(
        r'(?m)^(name = "gf-[^"]+"\nversion = ")[^"]+(")$',
        update_lock,
        lock_text,
    )
    if lock_count == 0:
        raise ValueError("failed to update Cargo.lock workspace package versions")
    CARGO_LOCK.write_text(lock_text, encoding="utf-8")

    py_text = PYPROJECT.read_text(encoding="utf-8")
    py_text, n = re.subn(
        r'(?m)^(version\s*=\s*")[^"]+(")',
        rf"\g<1>{expected['python']}\2",
        py_text,
        count=1,
    )
    if n != 1:
        raise ValueError("failed to update Python pyproject version")
    PYPROJECT.write_text(py_text, encoding="utf-8")

    for path, key in (
        (NODE_PACKAGE, "node"),
        (CLI_PACKAGE, "cli"),
        (SKILLS_PACKAGE, "skills"),
    ):
        meta = json.loads(path.read_text(encoding="utf-8"))
        meta["version"] = expected[key]
        path.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")

    compatibility = json.loads(SKILLS_COMPATIBILITY.read_text(encoding="utf-8"))
    compatibility["package_version"] = expected["skills"]
    SKILLS_COMPATIBILITY.write_text(json.dumps(compatibility, indent=2) + "\n", encoding="utf-8")

    return expected


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "version",
        nargs="?",
        help="Target version (e.g. 0.5.0 or 0.5.0-dev)",
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Verify Cargo/Python/Node/skills versions are aligned",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the mapping without writing files",
    )
    args = parser.parse_args(argv)

    if args.check:
        current = read_current()
        print("current:")
        for key, value in current.items():
            print(f"  {key}: {value}")
        errors = check_aligned()
        if errors:
            for error in errors:
                print(f"set-release-version: {error}", file=sys.stderr)
            return 1
        print("set-release-version: aligned")
        return 0

    if not args.version:
        parser.error("version is required unless --check")

    try:
        base, dev = parse_base(args.version)
        mapping = apply_version(base, dev=dev, dry_run=args.dry_run)
    except ValueError as exc:
        print(f"set-release-version: {exc}", file=sys.stderr)
        return 1

    action = "would set" if args.dry_run else "set"
    print(f"{action}:")
    for key, value in mapping.items():
        print(f"  {key}: {value}")
    if not args.dry_run:
        print()
        print("Next: review git diff before committing or publishing.")
        print("Do not push registry tags from this script.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
