#!/usr/bin/env python3
"""Set or check the GraphForge release version across publish surfaces.

Surfaces:
- Cargo workspace ``[workspace.package].version``
- Cargo lockfile entries for workspace packages
- Python ``crates/graphforge-bindings-py/pyproject.toml`` (PEP 440)
- Node ``crates/graphforge-bindings-node/package.json``
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
PYPROJECT = ROOT / "crates" / "graphforge-bindings-py" / "pyproject.toml"
NODE_PACKAGE = ROOT / "crates" / "graphforge-bindings-node" / "package.json"
NODE_NPM_DIR = ROOT / "crates" / "graphforge-bindings-node" / "npm"
CLI_PACKAGE = ROOT / "packages" / "cli" / "package.json"
SKILLS_PACKAGE = ROOT / "packages" / "agent-skills" / "package.json"
SKILLS_COMPATIBILITY = ROOT / "packages" / "agent-skills" / "compatibility.json"


def native_npm_packages() -> list[Path]:
    """Return checked-in native platform package.json paths."""
    if not NODE_NPM_DIR.is_dir():
        return []
    return sorted(NODE_NPM_DIR.glob("*/package.json"))


PATH_VERSION_DEP = re.compile(
    r'(?m)^(graphforge-[a-z0-9-]+\s*=\s*\{\s*version\s*=\s*")([^"]+)("\s*,\s*path\s*=)'
)


def crate_manifests() -> list[Path]:
    """Return first-party crate Cargo.toml paths under crates/."""
    return sorted((ROOT / "crates").glob("*/Cargo.toml"))


def path_version_pins() -> list[tuple[Path, str, str]]:
    """Return (manifest, dependency, version) for path+version graphforge deps."""
    pins: list[tuple[Path, str, str]] = []
    for path in crate_manifests():
        text = path.read_text(encoding="utf-8")
        for match in PATH_VERSION_DEP.finditer(text):
            dependency = match.group(1).split("=", 1)[0].strip()
            pins.append((path, dependency, match.group(2)))
    return pins


def cargo_lock_versions() -> dict[str, str]:
    """Return versions for local graphforge-* packages recorded in Cargo.lock."""
    text = CARGO_LOCK.read_text(encoding="utf-8")
    return dict(re.findall(r'(?m)^name = "(graphforge-[^"]+)"\nversion = "([^"]+)"$', text))


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
    if compatibility.get("graphforge_release") != expected["skills"]:
        errors.append(
            "skills compatibility graphforge_release: got "
            f"{compatibility.get('graphforge_release')!r}, expected {expected['skills']!r}"
        )
    skills_meta = json.loads(SKILLS_PACKAGE.read_text(encoding="utf-8"))
    skills_release = (skills_meta.get("graphforgeCompatibility") or {}).get("release")
    if skills_release != expected["skills"]:
        errors.append(
            "skills package graphforgeCompatibility.release: got "
            f"{skills_release!r}, expected {expected['skills']!r}"
        )
    for path in native_npm_packages():
        meta = json.loads(path.read_text(encoding="utf-8"))
        got = meta.get("version")
        if got != expected["node"]:
            errors.append(
                f"native npm {path.parent.name}: got {got!r}, expected {expected['node']!r}"
            )
    for path, dependency, got in path_version_pins():
        if got != expected["cargo"]:
            errors.append(
                f"{path.relative_to(ROOT)} dependency {dependency}: "
                f"got {got!r}, expected {expected['cargo']!r}"
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

    staged_manifests: list[tuple[Path, str]] = []
    pin_updates = 0
    for path in crate_manifests():
        text = path.read_text(encoding="utf-8")
        updated, count = PATH_VERSION_DEP.subn(
            rf"\g<1>{expected['cargo']}\3",
            text,
        )
        if count:
            staged_manifests.append((path, updated))
            pin_updates += count
    if pin_updates == 0:
        raise ValueError("failed to update any path+version graphforge crate dependencies")

    lock_text = CARGO_LOCK.read_text(encoding="utf-8")

    def update_lock(match: re.Match[str]) -> str:
        return f"{match.group(1)}{expected['cargo']}{match.group(2)}"

    lock_text, lock_count = re.subn(
        r'(?m)^(name = "graphforge-[^"]+"\nversion = ")[^"]+(")$',
        update_lock,
        lock_text,
    )
    if lock_count == 0:
        raise ValueError("failed to update Cargo.lock workspace package versions")

    py_text = PYPROJECT.read_text(encoding="utf-8")
    py_text, n = re.subn(
        r'(?m)^(version\s*=\s*")[^"]+(")',
        rf"\g<1>{expected['python']}\2",
        py_text,
        count=1,
    )
    if n != 1:
        raise ValueError("failed to update Python pyproject version")

    staged_packages: list[tuple[Path, dict]] = []
    for path, key in (
        (NODE_PACKAGE, "node"),
        (CLI_PACKAGE, "cli"),
        (SKILLS_PACKAGE, "skills"),
    ):
        meta = json.loads(path.read_text(encoding="utf-8"))
        meta["version"] = expected[key]
        if path == SKILLS_PACKAGE:
            compatibility_meta = meta.setdefault("graphforgeCompatibility", {})
            compatibility_meta["release"] = expected[key]
        staged_packages.append((path, meta))

    for path in native_npm_packages():
        meta = json.loads(path.read_text(encoding="utf-8"))
        meta["version"] = expected["node"]
        staged_packages.append((path, meta))

    compatibility = json.loads(SKILLS_COMPATIBILITY.read_text(encoding="utf-8"))
    compatibility["package_version"] = expected["skills"]
    compatibility["graphforge_release"] = expected["skills"]

    # Commit writes only after all updates validate.
    CARGO_TOML.write_text(cargo_text, encoding="utf-8")
    for path, updated in staged_manifests:
        path.write_text(updated, encoding="utf-8")
    CARGO_LOCK.write_text(lock_text, encoding="utf-8")
    PYPROJECT.write_text(py_text, encoding="utf-8")
    for path, meta in staged_packages:
        path.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
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
