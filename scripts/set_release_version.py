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
    python3 scripts/set_release_version.py 0.6.0-rc.1

One release version, spelled in each ecosystem's canonical form (ADR 0033):

============ ================ ================== ==================
Surface      Release          Development        Prerelease
============ ================ ================== ==================
cargo        ``0.6.0``        ``0.6.0-dev``      ``0.6.0-rc.1``
npm          ``0.6.0``        ``0.6.0-dev.0``    ``0.6.0-rc.1``
Python       ``0.6.0``        ``0.6.0.dev0``     ``0.6.0rc1``
============ ================ ================== ==================

A prerelease has exactly one canonical root spelling, ``MAJOR.MINOR.PATCH-rc.N``
(ADR 0034, issue #858): lowercase ``rc``, a dot separator, and a numeric
counter with no leading zero. ``0.6.0-rc1``, ``0.6.0rc1``, ``0.6.0-RC.1``,
``0.6.0-rc.01`` and ``1.0.0-beta.2`` are all refused. SemVer prerelease
spellings are not injective into PEP 440 -- the first four above all normalize
to ``0.6.0rc1`` -- so admitting more than one would let two distinct cargo/npm
versions share one Python identity and defeat the ADR 0017 one-version rule.

The Python prerelease spelling is whatever PEP 440 normalization produces from
the root version. It is derived by ``packaging``, never written by hand, and it
is not a second version.
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


RELEASE_RE = re.compile(r"\d+\.\d+\.\d+")
# The one canonical prerelease identifier: lowercase `rc`, a dot separator and a
# numeric counter with no leading zero (ADR 0034, issue #858). Exactly one
# spelling is admitted per release because SemVer prerelease spellings are not
# injective into PEP 440.
RC_COUNTER = r"(?:0|[1-9]\d*)"
PRERELEASE_RE = re.compile(rf"(\d+\.\d+\.\d+)-(rc\.{RC_COUNTER})")
CANONICAL_HINT = "X.Y.Z, X.Y.Z-dev, or X.Y.Z-rc.N (lowercase 'rc', no leading zero in N)"


def parse_base(version: str) -> tuple[str, bool, str | None]:
    """Return (MAJOR.MINOR.PATCH, is_dev, prerelease identifier or None)."""
    raw = version.strip()
    if not raw:
        raise ValueError("version must be non-empty")
    dev = False
    if raw.endswith("-dev") or raw.endswith(".dev0") or re.search(r"-dev\.\d+$", raw):
        dev = True
        raw = re.sub(r"(-dev(\.\d+)?)|(\.dev0)$", "", raw)
    if RELEASE_RE.fullmatch(raw):
        return raw, dev, None
    match = PRERELEASE_RE.fullmatch(raw)
    if match is None or dev:
        raise ValueError(f"unsupported version '{version}' (expected {CANONICAL_HINT})")
    base, prerelease = match.groups()
    # Fail closed here rather than at a registry writer: a prerelease we cannot
    # spell for PEP 440 is not a publishable GraphForge version (ADR 0033).
    python_version(base, dev=False, pre=prerelease)
    return base, dev, prerelease


def _pep440_prerelease(base: str, pre: str) -> str:
    """Return the PEP 440 spelling of one prerelease, derived by ``packaging``.

    ``packaging`` is imported lazily so that plain releases and development
    versions -- the only shapes this tool supported before ADR 0033 -- never
    depend on it.
    """
    counter = re.fullmatch(rf"rc\.({RC_COUNTER})", pre)
    if counter is None:
        raise ValueError(
            f"prerelease identifier '{pre}' is not canonical; "
            "the only release-candidate spelling is 'rc.N' with lowercase 'rc' "
            "and no leading zero in N (ADR 0034)"
        )
    try:
        from packaging.version import InvalidVersion, Version
    except ModuleNotFoundError as exc:  # pragma: no cover - environment defect
        raise ValueError(
            "prerelease versions need the 'packaging' distribution to derive the "
            "PEP 440 spelling; install it before setting a prerelease version"
        ) from exc
    raw = f"{base}-{pre}"
    try:
        parsed = Version(raw)
    except InvalidVersion as exc:
        raise ValueError(f"prerelease '{raw}' has no PEP 440 spelling") from exc
    if (
        parsed.base_version != base
        or parsed.pre is None
        or parsed.epoch
        or parsed.dev is not None
        or parsed.post is not None
        or parsed.local is not None
    ):
        raise ValueError(f"prerelease '{raw}' is not a plain PEP 440 prerelease of {base}")
    # Verify the reverse mapping: the normalized spelling must name the same
    # canonical candidate it came from, never a neighbouring one (issue #858).
    if parsed.pre != ("rc", int(counter.group(1))):
        raise ValueError(f"prerelease '{raw}' does not project to the same release candidate")
    return str(parsed)


def cargo_version(base: str, *, dev: bool, pre: str | None = None) -> str:
    if dev:
        return f"{base}-dev"
    return f"{base}-{pre}" if pre else base


def python_version(base: str, *, dev: bool, pre: str | None = None) -> str:
    if dev:
        return f"{base}.dev0"
    return _pep440_prerelease(base, pre) if pre else base


def npm_version(base: str, *, dev: bool, pre: str | None = None) -> str:
    if dev:
        return f"{base}-dev.0"
    return f"{base}-{pre}" if pre else base


def python_spelling(version: str) -> str:
    """Return the PEP 440 spelling of one root release version (ADR 0033)."""
    base, dev, pre = parse_base(version)
    return python_version(base, dev=dev, pre=pre)


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


def expected_for(base: str, *, dev: bool, pre: str | None = None) -> dict[str, str]:
    return {
        "cargo": cargo_version(base, dev=dev, pre=pre),
        "python": python_version(base, dev=dev, pre=pre),
        "node": npm_version(base, dev=dev, pre=pre),
        "cli": npm_version(base, dev=dev, pre=pre),
        "skills": npm_version(base, dev=dev, pre=pre),
    }


def check_aligned() -> list[str]:
    """Return drift errors if surfaces disagree on base/dev."""
    current = read_current()
    errors: list[str] = []
    try:
        base, dev, pre = parse_base(current["cargo"])
    except ValueError as exc:
        return [f"cargo version unusable: {exc}"]
    expected = expected_for(base, dev=dev, pre=pre)
    shape = f"cargo base {base} dev={dev}" + (f" pre={pre}" if pre else "")
    for key, want in expected.items():
        got = current[key]
        if got != want:
            errors.append(f"{key}: got {got!r}, expected {want!r} for {shape}")
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


def apply_version(base: str, *, dev: bool, dry_run: bool, pre: str | None = None) -> dict[str, str]:
    expected = expected_for(base, dev=dev, pre=pre)
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
        help="Target version (e.g. 0.5.0, 0.5.0-dev, or 0.6.0-rc.1)",
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
        base, dev, pre = parse_base(args.version)
        mapping = apply_version(base, dev=dev, pre=pre, dry_run=args.dry_run)
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
