#!/usr/bin/env python3
"""Fail closed before a GitHub Release can publish registry artifacts.

The release workflow invokes this against the immutable release-event SHA. It
verifies that the tag, every publishable version surface, and the dated
CHANGELOG section describe the same non-development version.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
from pathlib import Path
import re
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[2]
VERSION_SCRIPT = ROOT / "scripts" / "set_release_version.py"
CHANGELOG = ROOT / "CHANGELOG.md"
DOCS_CHANGELOG = ROOT / "docs" / "reference" / "changelog.md"
DOCS_URL = "https://docs.graphforge.sh/"
REPOSITORY_URL = "https://github.com/CurateLabs/graphforge"
REPOSITORY_GIT_URL = "git+https://github.com/CurateLabs/graphforge.git"
ISSUES_URL = f"{REPOSITORY_URL}/issues"


def load_version_module():
    spec = importlib.util.spec_from_file_location("set_release_version", VERSION_SCRIPT)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {VERSION_SCRIPT}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def git_head() -> str:
    result = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--verify", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    return result.stdout.strip() if result.returncode == 0 else "unknown"


def release_version(tag: str) -> str | None:
    match = re.fullmatch(r"v(\d+\.\d+\.\d+)", tag)
    return match.group(1) if match else None


def unreleased_body(changelog: str) -> str | None:
    match = re.search(
        r"(?ms)^## \[Unreleased\]\s*\n(?P<body>.*?)(?=^## \[)",
        changelog,
    )
    return match.group("body") if match else None


def validate_metadata() -> list[str]:
    """Validate the URLs projected into every published package surface."""
    errors: list[str] = []

    cargo = (ROOT / "Cargo.toml").read_text(encoding="utf-8")
    if f'repository = "{REPOSITORY_URL}"' not in cargo:
        errors.append("Cargo workspace repository URL does not target CurateLabs/graphforge")

    for path in (
        ROOT / "pyproject.toml",
        ROOT / "crates" / "gf-bindings-py" / "pyproject.toml",
    ):
        text = path.read_text(encoding="utf-8")
        if f'Homepage = "{DOCS_URL}"' not in text:
            errors.append(f"{path.relative_to(ROOT)} Homepage does not target {DOCS_URL}")
        if f'Repository = "{REPOSITORY_URL}"' not in text:
            errors.append(f"{path.relative_to(ROOT)} Repository does not target {REPOSITORY_URL}")
        if f'Issues = "{ISSUES_URL}"' not in text:
            errors.append(f"{path.relative_to(ROOT)} Issues does not target {ISSUES_URL}")
        if "Documentation =" in text and f'Documentation = "{DOCS_URL}"' not in text:
            errors.append(f"{path.relative_to(ROOT)} Documentation does not target {DOCS_URL}")

    for path in (
        ROOT / "crates" / "gf-bindings-node" / "package.json",
        ROOT / "packages" / "cli" / "package.json",
        ROOT / "packages" / "agent-skills" / "package.json",
    ):
        package = json.loads(path.read_text(encoding="utf-8"))
        label = path.relative_to(ROOT)
        if package.get("homepage") != DOCS_URL:
            errors.append(f"{label} homepage does not target {DOCS_URL}")
        if package.get("repository", {}).get("url") != REPOSITORY_GIT_URL:
            errors.append(f"{label} repository does not target CurateLabs/graphforge")
        if package.get("bugs", {}).get("url") != ISSUES_URL:
            errors.append(f"{label} bugs URL does not target {ISSUES_URL}")
    return errors


def validate(
    *,
    tag: str,
    expected_sha: str,
    actual_sha: str,
    versions: dict[str, str],
    changelog: str,
    docs_changelog: str,
) -> list[str]:
    errors: list[str] = []
    version = release_version(tag)
    if version is None:
        return [f"release tag must be exactly vMAJOR.MINOR.PATCH, got {tag!r}"]

    if not re.fullmatch(r"[0-9a-f]{40}", expected_sha):
        errors.append("expected SHA must be a lowercase 40-character commit SHA")
    if actual_sha != expected_sha:
        errors.append(f"checked-out SHA {actual_sha!r} does not match event SHA {expected_sha!r}")

    version_module = load_version_module()
    wanted = version_module.expected_for(version, dev=False)
    for surface, expected in wanted.items():
        actual = versions.get(surface)
        if actual != expected:
            errors.append(f"{surface} version is {actual!r}; expected release version {expected!r}")

    dated_heading = re.compile(rf"(?m)^## \[{re.escape(version)}\] - \d{{4}}-\d{{2}}-\d{{2}}\s*$")
    if not dated_heading.search(changelog):
        errors.append(f"CHANGELOG lacks a dated [{version}] release heading")

    body = unreleased_body(changelog)
    if body is None:
        errors.append("CHANGELOG lacks an [Unreleased] section before the release section")
    elif re.search(r"(?m)^\s*[-*]\s+", body):
        errors.append("CHANGELOG [Unreleased] still contains release-note entries")

    current_repo = "https://github.com/CurateLabs/graphforge"
    if f"[Unreleased]: {current_repo}/compare/v{version}...HEAD" not in changelog:
        errors.append("CHANGELOG [Unreleased] comparison link does not target the current repo")
    if f"[{version}]: {current_repo}/releases/tag/v{version}" not in changelog:
        errors.append(f"CHANGELOG [{version}] link does not target the current repo release")
    if docs_changelog != changelog:
        errors.append("docs/reference/changelog.md does not exactly mirror CHANGELOG.md")
    return errors


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True, help="Release tag, e.g. v0.5.0")
    parser.add_argument("--expected-sha", required=True, help="Release-event commit SHA")
    args = parser.parse_args(argv)

    version_module = load_version_module()
    errors = validate(
        tag=args.tag,
        expected_sha=args.expected_sha,
        actual_sha=git_head(),
        versions=version_module.read_current(),
        changelog=CHANGELOG.read_text(encoding="utf-8"),
        docs_changelog=DOCS_CHANGELOG.read_text(encoding="utf-8"),
    )
    errors.extend(version_module.check_aligned())
    errors.extend(validate_metadata())
    if errors:
        for error in errors:
            print(f"release-publish-preflight: {error}", file=sys.stderr)
        return 1
    print(f"release-publish-preflight: ready tag={args.tag} sha={args.expected_sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
