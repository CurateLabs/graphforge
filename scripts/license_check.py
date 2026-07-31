#!/usr/bin/env python3
"""Verify GraphForge's Apache-2.0 license and distribution metadata."""

from __future__ import annotations

import argparse
from collections.abc import Iterable
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
APACHE_SPDX = "Apache-2.0"
CANONICAL_LICENSE_SHA256 = "cfc7749b96f63bd31c3c42b5c471bf756814053e847c10f3eb003417bc523d30"

CARGO_PACKAGE_DIRS = tuple(
    ROOT / "crates" / name
    for name in (
        "gf-api",
        "gf-ast",
        "gf-bindings-node",
        "gf-bindings-py",
        "gf-cli",
        "gf-core",
        "gf-cypher",
        "gf-exec",
        "gf-io",
        "gf-ir",
        "gf-knowledge",
        "gf-ontology",
        "gf-plan",
        "gf-provenance",
        "gf-rel",
        "gf-search",
        "gf-storage",
    )
)
LICENSE_COPIES = (
    ROOT / "LICENSE",
    ROOT / "crates" / "gf-bindings-node" / "LICENSE",
    ROOT / "crates" / "gf-bindings-py" / "LICENSE",
    ROOT / "packages" / "agent-skills" / "LICENSE",
    ROOT / "packages" / "cli" / "LICENSE",
)
NOTICE_COPIES = (
    ROOT / "NOTICE",
    *(path / "NOTICE" for path in CARGO_PACKAGE_DIRS),
    ROOT / "packages" / "agent-skills" / "NOTICE",
    ROOT / "packages" / "cli" / "NOTICE",
)
THIRD_PARTY_NOTICE_COPIES = (
    ROOT / "legal" / "THIRD_PARTY_NOTICES.md",
    ROOT / "crates" / "gf-bindings-py" / "THIRD_PARTY_NOTICES.md",
    ROOT / "crates" / "gf-bindings-node" / "THIRD_PARTY_NOTICES.md",
    ROOT / "crates" / "gf-cli" / "THIRD_PARTY_NOTICES.md",
    ROOT / "packages" / "cli" / "THIRD_PARTY_NOTICES.md",
)
MANIFEST_EXPECTATIONS = {
    ROOT / "Cargo.toml": ('license = "Apache-2.0"',),
    ROOT / "pyproject.toml": ('license = "Apache-2.0"',),
    ROOT / "package.json": ('"license": "Apache-2.0"',),
    ROOT / "crates" / "gf-bindings-py" / "pyproject.toml": (
        'license = "Apache-2.0"',
        'license-files = ["LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md"]',
    ),
    ROOT / "crates" / "gf-bindings-node" / "package.json": (
        '"license": "Apache-2.0"',
        '"LICENSE"',
        '"NOTICE"',
        '"THIRD_PARTY_NOTICES.md"',
    ),
    ROOT / "packages" / "agent-skills" / "package.json": (
        '"license": "Apache-2.0"',
        '"LICENSE"',
        '"NOTICE"',
    ),
    ROOT / "packages" / "cli" / "package.json": (
        '"license": "Apache-2.0"',
        '"LICENSE"',
        '"NOTICE"',
        '"THIRD_PARTY_NOTICES.md"',
    ),
    ROOT / "docs-site" / "package.json": ('"license": "Apache-2.0"',),
    ROOT / "tests" / "features" / "node" / "package.json": ('"license": "Apache-2.0"',),
    ROOT / "fuzz" / "Cargo.toml": ('license = "Apache-2.0"',),
}

FORBIDDEN_TERMS = (
    "BU" + "SL-1.1",
    "Business Source" + " License",
    "source" + "-available",
    "AG" + "PL-3.0",
    "Additional Use" + " Grant",
    "Change" + " Date",
    "commercial" + " license",
    "Contributor License" + " Agreement",
)
FORBIDDEN_PATTERNS = (
    *(re.compile(re.escape(term), re.IGNORECASE) for term in FORBIDDEN_TERMS),
    re.compile(r"\bC" + r"LA\b"),
)
SCAN_EXCLUSIONS = frozenset(THIRD_PARTY_NOTICE_COPIES)
IGNORED_PARTS = {".git", ".venv", "node_modules", "target"}


def label(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return path.name


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def tracked_text_paths() -> list[Path]:
    result = subprocess.run(
        ["git", "-C", str(ROOT), "ls-files", "-z"],
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        return []
    paths: list[Path] = []
    for raw in result.stdout.split(b"\0"):
        if not raw:
            continue
        try:
            path = ROOT / raw.decode("utf-8")
        except UnicodeDecodeError:
            continue
        if path not in SCAN_EXCLUSIONS and path.is_file():
            paths.append(path)
    return paths


def validate_forbidden_claims(paths: Iterable[Path] | None = None) -> list[str]:
    errors: list[str] = []
    for path in paths if paths is not None else tracked_text_paths():
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for pattern in FORBIDDEN_PATTERNS:
            if pattern.search(text):
                errors.append(f"{label(path)} retains forbidden licensing claim: {pattern.pattern}")
    return errors


def validate_repository() -> list[str]:
    """Return all Apache-2.0 policy violations in the current tree."""
    errors: list[str] = []
    canonical = LICENSE_COPIES[0].read_text(encoding="utf-8") if LICENSE_COPIES[0].exists() else ""
    if sha256_text(canonical) != CANONICAL_LICENSE_SHA256:
        errors.append("LICENSE does not match the canonical Apache-2.0 text")
    for path in LICENSE_COPIES:
        if not path.exists() or path.read_text(encoding="utf-8") != canonical:
            errors.append(f"{label(path)} is missing or differs from LICENSE")

    notice = NOTICE_COPIES[0].read_text(encoding="utf-8") if NOTICE_COPIES[0].exists() else ""
    for path in NOTICE_COPIES:
        if not path.exists() or path.read_text(encoding="utf-8") != notice:
            errors.append(f"{label(path)} is missing or differs from NOTICE")

    third_party = (
        THIRD_PARTY_NOTICE_COPIES[0].read_text(encoding="utf-8")
        if THIRD_PARTY_NOTICE_COPIES[0].exists()
        else ""
    )
    if not third_party.strip():
        errors.append("legal/THIRD_PARTY_NOTICES.md is missing or empty")
    else:
        for path in THIRD_PARTY_NOTICE_COPIES:
            if not path.exists() or path.read_text(encoding="utf-8") != third_party:
                errors.append(f"{label(path)} differs from legal/THIRD_PARTY_NOTICES.md")
        if "Do not edit this file by hand" not in third_party:
            errors.append("legal/THIRD_PARTY_NOTICES.md lacks generated-file marker")

    for path, snippets in MANIFEST_EXPECTATIONS.items():
        text = path.read_text(encoding="utf-8") if path.exists() else ""
        for snippet in snippets:
            if snippet not in text:
                errors.append(f"{label(path)} lacks {snippet}")

    for path in ROOT.rglob("Cargo.toml"):
        if IGNORED_PARTS.intersection(path.parts):
            continue
        text = path.read_text(encoding="utf-8")
        if 'license = "Apache-2.0"' not in text and "license.workspace = true" not in text:
            errors.append(f"{label(path)} lacks Apache-2.0 license metadata")
        if path.parent in CARGO_PACKAGE_DIRS and "license-file.workspace = true" not in text:
            errors.append(f"{label(path)} does not package the workspace LICENSE")

    for path in ROOT.rglob("package.json"):
        if IGNORED_PARTS.intersection(path.parts):
            continue
        try:
            package = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            errors.append(f"{label(path)} is invalid JSON: {exc}")
            continue
        if package.get("license") != APACHE_SPDX:
            errors.append(f"{label(path)} lacks Apache-2.0 license metadata")

    for path in ROOT.rglob("pyproject.toml"):
        if IGNORED_PARTS.intersection(path.parts):
            continue
        if 'license = "Apache-2.0"' not in path.read_text(encoding="utf-8"):
            errors.append(f"{label(path)} lacks Apache-2.0 license metadata")

    if (ROOT / ("C" + "LA.md")).exists():
        errors.append("legacy contributor agreement must not remain")
    contributing = (ROOT / "CONTRIBUTING.md").read_text(encoding="utf-8")
    for required in ("CODE_OF_CONDUCT.md", "docs/legal/licensing.md", "SECURITY.md"):
        if required not in contributing:
            errors.append(f"CONTRIBUTING.md does not route contributors to {required}")

    errors.extend(validate_forbidden_claims())
    return errors


def build_report(errors: list[str]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "status": "pass" if not errors else "fail",
        "spdx_expression": APACHE_SPDX,
        "license_sha256": CANONICAL_LICENSE_SHA256,
        "license_copies": [label(path) for path in LICENSE_COPIES],
        "notice_copies": [label(path) for path in NOTICE_COPIES],
        "third_party_notice_copies": [label(path) for path in THIRD_PARTY_NOTICE_COPIES],
        "errors": errors,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args(argv)
    errors = validate_repository()
    if args.report:
        args.report.write_text(json.dumps(build_report(errors), indent=2) + "\n", encoding="utf-8")
    if errors:
        for error in errors:
            print(f"license-check: {error}", file=sys.stderr)
        return 1
    print("license-check: compliant")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
