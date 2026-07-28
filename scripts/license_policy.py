#!/usr/bin/env python3
"""Generate and verify GraphForge's release-specific license policy."""

from __future__ import annotations

import argparse
from calendar import isleap
from datetime import date
import hashlib
import json
from pathlib import Path
import re
import subprocess
import sys
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "license-policy.json"
TERMS_PATH = ROOT / "legal" / "BUSL-1.1-terms.txt"
GRANT_PATH = ROOT / "legal" / "ADDITIONAL-USE-GRANT.txt"
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
THIRD_PARTY_NOTICE = ROOT / "legal" / "THIRD_PARTY_NOTICES.md"
THIRD_PARTY_NOTICE_COPIES = (
    THIRD_PARTY_NOTICE,
    ROOT / "crates" / "gf-bindings-py" / "THIRD_PARTY_NOTICES.md",
    ROOT / "crates" / "gf-bindings-node" / "THIRD_PARTY_NOTICES.md",
    ROOT / "crates" / "gf-cli" / "THIRD_PARTY_NOTICES.md",
)
LICENSE_COPIES = (
    ROOT / "LICENSE",
    ROOT / "crates" / "gf-bindings-node" / "LICENSE",
    ROOT / "crates" / "gf-bindings-py" / "LICENSE",
    ROOT / "packages" / "agent-skills" / "LICENSE",
)
NOTICE_COPIES = (
    ROOT / "NOTICE",
    *(path / "NOTICE" for path in CARGO_PACKAGE_DIRS),
    ROOT / "packages" / "agent-skills" / "NOTICE",
)
MANIFEST_EXPECTATIONS = {
    ROOT / "Cargo.toml": ('license = "BUSL-1.1"',),
    ROOT / "pyproject.toml": ('license = "BUSL-1.1"',),
    ROOT / "package.json": ('"license": "BUSL-1.1"',),
    ROOT / "crates" / "gf-bindings-py" / "pyproject.toml": (
        'license = "BUSL-1.1"',
        'license-files = ["LICENSE", "NOTICE", "THIRD_PARTY_NOTICES.md"]',
    ),
    ROOT / "crates" / "gf-bindings-node" / "package.json": (
        '"license": "BUSL-1.1"',
        '"LICENSE"',
        '"NOTICE"',
        '"THIRD_PARTY_NOTICES.md"',
    ),
    ROOT / "packages" / "agent-skills" / "package.json": (
        '"license": "BUSL-1.1"',
        '"LICENSE"',
        '"NOTICE"',
    ),
    ROOT / "tests" / "features" / "node" / "package.json": ('"license": "BUSL-1.1"',),
    ROOT / "fuzz" / "Cargo.toml": ('license = "BUSL-1.1"',),
}
CURRENT_DOCS = (
    ROOT / "README.md",
    ROOT / "docs" / "index.md",
    ROOT / "CONTRIBUTING.md",
    ROOT / "docs" / "development" / "contributing.md",
    ROOT / "CLA.md",
    ROOT / "CODE_OF_CONDUCT.md",
    ROOT / "docs" / "community" / "code-of-conduct.md",
    ROOT / "docs" / "legal" / "licensing.md",
    ROOT / ".github" / "pull_request_template.md",
    ROOT / ".github" / "ISSUE_TEMPLATE" / "config.yml",
)
FORBIDDEN_CURRENT_CLAIMS = (
    "license-MIT",
    "MIT ©",
    "licensed under the MIT License",
    "License :: OSI Approved :: MIT License",
)
# BUSL-1.1 is frequently confused with the Boost Software License (BSL-1.0),
# and "BSL-1.1" is not a valid SPDX identifier for either.
INVALID_SPDX_PATTERN = re.compile(r"(?<![\w-])BSL-1\.1(?![\w-])")
CANONICAL_REPOSITORY = "https://github.com/CurateLabs/graphforge-legecy"
# The pre-transition repository identity. It still redirects, but naming it on a
# current surface points contributors and consumers at the MIT-era identity.
SUPERSEDED_REPOSITORY = "github.com/DecisionNerd/graphforge"
# Manifests whose published `repository`/`urls` metadata names the licensed
# work's home. History-of-record documents are intentionally excluded.
REPOSITORY_MANIFESTS = (
    ROOT / "Cargo.toml",
    ROOT / "pyproject.toml",
)
# The contributor onboarding path: license grant, contribution terms, and
# community standards must agree with each other.
CONTRIBUTING_PATH = ROOT / "CONTRIBUTING.md"
CONTRIBUTOR_CONTRACT = (
    ROOT / "CLA.md",
    ROOT / "CODE_OF_CONDUCT.md",
)
CODE_OF_CONDUCT_COPIES = (
    ROOT / "CODE_OF_CONDUCT.md",
    ROOT / "docs" / "community" / "code-of-conduct.md",
)
COVENANT_VERSION_PATTERN = re.compile(r"Contributor Covenant.{0,160}?version (\d+\.\d+)", re.DOTALL)
# Both conduct copies must adopt the same Covenant release. Upgrading is a
# deliberate edit here plus the copies, not a silent drift between them.
REQUIRED_COVENANT_VERSION = "2.1"
# Destinations `CONTRIBUTING.md` must reach so one document carries the whole
# onboarding contract: contribution terms, community standards, license scope,
# and vulnerability reporting.
REQUIRED_CONTRIBUTOR_ROUTES = (
    "CLA.md",
    "CODE_OF_CONDUCT.md",
    "docs/legal/licensing.md",
    "SECURITY.md",
)


class PolicyError(ValueError):
    """Raised when the checked-in license policy is inconsistent."""


def sha256_text(text: str) -> str:
    """Return the SHA-256 digest for UTF-8 text."""
    return hashlib.sha256(text.encode()).hexdigest()


def label(path: Path) -> str:
    """Return a repository-relative label, falling back to the file name."""
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return path.name


def third_anniversary(release_date: date) -> date:
    """Return three calendar years later, mapping leap day to February 28."""
    year = release_date.year + 3
    day = (
        28
        if release_date.month == 2 and release_date.day == 29 and not isleap(year)
        else release_date.day
    )
    return date(year, release_date.month, day)


def load_policy() -> dict[str, Any]:
    """Load the machine-readable policy."""
    return json.loads(POLICY_PATH.read_text(encoding="utf-8"))


def render_license(policy: dict[str, Any]) -> str:
    """Render the operative license without modifying canonical BUSL terms."""
    grant = GRANT_PATH.read_text(encoding="utf-8").strip()
    terms = TERMS_PATH.read_text(encoding="utf-8").strip()
    version = policy["release_version"]
    return (
        "Business Source License 1.1\n\n"
        "Parameters\n\n"
        f"Licensor: {policy['licensor']}\n\n"
        "Licensed Work: GraphForge, including its embeddable engine, first-party\n"
        "bindings, command-line tools, build and release tooling, and first-party\n"
        f"documentation, in version {version} as identified by the Git commit containing\n"
        "this License. Third-Party Materials identified in NOTICE or by their own\n"
        "license are not part of the Licensed Work.\n\n"
        f"Additional Use Grant: {grant}\n\n"
        f"Change Date: {policy['change_date']}\n\n"
        "Change License: GNU Affero General Public License, version 3.0 only\n"
        f"({policy['change_license']})\n\n"
        "For information about commercial licensing, visit:\n"
        f"{policy['commercial_contact']}\n\n"
        f"{terms}\n"
    )


def validate_policy(policy: dict[str, Any]) -> list[str]:
    """Return all policy violations."""
    errors: list[str] = []
    if policy.get("schema_version") != 1:
        errors.append("schema_version must be 1")
    if policy.get("status") != "active":
        errors.append("license policy must be active")
    if policy.get("spdx_expression") != "BUSL-1.1":
        errors.append("SPDX expression must be BUSL-1.1")
    if policy.get("change_license") != "AGPL-3.0-only":
        errors.append("Change License must be AGPL-3.0-only")

    try:
        released = date.fromisoformat(policy["release_date"])
        changed = date.fromisoformat(policy["change_date"])
    except (KeyError, TypeError, ValueError) as exc:
        errors.append(f"release/change date is invalid: {exc}")
    else:
        expected = third_anniversary(released)
        if changed != expected:
            errors.append(f"change_date must be {expected.isoformat()}")

    terms = TERMS_PATH.read_text(encoding="utf-8")
    grant = GRANT_PATH.read_text(encoding="utf-8")
    if policy.get("canonical_terms_sha256") != sha256_text(terms):
        errors.append("canonical BUSL terms digest does not match")
    if policy.get("additional_use_grant_sha256") != sha256_text(grant):
        errors.append("Additional Use Grant digest does not match")

    render_fields = (
        "licensor",
        "release_version",
        "change_date",
        "change_license",
        "commercial_contact",
    )
    missing_render_fields = [
        field
        for field in render_fields
        if not isinstance(policy.get(field), str) or not policy[field]
    ]
    if missing_render_fields:
        errors.append(f"license policy lacks render fields: {', '.join(missing_render_fields)}")
    else:
        rendered = render_license(policy)
        for path in LICENSE_COPIES:
            if not path.exists() or path.read_text(encoding="utf-8") != rendered:
                errors.append(
                    f"{path.relative_to(ROOT)} is missing or differs from generated LICENSE"
                )
    notice = NOTICE_COPIES[0].read_text(encoding="utf-8") if NOTICE_COPIES[0].exists() else ""
    for path in NOTICE_COPIES:
        if not path.exists() or path.read_text(encoding="utf-8") != notice:
            errors.append(f"{path.relative_to(ROOT)} is missing or differs from NOTICE")

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
                errors.append(
                    f"{path.relative_to(ROOT)} is missing or differs from "
                    "legal/THIRD_PARTY_NOTICES.md"
                )
        if "Do not edit this file by hand" not in third_party:
            errors.append("legal/THIRD_PARTY_NOTICES.md lacks generated-file marker")
        if "Business Source License 1.1" in third_party and "## Business Source" in third_party:
            errors.append(
                "legal/THIRD_PARTY_NOTICES.md incorrectly lists first-party BUSL as a "
                "third-party license section"
            )

    for path, snippets in MANIFEST_EXPECTATIONS.items():
        text = path.read_text(encoding="utf-8") if path.exists() else ""
        for snippet in snippets:
            if snippet not in text:
                errors.append(f"{path.relative_to(ROOT)} lacks {snippet}")

    ignored_parts = {".git", ".venv", "node_modules", "target"}
    for path in ROOT.rglob("Cargo.toml"):
        if ignored_parts.intersection(path.parts):
            continue
        text = path.read_text(encoding="utf-8")
        if 'license = "BUSL-1.1"' not in text and "license.workspace = true" not in text:
            errors.append(f"{path.relative_to(ROOT)} lacks BUSL-1.1 license metadata")
        if path.parent in CARGO_PACKAGE_DIRS and "license-file.workspace = true" not in text:
            errors.append(f"{path.relative_to(ROOT)} does not package the workspace LICENSE")
    for path in ROOT.rglob("package.json"):
        if ignored_parts.intersection(path.parts):
            continue
        try:
            package = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            errors.append(f"{path.relative_to(ROOT)} is invalid JSON: {exc}")
            continue
        if package.get("license") != "BUSL-1.1":
            errors.append(f"{path.relative_to(ROOT)} lacks BUSL-1.1 license metadata")
    for path in ROOT.rglob("pyproject.toml"):
        if ignored_parts.intersection(path.parts):
            continue
        if 'license = "BUSL-1.1"' not in path.read_text(encoding="utf-8"):
            errors.append(f"{path.relative_to(ROOT)} lacks BUSL-1.1 license metadata")
    for manifest_name in ("go.mod", "setup.py"):
        for path in ROOT.rglob(manifest_name):
            if not ignored_parts.intersection(path.parts):
                errors.append(
                    f"{path.relative_to(ROOT)} is a new manifest type; add it to license policy"
                )

    for path in CURRENT_DOCS:
        if not path.exists():
            errors.append(f"{label(path)} is missing")
            continue
        text = path.read_text(encoding="utf-8")
        for claim in FORBIDDEN_CURRENT_CLAIMS:
            if claim.lower() in text.lower():
                errors.append(f"{label(path)} retains current MIT claim: {claim}")
        if INVALID_SPDX_PATTERN.search(text):
            errors.append(
                f"{label(path)} uses invalid SPDX identifier BSL-1.1; the licensed work is BUSL-1.1"
            )
        if SUPERSEDED_REPOSITORY in text:
            errors.append(
                f"{label(path)} names the superseded repository "
                f"{SUPERSEDED_REPOSITORY}; use {CANONICAL_REPOSITORY}"
            )

    for path in REPOSITORY_MANIFESTS:
        text = path.read_text(encoding="utf-8") if path.exists() else ""
        if SUPERSEDED_REPOSITORY in text:
            errors.append(
                f"{label(path)} publishes the superseded repository "
                f"{SUPERSEDED_REPOSITORY}; use {CANONICAL_REPOSITORY}"
            )
        elif CANONICAL_REPOSITORY not in text:
            errors.append(f"{label(path)} does not publish {CANONICAL_REPOSITORY}")

    errors.extend(validate_contributor_contract(policy))
    return errors


def validate_contributor_contract(policy: dict[str, Any]) -> list[str]:
    """Return violations of the license / CLA / conduct onboarding path."""
    errors: list[str] = []
    for path in CONTRIBUTOR_CONTRACT:
        if not path.exists() or not path.read_text(encoding="utf-8").strip():
            errors.append(f"{label(path)} is missing or empty")

    contributing_text = (
        CONTRIBUTING_PATH.read_text(encoding="utf-8") if CONTRIBUTING_PATH.exists() else ""
    )
    for required in REQUIRED_CONTRIBUTOR_ROUTES:
        if required not in contributing_text:
            errors.append(f"CONTRIBUTING.md does not route contributors to {required}")

    contact = policy.get("commercial_contact")
    versions: dict[Path, str | None] = {}
    for path in CODE_OF_CONDUCT_COPIES:
        if not path.exists():
            errors.append(f"{label(path)} is missing")
            continue
        text = path.read_text(encoding="utf-8")
        if isinstance(contact, str) and contact and contact not in text:
            errors.append(
                f"{label(path)} does not name the recorded Curate Labs contact for conduct reports"
            )
        match = COVENANT_VERSION_PATTERN.search(text)
        versions[path] = match.group(1) if match else None
        if match is None:
            errors.append(f"{label(path)} does not state a Contributor Covenant version")
        elif match.group(1) != REQUIRED_COVENANT_VERSION:
            errors.append(
                f"{label(path)} declares Contributor Covenant version {match.group(1)}; "
                f"GraphForge adopts {REQUIRED_COVENANT_VERSION}"
            )

    declared = {version for version in versions.values() if version is not None}
    if len(declared) > 1:
        errors.append(
            "Code of Conduct copies declare different Contributor Covenant versions: "
            + ", ".join(sorted(declared))
        )
    return errors


def generate(release_version: str, release_date: str) -> None:
    """Update policy parameters and all distributed license copies."""
    if not release_version or any(char.isspace() for char in release_version):
        raise PolicyError("release version must be a non-empty token")
    try:
        released = date.fromisoformat(release_date)
    except ValueError as exc:
        raise PolicyError(f"invalid release date: {release_date}") from exc

    policy = load_policy()
    policy["release_version"] = release_version
    policy["release_date"] = released.isoformat()
    policy["change_date"] = third_anniversary(released).isoformat()
    policy["canonical_terms_sha256"] = sha256_text(TERMS_PATH.read_text(encoding="utf-8"))
    policy["additional_use_grant_sha256"] = sha256_text(GRANT_PATH.read_text(encoding="utf-8"))
    POLICY_PATH.write_text(json.dumps(policy, indent=2) + "\n", encoding="utf-8")

    rendered = render_license(policy)
    for path in LICENSE_COPIES:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(rendered, encoding="utf-8")
    notice = NOTICE_COPIES[0].read_text(encoding="utf-8")
    for path in NOTICE_COPIES[1:]:
        path.write_text(notice, encoding="utf-8")
    if THIRD_PARTY_NOTICE_COPIES[0].exists():
        third_party = THIRD_PARTY_NOTICE_COPIES[0].read_text(encoding="utf-8")
        for path in THIRD_PARTY_NOTICE_COPIES[1:]:
            path.write_text(third_party, encoding="utf-8")


def build_report(policy: dict[str, Any], errors: list[str]) -> dict[str, Any]:
    """Build a sanitized, SHA-bindable compliance report."""
    return {
        "schema_version": 1,
        "status": "pass" if not errors else "fail",
        "git_sha": _git_sha(),
        "release_version": policy.get("release_version"),
        "release_date": policy.get("release_date"),
        "change_date": policy.get("change_date"),
        "spdx_expression": policy.get("spdx_expression"),
        "change_license": policy.get("change_license"),
        "manifest_count": len(MANIFEST_EXPECTATIONS),
        "canonical_repository": CANONICAL_REPOSITORY,
        "current_docs": [str(path.relative_to(ROOT)) for path in CURRENT_DOCS],
        "code_of_conduct_copies": [str(path.relative_to(ROOT)) for path in CODE_OF_CONDUCT_COPIES],
        "license_copies": [str(path.relative_to(ROOT)) for path in LICENSE_COPIES],
        "notice_copies": [str(path.relative_to(ROOT)) for path in NOTICE_COPIES],
        "third_party_notice_copies": [
            str(path.relative_to(ROOT)) for path in THIRD_PARTY_NOTICE_COPIES
        ],
        "errors": errors,
    }


def _git_sha() -> str:
    result = subprocess.run(
        ["git", "-C", str(ROOT), "rev-parse", "--verify", "HEAD"],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        return "unknown"
    value = result.stdout.strip()
    return value if value else "unknown"


def main(argv: list[str] | None = None) -> int:
    """Run the generator or checker."""
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    generate_parser = subparsers.add_parser("generate")
    generate_parser.add_argument("--release-version", required=True)
    generate_parser.add_argument("--release-date", required=True)
    check_parser = subparsers.add_parser("check")
    check_parser.add_argument("--report", type=Path)
    args = parser.parse_args(argv)

    if args.command == "generate":
        try:
            generate(args.release_version, args.release_date)
        except PolicyError as exc:
            parser.error(str(exc))
        return 0

    policy = load_policy()
    errors = validate_policy(policy)
    if args.report:
        args.report.write_text(
            json.dumps(build_report(policy, errors), indent=2) + "\n",
            encoding="utf-8",
        )
    if errors:
        for error in errors:
            print(f"license-policy: {error}", file=sys.stderr)
        return 1
    print("license-policy: compliant")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
