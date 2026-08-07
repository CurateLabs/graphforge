"""Tests for Apache-2.0 repository licensing policy."""

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "license_check.py"
SPEC = importlib.util.spec_from_file_location("license_check", SCRIPT)
assert SPEC and SPEC.loader
license_check = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(license_check)


def test_current_tree_is_apache_compliant() -> None:
    assert license_check.validate_repository() == []


def test_license_and_notice_copies_match() -> None:
    license_text = license_check.LICENSE_COPIES[0].read_text(encoding="utf-8")
    assert license_check.sha256_text(license_text) == license_check.CANONICAL_LICENSE_SHA256
    assert all(
        path.read_text(encoding="utf-8") == license_text for path in license_check.LICENSE_COPIES
    )
    notice_text = license_check.NOTICE_COPIES[0].read_text(encoding="utf-8")
    assert all(
        path.read_text(encoding="utf-8") == notice_text for path in license_check.NOTICE_COPIES
    )


def test_manifest_expectations_cover_publishable_packages() -> None:
    for path in (
        license_check.ROOT / "packages" / "agent-skills" / "package.json",
        license_check.ROOT / "crates" / "graphforge-bindings-node" / "package.json",
        license_check.ROOT / "crates" / "graphforge-bindings-py" / "pyproject.toml",
    ):
        assert path in license_check.MANIFEST_EXPECTATIONS


def test_forbidden_claim_is_rejected(tmp_path: Path) -> None:
    stale = tmp_path / "stale.md"
    stale.write_text(
        "This package uses " + license_check.FORBIDDEN_TERMS[0] + ".\n", encoding="utf-8"
    )
    errors = license_check.validate_forbidden_claims((stale,))
    assert len(errors) == 1
    assert errors[0].startswith("stale.md retains forbidden licensing claim:")


def test_report_records_apache_spdx() -> None:
    report = license_check.build_report([])
    assert report["status"] == "pass"
    assert report["spdx_expression"] == "Apache-2.0"
    assert report["canonical_repository"] == license_check.CANONICAL_REPOSITORY


def test_retired_repository_identity_is_rejected_in_manifests(tmp_path: Path) -> None:
    stale = tmp_path / "Cargo.toml"
    stale.write_text(
        'repository = "https://github.com/DecisionNerd/graphforge"\n',
        encoding="utf-8",
    )
    # Exercise the marker helper used by validate_canonical_repository.
    assert (
        license_check._text_has_retired_repository(stale.read_text(encoding="utf-8"))
        == "DecisionNerd/graphforge"
    )
    assert (
        license_check._text_has_retired_repository(
            'repository = "https://github.com/CurateLabs/graphforge"\n'
        )
        is None
    )
