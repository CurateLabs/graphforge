"""Tests for packaged LICENSE/NOTICE verification."""

import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "verify_package_licenses.py"
SPEC = importlib.util.spec_from_file_location("verify_package_licenses", SCRIPT)
assert SPEC and SPEC.loader
verify_package_licenses = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(verify_package_licenses)

LICENSE_POLICY = Path(__file__).resolve().parents[2] / "scripts" / "license_policy.py"
LP_SPEC = importlib.util.spec_from_file_location("license_policy", LICENSE_POLICY)
assert LP_SPEC and LP_SPEC.loader
license_policy = importlib.util.module_from_spec(LP_SPEC)
LP_SPEC.loader.exec_module(license_policy)


def test_python_package_declares_busl_license_files() -> None:
    assert verify_package_licenses.verify_python_package() == []


def test_agent_skills_manifest_expects_license_and_notice() -> None:
    path = license_policy.ROOT / "packages" / "agent-skills" / "package.json"
    assert path in license_policy.MANIFEST_EXPECTATIONS
    snippets = license_policy.MANIFEST_EXPECTATIONS[path]
    assert '"LICENSE"' in snippets
    assert '"NOTICE"' in snippets
    assert license_policy.validate_policy(license_policy.load_policy()) == []


def test_npm_packages_pack_license_and_notice() -> None:
    for package_dir in verify_package_licenses.NPM_PACKAGES:
        assert verify_package_licenses.verify_npm_package(package_dir) == []


def test_cargo_core_package_includes_license_and_notice() -> None:
    # One leaf crate keeps CI light; full matrix is `make package-license-verify`.
    assert verify_package_licenses.verify_cargo_crate("gf-core") == []
