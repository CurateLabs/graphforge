#!/usr/bin/env python3
"""Deterministic contract for the planner-driven publication workflow."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = Path(__file__).with_name("release-publish-preflight.py")
WORKFLOW = ROOT / ".github" / "workflows" / "publish.yaml"
CREDENTIAL_WORKFLOW = ROOT / ".github" / "workflows" / "release-credential-preflight.yml"
WRITE_EVIDENCE = ROOT / "scripts" / "ci" / "download-release-write-evidence.sh"


def load_module():
    spec = importlib.util.spec_from_file_location("release_publish_preflight", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mod = load_module()
sha = "a" * 40
versions = dict.fromkeys(("cargo", "python", "node", "cli", "skills"), "0.5.1")
assert (
    mod.validate(
        tag="v0.5.1",
        expected_sha=sha,
        actual_sha=sha,
        versions=versions,
    )
    == []
)
assert mod.validate(
    tag="v0.5.1",
    expected_sha=sha,
    actual_sha=sha,
    versions={**versions, "skills": "0.5.2"},
)

workflow = WORKFLOW.read_text(encoding="utf-8")
assert "default: v0.5.1" in workflow
assert 'test "$release_version" != 0.5.0' in workflow
assert "candidate/v0.5.0-artifacts.json" not in workflow
assert "v0.5.0-npm-amendment.json" not in workflow
assert "scripts/set_release_version.py --check" in workflow
assert "waive_unreleased" not in workflow
assert "allow-unreleased-entries" not in workflow
assert "CHANGELOG" not in workflow
for group in ("manifest", "python", "npm", "crates", "evidence"):
    assert f"M1-Release-Candidate-{group}-" in workflow

preflight_source = SCRIPT.read_text(encoding="utf-8")
assert "CHANGELOG" not in preflight_source
assert "Unreleased" not in preflight_source
assert "docs/reference/changelog.md" not in preflight_source
assert "allow_unreleased_entries" not in preflight_source

preflight = workflow.split("  candidate-preflight:\n", 1)[1].split("\n  publish-pypi:", 1)[0]
assert "release-publish-preflight.py" in preflight
assert "release_registry.py observe-all" in preflight
assert "release_registry.py plan" in preflight
assert "--attempts-dir write-evidence/attempts" in preflight
assert "--receipts-dir write-evidence/receipts" in preflight
assert "offline-rehearsal.json" in preflight
assert "secrets." not in preflight

pypi = workflow.split("  publish-pypi:\n", 1)[1].split("\n  npm-native:", 1)[0]
native = workflow.split("  npm-native:\n", 1)[1].split("\n  npm-main:", 1)[0]
main = workflow.split("  npm-main:\n", 1)[1].split("\n  npm-cli:", 1)[0]
cli = workflow.split("  npm-cli:\n", 1)[1].split("\n  npm-skills:", 1)[0]
skills = workflow.split("  npm-skills:\n", 1)[1].split("\n  publish-crates:", 1)[0]
crates = workflow.split("  publish-crates:\n", 1)[1].split("\n  reconcile:", 1)[0]
summary = workflow.split("  reconcile:\n", 1)[1]

assert "needs: candidate-preflight" in pypi
assert "id-token: write" in pypi
assert "uv publish candidate/release-artifacts/python/*" in pypi
assert "secrets.NPM_TOKEN" not in pypi
assert "secrets.CARGO_REGISTRY_TOKEN" not in pypi

assert "fail-fast: false" in native
assert native.count("- graphforge-") == 5
assert "needs: candidate-preflight" in native
assert '--package "@curatelabs/${{ matrix.package }}"' in native
assert "secrets.NPM_TOKEN" in native
assert "secrets.CARGO_REGISTRY_TOKEN" not in native

assert "needs: [candidate-preflight, npm-native]" in main
assert "Require verified native fan-in and authorize main" in main
assert "--node npm:@curatelabs/graphforge" in main
assert "needs: [candidate-preflight, npm-main]" in cli
assert "--node npm:@curatelabs/graphforge-cli" in cli
assert "needs: [candidate-preflight, npm-cli]" in skills
assert "--node npm:@curatelabs/graphforge-agent-skills" in skills

assert "needs: candidate-preflight" in crates
assert "scripts/ci/crate-publish-plan.py list" in crates
assert "scripts/publish_crates.py" in crates
assert '--crate "$crate"' in crates
assert "secrets.CARGO_REGISTRY_TOKEN" in crates
assert "secrets.NPM_TOKEN" not in crates

for lane in (pypi, native, main, cli, skills, crates):
    assert "release_action.py" in lane
    assert "release_registry.py" in lane
    assert "release_action.py attempt" in lane
    assert "gh release upload" in lane
    assert "--attempts-dir write-evidence/attempts" in lane
    assert "--receipts-dir write-evidence/receipts" in lane

assert "if: always()" in summary
for job in (
    "candidate-preflight",
    "publish-pypi",
    "npm-native",
    "npm-main",
    "npm-cli",
    "npm-skills",
    "publish-crates",
):
    assert f"- {job}" in summary
assert "release_rehearsal.py reconcile" in summary
assert "M1-Release-Reconciliation-${{ github.run_id }}" in summary
assert ".complete == true and (.nodes | length) == 24" in summary

assert "sleep" not in workflow
assert "continue-on-error" not in workflow
assert "|| true" not in workflow
assert WRITE_EVIDENCE.is_file()
write_evidence = WRITE_EVIDENCE.read_text(encoding="utf-8")
assert "gh release view" in write_evidence
assert "gh release download" in write_evidence
assert "sleep" not in write_evidence

credential_workflow = CREDENTIAL_WORKFLOW.read_text(encoding="utf-8")
assert "npm whoami" in credential_workflow
assert "secrets.NPM_TOKEN" in credential_workflow
assert "secrets.CARGO_REGISTRY_TOKEN" in credential_workflow
for forbidden in ("npm publish", "uv publish", "cargo publish", "release:\n"):
    assert forbidden not in credential_workflow

print("release publish preflight tests passed")
