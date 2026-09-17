#!/usr/bin/env python3
"""Deterministic contract for the planner-driven publication workflow."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import re

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
versions = dict.fromkeys(("cargo", "python", "node", "cli", "skills"), "0.5.2")
assert (
    mod.validate(
        tag="v0.5.2",
        expected_sha=sha,
        actual_sha=sha,
        versions=versions,
    )
    == []
)
assert mod.validate(
    tag="v0.5.2",
    expected_sha=sha,
    actual_sha=sha,
    versions={**versions, "skills": "0.5.3"},
)

# The npm dist-tag policy is derived, not configured, and refuses the unclassifiable.
publisher_spec = importlib.util.spec_from_file_location(
    "publish_npm_artifacts", ROOT / "scripts" / "publish_npm_artifacts.py"
)
assert publisher_spec is not None and publisher_spec.loader is not None
publisher = importlib.util.module_from_spec(publisher_spec)
publisher_spec.loader.exec_module(publisher)
assert publisher.dist_tag_for("0.5.2") == "latest"
assert publisher.dist_tag_for("0.6.0") == "latest"
assert publisher.dist_tag_for("0.6.0-rc.1") == "next"
assert publisher.dist_tag_for("0.6.0-dev") == "next"
try:
    publisher.dist_tag_for("0.6.0rc1")
except publisher.DistTagError:
    pass
else:
    raise AssertionError("an unclassifiable version must not reach npm")

workflow = WORKFLOW.read_text(encoding="utf-8")
assert "default: v0.5.2" not in workflow
assert "recovery_overlay_sha:" in workflow
assert "cancel-in-progress: false" in workflow
group = (
    "group: publish-${{ github.event_name == 'release' && "
    "github.event.release.tag_name || inputs.release_tag }}"
)
assert group in workflow
assert 'test "$release_version" != 0.5.0' in workflow
assert "candidate/v0.5.0-artifacts.json" not in workflow
assert "v0.5.0-npm-amendment.json" not in workflow
assert "scripts/set_release_version.py --check" in workflow
assert "scripts/publish_npm_artifacts.py \\\n            --print-dist-tag" in workflow
assert "npm_dist_tag: ${{ steps.source.outputs.npm_dist_tag }}" in workflow
assert "waive_unreleased" not in workflow
assert "allow-unreleased-entries" not in workflow
assert "CHANGELOG" not in workflow
for group in ("manifest", "python", "npm", "crates", "evidence"):
    assert f"Release-Candidate-{group}-" in workflow

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
# The dist-tag is resolved once, before any lane can write to a registry.
assert "--print-dist-tag" in preflight
assert 'printf \'npm_dist_tag=%s\\n\' "$npm_dist_tag" >> "$GITHUB_OUTPUT"' in preflight

pypi = workflow.split("  publish-pypi:\n", 1)[1].split("\n  npm-native:", 1)[0]
native = workflow.split("  npm-native:\n", 1)[1].split("\n  npm-main:", 1)[0]
main = workflow.split("  npm-main:\n", 1)[1].split("\n  npm-cli:", 1)[0]
cli = workflow.split("  npm-cli:\n", 1)[1].split("\n  npm-skills:", 1)[0]
skills = workflow.split("  npm-skills:\n", 1)[1].split("\n  publish-crates:", 1)[0]
crates = workflow.split("  publish-crates:\n", 1)[1].split("\n  reconcile:", 1)[0]
summary = workflow.split("  reconcile:\n", 1)[1]

assert "needs: candidate-preflight" in pypi
assert "environment: release" in pypi
assert "id-token: write" in pypi
assert "uv publish candidate/release-artifacts/python/*" in pypi
assert "secrets.NPM_TOKEN" not in pypi
assert "secrets.CARGO_REGISTRY_TOKEN" not in pypi

assert "fail-fast: false" in native
assert native.count("- graphforge-") == 5
assert "needs: candidate-preflight" in native
assert "environment: release" in native
assert '--package "@curatelabs/${{ matrix.package }}"' in native
assert "id-token: write" in native
assert "secrets.NPM_TOKEN" not in native
assert "NODE_AUTH_TOKEN" not in native
assert "secrets.CARGO_REGISTRY_TOKEN" not in native

assert "needs: [candidate-preflight, npm-native]" in main
assert "environment: release" in main
assert "Require verified native fan-in and authorize main" in main
assert "--node npm:@curatelabs/graphforge" in main
assert "id-token: write" in main
assert "secrets.NPM_TOKEN" not in main
assert "NODE_AUTH_TOKEN" not in main
assert "needs: [candidate-preflight, npm-main]" in cli
assert "environment: release" in cli
assert "--node npm:@curatelabs/graphforge-cli" in cli
assert "id-token: write" in cli
assert "secrets.NPM_TOKEN" not in cli
assert "NODE_AUTH_TOKEN" not in cli
assert "needs: [candidate-preflight, npm-cli]" in skills
assert "environment: release" in skills
assert "--node npm:@curatelabs/graphforge-agent-skills" in skills
assert "id-token: write" in skills
assert "secrets.NPM_TOKEN" not in skills
assert "NODE_AUTH_TOKEN" not in skills

assert "needs: candidate-preflight" in crates
assert "environment: release" in crates
assert "timeout-minutes: 180" in crates
assert "Overlay reviewed recovery publish-path scripts" in crates
assert "RECOVERY_OVERLAY_SHA" in crates
assert "refs/remotes/origin/main:scripts/publish_crates.py" not in crates
assert "RECOVERY_OVERLAY_SHA" in preflight
assert "scripts/ci/crate-publish-plan.py list" in crates
assert "scripts/publish_crates.py" in crates
assert '--crate "$crate"' in crates
assert "id-token: write" in crates
assert 'CRATES_IO_TRUSTED_PUBLISHING: "true"' in crates
assert "rust-lang/crates-io-auth-action" not in crates
assert "secrets.CARGO_REGISTRY_TOKEN" not in crates
assert "secrets.NPM_TOKEN" not in crates
assert "Refresh this node and its crates dependencies before authorize" in crates
assert "scripts/ci/crate-authorize-refresh-nodes.py" in crates
assert (
    'release_registry.py observe --manifest "candidate/$MANIFEST_NAME" '
    '--node "$refresh_node" --live' in crates
)
observe_marker = (
    'release_registry.py observe --manifest "candidate/$MANIFEST_NAME" '
    '--node "$refresh_node" --live'
)
authorize_marker = (
    'release_action.py authorize --manifest "candidate/$MANIFEST_NAME" '
    "--observations observations.json"
)
assert crates.index(observe_marker) < crates.index(authorize_marker)

# Each npm lane asserts the preflight dist-tag; the publisher refuses a mismatch.
for lane in (native, main, cli, skills):
    assert "NPM_DIST_TAG: ${{ needs.candidate-preflight.outputs.npm_dist_tag }}" in lane
    assert '--dist-tag "$NPM_DIST_TAG"' in lane

for lane in (pypi, native, main, cli, skills, crates):
    assert "release_action.py" in lane
    assert "release_registry.py" in lane
    assert "release_action.py attempt" in lane
    assert 'gh release upload "$RELEASE_TAG" "$attempt" --clobber' in lane
    assert 'gh release upload "$RELEASE_TAG" "$receipt" --clobber' in lane
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
assert "Release-Reconciliation-${{ github.run_id }}" in summary
assert ".complete == true and (.nodes | length) == 25" in summary

# Recovery overlays one reviewed list for the whole publish path, never a
# per-lane subset: scripts/ci/test-release-recovery-overlay.py owns the list, and
# the only file any lane may fetch by hand is the runner that installs it.
overlaid_by_hand = re.findall(r'git show "\$RECOVERY_OVERLAY_SHA:([^"]+)"', workflow)
assert set(overlaid_by_hand) == {"scripts/ci/release-recovery-overlay.sh"}, overlaid_by_hand
for lane in (preflight, pypi, native, main, cli, skills, crates, summary):
    assert 'bash scripts/ci/release-recovery-overlay.sh "$RECOVERY_OVERLAY_SHA"' in lane
assert len(overlaid_by_hand) == 8
# The npm publisher is recoverable in every lane that can write to npm.
for lane in (native, main, cli, skills):
    assert "scripts/publish_npm_artifacts.py" in lane

assert "sleep" not in workflow
assert "continue-on-error" not in workflow
assert "|| true" not in workflow
assert WRITE_EVIDENCE.is_file()
write_evidence = WRITE_EVIDENCE.read_text(encoding="utf-8")
assert "gh release view" in write_evidence
assert "gh release download" in write_evidence
assert "sleep" not in write_evidence

credential_workflow = CREDENTIAL_WORKFLOW.read_text(encoding="utf-8")
assert "id-token: write" in credential_workflow
assert "${{ secrets.NPM_TOKEN }}" not in credential_workflow
assert "NODE_AUTH_TOKEN:" not in credential_workflow
assert "npm whoami" not in credential_workflow
assert "trusted-publishing" in credential_workflow
assert "secrets.CARGO_REGISTRY_TOKEN" not in credential_workflow
assert "crates-io-auth-action" not in credential_workflow
for forbidden in ("npm publish", "uv publish", "cargo publish", "release:\n"):
    assert forbidden not in credential_workflow

print("release publish preflight tests passed")
