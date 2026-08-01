#!/usr/bin/env python3
"""Deterministic tests for the release publication preflight and workflow gate."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = Path(__file__).with_name("release-publish-preflight.py")
WORKFLOW = ROOT / ".github" / "workflows" / "publish.yaml"
CREDENTIAL_WORKFLOW = ROOT / ".github" / "workflows" / "release-credential-preflight.yml"


def load_module():
    spec = importlib.util.spec_from_file_location("release_publish_preflight", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


mod = load_module()
sha = "a" * 40
versions = {
    "cargo": "0.5.0",
    "python": "0.5.0",
    "node": "0.5.0",
    "cli": "0.5.0",
    "skills": "0.5.0",
}
changelog = """# Changelog

## [Unreleased]

_Nothing yet._

## [0.5.0] - 2026-07-31

- Release GraphForge.

[Unreleased]: https://github.com/CurateLabs/graphforge/compare/v0.5.0...HEAD
[0.5.0]: https://github.com/CurateLabs/graphforge/releases/tag/v0.5.0
"""

assert (
    mod.validate(
        tag="v0.5.0",
        expected_sha=sha,
        actual_sha=sha,
        versions=versions,
        changelog=changelog,
        docs_changelog=changelog,
    )
    == []
)
assert mod.validate_metadata() == []
assert mod.load_version_module().check_aligned() == []

mutations = [
    {"tag": "0.5.0"},
    {"actual_sha": "b" * 40},
    {"versions": {**versions, "python": "0.5.0.dev0"}},
    {"changelog": changelog.replace("## [0.5.0] - 2026-07-31", "## [0.5.0]")},
    {"changelog": changelog.replace("_Nothing yet._", "- Stale release entry")},
    {
        "changelog": changelog.replace(
            "CurateLabs/graphforge/compare",
            "CurateLabs/graphforge-legecy/compare",
        )
    },
    {"docs_changelog": changelog.replace("Release GraphForge.", "Stale public changelog.")},
]
for mutation in mutations:
    values = {
        "tag": "v0.5.0",
        "expected_sha": sha,
        "actual_sha": sha,
        "versions": versions,
        "changelog": changelog,
        "docs_changelog": changelog,
        **mutation,
    }
    assert mod.validate(**values), mutation

stale_changelog = changelog.replace("_Nothing yet._", "- Stale release entry")
assert (
    mod.validate(
        tag="v0.5.0",
        expected_sha=sha,
        actual_sha=sha,
        versions=versions,
        changelog=stale_changelog,
        docs_changelog=stale_changelog,
        allow_unreleased_entries=True,
    )
    == []
)

workflow = WORKFLOW.read_text(encoding="utf-8")
preflight = workflow.split("  candidate-preflight:\n", 1)[1].split("\n  publish-pypi:", 1)[0]
assert "release-publish-preflight.py" in preflight
assert "github.event.release.tag_name" in preflight
assert "github.sha" in preflight
assert "refs/remotes/origin/main" in preflight
assert "workflow_dispatch:" in workflow
assert "waive_unreleased_entries:" in workflow
assert "RECOVERY_REASON" in preflight
assert "GH_TOKEN: ${{ github.token }}" in preflight
assert "--allow-unreleased-entries" in preflight
assert "git show" in preflight
assert "refs/remotes/origin/main:scripts/ci/release-publish-preflight.py" in preflight
assert "npm whoami" in preflight
assert "secrets.NPM_TOKEN" in preflight
assert "M1-Release-Candidate-$RELEASE_SHA" in preflight
assert "scripts/ci/release-candidate.py validate" in preflight
assert "gh release upload" in preflight
assert preflight.index("release-publish-preflight.py") < preflight.index("npm whoami")
assert preflight.index("npm whoami") < preflight.index("gh release upload")

pypi_job = workflow.split("  publish-pypi:\n", 1)[1].split("\n  publish-npm:", 1)[0]
assert "needs: candidate-preflight" in pypi_job
assert "--check-url https://pypi.org/simple/" in pypi_job
assert "candidate/release-artifacts/python/*" in pypi_job

npm_job = workflow.split("  publish-npm:\n", 1)[1].split("\n  publish-crates:", 1)[0]
assert "needs: [candidate-preflight, publish-pypi]" in npm_job
assert "scripts/publish_npm_artifacts.py" in npm_job
assert "Load reviewed npm recovery publisher" in npm_job
assert "refs/remotes/origin/main:scripts/publish_npm_artifacts.py" in npm_job
assert "--group native" in npm_job
assert "--group cli" in npm_job
assert "--group skills" in npm_job
assert "verify-node-cli-release-package.mjs" in npm_job

crates_job = workflow.split("  publish-crates:\n", 1)[1]
assert "needs: [candidate-preflight, publish-npm]" in crates_job
assert "scripts/publish_crates.py" in crates_job
assert "--release-record candidate/v0.5.0-artifacts.json" in crates_job
assert "secrets.CARGO_REGISTRY_TOKEN" in crates_job
assert "cargo publish" not in crates_job
for retired_job in ("build-wheels", "build-sdist", "build-node", "publish-node-cli"):
    assert f"  {retired_job}:" not in workflow

credential_workflow = CREDENTIAL_WORKFLOW.read_text(encoding="utf-8")
assert "workflow_dispatch:" in credential_workflow
assert "commit_sha:" in credential_workflow
assert "npm whoami" in credential_workflow
assert "secrets.NPM_TOKEN" in credential_workflow
assert "secrets.CARGO_REGISTRY_TOKEN" in credential_workflow
for forbidden in ("npm publish", "uv publish", "cargo publish", "release:\n"):
    assert forbidden not in credential_workflow

for job in (preflight, pypi_job, npm_job, crates_job, credential_workflow):
    assert "continue-on-error" not in job
    assert "|| true" not in job

print("release publish preflight tests passed")
