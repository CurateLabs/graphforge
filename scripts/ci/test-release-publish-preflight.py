#!/usr/bin/env python3
"""Deterministic tests for the release publication preflight and workflow gate."""

from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = Path(__file__).with_name("release-publish-preflight.py")
WORKFLOW = ROOT / ".github" / "workflows" / "publish.yaml"


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

workflow = WORKFLOW.read_text(encoding="utf-8")
license_job = workflow.split("  license-compliance:\n", 1)[1].split("\n  build-wheels:", 1)[0]
assert "release-publish-preflight.py" in license_job
assert "github.event.release.tag_name" in license_job
assert "github.sha" in license_job
assert "refs/remotes/origin/main" in license_job
assert "npm whoami" in license_job
assert "secrets.NPM_TOKEN" in license_job
assert license_job.index("release-publish-preflight.py") < license_job.index("npm whoami")
for build_job, next_job in (
    ("build-wheels", "build-sdist"),
    ("build-sdist", "publish"),
    ("build-node", "publish-node"),
):
    section = workflow.split(f"  {build_job}:\n", 1)[1].split(f"\n  {next_job}:\n", 1)[0]
    assert "needs: license-compliance" in section, build_job

skills_job = workflow.split("  publish-agent-skills:\n", 1)[1].split("\n  # ---- Rust crates", 1)[0]
assert "needs: publish-node-cli" in skills_job

crates_job = workflow.split("  publish-crates:\n", 1)[1]
assert "needs: publish-agent-skills" in crates_job
assert "scripts/publish_crates.py" in crates_job
assert "secrets.CARGO_REGISTRY_TOKEN" in crates_job
assert "cargo publish" not in crates_job

print("release publish preflight tests passed")
