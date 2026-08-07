#!/usr/bin/env python3
"""Deterministic contract for publish-track orchestration and safety gates."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "publish-track.yml"
PUBLISH = ROOT / ".github" / "workflows" / "publish.yaml"


def section(text: str, start: str, end: str) -> str:
    _, found, remainder = text.partition(start)
    assert found, f"missing workflow marker: {start}"
    result, found, _ = remainder.partition(end)
    assert found, f"missing workflow marker: {end}"
    return result


workflow = WORKFLOW.read_text(encoding="utf-8")
publish = PUBLISH.read_text(encoding="utf-8")

assert "name: Publish Track" in workflow
assert 'cron: "17 */6 * * *"' in workflow
assert "workflow_dispatch:" in workflow
assert "create_release:" in workflow
assert "confirm_registry_publish:" in workflow
assert "release_tag:" in workflow
assert "binding-release-candidate.yml" in workflow
assert '-f "commit_sha=$RELEASE_SHA"' in workflow
assert "actions: write" in workflow

resolve = section(workflow, "  resolve_source:\n", "  locate_candidate:\n")
assert "refs/heads/main:refs/remotes/origin/main" in resolve
assert 'test "$REQUESTED_SHA" = "$main_sha"' in resolve
assert "scripts/set_release_version.py --check" in resolve
assert "publish-track requires a non-development version" in resolve

locate = section(workflow, "  locate_candidate:\n", "  validate_candidate:\n")
assert "status=success&head_sha=$RELEASE_SHA" in locate
assert 'artifact_name="M1-Release-Candidate-$group-$RELEASE_SHA"' in locate
assert ".expired == false" in locate
assert 'if test "$count" != 1; then' in locate
assert "candidate_state=incomplete" in locate

validate = section(workflow, "  validate_candidate:\n", "  dispatch_binding_rc:\n")
assert "actions/download-artifact@" in validate
for group in ("manifest", "python", "npm", "crates", "evidence"):
    assert (
        f"M1-Release-Candidate-{group}-${{{{ needs.resolve_source.outputs.release_sha }}}}"
        in validate
    )
assert "scripts/ci/release-candidate.py validate" in validate
assert '--expected-sha "$RELEASE_SHA"' in validate
assert '--version "$RELEASE_VERSION"' in validate

dispatch = section(workflow, "  dispatch_binding_rc:\n", "  create_release:\n")
assert "always()" in dispatch
assert "needs.locate_candidate.result == 'success'" in dispatch
assert "needs.validate_candidate.result != 'success'" in dispatch
assert "inputs.create_release" in dispatch
assert "gh workflow run binding-release-candidate.yml" in dispatch
assert '--repo "$GITHUB_REPOSITORY"' in dispatch
assert "--ref main" in dispatch

release = workflow.split("  create_release:\n", 1)[1]
assert "inputs.create_release" in release
assert "inputs.confirm_registry_publish" in release
assert "needs.validate_candidate.result == 'success'" in release
assert 'test "$RELEASE_TAG" = "v$RELEASE_VERSION"' in release
assert 'test "$(git rev-parse "$RELEASE_TAG^{}")" = "$RELEASE_SHA"' in release
assert 'gh release view "$RELEASE_TAG"' in release
assert 'gh release create "$RELEASE_TAG"' in release
assert '--target "$RELEASE_SHA"' in release
assert "publish.yaml" in release

for forbidden in (
    "m1-release-certification",
    "checkpoint-recovery",
    "m20-contract",
    "m21-contract",
    "uv publish",
    "npm publish",
    "cargo publish",
    "sleep",
    "retry",
    "continue-on-error",
    "|| true",
):
    assert forbidden not in workflow.lower(), forbidden

# publish.yaml remains the only registry writer and revalidates retained bytes.
assert "uv publish candidate/release-artifacts/python/*" in publish
assert "scripts/ci/release-candidate.py validate" in publish
assert "PyO3/maturin-action" not in publish
assert "napi build" not in publish

print("publish-track workflow tests passed")
