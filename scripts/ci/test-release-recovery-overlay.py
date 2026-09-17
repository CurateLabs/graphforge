#!/usr/bin/env python3
"""Fail-closed contract for the publish.yaml recovery overlay set.

A recovery dispatch re-publishes the immutable tag's certified bytes and may
replace the scripts that decide whether and how those bytes reach a registry.
Before this contract existed the overlay was assembled ad hoc per job: the
crates lane overlaid four files, the preflight step overlaid one, and every npm
lane overlaid none, so ``scripts/publish_npm_artifacts.py`` -- the only npm
registry writer -- could not be repaired without cutting a new tag.

The intended set is enumerated here, so a future registry writer joins the
publish path only with a deliberate decision about its recoverability. The
checks below prove three things that an enumeration alone cannot:

1. the overlay list is the transitive closure of what the lanes execute, so an
   overlaid publisher never imports a stale module from the tag checkout;
2. every job that executes a publish-path script installs the reviewed overlay
   runner and invokes it, so the overlaid script is the one that runs;
3. the runner refuses anything but a 40-character SHA already merged to main.
"""

from __future__ import annotations

import io
from pathlib import Path
import re
import subprocess
import sys
import tempfile
import tokenize

sys.path.insert(0, str(Path(__file__).resolve().parent))

from workflow_policy import job_run_scalars, workflow_jobs

ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = ROOT / ".github" / "workflows" / "publish.yaml"
OVERLAY = ROOT / "scripts" / "ci" / "release-recovery-overlay.sh"

# The publish path: every script a publish.yaml lane executes, plus every module
# those scripts import. Adding a registry writer to publish.yaml without adding
# it here fails this test; removing one from here without removing it from the
# publish path fails too. Both directions are a deliberate decision.
EXPECTED_OVERLAY_PATHS = [
    "scripts/ci/crate-authorize-refresh-nodes.py",
    "scripts/ci/crate-publish-plan.py",
    "scripts/ci/download-release-write-evidence.sh",
    "scripts/ci/release-candidate.py",
    "scripts/ci/release-publish-preflight.py",
    "scripts/ci/release_action.py",
    "scripts/ci/release_candidate_manifest.py",
    "scripts/ci/release_registry.py",
    "scripts/ci/release_rehearsal.py",
    "scripts/publish_crates.py",
    "scripts/publish_npm_artifacts.py",
    "scripts/set_release_version.py",
]
# The runner installs itself from the reviewed SHA before it runs, so it is part
# of the publish path without being one of its own targets.
RUNNER_PATH = "scripts/ci/release-recovery-overlay.sh"
SHA_LENGTH = 40
BOOTSTRAP = (
    f'git show "$RECOVERY_OVERLAY_SHA:{RUNNER_PATH}"',
    f'install -m 0755 "$RUNNER_TEMP/release-recovery-overlay.sh" {RUNNER_PATH}',
    f'bash {RUNNER_PATH} "$RECOVERY_OVERLAY_SHA"',
)
SCRIPT_REFERENCE = re.compile(r"scripts/[A-Za-z0-9_./-]+\.(?:py|sh)")
SCRIPT_FILENAME = re.compile(r"^[A-Za-z0-9_.-]+\.(?:py|sh)$")
LOADED_MODULE = re.compile(r"spec_from_file_location\(\s*\"([A-Za-z0-9_]+)\"")
SIBLING_IMPORT = re.compile(r"(?m)^(?:import|from) ([a-z0-9_]+)(?: import| as |$)")


def declared_overlay_paths(source: str) -> list[str]:
    body = source.split("OVERLAY_PATHS=(", 1)[1].split(")", 1)[0]
    return [line.strip() for line in body.splitlines() if line.strip()]


def script_path(stem: str) -> str | None:
    """Resolve a module name or bare filename to its checked-in script path."""
    suffixes = ("", ".py") if stem.endswith((".py", ".sh")) else (".py",)
    for directory in ("scripts/ci", "scripts"):
        for name in (stem, stem.replace("_", "-")):
            for suffix in suffixes:
                candidate = f"{directory}/{name}{suffix}"
                if (ROOT / candidate).is_file():
                    return candidate
    return None


def dependencies_of(path: str) -> set[str]:
    """Return the checked-in scripts one overlaid script loads at runtime.

    Comments are excluded: a script that only names another script in prose does
    not execute it, and the publish path is about what runs.
    """
    source = (ROOT / path).read_text(encoding="utf-8")
    tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    code = "\n".join(token.string for token in tokens if token.type != tokenize.COMMENT)
    names = [*LOADED_MODULE.findall(code), *SIBLING_IMPORT.findall(source)]
    names += [
        token.string.strip("\"'")
        for token in tokens
        if token.type == tokenize.STRING and SCRIPT_FILENAME.match(token.string.strip("\"'"))
    ]
    resolved = {found for name in names if (found := script_path(name)) is not None}
    return resolved | set(SCRIPT_REFERENCE.findall(code))


def normalize(run: str) -> str:
    """Collapse shell layout, including backslash line continuations."""
    return " ".join(run.replace("\\\n", " ").split())


def run_overlay(work: Path, sha: str, runner: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["bash", str(runner), sha],
        cwd=work,
        capture_output=True,
        text=True,
        check=False,
    )


def git(work: Path, *args: str) -> str:
    return subprocess.run(
        ["git", *args],
        cwd=work,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()


overlay_source = OVERLAY.read_text(encoding="utf-8")
overlay_paths = declared_overlay_paths(overlay_source)

# 1. The overlay set is exactly the intended list, in one reviewed place.
assert overlay_paths == EXPECTED_OVERLAY_PATHS, overlay_paths
assert overlay_paths == sorted(overlay_paths), "keep the overlay list sorted"
assert len(set(overlay_paths)) == len(overlay_paths), "duplicate overlay path"
for path in (*overlay_paths, RUNNER_PATH):
    assert (ROOT / path).is_file(), path

# 2. The runner fails closed on anything but a reviewed, merged, full SHA.
assert overlay_source.startswith("#!/usr/bin/env bash")
assert "set -euo pipefail" in overlay_source
assert 'test "${#overlay_sha}" -eq 40' in overlay_source
assert 'git cat-file -e "$overlay_sha^{commit}"' in overlay_source
assert 'git merge-base --is-ancestor "$overlay_sha" refs/remotes/origin/main' in overlay_source
assert "refs/remotes/origin/main:scripts" not in overlay_source
for forbidden in ("|| true", "sleep", "set +e", "continue-on-error"):
    assert forbidden not in overlay_source, forbidden

workflow = WORKFLOW.read_text(encoding="utf-8")
jobs = workflow_jobs(workflow)
assert "recovery_overlay_sha:" in workflow

# 3. Every script the publish path executes is overlayable.
executed: dict[str, set[str]] = {}
for job, body in jobs.items():
    for run in job_run_scalars(body):
        for reference in SCRIPT_REFERENCE.findall(run):
            executed.setdefault(reference, set()).add(job)
for reference, owners in sorted(executed.items()):
    assert reference in overlay_paths or reference == RUNNER_PATH, (
        f"{reference} runs in publish.yaml job(s) {sorted(owners)} but is not overlayable"
    )

# 4. The closure holds: an overlaid script never imports a stale sibling.
for path in overlay_paths:
    if not path.endswith(".py"):
        continue
    for dependency in sorted(dependencies_of(path) - {path}):
        assert dependency in overlay_paths, f"{path} depends on un-overlaid {dependency}"

# 5. Every job that executes a publish-path script installs and runs the reviewed
#    runner, so the overlaid copy is the one the lane executes.
publish_path_jobs = {job for owners in executed.values() for job in owners}
assert publish_path_jobs == {
    "candidate-preflight",
    "publish-pypi",
    "npm-native",
    "npm-main",
    "npm-cli",
    "npm-skills",
    "publish-crates",
    "reconcile",
}, sorted(publish_path_jobs)
for job in sorted(publish_path_jobs):
    body = jobs[job]
    runs = [normalize(run) for run in job_run_scalars(body)]
    bootstrap = [run for run in runs if normalize(BOOTSTRAP[2]) in run]
    assert len(bootstrap) == 1, f"{job} must bootstrap the overlay runner exactly once"
    for fragment in BOOTSTRAP:
        assert normalize(fragment) in bootstrap[0], f"{job} is missing: {fragment}"
    # The reviewed SHA is proven merged before any of its content is executed.
    installed = bootstrap[0].index(normalize(BOOTSTRAP[0]))
    ancestor = bootstrap[0].index(
        'git merge-base --is-ancestor "$RECOVERY_OVERLAY_SHA" refs/remotes/origin/main'
    )
    assert ancestor < installed, f"{job} runs overlay content before proving it is merged"
    assert 'test "${#RECOVERY_OVERLAY_SHA}" -eq 40' in bootstrap[0], job
    # Overlays are a recovery affordance only; a release event runs the tag as
    # cut, gated either by the step condition or by the preflight else branch.
    step_gated = "if: github.event_name == 'workflow_dispatch'" in body
    release_branch = normalize('if test "$GITHUB_EVENT_NAME" = release; then')
    shell_gated = release_branch in bootstrap[0] and bootstrap[0].index(" else ") < installed
    assert step_gated or shell_gated, f"{job} overlays outside a recovery dispatch"

# 6. The runner actually replaces the tag's copies, and refuses an unreviewed SHA.
with tempfile.TemporaryDirectory() as scratch:
    root = Path(scratch)
    upstream = root / "upstream"
    upstream.mkdir()
    git(upstream, "init", "--quiet", "--initial-branch=main", ".")
    git(upstream, "config", "user.email", "release@example.invalid")
    git(upstream, "config", "user.name", "Release Fixture")
    git(upstream, "config", "uploadpack.allowAnySHA1InWant", "true")
    for generation in ("defective", "reviewed"):
        for path in overlay_paths:
            target = upstream / path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(f"{generation} {path}\n", encoding="utf-8")
        git(upstream, "add", "--all")
        git(upstream, "commit", "--quiet", "--message", generation)
    tag_sha = git(upstream, "rev-parse", "HEAD~1")
    reviewed_sha = git(upstream, "rev-parse", "HEAD")
    git(upstream, "checkout", "--quiet", "-b", "unmerged", tag_sha)
    (upstream / overlay_paths[0]).write_text("unreviewed\n", encoding="utf-8")
    git(upstream, "add", "--all")
    git(upstream, "commit", "--quiet", "--message", "unmerged")
    unmerged_sha = git(upstream, "rev-parse", "HEAD")
    git(upstream, "checkout", "--quiet", "main")

    work = root / "tag-checkout"
    git(root, "clone", "--quiet", str(upstream), str(work))
    git(work, "checkout", "--quiet", tag_sha)
    assert (
        (work / "scripts/publish_npm_artifacts.py")
        .read_text(encoding="utf-8")
        .startswith("defective")
    )

    runner = work / RUNNER_PATH
    runner.parent.mkdir(parents=True, exist_ok=True)
    runner.write_text(overlay_source, encoding="utf-8")

    accepted = run_overlay(work, reviewed_sha, runner)
    assert accepted.returncode == 0, accepted.stderr
    for path in overlay_paths:
        content = (work / path).read_text(encoding="utf-8")
        assert content == f"reviewed {path}\n", (path, content)
    # The npm publisher is the case that motivated this contract.
    assert f"overlaid scripts/publish_npm_artifacts.py from {reviewed_sha}" in accepted.stdout

    git(work, "checkout", "--quiet", "--force", tag_sha)
    assert run_overlay(work, unmerged_sha, runner).returncode != 0, "unmerged SHA must be refused"
    assert run_overlay(work, reviewed_sha[:7], runner).returncode != 0, "short SHA must be refused"
    assert run_overlay(work, "main", runner).returncode != 0, "a branch name must be refused"
    assert (
        (work / "scripts/publish_npm_artifacts.py")
        .read_text(encoding="utf-8")
        .startswith("defective")
    )
    assert len(reviewed_sha) == SHA_LENGTH

print("release recovery overlay tests passed")
