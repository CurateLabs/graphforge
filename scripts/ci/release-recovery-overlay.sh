#!/usr/bin/env bash
# Overlay the reviewed publish-path scripts onto an immutable tag checkout.
#
# A recovery dispatch re-runs publication against the tag whose retained bytes
# were certified by the candidate run. The bytes are never rebuilt: every lane
# downloads the same partitions and revalidates them against the manifest that
# is attached to the release. What the scripts below decide is *whether* and
# *how* an already-certified byte reaches a public registry, so replacing them
# with their reviewed counterparts from a merged commit cannot change what is
# published -- only whether a defect discovered after the tag was cut forces a
# new tag. That is the whole purpose of the recovery dispatch.
#
# Every script the publish path executes is listed here, including the modules
# those scripts import. Overlaying a top-level publisher while leaving the
# module it imports at the tag state silently runs the defective code, so the
# list is the transitive closure, not the set of command lines in the workflow.
#
# This runner is not in its own list: publish.yaml installs the reviewed copy
# of it from the same SHA before invoking it, so the list below is always the
# reviewed list and the file is never rewritten while bash is reading it.
#
# scripts/ci/test-release-recovery-overlay.py fails closed when the list and
# the publish path disagree, so a new registry writer cannot join publish.yaml
# without a deliberate decision about its recoverability.
set -euo pipefail

OVERLAY_PATHS=(
  scripts/ci/crate-authorize-refresh-nodes.py
  scripts/ci/crate-publish-plan.py
  scripts/ci/download-release-write-evidence.sh
  scripts/ci/release-candidate.py
  scripts/ci/release-publish-preflight.py
  scripts/ci/release_action.py
  scripts/ci/release_candidate_manifest.py
  scripts/ci/release_registry.py
  scripts/ci/release_rehearsal.py
  scripts/publish_crates.py
  scripts/publish_npm_artifacts.py
  scripts/set_release_version.py
)

overlay_sha="${1:?reviewed recovery overlay SHA is required}"

# A reviewed overlay is a full 40-character SHA that is already an ancestor of
# main: never a branch name, never a floating tip, never an unmerged commit.
test "${#overlay_sha}" -eq 40
git fetch --no-tags origin +refs/heads/main:refs/remotes/origin/main
git fetch --no-tags origin "$overlay_sha"
git cat-file -e "$overlay_sha^{commit}"
git merge-base --is-ancestor "$overlay_sha" refs/remotes/origin/main

for path in "${OVERLAY_PATHS[@]}"; do
  staged="${RUNNER_TEMP:-${TMPDIR:-/tmp}}/$(basename "$path")"
  git show "$overlay_sha:$path" >"$staged"
  install -m 0755 "$staged" "$path"
  rm -f "$staged"
  printf 'overlaid %s from %s\n' "$path" "$overlay_sha"
done
