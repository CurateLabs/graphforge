# Release process

GraphForge releases one coordinated version across all Rust crates, PyPI, npm
native packages, the main Node package, CLI, and agent skills. This page is the
operator checklist; the executable dependency and recovery rules live in
[`publication-order.md`](publication-order.md).

## 1. Select the release

1. Choose the intended reviewed `main` commit.
2. Confirm the milestone and release tracker contain only the intended scope.
3. Set one root version across every package surface.
4. Run `python3 scripts/set_release_version.py --check`.

Version-specific release prose belongs to the immutable GitHub Release. Use
GitHub-generated notes or explicitly supplied notes; GraphForge does not keep a
repository-maintained change history file.

## 2. Prove the candidate

CI Rust compilation and the mapped test graph are Bazel-owned under required
check **`CI Gate`** (see [bazel.md](bazel.md) and
[bazel-migration-cutover.md](bazel-migration-cutover.md)). Binding RC and publish
lanes must consume Bazel-built (or equivalent) natives rather than silently
recompiling a different native graph. Publish credentials and OIDC stay in
release workflows — never in cacheable Bazel actions.

The Binding Release Candidate workflow must retain the manifest, Python, npm,
crates, and evidence partitions for the same commit. Before any registry write,
the exact retained bytes must pass:

- complete archive inventory and required-file validation;
- one-version and first-party dependency validation;
- Python, Node/native, CLI, and agent-skills offline consumers;
- all 16 crate package and dependency checks; and
- license and notice validation.

A checksum match proves byte identity, not artifact completeness.

## 3. Authorize immutable release identity

A maintainer separately authorizes creation of the annotated tag and GitHub
Release. The tag must resolve to the reviewed `main` commit and is never moved.

```bash
git switch main
git pull --ff-only origin main
python3 scripts/set_release_version.py --check
git tag -a vX.Y.Z -m "GraphForge vX.Y.Z"
git push origin vX.Y.Z
gh release create vX.Y.Z --title "GraphForge vX.Y.Z" --generate-notes
```

Publishing is triggered only by the authorized GitHub Release or an authorized
recovery dispatch for the same immutable release identity. Recovery
`workflow_dispatch` requires `release_tag`, `recovery_reason`, and a reviewed
`recovery_overlay_sha` (publisher scripts overlay that SHA, not floating
`main`). Registry write jobs use GitHub Environment `release`; concurrency
group `publish-<tag>` sets `cancel-in-progress: false`.

## 4. Publish and reconcile

Publication is planner-driven:

- PyPI is an independent OIDC lane.
- Five native npm packages may run in parallel; main, CLI, and skills follow
  verified dependency fan-in.
- Crates publish in their checked dependency order.
- Every lane re-observes public registry truth immediately before a write.
- Accepted or ambiguous write attempts never authorize a duplicate write.
- The final reconciliation always runs and covers all 27 public nodes.

Job history is operator context only. Recovery state comes from the immutable
candidate, durable write evidence, retained artifacts, and live registry truth.

## 5. Verify public consumers

After reconciliation reports every node verified:

1. Install the exact version from PyPI in a clean environment.
2. Install the exact npm main, CLI, and skills packages from the public registry.
3. Resolve the crate graph from crates.io.
4. Confirm GitHub Release notes and public documentation are available.
5. Record the evidence on the human release tracker and close it.

## Recovery rules

- Do not move a tag or replace release assets with different bytes.
- Do not rebuild a missing partition from another commit.
- Do not retry a write whose acceptance is unknown.
- Stop on conflict, indeterminate registry truth, missing/expired artifacts, or
  credential failure.
- Yank or deprecate incorrect public versions and prepare one coordinated later
  version.

See [`publication-order.md`](publication-order.md) for the complete state model
and stop conditions.
