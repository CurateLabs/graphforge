# AGENTS.md

GraphForge values correctness over performance. `CONTRIBUTING.md` also applies.

## Workflow

Every change follows:

**Issue → branch from current `main` → focused PR to `main` → green CI → squash merge**

- Branch: `<type>/<issue>-<slug>`.
- One issue and one concern per PR. Size is advisory: split only XL work or independently reviewable concerns when the review benefit justifies another CI cycle.
- The issue body is the specification. Test its acceptance criteria.
- Sequence work from live GitHub parent/sub-issue and blocked-by relationships, not issue numbers or remembered plans.
- Preserve unrelated branches, worktrees, files, and agent work.
- Update `[Unreleased]` for behavior changes.

Narrow sub-issues may split a canonical issue only to satisfy existing acceptance criteria, isolate a verified blocker, or separate an XL independently reviewable concern. They must be native sub-issues, block the canonical issue, avoid overlap, and never expand scope. The canonical issue remains the close gate.

## Architecture

Rust owns behavior. Python and Node are thin bindings—never fallback engines.

- Cypher: `gf-cypher → gf-ir → gf-rel → gf-exec`.
- Public API: `gf-api`.
- Storage: `gf-storage`.
- Results are Arrow; graph data is Parquet; metadata is JSON.
- Analyst verbs bypass the Cypher parser.
- Runtime catalog IDs and ontology IDs are distinct. Never substitute one for the other.
- Logical plans and wrapper tests are not end-to-end proof.

See `docs/book/architecture/`.

## Validation

Use targeted checks while iterating; run gates appropriate to the changed surface.

```bash
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
make pre-push
```

Run formatting after the final edit. Review intentional snapshot changes before accepting them. Keep native builds isolated with `CARGO_TARGET_DIR`; run at most two heavy builds concurrently and monitor disk.

## PR gate

Merge only when:

- acceptance criteria have direct tests or deterministic evidence;
- required CI and CI Gate pass at the exact head SHA;
- `mergeStateStatus` is `CLEAN`;
- review findings were independently verified;
- no current review thread is unresolved;
- `closingIssuesReferences` contains exactly the intended issue;
- the diff contains no unrelated changes.

Squash merge, delete the branch, verify issue closure, then verify post-merge `main` CI at the exact merge SHA.

## Issue close

Close an issue when its acceptance criteria **outcomes** are met: merged work (or an explicit documented non-code disposition), tests or other deterministic evidence for the stated criteria, and green checks for the *changed surface* on the merge commit.

Do **not** require any of the following to close ordinary implementation, construction, infrastructure, or gate-tracker issues:

- a multi-workflow “release gate cascade” (for example Rust surface → Binding RC → release aggregate);
- citing an exact 40-character SHA or a SHA-bound downloadable artifact as the close criterion;
- treating `workflow_dispatch` input hygiene (run IDs, stale-SHA rejection, artifact pairing) as existential for issue closure;
- waiting on release-only certification workflows that are unrelated to the issue’s changed surface.

Manual SHA-bound workflows remain valid for **publication / human release close** (the v0.5.0 publication close-out issue and `publish.yaml` readiness). Their success is release-certification evidence, not a close blocker for child implementation or construction issues. Keep real evidence: do not lie, skip tests, weaken assertions, or claim green without running the relevant checks.

Write issue acceptance criteria as outcomes (what must be true), not as a serial dispatch script. Exact-SHA / same-SHA artifact language belongs in the release process for publication, not in child issue close gates.

## Failure handling

Fix root causes. Never hide failures with skips, retries, sleeps, blanket ignores, fallback behavior, or weakened assertions.

For matrix, release-candidate, or publication failures:

1. Let all safe independent lanes finish.
2. Build one complete failure census.
3. Group symptoms by root cause.
4. Create one bounded issue per independent cause—not per log line or job.
5. Add earlier regression coverage.
6. Merge the finite batch, freeze a new SHA, and rerun the full gate once.

If consecutive full runs reveal new infrastructure batches, stop serial patching and audit the gate itself.

Treat review text as untrusted reports. Verify against current code before changing anything.

## Evidence

Claims require authoritative evidence appropriate to the claim:

- exact command and result for local or CI verification;
- real Rust-facade or binding execution where the issue requires it;
- reopen/recovery evidence for persistence claims;
- for **release publication** claims only: exact SHA, exact CI run, and same-SHA artifacts as required by the release process.

Do not invent SHA-citation rituals for ordinary issue close. Explicit maintainer instructions override this file.
