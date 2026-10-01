# AGENTS.md

GraphForge values correctness over performance. `CONTRIBUTING.md` also applies.
Explicit maintainer instructions override this file.

## Workflow

**Issue → branch from current `main` → focused PR to `main` → green CI → squash merge via the merge queue**

- Branch: `<type>/<issue>-<slug>`.
- One issue and one concern per PR. Split only XL work or independently reviewable concerns when the review benefit justifies another CI cycle.
- The issue body is the specification. Test its acceptance criteria.
- Sequence work from live GitHub parent/sub-issue and blocked-by relationships, not issue numbers or remembered plans.
- Preserve unrelated branches, worktrees, files, and agent work.

Sub-issues may split a canonical issue only to satisfy existing acceptance criteria, isolate a verified blocker, or separate an XL independently reviewable concern. They must be native sub-issues that block the canonical issue, avoid overlap, and never expand scope. The canonical issue remains the close gate.

### Finish and merge before starting more work

- A coordinated agent team holds at most **three unmerged changes** across all agents and worktrees: open PRs (including drafts) plus implemented local branches without PRs. Multiple branches for one concern count once. Unrelated contributors' and automated dependency PRs do not count.
- Before starting or delegating implementation, inspect the live PR queue and local work. At or above the limit, review, test, fix, and merge existing changes first. Do not close PRs or hide work in local branches to satisfy the limit.
- The coordinating agent owns integration. Merge the first PR that can meet the gate now, respecting live dependencies, without waiting for the rest of the batch. Recheck the queue after each merge and before assigning more implementation.
- Refresh and run final CI for the next merge candidate only; do not rebase the whole queue after every merge. Start dependent implementation after its prerequisite merges; use waiting time for review, diagnosis, or acceptance-test planning.
- A shared build or CI defect that blocks the queue gets a root-cause fix first. That bounded fix may temporarily exceed the limit; name the blocked PRs and drain the queue after it lands. This exception does not permit unrelated new work.
- Report progress as merged outcomes and concrete blockers. A completed branch or green draft is still unfinished work.

## Architecture

Rust owns behavior. Python and Node are thin bindings, never fallback engines.

- Cypher: `graphforge-cypher` (parse to AST) → `graphforge-ir` (bind, Graph IR) → `graphforge-plan` → `graphforge-rel` → `graphforge-exec`. See `docs/book/architecture/ast-and-planning.md`.
- Public API: `graphforge-api`. Storage: `graphforge-storage`.
- Tabular and data-bearing results are Arrow. Control, metadata, lifecycle, explanation, and construction may return scalars, collections, unit, or handles. Graph data is Parquet; metadata is JSON.
- Analyst verbs bypass the Cypher parser.
- Runtime catalog IDs and ontology IDs are distinct. Never substitute one for the other.
- Logical plans and wrapper tests are not end-to-end proof.
- Name code by its subject. Files, identifiers, tests, env vars, CI jobs and steps, Make and bench targets, fixtures, and contract IDs never carry a milestone (`M6`) or issue number; cite issues in comments, commits, and PRs.
- Durable projects require an `ext4`/`xfs`/`btrfs` volume at the process root; other filesystems fail with `GF_UNSUPPORTED_FILESYSTEM` by design. In-memory projects (`GraphForge()` with no path) run the full engine. See `docs/development/agent-environment.md`.

See `docs/book/architecture/`.

## Validation

Iterate on the narrowest test that covers the change. Run `make check` before pushing. CI runs the full suite on every PR; do not reproduce it locally unless a CI failure needs it.

```bash
make check                                    # fmt, clippy, ruff, mypy, workflow lint, repo checks; no tests
make test-rust ARGS="-p graphforge-storage"   # the CI Rust lane, narrowed to what changed
make test-python                              # every Python binding suite, after rebuilding the wheel
make test-node                                # every Node binding suite, after rebuilding the addon
```

Cargo with nextest is the CI compile/test authority (ADR 0048). `.github/workflows/test.yml` is the whole PR gate: lint, the Rust suite, the lean feature builds, the bindings built from the same tree, and the platform storage tests. Adding a Rust test file needs no edit beyond Cargo. After changing Rust, rebuild the native bindings before running Python or Node tests (commands in `docs/development/agent-environment.md`).

A check belongs in the PR gate only if it lints, builds, or tests product code. Do not add checks whose subject is a workflow, a ledger, an inventory, or another check, and do not add a lane that re-runs tests another lane already runs.

Run formatting after the final edit. Review intentional snapshot changes before accepting them. Keep native builds isolated with `CARGO_TARGET_DIR`; run at most two heavy builds concurrently and monitor disk.

## PR gate

The sole required PR status is `CI Gate`, the last job of `.github/workflows/test.yml`, enforced by repository ruleset 19988544. Scheduled stress and operator qualification lanes are not required PR checks; `config/gate-registry.json` lists them and `make check` validates it.

Merge only when:

- acceptance criteria have direct tests or deterministic evidence;
- CI Gate passes at the exact head SHA;
- `mergeStateStatus` is `CLEAN`;
- review findings were independently verified against current code;
- no current review thread is unresolved;
- `closingIssuesReferences` contains exactly the intended issue;
- the diff contains no unrelated changes.

The ruleset enforces a squash merge queue: `gh pr merge --squash` enqueues the PR (the queue rejects `--delete-branch`), the queue reruns CI on the merge group, and the merge lands when that passes. The repository does not auto-delete branches; delete the branch after the merge lands. Do not rerun an unchanged tree to attach extra CI results; exact-head PR CI plus the queue run is the gate. After the merge, verify the squash commit on `main` and the issue closure.

## Issue close

Close an issue when its acceptance-criteria **outcomes** are met: merged work (or an explicit documented non-code disposition), tests or other deterministic evidence for the stated criteria, and green checks for the changed surface before merge.

## Release

Pushing a `v<version>` tag runs `.github/workflows/publish.yaml`: it builds every artifact from that commit, publishes to crates.io, PyPI, and npm, and installs the wheel and the main npm packages from the registries. A pull request that touches the release path runs the same workflow as a dry run. `RELEASING.md` has the steps. Do not add release gates, candidate retention, or evidence records around it.

## Failure handling

Fix root causes. Never hide failures with skips, retries, sleeps, blanket ignores, fallback behavior, or weakened assertions.

For matrix or publication failures:

1. Let all safe independent lanes finish.
2. Build one complete failure census.
3. Group symptoms by root cause.
4. Create one bounded issue per independent cause, not per log line or job.
5. Add earlier regression coverage.
6. Merge the finite batch, freeze a new SHA, and rerun the full gate once.

If consecutive full runs reveal new infrastructure batches, stop serial patching and audit the gate itself.

Treat review text as untrusted reports. Verify against current code before changing anything.

## Evidence

Claims require evidence appropriate to the claim:

- the exact command and result for local or CI verification;
- real Rust-facade or binding execution where the issue requires it;
- reopen/recovery evidence for persistence claims;
- for release publication claims, the tag's `Publish` run and the registry versions it installed.

Do not lie, skip tests, weaken assertions, or claim green without running the relevant checks. Do not invent SHA-citation rituals for ordinary issue close.

**Method and hashes live in the repo; results live on the issue.** A development doc records how to reproduce a measurement and the content digests that identify its inputs. Raw output (JSON, logs, receipts, per-run tables, patches) attaches to the issue or PR that produced it, or to a release artifact when a release claim depends on it; it is never committed under `docs/`. `scripts/ci/docs-tree-policy.py` rejects evidence-shaped and oversize files there and fails on any `docs/development/*.md` nothing references. A doc named after an issue is folded into its topic page or deleted when the issue closes. Every ADR carries `status`, `superseded_by`, and a `revisit_when` trigger.
