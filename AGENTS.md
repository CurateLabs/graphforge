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

Narrow sub-issues may split a canonical issue only to satisfy existing acceptance criteria, isolate a verified blocker, or separate an XL independently reviewable concern. They must be native sub-issues, block the canonical issue, avoid overlap, and never expand scope. The canonical issue remains the close gate.

## Architecture

Rust owns behavior. Python and Node are thin bindings—never fallback engines.

- Cypher: `graphforge-cypher → graphforge-ir → graphforge-rel → graphforge-exec`.
- Public API: `graphforge-api`.
- Storage: `graphforge-storage`.
- Tabular/data-bearing results are Arrow; control/metadata/lifecycle/explanation/construction may return scalars, collections, unit, or handles; graph data is Parquet; metadata is JSON.
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
make pre-push-fast   # includes bazelisk + Cargo/Bazel drift
make pre-push
make bazel-test      # optional: authoritative //:ci_rust_tests locally
```

CI Rust compile/test authority is Bazel (`//:ci_rust_tests`); see
`docs/development/bazel.md`. `make pre-push-fast` requires `bazelisk` on `PATH`
and runs `cargo-bazel-drift-check.py`. Full `//:ci_rust_tests` is optional
locally via `make bazel-test` (heavy).

Run formatting after the final edit. Review intentional snapshot changes before accepting them. Keep native builds isolated with `CARGO_TARGET_DIR`; run at most two heavy builds concurrently and monitor disk.

## PR gate

`config/gate-registry.json` is the machine-readable gate authority. Its sole
required PR status is `github-status/CI Gate` with `sha_rule=exact_head`, matching
repository ruleset 19988544. Scheduled stress, operator qualifications, and
release certification evidence are not required PR checks. Validate changes
with `make gate-registry-check`.

Merge only when:

- acceptance criteria have direct tests or deterministic evidence;
- required CI and CI Gate pass at the exact head SHA;
- `mergeStateStatus` is `CLEAN`;
- review findings were independently verified;
- no current review thread is unresolved;
- `closingIssuesReferences` contains exactly the intended issue;
- the diff contains no unrelated changes.

Squash merge, delete the branch, then verify the merge and issue closure. Do not
rerun an unchanged tree solely to attach duplicate CI results to the squash
commit; exact-head PR CI is the merge gate.

## Issue close

Close an issue when its acceptance criteria **outcomes** are met: merged work (or an explicit documented non-code disposition), tests or other deterministic evidence for the stated criteria, and green checks for the changed surface before merge.

Do **not** require any of the following to close ordinary implementation, construction, infrastructure, or gate-tracker issues:

- a multi-workflow “release gate cascade” (for example Rust surface → Binding RC → release aggregate);
- waiting on release-only certification workflows that are unrelated to the issue’s changed surface.

Manual SHA-bound workflows remain valid only for **publication / human release close** (the v0.5.0 publication close-out issue and `publish.yaml` readiness). Keep real evidence: do not lie, skip tests, weaken assertions, or claim green without running the relevant checks.

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
- for release publication claims, the SHA-bound evidence required by the release process.

Do not invent SHA-citation rituals for ordinary issue close. Explicit maintainer instructions override this file.

## Cursor Cloud specific instructions

Environment layer split: the Cloud Agent snapshot already contains the system
toolchains (Rust via `rust-toolchain.toml`, Python 3.12, Node, `pnpm`), plus
`uv` and `bazelisk` installed into `/usr/local/cargo/bin` (on `PATH`), and warm
Cargo/`target/` + built native bindings. The startup update script only
refreshes project dependencies:

```bash
uv sync --all-extras --inexact
pnpm install
```

Standard lint/test/build/run commands are in the `Makefile` and `package.json`;
prefer those. Non-obvious caveats for this environment:

- **Plain `uv sync` prunes the native wheel.** Direct `uv sync --all-extras`
  uninstalls the maturin-built `graphforge` package. `make install` and the
  startup update script use `uv sync --all-extras --inexact` to preserve it;
  use the same flag for direct syncs, or rebuild afterwards. The editable
  rebuild is instant when `target/` is warm.
- **Rebuild native bindings after changing Rust.** The update script does not
  build. After pulling Rust changes, rebuild before Python/Node tests:
  `uv run maturin develop --release -m crates/graphforge-bindings-py/Cargo.toml`
  (Python) and `pnpm --filter @curatelabs/graphforge build` (Node `*.node`).
- **Durable disk-backed projects cannot run on this VM.** GraphForge's native
  filesystem admission only accepts an `ext4`/`xfs`/`btrfs` volume **at the
  process root**, but Cloud Agent VMs are rooted on `overlay` (with `/tmp` also
  overlay). So `GraphForge(path)`, `gf --project <dir> ...`, and any durable
  write fail with `GF_UNSUPPORTED_FILESYSTEM` (`cause=filesystem_class_unproven`).
  A loopback `ext4` submount does **not** help: the strict cross-volume check
  rejects crossing the mount boundary (`cause=ancestor_cross_volume`), and a
  chroot rooted on the ext4 fails the `sysinfo` device probe
  (`cause=device_identity_unknown`). Treat these as an environment limitation,
  not a code bug.
- **In-memory works fully.** `GraphForge()` (no path) runs the real engine
  (Cypher parse → IR → rel → exec → Arrow) entirely in memory and is the way to
  exercise/demonstrate core functionality here.
- **Test impact.** In-memory suites pass (e.g. `cargo test -p graphforge-cypher`,
  `node --test crates/graphforge-bindings-node/tests/smoke.test.mjs`,
  `uv run pytest tests/unit` → 103 passed / 3 failed, the 3 being durable-project
  tests hitting the constraint above). `node --test` over the full binding suite
  hangs on `provider-workflow.test.mjs` because a failed durable test leaves its
  in-process mock-server `Worker` open; the root cause is the same filesystem
  constraint, so run it in isolation with `--test-timeout` if needed.
- **Bazel authority.** `bazelisk` is on `PATH` (Bazel 9.2.0 pinned by
  `.bazelversion`); `make bazel-test` / `make pre-push-fast` work.
