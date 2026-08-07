# Testing

GraphForge proves shippable behavior with deterministic Rust and binding tests,
the openCypher TCK as the language oracle, and contract inventories for non-Cypher
surfaces. Correctness and registry honesty are non-negotiable; skips, sleeps,
retries-as-green, and weakened assertions do not satisfy gates (`AGENTS.md`,
[`../development/testing.md`](../development/testing.md)).

**Speed is a first-class engineering value alongside honesty.** Every surface
has a wall-clock target, sheds work that is not required for its objective, and
parallelizes the rest. Frequent publishing uses the **publish-track**, not a
separately named “nightly” product. Full `llvm-cov` / `make coverage-rust` is a
local (or coverage-sensitive) honesty tool — **PR CI does not run full coverage**.

This page is the **v0.5.0 / release-prep testing strategy** that shipped on
`main`: how layers compose, what each gate proves, and what does not count as
end-to-end evidence. Command recipes and historical suite layout live in
[`../development/testing.md`](../development/testing.md). Workflow mechanics live
in [`.github/workflows/README.md`](../../.github/workflows/README.md).

## Dual-track objectives (PR / publish-track / human close)

| Surface | Objective | Required when | Wall-clock target | Must keep | Shed / defer |
| --- | --- | --- | --- | --- | --- |
| `pre-push-fast` | Policy/format | Local habit | ~30s | lint/license/workflow | Full coverage |
| PR Test Suite + CI Gate | Changed-surface correctness | Every PR → `main` | ≤10m p50 / ≤12m p95 | Classifier, same-SHA Linux bindings, workspace tests, Gate | Multi-OS, load, llvm-cov, Binding RC |
| `make coverage-rust` | Honest floors | Coverage-sensitive changes / floor claims | ≤20m p50 local | Hash/runtime/ledger; real acceptance | HTML by default; CI enforcement |
| Binding RC | Multi-OS publish bytes + offline rehearsal | publish-track and human close | ≤20m p50 warm / ≤35m cold | Retained multi-OS artifacts, same-SHA, offline rehearsal | Full PR suite re-run; cold builds when sticky hits |
| **publish-track** | Registry-honest publish certification | Whenever we publish (scheduled or on-demand) | ≤35m p50 / ≤50m cold (RC + tag + publish) | Binding RC bytes + `publish.yaml` no-rebuild | M1, checkpoint, m20/m21, full clean-env |
| **Human release close** | Milestone / coordinated GA confidence | Human publication close | publish-track + optional gates | publish-track honesty **plus** M1 / surface gates as documented | — |
| Unchanged-SHA reuse | Skip redundant RC | Same `main` tip + unexpired candidate | RC ~0; publish-only ≤15m | Candidate completeness checks | Rebuilding identical bytes |
| Fuzz / stress / viz | Diagnostic | Schedule/manual | N/A | Not merge or publish-track blockers | — |

**publish-track** is Binding RC → tag / release identity → `publish.yaml` on
retained bytes. M1 load, checkpoint recovery, and m20/m21 surface aggregates remain
**human-close / milestone** evidence — they are not registry-honesty inputs and
must not block every publish.

## Ownership

Rust owns behavior. The public facade is `graphforge-api`; Cypher runs
`graphforge-cypher → graphforge-ir → graphforge-rel → graphforge-exec`; storage and project format live in
`graphforge-storage`. Python and Node are thin bindings that project Rust semantics into
language-native APIs and Arrow/IPC — never fallback engines or parallel
implementations of graph logic.

Consequence for tests:

- Prove semantics in Rust (crate tests, facade integration, TCK BDD).
- Prove bindings by clean-install of a same-SHA wheel/addon and equality of
  results/errors against the Rust contract — not by re-implementing algorithms
  in the binding language.
- Treat logical-plan construction, wrapper smoke, and “compiled successfully”
  as necessary but **not** sufficient for shippable behavior.

### Public API BDD classifications

The shared scenarios in `tests/features/api/` use three explicit states:

- **Required:** the applicable Rust, Python, and Node runners call the real
  public surface and assert exact Arrow schema, rows, values, or structured
  error classes. Missing steps, exceptions, xfail/xpass, pending results, and
  unexpected skips fail the gate.
- **Product-excluded:** `@excluded-api-bdd` or
  `@excluded-node-api-bdd` identifies behavior that has a confirmed product
  defect. The scenario must appear in
  `tests/contracts/api-bdd-exclusions.json`, carry exactly one matching open
  `@issue-N` reference, and contributes only to the excluded total—never the
  passing total.
- **Binding-only:** runtime coercion and closed-handle scenarios execute in
  Python and Node but are reported as not applicable by the statically typed,
  non-closeable Rust facade. This classification is allowlisted by repository
  policy and is not a product-behavior exclusion.

`scripts/ci/api-bdd-policy.py` validates the corpus and writes
`target/api-bdd-policy.json` as machine-readable classification evidence.
Its policy mutation tests reject stale inventory rows, untracked exclusions,
language skip tags, xfail conversion, pending Node steps, and manufactured Rust
errors. The BDD mutation sentinels separately prove that wrong row counts,
missing columns, wrong values, wrong error classes, and `NotImplementedError`
all produce failing test processes.

This fail-closed public API model does not change the openCypher TCK. The TCK
continues to use its separately documented advisory passing-set baseline.

## Layered gates

Release readiness is a stack. Lower layers run on every applicable PR; higher
layers are SHA-bound release certification.

| Layer | When it runs | What green means |
| --- | --- | --- |
| Policy / docs | Every PR (docs path for site) | Workflows valid, license/domain policy hold; Starlight builds |
| Unit + workspace | Rust (or classified) changes | Crate logic and `cargo test --workspace` pass with Clippy `-D warnings` |
| Binding acceptance (PR) | Binding / classified changes | One same-SHA Linux Python wheel and Node addon; native contracts; short concurrency matrix |
| Language oracle | Workspace / TCK entrypoints | openCypher TCK runnable scenarios pass (currently **3897/3897**) |
| Binding release candidate | publish-track and human close; exact `main` SHA | Clean-install multi-OS natives + offline rehearsal; fail-closed aggregate; retained publish bytes |
| publish-track publication | Scheduled or on-demand publish | Binding RC retained bytes → tag → `publish.yaml` (no rebuild-on-write) |
| Surface / recovery / load certification | Human release close (optional / milestone) | Non-Cypher inventory, checkpoint recovery, XS–XL load ledger — **not** publish-track blockers |
| Human publication close | Coordinated GA / milestone | publish-track honesty **plus** documented human-close gates |

Ordinary implementation issues close on acceptance-criteria outcomes and green
checks for the **changed surface**. They do **not** require Binding RC,
publish-track, or the human-close cascade. Exact SHA pairing and downloadable
artifacts are publication evidence — see `AGENTS.md` § Issue close.

### Pull-request contract (Test Suite + CI Gate)

- A deterministic classifier enables only the Rust, Python, Gherkin, binding, or
  agent-skills jobs that own the diff. Docs-only PRs do not compile native code.
- One required **CI Gate** aggregates applicable jobs: intentionally skipped
  lanes are fine; failed or cancelled applicable jobs are not.
- PR native binding acceptance is **Linux-only** and uses Cargo’s `dev` profile.
  That is fast feedback, not multi-OS certification.
- When Rust surfaces change, Test Suite runs authoritative Bazel tests
  (`Bazel Bootstrap` → `//:ci_rust_tests`) plus Cargo fmt/clippy, and also runs
  `Windows graphforge-storage Locks`
  (`cargo test -p graphforge-storage project_generation::tests:: --lib` on
  `blacksmith-4vcpu-windows-2025`) for the `#[cfg(windows)]` project-root lock
  unit tests that Linux Bazel CI cannot execute.
- Repository policy always validates workflow syntax, the classifier, domain
  dependency directions, license compliance, and the ledgers that back later
  release gates (without running those heavy matrices on every PR).

### Binding Release Candidate

Maintainers dispatch Binding RC with an exact 40-character `main` SHA. It
clean-installs Python wheels and executes native Node addons on Linux, macOS,
and Windows, package-validates cross-built Node targets, and emits one
fail-closed aggregate. Missing targets, mixed SHAs, fallback execution, and
parity mismatches reject the candidate. It does not tag or publish.

**Windows posture:** the Windows Python lane proves user-facing use of the
installed abi3 wheel (build → clean-install → native contracts). It is **not** a
second MSVC `cargo test` of the full Rust workspace. Windows project-root lock
unit tests are hosted by Test Suite `Windows graphforge-storage Locks`, not Binding RC.
Do not treat “wheel contracts green” as “every Rust unit test ran under MSVC.”

### Non-Cypher surface and other publication gates

The TCK cannot substitute for construction, lifecycle, checkpoints, analyst
verbs, search, or knowledge/epistemic surfaces. The checked-in
`tests/contracts/non-cypher-rust-surface.json` inventory classifies every public
Rust receiver method (and related registry/mode rows) with linked evidence.
Manual SHA-bound workflows (Rust non-Cypher surface gate, knowledge/epistemic
contract gates, checkpoint recovery, final non-Cypher surface aggregate, load
matrix) assemble immutable publication reports. Some GitHub workflow *filenames*
and artifact names still carry historical tokens; document them by **role**, not
as product milestones.

### Documentation gate

Docs changes run `.github/workflows/docs.yml`: `pnpm docs:build` syncs
allowlisted `docs/**` into the Starlight site and fails the PR if the site does
not build. The same command imports only the pinned, checksummed public snapshot declared in
`docs-site/external-docs.json` from `graphforge-vscode/docs/published/`; mutable revisions,
missing sources, and checksum drift fail closed without requiring network access. The snapshot
is refreshed explicitly with `pnpm docs:update-extension <full-commit-sha>`. Locally, prefer
`pnpm docs:test-extension`, `pnpm docs:build`, and `pnpm docs:check-links` before
push when editing published pages. Docs green is part of merge readiness for
docs surfaces; it does not prove runtime behavior.

## What counts as proof

| Claim | Acceptable evidence | Not enough alone |
| --- | --- | --- |
| Cypher semantics | TCK BDD / `make test-tck`; facade `execute` tests returning Arrow | Parser-only or logical-plan unit tests |
| Analyst verbs / find | `graphforge-api` surface tests + non-Cypher inventory rows | Binding wrapper that never calls Rust |
| Persistence / reopen | Facade lifecycle + kill-reopen / recovery suites | “Wrote Parquet files” without reopen readback |
| Binding parity | Same-SHA clean-install wheel/addon; Arrow/IPC and error-code equality | Import smoke or stubbed natives |
| Concurrency contract | Frozen short matrix in PR CI; stress lane is diagnostic | Stress retries used as the merge gate |
| publish-track publication | Exact SHA + same-SHA Binding RC retained bytes + `publish.yaml` no-rebuild | Green PR CI on an unrelated SHA; M1/checkpoint alone |
| Human release close | publish-track honesty **plus** documented M1 / surface gates when required | Treating every human-close gate as a publish-track blocker |

Failure handling for matrix or RC failures: let safe lanes finish, census
symptoms, group by root cause, fix with earlier regression coverage, freeze a
new SHA, and rerun the full gate once — never hide flakes with skips or
weakened assertions (`AGENTS.md`).

## Strategy map

| Layer | What it verifies | Tools / entrypoints |
| --- | --- | --- |
| Unit | Crate-local logic (parse, lower, storage helpers) | `cargo test` inline + crate `tests/` |
| Integration / facade | Lifecycle, verbs, reopen, concurrency contracts | `graphforge-api` workspace tests |
| Language compliance | openCypher semantics | `cargo test -p graphforge-core --test bdd` / `make test-tck` |
| Binding / IPC | Python & Node projections match Rust semantics | pytest, Node BDD, Arrow/IPC equality |
| Contract gates | Non-Cypher public surface inventory + evidence | `scripts/ci/non-cypher-surface-gate.py`, surface-gate workflows |
| Agent skills | Offline pack/install, compatibility, schema fail-closed | `pnpm test:agent-skills`, `pnpm smoke:agent-skills` |
| Scale posture | Fixed-hop `LIMIT` materialization bounds | `make bench-fixed-hop-limit` (shape gate; see scale-limits) |
| Policy / docs | Format, lint, license, docs build | `make pre-push`, `.github/workflows/docs.yml` |

## Behavior coverage

PR CI does **not** enforce full `llvm-cov` floors. Use `make coverage-rust`
locally (or when claiming floor changes). Default maintainer loop is
`make pre-push-fast`; run full `make coverage` / `make pre-push` when the changed
surface needs coverage honesty.

### Rust coverage evidence

`make coverage-rust` measures four explicit totals: core Rust, Python adapter
Rust, Node adapter Rust, and their merged workspace. The adapter totals come
from the functional native acceptance suites—not placeholder binding tests—and
therefore include persistence/reopen, structured lifecycle errors, parity, and
no-fallback behavior executed through the instrumented PyO3 and napi-rs
artifacts.

The run uses an isolated `CARGO_TARGET_DIR` (defaulting under its output tree),
builds each native artifact once, and verifies that the loaded artifact hash
matches the measured object.
`build/coverage-rust/ledger.json` also binds the evidence to `HEAD`, the current
`origin/main` merge base, and the LLVM toolchain. Missing, empty, malformed,
stale, wrong-artifact, or wrong-SHA evidence fails before totals are accepted.
Core has a 95% ratchet, every non-binding production crate has an independent
80% floor, and changed executable Rust lines have a 90% floor. Each Rust
binding adapter also retains its
independent 80% floor; neither the merged workspace percentage nor a strong
crate can average away a failed surface. Patch coverage uses executable lines
from the core LCOV report, so documentation, tests, blank lines, and non-Rust
changes do not manufacture measured production coverage. Core, per-crate, and
patch production totals exclude crate-level `tests/`, `benches/`, and
`examples/` sources plus executable lines inside `#[cfg(test)]`-gated Rust
items. The source scan is comment-, string-, and brace-aware and fails closed
when it cannot prove an item's boundary; native binding adapter totals remain
unfiltered because their functional runtime suites are the measured surface.

| Experience / Requirement | Scenario (Given/When/Then) | Test / evidence |
| ------------------------ | -------------------------- | ---- |
| FR-1 Cypher → Arrow | Given a graph, when `execute` runs, then Arrow rows match | Workspace/`graphforge-api` query tests; TCK corpus |
| FR-2 Analyst verbs | Given a graph, when a verb runs, then Arrow scores/rows return | `graphforge-api` analyst-verb/find surface tests; non-Cypher gate |
| FR-3 Project reopen | Given a published project, when reopened, then reads see published state | `cargo test -p graphforge-api --test public_lifecycle_conformance`; composite recovery suites |
| FR-4 Ontology modes | Given exploratory vs strict, when labels/violations occur, then accept or fail closed | Ontology round-trip / mode tests; agent bootstrap mode conflicts |
| FR-5 Layer isolation | Given knowledge by UUID, when Cypher runs, then graph-only baseline holds | Layer/boundary regression coverage |
| FR-6 Binding parity | Given the same op on Rust/Python/Node, when compared, then Arrow/IPC agrees | Binding RC / concurrency parity suites |
| FR-7 Fail closed formats | Given unsupported container, when opened, then no mutation | Project format compatibility tests |
| FR-8 Structured errors | Given writer-busy / capability gap, when called, then stable code | Facade + skills adapter error contracts |
| NFR-1 TCK | Given the authoritative corpus, when BDD runs, then runnable scenarios pass | `make test-tck` (3897 scenarios) |
| NFR-7 Surface inventory | Given public non-Cypher methods, when gate runs, then all classified | `tests/contracts/non-cypher-rust-surface.json` + gate script |

## Traceability contract

| Link | Evidence |
| --- | --- |
| Public behavior → architecture / ADR | [`ARCHITECTURE.md`](ARCHITECTURE.md), [`../adr/`](../adr/) |
| Behavior → BDD / scenario | TCK scenarios for Cypher; documented behavior tables for other surfaces |
| Scenario → test | Paths above; contract manifests under `tests/contracts/` |
| Public API → contract inventory | Versioned manifests under `tests/contracts/` and their gate scripts |

## Evaluation against product goals

- **Language correctness:** TCK green on the release lineage (full runnable
  denominator — not a local subset).
- **Surface completeness:** every public non-Cypher method classified with
  linked evidence; a green TCK run cannot substitute for the inventory.
- **Embedded invariants:** zero-config, local-first, portable project —
  re-proven in release close-out checklists, not only unit tests.
- **Agent usability:** skills and structured errors exercised in
  release-candidate scenarios when those gates are in scope.
- **Scale honesty:** fixed-hop LIMIT materialization shape gate; wall-clock
  reported but not treated as a cross-machine SLO
  ([`../reference/scale-limits.md`](../reference/scale-limits.md)).

## Running the tests

```bash
# Default maintainer loop (policy/format; ~30s)
make pre-push-fast

# Changed-surface validation
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
make test-tck

# Coverage-sensitive changes / floor claims (local; not PR CI)
make coverage-rust
# Full local gate when needed
make pre-push

# Non-Cypher surface (Rust)
python3 scripts/ci/non-cypher-surface-gate.py
python3 scripts/ci/test-non-cypher-surface-gate.py
cargo test -p graphforge-api \
  --test public_lifecycle_conformance \
  --test m22_m18_public_surface \
  --test m22_m19_public_surface

# Agent skills
pnpm test:agent-skills
pnpm smoke:agent-skills

# Docs (when editing published pages)
pnpm docs:build
pnpm docs:check-links
```

Targeted iteration may use crate filters (`cargo test -p graphforge-cypher`). Keep native
builds isolated with `CARGO_TARGET_DIR`; limit concurrent heavy builds
(`AGENTS.md`). Literal `graphforge-api` integration test binary names above are checked-in
identifiers; they are not product milestone labels.

## Continuous integration

| Workflow surface | Role |
| --- | --- |
| `.github/workflows/test.yml` (Test Suite + CI Gate) | Classified PR/`main` policy, Rust, bindings, concurrency short matrix (not full llvm-cov) |
| `.github/workflows/binding-release-candidate.yml` | Multi-OS Binding RC for publish-track and human close (exact SHA) |
| Non-Cypher / recovery / load gate workflows | Human-close / milestone publication evidence (not publish-track blockers) |
| `.github/workflows/docs.yml` | Starlight `pnpm docs:build` |
| `.github/workflows/publish.yaml` | publish-track and human publication path (retained Binding RC bytes; no rebuild) |

Merge requires green required checks and CI Gate at the exact head SHA.
publish-track and human-close workflows certify registry publication; they are
not close rituals for ordinary implementation issues. Details:
[`.github/workflows/README.md`](../../.github/workflows/README.md).

## Test data & environments

- Prefer hermetic temp project directories; no shared mutable fixtures across tests.
- Release-load and scale fixtures are generated through approved bulk publication APIs.
- TCK corpus and contract JSON manifests are checked in; do not silently shrink denominators.
- Skills smoke packs twice and requires identical SHA-256 hashes; offline `npm install` only.
