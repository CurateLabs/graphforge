# Testing

GraphForge proves shippable behavior with deterministic Rust and binding tests,
the openCypher TCK as the language oracle, and contract inventories for non-Cypher
surfaces. Correctness beats performance; skips, sleeps, retries-as-green, and
weakened assertions do not satisfy gates (`AGENTS.md`,
[`../development/testing.md`](../development/testing.md)).

This page is the **v0.5.0 / release-prep testing strategy** that shipped on
`main`: how layers compose, what each gate proves, and what does not count as
end-to-end evidence. Command recipes and historical suite layout live in
[`../development/testing.md`](../development/testing.md). Workflow mechanics live
in [`.github/workflows/README.md`](../../.github/workflows/README.md).

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

## Layered gates

Release readiness is a stack. Lower layers run on every applicable PR; higher
layers are SHA-bound release certification.

| Layer | When it runs | What green means |
| --- | --- | --- |
| Policy / docs | Every PR (docs path for site) | Workflows valid, license/domain policy hold; Starlight builds |
| Unit + workspace | Rust (or classified) changes | Crate logic and `cargo test --workspace` pass with Clippy `-D warnings` |
| Binding acceptance (PR) | Binding / classified changes | One same-SHA Linux Python wheel and Node addon; native contracts; short concurrency matrix |
| Language oracle | Workspace / TCK entrypoints | openCypher TCK runnable scenarios pass (currently **3897/3897**) |
| Binding release candidate | Manual, exact `main` SHA | Clean-install native proof on Linux, macOS, and Windows; fail-closed aggregate |
| Surface / recovery / load certification | Manual, exact SHA | Non-Cypher inventory + facade matrix, checkpoint recovery, XS–XL load ledger as publication evidence |
| Publication | Human release path | `publish.yaml` / registry artifacts for the certified SHA |

Ordinary implementation issues close on acceptance-criteria outcomes and green
checks for the **changed surface**. They do **not** require the full
release-certification cascade (Binding RC → surface aggregate → publish). Exact
SHA pairing and downloadable artifacts are publication evidence — see
`AGENTS.md` § Issue close.

### Pull-request contract (Test Suite + CI Gate)

- A deterministic classifier enables only the Rust, Python, Gherkin, binding, or
  agent-skills jobs that own the diff. Docs-only PRs do not compile native code.
- One required **CI Gate** aggregates applicable jobs: intentionally skipped
  lanes are fine; failed or cancelled applicable jobs are not.
- PR native binding acceptance is **Linux-only** and uses Cargo’s `dev` profile.
  That is fast feedback, not multi-OS certification.
- When Rust surfaces change, Test Suite also runs `Windows graphforge-storage Locks`
  (`cargo test -p graphforge-storage project_generation::tests:: --lib` on
  `blacksmith-4vcpu-windows-2025`) for the `#[cfg(windows)]` project-root lock
  unit tests that Linux `rust-test` cannot execute.
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
| Release publication | Exact SHA + same-SHA Binding RC / surface artifacts + publish path | Green PR CI on an unrelated SHA |

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
# Default maintainer loop
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
make test-tck
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
| `.github/workflows/test.yml` (Test Suite + CI Gate) | Classified PR/`main` policy, Rust, bindings, concurrency short matrix |
| `.github/workflows/binding-release-candidate.yml` | Manual multi-OS clean-install Binding RC for an exact SHA |
| Non-Cypher / recovery / load gate workflows | SHA-bound publication evidence (inventory, recovery, XS–XL load) |
| `.github/workflows/docs.yml` | Starlight `pnpm docs:build` |
| `.github/workflows/publish.yaml` | Human publication path |

Merge requires green required checks and CI Gate at the exact head SHA. Manual
SHA-bound release workflows certify publication; they are not close rituals for
ordinary implementation issues. Details:
[`.github/workflows/README.md`](../../.github/workflows/README.md).

## Test data & environments

- Prefer hermetic temp project directories; no shared mutable fixtures across tests.
- Release-load and scale fixtures are generated through approved bulk publication APIs.
- TCK corpus and contract JSON manifests are checked in; do not silently shrink denominators.
- Skills smoke packs twice and requires identical SHA-256 hashes; offline `npm install` only.
