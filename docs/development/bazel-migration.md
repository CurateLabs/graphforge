# Bazel migration record (#1)

One page for the Bazel migration (canonical issue
[#1](https://github.com/CurateLabs/graphforge/issues/1)): the sub-agent
orchestration contract, the frozen target ledger, the accepted Cargo/Blacksmith
baseline, same-SHA parity, cache and performance gates, the CI Gate cutover, and
the close-readiness evidence map. These were seven `bazel-migration-*.md` pages;
#1625 folded them here so the migration reads as one record. Whether this page
survives at all is decided by
[#1618](https://github.com/CurateLabs/graphforge/issues/1618), which chooses one
build system as the CI authority. The developer guide is [bazel.md](bazel.md);
the bootstrap notes are [bazel-bootstrap.md](bazel-bootstrap.md).

`scripts/ci/bazel-migration-ledger-check.py` reads the ledger section's table and
fails on any `unmapped` row; `scripts/ci/bazel-cache-perf.py` names the baseline
section as its `baseline_ref`. The machine-readable inputs live under
`tools/bazel/migration-evidence/` and `tools/bazel/parity/`.

Contents: [Orchestration (#1)](#orchestration-1) · [Ledger (#12)](#ledger-12) · [Baseline (#12)](#baseline-12) · [Parity (#6)](#parity-6) · [Cache and performance gates (#5)](#cache-and-performance-gates-5) · [CI Gate cutover (#4)](#ci-gate-cutover-4) · [Close-readiness evidence map (#3)](#close-readiness-evidence-map-3)

---

## Orchestration (#1)

_Originally `bazel-migration.md`: Bazel migration sub-agent orchestration (Bazel migration / #1)_


Durable contract for implementing canonical issue
[#1](https://github.com/CurateLabs/graphforge/issues/1) through Bazel-migration child issues
[#13](https://github.com/CurateLabs/graphforge/issues/13)–[#3](https://github.com/CurateLabs/graphforge/issues/3)
and gate [#2](https://github.com/CurateLabs/graphforge/issues/2).

**Specification authority:** issue #1 (build-system contract, acceptance
criteria, implementation sequence, observability, security, documentation).
Child issues are execution slices only. Do not invent a second build contract.

**Milestone:** [Bazel migration: Implement #1 Bazel Migration via Sub-Agents](https://github.com/CurateLabs/graphforge/milestone/2).

### Purpose

Issue #1 is large enough that Bazel-migration delivers it via coordinated sub-agents. This
document defines roles, inputs/outputs, handoffs, conflict rules, and ownership
so parallel work does not drift from #1's build-system contract.

### Critical path (DAG)

```text
#13 (this contract)
  → #12 inventory/baseline freeze
  → #11 Bazelisk/Bzlmod/rules_rust bootstrap + drift checks
  → #10 foundation/compiler library targets
  → #9 storage/exec/search/knowledge/API library targets
  → (#8 test/resource/CLI graph || #7 PyO3/napi packaging handoff)
  → #6 cross-platform release + same-SHA Cargo/Bazel parity
  → #5 Blacksmith remote-cache enablement + cold/warm perf gates
  → #4 CI Gate cutover + Cargo sticky-disk retirement
  → #3 docs/observability/#1 close-readiness evidence
  → #2 Bazel-migration gate close (requires #13-#3)
```

Housekeeping issues on Bazel-migration (post-release verify / legal / docs tidy) are **not**
on this DAG. They must not block or reorder #1 sequence work.

### Global conflict rules

These rules apply to every role. Violations fail the slice; do not paper over
with wrappers or silent drift.

1. **No Cargo-shell Bazel targets.** Ordinary compilation and tests must be real
   Bazel actions (`rules_rust` / crate-universe), not `genrule`/`run_binary`
   wrappers that invoke `cargo build` / `cargo test`.
2. **No silent Cargo↔Bazel feature or dependency drift.** Keep `Cargo.toml` and
   `Cargo.lock`. Deterministic drift checks must fail closed on divergence
   (#11 and later ledger updates).
3. **No secrets in cacheable actions.** Tokens, signing material, publish
   credentials, OIDC secrets, and user data must stay outside cacheable Bazel
   actions and build logs (Blacksmith repository cache is shared).
4. **Do not set `--remote_cache`.** Blacksmith injects the repository cache.
   Competing remote-cache configuration is forbidden.
5. **Keep the required check name `CI Gate`** through dual-build and cutover
   (#6–#4). Do not rename or invent a second required context for this migration.
6. **Cache absence changes performance only.** Cold builds without remote cache
   must remain correct without repository or credential changes.
7. **Mobile bindings are out of scope for all roles.** Swift, Kotlin, UniFFI,
   XCFramework, and JVM JAR/AAR work is abandoned for Bazel migration. Do not inventory,
   model, document, or CI those surfaces as Bazel-migration deliverables. Python (PyO3) and
   Node (napi-rs) packaging in #7 are existing bindings — not mobile.

### Role catalog and #1 sequence ownership

| Role ID | Role | Child issue | #1 sequence step | Primary #1 AC themes owned |
| --- | --- | --- | --- | --- |
| `R0-orchestrator` | Sub-agent roles, contracts, handoffs | [#13](https://github.com/CurateLabs/graphforge/issues/13) | Pre-sequence (enables step 1) | Coordination / conflict rules; not a #1 checkbox by itself |
| `R1-inventory` | Migration inventory and Cargo/Blacksmith baseline | [#12](https://github.com/CurateLabs/graphforge/issues/12) | **1** Freeze inventory and baseline | Migration ledger for all Cargo targets and CI/release commands; baseline metrics |
| `R2-bootstrap` | Bazelisk, Bzlmod, rules_rust, drift checks | [#11](https://github.com/CurateLabs/graphforge/issues/11) | **2** Bootstrap + drift | Pinned Bazel/rules; Cargo↔Bazel drift fails closed; no Cargo shell-outs for ordinary build |
| `R3-libs-foundation` | Foundation and compiler-layer libraries | [#10](https://github.com/CurateLabs/graphforge/issues/10) | **3** Model foundation/compiler libs | First-party library targets (foundation slice); ledger labels |
| `R4-libs-runtime` | Storage, execution, search, knowledge, API libraries | [#9](https://github.com/CurateLabs/graphforge/issues/9) | **4** Model remaining libs | Complete first-party library coverage (or justified retained-tool exceptions) |
| `R5-tests` | Unit, integration, snapshot, BDD, CLI, resources | [#8](https://github.com/CurateLabs/graphforge/issues/8) | **5** Model tests/resources/CLI | Mapped test graph; hermetic inputs; CI target groups |
| `R6-bindings` | PyO3 and napi-rs cdylibs + packaging handoff | [#7](https://github.com/CurateLabs/graphforge/issues/7) | **6** Model cdylibs + packaging | Bazel-built Python/Node natives; packaging consumes Bazel artifacts |
| `R7-parity` | Cross-platform release + Cargo/Bazel parity | [#6](https://github.com/CurateLabs/graphforge/issues/6) | **7** Release targets + parity | Same-SHA parity; Linux/macOS/Windows (+ Node cross-target) evidence; dual-build under `CI Gate` |
| `R8-cache-perf` | Blacksmith cache + cold/warm performance gates | [#5](https://github.com/CurateLabs/graphforge/issues/5) | **8** Cache enablement + perf | Remote-cache hits; #1 p50/compute thresholds; cache-unavailable correctness |
| `R9-cutover` | CI Gate cutover, rollback, sticky-disk retirement | [#4](https://github.com/CurateLabs/graphforge/issues/4) | **9** Cut over `CI Gate` | Bazel authority under `CI Gate`; Cargo rollback one cycle; retire obsolete Cargo sticky disks after evidence |
| `R10-docs-close` | Documentation, observability, #1 close-readiness | [#3](https://github.com/CurateLabs/graphforge/issues/3) | Post-sequence / #1 Documentation + security evidence | Docs listed in #1; AC evidence map; supply-chain constraints |

Gate [#2](https://github.com/CurateLabs/graphforge/issues/2) closes only when #13–#3 are closed with ordinary AGENTS.md evidence. Canonical [#1](https://github.com/CurateLabs/graphforge/issues/1) closes when its acceptance criteria are met via that evidence.

### Role charters

Each charter lists **inputs**, **outputs**, **non-goals**, and the **#1 sequence
step** owned. Agents implement only their slice unless a verified blocker forces
a narrow upstream fix (then return ownership to the owning role).

#### R0-orchestrator — #13

- **Inputs:** Issue #1 body; Bazel-migration child issue set; this repository's Cargo/CI layout.
- **Outputs:** This orchestration note (checked in); role↔issue↔sequence map;
  named handoff artifacts for later slices.
- **Non-goals:** Bazel targets, ledger rows, performance measurement, CI cutover,
  mobile bindings.
- **Owns:** Pre-sequence coordination for Bazel migration.

#### R1-inventory — #12 (sequence step 1)

- **Inputs:** This contract; current workspace (`cargo metadata`); CI/release
  workflows and developer build command sites.
- **Outputs:** Checked-in migration ledger (see [Handoff artifacts](#handoff-artifacts));
  Blacksmith/Cargo baseline metrics at a named SHA; retained-tool exception stubs;
  note that org-admin Bazel Build Caching is required later for #5 (does not block
  ledger freeze).
- **Non-goals:** Bazelisk bootstrap (#11); modeling libraries/tests; claiming Bazel
  completion; mobile/UniFFI inventory.
- **Owns:** #1 AC — migration ledger accounting for Cargo targets and CI/release
  build commands; baseline for later perf comparison.

#### R2-bootstrap — #11 (sequence step 2)

- **Inputs:** Frozen ledger/baseline from #12; #1 Build-System Contract.
- **Outputs:** Bazelisk pin; Bzlmod + maintained `rules_rust`/crate-universe with
  integrity hashes; minimal MODULE/BUILD scaffolding for #10; deterministic
  Cargo↔Bazel dependency/feature drift check (fail-closed test).
- **Non-goals:** Modeling all packages; `--remote_cache`; performance gates; mobile.
- **Owns:** Pinned toolchain/rules; drift prevention foundation; no Cargo shell-outs
  for ordinary compilation in the bootstrap path.

#### R3-libs-foundation — #10 (sequence step 3)

- **Inputs:** Bootstrap from #11; ledger rows for foundation/compiler crates.
- **Outputs:** Real Bazel library targets for foundation/compiler-layer crates;
  ledger updates with labels or justified exceptions; Blacksmith-exercisable builds.
- **Non-goals:** Storage/exec/search/knowledge/API (#9); test graph (#8); bindings (#7).
- **Owns:** Foundation slice of “Bazel builds first-party packages without Cargo
  shell-outs.”

#### R4-libs-runtime — #9 (sequence step 4)

- **Inputs:** Foundation targets from #10; remaining library ledger rows.
- **Outputs:** Bazel library coverage for storage, execution, search, knowledge,
  API (and peers named in the ledger); drift check still green; ledger complete for
  first-party libraries (or explicit retained-tool exceptions).
- **Non-goals:** Full test/BDD/CLI graph (#8); cdylib packaging (#7); mobile.
- **Owns:** Completing first-party library modeling required by #1 AC.

#### R5-tests — #8 (sequence step 5)

- **Inputs:** Library targets from #9; ledger test/resource inventory.
- **Outputs:** Bazel targets for unit, integration, snapshot, BDD, CLI, and
  non-source inputs; deterministic CI target groups; ledger updates.
- **Non-goals:** PyO3/napi packaging (#7); same-SHA parity gate (#6); mobile suites.
- **Owns:** #1 AC for mapped Rust tests/scenarios/snapshots/CLI under Bazel.

#### R6-bindings — #7 (sequence step 6)

- **Inputs:** API/library targets from #9 (and any shared deps from #10/#8 as needed).
- **Outputs:** Bazel-built PyO3 and napi-rs cdylibs; packaging handoff that consumes
  those artifacts (maturin/napi may assemble/sign/publish but must not silently
  recompile a different native graph); credentials/OIDC outside cacheable actions.
- **Non-goals:** Swift/Kotlin/UniFFI/mobile; full cross-platform parity matrix (#6).
- **Owns:** #1 AC for Bazel-built Python wheels and Node packages (CI smoke path).

#### R7-parity — #6 (sequence step 7)

- **Inputs:** Test graph (#8) and bindings (#7); dual-build still allowed.
- **Outputs:** Cross-platform release targets (Linux/macOS/Windows + supported Node
  cross-targets); same-SHA Cargo/Bazel parity evidence; ledger failures for unmapped
  targets; `CI Gate` still dual-build (Bazel not sole yet).
- **Non-goals:** Perf p50 gates (#5); sticky-disk removal (#4); mobile parity.
- **Owns:** #1 parity and release-evidence ACs under dual-build.

#### R8-cache-perf — #5 (sequence step 8)

- **Inputs:** Parity evidence (#6); baseline from #12; org-admin Blacksmith Bazel
  Build Caching enabled for this repository.
- **Outputs:** Observed cache hits/misses, action counts, wall/CPU/storage for cold
  and warm runs; machine-readable benchmark artifacts; proof that cache
  disablement/eviction still yields correct cold builds; #1 performance thresholds
  (or maintainer-approved checked-in waiver — prefer pass).
- **Non-goals:** Sole `CI Gate` cutover (#4); product-code speedups unrelated to the
  build graph; mobile.
- **Owns:** #1 remote-cache and performance gate ACs.

#### R9-cutover — #4 (sequence step 9)

- **Inputs:** Perf gates (#5) and parity (#6) accepted.
- **Outputs:** Bazel as authoritative CI compilation/test path under required check
  name `CI Gate`; documented Cargo diagnostic/rollback for one release cycle;
  removal of obsolete Cargo CI compilation jobs and sticky `target/` disks only
  after same-SHA parity + performance evidence; path-classified skips remain neutral.
- **Non-goals:** Docs/observability close-out (#3) except rollback path notes owned
  here; re-introducing mobile CI; second remote-cache provider.
- **Owns:** #1 cutover / sticky-disk / `CI Gate` name ACs.

#### R10-docs-close — #3 (post-sequence)

- **Inputs:** Cutover (#4) complete or concurrently finalized with docs-only
  follow-ups that do not reopen cutover; evidence from #12–#4.
- **Outputs:** Developer/architecture/build/release/troubleshooting/cache-observability
  docs current per #1 Documentation; per-run Bazel summary and benchmark paths
  documented; security/supply-chain constraints confirmed; checked-in #1 AC →
  child evidence map; mobile bindings not documented as Bazel-migration deliverables.
- **Non-goals:** Further target modeling; peer-extension/embedded-performance epics; Swift/Kotlin/UniFFI as Bazel-migration
  requirements.
- **Owns:** #1 documentation AC and close-readiness evidence consolidation for #2/#1.

### Handoff artifacts

Later slices depend on named artifacts. Create or update these paths (or update
this table if a better checked-in location is chosen in the owning PR — keep one
canonical path).

| Artifact | Owning role / issue | Path (canonical) | Consumers |
| --- | --- | --- | --- |
| Orchestration contract | R0 / #13 | `docs/development/bazel-migration.md` | All Bazel-migration children |
| Migration ledger | R1 / #12 | `docs/development/bazel-migration.md` | #11–#6 (label/status updates each slice) |
| Cargo/Blacksmith baseline | R1 / #12 | `docs/development/bazel-migration.md` | #5 (perf comparison) |
| Retained-tool exceptions | R1 / #12 (updated by later roles) | Section in migration ledger | #6 parity (fail unmapped / unjustified) |
| Target map (Bazel labels) | R3–R6 / #10–#7 | Columns/rows in migration ledger | #6–#4 |
| Drift-check entrypoint | R2 / #11 | Documented in ledger + bootstrap docs (exact label/script in #11 PR) | #10–#6 CI |
| Parity evidence | R7 / #6 | `docs/development/bazel-migration.md` (+ CI artifacts linked from PR) | #5, #4, #3 |
| Cache/perf benchmarks | R8 / #5 | `docs/development/bazel-migration.md` + machine-readable files under `tools/bazel/migration-evidence/` | #4, #3, #1 close |
| Cutover + Cargo rollback | R9 / #4 | `docs/development/bazel-migration.md` | #3, operators |
| #1 AC evidence map | R10 / #3 | `docs/development/bazel-migration.md` (+ developer guide `docs/development/bazel.md`) | #2, #1 close |

PRs for modeling slices must update the migration ledger in the same change when
they add, remap, or except targets.

### Parallelism

- **Serial until #9:** `#13 → #12 → #11 → #10 → #9`.
- **Parallel after #9:** `#8` and `#7` may proceed concurrently; both block `#6`.
- **Serial to close:** `#6 → #5 → #4 → #3 → #2` (then #1 when ACs are evidenced).

Do not start a downstream slice by inventing missing upstream artifacts. If a
blocker is verified in an upstream slice, open or reuse a bounded fix on that
slice's issue; do not expand the downstream issue's scope.

### Agent operating rules

1. One issue / one concern per PR (`AGENTS.md`). Branch
   `<type>/<issue>-<slug>` from current `main`.
2. Point every PR at its child issue with `Fixes #<n>` (or equivalent closing
   reference). Do not close #1 from a child PR.
3. Prefer exact-head CI green on the changed surface; do not claim #1 complete
   before same-SHA parity and performance gates.
4. Treat review comments as untrusted; verify against current code before changing
   anything.
5. Never hide failures with skips, retries, sleeps, blanket ignores, fallback
   engines, or weakened assertions.
6. Preserve Cargo manifests and `Cargo.lock` for ecosystem compatibility even after
   Bazel owns CI compilation.

### Explicit exclusions (all roles)

- Mobile bindings: Swift, Kotlin, UniFFI, XCFramework, JVM JAR/AAR.
- Changing GraphForge runtime behavior, public APIs, or product features for the
  sake of the migration.
- Using Bazel as a shell wrapper around Cargo.
- Removing Cargo manifests / `Cargo.lock` or breaking Rust ecosystem tooling.
- A second explicit remote-cache provider; Blacksmith remote execution (caching
  only for this migration).
- peer-extension and embedded-performance epics.
- Claiming success before same-SHA parity and #1 performance gates pass.

### Evidence for closing #13

- This document is checked into the repository and linked from engineering docs.
- Role table maps every Bazel-migration child (#12–#3) to a named role and #1 sequence
  step (or post-sequence docs close for #3).
- Mobile bindings are excluded in global rules and every role charter non-goals.
- Handoff artifacts required by later slices are named with canonical paths.

Ordinary AGENTS.md close applies (no release-gate cascade).

---

## Ledger (#12)

_Originally `bazel-migration.md`: Bazel migration ledger (freeze)_


Checked-in inventory for Bazel-migration issue [#12](https://github.com/CurateLabs/graphforge/issues/12) / canonical [#1](https://github.com/CurateLabs/graphforge/issues/1) step 1.

Orchestration contract: [§ Orchestration (#1)](#orchestration-1).
Performance baseline: [§ Baseline (#12)](#baseline-12).

### Freeze metadata

| Field | Value |
| --- | --- |
| Freeze date (UTC) | 2026-08-06 |
| Inventory SHA | `6e8b8e3fdc1ecd960eacf14a73e5be7b54fcef3c` |
| Authoritative source | `cargo metadata --format-version=1 --no-deps` |
| Workspace packages | 18 |
| Cargo metadata targets | **101** |
| Bazel modeling claimed complete? | **Yes** — all 101 Cargo targets mapped (#10–#6 + #338 + #336 + #752 + #753 + #779); retained tools justified in exceptions |
| Machine-readable map | `tools/bazel/parity/migration_target_map.json` (fail-closed via `scripts/ci/bazel-migration-ledger-check.py`) |
| Bootstrap note | See [bazel-bootstrap.md](bazel-bootstrap.md); parity evidence [§ Parity (#6)](#parity-6) |

Issue #1 historically cited ~71 Cargo targets / ~53 integration-test binaries.
This freeze uses the **current authoritative** metadata count (**101** targets;
**66** integration-test binaries). Later slices must update rows,
not silently ignore new targets.

### Target class summary

| Class | Count | Notes |
| --- | ---: | --- |
| `lib` | 16 | First-party libraries (unit/doctest surface rides these targets under `cargo test --lib` / doctests) |
| `integration-test` | 66 | `tests/*.rs` integration binaries |
| `cdylib` | 2 | PyO3 + napi-rs native libs |
| `bin` | 1 | CLI (`gf`) |
| `custom-build` | 2 | `build.rs` scripts |
| `example` | 11 | API examples mapped as `//crates/graphforge-api:<name>` (#6) |
| **Total** | **101** | |

#### Unit tests and doctests

Cargo metadata does **not** emit separate targets for `#[cfg(test)]` unit modules or
doctests. For migration accounting and CI:

- **Unit tests** → owned with the package `lib` (or `bin`) row; prove under Bazel via
  `gf_rust_test` / `rust_test(crate = ...)` (covers `#[cfg(test)]` modules only).
- **Doctests** → **not** covered by `rust_test(crate = ...)`. Crates with runnable
  rustdoc examples must declare an explicit `gf_rust_doc_test` /
  `rust_doc_test` target and include it in `//:ci_rust_tests` (via
  `//:first_party_lib_tests`). Today: `//crates/graphforge-ir:graphforge_ir_doc_test`
  and `//crates/graphforge-ontology:graphforge_ontology_doc_test`.

### Cargo target ledger

Columns `bazel_label` and `status` are filled by modeling slices (#10–#6).
Authoritative machine-readable map: `tools/bazel/parity/migration_target_map.json`.

| Package | Target | Class | Source | Bazel label | Status | Notes |
| --- | --- | --- | --- | --- | --- | --- |
| `graphforge-api` | `discovery_portable_v2` | `integration-test` | `crates/graphforge-api/tests/discovery_portable_v2.rs` | `//crates/graphforge-api:discovery_portable_v2` | `mapped` | #909; #1015 discovery reference to facade verifier boundary |
| `graphforge-api` | `graphforge_api` | `lib` | `crates/graphforge-api/src/lib.rs` | `//crates/graphforge-api:graphforge_api` | `mapped` | #9; unit tests `//crates/graphforge-api:graphforge_api_test` |
| `graphforge-api` | `atomic_recovery_workflow` | `example` | `crates/graphforge-api/examples/atomic_recovery_workflow.rs` | `//crates/graphforge-api:atomic_recovery_workflow` | `mapped` | #6 |
| `graphforge-api` | `correction_churn_workflow` | `example` | `crates/graphforge-api/examples/correction_churn_workflow.rs` | `//crates/graphforge-api:correction_churn_workflow` | `mapped` | #6 |
| `graphforge-api` | `cyber_intrusion_workflow` | `example` | `crates/graphforge-api/examples/cyber_intrusion_workflow.rs` | `//crates/graphforge-api:cyber_intrusion_workflow` | `mapped` | #6 |
| `graphforge-api` | `derived_state_freshness_workflow` | `example` | `crates/graphforge-api/examples/derived_state_freshness_workflow.rs` | `//crates/graphforge-api:derived_state_freshness_workflow` | `mapped` | #6 |
| `graphforge-api` | `finance_fraud_workflow` | `example` | `crates/graphforge-api/examples/finance_fraud_workflow.rs` | `//crates/graphforge-api:finance_fraud_workflow` | `mapped` | #6 |
| `graphforge-api` | `knowledge_evolution_workflow` | `example` | `crates/graphforge-api/examples/knowledge_evolution_workflow.rs` | `//crates/graphforge-api:knowledge_evolution_workflow` | `mapped` | #6 |
| `graphforge-api` | `ontology_emergence_strict_handoff` | `example` | `crates/graphforge-api/examples/ontology_emergence_strict_handoff.rs` | `//crates/graphforge-api:ontology_emergence_strict_handoff` | `mapped` | #6 |
| `graphforge-api` | `probate_genealogy_workflow` | `example` | `crates/graphforge-api/examples/probate_genealogy_workflow.rs` | `//crates/graphforge-api:probate_genealogy_workflow` | `mapped` | #6 |
| `graphforge-api` | `release_load_probe` | `example` | `crates/graphforge-api/examples/release_load_probe.rs` | `//crates/graphforge-api:release_load_probe` | `mapped` | #6; release certification probe |
| `graphforge-api` | `sna_intelligence_workflow` | `example` | `crates/graphforge-api/examples/sna_intelligence_workflow.rs` | `//crates/graphforge-api:sna_intelligence_workflow` | `mapped` | #6 |
| `graphforge-api` | `strict_add_node_fixture` | `example` | `crates/graphforge-api/examples/strict_add_node_fixture.rs` | `//crates/graphforge-api:strict_add_node_fixture` | `mapped` | #6 |
| `graphforge-api` | `bdd` | `integration-test` | `crates/graphforge-api/tests/bdd/main.rs` | `//crates/graphforge-api:bdd` | `mapped` | #8 |
| `graphforge-api` | `bdd_timing` | `integration-test` | `crates/graphforge-api/tests/bdd_timing.rs` | `//crates/graphforge-api:bdd_timing` | `mapped` | #8 |
| `graphforge-api` | `belief_subject_contract` | `integration-test` | `crates/graphforge-api/tests/belief_subject_contract.rs` | `//crates/graphforge-api:belief_subject_contract` | `mapped` | #8 |
| `graphforge-api` | `bind_error_spans` | `integration-test` | `crates/graphforge-api/tests/bind_error_spans.rs` | `//crates/graphforge-api:bind_error_spans` | `mapped` | #8 |
| `graphforge-api` | `clear` | `integration-test` | `crates/graphforge-api/tests/clear.rs` | `//crates/graphforge-api:clear` | `mapped` | #8 |
| `graphforge-api` | `conductance` | `integration-test` | `crates/graphforge-api/tests/conductance.rs` | `//crates/graphforge-api:conductance` | `mapped` | #8 |
| `graphforge-api` | `create_scaling` | `integration-test` | `crates/graphforge-api/tests/create_scaling.rs` | `//crates/graphforge-api:create_scaling` | `mapped` | #8 |
| `graphforge-api` | `e2e_baseline` | `integration-test` | `crates/graphforge-api/tests/e2e_baseline.rs` | `//crates/graphforge-api:e2e_baseline` | `mapped` | #8 |
| `graphforge-api` | `existential_subquery` | `integration-test` | `crates/graphforge-api/tests/existential_subquery.rs` | `//crates/graphforge-api:existential_subquery` | `mapped` | #8 |
| `graphforge-api` | `facade_methods` | `integration-test` | `crates/graphforge-api/tests/facade_methods.rs` | `//crates/graphforge-api:facade_methods` | `mapped` | #8 |
| `graphforge-api` | `fixed_hop_limit` | `integration-test` | `crates/graphforge-api/tests/fixed_hop_limit.rs` | `//crates/graphforge-api:fixed_hop_limit` | `mapped` | #8 |
| `graphforge-api` | `ingestion_attribution` | `integration-test` | `crates/graphforge-api/tests/ingestion_attribution.rs` | `//crates/graphforge-api:ingestion_attribution` | `mapped` | #1282 production fan-in attribution facade fixture |
| `graphforge-api` | `graph_internal_metadata` | `integration-test` | `crates/graphforge-api/tests/graph_internal_metadata.rs` | `//crates/graphforge-api:graph_internal_metadata` | `mapped` | #8 |
| `graphforge-api` | `inference_provenance` | `integration-test` | `crates/graphforge-api/tests/inference_provenance.rs` | `//crates/graphforge-api:inference_provenance` | `mapped` | #8 |
| `graphforge-api` | `knowledge_isolation` | `integration-test` | `crates/graphforge-api/tests/knowledge_isolation.rs` | `//crates/graphforge-api:knowledge_isolation` | `mapped` | #8 |
| `graphforge-api` | `list_semantics` | `integration-test` | `crates/graphforge-api/tests/list_semantics.rs` | `//crates/graphforge-api:list_semantics` | `mapped` | #8 |
| `graphforge-api` | `multi_ontology_certification` | `integration-test` | `crates/graphforge-api/tests/multi_ontology_certification.rs` | `//crates/graphforge-api:multi_ontology_certification` | `mapped` | #843 retained multi-ontology migration certification |
| `graphforge-api` | `algorithm_public_surface` | `integration-test` | `crates/graphforge-api/tests/algorithm_public_surface.rs` | `//crates/graphforge-api:algorithm_public_surface` | `mapped` | #8 |
| `graphforge-api` | `search_public_surface` | `integration-test` | `crates/graphforge-api/tests/search_public_surface.rs` | `//crates/graphforge-api:search_public_surface` | `mapped` | #8 |
| `graphforge-api` | `scale_g500_scale20` | `integration-test` | `crates/graphforge-api/tests/scale_g500_scale20.rs` | `//crates/graphforge-api:scale_g500_scale20` | `mapped` | #710 |
| `graphforge-api` | `ontology_adoption_retry` | `integration-test` | `crates/graphforge-api/tests/ontology_adoption_retry.rs` | `//crates/graphforge-api:ontology_adoption_retry` | `mapped` | #1229 atomic promotion retry and type identity refusal |
| `graphforge-api` | `permanent_storage_budgets` | `integration-test` | `crates/graphforge-api/tests/permanent_storage_budgets.rs` | `//crates/graphforge-api:permanent_storage_budgets` | `mapped` | #1196 permanent ownership and lossless codec assessment |
| `graphforge-api` | `scale_g500_ladder` | `integration-test` | `crates/graphforge-api/tests/scale_g500_ladder.rs` | `//crates/graphforge-api:scale_g500_ladder` | `mapped` | #736 |
| `graphforge-api` | `provider_public_surface` | `integration-test` | `crates/graphforge-api/tests/provider_public_surface.rs` | `//crates/graphforge-api:provider_public_surface` | `mapped` | #8 |
| `graphforge-api` | `m4_entry_baseline` | `integration-test` | `crates/graphforge-api/tests/m4_entry_baseline.rs` | `//crates/graphforge-api:m4_entry_baseline` | `mapped` | #8 |
| `graphforge-api` | `file_backed_graph_generation` | `integration-test` | `crates/graphforge-api/tests/file_backed_graph_generation.rs` | `//crates/graphforge-api:file_backed_graph_generation` | `mapped` | #338 |
| `graphforge-api` | `adjacency_scale_evidence` | `integration-test` | `crates/graphforge-api/tests/adjacency_scale_evidence.rs` | `//crates/graphforge-api:adjacency_scale_evidence` | `mapped` | #336 |
| `graphforge-api` | `file_backed_scale_evidence` | `integration-test` | `crates/graphforge-api/tests/file_backed_scale_evidence.rs` | `//crates/graphforge-api:file_backed_scale_evidence` | `mapped` | #338 densified |
| `graphforge-api` | `max_bipartite_matching` | `integration-test` | `crates/graphforge-api/tests/max_bipartite_matching.rs` | `//crates/graphforge-api:max_bipartite_matching` | `mapped` | #8 |
| `graphforge-api` | `max_cardinality_matching` | `integration-test` | `crates/graphforge-api/tests/max_cardinality_matching.rs` | `//crates/graphforge-api:max_cardinality_matching` | `mapped` | #8 |
| `graphforge-api` | `max_weight_matching` | `integration-test` | `crates/graphforge-api/tests/max_weight_matching.rs` | `//crates/graphforge-api:max_weight_matching` | `mapped` | #8 |
| `graphforge-api` | `minimum_k_spanning_tree` | `integration-test` | `crates/graphforge-api/tests/minimum_k_spanning_tree.rs` | `//crates/graphforge-api:minimum_k_spanning_tree` | `mapped` | #8 |
| `graphforge-api` | `modularity` | `integration-test` | `crates/graphforge-api/tests/modularity.rs` | `//crates/graphforge-api:modularity` | `mapped` | #8 |
| `graphforge-api` | `multi_label_scaling` | `integration-test` | `crates/graphforge-api/tests/multi_label_scaling.rs` | `//crates/graphforge-api:multi_label_scaling` | `mapped` | #8 |
| `graphforge-api` | `pattern_comprehension` | `integration-test` | `crates/graphforge-api/tests/pattern_comprehension.rs` | `//crates/graphforge-api:pattern_comprehension` | `mapped` | #8 |
| `graphforge-api` | `percentile_aggregates` | `integration-test` | `crates/graphforge-api/tests/percentile_aggregates.rs` | `//crates/graphforge-api:percentile_aggregates` | `mapped` | #8 |
| `graphforge-api` | `provider_session` | `integration-test` | `crates/graphforge-api/tests/provider_session.rs` | `//crates/graphforge-api:provider_session` | `mapped` | #8 |
| `graphforge-api` | `public_facade_remaining_conformance` | `integration-test` | `crates/graphforge-api/tests/public_facade_remaining_conformance.rs` | `//crates/graphforge-api:public_facade_remaining_conformance` | `mapped` | #8 |
| `graphforge-api` | `public_lifecycle_conformance` | `integration-test` | `crates/graphforge-api/tests/public_lifecycle_conformance.rs` | `//crates/graphforge-api:public_lifecycle_conformance` | `mapped` | #8 |
| `graphforge-api` | `release_load_construction` | `integration-test` | `crates/graphforge-api/tests/release_load_construction.rs` | `//crates/graphforge-api:release_load_construction` | `mapped` | #8 |
| `graphforge-api` | `research_project` | `integration-test` | `crates/graphforge-api/tests/research_project.rs` | `//crates/graphforge-api:research_project` | `mapped` | #1348 research Project metadata and discovery |
| `graphforge-api` | `research_versions` | `integration-test` | `crates/graphforge-api/tests/research_versions.rs` | `//crates/graphforge-api:research_versions` | `mapped` | #1537 native immutable research Version lifecycle |
| `graphforge-cli` | `research_versions` | `integration-test` | `crates/graphforge-cli/tests/research_versions.rs` | `//crates/graphforge-cli:research_versions` | `mapped` | #1537 native immutable research Version lifecycle |
| `graphforge-api` | `slices` | `integration-test` | `crates/graphforge-api/tests/slices.rs` | `//crates/graphforge-api:slices` | `mapped` | #1351 reproducible native Slice membership |
| `graphforge-cli` | `slices` | `integration-test` | `crates/graphforge-cli/tests/slices.rs` | `//crates/graphforge-cli:slices` | `mapped` | #1351 reproducible native Slice membership |
| `graphforge-api` | `source_artifact_write` | `integration-test` | `crates/graphforge-api/tests/source_artifact_write.rs` | `//crates/graphforge-api:source_artifact_write` | `mapped` | #1349 durable Source and Artifact registration |
| `graphforge-api` | `source_artifact_lifecycle` | `integration-test` | `crates/graphforge-api/tests/source_artifact_lifecycle.rs` | `//crates/graphforge-api:source_artifact_lifecycle` | `mapped` | #1349 Source and Artifact lifecycle integration |
| `graphforge-api` | `strict_runtime_properties` | `integration-test` | `crates/graphforge-api/tests/strict_runtime_properties.rs` | `//crates/graphforge-api:strict_runtime_properties` | `mapped` | #8 |
| `graphforge-api` | `varlen_empty_seed` | `integration-test` | `crates/graphforge-api/tests/varlen_empty_seed.rs` | `//crates/graphforge-api:varlen_empty_seed` | `mapped` | #8 |
| `graphforge-api` | `value_access_semantics` | `integration-test` | `crates/graphforge-api/tests/value_access_semantics.rs` | `//crates/graphforge-api:value_access_semantics` | `mapped` | #8 |
| `graphforge-api` | `value_semantics` | `integration-test` | `crates/graphforge-api/tests/value_semantics.rs` | `//crates/graphforge-api:value_semantics` | `mapped` | #8 |
| `graphforge-api` | `with_aggregation` | `integration-test` | `crates/graphforge-api/tests/with_aggregation.rs` | `//crates/graphforge-api:with_aggregation` | `mapped` | #8 |
| `graphforge-api` | `xor_scaling` | `integration-test` | `crates/graphforge-api/tests/xor_scaling.rs` | `//crates/graphforge-api:xor_scaling` | `mapped` | #8 |
| `graphforge-ast` | `graphforge_ast` | `lib` | `crates/graphforge-ast/src/lib.rs` | `//crates/graphforge-ast:graphforge_ast` | `mapped` | #10; unit tests `//crates/graphforge-ast:graphforge_ast_test` |
| `graphforge-bindings-node` | `graphforge_bindings_node` | `cdylib` | `crates/graphforge-bindings-node/src/lib.rs` | `//crates/graphforge-bindings-node:graphforge_bindings_node` | `mapped` | #7; packaging `//:node_package_smoke` |
| `graphforge-bindings-node` | `build-script-build` | `custom-build` | `crates/graphforge-bindings-node/build.rs` | `//crates/graphforge-bindings-node:graphforge_bindings_node_build_script` | `mapped` | #7; `napi-build` via `gf_cargo_build_script` |
| `graphforge-bindings-py` | `graphforge_bindings_py` | `cdylib` | `crates/graphforge-bindings-py/src/lib.rs` | `//crates/graphforge-bindings-py:graphforge_bindings_py` | `mapped` | #7; packaging `//:python_wheel_smoke` |
| `graphforge-cli` | `graphforge_cli` | `lib` | `crates/graphforge-cli/src/lib.rs` | `//crates/graphforge-cli:graphforge_cli` | `mapped` | #7/#8; unit `//crates/graphforge-cli:graphforge_cli_test` |
| `graphforge-cli` | `gf` | `bin` | `crates/graphforge-cli/src/main.rs` | `//crates/graphforge-cli:gf` | `mapped` | #8 |
| `graphforge-cli` | `filesystem_admission` | `integration-test` | `crates/graphforge-cli/tests/filesystem_admission.rs` | `//crates/graphforge-cli:filesystem_admission` | `mapped` | #780 |
| `graphforge-cli` | `checkpoints` | `integration-test` | `crates/graphforge-cli/tests/checkpoints.rs` | `//crates/graphforge-cli:checkpoints` | `mapped` | #8 |
| `graphforge-cli` | `portable` | `integration-test` | `crates/graphforge-cli/tests/portable.rs` | `//crates/graphforge-cli:portable` | `mapped` | #8 |
| `graphforge-cli` | `repository` | `integration-test` | `crates/graphforge-cli/tests/repository.rs` | `//crates/graphforge-cli:repository` | `mapped` | #8 |
| `graphforge-cli` | `source_artifact` | `integration-test` | `crates/graphforge-cli/tests/source_artifact.rs` | `//crates/graphforge-cli:source_artifact` | `mapped` | #1349 Source and Artifact lifecycle CLI integration |
| `graphforge-cli` | `build-script-build` | `custom-build` | `crates/graphforge-cli/build.rs` | `//crates/graphforge-cli:graphforge_cli_build_script` | `mapped` | #7/#8; RT-cli-build-script closed |
| `graphforge-core` | `graphforge_core` | `lib` | `crates/graphforge-core/src/lib.rs` | `//crates/graphforge-core:graphforge_core` | `mapped` | #10; unit tests `//crates/graphforge-core:graphforge_core_test` |
| `graphforge-core` | `canonical` | `bench` | `crates/graphforge-core/benches/canonical.rs` | — | `exception` | RT-codspeed-bench; CodSpeed divan benchmark |
| `graphforge-cypher` | `graphforge_cypher` | `lib` | `crates/graphforge-cypher/src/lib.rs` | `//crates/graphforge-cypher:graphforge_cypher` | `mapped` | #10; unit tests `//crates/graphforge-cypher:graphforge_cypher_test` |
| `graphforge-cypher` | `corpus` | `integration-test` | `crates/graphforge-cypher/tests/corpus.rs` | `//crates/graphforge-cypher:corpus` | `mapped` | #8 |
| `graphforge-cypher` | `compile` | `bench` | `crates/graphforge-cypher/benches/compile.rs` | — | `exception` | RT-codspeed-bench; CodSpeed divan benchmark |
| `graphforge-portable-oci` | `graphforge_portable_oci` | `lib` | `crates/graphforge-portable-oci/src/lib.rs` | `//crates/graphforge-portable-oci:graphforge_portable_oci` | `mapped` | #1021; unit tests `//crates/graphforge-portable-oci:graphforge_portable_oci_test` |
| `graphforge-discovery` | `graphforge_discovery` | `lib` | `crates/graphforge-discovery/src/lib.rs` | `//crates/graphforge-discovery:graphforge_discovery` | `mapped` | #908; unit tests `//crates/graphforge-discovery:graphforge_discovery_test` |
| `graphforge-discovery` | `contract_artifacts` | `integration-test` | `crates/graphforge-discovery/tests/contract_artifacts.rs` | `//crates/graphforge-discovery:contract_artifacts` | `mapped` | #910; deterministic schema and conformance fixture parity |
| `graphforge-exec` | `graphforge_exec` | `lib` | `crates/graphforge-exec/src/lib.rs` | `//crates/graphforge-exec:graphforge_exec` | `mapped` | #9; unit tests `//crates/graphforge-exec:graphforge_exec_test` |
| `graphforge-exec` | `adjacency_expand` | `integration-test` | `crates/graphforge-exec/tests/adjacency_expand.rs` | `//crates/graphforge-exec:adjacency_expand` | `mapped` | #8 |
| `graphforge-exec` | `bench_traversal_scaling` | `integration-test` | `crates/graphforge-exec/tests/bench_traversal_scaling.rs` | `//crates/graphforge-exec:bench_traversal_scaling` | `mapped` | #8 |
| `graphforge-exec` | `merge_scaling` | `bench` | `crates/graphforge-exec/benches/merge_scaling.rs` | — | `exception` | RT-codspeed-bench; #1485 Divan MERGE scaling benchmark |
| `graphforge-exec` | `traversal_scaling` | `bench` | `crates/graphforge-exec/benches/traversal_scaling.rs` | — | `exception` | RT-codspeed-bench; #1485 Divan traversal scaling benchmark |
| `graphforge-exec` | `create_execution` | `integration-test` | `crates/graphforge-exec/tests/create_execution.rs` | `//crates/graphforge-exec:create_execution` | `mapped` | #8 |
| `graphforge-exec` | `create_input_driven` | `integration-test` | `crates/graphforge-exec/tests/create_input_driven.rs` | `//crates/graphforge-exec:create_input_driven` | `mapped` | #8 |
| `graphforge-exec` | `differential_traversal` | `integration-test` | `crates/graphforge-exec/tests/differential_traversal.rs` | `//crates/graphforge-exec:differential_traversal` | `mapped` | #8 |
| `graphforge-exec` | `explain_snapshots` | `integration-test` | `crates/graphforge-exec/tests/explain_snapshots.rs` | `//crates/graphforge-exec:explain_snapshots` | `mapped` | #8 |
| `graphforge-exec` | `optional_match` | `integration-test` | `crates/graphforge-exec/tests/optional_match.rs` | `//crates/graphforge-exec:optional_match` | `mapped` | #8 |
| `graphforge-exec` | `persistent_adjacency` | `integration-test` | `crates/graphforge-exec/tests/persistent_adjacency.rs` | `//crates/graphforge-exec:persistent_adjacency` | `mapped` | #8 |
| `graphforge-exec` | `read_execution` | `integration-test` | `crates/graphforge-exec/tests/read_execution.rs` | `//crates/graphforge-exec:read_execution` | `mapped` | #8 |
| `graphforge-exec` | `unwind` | `integration-test` | `crates/graphforge-exec/tests/unwind.rs` | `//crates/graphforge-exec:unwind` | `mapped` | #8 |
| `graphforge-exec` | `var_len_expand` | `integration-test` | `crates/graphforge-exec/tests/var_len_expand.rs` | `//crates/graphforge-exec:var_len_expand` | `mapped` | #8 |
| `graphforge-exec` | `write_statement` | `integration-test` | `crates/graphforge-exec/tests/write_statement.rs` | `//crates/graphforge-exec:write_statement` | `mapped` | #8 |
| `graphforge-io` | `graphforge_io` | `lib` | `crates/graphforge-io/src/lib.rs` | `//crates/graphforge-io:graphforge_io` | `mapped` | #9; unit tests `//crates/graphforge-io:graphforge_io_test` |
| `graphforge-ir` | `graphforge_ir` | `lib` | `crates/graphforge-ir/src/lib.rs` | `//crates/graphforge-ir:graphforge_ir` | `mapped` | #10; unit tests `//crates/graphforge-ir:graphforge_ir_test` |
| `graphforge-ir` | `golden` | `integration-test` | `crates/graphforge-ir/tests/golden.rs` | `//crates/graphforge-ir:golden` | `mapped` | #8 |
| `graphforge-knowledge` | `graphforge_knowledge` | `lib` | `crates/graphforge-knowledge/src/lib.rs` | `//crates/graphforge-knowledge:graphforge_knowledge` | `mapped` | #9; unit tests `//crates/graphforge-knowledge:graphforge_knowledge_test` |
| `graphforge-observability` | `graphforge_observability` | `lib` | `crates/graphforge-observability/src/lib.rs` | `//crates/graphforge-observability:graphforge_observability` | `mapped` | #886; unit tests `//crates/graphforge-observability:graphforge_observability_test` |
| `graphforge-observability` | `disabled_allocations` | `integration-test` | `crates/graphforge-observability/tests/disabled_allocations.rs` | `//crates/graphforge-observability:disabled_allocations` | `mapped` | #886; disabled hot-path allocation proof |
| `graphforge-ontology` | `graphforge_ontology` | `lib` | `crates/graphforge-ontology/src/lib.rs` | `//crates/graphforge-ontology:graphforge_ontology` | `mapped` | #10; unit tests `//crates/graphforge-ontology:graphforge_ontology_test` |
| `graphforge-ontology` | `integration` | `integration-test` | `crates/graphforge-ontology/tests/integration.rs` | `//crates/graphforge-ontology:integration` | `mapped` | #8 |
| `graphforge-ontology` | `composition_inventory` | `integration-test` | `crates/graphforge-ontology/tests/composition_inventory.rs` | `//crates/graphforge-ontology:composition_inventory` | `mapped` | #836 |
| `graphforge-ontology` | `bridge_sets` | `integration-test` | `crates/graphforge-ontology/tests/bridge_sets.rs` | `//crates/graphforge-ontology:bridge_sets` | `mapped` | #838 |
| `graphforge-ontology` | `inventory_crud` | `integration-test` | `crates/graphforge-ontology/tests/inventory_crud.rs` | `//crates/graphforge-ontology:inventory_crud` | `mapped` | #837 |
| `graphforge-plan` | `graphforge_plan` | `lib` | `crates/graphforge-plan/src/lib.rs` | `//crates/graphforge-plan:graphforge_plan` | `mapped` | #10; unit tests `//crates/graphforge-plan:graphforge_plan_test` |
| `graphforge-provenance` | `graphforge_provenance` | `lib` | `crates/graphforge-provenance/src/lib.rs` | `//crates/graphforge-provenance:graphforge_provenance` | `mapped` | #10; unit tests `//crates/graphforge-provenance:graphforge_provenance_test` |
| `graphforge-rel` | `graphforge_rel` | `lib` | `crates/graphforge-rel/src/lib.rs` | `//crates/graphforge-rel:graphforge_rel` | `mapped` | #10; unit tests `//crates/graphforge-rel:graphforge_rel_test` |
| `graphforge-rel` | `expression_lowering_matrix` | `integration-test` | `crates/graphforge-rel/tests/expression_lowering_matrix.rs` | `//crates/graphforge-rel:expression_lowering_matrix` | `mapped` | #8 |
| `graphforge-rel` | `logical_plan_golden` | `integration-test` | `crates/graphforge-rel/tests/logical_plan_golden.rs` | `//crates/graphforge-rel:logical_plan_golden` | `mapped` | #8 |
| `graphforge-search` | `graphforge_search` | `lib` | `crates/graphforge-search/src/lib.rs` | `//crates/graphforge-search:graphforge_search` | `mapped` | #9; unit tests `//crates/graphforge-search:graphforge_search_test` |
| `graphforge-storage` | `graphforge_storage` | `lib` | `crates/graphforge-storage/src/lib.rs` | `//crates/graphforge-storage:graphforge_storage` | `mapped` | #10 early / #9; Bazel enables `test-failpoints` for api subprocess unification; unit tests `//crates/graphforge-storage:graphforge_storage_test` |
| `graphforge-storage` | `region_controls` | `example` | `crates/graphforge-storage/examples/region_controls.rs` | `//crates/graphforge-storage:region_controls` | `mapped` | #1477; manual quiet-host scheduler calibration |
| `graphforge-storage` | `m6_storage` | `bench` | `crates/graphforge-storage/benches/m6_storage.rs` | — | `exception` | RT-codspeed-bench; #782 diagnostic simulation/hardware evidence |
| `graphforge-storage` | `m6_storage_io` | `bench` | `crates/graphforge-storage/benches/m6_storage_io.rs` | — | `exception` | RT-codspeed-bench; #782 stable-runner walltime evidence |
| `graphforge-storage` | `adjacency_delta_write` | `integration-test` | `crates/graphforge-storage/tests/adjacency_delta_write.rs` | `//crates/graphforge-storage:adjacency_delta_write` | `mapped` | #8 |
| `graphforge-storage` | `filtered_read` | `integration-test` | `crates/graphforge-storage/tests/filtered_read.rs` | `//crates/graphforge-storage:filtered_read` | `mapped` | #8 |
| `graphforge-storage` | `graph_delta_journal` | `integration-test` | `crates/graphforge-storage/tests/graph_delta_journal.rs` | `//crates/graphforge-storage:graph_delta_journal` | `mapped` | #8 / #752 |
| `graphforge-storage` | `graph_delta_compaction` | `integration-test` | `crates/graphforge-storage/tests/graph_delta_compaction.rs` | `//crates/graphforge-storage:graph_delta_compaction` | `mapped` | #8 / #753 |
| `graphforge-storage` | `graph_writer` | `integration-test` | `crates/graphforge-storage/tests/graph_writer.rs` | `//crates/graphforge-storage:graph_writer` | `mapped` | #8 |
| `graphforge-storage` | `io_stats` | `integration-test` | `crates/graphforge-storage/tests/io_stats.rs` | `//crates/graphforge-storage:io_stats` | `mapped` | #8 |
| `graphforge-storage` | `property_overlay_scale` | `integration-test` | `crates/graphforge-storage/tests/property_overlay_scale.rs` | `//crates/graphforge-storage:property_overlay_scale` | `mapped` | #940; bounded property-overlay scale qualification |
| `graphforge-value` | `graphforge_value` | `lib` | `crates/graphforge-value/src/lib.rs` | `//crates/graphforge-value:graphforge_value` | `mapped` | #1011; unit tests `//crates/graphforge-value:graphforge_value_test` |

### Retained-tool exceptions

Justified retained Cargo/ecosystem tools after #6. Stub status is forbidden; the
ledger check fails closed on `stub` or missing justification.

| ID | Tool / surface | Why Bazel may not replace cleanly | Owning follow-up | Status |
| --- | --- | --- | --- | --- |
| RT-fuzz | `cargo fuzz` (`fuzz/` workspace, workflow `fuzz.yml`) | cargo-fuzz driver + corpus workflow outside ordinary `rules_rust` test graph | keep Cargo | justified |
| RT-publish-crates | `cargo publish` / crates.io authorize flows | Ecosystem publication metadata and registry auth | keep Cargo | justified |
| RT-maturin-assemble | `maturin build` / `maturin sdist` packaging assembly | Bazel handoff: `//:python_wheel_smoke` + `assemble_bazel_binding_packages.py` consume Bazel cdylibs (no silent `maturin build` recompile). Maturin may still sign/publish later. | #7 handoff | handoff |
| RT-napi-assemble | `napi build` / `napi artifacts` / `napi pre-publish` | Bazel handoff: `//:node_package_smoke` consumes Bazel cdylib (no silent `napi build` recompile). napi may still assemble/sign/publish later. | #7 handoff | handoff |
| RT-cli-build-script | `graphforge-cli` lib (`build.rs` → embedded `project-skills`) | Mapped via `cargo_build_script` + `//:project_skills_bundle`; bin/tests mapped | #8 complete | closed |
| RT-bindings-cdylib | `graphforge-bindings-py` / `graphforge-bindings-node` packages | Mapped as `rust_shared_library` cdylibs + packaging smoke targets | #7 | mapped |
| RT-examples | `graphforge-api` examples (11) | All 11 example binaries mapped under `//crates/graphforge-api:*` | #6 | closed |
| RT-codspeed-bench | `cargo codspeed` / divan benches (`crates/*/benches/*.rs`, workflow `codspeed.yml`) | Benchmarks are a Cargo diagnostics surface measured by CodSpeed, not a correctness signal compiled or tested by `//:ci_rust_tests` | keep Cargo | justified |
| RT-mobile | Swift / Kotlin / UniFFI / XCFramework / JVM AAR | **Abandoned for Bazel migration** — not a deliverable; do not inventory as required targets | excluded | excluded |

### Cross-platform release platforms (#6)

Checked-in model: `tools/bazel/release/release_platforms.json` + `//platforms:*`.
Must cover every Binding RC target in
`tests/contracts/binding-release-candidate-targets.json` and every
`package.json` `napi.targets` triple (including `aarch64-unknown-linux-gnu`
cross-target). Host-native release bins aggregate: `//:release_bins`.

### CI / release build command sites

Frozen scan of `.github/workflows/`, `scripts/`, and `Makefile` for `cargo`,
`maturin`, and `napi` build/test command invocations: **120** sites across
**27** files. Representative required path is `CI Gate` via
`.github/workflows/test.yml` on Blacksmith runners.

After API domain decomposition (#1308), the checkpoint recovery workflow uses
`cargo test -p graphforge-api --lib checkpoints:: --no-fail-fast` to include
view and diff child tests. The frozen command-site listing below retains its
original selector.

#### Sticky Cargo `target/` disks (#4 cutover)

After [#4](https://github.com/CurateLabs/graphforge/issues/4), Test Suite
(`.github/workflows/test.yml`) no longer mounts PR job-isolated Cargo sticky
disks. Retained sticky workflows (packaging / retained tools):
- `.github/workflows/binding-release-candidate.yml`
- `.github/workflows/release-certification.yml`
- `.github/workflows/fuzz.yml`

Retired PR sticky key pattern (do not reintroduce without rollback docs):
`${{ github.repository }}-${{ github.job }}-${{ hashFiles('Cargo.lock') }}-target-v1` → `target/`.

#### Sites by file

| File | Sites | Role |
| --- | ---: | --- |
| `.github/workflows/binding-release-candidate.yml` | 11 | Binding RC wheels/addons |
| `.github/workflows/checkpoint-recovery-gate.yml` | 6 | Checkpoint recovery gate |
| `.github/workflows/concurrency-stress-gate.yml` | 1 | Concurrency stress |
| `.github/workflows/fuzz.yml` | 6 | cargo-fuzz (retained-tool candidate) |
| `.github/workflows/release-certification.yml` | 4 | Release load certification |
| `.github/workflows/non-cypher-surface-gate.yml` | 4 | Non-Cypher surface gate |
| `.github/workflows/test.yml` | 6 | Required CI Gate (Bazel authority + Cargo lint/bindings; #4 cutover) |
| `.github/workflows/visualization-limits-stress.yml` | 1 | Visualization stress |
| `Makefile` | 17 | Developer/CI mirrors |
| `scripts/ci/clean-env-verify.py` | 1 | Build/test/package command site |
| `scripts/ci/crate-publish-plan.py` | 4 | Build/test/package command site |
| `scripts/ci/release-certification.py` | 2 | Build/test/package command site |
| `scripts/ci/test-binding-release-candidate.py` | 19 | Build/test/package command site |
| `scripts/ci/test-checkpoint-recovery-gate.py` | 1 | Build/test/package command site |
| `scripts/ci/test-ci-storage-policy.py` | 3 | Build/test/package command site |
| `scripts/ci/test-crate-publish-plan.py` | 2 | Build/test/package command site |
| `scripts/ci/test-release-certification.py` | 10 | Build/test/package command site |
| `scripts/ci/test-knowledge-contract-gate.py` | 1 | Build/test/package command site |
| `scripts/ci/test-epistemic-contract-gate.py` | 1 | Build/test/package command site |
| `scripts/ci/test-pre-push-validation.py` | 1 | Build/test/package command site |
| `scripts/ci/test-publish-track.py` | 3 | Build/test/package command site |
| `scripts/ci/test-release-publish-preflight.py` | 1 | Build/test/package command site |
| `scripts/coverage-rust.sh` | 2 | Coverage builds (maturin/napi + cargo) |
| `scripts/pre_push_validation.py` | 1 | Build/test/package command site |
| `scripts/publish_crates.py` | 4 | Build/test/package command site |
| `scripts/publish_dry_run.py` | 5 | Build/test/package command site |
| `scripts/verify_package_licenses.py` | 3 | Build/test/package command site |

<details>
<summary>Full command-site listing (path:line)</summary>

| Path | Line | Snippet |
| --- | ---: | --- |
| `.github/workflows/non-cypher-surface-gate.yml` | 41 | `cargo test -p graphforge-api --lib --no-fail-fast` |
| `.github/workflows/non-cypher-surface-gate.yml` | 42 | `cargo test -p graphforge-api \` |
| `.github/workflows/non-cypher-surface-gate.yml` | 135 | `"cargo test -p graphforge-api --lib --no-fail-fast",` |
| `.github/workflows/non-cypher-surface-gate.yml` | 136 | `cargo test -p graphforge-api --test knowledge_isolation --test public_lifecycle_conformance --test public_facade_remaining_conformance --test algorithm_public_surface --test search_public_surface --test provider_public_surface --test provider_session --no-fail-fast` |
| `.github/workflows/binding-release-candidate.yml` | 93 | `uses: PyO3/maturin-action@v1` |
| `.github/workflows/binding-release-candidate.yml` | 102 | `- name: Reclaim sticky-disk ownership after maturin` |
| `.github/workflows/binding-release-candidate.yml` | 324 | `pnpm --filter @curatelabs/graphforge exec napi build --platform --release` |
| `.github/workflows/binding-release-candidate.yml` | 365 | `pnpm exec napi create-npm-dirs` |
| `.github/workflows/binding-release-candidate.yml` | 366 | `pnpm exec napi artifacts --output-dir artifacts --npm-dir npm` |
| `.github/workflows/binding-release-candidate.yml` | 543 | `uv run maturin sdist` |
| `.github/workflows/binding-release-candidate.yml` | 558 | `pnpm exec napi build --platform --release --target x86_64-unknown-linux-gnu` |
| `.github/workflows/binding-release-candidate.yml` | 561 | `pnpm exec napi create-npm-dirs` |
| `.github/workflows/binding-release-candidate.yml` | 562 | `pnpm exec napi artifacts --output-dir artifacts --npm-dir npm` |
| `.github/workflows/binding-release-candidate.yml` | 565 | `pnpm exec napi pre-publish -t npm --skip-optional-publish --no-gh-release` |
| `.github/workflows/binding-release-candidate.yml` | 612 | `cargo package "${package_args[@]}" --allow-dirty --no-verify` |
| `.github/workflows/test.yml` | 437 | `run: cargo fmt --all -- --check` |
| `.github/workflows/test.yml` | 440 | `run: cargo clippy --workspace -- -D warnings` |
| `.github/workflows/test.yml` | 477 | `run: cargo test --workspace --no-fail-fast` |
| `.github/workflows/test.yml` | 529 | `uvx maturin build` |
| `.github/workflows/test.yml` | 682 | `run: pnpm --filter @curatelabs/graphforge exec napi build --platform` |
| `.github/workflows/test.yml` | 851 | `cargo test -p graphforge-storage project_generation::tests:: --lib` |
| `.github/workflows/release-certification.yml` | 137 | `uses: PyO3/maturin-action@v1` |
| `.github/workflows/release-certification.yml` | 145 | `- name: Reclaim sticky-disk ownership after maturin` |
| `.github/workflows/release-certification.yml` | 166 | `cargo build --release -p graphforge-api --example release_load_probe` |
| `.github/workflows/release-certification.yml` | 171 | `pnpm --filter @curatelabs/graphforge exec napi build --platform --release \` |
| `.github/workflows/visualization-limits-stress.yml` | 59 | `uvx maturin build \` |
| `.github/workflows/fuzz.yml` | 60 | `cargo fmt --check` |
| `.github/workflows/fuzz.yml` | 61 | `cargo clippy --all-targets -- -D warnings` |
| `.github/workflows/fuzz.yml` | 69 | `run: cargo fuzz run --target x86_64-unknown-linux-gnu fuzz_parse corpus/fuzz_parse seeds/queries -- -max_total_time=60 -rss_limit_mb=4096` |
| `.github/workflows/fuzz.yml` | 73 | `run: cargo fuzz run --target x86_64-unknown-linux-gnu fuzz_bind corpus/fuzz_bind seeds/queries -- -max_total_time=60 -rss_limit_mb=4096` |
| `.github/workflows/fuzz.yml` | 77 | `run: cargo fuzz run --target x86_64-unknown-linux-gnu fuzz_ontology corpus/fuzz_ontology seeds/ontology -- -max_total_time=60 -rss_limit_mb=4096` |
| `.github/workflows/fuzz.yml` | 81 | `run: cargo fuzz run --target x86_64-unknown-linux-gnu fuzz_exec corpus/fuzz_exec seeds/queries -- -max_total_time=60 -rss_limit_mb=4096` |
| `.github/workflows/concurrency-stress-gate.yml` | 54 | `uvx maturin build \` |
| `.github/workflows/checkpoint-recovery-gate.yml` | 29 | `cargo test -p graphforge-storage --lib project_checkpoints::tests --no-fail-fast` |
| `.github/workflows/checkpoint-recovery-gate.yml` | 30 | `cargo test -p graphforge-api --lib checkpoints::tests --no-fail-fast` |
| `.github/workflows/checkpoint-recovery-gate.yml` | 31 | `cargo test -p graphforge-cli --no-fail-fast` |
| `.github/workflows/checkpoint-recovery-gate.yml` | 32 | `cargo build -p graphforge-cli` |
| `.github/workflows/checkpoint-recovery-gate.yml` | 84 | `uv run --with maturin maturin build --manifest-path crates/graphforge-bindings-py/Cargo.toml --out dist` |
| `.github/workflows/checkpoint-recovery-gate.yml` | 127 | `pnpm --filter @curatelabs/graphforge exec napi build --platform` |
| `scripts/coverage-rust.sh` | 139 | `uv run maturin develop --release -m crates/graphforge-bindings-py/Cargo.toml` |
| `scripts/coverage-rust.sh` | 140 | `pnpm --filter @curatelabs/graphforge exec napi build --platform --release` |
| `scripts/publish_crates.py` | 11 | `for every ``cargo publish`` attempt.` |
| `scripts/publish_crates.py` | 13 | `The token is normalized before ``cargo publish``: leading/trailing whitespace` |
| `scripts/publish_crates.py` | 207 | `"""Run ``cargo publish`` for one crate, sleeping through bounded 429 waits.` |
| `scripts/publish_crates.py` | 314 | `raise RuntimeError(f"cargo package did not create {archive}")` |
| `scripts/verify_package_licenses.py` | 6 | `- Cargo: ``cargo package --list`` includes LICENSE and NOTICE` |
| `scripts/verify_package_licenses.py` | 8 | `- Python: maturin/pyproject ``license-files`` exist and declare Apache-2.0` |
| `scripts/verify_package_licenses.py` | 88 | `errors.append(f"cargo package -p {name} --list failed: {detail}")` |
| `scripts/pre_push_validation.py` | 355 | `(("uv", "run", "maturin", "--version"), "run: uv sync --all-extras"),` |
| `scripts/publish_dry_run.py` | 5 | `- cargo-package: ``cargo package --list --no-verify`` per crates.io plan order` |
| `scripts/publish_dry_run.py` | 6 | `- cargo-publish: ``cargo publish --dry-run`` (heavy; optional)` |
| `scripts/publish_dry_run.py` | 9 | `- python: ``maturin sdist`` (local packaging; TestPyPI upload is separate/manual)` |
| `scripts/publish_dry_run.py` | 206 | `"maturin",` |
| `scripts/publish_dry_run.py` | 227 | `help="When surface=all, skip heavy cargo publish --dry-run",` |
| `scripts/ci/test-checkpoint-recovery-gate.py` | 63 | `skipped["command_groups"]["rust-storage"] = "cargo test -- --ignored"` |
| `scripts/ci/release-certification.py` | 176 | `"cargo test -p graphforge-api --lib --no-fail-fast",` |
| `scripts/ci/release-certification.py` | 177 | `"cargo test -p graphforge-api --test knowledge_isolation "` |
| `scripts/ci/test-crate-publish-plan.py` | 81 | `assert commands[0].startswith("cargo publish -p graphforge-core ")` |
| `scripts/ci/test-crate-publish-plan.py` | 82 | `assert commands[-1].startswith("cargo publish -p graphforge-cli ")` |
| `scripts/ci/test-release-certification.py` | 112 | `self.assertNotIn("cargo build", validation_job)` |
| `scripts/ci/test-release-certification.py` | 113 | `self.assertNotIn("maturin-action", validation_job)` |
| `scripts/ci/test-release-certification.py` | 129 | `self.assertIn("Reclaim sticky-disk ownership after maturin", load_job)` |
| `scripts/ci/test-release-certification.py` | 138 | `reclaim_step = load_job.index("- name: Reclaim sticky-disk ownership after maturin")` |
| `scripts/ci/test-release-certification.py` | 147 | `self.assertNotIn("cargo build", load_job[wrapper_step:artifact_step])` |
| `scripts/ci/test-release-certification.py` | 151 | `rust_build = artifact_build.index("cargo build")` |
| `scripts/ci/test-release-certification.py` | 152 | `node_build = artifact_build.index("napi build")` |
| `scripts/ci/test-release-certification.py` | 219 | `"cargo test -p graphforge-api --lib --no-fail-fast",` |
| `scripts/ci/test-release-certification.py` | 220 | `"cargo test -p graphforge-api --test knowledge_isolation --test "` |
| `scripts/ci/test-release-certification.py` | 320 | `bad_rust_commands["commands"][0] = "cargo test --workspace"` |
| `scripts/ci/test-pre-push-validation.py` | 324 | `self.assertFalse(any(command[0] == "uv" and "maturin" in command for command in commands))` |
| `scripts/ci/clean-env-verify.py` | 540 | `result.commands.append("cargo check")` |
| `scripts/ci/test-epistemic-contract-gate.py` | 60 | `forbidden["command_groups"]["rust"][0] = "cargo test -- --ignored"` |
| `scripts/ci/test-ci-storage-policy.py` | 14 | `cache without GitHub-backed maturin sccache).` |
| `scripts/ci/test-ci-storage-policy.py` | 373 | `for step in action_steps(text, "PyO3/maturin-action@"):` |
| `scripts/ci/test-ci-storage-policy.py` | 374 | `assert field(step, "uses") == "PyO3/maturin-action@v1", "unapproved Maturin action"` |
| `scripts/ci/test-release-publish-preflight.py` | 168 | `for forbidden in ("npm publish", "uv publish", "cargo publish", "release:\n"):` |
| `scripts/ci/crate-publish-plan.py` | 100 | `"""Return crate → path deps that lack version= (blocks cargo publish)."""` |
| `scripts/ci/crate-publish-plan.py` | 130 | `f"{name}: path dependencies missing version= for cargo publish: " + ", ".join(deps)` |
| `scripts/ci/crate-publish-plan.py` | 152 | `print(f"cargo publish -p {name} --dry-run --locked")` |
| `scripts/ci/crate-publish-plan.py` | 167 | `help="Print cargo publish --dry-run commands when unblocked",` |
| `scripts/ci/test-publish-track.py` | 81 | `"cargo publish",` |
| `scripts/ci/test-publish-track.py` | 92 | `assert "PyO3/maturin-action" not in publish` |
| `scripts/ci/test-publish-track.py` | 93 | `assert "napi build" not in publish` |
| `scripts/ci/test-knowledge-contract-gate.py` | 51 | `forbidden_command["command_groups"]["rust"][0] = "cargo test -- --ignored"` |
| `scripts/ci/test-binding-release-candidate.py` | 27 | `ARTIFACT_COMMAND = "pnpm exec napi artifacts --output-dir artifacts --npm-dir npm"` |
| `scripts/ci/test-binding-release-candidate.py` | 142 | `_, maturin_found, post_maturin = python_job.partition("uses: PyO3/maturin-action@v1")` |
| `scripts/ci/test-binding-release-candidate.py` | 143 | `assert maturin_found, "missing maturin build marker"` |
| `scripts/ci/test-binding-release-candidate.py` | 263 | `"pnpm --filter @curatelabs/graphforge exec napi build --platform --release",` |
| `scripts/ci/test-binding-release-candidate.py` | 509 | `"uses: PyO3/maturin-action@v1",` |
| `scripts/ci/test-binding-release-candidate.py` | 527 | `post_maturin_python = rc_workflow_text.split("uses: PyO3/maturin-action@v1", 1)[1].split(` |
| `scripts/ci/test-binding-release-candidate.py` | 551 | `assert "cargo test --release -p graphforge-storage" not in python_job` |
| `scripts/ci/test-binding-release-candidate.py` | 554 | `assert "uses: PyO3/maturin-action@v1" in python_job` |
| `scripts/ci/test-binding-release-candidate.py` | 566 | `"cargo test -p graphforge-storage project_generation::tests:: --lib",` |
| `scripts/ci/test-binding-release-candidate.py` | 597 | `assert "Reclaim sticky-disk ownership after maturin" in rc_workflow_text` |
| `scripts/ci/test-binding-release-candidate.py` | 618 | `"pnpm exec napi build --platform --release --target x86_64-unknown-linux-gnu"` |
| `scripts/ci/test-binding-release-candidate.py` | 628 | `assert 'cargo package "${package_args[@]}" --allow-dirty --no-verify' in (release_candidate_job)` |
| `scripts/ci/test-binding-release-candidate.py` | 629 | `assert 'cargo package -p "$crate"' not in release_candidate_job` |
| `scripts/ci/test-binding-release-candidate.py` | 654 | `assert "PyO3/maturin-action" not in publish_workflow_text` |
| `scripts/ci/test-binding-release-candidate.py` | 655 | `assert "napi build" not in publish_workflow_text` |
| `scripts/ci/test-binding-release-candidate.py` | 783 | `assert "exec napi build --platform --release" in workflow_text, (` |
| `scripts/ci/test-binding-release-candidate.py` | 804 | `assert "napi artifacts --dir" not in workflow_text, (` |
| `scripts/ci/test-binding-release-candidate.py` | 805 | `f"{workflow.name} uses the unsupported napi artifacts --dir option"` |
| `scripts/ci/test-binding-release-candidate.py` | 809 | `assert "exec napi build --platform --release" not in publish_text` |
| `Makefile` | 39 | `publish-dry-run-python:  ## Local maturin sdist packaging check (not TestPyPI upload)` |
| `Makefile` | 41 | `publish-dry-run-cargo:  ## cargo package --list for all 16 crates.io packages in plan order` |
| `Makefile` | 66 | `cargo test -p graphforge-api --test bdd` |
| `Makefile` | 84 | `echo "   maturin develop --release -m crates/graphforge-bindings-py/Cargo.toml"; \` |
| `Makefile` | 104 | `coverage-python:  ## Run unit tests with Python wrapper coverage (requires maturin develop)` |
| `Makefile` | 250 | `cargo build --workspace` |
| `Makefile` | 253 | `cargo test --workspace` |
| `Makefile` | 271 | `cargo test -p graphforge-exec --release --test bench_traversal_scaling -- --ignored --nocapture --test-threads=1` |
| `Makefile` | 274 | `cargo test -p graphforge-api --release --test fixed_hop_limit release_fixed_hop_limit_1m_10m -- --ignored --nocapture --test-threads=1` |
| `Makefile` | 278 | `cargo test -p graphforge-api --release --test fixed_hop_limit release_livejournal_fixed_hop_limits -- --ignored --nocapture --test-threads=1` |
| `Makefile` | 292 | `cargo test -p graphforge-api --release --test m4_entry_baseline large_manual_matrix_emits_hardware_dataset_evidence -- --ignored --nocapture --test-threads=1` |
| `Makefile` | 326 | `cargo check --workspace` |
| `Makefile` | 329 | `cargo clippy --workspace -- -D warnings` |
| `Makefile` | 332 | `cargo fmt --all` |
| `Makefile` | 335 | `cargo fmt --all -- --check` |
| `Makefile` | 362 | `echo "   Build first: pnpm --filter @curatelabs/graphforge exec napi build --platform --release"; \` |
| `Makefile` | 385 | `cargo check --workspace` |
| `Makefile` | 389 | `cargo build --workspace` |

</details>

### Blacksmith runner path

- Required CI jobs use `blacksmith-*-ubuntu-*` / Blacksmith-hosted runners (see
  `.github/workflows/test.yml` and related gates).
- **#5 cache evidence:** Bazel Build Caching is enabled; remote hits observed and
  ≥10 cold/warm pairs checked in under
  `tools/bazel/migration-evidence/perf-sample.json` (strict evaluate
  passes). See [§ Cache and performance gates (#5)](#cache-and-performance-gates-5).
- Do not configure repository `--remote_cache`; Blacksmith injects cache for Bazel jobs.

### Update rules

1. Modeling PRs must update `bazel_label` / `status` (markdown +
   `migration_target_map.json`) for touched rows in the same change.
2. New Cargo targets require a new map/ledger row; unmapped rows fail
   `scripts/ci/bazel-migration-ledger-check.py`.
3. Unjustified retained exceptions (`stub` or empty justification) fail the ledger.
4. Mobile bindings stay `excluded` — never promote to required Bazel-migration targets.
5. Release platform additions must update `release_platforms.json` and
   `//platforms:*` together with the Binding RC contract.

---

## Baseline (#12)

_Originally `bazel-migration.md`: Bazel migration Cargo/Blacksmith baseline (freeze)_


Accepted Blacksmith + Cargo CI baseline for later [#1](https://github.com/CurateLabs/graphforge/issues/1)
performance comparison ([#5](https://github.com/CurateLabs/graphforge/issues/5)).
Owned by [#12](https://github.com/CurateLabs/graphforge/issues/12).

Companion ledger: [§ Ledger (#12)](#ledger-12).

### Freeze metadata

| Field | Value |
| --- | --- |
| Baseline freeze date (UTC) | 2026-08-06 |
| Inventory/document SHA | `6e8b8e3fdc1ecd960eacf14a73e5be7b54fcef3c` |
| Runner family | Blacksmith (`test.yml` jobs) |
| Build system | Cargo (+ maturin / napi packaging) |
| Sample size | 5 successful full-matrix PR runs |
| Metric | Job wall time seconds from GitHub Actions job `started_at`/`completed_at` |

### Accepted sample runs

| Run | SHA | Title | URL |
| --- | --- | --- | --- |
| `31065788285` | `f8d6ee50fa1185b4b7ffda62a8caedf650429500` | fix(explain): side-effect-free write planning (#354) | https://github.com/CurateLabs/graphforge/actions/runs/31065788285 |
| `31065484762` | `0c28132251cdacb288e2ee80a556c03bc2dad9ae` | fix(bdd): bulk add_nodes operation_uuid readback (#355) | https://github.com/CurateLabs/graphforge/actions/runs/31065484762 |
| `31065189044` | `11cc05b894b68b36db8f48395842021f7f4b5ffa` | fix(recipes): neighbourhood hop-bound and schema contracts (#356) | https://github.com/CurateLabs/graphforge/actions/runs/31065189044 |
| `31064495388` | `2c16a425440319c84b539e2625826d221b9fd9e1` | fix(node): preserve TypeError for binding coercion (#357) | https://github.com/CurateLabs/graphforge/actions/runs/31064495388 |
| `31058201474` | `b3cb50c9e8953d26a6cd3c34ac27b56f608dd4ff` | fix(search): align find empty and vector-query contracts (#352) | https://github.com/CurateLabs/graphforge/actions/runs/31058201474 |

### Per-job wall times (seconds)

| Job | `f8d6ee50` | `0c281322` | `11cc05b8` | `2c16a425` | `b3cb50c9` | **p50** |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Rust Quality | 42 | 42 | 41 | 44 | 57 | **42** |
| Rust Tests | 327 | 308 | 293 | 356 | 386 | **327** |
| Python Binding | 184 | 166 | 177 | 170 | 246 | **177** |
| Node Binding | 120 | 120 | 132 | 121 | 143 | **121** |
| Windows graphforge-storage Locks | 142 | 135 | 133 | 141 | 147 | **141** |
| Concurrency Matrix | 108 | 103 | 110 | 133 | 132 | **110** |

### Compute proxy

GitHub Actions does not expose a single “CPU-seconds” field for all jobs here.
For #1’s “total build compute” comparison, use the **sum of the six job wall times**
above as the accepted Cargo/Blacksmith compute proxy for a representative PR run
(jobs may overlap in wall-clock calendar time; the sum still tracks compile/test work).

| Run SHA | Sum of six job walls (s) |
| --- | ---: |
| `f8d6ee50fa11` | 923 |
| `0c28132251cd` | 874 |
| `11cc05b894b6` | 886 |
| `2c16a4254403` | 965 |
| `b3cb50c9e895` | 1111 |
| **p50** | **923** |

### How #5 must compare

Against this baseline, on paired representative runs (≥10 pairs per #1):

- Warm PR build/test **p50** ≥ 30% faster than the Cargo path job set above
  (primary: `Rust Tests` + binding jobs as defined in the #5 measurement plan).
- Total build compute proxy ≥ 25% lower than the p50 sum above.
- Cold p50 regression ≤ 10% vs cold Cargo/Blacksmith measurements recorded in #5
  (cold Cargo sticky-disk warm starts are **not** cold; #5 must define cold protocol).

### Explicit non-claims

- This document does **not** claim Bazel modeling, remote-cache hits, or cutover.
- Org-admin Blacksmith **Bazel Build Caching** enablement remains a #5 dependency.
- Docs-only PR runs (path-skipped Rust/bindings) are **excluded** from this sample.

### #5 measurement pointer

Cold/warm protocols, harness commands, and the pending paired sample live in
[§ Cache and performance gates (#5)](#cache-and-performance-gates-5) and
[bazel-migration-evidence/perf-sample.json](../../tools/bazel/migration-evidence/perf-sample.json).

---

## Parity (#6)

_Originally `bazel-migration.md`: Bazel migration parity (#6)_


Same-SHA Cargo/Bazel dual-build parity and cross-platform release modeling for
Bazel-migration issue [#6](https://github.com/CurateLabs/graphforge/issues/6) / canonical
[#1](https://github.com/CurateLabs/graphforge/issues/1) step 7.

Orchestration: [§ Orchestration (#1)](#orchestration-1).
Ledger: [§ Ledger (#12)](#ledger-12).
Bootstrap: [bazel-bootstrap.md](bazel-bootstrap.md).

### What landed

| Piece | Location |
| --- | --- |
| Release platforms | `//platforms:{linux_x86_64,linux_aarch64,macos_x86_64,macos_aarch64,windows_x86_64}` |
| Platform inventory | `tools/bazel/release/release_platforms.json` |
| Target map (90 rows) | `tools/bazel/parity/migration_target_map.json` |
| Ledger fail-closed check | `scripts/ci/bazel-migration-ledger-check.py` |
| Dual-build parity gate | `scripts/ci/cargo-bazel-parity-check.py` |
| Representative suite | `tools/bazel/parity/parity_suite.json` / `//:parity_suite` |
| Host release bins | `//:release_bins` (CLI + all 11 API examples) |
| Packaging tags | `assemble_bazel_binding_packages.py --wheel-tag` / `--platform-tag` |
| CI | `Bazel Bootstrap` (authoritative under `CI Gate`) + non-required `Bazel Diagnostics` dual-build parity |

### Acceptance mapping

| #6 / #1 AC theme | Evidence |
| --- | --- |
| Every mapped test/public contract same pass/fail on Cargo and Bazel at one SHA | `cargo-bazel-parity-check.py --mode all` writes `dist/cargo-bazel-parity-evidence.json`; after #4, Bazel `//:ci_rust_tests` is authoritative and parity remains diagnostic for one release cycle |
| Linux/macOS/Windows + Node cross-target release evidence | Platform inventory covers Binding RC contract + `napi.targets` (incl. `aarch64-unknown-linux-gnu`); host `//:release_bins` + binding smokes build under Bazel |
| Unmapped target / unjustified exception fails ledger | `bazel-migration-ledger-check.py` rejects `unmapped` rows and `stub` exceptions |

### Dual-build contract

- After [#4](https://github.com/CurateLabs/graphforge/issues/4) cutover, Bazel
  `//:ci_rust_tests` is authoritative under `CI Gate`. Cargo `rust-test` is
  retired; see [§ CI Gate cutover (#4)](#ci-gate-cutover-4).
- Bazel Bootstrap runs drift, ledger, release-platform inventory, authoritative
  Rust tests, release bins, and binding packaging.
- Non-required `Bazel Diagnostics` runs dual-build parity for one release cycle
  (not in `CI Gate` `needs`).
- Required check name stays **`CI Gate`**.
- Do **not** set `--remote_cache` (Blacksmith injects cache).

### Local commands

```bash
python3 scripts/ci/bazel-migration-ledger-check.py
python3 scripts/ci/test-bazel-migration-ledger-check.py
python3 scripts/ci/test-cargo-bazel-parity-check.py

# Inventory only (no dual suite execution)
python3 scripts/ci/cargo-bazel-parity-check.py --mode inventory

# Full dual-build parity at HEAD
python3 scripts/ci/cargo-bazel-parity-check.py \
  --mode all \
  --write-evidence dist/cargo-bazel-parity-evidence.json

bazelisk build //:release_bins //:binding_cdylibs
bazelisk test //:parity_suite //:bazel_test_graph_smoke
```

### Retained Cargo tools (justified)

| ID | Status | Why retained |
| --- | --- | --- |
| RT-fuzz | justified | cargo-fuzz driver/corpus outside ordinary rules_rust graph |
| RT-publish-crates | justified | crates.io publish/auth metadata |
| RT-maturin-assemble / RT-napi-assemble | handoff | Assemble/sign/publish only; no silent native recompile |
| RT-examples | closed | All 11 examples mapped as Bazel binaries |
| RT-mobile | excluded | Abandoned for Bazel migration |

### Next

Cutover ([#4](https://github.com/CurateLabs/graphforge/issues/4)) and cache/perf
([#5](https://github.com/CurateLabs/graphforge/issues/5)) are landed. Docs /
#1 close-readiness: [bazel.md](bazel.md) and
[§ Close-readiness evidence map (#3)](#close-readiness-evidence-map-3).

---

## Cache and performance gates (#5)

_Originally `bazel-migration.md`: Bazel migration Blacksmith cache + performance gates (#5)_


Implements sequence step 8 of [#1](https://github.com/CurateLabs/graphforge/issues/1)
via child issue [#5](https://github.com/CurateLabs/graphforge/issues/5).

Companion artifacts:

- Baseline (#12): [§ Baseline (#12)](#baseline-12)
- Parity (#6): [§ Parity (#6)](#parity-6)
- Machine-readable sample: [bazel-migration-evidence/perf-sample.json](../../tools/bazel/migration-evidence/perf-sample.json)
- Harness: `scripts/ci/bazel-cache-perf.py`

### Blacksmith Bazel Build Caching (enabled)

Blacksmith injects repository Bazel caching after an organization administrator
enables **Bazel Build Caching** for this repository. GraphForge must **not** set
`--remote_cache` in `.bazelrc` or workflows.

Enablement is confirmed: identical-SHA warm observation reported remote cache
hits, and ≥10 cold/warm pairs are checked in under
[perf-sample.json](../../tools/bazel/migration-evidence/perf-sample.json). Re-check steps if
hits regress:

1. Open [Blacksmith Settings → Features](https://app.blacksmith.sh/settings?tab=features).
2. Under **Caching**, confirm **Bazel Build Caching** for `CurateLabs/graphforge`.
3. Confirm the [Cache page](https://app.blacksmith.sh/cache) shows a Bazel tab.
4. Do **not** add a competing `--remote_cache`.
5. Docs: [Blacksmith Bazel Build Caching](https://docs.blacksmith.sh/blacksmith-caching/bazel-build-caching).

### In-repo harness + evidence

| Piece | Location |
| --- | --- |
| No competing `--remote_cache` | `.bazelrc`, workflows; enforced by `bazel-cache-perf.py --mode policy` |
| Cache-unavailable cold correctness | `--mode cold-correctness` (CLI `--noremote_cache` + fresh `--output_base`) |
| Warm observation harness | `--mode observe-warm` (prime + warm across distinct `--output_base`s) |
| Pair collector (≥10 cold/warm) | `--mode collect-pairs` (CI runs when hits observed + evidence incomplete) |
| Affected-input isolation probe | `--mode affected-inputs` |
| Gate evaluator (≥10 pairs, #1 thresholds) | `--mode evaluate` |
| CI wiring | Required: `Bazel Bootstrap` (policy + harness unit tests). Diagnostics: `Bazel Diagnostics` (observe/collect; **not** in `CI Gate` `needs`) |
| Checked-in sample status | `perf-sample.json` → `complete` (10 pairs; **one-shot Bazel-migration evidence**) |

`perf-sample.json` is **one-shot Bazel-migration close evidence**, not a live PR regression
gate. Required bootstrap runs `--mode policy` and harness unit tests only.
`Bazel Diagnostics` may still observe/collect and roll up `evaluate` for
dashboards; failures there do not fail `CI Gate`.

### Measurement plan

#### Representative Bazel surface

Matches the Blacksmith `Bazel Bootstrap` compile/test path (not full TCK BDD):

- Test: `//:bazel_test_graph_smoke`
- Build: `//:bazel_smoke`, `//:first_party_libs`, `//:cli_bins`,
  `//:resource_inputs`, `//:release_bins`
- Bindings: `//:binding_cdylibs`

#### Cold protocol

- **Bazel cold (correctness):** CLI-only empty `--remote_cache` / `--disk_cache`
  (never checked in as repo defaults). Must succeed without repository changes
  and report zero remote cache hits.
- **Bazel cold (perf):** clean local output base / empty or evicted Blacksmith
  repository cache (admin can clear from the Cache page). Sticky local disks do
  not count as warm remote-cache hits.
- **Cargo cold:** empty `target/` **without** sticky-disk hydrate. Sticky-disk
  warm Cargo starts from the #12 sample are **not** cold.

#### Warm protocol

1. Populate cache with a successful representative Bazel run at SHA `S`.
2. Re-run the same targets at the same SHA on a Blacksmith runner into a
   **fresh** `--output_base` (same-base re-runs are satisfied locally and hide
   remote hits).
3. Bazel process summary must show `remote cache hit` counts &gt; 0.
4. Record wall seconds, process counts, and (when available) Blacksmith Cache
   dashboard storage / hit-rate links.

#### Paired sample (≥10)

Each pair records cold + warm Bazel walls for the representative surface at one
immutable SHA, plus optional `cargo_cold_wall_seconds` and
`compute_proxy_seconds` (sum of Bazel job walls comparable to the #12 proxy).

Append pairs into `perf-sample.json`, set:

- `observations.remote_cache_hits_on_identical_sha = true`
- `observations.cache_unavailable_cold_correct = true` (from CI/harness)
- `observations.affected_inputs_isolation = true` (from harness)
- `status = "complete"` only when gates pass
- `blacksmith_dashboard_links` to Cache/Bazel job URLs
- exact SHA in closure notes

#### Thresholds (#1 / baseline)

Against [§ Baseline (#12)](#baseline-12):

| Gate | Requirement |
| --- | --- |
| Warm PR build/test p50 | ≥ 30% faster than Cargo primary job set p50 (**625s** = Rust Tests 327 + Python 177 + Node 121) |
| Total build compute proxy | ≥ 25% lower than Cargo six-job sum p50 (**923s**) |
| Cold p50 regression | ≤ 10% vs cold Cargo walls recorded in the paired sample |
| Sample size | ≥ 10 pairs |
| Remote hits | Present on repeated identical-SHA builds |
| Affected inputs | Source change reruns only actions with changed declared inputs |
| Cache unavailable | Cold build remains correct |

Maintainer-approved waiver may be checked in under `waiver` (prefer pass).

### Local commands

```bash
python3 scripts/ci/bazel-cache-perf.py --mode policy
python3 scripts/ci/test-bazel-cache-perf.py

# Cache-unavailable correctness (does not require Blacksmith admin)
python3 scripts/ci/bazel-cache-perf.py --mode cold-correctness

# Warm observation (hits require org-admin enablement on Blacksmith runners)
mkdir -p dist
python3 scripts/ci/bazel-cache-perf.py --mode observe-warm --write dist/warm-observation.json

# Collect ≥10 cold/warm pairs (Blacksmith runners; long-running)
python3 scripts/ci/bazel-cache-perf.py --mode collect-pairs --pairs 10 \
  --write dist/perf-sample-collected.json

# Affected-input isolation probe
python3 scripts/ci/bazel-cache-perf.py --mode affected-inputs --write dist/affected-inputs.json

# Strict close gate (fails while pending_org_admin)
python3 scripts/ci/bazel-cache-perf.py --mode evaluate \
  --evidence tools/bazel/migration-evidence/perf-sample.json

# CI readiness evaluate
python3 scripts/ci/bazel-cache-perf.py --mode evaluate --allow-pending \
  --evidence tools/bazel/migration-evidence/perf-sample.json
```

### Security

- No secrets, tokens, OIDC material, or publish credentials in cacheable Bazel
  actions (unchanged release/publish boundary).
- Cross-branch cache reuse is safe only via Bazel action keys / declared inputs.
- Do not upload sensitive fixtures into remote cache payloads.

### Issue close rule

Close [#5](https://github.com/CurateLabs/graphforge/issues/5) only when:

1. Org-admin enablement is done and remote hits are observed on identical-SHA builds.
2. `perf-sample.json` has ≥10 pairs and `evaluate` passes **without** `--allow-pending`.
3. Cache-unavailable cold correctness and affected-input isolation are proven.
4. Exact SHA + Blacksmith dashboard links are in closure notes.

[#5](https://github.com/CurateLabs/graphforge/issues/5) is closed with complete
evidence. Cutover: [§ CI Gate cutover (#4)](#ci-gate-cutover-4) / [#4](https://github.com/CurateLabs/graphforge/issues/4).

---

## CI Gate cutover (#4)

_Originally `bazel-migration.md`: Bazel migration CI Gate cutover (#4)_


Implements sequence step 9 of [#1](https://github.com/CurateLabs/graphforge/issues/1)
via child issue [#4](https://github.com/CurateLabs/graphforge/issues/4).

Companion artifacts:

- Orchestration: [§ Orchestration (#1)](#orchestration-1)
- Parity (#6): [§ Parity (#6)](#parity-6)
- Cache/perf (#5): [§ Cache and performance gates (#5)](#cache-and-performance-gates-5)
- Ledger: [§ Ledger (#12)](#ledger-12)

### Cutover contract

| Piece | After #4 |
| --- | --- |
| Required check name | Exactly **`CI Gate`** (unchanged) |
| Live GitHub enforcement | Repository ruleset **19988544** (`main`, `~DEFAULT_BRANCH`) requires status check context **`CI Gate`** (#721) |
| Authoritative Rust compile/test | `Bazel Bootstrap` → `bazelisk test //:ci_rust_tests` (+ libs/CLI/resources/bindings builds) |
| Retired | Cargo `rust-test` workspace job; PR job-isolated Cargo `target/` sticky disks |
| Retained Cargo diagnostics | `Rust Quality` (fmt/clippy); Windows `graphforge-storage` lock unit tests; PR maturin/napi binding assembly (no sticky); Binding RC macOS/Windows/cross napi + fuzz / release-certification sticky packaging lanes |
| Path-classified skips | Remain neutral via `require-gates.sh` (`success` or `skipped`) |
| Dual-build parity | Diagnostic under non-required `Bazel Diagnostics` for **one release cycle** |

Do **not** set `--remote_cache` in-repo. Blacksmith injects repository Bazel caching.

### What changed in CI

1. Classifier: any `rust=true` change also enables `bazel=true`, so the authoritative
   Bazel job always runs for Rust surfaces.
2. `rust-test` (`cargo test --workspace`) removed from `.github/workflows/test.yml`
   and from `CI Gate` `needs`.
3. All five PR sticky mounts
   (`${{ github.repository }}-${{ github.job }}-${{ hashFiles('Cargo.lock') }}-target-v1`)
   removed from Test Suite.
4. `Bazel Bootstrap` runs `//:ci_rust_tests` (unit + integration + snapshot + CLI + API BDD)
   as the required Rust test graph.
5. Same-SHA Cargo/Bazel parity remains as a **diagnostic** step for one release cycle.

### Cargo diagnostic / rollback (one release cycle)

Use this if Bazel CI misbehaves and maintainers need Cargo as a temporary
authoritative path. Keep Cargo manifests and local `cargo` tooling regardless.

#### Local diagnostic (no workflow change)

```bash
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace --no-fail-fast

python3 scripts/ci/cargo-bazel-parity-check.py \
  --mode all \
  --write-evidence dist/cargo-bazel-parity-evidence.json
```

#### Restore Cargo `rust-test` under CI Gate (rollback)

1. Restore the `rust-test` job from git history prior to the #4 cutover commit
   (search `.github/workflows/test.yml` for `name: Rust Tests`).
2. Re-add `rust-test` to `ci-gate` `needs` and to `scripts/ci/require-gates.sh` args.
3. Optionally re-mount PR sticky disks for `rust-test` / `rust-lint` only
   (update `EXPECTED_STICKY_KEYS` / `EXPECTED_DEPENDENCY_KEYS` in
   `scripts/ci/test-ci-storage-policy.py` in the same change).
4. Keep required check name **`CI Gate`**. Do not invent a second required context.
   Live enforcement is ruleset `19988544` (not workflow naming alone); verify with
   `python3 scripts/ci/verify-ci-gate-enforcement.py --check-live`.
5. Prefer fixing Bazel root causes; treat this rollback as temporary for one
   release cycle after cutover, then remove again once Bazel is healthy.

#### Binding RC / publish sticky disks

Linux Binding RC **host** lanes (Python Ubuntu + Node `x86_64-unknown-linux-gnu`)
consume Bazel `//:binding_cdylibs` via
`scripts/ci/binding_rc_bazel_native.py` +
`assemble_bazel_binding_packages.py` — no maturin/napi native recompile and no
Cargo `target/` sticky mount on those lanes. Remaining Binding RC platforms
(macOS/Windows Python maturin; macOS/Windows Node napi; Linux aarch64
napi-cross) and release-load sticky `target/` volumes stay until follow-on
cutover. Fuzz retains its sticky disk as a justified retained tool.
`release_candidate` emits gitignored `index.js` / `index.d.ts` from a retained
Linux addon (`emit-node-loaders`) instead of `napi build` recompile.

### Acceptance mapping

| #4 / #1 AC | Evidence |
| --- | --- |
| Bazel authoritative under `CI Gate` | `Bazel Bootstrap` runs `//:ci_rust_tests`; `rust-test` absent from Test Suite / gate |
| Default branch requires exactly `CI Gate` | Live ruleset **19988544** `required_status_checks` context `CI Gate`; snapshot [ci-gate-ruleset-19988544.json](../../tools/bazel/migration-evidence/ci-gate-ruleset-19988544.json); `scripts/ci/verify-ci-gate-enforcement.py` (#721) |
| Cargo sticky disks retired without weakening gates | PR sticky keys gone; Binding RC/fuzz/release-certification retained; storage-policy tests updated |
| Documented Cargo rollback one release cycle | This document |
| Path-classified skips remain neutral | `require-gates.sh` still accepts `skipped` |

### Next

Evidence-map reconciliation after remediations: [#724](https://github.com/CurateLabs/graphforge/issues/724)
([§ Close-readiness evidence map (#3)](#close-readiness-evidence-map-3)). Live
`CI Gate` enforcement is #721 (ruleset **19988544**); do not treat workflow job
naming alone as merge-gate proof.

---

## Close-readiness evidence map (#3)

_Originally `bazel-migration.md`: #1 Bazel migration — close-readiness AC evidence map (#3)_


Checked-in map from canonical issue
[#1](https://github.com/CurateLabs/graphforge/issues/1) acceptance criteria to
Bazel-migration child-issue evidence (PR / merge SHA / artifact). Produced by
[#3](https://github.com/CurateLabs/graphforge/issues/3) so Bazel-migration gate
[#2](https://github.com/CurateLabs/graphforge/issues/2) can close when children
are complete.

Orchestration: [§ Orchestration (#1)](#orchestration-1).
Developer guide: [bazel.md](bazel.md).

**Cutover SHA (authoritative CI after #4):**
`75a33e5cf6d9dab1407eba719e98740c95426d91` ([PR #427](https://github.com/CurateLabs/graphforge/pull/427)).

### Child issue → merge evidence

| Child | Title | PR(s) | Merge SHA |
| --- | --- | --- | --- |
| [#13](https://github.com/CurateLabs/graphforge/issues/13) | Sub-agent orchestration | [#417](https://github.com/CurateLabs/graphforge/pull/417) | `6e8b8e3fdc1ecd960eacf14a73e5be7b54fcef3c` |
| [#12](https://github.com/CurateLabs/graphforge/issues/12) | Inventory + baseline freeze | [#418](https://github.com/CurateLabs/graphforge/pull/418) | `a8fa4298c51077c058b25b2e1d0b854597820cbf` |
| [#11](https://github.com/CurateLabs/graphforge/issues/11) | Bazelisk / Bzlmod / drift | [#419](https://github.com/CurateLabs/graphforge/pull/419) | `9d5a10fb8078130745bcb41f2835b7298ee6bb77` |
| [#10](https://github.com/CurateLabs/graphforge/issues/10) | Foundation / compiler libs | [#420](https://github.com/CurateLabs/graphforge/pull/420) | `ed96019e14ff5c7af28227c20f7195b7ceb7cd30` |
| [#9](https://github.com/CurateLabs/graphforge/issues/9) | Runtime libs | [#421](https://github.com/CurateLabs/graphforge/pull/421) | `b8c217802dd7e7c0d15cdef10ad76d3cbe9f45f3` |
| [#8](https://github.com/CurateLabs/graphforge/issues/8) | Tests / CLI / resources | [#423](https://github.com/CurateLabs/graphforge/pull/423) | `4d13fcdeb62386beed75e8aa2674432101e01904` |
| [#7](https://github.com/CurateLabs/graphforge/issues/7) | PyO3 / napi packaging | [#422](https://github.com/CurateLabs/graphforge/pull/422) | `1c3a4068bebc2f6bb22a5f8be835b37474d99e12` |
| [#6](https://github.com/CurateLabs/graphforge/issues/6) | Release platforms + parity | [#424](https://github.com/CurateLabs/graphforge/pull/424) | `457eb1171cbd240ade30efd63814d9b8748a9934` |
| [#5](https://github.com/CurateLabs/graphforge/issues/5) | Blacksmith cache + perf | [#425](https://github.com/CurateLabs/graphforge/pull/425), [#426](https://github.com/CurateLabs/graphforge/pull/426) | `c52be21063ea3cc65d7d66c2ae91816c78bb3907`, `bad99f97bd1355b21702a20d464e8a342776353d` |
| [#4](https://github.com/CurateLabs/graphforge/issues/4) | CI Gate cutover | [#427](https://github.com/CurateLabs/graphforge/pull/427) | `75a33e5cf6d9dab1407eba719e98740c95426d91` |
| [#3](https://github.com/CurateLabs/graphforge/issues/3) | Docs / observability / this map | [#428](https://github.com/CurateLabs/graphforge/pull/428) | `910af15030843d9060c51ec13d9924678aea2eae` |
| [#720](https://github.com/CurateLabs/graphforge/issues/720) | Bazel binding clean-install acceptance | [#760](https://github.com/CurateLabs/graphforge/pull/760) | `71687b4d377a4293ae6b86176c9876cda4167722` |
| [#721](https://github.com/CurateLabs/graphforge/issues/721) | Enforce `CI Gate` on default branch | [#759](https://github.com/CurateLabs/graphforge/pull/759) | `747f4af2966f378a8b80ccb695ec23999f066dd8` |
| [#722](https://github.com/CurateLabs/graphforge/issues/722) | coverage-rust Cucumber loader drift | [#758](https://github.com/CurateLabs/graphforge/pull/758) | `62684b6aa6403d681f2a2cb64d606c6b792be4c9` |
| [#723](https://github.com/CurateLabs/graphforge/issues/723) | Node feature TypeScript fail-closed check | [#757](https://github.com/CurateLabs/graphforge/pull/757) | `2b38538d029af3c6cda451b3273641848ef6ca1d` |

### #1 acceptance criteria → evidence

| #1 AC | Status | Child | Evidence pointer |
| --- | --- | --- | --- |
| Checked-in migration ledger for all Cargo targets and every CI/release build command | Met | #12 (+ updates #11–#6) | [§ Ledger (#12)](#ledger-12); `tools/bazel/parity/migration_target_map.json`; `scripts/ci/bazel-migration-ledger-check.py` |
| Bazel builds all 18 first-party packages without shelling out to Cargo for ordinary compilation or tests | Met | #11–#9, #8, #7, #779 | [bazel-bootstrap.md](bazel-bootstrap.md); `//:first_party_libs`, `//:binding_cdylibs`, `//:ci_rust_tests`; merge SHAs above |
| All 53 Rust integration tests, crate unit tests, doctest equivalents, BDD, snapshots, public-surface gates under mapped Bazel test graph | Met | #8, #6 | Ledger + `//:integration_tests` / `//:unit_tests` / `//:snapshot_tests` / `//:bdd_tests` / `//:ci_rust_tests`; [§ Parity (#6)](#parity-6) |
| Bazel-built Python wheels and Node packages pass clean-install, no-fallback, parity, persistence/reopen, structured-error suites | Met | #7, #6, #720 | PEP 427 wheel naming + synthetic Node `version()` loader in `scripts/ci/assemble_bazel_binding_packages.py`; Binding RC `--out dist` (#760 / `71687b4d377a4293ae6b86176c9876cda4167722`); unit proof in `scripts/ci/test-assemble-bazel-binding-packages.py`; `//:python_wheel_smoke` / `//:node_package_smoke` |
| Linux, macOS, Windows, and supported Node cross-target release evidence remains complete | Met | #6 | `tools/bazel/release/release_platforms.json`; `//platforms:*`; Binding RC contract unchanged |
| Cargo and Bazel dependency/feature graphs cannot drift silently | Met | #11 | `scripts/ci/cargo-bazel-drift-check.py`; `tools/bazel/drift/cargo_feature_fingerprint.json`; `cargo-bazel-lock.json` |
| Repeated identical-SHA builds report remote cache hits; source change reruns only actions whose declared inputs changed | Met | #5 | [§ Cache and performance gates (#5)](#cache-and-performance-gates-5); [perf-sample.json](../../tools/bazel/migration-evidence/perf-sample.json); `affected-inputs` harness mode |
| Blacksmith cache disablement or eviction produces a correct cold build without repository changes | Met | #5 | `bazel-cache-perf.py --mode cold-correctness`; observations in perf sample |
| Across ≥10 paired runs: warm PR p50 ≥30% faster and compute ≥25% lower than Cargo baseline; cold p50 regression ≤10% | Met | #5 (#12 baseline) | [§ Baseline (#12)](#baseline-12); `perf-sample.json` `status=complete` + strict `evaluate` |
| No secret, token, signing material, publish credential, or user data in a cacheable Bazel action or build log | Met | #7, #5, #3 | See [Security and supply chain](#security-and-supply-chain) below; publish OIDC/credentials stay in release workflows outside `Bazel Bootstrap` |
| Required check context remains `CI Gate`; path-classified skips remain neutral | Met | #6, #4, #721 | **Workflow shape:** job display name `CI Gate` + `scripts/ci/require-gates.sh` / `scripts/ci/test-ci-storage-policy.py`. **Live enforcement (distinct):** repository ruleset **19988544** requires status check context exactly `CI Gate` ([ci-gate-ruleset-19988544.json](../../tools/bazel/migration-evidence/ci-gate-ruleset-19988544.json); `GET /repos/CurateLabs/graphforge/rulesets/19988544`); `scripts/ci/verify-ci-gate-enforcement.py` fails when YAML names the job but the ruleset does not require it. Classic branch-protection API remains unused (`GET …/branches/main/protection` → 404). Merge SHA #759 / `747f4af2966f378a8b80ccb695ec23999f066dd8`. |
| Cargo CI compilation and sticky build disks removed only after same-SHA parity and performance gates | Met | #4 (after #6/#5) | PR sticky `target/` keys retired; Cargo `rust-test` removed; Binding RC / fuzz / release-certification retained as justified |
| Developer, architecture, build, release, troubleshooting, and cache-observability documentation is current | Met | #3, #724 | [bazel.md](bazel.md) + companions listed there; this evidence map reconciled after #720–#723 |

### #1 Documentation checklist

| #1 Documentation topic | Canonical doc |
| --- | --- |
| Build-system architecture and ownership | [bazel.md](bazel.md) § Architecture and ownership; [ARCHITECTURE.md](../engineering/ARCHITECTURE.md) |
| Bazel/Bazelisk installation and local commands | [bazel.md](bazel.md) § Install; [bazel-bootstrap.md](bazel-bootstrap.md) |
| Adding crates, dependencies, features, tests, fixtures, generated inputs | [bazel.md](bazel.md) § Extending the graph |
| Python/Node packaging handoff | [bazel.md](bazel.md) § Packaging handoff; [bazel-bootstrap.md](bazel-bootstrap.md) |
| Blacksmith Bazel cache enablement, metrics, eviction, troubleshooting | [§ Cache and performance gates (#5)](#cache-and-performance-gates-5); [bazel.md](bazel.md) § Cache and troubleshooting |
| Cargo compatibility and rollback | [§ CI Gate cutover (#4)](#ci-gate-cutover-4); [bazel.md](bazel.md) § Cargo compatibility |
| CI and release runbooks | [bazel.md](bazel.md) § CI and release; [§ CI Gate cutover (#4)](#ci-gate-cutover-4); [release-process.md](release-process.md) |

### Observability evidence paths

| Signal | Path / artifact |
| --- | --- |
| Per-run representative build log | CI `dist/bazel-representative-build.log` (uploaded via cache/perf artifact when present) |
| Per-run machine-readable process summary | `dist/bazel-representative-build.summary.json` |
| Warm observation | `dist/bazel-warm-observation.json` |
| Affected-input probe | `dist/bazel-affected-inputs.json` |
| CI observation rollup | `dist/bazel-cache-perf-ci-observation.json` |
| Checked-in ≥10-pair sample (one-shot Bazel-migration evidence) | [bazel-migration-evidence/perf-sample.json](../../tools/bazel/migration-evidence/perf-sample.json) |
| Diagnostic dual-build parity (one release cycle; non-required) | `dist/cargo-bazel-parity-evidence.json` via `Bazel Diagnostics` |
| Blacksmith Cache dashboard | https://app.blacksmith.sh/cache |
| Authoritative Rust test log | `dist/bazel-ci-rust-tests.log` |

See also [OBSERVABILITY.md](../engineering/OBSERVABILITY.md) (Bazel CI signals).

### Security and supply chain

Confirmed against the post-#4 tree (cutover SHA above):

| Constraint | Evidence |
| --- | --- |
| Pin Bazel, rules, toolchains, external archives with integrity hashes | `.bazelversion` (`9.2.0`); `MODULE.bazel` (`rules_rust` `0.73.0`, Rust `1.96.0`); `MODULE.bazel.lock` `registryFileHashes` / archive `sha256` |
| Third-party Rust deps from reviewed Cargo lock | `crate.from_cargo` + `Cargo.lock` + `cargo-bazel-lock.json` |
| No `--remote_cache` in repo / workflow (Blacksmith injects) | `.bazelrc`; `Bazel Bootstrap` comments; `bazel-cache-perf.py --mode policy` |
| OIDC / npm / PyPI / signing credentials outside cacheable Bazel actions | Publish/release workflows only; `Bazel Bootstrap` has no `NODE_AUTH_TOKEN` / PyPI / signing secrets; packaging smoke assembles from Bazel cdylibs without registry auth |
| Network access restricted where practical | Ordinary `rules_rust` compile/test actions are sandboxed; crate_universe fetch is lockfile-bound; no in-repo remote-cache URL |
| Cross-branch cache reuse only via action key + declared inputs | Bazel remote-cache semantics; documented in [§ Cache and performance gates (#5)](#cache-and-performance-gates-5) |
| No user data / sensitive fixtures uploaded as cache payloads | Test fixtures are hermetic source inputs; CI logs use `--test_output=errors` |

### Explicit non-deliverables (Bazel-migration)

- **Mobile bindings** (Swift, Kotlin, UniFFI, XCFramework, JVM JAR/AAR) are
  **abandoned for Bazel migration** — ledger row `RT-mobile` is `excluded`. Do not treat
  product roadmap “planned UniFFI” notes as Bazel migration deliverables.
- peer-extension and embedded-performance epics are out of scope.

### Gate close notes

- [#3](https://github.com/CurateLabs/graphforge/issues/3) closed via
  [#428](https://github.com/CurateLabs/graphforge/pull/428)
  (`910af15030843d9060c51ec13d9924678aea2eae`); this document’s #3 row records
  that merge SHA (no remaining `(this PR)` / `(fill at merge)` placeholders).
- Remediation children [#720](https://github.com/CurateLabs/graphforge/issues/720)–[#723](https://github.com/CurateLabs/graphforge/issues/723)
  closed via [#760](https://github.com/CurateLabs/graphforge/pull/760),
  [#759](https://github.com/CurateLabs/graphforge/pull/759),
  [#758](https://github.com/CurateLabs/graphforge/pull/758), and
  [#757](https://github.com/CurateLabs/graphforge/pull/757) with the merge SHAs
  in the child table above. Binding clean-install and live `CI Gate` enforcement
  claims are marked Met only with those pointers.
- [#724](https://github.com/CurateLabs/graphforge/issues/724) is this
  reconciliation of the checked-in map against those remediations (ordinary
  AGENTS.md docs close; no Binding RC / publish cascade).
- Close [#2](https://github.com/CurateLabs/graphforge/issues/2) only when every
  native Bazel child (#3–#13) and remediation blocker (#720–#724) is closed with
  ordinary AGENTS.md evidence.
- Close [#1](https://github.com/CurateLabs/graphforge/issues/1) when its AC
  outcomes are met via this map (no release-gate cascade required for ordinary
  close).
