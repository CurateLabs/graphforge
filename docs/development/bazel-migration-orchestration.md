# Bazel migration sub-agent orchestration (M2 / #1)

Durable contract for implementing canonical issue
[#1](https://github.com/CurateLabs/graphforge/issues/1) through M2 child issues
[#13](https://github.com/CurateLabs/graphforge/issues/13)–[#3](https://github.com/CurateLabs/graphforge/issues/3)
and gate [#2](https://github.com/CurateLabs/graphforge/issues/2).

**Specification authority:** issue #1 (build-system contract, acceptance
criteria, implementation sequence, observability, security, documentation).
Child issues are execution slices only. Do not invent a second build contract.

**Milestone:** [M2: Implement #1 Bazel Migration via Sub-Agents](https://github.com/CurateLabs/graphforge/milestone/2).

## Purpose

Issue #1 is large enough that M2 delivers it via coordinated sub-agents. This
document defines roles, inputs/outputs, handoffs, conflict rules, and ownership
so parallel work does not drift from #1's build-system contract.

## Critical path (DAG)

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
  → #2 M2 gate close (requires #13-#3)
```

Housekeeping issues on M2 (post-release verify / legal / docs tidy) are **not**
on this DAG. They must not block or reorder #1 sequence work.

## Global conflict rules

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
   XCFramework, and JVM JAR/AAR work is abandoned for M2. Do not inventory,
   model, document, or CI those surfaces as M2 deliverables. Python (PyO3) and
   Node (napi-rs) packaging in #7 are existing bindings — not mobile.

## Role catalog and #1 sequence ownership

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

## Role charters

Each charter lists **inputs**, **outputs**, **non-goals**, and the **#1 sequence
step** owned. Agents implement only their slice unless a verified blocker forces
a narrow upstream fix (then return ownership to the owning role).

### R0-orchestrator — #13

- **Inputs:** Issue #1 body; M2 child issue set; this repository's Cargo/CI layout.
- **Outputs:** This orchestration note (checked in); role↔issue↔sequence map;
  named handoff artifacts for later slices.
- **Non-goals:** Bazel targets, ledger rows, performance measurement, CI cutover,
  mobile bindings.
- **Owns:** Pre-sequence coordination for M2.

### R1-inventory — #12 (sequence step 1)

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

### R2-bootstrap — #11 (sequence step 2)

- **Inputs:** Frozen ledger/baseline from #12; #1 Build-System Contract.
- **Outputs:** Bazelisk pin; Bzlmod + maintained `rules_rust`/crate-universe with
  integrity hashes; minimal MODULE/BUILD scaffolding for #10; deterministic
  Cargo↔Bazel dependency/feature drift check (fail-closed test).
- **Non-goals:** Modeling all packages; `--remote_cache`; performance gates; mobile.
- **Owns:** Pinned toolchain/rules; drift prevention foundation; no Cargo shell-outs
  for ordinary compilation in the bootstrap path.

### R3-libs-foundation — #10 (sequence step 3)

- **Inputs:** Bootstrap from #11; ledger rows for foundation/compiler crates.
- **Outputs:** Real Bazel library targets for foundation/compiler-layer crates;
  ledger updates with labels or justified exceptions; Blacksmith-exercisable builds.
- **Non-goals:** Storage/exec/search/knowledge/API (#9); test graph (#8); bindings (#7).
- **Owns:** Foundation slice of “Bazel builds first-party packages without Cargo
  shell-outs.”

### R4-libs-runtime — #9 (sequence step 4)

- **Inputs:** Foundation targets from #10; remaining library ledger rows.
- **Outputs:** Bazel library coverage for storage, execution, search, knowledge,
  API (and peers named in the ledger); drift check still green; ledger complete for
  first-party libraries (or explicit retained-tool exceptions).
- **Non-goals:** Full test/BDD/CLI graph (#8); cdylib packaging (#7); mobile.
- **Owns:** Completing first-party library modeling required by #1 AC.

### R5-tests — #8 (sequence step 5)

- **Inputs:** Library targets from #9; ledger test/resource inventory.
- **Outputs:** Bazel targets for unit, integration, snapshot, BDD, CLI, and
  non-source inputs; deterministic CI target groups; ledger updates.
- **Non-goals:** PyO3/napi packaging (#7); same-SHA parity gate (#6); mobile suites.
- **Owns:** #1 AC for mapped Rust tests/scenarios/snapshots/CLI under Bazel.

### R6-bindings — #7 (sequence step 6)

- **Inputs:** API/library targets from #9 (and any shared deps from #10/#8 as needed).
- **Outputs:** Bazel-built PyO3 and napi-rs cdylibs; packaging handoff that consumes
  those artifacts (maturin/napi may assemble/sign/publish but must not silently
  recompile a different native graph); credentials/OIDC outside cacheable actions.
- **Non-goals:** Swift/Kotlin/UniFFI/mobile; full cross-platform parity matrix (#6).
- **Owns:** #1 AC for Bazel-built Python wheels and Node packages (CI smoke path).

### R7-parity — #6 (sequence step 7)

- **Inputs:** Test graph (#8) and bindings (#7); dual-build still allowed.
- **Outputs:** Cross-platform release targets (Linux/macOS/Windows + supported Node
  cross-targets); same-SHA Cargo/Bazel parity evidence; ledger failures for unmapped
  targets; `CI Gate` still dual-build (Bazel not sole yet).
- **Non-goals:** Perf p50 gates (#5); sticky-disk removal (#4); mobile parity.
- **Owns:** #1 parity and release-evidence ACs under dual-build.

### R8-cache-perf — #5 (sequence step 8)

- **Inputs:** Parity evidence (#6); baseline from #12; org-admin Blacksmith Bazel
  Build Caching enabled for this repository.
- **Outputs:** Observed cache hits/misses, action counts, wall/CPU/storage for cold
  and warm runs; machine-readable benchmark artifacts; proof that cache
  disablement/eviction still yields correct cold builds; #1 performance thresholds
  (or maintainer-approved checked-in waiver — prefer pass).
- **Non-goals:** Sole `CI Gate` cutover (#4); product-code speedups unrelated to the
  build graph; mobile.
- **Owns:** #1 remote-cache and performance gate ACs.

### R9-cutover — #4 (sequence step 9)

- **Inputs:** Perf gates (#5) and parity (#6) accepted.
- **Outputs:** Bazel as authoritative CI compilation/test path under required check
  name `CI Gate`; documented Cargo diagnostic/rollback for one release cycle;
  removal of obsolete Cargo CI compilation jobs and sticky `target/` disks only
  after same-SHA parity + performance evidence; path-classified skips remain neutral.
- **Non-goals:** Docs/observability close-out (#3) except rollback path notes owned
  here; re-introducing mobile CI; second remote-cache provider.
- **Owns:** #1 cutover / sticky-disk / `CI Gate` name ACs.

### R10-docs-close — #3 (post-sequence)

- **Inputs:** Cutover (#4) complete or concurrently finalized with docs-only
  follow-ups that do not reopen cutover; evidence from #12–#4.
- **Outputs:** Developer/architecture/build/release/troubleshooting/cache-observability
  docs current per #1 Documentation; per-run Bazel summary and benchmark paths
  documented; security/supply-chain constraints confirmed; checked-in #1 AC →
  child evidence map; mobile bindings not documented as M2 deliverables.
- **Non-goals:** Further target modeling; M3/M4 epics; Swift/Kotlin/UniFFI as M2
  requirements.
- **Owns:** #1 documentation AC and close-readiness evidence consolidation for #2/#1.

## Handoff artifacts

Later slices depend on named artifacts. Create or update these paths (or update
this table if a better checked-in location is chosen in the owning PR — keep one
canonical path).

| Artifact | Owning role / issue | Path (canonical) | Consumers |
| --- | --- | --- | --- |
| Orchestration contract | R0 / #13 | `docs/development/bazel-migration-orchestration.md` | All M2 Bazel children |
| Migration ledger | R1 / #12 | `docs/development/bazel-migration-ledger.md` | #11–#6 (label/status updates each slice) |
| Cargo/Blacksmith baseline | R1 / #12 | `docs/development/bazel-migration-baseline.md` | #5 (perf comparison) |
| Retained-tool exceptions | R1 / #12 (updated by later roles) | Section in migration ledger | #6 parity (fail unmapped / unjustified) |
| Target map (Bazel labels) | R3–R6 / #10–#7 | Columns/rows in migration ledger | #6–#4 |
| Drift-check entrypoint | R2 / #11 | Documented in ledger + bootstrap docs (exact label/script in #11 PR) | #10–#6 CI |
| Parity evidence | R7 / #6 | `docs/development/bazel-migration-parity.md` (+ CI artifacts linked from PR) | #5, #4, #3 |
| Cache/perf benchmarks | R8 / #5 | `docs/development/bazel-migration-perf.md` + machine-readable files under `docs/development/bazel-migration-evidence/` | #4, #3, #1 close |
| Cutover + Cargo rollback | R9 / #4 | `docs/development/bazel-migration-cutover.md` | #3, operators |
| #1 AC evidence map | R10 / #3 | `docs/development/bazel-migration-ac-evidence.md` | #2, #1 close |

PRs for modeling slices must update the migration ledger in the same change when
they add, remap, or except targets.

## Parallelism

- **Serial until #9:** `#13 → #12 → #11 → #10 → #9`.
- **Parallel after #9:** `#8` and `#7` may proceed concurrently; both block `#6`.
- **Serial to close:** `#6 → #5 → #4 → #3 → #2` (then #1 when ACs are evidenced).

Do not start a downstream slice by inventing missing upstream artifacts. If a
blocker is verified in an upstream slice, open or reuse a bounded fix on that
slice's issue; do not expand the downstream issue's scope.

## Agent operating rules

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

## Explicit exclusions (all roles)

- Mobile bindings: Swift, Kotlin, UniFFI, XCFramework, JVM JAR/AAR.
- Changing GraphForge runtime behavior, public APIs, or product features for the
  sake of the migration.
- Using Bazel as a shell wrapper around Cargo.
- Removing Cargo manifests / `Cargo.lock` or breaking Rust ecosystem tooling.
- A second explicit remote-cache provider; Blacksmith remote execution (caching
  only for this migration).
- M3 peer-extension and M4 embedded-performance epics.
- Claiming success before same-SHA parity and #1 performance gates pass.

## Evidence for closing #13

- This document is checked into the repository and linked from engineering docs.
- Role table maps every M2 Bazel child (#12–#3) to a named role and #1 sequence
  step (or post-sequence docs close for #3).
- Mobile bindings are excluded in global rules and every role charter non-goals.
- Handoff artifacts required by later slices are named with canonical paths.

Ordinary AGENTS.md close applies (no release-gate cascade).
