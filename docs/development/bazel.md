# Bazel build system (M2 / #1)

GraphForge uses **Bazel** (via **Bazelisk**) as the authoritative incremental
build and test engine for CI Rust compilation and mapped tests. Cargo manifests
and `Cargo.lock` remain the Rust ecosystem source of truth for dependencies,
local tooling, publishing metadata, and crate-universe generation.

Canonical contract: [#1](https://github.com/CurateLabs/graphforge/issues/1).
Close-readiness evidence map: [bazel-migration-ac-evidence.md](bazel-migration-ac-evidence.md).

## Architecture and ownership

| Concern | Owner | Notes |
| --- | --- | --- |
| Ordinary Rust compile + mapped tests in CI | Bazel (`rules_rust` / crate-universe) | Real targets — not `cargo` shell wrappers |
| Dependency declaration / lockfile | `Cargo.toml` + `Cargo.lock` | Drift check fails closed |
| Required PR merge check | **`CI Gate`** | Unchanged name through cutover |
| Remote cache | Blacksmith repository Bazel cache | Do **not** set `--remote_cache` in-repo |
| Python / Node package assembly | maturin / napi may assemble | Must consume Bazel-built natives; no silent recompile of a different graph |
| Fmt / Clippy | Cargo (`Rust Quality`) | Workspace lint policy remains Cargo-owned for now |
| Fuzz | `cargo-fuzz` (retained) | Justified exception `RT-fuzz` |
| crates.io publish metadata | Cargo (retained) | Justified exception `RT-publish-crates` |
| Binding RC / publish sticky disks | Packaging lanes | Not retired by M2 cutover |

Mobile bindings (Swift / Kotlin / UniFFI / XCFramework / JVM) are **not** an M2
Bazel deliverable. Product roadmap may still mention them as planned later work;
they must not appear as required migration targets.

Companion deep-dives:

| Doc | Role |
| --- | --- |
| [bazel-migration-orchestration.md](bazel-migration-orchestration.md) | Sub-agent roles and DAG |
| [bazel-migration-ledger.md](bazel-migration-ledger.md) | Target + CI command inventory |
| [bazel-migration-baseline.md](bazel-migration-baseline.md) | Accepted Cargo/Blacksmith baseline |
| [bazel-bootstrap.md](bazel-bootstrap.md) | Pins, labels, local smoke commands |
| [bazel-migration-parity.md](bazel-migration-parity.md) | Same-SHA Cargo/Bazel parity |
| [bazel-migration-perf.md](bazel-migration-perf.md) | Cache metrics, thresholds, troubleshooting |
| [bazel-migration-cutover.md](bazel-migration-cutover.md) | CI Gate cutover + Cargo rollback |
| [bazel-migration-ac-evidence.md](bazel-migration-ac-evidence.md) | #1 AC → child evidence |

## Install

1. Install [Bazelisk](https://github.com/bazelbuild/bazelisk) and ensure it is
   on `PATH` as `bazelisk` (and optionally `bazel`).
2. Repository pin: `.bazelversion` → **9.2.0** (do not override casually).
3. First run downloads the pinned Bazel and module deps via Bzlmod
   (`MODULE.bazel` / `MODULE.bazel.lock` carry integrity hashes).

```bash
bazelisk version   # must report 9.2.0 from .bazelversion
```

## Everyday local commands

```bash
# Authoritative CI-equivalent Rust test graph
bazelisk test //:ci_rust_tests

# Libraries, CLI, resources, release bins
bazelisk build //:first_party_libs //:cli_bins //:resource_inputs //:release_bins

# Binding cdylibs + packaging smoke (no maturin/napi native recompile)
bazelisk build //:binding_cdylibs //:python_wheel_smoke //:node_package_smoke

# Deterministic groups
bazelisk test //:unit_tests //:integration_tests //:snapshot_tests //:cli_tests
bazelisk test //:bdd_tests

# Fail-closed inventory / drift / parity diagnostics
python3 scripts/ci/cargo-bazel-drift-check.py
python3 scripts/ci/bazel-migration-ledger-check.py
python3 scripts/ci/cargo-bazel-parity-check.py --mode all \
  --write-evidence dist/cargo-bazel-parity-evidence.json

# Cache policy + cold correctness (no org-admin required)
python3 scripts/ci/bazel-cache-perf.py --mode policy
python3 scripts/ci/bazel-cache-perf.py --mode cold-correctness
```

Repo flags live in `.bazelrc` (Bzlmod on; **no** `--remote_cache`).

## Extending the graph

### New first-party crate

1. Add the crate under `crates/` and wire it into the Cargo workspace
   (`Cargo.toml` members + deps) as usual.
2. Add a `BUILD.bazel` using `gf_rust_library` / `gf_rust_test` from
   `tools/bazel/gf_rust.bzl` (see neighboring crates).
3. Attach the library to the appropriate root aggregate
   (`//:foundation_compiler_libs`, `//:runtime_libs`, or `//:first_party_libs`).
4. Add/update a row in `tools/bazel/parity/migration_target_map.json` and the
   human ledger ([bazel-migration-ledger.md](bazel-migration-ledger.md)).
5. Refresh Cargo↔Bazel lock/fingerprint state:

```bash
python3 scripts/ci/cargo-bazel-drift-check.py --write
CARGO_BAZEL_REPIN=1 bazelisk build --repo_env=CARGO_BAZEL_REPIN=1 //:first_party_libs
python3 scripts/ci/bazel-migration-ledger-check.py
```

### Dependencies and features

- Declare deps/features in Cargo manifests only; regenerate
  `cargo-bazel-lock.json` / fingerprint via the commands above.
- Do not hand-edit `@crates` labels to diverge from lock state — the drift
  check must stay green.

### Tests, fixtures, and generated inputs

- Unit tests: `gf_rust_test` next to the library.
- Integration binaries: follow `gf_rust_integration_test` patterns and attach to
  `//:integration_tests`.
- Snapshots / goldens / TCK / features / notebooks / contracts: declare as
  `filegroup` inputs under `//:resource_inputs` (or the specific package
  filegroup) so actions list hermetic inputs.
- BDD: keep scenarios under the existing `//:bdd_tests` /
  `//crates/graphforge-api:bdd` graph.
- After adding targets, update the migration target map in the **same** PR.

### CLI / build scripts

- Prefer `gf_rust_binary` / `gf_cargo_build_script` (see `graphforge-cli`).
- Skills and other non-source payloads stay declared filegroups — do not bury
  undeclared reads in actions.

## Packaging handoff (Python / Node)

| Stage | Owner |
| --- | --- |
| Native `.so` / `.dylib` / `.node` | Bazel `//:binding_cdylibs` |
| Wheel / npm package assembly smoke | `//:python_wheel_smoke`, `//:node_package_smoke` via `scripts/ci/assemble_bazel_binding_packages.py` |
| Multi-OS Binding RC certification | Existing Binding RC workflows (consume Bazel-built or equivalent natives; no silent alternate native graph) |
| Registry publish (OIDC / tokens) | Release workflows only — **never** cacheable Bazel actions |

Credentials, signing material, and publish tokens must stay out of Bazel actions
and remote-cache payloads.

## Cache and troubleshooting

### Enablement / metrics / eviction

See [bazel-migration-perf.md](bazel-migration-perf.md) for the full protocol.
Short form:

1. Org admin: Blacksmith
   [Settings → Features](https://app.blacksmith.sh/settings?tab=features) →
   **Bazel Build Caching** for `CurateLabs/graphforge`.
2. Confirm [Cache](https://app.blacksmith.sh/cache) shows a Bazel tab.
3. Never add a competing `--remote_cache`.
4. Eviction / empty cache: cold builds remain correct; only performance changes
   (`--mode cold-correctness`).

### Per-run observability paths

| Artifact | Meaning |
| --- | --- |
| `dist/bazel-ci-rust-tests.log` | Authoritative `//:ci_rust_tests` log |
| `dist/bazel-representative-build.log` | Representative libs/CLI/release build log |
| `dist/bazel-representative-build.summary.json` | Parsed remote-hit / process summary |
| `dist/bazel-warm-observation.json` | Warm identical-SHA observation |
| `dist/bazel-affected-inputs.json` | Affected-input isolation probe |
| `dist/bazel-cache-perf-ci-observation.json` | CI rollup for cache/perf |
| `docs/development/bazel-migration-evidence/perf-sample.json` | Checked-in ≥10-pair sample + gate results |
| `dist/cargo-bazel-parity-evidence.json` | Diagnostic dual-build parity (one release cycle) |

CI uploads the cache/perf set as `bazel-cache-perf-evidence-<run_id>`.

### Common failures

| Symptom | What to check |
| --- | --- |
| Drift check red | Re-run `--write` + `CARGO_BAZEL_REPIN=1` after intentional Cargo changes; commit lock/fingerprint |
| Ledger check red | Missing/unmapped row or `stub` retained exception — update map + justification |
| No remote cache hits | Org-admin enablement; fresh `--output_base` for warm observe; no competing `--remote_cache` |
| Cold build fails without cache | Bug — cache absence must not require repo/credential changes |
| Binding packaging mismatch | Ensure assembly consumes Bazel cdylib outputs; do not `maturin build` / `napi build` a second native graph in the smoke path |
| Need Cargo-only diagnosis | [bazel-migration-cutover.md](bazel-migration-cutover.md) rollback section |

## Cargo compatibility and rollback

- Keep using `cargo` locally for edit/build/test convenience; CI authority is Bazel.
- After intentional dependency changes, always refresh Bazel lock/fingerprint
  (commands above).
- For one release cycle after cutover, same-SHA dual-build parity remains a
  **diagnostic** under `Bazel Bootstrap`.
- Temporary CI rollback to Cargo `rust-test` is documented in
  [bazel-migration-cutover.md](bazel-migration-cutover.md). Prefer fixing Bazel
  root causes.

## CI and release runbooks

### Pull requests

1. Classifier: Rust changes enable `bazel=true` so `Bazel Bootstrap` runs.
2. Authoritative Rust tests: `bazelisk test //:ci_rust_tests`.
3. Also builds first-party libs, CLI, resources, release bins, binding smokes.
4. Fail-closed: drift, ledger, release-platform inventory, cache policy, strict
   perf `evaluate` against the checked-in sample.
5. Required aggregate remains exactly **`CI Gate`**; path-classified skips stay
   neutral.
6. PR Cargo sticky `target/` disks are retired; do not re-add without a
   documented rollback.

### Release / Binding RC

1. Prefer Bazel-built natives for packaging handoff; Binding RC still proves
   clean-install on certified OS/arch matrices.
2. Publish credentials and OIDC stay in release/publish workflows
   ([release-process.md](release-process.md), [PUBLISHING.md](../engineering/PUBLISHING.md)).
3. Do not claim M2 mobile-binding release surfaces.

## Security summary

- Pins + integrity: `.bazelversion`, `MODULE.bazel`, `MODULE.bazel.lock`.
- No secrets in cacheable actions; no in-repo remote-cache URL.
- Cross-branch cache reuse is safe only when Bazel action keys and declared
  inputs match.
- Details and AC pointers:
  [bazel-migration-ac-evidence.md](bazel-migration-ac-evidence.md).
