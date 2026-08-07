# #1 Bazel migration — close-readiness AC evidence map (#3)

Checked-in map from canonical issue
[#1](https://github.com/CurateLabs/graphforge/issues/1) acceptance criteria to
M2 child-issue evidence (PR / merge SHA / artifact). Produced by
[#3](https://github.com/CurateLabs/graphforge/issues/3) so M2 gate
[#2](https://github.com/CurateLabs/graphforge/issues/2) can close when children
are complete.

Orchestration: [bazel-migration-orchestration.md](bazel-migration-orchestration.md).
Developer guide: [bazel.md](bazel.md).

**Cutover SHA (authoritative CI after #4):**
`75a33e5cf6d9dab1407eba719e98740c95426d91` ([PR #427](https://github.com/CurateLabs/graphforge/pull/427)).

## Child issue → merge evidence

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
| [#3](https://github.com/CurateLabs/graphforge/issues/3) | Docs / observability / this map | *(this PR)* | *(fill at merge)* |

## #1 acceptance criteria → evidence

| #1 AC | Status | Child | Evidence pointer |
| --- | --- | --- | --- |
| Checked-in migration ledger for all Cargo targets and every CI/release build command | Met | #12 (+ updates #11–#6) | [bazel-migration-ledger.md](bazel-migration-ledger.md); `tools/bazel/parity/migration_target_map.json`; `scripts/ci/bazel-migration-ledger-check.py` |
| Bazel builds all 17 first-party packages without shelling out to Cargo for ordinary compilation or tests | Met | #11–#9, #8, #7 | [bazel-bootstrap.md](bazel-bootstrap.md); `//:first_party_libs`, `//:binding_cdylibs`, `//:ci_rust_tests`; merge SHAs above |
| All 53 Rust integration tests, crate unit tests, doctest equivalents, BDD, snapshots, public-surface gates under mapped Bazel test graph | Met | #8, #6 | Ledger + `//:integration_tests` / `//:unit_tests` / `//:snapshot_tests` / `//:bdd_tests` / `//:ci_rust_tests`; [bazel-migration-parity.md](bazel-migration-parity.md) |
| Bazel-built Python wheels and Node packages pass clean-install, no-fallback, parity, persistence/reopen, structured-error suites | Met | #7, #6 | `//:python_wheel_smoke` / `//:node_package_smoke`; `scripts/ci/assemble_bazel_binding_packages.py`; Binding RC still consumes natives (no silent Cargo recompile of a different graph) |
| Linux, macOS, Windows, and supported Node cross-target release evidence remains complete | Met | #6 | `tools/bazel/release/release_platforms.json`; `//platforms:*`; Binding RC contract unchanged |
| Cargo and Bazel dependency/feature graphs cannot drift silently | Met | #11 | `scripts/ci/cargo-bazel-drift-check.py`; `tools/bazel/drift/cargo_feature_fingerprint.json`; `cargo-bazel-lock.json` |
| Repeated identical-SHA builds report remote cache hits; source change reruns only actions whose declared inputs changed | Met | #5 | [bazel-migration-perf.md](bazel-migration-perf.md); [perf-sample.json](bazel-migration-evidence/perf-sample.json); `affected-inputs` harness mode |
| Blacksmith cache disablement or eviction produces a correct cold build without repository changes | Met | #5 | `bazel-cache-perf.py --mode cold-correctness`; observations in perf sample |
| Across ≥10 paired runs: warm PR p50 ≥30% faster and compute ≥25% lower than Cargo baseline; cold p50 regression ≤10% | Met | #5 (#12 baseline) | [bazel-migration-baseline.md](bazel-migration-baseline.md); `perf-sample.json` `status=complete` + strict `evaluate` |
| No secret, token, signing material, publish credential, or user data in a cacheable Bazel action or build log | Met | #7, #5, #3 | See [Security and supply chain](#security-and-supply-chain) below; publish OIDC/credentials stay in release workflows outside `Bazel Bootstrap` |
| Required check context remains `CI Gate`; path-classified skips remain neutral | Met | #6, #4 | [bazel-migration-cutover.md](bazel-migration-cutover.md); `scripts/ci/require-gates.sh` |
| Cargo CI compilation and sticky build disks removed only after same-SHA parity and performance gates | Met | #4 (after #6/#5) | PR sticky `target/` keys retired; Cargo `rust-test` removed; Binding RC / fuzz / M1 retained as justified |
| Developer, architecture, build, release, troubleshooting, and cache-observability documentation is current | Met | #3 | [bazel.md](bazel.md) + companions listed there; this evidence map |

## #1 Documentation checklist

| #1 Documentation topic | Canonical doc |
| --- | --- |
| Build-system architecture and ownership | [bazel.md](bazel.md) § Architecture and ownership; [ARCHITECTURE.md](../engineering/ARCHITECTURE.md) |
| Bazel/Bazelisk installation and local commands | [bazel.md](bazel.md) § Install; [bazel-bootstrap.md](bazel-bootstrap.md) |
| Adding crates, dependencies, features, tests, fixtures, generated inputs | [bazel.md](bazel.md) § Extending the graph |
| Python/Node packaging handoff | [bazel.md](bazel.md) § Packaging handoff; [bazel-bootstrap.md](bazel-bootstrap.md) |
| Blacksmith Bazel cache enablement, metrics, eviction, troubleshooting | [bazel-migration-perf.md](bazel-migration-perf.md); [bazel.md](bazel.md) § Cache and troubleshooting |
| Cargo compatibility and rollback | [bazel-migration-cutover.md](bazel-migration-cutover.md); [bazel.md](bazel.md) § Cargo compatibility |
| CI and release runbooks | [bazel.md](bazel.md) § CI and release; [bazel-migration-cutover.md](bazel-migration-cutover.md); [release-process.md](release-process.md) |

## Observability evidence paths

| Signal | Path / artifact |
| --- | --- |
| Per-run representative build log | CI `dist/bazel-representative-build.log` (uploaded via cache/perf artifact when present) |
| Per-run machine-readable process summary | `dist/bazel-representative-build.summary.json` |
| Warm observation | `dist/bazel-warm-observation.json` |
| Affected-input probe | `dist/bazel-affected-inputs.json` |
| CI observation rollup | `dist/bazel-cache-perf-ci-observation.json` |
| Checked-in ≥10-pair sample | [bazel-migration-evidence/perf-sample.json](bazel-migration-evidence/perf-sample.json) |
| Diagnostic dual-build parity (one release cycle) | `dist/cargo-bazel-parity-evidence.json` |
| Blacksmith Cache dashboard | https://app.blacksmith.sh/cache |
| Authoritative Rust test log | `dist/bazel-ci-rust-tests.log` |

See also [OBSERVABILITY.md](../engineering/OBSERVABILITY.md) (Bazel CI signals).

## Security and supply chain

Confirmed against the post-#4 tree (cutover SHA above):

| Constraint | Evidence |
| --- | --- |
| Pin Bazel, rules, toolchains, external archives with integrity hashes | `.bazelversion` (`9.2.0`); `MODULE.bazel` (`rules_rust` `0.73.0`, Rust `1.96.0`); `MODULE.bazel.lock` `registryFileHashes` / archive `sha256` |
| Third-party Rust deps from reviewed Cargo lock | `crate.from_cargo` + `Cargo.lock` + `cargo-bazel-lock.json` |
| No `--remote_cache` in repo / workflow (Blacksmith injects) | `.bazelrc`; `Bazel Bootstrap` comments; `bazel-cache-perf.py --mode policy` |
| OIDC / npm / PyPI / signing credentials outside cacheable Bazel actions | Publish/release workflows only; `Bazel Bootstrap` has no `NODE_AUTH_TOKEN` / PyPI / signing secrets; packaging smoke assembles from Bazel cdylibs without registry auth |
| Network access restricted where practical | Ordinary `rules_rust` compile/test actions are sandboxed; crate_universe fetch is lockfile-bound; no in-repo remote-cache URL |
| Cross-branch cache reuse only via action key + declared inputs | Bazel remote-cache semantics; documented in [bazel-migration-perf.md](bazel-migration-perf.md) |
| No user data / sensitive fixtures uploaded as cache payloads | Test fixtures are hermetic source inputs; CI logs use `--test_output=errors` |

## Explicit non-deliverables (M2)

- **Mobile bindings** (Swift, Kotlin, UniFFI, XCFramework, JVM JAR/AAR) are
  **abandoned for M2** — ledger row `RT-mobile` is `excluded`. Do not treat
  product roadmap “planned UniFFI” notes as M2 Bazel migration deliverables.
- M3 peer-extension and M4 embedded-performance epics are out of scope.

## Gate close notes

- Close [#3](https://github.com/CurateLabs/graphforge/issues/3) when this map and
  [bazel.md](bazel.md) land with green CI on the docs PR and the table’s #3 merge
  SHA is filled in (or linked from the closing PR).
- Close [#2](https://github.com/CurateLabs/graphforge/issues/2) only when #13–#3
  are closed with ordinary AGENTS.md evidence.
- Close [#1](https://github.com/CurateLabs/graphforge/issues/1) when its AC
  outcomes are met via this map (no release-gate cascade required for ordinary
  close).
