# Bazel migration parity (#6)

Same-SHA Cargo/Bazel dual-build parity and cross-platform release modeling for
M2 issue [#6](https://github.com/CurateLabs/graphforge/issues/6) / canonical
[#1](https://github.com/CurateLabs/graphforge/issues/1) step 7.

Orchestration: [bazel-migration-orchestration.md](bazel-migration-orchestration.md).
Ledger: [bazel-migration-ledger.md](bazel-migration-ledger.md).
Bootstrap: [bazel-bootstrap.md](bazel-bootstrap.md).

## What landed

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
| CI | `Bazel Bootstrap` dual-build steps; required check remains `CI Gate` |

## Acceptance mapping

| #6 / #1 AC theme | Evidence |
| --- | --- |
| Every mapped test/public contract same pass/fail on Cargo and Bazel at one SHA | `cargo-bazel-parity-check.py --mode all` writes `dist/cargo-bazel-parity-evidence.json`; Cargo `rust-test` + Bazel Bootstrap remain dual-build under `CI Gate` |
| Linux/macOS/Windows + Node cross-target release evidence | Platform inventory covers Binding RC contract + `napi.targets` (incl. `aarch64-unknown-linux-gnu`); host `//:release_bins` + binding smokes build under Bazel |
| Unmapped target / unjustified exception fails ledger | `bazel-migration-ledger-check.py` rejects `unmapped` rows and `stub` exceptions |

## Dual-build contract

- Cargo remains required for ordinary CI compilation/tests through #4 cutover.
- Bazel Bootstrap runs drift, ledger, release-platform inventory, smoke tests,
  release bins, binding packaging, and the dual-build parity suite.
- Required check name stays **`CI Gate`**. Do not make Bazel the sole path yet.
- Do **not** set `--remote_cache` (Blacksmith injects cache; enablement is #5).

## Local commands

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

## Retained Cargo tools (justified)

| ID | Status | Why retained |
| --- | --- | --- |
| RT-fuzz | justified | cargo-fuzz driver/corpus outside ordinary rules_rust graph |
| RT-publish-crates | justified | crates.io publish/auth metadata |
| RT-maturin-assemble / RT-napi-assemble | handoff | Assemble/sign/publish only; no silent native recompile |
| RT-examples | closed | All 11 examples mapped as Bazel binaries |
| RT-mobile | excluded | Abandoned for M2 |

## Next

1. [#5](https://github.com/CurateLabs/graphforge/issues/5) — Blacksmith Bazel Build
   Caching (org-admin enablement) + cold/warm performance gates.
2. [#4](https://github.com/CurateLabs/graphforge/issues/4) — `CI Gate` cutover and
   Cargo sticky-disk retirement after parity + perf evidence.
