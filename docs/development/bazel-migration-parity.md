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
| CI | `Bazel Bootstrap` (authoritative after #4) + diagnostic dual-build parity; required check remains `CI Gate` |

## Acceptance mapping

| #6 / #1 AC theme | Evidence |
| --- | --- |
| Every mapped test/public contract same pass/fail on Cargo and Bazel at one SHA | `cargo-bazel-parity-check.py --mode all` writes `dist/cargo-bazel-parity-evidence.json`; after #4, Bazel `//:ci_rust_tests` is authoritative and parity remains diagnostic for one release cycle |
| Linux/macOS/Windows + Node cross-target release evidence | Platform inventory covers Binding RC contract + `napi.targets` (incl. `aarch64-unknown-linux-gnu`); host `//:release_bins` + binding smokes build under Bazel |
| Unmapped target / unjustified exception fails ledger | `bazel-migration-ledger-check.py` rejects `unmapped` rows and `stub` exceptions |

## Dual-build contract

- After [#4](https://github.com/CurateLabs/graphforge/issues/4) cutover, Bazel
  `//:ci_rust_tests` is authoritative under `CI Gate`. Cargo `rust-test` is
  retired; see [bazel-migration-cutover.md](bazel-migration-cutover.md).
- Bazel Bootstrap runs drift, ledger, release-platform inventory, authoritative
  Rust tests, release bins, binding packaging, and diagnostic dual-build parity
  for one release cycle.
- Required check name stays **`CI Gate`**.
- Do **not** set `--remote_cache` (Blacksmith injects cache).

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

Cutover ([#4](https://github.com/CurateLabs/graphforge/issues/4)) and cache/perf
([#5](https://github.com/CurateLabs/graphforge/issues/5)) are landed. Docs /
#1 close-readiness: [bazel.md](bazel.md) and
[bazel-migration-ac-evidence.md](bazel-migration-ac-evidence.md).
