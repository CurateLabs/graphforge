# Bazel migration CI Gate cutover (#4)

Implements sequence step 9 of [#1](https://github.com/CurateLabs/graphforge/issues/1)
via child issue [#4](https://github.com/CurateLabs/graphforge/issues/4).

Companion artifacts:

- Orchestration: [bazel-migration-orchestration.md](bazel-migration-orchestration.md)
- Parity (#6): [bazel-migration-parity.md](bazel-migration-parity.md)
- Cache/perf (#5): [bazel-migration-perf.md](bazel-migration-perf.md)
- Ledger: [bazel-migration-ledger.md](bazel-migration-ledger.md)

## Cutover contract

| Piece | After #4 |
| --- | --- |
| Required check name | Exactly **`CI Gate`** (unchanged) |
| Authoritative Rust compile/test | `Bazel Bootstrap` → `bazelisk test //:ci_rust_tests` (+ libs/CLI/resources/bindings builds) |
| Retired | Cargo `rust-test` workspace job; PR job-isolated Cargo `target/` sticky disks |
| Retained Cargo diagnostics | `Rust Quality` (fmt/clippy); Windows `graphforge-storage` lock unit tests; PR maturin/napi binding assembly (no sticky); Binding RC macOS/Windows/cross napi + fuzz / release-certification sticky packaging lanes |
| Path-classified skips | Remain neutral via `require-gates.sh` (`success` or `skipped`) |
| Dual-build parity | Diagnostic under non-required `Bazel Diagnostics` for **one release cycle** |

Do **not** set `--remote_cache` in-repo. Blacksmith injects repository Bazel caching.

## What changed in CI

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

## Cargo diagnostic / rollback (one release cycle)

Use this if Bazel CI misbehaves and maintainers need Cargo as a temporary
authoritative path. Keep Cargo manifests and local `cargo` tooling regardless.

### Local diagnostic (no workflow change)

```bash
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace --no-fail-fast

python3 scripts/ci/cargo-bazel-parity-check.py \
  --mode all \
  --write-evidence dist/cargo-bazel-parity-evidence.json
```

### Restore Cargo `rust-test` under CI Gate (rollback)

1. Restore the `rust-test` job from git history prior to the #4 cutover commit
   (search `.github/workflows/test.yml` for `name: Rust Tests`).
2. Re-add `rust-test` to `ci-gate` `needs` and to `scripts/ci/require-gates.sh` args.
3. Optionally re-mount PR sticky disks for `rust-test` / `rust-lint` only
   (update `EXPECTED_STICKY_KEYS` / `EXPECTED_DEPENDENCY_KEYS` in
   `scripts/ci/test-ci-storage-policy.py` in the same change).
4. Keep required check name **`CI Gate`**. Do not invent a second required context.
5. Prefer fixing Bazel root causes; treat this rollback as temporary for one
   release cycle after cutover, then remove again once Bazel is healthy.

### Binding RC / publish sticky disks

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

## Acceptance mapping

| #4 / #1 AC | Evidence |
| --- | --- |
| Bazel authoritative under `CI Gate` | `Bazel Bootstrap` runs `//:ci_rust_tests`; `rust-test` absent from Test Suite / gate |
| Branch protection still requires exactly `CI Gate` | Job display name unchanged; no second required context |
| Cargo sticky disks retired without weakening gates | PR sticky keys gone; Binding RC/fuzz/release-certification retained; storage-policy tests updated |
| Documented Cargo rollback one release cycle | This document |
| Path-classified skips remain neutral | `require-gates.sh` still accepts `skipped` |

## Next

[#3](https://github.com/CurateLabs/graphforge/issues/3) docs/observability/#1
close-readiness: [bazel.md](bazel.md) and
[bazel-migration-ac-evidence.md](bazel-migration-ac-evidence.md).
