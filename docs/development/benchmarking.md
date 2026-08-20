# Benchmarking with CodSpeed

**Status:** Continuous on every pull request to `main` (`CodSpeed` workflow)

GraphForge values correctness over performance, but performance claims still
require comparable evidence. The `CodSpeed` workflow measures a fixed set of
Rust benchmarks on every pull request and reports the delta against the base
commit. It is not part of the `CI Gate` aggregate, but the PR must still reach
the repository's required `CLEAN` state: a red **CodSpeed Performance Analysis**
check must be resolved or receive an explicit, evidence-backed maintainer
disposition.

## What is measured

Benchmarks are [divan](https://github.com/nvzqz/divan) targets compiled through
the `codspeed-divan-compat` drop-in (the `divan` workspace dependency), so the
sources stay plain divan code.

`graphforge-core` — `benches/canonical.rs`

- `graphforge-canonical/1` record encode and strict decode (16/256/4096 rows);
- fingerprint preimage framing and full SHA-256 fingerprints (1 KiB – 1 MiB);
- the encode → fingerprint → UUIDv8 identity pipeline;
- the UUID v5 and v7 helpers used at the storage boundary.

`graphforge-cypher` — `benches/compile.rs`

- `lex`, `parse_ast`, `bind_ir`, and `parse_and_bind` across six query shapes
  (simple match, filtered traversal, aggregation, variable-length path, write
  pipeline, and a 64-branch `UNION ALL`);
- `parse_corpus`, one pass over the frozen 1.4k-query parser regression corpus
  (`crates/graphforge-cypher/tests/corpus/valid.json`).

Both targets run under the **CPU simulation** instrument: measurements are
instruction-level and hardware-agnostic, so a 4 vCPU shared runner still yields
comparable numbers between runs.

The Actions job **Rust Benchmarks** only builds and runs the suite; CodSpeed's
separate **Performance Analysis** check compares results to the base commit and
can fail independently of a green Actions job.

## Triaging Performance Analysis failures

Treat CodSpeed as a diagnostic signal, not a release or merge authority.

First verify that the report compares the intended base and head with the same
instrument and environment. Walltime evidence is valid only when both sides
come from `codspeed-macro`; an unknown/different hosted environment is an
infrastructure or baseline defect, not a regression disposition. Seed a fresh
`main` Macro Runner baseline, then run the changed PR head once against it.

For CPU simulation, tiny benchmarks can still be sensitive to the measurement
floor. Raise their signal-to-noise ratio inside the benchmark rather than
waiving a red check. When a comparable report remains red, investigate the
changed dependency surface and either repair the regression or document the
measured tradeoff through explicit maintainer disposition.

## Running them locally

```bash
make codspeed-build   # cargo codspeed build -m simulation (bench profile)
make codspeed-run     # codspeed run --mode simulation -- cargo codspeed run
```

`make codspeed-run` requires the [CodSpeed CLI](https://codspeed.io/docs/cli)
and an authenticated profile (`codspeed auth login`). Without the CLI you can
still execute the targets as ordinary divan benchmarks:

```bash
cargo bench -p graphforge-core --bench canonical
cargo bench -p graphforge-cypher --bench compile
cargo bench -p graphforge-storage --bench m6_storage
cargo bench -p graphforge-storage --bench m6_storage_io -- --sample-count 1
```

## M6 storage evidence

`m6_storage` uses synthetic, versioned fixtures and the `1 / 100 / 10,000`
operation ladder. GFDR framing, checksum verification, replay/merge fingerprints,
reachability, and transaction classification belong to CPU simulation; fixture
construction and correctness assertions stay outside timed closures.

Durable open, recovery, commit, garbage collection, spill, and compaction are
filesystem walltime measurements, not CPU-simulation claims. They run on
CodSpeed's dedicated `codspeed-macro` bare-metal ARM64 runner; shared hosted
runners are not a valid fallback because their environment variance makes
walltime comparisons incomparable. See CodSpeed's
[Macro Runner guidance](https://codspeed.io/docs/features/macro-runners) and
[walltime instrument contract](https://codspeed.io/docs/instruments/walltime).
Scheduled/manual hardware evidence records
the exact runner image, architecture, head/base SHA, fixture version, and
artifact URL. Until CodSpeed memory mode is available on
the project runner, replay and compaction peak-resident counters are emitted as
an explicit scheduled hardware artifact. CodSpeed remains diagnostic only:
Bazel tests, deterministic fault models, and native platform lanes are the
correctness authority. Material regressions must be repaired or documented with
their measured tradeoff; samples and thresholds must not be weakened.

The frozen pre-M6 comparison commit is
`aeb46d1b012d40e8a0af7873af9152b3aab752c6`, the first parent immediately
before the #777 replay merge. The walltime host contract is CodSpeed's
`codspeed-macro` ARM64 runner, Rust 1.96.0, `m6_storage_io` fixture v1, and
CodSpeed walltime mode. The scheduled memory fallback remains a separately
labelled Blacksmith diagnostic and uploads `/usr/bin/time -v` peak-resident
output for replay and spill/compaction, named with the exact head SHA.
Certification #756 records the
base/head SHAs, result URLs or artifact IDs, benchmark mode and any accepted
tradeoff. `scripts/ci/check-m6-benchmarks.py` freezes the v1 names and count.

## Adding a benchmark

1. Add `divan = { workspace = true }` to the crate's `[dev-dependencies]` and a
   `[[bench]]` section with `harness = false`.
2. Keep each benchmark deterministic and bounded — no network, no wall-clock
   dependence, and inputs built outside the measured closure.
3. Benchmarks are not Bazel targets: `//:ci_rust_tests` stays the authoritative
   compile/test surface, and `cargo codspeed` is a diagnostics-only Cargo path.
4. Adding a dev-dependency changes the Cargo feature graph, so refresh the Bazel
   drift fingerprint with
   `python3 scripts/ci/cargo-bazel-drift-check.py --write` and repin
   `cargo-bazel-lock.json`
   (`CARGO_BAZEL_REPIN=1 bazelisk build --repo_env=CARGO_BAZEL_REPIN=1 //:first_party_libs`).

## Related manual benchmarks

The scaling studies under `benchmarks/` (`make bench-traversal`,
`make bench-m4-entry`, and the fixed-hop LIMIT matrices) remain hardware-specific
manual evidence. They are unrelated to the continuous CodSpeed lane.
