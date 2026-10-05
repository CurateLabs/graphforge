# Benchmark measurement policy

GraphForge values correctness over performance, but performance claims still
require comparable evidence with explicit scope and provenance.

## Execution boundaries

Choose the framework by **execution boundary**, not an arbitrary duration cutoff:

| Boundary | Authority | Typical workloads |
| --- | --- | --- |
| Whole command / process tree | **BenchExec** | Certification ladder, lifecycle/GDC orchestration, external runners |
| In-process benchmark target | **Divan** (CodSpeed optional) | Parser, storage CPU simulation, traversal/MERGE scaling |
| Product semantic counters | **Direct deterministic evidence** | Logical bytes, reader calls, publication counts, ingest floor gates |
| Shared internal phase timing | **Diagnostic only** | BDD/TCK scenario timings, certify phase telemetry, construction receipts |

BenchExec owns command/process-tree timing and resource measurements. Divan owns
in-process benchmark sampling and timing. CodSpeed may execute and report Divan
benchmarks but is diagnostic, not a merge authority.

Every performance **gate** must consume framework-produced measurements for the
timing and resource quantities those frameworks own. Shared diagnostics cannot
substitute for gate input. Product semantic counters remain allowed when they are
distinctly named deterministic evidence, not relabeled physical resource
measurements.

Ordinary deadlines, cancellation tests, and approved shared diagnostics remain
allowed. `Instant` / `Duration` in product control paths are not banned globally.

## AssertionLedger merge comparison

The ignored test
`graphforge-knowledge::tests::quiet_host_assertion_merge_cost_measurement`
compares rebuilding the concatenated ledgers through `AssertionLedger::new`
with trusted `AssertionLedger::merge`. It checks exact output equality before
timing, then alternates baseline-first and candidate-first order over nine pairs
at each scale. The canonical input digests are pinned by the test:

| Existing assertions / references | Staged assertions / references | Canonical input SHA-256 |
| ---: | ---: | --- |
| 1,000 / 1,000 | 1 / 1 | `eac191d14d0f1088c9cf1b6f8e87df5e353b31dfa48de6490123cf177b2a0496` |
| 10,000 / 10,000 | 1 / 1 | `5553b15b4fc68dd53ddf0b071daa392bc7e04cb74d78b39178c5a7a102df1a03` |

Build into a dedicated target, then run `require_quiet_host` immediately before
measurement. The output reports every paired observation, medians, paired delta
range, and cost per existing row (assertion plus graph-reference rows):

```bash
CARGO_TARGET_DIR=/home/ubuntu/.cache/graphforge-target-1823-measurement \
  cargo test --release --locked -p graphforge-knowledge --lib \
  tests::quiet_host_assertion_merge_cost_measurement --no-run
source /home/ubuntu/.claude/gf-quiet-host.sh && require_quiet_host && \
  CARGO_TARGET_DIR=/home/ubuntu/.cache/graphforge-target-1823-measurement \
  cargo test --release --locked -p graphforge-knowledge --lib \
  tests::quiet_host_assertion_merge_cost_measurement -- \
  --ignored --nocapture --test-threads=1
```

This measures in-process merge work, not end-to-end workflow latency. The
trusted path still clones and sorts the merged rows and compares staged records;
it avoids the complete existing-row validation pass. Raw observations and the
executable digest belong on the issue or PR that produced them.

## ReasoningLedger merge comparison

The ignored test
`graphforge-knowledge::reasoning::tests::quiet_host_merge_cost_measurement`
compares rebuilding the concatenated records through `ReasoningLedger::new`
with trusted `ReasoningLedger::merge`. For each scale it checks complete ledger
equality before timing, then alternates baseline-first and incremental-first
order over nine pairs. The test prints the canonical input SHA-256, every sample,
and each median at 1,000 and 10,000 existing rows.

Build into an isolated target, then run the quiet-host guard immediately before
the measured command:

```bash
CARGO_TARGET_DIR=/home/ubuntu/.cache/graphforge-target-1828-measurement \
  cargo test --release --locked -p graphforge-knowledge --lib \
  reasoning::tests::quiet_host_merge_cost_measurement --no-run
source /home/ubuntu/.claude/gf-quiet-host.sh && require_quiet_host && \
  CARGO_TARGET_DIR=/home/ubuntu/.cache/graphforge-target-1828-measurement \
  cargo test --release --locked -p graphforge-knowledge --lib \
  reasoning::tests::quiet_host_merge_cost_measurement -- \
  --ignored --nocapture --test-threads=1
```

Record the current source digest with
`sha256sum crates/graphforge-knowledge/src/reasoning.rs`, the test executable
digest, exact command, input digests, paired samples, and medians on the issue.
This is an in-process merge measurement, not public API latency. The incremental
path still clones and sorts records and checks staged identities; it skips the
full revalidation of the existing rows.

## Construction syscall comparison

[`construction-syscall-comparison.py`](../../benchmarks/scripts/construction-syscall-comparison.py)
compares a frozen baseline and candidate through the current Graph500 S18/S20
profiles. Generate each input once with the frozen current generator, then share
those regular files between fresh projects. Registration arguments use absolute
input paths because the product refuses symlink sources. The five profile import
commands run under BenchExec with CPUs 0–15 and a 4000 MB memory limit. The driver
requests region diagnostics; optional allocation diagnostics remain disabled in
both modes. It provides no lifecycle-storage certification claim.

Build and archive both release executables before reserving the host. All input
generation and digest reads happen outside the timed workflow. Before each run,
input hashing verifies identical bytes and establishes the matched warm-input
policy. Required checks and barriers remain inside the complete ingest.

```bash
task_driver="$PWD/benchmarks/scripts/construction-syscall-comparison.py"
task_profile="$PWD/benchmarks/profiles/graph500/s18-local.json"
python3 "$task_driver" generate --profile "$task_profile" \
  --binary "$task_generator" --inputs "$task_inputs"
systemd-run --user --scope --slice=benchexec -p Delegate=yes \
  python3 "$task_driver" run --profile "$task_profile" \
  --binary "$task_baseline_gf" --inputs "$task_inputs" --run "$task_evidence/s18-a1"
```

Use task-owned absolute paths on the admitted ext4/xfs/btrfs volume, and repeat
with the candidate executable and `s20-provider.json`. Take three accepted pairs
per scale in A/B, B/A, A/B order, each with a distinct run directory. The parent
operator first records at least 60 seconds of CPU, I/O and process activity with
`vmstat -w 5`, `mpstat -P ALL 5`, `pidstat -durh -p ALL 5` and process-name samples,
reviews that quiet window, and keeps those observers plus a compiler-process
watchdog running throughout measurement. Boundary process checks alone do not
establish a quiet host. Preserve and exclude a contended or failed observation;
do not silently retry it into the accepted set.

After each workflow, the driver reopens the project and runs the profile's node
and edge recount plus ordered one-hop and two-hop queries. Compare all four
complete result digests and row counts across both modes; an absent digest is a
failure, while an absent scalar observation remains unavailable. Read complete
workflow wall/CPU/memory from `runexec.txt`. Run a separate S18 observation per
mode with `--trace`; its `validate.strace` gives `strace -f -c` syscall counts.
Keep traced runs out of the wall comparison.

Method input SHA-256 identities:

| Input | SHA-256 |
| --- | --- |
| Comparison driver | `a51cc92b75bf9dc99e567b025cfedee2a2a3ec4c9c726fd56fce45e2de81e05d` |
| S18 profile | `762fdac3d4ad790eaf1aa75348ccca692c709dcf578f91e1363f720afebc368b` |
| S20 profile | `b8af47526cad5641bfe85c8c68e507c46edf0de106f92bffd712e6ae90c9b59d` |
| S18 nodes.parquet | `44c9dfd9325013d0f6ea2f03bd86b00d2bea01265254cb28f70f0881a6478075` |
| S18 edges.parquet | `f112ccbec94875f36f113e9bdf3e6e7d88e3105bafbaeeb3a7cb42ac4883e9c4` |
| S20 nodes.parquet | `5792da943d39a3ec0cfe48c375fef1b078ae31f2c086af5d9bcfd417c74f24aa` |
| S20 edges.parquet | `3fb656aa9af568f12359c51fcb6a337460d0521ac07bd05def3a7e98f2e51086` |

These inputs use the profiles' edge factor 16 and seed 13907095936298285200.
The generator binary and generated Parquet hashes also appear in each input
`identity.json`. Raw receipts, counts, logs, per-run tables and binary provenance
attach to the producing issue/PR outside the repository.

## Inventory and enforcement

The inventory records each measurement site, its boundary, disposition, migration
owner, and any reviewed legacy signals still present. Dispositions:

- `framework_authority` — timing/resources come only from Divan or BenchExec.
- `framework_consumer` — outer orchestration consumes BenchExec evidence.
- `mixed_authority` — Divan plus reviewed nested product counters pending migration.
- `migrate` — custom in-process timers/statistics scheduled for Divan migration.
- `diagnostic_only` — informational timings that must not drive performance gates.

CI rejects new unclassified benchmark clocks, sampling loops, or homegrown
statistics in the scanned in-process benchmark surfaces (`crates/*/benches/`,
`crates/*/tests/bench_*`, traversal/MERGE scaling tests). Update the inventory
when adding a reviewed legacy exception or completing a migration; stale entries
fail closed.

### CodSpeed walltime raw results are Divan evidence

Maintainer decision on #1467 (2026-09-30): when a Divan target runs with
`CODSPEED_ENV` set, the per-benchmark walltime `raw_results` JSON that the
CodSpeed Divan integration writes
(`$CODSPEED_CARGO_WORKSPACE_ROOT/target/codspeed/walltime/raw_results/divan/*.json`)
is accepted as Divan evidence. Divan does the measuring; CodSpeed only
serializes the samples Divan collected. This does not make CodSpeed a merge
authority, and it does not admit any hand-written timer. Divan test mode
(`--test`, or `cargo test` on a bench target) runs each benchmark once and
writes no raw results, so it is never performance evidence.

### openCypher TCK scenario benchmark

`crates/graphforge-api/benches/tck_scenarios/` (#1653) is the in-process
per-scenario measurement boundary for the TCK. It parses the same ephemeral
normalized corpus as the Cucumber correctness run, and executes every step
through the step functions registered on `GraphForgeWorld` with the same pooled
fixture and clear-on-lease semantics. One timed iteration is a whole scenario,
including `Given an empty graph`, so fixture reset cost stays visible. Each
iteration's verdict is checked outside the timed region; a failing scenario
aborts the run before Divan records its timing. Benchmarks are named
`scenario[<feature>:<line>:<name>]`, the key `tests/tck/passing_baseline.txt`
uses. The default is 10 samples of one scenario execution each.

```bash
cargo bench -p graphforge-api --bench tck_scenarios -- --test      # run every scenario once, no timing
CODSPEED_ENV=local CODSPEED_CARGO_WORKSPACE_ROOT="$PWD" \
  cargo bench -p graphforge-api --bench tck_scenarios               # whole corpus, raw results
TCK_ONLY=Delete5 cargo bench -p graphforge-api --bench tck_scenarios # feature-file subset
```

`make bench-tck-scenarios` runs the whole corpus with the default sample count.

The target has no thresholds, baseline or comparison of its own. Its raw
results are the per-scenario input to `make tck-perf` (#1654), the single TCK
threshold consumer: it also measures the whole-TCK Cucumber process under
BenchExec for the aggregate, and compares only when every provenance key
(host, build profile and toolchain, workload, sample counts) matches a
host-local baseline. See `docs/reference/tck-compliance.md`. Divan orders
scenarios by name, not Cucumber file order, and `cargo bench` builds with an
optimizing profile while `cargo test` does not; both are provenance keys. The
bench is not part of the PR CI Gate:
`cargo test` and nextest do not run bench targets by default.
`tests/tck_scenario_bench.rs` covers it there, running the benchmark in
subprocesses: a passing scenario yields a keyed raw result, a failing step
aborts without one, and test mode writes none.

Functional benchmark checks (correctness, read counts, topology/I/O invariants)
stay in ordinary product CI. Comparable performance measurements use Divan
(`cargo bench`, CodSpeed) or BenchExec (native Linux cgroups-v2 hosts). Durable
temp-root and admitted-host requirements are documented per workload in
`benchmarks/README.md`.

# Benchmarking with CodSpeed

**Status:** Continuous on every pull request to `main` (`CodSpeed` workflow)

The `CodSpeed` workflow measures a fixed set of Rust benchmarks on every pull
request and reports the delta against the base commit. It is not part of the
`CI Gate` aggregate, but the PR must still reach the repository's required
`CLEAN` state: a red **CodSpeed Performance Analysis** check must be resolved or
receive an explicit, evidence-backed maintainer disposition.

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
cargo bench -p graphforge-storage --bench storage_kernels
cargo bench -p graphforge-storage --bench storage_io -- --sample-count 1
cargo bench -p graphforge-exec --bench traversal_scaling -- --sample-count 5
cargo bench -p graphforge-exec --bench merge_scaling -- --sample-count 5
```

## The ingest floor gate is a ratchet

`GF_INGEST_FLOOR_GATE=1 cargo bench -p graphforge-storage --bench
storage_io` runs the bulk-ingest gate instead of the divan benchmarks. Its
banked constants fail in **both** directions (#1476): a measurement past its
constant the wrong way is a regression, and a measurement beating its constant
by more than that metric's margin is an **unbanked gain** — the gate fails and
prints the exact constant to write, so an improvement cannot land without the
pull request that won it recording the new constant. Margins are per metric,
from each metric's recorded reproducibility (bytes and the growth ratio
reproduce to the byte: 10%; CPU: 25%; wall-clock throughput on its own runner:
40%). Each banked constant documents its host class, build profile and the
change that set it, and every metric records its execution scope, denominator
and units — do not transfer a number between scopes. The gate's judgment is
unit-tested in `tests/ingest_gate_verdict.rs`: a deliberate regression and a
deliberate improvement must each fail in the expected direction before a clean
pass is trusted.

Two of the four limits are **host-bound**. The throughput floor and the CPU
ceiling are banked from the isolated `codspeed-macro` runner the nightly runs
on, which is an ARM64 machine bound by its device rather than its CPU; its
numbers say nothing about an x86_64 development host, and the reverse. The
nightly declares the host with `GF_INGEST_GATE_BANKED_HOST=codspeed-macro` and
both limits are judged there. Run anywhere else, as in the command above, they
are printed as `not judged` notes and only the two deterministic byte-counter
limits can fail. Do not set that variable on another machine, and do not bank a
throughput or CPU constant from a local run: re-bank from scheduled nightlies
and put the table on the issue.

Per-region wall, CPU, fsync, and byte attribution for one import comes from the
stock receipt's `region_diagnostics` tree; see
[ingest-region-diagnostics.md](ingest-region-diagnostics.md) for how to read it.

Manual scaling studies also expose Makefile entry points (`make bench-traversal`,
`make bench-merge-scaling`). Divan test mode (`--sample-count 1`) exercises every
case without treating the output as performance evidence.

## Storage benchmark evidence

`storage_kernels` uses synthetic, versioned fixtures and the `1 / 100 / 10,000`
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
the CI Gate Rust tests, deterministic fault models, and native platform lanes are the
correctness authority. Material regressions must be repaired or documented with
their measured tradeoff; samples and thresholds must not be weakened.

The frozen pre-M6 comparison commit is
`aeb46d1b012d40e8a0af7873af9152b3aab752c6`, the first parent immediately
before the #777 replay merge. The walltime host contract is CodSpeed's
`codspeed-macro` ARM64 runner, Rust 1.96.0, `storage_io` fixture v1, and
CodSpeed walltime mode. The scheduled memory fallback remains a separately
labelled Blacksmith diagnostic and uploads `/usr/bin/time -v` peak-resident
output for replay and spill/compaction, named with the exact head SHA.
Certification #756 records the
base/head SHAs, result URLs or artifact IDs, benchmark mode and any accepted
tradeoff. `scripts/ci/check-storage-benchmarks.py` freezes the v1 names and count.

## Adding a benchmark

1. Add `divan = { workspace = true }` to the crate's `[dev-dependencies]` and a
   `[[bench]]` section with `harness = false`.
2. Keep each benchmark deterministic and bounded — no network, no wall-clock
   dependence, and inputs built outside the measured closure.
3. Benchmarks are not tests: the CI Gate Rust lane
   ([agent-environment.md](agent-environment.md#rust-test-gate)) stays the
   authoritative compile/test surface, and `cargo codspeed` is a
   diagnostics-only Cargo path.

## Related manual benchmarks

The scaling studies under `benchmarks/` (`make bench-traversal`,
`make bench-merge-scaling`, `make bench-embedded-performance`, and the fixed-hop LIMIT
matrices) remain hardware-specific manual evidence. They are unrelated to the
continuous CodSpeed lane.

## Confidence ledger merge measurement

The ignored `confidence::tests::quiet_host_confidence_merge_cost_measurement` test compares
the previous full-constructor merge with the incremental trusted-base merge. Fixture
construction, validation, sorting, and canonical input fingerprinting are outside the
timed interval. The test checks output equality before timing, alternates the order of
seven paired repetitions, and measures a staged assessment/input against bases of 10,000
and 100,000 assessments, each with one input per assessment. Report elapsed median time
and nanoseconds per existing assessment. Run on the guarded quiet host:

```bash
CARGO_TARGET_DIR=/home/ubuntu/.cache/graphforge-target-1825-measurement \
  cargo test --release --no-run --locked -p graphforge-knowledge
source /home/ubuntu/.claude/gf-quiet-host.sh && require_quiet_host && \
CARGO_TARGET_DIR=/home/ubuntu/.cache/graphforge-target-1825-measurement \
  cargo test --release --locked -p graphforge-knowledge \
  confidence::tests::quiet_host_confidence_merge_cost_measurement -- \
  --ignored --nocapture --test-threads=1
```

Canonical input digests (base rows followed by staged rows) are `da8bfd70ed1122f60fa9d9ad73112345244680c01c83fb34a495891a21323c3c` (10,000 assessments) and `a11eceb8f5cfa6a2f9c8bdfed3eda6374c6ae4faa7c46ae137e2f578aaf50d64` (100,000 assessments). The raw measured outputs and executable SHA-256 are on [issue #1825](https://github.com/CurateLabs/graphforge/issues/1825#issuecomment-5988658705).
