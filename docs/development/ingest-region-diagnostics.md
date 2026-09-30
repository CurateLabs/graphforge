# Ingest region diagnostics

Stock `gf --json --diagnostics import-session ...` receipts include
`region_diagnostics`. Ordinary commands leave optional lifecycle I/O, storage
read counters, construction diagnostics, and region timing disabled; an absent
observation is unavailable, never a measured zero. The explicit
`storage-attribution` command and `--allocation-diagnostics` also request their
collectors. Required receipt identities, allocation accounting, recovery
authority, and durability barriers run independently of these observers.
Import operation timings are also optional: their getter returns `None` without
capture and commands omit them or return `null`. Required progress elapsed time
and cancellation checks retain their normal clocks. Custom property-scan
execution metrics are collected only when requested; an explicit execution-demand
capture enables them while resource reservations and decoder limits remain active
for every query.
Storage APIs that explicitly return exact `PropertyOverlayMetrics` continue to
collect the work promised by that return value. Ordinary facade scans and
targeted reads use internal data-only paths; they do not allocate or update
those optional read and authentication counters. Decoder admission, retained
buffer ownership, and header bounds are still enforced on both paths.

No test feature or custom engine build is needed. Rust callers request lifecycle
and storage read measurements with `graphforge_api::LifecycleIoCapture::install()`.
Keep that guard alive around the operation, and read
`graphforge_api::lifecycle_io_snapshot()` before dropping it. The snapshot is
`None` without a requested, valid capture. Each operation owns its counters;
worker jobs and deferred readers carry the originating capture and nested guards
restore the previous one. Region timing has its own explicit
`graphforge_api::concurrency_attribution::RegionCapture::start` guard;
finish it after its nested guards have dropped. Captures are thread-bound,
non-durable, and contain static region names and counters, never source paths or
row payloads. A later status call measures that call; it does not replay prior work.

Each region reports `inclusive` measurements and a `residual` after subtracting
its immediate children. Sum residual **wall** times to reconcile with the root.
Do not sum inclusive rows. Parent and child regions deliberately overlap.
Misordered guards invalidate the capture (`complete: false`, no rows), rather
than returning plausible measurements assigned to the wrong stage. There are at
most 256 distinct paths and 16 nesting levels; exceeding either invalidates it.

## Units and boundaries

- `process_cpu_ns / wall_ns` is effective **cores**, not CPU-busy fraction,
  throughput speedup, or serialized fraction. It is the time-weighted amount of
  process CPU execution, at Linux `/proc`'s 10 ms CPU resolution. It includes all
  process threads, including unrelated embedding-host work. Captured region
  paths describe the calling thread; worker scopes are not assigned duplicate
  process CPU. The explicitly captured phase `snapshot()` also contains
  inclusive totals and must not be summed across overlapping phases.
- `thread_running_ns` measures calling-thread execution; `thread_runnable_ns`
  measures time waiting to run. They do not measure pool occupancy.
- `thread_sleeping_ns` measures completed non-runnable sleep, including
  uninterruptible blocking. `thread_uninterruptible_ns` is a **subset**;
  `thread_iowait_ns` is a further I/O-wait subset. Never add these three values.
  None of them measures whole-process inactivity or PSI `some`.
- `fsync` and `lock_wait` paths identify observed directory/membership-sync and
  blocking file-lock calls. Their scheduler counters separate running, runnable
  delay, and sleeping inside that call. They do not classify all process I/O:
  automatic cache-writer barriers, worker-thread syscalls, mutexes, and other
  uninstrumented operations remain in their enclosing region/residual.
- `thread_unknown_ns` is elapsed time remaining after available running,
  runnable, and sleeping observations. Missing counters stay `null`, and their
  time remains unknown. A negative counter difference or inconsistent boundary
  sample yields `null`, not a clamped claim of zero. Sampling is sequential:
  `sampling_uncertainty_ns` bounds the read windows, in addition to CPU tick
  quantization. Residual uncertainty adds parent and child windows.

Scheduler delay and sleep counters require Linux scheduler statistics enabled
by the operator (`kernel.sched_schedstats=1`). The library never changes this
setting. Linux kernels that lack a counter, or platforms without these proc
interfaces, report that counter unavailable. The sleep/block accounting and
units follow [Linux scheduler accounting](https://github.com/torvalds/linux/blob/master/kernel/sched/stats.c)
and [scheduler debug output](https://github.com/torvalds/linux/blob/master/kernel/sched/debug.c).

## Complete ingest and useful work

The certification runner adds `graphforge-workflow-timing/1` around the complete
five-command ingest workflow: begin, both source registrations, validate, commit.
It includes child startup, facade open, command handling, and command cleanup.
It reports runner CPU and reaped child CPU separately from the same workflow
boundary. These counters are shared with any unrelated work in that runner, so
use a dedicated runner process. The outer lifecycle storage observation happens
after this boundary, matching existing phase-wall semantics.

`lifecycle_runtime` reports the workflow CPU/wall, disjoint command-root totals,
and their wall/CPU residuals. The residual includes work outside the import
command handler, such as startup and facade opening. CPU residuals are signed
because separate proc samples can differ by a tick. Within a command,
`resume_import`, `register_parquet`, `stage+seal`, `open_construction`, `seal`, and
`commit/publish` retain their own explicit residuals. Existing operation timings
remain the five disjoint construction-call observations; their boundaries are
narrower than command scopes.

`commit/publish` is decomposed into sequential children so publication can be
budgeted inside complete ingest (#1481): `prepare_encoding` (reopening the
encoded inventory and reclaiming superseded payloads), `publication_authentication`
(inventory control, parent generation, manifest, retained-artifact and route
authority checks), `cas_install` (appending authenticated graph objects to the
content-addressed store), `publication_intent` (the durable intent record),
`generation_commit` (staging the generation and committing `CURRENT`),
`publication_receipt` (authenticating the published target and recording the
receipt), `hydration` (materializing the reader workspace) and
`read_authority` (runtime catalog, property inventory, ordinal handle and
adjacency provider). The residual of `commit/publish` is the visibility swap
plus uninstrumented time between those children. Boundaries to keep in mind
when reading the numbers: `cas_install` opens before the route-table authority
read (a few kilobytes) that the append needs, so that read counts as install;
an idempotent replay of an already-published session skips publication and
reports `hydration` and `read_authority` as siblings of `publication_receipt`.
For a fresh publication, reader preparation runs against the durable candidate
inside `generation_commit`, immediately before `CURRENT`: the receipts nest
`hydration` and `read_authority` under `generation_commit`, and a preparation
failure leaves `CURRENT` unchanged (fail-closed) instead of committing a
generation that cannot be hydrated. Candidate verification before that callback
(the durable manifest and lease authentication) remains in `generation_commit`'s
residual. Adjacency CSR encoding runs
inside `stage+seal/seal/canonical_encoding/adjacency_encoding`, not inside
publish; reconcile publication against that region rather than assuming CSR
cost lands in `commit`. Measured attribution on the integrated tree is recorded in
[`evidence/publication-attribution-1481.md`](https://github.com/CurateLabs/graphforge/blob/29a7b34ebe441a85ffb9274164d58aaeeb68dc8a/docs/development/evidence/publication-attribution-1481.md).

Registration reports successfully owned bytes (Arrow registration reports rows),
append reports accepted rows, and successful new shaping/encoding reports the
completed graph's node/edge counts. Reused artifacts do not claim new useful work.
Rates use that stage's own wall time. These are different populations; do not add
registration, append, shape, and encoding counts together.

A single receipt cannot establish speedup; its matched speedup is unavailable.
For two controlled worker-count runs, the comparison entrypoint is:

```bash
PYTHONPATH=benchmarks/harness python3 -m graphforge_bench.region_diagnostics \
  baseline.json candidate.json --scope import_command/stage+seal/seal/shaping --unit edges
```

Each file wraps the unmodified receipt as `receipt` plus `provenance` containing
`build`, `input`, `host`, `cache`, `resource_policy`, `workers`, and `processes`.
The experiment must provide truthful identities and configured worker counts;
the tool does not infer them from CPU use or pool capacity. It requires identical
provenance and useful work, one process in both runs, and one versus N workers.
It reports wall speedup separately from each run's CPU/wall. Stock construction
currently has no public per-stage worker override; use the existing controlled
worker experiment harness when supplying such pairs. N independent processes
cannot satisfy this comparison.

## Calibration

Deterministic tests cover nesting, guard misuse, unavailable counters, old/new
kernel field names, units, and exact residual reconciliation. Timing calibration
runs separately on a quiet host so competing tests cannot supply process CPU:

```bash
cargo build -p graphforge-storage --release --example region_controls
# With scheduler statistics enabled by the operator; select one allowed CPU:
taskset -c CPU target/release/examples/region_controls
```

The example asserts approximately one effective core for CPU work, at least
150 ms of observed sleep in a known 200 ms delay, and over 100 ms of runnable
scheduler delay for two CPU-bound threads pinned to one CPU. It fails when the
required observation is unavailable; it never substitutes a slow fsync for the
fixed-delay control. Restore the host's original scheduler-statistics setting
after an experiment.

The stock release example passed on 2026-09-19 with one allowed CPU selected
and scheduler statistics enabled for the experiment (then restored):

| Control | Wall | Process CPU/wall | Calling-thread observation |
| --- | ---: | ---: | --- |
| CPU loop | 400.17 ms | 0.950 cores | 384.48 ms running |
| Known 200 ms wait | 200.48 ms | 0.000 cores | 200.05 ms sleeping |
| Two busy threads on one CPU | 400.18 ms | 0.975 cores | 203.82 ms runnable delay |

These are calibration observations, not ingest performance thresholds. In the
last row process CPU includes both threads; the scheduler row observes only the
capturing thread.

## Digest inventory and isolated read-path accounting

The digest census method is `scripts/development/digest-census.py`. Run it on
an identified source tree, writing results outside the repository:

```bash
python3 scripts/development/digest-census.py --repo . --output /tmp/gf-digest-census
```

The method and reviewed classification inputs are pinned by SHA-256:
`digest-census.py` is
`066813e6666c1a77782402cee7ae957b450ccd47a7e14874c4e47b996aabb0eb`;
`digest-census-overrides.json` is
`7e6904dfc1a3cbcf9b06bcd69e046aa7ac3b40c4b1cfa88c1ea3d8b48f236553`.
Run the parser and stale-review regression fixtures with
`python3 scripts/development/test-digest-census.py`. Reviewed function bodies
are pinned individually; changed inputs, added producers in the same function,
missing review pins, and unknown digest algorithms make a strict run fail.
Refresh a classification only after reviewing its actual inputs and consumers.

The method records the source revision, source-file SHA-256 digests, the working
diff digest when present, and digests of the method and semantic override inputs.
Post the generated producer/delegate inventories and review disposition on the
owning issue. Refresh after the final source edit. Results and per-run tables do
not belong in this page or elsewhere under `docs/`.

Production producer sites exclude test-only items and descendants. A constructor
or static digest invocation is a producer site; calls into a digest-owning helper
form a separate delegate population. Do not add wrapper levels to infer runtime
passes. The semantic input and consumer, rather than an alias name, determine
classification: durable artifact/trust-boundary work, contract identity, or
optional evidence. Required control authentication is recorded separately within
the trust-boundary category. Mixed helpers list their input-specific callers.
The method refuses unresolved or stale classifications by default and records
its lexical limitations. Resolve remaining candidates against current source;
a text match alone is neither a runtime hash pass nor proof of exhaustiveness.

`graphforge_core::hash_observation::operation::Capture` is test-only scoped
accounting. It counts actual SHA update input bytes by artifact payload,
contract identity, control authentication, optional evidence, and unclassified
producer; actual XXH64 input is counted separately. Worker jobs capture and
attach the current operation context. Nested operations and unrelated parallel
tests retain separate collectors; there is no process-wide reset. Production
context wrappers are zero-sized when test support is disabled.

The actual facade regressions are in
`crates/graphforge-api/tests/facade_checksum_admission.rs`, identified by SHA-256
`0a9ac8fa6785ad0702e3de0be34f6d461ad122e6e32331ee4617377a9f4ddc56`.
They include a nonempty published semantic binding participant, qualified
property writes, fresh durable reopen and an actual qualified query. Required
ontology, schema and route identities are measured separately from graph payload.
Run them with the admitted test environment:

```bash
python3 scripts/test_environment.py -- cargo test -p graphforge-api --test facade_checksum_admission
```

Default read/query assertions require both artifact-payload and unclassified
SHA bytes to be zero. Nonzero checksum bytes prove that payload admission still
runs. Bounded control authentication and existing logical identity commitments
remain distinct work. Same-inode, same-length mutation tests target the owning
admission boundary. A full import/query/export accounting test additionally
bounds payload SHA input by actual published/exported bytes; standalone region
hash totals mix domains and cannot prove that bound.

## Written and hashed bytes and barriers

Stock `gf --json --diagnostics import-session validate --session-uuid UUID` stages and seals
sources. Its outcome and measured region are `stage+seal`; `validate` remains
the CLI command. `status` uses the same outcome after sealing. The persisted
`ImportPhase::Validated` state and its existing binding labels remain readable;
they describe the durable lifecycle state rather than the measured phase.

New receipts use `graphforge-region-diagnostics/2`. The certification reader
and schema also accept `/1` because the retained encoding-lane receipt fixture
and historical ladder receipts are consumers of that contract. Old receipts
keep their original names and have no invented byte or barrier observations.

Every region's `inclusive` and `residual` measurements include:

- `written_bytes`: Linux `/proc/self/io` `wchar` differences, bytes accepted by
  write syscalls on all process threads. This includes control and payload
  writes and any other writes in the process, including pipes. It is neither
  disk writeback nor the logical length of newly published objects. It is
  unavailable (`null`) when the kernel counter cannot be read.
- `hashed_bytes`: input bytes supplied to instrumented SHA-256 sites in storage,
  API, core canonical identities, and ontology compilation/composition.
  Repeated input counts repeatedly; finalization padding does not count as
  payload. The ordinary digest implementation and digest bytes are unchanged.
- `hash_elapsed_ns`: the sum of SHA update/finalize elapsed intervals across
  process threads, excluding counter updates. Concurrent intervals overlap;
  this is accumulated hashing effort, not a disjoint critical-path region or
  process CPU time. Instrumentation has overhead, so it is not a speedup proof.
- `fsync_calls` and `fsync_elapsed_ns`: attempted file/data/directory barriers
  and their accumulated elapsed intervals, including failed calls. They cover
  observed storage/API file barriers, stable-directory barriers and cache-writer
  barriers. They are not inferred from the number of named `fsync` scopes.

Counters activate only during explicit region capture and never reset another
capture's totals. Region boundary differences include workers even though
worker region trees are not captured. Like process CPU, these totals require
one purpose per process: simultaneous unrelated tasks contaminate attribution.
The `io_scope` field states their shared process scope. Sum disjoint residuals,
not inclusive parents and children; unknown or inconsistent differences stay
`null`. Useful-work counters remain a separate population.

`source_read` covers iterator decode/canonicalization calls; `normalization`
covers bounded normalization windows. Appends are split into `append_nodes`
and `append_edges`; `manifest_persistence` covers complete manifest checkpoints
including their barriers and installs. Import progress journal work uses the
disjoint `journal_append`, `journal_sync`, and `journal_namespace_publication`
leaves. `source_publication` covers the source rename and its retained parent
barrier. Seal, encoding, hydration and commit retain
their existing boundaries. Reader setup and uninstrumented work stay in the
reported residual; do not call the entire residual hashing or source reading.

The baseline S22 input identities are `nodes.parquet`
(`bcbcbea526e61ceb63f6006ee5f56de6bb4f74cffdd68dc6eff6d230d3897f06`)
and `edges.parquet`
(`1c0ff75485f75e904cbd59b6f5d42da1d8b1af6ddac59ee6c495a4948a428d13`).
They represent 4,194,304 input nodes and 67,108,864 input edges.

For a fresh successful S22 construction, independently reconcile the leaf
`import_command/stage+seal/seal/shaping/shape_routing/artifact_authentication`
against the retained `receipt-NNNNNNNNNNNNNNNNNNNN.json` chunk receipts under
`.graphforge-construction/`. Require contiguous accepted sequences starting at
zero and reconcile receipt rows by kind with the input node/edge counts. That
leaf authenticates each chunk's Parquet artifact once: its `hashed_bytes` must
equal the sum of `parquet.bytes`, with zero-byte tolerance, and its call count
must equal the number of accepted chunk receipts. Normal publication retains
these small receipts, so collect them after measurement. Original source file
sizes and aggregate application reads are different populations; the latter
also includes metadata reads outside this leaf.

To reproduce S22, use those digest-pinned inputs and the baseline host. Build an ordinary release CLI with an isolated
`CARGO_TARGET_DIR`, record the source revision and binary/input SHA-256 digests,
then finish all builds before measuring. Set `TMPDIR` and the new project to
native storage on the process-root volume. Check for compiler/benchmark
processes and low load, establish a quiet window, and sample process names/load
throughout the five-command begin/register-nodes/register-edges/validate/commit
workflow under `runexec --no-container --cores 0-15` on the baseline host.
A contended run cannot establish the priority decision.

Keep each command's JSON receipt and the runexec output outside `docs/`; attach
them to #1623 or its PR. The snapshot lives at `receipt.region_diagnostics`.
Report wall/CPU, CPU divided by wall, written/hashed bytes and barrier counts
per region, along with the parent-minus-immediate-children residual. Budget
shares use the S22 edge count divided by 1,000,000 edges/s. Compare accumulated
hash/barrier effort with the disjoint construction regions and disclose its
scope when recording #735's conditional priority decision. Reopen and recount
the published project before claiming a successful ingest.

The deterministic worker-thread counter and residual regression runs with:

```bash
cargo test -p graphforge-storage --test region_work
cargo test -p graphforge-cli --test portable import_operation_timings_survive_separate_cli_processes
```

## Matched import journal measurements

The [lane runner](../../scripts/development/import-journal/measure-lane.py),
[five-command driver](../../scripts/development/import-journal/driver.sh),
[comparator](../../scripts/development/import-journal/compare-pair.py), and
[qualification contract](../../scripts/development/import-journal/measurement_contract.py)
reproduce a journal-only comparison. Use the immediate integrated `main` as
baseline; the candidate may differ in only the three import-session Rust files.
Historical runs do not supply a matched baseline. Both lanes use the input
digests and populations above, the same release profile/features, explicit
`--json --diagnostics`, cores 0–15, and independent fresh projects on supported
native storage. All artifacts and build targets stay outside both checkouts.

Build each lane after fixing `task_worktree`, `task_target`, `task_artifacts`,
and the absolute `task_methods` path to `scripts/development/import-journal`.
Run the following from the same build environment for both lanes. This records
the actual build command, profile, feature settings, toolchain, source and
binary identities, and the whitelisted build environment before measurement:

```bash
mkdir -p "$task_artifacts"
PYTHONPATH="$task_methods" CARGO_TARGET_DIR="$task_target" \
  CARGO_BUILD_JOBS=4 CARGO_PROFILE_RELEASE_DEBUG=0 \
  python3 - "$task_worktree" "$task_target" "$task_artifacts" <<'PY'
import os
from pathlib import Path
import subprocess
import sys
from measurement_contract import BUILD_ENV, digest, require_external_output, write_json

tree, target, artifacts = (Path(value).resolve() for value in sys.argv[1:])
require_external_output(artifacts, tree, Path(os.environ["PYTHONPATH"]))
require_external_output(target, tree, Path(os.environ["PYTHONPATH"]))
def command(arguments):
    return subprocess.check_output(arguments, cwd=tree, text=True).strip()
source = command(["git", "rev-parse", "HEAD"])
subprocess.run(["git", "diff", "--quiet", "HEAD"], cwd=tree, check=True)
arguments = ["build", "--locked", "--release", "-p", "graphforge-cli", "--bin", "gf"]
metadata = {
    "source_sha": source, "profile": "release", "features": [], "default_features": True,
    "target": next(line[6:] for line in command(["rustc", "-vV"]).splitlines()
                   if line.startswith("host: ")),
    "rustc_version": command(["rustc", "-vV"]), "cargo_version": command(["cargo", "-V"]),
    "cargo_args": arguments, "build_environment": {key: os.environ.get(key) for key in BUILD_ENV},
}
with (artifacts / "build.log").open("w") as log:
    subprocess.run(["cargo", *arguments], cwd=tree, check=True,
                   stdout=log, stderr=subprocess.STDOUT)
assert source == command(["git", "rev-parse", "HEAD"])
subprocess.run(["git", "diff", "--quiet", "HEAD"], cwd=tree, check=True)
metadata["binary_sha256"] = digest(target / "release/gf")
write_json(artifacts / "build-provenance.json", metadata)
(artifacts / "build-source.sha").write_text(source + "\n")
PY
```

Finish all builds and tests before launching lanes. Set `task_evidence` to an
external output directory, `task_nodes` and `task_edges` to the absolute S22
input paths, and create `inputs.sha256` there with the two pinned digests and
those exact absolute paths in `sha256sum` format. For each lane, set
`task_lane` to `baseline` or `candidate`, its worktree/target/build artifacts,
and the same positive `task_pair` number, then run sequentially:

```bash
python3 "$task_methods/measure-lane.py" --lane "$task_lane" --pair "$task_pair" \
  --worktree "$task_worktree" --binary "$task_target/release/gf" \
  --expected-source-sha "$(cat "$task_artifacts/build-source.sha")" \
  --build-source-file "$task_artifacts/build-source.sha" \
  --build-provenance-file "$task_artifacts/build-provenance.json" \
  --evidence-root "$task_evidence" --inputs-sha-file "$task_evidence/inputs.sha256" \
  --nodes "$task_nodes" --edges "$task_edges"
# Compare only after both baseline and candidate lanes finish:
python3 "$task_methods/compare-pair.py" --pair "$task_pair" \
  --repository "$task_worktree" --evidence-root "$task_evidence"
```

The runner resets cold caches and requires passwordless operator permission
for `drop_caches`; schedule it on an exclusively available host. It persists
a 60-second quiet window (mean ≤0.2 and peak ≤0.5 busy cores), samples process
names every five seconds, and refuses observed compiler overlap. Sampling does
not prove absence of a process that starts and finishes between samples.
Qualification binds the five command receipts, quiet/during observations,
build/input identities and workload log by SHA-256. The comparator requires
both completed, qualified lanes and matching ambient resource settings.
Resource provenance resolves the current cgroup2 membership against mountinfo
and records every visible leaf-to-root ancestor: CPU quotas/weights/bursts,
CPU/NUMA sets, memory limits/protection/throttling/swap, I/O limits/weights and
controller exposure. Each unavailable field retains its reason. Derived CPU
quota/set and memory bounds use the minimum/intersection of observed ancestor
constraints; a missing finite limit remains unavailable, never invented
unlimited. Completeness is explicit. Unreadable, malformed, unmapped or hidden
ancestor policy is refused before cache reset. The saved
`resource-policy-after.json` must equal the initial policy, and both snapshots
are bound into qualification and revalidated by the comparator. It
reparses BenchExec's workload `returnvalue`, signal and termination fields:
the `runexec` process's successful exit alone cannot qualify a failed ingest.
Optimized Python (`-O` or `PYTHONOPTIMIZE`) is refused at shared-module import
before any lane work; qualification assertions cannot be disabled.
Missing/unavailable measurements and nested persistence rows are refused.
Manifest checkpoint, journal append, sync, journal namespace and source
publication costs are reported separately as disjoint leaves, including the
startup namespace barriers. The baseline source-publication helper is pinned
to its reviewed body digest
`30f1a0bf1ff07ab1ecfe842a3f6b57dd6cd3a165af234b7d801375d686b71637`;
unknown helper bodies are refused before assuming zero namespace barriers.
The baseline already renames each source without a measured source-publication
leaf, so its new directory-barrier count/time are known zero, while rename
wall/CPU and other unobserved costs remain unavailable. All candidate leaf
costs and the complete barrier count/time delta are included. Whole-persistence
wall comparison remains unavailable; manifest-checkpoint wall deltas retain
matched boundaries and remain comparable.

After each measurement, use the [reopen verifier](../../scripts/development/import-journal/verify-reopen.py)
from a Python environment with PyArrow installed:

```bash
python3 "$task_methods/verify-reopen.py" --lane "$task_lane" --pair "$task_pair" \
  --worktree "$task_worktree" --binary "$task_target/release/gf" \
  --evidence-root "$task_evidence"
```

It starts a new CLI process against the measured project and runs node and
directed-edge count queries plus one bounded non-count query into external
Parquet sinks. It requires one-row integer counts of exactly 4,194,304 nodes
and 67,108,864 edges, and one sample row. The lane's `reopen/proof.json` records
completion/verification, the exact command, query exit status, source/binary
and input identities, counts, and the SHA-256 of each output/receipt/log. A
failed query or count check retains a refused proof; existing proof directories
are never overwritten. The verifier checks the measured lane's qualification
and the unchanged binary/source before opening the project.

Keep these reopening/query proofs outside the measured workflow. A qualified
timing comparison alone does not establish published graph correctness.
Attach comparison, qualification, receipts, host samples and reopen proofs to
the producing issue or PR.

Method SHA-256 pins:

| Method | SHA-256 |
| --- | --- |
| `driver.sh` | `a011e6b682c49a59d08ef919cca6997971f3cbb0d52ff27fa0c2dd0e4aa8a498` |
| `measure-lane.py` | `a6eceaaccbeccf1b70d453bd40c26c2206e6ce2114d5e2f6d76e24b50acf8af7` |
| `measurement_contract.py` | `87d1861b62bb8313037338027c3e78c50b2fea00dc11913b6c60b002240be87f` |
| `compare-pair.py` | `e948cd34b6b3a4b16778f1afc3644f22806c1fd5e6676c47f8cdf70c276079a3` |
| `test-measurement-method.py` | `25d643c77ae7fa70310c711b5ae2b36b564f6164f50d4b6e0128cdba7f45b13d` |
| `verify-reopen.py` | `6cb6da70c9a0d7ee2d0f1d2debb3c39838daaa44c35a3d04b3e5b4caf937e3a5` |
| `test-reopen-method.py` | `ce22331249b55d5bb56f907f727c8ff98892b2c4aa0ee44cdfcaf1833dfb7d1e` |

The [method regression](../../scripts/development/import-journal/test-measurement-method.py)
runs with `python3 scripts/development/import-journal/test-measurement-method.py`.
It uses synthetic lanes plus tiny shell exit probes; it never resets caches,
builds GraphForge or imports the S22 inputs.

The [reopen-method regression](../../scripts/development/import-journal/test-reopen-method.py)
runs with `python3 scripts/development/import-journal/test-reopen-method.py` in
the same PyArrow environment. It decodes tiny real Parquet query-result
fixtures and checks persisted positive/refused proofs; GraphForge invocation
is replaced by a fixture producer, so it does not claim actual reopen evidence.
