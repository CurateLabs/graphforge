# External benchmark workspace

This directory contains two deliberately independent benchmark toolchains:

- Python owns orchestration, fixture discovery, evidence validation, ReFrame,
  and BenchExec integration.
- Rust owns benchmark-specific command-line runners that call GraphForge only
  through published public interfaces.

The Python and Rust dependency locks in this directory are independent of the
repository root. Benchmark-only dependencies must never be added to GraphForge
product crates or bindings. Product-only behavior, feature flags, and benchmark
hooks are prohibited.

The existing `algorithms/` scripts and benchmark documents predate this
workspace and remain unchanged. Migration or retirement of that legacy harness
belongs to issue #959, after parity is proven.

## Layout

- `harness/` — Python orchestration and fixture discovery.
- `profiles/` — execution-environment profiles; no credentials.
- `suites/` — benchmark-suite declarations.
- `fly/` — disposable-provider adapter definitions only.
- `schemas/` — sanitized evidence contracts.
- `tests/` — no-cost workspace and fixture-discovery tests.
- `runners/` — unpublished Rust benchmark runners.

## Public-interface certification runner

`runners/certify` owns the admission through reopen-proof lifecycle. A profile
declares a typed benchmark-owned generator action for `generate` and ordinary
`gf` public command actions for every product phase, in order. Ingest, recount,
query, and imported reopen proof use ordered multi-command actions; the runner
executes each child directly, stops on the first failure, and aggregates phase
duration and peak RSS. Runtime validation binds each product phase to its real
command family and rejects global help/version no-ops. The runner does not link product crates, call
storage internals, provision a host, or enforce CPU, memory, disk, or time
limits. BenchExec or another outer orchestrator owns deadlines and may terminate
the runner; the certification binary does not silently convert a policy timeout
into product evidence.

```bash
cargo run --locked --manifest-path benchmarks/Cargo.toml \
  --bin graphforge-benchmark-certify -- \
  run PROFILE.json benchmarks/outputs/evidence.json
```

Tiny fixtures exercise every phase and first-failure behavior, including one
executable scale-1 fixture that opens a real project and completes the ordinary
source/export/verify/clean-import/reopen lifecycle. Graph500 profiles supply the
real generate, ingest, recount, query, portable export/verify/import, and reopen commands in #956; a placeholder
profile that merely labels no-op commands as lifecycle work is intentionally not
shipped.

Evidence contains only phase names, typed pass/fail state, exit codes, elapsed
milliseconds, and observed peak RSS bytes. Command arguments and child output
are intentionally excluded so graph contents, credentials, and sensitive paths
cannot be emitted. Missing RSS is represented as `null`; this runner records
resource evidence but never enforces a resource policy. Certification stops at
the first failed phase. One sanitized JSON phase event is written to standard
output as each public command finishes; the final typed evidence document is
written only to the requested output file.

Phase RSS is an observation, not a memory budget. Ladder orchestration must
compare the same phase across scales and treat sustained edge-count-linear RSS
growth as an architectural failure signal. This runner does not infer that
cross-profile trend or convert the M5 certification ceiling into a requirement.

Legacy JSON can be converted into the same sanitized contract:

```bash
cargo run --locked --manifest-path benchmarks/Cargo.toml \
  --bin graphforge-benchmark-certify -- \
  normalize legacy-evidence.json benchmarks/outputs/normalized.json
```

The accepted legacy shape is `{profile, phases}` with each phase containing
`name`, `ok`, `duration_secs`, optional `max_rss_kib`, and optional
`exit_code`. Normalization is fail-closed and stops after the first failure.

## Linux resource authority

`definitions/graphforge-certification-v1.xml` is the versioned BenchExec entry
point for Linux certification. Its tool-info adapter invokes only the public
certification binary. BenchExec owns the complete process tree, limits,
termination, wall/CPU time, peak RSS, and read/write byte evidence; the Rust
runner's per-phase telemetry remains preserved as the product-side account.

`graphforge_bench.benchexec_authority.normalize_run` fails closed unless all
mandatory process-tree resource fields and a correctness verdict are present.
It reports timeout, OOM, non-zero exit, signal, harness termination, and
correctness failure as distinct typed outcomes. Status or wall-time disagreement
between BenchExec and GraphForge is retained explicitly instead of choosing the
more favorable value. Provider provisioning is outside this layer.

Generated datasets, credentials, execution outputs, and local environments are
ignored. Fly execution is forbidden until the complete benchmark stack is
merged and has passed local and hosted qualification.

## Smoke

From the repository root:

```bash
make -C benchmarks smoke
```

The smoke installs only the locked benchmark environment, imports ReFrame and
BenchExec, discovers the checked-in fixtures, runs the Python unit tests, and
checks the dependency-free Rust smoke runner. It does not run a benchmark,
start a provider, or open a GraphForge project.

## Native local admission

`make -C benchmarks local-admission` uses ReFrame's local scheduler and local
launcher to run the admission probe. The probe admits only native Linux with
cgroups v2 and the required namespace controls. BenchExec's namespace-enabled
`RunExecutor` must then account for CPU, wall time, peak memory, reads, and
writes while terminating a detached descendant tree at the wall-time limit.

Unsupported hosts return sanitized typed disqualification evidence. In
particular, macOS and Docker Desktop do not prove native Linux admission and
must not be used as substitutes. This probe creates no Fly resources and does
not authorize a Graph500 scale run.

## Progressive Graph500 qualification

`profiles/graph500/` contains distinct declarative S18 and S19 local profiles
and S20, S22, and S26 provider profiles. They pin the reference generator,
edge factor 16, seed, the same ten-phase public certification lifecycle, and a
closed sanitized evidence contract. Provider profiles describe environment
classes only; they contain no app, machine, volume, account, region, or secret
identifier.

The Python qualification policy consumes completed, correct ordinary-lifecycle
evidence and stops at the first failed rung. S20 requires S18 and S19. S22 has
its own S19/S20 gate. S26 is separately refused without adjacent S24/S25 disk
and bounded-RSS observations from the canonical ladder. Projections preserve
wall time, peak RSS, retained and transient storage, logical and physical I/O,
reader calls, and publication work as independent dimensions.

The provider ceiling is four hours, 4 GiB RSS, and 500 GiB storage. Admission
reserves 20% time and RSS headroom and 15% storage headroom (425 GiB usable),
which covers runtime variance and filesystem/package transients without
turning the M5 ceiling into a sizing target. Adjacent RSS growth above 10% is
an architectural refusal signal: GraphForge is expected to plateau in memory
while storage and I/O grow. These are engineering qualification claims only,
never official Graph500 submission claims.

The ReFrame cases are manual execution entry points and are deliberately
excluded from normal CI and `smoke`; list them with:

```bash
make -C benchmarks progressive-qualification-list
```

Actual Linux resource execution remains under the versioned 4 GiB/four-hour
BenchExec definition and public certification runner. Provider cases are not
valid on the local ReFrame system. Provider provisioning is a later, separate
operation; listing these profiles launches nothing.
