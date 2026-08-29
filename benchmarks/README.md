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
declares a typed benchmark-owned generator action for `generate` and one
ordinary `gf` public command action for every product phase, in order. Runtime
validation binds each product phase to its real command family and rejects
global help/version no-ops. The runner does not link product crates, call
storage internals, provision a host, or enforce CPU, memory, disk, or time
limits. BenchExec or another outer orchestrator owns deadlines and may terminate
the runner; the certification binary does not silently convert a policy timeout
into product evidence.

```bash
cargo run --locked --manifest-path benchmarks/Cargo.toml \
  --bin graphforge-benchmark-certify -- \
  run PROFILE.json benchmarks/outputs/evidence.json
```

Tiny in-process fixtures exercise every phase and first-failure behavior without
opening a project. Graph500 profiles supply the real generate, ingest, recount,
query, portable export/verify/import, and reopen commands in #956; a placeholder
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

The command is fail-closed: only `passed` exits successfully. A typed
`disqualified` result exits with status 2 and an execution failure exits with
status 1. The dedicated `Native Local Admission` workflow runs on a bare Linux
runner, requires ReFrame to report `passed`, validates the closed evidence
schema independently, and uploads the sanitized document even when admission
fails. An uploaded disqualification is diagnostic evidence, never a green
qualification.

The hosted workflow uses one explicit root authority for both BenchExec's
cgroup preflight and the measured fixture because the ephemeral runner's normal
UID is not delegated a writable cgroup subtree. This is recorded as a boolean
fact in evidence. It does not disable namespace containers, bypass cgroups, or
grant authority to a persistent/shared host.
