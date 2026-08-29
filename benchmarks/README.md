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

## Disposable Fly adapter

`graphforge_bench.fly_adapter` is an offline command planner, not another
benchmark controller. It requires merged #955/#956/#957 attestations, a full
commit, an immutable `registry.fly.io/...@sha256:...` image, a fixed region,
an explicitly measured Machine class, and an explicit maximum authorized
scale. It accepts the controller's lifecycle argv and checked-in profile
identity unchanged. It cannot select a rung, alter a threshold, or retrieve
anything except allowlisted sanitized JSON evidence.

The planned provider lifecycle is one remotely built image, one private app,
one encrypted volume, and one Machine with no services. The Machine remains
alive only so evidence can be retrieved from its attached volume; restart and
Fly Proxy autostop are disabled. Teardown uses a persisted ownership ledger and
is safe to repeat. Static tests launch nothing:

```bash
make -C benchmarks fly-adapter-static
```

The adapter remains disabled until #956 is complete. The tiny provider
qualification and independent post-teardown inventory are separate live
acceptance evidence and are not claimed by these fixtures.

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
cgroups v2 and the required namespace controls. BenchExec's supported `runexec`
entrypoint must then account for CPU, wall time, peak memory, reads, writes, and
pressure while terminating a detached descendant tree at the wall-time limit.

Unsupported hosts return sanitized typed disqualification evidence. In
particular, macOS and Docker Desktop do not prove native Linux admission and
must not be used as substitutes. This probe creates no Fly resources and does
not authorize a Graph500 scale run.

## Progressive Graph500 qualification

`profiles/graph500/` contains distinct declarative S18 and S19 local profiles
and S20, S22, S24, S25, and S26 provider profiles. They pin the reference generator,
edge factor 16, seed, the same ten-phase public certification lifecycle, and a
closed sanitized evidence contract. Provider profiles describe environment
classes only; they contain no app, machine, volume, account, region, or secret
identifier.

The Python qualification policy consumes completed, correct ordinary-lifecycle
evidence and stops at the first failed rung. Each provider rung projects from
the preceding two ladder rungs: S20 from S18/S19, S22 from S19/S20, S24 from
S20/S22, S25 from S22/S24, and S26 from S24/S25. S26 is separately refused
unless those adjacent S24/S25 observations came from the canonical ladder.
Projections preserve
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

The reproducible controller builds and hashes the three exact executables,
binds the checked-out commit and profile identity, stages the BenchExec XML in
a private directory, and writes only closed sanitized documents to the explicit
evidence directory. Planning is safe on unsupported hosts and executes no rung:

```bash
make -C benchmarks progressive-qualification-plan \
  RUNG=S18 OUTPUT_DIR=/admitted-volume/graphforge-evidence
```

The real one-command entry point is intentionally manual and outside normal CI:

```bash
make -C benchmarks progressive-qualification-run \
  RUNG=S18 OUTPUT_DIR=/admitted-volume/graphforge-evidence
```

It accepts only S18 followed by S19, refuses duplicate or out-of-order evidence,
requires native Linux cgroups-v2 BenchExec admission, and never provisions a
provider. S19 consumes the schema-valid passed `s18-rung.json` from the same
directory. After both local rungs pass, the existing progressive projection
policy consumes them as the adjacent S20 sources with one sanitized command:

```bash
make -C benchmarks progressive-qualification-project-s20 \
  OUTPUT_DIR=/admitted-volume/graphforge-evidence \
  PROVIDER_CAPACITY=/sanitized/provider-capacity.json
```

The controller derives bulk-ingest capability from the same run's bounded
ordinary `gf import-session commit --json` receipt: its construction evidence
must identify a configuration of at least 65,536 rows, accepted bulk chunks,
and the single committed publication. There is no separately plantable
capability file. BenchExec XML is the sole wall/RSS/physical-I/O authority.
Ordinary `storage-attribution --json` receipts preserve source and clean-import
allocated and logical-EOF components separately. Construction evidence owns
logical I/O, reader-call, transient-construction, and publication-work
components; it is not relabeled as a whole-lifecycle storage peak. Passed rung
evidence therefore also requires an authenticated `graphforge-lifecycle-storage/1`
owner-union receipt for retained and transient lifecycle maxima. The controller
fails closed while that ordinary receipt is absent rather than summing project
owners or treating portable logical payload bytes as allocated storage.

Ordinary result-sink receipts independently own scalar counts, fixed-hop result
cardinality, query evidence, and source/import logical-result digests. Missing
or contradictory storage, construction, or query receipts cause a typed first
failure; the controller never manufactures values from command counts,
recursive file scans, portable payload size, or synthetic labels.

## Native local admission deployment

The command is fail-closed: only `passed` exits successfully. A typed
`disqualified` result exits with status 2 and an execution failure exits with
status 1. The dedicated `Native Local Admission` workflow is manual because
ordinary pull-request runners do not promise this host topology. When explicitly
dispatched it requires ReFrame to report `passed`, validates the closed evidence
schema independently, and uploads the sanitized document even when admission
fails. An uploaded disqualification is diagnostic evidence, never a green
qualification or a masked workflow success.

The hosted workflow evaluates bare Blacksmith and GitHub-hosted Ubuntu 24.04 as
separate explicit authorities. Both install BenchExec from the official
SoSy-Lab Ubuntu PPA so its package-managed one-time cgroup setup is the system
authority. A
hard `python3 -m benchexec.check_cgroups` preflight runs before the identical
ReFrame check, and the measured fixture uses that same system interpreter. Per
BenchExec's cgroups-v2 installation guidance, both commands execute in one
`systemd-run --scope -p Delegate=yes` user scope. Two separately named system
authorities also use transient system services that explicitly delegate
`cpu cpuset io memory` while executing as the original unprivileged runner UID.
The service uses systemd's `DelegateSubgroup=init.scope` so its process does not
occupy the delegated unit root. The in-unit driver proves the live unit's
delegation controllers, initial subgroup, and process UID before admission; it
never writes cgroup control files or runs the benchmark as root.
Each job is independently strict and uploads a provider-labelled evidence
document; one provider is never a fallback that hides the other's
disqualification. The workflow does not disable namespace containers, bypass
cgroups, or add custom permission/systemd workarounds.

For a dedicated native Linux authority, provision the administrator-owned user
manager delegation before qualification:

```bash
sudo benchmarks/scripts/provision-benchexec-user-delegation.sh
sudo reboot
```

The tracked drop-in sets `Delegate=yes` on `user@.service`, as prescribed by
BenchExec's cgroups-v2/systemd installation guide. A reboot (or complete user
manager termination and fresh login) is mandatory; editing the unit does not
retroactively change an already-running user manager. The benchmark and both
strict checks still run as the unprivileged user.
