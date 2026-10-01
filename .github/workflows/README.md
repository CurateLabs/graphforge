# GitHub Actions Workflows

GraphForge uses one stable required `CI Gate`. Live default-branch enforcement
is repository ruleset **19988544** (required status check context exactly
`CI Gate`); workflow job naming alone is not sufficient.
A deterministic classifier runs only the policy, language, and binding jobs
relevant to the pull request.

`config/gate-registry.json` is the authoritative inventory for workflow and
operator gates. It records class, owner, canonical command, evidence contract,
freshness, and SHA binding. `scripts/ci/gate-registry.py` validates the registry
in the CI Lint job and renders commands for Make or operators.
Every gate belongs to exactly one of four classes: required PR check, scheduled
health/stress, operator qualification, or release certification. Supporting
automation is inventoried with a non-gate role because it produces no gate
decision. Only `github-status/CI Gate` is a required PR check.

Provider-backed qualifications are Python operator commands entered through
`pulumi env run`; Pulumi ESC owns secret projection and output filtering.
GitHub workflow files are compatibility wrappers and evidence viewers, not the
operational control plane.

**Speed is a first-class value alongside honesty.** Surfaces shed work that is
not required for their objective. The **Coverage** workflow runs
`llvm-cov` on every merge to `main` and enforces the floors there; a policy test
refuses it in any pull-request-triggered workflow.
Wall-clock targets live in
[`docs/engineering/TESTING.md`](../../docs/engineering/TESTING.md).

`publish.yaml` is the whole release path: one workflow builds, publishes, and
verifies a release. See `RELEASING.md` and the `publish.yaml` section below.

Linux jobs run on the pinned `blacksmith-4vcpu-ubuntu-2404` image. The CI Gate
Rust test lane is Cargo with nextest (`Rust Tests`, ADR 0048). It is the only
Test Suite job that mounts a `target/` sticky disk: one shared volume keyed by
the toolchain. The job-isolated PR Cargo volumes that #4 retired stay retired. Registry and pnpm dependencies still use
the colocated cache through upstream `actions/cache@v6` and `actions/setup-node`.
`publish.yaml` mounts no sticky disk; its build lanes use the colocated
registry cache. Where a lane does mount a `target/` volume, use a sticky disk,
not `actions/cache` blobs.

### Blacksmith-first CI storage policy

These rules apply (the earlier GitHub Actions cache-era bans blocked build speed):

| Allowed | Purpose |
| --- | --- |
| `useblacksmith/stickydisk` for `target/`, optional `.sccache`, large trees | Persist compile products across runs (~3s mount) |
| Upstream `actions/cache@v6` for `~/.cargo/registry` + git (and pnpm/uv) | Colocated Blacksmith cache; exact lockfile keys |
| Local `sccache` with `SCCACHE_DIR` on a sticky disk | Cross-crate compile cache without GHA-backend maturin sccache |
| Bigger Blacksmith runners for release build cells | Linux 8/16 vCPU; larger macOS/Windows when needed |

| Still forbidden | Why |
| --- | --- |
| Putting `target/` into `actions/cache` blobs | Wrong tool — use sticky disks |
| Maturin-action `sccache: true` (GHA-integrated) | Prefer sticky `SCCACHE_DIR` / sticky `target/` |
| Unbounded artifact uploads | Keep consumer-driven retention (`publish.yaml` keeps build artifacts 7 days) |

Every Linux Test Suite job that compiles the workspace (Rust Tests, the
harness/doc/feature job, the feature boundary, the bindings, and the benchmark
harness) mounts its own sticky `target/` disk, keyed by job and toolchain. The
Windows and macOS storage jobs do not. macOS/Windows release build cells use
larger Blacksmith runners + colocated registry cache.

## Pull-request contract

- A newer commit cancels obsolete Test Suite, Documentation, and auto-label
  runs for the same pull request. Pushes to `main` are never cancelled.
- Repository policy always validates workflow syntax, the classifier itself,
  ADR 0014 domain-dependency directions, and license compliance.
- Documentation and packaging-metadata-only changes do not compile Rust or
  native bindings.
- Rust changes run the `Lint` job (Cargo formatting/Clippy) and the Cargo
  test lane (`Rust Tests`: `cargo nextest run --workspace --locked`, then the
  `bdd` and `disabled_allocations` custom-harness targets and the doctests under
  `cargo test --workspace --locked`). Rust test data (TCK features, goldens,
  snapshots, fixtures, reference docs) also runs `Rust Tests`. The
  same Rust classification also runs native filesystem publication/admission
  tests on `blacksmith-4vcpu-windows-2025` and
  `blacksmith-12vcpu-macos-15`; Windows retains the existing project-root lock
  tests. Both native lanes also exercise mapped semantic routes, legacy CAS
  translation, reserved-route adjacency/deletion, lazy stream isolation, and full
  and projected portable export/import/reopen through the Rust facade. Linux
  Rust Tests cannot execute those host-specific contracts.
- Python, Gherkin, public binding, agent-skills, Pulumi static-validation, and
  Terraform static-validation gates run only when their owned surfaces change.
  Shared GraphForge configuration and infrastructure contract fixtures run both
  IaC gates. Pull requests classify from their base SHA; pushes classify from
  the event's prior SHA. Missing Git history fails safe by enabling every gate.
- Ordinary binding PRs build one same-SHA Linux Python wheel and Node addon,
  and run the full binding suites (`Python and Node Bindings` job).
  They never use committed binaries or binding-side algorithm substitutes.
- Checkpoint and non-Cypher surface checks run in the ordinary workspace and
  binding suites; there is no separate gate dispatch. Close
  issues on acceptance-criteria outcomes, merged work, and green checks for the
  changed surface (see `AGENTS.md` § Issue close).
- `CI Gate` accepts intentionally skipped, non-applicable jobs but fails for
  every failed or cancelled applicable job.

## Workflows

### `test.yml` — Test Suite

Jobs: **Classify Changes**, **Lint** (`make check` and the script
self-tests), **Rust Tests** (nextest over the workspace), **Rust Harness, Doc,
and Feature Tests** (custom-harness targets, doctests, feature-gated tests),
**API and Executor Feature Boundary**, **Python and Node Bindings** (full
binding suites on Linux, same-SHA wheel and addon), **Windows Storage**,
**macOS Storage**, **Benchmark Harness**, **Agent Skills**, **Pulumi Static
Validation**, **Terraform Static Validation**, and **CI Gate**.

Pull-request native binding acceptance is Linux-only and uses Cargo's `dev`
profile for maturin/napi assembly; `Python and Node Bindings` builds, installs,
and runs the full binding suites against the same-SHA wheel and addon. The Rust
test lane is `Rust Tests` (Cargo with nextest, dev/test profile, so debug
assertions and overflow checks stay on). `Benchmark Harness` runs the benchmark
harness tests and the tiny and ownership-growth lifecycle producer against a
Cargo-built `gf`. When any crate changes, `Windows Storage`
runs the native project-root lock, exact filesystem primitive, NTFS admission,
and real publication-kill/fault-oracle cross-checks on
`blacksmith-4vcpu-windows-2025`. `macOS Storage` runs the
corresponding native APFS primitive, admission, and publication-kill
cross-checks. Linux executes the same storage unit suite through `Rust Tests`.
These platform jobs record actual subprocess/handle observations; the simulator
does not stand in for native evidence.

### Behavioral acceptance

Rust runs the engine API features and TCK as part of the workspace test.
Python and Node each run one bounded native smoke suite against the same-SHA
wheel and addon built in their binding jobs.

### `docs.yml` — Documentation

Builds the Astro Starlight docs site (`docs-site/`) via `pnpm docs:build`.
Content is synced from allowlisted `docs/**` pages into the Starlight content
collection at build time. Pull-request runs are cancellable; `main`
deployments remain serialized to GitHub Pages.

### `codspeed.yml` — CodSpeed

Runs once nightly against the exact latest `main` SHA, plus explicit manual
dispatch. Pull requests and pushes do not trigger CodSpeed. The latest
successful scheduled workflow SHA skips all nightly benchmark runners when
`main` has not changed; missing or unsuccessful prior evidence fails closed to
running the suite. Its Cargo
build is a diagnostic path, not part of CI Gate.
Comparable-run and measurement-floor triage is documented in
[`docs/development/benchmarking.md`](../../docs/development/benchmarking.md).
Pure storage kernels use simulation on the ordinary pinned CI runner; durable
open/recovery/commit/GC/compaction use CodSpeed's isolated bare-metal
`codspeed-macro` ARM64 runner. Manual runs also retain exact-SHA replay and
compaction peak-RSS artifacts while CodSpeed memory mode is unavailable for
this project.

### Operator gates — no workflow

`progressive-ladder`, `fly-tiny-qualification`, `fly-tiny-recovery`, and
`native-local-admission` are registered under `operator_gates` in
`config/gate-registry.json` and have no Actions workflow. Actions holds no
provider credential, spend authority, or deletion authority, and hosted runners
are disqualified as BenchExec admission hosts by design, so a wrapper could only
print the command. Render a gate's command with:

```bash
python3 scripts/ci/gate-registry.py command <gate>
```

The Fly gates run only through the Python + Pulumi ESC control plane. All
controller flags are passed after `ARGS=`; the Python operator requires the
exact SHA, an explicit live mode, and disposable confirmation before it opens
the named ESC environment:

```bash
make -C benchmarks qualification-operator \
  GATE=fly-tiny-qualification \
  ESC_ENVIRONMENT=curatelabs/graphforge/qualification \
  ARGS='--expected-sha <sha> --execute --confirm-disposable <remaining-controller-args>'
```

Receipt-bound orphan cleanup uses `GATE=fly-tiny-recovery` through the same
operator. Native admission runs on the designated benchmark host with
`make -C benchmarks local-admission`.

### Scheduled and manual lane health

A scheduled lane that has been red for seven days is fixed or removed; a lane
nobody reads is cost without signal. A manual lane is dispatched once when it
is added, so it is known to work before anyone depends on it. `fuzz.yml` was
red for 51 days and `concurrency-stress-gate.yml` had never passed when #1671
took the first census.

### CodeRabbit

CodeRabbit automatic review is disabled to preserve its limited quota. After
the required `CI Gate` is green and a pull request is otherwise ready to merge,
request the final review explicitly with `@coderabbitai review`.

### `publish.yaml` — Publish

One workflow builds every artifact from one commit and publishes it. The
registries' trusted-publishing settings name this file and the `release`
environment; do not rename either. Steps for a release are in `RELEASING.md`.

- **Triggers.** A `v*` tag push publishes. `workflow_dispatch` and a pull
  request that touches the release path (`publish.yaml`, `publish_crates.py`,
  `publish_npm.py`, `set_release_version.py`, and four helper files; see the
  `paths:` filter) are dry runs: everything is built, packed,
  and smoke-tested; nothing is uploaded.
- **Jobs.** `version` checks one version across Cargo, Python, and Node and,
  on a tag, that the tag is `v<workspace version>`. `wheels` builds three abi3
  wheels (Linux x86_64 inside the manylinux2014 container with `--manylinux
  2_17`, macOS arm64, Windows amd64), checks the wheel tag, and clean-installs
  and runs the Python tests. `sdist` builds the source distribution. `addons`
  builds five Node addons (the Linux targets with `--use-napi-cross`, checked
  against a glibc 2.17 floor), loads and runs each where the runner can execute
  it, and package-validates the cross-built aarch64 Linux addon.
  `npm-packages` packs the 8 npm tarballs and installs three of them in a clean
  project. `crates` packages all 20 crates in publish order and verifies
  LICENSE and NOTICE in each package.
- **Publish.** `publish` runs only on a tag push, needs every job above, uses
  the `release` environment (`id-token: write`, `contents: write`), and runs
  crates.io (`publish_crates.py`), PyPI (`uv publish --check-url`), npm
  (`publish_npm.py`, dist-tag `latest` for a release or `next` for a
  prerelease), then creates the GitHub Release. Each registry step skips a
  version that is already published, so a failed run is re-run as-is.
- **Verify.** `verify-published` waits up to 15 minutes for PyPI and npm to
  serve the version, installs it in a clean environment, and runs smoke tests.
- Concurrency group `publish-<ref>` never cancels an in-flight tag run; only
  pull-request dry runs are cancellable.

### Concurrency tests in `test.yml`

When Rust or binding surfaces change, the `Python and Node Bindings` job runs
the full binding suites, which include concurrency test cases with bounded
timeouts. The scheduled `concurrency-stress-gate.yml` runs the longer mixed
workload.

### `concurrency-stress-gate.yml`

Scheduled weekly and available via `workflow_dispatch`. Runs the published-seed
bounded-resource stress lane, uploads case/reproduction/resource evidence, and
never substitutes for the required short concurrency matrix. Stress retries are
diagnostic only.

### `visualization-limits-stress.yml`

Maintainer `workflow_dispatch` only. Runs the #299 visualization limits harness
(Plotly, Plotly.js, Jaal, PyVis, Cytoscape.js, Sigma.js) on a standard hosted runner,
uploads machine-readable evidence, and is never a PR, push, scheduled, required,
or release gate. See [`examples/visualization/stress/`](../../examples/visualization/stress/).

## Local equivalents

Default maintainer loop is `make check` (~30s). Run `make coverage-rust`
when claiming coverage floors; PR CI does not enforce full llvm-cov. The Coverage
workflow runs the same ledger on every merge to `main` and enforces all floors.

```bash
make check                          # all static checks (mirrors CI Lint)
make test-rust                      # CI Rust lane (narrow: make test-rust ARGS="-p <crate>")
make test-python                    # Python binding suites
make test-node                      # Node binding suites
make test-scripts                   # scripts/ci self-tests
```

Cross-platform native matrices and release artifact builds remain CI-only. Run
`publish.yaml` as a dry run from the Actions UI
(`gh workflow run publish.yaml --ref <branch>`) to build and smoke-test every
release artifact without uploading. `make publish-dry-run` packages every crate
in publish order locally.
