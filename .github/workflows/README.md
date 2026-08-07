# GitHub Actions Workflows

GraphForge uses one stable required `CI Gate`. A deterministic classifier runs
only the policy, language, and binding jobs relevant to the pull request.

**Speed is a first-class value alongside honesty.** Surfaces shed work that is
not required for their objective. PR CI does **not** run full `llvm-cov`.
Frequent publishing uses the **publish-track** (Binding RC → tag →
`publish.yaml` on retained bytes). M1 load, checkpoint, and m20/m21 remain
**human-close / milestone** evidence and are not publish-track blockers.
Wall-clock targets and the dual-track table live in
[`docs/engineering/TESTING.md`](../../docs/engineering/TESTING.md).

`publish.yaml` consumes a retained Binding RC candidate (no rebuild-on-write)
after a GitHub Release / release identity exists for that SHA.

Linux jobs run on the pinned `blacksmith-4vcpu-ubuntu-2404` image. After the
Bazel CI Gate cutover (#4), Test Suite no longer mounts job-isolated Cargo
`target/` sticky disks; authoritative Rust compile/test is Bazel under
`Bazel Bootstrap` (`//:ci_rust_tests`). Registry and pnpm dependencies still use
the colocated cache through upstream `actions/cache@v6` and `actions/setup-node`.
Binding RC and the M1 host-native release load matrix retain sticky `target/`
volumes so maturin, Cargo, and napi share one build volume for packaging lanes
(see storage policy tests); put `target/` on sticky disks there, not in
`actions/cache` blobs.

### Blacksmith-first CI storage policy

`scripts/ci/test-ci-storage-policy.py` encodes these rules (not the GitHub
Actions cache-era bans that blocked RC speed):

| Allowed | Purpose |
| --- | --- |
| `useblacksmith/stickydisk` for `target/`, optional `.sccache`, large trees | Persist compile products across RC/publish-track runs (~3s mount) |
| Upstream `actions/cache@v6` for `~/.cargo/registry` + git (and pnpm/uv) | Colocated Blacksmith cache; exact lockfile keys |
| Local `sccache` with `SCCACHE_DIR` on a sticky disk | Cross-crate compile cache without GHA-backend maturin sccache |
| Bigger Blacksmith runners for RC cells | Linux 8/16 vCPU; larger macOS/Windows when needed |

| Still forbidden | Why |
| --- | --- |
| Putting `target/` into `actions/cache` blobs | Wrong tool — use sticky disks |
| Maturin-action `sccache: true` (GHA-integrated) | Prefer sticky `SCCACHE_DIR` / sticky `target/` |
| Unbounded artifact uploads | Keep consumer-driven retention for candidate partitions |

**Expected Binding RC Linux sticky keys** (release profile; shared across
Python-ubuntu and Node-linux when safe):

```text
${{ github.repository }}-binding-rc-linux-rust-<toolchain>-${{ hashFiles('Cargo.lock') }}-release-target-v1
${{ github.repository }}-release_candidate-rust-<toolchain>-${{ hashFiles('Cargo.lock') }}-release-target-v1
```

PR Test Suite sticky keys are retired after #4. macOS/Windows RC cells use
larger Blacksmith runners + colocated registry cache; use sticky disks there
only when the platform supports them.

## Pull-request contract

- A newer commit cancels obsolete Test Suite, Documentation, and auto-label
  runs for the same pull request. Pushes to `main` are never cancelled.
- Repository policy always validates workflow syntax, the classifier itself,
  ADR 0014 domain-dependency directions, and license compliance.
- Documentation and packaging-metadata-only changes do not compile Rust or
  native bindings.
- Rust changes run Cargo formatting/Clippy (`Rust Quality`) and authoritative
  Bazel tests (`Bazel Bootstrap` → `//:ci_rust_tests`, including API BDD). The
  same Rust classification also runs the Windows `graphforge-storage`
  `project_generation` lock unit tests on `blacksmith-4vcpu-windows-2025`
  (Linux Bazel CI cannot execute those `#[cfg(windows)]` cases).
- Python, Gherkin, public binding, Pulumi static-validation, and Terraform
  static-validation gates run only when their owned surfaces change. Shared
  GraphForge configuration and infrastructure contract fixtures run both IaC
  gates. Pull requests classify from their base SHA; pushes classify from the
  event's prior SHA. Missing Git history fails safe by enabling every gate.
- Ordinary binding PRs build one same-SHA Linux Python wheel and Node addon.
  They never use committed binaries or binding-side algorithm substitutes.
- SHA-bound checkpoint and non-Cypher evidence are explicit **release
  certification** gates, not duplicate per-PR suites. Maintainers dispatch them
  for a selected release candidate after the ordinary merge gate is green.
  Their success supports human publication close (the v0.5.0 publication close-out issue); it is not a serial
  close blocker for child implementation, construction, or gate-tracker issues.
  Close those on acceptance-criteria outcomes, merged work, and green checks for
  the changed surface (see `AGENTS.md` § Issue close).
- `CI Gate` accepts intentionally skipped, non-applicable jobs but fails for
  every failed or cancelled applicable job.

## Workflows

### `test.yml` — Test Suite

Runs the change classifier, repository policy, and only the applicable Rust,
Python, Gherkin, native binding, Pulumi, Terraform, or Bazel jobs. Pull-request
native binding acceptance is Linux-only and uses Cargo's `dev` profile for
maturin/napi assembly. Authoritative Rust compile/test is Bazel
(`//:ci_rust_tests`) under `Bazel Bootstrap`.
When Rust surfaces change, `Windows graphforge-storage Locks` runs
`cargo test -p graphforge-storage project_generation::tests:: --lib` on
`blacksmith-4vcpu-windows-2025` so the `#[cfg(windows)]` project-root lock unit
tests stay covered outside Binding RC.

### Behavioral acceptance

Rust BDD runs as part of the workspace test. Python and Node BDD run against
the same wheel/addon built in their binding job.

### `docs.yml` — Documentation

Builds the Astro Starlight docs site (`docs-site/`) via `pnpm docs:build`.
Content is synced from allowlisted `docs/**` pages into the Starlight content
collection at build time. Pull-request runs are cancellable; `main`
deployments remain serialized to GitHub Pages.

### `binding-release-candidate.yml`

A maintainer manually dispatches this non-publishing workflow with an exact
40-character commit SHA. It clean-installs Python wheels and executes native
Node addons on Linux, macOS, and Windows, package-validates any cross-built Node
target, and produces one fail-closed aggregate report. Missing targets,
mixed SHAs or per-language versions, fallback execution, failed or unclassified
cases, and parity differences reject the candidate. This workflow does not
create a tag or release and does not publish to PyPI or npm.
Its final assembly derives one aligned root version, packs the complete PyPI,
npm, and crates.io surfaces, records four non-overlapping artifact groups, and
reopens every archive with `graphforge-release-candidate-v2` completeness
validation. A checksum-valid archive with missing entrypoints, types, native
modules, dependency metadata, or legal files is rejected.
Linux Binding RC build cells mount a shared release-profile `target/` sticky
disk keyed by repository, RC Linux target family, Rust 1.96.0, and `Cargo.lock`;
the Python Ubuntu and Linux Node cells share it because their Cargo artifacts
are target-qualified. The release-assembly cell has its own equivalent sticky
disk. Registry and git dependencies use colocated `actions/cache@v6` on every
RC OS; no cache action transfers `target/`. macOS and Windows use larger
Blacksmith runners (12-vCPU macOS, 8-vCPU Windows) rather than sticky disks.

RC is intentionally slimmer than the PR suite: PR CI owns broad Linux binding
acceptance, while RC runs only clean-install smoke plus publish-critical native
parity/error contracts on every retained platform, then verifies the exact
retained partitions offline. This keeps multi-OS native evidence, offline
rehearsal, and same-SHA fail-closed packing intact. During the first three
comparable dispatches after this change, record the Actions duration in the PR
ledger: target p50 is ≤20 minutes warm and ≤35 minutes cold. Treat warm p50
above 25 minutes as a failed speed acceptance criterion and open a bounded
follow-up before declaring the change complete.
After the maturin wheel build, the workflow verifies that any inherited Rust
compiler wrapper is still executable before Python contracts may launch Cargo;
an unavailable wrapper is cleared without printing the job environment or PATH.
The wheel remains a read-only input under `dist`; classification and final Python
target evidence use a probed writable directory under the runner's temporary
storage on every operating system.
Release-candidate macOS and Windows jobs use Blacksmith's corresponding hosted
images so release evidence is independent of GitHub-hosted runner billing. The
Intel macOS Node lane installs an x64 Node runtime and verifies `process.arch`
before loading the x86_64 addon on the Apple Silicon runner.
The Windows Python lane proves user-facing use of the installed wheel: build the
native abi3 wheel, clean-install it, and run native Python contracts. It does
not run a second MSVC `graphforge-storage` release `cargo test` as Binding RC evidence.
Windows `#[cfg(windows)]` project-root lock unit tests run in the Test Suite
job `Windows graphforge-storage Locks` instead.

### `Concurrency Matrix` job in `test.yml`

When Rust or binding surfaces change, the required short concurrency matrix runs
the frozen Rust/Python/Node cases from
`tests/contracts/concurrency-short-matrix.json` with a bounded timeout. Repository
Policy always validates that matrix and the scheduled stress configuration.

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

### `checkpoint-recovery-gate.yml` and `non-cypher-surface-gate.yml`

Maintainers manually dispatch these SHA-bound release-certification workflows
when assembling publication evidence. Their acceptance commands remain covered
by the ordinary workspace and binding suites; dispatch adds immutable release
reports without rebuilding the same surfaces on every pull request. Green runs
are not close criteria for child or construction issues that already met their
acceptance criteria on ordinary CI.

### `m1-release-certification.yml`

A maintainer manually dispatches this **release-certification** workflow with
the exact current `main` SHA and the successful Rust-surface and Binding RC run
IDs for that SHA when assembling publication evidence for the v0.5.0 publication close-out. The cheap
validation job rejects stale SHA, failed or unexpected workflows, and missing,
duplicate, or expired component artifacts before any native build. One Linux
release-machine job then builds one same-SHA Rust probe, Python wheel, and Node
addon and executes the existing 144-case XS-XL matrix. The final job revalidates
the Rust, binding, and load ledgers and uploads one
`M1-Release-Certification-<sha>` artifact. The workflow is manual-only,
non-publishing, and cancels an obsolete duplicate dispatch for the same SHA.

The required Rust + Binding RC run IDs are an input contract for this workflow
only. They do **not** make the cascade a close gate for child implementation or
construction issues; those close on outcomes (see `AGENTS.md` § Issue close).

### `binding-release-candidate.yml`, `publish-track.yml`, `release-credential-preflight.yml`, and `publish.yaml`

The exact-SHA Binding RC retains tested release bytes and their partitioned v2
candidate manifest for 30 days. Credential preflight verifies the npm/crates.io
secret projections without publishing. The release-event workflow consumes the
retained candidate; ordinary PRs do not repeat that certification.

**publish-track** (registry-honest publish, scheduled or on-demand): successful
same-SHA Binding RC → tag / release identity → `publish.yaml` writes retained
bytes only. Skip re-RC when a complete unexpired candidate for the current
`main` tip already exists. Target wall-clock: Binding RC ≤20m p50 warm /
≤35m cold; publish-track ≤35m p50 / ≤50m cold (see TESTING.md). M1,
checkpoint, and m20/m21 are **not** required on this path.

`publish-track.yml` schedules exact-main Binding RC dispatch every six hours.
It reassembles and validates every retained partition before deciding a
candidate is reusable. Schedule runs never tag or publish. A maintainer must
set both `create_release` and `confirm_registry_publish` and supply the exact
root-version tag to create a published GitHub Release; that event triggers
`publish.yaml`. Mixed SHA, incomplete/expired partitions, tag disagreement,
existing Release identity, and all `publish.yaml` registry conflict checks fail
closed.

### `clean-env-verify.yml`

Maintainer `workflow_dispatch` after section 6 publication. Installs from **public**
PyPI/npm only and runs the #167 lanes (pip quickstart, npm
smoke, NPX CLI and skills compatibility, create/close/reopen Arrow
rows, docs/package URL resolve, optional checksum match against a
`graphforge-release-record-v1` file). Preflight fails closed when the requested
version is unpublished. Candidate v2 manifests and historical
`graphforge-release-record-v1` files are both accepted for checksum lookup.
Ordinary PRs run only the harness unit tests via
Repository Policy — they never claim clean-env success against missing packages.
See [`docs/development/clean-environment-verification.md`](../../docs/development/clean-environment-verification.md).

## Local equivalents

Default maintainer loop is `make pre-push-fast` (~30s). Run `make coverage-rust`
when claiming coverage floors; PR CI does not enforce full llvm-cov.

```bash
make pre-push-fast
make cargo-check
make cargo-clippy
make cargo-test
make cargo-fmt-check
make workflow-lint
scripts/ci/test-classify-changes.sh
scripts/ci/check-domain-dependencies.py
scripts/ci/test-domain-dependencies.py
```

Cross-platform native matrices and release artifact builds remain CI-only. Run
the binding release candidate from the Actions UI and retain its aggregate
artifact URL as **publication** evidence for the exact SHA when preparing
the v0.5.0 publication close-out issue / `publish.yaml` readiness—not as a close ritual for ordinary issues.

The full XS-XL load matrix is deliberately not a pull-request job. Repository
policy validates its contracts and mutation-sensitive aggregator tests only.
The final release-certification workflow runs `make release-load-matrix` on
a bounded release machine with same-SHA Rust, Python, and Node executors and
retains the resulting bundle inside the aggregate gate record. See
[`docs/development/release-load-matrix.md`](../../docs/development/release-load-matrix.md).
