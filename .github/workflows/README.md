# GitHub Actions Workflows

GraphForge uses one stable required `CI Gate`. A deterministic classifier runs
only the policy, language, and binding jobs relevant to the pull request.
Release certification remains in `publish.yaml`.

Linux jobs run on the pinned `blacksmith-4vcpu-ubuntu-2404` image. Native PR
jobs use Blacksmith sticky disks for job-isolated Cargo `target/` directories,
while registry and pnpm dependencies use the colocated cache through upstream
`actions/cache@v6` and `actions/setup-node@v6`. Cargo-lock changes create fresh
sticky disks; inactive disks expire under Blacksmith's retention policy. The
M22 host-native load matrix also mounts a sticky `target/` so maturin, Cargo,
and napi share one build volume instead of a second root-disk tree.

## Pull-request contract

- A newer commit cancels obsolete Test Suite, Documentation, and auto-label
  runs for the same pull request. Pushes to `main` are never cancelled.
- Repository policy always validates workflow syntax, the classifier itself,
  ADR 0014 domain-dependency directions, and license compliance.
- Documentation and packaging-metadata-only changes do not compile Rust or
  native bindings.
- Rust changes run formatting and Clippy in one clean job and workspace tests
  in another. The workspace test already contains the Rust BDD target, so CI
  does not compile it twice. The same Rust classification also runs the Windows
  `gf-storage` `project_generation` lock unit tests on
  `blacksmith-4vcpu-windows-2025` (Linux workspace tests cannot execute those
  `#[cfg(windows)]` cases).
- Python, Gherkin, and public binding gates run only when their owned surfaces
  change. Pull requests classify from their base SHA; pushes classify from the
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
Python, Gherkin, or native binding jobs. Pull-request native acceptance is
Linux-only and uses Cargo's `dev` profile.
When Rust surfaces change, `Windows gf-storage Locks` runs
`cargo test -p gf-storage project_generation::tests:: --lib` on
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
not run a second MSVC `gf-storage` release `cargo test` as Binding RC evidence.
Windows `#[cfg(windows)]` project-root lock unit tests run in the Test Suite
job `Windows gf-storage Locks` instead.

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

### `checkpoint-recovery-gate.yml` and `non-cypher-surface-gate.yml`

Maintainers manually dispatch these SHA-bound release-certification workflows
when assembling publication evidence. Their acceptance commands remain covered
by the ordinary workspace and binding suites; dispatch adds immutable release
reports without rebuilding the same surfaces on every pull request. Green runs
are not close criteria for child or construction issues that already met their
acceptance criteria on ordinary CI.

### `m22-non-cypher-surface-gate.yml`

A maintainer manually dispatches this **release-certification** workflow with
the exact current `main` SHA and the successful Rust-surface and Binding RC run
IDs for that SHA when assembling publication evidence for the v0.5.0 publication close-out. The cheap
validation job rejects stale SHA, failed or unexpected workflows, and missing,
duplicate, or expired component artifacts before any native build. One Linux
release-machine job then builds one same-SHA Rust probe, Python wheel, and Node
addon and executes the existing 144-case XS-XL matrix. The final job revalidates
the Rust, binding, and load ledgers and uploads one
`M22-Non-Cypher-Surface-Gate-<sha>` artifact. The workflow is manual-only,
non-publishing, and cancels an obsolete duplicate dispatch for the same SHA.

The required Rust + Binding RC run IDs are an input contract for this workflow
only. They do **not** make the cascade a close gate for child implementation or
construction issues; those close on outcomes (see `AGENTS.md` § Issue close).

### `fuzz.yml` and `publish.yaml`

Fuzzing is scheduled/manual. Publishing retains the cross-platform and
multi-version artifact matrix required for release readiness; ordinary PRs do
not repeat that certification.

### `clean-env-verify.yml`

Maintainer `workflow_dispatch` after section 6 publication. Installs from **public**
PyPI/npm/crates.io only and runs the #742 section 7 / #2795 lanes (pip quickstart, npm
smoke, NPX skills compatibility, cargo add smoke, create/close/reopen Arrow
rows, docs/package URL resolve, optional checksum match against a
`graphforge-release-record-v1` file). Preflight fails closed when the requested
version is unpublished. Ordinary PRs run only the harness unit tests via
Repository Policy — they never claim clean-env success against missing packages.
See [`docs/development/clean-environment-verification.md`](../../docs/development/clean-environment-verification.md).

## Local equivalents

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
