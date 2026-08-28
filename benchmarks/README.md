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
