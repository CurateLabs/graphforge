# Testing Strategy & Infrastructure

Reader-facing strategy for **v0.5.0 / release-prep** (ownership, layered gates,
what counts as proof): [`../engineering/TESTING.md`](../engineering/TESTING.md).
This page keeps command recipes, suite layout, and local tooling detail.

## Overview

GraphForge has two test suites that must both pass:

| Suite | Location | What it tests |
|-------|----------|---------------|
| **Rust tests** (`cargo test`) | `crates/*/src/` | Each Rust crate in isolation and integration |
| **Python tests** (`pytest`) | `tests/` | Python binding, end-to-end queries, TCK compliance |

The testing principles are the same for both:

1. **Spec-driven correctness** — openCypher semantics verified via TCK
2. **Fast feedback loops** — unit tests run in milliseconds
3. **Hermetic tests** — no shared state between tests
4. **Deterministic behavior** — tests pass or fail consistently

---

## Rust Tests

### Structure

Each crate contains unit tests inline with the source and integration tests in `tests/`:

```
crates/graphforge-cypher/
├── src/
│   ├── lexer.rs        # #[cfg(test)] inline unit tests
│   ├── parser.rs       # #[cfg(test)] inline unit tests
│   └── lib.rs
└── tests/
    └── parse_corpus.rs # end-to-end parse tests against golden corpus
```

### Running

```bash
# All crates
cargo test --workspace

# One crate
cargo test -p graphforge-cypher

# With output
cargo test --workspace -- --nocapture

# Only doctests
cargo test --doc --workspace
```

### v0.5.0 non-Cypher release conformance

Part of the **v0.5.0 testing strategy**: the openCypher TCK proves `execute()` language
semantics, but it does not cover construction, lifecycle, checkpoints, analyst verbs,
search, or the knowledge/epistemic surfaces. The checked-in
`tests/contracts/non-cypher-rust-surface.json` manifest classifies every public
Rust receiver method, all 94 analyst-verb registry entries, and
the supported find/index modes and policies. New public methods or registry values
must be classified and linked to deterministic evidence before merge.

Run the omission gate and bounded public-facade matrix with:

```bash
python3 scripts/ci/non-cypher-surface-gate.py
python3 scripts/ci/test-non-cypher-surface-gate.py
cargo test -p graphforge-api \
  --test public_lifecycle_conformance \
  --test m22_m18_public_surface \
  --test m22_m19_public_surface
```

The `Rust Non-Cypher Surface Gate` workflow runs the inventory validator,
`graphforge-api` unit contracts, and these persisted integration tests from one exact
source SHA when assembling release-certification evidence. Its downloadable
report records the inventory digest and test binary digests. Ordinary
implementation and construction issues close on acceptance-criteria outcomes
and the relevant PR/`main` checks for the changed surface; they do not require
this manual SHA-bound dispatch (see `AGENTS.md` § Issue close). A green TCK run
cannot substitute for the surface inventory itself.

### Rust test example

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_scan_roundtrip() {
        let op = GraphOp::NodeScan { var: VarId(0), ty: TypeId(1) };
        let json = serde_json::to_string(&op).unwrap();
        let back: GraphOp = serde_json::from_str(&json).unwrap();
        assert_eq!(op, back);
    }
}
```

### Parser differential corpus

The parser migration strategy requires differential testing between the Python LALR(1) parser and the LALRPOP Rust parser. The corpus lives in `tests/parser_corpus/` and includes:

- Valid queries (from the TCK and real-world examples)
- Invalid queries (error recovery cases)
- Precedence edge cases
- Unicode identifiers
- Parameter syntax
- Comments

Differential tests run both parsers on the same input and assert AST parity:

```bash
cargo test -p graphforge-cypher -- differential
```

---

## Python Tests

### Test Categories

#### 1. Unit Tests (`tests/unit/`)

Test individual components in isolation.

```
tests/unit/
├── parser/
├── planner/
├── executor/
├── storage/
├── algorithms/
├── search/
└── recipes/
```

Characteristics: no I/O, < 1 ms per test, ≥90% coverage target.

#### 2. Integration Tests (`tests/integration/`)

Test full query pipeline (parse → plan → execute), persistence, transactions,
and the Python API surface.

Characteristics: may use temporary databases, < 100 ms per test.

#### 3. openCypher TCK Tests (`tests/tck/`)

Official openCypher Technology Compatibility Kit. 3,885 scenarios; 100% passing
on `main`. TCK is a hard merge gate for the `rust-core` branch too.

```
tests/tck/
├── conftest.py
├── coverage_matrix.json
└── features/
```

#### 4. Property-Based Tests (`tests/property/`)

Hypothesis-driven generative tests for value semantics, expression evaluation,
and storage consistency invariants.

#### 5. Performance Benchmarks (`tests/benchmarks/`)

Real-dataset benchmarks tracked over time. Not part of the standard CI run.

### Pytest Configuration

```toml
[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = ["-ra", "--strict-markers", "--tb=short", "-v"]

markers = [
    "unit: unit tests (fast, isolated)",
    "integration: integration tests (may use I/O)",
    "tck: openCypher TCK compliance tests",
    "property: property-based tests",
    "benchmark: performance benchmarks",
    "slow: tests that take >1s",
]
```

### Running Python Tests

```bash
# All tests
make test

# By category
make test-unit
make test-integration
make test-tck

# With coverage (core Rust ≥85%; Rust adapters ≥80%; wrappers ≥85%)
make coverage            # all surfaces + thresholds
make coverage-rust       # core + same-SHA native adapter ledger
make coverage-python     # pytest-cov on graphforge-bindings-py/python/graphforge
make coverage-node       # c8 on @curatelabs/graphforge lib/ (needs *.node)
make coverage-report     # open Python HTML report
make coverage-diff       # changed Python wrapper files only

# Parallel (4× faster for TCK)
pytest tests/ -n auto
```

## Resumable full validation

`make pre-push` is the full local gate. It begins with a prerequisite and disk
preflight (including `bazelisk` on `PATH`), then records content-addressed
evidence for policy checks, Rust tests and coverage, the instrumented native
Python and Node builds consumed by acceptance, wrapper coverage, API BDD
acceptance, and coverage thresholds. `make pre-push-fast` (also invoked from the
policy-static stage) runs bazelisk presence + `cargo-bazel-drift-check.py`
before format/lint/security. Optional authoritative local Bazel suite:
`make bazel-test` → `bazelisk test //:ci_rust_tests` (see [bazel.md](bazel.md)).
It never skips a gate: a compatible passed stage is reused only when its exact
inputs, command contract, toolchain, dependency evidence, and required native
artifact identity still match.

The human-readable stage lines and machine-readable summary report elapsed time,
evidence hit or miss, identity digest, invalidation reason, and disk budget.
They are stored locally at `.graphforge/validation/v1/summary.json`; standalone
preflight writes `preflight-summary.json` so it cannot replace the full-run
outcome. These paths are ignored by Git and the evidence contains no command
output or secrets. The instrumented Rust coverage run executes the full Rust
corpus once and builds each native artifact once; later acceptance stages reuse
those exact artifact identities.
Compatible Cargo dependency compilation is shared beneath the common Git
metadata directory, while evidence and native binding artifacts stay scoped to
their individual worktree.

Run `make pre-push-preflight` to check those prerequisites and disk budget
without starting any heavy compilation.

Use `make pre-push-clean` to discard only this local validation evidence and
force every stage to rerun. If the preflight reports insufficient space, it does
not start compilation. Review its reported safe options first: `make
clean-builds` removes stale Rust artifacts when possible, while `make
clean-builds-all` removes all Rust artifacts and forces future recompilation.
Neither command is run automatically.

### Core Fixtures (`tests/conftest.py`)

```python
@pytest.fixture
def db():
    """Fresh in-memory GraphForge instance."""
    return GraphForge()

@pytest.fixture
def tmp_db(tmp_path):
    """GraphForge instance backed by a temporary Parquet directory."""
    return GraphForge(str(tmp_path / "graph"))
```

---

## Quality Gates

### Coverage Requirements

Local `make coverage` instruments the shipped surfaces independently. The Rust
run builds PyO3 and napi-rs once with LLVM instrumentation and executes the real
Python and Node native acceptance suites against those exact artifacts. The
command defaults to an isolated target under `build/coverage-rust/`; set
`CARGO_TARGET_DIR` to choose a different isolated location.

| Surface | Tool | Scope | Threshold |
|---------|------|-------|-----------|
| Core Rust | `cargo llvm-cov` | workspace Rust excluding binding adapters | ≥85% lines |
| Python adapter Rust | `cargo llvm-cov` + native acceptance | `graphforge-bindings-py` Rust | ≥80% lines |
| Node adapter Rust | `cargo llvm-cov` + native acceptance | `graphforge-bindings-node` Rust | ≥80% lines |
| Python | `pytest-cov` | `crates/graphforge-bindings-py/python/graphforge` | ≥85% lines; ≥90% patch on changed wrapper files |
| Node | `c8` | hand-written `@curatelabs/graphforge` `lib/**/*.mjs` | ≥85% lines |

`build/coverage-rust/ledger.json` is the machine-readable source of truth. It
records the exact source SHA, toolchain, artifact hashes, profile counts, and
separate core, adapter, and merged-workspace totals. `summary.txt` is the human
view and `lcov.info` is the merged report. A high workspace average never
substitutes for a missing profile or a failed adapter floor; stale, empty,
wrong-artifact, and wrong-SHA evidence fails closed.

Override floors with `COVERAGE_FAIL_UNDER_RUST`,
`COVERAGE_FAIL_UNDER_RUST_PYTHON_ADAPTER`,
`COVERAGE_FAIL_UNDER_RUST_NODE_ADAPTER`, `COVERAGE_FAIL_UNDER_PYTHON`, and
`COVERAGE_FAIL_UNDER_NODE`.

### Required Checks (all PRs)

1. `cargo clippy --workspace -- -D warnings` — zero warnings
2. `cargo test --workspace` — all Rust tests pass
3. `pytest -m unit` — all Python unit tests pass
4. `pytest -m integration` — all Python integration tests pass
5. `pytest -m tck` — all non-skipped TCK scenarios pass
6. `make coverage` — coverage thresholds met
7. `make lint` and `make type-check` — zero issues

---

## TCK Coverage Matrix

Maintain `tests/tck/coverage_matrix.json`:

```json
{
  "tck_version": "2024.2",
  "features": {
    "Match1_Nodes": {
      "status": "supported",
      "scenarios": {
        "Match single node": "pass",
        "Match node with label": "pass"
      }
    },
    "Match3_VariableLength": {
      "status": "supported"
    }
  }
}
```

When the Rust core implements a feature, verify the corresponding TCK scenarios
pass end-to-end before marking `"status": "supported"`.

---

## CI/CD

GitHub Actions runs the full suite on every PR:

```yaml
jobs:
  rust:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo clippy --workspace -- -D warnings
      - run: cargo test --workspace

  python:
    runs-on: ${{ matrix.os }}
    strategy:
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
        python-version: ["3.10", "3.11", "3.12", "3.13"]
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: actions/setup-python@v5
        with:
          python-version: ${{ matrix.python-version }}
      - run: pip install uv && uv sync --all-extras
      - run: maturin develop --release
      - run: make pre-push
```

---

## Known Issues

### pytest-xdist + pytest-cov deadlock on macOS / Python 3.13

**Symptom:** `make pre-push` hangs at the end of the test run — progress reaches
~100% then freezes. CPU drops to 0%. Only `kill` escapes it.

**Root cause:** `pytest-cov` collects coverage data from xdist workers via IPC
sockets. When workers close their sockets, a coverage collection thread in the
main process blocks on `read()`, deadlocking with the main thread. Reproduced on
macOS (Darwin 25.x) + Python 3.13 + pytest-cov 7.0.0 + pytest-xdist 3.x.

**Solution (current `Makefile`):** Run coverage serially, skipping SNAP tests:

```makefile
coverage:
    uv run pytest tests/unit tests/integration -m "not snap" \
        --cov=src --cov-branch \
        --cov-report=term-missing --cov-report=xml
```

The serial run is ~60 s slower than the parallel baseline but avoids the deadlock.
If the upstream `pytest-cov` / `pytest-xdist` fix lands, re-evaluate.

---

## References

- [pytest documentation](https://docs.pytest.org/)
- [openCypher TCK](https://github.com/opencypher/openCypher/tree/master/tck)
- [Hypothesis documentation](https://hypothesis.readthedocs.io/)
- [cargo test documentation](https://doc.rust-lang.org/cargo/commands/cargo-test.html)
