# Contributing to GraphForge

Thank you for your interest in contributing to GraphForge!

GraphForge **v0.5.0** is a Rust core with thin Python and Node bindings. All
product work targets `main`.

| Branch | Role |
|--------|------|
| `main` | Current product line (Rust core, Arrow results, Parquet projects, analyst verbs) |

---

## Development Setup

**Prerequisites:** Python 3.10+, Rust stable, [uv](https://github.com/astral-sh/uv), maturin

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update stable

git clone https://github.com/CurateLabs/graphforge.git
cd graphforge

# Install Python dev dependencies
uv sync --dev

# Build and install the native Python extension
maturin develop --release -m crates/gf-bindings-py/Cargo.toml

# Verify
cargo test --workspace
python -c "import graphforge; print(graphforge.__version__)"
```

See [Installation](../guide/installation.md) for the published-package path.

---

## Development Workflow

### Before Pushing Code

**Always run the full validation suite before pushing:**

```bash
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
make pre-push
```

`make pre-push` mirrors the CI gate for the changed surface.

### Running Tests

```bash
# Rust
cargo test --workspace                  # all crates
cargo test -p gf-cypher                 # one crate
cargo test --workspace -- --nocapture   # with output

# Python binding / workspace checks
make test
make test-unit
```

### Code Quality

```bash
# Rust
cargo fmt --all
cargo clippy --workspace -- -D warnings

# Python tooling (ruff / mypy via make targets)
make format
make lint
make type-check
```

---

## Project Structure

```
graphforge/
├── crates/                      # Rust workspace
│   ├── gf-api/                  # public Rust facade
│   ├── gf-cypher/               # openCypher parser
│   ├── gf-ir/                   # graph IR
│   ├── gf-rel/                  # relational lowering
│   ├── gf-exec/                 # execution + analyst verbs
│   ├── gf-storage/              # StorageProvider + Parquet
│   ├── gf-knowledge/            # M20/M21 record domains
│   ├── gf-bindings-py/          # PyO3 Python binding
│   ├── gf-bindings-node/        # napi-rs Node binding
│   └── gf-bindings-uniffi/      # UniFFI shared binding (Swift + Kotlin)
├── packages/                    # Node packaging and agent skills
├── docs/                        # Markdown sources (Starlight syncs an allowlist)
├── docs-site/                   # Astro Starlight site
├── tests/
├── Cargo.toml
└── pyproject.toml               # workspace tooling (not the published wheel)
```

The published `graphforge` wheel is built from `crates/gf-bindings-py`. Python and
Node are thin bindings — never fallback engines.

---

## Testing Guidelines

### Writing Tests

Prefer Rust tests colocated with the module under test for core behavior.
Binding tests exercise the thin adapter surface only.

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_scan_op() {
        let op = GraphOp::NodeScan { var: VarId(0), ty: TypeId(1) };
        assert!(matches!(op, GraphOp::NodeScan { .. }));
    }
}
```

### Test Quality Standards

- Fast where isolation allows
- Isolated: no shared mutable state between tests
- Deterministic: same input = same output
- Named descriptively

See [`../engineering/TESTING.md`](../engineering/TESTING.md) for the v0.5.0 /
release-prep testing strategy (layered gates, TCK posture, Binding RC), and
[testing.md](testing.md) for command recipes and suite layout.

---

## Code Style

### Rust

- `cargo fmt` enforced in CI
- `cargo clippy -- -D warnings` enforced in CI
- No `#[allow(dead_code)]` without explanation
- Public items need doc comments

### Python / TypeScript bindings

- Thin adapters only — no semantic ownership in the binding layer
- Type hints / typed APIs on public surfaces
- No `# type: ignore` without explanation

---

## Pull Request Process

### PR Size Guidelines

Keep PRs small and focused. CI tools work best with small, reviewable diffs.

**Good:**
- Single feature or bug fix
- Clear, focused purpose
- Acceptance criteria covered by tests or deterministic evidence

**Too large:**
- Multiple unrelated changes
- Refactoring + new feature + bug fixes combined

**Example — breaking up a Rust feature:**

```
PR 1: "gf-cypher: add WITH clause grammar and AST node"
PR 2: "gf-ir: add WithOp graph IR operator"
PR 3: "gf-rel: lower WITH to DataFusion logical plan"
PR 4: "gf-exec: integrate WITH in execution pipeline"
PR 5: "tests: WITH clause unit + integration + TCK"
```

### No Bandaid Fixes

Fix problems properly, not with temporary workarounds. Investigate root causes,
add regression tests, and keep CI checks enabled.

### PR Requirements

All PRs must:

- Pass required CI checks for the changed surface
- Include tests or deterministic evidence for new behavior
- Update relevant documentation
- Have a clear description
- Reference the issue number in the commit and PR body (`Closes #XX` or `Refs #XX`)

See [AGENTS.md](../../AGENTS.md) for agent workflow and
[CONTRIBUTING.md](../../CONTRIBUTING.md) for contribution, conduct, and licensing
onboarding contract.

---

## Design Principles

1. **Spec-driven correctness** — openCypher semantics over performance
2. **Arrow as the wire contract** — results cross language boundaries as Arrow RecordBatch streams
3. **GraphForge owns the semantics** — no binding or storage provider becomes the semantic owner
4. **Surfaces stay independent** — analyst verbs bypass the Cypher parser; they do not rewrite it
5. **Inspectable** — `explain` at every compiler stage; structured errors with spans

---

## openCypher TCK Compliance

When implementing openCypher features:

1. Check the TCK coverage matrix and related conformance docs under `docs/reference/`
2. Mark features as supported, planned, or unsupported as appropriate
3. Add corresponding TCK / regression coverage
4. Ensure semantic correctness per the openCypher specification

Supported features must pass their TCK scenarios — this is a hard merge gate.

---

## Documentation

### Code documentation

- Rust: doc comments (`///`) on all public items; `cargo doc` must build cleanly
- Bindings: document only the thin public adapter surface

### Project documentation

When adding features, update:

- `docs/book/architecture/` — if the change affects the compiler pipeline, storage, or execution model
- `docs/reference/` — if the public API changes
- `CHANGELOG.md` (`[Unreleased]` section)

---

## Releases and Versioning

GraphForge follows [Semantic Versioning](https://semver.org/). The current public
product line is **v0.5.0**.

When submitting PRs, update the `[Unreleased]` section of `CHANGELOG.md`:

```markdown
## [Unreleased]

### Added
- New feature you implemented

### Fixed
- Bug you fixed
```

See [release-process.md](release-process.md) for the full release procedure and
[roadmap.md](../releases/roadmap.md) for delivery sequencing.

---

## Getting Help

- **Questions:** [GitHub Discussions](https://github.com/CurateLabs/graphforge/discussions)
- **Bugs:** [GitHub Issues](https://github.com/CurateLabs/graphforge/issues)

## License

GraphForge is open source under the Apache License 2.0 (`Apache-2.0`). Under
Section 5 of that license, intentionally submitted contributions are provided
under Apache-2.0 unless explicitly stated otherwise. Contributors retain
ownership and must have the right to submit their work; contributions made
within the scope of employment require employer authorization.
