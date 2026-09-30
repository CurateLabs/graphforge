# Contributing to GraphForge

Thank you for your interest in contributing to GraphForge!

GraphForge is a Rust core with thin Python and Node bindings. The current
public release is **v0.5.1**. Develop and verify from source on `main` for
engine and binding work.

| Branch | Role |
|--------|------|
| `main` | Current product line (Rust core, Arrow results, Parquet projects, analyst verbs) |

**Next steps for contributors:** set up the environment below → run the validation
suite → open a focused PR against `main`. For release operators, start at
[Publishing](../engineering/PUBLISHING.md) and
[release process](release-process.md).

---

## Development Setup

**Prerequisites:** Python 3.10+, Rust stable (pinned by `rust-toolchain.toml`),
[uv](https://github.com/astral-sh/uv), maturin, pnpm for the Node binding, and
[cargo-nextest](https://nexte.st/). Cargo with nextest is the CI Rust
compile/test authority
([ADR 0048](../adr/0048-cargo-is-the-ci-build-authority.md)); the exact CI
commands are in [agent-environment.md](agent-environment.md#rust-test-gate).

```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update stable

# Install cargo-nextest
cargo install --locked cargo-nextest

git clone https://github.com/CurateLabs/graphforge.git
cd graphforge

# Install Python dev dependencies
uv sync --dev

# Build and install the native Python extension
maturin develop --release -m crates/graphforge-bindings-py/Cargo.toml

# Verify
cargo test --workspace
python -c "import graphforge; print(graphforge.__version__)"
make pre-push-fast
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
make pre-push-fast   # policy/inventory checks, then ruff format/lint/security/…
make pre-push
```

`make pre-push` mirrors the CI gate for the changed surface. The CI Gate Rust
lane's own commands (nextest over the workspace, then the custom-harness
targets and doctests) are in
[agent-environment.md](agent-environment.md#rust-test-gate).

### Running Tests

```bash
# Rust
cargo test --workspace                  # all crates
cargo test -p graphforge-cypher                 # one crate
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
│   ├── graphforge-api/                  # public Rust facade
│   ├── graphforge-cypher/               # openCypher parser
│   ├── graphforge-ir/                   # graph IR
│   ├── graphforge-rel/                  # relational lowering
│   ├── graphforge-exec/                 # execution + analyst verbs
│   ├── graphforge-storage/              # project generations, Arrow schemas, Parquet storage
│   ├── graphforge-knowledge/            # knowledge + epistemic record domains
│   ├── graphforge-bindings-py/          # PyO3 Python binding
│   ├── graphforge-bindings-node/        # napi-rs Node binding
│   └── …
├── packages/                    # Node packaging and agent skills
├── docs/                        # Markdown sources (Starlight syncs an allowlist)
├── docs-site/                   # Astro Starlight site
├── tests/
├── Cargo.toml
└── pyproject.toml               # workspace tooling (not the published wheel)
```

The published `graphforge` wheel is built from `crates/graphforge-bindings-py`. Python and
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

See [`../engineering/TESTING.md`](../engineering/TESTING.md) for the release-prep
testing strategy (layered gates, TCK posture, binding release-candidate evidence),
and [testing.md](testing.md) for command recipes and suite layout.

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
- First-party TypeScript compiler/loader policy:
  [typescript-toolchain.md](typescript-toolchain.md)

---

## Pull Request Process

### PR Size Guidelines

Keep each PR focused on one issue and one concern, with the tests needed to
prove its acceptance criteria. Size is advisory: split XL work or independently
reviewable concerns when the review benefit justifies another CI cycle.

**Good:**
- Single feature or bug fix
- Clear, focused purpose
- Acceptance criteria covered by tests or deterministic evidence

**Too large:**
- Multiple unrelated changes
- Refactoring + new feature + bug fixes combined

For example, a bounded Cypher feature may need parser, IR, lowering, execution,
and regression-test changes in one PR to demonstrate working behavior. Do not
split solely at crate boundaries or defer a change's acceptance tests to a
later PR. When a split is justified, each PR must have independently verifiable
acceptance criteria, and dependencies must be explicit.

### Merge cadence

Finish and merge reviewed work before starting more implementation. Coordinated
agent teams have a limit of three unmerged changes across all agents, including
draft PRs and implemented local branches. The coordinating agent owns the merge
queue; prioritize existing PRs and shared CI blockers, and integrate dependent
changes in order. See [the work-in-progress rules in AGENTS.md](../../AGENTS.md#finish-and-merge-before-starting-more-work)
for counting, the bounded blocker exception, and handling an inherited backlog.

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

See [AGENTS.md](../../AGENTS.md) for agent workflow,
[agent-environment.md](agent-environment.md) for sandbox and native-binding caveats, and
[CONTRIBUTING.md](../../CONTRIBUTING.md) for contribution, conduct, and licensing
onboarding contract.

---

## Design Principles

1. **Spec-driven correctness** — openCypher semantics over performance
2. **Arrow as the data-plane wire contract** — Cypher and analyst/data-bearing results cross language boundaries as Arrow RecordBatch streams; control/metadata/lifecycle/explanation/construction may return scalars, collections, unit, or handles
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

Method and hashes live in the repo; results live on the issue. A development
page records how to reproduce a measurement and the content digests that
identify its inputs. Raw output (JSON, logs, receipts, per-run tables) is
attached to the issue or pull request that produced it, or to a release
artifact when a release claim depends on it, and is never committed under
`docs/`. `scripts/ci/docs-tree-policy.py` fails CI on evidence-shaped or
oversize files under `docs/` and on any `docs/development/*.md` that no other
tracked file references; a page named after an issue is folded into its topic
page or deleted when the issue closes. `docs/` is the only hand-maintained
source: the published site renders an allowlisted subset and keeps no copies.

---

## Releases and Versioning

GraphForge follows [Semantic Versioning](https://semver.org/). The current
coordinated public release is **v0.5.1** (see
[installation](../guide/installation.md)).

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
