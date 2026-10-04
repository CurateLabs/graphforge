# Contributing to GraphForge

Thank you for your interest in contributing to GraphForge.

Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).
GraphForge is open source under the Apache License 2.0 (`Apache-2.0`).
Contributions are accepted under the same terms — see [License](#license)
below and [licensing details](docs/legal/licensing.md).

## Contributor guide

GraphForge is a **Rust-owned** workspace: product behavior lives in `crates/`;
Python and Node are thin bindings (never fallback engines). The full contributor
guide — prerequisites (Rust toolchain pinned by `rust-toolchain.toml`, Python
3.10+, uv, maturin; pnpm for the Node binding), layout, validation gates, PR
process, and design principles — is:

**[docs/development/contributing.md](docs/development/contributing.md)**

TypeScript first-party policy (compiler 5.9.3, `tsx`, no `ts-node`):
[docs/development/typescript-toolchain.md](docs/development/typescript-toolchain.md).

Documentation records method, not results. A page under `docs/development/`
says how to reproduce a measurement and which content digests identify its
inputs; the raw output (JSON, logs, receipts, run tables) goes on the issue or
pull request that produced it, never into the tree. CI rejects evidence-shaped
or oversize files under `docs/` and any development page nothing links to
(`scripts/ci/docs-tree-policy.py`). Per-issue pages are folded into their topic
page or deleted when the issue closes, and every ADR carries `status`,
`superseded_by`, and a `revisit_when` trigger.

Agent-oriented workflow and architecture rules are in [AGENTS.md](AGENTS.md).

### Validation (summary)

Before pushing, run the Rust gates appropriate to the changed surface, then the
Python/workspace mirror. Cargo with nextest is the CI build authority (ADR 0048);
[docs/development/agent-environment.md](docs/development/agent-environment.md#rust-test-gate)
gives the exact commands the CI Gate Rust lane runs.

```bash
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
make check   # Python format/lint/type-check/security, workflow lint, repo-checks
```

## Getting help

- **Questions:** [GitHub Discussions](https://github.com/CurateLabs/graphforge/discussions)
- **Bugs:** [GitHub Issues](https://github.com/CurateLabs/graphforge/issues)
- **Security:** report privately via [SECURITY.md](.github/SECURITY.md)
- **Conduct:** [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

## License

GraphForge is licensed under `Apache-2.0`. Under Section 5 of that license,
unless explicitly stated otherwise, any contribution intentionally submitted
for inclusion in GraphForge is provided under Apache-2.0 without additional
terms. You retain ownership of your contribution and must have the right to
submit it; contributions made within the scope of employment require employer
authorization.

See [LICENSE](LICENSE) for the authoritative terms and
[licensing details](docs/legal/licensing.md) for package and third-party
attribution guidance.
