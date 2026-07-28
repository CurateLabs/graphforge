# Contributing to GraphForge

Thank you for your interest in contributing to GraphForge.

Participation is governed by the [Code of Conduct](CODE_OF_CONDUCT.md).
GraphForge is source-available under `BUSL-1.1`, and contributions are accepted
under the [Contributor License Agreement](CLA.md) — see
[License](#license) below and [licensing details](docs/legal/licensing.md).

## Contributor guide

GraphForge is a **Rust-owned** workspace: product behavior lives in `crates/`;
Python and Node are thin bindings (never fallback engines). The full contributor
guide — prerequisites (Rust toolchain pinned by `rust-toolchain.toml`, Python
3.10+, uv, maturin; pnpm for the Node binding), layout, validation gates, PR
process, and design principles — is:

**[docs/development/contributing.md](docs/development/contributing.md)**

Agent-oriented workflow and architecture rules are in [AGENTS.md](AGENTS.md).

### Validation (summary)

Before pushing, run the Rust gates appropriate to the changed surface, then the
Python/workspace mirror:

```bash
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
make pre-push
```

## Getting help

- **Questions:** [GitHub Discussions](https://github.com/CurateLabs/graphforge-legecy/discussions)
- **Bugs:** [GitHub Issues](https://github.com/CurateLabs/graphforge-legecy/issues)
- **Security:** report privately via [SECURITY.md](.github/SECURITY.md)
- **Conduct:** [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

## License

GraphForge is source-available under `BUSL-1.1`. External contributors must
accept the [Contributor License Agreement](CLA.md), including its relicensing
grant, before a contribution may merge. Contributions made within the scope of
employment require employer authorization or an accepted corporate agreement.

The repository is currently private and does not accept external contributions
until the CLA service and required status check are enabled. Curate Labs
employees, authorized contractors, and explicitly approved dependency-update
bots follow separate authorization records.

`BUSL-1.1` is not an OSI-approved open-source license before a release's Change
Date, after which that release converts to `AGPL-3.0-only`. The scope of free
use, the Additional Use Grant, and when commercial terms are required are
described in [LICENSE](LICENSE) and summarized in
[licensing details](docs/legal/licensing.md).
