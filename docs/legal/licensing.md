# GraphForge Licensing

GraphForge `main` and the v0.5.x release line are open source under the
Apache License 2.0 (`Apache-2.0`). The repository-root [LICENSE](../../LICENSE)
contains the authoritative terms.

## Covered first-party work

The Apache-2.0 grant covers the first-party source and distributions in this
repository, including the Rust engine, language bindings, command-line tools,
agent skills, build and release tooling, and first-party documentation.
Published Cargo, Python, and npm package metadata reports `Apache-2.0` and the
packages include `LICENSE` and `NOTICE`.

The Apache license permits use, modification, redistribution, and commercial
use subject to its conditions. It does not grant rights to Curate Labs or
GraphForge trademarks beyond the license's customary descriptive-use terms.

Commercial extensions, managed services, support, or other products distributed
separately from this repository are not part of the covered work and may have
their own terms. Their existence does not narrow the Apache-2.0 rights granted
for this repository.

## Third-party open-source dependencies

Third-party crates and other incorporated materials retain their own licenses.

- The SPDX allowlist and CI gate are defined in [`deny.toml`](../../deny.toml)
  and run with `cargo deny check licenses` / `make cargo-deny-licenses`.
- Generated attribution texts for binary redistributions are in
  [`legal/THIRD_PARTY_NOTICES.md`](../../legal/THIRD_PARTY_NOTICES.md).
  Regenerate them after dependency changes with `make third-party-notices`.
- Published Python wheels, native Node addons, and CLI packages include the
  relevant third-party notices alongside `LICENSE` and `NOTICE`.
- Vendored openCypher TCK material under `tests/tck/` keeps its own Apache-2.0
  `LICENSE` and `NOTICE`.

## Previously published MIT artifacts

Releases through v0.4.0 were published under the MIT License. Those tags,
packages, and source archives retain the rights included with their artifacts;
the repository does not rewrite history or replace those grants. v0.5.0 is the
first GraphForge release line under Apache-2.0.

## Release controls

Release preparation verifies the license and packaged attribution surfaces:

```bash
python3 scripts/license_check.py --report license-compliance-report.json
make package-license-verify
cargo deny check licenses
python3 scripts/generate_third_party_notices.py --check
```

The release workflow blocks publication if first-party metadata, canonical
license copies, notices, or documentation drift.

## Contributions

Under Section 5 of Apache-2.0, unless explicitly stated otherwise, any
contribution intentionally submitted for inclusion in GraphForge is provided
under Apache-2.0 without additional terms. Contributors retain ownership and
must have the right to submit their work, including any required employer
authorization. See [CONTRIBUTING.md](../../CONTRIBUTING.md).

This page is an engineering summary, not legal advice. `LICENSE` controls.
