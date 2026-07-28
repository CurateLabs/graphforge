# GraphForge Licensing

GraphForge `main` is source-available under the Business Source License 1.1
(`BUSL-1.1`). **v0.5.0 is the first GraphForge line under BUSL-1.1** (the first
BUSL-1.1 publication line). The Business Source License is not an OSI-approved
open-source license before the applicable Change Date.

## Current license

- Licensor: Curate Labs Inc.
- Licensed Work: GraphForge first-party source and distributions identified by
  the commit or release tag that contains the license.
- Change License: GNU Affero General Public License v3.0 only
  (`AGPL-3.0-only`).
- Change Date: three calendar years after each release’s recorded release date.
  A February 29 release changes on February 28 when the third anniversary year
  is not a leap year.

The authoritative terms and the exact Additional Use Grant are in the
repository-root [`LICENSE`](https://github.com/CurateLabs/graphforge-legecy/blob/main/LICENSE).
The machine-readable release parameters are in
[`license-policy.json`](https://github.com/CurateLabs/graphforge-legecy/blob/main/license-policy.json).

## Additional Use Grant

The license permits non-production use. Its Additional Use Grant also permits:

- non-commercial production use by students, academic institutions, and active
  non-commercial open-source projects as defined in `LICENSE`; and
- commercial internal production use only while the workload stays at or below
  both 500,000 aggregate nodes and edges and three concurrently active
  GraphForge process instances.

A separate commercial license is required for commercial internal use above
either limit, any hosted GraphForge-powered service offered to third parties,
or embedding GraphForge in a closed-source product commercially supplied to
third parties.

The restrictions are contractual. GraphForge does not add telemetry, remote
metering, a kill switch, or collection of graph contents to enforce them.

## Third-party OSS dependencies

Third-party crates remain under their own licenses. GraphForge does not relicense
them.

- SPDX allowlist and CI gate: [`deny.toml`](https://github.com/CurateLabs/graphforge-legecy/blob/main/deny.toml)
  via `cargo deny check licenses` / `make cargo-deny-licenses`.
- Generated attribution texts for binary redistributions (Python wheels, native
  Node addons, CLI): [`legal/THIRD_PARTY_NOTICES.md`](https://github.com/CurateLabs/graphforge-legecy/blob/main/legal/THIRD_PARTY_NOTICES.md).
  Regenerate after dependency changes with `make third-party-notices`
  (requires [`cargo-about`](https://github.com/EmbarkStudios/cargo-about)).
- First-party `NOTICE` points at that inventory; published binary packages include
  `THIRD_PARTY_NOTICES.md` alongside `LICENSE` and `NOTICE`.
- Vendored openCypher TCK material under `tests/tck/` keeps its own Apache-2.0
  `LICENSE` and `NOTICE`.

Python and Node package metadata for published GraphForge packages report
`BUSL-1.1`. Their runtime/tooling dependencies (for example PyArrow on PyPI, or
dev-only npm packages) retain their upstream licenses; consult those packages’
own metadata when redistributing them separately.

## Previously published MIT artifacts

Prior releases through `v0.4.0` shipped under MIT (`license-policy.json`
`legacy_mit_boundary`). Those tags, published packages, and source archives keep
the MIT rights included with the artifacts; the repository does not rewrite
history or replace them. **v0.5.0** is the first GraphForge line under
BUSL-1.1.

## Release controls

Release preparation must run:

```bash
python3 scripts/license_policy.py generate \
  --release-version X.Y.Z \
  --release-date YYYY-MM-DD
python3 scripts/license_policy.py check \
  --report license-compliance-report.json
cargo deny check licenses
python3 scripts/generate_third_party_notices.py --check
```

Generation updates the version, release date, three-year Change Date, policy
digests, and root license together. The release workflow blocks package builds
and publication when the checked-in policy, canonical terms, manifests, or
documentation drift.

## Contributions

Future external contributors must accept the repository’s
[Contributor License Agreement](https://github.com/CurateLabs/graphforge-legecy/blob/main/CLA.md).
Because this repository is
currently private, external contributions remain disabled until a CLA service
and required status check are configured and their audit export is stored
under Curate Labs control.

## Authority record

On July 24, 2026, David Spencer, owner of Curate Labs Inc. and its Chairman and
Chief Executive Officer, directed the prospective MIT-to-BUSL transition for
GraphForge. Repository history at the transition boundary contains
contributions authored by David Spencer plus automated dependency-update
commits; third-party materials retain their own licenses.

This page is an engineering summary, not legal advice. The `LICENSE` controls.
