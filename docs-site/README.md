# GraphForge docs site (Astro Starlight)

Public documentation site for GraphForge. Most Markdown sources live in `../docs/`;
this package syncs an allowlisted subset into `src/content/docs/` at build time. The VS Code
extension guide comes from a checked-in snapshot of the extension repository's public-only
`docs/published/` subtree, guarded by the immutable revision and SHA-256 checksums in
`external-docs.json`.

## Run locally

From the **repository root** (not this directory):

```bash
pnpm install
pnpm docs:dev          # http://localhost:4321/
pnpm docs:build        # output: docs-site/dist/
pnpm docs:check-links  # after build: zero broken same-site hrefs
pnpm docs:preview
```

Or: `make docs-serve` / `make docs-build` / `make docs-clean`.

## Layout

| Path | Role |
| --- | --- |
| `astro.config.mjs` | Starlight config and **reader-journey** sidebar |
| `scripts/sync-content.mjs` | Allowlist sync from `docs/` → content collection; rewrites `.md` links to site paths |
| `external-docs.json` | Pinned `graphforge-vscode` published-page allowlist, destinations, and checksums |
| `external/graphforge-vscode/` | Mechanically generated, build-verifiable public extension snapshot |
| `scripts/update-extension-docs.mjs` | Authenticated refresh command; rejects changes to the published-file contract |
| `scripts/test-extension-docs.mjs` | Offline allowlist, checksum, destination, and private-marker contract test |
| `scripts/check-links.mjs` | Post-build internal link + stale-base checker |
| `src/content/docs/` | Generated — do not edit by hand |

Published sidebar order (reader experience): **Get started**, **Use every day**,
**Understand**, **Reference**, then collapsed **Contribute & operate**,
**Engineering** (includes ADRs), and **Community**.

On-disk authoring trees remain Guide / Book / Reference / `engineering/`
(+ `adr/`, `development/`); product/strategy DocSlime content is not published
from this repo.

## Updating the extension guide

After public extension documentation changes, authenticate `gh` for the CurateLabs repository
and run `pnpm docs:update-extension <full-graphforge-vscode-commit-sha>`. The command reads only
`docs/published/`, refreshes the checked-in snapshot, and updates its SHA-256 values. It stops
if the upstream published-file set changed so additions require an explicit destination and
review. Never use a branch name or manually add private extension documents. Run
`pnpm docs:build` and `pnpm docs:check-links`; normal builds are offline and fail on a missing
snapshot, mutable revision, unexpected allowlist, or checksum mismatch.
