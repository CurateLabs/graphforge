# GraphForge docs site (Astro Starlight)

Public documentation site for GraphForge. Markdown sources live in `../docs/`;
this package syncs an allowlisted subset into `src/content/docs/` at build time.

## Run locally

From the **repository root** (not this directory):

```bash
pnpm install
pnpm docs:dev          # http://localhost:4321/graphforge-legecy/
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
| `scripts/check-links.mjs` | Post-build internal link + stale-base checker |
| `src/content/docs/` | Generated — do not edit by hand |

Published sidebar order (reader experience): **Get started**, **Use every day**,
**Understand**, **Reference**, then collapsed **Contribute & operate**,
**Engineering** (includes ADRs), and **Community**.

On-disk authoring trees remain Guide / Book / Reference / `engineering/`
(+ `adr/`, `development/`); product/strategy DocSlime content is not published
from this repo.
