# Documentation

The **published Starlight site** is organized around **reader journeys** (Diátaxis-aligned):

1. **Get started** — install, quickstart, tutorial
2. **Use every day** — Cypher, construction, analytics, datasets
3. **Understand** — architecture Book, use cases, research
4. **Reference** — API, compatibility, TCK, changelog
5. **Contribute & operate** — development, testing, release, this map
6. **Engineering** — contributor lifecycle summaries and ADRs
7. **Community** — licensing, security, code of conduct

On disk, published sources live in Guide / Book / Reference / `engineering/` plus supporting
folders. This repository contains only current user and contributor documentation.

The Astro Starlight site (`docs-site/`) syncs an allowlisted subset of these trees.
Sidebar labels follow reader journeys; **content paths / URLs stay on the Guide / Book /
Reference / engineering layout** so existing public links remain stable.

## Local docs site

From the repository root (requires Node ≥ 22.12 and the repo `pnpm` workspace):

```bash
pnpm install
pnpm docs:dev          # http://localhost:4321/
pnpm docs:build        # output: docs-site/dist/
pnpm docs:check-links  # after build: zero broken same-site hrefs
pnpm docs:preview      # serve the build output
```

Makefile shortcuts: `make docs-serve`, `make docs-build`, `make docs-clean`.

Markdown sources stay in `docs/`; `docs-site/scripts/sync-content.mjs` copies the
allowlist into the Starlight content collection before `dev` / `build`. It also imports the
four public extension pages from a checked-in snapshot of
`CurateLabs/graphforge-vscode/docs/published/`. The authenticated
`pnpm docs:update-extension <full-commit-sha>` command refreshes that snapshot; normal builds
verify the immutable revision and checksums recorded in `docs-site/external-docs.json`. No other
extension documents are eligible for publication.

## Published reader map

| Journey | Start here |
| --- | --- |
| Get started | [`guide/installation.md`](guide/installation.md), [`guide/quickstart.md`](guide/quickstart.md), [`guide/tutorial.md`](guide/tutorial.md) |
| Use every day | [`guide/overview.md`](guide/overview.md), Cypher / construction / analytics / [`guide/datasets/`](guide/datasets/overview.md) |
| Understand | [`book/README.md`](book/README.md), architecture / use cases / research |
| Reference | [`reference/api.md`](reference/api.md) and siblings |
| Contribute & operate | [`development/contributing.md`](development/contributing.md), release docs, [`releases/roadmap.md`](releases/roadmap.md) |
| Engineering | [`engineering/`](engineering/README.md) (ADRs under Engineering), [`adr/`](adr/) |
| Community | [`legal/licensing.md`](legal/licensing.md), [`community/`](community/) |

## On-disk authoring trees

### Guide (basic usage)

| Path | Contents |
| --- | --- |
| [`guide/installation.md`](guide/installation.md) | Install via pip or uv |
| [`guide/quickstart.md`](guide/quickstart.md) | First graph in minutes |
| [`guide/tutorial.md`](guide/tutorial.md) | Guided walkthrough |
| [`guide/overview.md`](guide/overview.md) | Everyday workflows index |
| [`guide/cypher-guide.md`](guide/cypher-guide.md) | openCypher language guide |
| [`guide/graph-construction.md`](guide/graph-construction.md) | Build graphs with API and Cypher |
| [`guide/analytics-integration.md`](guide/analytics-integration.md) | Arrow, pandas, Polars, analyst verbs |
| [`guide/visualization.md`](guide/visualization.md) | Real-data Plotly / Jaal / PyVis / Cytoscape.js / Sigma.js examples |
| [`guide/datasets/`](guide/datasets/overview.md) | Load real-world networks |

### Book (research, architecture, deeper usage)

| Path | Contents |
| --- | --- |
| [`book/README.md`](book/README.md) | Book map |
| [`book/architecture/`](book/architecture/overview.md) | Pipeline, storage, execution, algorithms, contracts |
| [`book/use-cases/`](book/use-cases/README.md) | Deeper usage narratives |
| [`book/research/`](book/research/README.md) | Present-tense v0.5 research notes behind the use cases |

### Engineering (public contributor lifecycle)

| Document | Question it answers |
| --- | --- |
| [`engineering/ARCHITECTURE.md`](engineering/ARCHITECTURE.md) | Which concepts, boundaries, and components shape the system? |
| [`engineering/TESTING.md`](engineering/TESTING.md) | How do we prove it before release? |
| [`engineering/PUBLISHING.md`](engineering/PUBLISHING.md) | How do verified artifacts reach users safely? |
| [`engineering/OBSERVABILITY.md`](engineering/OBSERVABILITY.md) | How do CI/release signals feed learning? |
| [`engineering/adrs/`](engineering/adrs/) | ADR index (bodies in [`adr/`](adr/) `0001`–`0014`) |

### Supporting public trees

| Folder | Contents |
| --- | --- |
| [`index.md`](index.md) | Site home (reader-journey framed) |
| [`reference/`](reference/api.md) | API, compatibility, TCK, scale limits |
| [`development/`](development/contributing.md) | Contributor and release process detail |
| [`legal/licensing.md`](legal/licensing.md) | Licensing copy |
| [`adr/`](adr/) | ADR bodies (keepers through `0017`) |
| [`releases/roadmap.md`](releases/roadmap.md) | Public product roadmap |

## Conventions

- **Keep docs current.** When behavior changes, update the doc in the same change.
- **Link, don't duplicate.** Deep dives stay in `book/`; published pages stay focused on
  current product behavior and contributor operations.
- **Decisions are recorded.** Significant choices get ADRs under [`adr/`](adr/), indexed from
  [`engineering/adrs/`](engineering/adrs/).
- **Site tooling is separate.** Starlight config under `docs-site/` owns published nav.
- **Issue closure.** Docs and legal issues stay open until **manual approval**; PRs use
  `Refs #<issue>`, not `Closes`.
