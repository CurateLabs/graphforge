# Documentation

The public Starlight site targets **GraphForge v0.6.0**. It maintains one current
user guide set, without pre-v1 legacy instructions or migration promises.

## Reader journeys

Basic graph use is the first complete experience. Further journeys are optional:
users and agents do not need the full capability set to obtain a useful result.

| Navigation                         | Reader and purpose                                                                                                                                                                                                             |
| ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Basic                              | Nontechnical analyst working with an agent; no programming prerequisite: [first graph](guide/quickstart.md), [agent-assisted use](guide/work-with-an-agent.md), [first mixed-methods project](guide/first-research-project.md) |
| Advanced                           | Reader with basic Python, database, and terminal skills: [start here](guide/advanced.md), then choose a task                                                                                                                   |
| Internals (specialist)             | Architecture and implementation contracts; outside both learning paths                                                                                                                                                         |
| API and feature reference          | Lookup material for a specific operation or constraint                                                                                                                                                                         |
| Contribute & operate / Engineering | Contributor setup, testing, implementation decisions, and publishing                                                                                                                                                           |
| Community                          | Licensing, security, and conduct                                                                                                                                                                                               |

Both learning paths explain their subject without assuming expert knowledge.
Basic keeps commands in optional helper instructions and lets readers check the
meaning of results without reading code. Advanced may use code and database
vocabulary, but introduces GraphForge-specific terms before relying on them.
Do not relabel specialist contracts as Advanced tutorials without teaching the
concepts and showing a bounded task.

Introduce a concept at the task that needs it. Keep low-level request shapes and
receipts in references. A simple graph-model inquiry is distinct from native
immutable knowledge or research history; name that boundary explicitly.

## Authoring and publication

Sources live in `docs/guide`, `docs/book`, `docs/reference`, and supporting
engineering/development directories. Edit those sources, not generated
`docs-site/src/content/docs` files. `docs-site/scripts/sync-content.mjs` publishes
an allowlist; a new public page also needs an entry there and a discoverable
navigation or guide link in `docs-site/astro.config.mjs` or an existing page.

The VS Code guide is a pinned public snapshot from its owning repository.
`docs-site/external-docs.json` records the revision, checksums, and local patches.
Use the refresh procedure in [the site README](../docs-site/README.md).

## Preview and validate

From the repository root, with Node 22.12+ and pnpm:

```bash
pnpm install --frozen-lockfile
pnpm docs:dev
pnpm docs:build
pnpm docs:check-links
```

The development server defaults to port 4321. Execute changed examples against
the intended binding build; a successful site build checks rendering, not
engine behavior or human comprehension. Candidate-wide first-use qualification
belongs to [#1209](https://github.com/CurateLabs/graphforge/issues/1209).

## Conventions

- **Keep docs current.** When behavior changes, update the doc in the same change.
- **Link, don't duplicate.** Deep dives stay in `book/`; published pages stay focused on
  current product behavior and contributor operations. Planned requirements are
  explicitly marked and integrated into the relevant product and architecture docs;
  they are not claims of implementation.
- **Decisions are recorded.** Significant choices get ADRs under [`adr/`](adr/), indexed from
  [`engineering/adrs/`](engineering/adrs/).
- **Site tooling is separate.** Starlight config under `docs-site/` owns published nav.
  `docs/` is the single hand-maintained source; the site renders an allowlisted
  subset at build time and holds no second copy of Guide, Book, or ADR content.
- **Results live on the issue.** Development pages record method and content
  digests; raw measurement output attaches to the issue, PR, or release that
  produced it. `scripts/ci/docs-tree-policy.py` rejects evidence-shaped or
  oversize files under `docs/` and unreferenced `development/*.md` pages (#1625).
- **Issue closure.** Docs and legal issues stay open until **manual approval**; PRs use
  `Refs #<issue>`, not `Closes`.
