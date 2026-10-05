<h1 align="center">GraphForge</h1>

<p align="center">
  <a href="https://pypi.org/project/graphforge/"><img src="https://img.shields.io/pypi/v/graphforge.svg?label=PyPI&logo=pypi" alt="PyPI version" /></a>
  <a href="https://www.npmjs.com/package/@curatelabs/graphforge"><img src="https://img.shields.io/npm/v/%40curatelabs/graphforge.svg?label=npm&logo=npm" alt="npm version" /></a>
  <a href="https://www.npmjs.com/package/@curatelabs/graphforge-cli"><img src="https://img.shields.io/npm/v/%40curatelabs%2Fgraphforge-cli.svg?label=CLI&logo=npm" alt="CLI npm version" /></a>
  <a href="https://www.npmjs.com/package/@curatelabs/graphforge-agent-skills"><img src="https://img.shields.io/npm/v/%40curatelabs%2Fgraphforge-agent-skills.svg?label=skills&logo=npm" alt="agent-skills npm version" /></a>
  <a href="https://github.com/CurateLabs/graphforge/releases/latest"><img src="https://img.shields.io/github/v/release/CurateLabs/graphforge?label=GitHub%20Release&logo=github" alt="GitHub Release" /></a>
  <a href="#installation"><img src="https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?logo=python&logoColor=white" alt="Python 3.10 or newer" /></a>
  <a href="crates/graphforge-bindings-node"><img src="https://img.shields.io/badge/Node.js-20%2B-5FA04E.svg?logo=nodedotjs&logoColor=white" alt="Node.js 20 or newer" /></a>
  <a href="rust-toolchain.toml"><img src="https://img.shields.io/badge/Rust-1.96-000000.svg?logo=rust&logoColor=white" alt="Rust 1.96" /></a>
  <a href="https://github.com/CurateLabs/graphforge/actions/workflows/test.yml"><img src="https://img.shields.io/github/actions/workflow/status/CurateLabs/graphforge/test.yml?branch=main&label=CI%20Gate&logo=github" alt="CI Gate status" /></a>
  <a href="https://app.codspeed.io/CurateLabs/graphforge?utm_source=badge"><img src="https://img.shields.io/endpoint?url=https://codspeed.io/badge.json" alt="CodSpeed" /></a>
  <a href="https://docs.graphforge.sh/"><img src="https://img.shields.io/badge/docs-online-0A66C2.svg" alt="Documentation" /></a>
  <a href="https://docs.graphforge.sh/reference/tck-compliance/"><img src="https://img.shields.io/badge/openCypher%20TCK-regression%20baseline-blue.svg" alt="openCypher TCK" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="Apache License 2.0" /></a>
</p>

<p align="center">
  <strong>Composable graph tooling for analysis, construction, and refinement</strong>
</p>

<p align="center">
  An embedded, openCypher-compatible graph engine with a Rust core, Arrow results,
  and Parquet persistence — for research and investigative workflows
</p>

---

## Table of Contents

- [Why GraphForge?](#why-graphforge)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Datasets](#datasets)
- [Architecture](#architecture)
- [Development](#development)
- [Roadmap](#roadmap)
- [License](#license)

---

## Why GraphForge?

GraphForge is an embedded graph engine for nontechnical analysts working with
an agent, and for technical users who write their own code. Build a graph, ask a
question, and keep what you learn without running a database server. Rust owns
the behavior; durable projects use Parquet-backed storage.

Start with ordinary graph construction and querying. Saving analyses, recording
challenged hypotheses, independent research, and collaboration are successive,
optional journeys. A basic user does not need an ontology, Branch, Version,
Proposal, or decision model.

Use GraphForge to inspect a citation network, analyze dependencies, or connect
an inquiry to its evidence. For a centrally hosted application with many
concurrent users, evaluate an operational database against your workload.
[Scale guidance](docs/reference/scale-limits.md) explains measured scope and limits.

Choose a learning path:

- **[Basic](docs/guide/overview.md#basic):** no programming prerequisite.
  Understand a graph and complete a guided
  [mixed-methods example](docs/guide/first-research-project.md); an agent or helper
  handles execution.
- **[Advanced](docs/guide/advanced.md):** basic Python, database, and terminal
  skills. Write queries and use additional capabilities as needed.
  GraphForge-specific concepts are explained along the way.

Architecture and implementation contracts are separate specialist references.
Neither learning path assumes expert knowledge.

## Installation

The docs target **v0.6.0**. Check [release availability](https://github.com/CurateLabs/graphforge/releases)
before using these final-version commands:

```bash
python -m pip install graphforge==0.6.0
```

For Node integration:

```bash
npm install @curatelabs/graphforge@0.6.0 apache-arrow
```

Before final publication, select an available candidate or use a source build
as described in [Installation](docs/guide/installation.md). Before v1.0.0,
there is no backward-compatibility or migration guarantee. The docs maintain
one current guide set.

Choose your entry path: [Work with an agent](docs/guide/work-with-an-agent.md),
[Use a notebook](docs/guide/use-a-notebook.md), or
[Integrate GraphForge](docs/guide/integrate-graphforge.md).

## Quick Start

```python
from graphforge import GraphForge

forge = GraphForge()
survey = forge.add_node("Paper", title="Survey")
methods = forge.add_node("Paper", title="Methods")
forge.add_edge(survey, "CITES", methods)

result = forge.execute("""
    MATCH (source:Paper)-[:CITES]->(paper:Paper)
    RETURN source.title AS source, paper.title AS title
""")
print(result.to_pylist())
# [{'source': 'Survey', 'title': 'Methods'}]
forge.close()
```

This example runs in memory and loses state when closed. Follow
[Your first graph](docs/guide/quickstart.md) for a fuller question, then
[save and reopen](docs/guide/tutorial.md) if needed. Durable projects require
[admitted local storage](docs/guide/installation.md#durable-storage).

Later, [record and revisit an inquiry](docs/guide/record-an-inquiry.md), or
[keep research history](docs/guide/research-journey.md).

---

## Datasets

Canonical open-dataset catalogs (`graphforge.datasets`, SNAP / LDBC /
NetworkRepository convenience loaders) are a **backlog extension** and are
**not part of the core release**. Build graphs with the construction APIs or Cypher
today. [Choose data for a graph](docs/guide/datasets/overview.md) explains
external sources and the supported construction route.

---

## Architecture

GraphForge exposes one Rust-owned engine through Cypher and analyst-intent APIs:

```
forge.execute("MATCH ...")       → Cypher compiler and execution pipeline
forge.rank(..., by=...)          → Rust algorithm dispatch → Arrow Table
forge.cluster(..., by=...)       → Rust algorithm dispatch → Arrow Table
forge.similar(..., by=...)       → Rust algorithm dispatch → Arrow Table
forge.paths(..., by=...)         → Rust algorithm dispatch → Arrow Table
forge.analyze(..., by=...)       → Rust algorithm dispatch → Arrow Table
forge.find(...)                  → Search path → Arrow Table
```

The Cypher path is four independent Rust layers:

```
graphforge-cypher → graphforge-ir → graphforge-rel → graphforge-exec
                                      ↘ graphforge-storage (Parquet)
```

Algorithm verbs bypass the Cypher parser and dispatch directly to typed Rust
handlers. Tabular query and analysis results are `pyarrow.Table` objects in
Python and Arrow IPC bytes in Node. Construction methods can return handles;
metadata and lifecycle methods can return collections, scalars, or no value.
Graph identities are public UUIDs. Python and Node adapt arguments and native
Arrow data; igraph and NetworkX are optional development parity oracles, never
runtime backends or fallbacks.
Graph data persists as Arrow/Parquet; metadata uses JSON.

---

## Development

The [agent skills package](docs/agent-skills.md) has a deterministic local NPX
pack, offline install, and invocation workflow.

Documentation is an [Astro Starlight](https://starlight.astro.build/) site under
`docs-site/`. Markdown sources stay in `docs/`; the site syncs allowlisted pages
into the Starlight content collection at build time.

```bash
# Docs site (local) — see also docs/README.md and docs-site/README.md
pnpm install
pnpm docs:dev          # http://localhost:4321/
pnpm docs:build        # output: docs-site/dist/
pnpm docs:preview      # serve docs-site/dist/
# or: make docs-serve / make docs-build / make docs-clean

# Install with dev dependencies
uv sync --dev

# Run all checks (mirrors CI Lint)
make check

# Targeted Rust gates while iterating
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

Releases are cut by pushing a `v<version>` tag; see [`RELEASING.md`](RELEASING.md).

---

## Roadmap

| Version    | Focus                                                                                | Status                                     |
| ---------- | ------------------------------------------------------------------------------------ | ------------------------------------------ |
| **v0.6.0** | Basic graph use plus optional inquiry, research, decision, and interchange workflows | Release scope; see live readiness trackers |
| v1.0       | Long-term API stability commitment                                                   | Future                                     |

**Next steps:** [install](docs/guide/installation.md) → [quick start](docs/guide/quickstart.md)
→ [docs site](https://docs.graphforge.sh/). Contributors start at
[Contributing](docs/development/contributing.md); operators at
[Publishing](docs/engineering/PUBLISHING.md).

See [docs/releases/roadmap.md](docs/releases/roadmap.md) for delivery detail.
Release notes are attached to each immutable
[GitHub Release](https://github.com/CurateLabs/graphforge/releases).

---

## License

Open source under the Apache License 2.0 (`Apache-2.0`) © Curate Labs Inc.
You may use, modify, and distribute GraphForge, including for commercial
purposes, subject to the license terms. See [LICENSE](LICENSE) and
[licensing details](docs/legal/licensing.md).

Built on Apache Arrow, DataFusion, Parquet, and the
[openCypher](https://opencypher.org/) specification.
