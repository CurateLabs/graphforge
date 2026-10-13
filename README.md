<h1 align="center">GraphForge</h1>

<p align="center">
  <strong>Composable graph tooling for analysis, construction, and refinement</strong>
</p>

<p align="center">
  An embedded, openCypher-compatible graph engine with a Rust core, Arrow results,
  and Parquet persistence — for research and investigative workflows
</p>

<p align="center">
  <a href="https://pypi.org/project/graphforge/"><img src="https://img.shields.io/pypi/v/graphforge.svg?label=PyPI&logo=pypi" alt="PyPI version" /></a>
  <a href="https://www.npmjs.com/package/@curatelabs/graphforge"><img src="https://img.shields.io/npm/v/%40curatelabs/graphforge.svg?label=npm&logo=npm" alt="npm version" /></a>
  <a href="https://github.com/CurateLabs/graphforge/actions/workflows/test.yml"><img src="https://img.shields.io/github/actions/workflow/status/CurateLabs/graphforge/test.yml?branch=main&label=CI%20Gate&logo=github" alt="CI Gate status" /></a>
  <a href="https://docs.graphforge.sh/"><img src="https://img.shields.io/badge/docs-online-0A66C2.svg" alt="Documentation" /></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="Apache License 2.0" /></a>
  <a href="https://www.greptile.com/?utm_source=oss_badge&utm_medium=readme&utm_campaign=greptile_for_open_source"><img src="https://www.greptile.com/badge.svg" alt="Greptile: The War on Bugs" /></a>
</p>

## Why GraphForge?

Build a graph, ask it questions, and keep what you learn, without running a
database server. GraphForge is for analysts working with an agent and for
developers who write their own code.

- **Good fit:** inspecting a citation network, analyzing dependencies, or
  connecting an inquiry to its evidence.
- **Not a fit:** a centrally hosted application with many concurrent users.
  Evaluate an operational database against that workload;
  [scale guidance](docs/reference/scale-limits.md) gives the measured limits.

Start with plain graph construction and querying. Saved analyses, challenged
hypotheses, research history, and collaboration are there when you need them,
and none of them is required to begin.

## Installation

Python 3.10+ or Node.js 20+. The docs target **v0.6.0**:

```bash
python -m pip install graphforge==0.6.0
npm install @curatelabs/graphforge@0.6.0 apache-arrow   # Node
```

Until v0.6.0 is published, install the newest release candidate instead with
`python -m pip install --pre graphforge` or
`npm install @curatelabs/graphforge@next apache-arrow`.
[Releases](https://github.com/CurateLabs/graphforge/releases) lists what is
available, and the [installation guide](docs/guide/installation.md) covers
candidates and source builds. Before v1.0.0 there is no backward-compatibility
or migration guarantee.

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

This graph lives in memory and is gone when closed. To keep one,
[save and reopen](docs/guide/tutorial.md) a durable project; it needs
[supported local storage](docs/guide/installation.md#durable-storage), such as
ext4, xfs, or btrfs on Linux.

## Where to go next

| You want to…                                                 | Start here                                                                                                                                                                      |
| ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Work through a question with an agent, no programming needed | [Work with an agent](docs/guide/work-with-an-agent.md), then the [basic path](docs/guide/overview.md#basic) and a [mixed-methods example](docs/guide/first-research-project.md) |
| Explore interactively                                        | [Use a notebook](docs/guide/use-a-notebook.md)                                                                                                                                  |
| Write queries and build on the API                           | [Your first graph](docs/guide/quickstart.md), then the [advanced path](docs/guide/advanced.md)                                                                                  |
| Embed GraphForge in an application                           | [Integrate GraphForge](docs/guide/integrate-graphforge.md)                                                                                                                      |
| Load your own data                                           | [Choose data for a graph](docs/guide/datasets/overview.md)                                                                                                                      |
| Keep a line of inquiry                                       | [Record an inquiry](docs/guide/record-an-inquiry.md) and [keep research history](docs/guide/research-journey.md)                                                                |

Built-in open-dataset loaders (SNAP, LDBC, NetworkRepository) are a backlog
extension and not part of the core release; build graphs with the construction
APIs or Cypher. The full documentation is at
[docs.graphforge.sh](https://docs.graphforge.sh/).

## Architecture

One Rust engine sits behind both Cypher and the analyst verbs:

```
forge.execute("MATCH ...")       → Cypher compiler and execution pipeline
forge.rank(..., by=...)          → Rust algorithm dispatch → Arrow Table
forge.cluster(..., by=...)       → Rust algorithm dispatch → Arrow Table
forge.similar(..., by=...)       → Rust algorithm dispatch → Arrow Table
forge.paths(..., by=...)         → Rust algorithm dispatch → Arrow Table
forge.analyze(..., by=...)       → Rust algorithm dispatch → Arrow Table
forge.find(...)                  → Search path → Arrow Table
```

Cypher runs through five Rust layers; the analyst verbs bypass the parser and
dispatch straight to typed Rust handlers:

```
graphforge-cypher → graphforge-ir → graphforge-plan → graphforge-rel → graphforge-exec
                                                                         ↘ graphforge-storage (Parquet)
```

- **Results:** tabular results are `pyarrow.Table` in Python and Arrow IPC
  bytes in Node. Construction methods can return handles; metadata and
  lifecycle methods return collections, scalars, or nothing.
- **Storage:** graph data persists as Arrow/Parquet and metadata as JSON. Graph
  identities are public UUIDs.
- **Bindings:** Python and Node only adapt arguments and Arrow data. igraph and
  NetworkX are optional development parity oracles, never runtime backends or
  fallbacks.
- **Conformance:** the [openCypher TCK](https://docs.graphforge.sh/reference/tck-compliance/)
  runs as a regression baseline.

## Development

```bash
uv sync --dev                                # install with dev dependencies
make check                                   # all static checks (mirrors CI Lint)
make test-rust ARGS="-p graphforge-storage"  # Rust tests, narrowed to a crate
make test-python                             # Python binding suites
make test-node                               # Node binding suites
```

The documentation is an [Astro Starlight](https://starlight.astro.build/) site
in `docs-site/`, built from the Markdown in `docs/`:

```bash
pnpm install
pnpm docs:dev          # http://localhost:4321/
pnpm docs:build        # output: docs-site/dist/
```

Start with [Contributing](docs/development/contributing.md). The
[agent skills package](docs/agent-skills.md) documents its local pack and
offline install. Releases are cut by pushing a `v<version>` tag; see
[`RELEASING.md`](RELEASING.md) and [Publishing](docs/engineering/PUBLISHING.md).

## Roadmap

| Version    | Focus                                                                                | Status        |
| ---------- | ------------------------------------------------------------------------------------ | ------------- |
| **v0.6.0** | Basic graph use plus optional inquiry, research, decision, and interchange workflows | Release scope |
| v1.0       | Long-term API stability commitment                                                   | Future        |

[The roadmap](docs/releases/roadmap.md) has delivery detail. Release notes are
attached to each immutable [GitHub Release](https://github.com/CurateLabs/graphforge/releases).

## License

Open source under the Apache License 2.0 (`Apache-2.0`) © Curate Labs Inc.
You may use, modify, and distribute GraphForge, including for commercial
purposes, subject to the license terms. See [LICENSE](LICENSE) and
[licensing details](docs/legal/licensing.md).

Built on Apache Arrow, DataFusion, Parquet, and the
[openCypher](https://opencypher.org/) specification.
