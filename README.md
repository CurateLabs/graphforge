<h1 align="center">GraphForge</h1>

<p align="center">
  <a href="#installation"><img src="https://img.shields.io/badge/version-v0.5.1-F59E0B.svg" alt="GraphForge v0.5.1" /></a>
  <a href="#installation"><img src="https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?logo=python&logoColor=white" alt="Python 3.10 or newer" /></a>
  <a href="crates/graphforge-bindings-node"><img src="https://img.shields.io/badge/Node.js-20%2B-5FA04E.svg?logo=nodedotjs&logoColor=white" alt="Node.js 20 or newer" /></a>
  <a href="rust-toolchain.toml"><img src="https://img.shields.io/badge/Rust-1.96-000000.svg?logo=rust&logoColor=white" alt="Rust 1.96" /></a>
  <a href="https://github.com/CurateLabs/graphforge/actions/workflows/test.yml"><img src="https://img.shields.io/github/actions/workflow/status/CurateLabs/graphforge/test.yml?branch=main&label=CI&logo=github" alt="Test Suite status" /></a>
  <a href="https://docs.graphforge.sh/"><img src="https://img.shields.io/badge/docs-online-0A66C2.svg" alt="Documentation" /></a>
  <a href="https://docs.graphforge.sh/reference/tck-compliance/"><img src="https://img.shields.io/badge/openCypher%20TCK-3897%2F3897-brightgreen.svg" alt="openCypher TCK" /></a>
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
- [Cypher Features](#cypher-features)
- [Datasets](#datasets)
- [Architecture](#architecture)
- [Development](#development)
- [Roadmap](#roadmap)
- [License](#license)

---

## Why GraphForge?

> *We are not building a database for applications.*
> *We are building a graph execution environment for thinking.*

Modern data science and ML workflows increasingly produce graph-shaped data —
entity relationships extracted by LLMs, citation networks, dependency graphs,
social connections, knowledge bases. Working with this data shouldn't require
running a database server. GraphForge brings openCypher and analyst-intent verbs
to notebooks and scripts: zero configuration, Parquet-backed projects, and
first-class Arrow results across language bindings.

| | NetworkX | **GraphForge** | Neo4j / Memgraph |
|:---|:---|:---|:---|
| **Setup** | `pip install` | Embedded package (`graphforge==0.5.1`) | Run a server |
| **Query language** | Python API | **Full openCypher** | Full Cypher |
| **Persistence** | Manual | **Parquet project directory** | Native |
| **Results** | Python objects | **Apache Arrow Tables** | Driver rows |
| **Notebook-friendly** | ✓ | ✓ | Requires connection |
| **Graph size** | Millions | Research / notebook scale† | Billions |
| **TCK compliance** | N/A | **Full openCypher TCK corpus** | ~100% |

**Use GraphForge for:** knowledge graphs, citation networks, research workflows,
LLM output storage, social network analysis in notebooks.

**Use a production database for:** high throughput, multi-user access, or graphs
beyond the limits in [Scale Limits](docs/reference/scale-limits.md).

† *Fixed-hop traversal with `LIMIT` is the practical scaling path; full-scan
aggregations remain edge-count bound. See scale limits for measured ceilings.*

---

## Installation

Install GraphForge **v0.5.1** from PyPI or npm. Pin the release version — PyPI
also lists an unrelated pure-Python `graphforge` **0.4.0**; do not confuse it
with the CurateLabs native engine.

**pip**

```bash
pip install "graphforge==0.5.1"
```

**uv** (recommended)

```bash
uv add "graphforge==0.5.1"
```

**npm**

```bash
npm install @curatelabs/graphforge@0.5.1
```

**pnpm**

```bash
pnpm add @curatelabs/graphforge@0.5.1
```

```bash
python -c "import graphforge; print(graphforge.__version__)"  # 0.5.1…
```

**Requirements:** Python 3.10–3.14

See the [installation guide](docs/guide/installation.md) for source builds
and fuller verification.

### Ways to use GraphForge

| Surface | Current role |
|---|---|
| [Python](crates/graphforge-bindings-py/README.md) | Thin PyO3 binding, Arrow results, and the `graphforge` CLI launcher |
| [Node](crates/graphforge-bindings-node/README.md) | Thin N-API binding over the same Rust-owned behavior |
| [CLI](packages/cli/README.md) | Repository lifecycle, configuration, checkpoints, and portable import/export |
| [VS Code extension](https://docs.graphforge.sh/guide/vscode-extension/) | Project exploration, Cypher, analyst verbs, result views, and agent interop |

Swift and Kotlin bindings remain planned; they are not shipped surfaces.

---

## Quick Start

### In-memory graph

```python
from graphforge import GraphForge

forge = GraphForge()

alice = forge.add_node("Person", name="Alice", age=30)
bob = forge.add_node("Person", name="Bob", age=25)
forge.add_edge(alice, "KNOWS", bob, since=2020)

table = forge.execute("""
    MATCH (p:Person)-[:KNOWS]->(friend)
    WHERE p.age > 25
    RETURN p.name AS person, friend.name AS friend, p.age AS age
    ORDER BY p.age DESC
""")

print(table.to_pandas())
```

### Persistent graph

The project directory must already exist — GraphForge initializes a project inside it rather
than creating the directory.

```python
from pathlib import Path

Path("research").mkdir(parents=True, exist_ok=True)

forge = GraphForge("research/")
forge.add_node("Paper", title="Graph Neural Networks", year=2024)
forge.close()

forge = GraphForge("research/")
table = forge.execute("MATCH (p:Paper) RETURN p.title AS t")
print(table.column("t")[0].as_py())  # Graph Neural Networks
```

### Analyst verbs

```python
# Centrality — Arrow Table with a score column
table = forge.rank("Person", by="pagerank")

# Opt-in write-back, then query via Cypher
forge.rank("Person", by="pagerank", write_property="rank")
forge.execute("MATCH (n:Person) RETURN n.name, n.rank ORDER BY n.rank DESC LIMIT 5")

# Communities
table = forge.cluster("Person", by="louvain", via="KNOWS")

# Hybrid text + vector search (bring your own embeddings)
table = forge.find("graph neural networks", label="Paper", vector=query_embedding)
```

Every verb returns an Apache Arrow Table. Use `table.to_pandas()`,
`polars.from_arrow(table)`, or `table.to_pylist()`.

---

## Cypher Features

GraphForge implements the full openCypher language. See
[TCK Compliance](docs/reference/tck-compliance.md) for the current corpus gate.

### Clauses

```cypher
-- Reading
MATCH (n:Person)-[:KNOWS]->(friend)
OPTIONAL MATCH (n)-[:WORKS_AT]->(company)
WHERE n.age > 25
WITH n, count(friend) AS friends
RETURN n.name, friends
ORDER BY friends DESC
LIMIT 10

-- Writing
CREATE (n:Person {name: 'Alice'})
MERGE (n:Person {name: 'Alice'})
SET n.age = 30
REMOVE n.temp
DELETE n
DETACH DELETE n

-- Iteration
UNWIND [1, 2, 3] AS x
RETURN x * 2 AS doubled

-- Subqueries
MATCH (n) WHERE EXISTS { MATCH (n)-[:KNOWS]->() }
RETURN n
```

### Patterns

```cypher
(n)                                -- Any node
(n:Person)                         -- Node with label
(n:Person {age: 30})               -- Node with property
(a)-[r:KNOWS]->(b)                 -- Directed relationship
(a)-[r:KNOWS|LIKES]->(b)           -- Multiple types
(a)-[*1..3]->(b)                   -- Variable-length (1 to 3 hops)
(a)-[*]->(b)                       -- Any length
p = (a)-[*]->(b)                   -- Bind path to variable
```

### Functions

| Category | Functions |
|----------|-----------|
| String | `toLower`, `toUpper`, `trim`, `split`, `replace`, `substring`, `left`, `right`, `reverse`, `size` |
| Math | `abs`, `ceil`, `floor`, `round`, `sqrt`, `pow`, `exp`, `log`, `sin`, `cos`, `tan`, `pi`, `e` |
| List | `head`, `tail`, `last`, `range`, `size`, `reverse`, `sort`, `collect`, `reduce`, `filter`, `extract` |
| Aggregation | `count`, `sum`, `avg`, `min`, `max`, `collect`, `stDev`, `percentileDisc` |
| Predicate | `all`, `any`, `none`, `single`, `exists`, `isEmpty` |
| Temporal | `date`, `datetime`, `localDatetime`, `time`, `localtime`, `duration`, `now` |
| Spatial | `point`, `distance` |
| Graph | `id`, `labels`, `type`, `keys`, `properties`, `nodes`, `relationships`, `startNode`, `endNode` |
| Conversion | `toInteger`, `toFloat`, `toString`, `toBoolean`, `coalesce` |

### Temporal types (full precision)

```cypher
RETURN date('2024-01-15')
RETURN datetime('2024-01-15T14:30:00[Europe/London]')  -- IANA timezone
RETURN duration('P1Y2M3DT4H5M6.789S')
RETURN duration('PT0.000000789S').nanoseconds  -- 789
RETURN localdatetime('+999999999-12-31T23:59:59')
RETURN date('2024-01-01') + duration('P1M')  -- 2024-02-01
RETURN duration.between(date('2020-01-01'), date('2024-01-01'))
```

---

## Datasets

Canonical open-dataset catalogs (`graphforge.datasets`, SNAP / LDBC /
NetworkRepository convenience loaders) are a **backlog extension** and are
**not shipped** with v0.5.0. Build graphs with the construction APIs or Cypher
today. Planned catalog notes live under
[Datasets (reference)](docs/guide/datasets/overview.md).

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
handlers. Every result is an Apache Arrow Table with public UUID identity.
Python and Node adapt arguments and native Arrow data; igraph and NetworkX are
optional development parity oracles, never runtime backends or fallbacks.
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

# Run all checks (mirrors CI)
make pre-push

# Targeted Rust gates while iterating
cargo fmt --all -- --check
cargo clippy --workspace -- -D warnings
cargo test --workspace
```

`Binding Release Candidate` is post-merge, `main`-only evidence. Dispatch it
with the current 40-character `main` commit SHA; the workflow rejects branch
heads and stale commits before any platform matrix build starts.

---

## Roadmap

| Version | Focus | Status |
|---------|-------|--------|
| **v0.5.1** | Coordinated release across crates, PyPI, npm, CLI, and docs | **Current** |
| v0.5.x | Swift + Kotlin bindings (UniFFI) and follow-on surfaces | Planned |
| v1.0 | Long-term API stability commitment | Future |

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
