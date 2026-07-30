# GraphForge

**An embedded, openCypher-compatible graph database for Python, Node, Swift, and Kotlin**

[![PyPI](https://img.shields.io/pypi/v/graphforge.svg)](https://pypi.org/project/graphforge/)
[![Python](https://img.shields.io/pypi/pyversions/graphforge.svg)](https://pypi.org/project/graphforge/)
[![Docs](https://img.shields.io/badge/docs-online-0A66C2.svg)](https://docs.graphforge.sh/)
[![openCypher TCK](https://img.shields.io/badge/openCypher%20TCK-3897%2F3897-brightgreen.svg)](reference/tck-compliance.md)
[![Apache License 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../LICENSE)

GraphForge lets you write openCypher queries against an in-memory or Parquet-backed graph
with no external services. The v0.5.0 release passes **all 3,897 openCypher TCK scenarios**
and provides seven analyst-intent methods — all returning Apache Arrow Tables.

```python
from graphforge import GraphForge

forge = GraphForge()                    # in-memory
# forge = GraphForge("path/to/graph/") # or Parquet-backed (directory must exist)

alice = forge.add_node("Person", name="Alice", age=30)
bob   = forge.add_node("Person", name="Bob",   age=25)
forge.add_edge(alice, "KNOWS", bob, since=2020)

# openCypher — returns an Arrow Table
table = forge.execute("""
    MATCH (p:Person)-[:KNOWS]->(friend)
    RETURN p.name AS person, friend.name AS friend
""")

# Consume with pandas, Polars, or iterate directly
df = table.to_pandas()
print(df)
#   person friend
# 0  Alice    Bob
```

---

## Get started

| | |
|---|---|
| [Installation](guide/installation.md) | Install via pip or uv |
| [Quick Start](guide/quickstart.md) | Your first graph in five minutes |
| [Tutorial](guide/tutorial.md) | Step-by-step guided walkthrough |

---

## Use every day

| | |
|---|---|
| [Guide overview](guide/overview.md) | Everyday workflows index |
| [Cypher Reference](guide/cypher-guide.md) | Full openCypher language guide |
| [Graph Construction](guide/graph-construction.md) | Build graphs with Python API and Cypher |
| [Analytics Integration](guide/analytics-integration.md) | Arrow, pandas, Polars, rank, cluster, find |

---

## Understand

| | |
|---|---|
| [Book map](book/README.md) | Architecture, research, and deeper usage index |
| [Architecture Overview](book/architecture/overview.md) | Pipeline, storage, execution model |
| [Algorithm Catalog](book/architecture/algorithms.md) | All algorithms across rank/cluster/paths/analyze/similar |
| [Knowledge Graph Construction](book/use-cases/knowledge-graph-construction.md) | Extract entities, build and refine ontologies |
| [Network Analysis](book/use-cases/network-analysis.md) | Degree, paths, communities — in notebooks |
| [LLM-Powered Workflows](book/use-cases/llm-workflows.md) | Store LLM extractions, build retrieval context |
| [AI Agent Tool Recall](book/use-cases/agent-tool-recall.md) | Graph-structured tool libraries for LLM agents |
| [Agent Grounding](book/use-cases/agent-grounding.md) | Ground agents in domain ontologies |

---

## Reference

| | |
|---|---|
| [API Reference](reference/api.md) | Full method reference — all seven verbs |
| [OpenCypher Compatibility](reference/opencypher-compatibility.md) | Feature matrix — v0.5.0 (100%) |
| [TCK Compliance](reference/tck-compliance.md) | 3,897 / 3,897 passing |
| [Datasets (backlog)](guide/datasets/overview.md) | Planned open-dataset catalogs — not shipped in v0.5.0 |
| [Changelog](reference/changelog.md) | Keep a Changelog notes |

---

## Design Principles

1. **Correctness over performance** — openCypher semantics verified against the full TCK
2. **Zero configuration** — `pip install graphforge`, no servers, no connection strings
3. **Inspectable** — `explain` at every compiler stage; structured errors with source spans
4. **Arrow-first results** — every method returns an Apache Arrow Table

---

## Contribute & operate

| | |
|---|---|
| [Documentation map](README.md) | Public docs map and site tooling |
| [Contributing](development/contributing.md) | How to develop and send changes |
| [Testing strategy](engineering/TESTING.md) | How we prove correctness |
| [Release process](development/release-process.md) | How verified artifacts ship |
| [Product roadmap](releases/roadmap.md) | Near-term product direction |

---

## Engineering

| | |
|---|---|
| [Engineering overview](engineering/README.md) | Lifecycle: architecture → test → publish → observe |
| [Architecture Decision Records](adr/README.md) | Engineering decisions for the v0.5.0 keepers (nav: Engineering) |
| [ADR decision log](engineering/adrs/README.md) | Index linking the `docs/adr/` bodies |

Private product & strategy DocSlime docs live in
[`graphforge-nextjs`](https://github.com/CurateLabs/graphforge-nextjs) (not on this site).

---

## Architecture at a glance

GraphForge exposes seven analyst-intent methods that share a single Parquet-backed storage layer.

```
forge.execute("MATCH …")         →  Cypher path     (parser → binder → Graph IR → DataFusion)
forge.rank("Person", by=…)       →  Algorithm path  (centrality / structural scoring)
forge.cluster("Person", by=…)    →  Algorithm path  (community detection, components)
forge.paths(alice, bob, by=…)    →  Algorithm path  (shortest paths, flow, reachability)
forge.analyze(by=…)              →  Algorithm path  (DAG, coloring, spanning trees, embeddings)
forge.similar("Person", by=…)    →  Algorithm path  (pairwise node similarity)
forge.find("query", …)           →  Search path     (text + vector hybrid search)
```

The Rust core uses a hand-written recursive-descent + Pratt expression parser, DataFusion-backed
query execution, and Parquet storage. First-class bindings for Python (PyO3/maturin),
Node (napi-rs), Swift (UniFFI), and Kotlin (UniFFI) all return Arrow results.

See [Architecture Overview](book/architecture/overview.md).
