# GraphForge

**Composable graph tooling for analysis, construction, and refinement**

[![Version](https://img.shields.io/badge/version-v0.5.1-F59E0B.svg)](guide/installation.md)
[![Python](https://img.shields.io/badge/Python-3.10%2B-3776AB.svg?logo=python&logoColor=white)](guide/installation.md)
[![Node.js](https://img.shields.io/badge/Node.js-20%2B-5FA04E.svg?logo=nodedotjs&logoColor=white)](guide/installation.md)
[![Rust](https://img.shields.io/badge/Rust-1.96-000000.svg?logo=rust&logoColor=white)](development/contributing.md)
[![CI](https://img.shields.io/github/actions/workflow/status/CurateLabs/graphforge/test.yml?branch=main&label=CI&logo=github)](https://github.com/CurateLabs/graphforge/actions/workflows/test.yml)
[![openCypher TCK](https://img.shields.io/badge/openCypher%20TCK-3897%2F3897-brightgreen.svg)](reference/tck-compliance.md)
[![Apache License 2.0](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../LICENSE)

GraphForge is an embedded, local-first graph execution environment with a Rust core, Arrow
results, and Parquet persistence. It brings openCypher and analyst-intent verbs to notebooks,
scripts, repositories, and editor workflows without requiring a database server. The current
v0.5 engine passes all **3,897 openCypher TCK scenarios**.

> **Install tip:** Pin GraphForge **v0.5.1** from PyPI (`graphforge==0.5.1`) or npm
> (`@curatelabs/graphforge@0.5.1`). PyPI also lists an unrelated pure-Python `graphforge`
> **0.4.0**; do not assume an unpinned install resolves to the CurateLabs native engine.
> See [Installation](guide/installation.md).

```python
from graphforge import GraphForge

forge = GraphForge()
alice = forge.add_node("Person", name="Alice", age=30)
bob = forge.add_node("Person", name="Bob", age=25)
forge.add_edge(alice, "KNOWS", bob, since=2020)

table = forge.execute("""
    MATCH (p:Person)-[:KNOWS]->(friend)
    RETURN p.name AS person, friend.name AS friend
""")

print(table.to_pandas())
```

Every query and analyst verb returns an Apache Arrow Table. Python and Node are thin bindings;
they never replace or fall back from the Rust engine.

---

## Get started

| Page | Job |
|---|---|
| [Installation](guide/installation.md) | Install Python or Node packages, or build from source |
| [Quick Start](guide/quickstart.md) | Create, query, and persist your first graph |
| [Tutorial](guide/tutorial.md) | Work through a complete citation-network example |
| [CLI and repository integration](guides/repository-integration.md) | Initialize, validate, synchronize, checkpoint, export, and import a project |
| [VS Code extension](guide/vscode-extension/) | Explore projects, run Cypher, and pair with coding agents in your editor |

---

## Why GraphForge?

Modern research and investigation produce graph-shaped data: entity relationships extracted by
LLMs, citation networks, dependency graphs, social connections, and evolving knowledge bases.
GraphForge makes those graphs portable and inspectable without turning them into an application
database or requiring a long-running service.

<table>
  <thead>
    <tr>
      <th scope="col">Aspect</th>
      <th scope="col">NetworkX</th>
      <th scope="col">GraphForge</th>
      <th scope="col">Neo4j / Memgraph</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th scope="row">Setup</th>
      <td>Python package</td>
      <td>Embedded package</td>
      <td>Run a server</td>
    </tr>
    <tr>
      <th scope="row">Query language</th>
      <td>Python API</td>
      <td><strong>Full openCypher</strong></td>
      <td>Full Cypher</td>
    </tr>
    <tr>
      <th scope="row">Persistence</th>
      <td>Manual</td>
      <td><strong>Parquet project directory</strong></td>
      <td>Native</td>
    </tr>
    <tr>
      <th scope="row">Results</th>
      <td>Python objects</td>
      <td><strong>Apache Arrow Tables</strong></td>
      <td>Driver rows</td>
    </tr>
    <tr>
      <th scope="row">Notebook-friendly</th>
      <td>✓</td>
      <td>✓</td>
      <td>Requires connection</td>
    </tr>
    <tr>
      <th scope="row">Primary role</th>
      <td>In-memory graph library</td>
      <td>Local knowledge-analysis workbench</td>
      <td>Operational graph database</td>
    </tr>
  </tbody>
</table>

Use GraphForge for knowledge graphs, citation networks, LLM output storage, repository-aware
analysis, and social-network research. Use an operational database for high-throughput,
multi-user application workloads or graphs beyond the documented
[scale limits](reference/scale-limits.md).

---

## Use every day

| Page | Job |
|---|---|
| [Guide overview](guide/overview.md) | Navigate everyday GraphForge workflows |
| [Cypher guide](guide/cypher-guide.md) | Query and mutate graphs with openCypher |
| [Graph construction](guide/graph-construction.md) | Build graphs through Rust-owned APIs and Cypher |
| [Analytics integration](guide/analytics-integration.md) | Work with Arrow, pandas, Polars, and analyst verbs |
| [VS Code commands](guide/vscode-extension/commands.md) | Run GraphForge from VS Code or a compatible editor |
| [Agent interop](guide/vscode-extension/agent-interop.md) | Drive structured extension commands from coding agents |

---

## Architecture at a glance

GraphForge exposes one Rust-owned engine through Cypher, analyst-intent APIs, and repository
lifecycle commands:

```text
Python (PyO3, thin) ─┐
Node (N-API, thin) ──┼──> graphforge-api ──> Arrow results
CLI (thin launcher) ─┘

Cypher: graphforge-cypher ──> graphforge-ir ──> graphforge-rel ──> graphforge-exec
                                                   └──> graphforge-storage (Parquet + JSON metadata)
Analyst verbs bypass the Cypher parser and dispatch through Rust-owned typed handlers.
```

Python, Node, the CLI, and the VS Code extension project the same engine behavior. Swift and
Kotlin bindings are planned rather than shipped. See the
[architecture overview](book/architecture/overview.md) for storage, execution, ontology,
knowledge, checkpoint, and compatibility contracts.

---

## Understand and reference

| Page | Job |
|---|---|
| [Book](book/README.md) | Explore architecture, research, and deeper usage narratives |
| [API reference](reference/api.md) | Look up engine, lifecycle, and analyst surfaces |
| [Algorithm catalog](book/architecture/algorithms.md) | Choose rank, cluster, paths, analyze, or similar algorithms |
| [OpenCypher compatibility](reference/opencypher-compatibility.md) | Inspect supported language behavior |
| [TCK compliance](reference/tck-compliance.md) | Review the 3,897 / 3,897 language gate |

---

## Contribute and operate

| Page | Job |
|---|---|
| [Documentation map](README.md) | Understand the public information architecture |
| [Contributing](development/contributing.md) | Prerequisites, validation, and focused PRs |
| [Testing](engineering/TESTING.md) | See how GraphForge proves behavior |
| [Roadmap](releases/roadmap.md) | Review current and planned product surfaces |
| [Publishing](engineering/PUBLISHING.md) | Package destinations and release sequence |
| [Release process](development/release-process.md) | Operator checklist for cutting a release |

GraphForge is open source under the [Apache License 2.0](legal/licensing.md).
