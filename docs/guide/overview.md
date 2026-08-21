# Guide Overview

This guide is the basic-usage path: install GraphForge, run your first queries, and
cover everyday workflows. For architecture, research, and deeper narratives, see the
[Book](../book/README.md). The public [documentation map](../README.md) lists published
trees.

## Start here

| Page | Purpose |
| --- | --- |
| [Installation](installation.md) | Install v0.5.1 via pip, npm, or source |
| [Quick Start](quickstart.md) | First graph in five minutes |
| [Tutorial](tutorial.md) | Step-by-step walkthrough |
| [VS Code extension](vscode-extension/) | Explore projects, run Cypher, and pair with coding agents inside your editor |
| [Move projects with portable project v2](portable-projects.md) | Verify and move immutable projects locally, air-gapped, or through OCI |

## Everyday workflows

### [Use GraphForge in VS Code](vscode-extension/)
The optional GraphForge extension adds project exploration, Cypher execution, analyst verbs,
ontology views, result graphs, and structured command interop to VS Code-compatible editors.
It uses the native Node or Python binding; graph behavior remains owned by the Rust engine.
See the synchronized extension guide for setup, runtime selection, and the complete command map.

### [Cypher Query Language](cypher-guide.md)
Learn the openCypher query language — GraphForge's primary interface for working with graphs.

### [Graph Construction](graph-construction.md)
Build graphs programmatically using the Python API.

### [Analytics Integration](analytics-integration.md)
Export graphs to NetworkX, igraph, and pandas for further analysis.

### [Move projects with portable project v2](portable-projects.md)
Preview, export, verify, import, selectively share, and promote immutable project
packages without copying live storage state or committing graph data to Git.

### [Visualization examples](visualization.md)
Comparable Plotly, Jaal, PyVis, Cytoscape.js, and Sigma.js paths over one shared
real-data GraphForge projection.

### Ranking Nodes — `forge.rank()`
Score every node with a graph centrality algorithm (PageRank, betweenness, closeness, degree,
clustering coefficient, or triangle count). Returns an Arrow Table with node properties plus a
`score` column. Pass `write_property` to persist scores back to the graph.
See the [tutorial](tutorial.md#ranking-nodes-with-forgerank) for examples.

### Clustering Nodes — `forge.cluster()`
Assign community membership using Louvain or connected-components algorithms. Returns an Arrow
Table with node properties plus a `community_id` column. Pass `write_property` to persist
assignments back to the graph.
See the [tutorial](tutorial.md#clustering-nodes-with-forgecluster) for examples.

### Finding Nodes — `forge.find()`
Full-text, vector similarity, or hybrid search over node properties. Bring your own vectors —
GraphForge stores and queries them but does not generate embeddings. Returns an Arrow Table with
node properties plus `score` and `matched_on` columns.
See the [tutorial](tutorial.md#finding-nodes-with-forgefind) for examples.

## Reference (not everyday)

### [Datasets (backlog)](datasets/overview.md)
Planned open-dataset catalogs — **not shipped** in v0.5.0. Kept under Reference for readers
tracking the backlog extension.

## Core Concepts

### Graphs
A graph consists of **nodes** (vertices) and **relationships** (edges) connecting them.

### Nodes
Nodes represent entities in your graph. They can have:
- **Labels** - Types or categories (e.g., `Person`, `Product`)
- **Properties** - Key-value pairs with data

### Relationships
Relationships connect nodes and can have:
- **Type** - The nature of the connection (e.g., `KNOWS`, `PURCHASED`)
- **Direction** - From one node to another
- **Properties** - Additional data about the relationship

### Patterns
Cypher uses ASCII-art patterns to describe graph structures:

```cypher
(a:Person)-[:KNOWS]->(b:Person)
```

This pattern matches two Person nodes connected by a KNOWS relationship.

## Query Flow

1. **MATCH** - Find patterns in the graph
2. **WHERE** - Filter results
3. **RETURN** - Specify what to return
4. **ORDER BY** - Sort results
5. **LIMIT** - Limit number of results

## Result Types

**v0.5.0 data plane:** Cypher `execute`, analyst verbs (`rank`, `cluster`,
`paths`, `analyze`, `similar`, `find`), and tabular helpers such as `schema()`
return a PyArrow `Table`. There are no `CypherValue` wrappers and no
`SearchHit` objects for those results. Access values via `.as_py()` or pass the
table directly to pandas, Polars, or NetworkX.

**Control / construction plane:** methods such as `labels()`,
`relationship_types()`, `node_count()`, `explain()`, ontology lifecycle helpers,
and scalar `add_node` / `add_edge` return lists, integers, strings, `None`, or
construction handles — not Arrow tables. See the
[architecture overview](../book/architecture/overview.md#arrow-as-the-data-contract).

```python
table = forge.execute("MATCH (p:Person) RETURN p.name, p.age")
# table is a pyarrow.Table — use Arrow, pandas, or Polars to consume it
import pandas as pd
df = table.to_pandas()
```

## Next Steps

- [Cypher Guide](cypher-guide.md) — complete query language reference
- [Graph Construction](graph-construction.md) — build graphs with Python
- [VS Code extension](vscode-extension/) — use GraphForge from your editor or coding agent
- [Book](../book/README.md) — architecture, research, and deeper usage
- [Architecture Overview](../book/architecture/overview.md) — Rust core design
