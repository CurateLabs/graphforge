# Analytics Integration

Every GraphForge method returns an Apache Arrow Table. Arrow is the common currency —
you can pass the result directly to pandas, Polars, NetworkX, or any Arrow-aware library
with no intermediate conversion step.

---

## Arrow Results

`forge.execute()` returns a PyArrow `Table`. The same is true for `rank()`, `cluster()`,
and `find()`.

```python
from graphforge import GraphForge

forge = GraphForge()
forge.add_node("Person", name="Alice", age=30)
forge.add_node("Person", name="Bob",   age=25)

table = forge.execute("MATCH (p:Person) RETURN p.name AS name, p.age AS age")
# table is a pyarrow.Table

# Convert to pandas
import pandas as pd
df = table.to_pandas()

# Convert to Polars
import polars as pl
df = pl.from_arrow(table)

# Iterate rows as plain Python dicts
for row in table.to_pylist():
    print(row["name"], row["age"])

# Pass a single column to NumPy
ages = table.column("age").to_pylist()
```

There are no `CypherValue` wrappers or `.value` calls in v0.5.0. Column values are
standard Python types (str, int, float, bool, None) when accessed via `to_pandas()`,
`to_pylist()`, or `.as_py()`.

---

## NetworkX from Arrow

Convert an Arrow Table to a NetworkX graph for path algorithms and custom analysis:

```python
import networkx as nx

table = forge.execute("""
    MATCH (a:Person)-[:KNOWS]->(b:Person)
    RETURN a.name AS src, b.name AS dst
""")

G = nx.from_pandas_edgelist(table.to_pandas(), source="src", target="dst")
print(f"Nodes: {G.number_of_nodes()}, Edges: {G.number_of_edges()}")

# Run any NetworkX algorithm on the result
pr = nx.pagerank(G)
cc = nx.average_clustering(G.to_undirected())
print(f"Avg clustering: {cc:.4f}")
```

---

## forge.rank() — Centrality Analysis

`forge.rank()` scores every node of a given label and returns an Arrow Table with all
node properties plus a `score` column. It dispatches to an optimised algorithm backend —
no Cypher needed, no exporting the graph first.

```python
# PageRank across all Person nodes
table = forge.rank("Person", by="pagerank")
df = table.to_pandas().sort_values("score", ascending=False)
print(df[["name", "score"]].head(10))

# Restrict to a specific relationship type and direction
table = forge.rank("Person", by="betweenness", via="KNOWS", directed=True)

# Degree centrality — good proxy for raw connectivity
table = forge.rank("Person", by="degree")
```

**Available algorithms for `by`:**

| Value | Description |
|-------|-------------|
| `pagerank` | Eigenvector-based global influence |
| `betweenness` | Fraction of shortest paths passing through a node |
| `closeness` | Average inverse distance to all other nodes |
| `degree` | Number of direct connections |
| `clustering_coefficient` | Density of a node's local neighbourhood |
| `triangles` | Count of closed triangles the node participates in |

### Write-back (opt-in)

By default `rank()` is read-only. Pass `write_property` to store the score as a node
property, which you can then query with Cypher:

```python
forge.rank("Person", by="pagerank", write_property="rank")

top = forge.execute("""
    MATCH (n:Person)
    RETURN n.name AS name, n.rank AS rank
    ORDER BY n.rank DESC LIMIT 10
""")
print(top.to_pandas())
```

---

## forge.cluster() — Community Detection

`forge.cluster()` assigns every node of a given label to a community and returns an Arrow
Table with node properties plus a `community_id` column.

```python
# Louvain community detection
table = forge.cluster("Person", by="louvain")
df = table.to_pandas()

# See community sizes
print(df.groupby("community_id").size().sort_values(ascending=False))

# Restrict to one relationship type
table = forge.cluster("Person", by="louvain", via="KNOWS")

# Connected components — useful for finding isolated subgraphs
table = forge.cluster("Person", by="components")
```

**Available algorithms for `by`:**

| Value | Description |
|-------|-------------|
| `louvain` | Modularity-maximising community detection |
| `components` | Weakly connected components |

### Write-back (opt-in)

```python
forge.cluster("Person", by="louvain", write_property="community")

# Now query the community structure with Cypher
forge.execute("""
    MATCH (n:Person)
    RETURN n.community AS community, count(*) AS size
    ORDER BY size DESC LIMIT 5
""")
```

---

## forge.find() — Search and Retrieval

`forge.find()` combines full-text search and vector cosine similarity in a single call.
It returns an Arrow Table with node properties plus `score` and `matched_on` columns.

The index is built automatically on the first `find()` call — no explicit indexing step
is required for a standard workflow. `label` is required on every call.

```python
# Text search
table = forge.find("graph neural networks", label="Paper")
df = table.to_pandas()
print(df[["title", "score", "matched_on"]])

# Limit to a label and top-N results
table = forge.find("graph neural networks", label="Paper", limit=20)

# Hybrid text + vector search (bring your own embeddings)
import openai
client = openai.OpenAI()

def embed(text: str) -> list[float]:
    return client.embeddings.create(
        input=text, model="text-embedding-3-small"
    ).data[0].embedding

query_vec = embed("scalable graph representation learning")
table = forge.find("scalable graph learning", label="Paper", vector=query_vec)

# Vector-only search
table = forge.find(vector=query_vec, label="Paper")
```

**Result columns:**

| Column | Type | Description |
|--------|------|-------------|
| `node_uuid` | bytes | Canonical UUID identity of the matched node |
| (node properties) | varies | All properties of the matched node |
| `score` | float | Combined relevance score |
| `matched_on` | str | `"text"`, `"vector"`, or `"text+vector"` |

### Explicit indexing

`find()` builds its index automatically, but you can control the timing explicitly —
useful when you want to index a large batch before the first search call:

```python
# Index selected properties for text search
forge.index("Paper", properties=["title", "abstract"])

# Store a vector for a specific node — identity is the UUID `node_uuid`.
# The vector mode takes `node=` as a UUID string, a NodeHandle, or a
# label/property/value selector dict.
import uuid

nid = forge.execute(
    "MATCH (n:Paper {title: 'GNN Survey'}) RETURN n.node_uuid AS nid"
).column("nid")[0].as_py()

forge.index(
    "Paper",
    node=str(uuid.UUID(bytes=nid)),
    vector=embed("GNN Survey overview"),
    space="sbert",
)
```

### Using find() results in Cypher

The `node_uuid` column carries canonical node identity. Bind it as a `uuid.UUID` and match on
the `node_uuid` identity predicate for follow-up graph traversals:

```python
import uuid

table = forge.find("graph neural networks", label="Paper", limit=5)
top_uuid = uuid.UUID(bytes=table.column("node_uuid")[0].as_py())

neighbours = forge.execute("""
    MATCH (p:Paper)-[:CITES]->(cited:Paper)
    WHERE p.node_uuid = $nid
    RETURN cited.title AS title, cited.year AS year
    ORDER BY cited.year DESC
""", {"nid": top_uuid})
print(neighbours.to_pandas())
```

A typed UUID parameter is only valid as a direct `node_uuid` / `edge_uuid` identity equality
predicate; v0.5.0 exposes no numeric `id()` surrogate.

---

## Choosing Between Methods

| Goal | Method |
|------|--------|
| Declarative query — patterns, filters, aggregations | `forge.execute()` |
| Score nodes by graph influence | `forge.rank()` |
| Group nodes into communities | `forge.cluster()` |
| Search by keywords or semantic similarity | `forge.find()` |
| Custom graph algorithms via NetworkX | `forge.execute()` → Arrow → `nx.from_pandas_edgelist()` |

---

## Schema Introspection

```python
print(forge.labels())              # ['Author', 'Paper', 'Person']
print(forge.relationship_types())  # ['AUTHORED', 'CITES', 'KNOWS']
print(forge.node_count("Person"))  # 42
print(forge.schema())              # Arrow Table — labels, property names, types
```

---

## Next Steps

- [Visualization examples](visualization.md) — Plotly, Jaal, PyVis, Cytoscape.js, and Sigma.js over one shared real-data projection
- [Network Analysis use case](../book/use-cases/network-analysis.md) — worked examples on the SNAP ego-Facebook dataset
- [Knowledge Graph Construction](../book/use-cases/knowledge-graph-construction.md) — LangChain integration and MERGE patterns
- [API Reference](../reference/api.md) — full method signatures
