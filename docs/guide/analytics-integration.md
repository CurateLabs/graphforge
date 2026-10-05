# Analytics Integration

**Advanced:** this page assumes basic Python and the
[graph quickstart](quickstart.md). Choose it when you want table libraries,
network algorithms, or text/vector retrieval. For a guided first research
project, begin with the Basic lessons instead.

Queries and analyst verbs return Arrow data. In Python, inspect the PyArrow
table directly or convert it for pandas, Polars, or NetworkX as shown below.
Construction and control operations can return handles, metadata, or scalars.
Start with the [basic graph quickstart](quickstart.md); these integrations are optional.

The examples below use optional packages. Install them into the same Python
environment as GraphForge before running the conversion and NetworkX blocks:

```bash
python -m pip install pandas polars networkx
```

Graph metrics describe connections in the supplied data. A high score is not
evidence quality, statistical significance, or a causal explanation. Compare
numeric results with source material and state the limits of your sample.

---

## Arrow Results

`forge.execute()` returns a PyArrow `Table`. The same is true for `rank()`, `cluster()`,
and `find()`.

```python
from graphforge import GraphForge

forge = GraphForge()
alice = forge.add_node("Person", name="Alice", age=30)
bob = forge.add_node("Person", name="Bob", age=25)
forge.add_edge(alice, "KNOWS", bob)

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

# Extract a single column as a Python list
ages = table.column("age").to_pylist()
```

There are no `CypherValue` wrappers or `.value` calls. Column values are
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

G = nx.from_pandas_edgelist(
    table.to_pandas(), source="src", target="dst", create_using=nx.DiGraph(),
)
print(f"Nodes: {G.number_of_nodes()}, Edges: {G.number_of_edges()}")

# Inspect degree and clustering on this small example
print(dict(G.degree()))
cc = nx.average_clustering(G.to_undirected())
print(f"Avg clustering: {cc:.4f}")
```

This prints two nodes, one edge, degree 1 for each person, and average
clustering `0.0000`. The projection includes only nodes attached to a matching
relationship. If isolated people matter to your question, add them from a
separate node query rather than silently dropping them. Other NetworkX
algorithms can require additional dependencies.

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

| Value                    | Description                                                              |
| ------------------------ | ------------------------------------------------------------------------ |
| `pagerank`               | Structural score based on incoming links and the scores of their sources |
| `betweenness`            | Fraction of shortest paths passing through a node                        |
| `closeness`              | Average inverse distance to all other nodes                              |
| `degree`                 | Number of direct connections                                             |
| `clustering_coefficient` | Density of a node's local neighbourhood                                  |
| `triangles`              | Count of closed triangles the node participates in                       |

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

| Value        | Description                               |
| ------------ | ----------------------------------------- |
| `louvain`    | Modularity-maximising community detection |
| `components` | Weakly connected components               |

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

The following is a separate, self-contained in-memory example. Finish the
earlier graph with `forge.close()` before starting it in the same session.

```python
from graphforge import GraphForge

forge = GraphForge()
survey = forge.add_node("Paper", title="GNN Survey", abstract="graph neural networks", year=2024)
methods = forge.add_node("Paper", title="Network Methods", abstract="network analysis", year=2023)
forge.add_edge(survey, "CITES", methods)

# Text search
table = forge.find("graph neural networks", label="Paper")
df = table.to_pandas()
print(df[["title", "score", "matched_on"]])

# Limit to a label and top-N results
table = forge.find("graph neural networks", label="Paper", limit=20)

```

**Result columns:**

| Column            | Type   | Description                                 |
| ----------------- | ------ | ------------------------------------------- |
| `node_uuid`       | bytes  | Canonical UUID identity of the matched node |
| (node properties) | varies | All properties of the matched node          |
| `score`           | float  | Combined relevance score                    |
| `matched_on`      | str    | `"text"`, `"vector"`, or `"text+vector"`    |

### Explicit indexing

`find()` builds its index automatically, but you can control the timing explicitly —
useful when you want to index a large batch before the first search call:

```python
# Index selected properties for text search
forge.index("Paper", properties=["title", "abstract"])

```

### Optional vector retrieval

An embedding is a numeric representation produced by your chosen model or
method. GraphForge stores and compares these vectors; it does not create them
or interpret a similarity score as truth. In a real project, use the same
model, dimensions, and preprocessing for documents and queries.

These deliberately simple vectors demonstrate the API without a model service;
they do not encode the meaning of the papers:

```python
forge.publish_caller_embeddings(
    "demo",
    [{"node": survey, "vector": [1.0, 0.0]},
     {"node": methods, "vector": [0.0, 1.0]}],
    dimensions=2,
    source_projection={"label": "Paper", "recipe": "two_paper_demo"},
)
table = forge.find(vector=[1.0, 0.0], label="Paper", space="demo", limit=1)
print(table.select(["title", "score"]).to_pylist())
```

Expected output: `[{'title': 'GNN Survey', 'score': 1.0}]`.
For hybrid retrieval, supply both the text query and a vector from the same
space: `forge.find("graph neural networks", label="Paper", vector=[1.0, 0.0], space="demo")`.

### Using find() results in Cypher

The `node_uuid` column carries canonical node identity. Bind it as a `uuid.UUID` and match on
the `node_uuid` identity predicate for follow-up graph traversals:

```python
import uuid

table = forge.find("graph neural networks", label="Paper", limit=5)
if table.num_rows == 0:
    raise ValueError("No matching paper; inspect the query and source data.")
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
predicate; GraphForge exposes no numeric `id()` surrogate.

---

## Choosing Between Methods

| Goal                                                | Method                                                  |
| --------------------------------------------------- | ------------------------------------------------------- |
| Declarative query — patterns, filters, aggregations | `forge.execute()`                                       |
| Score nodes by graph influence                      | `forge.rank()`                                          |
| Group nodes into communities                        | `forge.cluster()`                                       |
| Search by keywords or semantic similarity           | `forge.find()`                                          |
| Custom graph algorithms via NetworkX                | `forge.execute()` → Arrow → `nx.from_pandas_edgelist()` |

---

## Schema Introspection

```python
print(forge.labels())              # Labels in this instance
print(forge.relationship_types())  # Relationship types in this instance
print(forge.node_count("Paper"))   # 2 in the search example
print(forge.schema())              # Arrow table of labels/types and their counts
forge.close()
```

---

## Next Steps

- [Visualization examples](visualization.md) — Plotly, Jaal, PyVis, Cytoscape.js, and Sigma.js over one shared real-data projection
- [Network Analysis use case](../book/use-cases/network-analysis.md) — worked examples on the SNAP ego-Facebook dataset
- [Knowledge Graph Construction](../book/use-cases/knowledge-graph-construction.md) — LangChain integration and MERGE patterns
- [API Reference](../reference/api.md) — full method signatures
