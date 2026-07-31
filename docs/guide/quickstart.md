# Quick Start

Get a graph running in five minutes on the v0.5.0 API. Every query and analyst
verb returns an Apache Arrow `Table`. Node and edge handles use stable `.uuid`
identity (no numeric storage ids).

For full install options, see [Installation](installation.md).
To run the same engine from your editor, see the [VS Code extension guide](vscode-extension/).

---

## Install

```bash
pip install graphforge
# or
uv add graphforge
```

---

## Create and Query a Graph

```python
from graphforge import GraphForge

forge = GraphForge()   # in-memory; use GraphForge("my-graph/") for persistence

# Add nodes — returns a NodeHandle (use .uuid for identity)
alice = forge.add_node("Person", name="Alice", age=30)
bob   = forge.add_node("Person", name="Bob",   age=25)

# Add a relationship
forge.add_edge(alice, "KNOWS", bob, since=2020)

# Query with openCypher — returns an Arrow Table
table = forge.execute("""
    MATCH (p:Person)-[:KNOWS]->(friend:Person)
    WHERE p.age > 25
    RETURN p.name AS person, friend.name AS friend, p.age AS age
    ORDER BY p.age DESC
""")

# Consume the result
df = table.to_pandas()
print(df)
#   person friend  age
# 0  Alice    Bob   30
```

`forge.execute()` always returns a PyArrow `Table`. Use `table.to_pandas()` for pandas,
`pl.from_arrow(table)` for Polars, or iterate rows with `table.to_pylist()`.

---

## Persist a Graph

Pass a directory path instead of leaving it empty. GraphForge initializes the project inside
that directory, stores the graph as Parquet files, and reloads it automatically on the next
open.

The directory must already exist — GraphForge opens a project root, it does not create the
directory for you. Opening a missing path raises `StorageError: path does not exist`.

```python
from pathlib import Path

Path("research").mkdir(parents=True, exist_ok=True)

forge = GraphForge("research/")
forge.add_node("Paper", title="Graph Neural Networks", year=2024)
forge.close()

# Reload in a later session (the directory now exists)
forge = GraphForge("research/")
table = forge.execute("MATCH (p:Paper) RETURN p.title AS title")
print(table.column("title")[0].as_py())   # Graph Neural Networks
```

---

## Bulk Load

For loading many nodes or edges at once, use the atomic Arrow bulk surfaces
(`publish_bulk_nodes` / `publish_bulk_edges`) or the convenience helpers
`add_nodes()` / `add_edges()`. Pass a stable `operation_uuid` and receive a
canonical receipt table. Inputs may be a list of dicts, a pandas DataFrame, or
an Arrow Table. See [Graph Construction](graph-construction.md) for the full
scalar + bulk path.

The operation identity must be a **UUIDv7** — `uuid.uuid4()` is rejected with
`GF_BULK_VALIDATION(invalid_uuid)`. Python 3.14 ships `uuid.uuid7()`; on earlier
versions generate one with the stdlib helper below.

```python
import os
import pandas as pd
import time
import uuid


def uuid7() -> uuid.UUID:
    """RFC 9562 UUIDv7. Use uuid.uuid7() directly on Python 3.14+."""
    stamp = int(time.time() * 1000).to_bytes(6, "big")
    raw = bytearray(stamp + os.urandom(10))
    raw[6] = (raw[6] & 0x0F) | 0x70  # version 7
    raw[8] = (raw[8] & 0x3F) | 0x80  # RFC 9562 variant
    return uuid.UUID(bytes=bytes(raw))


node_op = str(uuid7())
edge_op = str(uuid7())

# List of dicts → receipt table
nodes = forge.add_nodes(
    "Paper",
    [
        {"title": "Graph Neural Networks", "year": 2021, "citations": 150},
        {"title": "Deep Learning Fundamentals", "year": 2019, "citations": 500},
        {"title": "Attention Is All You Need", "year": 2017, "citations": 2000},
    ],
    operation_uuid=node_op,
)

# From a DataFrame (endpoint columns renamed to source_uuid/target_uuid)
edges_df = pd.DataFrame({
    "src_id": [nodes.column("entity_uuid")[0].as_py(), nodes.column("entity_uuid")[1].as_py()],
    "dst_id": [nodes.column("entity_uuid")[2].as_py(), nodes.column("entity_uuid")[2].as_py()],
    "weight": [0.8, 0.6],
})
forge.add_edges("CITES", edges_df, operation_uuid=edge_op, src="src_id", dst="dst_id")
```

---

## Rank Nodes

`forge.rank()` scores every node of a given label and returns an Arrow Table containing all
node properties plus a `score` column. No mutation happens unless you pass `write_property`.

```python
# Read-only — just get the scores back as a table
table = forge.rank("Person", by="pagerank")
df = table.to_pandas()
print(df[["name", "score"]].sort_values("score", ascending=False))

# Restrict to a specific relationship type
table = forge.rank("Person", by="betweenness", via="KNOWS", directed=False)

# Opt-in write-back — stores the score as a node property
forge.rank("Person", by="pagerank", write_property="rank")
forge.execute("MATCH (n:Person) RETURN n.name, n.rank ORDER BY n.rank DESC LIMIT 5")
```

Example values for `by`: `pagerank`, `betweenness`, `closeness`, `degree`,
`clustering_coefficient`, `triangles`. See the
[complete canonical catalog](../book/architecture/algorithms.md).

---

## Find Relevant Content

`forge.find()` runs a hybrid text + vector search and returns an Arrow Table with node
properties alongside `score` and `matched_on` columns. The index is built automatically on
the first call — no setup step required. `label` is required on every call.

```python
# Text search — index built lazily on first call
table = forge.find("graph neural networks", label="Paper")
df = table.to_pandas()
print(df[["title", "score", "matched_on"]])
#                       title     score matched_on
# 0     Graph Neural Networks  0.924       text
# 1   GNN Applications in NLP  0.781       text

# Restrict to a label and limit results
table = forge.find("graph neural networks", label="Paper", limit=20)

# Hybrid search — pass a vector alongside the text query
import openai
client = openai.OpenAI()
query_vec = client.embeddings.create(
    input="graph neural networks", model="text-embedding-3-small"
).data[0].embedding

table = forge.find("graph neural networks", label="Paper", vector=query_vec)

# Vector-only search
table = forge.find(vector=query_vec, label="Paper")
```

`matched_on` is `"text"`, `"vector"`, or `"text+vector"`. GraphForge stores and queries
vectors but does not generate them — bring your own embeddings from any model.

For explicit control over index timing (e.g. batch ingestion before first search):

```python
forge.index("Paper", properties=["title", "abstract"])

# Vector mode takes the node handle itself (node=), not a numeric id
forge.index("Paper", node=paper_handle, vector=embedding, space="sbert")
```

---

## Group into Communities

`forge.cluster()` assigns every node of a given label to a community and returns an Arrow
Table with node properties plus a `community_id` column.

```python
# Read-only community detection
table = forge.cluster("Person", by="louvain")
df = table.to_pandas()
print(df.groupby("community_id")["name"].apply(list))

# Restrict to a relationship type and write the result back
forge.cluster("Person", by="louvain", via="KNOWS", write_property="community")
forge.execute("""
    MATCH (n:Person)
    RETURN n.community AS community, count(*) AS size
    ORDER BY size DESC LIMIT 5
""")
```

Example values for `by`: `louvain`, `components`. See the
[complete canonical catalog](../book/architecture/algorithms.md).

---

## Next Steps

- [Tutorial](tutorial.md) — guided walkthrough with a full citation network example
- [Graph Construction](graph-construction.md) — scalar Python API and atomic bulk batches
- [Cypher Reference](cypher-guide.md) — complete query language documentation
- [Analytics Integration](analytics-integration.md) — Arrow, pandas, Polars, rank, cluster, find
- [API Reference](../reference/api.md) — full Python API
- [Datasets (backlog)](datasets/overview.md) — planned open-dataset catalogs (not in v0.5.0)
