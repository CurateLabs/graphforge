# Quick Start

Get a graph running in five minutes. Choose **Python** or
**Node** — both are thin bindings over the same Rust engine. Every query and
analyst verb returns Apache Arrow results. Node and edge handles use stable
`.uuid` identity (no numeric storage ids).

For full install options, see [Installation](installation.md).

> **Studio (editor):** Prefer working inside VS Code or Cursor? Install
> **[GraphForge for VS Code](https://marketplace.visualstudio.com/items?itemName=CurateLabsAI.graphforge)**
> (also on [Open VSX](https://open-vsx.org/extension/CurateLabsAI/graphforge)) —
> the editor workflow marketed as **Studio** on the product site. It detects
> Python- and Node-first workspaces, configures the matching binding, and runs
> the same Rust-owned engine. Setup and commands:
> [Studio / VS Code extension guide](vscode-extension/).

---

## Install

### Python

**pip**

```bash
pip install graphforge
```

**uv** (recommended)

```bash
uv add graphforge
```

### Node

**npm**

```bash
npm install @curatelabs/graphforge
```

**pnpm**

```bash
pnpm add @curatelabs/graphforge
```

Node query and analyst-verb results are Arrow IPC buffers. Decode them with
[`apache-arrow`](https://www.npmjs.com/package/apache-arrow) (`tableFromIPC`)
when you want table helpers in JavaScript.

---

## Create and Query a Graph

### Python

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

### Node

```js
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "@curatelabs/graphforge";

const forge = new GraphForge(); // in-memory; use new GraphForge("my-graph/") for persistence

// Add nodes — returns a NodeHandle (use .uuid for identity)
const alice = forge.addNode("Person", { name: "Alice", age: 30 });
const bob = forge.addNode("Person", { name: "Bob", age: 25 });

// Add a relationship
forge.addEdge(alice, "KNOWS", bob, { since: 2020 });

// Query with openCypher — returns an Arrow IPC buffer
const table = tableFromIPC(forge.execute(`
    MATCH (p:Person)-[:KNOWS]->(friend:Person)
    WHERE p.age > 25
    RETURN p.name AS person, friend.name AS friend, p.age AS age
    ORDER BY p.age DESC
`));

console.log(table.toArray());
// [ { person: 'Alice', friend: 'Bob', age: 30 } ]
```

`forge.execute()` returns an Arrow IPC buffer. Decode with `tableFromIPC(...)`
from `apache-arrow`, then use `table.toArray()`, column accessors, or other
Arrow JS helpers.

---

## Persist a Graph

Pass a directory path instead of leaving it empty. GraphForge initializes the project inside
that directory, stores the graph as Parquet files, and reloads it automatically on the next
open.

The directory must already exist — GraphForge opens a project root, it does not create the
directory for you. Opening a missing path raises `StorageError: path does not exist`.

### Python

```python
from pathlib import Path
from graphforge import GraphForge

Path("research").mkdir(parents=True, exist_ok=True)

forge = GraphForge("research/")
forge.add_node("Paper", title="Graph Neural Networks", year=2024)
forge.close()

# Reload in a later session (the directory now exists)
forge = GraphForge("research/")
table = forge.execute("MATCH (p:Paper) RETURN p.title AS title")
print(table.column("title")[0].as_py())   # Graph Neural Networks
```

### Node

```js
import { mkdirSync } from "node:fs";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "@curatelabs/graphforge";

mkdirSync("research", { recursive: true });

let forge = new GraphForge("research/");
forge.addNode("Paper", { title: "Graph Neural Networks", year: 2024 });
forge.close();

// Reload in a later session (the directory now exists)
forge = new GraphForge("research/");
const table = tableFromIPC(
  forge.execute("MATCH (p:Paper) RETURN p.title AS title"),
);
console.log(table.getChild("title").get(0)); // Graph Neural Networks
```

---

## Bulk Load

For loading many nodes or edges at once, use the atomic Arrow bulk surfaces
(`publish_bulk_nodes` / `publish_bulk_edges` in Python;
`publishBulkNodes` / `publishBulkEdges` in Node) or the Python convenience helpers
`add_nodes()` / `add_edges()`. Pass a stable `operation_uuid` and receive a
canonical receipt table. Python inputs may be a list of dicts, a pandas DataFrame,
or an Arrow Table. Node bulk publication takes Arrow IPC. See
[Graph Construction](graph-construction.md) for the full scalar + bulk path.

The operation identity must be a **UUIDv7** — `uuid.uuid4()` is rejected with
`GF_BULK_VALIDATION(invalid_uuid)`. Python 3.14 ships `uuid.uuid7()`; on earlier
versions generate one with the stdlib helper below.

### Python

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

### Node

Node bulk construction publishes Arrow IPC through `publishBulkNodes` /
`publishBulkEdges` with a stable UUIDv7 `operationUuid`. See
[Graph Construction](graph-construction.md) and the Node binding tests for the
IPC table shape.

---

## Rank Nodes

`forge.rank()` scores every node of a given label and returns an Arrow result
containing all node properties plus a `score` column. No mutation happens unless
you pass `write_property` / the write-back argument.

### Python

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

### Node

```js
import { tableFromIPC } from "apache-arrow";

// Read-only — decode the Arrow IPC buffer
const table = tableFromIPC(forge.rank("Person", "pagerank"));
console.log(table.toArray());

// Restrict to a relationship type (via, directed)
const between = tableFromIPC(
  forge.rank("Person", "betweenness", "KNOWS", false),
);

// Opt-in write-back — stores the score as a node property
forge.rank("Person", "pagerank", undefined, true, "rank");
forge.execute(
  "MATCH (n:Person) RETURN n.name, n.rank ORDER BY n.rank DESC LIMIT 5",
);
```

Example values for `by`: `pagerank`, `betweenness`, `closeness`, `degree`,
`clustering_coefficient`, `triangles`. See the
[complete canonical catalog](../book/architecture/algorithms.md).

---

## Find Relevant Content

`forge.find()` runs a hybrid text + vector search and returns an Arrow result with
node properties alongside `score` and `matched_on` columns. The index is built
automatically on the first call — no setup step required. `label` is required on
every call.

### Python

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

### Node

```js
import { tableFromIPC } from "apache-arrow";

// Text search — query, label, then optional vector / similarTo / semanticQuery / limit
const table = tableFromIPC(forge.find("graph neural networks", "Paper"));
console.log(table.toArray());

// Limit results (positional: query, label, vector, similarTo, semanticQuery, limit)
const limited = tableFromIPC(
  forge.find("graph neural networks", "Paper", undefined, undefined, undefined, 20),
);
```

---

## Group into Communities

`forge.cluster()` assigns every node of a given label to a community and returns an
Arrow result with node properties plus a `community_id` column.

### Python

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

### Node

```js
import { tableFromIPC } from "apache-arrow";

// Read-only community detection
const table = tableFromIPC(forge.cluster("Person", "louvain"));
console.log(table.toArray());

// Restrict to a relationship type and write the result back
forge.cluster("Person", "louvain", "KNOWS", false, "community");
forge.execute(`
    MATCH (n:Person)
    RETURN n.community AS community, count(*) AS size
    ORDER BY size DESC LIMIT 5
`);
```

Example values for `by`: `louvain`, `components`. See the
[complete canonical catalog](../book/architecture/algorithms.md).

---

## Next Steps

- [Studio / VS Code extension](vscode-extension/) — explore projects and run Cypher in the editor
- [Tutorial](tutorial.md) — guided walkthrough with a full citation network example
- [Graph Construction](graph-construction.md) — scalar API and atomic bulk batches
- [Cypher Reference](cypher-guide.md) — complete query language documentation
- [Analytics Integration](analytics-integration.md) — Arrow, pandas, Polars, rank, cluster, find
- [API Reference](../reference/api.md) — full Python API
- [Datasets (backlog)](datasets/overview.md) — planned open-dataset catalogs (not in v0.5.0)
