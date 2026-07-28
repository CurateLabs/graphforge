# GraphForge Tutorial

**A step-by-step guide to building, querying, and analyzing graphs with GraphForge (v0.5.0).**

Every `execute` / analyst-verb call returns an Apache Arrow `Table`. Construction
handles use stable `.uuid` identity. For a five-minute path, see
[Quick Start](quickstart.md); this page walks the same surfaces in more depth.

---

## Table of Contents

1. [Installation](#installation)
2. [Your First Graph](#your-first-graph)
3. [Querying with Cypher](#querying-with-cypher)
4. [Working with Persistence](#working-with-persistence)
5. [Advanced Queries](#advanced-queries)
6. [Real-World Example: Citation Network](#real-world-example-citation-network)
7. [Best Practices](#best-practices)
8. [Ranking nodes with forge.rank()](#ranking-nodes-with-forgerank)
9. [Clustering nodes with forge.cluster()](#clustering-nodes-with-forgecluster)
10. [Finding nodes with forge.find()](#finding-nodes-with-forgefind)
11. [Next Steps](#next-steps)

---

## Installation

Install GraphForge using uv (recommended) or pip:

```bash
# Using uv
uv add graphforge

# Using pip
pip install graphforge
```

Verify the installation:

```python
from graphforge import GraphForge
print("GraphForge installed successfully!")
```

---

## Your First Graph

Let's build a simple social network.

### Step 1: Create an In-Memory Graph

```python
from graphforge import GraphForge

# Create an in-memory graph (data is not persisted between sessions)
forge = GraphForge()
```

### Step 2: Add Nodes

Nodes represent entities in your graph. Use the Python API to create nodes:

```python
# add_node returns a NodeHandle with stable .uuid identity (no numeric id)
alice = forge.add_node("Person", name="Alice", age=30, city="NYC")
bob   = forge.add_node("Person", name="Bob",   age=25, city="NYC")
charlie = forge.add_node("Person", name="Charlie", age=35, city="LA")

print(f"Created node uuid: {alice.uuid}")
```

### Step 3: Add Relationships

Relationships connect nodes:

```python
# add_edge connects two NodeHandles with a named relationship type
forge.add_edge(alice, "KNOWS", bob,     since=2015, strength="strong")
forge.add_edge(alice, "KNOWS", charlie, since=2018, strength="medium")
```

### Step 4: Query the Graph

Use Cypher queries to explore your graph. `execute()` returns an Apache Arrow Table:

```python
# Find all people Alice knows
table = forge.execute("""
    MATCH (a:Person)-[r:KNOWS]->(friend:Person)
    WHERE a.name = 'Alice'
    RETURN friend.name AS name, r.since AS since
    ORDER BY r.since
""")

print("Alice knows:")
for row in table.to_pylist():
    print(f"  - {row['name']} (since {row['since']})")
```

**Output:**
```
Alice knows:
  - Bob (since 2015)
  - Charlie (since 2018)
```

---

## Querying with Cypher

GraphForge supports the full openCypher language for declarative graph queries.
Every `execute()` call returns a PyArrow Table — use `table.to_pandas()`,
`pl.from_arrow(table)`, or `table.to_pylist()` to work with the results.

### Basic Pattern Matching

```python
# Match all nodes
table = forge.execute("MATCH (n) RETURN n")
print(f"Total nodes: {table.num_rows}")

# Match nodes by label
table = forge.execute("""
    MATCH (p:Person)
    RETURN p.name AS name, p.age AS age
""")

for row in table.to_pylist():
    print(f"{row['name']} is {row['age']} years old")
```

### Filtering with WHERE

```python
# Find people over 25
table = forge.execute("""
    MATCH (p:Person)
    WHERE p.age > 25
    RETURN p.name AS name
""")

# Multiple conditions
table = forge.execute("""
    MATCH (p:Person)
    WHERE p.age > 25 AND p.city = 'NYC'
    RETURN p.name AS name
""")
```

### Traversing Relationships

```python
# Find who knows whom
table = forge.execute("""
    MATCH (a:Person)-[r:KNOWS]->(b:Person)
    RETURN a.name AS from, b.name AS to, r.since AS since
""")

# Two-hop traversal
table = forge.execute("""
    MATCH (a:Person)-[:KNOWS]->(:Person)-[:KNOWS]->(c:Person)
    WHERE a.name = 'Alice'
    RETURN c.name AS friend_of_friend
""")
```

### Aggregations

```python
# Count nodes
table = forge.execute("""
    MATCH (p:Person)
    RETURN count(*) AS total
""")
print(f"Total people: {table.column('total')[0].as_py()}")

# Group and aggregate
table = forge.execute("""
    MATCH (p:Person)
    RETURN p.city AS city, count(*) AS population
    ORDER BY population DESC
""")

for row in table.to_pylist():
    print(f"{row['city']}: {row['population']} people")

# Multiple aggregations
table = forge.execute("""
    MATCH (p:Person)
    RETURN
        count(*) AS total,
        avg(p.age) AS avg_age,
        min(p.age) AS youngest,
        max(p.age) AS oldest
""")

row = table.to_pylist()[0]
print(f"Total: {row['total']}")
print(f"Average age: {row['avg_age']:.1f}")
print(f"Age range: {row['youngest']}-{row['oldest']}")
```

---

## Working with Persistence

So far, we've used in-memory graphs that disappear when the program exits. Pass a directory
path to persist data to Parquet files on disk.

The directory must already exist. GraphForge initializes a project inside an existing
directory; it does not create the directory itself. Opening a path that does not exist raises
`StorageError: path does not exist`.

### Creating a Persistent Graph

```python
from pathlib import Path

# The project directory must exist before opening it
Path("my-research-graph").mkdir(parents=True, exist_ok=True)

# Specify a directory path for Parquet-backed storage
forge = GraphForge("my-research-graph/")

# Add data (works the same as in-memory)
forge.execute("CREATE (p:Person {name: 'Alice', age: 30})")
forge.execute("CREATE (p:Person {name: 'Bob', age: 25})")

# Flush and close
forge.close()
```

### Loading an Existing Graph

```python
# Later, in a new session...
forge = GraphForge("my-research-graph/")

# Data is still there
table = forge.execute("MATCH (p:Person) RETURN p.name AS name")
for row in table.to_pylist():
    print(f"Found: {row['name']}")

forge.close()
```

### Incremental Updates

```python
from pathlib import Path

Path("knowledge-base").mkdir(parents=True, exist_ok=True)

# Session 1: Initial data
forge = GraphForge("knowledge-base/")
forge.execute("CREATE (:Concept {name: 'Graph Databases'})")
forge.close()

# Session 2: Add more data
forge = GraphForge("knowledge-base/")
forge.execute("CREATE (:Concept {name: 'SQL Databases'})")
forge.execute("""
    MATCH (gdb:Concept {name: 'Graph Databases'})
    MATCH (sql:Concept {name: 'SQL Databases'})
    CREATE (gdb)-[:DIFFERENT_FROM]->(sql)
""")
forge.close()

# Session 3: Query accumulated data
forge = GraphForge("knowledge-base/")
table = forge.execute("MATCH (c:Concept) RETURN count(*) AS count")
print(f"Total concepts: {table.column('count')[0].as_py()}")  # 2
forge.close()
```

---

## Advanced Queries

### CREATE: Building Graphs with Cypher

You can use Cypher CREATE instead of the Python API:

```python
# Create nodes
forge.execute("CREATE (p:Person {name: 'Alice', age: 30})")

# Create nodes with relationships in one statement
forge.execute("""
    CREATE (a:Person {name: 'Alice'})-[:KNOWS {since: 2020}]->(b:Person {name: 'Bob'})
""")

# Create with RETURN
table = forge.execute("""
    CREATE (p:Person {name: 'Charlie', age: 35})
    RETURN p.name AS name, p.age AS age
""")
print(f"Created: {table.column('name')[0].as_py()}")
```

### SET: Updating Properties

```python
# Update single property
forge.execute("""
    MATCH (p:Person {name: 'Alice'})
    SET p.age = 31
""")

# Update multiple properties
forge.execute("""
    MATCH (p:Person {name: 'Alice'})
    SET p.age = 31, p.city = 'Boston', p.active = true
""")

# Update relationship properties
forge.execute("""
    MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'})
    SET r.strength = 'strong'
""")
```

### DELETE: Removing Data

```python
# Delete a node (must have no relationships — use DETACH DELETE to also remove them)
forge.execute("""
    MATCH (p:Person {name: 'Charlie'})
    DETACH DELETE p
""")

# Delete only a relationship
forge.execute("""
    MATCH (a:Person {name: 'Alice'})-[r:KNOWS]->(b:Person {name: 'Bob'})
    DELETE r
""")

# Delete multiple elements
forge.execute("""
    MATCH (p:Person)
    WHERE p.age < 25
    DELETE p
""")
```

### MERGE: Idempotent Creation

MERGE creates nodes if they don't exist, or matches them if they do:

```python
# Safe to run multiple times
forge.execute("MERGE (p:Person {name: 'Alice'})")
forge.execute("MERGE (p:Person {name: 'Alice'})")  # Matches existing node

# Check result
table = forge.execute("MATCH (p:Person {name: 'Alice'}) RETURN count(*) AS count")
print(table.column("count")[0].as_py())  # 1, not 2!

# Useful for ETL pipelines
forge.execute("MERGE (p:Person {name: 'Bob', email: 'bob@example.com'})")
```

---

## Real-World Example: Citation Network

Let's build a realistic citation network graph.

### Setup

```python
from pathlib import Path

from graphforge import GraphForge

Path("citation-network").mkdir(parents=True, exist_ok=True)
forge = GraphForge("citation-network/")
```

### Load Papers

```python
import os
import time
import uuid


def uuid7() -> uuid.UUID:
    """RFC 9562 UUIDv7. Use uuid.uuid7() directly on Python 3.14+."""
    stamp = int(time.time() * 1000).to_bytes(6, "big")
    raw = bytearray(stamp + os.urandom(10))
    raw[6] = (raw[6] & 0x0F) | 0x70  # version 7
    raw[8] = (raw[8] & 0x3F) | 0x80  # RFC 9562 variant
    return uuid.UUID(bytes=bytes(raw))


papers = [
    {"id": "P1", "title": "Graph Neural Networks", "year": 2021, "citations": 150},
    {"id": "P2", "title": "Deep Learning Fundamentals", "year": 2019, "citations": 500},
    {"id": "P3", "title": "GNN Applications in NLP", "year": 2022, "citations": 80},
    {"id": "P4", "title": "Attention Is All You Need", "year": 2017, "citations": 2000},
]

# Bulk helpers require a stable UUIDv7 operation_uuid and return a receipt table
receipt = forge.add_nodes("Paper", papers, operation_uuid=str(uuid7()))
print(f"Loaded {receipt.num_rows} papers")
```

### Add Authors

```python
authors = [
    {"name": "Alice Smith", "affiliation": "MIT"},
    {"name": "Bob Jones", "affiliation": "Stanford"},
    {"name": "Charlie Brown", "affiliation": "MIT"},
]

for author in authors:
    forge.execute(f"""
        MERGE (a:Author {{name: '{author['name']}'}})
        SET a.affiliation = '{author['affiliation']}'
    """)
```

### Link Authors to Papers

```python
authorships = [
    ("Alice Smith", "P1"),
    ("Alice Smith", "P3"),
    ("Bob Jones", "P2"),
    ("Charlie Brown", "P1"),
    ("Charlie Brown", "P4"),
]

for author_name, paper_id in authorships:
    forge.execute(f"""
        MATCH (a:Author {{name: '{author_name}'}})
        MATCH (p:Paper {{id: '{paper_id}'}})
        CREATE (a)-[:AUTHORED]->(p)
    """)
```

### Add Citation Links

```python
citations = [
    ("P1", "P2"),  # P1 cites P2
    ("P1", "P4"),  # P1 cites P4
    ("P3", "P1"),  # P3 cites P1
    ("P3", "P2"),  # P3 cites P2
]

for citing_id, cited_id in citations:
    forge.execute(f"""
        MATCH (citing:Paper {{id: '{citing_id}'}})
        MATCH (cited:Paper {{id: '{cited_id}'}})
        CREATE (citing)-[:CITES]->(cited)
    """)
```

### Analysis 1: Most Prolific Authors

```python
table = forge.execute("""
    MATCH (a:Author)-[:AUTHORED]->(p:Paper)
    RETURN a.name AS author, count(p) AS paper_count
    ORDER BY paper_count DESC
""")

print("Most prolific authors:")
for row in table.to_pylist():
    print(f"  {row['author']}: {row['paper_count']} papers")
```

**Output:**
```
Most prolific authors:
  Alice Smith: 2 papers
  Charlie Brown: 2 papers
  Bob Jones: 1 papers
```

### Analysis 2: Most Cited Papers

```python
table = forge.execute("""
    MATCH (p:Paper)<-[:CITES]-(citing:Paper)
    RETURN p.title AS paper, count(citing) AS citation_count
    ORDER BY citation_count DESC
""")

print("\nMost cited papers (in-network):")
for row in table.to_pylist():
    print(f"  {row['paper']}: {row['citation_count']} citations")
```

### Analysis 3: Collaboration Network

```python
table = forge.execute("""
    MATCH (a1:Author)-[:AUTHORED]->(p:Paper)<-[:AUTHORED]-(a2:Author)
    WHERE a1.name < a2.name
    RETURN a1.name AS author1, a2.name AS author2, count(p) AS papers
    ORDER BY papers DESC
""")

print("\nAuthor collaborations:")
for row in table.to_pylist():
    print(f"  {row['author1']} & {row['author2']}: {row['papers']} papers")
```

### Analysis 4: Papers by MIT Authors

```python
table = forge.execute("""
    MATCH (a:Author)-[:AUTHORED]->(p:Paper)
    WHERE a.affiliation = 'MIT'
    RETURN p.title AS paper, a.name AS author
""")

print("\nMIT papers:")
for row in table.to_pylist():
    print(f"  {row['paper']} by {row['author']}")
```

### Cleanup

```python
forge.close()
```

---

## Best Practices

### 1. Choose Storage Mode Appropriately

**Use in-memory graphs for:**
- Quick exploration and prototyping
- Throwaway analyses
- Testing

**Use persistent graphs for:**
- Long-running analyses
- Incremental graph construction
- Shared datasets
- Production workflows

```python
# Exploration
forge = GraphForge()

# Persistent
forge = GraphForge("production-graph/")
```

### 2. Always Close Persistent Graphs

```python
# Good: Using try-finally
forge = GraphForge("my-graph/")
try:
    # ... work with graph ...
    pass
finally:
    forge.close()
```

### 3. Use MERGE for Idempotent Operations

```python
# Safe to run multiple times
forge.execute("MERGE (p:Person {email: 'alice@example.com'})")
forge.execute("MERGE (p:Person {email: 'alice@example.com'})")  # No duplicates

# Avoid this pattern
forge.execute("CREATE (p:Person {email: 'alice@example.com'})")
forge.execute("CREATE (p:Person {email: 'alice@example.com'})")  # Creates duplicate!
```

### 4. Work with Arrow Results Directly

```python
table = forge.execute("MATCH (p:Person) RETURN p.name AS name, p.age AS age")

# pandas
df = table.to_pandas()

# Polars
import polars as pl
df = pl.from_arrow(table)

# Plain Python iteration
for row in table.to_pylist():
    name = row["name"]
    age  = row["age"]
```

### 5. Use WHERE for Complex Filtering

```python
# Prefer this
forge.execute("""
    MATCH (p:Person)
    WHERE p.age > 25 AND p.city = 'NYC'
    RETURN p
""")
```

### 6. Order Results Before Using LIMIT

```python
# Always order when using LIMIT
forge.execute("""
    MATCH (p:Person)
    RETURN p.name AS name, p.age AS age
    ORDER BY p.age DESC
    LIMIT 10
""")

# Without ORDER BY, results are non-deterministic
```

---

## Ranking nodes with forge.rank()

`forge.rank()` scores every node of a given label and returns an Arrow Table with node
properties plus a `score` column. It is read-only unless you pass `write_property`.

## Clustering nodes with forge.cluster()

`forge.cluster()` assigns community membership and returns an Arrow Table with a
`community_id` column. Same read-only default and optional write-back.

### Examples

`forge.rank()` and `forge.cluster()` run compiled graph algorithms directly — no Cypher
needed, no exporting the graph first. Both return Arrow Tables and are read-only by default.

```python
from graphforge import GraphForge
from graphforge.datasets import load_dataset

forge = GraphForge()
load_dataset(forge, "snap-ego-facebook")

# Score every node by PageRank — returns Arrow Table with a 'score' column
table = forge.rank("Node", by="pagerank")
df = table.to_pandas().sort_values("score", ascending=False)
print(df[["id", "score"]].head(10))

# Restrict to a specific relationship type and direction
table = forge.rank("Node", by="betweenness", via="KNOWS", directed=True)

# Opt-in write-back — stores the score as a node property for Cypher queries
forge.rank("Node", by="pagerank", write_property="pagerank")
top_nodes = forge.execute("""
    MATCH (n)
    RETURN n.id AS user, n.pagerank AS score
    ORDER BY n.pagerank DESC LIMIT 10
""")
print(top_nodes.to_pandas())

# Community detection — returns Arrow Table with a 'community_id' column
table = forge.cluster("Node", by="louvain")
df = table.to_pandas()
print(df.groupby("community_id").size().sort_values(ascending=False).head(5))

# Write communities back and query the structure
forge.cluster("Node", by="louvain", write_property="community")
communities = forge.execute("""
    MATCH (n)
    RETURN n.community AS community, count(*) AS size
    ORDER BY size DESC LIMIT 5
""")
print(communities.to_pandas())
```

**Example algorithms** (see the
[complete canonical catalog](../book/architecture/algorithms.md)):

| Method | `by=` value | Category |
|--------|-------------|----------|
| `forge.rank()` | `pagerank` | Centrality |
| `forge.rank()` | `betweenness` | Centrality |
| `forge.rank()` | `closeness` | Centrality |
| `forge.rank()` | `degree` | Centrality |
| `forge.rank()` | `clustering_coefficient` | Structural |
| `forge.rank()` | `triangles` | Structural |
| `forge.cluster()` | `louvain` | Community |
| `forge.cluster()` | `components` | Community |

All methods accept optional `via` (relationship type), `directed`, and `write_property`
parameters. Without `write_property`, the graph is never mutated.

---

## Finding nodes with forge.find()

`forge.find()` provides full-text search, vector similarity, and hybrid search over node
properties. It returns an Arrow Table with node properties plus `score` and `matched_on`
columns. The index is built automatically on the first call.

### Text Search

```python
from pathlib import Path

Path("citations").mkdir(parents=True, exist_ok=True)
forge = GraphForge("citations/")

# Index built lazily on first call — no setup required (label is required)
table = forge.find("graph neural networks", label="Paper", limit=10)
df = table.to_pandas()
print(df[["title", "score", "matched_on"]])
#                         title     score matched_on
# 0       Graph Neural Networks  0.924       text
# 1   GNN Applications in NLP   0.781       text
```

### Vector Search (bring your own embeddings)

GraphForge stores and queries vectors but does not generate them. Use any embedding model:

```python
import openai
client = openai.OpenAI()

def embed(text: str) -> list[float]:
    return client.embeddings.create(
        input=text, model="text-embedding-3-small"
    ).data[0].embedding

# Store embeddings for each paper. Identity is the UUID `node_uuid`; the vector
# mode of index() takes `node=` as a UUID string, a NodeHandle, or a selector dict.
import uuid

rows = forge.execute("MATCH (n:Paper) RETURN n.node_uuid AS nid, n.abstract AS abstract")
for row in rows.to_pylist():
    vec = embed(row["abstract"] or "")
    forge.index(
        "Paper",
        node=str(uuid.UUID(bytes=row["nid"])),
        vector=vec,
        space="text-embedding-3-small",
    )

# Query by vector similarity
query_vec = embed("scalable graph representation learning")
table = forge.find(vector=query_vec, label="Paper", limit=10)
```

### Hybrid Search

Combine text and vector signals in a single call:

```python
table = forge.find("scalable graph learning", label="Paper", vector=query_vec, limit=10)

for row in table.to_pylist():
    print(
        row["title"],
        f"score={row['score']:.3f}",
        f"via={row['matched_on']}",  # "text", "vector", or "text+vector"
    )
```

### Using Results in Cypher

Every row in the result table has a `node_uuid` column. Bind it as a `uuid.UUID` and match on
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
predicate — there is no numeric `id()` surrogate in v0.5.0.

---

## Next Steps

Congratulations! You've learned the fundamentals of GraphForge.

### Learn More

- **[API Reference](../reference/api.md)** — Complete Python API documentation
- **[Cypher Guide](../guide/cypher-guide.md)** — Full openCypher subset reference
- **[Analytics Integration](../guide/analytics-integration.md)** — Arrow, pandas, Polars, rank, cluster, find
- **[Architecture Overview](../book/architecture/overview.md)** — System design and internals

### Try These Exercises

1. **Social Network**: Build a graph of friends and their relationships. Use `forge.rank()` to find the most influential people.

2. **Knowledge Graph**: Extract entities from a document and link them with relationships. Use `forge.find()` to retrieve relevant context.

3. **Citation Analysis**: Load a set of papers and citations. Rank papers by betweenness centrality to find bridging works.

4. **Recommendation System**: Build a user-item graph and find similar users or items using `forge.cluster()` to identify taste communities.

5. **Data Lineage**: Track transformations in a data pipeline and query dependencies.

### Join the Community

- Report issues on [GitHub](https://github.com/CurateLabs/graphforge-legecy/issues)
- Continue in the [Book](../book/README.md) for architecture and deeper usage
