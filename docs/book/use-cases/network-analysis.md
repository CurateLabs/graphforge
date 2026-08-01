# Network Analysis in Notebooks

## Overview

GraphForge is a natural fit for network analysis workflows in Jupyter notebooks. It combines openCypher with analyst verbs and Parquet-backed projects, so you can load a dataset once, run exploratory queries interactively, persist computed metrics, and share a project directory as a reproducible artefact.

### Why GraphForge Instead of NetworkX?

| Feature | GraphForge | NetworkX |
|---------|-----------|----------|
| **Query language** | Declarative openCypher — patterns, aggregations, paths | Imperative Python — write loops manually |
| **Persistence** | Parquet project directory, zero config | `pickle` / GraphML / custom serializers |
| **Results** | Apache Arrow Tables | Python objects |
| **Dataset library** | `load_dataset()` for real-world graphs | Manual download and parsing |
| **Heterogeneous graphs** | Multiple labels, arbitrary property maps | Single node/edge attribute dicts |
| **Sharing** | Pass one project directory | Serialize and pray on pickle version |
| **Scale** | Research / notebook scale (see scale limits) | In-memory only, can exhaust RAM |

Both tools are complementary. GraphForge excels at storage, querying, and sharing; NetworkX can remain a development parity oracle. The [pandas integration section](#integration-with-pandas) shows how to bridge the two.

## Installation

**pip**

```bash
pip install "graphforge==0.5.1"
```

**uv** (recommended)

```bash
uv add "graphforge==0.5.1"
```

For persistent notebooks, use a Parquet-backed project directory so results survive kernel restarts:

```python
from graphforge import GraphForge

# In-memory (lost when kernel restarts)
forge = GraphForge()

# Persistent (survives restarts, shareable)
forge = GraphForge("analysis/")
```

---

## Loading a Real Dataset

GraphForge ships with a curated dataset library. The `snap-ego-facebook` dataset is a real-world social network from Stanford SNAP: ~4 000 nodes, ~88 000 edges.

```python
from graphforge import GraphForge
from graphforge.datasets import load_dataset, list_datasets

# See all available datasets
print(list_datasets())
# ['snap-ego-facebook', 'snap-ca-grqc', ...]

db = GraphForge("facebook/")
load_dataset(db, "snap-ego-facebook")
```

Verify the load:

```python
result = db.execute("MATCH (n) RETURN count(n) AS nodes")
print(result[0]['nodes'].value)   # 4039

result = db.execute("MATCH ()-[r]->() RETURN count(r) AS edges")
print(result[0]['edges'].value)   # 88234
```

Quick schema check — what labels and relationship types exist?

```python
labels = db.execute("MATCH (n) RETURN DISTINCT labels(n) AS lbls LIMIT 10")
for row in labels:
    print(row['lbls'].value)
# ['Node']

rel_types = db.execute("MATCH ()-[r]->() RETURN DISTINCT type(r) AS t")
for row in rel_types:
    print(row['t'].value)
# 'CONNECTED_TO'
```

---

## Degree Distribution

### Out-degree and In-degree

```python
# Out-degree distribution (how many connections each node has)
out_deg = db.execute("""
    MATCH (n:Node)
    OPTIONAL MATCH (n)-[:CONNECTED_TO]->()
    WITH n, count(*) AS out_deg
    RETURN out_deg, count(n) AS freq
    ORDER BY out_deg
""")

for row in out_deg[:5]:
    print(f"out_deg={row['out_deg'].value}  freq={row['freq'].value}")
# out_deg=0   freq=12
# out_deg=1   freq=243
# out_deg=2   freq=301
# ...
```

```python
# In-degree distribution
in_deg = db.execute("""
    MATCH (n:Node)
    OPTIONAL MATCH ()-[:CONNECTED_TO]->(n)
    WITH n, count(*) AS in_deg
    RETURN in_deg, count(n) AS freq
    ORDER BY in_deg
""")
```

### Top-N Hubs

Find the most connected nodes by total degree:

```python
hubs = db.execute("""
    MATCH (n:Node)
    OPTIONAL MATCH (n)-[:CONNECTED_TO]-(neighbor)
    WITH n, count(DISTINCT neighbor) AS degree
    RETURN n.id AS node_id, degree
    ORDER BY degree DESC
    LIMIT 10
""")

for row in hubs:
    print(f"node {row['node_id'].value}  degree={row['degree'].value}")
# node 107   degree=1045
# node 1684  degree=792
# node 3437  degree=547
# ...
```

---

## Path Analysis

### Reachability with Variable-Length Paths

Check whether two nodes are connected within a certain hop radius:

```python
# Is node 107 reachable from node 0 within 3 hops?
result = db.execute("""
    MATCH (a:Node {id: '0'})-[*1..3]-(b:Node {id: '107'})
    RETURN count(*) AS path_count
""")
print(result[0]['path_count'].value)   # 1 (or more, depending on paths found)
```

### Finding All Short Paths Between Two Nodes

Variable-length patterns return every qualifying path. Use `LIMIT` to avoid combinatorial explosion:

```python
paths = db.execute("""
    MATCH p = (a:Node {id: '0'})-[:CONNECTED_TO*1..4]-(b:Node {id: '42'})
    RETURN length(p) AS hops
    ORDER BY hops
    LIMIT 20
""")

for row in paths:
    print(f"path of length {row['hops'].value}")
# path of length 2
# path of length 2
# path of length 3
# ...
```

### Breadth-First Distance Approximation

`shortestPath()` is not yet implemented — calling it raises `NotImplementedError` with a workaround hint. The equivalent BFS pattern queries increasing hop counts:

```python
def approximate_distance(db, src_id: str, dst_id: str, max_hops: int = 6) -> int | None:
    """Return the smallest hop count between two nodes, or None if unreachable."""
    for hops in range(1, max_hops + 1):
        result = db.execute(
            f"""
            MATCH (a:Node {{id: $src}})-[:CONNECTED_TO*{hops}]-(b:Node {{id: $dst}})
            RETURN count(*) AS found
            """,
            {"src": src_id, "dst": dst_id},
        )
        if result[0]['found'].value > 0:
            return hops
    return None

dist = approximate_distance(db, "0", "107")
print(dist)   # 2
```

---

## Community Detection Proxy

Full community detection algorithms (Louvain, label propagation) require algorithmic libraries. GraphForge can act as the storage and retrieval layer while surfacing natural cluster structure through **neighbor overlap** queries.

### Jaccard Similarity of Neighborhoods

Two nodes with heavily overlapping neighbor sets are likely in the same community:

```python
# Jaccard similarity between two specific nodes
jaccard = db.execute("""
    MATCH (a:Node {id: '107'})-[:CONNECTED_TO]-(common)-[:CONNECTED_TO]-(b:Node {id: '1684'})
    WITH count(DISTINCT common) AS shared
    MATCH (a:Node {id: '107'})-[:CONNECTED_TO]-(na)
    WITH shared, count(DISTINCT na) AS deg_a
    MATCH (b:Node {id: '1684'})-[:CONNECTED_TO]-(nb)
    WITH shared, deg_a, count(DISTINCT nb) AS deg_b
    RETURN toFloat(shared) / (deg_a + deg_b - shared) AS jaccard
""")
print(jaccard[0]['jaccard'].value)   # e.g. 0.312
```

### Finding Dense Triangles (Friend-of-Friend Clusters)

Triangles are the building blocks of tightly-knit communities:

```python
# Count triangles each node participates in (sampled to top 20)
triangles = db.execute("""
    MATCH (a:Node)-[:CONNECTED_TO]-(b:Node)-[:CONNECTED_TO]-(c:Node)-[:CONNECTED_TO]-(a)
    WITH a, count(*) / 2 AS triangle_count
    RETURN a.id AS node_id, triangle_count
    ORDER BY triangle_count DESC
    LIMIT 20
""")

for row in triangles:
    print(f"node {row['node_id'].value}  triangles={row['triangle_count'].value}")
```

### Identifying Bridge Nodes

Nodes that connect otherwise-separate clusters have few common neighbors relative to their degree:

```python
# Low neighbor overlap = structural bridge candidate
bridges = db.execute("""
    MATCH (a:Node)-[:CONNECTED_TO]-(b:Node)
    WITH a, b,
         [(a)-[:CONNECTED_TO]-(x)-[:CONNECTED_TO]-(b) | x] AS common_neighbors
    WITH a, b, size(common_neighbors) AS overlap
    WHERE overlap = 0
    WITH a, count(b) AS isolated_connections
    RETURN a.id AS node_id, isolated_connections
    ORDER BY isolated_connections DESC
    LIMIT 10
""")
```

---

## Citation Network Analysis

### Building a Small Citation Network

```python
db_cite = GraphForge("citations/")

# Create papers
papers = [
    ("p1", "Attention Is All You Need", 2017),
    ("p2", "BERT: Pre-training of Deep Bidirectional Transformers", 2018),
    ("p3", "GPT-3: Language Models are Few-Shot Learners", 2020),
    ("p4", "Scaling Laws for Neural Language Models", 2020),
    ("p5", "Chain-of-Thought Prompting Elicits Reasoning", 2022),
    ("p6", "Emergent Abilities of Large Language Models", 2022),
]

for pid, title, year in papers:
    db_cite.execute(
        "CREATE (:Paper {id: $id, title: $title, year: $year})",
        {"id": pid, "title": title, "year": year},
    )

# Create citation edges (citing -> cited)
citations = [
    ("p2", "p1"), ("p3", "p1"), ("p3", "p2"),
    ("p4", "p1"), ("p4", "p3"), ("p5", "p1"),
    ("p5", "p2"), ("p5", "p3"), ("p6", "p3"),
    ("p6", "p4"), ("p6", "p5"),
]

for citing, cited in citations:
    db_cite.execute(
        """
        MATCH (a:Paper {id: $citing}), (b:Paper {id: $cited})
        CREATE (a)-[:CITES]->(b)
        """,
        {"citing": citing, "cited": cited},
    )
```

### Most-Cited Papers

```python
most_cited = db_cite.execute("""
    MATCH (cited:Paper)<-[:CITES]-(citing:Paper)
    WITH cited, count(citing) AS citation_count
    RETURN cited.title AS title, cited.year AS year, citation_count
    ORDER BY citation_count DESC
""")

for row in most_cited:
    print(f"{row['citation_count'].value}x  {row['title'].value} ({row['year'].value})")
# 5x  Attention Is All You Need (2017)
# 4x  GPT-3: Language Models are Few-Shot Learners (2020)
# 3x  BERT: Pre-training of Deep Bidirectional Transformers (2018)
```

### Co-Citation Pairs

Two papers that are frequently cited together are semantically related:

```python
co_citations = db_cite.execute("""
    MATCH (a:Paper)<-[:CITES]-(bridge:Paper)-[:CITES]->(b:Paper)
    WHERE id(a) < id(b)
    WITH a, b, count(bridge) AS co_cite_count
    WHERE co_cite_count >= 2
    RETURN a.title AS paper_a, b.title AS paper_b, co_cite_count
    ORDER BY co_cite_count DESC
""")

for row in co_citations:
    print(
        f"co-cited {row['co_cite_count'].value}x: "
        f"{row['paper_a'].value[:30]}...  <->  {row['paper_b'].value[:30]}..."
    )
```

### Citation Chains (Intellectual Lineage)

```python
# What is the full citation ancestry of the Chain-of-Thought paper?
ancestry = db_cite.execute("""
    MATCH (root:Paper {id: 'p5'})-[:CITES*1..5]->(ancestor:Paper)
    RETURN DISTINCT ancestor.title AS title, ancestor.year AS year
    ORDER BY ancestor.year
""")

for row in ancestry:
    print(f"{row['year'].value}  {row['title'].value}")
# 2017  Attention Is All You Need
# 2018  BERT: Pre-training of Deep Bidirectional Transformers
# 2020  GPT-3: Language Models are Few-Shot Learners
```

---

## Temporal Network Analysis

Track when relationships were created and analyze network evolution over time.

### Creating a Temporal Network

```python
db_temp = GraphForge("temporal_social/")

# Create users
for uid, name in [("u1", "Alice"), ("u2", "Bob"), ("u3", "Carol"), ("u4", "Dave")]:
    db_temp.execute(
        "CREATE (:User {id: $id, name: $name})",
        {"id": uid, "name": name},
    )

# Create FOLLOWS edges with a `since` date string (ISO 8601)
follows = [
    ("u1", "u2", "2023-01-15"),
    ("u1", "u3", "2023-03-22"),
    ("u2", "u3", "2023-06-01"),
    ("u3", "u4", "2024-01-10"),
    ("u2", "u4", "2024-02-28"),
    ("u4", "u1", "2024-11-05"),
]

for src, dst, since in follows:
    db_temp.execute(
        """
        MATCH (a:User {id: $src}), (b:User {id: $dst})
        CREATE (a)-[:FOLLOWS {since: $since}]->(b)
        """,
        {"src": src, "dst": dst, "since": since},
    )
```

### Querying by Date

```python
# All connections formed on or after 2024-01-01
new_connections = db_temp.execute("""
    MATCH (a:User)-[r:FOLLOWS]->(b:User)
    WHERE r.since >= '2024-01-01'
    RETURN a.name AS follower, b.name AS followee, r.since AS since
    ORDER BY r.since
""")

for row in new_connections:
    print(f"{row['follower'].value} -> {row['followee'].value}  ({row['since'].value})")
# Carol -> Dave  (2024-01-10)
# Bob   -> Dave  (2024-02-28)
# Dave  -> Alice (2024-11-05)
```

### Network Growth Over Time

```python
# Count cumulative edges added per quarter (string comparison on ISO dates works correctly)
growth = db_temp.execute("""
    MATCH ()-[r:FOLLOWS]->()
    WITH
        CASE
            WHEN r.since < '2023-04-01' THEN 'Q1 2023'
            WHEN r.since < '2023-07-01' THEN 'Q2 2023'
            WHEN r.since < '2024-01-01' THEN 'Q3-Q4 2023'
            ELSE '2024+'
        END AS period,
        count(*) AS new_edges
    RETURN period, new_edges
    ORDER BY period
""")

for row in growth:
    print(f"{row['period'].value}: {row['new_edges'].value} new connections")
# Q1 2023: 2
# Q2 2023: 1
# Q3-Q4 2023: 0   (implicit — no rows returned for empty periods)
# 2024+: 3
```

### Nodes Added Before a Cutoff with OPTIONAL MATCH

Use `OPTIONAL MATCH` to find users with no recent connections:

```python
inactive = db_temp.execute("""
    MATCH (u:User)
    OPTIONAL MATCH (u)-[r:FOLLOWS]->()
    WHERE r.since >= '2024-01-01'
    WITH u, count(r) AS recent_out
    WHERE recent_out = 0
    RETURN u.name AS name
""")

for row in inactive:
    print(row['name'].value)
# Alice
# Bob  (Bob's only 2024 edge is incoming, not outgoing)
```

---

## Persisting Analysis Results

Computed metrics can be stored back as node properties using `SET`. This makes them available for future sessions and avoids recomputation.

### Writing Degree Back to Nodes

```python
# Compute and persist degree centrality for the Facebook graph
db.execute("""
    MATCH (n:Node)
    OPTIONAL MATCH (n)-[:CONNECTED_TO]-(neighbor)
    WITH n, count(DISTINCT neighbor) AS degree
    SET n.degree = degree
""")
```

Verify the write:

```python
sample = db.execute("""
    MATCH (n:Node)
    WHERE EXISTS { MATCH (n)-[:CONNECTED_TO]-() }
    RETURN n.id AS id, n.degree AS degree
    ORDER BY degree DESC
    LIMIT 5
""")

for row in sample:
    print(f"node {row['id'].value}  degree={row['degree'].value}")
```

### Persisting Triangle Counts

```python
db.execute("""
    MATCH (a:Node)-[:CONNECTED_TO]-(b:Node)-[:CONNECTED_TO]-(c:Node)-[:CONNECTED_TO]-(a)
    WITH a, count(*) / 2 AS triangles
    SET a.triangle_count = triangles
""")
```

### Reloading Across Sessions

Because `db = GraphForge("facebook/")` persists as a Parquet project, any subsequent session can access the pre-computed properties immediately:

```python
# New notebook session — no recomputation needed
db = GraphForge("facebook/")

top_nodes = db.execute("""
    MATCH (n:Node)
    WHERE n.degree IS NOT NULL
    RETURN n.id AS id, n.degree AS degree, n.triangle_count AS triangles
    ORDER BY degree DESC
    LIMIT 10
""")
```

---

## Integration with pandas

`db.to_dataframe(query)` runs a query and returns a pandas DataFrame with all `CypherValue`s automatically unwrapped. No helper function needed.

### Degree Distribution Plot

```python
import matplotlib.pyplot as plt

df = db.to_dataframe("""
    MATCH (n:Node)
    RETURN n.degree AS degree
    ORDER BY degree
""")

df['degree'].value_counts().sort_index().plot(
    kind='bar', figsize=(12, 4),
    title='Degree Distribution — ego-Facebook',
    xlabel='Degree', ylabel='Number of nodes',
    logy=True,
)
plt.tight_layout()
plt.savefig("degree_distribution.png", dpi=150)
```

### Exporting to NetworkX

`db.to_networkx()` exports the full graph (or a filtered subset) as a NetworkX graph. Node attributes include all properties plus `_labels`; edge attributes include all properties plus `_type`.

```python
import networkx as nx

# Export as directed DiGraph (default)
G = db.to_networkx()

# Export as undirected Graph — required for community detection algorithms
G_undirected = db.to_networkx(directed=False)

# Use node property as graph key instead of internal IDs
G = db.to_networkx(node_id_property="id")
pr = nx.pagerank(G)
# pr = {"107": 0.0094, "1684": 0.0012, ...}  ← user id values, not internal ints

# Filter to a specific label / relationship type
G_sub = db.to_networkx(node_label="Node", rel_type="CONNECTED_TO", directed=False)

# Community detection (requires undirected graph)
communities = nx.algorithms.community.louvain_communities(G_sub, seed=42)

print(nx.density(G_sub))
print(nx.average_clustering(G_sub))
```

### Writing Algorithm Results Back

`set_node_properties()` bulk-writes algorithm results directly to the storage layer in a single transaction — avoiding the ~79s UNWIND overhead on 4 000-node graphs.

```python
import networkx as nx

# Compute centrality and write back with internal IDs
G = db.to_networkx()
dc = nx.degree_centrality(G)
db.set_node_properties({nid: {"degree_centrality": score} for nid, score in dc.items()})

# Or use a user-facing property as the key (matches node_id_property on export)
G = db.to_networkx(node_id_property="id")
dc = nx.degree_centrality(G)
db.set_node_properties(
    {uid: {"degree_centrality": score} for uid, score in dc.items()},
    node_id_property="id",
)

# Results are immediately queryable in Cypher
top = db.execute("""
    MATCH (n:Node)
    RETURN n.id AS id, n.degree_centrality AS dc
    ORDER BY dc DESC LIMIT 10
""")
```

### Aggregated Statistics Table

```python
stats = db.to_dataframe("""
    MATCH (n:Node)
    WHERE n.degree IS NOT NULL
    RETURN
        count(n)              AS total_nodes,
        avg(n.degree)         AS avg_degree,
        min(n.degree)         AS min_degree,
        max(n.degree)         AS max_degree,
        stDev(n.degree)       AS std_degree
""")

print(stats.to_string(index=False))
#  total_nodes  avg_degree  min_degree  max_degree  std_degree
#         4039       43.69           1        1045       52.41
```

---

## Graph Algorithms with Analyst Verbs

GraphForge runs graph algorithms in Rust through five analyst-intent verbs and
returns Apache Arrow Tables. Python and Node are thin bindings over the same
native behavior; igraph and NetworkX are optional development parity oracles,
not runtime backends or fallbacks.

### Centrality

```python
from graphforge import GraphForge
forge = GraphForge("analysis")

# PageRank — write scores back to nodes, then query via Cypher
forge.rank("Node", by="pagerank", write_property="pagerank")
top = forge.execute("""
    MATCH (n)
    RETURN n.id AS node, n.pagerank AS score
    ORDER BY n.pagerank DESC
    LIMIT 10
""")

# Betweenness centrality — read-only Arrow result
bc_scores = forge.rank("Node", by="betweenness")
top = bc_scores.to_pandas().sort_values("score", ascending=False).head(10)
```

### Community Detection

```python
# Louvain community detection — write community ID to each node
forge.cluster("Node", by="louvain", write_property="community")

# Count nodes per community via Cypher
communities = forge.execute("""
    MATCH (n)
    RETURN n.community AS community, count(*) AS size
    ORDER BY size DESC
    LIMIT 10
""")
```

### Algorithm Catalog

The canonical [`by=` catalog](../architecture/algorithms.md) covers `rank`,
`cluster`, `similar`, `paths`, and `analyze`. Use `label` and `via` to select a
subgraph. Only `rank` and `cluster` accept opt-in `write_property`; all other
results are read-only.

---

## Summary

| Task | GraphForge |
|------|------------|
| Count nodes / edges | `MATCH (n) RETURN count(n)` |
| Degree of a node | `MATCH (n)-[r]-() RETURN count(r)` |
| Top-N hubs | `ORDER BY degree DESC LIMIT N` |
| Variable-length paths | `(a)-[:CONNECTED_TO*1..4]-(b)` |
| Triangle count | `forge.analyze(by="triangle_count")` |
| Temporal filter | `WHERE r.since >= '2024-01-01'` |
| Persist metric | `forge.rank("Node", by="pagerank", write_property="pr")` |

## Next Steps

- [openCypher Reference](../../reference/opencypher-compatibility.md) — full clause and function coverage
- [Cypher Query Guide](../../guide/cypher-guide.md) — patterns, aggregations, and list comprehensions
- [Dataset Catalogue](../../guide/datasets/overview.md) — all available built-in datasets
- [AI Agent Grounding](agent-grounding.md) — using GraphForge as an ontology backend
