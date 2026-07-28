# Research: Analyst Verbs at Notebook Scale

!!! note "Research note"
    Durable findings for **GraphForge v0.5.0**. Prefer the
    [network analysis](../use-cases/network-analysis.md) use-case for present-tense
    examples.

**Scope:** `forge.rank` / `forge.cluster` timings and write-back on SNAP ego-facebook
(4,039 nodes, 88,234 edges).

---

## Executive summary

Compiled analyst verbs complete the usual notebook centrality and community
algorithms in well under a second on ego-facebook. Results match direct igraph
runs on the same projection. Opt-in `write_property` persists scores for later
Cypher without a per-node UNWIND loop.

| Algorithm | Verb | Typical time (s) | Notes |
| --- | --- | --- | --- |
| Triangle count | `forge.rank(..., by="triangles")` | ~0.07 | Full-graph triangles |
| Betweenness | `forge.rank(..., by="betweenness")` | ~0.16 | Normalized scores |
| Closeness | `forge.rank(..., by="closeness")` | ~0.11 | Connected graphs |
| PageRank | `forge.rank(..., by="pagerank")` | ~0.14 | Stream or write-back |
| Louvain | `forge.cluster(..., by="louvain")` | ~0.08 | Community labels |
| Connected components | `forge.cluster(..., by="components")` | ~0.06 | Tree / component IDs |
| Degree | `forge.rank(..., by="degree")` | ~0.06 | Fast baseline |
| Clustering coefficient | `forge.rank(..., by="clustering_coefficient")` | ~0.07 | Mean high on ego-facebook |

---

## Pattern

```python
from graphforge import GraphForge
from graphforge.datasets import load_dataset

forge = GraphForge()
load_dataset(forge, "snap-ego-facebook")

# Stream Arrow results
table = forge.rank("Node", by="pagerank")

# Or persist for Cypher follow-up
forge.rank("Node", by="pagerank", write_property="pagerank")
forge.cluster("Node", by="louvain", write_property="community")

top = forge.execute("""
    MATCH (n:Node)
    RETURN n.id AS id, n.pagerank AS score
    ORDER BY score DESC
    LIMIT 10
""")
```

Only `rank` and `cluster` accept opt-in `write_property`. Export to NetworkX or
igraph remains available when you need algorithms outside the shipped catalog —
see the [use-case guide](../use-cases/network-analysis.md#integration-with-networkx).

---

## Correctness

On ego-facebook, PageRank and betweenness top-10 rankings from `forge.rank` match
direct igraph rankings on `forge.to_igraph()` for the same undirected projection.
Treat NetworkX/igraph as oracles when validating new notebook pipelines.

---

## Recommendations

1. Prefer analyst verbs for the eight catalog algorithms above; reserve NX/igraph
   export for visualization and algorithms outside the catalog.
2. Use `write_property` when you will filter or join scores in Cypher later.
3. Restrict with `via=` / label filters when the graph is heterogeneous.
