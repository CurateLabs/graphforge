# Research: Network Analysis Notebook Workflow

!!! note "Research note"
    Durable findings for **GraphForge v0.5.0**. Prefer the
    [use-case guide](../use-cases/network-analysis.md) for present-tense examples.
    For algorithm timings, see [analyst verbs at scale](analyst-verbs-at-scale.md).

**Scope:** SNAP datasets, Cypher metrics, pandas/NetworkX bridge, analyst verbs.

---

## Executive summary

Notebook network analysis works best when GraphForge owns storage and Cypher,
analyst verbs own the common centrality/community algorithms, and NetworkX or
igraph are used as optional visualization / oracle layers.

**What holds up:**

- `load_dataset(..., "snap-ego-facebook")` loads `:Node` / `:CONNECTED_TO` — match
  those labels in Cypher (not invented `:Person` / `:FRIEND_OF` aliases)
- Degree, triangle, and path queries in openCypher for exploratory metrics
- `forge.rank` / `forge.cluster` with optional `write_property` for persistence
- `to_networkx()` / `to_igraph()` / `to_dataframe()` for bridges
- Parquet project directories for shareable notebook state

**Friction to design around:**

- Exported NetworkX node IDs are internal integers unless you pass
  `node_id_property=...` — plan write-back with an explicit id property
- Prefer analyst verbs over per-node Cypher UNWIND write-back for large score maps
- Align every snippet’s labels with the dataset schema before debugging the engine

---

## Label alignment

```python
from graphforge import GraphForge
from graphforge.datasets import load_dataset

forge = GraphForge("facebook/")
load_dataset(forge, "snap-ego-facebook")

# Dataset schema
forge.execute("MATCH (n) RETURN DISTINCT labels(n) AS labels LIMIT 5")
forge.execute("MATCH ()-[r]->() RETURN DISTINCT type(r) AS t")
```

---

## Recommendations

1. Discover labels once after `load_dataset`; never hard-code a different schema.
2. Use `forge.rank` / `forge.cluster` for catalog algorithms; see
   [analyst verbs at scale](analyst-verbs-at-scale.md).
3. Pass `node_id_property` when round-tripping algorithm results through NetworkX.
4. Persist notebooks with `GraphForge("analysis/")`, not ad-hoc pickle files.
