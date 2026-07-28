# Research: Genealogy — Modelling Family History in a Graph

!!! note "Research note"
    Durable findings for **GraphForge v0.5.0**. Schema and query patterns; no
    dedicated use-case guide yet.

**Scope:** Person schema, path queries, `forge.find` on names, `forge.cluster`,
`neighbourhood()`.

---

## Executive summary

Genealogy is a natural graph problem. GraphForge handles the core patterns well:

**What works:**

- Variable-length paths for ancestors, descendants, and relationship chains
- Multiple labels (`:Person:Royal`) for sub-classification
- `forge.find` on name / surname / aliases for partial-name lookup
- `forge.cluster(..., by="louvain"|"components")` for family clusters and disconnected trees
- `neighbourhood(forge, canonical, hops=2)` for “immediate family” LLM context

**Design around:**

- Prefer `[*1..N]` with `ORDER BY length(path) LIMIT 1` instead of assuming
  `shortestPath()`
- Avoid hyphens in indexed name text when they confuse tokenizer / query syntax —
  store `"Brown Smith"` and query without exclusion-style punctuation
- Store historical spelling variants explicitly in an `aliases` property

---

## Schema sketch

```cypher
(:Person:Royal {
    canonical: "g0-adam",
    name: "Adam Smith",
    surname: "Smith",
    aliases: "Adamson Smythe",
    birth_year: 1890
})

(parent:Person)-[:PARENT_OF]->(child:Person)
(person:Person)-[:MARRIED_TO]->(spouse:Person)
```

Create both directions for undirected marriages. Optional event nodes (`:Birth`,
`:Marriage`) keep dates queryable without stuffing every fact onto edges.

---

## Queries

```cypher
-- Ancestors within N generations
MATCH (ancestor:Person)-[:PARENT_OF*1..4]->(d:Person {canonical: $canonical})
RETURN ancestor.name AS name, ancestor.birth_year AS year
ORDER BY year

-- Cluster membership after write-back
MATCH (n:Person)
RETURN n.cluster AS c, collect(n.name) AS names
ORDER BY n.cluster
```

```python
from graphforge.recipes import neighbourhood

forge.cluster("Person", by="louvain", write_property="cluster")
forge.cluster("Person", by="components", write_property="tree_id")
context = neighbourhood(forge, "g2-emma", hops=2, label="Person")
hits = forge.find("brown smith", label="Person", limit=10)
```

---

## Recommendations

1. Index `name`, `surname`, and `aliases` together for `forge.find`.
2. Use components for disconnected trees; Louvain for surname clusters inside a tree.
3. Parse GEDCOM externally and MERGE into GraphForge — there is no built-in GEDCOM loader.
