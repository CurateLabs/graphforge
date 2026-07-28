# Research: Knowledge Graph Construction

!!! note "Research note"
    Durable findings for **GraphForge v0.5.0**. Prefer the
    [use-case guide](../use-cases/knowledge-graph-construction.md) for
    present-tense examples.

**Scope:** MERGE-based entity/relationship loading, provenance, confidence
updates, deduplication, export helpers.

---

## Executive summary

GraphForge’s MERGE-based construction pattern is solid for incremental knowledge
graphs: idempotent entity and relationship loading, provenance properties,
two-pass confidence updates, and the export suite (`to_dicts`, `to_networkx`,
`to_igraph`, `to_dataframe`) work together in a small multi-type pipeline.

**What holds up:**

- `MERGE` with `ON CREATE SET` / `ON MATCH SET` for idempotent loads
- Provenance fields (`source`, `extracted_at`, `confidence`) as ordinary properties
- Alias merge via edge migration + `DETACH DELETE`
- Bulk create helpers for raw node/relationship ingest when Cypher MERGE is not required
- `forge.find()` for candidate lookup during entity resolution (see
  [search & entity resolution](search-entity-resolution.md))

**Design around:**

- Prefer variable-length paths (`[*1..N]`) with `ORDER BY length(path) LIMIT 1`
  when you need a shortest-path style answer; do not assume `shortestPath()` is
  available in every openCypher dialect path
- Keep label and relationship-type names stable; discover them with Cypher
  `DISTINCT labels(n)` / `type(r)` rather than expecting a separate schema API

---

## Recommended load shape

```python
from graphforge import GraphForge

forge = GraphForge("kg/")

forge.execute("""
    MERGE (p:Person {name: $name})
    ON CREATE SET p.source = $source, p.confidence = $confidence, p.created_at = datetime()
    ON MATCH SET p.confirmations = coalesce(p.confirmations, 0) + 1
""", {"name": "Ada Lovelace", "source": "extract-v1", "confidence": 0.9})
```

After bulk MERGE passes, index text properties once and resolve fuzzy duplicates
with `forge.find` before creating another node — see the use-case
[deduplication section](../use-cases/knowledge-graph-construction.md#candidate-deduplication-with-forgefind).

---

## Recommendations

1. Treat MERGE + provenance properties as the default construction idiom.
2. Batch text indexing after ingest; search with `forge.find`, not exact-name-only Cypher.
3. Export to pandas/NetworkX only at analysis boundaries; keep the system of record
   in the Parquet project.
