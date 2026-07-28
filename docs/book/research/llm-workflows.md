# Research: LLM-Powered Extraction → Storage → Retrieval

!!! note "Research note"
    Durable findings for **GraphForge v0.5.0**. Prefer the
    [use-case guide](../use-cases/llm-workflows.md) for present-tense examples.
    Context composition details live in [LLM context building](llm-context-building.md).

**Scope:** Extract → MERGE → query → synthesise loops; provenance; neighbourhood
context; `forge.find`.

---

## Executive summary

The core LLM workflow — extract structured triples, MERGE them with provenance,
retrieve neighbourhood or search context, then synthesise — works cleanly on the
v0.5.0 surface.

**What holds up:**

- `MERGE` + `ON CREATE SET` / `ON MATCH SET` for idempotent entity upserts
- Provenance properties and confidence updates in Cypher
- Variable-length neighbourhood queries and the `neighbourhood()` recipe
- `forge.find` for fuzzy entity lookup and hybrid text/vector recall
- Batching many MERGEs in one transaction when ingesting a document batch

**Design around:**

- Prefer `neighbourhood` + `forge.find` over hand-rolled shortest-path helpers
- Avoid f-string interpolation of labels/relationship types from LLM output —
  whitelist types or parameterize property values only
- Do not expect `ORDER BY` after `UNION` to sort the combined result unless you
  wrap the union in a subquery / outer query that the dialect supports

---

## Pipeline sketch

```python
from graphforge import GraphForge
from graphforge.recipes import neighbourhood

forge = GraphForge("knowledge/")

# 1) store extraction
forge.execute("""
    MERGE (e:Entity {canonical: $canonical})
    ON CREATE SET e.name = $name, e.source = $source, e.confidence = $confidence
    ON MATCH SET e.confirmations = coalesce(e.confirmations, 0) + 1
""", extraction)

# 2) build context for the next LLM call
ctx = neighbourhood(forge, extraction["canonical"], hops=2)
hits = forge.find(question, label="Entity", limit=5)
```

---

## Recommendations

1. Keep extraction schemas narrow (canonical id + name + provenance).
2. Build prompts from `neighbourhood` (structure) + `forge.find` (loose recall).
3. Persist projects under a directory path so reopen/recovery matches production usage.
