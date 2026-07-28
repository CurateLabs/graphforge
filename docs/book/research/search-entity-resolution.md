# Research: Search and Entity Resolution

!!! note "Research note"
    Durable findings for **GraphForge v0.5.0**. Prefer
    [knowledge graph construction](../use-cases/knowledge-graph-construction.md)
    and [LLM workflows](../use-cases/llm-workflows.md) for present-tense examples.

**Scope:** `forge.find` text / vector / hybrid recall for half-remembered entities.

---

## Executive summary

Exact-match Cypher fails when an LLM emits `"alice c."` instead of
`"alice-chen"`. `forge.find` is the v0.5.0 answer for lexical, vector, and hybrid
lookup over node properties.

**Key findings:**

- Text search handles natural-language descriptions, paraphrases, stems, and case
  variants. It does not reliably handle single-character typos or abbreviations.
- Search latency stays low at notebook scale; index build cost dominates at 10K+
  nodes, not query time.
- Index **name and description** (and aliases) together — description text is what
  makes paraphrase recall work.
- Hybrid search needs real embeddings to beat text-only ranking; mock vectors do
  not demonstrate semantic quality.
- CamelCase tool names recall better when descriptions are indexed; store acronyms
  explicitly in aliases when needed.

---

## Pattern

```python
from graphforge import GraphForge

forge = GraphForge("entities/")

# After bulk ingest, rely on forge.find over indexed properties
table = forge.find("software engineer acme", label="Entity", limit=5)

# Hybrid when you already have query vectors
table = forge.find(
    "alice engineer",
    label="Entity",
    vector=query_vec,
    limit=10,
)
```

| Query style | Expectation |
| --- | --- |
| Exact / partial name tokens | Strong |
| Description paraphrase | Strong if description indexed |
| Stems (`engineering` → engineer) | Strong |
| Typos / abbreviations | Weak — use aliases or preprocessing |
| Meaning without keyword overlap | Needs vectors + hybrid |

---

## Recommendations

1. Always index descriptive text with names.
2. Prefer batch indexing after bulk MERGE ingest; incremental updates for live graphs.
3. Use `forge.find` as the primary entity-resolution entry point in LLM and agent flows.
