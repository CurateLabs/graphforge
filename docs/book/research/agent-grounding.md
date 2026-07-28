# Research: AI Agent Grounding — Ontology-Backed Tool Selection

!!! note "Research note"
    Durable findings for **GraphForge v0.5.0**. Prefer the
    [agent grounding](../use-cases/agent-grounding.md) and
    [tool recall](../use-cases/agent-tool-recall.md) guides for present-tense examples.

**Scope:** Capability → tool ontologies, Cypher selection, `forge.find` recall,
agent-loop ergonomics.

---

## Executive summary

GraphForge works as an embedded tool registry for LLM agents: model capabilities,
tools, parameters, dependencies, and permissions as a graph, then select with
Cypher and/or `forge.find`.

**What holds up:**

- Capability hierarchy + `HAS_TOOL` / `REQUIRES` / `PRODUCES` patterns for planning
- Permission and deprecation filters expressed as ordinary graph structure
- `to_dicts()` / Arrow tables as the ergonomic boundary into agent loops
- Sub-millisecond to low-millisecond lookup latency at tens–hundreds of tools
- `forge.find` on tool name + description for paraphrase and CamelCase-adjacent recall

**Design around:**

- Exact-match Cypher on tool names is brittle when models invent casing or
  paraphrases — index descriptions and search with `forge.find`
- Split multi-statement create/link sequences when a single `execute()` would
  rebind the same variable incorrectly
- Prefer `MERGE` for shared ontology hubs (`DataModel`, `Capability`) so tools
  link to one node rather than duplicates

---

## Selection sketch

```python
from graphforge import GraphForge

forge = GraphForge("tools/")

# Structured filter
allowed = forge.execute("""
    MATCH (r:Role {name: $role})-[:MAY_USE]->(t:Tool)
    WHERE coalesce(t.deprecated, false) = false
    RETURN t.name AS name, t.description AS description
""", {"role": "analyst"})

# Fuzzy intent
candidates = forge.find("search inventory stock", label="Tool", limit=5)
```

No major agent framework ships graph-native tool planning with dependency and
permission traversal. GraphForge fills that gap as a local registry beside
LangGraph, LlamaIndex, or custom loops.

---

## Recommendations

1. Index tool **name and description** for `forge.find`.
2. Model dependencies as edges; plan with path queries, not flat ranking alone.
3. Persist the registry as `GraphForge("tools/")` so agents reopen without rebuild.
