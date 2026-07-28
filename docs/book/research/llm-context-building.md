# Research: Building Context for an LLM Call

!!! note "Research note"
    Durable findings for **GraphForge v0.5.0**. Prefer the
    [LLM workflows](../use-cases/llm-workflows.md) use-case for present-tense examples.

**Scope:** `graphforge.recipes.neighbourhood()`, `forge.find`, token budget,
composition patterns.

---

## Executive summary

Given an entity or question, pull what the graph knows and hand it to the LLM.
Two complementary tools cover most cases:

1. `neighbourhood(forge, canonical, hops=2)` — structured n-hop context as plain dicts
2. `forge.find(...)` — lexical / hybrid recall for nodes not on the traversal frontier

**Key findings:**

- `hops=2` is the practical default: enough structure for most KG questions, typically
  a few thousand tokens on hub-and-spoke graphs
- `hops=3` rarely adds nodes in leaf-heavy schemas
- Neighbourhood latency stays in the low tens of milliseconds at 10K-node sparse graphs
- Combine neighbourhood (edges) with `forge.find` (keywords / vectors) and dedupe by
  `canonical`

---

## Composition pattern

```python
from graphforge.recipes import neighbourhood

def build_context(forge, canonical: str, question: str | None = None) -> str:
    graph_context = neighbourhood(forge, canonical, hops=2)
    search_context = []
    if question:
        table = forge.find(question, label="Entity", limit=5)
        search_context = table.to_pylist()

    seen = {canonical}
    combined = []
    for row in graph_context + search_context:
        c = row.get("canonical", "")
        if c and c not in seen:
            seen.add(c)
            combined.append(row)
    return "\n".join(str(row) for row in combined)
```

| Approach | Best for | Misses |
| --- | --- | --- |
| `neighbourhood(hops=1)` | Direct facts | Indirect relations |
| `neighbourhood(hops=2)` | Most KG Q&A | Disconnected but relevant nodes |
| `forge.find(question)` | Natural-language recall | Pure structure without keyword overlap |
| Combined | Comprehensive prompts | Overlong contexts — trim properties |

Re-enter the graph with the `canonical` property from neighbourhood dicts; the
recipe returns plain Python, ready for prompt serialization.
