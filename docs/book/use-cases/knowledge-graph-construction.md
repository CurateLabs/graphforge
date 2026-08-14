# Knowledge Graph Construction

## Overview

Knowledge graphs give structure to information that starts life as unstructured text —
research papers, support tickets, product catalogues, codebases, or any corpus where
entities and their relationships matter. GraphForge is well suited to this pattern
because it is embedded (no server, no network hop), openCypher-compatible for the
shipped surface, and provides MERGE semantics (standalone nodes and
referenced-endpoint relationships; see
[MERGE status](../../reference/implementation-status/clauses.md#merge)) that make
incremental, idempotent graph construction natural.

### When to use this pattern

- **LLM extraction pipelines** — run a language model over documents to pull out
  entities and relationships, then load each batch into the graph.
- **Document analysis** — index papers, reports, or web pages; connect concepts
  across documents; answer "what is linked to what?" queries.
- **Ontology building** — define a class hierarchy, populate instances from data,
  iterate as understanding of the domain evolves.
- **Multi-source fusion** — merge extractions from several models or tools into a
  single coherent graph, tracking provenance per node and edge.

---

## Schema Design

Consistent naming makes Cypher queries readable and avoids accidental duplication.

### Label conventions

Use **PascalCase** for labels. One label per logical entity type is a good default;
add secondary labels when you need to tag provenance or status.

```
Person  Technology  Document  Concept  Organisation
```

### Property keys

Use **camelCase** for property keys. Reserve `name` as the primary human-readable
identifier on every node. Add `id` when you need a stable external key (e.g. a
Wikidata QID or DOI).

```
name  description  id  url  createdAt  confidence  source
```

### Relationship naming

Use **SCREAMING_SNAKE_CASE** for relationship types. Prefer verb phrases that read
naturally from left to right.

```
MENTIONS  RELATES_TO  DEPENDS_ON  PART_OF  WROTE  CITES  IMPLEMENTS
```

---

## LangChain Integration — `add_graph_documents()`

If you are using LangChain's extraction pipeline, GraphForge accepts
`GraphDocument` objects directly. No manual translation required:

```python
from graphforge import GraphForge
# from langchain_community.graphs.graph_document import GraphDocument, Node, Relationship

gf = GraphForge("knowledge_graph/")

# LangChain extraction output (or duck-typed equivalents)
# doc = GraphDocument(nodes=[...], relationships=[...], source=document)
# gf.add_graph_documents([doc], include_source=True)
```

`add_graph_documents()` is idempotent: calling it twice with the same data
produces no duplicate nodes or edges. Nodes are merged on `id` + label;
relationships are deduplicated on `(source, target, type, properties)`.

> **Label safety:** all node labels and relationship types are validated before
> any data is written. A single invalid identifier aborts the entire call with
> `ValueError` and leaves the graph unchanged.

Plain dicts are also accepted:

```python
gf.add_graph_documents([
    {
        "nodes": [
            {"id": "python",     "type": "Language",  "properties": {"description": "Dynamic language"}},
            {"id": "graphforge", "type": "Library",   "properties": {"description": "Embedded graph DB"}},
        ],
        "relationships": [
            {"source_id": "graphforge", "target_id": "python", "type": "RUNS_ON", "properties": {}},
        ],
    }
])
```

---

## Entity Extraction Pattern

The canonical Cypher MERGE pattern for loading extracted entities — use this when
you need fine-grained control over `ON CREATE SET` / `ON MATCH SET` semantics or
when you are not using LangChain.

> **CREATE vs MERGE:** `CREATE` always inserts a new node, even if an identical
> one already exists. Always use `MERGE` (or `add_graph_documents()`) for
> extraction pipelines — running the same extraction twice with `CREATE` will
> double your graph.

```python
from graphforge import GraphForge

forge = GraphForge("knowledge_graph/")   # Parquet-backed project

# --- raw extraction output (e.g. from an LLM) ---
entities = [
    {"type": "Technology", "name": "Python",    "description": "General-purpose language"},
    {"type": "Technology", "name": "GraphForge","description": "Embedded graph database"},
    {"type": "Technology", "name": "Python",    "description": "General-purpose language"},  # duplicate
]

# Load all entities idempotently
for entity in entities:
    db.execute(
        """
        MERGE (e:{label} {{name: $name}})
        ON CREATE SET e.description = $description,
                      e.createdAt   = datetime()
        """.format(label=entity["type"]),
        {"name": entity["name"], "description": entity["description"]},
    )

# Confirm: only two Technology nodes despite three inputs
results = db.execute("MATCH (t:Technology) RETURN t.name AS name ORDER BY name")
print([row["name"].value for row in results])
# ['GraphForge', 'Python']
```

MERGE on `name` ensures the second `Python` entry is silently skipped rather than
inserted again. `ON CREATE SET` populates properties only when the node is new, so
a second pass does not overwrite values that may have been updated manually.

---

## Relationship Extraction Pattern

Once entities are in the graph, link them with MERGE to avoid duplicate edges.
Always MATCH both endpoints first — if either is missing the relationship is skipped
rather than creating a dangling reference.

```python
relationships = [
    {"from": "GraphForge", "to": "Python", "type": "IMPLEMENTED_IN", "confidence": 0.98},
    {"from": "GraphForge", "to": "Python", "type": "IMPLEMENTED_IN", "confidence": 0.98},  # dup
]

for rel in relationships:
    db.execute(
        """
        MATCH (a {{name: $from_name}})
        MATCH (b {{name: $to_name}})
        MERGE (a)-[r:{rel_type}]->(b)
        ON CREATE SET r.confidence = $confidence,
                      r.createdAt  = datetime()
        """.format(rel_type=rel["type"]),
        {
            "from_name":  rel["from"],
            "to_name":    rel["to"],
            "confidence": rel["confidence"],
        },
    )

results = db.execute(
    "MATCH (:Technology)-[r:IMPLEMENTED_IN]->(:Technology) RETURN count(r) AS total"
)
print(results[0]["total"].value)   # 1 — not 2
```

---

## Iterative Refinement

Knowledge graphs are rarely built in a single pass. A typical pipeline runs multiple
extraction rounds, each adding detail or correcting earlier mistakes.

### Add provenance and source tracking

```python
# Tag each node with the document it came from
db.execute(
    """
    MATCH (e:Technology {name: $name})
    SET e.source     = $source,
        e.extractedAt = datetime()
    """,
    {"name": "Python", "source": "doc:arxiv:2301.00001"},
)
```

### Update confidence scores

When a second extraction confirms an existing relationship, raise its confidence:

```python
db.execute(
    """
    MATCH (a {name: $from_name})-[r:IMPLEMENTED_IN]->(b {name: $to_name})
    SET r.confidence    = CASE WHEN r.confidence < $new_conf
                               THEN $new_conf
                               ELSE r.confidence
                          END,
        r.confirmations = coalesce(r.confirmations, 1) + 1
    """,
    {"from_name": "GraphForge", "to_name": "Python", "new_conf": 0.99},
)
```

### Merge duplicate entities

When two nodes represent the same real-world entity (detected by a fuzzy match or
a human review step), redirect all edges to the canonical node and delete the alias.

```python
# Suppose 'GraphForge DB' and 'GraphForge' are the same thing
db.execute(
    """
    MATCH (alias:Technology {name: 'GraphForge DB'})
    MATCH (canon:Technology {name: 'GraphForge'})
    WITH alias, canon
    MATCH (alias)-[r]->(target)
    MERGE (canon)-[:{rel_type} {{confidence: r.confidence}}]->(target)
    WITH alias
    DETACH DELETE alias
    """.replace("{rel_type}", "IMPLEMENTED_IN")  # repeat per rel type as needed
)
```

---

## Querying Patterns

### Find all entities of a type

```python
results = db.execute(
    """
    MATCH (t:Technology)
    RETURN t.name AS name, t.description AS description
    ORDER BY t.name
    """
)
for row in results:
    print(f"{row['name'].value}: {row['description'].value}")
```

### Traverse relationships from a starting node

```python
results = db.execute(
    """
    MATCH (start:Technology {name: $name})-[r]->(related)
    RETURN type(r) AS rel, related.name AS target, r.confidence AS conf
    ORDER BY conf DESC
    """,
    {"name": "GraphForge"},
)
for row in results:
    print(f"-[{row['rel'].value}]-> {row['target'].value} ({row['conf'].value:.2f})")
```

### Find connections between two concepts (shortest path)

`shortestPath()` raises `NotImplementedError` — use a variable-length pattern with
`ORDER BY` and `LIMIT 1` as a BFS equivalent:

```python
results = db.execute(
    """
    MATCH path = (a:Technology {name: $from_name})-[*1..6]-(b:Technology {name: $to_name})
    RETURN length(path) AS hops
    ORDER BY hops ASC LIMIT 1
    """,
    {"from_name": "Python", "to_name": "SQLite"},
)
if results:
    print(f"Connected by {results[0]['hops'].value} hops")
```

> **Note:** This enumerates all paths up to `max_hops` before returning the
> shortest. Keep `max_hops ≤ 3` for graphs above ~1 K nodes.

### Aggregate by relationship type

```python
results = db.execute(
    """
    MATCH ()-[r]->()
    RETURN type(r) AS rel_type, count(r) AS total
    ORDER BY total DESC
    """
)
for row in results:
    print(f"{row['rel_type'].value}: {row['total'].value}")
```

---

## Provenance and Confidence

Store extraction metadata directly on nodes and edges so that every fact in the
graph can be traced back to its origin.

**Recommended provenance properties**

| Property | Type | Description |
|------------------|----------|-----------------------------------------------|
| `source` | String | Document ID or URL |
| `extractedAt` | Datetime | When the fact was extracted |
| `extractedBy` | String | Model or tool name (e.g. `"gpt-4o"`) |
| `confidence` | Float | Model confidence or human review score [0, 1] |
| `confirmations` | Integer | How many independent extractions agree |

```python
db.execute(
    """
    MERGE (a:Technology {name: $from_name})
      ON CREATE SET a.source = $source, a.extractedBy = $model
    MERGE (b:Technology {name: $to_name})
      ON CREATE SET b.source = $source, b.extractedBy = $model
    MERGE (a)-[r:DEPENDS_ON]->(b)
      ON CREATE SET r.confidence   = $confidence,
                    r.source       = $source,
                    r.extractedAt  = datetime(),
                    r.extractedBy  = $model,
                    r.confirmations = 1
    """,
    {
        "from_name":  "graphforge",
        "to_name":    "lark",
        "source":     "doc:github:graphforge/pyproject.toml",
        "model":      "gpt-4o",
        "confidence": 0.97,
    },
)
```

Query to surface low-confidence facts for human review:

```python
results = db.execute(
    """
    MATCH (a)-[r]->(b)
    WHERE r.confidence < 0.7
    RETURN a.name AS from_node, type(r) AS rel, b.name AS to_node,
           r.confidence AS conf, r.source AS src
    ORDER BY conf ASC
    LIMIT 20
    """
)
for row in results:
    print(
        f"{row['from_node'].value} -[{row['rel'].value}]-> {row['to_node'].value}"
        f"  conf={row['conf'].value:.2f}  src={row['src'].value}"
    )
```

---

## Exporting a Focused Subgraph

When sharing results or feeding a downstream model, extract a coherent slice of the
graph rather than dumping everything.

```python
# Pull all nodes and edges within 2 hops of a seed concept
results = db.execute(
    """
    MATCH path = (seed:Technology {name: $seed})-[*1..2]-(related)
    WITH nodes(path)  AS ns,
         relationships(path) AS rs
    UNWIND ns AS n
    WITH DISTINCT n, rs
    RETURN n.name AS node, n.description AS desc,
           labels(n)[0] AS label
    ORDER BY label, node
    """,
    {"seed": "GraphForge"},
)

subgraph_nodes = [
    {"name": row["node"].value, "label": row["label"].value, "desc": row["desc"].value}
    for row in results
]
```

---

## Complete Worked Example

Extract Python ecosystem entities from a mock LLM output, build a knowledge graph,
then query it.

```python
from graphforge import GraphForge

# -----------------------------------------------------------------
# 1. Mock LLM extraction output
# -----------------------------------------------------------------
llm_output = {
    "entities": [
        {"label": "Language",  "name": "Python",       "description": "Dynamic, interpreted language"},
        {"label": "Framework", "name": "FastAPI",       "description": "Async web framework"},
        {"label": "Framework", "name": "Django",        "description": "Batteries-included web framework"},
        {"label": "Library",   "name": "Pydantic",      "description": "Data validation using type hints"},
        {"label": "Library",   "name": "GraphForge",    "description": "Embedded graph database"},
        {"label": "Library",   "name": "SQLAlchemy",    "description": "SQL toolkit and ORM"},
        {"label": "Library",   "name": "Lark",          "description": "Parsing toolkit for Python"},
    ],
    "relationships": [
        {"from": "FastAPI",    "to": "Python",      "type": "RUNS_ON",    "confidence": 1.0},
        {"from": "Django",     "to": "Python",      "type": "RUNS_ON",    "confidence": 1.0},
        {"from": "GraphForge", "to": "Python",      "type": "RUNS_ON",    "confidence": 1.0},
        {"from": "FastAPI",    "to": "Pydantic",    "type": "DEPENDS_ON", "confidence": 0.98},
        {"from": "Django",     "to": "SQLAlchemy",  "type": "DEPENDS_ON", "confidence": 0.85},
        {"from": "GraphForge", "to": "Lark",        "type": "DEPENDS_ON", "confidence": 0.99},
        {"from": "GraphForge", "to": "Pydantic",    "type": "DEPENDS_ON", "confidence": 0.99},
    ],
}

# -----------------------------------------------------------------
# 2. Build the graph
# -----------------------------------------------------------------
db = GraphForge()   # in-memory for this example; use GraphForge("kg/") to persist

SOURCE  = "doc:example:python-ecosystem"
MODEL   = "gpt-4o-mock"

# Each execute() below is an atomic publication. Build one
# publish_composite_transaction() request instead when the entire import must
# publish as one committed generation.
# Load entities
for ent in llm_output["entities"]:
    db.execute(
        """
        MERGE (e:{label} {{name: $name}})
        ON CREATE SET e.description  = $description,
                      e.source       = $source,
                      e.extractedBy  = $model,
                      e.extractedAt  = datetime()
        """.format(label=ent["label"]),
        {
            "name":        ent["name"],
            "description": ent["description"],
            "source":      SOURCE,
            "model":       MODEL,
        },
    )

# Load relationships
for rel in llm_output["relationships"]:
    db.execute(
        """
        MATCH (a {{name: $from_name}})
        MATCH (b {{name: $to_name}})
        MERGE (a)-[r:{rel_type}]->(b)
        ON CREATE SET r.confidence   = $confidence,
                      r.source       = $source,
                      r.extractedAt  = datetime(),
                      r.extractedBy  = $model,
                      r.confirmations = 1
        """.format(rel_type=rel["type"]),
        {
            "from_name":  rel["from"],
            "to_name":    rel["to"],
            "confidence": rel["confidence"],
            "source":     SOURCE,
            "model":      MODEL,
        },
    )

# -----------------------------------------------------------------
# 3. Query the graph
# -----------------------------------------------------------------

# Which frameworks and libraries run on Python?
print("=== Runs on Python ===")
rows = db.execute(
    """
    MATCH (thing)-[:RUNS_ON]->(lang:Language {name: 'Python'})
    RETURN labels(thing)[0] AS kind, thing.name AS name
    ORDER BY kind, name
    """
)
for row in rows:
    print(f"  [{row['kind'].value}] {row['name'].value}")

# What does GraphForge depend on?
print("\n=== GraphForge dependencies ===")
rows = db.execute(
    """
    MATCH (gf:Library {name: 'GraphForge'})-[r:DEPENDS_ON]->(dep)
    RETURN dep.name AS dependency, r.confidence AS conf
    ORDER BY conf DESC
    """
)
for row in rows:
    print(f"  {row['dependency'].value}  (conf={row['conf'].value:.2f})")

# Which libraries share a common dependency?
print("\n=== Libraries sharing a dependency ===")
rows = db.execute(
    """
    MATCH (a)-[:DEPENDS_ON]->(shared)<-[:DEPENDS_ON]-(b)
    WHERE a.name < b.name
    RETURN a.name AS lib_a, b.name AS lib_b, shared.name AS via
    ORDER BY lib_a, lib_b
    """
)
for row in rows:
    print(f"  {row['lib_a'].value} and {row['lib_b'].value} both depend on {row['via'].value}")

# Summarise by label
print("\n=== Node counts by label ===")
rows = db.execute(
    """
    MATCH (n)
    RETURN labels(n)[0] AS label, count(n) AS total
    ORDER BY total DESC
    """
)
for row in rows:
    print(f"  {row['label'].value}: {row['total'].value}")
```

Expected output:

```
=== Runs on Python ===
  [Framework] Django
  [Framework] FastAPI
  [Library] GraphForge

=== GraphForge dependencies ===
  Lark    (conf=0.99)
  Pydantic  (conf=0.99)

=== Libraries sharing a dependency ===
  FastAPI and GraphForge both depend on Pydantic

=== Node counts by label ===
  Library: 4
  Framework: 2
  Language: 1
```

---

## Candidate Deduplication with `forge.find()`

`MERGE` handles exact canonical keys. For surface-form variation, store the aliases you
already know in one string property and use GraphForge's native BM25 search to retrieve
candidates before applying your domain's identity rule. Text search is candidate retrieval,
not an automatic assertion that two entities are the same.

### Index Known Names and Aliases

```python
from graphforge import GraphForge

forge = GraphForge("knowledge_graph")

countries = [
    {"name": "United States", "search_text": "United States US USA"},
    {"name": "France", "search_text": "France French Republic"},
]
for country in countries:
    forge.add_node("Country", **country)

# The receipt proves the complete artifact and exact committed source identity.
receipt = forge.index("Country", properties=["name", "search_text"])
assert receipt["state"] == "current"
assert receipt["artifact_source_fingerprint"] == receipt["source_fingerprint"]
```

### Retrieve Candidates

`find()` returns a `pyarrow.Table`. Every row has stable `node_uuid`, finite `score`,
closed `matched_on`, and the indexed graph properties—never a numeric storage ID or a
bespoke hit wrapper.

```python
def entity_candidates(forge: GraphForge, query: str, label: str) -> list[dict]:
    table = forge.find(query, label=label, limit=3)
    return table.select(
        ["node_uuid", "name", "search_text", "score", "matched_on"]
    ).to_pylist()


for candidate in entity_candidates(forge, "US", "Country"):
    print(candidate["name"], candidate["score"], candidate["node_uuid"].hex())
```

Compare the returned canonical name, external identifier, or alias set according to your
application's policy. If no candidate satisfies that policy, add the new node. A later
`find()` detects the graph mutation and coordinates one fresh rebuild; bulk loaders can
call `index(..., rebuild=True)` once after the batch when they want that rebuild eagerly.
`inspect_text_index(...)` reports `stale` before that refresh and `current` only after the
replacement and its containing project generation are atomically published.

---

## Next Steps

- Add a second extraction pass over a different document and observe MERGE preventing
  duplicates while `confirmations` counts accumulate.
- Introduce a `Document` node and connect every extracted entity to it with a
  `MENTIONED_IN` relationship for full document-level provenance.
- Persist the graph to disk (`GraphForge("kg/")`) and reload it across sessions
  without re-running extraction.
- Combine this pattern with the [AI Agent Grounding](agent-grounding.md) guide to
  build an agent that reasons over a dynamically constructed knowledge graph.

## Resources

- [GraphForge Documentation](../../index.md)
- [openCypher Reference](../../reference/opencypher-compatibility.md)
- [API Reference](../../reference/api.md)
- [Agent Grounding use case](agent-grounding.md)
