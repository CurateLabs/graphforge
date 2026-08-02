# LLM-Powered Workflows

## Overview

Large language models are good at extracting structure from text, but they have no memory between calls and no native way to query what they already know. GraphForge bridges that gap: the LLM produces structured data, GraphForge stores and indexes it, and queries against the graph feed fresh, precise context back into the next LLM call.

The pattern looks like this:

```
Text corpus
     │
     ▼
┌──────────────────────┐
│  LLM extraction      │  entity + relationship extraction
└──────────┬───────────┘
           │  structured data
           ▼
┌──────────────────────┐
│  GraphForge          │  MERGE, SET, CREATE
│  (persistent graph)  │◄── provenance metadata
└──────────┬───────────┘
           │  Cypher queries
           ▼
┌──────────────────────┐
│  Context builder     │  n-hop neighbourhood, aggregations
└──────────┬───────────┘
           │  context string
           ▼
┌──────────────────────┐
│  LLM synthesis       │  Q&A, summarisation, refinement
└──────────────────────┘
```

GraphForge is a good fit here because it runs embedded in the same Python process as your LLM library, requires zero infrastructure, and speaks full openCypher so queries stay readable.

---

## 1. Schema: Documents, Entities, and Relationships

A minimal schema that covers most extraction pipelines:

- **Document** — the source text (URL, file path, or an internal id)
- **Entity** — a named thing: person, organisation, concept, product, …
- **Relationship** — a typed, directed edge between two entities, with provenance

```python
from graphforge import GraphForge

db = GraphForge("knowledge_graph")   # persistent Parquet project; use GraphForge() in-memory

# ── Documents ──────────────────────────────────────────────────────────────────
db.execute("""
    CREATE (:Document {
        id:       'doc-001',
        title:    'GraphForge release notes',
        source:   'https://example.com/release-notes',
        ingested: datetime()
    })
""")

# ── Entities ───────────────────────────────────────────────────────────────────
# Use MERGE so that re-running extraction never creates duplicates.
db.execute("""
    MERGE (:Entity:Person  {name: 'Alice Chen',    canonical: 'alice-chen'})
    MERGE (:Entity:Project {name: 'GraphForge',    canonical: 'graphforge'})
    MERGE (:Entity:Org     {name: 'DecisionNerd',  canonical: 'decisionnerd'})
""")

# ── Relationships between entities ─────────────────────────────────────────────
db.execute("""
    MATCH  (a:Person  {canonical: 'alice-chen'}),
           (p:Project {canonical: 'graphforge'})
    MERGE  (a)-[:CONTRIBUTES_TO {since: '2024-01', confidence: 0.92}]->(p)
""")

# ── Provenance: which document mentioned this fact ─────────────────────────────
db.execute("""
    MATCH (doc:Document {id: 'doc-001'}),
          (a:Person     {canonical: 'alice-chen'}),
          (p:Project    {canonical: 'graphforge'})
    MERGE (doc)-[:MENTIONS]->(a)
    MERGE (doc)-[:MENTIONS]->(p)
""")
```

---

## 2. Entity Deduplication with MERGE

When you process many documents the same real-world entity will appear under slightly different surface forms. The safest strategy is to normalise to a canonical key before writing and always use `MERGE` on that key.

> **Label safety:** openCypher cannot parameterise labels or relationship types — they
> must be interpolated as string literals. When these values come from LLM extraction
> output, validate them before interpolation to prevent parse errors from unexpected
> characters.

```python
import re

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def safe_identifier(value: str) -> str:
    """Validate a label or relationship type before Cypher interpolation."""
    if not _SAFE_IDENTIFIER.match(value):
        raise ValueError(f"Unsafe Cypher identifier: {value!r}")
    return value


def store_entity(db, label: str, name: str, canonical: str, **extra):
    """Idempotent upsert — safe to call repeatedly."""
    db.execute(
        f"MERGE (e:Entity:{safe_identifier(label)} {{canonical: $canonical}}) "
        "ON CREATE SET e.name = $name, e.created = datetime() "
        "ON MATCH  SET e.name = $name "   # keep name fresh in case of corrections
        + (", ".join(f"e.{k} = ${k}" for k in extra) if extra else ""),
        {"canonical": canonical, "name": name, **extra},
    )


def store_relationship(db, src_canonical: str, dst_canonical: str,
                       rel_type: str, confidence: float, model: str):
    """Store a typed relationship with provenance."""
    db.execute(
        "MATCH (src:Entity {canonical: $src}), (dst:Entity {canonical: $dst}) "
        f"MERGE (src)-[r:{safe_identifier(rel_type)}]->(dst) "
        "ON CREATE SET r.confidence = $conf, r.model = $model, r.created = datetime() "
        "ON MATCH  SET r.confidence = $conf, r.model = $model, r.updated = datetime()",
        {"src": src_canonical, "dst": dst_canonical, "conf": confidence, "model": model},
    )
```

Running the same extraction twice over the same document will not create duplicate nodes or edges.

---

## 3. Tracking Extraction Provenance

Every fact in your graph should record where it came from and how reliable it is:

```python
# Attach provenance directly to the relationship
db.execute("""
    MATCH (src:Entity {canonical: $src}),
          (dst:Entity {canonical: $dst})
    MERGE (src)-[r:RELATED_TO]->(dst)
    ON CREATE SET
        r.confidence  = $conf,
        r.model       = $model,
        r.extracted   = datetime(),
        r.source_doc  = $doc_id
    ON MATCH SET
        r.confidence  = $conf,
        r.model       = $model,
        r.updated     = datetime()
""", {
    "src":    "graphforge",
    "dst":    "decisionnerd",
    "conf":   0.88,
    "model":  "gpt-4o-mini",
    "doc_id": "doc-001",
})
```

Query provenance later to audit the graph or filter by quality:

```python
rows = db.to_dicts("""
    MATCH (src:Entity)-[r]->(dst:Entity)
    WHERE r.confidence >= 0.85
    RETURN src.name AS subject,
           type(r)   AS predicate,
           dst.name  AS object,
           r.model   AS model,
           r.confidence AS conf
    ORDER BY r.confidence DESC
    LIMIT 20
""")

for row in rows:
    print(
        f"{row['subject']!r:25s}  "
        f"--{row['predicate']}-->  "
        f"{row['object']!r:25s}  "
        f"[{row['conf']:.2f} via {row['model']}]"
    )
```

---

## 4. Hybrid Retrieval: n-Hop Neighbourhood

The most useful context for an LLM is often the subgraph *around* an entity — its immediate neighbours and their neighbours. The Cypher variable-length path syntax handles this directly.

```python
def get_neighbourhood(db, canonical: str, hops: int = 2) -> list[dict]:
    """Return all entities reachable within `hops` steps from `canonical`."""
    return db.to_dicts(
        "MATCH (seed:Entity {canonical: $canonical})"
        "-[*1.." + str(hops) + "]-(neighbour:Entity) "
        "WHERE neighbour.canonical <> $canonical "
        "RETURN DISTINCT "
        "    neighbour.name AS name, "
        "    neighbour.canonical AS canonical, "
        "    labels(neighbour)  AS labels",
        {"canonical": canonical},
    )


neighbours = get_neighbourhood(db, "graphforge", hops=2)
# [{'name': 'Alice Chen', 'canonical': 'alice-chen', 'labels': ['Entity', 'Person']}, ...]
```

You can also find the *shortest path* between two entities — useful for explaining
connections. `shortestPath()` is not yet supported; use this BFS workaround for
graphs up to ~2K nodes:

```python
rows = db.execute(
    "MATCH path = (a:Entity {canonical: $src})-[*1..5]-(b:Entity {canonical: $dst}) "
    "RETURN length(path) AS hops "
    "ORDER BY hops ASC LIMIT 1",
    {"src": "alice-chen", "dst": "decisionnerd"},
)
if rows:
    print(f"Connected by {rows[0]['hops'].value} hops")
```

> **Note:** The BFS workaround enumerates all paths up to `max_hops` before returning
> the shortest. On dense graphs (high-degree nodes) with large `max_hops`, this can be
> slow. Use `max_hops ≤ 3` for graphs above 1K nodes.

---

## 5. Building a Q&A Context String

Given a natural-language question, extract the key entity, pull its neighbourhood from the graph, then hand the resulting context to the LLM.

```python
def build_context_for_question(db, entity_canonical: str, max_facts: int = 30) -> str:
    """
    Build a compact context string from the graph for use in an LLM prompt.
    Returns a bullet-list of (subject, predicate, object) triples.
    """
    # Fetch outbound and inbound edges via UNION.
    # ORDER BY after UNION only sorts the second branch — sort globally in Python.
    rows = db.to_dicts(
        "MATCH (src:Entity {canonical: $canonical})-[r]->(dst:Entity) "
        "RETURN src.name AS subject, type(r) AS predicate, dst.name AS object, "
        "       r.confidence AS confidence "
        "UNION "
        "MATCH (src:Entity)-[r]->(dst:Entity {canonical: $canonical}) "
        "RETURN src.name AS subject, type(r) AS predicate, dst.name AS object, "
        "       r.confidence AS confidence",
        {"canonical": entity_canonical},
    )

    if not rows:
        return "No relevant facts found in the knowledge graph."

    # Global sort by confidence, then cap at max_facts
    rows = sorted(rows, key=lambda r: r["confidence"], reverse=True)[:max_facts]

    lines = ["Relevant facts from the knowledge graph:\n"]
    for row in rows:
        subj = row["subject"]
        pred = row["predicate"].replace("_", " ").lower()
        obj  = row["object"]
        conf = row["confidence"]
        lines.append(f"  - {subj} {pred} {obj}  (confidence: {conf:.0%})")

    return "\n".join(lines)


context = build_context_for_question(db, "graphforge")

# Pass to LLM
prompt = f"""
You are a helpful assistant. Use the facts below to answer the question.

{context}

Question: Who contributes to GraphForge and what organisation are they from?
"""
# response = llm.complete(prompt)
```

---

## 6. Iterative Knowledge Refinement

After the LLM processes graph data you can write its assessments back. This lets you build a feedback loop where the graph becomes progressively more accurate.

```python
def update_confidence(db, src_canonical: str, dst_canonical: str,
                      rel_type: str, new_confidence: float, reviewer: str):
    """Update an edge's confidence after LLM or human review."""
    db.execute(
        f"MATCH (src:Entity {{canonical: $src}})"
        f"-[r:{safe_identifier(rel_type)}]->(dst:Entity {{canonical: $dst}}) "
        "SET r.confidence = $conf, r.reviewed_by = $reviewer, r.reviewed_at = datetime()",
        {
            "src":      src_canonical,
            "dst":      dst_canonical,
            "conf":     new_confidence,
            "reviewer": reviewer,
        },
    )


def mark_entity_verified(db, canonical: str, verified_by: str):
    """Flag an entity as verified after manual or LLM review."""
    db.execute(
        "MATCH (e:Entity {canonical: $canonical}) "
        "SET e.verified = true, e.verified_by = $by, e.verified_at = datetime()",
        {"canonical": canonical, "by": verified_by},
    )


# Demote a low-quality edge found by LLM review
update_confidence(db, "alice-chen", "graphforge", "CONTRIBUTES_TO",
                  new_confidence=0.45, reviewer="gpt-4o")

# Promote a confirmed entity
mark_entity_verified(db, "graphforge", verified_by="human")
```

You can then filter your context queries to include only high-quality, verified data when stakes are high:

```python
rows = db.execute("""
    MATCH (src:Entity)-[r]->(dst:Entity)
    WHERE r.confidence >= 0.8
      AND src.verified = true
    RETURN src.name AS subject, type(r) AS predicate, dst.name AS object
    ORDER BY r.confidence DESC
""")
```

---

## 7. Complete Mini-Pipeline

The example below simulates an end-to-end extraction pipeline without calling a real LLM API. Swap `mock_extract` for your actual LLM client.

> **Atomic publication:** Every `execute()` write publishes atomically. If the whole
> ingestion must become visible as one committed generation, construct one
> `publish_composite_transaction()` request. GraphForge does not expose generic
> `begin()`, `commit()`, or `rollback()` methods.

```python
from __future__ import annotations
import re
from graphforge import GraphForge

_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def safe_identifier(value: str) -> str:
    if not _SAFE_IDENTIFIER.match(value):
        raise ValueError(f"Unsafe Cypher identifier: {value!r}")
    return value


# ── Mock extraction function ───────────────────────────────────────────────────
# Replace this with a real LLM call (OpenAI, Anthropic, etc.)

def mock_extract(text: str) -> dict:
    """
    Simulates LLM structured extraction.
    Returns {'entities': [...], 'relationships': [...]}.
    """
    if "Alice" in text and "GraphForge" in text:
        return {
            "entities": [
                {"name": "Alice Chen",   "label": "Person",  "canonical": "alice-chen"},
                {"name": "GraphForge",   "label": "Project", "canonical": "graphforge"},
                {"name": "DecisionNerd", "label": "Org",     "canonical": "decisionnerd"},
            ],
            "relationships": [
                {"src": "alice-chen",   "dst": "graphforge",   "type": "CONTRIBUTES_TO", "confidence": 0.94},
                {"src": "alice-chen",   "dst": "decisionnerd", "type": "WORKS_AT",        "confidence": 0.89},
                {"src": "decisionnerd", "dst": "graphforge",   "type": "MAINTAINS",       "confidence": 0.97},
            ],
        }
    if "Bob" in text:
        return {
            "entities": [
                {"name": "Bob Lim",    "label": "Person",  "canonical": "bob-lim"},
                {"name": "GraphForge", "label": "Project", "canonical": "graphforge"},
            ],
            "relationships": [
                {"src": "bob-lim", "dst": "graphforge", "type": "USES", "confidence": 0.78},
            ],
        }
    return {"entities": [], "relationships": []}


# ── Pipeline ───────────────────────────────────────────────────────────────────

def ingest_documents(db: GraphForge, documents: list[dict], model: str = "mock-v1"):
    """Process a list of documents, extract entities and relationships, store all results."""

    for doc in documents:
        # 1. Upsert the document node
        db.execute(
            "MERGE (d:Document {id: $id}) "
            "ON CREATE SET d.title = $title, d.ingested = datetime() "
            "ON MATCH  SET d.title = $title",
            {"id": doc["id"], "title": doc["title"]},
        )

        # 2. Run extraction
        extraction = mock_extract(doc["text"])

        # 3. Store entities (idempotent)
        for ent in extraction["entities"]:
            db.execute(
                f"MERGE (e:Entity:{safe_identifier(ent['label'])} {{canonical: $canonical}}) "
                "ON CREATE SET e.name = $name, e.created = datetime() "
                "ON MATCH  SET e.name = $name",
                {"canonical": ent["canonical"], "name": ent["name"]},
            )
            # Link entity → document
            db.execute(
                "MATCH (d:Document {id: $doc_id}), (e:Entity {canonical: $canonical}) "
                "MERGE (d)-[:MENTIONS]->(e)",
                {"doc_id": doc["id"], "canonical": ent["canonical"]},
            )

        # 4. Store relationships with provenance
        for rel in extraction["relationships"]:
            db.execute(
                "MATCH (src:Entity {canonical: $src}), (dst:Entity {canonical: $dst}) "
                f"MERGE (src)-[r:{safe_identifier(rel['type'])}]->(dst) "
                "ON CREATE SET r.confidence = $conf, r.model = $model, r.created = datetime() "
                "ON MATCH  SET r.confidence = $conf, r.model = $model, r.updated = datetime()",
                {
                    "src":   rel["src"],
                    "dst":   rel["dst"],
                    "conf":  rel["confidence"],
                    "model": model,
                },
            )


def summarise_graph(db: GraphForge) -> str:
    """Query the graph and produce a human-readable summary."""

    # Entity counts by label
    entity_rows = db.to_dicts("""
        MATCH (e:Entity)
        UNWIND labels(e) AS lbl
        WHERE lbl <> 'Entity'
        RETURN lbl AS label, count(e) AS n
        ORDER BY n DESC
    """)

    # High-confidence relationships
    rel_rows = db.to_dicts("""
        MATCH (src:Entity)-[r]->(dst:Entity)
        WHERE r.confidence >= 0.8
        RETURN src.name AS subject,
               type(r)   AS predicate,
               dst.name  AS object,
               r.confidence AS conf
        ORDER BY r.confidence DESC
        LIMIT 10
    """)

    lines = ["=== Knowledge Graph Summary ===\n"]

    lines.append("Entity counts:")
    for row in entity_rows:
        lines.append(f"  {row['label']}: {row['n']}")

    lines.append("\nHigh-confidence facts (≥ 0.80):")
    for row in rel_rows:
        subj = row["subject"]
        pred = row["predicate"].replace("_", " ").lower()
        obj  = row["object"]
        conf = row["conf"]
        lines.append(f"  {subj} {pred} {obj}  [{conf:.0%}]")

    return "\n".join(lines)


# ── Run the pipeline ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    db = GraphForge()   # in-memory for this example

    corpus = [
        {
            "id":    "doc-001",
            "title": "About GraphForge",
            "text":  "Alice Chen at DecisionNerd created GraphForge.",
        },
        {
            "id":    "doc-002",
            "title": "Community update",
            "text":  "Bob Lim uses GraphForge in his research pipeline.",
        },
        # Re-processing doc-001 should not create duplicates
        {
            "id":    "doc-001",
            "title": "About GraphForge (updated)",
            "text":  "Alice Chen at DecisionNerd created GraphForge.",
        },
    ]

    # ingest_documents uses atomic execute() writes. Use one composite publication
    # instead if the complete batch must become visible as a single generation.
    ingest_documents(db, corpus)

    print(summarise_graph(db))
```

Expected output:

```
=== Knowledge Graph Summary ===

Entity counts:
  Project: 1
  Person: 2
  Org: 1

High-confidence facts (≥ 0.80):
  DecisionNerd maintains graphforge  [97%]
  Alice Chen contributes to graphforge  [94%]
  Alice Chen works at decisionnerd  [89%]
  Bob Lim uses graphforge  [78%]
```

---

## 8. Integration Tips

### LangChain

Use GraphForge as a retriever inside a LangChain chain:

```python
from langchain_core.documents import Document as LCDocument  # langchain >= 0.1
from graphforge import GraphForge

db = GraphForge("knowledge/")

def graphforge_retriever(query_entity: str) -> list[LCDocument]:
    rows = db.to_dicts(
        "MATCH (e:Entity {canonical: $canonical})-[r]->(other:Entity) "
        "RETURN e.name AS subject, type(r) AS predicate, other.name AS object",
        {"canonical": query_entity},
    )
    return [
        LCDocument(
            page_content=(
                f"{row['subject']} "
                f"{row['predicate'].replace('_', ' ').lower()} "
                f"{row['object']}"
            )
        )
        for row in rows
    ]
```

### LlamaIndex

Expose the graph as a custom query engine:

```python
from llama_index.core import QueryBundle
from llama_index.core.retrievers import BaseRetriever
from llama_index.core.schema import NodeWithScore, TextNode
from graphforge import GraphForge

class GraphForgeRetriever(BaseRetriever):
    def __init__(self, db: GraphForge):
        self._db = db
        super().__init__()

    def _retrieve(self, query_bundle: QueryBundle) -> list[NodeWithScore]:
        # Simple keyword → canonical lookup; replace with embedding search as needed
        rows = self._db.to_dicts(
            "MATCH (e:Entity) WHERE toLower(e.name) CONTAINS toLower($q) "
            "RETURN e.canonical AS canonical LIMIT 1",
            {"q": query_bundle.query_str},
        )
        if not rows:
            return []
        context = build_context_for_question(self._db, rows[0]["canonical"])
        return [NodeWithScore(node=TextNode(text=context), score=1.0)]
```

### Any Python LLM Library

GraphForge works with any library that accepts a Python string as context — OpenAI, Anthropic, Cohere, Ollama, etc. The pattern is always the same:

```python
context = build_context_for_question(db, entity_canonical)

messages = [
    {"role": "system",  "content": "Answer using only the facts provided."},
    {"role": "user",    "content": f"{context}\n\nQuestion: {question}"},
]

# client.chat.completions.create(model="...", messages=messages)
```

---

## 9. Native Text, Vector, and Hybrid Retrieval

Once your knowledge graph is populated, `forge.find()` provides deterministic BM25 text
search, exact cosine vector search, and `rrf@1` hybrid fusion. The engine returns one stable
Arrow schema in every mode, so retrieval composes directly with Arrow-aware LLM tooling.

### Indexing Entities for Text Search

```python
# Derived text indexes read selected string properties from the committed graph.
forge = db  # Continue with the GraphForge instance populated above.
forge.index("Entity", properties=["name", "description"])
text_results = forge.find(
    "renewable energy policy framework",
    label="Entity",
    limit=10,
)
```

### Storing Vector Embeddings (bring-your-own)

GraphForge accepts caller-supplied vectors and can also generate complete spaces through a
configured provider. This example calls an embedding client directly:

```python
import uuid
import openai

client = openai.OpenAI()
rows = forge.execute(
    "MATCH (e:Entity) "
    "RETURN e.node_uuid AS node_uuid, e.name AS name, e.description AS description"
).to_pylist()
embedding_rows = []
for row in rows:
    text = row["description"] or row["name"]
    vector = client.embeddings.create(
        input=text,
        model="text-embedding-3-small",
    ).data[0].embedding
    embedding_rows.append(
        {
            "node": str(uuid.UUID(bytes=row["node_uuid"])),
            "vector": vector,
        }
    )

# A complete caller batch becomes visible in one atomic generation swap.
forge.publish_caller_embeddings(
    "text-embedding-3-small",
    embedding_rows,
    dimensions=len(embedding_rows[0]["vector"]),
    source_projection={
        "label": "Entity",
        "properties": "name,description",
        "recipe": "text-embedding-3-small-v1",
    },
)
```

### Hybrid Retrieval

```python
# Embed the user query with the same model
query = "renewable energy policy framework"
query_vec = client.embeddings.create(input=query, model="text-embedding-3-small").data[0].embedding

# Text and vector rankings are fused deterministically with rrf@1.
results = forge.find(
    query,
    label="Entity",
    vector=query_vec,
    space="text-embedding-3-small",
    limit=10,
)

for row in results.to_pylist():
    print(
        f"{row['name']}  score={row['score']:.3f}  "
        f"via={row['matched_on']}  uuid={row['node_uuid'].hex()}"
    )
```

### Using Results in Subsequent Queries

Every result carries the live graph node's UUID. Use that public identity with another
analyst verb without exposing an internal numeric surrogate:

```python
top_uuid = str(uuid.UUID(bytes=results.column("node_uuid")[0].as_py()))
context = forge.paths(top_uuid, by="bfs", directed=False)
```

### n-Hop Context Building with recipes

`graphforge.recipes.neighbourhood()` expands from a named entity outward and returns another
Arrow table:

```python
from graphforge.recipes import neighbourhood

# 2-hop neighbourhood around a search result
anchor_name = results.column("name")[0].as_py()
context_nodes = neighbourhood(
    forge,
    anchor_name,
    hops=2,
    label="Entity",
    canonical_prop="name",
)
prompt_context = "\n".join(str(row) for row in context_nodes.to_pylist())
```

---

## Summary

| Task | GraphForge feature |
|------|-------------------|
| Idempotent entity upsert | `MERGE … ON CREATE SET … ON MATCH SET` |
| n-hop neighbourhood query | `MATCH (e)-[*1..2]-(n)` or `recipes.neighbourhood()` |
| Shortest path (workaround) | `MATCH path = (a)-[*1..N]-(b) RETURN length(path) ORDER BY … LIMIT 1` ( for native support) |
| Provenance on edges | Properties on relationships (`r.model`, `r.confidence`) |
| Iterative refinement | `SET r.confidence = …` after LLM review |
| List expansion | `UNWIND` for multi-value properties |
| Temporal tracking | `datetime()`, `duration.between()` |
| Safe identifier validation | `safe_identifier()` before f-string label/rel_type interpolation |
| Semantic retrieval | `forge.find(query, vector=vec, space=...)` → `pyarrow.Table` |

GraphForge's embedded design means the graph lives in the same process as your LLM calls — no network hop, no server, no configuration. That makes it well-suited to iterative, notebook-style workflows where you extract, store, query, and refine in a tight loop.
