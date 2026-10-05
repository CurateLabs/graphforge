# Knowledge graph construction

**Advanced:** basic Python, database queries, and terminal use.

This optional Python guide connects an entity mention to the document that
contains it. It assumes [basic graph use](../../guide/quickstart.md) and the
matching [installation](../../guide/installation.md). For a first mixed-methods
assignment, start with [Your first research project](../../guide/first-research-project.md).

A graph records what you entered. A source statement, a machine extraction,
and your interpretation have different meanings, even when they refer to the
same entity. Keep those records distinguishable. The examples below use ordinary
graph properties; they do not activate native assertion status or immutable
research history.

## Schema design

Use a stable source key to identify each document and a source-specific key for
each mention. An entity's name alone is not a reliable identity rule: two people
can share a name, and one person can have several names. Decide and document
your identity rule before merging records.

This synthetic example contains one document, one entity, and one mention:

```python
from graphforge import GraphForge

forge = GraphForge()
document = forge.add_node(
    "Document", key="note-1", text="The study group met at Central Library."
)
library = forge.add_node("Entity", key="place-1", name="Central Library")
mention = forge.add_node(
    "Mention", key="note-1-mention-1", quote="Central Library", method="manual"
)
forge.add_edge(mention, "IN_DOCUMENT", document)
forge.add_edge(mention, "REFERS_TO", library)
```

The mention records a phrase in the document. It does not establish the
library's location, the meeting's date, or the statement's reliability.

## Entity extraction pattern

For repeated imports, use `MERGE` with your chosen stable key. It matches that
key; it does not decide whether two descriptions refer to the same real thing.

```python
for _ in range(2):
    forge.execute("""
        MERGE (entity:Entity {key: $key})
        ON CREATE SET entity.name = $name
    """, {"key": "place-1", "name": "Central Library"})

print(forge.execute("MATCH (entity:Entity) RETURN count(entity) AS entities").to_pylist())
```

Expected output: `[{'entities': 1}]`. Repeating `CREATE` or `add_node` instead
would create more nodes. Retrying an extraction is not independent confirmation
of a claim. A count of extraction runs must not be presented as a count of
independent supporting sources.

For extraction frameworks, translate their output into the public construction
API or parameterized Cypher. GraphForge has no `add_graph_documents()` method.
Use fixed labels and relationship types in your importer, and pass source text
through query parameters. See [bulk construction](../../guide/graph-construction.md)
when a larger import needs bounded or atomic batches.

## Query the source behind a mention

```python
result = forge.execute("""
    MATCH (mention:Mention)-[:REFERS_TO]->(entity:Entity)
    MATCH (mention)-[:IN_DOCUMENT]->(document:Document)
    RETURN entity.name AS entity, mention.quote AS quote,
           document.key AS source, document.text AS passage
""")
print(result.to_pylist())
```

Expected output:

```text
[{'entity': 'Central Library', 'quote': 'Central Library', 'source': 'note-1', 'passage': 'The study group met at Central Library.'}]
```

For real sources, also retain the locator needed to recover the passage, such
as a page, paragraph, or transcript segment; record its edition or capture date.
A URL alone does not retain the bytes that you read. Keep original source data
and your import code. Native [Source/Artifact and knowledge contracts](../architecture/knowledge-public-api-v1.md)
provide optional richer provenance.

## Provenance and confidence

Keep the source passage, extraction method, reviewer, and interpretation
separate. A model score or a human review rating needs an explicit meaning;
neither becomes a probability of truth merely because its range is `[0, 1]`.
Do not increase confidence solely because a repeated model call agrees with
its earlier output. Revisit the evidence and look for disagreement.

If another source contradicts a claim, retain both sources and the disagreement.
Overwriting a single `source` property or keeping only the largest confidence
score would lose information needed to understand the change. See
[recording an inquiry](../../guide/record-an-inquiry.md) for a simple record of
the challenge and bounded conclusion.

## Candidate deduplication with `forge.find()`

Search retrieves possible matches; a person or an explicit domain rule decides
identity. A fuzzy match alone is not permission to merge records or delete an
alias and its relationships.

```python
forge.index("Entity", properties=["name"])
candidates = forge.find("Central Library", label="Entity", limit=3)
print(candidates.select(["name"]).to_pylist())
```

Expected output: `[{'name': 'Central Library'}]`.

Search results also carry a public `node_uuid`, a retrieval `score`, and
`matched_on`. Retrieval score measures the search ranking, not factual
confidence. Store known aliases when useful, inspect candidate details, and
preserve source-specific mentions when resolving identity. Exact API signatures
are in the [API reference](../../reference/api.md).

## Save or extend the graph

```python
forge.close()
```

This example is memory-only. Use [save and reopen](../../guide/tutorial.md) for
durable storage and [portable projects](../../guide/portable-projects.md) for
transfer. Use [LLM workflows](llm-workflows.md) only when you need an extraction
assistant, and [agent grounding](agent-grounding.md) only when building a tool
selection application. Neither is required to document an inquiry.

## Resources

- [GraphForge documentation](../../index.md)
- [openCypher compatibility](../../reference/opencypher-compatibility.md)
- [API reference](../../reference/api.md)
