# Research notes: modelling family history

This optional domain example assumes [basic graph use](../../guide/quickstart.md).
For an integrated social-science assignment, start with
[Your first research project](../../guide/first-research-project.md).

## Model a reported relationship

Distinguish a record's statement from a verified family relationship. Preserve
its source, date or edition, and locator; record disputed parentage as competing
claims rather than overwriting one parent edge and losing the earlier evidence.
The synthetic example below models a reported relationship only:

```python
from graphforge import GraphForge

forge = GraphForge()
parent = forge.add_node("Person", key="person-1", name="Alex")
child = forge.add_node("Person", key="person-2", name="Sam")
forge.add_edge(
    parent, "REPORTED_PARENT_OF", child,
    source="Synthetic family record", locator="entry 1",
)

result = forge.execute("""
    MATCH (parent:Person)-[report:REPORTED_PARENT_OF]->(child:Person)
    WHERE child.key = $key
    RETURN parent.name AS reported_parent, report.source AS source,
           report.locator AS locator
""", {"key": "person-2"})
print(result.to_pylist())
forge.close()
```

Expected output:

```text
[{'reported_parent': 'Alex', 'source': 'Synthetic family record', 'locator': 'entry 1'}]
```

## Extend only as needed

Use distinct stable keys for people with the same name. Keep historical spelling
and aliases as recorded; search is candidate retrieval, not proof of identity.
A directed parent relationship is different from a marriage or an event. Define
those meanings explicitly before traversing multiple generations.

A path through reported relationships is a path through those reports, not
independent proof of ancestry. Connected components describe connectivity;
Louvain does not infer surname groups or biological relationships. Retain source
context and disagreements when interpreting a result.

GraphForge has no built-in GEDCOM loader. Parse a selected source externally,
document your mapping, and use [construction APIs](../../guide/graph-construction.md).
Use [save and reopen](../../guide/tutorial.md) for durable state and
[portable projects](../../guide/portable-projects.md) for transfer. Richer
[knowledge contracts](../architecture/knowledge-public-api-v1.md) are optional
when source-derived assertions and status history are needed.
