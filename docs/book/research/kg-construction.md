# Research notes: knowledge graph construction

This Advanced note supports the runnable
[source-linked construction guide](../use-cases/knowledge-graph-construction.md).
For a first mixed-methods assignment, use
[Your first research project](../../guide/first-research-project.md).

## Preserve meaning while combining records

Give documents and source-specific observations stable keys. Keep the original
passage distinct from an extracted mention, resolved entity, coding decision,
and research conclusion. A node named `Claim` is an ordinary graph record;
that label does not activate immutable native assertion semantics.

`MERGE` supports repeat imports under an explicit key. It does not establish
that two names refer to the same person or that two documents are independent
sources. Duplicate model calls must not accumulate as independent confirmations.

Store disagreements and new reviews separately instead of overwriting one
source field or retaining only the largest confidence score. Record the meaning
of a score and its producer; a normalized score is not necessarily a calibrated
probability. See [recording an inquiry](../../guide/record-an-inquiry.md).

## Supported construction and retrieval

Use native `add_node` / `add_edge`, parameterized Cypher, or the
[bulk construction API](../../guide/graph-construction.md). Framework outputs
need explicit translation; the Python facade has no `add_graph_documents()`.
Use Arrow results through `execute(...).to_pylist()` or `to_pandas()` as needed.
There are no `to_dicts()` / `to_dataframe()` facade methods.

For candidate entity matches, use [search](search-entity-resolution.md) and an
explicit identity rule. Similar names alone do not justify deleting an alias
or merging all of its relationships. Original mentions remain useful even when
resolved to one entity.

## Retain the basis of the result

Keep source locators and original input data alongside reproducible construction
code. A URL alone cannot reproduce changed or unavailable content. Use
[durable projects](../../guide/tutorial.md) for working state,
[portable interchange](../../guide/portable-projects.md) for transfer, and
[knowledge contracts](../architecture/knowledge-public-api-v1.md) when richer
native evidence/status semantics are needed.
