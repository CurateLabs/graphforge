# Adapt a Neo4j teaching dataset

**Advanced:** assumes basic Python, tabular data, and graph queries. For a
guided introduction, start with [Your first graph](../quickstart.md).

[Neo4j Graph Examples](https://github.com/neo4j-graph-examples) contains external
teaching projects. Start with [Your first graph](../quickstart.md) if you have
not yet created and queried a GraphForge graph. GraphForge v0.6.0 does not ship
a loader for this collection.

## Choose a small example

Read the selected project's README, data files, license, and required Neo4j
version or extensions. A teaching movie graph can help you learn relationships;
it is not a representative research sample.

Identify which source records become nodes and which become relationships.
Keep the original record identifiers and the source citation. Do not assume
all projects in the collection share a license, format, or execution setup.

## Adapt the data and queries

Use [Graph construction](../graph-construction.md) for Python construction, or
review and execute individual statements through `forge.execute()`.
The [Cypher reference](../cypher-guide.md) describes GraphForge's query surface.
Neo4j-specific procedures, administration commands, and extensions are not
automatically translated.

Read [Cypher script handling](cypher-script-loading.md) before adapting an
existing script. Do not discard constraints or failed statements and assume the
remaining data is equivalent. Verify counts, source identifiers, and a few
known relationships after construction.

For a complete supported example without adaptation, use the
[citation tutorial](../tutorial.md).
