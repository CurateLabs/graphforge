# Adapt a Cypher script

**Advanced:** assumes basic Python, tabular data, and graph queries. For a
guided introduction, start with [Your first graph](../quickstart.md).

Use this page after you can run a query through the
[Python quickstart](../quickstart.md). It is for adapting an existing script,
not the first route for importing interview or spreadsheet data.

GraphForge v0.6.0 does not ship `CypherLoader`, a `graphforge.datasets` module,
or a general multi-statement script runner. The supported Python surface
executes one reviewed statement at a time:

```python
from graphforge import GraphForge

forge = GraphForge()
result = forge.execute(
    "CREATE (p:Paper {title: $title}) RETURN p.title AS title",
    {"title": "Methods"},
)
print(result.to_pylist())
forge.close()
```

Expected output: `[{'title': 'Methods'}]`. This example is in memory.

## Review before executing

Read the script's source, intended dataset, and required database extensions.
Identify data-changing statements and any assumptions about uniqueness or
required properties. Neo4j administration commands and procedures are not
automatically skipped or translated by GraphForge.

Do not split an arbitrary script on semicolons: quoted values and comments can
contain them. For a small teaching script, extract and review individual
statements, then pass them separately to `execute()`. Variables do not persist
between calls; a later statement must match existing nodes by their properties
or bind explicit parameters.

If a statement is unsupported, stop and adapt that operation explicitly using
the [Cypher reference](../cypher-guide.md) or
[construction API](../graph-construction.md). Silently omitting constraints,
procedures, or failed writes can change the dataset and its meaning.

After loading, check counts and known relationships against the source.
Keep the original script and your adaptation. For row-based files, start with
[data preparation](overview.md#bring-your-own-files).
