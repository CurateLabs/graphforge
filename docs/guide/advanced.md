# Advanced: work directly with GraphForge

This path assumes you can read **basic Python**, understand database tables and
queries, and run commands in a terminal. It does not assume graph-database, research-platform, or storage-engine
expertise. For guided research without writing code, choose the [Basic path](overview.md#basic).

## Bring what you already know

| Familiar idea                                  | GraphForge idea                    | What you do                                                          |
| ---------------------------------------------- | ---------------------------------- | -------------------------------------------------------------------- |
| A row representing a person or document        | A node with a label and properties | `add_node("Paper", title="Survey")` creates one paper                |
| A link between rows, often queried with a join | A named, directed relationship     | `add_edge(survey, "CITES", methods)` records which paper cites which |
| A query producing a table                      | An openCypher query                | `execute()` returns a result table in Python                         |
| Data stored between runs                       | A project directory                | Open `GraphForge(path)` and close it when finished                   |

A label groups similar nodes; a property stores a detail. A direction such as
`CITES` is part of your model, not a causal claim. Unlike a declared SQL schema,
an exploratory graph can start without an ontology. An **ontology** is an
optional set of rules about entity types, relationships, and values; learn it
when you need shared definitions or validation.

## Run, inspect, then adapt

1. [Install a matching package](installation.md). If you prefer cells to a script,
   [start a local notebook](use-a-notebook.md).
2. Run [Your first graph](quickstart.md), including its expandable Python steps.
   Confirm the expected result before changing the example.
3. [Query, analyze, and save](tutorial.md). Supply query values as parameters and
   reopen the saved project in a new process.
4. [Construct your own graph](graph-construction.md), then
   [query it with Cypher](cypher-guide.md). Choose identifiers and what each
   relationship means before importing rows.

For example, this pattern follows a stored citation:

```cypher
MATCH (source:Paper)-[:CITES]->(target:Paper)
RETURN source.title AS source, target.title AS target
```

`MATCH` selects matching connections. Parentheses name nodes; `:Paper` selects
the label; `-[:CITES]->` selects an outgoing citation. `RETURN` chooses the
result columns. It does not infer a connection that you did not store.

Tabular query and analysis results in Python are **Arrow tables**, a column-based
data format. Start with
`result.to_pylist()` to inspect ordinary Python rows. Conversion to pandas or
Polars and graph algorithms are explained in [analytics integration](analytics-integration.md).
In Node, those tables arrive as Arrow IPC bytes; the
[integration guide](integrate-graphforge.md) shows decoding. Construction can
return handles, and metadata or lifecycle operations can return collections,
scalars, or no value. You do not need serialization internals to use the Python path.

## Add one capability for the next task

| Need                                           | Read next                                                 | Concept introduced there                             |
| ---------------------------------------------- | --------------------------------------------------------- | ---------------------------------------------------- |
| Combine numerical and interview evidence       | [Mixed-methods worked example](first-research-project.md) | Unit of analysis, coding, integrated interpretation  |
| Save a challenged hypothesis and find it again | [Record an inquiry](record-an-inquiry.md)                 | Scope and evidence, stored as ordinary graph records |
| Keep an exact past state while editing         | [Keep research history](research-journey.md)              | Version: a retained state unaffected by later edits  |
| Move a project to another folder or machine    | [Move and share](portable-projects.md)                    | Exported package and verified import                 |
| Explore optional model-assisted applications   | [Application examples](../book/use-cases/README.md)       | Human-reviewed extraction and retrieval              |

A **UUID** is an identifier rather than a row number. Some advanced operations
also require an **operation identity**, which lets the engine recognize an exact
retry. Reuse the original request and identity for that retry; a changed action
is a new operation. Follow the concrete example for the operation you need.

Look up exact method signatures in the [API reference](../reference/api.md).
Architecture documents, wire formats, and contributor requirements explain how
the engine is built. They are separate specialist material and are not the next
level you must complete to use these advanced guides.
