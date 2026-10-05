# Query, analyze, and save a graph

**Advanced · Assumes basic Python, database, and terminal skills.**
Start with the [Advanced introduction](advanced.md) for GraphForge terminology,
or [Basic](overview.md#basic) for a guided first graph.

After the [basic graph quickstart](quickstart.md), use this optional journey to
keep a graph between sessions. It uses the same synthetic citation network.
Research Branches and Versions are not needed for ordinary persistence.

## Create a durable project

Read the [durable-storage requirements](installation.md#durable-storage) first.
The directory must be on an admitted local filesystem and must already exist.
Use a new empty directory for this example. Before v1.0.0 there is no promise
that a project from an earlier GraphForge version can be opened or migrated.

```python
from pathlib import Path
from graphforge import GraphForge

Path("citation-project").mkdir()
forge = GraphForge("citation-project")
survey = forge.add_node("Paper", title="Survey")
methods = forge.add_node("Paper", title="Methods")
replication = forge.add_node("Paper", title="Replication")
forge.add_edge(survey, "CITES", methods)
forge.add_edge(survey, "CITES", replication)
forge.add_edge(replication, "CITES", methods)
```

Opening a path creates project files inside it. If `citation-project` already
exists, open the existing project to continue, or choose another empty directory
to repeat the example. Do not delete an existing project just to follow a guide.

## Ask a different question

Which papers does Survey cite?

```python
query = """
    MATCH (source:Paper {title: $title})-[:CITES]->(paper:Paper)
    RETURN paper.title AS title
    ORDER BY title
"""
print(forge.execute(query, {"title": "Survey"}).to_pylist())
```

Expected output:

```text
[{'title': 'Methods'}, {'title': 'Replication'}]
```

Pass inputs as parameters rather than assembling query strings. Use `ORDER BY`
when the order of the result matters.

## Close and reopen

```python
forge.close()
```

Then reopen the project:

```python
forge = GraphForge("citation-project")
print(forge.execute(query, {"title": "Survey"}).to_pylist())
```

The result is the same two titles. In a new process or notebook kernel, import
GraphForge and define `query` again before running this section. The project
retains graph data; it does not retain Python variables.

## Add an analysis

Analyst verbs let you ask for an algorithm directly. For example, rank the
papers by PageRank over their citation relationships:

```python
ranking = forge.rank("Paper", by="pagerank", via="CITES", directed=True)
print(ranking.column_names)
```

The result includes a `score` column. PageRank describes this graph's structure;
it is not a measure of evidence quality. This call reads the graph. Writing a
score into properties is an explicit option covered in
[analytics integration](analytics-integration.md).

```python
forge.close()
```

## Choose a next task

- [Save a reusable query](cypher-guide.md#saved-queries).
- [Record and revisit an inquiry](record-an-inquiry.md).
- [Move the project](portable-projects.md) using export, verification, and import. Do not copy live project storage or commit it to Git.
- [Explore independently](research-journey.md) when you need research history
  and reviewed contributions.
