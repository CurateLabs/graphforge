# Network analysis in notebooks

**Advanced:** basic Python, database queries, and terminal use.

This optional guide is for readers who can run Python and want to inspect a
network. Start with [a notebook](../../guide/use-a-notebook.md) and
[your first graph](../../guide/quickstart.md). For a first social-science project
combining numerical observations and interpreted passages, use
[Your first research project](../../guide/first-research-project.md).

The examples use a synthetic citation network. Its counts describe the entered
relationships; they do not measure a paper's quality, prove influence, or
estimate a population characteristic. Install the matching version through
[Installation](../../guide/installation.md).

## Create a small network

Run the Python blocks in order in one session. No dataset download is needed.
GraphForge does not ship the proposed `graphforge.datasets` catalogue. For real
data, obtain and document your source, then use the supported
[construction APIs](../../guide/graph-construction.md).

```python
from graphforge import GraphForge

forge = GraphForge()
survey = forge.add_node("Paper", title="Survey")
methods = forge.add_node("Paper", title="Methods")
replication = forge.add_node("Paper", title="Replication")
forge.add_edge(survey, "CITES", methods)
forge.add_edge(survey, "CITES", replication)
forge.add_edge(replication, "CITES", methods)
```

One node represents a paper; a directed `CITES` relationship runs from the
citing paper to the cited paper. Before analyzing another network, state what
its nodes and relationships represent, whether direction matters, and which
records are missing. Two datasets using the word "connection" need not measure
the same thing.

## Degree distribution

Count incoming citations for every paper, including papers with no citations:

```python
degrees = forge.execute("""
    MATCH (paper:Paper)
    OPTIONAL MATCH (:Paper)-[citation:CITES]->(paper)
    RETURN paper.title AS title, count(citation) AS citations
    ORDER BY citations DESC, title
""")
print(degrees.to_pylist())
```

Expected output:

```text
[{'title': 'Methods', 'citations': 2}, {'title': 'Replication', 'citations': 1}, {'title': 'Survey', 'citations': 0}]
```

`count(citation)` excludes the null relationship from an unmatched
`OPTIONAL MATCH`. Using `count(*)` here would incorrectly count that row as one
citation. Decide whether you need a relationship count or a count of distinct
neighbors: repeated relationships can make them different quantities.

## Path analysis

Inspect the direct and indirect routes from Survey to Methods:

```python
paths = forge.execute("""
    MATCH path = (source:Paper {title: $source})-[:CITES*1..2]->(target:Paper {title: $target})
    RETURN length(path) AS hops
    ORDER BY hops
""", {"source": "Survey", "target": "Methods"})
print(paths.to_pylist())
```

Expected output: `[{'hops': 1}, {'hops': 2}]`.

These are paths in the recorded citation graph, not proof that one author read
another or that an idea spread along that route. Variable-length queries can
enumerate many paths; choose a justified hop bound. `ORDER BY ... LIMIT` limits
the returned rows and does not by itself establish a bound on traversal work.
See [native path algorithms](../architecture/algorithms.md) for other tasks.

## Graph algorithms with analyst verbs

```python
ranking = forge.rank("Paper", by="pagerank", via="CITES", directed=True)
print(ranking.select(["title", "score"]).to_pylist())
```

PageRank summarizes this graph's structure. Scores depend on the graph,
direction, and algorithm settings; a high score is not a measure of truth,
research quality, or causal importance.

For a larger network where grouping is useful, community detection is optional:

```python
communities = forge.cluster("Paper", by="louvain", via="CITES")
print(communities.select(["title", "community_id"]).to_pylist())
```

A computed community is an algorithmic grouping. Inspect its members and edges
before assigning a social meaning or comparing it with interview themes. A
three-paper teaching graph does not establish useful real-world communities.
The [algorithm catalog](../architecture/algorithms.md) defines the available
verbs and options. `rank` and `cluster` support explicit `write_property`;
writing a score does not give it permanent validity after the graph changes.

## Integration with pandas

Python queries and analyst verbs return PyArrow tables. Use `to_pylist()` for
ordinary rows or `degrees.to_pandas()` if you installed pandas. There are no
`CypherValue` wrappers or row `.value` calls. For plotting and explicit
NetworkX/igraph construction from selected Arrow results, use
[analytics integration](../../guide/analytics-integration.md) and
[visualization examples](../../guide/visualization.md). Those integrations do
not require an alternate graph execution engine inside GraphForge.

## Persisting analysis results

```python
forge.close()
```

This example is memory-only. To keep a graph across kernel resets, follow
[save and reopen](../../guide/tutorial.md) using an existing directory on
supported storage. Keep original data and the code that constructs and analyzes
it. Use [portable export, verification, and import](../../guide/portable-projects.md)
to transfer a project; do not copy live project storage or commit it to Git.

## Next steps

- [Record and revisit an inquiry](../../guide/record-an-inquiry.md) to retain a
  challenge, its evidence, and a bounded conclusion.
- [Cypher guide](../../guide/cypher-guide.md) and
  [openCypher compatibility](../../reference/opencypher-compatibility.md).
- [Dataset catalogue proposals](../../guide/datasets/overview.md), which are
  not built-in loaders.
- [Agent grounding](agent-grounding.md), when building an application that
  selects tools from a graph.
