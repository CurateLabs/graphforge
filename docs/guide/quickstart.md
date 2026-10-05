# Your first graph

**Question: which papers in a small reading list are cited by other papers?**

**Basic · No programming knowledge needed to understand this example.**

A graph records items and their connections. Here, each **node** is a paper;
a **relationship** says that one paper cites another. A **query** is a question
we ask about the recorded connections. This is a graph of relationships, not a
bar chart.

Our three fictional papers have these connections:

| Paper       | Cites                     |
| ----------- | ------------------------- |
| Survey      | Methods and Replication   |
| Replication | Methods                   |
| Methods     | No papers in this example |

Count how often each title appears in the right column: Methods appears twice,
Replication once, and Survey never. Those are the answers we expect from
GraphForge. A citation count describes these records; it does not establish a
paper's quality.

## Have GraphForge check the answer

Use [Work with an agent](work-with-an-agent.md) to have a coding assistant run
this example. You should be able to compare the returned answer with the table
above without understanding Python. Initial computer setup may need help from
a technical colleague or computer support person. A chat window that cannot run code cannot perform
this check.

If you already know basic Python and terminal commands, follow
[Installation](installation.md), then run the code below in a script or
[notebook](use-a-notebook.md).

<details>
<summary>Python instructions for an agent, helper, or Advanced reader</summary>

This v0.6.0 example runs in memory: its data lasts only while the program is
open. It needs no account or saved project folder.

## Create connected data

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

These are synthetic papers. The graph says that Survey cites two papers and
Replication cites Methods. `add_node` returns a handle you can pass directly
to `add_edge`; you do not need to construct identities yourself.

## Ask a question

```python
result = forge.execute("""
    MATCH (:Paper)-[:CITES]->(paper:Paper)
    RETURN paper.title AS title, count(*) AS citations
    ORDER BY citations DESC, title
""")
print(result.to_pylist())
```

Expected output:

```text
[{'title': 'Methods', 'citations': 2}, {'title': 'Replication', 'citations': 1}]
```

Methods has two incoming citations in this graph. Survey has none, so it is
absent from this query's result. The answer describes the example data, not
the papers' real-world quality or influence.

The result is an Arrow table. `to_pylist()` makes it easy to inspect without
installing pandas or learning Arrow's storage format.

## Close the example

```python
forge.close()
```

Closing this in-memory instance discards its graph. Re-running the creation
cell against the same open instance adds another set of nodes; start with a
new instance when repeating the example.

</details>

## Check the result

The returned answer should contain **Methods: 2** and **Replication: 1**.
Survey is absent because the question asks for papers that have incoming
citations. No result here means no matching connection in these records;
it does not mean that nobody anywhere has cited Survey.

This example keeps its graph in memory, so closing the program discards it.
Keep the example instructions to repeat it. Saving a project is a separate
step.

You have completed basic graph use. To connect numerical and interview evidence,
continue with [Your first mixed-methods project](first-research-project.md).
You can stop at either lesson; neither requires learning every GraphForge feature.

To write your own code, move to [Advanced](advanced.md). It assumes basic Python,
database, and terminal skills and explains how to save, query, and analyze a graph.
