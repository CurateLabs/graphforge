# Record and revisit an inquiry

**Advanced · Assumes basic Python, database, and terminal skills.**
Start with the [Advanced introduction](advanced.md) for GraphForge terminology,
or [Basic](overview.md#basic) for a guided first graph.

Use this optional journey after [basic graph use](quickstart.md). Save a
hypothesis, the challenge you applied, and a bounded conclusion, then find that
work by subject when you start a related inquiry.

This example uses ordinary graph nodes and relationships. It needs no research
Branch, Proposal, model service, or ontology. These are your own record types;
labeling a node `Finding` does not give it immutable history or native assertion
status. Those capabilities are available later through the
[knowledge API](../book/architecture/knowledge-public-api-v1.md) and
[research workspaces](research-journey.md).

## State a bounded hypothesis

For the synthetic three-paper graph, challenge this absence claim:

> No paper in this captured reading list cites Survey.

This is a finite graph question, not a statistical test. We create the entire
teaching dataset below, then search its citations for a counterexample. Finding
none establishes an absence **in that captured dataset**. It does not establish
that Survey has no citations elsewhere or that a real literature search was
complete. Failure to reject a statistical null hypothesis is not proof of the
null; record the test, assumptions, uncertainty, and scope of such a result.

## Create the graph and record the challenge

Use a new empty directory on [supported storage](installation.md#durable-storage).
The three papers and three citations below are the complete synthetic input.

```python
from pathlib import Path
from graphforge import GraphForge

Path("inquiry-project").mkdir()
forge = GraphForge("inquiry-project")
survey = forge.add_node("Paper", title="Survey")
methods = forge.add_node("Paper", title="Methods")
replication = forge.add_node("Paper", title="Replication")
forge.add_edge(survey, "CITES", methods)
forge.add_edge(survey, "CITES", replication)
forge.add_edge(replication, "CITES", methods)

challenge = """
    MATCH (source:Paper)-[:CITES]->(:Paper {title: $title})
    RETURN source.title AS citing_paper
    ORDER BY citing_paper
"""
counterexamples = forge.execute(challenge, {"title": "Survey"}).to_pylist()
print("Counterexamples:", counterexamples)

# The author interprets the query result within this known, complete fixture.
if counterexamples:
    conclusion = "A paper in this captured reading list cites Survey."
else:
    conclusion = "No paper in this captured reading list cites Survey."

inquiry = forge.add_node(
    "Inquiry",
    subject="Survey",
    hypothesis="No paper in this captured reading list cites Survey.",
)
evidence = forge.add_node(
    "Evidence",
    query=challenge,
    target="Survey",
    counterexample_count=len(counterexamples),
    scope="Survey, Methods, Replication; three supplied citations",
    source="Synthetic citation graph in the GraphForge inquiry guide",
)
finding = forge.add_node(
    "Finding",
    conclusion=conclusion,
    limitation="This result does not cover papers outside the captured reading list.",
)
forge.add_edge(inquiry, "CHALLENGED_BY", evidence)
forge.add_edge(evidence, "RECORDED_AS", finding)
forge.close()
```

Expected output: `Counterexamples: []`.

The `if` statement is our interpretation of the result, not an engine verdict.
For a real inquiry, a zero count alone is insufficient: an empty graph, a missing
target, or incomplete source collection can also return no matches. Check that
the intended records are present and that the query addresses your question;
otherwise record that the evidence is insufficient. Inspect the source behind
any candidate counterexample before treating it as a refutation.

These records store the question, query, target, count, source, scope, and
conclusion. They remain editable. Changing the graph later does not rerun the
query or update the saved conclusion. To preserve the exact graph used for an
analysis, use [a retained Version](research-journey.md#keep-an-exact-state-while-you-continue).
A source description alone does not archive an external source file.

## Find the prior work in a later session

Open the same path from the same parent directory. Search by the subject of
your new question; you do not need to remember or create an identifier.

```python
from graphforge import GraphForge

forge = GraphForge("inquiry-project")
records = forge.execute("""
    MATCH (q:Inquiry)-[:CHALLENGED_BY]->(e:Evidence)-[:RECORDED_AS]->(f:Finding)
    WHERE q.subject = $subject
    RETURN q.hypothesis AS hypothesis, e.scope AS scope,
           e.query AS challenge, e.target AS target, e.source AS source,
           e.counterexample_count AS counterexamples,
           f.conclusion AS conclusion, f.limitation AS limitation
""", {"subject": "Survey"}).to_pylist()
if not records:
    print("No prior findings for this subject.")
for record in records:
    print(record["hypothesis"])
    print(record["scope"])
    print("Counterexamples:", record["counterexamples"])
    print(record["conclusion"])
    print(record["limitation"])
forge.close()
```

Expected output:

```text
No paper in this captured reading list cites Survey.
Survey, Methods, Replication; three supplied citations
Counterexamples: 0
No paper in this captured reading list cites Survey.
This result does not cover papers outside the captured reading list.
```

The returned records also contain the challenge query, target, and source.
Inspect those and the original scope before reusing the conclusion. Subject
matching is exact: an unrecorded subject returns no records and the explicit
no-prior-findings message. This example does not provide semantic search.

If your next inquiry asks whether an expanded reading list cites Survey, treat
that as a new question. Collect and check the additional evidence, challenge the
claim again, and save a new record with its own scope. The earlier conclusion
still describes only the original three-paper graph. Multiple records may share
a subject; review each one's scope before choosing what informs the new inquiry.
