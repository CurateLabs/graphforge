# LLM-powered extraction workflows

**Advanced:** basic Python, database queries, and terminal use.

This is an optional integration guide for Python users who want an extraction
assistant. It is not required for [a first research project](../../guide/first-research-project.md).
Start with [basic graph use](../../guide/quickstart.md) and the matching
[installation](../../guide/installation.md). An LLM client, its account, and its
execution belong to your application; the example below uses a labeled local
fixture and makes no model call.

The useful sequence is: retain a passage, record a proposed extraction, inspect
it beside its source, then decide how to use it. Storing an extraction does not
make it a fact. Model agreement, a confidence score, and a graph query do not
substitute for checking the original evidence.

## Keep the original and the proposed interpretation separate

This synthetic passage could appear in an interview-coding exercise. The
proposal is a suggested theme, not an established explanation of the speaker's
behavior.

```python
from graphforge import GraphForge

forge = GraphForge()
text = "I study at the library because my flat is noisy."
passage = forge.add_node(
    "Passage", key="interview-1-segment-1", text=text,
    source="Synthetic interview for this guide",
)

# A local teaching fixture, not the output of a model run.
proposal = {
    "quote": "my flat is noisy",
    "theme": "study environment",
    "producer": "local teaching fixture",
}
if proposal["quote"] not in text:
    raise ValueError("The proposed quote is absent from the retained passage")

extraction = forge.add_node(
    "Extraction", quote=proposal["quote"], proposed_theme=proposal["theme"],
    producer=proposal["producer"], review_status="unreviewed",
)
forge.add_edge(extraction, "DERIVED_FROM", passage)
```

A substring check only establishes that the text occurs in this passage. It
cannot establish that the theme is appropriate, that the quote represents the
whole interview, or that the source statement is true. In real material,
retain enough surrounding text and a source locator to inspect the context.

## Retrieve the proposal with its evidence

```python
review = forge.execute("""
    MATCH (extraction:Extraction)-[:DERIVED_FROM]->(passage:Passage)
    RETURN passage.key AS source, passage.text AS passage,
           extraction.quote AS quote, extraction.proposed_theme AS proposed_theme,
           extraction.review_status AS review_status
""")
print(review.to_pylist())
```

Expected output:

```text
[{'source': 'interview-1-segment-1', 'passage': 'I study at the library because my flat is noisy.', 'quote': 'my flat is noisy', 'proposed_theme': 'study environment', 'review_status': 'unreviewed'}]
```

This is a review prompt, not a validated finding. A reviewer can retain the
proposal, revise it, or disagree. Record the decision and reason separately
from the original proposal. Avoid rewriting a model score and describing the
result as "verified data"; neither a property name nor a threshold validates
an interpretation.

## Record a review without replacing the extraction

```python
review_record = forge.add_node(
    "Review", reviewer="demo analyst", decision="retain as a candidate code",
    reason="The passage explicitly connects study location with noise at home.",
    limitation="One synthetic passage; no claim about prevalence or causation.",
)
forge.add_edge(review_record, "REVIEWS", extraction)

reviewed = forge.execute("""
    MATCH (review:Review)-[:REVIEWS]->(extraction:Extraction)-[:DERIVED_FROM]->(passage:Passage)
    RETURN passage.key AS source, review.decision AS decision,
           review.limitation AS limitation
""")
print(reviewed.to_pylist())
```

Expected output:

```text
[{'source': 'interview-1-segment-1', 'decision': 'retain as a candidate code', 'limitation': 'One synthetic passage; no claim about prevalence or causation.'}]
```

These are ordinary editable graph records, not native immutable assertions or
an enforced review workflow. Use [recording an inquiry](../../guide/record-an-inquiry.md)
for a bounded conclusion and [research workspaces](../../guide/research-journey.md)
when retained context, independent work, or formal review becomes useful.

## Substitute a real producer deliberately

Replace the fixture with your chosen caller-owned extraction function only
when you need it. Give it a narrow output schema, preserve the returned proposal,
and record its available model/revision and source details. Do not invent an
unknown model revision or infer a score's meaning. Validate fields and preserve
errors or missing results rather than manufacturing negative answers.

Use stable source and extraction keys for repeat imports; see
[knowledge graph construction](knowledge-graph-construction.md). Re-running the
same producer on the same passage does not create independent corroboration.
If multiple sources or reviewers disagree, keep their separate records.

For a numerical comparison, decide whether each counted unit is a participant,
a document, a passage, or a coding decision. Counting extractions can inflate
apparent support when one source has many passages or multiple model runs.
Return to [the mixed-methods guide](../../guide/first-research-project.md) to
connect descriptive counts with interpreted passages and exceptions.

## Native text, vector, and hybrid retrieval

`forge.find()` can retrieve candidate nodes using text, caller-supplied vectors,
or configured provider operations. Retrieval scores are not factual confidence
or coding agreement. Use the [embedding publication contract](../architecture/embedding-v1.md#embedding-space-publication)
and [API reference](../../reference/api.md) for exact configuration and modes.

`graphforge.recipes.neighbourhood()` is a thin helper that returns a PyArrow
table through the native query engine. It expects the labels and identifying
property you specify; it does not infer your schema or retrieve every relevant
source. For this example, retrieve the explicit `DERIVED_FROM` path above.

## Retain or share your work

```python
forge.close()
```

This example is memory-only. Follow [save and reopen](../../guide/tutorial.md)
for durable projects and [portable projects](../../guide/portable-projects.md)
for transfer. A model prompt or a summary alone is not a retained source.

For typed external choice/rubric/probability results, use the optional
[decision workflow](decision-workflows.md). Its validator checks structure and
correlation; it does not determine scientific validity or authorize action.
