# Research notes: building context for an LLM call

This Advanced note supports [LLM workflows](../use-cases/llm-workflows.md).
For source interpretation without a model, start with
[Your first research project](../../guide/first-research-project.md).

## Choose context for the question

A neighborhood query follows recorded relationships; text/vector search finds
candidate nodes under its retrieval rules. Neither guarantees complete relevant
evidence. A fixed two-hop neighborhood may be too large, too small, or unrelated
to the question. Inspect what was selected and what was omitted.

`graphforge.recipes.neighbourhood(forge, canonical, hops=2)` returns a PyArrow
table. Its default label is `Entity` and identifying property is `canonical`;
pass `label` and `canonical_prop` to match another schema. Use `.to_pylist()`
when constructing a Python prompt. Do not concatenate an Arrow table with a list
or assume the helper retains source documents automatically.

## Preserve evidence and interpretation

For each selected interpretation, include the source passage or locator,
relevant surrounding context, and review status. Keep alternative explanations
and contrary evidence visible. Retrieval rank is not probability of truth.
When you trim to a token budget, disclose the selection rule; do not present
truncated context as an exhaustive literature search.

The [source-linked extraction example](../use-cases/llm-workflows.md#retrieve-the-proposal-with-its-evidence)
uses explicit graph relationships to show a proposal beside its original text.
That is often sufficient before adding generic neighborhood or hybrid retrieval.

## Evaluate the selection

Use representative questions with known relevant and contrary material. Check
whether the selected context supports the eventual answer, which relevant
sources were missed, and whether a reader can recover the source. Record
latency and context size for that workload separately from answer quality.
No general two-hop token count or lookup-latency guarantee follows from the API.
