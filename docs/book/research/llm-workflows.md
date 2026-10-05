# Research notes: LLM extraction, storage, and review

This Advanced note supports the runnable
[extraction workflow](../use-cases/llm-workflows.md). An LLM is optional;
[Your first research project](../../guide/first-research-project.md) can begin
with source text and human coding.

## Separate the layers of a claim

Retain the source passage, proposed extraction, producer details where known,
review decision, and bounded research conclusion as separate records. A prompt
that calls all retrieved records "facts" erases these distinctions. Neither
model confidence nor a `verified` property establishes correctness.

Repeated extraction can repeat an error. Agreement with a prior model output is
not independent corroboration. Preserve disagreements, missing output, and
unavailable source material instead of converting them into confident answers.
A graph makes such records retrievable; it does not adjudicate them.

## Keep the producer in caller code

Use a narrow output schema and validate it before storing proposed records.
Fixed labels and relationship types avoid inserting model-generated query
structure; source text belongs in parameters. The worked guide uses a clearly
labeled local fixture, not a real model or an accuracy demonstration.

`execute()` returns a PyArrow table; use `to_pylist()` for ordinary dictionaries.
`graphforge.recipes.neighbourhood()` also returns an Arrow table, not a list.
Search results are candidates with retrieval scores, not confidence judgments.
See [context building](llm-context-building.md) and
[entity resolution](search-entity-resolution.md).

## Evaluate against the intended task

Compare proposed extractions with independently reviewed source passages.
Report omissions, unsupported additions, and disagreements as well as successes.
For mixed methods, distinguish counts of participants from counts of passages
or extraction runs. Preserve a passage that challenges the dominant theme.

Use [decision workflows](../use-cases/decision-workflows.md) only when typed
choice/rubric/probability validation is needed. It checks the declared contract,
not scientific validity, and does not authorize action. Persistence requires
[save and reopen](../../guide/tutorial.md); model context alone is not memory.
