# Research notes: search and entity resolution

This Advanced note supports
[knowledge graph construction](../use-cases/knowledge-graph-construction.md)
and [LLM workflows](../use-cases/llm-workflows.md). For a first research project,
start with the [integrated guide](../../guide/first-research-project.md).

## Candidate retrieval is not identity resolution

`forge.find()` returns candidates as an Arrow table, with a public `node_uuid`,
retrieval `score`, and `matched_on`. Use the source-specific identifier, known
aliases, and other distinguishing evidence to decide whether a candidate is the
same entity. A similar name can belong to a different person or organization.

Text search uses BM25. It does not guarantee synonym, paraphrase, abbreviation,
or typo recall. Store known aliases without replacing original source spelling.
Vector or hybrid retrieval additionally depends on the embedding space and
query representation; synthetic vectors do not demonstrate semantic quality.

The [worked candidate lookup](../use-cases/knowledge-graph-construction.md#candidate-deduplication-with-forgefind)
uses the supported native index and search APIs. See the
[API reference](../../reference/api.md) for exact signatures and space options.

## Check errors before merging

Prepare representative known matches and deliberately similar nonmatches.
Measure missed candidates and false matches separately. Leave ambiguous cases
unresolved until a person or a documented domain rule can decide. Preserve the
original mentions and source links even when several mentions resolve to one
entity. Never delete an alias and its relationships just because it ranked first.

Measure index construction and lookup on the actual workload before making
performance claims. Record the engine version, source scope, index properties,
and query set. Search score is not factual confidence or coding agreement.
