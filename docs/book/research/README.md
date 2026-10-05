# Book — Research and evaluation notes

These are Advanced design and evaluation notes for readers with basic Python
and database skills, not a course in research
methods or a required setup path. Start with
[Your first research project](../../guide/first-research-project.md) for an
integrated social-science example, or the [basic graph](../../guide/quickstart.md)
for graph construction and queries.

The notes distinguish supported API behavior from methodological choices and
workload-specific measurements that need their own evidence. A runnable graph
query does not establish valid interpretation, model quality, or usability.

| Note                                                        | Worked guide                                                                                            |
| ----------------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| [Knowledge graph construction](kg-construction.md)          | [Source-linked mentions](../use-cases/knowledge-graph-construction.md)                                  |
| [Network analysis](network-analysis.md)                     | [Notebook network analysis](../use-cases/network-analysis.md)                                           |
| [Analyst verbs at scale](analyst-verbs-at-scale.md)         | [Native graph algorithms](../use-cases/network-analysis.md#graph-algorithms-with-analyst-verbs)         |
| [LLM-powered workflows](llm-workflows.md)                   | [Extraction and human review](../use-cases/llm-workflows.md)                                            |
| [LLM context building](llm-context-building.md)             | [Source-linked extraction](../use-cases/llm-workflows.md)                                               |
| [Agent grounding](agent-grounding.md)                       | [Tool and capability graphs](../use-cases/agent-grounding.md)                                           |
| [Search and entity resolution](search-entity-resolution.md) | [Candidate lookup](../use-cases/knowledge-graph-construction.md#candidate-deduplication-with-forgefind) |
| [Genealogy modelling](genealogy.md)                         | Source-qualified family relationships                                                                   |

Current signatures live in the [API reference](../../reference/api.md).
Measurement results belong on their owning issue; these notes describe methods
and limitations rather than offering unqualified latency or quality guarantees.
