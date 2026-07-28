# Book — Use Cases

Deeper worked examples for GraphForge **v0.5.0**: embedded Rust/Arrow engine,
Parquet-backed projects, openCypher, and analyst verbs (`forge.rank`,
`forge.cluster`, `forge.find`, …).

For install and everyday workflows, start in the [Guide](../../guide/overview.md).
For validation notes behind these guides, see [research](../research/).

| Guide | What you build |
| --- | --- |
| [Knowledge graph construction](knowledge-graph-construction.md) | MERGE-based entity graphs, provenance, dedup, `forge.find` |
| [Network analysis](network-analysis.md) | Notebook metrics, datasets, `forge.rank` / `forge.cluster`, pandas/NetworkX bridge |
| [LLM-powered workflows](llm-workflows.md) | Extract → store → retrieve → synthesise loops |
| [AI agent grounding](agent-grounding.md) | Ontology-backed tool and capability graphs |
| [AI agent tool recall](agent-tool-recall.md) | Large tool registries with dependencies and permissions |

Prefer project directories for persistence (`GraphForge("my-graph/")`), not legacy
file-extension paths.
