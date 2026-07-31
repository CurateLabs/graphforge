# Overview

**GraphForge for VS Code** (`CurateLabs.graphforge`) brings [GraphForge](https://docs.graphforge.sh/)
projects into the editor: an openCypher query surface, a set of analyst verbs (Rank, Cluster,
Paths, Analyze, Similar, Find), a progressive ontology viewer, and an epistemic-aware result
graph.

## Who it's for

- Analysts and engineers who already have (or are building) a GraphForge project and want to
  query and explore it without leaving VS Code.
- Python-first analysts working out of a `pyproject.toml`/`uv`-managed project or a
  notebook-heavy workspace — the extension runs the same commands against the `graphforge`
  PyPI package, not just the native Node binding.
- Coding agents (Cursor, GitHub Copilot Agent Mode, or similar) driving the extension
  end-to-end via stable `vscode.commands.executeCommand` IDs — see
  [`agent-interop.md`](agent-interop.md).

## What you get

| Surface | What it does |
|---|---|
| **Setup** | `GraphForge: Check Environment` and the two `Setup … Binding` commands are the palette-first, no-dead-end entry point — see [`install.md`](install.md). |
| **Cypher** | `.cypher` / `.cql` language support with syntax highlighting, plus **Run Query** (and **Run Query with Parameters…**). |
| **Analyst verbs** | Rank, Cluster, Paths, Analyze, Similar, Find — QuickPick-driven, each with an **Advanced…** variant for optional parameters. |
| **Projects** | An Activity Bar explorer that lists folders containing a valid GraphForge `FORMAT` marker. |
| **Ontology** | A mode badge (exploratory/advisory/strict) plus an entity/relation tree and an Ontology Viewer webview, including a **Load Ontology…** action. |
| **Knowledge** | List, inspect, and create knowledge-ledger assertions, with Advanced commands for attaching evidence, assessing confidence, and recording status. |
| **Result Graph** | A webview rendering query/verb results as a graph, styled by class and epistemic status. |

## Two runtimes, one extension

The extension can execute Cypher and analyst verbs through either engine binding — a native
`@graphforge/node` addon, or the `graphforge` Python package — and picks sensibly between them
by default. See [`install.md`](install.md) for the full setup and selection story.

## License

Apache-2.0 © Curate Labs Inc.
