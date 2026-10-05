# Integrate GraphForge

**Advanced · Assumes basic Python, database, and terminal skills.**
Start with the [Advanced introduction](advanced.md) for GraphForge terminology,
or [Basic](overview.md#basic) for a guided first graph.

Use this path when you are writing a script, tool, or application. Start with
ordinary graph queries; research and collaboration APIs are optional additions.
See [Installation](installation.md) for the v0.6.0 package and storage contract.

## Python

The [basic graph quickstart](quickstart.md) is a complete Python example.
`GraphForge.execute()` returns a `pyarrow.Table`; `to_pylist()` gives you rows.
Pass query inputs through the parameter dictionary. Keep a reference to the
instance and call `close()` when finished.

## Node and TypeScript

This optional section additionally assumes basic JavaScript and npm use. Stay
with Python if those are unfamiliar.

Install `@curatelabs/graphforge` at the version selected in
[Installation](installation.md), plus `apache-arrow` for table helpers. Save
the following as an `.mjs` file and run it with Node:

```js
import { GraphForge } from "@curatelabs/graphforge";
import { tableFromIPC } from "apache-arrow";

const forge = new GraphForge();
const survey = forge.addNode("Paper", { title: "Survey" });
const methods = forge.addNode("Paper", { title: "Methods" });
forge.addEdge(survey, "CITES", methods);

const table = tableFromIPC(
  forge.execute(`
  MATCH (source:Paper)-[:CITES]->(paper:Paper)
  RETURN source.title AS source, paper.title AS title
  ORDER BY source, title
`),
);
console.log(table.toArray().map((row) => row.toJSON()));
forge.close();
```

The result contains one row: `{ source: 'Survey', title: 'Methods' }`.
Node's native query result is Arrow IPC bytes. This explicit decoding is an
integration concern; the basic Python/agent path needs no IPC plumbing.
Arrow preserves integer widths, so some numeric columns become JavaScript
`bigint` values rather than JSON numbers.

## Rust and CLI

The Rust links are specialist integration references and assume Rust experience.
The CLI can be used with the introductory terminal skills of the Advanced path.

Rust owns behavior through `graphforge-api`. See the
[Rust API reference](../reference/api.md#rust-crate-api) and the
[public facade source](https://github.com/CurateLabs/graphforge/tree/main/crates/graphforge-api).

The Python package supplies the `graphforge` CLI. The npm CLI package supplies
both `graphforge` and `gf`. [Repository integration](repository-integration.md)
documents initialization, synchronization, checkpoints, and export/import.
Use each command's `--help` for its request and output format. A query export
command writes a result file; it is not an interactive row-printing shell.

## Add capabilities by task

| Task                                | Contract or guide                                                  |
| ----------------------------------- | ------------------------------------------------------------------ |
| Reuse read-only analyses            | [Saved queries](cypher-guide.md#saved-queries)                     |
| Load many nodes and edges           | [Graph construction](graph-construction.md)                        |
| Retain a simple inquiry record      | [Record an inquiry](record-an-inquiry.md)                          |
| Track evidence and assertion status | [Knowledge API](../book/architecture/knowledge-public-api-v1.md)   |
| Work on retained research context   | [Research workspaces](../book/architecture/research-workspaces.md) |
| Validate a caller-supplied decision | [Decision workflows](../book/use-cases/decision-workflows.md)      |
| Move a project                      | [Portable projects](portable-projects.md)                          |

The engine returns typed data and errors. Your application decides how to
present results, request review, and enforce hosted access. Before v1.0.0,
pin matching versions and expect intentional API/format changes without a
migration guarantee.
