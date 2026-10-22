# `@curatelabs/graphforge`

Native Node.js bindings for the GraphForge embedded graph engine. The package
loads the Rust implementation for the current platform; it does not include a
JavaScript fallback engine.

## Install

**npm**

```bash
npm install @curatelabs/graphforge
```

**pnpm**

```bash
pnpm add @curatelabs/graphforge
```

`query()` from the package returns an
[`apache-arrow`](https://www.npmjs.com/package/apache-arrow) `Table` directly,
so reading a useful result needs no Arrow IPC decoding. `apache-arrow` is an
optional peer dependency used only by `query()` and your own table helpers;
install it next to the binding (`npm install @curatelabs/graphforge apache-arrow`
/ `pnpm add @curatelabs/graphforge apache-arrow`). The native engine and CLI do
not need it.

Node.js 20 or newer is required. Prebuilt packages are published for:

- macOS: Apple silicon and Intel
- Linux glibc: ARM64 and x64
- Windows: x64

## Quick start

```js
import { GraphForge } from "@curatelabs/graphforge";
import { query } from "@curatelabs/graphforge/lib/query.mjs";

const forge = new GraphForge();
forge.execute("CREATE (:Person {name: 'Alice', age: 30})");

const table = query(
  forge,
  "MATCH (p:Person) RETURN p.name AS name, p.age AS age",
);

console.log(table.toArray());
```

`query(forge, cypher, params)` runs the statement and decodes the Arrow IPC
result into an apache-arrow `Table`. `forge.execute()` keeps returning the raw
Arrow IPC buffer for code that depends on that form; decode it with
`tableFromIPC` from `apache-arrow` as before. Writes (`CREATE`/`SET`/`DELETE`/…)
execute and return a summary.

## External decision results

Call `validateDecisionBatch(request)` with the provider-neutral v1 contract to
validate externally produced choices, ordered rubric scores, probabilities,
uncertainty and correlation. It returns Arrow IPC with the exact
`decision_result/1` schema; decode it with `tableFromIPC`. Import the
discriminated TypeScript input types from
`@curatelabs/graphforge/lib/decision`. The method only validates. Your
application owns producer execution, thresholds, review and any action.

## Graph inspection

```js
const labels = forge.labels();
const relationshipTypes = forge.relationshipTypes();
const totalNodes = forge.nodeCount();
const people = forge.nodeCount("Person");
const schema = tableFromIPC(forge.schema());
```

Names are sorted and include only values present in the committed graph.
`schema()` returns `label`, `node_count`, `rel_type`, and `rel_count`; label rows
come first, followed by relationship rows, with the unrelated columns null.

There is no generic `begin()` / `commit()` / `rollback()` surface. Each
`execute()` write is atomic. Use `publishCompositeTransaction()` when graph and
knowledge mutations must publish in one committed generation.

For persistent projects, create the project directory before passing its path
to `new GraphForge(path)`.

Choose an embedded write mode with the optional second constructor argument:

```js
const forge = new GraphForge(path, {
  writeMode: "optimistic_multi_writer",
  writeQueueCapacity: 64,
  maxRebaseAttempts: 3,
});
```

The stable mode names are `single_writer` (default), `queued_writer`, and
`optimistic_multi_writer`. Queue capacity is bounded to 1–65,536 and rebase
attempts to 0–32. Optimistic replay applies only to composite transactions.
GraphForge remains embedded; remote transports are separate extensions.

## Ontology lifecycle

The native binding exposes the Rust-owned #236 operations through
`inspectRuntimeCatalog()`, `suggestOntology()`, `validateOntology()`, and
`exportOntology()`. These inspect or derive explicit artifacts without changing
durable project authority. Issue #237 supplies the same thin Python/Node parity
and adds durable `workspaceOntology()`, `adoptOntology()`, and
`clearOntology()` operations.

These APIs are distinct from the repository CLI's `export` and `import`
commands, which move one complete portable project generation. Repository
interchange never implicitly inspects, suggests, validates, exports, adopts, or
clears an ontology.

## Documentation and support

- [Documentation](https://github.com/CurateLabs/graphforge/tree/main/docs)
- [Issue tracker](https://github.com/CurateLabs/graphforge/issues)
- [Source](https://github.com/CurateLabs/graphforge/tree/main/crates/graphforge-bindings-node)

## License

GraphForge is open source under the Apache License 2.0. See the
included `LICENSE` and `NOTICE` files for terms and attribution.

## Algorithm definitions

`rank` and `cluster` take their optional controls in one trailing options
object; existing defaults apply when fields are omitted. Rank accepts `via`,
`directed`, `writeProperty`, `damping`, `iterations` (including zero), and
`clusteringNormalization` (`"fagiolo"` or `"neighbor_edges"`). Cluster accepts
`via`, `directed`, `writeProperty`, `vectorProperty`, `synchronousIterations`
(including zero), and `initialLabelProperty`.

```js
forge.rank("Person", "pagerank", { directed: true, damping: 0.85, iterations: 20 });
forge.cluster("Person", "label_propagation", {
  directed: false,
  synchronousIterations: 10,
  initialLabelProperty: "external_id",
});
forge.rank("Person", "clustering_coefficient", {
  directed: true,
  clusteringNormalization: "neighbor_edges",
});
```

`initialLabelProperty` requires synchronous rounds and an Int64 property on
every selected vertex. Its values become the output labels, with ties resolved
to the smallest label. Without the property, initial labels are selected vertex
ordinals. Rank and cluster invocation descriptors accept the same controls,
including descriptors prepared from resolved belief projections. Rust owns the
algorithms and validates their semantics.
