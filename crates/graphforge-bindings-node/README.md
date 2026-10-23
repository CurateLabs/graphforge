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
(including zero), and `initialLabelProperty`. An unknown option key (for example a misspelled
`iteratons`) is refused with a validation error that lists the accepted keys,
rather than running with defaults.

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

## Typed Hub and research workflow

Import `HubProject` from `@curatelabs/graphforge/lib/hub.mjs`; its declarations
ship in `lib/hub.d.mts`. It calls the same in-process Rust CLI as `gf`,
synchronously. Each verb opens the project afresh; close other writable handles
first. Control results retain the native field names and UUIDs. Native graph
reads continue to return Arrow IPC for `tableFromIPC` or other Arrow consumers.

Public clone/query needs a read-capable Hub. `push`/`publish` and collaborator
`pull` require a separately verified conforming write-capable Hub and a scoped
publish token. Production graphforge.sh is not a qualified hosted publishing
target at launch. Tests use native ReferenceHub (Hub publish/1 and discovery/1),
which does not establish production hosted publishing.

```js
import { GraphForge } from "@curatelabs/graphforge";
import { HubProject, HubWorkflowError } from "@curatelabs/graphforge/lib/hub.mjs";
import { tableFromIPC } from "apache-arrow";

// Set MY_HUB_TOKEN using your target Hub's credential flow.
const work = new HubProject("./research-copy", { tokenEnv: "MY_HUB_TOKEN" });
const cloned = work.clone("https://your-write-capable-hub.example/owner/repository", { ref: "main" });
const credit = { author: "Alice <alice@example.org>" };
const first = work.commit("First score", "MATCH (n:Item) SET n.score=1", credit);
const second = work.commit("Second score", "MATCH (n:Item) SET n.score=2", credit);
work.tag("v1");
const receipt = work.push(); // publish() is the same current-Branch operation
// After a collaborator pushes a compatible change:
const pulled = work.pull(undefined, credit);
console.log(work.log(undefined, { all: true }));
const reopened = new GraphForge(work.path);
try {
  console.log(tableFromIPC(reopened.queryResearchVersion(first.version_uuid,
    "MATCH (n:Item) RETURN n.score")).toArray());
} finally {
  reopened.close();
}
```

`clone` returns `CloneResult`; `push`/`publish` returns `PublishResult`; `pull`
returns `PullResult | UpToDateResult`; `log` returns `LogEntry[]`.
`log(undefined, { remote: true })` reads published history; `log("v1")` resolves
an immutable local tag. Mutations accept `operationUuid` for exact retry identity.

`HubWorkflowError` retains `envelope`, `details`, `exitCode`, `stdout` and
`stderr`. Its `code` selects native `details.semantic_code`, falling back to
`nativeCode` (the outer code). Retained `error.diagnostics` may carry more
specific multi-ontology codes; inspect those before classifying an outer code.

The latest native Core version warnings are available in `work.warnings`,
including when the native CLI wraps a history array with its warning envelope.
