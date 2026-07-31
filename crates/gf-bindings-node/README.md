# `@graphforge/node`

Native Node.js bindings for the GraphForge embedded graph engine. The package
loads the Rust implementation for the current platform; it does not include a
JavaScript fallback engine.

## Install

```bash
npm install @graphforge/node apache-arrow
```

Node.js 20 or newer is required. Prebuilt packages are published for:

- macOS: Apple silicon and Intel
- Linux glibc: ARM64 and x64
- Windows: x64

## Quick start

```js
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "@graphforge/node";

const forge = new GraphForge();
forge.execute("CREATE (:Person {name: 'Alice', age: 30})");

const result = forge.execute(
  "MATCH (p:Person) RETURN p.name AS name, p.age AS age",
);
const table = tableFromIPC(result);

console.log(table.toArray());
```

GraphForge returns Arrow IPC buffers from query and analyst-verb result
surfaces. Decode them with `apache-arrow` as shown above.

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
- [Source](https://github.com/CurateLabs/graphforge/tree/main/crates/gf-bindings-node)

## License

GraphForge is open source under the Apache License 2.0. See the
included `LICENSE` and `NOTICE` files for terms and attribution.
