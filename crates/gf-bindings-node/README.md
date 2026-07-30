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

## Documentation and support

- [Documentation](https://curatelabs.github.io/graphforge-legecy/)
- [Issue tracker](https://github.com/CurateLabs/graphforge-x/issues)
- [Source](https://github.com/CurateLabs/graphforge-x/tree/main/crates/gf-bindings-node)

## License

GraphForge is source-available under the Business Source License 1.1. See the
included `LICENSE` and `NOTICE` files for terms and attribution.
