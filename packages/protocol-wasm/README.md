# @curatelabs/graphforge-protocol-wasm

The Rust-owned GraphForge Hub validators as a standard WebAssembly ES module,
for Cloudflare Workers, Node, and browsers. A Hub applies the discovery v1 and
`graphforge-hub-publish/1` rules by calling this module; it never reimplements
them in TypeScript.

It validates bounded JSON control documents and bounded PNG/SVG previews. It
never loads a portable graph package or runs the engine.

## Install and load

```sh
npm install @curatelabs/graphforge-protocol-wasm
```

The module follows the `wasm-bindgen --target web` convention: instantiate it
once, then call the exports.

```js
// Node (20+): synchronous.
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import * as protocol from "@curatelabs/graphforge-protocol-wasm";
const wasm = createRequire(import.meta.url).resolve(
  "@curatelabs/graphforge-protocol-wasm/wasm",
);
protocol.initSync({ module: readFileSync(wasm) });

// Cloudflare Workers: the bundler imports the .wasm file as a WebAssembly.Module.
import wasm from "@curatelabs/graphforge-protocol-wasm/wasm";
import * as protocol from "@curatelabs/graphforge-protocol-wasm";
protocol.initSync({ module: wasm });

// Browsers and anywhere `fetch` can reach the file:
import init, * as protocol from "@curatelabs/graphforge-protocol-wasm";
await init({
  module_or_path: new URL("graphforge_protocol_wasm_bg.wasm", import.meta.url),
});
```

## Use

```js
const { document, canonical_json, digest } =
  protocol.validate_manifest(manifestText);
protocol.bind_summary(manifestText, summaryText);
const commitment = protocol.request_commitment({
  repository: { owner: "opendata", repository: "openalex" },
  intent_digest,
  objects, // [{ digest, length, media_type }], ascending by digest
  manifest_validator: digest,
  refs: ["main"],
  expected_revision: null,
});
```

A document argument is a JSON string, validated byte for byte (duplicate
members and oversize input are refused as in native Rust), or a plain object.
Results are plain objects.

Permanent item references use Core's shared grammar directly:

```js
protocol.parse_item_id("ONT_7K2M9X4Q8R3T5V1W");
// { id: "ont_7k2m9x4q8r3t5v1w",
//   uri: "https://graphforge.sh/ont_7k2m9x4q8r3t5v1w",
//   kind: "ontology_module" }
protocol.has_item_id_shape("abc_0000000000000000"); // true (reserved name)
```

`parse_item_id(reference)` accepts a bare ID or `https://graphforge.sh/<id>`,
folding ASCII case only. It recognizes `ont` (`ontology_module`) and `brg`
(`bridge_set`), rejects Crockford aliases `i`, `l`, `o`, `u`, unknown prefixes,
foreign URIs, extra paths, query strings and fragments, and throws a structured
`invalid_argument` error on failure. `has_item_id_shape(name)` returns a boolean:
any three lowercase letters followed by `_` and 16 lowercase Crockford base32
characters are reserved, including unknown prefixes. It does not normalize case
or accept a URI. Both calls work without entropy; ID generation is not exported.

**Boundary.** Rust never reads a JavaScript object. The package's `index.js`
turns an object argument into JSON text with `JSON.stringify` and passes only
strings and bytes to the WebAssembly exports, so an object and its JSON text
always get the same verdict, and a throwing proxy, getter or `toJSON` yields a
structured `invalid_argument` error without touching the module. A string must be
well-formed UTF-16 (a lone surrogate is refused rather than read as U+FFFD; in
an object it is escaped by `JSON.stringify` and refused by the validator like
the same text). Objects follow `JSON.stringify` (`Date` becomes its string,
`undefined` members are omitted, `-0` is `0`), except that a `bigint` is written
exactly and an integer above 2^53 given as a number (already rounded) or a
non-finite number is refused.

**Integers.** Every 64-bit integer in a result (`length`, `max_object_bytes`,
`node_count`, and every integer inside a free-form `extensions` value) is a
`bigint`, because a JavaScript number cannot hold a `u64` above 2^53 exactly;
the generated declarations say `bigint`. In an object argument pass a `bigint`
(or a safe integer) or send the value inside a JSON string.

**Results** are built from Rust data only: `null` fields are `null`, and a JSON
object (including `extensions`) is a plain object whose keys, `__proto__`
included, are own properties.

| Area           | Exports                                                                                                                                |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| Permanent IDs  | `parse_item_id`, `has_item_id_shape`                                                                                                   |
| Discovery      | `validate_manifest`, `validate_refs`, `validate_summary`, `validate_lineage`, `validate_repository_moved`                              |
| Binding        | `bind_summary`, `bind_lineage`, `bind_summary_lineage`, `bind_refs`, `ontology_module_object`, `research_version_object`               |
| Re-homing      | `rehome_repository`                                                                                                                    |
| Previews       | `validate_image`, `validate_preview`, `validate_preview_descriptors`, `manifest_preview_descriptors`                                   |
| Publish        | `validate_capabilities`, `validate_session_request`, `validate_commit_request`, `validate_receipt`, `validate_operation_status`        |
| Publish commit | `request_commitment`, `check_commit_manifest` (repository, exact inventory at template locations, `resolved_ref` in the advanced refs) |

`validate_manifest` and the other discovery exports take an optional
`DiscoveryLimitsOverride` (`max_response_bytes`, `max_refs`, ...) naming only
the bounds to change.

Every failure is thrown as a plain object `{ code, path?, version?, message }`
with the code Rust returns: `malformed_response`, `unsupported_future`,
`integrity_failure`, `limit_exceeded`, ... for discovery;
`invalid_input`, `auth_denied`, ... for publish documents; `preview.limit`,
`preview.unsafe_svg`, ... for previews. `invalid_argument` means the argument
was not the JSON value or string the export needs, or was an invalid permanent
item reference.

TypeScript declarations (`dist/graphforge_protocol_wasm.d.ts`) are generated
from the Rust wire types; nothing in them is written by hand.

## Footprint

Measured on the release build (`opt-level = "z"`, LTO), Node 22:

| Measure                                     | Value                                                         |
| ------------------------------------------- | ------------------------------------------------------------- |
| Delivered `.wasm` size                      | about 354 KB gzip, about 282 KB brotli (1.07 MB uncompressed) |
| Linear memory after instantiation           | about 1.3 MiB                                                 |
| `validate_manifest`, openalex (2.2 KB)      | about 0.3 ms                                                  |
| `validate_lineage`, largest corpus (4.5 KB) | about 0.5 ms                                                  |
| `request_commitment`                        | about 20 µs                                                   |

The compressed size is what a Worker or browser downloads, and it is far below
the Workers limits. Running Binaryen's `wasm-opt -Oz` over the module was tried
and is not applied: it shrinks the uncompressed file by 11% but grows it to
381 KB gzip and 299 KB brotli, because it removes repetition the compressor was
already using.

Where the bytes are (uncompressed function sizes by crate, from `twiggy`; approximate): `serde_json`
monomorphized for each wire type about 310 KB; `core`, `alloc` and `std` about
230 KB; compiler-generated glue and closures about 210 KB; Unicode and IDNA
tables and strings in the data segment about 208 KB; `graphforge-discovery` about
104 KB; `png` about 43 KB; `serde`, the adapter, `roxmltree`, `url`,
`graphforge-hub-publish`, `quick-xml` and `wasm-bindgen` about 14-35 KB each.

In a Worker (`tests/worker.test.mjs`, Miniflare 3 running workerd): linear memory
1.3 MiB (the isolate limit is 128 MB); the whole workerd process grew from about
66 MB to about 88 MB resident over 200 requests, runtime included; a request that
validates six documents and computes a commitment took 4-5 ms including the HTTP
round trip from Node. workerd does not expose CPU time, and a Worker's own timers
do not advance during computation, so per-request CPU is not measured there. Run
`node --expose-gc --test tests/*.test.mjs` to print the numbers for your machine
(`# {"wasm_bytes": ...}` and `# {"linear_memory_bytes": ...}` diagnostics).

A WebAssembly build aborts on a Rust panic, so a panic cannot be turned into a
thrown error there. The adapter calls only validators that return errors for
untrusted input, and `tests/boundary.test.mjs` feeds every export hostile values (throwing
proxies, getters, cycles, 100,000-deep objects) and confirms each fails with a
structured error and none traps.

## Build and test

Needs the Rust toolchain from `rust-toolchain.toml`, the
`wasm32-unknown-unknown` target, and a `wasm-bindgen` CLI whose version equals
the `wasm-bindgen` crate in `Cargo.lock`:

```sh
rustup target add wasm32-unknown-unknown
scripts/ci/install-wasm-bindgen.sh "$HOME/.wasm-bindgen"   # cargo install of the locked version
export PATH="$HOME/.wasm-bindgen/bin:$PATH"
make test-protocol-wasm
```

`pnpm install` also provides the pinned `miniflare` development dependency the
Worker test uses.

The tests replay the discovery v1 and hub-publish v1 corpora
(`docs/reference/`) through the built module and compare each result with the
outcome the corpus records from native Rust. The hub-publish corpus drives a
stateful reference Hub over HTTP, which does not exist in WebAssembly, so the
replay checks every control document the exchange carries against a verdict
that native Rust recorded for it (`accept` or the error code, with one mutated
copy of each kind) in `crates/graphforge-protocol-wasm/tests/hub-publish-documents.json`.
The module must accept exactly what native Rust accepts and refuse the rest with
the same code; the file is regenerated with
`GRAPHFORGE_UPDATE_PROTOCOL_WASM_EXPECTED=1 cargo test -p graphforge-protocol-wasm`.
The replay also recomputes each request commitment from the open and commit
requests and checks refs revisions against their ETag.
`cargo test -p graphforge-protocol-wasm` runs the same corpora and the openalex
fixture natively through the same engine functions.

## Running on Cloudflare Workers

The module needs no Node APIs. In a Worker, import the `.wasm` file as above and
call `initSync` once at module scope; `wrangler dev` or `wrangler deploy` bundles
it. `tests/worker.test.mjs` runs such a Worker (`tests/worker/worker.mjs`) in
Cloudflare's own workerd runtime through Miniflare, with the package's
specifiers mapped to its files as the `exports` map does, and checks that it
validates the openalex fixture and computes the request commitment and re-homed
revision the native Rust tests pin. It needs no account and no network, runs on
Linux, macOS and Windows (workerd ships for all three) and on Node 18 or newer
(Miniflare 3). A deployed Worker on Cloudflare's network remains a manual check.
