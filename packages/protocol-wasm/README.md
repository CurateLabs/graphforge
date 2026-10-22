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
  repository: { owner: "openalex", repository: "openalex" },
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

**Integers.** Every 64-bit integer in a result (`length`, `max_object_bytes`,
`node_count`, ...) is a `bigint`, because a JavaScript number cannot hold a
`u64` above 2^53 exactly; the generated declarations say `bigint`. In an object
argument a `bigint` is passed through with every digit, a safe integer or a
fraction is accepted as a number, and an integer above 2^53 given as a number is
**refused** with `invalid_argument` (it is already rounded, so accepting it would
validate a different value). Send such a value as a `bigint` or inside a JSON
string. Objects must be plain JSON data: functions, `Date`, `Map`, class
instances and `NaN` are refused rather than reshaped.

| Area           | Exports                                                                                                                                |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
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
was not the JSON value or string the export needs.

TypeScript declarations (`dist/graphforge_protocol_wasm.d.ts`) are generated
from the Rust wire types; nothing in them is written by hand.

## Footprint

Measured on the release build (`opt-level = "z"`, LTO, no `wasm-opt`), Node 22:

| Measure                                     | Value                 |
| ------------------------------------------- | --------------------- |
| `.wasm` file                                | 1.06 MB (350 KB gzip) |
| Linear memory after instantiation           | about 1.3 MiB         |
| `validate_manifest`, openalex (2.2 KB)      | about 0.3 ms          |
| `validate_lineage`, largest corpus (4.5 KB) | about 0.5 ms          |
| `request_commitment`                        | about 20 µs           |

About 200 KB of the file is Unicode and IDNA tables from the `url` crate that
location validation needs. Workers compressed-size limits are well above the
gzip size; the 1 ms of CPU a typical validation uses is far below the Workers
CPU limits. Run `node --expose-gc --test tests/*.test.mjs` to print the same
numbers for your machine (`# {"wasm_bytes": ...}` diagnostic).

A WebAssembly build aborts on a Rust panic, so a panic cannot be turned into a
thrown error there. The adapter calls only validators that return errors for
untrusted input, and `tests/openalex.test.mjs` feeds every document export
hostile values to confirm none traps.

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

The module needs no Node APIs. In a Worker, import the `.wasm` file as above
and call `initSync` once at module scope; `wrangler dev` or `wrangler deploy`
bundles it. Workers are not available in this repository's CI, so the Worker
run of the openalex acceptance case (`tests/openalex.test.mjs`) is a manual
step recorded on issue #1766.
