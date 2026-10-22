#!/usr/bin/env node
// Clean-install oracle for the packed WebAssembly protocol package: load the
// installed module the way a Node consumer does and validate a document.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

const require = createRequire(join(process.cwd(), "package.json"));
const protocol = await import(
  pathToFileURL(require.resolve("@curatelabs/graphforge-protocol-wasm"))
);
protocol.initSync({
  module: readFileSync(
    require.resolve("@curatelabs/graphforge-protocol-wasm/wasm"),
  ),
});

const fixture = join(
  process.argv[2] ?? "",
  "tests",
  "fixtures",
  "hub",
  "generated",
  "v1",
);
const manifest = readFileSync(join(fixture, "manifest.json"), "utf8");
const { digest } = protocol.validate_manifest(manifest);
assert.equal(
  JSON.parse(readFileSync(join(fixture, "fixture.json"), "utf8"))
    .manifest_digest,
  digest,
);
assert.throws(
  () => protocol.validate_manifest("{"),
  (error) => error.code === "malformed_response",
);
console.log("packed WebAssembly protocol package: PASS");
