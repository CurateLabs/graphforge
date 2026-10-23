#!/usr/bin/env node
// Run the protocol conformance suite against a clean npm installation, using
// the matching source checkout only for tests, fixtures and native oracles.

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { readdirSync, readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { join, resolve } from "node:path";
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

const root = resolve(process.argv[2] ?? "");
const fixture = join(root, "tests", "fixtures", "hub", "generated", "v1");
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

// Reuse the full Node conformance/boundary suite against the installed package,
// never this checkout's build output. Worker qualification is run separately
// with Miniflare, which is a development dependency rather than a consumer one.
const tests = join(root, "packages", "protocol-wasm", "tests");
execFileSync(
  process.execPath,
  [
    "--expose-gc",
    "--test",
    ...readdirSync(tests)
      .filter(
        (name) => name.endsWith(".test.mjs") && name !== "worker.test.mjs",
      )
      .sort()
      .map((name) => join(tests, name)),
  ],
  {
    stdio: "inherit",
    env: {
      ...process.env,
      GRAPHFORGE_PROTOCOL_PACKAGE_ROOT: process.cwd(),
    },
  },
);
console.log("installed WebAssembly protocol package conformance: PASS");
