// #1360 — the package must resolve from an ES module and from CommonJS, and
// the declared napi export surface must match what the built addon exports.
//
// The Bazel Binding RC lane cannot run `napi build`, so it synthesizes
// index.js/index.d.ts from napi-export-surface.json. These tests run against
// the napi-generated loader and fail closed when that manifest drifts.

import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { GraphForge, version } from "../index.js";

const here = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);
const declared = JSON.parse(
  readFileSync(join(here, "..", "napi-export-surface.json"), "utf8"),
).exports;

test("a named ES module import resolves against the built package", () => {
  assert.equal(typeof GraphForge, "function");
  assert.equal(typeof version(), "string");
  const forge = new GraphForge();
  forge.close();
});

test("a CommonJS require resolves the same named exports", () => {
  const binding = require("../index.js");
  assert.equal(typeof binding.GraphForge, "function");
  assert.equal(binding.GraphForge, GraphForge);
  assert.equal(typeof binding.version(), "string");
  const forge = new binding.GraphForge();
  forge.close();
});

test("the declared napi export surface matches the built addon", () => {
  const binding = require("../index.js");
  assert.deepEqual(
    Object.keys(declared).sort(),
    Object.keys(binding).sort(),
    "napi-export-surface.json drifted from the built addon; the Bazel " +
      "Binding RC loader is generated from it",
  );
  for (const [name, kind] of Object.entries(declared)) {
    if (kind === "value") {
      assert.notEqual(typeof binding[name], "function", name);
    } else {
      assert.equal(typeof binding[name], "function", name);
    }
  }
});

test("the declared export kinds match the generated TypeScript surface", () => {
  const declarations = readFileSync(join(here, "..", "index.d.ts"), "utf8");
  for (const [name, kind] of Object.entries(declared)) {
    const pattern =
      kind === "class"
        ? new RegExp(`^export declare class ${name}[\\s<{]`, "m")
        : kind === "function"
          ? new RegExp(`^export declare function ${name}[\\s<(]`, "m")
          : new RegExp(`^export declare (?:const|enum) ${name}[\\s<:{]`, "m");
    assert.match(declarations, pattern, `${name} is not declared as ${kind}`);
  }
});
