// #1360 — the package must resolve from an ES module and from CommonJS.

import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { test } from "node:test";
import { GraphForge, version } from "../index.js";

const require = createRequire(import.meta.url);

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
