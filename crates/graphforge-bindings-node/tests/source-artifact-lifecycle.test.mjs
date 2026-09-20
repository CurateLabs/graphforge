// Source and Artifact lifecycle remains Rust-authoritative until binding parity (#1349).

import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { test } from "node:test";

const require = createRequire(import.meta.url);
let { GraphForge } = require("../index.js");
if (typeof GraphForge !== "function") {
  ({ GraphForge } = require("../graphforge.node"));
}

const RUST_ONLY_METHODS = [
  "registerSource",
  "registerArtifact",
  "source",
  "artifact",
  "listSources",
  "listArtifacts",
];

const RUST_ONLY_LIFECYCLE_METHODS = [
  "researchLineage",
  "setPreferredArtifact",
  "replacementImpact",
  "retentionDependencyClosure",
];

test("source and artifact lifecycle remains Rust-authoritative until binding parity", () => {
  const graph = new GraphForge();
  for (const method of RUST_ONLY_METHODS) {
    assert.equal(
      typeof graph[method],
      "undefined",
      `unexpected Node binding for ${method}`,
    );
  }
});

test("lifecycle query and preference APIs remain Rust-authoritative until binding parity", () => {
  const graph = new GraphForge();
  for (const method of RUST_ONLY_LIFECYCLE_METHODS) {
    assert.equal(
      typeof graph[method],
      "undefined",
      `unexpected Node binding for ${method}`,
    );
  }
});
