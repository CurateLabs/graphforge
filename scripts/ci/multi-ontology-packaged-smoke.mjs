#!/usr/bin/env node
// Clean-install oracle for the packed Node binding and CLI artifacts (#842).

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createRequire } from "node:module";
import { join } from "node:path";

const require = createRequire(join(process.cwd(), "package.json"));
const { GraphForge } = require("@curatelabs/graphforge");
const forge = new GraphForge();
try {
  assert.deepEqual(forge.ontologyModules(), []);
  assert.deepEqual(forge.ontologyBridges(), []);
  assert.equal(typeof forge.portableOntologyStaging, "function");
} finally {
  forge.close();
}

const cliModule = require.resolve("@curatelabs/graphforge-cli");
const cli = join(cliModule, "..", "..", "bin", "graphforge.js");
const help = execFileSync(process.execPath, [cli, "ontology", "module", "list", "--help"], {
  encoding: "utf8",
});
// #1369 — `ontology module list` carries no clap `about`, so its own help
// has never contained "ontology modules"; the phrase belongs to the parent
// `ontology module` command. This job has not run since the four platform
// lanes went red, so the stale assertion went unnoticed. Assert the usage
// line the Rust CLI actually emits for the leaf command instead.
assert.match(help, /Usage: graphforge ontology module list/i);
console.log("multi-ontology packed Node package and CLI binary: PASS");
