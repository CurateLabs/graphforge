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
assert.match(help, /ontology modules/i);
console.log("multi-ontology packed Node package and CLI binary: PASS");
