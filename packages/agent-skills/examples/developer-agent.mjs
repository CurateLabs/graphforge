#!/usr/bin/env node
/**
 * Executable developer-agent example.
 *
 * Shares scenario source with `tests/rc-e2e.test.mjs` and the native RC runner.
 *
 * Usage (mock):
 *   node examples/developer-agent.mjs
 *
 * Usage (native):
 *   GRAPHFORGE_NODE_MODULE=/path/to/@graphforge/node/index.js \
 *     node examples/developer-agent.mjs --native
 */

import { mkdtempSync, realpathSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { createMockProject } from "../rc/mock-graphforge.js";
import { runDeveloperScenario } from "../rc/scenarios.js";

const native = process.argv.includes("--native");
const projectPath = realpathSync(
  mkdtempSync(join(tmpdir(), "gf-agent-example-developer-")),
);
const packageRoot = dirname(
  fileURLToPath(new URL("../package.json", import.meta.url)),
);

let GraphForge;
let tableFromIPC;

if (native) {
  const modulePath =
    process.env.GRAPHFORGE_NODE_MODULE ??
    join(packageRoot, "../../crates/graphforge-bindings-node/index.js");
  ({ GraphForge } = await import(pathToFileURL(modulePath).href));
  const require = createRequire(join(dirname(modulePath), "package.json"));
  ({ tableFromIPC } = require("apache-arrow"));
} else {
  ({ GraphForge, tableFromIPC } = createMockProject(projectPath));
}

const evidence = await runDeveloperScenario({
  GraphForge,
  projectPath,
  tableFromIPC,
});
process.stdout.write(`${JSON.stringify(evidence, null, 2)}\n`);
