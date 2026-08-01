#!/usr/bin/env node
/**
 * Executable analyst-agent example.
 *
 * Shares scenario source with `tests/rc-e2e.test.mjs` and the native RC runner.
 *
 * Usage (mock, no native binding):
 *   node examples/analyst-agent.mjs
 *
 * Usage (native):
 *   GRAPHFORGE_NODE_MODULE=/path/to/@curatelabs/graphforge/index.js \
 *     node examples/analyst-agent.mjs --native
 */

import { mkdtempSync, realpathSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import { createMockProject } from "../rc/mock-graphforge.js";
import {
  prepareSearchIndexNative,
  seedCompetingHypothesesNative,
} from "../rc/native-hooks.js";
import { runAnalystScenario } from "../rc/scenarios.js";

const native = process.argv.includes("--native");
const projectPath = realpathSync(
  mkdtempSync(join(tmpdir(), "gf-agent-example-analyst-")),
);
const packageRoot = dirname(
  fileURLToPath(new URL("../package.json", import.meta.url)),
);

let GraphForge;
let tableFromIPC;
let seedCompetingHypotheses;
let prepareSearchIndex;

if (native) {
  const modulePath =
    process.env.GRAPHFORGE_NODE_MODULE ??
    join(packageRoot, "../../crates/graphforge-bindings-node/index.js");
  ({ GraphForge } = await import(pathToFileURL(modulePath).href));
  const require = createRequire(join(dirname(modulePath), "package.json"));
  ({ tableFromIPC } = require("apache-arrow"));
  seedCompetingHypotheses = seedCompetingHypothesesNative;
  prepareSearchIndex = prepareSearchIndexNative;
} else {
  ({ GraphForge, seedCompetingHypotheses, tableFromIPC } =
    createMockProject(projectPath));
}

const evidence = await runAnalystScenario({
  GraphForge,
  prepareSearchIndex,
  projectPath,
  seedCompetingHypotheses,
  tableFromIPC,
});
process.stdout.write(`${JSON.stringify(evidence, null, 2)}\n`);
