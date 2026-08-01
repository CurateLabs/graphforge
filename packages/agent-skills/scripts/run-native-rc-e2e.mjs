#!/usr/bin/env node
/**
 * Native release-candidate E2E for #2422.
 *
 * 1. npm-pack the package and offline-install into a clean temp directory
 * 2. Run analyst + developer scenarios against a local `@curatelabs/graphforge` build
 * 3. Write redacted evidence with commit SHA and runtime versions
 *
 * Usage:
 *   GRAPHFORGE_NODE_MODULE=$PWD/crates/graphforge-bindings-node/index.js \
 *     node packages/agent-skills/scripts/run-native-rc-e2e.mjs \
 *       --commit-sha $(git rev-parse HEAD) \
 *       --evidence /tmp/agent-skills-rc-e2e.json
 */

import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  mkdirSync,
  mkdtempSync,
  readFileSync,
  realpathSync,
  writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

import {
  prepareSearchIndexNative,
  seedCompetingHypothesesNative,
} from "../rc/native-hooks.js";
import {
  evidenceEnvelope,
  runAnalystScenario,
  runDeveloperScenario,
} from "../rc/scenarios.js";

const args = Object.fromEntries(
  process.argv.slice(2).reduce((pairs, value, index, values) => {
    if (value.startsWith("--")) pairs.push([value.slice(2), values[index + 1]]);
    return pairs;
  }, []),
);

const packageRoot = dirname(
  fileURLToPath(new URL("../package.json", import.meta.url)),
);
const repoRoot = join(packageRoot, "../..");
const commitSha =
  args["commit-sha"] ??
  execFileSync("git", ["-C", repoRoot, "rev-parse", "HEAD"], {
    encoding: "utf8",
  }).trim();
const evidencePath =
  args.evidence ??
  join(repoRoot, "target/release-workflows/agent-skills/rc-e2e.json");

const modulePath =
  process.env.GRAPHFORGE_NODE_MODULE ??
  join(repoRoot, "crates/graphforge-bindings-node/index.js");
const { GraphForge, version } = await import(pathToFileURL(modulePath).href);
const require = createRequire(join(dirname(modulePath), "package.json"));
const { tableFromIPC } = require("apache-arrow");
const nodePackage = JSON.parse(
  readFileSync(join(dirname(modulePath), "package.json"), "utf8"),
);
const skillsPackage = JSON.parse(
  readFileSync(join(packageRoot, "package.json"), "utf8"),
);

const temporary = mkdtempSync(join(tmpdir(), "graphforge-agent-skills-rc-"));
const packDir = join(temporary, "pack");
mkdirSync(packDir);
const packJson = JSON.parse(
  execFileSync(
    "npm",
    [
      "pack",
      packageRoot,
      "--pack-destination",
      packDir,
      "--ignore-scripts",
      "--json",
    ],
    { encoding: "utf8" },
  ),
)[0];
const artifact = join(packDir, packJson.filename);
const pack = {
  filename: packJson.filename,
  files: packJson.files.map(({ path }) => path).sort(),
  sha256: createHash("sha256").update(readFileSync(artifact)).digest("hex"),
};
assert.ok(pack.files.includes("LICENSE"));
assert.ok(pack.files.includes("NOTICE"));
assert.ok(pack.files.includes("compatibility.json"));
assert.ok(pack.files.includes("workflows/index.js"));

const consumer = join(temporary, "consumer");
mkdirSync(consumer);
writeFileSync(
  join(consumer, "package.json"),
  `${JSON.stringify({ name: "agent-skills-rc-e2e", private: true })}\n`,
);
execFileSync(
  "npm",
  [
    "install",
    "--offline",
    "--ignore-scripts",
    "--no-audit",
    "--no-fund",
    artifact,
  ],
  { cwd: consumer, stdio: "inherit" },
);

const installedWorkflows = await import(
  pathToFileURL(
    join(
      consumer,
      "node_modules/@curatelabs/graphforge-agent-skills/workflows/index.js",
    ),
  ).href
);
for (const name of [
  "bootstrapProject",
  "buildKnowledge",
  "exploreGraph",
  "retrieveAnalyze",
  "resolveBeliefSubject",
  "narrateBeliefRecords",
]) {
  assert.equal(typeof installedWorkflows[name], "function", name);
}

const compatibility = JSON.parse(
  execFileSync(
    "npx",
    [
      "--offline",
      "--no-install",
      "graphforge-agent-skills",
      "compatibility",
      "--json",
    ],
    { cwd: consumer, encoding: "utf8" },
  ),
);
const packageMetadata = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8"),
);
assert.equal(compatibility.graphforge_release, packageMetadata.version);

const analystPath = realpathSync(mkdtempSync(join(temporary, "analyst-")));
const developerPath = realpathSync(mkdtempSync(join(temporary, "developer-")));

const analyst = await runAnalystScenario({
  GraphForge,
  prepareSearchIndex: prepareSearchIndexNative,
  projectPath: analystPath,
  seedCompetingHypotheses: seedCompetingHypothesesNative,
  tableFromIPC,
});
const developer = await runDeveloperScenario({
  GraphForge,
  projectPath: developerPath,
  tableFromIPC,
});

assert.equal(analyst.reopen.marker_matches, true);
assert.equal(analyst.belief.competing_member_count >= 2, true);
assert.equal(developer.reopen.marker_matches, true);
assert.equal(developer.errors.subprocess, "GF_AGENT_SUBPROCESS_UNSUPPORTED");

const evidence = evidenceEnvelope({
  analyst,
  commitSha,
  developer,
  graphforgeVersion: version?.() ?? nodePackage.version,
  nodeVersion: process.version,
  packageVersion: skillsPackage.version,
  pack: {
    ...pack,
    compatibility,
    install: "npm install --offline --ignore-scripts <tarball>",
  },
});

mkdirSync(dirname(evidencePath), { recursive: true });
writeFileSync(evidencePath, `${JSON.stringify(evidence, null, 2)}\n`);
process.stdout.write(
  `${JSON.stringify({ evidence: evidencePath, commit_sha: commitSha })}\n`,
);
