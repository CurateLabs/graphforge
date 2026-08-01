import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const packageRoot = new URL("../", import.meta.url);
const temporary = mkdtempSync(join(tmpdir(), "graphforge-agent-skills-"));

function pack(destination) {
  mkdirSync(destination);
  const result = JSON.parse(
    execFileSync(
      "npm",
      [
        "pack",
        fileURLToPath(packageRoot),
        "--pack-destination",
        destination,
        "--ignore-scripts",
        "--json",
      ],
      { encoding: "utf8" },
    ),
  )[0];
  const artifact = join(destination, result.filename);
  return {
    artifact,
    files: result.files.map(({ path }) => path).sort(),
    sha256: createHash("sha256").update(readFileSync(artifact)).digest("hex"),
  };
}

const first = pack(join(temporary, "pack-one"));
const second = pack(join(temporary, "pack-two"));
assert.deepEqual(second.files, first.files);
assert.equal(second.sha256, first.sha256);
assert.deepEqual(first.files, [
  "LICENSE",
  "NOTICE",
  "README.md",
  "adapter/index.js",
  "bin/graphforge-agent-skills.js",
  "compatibility.json",
  "package.json",
  "schemas/README.md",
  "schemas/input-envelope-v1.json",
  "schemas/output-envelope-v1.json",
  "schemas/skill-manifest-v1.json",
  "schemas/validator.js",
  "skills/README.md",
  "skills/bootstrap/manifest.json",
  "skills/build-knowledge/manifest.json",
  "workflows/index.js",
]);

const consumer = join(temporary, "consumer");
mkdirSync(consumer);
writeFileSync(
  join(consumer, "package.json"),
  `${JSON.stringify({ name: "offline-smoke", private: true })}\n`,
);
execFileSync(
  "npm",
  [
    "install",
    "--offline",
    "--ignore-scripts",
    "--no-audit",
    "--no-fund",
    first.artifact,
  ],
  { cwd: consumer, stdio: "inherit" },
);
const output = execFileSync(
  "npx",
  [
    "--offline",
    "--no-install",
    "graphforge-agent-skills",
    "compatibility",
    "--json",
  ],
  { cwd: consumer, encoding: "utf8" },
);
const compatibility = JSON.parse(output);
const packageMetadata = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8"),
);
assert.equal(compatibility.graphforge_release, packageMetadata.version);
assert.equal(
  compatibility.graphforge_version_range,
  packageMetadata.graphforgeCompatibility.range,
);
assert.deepEqual(compatibility.security_contract, {
  max_value_depth: 16,
  max_value_entries: 4096,
  max_value_string_length: 4096,
  project_symlinks: "rejected",
  subprocess: "unsupported",
});
for (const path of first.files.filter((path) => path.endsWith(".js"))) {
  const source = readFileSync(
    join(consumer, "node_modules/@curatelabs/graphforge-agent-skills", path),
    "utf8",
  );
  assert.equal(/(?:node:)?child_process/.test(source), false, path);
}

const adapterOutput = execFileSync(
  process.execPath,
  [
    "--input-type=module",
    "--eval",
    "import { ADAPTER_CONTRACT_VERSION, stableJson } from '@curatelabs/graphforge-agent-skills'; process.stdout.write(stableJson({ version: ADAPTER_CONTRACT_VERSION }));",
  ],
  { cwd: consumer, encoding: "utf8" },
);
assert.equal(adapterOutput, '{"version":1}\n');

const workflowOutput = execFileSync(
  process.execPath,
  [
    "--input-type=module",
    "--eval",
    "import { bootstrapProject, buildKnowledge, resolveBeliefSubject, narrateBeliefRecords, dispatchRecordedNeutralAnalysis, exploreGraph, retrieveAnalyze } from '@curatelabs/graphforge-agent-skills/workflows'; process.stdout.write(JSON.stringify([typeof bootstrapProject, typeof buildKnowledge, typeof resolveBeliefSubject, typeof narrateBeliefRecords, typeof dispatchRecordedNeutralAnalysis, typeof exploreGraph, typeof retrieveAnalyze]));",
  ],
  { cwd: consumer, encoding: "utf8" },
);
assert.equal(
  workflowOutput,
  '["function","function","function","function","function","function","function"]',
);
const securityOutput = execFileSync(
  process.execPath,
  [
    "--input-type=module",
    "--eval",
    "import { requestSubprocess } from '@curatelabs/graphforge-agent-skills'; try { requestSubprocess({command:'SECRET_TOKEN_DO_NOT_ECHO'}); } catch (error) { process.stdout.write(JSON.stringify(error)); }",
  ],
  { cwd: consumer, encoding: "utf8" },
);
assert.equal(
  JSON.parse(securityOutput).code,
  "GF_AGENT_SUBPROCESS_UNSUPPORTED",
);
assert.equal(securityOutput.includes("SECRET_TOKEN_DO_NOT_ECHO"), false);

const schemaOutput = execFileSync(
  process.execPath,
  [
    "--input-type=module",
    "--eval",
    "import { validateSkillInput } from '@curatelabs/graphforge-agent-skills/schemas'; process.stdout.write(JSON.stringify(validateSkillInput({schema_version:2})));",
  ],
  { cwd: consumer, encoding: "utf8" },
);
const schemaValidation = JSON.parse(schemaOutput);
assert.equal(schemaValidation.valid, false);
assert.equal(
  schemaValidation.diagnostics.some(
    ({ code }) => code === "incompatible_version",
  ),
  true,
);

process.stdout.write(
  `${JSON.stringify({ artifact_sha256: first.sha256, files: first.files })}\n`,
);
