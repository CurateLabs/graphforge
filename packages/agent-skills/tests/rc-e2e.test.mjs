import assert from "node:assert/strict";
import {
  mkdtempSync,
  readFileSync,
  realpathSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { createMockProject } from "../rc/mock-graphforge.js";
import { redactEvidence } from "../rc/redact.js";
import {
  DESIGNED_ONLY_SURFACE,
  assertNoDesignedOnlyReferences,
  runAnalystScenario,
  runDeveloperScenario,
} from "../rc/scenarios.js";

const here = dirname(fileURLToPath(import.meta.url));
const goldenDir = join(here, "goldens");
const updateGoldens = process.env.UPDATE_RC_GOLDENS === "1";

function loadGolden(name) {
  return JSON.parse(readFileSync(join(goldenDir, name), "utf8"));
}

test("skill sources reject Designed-only surface references", () => {
  assertNoDesignedOnlyReferences();
  assert.equal(DESIGNED_ONLY_SURFACE.startsWith("CheckpointView."), true);
});

test("redaction strips absolute paths and timestamps", () => {
  const redacted = redactEvidence({
    path: "/var/folders/ab/tmp/project",
    nested: { when: "2026-07-27T12:34:56.789Z", keep: "stable" },
    transaction_cutoff_micros: 1234567890,
  });
  assert.equal(redacted.path, "<redacted-path>");
  assert.equal(redacted.nested.when, "<redacted-timestamp>");
  assert.equal(redacted.nested.keep, "stable");
  assert.equal(redacted.transaction_cutoff_micros, "<redacted-micros>");
});

test("analyst-agent RC scenario matches golden output", async () => {
  const projectPath = realpathSync(
    mkdtempSync(join(tmpdir(), "gf-agent-rc-analyst-")),
  );
  const { GraphForge, seedCompetingHypotheses, tableFromIPC } =
    createMockProject(projectPath);
  const evidence = await runAnalystScenario({
    GraphForge,
    projectPath,
    seedCompetingHypotheses,
    tableFromIPC,
  });
  assert.equal(evidence.reopen.marker_matches, true);
  assert.equal(evidence.belief.competing_member_count, 2);
  assert.equal(evidence.belief.superseded_present, true);
  assert.equal(evidence.belief.selected_assertion_uuid, null);
  assert.equal(evidence.narration.has_projection_descriptors, true);
  if (updateGoldens) {
    writeFileSync(
      join(goldenDir, "analyst-agent.json"),
      `${JSON.stringify(evidence, null, 2)}\n`,
    );
  }
  assert.deepEqual(evidence, loadGolden("analyst-agent.json"));
});

test("developer-agent RC scenario matches golden output", async () => {
  const projectPath = realpathSync(
    mkdtempSync(join(tmpdir(), "gf-agent-rc-developer-")),
  );
  const { GraphForge, tableFromIPC } = createMockProject(projectPath);
  const evidence = await runDeveloperScenario({
    GraphForge,
    projectPath,
    tableFromIPC,
  });
  assert.equal(evidence.reopen.marker_matches, true);
  assert.equal(evidence.errors.subprocess, "GF_AGENT_SUBPROCESS_UNSUPPORTED");
  assert.equal(
    evidence.errors.missing_capability,
    "GF_AGENT_CAPABILITY_MISSING",
  );
  assert.equal(
    evidence.errors.designed_only,
    "GF_AGENT_CAPABILITY_UNSUPPORTED",
  );
  assert.equal(evidence.arrow_json.encoded.includes("SECRET"), false);
  if (updateGoldens) {
    writeFileSync(
      join(goldenDir, "developer-agent.json"),
      `${JSON.stringify(evidence, null, 2)}\n`,
    );
  }
  assert.deepEqual(evidence, loadGolden("developer-agent.json"));
});
