import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

import { GraphForge } from "../index.js";

const ROOT = fileURLToPath(new URL("../../..", import.meta.url));
const FIXTURE = join(ROOT, "tests/fixtures/multi-ontology-v1/certification-v1");

function authority(forge, operationUuid = randomUUID()) {
  const state = forge.ontologyAuthorityState();
  return {
    operationUuid,
    expectedProjectGenerationUuid: state.project_generation_uuid,
    expectedCompositionFingerprint: state.composition_fingerprint ?? undefined,
  };
}

function substitute(value, identities) {
  if (typeof value === "string" && identities[value])
    return structuredClone(identities[value]);
  if (Array.isArray(value))
    return value.map((item) => substitute(item, identities));
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [
        key,
        substitute(item, identities),
      ]),
    );
  return value;
}

async function certify() {
  const manifest = JSON.parse(
    readFileSync(join(FIXTURE, "certification.json"), "utf8"),
  );
  const root = mkdtempSync(join(tmpdir(), "graphforge-certification-node-"));
  const project = join(root, "project");
  mkdirSync(project);
  const forge = new GraphForge(project);
  const identities = {};
  for (const filename of manifest.modules) {
    const document = JSON.parse(readFileSync(join(FIXTURE, filename), "utf8"));
    const candidate = forge.createOntologyModule({ document });
    await forge.adoptOntologyModule({ authority: authority(forge), candidate });
    identities[`$${document.ontology_id.split("/").at(-1)}`] = candidate.id;
    if (filename === "genealogy-v1.json")
      forge.execute(
        "CREATE (:Person {full_name: 'Ada Lovelace', birth_year: 1815})",
      );
  }
  for (const filename of manifest.bridges) {
    const document = substitute(
      JSON.parse(readFileSync(join(FIXTURE, filename), "utf8")),
      identities,
    );
    const candidate = forge.createOntologyBridge(document);
    await forge.adoptOntologyBridge({ authority: authority(forge), candidate });
  }

  const before = forge.ontologyAuthorityState().composition_fingerprint;
  const target = JSON.parse(
    readFileSync(join(FIXTURE, manifest.migration_target), "utf8"),
  );
  const migrationAuthority = authority(forge);
  const request = {
    authority: migrationAuthority,
    selector: { exact: identities.$genealogy },
    document: target,
  };
  const preview = forge.previewMigrateOntologyModule(request);
  assert.ok(preview.plan.retained_rows_scanned > 0);
  const receipt = await forge.migrateOntologyModule({ ...request, preview });
  forge.close();

  const reopened = new GraphForge(project);
  const report = reopened.multiOntologyCertificationReport(
    before,
    receipt.plan_digest,
    receipt.retained_rows_scanned,
  );
  reopened.close();
  assert.equal(report.surface, "node");
  assert.deepEqual(report.retained_data, {
    rows_scanned: receipt.retained_rows_scanned,
    name: "Ada Lovelace",
    birth_year: 1815,
  });
  const output = process.env.GRAPHFORGE_MULTI_ONTOLOGY_CERTIFICATION_REPORT;
  if (output) writeFileSync(output, `${JSON.stringify(report)}\n`);
  return report;
}

test("retained-data certification uses Rust authority", async () => {
  const report = await certify();
  assert.notEqual(report.composition_before, report.composition_after);
  assert.equal(report.module_ids.length, 6);
  assert.equal(report.bridge_ids.length, 3);
});
