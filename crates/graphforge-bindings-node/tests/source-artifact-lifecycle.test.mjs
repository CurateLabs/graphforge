// Source and Artifact lifecycle binding parity (#1349).

import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function openAdmitted(path) {
  try {
    return new GraphForge(path);
  } catch (error) {
    if (error.code === "GF_UNSUPPORTED_FILESYSTEM") {
      return null;
    }
    throw error;
  }
}

function uuidFromBytes(value) {
  const hex = Buffer.from(value).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function uuidColumn(table, name) {
  const column = table.getChild(name);
  return Array.from({ length: table.numRows }, (_, index) =>
    uuidFromBytes(column.get(index)),
  );
}

test("source and artifact lifecycle survives reopen through the Node surface", async () => {
  const root = mkdtempSync(join(tmpdir(), "gf-source-artifact-"));
  try {
    if (openAdmitted(root) === null) {
      return;
    }

    const sourceUuid = "018f0f4e-7b8c-7000-8000-000000001301";
    const scanUuid = "018f0f4e-7b8c-7000-8000-000000001302";
    const ocrUuid = "018f0f4e-7b8c-7000-8000-000000001303";
    const preferenceUuid = "018f0f4e-7b8c-7000-8000-000000001304";

    const forge = new GraphForge(root);
    await forge.enableCapability({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000001100",
      capabilityId: "provenance",
      capabilityVersion: 1,
    });
    await forge.enableCapability({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000001101",
      capabilityId: "knowledge",
      capabilityVersion: 1,
    });
    await forge.registerSource({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000001201",
      sourceUuid,
      label: "Codex A",
      sourceKind: "manuscript",
    });
    await forge.registerArtifact({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000001202",
      artifactUuid: scanUuid,
      sourceUuid,
      artifactKind: "raw_scan",
      mediaType: "image/tiff",
      payload: { kind: "local_bytes", bytes: Buffer.from("scan bytes") },
    });
    await forge.registerArtifact({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000001203",
      artifactUuid: ocrUuid,
      sourceUuid,
      artifactKind: "ocr_text",
      mediaType: "text/plain",
      payload: { kind: "local_bytes", bytes: Buffer.from("ocr text") },
      derivationInputs: [{ inputUuid: scanUuid, inputKind: "artifact" }],
    });
    await forge.setPreferredArtifact({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000001204",
      preferenceEventUuid: preferenceUuid,
      sourceUuid,
      artifactUuid: scanUuid,
      reason: "initial preferred scan",
    });

    const impact = tableFromIPC(
      await forge.replacementImpact({
        sourceUuid,
        artifactUuid: ocrUuid,
      }),
    );
    assert.equal(impact.numRows, 2);
    const impacted = uuidColumn(impact, "artifact_uuid");
    assert.ok(impacted.includes(scanUuid));
    assert.ok(impacted.includes(ocrUuid));

    await forge.setPreferredArtifact({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000001205",
      preferenceEventUuid: "018f0f4e-7b8c-7000-8000-000000001305",
      sourceUuid,
      artifactUuid: ocrUuid,
      reason: "better OCR available",
    });

    const reopened = new GraphForge(root);
    const backward = tableFromIPC(
      await reopened.researchLineage(ocrUuid, {
        subjectKind: "artifact",
        direction: "backward",
        maxDepth: 4,
      }),
    );
    assert.equal(backward.numRows, 1);
    assert.deepEqual(uuidColumn(backward, "input_uuid"), [scanUuid]);

    const closure = tableFromIPC(
      await reopened.retentionDependencyClosure(sourceUuid),
    );
    assert.equal(closure.numRows, 0);

    const postReopenImpact = tableFromIPC(
      await reopened.replacementImpact({
        sourceUuid,
        artifactUuid: scanUuid,
      }),
    );
    assert.equal(postReopenImpact.numRows, 1);
    assert.deepEqual(uuidColumn(postReopenImpact, "artifact_uuid"), [ocrUuid]);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
