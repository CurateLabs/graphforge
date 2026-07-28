import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { GraphForge } from "../index.js";

function expectValidation(fragment, call) {
  assert.throws(
    call,
    (error) =>
      error.code === "ValidationError" && error.message.includes(fragment),
  );
}

test("embedding refresh policy and worker inspection are durable and content-free", () => {
  const project = mkdtempSync(join(tmpdir(), "gf-node-embedding-refresh-"));
  const forge = new GraphForge(project);
  try {
    const node = forge.addNode("Person", { name: "Alice" });
    forge.publishCallerEmbeddings("semantic", {
      rows: [{ node, vector: [1, 0] }],
      dimensions: 2,
      sourceProjection: { label: "Person", recipe: "all_people_v1" },
    });
    forge.setDefaultEmbeddingSpace("semantic");

    const freshness = forge.inspectEmbeddingSpaceFreshness("semantic");
    assert.deepEqual(forge.inspectEmbeddingSpaceFreshness(), freshness);
    assert.equal(freshness.state, "fresh");
    assert.deepEqual(freshness.decision, { kind: "serve_fresh" });
    assert.deepEqual(forge.embeddingRefreshProjectPolicy(), {
      proactive: true,
      debounceMillis: 500,
      maxConcurrentJobs: 2,
    });
    const projectPolicy = forge.setEmbeddingRefreshProjectPolicy(false, 250, 1);
    assert.deepEqual(projectPolicy, {
      proactive: false,
      debounceMillis: 250,
      maxConcurrentJobs: 1,
    });

    const inspection = forge.setEmbeddingRefreshSpacePolicy(
      undefined,
      true,
      25,
    );
    assert.deepEqual(inspection.spacePolicy, {
      proactive: true,
      debounceMillis: 25,
    });
    assert.deepEqual(inspection.resolvedPolicy, {
      proactive: true,
      debounceMillis: 25,
      maxConcurrentJobs: 1,
    });
    assert.deepEqual(inspection.freshness, freshness);
    assert.equal(inspection.lastOutcome, null);
    assert.equal(inspection.worker.state, "running");
    assert.equal(inspection.worker.queuedLineages, 0);
    assert.equal(inspection.worker.inFlightLineages, 0);
    assert.doesNotMatch(
      JSON.stringify(inspection),
      /sourceText|providerPayload|credentials|confidence|provenanceId|assertionUuid|beliefStatus|validTime/,
    );

    const cleared = forge.setEmbeddingRefreshSpacePolicy(
      undefined,
      undefined,
      undefined,
      true,
    );
    assert.equal(cleared.spacePolicy, null);
    assert.deepEqual(cleared.resolvedPolicy, projectPolicy);
    expectValidation("requires an override", () =>
      forge.setEmbeddingRefreshSpacePolicy(),
    );
    expectValidation("cannot include overrides", () =>
      forge.setEmbeddingRefreshSpacePolicy(undefined, true, undefined, true),
    );
    expectValidation("not configured", () =>
      forge.inspectEmbeddingRefresh("missing"),
    );
    forge.close();

    const reopened = new GraphForge(project);
    try {
      assert.deepEqual(reopened.embeddingRefreshProjectPolicy(), projectPolicy);
      const reopenedInspection = reopened.inspectEmbeddingRefresh();
      assert.equal(reopenedInspection.worker.state, "running");
    } finally {
      reopened.close();
    }
  } finally {
    forge.close();
    rmSync(project, { recursive: true, force: true });
  }
});
