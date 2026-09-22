// Real Node parity for Rust-owned immutable research Versions.
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
const require = createRequire(import.meta.url);
const { GraphForge } = require("../index.js");
const identity = (number) =>
  `018f0f4e-7b8c-7000-8000-${String(number).padStart(12, "0")}`;
const count = (buffer) => Number(tableFromIPC(buffer).getChildAt(0).get(0));

test("historical graph ontology and Artifact bytes survive compaction, reopen and restore", async () => {
  const directory = mkdtempSync(join(tmpdir(), "gf-versions-"));
  let graph;
  try {
    const root = join(directory, "project");
    graph = new GraphForge(root);
    await graph.execute("CREATE (:Person)");
    const ontology = join(directory, "ontology.yaml");
    writeFileSync(
      ontology,
      'ontology_id: historical\nversion: "1"\nentity_types:\n  - name: Person\n    abstract: false\nrelation_types: []\n',
    );
    graph.adoptOntology(ontology, "advisory", identity(1));
    for (const [number, capabilityId] of [
      [2, "provenance"],
      [3, "knowledge"],
    ]) {
      await graph.enableCapability({
        operationUuid: identity(number),
        capabilityId,
        capabilityVersion: 1,
      });
    }
    await graph.registerSource({
      operationUuid: identity(4),
      sourceUuid: identity(5),
      label: "Original",
      sourceKind: "manuscript",
    });
    await graph.registerArtifact({
      operationUuid: identity(6),
      artifactUuid: identity(7),
      sourceUuid: identity(5),
      artifactKind: "raw_scan",
      mediaType: "application/octet-stream",
      payload: { kind: "local_bytes", bytes: Buffer.from("frozen bytes") },
    });
    const frozenOntology = graph.workspaceOntology();
    const metadata = graph.researchProjectMetadata();
    metadata.title = "Frozen";
    graph.updateResearchMetadata({ metadata, operationUuid: identity(8) });
    const prepared = graph.prepareResearchVersion({
      operation_uuid: identity(9),
      version_uuid: identity(10),
      context_uuid: identity(11),
      label: "Citation",
      description: null,
      created_at: 1,
      required_versions: [],
    });
    const receipt = await graph.commitResearchVersionOperation(prepared);
    await graph.checkpoint({
      name: "Independent",
      idempotencyKey: identity(12),
    });
    await graph.execute("CREATE (:Person)");
    await graph.deleteCheckpoint({
      name: "Independent",
      idempotencyKey: identity(13),
    });
    metadata.title = "Later";
    graph.updateResearchMetadata({ metadata, operationUuid: identity(14) });
    await graph.commitResearchVersionOperation({
      operation_uuid: identity(15),
      expected_generation_uuid:
        graph.researchProjectSummary().identity.generationUuid,
      mutation: { operation: "compact", versions: [identity(10)] },
    });
    assert.equal(
      tableFromIPC(graph.researchVersionArtifact(identity(10), identity(7)))
        .numRows,
      1,
    );
    const frozen = graph.researchVersion(identity(10));
    graph.close();
    graph = new GraphForge(root);
    assert.deepEqual(graph.researchVersion(identity(10)), frozen);
    assert.equal(
      count(
        graph.queryResearchVersion(identity(10), "MATCH (n) RETURN count(n)"),
      ),
      1,
    );
    assert.deepEqual(
      graph.researchVersionOntology(identity(10)),
      frozenOntology,
    );
    assert.equal(graph.researchVersionMetadata(identity(10)).title, "Frozen");
    assert.deepEqual(
      Buffer.from(
        tableFromIPC(
          graph.researchVersionArtifactPayload(identity(10), identity(7)),
        )
          .getChildAt(0)
          .get(0),
      ),
      Buffer.from("frozen bytes"),
    );
    const restore = {
      operation_uuid: identity(16),
      expected_generation_uuid:
        graph.researchProjectSummary().identity.generationUuid,
      mutation: {
        operation: "restore_project",
        context_uuid: identity(11),
        source_version: identity(10),
        version_uuid: identity(17),
        created_at: 2,
      },
    };
    const restored = await graph.commitResearchVersionOperation(restore);
    assert.equal(count(await graph.execute("MATCH (n) RETURN count(n)")), 1);
    await graph.execute("CREATE (:Person)");
    assert.deepEqual(
      await graph.commitResearchVersionOperation(restore),
      restored,
    );
    assert.deepEqual(
      await graph.commitResearchVersionOperation(prepared),
      receipt,
    );
    assert.equal(count(await graph.execute("MATCH (n) RETURN count(n)")), 2);
    assert.equal(tableFromIPC(graph.listResearchVersions()).numRows, 2);
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(
      graph.commitResearchVersionOperation(prepared, controller.signal),
      (error) => error.code === "GF_CANCELLED",
    );
  } finally {
    graph?.close();
    rmSync(directory, { recursive: true, force: true });
  }
});

test("malformed Version requests do not echo private values or field names", () => {
  const graph = new GraphForge();
  const sentinel = "PRIVATE_SOURCE_SENTINEL";
  const valid = {
    operation_uuid: identity(101),
    version_uuid: identity(102),
    context_uuid: identity(103),
    created_at: 1,
    required_versions: [],
  };
  try {
    for (const request of [
      { ...valid, created_at: sentinel },
      { ...valid, [sentinel]: true },
    ]) {
      assert.throws(
        () => graph.prepareResearchVersion(request),
        (error) =>
          error.message.length < 256 && !error.message.includes(sentinel),
      );
    }
    const prepared = graph.prepareResearchVersion(valid);
    assert.throws(
      () =>
        graph.commitResearchVersionOperation({
          ...prepared,
          mutation: { operation: sentinel },
        }),
      (error) => !error.message.includes(sentinel),
    );
  } finally {
    graph.close();
  }
});
