import assert from "node:assert/strict";
import { mkdtempSync, realpathSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { dispatchRecordedNeutralAnalysis } from "../workflows/index.js";

const UUIDS = Array.from(
  { length: 16 },
  (_, index) =>
    `018f0f4e-7b8c-7000-8000-${String(index + 1).padStart(12, "0")}`,
);

function table(rows) {
  const fields = [...new Set(rows.flatMap((row) => Object.keys(row)))];
  return {
    getChild: (name) => ({ get: (index) => rows[index]?.[name] }),
    numRows: rows.length,
    schema: { fields: fields.map((name) => ({ name })) },
  };
}

function project(overrides = {}) {
  const path = realpathSync(
    mkdtempSync(join(tmpdir(), "graphforge-recorded-analysis-")),
  );
  writeFileSync(join(path, "FORMAT"), "graphforge-project/v1\n");
  class GraphForge {
    static instance;

    constructor(openedPath) {
      assert.equal(openedPath, path);
      this.calls = [];
      GraphForge.instance = this;
    }

    async projectCapabilities() {
      return table(
        ["graph", "knowledge", "epistemic"].map((capability_id) => ({
          capability_id,
          capability_version: 1,
          status: "supported",
        })),
      );
    }

    async invokeResolvedRecorded(projection, request) {
      this.calls.push(["invokeResolvedRecorded", projection, request]);
      if (overrides.recordedError) throw overrides.recordedError;
      return {
        attachment: overrides.attachmentFailure
          ? undefined
          : table([{ attachment_uuid: request.attachmentUuid }]),
        attachmentErrorCode: overrides.attachmentFailure
          ? "GF_ATTACHMENT_FAILED"
          : undefined,
        attachmentState: overrides.attachmentFailure
          ? "attachment_failed"
          : "attached",
        attachmentUuid: request.attachmentUuid,
        result: table([{ node_uuid: UUIDS[10], score: 1 }]),
        runUuid: request.runUuid,
      };
    }

    async algorithmRun(runUuid) {
      this.calls.push(["algorithmRun", runUuid]);
      return table([
        { algorithm: "rank.degree", run_uuid: runUuid, state: "completed" },
      ]);
    }

    async algorithmRunEvents(runUuid) {
      this.calls.push(["algorithmRunEvents", runUuid]);
      return table([
        { run_uuid: runUuid, state: "started" },
        { run_uuid: runUuid, state: "completed" },
      ]);
    }

    close() {
      this.closed = true;
    }
  }
  return { GraphForge, path };
}

test("recorded analysis forwards the exact descriptor and preserves attachment failure", async () => {
  const { GraphForge, path } = project({ attachmentFailure: true });
  const resolvedProjection = {
    fingerprint: "projection",
    graphContentFingerprint: "11".repeat(32),
    policyFingerprint: "22".repeat(32),
    snapshotFingerprint: "33".repeat(32),
    sourceGenerationUuid: UUIDS[12],
    sourceRecordUuids: [UUIDS[0]],
  };
  const descriptor = {
    algorithm: "degree",
    fingerprint: "bb".repeat(32),
    verb: "rank",
  };
  const result = await dispatchRecordedNeutralAnalysis({
    GraphForge,
    tableFromIPC: (value) => value,
    path,
    input: {
      attachment_uuid: UUIDS[8],
      descriptor,
      operation_uuid: UUIDS[6],
      projection: resolvedProjection,
      run_uuid: UUIDS[7],
    },
  });

  assert.equal(result.run_uuid, UUIDS[7]);
  assert.equal(result.attachment_state, "attachment_failed");
  assert.equal(result.attachment_error_code, "GF_ATTACHMENT_FAILED");
  assert.deepEqual(result.attachment, []);
  assert.equal(result.run[0].state, "completed");
  assert.deepEqual(
    result.run_events.map((row) => row.state),
    ["started", "completed"],
  );
  assert.equal(result.descriptor_fingerprint, descriptor.fingerprint);
  assert.deepEqual(GraphForge.instance.calls[0], [
    "invokeResolvedRecorded",
    resolvedProjection,
    {
      actorUuid: undefined,
      attachmentUuid: UUIDS[8],
      descriptor,
      operationUuid: UUIDS[6],
      runUuid: UUIDS[7],
      signal: undefined,
    },
  ]);
  assert.equal(GraphForge.instance.closed, true);
});

test("recorded analysis rejects missing descriptor or projection", async () => {
  const { GraphForge, path } = project();
  await assert.rejects(
    dispatchRecordedNeutralAnalysis({
      GraphForge,
      tableFromIPC: (value) => value,
      path,
      input: {
        attachment_uuid: UUIDS[8],
        operation_uuid: UUIDS[6],
        projection: { ok: true },
        run_uuid: UUIDS[7],
      },
    }),
    { code: "GF_AGENT_ANALYSIS_DESCRIPTOR_REQUIRED" },
  );
  await assert.rejects(
    dispatchRecordedNeutralAnalysis({
      GraphForge,
      tableFromIPC: (value) => value,
      path,
      input: {
        attachment_uuid: UUIDS[8],
        descriptor: { algorithm: "degree", fingerprint: "x", verb: "rank" },
        operation_uuid: UUIDS[6],
        run_uuid: UUIDS[7],
      },
    }),
    { code: "GF_AGENT_ANALYSIS_PROJECTION_REQUIRED" },
  );
});
