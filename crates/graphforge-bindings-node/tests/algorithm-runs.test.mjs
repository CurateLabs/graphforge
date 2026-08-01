// Fresh-native M20 recorded algorithm lifecycle acceptance (#2003).

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

test("recorded rank exposes the same result and durable lifecycle", async () => {
  const forge = new GraphForge();
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000701",
    capabilityId: "provenance",
    capabilityVersion: 1,
  });
  forge.addNode("Person", { name: "Ada" });
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000702",
    capabilityId: "knowledge",
    capabilityVersion: 1,
  });
  const descriptor = forge.prepareRankInvocation(
    "Person",
    "degree",
    undefined,
    true,
  );
  const runUuid = "018f0f4e-7b8c-7000-8000-000000000703";
  const recorded = await forge.invokeRecorded({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000704",
    runUuid,
    descriptor,
  });
  assert.equal(recorded.runUuid, runUuid);
  assert.equal(tableFromIPC(recorded.result).numRows, 1);

  const run = tableFromIPC(await forge.algorithmRun(runUuid));
  assert.equal(run.getChild("algorithm").get(0), "rank.degree");
  const events = tableFromIPC(await forge.algorithmRunEvents(runUuid));
  assert.deepEqual(events.getChild("state").toArray(), [
    "started",
    "completed",
  ]);
  assert.equal(
    tableFromIPC(await forge.listAlgorithmRuns({ algorithm: "rank.degree" }))
      .numRows,
    1,
  );

  const conflicting = forge.prepareRankInvocation(
    "Person",
    "pagerank",
    undefined,
    true,
  );
  await assert.rejects(
    forge.invokeRecorded({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000000705",
      runUuid,
      descriptor: conflicting,
    }),
    (error) => error.code === "GF_IDEMPOTENCY_CONFLICT",
  );
  assert.equal(
    tableFromIPC(await forge.algorithmRunEvents(runUuid)).numRows,
    2,
  );

  const controller = new AbortController();
  const cancelledRun = "018f0f4e-7b8c-7000-8000-000000000706";
  const cancelled = forge.invokeRecorded({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000707",
    runUuid: cancelledRun,
    descriptor,
    signal: controller.signal,
  });
  controller.abort();
  await assert.rejects(cancelled, (error) => error.code === "GF_CANCELLED");
  assert.deepEqual(
    tableFromIPC(await forge.algorithmRunEvents(cancelledRun))
      .getChild("state")
      .toArray(),
    ["started", "cancelled"],
  );
});
