// Fresh-native knowledge/epistemic resolved-belief projection acceptance (#2004).

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

test("resolved rank records knowledge output and exactly retries its epistemic attachment", async () => {
  const forge = new GraphForge();
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000801",
    capabilityId: "provenance",
    capabilityVersion: 1,
  });
  const node = forge.addNode("Person", { name: "Ada" });
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000802",
    capabilityId: "knowledge",
    capabilityVersion: 1,
  });
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000803",
    capabilityId: "epistemic",
    capabilityVersion: 1,
  });
  await forge.createAssertionWithStatus({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000804",
    assertionUuid: "018f0f4e-7b8c-7000-8000-000000000805",
    claim: "Ada participates in this projection",
    graphRefs: [
      {
        graphUuid: node.uuid,
        graphKind: "node",
        role: "subject",
        ordinal: 0,
      },
    ],
    statusEventUuid: "018f0f4e-7b8c-7000-8000-000000000806",
    status: "supported",
  });

  const projection = await forge.resolveBeliefProjection({
    transactionCutoffMicros: Number.MAX_SAFE_INTEGER,
    policy: {
      includedStatuses: ["supported"],
      statusless: "exclude",
      supersessionBranches: "include_all_leaves",
      hypotheses: "exclude_unselected_group",
    },
  });
  assert.equal(projection.sourceRecordUuids.length > 0, true);
  assert.equal(projection.graphContentFingerprint.length, 64);

  const descriptor = projection.prepareRankInvocation(
    "Person",
    "degree",
    undefined,
    true,
  );
  const runUuid = "018f0f4e-7b8c-7000-8000-000000000807";
  const attachmentUuid = "018f0f4e-7b8c-7000-8000-000000000808";
  const resolved = await forge.invokeResolvedRecorded(projection, {
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000809",
    runUuid,
    attachmentUuid,
    descriptor,
  });
  assert.equal(resolved.runUuid, runUuid);
  assert.equal(tableFromIPC(resolved.result).numRows, 1);
  assert.equal(resolved.attachmentState, "attached");
  assert.equal(tableFromIPC(resolved.attachment).numRows, 1);

  const retried = await forge.attachResolvedRun(projection, {
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000810",
    attachmentUuid,
    runUuid,
    descriptor,
  });
  assert.ok(Buffer.from(retried).equals(Buffer.from(resolved.attachment)));
});
