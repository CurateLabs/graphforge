// Fresh-native M21 assertion-supersession acceptance (#778).

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function uuidFromBytes(value) {
  const hex = Buffer.from(value).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

test("supersession atomically publishes Arrow relation and terminal status", async () => {
  const forge = new GraphForge();
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000461",
    capabilityId: "provenance",
    capabilityVersion: 1,
  });
  const node = forge.addNode("Person", { name: "Ada" });
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000462",
    capabilityId: "knowledge",
    capabilityVersion: 1,
  });
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000463",
    capabilityId: "epistemic",
    capabilityVersion: 1,
  });
  const priorAssertionUuid = "018f0f4e-7b8c-7000-8000-000000000464";
  const replacementAssertionUuid = "018f0f4e-7b8c-7000-8000-000000000465";
  const graphRefs = [
    {
      graphUuid: node.uuid,
      graphKind: "node",
      role: "subject",
      ordinal: 0,
    },
  ];
  const prior = tableFromIPC(
    await forge.createAssertion({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000000466",
      assertionUuid: priorAssertionUuid,
      claim: "prior claim",
      graphRefs,
    }),
  );
  await forge.createAssertion({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000467",
    assertionUuid: replacementAssertionUuid,
    claim: "replacement claim",
    graphRefs,
  });
  const provenanceUuid = uuidFromBytes(
    prior.getChild("provenance_uuid").get(0),
  );
  const reasoningUuid = "018f0f4e-7b8c-7000-8000-000000000468";
  await forge.recordReasoning({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000469",
    reasoningUuid,
    assertionUuid: priorAssertionUuid,
    kind: "decision_rationale",
    contentFormat: "text/plain",
    content: Buffer.from("replacement rationale"),
    provenanceUuid,
  });
  const request = {
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000470",
    supersessionUuid: "018f0f4e-7b8c-7000-8000-000000000471",
    priorAssertionUuid,
    replacementAssertionUuid,
    statusEventUuid: "018f0f4e-7b8c-7000-8000-000000000472",
    reasoningUuid,
    provenanceUuid,
  };
  const created = tableFromIPC(await forge.supersedeAssertion(request));
  const replayed = tableFromIPC(await forge.supersedeAssertion(request));
  assert.deepEqual(created.toArray(), replayed.toArray());
  assert.deepEqual(
    tableFromIPC(
      await forge.listAssertionSupersessions({ priorAssertionUuid }),
    ).toArray(),
    created.toArray(),
  );
  const status = tableFromIPC(await forge.assertionStatus(priorAssertionUuid));
  assert.equal(status.getChild("status").get(0), "superseded");
  assert.equal(
    uuidFromBytes(status.getChild("status_event_uuid").get(0)),
    request.statusEventUuid,
  );
});
