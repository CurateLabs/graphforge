// Fresh-native epistemic immutable reasoning acceptance (#780).

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function uuidFromBytes(value) {
  const hex = Buffer.from(value).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

test("reasoning is immutable, exact, idempotent, and Arrow-only", async () => {
  const forge = new GraphForge();
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000441",
    capabilityId: "provenance",
    capabilityVersion: 1,
  });
  const node = forge.addNode("Person", { name: "Ada" });
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000442",
    capabilityId: "knowledge",
    capabilityVersion: 1,
  });
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000443",
    capabilityId: "epistemic",
    capabilityVersion: 1,
  });
  const assertionUuid = "018f0f4e-7b8c-7000-8000-000000000444";
  const assertion = tableFromIPC(
    await forge.createAssertion({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000000445",
      assertionUuid,
      claim: "reasoning target",
      graphRefs: [
        {
          graphUuid: node.uuid,
          graphKind: "node",
          role: "subject",
          ordinal: 0,
        },
      ],
    }),
  );
  const provenanceUuid = uuidFromBytes(
    assertion.getChild("provenance_uuid").get(0),
  );
  const request = {
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000446",
    reasoningUuid: "018f0f4e-7b8c-7000-8000-000000000447",
    assertionUuid,
    kind: "logical_inference",
    contentFormat: "text/plain",
    content: Buffer.from("exact reasoning"),
    provenanceUuid,
  };
  const created = tableFromIPC(await forge.recordReasoning(request));
  const replayed = tableFromIPC(await forge.recordReasoning(request));
  assert.deepEqual(created.toArray(), replayed.toArray());
  assert.deepEqual(
    created.schema.fields.map((field) => field.name),
    [
      "reasoning_uuid",
      "assertion_uuid",
      "kind",
      "content_format",
      "content",
      "supersedes_reasoning_uuid",
      "provenance_uuid",
      "recorded_at",
      "contract_version",
    ],
  );
  assert.equal(
    Buffer.from(created.getChild("content").get(0)).toString(),
    "exact reasoning",
  );
  assert.deepEqual(
    tableFromIPC(await forge.reasoning(request.reasoningUuid)).toArray(),
    created.toArray(),
  );
  assert.deepEqual(
    tableFromIPC(await forge.listReasoning({ assertionUuid })).toArray(),
    created.toArray(),
  );
});
