// Fresh-native M21 explicit assertion-status acceptance (#777).

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function uuidFromBytes(value) {
  const hex = Buffer.from(value).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

test("assertion status is explicit, atomic, append-only, and Arrow-only", async () => {
  const forge = new GraphForge();
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000351",
    capabilityId: "provenance",
    capabilityVersion: 1,
  });
  const node = forge.addNode("Person", { name: "Ada" });
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000352",
    capabilityId: "knowledge",
    capabilityVersion: 1,
  });
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000353",
    capabilityId: "epistemic",
    capabilityVersion: 1,
  });

  const assertionUuid = "018f0f4e-7b8c-7000-8000-000000000354";
  const created = tableFromIPC(
    await forge.createAssertionWithStatus({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000000355",
      assertionUuid,
      claim: "explicit status target",
      graphRefs: [
        {
          graphUuid: node.uuid,
          graphKind: "node",
          role: "subject",
          ordinal: 0,
        },
      ],
      statusEventUuid: "018f0f4e-7b8c-7000-8000-000000000356",
      status: "hypothesis",
    }),
  );
  assert.deepEqual(
    created.schema.fields.map((field) => field.name),
    [
      "status_event_uuid",
      "assertion_uuid",
      "status",
      "confidence_uuid",
      "reasoning_uuid",
      "provenance_uuid",
      "recorded_at",
      "contract_version",
    ],
  );
  assert.equal(created.getChild("status").get(0), "hypothesis");
  assert.deepEqual(
    tableFromIPC(await forge.assertionStatus(assertionUuid)).toArray(),
    created.toArray(),
  );

  const updated = tableFromIPC(
    await forge.recordAssertionStatus({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000000357",
      statusEventUuid: "018f0f4e-7b8c-7000-8000-000000000358",
      assertionUuid,
      status: "supported",
      provenanceUuid: uuidFromBytes(created.getChild("provenance_uuid").get(0)),
    }),
  );
  assert.equal(updated.getChild("status").get(0), "supported");
  const history = tableFromIPC(
    await forge.listAssertionStatus({ assertionUuid }),
  );
  assert.equal(history.numRows, 2);
  assert.deepEqual(
    tableFromIPC(await forge.assertionStatus(assertionUuid)).toArray(),
    updated.toArray(),
  );
});
