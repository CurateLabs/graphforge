// Fresh-native knowledge immutable assertion acceptance (#2411).

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

test("assertions are atomic, idempotent, exact, and Arrow-only", async () => {
  const forge = new GraphForge();
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000401",
    capabilityId: "provenance",
    capabilityVersion: 1,
  });
  const node = forge.addNode("Person", { name: "Ada" });
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000402",
    capabilityId: "knowledge",
    capabilityVersion: 1,
  });
  const request = {
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000403",
    assertionUuid: "018f0f4e-7b8c-7000-8000-000000000404",
    claim: "e\u0301 is not normalized to é",
    graphRefs: [
      {
        graphUuid: node.uuid,
        graphKind: "node",
        role: "subject",
        ordinal: 0,
      },
    ],
  };

  const created = tableFromIPC(await forge.createAssertion(request));
  const replayed = tableFromIPC(await forge.createAssertion(request));
  assert.equal(created.numRows, 1);
  assert.deepEqual(created.toArray(), replayed.toArray());
  assert.equal(created.getChild("claim").get(0), request.claim);
  assert.deepEqual(
    created.schema.fields.map((field) => field.name),
    [
      "assertion_uuid",
      "claim",
      "provenance_uuid",
      "recorded_at",
      "contract_version",
    ],
  );

  const fetched = tableFromIPC(await forge.assertion(request.assertionUuid));
  const listed = tableFromIPC(
    await forge.listAssertions({ graphUuid: node.uuid }),
  );
  const refs = tableFromIPC(
    await forge.assertionGraphRefs(request.assertionUuid),
  );
  assert.deepEqual(fetched.toArray(), created.toArray());
  assert.deepEqual(listed.toArray(), created.toArray());
  assert.equal(refs.numRows, 1);
  assert.equal(refs.getChild("role").get(0), "subject");
  assert.equal(refs.getChild("graph_kind").get(0), "node");
});
