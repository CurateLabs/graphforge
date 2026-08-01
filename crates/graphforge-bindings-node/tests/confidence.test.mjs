// Fresh-native M20 immutable confidence acceptance (#774).

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

test("confidence assessments are atomic, idempotent, and Arrow-only", async () => {
  const forge = new GraphForge();
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000501",
    capabilityId: "provenance",
    capabilityVersion: 1,
  });
  const node = forge.addNode("Person", { name: "Ada" });
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000502",
    capabilityId: "knowledge",
    capabilityVersion: 1,
  });
  const assertionUuid = "018f0f4e-7b8c-7000-8000-000000000503";
  await forge.createAssertion({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000504",
    assertionUuid,
    claim: "confidence target",
    graphRefs: [
      {
        graphUuid: node.uuid,
        graphKind: "node",
        role: "subject",
        ordinal: 0,
      },
    ],
  });
  const confidenceUuid = "018f0f4e-7b8c-7000-8000-000000000505";
  const request = {
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000506",
    confidenceUuid,
    assertionUuid,
    policy: "explicit",
    value: 0.75,
  };
  const created = tableFromIPC(await forge.assessConfidence(request));
  const replayed = tableFromIPC(await forge.assessConfidence(request));
  assert.deepEqual(created.toArray(), replayed.toArray());
  assert.equal(created.getChild("value").get(0), 0.75);
  assert.deepEqual(
    created.schema.fields.map((field) => field.name),
    [
      "confidence_uuid",
      "assertion_uuid",
      "policy",
      "policy_version",
      "value",
      "provenance_uuid",
      "recorded_at",
      "contract_version",
    ],
  );

  const fetched = tableFromIPC(
    await forge.confidenceAssessment(confidenceUuid),
  );
  const listed = tableFromIPC(
    await forge.listConfidenceAssessments({ assertionUuid }),
  );
  const inputs = tableFromIPC(await forge.confidenceInputs(confidenceUuid));
  assert.deepEqual(fetched.toArray(), created.toArray());
  assert.deepEqual(listed.toArray(), created.toArray());
  assert.equal(inputs.numRows, 0);
});
