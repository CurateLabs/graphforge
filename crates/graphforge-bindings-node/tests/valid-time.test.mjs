// Fresh-native M21 optional valid-time acceptance (#781).

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function uuidFromBytes(value) {
  const hex = Buffer.from(value).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

test("valid time is opt-in, append-only, and evaluated after transaction time", async () => {
  const forge = new GraphForge();
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000381",
    capabilityId: "provenance",
    capabilityVersion: 1,
  });
  const node = forge.addNode("Person", { name: "Ada" });
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000382",
    capabilityId: "knowledge",
    capabilityVersion: 1,
  });
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000383",
    capabilityId: "epistemic",
    capabilityVersion: 1,
  });

  const assertionUuid = "018f0f4e-7b8c-7000-8000-000000000384";
  const assertion = tableFromIPC(
    await forge.createAssertionWithStatus({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000000385",
      assertionUuid,
      claim: "Ada held the role during the interval",
      graphRefs: [
        {
          graphUuid: node.uuid,
          graphKind: "node",
          role: "subject",
          ordinal: 0,
        },
      ],
      statusEventUuid: "018f0f4e-7b8c-7000-8000-000000000386",
      status: "supported",
    }),
  );
  const validityRequest = {
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000388",
    validityEventUuid: "018f0f4e-7b8c-7000-8000-000000000389",
    assertionUuid,
    validFromMicros: 100,
    validToMicros: 200,
    provenanceUuid: uuidFromBytes(assertion.getChild("provenance_uuid").get(0)),
  };
  const disabled = (error) => error.code === "GF_CAPABILITY_DISABLED";
  await assert.rejects(
    forge.recordAssertionValidity(validityRequest),
    disabled,
  );
  await assert.rejects(
    forge.applyValidTime({
      transactionCutoffMicros: Number.MAX_SAFE_INTEGER,
      validTimeMicros: 150,
    }),
    disabled,
  );
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000387",
    capabilityId: "valid_time",
    capabilityVersion: 1,
  });

  const validity = tableFromIPC(
    await forge.recordAssertionValidity(validityRequest),
  );
  assert.equal(validity.numRows, 1);
  assert.deepEqual(
    tableFromIPC(
      await forge.recordAssertionValidity(validityRequest),
    ).toArray(),
    validity.toArray(),
  );
  await assert.rejects(
    forge.recordAssertionValidity({
      ...validityRequest,
      validToMicros: 201,
    }),
    /validity event UUID was reused for different content/,
  );
  assert.equal(
    tableFromIPC(await forge.listAssertionValidity({ assertionUuid })).numRows,
    1,
  );

  const inside = tableFromIPC(
    await forge.applyValidTime({
      transactionCutoffMicros: Number.MAX_SAFE_INTEGER,
      validTimeMicros: 150,
    }),
  );
  assert.equal(inside.getChild("interpretation").get(0), "interpreted");
  assert.equal(inside.getChild("is_valid").get(0), true);

  const upperBound = tableFromIPC(
    await forge.applyValidTime({
      transactionCutoffMicros: Number.MAX_SAFE_INTEGER,
      validTimeMicros: 200,
    }),
  );
  assert.equal(upperBound.getChild("is_valid").get(0), false);
});
