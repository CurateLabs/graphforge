// Fresh-native epistemic explicit hypothesis-selection acceptance (#779).

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function uuidFromBytes(value) {
  const hex = Buffer.from(value).toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

test("hypothesis membership and selection are explicit and Arrow-only", async () => {
  const forge = new GraphForge();
  for (const [operationUuid, capabilityId] of [
    ["018f0f4e-7b8c-7000-8000-000000000381", "provenance"],
    ["018f0f4e-7b8c-7000-8000-000000000382", "knowledge"],
    ["018f0f4e-7b8c-7000-8000-000000000383", "epistemic"],
  ]) {
    await forge.enableCapability({
      operationUuid,
      capabilityId,
      capabilityVersion: 1,
    });
  }
  const node = forge.addNode("Person", { name: "Ada" });
  const assertionUuid = "018f0f4e-7b8c-7000-8000-000000000384";
  const assertion = tableFromIPC(
    await forge.createAssertion({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000000385",
      assertionUuid,
      claim: "explicit hypothesis",
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
  const reasoningUuid = "018f0f4e-7b8c-7000-8000-000000000386";
  await forge.recordReasoning({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000387",
    reasoningUuid,
    assertionUuid,
    kind: "decision_rationale",
    contentFormat: "text/plain",
    content: Buffer.from("explicit selection"),
    provenanceUuid,
  });
  const groupUuid = "018f0f4e-7b8c-7000-8000-000000000388";
  await forge.createHypothesisGroup({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000389",
    groupUuid,
    questionKey: "binding.selection.v1",
    provenanceUuid,
  });
  await forge.recordHypothesisMembership({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000390",
    membershipEventUuid: "018f0f4e-7b8c-7000-8000-000000000391",
    groupUuid,
    assertionUuid,
    action: "added",
    reasoningUuid,
    provenanceUuid,
  });
  assert.equal(
    tableFromIPC(await forge.hypothesisSelection(groupUuid)).numRows,
    0,
  );
  const selected = tableFromIPC(
    await forge.recordHypothesisSelection({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000000392",
      selectionEventUuid: "018f0f4e-7b8c-7000-8000-000000000393",
      groupUuid,
      selectedAssertionUuid: assertionUuid,
      reasoningUuid,
      provenanceUuid,
    }),
  );
  assert.deepEqual(
    tableFromIPC(await forge.hypothesisSelection(groupUuid)).toArray(),
    selected.toArray(),
  );
  assert.equal(
    tableFromIPC(await forge.hypothesisMembers(groupUuid)).numRows,
    1,
  );
  assert.equal(
    tableFromIPC(
      await forge.listHypothesisGroups({
        questionKey: "binding.selection.v1",
      }),
    ).numRows,
    1,
  );
  assert.equal(
    tableFromIPC(await forge.listHypothesisMembership({ groupUuid })).numRows,
    1,
  );
  assert.deepEqual(
    tableFromIPC(await forge.listHypothesisSelection({ groupUuid })).toArray(),
    selected.toArray(),
  );
  const snapshot = tableFromIPC(
    await forge.epistemicSnapshot(Number.MAX_SAFE_INTEGER),
  );
  assert.equal(snapshot.numRows, 2);
  assert.deepEqual(
    [...snapshot.getChild("entity_kind").toArray()],
    ["assertion", "hypothesis_group"],
  );
});
