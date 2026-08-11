// Fresh-native knowledge immutable evidence acceptance (#775).

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

test("assertion and evidence publish atomically through the Node surface", async () => {
  const forge = new GraphForge();
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000601",
    capabilityId: "provenance",
    capabilityVersion: 1,
  });
  const node = forge.addNode("Person", { name: "Ada" });
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000602",
    capabilityId: "knowledge",
    capabilityVersion: 1,
  });
  const assertionUuid = "018f0f4e-7b8c-7000-8000-000000000603";
  const evidenceUuid = "018f0f4e-7b8c-7000-8000-000000000604";
  const request = {
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000605",
    assertionUuid,
    claim: "evidence target",
    graphRefs: [
      {
        graphUuid: node.uuid,
        graphKind: "node",
        role: "subject",
        ordinal: 0,
      },
    ],
    evidence: [
      {
        evidenceUuid,
        sourceUuid: node.uuid,
        sourceKind: "graph_node",
        role: "supports",
        weight: 0.8,
      },
    ],
  };
  const created = tableFromIPC(
    await forge.createAssertionWithEvidence(request),
  );
  const replayed = tableFromIPC(
    await forge.createAssertionWithEvidence(request),
  );
  assert.deepEqual(created.toArray(), replayed.toArray());

  const evidence = tableFromIPC(await forge.evidenceLink(evidenceUuid));
  assert.equal(evidence.getChild("role").get(0), "supports");
  assert.equal(evidence.getChild("weight").get(0), 0.8);
  const listed = tableFromIPC(await forge.listEvidenceLinks({ assertionUuid }));
  assert.deepEqual(listed.toArray(), evidence.toArray());
});
