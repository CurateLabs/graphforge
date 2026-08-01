// Fresh-native M20 provenance and construction acceptance (#773).

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const uuidString = (value) => {
  const hex = Buffer.from(value).toString("hex");
  return [
    hex.slice(0, 8),
    hex.slice(8, 12),
    hex.slice(12, 16),
    hex.slice(16, 20),
    hex.slice(20),
  ].join("-");
};

async function settlesOrCancels(promise, cancellationCode, assertCompletion) {
  let result;
  try {
    result = await promise;
  } catch (error) {
    assert.equal(error.code, cancellationCode);
    assert.notEqual(error.name, "AbortError");
    return;
  }
  assertCompletion(tableFromIPC(result));
}

test("provenance reads, addEdge, and cancellation use the native contract", async () => {
  const forge = new GraphForge();
  await forge.enableCapability({
    operationUuid: "018f0f4e-7b8c-7000-8000-000000000101",
    capabilityId: "provenance",
    capabilityVersion: 1,
  });
  const ada = forge.addNode("Person", { name: "Ada" });
  const grace = forge.addNode("Person", { name: "Grace" });
  forge.addEdge(ada, "KNOWS", grace, { since: 2026 });

  const history = tableFromIPC(
    await forge.listProvenanceHistory({ subjectUuid: ada.uuid }),
  );
  assert.ok(history.numRows >= 2);
  assert.ok(
    [...history.getChild("event_kind").toArray()].includes("create_node"),
  );
  assert.ok(
    [...history.getChild("event_kind").toArray()].includes("create_edge"),
  );

  const eventUuid = uuidString(history.getChild("provenance_uuid").get(0));
  const event = tableFromIPC(await forge.provenanceEvent(eventUuid));
  assert.equal(event.numRows, 1);
  assert.deepEqual(
    event.schema.fields.map((field) => field.name),
    [
      "provenance_uuid",
      "operation_uuid",
      "event_kind",
      "actor_uuid",
      "recorded_at",
      "contract_version",
    ],
  );

  const controller = new AbortController();
  const cancelled = forge.listProvenanceHistory({
    signal: controller.signal,
  });
  controller.abort();
  await settlesOrCancels(cancelled, "GF_CANCELLED", (table) => {
    assert.ok(table.numRows >= 2);
    assert.ok(
      [...table.getChild("event_kind").toArray()].includes("create_edge"),
    );
  });
});
