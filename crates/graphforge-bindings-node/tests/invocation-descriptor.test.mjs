import assert from "node:assert/strict";
import test from "node:test";

import { tableFromIPC } from "apache-arrow";

import { GraphForge } from "../index.js";

test("rank descriptor matches the frozen cross-language vector and dispatches", () => {
  const graph = new GraphForge();
  const descriptor = graph.prepareRankInvocation(
    "Person",
    "degree",
    "KNOWS",
    true,
  );

  assert.equal(descriptor.verb, "rank");
  assert.equal(descriptor.algorithm, "degree");
  assert.ok(Buffer.isBuffer(descriptor.canonicalBytes));
  assert.equal(descriptor.projectionFingerprint.length, 64);
  assert.equal(
    descriptor.fingerprint,
    "61be156b4aea627fd2cdbf75e18bcc5d0cfc1df53de51ceec5ab9c98f5e19992",
  );

  const result = tableFromIPC(graph.invokeDescriptor(descriptor));
  assert.deepEqual(
    result.schema.fields.map((field) => field.name),
    ["node_uuid", "score"],
  );
  assert.equal(result.numRows, 0);
  const replayed = tableFromIPC(
    graph.invokeDescriptorBytes(descriptor.canonicalBytes),
  );
  assert.equal(replayed.numRows, 0);
  assert.deepEqual(
    replayed.schema.fields.map((field) => field.name),
    ["node_uuid", "score"],
  );
});
