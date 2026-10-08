// Fresh-native execution of optional Graphalytics-compatible algorithm modes.
import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function scores(ipc) {
  const table = tableFromIPC(ipc);
  return Object.fromEntries(
    [...table.getChild("name").toArray()].map((name, i) => [
      name,
      table.getChild("score").get(i),
    ]),
  );
}

function communities(ipc) {
  return [...tableFromIPC(ipc).getChild("community_id").toArray()].map(Number);
}

test("PageRank controls survive ordinary, descriptor and recorded dispatch", async () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (a)-[:LINK]->(b)",
  );
  const result = forge.rank(
    "Person",
    "pagerank",
    "LINK",
    true,
    undefined,
    0.5,
    1,
  );
  assert.deepEqual(scores(result), { A: 0.375, B: 0.625 });
  assert.deepEqual(
    scores(
      forge.rank("Person", "pagerank", "LINK", true, undefined, undefined, 0),
    ),
    { A: 0.5, B: 0.5 },
  );
  const descriptor = forge.prepareRankInvocation(
    "Person",
    "pagerank",
    "LINK",
    true,
    0.5,
    1,
  );
  assert.deepEqual(scores(forge.invokeDescriptor(descriptor)), scores(result));
  assert.deepEqual(
    scores(forge.invokeDescriptorBytes(descriptor.canonicalBytes)),
    scores(result),
  );
  assert.notEqual(
    descriptor.fingerprint,
    forge.prepareRankInvocation("Person", "pagerank", "LINK", true, 0.5, 2)
      .fingerprint,
  );
  for (const capabilityId of ["provenance", "knowledge"]) {
    await forge.enableCapability({
      operationUuid: randomUUID(),
      capabilityId,
      capabilityVersion: 1,
    });
  }
  const recorded = await forge.invokeRecorded({
    operationUuid: randomUUID(),
    runUuid: randomUUID(),
    descriptor,
  });
  assert.deepEqual(scores(recorded.result), scores(result));
});

test("directed neighbor-edge LCC preserves the default Fagiolo mode", () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), (d:Person {name:'D'}), (a)-[:LINK]->(b), (b)-[:LINK]->(a), (a)-[:LINK]->(c), (a)-[:LINK]->(d), (b)-[:LINK]->(c), (c)-[:LINK]->(b)",
  );
  const defaults = scores(
    forge.rank("Person", "clustering_coefficient", "LINK", true),
  );
  assert.deepEqual(
    scores(
      forge.rank(
        "Person",
        "clustering_coefficient",
        "LINK",
        true,
        undefined,
        undefined,
        undefined,
        "fagiolo",
      ),
    ),
    defaults,
  );
  const explicit = forge.rank(
    "Person",
    "clustering_coefficient",
    "LINK",
    true,
    undefined,
    undefined,
    undefined,
    "neighbor_edges",
  );
  assert.equal(scores(explicit).A, 1 / 3);
  assert.equal(defaults.A, 0.4);
  const descriptor = forge.prepareRankInvocation(
    "Person",
    "clustering_coefficient",
    "LINK",
    true,
    undefined,
    undefined,
    "neighbor_edges",
  );
  assert.deepEqual(
    scores(forge.invokeDescriptor(descriptor)),
    scores(explicit),
  );
});

test("synchronous labels retain integer identities across descriptor dispatch", () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A', external_id:10}), (b:Person {name:'B', external_id:20}), (a)-[:LINK]->(b), (b)-[:LINK]->(a)",
  );
  const result = forge.cluster(
    "Person",
    "label_propagation",
    "LINK",
    true,
    undefined,
    undefined,
    1,
    "external_id",
  );
  assert.deepEqual(communities(result), [20, 10]);
  assert.deepEqual(
    communities(
      forge.cluster(
        "Person",
        "label_propagation",
        "LINK",
        true,
        undefined,
        undefined,
        2,
        "external_id",
      ),
    ),
    [10, 20],
  );
  const descriptor = forge.prepareClusterInvocation(
    "Person",
    "label_propagation",
    "LINK",
    true,
    undefined,
    1,
    "external_id",
  );
  assert.deepEqual(
    communities(forge.invokeDescriptor(descriptor)),
    communities(result),
  );
  assert.deepEqual(
    communities(forge.invokeDescriptorBytes(descriptor.canonicalBytes)),
    communities(result),
  );
  for (const call of [
    () =>
      forge.rank(
        "Person",
        "clustering_coefficient",
        undefined,
        true,
        undefined,
        undefined,
        undefined,
        "invalid",
      ),
    () =>
      forge.prepareRankInvocation(
        "Person",
        "clustering_coefficient",
        undefined,
        true,
        undefined,
        undefined,
        "invalid",
      ),
    () =>
      forge.cluster(
        "Person",
        "label_propagation",
        undefined,
        true,
        undefined,
        undefined,
        undefined,
        "external_id",
      ),
    () =>
      forge.prepareClusterInvocation(
        "Person",
        "label_propagation",
        undefined,
        true,
        undefined,
        undefined,
        "external_id",
      ),
  ]) {
    assert.throws(call, (error) => error.code === "ValidationError");
  }
});

test("resolved belief projection forwards every optional algorithm mode", async () => {
  const forge = new GraphForge();
  for (const capabilityId of ["provenance", "knowledge", "epistemic"]) {
    await forge.enableCapability({
      operationUuid: randomUUID(),
      capabilityId,
      capabilityVersion: 1,
    });
  }
  const node = forge.addNode("Person", { name: "A", external_id: 42 });
  await forge.createAssertionWithStatus({
    operationUuid: randomUUID(),
    assertionUuid: randomUUID(),
    claim: "A participates in the graph",
    graphRefs: [
      { graphUuid: node.uuid, graphKind: "node", role: "subject", ordinal: 0 },
    ],
    statusEventUuid: randomUUID(),
    status: "supported",
  });
  const projection = await forge.resolveBeliefProjection({
    transactionCutoffMicros: Number.MAX_SAFE_INTEGER,
    policy: {
      includedStatuses: ["supported"],
      statusless: "exclude",
      supersessionBranches: "include_all_leaves",
      hypotheses: "exclude_unselected_group",
    },
  });
  const descriptors = [
    [
      projection.prepareRankInvocation(
        "Person",
        "pagerank",
        undefined,
        true,
        0.5,
        1,
      ),
      "score",
      1,
    ],
    [
      projection.prepareRankInvocation(
        "Person",
        "clustering_coefficient",
        undefined,
        true,
        undefined,
        undefined,
        "neighbor_edges",
      ),
      "score",
      0,
    ],
    [
      projection.prepareClusterInvocation(
        "Person",
        "label_propagation",
        undefined,
        false,
        undefined,
        1,
        "external_id",
      ),
      "community_id",
      42n,
    ],
  ];
  for (const [descriptor, column, expected] of descriptors) {
    const result = await forge.invokeResolvedRecorded(projection, {
      operationUuid: randomUUID(),
      runUuid: randomUUID(),
      attachmentUuid: randomUUID(),
      descriptor,
    });
    assert.equal(tableFromIPC(result.result).getChild(column).get(0), expected);
    assert.equal(result.attachmentState, "attached");
  }
});

test("algorithm iteration counts reject JavaScript truncation and wrapping", () => {
  const forge = new GraphForge();
  forge.addNode("Person", { name: "A" });
  for (const invalid of [-1, 0.5, NaN, Infinity, 2 ** 32]) {
    for (const call of [
      () =>
        forge.rank(
          "Person",
          "pagerank",
          undefined,
          true,
          undefined,
          0.85,
          invalid,
        ),
      () =>
        forge.prepareRankInvocation(
          "Person",
          "pagerank",
          undefined,
          true,
          0.85,
          invalid,
        ),
      () =>
        forge.cluster(
          "Person",
          "label_propagation",
          undefined,
          false,
          undefined,
          undefined,
          invalid,
        ),
      () =>
        forge.prepareClusterInvocation(
          "Person",
          "label_propagation",
          undefined,
          false,
          undefined,
          invalid,
        ),
    ]) {
      assert.throws(
        call,
        (error) =>
          error.code === "ValidationError" &&
          error.message.includes("unsigned 32-bit integer"),
      );
    }
  }
});
