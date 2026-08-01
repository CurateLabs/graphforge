// Native acceptance for this coherent algorithm family.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function checkNodeSimilarity() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), " +
      "(c:Person {name: 'Carol'}), (d:Person {name: 'Dan'}), (e:Person {name: 'Eve'}), " +
      "(a)-[:KNOWS]->(d), (a)-[:KNOWS]->(e), (a)-[:KNOWS]->(e), " +
      "(b)-[:KNOWS]->(d), (b)-[:KNOWS]->(e), (c)-[:KNOWS]->(d), " +
      "(a)-[:OTHER]->(d), (c)-[:OTHER]->(d)",
  );
  const table = tableFromIPC(
    forge.similar("Person", "node_similarity", 2, undefined, "KNOWS"),
  );
  const leftField = table.schema.fields.find(
    (field) => field.name === "node1_uuid",
  );
  const rightField = table.schema.fields.find(
    (field) => field.name === "node2_uuid",
  );
  const scoreField = table.schema.fields.find(
    (field) => field.name === "similarity",
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node1_uuid", "node2_uuid", "similarity"],
  );
  assert.equal(String(leftField?.type), "FixedSizeBinary[16]");
  assert.equal(String(rightField?.type), "FixedSizeBinary[16]");
  assert.equal(String(scoreField?.type), "Float64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "node_similarity",
  );

  const identities = tableFromIPC(
    forge.execute(
      "MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name",
    ),
  );
  const uuids = Array.from(identities.getChild("uuid"), (value) =>
    Buffer.from(value).toString("hex"),
  );
  const expected = [
    [0, 1],
    [0, 2],
    [1, 0],
    [1, 2],
    [2, 0],
    [2, 1],
  ];
  assert.deepEqual(
    Array.from(table.getChild("node1_uuid"), (value) =>
      Buffer.from(value).toString("hex"),
    ),
    expected.map(([left]) => uuids[left]),
  );
  assert.deepEqual(
    Array.from(table.getChild("node2_uuid"), (value) =>
      Buffer.from(value).toString("hex"),
    ),
    expected.map(([, right]) => uuids[right]),
  );
  assert.deepEqual(
    [...table.getChild("similarity").toArray()],
    [1, 0.5, 1, 0.5, 0.5, 0.5],
  );
  assert.equal(
    tableFromIPC(
      forge.similar("Person", "node_similarity", 1, undefined, "KNOWS"),
    ).numRows,
    3,
  );
  assert.equal(
    tableFromIPC(
      forge.similar("Person", "node_similarity", undefined, undefined, "OTHER"),
    ).numRows,
    2,
  );
  assert.equal(
    tableFromIPC(new GraphForge().similar("Person", "node_similarity")).numRows,
    0,
  );
  assert.throws(
    () => forge.similar("Person", "node_similarity", 0),
    (error) => error.code === "ValidationError",
  );
  assert.throws(
    () => forge.similar("Person", "node_similarity", undefined, "embedding"),
    (error) => error.code === "ValidationError",
  );
  assert.throws(
    () => forge.similar("Person", "knn"),
    (error) => error.code === "ValidationError",
  );
}

function checkFilteredNodeSimilarity() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), (e:Person {name:'Eve'}), " +
      "(a)-[:KNOWS]->(a), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c), " +
      "(a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), (b)-[:KNOWS]->(a), " +
      "(b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), " +
      "(d)-[:KNOWS]->(c)",
  );
  const run = () =>
    tableFromIPC(
      forge.similar(
        "Person",
        "filtered_node_similarity",
        2,
        undefined,
        "KNOWS",
      ),
    );
  const table = run();
  assert.deepEqual(
    table.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [
      ["node1_uuid", "FixedSizeBinary[16]", false],
      ["node2_uuid", "FixedSizeBinary[16]", false],
      ["similarity", "Float64", false],
    ],
  );
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "filtered_node_similarity",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "similar");
  const identities = tableFromIPC(
    forge.execute(
      "MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name",
    ),
  );
  const uuids = Array.from(identities.getChild("uuid"), (value) =>
    Buffer.from(value).toString("hex"),
  );
  const rows = (value) =>
    Array.from({ length: value.numRows }, (_, row) => [
      Buffer.from(value.getChild("node1_uuid").get(row)).toString("hex"),
      Buffer.from(value.getChild("node2_uuid").get(row)).toString("hex"),
      value.getChild("similarity").get(row),
    ]);
  const expected = [
    [0, 1],
    [0, 2],
    [1, 0],
    [1, 2],
  ];
  assert.deepEqual(
    rows(table).map(([left, right]) => [left, right]),
    expected.map(([left, right]) => [uuids[left], uuids[right]]),
  );
  const scores = [0.75, 0.25, 0.75, 1 / 3];
  assert.ok(
    rows(table).every(
      ([, , score], index) => Math.abs(score - scores[index]) < 1e-12,
    ),
  );
  assert.deepEqual(rows(run()), rows(table));
  assert.equal(
    tableFromIPC(
      forge.similar(
        "Person",
        "filtered_node_similarity",
        undefined,
        undefined,
        "KNOWS",
      ),
    ).numRows,
    6,
  );
  assert.equal(
    tableFromIPC(forge.similar("Person", "filtered_node_similarity", 2))
      .numRows,
    4,
  );
  assert.equal(
    tableFromIPC(
      forge.similar(
        "Person",
        "filtered_node_similarity",
        2,
        undefined,
        "MISSING",
      ),
    ).numRows,
    0,
  );

  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (c:Person {name:'Carol'}) CREATE (c)-[:KNOWS]->(a)",
  );
  assert.equal(run().numRows, 5);
  assert.equal(
    tableFromIPC(new GraphForge().similar("Person", "filtered_node_similarity"))
      .numRows,
    0,
  );
  for (const args of [
    ["Person", "filtered_node_similarity", 0],
    ["Person", "filtered_node_similarity", 2, undefined, " "],
    ["Person", "filtered_node_similarity", 2, "embedding"],
  ]) {
    assert.throws(
      () => forge.similar(...args),
      (error) => error.code === "ValidationError",
    );
  }
}

test("node similarity", checkNodeSimilarity);
test("filtered node similarity", checkFilteredNodeSimilarity);
