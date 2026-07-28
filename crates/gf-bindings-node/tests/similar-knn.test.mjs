// Native acceptance for this coherent algorithm family.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge, version } from "../index.js";

function checkKnn() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (:Person {name:'a', embedding:[1.0, 0.0]}), " +
      "(:Person {name:'b', embedding:[1.0, 0.0]}), " +
      "(:Person {name:'c', embedding:[1.0, 1.0]}), " +
      "(:Person {name:'d', embedding:[0.0, 1.0]}), " +
      "(:Person {name:'e', embedding:[-1.0, 0.0]})",
  );
  const result = () =>
    tableFromIPC(forge.similar("Person", "knn", 2, "embedding"));
  const table = result();
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
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "knn");
  assert.equal(table.schema.metadata.get("graphforge.verb"), "similar");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
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
    [3, 2],
    [3, 0],
    [4, 3],
  ];
  const rows = (value) =>
    Array.from({ length: value.numRows }, (_, row) => [
      Buffer.from(value.getChild("node1_uuid").get(row)).toString("hex"),
      Buffer.from(value.getChild("node2_uuid").get(row)).toString("hex"),
      value.getChild("similarity").get(row),
    ]);
  assert.deepEqual(
    rows(table).map(([left, right]) => [left, right]),
    expected.map(([left, right]) => [uuids[left], uuids[right]]),
  );
  const scores = rows(table).map(([, , score]) => score);
  const expectedScores = [
    1,
    Math.SQRT1_2,
    1,
    Math.SQRT1_2,
    Math.SQRT1_2,
    Math.SQRT1_2,
    Math.SQRT1_2,
    0,
    0,
  ];
  assert.ok(
    scores.every(
      (score, index) => Math.abs(score - expectedScores[index]) < 1e-12,
    ),
  );
  assert.equal(
    tableFromIPC(forge.similar("Person", "knn", undefined, "embedding"))
      .numRows,
    14,
  );

  forge.execute(
    "MATCH (a:Person {name:'a'}), (b:Person {name:'b'}) " +
      "CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a)",
  );
  assert.deepEqual(rows(result()), rows(table));
  assert.equal(
    tableFromIPC(new GraphForge().similar("Person", "knn", 2, "embedding"))
      .numRows,
    0,
  );
  assert.throws(
    () => forge.similar("Person", "knn"),
    (error) => error.code === "ValidationError",
  );
  assert.throws(
    () => forge.similar("Person", "knn", 2, "embedding", "KNOWS"),
    (error) => error.code === "ValidationError",
  );
  for (const cypher of [
    "CREATE (:Person {embedding:[0.0, 0.0]})",
    "CREATE (:Person {embedding:[1.0]}), (:Person {embedding:[1.0, 2.0]})",
  ]) {
    const invalid = new GraphForge();
    invalid.execute(cypher);
    assert.throws(
      () => invalid.similar("Person", "knn", 2, "embedding"),
      (error) => error.code === "ValidationError",
    );
  }
}

function checkFilteredKnn() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (:Person {name:'a', embedding:[1.0, 0.0]}), " +
      "(:Person {name:'b', embedding:[1.0, 0.0]}), " +
      "(:Person {name:'c', embedding:[1.0, 1.0]}), " +
      "(:Person {name:'d', embedding:[0.0, 1.0]}), " +
      "(:Person {name:'e', embedding:[-1.0, 0.0]})",
  );
  forge.execute(
    "MATCH (a:Person {name:'a'}), (b:Person {name:'b'}), " +
      "(c:Person {name:'c'}), (d:Person {name:'d'}), (e:Person {name:'e'}) " +
      "CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c), " +
      "(a)-[:KNOWS]->(a), (a)-[:OTHER]->(e), (b)-[:OTHER]->(a), " +
      "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(b), (d)-[:KNOWS]->(c), " +
      "(d)-[:KNOWS]->(a), (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(d)",
  );
  const run = () =>
    tableFromIPC(
      forge.similar("Person", "filtered_knn", 2, "embedding", "KNOWS"),
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
    "filtered_knn",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "similar");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );
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
    [2, 0],
    [2, 1],
    [3, 2],
    [3, 0],
    [4, 3],
  ];
  assert.deepEqual(
    rows(table).map(([left, right]) => [left, right]),
    expected.map(([left, right]) => [uuids[left], uuids[right]]),
  );
  const expectedScores = [
    1,
    Math.SQRT1_2,
    Math.SQRT1_2,
    Math.SQRT1_2,
    Math.SQRT1_2,
    0,
    0,
  ];
  assert.ok(
    rows(table).every(
      ([, , score], index) => Math.abs(score - expectedScores[index]) < 1e-12,
    ),
  );
  assert.deepEqual(rows(run()), rows(table));
  assert.equal(
    tableFromIPC(
      forge.similar("Person", "filtered_knn", undefined, "embedding", "KNOWS"),
    ).numRows,
    8,
  );
  assert.equal(
    tableFromIPC(forge.similar("Person", "filtered_knn", 2, "embedding"))
      .numRows,
    8,
  );
  assert.equal(
    tableFromIPC(
      forge.similar("Person", "filtered_knn", 2, "embedding", "MISSING"),
    ).numRows,
    0,
  );

  forge.execute(
    "MATCH (a:Person {name:'a'}), (b:Person {name:'b'}) CREATE (b)-[:KNOWS]->(a)",
  );
  assert.equal(run().numRows, 8);
  assert.equal(
    tableFromIPC(
      new GraphForge().similar("Person", "filtered_knn", 2, "embedding"),
    ).numRows,
    0,
  );
  for (const args of [
    ["Person", "filtered_knn"],
    ["Person", "filtered_knn", 0, "embedding"],
    ["Person", "filtered_knn", 2, "embedding", " "],
  ]) {
    assert.throws(
      () => forge.similar(...args),
      (error) => error.code === "ValidationError",
    );
  }
  for (const cypher of [
    "CREATE (:Person {embedding:[0.0, 0.0]})",
    "CREATE (:Person {embedding:[1.0]}), (:Person {embedding:[1.0, 2.0]})",
  ]) {
    const invalid = new GraphForge();
    invalid.execute(cypher);
    assert.throws(
      () => invalid.similar("Person", "filtered_knn", 2, "embedding"),
      (error) => error.code === "ValidationError",
    );
  }
}

function checkCosine() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (:Person {name:'a', embedding:[1.0, 0.0]}), " +
      "(:Person {name:'b', embedding:[0.0, 1.0]}), " +
      "(:Person {name:'c', embedding:[-1.0, 0.0]}), " +
      "(:Person {name:'d', embedding:[-1.0, -1.0]})",
  );
  const run = () =>
    tableFromIPC(forge.similar("Person", "cosine", 3, "embedding"));
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
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "cosine");
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
    [0, 3],
    [0, 2],
    [1, 0],
    [1, 2],
    [1, 3],
    [2, 3],
    [2, 1],
    [2, 0],
    [3, 2],
    [3, 0],
    [3, 1],
  ];
  assert.deepEqual(
    rows(table).map(([left, right]) => [left, right]),
    expected.map(([left, right]) => [uuids[left], uuids[right]]),
  );
  const expectedScores = [
    0,
    -Math.SQRT1_2,
    -1,
    0,
    0,
    -Math.SQRT1_2,
    Math.SQRT1_2,
    0,
    -1,
    Math.SQRT1_2,
    -Math.SQRT1_2,
    -Math.SQRT1_2,
  ];
  assert.ok(
    rows(table).every(
      ([, , score], index) => Math.abs(score - expectedScores[index]) < 1e-12,
    ),
  );
  assert.deepEqual(rows(run()), rows(table));
  assert.equal(
    tableFromIPC(forge.similar("Person", "cosine", undefined, "embedding"))
      .numRows,
    12,
  );
  assert.equal(
    tableFromIPC(forge.similar("Person", "cosine", 2, "embedding")).numRows,
    8,
  );
  forge.execute(
    "MATCH (a:Person {name:'a'}), (b:Person {name:'b'}) CREATE (a)-[:KNOWS]->(b)",
  );
  assert.deepEqual(rows(run()), rows(table));
  assert.equal(
    tableFromIPC(new GraphForge().similar("Person", "cosine", 3, "embedding"))
      .numRows,
    0,
  );

  const invalidCalls = [
    () => forge.similar("Person", "cosine"),
    () => forge.similar("Person", "cosine", 0, "embedding"),
    () => forge.similar("Person", "cosine", 3, " embedding"),
    () => forge.similar("Person", "cosine", 3, "embedding", "KNOWS"),
  ];
  for (const cypher of [
    "CREATE (:Person {name:'missing'})",
    "CREATE (:Person {embedding:[0.0, 0.0]})",
    "CREATE (:Person {embedding:[1.0]}), (:Person {embedding:[1.0, 2.0]})",
  ]) {
    const invalid = new GraphForge();
    invalid.execute(cypher);
    invalidCalls.push(() =>
      invalid.similar("Person", "cosine", 3, "embedding"),
    );
  }
  assert.throws(
    () => new GraphForge().addNode("Person", { embedding: [Number.NaN] }),
    (error) => error.code === "InvalidArg",
  );
  for (const call of invalidCalls) {
    assert.throws(call, (error) => error.code === "ValidationError");
  }
}

test("knn", checkKnn);
test("filtered knn", checkFilteredKnn);
test("cosine", checkCosine);
