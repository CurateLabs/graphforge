// Native acceptance for this coherent algorithm family.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function checkPreferentialAttachment() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), " +
      "(c:Person {name:'C'}), (d:Person {name:'D'}), " +
      "(e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c), " +
      "(a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), " +
      "(d)-[:KNOWS]->(c), (e)-[:OTHER]->(f)",
  );
  const table = tableFromIPC(
    forge.rank("Person", "preferential_attachment", "KNOWS"),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "preferential_attachment",
  );
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDEF"]);
  assert.deepEqual([...table.getChild("score").toArray()], [2, 3, 2, 3, 0, 0]);
  const repeated = tableFromIPC(
    forge.rank("Person", "preferential_attachment", "KNOWS"),
  );
  assert.deepEqual(
    [...repeated.getChild("name").toArray()],
    [...table.getChild("name").toArray()],
  );
  assert.deepEqual(
    [...repeated.getChild("score").toArray()],
    [...table.getChild("score").toArray()],
  );
  assert.deepEqual(
    [
      ...tableFromIPC(
        forge.rank("Person", "preferential_attachment", "KNOWS", false),
      )
        .getChild("score")
        .toArray(),
    ],
    [2, 2, 0, 4, 0, 0],
  );
  assert.deepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "preferential_attachment"))
        .getChild("score")
        .toArray(),
    ],
    [4, 4, 3, 4, 5, 0],
  );
  assert.equal(
    tableFromIPC(
      forge.rank("Person", "preferential_attachment", "KNOWS", true, "pa"),
    ).numRows,
    6,
  );
  const persisted = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) WHERE n.pa IS NOT NULL RETURN n.name AS name, n.pa AS pa ORDER BY name",
    ),
  );
  assert.deepEqual([...persisted.getChild("name").toArray()], [..."ABCDEF"]);
  assert.deepEqual([...persisted.getChild("pa").toArray()], [2, 3, 2, 3, 0, 0]);
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "preferential_attachment"))
      .numRows,
    0,
  );
}

function checkAdamicAdar() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), " +
      "(c:Person {name:'C'}), (d:Person {name:'D'}), " +
      "(e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), " +
      "(a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), " +
      "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(e), (d)-[:KNOWS]->(e), " +
      "(a)-[:OTHER]->(f), (b)-[:OTHER]->(f)",
  );
  const table = tableFromIPC(forge.rank("Person", "adamic_adar", "KNOWS"));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "adamic_adar",
  );
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDEF"]);
  const inverseLogTwo = 1 / Math.log(2);
  const expected = [
    2 * inverseLogTwo,
    2 * inverseLogTwo,
    inverseLogTwo,
    inverseLogTwo,
    0,
    0,
  ];
  const scores = [...table.getChild("score").toArray()];
  assert.ok(
    scores.every((score, index) => Math.abs(score - expected[index]) <= 1e-12),
  );
  const repeated = tableFromIPC(forge.rank("Person", "adamic_adar", "KNOWS"));
  assert.deepEqual(
    [...repeated.getChild("name").toArray()],
    [...table.getChild("name").toArray()],
  );
  assert.deepEqual([...repeated.getChild("score").toArray()], scores);
  assert.notDeepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "adamic_adar", "KNOWS", false))
        .getChild("score")
        .toArray(),
    ],
    scores,
  );
  assert.notDeepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "adamic_adar"))
        .getChild("score")
        .toArray(),
    ],
    scores,
  );
  assert.equal(
    tableFromIPC(forge.rank("Person", "adamic_adar", "KNOWS", true, "adamic"))
      .numRows,
    6,
  );
  const persisted = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) WHERE n.adamic IS NOT NULL RETURN n.name AS name, n.adamic AS score ORDER BY name",
    ),
  );
  assert.deepEqual([...persisted.getChild("name").toArray()], [..."ABCDEF"]);
  assert.ok(
    [...persisted.getChild("score").toArray()].every(
      (score, index) => Math.abs(score - expected[index]) <= 1e-12,
    ),
  );
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "adamic_adar")).numRows,
    0,
  );
}

function checkCommonNeighbors() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), " +
      "(c:Person {name:'C'}), (d:Person {name:'D'}), " +
      "(e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), " +
      "(a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), " +
      "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(e), (d)-[:KNOWS]->(e), " +
      "(a)-[:OTHER]->(f), (b)-[:OTHER]->(f)",
  );
  const table = tableFromIPC(forge.rank("Person", "common_neighbors", "KNOWS"));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "common_neighbors",
  );
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDEF"]);
  assert.deepEqual([...table.getChild("score").toArray()], [2, 2, 1, 1, 0, 0]);
  const repeated = tableFromIPC(
    forge.rank("Person", "common_neighbors", "KNOWS"),
  );
  assert.deepEqual(
    [...repeated.getChild("name").toArray()],
    [...table.getChild("name").toArray()],
  );
  assert.deepEqual(
    [...repeated.getChild("score").toArray()],
    [...table.getChild("score").toArray()],
  );
  assert.deepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "common_neighbors", "KNOWS", false))
        .getChild("score")
        .toArray(),
    ],
    [4, 4, 3, 3, 4, 0],
  );
  assert.deepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "common_neighbors"))
        .getChild("score")
        .toArray(),
    ],
    [3, 3, 1, 1, 0, 0],
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.common IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    0,
  );
  assert.equal(
    tableFromIPC(
      forge.rank("Person", "common_neighbors", "KNOWS", true, "common"),
    ).numRows,
    6,
  );
  const persisted = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) WHERE n.common IS NOT NULL RETURN n.name AS name, n.common AS score ORDER BY name",
    ),
  );
  assert.deepEqual(
    [...persisted.getChild("score").toArray()],
    [2, 2, 1, 1, 0, 0],
  );
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "common_neighbors")).numRows,
    0,
  );
}

function checkResourceAllocation() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), " +
      "(c:Person {name:'C'}), (d:Person {name:'D'}), " +
      "(e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), " +
      "(a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), " +
      "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(e), (d)-[:KNOWS]->(e), " +
      "(a)-[:OTHER]->(f), (b)-[:OTHER]->(f)",
  );
  const table = tableFromIPC(
    forge.rank("Person", "resource_allocation", "KNOWS"),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "resource_allocation",
  );
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDEF"]);
  assert.deepEqual(
    [...table.getChild("score").toArray()],
    [1, 1, 0.5, 0.5, 0, 0],
  );
  const repeated = tableFromIPC(
    forge.rank("Person", "resource_allocation", "KNOWS"),
  );
  assert.deepEqual(
    [...repeated.getChild("name").toArray()],
    [...table.getChild("name").toArray()],
  );
  assert.deepEqual(
    [...repeated.getChild("score").toArray()],
    [...table.getChild("score").toArray()],
  );
  const undirected = [
    ...tableFromIPC(forge.rank("Person", "resource_allocation", "KNOWS", false))
      .getChild("score")
      .toArray(),
  ];
  const expectedUndirected = [4 / 3, 4 / 3, 1.5, 1.5, 4 / 3, 0];
  assert.ok(
    undirected.every(
      (score, index) => Math.abs(score - expectedUndirected[index]) <= 1e-12,
    ),
  );
  assert.deepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "resource_allocation"))
        .getChild("score")
        .toArray(),
    ],
    [1.5, 1.5, 0.5, 0.5, 0, 0],
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.resource IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    0,
  );
  assert.equal(
    tableFromIPC(
      forge.rank("Person", "resource_allocation", "KNOWS", true, "resource"),
    ).numRows,
    6,
  );
  const persisted = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) WHERE n.resource IS NOT NULL RETURN n.name AS name, n.resource AS score ORDER BY name",
    ),
  );
  assert.deepEqual(
    [...persisted.getChild("score").toArray()],
    [1, 1, 0.5, 0.5, 0, 0],
  );
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "resource_allocation"))
      .numRows,
    0,
  );
}

function checkTotalNeighbors() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), " +
      "(c:Person {name:'C'}), (d:Person {name:'D'}), " +
      "(e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), " +
      "(a)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), " +
      "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(e), (d)-[:KNOWS]->(e), " +
      "(a)-[:OTHER]->(f), (b)-[:OTHER]->(f)",
  );
  const table = tableFromIPC(forge.rank("Person", "total_neighbors", "KNOWS"));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "total_neighbors",
  );
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDEF"]);
  assert.deepEqual([...table.getChild("score").toArray()], [6, 6, 8, 9, 7, 7]);
  const repeated = tableFromIPC(
    forge.rank("Person", "total_neighbors", "KNOWS"),
  );
  assert.deepEqual(
    [...repeated.getChild("name").toArray()],
    [...table.getChild("name").toArray()],
  );
  assert.deepEqual(
    [...repeated.getChild("score").toArray()],
    [...table.getChild("score").toArray()],
  );
  assert.deepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "total_neighbors", "KNOWS", false))
        .getChild("score")
        .toArray(),
    ],
    [6, 6, 6, 6, 6, 12],
  );
  assert.deepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "total_neighbors"))
        .getChild("score")
        .toArray(),
    ],
    [6, 6, 9, 11, 9, 9],
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.total IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    0,
  );
  assert.equal(
    tableFromIPC(
      forge.rank("Person", "total_neighbors", "KNOWS", true, "total"),
    ).numRows,
    6,
  );
  const persisted = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) WHERE n.total IS NOT NULL RETURN n.total AS score ORDER BY n.name",
    ),
  );
  assert.deepEqual(
    [...persisted.getChild("score").toArray()],
    [6, 6, 8, 9, 7, 7],
  );
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "total_neighbors")).numRows,
    0,
  );
}

test("preferential attachment", checkPreferentialAttachment);
test("adamic adar", checkAdamicAdar);
test("common neighbors", checkCommonNeighbors);
test("resource allocation", checkResourceAllocation);
test("total neighbors", checkTotalNeighbors);
