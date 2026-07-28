// Native acceptance for this coherent algorithm family.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function checkDegreeRank() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), (c:Person {name: 'Carol'}), " +
      "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (a)-[:OTHER]->(c)",
  );
  const table = tableFromIPC(forge.rank("Person", "degree"));
  assert.equal(table.numRows, 3);
  const uuidField = table.schema.fields.find(
    (field) => field.name === "node_uuid",
  );
  const scoreField = table.schema.fields.find(
    (field) => field.name === "score",
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(uuidField?.type), "FixedSizeBinary[16]");
  assert.equal(uuidField?.type.byteWidth, 16);
  assert.equal(String(scoreField?.type), "Float64");
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "degree");
  assert.deepEqual(
    [...table.getChild("name").toArray()],
    ["Alice", "Bob", "Carol"],
  );
  assert.deepEqual([...table.getChild("score").toArray()], [1.5, 0, 0]);

  const via = tableFromIPC(forge.rank("Person", "degree", "KNOWS"));
  assert.deepEqual([...via.getChild("score").toArray()], [1, 0, 0]);
  const undirected = tableFromIPC(
    forge.rank("Person", "degree", "KNOWS", false, "degree_score"),
  );
  assert.deepEqual([...undirected.getChild("score").toArray()], [1, 1, 0]);
  const written = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) RETURN n.name AS name, n.degree_score AS degree_score ORDER BY name",
    ),
  );
  assert.deepEqual(
    [...written.getChild("name").toArray()],
    ["Alice", "Bob", "Carol"],
  );
  assert.deepEqual([...written.getChild("degree_score").toArray()], [1, 1, 0]);
  assert.throws(
    () => forge.rank("Person", "not_a_rank"),
    (error) => error.code === "ValidationError",
  );
}

function checkPageRank() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (a)-[:KNOWS]->(b), " +
      "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(a), (a)-[:OTHER]->(c)",
  );
  const table = tableFromIPC(forge.rank("Person", "pagerank", "KNOWS"));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "pagerank");
  const scores = [...table.getChild("score").toArray()];
  assert.deepEqual(scores, [
    ...tableFromIPC(forge.rank("Person", "pagerank", "KNOWS"))
      .getChild("score")
      .toArray(),
  ]);
  assert.ok(Math.abs(scores.reduce((sum, score) => sum + score, 0) - 1) < 1e-9);
  assert.notDeepEqual(scores, [
    ...tableFromIPC(forge.rank("Person", "pagerank", undefined, false))
      .getChild("score")
      .toArray(),
  ]);
  assert.equal(
    tableFromIPC(forge.rank("Person", "pagerank", undefined, true, "page_rank"))
      .numRows,
    3,
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.page_rank IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    3,
  );
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "pagerank")).numRows,
    0,
  );
}

function checkBetweenness() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), " +
      "(a)-[:KNOWS]->(d), (d)-[:KNOWS]->(c), (b)-[:KNOWS]->(b), " +
      "(a)-[:OTHER]->(c)",
  );
  const table = tableFromIPC(forge.rank("Person", "betweenness", "KNOWS"));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "betweenness",
  );
  assert.deepEqual(
    [...table.getChild("name").toArray()],
    ["Alice", "Bob", "Carol", "Dan"],
  );
  const scores = [...table.getChild("score").toArray()];
  assert.deepEqual(scores, [0, 1 / 9, 0, 1 / 18]);
  assert.deepEqual(scores, [
    ...tableFromIPC(forge.rank("Person", "betweenness", "KNOWS"))
      .getChild("score")
      .toArray(),
  ]);
  assert.notDeepEqual(scores, [
    ...tableFromIPC(forge.rank("Person", "betweenness", undefined, false))
      .getChild("score")
      .toArray(),
  ]);
  assert.notDeepEqual(scores, [
    ...tableFromIPC(forge.rank("Person", "betweenness"))
      .getChild("score")
      .toArray(),
  ]);
  assert.equal(
    tableFromIPC(
      forge.rank("Person", "betweenness", undefined, true, "between"),
    ).numRows,
    4,
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.between IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    4,
  );
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "betweenness")).numRows,
    0,
  );
}

function checkCloseness() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), " +
      "(b)-[:KNOWS]->(b), (a)-[:OTHER]->(c)",
  );
  const table = tableFromIPC(forge.rank("Person", "closeness", "KNOWS"));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "closeness");
  assert.deepEqual(
    [...table.getChild("name").toArray()],
    ["Alice", "Bob", "Carol", "Dan"],
  );
  const scores = [...table.getChild("score").toArray()];
  assert.deepEqual(scores, [4 / 9, 1 / 3, 0, 0]);
  assert.deepEqual(scores, [
    ...tableFromIPC(forge.rank("Person", "closeness", "KNOWS"))
      .getChild("score")
      .toArray(),
  ]);
  assert.deepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "closeness", "KNOWS", false))
        .getChild("score")
        .toArray(),
    ],
    [4 / 9, 2 / 3, 4 / 9, 0],
  );
  assert.notDeepEqual(scores, [
    ...tableFromIPC(forge.rank("Person", "closeness"))
      .getChild("score")
      .toArray(),
  ]);
  assert.equal(
    tableFromIPC(
      forge.rank("Person", "closeness", undefined, true, "close_score"),
    ).numRows,
    4,
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.close_score IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    4,
  );
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "closeness")).numRows,
    0,
  );
}

function checkHarmonicCloseness() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), " +
      "(b)-[:KNOWS]->(b), (a)-[:OTHER]->(c)",
  );
  const table = tableFromIPC(
    forge.rank("Person", "harmonic_closeness", "KNOWS"),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "harmonic_closeness",
  );
  assert.deepEqual(
    [...table.getChild("name").toArray()],
    ["Alice", "Bob", "Carol", "Dan"],
  );
  const scores = [...table.getChild("score").toArray()];
  assert.deepEqual(scores, [0.5, 1 / 3, 0, 0]);
  assert.deepEqual(scores, [
    ...tableFromIPC(forge.rank("Person", "harmonic_closeness", "KNOWS"))
      .getChild("score")
      .toArray(),
  ]);
  assert.deepEqual(
    [
      ...tableFromIPC(
        forge.rank("Person", "harmonic_closeness", "KNOWS", false),
      )
        .getChild("score")
        .toArray(),
    ],
    [0.5, 2 / 3, 0.5, 0],
  );
  assert.notDeepEqual(scores, [
    ...tableFromIPC(forge.rank("Person", "harmonic_closeness"))
      .getChild("score")
      .toArray(),
  ]);
  assert.equal(
    tableFromIPC(
      forge.rank("Person", "harmonic_closeness", undefined, true, "harmonic"),
    ).numRows,
    4,
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.harmonic IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    4,
  );
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "harmonic_closeness")).numRows,
    0,
  );
}

test("degree rank", checkDegreeRank);
test("page rank", checkPageRank);
test("betweenness", checkBetweenness);
test("closeness", checkCloseness);
test("harmonic closeness", checkHarmonicCloseness);
