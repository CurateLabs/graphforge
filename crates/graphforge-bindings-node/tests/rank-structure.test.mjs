// Native acceptance for this coherent algorithm family.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function checkCelf() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), " +
      "(a)-[:OTHER]->(c), (a)-[:OTHER]->(c), (c)-[:OTHER]->(c)",
  );
  const table = tableFromIPC(forge.rank("Person", "celf", "KNOWS"));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "celf");
  assert.deepEqual(
    [...table.getChild("name").toArray()],
    ["Alice", "Bob", "Carol", "Dan"],
  );
  const scores = [...table.getChild("score").toArray()];
  assert.ok(scores.every((score) => Number.isFinite(score) && score >= 0));
  assert.ok(
    Math.abs(scores.reduce((sum, score) => sum + score, 0) - 4) <= 1e-12,
  );
  assert.ok(Math.abs(scores[3] - 1) <= 1e-12);
  assert.deepEqual(scores, [
    ...tableFromIPC(forge.rank("Person", "celf", "KNOWS"))
      .getChild("score")
      .toArray(),
  ]);
  const undirected = [
    ...tableFromIPC(forge.rank("Person", "celf", "KNOWS", false))
      .getChild("score")
      .toArray(),
  ];
  assert.ok(
    Math.abs(undirected.reduce((sum, score) => sum + score, 0) - 4) <= 1e-12,
  );
  assert.notDeepEqual(undirected, scores);
  const allEdges = [
    ...tableFromIPC(forge.rank("Person", "celf")).getChild("score").toArray(),
  ];
  assert.ok(
    Math.abs(allEdges.reduce((sum, score) => sum + score, 0) - 4) <= 1e-12,
  );
  assert.notDeepEqual(allEdges, scores);
  assert.equal(
    tableFromIPC(forge.rank("Person", "celf", undefined, true, "celf_score"))
      .numRows,
    4,
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.celf_score IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    4,
  );
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "celf")).numRows,
    0,
  );
}

function checkClusteringCoefficient() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(e:Person {name:'Eve'}), (a)-[:KNOWS]->(b), " +
      "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), " +
      "(c)-[:KNOWS]->(c), (d)-[:KNOWS]->(e), (a)-[:OTHER]->(d)",
  );
  const table = tableFromIPC(
    forge.rank("Person", "clustering_coefficient", "KNOWS"),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "clustering_coefficient",
  );
  assert.deepEqual(
    [...table.getChild("name").toArray()],
    ["Alice", "Bob", "Carol", "Dan", "Eve"],
  );
  const scores = [...table.getChild("score").toArray()];
  assert.deepEqual(scores, [0.5, 0.5, 0.5, 0, 0]);
  assert.deepEqual(
    [
      ...tableFromIPC(
        forge.rank("Person", "local_clustering_coefficient", "KNOWS"),
      )
        .getChild("score")
        .toArray(),
    ],
    scores,
  );
  assert.deepEqual(
    [
      ...tableFromIPC(
        forge.rank("Person", "clustering_coefficient", "KNOWS", false),
      )
        .getChild("score")
        .toArray(),
    ],
    [1, 1, 1, 0, 0],
  );
  assert.notDeepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "clustering_coefficient"))
        .getChild("score")
        .toArray(),
    ],
    scores,
  );
  assert.equal(
    tableFromIPC(
      forge.rank(
        "Person",
        "clustering_coefficient",
        undefined,
        true,
        "clustering",
      ),
    ).numRows,
    5,
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.clustering IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    5,
  );
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "clustering_coefficient"))
      .numRows,
    0,
  );
}

function checkTriangles() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(e:Person {name:'Eve'}), (f:Person {name:'Finn'}), " +
      "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), " +
      "(b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), " +
      "(d)-[:KNOWS]->(a), (c)-[:KNOWS]->(c), (e)-[:KNOWS]->(f), " +
      "(b)-[:OTHER]->(d)",
  );
  const table = tableFromIPC(forge.rank("Person", "triangles", "KNOWS"));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "triangles");
  assert.deepEqual(
    [...table.getChild("name").toArray()],
    ["Alice", "Bob", "Carol", "Dan", "Eve", "Finn"],
  );
  const scores = [...table.getChild("score").toArray()];
  assert.deepEqual(scores, [2, 1, 2, 1, 0, 0]);
  assert.deepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "triangles", "KNOWS"))
        .getChild("score")
        .toArray(),
    ],
    scores,
  );
  assert.deepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "triangles", "KNOWS", false))
        .getChild("score")
        .toArray(),
    ],
    scores,
  );
  assert.deepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "triangles"))
        .getChild("score")
        .toArray(),
    ],
    [3, 3, 3, 3, 0, 0],
  );
  assert.equal(
    tableFromIPC(
      forge.rank("Person", "triangles", undefined, true, "triangle_count"),
    ).numRows,
    6,
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.triangle_count IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    6,
  );
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "triangles")).numRows,
    0,
  );
}

function checkKCore() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), " +
      "(c:Person {name:'C'}), (d:Person {name:'D'}), " +
      "(e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(g:Person {name:'G'}), (h:Person {name:'H'}), " +
      "(i:Person {name:'I'}), (j:Person {name:'J'}), " +
      "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a), " +
      "(a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), (b)-[:KNOWS]->(c), " +
      "(b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), (c)-[:KNOWS]->(c), " +
      "(a)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), " +
      "(h)-[:KNOWS]->(i), (i)-[:KNOWS]->(j), (j)-[:KNOWS]->(h), " +
      "(f)-[:OTHER]->(a)",
  );
  const table = tableFromIPC(forge.rank("Person", "k_core", "KNOWS"));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "k_core");
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDEFGHIJ"]);
  const scores = [...table.getChild("score").toArray()];
  assert.deepEqual(scores, [3, 3, 3, 3, 1, 1, 0, 2, 2, 2]);
  assert.deepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "k_core", "KNOWS"))
        .getChild("score")
        .toArray(),
    ],
    scores,
  );
  assert.deepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "k_core", "KNOWS", false))
        .getChild("score")
        .toArray(),
    ],
    scores,
  );
  assert.deepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "k_core"))
        .getChild("score")
        .toArray(),
    ],
    [3, 3, 3, 3, 2, 2, 0, 2, 2, 2],
  );
  assert.equal(
    tableFromIPC(forge.rank("Person", "k_core", undefined, true, "core"))
      .numRows,
    10,
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.core IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    10,
  );
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "k_core")).numRows,
    0,
  );
}

test("celf", checkCelf);
test("clustering coefficient", checkClusteringCoefficient);
test("triangles", checkTriangles);
test("k core", checkKCore);
