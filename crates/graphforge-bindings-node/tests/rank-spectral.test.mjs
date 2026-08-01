// Native acceptance for this coherent algorithm family.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function checkEigenvector() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(b), " +
      "(a)-[:OTHER]->(c)",
  );
  const table = tableFromIPC(forge.rank("Person", "eigenvector", "KNOWS"));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "eigenvector",
  );
  assert.deepEqual(
    [...table.getChild("name").toArray()],
    ["Alice", "Bob", "Carol", "Dan"],
  );
  const ratio = 3 * 2 ** 20 - 2;
  const norm = Math.sqrt(ratio * ratio + 3);
  const scores = [...table.getChild("score").toArray()];
  const expected = [1 / norm, ratio / norm, 1 / norm, 1 / norm];
  assert.ok(
    scores.every((score, index) => Math.abs(score - expected[index]) <= 1e-15),
  );
  assert.deepEqual(scores, [
    ...tableFromIPC(forge.rank("Person", "eigenvector", "KNOWS"))
      .getChild("score")
      .toArray(),
  ]);
  const undirected = [
    ...tableFromIPC(forge.rank("Person", "eigenvector", "KNOWS", false))
      .getChild("score")
      .toArray(),
  ];
  const phi = (1 + Math.sqrt(5)) / 2;
  const principalNorm = Math.sqrt(1 + phi * phi);
  assert.ok(Math.abs(undirected[0] - 1 / principalNorm) <= 1e-7);
  assert.ok(Math.abs(undirected[1] - phi / principalNorm) <= 1e-7);
  assert.ok(
    tableFromIPC(forge.rank("Person", "eigenvector")).getChild("score").get(2) >
      scores[0],
  );
  assert.equal(
    tableFromIPC(
      forge.rank("Person", "eigenvector", undefined, true, "eigen_score"),
    ).numRows,
    4,
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.eigen_score IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    4,
  );
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "eigenvector")).numRows,
    0,
  );
}

function checkArticleRank() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(a)-[:KNOWS]->(b), (a)-[:OTHER]->(c), " +
      "(a)-[:OTHER]->(c), (c)-[:OTHER]->(c)",
  );
  const table = tableFromIPC(forge.rank("Person", "article_rank", "KNOWS"));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "article_rank",
  );
  assert.deepEqual(
    [...table.getChild("name").toArray()],
    ["Alice", "Bob", "Carol", "Dan"],
  );
  const scores = [...table.getChild("score").toArray()];
  assert.ok(
    scores.every(
      (score, index) =>
        Math.abs(score - [0.15, 0.252, 0.15, 0.15][index]) <= 1e-15,
    ),
  );
  assert.deepEqual(scores, [
    ...tableFromIPC(forge.rank("Person", "article_rank", "KNOWS"))
      .getChild("score")
      .toArray(),
  ]);
  assert.notDeepEqual(scores, [
    ...tableFromIPC(forge.rank("Person", "article_rank", "KNOWS", false))
      .getChild("score")
      .toArray(),
  ]);
  assert.ok(
    tableFromIPC(forge.rank("Person", "article_rank"))
      .getChild("score")
      .get(2) > scores[1],
  );
  assert.equal(
    tableFromIPC(
      forge.rank("Person", "article_rank", undefined, true, "article_score"),
    ).numRows,
    4,
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.article_score IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    4,
  );
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "article_rank")).numRows,
    0,
  );
}

function checkHitsHub() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), " +
      "(a)-[:OTHER]->(c), (a)-[:OTHER]->(c), (c)-[:OTHER]->(c)",
  );
  const table = tableFromIPC(forge.rank("Person", "hits_hub", "KNOWS"));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "hits_hub");
  assert.deepEqual(
    [...table.getChild("name").toArray()],
    ["Alice", "Bob", "Carol", "Dan"],
  );
  const scores = [...table.getChild("score").toArray()];
  const expected = [1 / Math.sqrt(2), 1 / Math.sqrt(2), 0, 0];
  assert.ok(
    scores.every((score, index) => Math.abs(score - expected[index]) <= 1e-15),
  );
  assert.deepEqual(scores, [
    ...tableFromIPC(forge.rank("Person", "hits_hub", "KNOWS"))
      .getChild("score")
      .toArray(),
  ]);
  const undirected = [
    ...tableFromIPC(forge.rank("Person", "hits_hub", "KNOWS", false))
      .getChild("score")
      .toArray(),
  ];
  assert.ok(
    undirected
      .slice(0, 3)
      .every((score) => Math.abs(score - 1 / Math.sqrt(3)) <= 1e-12),
  );
  assert.ok(
    tableFromIPC(forge.rank("Person", "hits_hub")).getChild("score").get(2) > 0,
  );
  assert.equal(
    tableFromIPC(forge.rank("Person", "hits_hub", undefined, true, "hub_score"))
      .numRows,
    4,
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.hub_score IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    4,
  );
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "hits_hub")).numRows,
    0,
  );
}

function checkHitsAuthority() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), " +
      "(a)-[:OTHER]->(c), (a)-[:OTHER]->(c), (c)-[:OTHER]->(c)",
  );
  const table = tableFromIPC(forge.rank("Person", "hits_authority", "KNOWS"));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "score", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Float64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "hits_authority",
  );
  assert.deepEqual(
    [...table.getChild("name").toArray()],
    ["Alice", "Bob", "Carol", "Dan"],
  );
  const scores = [...table.getChild("score").toArray()];
  const expected = [0, 1 / Math.sqrt(2), 1 / Math.sqrt(2), 0];
  assert.ok(
    scores.every((score, index) => Math.abs(score - expected[index]) <= 1e-15),
  );
  assert.deepEqual(scores, [
    ...tableFromIPC(forge.rank("Person", "hits_authority", "KNOWS"))
      .getChild("score")
      .toArray(),
  ]);
  const undirected = [
    ...tableFromIPC(forge.rank("Person", "hits_authority", "KNOWS", false))
      .getChild("score")
      .toArray(),
  ];
  const expectedUndirected = [
    1 / Math.sqrt(6),
    2 / Math.sqrt(6),
    1 / Math.sqrt(6),
    0,
  ];
  assert.ok(
    undirected.every(
      (score, index) => Math.abs(score - expectedUndirected[index]) <= 1e-12,
    ),
  );
  assert.notDeepEqual(
    [
      ...tableFromIPC(forge.rank("Person", "hits_authority"))
        .getChild("score")
        .toArray(),
    ],
    scores,
  );
  assert.equal(
    tableFromIPC(
      forge.rank(
        "Person",
        "hits_authority",
        undefined,
        true,
        "authority_score",
      ),
    ).numRows,
    4,
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.authority_score IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    4,
  );
  assert.equal(
    tableFromIPC(new GraphForge().rank("Person", "hits_authority")).numRows,
    0,
  );
}

test("eigenvector", checkEigenvector);
test("article rank", checkArticleRank);
test("hits hub", checkHitsHub);
test("hits authority", checkHitsAuthority);
