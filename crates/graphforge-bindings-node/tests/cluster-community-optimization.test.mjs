// Native acceptance for this coherent algorithm family.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function checkModularityOptimizationCluster() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), " +
      "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), " +
      "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), " +
      "(e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d)",
  );
  const table = tableFromIPC(
    forge.cluster("Person", "modularity_optimization", "KNOWS", true),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "modularity_optimization",
  );
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDEFG"]);
  assert.deepEqual(
    [...table.getChild("community_id").toArray()],
    [0n, 0n, 0n, 1n, 1n, 1n, 2n],
  );
}

function checkFastGreedyCluster() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), " +
      "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), " +
      "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), " +
      "(e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d)",
  );
  const table = tableFromIPC(
    forge.cluster("Person", "fastgreedy", "KNOWS", true),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "fastgreedy");
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDEFG"]);
  assert.deepEqual(
    [...table.getChild("community_id").toArray()],
    [0n, 0n, 0n, 1n, 1n, 1n, 2n],
  );
}

function checkInfoMapCluster() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), " +
      "(d:Person {name:'D'}), (e:Person {name:'E'}), (a)-[:KNOWS]->(b), " +
      "(b)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(c)",
  );
  const table = tableFromIPC(forge.cluster("Person", "infomap", "KNOWS", true));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "infomap");
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDE"]);
  assert.deepEqual(
    [...table.getChild("community_id").toArray()],
    [0n, 0n, 1n, 1n, 2n],
  );
}

function checkLeadingEigenvectorCluster() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), " +
      "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), " +
      "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), " +
      "(e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d)",
  );
  const table = tableFromIPC(
    forge.cluster("Person", "leading_eigenvector", "KNOWS", true),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "leading_eigenvector",
  );
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDEFG"]);
  assert.deepEqual(
    [...table.getChild("community_id").toArray()],
    [0n, 0n, 0n, 1n, 1n, 1n, 2n],
  );
}

function checkWalktrapCluster() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), " +
      "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), " +
      "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), " +
      "(e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d)",
  );
  const table = tableFromIPC(
    forge.cluster("Person", "walktrap", "KNOWS", true),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "walktrap");
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDEFG"]);
  assert.deepEqual(
    [...table.getChild("community_id").toArray()],
    [0n, 0n, 0n, 1n, 1n, 1n, 2n],
  );
}

function checkSpinglassCluster() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), " +
      "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), " +
      "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), " +
      "(e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d)",
  );
  const table = tableFromIPC(
    forge.cluster("Person", "spinglass", "KNOWS", true),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "spinglass");
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDEFG"]);
  assert.deepEqual(
    [...table.getChild("community_id").toArray()],
    [0n, 0n, 0n, 1n, 1n, 1n, 2n],
  );
}

test("modularity optimization cluster", checkModularityOptimizationCluster);
test("fast greedy cluster", checkFastGreedyCluster);
test("info map cluster", checkInfoMapCluster);
test("leading eigenvector cluster", checkLeadingEigenvectorCluster);
test("walktrap cluster", checkWalktrapCluster);
test("spinglass cluster", checkSpinglassCluster);
