// Native acceptance for this coherent algorithm family.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function checkHdbscanCluster() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (:Point {name:'a0', features:[0.0]}), (:Point {name:'a1', features:[0.1]}), " +
      "(:Point {name:'a2', features:[0.2]}), (:Point {name:'a3', features:[0.3]}), " +
      "(:Point {name:'a4', features:[0.4]}), (:Point {name:'b0', features:[10.0]}), " +
      "(:Point {name:'b1', features:[10.1]}), (:Point {name:'b2', features:[10.2]}), " +
      "(:Point {name:'b3', features:[10.3]}), (:Point {name:'b4', features:[10.4]}), " +
      "(:Point {name:'noise', features:[100.0]})",
  );
  const table = tableFromIPC(
    forge.cluster("Point", "hdbscan", undefined, true, undefined, "features"),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "features", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "hdbscan");
  assert.deepEqual(
    [...table.getChild("name").toArray()],
    ["a0", "a1", "a2", "a3", "a4", "b0", "b1", "b2", "b3", "b4", "noise"],
  );
  assert.deepEqual(
    [...table.getChild("community_id").toArray()],
    [0n, 0n, 0n, 0n, 0n, 1n, 1n, 1n, 1n, 1n, -1n],
  );
  const repeated = tableFromIPC(
    forge.cluster("Point", "hdbscan", undefined, false, undefined, "features"),
  );
  assert.deepEqual(
    [...repeated.getChild("node_uuid").toArray()],
    [...table.getChild("node_uuid").toArray()],
  );
  assert.deepEqual(
    [...repeated.getChild("community_id").toArray()],
    [...table.getChild("community_id").toArray()],
  );
}

function checkKMeansCluster() {
  const forge = new GraphForge();
  const values = Array.from(
    { length: 20 },
    (_, point) => Math.floor(point / 2) * 10 + (point % 2) * 0.25,
  );
  const nodes = values
    .map(
      (value, point) =>
        `(:Point {name:'p${String(point).padStart(2, "0")}', features:[${value.toFixed(2)}]})`,
    )
    .join(",");
  forge.execute(`CREATE ${nodes}`);
  const cluster = (directed, writeProperty) =>
    tableFromIPC(
      forge.cluster(
        "Point",
        "k_means",
        undefined,
        directed,
        writeProperty,
        "features",
      ),
    );
  const table = cluster(true, undefined);
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "features", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "k_means");
  assert.equal(table.getChild("node_id"), null);
  const uuids = [...table.getChild("node_uuid").toArray()].map(String);
  assert.equal(new Set(uuids).size, 20);
  assert.deepEqual(
    [...table.getChild("name").toArray()],
    Array.from(
      { length: 20 },
      (_, point) => `p${String(point).padStart(2, "0")}`,
    ),
  );
  assert.deepEqual(
    table
      .getChild("features")
      .toArray()
      .map((vector) => [...vector]),
    values.map((value) => [value]),
  );
  const expected = Array.from({ length: 20 }, (_, point) =>
    BigInt(Math.floor(point / 2)),
  );
  assert.deepEqual([...table.getChild("community_id").toArray()], expected);
  const undirected = cluster(false, undefined);
  assert.deepEqual(
    [...undirected.getChild("node_uuid").toArray()].map(String),
    uuids,
  );
  assert.deepEqual(
    [...undirected.getChild("community_id").toArray()],
    expected,
  );
  assert.deepEqual(
    [...undirected.getChild("name").toArray()],
    [...table.getChild("name").toArray()],
  );
  assert.deepEqual(
    undirected
      .getChild("features")
      .toArray()
      .map((vector) => [...vector]),
    table
      .getChild("features")
      .toArray()
      .map((vector) => [...vector]),
  );
  assert.equal(
    tableFromIPC(
      forge.execute("MATCH (p:Point) WHERE p.community IS NOT NULL RETURN p"),
    ).numRows,
    0,
  );

  forge.execute("MATCH (p:Point {name:'p00'}) SET p.atomic_group = 'old'");
  assert.throws(
    () => cluster(false, "atomic_group"),
    (error) => error.code === "ValidationError",
  );
  const unchanged = tableFromIPC(
    forge.execute(
      "MATCH (p:Point) WHERE p.atomic_group IS NOT NULL RETURN p.atomic_group AS value",
    ),
  );
  assert.deepEqual([...unchanged.getChild("value").toArray()], ["old"]);
  cluster(false, "community");
  const written = tableFromIPC(
    forge.execute(
      "MATCH (p:Point) RETURN p.community AS value ORDER BY p.name",
    ),
  );
  assert.deepEqual([...written.getChild("value").toArray()], expected);

  assert.throws(
    () => forge.cluster("Point", "k_means"),
    (error) => error.code === "ValidationError",
  );
  assert.throws(
    () =>
      forge.cluster("Point", "k_means", "KNOWS", false, undefined, "features"),
    (error) => error.code === "ValidationError",
  );
  const small = new GraphForge();
  small.execute("CREATE (:Point {features:[0.0]}), (:Point {features:[1.0]})");
  assert.throws(
    () =>
      small.cluster(
        "Point",
        "k_means",
        undefined,
        false,
        undefined,
        "features",
      ),
    (error) => error.code === "ExecutionError",
  );
}

function checkApproximateMaxCutCluster() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), " +
      "(c:Person {name:'c'}), (d:Person {name:'d'}), " +
      "(e:Person {name:'e'}), (a)-[:KNOWS]->(b), " +
      "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(b), " +
      "(b)-[:KNOWS]->(c), (c)-[:KNOWS]->(d), " +
      "(d)-[:KNOWS]->(a), (a)-[:OTHER]->(e)",
  );
  const cluster = (directed, writeProperty) =>
    tableFromIPC(
      forge.cluster(
        "Person",
        "approximate_max_k_cut",
        "KNOWS",
        directed,
        writeProperty,
      ),
    );
  const table = cluster(true, undefined);
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(table.schema.fields[0].nullable, false);
  assert.equal(table.schema.fields[1].nullable, false);
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "approximate_max_k_cut",
  );
  assert.equal(table.getChild("node_id"), null);
  assert.deepEqual([...table.getChild("name").toArray()], [..."abcde"]);
  const expectedCommunities = [0n, 1n, 0n, 1n, 0n];
  assert.deepEqual(
    [...table.getChild("community_id").toArray()],
    expectedCommunities,
  );
  const uuids = [...table.getChild("node_uuid").toArray()].map(String);
  assert.equal(new Set(uuids).size, 5);
  const expected = tableFromIPC(
    forge.execute(
      "MATCH (p:Person) RETURN p.node_uuid AS node_uuid ORDER BY p.name",
    ),
  );
  assert.deepEqual(
    uuids,
    [...expected.getChild("node_uuid").toArray()].map(String),
  );
  const undirected = cluster(false, undefined);
  assert.deepEqual(
    [...undirected.getChild("community_id").toArray()],
    expectedCommunities,
  );
  assert.deepEqual(
    [...undirected.getChild("node_uuid").toArray()].map(String),
    uuids,
  );
  assert.deepEqual(
    [
      ...tableFromIPC(forge.cluster("Person", "approximate_max_k_cut"))
        .getChild("community_id")
        .toArray(),
    ],
    [0n, 1n, 0n, 1n, 1n],
  );
  assert.equal(
    tableFromIPC(
      forge.execute("MATCH (p:Person) WHERE p.cut IS NOT NULL RETURN p"),
    ).numRows,
    0,
  );

  forge.execute("MATCH (p:Person {name:'a'}) SET p.atomic_cut = 'old'");
  assert.throws(
    () => cluster(false, "atomic_cut"),
    (error) => error.code === "ValidationError",
  );
  const unchanged = tableFromIPC(
    forge.execute(
      "MATCH (p:Person) WHERE p.atomic_cut IS NOT NULL RETURN p.atomic_cut AS value",
    ),
  );
  assert.deepEqual([...unchanged.getChild("value").toArray()], ["old"]);
  cluster(false, "cut");
  const written = tableFromIPC(
    forge.execute("MATCH (p:Person) RETURN p.cut AS value ORDER BY p.name"),
  );
  assert.deepEqual(
    [...written.getChild("value").toArray()],
    expectedCommunities,
  );

  assert.throws(
    () =>
      forge.cluster(
        "Person",
        "approximate_max_k_cut",
        undefined,
        false,
        undefined,
        "features",
      ),
    (error) => error.code === "ValidationError",
  );
  const oversized = new GraphForge();
  oversized.execute(
    `CREATE ${Array.from({ length: 4097 }, () => "(:Oversized)").join(",")}`,
  );
  assert.throws(
    () => oversized.cluster("Oversized", "approximate_max_k_cut"),
    (error) =>
      error.code === "ExecutionError" &&
      error.message.includes(
        "algorithm node limit exceeded: observed 4097, limit 4096",
      ),
  );
}

test("hdbscan cluster", checkHdbscanCluster);
test("k means cluster", checkKMeansCluster);
test("approximate max cut cluster", checkApproximateMaxCutCluster);
