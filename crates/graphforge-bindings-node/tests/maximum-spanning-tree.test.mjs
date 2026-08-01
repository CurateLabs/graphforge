// Native maximum-spanning-tree acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
import { uuidHex } from "../lib/helpers.mjs";

const handleHex = (handle) => handle.uuid.replaceAll("-", "");

const expectValidation = (message, call) => {
  assert.throws(call, (error) => {
    assert.equal(error.code, "ValidationError");
    assert.equal(error.message, message);
    return true;
  });
};

test("maximum spanning tree returns deterministic UUID-only Arrow rows", () => {
  const forge = new GraphForge();
  const nodes = Object.fromEntries(
    ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox", "Gus"].map((name) => [
      name,
      forge.addNode("Person", { name }),
    ]),
  );
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(e:Person {name:'Eve'}), (f:Person {name:'Fox'}) " +
      "CREATE (a)-[:ROAD {cost:4.0}]->(b), " +
      "(a)-[:ROAD {cost:9.0}]->(b), (b)-[:ROAD {cost:8.0}]->(a), " +
      "(a)-[:ROAD {cost:7.0}]->(c), (b)-[:ROAD {cost:6.0}]->(c), " +
      "(b)-[:ROAD {cost:-3.0}]->(d), (c)-[:ROAD {cost:-1.0}]->(d), " +
      "(e)-[:ROAD {cost:-5.0}]->(f), (e)-[:ROAD {cost:-2.0}]->(f), " +
      "(d)-[:ROAD {cost:1e308}]->(d), " +
      "(a)-[:OTHER {cost:100.0}]->(d)",
  );

  const run = () =>
    tableFromIPC(
      forge.analyze("maximum_spanning_tree", "Person", "ROAD", false, "cost"),
    );
  const table = run();
  assert.deepEqual(
    table.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [
      ["edge_uuid", "FixedSizeBinary[16]", false],
      ["source_uuid", "FixedSizeBinary[16]", false],
      ["target_uuid", "FixedSizeBinary[16]", false],
      ["weight", "Float64", true],
    ],
  );
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "maximum_spanning_tree",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );
  assert.equal(table.getChild("edge_id"), null);
  assert.equal(table.getChild("edge_uuid").nullCount, 0);
  assert.equal(table.getChild("source_uuid").nullCount, 0);
  assert.equal(table.getChild("target_uuid").nullCount, 0);
  assert.deepEqual([...table.getChild("weight").toArray()], [9, 7, -1, -2]);
  assert.equal(table.getChild("weight").nullCount, 0);

  const expectedPairs = [
    ["Alice", "Bob"],
    ["Alice", "Carol"],
    ["Carol", "Dan"],
    ["Eve", "Fox"],
  ].map(([left, right]) =>
    [handleHex(nodes[left]), handleHex(nodes[right])].sort(),
  );
  const actualPairs = Array.from({ length: table.numRows }, (_, row) => [
    uuidHex(table.getChild("source_uuid").get(row)),
    uuidHex(table.getChild("target_uuid").get(row)),
  ]);
  assert.deepEqual(actualPairs, expectedPairs);
  assert.ok(actualPairs.every(([source, target]) => source < target));
  assert.ok(!actualPairs.flat().includes(handleHex(nodes.Gus)));
  assert.deepEqual(
    Array.from(run().getChild("edge_uuid"), uuidHex),
    Array.from(table.getChild("edge_uuid"), uuidHex),
  );
  const missing = tableFromIPC(
    forge.analyze("maximum_spanning_tree", "Missing", "ROAD", false, "cost"),
  );
  const empty = tableFromIPC(
    new GraphForge().analyze(
      "maximum_spanning_tree",
      undefined,
      undefined,
      false,
    ),
  );
  for (const result of [missing, empty]) {
    assert.equal(result.numRows, 0);
    assert.deepEqual(result.schema.fields, table.schema.fields);
    assert.deepEqual(
      [...result.schema.metadata.entries()],
      [...table.schema.metadata.entries()],
    );
  }
});

test("maximum spanning tree preserves Rust validation", () => {
  const empty = new GraphForge();
  expectValidation("maximum_spanning_tree requires directed=false", () =>
    empty.analyze("maximum_spanning_tree"),
  );
  expectValidation('invalid analyze relationship selector " "', () =>
    empty.analyze("maximum_spanning_tree", undefined, " ", false),
  );
  expectValidation('invalid analyze weight property " "', () =>
    empty.analyze("maximum_spanning_tree", undefined, "ROAD", false, " "),
  );
});
