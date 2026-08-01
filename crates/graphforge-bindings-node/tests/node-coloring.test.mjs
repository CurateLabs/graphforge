// Native node-coloring acceptance against the freshly built addon.

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

const assertSchemaAndMetadata = (table) => {
  assert.deepEqual(
    table.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [
      ["node_uuid", "FixedSizeBinary[16]", false],
      ["color", "Uint64", false],
    ],
  );
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "node_coloring",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );
  assert.equal(table.getChild("node_uuid").nullCount, 0);
  assert.equal(table.getChild("color").nullCount, 0);
  assert.equal(table.getChild("node_id"), null);
};

const fixture = () => {
  const forge = new GraphForge();
  const people = ["Alice", "Bob", "Carol", "Dan", "Eve"]
    .map((name) => [name, forge.addNode("Person", { name })])
    .sort((left, right) =>
      handleHex(left[1]).localeCompare(handleHex(right[1])),
    );
  forge.addNode("Animal", { name: "Fox" });
  forge.execute(
    `MATCH (a:Person {name:'${people[0][0]}'}), ` +
      `(b:Person {name:'${people[1][0]}'}), ` +
      `(c:Person {name:'${people[2][0]}'}), ` +
      `(d:Person {name:'${people[3][0]}'}), ` +
      `(e:Person {name:'${people[4][0]}'}), ` +
      "(f:Animal {name:'Fox'}) " +
      "CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(c), " +
      "(b)-[:ROAD]->(c), (c)-[:ROAD]->(d), " +
      "(a)-[:ROAD]->(b), (b)-[:ROAD]->(a), " +
      "(d)-[:OTHER]->(e), (f)-[:ROAD]->(a)",
  );
  return { forge, people };
};

test("node coloring returns deterministic UUID colors from native Rust", () => {
  const { forge, people } = fixture();
  const run = () =>
    tableFromIPC(forge.analyze("node_coloring", "Person", "ROAD", false));
  const table = run();

  assertSchemaAndMetadata(table);
  const expectedUuids = people.map(([, handle]) => handleHex(handle));
  assert.deepEqual(
    Array.from(table.getChild("node_uuid"), uuidHex),
    expectedUuids,
  );
  assert.deepEqual(
    [...table.getChild("color").toArray()],
    [0n, 1n, 2n, 0n, 0n],
  );
  assert.deepEqual(
    Array.from(run().getChild("node_uuid"), uuidHex),
    expectedUuids,
  );
  assert.deepEqual(
    [...run().getChild("color").toArray()],
    [...table.getChild("color").toArray()],
  );

  const colors = new Map(
    expectedUuids.map((node, index) => [
      node,
      table.getChild("color").get(index),
    ]),
  );
  for (const [left, right] of [
    [0, 1],
    [0, 2],
    [1, 2],
    [2, 3],
  ]) {
    assert.notEqual(
      colors.get(expectedUuids[left]),
      colors.get(expectedUuids[right]),
    );
  }
});

test("node coloring preserves projection, multigraph, and typed empty behavior", () => {
  const { forge } = fixture();
  const reference = tableFromIPC(
    forge.analyze("node_coloring", "Person", "ROAD", false),
  );
  const results = [
    tableFromIPC(forge.analyze("node_coloring", "Missing", "ROAD", false)),
    tableFromIPC(
      new GraphForge().analyze("node_coloring", undefined, undefined, false),
    ),
  ];
  for (const result of results) {
    assertSchemaAndMetadata(result);
    assert.equal(result.numRows, 0);
    assert.deepEqual(result.schema.fields, reference.schema.fields);
    assert.deepEqual(
      [...result.schema.metadata.entries()],
      [...reference.schema.metadata.entries()],
    );
  }
});

test("node coloring preserves Rust self-loop and option validation", () => {
  const loop = new GraphForge();
  loop.addNode("Person", { name: "Loop" });
  loop.execute("MATCH (n:Person) CREATE (n)-[:ROAD]->(n)");
  assert.throws(
    () => loop.analyze("node_coloring", "Person", "ROAD", false),
    (error) => {
      assert.equal(error.code, "ExecutionError");
      assert.equal(
        error.message,
        "Rust algorithm execution failed: node_coloring cannot color a graph " +
          "containing a self-loop",
      );
      return true;
    },
  );

  const forge = new GraphForge();
  expectValidation("node_coloring requires directed=false", () =>
    forge.analyze("node_coloring"),
  );
  expectValidation(
    "node_coloring does not accept an edge weight property",
    () => forge.analyze("node_coloring", undefined, undefined, false, "cost"),
  );
  expectValidation('invalid analyze relationship selector " "', () =>
    forge.analyze("node_coloring", undefined, " ", false),
  );
  expectValidation('invalid analyze label ""', () =>
    forge.analyze("node_coloring", "", undefined, false),
  );
});
