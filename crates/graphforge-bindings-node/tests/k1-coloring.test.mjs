// Native K1-coloring acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
import { uuidHex } from "../lib/helpers.mjs";

const handleHex = (handle) => handle.uuid.replaceAll("-", "");

const fixture = () => {
  const forge = new GraphForge();
  const people = ["A", "B", "C", "D", "E", "F", "Isolate"]
    .map((name) => [name, forge.addNode("Person", { name })])
    .sort((left, right) =>
      handleHex(left[1]).localeCompare(handleHex(right[1])),
    );

  for (const left of [0, 2, 4]) {
    for (const right of [1, 3, 5]) {
      if (Math.floor(left / 2) === Math.floor(right / 2)) continue;
      forge.execute(
        `MATCH (a:Person {name:'${people[left][0]}'}), ` +
          `(b:Person {name:'${people[right][0]}'}) ` +
          "CREATE (a)-[:ROAD]->(b)",
      );
    }
  }
  forge.execute(
    `MATCH (a:Person {name:'${people[0][0]}'}), ` +
      `(b:Person {name:'${people[3][0]}'}) ` +
      "CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(a)",
  );
  return { forge, people };
};

const run = (forge) =>
  tableFromIPC(forge.analyze("k1_coloring", "Person", "ROAD", false));

test("K1 coloring returns deterministic UUID colors from native Rust", () => {
  const { forge, people } = fixture();
  const table = run(forge);

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
  assert.deepEqual(Object.fromEntries(table.schema.metadata), {
    "graphforge.algorithm": "k1_coloring",
    "graphforge.algorithm_schema_version": "1",
    "graphforge.verb": "analyze",
  });
  assert.equal(table.getChild("node_uuid").nullCount, 0);
  assert.equal(table.getChild("color").nullCount, 0);
  assert.equal(table.getChild("node_id"), null);

  const expectedUuids = people.map(([, handle]) => handleHex(handle));
  assert.deepEqual(
    Array.from(table.getChild("node_uuid"), uuidHex),
    expectedUuids,
  );
  assert.deepEqual(
    [...table.getChild("color").toArray()],
    [0n, 0n, 1n, 1n, 2n, 2n, 0n],
  );
  assert.deepEqual(
    Array.from(run(forge).getChild("node_uuid"), uuidHex),
    expectedUuids,
  );
  assert.deepEqual(
    [...run(forge).getChild("color").toArray()],
    [...table.getChild("color").toArray()],
  );

  const exact = tableFromIPC(
    forge.analyze("chromatic_number", "Person", "ROAD", false),
  );
  assert.deepEqual([...exact.getChild("chromatic_number").toArray()], [2n]);
});

test("K1 coloring preserves Rust self-loop and option validation", () => {
  const loop = new GraphForge();
  loop.addNode("Person", { name: "Loop" });
  loop.execute("MATCH (n:Person) CREATE (n)-[:ROAD]->(n)");
  assert.throws(
    () => loop.analyze("k1_coloring", "Person", "ROAD", false),
    (error) => {
      assert.equal(error.code, "ExecutionError");
      assert.equal(
        error.message,
        "Rust algorithm execution failed: k1_coloring cannot color a graph " +
          "containing a self-loop",
      );
      return true;
    },
  );

  const forge = new GraphForge();
  for (const [call, message] of [
    [() => forge.analyze("k1_coloring"), "k1_coloring requires directed=false"],
    [
      () => forge.analyze("k1_coloring", undefined, undefined, false, "cost"),
      "k1_coloring does not accept an edge weight property",
    ],
  ]) {
    assert.throws(call, (error) => {
      assert.equal(error.code, "ValidationError");
      assert.equal(error.message, message);
      return true;
    });
  }
});
