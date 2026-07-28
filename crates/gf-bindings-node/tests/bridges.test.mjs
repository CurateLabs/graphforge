// Native Bridges acceptance against the freshly built addon.

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

test("bridges returns canonical UUID-only native Arrow rows", () => {
  const forge = new GraphForge();
  const nodes = Object.fromEntries(
    ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox", "Gus", "Hal"].map((name) => [
      name,
      forge.addNode("Person", { name }),
    ]),
  );
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(e:Person {name:'Eve'}), (f:Person {name:'Fox'}), " +
      "(g:Person {name:'Gus'}) " +
      "CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), " +
      "(b)-[:ROAD]->(d), (d)-[:ROAD]->(b), (d)-[:ROAD]->(e), " +
      "(d)-[:ROAD]->(d), (f)-[:ROAD]->(g), (a)-[:OTHER]->(e)",
  );

  const run = () =>
    tableFromIPC(forge.analyze("bridges", "Person", "ROAD", false));
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
    ],
  );
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "bridges");
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(table.getChild("edge_id"), null);
  assert.equal(table.getChild("edge_uuid").nullCount, 0);
  assert.equal(table.getChild("source_uuid").nullCount, 0);
  assert.equal(table.getChild("target_uuid").nullCount, 0);

  const rows = Array.from({ length: table.numRows }, (_, row) => [
    uuidHex(table.getChild("source_uuid").get(row)),
    uuidHex(table.getChild("target_uuid").get(row)),
    uuidHex(table.getChild("edge_uuid").get(row)),
  ]);
  const expectedPairs = new Set(
    [
      ["Dan", "Eve"],
      ["Fox", "Gus"],
    ].map(([source, target]) =>
      [handleHex(nodes[source]), handleHex(nodes[target])].sort().join(":"),
    ),
  );
  assert.equal(rows.length, expectedPairs.size);
  assert.deepEqual(
    new Set(rows.map(([source, target]) => `${source}:${target}`)),
    expectedPairs,
  );
  assert.deepEqual(
    rows,
    [...rows].sort((left, right) =>
      left.join(":").localeCompare(right.join(":")),
    ),
  );
  assert.deepEqual(
    Array.from(run().getChild("edge_uuid"), uuidHex),
    rows.map((row) => row[2]),
  );
});

test("bridges preserves empty output and structured Rust errors", () => {
  const empty = new GraphForge();
  const table = tableFromIPC(
    empty.analyze("bridges", undefined, undefined, false),
  );
  const missing = tableFromIPC(
    empty.analyze("bridges", "Missing", undefined, false),
  );
  assert.equal(table.numRows, 0);
  assert.deepEqual(table.schema.fields, missing.schema.fields);
  assert.deepEqual(
    [...table.schema.metadata.entries()],
    [...missing.schema.metadata.entries()],
  );
  expectValidation("bridges requires directed=false", () =>
    empty.analyze("bridges"),
  );
  expectValidation("bridges does not accept an edge weight property", () =>
    empty.analyze("bridges", undefined, undefined, false, "cost"),
  );
  expectValidation('invalid analyze relationship selector " "', () =>
    empty.analyze("bridges", undefined, " ", false),
  );
  expectValidation('invalid analyze label ""', () =>
    empty.analyze("bridges", "", undefined, false),
  );
});
