// Native triangle-count acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

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
    [["triangle_count", "Uint64", false]],
  );
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "triangle_count",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );
  assert.equal(table.getChild("triangle_count").nullCount, 0);
};

test("triangle count returns deterministic exact native scalar", () => {
  const forge = new GraphForge();
  for (const name of ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox", "Gus"]) {
    forge.addNode("Person", { name });
  }
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(e:Person {name:'Eve'}), (f:Person {name:'Fox'}) " +
      "CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), " +
      "(b)-[:ROAD]->(c), (c)-[:ROAD]->(a), (a)-[:ROAD]->(a), " +
      "(d)-[:ROAD]->(e), (e)-[:ROAD]->(f), (f)-[:ROAD]->(d), " +
      "(a)-[:OTHER]->(d)",
  );

  const run = () =>
    tableFromIPC(forge.analyze("triangle_count", "Person", "ROAD", false));
  const table = run();
  assertSchemaAndMetadata(table);
  assert.equal(table.numRows, 1);
  assert.deepEqual([...table.getChild("triangle_count").toArray()], [2n]);
  assert.deepEqual(
    [...run().getChild("triangle_count").toArray()],
    [...table.getChild("triangle_count").toArray()],
  );

  const results = [
    tableFromIPC(forge.analyze("triangle_count", "Missing", "ROAD", false)),
    tableFromIPC(
      new GraphForge().analyze("triangle_count", undefined, undefined, false),
    ),
    tableFromIPC(forge.analyze("triangle_count", "Person", "OTHER", false)),
  ];
  for (const result of results) {
    assertSchemaAndMetadata(result);
    assert.equal(result.numRows, 1);
    assert.deepEqual([...result.getChild("triangle_count").toArray()], [0n]);
    assert.deepEqual(result.schema.fields, table.schema.fields);
    assert.deepEqual(
      [...result.schema.metadata.entries()],
      [...table.schema.metadata.entries()],
    );
  }
});

test("triangle count preserves Rust registry validation", () => {
  const forge = new GraphForge();
  expectValidation("triangle_count requires directed=false", () =>
    forge.analyze("triangle_count"),
  );
  expectValidation('invalid analyze relationship selector " "', () =>
    forge.analyze("triangle_count", undefined, " ", false),
  );
  expectValidation('invalid analyze label ""', () =>
    forge.analyze("triangle_count", "", undefined, false),
  );
  expectValidation(
    "triangle_count does not accept an edge weight property",
    () => forge.analyze("triangle_count", undefined, undefined, false, "cost"),
  );
});
