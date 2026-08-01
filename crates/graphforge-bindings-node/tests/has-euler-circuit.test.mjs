// Native Euler-circuit acceptance against the freshly built addon.

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
    [["has_euler_circuit", "Bool", false]],
  );
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "has_euler_circuit",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );
  assert.equal(table.numRows, 1);
  assert.equal(table.getChild("has_euler_circuit").nullCount, 0);
};

const fixture = () => {
  const forge = new GraphForge();
  for (const name of ["Alice", "Bob", "Carol", "Isolate"]) {
    forge.addNode("Person", { name });
  }
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}) " +
      "CREATE (a)-[:CYCLE]->(b), (b)-[:CYCLE]->(c), (c)-[:CYCLE]->(a), " +
      "(a)-[:PATH]->(b), (b)-[:PATH]->(c), " +
      "(a)-[:LOOP]->(a), " +
      "(a)-[:PARALLEL]->(b), (a)-[:PARALLEL]->(b), " +
      "(a)-[:RECIPROCAL]->(b), (b)-[:RECIPROCAL]->(a)",
  );
  return forge;
};

const analyze = (forge, via, directed) =>
  tableFromIPC(forge.analyze("has_euler_circuit", "Person", via, directed));

const predicate = (table) => {
  assertSchemaAndMetadata(table);
  return table.getChild("has_euler_circuit").get(0);
};

test("has Euler circuit classifies directed and undirected public graphs", () => {
  const forge = fixture();
  assert.equal(predicate(analyze(forge, "CYCLE", false)), true);
  assert.equal(predicate(analyze(forge, "CYCLE", true)), true);
  assert.equal(predicate(analyze(forge, "PATH", false)), false);
  assert.equal(predicate(analyze(forge, "PATH", true)), false);
});

test("has Euler circuit ignores isolates and handles loops", () => {
  const forge = fixture();
  assert.equal(predicate(analyze(forge, "LOOP", false)), true);
  assert.equal(predicate(analyze(forge, "LOOP", true)), true);

  const missing = analyze(forge, "MISSING", false);
  const empty = tableFromIPC(
    new GraphForge().analyze("has_euler_circuit", undefined, undefined, true),
  );
  assert.equal(predicate(missing), true);
  assert.equal(predicate(empty), true);
  assert.deepEqual(missing.schema.fields, empty.schema.fields);
  assert.deepEqual(
    [...missing.schema.metadata.entries()],
    [...empty.schema.metadata.entries()],
  );
});

test("has Euler circuit counts parallel and reciprocal edge UUIDs", () => {
  const forge = fixture();
  assert.equal(predicate(analyze(forge, "PARALLEL", false)), true);
  assert.equal(predicate(analyze(forge, "PARALLEL", true)), false);
  assert.equal(predicate(analyze(forge, "RECIPROCAL", false)), true);
  assert.equal(predicate(analyze(forge, "RECIPROCAL", true)), true);
});

test("has Euler circuit preserves Rust registry validation", () => {
  const forge = new GraphForge();
  expectValidation('invalid analyze relationship selector " "', () =>
    forge.analyze("has_euler_circuit", undefined, " ", false),
  );
  expectValidation('invalid analyze label ""', () =>
    forge.analyze("has_euler_circuit", "", undefined, false),
  );
  expectValidation(
    "has_euler_circuit does not accept an edge weight property",
    () =>
      forge.analyze("has_euler_circuit", undefined, undefined, false, "cost"),
  );
});
