// Native Euler-path acceptance against the freshly built addon.

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
    [["has_euler_path", "Bool", false]],
  );
  assert.deepEqual(
    table.schema.metadata,
    new Map([
      ["graphforge.algorithm", "has_euler_path"],
      ["graphforge.algorithm_schema_version", "1"],
      ["graphforge.verb", "analyze"],
    ]),
  );
  assert.equal(table.numRows, 1);
  assert.equal(table.getChild("has_euler_path").nullCount, 0);
};

const fixture = () => {
  const forge = new GraphForge();
  for (const name of ["Alice", "Bob", "Carol", "Dan", "Isolate"]) {
    forge.addNode("Person", { name });
  }
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}) " +
      "CREATE (a)-[:PATH]->(b), (b)-[:PATH]->(c), " +
      "(a)-[:STAR]->(b), (a)-[:STAR]->(c), (a)-[:STAR]->(d), " +
      "(a)-[:LOOP]->(a), " +
      "(a)-[:PARALLEL]->(b), (a)-[:PARALLEL]->(b), " +
      "(a)-[:RECIPROCAL]->(b), (b)-[:RECIPROCAL]->(a), " +
      "(a)-[:DISCONNECTED]->(b), (c)-[:DISCONNECTED]->(d)",
  );
  return forge;
};

const analyze = (forge, via, directed) =>
  tableFromIPC(forge.analyze("has_euler_path", "Person", via, directed));

const predicate = (table) => {
  assertSchemaAndMetadata(table);
  return table.getChild("has_euler_path").get(0);
};

test("has Euler path classifies directed and undirected public graphs", () => {
  // The binding supplies selectors and decodes Arrow; Rust owns the predicate (#772).
  const forge = fixture();
  assert.equal(predicate(analyze(forge, "PATH", false)), true);
  assert.equal(predicate(analyze(forge, "PATH", true)), true);
  assert.equal(predicate(analyze(forge, "STAR", false)), false);
  assert.equal(predicate(analyze(forge, "STAR", true)), false);
  assert.equal(predicate(analyze(forge, "DISCONNECTED", false)), false);
  assert.equal(predicate(analyze(forge, "DISCONNECTED", true)), false);
});

test("has Euler path ignores isolates and handles empty, edgeless, and loops", () => {
  const forge = fixture();
  assert.equal(predicate(analyze(forge, "LOOP", false)), true);
  assert.equal(predicate(analyze(forge, "LOOP", true)), true);

  const edgeless = analyze(forge, "MISSING", false);
  const empty = tableFromIPC(
    new GraphForge().analyze("has_euler_path", undefined, undefined, true),
  );
  assert.equal(predicate(edgeless), true);
  assert.equal(predicate(empty), true);
  assert.deepEqual(edgeless.schema.fields, empty.schema.fields);
  assert.deepEqual(
    [...edgeless.schema.metadata.entries()],
    [...empty.schema.metadata.entries()],
  );
});

test("has Euler path counts parallel and reciprocal edge UUIDs", () => {
  const forge = fixture();
  assert.equal(predicate(analyze(forge, "PARALLEL", false)), true);
  assert.equal(predicate(analyze(forge, "PARALLEL", true)), false);
  assert.equal(predicate(analyze(forge, "RECIPROCAL", false)), true);
  assert.equal(predicate(analyze(forge, "RECIPROCAL", true)), true);
});

test("has Euler path preserves Rust registry validation", () => {
  const forge = new GraphForge();
  expectValidation('invalid analyze relationship selector " "', () =>
    forge.analyze("has_euler_path", undefined, " ", false),
  );
  expectValidation('invalid analyze label ""', () =>
    forge.analyze("has_euler_path", "", undefined, false),
  );
  expectValidation(
    "has_euler_path does not accept an edge weight property",
    () => forge.analyze("has_euler_path", undefined, undefined, false, "cost"),
  );
});
