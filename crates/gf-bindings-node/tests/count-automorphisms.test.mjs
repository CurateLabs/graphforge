// Native automorphism-count acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const forbiddenColumns = new Set([
  "node_uuid",
  "provenance",
  "confidence",
  "assertion",
  "evidence",
  "belief",
  "hypothesis",
  "valid_time",
  "algorithm_run_uuid",
  "run_uuid",
]);

const fixture = () => {
  const forge = new GraphForge();
  for (const [index, name] of ["A", "B", "C", "D"].entries()) {
    forge.addNode("Person", {
      name,
      payload: `unique-property-${name}`,
      confidence: (index + 1) / 10,
    });
  }
  forge.execute(
    "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), " +
      "(c:Person {name:'C'}), (d:Person {name:'D'}) " +
      "CREATE (a)-[:ROAD]->(a), (b)-[:ROAD]->(b), " +
      "(a)-[:ROAD]->(b), (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), " +
      "(c)-[:ROAD]->(d), (d)-[:ROAD]->(c)",
  );
  return forge;
};

const run = (forge, { directed, label = "Person", via = "ROAD" } = {}) =>
  tableFromIPC(
    forge.analyze("count_automorphisms", label, via, directed ?? true),
  );

const assertCanonicalResult = (result, expected) => {
  assert.deepEqual(
    result.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [["count", "Uint64", false]],
  );
  assert.deepEqual(
    [...result.schema.metadata.entries()],
    [
      ["graphforge.algorithm", "count_automorphisms"],
      ["graphforge.algorithm_schema_version", "1"],
      ["graphforge.verb", "analyze"],
    ],
  );
  assert.equal(result.numRows, 1);
  assert.equal(result.getChild("count").nullCount, 0);
  assert.deepEqual([...result.getChild("count").toArray()], [expected]);
  assert.ok(
    result.schema.fields.every((field) => !forbiddenColumns.has(field.name)),
  );
};

test("count automorphisms preserves directed loop and parallel multiplicity", () => {
  const forge = fixture();
  const result = run(forge, { directed: true });

  assertCanonicalResult(result, 2n);
  assert.deepEqual(
    [...run(forge, { directed: true }).getChild("count").toArray()],
    [...result.getChild("count").toArray()],
  );
});

test("count automorphisms uses undirected multigraph semantics", () => {
  const forge = fixture();
  const result = run(forge, { directed: false });

  assertCanonicalResult(result, 4n);
  assert.deepEqual(
    [...run(forge, { directed: false }).getChild("count").toArray()],
    [...result.getChild("count").toArray()],
  );
});

test("count automorphisms returns one for empty and singleton graphs", () => {
  const empty = run(new GraphForge(), {
    directed: false,
    label: undefined,
    via: undefined,
  });
  const singleton = new GraphForge();
  singleton.addNode("Person", { name: "only", evidence: "ignored" });

  assertCanonicalResult(empty, 1n);
  assertCanonicalResult(
    run(singleton, { directed: false, via: undefined }),
    1n,
  );
});

test("count automorphisms rejects invalid relationship selectors", () => {
  const forge = fixture();
  assert.throws(
    () => run(forge, { directed: true, via: " " }),
    (error) => {
      assert.equal(error.code, "ValidationError");
      assert.equal(error.message, 'invalid analyze relationship selector " "');
      return true;
    },
  );
});
