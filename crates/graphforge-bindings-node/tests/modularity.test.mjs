// Native modularity acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const fixture = () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE " +
      "(a:Person {side:'alpha', bucket:1}), " +
      "(b:Person {side:'alpha', bucket:1}), " +
      "(c:Person {side:'beta', bucket:2}), " +
      "(d:Person {side:'beta', bucket:2}), " +
      "(a)-[:LINK {weight:2}]->(b), " +
      "(a)-[:LINK {weight:1}]->(b), " +
      "(c)-[:LINK {weight:2}]->(d), " +
      "(b)-[:LINK {weight:1}]->(c), " +
      "(a)-[:LINK {weight:3}]->(a)",
  );
  return forge;
};

const analyze = (forge, weight, partitionProperty = "side") =>
  tableFromIPC(
    forge.analyze(
      "modularity",
      "Person",
      undefined,
      false,
      weight,
      partitionProperty,
    ),
  );

const score = (table) => table.getChild("modularity").get(0);

const expectValidation = (message, call) => {
  assert.throws(call, (error) => {
    assert.equal(error.code, "ValidationError");
    if (message instanceof RegExp) {
      assert.match(error.message, message);
    } else {
      assert.equal(error.message, message);
    }
    return true;
  });
};

test("modularity returns the exact deterministic native scalar", () => {
  const forge = fixture();
  const table = analyze(forge, "weight");
  assert.deepEqual(
    table.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [["modularity", "Float64", false]],
  );
  assert.deepEqual(Object.fromEntries(table.schema.metadata), {
    "graphforge.algorithm": "modularity",
    "graphforge.verb": "analyze",
    "graphforge.algorithm_schema_version": "1",
  });
  assert.equal(table.numRows, 1);
  assert.equal(table.getChild("modularity").nullCount, 0);
  const expected = 6 / 9 - (13 / 18) ** 2 + 2 / 9 - (5 / 18) ** 2;
  assert.equal(score(table), expected);
  assert.equal(score(analyze(forge, "weight")), expected);
  assert.equal(score(analyze(forge, "weight", "bucket")), expected);
});

test("modularity supports unit weights without knowledge-layer output", () => {
  const table = analyze(fixture(), undefined);
  const expected = 3 / 5 - (7 / 10) ** 2 + 1 / 5 - (3 / 10) ** 2;
  assert.equal(score(table), expected);
  for (const forbidden of [
    "confidence",
    "provenance_id",
    "evidence_uuid",
    "assertion_uuid",
    "belief_status",
    "hypothesis_uuid",
    "valid_time",
    "as_of",
  ]) {
    assert.equal(table.getChild(forbidden), null);
  }
});

test("modularity preserves structured native validation failures", () => {
  const forge = fixture();
  expectValidation("modularity requires directed=false", () =>
    forge.analyze("modularity", "Person", undefined, true, undefined, "side"),
  );

  const incomplete = new GraphForge();
  incomplete.execute(
    "CREATE " +
      "(a:Person {side:'alpha'}), " +
      "(b:Person), " +
      "(a)-[:LINK]->(b)",
  );
  expectValidation(/missing a partition value/, () =>
    incomplete.analyze(
      "modularity",
      "Person",
      undefined,
      false,
      undefined,
      "side",
    ),
  );
});
