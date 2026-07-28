// Native maximum-weight-matching acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const uuidHex = (value) => Buffer.from(value).toString("hex");

const analyze = (forge, weight) =>
  tableFromIPC(
    forge.analyze("max_weight_matching", "Node", "PAIR", false, weight),
  );

const rows = (table) =>
  Array.from({ length: table.numRows }, (_, row) => ({
    edge: uuidHex(table.getChild("edge_uuid").get(row)),
    source: uuidHex(table.getChild("source_uuid").get(row)),
    target: uuidHex(table.getChild("target_uuid").get(row)),
    weight: table.getChild("weight").get(row),
  }));

const fixture = () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE " +
      "(a:Node), (b:Node), (c:Node), (d:Node), " +
      "(e:Node), (f:Node), (g:Node), " +
      "(a)-[:PAIR {tag:'ab0', weight:10}]->(b), " +
      "(a)-[:PAIR {tag:'ab1', weight:10}]->(b), " +
      "(b)-[:PAIR {tag:'bc', weight:7}]->(c), " +
      "(c)-[:PAIR {tag:'ca', weight:6}]->(a), " +
      "(d)-[:PAIR {tag:'de', weight:5}]->(e), " +
      "(f)-[:PAIR {tag:'fg', weight:-2}]->(g), " +
      "(a)-[:PAIR {tag:'loop', weight:100}]->(a)",
  );
  return forge;
};

test("maximum-weight matching exposes exact stable native rows", () => {
  const forge = fixture();
  const table = analyze(forge, "weight");
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
    "max_weight_matching",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );
  for (const field of table.schema.fields) {
    assert.equal(table.getChild(field.name).nullCount, 0);
  }

  const topology = tableFromIPC(
    forge.execute(
      "MATCH (a)-[r:PAIR]->(b) " +
        "RETURN r.tag AS tag, r.edge_uuid AS edge_uuid, " +
        "a.node_uuid AS source_uuid, b.node_uuid AS target_uuid",
    ),
  );
  const edges = new Map();
  for (let row = 0; row < topology.numRows; row += 1) {
    const endpoints = [
      uuidHex(topology.getChild("source_uuid").get(row)),
      uuidHex(topology.getChild("target_uuid").get(row)),
    ].sort();
    edges.set(topology.getChild("tag").get(row), {
      edge: uuidHex(topology.getChild("edge_uuid").get(row)),
      source: endpoints[0],
      target: endpoints[1],
    });
  }
  const parallel =
    edges.get("ab0").edge < edges.get("ab1").edge
      ? edges.get("ab0")
      : edges.get("ab1");
  const expected = [
    { ...parallel, weight: 10 },
    { ...edges.get("de"), weight: 5 },
  ].sort(
    (left, right) =>
      left.source.localeCompare(right.source) ||
      left.target.localeCompare(right.target) ||
      left.edge.localeCompare(right.edge),
  );
  assert.deepEqual(rows(table), expected);
  assert.deepEqual(rows(analyze(forge, "weight")), expected);

  const unit = analyze(forge, undefined);
  assert.equal(unit.numRows, 3);
  assert.deepEqual([...unit.getChild("weight").toArray()], [1, 1, 1]);
});

test("maximum-weight matching preserves structured native failures", () => {
  const forge = fixture();
  assert.throws(
    () =>
      forge.analyze("max_weight_matching", "Node", "PAIR", undefined, "weight"),
    (error) => {
      assert.equal(error.code, "ValidationError");
      assert.match(error.message, /requires directed=false/);
      return true;
    },
  );

  const invalid = new GraphForge();
  invalid.execute(
    "CREATE (a:Node), (b:Node), " + "(a)-[:PAIR {weight:1e308 * 2.0}]->(b)",
  );
  assert.throws(
    () => analyze(invalid, "weight"),
    (error) => {
      assert.equal(error.code, "ValidationError");
      assert.match(error.message, /missing, NULL, NaN, or infinite/);
      return true;
    },
  );
});
