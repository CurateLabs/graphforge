// Fresh-addon acceptance for source-free Gomory-Hu forests.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const hex = (value) => Buffer.from(value).toString("hex");
const handleHex = (handle) => handle.uuid.replaceAll("-", "");
const rows = (table) =>
  Array.from({ length: table.numRows }, (_, row) => [
    hex(table.getChild("source_uuid").get(row)),
    hex(table.getChild("target_uuid").get(row)),
    table.getChild("cut_value").get(row),
  ]);

function paths(forge, overrides = {}) {
  const options = {
    source: undefined,
    target: undefined,
    by: "gomory_hu_tree",
    via: "PIPE",
    directed: false,
    k: 1,
    weight: "capacity",
    heuristic: undefined,
    walkLength: undefined,
    seed: undefined,
    terminalUuids: undefined,
    prizeProperty: undefined,
    capacityProperty: undefined,
    costProperty: undefined,
    ...overrides,
  };
  return tableFromIPC(
    forge.paths(
      options.source,
      options.target,
      options.by,
      options.via,
      options.directed,
      options.k,
      options.weight,
      options.heuristic,
      options.walkLength,
      options.seed,
      options.terminalUuids,
      options.prizeProperty,
      options.capacityProperty,
      options.costProperty,
    ),
  );
}

function assertContract(table) {
  assert.deepEqual(
    table.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [
      ["source_uuid", "FixedSizeBinary[16]", false],
      ["target_uuid", "FixedSizeBinary[16]", false],
      ["cut_value", "Float64", false],
    ],
  );
  assert.deepEqual(
    [...table.schema.metadata.entries()],
    [
      ["graphforge.algorithm", "gomory_hu_tree"],
      ["graphforge.algorithm_schema_version", "1"],
      ["graphforge.verb", "paths"],
    ],
  );
  for (const field of table.schema.fields)
    assert.equal(table.getChild(field.name).nullCount, 0);
  for (const forbidden of [
    "node_id",
    "edge_id",
    "provenance_id",
    "confidence",
    "assertion_uuid",
    "evidence_uuid",
    "belief_status",
    "hypothesis",
    "valid_time",
    "as_of",
    "run_uuid",
  ]) {
    assert.equal(table.getChild(forbidden), null);
  }
  assert.deepEqual(
    rows(table),
    rows(table).toSorted((left, right) =>
      left[0] === right[0]
        ? left[1].localeCompare(right[1])
        : left[0].localeCompare(right[0]),
    ),
  );
  assert.ok(
    rows(table).every(([source, target, cut]) => source < target && cut >= 0),
  );
}

test("Gomory-Hu exposes a canonical weighted capacity multigraph forest", () => {
  const forge = new GraphForge();
  const nodes = Object.fromEntries(
    ["A", "B", "C", "D", "Isolated"].map((name) => [
      name,
      forge.addNode("Person", { name }),
    ]),
  );
  forge.execute(
    "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), " +
      "(c:Person {name:'C'}), (d:Person {name:'D'}) " +
      "CREATE (a)-[:PIPE {capacity:3.0}]->(b), " +
      "(a)-[:PIPE {capacity:1.0}]->(b), " +
      "(b)-[:PIPE {capacity:2.0}]->(a), " +
      "(a)-[:PIPE {capacity:2.0}]->(c), " +
      "(c)-[:PIPE {capacity:1.0}]->(a), " +
      "(b)-[:PIPE {capacity:4.0}]->(c), " +
      "(c)-[:PIPE {capacity:5.0}]->(d), " +
      "(a)-[:PIPE {capacity:99.0}]->(a), " +
      "(a)-[:OTHER {capacity:99.0}]->(d)",
  );

  const weighted = paths(forge);
  assertContract(weighted);
  assert.deepEqual(rows(weighted), rows(paths(forge)));
  assert.equal(weighted.numRows, 3);
  assert.deepEqual(
    [...weighted.getChild("cut_value").toArray()].sort((a, b) => a - b),
    [5, 7, 9],
  );
  const endpoints = new Set(rows(weighted).flatMap((row) => row.slice(0, 2)));
  assert.deepEqual(
    endpoints,
    new Set(["A", "B", "C", "D"].map((name) => handleHex(nodes[name]))),
  );
  assert.equal(endpoints.has(handleHex(nodes.Isolated)), false);

  const unit = paths(forge, { weight: undefined });
  assertContract(unit);
  assert.equal(unit.numRows, 3);
  assert.deepEqual(
    [...unit.getChild("cut_value").toArray()].sort((a, b) => a - b),
    [1, 3, 4],
  );
});

test("Gomory-Hu handles empty and singleton graphs", () => {
  const empty = paths(new GraphForge());
  assertContract(empty);
  assert.equal(empty.numRows, 0);

  const singleton = new GraphForge();
  singleton.addNode("Person", { name: "Only" });
  const result = paths(singleton);
  assertContract(result);
  assert.equal(result.numRows, 0);
});

test("Gomory-Hu preserves structured errors and rejects forbidden options", () => {
  const singleton = new GraphForge();
  const only = singleton.addNode("Person", { name: "Only" });
  const invalid = new GraphForge();
  invalid.addNode("Person", { name: "Left" });
  invalid.addNode("Person", { name: "Right" });
  invalid.execute(
    "MATCH (l:Person {name:'Left'}), (r:Person {name:'Right'}) " +
      "CREATE (l)-[:PIPE {capacity:-1.0}]->(r)",
  );
  const missing = new GraphForge();
  missing.addNode("Person", { name: "Left" });
  missing.addNode("Person", { name: "Right" });
  missing.execute(
    "MATCH (l:Person {name:'Left'}), (r:Person {name:'Right'}) " +
      "CREATE (l)-[:PIPE {capacity:1.0}]->(r)",
  );

  const expect = (forge, overrides, code, text) =>
    assert.throws(
      () => paths(forge, overrides),
      (error) => error.code === code && error.message.includes(text),
    );
  expect(singleton, { source: only }, "ValidationError", "positional source");
  expect(singleton, { target: only }, "ValidationError", "positional source");
  expect(singleton, { directed: true }, "ValidationError", "directed=false");
  expect(invalid, {}, "ExecutionError", "finite nonnegative");
  for (const [option, value, text] of [
    ["k", 2, "k"],
    ["heuristic", "capacity", "heuristic"],
    ["capacityProperty", "capacity", "min-cost"],
    ["costProperty", "capacity", "min-cost"],
    ["walkLength", 2, "walk"],
    ["seed", 7n, "random-walk"],
    ["terminalUuids", [only.uuid], "terminal"],
    ["prizeProperty", "capacity", "prize"],
  ]) {
    expect(singleton, { [option]: value }, "ValidationError", text);
  }
  expect(missing, { weight: "missing" }, "ValidationError", "missing");
});
