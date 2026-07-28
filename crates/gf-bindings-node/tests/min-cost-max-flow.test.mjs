import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const uuidHex = (value) => Buffer.from(value).toString("hex");
const handleHex = (handle) => handle.uuid.replaceAll("-", "");

function fixture() {
  const forge = new GraphForge();
  const source = forge.addNode("Person", { name: "Source" });
  const middle = forge.addNode("Person", { name: "Middle" });
  const sink = forge.addNode("Person", { name: "Sink" });
  forge.execute(
    "MATCH (s:Person {name:'Source'}), (m:Person {name:'Middle'}), " +
      "(t:Person {name:'Sink'}) " +
      "CREATE (s)-[:PIPE {capacity:2.0, cost:-1.0}]->(m), " +
      "(m)-[:PIPE {capacity:2.0, cost:3.0}]->(t), " +
      "(s)-[:PIPE {capacity:1.0, cost:5.0}]->(t), " +
      "(m)-[:PIPE {capacity:9.0, cost:-8.0}]->(m), " +
      "(s)-[:OTHER {capacity:100.0, cost:-100.0}]->(t)",
  );
  return { forge, source, middle, sink };
}

function minCostFlow(
  forge,
  source,
  target,
  by,
  capacityProperty = "capacity",
  costProperty = "cost",
) {
  return tableFromIPC(
    forge.paths(
      source,
      target,
      by,
      "PIPE",
      true,
      1,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
      capacityProperty,
      costProperty,
    ),
  );
}

const schema = (table) =>
  table.schema.fields.map((field) => [
    field.name,
    String(field.type),
    field.nullable,
  ]);

test("min-cost maximum-flow scalar and edge views share one native solution", () => {
  const { forge, source, middle, sink } = fixture();
  const scalar = minCostFlow(forge, source, sink, "min_cost_max_flow");
  const edges = minCostFlow(forge, source, sink, "min_cost_max_flow_edges");

  assert.deepEqual(schema(scalar), [
    ["source_uuid", "FixedSizeBinary[16]", false],
    ["sink_uuid", "FixedSizeBinary[16]", false],
    ["flow", "Float64", false],
    ["cost", "Float64", false],
  ]);
  assert.deepEqual(schema(edges), [
    ["edge_uuid", "FixedSizeBinary[16]", false],
    ["source_uuid", "FixedSizeBinary[16]", false],
    ["target_uuid", "FixedSizeBinary[16]", false],
    ["flow", "Float64", false],
    ["unit_cost", "Float64", false],
    ["flow_cost", "Float64", false],
  ]);
  for (const [table, algorithm] of [
    [scalar, "min_cost_max_flow"],
    [edges, "min_cost_max_flow_edges"],
  ]) {
    assert.deepEqual(
      [...table.schema.metadata.entries()],
      [
        ["graphforge.algorithm", algorithm],
        ["graphforge.algorithm_schema_version", "1"],
        ["graphforge.verb", "paths"],
      ],
    );
    for (const forbidden of [
      "node_id",
      "edge_id",
      "provenance_id",
      "confidence",
      "assertion_uuid",
      "belief_status",
      "valid_time",
    ]) {
      assert.equal(table.getChild(forbidden), null);
    }
  }

  assert.deepEqual(
    [
      uuidHex(scalar.getChild("source_uuid").get(0)),
      uuidHex(scalar.getChild("sink_uuid").get(0)),
      scalar.getChild("flow").get(0),
      scalar.getChild("cost").get(0),
    ],
    [handleHex(source), handleHex(sink), 3, 9],
  );
  const rows = Array.from({ length: edges.numRows }, (_, row) => ({
    edge: uuidHex(edges.getChild("edge_uuid").get(row)),
    source: uuidHex(edges.getChild("source_uuid").get(row)),
    target: uuidHex(edges.getChild("target_uuid").get(row)),
    flow: edges.getChild("flow").get(row),
    unitCost: edges.getChild("unit_cost").get(row),
    flowCost: edges.getChild("flow_cost").get(row),
  }));
  assert.deepEqual(
    rows.map(({ edge }) => edge),
    rows.map(({ edge }) => edge).sort(),
  );
  assert.equal(new Set(rows.map(({ edge }) => edge)).size, 4);
  assert.equal(
    rows.reduce((sum, row) => sum + row.flowCost, 0),
    9,
  );
  assert.ok(rows.every((row) => row.flowCost === row.flow * row.unitCost));
  assert.equal(
    rows
      .filter((row) => row.source === handleHex(source))
      .reduce((sum, row) => sum + row.flow, 0),
    3,
  );
  assert.equal(
    rows
      .filter((row) => row.target === handleHex(sink))
      .reduce((sum, row) => sum + row.flow, 0),
    3,
  );
  assert.equal(
    rows
      .filter((row) => row.target === handleHex(middle))
      .reduce((sum, row) => sum + row.flow, 0),
    rows
      .filter((row) => row.source === handleHex(middle))
      .reduce((sum, row) => sum + row.flow, 0),
  );
});

test("omitted capacity uses unit capacity", () => {
  const { forge, source, sink } = fixture();
  const scalar = minCostFlow(
    forge,
    source.uuid,
    sink.uuid,
    "min_cost_max_flow",
    null,
  );
  assert.equal(scalar.getChild("flow").get(0), 2);
  assert.equal(scalar.getChild("cost").get(0), 7);
});

test("structured Rust validation errors cross the Node boundary", () => {
  const { forge, source, sink } = fixture();
  assert.throws(
    () =>
      minCostFlow(forge, source, sink, "min_cost_max_flow", "capacity", null),
    (error) =>
      error.code === "ValidationError" &&
      error.message.includes("cost_property"),
  );
  assert.throws(
    () => minCostFlow(forge, source, undefined, "min_cost_max_flow"),
    (error) => error.code === "ValidationError",
  );
  assert.throws(
    () => minCostFlow(forge, "not-a-uuid", sink, "min_cost_max_flow"),
    (error) => error.code === "ValidationError",
  );
  assert.throws(
    () => minCostFlow(forge, source, source, "min_cost_max_flow_edges"),
    (error) =>
      error.code === "ExecutionError" &&
      error.message.includes("distinct endpoints"),
  );
});
