import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const uuidHex = (value) => Buffer.from(value).toString("hex");
const handleHex = (handle) => handle.uuid.replaceAll("-", "");

function fixture() {
  const forge = new GraphForge();
  const source = forge.addNode("Person", { name: "Source" });
  const a = forge.addNode("Person", { name: "A" });
  const b = forge.addNode("Person", { name: "B" });
  const sink = forge.addNode("Person", { name: "Sink" });
  forge.execute(
    "MATCH (s:Person {name:'Source'}), (a:Person {name:'A'}), " +
      "(b:Person {name:'B'}), (t:Person {name:'Sink'}) " +
      "CREATE (s)-[:PIPE {capacity:3.0}]->(a), " +
      "(s)-[:PIPE {capacity:2.0}]->(b), " +
      "(a)-[:PIPE {capacity:1.0}]->(b), " +
      "(a)-[:PIPE {capacity:2.0}]->(t), " +
      "(b)-[:PIPE {capacity:3.0}]->(t), " +
      "(a)-[:PIPE {capacity:7.0}]->(a), " +
      "(b)-[:PIPE {capacity:0.0}]->(a), " +
      "(s)-[:OTHER {capacity:100.0}]->(t)",
  );
  return { forge, source, a, b, sink };
}

function maximumFlow(forge, source, target, by) {
  return tableFromIPC(
    forge.paths(
      source,
      target,
      by,
      "PIPE",
      true,
      1,
      "capacity",
      undefined,
      undefined,
      undefined,
    ),
  );
}

function edgeRows(table) {
  return Array.from({ length: table.numRows }, (_, row) => ({
    edge: uuidHex(table.getChild("edge_uuid").get(row)),
    source: uuidHex(table.getChild("source_uuid").get(row)),
    target: uuidHex(table.getChild("target_uuid").get(row)),
    flow: table.getChild("flow").get(row),
  }));
}

test("maximum-flow native views share one deterministic solution", () => {
  const { forge, source, a, b, sink } = fixture();
  const scalar = maximumFlow(forge, source, sink, "max_flow");
  const edges = maximumFlow(forge, source, sink, "max_flow_edges");

  assert.deepEqual(
    scalar.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [
      ["source_uuid", "FixedSizeBinary[16]", false],
      ["sink_uuid", "FixedSizeBinary[16]", false],
      ["flow", "Float64", false],
    ],
  );
  assert.deepEqual(
    edges.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [
      ["edge_uuid", "FixedSizeBinary[16]", false],
      ["source_uuid", "FixedSizeBinary[16]", false],
      ["target_uuid", "FixedSizeBinary[16]", false],
      ["flow", "Float64", false],
    ],
  );
  for (const [table, algorithm] of [
    [scalar, "max_flow"],
    [edges, "max_flow_edges"],
  ]) {
    assert.equal(table.schema.metadata.get("graphforge.algorithm"), algorithm);
    assert.equal(
      table.schema.metadata.get("graphforge.algorithm_schema_version"),
      "1",
    );
    assert.equal(table.schema.metadata.get("graphforge.verb"), "paths");
  }

  assert.equal(
    uuidHex(scalar.getChild("source_uuid").get(0)),
    handleHex(source),
  );
  assert.equal(uuidHex(scalar.getChild("sink_uuid").get(0)), handleHex(sink));
  assert.equal(scalar.getChild("flow").get(0), 5);
  const rows = edgeRows(edges);
  assert.deepEqual(
    rows.map(({ edge }) => edge),
    rows.map(({ edge }) => edge).sort(),
  );
  assert.equal(new Set(rows.map(({ edge }) => edge)).size, 7);
  assert.deepEqual(
    Object.fromEntries(
      rows.map(({ source, target, flow }) => [`${source}->${target}`, flow]),
    ),
    {
      [`${handleHex(source)}->${handleHex(a)}`]: 3,
      [`${handleHex(source)}->${handleHex(b)}`]: 2,
      [`${handleHex(a)}->${handleHex(b)}`]: 1,
      [`${handleHex(a)}->${handleHex(sink)}`]: 2,
      [`${handleHex(b)}->${handleHex(sink)}`]: 3,
      [`${handleHex(a)}->${handleHex(a)}`]: 0,
      [`${handleHex(b)}->${handleHex(a)}`]: 0,
    },
  );
  assert.deepEqual(
    edgeRows(maximumFlow(forge, source.uuid, sink.uuid, "max_flow_edges")),
    rows,
  );
  assert.equal(
    rows
      .filter((row) => row.source === handleHex(source))
      .reduce((sum, row) => sum + row.flow, 0),
    5,
  );
  assert.equal(
    rows
      .filter((row) => row.target === handleHex(sink))
      .reduce((sum, row) => sum + row.flow, 0),
    5,
  );
});

test("maximum-flow native views preserve equivalent structured Rust errors", () => {
  const { forge, source } = fixture();
  for (const by of ["max_flow", "max_flow_edges"]) {
    assert.throws(
      () => maximumFlow(forge, source, undefined, by),
      (error) => error.code === "ValidationError",
    );
    assert.throws(
      () => maximumFlow(forge, source, source, by),
      (error) => error.code === "ExecutionError",
    );
  }

  const invalid = new GraphForge();
  const invalidSource = invalid.addNode("Person", { name: "Source" });
  const invalidSink = invalid.addNode("Person", { name: "Sink" });
  invalid.execute(
    "MATCH (s:Person {name:'Source'}), (t:Person {name:'Sink'}) " +
      "CREATE (s)-[:PIPE {capacity:-1.0}]->(t)",
  );
  for (const by of ["max_flow", "max_flow_edges"]) {
    assert.throws(
      () => maximumFlow(invalid, invalidSource, invalidSink, by),
      (error) => error.code === "ExecutionError",
    );
  }
});
