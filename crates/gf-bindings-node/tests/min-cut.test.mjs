import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const uuidHex = (value) => Buffer.from(value).toString("hex");
const handleHex = (handle) => handle.uuid.replaceAll("-", "");

function fixture() {
  const forge = new GraphForge();
  const nodes = Object.fromEntries(
    ["Source", "A", "B", "Sink", "Unreachable"].map((name) => [
      name,
      forge.addNode("Person", { name }),
    ]),
  );
  forge.execute(
    "MATCH (s:Person {name:'Source'}), (a:Person {name:'A'}), " +
      "(b:Person {name:'B'}), (t:Person {name:'Sink'}) " +
      "CREATE (s)-[:PIPE {capacity:3.0}]->(a), " +
      "(s)-[:PIPE {capacity:2.0}]->(b), " +
      "(a)-[:PIPE {capacity:1.0}]->(b), " +
      "(a)-[:PIPE {capacity:2.0}]->(t), " +
      "(b)-[:PIPE {capacity:4.0}]->(t), " +
      "(a)-[:PIPE {capacity:7.0}]->(a), " +
      "(b)-[:PIPE {capacity:0.0}]->(a), " +
      "(s)-[:OTHER {capacity:100.0}]->(t)",
  );
  return { forge, nodes };
}

function minimumCut(
  forge,
  source,
  target,
  by,
  directed = true,
  weight = "capacity",
) {
  return tableFromIPC(
    forge.paths(
      source,
      target,
      by,
      "PIPE",
      directed,
      1,
      weight,
      undefined,
      undefined,
      undefined,
    ),
  );
}

test("minimum-cut views expose one deterministic native solution", () => {
  const { forge, nodes } = fixture();
  const scalar = minimumCut(forge, nodes.Source, nodes.Sink, "min_cut");
  const edges = minimumCut(forge, nodes.Source, nodes.Sink, "min_cut_edges");
  const schemas = {
    min_cut: [
      ["source_uuid", "FixedSizeBinary[16]", false],
      ["sink_uuid", "FixedSizeBinary[16]", false],
      ["cut_value", "Float64", false],
    ],
    min_cut_edges: [
      ["edge_uuid", "FixedSizeBinary[16]", false],
      ["source_uuid", "FixedSizeBinary[16]", false],
      ["target_uuid", "FixedSizeBinary[16]", false],
      ["capacity", "Float64", false],
    ],
  };
  for (const [table, algorithm] of [
    [scalar, "min_cut"],
    [edges, "min_cut_edges"],
  ]) {
    assert.deepEqual(
      table.schema.fields.map((field) => [
        field.name,
        String(field.type),
        field.nullable,
      ]),
      schemas[algorithm],
    );
    assert.deepEqual(
      [...table.schema.metadata.entries()],
      [
        ["graphforge.algorithm", algorithm],
        ["graphforge.algorithm_schema_version", "1"],
        ["graphforge.verb", "paths"],
      ],
    );
    assert.ok(table.schema.fields.every((field) => field.nullable === false));
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
      scalar.getChild("cut_value").get(0),
    ],
    [handleHex(nodes.Source), handleHex(nodes.Sink), 5],
  );
  const rows = Array.from({ length: edges.numRows }, (_, row) => ({
    edge: uuidHex(edges.getChild("edge_uuid").get(row)),
    source: uuidHex(edges.getChild("source_uuid").get(row)),
    target: uuidHex(edges.getChild("target_uuid").get(row)),
    capacity: edges.getChild("capacity").get(row),
  }));
  assert.deepEqual(
    rows.map(({ edge }) => edge),
    rows.map(({ edge }) => edge).sort(),
  );
  assert.deepEqual(
    new Set(
      rows.map(
        ({ source, target, capacity }) => `${source}->${target}:${capacity}`,
      ),
    ),
    new Set([
      `${handleHex(nodes.Source)}->${handleHex(nodes.A)}:3`,
      `${handleHex(nodes.Source)}->${handleHex(nodes.B)}:2`,
    ]),
  );
  assert.equal(
    rows.reduce((sum, row) => sum + row.capacity, 0),
    5,
  );
  assert.deepEqual(
    minimumCut(forge, nodes.Source, nodes.Sink, "min_cut_edges").toArray(),
    edges.toArray(),
  );
  assert.equal(
    minimumCut(forge, nodes.Source, nodes.Sink, "min_cut", true, null)
      .getChild("cut_value")
      .get(0),
    2,
  );
  assert.equal(
    minimumCut(forge, nodes.Source, nodes.Unreachable, "min_cut")
      .getChild("cut_value")
      .get(0),
    0,
  );
  assert.equal(
    minimumCut(forge, nodes.Source, nodes.Unreachable, "min_cut_edges").numRows,
    0,
  );
});

test("minimum cut preserves undirected orientation and structured errors", () => {
  const forge = new GraphForge();
  const left = forge.addNode("Person", { name: "Left" });
  forge.addNode("Person", { name: "Middle" });
  const right = forge.addNode("Person", { name: "Right" });
  forge.execute(
    "MATCH (l:Person {name:'Left'}), (m:Person {name:'Middle'}), " +
      "(r:Person {name:'Right'}) " +
      "CREATE (l)-[:PIPE {capacity:2.0}]->(m), " +
      "(m)-[:PIPE {capacity:2.0}]->(r)",
  );
  const reverse = minimumCut(forge, right, left, "min_cut_edges", false);
  assert.equal(
    uuidHex(reverse.getChild("source_uuid").get(0)),
    handleHex(left),
  );

  assert.throws(
    () => minimumCut(forge, left, undefined, "min_cut"),
    (error) => error.code === "ValidationError",
  );
  assert.throws(
    () => minimumCut(forge, left, left, "min_cut_edges"),
    (error) => error.code === "ExecutionError",
  );
});
