// Fresh-addon acceptance for the two source-free Steiner path algorithms.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const hex = (value) => Buffer.from(value).toString("hex");
const handleHex = (handle) => handle.uuid.replaceAll("-", "");
const rows = (table) =>
  Array.from({ length: table.numRows }, (_, row) => [
    hex(table.getChild("edge_uuid").get(row)),
    hex(table.getChild("source_uuid").get(row)),
    hex(table.getChild("target_uuid").get(row)),
    table.getChild("weight").get(row),
  ]);

function paths(forge, by, terminals, weight, prizeProperty) {
  return tableFromIPC(
    forge.paths(
      undefined,
      undefined,
      by,
      "ROAD",
      false,
      1,
      weight,
      undefined,
      undefined,
      undefined,
      terminals.map((terminal) => terminal.uuid ?? terminal),
      prizeProperty,
    ),
  );
}

function assertContract(table, algorithm) {
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
      ["weight", "Float64", false],
    ],
  );
  assert.deepEqual(
    [...table.schema.metadata.entries()],
    [
      ["graphforge.algorithm", algorithm],
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
    "belief_status",
    "valid_time",
  ]) {
    assert.equal(table.getChild(forbidden), null);
  }
}

function edgeCatalog(forge) {
  const table = tableFromIPC(
    forge.execute(
      "MATCH (s:Person)-[r:ROAD]->(t:Person) " +
        "RETURN r.edge_uuid AS edge_uuid, s.node_uuid AS source_uuid, " +
        "t.node_uuid AS target_uuid, r.tag AS tag",
    ),
  );
  return Array.from({ length: table.numRows }, (_, row) => ({
    edge: hex(table.getChild("edge_uuid").get(row)),
    source: hex(table.getChild("source_uuid").get(row)),
    target: hex(table.getChild("target_uuid").get(row)),
    tag: table.getChild("tag").get(row),
  }));
}

test("minimum Steiner tree has exact canonical weighted and unit results", () => {
  const forge = new GraphForge();
  const nodes = Object.fromEntries(
    ["A", "B", "Center", "Unused"].map((name) => [
      name,
      forge.addNode("Person", { name }),
    ]),
  );
  forge.execute(
    "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), " +
      "(c:Person {name:'Center'}), (u:Person {name:'Unused'}) " +
      "CREATE (a)-[:ROAD {cost:1.0, tag:'ac-first'}]->(c), " +
      "(b)-[:ROAD {cost:1.0, tag:'bc'}]->(c), " +
      "(a)-[:ROAD {cost:5.0, tag:'ab'}]->(b), " +
      "(a)-[:ROAD {cost:1.0, tag:'ac-second'}]->(c), " +
      "(c)-[:ROAD {cost:0.0, tag:'loop'}]->(c), " +
      "(u)-[:ROAD {cost:9.0, tag:'unused-loop'}]->(u), " +
      "(a)-[:OTHER {cost:0.0}]->(b)",
  );
  const result = paths(
    forge,
    "min_steiner_tree",
    [nodes.B, nodes.A, nodes.B],
    "cost",
  );
  assertContract(result, "min_steiner_tree");
  assert.deepEqual(
    rows(result),
    rows(paths(forge, "min_steiner_tree", [nodes.B, nodes.A, nodes.B], "cost")),
  );
  assert.deepEqual(
    result.getChild("weight").toArray(),
    new Float64Array([1, 1]),
  );
  assert.deepEqual(
    rows(result),
    rows(result).toSorted((left, right) => left[0].localeCompare(right[0])),
  );

  const catalog = edgeCatalog(forge);
  const expected = [
    [nodes.A, nodes.Center],
    [nodes.B, nodes.Center],
  ]
    .map((ends) => new Set(ends.map(handleHex)))
    .map(
      (pair) =>
        catalog
          .filter((row) => pair.has(row.source) && pair.has(row.target))
          .map((row) => row.edge)
          .sort()[0],
    )
    .sort();
  assert.deepEqual(
    rows(result).map((row) => row[0]),
    expected,
  );
  assert.deepEqual(
    new Set(rows(result).map((row) => [row[1], row[2]].sort().join(":"))),
    new Set([
      [handleHex(nodes.A), handleHex(nodes.Center)].sort().join(":"),
      [handleHex(nodes.B), handleHex(nodes.Center)].sort().join(":"),
    ]),
  );
  assert.deepEqual(
    [
      ...paths(forge, "min_steiner_tree", [nodes.A, nodes.B])
        .getChild("weight")
        .toArray(),
    ],
    [1],
  );
});

test("minimum Steiner tree preserves representative structured errors", () => {
  const forge = new GraphForge();
  const a = forge.addNode("Person", { name: "A" });
  const b = forge.addNode("Person", { name: "B" });
  const isolated = forge.addNode("Person", { name: "Isolated" });
  forge.execute(
    "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) CREATE (a)-[:ROAD {cost:1.0}]->(b)",
  );
  assert.throws(
    () =>
      forge.paths(
        undefined,
        undefined,
        "min_steiner_tree",
        "ROAD",
        true,
        1,
        "cost",
        undefined,
        undefined,
        undefined,
        [a.uuid, b.uuid],
      ),
    (error) =>
      error.code === "ExecutionError" &&
      error.message.includes("must be false"),
  );
  assert.throws(
    () => paths(forge, "min_steiner_tree", [a], "cost"),
    (error) =>
      error.code === "ExecutionError" &&
      error.message.includes("2 distinct terminals"),
  );
  assert.throws(
    () => paths(forge, "min_steiner_tree", [a, isolated], "cost"),
    (error) =>
      error.code === "ExecutionError" && error.message.includes("disconnected"),
  );
  assert.throws(
    () =>
      forge.paths(
        a,
        undefined,
        "min_steiner_tree",
        "ROAD",
        false,
        1,
        "cost",
        undefined,
        undefined,
        undefined,
        [a.uuid, b.uuid],
      ),
    (error) =>
      error.code === "ValidationError" && error.message.includes("positional"),
  );
});

test("prize-collecting Steiner uses explicit prizes and canonical ties", () => {
  const forge = new GraphForge();
  const terminal = forge.addNode("Person", {
    name: "Terminal",
    prize: 0,
    confidence: 1,
  });
  const winner = forge.addNode("Person", {
    name: "Winner",
    prize: 10,
    confidence: 0,
  });
  const excluded = forge.addNode("Person", {
    name: "Excluded",
    prize: 2,
    confidence: 1,
  });
  forge.execute(
    "MATCH (t:Person {name:'Terminal'}), (w:Person {name:'Winner'}), " +
      "(x:Person {name:'Excluded'}) " +
      "CREATE (t)-[:ROAD {cost:3.0, tag:'winner-first'}]->(w), " +
      "(t)-[:ROAD {cost:3.0, tag:'winner-second'}]->(w), " +
      "(t)-[:ROAD {cost:5.0, tag:'excluded'}]->(x), " +
      "(w)-[:ROAD {cost:0.0, tag:'loop'}]->(w), " +
      "(t)-[:OTHER {cost:0.0}]->(x)",
  );
  const result = paths(
    forge,
    "prize_collecting_steiner_tree",
    [terminal],
    "cost",
    "prize",
  );
  assertContract(result, "prize_collecting_steiner_tree");
  assert.deepEqual(
    rows(result),
    rows(
      paths(
        forge,
        "prize_collecting_steiner_tree",
        [terminal],
        "cost",
        "prize",
      ),
    ),
  );
  assert.equal(result.getChild("weight").get(0), 3);
  const pair = new Set([handleHex(terminal), handleHex(winner)]);
  const expected = edgeCatalog(forge)
    .filter((row) => pair.has(row.source) && pair.has(row.target))
    .map((row) => row.edge)
    .sort()[0];
  assert.equal(rows(result)[0][0], expected);
  assert.deepEqual(new Set(rows(result)[0].slice(1, 3)), pair);
  assert.ok(!rows(result)[0].slice(1, 3).includes(handleHex(excluded)));

  const unit = paths(
    forge,
    "prize_collecting_steiner_tree",
    [winner, terminal, winner],
    undefined,
    "prize",
  );
  assert.deepEqual([...unit.getChild("weight").toArray()], [1, 1]);
  assert.deepEqual(
    rows(unit),
    rows(unit).toSorted((left, right) => left[0].localeCompare(right[0])),
  );
});

test("prize-collecting Steiner rejects missing, null, and invalid prizes", () => {
  for (const [properties, code, text] of [
    [{}, "ValidationError", "missing"],
    [{ prize: null }, "ValidationError", "missing"],
    [{ prize: -1 }, "ExecutionError", "nonnegative"],
  ]) {
    const forge = new GraphForge();
    const node = forge.addNode("Person", { name: "Bad", ...properties });
    assert.throws(
      () =>
        paths(
          forge,
          "prize_collecting_steiner_tree",
          [node],
          undefined,
          "prize",
        ),
      (error) => error.code === code && error.message.includes(text),
    );
  }
  const forge = new GraphForge();
  const node = forge.addNode("Person", { prize: 0 });
  assert.throws(
    () => paths(forge, "prize_collecting_steiner_tree", [node]),
    (error) =>
      error.code === "ExecutionError" &&
      error.message.includes("prize_property"),
  );
  assert.throws(
    () =>
      forge.paths(
        undefined,
        undefined,
        "prize_collecting_steiner_tree",
        "ROAD",
        true,
        1,
        undefined,
        undefined,
        undefined,
        undefined,
        [node.uuid],
        "prize",
      ),
    (error) =>
      error.code === "ExecutionError" &&
      error.message.includes("must be false"),
  );
});
