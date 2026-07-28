// Native maximum-cardinality-matching acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const uuidHex = (value) => Buffer.from(value).toString("hex");

const analyze = (forge, directed = false, weight) =>
  tableFromIPC(
    forge.analyze("max_cardinality_matching", "Node", "PAIR", directed, weight),
  );

const rows = (table) =>
  Array.from({ length: table.numRows }, (_, row) => ({
    edge: uuidHex(table.getChild("edge_uuid").get(row)),
    source: uuidHex(table.getChild("source_uuid").get(row)),
    target: uuidHex(table.getChild("target_uuid").get(row)),
  }));

const fixture = () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE " +
      "(a:Node), (b:Node), (c:Node), (d:Node), " +
      "(e:Node), (f:Node), (g:Node), (h:Node), " +
      "(a)-[:PAIR {tag:'ab0'}]->(b), " +
      "(a)-[:PAIR {tag:'ab1'}]->(b), " +
      "(b)-[:PAIR {tag:'bc'}]->(c), " +
      "(c)-[:PAIR {tag:'ca'}]->(a), " +
      "(b)-[:PAIR {tag:'bd'}]->(d), " +
      "(c)-[:PAIR {tag:'ce'}]->(e), " +
      "(f)-[:PAIR {tag:'fg'}]->(g), " +
      "(h)-[:PAIR {tag:'loop'}]->(h)",
  );
  return forge;
};

const topology = (forge) => {
  const table = tableFromIPC(
    forge.execute(
      "MATCH (a)-[r:PAIR]->(b) " +
        "RETURN r.edge_uuid AS edge_uuid, " +
        "a.node_uuid AS source_uuid, b.node_uuid AS target_uuid",
    ),
  );
  return Array.from({ length: table.numRows }, (_, row) => {
    const endpoints = [
      uuidHex(table.getChild("source_uuid").get(row)),
      uuidHex(table.getChild("target_uuid").get(row)),
    ].sort();
    return {
      edge: uuidHex(table.getChild("edge_uuid").get(row)),
      source: endpoints[0],
      target: endpoints[1],
    };
  }).sort((left, right) => left.edge.localeCompare(right.edge));
};

const exactOracle = (edges) => {
  let best = [];
  for (let mask = 0; mask < 2 ** edges.length; mask += 1) {
    const used = new Set();
    const candidate = [];
    let valid = true;
    for (let index = 0; index < edges.length; index += 1) {
      if ((mask & (2 ** index)) === 0) continue;
      const edge = edges[index];
      if (
        edge.source === edge.target ||
        used.has(edge.source) ||
        used.has(edge.target)
      ) {
        valid = false;
        break;
      }
      used.add(edge.source);
      used.add(edge.target);
      candidate.push(edge);
    }
    if (
      valid &&
      (candidate.length > best.length ||
        (candidate.length === best.length &&
          candidate.map(({ edge }) => edge).join() <
            best.map(({ edge }) => edge).join()))
    ) {
      best = candidate;
    }
  }
  return best;
};

test("maximum-cardinality matching returns the canonical native optimum", () => {
  const forge = fixture();
  const table = analyze(forge);
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
    ],
  );
  assert.deepEqual(Object.fromEntries(table.schema.metadata), {
    "graphforge.algorithm": "max_cardinality_matching",
    "graphforge.verb": "analyze",
    "graphforge.algorithm_schema_version": "1",
  });
  assert.ok(
    table.schema.fields.every(
      (field) => table.getChild(field.name).nullCount === 0,
    ),
  );
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

  const expected = exactOracle(topology(forge));
  assert.equal(expected.length, 3);
  assert.deepEqual(rows(table), expected);
  assert.deepEqual(rows(analyze(forge)), expected);
  assert.ok(
    rows(table).every(
      (row, index, result) =>
        row.source < row.target &&
        (index === 0 || result[index - 1].edge < row.edge),
    ),
  );
});

test("maximum-cardinality matching returns a typed empty native table", () => {
  const forge = new GraphForge();
  forge.execute("CREATE (:Node), (:Node)");
  const table = analyze(forge);
  assert.equal(table.numRows, 0);
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["edge_uuid", "source_uuid", "target_uuid"],
  );
});

test("maximum-cardinality matching preserves structured native failures", () => {
  const forge = fixture();
  for (const [invoke, message] of [
    [() => analyze(forge, true), /requires directed=false/],
    [
      () => analyze(forge, false, "weight"),
      /does not accept an edge weight property/,
    ],
  ]) {
    assert.throws(invoke, (error) => {
      assert.equal(error.code, "ValidationError");
      assert.match(error.message, message);
      return true;
    });
  }
});
