// Native maximum-bipartite-matching acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const uuidHex = (value) => Buffer.from(value).toString("hex");

const analyze = (forge, partitionProperty) =>
  tableFromIPC(
    forge.analyze(
      "max_bipartite_matching",
      "Person",
      "BIPARTITE",
      false,
      undefined,
      partitionProperty,
    ),
  );

const expectError = (code, message, call) => {
  assert.throws(call, (error) => {
    assert.equal(error.code, code);
    assert.match(error.message, message);
    return true;
  });
};

const matchingRows = (table) =>
  Array.from({ length: table.numRows }, (_, row) => ({
    edge: uuidHex(table.getChild("edge_uuid").get(row)),
    source: uuidHex(table.getChild("source_uuid").get(row)),
    target: uuidHex(table.getChild("target_uuid").get(row)),
  }));

test("maximum bipartite matching returns stable explicit native rows", () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE " +
      "(l1:Person {name:'l1', side:'a'}), " +
      "(l2:Person {name:'l2', side:'a'}), " +
      "(l3:Person {name:'l3', side:'a'}), " +
      "(r1:Person {name:'r1', side:'z'}), " +
      "(r2:Person {name:'r2', side:'z'}), " +
      "(r3:Person {name:'r3', side:'z'}), " +
      "(isolate:Person {name:'isolate', side:'a'}), " +
      "(l1)-[:BIPARTITE]->(r1), " +
      "(l1)-[:BIPARTITE]->(r2), " +
      "(l1)-[:BIPARTITE]->(r2), " +
      "(l2)-[:BIPARTITE]->(r1), " +
      "(l3)-[:BIPARTITE]->(r2), " +
      "(l3)-[:BIPARTITE]->(r3)",
  );
  const table = analyze(forge, "side");

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
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "max_bipartite_matching",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );

  const rows = matchingRows(table);
  assert.equal(rows.length, 3);
  assert.deepEqual(
    rows,
    [...rows].sort(
      (left, right) =>
        left.source.localeCompare(right.source) ||
        left.target.localeCompare(right.target) ||
        left.edge.localeCompare(right.edge),
    ),
  );
  assert.deepEqual(matchingRows(analyze(forge, "side")), rows);

  const nodeTable = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) RETURN n.node_uuid AS uuid, n.name AS name",
    ),
  );
  const names = new Map(
    Array.from({ length: nodeTable.numRows }, (_, row) => [
      uuidHex(nodeTable.getChild("uuid").get(row)),
      nodeTable.getChild("name").get(row),
    ]),
  );
  const topology = tableFromIPC(
    forge.execute(
      "MATCH (a:Person)-[r:BIPARTITE]->(b:Person) " +
        "RETURN r.edge_uuid AS edge_uuid, a.node_uuid AS source_uuid, " +
        "b.node_uuid AS target_uuid",
    ),
  );
  const topologyRows = matchingRows(topology);
  assert.equal(new Set(rows.map((row) => row.source)).size, 3);
  assert.equal(new Set(rows.map((row) => row.target)).size, 3);
  for (const row of rows) {
    assert.match(names.get(row.source), /^l/);
    assert.match(names.get(row.target), /^r/);
    assert.notEqual(names.get(row.source), "isolate");
    assert.notEqual(names.get(row.target), "isolate");
    assert.equal(
      row.edge,
      topologyRows
        .filter(
          (candidate) =>
            candidate.source === row.source && candidate.target === row.target,
        )
        .map((candidate) => candidate.edge)
        .sort()[0],
    );
  }
});

test("maximum bipartite matching infers canonical disconnected sides", () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE " +
      "(a:Person {name:'a'}), (b:Person {name:'b'}), " +
      "(c:Person {name:'c'}), (d:Person {name:'d'}), " +
      "(isolate:Person {name:'isolate'}), " +
      "(b)-[:BIPARTITE]->(a), (d)-[:BIPARTITE]->(c)",
  );
  const rows = matchingRows(analyze(forge));
  assert.equal(rows.length, 2);
  assert.deepEqual(matchingRows(analyze(forge)), rows);
  for (const row of rows) {
    assert.ok(row.source < row.target);
  }
});

test("maximum bipartite matching preserves structured native failures", () => {
  const valid = new GraphForge();
  valid.execute(
    "CREATE (a:Person {side:'x'}), (b:Person {side:'y'}), " +
      "(a)-[:BIPARTITE]->(b)",
  );
  expectError("ValidationError", /requires directed=false/, () =>
    valid.analyze(
      "max_bipartite_matching",
      "Person",
      "BIPARTITE",
      undefined,
      undefined,
      "side",
    ),
  );
  expectError(
    "ValidationError",
    /does not accept an edge weight property/,
    () =>
      valid.analyze(
        "max_bipartite_matching",
        "Person",
        "BIPARTITE",
        false,
        "weight",
        "side",
      ),
  );

  for (const [query, partitionProperty, code, message] of [
    [
      "CREATE (a:Person {side:'x'}), (b:Person {side:'x'}), " +
        "(a)-[:BIPARTITE]->(b)",
      "side",
      "ExecutionError",
      /exactly two partitions/,
    ],
    [
      "CREATE (a:Person {side:'x'}), (b:Person), " + "(a)-[:BIPARTITE]->(b)",
      "side",
      "ValidationError",
      /missing a partition value/,
    ],
    [
      "CREATE (a:Person {side:'x'}), (b:Person {side:null}), " +
        "(a)-[:BIPARTITE]->(b)",
      "side",
      "ValidationError",
      /missing a partition value/,
    ],
    [
      "CREATE (a:Person), (b:Person), (c:Person), " +
        "(a)-[:BIPARTITE]->(b), (b)-[:BIPARTITE]->(c), " +
        "(c)-[:BIPARTITE]->(a)",
      undefined,
      "ExecutionError",
      /odd cycle/,
    ],
  ]) {
    const forge = new GraphForge();
    forge.execute(query);
    expectError(code, message, () => analyze(forge, partitionProperty));
  }
});
