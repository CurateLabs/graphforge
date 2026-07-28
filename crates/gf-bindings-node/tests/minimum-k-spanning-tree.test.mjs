// Native minimum-k-spanning-tree acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
import { uuidHex } from "../lib/helpers.mjs";

const analyze = (forge, k) =>
  tableFromIPC(
    forge.analyze(
      "minimum_k_spanning_tree",
      "Node",
      "LINK",
      false,
      "weight",
      undefined,
      k,
    ),
  );

test("minimum-k spanning tree forwards default and explicit k deterministically", () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Node), (b:Node), (c:Node), " +
      "(a)-[:LINK {weight:1}]->(b), " +
      "(a)-[:LINK {weight:1}]->(c), " +
      "(b)-[:LINK {weight:2}]->(c)",
  );

  const defaultResult = analyze(forge);
  assert.deepEqual(
    defaultResult.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [
      ["tree_id", "Uint64", false],
      ["edge_uuid", "FixedSizeBinary[16]", false],
      ["source_uuid", "FixedSizeBinary[16]", false],
      ["target_uuid", "FixedSizeBinary[16]", false],
      ["weight", "Float64", false],
    ],
  );
  assert.equal(
    defaultResult.schema.metadata.get("graphforge.algorithm"),
    "minimum_k_spanning_tree",
  );
  assert.equal(defaultResult.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(
    defaultResult.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );
  assert.deepEqual([...defaultResult.getChild("tree_id").toArray()], [0n, 0n]);
  for (const field of defaultResult.schema.fields) {
    assert.equal(defaultResult.getChild(field.name).nullCount, 0);
  }
  assert.equal(defaultResult.getChild("edge_id"), null);

  const explicit = analyze(forge, 2);
  assert.deepEqual(
    [...explicit.getChild("tree_id").toArray()],
    [0n, 0n, 1n, 1n],
  );
  assert.deepEqual([...explicit.getChild("weight").toArray()], [1, 1, 1, 2]);
  const rows = (table) =>
    Array.from({ length: table.numRows }, (_, row) => [
      table.getChild("tree_id").get(row),
      uuidHex(table.getChild("edge_uuid").get(row)),
      uuidHex(table.getChild("source_uuid").get(row)),
      uuidHex(table.getChild("target_uuid").get(row)),
      table.getChild("weight").get(row),
    ]);
  assert.deepEqual(rows(explicit), rows(analyze(forge, 2)));
  assert.deepEqual(rows(defaultResult), rows(explicit).slice(0, 2));
});

test("minimum-k spanning tree preserves structured k=0 rejection", () => {
  const forge = new GraphForge();
  assert.throws(
    () => analyze(forge, 0),
    (error) => {
      assert.equal(error.code, "ValidationError");
      assert.equal(
        error.message,
        "minimum_k_spanning_tree requires k greater than zero",
      );
      return true;
    },
  );
});
