// Arrow/IPC round-trip data-contract tests — Node side (#595).
//
// Mirrors tests/integration/test_arrow_roundtrip.py: execute() returns an Arrow
// IPC stream Buffer that apache-arrow decodes faithfully (types, values, nulls,
// schema metadata, multi-column order, large batch, zero-row schema). The v0.5
// contract is plain Arrow — no CypherValue wrappers, so the old `.value`
// accessor never appears. Run with `node --test`.

import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

import { tableFromIPC, Type } from "apache-arrow";

import { GraphForge } from "../index.js";

const decode = (buf) => tableFromIPC(buf);

const ONTOLOGY = [
  "ontology_id: people",
  'version: "2026.06"',
  "entity_types:",
  "  - name: Person",
  "properties:",
  "  - name: name",
  "    owner: Person",
  "    type: utf8",
  "",
].join("\n");

test("type fidelity: string/int/float/bool/null survive the round-trip", () => {
  const table = decode(
    new GraphForge().execute(
      "RETURN 'hi' AS s, 42 AS i, 3.14 AS f, true AS b, null AS n",
    ),
  );
  assert.equal(table.numRows, 1);
  assert.equal(table.getChild("s").type.typeId, Type.Utf8);
  assert.equal(table.getChild("b").type.typeId, Type.Bool);
  assert.equal(table.getChild("n").type.typeId, Type.Null);
  // Enforce the specific 64-bit widths (mirrors the Python is_int64/is_float64).
  // typeId for Int64/Float64 is the physical Type.Int/Type.Float, so assert the
  // precise type via toString.
  assert.equal(table.getChild("i").type.toString(), "Int64");
  assert.equal(table.getChild("f").type.toString(), "Float64");
  assert.equal(table.getChild("s").get(0), "hi");
  assert.equal(table.getChild("i").get(0), 42n); // Int64 decodes as BigInt
  assert.ok(Math.abs(table.getChild("f").get(0) - 3.14) < 1e-9);
  assert.equal(table.getChild("b").get(0), true);
  assert.equal(table.getChild("n").get(0), null);
});

test("schema metadata carries the graphforge.* keys", () => {
  const table = decode(new GraphForge().execute("RETURN 1 AS a"));
  for (const key of [
    "graphforge.query_id",
    "graphforge.ir_version",
    "graphforge.ontology_mode",
  ]) {
    assert.ok(table.schema.metadata.has(key), `missing ${key}`);
  }
});

test("ontology_version metadata present once an ontology is loaded", () => {
  const forge = new GraphForge();
  const dir = mkdtempSync(join(tmpdir(), "gf-node-rt-"));
  try {
    const path = join(dir, "ontology.yaml");
    writeFileSync(path, ONTOLOGY);
    forge.loadOntology(path);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
  const table = decode(forge.execute("RETURN 1 AS a"));
  assert.ok(table.schema.metadata.has("graphforge.ontology_version"));
});

test("multi-column results keep column order", () => {
  const table = decode(
    new GraphForge().execute("RETURN 1 AS a, 2 AS b, 3 AS c"),
  );
  assert.deepEqual(
    table.schema.fields.map((f) => f.name),
    ["a", "b", "c"],
  );
});

test("null value decodes as null", () => {
  const table = decode(new GraphForge().execute("RETURN null AS x"));
  assert.equal(table.numRows, 1);
  assert.equal(table.getChild("x").get(0), null);
});

test("large batch (10k rows) materialises correctly", () => {
  const literal =
    "[" + Array.from({ length: 10000 }, (_, i) => i + 1).join(",") + "]";
  const table = decode(
    new GraphForge().execute(`UNWIND ${literal} AS i RETURN i AS n`),
  );
  assert.equal(table.numRows, 10000);
  assert.equal(table.getChild("n").get(0), 1n);
  assert.equal(table.getChild("n").get(9999), 10000n);
});

test("zero-row result keeps a valid schema", () => {
  const table = decode(
    new GraphForge().execute("MATCH (n:Nope) RETURN n.node_uuid AS id"),
  );
  assert.equal(table.numRows, 0);
  assert.ok(table.schema.fields.some((f) => f.name === "id"));
  assert.ok(table.schema.metadata.has("graphforge.query_id"));
});
