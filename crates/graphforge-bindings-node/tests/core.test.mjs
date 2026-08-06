// Native core acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { tableFromIPC } from "apache-arrow";
import { GraphForge, NodeHandle, version } from "../index.js";

function checkAddNode() {
  // #1302 — construction delegates to Rust and exposes UUID identity only.
  const forge = new GraphForge();
  const handle = forge.addNode("Person", { name: "Alice", score: 7 });
  assert.ok(handle instanceof NodeHandle);
  assert.equal(handle.label, "Person");
  assert.match(
    handle.uuid,
    /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
  );
  assert.equal("id" in handle, false, "internal node id must not escape");

  const table = tableFromIPC(
    forge.execute("MATCH (n:Person) RETURN n.name AS name, n.score AS score"),
  );
  assert.equal(table.numRows, 1);
  assert.deepEqual([...table.getChild("name").toArray()], ["Alice"]);
  assert.deepEqual([...table.getChild("score").toArray()], [7n]);

  assert.throws(
    () => forge.addNode("Person", { nested: { unsupported: true } }),
    (error) => error instanceof TypeError,
  );
  assert.equal(
    tableFromIPC(forge.execute("MATCH (n:Person) RETURN n")).numRows,
    1,
  );
}

function checkExplain() {
  const forge = new GraphForge();
  const plan = forge.explain("MATCH (n:Person) RETURN n.node_uuid AS id");
  assert.ok(plan.includes("NodeScan"), plan);
}

function checkLoadOntology() {
  // A valid native ontology loads and promotes the mode exploratory→advisory.
  const yaml = [
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
  const dir = mkdtempSync(join(tmpdir(), "gf-node-smoke-"));
  try {
    const path = join(dir, "ontology.yaml");
    writeFileSync(path, yaml);
    const forge = new GraphForge();
    forge.loadOntology(path); // no throw on a valid ontology
    assert.equal(forge.ontologyMode, "advisory", forge.ontologyMode);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

function checkParseError() {
  const forge = new GraphForge();
  try {
    forge.explain("MATCH (n) RETURN n WHERE");
  } catch (e) {
    assert.equal(e.code, "ParseError", `got code=${e.code}`);
    // ParseError encodes its span as a leading [span:<start>:<len>] token.
    assert.match(e.message, /^\[span:\d+:\d+\] /, e.message);
    return;
  }
  throw new Error("expected ParseError for invalid Cypher");
}

function checkPreV1ProjectError() {
  const dir = mkdtempSync(join(tmpdir(), "gf-node-pre-v1-"));
  try {
    const topology = join(dir, "topology");
    const legacy = join(topology, "nodes.parquet");
    mkdirSync(topology);
    writeFileSync(legacy, "legacy");
    assert.throws(
      () => new GraphForge(dir),
      (error) => error.code === "GF_UNSUPPORTED_PROJECT_FORMAT",
    );
    assert.equal(readFileSync(legacy, "utf8"), "legacy");
    assert.equal(existsSync(join(dir, "FORMAT")), false);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

function checkInspectionSurface() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person:Author), (b:Person), (p:Paper), " +
      "(a)-[:AUTHORED]->(p), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(a)",
  );

  assert.deepEqual(forge.labels(), ["Author", "Paper", "Person"]);
  assert.deepEqual(forge.relationshipTypes(), ["AUTHORED", "KNOWS"]);
  assert.equal(forge.nodeCount(), 3);
  assert.equal(forge.nodeCount("Person"), 2);
  assert.equal(forge.nodeCount("Missing"), 0);
  assert.equal(forge.nodeCount("Person') MATCH (n) RETURN n //"), 0);

  const schema = tableFromIPC(forge.schema());
  assert.deepEqual(
    schema.schema.fields.map((field) => [
      field.name,
      field.type.toString(),
      field.nullable,
    ]),
    [
      ["label", "Utf8", true],
      ["node_count", "Uint64", true],
      ["rel_type", "Utf8", true],
      ["rel_count", "Uint64", true],
    ],
  );
  assert.deepEqual(
    [...schema.getChild("label").toArray()],
    ["Author", "Paper", "Person", null, null],
  );
  const nodeCounts = schema.getChild("node_count");
  assert.deepEqual(
    Array.from({ length: schema.numRows }, (_, row) => nodeCounts.get(row)),
    [1n, 1n, 2n, null, null],
  );
  assert.deepEqual(
    [...schema.getChild("rel_type").toArray()],
    [null, null, null, "AUTHORED", "KNOWS"],
  );
  const relationshipCounts = schema.getChild("rel_count");
  assert.deepEqual(
    Array.from({ length: schema.numRows }, (_, row) =>
      relationshipCounts.get(row),
    ),
    [null, null, null, 1n, 2n],
  );

  forge.close();
  for (const call of [
    () => forge.labels(),
    () => forge.relationshipTypes(),
    () => forge.nodeCount(),
    () => forge.schema(),
  ]) {
    assert.throws(call, (error) => error.code === "LifecycleError");
  }
}

function checkRemovedSurfaceIsAbsent() {
  for (const name of ["begin", "commit", "rollback", "addNodes", "addEdges"]) {
    assert.equal(name in GraphForge.prototype, false, `${name} must not ship`);
  }
  const declarations = readFileSync(
    new URL("../index.d.ts", import.meta.url),
    "utf8",
  );
  assert.doesNotMatch(
    declarations,
    /^\s+(?:begin|commit|rollback|addNodes|addEdges)\([^)]*\):/m,
  );
}

function checkFind() {
  // #2308 — find delegates to Rust and returns canonical Arrow IPC.
  const forge = new GraphForge();
  forge.execute("CREATE (:Person {name: 'Alice'}), (:Person {name: 'Bob'})");
  const table = tableFromIPC(forge.find("alice", "Person"));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "name", "score", "matched_on"],
  );
  assert.equal(table.numRows, 1);
  assert.deepEqual([...table.getChild("name").toArray()], ["Alice"]);
  assert.deepEqual([...table.getChild("matched_on").toArray()], ["text"]);
}

async function checkPlanHandle() {
  // #593 — plan() returns a PlanHandle: sync explain + async collectIpc/sinkParquet.
  const forge = new GraphForge();
  forge.execute("CREATE (:Person {name: 'Alice'})");

  // explain() binds against the topology/ontology, so use a topology column
  // (node_uuid) — same as checkExplain; a runtime-only property isn't bindable.
  const plan = forge.plan("MATCH (p:Person) RETURN p.node_uuid AS id");
  const explained = plan.explain();
  assert.ok(explained.includes("NodeScan"), explained);

  // collectIpc resolves to an Arrow IPC Buffer (1 Person).
  const buf = await plan.collectIpc();
  assert.ok(Buffer.isBuffer(buf), "collectIpc should resolve to a Buffer");
  const table = tableFromIPC(buf);
  assert.equal(table.numRows, 1, `rows=${table.numRows}`);
  assert.ok(
    table.schema.fields.some((f) => f.name === "id"),
    "missing id column",
  );

  // params bind through the plan (the execute path resolves runtime properties).
  const filtered = tableFromIPC(
    await forge
      .plan("MATCH (p:Person) WHERE p.name = $n RETURN p.name AS name", {
        n: "Alice",
      })
      .collectIpc(),
  );
  assert.deepEqual([...filtered.getChild("name").toArray()], ["Alice"]);

  // sinkParquet writes a file, binding the plan's params through the write path.
  const dir = mkdtempSync(join(tmpdir(), "gf-node-parquet-"));
  try {
    const out = join(dir, "out.parquet");
    await forge
      .plan("MATCH (p:Person) WHERE p.name = $n RETURN p.name AS name", {
        n: "Alice",
      })
      .sinkParquet(out);
    assert.ok(existsSync(out), "sinkParquet should write a Parquet file");
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test("add node", checkAddNode);
test("explain", checkExplain);
test("load ontology", checkLoadOntology);
test("parse error", checkParseError);
test("pre-v1 project error", checkPreV1ProjectError);
test("find", checkFind);
test("graph inspection surface", checkInspectionSurface);
test(
  "generic transaction and bulk convenience stub surfaces are absent",
  checkRemovedSurfaceIsAbsent,
);
test("plan handle", checkPlanHandle);
