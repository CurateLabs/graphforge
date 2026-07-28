// Native acceptance for ranked path algorithms.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
import { pathHex, uuidHex } from "../lib/helpers.mjs";

const handleHex = (handle) => handle.uuid.replaceAll("-", "");

const schemaShape = (schema) =>
  schema.fields.map((field) => ({
    name: field.name,
    type: String(field.type),
    nullable: field.nullable,
  }));

function expectError(code, message, call) {
  assert.throws(call, (error) => {
    assert.equal(error.code, code);
    assert.equal(error.message, message);
    return true;
  });
}

function checkYensPaths() {
  // #1715 — napi forwards selectors/options and only transports Rust Arrow IPC.
  const forge = new GraphForge();
  const handles = Object.fromEntries(
    ["Alice", "Bob", "Carol", "Dan", "Eve"].map((name) => [
      name,
      forge.addNode("Person", { name }),
    ]),
  );
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}) " +
      "CREATE (a)-[:ROAD {cost:4.0}]->(b), " +
      "(a)-[:ROAD {cost:1.0}]->(b), (b)-[:ROAD {cost:2.0}]->(d), " +
      "(a)-[:ROAD {cost:1.0}]->(c), (c)-[:ROAD {cost:2.0}]->(d), " +
      "(b)-[:ROAD {cost:0.5}]->(c), (a)-[:ROAD {cost:4.0}]->(d), " +
      "(a)-[:ROAD {cost:0.0}]->(a), (c)-[:ROAD {cost:0.0}]->(a), " +
      "(a)-[:UNIT]->(d), (a)-[:UNIT]->(b), (b)-[:UNIT]->(d)",
  );
  const uuids = Object.fromEntries(
    ["Alice", "Bob", "Carol", "Dan", "Eve"].map((name) => [
      name,
      handleHex(handles[name]),
    ]),
  );
  const run = () =>
    tableFromIPC(
      forge.paths(handles.Alice, handles.Dan, "yens", "ROAD", true, 10, "cost"),
    );
  const table = run();

  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["source_uuid", "target_uuid", "rank", "cost", "path"],
  );
  assert.ok(table.schema.fields.every((field) => !field.nullable));
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[2].type), "Uint64");
  assert.equal(String(table.schema.fields[3].type), "Float64");
  assert.equal(
    String(table.schema.fields[4].type),
    "List<FixedSizeBinary[16]>",
  );
  assert.equal(table.schema.fields[4].type.children[0].nullable, false);
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "yens");
  assert.equal(table.schema.metadata.get("graphforge.verb"), "paths");
  assert.ok(table.schema.fields.every((field) => !field.name.endsWith("_id")));
  assert.deepEqual([...table.getChild("rank").toArray()], [1n, 2n, 3n, 4n]);
  assert.deepEqual([...table.getChild("cost").toArray()], [3, 3, 3.5, 4]);
  assert.deepEqual(
    Array.from(table.getChild("source_uuid"), uuidHex),
    Array(4).fill(uuids.Alice),
  );
  assert.deepEqual(
    Array.from(table.getChild("target_uuid"), uuidHex),
    Array(4).fill(uuids.Dan),
  );
  assert.deepEqual(
    Array.from({ length: table.numRows }, (_, row) => pathHex(table, row)),
    [
      [uuids.Alice, uuids.Bob, uuids.Dan],
      [uuids.Alice, uuids.Carol, uuids.Dan],
      [uuids.Alice, uuids.Bob, uuids.Carol, uuids.Dan],
      [uuids.Alice, uuids.Dan],
    ],
  );
  const repeated = run();
  assert.deepEqual(
    Array.from({ length: repeated.numRows }, (_, row) =>
      pathHex(repeated, row),
    ),
    Array.from({ length: table.numRows }, (_, row) => pathHex(table, row)),
  );
  const uuidSelector = tableFromIPC(
    forge.paths(
      handles.Alice.uuid,
      { label: "Person", property: "name", value: "Dan" },
      "yens",
      "ROAD",
      true,
      10,
      "cost",
    ),
  );
  assert.deepEqual(schemaShape(uuidSelector.schema), schemaShape(table.schema));
  assert.deepEqual(uuidSelector.schema.metadata, table.schema.metadata);
  assert.deepEqual(
    Array.from({ length: uuidSelector.numRows }, (_, row) =>
      pathHex(uuidSelector, row),
    ),
    Array.from({ length: table.numRows }, (_, row) => pathHex(table, row)),
  );

  const unit = tableFromIPC(
    forge.paths(handles.Alice, handles.Dan, "yens", "UNIT", true, 2),
  );
  assert.deepEqual([...unit.getChild("cost").toArray()], [1, 2]);
  assert.equal(
    tableFromIPC(
      forge.paths(handles.Dan, handles.Alice, "yens", "ROAD", true, 2, "cost"),
    ).numRows,
    0,
  );
  assert.ok(
    tableFromIPC(
      forge.paths(handles.Dan, handles.Alice, "yens", "ROAD", false, 2, "cost"),
    ).numRows > 0,
  );
  assert.equal(
    tableFromIPC(
      forge.paths(handles.Alice, handles.Eve, "yens", "ROAD", true, 2, "cost"),
    ).numRows,
    0,
  );
  const singleton = tableFromIPC(
    forge.paths(handles.Alice, handles.Alice, "yens", "ROAD", true, 4, "cost"),
  );
  assert.deepEqual([...singleton.getChild("rank").toArray()], [1n]);
  assert.deepEqual([...singleton.getChild("cost").toArray()], [0]);
  assert.deepEqual(pathHex(singleton, 0), [uuids.Alice]);

  expectError("ValidationError", "yens requires a target selector", () =>
    forge.paths(handles.Alice, undefined, "yens", "ROAD", true, 2, "cost"),
  );
  expectError("ValidationError", "yens k must be at least 1", () =>
    forge.paths(handles.Alice, handles.Dan, "yens", "ROAD", true, 0, "cost"),
  );
  expectError(
    "ValidationError",
    "yens does not accept a heuristic property",
    () =>
      forge.paths(
        handles.Alice,
        handles.Dan,
        "yens",
        "ROAD",
        true,
        2,
        "cost",
        "estimate",
      ),
  );
  expectError(
    "ValidationError",
    'invalid paths relationship selector " "',
    () => forge.paths(handles.Alice, handles.Dan, "yens", " ", true, 2),
  );
  expectError("ValidationError", 'invalid paths weight property " "', () =>
    forge.paths(handles.Alice, handles.Dan, "yens", "ROAD", true, 2, " "),
  );
  expectError(
    "ValidationError",
    'edge weight property "missing" does not exist',
    () =>
      forge.paths(
        handles.Alice,
        handles.Dan,
        "yens",
        "ROAD",
        true,
        2,
        "missing",
      ),
  );

  for (const [literal, code, fixedMessage] of [
    ["null", "ValidationError", undefined],
    [
      "'heavy'",
      "ValidationError",
      'edge weight property "cost" must be numeric',
    ],
    ["1e308 * 2.0", "ValidationError", undefined],
    [
      "-1.0",
      "ExecutionError",
      "Rust algorithm execution failed: yens requires finite non-negative edge weights",
    ],
  ]) {
    const invalid = new GraphForge();
    const source = invalid.addNode("Person", { name: "source" });
    const target = invalid.addNode("Person", { name: "target" });
    invalid.execute(
      "MATCH (s:Person {name:'source'}), (t:Person {name:'target'}) " +
        `CREATE (s)-[:ROAD {cost:${literal}}]->(t)`,
    );
    let message = fixedMessage;
    if (message === undefined) {
      const edge = tableFromIPC(
        invalid.execute("MATCH ()-[r:ROAD]->() RETURN r.edge_uuid AS uuid"),
      );
      const hex = uuidHex(edge.getChild("uuid").get(0));
      const edgeUuid = hex.replace(
        /^(.{8})(.{4})(.{4})(.{4})(.{12})$/,
        "$1-$2-$3-$4-$5",
      );
      message =
        "edge weight is missing, NULL, NaN, or infinite for edge " + edgeUuid;
    }
    expectError(code, message, () =>
      invalid.paths(source, target, "yens", "ROAD", true, 2, "cost"),
    );
  }
}

test("yen ranked paths", checkYensPaths);
