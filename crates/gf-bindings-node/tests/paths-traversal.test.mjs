// Native acceptance for this coherent algorithm family.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
import { pathHex, uuidHex } from "../lib/helpers.mjs";

function checkBfsPaths() {
  // #1352 — napi only coerces selectors/options and encodes native Arrow IPC.
  const forge = new GraphForge();
  const handles = Object.fromEntries(
    ["Alice", "Bob", "Carol", "Dan", "Eve"].map((name) => [
      name,
      forge.addNode("Person", { name }),
    ]),
  );
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), (e:Person {name:'Eve'}) " +
      "CREATE (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), " +
      "(a)-[:KNOWS]->(a), (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), " +
      "(d)-[:OTHER]->(e)",
  );
  const identities = tableFromIPC(
    forge.execute(
      "MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name",
    ),
  );
  const uuids = Array.from(identities.getChild("uuid"), uuidHex);

  const table = tableFromIPC(
    forge.paths(handles.Alice, undefined, "bfs", "KNOWS"),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["source_uuid", "target_uuid", "cost", "path"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[2].type), "Float64");
  assert.equal(
    String(table.schema.fields[3].type),
    "List<FixedSizeBinary[16]>",
  );
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "bfs");
  assert.ok(table.schema.fields.every((field) => !field.name.endsWith("_id")));
  assert.deepEqual(
    Array.from(table.getChild("source_uuid"), uuidHex),
    Array(4).fill(uuids[0]),
  );
  assert.deepEqual(
    Array.from(table.getChild("target_uuid"), uuidHex),
    uuids.slice(0, 4),
  );
  assert.deepEqual([...table.getChild("cost").toArray()], [0, 1, 1, 2]);
  assert.deepEqual(pathHex(table, 3), [uuids[0], uuids[1], uuids[3]]);

  const repeated = tableFromIPC(
    forge.paths(handles.Alice.uuid, undefined, "bfs", "KNOWS"),
  );
  assert.deepEqual(
    Array.from(repeated.getChild("target_uuid"), uuidHex),
    uuids.slice(0, 4),
  );
  const targeted = tableFromIPC(
    forge.paths(
      handles.Alice,
      { label: "Person", property: "name", value: "Dan" },
      "bfs",
      "KNOWS",
    ),
  );
  assert.equal(targeted.numRows, 1);
  assert.deepEqual(pathHex(targeted, 0), [uuids[0], uuids[1], uuids[3]]);
  const reverse = tableFromIPC(
    forge.paths(handles.Dan, handles.Alice, "bfs", "KNOWS", false),
  );
  assert.deepEqual(pathHex(reverse, 0), [uuids[3], uuids[1], uuids[0]]);
  assert.equal(
    tableFromIPC(forge.paths(handles.Dan, handles.Eve, "bfs", "OTHER")).numRows,
    1,
  );
  assert.equal(
    tableFromIPC(forge.paths(handles.Alice, handles.Eve, "bfs", "KNOWS"))
      .numRows,
    0,
  );

  const expectCode = (name, code, call) => {
    assert.throws(call, (error) => {
      assert.equal(error.code, code, `${name}: got code=${error.code}`);
      return true;
    });
  };
  expectCode("invalid k", "ValidationError", () =>
    forge.paths(handles.Alice, undefined, "bfs", undefined, undefined, 2),
  );
  expectCode("invalid weight", "ValidationError", () =>
    forge.paths(
      handles.Alice,
      undefined,
      "bfs",
      undefined,
      undefined,
      undefined,
      "distance",
    ),
  );
  expectCode("unavailable algorithm", "ValidationError", () =>
    forge.paths(handles.Alice, undefined, "astar"),
  );
  expectCode("missing UUID", "ValidationError", () =>
    forge.paths("01900000-0000-7000-8000-000000000000", handles.Bob, "bfs"),
  );
  expectCode("malformed selector", "ValidationError", () =>
    forge.paths({ label: "Person", property: "name" }, handles.Bob, "bfs"),
  );
  forge.addNode("Person", { name: "Alice" });
  expectCode("ambiguous selector", "ValidationError", () =>
    forge.paths(
      { label: "Person", property: "name", value: "Alice" },
      handles.Bob,
      "bfs",
    ),
  );

  const other = new GraphForge();
  const foreign = other.addNode("Person", { name: "Mallory" });
  expectCode("cross-graph handle", "ValidationError", () =>
    forge.paths(foreign, handles.Bob, "bfs"),
  );
}

test("bfs paths", checkBfsPaths);
