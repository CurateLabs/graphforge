// Native DFS acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
import { uuidHex } from "../lib/helpers.mjs";

const fixture = () => {
  const forge = new GraphForge();
  const handles = Object.fromEntries(
    ["Alice", "Bob", "Carol", "Dan", "Eve", "Isolate"].map((name) => [
      name,
      forge.addNode("Person", { name }),
    ]),
  );
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), (e:Person {name:'Eve'}) " +
      "CREATE (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), " +
      "(a)-[:KNOWS]->(a), (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), " +
      "(a)-[:OTHER]->(e)",
  );
  const identities = tableFromIPC(
    forge.execute(
      "MATCH (p:Person) RETURN p.name AS name, p.node_uuid AS uuid ORDER BY p.name",
    ),
  );
  return {
    forge,
    handles,
    uuids: Object.fromEntries(
      Array.from({ length: identities.numRows }, (_, row) => [
        identities.getChild("name").get(row),
        uuidHex(identities.getChild("uuid").get(row)),
      ]),
    ),
  };
};

const dfs = (forge, source, via, directed) =>
  tableFromIPC(forge.paths(source, undefined, "dfs", via, directed));

const expectValidation = (message, call) => {
  assert.throws(call, (error) => {
    assert.equal(error.code, "ValidationError");
    assert.equal(error.message, message);
    return true;
  });
};

test("DFS returns the exact UUID preorder and Arrow contract", () => {
  const { forge, handles, uuids } = fixture();
  const table = dfs(forge, handles.Alice, "KNOWS");
  assert.deepEqual(
    table.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [
      ["node_uuid", "FixedSizeBinary[16]", false],
      ["depth", "Uint64", false],
      ["order", "Uint64", false],
    ],
  );
  assert.deepEqual(
    table.schema.metadata,
    new Map([
      ["graphforge.algorithm", "dfs"],
      ["graphforge.algorithm_schema_version", "1"],
      ["graphforge.verb", "paths"],
    ]),
  );
  assert.deepEqual(
    Array.from(table.getChild("node_uuid"), uuidHex),
    ["Alice", "Bob", "Dan", "Carol"].map((name) => uuids[name]),
  );
  assert.deepEqual([...table.getChild("depth").toArray()], [0n, 1n, 2n, 1n]);
  assert.deepEqual([...table.getChild("order").toArray()], [0n, 1n, 2n, 3n]);
  assert.deepEqual(
    Array.from(
      dfs(forge, handles.Alice.uuid, "KNOWS").getChild("node_uuid"),
      uuidHex,
    ),
    Array.from(table.getChild("node_uuid"), uuidHex),
  );
});

test("DFS obeys direction and relationship filtering", () => {
  const { forge, handles, uuids } = fixture();
  assert.deepEqual(
    Array.from(dfs(forge, handles.Dan, "KNOWS").getChild("node_uuid"), uuidHex),
    [uuids.Dan],
  );
  assert.deepEqual(
    new Set(
      Array.from(
        dfs(forge, handles.Dan, "KNOWS", false).getChild("node_uuid"),
        uuidHex,
      ),
    ),
    new Set(["Alice", "Bob", "Carol", "Dan"].map((name) => uuids[name])),
  );
  assert.deepEqual(
    Array.from(
      dfs(forge, handles.Alice, "OTHER").getChild("node_uuid"),
      uuidHex,
    ),
    [uuids.Alice, uuids.Eve],
  );
});

test("DFS keeps isolated, disconnected, and missing-edge sources as singletons", () => {
  const { forge, handles, uuids } = fixture();
  for (const [source, via] of [
    [handles.Isolate, undefined],
    [handles.Alice, "MISSING"],
  ]) {
    const table = dfs(forge, source, via);
    assert.equal(table.numRows, 1);
    assert.deepEqual([...table.getChild("depth").toArray()], [0n]);
    assert.deepEqual([...table.getChild("order").toArray()], [0n]);
  }
  assert.equal(
    uuidHex(dfs(forge, handles.Isolate).getChild("node_uuid").get(0)),
    uuids.Isolate,
  );
});

test("DFS preserves native selector, option, empty-graph, and lifecycle errors", () => {
  const { forge, handles } = fixture();
  expectValidation("dfs does not accept a target selector", () =>
    forge.paths(handles.Alice, handles.Dan, "dfs"),
  );
  expectValidation("dfs k must be 1", () =>
    forge.paths(handles.Alice, undefined, "dfs", undefined, undefined, 2),
  );
  expectValidation("dfs does not accept an edge weight property", () =>
    forge.paths(
      handles.Alice,
      undefined,
      "dfs",
      undefined,
      undefined,
      undefined,
      "distance",
    ),
  );
  expectValidation("dfs does not accept a heuristic property", () =>
    forge.paths(
      handles.Alice,
      undefined,
      "dfs",
      undefined,
      undefined,
      undefined,
      undefined,
      "estimate",
    ),
  );
  expectValidation('invalid paths relationship selector " "', () =>
    forge.paths(handles.Alice, undefined, "dfs", " "),
  );
  expectValidation("node selector matched no nodes", () =>
    forge.paths("01900000-0000-7000-8000-000000000000", undefined, "dfs"),
  );
  expectValidation("node selector matched no nodes", () =>
    new GraphForge().paths(
      "01900000-0000-7000-8000-000000000000",
      undefined,
      "dfs",
    ),
  );

  const closed = new GraphForge();
  const handle = closed.addNode("Person", { name: "Closed" });
  closed.close();
  assert.throws(
    () => closed.paths(handle, undefined, "dfs"),
    (error) => error.code === "LifecycleError",
  );
});
