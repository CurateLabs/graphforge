// Native acceptance for reachability algorithms.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
import { uuidHex } from "../lib/helpers.mjs";

function rows(table) {
  return Array.from(table.getChild("source_uuid"), uuidHex).map(
    (source, index) => [
      source,
      uuidHex(table.getChild("target_uuid").get(index)),
    ],
  );
}

function sortedPairs(pairs) {
  return pairs.sort(
    ([leftSource, leftTarget], [rightSource, rightTarget]) =>
      leftSource.localeCompare(rightSource) ||
      leftTarget.localeCompare(rightTarget),
  );
}

test("transitive closure delegates global positive-length reachability to Rust", () => {
  const forge = new GraphForge();
  const handles = Object.fromEntries(
    ["Alice", "Bob", "Carol", "Dan", "Eve"].map((name) => [
      name,
      forge.addNode("Person", { name }),
    ]),
  );
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(e:Person {name:'Eve'}) " +
      "CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), " +
      "(b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), " +
      "(c)-[:KNOWS]->(c), (d)-[:OTHER]->(e)",
  );
  const identityTable = tableFromIPC(
    forge.execute(
      "MATCH (p:Person) RETURN p.name AS name, p.node_uuid AS uuid ORDER BY p.name",
    ),
  );
  const uuids = Object.fromEntries(
    Array.from(identityTable.getChild("name"), (name, index) => [
      name,
      uuidHex(identityTable.getChild("uuid").get(index)),
    ]),
  );
  const source = { label: "Person", property: "name", value: "Eve" };

  const table = tableFromIPC(
    forge.paths(source, undefined, "transitive_closure", "KNOWS"),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [
      ["source_uuid", "FixedSizeBinary[16]", false],
      ["target_uuid", "FixedSizeBinary[16]", false],
    ],
  );
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "transitive_closure",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "paths");
  const expected = sortedPairs(
    [
      ["Alice", "Alice"],
      ["Alice", "Bob"],
      ["Alice", "Carol"],
      ["Bob", "Alice"],
      ["Bob", "Bob"],
      ["Bob", "Carol"],
      ["Carol", "Carol"],
    ].map(([sourceName, targetName]) => [uuids[sourceName], uuids[targetName]]),
  );
  assert.deepEqual(rows(table), expected);

  const repeated = tableFromIPC(
    forge.paths(handles.Eve.uuid, undefined, "transitive_closure", "KNOWS"),
  );
  assert.deepEqual(rows(repeated), expected);

  const undirected = tableFromIPC(
    forge.paths(handles.Alice, undefined, "transitive_closure", "KNOWS", false),
  );
  const connected = ["Alice", "Bob", "Carol"].map((name) => uuids[name]);
  assert.deepEqual(
    rows(undirected),
    sortedPairs(
      connected.flatMap((sourceUuid) =>
        connected.map((targetUuid) => [sourceUuid, targetUuid]),
      ),
    ),
  );

  const other = tableFromIPC(
    forge.paths(handles.Alice, undefined, "transitive_closure", "OTHER"),
  );
  assert.deepEqual(rows(other), [[uuids.Dan, uuids.Eve]]);

  const isolated = new GraphForge();
  const isolatedSource = isolated.addNode("Person", { name: "Only" });
  assert.equal(
    tableFromIPC(
      isolated.paths(isolatedSource, undefined, "transitive_closure"),
    ).numRows,
    0,
  );

  const expectValidationError = (call) => {
    assert.throws(call, (error) => {
      assert.equal(error.code, "ValidationError");
      return true;
    });
  };
  expectValidationError(() =>
    forge.paths(handles.Alice, handles.Bob, "transitive_closure"),
  );
  expectValidationError(() =>
    forge.paths(
      handles.Alice,
      undefined,
      "transitive_closure",
      undefined,
      undefined,
      2,
    ),
  );
  expectValidationError(() =>
    forge.paths(
      handles.Alice,
      undefined,
      "transitive_closure",
      undefined,
      undefined,
      undefined,
      "cost",
    ),
  );
  expectValidationError(() =>
    forge.paths(
      handles.Alice,
      undefined,
      "transitive_closure",
      undefined,
      undefined,
      undefined,
      undefined,
      "estimate",
    ),
  );
  expectValidationError(() =>
    forge.paths(handles.Alice, undefined, "transitive_closure", " "),
  );
});
