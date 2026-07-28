// Native edge-coloring acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
import { uuidHex } from "../lib/helpers.mjs";

const expectValidation = (message, call) => {
  assert.throws(call, (error) => {
    assert.equal(error.code, "ValidationError");
    assert.equal(error.message, message);
    return true;
  });
};

const assertSchemaAndMetadata = (table) => {
  assert.deepEqual(
    table.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [
      ["edge_uuid", "FixedSizeBinary[16]", false],
      ["color", "Uint64", false],
    ],
  );
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "edge_coloring",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );
  assert.equal(table.getChild("edge_uuid").nullCount, 0);
  assert.equal(table.getChild("color").nullCount, 0);
  assert.equal(table.getChild("edge_id"), null);
};

const fixture = () => {
  const forge = new GraphForge();
  for (const [label, name] of [
    ["Person", "Alice"],
    ["Person", "Bob"],
    ["Person", "Carol"],
    ["Person", "Dan"],
    ["Person", "Eve"],
    ["Animal", "Fox"],
  ]) {
    forge.addNode(label, { name });
  }
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(e:Person {name:'Eve'}), (f:Animal {name:'Fox'}) " +
      "CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), " +
      "(b)-[:ROAD]->(a), (c)-[:ROAD]->(d), " +
      "(d)-[:OTHER]->(e), (f)-[:ROAD]->(a)",
  );
  return forge;
};

test("edge coloring returns deterministic proper UUID colors from native Rust", () => {
  const forge = fixture();
  const run = () =>
    tableFromIPC(forge.analyze("edge_coloring", "Person", "ROAD", false));
  const table = run();

  assertSchemaAndMetadata(table);
  const edgeIds = Array.from(table.getChild("edge_uuid"), uuidHex);
  assert.deepEqual(edgeIds, [...edgeIds].sort());
  assert.equal(edgeIds.length, 4);
  assert.deepEqual(Array.from(run().getChild("edge_uuid"), uuidHex), edgeIds);
  assert.deepEqual(
    [...run().getChild("color").toArray()],
    [...table.getChild("color").toArray()],
  );

  const projected = tableFromIPC(
    forge.execute(
      "MATCH (s:Person)-[r:ROAD]->(t:Person) " +
        "RETURN r.edge_uuid AS edge_uuid, " +
        "s.node_uuid AS source_uuid, t.node_uuid AS target_uuid",
    ),
  );
  const endpoints = new Map();
  for (let row = 0; row < projected.numRows; row += 1) {
    endpoints.set(uuidHex(projected.getChild("edge_uuid").get(row)), [
      uuidHex(projected.getChild("source_uuid").get(row)),
      uuidHex(projected.getChild("target_uuid").get(row)),
    ]);
  }
  assert.deepEqual(new Set(edgeIds), new Set(endpoints.keys()));

  const colors = new Map(
    edgeIds.map((edgeId, index) => [
      edgeId,
      table.getChild("color").get(index),
    ]),
  );
  for (let left = 0; left < edgeIds.length; left += 1) {
    for (let right = left + 1; right < edgeIds.length; right += 1) {
      if (
        endpoints
          .get(edgeIds[left])
          .some((node) => endpoints.get(edgeIds[right]).includes(node))
      ) {
        assert.notEqual(colors.get(edgeIds[left]), colors.get(edgeIds[right]));
      }
    }
  }
});

test("edge coloring preserves selection, multigraph, and typed empty behavior", () => {
  const forge = fixture();
  const reference = tableFromIPC(
    forge.analyze("edge_coloring", "Person", "ROAD", false),
  );
  const results = [
    tableFromIPC(forge.analyze("edge_coloring", "Missing", "ROAD", false)),
    tableFromIPC(forge.analyze("edge_coloring", "Person", "MISSING", false)),
    tableFromIPC(
      new GraphForge().analyze("edge_coloring", undefined, undefined, false),
    ),
  ];
  for (const result of results) {
    assertSchemaAndMetadata(result);
    assert.equal(result.numRows, 0);
    assert.deepEqual(result.schema.fields, reference.schema.fields);
    assert.deepEqual(
      [...result.schema.metadata.entries()],
      [...reference.schema.metadata.entries()],
    );
  }
});

test("edge coloring preserves Rust self-loop and option validation", () => {
  const loop = new GraphForge();
  loop.addNode("Person", { name: "Loop" });
  loop.execute("MATCH (n:Person) CREATE (n)-[:ROAD]->(n)");
  assert.throws(
    () => loop.analyze("edge_coloring", "Person", "ROAD", false),
    (error) => {
      assert.equal(error.code, "ExecutionError");
      assert.equal(
        error.message,
        "Rust algorithm execution failed: edge_coloring cannot color a graph " +
          "containing a self-loop",
      );
      return true;
    },
  );

  const forge = new GraphForge();
  expectValidation("edge_coloring requires directed=false", () =>
    forge.analyze("edge_coloring"),
  );
  expectValidation(
    "edge_coloring does not accept an edge weight property",
    () => forge.analyze("edge_coloring", undefined, undefined, false, "cost"),
  );
  expectValidation('invalid analyze relationship selector " "', () =>
    forge.analyze("edge_coloring", undefined, " ", false),
  );
  expectValidation('invalid analyze label ""', () =>
    forge.analyze("edge_coloring", "", undefined, false),
  );
});
