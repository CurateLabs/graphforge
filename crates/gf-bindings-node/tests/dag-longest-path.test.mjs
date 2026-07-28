// Native DAG-longest-path acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
import { pathHex } from "../lib/helpers.mjs";

const handleHex = (handle) => handle.uuid.replaceAll("-", "");

const assertSchemaAndMetadata = (table) => {
  const pathField = table.schema.fields[1];
  assert.deepEqual(
    table.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [
      ["cost", "Float64", false],
      ["path", "List<FixedSizeBinary[16]>", false],
    ],
  );
  assert.equal(pathField.type.children[0].nullable, false);
  assert.equal(String(pathField.type.children[0].type), "FixedSizeBinary[16]");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "dag_longest_path",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );
  assert.equal(table.getChild("cost").nullCount, 0);
  assert.equal(table.getChild("path").nullCount, 0);
};

test("dag longest path returns deterministic native UUID output", () => {
  // This test only invokes and decodes the Rust implementation (#772).
  const forge = new GraphForge();
  const nodes = Object.fromEntries(
    ["Alice", "Bob", "Carol", "Dan", "Eve"].map((name) => [
      name,
      forge.addNode("Person", { name }),
    ]),
  );
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(e:Person {name:'Eve'}) " +
      "CREATE (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(c), " +
      "(b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), (a)-[:OTHER]->(e)",
  );
  const run = () =>
    tableFromIPC(forge.analyze("dag_longest_path", "Person", "KNOWS", true));
  const table = run();

  assertSchemaAndMetadata(table);
  assert.equal(table.numRows, 1);
  assert.deepEqual([...table.getChild("cost").toArray()], [2]);
  const middle = [handleHex(nodes.Bob), handleHex(nodes.Carol)].sort()[0];
  assert.deepEqual(pathHex(table, 0), [
    handleHex(nodes.Alice),
    middle,
    handleHex(nodes.Dan),
  ]);
  assert.deepEqual(pathHex(run(), 0), pathHex(table, 0));
});

test("dag longest path preserves typed empty selection results", () => {
  const forge = new GraphForge();
  const reference = tableFromIPC(
    forge.analyze("dag_longest_path", undefined, undefined, true),
  );
  const missing = tableFromIPC(
    forge.analyze("dag_longest_path", "Missing", undefined, true),
  );

  for (const result of [reference, missing]) {
    assertSchemaAndMetadata(result);
    assert.equal(result.numRows, 1);
    assert.deepEqual([...result.getChild("cost").toArray()], [0]);
    assert.deepEqual(pathHex(result, 0), []);
    assert.deepEqual(result.schema.fields, reference.schema.fields);
    assert.deepEqual(
      [...result.schema.metadata.entries()],
      [...reference.schema.metadata.entries()],
    );
  }
});

test("dag longest path preserves structured native failures", () => {
  const forge = new GraphForge();
  forge.execute("CREATE (a:Person)-[:KNOWS]->(b:Person)-[:KNOWS]->(a)");
  assert.throws(
    () => forge.analyze("dag_longest_path", undefined, undefined, true),
    (error) => {
      assert.equal(error.code, "ExecutionError");
      assert.equal(
        error.message,
        "Rust algorithm execution failed: " +
          "dag_longest_path requires a directed acyclic graph",
      );
      return true;
    },
  );

  for (const [message, call] of [
    [
      "dag_longest_path requires directed=true",
      () => forge.analyze("dag_longest_path", undefined, undefined, false),
    ],
    [
      "dag_longest_path does not accept an edge weight property",
      () =>
        forge.analyze("dag_longest_path", undefined, undefined, true, "cost"),
    ],
    [
      'invalid analyze relationship selector " "',
      () => forge.analyze("dag_longest_path", undefined, " ", true),
    ],
    [
      'invalid analyze label ""',
      () => forge.analyze("dag_longest_path", "", undefined, true),
    ],
  ]) {
    assert.throws(call, (error) => {
      assert.equal(error.code, "ValidationError");
      assert.equal(error.message, message);
      return true;
    });
  }
});
