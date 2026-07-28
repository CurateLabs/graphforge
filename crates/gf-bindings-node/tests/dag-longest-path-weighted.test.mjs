// Native weighted-DAG-longest-path acceptance against the freshly built addon.

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
    "dag_longest_path_weighted",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );
  assert.equal(table.getChild("cost").nullCount, 0);
  assert.equal(table.getChild("path").nullCount, 0);
};

test("weighted DAG longest path returns deterministic native UUID output", () => {
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
      "CREATE (a)-[:ROAD {cost:2.0}]->(b), " +
      "(b)-[:ROAD {cost:3.0}]->(d), " +
      "(a)-[:ROAD {cost:2.0}]->(c), " +
      "(c)-[:ROAD {cost:3.0}]->(d), " +
      "(e)-[:ROAD {cost:-8.0}]->(d), " +
      "(a)-[:OTHER {cost:100.0}]->(d)",
  );
  const run = () =>
    tableFromIPC(
      forge.analyze(
        "dag_longest_path_weighted",
        "Person",
        "ROAD",
        true,
        "cost",
      ),
    );
  const table = run();

  assertSchemaAndMetadata(table);
  assert.equal(table.numRows, 1);
  assert.deepEqual([...table.getChild("cost").toArray()], [5]);
  const middle = [handleHex(nodes.Bob), handleHex(nodes.Carol)].sort()[0];
  assert.deepEqual(pathHex(table, 0), [
    handleHex(nodes.Alice),
    middle,
    handleHex(nodes.Dan),
  ]);
  assert.deepEqual(pathHex(run(), 0), pathHex(table, 0));
});

test("weighted DAG longest path preserves typed empty selection results", () => {
  const forge = new GraphForge();
  const reference = tableFromIPC(
    forge.analyze(
      "dag_longest_path_weighted",
      undefined,
      undefined,
      true,
      "cost",
    ),
  );
  const missing = tableFromIPC(
    forge.analyze(
      "dag_longest_path_weighted",
      "Missing",
      undefined,
      true,
      "cost",
    ),
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

test("weighted DAG longest path preserves structured native failures", () => {
  const cyclic = new GraphForge();
  cyclic.execute(
    "CREATE (a:Person)-[:ROAD {cost:1.0}]->" +
      "(b:Person)-[:ROAD {cost:1.0}]->(a)",
  );
  assert.throws(
    () =>
      cyclic.analyze(
        "dag_longest_path_weighted",
        undefined,
        undefined,
        true,
        "cost",
      ),
    (error) => {
      assert.equal(error.code, "ExecutionError");
      assert.equal(
        error.message,
        "Rust algorithm execution failed: " +
          "dag_longest_path_weighted requires a directed acyclic graph",
      );
      return true;
    },
  );

  const forge = new GraphForge();
  forge.execute("CREATE (:Person)-[:ROAD {cost:1.0}]->(:Person)");
  const invalidWeight = new GraphForge();
  invalidWeight.execute("CREATE (:Person)-[:ROAD {cost:'heavy'}]->(:Person)");
  for (const [message, call] of [
    [
      "dag_longest_path_weighted requires directed=true",
      () =>
        forge.analyze(
          "dag_longest_path_weighted",
          undefined,
          undefined,
          false,
          "cost",
        ),
    ],
    [
      "dag_longest_path_weighted requires an edge weight property",
      () => forge.analyze("dag_longest_path_weighted"),
    ],
    [
      'invalid analyze weight property " "',
      () =>
        forge.analyze(
          "dag_longest_path_weighted",
          undefined,
          undefined,
          true,
          " ",
        ),
    ],
    [
      'invalid analyze relationship selector " "',
      () =>
        forge.analyze(
          "dag_longest_path_weighted",
          undefined,
          " ",
          true,
          "cost",
        ),
    ],
    [
      'invalid analyze label ""',
      () =>
        forge.analyze("dag_longest_path_weighted", "", undefined, true, "cost"),
    ],
    [
      'edge weight property "missing" does not exist',
      () =>
        forge.analyze(
          "dag_longest_path_weighted",
          undefined,
          undefined,
          true,
          "missing",
        ),
    ],
    [
      'edge weight property "cost" must be numeric',
      () =>
        invalidWeight.analyze(
          "dag_longest_path_weighted",
          undefined,
          undefined,
          true,
          "cost",
        ),
    ],
  ]) {
    assert.throws(call, (error) => {
      assert.equal(error.code, "ValidationError");
      assert.equal(error.message, message);
      return true;
    });
  }
});
