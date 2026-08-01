// Native chromatic-number acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

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
    [["chromatic_number", "Uint64", false]],
  );
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "chromatic_number",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );
  assert.equal(table.numRows, 1);
  assert.equal(table.getChild("chromatic_number").nullCount, 0);
};

const fixture = () => {
  const forge = new GraphForge();
  for (const name of ["Alice", "Bob", "Carol", "Dan", "Eve"]) {
    forge.addNode("Person", { name });
  }
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(e:Person {name:'Eve'}) " +
      "CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), " +
      "(c)-[:ROAD]->(a), (a)-[:ROAD]->(b), " +
      "(b)-[:ROAD]->(a), (d)-[:OTHER]->(e)",
  );
  return forge;
};

test("chromatic number returns the exact native Rust scalar", () => {
  const forge = fixture();
  const run = () =>
    tableFromIPC(forge.analyze("chromatic_number", "Person", "ROAD", false));
  const table = run();

  assertSchemaAndMetadata(table);
  assert.deepEqual([...table.getChild("chromatic_number").toArray()], [3n]);
  assert.deepEqual(
    [...run().getChild("chromatic_number").toArray()],
    [...table.getChild("chromatic_number").toArray()],
  );
});

test("chromatic number preserves selectors and typed scalar empty behavior", () => {
  const forge = fixture();
  const results = [
    [
      tableFromIPC(
        forge.analyze("chromatic_number", "Person", "MISSING", false),
      ),
      1n,
    ],
    [
      tableFromIPC(forge.analyze("chromatic_number", "Missing", "ROAD", false)),
      0n,
    ],
    [
      tableFromIPC(
        new GraphForge().analyze(
          "chromatic_number",
          undefined,
          undefined,
          false,
        ),
      ),
      0n,
    ],
  ];
  for (const [result, expected] of results) {
    assertSchemaAndMetadata(result);
    assert.deepEqual(
      [...result.getChild("chromatic_number").toArray()],
      [expected],
    );
  }
});

test("chromatic number preserves Rust loop and option validation", () => {
  const loop = new GraphForge();
  loop.addNode("Person", { name: "Loop" });
  loop.execute("MATCH (n:Person) CREATE (n)-[:ROAD]->(n)");
  assert.throws(
    () => loop.analyze("chromatic_number", "Person", "ROAD", false),
    (error) => {
      assert.equal(error.code, "ExecutionError");
      assert.equal(
        error.message,
        "Rust algorithm execution failed: chromatic_number is undefined for " +
          "a graph containing a self-loop",
      );
      return true;
    },
  );

  const forge = new GraphForge();
  expectValidation("chromatic_number requires directed=false", () =>
    forge.analyze("chromatic_number"),
  );
  expectValidation(
    "chromatic_number does not accept an edge weight property",
    () =>
      forge.analyze("chromatic_number", undefined, undefined, false, "cost"),
  );
  expectValidation('invalid analyze relationship selector " "', () =>
    forge.analyze("chromatic_number", undefined, " ", false),
  );
  expectValidation('invalid analyze label ""', () =>
    forge.analyze("chromatic_number", "", undefined, false),
  );
});
