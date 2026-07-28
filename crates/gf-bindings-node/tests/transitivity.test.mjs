// Native transitivity acceptance against the freshly built addon.

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
    [["transitivity", "Float64", false]],
  );
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "transitivity",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );
  assert.equal(table.getChild("transitivity").nullCount, 0);
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
      "CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), " +
      "(b)-[:ROAD]->(c), (c)-[:ROAD]->(a), " +
      "(b)-[:ROAD]->(d), (c)-[:ROAD]->(d), " +
      "(d)-[:ROAD]->(d), (a)-[:OTHER]->(e), " +
      "(e)-[:OTHER]->(c), (f)-[:ROAD]->(a)",
  );
  return forge;
};

test("transitivity returns the deterministic native scalar", () => {
  const forge = fixture();
  const run = () =>
    tableFromIPC(forge.analyze("transitivity", "Person", "ROAD", false));
  const table = run();

  assertSchemaAndMetadata(table);
  assert.equal(table.numRows, 1);
  assert.deepEqual([...table.getChild("transitivity").toArray()], [0.75]);
  assert.deepEqual(
    [...run().getChild("transitivity").toArray()],
    [...table.getChild("transitivity").toArray()],
  );
});

test("transitivity preserves projection and typed zero behavior", () => {
  const forge = fixture();
  const reference = tableFromIPC(
    forge.analyze("transitivity", "Person", "ROAD", false),
  );
  const results = [
    tableFromIPC(forge.analyze("transitivity", "Person", "OTHER", false)),
    tableFromIPC(forge.analyze("transitivity", "Missing", "ROAD", false)),
    tableFromIPC(
      new GraphForge().analyze("transitivity", undefined, undefined, false),
    ),
  ];

  for (const result of results) {
    assertSchemaAndMetadata(result);
    assert.equal(result.numRows, 1);
    assert.deepEqual([...result.getChild("transitivity").toArray()], [0]);
    assert.deepEqual(result.schema.fields, reference.schema.fields);
    assert.deepEqual(
      [...result.schema.metadata.entries()],
      [...reference.schema.metadata.entries()],
    );
  }
});

test("transitivity preserves Rust registry validation", () => {
  const forge = new GraphForge();
  expectValidation("transitivity requires directed=false", () =>
    forge.analyze("transitivity"),
  );
  expectValidation('invalid analyze relationship selector " "', () =>
    forge.analyze("transitivity", undefined, " ", false),
  );
  expectValidation('invalid analyze label ""', () =>
    forge.analyze("transitivity", "", undefined, false),
  );
  expectValidation("transitivity does not accept an edge weight property", () =>
    forge.analyze("transitivity", undefined, undefined, false, "cost"),
  );
});
