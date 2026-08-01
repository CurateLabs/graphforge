// Native dyad-census acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const categories = ["mutual", "asymmetric", "null"];

const fixture = () => {
  const forge = new GraphForge();
  for (const name of ["Alice", "Bob", "Carol", "Dan", "Isolate"]) {
    forge.addNode("Person", { name });
  }
  for (const name of ["Fox", "Owl"]) {
    forge.addNode("Animal", { name });
  }
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}) " +
      "CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(a), " +
      "(a)-[:ROAD]->(b), (a)-[:ROAD]->(c), (d)-[:ROAD]->(c), " +
      "(a)-[:ROAD]->(a), (c)-[:OTHER]->(a)",
  );
  return forge;
};

const table = (forge, label = "Person", via = "ROAD") =>
  tableFromIPC(forge.analyze("dyad_census", label, via, true));

const assertSchema = (result) => {
  assert.deepEqual(
    result.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [
      ["dyad_type", "Utf8", false],
      ["count", "Uint64", false],
    ],
  );
  assert.deepEqual(
    [...result.schema.metadata.entries()],
    [
      ["graphforge.algorithm", "dyad_census"],
      ["graphforge.algorithm_schema_version", "1"],
      ["graphforge.verb", "analyze"],
    ],
  );
  assert.equal(result.numRows, 3);
  assert.deepEqual([...result.getChild("dyad_type").toArray()], categories);
};

test("dyad census returns canonical native rows and normalized counts", () => {
  const forge = fixture();
  const result = table(forge);
  assertSchema(result);
  assert.deepEqual([...result.getChild("count").toArray()], [1n, 2n, 7n]);
  assert.deepEqual(
    [...table(forge).getChild("count").toArray()],
    [...result.getChild("count").toArray()],
  );

  const allRelationships = tableFromIPC(
    forge.analyze("dyad_census", "Person", undefined, true),
  );
  assertSchema(allRelationships);
  assert.deepEqual(
    [...allRelationships.getChild("count").toArray()],
    [2n, 1n, 7n],
  );
});

test("dyad census preserves selected empty singleton and edgeless rows", () => {
  const forge = fixture();
  for (const [result, counts] of [
    [table(forge, "Missing"), [0n, 0n, 0n]],
    [table(forge, "Animal"), [0n, 0n, 1n]],
    [
      tableFromIPC(
        new GraphForge().analyze("dyad_census", undefined, undefined, true),
      ),
      [0n, 0n, 0n],
    ],
  ]) {
    assertSchema(result);
    assert.deepEqual([...result.getChild("count").toArray()], counts);
  }

  const singleton = new GraphForge();
  singleton.addNode("Person", { name: "Solo" });
  const singletonResult = table(singleton);
  assertSchema(singletonResult);
  assert.deepEqual(
    [...singletonResult.getChild("count").toArray()],
    [0n, 0n, 0n],
  );
});

test("dyad census preserves structured directed and selector validation", () => {
  const forge = new GraphForge();
  const invalid = [
    [
      "dyad_census requires directed=true",
      () => forge.analyze("dyad_census", undefined, undefined, false),
    ],
    [
      'invalid analyze relationship selector " "',
      () => forge.analyze("dyad_census", undefined, " ", true),
    ],
    [
      'invalid analyze label ""',
      () => forge.analyze("dyad_census", "", undefined, true),
    ],
    [
      "dyad_census does not accept an edge weight property",
      () => forge.analyze("dyad_census", undefined, undefined, true, "cost"),
    ],
  ];
  for (const [message, call] of invalid) {
    assert.throws(call, (error) => {
      assert.equal(error.code, "ValidationError");
      assert.equal(error.message, message);
      return true;
    });
  }
});
