// Native triad-census acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const names = [
  "003",
  "012",
  "102",
  "021D",
  "021U",
  "021C",
  "111D",
  "111U",
  "030T",
  "030C",
  "201",
  "120D",
  "120U",
  "120C",
  "210",
  "300",
];

const fixture = () => {
  const forge = new GraphForge();
  for (const name of ["Alice", "Bob", "Carol", "Isolate"]) {
    forge.addNode("Person", { name });
  }
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}) " +
      "CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), " +
      "(a)-[:ROAD]->(a), (a)-[:ROAD]->(b), (a)-[:OTHER]->(c)",
  );
  return forge;
};

const table = (forge, label = "Person", via = "ROAD") =>
  tableFromIPC(forge.analyze("triad_census", label, via, true));

const assertSchema = (result) => {
  assert.deepEqual(
    result.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [
      ["triad_type", "Utf8", false],
      ["count", "Uint64", false],
    ],
  );
  assert.equal(
    result.schema.metadata.get("graphforge.algorithm"),
    "triad_census",
  );
  assert.equal(result.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(
    result.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );
};

test("triad census returns canonical native rows", () => {
  const forge = fixture();
  const result = table(forge);
  assertSchema(result);
  assert.equal(result.numRows, 16);
  assert.deepEqual([...result.getChild("triad_type").toArray()], names);
  assert.deepEqual(
    [...result.getChild("count").toArray()],
    [0n, 3n, 0n, 0n, 0n, 0n, 0n, 0n, 0n, 1n, 0n, 0n, 0n, 0n, 0n, 0n],
  );
  assert.deepEqual(
    [...table(forge).getChild("count").toArray()],
    [...result.getChild("count").toArray()],
  );
});

test("triad census preserves typed empty and validation behavior", () => {
  for (const result of [
    table(fixture(), "Missing"),
    tableFromIPC(
      new GraphForge().analyze("triad_census", undefined, undefined, true),
    ),
  ]) {
    assertSchema(result);
    assert.equal(result.numRows, 16);
    assert.deepEqual([...result.getChild("triad_type").toArray()], names);
    assert.equal(
      [...result.getChild("count").toArray()].reduce(
        (sum, count) => sum + count,
        0n,
      ),
      0n,
    );
  }

  const forge = new GraphForge();
  const invalid = [
    [
      "triad_census requires directed=true",
      () => forge.analyze("triad_census", undefined, undefined, false),
    ],
    [
      'invalid analyze relationship selector " "',
      () => forge.analyze("triad_census", undefined, " ", true),
    ],
    [
      'invalid analyze label ""',
      () => forge.analyze("triad_census", "", undefined, true),
    ],
    [
      "triad_census does not accept an edge weight property",
      () => forge.analyze("triad_census", undefined, undefined, true, "cost"),
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
