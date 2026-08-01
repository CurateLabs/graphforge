// Native is-planar acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const fixture = () => {
  const forge = new GraphForge();
  for (const name of ["A", "B", "C", "D", "E", "F"]) {
    forge.addNode("Person", { name });
  }
  forge.addNode("Animal", { name: "Fox" });
  forge.execute(
    "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), " +
      "(c:Person {name:'C'}), (d:Person {name:'D'}), " +
      "(e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(fox:Animal {name:'Fox'}) " +
      "CREATE (a)-[:ROAD]->(d), (a)-[:ROAD]->(e), (a)-[:ROAD]->(f), " +
      "(b)-[:ROAD]->(d), (b)-[:ROAD]->(e), (b)-[:ROAD]->(f), " +
      "(c)-[:ROAD]->(d), (c)-[:ROAD]->(e), (c)-[:ROAD]->(f), " +
      "(a)-[:ROAD]->(d), (d)-[:ROAD]->(a), (a)-[:ROAD]->(a), " +
      "(a)-[:OTHER]->(b), (fox)-[:ROAD]->(a)",
  );
  return forge;
};

const table = (forge, label = "Person", via = "ROAD") =>
  tableFromIPC(forge.analyze("is_planar", label, via, false));

const assertSchema = (result) => {
  assert.deepEqual(
    result.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [["is_planar", "Bool", false]],
  );
  assert.deepEqual(
    [...result.schema.metadata.entries()],
    [
      ["graphforge.algorithm", "is_planar"],
      ["graphforge.algorithm_schema_version", "1"],
      ["graphforge.verb", "analyze"],
    ],
  );
  assert.equal(result.numRows, 1);
  assert.equal(result.getChild("is_planar").nullCount, 0);
};

test("is_planar returns exact native Boolean and normalized projection", () => {
  const forge = fixture();
  const result = table(forge);
  assertSchema(result);
  assert.deepEqual([...result.getChild("is_planar").toArray()], [false]);
  assert.deepEqual(
    [...table(forge).getChild("is_planar").toArray()],
    [...result.getChild("is_planar").toArray()],
  );

  const other = table(forge, "Person", "OTHER");
  assertSchema(other);
  assert.deepEqual([...other.getChild("is_planar").toArray()], [true]);
});

test("is_planar preserves empty singleton forest and selection behavior", () => {
  const forge = fixture();
  for (const result of [
    table(forge, "Missing"),
    tableFromIPC(
      new GraphForge().analyze("is_planar", undefined, undefined, false),
    ),
  ]) {
    assertSchema(result);
    assert.deepEqual([...result.getChild("is_planar").toArray()], [true]);
  }

  const forest = new GraphForge();
  forest.execute(
    "CREATE (:Person {name:'A'})-[:ROAD]->(:Person {name:'B'}), " +
      "(:Person {name:'C'}), " +
      "(:Person {name:'D'})-[:ROAD]->(:Person {name:'E'})",
  );
  const forestResult = table(forest);
  assertSchema(forestResult);
  assert.deepEqual([...forestResult.getChild("is_planar").toArray()], [true]);
});

test("is_planar preserves structured option and selector validation", () => {
  const forge = new GraphForge();
  const invalid = [
    ["is_planar requires directed=false", () => forge.analyze("is_planar")],
    [
      'invalid analyze relationship selector " "',
      () => forge.analyze("is_planar", undefined, " ", false),
    ],
    [
      'invalid analyze label ""',
      () => forge.analyze("is_planar", "", undefined, false),
    ],
    [
      "is_planar does not accept an edge weight property",
      () => forge.analyze("is_planar", undefined, undefined, false, "cost"),
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
