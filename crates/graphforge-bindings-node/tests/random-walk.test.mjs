import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const uuidHex = (value) => Buffer.from(value).toString("hex");
const handleHex = (handle) => handle.uuid.replaceAll("-", "");
const walkHex = (table, row) =>
  Array.from(table.getChild("walk").get(row), uuidHex);
const resultHex = (table) => ({
  starts: Array.from(table.getChild("start_uuid"), uuidHex),
  walks: Array.from({ length: table.numRows }, (_, row) => walkHex(table, row)),
});

function fixture() {
  const forge = new GraphForge();
  const alice = forge.addNode("Person", { name: "Alice" });
  const bob = forge.addNode("Person", { name: "Bob" });
  const carol = forge.addNode("Person", { name: "Carol" });
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}) " +
      "CREATE (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c)",
  );
  return { forge, alice, bob, carol };
}

function randomWalk(forge, source, overrides = {}) {
  const options = {
    target: undefined,
    by: "random_walk",
    via: "KNOWS",
    directed: true,
    k: 2,
    weight: undefined,
    heuristic: undefined,
    walkLength: 2,
    seed: 42n,
    ...overrides,
  };
  return tableFromIPC(
    forge.paths(
      source,
      options.target,
      options.by,
      options.via,
      options.directed,
      options.k,
      options.weight,
      options.heuristic,
      options.walkLength,
      options.seed,
    ),
  );
}

test("random walk is seeded and exposes the canonical UUID-only schema", () => {
  const { forge, alice, bob, carol } = fixture();
  const first = randomWalk(forge, alice);
  const repeated = randomWalk(forge, alice.uuid);
  assert.deepEqual(
    first.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [
      ["start_uuid", "FixedSizeBinary[16]", false],
      ["walk", "List<FixedSizeBinary[16]>", false],
    ],
  );
  assert.equal(
    first.schema.metadata.get("graphforge.algorithm"),
    "random_walk",
  );
  assert.deepEqual(resultHex(first), resultHex(repeated));
  assert.deepEqual(Array.from(first.getChild("start_uuid"), uuidHex), [
    handleHex(alice),
    handleHex(alice),
  ]);
  assert.deepEqual(walkHex(first, 0), [
    handleHex(alice),
    handleHex(bob),
    handleHex(carol),
  ]);
  assert.deepEqual(walkHex(first, 1), walkHex(first, 0));
});

test("random walk preserves structured Rust validation errors", () => {
  const { forge, alice, bob } = fixture();
  for (const overrides of [
    { target: bob },
    { k: 0 },
    { by: "bfs", walkLength: 2 },
    { seed: -1n },
  ]) {
    assert.throws(
      () => randomWalk(forge, alice, overrides),
      (error) => error.code === "ValidationError",
    );
  }
});
