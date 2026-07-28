// Native find-cycles acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
import { uuidHex } from "../lib/helpers.mjs";

const handleHex = (handle) => handle.uuid.replaceAll("-", "");

const expectValidation = (message, call) => {
  assert.throws(call, (error) => {
    assert.equal(error.code, "ValidationError");
    assert.equal(error.message, message);
    return true;
  });
};

const canonical = (cycle, directed) => {
  const rotations = (values) =>
    values.map((_, offset) => [
      ...values.slice(offset),
      ...values.slice(0, offset),
    ]);
  const candidates = rotations(cycle);
  if (!directed && cycle.length > 1) {
    candidates.push(...rotations([...cycle].reverse()));
  }
  return candidates.sort((left, right) =>
    left.join("").localeCompare(right.join("")),
  )[0];
};

const cycleRows = (table) => {
  const column = table.getChild("cycle");
  assert.equal(column.nullCount, 0);
  return Array.from({ length: table.numRows }, (_, row) => {
    const cycle = Array.from(column.get(row), uuidHex);
    assert.ok(cycle.every((item) => item !== null));
    assert.ok(cycle.length === 1 || cycle[0] !== cycle.at(-1));
    return cycle;
  });
};

const assertSchemaAndMetadata = (table) => {
  const field = table.schema.fields[0];
  assert.deepEqual(
    table.schema.fields.map((value) => [
      value.name,
      String(value.type),
      value.nullable,
    ]),
    [["cycle", "List<FixedSizeBinary[16]>", false]],
  );
  assert.equal(field.type.children[0].nullable, false);
  assert.equal(String(field.type.children[0].type), "FixedSizeBinary[16]");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "find_cycles",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );
};

test("find cycles returns canonical direction-aware UUID lists", () => {
  const forge = new GraphForge();
  const nodes = Object.fromEntries(
    ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox", "Gus"].map((name) => [
      name,
      forge.addNode("Person", { name }),
    ]),
  );
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(e:Person {name:'Eve'}), (f:Person {name:'Fox'}) " +
      "CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), " +
      "(b)-[:ROAD]->(c), (c)-[:ROAD]->(a), " +
      "(b)-[:ROAD]->(d), (d)-[:ROAD]->(b), (d)-[:ROAD]->(d), " +
      "(e)-[:ROAD]->(f), (a)-[:OTHER]->(d), (d)-[:OTHER]->(a)",
  );
  const uuid = (name) => handleHex(nodes[name]);
  const run = (directed = true) =>
    tableFromIPC(forge.analyze("find_cycles", "Person", "ROAD", directed));

  const directed = run();
  assertSchemaAndMetadata(directed);
  const expectedDirected = [
    canonical(["Alice", "Bob", "Carol"].map(uuid), true),
    canonical(["Bob", "Dan"].map(uuid), true),
    [uuid("Dan")],
  ].sort();
  assert.deepEqual(cycleRows(directed), expectedDirected);
  assert.deepEqual(cycleRows(run()), expectedDirected);
  assert.ok(!expectedDirected.flat().includes(uuid("Gus")));

  const undirected = run(false);
  assertSchemaAndMetadata(undirected);
  assert.deepEqual(
    cycleRows(undirected),
    [
      canonical(["Alice", "Bob", "Carol"].map(uuid), false),
      [uuid("Dan")],
    ].sort(),
  );

  const missing = tableFromIPC(forge.analyze("find_cycles", "Missing", "ROAD"));
  const empty = tableFromIPC(new GraphForge().analyze("find_cycles"));
  for (const result of [missing, empty]) {
    assertSchemaAndMetadata(result);
    assert.equal(result.numRows, 0);
    assert.deepEqual(result.schema.fields, directed.schema.fields);
    assert.deepEqual(
      [...result.schema.metadata.entries()],
      [...directed.schema.metadata.entries()],
    );
    assert.deepEqual(cycleRows(result), []);
  }
});

test("find cycles preserves Rust registry validation", () => {
  const forge = new GraphForge();
  expectValidation("find_cycles does not accept an edge weight property", () =>
    forge.analyze("find_cycles", undefined, undefined, true, "cost"),
  );
  expectValidation('invalid analyze relationship selector " "', () =>
    forge.analyze("find_cycles", undefined, " "),
  );
  expectValidation('invalid analyze label ""', () =>
    forge.analyze("find_cycles", ""),
  );
});
