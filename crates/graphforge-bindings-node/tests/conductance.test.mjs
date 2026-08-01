// Native conductance acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const analyze = (forge, weight = "weight") =>
  tableFromIPC(
    forge.analyze("conductance", "Person", undefined, false, weight, "side"),
  );

const expectError = (code, message, call) => {
  assert.throws(call, (error) => {
    assert.equal(error.code, code);
    if (message instanceof RegExp) {
      assert.match(error.message, message);
    } else {
      assert.equal(error.message, message);
    }
    return true;
  });
};

test("conductance returns exact deterministic native partition rows", () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE " +
      "(a:Person {side:'alpha'}), " +
      "(b:Person {side:'alpha'}), " +
      "(c:Person {side:'beta'}), " +
      "(d:Person {side:'beta'}), " +
      "(a)-[:LINK {weight:2}]->(c), " +
      "(a)-[:LINK {weight:1}]->(c), " +
      "(b)-[:LINK {weight:1}]->(c), " +
      "(a)-[:LINK {weight:3}]->(b), " +
      "(d)-[:LINK {weight:4}]->(d)",
  );
  const table = analyze(forge);

  assert.deepEqual(
    table.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [
      ["partition_id", "Utf8", false],
      ["conductance", "Float64", false],
    ],
  );
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "conductance",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );
  assert.deepEqual([...table.getChild("partition_id")], ["alpha", "beta"]);
  assert.deepEqual([...table.getChild("conductance").toArray()], [0.4, 0.4]);

  const replay = analyze(forge);
  assert.deepEqual(
    [...replay.getChild("partition_id")],
    [...table.getChild("partition_id")],
  );
  assert.deepEqual(
    [...replay.getChild("conductance").toArray()],
    [...table.getChild("conductance").toArray()],
  );
});

test("conductance requires an undirected explicit partition", () => {
  const forge = new GraphForge();
  expectError("ValidationError", "conductance requires directed=false", () =>
    forge.analyze("conductance", "Person", undefined, true, undefined, "side"),
  );
  expectError(
    "ValidationError",
    "conductance requires a non-empty partition_property",
    () => forge.analyze("conductance", "Person", undefined, false),
  );
});

test("conductance rejects missing and invalid partition values", () => {
  for (const [properties, expected] of [
    ["", /missing a partition value/],
    ["side:1.5", /unsupported partition type/],
  ]) {
    const node = properties ? `(b:Person {${properties}})` : "(b:Person)";
    const forge = new GraphForge();
    forge.execute(
      `CREATE (a:Person {side:'alpha'}), ${node}, (a)-[:LINK]->(b)`,
    );
    expectError("ValidationError", expected, () =>
      forge.analyze(
        "conductance",
        "Person",
        undefined,
        false,
        undefined,
        "side",
      ),
    );
  }
});

test("conductance reports zero volume as a structured execution error", () => {
  const forge = new GraphForge();
  forge.execute("CREATE (a:Person {side:'alpha'}), (b:Person {side:'beta'})");
  expectError(
    "ExecutionError",
    "conductance is undefined for partition alpha: denominator volume is zero",
    () =>
      forge.analyze(
        "conductance",
        "Person",
        undefined,
        false,
        undefined,
        "side",
      ),
  );
});
