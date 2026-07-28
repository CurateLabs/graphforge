// Native analyze acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
import { uuidHex } from "../lib/helpers.mjs";

const handleHex = (handle) => handle.uuid.replaceAll("-", "");

function expectValidation(message, call) {
  assert.throws(call, (error) => {
    assert.equal(error.code, "ValidationError");
    if (message instanceof RegExp) {
      assert.match(error.message, message);
    } else {
      assert.equal(error.message, message);
    }
    return true;
  });
}

function checkIsDag() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), " +
      "(c:Person {name: 'Carol'}), (x:Animal {name: 'Fox'}), " +
      "(y:Animal {name: 'Wolf'}), (a)-[:KNOWS]->(b), " +
      "(a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), " +
      "(x)-[:OTHER]->(y), (y)-[:OTHER]->(x)",
  );

  const table = tableFromIPC(forge.analyze("is_dag"));
  const field = table.schema.fields[0];
  assert.deepEqual(
    table.schema.fields.map((item) => item.name),
    ["is_dag"],
  );
  assert.equal(String(field.type), "Bool");
  assert.equal(field.nullable, false);
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "is_dag");
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(table.numRows, 1);
  assert.equal(table.getChild("is_dag").get(0), false);
  assert.equal(
    tableFromIPC(forge.analyze("is_dag", "Person")).getChild("is_dag").get(0),
    true,
  );
  assert.equal(
    tableFromIPC(forge.analyze("is_dag", undefined, "KNOWS"))
      .getChild("is_dag")
      .get(0),
    true,
  );
  assert.equal(
    tableFromIPC(forge.analyze("is_dag", "Person", undefined, false))
      .getChild("is_dag")
      .get(0),
    false,
  );
  assert.equal(
    tableFromIPC(new GraphForge().analyze("is_dag")).getChild("is_dag").get(0),
    true,
  );
  assert.throws(
    () => forge.analyze("is_dag", ""),
    (error) => error.code === "ValidationError",
  );
  assert.throws(
    () => forge.analyze("is_dag", undefined, " "),
    (error) => error.code === "ValidationError",
  );
}

function checkTopologicalSort() {
  const forge = new GraphForge();
  const people = ["Alice", "Bob", "Carol", "Dan"]
    .map((name) => [name, forge.addNode("Person", { name })])
    .sort((left, right) =>
      handleHex(left[1]).localeCompare(handleHex(right[1])),
    );
  forge.addNode("Animal", { name: "Fox" });
  forge.addNode("Animal", { name: "Wolf" });
  forge.execute(
    `MATCH (a:Person {name:'${people[0][0]}'}), ` +
      `(b:Person {name:'${people[1][0]}'}), ` +
      `(c:Person {name:'${people[2][0]}'}), ` +
      "(f:Animal {name:'Fox'}), (w:Animal {name:'Wolf'}) " +
      "CREATE (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(c), " +
      "(b)-[:KNOWS]->(c), (f)-[:OTHER]->(w), (w)-[:OTHER]->(f)",
  );

  const run = () => tableFromIPC(forge.analyze("topological_sort", "Person"));
  const table = run();
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "order"],
  );
  assert.deepEqual(
    table.schema.fields.map((field) => String(field.type)),
    ["FixedSizeBinary[16]", "Uint64"],
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.nullable),
    [false, false],
  );
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "topological_sort",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(table.getChild("node_id"), null);
  assert.deepEqual(
    Array.from(table.getChild("node_uuid"), uuidHex),
    people.map(([, handle]) => handleHex(handle)),
  );
  assert.deepEqual([...table.getChild("order").toArray()], [0n, 1n, 2n, 3n]);
  assert.deepEqual(
    Array.from(run().getChild("node_uuid"), uuidHex),
    Array.from(table.getChild("node_uuid"), uuidHex),
  );

  const via = tableFromIPC(
    forge.analyze("topological_sort", undefined, "KNOWS"),
  );
  assert.equal(via.numRows, 6);
  assert.deepEqual(
    [...via.getChild("order").toArray()],
    [0n, 1n, 2n, 3n, 4n, 5n],
  );
  assert.throws(
    () => forge.analyze("topological_sort"),
    (error) => {
      assert.equal(error.code, "ExecutionError");
      assert.equal(
        error.message,
        "Rust algorithm execution failed: selected graph contains a cycle",
      );
      return true;
    },
  );

  const empty = new GraphForge();
  const emptyTable = tableFromIPC(empty.analyze("topological_sort"));
  const missing = tableFromIPC(empty.analyze("topological_sort", "Missing"));
  assert.equal(emptyTable.numRows, 0);
  assert.deepEqual(emptyTable.schema.fields, missing.schema.fields);
  expectValidation("topological_sort requires directed=true", () =>
    empty.analyze("topological_sort", undefined, undefined, false),
  );
  expectValidation(
    "topological_sort does not accept an edge weight property",
    () => empty.analyze("topological_sort", undefined, undefined, true, "cost"),
  );
  expectValidation('invalid analyze label ""', () =>
    empty.analyze("topological_sort", ""),
  );
  expectValidation('invalid analyze relationship selector " "', () =>
    empty.analyze("topological_sort", undefined, " "),
  );
}

function checkArticulationPoints() {
  const forge = new GraphForge();
  const nodes = Object.fromEntries(
    ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox", "Gus", "Hal"].map((name) => [
      name,
      forge.addNode("Person", { name }),
    ]),
  );
  forge.addNode("Animal", { name: "Wolf" });
  forge.addNode("Animal", { name: "Yak" });
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(e:Person {name:'Eve'}), (f:Person {name:'Fox'}), " +
      "(g:Person {name:'Gus'}), (w:Animal {name:'Wolf'}), " +
      "(y:Animal {name:'Yak'}) " +
      "CREATE (a)-[:ROAD]->(b), (b)-[:ROAD]->(c), (c)-[:ROAD]->(a), " +
      "(b)-[:ROAD]->(d), (d)-[:ROAD]->(b), (d)-[:ROAD]->(e), " +
      "(d)-[:ROAD]->(d), (f)-[:ROAD]->(g), (a)-[:OTHER]->(e), " +
      "(w)-[:ROAD]->(y)",
  );

  const run = () =>
    tableFromIPC(forge.analyze("articulation_points", "Person", "ROAD", false));
  const table = run();
  assert.deepEqual(
    table.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [["node_uuid", "FixedSizeBinary[16]", false]],
  );
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "articulation_points",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(table.getChild("node_id"), null);
  const expected = [nodes.Bob, nodes.Dan]
    .map(handleHex)
    .sort((left, right) => left.localeCompare(right));
  assert.deepEqual(Array.from(table.getChild("node_uuid"), uuidHex), expected);
  assert.equal(table.getChild("node_uuid").nullCount, 0);
  assert.deepEqual(Array.from(run().getChild("node_uuid"), uuidHex), expected);

  const noResult = tableFromIPC(
    forge.analyze("articulation_points", "Person", "OTHER", false),
  );
  const missing = tableFromIPC(
    forge.analyze("articulation_points", "Missing", "ROAD", false),
  );
  const empty = tableFromIPC(
    new GraphForge().analyze(
      "articulation_points",
      undefined,
      undefined,
      false,
    ),
  );
  assert.equal(noResult.numRows, 0);
  assert.equal(missing.numRows, 0);
  assert.equal(empty.numRows, 0);
  assert.deepEqual(empty.schema.fields, table.schema.fields);

  expectValidation("articulation_points requires directed=false", () =>
    forge.analyze("articulation_points"),
  );
  expectValidation(
    "articulation_points does not accept an edge weight property",
    () =>
      forge.analyze("articulation_points", undefined, undefined, false, "cost"),
  );
  expectValidation('invalid analyze relationship selector " "', () =>
    forge.analyze("articulation_points", undefined, " ", false),
  );
  expectValidation('invalid analyze label ""', () =>
    forge.analyze("articulation_points", "", undefined, false),
  );
}

function checkMinimumSpanningTree() {
  const forge = new GraphForge();
  const nodes = Object.fromEntries(
    ["Alice", "Bob", "Carol", "Dan", "Eve", "Fox"].map((name) => [
      name,
      forge.addNode("Person", { name }),
    ]),
  );
  forge.addNode("Animal", { name: "Wolf" });
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), " +
      "(e:Person {name:'Eve'}), (f:Person {name:'Fox'}) " +
      "CREATE (a)-[:ROAD {cost:4.0}]->(b), " +
      "(a)-[:ROAD {cost:3.0}]->(c), (b)-[:ROAD {cost:1.0}]->(c), " +
      "(b)-[:ROAD {cost:2.0}]->(d), (c)-[:ROAD {cost:4.0}]->(d), " +
      "(e)-[:ROAD {cost:-2.0}]->(f), (e)-[:ROAD {cost:3.0}]->(f), " +
      "(d)-[:ROAD {cost:-10.0}]->(d), " +
      "(a)-[:OTHER {cost:-100.0}]->(d)",
  );

  const run = () =>
    tableFromIPC(
      forge.analyze("minimum_spanning_tree", "Person", "ROAD", false, "cost"),
    );
  const table = run();
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["edge_uuid", "source_uuid", "target_uuid", "weight"],
  );
  assert.deepEqual(
    table.schema.fields.map((field) => String(field.type)),
    [
      "FixedSizeBinary[16]",
      "FixedSizeBinary[16]",
      "FixedSizeBinary[16]",
      "Float64",
    ],
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.nullable),
    [false, false, false, true],
  );
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "minimum_spanning_tree",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  assert.equal(table.numRows, 4);
  assert.deepEqual([...table.getChild("weight").toArray()], [-2, 1, 2, 3]);
  assert.equal(table.getChild("weight").nullCount, 0);

  const expectedPairs = [
    ["Eve", "Fox"],
    ["Bob", "Carol"],
    ["Bob", "Dan"],
    ["Alice", "Carol"],
  ].map(([left, right]) =>
    [handleHex(nodes[left]), handleHex(nodes[right])].sort(),
  );
  const actualPairs = Array.from({ length: table.numRows }, (_, row) => [
    uuidHex(table.getChild("source_uuid").get(row)),
    uuidHex(table.getChild("target_uuid").get(row)),
  ]);
  assert.deepEqual(actualPairs, expectedPairs);
  assert.ok(actualPairs.every(([source, target]) => source < target));
  assert.equal(
    new Set(Array.from(table.getChild("edge_uuid"), uuidHex)).size,
    table.numRows,
  );
  assert.deepEqual(
    Array.from(run().getChild("edge_uuid"), uuidHex),
    Array.from(table.getChild("edge_uuid"), uuidHex),
  );
  assert.equal(
    tableFromIPC(
      forge.analyze("minimum_spanning_tree", "Missing", "ROAD", false, "cost"),
    ).numRows,
    0,
  );

  const tied = new GraphForge();
  const tiedNodes = ["Alice", "Bob", "Carol"]
    .map((name) => tied.addNode("Person", { name }))
    .sort((left, right) => handleHex(left).localeCompare(handleHex(right)));
  tied.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}) " +
      "CREATE (a)-[:ROAD]->(b), (a)-[:ROAD]->(b), " +
      "(a)-[:ROAD]->(c), (b)-[:ROAD]->(c)",
  );
  const unit = tableFromIPC(
    tied.analyze("minimum_spanning_tree", undefined, "ROAD", false),
  );
  assert.deepEqual([...unit.getChild("weight").toArray()], [1, 1]);
  assert.deepEqual(
    Array.from({ length: 2 }, (_, row) => [
      uuidHex(unit.getChild("source_uuid").get(row)),
      uuidHex(unit.getChild("target_uuid").get(row)),
    ]),
    [1, 2].map((target) => [
      handleHex(tiedNodes[0]),
      handleHex(tiedNodes[target]),
    ]),
  );
  const empty = tableFromIPC(
    new GraphForge().analyze(
      "minimum_spanning_tree",
      undefined,
      undefined,
      false,
    ),
  );
  assert.equal(empty.numRows, 0);
  assert.deepEqual(
    empty.schema.fields.map((field) => String(field.type)),
    unit.schema.fields.map((field) => String(field.type)),
  );

  const invalid = new GraphForge();
  invalid.execute(
    "CREATE (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(a)-[:ROAD {null_cost:null, text_cost:'heavy', " +
      "infinite_cost:1e308 * 2.0}]->(b)",
  );
  expectValidation("minimum_spanning_tree requires directed=false", () =>
    invalid.analyze("minimum_spanning_tree"),
  );
  expectValidation('invalid analyze relationship selector " "', () =>
    invalid.analyze("minimum_spanning_tree", undefined, " ", false),
  );
  expectValidation('invalid analyze weight property " "', () =>
    invalid.analyze("minimum_spanning_tree", undefined, "ROAD", false, " "),
  );
  expectValidation('edge weight property "missing" does not exist', () =>
    invalid.analyze(
      "minimum_spanning_tree",
      undefined,
      "ROAD",
      false,
      "missing",
    ),
  );
  expectValidation('edge weight property "text_cost" must be numeric', () =>
    invalid.analyze(
      "minimum_spanning_tree",
      undefined,
      "ROAD",
      false,
      "text_cost",
    ),
  );
  for (const property of ["null_cost", "infinite_cost"]) {
    expectValidation(
      /^edge weight is missing, NULL, NaN, or infinite for edge [0-9a-f-]{36}$/,
      () =>
        invalid.analyze(
          "minimum_spanning_tree",
          undefined,
          "ROAD",
          false,
          property,
        ),
    );
  }
  expectValidation("is_dag does not accept an edge weight property", () =>
    invalid.analyze("is_dag", undefined, undefined, true, "cost"),
  );
}

test("is dag", checkIsDag);
test("topological sort", checkTopologicalSort);
test("articulation points", checkArticulationPoints);
test("minimum spanning tree", checkMinimumSpanningTree);
