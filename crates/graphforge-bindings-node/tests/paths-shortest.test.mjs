// Native acceptance for this coherent algorithm family.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
import { pathHex, uuidHex } from "../lib/helpers.mjs";

function checkDijkstraPaths() {
  // #1666 — napi delegates weighted path execution and only transports Arrow IPC.
  const forge = new GraphForge();
  const handles = Object.fromEntries(
    ["Alice", "Bob", "Carol", "Dan", "Eve"].map((name) => [
      name,
      forge.addNode("Person", { name }),
    ]),
  );
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), (e:Person {name:'Eve'}) " +
      "CREATE (a)-[:ROAD {cost:1.0, bad:'x', negative:-1.0}]->(c), " +
      "(a)-[:ROAD {cost:1.0, bad:'x', negative:-1.0}]->(b), " +
      "(b)-[:ROAD {cost:2.0, bad:'x', negative:-1.0}]->(d), " +
      "(c)-[:ROAD {cost:2.0, bad:'x', negative:-1.0}]->(d), " +
      "(a)-[:ROAD {cost:9.0, bad:'x', negative:-1.0}]->(d), " +
      "(d)-[:OTHER {cost:0.5}]->(e)",
  );
  const identities = tableFromIPC(
    forge.execute(
      "MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name",
    ),
  );
  const uuids = Array.from(identities.getChild("uuid"), uuidHex);
  const weighted = tableFromIPC(
    forge.paths(handles.Alice, undefined, "dijkstra", "ROAD", true, 1, "cost"),
  );
  assert.deepEqual(
    weighted.schema.fields.map((field) => field.name),
    ["source_uuid", "target_uuid", "cost", "path"],
  );
  assert.equal(
    weighted.schema.metadata.get("graphforge.algorithm"),
    "dijkstra",
  );
  assert.ok(weighted.schema.fields.every((field) => !field.nullable));
  assert.deepEqual(
    Array.from(weighted.getChild("target_uuid"), uuidHex),
    uuids.slice(0, 4),
  );
  assert.deepEqual([...weighted.getChild("cost").toArray()], [0, 1, 1, 3]);
  assert.deepEqual(pathHex(weighted, 3), [uuids[0], uuids[1], uuids[3]]);
  const repeated = tableFromIPC(
    forge.paths(
      handles.Alice.uuid,
      undefined,
      "dijkstra",
      "ROAD",
      true,
      1,
      "cost",
    ),
  );
  assert.deepEqual(pathHex(repeated, 3), pathHex(weighted, 3));
  const targeted = tableFromIPC(
    forge.paths(
      handles.Alice,
      handles.Dan,
      "dijkstra",
      "ROAD",
      true,
      1,
      "cost",
    ),
  );
  assert.equal(targeted.numRows, 1);
  assert.deepEqual(pathHex(targeted, 0), pathHex(weighted, 3));
  assert.equal(
    tableFromIPC(
      forge.paths(handles.Dan, handles.Alice, "dijkstra", "ROAD", true),
    ).numRows,
    0,
  );
  assert.equal(
    tableFromIPC(
      forge.paths(handles.Dan, handles.Alice, "dijkstra", "ROAD", false),
    ).numRows,
    1,
  );
  assert.deepEqual(
    [
      ...tableFromIPC(
        forge.paths(handles.Alice, handles.Dan, "dijkstra", "ROAD"),
      ).getChild("cost"),
    ],
    [1],
  );
  const allPairs = tableFromIPC(
    forge.paths(
      handles.Eve,
      undefined,
      "dijkstra_all_pairs",
      undefined,
      true,
      1,
      "cost",
    ),
  );
  assert.deepEqual(
    allPairs.schema.fields.map((field) => field.name),
    ["source_uuid", "target_uuid", "cost", "path"],
  );
  assert.equal(
    allPairs.schema.metadata.get("graphforge.algorithm"),
    "dijkstra_all_pairs",
  );
  assert.ok(allPairs.schema.fields.every((field) => !field.nullable));
  assert.deepEqual(Array.from(allPairs.getChild("source_uuid"), uuidHex), [
    uuids[0],
    uuids[0],
    uuids[0],
    uuids[0],
    uuids[1],
    uuids[1],
    uuids[2],
    uuids[2],
    uuids[3],
  ]);
  assert.deepEqual(Array.from(allPairs.getChild("target_uuid"), uuidHex), [
    uuids[1],
    uuids[2],
    uuids[3],
    uuids[4],
    uuids[3],
    uuids[4],
    uuids[3],
    uuids[4],
    uuids[4],
  ]);
  assert.deepEqual(
    [...allPairs.getChild("cost").toArray()],
    [1, 1, 3, 3.5, 2, 2.5, 2, 2.5, 0.5],
  );
  assert.deepEqual(pathHex(allPairs, 2), [uuids[0], uuids[1], uuids[3]]);
  assert.deepEqual(
    pathHex(
      tableFromIPC(
        forge.paths(
          handles.Alice.uuid,
          undefined,
          "dijkstra_all_pairs",
          undefined,
          true,
          1,
          "cost",
        ),
      ),
      2,
    ),
    pathHex(allPairs, 2),
  );
  assert.equal(
    tableFromIPC(
      forge.paths(
        handles.Alice,
        undefined,
        "dijkstra_all_pairs",
        "ROAD",
        true,
        1,
        "cost",
      ),
    ).numRows,
    5,
  );
  assert.equal(
    tableFromIPC(
      forge.paths(
        handles.Alice,
        undefined,
        "dijkstra_all_pairs",
        undefined,
        false,
      ),
    ).numRows,
    20,
  );
  assert.deepEqual(
    [
      ...tableFromIPC(
        forge.paths(handles.Alice, undefined, "dijkstra_all_pairs", "ROAD"),
      ).getChild("cost"),
    ],
    [1, 1, 1, 1, 1],
  );
  const isolated = new GraphForge();
  const isolatedSource = isolated.addNode("Person", { name: "Solo" });
  const emptyPairs = tableFromIPC(
    isolated.paths(isolatedSource, undefined, "dijkstra_all_pairs"),
  );
  assert.equal(emptyPairs.numRows, 0);
  assert.deepEqual(
    emptyPairs.schema.fields.map((field) => field.name),
    allPairs.schema.fields.map((field) => field.name),
  );
  assert.throws(
    () => forge.paths(handles.Alice, handles.Dan, "dijkstra_all_pairs"),
    (error) => error.code === "ValidationError",
  );
  for (const [args, expectedCode] of [
    [["ROAD", true, 2], "ValidationError"],
    [[" ", true, 1], "ValidationError"],
    [["ROAD", true, 1, " "], "ValidationError"],
    [["ROAD", true, 1, "missing"], "ValidationError"],
    [["ROAD", true, 1, "bad"], "ValidationError"],
    [["ROAD", true, 1, "negative"], "ExecutionError"],
  ]) {
    assert.throws(
      () => forge.paths(handles.Alice, handles.Dan, "dijkstra", ...args),
      (error) => error.code === expectedCode,
    );
    assert.throws(
      () =>
        forge.paths(handles.Alice, undefined, "dijkstra_all_pairs", ...args),
      (error) => error.code === expectedCode,
    );
  }
  assert.throws(
    () =>
      forge.paths(
        "00000000-0000-0000-0000-000000000000",
        undefined,
        "dijkstra_all_pairs",
      ),
    (error) => error.code === "ValidationError",
  );
}

function checkAStarPaths() {
  // #1684 — napi forwards the heuristic selector to Rust without local execution.
  const forge = new GraphForge();
  const estimates = { Alice: 3, Bob: 2, Carol: 2, Dan: 0, Eve: 8 };
  const handles = Object.fromEntries(
    Object.entries(estimates).map(([name, heuristic]) => [
      name,
      forge.addNode("Person", { name, heuristic }),
    ]),
  );
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}), (e:Person {name:'Eve'}) " +
      "CREATE (a)-[:ROAD {cost:1.0}]->(c), (a)-[:ROAD {cost:1.0}]->(b), " +
      "(b)-[:ROAD {cost:2.0}]->(d), (c)-[:ROAD {cost:2.0}]->(d), " +
      "(a)-[:ROAD {cost:9.0}]->(d), (a)-[:UNIT]->(b), (b)-[:UNIT]->(e)",
  );
  const identities = tableFromIPC(
    forge.execute(
      "MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name",
    ),
  );
  const uuids = Array.from(identities.getChild("uuid"), uuidHex);
  const table = tableFromIPC(
    forge.paths(
      handles.Alice,
      handles.Dan,
      "astar",
      "ROAD",
      true,
      1,
      "cost",
      "heuristic",
    ),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["source_uuid", "target_uuid", "cost", "path"],
  );
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "astar");
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(
    String(table.schema.fields[3].type),
    "List<FixedSizeBinary[16]>",
  );
  assert.deepEqual([...table.getChild("cost").toArray()], [3]);
  assert.deepEqual(pathHex(table, 0), [uuids[0], uuids[1], uuids[3]]);
  const repeated = tableFromIPC(
    forge.paths(
      handles.Alice.uuid,
      { label: "Person", property: "name", value: "Dan" },
      "astar",
      "ROAD",
      true,
      1,
      "cost",
      "heuristic",
    ),
  );
  assert.deepEqual(pathHex(repeated, 0), pathHex(table, 0));
  const zero = tableFromIPC(
    forge.paths(handles.Alice, handles.Dan, "astar", "ROAD", true, 1, "cost"),
  );
  assert.deepEqual(pathHex(zero, 0), pathHex(table, 0));
  const unit = tableFromIPC(
    forge.paths(handles.Alice, handles.Eve, "astar", "UNIT"),
  );
  assert.deepEqual([...unit.getChild("cost").toArray()], [2]);
  assert.deepEqual(pathHex(unit, 0), [uuids[0], uuids[1], uuids[4]]);
  assert.equal(
    tableFromIPC(
      forge.paths(handles.Dan, handles.Alice, "astar", "ROAD", true, 1, "cost"),
    ).numRows,
    0,
  );
  assert.equal(
    tableFromIPC(
      forge.paths(
        handles.Dan,
        handles.Alice,
        "astar",
        "ROAD",
        false,
        1,
        "cost",
      ),
    ).numRows,
    1,
  );
  const singleton = tableFromIPC(
    forge.paths(
      handles.Dan,
      handles.Dan,
      "astar",
      undefined,
      true,
      1,
      undefined,
      "heuristic",
    ),
  );
  assert.deepEqual([...singleton.getChild("cost").toArray()], [0]);

  const expectCode = (name, code, call) => {
    assert.throws(call, (error) => {
      assert.equal(error.code, code, `${name}: got code=${error.code}`);
      return true;
    });
  };
  expectCode("missing target", "ValidationError", () =>
    forge.paths(handles.Alice, undefined, "astar"),
  );
  expectCode("invalid k", "ValidationError", () =>
    forge.paths(handles.Alice, handles.Dan, "astar", undefined, undefined, 2),
  );
  expectCode("invalid heuristic selector", "ValidationError", () =>
    forge.paths(
      handles.Alice,
      handles.Dan,
      "astar",
      undefined,
      undefined,
      undefined,
      undefined,
      " ",
    ),
  );
  expectCode("missing heuristic", "ValidationError", () =>
    forge.paths(
      handles.Alice,
      handles.Dan,
      "astar",
      undefined,
      undefined,
      undefined,
      undefined,
      "missing",
    ),
  );
  expectCode("missing weight", "ValidationError", () =>
    forge.paths(
      handles.Alice,
      handles.Dan,
      "astar",
      "ROAD",
      true,
      1,
      "missing",
    ),
  );

  const invalidTarget = new GraphForge();
  const badSource = invalidTarget.addNode("Person", { heuristic: 1 });
  const badTarget = invalidTarget.addNode("Person", { heuristic: 1 });
  expectCode("target heuristic", "ExecutionError", () =>
    invalidTarget.paths(
      badSource,
      badTarget,
      "astar",
      undefined,
      undefined,
      undefined,
      undefined,
      "heuristic",
    ),
  );
  const nonNumeric = new GraphForge();
  const textSource = nonNumeric.addNode("Person", { heuristic: "near" });
  const zeroTarget = nonNumeric.addNode("Person", { heuristic: 0 });
  expectCode("non-numeric heuristic", "ValidationError", () =>
    nonNumeric.paths(
      textSource,
      zeroTarget,
      "astar",
      undefined,
      undefined,
      undefined,
      undefined,
      "heuristic",
    ),
  );
  for (const [name, value, code] of [
    ["null heuristic", null, "ValidationError"],
    ["negative heuristic", -1, "ExecutionError"],
  ]) {
    const invalidHeuristic = new GraphForge();
    const source = invalidHeuristic.addNode("Person", { heuristic: value });
    const target = invalidHeuristic.addNode("Person", { heuristic: 0 });
    expectCode(name, code, () =>
      invalidHeuristic.paths(
        source,
        target,
        "astar",
        undefined,
        undefined,
        undefined,
        undefined,
        "heuristic",
      ),
    );
  }
  assert.throws(
    () => new GraphForge().addNode("Person", { heuristic: Number.NaN }),
    (error) =>
      error.code === "ValidationError" && /non-finite node property/.test(error.message),
  );

  for (const [name, literal, code] of [
    ["null weight", "null", "ValidationError"],
    ["non-numeric weight", "'heavy'", "ValidationError"],
    ["negative weight", "-1.0", "ExecutionError"],
    ["non-finite weight", "1e308 * 2.0", "ValidationError"],
  ]) {
    const invalidWeight = new GraphForge();
    const source = invalidWeight.addNode("Person", { name: "source" });
    const target = invalidWeight.addNode("Person", { name: "target" });
    invalidWeight.execute(
      "MATCH (s:Person {name:'source'}), (t:Person {name:'target'}) " +
        `CREATE (s)-[:ROAD {cost:${literal}}]->(t)`,
    );
    expectCode(name, code, () =>
      invalidWeight.paths(source, target, "astar", "ROAD", true, 1, "cost"),
    );
  }
}

test("dijkstra paths", checkDijkstraPaths);
test("a star paths", checkAStarPaths);
