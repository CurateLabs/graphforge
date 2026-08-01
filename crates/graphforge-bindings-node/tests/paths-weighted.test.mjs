// Native acceptance for this coherent algorithm family.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";
import { pathHex, uuidHex } from "../lib/helpers.mjs";

function checkBellmanFordPaths() {
  // #1692 — both native bindings delegate negative-weight paths to Rust.
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
      "CREATE (a)-[:ROAD {cost:5.0}]->(c), (a)-[:ROAD {cost:4.0}]->(b), " +
      "(b)-[:ROAD {cost:-2.0}]->(c), (b)-[:ROAD {cost:6.0}]->(d), " +
      "(c)-[:ROAD {cost:3.0}]->(d), (d)-[:ROAD {cost:-1.0}]->(e), " +
      "(a)-[:UNIT]->(b), (b)-[:UNIT]->(e), (d)-[:BACK]->(a)",
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
      undefined,
      "bellman_ford",
      "ROAD",
      true,
      1,
      "cost",
    ),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["source_uuid", "target_uuid", "cost", "path"],
  );
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "bellman_ford",
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "FixedSizeBinary[16]");
  assert.equal(
    String(table.schema.fields[3].type),
    "List<FixedSizeBinary[16]>",
  );
  assert.deepEqual(
    Array.from(table.getChild("source_uuid"), uuidHex),
    Array(5).fill(uuids[0]),
  );
  assert.deepEqual(Array.from(table.getChild("target_uuid"), uuidHex), uuids);
  assert.deepEqual([...table.getChild("cost").toArray()], [0, 4, 2, 5, 4]);
  assert.deepEqual(pathHex(table, 4), uuids);
  const repeated = tableFromIPC(
    forge.paths(
      handles.Alice.uuid,
      { label: "Person", property: "name", value: "Eve" },
      "bellman_ford",
      "ROAD",
      true,
      1,
      "cost",
    ),
  );
  assert.deepEqual(pathHex(repeated, 0), pathHex(table, 4));
  const unit = tableFromIPC(
    forge.paths(handles.Alice, handles.Eve, "bellman_ford", "UNIT"),
  );
  assert.deepEqual([...unit.getChild("cost").toArray()], [2]);
  assert.equal(
    tableFromIPC(
      forge.paths(handles.Alice, handles.Dan, "bellman_ford", "BACK"),
    ).numRows,
    0,
  );
  assert.equal(
    tableFromIPC(
      forge.paths(handles.Alice, handles.Dan, "bellman_ford", "BACK", false),
    ).numRows,
    1,
  );
  const singleton = tableFromIPC(
    forge.paths(
      handles.Alice,
      handles.Alice,
      "bellman_ford",
      "ROAD",
      true,
      1,
      "cost",
    ),
  );
  assert.deepEqual([...singleton.getChild("cost").toArray()], [0]);

  const tie = new GraphForge();
  const tieHandles = Object.fromEntries(
    ["source", "alpha", "beta", "target", "isolated"].map((name) => [
      name,
      tie.addNode("Person", { name }),
    ]),
  );
  tie.execute(
    "MATCH (s:Person {name:'source'}), (a:Person {name:'alpha'}), " +
      "(b:Person {name:'beta'}), (t:Person {name:'target'}) " +
      "CREATE (s)-[:ROAD {cost:1.0}]->(a), (a)-[:ROAD {cost:1.0}]->(t), " +
      "(s)-[:ROAD {cost:1.0}]->(b), (b)-[:ROAD {cost:1.0}]->(t)",
  );
  const tieIdentities = tableFromIPC(
    tie.execute("MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name"),
  );
  const tieUuids = Object.fromEntries(
    ["alpha", "beta", "isolated", "source", "target"].map((name, index) => [
      name,
      uuidHex(tieIdentities.getChild("uuid").get(index)),
    ]),
  );
  const tied = tableFromIPC(
    tie.paths(
      tieHandles.source,
      undefined,
      "bellman_ford",
      "ROAD",
      true,
      1,
      "cost",
    ),
  );
  const tiedTargets = Array.from(tied.getChild("target_uuid"), uuidHex);
  assert.deepEqual(
    tiedTargets,
    [tieUuids.source, tieUuids.alpha, tieUuids.beta, tieUuids.target].sort(),
  );
  assert.ok(!tiedTargets.includes(tieUuids.isolated));
  assert.deepEqual(pathHex(tied, tiedTargets.indexOf(tieUuids.target)), [
    tieUuids.source,
    tieUuids.alpha,
    tieUuids.target,
  ]);

  const expectCode = (name, code, message, call) => {
    assert.throws(call, (error) => {
      assert.equal(error.code, code, `${name}: got code=${error.code}`);
      assert.equal(
        error.message,
        message,
        `${name}: got message=${error.message}`,
      );
      return true;
    });
  };
  const cycle = new GraphForge();
  const source = cycle.addNode("Person", { name: "source" });
  const target = cycle.addNode("Person", { name: "target" });
  cycle.execute(
    "MATCH (s:Person {name:'source'}), (t:Person {name:'target'}) " +
      "CREATE (s)-[:ROAD {cost:-1.0}]->(t), (t)-[:ROAD {cost:0.0}]->(s)",
  );
  expectCode(
    "negative cycle",
    "ExecutionError",
    "Rust algorithm execution failed: bellman_ford found a negative cycle reachable from the source",
    () => cycle.paths(source, target, "bellman_ford", "ROAD", true, 1, "cost"),
  );
  expectCode("invalid k", "ValidationError", "bellman_ford k must be 1", () =>
    forge.paths(
      handles.Alice,
      undefined,
      "bellman_ford",
      undefined,
      undefined,
      2,
    ),
  );
  expectCode(
    "invalid heuristic",
    "ValidationError",
    "bellman_ford does not accept a heuristic property",
    () =>
      forge.paths(
        handles.Alice,
        undefined,
        "bellman_ford",
        undefined,
        undefined,
        undefined,
        undefined,
        "heuristic",
      ),
  );
  expectCode(
    "invalid via",
    "ValidationError",
    'invalid paths relationship selector " "',
    () => forge.paths(handles.Alice, undefined, "bellman_ford", " "),
  );
  expectCode(
    "invalid weight selector",
    "ValidationError",
    'invalid paths weight property " "',
    () =>
      forge.paths(
        handles.Alice,
        undefined,
        "bellman_ford",
        undefined,
        undefined,
        undefined,
        " ",
      ),
  );
  expectCode(
    "missing weight",
    "ValidationError",
    'edge weight property "missing" does not exist',
    () =>
      forge.paths(
        handles.Alice,
        undefined,
        "bellman_ford",
        undefined,
        undefined,
        undefined,
        "missing",
      ),
  );
  for (const [name, literal, fixedMessage] of [
    ["null weight", "null", undefined],
    [
      "non-numeric weight",
      "'heavy'",
      'edge weight property "cost" must be numeric',
    ],
    ["non-finite weight", "1e308 * 2.0", undefined],
  ]) {
    const invalidWeight = new GraphForge();
    const invalidSource = invalidWeight.addNode("Person", { name: "source" });
    const invalidTarget = invalidWeight.addNode("Person", { name: "target" });
    invalidWeight.execute(
      "MATCH (s:Person {name:'source'}), (t:Person {name:'target'}) " +
        `CREATE (s)-[:ROAD {cost:${literal}}]->(t)`,
    );
    let message = fixedMessage;
    if (message === undefined) {
      const edgeTable = tableFromIPC(
        invalidWeight.execute(
          "MATCH ()-[r:ROAD]->() RETURN r.edge_uuid AS uuid",
        ),
      );
      const edgeUuid = uuidHex(edgeTable.getChild("uuid").get(0));
      message =
        "edge weight is missing, NULL, NaN, or infinite for edge " +
        edgeUuid.replace(/^(.{8})(.{4})(.{4})(.{4})(.{12})$/, "$1-$2-$3-$4-$5");
    }
    expectCode(name, "ValidationError", message, () =>
      invalidWeight.paths(
        invalidSource,
        invalidTarget,
        "bellman_ford",
        "ROAD",
        true,
        1,
        "cost",
      ),
    );
  }
}

function checkDeltaSteppingPaths() {
  // #1707 — the native addon only adapts arguments and Rust Arrow IPC.
  const forge = new GraphForge();
  const handles = Object.fromEntries(
    ["Alice", "Bob", "Carol", "Dan", "Eve"].map((name) => [
      name,
      forge.addNode("Person", { name }),
    ]),
  );
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}) " +
      "CREATE (a)-[:ROAD {cost:1.0}]->(c), (a)-[:ROAD {cost:0.5}]->(b), " +
      "(b)-[:ROAD {cost:0.5}]->(c), (a)-[:ROAD {cost:5.0}]->(d), " +
      "(c)-[:ROAD {cost:2.0}]->(d), (a)-[:UNIT]->(b), " +
      "(b)-[:UNIT]->(d), (d)-[:BACK]->(a)",
  );
  const identities = tableFromIPC(
    forge.execute(
      "MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name",
    ),
  );
  const uuids = Object.fromEntries(
    ["Alice", "Bob", "Carol", "Dan", "Eve"].map((name, index) => [
      name,
      uuidHex(identities.getChild("uuid").get(index)),
    ]),
  );
  const table = tableFromIPC(
    forge.paths(
      handles.Alice,
      undefined,
      "delta_stepping",
      "ROAD",
      true,
      1,
      "cost",
    ),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["source_uuid", "target_uuid", "cost", "path"],
  );
  assert.ok(table.schema.fields.every((field) => !field.nullable));
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "delta_stepping",
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(
    String(table.schema.fields[3].type),
    "List<FixedSizeBinary[16]>",
  );
  assert.deepEqual(
    Array.from(table.getChild("source_uuid"), uuidHex),
    Array(4).fill(uuids.Alice),
  );
  assert.deepEqual(Array.from(table.getChild("target_uuid"), uuidHex), [
    uuids.Alice,
    uuids.Bob,
    uuids.Carol,
    uuids.Dan,
  ]);
  assert.deepEqual([...table.getChild("cost").toArray()], [0, 0.5, 1, 3]);
  assert.deepEqual(pathHex(table, 2), [uuids.Alice, uuids.Bob, uuids.Carol]);
  assert.deepEqual(pathHex(table, 3), [
    uuids.Alice,
    uuids.Bob,
    uuids.Carol,
    uuids.Dan,
  ]);
  assert.ok(
    !Array.from(table.getChild("target_uuid"), uuidHex).includes(uuids.Eve),
  );

  const targeted = tableFromIPC(
    forge.paths(
      handles.Alice.uuid,
      { label: "Person", property: "name", value: "Dan" },
      "delta_stepping",
      "ROAD",
      true,
      1,
      "cost",
    ),
  );
  assert.deepEqual([...targeted.getChild("cost").toArray()], [3]);
  assert.deepEqual(pathHex(targeted, 0), pathHex(table, 3));
  const unit = tableFromIPC(
    forge.paths(handles.Alice, handles.Dan, "delta_stepping", "UNIT"),
  );
  assert.deepEqual([...unit.getChild("cost").toArray()], [2]);
  assert.deepEqual(pathHex(unit, 0), [uuids.Alice, uuids.Bob, uuids.Dan]);
  assert.equal(
    tableFromIPC(
      forge.paths(handles.Alice, handles.Eve, "delta_stepping", "ROAD"),
    ).numRows,
    0,
  );
  assert.equal(
    tableFromIPC(
      forge.paths(handles.Alice, handles.Dan, "delta_stepping", "BACK"),
    ).numRows,
    0,
  );
  assert.equal(
    tableFromIPC(
      forge.paths(handles.Alice, handles.Dan, "delta_stepping", "BACK", false),
    ).numRows,
    1,
  );
  const singleton = tableFromIPC(
    forge.paths(
      handles.Alice,
      handles.Alice,
      "delta_stepping",
      "ROAD",
      true,
      1,
      "cost",
    ),
  );
  assert.deepEqual([...singleton.getChild("cost").toArray()], [0]);
  assert.deepEqual(pathHex(singleton, 0), [uuids.Alice]);

  const tie = new GraphForge();
  const tieHandles = Object.fromEntries(
    ["source", "alpha", "beta", "target", "isolated"].map((name) => [
      name,
      tie.addNode("Person", { name }),
    ]),
  );
  tie.execute(
    "MATCH (s:Person {name:'source'}), (a:Person {name:'alpha'}), " +
      "(b:Person {name:'beta'}), (t:Person {name:'target'}) " +
      "CREATE (s)-[:ROAD {cost:1.0}]->(a), (a)-[:ROAD {cost:1.0}]->(t), " +
      "(s)-[:ROAD {cost:1.0}]->(b), (b)-[:ROAD {cost:1.0}]->(t)",
  );
  const tieIdentities = tableFromIPC(
    tie.execute("MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name"),
  );
  const tieUuids = Object.fromEntries(
    ["alpha", "beta", "isolated", "source", "target"].map((name, index) => [
      name,
      uuidHex(tieIdentities.getChild("uuid").get(index)),
    ]),
  );
  const tied = tableFromIPC(
    tie.paths(
      tieHandles.source,
      undefined,
      "delta_stepping",
      "ROAD",
      true,
      1,
      "cost",
    ),
  );
  const tiedTargets = Array.from(tied.getChild("target_uuid"), uuidHex);
  assert.deepEqual(
    tiedTargets,
    [tieUuids.source, tieUuids.alpha, tieUuids.beta, tieUuids.target].sort(),
  );
  assert.ok(!tiedTargets.includes(tieUuids.isolated));
  assert.deepEqual(pathHex(tied, tiedTargets.indexOf(tieUuids.target)), [
    tieUuids.source,
    tieUuids.alpha,
    tieUuids.target,
  ]);

  const expectCode = (name, code, message, call) => {
    assert.throws(call, (error) => {
      assert.equal(error.code, code, `${name}: got code=${error.code}`);
      assert.equal(
        error.message,
        message,
        `${name}: got message=${error.message}`,
      );
      return true;
    });
  };
  expectCode("invalid k", "ValidationError", "delta_stepping k must be 1", () =>
    forge.paths(
      handles.Alice,
      undefined,
      "delta_stepping",
      undefined,
      undefined,
      2,
    ),
  );
  expectCode(
    "invalid heuristic",
    "ValidationError",
    "delta_stepping does not accept a heuristic property",
    () =>
      forge.paths(
        handles.Alice,
        undefined,
        "delta_stepping",
        undefined,
        undefined,
        undefined,
        undefined,
        "heuristic",
      ),
  );
  expectCode(
    "invalid via",
    "ValidationError",
    'invalid paths relationship selector " "',
    () => forge.paths(handles.Alice, undefined, "delta_stepping", " "),
  );
  expectCode(
    "invalid weight selector",
    "ValidationError",
    'invalid paths weight property " "',
    () =>
      forge.paths(
        handles.Alice,
        undefined,
        "delta_stepping",
        undefined,
        undefined,
        undefined,
        " ",
      ),
  );
  expectCode(
    "missing weight",
    "ValidationError",
    'edge weight property "missing" does not exist',
    () =>
      forge.paths(
        handles.Alice,
        undefined,
        "delta_stepping",
        undefined,
        undefined,
        undefined,
        "missing",
      ),
  );
  for (const [name, literal, fixedCode, fixedMessage] of [
    ["null weight", "null", "ValidationError", undefined],
    [
      "non-numeric weight",
      "'heavy'",
      "ValidationError",
      'edge weight property "cost" must be numeric',
    ],
    ["non-finite weight", "1e308 * 2.0", "ValidationError", undefined],
    [
      "negative weight",
      "-1.0",
      "ExecutionError",
      "Rust algorithm execution failed: delta_stepping requires finite non-negative edge weights",
    ],
  ]) {
    const invalidWeight = new GraphForge();
    const invalidSource = invalidWeight.addNode("Person", { name: "source" });
    const invalidTarget = invalidWeight.addNode("Person", { name: "target" });
    invalidWeight.execute(
      "MATCH (s:Person {name:'source'}), (t:Person {name:'target'}) " +
        `CREATE (s)-[:ROAD {cost:${literal}}]->(t)`,
    );
    let message = fixedMessage;
    if (message === undefined) {
      const edgeTable = tableFromIPC(
        invalidWeight.execute(
          "MATCH ()-[r:ROAD]->() RETURN r.edge_uuid AS uuid",
        ),
      );
      const edgeUuid = uuidHex(edgeTable.getChild("uuid").get(0));
      message =
        "edge weight is missing, NULL, NaN, or infinite for edge " +
        edgeUuid.replace(/^(.{8})(.{4})(.{4})(.{4})(.{12})$/, "$1-$2-$3-$4-$5");
    }
    expectCode(name, fixedCode, message, () =>
      invalidWeight.paths(
        invalidSource,
        invalidTarget,
        "delta_stepping",
        "ROAD",
        true,
        1,
        "cost",
      ),
    );
  }
}

function checkFloydWarshallPaths() {
  // #1702 — the native addon forwards all options and decodes Rust Arrow IPC.
  const forge = new GraphForge();
  const handles = Object.fromEntries(
    ["Alice", "Bob", "Carol", "Dan", "Eve"].map((name) => [
      name,
      forge.addNode("Person", { name }),
    ]),
  );
  forge.execute(
    "MATCH (a:Person {name:'Alice'}), (b:Person {name:'Bob'}), " +
      "(c:Person {name:'Carol'}), (d:Person {name:'Dan'}) " +
      "CREATE (a)-[:ROAD {cost:4.0}]->(b), (a)-[:ROAD {cost:5.0}]->(c), " +
      "(b)-[:ROAD {cost:-2.0}]->(c), (c)-[:ROAD {cost:3.0}]->(d), " +
      "(a)-[:UNIT]->(d)",
  );
  const hex = (value) => Buffer.from(value).toString("hex");
  const uuidRows = tableFromIPC(
    forge.execute(
      "MATCH (p:Person) RETURN p.node_uuid AS uuid ORDER BY p.name",
    ),
  );
  const uuids = Object.fromEntries(
    ["Alice", "Bob", "Carol", "Dan", "Eve"].map((name, index) => [
      name,
      hex(uuidRows.getChild("uuid").get(index)),
    ]),
  );
  const table = tableFromIPC(
    forge.paths(
      handles.Eve,
      undefined,
      "floyd_warshall",
      "ROAD",
      true,
      1,
      "cost",
    ),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["source_uuid", "target_uuid", "cost", "path"],
  );
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "floyd_warshall",
  );
  assert.ok(table.schema.fields.every((field) => !field.nullable));
  assert.deepEqual(Array.from(table.getChild("source_uuid"), hex), [
    uuids.Alice,
    uuids.Alice,
    uuids.Alice,
    uuids.Bob,
    uuids.Bob,
    uuids.Carol,
  ]);
  assert.deepEqual(Array.from(table.getChild("target_uuid"), hex), [
    uuids.Bob,
    uuids.Carol,
    uuids.Dan,
    uuids.Carol,
    uuids.Dan,
    uuids.Dan,
  ]);
  assert.deepEqual([...table.getChild("cost").toArray()], [4, 2, 5, -2, 1, 3]);
  assert.deepEqual(Array.from(table.getChild("path").get(2), hex), [
    uuids.Alice,
    uuids.Bob,
    uuids.Carol,
    uuids.Dan,
  ]);
  assert.ok(
    !Array.from(table.getChild("source_uuid"), hex).includes(uuids.Eve),
  );
  const repeated = tableFromIPC(
    forge.paths(
      handles.Alice.uuid,
      undefined,
      "floyd_warshall",
      "ROAD",
      true,
      1,
      "cost",
    ),
  );
  assert.deepEqual(Array.from(repeated.getChild("path").get(2), hex), [
    uuids.Alice,
    uuids.Bob,
    uuids.Carol,
    uuids.Dan,
  ]);
  assert.deepEqual(
    [
      ...tableFromIPC(
        forge.paths(handles.Alice, undefined, "floyd_warshall", "UNIT"),
      ).getChild("cost"),
    ],
    [1],
  );

  const expectCode = (code, message, call) => {
    assert.throws(
      call,
      (error) => error.code === code && error.message === message,
    );
  };
  expectCode(
    "ValidationError",
    "floyd_warshall does not accept a target selector",
    () => forge.paths(handles.Alice, handles.Dan, "floyd_warshall"),
  );
  expectCode("ValidationError", "floyd_warshall k must be 1", () =>
    forge.paths(handles.Alice, undefined, "floyd_warshall", undefined, true, 2),
  );
  expectCode(
    "ValidationError",
    "floyd_warshall does not accept a heuristic property",
    () =>
      forge.paths(
        handles.Alice,
        undefined,
        "floyd_warshall",
        undefined,
        true,
        1,
        undefined,
        "estimate",
      ),
  );
  assert.throws(
    () =>
      forge.paths(
        handles.Alice,
        undefined,
        "floyd_warshall",
        "ROAD",
        true,
        1,
        "missing",
      ),
    (error) => error.code === "ValidationError",
  );

  const cycle = new GraphForge();
  const source = cycle.addNode("Person", { name: "source" });
  cycle.addNode("Person", { name: "target" });
  cycle.execute(
    "MATCH (s:Person {name:'source'}), (t:Person {name:'target'}) " +
      "CREATE (s)-[:ROAD {cost:-2.0}]->(t), (t)-[:ROAD {cost:1.0}]->(s)",
  );
  expectCode(
    "ExecutionError",
    "Rust algorithm execution failed: floyd_warshall found a negative cycle in the selected graph",
    () =>
      cycle.paths(source, undefined, "floyd_warshall", "ROAD", true, 1, "cost"),
  );
}

test("bellman ford paths", checkBellmanFordPaths);
test("delta stepping paths", checkDeltaSteppingPaths);
test("floyd warshall paths", checkFloydWarshallPaths);
