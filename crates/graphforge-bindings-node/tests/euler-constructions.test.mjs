// Native Euler-construction acceptance against the freshly built addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

const uuidHex = (value) => Buffer.from(value).toString("hex");
const handleHex = (handle) => handle.uuid.replaceAll("-", "");

const analyze = (forge, algorithm, via, directed) =>
  tableFromIPC(forge.analyze(algorithm, "Person", via, directed));

const assertSchema = (table, algorithm) => {
  assert.deepEqual(
    table.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [
      ["node_path", "List<FixedSizeBinary[16]>", false],
      ["edge_path", "List<FixedSizeBinary[16]>", false],
    ],
  );
  assert.equal(table.schema.metadata.size, 3);
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), algorithm);
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm_schema_version"),
    "1",
  );
  assert.equal(table.schema.metadata.get("graphforge.verb"), "analyze");
  for (const field of table.schema.fields) {
    assert.equal(field.nullable, false);
    assert.equal(field.type.children.length, 1);
    assert.equal(field.type.children[0].nullable, false);
  }
  for (const forbidden of [
    "node_id",
    "edge_id",
    "provenance_id",
    "confidence",
    "assertion_uuid",
    "belief_status",
    "valid_time",
  ]) {
    assert.equal(table.getChild(forbidden), null);
  }
};

const paths = (table) => {
  assert.equal(table.numRows, 1);
  assert.equal(table.getChild("node_path").nullCount, 0);
  assert.equal(table.getChild("edge_path").nullCount, 0);
  return {
    nodes: Array.from(table.getChild("node_path").get(0), uuidHex),
    edges: Array.from(table.getChild("edge_path").get(0), uuidHex),
  };
};

const relationshipRows = (forge, via) => {
  const table = tableFromIPC(
    forge.execute(
      `MATCH (s:Person)-[r:${via}]->(t:Person) ` +
        "RETURN r.edge_uuid AS edge_uuid, " +
        "s.node_uuid AS source_uuid, t.node_uuid AS target_uuid",
    ),
  );
  return Array.from({ length: table.numRows }, (_, row) => ({
    edge: uuidHex(table.getChild("edge_uuid").get(row)),
    source: uuidHex(table.getChild("source_uuid").get(row)),
    target: uuidHex(table.getChild("target_uuid").get(row)),
  }));
};

const assertCoherentTrail = (trail, relationships, directed) => {
  assert.equal(trail.nodes.length, trail.edges.length + 1);
  assert.deepEqual(
    [...trail.edges].sort(),
    relationships.map(({ edge }) => edge).sort(),
  );
  assert.equal(new Set(trail.edges).size, trail.edges.length);
  const byEdge = new Map(relationships.map((row) => [row.edge, row]));
  for (let index = 0; index < trail.edges.length; index += 1) {
    const { source, target } = byEdge.get(trail.edges[index]);
    const from = trail.nodes[index];
    const to = trail.nodes[index + 1];
    assert.equal(
      directed
        ? from === source && to === target
        : (from === source && to === target) ||
            (from === target && to === source),
      true,
    );
  }
};

test("Euler circuit returns deterministic directed native UUID paths", () => {
  const forge = new GraphForge();
  const nodes = ["A", "B"].map((name) => forge.addNode("Person", { name }));
  forge.execute(
    "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) " +
      "CREATE (a)-[:ARC]->(b), (b)-[:ARC]->(a), (a)-[:ARC]->(a)",
  );
  const run = () => analyze(forge, "euler_circuit", "ARC", true);
  const table = run();
  assertSchema(table, "euler_circuit");
  const trail = paths(table);
  assert.deepEqual(paths(run()), trail);
  assert.equal(trail.nodes[0], trail.nodes.at(-1));
  assert.equal(
    trail.nodes[0],
    [handleHex(nodes[0]), handleHex(nodes[1])].sort()[0],
  );
  assertCoherentTrail(trail, relationshipRows(forge, "ARC"), true);
});

test("Euler circuit preserves undirected loops and parallel edge UUIDs", () => {
  const forge = new GraphForge();
  for (const name of ["A", "B"]) forge.addNode("Person", { name });
  forge.execute(
    "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}) " +
      "CREATE (a)-[:TRAIL]->(b), (a)-[:TRAIL]->(b), (a)-[:TRAIL]->(a)",
  );
  const run = () => analyze(forge, "euler_circuit", "TRAIL", false);
  const table = run();
  assertSchema(table, "euler_circuit");
  const trail = paths(table);
  assert.deepEqual(paths(run()), trail);
  assert.equal(trail.nodes[0], trail.nodes.at(-1));
  assertCoherentTrail(trail, relationshipRows(forge, "TRAIL"), false);
});

test("Euler path uses the canonical open start in directed and undirected graphs", () => {
  for (const directed of [true, false]) {
    const forge = new GraphForge();
    const [a, , c] = ["A", "B", "C"].map((name) =>
      forge.addNode("Person", { name }),
    );
    forge.execute(
      "MATCH (a:Person {name:'A'}), (b:Person {name:'B'}), " +
        "(c:Person {name:'C'}) " +
        "CREATE (a)-[:TRAIL]->(b), (b)-[:TRAIL]->(b), (b)-[:TRAIL]->(c)",
    );
    const run = () => analyze(forge, "euler_path", "TRAIL", directed);
    const table = run();
    assertSchema(table, "euler_path");
    const trail = paths(table);
    assert.deepEqual(paths(run()), trail);
    assert.equal(
      trail.nodes[0],
      directed ? handleHex(a) : [handleHex(a), handleHex(c)].sort()[0],
    );
    assert.notEqual(trail.nodes[0], trail.nodes.at(-1));
    assertCoherentTrail(trail, relationshipRows(forge, "TRAIL"), directed);
  }
});

for (const algorithm of ["euler_circuit", "euler_path"]) {
  test(`${algorithm} returns typed empty and singleton edgeless results`, () => {
    const empty = analyze(new GraphForge(), algorithm, undefined, false);
    assertSchema(empty, algorithm);
    assert.equal(empty.numRows, 0);

    const forge = new GraphForge();
    const singleton = forge.addNode("Person", { name: "Only" });
    const table = analyze(forge, algorithm, undefined, false);
    assertSchema(table, algorithm);
    assert.deepEqual(paths(table), {
      nodes: [handleHex(singleton)],
      edges: [],
    });
  });
}

test("Euler circuit reports its leaf-specific structured undefined error", () => {
  const forge = new GraphForge();
  forge.execute("CREATE (:Person)-[:TRAIL]->(:Person)");
  assert.throws(
    () => analyze(forge, "euler_circuit", "TRAIL", false),
    (error) => {
      assert.equal(error.code, "ExecutionError");
      assert.match(
        error.message,
        /Euler circuit is undefined for the selected graph$/,
      );
      return true;
    },
  );
});

test("Euler path reports its leaf-specific structured undefined error", () => {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person), (b:Person), (c:Person), (d:Person) " +
      "CREATE (a)-[:TRAIL]->(b), (a)-[:TRAIL]->(c), (a)-[:TRAIL]->(d)",
  );
  assert.throws(
    () => analyze(forge, "euler_path", "TRAIL", false),
    (error) => {
      assert.equal(error.code, "ExecutionError");
      assert.match(
        error.message,
        /Euler path is undefined for the selected graph$/,
      );
      return true;
    },
  );
});
