// Native acceptance for this coherent algorithm family.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function checkComponentsCluster() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}), " +
      "(c:Person {name: 'Carol'}), (d:Person {name: 'Dan'}), (e:Person {name: 'Eve'}), " +
      "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(a), (c)-[:OTHER]->(d)",
  );
  const table = tableFromIPC(forge.cluster("Person", "components"));
  const uuidField = table.schema.fields.find(
    (field) => field.name === "node_uuid",
  );
  const communityField = table.schema.fields.find(
    (field) => field.name === "community_id",
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "name"],
  );
  assert.equal(String(uuidField?.type), "FixedSizeBinary[16]");
  assert.equal(uuidField?.type.byteWidth, 16);
  assert.equal(String(communityField?.type), "Int64");
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "components");
  assert.deepEqual(
    [...table.getChild("name").toArray()],
    ["Alice", "Bob", "Carol", "Dan", "Eve"],
  );
  assert.deepEqual(
    [...table.getChild("community_id").toArray()],
    [0n, 0n, 1n, 1n, 2n],
  );

  const via = tableFromIPC(
    forge.cluster("Person", "components", "KNOWS", true),
  );
  assert.deepEqual(
    [...via.getChild("community_id").toArray()],
    [0n, 0n, 1n, 2n, 3n],
  );
  const written = tableFromIPC(
    forge.cluster("Person", "components", "KNOWS", false, "component"),
  );
  assert.deepEqual(
    [...written.getChild("community_id").toArray()],
    [0n, 0n, 1n, 2n, 3n],
  );
  const readback = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) RETURN n.component AS component ORDER BY n.name",
    ),
  );
  assert.deepEqual(
    [...readback.getChild("component").toArray()],
    [0n, 0n, 1n, 2n, 3n],
  );
  const expectClusterValidation = (target, by, vectorProperty, message) => {
    assert.throws(
      () =>
        target.cluster(
          "Person",
          by,
          undefined,
          undefined,
          undefined,
          vectorProperty,
        ),
      (error) => {
        assert.equal(error.code, "ValidationError");
        assert.equal(error.message, message);
        return true;
      },
    );
  };
  expectClusterValidation(
    forge,
    "hdbscan",
    undefined,
    "cluster.hdbscan requires vector_property",
  );
  expectClusterValidation(
    forge,
    "hdbscan",
    " ",
    'invalid cluster vector property " "',
  );
  expectClusterValidation(
    forge,
    "components",
    "features",
    "cluster.components does not accept vector_property",
  );
}

function checkStronglyConnectedCluster() {
  assert.equal(
    tableFromIPC(new GraphForge().cluster("Person", "strongly_connected"))
      .numRows,
    0,
  );
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), " +
      "(c:Person {name:'c'}), (d:Person {name:'d'}), " +
      "(e:Person {name:'e'}), (f:Person {name:'f'}), (g:Person {name:'g'}), " +
      "(a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(b), " +
      "(b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), " +
      "(d)-[:KNOWS]->(e), (e)-[:KNOWS]->(d), (e)-[:KNOWS]->(f), " +
      "(f)-[:OTHER]->(a)",
  );
  const table = tableFromIPC(
    forge.cluster("Person", "strongly_connected", "KNOWS", true),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(table.schema.fields[0].nullable, false);
  assert.equal(table.schema.fields[1].nullable, false);
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "strongly_connected",
  );
  assert.deepEqual([...table.getChild("name").toArray()], [..."abcdefg"]);
  assert.deepEqual(
    [...table.getChild("community_id").toArray()],
    [0n, 0n, 0n, 1n, 1n, 2n, 3n],
  );
  const expected = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) RETURN n.node_uuid AS node_uuid ORDER BY n.name",
    ),
  );
  assert.deepEqual(
    [...table.getChild("node_uuid").toArray()],
    [...expected.getChild("node_uuid").toArray()],
  );
  const undirected = tableFromIPC(
    forge.cluster("Person", "strongly_connected", "KNOWS", false),
  );
  assert.deepEqual(
    [...undirected.getChild("community_id").toArray()],
    [0n, 0n, 0n, 0n, 0n, 0n, 1n],
  );
  const allEdges = tableFromIPC(
    forge.cluster("Person", "strongly_connected", undefined, true),
  );
  assert.deepEqual(
    [...allEdges.getChild("community_id").toArray()],
    [0n, 0n, 0n, 0n, 0n, 0n, 1n],
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.scc IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    0,
  );

  forge.execute("MATCH (n:Person {name:'a'}) SET n.atomic_scc = 'old'");
  for (const [run, message] of [
    [
      () =>
        forge.cluster(
          "Person",
          "strongly_connected",
          "KNOWS",
          true,
          "atomic_scc",
        ),
      'write_property "atomic_scc" collides with existing Utf8 data',
    ],
    [
      () =>
        forge.cluster(
          "Person",
          "strongly_connected",
          undefined,
          undefined,
          undefined,
          "features",
        ),
      "cluster.strongly_connected does not accept vector_property",
    ],
  ]) {
    assert.throws(run, (error) => {
      assert.equal(error.code, "ValidationError");
      assert.ok(error.message.includes(message));
      return true;
    });
  }
  const unchanged = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) WHERE n.atomic_scc IS NOT NULL RETURN n.atomic_scc AS value",
    ),
  );
  assert.deepEqual([...unchanged.getChild("value").toArray()], ["old"]);
  const written = tableFromIPC(
    forge.cluster("Person", "strongly_connected", "KNOWS", true, "scc"),
  );
  assert.deepEqual(
    [...written.getChild("community_id").toArray()],
    [0n, 0n, 0n, 1n, 1n, 2n, 3n],
  );
  const persisted = tableFromIPC(
    forge.execute("MATCH (n:Person) RETURN n.scc AS scc ORDER BY n.name"),
  );
  assert.deepEqual(
    [...persisted.getChild("scc").toArray()],
    [0n, 0n, 0n, 1n, 1n, 2n, 3n],
  );
}

function checkBiconnectedCluster() {
  assert.equal(
    tableFromIPC(new GraphForge().cluster("Person", "biconnected")).numRows,
    0,
  );
  const edgeless = new GraphForge();
  edgeless.execute("CREATE (:Person {name:'a'}), (:Person {name:'b'})");
  assert.deepEqual(
    [
      ...tableFromIPC(edgeless.cluster("Person", "biconnected"))
        .getChild("community_id")
        .toArray(),
    ],
    [0n, 1n],
  );

  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), " +
      "(c:Person {name:'c'}), (d:Person {name:'d'}), " +
      "(e:Person {name:'e'}), (f:Person {name:'f'}), (g:Person {name:'g'}), " +
      "(a)-[:KNOWS {weight:99}]->(b), (a)-[:KNOWS]->(b), " +
      "(b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), " +
      "(d)-[:KNOWS]->(e), (e)-[:KNOWS]->(c), (e)-[:KNOWS]->(f), " +
      "(f)-[:KNOWS]->(f), (g)-[:OTHER]->(a)",
  );
  const table = tableFromIPC(
    forge.cluster("Person", "biconnected", "KNOWS", true),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(table.schema.fields[0].nullable, false);
  assert.equal(table.schema.fields[1].nullable, false);
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "biconnected",
  );
  assert.deepEqual([...table.getChild("name").toArray()], [..."abcdefg"]);
  assert.deepEqual(
    [...table.getChild("community_id").toArray()],
    [0n, 0n, 0n, 1n, 1n, 2n, 3n],
  );
  const expected = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) RETURN n.node_uuid AS node_uuid ORDER BY n.name",
    ),
  );
  assert.deepEqual(
    [...table.getChild("node_uuid").toArray()],
    [...expected.getChild("node_uuid").toArray()],
  );
  const undirected = tableFromIPC(
    forge.cluster("Person", "biconnected", "KNOWS", false),
  );
  assert.deepEqual(
    [...undirected.getChild("community_id").toArray()],
    [0n, 0n, 0n, 1n, 1n, 2n, 3n],
  );
  const via = tableFromIPC(forge.cluster("Person", "biconnected", "OTHER"));
  assert.deepEqual(
    [...via.getChild("community_id").toArray()],
    [0n, 1n, 2n, 3n, 4n, 5n, 0n],
  );
  assert.equal(
    tableFromIPC(
      forge.execute("MATCH (n:Person) WHERE n.block IS NOT NULL RETURN n"),
    ).numRows,
    0,
  );

  forge.execute("MATCH (n:Person {name:'a'}) SET n.atomic_block = 'old'");
  for (const [run, message] of [
    [
      () =>
        forge.cluster("Person", "biconnected", "KNOWS", true, "atomic_block"),
      'write_property "atomic_block" collides with existing Utf8 data',
    ],
    [
      () =>
        forge.cluster(
          "Person",
          "biconnected",
          undefined,
          undefined,
          undefined,
          "features",
        ),
      "cluster.biconnected does not accept vector_property",
    ],
  ]) {
    assert.throws(run, (error) => {
      assert.equal(error.code, "ValidationError");
      assert.ok(error.message.includes(message));
      return true;
    });
  }
  const unchanged = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) WHERE n.atomic_block IS NOT NULL RETURN n.atomic_block AS value",
    ),
  );
  assert.deepEqual([...unchanged.getChild("value").toArray()], ["old"]);
  const written = tableFromIPC(
    forge.cluster("Person", "biconnected", "KNOWS", true, "block"),
  );
  assert.deepEqual(
    [...written.getChild("community_id").toArray()],
    [0n, 0n, 0n, 1n, 1n, 2n, 3n],
  );
  const persisted = tableFromIPC(
    forge.execute("MATCH (n:Person) RETURN n.block AS block ORDER BY n.name"),
  );
  assert.deepEqual(
    [...persisted.getChild("block").toArray()],
    [0n, 0n, 0n, 1n, 1n, 2n, 3n],
  );
}

function checkKCoreDecompositionCluster() {
  assert.equal(
    tableFromIPC(new GraphForge().cluster("Person", "k_core_decomposition"))
      .numRows,
    0,
  );
  const edgeless = new GraphForge();
  edgeless.execute("CREATE (:Person {name:'a'}), (:Person {name:'b'})");
  assert.deepEqual(
    [
      ...tableFromIPC(edgeless.cluster("Person", "k_core_decomposition"))
        .getChild("community_id")
        .toArray(),
    ],
    [0n, 0n],
  );

  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'a'}), (b:Person {name:'b'}), " +
      "(c:Person {name:'c'}), (d:Person {name:'d'}), " +
      "(e:Person {name:'e'}), (f:Person {name:'f'}), " +
      "(g:Person {name:'g'}), (h:Person {name:'h'}), " +
      "(i:Person {name:'i'}), (j:Person {name:'j'}), " +
      "(a)-[:KNOWS {weight:99}]->(b), (a)-[:KNOWS]->(b), " +
      "(b)-[:KNOWS]->(a), (a)-[:KNOWS]->(c), (a)-[:KNOWS]->(d), " +
      "(b)-[:KNOWS]->(c), (b)-[:KNOWS]->(d), (c)-[:KNOWS]->(d), " +
      "(c)-[:KNOWS]->(c), (a)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), " +
      "(h)-[:KNOWS]->(i), (i)-[:KNOWS]->(j), (j)-[:KNOWS]->(h), " +
      "(f)-[:OTHER]->(a)",
  );
  const table = tableFromIPC(
    forge.cluster("Person", "k_core_decomposition", "KNOWS", true),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(table.schema.fields[0].nullable, false);
  assert.equal(table.schema.fields[1].nullable, false);
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "k_core_decomposition",
  );
  assert.deepEqual([...table.getChild("name").toArray()], [..."abcdefghij"]);
  const expectedCores = [3n, 3n, 3n, 3n, 1n, 1n, 0n, 2n, 2n, 2n];
  assert.deepEqual(
    [...table.getChild("community_id").toArray()],
    expectedCores,
  );
  const identities = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) RETURN n.node_uuid AS uuid ORDER BY n.name",
    ),
  );
  assert.deepEqual(
    [...table.getChild("node_uuid").toArray()],
    [...identities.getChild("uuid").toArray()],
  );
  const undirected = tableFromIPC(
    forge.cluster("Person", "k_core_decomposition", "KNOWS", false),
  );
  assert.deepEqual(
    [...undirected.getChild("community_id").toArray()],
    expectedCores,
  );
  const via = tableFromIPC(
    forge.cluster("Person", "k_core_decomposition", "OTHER"),
  );
  assert.deepEqual(
    [...via.getChild("community_id").toArray()],
    [1n, 0n, 0n, 0n, 0n, 1n, 0n, 0n, 0n, 0n],
  );
  assert.equal(
    tableFromIPC(
      forge.execute("MATCH (n:Person) WHERE n.core IS NOT NULL RETURN n"),
    ).numRows,
    0,
  );

  forge.execute("MATCH (n:Person {name:'a'}) SET n.atomic_core = 'old'");
  for (const run of [
    () =>
      forge.cluster(
        "Person",
        "k_core_decomposition",
        "KNOWS",
        true,
        "atomic_core",
      ),
    () =>
      forge.cluster(
        "Person",
        "k_core_decomposition",
        undefined,
        undefined,
        undefined,
        "features",
      ),
  ]) {
    assert.throws(run, (error) => error.code === "ValidationError");
  }
  const unchanged = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) WHERE n.atomic_core IS NOT NULL RETURN n.atomic_core AS value",
    ),
  );
  assert.deepEqual([...unchanged.getChild("value").toArray()], ["old"]);
  const written = tableFromIPC(
    forge.cluster("Person", "k_core_decomposition", "KNOWS", true, "core"),
  );
  assert.deepEqual(
    [...written.getChild("community_id").toArray()],
    expectedCores,
  );
  const persisted = tableFromIPC(
    forge.execute("MATCH (n:Person) RETURN n.core AS core ORDER BY n.name"),
  );
  assert.deepEqual([...persisted.getChild("core").toArray()], expectedCores);
}

test("components cluster", checkComponentsCluster);
test("strongly connected cluster", checkStronglyConnectedCluster);
test("biconnected cluster", checkBiconnectedCluster);
test("k core decomposition cluster", checkKCoreDecompositionCluster);
