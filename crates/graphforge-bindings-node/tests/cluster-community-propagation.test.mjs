// Native acceptance for this coherent algorithm family.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge } from "../index.js";

function checkLouvainCluster() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), " +
      "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), " +
      "(b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), " +
      "(a)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), " +
      "(e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d), (f)-[:KNOWS]->(f), " +
      "(a)-[:OTHER]->(g)",
  );
  const table = tableFromIPC(forge.cluster("Person", "louvain", "KNOWS", true));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "louvain");
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDEFG"]);
  assert.deepEqual(
    [...table.getChild("community_id").toArray()],
    [0n, 0n, 0n, 1n, 1n, 1n, 2n],
  );
  const undirected = tableFromIPC(
    forge.cluster("Person", "louvain", "KNOWS", false),
  );
  assert.deepEqual(
    [...undirected.getChild("community_id").toArray()],
    [...table.getChild("community_id").toArray()],
  );
  const allEdges = tableFromIPC(forge.cluster("Person", "louvain"));
  assert.deepEqual(
    [...allEdges.getChild("community_id").toArray()],
    [0n, 0n, 0n, 1n, 1n, 1n, 0n],
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.group_id IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    0,
  );
  const written = tableFromIPC(
    forge.cluster("Person", "louvain", "KNOWS", true, "group_id"),
  );
  assert.deepEqual(
    [...written.getChild("community_id").toArray()],
    [0n, 0n, 0n, 1n, 1n, 1n, 2n],
  );
  const persisted = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) RETURN n.group_id AS id ORDER BY id, n.name",
    ),
  );
  assert.deepEqual(
    [...persisted.getChild("id").toArray()],
    [0n, 0n, 0n, 1n, 1n, 1n, 2n],
  );
  assert.equal(
    tableFromIPC(new GraphForge().cluster("Person", "louvain")).numRows,
    0,
  );
}

function checkLeidenCluster() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), " +
      "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(g:Person {name:'G'}), (h:Person {name:'H'}), (a)-[:KNOWS]->(e), " +
      "(a)-[:KNOWS]->(e), (e)-[:KNOWS]->(a), (a)-[:KNOWS]->(g), " +
      "(b)-[:KNOWS]->(c), (b)-[:KNOWS]->(f), (b)-[:KNOWS]->(g), " +
      "(c)-[:KNOWS]->(g), (d)-[:KNOWS]->(g), (e)-[:KNOWS]->(g), " +
      "(f)-[:KNOWS]->(g), (a)-[:KNOWS]->(a), (a)-[:OTHER]->(h)",
  );
  const table = tableFromIPC(forge.cluster("Person", "leiden", "KNOWS", true));
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(table.schema.metadata.get("graphforge.algorithm"), "leiden");
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDEFGH"]);
  const expected = [0n, 1n, 1n, 0n, 0n, 1n, 0n, 2n];
  assert.deepEqual([...table.getChild("community_id").toArray()], expected);
  const undirected = tableFromIPC(
    forge.cluster("Person", "leiden", "KNOWS", false),
  );
  assert.deepEqual(
    [...undirected.getChild("community_id").toArray()],
    expected,
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.group_id IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    0,
  );
  const written = tableFromIPC(
    forge.cluster("Person", "leiden", "KNOWS", true, "group_id"),
  );
  assert.deepEqual([...written.getChild("community_id").toArray()], expected);
  const persisted = tableFromIPC(
    forge.execute("MATCH (n:Person) RETURN n.group_id AS id ORDER BY n.name"),
  );
  assert.deepEqual([...persisted.getChild("id").toArray()], expected);
  assert.equal(
    tableFromIPC(new GraphForge().cluster("Person", "leiden")).numRows,
    0,
  );
}

function checkLabelPropagationCluster() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), " +
      "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), " +
      "(b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), " +
      "(a)-[:KNOWS]->(a), (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), " +
      "(f)-[:KNOWS]->(d), (f)-[:KNOWS]->(f), (c)-[:OTHER]->(d)",
  );
  const table = tableFromIPC(
    forge.cluster("Person", "label_propagation", "KNOWS", true),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "label_propagation",
  );
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDEFG"]);
  const expected = [0n, 0n, 0n, 1n, 1n, 1n, 2n];
  assert.deepEqual([...table.getChild("community_id").toArray()], expected);
  const undirected = tableFromIPC(
    forge.cluster("Person", "label_propagation", "KNOWS", false),
  );
  assert.deepEqual(
    [...undirected.getChild("community_id").toArray()],
    expected,
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.group_id IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    0,
  );
  const written = tableFromIPC(
    forge.cluster("Person", "label_propagation", "KNOWS", true, "group_id"),
  );
  assert.deepEqual([...written.getChild("community_id").toArray()], expected);
  const persisted = tableFromIPC(
    forge.execute("MATCH (n:Person) RETURN n.group_id AS id ORDER BY n.name"),
  );
  assert.deepEqual([...persisted.getChild("id").toArray()], expected);
  assert.equal(
    tableFromIPC(new GraphForge().cluster("Person", "label_propagation"))
      .numRows,
    0,
  );
}

function checkSpeakerListenerCluster() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), " +
      "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (a)-[:KNOWS]->(b), " +
      "(b)-[:KNOWS]->(a), (b)-[:KNOWS]->(c), (c)-[:KNOWS]->(a), " +
      "(a)-[:KNOWS]->(a), (d)-[:KNOWS]->(e), (e)-[:KNOWS]->(f), " +
      "(f)-[:KNOWS]->(d), (f)-[:KNOWS]->(f), (c)-[:OTHER]->(d)",
  );
  const table = tableFromIPC(
    forge.cluster("Person", "speaker_listener", "KNOWS", true),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "speaker_listener",
  );
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDEFG"]);
  const expected = [0n, 0n, 0n, 1n, 1n, 1n, 2n];
  assert.deepEqual([...table.getChild("community_id").toArray()], expected);
  const undirected = tableFromIPC(
    forge.cluster("Person", "speaker_listener", "KNOWS", false),
  );
  assert.deepEqual(
    [...undirected.getChild("community_id").toArray()],
    expected,
  );
  assert.equal(
    tableFromIPC(
      forge.execute(
        "MATCH (n:Person) WHERE n.slpa_group IS NOT NULL RETURN n.node_uuid",
      ),
    ).numRows,
    0,
  );
  const written = tableFromIPC(
    forge.cluster("Person", "speaker_listener", "KNOWS", true, "slpa_group"),
  );
  assert.deepEqual([...written.getChild("community_id").toArray()], expected);
  const persisted = tableFromIPC(
    forge.execute("MATCH (n:Person) RETURN n.slpa_group AS id ORDER BY n.name"),
  );
  assert.deepEqual([...persisted.getChild("id").toArray()], expected);
  assert.equal(
    tableFromIPC(new GraphForge().cluster("Person", "speaker_listener"))
      .numRows,
    0,
  );
}

function checkGirvanNewmanCluster() {
  const forge = new GraphForge();
  forge.execute(
    "CREATE (a:Person {name:'A'}), (b:Person {name:'B'}), (c:Person {name:'C'}), " +
      "(d:Person {name:'D'}), (e:Person {name:'E'}), (f:Person {name:'F'}), " +
      "(g:Person {name:'G'}), (a)-[:KNOWS]->(b), (b)-[:KNOWS]->(c), " +
      "(c)-[:KNOWS]->(a), (c)-[:KNOWS]->(d), (d)-[:KNOWS]->(e), " +
      "(e)-[:KNOWS]->(f), (f)-[:KNOWS]->(d)",
  );
  const table = tableFromIPC(
    forge.cluster("Person", "girvan_newman", "KNOWS", true),
  );
  assert.deepEqual(
    table.schema.fields.map((field) => field.name),
    ["node_uuid", "community_id", "name"],
  );
  assert.equal(String(table.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(String(table.schema.fields[1].type), "Int64");
  assert.equal(
    table.schema.metadata.get("graphforge.algorithm"),
    "girvan_newman",
  );
  assert.deepEqual([...table.getChild("name").toArray()], [..."ABCDEFG"]);
  assert.deepEqual(
    [...table.getChild("community_id").toArray()],
    [0n, 0n, 0n, 1n, 1n, 1n, 2n],
  );
}

test("louvain cluster", checkLouvainCluster);
test("leiden cluster", checkLeidenCluster);
test("label propagation cluster", checkLabelPropagationCluster);
test("speaker listener cluster", checkSpeakerListenerCluster);
test("girvan newman cluster", checkGirvanNewmanCluster);
