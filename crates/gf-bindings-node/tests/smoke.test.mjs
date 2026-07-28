// Minimal clean-build acceptance for the freshly built native addon.

import assert from "node:assert/strict";
import { test } from "node:test";
import { tableFromIPC } from "apache-arrow";
import { GraphForge, version } from "../index.js";

function checkVersion() {
  const v = version();
  assert.ok(typeof v === "string" && v.length > 0, "missing version");
}

function checkConstruction() {
  const forge = new GraphForge();
  assert.equal(forge.ontologyMode, "exploratory", forge.ontologyMode);
  assert.equal(forge.path, null);
}

function checkConstructionError() {
  // A real fault (missing project dir) maps to err.code === "StorageError".
  try {
    new GraphForge("/no/such/dir/graphforge-node-smoke");
  } catch (e) {
    assert.equal(e.code, "StorageError", `got code=${e.code}`);
    return;
  }
  throw new Error("expected StorageError for a missing path");
}

function checkExecute() {
  // #592 — execute returns an Arrow IPC stream Buffer decodable by apache-arrow.
  const forge = new GraphForge();
  // The write path also returns a valid IPC summary Buffer.
  const writeSummary = forge.execute(
    "CREATE (:Person {name: 'Alice', age: 30})",
  );
  assert.ok(Buffer.isBuffer(writeSummary), "write should return a Buffer");
  tableFromIPC(writeSummary); // must decode without throwing
  forge.execute("CREATE (:Person {name: 'Bob', age: 25})");

  const buf = forge.execute(
    "MATCH (p:Person) RETURN p.name AS name, p.age AS age",
  );
  assert.ok(Buffer.isBuffer(buf), "execute should return a Buffer");
  const table = tableFromIPC(buf);
  assert.equal(table.numRows, 2, `rows=${table.numRows}`);
  const names = [...table.getChild("name").toArray()].sort();
  assert.deepEqual(names, ["Alice", "Bob"], `names=${names}`);
  // The schema carries the graphforge.* result metadata.
  assert.ok(
    table.schema.metadata.has("graphforge.query_id"),
    "missing query_id metadata",
  );

  // Parameters bind $placeholders.
  const filtered = tableFromIPC(
    forge.execute("MATCH (p:Person) WHERE p.age > $min RETURN p.name AS name", {
      min: 28,
    }),
  );
  assert.deepEqual([...filtered.getChild("name").toArray()], ["Alice"]);

  // A zero-row result still emits a valid schema-only stream.
  const empty = tableFromIPC(
    forge.execute("MATCH (n:Nope) RETURN n.node_uuid AS id"),
  );
  assert.equal(empty.numRows, 0);
  assert.ok(
    empty.schema.fields.some((f) => f.name === "id"),
    "missing id column",
  );
}

function checkTypedUuidParameters() {
  const forge = new GraphForge();
  const alice = forge.addNode("Person", { name: "Alice" });
  const bob = forge.addNode("Person", { name: "Bob" });
  const carol = forge.addNode("Person", { name: "Carol" });
  const edge = forge.addEdge(alice, "KNOWS", bob, {});
  const tagged = (uuid) => ({ $uuid: uuid });
  const uuidBytes = (value) => Buffer.from(value).toString("hex");
  const canonicalHex = (value) => value.replaceAll("-", "");

  const nodes = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) WHERE n.node_uuid = $id RETURN n.node_uuid AS node_uuid, n.name AS name",
      { id: tagged(alice.uuid) },
    ),
  );
  assert.deepEqual(
    nodes.schema.fields.map((field) => [field.name, String(field.type)]),
    [
      ["node_uuid", "FixedSizeBinary[16]"],
      ["name", "Utf8"],
    ],
  );
  assert.deepEqual([...nodes.getChild("name").toArray()], ["Alice"]);
  assert.equal(
    uuidBytes(nodes.getChild("node_uuid").get(0)),
    canonicalHex(alice.uuid),
  );
  const assertQueryMetadata = (metadata) => {
    assert.deepEqual([...metadata.keys()].sort(), [
      "graphforge.ir_version",
      "graphforge.ontology_mode",
      "graphforge.query_id",
    ]);
    assert.equal(metadata.get("graphforge.ir_version"), "0.3.0");
    assert.equal(metadata.get("graphforge.ontology_mode"), "exploratory");
    assert.match(
      metadata.get("graphforge.query_id"),
      /^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
    );
  };
  assertQueryMetadata(nodes.schema.metadata);

  const ordered = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) RETURN n.node_uuid AS node_uuid ORDER BY node_uuid",
    ),
  );
  assert.deepEqual(
    ordered.schema.fields.map((field) => [
      field.name,
      String(field.type),
      field.nullable,
    ]),
    [["node_uuid", "FixedSizeBinary[16]", false]],
  );
  const expectedOrder = [alice.uuid, bob.uuid, carol.uuid]
    .map(canonicalHex)
    .sort();
  assert.deepEqual(
    Array.from({ length: ordered.numRows }, (_, row) =>
      uuidBytes(ordered.getChild("node_uuid").get(row)),
    ),
    expectedOrder,
  );
  assertQueryMetadata(ordered.schema.metadata);

  const edges = tableFromIPC(
    forge.execute(
      "MATCH ()-[r:KNOWS]->() WHERE r.edge_uuid = $id RETURN r.edge_uuid AS edge_uuid",
      { id: tagged(edge.uuid) },
    ),
  );
  assert.equal(String(edges.schema.fields[0].type), "FixedSizeBinary[16]");
  assert.equal(
    uuidBytes(edges.getChild("edge_uuid").get(0)),
    canonicalHex(edge.uuid),
  );

  const textIsNotUuid = tableFromIPC(
    forge.execute(
      "MATCH (n:Person) WHERE n.node_uuid = $id RETURN n.node_uuid AS node_uuid",
      { id: alice.uuid },
    ),
  );
  assert.equal(textIsNotUuid.numRows, 0);
  assert.throws(
    () =>
      forge.execute(
        "MATCH (n:Person) WHERE n.name = $id RETURN n.name AS name",
        {
          id: tagged(alice.uuid),
        },
      ),
    (error) =>
      error.code === "GF_VALIDATION" &&
      error.message ===
        "typed UUID parameter `$id` is only supported as a direct node_uuid or edge_uuid identity equality predicate",
  );
  assert.equal(
    tableFromIPC(forge.execute("MATCH (n) RETURN n.node_uuid AS id")).numRows,
    3,
    "incompatible predicate validation mutated the graph",
  );

  forge.execute("CREATE (:Token {value:$value})", { value: alice.uuid });
  forge.execute("MATCH (n:Token) SET n.copy = $value", { value: alice.uuid });
  const writableText = tableFromIPC(
    forge.execute(
      "MATCH (n:Token) WHERE n.value = $value AND n.copy = $value RETURN n.value AS value",
      { value: alice.uuid },
    ),
  );
  assert.deepEqual([...writableText.getChild("value").toArray()], [alice.uuid]);

  const before = tableFromIPC(
    forge.execute("MATCH (n) RETURN n.node_uuid AS id"),
  ).numRows;
  const expectValidation = (params, message) => {
    assert.throws(
      () => forge.execute("CREATE (:Rejected {value:$value})", params),
      (error) => error.code === "GF_VALIDATION" && error.message === message,
    );
    assert.equal(
      tableFromIPC(forge.execute("MATCH (n) RETURN n.node_uuid AS id")).numRows,
      before,
      "validation failure mutated the graph",
    );
  };
  expectValidation(
    { value: { $uuid: "not-a-uuid" } },
    "UUID parameter must be canonical hyphenated UUID text",
  );
  expectValidation(
    { value: { $uuid: alice.uuid.toUpperCase() } },
    "UUID parameter must be canonical hyphenated UUID text",
  );
  expectValidation(
    { value: { $uuid: alice.uuid, extra: true } },
    "UUID parameter tag must contain only $uuid",
  );
  expectValidation(
    { value: { $uuid: 42 } },
    "UUID parameter $uuid value must be a string",
  );
  expectValidation(
    { value: tagged(alice.uuid) },
    "typed UUID parameter `$value` is only supported as a direct node_uuid or edge_uuid identity equality predicate",
  );
  expectValidation(
    { value: ["safe", tagged(alice.uuid)] },
    "typed UUID parameter `$value` is only supported as a direct node_uuid or edge_uuid identity equality predicate",
  );
  expectValidation(
    { value: { nested: tagged(alice.uuid) } },
    "typed UUID parameter `$value` is only supported as a direct node_uuid or edge_uuid identity equality predicate",
  );
  for (const value of [
    tagged(alice.uuid),
    [tagged(alice.uuid)],
    { nested: tagged(alice.uuid) },
  ]) {
    assert.throws(
      () => forge.execute("MATCH (n:Token) SET n.value = $value", { value }),
      (error) =>
        error.code === "GF_VALIDATION" &&
        error.message ===
          "typed UUID parameter `$value` is only supported as a direct node_uuid or edge_uuid identity equality predicate",
    );
    const unchanged = tableFromIPC(
      forge.execute("MATCH (n:Token) RETURN n.value AS value"),
    );
    assert.deepEqual([...unchanged.getChild("value").toArray()], [alice.uuid]);
  }
}

async function checkProjectCapabilities() {
  const forge = new GraphForge();
  const initial = tableFromIPC(await forge.projectCapabilities());
  assert.deepEqual(
    [...initial.getChild("capability_id").toArray()],
    ["graph", "workspace"],
  );

  const operationUuid = "018f0f4e-7b8c-7000-8000-000000000001";
  const request = {
    operationUuid,
    capabilityId: "knowledge",
    capabilityVersion: 1,
  };
  const enabled = tableFromIPC(await forge.enableCapability(request));
  const replayed = tableFromIPC(await forge.enableCapability(request));
  assert.deepEqual(
    [...enabled.getChild("capability_id").toArray()],
    ["graph", "knowledge", "workspace"],
  );
  assert.deepEqual(
    [...replayed.getChild("generation_uuid").toArray()],
    [...enabled.getChild("generation_uuid").toArray()],
  );
  assert.deepEqual(
    [...replayed.getChild("capability_id").toArray()],
    [...enabled.getChild("capability_id").toArray()],
  );

  await assert.rejects(
    forge.enableCapability({
      operationUuid: "018f0f4e-7b8c-7000-8000-000000000002",
      capabilityId: "knowledge",
      capabilityVersion: 2,
    }),
    (error) => error.code === "GF_UNSUPPORTED_CAPABILITY_VERSION",
  );
}

function checkLifecycle() {
  const forge = new GraphForge();
  forge.close();
  forge.close(); // idempotent
  assert.throws(
    () => forge.addNode("Person", { nested: { unsupported: true } }),
    (error) => error.code === "LifecycleError",
  );
  assert.throws(
    () => forge.paths("not-a-uuid", null, "bfs"),
    (error) => error.code === "LifecycleError",
  );
  try {
    forge.explain("MATCH (n) RETURN n.node_uuid AS id");
  } catch (e) {
    assert.equal(e.code, "LifecycleError", `got code=${e.code}`);
    return;
  }
  throw new Error("expected LifecycleError after close()");
}

test("version", checkVersion);
test("construction", checkConstruction);
test("construction error", checkConstructionError);
test("execute", checkExecute);
test("typed UUID parameters", checkTypedUuidParameters);
test("project capabilities", checkProjectCapabilities);
test("lifecycle", checkLifecycle);
