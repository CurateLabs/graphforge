// Repository-owned release-load probe against the freshly built native addon.
//
// Dense L/XL fixtures exceed the recovery publication bound under scalar
// addNode/addEdge, so construction uses publishBulkNodes/publishBulkEdges.

import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  readFileSync,
  rmSync,
  writeFileSync,
  mkdtempSync,
  readdirSync,
  statSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  Bool,
  Field,
  FixedSizeBinary,
  Float64,
  Int64,
  RecordBatchStreamWriter,
  Schema,
  Table,
  Utf8,
  tableFromIPC,
  vectorFromArray,
} from "apache-arrow";
import { GraphForge } from "../index.js";

const NODE_OPERATION = "018f0f4e-7b8c-7000-8000-00000000b001";
const EDGE_OPERATION = "018f0f4e-7b8c-7000-8000-00000000b002";

const bulkNodeSchema = new Schema(
  [
    new Field("node_uuid", new FixedSizeBinary(16), true),
    new Field("label", new Utf8(), false),
    new Field("active", new Bool(), false),
    new Field("group", new Int64(), false),
    new Field("name", new Utf8(), false),
    new Field("nullable", new Utf8(), true),
    new Field("ordinal", new Int64(), false),
    new Field("salience", new Float64(), false),
  ],
  new Map([
    ["graphforge.bulk_contract_version", "1"],
    ["graphforge.bulk_kind", "node"],
    ["graphforge.row_order", "logical_input_order"],
  ]),
);

const bulkEdgeSchema = new Schema(
  [
    new Field("edge_uuid", new FixedSizeBinary(16), true),
    new Field("rel_type", new Utf8(), false),
    new Field("source_uuid", new FixedSizeBinary(16), false),
    new Field("target_uuid", new FixedSizeBinary(16), false),
    new Field("weight", new Float64(), false),
  ],
  new Map([
    ["graphforge.bulk_contract_version", "1"],
    ["graphforge.bulk_kind", "edge"],
    ["graphforge.row_order", "logical_input_order"],
  ]),
);

const valueAfter = (name) => {
  const index = process.argv.indexOf(name);
  if (index < 0 || index + 1 >= process.argv.length)
    throw new Error(`missing ${name}`);
  return process.argv[index + 1];
};

function directoryBytes(path) {
  let total = 0;
  for (const entry of readdirSync(path, { withFileTypes: true })) {
    const child = join(path, entry.name);
    total += entry.isDirectory() ? directoryBytes(child) : statSync(child).size;
  }
  return total;
}

// Match Python json.dumps / Rust serde_json number spelling so whole floats
// fingerprint as `1.0` (not JSON.stringify's `1`). Nested objects sort keys.
function canonicalJson(value) {
  if (value === null) return "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new Error(`non-finite fingerprint number: ${value}`);
    }
    if (Object.is(value, -0)) return "-0.0";
    if (Number.isInteger(value)) return `${value}.0`;
    return String(value);
  }
  if (typeof value === "string") return JSON.stringify(value);
  if (Array.isArray(value)) {
    return `[${value.map((item) => canonicalJson(item)).join(",")}]`;
  }
  if (typeof value === "object") {
    const keys = Object.keys(value).sort();
    return `{${keys
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(",")}}`;
  }
  throw new Error(`unsupported fingerprint value: ${typeof value}`);
}

const fingerprint = (value) =>
  createHash("sha256").update(canonicalJson(value)).digest("hex");

function tableToIpc(table) {
  return Buffer.from(
    RecordBatchStreamWriter.writeAll(table).toUint8Array(true),
  );
}

function loadFixture(forge, fixture) {
  const nodes = fixture.nodes;
  const nodeTable = new Table(bulkNodeSchema, {
    node_uuid: vectorFromArray(
      nodes.map(() => null),
      new FixedSizeBinary(16),
    ),
    label: vectorFromArray(
      nodes.map((node) => node.label),
      new Utf8(),
    ),
    active: vectorFromArray(
      nodes.map((node) => node.active),
      new Bool(),
    ),
    group: vectorFromArray(
      nodes.map((node) => BigInt(node.group)),
      new Int64(),
    ),
    name: vectorFromArray(
      nodes.map((node) => node.name),
      new Utf8(),
    ),
    nullable: vectorFromArray(
      nodes.map((node) => node.nullable ?? null),
      new Utf8(),
    ),
    ordinal: vectorFromArray(
      nodes.map((node) => BigInt(node.ordinal)),
      new Int64(),
    ),
    salience: vectorFromArray(
      nodes.map((node) => node.salience),
      new Float64(),
    ),
  });
  const nodeReceipt = tableFromIPC(
    forge.publishBulkNodes(NODE_OPERATION, tableToIpc(nodeTable)),
  );
  if (nodeReceipt.numRows !== nodes.length) {
    throw new Error("bulk node receipt row count drifted from fixture");
  }
  const nodeIds = [...nodeReceipt.getChild("entity_uuid")];

  const edges = fixture.edges;
  const edgeTable = new Table(bulkEdgeSchema, {
    edge_uuid: vectorFromArray(
      edges.map(() => null),
      new FixedSizeBinary(16),
    ),
    rel_type: vectorFromArray(
      edges.map((edge) => edge.type),
      new Utf8(),
    ),
    source_uuid: vectorFromArray(
      edges.map((edge) => nodeIds[edge.source]),
      new FixedSizeBinary(16),
    ),
    target_uuid: vectorFromArray(
      edges.map((edge) => nodeIds[edge.target]),
      new FixedSizeBinary(16),
    ),
    weight: vectorFromArray(
      edges.map((edge) => edge.weight),
      new Float64(),
    ),
  });
  forge.publishBulkEdges(EDGE_OPERATION, tableToIpc(edgeTable));
}

const request = JSON.parse(readFileSync(valueAfter("--request"), "utf8"));
const fixture = JSON.parse(readFileSync(request.fixture, "utf8"));
const workload = request.workload.id;
const project = mkdtempSync(join(tmpdir(), "gf-load-node-"));
let report;
let forge;
let reopened;
try {
  forge = new GraphForge(project);
  loadFixture(forge, fixture);
  const nodes = tableFromIPC(
    forge.execute("MATCH (n) RETURN n.name AS name ORDER BY name"),
  );
  const nodeRows = nodes.numRows;
  const schemaSha256 = fingerprint(
    nodes.schema.fields.map((field) => [
      field.name,
      field.type.toString().toLowerCase(),
    ]),
  );
  const nodeResultSha256 = fingerprint([...nodes.getChild("name").toArray()]);
  const edgeRows = tableFromIPC(
    forge.execute("MATCH ()-[r]->() RETURN r"),
  ).numRows;
  let rankRows = 0;
  let findRows = 0;
  let rankResultSha256 = fingerprint([]);
  let findResultSha256 = fingerprint([]);
  if (workload.startsWith("m18-")) {
    const rank = tableFromIPC(forge.rank("Entity", "degree", "LINK"));
    rankRows = rank.numRows;
    rankResultSha256 = fingerprint(
      [...Array(rank.numRows).keys()]
        .map((index) => [
          rank.getChild("name").get(index),
          rank.getChild("score").get(index),
        ])
        .sort((left, right) => left[0].localeCompare(right[0])),
    );
  }
  if (workload.startsWith("m19-")) {
    forge.index("Entity", { properties: ["name"] });
    const found = tableFromIPC(
      forge.find("n-00000001", "Entity", undefined, undefined, undefined, 10),
    );
    findRows = found.numRows;
    findResultSha256 = fingerprint(
      [...Array(found.numRows).keys()]
        .map((index) => [
          found.getChild("name").get(index),
          found.getChild("matched_on").get(index),
        ])
        .sort((left, right) => left[0].localeCompare(right[0])),
    );
  }
  forge.close();
  forge = undefined;
  const persistedBytes = directoryBytes(project);
  reopened = new GraphForge(project);
  const reopenedNodes = tableFromIPC(
    reopened.execute("MATCH (n) RETURN n.name AS name ORDER BY name"),
  );
  const reopenNodeRows = reopenedNodes.numRows;
  const reopenNodeResultSha256 = fingerprint([
    ...reopenedNodes.getChild("name").toArray(),
  ]);
  if (workload.startsWith("m18-")) {
    assert.equal(
      tableFromIPC(reopened.rank("Entity", "degree", "LINK")).numRows,
      rankRows,
    );
  }
  if (workload.startsWith("m19-")) {
    assert.equal(
      tableFromIPC(
        reopened.find(
          "n-00000001",
          "Entity",
          undefined,
          undefined,
          undefined,
          10,
        ),
      ).numRows,
      findRows,
    );
  }
  report = {
    schema: "graphforge-load-native-probe/1",
    language: "node",
    dataset_sha256: request.manifest.content_sha256,
    workload,
    observed: {
      node_rows: nodeRows,
      edge_rows: edgeRows,
      rank_rows: rankRows,
      find_rows: findRows,
      reopen_node_rows: reopenNodeRows,
      schema_sha256: schemaSha256,
      ordering_sha256: nodeResultSha256,
      node_result_sha256: nodeResultSha256,
      rank_result_sha256: rankResultSha256,
      find_result_sha256: findResultSha256,
    },
    persisted_bytes: persistedBytes,
    temporary_bytes: Math.max(0, directoryBytes(project) - persistedBytes),
    cleanup: "pending",
    reopen_equivalent:
      reopenNodeRows === nodeRows &&
      reopenNodeResultSha256 === nodeResultSha256,
  };
} finally {
  try {
    reopened?.close();
    forge?.close();
  } finally {
    rmSync(project, { recursive: true });
  }
}
report.cleanup = "complete";
writeFileSync(valueAfter("--output"), `${JSON.stringify(report)}\n`);
